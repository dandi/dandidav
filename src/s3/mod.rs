mod streams;
use self::streams::ListEntryPages;
use crate::httputil::{self, BuildClientError, HttpError};
use crate::paths::{ParsePureDirPathError, ParsePurePathError, PureDirPath, PurePath};
use crate::validstr::TryFromStringError;
use async_stream::try_stream;
use aws_sdk_s3::{operation::list_objects_v2::ListObjectsV2Error, types::CommonPrefix, Client};
use aws_smithy_runtime_api::client::{orchestrator::HttpResponse, result::SdkError};
use aws_smithy_types_convert::date_time::DateTimeExt;
use futures_util::{Stream, TryStreamExt};
use smartstring::alias::CompactString;
use std::cmp::Ordering;
use std::sync::Arc;
use thiserror::Error;
use time::OffsetDateTime;
use url::{Host, Url};

type ListObjectsError = SdkError<ListObjectsV2Error, HttpResponse>;

#[derive(Clone, Debug)]
pub(crate) struct S3Client {
    inner: Client,
    bucket: CompactString,
}

impl S3Client {
    pub(crate) async fn new(bucket: CompactString, region: String) -> S3Client {
        let config = aws_config::from_env()
            .app_name(
                aws_config::AppName::new("dandidav")
                    .expect(r#""dandidav" should be a valid app name"#),
            )
            .no_credentials()
            .region(aws_config::Region::new(region))
            .load()
            .await;
        let inner = Client::new(&config);
        S3Client { inner, bucket }
    }

    pub(crate) fn with_prefix(self: Arc<Self>, prefix: PureDirPath) -> PrefixedS3Client {
        PrefixedS3Client {
            inner: self,
            prefix,
        }
    }

    // `key_prefix` may or may not end with `/`; it is used as-is
    fn list_entry_pages<'a>(&'a self, key_prefix: &'a str) -> ListEntryPages<'a> {
        ListEntryPages::new(self, key_prefix)
    }

    pub(crate) fn get_folder_entries<'a>(
        &'a self,
        key_prefix: &'a PureDirPath,
    ) -> impl Stream<Item = Result<S3Entry, S3Error>> + 'a {
        try_stream! {
            let mut stream = self.list_entry_pages(key_prefix.as_ref());
            while let Some(page) = stream.try_next().await? {
                for entry in page {
                    yield entry;
                }
            }
        }
    }

    // Returns `None` if nothing found at path
    pub(crate) async fn get_path(&self, path: &PurePath) -> Result<Option<S3Entry>, S3Error> {
        let mut surpassed_objects = false;
        let mut surpassed_folders = false;
        let folder_cutoff = format!("{path}/");
        let mut stream = self.list_entry_pages(path);
        while let Some(page) = stream.try_next().await? {
            if !surpassed_objects {
                for obj in page.objects {
                    match path.cmp(&obj.key) {
                        Ordering::Equal => return Ok(Some(S3Entry::Object(obj))),
                        Ordering::Less => {
                            surpassed_objects = true;
                            break;
                        }
                        Ordering::Greater => (),
                    }
                }
            }
            if !surpassed_folders {
                for folder in page.folders {
                    match (*folder_cutoff).cmp(&*folder.key_prefix) {
                        Ordering::Equal => return Ok(Some(S3Entry::Folder(folder))),
                        Ordering::Less => {
                            surpassed_folders = true;
                            break;
                        }
                        Ordering::Greater => (),
                    }
                }
            }
            if surpassed_objects && surpassed_folders {
                break;
            }
        }
        Ok(None)
    }
}

// Like `S3Client`, except all paths passed to and in objects returned from
// this type are relative to a prefix
#[derive(Clone, Debug)]
pub(crate) struct PrefixedS3Client {
    inner: Arc<S3Client>,
    prefix: PureDirPath,
}

impl PrefixedS3Client {
    pub(crate) fn get_root_entries(&self) -> impl Stream<Item = Result<S3Entry, S3Error>> + '_ {
        let stream = self.inner.get_folder_entries(&self.prefix);
        try_stream! {
            tokio::pin!(stream);
            while let Some(entry) = stream.try_next().await? {
                if let Some(entry) = entry.relative_to(&self.prefix) {
                    yield entry;
                }
                // TODO: Else: Error? Warn?
            }
        }
    }

    pub(crate) fn get_folder_entries<'a>(
        &'a self,
        dirpath: &'a PureDirPath,
    ) -> impl Stream<Item = Result<S3Entry, S3Error>> + 'a {
        try_stream! {
            let key_prefix = self.prefix.join_dir(dirpath);
            let stream = self.inner.get_folder_entries(&key_prefix);
            tokio::pin!(stream);
            while let Some(entry) = stream.try_next().await? {
                if let Some(entry) = entry.relative_to(&self.prefix) {
                    yield entry;
                }
                // TODO: Else: Error? Warn?
            }
        }
    }

    // Returns `None` if nothing found at path
    pub(crate) async fn get_path(&self, path: &PurePath) -> Result<Option<S3Entry>, S3Error> {
        let fullpath = self.prefix.join(path);
        Ok(self
            .inner
            .get_path(&fullpath)
            .await?
            // TODO: If relative_to() returns None: Error? Warn?
            .and_then(|entry| entry.relative_to(&self.prefix)))
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct BucketSpec {
    pub(crate) bucket: CompactString,
    pub(crate) region: Option<String>,
}

impl BucketSpec {
    pub(crate) async fn into_s3client(self) -> Result<S3Client, GetBucketRegionError> {
        let region = match self.region {
            Some(region) => region,
            None => get_bucket_region(&self.bucket).await?,
        };
        Ok(S3Client::new(self.bucket, region).await)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct S3Location {
    pub(crate) bucket_spec: BucketSpec,
    pub(crate) key: String, // Does not start with a slash
}

impl S3Location {
    pub(crate) fn parse_url(url: &Url) -> Result<S3Location, S3UrlError> {
        // cf. <https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html>
        if !matches!(url.scheme(), "http" | "https") {
            return Err(S3UrlError::NotHttp);
        }
        let Some(Host::Domain(fqdn)) = url.host() else {
            return Err(S3UrlError::NoDomain);
        };
        // Possible domain formats (See link above):
        // - {bucket}.s3.{region}.amazonaws.com
        // - {bucket}.s3-{region}.amazonaws.com
        // - {bucket}.s3.amazonaws.com
        let e = S3UrlError::InvalidDomain;
        let (bucket, s) = fqdn.split_once('.').ok_or(e)?;
        let s = s
            .strip_prefix("s3")
            .ok_or(e)?
            .strip_suffix(".amazonaws.com")
            .ok_or(e)?;
        let region = if s.is_empty() {
            None
        } else if let Some(region) = s.strip_prefix(['.', '-']) {
            if !region.contains('.') {
                Some(region)
            } else {
                return Err(e);
            }
        } else {
            return Err(e);
        };
        let path = url.path();
        let path = path.strip_prefix('/').unwrap_or(path);
        let key = percent_encoding::percent_decode_str(path)
            .decode_utf8()
            .map_err(S3UrlError::BadPath)?
            .into_owned();
        Ok(S3Location {
            bucket_spec: BucketSpec {
                bucket: bucket.into(),
                region: region.map(String::from),
            },
            key,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum S3UrlError {
    #[error("URL is not HTTP(S)")]
    NotHttp,
    #[error("URL lacks domain name")]
    NoDomain,
    #[error("domain in URL is not S3")]
    InvalidDomain,
    #[error("URL path does not decode to UTF-8")]
    BadPath(#[source] std::str::Utf8Error),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct S3EntryPage {
    folders: Vec<S3Folder>,
    objects: Vec<S3Object>,
}

impl IntoIterator for S3EntryPage {
    type Item = S3Entry;
    type IntoIter = S3EntryPageIter;

    fn into_iter(self) -> S3EntryPageIter {
        S3EntryPageIter::new(self)
    }
}

#[derive(Clone, Debug)]
struct S3EntryPageIter {
    folders_iter: std::vec::IntoIter<S3Folder>,
    objects_iter: std::vec::IntoIter<S3Object>,
}

impl S3EntryPageIter {
    fn new(page: S3EntryPage) -> S3EntryPageIter {
        S3EntryPageIter {
            folders_iter: page.folders.into_iter(),
            objects_iter: page.objects.into_iter(),
        }
    }
}

impl Iterator for S3EntryPageIter {
    type Item = S3Entry;

    fn next(&mut self) -> Option<S3Entry> {
        self.folders_iter
            .next()
            .map(S3Entry::Folder)
            .or_else(|| self.objects_iter.next().map(S3Entry::Object))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum S3Entry {
    Folder(S3Folder),
    Object(S3Object),
}

impl S3Entry {
    pub(crate) fn relative_to(&self, dirpath: &PureDirPath) -> Option<S3Entry> {
        match self {
            S3Entry::Folder(r) => Some(S3Entry::Folder(r.relative_to(dirpath)?)),
            S3Entry::Object(r) => Some(S3Entry::Object(r.relative_to(dirpath)?)),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct S3Folder {
    pub(crate) key_prefix: PureDirPath,
}

impl S3Folder {
    pub(crate) fn relative_to(&self, dirpath: &PureDirPath) -> Option<S3Folder> {
        Some(S3Folder {
            key_prefix: self.key_prefix.relative_to(dirpath)?,
        })
    }
}

impl TryFrom<CommonPrefix> for S3Folder {
    type Error = TryFromCommonPrefixError;

    fn try_from(value: CommonPrefix) -> Result<S3Folder, Self::Error> {
        let Some(prefix) = value.prefix else {
            return Err(TryFromCommonPrefixError::NoPrefix);
        };
        let key_prefix =
            PureDirPath::try_from(prefix).map_err(TryFromCommonPrefixError::BadPrefix)?;
        Ok(S3Folder { key_prefix })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct S3Object {
    pub(crate) key: PurePath,
    pub(crate) modified: OffsetDateTime,
    pub(crate) size: i64,
    pub(crate) etag: String,
    pub(crate) download_url: Url,
}

impl S3Object {
    fn try_from_aws_object(
        obj: aws_sdk_s3::types::Object,
        bucket: &str,
    ) -> Result<S3Object, TryFromAwsObjectError> {
        let Some(key) = obj.key else {
            return Err(TryFromAwsObjectError::NoKey);
        };
        let Some(modified) = obj.last_modified else {
            return Err(TryFromAwsObjectError::NoLastModified { key });
        };
        let Some(etag) = obj.e_tag else {
            return Err(TryFromAwsObjectError::NoETag { key });
        };
        let Some(size) = obj.size else {
            return Err(TryFromAwsObjectError::NoSize { key });
        };
        let keypath = PurePath::try_from(key.clone()).map_err(TryFromAwsObjectError::BadKey)?;
        let mut download_url = Url::parse(&format!("https://{bucket}.s3.amazonaws.com"))
            .expect("bucket should be a valid hostname component");
        // Adding the key this way is necessary in order for URL-unsafe
        // characters to be percent-encoded:
        download_url
            .path_segments_mut()
            .expect("HTTPS URL should be able to be a base")
            .extend(key.split('/'));
        let modified = modified
            .to_time()
            .map_err(|source| TryFromAwsObjectError::BadModified {
                key,
                modified,
                source,
            })?;
        Ok(S3Object {
            key: keypath,
            modified,
            size,
            etag,
            download_url,
        })
    }
}

impl S3Object {
    pub(crate) fn relative_to(&self, dirpath: &PureDirPath) -> Option<S3Object> {
        let key = self.key.relative_to(dirpath)?;
        Some(S3Object {
            key,
            modified: self.modified,
            size: self.size,
            etag: self.etag.clone(),
            download_url: self.download_url.clone(),
        })
    }
}

#[derive(Debug, Error)]
pub(crate) enum S3Error {
    #[error("failed to list S3 objects in bucket {bucket:?} with prefix {prefix:?}")]
    ListObjects {
        bucket: CompactString,
        prefix: String,
        source: ListObjectsError,
    },
    #[error("invalid object found in S3 bucket {bucket:?} under prefix {prefix:?}")]
    BadObject {
        bucket: CompactString,
        prefix: String,
        source: TryFromAwsObjectError,
    },
    #[error("invalid common prefix found in bucket {bucket:?} under prefix {prefix:?}")]
    BadPrefix {
        bucket: CompactString,
        prefix: String,
        source: TryFromCommonPrefixError,
    },
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub(crate) enum TryFromCommonPrefixError {
    #[error(r#"CommonPrefix lacks "prefix" field"#)]
    NoPrefix,
    #[error("CommonPrefix is not a well-formed directory path")]
    BadPrefix(#[source] TryFromStringError<ParsePureDirPathError>),
}

#[derive(Debug, Error)]
pub(crate) enum TryFromAwsObjectError {
    #[error("S3 object lacks key")]
    NoKey,
    #[error("S3 object with key {key:?} lacks last_modified")]
    NoLastModified { key: String },
    #[error("S3 object with key {key:?} lacks e_tag")]
    NoETag { key: String },
    #[error("S3 object with key {key:?} lacks size")]
    NoSize { key: String },
    #[error("S3 key is not a well-formed path")]
    BadKey(#[source] TryFromStringError<ParsePurePathError>),
    #[error(
        "last_modified value {modified} for S3 object {key:?} is outside time library's range"
    )]
    BadModified {
        key: String,
        modified: aws_sdk_s3::primitives::DateTime,
        source: aws_smithy_types_convert::date_time::Error,
    },
}

// The AWS SDK currently cannot be used for this:
// <https://github.com/awslabs/aws-sdk-rust/issues/1052>
pub(crate) async fn get_bucket_region(bucket: &str) -> Result<String, GetBucketRegionError> {
    let url_str = format!("https://{bucket}.s3.amazonaws.com");
    let url = url_str
        .parse::<Url>()
        .map_err(|source| GetBucketRegionError::BadUrl {
            url: url_str,
            source,
        })?;
    let client = httputil::Client::new()?;
    let r = client.head(url).await?;
    match r.headers().get("x-amz-bucket-region").map(|hv| hv.to_str()) {
        Some(Ok(region)) => Ok(region.to_owned()),
        Some(Err(e)) => Err(GetBucketRegionError::BadHeader(e)),
        None => Err(GetBucketRegionError::NoHeader),
    }
}

#[derive(Debug, Error)]
pub(crate) enum GetBucketRegionError {
    #[error(transparent)]
    BuildClient(#[from] BuildClientError),
    #[error(transparent)]
    Http(#[from] HttpError),
    #[error("URL constructed for bucket is invalid: {url:?}")]
    BadUrl {
        url: String,
        source: url::ParseError,
    },
    #[error("S3 response lacked x-amz-bucket-region header")]
    NoHeader,
    #[error("S3 response had undecodable x-amz-bucket-region header")]
    BadHeader(#[source] reqwest::header::ToStrError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case(
        "https://dandiarchive.s3.amazonaws.com/zarr/bf47be1a-4fed-4105-bcb4-c52534a45b82/",
        "dandiarchive",
        None,
        "zarr/bf47be1a-4fed-4105-bcb4-c52534a45b82/"
    )]
    #[case("https://dandiarchive.s3.us-west-2.amazonaws.com/zarr/bf47be1a-4fed-4105-bcb4-c52534a45b82/", "dandiarchive", Some("us-west-2"), "zarr/bf47be1a-4fed-4105-bcb4-c52534a45b82/")]
    #[case("https://dandiarchive.s3-us-west-2.amazonaws.com/zarr/bf47be1a-4fed-4105-bcb4-c52534a45b82/", "dandiarchive", Some("us-west-2"), "zarr/bf47be1a-4fed-4105-bcb4-c52534a45b82/")]
    #[case(
        "https://dandiarchive.s3.amazonaws.com/zarr/bf47be1a-4fed-4105-bcb4-c52534a45b82/foo%20bar.txt",
        "dandiarchive",
        None,
        "zarr/bf47be1a-4fed-4105-bcb4-c52534a45b82/foo bar.txt"
    )]
    fn test_good_s3location_urls(
        #[case] url: Url,
        #[case] bucket: &str,
        #[case] region: Option<&str>,
        #[case] key: &str,
    ) {
        let s3loc = S3Location::parse_url(&url).unwrap();
        assert_eq!(s3loc.bucket_spec.bucket, bucket);
        assert_eq!(s3loc.bucket_spec.region.as_deref(), region);
        assert_eq!(s3loc.key, key);
    }

    #[rstest]
    #[case("https://s3.amazonaws.com/dandiarchive/zarr/bf47be1a-4fed-4105-bcb4-c52534a45b82/")]
    #[case("https://dandiarchive.amazonaws.com/zarr/bf47be1a-4fed-4105-bcb4-c52534a45b82/")]
    #[case(
        "https://dandiarchive.us-west-2.amazonaws.com/zarr/bf47be1a-4fed-4105-bcb4-c52534a45b82/"
    )]
    fn test_bad_s3location_urls(#[case] url: Url) {
        let r = S3Location::parse_url(&url);
        assert!(r.is_err());
    }
}
