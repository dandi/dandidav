use super::consts::USER_AGENT;
use async_stream::try_stream;
use aws_sdk_s3::{operation::list_objects_v2::ListObjectsV2Error, Client};
use aws_smithy_runtime_api::client::{orchestrator::HttpResponse, result::SdkError};
use futures_util::{Stream, TryStreamExt};
use reqwest::{ClientBuilder, StatusCode};
use smartstring::alias::CompactString;
use std::borrow::Cow;
use std::cmp::Ordering;
use thiserror::Error;
use url::Url;

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

    // `key_prefix` may or may not end with `/`; it is used as-is
    fn list_entry_pages(
        &self,
        key_prefix: &str,
    ) -> impl Stream<Item = Result<S3EntryPage, S3Error>> {
        let this = self.clone();
        let key_prefix = key_prefix.to_owned();
        try_stream! {
            let mut stream = this.inner
                .list_objects_v2()
                .bucket(&*this.bucket)
                .prefix(key_prefix)
                .delimiter("/")
                .into_paginator()
                .send();
            while let Some(page) = stream.try_next().await? {
                let objects = page.contents.unwrap_or_default().into_iter().filter_map(|obj| {
                    let aws_sdk_s3::types::Object {
                        key: Some(key),
                        last_modified: Some(modified),
                        e_tag: Some(etag),
                        size: Some(size),
                        ..
                    } = obj else {
                        // TODO: Error?  Emit a warning?
                        return None;
                    };
                    // This step shouldn't be necessary, but just in case…
                    let key = match key.strip_prefix('/') {
                        Some(k) => Cow::from(k),
                        None => Cow::from(key),
                    };
                    let download_url = format!("https://{}.s3.amazonaws.com/{}", this.bucket, key);
                    // TODO: Handle this error!
                    let download_url = Url::parse(&download_url).expect("download URL should be valid URL");
                    Some(Object {
                        key: key.to_string(),
                        modified,
                        size,
                        etag,
                        download_url,
                    })
                }).collect::<Vec<_>>();
                let folders = page.common_prefixes
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|compre| {
                        // TODO on None: Error?  Emit a warning?
                        compre.prefix.map(|key_prefix| S3Folder {key_prefix})
                    }).collect::<Vec<_>>();
                yield S3EntryPage {folders, objects};
            }
        }
    }

    // TODO: Enforce that `key_prefix` ends in `/`
    pub(crate) fn get_folder_entries(
        &self,
        key_prefix: &str,
    ) -> impl Stream<Item = Result<S3Entry, S3Error>> {
        let stream = self.list_entry_pages(key_prefix);
        try_stream! {
            tokio::pin!(stream);
            while let Some(page) = stream.try_next().await? {
                for entry in page.flatten() {
                    yield entry;
                }
            }
        }
    }

    // Returns `None` if nothing found at path
    // TODO: Enforce that `path` does NOT end in `/`
    pub(crate) async fn get_path(&self, path: &str) -> Result<Option<S3Entry>, S3Error> {
        let mut surpassed_objects = false;
        let mut surpassed_folders = false;
        let folder_cutoff = format!("{path}/");
        let stream = self.list_entry_pages(path);
        tokio::pin!(stream);
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
                    match folder_cutoff.cmp(&folder.key_prefix) {
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct S3Location {
    bucket: String,
    region: Option<String>,
    key: String,
}

impl std::str::FromStr for S3Location {
    type Err = ParseS3LocationError;

    fn from_str(s: &str) -> Result<S3Location, Self::Err> {
        todo!()
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("TODO")]
pub(crate) struct ParseS3LocationError;

#[derive(Clone, Debug, Eq, PartialEq)]
struct S3EntryPage {
    folders: Vec<S3Folder>,
    objects: Vec<Object>,
}

impl S3EntryPage {
    // TODO: Make this return an iterator instead
    fn flatten(self) -> Vec<S3Entry> {
        let mut output = Vec::with_capacity(self.folders.len().saturating_add(self.objects.len()));
        output.extend(self.folders.into_iter().map(S3Entry::Folder));
        output.extend(self.objects.into_iter().map(S3Entry::Object));
        output
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum S3Entry {
    Folder(S3Folder),
    Object(Object),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct S3Folder {
    pub(crate) key_prefix: String, // End with '/'
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Object {
    pub(crate) key: String,
    pub(crate) modified: aws_sdk_s3::primitives::DateTime,
    pub(crate) size: i64,
    pub(crate) etag: String,
    pub(crate) download_url: Url,
}

#[derive(Debug, Error)]
pub(crate) enum S3Error {
    #[error("failed to list objects")]
    ListObjects(#[from] SdkError<ListObjectsV2Error, HttpResponse>),
}

// The AWS SDK currently cannot be used for this:
// <https://github.com/awslabs/aws-sdk-rust/issues/1052>
pub(crate) async fn get_bucket_region(bucket: &str) -> Result<String, GetBucketRegionError> {
    let client = ClientBuilder::new()
        .user_agent(USER_AGENT)
        .https_only(true)
        .build()
        .map_err(GetBucketRegionError::BuildClient)?;
    let r = client
        .head(format!("https://{bucket}.amazonaws.com"))
        .send()
        .await
        .map_err(GetBucketRegionError::Send)?
        .error_for_status()
        .map_err(GetBucketRegionError::Status)?;
    match r.headers().get("x-amz-bucket-region").map(|hv| hv.to_str()) {
        Some(Ok(region)) => Ok(region.to_owned()),
        Some(Err(e)) => Err(GetBucketRegionError::BadHeader(e)),
        None => Err(GetBucketRegionError::NoHeader),
    }
}

#[derive(Debug, Error)]
pub(crate) enum GetBucketRegionError {
    #[error("failed to initialize HTTP client")]
    BuildClient(#[source] reqwest::Error),
    #[error("failed to make S3 request")]
    Send(#[source] reqwest::Error),
    #[error("S3 request returned error")]
    Status(#[source] reqwest::Error),
    #[error("S3 response lacked x-amz-bucket-region header")]
    NoHeader,
    #[error("S3 response had undecodable x-amz-bucket-region header")]
    BadHeader(#[source] reqwest::header::ToStrError),
}
