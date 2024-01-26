mod dandiset_id;
mod types;
mod version_id;
pub(crate) use self::dandiset_id::*;
pub(crate) use self::types::*;
pub(crate) use self::version_id::*;
use crate::consts::{S3CLIENT_CACHE_SIZE, USER_AGENT};
use crate::paths::{ParsePureDirPathError, PureDirPath, PurePath};
use crate::s3::{
    BucketSpec, GetBucketRegionError, PrefixedS3Client, S3Client, S3Entry, S3Error, S3Location,
};
use async_stream::try_stream;
use futures_util::{Stream, TryStreamExt};
use lru::LruCache;
use reqwest::{ClientBuilder, StatusCode};
use serde::de::DeserializeOwned;
use smartstring::alias::CompactString;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Mutex;
use url::Url;

#[derive(Clone, Debug)]
pub(crate) struct Client {
    inner: reqwest::Client,
    api_url: Url,
    s3clients: Arc<Mutex<LruCache<BucketSpec, Arc<S3Client>>>>,
}

impl Client {
    pub(crate) fn new(api_url: Url) -> Result<Self, BuildClientError> {
        let inner = ClientBuilder::new().user_agent(USER_AGENT).build()?;
        let s3clients = Arc::new(Mutex::new(LruCache::new(S3CLIENT_CACHE_SIZE)));
        Ok(Client {
            inner,
            api_url,
            s3clients,
        })
    }

    async fn get<T: DeserializeOwned>(&self, url: Url) -> Result<T, ApiError> {
        let r = self
            .inner
            .get(url.clone())
            .send()
            .await
            .map_err(|source| ApiError::Send {
                url: url.clone(),
                source,
            })?;
        if r.status() == StatusCode::NOT_FOUND {
            return Err(ApiError::NotFound { url: url.clone() });
        }
        r.error_for_status()
            .map_err(|source| ApiError::Status {
                url: url.clone(),
                source,
            })?
            .json::<T>()
            .await
            .map_err(move |source| ApiError::Deserialize { url, source })
    }

    fn paginate<T: DeserializeOwned>(&self, url: Url) -> impl Stream<Item = Result<T, ApiError>> {
        let this = self.clone();
        try_stream! {
            let mut url = Some(url);
            while let Some(u) = url {
                let resp = this.inner
                    .get(u.clone())
                    .send()
                    .await
                    .map_err(|source| ApiError::Send {url: u.clone(), source})?;
                if resp.status() == StatusCode::NOT_FOUND {
                    Err(ApiError::NotFound {url: u.clone() })?;
                }
                let page = resp.error_for_status()
                    .map_err(|source| ApiError::Status {url: u.clone(), source})?
                    .json::<Page<T>>()
                    .await
                    .map_err(move |source| ApiError::Deserialize {url: u, source})?;
                for r in page.results {
                    yield r;
                }
                url = page.next;
            }
        }
    }

    async fn get_s3client(&self, loc: S3Location) -> Result<PrefixedS3Client, ApiError> {
        let S3Location {
            bucket_spec,
            mut key,
        } = loc;
        if !key.ends_with('/') {
            key.push('/');
        }
        let prefix = key
            .parse::<PureDirPath>()
            .map_err(|source| ApiError::BadS3Key { key, source })?;
        let client = {
            let mut cache = self.s3clients.lock().await;
            if let Some(client) = cache.get(&bucket_spec) {
                client.clone()
            } else {
                match bucket_spec.clone().into_s3client().await {
                    Ok(client) => {
                        let client = Arc::new(client);
                        cache.put(bucket_spec, client.clone());
                        client
                    }
                    Err(source) => {
                        return Err(ApiError::LocateBucket {
                            bucket: bucket_spec.bucket,
                            source,
                        })
                    }
                }
            }
        };
        Ok(client.with_prefix(prefix))
    }

    pub(crate) fn get_all_dandisets(&self) -> impl Stream<Item = Result<Dandiset, ApiError>> {
        self.paginate(urljoin(&self.api_url, ["dandisets"]))
    }

    pub(crate) fn dandiset<'a>(&'a self, dandiset_id: &'a DandisetId) -> DandisetEndpoint<'a> {
        DandisetEndpoint::new(self, dandiset_id)
    }
}

#[derive(Copy, Clone, Debug)]
pub(crate) struct DandisetEndpoint<'a> {
    client: &'a Client,
    dandiset_id: &'a DandisetId,
}

impl<'a> DandisetEndpoint<'a> {
    fn new(client: &'a Client, dandiset_id: &'a DandisetId) -> Self {
        Self {
            client,
            dandiset_id,
        }
    }

    pub(crate) fn version(self, version_id: &'a VersionId) -> VersionEndpoint<'a> {
        VersionEndpoint::new(self, version_id)
    }

    pub(crate) async fn get(&self) -> Result<Dandiset, ApiError> {
        self.client
            .get(urljoin(
                &self.client.api_url,
                ["dandisets", self.dandiset_id.as_ref()],
            ))
            .await
    }

    pub(crate) fn get_all_versions(&self) -> impl Stream<Item = Result<DandisetVersion, ApiError>> {
        self.client.paginate(urljoin(
            &self.client.api_url,
            ["dandisets", self.dandiset_id.as_ref(), "versions"],
        ))
    }
}

#[derive(Copy, Clone, Debug)]
pub(crate) struct VersionEndpoint<'a> {
    client: &'a Client,
    dandiset_id: &'a DandisetId,
    version_id: &'a VersionId,
}

impl<'a> VersionEndpoint<'a> {
    fn new(upper: DandisetEndpoint<'a>, version_id: &'a VersionId) -> Self {
        Self {
            client: upper.client,
            dandiset_id: upper.dandiset_id,
            version_id,
        }
    }

    pub(crate) async fn get(&self) -> Result<DandisetVersion, ApiError> {
        self.client
            .get(urljoin(
                &self.client.api_url,
                [
                    "dandisets",
                    self.dandiset_id.as_ref(),
                    "versions",
                    self.version_id.as_ref(),
                    "info",
                ],
            ))
            .await
    }

    pub(crate) async fn get_metadata(&self) -> Result<VersionMetadata, ApiError> {
        let data = self
            .client
            .get::<serde_json::Value>(urljoin(
                &self.client.api_url,
                [
                    "dandisets",
                    self.dandiset_id.as_ref(),
                    "versions",
                    self.version_id.as_ref(),
                ],
            ))
            .await?;
        Ok(VersionMetadata(dump_json_as_yaml(data).into_bytes()))
    }

    pub(crate) fn get_folder_entries(
        &self,
        path: &AssetFolder,
    ) -> impl Stream<Item = Result<FolderEntry, ApiError>> {
        let mut url = urljoin(
            &self.client.api_url,
            [
                "dandisets",
                self.dandiset_id.as_ref(),
                "versions",
                self.version_id.as_ref(),
                "assets",
                "paths",
            ],
        );
        if let AssetFolder::Path(path) = path {
            // Experimentation has shown that adding a trailing slash to the
            // `path_prefix` is superfluous, and the Archive will do the right
            // thing (namely, treat the prefix as a full folder path) even if
            // `path_prefix=foo` and there exists a `foobar.txt`.
            url.query_pairs_mut()
                .append_pair("path_prefix", path.as_ref());
        }
        self.client.paginate(url)
    }

    pub(crate) async fn get_path(&self, path: &PurePath) -> Result<AtAssetPath, ApiError> {
        let mut url = urljoin(
            &self.client.api_url,
            [
                "dandisets",
                self.dandiset_id.as_ref(),
                "versions",
                self.version_id.as_ref(),
                "assets",
            ],
        );
        url.query_pairs_mut()
            .append_pair("path", path.as_ref())
            .append_pair("metadata", "1")
            .append_pair("order", "path");
        let cutoff = format!("{path}/");
        let mut stream = self.client.paginate::<Asset>(url.clone());
        tokio::pin!(stream);
        while let Some(asset) = stream.try_next().await? {
            if asset.path() == path {
                return Ok(AtAssetPath::Asset(asset));
            } else if asset.path().is_strictly_under(path) {
                return Ok(AtAssetPath::Folder(AssetFolder::Path(path.clone())));
            } else if **asset.path() > *cutoff {
                break;
            }
        }
        Err(ApiError::NotFound { url })
    }

    async fn get_resource_with_s3(&self, path: &PurePath) -> Result<DandiResourceWithS3, ApiError> {
        /*
        Algorithm for efficiently (yet not always correctly) splitting `path`
        into an asset path and an optional Zarr entry path (cf.
        <https://github.com/dandi/dandi-webdav/issues/5>):

        - For each non-final component in `path` from left to right that has a
          `.zarr` or `.ngff` extension (case sensitive), query the asset path
          up through that component.  If 404, return 404.  If blob asset,
          return 404.  If folder, go to next candidate.  Otherwise, we have a
          Zarr asset, and the rest of the original path is the Zarr entry path.

        - If all components are exhausted without erroring or finding a Zarr,
          treat the entirety of `path` as an asset/folder path.
        */
        for (zarr_path, entry_path) in path.split_zarr_candidates() {
            match self.get_path(&zarr_path).await? {
                AtAssetPath::Folder(_) => continue,
                AtAssetPath::Asset(Asset::Blob(_)) => {
                    let mut url = urljoin(
                        &self.client.api_url,
                        [
                            "dandisets",
                            self.dandiset_id.as_ref(),
                            "versions",
                            self.version_id.as_ref(),
                            "assets",
                        ],
                    );
                    url.query_pairs_mut().append_pair("path", path.as_ref());
                    return Err(ApiError::NotFound { url });
                }
                AtAssetPath::Asset(Asset::Zarr(zarr)) => {
                    let Some(s3loc) = zarr.s3location() else {
                        return Err(ApiError::ZarrLacksS3Url {
                            asset_id: zarr.asset_id,
                        });
                    };
                    let s3 = self.client.get_s3client(s3loc).await?;
                    return match s3.get_path(&entry_path).await? {
                        Some(S3Entry::Folder(folder)) => Ok(DandiResourceWithS3::ZarrFolder {
                            folder: ZarrFolder {
                                path: folder.key_prefix,
                            },
                            s3,
                        }),
                        Some(S3Entry::Object(obj)) => {
                            Ok(DandiResourceWithS3::ZarrEntry(ZarrEntry {
                                path: obj.key,
                                size: obj.size,
                                modified: obj.modified,
                                etag: obj.etag,
                                url: obj.download_url,
                            }))
                        }
                        None => Err(ApiError::ZarrEntryNotFound {
                            zarr_path,
                            entry_path,
                        }),
                    };
                }
            }
        }
        self.get_path(path).await.map(Into::into)
    }

    pub(crate) async fn get_resource(&self, path: &PurePath) -> Result<DandiResource, ApiError> {
        self.get_resource_with_s3(path).await.map(Into::into)
    }

    pub(crate) async fn get_resource_with_children(
        &self,
        path: &PurePath,
    ) -> Result<DandiResourceWithChildren, ApiError> {
        match self.get_resource_with_s3(path).await? {
            DandiResourceWithS3::Folder(r) => {
                // - Call `self.get_folder_entries()`
                // - Get properties for each asset fetched
                todo!()
            }
            DandiResourceWithS3::Asset(Asset::Blob(r)) => Ok(DandiResourceWithChildren::Blob(r)),
            DandiResourceWithS3::Asset(Asset::Zarr(zarr)) => {
                // - Construct S3 client
                // - Get children from S3
                // - Return Zarr along with children
                todo!()
            }
            DandiResourceWithS3::ZarrFolder { folder, s3 } => {
                // Call S3Client.get_folder_entries("{prefix}/")
                todo!()
            }
            DandiResourceWithS3::ZarrEntry(r) => Ok(DandiResourceWithChildren::ZarrEntry(r)),
        }
    }
}

#[derive(Debug, Error)]
#[error("failed to initialize Dandi API client")]
pub(crate) struct BuildClientError(#[from] reqwest::Error);

#[derive(Debug, Error)]
pub(crate) enum ApiError {
    #[error("failed to make request to {url}")]
    Send { url: Url, source: reqwest::Error },
    #[error("no such resource: {url}")]
    NotFound { url: Url },
    #[error("entry {entry_path:?} in Zarr {zarr_path:?} not found")]
    ZarrEntryNotFound {
        zarr_path: PurePath,
        entry_path: PurePath,
    },
    #[error("request to {url} returned error")]
    Status { url: Url, source: reqwest::Error },
    #[error("failed to deserialize response body from {url}")]
    Deserialize { url: Url, source: reqwest::Error },
    #[error("key in S3 URL is not a well-formed path: {key:?}")]
    BadS3Key {
        key: String,
        source: ParsePureDirPathError,
    },
    #[error("failed to determine region for S3 bucket {bucket:?}")]
    LocateBucket {
        bucket: CompactString,
        source: GetBucketRegionError,
    },
    #[error("Zarr with asset ID {asset_id} does not have an S3 download URL")]
    ZarrLacksS3Url { asset_id: String },
    #[error(transparent)]
    S3(#[from] S3Error),
}

fn urljoin<I>(url: &Url, segments: I) -> Url
where
    I: IntoIterator,
    I::Item: AsRef<str>,
{
    let mut url = url.clone();
    url.path_segments_mut()
        .expect("API URL should be able to be a base")
        .pop_if_empty()
        .extend(segments);
    url
}

fn dump_json_as_yaml(data: serde_json::Value) -> String {
    serde_yaml::to_string(&data).expect("converting JSON to YAML should not fail")
}

#[cfg(test)]
mod tests {
    use super::*;
    use indoc::indoc;
    use rstest::rstest;
    use serde_json::json;

    #[rstest]
    #[case("https://api.github.com")]
    #[case("https://api.github.com/")]
    fn test_urljoin_nopath(#[case] base: Url) {
        let u = urljoin(&base, ["foo"]);
        assert_eq!(u.as_str(), "https://api.github.com/foo");
        let u = urljoin(&base, ["foo", "bar"]);
        assert_eq!(u.as_str(), "https://api.github.com/foo/bar");
    }

    #[rstest]
    #[case("https://api.github.com/foo/bar")]
    #[case("https://api.github.com/foo/bar/")]
    fn test_urljoin_path(#[case] base: Url) {
        let u = urljoin(&base, ["gnusto"]);
        assert_eq!(u.as_str(), "https://api.github.com/foo/bar/gnusto");
        let u = urljoin(&base, ["gnusto", "cleesh"]);
        assert_eq!(u.as_str(), "https://api.github.com/foo/bar/gnusto/cleesh");
    }

    #[rstest]
    #[case("foo#bar", "https://api.github.com/base/foo%23bar")]
    #[case("foo%bar", "https://api.github.com/base/foo%25bar")]
    #[case("foo/bar", "https://api.github.com/base/foo%2Fbar")]
    #[case("foo?bar", "https://api.github.com/base/foo%3Fbar")]
    fn test_urljoin_special_chars(#[case] path: &str, #[case] expected: &str) {
        let base = Url::parse("https://api.github.com/base").unwrap();
        let u = urljoin(&base, [path]);
        assert_eq!(u.as_str(), expected);
    }

    #[test]
    fn test_dump_json_as_yaml() {
        let data = json! ({
            "key": "value",
            "int": 42,
            "truth": true,
            "void": null,
            "list": ["apple", "banana", "coconut"],
            "mapping": {
                "apple": "green",
                "banana": "yellow",
                "coconut": "brown",
            }
        });
        let s = dump_json_as_yaml(data);
        assert_eq!(
            s,
            indoc! {"
            key: value
            int: 42
            truth: true
            void: null
            list:
            - apple
            - banana
            - coconut
            mapping:
              apple: green
              banana: yellow
              coconut: brown
        "}
        );
    }
}
