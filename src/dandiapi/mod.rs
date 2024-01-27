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

    fn get_url<I>(&self, segments: I) -> Url
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        urljoin(&self.api_url, segments)
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

    fn paginate<T: DeserializeOwned + 'static>(
        &self,
        url: Url,
    ) -> impl Stream<Item = Result<T, ApiError>> + '_ {
        try_stream! {
            let mut url = Some(url);
            while let Some(u) = url {
                let resp = self.inner
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

    async fn get_s3client(&self, loc: S3Location) -> Result<PrefixedS3Client, ZarrToS3Error> {
        let S3Location {
            bucket_spec,
            mut key,
        } = loc;
        if !key.ends_with('/') {
            key.push('/');
        }
        let prefix = key
            .parse::<PureDirPath>()
            .map_err(|source| ZarrToS3Error::BadS3Key { key, source })?;
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
                        return Err(ZarrToS3Error::LocateBucket {
                            bucket: bucket_spec.bucket,
                            source,
                        })
                    }
                }
            }
        };
        Ok(client.with_prefix(prefix))
    }

    async fn get_s3client_for_zarr(&self, zarr: &ZarrAsset) -> Result<PrefixedS3Client, ApiError> {
        let Some(s3loc) = zarr.s3location() else {
            return Err(ApiError::ZarrToS3Error {
                asset_id: zarr.asset_id.clone(),
                source: ZarrToS3Error::ZarrLacksS3Url,
            });
        };
        self.get_s3client(s3loc)
            .await
            .map_err(|source| ApiError::ZarrToS3Error {
                asset_id: zarr.asset_id.clone(),
                source,
            })
    }

    pub(crate) fn get_all_dandisets(&self) -> impl Stream<Item = Result<Dandiset, ApiError>> + '_ {
        self.paginate(self.get_url(["dandisets"]))
    }

    pub(crate) fn dandiset(&self, dandiset_id: DandisetId) -> DandisetEndpoint<'_> {
        DandisetEndpoint::new(self, dandiset_id)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DandisetEndpoint<'a> {
    client: &'a Client,
    dandiset_id: DandisetId,
}

impl<'a> DandisetEndpoint<'a> {
    fn new(client: &'a Client, dandiset_id: DandisetId) -> Self {
        Self {
            client,
            dandiset_id,
        }
    }

    pub(crate) fn version(self, version_id: VersionId) -> VersionEndpoint<'a> {
        VersionEndpoint::new(self, version_id)
    }

    pub(crate) async fn get(&self) -> Result<Dandiset, ApiError> {
        self.client
            .get(
                self.client
                    .get_url(["dandisets", self.dandiset_id.as_ref()]),
            )
            .await
    }

    pub(crate) fn get_all_versions(
        &self,
    ) -> impl Stream<Item = Result<DandisetVersion, ApiError>> + '_ {
        self.client.paginate(self.client.get_url([
            "dandisets",
            self.dandiset_id.as_ref(),
            "versions",
        ]))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct VersionEndpoint<'a> {
    client: &'a Client,
    dandiset_id: DandisetId,
    version_id: VersionId,
}

impl<'a> VersionEndpoint<'a> {
    fn new(upper: DandisetEndpoint<'a>, version_id: VersionId) -> Self {
        Self {
            client: upper.client,
            dandiset_id: upper.dandiset_id,
            version_id,
        }
    }

    pub(crate) async fn get(&self) -> Result<DandisetVersion, ApiError> {
        self.client
            .get(self.client.get_url([
                "dandisets",
                self.dandiset_id.as_ref(),
                "versions",
                self.version_id.as_ref(),
                "info",
            ]))
            .await
    }

    pub(crate) async fn get_metadata(&self) -> Result<VersionMetadata, ApiError> {
        let data = self
            .client
            .get::<serde_json::Value>(self.client.get_url([
                "dandisets",
                self.dandiset_id.as_ref(),
                "versions",
                self.version_id.as_ref(),
            ]))
            .await?;
        Ok(VersionMetadata(dump_json_as_yaml(data).into_bytes()))
    }

    pub(crate) async fn get_asset_by_id(&self, id: &str) -> Result<Asset, ApiError> {
        self.client
            .get(self.client.get_url([
                "dandisets",
                self.dandiset_id.as_ref(),
                "versions",
                self.version_id.as_ref(),
                "assets",
                id,
                "info",
            ]))
            .await
    }

    pub(crate) fn get_root_children(
        &self,
    ) -> impl Stream<Item = Result<DandiResource, ApiError>> + '_ {
        try_stream! {
            let stream = self.get_entries_under_path(None);
            tokio::pin!(stream);
            while let Some(entry) = stream.try_next().await? {
                match entry {
                    FolderEntry::Folder(subf) => yield DandiResource::Folder(subf),
                    FolderEntry::Asset { id, path } => match self.get_asset_by_id(&id).await {
                        Ok(asset) => yield DandiResource::Asset(asset),
                        Err(ApiError::NotFound { .. }) => {
                            Err(ApiError::DisappearingAsset { asset_id: id, path })?;
                        }
                        Err(e) => Err(e)?,
                    },
                }
            }
        }
    }

    fn get_folder_entries(
        &self,
        path: &AssetFolder,
    ) -> impl Stream<Item = Result<FolderEntry, ApiError>> + '_ {
        self.get_entries_under_path(Some(&path.path))
    }

    fn get_entries_under_path(
        &self,
        path: Option<&PureDirPath>,
    ) -> impl Stream<Item = Result<FolderEntry, ApiError>> + '_ {
        let mut url = self.client.get_url([
            "dandisets",
            self.dandiset_id.as_ref(),
            "versions",
            self.version_id.as_ref(),
            "assets",
            "paths",
        ]);
        if let Some(path) = path {
            url.query_pairs_mut()
                .append_pair("path_prefix", path.as_ref());
        }
        self.client.paginate(url)
    }

    pub(crate) async fn get_path(&self, path: &PurePath) -> Result<AtAssetPath, ApiError> {
        let mut url = self.client.get_url([
            "dandisets",
            self.dandiset_id.as_ref(),
            "versions",
            self.version_id.as_ref(),
            "assets",
        ]);
        url.query_pairs_mut()
            .append_pair("path", path.as_ref())
            .append_pair("metadata", "1")
            .append_pair("order", "path");
        let dirpath = path.to_dir_path();
        let stream = self.client.paginate::<Asset>(url.clone());
        tokio::pin!(stream);
        while let Some(asset) = stream.try_next().await? {
            if asset.path() == path {
                return Ok(AtAssetPath::Asset(asset));
            } else if asset.path().is_strictly_under(&dirpath) {
                return Ok(AtAssetPath::Folder(AssetFolder { path: dirpath }));
            } else if **asset.path() > *dirpath {
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
                    let mut url = self.client.get_url([
                        "dandisets",
                        self.dandiset_id.as_ref(),
                        "versions",
                        self.version_id.as_ref(),
                        "assets",
                    ]);
                    url.query_pairs_mut().append_pair("path", path.as_ref());
                    return Err(ApiError::NotFound { url });
                }
                AtAssetPath::Asset(Asset::Zarr(zarr)) => {
                    let s3 = self.client.get_s3client_for_zarr(&zarr).await?;
                    return match s3.get_path(&entry_path).await? {
                        Some(entry) => Ok(DandiResource::from(entry).with_s3(s3)),
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
            DandiResourceWithS3::Folder(folder) => {
                let mut children = Vec::new();
                let stream = self.get_folder_entries(&folder);
                tokio::pin!(stream);
                while let Some(child) = stream.try_next().await? {
                    let child = match child {
                        FolderEntry::Folder(subf) => DandiResource::Folder(subf),
                        FolderEntry::Asset { id, path } => match self.get_asset_by_id(&id).await {
                            Ok(asset) => DandiResource::Asset(asset),
                            Err(ApiError::NotFound { .. }) => {
                                return Err(ApiError::DisappearingAsset { asset_id: id, path })
                            }
                            Err(e) => return Err(e),
                        },
                    };
                    children.push(child);
                }
                Ok(DandiResourceWithChildren::Folder { folder, children })
            }
            DandiResourceWithS3::Asset(Asset::Blob(r)) => Ok(DandiResourceWithChildren::Blob(r)),
            DandiResourceWithS3::Asset(Asset::Zarr(zarr)) => {
                let s3 = self.client.get_s3client_for_zarr(&zarr).await?;
                let mut children = Vec::new();
                {
                    let stream = s3.get_root_entries();
                    tokio::pin!(stream);
                    while let Some(child) = stream.try_next().await? {
                        children.push(DandiResource::from(child));
                    }
                }
                Ok(DandiResourceWithChildren::Zarr { zarr, children })
            }
            DandiResourceWithS3::ZarrFolder { folder, s3 } => {
                let mut children = Vec::new();
                {
                    let stream = s3.get_folder_entries(&folder.path);
                    tokio::pin!(stream);
                    while let Some(child) = stream.try_next().await? {
                        children.push(DandiResource::from(child));
                    }
                }
                Ok(DandiResourceWithChildren::ZarrFolder { folder, children })
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
    #[error("folder listing included asset ID={asset_id} at path {path:?}, but request to asset returned 404")]
    DisappearingAsset { asset_id: String, path: PurePath },
    #[error("request to {url} returned error")]
    Status { url: Url, source: reqwest::Error },
    #[error("failed to deserialize response body from {url}")]
    Deserialize { url: Url, source: reqwest::Error },
    #[error("failed to acquire S3 client for Zarr with asset ID {asset_id}")]
    ZarrToS3Error {
        asset_id: String,
        source: ZarrToS3Error,
    },
    #[error(transparent)]
    S3(#[from] S3Error),
}

#[derive(Debug, Error)]
pub(crate) enum ZarrToS3Error {
    #[error("Zarr does not have an S3 download URL")]
    ZarrLacksS3Url,
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
        .extend(segments)
        // Add an empty segment so that the final URL will end with a slash:
        .push("");
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
        assert_eq!(u.as_str(), "https://api.github.com/foo/");
        let u = urljoin(&base, ["foo", "bar"]);
        assert_eq!(u.as_str(), "https://api.github.com/foo/bar/");
    }

    #[rstest]
    #[case("https://api.github.com/foo/bar")]
    #[case("https://api.github.com/foo/bar/")]
    fn test_urljoin_path(#[case] base: Url) {
        let u = urljoin(&base, ["gnusto"]);
        assert_eq!(u.as_str(), "https://api.github.com/foo/bar/gnusto/");
        let u = urljoin(&base, ["gnusto", "cleesh"]);
        assert_eq!(u.as_str(), "https://api.github.com/foo/bar/gnusto/cleesh/");
    }

    #[rstest]
    #[case("foo#bar", "https://api.github.com/base/foo%23bar/")]
    #[case("foo%bar", "https://api.github.com/base/foo%25bar/")]
    #[case("foo/bar", "https://api.github.com/base/foo%2Fbar/")]
    #[case("foo?bar", "https://api.github.com/base/foo%3Fbar/")]
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
