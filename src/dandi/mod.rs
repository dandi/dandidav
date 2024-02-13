mod dandiset_id;
mod types;
mod version_id;
pub(crate) use self::dandiset_id::*;
pub(crate) use self::types::*;
pub(crate) use self::version_id::*;
use crate::consts::S3CLIENT_CACHE_SIZE;
use crate::httputil::{urljoin_slashed, BuildClientError, Client, HttpError};
use crate::paths::{ParsePureDirPathError, PureDirPath, PurePath};
use crate::s3::{
    BucketSpec, GetBucketRegionError, PrefixedS3Client, S3Client, S3Error, S3Location,
};
use async_stream::try_stream;
use futures_util::{Stream, TryStreamExt};
use moka::future::{Cache, CacheBuilder};
use serde::de::DeserializeOwned;
use smartstring::alias::CompactString;
use std::sync::Arc;
use thiserror::Error;
use url::Url;

#[derive(Clone, Debug)]
pub(crate) struct DandiClient {
    inner: Client,
    api_url: Url,
    s3clients: Cache<BucketSpec, Arc<S3Client>>,
}

impl DandiClient {
    pub(crate) fn new(api_url: Url) -> Result<Self, BuildClientError> {
        let inner = Client::new()?;
        let s3clients = CacheBuilder::new(S3CLIENT_CACHE_SIZE)
            .name("s3clients")
            .build();
        Ok(DandiClient {
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
        urljoin_slashed(&self.api_url, segments)
    }

    async fn get<T: DeserializeOwned>(&self, url: Url) -> Result<T, DandiError> {
        self.inner.get_json(url).await.map_err(Into::into)
    }

    fn paginate<T: DeserializeOwned + 'static>(
        &self,
        url: Url,
    ) -> impl Stream<Item = Result<T, DandiError>> + '_ {
        try_stream! {
            let mut url = Some(url);
            while let Some(u) = url {
                let page = self.inner.get_json::<Page<T>>(u).await?;
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
        let prefix = PureDirPath::try_from(key).map_err(ZarrToS3Error::BadS3Key)?;
        // Box large future:
        match Box::pin(self.s3clients.try_get_with_by_ref(&bucket_spec, async {
            bucket_spec.clone().into_s3client().await.map(Arc::new)
        }))
        .await
        {
            Ok(client) => Ok(client.with_prefix(prefix)),
            Err(source) => Err(ZarrToS3Error::LocateBucket {
                bucket: bucket_spec.bucket,
                source,
            }),
        }
    }

    async fn get_s3client_for_zarr(
        &self,
        zarr: &ZarrAsset,
    ) -> Result<PrefixedS3Client, DandiError> {
        let Some(s3loc) = zarr.s3location() else {
            return Err(DandiError::ZarrToS3Error {
                asset_id: zarr.asset_id.clone(),
                source: ZarrToS3Error::ZarrLacksS3Url,
            });
        };
        self.get_s3client(s3loc)
            .await
            .map_err(|source| DandiError::ZarrToS3Error {
                asset_id: zarr.asset_id.clone(),
                source,
            })
    }

    pub(crate) fn get_all_dandisets(
        &self,
    ) -> impl Stream<Item = Result<Dandiset, DandiError>> + '_ {
        self.paginate(self.get_url(["dandisets"]))
    }

    pub(crate) fn dandiset(&self, dandiset_id: DandisetId) -> DandisetEndpoint<'_> {
        DandisetEndpoint::new(self, dandiset_id)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DandisetEndpoint<'a> {
    client: &'a DandiClient,
    dandiset_id: DandisetId,
}

impl<'a> DandisetEndpoint<'a> {
    fn new(client: &'a DandiClient, dandiset_id: DandisetId) -> Self {
        Self {
            client,
            dandiset_id,
        }
    }

    pub(crate) fn version(self, version_id: VersionId) -> VersionEndpoint<'a> {
        VersionEndpoint::new(self, version_id)
    }

    pub(crate) async fn get(&self) -> Result<Dandiset, DandiError> {
        self.client
            .get(
                self.client
                    .get_url(["dandisets", self.dandiset_id.as_ref()]),
            )
            .await
    }

    pub(crate) fn get_all_versions(
        &self,
    ) -> impl Stream<Item = Result<DandisetVersion, DandiError>> + '_ {
        self.client.paginate(self.client.get_url([
            "dandisets",
            self.dandiset_id.as_ref(),
            "versions",
        ]))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct VersionEndpoint<'a> {
    client: &'a DandiClient,
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

    pub(crate) async fn get(&self) -> Result<DandisetVersion, DandiError> {
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

    pub(crate) async fn get_metadata(&self) -> Result<VersionMetadata, DandiError> {
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

    async fn get_asset_by_id(&self, id: &str) -> Result<Asset, DandiError> {
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
    ) -> impl Stream<Item = Result<DandiResource, DandiError>> + '_ {
        try_stream! {
            let stream = self.get_entries_under_path(None);
            tokio::pin!(stream);
            while let Some(entry) = stream.try_next().await? {
                match entry {
                    FolderEntry::Folder(subf) => yield DandiResource::Folder(subf),
                    FolderEntry::Asset { id, path } => match self.get_asset_by_id(&id).await {
                        Ok(asset) => yield DandiResource::Asset(asset),
                        Err(DandiError::Http(HttpError::NotFound { .. })) => {
                            Err(DandiError::DisappearingAsset { asset_id: id, path })?;
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
    ) -> impl Stream<Item = Result<FolderEntry, DandiError>> + '_ {
        self.get_entries_under_path(Some(&path.path))
    }

    fn get_entries_under_path(
        &self,
        path: Option<&PureDirPath>,
    ) -> impl Stream<Item = Result<FolderEntry, DandiError>> + '_ {
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

    async fn get_path(&self, path: &PurePath) -> Result<AtAssetPath, DandiError> {
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
            } else if asset.path().as_ref() > dirpath.as_ref() {
                break;
            }
        }
        Err(DandiError::PathNotFound { path: path.clone() })
    }

    async fn get_resource_with_s3(
        &self,
        path: &PurePath,
    ) -> Result<DandiResourceWithS3, DandiError> {
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
                    return Err(DandiError::PathUnderBlob {
                        path: path.clone(),
                        blob_path: zarr_path,
                    });
                }
                AtAssetPath::Asset(Asset::Zarr(zarr)) => {
                    let s3 = self.client.get_s3client_for_zarr(&zarr).await?;
                    return match s3.get_path(&entry_path).await? {
                        Some(entry) => Ok(zarr.make_resource(entry).with_s3(s3)),
                        None => Err(DandiError::ZarrEntryNotFound {
                            zarr_path,
                            entry_path,
                        }),
                    };
                }
            }
        }
        self.get_path(path).await.map(Into::into)
    }

    pub(crate) async fn get_resource(&self, path: &PurePath) -> Result<DandiResource, DandiError> {
        self.get_resource_with_s3(path).await.map(Into::into)
    }

    pub(crate) async fn get_resource_with_children(
        &self,
        path: &PurePath,
    ) -> Result<DandiResourceWithChildren, DandiError> {
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
                            Err(DandiError::Http(HttpError::NotFound { .. })) => {
                                return Err(DandiError::DisappearingAsset { asset_id: id, path })
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
                        children.push(zarr.make_resource(child));
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
                        children.push(folder.make_resource(child));
                    }
                }
                Ok(DandiResourceWithChildren::ZarrFolder { folder, children })
            }
            DandiResourceWithS3::ZarrEntry(r) => Ok(DandiResourceWithChildren::ZarrEntry(r)),
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum DandiError {
    #[error(transparent)]
    Http(#[from] HttpError),
    #[error("path {path:?} not found in assets")]
    PathNotFound { path: PurePath },
    #[error("path {path:?} points nowhere as leading portion {blob_path:?} points to a blob")]
    PathUnderBlob { path: PurePath, blob_path: PurePath },
    #[error("entry {entry_path:?} in Zarr {zarr_path:?} not found")]
    ZarrEntryNotFound {
        zarr_path: PurePath,
        entry_path: PurePath,
    },
    #[error("folder listing included asset ID={asset_id} at path {path:?}, but request to asset returned 404")]
    DisappearingAsset { asset_id: String, path: PurePath },
    #[error("failed to acquire S3 client for Zarr with asset ID {asset_id}")]
    ZarrToS3Error {
        asset_id: String,
        source: ZarrToS3Error,
    },
    #[error(transparent)]
    S3(#[from] S3Error),
}

impl DandiError {
    pub(crate) fn is_404(&self) -> bool {
        matches!(
            self,
            DandiError::Http(HttpError::NotFound { .. })
                | DandiError::PathNotFound { .. }
                | DandiError::PathUnderBlob { .. }
                | DandiError::ZarrEntryNotFound { .. }
        )
    }
}

#[derive(Debug, Error)]
pub(crate) enum ZarrToS3Error {
    #[error("Zarr does not have an S3 download URL")]
    ZarrLacksS3Url,
    #[error("key in S3 URL is not a well-formed path")]
    BadS3Key(#[source] crate::validstr::TryFromStringError<ParsePureDirPathError>),
    #[error("failed to determine region for S3 bucket {bucket:?}")]
    LocateBucket {
        bucket: CompactString,
        source: Arc<GetBucketRegionError>,
    },
}

fn dump_json_as_yaml(data: serde_json::Value) -> String {
    serde_yaml::to_string(&data).expect("converting JSON to YAML should not fail")
}

#[cfg(test)]
mod tests {
    use super::*;
    use indoc::indoc;
    use serde_json::json;

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
