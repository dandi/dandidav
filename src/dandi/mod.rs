//! The implementation of the data source for the `/dandisets/` hierarchy
mod dandiset_id;
mod streams;
mod types;
mod version_id;
pub(crate) use self::dandiset_id::*;
use self::streams::Paginate;
pub(crate) use self::types::*;
pub(crate) use self::version_id::*;
use crate::consts::S3CLIENT_CACHE_SIZE;
use crate::dav::ErrorClass;
use crate::httputil::{BuildClientError, Client, HttpError, HttpUrl};
use crate::paths::{ParsePureDirPathError, PureDirPath, PurePath};
use crate::s3::{
    BucketSpec, GetBucketRegionError, PrefixedS3Client, S3Client, S3Error, S3Location,
};
use futures_util::{Stream, TryStreamExt};
use moka::future::{Cache, CacheBuilder};
use serde::de::DeserializeOwned;
use smartstring::alias::CompactString;
use std::sync::Arc;
use thiserror::Error;

/// A client for fetching data about Dandisets, their versions, and their
/// assets from a DANDI Archive instance
#[derive(Clone, Debug)]
pub(crate) struct DandiClient {
    /// The HTTP client used for making requests to the Archive instance's API
    inner: Client,

    /// The base API URL of the Archive instance
    api_url: HttpUrl,

    /// A cache of [`S3Client`] instances that are used for listing Zarr
    /// entries on the Archive's S3 bucket.
    ///
    /// In order to avoid the user running `dandidav` having to supply details
    /// on the Archive instance's S3 bucket, these details are instead derived
    /// automatically from the `contentUrl` fields of Zarr assets' metadata
    /// once they are needed.  Each bucket needs an `S3Client` to access it,
    /// and as construction of the inner `aws_sdk_s3::Client` is expensive, we
    /// cache them.
    s3clients: Cache<BucketSpec, Arc<S3Client>>,
}

impl DandiClient {
    /// Construct a new `DandiClient` for the Archive instance with the given
    /// base API URL
    ///
    /// # Errors
    ///
    /// Returns an error if construction of the inner `reqwest::Client` fails
    pub(crate) fn new(api_url: HttpUrl) -> Result<Self, BuildClientError> {
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

    /// Return the URL formed by appending the given path segments and a
    /// trailing slash to the path of the API base URL
    fn get_url<I>(&self, segments: I) -> HttpUrl
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        let mut url = self.api_url.clone();
        url.extend(segments).ensure_dirpath();
        url
    }

    /// Perform a `GET` request to the given URL and return the deserialized
    /// JSON response body
    async fn get<T: DeserializeOwned>(&self, url: HttpUrl) -> Result<T, DandiError> {
        self.inner.get_json(url).await.map_err(Into::into)
    }

    /// Return a [`futures_util::Stream`] that makes paginated `GET` requests
    /// to the given URL and its subsequent pages and yields a `Result<T,
    /// DandiError>` value for each item deserialized from the responses
    fn paginate<T: DeserializeOwned + 'static>(&self, url: HttpUrl) -> Paginate<T> {
        Paginate::new(self, url)
    }

    /// Given a Zarr asset, return a [`PrefixedS3Client`] for fetching
    /// information from S3 about the keys under the Zarr's key prefix on its
    /// bucket.  If a client has not already been constructed for the bucket in
    /// question, one is constructed & cached.
    ///
    /// Specifically, the first `contentUrl` of the Zarr that can be parsed by
    /// [`S3Location::parse_url()`] into a bucket, optional region, and key
    /// prefix is used to construct the `PrefixedS3Client` (with a trailing
    /// slash appended to the key prefix if one isn't already present), with
    /// the assumption that the Zarr's entries are laid out under the given key
    /// prefix on the given bucket using the same names & directory structure
    /// as the actual Zarr.
    async fn get_s3client_for_zarr(
        &self,
        zarr: &ZarrAsset,
    ) -> Result<PrefixedS3Client, DandiError> {
        let Some(S3Location {
            bucket_spec,
            mut key,
        }) = zarr.s3location()
        else {
            return Err(DandiError::ZarrToS3Error {
                asset_id: zarr.asset_id.clone(),
                source: ZarrToS3Error::NoS3Url,
            });
        };
        if !key.ends_with('/') {
            key.push('/');
        }
        let prefix = PureDirPath::try_from(key).map_err(|source| DandiError::ZarrToS3Error {
            asset_id: zarr.asset_id.clone(),
            source: ZarrToS3Error::BadS3Key(source),
        })?;
        match self
            .s3clients
            .try_get_with_by_ref(
                &bucket_spec,
                // Box the future passed to moka in order to minimize the size
                // of the moka future (cf.
                // <https://github.com/moka-rs/moka/issues/212>):
                Box::pin(async { bucket_spec.clone().into_s3client().await.map(Arc::new) }),
            )
            .await
        {
            Ok(client) => Ok(client.with_prefix(prefix)),
            Err(source) => Err(DandiError::ZarrToS3Error {
                asset_id: zarr.asset_id.clone(),
                source: ZarrToS3Error::LocateBucket {
                    bucket: bucket_spec.bucket,
                    source,
                },
            }),
        }
    }

    /// Return a [`futures_util::Stream`] that yields a `Dandiset` for each
    /// Dandiset on the Archive instance
    pub(crate) fn get_all_dandisets(
        &self,
    ) -> impl Stream<Item = Result<Dandiset, DandiError>> + '_ {
        self.paginate::<RawDandiset>(self.get_url(["dandisets"]))
            .map_ok(|ds| ds.with_metadata_urls(self))
    }

    /// Return an endpoint object for making requests for information about the
    /// given Dandiset
    pub(crate) fn dandiset(&self, dandiset_id: DandisetId) -> DandisetEndpoint<'_> {
        DandisetEndpoint::new(self, dandiset_id)
    }

    /// Return the URL for the metadata for the given version of the given
    /// Dandiset
    fn version_metadata_url(&self, dandiset_id: &DandisetId, version_id: &VersionId) -> HttpUrl {
        self.get_url([
            "dandisets",
            dandiset_id.as_ref(),
            "versions",
            version_id.as_ref(),
        ])
    }
}

/// An object for making requests relating to a specific Dandiset
#[derive(Clone, Debug)]
pub(crate) struct DandisetEndpoint<'a> {
    /// Client for the Dandiset's Archive instance
    client: &'a DandiClient,

    /// The ID of the Dandiset this instance operates on
    dandiset_id: DandisetId,
}

impl<'a> DandisetEndpoint<'a> {
    /// Construct a `DandisetEndpoint` for the given `client` and `dandiset_id`
    fn new(client: &'a DandiClient, dandiset_id: DandisetId) -> Self {
        Self {
            client,
            dandiset_id,
        }
    }

    /// Return an endpoint object for making requests for information about the
    /// given version of the Dandiset
    pub(crate) fn version(self, version_id: VersionId) -> VersionEndpoint<'a> {
        VersionEndpoint::new(self, version_id)
    }

    /// Retrieve information about the Dandiset
    pub(crate) async fn get(&self) -> Result<Dandiset, DandiError> {
        self.client
            .get::<RawDandiset>(
                self.client
                    .get_url(["dandisets", self.dandiset_id.as_ref()]),
            )
            .await
            .map(|ds| ds.with_metadata_urls(self.client))
    }

    /// Return a [`futures_util::Stream`] that yields a `DandisetVersion` for
    /// each version of the Dandiset
    pub(crate) fn get_all_versions(
        &self,
    ) -> impl Stream<Item = Result<DandisetVersion, DandiError>> + '_ {
        self.client
            .paginate::<RawDandisetVersion>(self.client.get_url([
                "dandisets",
                self.dandiset_id.as_ref(),
                "versions",
            ]))
            .map_ok(|v| {
                let url = self
                    .client
                    .version_metadata_url(&self.dandiset_id, &v.version);
                v.with_metadata_url(url)
            })
    }
}

/// An object for making requests relating to a specific version of a Dandiset
#[derive(Clone, Debug)]
pub(crate) struct VersionEndpoint<'a> {
    /// Client for the Archive instance
    client: &'a DandiClient,

    /// The ID of the Dandiset this instance operates on
    dandiset_id: DandisetId,

    /// The ID of the version this instance operates on
    version_id: VersionId,
}

impl<'a> VersionEndpoint<'a> {
    /// Construct a `VersionEndpoint` from a `DandisetEndpoint` and `VersionId`
    fn new(upper: DandisetEndpoint<'a>, version_id: VersionId) -> Self {
        Self {
            client: upper.client,
            dandiset_id: upper.dandiset_id,
            version_id,
        }
    }

    /// Retrieve information about the version
    pub(crate) async fn get(&self) -> Result<VersionInfo, DandiError> {
        self.client
            .get::<RawVersionInfo>(self.client.get_url([
                "dandisets",
                self.dandiset_id.as_ref(),
                "versions",
                self.version_id.as_ref(),
                "info",
            ]))
            .await
            .map(|v| v.with_metadata_url(self.metadata_url()))
    }

    /// Retrieve the version's metadata as serialized YAML
    pub(crate) async fn get_metadata(&self) -> Result<VersionMetadata, DandiError> {
        self.client
            .get::<VersionMetadata>(self.metadata_url())
            .await
            .map_err(Into::into)
    }

    /// Get details on the resource at the given `path` in the version's file
    /// hierarchy, treating Zarrs as directories of their entries
    ///
    /// Although `path` is a `PurePath`, the resulting resource may be a
    /// collection.
    pub(crate) async fn get_resource(&self, path: &PurePath) -> Result<DandiResource, DandiError> {
        self.get_resource_with_s3(path).await.map(Into::into)
    }

    /// Get details on the resource at the given `path` in the version's file
    /// hierarchy (treating Zarrs as directories of their entries) along with
    /// its immediate child resources (if any).
    ///
    /// Although `path` is a `PurePath`, the resulting resource may be a
    /// collection.
    pub(crate) async fn get_resource_with_children(
        &self,
        path: &PurePath,
    ) -> Result<DandiResourceWithChildren, DandiError> {
        match self.get_resource_with_s3(path).await? {
            DandiResourceWithS3::Folder(folder) => {
                let mut children = Vec::new();
                let mut stream = self.get_folder_entries(&folder);
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
                let children = s3
                    .get_root_entries()
                    .map_ok(|child| zarr.make_resource(child))
                    .try_collect::<Vec<_>>()
                    .await?;
                Ok(DandiResourceWithChildren::Zarr { zarr, children })
            }
            DandiResourceWithS3::ZarrFolder { folder, s3 } => {
                let children = s3
                    .get_folder_entries(&folder.path)
                    .map_ok(|child| folder.make_resource(child))
                    .try_collect::<Vec<_>>()
                    .await?;
                Ok(DandiResourceWithChildren::ZarrFolder { folder, children })
            }
            DandiResourceWithS3::ZarrEntry(r) => Ok(DandiResourceWithChildren::ZarrEntry(r)),
        }
    }

    /// Return a [`futures_util::Stream`] that yields the resources at the root
    /// of the version's file hierarchy
    pub(crate) fn get_root_children(
        &self,
    ) -> impl Stream<Item = Result<DandiResource, DandiError>> + '_ {
        self.get_entries_under_path(None)
            .and_then(move |entry| async move {
                match entry {
                    FolderEntry::Folder(subf) => Ok(DandiResource::Folder(subf)),
                    FolderEntry::Asset { id, path } => match self.get_asset_by_id(&id).await {
                        Ok(asset) => Ok(DandiResource::Asset(asset)),
                        Err(DandiError::Http(HttpError::NotFound { .. })) => {
                            Err(DandiError::DisappearingAsset { asset_id: id, path })
                        }
                        Err(e) => Err(e),
                    },
                }
            })
    }

    /// Get details on the resource at the given `path` in the version's file
    /// hierarchy, treating Zarrs as directories of their entries
    ///
    /// In order to determine whether `path` consists of a path to a Zarr asset
    /// followed by a path to a resource within that Zarr, we perform the
    /// following algorithm, which is efficient but not always correct (cf.
    /// <https://github.com/dandi/dandi-webdav/issues/5> and
    /// <https://github.com/dandi/dandidav/issues/10>).
    ///
    /// - For each non-final component in `path` from left to right that has a
    ///   `.zarr` or `.ngff` extension (case sensitive), query the asset path
    ///   up through that component.  If 404, return 404.  If blob asset,
    ///   return 404.  If folder, go to next candidate.  Otherwise, we have a
    ///   Zarr asset, and the rest of the original path is the Zarr entry path.
    ///
    /// - If all components are exhausted without erroring or finding a Zarr,
    ///   treat the entirety of `path` as an asset/folder path.
    async fn get_resource_with_s3(
        &self,
        path: &PurePath,
    ) -> Result<DandiResourceWithS3, DandiError> {
        for (zarr_path, entry_path) in path.split_zarr_candidates() {
            match self.get_path(&zarr_path).await? {
                AtAssetPath::Folder(_) => continue,
                AtAssetPath::Asset(Asset::Blob(_)) => {
                    return Err(DandiError::PathUnderBlob {
                        path: path.clone(),
                        blob_path: zarr_path,
                    })
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

    /// Return the URL for the version's metadata
    fn metadata_url(&self) -> HttpUrl {
        self.client
            .version_metadata_url(&self.dandiset_id, &self.version_id)
    }

    /// Retrieve information on the asset in this version with the given asset
    /// ID
    async fn get_asset_by_id(&self, id: &str) -> Result<Asset, DandiError> {
        self.client
            .get::<RawAsset>(self.client.get_url([
                "dandisets",
                self.dandiset_id.as_ref(),
                "versions",
                self.version_id.as_ref(),
                "assets",
                id,
                "info",
            ]))
            .await?
            .try_into_asset(self)
            .map_err(Into::into)
    }

    /// Return the URL for the metadata of the asset in this version with the
    /// given asset ID
    fn asset_metadata_url(&self, asset_id: &str) -> HttpUrl {
        self.client.get_url([
            "dandisets",
            self.dandiset_id.as_ref(),
            "versions",
            self.version_id.as_ref(),
            "assets",
            asset_id,
        ])
    }

    /// Get details on the resource (an asset or folder) at the given `path` in
    /// the version's file hierarchy, treating Zarrs as non-collections.
    ///
    /// This method paginates over all assets in the version whose paths start
    /// with `path`, sorted by asset paths in lexicographic order.  If an exact
    /// match is found, that asset is returned.  If an asset is found whose
    /// path is a descendant of `path`, then `path` is a folder.
    async fn get_path(&self, path: &PurePath) -> Result<AtAssetPath, DandiError> {
        let mut url = self.client.get_url([
            "dandisets",
            self.dandiset_id.as_ref(),
            "versions",
            self.version_id.as_ref(),
            "assets",
        ]);
        url.append_query_param("path", path.as_ref());
        url.append_query_param("metadata", "1");
        url.append_query_param("order", "path");
        let dirpath = path.to_dir_path();
        let mut stream = self.client.paginate::<RawAsset>(url.clone());
        while let Some(asset) = stream.try_next().await? {
            if &asset.path == path {
                return Ok(AtAssetPath::Asset(asset.try_into_asset(self)?));
            } else if asset.path.is_strictly_under(&dirpath) {
                return Ok(AtAssetPath::Folder(AssetFolder { path: dirpath }));
            } else if asset.path.as_ref() > dirpath.as_ref() {
                break;
            }
        }
        Err(DandiError::PathNotFound { path: path.clone() })
    }

    /// Return a [`futures_util::Stream`] that yields a [`FolderEntry`] object
    /// for each immediate child resource (both assets and folders) of the
    /// folder at `path` in the version's file hierarchy, treating Zarrs as
    /// non-collections.  If `path` is `None`, the resources at the root of the
    /// file hierarchy are yielded.
    fn get_entries_under_path(&self, path: Option<&PureDirPath>) -> Paginate<FolderEntry> {
        let mut url = self.client.get_url([
            "dandisets",
            self.dandiset_id.as_ref(),
            "versions",
            self.version_id.as_ref(),
            "assets",
            "paths",
        ]);
        if let Some(path) = path {
            url.append_query_param("path_prefix", path.as_ref());
        }
        self.client.paginate(url)
    }

    /// Return a [`futures_util::Stream`] that yields a [`FolderEntry`] object
    /// for each immediate child resource (both assets and folders) of the
    /// folder at `path` in the version's file hierarchy, treating Zarrs as
    /// non-collections.
    fn get_folder_entries(&self, path: &AssetFolder) -> Paginate<FolderEntry> {
        self.get_entries_under_path(Some(&path.path))
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
    #[error("folder listing included asset ID {asset_id} at path {path:?}, but request to asset returned 404")]
    DisappearingAsset { asset_id: String, path: PurePath },
    #[error("failed to acquire S3 client for Zarr with asset ID {asset_id}")]
    ZarrToS3Error {
        asset_id: String,
        source: ZarrToS3Error,
    },
    #[error(transparent)]
    AssetType(#[from] AssetTypeError),
    #[error(transparent)]
    S3(#[from] S3Error),
}

impl DandiError {
    /// Classify the general type of error
    pub(crate) fn class(&self) -> ErrorClass {
        match self {
            DandiError::Http(source) => source.class(),
            DandiError::PathNotFound { .. }
            | DandiError::PathUnderBlob { .. }
            | DandiError::ZarrEntryNotFound { .. } => ErrorClass::NotFound,
            DandiError::DisappearingAsset { .. } => ErrorClass::BadGateway,
            DandiError::ZarrToS3Error { source, .. } => source.class(),
            DandiError::AssetType(_) => ErrorClass::BadGateway,
            DandiError::S3(source) => source.class(),
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum ZarrToS3Error {
    #[error("Zarr does not have an S3 download URL")]
    NoS3Url,
    #[error("key in S3 URL is not a well-formed path")]
    BadS3Key(#[source] crate::validstr::TryFromStringError<ParsePureDirPathError>),
    #[error("failed to determine region for S3 bucket {bucket:?}")]
    LocateBucket {
        bucket: CompactString,
        source: Arc<GetBucketRegionError>,
    },
}

impl ZarrToS3Error {
    /// Classify the general type of error
    pub(crate) fn class(&self) -> ErrorClass {
        match self {
            ZarrToS3Error::NoS3Url => ErrorClass::BadGateway,
            ZarrToS3Error::BadS3Key(_) => ErrorClass::BadGateway,
            ZarrToS3Error::LocateBucket { source, .. } => {
                let class = source.class();
                if class == ErrorClass::NotFound {
                    // This only happens if the bucket does not exist, in which
                    // case the Archive lied to us about the Zarr's contentUrl,
                    // which is a problem with the Archive response and thus a
                    // Bad Gateway error.
                    ErrorClass::BadGateway
                } else {
                    class
                }
            }
        }
    }
}
