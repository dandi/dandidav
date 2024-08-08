use super::{DandisetId, VersionId};
use crate::httputil::HttpUrl;
use crate::paths::{PureDirPath, PurePath};
use crate::s3::{PrefixedS3Client, S3Entry, S3Folder, S3Location, S3Object};
use serde::Deserialize;
use thiserror::Error;
use time::OffsetDateTime;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub(super) struct RawDandiset {
    identifier: DandisetId,
    #[serde(with = "time::serde::rfc3339")]
    created: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    modified: OffsetDateTime,
    //contact_person: String,
    //embargo_status: ...,
    draft_version: RawDandisetVersion,
    most_recent_published_version: Option<RawDandisetVersion>,
}

impl RawDandiset {
    pub(super) fn with_metadata_urls(self, client: &super::DandiClient) -> Dandiset {
        let draft_version = self
            .draft_version
            .with_metadata_url(client.version_metadata_url(&self.identifier, &VersionId::Draft));
        let most_recent_published_version = self.most_recent_published_version.map(|v| {
            let url = client.version_metadata_url(&self.identifier, &v.version);
            v.with_metadata_url(url)
        });
        Dandiset {
            identifier: self.identifier,
            created: self.created,
            modified: self.modified,
            draft_version,
            most_recent_published_version,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Dandiset {
    pub(crate) identifier: DandisetId,
    pub(crate) created: OffsetDateTime,
    pub(crate) modified: OffsetDateTime,
    pub(crate) draft_version: DandisetVersion,
    pub(crate) most_recent_published_version: Option<DandisetVersion>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub(super) struct RawDandisetVersion {
    pub(super) version: VersionId,
    //name: String,
    //asset_count: u64,
    size: i64,
    //status: ...,
    #[serde(with = "time::serde::rfc3339")]
    created: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    modified: OffsetDateTime,
}

impl RawDandisetVersion {
    pub(super) fn with_metadata_url(self, metadata_url: HttpUrl) -> DandisetVersion {
        DandisetVersion {
            version: self.version,
            size: self.size,
            created: self.created,
            modified: self.modified,
            metadata_url,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DandisetVersion {
    pub(crate) version: VersionId,
    pub(crate) size: i64,
    pub(crate) created: OffsetDateTime,
    pub(crate) modified: OffsetDateTime,
    pub(crate) metadata_url: HttpUrl,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct VersionMetadata(pub(super) Vec<u8>);

impl VersionMetadata {
    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }
}

impl From<VersionMetadata> for Vec<u8> {
    fn from(value: VersionMetadata) -> Vec<u8> {
        value.0
    }
}

// Item in a `/dandisets/{dandiset_id}/versions/{version_id}/assets/paths/`
// response
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(from = "RawFolderEntry")]
pub(crate) enum FolderEntry {
    Folder(AssetFolder),
    Asset { path: PurePath, id: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AssetFolder {
    pub(crate) path: PureDirPath,
}

impl From<RawFolderEntry> for FolderEntry {
    fn from(entry: RawFolderEntry) -> FolderEntry {
        if let Some(asset) = entry.asset {
            FolderEntry::Asset {
                path: entry.path,
                id: asset.asset_id,
            }
        } else {
            FolderEntry::Folder(AssetFolder {
                path: entry.path.to_dir_path(),
            })
        }
    }
}

// Raw item in a `/dandisets/{dandiset_id}/versions/{version_id}/assets/paths/`
// response
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
struct RawFolderEntry {
    path: PurePath,
    asset: Option<RawFolderEntryAsset>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
struct RawFolderEntryAsset {
    asset_id: String,
}

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AtAssetPath {
    Folder(AssetFolder),
    Asset(Asset),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Asset {
    Blob(BlobAsset),
    Zarr(ZarrAsset),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BlobAsset {
    pub(crate) asset_id: String,
    pub(crate) blob_id: String,
    pub(crate) path: PurePath,
    pub(crate) size: i64,
    pub(crate) created: OffsetDateTime,
    pub(crate) modified: OffsetDateTime,
    pub(crate) metadata: AssetMetadata,
    pub(crate) metadata_url: HttpUrl,
}

impl BlobAsset {
    pub(crate) fn content_type(&self) -> Option<&str> {
        self.metadata.encoding_format.as_deref()
    }

    pub(crate) fn etag(&self) -> Option<&str> {
        self.metadata.digest.dandi_etag.as_deref()
    }

    pub(crate) fn archive_url(&self) -> Option<&HttpUrl> {
        self.metadata
            .content_url
            .iter()
            .find(|url| S3Location::parse_url(url.as_url()).is_err())
    }

    pub(crate) fn s3_url(&self) -> Option<&HttpUrl> {
        self.metadata
            .content_url
            .iter()
            .find(|url| S3Location::parse_url(url.as_url()).is_ok())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ZarrAsset {
    pub(crate) asset_id: String,
    pub(crate) zarr_id: String,
    pub(crate) path: PurePath,
    pub(crate) size: i64,
    pub(crate) created: OffsetDateTime,
    pub(crate) modified: OffsetDateTime,
    pub(crate) metadata: AssetMetadata,
    pub(crate) metadata_url: HttpUrl,
}

impl ZarrAsset {
    pub(crate) fn s3location(&self) -> Option<S3Location> {
        self.metadata
            .content_url
            .iter()
            .find_map(|url| S3Location::parse_url(url.as_url()).ok())
    }

    pub(crate) fn make_resource(&self, value: S3Entry) -> DandiResource {
        match value {
            S3Entry::Folder(folder) => DandiResource::ZarrFolder(self.make_folder(folder)),
            S3Entry::Object(obj) => DandiResource::ZarrEntry(self.make_entry(obj)),
        }
    }

    /// Return a `ZarrFolder` for the folder within this Zarr described by
    /// `folder`
    fn make_folder(&self, folder: S3Folder) -> ZarrFolder {
        ZarrFolder {
            zarr_path: self.path.clone(),
            path: folder.key_prefix,
        }
    }

    /// Return a `ZarrEntry` for the entry within this Zarr described by `obj`
    fn make_entry(&self, obj: S3Object) -> ZarrEntry {
        ZarrEntry {
            zarr_path: self.path.clone(),
            path: obj.key,
            size: obj.size,
            modified: obj.modified,
            etag: obj.etag,
            url: obj.download_url,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AssetMetadata {
    encoding_format: Option<String>,
    content_url: Vec<HttpUrl>,
    digest: AssetDigests,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub(crate) struct AssetDigests {
    #[serde(rename = "dandi:dandi-etag")]
    dandi_etag: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub(super) struct RawAsset {
    asset_id: String,
    blob: Option<String>,
    zarr: Option<String>,
    pub(super) path: PurePath,
    size: i64,
    #[serde(with = "time::serde::rfc3339")]
    created: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    modified: OffsetDateTime,
    metadata: AssetMetadata,
}

impl RawAsset {
    pub(super) fn try_into_asset(
        self,
        endpoint: &super::VersionEndpoint<'_>,
    ) -> Result<Asset, AssetTypeError> {
        let metadata_url = endpoint.asset_metadata_url(&self.asset_id);
        match (self.blob, self.zarr) {
            (Some(blob_id), None) => Ok(Asset::Blob(BlobAsset {
                asset_id: self.asset_id,
                blob_id,
                path: self.path,
                size: self.size,
                created: self.created,
                modified: self.modified,
                metadata: self.metadata,
                metadata_url,
            })),
            (None, Some(zarr_id)) => Ok(Asset::Zarr(ZarrAsset {
                asset_id: self.asset_id,
                zarr_id,
                path: self.path,
                size: self.size,
                created: self.created,
                modified: self.modified,
                metadata: self.metadata,
                metadata_url,
            })),
            (None, None) => Err(AssetTypeError::Neither {
                asset_id: self.asset_id,
            }),
            (Some(_), Some(_)) => Err(AssetTypeError::Both {
                asset_id: self.asset_id,
            }),
        }
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum AssetTypeError {
    #[error(r#"asset {asset_id} has neither "blob" nor "zarr" set"#)]
    Neither { asset_id: String },
    #[error(r#"asset {asset_id} has both "blob" and "zarr" set"#)]
    Both { asset_id: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DandiResource {
    Folder(AssetFolder),
    Asset(Asset),
    ZarrFolder(ZarrFolder),
    ZarrEntry(ZarrEntry),
}

impl DandiResource {
    pub(super) fn with_s3(self, s3: PrefixedS3Client) -> DandiResourceWithS3 {
        match self {
            DandiResource::Folder(r) => DandiResourceWithS3::Folder(r),
            DandiResource::Asset(r) => DandiResourceWithS3::Asset(r),
            DandiResource::ZarrFolder(folder) => DandiResourceWithS3::ZarrFolder { folder, s3 },
            DandiResource::ZarrEntry(r) => DandiResourceWithS3::ZarrEntry(r),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ZarrFolder {
    pub(crate) zarr_path: PurePath,
    pub(crate) path: PureDirPath,
}

impl ZarrFolder {
    pub(crate) fn make_resource(&self, value: S3Entry) -> DandiResource {
        match value {
            S3Entry::Folder(folder) => DandiResource::ZarrFolder(self.make_folder(folder)),
            S3Entry::Object(obj) => DandiResource::ZarrEntry(self.make_entry(obj)),
        }
    }

    pub(crate) fn make_folder(&self, folder: S3Folder) -> ZarrFolder {
        ZarrFolder {
            zarr_path: self.zarr_path.clone(),
            path: folder.key_prefix,
        }
    }

    pub(crate) fn make_entry(&self, obj: S3Object) -> ZarrEntry {
        ZarrEntry {
            zarr_path: self.zarr_path.clone(),
            path: obj.key,
            size: obj.size,
            modified: obj.modified,
            etag: obj.etag,
            url: obj.download_url,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ZarrEntry {
    pub(crate) zarr_path: PurePath,
    pub(crate) path: PurePath,
    pub(crate) size: i64,
    pub(crate) modified: OffsetDateTime,
    pub(crate) etag: String,
    pub(crate) url: HttpUrl,
}

#[derive(Clone, Debug)]
pub(super) enum DandiResourceWithS3 {
    Folder(AssetFolder),
    Asset(Asset),
    ZarrFolder {
        folder: ZarrFolder,
        s3: PrefixedS3Client,
    },
    ZarrEntry(ZarrEntry),
}

impl From<AtAssetPath> for DandiResourceWithS3 {
    fn from(value: AtAssetPath) -> DandiResourceWithS3 {
        match value {
            AtAssetPath::Folder(r) => DandiResourceWithS3::Folder(r),
            AtAssetPath::Asset(r) => DandiResourceWithS3::Asset(r),
        }
    }
}

impl From<DandiResourceWithS3> for DandiResource {
    fn from(value: DandiResourceWithS3) -> DandiResource {
        match value {
            DandiResourceWithS3::Folder(r) => DandiResource::Folder(r),
            DandiResourceWithS3::Asset(r) => DandiResource::Asset(r),
            DandiResourceWithS3::ZarrFolder { folder, .. } => DandiResource::ZarrFolder(folder),
            DandiResourceWithS3::ZarrEntry(r) => DandiResource::ZarrEntry(r),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DandiResourceWithChildren {
    Folder {
        folder: AssetFolder,
        children: Vec<DandiResource>,
    },
    Blob(BlobAsset),
    Zarr {
        zarr: ZarrAsset,
        children: Vec<DandiResource>,
    },
    ZarrFolder {
        folder: ZarrFolder,
        children: Vec<DandiResource>,
    },
    ZarrEntry(ZarrEntry),
}
