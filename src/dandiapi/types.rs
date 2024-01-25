use super::{AssetPath, DandisetId, VersionId};
use serde::Deserialize;
use thiserror::Error;
use time::OffsetDateTime;
use url::Url;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub(super) struct Page<T> {
    pub(super) next: Option<Url>,
    pub(super) results: Vec<T>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub(crate) struct Dandiset {
    pub(crate) identifier: DandisetId,
    #[serde(with = "time::serde::rfc3339")]
    pub(crate) created: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub(crate) modified: OffsetDateTime,
    //contact_person: String,
    //embargo_status: ...,
    pub(crate) draft_version: Option<DandisetVersion>,
    pub(crate) most_recent_published_version: Option<DandisetVersion>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub(crate) struct DandisetVersion {
    pub(crate) version: VersionId,
    //name: String,
    //asset_count: u64,
    pub(crate) size: u64,
    //status: ...,
    #[serde(with = "time::serde::rfc3339")]
    pub(crate) created: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub(crate) modified: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct VersionMetadata(pub(super) Vec<u8>);

impl VersionMetadata {
    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }
}

// Item in a `/dandisets/{dandiset_id}/versions/{version_id}/assets/paths`
// response
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(from = "RawFolderEntry")]
pub(crate) enum FolderEntry {
    Folder(AssetFolder),
    Asset { path: AssetPath, id: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AssetFolder {
    Root,
    Path(AssetPath),
}

impl From<RawFolderEntry> for FolderEntry {
    fn from(entry: RawFolderEntry) -> FolderEntry {
        if let Some(asset) = entry.asset {
            FolderEntry::Asset {
                path: entry.path,
                id: asset.asset_id,
            }
        } else {
            FolderEntry::Folder(AssetFolder::Path(entry.path))
        }
    }
}

// Raw item in a `/dandisets/{dandiset_id}/versions/{version_id}/assets/paths`
// response
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
struct RawFolderEntry {
    path: AssetPath,
    asset: Option<RawFolderEntryAsset>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
struct RawFolderEntryAsset {
    asset_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AtAssetPath {
    Folder(AssetFolder),
    Asset(Asset),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(try_from = "RawAsset")]
pub(crate) enum Asset {
    Blob(BlobAsset),
    Zarr(ZarrAsset),
}

impl Asset {
    pub(crate) fn path(&self) -> &AssetPath {
        match self {
            Asset::Blob(a) => &a.path,
            Asset::Zarr(a) => &a.path,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BlobAsset {
    pub(crate) asset_id: String,
    pub(crate) blob_id: String,
    pub(crate) path: AssetPath,
    pub(crate) size: u64,
    pub(crate) created: OffsetDateTime,
    pub(crate) modified: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ZarrAsset {
    pub(crate) asset_id: String,
    pub(crate) zarr_id: String,
    pub(crate) path: AssetPath,
    pub(crate) size: u64,
    pub(crate) created: OffsetDateTime,
    pub(crate) modified: OffsetDateTime,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
struct RawAsset {
    asset_id: String,
    blob: Option<String>,
    zarr: Option<String>,
    path: AssetPath,
    size: u64,
    #[serde(with = "time::serde::rfc3339")]
    created: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    modified: OffsetDateTime,
}

impl TryFrom<RawAsset> for Asset {
    type Error = AssetTypeError;

    fn try_from(value: RawAsset) -> Result<Asset, AssetTypeError> {
        match (value.blob, value.zarr) {
            (Some(blob_id), None) => Ok(Asset::Blob(BlobAsset {
                asset_id: value.asset_id,
                blob_id,
                path: value.path,
                size: value.size,
                created: value.created,
                modified: value.modified,
            })),
            (None, Some(zarr_id)) => Ok(Asset::Zarr(ZarrAsset {
                asset_id: value.asset_id,
                zarr_id,
                path: value.path,
                size: value.size,
                created: value.created,
                modified: value.modified,
            })),
            (None, None) => Err(AssetTypeError::Neither {
                asset_id: value.asset_id,
            }),
            (Some(_), Some(_)) => Err(AssetTypeError::Both {
                asset_id: value.asset_id,
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
