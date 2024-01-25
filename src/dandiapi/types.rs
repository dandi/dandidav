use super::{AssetPath, DandisetId, VersionId};
use serde::Deserialize;
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
