use super::dandiset_id::DandisetId;
use super::version_id::VersionId;
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
