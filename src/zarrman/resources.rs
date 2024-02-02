use crate::paths::{PureDirPath, PurePath};
use time::OffsetDateTime;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ZarrManResource {
    WebFolder(WebFolder),
    Manifest(Manifest),
    ManFolder(ManifestFolder),
    ManEntry(ManifestEntry),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WebFolder {
    web_path: PureDirPath,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Manifest {
    web_path: PureDirPath,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ZarrManResourceWithChildren {
    WebFolder {
        folder: WebFolder,
        children: Vec<ZarrManResource>,
    },
    Manifest {
        folder: Manifest,
        children: Vec<ZarrManResource>,
    },
    ManFolder {
        folder: ManifestFolder,
        children: Vec<ZarrManResource>,
    },
    ManEntry(ManifestEntry),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ManifestFolder {
    //pub(crate) manifest_path: PurePath,
    //pub(crate) path: PureDirPath,
    pub(crate) web_path: PureDirPath,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ManifestEntry {
    //pub(crate) manifest_path: PurePath,
    //pub(crate) path: PurePath,
    pub(crate) web_path: PurePath,
    //pub(crate) version_id: String,
    pub(crate) size: i64,
    pub(crate) modified: OffsetDateTime,
    pub(crate) etag: String,
    pub(crate) url: url::Url,
}
