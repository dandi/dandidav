use crate::httputil::urljoin;
use crate::paths::{Component, PureDirPath, PurePath};
use std::borrow::Cow;
use std::fmt;
use time::OffsetDateTime;
use url::Url;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ZarrManResource {
    WebFolder(WebFolder),
    Manifest(Manifest),
    ManFolder(ManifestFolder),
    ManEntry(ManifestEntry),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WebFolder {
    pub(crate) web_path: PureDirPath,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Manifest {
    pub(crate) path: ManifestPath,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ManifestPath {
    pub(super) prefix: PureDirPath,
    pub(super) zarr_id: Component,
    pub(super) checksum: Component,
}

impl ManifestPath {
    pub(super) fn zarr_id(&self) -> &str {
        self.zarr_id.as_ref()
    }

    pub(crate) fn to_web_path(&self) -> PureDirPath {
        format!("zarrs/{}{}/{}/", self.prefix, self.zarr_id, self.checksum)
            .parse::<PureDirPath>()
            .expect("ManifestPath should have valid web_path")
    }

    pub(crate) fn urljoin(&self, url: &Url) -> Url {
        urljoin(
            url,
            self.prefix
                .component_strs()
                .map(Cow::from)
                .chain(std::iter::once(Cow::from(&*self.zarr_id)))
                .chain(std::iter::once(Cow::from(format!(
                    "{}.json",
                    self.checksum
                )))),
        )
    }
}

impl fmt::Display for ManifestPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            r#""{}/{}/{}/""#,
            self.prefix.escape_debug(),
            self.zarr_id.escape_debug(),
            self.checksum.escape_debug()
        )
    }
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
    pub(crate) web_path: PureDirPath,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ManifestEntry {
    pub(crate) web_path: PurePath,
    pub(crate) size: i64,
    pub(crate) modified: OffsetDateTime,
    pub(crate) etag: String,
    pub(crate) url: Url,
}
