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
        PureDirPath::try_from(format!(
            "zarrs/{}{}/{}.zarr/",
            self.prefix, self.zarr_id, self.checksum
        ))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_path_to_urls() {
        let mp = ManifestPath {
            prefix: "128/4a1/".parse().unwrap(),
            zarr_id: "1284a14f-fe4f-4dc3-b10d-48e5db8bf18d".parse().unwrap(),
            checksum: "6ddc4625befef8d6f9796835648162be-509--710206390"
                .parse()
                .unwrap(),
        };
        assert_eq!(mp.to_web_path(), "zarrs/128/4a1/1284a14f-fe4f-4dc3-b10d-48e5db8bf18d/6ddc4625befef8d6f9796835648162be-509--710206390.zarr/");
        assert_eq!(mp.urljoin(&"https://datasets.datalad.org/dandi/zarr-manifests/zarr-manifests-v2-sorted/".parse().unwrap()).as_str(), "https://datasets.datalad.org/dandi/zarr-manifests/zarr-manifests-v2-sorted/128/4a1/1284a14f-fe4f-4dc3-b10d-48e5db8bf18d/6ddc4625befef8d6f9796835648162be-509--710206390.json");
    }
}
