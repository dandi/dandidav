use crate::httputil::HttpUrl;
use crate::paths::{Component, PureDirPath, PurePath};
use std::fmt;
use time::OffsetDateTime;

/// A resource served under `dandidav`'s `/zarrs/` hierarchy, not including
/// information on child resources
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ZarrManResource {
    WebFolder(WebFolder),
    Manifest(Manifest),
    ManFolder(ManifestFolder),
    ManEntry(ManifestEntry),
}

/// A collection between the root of the `/zarrs/` hierarchy and the Zarr
/// manifests
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WebFolder {
    /// The path to the entry as served by `dandidav`, including the leading
    /// `zarrs/`.  The path will have one of the following formats:
    ///
    /// - `zarrs/{prefix1}/`
    /// - `zarrs/{prefix1}/{prefix2}/`
    /// - `zarrs/{prefix1}/{prefix2}/{zarr_id}/`
    pub(crate) web_path: PureDirPath,
}

/// A Zarr manifest, served as a virtual collection of the Zarr's entries
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Manifest {
    pub(crate) path: ManifestPath,
}

/// A path to a Zarr manifest in the manifest tree or a Zarr collection in the
/// `/zarrs/` hierarchy
#[derive(Clone, Eq, Hash, PartialEq)]
pub(crate) struct ManifestPath {
    /// The portion of the path between the manifest root and the Zarr ID, of
    /// the form `{prefix1}/{prefix2}/`
    pub(super) prefix: PureDirPath,

    /// The Zarr ID
    pub(super) zarr_id: Component,

    /// The Zarr's checksum
    pub(super) checksum: Component,
}

impl ManifestPath {
    /// Returns the Zarr ID
    pub(super) fn zarr_id(&self) -> &str {
        self.zarr_id.as_ref()
    }

    /// Returns the path to the Zarr as served by `dandidav`, in the form
    /// `zarrs/{prefix1}/{prefix2}/{zarr_id}/{checksum}.zarr/`.
    pub(crate) fn to_web_path(&self) -> PureDirPath {
        PureDirPath::try_from(format!(
            "zarrs/{}{}/{}.zarr/",
            self.prefix, self.zarr_id, self.checksum
        ))
        .expect("ManifestPath should have valid web_path")
    }

    /// Returns the URL of the Zarr manifest underneath the given manifest root
    pub(crate) fn under_manifest_root(&self, manifest_root_url: &HttpUrl) -> HttpUrl {
        let mut url = manifest_root_url.clone();
        url.extend(self.prefix.component_strs());
        url.push(&self.zarr_id);
        url.push(format!("{}.json", self.checksum));
        url
    }
}

impl fmt::Display for ManifestPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}/{}.json", self.prefix, self.zarr_id, self.checksum)
    }
}

impl fmt::Debug for ManifestPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.to_string())
    }
}

/// A resource served under `dandidav`'s `/zarrs/` hierarchy, including
/// information on child resources
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

/// A folder within a Zarr
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ManifestFolder {
    pub(crate) web_path: PureDirPath,
}

/// An entry within a Zarr
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ManifestEntry {
    /// The path to the entry as served by `dandidav`, i.e., a path of the form
    /// `zarrs/{p1}/{p2}/{zarr_id}/{checksum}.zarr/{entry_path}`
    pub(crate) web_path: PurePath,

    /// The size of the entry in bytes
    pub(crate) size: i64,

    /// The entry's S3 object's modification time
    pub(crate) modified: OffsetDateTime,

    /// The ETag of the entry's S3 object
    pub(crate) etag: String,

    /// The download URL for the entry
    pub(crate) url: HttpUrl,
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
        assert_eq!(mp.under_manifest_root(&"https://datasets.datalad.org/dandi/zarr-manifests/zarr-manifests-v2-sorted/".parse().unwrap()).as_str(), "https://datasets.datalad.org/dandi/zarr-manifests/zarr-manifests-v2-sorted/128/4a1/1284a14f-fe4f-4dc3-b10d-48e5db8bf18d/6ddc4625befef8d6f9796835648162be-509--710206390.json");
    }
}
