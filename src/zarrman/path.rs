use super::resources::ManifestPath;
use crate::paths::{PureDirPath, PurePath};

/// A parsed representation of a path under the `/zarrs/` hierarchy
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum ReqPath {
    /// A directory path between the manifest root and the Zarr manifests.  The
    /// path will have one of the following formats:
    ///
    /// - `{prefix1}/`
    /// - `{prefix1}/{prefix2}/`
    /// - `{prefix1}/{prefix2}/{zarr_id}/`
    Dir(PureDirPath),

    /// A path to a manifest file
    Manifest(ManifestPath),

    /// A path beneath a manifest file, i.e., inside a Zarr
    InManifest {
        /// The path to the manifest file
        manifest_path: ManifestPath,
        /// The portion of the path within the Zarr
        entry_path: PurePath,
    },
}

impl ReqPath {
    /// Parse a path (sans leading `zarrs/`) to a resource in the `/zarrs/`
    /// hierarchy into a `ReqPath`.  Returns `None` if the path is invalid.
    pub(super) fn parse_path(path: &PurePath) -> Option<ReqPath> {
        let mut components = path.components();
        let Some(c1) = components.next() else {
            unreachable!("path should have at least one component");
        };
        let mut prefix = PureDirPath::from(c1);
        let Some(c2) = components.next() else {
            return Some(ReqPath::Dir(prefix));
        };
        prefix.push(&c2);
        let Some(zarr_id) = components.next() else {
            return Some(ReqPath::Dir(prefix));
        };
        let Some(checksum) = components.next() else {
            prefix.push(&zarr_id);
            return Some(ReqPath::Dir(prefix));
        };
        let checksum = checksum.strip_suffix(".zarr")?;
        if checksum.contains('.') {
            return None;
        }
        let manifest_path = ManifestPath {
            prefix,
            zarr_id,
            checksum,
        };
        let Some(e1) = components.next() else {
            return Some(ReqPath::Manifest(manifest_path));
        };
        let mut entry_path = PurePath::from(e1);
        for e in components {
            entry_path.push(&e);
        }
        Some(ReqPath::InManifest {
            manifest_path,
            entry_path,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use rstest::rstest;

    #[rstest]
    #[case("128", "128/")]
    #[case("128/4a1", "128/4a1/")]
    #[case(
        "128/4a1/1284a14f-fe4f-4dc3-b10d-48e5db8bf18d",
        "128/4a1/1284a14f-fe4f-4dc3-b10d-48e5db8bf18d/"
    )]
    fn test_parse_dir(#[case] inpath: PurePath, #[case] outpath: PureDirPath) {
        assert_matches!(ReqPath::parse_path(&inpath), Some(ReqPath::Dir(p)) => {
            assert_eq!(p, outpath);
        });
    }

    #[test]
    fn test_parse_manifest() {
        let path = "128/4a1/1284a14f-fe4f-4dc3-b10d-48e5db8bf18d/6ddc4625befef8d6f9796835648162be-509--710206390.zarr".parse::<PurePath>().unwrap();
        assert_matches!(
            ReqPath::parse_path(&path),
            Some(ReqPath::Manifest(ManifestPath {prefix, zarr_id, checksum})) => {
                assert_eq!(prefix, "128/4a1/");
                assert_eq!(zarr_id, "1284a14f-fe4f-4dc3-b10d-48e5db8bf18d");
                assert_eq!(checksum, "6ddc4625befef8d6f9796835648162be-509--710206390");
            }
        );
    }

    #[rstest]
    #[case(".zarray")]
    #[case("0")]
    #[case("0/0/0")]
    fn test_parse_in_manifest(#[case] part2: PurePath) {
        let part1 = "128/4a1/1284a14f-fe4f-4dc3-b10d-48e5db8bf18d/6ddc4625befef8d6f9796835648162be-509--710206390.zarr".parse::<PurePath>().unwrap();
        let path = part1.to_dir_path().join(&part2);
        assert_matches!(
            ReqPath::parse_path(&path),
            Some(ReqPath::InManifest {
                manifest_path: ManifestPath { prefix, zarr_id, checksum },
                entry_path
            }) => {
                assert_eq!(prefix, "128/4a1/");
                assert_eq!(zarr_id, "1284a14f-fe4f-4dc3-b10d-48e5db8bf18d");
                assert_eq!(checksum, "6ddc4625befef8d6f9796835648162be-509--710206390");
                assert_eq!(entry_path, part2);
            }
        );
    }

    #[rstest]
    #[case("128/4a1/1284a14f-fe4f-4dc3-b10d-48e5db8bf18d/6ddc4625befef8d6f9796835648162be-509--710206390")]
    #[case("128/4a1/1284a14f-fe4f-4dc3-b10d-48e5db8bf18d/6ddc4625befef8d6f9796835648162be-509--710206390.json")]
    #[case("128/4a1/1284a14f-fe4f-4dc3-b10d-48e5db8bf18d/6ddc4625befef8d6f9796835648162be-509--710206390.versionid")]
    #[case("128/4a1/1284a14f-fe4f-4dc3-b10d-48e5db8bf18d/6ddc4625befef8d6f9796835648162be-509--710206390.versionid/0")]
    fn test_reject_bad_ext(#[case] path: PurePath) {
        assert_eq!(ReqPath::parse_path(&path), None);
    }
}
