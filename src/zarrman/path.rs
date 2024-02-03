use super::resources::ManifestPath;
use crate::paths::{PureDirPath, PurePath};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum ReqPath {
    Dir(PureDirPath),
    Manifest(ManifestPath),
    InManifest {
        manifest_path: ManifestPath,
        entry_path: PurePath,
    },
}

impl ReqPath {
    pub(super) fn parse_path(_path: &PurePath) -> Option<ReqPath> {
        todo!()
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
        let path = "128/4a1/1284a14f-fe4f-4dc3-b10d-48e5db8bf18d/6ddc4625befef8d6f9796835648162be-509--710206390".parse::<PurePath>().unwrap();
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
        let part1 = "128/4a1/1284a14f-fe4f-4dc3-b10d-48e5db8bf18d/6ddc4625befef8d6f9796835648162be-509--710206390".parse::<PurePath>().unwrap();
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
    #[case("128/4a1/1284a14f-fe4f-4dc3-b10d-48e5db8bf18d/6ddc4625befef8d6f9796835648162be-509--710206390.versionid")]
    #[case("128/4a1/1284a14f-fe4f-4dc3-b10d-48e5db8bf18d/6ddc4625befef8d6f9796835648162be-509--710206390.versionid/0")]
    fn test_reject_versionid(#[case] path: PurePath) {
        assert_eq!(ReqPath::parse_path(&path), None);
    }
}
