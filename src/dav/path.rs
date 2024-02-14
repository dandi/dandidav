use crate::consts::FAST_NOT_EXIST;
use crate::dandi::{DandisetId, PublishedVersionId};
use crate::paths::{Component, ParseComponentError, PurePath};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum DavPath {
    Root,
    DandisetIndex,
    Dandiset {
        dandiset_id: DandisetId,
    },
    DandisetReleases {
        dandiset_id: DandisetId,
    },
    Version {
        dandiset_id: DandisetId,
        version: VersionSpec,
    },
    DandisetYaml {
        dandiset_id: DandisetId,
        version: VersionSpec,
    },
    DandiResource {
        dandiset_id: DandisetId,
        version: VersionSpec,
        path: PurePath,
    },
    ZarrIndex,
    ZarrPath {
        path: PurePath,
    },
}

impl DavPath {
    pub(super) fn from_components(parts: Vec<Component>) -> Option<DavPath> {
        let mut iter = parts.into_iter();
        let Some(p1) = iter.next() else {
            return Some(DavPath::Root);
        };
        if p1.eq_ignore_ascii_case("dandisets") {
            let Some(did) = iter.next() else {
                return Some(DavPath::DandisetIndex);
            };
            let Ok(dandiset_id) = did.parse::<DandisetId>() else {
                return None;
            };
            let Some(p3) = iter.next() else {
                return Some(DavPath::Dandiset { dandiset_id });
            };
            let version = if p3.eq_ignore_ascii_case("releases") {
                let Some(v) = iter.next() else {
                    return Some(DavPath::DandisetReleases { dandiset_id });
                };
                let Ok(pv) = v.parse::<PublishedVersionId>() else {
                    return None;
                };
                VersionSpec::Published(pv)
            } else if p3.eq_ignore_ascii_case("latest") {
                VersionSpec::Latest
            } else if p3.eq_ignore_ascii_case("draft") {
                VersionSpec::Draft
            } else {
                return None;
            };
            match join_parts(iter)? {
                None => Some(DavPath::Version {
                    dandiset_id,
                    version,
                }),
                Some(p) if p == "dandiset.yaml" => Some(DavPath::DandisetYaml {
                    dandiset_id,
                    version,
                }),
                Some(path) => Some(DavPath::DandiResource {
                    dandiset_id,
                    version,
                    path,
                }),
            }
        } else if p1.eq_ignore_ascii_case("zarrs") {
            match join_parts(iter)? {
                None => Some(DavPath::ZarrIndex),
                Some(path) => Some(DavPath::ZarrPath { path }),
            }
        } else {
            None
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum VersionSpec {
    Draft,
    Published(PublishedVersionId),
    Latest,
}

pub(super) fn split_uri_path(s: &str) -> Option<Vec<Component>> {
    // TODO: Convert decoding-failures into DavError:
    let path = percent_encoding::percent_decode_str(s).decode_utf8().ok()?;
    let mut parts = Vec::new();
    for p in SplitComponents::new(&path) {
        match p.parse::<Component>() {
            Ok(c) => parts.push(c),
            Err(ParseComponentError::Empty) => unreachable!("part should not be empty"),
            Err(ParseComponentError::Slash) => {
                unreachable!("part should not contain / after splitting on /")
            }
            // TODO: Report NULs as DavErrors:
            Err(ParseComponentError::Nul) => return None,
            Err(ParseComponentError::CurDir) => (),
            Err(ParseComponentError::ParentDir) => {
                let _ = parts.pop();
            }
        }
    }
    Some(parts)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SplitComponents<'a>(&'a str);

impl<'a> SplitComponents<'a> {
    fn new(s: &'a str) -> Self {
        SplitComponents(s.trim_start_matches('/'))
    }
}

impl<'a> Iterator for SplitComponents<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<&'a str> {
        if self.0.is_empty() {
            None
        } else {
            let (pre, post) = match self.0.split_once('/') {
                Some((pre, post)) => (pre, post.trim_start_matches('/')),
                None => (self.0, ""),
            };
            self.0 = post;
            Some(pre)
        }
    }
}

impl std::iter::FusedIterator for SplitComponents<'_> {}

fn is_fast_not_exist(s: &str) -> bool {
    FAST_NOT_EXIST.binary_search(&s).is_ok()
}

// - Returns `None` if path does not exist due to containing a `FAST_NOT_EXIST`
//   component
// - Returns `Some(None)` if path is empty/root/has no components
fn join_parts<I: Iterator<Item = Component>>(iter: I) -> Option<Option<PurePath>> {
    let parts = iter.collect::<Vec<_>>();
    parts
        .iter()
        .all(|c| !is_fast_not_exist(c))
        .then(|| PurePath::from_components(parts))
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use rstest::rstest;

    #[rstest]
    #[case("/foo")]
    #[case("/dandisets/123")]
    #[case("/dandisets/draft")]
    #[case("/dandisets/000123/0.201234.1")]
    #[case("/dandisets/000123/releases/draft")]
    #[case("/dandisets/000123/draft/.git/index")]
    #[case("/dandisets/000123/draft/foo/.svn")]
    #[case("/dandisets/000123/draft/foo/%2esvn")]
    #[case("/dandisets/000123/draft/foo%2f.svn")]
    #[case("/dandisets/000123/draft/foo/.nols/bar")]
    #[case("/dandisets/000123/draft/.bzr")]
    fn test_bad_uri_paths(#[case] path: &str) {
        let parts = split_uri_path(path).unwrap();
        assert_eq!(DavPath::from_components(parts), None);
    }

    #[rstest]
    #[case("")]
    #[case("/")]
    #[case("//")]
    fn test_root(#[case] path: &str) {
        let parts = split_uri_path(path).unwrap();
        assert_eq!(DavPath::from_components(parts), Some(DavPath::Root));
    }

    #[rstest]
    #[case("/dandisets")]
    #[case("/dandisets/")]
    #[case("/dandisets//")]
    #[case("//dandisets/")]
    #[case("/Dandisets")]
    #[case("/DandiSets")]
    fn test_dandiset_index(#[case] path: &str) {
        let parts = split_uri_path(path).unwrap();
        assert_eq!(
            DavPath::from_components(parts),
            Some(DavPath::DandisetIndex)
        );
    }

    #[rstest]
    #[case("/dandisets/000123")]
    #[case("/dandisets/000123/")]
    #[case("/dandisets//000123")]
    #[case("/Dandisets/000123")]
    #[case("/DandiSets/000123")]
    fn test_dandiset(#[case] path: &str) {
        let parts = split_uri_path(path).unwrap();
        assert_matches!(DavPath::from_components(parts), Some(DavPath::Dandiset {dandiset_id}) => {
            assert_eq!(dandiset_id, "000123");
        });
    }

    #[rstest]
    #[case("/dandisets/000123/releases")]
    #[case("/dandisets/000123/releases/")]
    #[case("/Dandisets/000123/Releases")]
    #[case("/DandiSets/000123/ReLeAsEs/")]
    fn test_dandiset_releases(#[case] path: &str) {
        let parts = split_uri_path(path).unwrap();
        assert_matches!(DavPath::from_components(parts), Some(DavPath::DandisetReleases {dandiset_id}) => {
            assert_eq!(dandiset_id, "000123");
        });
    }

    #[rstest]
    #[case("/dandisets/000123/draft")]
    #[case("/dandisets/000123/draft/")]
    #[case("/Dandisets/000123/Draft")]
    #[case("/DandiSets/000123/dRaFt/")]
    fn test_dandiset_draft(#[case] path: &str) {
        let parts = split_uri_path(path).unwrap();
        assert_matches!(DavPath::from_components(parts), Some(DavPath::Version {dandiset_id, version}) => {
            assert_eq!(dandiset_id, "000123");
            assert_eq!(version, VersionSpec::Draft);
        });
    }

    #[rstest]
    #[case("/dandisets/000123/latest")]
    #[case("/dandisets/000123/latest/")]
    #[case("/Dandisets/000123/Latest")]
    #[case("/DandiSets/000123/LaTeST/")]
    fn test_dandiset_latest(#[case] path: &str) {
        let parts = split_uri_path(path).unwrap();
        assert_matches!(DavPath::from_components(parts), Some(DavPath::Version {dandiset_id, version}) => {
            assert_eq!(dandiset_id, "000123");
            assert_eq!(version, VersionSpec::Latest);
        });
    }

    #[rstest]
    #[case("/dandisets/000123/releases/0.240123.42")]
    #[case("/dandisets/000123/releases/0.240123.42/")]
    #[case("/Dandisets/000123/Releases//0.240123.42")]
    #[case("/DandiSets/000123/ReLeAsEs/0.240123.42//")]
    fn test_dandiset_published_version(#[case] path: &str) {
        let parts = split_uri_path(path).unwrap();
        assert_matches!(DavPath::from_components(parts), Some(DavPath::Version {dandiset_id, version}) => {
            assert_eq!(dandiset_id, "000123");
            assert_matches!(version, VersionSpec::Published(v) => {
                assert_eq!(v, "0.240123.42");
            });
        });
    }

    #[rstest]
    #[case("/dandisets/000123/draft/dandiset.yaml")]
    #[case("/dandisets/000123/draft/dandiset.yaml/")]
    #[case("/Dandisets/000123/Draft/dandiset.yaml")]
    #[case("/DandiSets/000123/dRaFt/dandiset.yaml")]
    fn test_dandiset_draft_dandiset_yaml(#[case] path: &str) {
        let parts = split_uri_path(path).unwrap();
        assert_matches!(DavPath::from_components(parts), Some(DavPath::DandisetYaml {dandiset_id, version}) => {
            assert_eq!(dandiset_id, "000123");
            assert_eq!(version, VersionSpec::Draft);
        });
    }

    #[rstest]
    #[case("/dandisets/000123/draft/Dandiset.yaml", "Dandiset.yaml")]
    #[case("/dandisets/000123/draft/dandiset.yml", "dandiset.yml")]
    #[case("/dandisets/000123/draft/foo", "foo")]
    #[case("/dandisets/000123/draft/foo/bar", "foo/bar")]
    #[case("/dandisets/000123/draft/foo%2fbar", "foo/bar")]
    #[case("/dandisets/000123/draft/foo%20bar", "foo bar")]
    #[case("/dandisets/000123/draft/foo/./bar", "foo/bar")]
    #[case("/dandisets/000123/draft//foo//bar/", "foo/bar")]
    #[case("/dandisets/000123/draft/foo/../bar", "bar")]
    #[case("/dandisets/000123/draft/foo/%2e%2e/bar", "bar")]
    fn test_dandiset_draft_resource(#[case] s: &str, #[case] respath: &str) {
        let parts = split_uri_path(s).unwrap();
        assert_matches!(DavPath::from_components(parts), Some(DavPath::DandiResource {dandiset_id, version, path}) => {
            assert_eq!(dandiset_id, "000123");
            assert_eq!(version, VersionSpec::Draft);
            assert_eq!(path, respath);
        });
    }

    #[rstest]
    #[case("/dandisets/000123/latest/Dandiset.yaml", "Dandiset.yaml")]
    #[case("/dandisets/000123/latest/dandiset.yml", "dandiset.yml")]
    #[case("/dandisets/000123/latest/foo", "foo")]
    #[case("/dandisets/000123/latest/foo/bar", "foo/bar")]
    #[case("/dandisets/000123/latest/foo%2fbar", "foo/bar")]
    #[case("/dandisets/000123/latest/foo%20bar", "foo bar")]
    #[case("/dandisets/000123/latest/foo/./bar", "foo/bar")]
    #[case("/dandisets/000123/latest//foo//bar/", "foo/bar")]
    fn test_dandiset_latest_resource(#[case] s: &str, #[case] respath: &str) {
        let parts = split_uri_path(s).unwrap();
        assert_matches!(DavPath::from_components(parts), Some(DavPath::DandiResource {dandiset_id, version, path}) => {
            assert_eq!(dandiset_id, "000123");
            assert_eq!(version, VersionSpec::Latest);
            assert_eq!(path, respath);
        });
    }

    #[rstest]
    #[case(
        "/dandisets/000123/releases/0.240123.42/Dandiset.yaml",
        "Dandiset.yaml"
    )]
    #[case("/dandisets/000123/releases/0.240123.42/dandiset.yml", "dandiset.yml")]
    #[case("/dandisets/000123/releases/0.240123.42/foo", "foo")]
    #[case("/dandisets/000123/Releases/0.240123.42/foo/bar", "foo/bar")]
    #[case("/dandisets/000123/rElEaSeS/0.240123.42/foo%2fbar", "foo/bar")]
    #[case("/dandisets/000123/ReLeAsEs/0.240123.42/foo%20bar", "foo bar")]
    #[case("/dandisets/000123/RELEASES/0.240123.42/foo/./bar", "foo/bar")]
    #[case("/dandisets/000123/releases/0.240123.42//foo//bar/", "foo/bar")]
    fn test_dandiset_publish_version_resource(#[case] s: &str, #[case] respath: &str) {
        let parts = split_uri_path(s).unwrap();
        assert_matches!(DavPath::from_components(parts), Some(DavPath::DandiResource {dandiset_id, version, path}) => {
            assert_eq!(dandiset_id, "000123");
            assert_matches!(version, VersionSpec::Published(v) => {
                assert_eq!(v, "0.240123.42");
            });
            assert_eq!(path, respath);
        });
    }

    #[rstest]
    #[case("/zarrs")]
    #[case("/zarrs/")]
    #[case("/zarrs//")]
    #[case("//zarrs/")]
    #[case("/Zarrs")]
    #[case("/ZARRS")]
    fn test_zarr_index(#[case] path: &str) {
        let parts = split_uri_path(path).unwrap();
        assert_eq!(DavPath::from_components(parts), Some(DavPath::ZarrIndex));
    }

    #[rstest]
    #[case("/zarrs/123", "123")]
    #[case("/zarrs/123/", "123")]
    #[case("/zarrs/123/abc", "123/abc")]
    #[case("/ZARRS/123/ABC", "123/ABC")]
    fn test_zarr_path(#[case] s: &str, #[case] respath: &str) {
        let parts = split_uri_path(s).unwrap();
        assert_matches!(DavPath::from_components(parts), Some(DavPath::ZarrPath {path}) => {
            assert_eq!(path, respath);
        });
    }
}
