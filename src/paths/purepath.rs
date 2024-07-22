use super::{Component, PureDirPath};
use crate::consts::ZARR_EXTENSIONS;
use thiserror::Error;

/// A nonempty, forward-slash-separated path that does not contain any of the
/// following:
///
/// - a `.` or `..` component
/// - a leading or trailing forward slash
/// - two or more consecutive forward slashes
/// - NUL
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct PurePath(pub(super) String);

fn validate(s: &str) -> Result<(), ParsePurePathError> {
    if s.is_empty() {
        Err(ParsePurePathError::Empty)
    } else if s.starts_with('/') {
        Err(ParsePurePathError::StartsWithSlash)
    } else if s.ends_with('/') {
        Err(ParsePurePathError::EndsWithSlash)
    } else if s.contains('\0') {
        Err(ParsePurePathError::Nul)
    } else if s.split('/').any(|p| p.is_empty() || p == "." || p == "..") {
        Err(ParsePurePathError::NotNormalized)
    } else {
        Ok(())
    }
}

validstr!(
    PurePath,
    ParsePurePathError,
    validate,
    "a normalized relative path"
);

impl PurePath {
    pub(crate) fn name_str(&self) -> &str {
        self.0
            .split('/')
            .next_back()
            .expect("path should be nonempty")
    }

    pub(crate) fn join_one(&self, c: &Component) -> PurePath {
        PurePath(format!("{self}/{c}"))
    }

    pub(crate) fn is_strictly_under(&self, other: &PureDirPath) -> bool {
        self.0.starts_with(&other.0)
    }

    /// For each non-final component in the path that has an extension of
    /// `.zarr` or `.ngff` (case sensitive), yield the portion of the path up
    /// through that component along with the rest of the path.
    pub(crate) fn split_zarr_candidates(&self) -> SplitZarrCandidates<'_> {
        SplitZarrCandidates::new(self)
    }

    pub(crate) fn relative_to(&self, dirpath: &PureDirPath) -> Option<PurePath> {
        let s = self.0.strip_prefix(&dirpath.0)?;
        debug_assert!(
            !s.is_empty(),
            "{self:?} relative to {dirpath:?} should not be empty"
        );
        Some(PurePath(s.to_owned()))
    }

    pub(crate) fn to_dir_path(&self) -> PureDirPath {
        PureDirPath(format!("{}/", self.0))
    }

    pub(crate) fn component_strs(&self) -> std::str::Split<'_, char> {
        self.0.split('/')
    }

    pub(crate) fn components(&self) -> impl Iterator<Item = Component> + '_ {
        self.0.split('/').map(|c| Component(c.into()))
    }

    pub(crate) fn push(&mut self, c: &Component) {
        self.0.push('/');
        self.0.push_str(c.as_ref());
    }

    pub(crate) fn from_components<I: IntoIterator<Item = Component>>(iter: I) -> Option<PurePath> {
        let mut iter = iter.into_iter();
        let mut path = PurePath::from(iter.next()?);
        for c in iter {
            path.push(&c);
        }
        Some(path)
    }
}

impl From<Component> for PurePath {
    fn from(value: Component) -> PurePath {
        PurePath(value.into())
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum ParsePurePathError {
    #[error("paths cannot be empty")]
    Empty,
    #[error("paths cannot start with a forward slash")]
    StartsWithSlash,
    #[error("paths cannot end with a forward slash")]
    EndsWithSlash,
    #[error("paths cannot contain NUL")]
    Nul,
    #[error("path is not normalized")]
    NotNormalized,
}

#[derive(Clone, Debug)]
pub(crate) struct SplitZarrCandidates<'a> {
    s: &'a str,
    inner: std::str::MatchIndices<'a, char>,
}

impl<'a> SplitZarrCandidates<'a> {
    fn new(path: &'a PurePath) -> Self {
        let s = &path.0;
        let inner = s.match_indices('/');
        SplitZarrCandidates { s, inner }
    }
}

impl Iterator for SplitZarrCandidates<'_> {
    type Item = (PurePath, PurePath);

    fn next(&mut self) -> Option<(PurePath, PurePath)> {
        for (i, _) in self.inner.by_ref() {
            let zarrpath = &self.s[..i];
            let entrypath = &self.s[(i + 1)..];
            for ext in ZARR_EXTENSIONS {
                if let Some(pre) = zarrpath.strip_suffix(ext) {
                    if !pre.is_empty() && !pre.ends_with('/') {
                        let zarrpath = PurePath(zarrpath.into());
                        let entrypath = PurePath(entrypath.into());
                        return Some((zarrpath, entrypath));
                    }
                }
            }
        }
        None
    }
}

impl std::iter::FusedIterator for SplitZarrCandidates<'_> {}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use rstest::rstest;

    #[rstest]
    #[case("foo", "foo")]
    #[case("foo/bar/baz", "baz")]
    fn test_name_str(#[case] p: PurePath, #[case] name: &str) {
        assert_eq!(p.name_str(), name);
    }

    #[rstest]
    #[case("foo.nwb")]
    #[case("foo/bar.nwb")]
    fn test_good_paths(#[case] s: &str) {
        let r = s.parse::<PurePath>();
        assert_matches!(r, Ok(_));
    }

    #[rstest]
    #[case("")]
    #[case("/")]
    #[case("/foo")]
    #[case("foo/")]
    #[case("/foo/")]
    #[case("foo//bar.nwb")]
    #[case("foo///bar.nwb")]
    #[case("foo/bar\0.nwb")]
    #[case("foo/./bar.nwb")]
    #[case("foo/../bar.nwb")]
    #[case("./foo/bar.nwb")]
    #[case("../foo/bar.nwb")]
    #[case("foo/bar.nwb/.")]
    #[case("foo/bar.nwb/..")]
    fn test_bad_paths(#[case] s: &str) {
        let r = s.parse::<PurePath>();
        assert_matches!(r, Err(_));
    }

    #[rstest]
    #[case("foo/bar/baz", "foo/bar/baz/", false)]
    #[case("foo/bar/baz", "foo/bar/", true)]
    #[case("foo/bar/baz", "foo/", true)]
    #[case("foo/bar", "foo/bar/baz/", false)]
    #[case("foo", "foo/bar/baz/", false)]
    #[case("foobar", "foo/", false)]
    fn test_is_strictly_under(#[case] p1: PurePath, #[case] p2: PureDirPath, #[case] r: bool) {
        assert_eq!(p1.is_strictly_under(&p2), r);
    }

    #[rstest]
    #[case("foo", "bar", "foo/bar")]
    #[case("foo/bar", "quux", "foo/bar/quux")]
    fn test_join_one(#[case] path: PurePath, #[case] c: Component, #[case] res: PurePath) {
        assert_eq!(path.join_one(&c), res);
    }

    #[rstest]
    #[case("foo/bar", "foo/", Some("bar"))]
    #[case("foo/bar/quux", "foo/", Some("bar/quux"))]
    #[case("foo/bar/quux", "foo/bar/", Some("quux"))]
    #[case("foo", "foo/bar/", None)]
    #[case("bar/quux", "foo/bar/quux/", None)]
    #[case("foo/bar", "quux/bar/", None)]
    fn test_relative_to(
        #[case] path: PurePath,
        #[case] dirpath: PureDirPath,
        #[case] relpath: Option<&str>,
    ) {
        assert_eq!(path.relative_to(&dirpath).as_deref(), relpath);
    }

    #[rstest]
    #[case("foo", vec!["foo"])]
    #[case("foo/bar", vec!["foo", "bar"])]
    #[case("foo/bar/quux", vec!["foo", "bar", "quux"])]
    fn test_components(#[case] path: PurePath, #[case] comps: Vec<&str>) {
        assert_eq!(path.components().collect::<Vec<_>>(), comps);
        assert_eq!(path.component_strs().collect::<Vec<_>>(), comps);
    }

    #[rstest]
    #[case("foo", "bar", "foo/bar")]
    #[case("foo/bar", "quux", "foo/bar/quux")]
    fn test_push(#[case] mut path: PurePath, #[case] c: Component, #[case] res: PurePath) {
        path.push(&c);
        assert_eq!(path, res);
    }

    #[rstest]
    #[case(Vec::new(), None)]
    #[case(vec!["foo"], Some("foo"))]
    #[case(vec!["foo", "bar"], Some("foo/bar"))]
    #[case(vec!["foo", "bar", "quux"], Some("foo/bar/quux"))]
    fn test_from_components(#[case] comps: Vec<&str>, #[case] path: Option<&str>) {
        assert_eq!(
            PurePath::from_components(comps.into_iter().map(|s| s.parse::<Component>().unwrap()))
                .as_deref(),
            path
        );
    }

    mod split_zarr_candidates {
        use super::*;

        #[test]
        fn no_zarr() {
            let path = "foo/bar/baz".parse::<PurePath>().unwrap();
            let mut iter = path.split_zarr_candidates();
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        #[test]
        fn one_zarr() {
            let path = "foo/bar.zarr/baz".parse::<PurePath>().unwrap();
            let mut iter = path.split_zarr_candidates();
            assert_matches!(iter.next(), Some((zp, ep)) => {
                assert_eq!(zp, "foo/bar.zarr");
                assert_eq!(ep, "baz");
            });
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        #[test]
        fn one_ngff() {
            let path = "foo/bar.ngff/baz".parse::<PurePath>().unwrap();
            let mut iter = path.split_zarr_candidates();
            assert_matches!(iter.next(), Some((zp, ep)) => {
                assert_eq!(zp, "foo/bar.ngff");
                assert_eq!(ep, "baz");
            });
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        #[test]
        fn multiple_zarrs() {
            let path = "foo.zarr/bar/baz.zarr/quux/glarch/cleesh.zarr/gnusto"
                .parse::<PurePath>()
                .unwrap();
            let mut iter = path.split_zarr_candidates();
            assert_matches!(iter.next(), Some((zp, ep)) => {
                assert_eq!(zp, "foo.zarr");
                assert_eq!(ep, "bar/baz.zarr/quux/glarch/cleesh.zarr/gnusto");
            });
            assert_matches!(iter.next(), Some((zp, ep)) => {
                assert_eq!(zp, "foo.zarr/bar/baz.zarr");
                assert_eq!(ep, "quux/glarch/cleesh.zarr/gnusto");
            });
            assert_matches!(iter.next(), Some((zp, ep)) => {
                assert_eq!(zp, "foo.zarr/bar/baz.zarr/quux/glarch/cleesh.zarr");
                assert_eq!(ep, "gnusto");
            });
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        #[test]
        fn consecutive_zarrs() {
            let path = "foo/bar.zarr/baz.zarr/quux".parse::<PurePath>().unwrap();
            let mut iter = path.split_zarr_candidates();
            assert_matches!(iter.next(), Some((zp, ep)) => {
                assert_eq!(zp, "foo/bar.zarr");
                assert_eq!(ep, "baz.zarr/quux");
            });
            assert_matches!(iter.next(), Some((zp, ep)) => {
                assert_eq!(zp, "foo/bar.zarr/baz.zarr");
                assert_eq!(ep, "quux");
            });
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        #[test]
        fn capital_zarr_ext() {
            let path = "foo/bar.Zarr/baz".parse::<PurePath>().unwrap();
            let mut iter = path.split_zarr_candidates();
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        #[test]
        fn capital_ngff_ext() {
            let path = "foo/bar.Ngff/baz".parse::<PurePath>().unwrap();
            let mut iter = path.split_zarr_candidates();
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        #[test]
        fn final_zarr() {
            let path = "foo/bar/baz.zarr".parse::<PurePath>().unwrap();
            let mut iter = path.split_zarr_candidates();
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        #[test]
        fn ext_component() {
            let path = "foo/.zarr/baz".parse::<PurePath>().unwrap();
            let mut iter = path.split_zarr_candidates();
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        #[test]
        fn ext_first_component() {
            let path = ".zarr/foo/baz".parse::<PurePath>().unwrap();
            let mut iter = path.split_zarr_candidates();
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        #[test]
        fn just_zarr() {
            let path = "foo.zarr".parse::<PurePath>().unwrap();
            let mut iter = path.split_zarr_candidates();
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        #[test]
        fn just_ext() {
            let path = ".zarr".parse::<PurePath>().unwrap();
            let mut iter = path.split_zarr_candidates();
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }
    }
}
