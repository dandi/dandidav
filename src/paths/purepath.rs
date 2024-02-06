use super::{Component, PureDirPath};
use crate::consts::ZARR_EXTENSIONS;
use derive_more::{AsRef, Deref, Display};
use serde::{
    de::{Deserializer, Unexpected, Visitor},
    ser::Serializer,
    Deserialize, Serialize,
};
use std::fmt;
use thiserror::Error;

/// A nonempty, forward-slash-separated path that does not contain any of the
/// following:
///
/// - a `.` or `..` component
/// - a leading or trailing forward slash
/// - two or more consecutive forward slashes
/// - NUL
#[derive(AsRef, Clone, Deref, Display, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[as_ref(forward)]
#[deref(forward)]
pub(crate) struct PurePath(pub(super) String);

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
        self.0.split('/').map(|c| Component(c.to_owned()))
    }
}

impl fmt::Debug for PurePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl PartialEq<str> for PurePath {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl<'a> PartialEq<&'a str> for PurePath {
    fn eq(&self, other: &&'a str) -> bool {
        &self.0 == other
    }
}

impl std::str::FromStr for PurePath {
    type Err = ParsePurePathError;

    fn from_str(s: &str) -> Result<PurePath, ParsePurePathError> {
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
            Ok(PurePath(s.into()))
        }
    }
}

impl From<Component> for PurePath {
    fn from(value: Component) -> PurePath {
        PurePath(value.0)
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

impl Serialize for PurePath {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_ref())
    }
}

impl<'de> Deserialize<'de> for PurePath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct PurePathVisitor;

        impl Visitor<'_> for PurePathVisitor {
            type Value = PurePath;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a normalized relative path")
            }

            fn visit_str<E>(self, input: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                input
                    .parse::<PurePath>()
                    .map_err(|_| E::invalid_value(Unexpected::Str(input), &self))
            }
        }

        deserializer.deserialize_str(PurePathVisitor)
    }
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
    fn test_name(#[case] p: PurePath, #[case] name: &str) {
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
