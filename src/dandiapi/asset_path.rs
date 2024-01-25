use derive_more::{AsRef, Deref, Display};
use serde::{
    de::{Deserializer, Unexpected, Visitor},
    ser::Serializer,
    Deserialize, Serialize,
};
use std::fmt;
use thiserror::Error;

// A nonempty, forward-slash-separated path that does not contain any of the
// following:
// - a `.` or `..` component
// - a leading or trailing forward slash
// - two or more consecutive forward slashes
// - NUL
#[derive(AsRef, Clone, Deref, Display, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[as_ref(forward)]
#[deref(forward)]
pub(crate) struct AssetPath(String);

impl AssetPath {
    pub(crate) fn name(&self) -> &str {
        self.0
            .split('/')
            .next_back()
            .expect("asset path should be nonempty")
    }

    pub(crate) fn join(&self, subpath: &AssetPath) -> AssetPath {
        AssetPath(format!("{self}/{subpath}"))
    }

    pub(crate) fn is_strictly_under(&self, other: &AssetPath) -> bool {
        let Some(rest) = self.0.strip_prefix(&other.0) else {
            return false;
        };
        rest.starts_with('/')
    }
}

impl fmt::Debug for AssetPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl PartialEq<str> for AssetPath {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl<'a> PartialEq<&'a str> for AssetPath {
    fn eq(&self, other: &&'a str) -> bool {
        &self.0 == other
    }
}

impl std::str::FromStr for AssetPath {
    type Err = ParseAssetPathError;

    fn from_str(s: &str) -> Result<AssetPath, ParseAssetPathError> {
        if s.is_empty() {
            Err(ParseAssetPathError::Empty)
        } else if s.starts_with('/') {
            Err(ParseAssetPathError::StartsWithSlash)
        } else if s.ends_with('/') {
            Err(ParseAssetPathError::EndsWithSlash)
        } else if s.contains('\0') {
            Err(ParseAssetPathError::Nul)
        } else if s.split('/').any(|p| p.is_empty() || p == "." || p == "..") {
            Err(ParseAssetPathError::NotNormalized)
        } else {
            Ok(AssetPath(s.into()))
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum ParseAssetPathError {
    #[error("asset paths cannot be empty")]
    Empty,
    #[error("asset paths cannot start with a forward slash")]
    StartsWithSlash,
    #[error("asset paths cannot end with a forward slash")]
    EndsWithSlash,
    #[error("asset paths cannot contain NUL")]
    Nul,
    #[error("asset path is not normalized")]
    NotNormalized,
}

impl Serialize for AssetPath {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_ref())
    }
}

impl<'de> Deserialize<'de> for AssetPath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct AssetPathVisitor;

        impl Visitor<'_> for AssetPathVisitor {
            type Value = AssetPath;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a valid asset path")
            }

            fn visit_str<E>(self, input: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                input
                    .parse::<AssetPath>()
                    .map_err(|_| E::invalid_value(Unexpected::Str(input), &self))
            }
        }

        deserializer.deserialize_str(AssetPathVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use rstest::rstest;

    #[rstest]
    #[case("foo", "foo")]
    #[case("foo/bar/baz", "baz")]
    fn test_name(#[case] p: AssetPath, #[case] name: &str) {
        assert_eq!(p.name(), name);
    }

    #[rstest]
    #[case("foo.nwb")]
    #[case("foo/bar.nwb")]
    fn test_good_asset_paths(#[case] s: &str) {
        let r = s.parse::<AssetPath>();
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
    fn test_bad_asset_paths(#[case] s: &str) {
        let r = s.parse::<AssetPath>();
        assert_matches!(r, Err(_));
    }

    #[rstest]
    #[case("foo/bar/baz", "foo/bar/baz", false)]
    #[case("foo/bar/baz", "foo/bar", true)]
    #[case("foo/bar/baz", "foo", true)]
    #[case("foo/bar", "foo/bar/baz", false)]
    #[case("foo", "foo/bar/baz", false)]
    #[case("foobar", "foo", false)]
    fn test_is_strictly_under(#[case] p1: AssetPath, #[case] p2: AssetPath, #[case] r: bool) {
        assert_eq!(p1.is_strictly_under(&p2), r);
    }
}
