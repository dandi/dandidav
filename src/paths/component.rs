use derive_more::{AsRef, Deref, Display};
use serde::{
    de::{Deserializer, Unexpected, Visitor},
    ser::Serializer,
    Deserialize, Serialize,
};
use std::fmt;
use thiserror::Error;

/// A nonempty path component that does not contain a forward slash or NUL nor
/// equals `.` or `..`
#[derive(AsRef, Clone, Deref, Display, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[as_ref(forward)]
#[deref(forward)]
pub(crate) struct Component(pub(super) String);

impl fmt::Debug for Component {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl PartialEq<str> for Component {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl<'a> PartialEq<&'a str> for Component {
    fn eq(&self, other: &&'a str) -> bool {
        &self.0 == other
    }
}

impl std::str::FromStr for Component {
    type Err = ParseComponentError;

    fn from_str(s: &str) -> Result<Component, ParseComponentError> {
        if s.is_empty() {
            Err(ParseComponentError::Empty)
        } else if s.contains('/') {
            Err(ParseComponentError::Slash)
        } else if s.contains('\0') {
            Err(ParseComponentError::Nul)
        } else if s == "." || s == ".." {
            Err(ParseComponentError::SpecialDir)
        } else {
            Ok(Component(s.into()))
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum ParseComponentError {
    #[error("path components cannot be empty")]
    Empty,
    #[error("path components cannot contain a forward slash")]
    Slash,
    #[error("path components cannot contain NUL")]
    Nul,
    #[error(r#"path components cannot equal "." or "..""#)]
    SpecialDir,
}

impl Serialize for Component {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_ref())
    }
}

impl<'de> Deserialize<'de> for Component {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ComponentVisitor;

        impl Visitor<'_> for ComponentVisitor {
            type Value = Component;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a plain path component")
            }

            fn visit_str<E>(self, input: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                input
                    .parse::<Component>()
                    .map_err(|_| E::invalid_value(Unexpected::Str(input), &self))
            }
        }

        deserializer.deserialize_str(ComponentVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use rstest::rstest;

    #[rstest]
    #[case("foo")]
    #[case("foo.nwb")]
    #[case("foo bar")]
    fn test_good(#[case] s: &str) {
        let r = s.parse::<Component>();
        assert_matches!(r, Ok(c) => {
            assert_eq!(c, s);
        });
    }

    #[rstest]
    #[case("")]
    #[case(".")]
    #[case("..")]
    #[case("/")]
    #[case("\0")]
    #[case("foo/bar.nwb")]
    #[case("foo\0bar.nwb")]
    #[case("/foo")]
    #[case("foo/")]
    #[case("/foo/")]
    fn test_bad(#[case] s: &str) {
        let r = s.parse::<Component>();
        assert_matches!(r, Err(_));
    }
}
