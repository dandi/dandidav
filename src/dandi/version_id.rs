use serde::{
    de::{Deserializer, Unexpected, Visitor},
    ser::Serializer,
    Deserialize, Serialize,
};
use smartstring::alias::CompactString;
use std::fmt;
use thiserror::Error;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) enum VersionId {
    Draft,
    Published(PublishedVersionId),
}

impl AsRef<str> for VersionId {
    fn as_ref(&self) -> &str {
        match self {
            VersionId::Draft => "draft",
            VersionId::Published(v) => v.as_ref(),
        }
    }
}

impl std::ops::Deref for VersionId {
    type Target = str;

    fn deref(&self) -> &str {
        self.as_ref()
    }
}

impl PartialEq<str> for VersionId {
    fn eq(&self, other: &str) -> bool {
        match self {
            VersionId::Draft => other == "draft",
            VersionId::Published(v) => v == other,
        }
    }
}

impl<'a> PartialEq<&'a str> for VersionId {
    fn eq(&self, other: &&'a str) -> bool {
        self == *other
    }
}

impl fmt::Display for VersionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VersionId::Draft => write!(f, "draft"),
            VersionId::Published(v) => write!(f, "{v}"),
        }
    }
}

impl std::str::FromStr for VersionId {
    type Err = ParseVersionIdError;

    fn from_str(s: &str) -> Result<VersionId, ParseVersionIdError> {
        if s == "draft" {
            Ok(VersionId::Draft)
        } else {
            let v = s
                .parse::<PublishedVersionId>()
                .map_err(|_| ParseVersionIdError)?;
            Ok(VersionId::Published(v))
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error(r#"Version IDs must be "draft" or of the form "N.N.N""#)]
pub(crate) struct ParseVersionIdError;

impl Serialize for VersionId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_ref())
    }
}

impl<'de> Deserialize<'de> for VersionId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct VersionIdVisitor;

        impl Visitor<'_> for VersionIdVisitor {
            type Value = VersionId;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a Dandiset version ID")
            }

            fn visit_str<E>(self, input: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                input
                    .parse::<VersionId>()
                    .map_err(|_| E::invalid_value(Unexpected::Str(input), &self))
            }
        }

        deserializer.deserialize_str(VersionIdVisitor)
    }
}

#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct PublishedVersionId(CompactString);

fn validate(s: &str) -> Result<(), ParsePublishedVersionIdError> {
    let e = Err(ParsePublishedVersionIdError);
    let mut parts = s.split('.');
    for _ in 0..3 {
        let Some(p) = parts.next() else {
            return e;
        };
        if p.is_empty() || !p.chars().all(|c| c.is_ascii_digit()) {
            return e;
        }
    }
    if parts.next().is_none() {
        Ok(())
    } else {
        e
    }
}

validstr!(
    PublishedVersionId,
    ParsePublishedVersionIdError,
    validate,
    "a published Dandiset version ID"
);

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error(r#"Published version IDs must be of the form "N.N.N""#)]
pub(crate) struct ParsePublishedVersionIdError;

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case("draft", false)]
    #[case("0.210831.2033", true)]
    #[case("000027", false)]
    #[case("0.210831", false)]
    #[case("0.210831.2033.", false)]
    #[case("0.210831..2033", false)]
    #[case("1.2.3", true)]
    #[case("1.2.3.4", false)]
    #[case("", false)]
    fn test_published_version_id(#[case] s: &str, #[case] r: bool) {
        assert_eq!(s.parse::<PublishedVersionId>().is_ok(), r);
    }
}
