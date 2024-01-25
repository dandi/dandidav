use derive_more::{AsRef, Deref, Display};
use serde::{
    de::{Deserializer, Unexpected, Visitor},
    ser::Serializer,
    Deserialize, Serialize,
};
use smartstring::alias::CompactString;
use std::fmt;
use thiserror::Error;

#[derive(AsRef, Clone, Deref, Display, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[as_ref(forward)]
#[deref(forward)]
pub(crate) struct VersionId(CompactString);

impl VersionId {
    pub(crate) fn is_draft(&self) -> bool {
        self.0 == "draft"
    }
}

impl fmt::Debug for VersionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl PartialEq<str> for VersionId {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl<'a> PartialEq<&'a str> for VersionId {
    fn eq(&self, other: &&'a str) -> bool {
        &self.0 == other
    }
}

impl std::str::FromStr for VersionId {
    type Err = ParseVersionIdError;

    fn from_str(s: &str) -> Result<VersionId, ParseVersionIdError> {
        if s == "draft" || is_published_version_id(s) {
            Ok(VersionId(CompactString::from(s)))
        } else {
            Err(ParseVersionIdError)
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

fn is_published_version_id(s: &str) -> bool {
    let mut parts = s.split('.');
    for _ in 0..3 {
        let Some(p) = parts.next() else {
            return false;
        };
        if p.is_empty() || !p.chars().all(|c| c.is_ascii_digit()) {
            return false;
        }
    }
    parts.next().is_none()
}

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
    fn test_is_published_version_id(#[case] s: &str, #[case] r: bool) {
        assert_eq!(is_published_version_id(s), r);
    }
}
