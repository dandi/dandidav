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
pub(crate) struct DandisetId(CompactString);

impl fmt::Debug for DandisetId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl PartialEq<str> for DandisetId {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl<'a> PartialEq<&'a str> for DandisetId {
    fn eq(&self, other: &&'a str) -> bool {
        &self.0 == other
    }
}

impl std::str::FromStr for DandisetId {
    type Err = ParseDandisetIdError;

    fn from_str(s: &str) -> Result<DandisetId, ParseDandisetIdError> {
        if s.chars().all(|c| c.is_ascii_digit()) && s.len() >= 6 {
            Ok(DandisetId(CompactString::from(s)))
        } else {
            Err(ParseDandisetIdError)
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("Dandiset IDs must be six or more decimal digits")]
pub(crate) struct ParseDandisetIdError;

impl Serialize for DandisetId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_ref())
    }
}

impl<'de> Deserialize<'de> for DandisetId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct DandisetIdVisitor;

        impl Visitor<'_> for DandisetIdVisitor {
            type Value = DandisetId;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a Dandiset ID")
            }

            fn visit_str<E>(self, input: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                input
                    .parse::<DandisetId>()
                    .map_err(|_| E::invalid_value(Unexpected::Str(input), &self))
            }
        }

        deserializer.deserialize_str(DandisetIdVisitor)
    }
}
