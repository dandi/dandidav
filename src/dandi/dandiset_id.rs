use smartstring::alias::CompactString;
use thiserror::Error;

#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct DandisetId(CompactString);

fn validate(s: &str) -> Result<(), ParseDandisetIdError> {
    if s.chars().all(|c| c.is_ascii_digit()) && s.len() >= 6 {
        Ok(())
    } else {
        Err(ParseDandisetIdError)
    }
}

validstr!(DandisetId, ParseDandisetIdError, validate, "a Dandiset ID");

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("Dandiset IDs must be six or more decimal digits")]
pub(crate) struct ParseDandisetIdError;
