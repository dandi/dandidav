use super::consts::USER_AGENT;
use async_stream::try_stream;
use derive_more::{AsRef, Deref, Display};
use futures_util::Stream;
use reqwest::ClientBuilder;
use serde::{
    de::{Deserializer, Unexpected, Visitor},
    ser::Serializer,
    Deserialize, Serialize,
};
use smartstring::alias::CompactString;
use std::fmt;
use thiserror::Error;
use time::OffsetDateTime;
use url::Url;

#[derive(Clone, Debug)]
pub(crate) struct Client {
    client: reqwest::Client,
    api_url: Url,
}

impl Client {
    pub(crate) fn new(api_url: Url) -> Result<Self, BuildClientError> {
        let client = ClientBuilder::new()
            .user_agent(USER_AGENT)
            .https_only(true)
            .build()?;
        Ok(Client { client, api_url })
    }

    pub(crate) fn get_dandisets(&self) -> impl Stream<Item = Result<Dandiset, ApiError>> {
        let this = self.clone();
        try_stream! {
            let mut url = Some(urljoin(&this.api_url, ["dandisets"]));
            while let Some(u) = url {
                let page = this.client
                    .get(u.clone())
                    .send()
                    .await
                    .map_err(|source| ApiError::Send {url: u.clone(), source})?
                    .error_for_status()
                    .map_err(|source| ApiError::Status {url: u.clone(), source})?
                    .json::<Page<Dandiset>>()
                    .await
                    .map_err(|source| ApiError::Deserialize {url: u, source})?;
                for r in page.results {
                    yield r;
                }
                url = page.next;
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
struct Page<T> {
    next: Option<Url>,
    results: Vec<T>,
}

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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub(crate) struct Dandiset {
    identifier: DandisetId,
    #[serde(with = "time::serde::rfc3339")]
    created: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    modified: OffsetDateTime,
    //contact_person: String,
    //embargo_status: ...,
    draft_version: Option<DandisetVersion>,
    most_recent_published_version: Option<DandisetVersion>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub(crate) struct DandisetVersion {
    version: String,
    //name: String,
    //asset_count: u64,
    size: u64,
    //status: ...,
    #[serde(with = "time::serde::rfc3339")]
    created: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    modified: OffsetDateTime,
}

#[derive(Debug, Error)]
#[error("failed to initialize Dandi API client")]
pub(crate) struct BuildClientError(#[from] reqwest::Error);

#[derive(Debug, Error)]
pub(crate) enum ApiError {
    #[error("failed to make request to {url}")]
    Send { url: Url, source: reqwest::Error },
    #[error("request to {url} returned error")]
    Status { url: Url, source: reqwest::Error },
    #[error("failed to deserialize response body from request to {url}")]
    Deserialize { url: Url, source: reqwest::Error },
}

fn urljoin<I>(url: &Url, segments: I) -> Url
where
    I: IntoIterator,
    I::Item: AsRef<str>,
{
    let mut url = url.clone();
    url.path_segments_mut()
        .expect("API URL should be able to be a base")
        .pop_if_empty()
        .extend(segments);
    url
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case("https://api.github.com")]
    #[case("https://api.github.com/")]
    fn test_urljoin_nopath(#[case] base: Url) {
        let u = urljoin(&base, ["foo"]);
        assert_eq!(u.as_str(), "https://api.github.com/foo");
        let u = urljoin(&base, ["foo", "bar"]);
        assert_eq!(u.as_str(), "https://api.github.com/foo/bar");
    }

    #[rstest]
    #[case("https://api.github.com/foo/bar")]
    #[case("https://api.github.com/foo/bar/")]
    fn test_urljoin_path(#[case] base: Url) {
        let u = urljoin(&base, ["gnusto"]);
        assert_eq!(u.as_str(), "https://api.github.com/foo/bar/gnusto");
        let u = urljoin(&base, ["gnusto", "cleesh"]);
        assert_eq!(u.as_str(), "https://api.github.com/foo/bar/gnusto/cleesh");
    }

    #[rstest]
    #[case("foo#bar", "https://api.github.com/base/foo%23bar")]
    #[case("foo%bar", "https://api.github.com/base/foo%25bar")]
    #[case("foo/bar", "https://api.github.com/base/foo%2Fbar")]
    #[case("foo?bar", "https://api.github.com/base/foo%3Fbar")]
    fn test_urljoin_special_chars(#[case] path: &str, #[case] expected: &str) {
        let base = Url::parse("https://api.github.com/base").unwrap();
        let u = urljoin(&base, [path]);
        assert_eq!(u.as_str(), expected);
    }
}
