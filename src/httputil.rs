use crate::consts::USER_AGENT;
use reqwest::{ClientBuilder, StatusCode};
use serde::de::DeserializeOwned;
use thiserror::Error;
use url::Url;

pub(crate) fn new_client() -> Result<reqwest::Client, BuildClientError> {
    ClientBuilder::new()
        .user_agent(USER_AGENT)
        .build()
        .map_err(Into::into)
}

#[derive(Debug, Error)]
#[error("failed to initialize HTTP client")]
pub(crate) struct BuildClientError(#[from] reqwest::Error);

async fn get_response(client: &reqwest::Client, url: Url) -> Result<reqwest::Response, HttpError> {
    let r = client
        .get(url.clone())
        .send()
        .await
        .map_err(|source| HttpError::Send {
            url: url.clone(),
            source,
        })?;
    if r.status() == StatusCode::NOT_FOUND {
        return Err(HttpError::NotFound { url: url.clone() });
    }
    r.error_for_status().map_err(|source| HttpError::Status {
        url: url.clone(),
        source,
    })
}

pub(crate) async fn get_json<T: DeserializeOwned>(
    client: &reqwest::Client,
    url: Url,
) -> Result<T, HttpError> {
    get_response(client, url.clone())
        .await?
        .json::<T>()
        .await
        .map_err(move |source| HttpError::Deserialize { url, source })
}

#[derive(Debug, Error)]
pub(crate) enum HttpError {
    #[error("failed to make request to {url}")]
    Send { url: Url, source: reqwest::Error },
    #[error("no such resource: {url}")]
    NotFound { url: Url },
    #[error("request to {url} returned error")]
    Status { url: Url, source: reqwest::Error },
    #[error("failed to deserialize response body from {url}")]
    Deserialize { url: Url, source: reqwest::Error },
}

pub(crate) fn urljoin<I>(url: &Url, segments: I) -> Url
where
    I: IntoIterator,
    I::Item: AsRef<str>,
{
    let mut url = url.clone();
    url.path_segments_mut()
        .expect("URL should be able to be a base")
        .pop_if_empty()
        .extend(segments);
    url
}

pub(crate) fn urljoin_slashed<I>(url: &Url, segments: I) -> Url
where
    I: IntoIterator,
    I::Item: AsRef<str>,
{
    let mut url = url.clone();
    url.path_segments_mut()
        .expect("URL should be able to be a base")
        .pop_if_empty()
        .extend(segments)
        // Add an empty segment so that the final URL will end with a slash:
        .push("");
    url
}

#[cfg(test)]
mod tests {
    use super::*;

    mod urljoin {
        use super::*;
        use rstest::rstest;

        #[rstest]
        #[case("https://api.github.com")]
        #[case("https://api.github.com/")]
        fn nopath(#[case] base: Url) {
            let u = urljoin(&base, ["foo"]);
            assert_eq!(u.as_str(), "https://api.github.com/foo");
            let u = urljoin(&base, ["foo", "bar"]);
            assert_eq!(u.as_str(), "https://api.github.com/foo/bar");
        }

        #[rstest]
        #[case("https://api.github.com/foo/bar")]
        #[case("https://api.github.com/foo/bar/")]
        fn path(#[case] base: Url) {
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
        fn special_chars(#[case] path: &str, #[case] expected: &str) {
            let base = Url::parse("https://api.github.com/base").unwrap();
            let u = urljoin(&base, [path]);
            assert_eq!(u.as_str(), expected);
        }
    }

    mod urljoin_slashed {
        use super::*;
        use rstest::rstest;

        #[rstest]
        #[case("https://api.github.com")]
        #[case("https://api.github.com/")]
        fn nopath(#[case] base: Url) {
            let u = urljoin_slashed(&base, ["foo"]);
            assert_eq!(u.as_str(), "https://api.github.com/foo/");
            let u = urljoin_slashed(&base, ["foo", "bar"]);
            assert_eq!(u.as_str(), "https://api.github.com/foo/bar/");
        }

        #[rstest]
        #[case("https://api.github.com/foo/bar")]
        #[case("https://api.github.com/foo/bar/")]
        fn path(#[case] base: Url) {
            let u = urljoin_slashed(&base, ["gnusto"]);
            assert_eq!(u.as_str(), "https://api.github.com/foo/bar/gnusto/");
            let u = urljoin_slashed(&base, ["gnusto", "cleesh"]);
            assert_eq!(u.as_str(), "https://api.github.com/foo/bar/gnusto/cleesh/");
        }

        #[rstest]
        #[case("foo#bar", "https://api.github.com/base/foo%23bar/")]
        #[case("foo%bar", "https://api.github.com/base/foo%25bar/")]
        #[case("foo/bar", "https://api.github.com/base/foo%2Fbar/")]
        #[case("foo?bar", "https://api.github.com/base/foo%3Fbar/")]
        fn special_chars(#[case] path: &str, #[case] expected: &str) {
            let base = Url::parse("https://api.github.com/base").unwrap();
            let u = urljoin_slashed(&base, [path]);
            assert_eq!(u.as_str(), expected);
        }
    }
}
