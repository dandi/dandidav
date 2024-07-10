use crate::consts::USER_AGENT;
use reqwest::{Method, Request, Response, StatusCode};
use reqwest_middleware::{Middleware, Next};
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use serde::de::DeserializeOwned;
use std::future::Future;
use thiserror::Error;
use tracing::Instrument;
use url::Url;

#[derive(Debug, Clone)]
pub(crate) struct Client(reqwest_middleware::ClientWithMiddleware);

impl Client {
    pub(crate) fn new() -> Result<Client, BuildClientError> {
        let retry_policy = ExponentialBackoff::builder()
            .base(2)
            .build_with_max_retries(4);
        let client = reqwest_middleware::ClientBuilder::new(
            reqwest::ClientBuilder::new()
                .user_agent(USER_AGENT)
                .build()?,
        )
        .with(SimpleReqwestLogger)
        // Retry network errors and responses of 408, 429, or 5xx up to four
        // times, sleeping for 1s/2s/4s/8s before each retry attempt.
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build();
        Ok(Client(client))
    }

    pub(crate) async fn request(&self, method: Method, url: Url) -> Result<Response, HttpError> {
        let r = self
            .0
            .request(method, url.clone())
            .send()
            .await
            .map_err(|source| HttpError::Send {
                url: url.clone(),
                source,
            })?;
        if r.status() == StatusCode::NOT_FOUND {
            return Err(HttpError::NotFound { url });
        }
        r.error_for_status()
            .map_err(|source| HttpError::Status { url, source })
    }

    pub(crate) async fn head(&self, url: Url) -> Result<Response, HttpError> {
        self.request(Method::HEAD, url).await
    }

    pub(crate) async fn get(&self, url: Url) -> Result<Response, HttpError> {
        self.request(Method::GET, url).await
    }

    pub(crate) fn get_json<T: DeserializeOwned>(
        &self,
        url: Url,
    ) -> impl Future<Output = Result<T, HttpError>> {
        // Clone the client and move it into an async block (as opposed to just
        // writing a "normal" async function) so that the resulting Future will
        // be 'static rather than retaining a reference to &self, thereby
        // facilitating the Future's use by the Paginate stream.
        let client = self.clone();
        async move {
            client
                .get(url.clone())
                .await?
                .json::<T>()
                .await
                .map_err(move |source| HttpError::Deserialize { url, source })
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct SimpleReqwestLogger;

#[async_trait::async_trait]
impl Middleware for SimpleReqwestLogger {
    async fn handle(
        &self,
        req: Request,
        extensions: &mut axum::http::Extensions,
        next: Next<'_>,
    ) -> reqwest_middleware::Result<Response> {
        let span =
            tracing::debug_span!("outgoing-request", url = %req.url(), method = %req.method());
        async move {
            tracing::debug!("Making HTTP request");
            let r = next.run(req, extensions).await;
            match r {
                Ok(ref resp) => tracing::debug!(status = %resp.status(), "Response received"),
                Err(ref e) => tracing::debug!(error = ?e, "Failed to receive response"),
            }
            r
        }
        .instrument(span)
        .await
    }
}

#[derive(Debug, Error)]
#[error("failed to initialize HTTP client")]
pub(crate) struct BuildClientError(#[from] reqwest::Error);

#[derive(Debug, Error)]
pub(crate) enum HttpError {
    #[error("failed to make request to {url}")]
    Send {
        url: Url,
        source: reqwest_middleware::Error,
    },
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
