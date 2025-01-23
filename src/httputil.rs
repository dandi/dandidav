//! HTTP utilities
use crate::consts::USER_AGENT;
use crate::dav::ErrorClass;
use reqwest::{Method, Request, Response, StatusCode};
use reqwest_middleware::{Middleware, Next};
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use serde::{
    de::{DeserializeOwned, Deserializer, Error as _},
    Deserialize,
};
use std::fmt;
use std::future::Future;
use std::str::FromStr;
use thiserror::Error;
use tracing::Instrument;
use url::Url;

/// An HTTP client that logs all requests and retries failed requests
#[derive(Debug, Clone)]
pub(crate) struct Client(reqwest_middleware::ClientWithMiddleware);

impl Client {
    /// Construct a new client
    ///
    /// # Errors
    ///
    /// Returns an error if construction of the inner `reqwest::Client` fails
    pub(crate) fn new() -> Result<Client, BuildClientError> {
        let retry_policy = ExponentialBackoff::builder()
            .base(2)
            .build_with_max_retries(4);
        let client = reqwest_middleware::ClientBuilder::new(
            reqwest::ClientBuilder::new()
                .user_agent(USER_AGENT)
                .timeout(std::time::Duration::from_secs(10))
                .build()?,
        )
        .with(SimpleReqwestLogger)
        // Retry network errors and responses of 408, 429, or 5xx up to four
        // times, sleeping for 1s/2s/4s/8s before each retry attempt.
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build();
        Ok(Client(client))
    }

    /// Perform an HTTP request with the given method to the given URL
    ///
    /// # Errors
    ///
    /// If sending the request fails or the response has a 4xx or 5xx status,
    /// an error is returned.
    pub(crate) async fn request(
        &self,
        method: Method,
        url: HttpUrl,
    ) -> Result<Response, HttpError> {
        let r = self
            .0
            .request(method, Url::from(url.clone()))
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

    /// Perform a `HEAD` request to the given URL
    ///
    /// # Errors
    ///
    /// If sending the request fails or the response has a 4xx or 5xx status,
    /// an error is returned.
    pub(crate) async fn head(&self, url: HttpUrl) -> Result<Response, HttpError> {
        self.request(Method::HEAD, url).await
    }

    /// Perform a `GET` request to the given URL
    ///
    /// # Errors
    ///
    /// If sending the request fails or the response has a 4xx or 5xx status,
    /// an error is returned.
    pub(crate) async fn get(&self, url: HttpUrl) -> Result<Response, HttpError> {
        self.request(Method::GET, url).await
    }

    /// Perform a `GET` request to the given URL and deserialize the response
    /// body as JSON into `T`
    ///
    /// # Errors
    ///
    /// If sending the request fails, the response has a 4xx or 5xx status, or
    /// deserialization of the response body fails, an error is returned.
    pub(crate) fn get_json<T: DeserializeOwned>(
        &self,
        url: HttpUrl,
    ) -> impl Future<Output = Result<T, HttpError>> {
        // Clone the client and move it into an async block (as opposed to just
        // writing a "normal" async function) so that the resulting Future will
        // be 'static rather than retaining a reference to &self, thereby
        // simplifying the Future's use by the Paginate stream.
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

/// Middleware for a `reqwest::Client` that adds logging of HTTP requests and
/// their responses
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

/// Error returned if initializing an HTTP client fails
#[derive(Debug, Error)]
#[error("failed to initialize HTTP client")]
pub(crate) struct BuildClientError(#[from] reqwest::Error);

/// Error returned if an outgoing HTTP request fails
#[derive(Debug, Error)]
pub(crate) enum HttpError {
    /// Sending the request failed
    #[error("failed to make request to {url}")]
    Send {
        url: HttpUrl,
        source: reqwest_middleware::Error,
    },

    /// The server returned a 404 response
    #[error("no such resource: {url}")]
    NotFound { url: HttpUrl },

    /// The server returned a 4xx or 5xx response other than 404
    #[error("request to {url} returned error")]
    Status {
        url: HttpUrl,
        source: reqwest::Error,
    },

    /// Deserializing the response body as JSON failed
    #[error("failed to deserialize response body from {url}")]
    Deserialize {
        url: HttpUrl,
        source: reqwest::Error,
    },
}

impl HttpError {
    /// Classify the general type of error
    pub(crate) fn class(&self) -> ErrorClass {
        match self {
            HttpError::NotFound { .. } => ErrorClass::NotFound,
            _ => ErrorClass::BadGateway,
        }
    }
}

/// A wrapper around [`url::Url`] that enforces a scheme of "http" or "https"
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct HttpUrl(Url);

impl HttpUrl {
    /// Return the URL as a string
    pub(crate) fn as_str(&self) -> &str {
        self.0.as_str()
    }

    /// Return a reference to the underlying [`url::Url`]
    pub(crate) fn as_url(&self) -> &Url {
        &self.0
    }

    /// Append the given path segment to this URL's path component.
    ///
    /// If the URL does not end with a forward slash, one will be appended, and
    /// then the segment will be added after that.
    pub(crate) fn push<S: AsRef<str>>(&mut self, segment: S) -> &mut Self {
        {
            let Ok(mut ps) = self.0.path_segments_mut() else {
                unreachable!("HTTP(S) URLs should always be able to be a base");
            };
            ps.pop_if_empty().push(segment.as_ref());
        }
        self
    }

    /// Append the given path segments to this URL's path component.
    ///
    /// If the URL does not end with a forward slash, one will be appended, and
    /// then the segments will be added after that.
    pub(crate) fn extend<I>(&mut self, segments: I) -> &mut Self
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        {
            let Ok(mut ps) = self.0.path_segments_mut() else {
                unreachable!("HTTP(S) URLs should always be able to be a base");
            };
            ps.pop_if_empty().extend(segments);
        }
        self
    }

    /// Append a trailing forward slash to the URL if it does not already end
    /// with one
    pub(crate) fn ensure_dirpath(&mut self) -> &mut Self {
        {
            let Ok(mut ps) = self.0.path_segments_mut() else {
                unreachable!("HTTP(S) URLs should always be able to be a base");
            };
            ps.pop_if_empty().push("");
        }
        self
    }

    /// Append `"{key}={value}"` (after percent-encoding) to the URL's query
    /// parameters
    pub(crate) fn append_query_param(&mut self, key: &str, value: &str) -> &mut Self {
        self.0.query_pairs_mut().append_pair(key, value);
        self
    }
}

impl From<HttpUrl> for Url {
    fn from(value: HttpUrl) -> Url {
        value.0
    }
}

impl fmt::Display for HttpUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for HttpUrl {
    type Err = ParseHttpUrlError;

    fn from_str(s: &str) -> Result<HttpUrl, ParseHttpUrlError> {
        let url = s.parse::<Url>()?;
        if matches!(url.scheme(), "http" | "https") {
            Ok(HttpUrl(url))
        } else {
            Err(ParseHttpUrlError::BadScheme)
        }
    }
}

impl<'de> Deserialize<'de> for HttpUrl {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let url = Url::deserialize(deserializer)?;
        if matches!(url.scheme(), "http" | "https") {
            Ok(HttpUrl(url))
        } else {
            Err(D::Error::custom("expected URL with HTTP(S) scheme"))
        }
    }
}

/// Error returned by [`HttpUrl`]'s `FromStr` implementation
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum ParseHttpUrlError {
    /// The string was a valid URL, but the scheme was neither HTTP nor HTTPS
    #[error(r#"URL scheme must be "http" or "https""#)]
    BadScheme,

    /// The string was not a valid URL
    #[error(transparent)]
    Url(#[from] url::ParseError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case("foo#bar", "https://api.github.com/base/foo%23bar")]
    #[case("foo%bar", "https://api.github.com/base/foo%25bar")]
    #[case("foo/bar", "https://api.github.com/base/foo%2Fbar")]
    #[case("foo?bar", "https://api.github.com/base/foo%3Fbar")]
    fn push_special_chars(#[case] path: &str, #[case] expected: &str) {
        let mut base = "https://api.github.com/base".parse::<HttpUrl>().unwrap();
        base.push(path);
        assert_eq!(base.as_str(), expected);
    }

    #[rstest]
    #[case(&["foo"], "https://api.github.com/foo")]
    #[case(&["foo", "bar"], "https://api.github.com/foo/bar")]
    fn extend_nopath(
        #[values("https://api.github.com", "https://api.github.com/")] mut base: HttpUrl,
        #[case] segments: &[&str],
        #[case] expected: &str,
    ) {
        base.extend(segments);
        assert_eq!(base.as_str(), expected);
    }

    #[rstest]
    #[case(&["gnusto"], "https://api.github.com/foo/bar/gnusto")]
    #[case(&["gnusto", "cleesh"], "https://api.github.com/foo/bar/gnusto/cleesh")]
    fn extend_path(
        #[values("https://api.github.com/foo/bar", "https://api.github.com/foo/bar/")]
        mut base: HttpUrl,
        #[case] segments: &[&str],
        #[case] expected: &str,
    ) {
        base.extend(segments);
        assert_eq!(base.as_str(), expected);
    }

    #[rstest]
    #[case("https://api.github.com", "https://api.github.com/")]
    #[case("https://api.github.com/", "https://api.github.com/")]
    #[case("https://api.github.com/foo", "https://api.github.com/foo/")]
    #[case("https://api.github.com/foo/", "https://api.github.com/foo/")]
    fn ensure_dirpath(#[case] mut before: HttpUrl, #[case] after: &str) {
        before.ensure_dirpath();
        assert_eq!(before.as_str(), after);
    }

    #[test]
    fn append_query_param() {
        let mut url = "https://api.github.com/foo".parse::<HttpUrl>().unwrap();
        assert_eq!(url.as_str(), "https://api.github.com/foo");
        url.append_query_param("bar", "baz");
        assert_eq!(url.as_str(), "https://api.github.com/foo?bar=baz");
        url.append_query_param("quux", "with space");
        assert_eq!(
            url.as_str(),
            "https://api.github.com/foo?bar=baz&quux=with+space"
        );
        url.append_query_param("bar", "rod");
        assert_eq!(
            url.as_str(),
            "https://api.github.com/foo?bar=baz&quux=with+space&bar=rod"
        );
    }
}
