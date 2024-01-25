mod dandiset_id;
mod types;
mod version_id;
pub(crate) use self::dandiset_id::*;
pub(crate) use self::types::*;
pub(crate) use self::version_id::*;
use super::consts::USER_AGENT;
use async_stream::try_stream;
use futures_util::Stream;
use reqwest::{ClientBuilder, StatusCode};
use serde::de::DeserializeOwned;
use thiserror::Error;
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

    pub(crate) async fn get<T: DeserializeOwned>(&self, url: Url) -> Result<T, ApiError> {
        let r = self
            .client
            .get(url.clone())
            .send()
            .await
            .map_err(|source| ApiError::Send {
                url: url.clone(),
                source,
            })?;
        if r.status() == StatusCode::NOT_FOUND {
            return Err(ApiError::NotFound { url: url.clone() });
        }
        r.error_for_status()
            .map_err(|source| ApiError::Status {
                url: url.clone(),
                source,
            })?
            .json::<T>()
            .await
            .map_err(move |source| ApiError::Deserialize { url, source })
    }

    fn paginate<T: DeserializeOwned>(&self, url: Url) -> impl Stream<Item = Result<T, ApiError>> {
        let this = self.clone();
        try_stream! {
            let mut url = Some(url);
            while let Some(u) = url {
                let resp = this.client
                    .get(u.clone())
                    .send()
                    .await
                    .map_err(|source| ApiError::Send {url: u.clone(), source})?;
                if resp.status() == StatusCode::NOT_FOUND {
                    Err(ApiError::NotFound {url: u.clone() })?;
                }
                let page = resp.error_for_status()
                    .map_err(|source| ApiError::Status {url: u.clone(), source})?
                    .json::<Page<T>>()
                    .await
                    .map_err(move |source| ApiError::Deserialize {url: u, source})?;
                for r in page.results {
                    yield r;
                }
                url = page.next;
            }
        }
    }

    pub(crate) fn get_all_dandisets(&self) -> impl Stream<Item = Result<Dandiset, ApiError>> {
        self.paginate(urljoin(&self.api_url, ["dandisets"]))
    }

    pub(crate) fn dandiset<'a>(&'a self, dandiset_id: &'a DandisetId) -> DandisetEndpoint<'a> {
        DandisetEndpoint::new(self, dandiset_id)
    }
}

#[derive(Copy, Clone, Debug)]
pub(crate) struct DandisetEndpoint<'a> {
    client: &'a Client,
    dandiset_id: &'a DandisetId,
}

impl<'a> DandisetEndpoint<'a> {
    fn new(client: &'a Client, dandiset_id: &'a DandisetId) -> Self {
        Self {
            client,
            dandiset_id,
        }
    }

    pub(crate) fn version(self, version_id: &'a VersionId) -> VersionEndpoint<'a> {
        VersionEndpoint::new(self, version_id)
    }

    pub(crate) async fn get(&self) -> Result<Dandiset, ApiError> {
        self.client
            .get(urljoin(
                &self.client.api_url,
                ["dandisets", self.dandiset_id.as_ref()],
            ))
            .await
    }

    pub(crate) fn get_all_versions(&self) -> impl Stream<Item = Result<DandisetVersion, ApiError>> {
        self.client.paginate(urljoin(
            &self.client.api_url,
            ["dandisets", self.dandiset_id.as_ref(), "versions"],
        ))
    }
}

#[derive(Copy, Clone, Debug)]
pub(crate) struct VersionEndpoint<'a> {
    client: &'a Client,
    dandiset_id: &'a DandisetId,
    version_id: &'a VersionId,
}

impl<'a> VersionEndpoint<'a> {
    fn new(upper: DandisetEndpoint<'a>, version_id: &'a VersionId) -> Self {
        Self {
            client: upper.client,
            dandiset_id: upper.dandiset_id,
            version_id,
        }
    }

    pub(crate) async fn get(&self) -> Result<DandisetVersion, ApiError> {
        self.client
            .get(urljoin(
                &self.client.api_url,
                [
                    "dandisets",
                    self.dandiset_id.as_ref(),
                    "versions",
                    self.version_id.as_ref(),
                    "info",
                ],
            ))
            .await
    }

    // get_metadata
    // get_resource(path: &str, with_children: bool)
}

#[derive(Debug, Error)]
#[error("failed to initialize Dandi API client")]
pub(crate) struct BuildClientError(#[from] reqwest::Error);

#[derive(Debug, Error)]
pub(crate) enum ApiError {
    #[error("failed to make request to {url}")]
    Send { url: Url, source: reqwest::Error },
    #[error("no such resource: {url}")]
    NotFound { url: Url },
    #[error("request to {url} returned error")]
    Status { url: Url, source: reqwest::Error },
    #[error("failed to deserialize response body from {url}")]
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
