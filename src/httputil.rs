use crate::consts::USER_AGENT;
use reqwest::ClientBuilder;
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
    use rstest::rstest;

    #[rstest]
    #[case("https://api.github.com")]
    #[case("https://api.github.com/")]
    fn test_urljoin_slashed_nopath(#[case] base: Url) {
        let u = urljoin_slashed(&base, ["foo"]);
        assert_eq!(u.as_str(), "https://api.github.com/foo/");
        let u = urljoin_slashed(&base, ["foo", "bar"]);
        assert_eq!(u.as_str(), "https://api.github.com/foo/bar/");
    }

    #[rstest]
    #[case("https://api.github.com/foo/bar")]
    #[case("https://api.github.com/foo/bar/")]
    fn test_urljoin_slashed_path(#[case] base: Url) {
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
    fn test_urljoin_slashed_special_chars(#[case] path: &str, #[case] expected: &str) {
        let base = Url::parse("https://api.github.com/base").unwrap();
        let u = urljoin_slashed(&base, [path]);
        assert_eq!(u.as_str(), expected);
    }
}
