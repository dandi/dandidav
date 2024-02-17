use time::{format_description::FormatItem, macros::format_description};

/// The "User-Agent" value sent in outgoing HTTP requests
pub(crate) static USER_AGENT: &str = concat!(
    env!("CARGO_PKG_NAME"),
    "/",
    env!("CARGO_PKG_VERSION"),
    " (",
    env!("CARGO_PKG_REPOSITORY"),
    ")",
);

/// The "Server" value returned in all responses from dandidav
pub(crate) static SERVER_VALUE: &str =
    concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"));

/// The default value of the `--api-url` command-line option
pub(crate) static DEFAULT_API_URL: &str = "https://api.dandiarchive.org/api";

/// File extensions (case sensitive) for Zarrs, including the leading periods
pub(crate) static ZARR_EXTENSIONS: [&str; 2] = [".zarr", ".ngff"];

/// The maximum number of S3 clients cached at once by `DandiClient`
pub(crate) const S3CLIENT_CACHE_SIZE: u64 = 8;

/// The "Content-Type" value for HTML responses to `GET` requests for
/// collections
pub(crate) static HTML_CONTENT_TYPE: &str = "text/html; charset=utf-8";

/// The "Content-Type" value for the stylesheet
pub(crate) static CSS_CONTENT_TYPE: &str = "text/css; charset=utf-8";

/// The "Content-Type" value (reported in both `GET` and `PROPFIND` responses)
/// for virtual `dandiset.yaml` files
pub(crate) static YAML_CONTENT_TYPE: &str = "text/yaml; charset=utf-8";

/// The "Content-Type" value given in `PROPFIND` responses for blob assets with
/// no `encodingFormat` set and also for Zarr entries
pub(crate) static DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";

/// The "Content-Type" value for `PROPFIND` XML responses
pub(crate) static DAV_XML_CONTENT_TYPE: &str = "text/xml; charset=utf-8";

/// The XML namespace for standard WebDAV elements
pub(crate) static DAV_XMLNS: &str = "DAV:";

/// The display format for timestamps shown in collections' HTML views (after
/// converting to UTC)
pub(crate) static HTML_TIMESTAMP_FORMAT: &[FormatItem<'_>] =
    format_description!("[year]-[month]-[day] [hour]:[minute]:[second]Z");

/// If a client makes a request for a resource with one of these names as a
/// component (case insensitive), assume it doesn't exist without bothering to
/// check the backend.
///
/// This list must be kept in sorted order; this is enforced by a test below.
pub(crate) static FAST_NOT_EXIST: &[&str] = &[".bzr", ".git", ".nols", ".svn"];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fast_not_exist_is_sorted() {
        assert!(FAST_NOT_EXIST.windows(2).all(|ab| {
            assert!(ab.len() == 2);
            ab[0] < ab[1]
        }));
    }
}
