use time::{format_description::FormatItem, macros::format_description};

/// The value of the "User-Agent" header sent in requests to the Dandi Archive
/// and S3
pub(crate) static USER_AGENT: &str = concat!(
    env!("CARGO_PKG_NAME"),
    "/",
    env!("CARGO_PKG_VERSION"),
    " (",
    env!("CARGO_PKG_REPOSITORY"),
    ")",
);

/// The value of the "Server" header returned in all responses from dandidav
pub(crate) static SERVER_VALUE: &str =
    concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"));

pub(crate) static DEFAULT_API_URL: &str = "https://api.dandiarchive.org/api";

// Case sensitive:
pub(crate) static ZARR_EXTENSIONS: [&str; 2] = [".zarr", ".ngff"];

pub(crate) const S3CLIENT_CACHE_SIZE: u64 = 8;

pub(crate) static HTML_CONTENT_TYPE: &str = "text/html; charset=utf-8";

pub(crate) static CSS_CONTENT_TYPE: &str = "text/css; charset=utf-8";

pub(crate) static YAML_CONTENT_TYPE: &str = "text/yaml; charset=utf-8";

pub(crate) static DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";

pub(crate) static DAV_XML_CONTENT_TYPE: &str = "text/xml; charset=utf-8";

pub(crate) static DAV_XMLNS: &str = "DAV:";

pub(crate) static HTML_TIMESTAMP_FORMAT: &[FormatItem<'_>] = format_description!(
    "[year]-[month]-[day] [hour]:[minute]:[second] [offset_hour sign:mandatory]:[offset_minute]"
);

/// If a client makes a request for a resource with one of these names as a
/// component, assume it doesn't exist without checking the Archive.
///
/// This list must be kept in sorted order; this is enforced by a test below.
pub(crate) static FAST_NOT_EXIST: &[&str] = &[".bzr", ".git", ".nols", ".svn"];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fast_not_exist_is_sorted() {
        assert!(FAST_NOT_EXIST.windows(2).all(|ab| ab[0] < ab[1]));
    }
}
