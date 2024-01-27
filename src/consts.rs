use std::num::NonZeroUsize;
use time::{format_description::FormatItem, macros::format_description};

pub(crate) static USER_AGENT: &str = concat!(
    env!("CARGO_PKG_NAME"),
    "/",
    env!("CARGO_PKG_VERSION"),
    " (",
    env!("CARGO_PKG_REPOSITORY"),
    ")",
);

pub(crate) static DEFAULT_API_URL: &str = "https://api.dandiarchive.org/api";

// Case sensitive:
pub(crate) static ZARR_EXTENSIONS: [&str; 2] = [".zarr", ".ngff"];

const S3CLIENT_CACHE_SIZE_RAW: usize = 8;

#[allow(unsafe_code)]
#[allow(clippy::assertions_on_constants)]
// <https://stackoverflow.com/q/66838439/744178>
pub(crate) const S3CLIENT_CACHE_SIZE: NonZeroUsize = {
    assert!(
        S3CLIENT_CACHE_SIZE_RAW != 0,
        "cache size should not be zero"
    );
    // SAFETY: Cache size is not zero
    unsafe { NonZeroUsize::new_unchecked(S3CLIENT_CACHE_SIZE_RAW) }
};

pub(crate) static HTML_CONTENT_TYPE: &str = "text/html; charset=utf-8";

pub(crate) static YAML_CONTENT_TYPE: &str = "text/yaml; charset=utf-8";

pub(crate) static DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";

pub(crate) static HTML_TIMESTAMP_FORMAT: &[FormatItem<'_>] = format_description!(
    "[year]-[month]-[day] [hour]:[minute]:[second] [offset_hour sign:mandatory]:[offset_minute]"
);
