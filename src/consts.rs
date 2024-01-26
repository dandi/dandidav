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
