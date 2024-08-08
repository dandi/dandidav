//! Constants and compile-time configuration for the `/zarrs/` hierarchy
use std::time::Duration;

/// The manifest root URL.
///
/// This is the base URL of the manifest tree (a URL hierarchy containing Zarr
/// manifests).
///
/// The current value is a subdirectory of a mirror of
/// <https://github.com/dandi/zarr-manifests>.
pub(super) static MANIFEST_ROOT_URL: &str =
    "https://datasets.datalad.org/dandi/zarr-manifests/zarr-manifests-v2-sorted/";

/// The URL beneath which Zarr entries listed in the Zarr manifests should be
/// available for download.
///
/// Given a Zarr with Zarr ID `zarr_id` and an entry therein at path
/// `entry_path`, the download URL for the entry is expected to be
/// `{ENTRY_DOWNLOAD_PREFIX}/{zarr_id}/{entry_path}`.
pub(super) static ENTRY_DOWNLOAD_PREFIX: &str = "https://dandiarchive.s3.amazonaws.com/zarr/";

/// Expire any manifest cache entries that haven't been accessed for this long
pub(super) const MANIFEST_CACHE_IDLE_EXPIRY: Duration = Duration::from_secs(300);
