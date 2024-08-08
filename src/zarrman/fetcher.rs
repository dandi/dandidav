use super::consts::{MANIFEST_CACHE_IDLE_EXPIRY, MANIFEST_ROOT_URL};
use super::manifest::Manifest;
use super::resources::ManifestPath;
use super::util::{Index, ZarrManError};
use crate::httputil::{BuildClientError, Client, HttpError, HttpUrl};
use crate::paths::PureDirPath;
use get_size::GetSize;
use moka::{
    future::{Cache, CacheBuilder},
    ops::compute::{CompResult, Op},
};
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;

/// A client for fetching & caching data from the manifest tree
#[derive(Clone, Debug)]
pub(crate) struct ManifestFetcher {
    /// The HTTP client used for making requests to the manifest tree
    inner: Client,

    /// A cache of parsed manifest files, keyed by their path under
    /// `MANIFEST_ROOT_URL`
    cache: Cache<ManifestPath, Arc<Manifest>>,

    /// [`MANIFEST_ROOT_URL`], parsed into an [`HttpUrl`]
    manifest_root_url: HttpUrl,
}

impl ManifestFetcher {
    /// Construct a new client instance
    ///
    /// # Errors
    ///
    /// Returns an error if construction of the inner `reqwest::Client` fails
    pub(crate) fn new(cache_size: u64) -> Result<Self, BuildClientError> {
        let inner = Client::new()?;
        let cache: Cache<ManifestPath, Arc<Manifest>> = CacheBuilder::new(cache_size)
            .name("zarr-manifests")
            .weigher(|_, manifest: &Arc<Manifest>| {
                u32::try_from(manifest.get_size()).unwrap_or(u32::MAX)
            })
            .time_to_idle(MANIFEST_CACHE_IDLE_EXPIRY)
            .eviction_listener(|path, manifest, cause| {
                tracing::debug!(
                    cache_event = "evict",
                    cache = "zarr-manifests",
                    manifest = %path,
                    manifest_size = manifest.get_size(),
                    ?cause,
                    "Zarr manifest evicted from cache",
                );
            })
            .build();
        let manifest_root_url = MANIFEST_ROOT_URL
            .parse::<HttpUrl>()
            .expect("MANIFEST_ROOT_URL should be a valid HTTP URL");
        Ok(ManifestFetcher {
            inner,
            cache,
            manifest_root_url,
        })
    }

    /// Retrieve the manifest index in the given directory of the manifest
    /// tree.
    ///
    /// `path` must be relative to the manifest root.  A `path` of `None`
    /// denotes the manifest root itself.
    pub(super) async fn fetch_index(&self, path: Option<&PureDirPath>) -> Result<Index, HttpError> {
        let mut url = self.manifest_root_url.clone();
        if let Some(p) = path {
            url.extend(p.component_strs()).ensure_dirpath();
        }
        self.inner.get_json::<Index>(url).await
    }

    /// Retrieve the Zarr manifest at the given [`ManifestPath`] in the
    /// manifest tree, either via an HTTP request or from a cache
    #[tracing::instrument(skip_all, fields(id = %uuid::Uuid::new_v4(), manifest = %path))]
    pub(super) async fn fetch_manifest(
        &self,
        path: &ManifestPath,
    ) -> Result<Arc<Manifest>, ZarrManError> {
        let result = self
            .cache
            .entry_by_ref(path)
            .and_try_compute_with(|entry| async move {
                if entry.is_none() {
                    tracing::debug!(
                        cache_event = "miss_pre",
                        cache = "zarr-manifests",
                        manifest = %path,
                        approx_cache_len = self.cache.entry_count(),
                        approx_cache_size = self.cache.weighted_size(),
                        "Cache miss for Zarr manifest; about to fetch from repository",
                    );
                    self.inner
                        .get_json::<Manifest>(path.under_manifest_root(&self.manifest_root_url))
                        .await
                        .map(|zman| Op::Put(Arc::new(zman)))
                } else {
                    Ok(Op::Nop)
                }
            })
            .await?;
        let entry = match result {
            CompResult::Inserted(entry) => {
                tracing::debug!(
                    cache_event = "miss_post",
                    cache = "zarr-manifests",
                    manifest = %path,
                    manifest_size = entry.value().get_size(),
                    approx_cache_len = self.cache.entry_count(),
                    approx_cache_size = self.cache.weighted_size(),
                    "Fetched Zarr manifest from repository",
                );
                entry
            }
            CompResult::Unchanged(entry) => {
                tracing::debug!(
                    cache_event = "hit",
                    cache = "zarr-manifests",
                    manifest = %path,
                    manifest_size = entry.value().get_size(),
                    approx_cache_len = self.cache.entry_count(),
                    approx_cache_size = self.cache.weighted_size(),
                    "Fetched Zarr manifest from cache",
                );
                entry
            }
            _ => unreachable!(
                "Call to and_try_compute_with() should only ever return Inserted or Unchanged"
            ),
        };
        Ok(entry.into_value())
    }

    pub(crate) fn install_periodic_dump(&self, period: Duration) {
        let this = self.clone();
        let mut schedule = tokio::time::interval(period);
        schedule.reset(); // Don't tick immediately
        schedule.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        tokio::spawn({
            async move {
                loop {
                    schedule.tick().await;
                    this.log_cache();
                }
            }
        });
    }

    pub(crate) fn log_cache(&self) {
        let entries = self
            .cache
            .iter()
            .map(|(path, manifest)| EntryStat {
                manifest_path: path.to_string(),
                size: manifest.get_size(),
            })
            .collect::<Vec<_>>();
        match serde_json::to_string(&entries) {
            Ok(entries_json) => {
                tracing::debug!(
                    cache_event = "dump",
                    cache = "zarr-manifests",
                    %entries_json,
                    "Dumping cached manifests and their sizes",
                );
            }
            Err(e) => {
                tracing::warn!(
                    cache_event = "dump-error",
                    cache = "zarr-manifests",
                    error = %e,
                    "Failed to serialize cache contents as JSON",
                );
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct EntryStat {
    manifest_path: String,
    size: usize,
}
