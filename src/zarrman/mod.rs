//! The implementation of the data source for the `/zarrs/` hierarchy
//!
//! Information about Zarrs and their entries is parsed from documents called
//! *Zarr manifests*, which are retrieved from a URL hierarchy (the *manifest
//! tree*), the base URL of which is the *manifest root*.  See `doc/zarrman.md`
//! in the source repository for information on the manifest tree API and Zarr
//! manifest format.
//!
//! The hierarchy that `dandidav` serves under `/zarrs/` mirrors the layout of
//! the manifest tree, except that Zarr manifests are replaced by collections
//! (with the same name as the corresponding Zarr manifests, but with the
//! `.json` extension changed to `.zarr`) containing the respective Zarrs'
//! entry hierarchies.

mod manifest;
mod path;
mod resources;
use self::path::ReqPath;
pub(crate) use self::resources::*;
use crate::dav::ErrorClass;
use crate::httputil::{BuildClientError, Client, HttpError, HttpUrl};
use crate::paths::{Component, PureDirPath, PurePath};
use moka::{
    future::{Cache, CacheBuilder},
    ops::compute::{CompResult, Op},
};
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;

/// The manifest root URL.
///
/// This is the base URL of the manifest tree (a URL hierarchy containing Zarr
/// manifests).
///
/// The current value is a subdirectory of a mirror of
/// <https://github.com/dandi/zarr-manifests>.
static MANIFEST_ROOT_URL: &str =
    "https://datasets.datalad.org/dandi/zarr-manifests/zarr-manifests-v2-sorted/";

/// The URL beneath which Zarr entries listed in the Zarr manifests should be
/// available for download.
///
/// Given a Zarr with Zarr ID `zarr_id` and an entry therein at path
/// `entry_path`, the download URL for the entry is expected to be
/// `{ENTRY_DOWNLOAD_PREFIX}/{zarr_id}/{entry_path}`.
static ENTRY_DOWNLOAD_PREFIX: &str = "https://dandiarchive.s3.amazonaws.com/zarr/";

/// The maximum number of manifests cached at once
const MANIFEST_CACHE_SIZE: u64 = 16;

/// A client for fetching data about Zarrs via Zarr manifest files
#[derive(Clone, Debug)]
pub(crate) struct ZarrManClient {
    /// The HTTP client used for making requests to the manifest tree
    inner: Client,

    /// A cache of parsed manifest files, keyed by their path under
    /// `MANIFEST_ROOT_URL`
    manifests: Cache<ManifestPath, Arc<manifest::Manifest>>,

    /// [`MANIFEST_ROOT_URL`], parsed into an [`HttpUrl`]
    manifest_root_url: HttpUrl,

    /// [`ENTRY_DOWNLOAD_PREFIX`], parsed into an [`HttpUrl`]
    entry_download_prefix: HttpUrl,

    /// The directory path `"zarrs/"`, used at various points in the code,
    /// pre-parsed for convenience
    web_path_prefix: PureDirPath,
}

impl ZarrManClient {
    /// Construct a new client instance
    ///
    /// # Errors
    ///
    /// Returns an error if construction of the inner `reqwest::Client` fails
    pub(crate) fn new() -> Result<Self, BuildClientError> {
        let inner = Client::new()?;
        let manifests = CacheBuilder::new(MANIFEST_CACHE_SIZE)
            .name("zarr-manifests")
            .time_to_idle(Duration::from_secs(300))
            .eviction_listener(|path, _, cause| {
                tracing::debug!(
                    event = "manifest_cache_evict",
                    manifest = ?path,
                    ?cause,
                    "Zarr manifest evicted from cache",
                );
            })
            .build();
        let manifest_root_url = MANIFEST_ROOT_URL
            .parse::<HttpUrl>()
            .expect("MANIFEST_ROOT_URL should be a valid HTTP URL");
        let entry_download_prefix = ENTRY_DOWNLOAD_PREFIX
            .parse::<HttpUrl>()
            .expect("ENTRY_DOWNLOAD_PREFIX should be a valid HTTP URL");
        let web_path_prefix = "zarrs/"
            .parse::<PureDirPath>()
            .expect(r#""zarrs/" should be a valid directory path"#);
        Ok(ZarrManClient {
            inner,
            manifests,
            manifest_root_url,
            entry_download_prefix,
            web_path_prefix,
        })
    }

    /// Retrieve the resources at the top level of `/zarrs/`, i.e., those
    /// matching the resources at the top level of the manifest tree
    pub(crate) async fn get_top_level_dirs(&self) -> Result<Vec<ZarrManResource>, ZarrManError> {
        self.get_index_entries(None).await
    }

    /// Get details on the resource at the given `path` (sans leading `zarrs/`)
    /// in the `/zarrs/` hierarchy
    ///
    /// Although `path` is a `PurePath`, the resulting resource may be a
    /// collection.
    pub(crate) async fn get_resource(
        &self,
        path: &PurePath,
    ) -> Result<ZarrManResource, ZarrManError> {
        let Some(rp) = ReqPath::parse_path(path) else {
            return Err(ZarrManError::InvalidPath { path: path.clone() });
        };
        match rp {
            ReqPath::Dir(p) => {
                // Make a request to confirm that directory exists
                let _ = self.get_index_entries(Some(&p)).await?;
                Ok(ZarrManResource::WebFolder(WebFolder {
                    web_path: self.web_path_prefix.join_dir(&p),
                }))
            }
            ReqPath::Manifest(path) => {
                // Make a request to confirm that manifest exists
                let _ = self.get_zarr_manifest(&path).await?;
                Ok(ZarrManResource::Manifest(Manifest { path }))
            }
            ReqPath::InManifest {
                manifest_path,
                entry_path,
            } => {
                let man = self.get_zarr_manifest(&manifest_path).await?;
                match man.get(&entry_path) {
                    Some(manifest::EntryRef::Folder(_)) => {
                        let web_path = manifest_path
                            .to_web_path()
                            .join_dir(&entry_path.to_dir_path());
                        Ok(ZarrManResource::ManFolder(ManifestFolder { web_path }))
                    }
                    Some(manifest::EntryRef::Entry(entry)) => Ok(ZarrManResource::ManEntry(
                        self.convert_manifest_entry(&manifest_path, &entry_path, entry),
                    )),
                    None => Err(ZarrManError::ManifestPathNotFound {
                        manifest_path,
                        entry_path,
                    }),
                }
            }
        }
    }

    /// Get details on the resource at the given `path` (sans leading `zarrs/`)
    /// in the `/zarrs/` hierarchy along with its immediate child resources (if
    /// any)
    ///
    /// Although `path` is a `PurePath`, the resulting resource may be a
    /// collection.
    pub(crate) async fn get_resource_with_children(
        &self,
        path: &PurePath,
    ) -> Result<ZarrManResourceWithChildren, ZarrManError> {
        let Some(rp) = ReqPath::parse_path(path) else {
            return Err(ZarrManError::InvalidPath { path: path.clone() });
        };
        match rp {
            ReqPath::Dir(p) => {
                let children = self.get_index_entries(Some(&p)).await?;
                let folder = WebFolder {
                    web_path: self.web_path_prefix.join_dir(&p),
                };
                Ok(ZarrManResourceWithChildren::WebFolder { folder, children })
            }
            ReqPath::Manifest(path) => {
                let man = self.get_zarr_manifest(&path).await?;
                let children = self.convert_manifest_folder_children(&path, None, &man.entries);
                let folder = Manifest { path };
                Ok(ZarrManResourceWithChildren::Manifest { folder, children })
            }
            ReqPath::InManifest {
                manifest_path,
                entry_path,
            } => {
                let man = self.get_zarr_manifest(&manifest_path).await?;
                match man.get(&entry_path) {
                    Some(manifest::EntryRef::Folder(folref)) => {
                        let web_path = manifest_path
                            .to_web_path()
                            .join_dir(&entry_path.to_dir_path());
                        let children = self.convert_manifest_folder_children(
                            &manifest_path,
                            Some(&entry_path),
                            folref,
                        );
                        let folder = ManifestFolder { web_path };
                        Ok(ZarrManResourceWithChildren::ManFolder { folder, children })
                    }
                    Some(manifest::EntryRef::Entry(entry)) => {
                        Ok(ZarrManResourceWithChildren::ManEntry(
                            self.convert_manifest_entry(&manifest_path, &entry_path, entry),
                        ))
                    }
                    None => Err(ZarrManError::ManifestPathNotFound {
                        manifest_path,
                        entry_path,
                    }),
                }
            }
        }
    }

    /// Retrieve the resources in the given directory of the manifest tree.
    ///
    /// `path` must be relative to the manifest root.  Unlike the
    /// `get_resource*()` methods, Zarr manifests are not transparently
    /// converted to collections.
    async fn get_index_entries(
        &self,
        path: Option<&PureDirPath>,
    ) -> Result<Vec<ZarrManResource>, ZarrManError> {
        let mut url = self.manifest_root_url.clone();
        if let Some(p) = path {
            url.extend(p.component_strs()).ensure_dirpath();
        }
        let index = self.inner.get_json::<Index>(url).await?;
        let mut entries =
            Vec::with_capacity(index.files.len().saturating_add(index.directories.len()));
        if let Some(path) = path {
            if let Some(prefix) = path.parent() {
                for f in index.files {
                    // This calls Component::strip_suffix(), so `checksum` is
                    // guaranteed to be non-empty.
                    let Some(checksum) = f.strip_suffix(".json") else {
                        // Ignore
                        continue;
                    };
                    if !checksum.contains('.') {
                        entries.push(ZarrManResource::Manifest(Manifest {
                            path: ManifestPath {
                                prefix: prefix.clone(),
                                zarr_id: path.name(),
                                checksum,
                            },
                        }));
                    }
                    // else: Ignore
                }
            }
        }
        // else: Ignore
        let web_path_prefix = match path {
            Some(p) => self.web_path_prefix.join_dir(p),
            None => self.web_path_prefix.clone(),
        };
        for d in index.directories {
            let web_path = web_path_prefix.join_one_dir(&d);
            entries.push(ZarrManResource::WebFolder(WebFolder { web_path }));
        }
        Ok(entries)
    }

    /// Retrieve the Zarr manifest at the given [`ManifestPath`] in the
    /// manifest tree, either via an HTTP request or from a cache
    async fn get_zarr_manifest(
        &self,
        path: &ManifestPath,
    ) -> Result<Arc<manifest::Manifest>, ZarrManError> {
        let result = self
            .manifests
            .entry_by_ref(path)
            .and_try_compute_with(|entry| async move {
                if entry.is_none() {
                    tracing::debug!(
                        event = "manifest_cache_miss_pre",
                        manifest = ?path,
                        cache_len = self.manifests.entry_count(),
                        "Cache miss for Zarr manifest; about to fetch from repository",
                    );
                    self.inner
                        .get_json::<manifest::Manifest>(
                            path.under_manifest_root(&self.manifest_root_url),
                        )
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
                    event = "manifest_cache_miss_post",
                    manifest = ?path,
                    cache_len = self.manifests.entry_count(),
                    "Fetched Zarr manifest from repository",
                );
                entry
            }
            CompResult::Unchanged(entry) => {
                tracing::debug!(
                    event = "manifest_cache_hit",
                    manifest = ?path,
                    cache_len = self.manifests.entry_count(),
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

    /// Convert the [`manifest::ManifestEntry`] `entry` with path `entry_path`
    /// in the manifest at `manifest_path` to a [`ManifestEntry`].
    ///
    /// This largely consists of calculating the `web_path` and `url` fields of
    /// the entry.
    fn convert_manifest_entry(
        &self,
        manifest_path: &ManifestPath,
        entry_path: &PurePath,
        entry: &manifest::ManifestEntry,
    ) -> ManifestEntry {
        let web_path = manifest_path.to_web_path().join(entry_path);
        let mut url = self.entry_download_prefix.clone();
        url.push(manifest_path.zarr_id());
        url.extend(entry_path.component_strs());
        url.append_query_param("versionId", &entry.version_id);
        ManifestEntry {
            web_path,
            size: entry.size,
            modified: entry.modified,
            etag: entry.etag.clone(),
            url,
        }
    }

    /// Convert the entries in `folder` (a folder at path `folder_path` in the
    /// manifest at `manifest_path`) to [`ZarrManResource`]s
    fn convert_manifest_folder_children(
        &self,
        manifest_path: &ManifestPath,
        folder_path: Option<&PurePath>,
        folder: &manifest::ManifestFolder,
    ) -> Vec<ZarrManResource> {
        let mut children = Vec::with_capacity(folder.len());
        let web_path_prefix = match folder_path {
            Some(p) => manifest_path.to_web_path().join_dir(&p.to_dir_path()),
            None => manifest_path.to_web_path(),
        };
        for (name, child) in folder {
            match child {
                manifest::FolderEntry::Folder(_) => {
                    children.push(ZarrManResource::ManFolder(ManifestFolder {
                        web_path: web_path_prefix.join_one_dir(name),
                    }));
                }
                manifest::FolderEntry::Entry(entry) => {
                    let entry_path = match folder_path {
                        Some(p) => p.join_one(name),
                        None => PurePath::from(name.clone()),
                    };
                    children.push(ZarrManResource::ManEntry(self.convert_manifest_entry(
                        manifest_path,
                        &entry_path,
                        entry,
                    )));
                }
            }
        }
        children
    }
}

#[derive(Debug, Error)]
pub(crate) enum ZarrManError {
    /// An HTTP error occurred while interacting with the manifest tree
    #[error(transparent)]
    Http(#[from] HttpError),

    /// The request path was invalid for the `/zarrs/` hierarchy
    #[error("invalid path requested: {path:?}")]
    InvalidPath { path: PurePath },

    /// An request was made for a nonexistent path inside an extant Zarr
    #[error("path {entry_path:?} inside manifest at {manifest_path:?} does not exist")]
    ManifestPathNotFound {
        manifest_path: ManifestPath,
        entry_path: PurePath,
    },
}

impl ZarrManError {
    /// Classify the general type of error
    pub(crate) fn class(&self) -> ErrorClass {
        match self {
            ZarrManError::Http(source) => source.class(),
            ZarrManError::InvalidPath { .. } | ZarrManError::ManifestPathNotFound { .. } => {
                ErrorClass::NotFound
            }
        }
    }
}

/// A directory listing parsed from the response to a `GET` request to a
/// directory in the manifest tree
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
struct Index {
    // Returned by the manifest tree API but not used by dandidav:
    //path: String,
    /// The names of the files in the directory
    files: Vec<Component>,
    /// The names of the subdirectories of the directory
    directories: Vec<Component>,
}
