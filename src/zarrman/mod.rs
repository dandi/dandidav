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

mod consts;
mod fetcher;
mod manifest;
mod path;
mod resources;
mod util;
use self::consts::ENTRY_DOWNLOAD_PREFIX;
pub(crate) use self::fetcher::ManifestFetcher;
use self::path::ReqPath;
pub(crate) use self::resources::*;
pub(crate) use self::util::ZarrManError;
use crate::httputil::HttpUrl;
use crate::paths::{PureDirPath, PurePath};

/// A client for fetching data about Zarrs via Zarr manifest files
#[derive(Clone, Debug)]
pub(crate) struct ZarrManClient {
    /// The actual client for fetching & caching Zarr manifests
    fetcher: ManifestFetcher,

    /// [`ENTRY_DOWNLOAD_PREFIX`], parsed into an [`HttpUrl`]
    entry_download_prefix: HttpUrl,

    /// The directory path `"zarrs/"`, used at various points in the code,
    /// pre-parsed for convenience
    web_path_prefix: PureDirPath,
}

impl ZarrManClient {
    /// Construct a new client instance
    pub(crate) fn new(fetcher: ManifestFetcher) -> Self {
        let entry_download_prefix = ENTRY_DOWNLOAD_PREFIX
            .parse::<HttpUrl>()
            .expect("ENTRY_DOWNLOAD_PREFIX should be a valid HTTP URL");
        let web_path_prefix = "zarrs/"
            .parse::<PureDirPath>()
            .expect(r#""zarrs/" should be a valid directory path"#);
        ZarrManClient {
            fetcher,
            entry_download_prefix,
            web_path_prefix,
        }
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
                let _ = self.fetcher.fetch_manifest(&path).await?;
                Ok(ZarrManResource::Manifest(Manifest { path }))
            }
            ReqPath::InManifest {
                manifest_path,
                entry_path,
            } => {
                let man = self.fetcher.fetch_manifest(&manifest_path).await?;
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
                let man = self.fetcher.fetch_manifest(&path).await?;
                let children = self.convert_manifest_folder_children(&path, None, &man.entries);
                let folder = Manifest { path };
                Ok(ZarrManResourceWithChildren::Manifest { folder, children })
            }
            ReqPath::InManifest {
                manifest_path,
                entry_path,
            } => {
                let man = self.fetcher.fetch_manifest(&manifest_path).await?;
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
    /// `path` must be relative to the manifest root.  A `path` of `None`
    /// denotes the manifest root itself.  Unlike the `get_resource*()`
    /// methods, Zarr manifests are not transparently converted to collections.
    async fn get_index_entries(
        &self,
        path: Option<&PureDirPath>,
    ) -> Result<Vec<ZarrManResource>, ZarrManError> {
        let index = self.fetcher.fetch_index(path).await?;
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
