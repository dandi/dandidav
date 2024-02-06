mod manifest;
mod path;
mod resources;
use self::path::ReqPath;
pub(crate) use self::resources::*;
use crate::httputil::{
    get_json, new_client, urljoin, urljoin_slashed, BuildClientError, HttpError,
};
use crate::paths::{Component, PureDirPath, PurePath};
use moka::future::{Cache, CacheBuilder};
use serde::Deserialize;
use std::sync::Arc;
use thiserror::Error;
use url::Url;

static MANIFEST_ROOT_URL: &str =
    "https://datasets.datalad.org/dandi/zarr-manifests/zarr-manifests-v2-sorted/";

static S3_DOWNLOAD_PREFIX: &str = "https://dandiarchive.s3.amazonaws.com/zarr/";

const MANIFEST_CACHE_SIZE: u64 = 16;

#[derive(Clone, Debug)]
pub(crate) struct ZarrManClient {
    inner: reqwest::Client,
    manifests: Cache<ManifestPath, Arc<manifest::Manifest>>,
    manifest_root_url: Url,
    s3_download_prefix: Url,
    web_path_prefix: PureDirPath,
}

impl ZarrManClient {
    pub(crate) fn new() -> Result<Self, BuildClientError> {
        let inner = new_client()?;
        let manifests = CacheBuilder::new(MANIFEST_CACHE_SIZE)
            .name("zarr-manifests")
            .build();
        let manifest_root_url =
            Url::parse(MANIFEST_ROOT_URL).expect("MANIFEST_ROOT_URL should be a valid URL");
        let s3_download_prefix =
            Url::parse(S3_DOWNLOAD_PREFIX).expect("S3_DOWNLOAD_PREFIX should be a valid URL");
        let web_path_prefix = "zarrs/"
            .parse::<PureDirPath>()
            .expect(r#""zarrs/" should be a valid URL"#);
        Ok(ZarrManClient {
            inner,
            manifests,
            manifest_root_url,
            s3_download_prefix,
            web_path_prefix,
        })
    }

    pub(crate) async fn get_top_level_dirs(&self) -> Result<Vec<ZarrManResource>, ZarrManError> {
        self.get_index_entries(None).await
    }

    async fn get_index_entries(
        &self,
        path: Option<&PureDirPath>,
    ) -> Result<Vec<ZarrManResource>, ZarrManError> {
        let url = match path {
            Some(p) => urljoin_slashed(&self.manifest_root_url, p.component_strs()),
            None => self.manifest_root_url.clone(),
        };
        let index = get_json::<Index>(&self.inner, url).await?;
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

    async fn get_zarr_manifest(
        &self,
        path: &ManifestPath,
    ) -> Result<Arc<manifest::Manifest>, ZarrManError> {
        self.manifests
            .try_get_with_by_ref(path, async move {
                get_json::<manifest::Manifest>(&self.inner, path.urljoin(&self.manifest_root_url))
                    .await
                    .map(Arc::new)
            })
            .await
            .map_err(Into::into)
    }

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

    fn convert_manifest_entry(
        &self,
        manifest_path: &ManifestPath,
        entry_path: &PurePath,
        entry: &manifest::ManifestEntry,
    ) -> ManifestEntry {
        let web_path = manifest_path.to_web_path().join(entry_path);
        let mut url = urljoin(
            &self.s3_download_prefix,
            std::iter::once(manifest_path.zarr_id()).chain(entry_path.component_strs()),
        );
        url.query_pairs_mut()
            .append_pair("versionId", &entry.version_id);
        ManifestEntry {
            web_path,
            size: entry.size,
            modified: entry.modified,
            etag: entry.etag.clone(),
            url,
        }
    }

    fn convert_manifest_folder_children(
        &self,
        manifest_path: &ManifestPath,
        entry_path: Option<&PurePath>,
        folder: &manifest::ManifestFolder,
    ) -> Vec<ZarrManResource> {
        let mut children = Vec::with_capacity(folder.len());
        let web_path_prefix = match entry_path {
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
                    let thispath = match entry_path {
                        Some(p) => p.join_one(name),
                        None => PurePath::from(name.clone()),
                    };
                    children.push(ZarrManResource::ManEntry(self.convert_manifest_entry(
                        manifest_path,
                        &thispath,
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
    #[error(transparent)]
    Http(#[from] Arc<HttpError>),
    #[error("invalid path requested: {path:?}")]
    InvalidPath { path: PurePath },
    #[error("path {entry_path:?} inside manifest at {manifest_path} does not exist")]
    ManifestPathNotFound {
        manifest_path: ManifestPath,
        entry_path: PurePath,
    },
}

impl ZarrManError {
    pub(crate) fn is_404(&self) -> bool {
        matches!(
            self,
            ZarrManError::Http(e) if matches!(**e, HttpError::NotFound {..})
        ) || matches!(
            self,
            ZarrManError::InvalidPath { .. } | ZarrManError::ManifestPathNotFound { .. }
        )
    }
}

impl From<HttpError> for ZarrManError {
    fn from(e: HttpError) -> ZarrManError {
        Arc::new(e).into()
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
struct Index {
    //path: String,
    files: Vec<Component>,
    directories: Vec<Component>,
}
