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
    manifests: Arc<Cache<PurePath, Arc<manifest::Manifest>>>,
    manifest_root_url: Url,
    s3_download_prefix: Url,
    web_path_prefix: PureDirPath,
}

impl ZarrManClient {
    pub(crate) fn new() -> Result<Self, BuildClientError> {
        let inner = new_client()?;
        let manifests = Arc::new(
            CacheBuilder::new(MANIFEST_CACHE_SIZE)
                .name("zarr-manifests")
                .build(),
        );
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
            Some(p) => urljoin_slashed(&self.manifest_root_url, p.components()),
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
                                zarr_id: path.name_component(),
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

    #[allow(clippy::unused_async)]
    #[allow(unused_variables)]
    async fn get_zarr_manifest(
        &self,
        path: &ManifestPath,
    ) -> Result<manifest::Manifest, ZarrManError> {
        todo!()
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
                Ok(ZarrManResource::WebFolder(WebFolder { web_path: p }))
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

    #[allow(clippy::unused_async)]
    #[allow(unused_variables)]
    pub(crate) async fn get_resource_with_children(
        &self,
        path: &PurePath,
    ) -> Result<ZarrManResourceWithChildren, ZarrManError> {
        todo!()
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
            std::iter::once(manifest_path.zarr_id()).chain(entry_path.components()),
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
}

#[derive(Debug, Error)]
pub(crate) enum ZarrManError {
    #[error(transparent)]
    Http(#[from] HttpError),
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
            ZarrManError::Http(HttpError::NotFound { .. })
                | ZarrManError::InvalidPath { .. }
                | ZarrManError::ManifestPathNotFound { .. }
        )
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
struct Index {
    //path: String,
    files: Vec<Component>,
    directories: Vec<Component>,
}
