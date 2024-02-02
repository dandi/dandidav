mod manifest;
mod resources;
pub(crate) use self::resources::*;
use crate::httputil::{get_json, new_client, urljoin_slashed, BuildClientError, HttpError};
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
    //TODO: manifests: Arc<Cache<(ZarrId, Checksum), Arc<Manifest>>>,
    manifests: Arc<Cache<(), Arc<Manifest>>>,
}

impl ZarrManClient {
    pub(crate) fn new() -> Result<Self, BuildClientError> {
        let inner = new_client()?;
        let manifests = Arc::new(
            CacheBuilder::new(MANIFEST_CACHE_SIZE)
                .name("zarr-manifests")
                .build(),
        );
        Ok(ZarrManClient { inner, manifests })
    }

    pub(crate) async fn get_top_level_dirs(&self) -> Result<Vec<ZarrManResource>, ZarrManError> {
        self.get_index_entries(None).await
    }

    async fn get_index_entries(
        &self,
        path: Option<&PureDirPath>,
    ) -> Result<Vec<ZarrManResource>, ZarrManError> {
        let mut url =
            Url::parse(MANIFEST_ROOT_URL).expect("MANIFEST_ROOT_URL should be a valid URL");
        if let Some(p) = path {
            url = urljoin_slashed(&url, p.components());
        }
        let index = get_json::<Index>(&self.inner, url).await?;
        let mut entries =
            Vec::with_capacity(index.files.len().saturating_add(index.directories.len()));
        for f in index.files {
            if f.ends_with(".json") && !f.ends_with(".versionid.json") {
                let web_path = match path {
                    Some(p) => p.join_one_dir(&f),
                    None => PureDirPath::from(f),
                };
                entries.push(ZarrManResource::Manifest(Manifest { web_path }));
            }
            // else: Ignore
        }
        for d in index.directories {
            let web_path = match path {
                Some(p) => p.join_one_dir(&d),
                None => PureDirPath::from(d),
            };
            entries.push(ZarrManResource::WebFolder(WebFolder { web_path }));
        }
        Ok(entries)
    }

    #[allow(clippy::unused_async)]
    #[allow(unused_variables)]
    pub(crate) async fn get_resource(
        &self,
        path: &PurePath,
    ) -> Result<ZarrManResource, ZarrManError> {
        todo!()
    }

    #[allow(clippy::unused_async)]
    #[allow(unused_variables)]
    pub(crate) async fn get_resource_with_children(
        &self,
        path: &PurePath,
    ) -> Result<ZarrManResourceWithChildren, ZarrManError> {
        todo!()
    }
}

#[derive(Debug, Error)]
pub(crate) enum ZarrManError {
    #[error(transparent)]
    Http(#[from] HttpError),
    #[error("invalid path requested: {path:?}")]
    InvalidPath { path: PurePath },
}

impl ZarrManError {
    pub(crate) fn is_404(&self) -> bool {
        matches!(
            self,
            ZarrManError::Http(HttpError::NotFound { .. }) | ZarrManError::InvalidPath { .. }
        )
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
struct Index {
    //path: String,
    files: Vec<Component>,
    directories: Vec<Component>,
}
