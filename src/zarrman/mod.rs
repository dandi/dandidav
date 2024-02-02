mod index;
mod manifest;
mod resources;
use self::index::*;
pub(crate) use self::resources::*;
use crate::httputil::{new_client, urljoin_slashed, BuildClientError};
use crate::paths::{PureDirPath, PurePath};
use moka::future::{Cache, CacheBuilder};
use reqwest::StatusCode;
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
        let entries = self.get_index_entries(None).await?;
        Ok(entries
            .into_iter()
            .map(|path| {
                ZarrManResource::WebFolder(WebFolder {
                    web_path: path.to_dir_path(),
                })
            })
            .collect())
    }

    async fn get_index_entries(
        &self,
        path: Option<&PureDirPath>,
    ) -> Result<Vec<PurePath>, ZarrManError> {
        let mut url =
            Url::parse(MANIFEST_ROOT_URL).expect("MANIFEST_ROOT_URL should be a valid URL");
        if let Some(p) = path {
            url = urljoin_slashed(&url, p.components());
        }
        let r = self
            .inner
            .get(url.clone())
            .send()
            .await
            .map_err(|source| ZarrManError::Send {
                url: url.clone(),
                source,
            })?;
        if r.status() == StatusCode::NOT_FOUND {
            return Err(ZarrManError::NotFound { url: url.clone() });
        }
        let txt = r
            .error_for_status()
            .map_err(|source| ZarrManError::Status {
                url: url.clone(),
                source,
            })?
            .text()
            .await
            .map_err(|source| ZarrManError::Read {
                url: url.clone(),
                source,
            })?;
        parse_apache_index(&txt).map_err(|source| ZarrManError::ParseIndex {
            url: url.clone(),
            source,
        })
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
    #[error("failed to make request to {url}")]
    Send { url: Url, source: reqwest::Error },
    #[error("no such resource: {url}")]
    NotFound { url: Url },
    #[error("request to {url} returned error")]
    Status { url: Url, source: reqwest::Error },
    #[error("failed to read response from {url}")]
    Read { url: Url, source: reqwest::Error },
    #[error("failed to parse Apache index at {url}")]
    ParseIndex { url: Url, source: ParseIndexError },
    #[error("invalid path requested: {path:?}")]
    InvalidPath { path: PurePath },
}
