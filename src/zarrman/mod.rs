mod index;
mod manifest;
mod resources;
use self::index::*;
pub(crate) use self::resources::*;
use crate::httputil::{get_text, new_client, urljoin_slashed, BuildClientError, HttpError};
use crate::paths::{PureDirPath, PurePath};
use moka::future::{Cache, CacheBuilder};
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
        let txt = get_text(&self.inner, url.clone()).await?;
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
    #[error(transparent)]
    Http(#[from] HttpError),
    #[error("failed to parse Apache index at {url}")]
    ParseIndex { url: Url, source: ParseIndexError },
    #[error("invalid path requested: {path:?}")]
    InvalidPath { path: PurePath },
}
