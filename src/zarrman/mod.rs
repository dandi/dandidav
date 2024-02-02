mod manifest;
mod resources;
pub(crate) use self::resources::*;
use crate::httputil::{new_client, BuildClientError};
use crate::paths::PurePath;
use moka::future::{Cache, CacheBuilder};
use std::sync::Arc;
use thiserror::Error;

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

    #[allow(clippy::unused_async)]
    pub(crate) async fn get_top_level_dirs(&self) -> Result<Vec<WebFolder>, ZarrManError> {
        todo!()
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
#[error("TODO")]
pub(crate) struct ZarrManError; // TODO
