mod html;
mod types;
mod util;
use self::types::*;
use self::util::*;
use crate::dandi::*;
use crate::paths::PurePath;
use axum::{
    body::Body,
    http::StatusCode,
    response::{IntoResponse, Redirect},
};
use futures_util::TryStreamExt;
use http::response::Response;
use thiserror::Error;

pub(crate) struct DandiDav {
    client: Client,
    title: String,
}

impl DandiDav {
    pub(crate) fn new(client: Client, title: String) -> DandiDav {
        DandiDav { client, title }
    }

    pub(crate) async fn get(&self, path: &DavPath) -> Result<Response<Body>, DavError> {
        match self.resolve_with_children(path).await? {
            DavResourceWithChildren::Collection { children, .. } => {
                // Render HTML table
                todo!()
            }
            DavResourceWithChildren::Item(DavItem {
                content_type,
                content: DavContent::Blob(blob),
                ..
            }) => Ok(([("Content-Type", content_type)], blob).into_response()),
            DavResourceWithChildren::Item(DavItem {
                content: DavContent::Redirect(url),
                ..
            }) => Ok(Redirect::temporary(url.as_str()).into_response()),
            DavResourceWithChildren::Item(DavItem {
                content: DavContent::Missing,
                ..
            }) => Ok(StatusCode::NOT_FOUND.into_response()),
        }
    }

    #[allow(clippy::unused_async)]
    pub(crate) async fn propfind(
        &self,
        path: &DavPath,
        depth1: bool,
        body: Option<Propfind>,
    ) -> Result<Response<Body>, DavError> {
        todo!()
    }

    async fn get_version_endpoint(
        &self,
        dandiset_id: &DandisetId,
        version: &VersionSpec,
    ) -> Result<VersionEndpoint<'_>, DavError> {
        let d = self.client.dandiset(dandiset_id.clone());
        match version {
            VersionSpec::Draft => Ok(d.version(VersionId::Draft)),
            VersionSpec::Published(v) => Ok(d.version(VersionId::Published(v.clone()))),
            VersionSpec::Latest => match d.get().await?.most_recent_published_version {
                Some(DandisetVersion { version, .. }) => Ok(d.version(version)),
                None => Err(DavError::NoLatestVersion {
                    dandiset_id: dandiset_id.clone(),
                }),
            },
        }
    }

    async fn get_dandiset_yaml(
        &self,
        dandiset_id: &DandisetId,
        version: &VersionSpec,
    ) -> Result<DavItem, DavError> {
        let md = self
            .get_version_endpoint(dandiset_id, version)
            .await?
            .get_metadata()
            .await?;
        Ok(DavItem::from(md).under_version_path(dandiset_id, version))
    }

    async fn get_dandiset_version(
        &self,
        dandiset_id: &DandisetId,
        version: &VersionSpec,
    ) -> Result<(DavCollection, VersionEndpoint<'_>), DavError> {
        let endpoint = self.get_version_endpoint(dandiset_id, version).await?;
        let v = endpoint.get().await?;
        let path = version_path(dandiset_id, version);
        let col = DavCollection::dandiset_version(v, path);
        Ok((col, endpoint))
    }

    async fn resolve(&self, path: &DavPath) -> Result<DavResource, DavError> {
        match path {
            DavPath::Root => Ok(DavResource::root()),
            DavPath::DandisetIndex => Ok(DavResource::Collection(DavCollection::dandiset_index())),
            DavPath::Dandiset { dandiset_id } => {
                let ds = self.client.dandiset(dandiset_id.clone()).get().await?;
                Ok(DavResource::Collection(ds.into()))
            }
            DavPath::DandisetReleases { dandiset_id } => {
                // TODO: Should this return a 404 when the Dandiset doesn't
                // have any published releases?
                Ok(DavResource::Collection(DavCollection::dandiset_releases(
                    dandiset_id,
                )))
            }
            DavPath::Version {
                dandiset_id,
                version,
            } => self
                .get_dandiset_version(dandiset_id, version)
                .await
                .map(|(col, _)| DavResource::Collection(col)),
            DavPath::DandisetYaml {
                dandiset_id,
                version,
            } => self
                .get_dandiset_yaml(dandiset_id, version)
                .await
                .map(DavResource::Item),
            DavPath::DandiResource {
                dandiset_id,
                version,
                path,
            } => {
                let res = self
                    .get_version_endpoint(dandiset_id, version)
                    .await?
                    .get_resource(path)
                    .await?;
                Ok(DavResource::from(res).under_version_path(dandiset_id, version))
            }
        }
    }

    async fn resolve_with_children(
        &self,
        path: &DavPath,
    ) -> Result<DavResourceWithChildren, DavError> {
        match path {
            DavPath::Root => Ok(DavResourceWithChildren::root()),
            DavPath::DandisetIndex => {
                let col = DavCollection::dandiset_index();
                let mut children = Vec::new();
                let stream = self.client.get_all_dandisets();
                tokio::pin!(stream);
                while let Some(ds) = stream.try_next().await? {
                    children.push(DavResource::Collection(ds.into()));
                }
                Ok(DavResourceWithChildren::Collection { col, children })
            }
            DavPath::Dandiset { dandiset_id } => {
                let ds = self.client.dandiset(dandiset_id.clone()).get().await?;
                let draft = DavResource::Collection(DavCollection::dandiset_version(
                    ds.draft_version.clone(),
                    version_path(dandiset_id, &VersionSpec::Draft),
                ));
                let children = match ds.most_recent_published_version {
                    Some(ref v) => {
                        let latest = DavCollection::dandiset_version(
                            v.clone(),
                            version_path(dandiset_id, &VersionSpec::Latest),
                        );
                        let latest = DavResource::Collection(latest);
                        let releases =
                            DavResource::Collection(DavCollection::dandiset_releases(dandiset_id));
                        vec![draft, latest, releases]
                    }
                    None => vec![draft],
                };
                let col = DavCollection::from(ds);
                Ok(DavResourceWithChildren::Collection { col, children })
            }
            DavPath::DandisetReleases { dandiset_id } => {
                // TODO: Should this return a 404 when the Dandiset doesn't
                // have any published releases?
                let col = DavCollection::dandiset_releases(dandiset_id);
                let mut children = Vec::new();
                let endpoint = self.client.dandiset(dandiset_id.clone());
                let stream = endpoint.get_all_versions();
                tokio::pin!(stream);
                while let Some(v) = stream.try_next().await? {
                    if let VersionId::Published(ref pvid) = v.version {
                        let path = version_path(dandiset_id, &VersionSpec::Published(pvid.clone()));
                        children.push(DavResource::Collection(DavCollection::dandiset_version(
                            v, path,
                        )));
                    }
                }
                Ok(DavResourceWithChildren::Collection { col, children })
            }
            DavPath::Version {
                dandiset_id,
                version,
            } => {
                let (col, endpoint) = self.get_dandiset_version(dandiset_id, version).await?;
                let mut children = Vec::new();
                let stream = endpoint.get_root_children();
                tokio::pin!(stream);
                while let Some(res) = stream.try_next().await? {
                    children.push(DavResource::from(res).under_version_path(dandiset_id, version));
                }
                Ok(DavResourceWithChildren::Collection { col, children })
            }
            DavPath::DandisetYaml {
                dandiset_id,
                version,
            } => self
                .get_dandiset_yaml(dandiset_id, version)
                .await
                .map(DavResourceWithChildren::Item),
            DavPath::DandiResource {
                dandiset_id,
                version,
                path,
            } => {
                let res = self
                    .get_version_endpoint(dandiset_id, version)
                    .await?
                    .get_resource_with_children(path)
                    .await?;
                Ok(DavResourceWithChildren::from(res).under_version_path(dandiset_id, version))
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DavPath {
    Root,
    DandisetIndex,
    Dandiset {
        dandiset_id: DandisetId,
    },
    DandisetReleases {
        dandiset_id: DandisetId,
    },
    Version {
        dandiset_id: DandisetId,
        version: VersionSpec,
    },
    DandisetYaml {
        dandiset_id: DandisetId,
        version: VersionSpec,
    },
    DandiResource {
        dandiset_id: DandisetId,
        version: VersionSpec,
        path: PurePath,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum VersionSpec {
    Draft,
    Published(PublishedVersionId),
    Latest,
}

#[derive(Debug, Error)]
pub(crate) enum DavError {
    #[error("failed to fetch data from Archive")]
    DandiApi(#[from] DandiError),
    #[error(
        "latest version was requested for Dandiset {dandiset_id}, but it has not been published"
    )]
    NoLatestVersion { dandiset_id: DandisetId },
}

impl DavError {
    pub(crate) fn is_404(&self) -> bool {
        matches!(
            self,
            DavError::DandiApi(DandiError::NotFound { .. } | DandiError::ZarrEntryNotFound { .. })
                | DavError::NoLatestVersion { .. }
        )
    }
}

pub(crate) struct Propfind; // TODO
