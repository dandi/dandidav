mod html;
mod path;
mod types;
mod util;
mod xml;
pub(crate) use self::html::Templater;
use self::html::*;
use self::path::*;
use self::types::*;
use self::util::*;
use self::xml::*;
use crate::consts::{DAV_XML_CONTENT_TYPE, HTML_CONTENT_TYPE};
use crate::dandi::*;
use crate::paths::Component;
use crate::zarrman::*;
use axum::{
    body::Body,
    extract::Request,
    http::{header::CONTENT_TYPE, response::Response, Method, StatusCode},
    response::{IntoResponse, Redirect},
    RequestExt,
};
use futures_util::TryStreamExt;
use std::convert::Infallible;
use thiserror::Error;

const WEBDAV_RESPONSE_HEADERS: [(&str, &str); 2] = [
    ("Allow", "GET, HEAD, OPTIONS, PROPFIND"),
    // <http://www.webdav.org/specs/rfc4918.html#HEADER_DAV>
    ("DAV", "1, 3"),
];

pub(crate) struct DandiDav {
    pub(crate) dandi: DandiClient,
    pub(crate) zarrman: ZarrManClient,
    pub(crate) templater: Templater,
    pub(crate) title: String,
    pub(crate) prefer_s3_redirects: bool,
}

impl DandiDav {
    pub(crate) async fn handle_request(
        &self,
        req: Request<Body>,
    ) -> Result<Response<Body>, Infallible> {
        // Box large future:
        let resp = match Box::pin(self.inner_handle_request(req)).await {
            Ok(r) => r,
            Err(e) if e.is_404() => {
                let e = anyhow::Error::from(e);
                tracing::info!(error = ?e, "Resource not found");
                not_found()
            }
            Err(e) => {
                let e = anyhow::Error::from(e);
                tracing::error!(error = ?e, "Internal server error");
                (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:?}")).into_response()
            }
        };
        Ok((WEBDAV_RESPONSE_HEADERS, resp).into_response())
    }

    async fn inner_handle_request(&self, req: Request<Body>) -> Result<Response<Body>, DavError> {
        let uri_path = req.uri().path();
        match req.method() {
            &Method::GET => {
                let Some(parts) = split_uri_path(uri_path) else {
                    return Ok(not_found());
                };
                let Some(path) = DavPath::from_components(parts.clone()) else {
                    return Ok(not_found());
                };
                self.get(&path, parts).await
            }
            &Method::OPTIONS => Ok(StatusCode::NO_CONTENT.into_response()),
            m if m.as_str().eq_ignore_ascii_case("PROPFIND") => {
                let Some(path) = split_uri_path(uri_path).and_then(DavPath::from_components) else {
                    return Ok(not_found());
                };
                match req.extract::<(FiniteDepth, PropFind), _>().await {
                    Ok((depth, pf)) => self.propfind(&path, depth, pf).await,
                    Err(r) => Ok(r),
                }
            }
            _ => Ok(StatusCode::METHOD_NOT_ALLOWED.into_response()),
        }
    }

    async fn get(
        &self,
        path: &DavPath,
        pathparts: Vec<Component>,
    ) -> Result<Response<Body>, DavError> {
        match self.resolve_with_children(path).await? {
            DavResourceWithChildren::Collection { children, .. } => {
                let context = CollectionContext::new(children, &self.title, pathparts);
                let html = self.templater.render_collection(context)?;
                Ok(([(CONTENT_TYPE, HTML_CONTENT_TYPE)], html).into_response())
            }
            DavResourceWithChildren::Item(DavItem {
                content_type,
                content: DavContent::Blob(blob),
                ..
            }) => Ok(([(CONTENT_TYPE, content_type)], blob).into_response()),
            DavResourceWithChildren::Item(DavItem {
                content: DavContent::Redirect(redir),
                ..
            }) => Ok(
                Redirect::temporary(redir.get_url(self.prefer_s3_redirects).as_str())
                    .into_response(),
            ),
            DavResourceWithChildren::Item(DavItem {
                content: DavContent::Missing,
                ..
            }) => Ok(not_found()),
        }
    }

    async fn propfind(
        &self,
        path: &DavPath,
        depth: FiniteDepth,
        query: PropFind,
    ) -> Result<Response<Body>, DavError> {
        let resources = match depth {
            FiniteDepth::Zero => vec![self.resolve(path).await?],
            FiniteDepth::One => match self.resolve_with_children(path).await? {
                DavResourceWithChildren::Collection { col, children } => {
                    let mut reses = Vec::with_capacity(children.len().saturating_add(1));
                    reses.push(DavResource::from(col));
                    reses.extend(children);
                    reses
                }
                DavResourceWithChildren::Item(item) => vec![DavResource::Item(item)],
            },
        };
        let response = resources
            .into_iter()
            .map(|r| query.find(&r))
            .collect::<Vec<_>>();
        Ok((
            StatusCode::MULTI_STATUS,
            [(CONTENT_TYPE, DAV_XML_CONTENT_TYPE)],
            (Multistatus { response }).to_xml()?,
        )
            .into_response())
    }

    async fn get_version_endpoint(
        &self,
        dandiset_id: &DandisetId,
        version: &VersionSpec,
    ) -> Result<VersionEndpoint<'_>, DavError> {
        let d = self.dandi.dandiset(dandiset_id.clone());
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
                let ds = self.dandi.dandiset(dandiset_id.clone()).get().await?;
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
            DavPath::ZarrIndex => Ok(DavResource::Collection(DavCollection::zarr_index())),
            DavPath::ZarrPath { path } => {
                let res = self.zarrman.get_resource(path).await?;
                Ok(DavResource::from(res))
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
                let stream = self.dandi.get_all_dandisets();
                tokio::pin!(stream);
                while let Some(ds) = stream.try_next().await? {
                    children.push(DavResource::Collection(ds.into()));
                }
                Ok(DavResourceWithChildren::Collection { col, children })
            }
            DavPath::Dandiset { dandiset_id } => {
                let mut ds = self.dandi.dandiset(dandiset_id.clone()).get().await?;
                let draft = DavResource::Collection(DavCollection::dandiset_version(
                    ds.draft_version.clone(),
                    version_path(dandiset_id, &VersionSpec::Draft),
                ));
                let children = match ds.most_recent_published_version.take() {
                    Some(v) => {
                        let latest = DavCollection::dandiset_version(
                            v,
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
                let endpoint = self.dandi.dandiset(dandiset_id.clone());
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
                children.push(
                    self.get_dandiset_yaml(dandiset_id, version)
                        .await
                        .map(DavResource::Item)?,
                );
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
            DavPath::ZarrIndex => {
                let col = DavCollection::zarr_index();
                let children = self
                    .zarrman
                    .get_top_level_dirs()
                    .await?
                    .into_iter()
                    .map(DavResource::from)
                    .collect();
                Ok(DavResourceWithChildren::Collection { col, children })
            }
            DavPath::ZarrPath { path } => {
                let res = self.zarrman.get_resource_with_children(path).await?;
                Ok(DavResourceWithChildren::from(res))
            }
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum DavError {
    #[error("failed to fetch data from Archive")]
    Dandi(#[from] DandiError),
    #[error("failed to fetch data from Zarr manifests")]
    ZarrMan(#[from] ZarrManError),
    #[error(
        "latest version was requested for Dandiset {dandiset_id}, but it has not been published"
    )]
    NoLatestVersion { dandiset_id: DandisetId },
    #[error(transparent)]
    Template(#[from] TemplateError),
    #[error(transparent)]
    Xml(#[from] ToXmlError),
}

impl DavError {
    pub(crate) fn is_404(&self) -> bool {
        match self {
            DavError::Dandi(e) => e.is_404(),
            DavError::ZarrMan(e) => e.is_404(),
            DavError::NoLatestVersion { .. } => true,
            _ => false,
        }
    }
}

fn not_found() -> Response<Body> {
    (StatusCode::NOT_FOUND, "404\n").into_response()
}
