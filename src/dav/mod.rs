mod html;
mod multistatus;
mod path;
mod propfind;
mod types;
mod util;
pub(crate) use self::html::Templater;
use self::html::*;
use self::multistatus::*;
use self::path::*;
use self::propfind::*;
use self::types::*;
use self::util::*;
use crate::consts::{CSS_CONTENT_TYPE, DAV_XML_CONTENT_TYPE, HTML_CONTENT_TYPE};
use crate::dandi::*;
use axum::{
    body::Body,
    extract::Request,
    http::{header::CONTENT_TYPE, response::Response, Method, StatusCode},
    response::{IntoResponse, Redirect},
    RequestExt,
};
use futures_util::TryStreamExt;
use thiserror::Error;

const WEBDAV_RESPONSE_HEADERS: [(&str, &str); 2] = [
    ("Allow", "GET, HEAD, OPTIONS, PROPFIND"),
    // <http://www.webdav.org/specs/rfc4918.html#HEADER_DAV>
    ("DAV", "1, 3"),
];

static STYLESHEET: &str = include_str!("static/styles.css");

pub(crate) struct DandiDav {
    client: Client,
    templater: Templater,
    title: String,
}

impl DandiDav {
    pub(crate) fn new(client: Client, templater: Templater, title: String) -> DandiDav {
        DandiDav {
            client,
            templater,
            title,
        }
    }

    pub(crate) async fn handle_request(
        &self,
        mut req: Request<Body>,
    ) -> Result<Response<Body>, DavError> {
        let uri_path = req.uri().path();
        let resp = match req.method() {
            &Method::GET if uri_path == "/.static/styles.css" => {
                // Don't add WebDAV headers
                return Ok(([(CONTENT_TYPE, CSS_CONTENT_TYPE)], STYLESHEET).into_response());
            }
            &Method::GET => {
                let Some(path) = DavPath::parse_uri_path(uri_path) else {
                    return Ok(not_found());
                };
                self.get(&path, uri_path).await?
            }
            &Method::OPTIONS => StatusCode::NO_CONTENT.into_response(),
            m if m.as_str().eq_ignore_ascii_case("PROPFIND") => {
                let Some(path) = DavPath::parse_uri_path(uri_path) else {
                    return Ok(not_found());
                };
                match req.extract_parts::<FiniteDepth>().await {
                    Ok(depth) => {
                        // TODO: Extract request body
                        self.propfind(&path, depth, PropFind::default()).await?
                    }
                    Err(r) => r,
                }
            }
            _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
        };
        Ok((WEBDAV_RESPONSE_HEADERS, resp).into_response())
    }

    async fn get(&self, path: &DavPath, uri_path: &str) -> Result<Response<Body>, DavError> {
        match self.resolve_with_children(path).await? {
            DavResourceWithChildren::Collection { col, children } => {
                let mut rows = children.into_iter().map(ColRow::from).collect::<Vec<_>>();
                rows.sort_unstable();
                if path != &DavPath::Root {
                    rows.insert(0, ColRow::parentdir(col.parent_web_link()));
                }
                let context = CollectionContext {
                    title: format!("{} — {}", self.title, uri_path),
                    rows,
                    package_url: env!("CARGO_PKG_REPOSITORY"),
                    package_version: env!("CARGO_PKG_VERSION"),
                    package_commit: option_env!("GIT_COMMIT"),
                };
                let html = self.templater.render_collection(context)?;
                Ok(([(CONTENT_TYPE, HTML_CONTENT_TYPE)], html).into_response())
            }
            DavResourceWithChildren::Item(DavItem {
                content_type,
                content: DavContent::Blob(blob),
                ..
            }) => Ok(([(CONTENT_TYPE, content_type)], blob).into_response()),
            DavResourceWithChildren::Item(DavItem {
                content: DavContent::Redirect(url),
                ..
            }) => Ok(Redirect::temporary(url.as_str()).into_response()),
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
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum DavError {
    #[error("failed to fetch data from Archive")]
    DandiApi(#[from] DandiError),
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
        matches!(
            self,
            DavError::DandiApi(DandiError::NotFound { .. } | DandiError::ZarrEntryNotFound { .. })
                | DavError::NoLatestVersion { .. }
        )
    }
}

impl IntoResponse for DavError {
    fn into_response(self) -> Response<Body> {
        if self.is_404() {
            not_found()
        } else {
            let traceback = format!("{:?}\n", anyhow::Error::from(self));
            // TODO: Log error details
            (StatusCode::INTERNAL_SERVER_ERROR, traceback).into_response()
        }
    }
}

fn not_found() -> Response<Body> {
    (StatusCode::NOT_FOUND, WEBDAV_RESPONSE_HEADERS, "404\n").into_response()
}
