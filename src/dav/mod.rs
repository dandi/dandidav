//! The WebDAV component of `dandidav`
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
use crate::paths::PurePath;
use crate::zarrman::*;
use axum::{
    body::Body,
    extract::Request,
    http::{header::CONTENT_TYPE, response::Response, StatusCode},
    response::{IntoResponse, Redirect},
    RequestExt,
};
use futures_util::TryStreamExt;
use std::convert::Infallible;
use thiserror::Error;

/// HTTP headers to include in all responses for WebDAV resources
const WEBDAV_RESPONSE_HEADERS: [(&str, &str); 2] = [
    ("Allow", "GET, HEAD, OPTIONS, PROPFIND"),
    // <http://www.webdav.org/specs/rfc4918.html#HEADER_DAV>
    ("DAV", "1, 3"),
];

/// Manager for handling WebDAV requests
pub(crate) struct DandiDav {
    /// A client for fetching data from the Dandi Archive
    pub(crate) dandi: DandiClient,

    /// A client for fetching data from
    /// <https://github.com/dandi/zarr-manifests>
    pub(crate) zarrman: ZarrManClient,

    /// Manager for templating of HTML responses
    pub(crate) templater: Templater,

    /// Whether `GET` requests for blob assets should be responded to with
    /// redirects to S3 (`true`) or to Archive download URLs that then redirect
    /// to S3 (`false`).  The latter setting results in the final response
    /// having a `Content-Disposition` header, so that the blob is downloaded
    /// to the same filename as the asset, rather than to a file named after
    /// the blob ID.  On the other hand, certain WebDAV clients (i.e., davfs2)
    /// do not support multi-step redirects, so setting this to `true` is
    /// necessary to allow such clients to download from `dandidav`.
    pub(crate) prefer_s3_redirects: bool,
}

impl DandiDav {
    /// Handle an incoming HTTP request and return a response.  This method
    /// must return `Result<T, Infallible>` for compatibility with `axum`.
    ///
    /// The request parameters from the URL path and (for `PROPFIND`) "Depth"
    /// header & request body are parsed & extracted and then passed to the
    /// appropriate method for the request's verb for dedicated handling.
    ///
    /// Any errors returned are logged and converted to 4xx or 5xx responses,
    /// as appropriate.  The final response also has
    /// [`WEBDAV_RESPONSE_HEADERS`] added.
    pub(crate) async fn handle_request(
        &self,
        req: Request<Body>,
    ) -> Result<Response<Body>, Infallible> {
        let resp = match req.extract::<DavRequest, _>().await {
            Ok(DavRequest::Get { path, pathparts }) => self.get(&path, pathparts).await,
            Ok(DavRequest::Propfind { path, depth, query }) => {
                self.propfind(&path, depth, query).await
            }
            Ok(DavRequest::Options) => Ok(StatusCode::NO_CONTENT.into_response()),
            Err(r) => Ok(r),
        };
        let resp = resp.unwrap_or_else(|e| {
                let class = e.class();
                let e = anyhow::Error::from(e);
                tracing::info!(error = ?e, status = class.to_status().as_u16(), "Error processing request");
                if class == ErrorClass::NotFound {
                    not_found()
                } else {
                    (class.to_status(), format!("{e:?}")).into_response()
                }
            });
        Ok((WEBDAV_RESPONSE_HEADERS, resp).into_response())
    }

    /// Handle a `GET` request for the given `path`.
    ///
    /// `pathparts` contains the individual components of the request URL path
    /// prior to parsing into `path`.  It is needed for things like breadcrumbs
    /// in HTML views of collection resources.
    async fn get(
        &self,
        path: &DavPath,
        pathparts: Vec<Component>,
    ) -> Result<Response<Body>, DavError> {
        match self.get_resource_with_children(path).await? {
            DavResourceWithChildren::Collection { children, .. } => {
                let html = self.templater.render_collection(children, pathparts)?;
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
            }) => {
                // TODO: Log something
                Ok(not_found())
            }
        }
    }

    /// Handle a `PROPFIND` request for the given `path`.  `depth` is the value
    /// of the `Depth` header, and `query` is the parsed request body (with an
    /// empty body already defaulted to "allprop" as per the RFC).
    async fn propfind(
        &self,
        path: &DavPath,
        depth: FiniteDepth,
        query: PropFind,
    ) -> Result<Response<Body>, DavError> {
        let resources = match depth {
            FiniteDepth::Zero => vec![self.get_resource(path).await?],
            FiniteDepth::One => self.get_resource_with_children(path).await?.into_vec(),
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

    /// Obtain a handler for fetching resources for the given version of the
    /// given Dandiset.  If `version` is `VersionSpec::Latest`, the most recent
    /// published version of the Dandiset is used.
    async fn get_version_handler<'a>(
        &'a self,
        dandiset_id: &'a DandisetId,
        version_spec: &'a VersionSpec,
    ) -> Result<VersionHandler<'a>, DavError> {
        let d = self.dandi.dandiset(dandiset_id.clone());
        let endpoint = match version_spec {
            VersionSpec::Draft => d.version(VersionId::Draft),
            VersionSpec::Published(v) => d.version(VersionId::Published(v.clone())),
            VersionSpec::Latest => match d.get().await?.most_recent_published_version {
                Some(DandisetVersion { version, .. }) => d.version(version),
                None => {
                    return Err(DavError::NoLatestVersion {
                        dandiset_id: dandiset_id.clone(),
                    })
                }
            },
        };
        Ok(VersionHandler {
            dandiset_id,
            version_spec,
            endpoint,
        })
    }

    /// Get details on the resource at the given `path`
    async fn get_resource(&self, path: &DavPath) -> Result<DavResource, DavError> {
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
                .get_version_handler(dandiset_id, version)
                .await?
                .get()
                .await
                .map(|(col, _)| DavResource::Collection(col)),
            DavPath::DandisetYaml {
                dandiset_id,
                version,
            } => self
                .get_version_handler(dandiset_id, version)
                .await?
                .get_dandiset_yaml()
                .await
                .map(DavResource::Item),
            DavPath::DandiResource {
                dandiset_id,
                version,
                path,
            } => {
                self.get_version_handler(dandiset_id, version)
                    .await?
                    .get_resource(path)
                    .await
            }
            DavPath::ZarrIndex => Ok(DavResource::Collection(DavCollection::zarr_index())),
            DavPath::ZarrPath { path } => {
                let res = self.zarrman.get_resource(path).await?;
                Ok(DavResource::from(res))
            }
        }
    }

    /// Get details on the resource at the given `path` along with its
    /// immediate child resources (if any).
    ///
    /// If `path` points to a Dandiset version, the child resources will
    /// include `dandiset.yaml` as a virtual asset.
    async fn get_resource_with_children(
        &self,
        path: &DavPath,
    ) -> Result<DavResourceWithChildren, DavError> {
        match path {
            DavPath::Root => Ok(DavResourceWithChildren::root()),
            DavPath::DandisetIndex => {
                let col = DavCollection::dandiset_index();
                let children = self
                    .dandi
                    .get_all_dandisets()
                    .map_ok(|ds| DavResource::Collection(ds.into()))
                    .try_collect::<Vec<_>>()
                    .await?;
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
                let mut stream = endpoint.get_all_versions();
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
                let handler = self.get_version_handler(dandiset_id, version).await?;
                let (col, dsyaml) = handler.get().await?;
                let mut children = handler.get_root_children().await?;
                children.push(DavResource::Item(dsyaml));
                Ok(DavResourceWithChildren::Collection { col, children })
            }
            DavPath::DandisetYaml {
                dandiset_id,
                version,
            } => self
                .get_version_handler(dandiset_id, version)
                .await?
                .get_dandiset_yaml()
                .await
                .map(DavResourceWithChildren::Item),
            DavPath::DandiResource {
                dandiset_id,
                version,
                path,
            } => {
                self.get_version_handler(dandiset_id, version)
                    .await?
                    .get_resource_with_children(path)
                    .await
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

/// A handler for fetching resources belonging to a certain Dandiset & version.
///
/// Resources returned by this type's methods all have their paths prefixed
/// with the path to the Dandiset & version.
#[derive(Clone, Debug)]
struct VersionHandler<'a> {
    dandiset_id: &'a DandisetId,
    version_spec: &'a VersionSpec,
    endpoint: VersionEndpoint<'a>,
}

impl VersionHandler<'_> {
    /// Get details on the version itself as a collection sans children.  The
    /// `dandiset.yaml` item is also included in order to save on a request
    /// later in the "with children" case.
    async fn get(&self) -> Result<(DavCollection, DavItem), DavError> {
        let VersionInfo {
            properties,
            metadata,
        } = self.endpoint.get().await?;
        let path = version_path(self.dandiset_id, self.version_spec);
        let col = DavCollection::dandiset_version(properties, path);
        let dandiset_yaml =
            DavItem::from(metadata).under_version_path(self.dandiset_id, self.version_spec);
        Ok((col, dandiset_yaml))
    }

    /// Get details on all resources at the root of the version's file tree
    /// (not including the `dandiset.yaml` file)
    async fn get_root_children(&self) -> Result<Vec<DavResource>, DandiError> {
        self.endpoint
            .get_root_children()
            .map_ok(|res| {
                DavResource::from(res).under_version_path(self.dandiset_id, self.version_spec)
            })
            .try_collect::<Vec<_>>()
            .await
    }

    /// Get the version's `dandiset.yaml` file
    async fn get_dandiset_yaml(&self) -> Result<DavItem, DavError> {
        let md = self.endpoint.get_metadata().await?;
        Ok(DavItem::from(md).under_version_path(self.dandiset_id, self.version_spec))
    }

    /// Get details on the resource at the given `path`
    async fn get_resource(&self, path: &PurePath) -> Result<DavResource, DavError> {
        let res = self.endpoint.get_resource(path).await?;
        Ok(DavResource::from(res).under_version_path(self.dandiset_id, self.version_spec))
    }

    /// Get details on the resource at the given `path` along with its
    /// immediate child resources (if any)
    async fn get_resource_with_children(
        &self,
        path: &PurePath,
    ) -> Result<DavResourceWithChildren, DavError> {
        let res = self.endpoint.get_resource_with_children(path).await?;
        Ok(DavResourceWithChildren::from(res)
            .under_version_path(self.dandiset_id, self.version_spec))
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
    /// Classify the general type of error
    pub(crate) fn class(&self) -> ErrorClass {
        match self {
            DavError::Dandi(e) => e.class(),
            DavError::ZarrMan(e) => e.class(),
            DavError::NoLatestVersion { .. } => ErrorClass::NotFound,
            DavError::Template(_) | DavError::Xml(_) => ErrorClass::Internal,
        }
    }
}

/// A classification of a `DavError` for use in determining the HTTP status
/// code to reply with
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum ErrorClass {
    /// The error was ultimately caused by something not being found
    NotFound,

    /// The error was ultimately caused by an upstream server returning an
    /// error or invalid response
    BadGateway,

    /// The error was ultimately caused by a request to an upstream server
    /// timing out
    GatewayTimeout,

    /// The error was ultimately caused by something going wrong in `dandidav`
    Internal,
}

impl ErrorClass {
    /// Return the HTTP status code matching this error class
    fn to_status(self) -> StatusCode {
        match self {
            ErrorClass::NotFound => StatusCode::NOT_FOUND,
            ErrorClass::BadGateway => StatusCode::BAD_GATEWAY,
            ErrorClass::GatewayTimeout => StatusCode::GATEWAY_TIMEOUT,
            ErrorClass::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
