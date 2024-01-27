use crate::consts::{DEFAULT_CONTENT_TYPE, YAML_CONTENT_TYPE};
use crate::dandiapi::*;
use crate::paths::PurePath;
use axum::{
    body::Body,
    http::StatusCode,
    response::{IntoResponse, Redirect},
};
use futures_util::{Stream, TryStreamExt};
use http::response::Response;
use std::fmt;
use thiserror::Error;
use time::OffsetDateTime;
use url::Url;

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
        let mut item = DavItem::from(md);
        // TODO: Do this more efficiently:
        item.href = DavPath::DandisetYaml {
            dandiset_id: dandiset_id.clone(),
            version: version.clone(),
        }
        .to_string();
        Ok(item)
    }

    async fn get_dandiset_version(
        &self,
        dandiset_id: &DandisetId,
        version: &VersionSpec,
    ) -> Result<DavCollection, DavError> {
        let v = self
            .get_version_endpoint(dandiset_id, version)
            .await?
            .get()
            .await?;
        // TODO: Do this more efficiently:
        let href = DavPath::Version {
            dandiset_id: dandiset_id.clone(),
            version: version.clone(),
        }
        .to_string();
        Ok(DavCollection::dandiset_version(v, href))
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
                .map(DavResource::Collection),
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
                Ok(DavResource::dandi_resource(res, path.to_string()))
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
            DavPath::Dandiset { dandiset_id } => todo!(),
            DavPath::DandisetReleases { dandiset_id } => {
                // TODO: Should this return a 404 when the Dandiset doesn't
                // have any published releases?
                let col = DavCollection::dandiset_releases(dandiset_id);
                let mut children = Vec::new();
                let endpoint = self.client.dandiset(dandiset_id.clone());
                let stream = endpoint.get_all_versions();
                tokio::pin!(stream);
                while let Some(v) = stream.try_next().await? {
                    if v.version != VersionId::Draft {
                        let href = format!("/dandisets/{}/releases/{}/", dandiset_id, v.version);
                        children.push(DavResource::Collection(DavCollection::dandiset_version(
                            v, href,
                        )));
                    }
                }
                Ok(DavResourceWithChildren::Collection { col, children })
            }
            DavPath::Version {
                dandiset_id,
                version,
            } => todo!(),
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
            } => todo!(),
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

impl fmt::Display for DavPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DavPath::Root => write!(f, "/"),
            DavPath::DandisetIndex => write!(f, "/dandisets/"),
            DavPath::Dandiset { dandiset_id } => write!(f, "/dandisets/{dandiset_id}/"),
            DavPath::DandisetReleases { dandiset_id } => {
                write!(f, "/dandisets/{dandiset_id}/releases/")
            }
            DavPath::Version {
                dandiset_id,
                version,
            } => {
                write!(f, "/dandisets/{dandiset_id}/")?;
                version.write_davpath_fragment(f)?;
                write!(f, "/")?;
                Ok(())
            }
            DavPath::DandisetYaml {
                dandiset_id,
                version,
            } => {
                write!(f, "/dandisets/{dandiset_id}/")?;
                version.write_davpath_fragment(f)?;
                write!(f, "/dandiset.yaml")?;
                Ok(())
            }
            DavPath::DandiResource {
                dandiset_id,
                version,
                path,
            } => {
                write!(f, "/dandisets/{dandiset_id}/")?;
                version.write_davpath_fragment(f)?;
                write!(f, "/{path}")?;
                Ok(())
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum VersionSpec {
    Draft,
    Published(PublishedVersionId),
    Latest,
}

impl VersionSpec {
    fn write_davpath_fragment(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VersionSpec::Draft => write!(f, "draft"),
            VersionSpec::Published(v) => write!(f, "releases/{v}"),
            VersionSpec::Latest => write!(f, "latest"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum DavResource {
    Collection(DavCollection),
    Item(DavItem),
}

impl DavResource {
    fn root() -> Self {
        DavResource::Collection(DavCollection::root())
    }

    fn with_href_prefix(self, mut prefix: String) -> DavResource {
        match self {
            DavResource::Collection(col) => DavResource::Collection(col.with_href_prefix(prefix)),
            DavResource::Item(item) => DavResource::Item(item.with_href_prefix(prefix)),
        }
    }

    fn dandi_resource(res: DandiResource, href: String) -> Self {
        match res {
            DandiResource::Folder(AssetFolder::Root) => todo!(),
            DandiResource::Folder(AssetFolder::Path(path)) => todo!(),
            DandiResource::Asset(Asset::Blob(blob)) => DavResource::Item(DavItem {
                name: blob.path.name().to_owned(),
                href,
                created: Some(blob.created),
                modified: Some(blob.modified),
                content_type: blob
                    .content_type()
                    .unwrap_or(DEFAULT_CONTENT_TYPE)
                    .to_owned(),
                size: Some(blob.size),
                etag: blob.etag().map(String::from),
                kind: ResourceKind::Blob,
                content: match blob.download_url() {
                    Some(url) => DavContent::Redirect(url.clone()),
                    // TODO: Log a warning when asset doesn't have a download
                    // URL?
                    None => DavContent::Missing,
                },
            }),
            DandiResource::Asset(Asset::Zarr(zarr)) => DavResource::Collection(DavCollection {
                name: Some(zarr.path.name().to_owned()),
                href,
                created: Some(zarr.created),
                modified: Some(zarr.modified),
                size: Some(zarr.size),
                kind: ResourceKind::Zarr,
            }),
            DandiResource::ZarrFolder(ZarrFolder { path }) => todo!(),
            DandiResource::ZarrEntry(entry) => todo!(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum DavResourceWithChildren {
    Collection {
        col: DavCollection,
        children: Vec<DavResource>,
    },
    Item(DavItem),
}

impl DavResourceWithChildren {
    fn root() -> Self {
        DavResourceWithChildren::Collection {
            col: DavCollection::root(),
            children: vec![DavResource::Collection(DavCollection::dandiset_index())],
        }
    }

    fn with_href_prefix(self, mut prefix: String) -> DavResourceWithChildren {
        match self {
            DavResourceWithChildren::Collection { col, children } => {
                DavResourceWithChildren::Collection {
                    col: col.with_href_prefix(prefix),
                    children,
                }
            }
            DavResourceWithChildren::Item(item) => {
                DavResourceWithChildren::Item(item.with_href_prefix(prefix))
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DavCollection {
    name: Option<String>, // None for root collection
    href: String,
    created: Option<OffsetDateTime>,
    modified: Option<OffsetDateTime>,
    size: Option<i64>,
    kind: ResourceKind,
}

impl DavCollection {
    fn with_href_prefix(mut self, mut prefix: String) -> DavCollection {
        prefix.push_str(&self.href);
        self.href = prefix;
        self
    }

    fn root() -> Self {
        DavCollection {
            name: None,
            href: "/".to_owned(),
            created: None,
            modified: None,
            size: None,
            kind: ResourceKind::Root,
        }
    }

    fn dandiset_index() -> Self {
        DavCollection {
            name: Some("dandisets".to_owned()),
            href: "/dandisets/".to_owned(),
            created: None,
            modified: None,
            size: None,
            kind: ResourceKind::DandisetIndex,
        }
    }

    fn dandiset_releases(dandiset_id: &DandisetId) -> Self {
        DavCollection {
            name: Some("releases".to_owned()),
            href: format!("/dandisets/{dandiset_id}/releases/"),
            created: None,
            modified: None,
            size: None,
            kind: ResourceKind::DandisetReleases,
        }
    }

    fn dandiset_version(v: DandisetVersion, href: String) -> Self {
        DavCollection {
            name: Some(v.version.to_string()),
            href,
            //href: format!("/dandisets/{}/releases/{}/", ds.identifier, v.version),
            created: Some(v.created),
            modified: Some(v.modified),
            size: Some(v.size),
            kind: ResourceKind::Version,
        }
    }
}

impl From<Dandiset> for DavCollection {
    fn from(ds: Dandiset) -> DavCollection {
        DavCollection {
            name: Some(ds.identifier.to_string()),
            href: format!("/dandisets/{}/", ds.identifier),
            created: Some(ds.created),
            modified: Some(ds.modified),
            size: None,
            kind: ResourceKind::Dandiset,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DavItem {
    name: String,
    href: String,
    created: Option<OffsetDateTime>,
    modified: Option<OffsetDateTime>,
    content_type: String,
    size: Option<i64>,
    etag: Option<String>,
    kind: ResourceKind,
    content: DavContent,
}

impl DavItem {
    fn with_href_prefix(mut self, mut prefix: String) -> DavItem {
        prefix.push_str(&self.href);
        self.href = prefix;
        self
    }
}

impl From<VersionMetadata> for DavItem {
    fn from(value: VersionMetadata) -> DavItem {
        let len = value.len();
        let blob = Vec::<u8>::from(value);
        DavItem {
            name: "dandiset.yaml".to_owned(),
            href: "/dandiset.yaml".to_owned(),
            created: None,
            modified: None,
            content_type: YAML_CONTENT_TYPE.to_owned(),
            size: i64::try_from(len).ok(),
            etag: None,
            kind: ResourceKind::VersionMetadata,
            content: DavContent::Blob(blob),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum DavContent {
    Blob(Vec<u8>),
    Redirect(Url),
    // Used when a blob asset lacks an S3 download URL
    Missing,
}

// For use in rendering the "Type" column in HTML views
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum ResourceKind {
    Root,
    DandisetIndex,
    Dandiset,
    DandisetReleases,
    Version,
    VersionMetadata,
    AssetFolder,
    Blob,
    Zarr,
    ZarrEntry,
    ZarrFolder,
}

impl fmt::Display for ResourceKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ResourceKind::Root => "Root", // Not actually shown
            ResourceKind::DandisetIndex => "Dandisets",
            ResourceKind::Dandiset => "Dandiset",
            ResourceKind::DandisetReleases => "Published versions",
            ResourceKind::Version => "Dandiset version",
            ResourceKind::VersionMetadata => "Version metadata",
            ResourceKind::AssetFolder => "Directory",
            ResourceKind::Blob => "Blob asset",
            ResourceKind::Zarr => "Zarr asset",
            ResourceKind::ZarrEntry => "Zarr entry",
            ResourceKind::ZarrFolder => "Directory",
        };
        write!(f, "{s}")
    }
}

#[derive(Debug, Error)]
pub(crate) enum DavError {
    #[error("failed to fetch data from Archive")]
    DandiApi(#[from] ApiError),
    #[error(
        "latest version was requested for Dandiset {dandiset_id}, but it has not been published"
    )]
    NoLatestVersion { dandiset_id: DandisetId },
}

impl DavError {
    pub(crate) fn is_404(&self) -> bool {
        matches!(
            self,
            DavError::DandiApi(ApiError::NotFound { .. } | ApiError::ZarrEntryNotFound { .. })
                | DavError::NoLatestVersion { .. }
        )
    }
}

pub(crate) struct Propfind; // TODO
