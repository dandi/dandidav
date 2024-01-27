use crate::consts::{DEFAULT_CONTENT_TYPE, YAML_CONTENT_TYPE};
use crate::dandi::*;
use crate::paths::{PureDirPath, PurePath};
use axum::{
    body::Body,
    http::StatusCode,
    response::{IntoResponse, Redirect},
};
use futures_util::{Stream, TryStreamExt};
use http::response::Response;
use std::fmt::{self, Write};
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

#[derive(Clone, Debug, Eq, PartialEq)]
enum DavResource {
    Collection(DavCollection),
    Item(DavItem),
}

impl DavResource {
    fn root() -> Self {
        DavResource::Collection(DavCollection::root())
    }

    fn under_version_path(self, dandiset_id: &DandisetId, version: &VersionSpec) -> DavResource {
        match self {
            DavResource::Collection(col) => {
                DavResource::Collection(col.under_version_path(dandiset_id, version))
            }
            DavResource::Item(item) => {
                DavResource::Item(item.under_version_path(dandiset_id, version))
            }
        }
    }
}

impl From<DavCollection> for DavResource {
    fn from(value: DavCollection) -> DavResource {
        DavResource::Collection(value)
    }
}

impl From<DavItem> for DavResource {
    fn from(value: DavItem) -> DavResource {
        DavResource::Item(value)
    }
}

impl From<DandiResource> for DavResource {
    fn from(res: DandiResource) -> DavResource {
        match res {
            DandiResource::Folder(folder) => DavResource::Collection(folder.into()),
            DandiResource::Asset(Asset::Blob(blob)) => DavResource::Item(blob.into()),
            DandiResource::Asset(Asset::Zarr(zarr)) => DavResource::Collection(zarr.into()),
            DandiResource::ZarrFolder(folder) => DavResource::Collection(folder.into()),
            DandiResource::ZarrEntry(entry) => DavResource::Item(entry.into()),
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

    fn under_version_path(
        self,
        dandiset_id: &DandisetId,
        version: &VersionSpec,
    ) -> DavResourceWithChildren {
        match self {
            DavResourceWithChildren::Collection { col, children } => {
                DavResourceWithChildren::Collection {
                    col: col.under_version_path(dandiset_id, version),
                    children: children
                        .into_iter()
                        .map(|r| r.under_version_path(dandiset_id, version))
                        .collect(),
                }
            }
            DavResourceWithChildren::Item(item) => {
                DavResourceWithChildren::Item(item.under_version_path(dandiset_id, version))
            }
        }
    }
}

impl From<DavItem> for DavResourceWithChildren {
    fn from(value: DavItem) -> DavResourceWithChildren {
        DavResourceWithChildren::Item(value)
    }
}

impl From<DandiResourceWithChildren> for DavResourceWithChildren {
    fn from(res: DandiResourceWithChildren) -> DavResourceWithChildren {
        fn map_children(vec: Vec<DandiResource>) -> Vec<DavResource> {
            vec.into_iter().map(DavResource::from).collect()
        }

        use DandiResourceWithChildren::*;
        match res {
            Folder { folder, children } => DavResourceWithChildren::Collection {
                col: DavCollection::from(folder),
                children: map_children(children),
            },
            Blob(blob) => DavResourceWithChildren::Item(blob.into()),
            Zarr { zarr, children } => DavResourceWithChildren::Collection {
                col: DavCollection::from(zarr),
                children: map_children(children),
            },
            ZarrFolder { folder, children } => DavResourceWithChildren::Collection {
                col: DavCollection::from(folder),
                children: map_children(children),
            },
            ZarrEntry(entry) => DavResourceWithChildren::Item(entry.into()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DavCollection {
    path: Option<PureDirPath>, // None for root collection
    created: Option<OffsetDateTime>,
    modified: Option<OffsetDateTime>,
    size: Option<i64>,
    kind: ResourceKind,
}

impl DavCollection {
    pub(crate) fn name(&self) -> Option<&str> {
        self.path.as_ref().map(PureDirPath::name)
    }

    pub(crate) fn href(&self) -> String {
        match self.path {
            Some(ref p) => format!("/{p}"),
            None => "/".to_owned(),
        }
    }

    fn under_version_path(
        mut self,
        dandiset_id: &DandisetId,
        version: &VersionSpec,
    ) -> DavCollection {
        let vpath = version_path(dandiset_id, version);
        let path = match self.path {
            Some(p) => vpath.join_dir(&p),
            None => vpath,
        };
        self.path = Some(path);
        self
    }

    fn root() -> Self {
        DavCollection {
            path: None,
            created: None,
            modified: None,
            size: None,
            kind: ResourceKind::Root,
        }
    }

    fn dandiset_index() -> Self {
        DavCollection {
            path: Some(
                "dandisets/"
                    .parse::<PureDirPath>()
                    .expect(r#""dandisets/" should be a valid dir path"#),
            ),
            created: None,
            modified: None,
            size: None,
            kind: ResourceKind::DandisetIndex,
        }
    }

    fn dandiset_releases(dandiset_id: &DandisetId) -> Self {
        DavCollection {
            path: Some(
                format!("dandisets/{dandiset_id}/releases/")
                    .parse::<PureDirPath>()
                    .expect("should be a valid dir path"),
            ),
            created: None,
            modified: None,
            size: None,
            kind: ResourceKind::DandisetReleases,
        }
    }

    fn dandiset_version(v: DandisetVersion, path: PureDirPath) -> Self {
        DavCollection {
            path: Some(path),
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
            path: Some(
                format!("dandisets/{}", ds.identifier)
                    .parse::<PureDirPath>()
                    .expect("should be a valid dir path"),
            ),
            created: Some(ds.created),
            modified: Some(ds.modified),
            size: None,
            kind: ResourceKind::Dandiset,
        }
    }
}

impl From<AssetFolder> for DavCollection {
    fn from(AssetFolder { path }: AssetFolder) -> DavCollection {
        DavCollection {
            path: Some(path),
            created: None,
            modified: None,
            size: None,
            kind: ResourceKind::AssetFolder,
        }
    }
}

impl From<ZarrAsset> for DavCollection {
    fn from(zarr: ZarrAsset) -> DavCollection {
        DavCollection {
            path: Some(zarr.path.to_dir_path()),
            created: Some(zarr.created),
            modified: Some(zarr.modified),
            size: Some(zarr.size),
            kind: ResourceKind::Zarr,
        }
    }
}

impl From<ZarrFolder> for DavCollection {
    fn from(ZarrFolder { path }: ZarrFolder) -> DavCollection {
        DavCollection {
            path: Some(path),
            created: None,
            modified: None,
            size: None,
            kind: ResourceKind::ZarrFolder,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DavItem {
    path: PurePath,
    created: Option<OffsetDateTime>,
    modified: Option<OffsetDateTime>,
    content_type: String,
    size: Option<i64>,
    etag: Option<String>,
    kind: ResourceKind,
    content: DavContent,
}

impl DavItem {
    fn under_version_path(mut self, dandiset_id: &DandisetId, version: &VersionSpec) -> DavItem {
        let path = version_path(dandiset_id, version).join(&self.path);
        self.path = path;
        self
    }
}

impl From<VersionMetadata> for DavItem {
    fn from(value: VersionMetadata) -> DavItem {
        let len = value.len();
        let blob = Vec::<u8>::from(value);
        DavItem {
            path: "dandiset.yaml"
                .parse::<PurePath>()
                .expect(r#""dandiset.yaml" should be a valid path"#),
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

impl From<BlobAsset> for DavItem {
    fn from(blob: BlobAsset) -> DavItem {
        // Call methods before moving out `path` field:
        let content_type = blob
            .content_type()
            .unwrap_or(DEFAULT_CONTENT_TYPE)
            .to_owned();
        let etag = blob.etag().map(String::from);
        let content = match blob.download_url() {
            Some(url) => DavContent::Redirect(url.clone()),
            // TODO: Log a warning when asset doesn't have a download URL?
            None => DavContent::Missing,
        };
        DavItem {
            path: blob.path,
            created: Some(blob.created),
            modified: Some(blob.modified),
            content_type,
            size: Some(blob.size),
            etag,
            kind: ResourceKind::Blob,
            content,
        }
    }
}

impl From<ZarrEntry> for DavItem {
    fn from(entry: ZarrEntry) -> DavItem {
        DavItem {
            path: entry.path,
            created: None,
            modified: Some(entry.modified),
            content_type: DEFAULT_CONTENT_TYPE.to_owned(),
            size: Some(entry.size),
            etag: Some(entry.etag),
            kind: ResourceKind::ZarrEntry,
            content: DavContent::Redirect(entry.url),
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

fn version_path(dandiset_id: &DandisetId, version: &VersionSpec) -> PureDirPath {
    fn writer(s: &mut String, dandiset_id: &DandisetId, version: &VersionSpec) -> fmt::Result {
        write!(s, "dandisets/{dandiset_id}/")?;
        match version {
            VersionSpec::Draft => write!(s, "draft")?,
            VersionSpec::Published(v) => write!(s, "releases/{v}")?,
            VersionSpec::Latest => write!(s, "latest")?,
        }
        write!(s, "/")?;
        Ok(())
    }

    let mut s = String::new();
    writer(&mut s, dandiset_id, version).expect("writing to a String shouldn't fail");
    s.parse::<PureDirPath>()
        .expect("should be a valid dir path")
}
