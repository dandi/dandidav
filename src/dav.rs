use crate::dandiapi::{Client, DandisetId, VersionId};
use crate::paths::PurePath;
use axum::{
    body::Body,
    response::{IntoResponse, Redirect},
};
use http::response::Response;
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

    #[allow(clippy::unused_async)]
    async fn resolve(&self, path: &DavPath) -> Result<DavResource, DavError> {
        todo!()
    }

    #[allow(clippy::unused_async)]
    async fn resolve_with_children(
        &self,
        path: &DavPath,
    ) -> Result<DavResourceWithChildren, DavError> {
        todo!()
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
    Folder {
        dandiset_id: DandisetId,
        version: VersionSpec,
        path: PurePath,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum VersionSpec {
    Fixed(VersionId),
    Latest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum DavResource {
    Collection(DavCollection),
    Item(DavItem),
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum DavResourceWithChildren {
    Collection {
        col: DavCollection,
        children: Vec<DavResource>,
    },
    Item(DavItem),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DavCollection {
    name: String,
    href: String,
    created: Option<OffsetDateTime>,
    modified: Option<OffsetDateTime>,
    kind: ResourceKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DavItem {
    name: String,
    href: String,
    created: Option<OffsetDateTime>,
    modified: Option<OffsetDateTime>,
    content_type: String,
    size: i64,
    etag: Option<String>,
    kind: ResourceKind,
    content: DavContent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum DavContent {
    Blob(Vec<u8>),
    Redirect(Url),
}

// For use in rendering the "Type" column in HTML views
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum ResourceKind {
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

pub(crate) struct Propfind; // TODO
pub(crate) struct DavError; // TODO
