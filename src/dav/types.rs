use super::util::{format_creationdate, format_modifieddate, version_path, Href};
use super::xml::{PropValue, Property};
use super::VersionSpec;
use crate::consts::{DEFAULT_CONTENT_TYPE, YAML_CONTENT_TYPE};
use crate::dandi::*;
use crate::paths::{PureDirPath, PurePath};
use crate::zarrman::*;
use enum_dispatch::enum_dispatch;
use serde::{ser::Serializer, Serialize};
use time::OffsetDateTime;
use url::Url;

#[enum_dispatch]
pub(super) trait HasProperties {
    fn href(&self) -> Href;
    fn creationdate(&self) -> Option<String>;
    fn displayname(&self) -> Option<String>;
    fn getcontentlength(&self) -> Option<i64>;
    fn getcontenttype(&self) -> Option<String>;
    fn getetag(&self) -> Option<String>;
    fn getlastmodified(&self) -> Option<String>;
    fn is_collection(&self) -> bool;

    fn property(&self, prop: &Property) -> Option<PropValue> {
        match prop {
            Property::CreationDate => self.creationdate().map(Into::into),
            Property::DisplayName => self.displayname().map(Into::into),
            Property::GetContentLength => self.getcontentlength().map(Into::into),
            Property::GetContentType => self.getcontenttype().map(Into::into),
            Property::GetETag => self.getetag().map(Into::into),
            Property::GetLastModified => self.getlastmodified().map(Into::into),
            Property::ResourceType => {
                if self.is_collection() {
                    Some(PropValue::Collection)
                } else {
                    Some(PropValue::Empty)
                }
            }
            Property::Custom { .. } => None,
        }
    }
}

#[allow(clippy::large_enum_variant)]
#[enum_dispatch(HasProperties)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum DavResource {
    Collection(DavCollection),
    Item(DavItem),
}

impl DavResource {
    pub(super) fn root() -> Self {
        DavResource::Collection(DavCollection::root())
    }

    pub(super) fn under_version_path(
        self,
        dandiset_id: &DandisetId,
        version: &VersionSpec,
    ) -> DavResource {
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

impl From<ZarrManResource> for DavResource {
    fn from(res: ZarrManResource) -> DavResource {
        match res {
            ZarrManResource::WebFolder(folder) => DavResource::Collection(folder.into()),
            ZarrManResource::Manifest(folder) => DavResource::Collection(folder.into()),
            ZarrManResource::ManFolder(folder) => DavResource::Collection(folder.into()),
            ZarrManResource::ManEntry(entry) => DavResource::Item(entry.into()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum DavResourceWithChildren {
    Collection {
        col: DavCollection,
        children: Vec<DavResource>,
    },
    Item(DavItem),
}

impl DavResourceWithChildren {
    pub(super) fn root() -> Self {
        DavResourceWithChildren::Collection {
            col: DavCollection::root(),
            children: vec![
                DavResource::Collection(DavCollection::dandiset_index()),
                DavResource::Collection(DavCollection::zarr_index()),
            ],
        }
    }

    pub(super) fn under_version_path(
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

    pub(super) fn into_vec(self) -> Vec<DavResource> {
        match self {
            DavResourceWithChildren::Collection { col, children } => {
                let mut vec = Vec::with_capacity(children.len().saturating_add(1));
                vec.push(DavResource::from(col));
                vec.extend(children);
                vec
            }
            DavResourceWithChildren::Item(item) => vec![DavResource::Item(item)],
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

impl From<ZarrManResourceWithChildren> for DavResourceWithChildren {
    fn from(res: ZarrManResourceWithChildren) -> DavResourceWithChildren {
        fn map_children(vec: Vec<ZarrManResource>) -> Vec<DavResource> {
            vec.into_iter().map(DavResource::from).collect()
        }

        use ZarrManResourceWithChildren::*;
        match res {
            WebFolder { folder, children } => DavResourceWithChildren::Collection {
                col: DavCollection::from(folder),
                children: map_children(children),
            },
            Manifest { folder, children } => DavResourceWithChildren::Collection {
                col: DavCollection::from(folder),
                children: map_children(children),
            },
            ManFolder { folder, children } => DavResourceWithChildren::Collection {
                col: DavCollection::from(folder),
                children: map_children(children),
            },
            ManEntry(entry) => DavResourceWithChildren::Item(entry.into()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct DavCollection {
    pub(super) path: Option<PureDirPath>, // None for root collection
    pub(super) created: Option<OffsetDateTime>,
    pub(super) modified: Option<OffsetDateTime>,
    pub(super) size: Option<i64>,
    pub(super) kind: ResourceKind,
    pub(super) metadata_url: Option<Url>,
}

impl DavCollection {
    pub(super) fn name(&self) -> Option<&str> {
        self.path.as_ref().map(PureDirPath::name_str)
    }

    pub(super) fn web_link(&self) -> Href {
        match self.path {
            Some(ref p) => Href::from_path(&format!("/{p}")),
            None => Href::from_path("/"),
        }
    }

    pub(super) fn under_version_path(
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

    pub(super) fn root() -> Self {
        DavCollection {
            path: None,
            created: None,
            modified: None,
            size: None,
            kind: ResourceKind::Root,
            metadata_url: None,
        }
    }

    pub(super) fn dandiset_index() -> Self {
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
            metadata_url: None,
        }
    }

    pub(super) fn dandiset_releases(dandiset_id: &DandisetId) -> Self {
        DavCollection {
            path: Some(
                PureDirPath::try_from(format!("dandisets/{dandiset_id}/releases/"))
                    .expect("should be a valid dir path"),
            ),
            created: None,
            modified: None,
            size: None,
            kind: ResourceKind::DandisetReleases,
            metadata_url: None,
        }
    }

    pub(super) fn dandiset_version(v: DandisetVersion, path: PureDirPath) -> Self {
        DavCollection {
            path: Some(path),
            created: Some(v.created),
            modified: Some(v.modified),
            size: Some(v.size),
            kind: ResourceKind::Version,
            metadata_url: Some(v.metadata_url),
        }
    }

    pub(super) fn zarr_index() -> Self {
        DavCollection {
            path: Some(
                "zarrs/"
                    .parse::<PureDirPath>()
                    .expect(r#""zarrs/" should be a valid dir path"#),
            ),
            created: None,
            modified: None,
            size: None,
            kind: ResourceKind::ZarrIndex,
            metadata_url: None,
        }
    }
}

impl HasProperties for DavCollection {
    fn href(&self) -> Href {
        self.web_link()
    }

    fn creationdate(&self) -> Option<String> {
        self.created.map(format_creationdate)
    }

    fn displayname(&self) -> Option<String> {
        self.name().map(String::from)
    }

    fn getcontentlength(&self) -> Option<i64> {
        self.size
    }

    fn getcontenttype(&self) -> Option<String> {
        None
    }

    fn getetag(&self) -> Option<String> {
        None
    }

    fn getlastmodified(&self) -> Option<String> {
        self.modified.map(format_modifieddate)
    }

    fn is_collection(&self) -> bool {
        true
    }
}

impl From<Dandiset> for DavCollection {
    fn from(ds: Dandiset) -> DavCollection {
        DavCollection {
            path: Some(
                PureDirPath::try_from(format!("dandisets/{}/", ds.identifier))
                    .expect("should be a valid dir path"),
            ),
            created: Some(ds.created),
            modified: Some(ds.modified),
            size: None,
            kind: ResourceKind::Dandiset,
            metadata_url: None,
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
            kind: ResourceKind::Directory,
            metadata_url: None,
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
            metadata_url: Some(zarr.metadata_url),
        }
    }
}

impl From<ZarrFolder> for DavCollection {
    fn from(ZarrFolder { zarr_path, path }: ZarrFolder) -> DavCollection {
        DavCollection {
            path: Some(zarr_path.to_dir_path().join_dir(&path)),
            created: None,
            modified: None,
            size: None,
            kind: ResourceKind::Directory,
            metadata_url: None,
        }
    }
}

impl From<WebFolder> for DavCollection {
    fn from(WebFolder { web_path }: WebFolder) -> DavCollection {
        DavCollection {
            path: Some(web_path),
            created: None,
            modified: None,
            size: None,
            kind: ResourceKind::Directory,
            metadata_url: None,
        }
    }
}

impl From<Manifest> for DavCollection {
    fn from(Manifest { path }: Manifest) -> DavCollection {
        DavCollection {
            path: Some(path.to_web_path()),
            created: None,
            modified: None,
            size: None,
            kind: ResourceKind::Zarr,
            metadata_url: None,
        }
    }
}

impl From<ManifestFolder> for DavCollection {
    fn from(ManifestFolder { web_path }: ManifestFolder) -> DavCollection {
        DavCollection {
            path: Some(web_path),
            created: None,
            modified: None,
            size: None,
            kind: ResourceKind::Directory,
            metadata_url: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct DavItem {
    pub(super) path: PurePath,
    pub(super) created: Option<OffsetDateTime>,
    pub(super) modified: Option<OffsetDateTime>,
    pub(super) content_type: String,
    pub(super) size: Option<i64>,
    pub(super) etag: Option<String>,
    pub(super) kind: ResourceKind,
    pub(super) content: DavContent,
    pub(super) metadata_url: Option<Url>,
}

impl DavItem {
    pub(super) fn name(&self) -> &str {
        self.path.name_str()
    }

    pub(super) fn web_link(&self) -> Href {
        if let DavContent::Redirect(ref redir) = self.content {
            // Link directly to the download URL in the web view in order to
            // save a request
            redir.get_url(false).into()
        } else {
            Href::from_path(&format!("/{}", self.path))
        }
    }

    pub(super) fn under_version_path(
        mut self,
        dandiset_id: &DandisetId,
        version: &VersionSpec,
    ) -> DavItem {
        let path = version_path(dandiset_id, version).join(&self.path);
        self.path = path;
        self
    }
}

impl HasProperties for DavItem {
    fn href(&self) -> Href {
        Href::from_path(&format!("/{}", self.path))
    }

    fn creationdate(&self) -> Option<String> {
        self.created.map(format_creationdate)
    }

    fn displayname(&self) -> Option<String> {
        Some(self.name().to_owned())
    }

    fn getcontentlength(&self) -> Option<i64> {
        self.size
    }

    fn getcontenttype(&self) -> Option<String> {
        Some(self.content_type.clone())
    }

    fn getetag(&self) -> Option<String> {
        self.etag.as_ref().map(String::from)
    }

    fn getlastmodified(&self) -> Option<String> {
        self.modified.map(format_modifieddate)
    }

    fn is_collection(&self) -> bool {
        false
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
            metadata_url: None,
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
        let content = match (blob.archive_url(), blob.s3_url()) {
            (Some(archive), Some(s3)) => DavContent::Redirect(Redirect::Alt {
                s3: s3.clone(),
                archive: archive.clone(),
            }),
            (Some(u), None) | (None, Some(u)) => DavContent::Redirect(Redirect::Direct(u.clone())),
            // TODO: Log a warning when asset doesn't have a download URL?
            (None, None) => DavContent::Missing,
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
            metadata_url: Some(blob.metadata_url),
        }
    }
}

impl From<ZarrEntry> for DavItem {
    fn from(entry: ZarrEntry) -> DavItem {
        DavItem {
            path: entry.zarr_path.to_dir_path().join(&entry.path),
            created: None,
            modified: Some(entry.modified),
            content_type: DEFAULT_CONTENT_TYPE.to_owned(),
            size: Some(entry.size),
            etag: Some(entry.etag),
            kind: ResourceKind::ZarrEntry,
            content: DavContent::Redirect(Redirect::Direct(entry.url)),
            metadata_url: None,
        }
    }
}

impl From<ManifestEntry> for DavItem {
    fn from(entry: ManifestEntry) -> DavItem {
        DavItem {
            path: entry.web_path,
            created: None,
            modified: Some(entry.modified),
            content_type: DEFAULT_CONTENT_TYPE.to_owned(),
            size: Some(entry.size),
            etag: Some(entry.etag),
            kind: ResourceKind::ZarrEntry,
            content: DavContent::Redirect(Redirect::Direct(entry.url)),
            metadata_url: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum DavContent {
    Blob(Vec<u8>),
    Redirect(Redirect),
    // Used when a blob asset lacks an S3 download URL
    Missing,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum Redirect {
    Direct(Url),
    Alt { s3: Url, archive: Url },
}

impl Redirect {
    pub(super) fn get_url(&self, prefer_s3: bool) -> &Url {
        match self {
            Redirect::Direct(u) => u,
            Redirect::Alt { s3, archive } => {
                if prefer_s3 {
                    s3
                } else {
                    archive
                }
            }
        }
    }
}

// For use in rendering the "Type" column in HTML views
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) enum ResourceKind {
    Root,
    Parent,
    DandisetIndex,
    Dandiset,
    DandisetReleases,
    Version,
    VersionMetadata,
    Directory,
    Blob,
    Zarr,
    ZarrEntry,
    ZarrIndex,
}

impl ResourceKind {
    pub(super) fn as_str(&self) -> &'static str {
        match self {
            ResourceKind::Root => "Root", // Not actually shown
            ResourceKind::Parent => "Parent directory",
            ResourceKind::DandisetIndex => "Dandisets",
            ResourceKind::Dandiset => "Dandiset",
            ResourceKind::DandisetReleases => "Published versions",
            ResourceKind::Version => "Dandiset version",
            ResourceKind::VersionMetadata => "Version metadata",
            ResourceKind::Directory => "Directory",
            ResourceKind::Blob => "Blob asset",
            ResourceKind::Zarr => "Zarr asset",
            ResourceKind::ZarrEntry => "Zarr entry",
            ResourceKind::ZarrIndex => "Zarrs",
        }
    }
}

impl Serialize for ResourceKind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}
