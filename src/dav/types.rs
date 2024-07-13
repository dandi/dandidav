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

/// Trait for querying the values of WebDAV properties from WebDAV resources
///
/// If a property is queried on a resource that does not have it defined, the
/// query method should return `None`.
#[enum_dispatch]
pub(super) trait HasProperties {
    /// Return the value of the "href" element to use in a "response" for this
    /// resource.
    ///
    /// For `dandidav`, this is the absolute path at which the resource is
    /// served.
    ///
    /// This is technically not a WebDAV property, but it's close enough for
    /// our purposes.
    fn href(&self) -> Href;

    /// Return the value of the "creationdate" property in RFC 3339 format
    fn creationdate(&self) -> Option<String>;

    /// Return the value of the "displayname" property
    ///
    /// For `dandidav`, this is the same as the resource's filename.
    fn displayname(&self) -> Option<String>;

    /// Return the value of the "getcontentlength" property
    fn getcontentlength(&self) -> Option<i64>;

    /// Return the value of the "getcontenttype" property
    fn getcontenttype(&self) -> Option<String>;

    /// Return the value of the "getetag" property
    fn getetag(&self) -> Option<String>;

    /// Return the value of the "getlastmodified" property in RFC 1123 format
    fn getlastmodified(&self) -> Option<String>;

    /// Return `true` iff this is a collection resource
    fn is_collection(&self) -> bool;

    /// Return the value of the given property.  `Property::Custom` inputs will
    /// always evaluate to `None`.
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
            Property::Custom(_) => None,
        }
    }
}

/// Information about a WebDAV resource, not including child resources
#[allow(clippy::large_enum_variant)]
#[enum_dispatch(HasProperties)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum DavResource {
    Collection(DavCollection),
    Item(DavItem),
}

impl DavResource {
    /// Construct a `DavResource` representing the root of the hierarchy served
    /// by `dandidav`
    pub(super) fn root() -> Self {
        DavResource::Collection(DavCollection::root())
    }

    /// Prefix the resource's path with the path at which `dandidav` serves the
    /// given Dandiset & version under `/dandisets/`.
    ///
    /// See [`version_path()`] for more information.
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

/// Information about a WebDAV resource and its immediate child resources (if
/// any)
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum DavResourceWithChildren {
    Collection {
        /// A collection resource
        col: DavCollection,

        /// The child resources of the collection
        children: Vec<DavResource>,
    },
    Item(DavItem),
}

impl DavResourceWithChildren {
    /// Construct a `DavResourceWithChildren` representing the root of the
    /// hierarchy served by `dandidav`
    pub(super) fn root() -> Self {
        DavResourceWithChildren::Collection {
            col: DavCollection::root(),
            children: vec![
                DavResource::Collection(DavCollection::dandiset_index()),
                DavResource::Collection(DavCollection::zarr_index()),
            ],
        }
    }

    /// Prefix the paths of the resource and its child resources with the path
    /// at which `dandidav` serves the given Dandiset & version under
    /// `/dandisets/`.
    ///
    /// See [`version_path()`] for more information.
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

    /// Convert to a `Vec` of all `DavResources`s represented within `self`
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

/// Information on a collection resource
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct DavCollection {
    /// The path at which the collection is served by `dandidav`.  This is
    /// `None` iff the collection is the root collection.
    ///
    /// Note that collections inside a Dandiset version need to have
    /// `under_version_path()` called on them in order for `path` to be
    /// complete.
    pub(super) path: Option<PureDirPath>,

    /// The timestamp at which the resource was created
    pub(super) created: Option<OffsetDateTime>,

    /// The timestamp at which the resource was last modified
    pub(super) modified: Option<OffsetDateTime>,

    /// The size of the resource.
    ///
    /// When defined, this is the sum of the sizes of all descendant
    /// non-collection resources within the collection.
    pub(super) size: Option<i64>,

    /// The type of resource, for display in the "Type" column of HTML tables
    pub(super) kind: ResourceKind,

    /// A URL for retrieving the resource's associated metadata (if any) from
    /// the Archive instance
    pub(super) metadata_url: Option<Url>,
}

impl DavCollection {
    /// Return the base name of the resource's path
    pub(super) fn name(&self) -> Option<&str> {
        self.path.as_ref().map(PureDirPath::name_str)
    }

    /// Return the link to use for the resource in the HTML view of its parent
    /// collection as an absolute URL path (including leading slash)
    pub(super) fn web_link(&self) -> Href {
        match self.path {
            Some(ref p) => Href::from_path(&format!("/{p}")),
            None => Href::from_path("/"),
        }
    }

    /// Prefix the resource's path with the path at which `dandidav` serves the
    /// given Dandiset & version under `/dandisets/`.
    ///
    /// See [`version_path()`] for more information.
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

    /// Construct a `DavCollection` representing the root of the hierarchy
    /// served by `dandidav`
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

    /// Construct a `DavCollection` representing the list of Dandisets at
    /// `/dandisets/`
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

    /// Construct a `DavCollection` representing the listing for the given
    /// Dandiset's published versions at `/dandiset/{dandiset_id}/releases/`
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

    /// Construct a `DavCollection` representing the Dandiset version `v`
    /// as served at path `path`
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

    /// Construct a `DavCollection` representing the top of the Zarr manifest
    /// tree at `/zarrs/`
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

/// Information on a non-collection resource
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct DavItem {
    /// The path at which the resource is served by `dandidav`
    pub(super) path: PurePath,

    /// The timestamp at which the resource was created
    pub(super) created: Option<OffsetDateTime>,

    /// The timestamp at which the resource was last modified
    pub(super) modified: Option<OffsetDateTime>,

    /// The resource's Content-Type/MIME type
    pub(super) content_type: String,

    /// The size of the resource
    pub(super) size: Option<i64>,

    /// The resource's ETag
    pub(super) etag: Option<String>,

    /// The type of resource, for display in the "Type" column of HTML tables
    pub(super) kind: ResourceKind,

    /// The content of the resource or a link to it
    pub(super) content: DavContent,

    /// A URL for retrieving the resource's associated metadata (if any) from
    /// the Archive instance
    pub(super) metadata_url: Option<Url>,
}

impl DavItem {
    /// Return the base name of the resource's path
    pub(super) fn name(&self) -> &str {
        self.path.name_str()
    }

    /// Return the link to use for the resource in the HTML view of its parent
    /// collection as an absolute URL path (including leading slash)
    pub(super) fn web_link(&self) -> Href {
        if let DavContent::Redirect(ref redir) = self.content {
            // Link directly to the download URL in the web view in order to
            // save a request
            redir.get_url(false).into()
        } else {
            Href::from_path(&format!("/{}", self.path))
        }
    }

    /// Prefix the resource's path with the path at which `dandidav` serves the
    /// given Dandiset & version under `/dandisets/`.
    ///
    /// See [`version_path()`] for more information.
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

/// The content of a non-collection resource or a link thereto
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum DavContent {
    /// The raw content to serve in response to a `GET` request for the
    /// resource.
    ///
    /// This is only used for `dandiset.yaml` resources, for which the content
    /// is automatically generated by `dandidav`.
    Blob(Vec<u8>),

    /// A URL that `dandidav` should redirect to when a `GET` request is made
    /// for the resource
    Redirect(Redirect),

    /// No download URL could be determined for the resource
    Missing,
}

/// A URL or choice of URLs to redirect a request to
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum Redirect {
    /// A single URL to always redirect to
    Direct(Url),

    /// An S3 URL and an Archive instance URL, to be selected between based on
    /// whether `--prefer-s3-redirects` was supplied at program invocation
    Alt { s3: Url, archive: Url },
}

impl Redirect {
    /// Resolve to a single URL.
    ///
    /// If `prefer_s3` is `true`, `Alt` variants resolve to their `s3` field;
    /// otherwise, they resolve to their `archive` field.
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

/// An enumeration of resource types for use in the "Type" column of HTML views
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) enum ResourceKind {
    /// The root of the hierarchy served by `dandidav`
    Root,

    /// Link to parent directory
    Parent,

    /// The list of Dandisets at `/dandisets/`
    DandisetIndex,

    /// A listing for a Dandiset at `/dandiset/{dandiset_id}/`
    Dandiset,

    /// A listing for a Dandiset's published versions at
    /// `/dandiset/{dandiset_id}/releases/`
    DandisetReleases,

    /// A listing of the top level of a Dandiset version's file hierarchy
    Version,

    /// The `dandiset.yaml` file for a Dandiset version
    VersionMetadata,

    /// A generic directory
    Directory,

    /// A blob asset
    Blob,

    /// A Zarr asset
    Zarr,

    /// A Zarr entry
    ZarrEntry,

    /// The top of the Zarr manifest tree at `/zarrs/`
    ZarrIndex,
}

impl ResourceKind {
    /// Return a human-readable string to display in a "Type" column
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
