//! Rendering resource listings as HTML documents
use super::util::Href;
use super::{DavCollection, DavItem, DavResource, ResourceKind};
use crate::consts::HTML_TIMESTAMP_FORMAT;
use crate::paths::Component;
use humansize::{format_size_i, BINARY};
use serde::{ser::Serializer, Serialize};
use std::collections::HashMap;
use tera::{Context, Error, Filter, Tera, Value};
use thiserror::Error;
use time::OffsetDateTime;

/// The [Tera](https://keats.github.io/tera/) template for HTML collection
/// views
static COLLECTION_TEMPLATE: &str = include_str!("templates/collection.html.tera");

/// A template manager
pub(crate) struct Templater {
    /// Tera templater
    engine: Tera,

    /// Site title to display in HTML responses
    title: String,
}

impl Templater {
    /// Create a new templater with site title `title` and load all templates
    /// into it
    ///
    /// # Errors
    ///
    /// If any template fails to load, a [`TemplateError::Load`] is returned.
    pub(crate) fn new(title: String) -> Result<Self, TemplateError> {
        let mut engine = Tera::default();
        engine.register_filter("formatsize", FormatSizeFilter);
        engine
            .add_raw_template("collection.html", COLLECTION_TEMPLATE)
            .map_err(|source| TemplateError::Load {
                template_name: "collection.html",
                source,
            })?;
        Ok(Templater { engine, title })
    }

    /// Render an HTML document containing a table listing the resources in
    /// `entries`.  `pathparts` contains the individual components of the
    /// request URL path.
    pub(super) fn render_collection(
        &self,
        entries: Vec<DavResource>,
        pathparts: Vec<Component>,
    ) -> Result<String, TemplateError> {
        let template_name = "collection.html";
        let colctx = self.collection_context(entries, pathparts);
        let context =
            Context::from_serialize(colctx).map_err(|source| TemplateError::MakeContext {
                template_name,
                source,
            })?;
        self.engine
            .render(template_name, &context)
            .map_err(|source| TemplateError::Render {
                template_name,
                source,
            })
    }

    /// Construct the context for displaying the given `entries`.  `pathparts`
    /// contains the individual components of the request URL path.
    fn collection_context(
        &self,
        entries: Vec<DavResource>,
        pathparts: Vec<Component>,
    ) -> CollectionContext {
        let mut rows = entries.into_iter().map(ColRow::from).collect::<Vec<_>>();
        rows.sort_unstable();
        if let Some((_, pp)) = pathparts.split_last() {
            rows.insert(
                0,
                ColRow::parentdir(Href::from_path(&abs_dir_from_components(pp))),
            );
        }
        let title_path = abs_dir_from_components(&pathparts);
        let title = format!("{} \u{2014} {}", self.title, title_path);
        CollectionContext {
            title,
            breadcrumbs: self.make_breadcrumbs(pathparts),
            rows,
            package_url: env!("CARGO_PKG_REPOSITORY"),
            package_version: env!("CARGO_PKG_VERSION"),
            package_commit: option_env!("GIT_COMMIT"),
        }
    }

    /// Create breadcrumbs for the given request URL path components
    fn make_breadcrumbs(&self, pathparts: Vec<Component>) -> Vec<Link> {
        let mut links = Vec::with_capacity(pathparts.len().saturating_add(1));
        let mut cumpath = String::from("/");
        links.push(Link {
            text: self.title.clone(),
            href: Href::from_path(&cumpath),
        });
        for p in pathparts {
            cumpath.push_str(&p);
            cumpath.push('/');
            links.push(Link {
                text: p.into(),
                href: Href::from_path(&cumpath),
            });
        }
        links
    }
}

/// Context to provide to the `collection.html` template
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct CollectionContext {
    /// Page title
    title: String,

    /// Breadcrumb links
    breadcrumbs: Vec<Link>,

    /// Rows of the table
    rows: Vec<ColRow>,

    /// URL to link "dandidav" in the page's footer to
    package_url: &'static str,

    /// `dandidav` version
    package_version: &'static str,

    /// Current `dandidav` commit hash (if known)
    #[serde(skip_serializing_if = "Option::is_none")]
    package_commit: Option<&'static str>,
}

/// A hyperlink to display in an HTML document
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct Link {
    /// The text of the link
    text: String,

    /// The value of the link's `href` attribute
    href: Href,
}

/// A row of a table listing the resources within a collection
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
struct ColRow {
    /// Resource basename
    name: String,

    /// URL to link the resource to
    href: Href,

    /// `true` iff the resource is a collection
    is_dir: bool,

    /// Type of resource
    kind: ResourceKind,

    /// The size of the resource
    #[serde(skip_serializing_if = "Option::is_none")]
    size: Option<i64>,

    /// The timestamp at which the resource was created
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "maybe_timestamp"
    )]
    created: Option<OffsetDateTime>,

    /// The timestamp at which the resource was last modified
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "maybe_timestamp"
    )]
    modified: Option<OffsetDateTime>,

    /// A URL for retrieving the resource's associated metadata (if any) from
    /// the Archive instance
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata_url: Option<Href>,
}

impl ColRow {
    /// Construct a `ColRow` representing the parent of the current collection,
    /// with the parent being served at `href`
    fn parentdir(href: Href) -> ColRow {
        ColRow {
            name: "..".to_owned(),
            href,
            is_dir: true,
            kind: ResourceKind::Parent,
            size: None,
            created: None,
            modified: None,
            metadata_url: None,
        }
    }
}

impl From<DavResource> for ColRow {
    fn from(res: DavResource) -> ColRow {
        match res {
            DavResource::Collection(col) => col.into(),
            DavResource::Item(item) => item.into(),
        }
    }
}

impl From<DavCollection> for ColRow {
    fn from(col: DavCollection) -> ColRow {
        ColRow {
            name: col.name().unwrap_or("/").to_owned(),
            href: col.web_link(),
            is_dir: true,
            kind: col.kind,
            size: col.size,
            created: col.created,
            modified: col.modified,
            metadata_url: col.metadata_url.map(Into::into),
        }
    }
}

impl From<DavItem> for ColRow {
    fn from(item: DavItem) -> ColRow {
        ColRow {
            name: item.name().to_owned(),
            href: item.web_link(),
            is_dir: false,
            kind: item.kind,
            size: item.size,
            created: item.created,
            modified: item.modified,
            metadata_url: item.metadata_url.map(Into::into),
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum TemplateError {
    /// Failed to load a template
    #[error("failed to load template {template_name:?}")]
    Load {
        template_name: &'static str,
        source: Error,
    },

    /// Failed to create context for a template
    #[error("failed to create context for template {template_name:?}")]
    MakeContext {
        template_name: &'static str,
        source: Error,
    },

    /// Failed to render a template
    #[error("failed to render template {template_name:?}")]
    Render {
        template_name: &'static str,
        source: Error,
    },
}

/// If `ts` is non-`None`, format it and serialize the resulting string to
/// `serializer`
fn maybe_timestamp<S: Serializer>(
    ts: &Option<OffsetDateTime>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    match ts.as_ref() {
        Some(ts) => {
            let s = ts
                .to_offset(time::UtcOffset::UTC)
                .format(&HTML_TIMESTAMP_FORMAT)
                .expect("timestamp formatting should not fail");
            serializer.serialize_some(&s)
        }
        None => serializer.serialize_none(),
    }
}

/// Given an iterator of `&Component` values, join them together with forward
/// slashes and add a leading & trailing slash.
fn abs_dir_from_components<'a, I>(iter: I) -> String
where
    I: IntoIterator<Item = &'a Component>,
{
    let mut s = String::from("/");
    for p in iter {
        s.push_str(p);
        s.push('/');
    }
    s
}

/// A custom Tera filter for formatting file sizes.
///
/// Unlike the `filesizeformat` filter built into Tera, this filter uses binary
/// units with unambiguous abbreviations, e.g., "10 KiB".
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct FormatSizeFilter;

impl Filter for FormatSizeFilter {
    fn filter(&self, value: &Value, _args: &HashMap<String, Value>) -> Result<Value, Error> {
        match value.as_i64() {
            Some(size) => Ok(formatsize(size).into()),
            None => Err(Error::msg("Input to formatsize filter must be an integer")),
        }
    }

    fn is_safe(&self) -> bool {
        true
    }
}

/// Format a file size in binary units using unambiguous abbreviations.
///
/// This function is separate from `FormatSizeFilter` for testing purposes.
fn formatsize(size: i64) -> String {
    format_size_i(size, BINARY)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dav::types::Redirect;
    use rstest::rstest;

    #[rstest]
    #[case(0, "0 B")]
    #[case(42, "42 B")]
    #[case(1000, "1000 B")]
    #[case(1024, "1 KiB")]
    #[case(1025, "1.00 KiB")]
    #[case(1525, "1.49 KiB")]
    #[case(1535, "1.50 KiB")]
    #[case(1536, "1.50 KiB")]
    #[case(10240, "10 KiB")]
    #[case(10752, "10.50 KiB")]
    fn test_formatsize(#[case] size: i64, #[case] s: &str) {
        assert_eq!(formatsize(size), s);
    }

    mod render_collection {
        use super::*;
        use crate::dav::{DavContent, DavResourceWithChildren};
        use pretty_assertions::assert_eq;
        use std::borrow::Cow;
        use time::macros::datetime;

        #[test]
        fn basic() {
            let templater = Templater::new("Dandidav Test".to_owned()).unwrap();
            let entries = vec![
                DavResource::Collection(DavCollection {
                    path: Some("foo/bar/baz/a.zarr/".parse().unwrap()),
                    created: Some(datetime!(2021-01-01 01:23:45 UTC)),
                    modified: Some(datetime!(2023-12-31 12:34:56 UTC)),
                    size: Some(1234567890),
                    kind: ResourceKind::Zarr,
                    metadata_url: None,
                }),
                DavResource::Collection(DavCollection {
                    path: Some(r#"foo/bar/baz/"quoted"/"#.parse().unwrap()),
                    created: None,
                    modified: None,
                    size: None,
                    kind: ResourceKind::Directory,
                    metadata_url: None,
                }),
                DavResource::Item(DavItem {
                    path: "foo/bar/baz/empty.txt".parse().unwrap(),
                    created: Some(datetime!(2024-02-14 22:13:22 -5)),
                    modified: Some(datetime!(2024-02-14 22:13:35 -5)),
                    content_type: Some("text/plain".into()),
                    size: Some(0),
                    etag: Some(r#""00000000""#.into()),
                    kind: ResourceKind::Blob,
                    content: DavContent::Redirect(Redirect::Direct(
                        "https://dandiarchive-test.s3.amazonaws.com/blobs/empty.txt"
                            .parse()
                            .unwrap(),
                    )),
                    metadata_url: Some(
                        "https://api-test.dandiarchive.org/blobs/?name=empty.txt"
                            .parse()
                            .unwrap(),
                    ),
                }),
                DavResource::Item(DavItem {
                    path: "foo/bar/baz/spaced file.dat".parse().unwrap(),
                    created: Some(datetime!(2021-02-03 06:47:50 UTC)),
                    modified: Some(datetime!(2022-03-10 12:03:29 UTC)),
                    content_type: Some("application/octet-stream".into()),
                    size: Some(123456),
                    etag: Some(r#""abcdefgh""#.into()),
                    kind: ResourceKind::Blob,
                    content: DavContent::Redirect(Redirect::Direct(
                        "https://dandiarchive-test.s3.amazonaws.com/blobs/spaced%20file.dat"
                            .parse()
                            .unwrap(),
                    )),
                    metadata_url: Some(
                        "https://api-test.dandiarchive.org/blobs/?name=spaced%20file.dat"
                            .parse()
                            .unwrap(),
                    ),
                }),
                DavResource::Item(DavItem {
                    path: "foo/bar/baz/dandiset.yaml".parse().unwrap(),
                    created: None,
                    modified: None,
                    content_type: Some("text/yaml".into()),
                    size: Some(42),
                    etag: None,
                    kind: ResourceKind::VersionMetadata,
                    content: DavContent::Blob(Vec::new()),
                    metadata_url: None,
                }),
            ];
            let rendered = templater
                .render_collection(
                    entries,
                    vec![
                        "foo".parse().unwrap(),
                        "bar".parse().unwrap(),
                        "baz".parse().unwrap(),
                    ],
                )
                .unwrap();
            let commit_str = match option_env!("GIT_COMMIT") {
                Some(s) => Cow::from(format!(", commit {s}")),
                None => Cow::from(""),
            };
            let expected = include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/src/testdata/render-collection/basic.html"
            ))
            .replacen(
                "{package_url}",
                &env!("CARGO_PKG_REPOSITORY").replace('/', "&#x2F;"),
                1,
            )
            .replacen("{version}", env!("CARGO_PKG_VERSION"), 1)
            .replacen("{commit}", &commit_str, 1);
            assert_eq!(rendered, expected);
        }

        #[test]
        fn root() {
            let templater = Templater::new("Dandidav Test".to_owned()).unwrap();
            let DavResourceWithChildren::Collection { children, .. } =
                DavResourceWithChildren::root()
            else {
                panic!("DavResourceWithChildren::root() should be a Collection");
            };
            let rendered = templater.render_collection(children, Vec::new()).unwrap();
            let commit_str = match option_env!("GIT_COMMIT") {
                Some(s) => Cow::from(format!(", commit {s}")),
                None => Cow::from(""),
            };
            let expected = include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/src/testdata/render-collection/root.html"
            ))
            .replacen(
                "{package_url}",
                &env!("CARGO_PKG_REPOSITORY").replace('/', "&#x2F;"),
                1,
            )
            .replacen("{version}", env!("CARGO_PKG_VERSION"), 1)
            .replacen("{commit}", &commit_str, 1);
            assert_eq!(rendered, expected);
        }
    }
}
