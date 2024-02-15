use super::util::Href;
use super::{DavCollection, DavItem, DavResource, ResourceKind};
use crate::consts::HTML_TIMESTAMP_FORMAT;
use crate::paths::Component;
use serde::{ser::Serializer, Serialize};
use tera::{Context, Error, Tera};
use thiserror::Error;
use time::OffsetDateTime;

static COLLECTION_TEMPLATE: &str = include_str!("templates/collection.html.tera");

pub(crate) struct Templater(Tera);

impl Templater {
    pub(crate) fn load() -> Result<Self, TemplateError> {
        let mut engine = Tera::default();
        engine
            .add_raw_template("collection.html", COLLECTION_TEMPLATE)
            .map_err(|source| TemplateError::Load {
                path: "collection.html",
                source,
            })?;
        Ok(Templater(engine))
    }

    pub(super) fn render_collection(
        &self,
        context: CollectionContext,
    ) -> Result<String, TemplateError> {
        let context =
            Context::from_serialize(context).map_err(|source| TemplateError::MakeContext {
                path: "collection.html",
                source,
            })?;
        self.0
            .render("collection.html", &context)
            .map_err(|source| TemplateError::Render {
                path: "collection.html",
                source,
            })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(super) struct CollectionContext {
    pub(super) title: String,
    pub(super) breadcrumbs: Vec<Link>,
    pub(super) rows: Vec<ColRow>,
    pub(super) package_url: &'static str,
    pub(super) package_version: &'static str,
    pub(super) package_commit: Option<&'static str>,
}

impl CollectionContext {
    pub(super) fn new(
        entries: Vec<DavResource>,
        title: &str,
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
        let full_title = format!("{title} \u{2014} {title_path}");
        CollectionContext {
            title: full_title,
            breadcrumbs: make_breadcrumbs(title, pathparts),
            rows,
            package_url: env!("CARGO_PKG_REPOSITORY"),
            package_version: env!("CARGO_PKG_VERSION"),
            package_commit: option_env!("GIT_COMMIT"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(super) struct Link {
    name: String,
    href: Href,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct ColRow {
    name: String,
    href: Href,
    is_dir: bool,
    kind: ResourceKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    size: Option<i64>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "maybe_timestamp"
    )]
    created: Option<OffsetDateTime>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "maybe_timestamp"
    )]
    modified: Option<OffsetDateTime>,
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata_url: Option<Href>,
}

impl ColRow {
    pub(super) fn parentdir(href: Href) -> ColRow {
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
    #[error("failed to load template {path:?}")]
    Load { path: &'static str, source: Error },
    #[error("failed to create context for template {path:?}")]
    MakeContext { path: &'static str, source: Error },
    #[error("failed to render template {path:?}")]
    Render { path: &'static str, source: Error },
}

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

fn make_breadcrumbs(title: &str, pathparts: Vec<Component>) -> Vec<Link> {
    let mut links = Vec::with_capacity(pathparts.len().saturating_add(1));
    let mut cumpath = String::from("/");
    links.push(Link {
        name: title.to_owned(),
        href: Href::from_path(&cumpath),
    });
    for p in pathparts {
        cumpath.push_str(&p);
        cumpath.push('/');
        links.push(Link {
            name: p.into(),
            href: Href::from_path(&cumpath),
        });
    }
    links
}

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

#[cfg(test)]
mod tests {
    use super::*;

    mod render_collection {
        use super::*;
        use crate::dav::{DavContent, DavResourceWithChildren};
        use pretty_assertions::assert_eq;
        use std::borrow::Cow;
        use time::macros::datetime;

        #[test]
        fn basic() {
            let templater = Templater::load().unwrap();
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
                    content_type: "text/plain".into(),
                    size: Some(0),
                    etag: Some(r#""00000000""#.into()),
                    kind: ResourceKind::Blob,
                    content: DavContent::Redirect(
                        "https://dandiarchive-test.s3.amazonaws.com/blobs/empty.txt"
                            .parse()
                            .unwrap(),
                    ),
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
                    content_type: "application/octet-stream".into(),
                    size: Some(123456),
                    etag: Some(r#""abcdefgh""#.into()),
                    kind: ResourceKind::Blob,
                    content: DavContent::Redirect(
                        "https://dandiarchive-test.s3.amazonaws.com/blobs/spaced%20file.dat"
                            .parse()
                            .unwrap(),
                    ),
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
                    content_type: "text/yaml".into(),
                    size: Some(42),
                    etag: None,
                    kind: ResourceKind::VersionMetadata,
                    content: DavContent::Blob(Vec::new()),
                    metadata_url: None,
                }),
            ];
            let context = CollectionContext::new(
                entries,
                "Dandidav Test",
                vec![
                    "foo".parse().unwrap(),
                    "bar".parse().unwrap(),
                    "baz".parse().unwrap(),
                ],
            );
            let rendered = templater.render_collection(context).unwrap();
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
            let templater = Templater::load().unwrap();
            let DavResourceWithChildren::Collection { children, .. } =
                DavResourceWithChildren::root()
            else {
                panic!("DavResourceWithChildren::root() should be a Collection");
            };
            let context = CollectionContext::new(children, "Dandidav Test", Vec::new());
            let rendered = templater.render_collection(context).unwrap();
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
