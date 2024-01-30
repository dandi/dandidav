use super::util::Href;
use super::{DavCollection, DavItem, DavResource, ResourceKind};
use crate::consts::HTML_TIMESTAMP_FORMAT;
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
    pub(super) rows: Vec<ColRow>,
    pub(super) package_url: &'static str,
    pub(super) package_version: &'static str,
    pub(super) package_commit: Option<&'static str>,
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
                .format(&HTML_TIMESTAMP_FORMAT)
                .expect("timestamp formatting should not fail");
            serializer.serialize_some(&s)
        }
        None => serializer.serialize_none(),
    }
}
