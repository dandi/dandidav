use super::ResourceKind;
use crate::consts::HTML_TIMESTAMP_FORMAT;
use serde::{ser::Serializer, Serialize};
use tera::{Context, Error, Tera};
use thiserror::Error;
use time::OffsetDateTime;

static COLLECTION_TEMPLATE: &str = include_str!("templates/collection.html.tera");

pub(crate) struct Templater(Tera);

impl Templater {
    pub(super) fn load() -> Result<Self, TemplateError> {
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
    title: String,
    rows: Vec<ColRow>,
    package_url: &'static str,
    package_version: &'static str,
    package_commit: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(super) struct ColRow {
    name: String,
    is_dir: bool,
    kind: ResourceKind,
    size: Option<i64>,
    #[serde(serialize_with = "maybe_timestamp")]
    created: Option<OffsetDateTime>,
    #[serde(serialize_with = "maybe_timestamp")]
    modified: Option<OffsetDateTime>,
}

#[derive(Debug, Error)]
pub(super) enum TemplateError {
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
