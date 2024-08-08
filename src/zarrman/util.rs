use super::resources::ManifestPath;
use crate::dav::ErrorClass;
use crate::httputil::HttpError;
use crate::paths::{Component, PurePath};
use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum ZarrManError {
    /// An HTTP error occurred while interacting with the manifest tree
    #[error(transparent)]
    Http(#[from] HttpError),

    /// The request path was invalid for the `/zarrs/` hierarchy
    #[error("invalid path requested: {path:?}")]
    InvalidPath { path: PurePath },

    /// An request was made for a nonexistent path inside an extant Zarr
    #[error("path {entry_path:?} inside manifest at {manifest_path:?} does not exist")]
    ManifestPathNotFound {
        manifest_path: ManifestPath,
        entry_path: PurePath,
    },
}

impl ZarrManError {
    /// Classify the general type of error
    pub(crate) fn class(&self) -> ErrorClass {
        match self {
            ZarrManError::Http(source) => source.class(),
            ZarrManError::InvalidPath { .. } | ZarrManError::ManifestPathNotFound { .. } => {
                ErrorClass::NotFound
            }
        }
    }
}

/// A directory listing parsed from the response to a `GET` request to a
/// directory in the manifest tree
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub(super) struct Index {
    // Returned by the manifest tree API but not used by dandidav:
    //pub(super) path: String,
    /// The names of the files in the directory
    pub(super) files: Vec<Component>,
    /// The names of the subdirectories of the directory
    pub(super) directories: Vec<Component>,
}
