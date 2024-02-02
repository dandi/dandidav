use crate::paths::PurePath;
use itertools::{Itertools, Position};
use serde::Deserialize;
use std::collections::BTreeMap;
use time::OffsetDateTime;

// TODO: Add a deserialization test
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub(crate) struct Manifest {
    entries: ManifestFolder,
}

impl Manifest {
    pub(super) fn get(&self, path: &PurePath) -> Option<EntryRef<'_>> {
        let mut folder = &self.entries;
        for (pos, p) in path.split('/').with_position() {
            match folder.get(p)? {
                FolderEntry::Folder(f) => folder = f,
                FolderEntry::Entry(e) if matches!(pos, Position::Last | Position::Only) => {
                    return Some(EntryRef::Entry(e))
                }
                FolderEntry::Entry(_) => return None,
            }
        }
        Some(EntryRef::Folder(folder))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum EntryRef<'a> {
    Folder(&'a ManifestFolder),
    Entry(&'a ManifestEntry),
}

pub(super) type ManifestFolder = BTreeMap<String, FolderEntry>;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(untagged)]
pub(crate) enum FolderEntry {
    Folder(ManifestFolder),
    Entry(ManifestEntry),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub(crate) struct ManifestEntry {
    // Keep these fields in this order so that deserialization will work
    // properly!
    version_id: String,
    size: i64,
    #[serde(with = "time::serde::rfc3339")]
    modified: OffsetDateTime,
    etag: String,
}
