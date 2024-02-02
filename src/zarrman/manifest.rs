use crate::paths::PurePath;
use itertools::{Itertools, Position};
use serde::Deserialize;
use std::collections::BTreeMap;
use time::OffsetDateTime;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub(crate) struct Manifest {
    entries: ManifestFolder,
}

impl Manifest {
    pub(super) fn get(&self, path: &PurePath) -> Option<EntryRef<'_>> {
        let mut folder = &self.entries;
        for (pos, p) in path.components().with_position() {
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
    #[serde(with = "time::serde::rfc3339")]
    modified: OffsetDateTime,
    size: i64,
    etag: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use indoc::indoc;
    use time::macros::datetime;

    #[test]
    fn test_manifest() {
        let s = indoc! {r#"
        {
         "schemaVersion": 2,
         "fields": ["versionId","lastModified","size","ETag"],
         "statistics": {
          "entries": 509,
          "depth": 5,
          "totalSize": 710206390,
          "lastModified": "2022-06-27T23:09:39+00:00",
          "zarrChecksum": "6ddc4625befef8d6f9796835648162be-509--710206390"
         },
         "entries": {
          ".zattrs": ["VwOSu7IVLAQcQHcqOesmlrEDm2sL_Tfs","2022-06-27T23:07:47+00:00",8312,"cb32b88f6488d55818aba94746bcc19a"],
          ".zgroup": ["7obAY5BUNOdI1Uch3RoI4oHuGXhW4h0R","2022-06-27T23:07:47+00:00",24,"e20297935e73dd0154104d4ea53040ab"],
          ".zmetadata": ["Vfe0W0v4zkydmzyXkUMjm2Xr7.rIvfZQ","2022-06-27T23:07:47+00:00",15191,"4f505878fbb943a9793516cf084e07ad"],
          "0": {
           ".zarray": ["Ou6TnKwWPmEJrL.0utCWLPxgfr_lA0I1","2022-06-27T23:07:48+00:00",446,"5477ec3da352681e5ba6f6ea550ef740"],
           "0": {
            "0": {
             "13": {
              "8": {
               "100": ["lqNZ6OQ6lKd2QRW8ekWOiVfdZhiicWsh","2022-06-27T23:09:11+00:00",1793451,"7b5af4c6c28047c83dd86e4814bc0272"],
               "101": ["_i9cZBerb4mB9D8IFbPHo8nrefWcbq0p","2022-06-27T23:09:28+00:00",1799564,"50b6cfb69609319da9bf900a21d5f25c"]
              }
             }
            }
           }
          }
         }
        }
        "#};

        let manifest = serde_json::from_str::<Manifest>(s).unwrap();
        let zattrs = ManifestEntry {
            version_id: "VwOSu7IVLAQcQHcqOesmlrEDm2sL_Tfs".into(),
            modified: datetime!(2022-06-27 23:07:47 UTC),
            size: 8312,
            etag: "cb32b88f6488d55818aba94746bcc19a".into(),
        };
        let zarray = ManifestEntry {
            version_id: "Ou6TnKwWPmEJrL.0utCWLPxgfr_lA0I1".into(),
            modified: datetime!(2022-06-27 23:07:48 UTC),
            size: 446,
            etag: "5477ec3da352681e5ba6f6ea550ef740".into(),
        };
        let entry_100 = ManifestEntry {
            version_id: "lqNZ6OQ6lKd2QRW8ekWOiVfdZhiicWsh".into(),
            modified: datetime!(2022-06-27 23:09:11 UTC),
            size: 1793451,
            etag: "7b5af4c6c28047c83dd86e4814bc0272".into(),
        };

        assert_eq!(
            manifest,
            Manifest {
                entries: BTreeMap::from([
                    (".zattrs".into(), FolderEntry::Entry(zattrs.clone())),
                    (
                        ".zgroup".into(),
                        FolderEntry::Entry(ManifestEntry {
                            version_id: "7obAY5BUNOdI1Uch3RoI4oHuGXhW4h0R".into(),
                            modified: datetime!(2022-06-27 23:07:47 UTC),
                            size: 24,
                            etag: "e20297935e73dd0154104d4ea53040ab".into(),
                        })
                    ),
                    (
                        ".zmetadata".into(),
                        FolderEntry::Entry(ManifestEntry {
                            version_id: "Vfe0W0v4zkydmzyXkUMjm2Xr7.rIvfZQ".into(),
                            modified: datetime!(2022-06-27 23:07:47 UTC),
                            size: 15191,
                            etag: "4f505878fbb943a9793516cf084e07ad".into(),
                        })
                    ),
                    (
                        "0".into(),
                        FolderEntry::Folder(BTreeMap::from([
                            (".zarray".into(), FolderEntry::Entry(zarray.clone())),
                            (
                                "0".into(),
                                FolderEntry::Folder(BTreeMap::from([(
                                    "0".into(),
                                    FolderEntry::Folder(BTreeMap::from([(
                                        "13".into(),
                                        FolderEntry::Folder(BTreeMap::from([(
                                            "8".into(),
                                            FolderEntry::Folder(BTreeMap::from([
                                                (
                                                    "100".into(),
                                                    FolderEntry::Entry(entry_100.clone())
                                                ),
                                                (
                                                    "101".into(),
                                                    FolderEntry::Entry(ManifestEntry {
                                                        version_id:
                                                            "_i9cZBerb4mB9D8IFbPHo8nrefWcbq0p"
                                                                .into(),
                                                        modified: datetime!(2022-06-27 23:09:28 UTC),
                                                        size: 1799564,
                                                        etag: "50b6cfb69609319da9bf900a21d5f25c"
                                                            .into(),
                                                    })
                                                ),
                                            ]))
                                        )]))
                                    )]))
                                )]))
                            )
                        ]))
                    )
                ])
            }
        );

        assert_eq!(
            manifest.get(&".zattrs".parse::<PurePath>().unwrap()),
            Some(EntryRef::Entry(&zattrs))
        );
        assert_eq!(
            manifest.get(&"not-found".parse::<PurePath>().unwrap()),
            None,
        );
        assert_eq!(
            manifest.get(&".zattrs/0".parse::<PurePath>().unwrap()),
            None,
        );
        assert_eq!(
            manifest.get(&"0/.zarray".parse::<PurePath>().unwrap()),
            Some(EntryRef::Entry(&zarray))
        );
        assert_eq!(
            manifest.get(&"0/not-found".parse::<PurePath>().unwrap()),
            None,
        );
        assert_eq!(
            manifest.get(&"0/0/0/13/8/100".parse::<PurePath>().unwrap()),
            Some(EntryRef::Entry(&entry_100))
        );
        assert_matches!(
            manifest.get(&"0/0/0/13/8".parse::<PurePath>().unwrap()),
            Some(EntryRef::Folder(folder)) => {
                assert_eq!(folder.keys().collect::<Vec<_>>(), ["100", "101"]);
            }
        );
    }
}
