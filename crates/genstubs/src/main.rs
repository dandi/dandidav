use clap::Parser;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

const PAGE_SIZE: usize = 25;

#[derive(Clone, Debug, Eq, PartialEq, Parser)]
struct Arguments {
    /// Path to the directory in which the YAML mock specs are located
    specdir: PathBuf,

    /// Path to the directory in which to create the API JSON response stubs
    stubdir: PathBuf,
}

fn main() -> anyhow::Result<()> {
    let args = Arguments::parse();
    let dandisets_yaml = fs_err::read_to_string(args.specdir.join("dandisets.yaml"))?;
    let dandiset_specs = serde_yaml::from_str::<Vec<DandisetSpec>>(&dandisets_yaml)?;
    let mut dandisets = Vec::new();
    for ds in dandiset_specs {
        let mut versions = Vec::new();
        for v in ds.versions {
            let assets_yaml = fs_err::read_to_string(
                args.specdir
                    .join("assets")
                    .join(&ds.identifier)
                    .join(format!("{}.yaml", v.version)),
            )?;
            let mut assets = serde_yaml::from_str::<Vec<Asset>>(&assets_yaml)?;
            assets.sort_unstable_by(|a, b| a.properties.path.cmp(&b.properties.path));
            let asset_count = assets.len();
            let size = assets.iter().map(|a| a.properties.size).sum();
            versions.push(Version {
                id: v.version.clone(),
                api_payload: ApiVersion {
                    version: v.version,
                    name: v.name,
                    asset_count,
                    size,
                    status: v.status,
                    created: v.created,
                    modified: v.modified,
                },
                metadata: v.metadata,
                assets,
                atpath: v.atpath,
            });
        }
        let mrpv = versions
            .iter()
            .filter(|v| v.id != "draft")
            .max_by(|a, b| a.api_payload.created.cmp(&b.api_payload.created))
            .map(|v| v.api_payload.clone());
        let Some(draft) = versions
            .iter()
            .find(|v| v.id == "draft")
            .map(|v| v.api_payload.clone())
        else {
            anyhow::bail!("Dandiset {} does not have a draft version", ds.identifier);
        };
        dandisets.push(Dandiset {
            id: ds.identifier.clone(),
            api_payload: ApiDandiset {
                identifier: ds.identifier,
                created: ds.created,
                modified: ds.modified,
                contact_person: ds.contact_person,
                embargo_status: ds.embargo_status,
                most_recent_published_version: mrpv,
                draft_version: draft,
            },
            versions,
        });
    }

    dandisets.sort_unstable_by(|a, b| a.id.cmp(&b.id));
    dump_json(
        &paginate(
            &dandisets
                .iter()
                .map(|ds| &ds.api_payload)
                .collect::<Vec<_>>(),
            "/api/dandisets/",
            BTreeMap::new(),
        ),
        args.stubdir.join("api").join("dandisets.json"),
    )?;

    let mut atpath_responses = Vec::new();

    for d in dandisets {
        dump_json(
            &vec![Stub::from(&d.api_payload)],
            args.stubdir
                .join("api")
                .join("dandisets")
                .join(format!("{}.json", d.id)),
        )?;

        dump_json(
            &paginate(
                &d.versions
                    .iter()
                    .map(|v| &v.api_payload)
                    .collect::<Vec<_>>(),
                &format!("/api/dandisets/{}/versions/", d.id),
                BTreeMap::new(),
            ),
            args.stubdir
                .join("api")
                .join("dandisets")
                .join(&d.id)
                .join("versions.json"),
        )?;

        for v in d.versions {
            dump_json(
                &vec![Stub::from(&v.metadata)],
                args.stubdir
                    .join("api")
                    .join("dandisets")
                    .join(&d.id)
                    .join("versions")
                    .join(format!("{}.json", v.id)),
            )?;

            dump_json(
                &vec![Stub::from(&ApiVersionInfo {
                    version: &v.api_payload,
                    metadata: &v.metadata,
                })],
                args.stubdir
                    .join("api")
                    .join("dandisets")
                    .join(&d.id)
                    .join("versions")
                    .join(&v.id)
                    .join("info.json"),
            )?;

            for p in v.atpath.assets_no_children {
                let assets = v
                    .assets
                    .iter()
                    .filter(|a| a.properties.path.starts_with(&p))
                    .map(|a| AtPathResource::Asset(a.clone()))
                    .collect::<Vec<_>>();
                atpath_responses.extend(paginate(
                    &assets,
                    "/api/webdav/assets/atpath/",
                    BTreeMap::from([
                        ("dandiset_id".to_owned(), d.id.clone()),
                        ("version_id".to_owned(), v.id.clone()),
                        ("path".to_owned(), p.clone()),
                        ("metadata".to_owned(), "true".to_owned()),
                    ]),
                ));
            }

            for p in v.atpath.assets {
                let assets = v
                    .assets
                    .iter()
                    .filter(|a| a.properties.path.starts_with(&p))
                    .map(|a| AtPathResource::Asset(a.clone()))
                    .collect::<Vec<_>>();
                atpath_responses.extend(paginate(
                    &assets,
                    "/api/webdav/assets/atpath/",
                    BTreeMap::from([
                        ("dandiset_id".to_owned(), d.id.clone()),
                        ("version_id".to_owned(), v.id.clone()),
                        ("path".to_owned(), p.clone()),
                        ("children".to_owned(), "true".to_owned()),
                        ("metadata".to_owned(), "true".to_owned()),
                    ]),
                ));
            }

            for dirpath in v.atpath.folders {
                let prefix = match dirpath {
                    Some(ref p) => format!("{p}/"),
                    None => String::new(),
                };
                let mut assets_in_dir = Vec::new();
                let mut dirs_in_dir = BTreeMap::<String, AssetCounter>::new();
                let mut total_count = AssetCounter::default();
                for a in &v.assets {
                    if let Some(p) = a.properties.path.strip_prefix(&prefix) {
                        total_count.add(a);
                        if let Some((pre, _)) = p.split_once('/') {
                            dirs_in_dir.entry(pre.to_owned()).or_default().add(a);
                        } else {
                            assets_in_dir.push(a.clone());
                        }
                    }
                }
                let mut entries = assets_in_dir
                    .into_iter()
                    .map(AtPathResource::Asset)
                    .chain(dirs_in_dir.into_iter().map(|(path, counts)| {
                        AtPathResource::Folder(AssetFolder {
                            path: format!("{prefix}{path}"),
                            total_assets: counts.count,
                            total_size: counts.total_size,
                        })
                    }))
                    .collect::<Vec<_>>();
                let mut params = BTreeMap::from([
                    ("dandiset_id".to_owned(), d.id.clone()),
                    ("version_id".to_owned(), v.id.clone()),
                    ("children".to_owned(), "true".to_owned()),
                    ("metadata".to_owned(), "true".to_owned()),
                ]);
                if let Some(ref p) = dirpath {
                    entries.push(AtPathResource::Folder(AssetFolder {
                        path: p.to_owned(),
                        total_assets: total_count.count,
                        total_size: total_count.total_size,
                    }));
                    params.insert("path".to_owned(), p.to_owned());
                }
                entries.sort_unstable_by(|a, b| a.path().cmp(b.path()));
                atpath_responses.extend(paginate(&entries, "/api/webdav/assets/atpath/", params));
            }
        }
    }

    if !atpath_responses.is_empty() {
        dump_json(
            &atpath_responses,
            args.stubdir
                .join("api")
                .join("webdav")
                .join("assets")
                .join("atpath.json"),
        )?;
    }

    Ok(())
}

fn paginate<T: Clone>(
    mut items: &[T],
    path: &str,
    params: BTreeMap<String, String>,
) -> Vec<Stub<Page<T>>> {
    let mut pages = Vec::new();
    let count = items.len();
    let mut pageno = 1;
    let query_prefix = params
        .iter()
        .format_with("", |(k, v), f| f(&format_args!("{k}={v}&")))
        .to_string();
    loop {
        let len = items.len().min(PAGE_SIZE);
        if len == 0 && pageno != 1 {
            break;
        }
        let (results, items2) = items.split_at(len);
        items = items2;
        let mut params = params.clone();
        params.insert("page_size".to_owned(), PAGE_SIZE.to_string());
        if pageno != 1 {
            params.insert("page".to_owned(), pageno.to_string());
        }
        pages.push(Stub {
            params,
            response: Page {
                count,
                next: (!items.is_empty()).then(|| {
                    format!(
                        "{{base_url}}{path}?{query_prefix}page={next_page}&page_size={PAGE_SIZE}",
                        next_page = pageno + 1
                    )
                }),
                results: results.to_vec(),
            },
        });
        pageno += 1;
    }
    pages
}

fn dump_json<T: Serialize, P: Into<PathBuf>>(value: &T, path: P) -> anyhow::Result<()> {
    let path = path.into();
    if let Some(pp) = path.parent() {
        fs_err::create_dir_all(pp)?;
    }
    let mut fp = BufWriter::new(fs_err::File::create(path)?);
    serde_json::to_writer_pretty(&mut fp, value)?;
    fp.write_all(b"\n")?;
    fp.flush()?;
    Ok(())
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
struct DandisetSpec {
    identifier: String,
    created: String,
    modified: String,
    contact_person: String,
    embargo_status: String,
    versions: Vec<VersionSpec>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
struct VersionSpec {
    version: String,
    name: String,
    status: String,
    created: String,
    modified: String,
    metadata: serde_json::Value,
    #[serde(default)]
    atpath: AtPathSpec,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
struct AtPathSpec {
    #[serde(default)]
    assets: Vec<String>,
    #[serde(default)]
    assets_no_children: Vec<String>,
    #[serde(default)]
    folders: Vec<Option<String>>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct Asset {
    #[serde(flatten)]
    properties: ApiAssetProperties,
    metadata: serde_json::Value,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct ApiDandiset {
    identifier: String,
    created: String,
    modified: String,
    contact_person: String,
    embargo_status: String,
    most_recent_published_version: Option<ApiVersion>,
    draft_version: ApiVersion,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct ApiVersion {
    version: String,
    name: String,
    asset_count: usize,
    size: u64,
    status: String,
    created: String,
    modified: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct ApiVersionInfo<'a> {
    #[serde(flatten)]
    version: &'a ApiVersion,
    metadata: &'a serde_json::Value,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ApiAssetProperties {
    asset_id: String,
    blob: Option<String>,
    zarr: Option<String>,
    path: String,
    size: u64,
    created: String,
    modified: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Dandiset {
    id: String,
    api_payload: ApiDandiset,
    versions: Vec<Version>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Version {
    id: String,
    api_payload: ApiVersion,
    metadata: serde_json::Value,
    assets: Vec<Asset>,
    atpath: AtPathSpec,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct Stub<T> {
    params: BTreeMap<String, String>,
    response: T,
}

impl<T> From<T> for Stub<T> {
    fn from(response: T) -> Stub<T> {
        Stub {
            params: BTreeMap::new(),
            response,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct Page<T> {
    count: usize,
    next: Option<String>,
    //previous
    results: Vec<T>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct AssetCounter {
    count: usize,
    total_size: u64,
}

impl AssetCounter {
    fn add(&mut self, asset: &Asset) {
        self.count += 1;
        self.total_size += asset.properties.size;
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "type", content = "resource", rename_all = "lowercase")]
enum AtPathResource {
    Folder(AssetFolder),
    Asset(Asset),
}

impl AtPathResource {
    fn path(&self) -> &str {
        match self {
            AtPathResource::Folder(AssetFolder { path, .. }) => path,
            AtPathResource::Asset(Asset { properties, .. }) => &properties.path,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct AssetFolder {
    path: String,
    total_assets: usize,
    total_size: u64,
}
