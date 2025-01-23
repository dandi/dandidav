use clap::Parser;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

const PER_PAGE: usize = 25;

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
                asset_dirs: v.asset_dirs,
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
            let mut assets_responses = Vec::new();
            let mut assets_paths_responses = Vec::new();
            for dirpath in v.asset_dirs {
                let prefix = match dirpath {
                    Some(ref p) => format!("{p}/"),
                    None => String::new(),
                };
                let mut assets_in_dir = Vec::new();
                let mut dirs_in_dir = BTreeMap::<String, AssetCounter>::new();
                for a in &v.assets {
                    if let Some(p) = a.properties.path.strip_prefix(&prefix) {
                        if let Some((pre, _)) = p.split_once('/') {
                            dirs_in_dir.entry(pre.to_owned()).or_default().add(a);
                        } else {
                            assets_in_dir.push(a);
                        }
                    }
                }
                let mut entries = assets_in_dir
                    .iter()
                    .copied()
                    .map(AssetPathsEntry::for_asset)
                    .chain(dirs_in_dir.into_iter().map(|(path, counts)| {
                        AssetPathsEntry::for_folder(format!("{prefix}{path}"), counts)
                    }))
                    .collect::<Vec<_>>();
                entries.sort_unstable_by(|a, b| a.path.cmp(&b.path));
                let params = match dirpath {
                    Some(ref p) => BTreeMap::from([("path_prefix".to_owned(), format!("{p}/"))]),
                    None => BTreeMap::new(),
                };
                assets_paths_responses.extend(paginate(
                    &entries,
                    &format!("/api/dandisets/{}/versions/{}/assets/paths/", d.id, v.id),
                    params,
                ));
                for a in assets_in_dir {
                    dump_json(
                        &vec![Stub::from(a)],
                        args.stubdir
                            .join("api")
                            .join("dandisets")
                            .join(&d.id)
                            .join("versions")
                            .join(&v.id)
                            .join("assets")
                            .join(&a.properties.asset_id)
                            .join("info.json"),
                    )?;
                }
                if let Some(p) = dirpath {
                    let assets = v
                        .assets
                        .iter()
                        .filter(|a| a.properties.path.starts_with(&p))
                        .collect::<Vec<_>>();
                    assets_responses.extend(paginate(
                        &assets,
                        &format!("/api/dandisets/{}/versions/{}/assets/", d.id, v.id),
                        BTreeMap::from([
                            ("metadata".to_owned(), "1".to_owned()),
                            ("order".to_owned(), "path".to_owned()),
                            ("path".to_owned(), p),
                        ]),
                    ));
                }
            }
            if !assets_responses.is_empty() {
                dump_json(
                    &assets_responses,
                    args.stubdir
                        .join("api")
                        .join("dandisets")
                        .join(&d.id)
                        .join("versions")
                        .join(&v.id)
                        .join("assets.json"),
                )?;
            }
            if !assets_paths_responses.is_empty() {
                dump_json(
                    &assets_paths_responses,
                    args.stubdir
                        .join("api")
                        .join("dandisets")
                        .join(&d.id)
                        .join("versions")
                        .join(&v.id)
                        .join("assets")
                        .join("paths.json"),
                )?;
            }
        }
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
        let len = items.len().min(PER_PAGE);
        if len == 0 && pageno != 1 {
            break;
        }
        let (results, items2) = items.split_at(len);
        items = items2;
        pages.push(Stub {
            params: if pageno == 1 {
                params.clone()
            } else {
                let mut p2 = params.clone();
                p2.insert("page".to_owned(), pageno.to_string());
                p2
            },
            response: Page {
                count,
                next: (!items.is_empty()).then(|| {
                    format!(
                        "{{base_url}}{path}?{query_prefix}page={next_page}",
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
    asset_dirs: Vec<Option<String>>,
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
    asset_dirs: Vec<Option<String>>,
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
struct AssetPathsEntry {
    path: String,
    aggregate_files: usize,
    aggregate_size: u64,
    asset: Option<AssetPathsAsset>,
}

impl AssetPathsEntry {
    fn for_asset(asset: &Asset) -> AssetPathsEntry {
        AssetPathsEntry {
            path: asset.properties.path.clone(),
            aggregate_files: 1,
            aggregate_size: asset.properties.size,
            asset: Some(AssetPathsAsset {
                asset_id: asset.properties.asset_id.clone(),
            }),
        }
    }

    fn for_folder(path: String, size: AssetCounter) -> AssetPathsEntry {
        AssetPathsEntry {
            path,
            aggregate_files: size.count,
            aggregate_size: size.total_size,
            asset: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct AssetPathsAsset {
    asset_id: String,
}
