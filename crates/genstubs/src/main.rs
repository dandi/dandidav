use clap::Parser;
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
        }
    }
    Ok(())
}

fn paginate<T: Clone>(mut items: &[T], path: &str) -> Vec<Stub<Page<T>>> {
    let mut pages = Vec::new();
    let count = items.len();
    let mut first = true;
    let mut next_page = 2;
    loop {
        let len = items.len().min(PER_PAGE);
        if len == 0 && !first {
            break;
        }
        let (results, items2) = items.split_at(len);
        items = items2;
        pages.push(Stub {
            params: BTreeMap::new(),
            response: Page {
                count,
                next: (!items.is_empty()).then(|| format!("{{base_url}}{path}?page={next_page}")),
                results: results.to_vec(),
            },
        });
        first = false;
        next_page += 1;
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
