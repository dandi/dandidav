#![cfg(test)]
use super::*;
use crate::consts::YAML_CONTENT_TYPE;
use axum::body::Bytes;
use http_body_util::BodyExt; // for `collect`
use indoc::indoc;
use testutils::{CollectionEntry, CollectionPage, Link};
use tower::{Service, ServiceExt}; // for `ready`

fn fill_html_footer(html: &str) -> String {
    let commit_str = match option_env!("GIT_COMMIT") {
        Some(s) => std::borrow::Cow::from(format!(", commit {s}")),
        None => std::borrow::Cow::from(""),
    };
    html.replacen(
        "{package_url}",
        &env!("CARGO_PKG_REPOSITORY").replace('/', "&#x2F;"),
        1,
    )
    .replacen("{version}", env!("CARGO_PKG_VERSION"), 1)
    .replacen("{commit}", &commit_str, 1)
}

#[derive(Debug)]
struct MockApp {
    app: Router,
    #[allow(dead_code)]
    mock_archive: wiremock::MockServer,
    archive_url: String,
}

impl MockApp {
    async fn new() -> MockApp {
        let mock_archive = testutils::make_mock_archive(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/testdata/stubs"
        ))
        .await;
        let archive_url = format!("{}/api", mock_archive.uri());
        let cfg = Config {
            api_url: archive_url.parse::<HttpUrl>().unwrap(),
            ..Config::default()
        };
        let app = get_app(cfg).unwrap();
        MockApp {
            app,
            mock_archive,
            archive_url,
        }
    }

    async fn get(&mut self, path: &str) -> Response<Bytes> {
        let response = <Router as ServiceExt<Request<Body>>>::ready(&mut self.app)
            .await
            .unwrap()
            .call(
                Request::builder()
                    .uri(path)
                    .header("X-Forwarded-For", "127.0.0.1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let (parts, body) = response.into_parts();
        let body = body.collect().await.unwrap().to_bytes();
        Response::from_parts(parts, body)
    }

    async fn head(&mut self, path: &str) -> Response<Bytes> {
        let response = <Router as ServiceExt<Request<Body>>>::ready(&mut self.app)
            .await
            .unwrap()
            .call(
                Request::builder()
                    .method(Method::HEAD)
                    .uri(path)
                    .header("X-Forwarded-For", "127.0.0.1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let (parts, body) = response.into_parts();
        let body = body.collect().await.unwrap().to_bytes();
        Response::from_parts(parts, body)
    }

    async fn get_collection_html(&mut self, path: &str) -> CollectionPage {
        let response = self.get(path).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some(HTML_CONTENT_TYPE)
        );
        let body = std::str::from_utf8(response.body()).unwrap();
        testutils::parse_collection_page(body).unwrap()
    }
}

#[tokio::test]
async fn test_get_styles() {
    let mut app = MockApp::new().await;
    let response = app.get("/.static/styles.css").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
        Some(CSS_CONTENT_TYPE)
    );
    assert!(!response.headers().contains_key("DAV"));
    let body = String::from_utf8_lossy(response.body());
    assert_eq!(body, STYLESHEET);
}

#[tokio::test]
async fn test_get_root() {
    let mut app = MockApp::new().await;
    let response = app.get("/").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
        Some(HTML_CONTENT_TYPE)
    );
    assert!(response.headers().contains_key("DAV"));
    let body = String::from_utf8_lossy(response.body());
    let expected = fill_html_footer(include_str!("testdata/index.html"));
    assert_eq!(body, expected);
}

#[tokio::test]
async fn test_head_styles() {
    let mut app = MockApp::new().await;
    let response = app.head("/.static/styles.css").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
        Some(CSS_CONTENT_TYPE)
    );
    assert_eq!(
        response
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<usize>().ok()),
        Some(STYLESHEET.len())
    );
    assert!(!response.headers().contains_key("DAV"));
    assert!(response.body().is_empty());
}

#[tokio::test]
async fn test_head_root() {
    let mut app = MockApp::new().await;
    let response = app.head("/").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
        Some(HTML_CONTENT_TYPE)
    );
    let expected = fill_html_footer(include_str!("testdata/index.html"));
    assert_eq!(
        response
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<usize>().ok()),
        Some(expected.len())
    );
    assert!(response.headers().contains_key("DAV"));
    assert!(response.body().is_empty());
}

#[tokio::test]
async fn test_get_dandisets_index() {
    let mut app = MockApp::new().await;
    let page = app.get_collection_html("/dandisets/").await;
    pretty_assertions::assert_eq!(
        page,
        CollectionPage {
            breadcrumbs: vec![
                Link {
                    text: "dandidav".into(),
                    href: "/".into()
                },
                Link {
                    text: "dandisets".into(),
                    href: "/dandisets/".into()
                },
            ],
            table: vec![
                CollectionEntry {
                    name: Link {
                        text: "../".into(),
                        href: "/".into()
                    },
                    metadata_link: None,
                    typekind: "Parent directory".into(),
                    size: "\u{2014}".into(),
                    created: "\u{2014}".into(),
                    modified: "\u{2014}".into(),
                },
                CollectionEntry {
                    name: Link {
                        text: "000001/".into(),
                        href: "/dandisets/000001/".into()
                    },
                    metadata_link: None,
                    typekind: "Dandiset".into(),
                    size: "\u{2014}".into(),
                    created: "2020-03-15 22:56:55Z".into(),
                    modified: "2020-11-06 17:20:30Z".into(),
                },
                CollectionEntry {
                    name: Link {
                        text: "000002/".into(),
                        href: "/dandisets/000002/".into()
                    },
                    metadata_link: None,
                    typekind: "Dandiset".into(),
                    size: "\u{2014}".into(),
                    created: "2020-03-16 21:48:04Z".into(),
                    modified: "2020-10-03 07:01:25Z".into(),
                },
                CollectionEntry {
                    name: Link {
                        text: "000003/".into(),
                        href: "/dandisets/000003/".into()
                    },
                    metadata_link: None,
                    typekind: "Dandiset".into(),
                    size: "\u{2014}".into(),
                    created: "2020-03-16 22:52:44Z".into(),
                    modified: "2020-04-09 20:59:35Z".into(),
                },
            ],
        }
    );
}

#[tokio::test]
async fn test_get_dandiset_with_published() {
    let mut app = MockApp::new().await;
    let page = app.get_collection_html("/dandisets/000001/").await;
    pretty_assertions::assert_eq!(
        page,
        CollectionPage {
            breadcrumbs: vec![
                Link {
                    text: "dandidav".into(),
                    href: "/".into()
                },
                Link {
                    text: "dandisets".into(),
                    href: "/dandisets/".into()
                },
                Link {
                    text: "000001".into(),
                    href: "/dandisets/000001/".into()
                },
            ],
            table: vec![
                CollectionEntry {
                    name: Link {
                        text: "../".into(),
                        href: "/dandisets/".into()
                    },
                    metadata_link: None,
                    typekind: "Parent directory".into(),
                    size: "\u{2014}".into(),
                    created: "\u{2014}".into(),
                    modified: "\u{2014}".into(),
                },
                CollectionEntry {
                    name: Link {
                        text: "draft/".into(),
                        href: "/dandisets/000001/draft/".into()
                    },
                    metadata_link: Some(format!(
                        "{}/dandisets/000001/versions/draft/",
                        app.archive_url
                    )),
                    typekind: "Dandiset version".into(),
                    size: "18.35 KiB".into(),
                    created: "2020-03-15 22:56:55Z".into(),
                    modified: "2024-05-18 17:13:27Z".into(),
                },
                CollectionEntry {
                    name: Link {
                        text: "latest/".into(),
                        href: "/dandisets/000001/latest/".into()
                    },
                    metadata_link: Some(format!(
                        "{}/dandisets/000001/versions/0.230629.1955/",
                        app.archive_url
                    )),
                    typekind: "Dandiset version".into(),
                    size: "171.91 KiB".into(),
                    created: "2023-06-29 19:55:31Z".into(),
                    modified: "2023-06-29 19:55:35Z".into(),
                },
                CollectionEntry {
                    name: Link {
                        text: "releases/".into(),
                        href: "/dandisets/000001/releases/".into()
                    },
                    metadata_link: None,
                    typekind: "Published versions".into(),
                    size: "\u{2014}".into(),
                    created: "\u{2014}".into(),
                    modified: "\u{2014}".into(),
                },
            ],
        }
    );
}

#[tokio::test]
async fn test_get_dandiset_without_published() {
    let mut app = MockApp::new().await;
    let page = app.get_collection_html("/dandisets/000003/").await;
    pretty_assertions::assert_eq!(
        page,
        CollectionPage {
            breadcrumbs: vec![
                Link {
                    text: "dandidav".into(),
                    href: "/".into()
                },
                Link {
                    text: "dandisets".into(),
                    href: "/dandisets/".into()
                },
                Link {
                    text: "000003".into(),
                    href: "/dandisets/000003/".into()
                },
            ],
            table: vec![
                CollectionEntry {
                    name: Link {
                        text: "../".into(),
                        href: "/dandisets/".into()
                    },
                    metadata_link: None,
                    typekind: "Parent directory".into(),
                    size: "\u{2014}".into(),
                    created: "\u{2014}".into(),
                    modified: "\u{2014}".into(),
                },
                CollectionEntry {
                    name: Link {
                        text: "draft/".into(),
                        href: "/dandisets/000003/draft/".into()
                    },
                    metadata_link: Some(format!(
                        "{}/dandisets/000003/versions/draft/",
                        app.archive_url
                    )),
                    typekind: "Dandiset version".into(),
                    size: "87.86 MiB".into(),
                    created: "2020-03-16 22:52:44Z".into(),
                    modified: "2020-04-09 20:59:35Z".into(),
                },
            ],
        }
    );
}

#[tokio::test]
async fn test_get_dandiset_releases() {
    let mut app = MockApp::new().await;
    let page = app.get_collection_html("/dandisets/000001/releases/").await;
    pretty_assertions::assert_eq!(
        page,
        CollectionPage {
            breadcrumbs: vec![
                Link {
                    text: "dandidav".into(),
                    href: "/".into()
                },
                Link {
                    text: "dandisets".into(),
                    href: "/dandisets/".into()
                },
                Link {
                    text: "000001".into(),
                    href: "/dandisets/000001/".into()
                },
                Link {
                    text: "releases".into(),
                    href: "/dandisets/000001/releases/".into()
                },
            ],
            table: vec![
                CollectionEntry {
                    name: Link {
                        text: "../".into(),
                        href: "/dandisets/000001/".into()
                    },
                    metadata_link: None,
                    typekind: "Parent directory".into(),
                    size: "\u{2014}".into(),
                    created: "\u{2014}".into(),
                    modified: "\u{2014}".into(),
                },
                CollectionEntry {
                    name: Link {
                        text: "0.210512.1623/".into(),
                        href: "/dandisets/000001/releases/0.210512.1623/".into()
                    },
                    metadata_link: Some(format!(
                        "{}/dandisets/000001/versions/0.210512.1623/",
                        app.archive_url
                    )),
                    typekind: "Dandiset version".into(),
                    size: "40.54 MiB".into(),
                    created: "2021-05-12 16:23:14Z".into(),
                    modified: "2021-05-12 16:23:19Z".into(),
                },
                CollectionEntry {
                    name: Link {
                        text: "0.230629.1955/".into(),
                        href: "/dandisets/000001/releases/0.230629.1955/".into()
                    },
                    metadata_link: Some(format!(
                        "{}/dandisets/000001/versions/0.230629.1955/",
                        app.archive_url
                    )),
                    typekind: "Dandiset version".into(),
                    size: "171.91 KiB".into(),
                    created: "2023-06-29 19:55:31Z".into(),
                    modified: "2023-06-29 19:55:35Z".into(),
                },
            ],
        }
    );
}

#[tokio::test]
async fn test_get_version_toplevel() {
    let mut app = MockApp::new().await;
    let page = app.get_collection_html("/dandisets/000002/draft/").await;
    pretty_assertions::assert_eq!(
        page,
        CollectionPage {
            breadcrumbs: vec![
                Link {
                    text: "dandidav".into(),
                    href: "/".into()
                },
                Link {
                    text: "dandisets".into(),
                    href: "/dandisets/".into()
                },
                Link {
                    text: "000002".into(),
                    href: "/dandisets/000002/".into()
                },
                Link {
                    text: "draft".into(),
                    href: "/dandisets/000002/draft/".into()
                },
            ],
            table: vec![
                CollectionEntry {
                    name: Link {
                        text: "../".into(),
                        href: "/dandisets/000002/".into()
                    },
                    metadata_link: None,
                    typekind: "Parent directory".into(),
                    size: "\u{2014}".into(),
                    created: "\u{2014}".into(),
                    modified: "\u{2014}".into(),
                },
                CollectionEntry {
                    name: Link {
                        text: "0tRyK6Tf.nwb".into(),
                        href: "https://api.dandiarchive.org/api/assets/3dd294c8-0296-4b88-bf5c-427700982bc5/download/".into()
                    },
                    metadata_link: Some(format!(
                        "{}/dandisets/000002/versions/draft/assets/3dd294c8-0296-4b88-bf5c-427700982bc5/",
                        app.archive_url
                    )),
                    typekind: "Blob asset".into(),
                    size: "4.25 KiB".into(),
                    created: "2022-10-15 22:36:14Z".into(),
                    modified: "2024-12-21 09:05:49Z".into(),
                },
                CollectionEntry {
                    name: Link {
                        text: "2jsP1o.nwb".into(),
                        href: "https://api.dandiarchive.org/api/assets/60e3e62f-679d-4903-8ff9-eed67cb83947/download/".into()
                    },
                    metadata_link: Some(format!(
                        "{}/dandisets/000002/versions/draft/assets/60e3e62f-679d-4903-8ff9-eed67cb83947/",
                        app.archive_url
                    )),
                    typekind: "Blob asset".into(),
                    size: "3.50 KiB".into(),
                    created: "2020-12-12 13:01:04Z".into(),
                    modified: "2024-10-01 03:43:25Z".into(),
                },
                CollectionEntry {
                    name: Link {
                        text: "30l.nwb".into(),
                        href: "https://api.dandiarchive.org/api/assets/7204338c-874b-4081-a494-d8b1988561d6/download/".into()
                    },
                    metadata_link: Some(format!(
                        "{}/dandisets/000002/versions/draft/assets/7204338c-874b-4081-a494-d8b1988561d6/",
                        app.archive_url
                    )),
                    typekind: "Blob asset".into(),
                    size: "8.15 KiB".into(),
                    created: "2020-10-22 01:27:45Z".into(),
                    modified: "2024-12-17 19:07:43Z".into(),
                },
                CollectionEntry {
                    name: Link {
                        text: "F2GW.nwb".into(),
                        href: "https://api.dandiarchive.org/api/assets/fa25bbd0-e053-46a6-8404-a4f88457c898/download/".into()
                    },
                    metadata_link: Some(format!(
                        "{}/dandisets/000002/versions/draft/assets/fa25bbd0-e053-46a6-8404-a4f88457c898/",
                        app.archive_url
                    )),
                    typekind: "Blob asset".into(),
                    size: "7.47 KiB".into(),
                    created: "2022-12-22 14:41:05Z".into(),
                    modified: "2024-05-06 15:45:55Z".into(),
                },
                CollectionEntry {
                    name: Link {
                        text: "JEhE.tsv".into(),
                        href: "https://api.dandiarchive.org/api/assets/faeb54dc-f906-40fa-b48e-28816c422ec5/download/".into()
                    },
                    metadata_link: Some(format!(
                        "{}/dandisets/000002/versions/draft/assets/faeb54dc-f906-40fa-b48e-28816c422ec5/",
                        app.archive_url
                    )),
                    typekind: "Blob asset".into(),
                    size: "5.55 KiB".into(),
                    created: "2022-03-28 09:43:13Z".into(),
                    modified: "2024-09-28 19:10:09Z".into(),
                },
                CollectionEntry {
                    name: Link {
                        text: "KsAtGTXP/".into(),
                        href: "/dandisets/000002/draft/KsAtGTXP/".into()
                    },
                    metadata_link: None,
                    typekind: "Directory".into(),
                    size: "\u{2014}".into(),
                    created: "\u{2014}".into(),
                    modified: "\u{2014}".into(),
                },
                CollectionEntry {
                    name: Link {
                        text: "bM1QHwex.nwb".into(),
                        href: "https://api.dandiarchive.org/api/assets/f4274f89-301e-4ce9-9425-a096970cfcba/download/".into()
                    },
                    metadata_link: Some(format!(
                        "{}/dandisets/000002/versions/draft/assets/f4274f89-301e-4ce9-9425-a096970cfcba/",
                        app.archive_url
                    )),
                    typekind: "Blob asset".into(),
                    size: "4.39 KiB".into(),
                    created: "2020-02-13 22:02:27Z".into(),
                    modified: "2023-09-02 01:54:48Z".into(),
                },
                CollectionEntry {
                    name: Link {
                        text: "dandiset.yaml".into(),
                        href: "/dandisets/000002/draft/dandiset.yaml".into(),
                    },
                    metadata_link: None,
                    typekind: "Version metadata".into(),
                    size: "410 B".into(),
                    created: "\u{2014}".into(),
                    modified: "\u{2014}".into(),
                },
                CollectionEntry {
                    name: Link {
                        text: "fRLy/".into(),
                        href: "/dandisets/000002/draft/fRLy/".into()
                    },
                    metadata_link: None,
                    typekind: "Directory".into(),
                    size: "\u{2014}".into(),
                    created: "\u{2014}".into(),
                    modified: "\u{2014}".into(),
                },
                CollectionEntry {
                    name: Link {
                        text: "nPjB.json".into(),
                        href: "https://api.dandiarchive.org/api/assets/08938c9b-b248-4aa0-b963-859029a1f38b/download/".into()
                    },
                    metadata_link: Some(format!(
                        "{}/dandisets/000002/versions/draft/assets/08938c9b-b248-4aa0-b963-859029a1f38b/",
                        app.archive_url
                    )),
                    typekind: "Blob asset".into(),
                    size: "11.62 KiB".into(),
                    created: "2020-04-27 04:10:28Z".into(),
                    modified: "2024-12-30 08:57:09Z".into(),
                },
                CollectionEntry {
                    name: Link {
                        text: "ykBgN.tsv".into(),
                        href: "https://api.dandiarchive.org/api/assets/3a4bc5da-4920-467b-9a82-4e1a9cac90b7/download/".into()
                    },
                    metadata_link: Some(format!(
                        "{}/dandisets/000002/versions/draft/assets/3a4bc5da-4920-467b-9a82-4e1a9cac90b7/",
                        app.archive_url
                    )),
                    typekind: "Blob asset".into(),
                    size: "8.51 KiB".into(),
                    created: "2022-01-13 21:11:18Z".into(),
                    modified: "2024-12-11 08:29:21Z".into(),
                },
            ],
        }
    );
}

#[tokio::test]
async fn test_get_asset_folder() {
    let mut app = MockApp::new().await;
    let page = app
        .get_collection_html("/dandisets/000002/draft/fRLy/")
        .await;
    // TODO: Expand into a full comparison
    pretty_assertions::assert_eq!(
        page.breadcrumbs,
        vec![
            Link {
                text: "dandidav".into(),
                href: "/".into()
            },
            Link {
                text: "dandisets".into(),
                href: "/dandisets/".into()
            },
            Link {
                text: "000002".into(),
                href: "/dandisets/000002/".into()
            },
            Link {
                text: "draft".into(),
                href: "/dandisets/000002/draft/".into()
            },
            Link {
                text: "fRLy".into(),
                href: "/dandisets/000002/draft/fRLy/".into()
            },
        ]
    );
    assert_eq!(
        page.into_names(),
        vec![
            "../",
            "9xj.nwb",
            "EZP9eyg/",
            "GpJEYT9.ngff/",
            "NYW8SD/",
            "OWG.nwb",
            "UP8CFrHpN/",
            "hH.nwb",
            "yY9p4f.nwb",
            "zBbN.nwb",
            "zfa6zGT.zarr/"
        ]
    );
}

#[tokio::test]
async fn get_dandiset_yaml() {
    let mut app = MockApp::new().await;
    let response = app.get("/dandisets/000001/draft/dandiset.yaml").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
        Some(YAML_CONTENT_TYPE)
    );
    assert!(response.headers().contains_key("DAV"));
    let body = String::from_utf8_lossy(response.body());
    pretty_assertions::assert_eq!(
        body,
        indoc! {"
      '@context': https://raw.githubusercontent.com/dandi/schema/master/releases/0.6.0/context.json
      dateCreated: 2020-03-15T22:56:55.655000+00:00
      description: Researcher is seeking funding for surgery to fix goring injuries.
      id: DANDI:000001/draft
      identifier: DANDI:000027
      license:
      - spdx:CC-BY-4.0
      name: Brainscan of a Unicorn
      schemaKey: Dandiset
      url: https://dandiarchive.mock/dandiset/000001/draft
      version: draft
    "}
    );
}
