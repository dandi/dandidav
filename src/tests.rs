#![cfg(test)]
use super::*;
use crate::consts::{DAV_XML_CONTENT_TYPE, YAML_CONTENT_TYPE};
use axum::body::Bytes;
use http_body_util::BodyExt; // for `collect`
use indoc::indoc;
use testutils::{CollectionEntry, CollectionPage, Link, Resource, Trinary};
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
        MockApp::with_config(Config::default()).await
    }

    async fn with_config(mut cfg: Config) -> MockApp {
        let mock_archive = testutils::make_mock_archive(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/testdata/stubs"
        ))
        .await;
        let archive_url = format!("{}/api", mock_archive.uri());
        cfg.api_url = archive_url.parse::<HttpUrl>().unwrap();
        let app = get_app(cfg).unwrap();
        MockApp {
            app,
            mock_archive,
            archive_url,
        }
    }

    async fn request(&mut self, req: Request) -> Response<Bytes> {
        let response = <Router as ServiceExt<Request<Body>>>::ready(&mut self.app)
            .await
            .unwrap()
            .call(req)
            .await
            .unwrap();
        let (parts, body) = response.into_parts();
        let body = body.collect().await.unwrap().to_bytes();
        Response::from_parts(parts, body)
    }

    async fn get(&mut self, path: &str) -> Response<Bytes> {
        self.request(
            Request::builder()
                .uri(path)
                .header("X-Forwarded-For", "127.0.0.1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
    }

    async fn head(&mut self, path: &str) -> Response<Bytes> {
        self.request(
            Request::builder()
                .method(Method::HEAD)
                .uri(path)
                .header("X-Forwarded-For", "127.0.0.1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
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

    fn propfind(&mut self, path: &'static str) -> Propfinder<'_> {
        Propfinder::new(self, path)
    }
}

#[derive(Debug)]
struct Propfinder<'a> {
    app: &'a mut MockApp,
    path: &'static str,
    body: Option<&'static str>,
    depth: Option<&'static str>,
}

impl<'a> Propfinder<'a> {
    fn new(app: &'a mut MockApp, path: &'static str) -> Self {
        Propfinder {
            app,
            path,
            body: None,
            depth: Some("1"),
        }
    }

    /*
    fn body(mut self, body: &'static str) -> Self {
        self.body = Some(body);
        self
    }
    */

    fn depth(mut self, depth: &'static str) -> Self {
        self.depth = Some(depth);
        self
    }

    fn no_depth(mut self) -> Self {
        self.depth = None;
        self
    }

    async fn send(self) -> PropfindResponse {
        let mut req = Request::builder()
            .method("PROPFIND")
            .uri(self.path)
            .header("X-Forwarded-For", "127.0.0.1");
        if let Some(depth) = self.depth {
            req = req.header("Depth", depth);
        }
        let req = req
            .body(self.body.map_or_else(Body::empty, Body::from))
            .unwrap();
        let resp = self.app.request(req).await;
        PropfindResponse(resp)
    }
}

#[derive(Clone, Debug)]
struct PropfindResponse(Response<Bytes>);

impl PropfindResponse {
    fn assert_status(self, status: StatusCode) -> Self {
        assert_eq!(self.0.status(), status);
        self
    }

    fn assert_header(self, header: axum::http::header::HeaderName, value: &str) -> Self {
        assert_eq!(
            self.0.headers().get(header).and_then(|v| v.to_str().ok()),
            Some(value)
        );
        self
    }

    fn success(self) -> Self {
        self.assert_status(StatusCode::MULTI_STATUS)
            .assert_header(CONTENT_TYPE, DAV_XML_CONTENT_TYPE)
    }

    fn into_resources(self) -> Vec<Resource> {
        let body = std::str::from_utf8(self.0.body()).unwrap();
        testutils::parse_propfind_response(body).unwrap()
    }

    fn assert_body(self, expected: &str) -> Self {
        let body = std::str::from_utf8(self.0.body()).unwrap();
        assert_eq!(body, expected);
        self
    }
}

#[tokio::test]
async fn get_styles() {
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
async fn get_root() {
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
async fn head_styles() {
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
async fn head_root() {
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
async fn propfind_root_depth_0() {
    let mut app = MockApp::new().await;
    let resources = app
        .propfind("/")
        .depth("0")
        .send()
        .await
        .success()
        .into_resources();
    pretty_assertions::assert_eq!(
        resources,
        vec![Resource {
            href: "/".into(),
            creation_date: Trinary::Void,
            display_name: Trinary::Void,
            content_length: Trinary::Void,
            content_type: Trinary::Void,
            last_modified: Trinary::Void,
            etag: Trinary::Void,
            language: Trinary::Void,
            is_collection: Some(true),
        }],
    );
}

#[tokio::test]
async fn propfind_root_depth_1() {
    let mut app = MockApp::new().await;
    let resources = app
        .propfind("/")
        .depth("1")
        .send()
        .await
        .success()
        .into_resources();
    pretty_assertions::assert_eq!(
        resources,
        vec![
            Resource {
                href: "/".into(),
                creation_date: Trinary::Void,
                display_name: Trinary::Void,
                content_length: Trinary::Void,
                content_type: Trinary::Void,
                last_modified: Trinary::Void,
                etag: Trinary::Void,
                language: Trinary::Void,
                is_collection: Some(true),
            },
            Resource {
                href: "/dandisets/".into(),
                creation_date: Trinary::Void,
                display_name: Trinary::Set("dandisets".into()),
                content_length: Trinary::Void,
                content_type: Trinary::Void,
                last_modified: Trinary::Void,
                etag: Trinary::Void,
                language: Trinary::Void,
                is_collection: Some(true),
            },
            Resource {
                href: "/zarrs/".into(),
                creation_date: Trinary::Void,
                display_name: Trinary::Set("zarrs".into()),
                content_length: Trinary::Void,
                content_type: Trinary::Void,
                last_modified: Trinary::Void,
                etag: Trinary::Void,
                language: Trinary::Void,
                is_collection: Some(true),
            },
        ],
    );
}

#[tokio::test]
async fn get_dandisets_index() {
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
async fn get_dandiset_with_published() {
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
async fn get_dandiset_without_published() {
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
async fn get_dandiset_releases() {
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
                    size: "40.52 MiB".into(),
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
async fn get_version_toplevel() {
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
async fn get_asset_folder() {
    let mut app = MockApp::new().await;
    let page = app
        .get_collection_html("/dandisets/000002/draft/fRLy/")
        .await;
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
                Link {
                    text: "fRLy".into(),
                    href: "/dandisets/000002/draft/fRLy/".into()
                },
            ],
            table: vec![
                CollectionEntry {
                    name: Link {
                        text: "../".into(),
                        href: "/dandisets/000002/draft/".into()
                    },
                    metadata_link: None,
                    typekind: "Parent directory".into(),
                    size: "\u{2014}".into(),
                    created: "\u{2014}".into(),
                    modified: "\u{2014}".into(),
                },
                CollectionEntry {
                    name: Link {
                        text: "9xj.nwb".into(),
                        href: "https://api.dandiarchive.org/api/assets/b82113fc-48e4-4645-a52f-d8fdf47e1624/download/".into()
                    },
                    metadata_link: Some(format!(
                        "{}/dandisets/000002/versions/draft/assets/b82113fc-48e4-4645-a52f-d8fdf47e1624/",
                        app.archive_url
                    )),
                    typekind: "Blob asset".into(),
                    size: "24.28 KiB".into(),
                    created: "2023-06-03 21:54:42Z".into(),
                    modified: "2024-12-30 01:25:21Z".into(),
                },
                CollectionEntry {
                    name: Link {
                        text: "EZP9eyg/".into(),
                        href: "/dandisets/000002/draft/fRLy/EZP9eyg/".into()
                    },
                    metadata_link: None,
                    typekind: "Directory".into(),
                    size: "\u{2014}".into(),
                    created: "\u{2014}".into(),
                    modified: "\u{2014}".into(),
                },
                CollectionEntry {
                    name: Link {
                        text: "GpJEYT9.ngff/".into(),
                        href: "/dandisets/000002/draft/fRLy/GpJEYT9.ngff/".into()
                    },
                    metadata_link: Some(format!(
                        "{}/dandisets/000002/versions/draft/assets/869c1d3e-ab25-452c-bc69-605e094aa3a2/",
                        app.archive_url
                    )),
                    typekind: "Zarr asset".into(),
                    size: "11.53 MiB".into(),
                    created: "2020-11-10 19:51:46Z".into(),
                    modified: "2024-09-24 14:57:34Z".into(),
                },
                CollectionEntry {
                    name: Link {
                        text: "NYW8SD/".into(),
                        href: "/dandisets/000002/draft/fRLy/NYW8SD/".into()
                    },
                    metadata_link: None,
                    typekind: "Directory".into(),
                    size: "\u{2014}".into(),
                    created: "\u{2014}".into(),
                    modified: "\u{2014}".into(),
                },
                CollectionEntry {
                    name: Link {
                        text: "OWG.nwb".into(),
                        href: "https://api.dandiarchive.org/api/assets/e46ae092-a818-42ef-a7c1-12bca5b4ffdd/download/".into()
                    },
                    metadata_link: Some(format!(
                        "{}/dandisets/000002/versions/draft/assets/e46ae092-a818-42ef-a7c1-12bca5b4ffdd/",
                        app.archive_url
                    )),
                    typekind: "Blob asset".into(),
                    size: "46.65 KiB".into(),
                    created: "2022-04-16 12:10:08Z".into(),
                    modified: "2024-11-17 14:09:58Z".into(),
                },
                CollectionEntry {
                    name: Link {
                        text: "UP8CFrHpN/".into(),
                        href: "/dandisets/000002/draft/fRLy/UP8CFrHpN/".into()
                    },
                    metadata_link: None,
                    typekind: "Directory".into(),
                    size: "\u{2014}".into(),
                    created: "\u{2014}".into(),
                    modified: "\u{2014}".into(),
                },
                CollectionEntry {
                    name: Link {
                        text: "hH.nwb".into(),
                        href: "https://api.dandiarchive.org/api/assets/6243f6dd-4418-498d-afe3-589c6ac8778e/download/".into()
                    },
                    metadata_link: Some(format!(
                        "{}/dandisets/000002/versions/draft/assets/6243f6dd-4418-498d-afe3-589c6ac8778e/",
                        app.archive_url
                    )),
                    typekind: "Blob asset".into(),
                    size: "7.54 KiB".into(),
                    created: "2022-06-07 18:41:49Z".into(),
                    modified: "2024-10-24 07:13:43Z".into(),
                },
                CollectionEntry {
                    name: Link {
                        text: "yY9p4f.nwb".into(),
                        href: "https://api.dandiarchive.org/api/assets/d0e64671-2d87-4ecf-8287-709cdafbce70/download/".into()
                    },
                    metadata_link: Some(format!(
                        "{}/dandisets/000002/versions/draft/assets/d0e64671-2d87-4ecf-8287-709cdafbce70/",
                        app.archive_url
                    )),
                    typekind: "Blob asset".into(),
                    size: "21.09 KiB".into(),
                    created: "2021-02-14 11:32:18Z".into(),
                    modified: "2022-07-08 12:49:19Z".into(),
                },
                CollectionEntry {
                    name: Link {
                        text: "zBbN.nwb".into(),
                        href: "https://api.dandiarchive.org/api/assets/c28fd72b-944f-4e3d-865a-0f6eb38b7e17/download/".into()
                    },
                    metadata_link: Some(format!(
                        "{}/dandisets/000002/versions/draft/assets/c28fd72b-944f-4e3d-865a-0f6eb38b7e17/",
                        app.archive_url
                    )),
                    typekind: "Blob asset".into(),
                    size: "1.37 KiB".into(),
                    created: "2021-11-02 19:29:07Z".into(),
                    modified: "2024-12-12 09:12:44Z".into(),
                },
                CollectionEntry {
                    name: Link {
                        text: "zfa6zGT.zarr/".into(),
                        href: "/dandisets/000002/draft/fRLy/zfa6zGT.zarr/".into()
                    },
                    metadata_link: Some(format!(
                        "{}/dandisets/000002/versions/draft/assets/94e691a5-8031-4a73-b063-374bccee7154/",
                        app.archive_url
                    )),
                    typekind: "Zarr asset".into(),
                    size: "769.20 KiB".into(),
                    created: "2021-04-13 17:19:48Z".into(),
                    modified: "2024-07-08 23:18:03Z".into(),
                },
            ],
        }
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

#[tokio::test]
async fn propfind_dandiset_yaml() {
    let mut app = MockApp::new().await;
    for depth in ["0", "1"] {
        let resources = app
            .propfind("/dandisets/000001/draft/dandiset.yaml")
            .depth(depth)
            .send()
            .await
            .success()
            .into_resources();
        pretty_assertions::assert_eq!(
            resources,
            vec![Resource {
                href: "/dandisets/000001/draft/dandiset.yaml".into(),
                creation_date: Trinary::Void,
                display_name: Trinary::Set("dandiset.yaml".into()),
                content_length: Trinary::Set(410),
                content_type: Trinary::Set(YAML_CONTENT_TYPE.into()),
                last_modified: Trinary::Void,
                etag: Trinary::Void,
                language: Trinary::Void,
                is_collection: Some(false),
            }],
        );
    }
}

#[tokio::test]
async fn get_paginated_assets() {
    let mut app = MockApp::new().await;
    let page = app.get_collection_html("/dandisets/000003/draft/").await;
    pretty_assertions::assert_eq!(
        page.into_names(),
        vec![
            "../",
            "15j3l.nwb",
            "17Z.tsv",
            "1NjQC.nwb",
            "1eoEJ.nwb",
            "1fCq6.nwb",
            "2k.zarr/",
            "37GrgQ1.nwb",
            "4Wy1T8.zarr/",
            "4r05W.nwb",
            "4uR.nwb",
            "5Fc4.nwb",
            "5T3A.nwb",
            "5UZm.nwb",
            "6.ngff/",
            "6ZTXo.json",
            "7MDPQWiFr.nwb",
            "7kQh.nwb",
            "7t6W4.json",
            "8E2BCLV1.nwb",
            "8sBwoQQs7.nwb",
            "8sM.nwb",
            "9QtXsu0.nwb",
            "9c0OSZ.json",
            "9q63CS.nwb",
            "9qeEtGK3Q.json",
            "ADKiUC.nwb",
            "AXq5IXd.tsv",
            "BGklF.nwb",
            "BIS7eFZbbq1.nwb",
            "BKK.nwb",
            "C7ZG51.nwb",
            "CMqZ.nwb",
            "CSZ5s8.nwb",
            "Cmt.nwb",
            "Cn.nwb",
            "DmFPfNMF.nwb",
            "DpmL.nwb",
            "EN4tkG.nwb",
            "EbsJOJ.nwb",
            "FDCkjq.ngff/",
            "FwQp.nwb",
            "GsjxHru.nwb",
            "HxjO.nwb",
            "I5sI5g8uuI.tsv",
            "I69RZy.nwb",
            "I6a1OD6.nwb",
            "ICVSnu.nwb",
            "INOz0S.ngff/",
            "J8U1J.nwb",
            "JnwbKo.tsv",
            "LK.nwb",
            "LaI8.nwb",
            "MMWRtm.zarr/",
            "NX6Ee.nwb",
            "NZMqQ.nwb",
            "NqAzdR.json",
            "OEuTn.nwb",
            "Q.nwb",
            "QAsu2PQ.json",
            "RDGFMu.nwb",
            "RRH.nwb",
            "SDeG.json",
            "TGOl.nwb",
            "UZhMF.nwb",
            "VEU.nwb",
            "WN1zzt.nwb",
            "WnZJ.zarr/",
            "XhiuiyWH.nwb",
            "XmNaS.nwb",
            "Y.nwb",
            "Yv.nwb",
            "ZszxeN.nwb",
            "aDOGxj.tsv",
            "be5.nwb",
            "bkpe.nwb",
            "bw6B.nwb",
            "cCcOG.nwb",
            "cZyni.nwb",
            "dandiset.yaml",
            "djkK.nwb",
            "dudzw.nwb",
            "e7da.nwb",
            "eFLJW.nwb",
            "eVcVtX5.nwb",
            "f2uf.ngff/",
            "fElqTX.nwb",
            "fRBSP.zarr/",
            "g7lf3c0.nwb",
            "gqwwP.nwb",
            "gx8mRsV.tsv",
            "hwi8.zarr/",
            "jQ.nwb",
            "jQkb.nwb",
            "jby.json",
            "jt4Lg.nwb",
            "kCmm9z.zarr/",
            "kMHk5hZK.nwb",
            "kQa76T.nwb",
            "lG89e.nwb",
            "lg.zarr/",
            "lxey.json",
            "mQJQ.nwb",
            "mb9.zarr/",
            "mv3.nwb",
            "n9ngBF.nwb",
            "nD69k.ngff/",
            "nRG.nwb",
            "nz3hfBA.nwb",
            "pD7BaY.nwb",
            "pLFH.json",
            "q7UK9e.nwb",
            "qnqQx8.nwb",
            "qrarmCzj.nwb",
            "rWQvwKwv.nwb",
            "sEA7W.nwb",
            "sXCkWz.nwb",
            "se7zPH.nwb",
            "sju0Oc2.nwb",
            "t2ZsXo.json",
            "t4G8u.nwb",
            "tWL4.json",
            "uZ2v8IBn.nwb",
            "v2MbtC1L.json",
            "vWqtq1z.nwb",
            "vadY9.nwb",
            "x63UdC.tsv",
            "xAYoWn.nwb",
            "xv2ucf6.nwb",
            "y1kz9.nwb",
            "yEprIh.nwb",
            "yvCRuG6N.nwb",
        ]
    );
}

#[tokio::test]
async fn get_blob_asset() {
    let mut app = MockApp::new().await;
    let response = app
        .get("/dandisets/000001/draft/sub-RAT123/sub-RAT123.nwb")
        .await;
    assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
    assert_eq!(
        response
            .headers()
            .get(axum::http::header::LOCATION)
            .and_then(|v| v.to_str().ok()),
        Some("https://api.dandiarchive.org/api/assets/838bab7b-9ab4-4d66-97b3-898a367c9c7e/download/"),
    );
    assert!(response.headers().contains_key("DAV"));
    assert!(response.body().is_empty());
}

#[tokio::test]
async fn get_blob_asset_prefer_s3_redirects() {
    let mut app = MockApp::with_config(Config {
        prefer_s3_redirects: true,
        ..Config::default()
    })
    .await;
    let response = app
        .get("/dandisets/000001/draft/sub-RAT123/sub-RAT123.nwb")
        .await;
    assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
    assert_eq!(
        response
            .headers()
            .get(axum::http::header::LOCATION)
            .and_then(|v| v.to_str().ok()),
        Some("https://dandiarchive.s3.amazonaws.com/blobs/2db/af0/2dbaf0fd-5003-4a0a-b4c0-bc8cdbdb3826"),
    );
    assert!(response.headers().contains_key("DAV"));
    assert!(response.body().is_empty());
}

#[tokio::test]
async fn propfind_blob_asset() {
    let mut app = MockApp::new().await;
    for depth in ["0", "1"] {
        let resources = app
            .propfind("/dandisets/000001/draft/sub-RAT123/sub-RAT123.nwb")
            .depth(depth)
            .send()
            .await
            .success()
            .into_resources();
        pretty_assertions::assert_eq!(
            resources,
            vec![Resource {
                href: "/dandisets/000001/draft/sub-RAT123/sub-RAT123.nwb".into(),
                creation_date: Trinary::Set("2023-03-02T22:10:45.985334Z".into()),
                display_name: Trinary::Set("sub-RAT123.nwb".into()),
                content_length: Trinary::Set(18792),
                content_type: Trinary::Set("application/x-nwb".into()),
                last_modified: Trinary::Set("Thu, 02 Mar 2023 22:10:46 GMT".into()),
                etag: Trinary::Set("6ec084ca9d3be17ec194a8f700d65344-1".into()),
                language: Trinary::Void,
                is_collection: Some(false),
            }],
        );
    }
}

#[tokio::test]
async fn get_latest_version() {
    let mut app = MockApp::new().await;
    let page = app.get_collection_html("/dandisets/000001/latest/").await;
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
                    text: "latest".into(),
                    href: "/dandisets/000001/latest/".into()
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
                        text: "9coP.nwb".into(),
                        href: "https://api.dandiarchive.org/api/assets/af17d53e-1bbf-473b-9a3c-5ca32db1e90d/download/".into()
                    },
                    metadata_link: Some(format!(
                        "{}/dandisets/000001/versions/0.230629.1955/assets/af17d53e-1bbf-473b-9a3c-5ca32db1e90d/",
                        app.archive_url
                    )),
                    typekind: "Blob asset".into(),
                    size: "3.79 KiB".into(),
                    created: "2020-05-09 00:21:08Z".into(),
                    modified: "2023-01-29 18:58:27Z".into(),
                },
                CollectionEntry {
                    name: Link {
                        text: "OK.nwb".into(),
                        href: "https://api.dandiarchive.org/api/assets/86645ab4-782a-403e-9e1a-f65df91b70a9/download/".into()
                    },
                    metadata_link: Some(format!(
                        "{}/dandisets/000001/versions/0.230629.1955/assets/86645ab4-782a-403e-9e1a-f65df91b70a9/",
                        app.archive_url
                    )),
                    typekind: "Blob asset".into(),
                    size: "14.10 KiB".into(),
                    created: "2021-04-07 15:29:52Z".into(),
                    modified: "2024-12-29 20:05:23Z".into(),
                },
                CollectionEntry {
                    name: Link {
                        text: "PYQIm.tsv".into(),
                        href: "https://api.dandiarchive.org/api/assets/34523ca7-ff7c-42b3-a311-2f2d0ccc780f/download/".into()
                    },
                    metadata_link: Some(format!(
                        "{}/dandisets/000001/versions/0.230629.1955/assets/34523ca7-ff7c-42b3-a311-2f2d0ccc780f/",
                        app.archive_url
                    )),
                    typekind: "Blob asset".into(),
                    size: "6.89 KiB".into(),
                    created: "2020-08-17 10:19:11Z".into(),
                    modified: "2024-06-05 02:34:12Z".into(),
                },
                CollectionEntry {
                    name: Link {
                        text: "b.zarr/".into(),
                        href: "/dandisets/000001/latest/b.zarr/".into()
                    },
                    metadata_link: Some(format!(
                        "{}/dandisets/000001/versions/0.230629.1955/assets/9bb35766-4d95-48d7-88df-68a23cd43b74/",
                        app.archive_url
                    )),
                    typekind: "Zarr asset".into(),
                    size: "125.56 KiB".into(),
                    created: "2021-04-04 04:17:58Z".into(),
                    modified: "2024-07-07 10:10:04Z".into(),
                },
                CollectionEntry {
                    name: Link {
                        text: "dandiset.yaml".into(),
                        href: "/dandisets/000001/latest/dandiset.yaml".into(),
                    },
                    metadata_link: None,
                    typekind: "Version metadata".into(),
                    size: "429 B".into(),
                    created: "\u{2014}".into(),
                    modified: "\u{2014}".into(),
                },
                CollectionEntry {
                    name: Link {
                        text: "mv.nwb".into(),
                        href: "https://api.dandiarchive.org/api/assets/6d8e773d-fb9c-45e6-9a14-2c249399a901/download/".into()
                    },
                    metadata_link: Some(format!(
                        "{}/dandisets/000001/versions/0.230629.1955/assets/6d8e773d-fb9c-45e6-9a14-2c249399a901/",
                        app.archive_url
                    )),
                    typekind: "Blob asset".into(),
                    size: "14.61 KiB".into(),
                    created: "2022-11-14 02:46:29Z".into(),
                    modified: "2024-09-23 17:50:51Z".into(),
                },
                CollectionEntry {
                    name: Link {
                        text: "yCw7krL6rM.nwb".into(),
                        href: "https://api.dandiarchive.org/api/assets/864dffcb-61f7-4a1a-b26c-462739931efa/download/".into()
                    },
                    metadata_link: Some(format!(
                        "{}/dandisets/000001/versions/0.230629.1955/assets/864dffcb-61f7-4a1a-b26c-462739931efa/",
                        app.archive_url
                    )),
                    typekind: "Blob asset".into(),
                    size: "6.96 KiB".into(),
                    created: "2022-03-19 20:29:49Z".into(),
                    modified: "2024-10-16 15:26:16Z".into(),
                },
            ],
        }
    );
}

#[tokio::test]
async fn get_404() {
    let mut app = MockApp::new().await;
    let response = app.get("/dandisets/999999/").await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn propfind_404() {
    let mut app = MockApp::new().await;
    for depth in ["0", "1"] {
        app.propfind("/dandisets/999999/")
            .depth(depth)
            .send()
            .await
            .assert_status(StatusCode::NOT_FOUND);
    }
}

#[tokio::test]
async fn propfind_infinite_depth() {
    let mut app = MockApp::new().await;
    app.propfind("/")
        .depth("infinity")
        .send()
        .await
        .assert_status(StatusCode::FORBIDDEN)
        .assert_header(CONTENT_TYPE, DAV_XML_CONTENT_TYPE)
        .assert_body(indoc! {r#"
            <?xml version="1.0" encoding="utf-8"?>
            <error xmlns="DAV:">
                <propfind-finite-depth />
            </error>
            "#});
}

#[tokio::test]
async fn propfind_no_depth() {
    let mut app = MockApp::new().await;
    app.propfind("/")
        .no_depth()
        .send()
        .await
        .assert_status(StatusCode::FORBIDDEN)
        .assert_header(CONTENT_TYPE, DAV_XML_CONTENT_TYPE)
        .assert_body(indoc! {r#"
            <?xml version="1.0" encoding="utf-8"?>
            <error xmlns="DAV:">
                <propfind-finite-depth />
            </error>
            "#});
}

#[tokio::test]
async fn propfind_invalid_depth() {
    let mut app = MockApp::new().await;
    app.propfind("/")
        .depth("2")
        .send()
        .await
        .assert_status(StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn propfind_version_toplevel() {
    let mut app = MockApp::new().await;
    let resources = app
        .propfind("/dandisets/000001/releases/0.210512.1623/")
        .send()
        .await
        .success()
        .into_resources();
    pretty_assertions::assert_eq!(
        resources,
        vec![
            Resource {
                href: "/dandisets/000001/releases/0.210512.1623/".into(),
                creation_date: Trinary::Set("2021-05-12T16:23:14.388489Z".into()),
                display_name: Trinary::Set("0.210512.1623".into()),
                content_length: Trinary::Set(42489179,),
                content_type: Trinary::Void,
                last_modified: Trinary::Set("Wed, 12 May 2021 16:23:19 GMT".into()),
                etag: Trinary::Void,
                language: Trinary::Void,
                is_collection: Some(true),
            },
            Resource {
                href: "/dandisets/000001/releases/0.210512.1623/participants.tsv".into(),
                creation_date: Trinary::Set("2022-08-26T03:21:32.305654Z".into()),
                display_name: Trinary::Set("participants.tsv".into()),
                content_length: Trinary::Set(5968),
                content_type: Trinary::Set("text/tab-separated-values".into()),
                last_modified: Trinary::Set("Fri, 04 Oct 2024 05:53:14 GMT".into()),
                etag: Trinary::Set("d80b74152eed942fca5845273a4f1256-1".into()),
                language: Trinary::Void,
                is_collection: Some(false),
            },
            Resource {
                href: "/dandisets/000001/releases/0.210512.1623/sub-RAT123/".into(),
                creation_date: Trinary::Void,
                display_name: Trinary::Set("sub-RAT123".into()),
                content_length: Trinary::Void,
                content_type: Trinary::Void,
                last_modified: Trinary::Void,
                etag: Trinary::Void,
                language: Trinary::Void,
                is_collection: Some(true),
            },
            Resource {
                href: "/dandisets/000001/releases/0.210512.1623/dandiset.yaml".into(),
                creation_date: Trinary::Void,
                display_name: Trinary::Set("dandiset.yaml".into()),
                content_length: Trinary::Set(429),
                content_type: Trinary::Set(YAML_CONTENT_TYPE.into()),
                last_modified: Trinary::Void,
                etag: Trinary::Void,
                language: Trinary::Void,
                is_collection: Some(false),
            },
        ]
    );
}

#[tokio::test]
async fn propfind_asset_folder() {
    let mut app = MockApp::new().await;
    let resources = app
        .propfind("/dandisets/000001/releases/0.210512.1623/sub-RAT123/")
        .send()
        .await
        .success()
        .into_resources();
    pretty_assertions::assert_eq!(
        resources,
        vec![
            Resource {
                href: "/dandisets/000001/releases/0.210512.1623/sub-RAT123/".into(),
                creation_date: Trinary::Void,
                display_name: Trinary::Set("sub-RAT123".into()),
                content_length: Trinary::Void,
                content_type: Trinary::Void,
                last_modified: Trinary::Void,
                etag: Trinary::Void,
                language: Trinary::Void,
                is_collection: Some(true),
            },
            Resource {
                href: "/dandisets/000001/releases/0.210512.1623/sub-RAT123/sub-RAT123.nwb".into(),
                creation_date: Trinary::Set("2023-03-02T22:10:45.985334Z".into()),
                display_name: Trinary::Set("sub-RAT123.nwb".into()),
                content_length: Trinary::Set(18792),
                content_type: Trinary::Set("application/x-nwb".into()),
                last_modified: Trinary::Set("Thu, 02 Mar 2023 22:10:46 GMT".into()),
                etag: Trinary::Set("6ec084ca9d3be17ec194a8f700d65344-1".into()),
                language: Trinary::Void,
                is_collection: Some(false),
            },
            Resource {
                href: "/dandisets/000001/releases/0.210512.1623/sub-RAT123/sub-RAT456.zarr/".into(),
                creation_date: Trinary::Set("2022-12-03T20:19:13.983328Z".into()),
                display_name: Trinary::Set("sub-RAT456.zarr".into()),
                content_length: Trinary::Set(42464419),
                content_type: Trinary::Void,
                last_modified: Trinary::Set("Tue, 03 Dec 2024 10:09:28 GMT".into()),
                etag: Trinary::Void,
                language: Trinary::Void,
                is_collection: Some(true),
            },
        ]
    );
}
