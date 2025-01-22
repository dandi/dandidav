#![cfg(test)]
use super::*;
use axum::body::Bytes;
use http_body_util::BodyExt; // for `collect`
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
