#![cfg(test)]
use super::*;
use axum::body::Bytes;
use http_body_util::BodyExt; // for `collect`
use testutils::{CollectionEntry, CollectionPage, Link};
use tower::ServiceExt; // for `oneshot`

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
}

impl MockApp {
    async fn new() -> MockApp {
        let mock_archive = testutils::make_mock_archive(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/testdata/stubs"
        ))
        .await;
        let cfg = Config {
            api_url: format!("{}/api", mock_archive.uri())
                .parse::<HttpUrl>()
                .unwrap(),
            ..Config::default()
        };
        let app = get_app(cfg).unwrap();
        MockApp { app, mock_archive }
    }

    async fn get(self, path: &str) -> Response<Bytes> {
        let response = self
            .app
            .oneshot(
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

    async fn head(self, path: &str) -> Response<Bytes> {
        let response = self
            .app
            .oneshot(
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

    async fn get_collection_html(self, path: &str) -> CollectionPage {
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
    let app = MockApp::new().await;
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
    let app = MockApp::new().await;
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
    let app = MockApp::new().await;
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
    let app = MockApp::new().await;
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
    let app = MockApp::new().await;
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
