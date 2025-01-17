use itertools::{Itertools, Position};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use wiremock::{matchers::method, Mock, MockServer, Request, Respond, ResponseTemplate};

pub async fn make_mock_archive(stubdir: &Path) -> MockServer {
    let server = MockServer::start().await;
    let mock = Mock::given(method("GET")).respond_with(StubResponder {
        stubdir: stubdir.into(),
        base_url: server.uri(),
    });
    server.register(mock).await;
    server
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StubResponder {
    stubdir: PathBuf,
    base_url: String,
}

impl StubResponder {
    fn template_next(&self, response: &mut serde_json::Value) {
        let Some(serde_json::Value::String(ref mut s)) =
            response.as_object_mut().and_then(|m| m.get_mut("next"))
        else {
            return;
        };
        let newstr = s.replace("{base_url}", &self.base_url);
        s.replace_range(.., &newstr);
    }
}

impl Respond for StubResponder {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let Some(parts) = request.url.path_segments() else {
            return ResponseTemplate::new(404);
        };
        let mut stubfile = self.stubdir.clone();
        let mut nonempty = false;
        for (pos, p) in parts.with_position() {
            if matches!(pos, Position::Last | Position::Only) {
                stubfile.push(format!("{p}.json"));
                nonempty = true;
            } else {
                stubfile.push(p);
            }
        }
        if !nonempty {
            return ResponseTemplate::new(404);
        };
        let stubtext = match std::fs::read_to_string(&stubfile) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return ResponseTemplate::new(404)
            }
            Err(_) => return ResponseTemplate::new(500),
        };
        let Ok(stubs) = serde_json::from_str::<Vec<Stub>>(&stubtext) else {
            return ResponseTemplate::new(500);
        };
        let params = HashMap::from_iter(
            request
                .url
                .query_pairs()
                .map(|(k, v)| (k.into_owned(), v.into_owned())),
        );
        for st in stubs {
            if st.params == params {
                let mut response = st.response;
                self.template_next(&mut response);
                return ResponseTemplate::new(200).set_body_json(response);
            }
        }
        ResponseTemplate::new(404)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
struct Stub {
    params: HashMap<String, String>,
    response: serde_json::Value,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_template_next() {
        let stubby = StubResponder {
            stubdir: PathBuf::new(),
            base_url: "http://localhost:8080".into(),
        };
        let mut response = serde_json::json!({
            "next": "{base_url}/api/dandisets?page=2",
            "results": [
                {"identifier": "000001"},
                {"identifier": "000002"},
                {"identifier": "000003"},
                {"identifier": "000004"},
                {"identifier": "000005"},
            ],
        });
        stubby.template_next(&mut response);
        assert_eq!(
            response,
            serde_json::json!({
                "next": "http://localhost:8080/api/dandisets?page=2",
                "results": [
                    {"identifier": "000001"},
                    {"identifier": "000002"},
                    {"identifier": "000003"},
                    {"identifier": "000004"},
                    {"identifier": "000005"},
                ],
            })
        );
    }

    #[test]
    fn test_template_null_next() {
        let stubby = StubResponder {
            stubdir: PathBuf::new(),
            base_url: "http://localhost:8080".into(),
        };
        let mut response = serde_json::json!({
            "next": null,
            "results": [
                {"identifier": "000001"},
                {"identifier": "000002"},
                {"identifier": "000003"},
                {"identifier": "000004"},
                {"identifier": "000005"},
            ],
        });
        stubby.template_next(&mut response);
        assert_eq!(
            response,
            serde_json::json!({
                "next": null,
                "results": [
                    {"identifier": "000001"},
                    {"identifier": "000002"},
                    {"identifier": "000003"},
                    {"identifier": "000004"},
                    {"identifier": "000005"},
                ],
            })
        );
    }

    #[test]
    fn test_template_no_next() {
        let stubby = StubResponder {
            stubdir: PathBuf::new(),
            base_url: "http://localhost:8080".into(),
        };
        let mut response = serde_json::json!({"identifier": "000001"});
        stubby.template_next(&mut response);
        assert_eq!(response, serde_json::json!({"identifier": "000001"}));
    }
}
