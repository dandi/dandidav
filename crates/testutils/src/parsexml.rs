use serde::Deserialize;
use thiserror::Error;

// Only honors tags defined in RFC 4918
pub fn parse_propfind_response(xml: &str) -> Result<Vec<Resource>, PropfindError> {
    quick_xml::de::from_str::<Multistatus>(xml)?
        .response
        .into_iter()
        .map(Resource::try_from)
        .collect::<Result<Vec<_>, _>>()
        .map_err(Into::into)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Resource {
    pub href: String,
    pub creation_date: Trinary<String>,
    pub display_name: Trinary<String>,
    pub content_length: Trinary<u64>,
    pub content_type: Trinary<String>,
    pub last_modified: Trinary<String>,
    pub etag: Trinary<String>,
    pub language: Trinary<String>,
    pub is_collection: Option<bool>,
}

impl TryFrom<Response> for Resource {
    type Error = ResourceError;

    fn try_from(value: Response) -> Result<Resource, ResourceError> {
        let mut r = Resource {
            href: value.href,
            creation_date: Trinary::Void,
            display_name: Trinary::Void,
            content_length: Trinary::Void,
            content_type: Trinary::Void,
            last_modified: Trinary::Void,
            etag: Trinary::Void,
            language: Trinary::Void,
            is_collection: None,
        };
        let mut seen_200 = false;
        let mut seen_404 = false;
        for ps in value.propstat {
            if ps.status == "HTTP/1.1 200 OK" {
                if std::mem::replace(&mut seen_200, true) {
                    return Err(ResourceError::Multiple200(r.href));
                }
                if let Some(s) = ps.prop.creationdate {
                    r.creation_date = Trinary::Set(s);
                }
                if let Some(s) = ps.prop.displayname {
                    r.display_name = Trinary::Set(s);
                }
                if let Some(text) = ps.prop.getcontentlength {
                    match text.parse::<u64>() {
                        Ok(i) => r.content_length = Trinary::Set(i),
                        Err(_) => return Err(ResourceError::BadLength { href: r.href, text }),
                    }
                }
                if let Some(s) = ps.prop.getcontenttype {
                    r.content_type = Trinary::Set(s);
                }
                if let Some(s) = ps.prop.getlastmodified {
                    r.last_modified = Trinary::Set(s);
                }
                if let Some(s) = ps.prop.getetag {
                    r.etag = Trinary::Set(s);
                }
                if let Some(s) = ps.prop.getcontentlanguage {
                    r.language = Trinary::Set(s);
                }
                if let Some(rt) = ps.prop.resourcetype {
                    r.is_collection = Some(rt.collection.is_some());
                }
            } else if ps.status == "HTTP/1.1 404 NOT FOUND" {
                if std::mem::replace(&mut seen_404, true) {
                    return Err(ResourceError::Multiple404(r.href));
                }
                if ps.prop.creationdate.is_some() {
                    r.creation_date = Trinary::NotFound;
                }
                if ps.prop.displayname.is_some() {
                    r.display_name = Trinary::NotFound;
                }
                if ps.prop.getcontentlength.is_some() {
                    r.content_length = Trinary::NotFound;
                }
                if ps.prop.getcontenttype.is_some() {
                    r.content_type = Trinary::NotFound;
                }
                if ps.prop.getlastmodified.is_some() {
                    r.last_modified = Trinary::NotFound;
                }
                if ps.prop.getetag.is_some() {
                    r.etag = Trinary::NotFound;
                }
                if ps.prop.getcontentlanguage.is_some() {
                    r.language = Trinary::NotFound;
                }
                if ps.prop.resourcetype.is_some() {
                    return Err(ResourceError::ResourceType404(r.href));
                }
            } else {
                return Err(ResourceError::BadStatus {
                    href: r.href,
                    status: ps.status,
                });
            }
        }
        Ok(r)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Trinary<T> {
    Set(T),
    NotFound,
    Void,
}

#[derive(Clone, Debug, Error)]
pub enum PropfindError {
    #[error("failed to deserialize XML")]
    Deserialize(#[from] quick_xml::errors::serialize::DeError),
    #[error(transparent)]
    Resource(#[from] ResourceError),
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ResourceError {
    #[error("response for {0:?} contains multiple propstats with status 200")]
    Multiple200(String),
    #[error("response for {0:?} contains multiple propstats with status 404")]
    Multiple404(String),
    #[error("response for {href:?} contains propstat with unrecognized status {status:?}")]
    BadStatus { href: String, status: String },
    #[error("response for {0:?} lists resourcetype as undefined property")]
    ResourceType404(String),
    #[error("response for {href:?} contains unparseable getcontentlength: {text:?}")]
    BadLength { href: String, text: String },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
struct Multistatus {
    response: Vec<Response>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
struct Response {
    href: String,
    propstat: Vec<Propstat>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
struct Propstat {
    prop: Prop,
    status: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
struct Prop {
    creationdate: Option<String>,
    displayname: Option<String>,
    getcontentlanguage: Option<String>,
    // We can't use Option<u64> here, as that won't work with empty
    // <getcontentlength/> tags
    getcontentlength: Option<String>,
    getcontenttype: Option<String>,
    getlastmodified: Option<String>,
    getetag: Option<String>,
    resourcetype: Option<ResourceType>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
struct ResourceType {
    collection: Option<()>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_propfind_response() {
        let xml = include_str!("testdata/propfind.xml");
        let resources = parse_propfind_response(xml).unwrap();
        assert_eq!(
            resources,
            vec![
                Resource {
                    href: "/dandisets/000108/draft/".into(),
                    creation_date: Trinary::Set("2021-06-01T14:35:34.214567Z".into()),
                    display_name: Trinary::Set("draft".into()),
                    content_length: Trinary::Set(374730983049354),
                    last_modified: Trinary::Set("Fri, 03 Nov 2023 11:01:20 GMT".into()),
                    is_collection: Some(true),
                    content_type: Trinary::NotFound,
                    etag: Trinary::NotFound,
                    language: Trinary::Void,
                },
                Resource {
                    href: "/dandisets/000108/draft/dataset_description.json".into(),
                    creation_date: Trinary::Set("2021-07-03T04:23:52.38146Z".into()),
                    display_name: Trinary::Set("dataset_description.json".into()),
                    content_length: Trinary::Set(71),
                    content_type: Trinary::Set("application/json".into()),
                    etag: Trinary::Set("f4a034fbf965f76828fa027c29860bc0-1".into()),
                    last_modified: Trinary::Set("Wed, 13 Jul 2022 21:40:28 GMT".into()),
                    is_collection: Some(false),
                    language: Trinary::Void,
                },
                Resource {
                    href: "/dandisets/000108/draft/samples.tsv".into(),
                    creation_date: Trinary::Set("2021-07-21T23:39:29.733695Z".into()),
                    display_name: Trinary::Set("samples.tsv".into()),
                    content_length: Trinary::Set(572),
                    content_type: Trinary::Set("text/tab-separated-values".into()),
                    etag: Trinary::Set("a6ac1fb127e17b2e3360c64154f69a57-1".into()),
                    last_modified: Trinary::Set("Wed, 13 Jul 2022 21:41:07 GMT".into()),
                    is_collection: Some(false),
                    language: Trinary::Void,
                },
                Resource {
                    href: "/dandisets/000108/draft/sub-mEhm/".into(),
                    display_name: Trinary::Set("sub-mEhm".into()),
                    is_collection: Some(true),
                    creation_date: Trinary::NotFound,
                    content_length: Trinary::NotFound,
                    content_type: Trinary::NotFound,
                    etag: Trinary::NotFound,
                    last_modified: Trinary::NotFound,
                    language: Trinary::Void,
                },
                Resource {
                    href: "/dandisets/000108/draft/sub-MITU01/".into(),
                    display_name: Trinary::Set("sub-MITU01".into()),
                    is_collection: Some(true),
                    creation_date: Trinary::NotFound,
                    content_length: Trinary::NotFound,
                    content_type: Trinary::NotFound,
                    etag: Trinary::NotFound,
                    last_modified: Trinary::NotFound,
                    language: Trinary::Void,
                },
                Resource {
                    href: "/dandisets/000108/draft/sub-MITU01h3/".into(),
                    display_name: Trinary::Set("sub-MITU01h3".into()),
                    is_collection: Some(true),
                    creation_date: Trinary::NotFound,
                    content_length: Trinary::NotFound,
                    content_type: Trinary::NotFound,
                    etag: Trinary::NotFound,
                    last_modified: Trinary::NotFound,
                    language: Trinary::Void,
                },
                Resource {
                    href: "/dandisets/000108/draft/sub-SChmi53/".into(),
                    display_name: Trinary::Set("sub-SChmi53".into()),
                    is_collection: Some(true),
                    creation_date: Trinary::NotFound,
                    content_length: Trinary::NotFound,
                    content_type: Trinary::NotFound,
                    etag: Trinary::NotFound,
                    last_modified: Trinary::NotFound,
                    language: Trinary::Void,
                },
                Resource {
                    href: "/dandisets/000108/draft/sub-U01hm15x/".into(),
                    display_name: Trinary::Set("sub-U01hm15x".into()),
                    is_collection: Some(true),
                    creation_date: Trinary::NotFound,
                    content_length: Trinary::NotFound,
                    content_type: Trinary::NotFound,
                    etag: Trinary::NotFound,
                    last_modified: Trinary::NotFound,
                    language: Trinary::Void,
                },
                Resource {
                    href: "/dandisets/000108/draft/dandiset.yaml".into(),
                    creation_date: Trinary::NotFound,
                    display_name: Trinary::Set("dandiset.yaml".into()),
                    content_length: Trinary::Set(4543),
                    content_type: Trinary::Set("text/yaml; charset=utf-8".into()),
                    etag: Trinary::NotFound,
                    last_modified: Trinary::NotFound,
                    language: Trinary::Void,
                    is_collection: Some(false),
                },
            ]
        );
    }
}
