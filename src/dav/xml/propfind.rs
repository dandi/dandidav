use axum::{
    async_trait,
    body::Body,
    extract::{FromRequest, Request},
    http::{response::Response, StatusCode},
    response::IntoResponse,
};

use super::multistatus::{DavResponse, PropStat};
use super::{PropValue, Property, Tag};
use crate::dav::types::HasProperties;
use bytes::{Buf, Bytes};
use std::collections::BTreeMap;
use std::fmt;
use thiserror::Error;
use xml::reader::{Error as XmlError, ParserConfig2, XmlEvent};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::dav) enum PropFind {
    AllProp { include: Vec<Property> },
    Prop(Vec<Property>),
    PropName,
}

impl PropFind {
    pub(in crate::dav) fn from_xml(blob: Bytes) -> Result<PropFind, FromXmlError> {
        let reader = ParserConfig2::new()
            .ignore_invalid_encoding_declarations(false)
            .allow_multiple_root_elements(false)
            .trim_whitespace(true)
            .create_reader(blob.reader());
        let mut parser = PropFindParser::new();
        for event in reader {
            use XmlEvent::*;
            match event? {
                StartElement { name, .. } => parser.start_tag(Tag {
                    name: name.local_name,
                    namespace: name.namespace,
                })?,
                EndElement { .. } => parser.end_tag()?,
                StartDocument { .. } | EndDocument | Comment(..) | Whitespace(..) => (),
                ProcessingInstruction { .. } | CData(..) | Characters(..) => {
                    return Err(FromXmlError::UnexpectedContent)
                }
            }
        }
        parser.finish().map_err(Into::into)
    }

    pub(in crate::dav) fn find<P: HasProperties>(&self, res: &P) -> DavResponse {
        let mut found = BTreeMap::new();
        let mut missing = BTreeMap::new();
        match self {
            PropFind::AllProp { include } => {
                for prop in Property::iter_standard() {
                    if let Some(value) = res.property(&prop) {
                        found.insert(prop, value);
                    }
                }
                for prop in include {
                    if let Some(value) = res.property(prop) {
                        found.insert(prop.clone(), value);
                    } else {
                        missing.insert(prop.clone(), PropValue::Empty);
                    }
                }
            }
            PropFind::Prop(props) => {
                for prop in props {
                    if let Some(value) = res.property(prop) {
                        found.insert(prop.clone(), value);
                    } else {
                        missing.insert(prop.clone(), PropValue::Empty);
                    }
                }
            }
            PropFind::PropName => {
                for prop in Property::iter_standard() {
                    found.insert(prop, PropValue::Empty);
                }
            }
        }
        let mut propstat = Vec::with_capacity(2);
        if !found.is_empty() || missing.is_empty() {
            propstat.push(PropStat {
                prop: found,
                status: "HTTP/1.1 200 OK".into(),
            });
        }
        if !missing.is_empty() {
            propstat.push(PropStat {
                prop: missing,
                status: "HTTP/1.1 404 NOT FOUND".into(),
            });
        }
        DavResponse {
            href: res.href(),
            propstat,
            // TODO: Should `location` be set to redirect URLs?
            location: None,
        }
    }
}

#[async_trait]
impl<S: Send + Sync> FromRequest<S> for PropFind
where
    Bytes: FromRequest<S>,
{
    type Rejection = Response<Body>;

    async fn from_request(req: Request<Body>, state: &S) -> Result<Self, Self::Rejection> {
        let blob = Bytes::from_request(req, state)
            .await
            .map_err(IntoResponse::into_response)?;
        // TODO: Accept all-whitespace bodies
        if blob.is_empty() {
            Ok(PropFind::default())
        } else {
            match PropFind::from_xml(blob) {
                Ok(pf) => Ok(pf),
                Err(_) => Err((StatusCode::BAD_REQUEST, "Invalid request body\n").into_response()),
            }
        }
    }
}

impl Default for PropFind {
    fn default() -> PropFind {
        PropFind::AllProp {
            include: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PropFindParser {
    state: State,
    tag_stack: Vec<PropFindTag>,
}

impl PropFindParser {
    fn new() -> Self {
        PropFindParser {
            state: State::default(),
            tag_stack: vec![PropFindTag::Root],
        }
    }

    fn start_tag(&mut self, tag: Tag) -> Result<(), PropFindError> {
        let current = self.tag_stack.last().expect("tag stack should be nonempty");
        let tagdisp = tag.to_string();
        if let Some(pt) = current.accept(tag, &mut self.state) {
            self.tag_stack.push(pt);
            Ok(())
        } else {
            Err(PropFindError::UnexpectedTag {
                container: current.to_string(),
                tag: tagdisp,
            })
        }
    }

    fn end_tag(&mut self) -> Result<(), PropFindError> {
        let Some(current) = self.tag_stack.pop() else {
            return Err(PropFindError::TooManyEnds);
        };
        let tagdisp = current.to_string();
        if current.end(&mut self.state) {
            Ok(())
        } else {
            Err(PropFindError::PrematureEnd(tagdisp))
        }
    }

    fn finish(self) -> Result<PropFind, PropFindError> {
        if self.tag_stack != [PropFindTag::Root] {
            return Err(PropFindError::FinishedInMiddle);
        }
        match self.state.mode {
            Some(Mode::PropName) => Ok(PropFind::PropName),
            Some(Mode::AllProp) => Ok(PropFind::AllProp {
                include: self.state.properties,
            }),
            Some(Mode::Prop) => Ok(PropFind::Prop(self.state.properties)),
            None => Err(PropFindError::EmptyPropFind),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct State {
    mode: Option<Mode>,
    properties: Vec<Property>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum Mode {
    PropName,
    AllProp,
    Prop,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum PropFindTag {
    Root,
    PropFind,
    PropName,
    AllProp,
    Include,
    Prop,
    Property(Tag),
}

impl PropFindTag {
    fn accept(&self, tag: Tag, state: &mut State) -> Option<PropFindTag> {
        match (self, &*tag.name, tag.is_dav()) {
            (PropFindTag::Root, "propfind", true) => Some(PropFindTag::PropFind),
            (PropFindTag::Root, _, _) => None,
            (PropFindTag::PropFind, "propname", true) => {
                state.mode = Some(Mode::PropName);
                Some(PropFindTag::PropName)
            }
            (PropFindTag::PropFind, "allprop", true) => {
                state.mode = Some(Mode::AllProp);
                Some(PropFindTag::AllProp)
            }
            (PropFindTag::PropFind, "prop", true) => {
                state.mode = Some(Mode::Prop);
                Some(PropFindTag::Prop)
            }
            (PropFindTag::PropFind, "include", true) if state.mode.is_some() => {
                Some(PropFindTag::Include)
            }
            (PropFindTag::PropFind, _, _) => None,
            (PropFindTag::PropName, _, _) => None,
            (PropFindTag::AllProp, _, _) => None,
            (PropFindTag::Include, _, _) => Some(PropFindTag::Property(tag)),
            (PropFindTag::Prop, _, _) => Some(PropFindTag::Property(tag)),
            (PropFindTag::Property(_), _, _) => None,
        }
    }

    fn end(self, state: &mut State) -> bool {
        match self {
            PropFindTag::Root => false,
            PropFindTag::PropFind => state.mode.is_some(),
            PropFindTag::PropName => true,
            PropFindTag::AllProp => true,
            PropFindTag::Include => true,
            PropFindTag::Prop => true,
            PropFindTag::Property(tag) => {
                state.properties.push(Property::from(tag));
                true
            }
        }
    }
}

impl fmt::Display for PropFindTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PropFindTag::Root => write!(f, "[root]"),
            PropFindTag::PropFind => write!(f, "{{DAV:}}propfind"),
            PropFindTag::PropName => write!(f, "{{DAV:}}propname"),
            PropFindTag::AllProp => write!(f, "{{DAV:}}allprop"),
            PropFindTag::Include => write!(f, "{{DAV:}}include"),
            PropFindTag::Prop => write!(f, "{{DAV:}}prop"),
            PropFindTag::Property(tag) => write!(f, "{tag}"),
        }
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum FromXmlError {
    #[error("failed to parse XML")]
    Xml(#[from] XmlError),
    #[error("XML contained unexpected non-tag content")]
    UnexpectedContent,
    #[error("XML is not valid <propfind> document")]
    Schema(#[from] PropFindError),
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum PropFindError {
    #[error("unexpected tag {tag:?} encountered in {container:?} tag")]
    UnexpectedTag { container: String, tag: String },
    #[error("tag {0:?} ended without encountering expected contents")]
    PrematureEnd(String),
    #[error("finish() called before end of document reached")]
    FinishedInMiddle,
    #[error("<propfind> is empty")]
    EmptyPropFind,
    #[error("too many end tags")]
    TooManyEnds,
}
