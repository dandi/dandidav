use std::collections::BTreeMap;
use std::io::{Cursor, Write};
use thiserror::Error;
use xml::writer::{events::XmlEvent, EmitterConfig, Error as WriteError, EventWriter};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Multistatus {
    response: Vec<Response>,
    //responsedescription
}

impl Multistatus {
    pub(super) fn to_xml(&self) -> Result<String, ToXmlError> {
        let mut writer = XmlWriter::new();
        writer.tag_xmlns("multistatus", "DAV:", |writer| {
            for r in &self.response {
                r.write_xml(writer)?;
            }
            Ok(())
        })?;
        let mut s = writer.into_string()?;
        s.push('\n');
        Ok(s)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Response {
    href: String,
    // TODO: RFC 4918 says <response> can contain (href*, status) as an
    // alternative to propstat.  When does that apply?
    // TODO: RFC 4918 says a <response> can contain multiple <propstat>s.  When
    // does that apply?
    propstat: PropStat,
    //error
    //responsedescription
    location: Option<String>,
}

impl Response {
    fn write_xml(&self, writer: &mut XmlWriter) -> Result<(), WriteError> {
        writer.tag("response", |writer| {
            writer.text_tag("href", &self.href)?;
            self.propstat.write_xml(writer)?;
            if let Some(ref loc) = self.location {
                writer.text_tag("location", loc)?;
            }
            Ok(())
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PropStat {
    prop: BTreeMap<String, PropValue>,
    status: String,
    //error
    //responsedescription
}

impl PropStat {
    fn write_xml(&self, writer: &mut XmlWriter) -> Result<(), WriteError> {
        writer.tag("propstat", |writer| {
            writer.tag("prop", |writer| {
                for (k, v) in &self.prop {
                    writer.tag(k, |writer| v.write_xml(writer))?;
                }
                Ok(())
            })?;
            writer.text_tag("status", &self.status)?;
            Ok(())
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum PropValue {
    // Used for <resourcetype> of non-collections and for <propname> responses
    Empty,
    Collection,
    String(String),
    Int(i64),
}

impl PropValue {
    fn write_xml(&self, writer: &mut XmlWriter) -> Result<(), WriteError> {
        match self {
            PropValue::Empty => Ok(()),
            PropValue::Collection => writer.empty_tag("collection"),
            PropValue::String(s) => writer.text(s),
            PropValue::Int(i) => writer.text(&format!("{i}")),
        }
    }
}

struct XmlWriter(EventWriter<Vec<u8>>);

impl XmlWriter {
    fn new() -> Self {
        XmlWriter(
            EmitterConfig::new()
                .indent_string("    ")
                .perform_indent(true)
                .write_document_declaration(true)
                .create_writer(Vec::new()),
        )
    }

    fn into_string(self) -> Result<String, std::str::Utf8Error> {
        let buf = self.0.into_inner();
        String::from_utf8(buf).map_err(|e| e.utf8_error())
    }

    fn tag_xmlns<F>(&mut self, name: &str, ns: &str, func: F) -> Result<(), WriteError>
    where
        F: FnOnce(&mut Self) -> Result<(), WriteError>,
    {
        self.0.write(XmlEvent::start_element(name).default_ns(ns))?;
        func(self)?;
        self.end_tag()?;
        Ok(())
    }

    fn tag<F>(&mut self, name: &str, func: F) -> Result<(), WriteError>
    where
        F: FnOnce(&mut Self) -> Result<(), WriteError>,
    {
        self.start_tag(name)?;
        func(self)?;
        self.end_tag()?;
        Ok(())
    }

    fn start_tag(&mut self, name: &str) -> Result<(), WriteError> {
        self.0.write(XmlEvent::start_element(name))
    }

    fn end_tag(&mut self) -> Result<(), WriteError> {
        self.0.write(XmlEvent::end_element())
    }

    fn empty_tag(&mut self, name: &str) -> Result<(), WriteError> {
        self.start_tag(name)?;
        self.end_tag()?;
        Ok(())
    }

    fn text(&mut self, text: &str) -> Result<(), WriteError> {
        self.0.write(XmlEvent::characters(text))
    }

    fn text_tag(&mut self, name: &str, text: &str) -> Result<(), WriteError> {
        self.start_tag(name)?;
        self.text(text)?;
        self.end_tag()?;
        Ok(())
    }
}

#[derive(Debug, Error)]
pub(super) enum ToXmlError {
    #[error("failed to generate XML")]
    Xml(#[from] WriteError),
    #[error("generated XML was not valid UTF-8")]
    Decode(#[from] std::str::Utf8Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use indoc::indoc;
    use pretty_assertions::assert_eq;

    #[test]
    fn multistatus_to_xml() {
        let value = Multistatus {
            response: vec![
                Response {
                    href: "/foo/".into(),
                    propstat: PropStat {
                        prop: BTreeMap::from([
                            ("resourcetype".into(), PropValue::Collection),
                            ("displayname".into(), PropValue::String("foo".into())),
                        ]),
                        status: "HTTP/1.1 200 OK".into(),
                    },
                    location: None,
                },
                Response {
                    href: "/foo/bar.txt".into(),
                    propstat: PropStat {
                        prop: BTreeMap::from([
                            (
                                "creationdate".into(),
                                PropValue::String("2024-01-28T13:36:54+05:00".into()),
                            ),
                            ("displayname".into(), PropValue::String("bar.txt".into())),
                            ("getcontentlength".into(), PropValue::Int(42)),
                            (
                                "getcontenttype".into(),
                                PropValue::String("text/plain; charset=us-ascii".into()),
                            ),
                            (
                                "getetag".into(),
                                PropValue::String(r#""0123456789abcdef""#.into()),
                            ),
                            (
                                "getlastmodified".into(),
                                PropValue::String("2024-01-28T13:38:10+05:00".into()),
                            ),
                            ("resourcetype".into(), PropValue::Empty),
                        ]),
                        status: "HTTP/1.1 200 OK".into(),
                    },
                    location: None,
                },
                Response {
                    href: "/foo/quux.dat".into(),
                    propstat: PropStat {
                        prop: BTreeMap::from([
                            ("displayname".into(), PropValue::String("quux.dat".into())),
                            ("getcontentlength".into(), PropValue::Int(65535)),
                            (
                                "getcontenttype".into(),
                                PropValue::String("application/octet-stream".into()),
                            ),
                            (
                                "getetag".into(),
                                PropValue::String(r#""ABCDEFGHIJKLMNOPQRSTUVWXYZ""#.into()),
                            ),
                            (
                                "getlastmodified".into(),
                                PropValue::String("2024-01-28T13:39:25+05:00".into()),
                            ),
                            ("resourcetype".into(), PropValue::Empty),
                        ]),
                        status: "HTTP/1.1 307 TEMPORARY REDIRECT".into(),
                    },
                    location: Some("https://www.example.com/data/quux.dat".into()),
                },
            ],
        };

        assert_eq!(
            value.to_xml().unwrap(),
            indoc! {r#"
            <?xml version="1.0" encoding="utf-8"?>
            <multistatus xmlns="DAV:">
                <response>
                    <href>/foo/</href>
                    <propstat>
                        <prop>
                            <displayname>foo</displayname>
                            <resourcetype>
                                <collection />
                            </resourcetype>
                        </prop>
                        <status>HTTP/1.1 200 OK</status>
                    </propstat>
                </response>
                <response>
                    <href>/foo/bar.txt</href>
                    <propstat>
                        <prop>
                            <creationdate>2024-01-28T13:36:54+05:00</creationdate>
                            <displayname>bar.txt</displayname>
                            <getcontentlength>42</getcontentlength>
                            <getcontenttype>text/plain; charset=us-ascii</getcontenttype>
                            <getetag>"0123456789abcdef"</getetag>
                            <getlastmodified>2024-01-28T13:38:10+05:00</getlastmodified>
                            <resourcetype />
                        </prop>
                        <status>HTTP/1.1 200 OK</status>
                    </propstat>
                </response>
                <response>
                    <href>/foo/quux.dat</href>
                    <propstat>
                        <prop>
                            <displayname>quux.dat</displayname>
                            <getcontentlength>65535</getcontentlength>
                            <getcontenttype>application/octet-stream</getcontenttype>
                            <getetag>"ABCDEFGHIJKLMNOPQRSTUVWXYZ"</getetag>
                            <getlastmodified>2024-01-28T13:39:25+05:00</getlastmodified>
                            <resourcetype />
                        </prop>
                        <status>HTTP/1.1 307 TEMPORARY REDIRECT</status>
                    </propstat>
                    <location>https://www.example.com/data/quux.dat</location>
                </response>
            </multistatus>
        "#}
        );
    }
}
