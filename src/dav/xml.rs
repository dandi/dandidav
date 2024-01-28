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
    prop: Prop,
    status: String,
    //error
    //responsedescription
}

impl PropStat {
    fn write_xml(&self, writer: &mut XmlWriter) -> Result<(), WriteError> {
        writer.tag("propstat", |writer| {
            self.prop.write_xml(writer)?;
            writer.text_tag("status", &self.status)?;
            Ok(())
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Prop {
    creationdate: Option<String>,
    displayname: Option<String>,
    getcontentlength: Option<i64>,
    getetag: Option<String>,
    getcontenttype: Option<String>,
    getlastmodified: Option<String>,
    is_collection: bool,
}

impl Prop {
    fn write_xml(&self, writer: &mut XmlWriter) -> Result<(), WriteError> {
        writer.tag("prop", |writer| {
            writer.tag("resourcetype", |writer| {
                if self.is_collection {
                    writer.empty_tag("collection")?;
                }
                Ok(())
            })?;
            if let Some(ref s) = self.creationdate {
                writer.text_tag("creationdate", s)?;
            }
            if let Some(ref s) = self.displayname {
                writer.text_tag("displayname", s)?;
            }
            if let Some(size) = self.getcontentlength {
                writer.text_tag("getcontentlength", &format!("{size}"))?;
            }
            if let Some(ref s) = self.getetag {
                writer.text_tag("getetag", s)?;
            }
            if let Some(ref s) = self.getcontenttype {
                writer.text_tag("getcontenttype", s)?;
            }
            if let Some(ref s) = self.getlastmodified {
                writer.text_tag("getlastmodified", s)?;
            }
            Ok(())
        })
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
                        prop: Prop {
                            creationdate: None,
                            displayname: Some("foo".into()),
                            getcontentlength: None,
                            getetag: None,
                            getcontenttype: None,
                            getlastmodified: None,
                            is_collection: true,
                        },
                        status: "HTTP/1.1 200 OK".into(),
                    },
                    location: None,
                },
                Response {
                    href: "/foo/bar.txt".into(),
                    propstat: PropStat {
                        prop: Prop {
                            creationdate: Some("2024-01-28T13:36:54+05:00".into()),
                            displayname: Some("bar.txt".into()),
                            getcontentlength: Some(42),
                            getetag: Some(r#""0123456789abcdef""#.into()),
                            getcontenttype: Some("text/plain; charset=us-ascii".into()),
                            getlastmodified: Some("2024-01-28T13:38:10+05:00".into()),
                            is_collection: false,
                        },
                        status: "HTTP/1.1 200 OK".into(),
                    },
                    location: None,
                },
                Response {
                    href: "/foo/quux.dat".into(),
                    propstat: PropStat {
                        prop: Prop {
                            creationdate: None,
                            displayname: Some("quux.dat".into()),
                            getcontentlength: Some(65535),
                            getetag: Some(r#""ABCDEFGHIJKLMNOPQRSTUVWXYZ""#.into()),
                            getcontenttype: Some("application/octet-stream".into()),
                            getlastmodified: Some("2024-01-28T13:39:25+05:00".into()),
                            is_collection: false,
                        },
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
                            <resourcetype>
                                <collection />
                            </resourcetype>
                            <displayname>foo</displayname>
                        </prop>
                        <status>HTTP/1.1 200 OK</status>
                    </propstat>
                </response>
                <response>
                    <href>/foo/bar.txt</href>
                    <propstat>
                        <prop>
                            <resourcetype />
                            <creationdate>2024-01-28T13:36:54+05:00</creationdate>
                            <displayname>bar.txt</displayname>
                            <getcontentlength>42</getcontentlength>
                            <getetag>"0123456789abcdef"</getetag>
                            <getcontenttype>text/plain; charset=us-ascii</getcontenttype>
                            <getlastmodified>2024-01-28T13:38:10+05:00</getlastmodified>
                        </prop>
                        <status>HTTP/1.1 200 OK</status>
                    </propstat>
                </response>
                <response>
                    <href>/foo/quux.dat</href>
                    <propstat>
                        <prop>
                            <resourcetype />
                            <displayname>quux.dat</displayname>
                            <getcontentlength>65535</getcontentlength>
                            <getetag>"ABCDEFGHIJKLMNOPQRSTUVWXYZ"</getetag>
                            <getcontenttype>application/octet-stream</getcontenttype>
                            <getlastmodified>2024-01-28T13:39:25+05:00</getlastmodified>
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
