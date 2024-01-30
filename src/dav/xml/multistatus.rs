use super::*;
use crate::consts::DAV_XMLNS;
use crate::dav::util::Href;
use std::collections::BTreeMap;
use thiserror::Error;
use xml::writer::{events::XmlEvent, EmitterConfig, Error as WriteError, EventWriter};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::dav) struct Multistatus {
    pub(in crate::dav) response: Vec<DavResponse>,
    //responsedescription
}

impl Multistatus {
    pub(in crate::dav) fn to_xml(&self) -> Result<String, ToXmlError> {
        let mut writer = XmlWriter::new();
        writer.tag_xmlns("multistatus", DAV_XMLNS, |writer| {
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
pub(in crate::dav) struct DavResponse {
    pub(in crate::dav) href: Href,
    // TODO: RFC 4918 says <response> can contain (href*, status) as an
    // alternative to propstat.  When does that apply?
    pub(in crate::dav) propstat: Vec<PropStat>,
    //error
    //responsedescription
    pub(in crate::dav) location: Option<Href>,
}

impl DavResponse {
    fn write_xml(&self, writer: &mut XmlWriter) -> Result<(), WriteError> {
        writer.tag("response", |writer| {
            writer.text_tag("href", self.href.as_ref())?;
            for p in &self.propstat {
                p.write_xml(writer)?;
            }
            if let Some(ref loc) = self.location {
                writer.tag("location", |writer| writer.text_tag("href", loc.as_ref()))?;
            }
            Ok(())
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::dav) struct PropStat {
    pub(in crate::dav) prop: BTreeMap<Property, PropValue>,
    pub(in crate::dav) status: String,
    //error
    //responsedescription
}

impl PropStat {
    fn write_xml(&self, writer: &mut XmlWriter) -> Result<(), WriteError> {
        writer.tag("propstat", |writer| {
            writer.tag("prop", |writer| {
                for (k, v) in &self.prop {
                    k.write_xml(writer, v)?;
                }
                Ok(())
            })?;
            writer.text_tag("status", &self.status)?;
            Ok(())
        })
    }
}

pub(in crate::dav) struct XmlWriter(EventWriter<Vec<u8>>);

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
        self.start_tag_ns(name, ns)?;
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

    pub(super) fn start_tag(&mut self, name: &str) -> Result<(), WriteError> {
        self.0.write(XmlEvent::start_element(name))
    }

    pub(super) fn start_tag_ns(&mut self, name: &str, ns: &str) -> Result<(), WriteError> {
        self.0.write(XmlEvent::start_element(name).default_ns(ns))
    }

    pub(super) fn end_tag(&mut self) -> Result<(), WriteError> {
        self.0.write(XmlEvent::end_element())
    }

    pub(super) fn empty_tag(&mut self, name: &str) -> Result<(), WriteError> {
        self.start_tag(name)?;
        self.end_tag()?;
        Ok(())
    }

    pub(super) fn text(&mut self, text: &str) -> Result<(), WriteError> {
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
pub(crate) enum ToXmlError {
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
                DavResponse {
                    href: Href::from_path("/foo/"),
                    propstat: vec![PropStat {
                        prop: BTreeMap::from([
                            (Property::ResourceType, PropValue::Collection),
                            (Property::DisplayName, PropValue::String("foo".into())),
                        ]),
                        status: "HTTP/1.1 200 OK".into(),
                    }],
                    location: None,
                },
                DavResponse {
                    href: Href::from_path("/foo/bar.txt"),
                    propstat: vec![PropStat {
                        prop: BTreeMap::from([
                            (
                                Property::CreationDate,
                                PropValue::String("2024-01-28T13:36:54+05:00".into()),
                            ),
                            (Property::DisplayName, PropValue::String("bar.txt".into())),
                            (Property::GetContentLength, PropValue::Int(42)),
                            (
                                Property::GetContentType,
                                PropValue::String("text/plain; charset=us-ascii".into()),
                            ),
                            (
                                Property::GetETag,
                                PropValue::String(r#""0123456789abcdef""#.into()),
                            ),
                            (
                                Property::GetLastModified,
                                PropValue::String("2024-01-28T13:38:10+05:00".into()),
                            ),
                            (Property::ResourceType, PropValue::Empty),
                        ]),
                        status: "HTTP/1.1 200 OK".into(),
                    }],
                    location: None,
                },
                DavResponse {
                    href: Href::from_path("/foo/quux.dat"),
                    propstat: vec![PropStat {
                        prop: BTreeMap::from([
                            (Property::DisplayName, PropValue::String("quux.dat".into())),
                            (Property::GetContentLength, PropValue::Int(65535)),
                            (
                                Property::GetContentType,
                                PropValue::String("application/octet-stream".into()),
                            ),
                            (
                                Property::GetETag,
                                PropValue::String(r#""ABCDEFGHIJKLMNOPQRSTUVWXYZ""#.into()),
                            ),
                            (
                                Property::GetLastModified,
                                PropValue::String("2024-01-28T13:39:25+05:00".into()),
                            ),
                            (Property::ResourceType, PropValue::Empty),
                        ]),
                        status: "HTTP/1.1 307 TEMPORARY REDIRECT".into(),
                    }],
                    location: Some(
                        url::Url::parse("https://www.example.com/data/quux.dat")
                            .unwrap()
                            .into(),
                    ),
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
                    <location>
                        <href>https://www.example.com/data/quux.dat</href>
                    </location>
                </response>
            </multistatus>
        "#}
        );
    }
}
