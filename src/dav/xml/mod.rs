//! Working with WebDAV XML documents
mod multistatus;
mod propfind;
pub(super) use self::multistatus::*;
pub(super) use self::propfind::*;
use crate::consts::DAV_XMLNS;
use std::fmt;
use xml::writer::Error as WriteError;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(in crate::dav) enum Property {
    CreationDate,
    DisplayName,
    //GetContentLanguage,
    GetContentLength,
    GetContentType,
    GetETag,
    GetLastModified,
    ResourceType,
    //LockDiscovery,
    //SupportedLock,
    Custom(Tag),
}

impl Property {
    pub(in crate::dav) fn iter_standard() -> impl Iterator<Item = Property> {
        [
            Property::CreationDate,
            Property::DisplayName,
            Property::GetContentLength,
            Property::GetContentType,
            Property::GetETag,
            Property::GetLastModified,
            Property::ResourceType,
        ]
        .into_iter()
    }

    fn write_xml(&self, writer: &mut XmlWriter, value: &PropValue) -> Result<(), WriteError> {
        match self {
            Property::CreationDate => writer.start_tag("creationdate")?,
            Property::DisplayName => writer.start_tag("displayname")?,
            Property::GetContentLength => writer.start_tag("getcontentlength")?,
            Property::GetContentType => writer.start_tag("getcontenttype")?,
            Property::GetETag => writer.start_tag("getetag")?,
            Property::GetLastModified => writer.start_tag("getlastmodified")?,
            Property::ResourceType => writer.start_tag("resourcetype")?,
            Property::Custom(tag) => writer.start_tag_ns(&tag.name, &tag.namespace)?,
        }
        value.write_xml(writer)?;
        writer.end_tag()?;
        Ok(())
    }
}

impl From<Tag> for Property {
    fn from(tag: Tag) -> Property {
        match tag.dav_name() {
            Some("creationdate") => Property::CreationDate,
            Some("displayname") => Property::DisplayName,
            Some("getcontentlength") => Property::GetContentLength,
            Some("getcontenttype") => Property::GetContentType,
            Some("getetag") => Property::GetETag,
            Some("getlastmodified") => Property::GetLastModified,
            Some("resourcetype") => Property::ResourceType,
            _ => Property::Custom(tag),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::dav) enum PropValue {
    // Used for <resourcetype> of non-collections, requested properties that
    // aren't present, and <propname> responses
    Empty,
    // `<resourcetype>` value for collections
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

impl From<String> for PropValue {
    fn from(value: String) -> PropValue {
        PropValue::String(value)
    }
}

impl From<i64> for PropValue {
    fn from(value: i64) -> PropValue {
        PropValue::Int(value)
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct Tag {
    pub(super) name: String,
    pub(super) namespace: String,
}

impl Tag {
    pub(super) fn new(name: String, namespace: Option<String>) -> Tag {
        Tag {
            name,
            namespace: namespace.unwrap_or_else(|| DAV_XMLNS.to_owned()),
        }
    }

    pub(super) fn dav_name(&self) -> Option<&str> {
        (self.namespace == DAV_XMLNS).then(|| &*self.name)
    }
}

impl fmt::Display for Tag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{{}}}{}", self.namespace, self.name)
    }
}
