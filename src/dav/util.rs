use super::VersionSpec;
use crate::consts::DAV_XML_CONTENT_TYPE;
use crate::dandi::DandisetId;
use crate::httputil::HttpUrl;
use crate::paths::PureDirPath;
use axum::{
    async_trait,
    body::Body,
    extract::FromRequestParts,
    http::{header::CONTENT_TYPE, request::Parts, response::Response, StatusCode},
    response::IntoResponse,
};
use indoc::indoc;
use percent_encoding::{percent_encode, AsciiSet, NON_ALPHANUMERIC};
use serde::{ser::Serializer, Serialize};
use std::fmt::{self, Write};
use time::{
    format_description::{well_known::Rfc3339, FormatItem},
    macros::format_description,
    OffsetDateTime,
};

/// Timestamp format for display of the "getlastmodified" property in WebDAV
/// XML documents
static RFC1123: &[FormatItem<'_>] = format_description!(
    "[weekday repr:short], [day] [month repr:short] [year] [hour]:[minute]:[second] GMT"
);

/// ASCII bytes in "href" values to percent-encode
///
/// The character set is based on the behavior of Python's
/// `urllib.parse.quote()`
static PERCENT_ESCAPED: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'/')
    .remove(b'_')
    .remove(b'~');

/// Response body to return in reply to `PROPFIND` requests with missing or
/// "infinite" `Depth` headers
static INFINITE_DEPTH_RESPONSE: &str = indoc! {r#"
<?xml version="1.0" encoding="utf-8"?>
<error xmlns="DAV:">
    <propfind-finite-depth />
</error>
"#};

/// Return the path at which `dandidav` serves the given Dandiset & version
/// under `/dandisets/`.
///
/// The returned value will have one of the following formats:
///
/// - `dandiset/{dandiset_id}/draft/`
/// - `dandiset/{dandiset_id}/latest/`
/// - `dandiset/{dandiset_id}/releases/{version_id}/`
pub(super) fn version_path(dandiset_id: &DandisetId, version: &VersionSpec) -> PureDirPath {
    fn writer(s: &mut String, dandiset_id: &DandisetId, version: &VersionSpec) -> fmt::Result {
        write!(s, "dandisets/{dandiset_id}/")?;
        match version {
            VersionSpec::Draft => write!(s, "draft")?,
            VersionSpec::Published(v) => write!(s, "releases/{v}")?,
            VersionSpec::Latest => write!(s, "latest")?,
        }
        write!(s, "/")?;
        Ok(())
    }

    let mut s = String::new();
    writer(&mut s, dandiset_id, version).expect("writing to a String shouldn't fail");
    PureDirPath::try_from(s).expect("should be a valid dir path")
}

/// Format a timestamp for display as a "creationdate" property in a WebDAV XML
/// document
pub(super) fn format_creationdate(dt: OffsetDateTime) -> String {
    dt.format(&Rfc3339)
        .expect("formatting an OffsetDateTime in RFC 3339 format should not fail")
}

/// Format a timestamp for display as a "getlastmodified" property in a WebDAV
/// XML document
pub(super) fn format_modifieddate(dt: OffsetDateTime) -> String {
    dt.to_offset(time::UtcOffset::UTC)
        .format(&RFC1123)
        .expect("formatting an OffsetDateTime in RFC 1123 format should not fail")
}

/// A non-infinite `Depth` WebDAV header value
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum FiniteDepth {
    Zero,
    One,
}

#[async_trait]
impl<S: Send + Sync> FromRequestParts<S> for FiniteDepth {
    type Rejection = Response<Body>;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        match parts.headers.get("Depth").map(|v| v.to_str()) {
            Some(Ok("0")) => Ok(FiniteDepth::Zero),
            Some(Ok("1")) => Ok(FiniteDepth::One),
            Some(Ok("infinity")) | None => Err((
                StatusCode::FORBIDDEN,
                [(CONTENT_TYPE, DAV_XML_CONTENT_TYPE)],
                INFINITE_DEPTH_RESPONSE,
            )
                .into_response()),
            _ => Err((StatusCode::BAD_REQUEST, "Invalid \"Depth\" header\n").into_response()),
        }
    }
}

/// A percent-encoded URI or URI path, for use in the `href` attribute of an
/// HTML `<a>` tag or in a `<DAV:href>` tag in a `PROPFIND` response
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct Href(String);

impl Href {
    /// Construct an `Href` from a non-percent-encoded URI path
    pub(super) fn from_path(path: &str) -> Href {
        Href(percent_encode(path.as_ref(), PERCENT_ESCAPED).to_string())
    }
}

impl AsRef<str> for Href {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

impl From<HttpUrl> for Href {
    fn from(value: HttpUrl) -> Href {
        Href(value.to_string())
    }
}

impl From<&HttpUrl> for Href {
    fn from(value: &HttpUrl) -> Href {
        Href(value.to_string())
    }
}

impl Serialize for Href {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    #[test]
    fn test_href_from_path() {
        let s = "/~cleesh/foo bar/baz_quux.gnusto/red&green?blue";
        assert_eq!(
            Href::from_path(s).as_ref(),
            "/~cleesh/foo%20bar/baz_quux.gnusto/red%26green%3Fblue"
        );
    }

    #[test]
    fn test_format_modifieddate() {
        let dt = datetime!(1994-11-06 03:49:37 -5);
        assert_eq!(format_modifieddate(dt), "Sun, 06 Nov 1994 08:49:37 GMT");
    }
}
