use super::VersionSpec;
use crate::dandi::DandisetId;
use crate::paths::PureDirPath;
use percent_encoding::{percent_encode, AsciiSet, NON_ALPHANUMERIC};
use std::fmt::{self, Write};
use time::{
    format_description::{well_known::Rfc3339, FormatItem},
    macros::format_description,
    OffsetDateTime,
};

static RFC1123: &[FormatItem<'_>] = format_description!(
    "[weekday repr:short], [day] [month repr:short] [year] [hour]:[minute]:[second] GMT"
);

// Selection of safe characters based on Python's `urllib.parse.quote()`
static PERCENT_ESCAPED: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'/')
    .remove(b'_')
    .remove(b'~');

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
    s.parse::<PureDirPath>()
        .expect("should be a valid dir path")
}

pub(super) fn urlencode(s: &str) -> String {
    percent_encode(s.as_ref(), PERCENT_ESCAPED).to_string()
}

pub(super) fn format_creationdate(dt: OffsetDateTime) -> String {
    dt.format(&Rfc3339)
        .expect("formatting an OffsetDateTime in RFC 3339 format should not fail")
}

pub(super) fn format_modifieddate(dt: OffsetDateTime) -> String {
    dt.to_offset(time::UtcOffset::UTC)
        .format(&RFC1123)
        .expect("formatting an OffsetDateTime in RFC 1123 format should not fail")
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    #[test]
    fn test_urlencode() {
        let s = "/~cleesh/foo bar/baz_quux.gnusto/red&green?blue";
        assert_eq!(
            urlencode(s),
            "/~cleesh/foo%20bar/baz_quux.gnusto/red%26green%3Fblue"
        );
    }

    #[test]
    fn test_format_modifieddate() {
        let dt = datetime!(1994-11-06 03:49:37 -5);
        assert_eq!(format_modifieddate(dt), "Sun, 06 Nov 1994 08:49:37 GMT");
    }
}
