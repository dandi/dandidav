use thiserror::Error;

/// A nonempty path component that does not contain a forward slash or NUL nor
/// equals `.` or `..`
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct Component(pub(super) String);

fn validate(s: &str) -> Result<(), ParseComponentError> {
    if s.is_empty() {
        Err(ParseComponentError::Empty)
    } else if s.contains('/') {
        Err(ParseComponentError::Slash)
    } else if s.contains('\0') {
        Err(ParseComponentError::Nul)
    } else if s == "." || s == ".." {
        Err(ParseComponentError::SpecialDir)
    } else {
        Ok(())
    }
}

validstr!(
    Component,
    ParseComponentError,
    validate,
    "a plain path component"
);

impl Component {
    pub(crate) fn strip_suffix(&self, suffix: &str) -> Option<Component> {
        let s = self.0.strip_suffix(suffix)?;
        (!s.is_empty()).then(|| Component(s.into()))
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum ParseComponentError {
    #[error("path components cannot be empty")]
    Empty,
    #[error("path components cannot contain a forward slash")]
    Slash,
    #[error("path components cannot contain NUL")]
    Nul,
    #[error(r#"path components cannot equal "." or "..""#)]
    SpecialDir,
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use rstest::rstest;

    #[rstest]
    #[case("foo")]
    #[case("foo.nwb")]
    #[case("foo bar")]
    fn test_good(#[case] s: &str) {
        let r = s.parse::<Component>();
        assert_matches!(r, Ok(c) => {
            assert_eq!(c, s);
        });
    }

    #[rstest]
    #[case("")]
    #[case(".")]
    #[case("..")]
    #[case("/")]
    #[case("\0")]
    #[case("foo/bar.nwb")]
    #[case("foo\0bar.nwb")]
    #[case("/foo")]
    #[case("foo/")]
    #[case("/foo/")]
    fn test_bad(#[case] s: &str) {
        let r = s.parse::<Component>();
        assert_matches!(r, Err(_));
    }
}
