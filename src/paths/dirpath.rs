use super::{Component, PurePath};
use thiserror::Error;

/// A nonempty, forward-slash-separated path that ends in (but does not equal)
/// a forward slash and does not contain any of the following:
///
/// - a `.` or `..` component
/// - a leading forward slash
/// - two or more consecutive forward slashes
/// - NUL
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct PureDirPath(pub(super) String);

fn validate(s: &str) -> Result<(), ParsePureDirPathError> {
    let Some(pre) = s.strip_suffix('/') else {
        return Err(ParsePureDirPathError::NotDir);
    };
    if s.starts_with('/') {
        Err(ParsePureDirPathError::StartsWithSlash)
    } else if s.contains('\0') {
        Err(ParsePureDirPathError::Nul)
    } else if pre
        .split('/')
        .any(|p| p.is_empty() || p == "." || p == "..")
    {
        Err(ParsePureDirPathError::NotNormalized)
    } else {
        Ok(())
    }
}

validstr!(
    PureDirPath,
    ParsePureDirPathError,
    validate,
    "a normalized relative directory path"
);

impl PureDirPath {
    pub(crate) fn name_str(&self) -> &str {
        self.0
            .trim_end_matches('/')
            .split('/')
            .next_back()
            .expect("path should be nonempty")
    }

    pub(crate) fn name(&self) -> Component {
        Component(self.name_str().to_owned())
    }

    pub(crate) fn parent(&self) -> Option<PureDirPath> {
        let i = self.0.trim_end_matches('/').rfind('/')?;
        Some(PureDirPath(self.0[..=i].to_owned()))
    }

    pub(crate) fn join(&self, path: &PurePath) -> PurePath {
        PurePath(format!("{self}{path}"))
    }

    pub(crate) fn join_dir(&self, path: &PureDirPath) -> PureDirPath {
        PureDirPath(format!("{self}{path}"))
    }

    pub(crate) fn join_one_dir(&self, c: &Component) -> PureDirPath {
        PureDirPath(format!("{self}{c}/"))
    }

    pub(crate) fn push(&mut self, c: &Component) {
        self.0.push_str(c.as_ref());
        self.0.push('/');
    }

    pub(crate) fn relative_to(&self, dirpath: &PureDirPath) -> Option<PureDirPath> {
        let s = self.0.strip_prefix(&dirpath.0)?;
        (!s.is_empty()).then(|| PureDirPath(s.to_owned()))
    }

    pub(crate) fn component_strs(&self) -> std::str::Split<'_, char> {
        self.0.trim_end_matches('/').split('/')
    }
}

impl From<Component> for PureDirPath {
    fn from(value: Component) -> PureDirPath {
        let mut s = value.0;
        s.push('/');
        PureDirPath(s)
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum ParsePureDirPathError {
    #[error("path does not end with a forward slash")]
    NotDir,
    #[error("paths cannot start with a forward slash")]
    StartsWithSlash,
    #[error("paths cannot contain NUL")]
    Nul,
    #[error("path is not normalized")]
    NotNormalized,
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use rstest::rstest;

    #[rstest]
    #[case("foo.nwb/")]
    #[case("foo/bar.nwb/")]
    fn test_good_paths(#[case] s: &str) {
        let r = s.parse::<PureDirPath>();
        assert_matches!(r, Ok(_));
    }

    #[rstest]
    #[case("")]
    #[case("/")]
    #[case("/foo")]
    #[case("foo")]
    #[case("foo/bar.nwb")]
    #[case("/foo/")]
    #[case("foo//bar.nwb/")]
    #[case("foo///bar.nwb/")]
    #[case("foo/bar\0.nwb/")]
    #[case("foo/./bar.nwb/")]
    #[case("foo/../bar.nwb/")]
    #[case("./foo/bar.nwb/")]
    #[case("../foo/bar.nwb/")]
    #[case("foo/bar.nwb/.")]
    #[case("foo/bar.nwb/..")]
    #[case("foo/bar.nwb/./")]
    #[case("foo/bar.nwb/../")]
    fn test_bad_paths(#[case] s: &str) {
        let r = s.parse::<PureDirPath>();
        assert_matches!(r, Err(_));
    }

    #[test]
    fn test_parent() {
        let p = "foo/bar/baz/".parse::<PureDirPath>().unwrap();
        assert_matches!(p.parent(), Some(pp) => {
            assert_eq!(pp, "foo/bar/");
        });
    }

    #[test]
    fn test_noparent() {
        let p = "foo/".parse::<PureDirPath>().unwrap();
        assert_matches!(p.parent(), None);
    }

    #[rstest]
    #[case("foo/", "foo")]
    #[case("foo/bar/", "bar")]
    fn test_name(#[case] dirpath: PureDirPath, #[case] name: &str) {
        assert_eq!(dirpath.name(), name);
        assert_eq!(dirpath.name_str(), name);
    }

    #[rstest]
    #[case("foo/", "bar", "foo/bar")]
    #[case("foo/", "baz/quux", "foo/baz/quux")]
    #[case("foo/bar/", "quux", "foo/bar/quux")]
    #[case("foo/bar/", "gnusto/cleesh", "foo/bar/gnusto/cleesh")]
    fn test_join(#[case] dirpath: PureDirPath, #[case] path: PurePath, #[case] res: PurePath) {
        assert_eq!(dirpath.join(&path), res);
    }

    #[rstest]
    #[case("foo/", "bar/", "foo/bar/")]
    #[case("foo/", "baz/quux/", "foo/baz/quux/")]
    #[case("foo/bar/", "quux/", "foo/bar/quux/")]
    #[case("foo/bar/", "gnusto/cleesh/", "foo/bar/gnusto/cleesh/")]
    fn test_join_dir(
        #[case] dirpath: PureDirPath,
        #[case] path: PureDirPath,
        #[case] res: PureDirPath,
    ) {
        assert_eq!(dirpath.join_dir(&path), res);
    }

    #[rstest]
    #[case("foo/", "bar", "foo/bar/")]
    #[case("foo/bar/", "quux", "foo/bar/quux/")]
    fn test_join_one_dir(
        #[case] dirpath: PureDirPath,
        #[case] c: Component,
        #[case] res: PureDirPath,
    ) {
        assert_eq!(dirpath.join_one_dir(&c), res);
    }

    #[rstest]
    #[case("foo/", "bar", "foo/bar/")]
    #[case("foo/bar/", "quux", "foo/bar/quux/")]
    fn test_push(#[case] mut dirpath: PureDirPath, #[case] c: Component, #[case] res: PureDirPath) {
        dirpath.push(&c);
        assert_eq!(dirpath, res);
    }

    #[rstest]
    #[case("foo/bar/", "foo/", Some("bar/"))]
    #[case("foo/bar/quux/", "foo/", Some("bar/quux/"))]
    #[case("foo/bar/quux/", "foo/bar/", Some("quux/"))]
    #[case("foo/", "foo/bar/", None)]
    #[case("bar/quux/", "foo/bar/quux/", None)]
    #[case("foo/bar/", "quux/bar/", None)]
    fn test_relative_to(
        #[case] path: PureDirPath,
        #[case] dirpath: PureDirPath,
        #[case] relpath: Option<&str>,
    ) {
        assert_eq!(path.relative_to(&dirpath).as_deref(), relpath);
    }

    #[rstest]
    #[case("foo/", vec!["foo"])]
    #[case("foo/bar/", vec!["foo", "bar"])]
    #[case("foo/bar/quux/", vec!["foo", "bar", "quux"])]
    fn test_component_strs(#[case] dirpath: PureDirPath, #[case] comps: Vec<&str>) {
        assert_eq!(dirpath.component_strs().collect::<Vec<_>>(), comps);
    }

    #[test]
    fn test_from_component() {
        let c = "foo".parse::<Component>().unwrap();
        let p = PureDirPath::from(c);
        assert_eq!(p, "foo/");
    }
}
