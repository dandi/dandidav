use crate::paths::PurePath;
use thiserror::Error;

pub(super) fn parse_apache_index(_s: &str) -> Result<Vec<PurePath>, ParseIndexError> {
    todo!()
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("TODO")]
pub(crate) struct ParseIndexError;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_apache_index() {
        let s = include_str!("testdata/016.html");
        assert_eq!(parse_apache_index(s).unwrap(), ["5a2", "fc8", "foo bar"]);
    }
}
