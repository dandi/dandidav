use html5ever::rcdom::Handle;
use soup::{NodeExt, QueryBuilderExt, Soup};
use thiserror::Error;

static COLUMNS: usize = 5;

static EXPECTED_COLUMN_NAMES: [&str; COLUMNS] = ["Name", "Type", "Size", "Created", "Modified"];

pub fn parse_collection_page(html: &str) -> Result<CollectionPage, ParseCollectionError> {
    let soup = Soup::new(html);
    let breadcrumbs = soup
        .tag("div")
        .class("breadcrumbs")
        .find()
        .ok_or(ParseCollectionError::NoBreadcrumbs)?
        .tag("a")
        .find_all()
        .map(Link::from_handle)
        .collect::<Result<Vec<_>, _>>()?;
    let table_tag = soup
        .tag("table")
        .class("collection")
        .find()
        .ok_or(ParseCollectionError::NoTable)?;
    let header_cells = table_tag
        .tag("thead")
        .find()
        .ok_or(ParseCollectionError::NoTHead)?
        .tag("tr")
        .find()
        .ok_or(ParseCollectionError::NoHeaderRow)?
        .tag("th")
        .find_all()
        .collect::<Vec<_>>();
    if header_cells.len() != COLUMNS {
        return Err(ParseCollectionError::ColumnQtyMismatch {
            expected: COLUMNS,
            actual: header_cells.len(),
        });
    }
    for (expected, th) in std::iter::zip(EXPECTED_COLUMN_NAMES, header_cells) {
        let actual = th.text();
        if actual != expected {
            return Err(ParseCollectionError::ColumnNameMismatch { expected, actual });
        }
    }
    let mut table = Vec::new();
    for tr in table_tag
        .tag("tbody")
        .find()
        .ok_or(ParseCollectionError::NoTBody)?
        .tag("tr")
        .find_all()
    {
        let mut cells = tr.tag("td").find_all();
        let Some(name_td) = cells.next() else {
            return Err(ParseCollectionError::RowLengthMismatch {
                expected: COLUMNS,
                actual: 0,
            });
        };
        let Some(name) = name_td
            .tag("span")
            .class("item-link")
            .find()
            .map(Link::from_handle)
        else {
            return Err(ParseCollectionError::NoItemLink);
        };
        let name = name?;
        let metadata_link = name_td
            .tag("span")
            .class("metadata-link")
            .find()
            .and_then(|handle| handle.tag("a").find())
            .and_then(|atag| atag.get("href"));
        let Some(typekind) = cells.next().map(|td| td.text()) else {
            return Err(ParseCollectionError::RowLengthMismatch {
                expected: COLUMNS,
                actual: 1,
            });
        };
        let Some(size) = cells.next().map(|td| td.text()) else {
            return Err(ParseCollectionError::RowLengthMismatch {
                expected: COLUMNS,
                actual: 2,
            });
        };
        let Some(created) = cells.next().map(|td| td.text()) else {
            return Err(ParseCollectionError::RowLengthMismatch {
                expected: COLUMNS,
                actual: 3,
            });
        };
        let Some(modified) = cells.next().map(|td| td.text()) else {
            return Err(ParseCollectionError::RowLengthMismatch {
                expected: COLUMNS,
                actual: 4,
            });
        };
        let remaining = cells.count();
        if remaining > 0 {
            return Err(ParseCollectionError::RowLengthMismatch {
                expected: COLUMNS,
                actual: remaining + 5,
            });
        };
        table.push(CollectionEntry {
            name,
            metadata_link,
            typekind,
            size,
            created,
            modified,
        });
    }
    Ok(CollectionPage { breadcrumbs, table })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CollectionPage {
    pub breadcrumbs: Vec<Link>,
    pub table: Vec<CollectionEntry>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CollectionEntry {
    pub name: Link,
    pub metadata_link: Option<String>,
    pub typekind: String,
    pub size: String,
    pub created: String,
    pub modified: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Link {
    text: String,
    href: String,
}

impl Link {
    fn from_handle(handle: Handle) -> Result<Link, ParseLinkError> {
        let href = handle.get("href").ok_or(ParseLinkError::NoHref)?;
        let text = handle.text();
        Ok(Link { text, href })
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ParseCollectionError {
    #[error("breadcrumbs div not found in source")]
    NoBreadcrumbs,
    #[error("collection table not found in source")]
    NoTable,
    #[error("collection table did not contain a <thead> element")]
    NoTHead,
    #[error("collection table's <thead> did not contain a <tr> element")]
    NoHeaderRow,
    #[error("expected collection table to have {expected} columns, but found {actual}")]
    ColumnQtyMismatch { expected: usize, actual: usize },
    #[error(
        "expected a collection header to have text {expected:?} columns, but found {actual:?}"
    )]
    ColumnNameMismatch {
        expected: &'static str,
        actual: String,
    },
    #[error("collection table did not contain a <tbody> element")]
    NoTBody,
    #[error("row in collection table had {actual} columns; expected {expected}")]
    RowLengthMismatch { expected: usize, actual: usize },
    #[error("row in collection table missing item-link span")]
    NoItemLink,
    #[error(transparent)]
    Link(#[from] ParseLinkError),
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ParseLinkError {
    #[error("<a> tag missing href attribute")]
    NoHref,
}
