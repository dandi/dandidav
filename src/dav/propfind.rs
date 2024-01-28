use super::multistatus::{DavResponse, PropStat, PropValue, Property};
use super::types::HasProperties;
use std::collections::BTreeMap;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum PropFind {
    AllProp {
        include: Vec<Property>,
    },
    #[allow(dead_code)]
    Prop(Vec<Property>),
    #[allow(dead_code)]
    PropName,
}

impl PropFind {
    pub(super) fn find<P: HasProperties>(&self, res: &P) -> DavResponse {
        let mut found = BTreeMap::new();
        let mut missing = BTreeMap::new();
        match self {
            PropFind::AllProp { include } => {
                for prop in Property::iter_standard() {
                    if let Some(value) = res.property(&prop) {
                        found.insert(prop, value);
                    }
                }
                for prop in include {
                    if let Some(value) = res.property(prop) {
                        found.insert(prop.clone(), value);
                    } else {
                        missing.insert(prop.clone(), PropValue::Empty);
                    }
                }
            }
            PropFind::Prop(props) => {
                for prop in props {
                    if let Some(value) = res.property(prop) {
                        found.insert(prop.clone(), value);
                    } else {
                        missing.insert(prop.clone(), PropValue::Empty);
                    }
                }
            }
            PropFind::PropName => {
                for prop in Property::iter_standard() {
                    found.insert(prop, PropValue::Empty);
                }
            }
        }
        let mut propstat = Vec::with_capacity(2);
        if !found.is_empty() || missing.is_empty() {
            propstat.push(PropStat {
                prop: found,
                status: "HTTP/1.1 200 OK".into(),
            });
        }
        if !missing.is_empty() {
            propstat.push(PropStat {
                prop: missing,
                status: "HTTP/1.1 404 NOT FOUND".into(),
            });
        }
        DavResponse {
            href: res.href(),
            propstat,
            // TODO: Should `location` be set to redirect URLs?
            location: None,
        }
    }
}

impl Default for PropFind {
    fn default() -> PropFind {
        PropFind::AllProp {
            include: Vec::new(),
        }
    }
}
