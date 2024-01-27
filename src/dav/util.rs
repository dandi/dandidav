use super::VersionSpec;
use crate::dandi::DandisetId;
use crate::paths::PureDirPath;
use std::fmt::{self, Write};

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
