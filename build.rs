use anyhow::{bail, Context};
use std::env;
use std::io::ErrorKind;
use std::process::{Command, Stdio};

fn main() -> anyhow::Result<()> {
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/refs");
    let pkg_version = getenv("CARGO_PKG_VERSION")?;
    match get_commit_hash()? {
        Some(commit) => {
            println!("cargo:rustc-env=GIT_COMMIT={commit}");
            println!("cargo:rustc-env=VERSION_WITH_GIT={pkg_version} (commit: {commit})");
        }
        None => println!("cargo:rustc-env=VERSION_WITH_GIT={pkg_version}"),
    }
    Ok(())
}

fn get_commit_hash() -> anyhow::Result<Option<String>> {
    let manifest_dir = getenv("CARGO_MANIFEST_DIR")?;
    match Command::new("git")
        .arg("rev-parse")
        .arg("--git-dir")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .current_dir(&manifest_dir)
        .status()
    {
        Ok(rc) if rc.success() => {
            // We are in a Git repository
            let output = Command::new("git")
                .arg("rev-parse")
                .arg("--short")
                .arg("HEAD")
                .current_dir(&manifest_dir)
                .output()
                .context("failed to run `git rev-parse --short HEAD`")?;
            if !output.status.success() {
                bail!(
                    "`git rev-parse --short HEAD` command was not successful: {}",
                    output.status
                );
            }
            let revision = std::str::from_utf8(&output.stdout)
                .context("`git rev-parse --short HEAD` output was not UTF-8")?
                .trim()
                .to_owned();
            Ok(Some(revision))
        }
        Ok(_) => Ok(None), // We are not in a Git repository
        Err(e) if e.kind() == ErrorKind::NotFound => {
            // Git doesn't seem to be installed, so assume we're not in a Git
            // repository
            Ok(None)
        }
        Err(e) => Err(e).context("failed to run `git rev-parse --git-dir`"),
    }
}

fn getenv(name: &str) -> anyhow::Result<String> {
    env::var(name).with_context(|| format!("{name} envvar not set"))
}
