use anyhow::{bail, Context};
use std::env;
use std::io::ErrorKind;
use std::process::{Command, Stdio};

fn main() -> anyhow::Result<()> {
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/refs");
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
            let mut revision = String::from_utf8(output.stdout)
                .context("`git rev-parse --short HEAD` output was not UTF-8")?;
            chomp(&mut revision);
            println!("cargo:rustc-env=GIT_COMMIT={revision}");
            Ok(())
        }
        Ok(_) => Ok(()), // We are not in a Git repository
        Err(e) if e.kind() == ErrorKind::NotFound => {
            // Git doesn't seem to be installed, so assume we're not in a Git
            // repository
            Ok(())
        }
        Err(e) => Err(e).context("failed to run `git rev-parse --git-dir`"),
    }
}

fn getenv(name: &str) -> anyhow::Result<String> {
    env::var(name).with_context(|| format!("{name} envvar not set"))
}

fn chomp(s: &mut String) {
    if s.ends_with('\n') {
        s.pop();
        if s.ends_with('\r') {
            s.pop();
        }
    }
}
