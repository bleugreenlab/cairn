//! Runner-only publication of validated executor trees into jj history.

use std::io::Write as _;
use std::path::Path;
use std::path::PathBuf;
use std::process::Stdio;

/// A Git pack protected from pruning while a ref-less delta is being folded.
pub struct DeltaObjectPin {
    keep_path: PathBuf,
    owned: bool,
}

impl Drop for DeltaObjectPin {
    fn drop(&mut self) {
        if !self.owned {
            return;
        }
        if let Err(error) = std::fs::remove_file(&self.keep_path) {
            if error.kind() != std::io::ErrorKind::NotFound {
                log::warn!(
                    "failed to release delta object pin {}: {error}",
                    self.keep_path.display()
                );
            }
        }
    }
}

/// Protect a validated delta from even immediate Git pruning until its tree is
/// committed into jj. Managed packs pass their installed path directly;
/// colocated loose objects are copied into a dedicated non-thin pack first.
pub(crate) fn pin_validated_delta(
    repository: &Path,
    base_commit: &str,
    delta_commit: &str,
    installed_pack: Option<&Path>,
) -> Result<DeltaObjectPin, String> {
    let pack_path = if let Some(pack_path) = installed_pack {
        pack_path.to_path_buf()
    } else {
        let output = crate::env::git()
            .args(["rev-parse", "--git-path", "objects"])
            .current_dir(repository)
            .output()
            .map_err(|error| format!("resolve delta pin object database: {error}"))?;
        if !output.status.success() {
            return Err(format!(
                "resolve delta pin object database: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
        let objects = PathBuf::from(String::from_utf8_lossy(&output.stdout).trim());
        let objects = if objects.is_absolute() {
            objects
        } else {
            repository.join(objects)
        };
        let prefix = objects.join("pack").join("cairn-delta-pin");
        std::fs::create_dir_all(prefix.parent().expect("pack prefix has parent"))
            .map_err(|error| format!("create delta pin pack directory: {error}"))?;
        let mut child = crate::env::git()
            .args(["pack-objects", "--revs"])
            .arg(&prefix)
            .current_dir(repository)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| format!("start delta pin pack construction: {error}"))?;
        write!(
            child.stdin.take().expect("piped stdin"),
            "{delta_commit}\n^{base_commit}\n"
        )
        .map_err(|error| format!("write delta pin revision set: {error}"))?;
        let output = child
            .wait_with_output()
            .map_err(|error| format!("wait for delta pin pack construction: {error}"))?;
        if !output.status.success() {
            return Err(format!(
                "construct delta pin pack: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
        let checksum = String::from_utf8_lossy(&output.stdout).trim().to_string();
        prefix.with_file_name(format!("cairn-delta-pin-{checksum}.pack"))
    };
    let keep_path = pack_path.with_extension("keep");
    let owned = match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&keep_path)
    {
        Ok(mut file) => {
            if let Err(error) = file.write_all(b"Cairn validated delta publication\n") {
                let _ = std::fs::remove_file(&keep_path);
                return Err(format!(
                    "write delta object pin {}: {error}",
                    keep_path.display()
                ));
            }
            true
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => false,
        Err(error) => {
            return Err(format!(
                "create delta object pin {}: {error}",
                keep_path.display()
            ))
        }
    };
    Ok(DeltaObjectPin { keep_path, owned })
}
