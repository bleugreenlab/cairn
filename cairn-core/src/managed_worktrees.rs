use std::path::{Component, Path, PathBuf};

/// The shared base for every Cairn-owned worktree. This is deliberately not
/// instance-scoped: production and development instances coordinate through the
/// same managed workspace directory.
pub(crate) fn base_dir() -> Option<PathBuf> {
    dirs::home_dir().map(|home| home.join(".cairn").join("worktrees"))
}

/// Validate the durable path of a Cairn-owned worktree.
///
/// Managed worktrees and their teardown tombstones are direct children of the
/// shared base. Requiring one normal child component rejects empty/root paths,
/// project checkouts, nested escape attempts (`..`), and the base directory
/// itself without depending on the target still existing for canonicalization.
pub(crate) fn validate_path(path: &Path) -> Result<(), String> {
    let base = base_dir()
        .ok_or_else(|| "could not resolve managed worktrees base dir (no home dir)".to_string())?;
    validate_path_under(path, &base)
}

pub(crate) fn validate_path_under(path: &Path, base: &Path) -> Result<(), String> {
    let invalid = || {
        format!(
            "refusing worktree path outside managed base {}: {}",
            base.display(),
            path.display()
        )
    };

    if !path.is_absolute() || !base.is_absolute() {
        return Err(invalid());
    }
    let relative = path.strip_prefix(base).map_err(|_| invalid())?;
    let mut components = relative.components();
    match (components.next(), components.next()) {
        (Some(Component::Normal(name)), None) if !name.is_empty() => Ok(()),
        _ => Err(invalid()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_direct_managed_child() {
        let base = Path::new("/Users/test/.cairn/worktrees");
        assert!(validate_path_under(&base.join("CAIRN-1-builder-0"), base).is_ok());
        assert!(validate_path_under(&base.join("CAIRN-1-builder-0.trash-1"), base).is_ok());
    }

    #[test]
    fn rejects_empty_root_base_nested_and_traversal_paths() {
        let base = Path::new("/Users/test/.cairn/worktrees");
        for path in [
            Path::new(""),
            Path::new("/"),
            base,
            Path::new("/Users/test/project"),
            Path::new("/Users/test/.cairn/worktrees/one/two"),
            Path::new("/Users/test/.cairn/worktrees/one/../two"),
        ] {
            assert!(
                validate_path_under(path, base).is_err(),
                "accepted {}",
                path.display()
            );
        }
    }
}
