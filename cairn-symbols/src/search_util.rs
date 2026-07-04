//! Small, dependency-light path/glob/format helpers shared by the two search
//! engines this crate holds — the structural (`symbols`) walk and the warm fff
//! (`worktree_search`) index — and re-exported back into cairn-core so its
//! ripgrep grep handler formats and filters identically. One canonical copy,
//! never a fork: the warm-vs-cold parity the whole design rests on depends on
//! these being the same functions.

use std::path::{Path, PathBuf};

/// Whether `full_path` resolves into any of `dirs` (i.e. equals or is nested
/// under one). Resolves by canonicalizing, or falling back to the nearest
/// existing ancestor, so a not-yet-existing path is judged by where it would
/// live. Used both for the read denylist (gate reads of credential stores/keys)
/// and the write writable-extra set (temp/toolchain writes are in-sandbox).
pub fn path_within_any(full_path: &Path, dirs: &[PathBuf]) -> bool {
    if dirs.is_empty() {
        return false;
    }
    let resolved = if full_path.exists() {
        full_path.canonicalize().ok()
    } else {
        full_path
            .ancestors()
            .find(|ancestor| ancestor.exists())
            .and_then(|ancestor| ancestor.canonicalize().ok())
    };
    let Some(resolved) = resolved else {
        return false;
    };
    dirs.iter().any(|dir| {
        let dir_canon = dir.canonicalize().unwrap_or_else(|_| dir.clone());
        resolved.starts_with(&dir_canon)
    })
}

/// Compile a single glob pattern into a matcher. `literal_separator(false)` lets
/// `*` cross path separators, matching the walk semantics both the ripgrep grep
/// handler and the warm index rely on.
pub fn build_glob_matcher(pattern: &str) -> Result<globset::GlobSet, String> {
    let glob = globset::GlobBuilder::new(pattern)
        .literal_separator(false)
        .build()
        .map_err(|e| format!("Invalid glob pattern '{}': {}", pattern, e))?;
    globset::GlobSetBuilder::new()
        .add(glob)
        .build()
        .map_err(|e| format!("Failed to build glob matcher: {}", e))
}

/// Format one grep content line. With a non-empty `relative` label this is the
/// directory/single-file form `path:N:text` (match) / `path:N-text` (context);
/// with an empty label (a materialized in-memory body — a rendered resource,
/// web markdown, artifact, or transcript) the path prefix is dropped entirely,
/// yielding `N:text` / `N-text`. `sep` is `':'` for a match line and `'-'` for a
/// context line; both forms collapse to bare `text` when line numbers are off.
pub fn format_grep_line(relative: &str, line_number: Option<u64>, sep: char, text: &str) -> String {
    match (relative.is_empty(), line_number) {
        (true, Some(n)) => format!("{}{}{}", n, sep, text),
        (true, None) => text.to_string(),
        (false, Some(n)) => format!("{}:{}{}{}", relative, n, sep, text),
        (false, None) => format!("{}{}{}", relative, sep, text),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn path_within_any_matches_only_listed_subtrees() {
        let temp = tempdir().unwrap();
        let secrets = temp.path().join("secrets");
        std::fs::create_dir_all(&secrets).unwrap();
        std::fs::write(secrets.join("creds"), "x").unwrap();
        let public = temp.path().join("public.txt");
        std::fs::write(&public, "y").unwrap();

        let dirs = vec![secrets.clone()];
        // A file inside a listed subtree matches.
        assert!(path_within_any(&secrets.join("creds"), &dirs));
        // A file outside any listed subtree does not.
        assert!(!path_within_any(&public, &dirs));
        // A not-yet-existing file under a listed dir still matches (via ancestor).
        assert!(path_within_any(&secrets.join("new/deep"), &dirs));
        // An empty list never matches.
        assert!(!path_within_any(&public, &[]));
    }
}
