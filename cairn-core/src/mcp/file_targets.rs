//! File target resolution and read-error helpers for MCP handlers.

use std::path::{Component, Path, PathBuf};

/// The `file:` URI scheme prefix for worktree and global filesystem targets.
pub const FILE_URI_SCHEME: &str = "file:";

/// Cap on "did you mean" path suggestions surfaced for a missing `file:` target.
const MAX_PATH_SUGGESTIONS: usize = 5;

/// Cap on candidates collected before ranking, bounding work for a basename
/// (e.g. `mod.rs`) that recurs throughout the tree.
const MAX_PATH_CANDIDATES: usize = 64;

/// Safety net so a suggestion walk on a huge tree can't stall an error path.
const SUGGEST_WALK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedFileTarget {
    pub uri: String,
    pub relative_path: String,
    pub full_path: PathBuf,
}

/// Classification of a `file:` target under shell-style path rules.
///
/// The discriminator is the shell rule: a leading `/` after the scheme means
/// absolute; anything else is worktree-relative. Bare `file:` is the worktree
/// root. There are no URI-authority / triple-slash semantics.
enum FileTargetKind {
    /// Bare `file:` — the worktree root.
    Root,
    /// Worktree-relative path. May contain `..`; escape enforcement is
    /// per-operation (`read` permits escapes, `write` rejects them).
    Relative(String),
    /// Absolute path (leading `/` after the scheme).
    Absolute(PathBuf),
}

fn invalid_file_target_message(target: &str) -> String {
    format!(
        "Invalid file target '{target}': expected file: (worktree root), file:relative/path, or file:/absolute/path"
    )
}

fn worktree_only_message(target: &str) -> String {
    format!("change is worktree-only; use a relative path like file:src/x (got '{target}')")
}

fn legacy_tilde_message(target: &str) -> String {
    format!(
        "the file:~ worktree convention was removed; use bare file: for the worktree root or file:<relative/path> for a worktree file (got '{target}')"
    )
}

/// Normalize the worktree-relative portion of a `file:` target.
///
/// Drops `.` and empty segments and preserves `..` — escape enforcement is the
/// caller's job, per operation.
fn normalize_relative_components(path: &str) -> String {
    let mut parts: Vec<String> = Vec::new();
    for component in Path::new(path).components() {
        match component {
            Component::Normal(part) => parts.push(part.to_string_lossy().to_string()),
            Component::ParentDir => parts.push("..".to_string()),
            Component::CurDir | Component::RootDir | Component::Prefix(_) => {}
        }
    }
    parts.join("/")
}

fn classify_file_target(target: &str) -> Result<FileTargetKind, String> {
    let rest = target
        .strip_prefix(FILE_URI_SCHEME)
        .ok_or_else(|| invalid_file_target_message(target))?;
    if rest.is_empty() {
        return Ok(FileTargetKind::Root);
    }
    if rest.starts_with('/') {
        return Ok(FileTargetKind::Absolute(PathBuf::from(rest)));
    }
    // Hard cut: the old `file:~`/`file:~/...` worktree convention is gone.
    if rest == "~" || rest.starts_with("~/") {
        return Err(legacy_tilde_message(target));
    }
    let normalized = normalize_relative_components(rest);
    if normalized.is_empty() {
        Ok(FileTargetKind::Root)
    } else {
        Ok(FileTargetKind::Relative(normalized))
    }
}

/// Normalize a `file:` target to its canonical URI string.
///
/// Worktree-relative only: rejects absolute targets, since the only caller
/// (`write`) is worktree-jailed.
pub fn normalize_file_uri(target: &str) -> Result<String, String> {
    match classify_file_target(target)? {
        FileTargetKind::Root => Ok(FILE_URI_SCHEME.to_string()),
        FileTargetKind::Relative(rel) => Ok(format!("{FILE_URI_SCHEME}{rel}")),
        FileTargetKind::Absolute(_) => Err(worktree_only_message(target)),
    }
}

/// Resolve a `file:` target to a concrete path.
///
/// `allow_absolute` co-varies with the jail: when true (read), absolute targets
/// are permitted and the worktree-escape check is skipped (global reads,
/// including `..`, are allowed); when false (change), absolute targets are
/// rejected and the resolved path must stay within the worktree root.
fn resolve_file_target_internal(
    worktree_path: &Path,
    target: &str,
    create_missing_dirs: bool,
    require_exists: bool,
    allow_absolute: bool,
) -> Result<ResolvedFileTarget, String> {
    let (uri, relative_path, joined_path) = match classify_file_target(target)? {
        FileTargetKind::Root => (
            FILE_URI_SCHEME.to_string(),
            String::new(),
            worktree_path.to_path_buf(),
        ),
        FileTargetKind::Relative(rel) => {
            let uri = format!("{FILE_URI_SCHEME}{rel}");
            let joined = worktree_path.join(&rel);
            (uri, rel, joined)
        }
        FileTargetKind::Absolute(abs) => {
            if !allow_absolute {
                return Err(worktree_only_message(target));
            }
            let uri = format!("{FILE_URI_SCHEME}{}", abs.display());
            (uri, abs.display().to_string(), abs)
        }
    };

    let worktree_canonical = worktree_path
        .canonicalize()
        .map_err(|e| format!("Failed to resolve worktree path: {e}"))?;

    if require_exists && !joined_path.exists() {
        return Err(format!(
            "Entered path does not exist: {uri}{}",
            did_you_mean_block(worktree_path, &uri)
        ));
    }

    let full_path = if joined_path.exists() {
        let canonical = joined_path
            .canonicalize()
            .map_err(|e| format!("Failed to resolve path: {e}"))?;

        if !allow_absolute && !canonical.starts_with(&worktree_canonical) {
            return Err(format!("Path escapes the worktree root: {uri}"));
        }

        canonical
    } else {
        let parent = joined_path
            .parent()
            .ok_or_else(|| "Invalid file path: no parent directory".to_string())?;

        if create_missing_dirs && !parent.exists() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("Failed to create parent directories: {e}"))?;
        }

        let existing_ancestor = parent
            .ancestors()
            .find(|ancestor| ancestor.exists())
            .ok_or_else(|| "No existing ancestor directory found".to_string())?;

        let canonical_ancestor = existing_ancestor
            .canonicalize()
            .map_err(|e| format!("Failed to resolve ancestor path: {e}"))?;

        if !allow_absolute && !canonical_ancestor.starts_with(&worktree_canonical) {
            return Err(format!("Path escapes the worktree root: {uri}"));
        }

        joined_path
    };

    Ok(ResolvedFileTarget {
        uri,
        relative_path,
        full_path,
    })
}

/// Search the worktree for files sharing the missing target's basename and
/// return up to [`MAX_PATH_SUGGESTIONS`] `file:`-prefixed relative paths,
/// best-effort. Handles the common "right filename, wrong directory" typo by
/// ranking paths that are a suffix of the entered path first, then preferring
/// shallower paths. Walks with the same `.gitignore`-aware walker the glob/grep
/// handlers use, so heavy build dirs (`target/`, `node_modules/`) are skipped.
/// Returns empty on any error, when the basename can't be determined, or when
/// nothing matches.
pub fn suggest_similar_paths(worktree_path: &Path, missing_uri: &str) -> Vec<String> {
    let entered = missing_uri
        .strip_prefix(FILE_URI_SCHEME)
        .unwrap_or(missing_uri);
    let target_name = match Path::new(entered)
        .file_name()
        .and_then(|name| name.to_str())
    {
        Some(name) if !name.is_empty() => name.to_string(),
        _ => return Vec::new(),
    };

    let walker = ignore::WalkBuilder::new(worktree_path)
        .hidden(false)
        .git_ignore(true)
        .git_global(true)
        .git_exclude(true)
        .build();

    let deadline = std::time::Instant::now() + SUGGEST_WALK_TIMEOUT;
    let mut candidates: Vec<String> = Vec::new();
    for entry in walker {
        if std::time::Instant::now() > deadline {
            break;
        }
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => continue,
        };
        if entry.file_type().is_none_or(|ft| ft.is_dir()) {
            continue;
        }
        if entry.file_name().to_str() != Some(target_name.as_str()) {
            continue;
        }
        let path = entry.path();
        let relative = path.strip_prefix(worktree_path).unwrap_or(path);
        candidates.push(relative.to_string_lossy().replace('\\', "/"));
        if candidates.len() >= MAX_PATH_CANDIDATES {
            break;
        }
    }

    // Rank: a candidate that is a path-suffix of the entered path is the
    // "missing a directory prefix" case and almost always the intended file, so
    // surface those first; then prefer shallower, shorter paths.
    candidates.sort_by_key(|rel| {
        let is_suffix = rel == entered || rel.ends_with(&format!("/{entered}"));
        (!is_suffix, rel.matches('/').count(), rel.len())
    });
    candidates.truncate(MAX_PATH_SUGGESTIONS);
    candidates
        .into_iter()
        .map(|rel| format!("{FILE_URI_SCHEME}{rel}"))
        .collect()
}

/// Format a "Did you mean:" block (leading newline included) for a missing
/// `file:` target, or an empty string when there are no close matches. Callers
/// append it directly to a "does not exist" error message.
pub fn did_you_mean_block(worktree_path: &Path, missing_uri: &str) -> String {
    let suggestions = suggest_similar_paths(worktree_path, missing_uri);
    if suggestions.is_empty() {
        return String::new();
    }
    let mut block = String::from("\nDid you mean:");
    for suggestion in suggestions {
        block.push_str("\n  ");
        block.push_str(&suggestion);
    }
    block
}

/// Validate that a file URI stays within the worktree (no path traversal).
pub fn validate_file_path(
    worktree_path: &Path,
    file_uri: &str,
) -> Result<ResolvedFileTarget, String> {
    resolve_file_target_internal(worktree_path, file_uri, true, false, false)
}

/// Validate that a file path stays within the worktree WITHOUT creating directories.
///
/// Like `validate_file_path` but non-mutating: for new files in missing directories,
/// walks up to find the nearest existing ancestor and verifies it's inside the worktree.
/// Used by write Phase 1 validation to avoid side effects before all changes are validated.
pub fn validate_file_path_dry(worktree_path: &Path, file_uri: &str) -> Result<PathBuf, String> {
    resolve_file_target_internal(worktree_path, file_uri, false, false, false)
        .map(|target| target.full_path)
}

/// Validate file URI for read operations.
pub fn validate_read_path(
    worktree_path: &Path,
    file_uri: &str,
) -> Result<ResolvedFileTarget, String> {
    resolve_file_target_internal(worktree_path, file_uri, false, true, true)
}

/// Resolve a file URI for a read operation without requiring the target to exist.
///
/// Branch-scoped reads serve bytes from a VCS object rather than the current
/// working tree, so the current checkout cannot be the existence oracle. This
/// keeps the same URI normalization and absolute-path rules as `validate_read_path`
/// while deferring existence/type checks to the requested ref.
pub fn resolve_read_target_lenient(
    worktree_path: &Path,
    file_uri: &str,
) -> Result<ResolvedFileTarget, String> {
    resolve_file_target_internal(worktree_path, file_uri, false, false, true)
}

/// True if `full_path` resolves outside `worktree_path`. For a not-yet-existing
/// path the nearest existing ancestor is checked, matching how new-file writes
/// are jailed. Permissive on resolution failure: returns false (no fence) when
/// neither the worktree nor any ancestor can be canonicalized.
///
/// This is the single prefix-comparison the worktree fence uses for reads and
/// writes, so the escape rule lives in one place.
pub fn path_escapes_worktree(worktree_path: &Path, full_path: &Path) -> bool {
    let Ok(worktree_canonical) = worktree_path.canonicalize() else {
        return false;
    };
    let resolved = if full_path.exists() {
        full_path.canonicalize().ok()
    } else {
        full_path
            .ancestors()
            .find(|ancestor| ancestor.exists())
            .and_then(|ancestor| ancestor.canonicalize().ok())
    };
    match resolved {
        Some(path) => !path.starts_with(&worktree_canonical),
        None => false,
    }
}

/// Whether a path resolves into any of a set of directories. The canonical
/// implementation now lives in [`cairn_symbols::search_util`] so the fff
/// worktree index and this crate's read/write fence checks share one copy;
/// re-exported here so existing `crate::mcp::file_targets::path_within_any`
/// call sites keep resolving.
pub use cairn_symbols::search_util::path_within_any;

/// Resolve a `file:` read target to an absolute path **without** requiring it to
/// exist. Lets the read denylist gate run before existence validation, so a
/// denylisted path is denied uniformly whether or not it exists (no
/// deny-vs-"does not exist" existence enumeration).
pub fn resolve_file_path_lenient(worktree_path: &Path, target: &str) -> Result<PathBuf, String> {
    resolve_file_target_internal(worktree_path, target, false, false, true).map(|t| t.full_path)
}

/// Normalize a `file:` change target, permitting absolute paths when
/// `allow_escape` (the worktree fence adjudicates the crossing). With
/// `allow_escape` false this is exactly [`normalize_file_uri`] (worktree-only).
pub fn normalize_change_target(target: &str, allow_escape: bool) -> Result<String, String> {
    match classify_file_target(target)? {
        FileTargetKind::Root => Ok(FILE_URI_SCHEME.to_string()),
        FileTargetKind::Relative(rel) => Ok(format!("{FILE_URI_SCHEME}{rel}")),
        FileTargetKind::Absolute(abs) => {
            if allow_escape {
                Ok(format!("{FILE_URI_SCHEME}{}", abs.display()))
            } else {
                Err(worktree_only_message(target))
            }
        }
    }
}

/// Resolve a `file:` change target to a concrete path, permitting absolute and
/// escaping targets when `allow_escape`. Used by `write` so the worktree fence
/// can adjudicate an out-of-worktree write instead of the resolver hard-
/// rejecting it. Never creates directories or requires existence (the caller
/// checks existence per mutation mode).
pub fn resolve_change_target(
    worktree_path: &Path,
    file_uri: &str,
    allow_escape: bool,
) -> Result<PathBuf, String> {
    resolve_file_target_internal(worktree_path, file_uri, false, false, allow_escape)
        .map(|target| target.full_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_normalize_file_uri_root_and_relative() {
        assert_eq!(normalize_file_uri("file:").unwrap(), "file:");
        assert_eq!(
            normalize_file_uri("file:src/lib/mod.rs").unwrap(),
            "file:src/lib/mod.rs"
        );
        // A leading "./" normalizes away.
        assert_eq!(
            normalize_file_uri("file:./src/lib.rs").unwrap(),
            "file:src/lib.rs"
        );
    }

    #[test]
    fn test_normalize_file_uri_rejects_absolute_and_non_scheme() {
        // change is worktree-only: absolute targets are rejected.
        assert!(normalize_file_uri("file:/tmp/lib.rs").is_err());
        // Bare paths without the scheme are not file targets.
        assert!(normalize_file_uri("src/lib.rs").is_err());
        assert!(normalize_file_uri("/tmp/lib.rs").is_err());
    }

    #[test]
    fn test_legacy_tilde_targets_are_rejected() {
        for legacy in ["file:~", "file:~/", "file:~/src/lib.rs"] {
            let err = normalize_file_uri(legacy).unwrap_err();
            assert!(
                err.contains("file:~ worktree convention was removed"),
                "expected hard-cut message for {legacy}, got: {err}"
            );
        }
        let temp = tempdir().unwrap();
        let err = validate_read_path(temp.path(), "file:~/Cargo.toml").unwrap_err();
        assert!(err.contains("file:~ worktree convention was removed"));
    }

    #[test]
    fn path_escapes_worktree_detects_inside_and_outside() {
        let temp = tempdir().unwrap();
        let worktree = temp.path().join("wt");
        std::fs::create_dir_all(worktree.join("src")).unwrap();
        std::fs::write(worktree.join("src/lib.rs"), "x").unwrap();
        std::fs::write(temp.path().join("outside.txt"), "y").unwrap();

        // Existing in-worktree paths (incl. nested) and the root do not escape.
        assert!(!path_escapes_worktree(&worktree, &worktree));
        assert!(!path_escapes_worktree(
            &worktree,
            &worktree.join("src/lib.rs")
        ));
        // Existing paths outside the worktree escape.
        assert!(path_escapes_worktree(
            &worktree,
            &temp.path().join("outside.txt")
        ));
        assert!(path_escapes_worktree(&worktree, Path::new("/etc")));
    }

    #[test]
    fn path_escapes_worktree_handles_nonexistent_via_ancestor() {
        let temp = tempdir().unwrap();
        let worktree = temp.path().join("wt");
        std::fs::create_dir_all(worktree.join("src")).unwrap();

        // A not-yet-existing file resolves via its nearest existing ancestor:
        // an in-worktree ancestor means the new file is in-worktree.
        assert!(!path_escapes_worktree(
            &worktree,
            &worktree.join("src/new.rs")
        ));
        assert!(!path_escapes_worktree(
            &worktree,
            &worktree.join("brand/new/deep.rs")
        ));
        // A not-yet-existing file whose nearest existing ancestor is outside the
        // worktree escapes.
        assert!(path_escapes_worktree(
            &worktree,
            &temp.path().join("sibling/new.rs")
        ));
    }

    #[test]
    fn test_validate_read_path_resolves_root() {
        let temp = tempdir().unwrap();
        let worktree = temp.path();

        let root = validate_read_path(worktree, "file:").unwrap();
        assert_eq!(root.uri, "file:");
        assert_eq!(root.relative_path, "");
        assert_eq!(root.full_path, worktree.canonicalize().unwrap());
    }

    #[test]
    fn test_validate_read_path_resolves_relative() {
        let temp = tempdir().unwrap();
        let worktree = temp.path();
        std::fs::create_dir_all(worktree.join("src/lib")).unwrap();
        std::fs::write(worktree.join("src/lib/mod.rs"), "content").unwrap();

        let result = validate_read_path(worktree, "file:src/lib/mod.rs").unwrap();
        assert_eq!(result.uri, "file:src/lib/mod.rs");
        assert_eq!(result.relative_path, "src/lib/mod.rs");
        assert!(result.full_path.ends_with("src/lib/mod.rs"));
    }

    #[test]
    fn test_validate_read_path_resolves_absolute_global() {
        let temp = tempdir().unwrap();
        let worktree = temp.path();
        // A global file outside the worktree resolves for read.
        let outside = tempdir().unwrap();
        let global = outside.path().join("global.txt");
        std::fs::write(&global, "global").unwrap();

        let uri = format!("file:{}", global.display());
        let result = validate_read_path(worktree, &uri).unwrap();
        assert_eq!(result.full_path, global.canonicalize().unwrap());
    }

    #[test]
    fn test_validate_read_path_allows_parent_escape() {
        let temp = tempdir().unwrap();
        let worktree = temp.path().join("wt");
        std::fs::create_dir_all(&worktree).unwrap();
        std::fs::write(temp.path().join("outside.txt"), "content").unwrap();

        // read permits ".." escapes (global reach).
        let result = validate_read_path(&worktree, "file:../outside.txt").unwrap();
        assert!(result.full_path.ends_with("outside.txt"));
    }

    #[test]
    fn test_validate_read_path_rejects_nonexistent() {
        let temp = tempdir().unwrap();
        let worktree = temp.path();

        let result = validate_read_path(worktree, "file:missing.txt");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("does not exist"));
    }

    #[test]
    fn test_suggest_similar_paths_finds_prefixed_match() {
        let temp = tempdir().unwrap();
        let worktree = temp.path();
        std::fs::create_dir_all(worktree.join("src-tauri/turso_migrations")).unwrap();
        std::fs::write(
            worktree.join("src-tauri/turso_migrations/0007_add_uri.sql"),
            "sql",
        )
        .unwrap();

        // Entered with the directory prefix missing — the deeper file is the
        // intended one and should be the top suggestion.
        let suggestions = suggest_similar_paths(worktree, "file:turso_migrations/0007_add_uri.sql");
        assert_eq!(
            suggestions,
            vec!["file:src-tauri/turso_migrations/0007_add_uri.sql".to_string()]
        );
    }

    #[test]
    fn test_suggest_similar_paths_ranks_suffix_match_first() {
        let temp = tempdir().unwrap();
        let worktree = temp.path();
        std::fs::create_dir_all(worktree.join("a/turso_migrations")).unwrap();
        std::fs::create_dir_all(worktree.join("unrelated")).unwrap();
        // A suffix match (right tail, wrong prefix) and an unrelated same-name file.
        std::fs::write(worktree.join("a/turso_migrations/0007.sql"), "x").unwrap();
        std::fs::write(worktree.join("unrelated/0007.sql"), "x").unwrap();

        let suggestions = suggest_similar_paths(worktree, "file:turso_migrations/0007.sql");
        assert_eq!(
            suggestions.first().map(String::as_str),
            Some("file:a/turso_migrations/0007.sql"),
            "the path-suffix match should rank first, got: {suggestions:?}"
        );
        assert!(suggestions.contains(&"file:unrelated/0007.sql".to_string()));
    }

    #[test]
    fn test_suggest_similar_paths_returns_empty_when_no_basename_match() {
        let temp = tempdir().unwrap();
        let worktree = temp.path();
        std::fs::write(worktree.join("present.txt"), "x").unwrap();

        assert!(suggest_similar_paths(worktree, "file:absent.txt").is_empty());
    }

    #[test]
    fn test_validate_read_path_nonexistent_suggests_correct_path() {
        let temp = tempdir().unwrap();
        let worktree = temp.path();
        std::fs::create_dir_all(worktree.join("src-tauri/turso_migrations")).unwrap();
        std::fs::write(
            worktree.join("src-tauri/turso_migrations/0007_add_uri.sql"),
            "sql",
        )
        .unwrap();

        let err =
            validate_read_path(worktree, "file:turso_migrations/0007_add_uri.sql").unwrap_err();
        assert!(err.contains("Entered path does not exist"), "got: {err}");
        assert!(err.contains("Did you mean:"), "got: {err}");
        assert!(
            err.contains("file:src-tauri/turso_migrations/0007_add_uri.sql"),
            "got: {err}"
        );
    }

    #[test]
    fn test_validate_file_path_returns_canonical_uri_and_relative_path() {
        let temp = tempdir().unwrap();
        let worktree = temp.path();
        std::fs::create_dir_all(worktree.join("src/lib")).unwrap();
        std::fs::write(worktree.join("src/lib/mod.rs"), "content").unwrap();

        let result = validate_file_path(worktree, "file:src/lib/mod.rs").unwrap();
        assert_eq!(result.uri, "file:src/lib/mod.rs");
        assert_eq!(result.relative_path, "src/lib/mod.rs");
        assert!(result.full_path.ends_with("src/lib/mod.rs"));
    }

    #[test]
    fn test_validate_file_path_creates_parent_dirs_for_new_file() {
        let temp = tempdir().unwrap();
        let worktree = temp.path();

        let result = validate_file_path(worktree, "file:new/nested/dir/file.txt").unwrap();
        assert_eq!(result.uri, "file:new/nested/dir/file.txt");
        assert_eq!(result.relative_path, "new/nested/dir/file.txt");
        assert!(worktree.join("new/nested/dir").exists());
    }

    #[test]
    fn test_validate_file_path_blocks_relative_traversal() {
        let temp = tempdir().unwrap();
        let worktree = temp.path();

        let result = validate_file_path(worktree, "file:src/../../outside.txt");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("worktree"));
    }

    #[test]
    fn test_validate_file_path_rejects_absolute() {
        let temp = tempdir().unwrap();
        let worktree = temp.path();

        let result = validate_file_path(worktree, "file:/tmp/outside.txt");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("worktree-only"));
    }
}
