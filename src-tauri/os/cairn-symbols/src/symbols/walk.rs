//! Shared source-file walker for the structural read surface.
//!
//! Both `?ast=` search and symbol navigation walk a worktree the same way:
//! honor gitignore, apply an optional `?glob=`, and keep only files with a
//! bundled grammar. Centralizing it here keeps one walk implementation rather
//! than parallel copies in `search` and `nav`.

use std::path::{Path, PathBuf};

use ast_grep_language::SupportLang;
use globset::{Glob, GlobSet, GlobSetBuilder};
use ignore::WalkBuilder;

use super::engine::lang_for_path;

/// Compile a single `?glob=` pattern into a matcher, with a friendly error.
pub(crate) fn build_globset(glob: &str) -> Result<GlobSet, String> {
    let mut builder = GlobSetBuilder::new();
    builder.add(Glob::new(glob).map_err(|err| format!("invalid glob '{glob}': {err}"))?);
    builder
        .build()
        .map_err(|err| format!("invalid glob '{glob}': {err}"))
}

/// Relativize `path` against `root`, falling back to the full path.
pub(crate) fn relative<'a>(root: &Path, path: &'a Path) -> &'a Path {
    path.strip_prefix(root).unwrap_or(path)
}

/// Walk `dir` for source files with a bundled grammar, honoring gitignore and an
/// optional glob (matched against the path relative to `root` and the bare file
/// name). Results are sorted for deterministic output.
pub(crate) fn source_files(
    root: &Path,
    dir: &Path,
    globset: Option<&GlobSet>,
) -> Vec<(PathBuf, SupportLang)> {
    let mut out = Vec::new();
    for entry in WalkBuilder::new(dir).hidden(false).build().flatten() {
        if !entry.file_type().is_some_and(|t| t.is_file()) {
            continue;
        }
        let path = entry.path();
        if let Some(set) = globset {
            let rel = relative(root, path);
            let name_match = path
                .file_name()
                .is_some_and(|name| set.is_match(Path::new(name)));
            if !set.is_match(rel) && !name_match {
                continue;
            }
        }
        if let Some(lang) = lang_for_path(path) {
            out.push((path.to_path_buf(), lang));
        }
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}
