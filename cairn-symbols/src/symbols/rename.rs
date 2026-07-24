//! Structural rename: compute the set of edit sites for a symbol across the
//! worktree and produce a [`RenamePlan`] the change machinery applies and
//! commits.
//!
//! This is the structural replacement for the LSP rename. There is no semantic
//! resolution: a rename matches every identifier with the target name across the
//! worktree (or, for a position-anchored local binding, within the enclosing
//! function). Safety is preview-by-default — the change preview shows every edit
//! site before anything is written, and `symbol_at` narrows scope when a name is
//! shadowed. The only hard stop is a genuine miss (no occurrence found). Over-
//! match across distinct same-name symbols is surfaced in the preview for the
//! reviewer, never refused.

use std::ops::Range;
use std::path::{Path, PathBuf};

use ast_grep_language::SupportLang;

use super::engine::{lang_for_path, parse, SymbolNode};
use super::walk::source_files;

/// How the symbol to rename is located.
pub enum RenameSpec {
    /// A bare name: rename every occurrence across the worktree.
    Name(String),
    /// A document position (0-based line, column): resolve the identifier there,
    /// then rename worktree-wide — or within the enclosing function when the
    /// name is a local binding.
    At(PathBuf, (u32, u32)),
}

/// Compute the same rename plan from an immutable logical-head snapshot rather
/// than a materialized worktree. Paths must be absolute so returned edits retain
/// the existing `RenamePlan` contract.
pub fn compute_plan_from_files(
    files: &[(PathBuf, String)],
    spec: RenameSpec,
    new_name: &str,
) -> Result<RenamePlan, String> {
    let (name, scope) = match spec {
        RenameSpec::Name(name) => (name, Scope::Worktree),
        RenameSpec::At(path, position) => {
            let lang = lang_for_path(&path)
                .ok_or_else(|| format!("no bundled grammar for {}", path.display()))?;
            let src = files
                .iter()
                .find(|(candidate, _)| candidate == &path)
                .map(|(_, content)| content.as_str())
                .ok_or_else(|| format!("failed to read {} from logical head", path.display()))?;
            let offset = byte_offset(src, position.0 as usize, position.1 as usize);
            let ast = parse(src, lang);
            let node = identifier_at(&ast.root(), offset).ok_or_else(|| {
                format!(
                    "no identifier at {}:{}:{}",
                    path.display(),
                    position.0 + 1,
                    position.1 + 1
                )
            })?;
            let name = node.text().into_owned();
            let scope = if let Some(func) = enclosing_function(&node) {
                if function_binds_local(&func, &name) {
                    Scope::Function {
                        path,
                        lang,
                        range: func.range(),
                    }
                } else {
                    Scope::Worktree
                }
            } else {
                Scope::Worktree
            };
            (name, scope)
        }
    };
    if name.is_empty() {
        return Err("could not resolve a symbol to rename".to_string());
    }
    let mut file_edits = Vec::new();
    for (path, src) in files {
        let Some(lang) = lang_for_path(path) else {
            continue;
        };
        let within = match &scope {
            Scope::Worktree => None,
            Scope::Function {
                path: scoped_path,
                range,
                ..
            } if scoped_path == path => Some(range.clone()),
            Scope::Function { .. } => continue,
        };
        if let Some(edit) = rename_in_content(path, src, lang, &name, new_name, within) {
            file_edits.push(edit);
        }
    }
    if file_edits.is_empty() {
        return Err(format!("no symbol named `{name}` found to rename"));
    }
    file_edits.sort_by(|a, b| a.worktree_path.cmp(&b.worktree_path));
    Ok(RenamePlan { file_edits })
}

/// One file's resulting state after applying a rename's edits. Mirrors the shape
/// the change machinery (`prepare_rename_changes`) consumes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileEdit {
    /// Worktree-absolute path of the file the edit reads from.
    pub worktree_path: PathBuf,
    /// The full post-edit content to write. `None` means delete `worktree_path`.
    pub new_content: Option<String>,
    /// Destination path when the symbol's file is renamed/moved (unused by the
    /// structural engine today; kept for the plan/apply contract).
    pub move_to: Option<PathBuf>,
    /// Number of individual edit sites applied to this file.
    pub site_count: usize,
}

/// The computed set of file edits a rename will apply.
#[derive(Debug)]
pub struct RenamePlan {
    pub file_edits: Vec<FileEdit>,
}

enum Scope {
    /// Every file in the worktree.
    Worktree,
    /// A single function subtree in one file (local-binding rename).
    Function {
        path: PathBuf,
        lang: SupportLang,
        range: Range<usize>,
    },
}

/// Compute a structural rename plan: rename the symbol identified by `spec` to
/// `new_name`. `file` is the route file (the rename item's target).
pub fn compute_plan(
    worktree: &Path,
    file: &Path,
    spec: RenameSpec,
    new_name: &str,
) -> Result<RenamePlan, String> {
    let _ = file;
    let (name, scope) = resolve(spec)?;
    if name.is_empty() {
        return Err("could not resolve a symbol to rename".to_string());
    }

    let mut file_edits = Vec::new();
    match scope {
        Scope::Worktree => {
            for (path, lang) in source_files(worktree, worktree, None) {
                if let Some(edit) = rename_in_file(&path, lang, &name, new_name, None) {
                    file_edits.push(edit);
                }
            }
        }
        Scope::Function { path, lang, range } => {
            if let Some(edit) = rename_in_file(&path, lang, &name, new_name, Some(range)) {
                file_edits.push(edit);
            }
        }
    }
    if file_edits.is_empty() {
        return Err(format!("no symbol named `{name}` found to rename"));
    }
    file_edits.sort_by(|a, b| a.worktree_path.cmp(&b.worktree_path));
    Ok(RenamePlan { file_edits })
}

fn resolve(spec: RenameSpec) -> Result<(String, Scope), String> {
    match spec {
        RenameSpec::Name(name) => Ok((name, Scope::Worktree)),
        RenameSpec::At(path, position) => {
            let lang = lang_for_path(&path)
                .ok_or_else(|| format!("no bundled grammar for {}", path.display()))?;
            let src = std::fs::read_to_string(&path)
                .map_err(|err| format!("failed to read {}: {err}", path.display()))?;
            let offset = byte_offset(&src, position.0 as usize, position.1 as usize);
            let ast = parse(&src, lang);
            let node = identifier_at(&ast.root(), offset).ok_or_else(|| {
                format!(
                    "no identifier at {}:{}:{}",
                    path.display(),
                    position.0 + 1,
                    position.1 + 1
                )
            })?;
            let name = node.text().into_owned();
            // Narrow to the enclosing function when it binds `name` locally, so a
            // shadowed local rename does not spill across the worktree.
            if let Some(func) = enclosing_function(&node) {
                if function_binds_local(&func, &name) {
                    return Ok((
                        name,
                        Scope::Function {
                            path,
                            lang,
                            range: func.range(),
                        },
                    ));
                }
            }
            Ok((name, Scope::Worktree))
        }
    }
}

fn rename_in_file(
    path: &Path,
    lang: SupportLang,
    name: &str,
    new_name: &str,
    within: Option<Range<usize>>,
) -> Option<FileEdit> {
    let src = std::fs::read_to_string(path).ok()?;
    rename_in_content(path, &src, lang, name, new_name, within)
}

fn rename_in_content(
    path: &Path,
    src: &str,
    lang: SupportLang,
    name: &str,
    new_name: &str,
    within: Option<Range<usize>>,
) -> Option<FileEdit> {
    let ast = parse(src, lang);
    let mut sites: Vec<Range<usize>> = Vec::new();
    for node in ast.root().dfs() {
        if node.kind().ends_with("identifier") && node.text().as_ref() == name {
            let range = node.range();
            if let Some(scope) = &within {
                if range.start < scope.start || range.end > scope.end {
                    continue;
                }
            }
            sites.push(range);
        }
    }
    if sites.is_empty() {
        return None;
    }
    // Apply right-to-left so earlier byte ranges stay valid as the string shifts.
    sites.sort_by(|a, b| b.start.cmp(&a.start));
    let mut content = src.to_string();
    for site in &sites {
        content.replace_range(site.clone(), new_name);
    }
    Some(FileEdit {
        worktree_path: path.to_path_buf(),
        new_content: Some(content),
        move_to: None,
        site_count: sites.len(),
    })
}

/// Byte offset of a 0-based (line, column) position, treating column as a count
/// of characters into the line (byte-equal for ASCII identifiers).
fn byte_offset(src: &str, line: usize, col: usize) -> usize {
    let mut offset = 0;
    for (index, text) in src.split_inclusive('\n').enumerate() {
        if index == line {
            for (chars, (byte, _)) in text.char_indices().enumerate() {
                if chars == col {
                    return offset + byte;
                }
            }
            return offset + text.len();
        }
        offset += text.len();
    }
    offset
}

/// The tightest identifier node whose byte range contains `offset`.
fn identifier_at<'r>(root: &SymbolNode<'r>, offset: usize) -> Option<SymbolNode<'r>> {
    let mut best: Option<SymbolNode<'r>> = None;
    for node in root.dfs() {
        let range = node.range();
        if node.kind().ends_with("identifier") && range.start <= offset && offset < range.end {
            let span = range.end - range.start;
            if best
                .as_ref()
                .is_none_or(|b| span < (b.range().end - b.range().start))
            {
                best = Some(node);
            }
        }
    }
    best
}

const FUNCTION_KINDS: &[&str] = &[
    "function_item",
    "closure_expression",
    "function_declaration",
    "generator_function_declaration",
    "method_definition",
    "method_declaration",
    "arrow_function",
    "function_expression",
    "function_definition",
    "function",
];

fn enclosing_function<'r>(node: &SymbolNode<'r>) -> Option<SymbolNode<'r>> {
    node.ancestors()
        .find(|ancestor| FUNCTION_KINDS.contains(&ancestor.kind().as_ref()))
}

/// Whether `func` binds `name` as a parameter or local variable.
fn function_binds_local(func: &SymbolNode, name: &str) -> bool {
    for node in func.dfs() {
        let is_binding = matches!(
            node.kind().as_ref(),
            "parameter"
                | "typed_parameter"
                | "required_parameter"
                | "optional_parameter"
                | "closure_parameters"
                | "let_declaration"
                | "variable_declarator"
        );
        if is_binding && binds_name(&node, name) {
            return true;
        }
    }
    false
}

fn binds_name(node: &SymbolNode, name: &str) -> bool {
    let pattern = node.field("pattern").or_else(|| node.field("name"));
    let target = pattern.as_ref().unwrap_or(node);
    let bound = target.dfs().any(|descendant| {
        descendant.kind().ends_with("identifier") && descendant.text().as_ref() == name
    });
    bound
}

/// Parse a `?at=file:PATH:LINE[:COL]` / `symbol_at` target into an absolute path
/// and a 0-based position. 1-based line/column input (grep-style) maps to
/// 0-based. Shared between the `rename` change mode's `symbol_at` payload and the
/// `symbols` resource's `?at=` so position parsing has one implementation.
pub fn parse_at(raw: &str, worktree: &Path) -> Result<(PathBuf, (u32, u32)), String> {
    let body = raw.strip_prefix("file:").unwrap_or(raw);
    let parts: Vec<&str> = body.rsplitn(3, ':').collect();
    let (path_str, line_str, col_str) = match parts.as_slice() {
        [col, line, path] if is_num(col) && is_num(line) => (*path, *line, Some(*col)),
        [line, path] if is_num(line) => (*path, *line, None),
        _ => {
            return Err(format!(
                "invalid 'at' target '{raw}'; expected file:PATH:LINE[:COL]"
            ))
        }
    };
    let line: u32 = line_str
        .parse()
        .map_err(|_| format!("invalid line in 'at' target '{raw}'"))?;
    let col: u32 = match col_str {
        Some(col) => col
            .parse()
            .map_err(|_| format!("invalid column in 'at' target '{raw}'"))?,
        None => 1,
    };
    let position = (line.saturating_sub(1), col.saturating_sub(1));
    let path = if Path::new(path_str).is_absolute() {
        PathBuf::from(path_str)
    } else {
        worktree.join(path_str)
    };
    Ok((path, position))
}

fn is_num(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn write(dir: &Path, rel: &str, content: &str) {
        let path = dir.join(rel);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, content).unwrap();
    }

    #[test]
    fn renames_name_across_worktree() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "a.rs", "fn alpha() {}\n");
        write(dir.path(), "b.rs", "fn beta() { alpha(); alpha(); }\n");
        let plan = compute_plan(
            dir.path(),
            &dir.path().join("a.rs"),
            RenameSpec::Name("alpha".into()),
            "renamed",
        )
        .unwrap();
        assert_eq!(plan.file_edits.len(), 2);
        let total: usize = plan.file_edits.iter().map(|e| e.site_count).sum();
        assert_eq!(total, 3);
        for edit in &plan.file_edits {
            assert!(!edit.new_content.as_ref().unwrap().contains("alpha"));
            assert!(edit.new_content.as_ref().unwrap().contains("renamed"));
        }
    }

    #[test]
    fn miss_reports_error() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "a.rs", "fn alpha() {}\n");
        let err = compute_plan(
            dir.path(),
            &dir.path().join("a.rs"),
            RenameSpec::Name("nonexistent".into()),
            "x",
        )
        .unwrap_err();
        assert!(err.contains("no symbol named"));
    }

    #[test]
    fn symbol_at_local_binding_scopes_to_function() {
        let dir = tempfile::tempdir().unwrap();
        // `value` is a local in `first` and a separate local in `second`.
        let src = "fn first() {\n    let value = 1;\n    let _ = value;\n}\nfn second() {\n    let value = 2;\n}\n";
        write(dir.path(), "a.rs", src);
        // Position on `value` in `first` (line 2 col 9, 1-based).
        let (_, pos) = parse_at("file:a.rs:2:9", dir.path()).unwrap();
        let plan = compute_plan(
            dir.path(),
            &dir.path().join("a.rs"),
            RenameSpec::At(dir.path().join("a.rs"), pos),
            "count",
        )
        .unwrap();
        assert_eq!(plan.file_edits.len(), 1);
        let content = plan.file_edits[0].new_content.as_ref().unwrap();
        // Only `first`'s two `value` sites renamed; `second`'s `value` untouched.
        assert_eq!(plan.file_edits[0].site_count, 2);
        assert!(content.contains("let count = 1;"));
        assert!(content.contains("let _ = count;"));
        assert!(content.contains("let value = 2;"));
    }

    #[test]
    fn parse_at_maps_to_zero_based_position() {
        let worktree = Path::new("/wt");
        assert_eq!(
            parse_at("file:src/lib.rs:15:7", worktree).unwrap(),
            (PathBuf::from("/wt/src/lib.rs"), (14, 6))
        );
        assert_eq!(
            parse_at("file:src/lib.rs:15", worktree).unwrap(),
            (PathBuf::from("/wt/src/lib.rs"), (14, 0))
        );
        assert!(parse_at("garbage", worktree).is_err());
    }
}
