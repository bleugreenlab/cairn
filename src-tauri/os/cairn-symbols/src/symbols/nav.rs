//! Symbol navigation as grammar-driven structural queries.
//!
//! These are the LSP read-surface replacement ops. They are *structural*, not
//! semantic: there is no cross-file name resolution or type inference, so a
//! query matches by name + node kind across the worktree. The trade is
//! deliberate — instant, zero-config, universal — and agents triangulate with
//! `grep` and the compiler for the cases that need semantic precision.
//!
//! - `definition` — declaration sites of a name (functions, types, etc.).
//! - `references` — every identifier occurrence of a name.
//! - `callers` — functions that call a name (incoming calls).
//! - `implementations` — `impl` blocks (Rust) / `implements`/`extends` clauses
//!   (TS) naming the symbol.
//! - overview (no op) — definition sites + a reference count.

use std::path::{Path, PathBuf};

use ast_grep_language::SupportLang;

use super::engine::{lang_for_path, parse, SymbolNode};
use super::grammar::{self, LangSpec};
use super::render::{render_locations, render_locations_with_context, LocationHit, Rendered};
use super::walk::{build_globset, relative, source_files};

/// Apply the row cap, reporting what it elided.
///
/// A truncated location list that says nothing is indistinguishable from a
/// complete one — the same defect `head_limit` carried on grep — and "every
/// caller of this symbol" is exactly the question these ops are asked before a
/// signature change. Returns `(shown, total)` when rows were dropped.
fn cap_locations(hits: &mut Vec<LocationHit>, limit: Option<usize>) -> Option<(usize, usize)> {
    let total = hits.len();
    let limit = limit?;
    if total <= limit {
        return None;
    }
    hits.truncate(limit);
    Some((limit, total))
}

/// Append the elision notice to a rendered location list. The symbols renderer
/// has no resource URI to build a continue from, so it names the total and the
/// knob that widens the window.
fn with_window_notice(mut rendered: Rendered, window: Option<(usize, usize)>) -> Rendered {
    let Some((shown, total)) = window else {
        return rendered;
    };
    rendered.body.push_str(&format!(
        "\n[locations 1\u{2013}{shown} of {total} — raise head_limit to see the rest]"
    ));
    rendered
}

/// The structural navigation operations the symbols resource exposes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SymbolOp {
    Definition,
    References,
    Callers,
    Implementations,
}

/// Run structural navigation over a store-native logical file set.
pub fn query_texts(
    files: &[(String, String)],
    op: Option<SymbolOp>,
    name: &str,
    glob: Option<&str>,
    proj: &NavProjection,
) -> Rendered {
    if name.trim().is_empty() {
        return Rendered::message("append a symbol name (ops: definition|references|callers|implementations; absent op = overview)");
    }
    let globset = match glob {
        Some(raw) => match build_globset(raw) {
            Ok(set) => Some(set),
            Err(error) => return Rendered::message(error),
        },
        None => None,
    };
    let sources = files
        .iter()
        .filter(|(path, _)| {
            globset.as_ref().is_none_or(|set| {
                let path = Path::new(path);
                set.is_match(path)
                    || path
                        .file_name()
                        .is_some_and(|name| set.is_match(Path::new(name)))
            })
        })
        .filter_map(|(path, source)| {
            lang_for_path(Path::new(path)).map(|lang| (path.as_str(), source.as_str(), lang))
        })
        .collect::<Vec<_>>();
    let collect_texts = |op| {
        let mut hits = Vec::new();
        for (path, source, lang) in &sources {
            for line0 in lines_for(source, *lang, op, name) {
                hits.push(LocationHit {
                    path: (*path).to_string(),
                    line: (line0 as u32) + 1,
                    snippet: line_text(source, line0),
                });
            }
        }
        hits
    };
    match op {
        Some(op) => {
            let mut hits = collect_texts(op);
            let window = cap_locations(&mut hits, proj.limit);
            let rendered = if proj.before == 0 && proj.after == 0 {
                render_locations(&hits)
            } else {
                let source_map = sources
                    .iter()
                    .map(|(path, source, _)| (*path, *source))
                    .collect();
                super::render::render_locations_with_text_context(
                    &source_map,
                    &hits,
                    proj.before,
                    proj.after,
                )
            };
            with_window_notice(rendered, window)
        }
        None => {
            let defs = collect_texts(SymbolOp::Definition);
            let refs = collect_texts(SymbolOp::References).len();
            let body = if defs.is_empty() {
                format!("no declaration of '{name}' found\n\nreferences: {refs}")
            } else {
                format!(
                    "definition:\n{}\n\nreferences: {refs}",
                    defs.iter()
                        .map(|hit| format!("{}:{}:{}", hit.path, hit.line, hit.snippet))
                        .collect::<Vec<_>>()
                        .join("\n")
                )
            };
            Rendered::message(body)
        }
    }
}

impl SymbolOp {
    pub fn as_str(&self) -> &'static str {
        match self {
            SymbolOp::Definition => "definition",
            SymbolOp::References => "references",
            SymbolOp::Callers => "callers",
            SymbolOp::Implementations => "implementations",
        }
    }

    pub fn from_name(name: &str) -> Option<SymbolOp> {
        Some(match name {
            "definition" => SymbolOp::Definition,
            "references" => SymbolOp::References,
            "callers" => SymbolOp::Callers,
            "implementations" => SymbolOp::Implementations,
            _ => return None,
        })
    }
}

/// Read projections for symbol navigation: a context window around each
/// location row plus an optional cap on the number of rows. Mirrors the grep
/// modifier vocabulary (`-A`/`-B`/`-C`/`context`, `head_limit`/`limit`).
#[derive(Debug, Clone, Copy, Default)]
pub struct NavProjection {
    pub before: usize,
    pub after: usize,
    pub limit: Option<usize>,
}

/// Run a symbol navigation query over `dir` (rooted at `root` for output paths).
/// `op = None` returns the overview (definition sites + reference count); the
/// projection applies only to the location-list ops, not the overview.
pub fn query(
    root: &Path,
    dir: &Path,
    op: Option<SymbolOp>,
    name: &str,
    glob: Option<&str>,
    proj: &NavProjection,
) -> Rendered {
    if name.trim().is_empty() {
        return Rendered::message(
            "append a symbol name, e.g. `/IssueStatus` with `?op=references` \
             (ops: definition|references|callers|implementations; absent op = overview)",
        );
    }
    let globset = match glob {
        Some(raw) => match build_globset(raw) {
            Ok(set) => Some(set),
            Err(err) => return Rendered::message(err),
        },
        None => None,
    };
    let files = source_files(root, dir, globset.as_ref());
    match op {
        Some(op) => {
            let mut hits = collect(root, &files, op, name);
            let window = cap_locations(&mut hits, proj.limit);
            let rendered = if proj.before == 0 && proj.after == 0 {
                render_locations(&hits)
            } else {
                render_locations_with_context(root, &hits, proj.before, proj.after)
            };
            with_window_notice(rendered, window)
        }
        None => overview(root, &files, name),
    }
}

fn overview(root: &Path, files: &[(PathBuf, SupportLang)], name: &str) -> Rendered {
    let defs = collect(root, files, SymbolOp::Definition, name);
    let ref_count = collect(root, files, SymbolOp::References, name).len();
    let mut body = String::new();
    if defs.is_empty() {
        body.push_str(&format!("no declaration of '{name}' found"));
    } else {
        body.push_str("definition:\n");
        // Through the same renderer as every other location list, so the
        // overview's rows never drift from the ones `?op=definition` returns.
        body.push_str(&render_locations(&defs).body);
    }
    body.push_str(&format!("\n\nreferences: {ref_count}"));
    Rendered::message(body)
}

fn collect(
    root: &Path,
    files: &[(PathBuf, SupportLang)],
    op: SymbolOp,
    name: &str,
) -> Vec<LocationHit> {
    let mut hits = Vec::new();
    for (path, lang) in files {
        let Ok(src) = std::fs::read_to_string(path) else {
            continue;
        };
        let lines = lines_for(&src, *lang, op, name);
        for line0 in lines {
            hits.push(LocationHit {
                path: relative(root, path).to_string_lossy().into_owned(),
                line: (line0 as u32) + 1,
                snippet: line_text(&src, line0),
            });
        }
    }
    hits
}

/// Per-file, per-op line extraction. Returns deduplicated, sorted 0-based lines.
fn lines_for(src: &str, lang: SupportLang, op: SymbolOp, name: &str) -> Vec<usize> {
    let spec = grammar::spec(lang);
    let ast = parse(src, lang);
    let root = ast.root();
    let mut lines: Vec<usize> = Vec::new();
    for node in root.dfs() {
        match op {
            SymbolOp::Definition => {
                if spec.is_decl(node.kind().as_ref()) {
                    if let Some(name_node) = node.field("name") {
                        if name_node.text().as_ref() == name {
                            lines.push(node.start_pos().line());
                        }
                    }
                }
            }
            SymbolOp::References => {
                if node.kind().ends_with("identifier") && node.text().as_ref() == name {
                    lines.push(node.start_pos().line());
                }
            }
            SymbolOp::Callers => {
                if spec.is_call(node.kind().as_ref())
                    && call_callee(&node, spec).as_deref() == Some(name)
                {
                    if let Some(line) = enclosing_decl_line(&node, spec) {
                        lines.push(line);
                    }
                }
            }
            SymbolOp::Implementations => {
                if let Some(line) = implementation_line(&node, name) {
                    lines.push(line);
                }
            }
        }
    }
    lines.sort_unstable();
    lines.dedup();
    lines
}

/// The callee name of a call node: the trailing identifier of its callee field.
fn call_callee(node: &SymbolNode, spec: &LangSpec) -> Option<String> {
    for field in spec.callee_fields() {
        if let Some(callee) = node.field(field) {
            if let Some(name) = trailing_identifier(&callee) {
                return Some(name);
            }
        }
    }
    None
}

/// The nearest enclosing declaration's start line (the calling function).
fn enclosing_decl_line(node: &SymbolNode, spec: &LangSpec) -> Option<usize> {
    node.ancestors()
        .find(|ancestor| spec.is_decl(ancestor.kind().as_ref()) && ancestor.field("name").is_some())
        .map(|ancestor| ancestor.start_pos().line())
}

/// Implementation/inheritance line for a name: Rust `impl` blocks naming the
/// type or trait, and TS class/interface heritage clauses naming the symbol.
fn implementation_line(node: &SymbolNode, name: &str) -> Option<usize> {
    match node.kind().as_ref() {
        "impl_item" => {
            let names = [node.field("trait"), node.field("type")]
                .into_iter()
                .flatten()
                .any(|target| trailing_identifier(&target).as_deref() == Some(name));
            names.then(|| node.start_pos().line())
        }
        "class_declaration" | "abstract_class_declaration" | "interface_declaration" => {
            let hit = node.children().any(|child| {
                child.kind().as_ref() == "class_heritage"
                    && child
                        .dfs()
                        .any(|n| n.kind().ends_with("identifier") && n.text().as_ref() == name)
            });
            hit.then(|| node.start_pos().line())
        }
        _ => None,
    }
}

/// The last identifier-like descendant of a node, e.g. `baz` of `foo::bar::baz`
/// or `method` of `obj.method`.
fn trailing_identifier(node: &SymbolNode) -> Option<String> {
    let mut last = None;
    for descendant in node.dfs() {
        if descendant.kind().ends_with("identifier") {
            last = Some(descendant.text().to_string());
        }
    }
    last
}

/// The trimmed source line at a 0-based line number.
fn line_text(src: &str, line0: usize) -> String {
    src.lines()
        .nth(line0)
        .map(str::trim)
        .map(str::to_string)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_rust_definition() {
        let src = "fn alpha() {}\nstruct Beta;\nfn gamma() { alpha(); }\n";
        let lines = lines_for(src, SupportLang::Rust, SymbolOp::Definition, "alpha");
        assert_eq!(lines, vec![0]);
        let beta = lines_for(src, SupportLang::Rust, SymbolOp::Definition, "Beta");
        assert_eq!(beta, vec![1]);
    }

    #[test]
    fn finds_references_across_occurrences() {
        let src = "fn alpha() {}\nfn gamma() { alpha(); alpha(); }\n";
        let lines = lines_for(src, SupportLang::Rust, SymbolOp::References, "alpha");
        // Declaration line 0 and call line 1 (deduped to one row per line).
        assert_eq!(lines, vec![0, 1]);
    }

    #[test]
    fn finds_callers_as_enclosing_function() {
        let src = "fn alpha() {}\nfn gamma() {\n    alpha();\n}\n";
        let lines = lines_for(src, SupportLang::Rust, SymbolOp::Callers, "alpha");
        // The caller is `gamma`, whose declaration starts on line 1.
        assert_eq!(lines, vec![1]);
    }

    #[test]
    fn finds_rust_trait_implementations() {
        let src = "trait Greet {}\nstruct Foo;\nimpl Greet for Foo {}\n";
        let lines = lines_for(src, SupportLang::Rust, SymbolOp::Implementations, "Greet");
        assert_eq!(lines, vec![2]);
        let on_foo = lines_for(src, SupportLang::Rust, SymbolOp::Implementations, "Foo");
        assert_eq!(on_foo, vec![2]);
    }

    #[test]
    fn finds_typescript_implements() {
        let src = "interface Shape {}\nclass Circle implements Shape {}\n";
        let lines = lines_for(
            src,
            SupportLang::TypeScript,
            SymbolOp::Implementations,
            "Shape",
        );
        assert_eq!(lines, vec![1]);
    }

    #[test]
    fn finds_python_definition_and_callers() {
        let src = "def helper():\n    pass\n\ndef main():\n    helper()\n";
        let defs = lines_for(src, SupportLang::Python, SymbolOp::Definition, "helper");
        assert_eq!(defs, vec![0]);
        let callers = lines_for(src, SupportLang::Python, SymbolOp::Callers, "helper");
        assert_eq!(callers, vec![3]);
    }

    #[test]
    fn overview_reports_definition_and_ref_count() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("a.rs"),
            "fn alpha() {}\nfn beta() { alpha(); alpha(); }\n",
        )
        .unwrap();
        let r = query(
            dir.path(),
            dir.path(),
            None,
            "alpha",
            None,
            &NavProjection::default(),
        );
        assert!(r.body.contains("definition:"));
        assert!(r.body.contains("a.rs\n1:"), "body: {}", r.body);
        // Three occurrences across two lines: decl line + the call line.
        assert!(r.body.contains("references: 2"));
    }

    /// The cap counts rows, and a capped list says what it left out: one row of
    /// three, plus the notice that makes the difference visible. A location list
    /// answers "every reference to this name", so a silent cap turns a complete
    /// answer and a sample into the same document.
    #[test]
    fn limit_caps_rows_and_says_what_it_elided() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("a.rs"),
            "fn alpha() {}\nfn beta() { alpha(); }\nfn gamma() { alpha(); }\n",
        )
        .unwrap();
        // References span three lines; cap to a single row.
        let proj = NavProjection {
            limit: Some(1),
            ..Default::default()
        };
        let r = query(
            dir.path(),
            dir.path(),
            Some(SymbolOp::References),
            "alpha",
            None,
            &proj,
        );
        // The path heading, the single kept row, then the notice.
        let lines: Vec<&str> = r.body.lines().collect();
        assert_eq!(lines.len(), 3, "body: {}", r.body);
        assert_eq!(lines[0], "a.rs", "body: {}", r.body);
        assert!(lines[1].starts_with("1:"), "body: {}", r.body);
        assert_eq!(
            lines[2], "[locations 1\u{2013}1 of 3 \u{2014} raise head_limit to see the rest]",
            "body: {}",
            r.body
        );

        // An uncapped read of the same symbol carries no notice — the marker
        // means something because a complete list does not have it.
        let complete = query(
            dir.path(),
            dir.path(),
            Some(SymbolOp::References),
            "alpha",
            None,
            &NavProjection::default(),
        );
        // One heading plus three rows, and no notice.
        assert_eq!(complete.body.lines().count(), 4, "body: {}", complete.body);
        assert!(!complete.body.contains("raise head_limit"));
    }

    #[test]
    fn context_includes_surrounding_lines() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("a.rs"),
            "fn alpha() {}\nfn beta() {\n    alpha();\n}\n",
        )
        .unwrap();
        let proj = NavProjection {
            before: 1,
            after: 1,
            limit: None,
        };
        let r = query(
            dir.path(),
            dir.path(),
            Some(SymbolOp::Callers),
            "alpha",
            None,
            &proj,
        );
        // Caller is `beta`, declared on 1-based line 2 (the match), with line 1
        // and line 3 as context rows rendered with `-` separators.
        assert!(r.body.starts_with("a.rs\n"), "body: {}", r.body);
        assert!(r.body.contains("\n2:"), "body: {}", r.body);
        assert!(r.body.contains("\n1-"), "body: {}", r.body);
        assert!(r.body.contains("\n3-    alpha();"), "body: {}", r.body);
    }

    #[test]
    fn default_projection_matches_bare_rows() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("a.rs"),
            "fn alpha() {}\nfn beta() { alpha(); }\n",
        )
        .unwrap();
        let r = query(
            dir.path(),
            dir.path(),
            Some(SymbolOp::Definition),
            "alpha",
            None,
            &NavProjection::default(),
        );
        assert_eq!(r.body, "a.rs\n1:fn alpha() {}");
    }
}
