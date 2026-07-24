//! The `?ast=` read modifier: a raw structural pattern search over a file or a
//! directory tree. Sibling to `?grep=` — it composes with `?glob=` and renders
//! the same grep-style `path:line:snippet` rows.
//!
//! A pattern is language-specific (it parses under one grammar), so over a mixed
//! tree the engine compiles the pattern lazily per language and skips files
//! whose language the pattern does not compile against. When nothing in scope
//! compiles, the friendly compile error is surfaced rather than a bare
//! "0 matches".

use std::collections::HashMap;
use std::path::Path;

use ast_grep_core::matcher::Pattern;
use ast_grep_language::SupportLang;

use super::engine::{compile_pattern, lang_for_path, parse, run_pattern};
use super::render::{render_locations, LocationHit, Rendered};
use super::walk::{build_globset, relative, source_files};

/// Run an `?ast=` structural search rooted at `root`. `target` is the file or
/// directory being read (under `root`); `glob` optionally filters walked files.
pub fn search(root: &Path, target: &Path, pattern: &str, glob: Option<&str>) -> Rendered {
    if target.is_file() {
        return search_file(root, target, pattern);
    }
    search_dir(root, target, pattern, glob)
}

pub fn search_texts(files: &[(String, String)], pattern: &str, glob: Option<&str>) -> Rendered {
    let globset = match glob {
        Some(raw) => match build_globset(raw) {
            Ok(set) => Some(set),
            Err(err) => return Rendered::message(err),
        },
        None => None,
    };
    let mut compiled: HashMap<SupportLang, Option<Pattern>> = HashMap::new();
    let mut compile_err = None;
    let mut any_lang_ok = false;
    let mut hits = Vec::new();
    for (path, src) in files {
        let path_ref = Path::new(path);
        if globset.as_ref().is_some_and(|set| {
            !set.is_match(path_ref) && !path_ref.file_name().is_some_and(|name| set.is_match(name))
        }) {
            continue;
        }
        let Some(lang) = lang_for_path(path_ref) else {
            continue;
        };
        let compiled =
            compiled
                .entry(lang)
                .or_insert_with(|| match compile_pattern(pattern, lang) {
                    Ok(compiled) => Some(compiled),
                    Err(error) => {
                        compile_err.get_or_insert(error);
                        None
                    }
                });
        let Some(compiled) = compiled else {
            continue;
        };
        any_lang_ok = true;
        hits.extend(collect_hits(path, src, lang, compiled));
    }
    if !any_lang_ok {
        if let Some(error) = compile_err {
            return Rendered::message(error);
        }
    }
    hits.sort_by(|a, b| a.path.cmp(&b.path).then(a.line.cmp(&b.line)));
    if hits.is_empty() {
        empty_ast_result()
    } else {
        render_locations(&hits)
    }
}

/// Parse `src` under `lang`, run `compiled`, and label rows with `display_path`.
///
/// The row snippet is the full source line containing the match start (trimmed),
/// matching `?grep=`'s line-oriented output rather than the matched node's own
/// text — so a pattern that resolves to a small node (a bare keyword, a call)
/// still shows the readable enclosing line instead of a fragment.
fn collect_hits(
    display_path: &str,
    src: &str,
    lang: SupportLang,
    compiled: &Pattern,
) -> Vec<LocationHit> {
    let ast = parse(src, lang);
    let lines: Vec<&str> = src.lines().collect();
    run_pattern(&ast, compiled)
        .into_iter()
        .map(|m| LocationHit {
            path: display_path.to_string(),
            line: (m.start_line as u32) + 1,
            snippet: lines
                .get(m.start_line)
                .map(|line| line.trim().to_string())
                .unwrap_or_else(|| m.snippet()),
        })
        .collect()
}

/// The 0-match result for a pattern that compiled but matched nothing. The
/// reported confusion is that `?ast=` looks like a node-kind selector; it is
/// not, so a clean miss spells out the actual pattern grammar instead of a bare
/// "0 matches".
fn empty_ast_result() -> Rendered {
    Rendered {
        body: "no matches. `?ast=` takes an ast-grep pattern — real code with \
               metavariables (`$VAR` matches one node, `$$$` a run of nodes), \
               e.g. `fn $NAME($$$) { $$$ }` (Rust) or `console.log($$$)` (TS). \
               It is not a tree-sitter node-kind name like `function_declaration`."
            .to_string(),
        suffix: Some("0 matches".to_string()),
    }
}

fn hits_for_file(
    root: &Path,
    path: &Path,
    compiled: &Pattern,
    lang: SupportLang,
) -> Vec<LocationHit> {
    let Ok(src) = std::fs::read_to_string(path) else {
        return Vec::new();
    };
    let display = relative(root, path).to_string_lossy().into_owned();
    collect_hits(&display, &src, lang, compiled)
}

/// Run an `?ast=` search over a single in-memory source, labeling rows with
/// `display_path`. This is the bytes-based entry the archival/gitcoord read path
/// uses: a recorded single-file structural search reconstructs identically from
/// a git blob because the engine only parses source text — it has no live-server
/// or whole-tree dependency. `lang` is the grammar detected from the path
/// (`None` → no bundled grammar for that file type).
pub fn search_text(
    display_path: &str,
    src: &str,
    lang: Option<SupportLang>,
    pattern: &str,
) -> Rendered {
    let Some(lang) = lang else {
        return Rendered::message(format!(
            "no bundled grammar for {display_path} — structural search needs a known file type"
        ));
    };
    let compiled = match compile_pattern(pattern, lang) {
        Ok(pattern) => pattern,
        Err(err) => return Rendered::message(err),
    };
    let hits = collect_hits(display_path, src, lang, &compiled);
    if hits.is_empty() {
        return empty_ast_result();
    }
    render_locations(&hits)
}

fn search_file(root: &Path, path: &Path, pattern: &str) -> Rendered {
    let display = relative(root, path).to_string_lossy().into_owned();
    let src = match std::fs::read_to_string(path) {
        Ok(src) => src,
        Err(err) => return Rendered::message(format!("failed to read {display}: {err}")),
    };
    search_text(&display, &src, lang_for_path(path), pattern)
}

fn search_dir(root: &Path, dir: &Path, pattern: &str, glob: Option<&str>) -> Rendered {
    let globset = match glob {
        Some(raw) => match build_globset(raw) {
            Ok(set) => Some(set),
            Err(err) => return Rendered::message(err),
        },
        None => None,
    };

    // Per-language compiled pattern (None = the pattern does not compile for
    // that language). `compile_err` keeps the first compile failure so an
    // all-skipped search reports the real reason instead of "0 matches".
    let mut compiled: HashMap<SupportLang, Option<Pattern>> = HashMap::new();
    let mut compile_err: Option<String> = None;
    let mut any_lang_ok = false;
    let mut hits: Vec<LocationHit> = Vec::new();

    for (path, lang) in source_files(root, dir, globset.as_ref()) {
        let pattern_for_lang =
            compiled
                .entry(lang)
                .or_insert_with(|| match compile_pattern(pattern, lang) {
                    Ok(compiled) => Some(compiled),
                    Err(err) => {
                        compile_err.get_or_insert(err);
                        None
                    }
                });
        let Some(pattern_for_lang) = pattern_for_lang else {
            continue;
        };
        any_lang_ok = true;
        hits.extend(hits_for_file(root, &path, pattern_for_lang, lang));
    }

    if !any_lang_ok {
        if let Some(err) = compile_err {
            return Rendered::message(err);
        }
    }

    hits.sort_by(|a, b| a.path.cmp(&b.path).then(a.line.cmp(&b.line)));
    if hits.is_empty() {
        return empty_ast_result();
    }
    render_locations(&hits)
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
    fn searches_single_file() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "a.rs", "fn one() {}\nfn two() {}\n");
        let r = search(dir.path(), &dir.path().join("a.rs"), "fn $N() {}", None);
        assert_eq!(r.suffix.as_deref(), Some("2 matches"));
        assert!(r.body.contains("a.rs:1:"));
        assert!(r.body.contains("a.rs:2:"));
    }

    #[test]
    fn walks_directory_and_relativizes_paths() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "src/a.rs", "fn one() {}\n");
        write(dir.path(), "src/b.rs", "fn two() {}\n");
        let r = search(dir.path(), dir.path(), "fn $N() {}", None);
        assert_eq!(r.suffix.as_deref(), Some("2 matches in 2 files"));
        assert!(r.body.contains("src/a.rs:1:"));
        assert!(r.body.contains("src/b.rs:1:"));
    }

    #[test]
    fn glob_filters_walked_files() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "a.rs", "fn one() {}\n");
        write(dir.path(), "b.py", "def two():\n    pass\n");
        let r = search(dir.path(), dir.path(), "fn $N() {}", Some("**/*.rs"));
        assert_eq!(r.suffix.as_deref(), Some("1 matches"));
        assert!(r.body.contains("a.rs:1:"));
    }

    #[test]
    fn search_text_runs_over_in_memory_source() {
        // The archival/gitcoord path hands raw blob text + the relative path.
        let r = search_text(
            "src/a.rs",
            "fn one() {}\nfn two() {}\n",
            Some(SupportLang::Rust),
            "fn $N() {}",
        );
        assert_eq!(r.suffix.as_deref(), Some("2 matches"));
        assert!(r.body.contains("src/a.rs:1:"));
        assert!(r.body.contains("src/a.rs:2:"));
    }

    #[test]
    fn search_text_reconstructs_disk_search_output() {
        // Equivalence the gitcoord reconstruction relies on: running over blob
        // text yields the same rows as the live single-file disk search.
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "a.rs", "fn one() {}\nfn two() {}\n");
        let disk = search(dir.path(), &dir.path().join("a.rs"), "fn $N() {}", None);
        let blob = search_text(
            "a.rs",
            "fn one() {}\nfn two() {}\n",
            Some(SupportLang::Rust),
            "fn $N() {}",
        );
        assert_eq!(disk.body, blob.body);
        assert_eq!(disk.suffix, blob.suffix);
    }

    #[test]
    fn zero_match_pattern_hints_at_pattern_grammar() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "a.rs", "const X: i32 = 1;\n");
        let r = search(dir.path(), dir.path(), "fn $N() {}", Some("**/*.rs"));
        assert_eq!(r.suffix.as_deref(), Some("0 matches"));
        assert!(r.body.contains("ast-grep pattern"));
        assert!(r.body.contains("function_declaration"));
    }

    #[test]
    fn snippet_is_the_full_source_line_not_the_node_fragment() {
        let dir = tempfile::tempdir().unwrap();
        // The call `alpha()` is a small node, but the row shows its enclosing
        // source line, like grep.
        write(dir.path(), "a.rs", "fn beta() { alpha(); }\n");
        let r = search(dir.path(), &dir.path().join("a.rs"), "alpha()", None);
        assert!(
            r.body.contains("a.rs:1:fn beta() { alpha(); }"),
            "body: {}",
            r.body
        );
    }

    #[test]
    fn surfaces_compile_error_when_nothing_compiles() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "a.rs", "fn one() {}\n");
        let r = search(dir.path(), dir.path(), "   ", Some("**/*.rs"));
        assert!(r.body.contains("empty"));
    }
}
