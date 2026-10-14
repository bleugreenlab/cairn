//! Small, dependency-light path/glob/format helpers shared by structural and
//! logical project search, and re-exported into cairn-core so every renderer
//! formats and filters identically.

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

/// One line of grep output, independent of which engine produced it.
///
/// Both engines emit these and [`render_grep_lines`] turns them into bytes, so
/// warm/cold parity is structural rather than two renderers agreeing by
/// inspection. It also makes results mergeable: an engine that can only cover
/// part of a query hands back the lines it proved and the caller appends its
/// own before rendering once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrepLine {
    /// Path relative to the search root. Empty for a materialized in-memory
    /// body (a rendered resource, web markdown, artifact, or transcript), which
    /// has no file dimension and so carries no path prefix.
    pub path: String,
    pub line_number: u64,
    /// A matched line (`:` separator) rather than a context line (`-`).
    pub is_match: bool,
    pub text: String,
}

/// Put collected grep lines in render order and collapse duplicate coordinates:
/// ordered by path, then line number, with a match kept over a context line at
/// the same coordinate (a line that is both a match and another match's context
/// renders as the match).
///
/// [`render_grep_lines`] applies this itself, and it is idempotent. It is public
/// because a caller that windows lines BEFORE rendering — a `head_limit` that
/// caps matches — has to select over the same order and the same deduplication
/// the reader will see, and that ordering needs one definition rather than two
/// that agree by inspection.
pub fn order_grep_lines(lines: &mut Vec<GrepLine>) {
    lines.sort_by(|left, right| {
        left.path
            .cmp(&right.path)
            .then_with(|| left.line_number.cmp(&right.line_number))
            // Matches sort ahead of context at the same coordinate so the
            // dedupe below keeps the match.
            .then_with(|| right.is_match.cmp(&left.is_match))
    });
    lines.dedup_by(|later, earlier| {
        later.path == earlier.path && later.line_number == earlier.line_number
    });
}

/// Render collected grep lines into the output contract for `output_mode`.
/// This is the single definition of ripgrep parity:
///
/// - each `(path, line_number)` coordinate appears exactly once, and a line
///   that is both a match and some other match's context renders as the match.
///   Overlapping context windows therefore cannot double-print;
/// - lines are ordered by path, then line number, so merged results from more
///   than one source come out correctly ordered by construction;
/// - a body with a path dimension is laid out the way ripgrep's `--heading`
///   renders: each path appears once as a heading above its file's rows, every
///   row beneath it is `N:text` (match) / `N-text` (context), and a blank line
///   opens each new file group. A path-less body (a materialized in-memory
///   read) has no heading and renders bare rows;
/// - `--` separates context groups within a file across a line-number gap when
///   context was requested; a file boundary is marked by the blank line and the
///   next heading instead, so a gap and a file change never look alike;
/// - `files_with_matches` projects to unique matching paths and `count` to
///   `path:count`.
///
/// `native_output` renders the way piped ripgrep does instead — every row
/// carrying its own path prefix, context as `path-N-text` — because that body
/// is handed back as ripgrep's own output to a translated shell search.
pub fn render_grep_lines(
    mut lines: Vec<GrepLine>,
    output_mode: &str,
    show_line_numbers: bool,
    native_output: bool,
    context_requested: bool,
) -> String {
    order_grep_lines(&mut lines);

    match output_mode {
        "files_with_matches" => {
            let mut paths: Vec<&str> = lines
                .iter()
                .filter(|line| line.is_match)
                .map(|line| line.path.as_str())
                .collect();
            paths.dedup();
            paths.join("\n")
        }
        "count" => {
            let mut counts: Vec<(&str, usize)> = Vec::new();
            for line in lines.iter().filter(|line| line.is_match) {
                match counts.last_mut() {
                    Some((path, count)) if *path == line.path => *count += 1,
                    _ => counts.push((line.path.as_str(), 1)),
                }
            }
            counts
                .into_iter()
                .map(|(path, count)| format!("{path}:{count}"))
                .collect::<Vec<_>>()
                .join("\n")
        }
        // "content" and any already-validated mode fall here.
        _ => {
            // The path is a heading over its file's rows rather than a prefix
            // on each one: with context requested a long path was repeated on
            // every line of every window, and after its first appearance in a
            // group it carries no information. `native_output` is the one
            // caller that must stay byte-identical to piped ripgrep, so it
            // keeps the per-row prefix.
            let heading_style = !native_output;
            let mut rendered: Vec<String> = Vec::with_capacity(lines.len() + 2);
            let mut previous: Option<(&str, u64)> = None;
            for line in &lines {
                let headed = heading_style && !line.path.is_empty();
                if headed && previous.is_none_or(|(path, _)| path != line.path) {
                    // A blank line before every heading but the first keeps a
                    // heading from reading as a row of the group above it.
                    if !rendered.is_empty() {
                        rendered.push(String::new());
                    }
                    rendered.push(line.path.clone());
                } else if context_requested
                    && previous.is_some_and(|(path, number)| {
                        path != line.path || line.line_number > number + 1
                    })
                {
                    rendered.push("--".to_string());
                }
                let separator = if line.is_match { ':' } else { '-' };
                rendered.push(format_grep_line(
                    if headed { "" } else { line.path.as_str() },
                    show_line_numbers.then_some(line.line_number),
                    separator,
                    &line.text,
                    native_output,
                ));
                previous = Some((line.path.as_str(), line.line_number));
            }
            rendered.join("\n")
        }
    }
}

/// Format one grep content row. An empty `relative` label drops the path prefix
/// entirely, yielding `N:text` / `N-text` — the form every row takes under a
/// heading, and the form a path-less in-memory body takes throughout. A
/// non-empty label prefixes the row (`path:N:text` / `path:N-text`), which is
/// what `native_output` needs; `native_output` further swaps the context form
/// for ripgrep's own `path-N-text`. Every form collapses to bare `text` when
/// line numbers are off.
fn format_grep_line(
    relative: &str,
    line_number: Option<u64>,
    sep: char,
    text: &str,
    native_output: bool,
) -> String {
    if native_output && sep == '-' {
        return match (relative.is_empty(), line_number) {
            (true, Some(n)) => format!("{n}-{text}"),
            (true, None) => text.to_string(),
            (false, Some(n)) => format!("{relative}-{n}-{text}"),
            (false, None) => format!("{relative}-{text}"),
        };
    }
    match (relative.is_empty(), line_number) {
        (true, Some(n)) => format!("{n}{sep}{text}"),
        (true, None) => text.to_string(),
        (false, Some(n)) => format!("{relative}:{n}{sep}{text}"),
        (false, None) => format!("{relative}{sep}{text}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn line(path: &str, number: u64, is_match: bool, text: &str) -> GrepLine {
        GrepLine {
            path: path.to_string(),
            line_number: number,
            is_match,
            text: text.to_string(),
        }
    }

    #[test]
    fn overlapping_context_windows_emit_each_line_once() {
        // Two matches three lines apart with -C2: their windows share lines 4
        // and 5. Rendering each window independently prints those twice.
        let lines = vec![
            line("a.rs", 1, false, "one"),
            line("a.rs", 2, false, "two"),
            line("a.rs", 3, true, "MATCH"),
            line("a.rs", 4, false, "four"),
            line("a.rs", 5, false, "five"),
            line("a.rs", 4, false, "four"),
            line("a.rs", 5, false, "five"),
            line("a.rs", 6, true, "MATCH"),
            line("a.rs", 7, false, "seven"),
            line("a.rs", 8, false, "eight"),
        ];
        assert_eq!(
            render_grep_lines(lines, "content", true, false, true),
            "a.rs\n1-one\n2-two\n3:MATCH\n4-four\n5-five\n6:MATCH\n7-seven\n8-eight"
        );
    }

    #[test]
    fn a_match_inside_another_matchs_context_renders_as_a_match() {
        // Line 2 is both a match and line 1's after-context. rg prints it once,
        // with the `:` match separator.
        let lines = vec![
            line("a.rs", 1, true, "MATCH one"),
            line("a.rs", 2, false, "MATCH two"),
            line("a.rs", 1, false, "MATCH one"),
            line("a.rs", 2, true, "MATCH two"),
        ];
        assert_eq!(
            render_grep_lines(lines, "content", true, false, true),
            "a.rs\n1:MATCH one\n2:MATCH two"
        );
    }

    #[test]
    fn a_path_appears_once_per_file_group_with_gaps_separated_inside_it() {
        let lines = vec![
            line("a.rs", 5, true, "first"),
            line("a.rs", 6, false, "adjacent"),
            // Gap: 6 -> 20 is a `--` separator WITHIN the file.
            line("a.rs", 20, true, "second"),
            // A file boundary is the blank line and the next heading instead.
            line("b.rs", 1, true, "third"),
        ];
        assert_eq!(
            render_grep_lines(lines.clone(), "content", true, false, true),
            "a.rs\n5:first\n6-adjacent\n--\n20:second\n\nb.rs\n1:third"
        );
        // Without a context request there are no groups to separate, but the
        // file grouping (and so the heading) is unchanged.
        assert_eq!(
            render_grep_lines(lines, "content", true, false, false),
            "a.rs\n5:first\n6-adjacent\n20:second\n\nb.rs\n1:third"
        );
    }

    #[test]
    fn native_output_keeps_ripgrep_path_prefixes_and_separators() {
        // A translated shell search hands this body back as ripgrep's own, so
        // it stays in piped-rg form: no headings, a path on every row, and `--`
        // at the file boundary.
        let lines = vec![
            line("src/a.txt", 3, false, "before"),
            line("src/a.txt", 4, true, "match"),
            line("src/b.txt", 1, true, "other"),
        ];
        assert_eq!(
            render_grep_lines(lines.clone(), "content", true, true, true),
            "src/a.txt-3-before\nsrc/a.txt:4:match\n--\nsrc/b.txt:1:other"
        );
        let lines = vec![
            line("src/a.txt", 3, false, "before"),
            line("src/a.txt", 4, true, "match"),
        ];
        assert_eq!(
            render_grep_lines(lines, "content", false, true, true),
            "src/a.txt-before\nsrc/a.txt:match"
        );
    }

    #[test]
    fn projections_order_by_path_and_ignore_context_lines() {
        let lines = vec![
            line("b.rs", 9, true, "x"),
            line("a.rs", 2, true, "x"),
            line("b.rs", 1, true, "x"),
            line("b.rs", 2, false, "context does not count"),
        ];
        assert_eq!(
            render_grep_lines(lines.clone(), "files_with_matches", true, false, false),
            "a.rs\nb.rs"
        );
        assert_eq!(
            render_grep_lines(lines, "count", true, false, false),
            "a.rs:1\nb.rs:2"
        );
    }

    #[test]
    fn an_in_memory_body_renders_without_a_path_prefix() {
        let lines = vec![
            GrepLine {
                path: String::new(),
                line_number: 2,
                is_match: true,
                text: "needle".to_string(),
            },
            GrepLine {
                path: String::new(),
                line_number: 3,
                is_match: false,
                text: "after".to_string(),
            },
        ];
        assert_eq!(
            render_grep_lines(lines, "content", true, false, true),
            "2:needle\n3-after"
        );
    }

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
