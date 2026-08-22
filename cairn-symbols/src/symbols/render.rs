//! Read-style rendering for structural results.
//!
//! Produces a `{ body, suffix }` pair so the read surface composes the canonical
//! `=== <descriptor> [<suffix>] ===` header itself. The suffix grammar mirrors
//! the read view's grep segment exactly (`{n} matches in {m} files` /
//! `{n} matches`); location bodies are rendered by the canonical grep renderer
//! itself. Matching the existing shape means agents see no new output format.

use std::collections::BTreeSet;
use std::path::Path;

use crate::search_util::{order_grep_lines, render_grep_lines, GrepLine};

/// A rendered result: the body text, the optional header suffix, and the result
/// counts behind both.
///
/// The counts travel with the body because rendered text cannot be classified
/// back into results without guessing. Under the heading layout a file named
/// `1:foo.rs` is indistinguishable from a match row, and one named `--` from a
/// group separator, so any reader that re-parses a body to count it is one
/// unusual filename away from a wrong header. Counting where the results are
/// still structured removes the question.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Rendered {
    pub body: String,
    pub(crate) suffix: Option<String>,
    /// Result rows in `body` — matches for a search, entries for an outline.
    pub matches: usize,
    /// Distinct files those rows came from; `0` for a body with no path
    /// dimension (a single in-memory source renders path-less rows).
    pub files: usize,
}

/// One location row: file-relative path, 1-based line, single-line snippet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocationHit {
    pub(crate) path: String,
    pub(crate) line: u32,
    pub(crate) snippet: String,
}

impl Rendered {
    /// A plain message with no header suffix. Prose, not results, so it counts
    /// as nothing.
    pub(crate) fn message(body: impl Into<String>) -> Self {
        Self {
            body: body.into(),
            suffix: None,
            matches: 0,
            files: 0,
        }
    }

    /// A body whose counts were taken while it was assembled.
    pub(crate) fn counted(body: impl Into<String>, matches: usize, files: usize) -> Self {
        Self {
            body: body.into(),
            suffix: None,
            matches,
            files,
        }
    }
}

/// Render location hits as grep rows with a match-count suffix.
pub(crate) fn render_locations(hits: &[LocationHit]) -> Rendered {
    render_grep(hits.iter().map(match_line).collect(), false)
}

/// One structural hit as a grep match line.
fn match_line(hit: &LocationHit) -> GrepLine {
    GrepLine {
        path: hit.path.clone(),
        line_number: hit.line as u64,
        is_match: true,
        text: hit.snippet.clone(),
    }
}

/// Render collected grep lines through the canonical renderer, with a suffix
/// counting the body it produced.
///
/// Structural results render through `render_grep_lines` rather than composing
/// rows here, so `?ast=` and `cairn:~/symbols` read in exactly the shape
/// `?grep=` does — including the path-as-heading layout — and one change to
/// that contract moves every surface at once. The suffix counts rendered match
/// lines rather than incoming hits because the renderer collapses duplicate
/// `(path, line)` coordinates: two references on one source line are one row,
/// and a header still claiming two would describe a body that shows one.
fn render_grep(mut lines: Vec<GrepLine>, context_requested: bool) -> Rendered {
    order_grep_lines(&mut lines);
    let matches = lines.iter().filter(|line| line.is_match).count();
    if matches == 0 {
        return Rendered {
            body: "no results".to_string(),
            suffix: Some("0 matches".to_string()),
            matches: 0,
            files: 0,
        };
    }
    let files = lines
        .iter()
        .filter(|line| line.is_match)
        .map(|line| line.path.as_str())
        .collect::<BTreeSet<_>>()
        .len();
    let suffix = if files > 1 {
        format!("{matches} matches in {files} files")
    } else {
        format!("{matches} matches")
    };
    Rendered {
        body: render_grep_lines(lines, "content", true, false, context_requested),
        suffix: Some(suffix),
        matches,
        files,
    }
}

/// Render location hits with `before`/`after` context lines per hit, in the
/// same shape as the grep content path: the path once as a heading, match rows
/// as `N:text`, context rows as `N-text`, and `--` between non-contiguous
/// groups within a file. `root` resolves each hit's relative path back to its
/// file for source lines. Context rows are shown untrimmed (like grep) so
/// indentation is visible; the no-context path keeps the trimmed snippet rows.
pub(crate) fn render_locations_with_text_context(
    sources: &std::collections::HashMap<&str, &str>,
    hits: &[LocationHit],
    before: usize,
    after: usize,
) -> Rendered {
    render_locations_with_source(hits, before, after, |path| {
        sources.get(path).map(|source| (*source).to_string())
    })
}

pub(crate) fn render_locations_with_context(
    root: &Path,
    hits: &[LocationHit],
    before: usize,
    after: usize,
) -> Rendered {
    render_locations_with_source(hits, before, after, |path| {
        std::fs::read_to_string(root.join(path)).ok()
    })
}

fn render_locations_with_source(
    hits: &[LocationHit],
    before: usize,
    after: usize,
    mut source: impl FnMut(&str) -> Option<String>,
) -> Rendered {
    // Group hits by path, preserving the incoming (path, line) order. Hits
    // arrive sorted by `(path, line)`, so consecutive same-path runs are whole
    // file groups and each source is read exactly once.
    let mut groups: Vec<(String, Vec<&LocationHit>)> = Vec::new();
    for hit in hits {
        match groups.last_mut() {
            Some((path, group)) if *path == hit.path => group.push(hit),
            _ => groups.push((hit.path.clone(), vec![hit])),
        }
    }

    // Expand each hit into its context window as grep lines. Windows that
    // overlap or touch need no merging here: the renderer collapses duplicate
    // coordinates and separates groups on a line-number gap, so contiguity is
    // decided in one place for structural results and grep alike.
    let mut lines: Vec<GrepLine> = Vec::new();
    for (path, group) in &groups {
        let Some(src) = source(path) else {
            // Source unreadable: keep this file's hits as bare snippet rows so
            // a partial result still renders.
            lines.extend(group.iter().copied().map(match_line));
            continue;
        };
        let src_lines: Vec<&str> = src.lines().collect();
        let total = src_lines.len();
        let match_lines: BTreeSet<usize> = group.iter().map(|h| h.line as usize).collect();
        for hit in group {
            let at = hit.line as usize;
            let start = at.saturating_sub(before).max(1);
            let end = (at + after).min(total);
            if at > total || start > end {
                // A hit past the end of the source it was recorded against:
                // keep its snippet rather than dropping the location.
                lines.push(match_line(hit));
                continue;
            }
            for number in start..=end {
                lines.push(GrepLine {
                    path: path.clone(),
                    line_number: number as u64,
                    is_match: match_lines.contains(&number),
                    text: src_lines.get(number - 1).copied().unwrap_or("").to_string(),
                });
            }
        }
    }

    render_grep(lines, before > 0 || after > 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hit(path: &str, line: u32, snippet: &str) -> LocationHit {
        LocationHit {
            path: path.to_string(),
            line,
            snippet: snippet.to_string(),
        }
    }

    #[test]
    fn renders_single_file_locations() {
        let hits = vec![
            hit("src/lib.rs", 10, "fn target() {"),
            hit("src/lib.rs", 42, "target();"),
        ];
        let r = render_locations(&hits);
        assert_eq!(r.suffix.as_deref(), Some("2 matches"));
        assert_eq!((r.matches, r.files), (2, 1));
        assert_eq!(r.body, "src/lib.rs\n10:fn target() {\n42:target();");
    }

    #[test]
    fn counts_survive_paths_that_look_like_rows_or_separators() {
        // Under the heading layout a file named `1:odd.rs` renders a heading
        // indistinguishable from a match row, and one named `--` from a group
        // separator. Counting the hits instead of the rendered text cannot be
        // fooled by either.
        let hits = vec![hit("1:odd.rs", 3, "hit"), hit("--", 4, "hit")];
        let r = render_locations(&hits);
        assert_eq!((r.matches, r.files), (2, 2));
        assert_eq!(r.suffix.as_deref(), Some("2 matches in 2 files"));
    }

    #[test]
    fn renders_multi_file_suffix() {
        let hits = vec![
            hit("a.rs", 1, "x"),
            hit("b.rs", 2, "y"),
            hit("a.rs", 3, "z"),
        ];
        let r = render_locations(&hits);
        assert_eq!(r.suffix.as_deref(), Some("3 matches in 2 files"));
    }

    #[test]
    fn empty_renders_zero_matches() {
        let r = render_locations(&[]);
        assert_eq!(r.suffix.as_deref(), Some("0 matches"));
        assert_eq!(r.body, "no results");
    }

    #[test]
    fn context_renders_match_and_surrounding_lines() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("a.rs"),
            "line one\nline two\nMATCH here\nline four\nline five\n",
        )
        .unwrap();
        let hits = vec![hit("a.rs", 3, "MATCH here")];
        let r = render_locations_with_context(dir.path(), &hits, 1, 1);
        // Match line uses `:`, context lines use `-`, untrimmed source text.
        assert_eq!(r.body, "a.rs\n2-line two\n3:MATCH here\n4-line four");
        assert_eq!(r.suffix.as_deref(), Some("1 matches"));
    }

    #[test]
    fn noncontiguous_hits_get_separator() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.rs"), "a\nb\nM1\nd\ne\nf\ng\nM2\ni\n").unwrap();
        let hits = vec![hit("a.rs", 3, "M1"), hit("a.rs", 8, "M2")];
        let r = render_locations_with_context(dir.path(), &hits, 1, 1);
        // Windows [2,4] and [7,9] are non-contiguous, separated by `--`.
        assert!(r.body.contains("\n--\n"), "body: {}", r.body);
        // The path is a heading, so the rows beneath it are bare.
        assert_eq!(r.body.matches("a.rs").count(), 1, "body: {}", r.body);
        assert!(r.body.contains("\n3:M1"));
        assert!(r.body.contains("\n8:M2"));
    }

    #[test]
    fn touching_windows_merge_without_duplicates() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.rs"), "a\nM1\nc\nM2\ne\n").unwrap();
        let hits = vec![hit("a.rs", 2, "M1"), hit("a.rs", 4, "M2")];
        let r = render_locations_with_context(dir.path(), &hits, 1, 1);
        // [1,3] and [3,5] overlap at line 3, merging to [1,5]: no `--`, no dup.
        assert!(!r.body.contains("--"), "body: {}", r.body);
        assert_eq!(r.body.matches("3-c").count(), 1);
    }

    #[test]
    fn context_empty_hits_render_no_results() {
        let dir = tempfile::tempdir().unwrap();
        let r = render_locations_with_context(dir.path(), &[], 1, 1);
        assert_eq!(r.body, "no results");
        assert_eq!(r.suffix.as_deref(), Some("0 matches"));
    }

    #[test]
    fn missing_file_falls_back_to_snippet_rows() {
        let dir = tempfile::tempdir().unwrap();
        let hits = vec![hit("gone.rs", 5, "snippet text")];
        let r = render_locations_with_context(dir.path(), &hits, 2, 2);
        assert_eq!(r.body, "gone.rs\n5:snippet text");
        assert_eq!(r.suffix.as_deref(), Some("1 matches"));
    }
}
