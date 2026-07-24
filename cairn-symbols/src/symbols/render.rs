//! Read-style rendering for structural results.
//!
//! Produces a `{ body, suffix }` pair so the read surface composes the canonical
//! `=== <descriptor> [<suffix>] ===` header itself. The suffix grammar mirrors
//! the read view's grep segment exactly (`{n} matches in {m} files` /
//! `{n} matches`); location bodies are grep-style `path:line:snippet` rows.
//! Matching the existing shape means agents see no new output format.

use std::collections::BTreeSet;
use std::path::Path;

/// A rendered result: the body text plus the optional header suffix.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Rendered {
    pub body: String,
    pub(crate) suffix: Option<String>,
}

/// One location row: file-relative path, 1-based line, single-line snippet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocationHit {
    pub(crate) path: String,
    pub(crate) line: u32,
    pub(crate) snippet: String,
}

impl Rendered {
    /// A plain message with no header suffix.
    pub(crate) fn message(body: impl Into<String>) -> Self {
        Self {
            body: body.into(),
            suffix: None,
        }
    }
}

/// Render location hits as grep-style rows with a match-count suffix.
pub(crate) fn render_locations(hits: &[LocationHit]) -> Rendered {
    if hits.is_empty() {
        return Rendered {
            body: "no results".to_string(),
            suffix: Some("0 matches".to_string()),
        };
    }
    let body = hits
        .iter()
        .map(|h| format!("{}:{}:{}", h.path, h.line, h.snippet))
        .collect::<Vec<_>>()
        .join("\n");
    let files: BTreeSet<&str> = hits.iter().map(|h| h.path.as_str()).collect();
    let n = hits.len();
    let suffix = if files.len() > 1 {
        format!("{n} matches in {} files", files.len())
    } else {
        format!("{n} matches")
    };
    Rendered {
        body,
        suffix: Some(suffix),
    }
}

/// Render location hits with `before`/`after` context lines per hit, in the
/// same shape as the grep content path: match lines use `path:N:text`, context
/// lines use `path:N-text`, and non-contiguous groups are separated by `--`.
/// `root` resolves each hit's relative path back to its file for source lines.
/// Context lines are shown untrimmed (like grep) so indentation is visible;
/// the no-context path keeps the existing trimmed `path:N:snippet` rows.
pub(crate) fn render_locations_with_context(
    root: &Path,
    hits: &[LocationHit],
    before: usize,
    after: usize,
) -> Rendered {
    if hits.is_empty() {
        return Rendered {
            body: "no results".to_string(),
            suffix: Some("0 matches".to_string()),
        };
    }

    // Group hits by path, preserving the incoming (path, line) order. Hits
    // arrive sorted by `(path, line)`, so consecutive same-path runs are whole
    // file groups.
    let mut groups: Vec<(String, Vec<&LocationHit>)> = Vec::new();
    for hit in hits {
        match groups.last_mut() {
            Some((path, group)) if *path == hit.path => group.push(hit),
            _ => groups.push((hit.path.clone(), vec![hit])),
        }
    }

    // Each block is a contiguous interval's rows (or a fallback snippet block);
    // `--` separates non-contiguous blocks, within a file and between files.
    let mut blocks: Vec<String> = Vec::new();
    for (path, group) in &groups {
        match std::fs::read_to_string(root.join(path)) {
            Ok(src) => {
                let lines: Vec<&str> = src.lines().collect();
                let total = lines.len();
                let match_lines: BTreeSet<usize> = group.iter().map(|h| h.line as usize).collect();

                // Build per-match windows, merging those that overlap or touch
                // (`start <= last_end + 1`). `match_lines` is ascending, so the
                // windows are produced in order and a single back-merge suffices.
                let mut intervals: Vec<(usize, usize)> = Vec::new();
                for &m in &match_lines {
                    let start = m.saturating_sub(before).max(1);
                    let end = (m + after).min(total);
                    match intervals.last_mut() {
                        Some(last) if start <= last.1 + 1 => last.1 = last.1.max(end),
                        _ => intervals.push((start, end)),
                    }
                }

                for (start, end) in intervals {
                    if !blocks.is_empty() {
                        blocks.push("--".to_string());
                    }
                    let rows = (start..=end)
                        .map(|line_no| {
                            let text = lines.get(line_no - 1).copied().unwrap_or("");
                            let sep = if match_lines.contains(&line_no) {
                                ':'
                            } else {
                                '-'
                            };
                            format!("{path}:{line_no}{sep}{text}")
                        })
                        .collect::<Vec<_>>()
                        .join("\n");
                    blocks.push(rows);
                }
            }
            Err(_) => {
                // Source unreadable: emit this file's hits as bare snippet rows
                // so a partial result still renders.
                if !blocks.is_empty() {
                    blocks.push("--".to_string());
                }
                blocks.push(
                    group
                        .iter()
                        .map(|h| format!("{}:{}:{}", h.path, h.line, h.snippet))
                        .collect::<Vec<_>>()
                        .join("\n"),
                );
            }
        }
    }

    let files: BTreeSet<&str> = hits.iter().map(|h| h.path.as_str()).collect();
    let n = hits.len();
    let suffix = if files.len() > 1 {
        format!("{n} matches in {} files", files.len())
    } else {
        format!("{n} matches")
    };
    Rendered {
        body: blocks.join("\n"),
        suffix: Some(suffix),
    }
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
        assert_eq!(
            r.body,
            "src/lib.rs:10:fn target() {\nsrc/lib.rs:42:target();"
        );
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
        assert_eq!(
            r.body,
            "a.rs:2-line two\na.rs:3:MATCH here\na.rs:4-line four"
        );
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
        assert!(r.body.contains("a.rs:3:M1"));
        assert!(r.body.contains("a.rs:8:M2"));
    }

    #[test]
    fn touching_windows_merge_without_duplicates() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.rs"), "a\nM1\nc\nM2\ne\n").unwrap();
        let hits = vec![hit("a.rs", 2, "M1"), hit("a.rs", 4, "M2")];
        let r = render_locations_with_context(dir.path(), &hits, 1, 1);
        // [1,3] and [3,5] overlap at line 3, merging to [1,5]: no `--`, no dup.
        assert!(!r.body.contains("--"), "body: {}", r.body);
        assert_eq!(r.body.matches("a.rs:3-c").count(), 1);
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
        assert_eq!(r.body, "gone.rs:5:snippet text");
        assert_eq!(r.suffix.as_deref(), Some("1 matches"));
    }
}
