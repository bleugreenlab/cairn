//! Read-style rendering of query outcomes.
//!
//! Produces a `{ body, suffix }` pair so the Phase-3 read surface composes the
//! canonical `=== <descriptor> [<suffix>] ===` header itself. The suffix grammar
//! mirrors the read view's grep segment exactly (`{n} matches in {m} files` /
//! `{n} matches`); location bodies are grep-style `path:line:snippet` rows.

use std::collections::BTreeSet;

use super::queries::{Candidate, LocationHit, QueryOutcome};

/// A rendered outcome: the body text plus the optional header suffix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rendered {
    pub body: String,
    pub suffix: Option<String>,
}

/// Appended when project-wide results were answered before the index finished.
const STILL_INDEXING: &str = "(server still indexing; results may be incomplete)";

/// Render an outcome, appending the still-indexing note when `still_indexing`.
pub fn render(outcome: &QueryOutcome, still_indexing: bool) -> Rendered {
    let mut rendered = match outcome {
        QueryOutcome::Locations(hits) => render_locations(hits),
        QueryOutcome::Hover(markdown) => Rendered {
            body: markdown.clone(),
            suffix: None,
        },
        QueryOutcome::Candidates(candidates) => render_candidates(candidates),
        QueryOutcome::Miss => Rendered {
            body: "no results".to_string(),
            suffix: None,
        },
        QueryOutcome::Unsupported => Rendered {
            body: "unsupported by this server".to_string(),
            suffix: None,
        },
    };
    if still_indexing {
        if rendered.body.is_empty() {
            rendered.body = STILL_INDEXING.to_string();
        } else {
            rendered.body.push('\n');
            rendered.body.push_str(STILL_INDEXING);
        }
    }
    rendered
}

/// Compose the full read-style block for a descriptor (used by the file-anchored
/// `lsp_query` and any caller that wants the complete string rather than parts).
pub fn to_block(descriptor: &str, rendered: &Rendered) -> String {
    match &rendered.suffix {
        Some(suffix) => format!("=== {descriptor} [{suffix}] ===\n{}", rendered.body),
        None => format!("=== {descriptor} ===\n{}", rendered.body),
    }
}

fn render_locations(hits: &[LocationHit]) -> Rendered {
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

fn render_candidates(candidates: &[Candidate]) -> Rendered {
    let mut lines = vec![format!("ambiguous: {} candidates", candidates.len())];
    for c in candidates {
        let qualified = match &c.container {
            Some(container) if !container.is_empty() => format!("{container} · {}", c.name),
            _ => c.name.clone(),
        };
        lines.push(format!("{}:{}: {}", c.path, c.display_line(), qualified));
    }
    Rendered {
        body: lines.join("\n"),
        suffix: Some(format!("{} candidates", candidates.len())),
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
        let r = render(&QueryOutcome::Locations(hits), false);
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
        let r = render(&QueryOutcome::Locations(hits), false);
        assert_eq!(r.suffix.as_deref(), Some("3 matches in 2 files"));
    }

    #[test]
    fn renders_hover_markdown_without_suffix() {
        let r = render(&QueryOutcome::Hover("fn foo() -> i32".to_string()), false);
        assert_eq!(r.suffix, None);
        assert_eq!(r.body, "fn foo() -> i32");
    }

    #[test]
    fn renders_candidate_list_distinctly() {
        let candidates = vec![
            Candidate {
                name: "dup".to_string(),
                container: Some("a".to_string()),
                kind: 12,
                uri: "file:///wt/a.rs".to_string(),
                path: "a.rs".to_string(),
                line: 0,
                character: 0,
            },
            Candidate {
                name: "dup".to_string(),
                container: Some("b".to_string()),
                kind: 12,
                uri: "file:///wt/b.rs".to_string(),
                path: "b.rs".to_string(),
                line: 4,
                character: 0,
            },
        ];
        let r = render(&QueryOutcome::Candidates(candidates), false);
        assert_eq!(r.suffix.as_deref(), Some("2 candidates"));
        assert_eq!(
            r.body,
            "ambiguous: 2 candidates\na.rs:1: a · dup\nb.rs:5: b · dup"
        );
    }

    #[test]
    fn appends_still_indexing_note() {
        let hits = vec![hit("a.rs", 1, "x")];
        let r = render(&QueryOutcome::Locations(hits), true);
        assert_eq!(r.suffix.as_deref(), Some("1 matches"));
        assert_eq!(
            r.body,
            "a.rs:1:x\n(server still indexing; results may be incomplete)"
        );
    }

    #[test]
    fn to_block_composes_header() {
        let r = Rendered {
            body: "a.rs:1:x".to_string(),
            suffix: Some("1 matches".to_string()),
        };
        assert_eq!(
            to_block("lsp:references/foo", &r),
            "=== lsp:references/foo [1 matches] ===\na.rs:1:x"
        );
    }

    #[test]
    fn unsupported_renders_message() {
        let r = render(&QueryOutcome::Unsupported, false);
        assert_eq!(r.body, "unsupported by this server");
        assert_eq!(r.suffix, None);
    }
}
