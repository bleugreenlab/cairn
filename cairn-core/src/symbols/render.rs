//! Read-style rendering for structural results.
//!
//! Produces a `{ body, suffix }` pair so the read surface composes the canonical
//! `=== <descriptor> [<suffix>] ===` header itself. The suffix grammar mirrors
//! the read view's grep segment exactly (`{n} matches in {m} files` /
//! `{n} matches`); location bodies are grep-style `path:line:snippet` rows.
//! Matching the existing shape means agents see no new output format.

use std::collections::BTreeSet;

/// A rendered result: the body text plus the optional header suffix.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Rendered {
    pub body: String,
    pub suffix: Option<String>,
}

/// One location row: file-relative path, 1-based line, single-line snippet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocationHit {
    pub path: String,
    pub line: u32,
    pub snippet: String,
}

impl Rendered {
    /// A plain message with no header suffix.
    pub fn message(body: impl Into<String>) -> Self {
        Self {
            body: body.into(),
            suffix: None,
        }
    }
}

/// Render location hits as grep-style rows with a match-count suffix.
pub fn render_locations(hits: &[LocationHit]) -> Rendered {
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
        assert_eq!(r.body, "src/lib.rs:10:fn target() {\nsrc/lib.rs:42:target();");
    }

    #[test]
    fn renders_multi_file_suffix() {
        let hits = vec![hit("a.rs", 1, "x"), hit("b.rs", 2, "y"), hit("a.rs", 3, "z")];
        let r = render_locations(&hits);
        assert_eq!(r.suffix.as_deref(), Some("3 matches in 2 files"));
    }

    #[test]
    fn empty_renders_zero_matches() {
        let r = render_locations(&[]);
        assert_eq!(r.suffix.as_deref(), Some("0 matches"));
        assert_eq!(r.body, "no results");
    }
}
