//! Token counting for `read` tool-result bodies (CAIRN-1593).
//!
//! The transcript list renders read rows as a per-target token count instead of
//! shipping the full read body to the frontend. Counts are computed here with
//! tiktoken-rs's `o200k_base` BPE. That is an *approximation* of Claude's
//! tokenizer — Claude's tokenizer is not public — so counts are surfaced in the
//! UI as `~N tok`, never as an exact figure.
//!
//! Section parsing mirrors the frontend grammar in
//! `src/components/toolFormatters/read.tsx` (`parseReadSections`,
//! `HEADER_SUFFIX_RE`): a composed batch read is split on `=== uri [suffix] ===`
//! headers, and a single-target read with no header falls back to one segment
//! covering the whole body. Segments come out in input order because the batch
//! assembler composes targets in input order.

use regex::Regex;
use serde::{Deserialize, Serialize};
use std::sync::OnceLock;
use tiktoken_rs::{o200k_base, CoreBPE};

/// Per-target token count for one section of a composed read result. Serialized
/// onto `Event.read_segments` (camelCase `readSegments`) and consumed by the
/// read formatter, which maps segments to rows by input order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadSegmentTokens {
    /// Suffix-stripped header URI for this section, or the single input path for
    /// a no-header read. Empty when neither is known.
    pub target: String,
    /// Approximate token count of this section's body.
    pub tokens: i64,
}

fn bpe() -> &'static CoreBPE {
    static BPE: OnceLock<CoreBPE> = OnceLock::new();
    BPE.get_or_init(|| o200k_base().expect("o200k_base BPE initializes"))
}

/// Approximate token count of `text` using the `o200k_base` BPE. Ordinary
/// encoding (no special-token handling) so arbitrary file content never errors.
pub fn count_tokens(text: &str) -> usize {
    bpe().encode_ordinary(text).len()
}

/// True when `name` denotes the unified `read` tool, matching the frontend's
/// `normalizeToolName` (strip an `mcp__<server>__` prefix, case-insensitive).
pub fn is_read_tool(name: &str) -> bool {
    let base = name.rsplit("__").next().unwrap_or(name);
    base.eq_ignore_ascii_case("read")
}

/// Extract the `paths` array from a read tool-call input, ignoring per-target
/// query scoping (kept intact — headers carry the full target string).
pub fn extract_paths(input: &serde_json::Value) -> Vec<String> {
    input
        .get("paths")
        .and_then(|p| p.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

fn header_suffix_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    // U+2013 (en dash) is the separator the backend emits in `lines A\u{2013}B`.
    RE.get_or_init(|| {
        Regex::new(
            r" \[(?:lines \d+\u{2013}\d+ of \d+|\d+ matches(?: in \d+ files)?|\d+ files|truncated)\]$",
        )
        .expect("header suffix regex compiles")
    })
}

fn strip_header_suffix(target: &str) -> String {
    header_suffix_re().replace(target, "").into_owned()
}

/// The inner `<uri> [suffix]` of a `=== ... ===` header line, suffix intact.
fn header_inner(line: &str) -> Option<&str> {
    let inner = line.strip_prefix("=== ")?.strip_suffix(" ===")?;
    if inner.is_empty() {
        None
    } else {
        Some(inner)
    }
}

/// A header URI is the input target plus an optional enriched ` [suffix]`, so a
/// target matches its header when the inner equals the target or is the target
/// followed by ` [`. (A glob URI's own `[abc]` brackets never collide — they are
/// not preceded by ` [`.)
fn header_matches_target(inner: &str, target: &str) -> bool {
    inner == target || inner.starts_with(&format!("{target} ["))
}

/// Whether a header's (suffix-stripped) inner looks like a read target URI. Used
/// to tell a real section frame from a content line that merely looks like
/// `=== something ===`. Notably, the CLI relativizes cairn URIs in result text
/// (`cairn://p/X/…` → `cairn:~/…`), so a resource header can disagree with the
/// canonical input path while still being a genuine frame.
fn is_uri_shaped(inner: &str) -> bool {
    const SCHEMES: [&str; 5] = ["cairn://", "cairn:~", "file:", "http://", "https://"];
    let stripped = strip_header_suffix(inner);
    SCHEMES.iter().any(|scheme| stripped.starts_with(scheme))
}

/// A `=== … ===` line is a section frame when its inner matches one of the input
/// targets or is itself shaped like a target URI.
fn is_frame_header(inner: &str, expected: &[String]) -> bool {
    expected
        .iter()
        .any(|target| header_matches_target(inner, target))
        || is_uri_shaped(inner)
}

/// Whether the body is a framed batch: its first non-empty line is a frame
/// header. Single-target reads are emitted unframed (no header), so a bare
/// URI-shaped `=== … ===` line *inside* such a body is content, not a frame —
/// this guard keeps it whole instead of mis-splitting on it.
fn is_framed(output: &str, expected: &[String]) -> bool {
    output
        .lines()
        .find(|line| !line.trim().is_empty())
        .and_then(header_inner)
        .map(|inner| is_frame_header(inner, expected))
        .unwrap_or(false)
}

/// Split a composed read body into framed `(header_inner, body)` sections, in
/// document order, trimming each section's trailing batch-truncation notes.
fn split_framed_sections(output: &str, expected: &[String]) -> Vec<(String, String)> {
    let mut sections: Vec<(String, Vec<String>)> = Vec::new();
    let mut current: Option<String> = None;
    let mut body: Vec<String> = Vec::new();

    for line in output.split('\n') {
        if let Some(inner) = header_inner(line) {
            if is_frame_header(inner, expected) {
                if let Some(header) = current.take() {
                    sections.push((header, std::mem::take(&mut body)));
                }
                current = Some(inner.to_string());
                continue;
            }
        }
        if current.is_some() {
            body.push(line.to_string());
        }
    }
    if let Some(header) = current.take() {
        sections.push((header, std::mem::take(&mut body)));
    }

    sections
        .into_iter()
        .map(|(header, mut lines)| {
            while lines.last().map(|l| is_batch_truncated(l)).unwrap_or(false) {
                lines.pop();
            }
            (header, lines.join("\n"))
        })
        .collect()
}

fn is_batch_truncated(line: &str) -> bool {
    line.starts_with("--- batch truncated: ") && line.ends_with(" ---")
}

/// Mirror of the frontend `extractOutput` for the cases a read body hits: an
/// MCP `[{type:"text", text}]` array is unwrapped and joined; anything else
/// (the common composed-batch text) is returned as-is.
fn extract_output(result: &str) -> String {
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(result) {
        if let Some(arr) = value.as_array() {
            let has_mcp = arr.iter().any(|item| item.get("type").is_some());
            if has_mcp {
                return arr
                    .iter()
                    .filter(|item| item.get("type").and_then(|t| t.as_str()) == Some("text"))
                    .filter_map(|item| item.get("text").and_then(|t| t.as_str()))
                    .collect::<Vec<_>>()
                    .join("\n");
            }
        }
    }
    result.to_string()
}

/// Split a composed read result into per-target token segments. `expected` is
/// the read's input `paths`. The batch is assembled in input order with one
/// frame per target, so when the framed-section count matches the input count,
/// **position** decides each segment's target — which survives the CLI
/// relativizing cairn URIs in headers (a resource header can read `cairn:~/…`
/// while the input path is canonical `cairn://p/X/…`). When the counts disagree
/// (a stray content frame, say), fall back to matching each header against the
/// input paths. A single unframed read yields one whole-body segment.
pub fn read_segment_tokens(result_body: &str, expected: &[String]) -> Vec<ReadSegmentTokens> {
    let output = extract_output(result_body);
    if !is_framed(&output, expected) {
        return vec![ReadSegmentTokens {
            target: expected.first().cloned().unwrap_or_default(),
            tokens: count_tokens(&output) as i64,
        }];
    }

    let sections = split_framed_sections(&output, expected);
    if sections.is_empty() {
        return vec![ReadSegmentTokens {
            target: expected.first().cloned().unwrap_or_default(),
            tokens: count_tokens(&output) as i64,
        }];
    }

    let positional = !expected.is_empty() && sections.len() == expected.len();
    sections
        .into_iter()
        .enumerate()
        .map(|(index, (header, body))| {
            let target = if positional {
                expected[index].clone()
            } else {
                expected
                    .iter()
                    .find(|target| header_matches_target(&header, target))
                    .cloned()
                    .unwrap_or_else(|| strip_header_suffix(&header))
            };
            ReadSegmentTokens {
                target,
                tokens: count_tokens(&body) as i64,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_read_tool_matches_unified_read_and_mcp_prefix() {
        assert!(is_read_tool("read"));
        assert!(is_read_tool("Read"));
        assert!(is_read_tool("mcp__cairn__read"));
        assert!(!is_read_tool("write"));
        assert!(!is_read_tool("mcp__cairn__run"));
    }

    #[test]
    fn multi_section_batch_yields_one_segment_per_target_in_order() {
        let body = "=== file:a.rs ===\nline one\nline two\n=== file:b.rs ===\nbeta";
        let segs = read_segment_tokens(body, &["file:a.rs".into(), "file:b.rs".into()]);
        assert_eq!(segs.len(), 2);
        assert_eq!(segs[0].target, "file:a.rs");
        assert_eq!(segs[1].target, "file:b.rs");
        assert!(segs[0].tokens > 0);
        assert!(segs[1].tokens > 0);
    }

    #[test]
    fn single_no_header_read_yields_one_segment_with_input_target() {
        let body = "just some file content\nwith two lines";
        let segs = read_segment_tokens(body, &["file:only.rs".into()]);
        assert_eq!(segs.len(), 1);
        assert_eq!(segs[0].target, "file:only.rs");
        assert!(segs[0].tokens > 0);
    }

    #[test]
    fn single_relativized_resource_read_counts_body_only() {
        // The CLI relativizes cairn URIs in result text, so the header reads
        // `cairn:~/issues` while the input path is canonical. A single read must
        // still attach a count, and tokenize the body only (not the header line).
        let segs = read_segment_tokens(
            "=== cairn:~/issues?limit=3 [3 of 3 issues] ===\nissue body here",
            &["cairn://p/RB/issues?limit=3".into()],
        );
        assert_eq!(segs.len(), 1);
        assert_eq!(segs[0].target, "cairn://p/RB/issues?limit=3");
        assert!(segs[0].tokens > 0);
    }

    #[test]
    fn relativized_resource_header_maps_to_input_path_positionally() {
        // A file read + a resource read in one batch, where the resource header
        // was relativized away from the canonical input path. Positional mapping
        // must still give the resource row its own segment keyed to the input.
        let body = "=== file:a.rs [lines 1\u{2013}2 of 2] ===\nfn a() {}\n=== cairn:~/issues?limit=2 [2 of 9 issues] ===\nissue list here";
        let segs = read_segment_tokens(
            body,
            &["file:a.rs".into(), "cairn://p/RB/issues?limit=2".into()],
        );
        assert_eq!(segs.len(), 2);
        assert_eq!(segs[0].target, "file:a.rs");
        assert_eq!(segs[1].target, "cairn://p/RB/issues?limit=2");
        assert!(segs[1].tokens > 0);
    }

    #[test]
    fn header_suffixes_are_stripped_from_target() {
        let grep = read_segment_tokens("=== file:a.rs [3 matches] ===\nhit", &["file:a.rs".into()]);
        assert_eq!(grep[0].target, "file:a.rs");

        let lines = read_segment_tokens(
            "=== file:a.rs [lines 1\u{2013}20 of 99] ===\nbody",
            &["file:a.rs".into()],
        );
        assert_eq!(lines[0].target, "file:a.rs");

        let trunc =
            read_segment_tokens("=== file:a.rs [truncated] ===\nbody", &["file:a.rs".into()]);
        assert_eq!(trunc[0].target, "file:a.rs");
    }

    #[test]
    fn unframed_single_read_with_internal_uri_line_stays_whole() {
        // A single read is unframed; a bare `=== file:… ===` line in its content
        // must not be treated as a frame and mis-split the body.
        let body = "intro line\n=== file:other.rs ===\nmore content";
        let segs = read_segment_tokens(body, &["file:a.rs".into()]);
        assert_eq!(segs.len(), 1);
        assert_eq!(segs[0].target, "file:a.rs");
        let whole = read_segment_tokens("intro line\nmore content", &["file:a.rs".into()]);
        // The count covers the whole body, internal pseudo-header included.
        assert!(segs[0].tokens >= whole[0].tokens);
    }

    #[test]
    fn empty_body_yields_zero_token_segment() {
        let segs = read_segment_tokens("", &["file:a.rs".into()]);
        assert_eq!(segs.len(), 1);
        assert_eq!(segs[0].target, "file:a.rs");
        assert_eq!(segs[0].tokens, 0);
    }

    #[test]
    fn no_expected_targets_still_splits_on_headers() {
        let segs = read_segment_tokens("=== file:a.rs ===\nbody\n=== file:b.rs ===\nmore", &[]);
        assert_eq!(segs.len(), 2);
    }

    #[test]
    fn unexpected_header_line_in_content_is_not_a_boundary() {
        // The file content contains a `=== x ===` line that is not an input path;
        // with expected targets it must stay inside the section, not split it.
        let body = "=== file:a.rs ===\nprefix\n=== not a target ===\nsuffix";
        let segs = read_segment_tokens(body, &["file:a.rs".into()]);
        assert_eq!(segs.len(), 1);
        assert_eq!(segs[0].target, "file:a.rs");
    }

    #[test]
    fn trailing_batch_truncated_marker_is_dropped() {
        let body = "=== file:a.rs ===\nbody\n--- batch truncated: too big ---";
        let with = read_segment_tokens(body, &["file:a.rs".into()]);
        let without = read_segment_tokens("=== file:a.rs ===\nbody", &["file:a.rs".into()]);
        assert_eq!(with[0].tokens, without[0].tokens);
    }
}
