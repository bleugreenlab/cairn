//! Structured batch read: producers, the shared view layer, and the assembler.
//!
//! A `read_batch` callback carries the resolved target array. [`batch`] classifies
//! each target, runs its producer to a [`cairn_common::read::ReadSegment`], and
//! [`view`] windows/truncates each under one shared budget while emitting the
//! enriched `=== uri [suffix] ===` header and an always-valid continue footer.

pub mod batch;
pub mod view;

use cairn_common::read::{NaturalUnit, ReadSegment, SegmentKind, SegmentMeta};

pub use batch::handle_read_batch;

/// A producer outcome: a finished segment, or a fence suspension that must abort
/// the whole batch so the permission flow can re-dispatch it once approved.
///
/// `Segment` legitimately dwarfs `Suspended`: this is a short-lived, per-target
/// outcome that is `Segment` on every successful read, so boxing the common
/// variant would add an allocation on the hot path to satisfy a size heuristic.
#[allow(clippy::large_enum_variant)]
pub enum Produced {
    Segment(ReadSegment),
    Suspended(String),
}

/// Build an `Error`-kind segment carrying an inline failure message for one
/// target. A producer error never aborts the batch — it shows in place.
pub fn error_segment(uri: impl Into<String>, message: impl Into<String>) -> ReadSegment {
    ReadSegment::text(
        message.into(),
        SegmentMeta::new(uri, SegmentKind::Error, NaturalUnit::Line),
    )
}

/// Derive match/file counts from a rendered grep body.
///
/// With line numbers on (the default), a directory/single-file match line is
/// `path:N:text` and a context line is `path:N-text`; the `path` is captured
/// non-greedily up to the first `:<digits>:` (match) or `:<digits>-` (context)
/// boundary so the count reflects real matches, not context lines. A grep over
/// a materialized in-memory body (a rendered resource, web markdown, artifact,
/// or transcript) drops the path prefix entirely, so its match line is `N:text`
/// and its context line is `N-text`; those path-less forms are recognized too so
/// counts work uniformly across every grep target. Returns
/// `(match_count, file_count)` over the (already match-windowed) output;
/// path-less bodies have no file dimension and do not contribute to the file
/// count.
pub fn grep_counts(body: &str) -> (usize, usize) {
    use std::collections::HashSet;
    use std::sync::LazyLock;

    static MATCH_RE: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(r"^(.*?):\d+:").unwrap());
    static CONTEXT_RE: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(r"^(.*?):\d+-").unwrap());
    static BARE_MATCH_RE: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(r"^\d+:").unwrap());
    static BARE_CONTEXT_RE: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(r"^\d+-").unwrap());

    let mut matches = 0usize;
    let mut files: HashSet<&str> = HashSet::new();
    for line in body.lines() {
        if line == "--" {
            continue;
        }
        if let Some(caps) = MATCH_RE.captures(line) {
            matches += 1;
            files.insert(caps.get(1).map(|m| m.as_str()).unwrap_or(""));
        } else if BARE_MATCH_RE.is_match(line) {
            // Path-less materialized-body match line (`N:text`).
            matches += 1;
        } else if let Some(caps) = CONTEXT_RE.captures(line) {
            files.insert(caps.get(1).map(|m| m.as_str()).unwrap_or(""));
        } else if BARE_CONTEXT_RE.is_match(line) {
            // Path-less context line (`N-text`): no match, no file dimension.
        }
    }
    (matches, files.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grep_counts_distinguishes_matches_from_context() {
        // Two matches, surrounded by context lines and a separator. The count
        // must report matches (2), not match+context lines.
        let body = "src/a.rs:9-before\nsrc/a.rs:10:hit\nsrc/a.rs:11-after\n--\nsrc/b.rs:3:hit";
        let (matches, files) = grep_counts(body);
        assert_eq!(matches, 2);
        assert_eq!(files, 2);
    }

    #[test]
    fn grep_counts_handles_colon_in_path() {
        let body = "weird:path/file.rs:4:match";
        let (matches, files) = grep_counts(body);
        assert_eq!(matches, 1);
        assert_eq!(files, 1);
    }

    #[test]
    fn grep_counts_path_less_body_form() {
        // A materialized-body grep drops the path prefix: `N:text` is a match,
        // `N-text` is context. Counts must reflect matches only, and a path-less
        // body contributes no files.
        let body = "9-before\n10:hit\n11-after\n--\n14:hit again";
        let (matches, files) = grep_counts(body);
        assert_eq!(matches, 2);
        assert_eq!(files, 0);
    }
}
