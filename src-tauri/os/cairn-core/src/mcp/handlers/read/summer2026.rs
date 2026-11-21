//! The single-file grep segment of [`ReadComposition::Summer2026`]: the read
//! composition of the June and July 2026 builds that archived the first
//! gitcoord reads, before `a27231e2a737` (`head_limit` counts matches) and
//! `cb585de35f50` (the path as a heading) changed it in August (cairn/6792).
//!
//! A frozen decoder for rows those builds archived, reached only from
//! archival reconstruction when the current composition does not reproduce a
//! row's recorded sha. The search itself is shared
//! ([`search::collect_single_file_grep`]); what is frozen here is what those
//! builds did with its lines:
//!
//! - a content row carries its path on every line, `name:N:text` for a match
//!   and `name:N-text` for context, with `--` between context groups;
//! - `offset` and `head_limit` slice output LINES, context and separators
//!   included;
//! - the header's match count is read back off the sliced body by the regexes
//!   of that era, which is what the archived bytes were rendered from, so they
//!   are kept exactly, misreadings and all.
//!
//! The fair-share estimate of the same era lives with the composer, in
//! `storage::render`. Delete this module with `ReadComposition::Summer2026`.
//!
//! [`ReadComposition::Summer2026`]: crate::storage::render::ReadComposition::Summer2026

use std::collections::HashSet;
use std::sync::LazyLock;

use cairn_common::read::{NaturalUnit, ReadSegment, SegmentKind, SegmentMeta};
use cairn_symbols::search_util::{order_grep_lines, render_grep_lines, GrepLine};

use crate::mcp::handlers::read::error_segment;
use crate::mcp::handlers::search::{self, GrepPayload};

/// The grep segment the Summer 2026 builds produced for one archived file,
/// with `payload.output_mode` already resolved. A failed search is the same
/// error segment it was then.
pub(crate) fn grep_segment(
    uri: String,
    bytes: &[u8],
    label: &str,
    payload: &GrepPayload,
) -> ReadSegment {
    let lines = match search::collect_single_file_grep(bytes, label, payload) {
        Ok(lines) => lines,
        Err(error) => return error_segment(uri, error),
    };
    let output_mode = payload.output_mode.as_deref().unwrap_or("content");
    let show_line_numbers = payload.line_numbers.unwrap_or(true);
    let (before, after) = search::grep_context_window(payload);
    let context_requested = before > 0 || after > 0;
    let rendered = match output_mode {
        "content" => prefixed_rows(lines, show_line_numbers, context_requested),
        // The row modes rendered then exactly as they do now.
        _ => render_grep_lines(
            lines,
            output_mode,
            show_line_numbers,
            false,
            context_requested,
        ),
    };
    let body = slice_output_lines(
        rendered,
        &payload.pattern,
        payload.offset.unwrap_or(0) as usize,
        payload.head_limit,
    );
    let (match_count, file_count) = body_counts(&body, output_mode, &payload.pattern);
    let mut meta = SegmentMeta::new(uri, SegmentKind::Grep, NaturalUnit::Match);
    meta.match_count = match_count;
    meta.file_count = (output_mode == "files_with_matches").then_some(file_count);
    ReadSegment::text(body, meta)
}

/// Content rows with the path on each one.
fn prefixed_rows(mut lines: Vec<GrepLine>, show_line_numbers: bool, context: bool) -> String {
    order_grep_lines(&mut lines);
    let mut rendered: Vec<String> = Vec::with_capacity(lines.len());
    let mut previous: Option<(&str, u64)> = None;
    for line in &lines {
        if context
            && previous
                .is_some_and(|(path, number)| path != line.path || line.line_number > number + 1)
        {
            rendered.push("--".to_string());
        }
        let sep = if line.is_match { ':' } else { '-' };
        let (path, text) = (line.path.as_str(), line.text.as_str());
        rendered.push(
            match (
                path.is_empty(),
                show_line_numbers.then_some(line.line_number),
            ) {
                (true, Some(n)) => format!("{n}{sep}{text}"),
                (true, None) => text.to_string(),
                (false, Some(n)) => format!("{path}:{n}{sep}{text}"),
                (false, None) => format!("{path}{sep}{text}"),
            },
        );
        previous = Some((path, line.line_number));
    }
    rendered.join("\n")
}

/// The empty-result message, then `offset`/`head_limit` over output lines.
fn slice_output_lines(
    output: String,
    pattern: &str,
    offset: usize,
    head_limit: Option<u32>,
) -> String {
    if output.is_empty() {
        return search::grep_no_matches_body(pattern);
    }
    let lines: Vec<&str> = output.lines().collect();
    if offset >= lines.len() {
        return String::new();
    }
    let rest = &lines[offset..];
    let kept = match head_limit {
        Some(limit) => &rest[..rest.len().min(limit as usize)],
        None => rest,
    };
    kept.join("\n")
}

/// `(match_count, file_count)` read off a sliced body in the shape its output
/// mode wrote; `match_count` is `None` under `files_with_matches`.
fn body_counts(body: &str, output_mode: &str, pattern: &str) -> (Option<usize>, usize) {
    if body == search::grep_no_matches_body(pattern) {
        return match output_mode {
            "files_with_matches" => (None, 0),
            _ => (Some(0), 0),
        };
    }
    match output_mode {
        "files_with_matches" => (None, body.lines().filter(|line| !line.is_empty()).count()),
        "count" => {
            let (mut matches, mut files) = (0usize, 0usize);
            for line in body.lines() {
                let Some((_, tally)) = line.rsplit_once(':') else {
                    continue;
                };
                let Ok(tally) = tally.parse::<usize>() else {
                    continue;
                };
                matches += tally;
                files += 1;
            }
            (Some(matches), files)
        }
        _ => {
            let (matches, files) = content_counts(body);
            (Some(matches), files)
        }
    }
}

/// Matches and files in a content body. The path is taken non-greedily up to
/// the first `:<digits>:` (match) or `:<digits>-` (context), and a path-less
/// `N:` row is a match with no file.
fn content_counts(body: &str) -> (usize, usize) {
    static MATCH_RE: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(r"^(.*?):\d+:").unwrap());
    static CONTEXT_RE: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(r"^(.*?):\d+-").unwrap());
    static BARE_MATCH_RE: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(r"^\d+:").unwrap());

    let mut matches = 0usize;
    let mut files: HashSet<&str> = HashSet::new();
    for line in body.lines() {
        if line == "--" {
            continue;
        }
        if let Some(caps) = MATCH_RE.captures(line) {
            matches += 1;
            files.insert(caps.get(1).map_or("", |m| m.as_str()));
        } else if BARE_MATCH_RE.is_match(line) {
            matches += 1;
        } else if let Some(caps) = CONTEXT_RE.captures(line) {
            files.insert(caps.get(1).map_or("", |m| m.as_str()));
        }
    }
    (matches, files.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload(pattern: &str, head_limit: Option<u32>, context: Option<u32>) -> GrepPayload {
        GrepPayload {
            pattern: pattern.to_string(),
            path: None,
            glob: None,
            file_type: None,
            output_mode: Some("content".to_string()),
            context,
            after_context: None,
            before_context: None,
            context_alias: None,
            case_insensitive: None,
            line_numbers: None,
            head_limit,
            offset: None,
            multiline: None,
        }
    }

    /// The specimen's shape (cairn/6792, event 507f89cf): one match in a named
    /// file, its path on the row.
    #[test]
    fn a_content_row_carries_its_path() {
        let segment = grep_segment(
            "file:src/projects.rs?grep=attach_remote&head_limit=5".into(),
            b"fn a() {}\n    remote::attach_remote(\n",
            "projects.rs",
            &payload("attach_remote", Some(5), None),
        );
        assert_eq!(segment.body, "projects.rs:2:    remote::attach_remote(");
        assert_eq!(segment.meta.match_count, Some(1));
        assert_eq!(segment.meta.file_count, None);
        assert_eq!(segment.meta.grep_shown, None);
    }

    /// `head_limit` counted output lines, context and separators included, and
    /// the header counted only the matches left in the slice.
    #[test]
    fn head_limit_slices_output_lines_and_the_count_follows_the_slice() {
        let bytes = b"x1\nctx\nx2\nctx\nctx\nctx\nx3\n";
        let segment = grep_segment(
            "file:f.txt?grep=^x&-C=1&head_limit=4".into(),
            bytes,
            "f.txt",
            &payload("^x", Some(4), Some(1)),
        );
        assert_eq!(
            segment.body,
            "f.txt:1:x1\nf.txt:2-ctx\nf.txt:3:x2\nf.txt:4-ctx"
        );
        assert_eq!(segment.meta.match_count, Some(2));
    }

    #[test]
    fn no_match_is_the_no_matches_message_counted_as_zero() {
        let segment = grep_segment(
            "file:f.txt?grep=absent".into(),
            b"nothing here\n",
            "f.txt",
            &payload("absent", None, None),
        );
        assert_eq!(segment.body, "No matches found for pattern 'absent'");
        assert_eq!(segment.meta.match_count, Some(0));
    }
}
