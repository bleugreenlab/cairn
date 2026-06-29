//! Shared view layer for batch reads.
//!
//! Single source of truth for turning a produced [`ReadSegment`] into its final
//! text block: budget truncation on line boundaries, the single-huge-line
//! character-fallback continuation (the dead-end fix), the always-valid continue
//! footer, and the enriched `=== uri [suffix] ===` header. The per-source line
//! extraction (numbered file text vs plain web/resource markdown) is thin and
//! lives with the producers; everything that was previously duplicated in the
//! cairn-cli composer lives here.

use cairn_common::query::{encode_query_params, split_target_query, QueryParam};
use cairn_common::read::{
    Affordance, ImageBlock, NaturalUnit, ReadBatchEnvelope, ReadSegment, SegmentKind, SegmentMeta,
};

/// Single backend budget for one whole `read` batch, applied once at the
/// assembler. Kept at ~45_000 characters: it approximates the agent-CLI
/// tool-result ceiling (~25k tokens for Claude Code; at ~2.2 chars/token that is
/// ~45–55k chars) with headroom for headers, footers, and affordance overhead.
/// The unit is characters because a real tokenizer metric is a later child; this
/// constant names the token economics it stands in for.
pub const READ_BATCH_CHAR_BUDGET: usize = 45_000;

/// Resolve a possibly-negative offset against a total count (negative = tail).
pub fn resolve_offset(offset: Option<i64>, total: usize) -> usize {
    match offset.unwrap_or(0) {
        raw if raw < 0 => total.saturating_sub(raw.unsigned_abs() as usize),
        raw => (raw as usize).min(total),
    }
}

/// A plain (un-numbered) line window over `raw`, used by the web and resource
/// producers. The file producer windows + numbers in [`super`]'s renderer.
pub struct LineWindow {
    pub body: String,
    pub total: usize,
    pub offset: usize,
    pub shown: usize,
}

/// Window `raw` by lines (offset/limit, negative offset = tail), without
/// numbering. Shared by web and resource segments.
pub fn window_text_lines(raw: &str, offset: Option<i64>, limit: Option<usize>) -> LineWindow {
    // Generated resource and web markdown often ends with one or more formatting
    // newlines. They should not create a phantom final page after the whole
    // meaningful body has already been shown; internal blank lines remain real
    // lines because only the tail is normalized.
    let normalized = raw.trim_end_matches(['\r', '\n']);
    let lines: Vec<&str> = normalized.lines().collect();
    let total = lines.len();
    let start = resolve_offset(offset, total);
    if start >= total {
        return LineWindow {
            body: String::new(),
            total,
            offset: start,
            shown: 0,
        };
    }
    let end = match limit {
        Some(n) => (start + n).min(total),
        None => total,
    };
    let slice = &lines[start..end];
    LineWindow {
        body: slice.join("\n"),
        total,
        offset: start,
        shown: slice.len(),
    }
}

/// Water-filling fair share of `total_budget` across segment lengths: short
/// segments take exactly what they need and donate the remainder to longer ones.
pub fn fair_share_budgets(lengths: &[usize], total_budget: usize) -> Vec<usize> {
    if lengths.is_empty() {
        return Vec::new();
    }
    let mut budgets = vec![0; lengths.len()];
    let mut remaining_budget = total_budget;
    let mut remaining_count = lengths.len();
    let mut by_length: Vec<(usize, usize)> = lengths.iter().copied().enumerate().collect();
    by_length.sort_by_key(|(_, length)| *length);

    let mut index = 0;
    while index < by_length.len() && remaining_count > 0 {
        let floor = remaining_budget / remaining_count;
        let (original_index, length) = by_length[index];
        if length > floor {
            break;
        }
        budgets[original_index] = length;
        remaining_budget = remaining_budget.saturating_sub(length);
        remaining_count -= 1;
        index += 1;
    }
    if remaining_count > 0 {
        let floor = remaining_budget / remaining_count;
        let extra = remaining_budget % remaining_count;
        for (extra_index, (original_index, _)) in by_length[index..].iter().enumerate() {
            budgets[*original_index] = floor + usize::from(extra_index < extra);
        }
    }
    budgets
}

// ---------------------------------------------------------------------------
// Continue-URI helpers (ported from the cairn-cli composer).
// ---------------------------------------------------------------------------

fn target_with_query_param(uri: &str, key: &str, value: usize) -> String {
    let split = match split_target_query(uri) {
        Ok(split) => split,
        Err(_) => return format!("{uri}?{key}={value}"),
    };
    let mut params: Vec<QueryParam> = split
        .params
        .into_iter()
        .filter(|param| param.key != key)
        .collect();
    params.push(QueryParam {
        key: key.to_string(),
        value: value.to_string(),
    });
    format!("{}?{}", split.identity, encode_query_params(&params))
}

fn target_without_query_param(uri: &str, key: &str) -> String {
    let split = match split_target_query(uri) {
        Ok(split) => split,
        Err(_) => return uri.to_string(),
    };
    let params: Vec<QueryParam> = split
        .params
        .into_iter()
        .filter(|param| param.key != key)
        .collect();
    if params.is_empty() {
        split.identity
    } else {
        format!("{}?{}", split.identity, encode_query_params(&params))
    }
}

fn target_head_limit(uri: &str) -> usize {
    split_target_query(uri)
        .ok()
        .and_then(|split| {
            split
                .params
                .iter()
                .rev()
                .find(|param| param.key == "head_limit")
                .and_then(|param| param.value.parse::<usize>().ok())
        })
        .unwrap_or(0)
}

/// Continue URI for a line/file-unit window. When `char_offset` is set, the
/// single-huge-line fallback fired: resume at the *same* line and skip the shown
/// characters, so the continue strictly advances within the line.
fn line_continue_target(
    uri: &str,
    offset: usize,
    shown_lines: usize,
    original_limit: Option<usize>,
    char_offset: Option<usize>,
) -> String {
    if let Some(chars) = char_offset {
        let target = target_with_query_param(uri, "offset", offset);
        return target_with_query_param(&target, "char_offset", chars);
    }
    let target = target_with_query_param(uri, "offset", offset + shown_lines);
    match original_limit {
        None => target_without_query_param(&target, "limit"),
        Some(limit) if shown_lines < limit => {
            target_with_query_param(&target, "limit", limit - shown_lines)
        }
        Some(limit) => target_with_query_param(&target, "limit", limit),
    }
}

fn grep_continue_target(uri: &str, shown_lines: usize) -> String {
    let next_head_limit = target_head_limit(uri).max(shown_lines) + shown_lines.max(1);
    target_with_query_param(uri, "head_limit", next_head_limit)
}

fn line_footer(
    uri: &str,
    offset: usize,
    shown_lines: usize,
    total_lines: usize,
    original_limit: Option<usize>,
    budget_truncated: bool,
    char_offset: Option<usize>,
) -> String {
    let start_line = offset + 1;
    let end_line = offset + shown_lines;
    let reason = if budget_truncated {
        "output truncated to fit budget; "
    } else {
        ""
    };
    format!(
        "[lines {}\u{2013}{} of {} — {}continue: {}]",
        start_line,
        end_line,
        total_lines,
        reason,
        line_continue_target(uri, offset, shown_lines, original_limit, char_offset),
    )
}

/// Continue footer for a `Record` page: advance `offset` by the items already
/// emitted. Only called when a further page exists and the total is known.
fn record_footer(uri: &str, offset: usize, shown: usize, total: usize, noun: &str) -> String {
    format!(
        "[{} {}\u{2013}{} of {} — continue: {}]",
        noun,
        offset + 1,
        offset + shown,
        total,
        target_with_query_param(uri, "offset", offset + shown),
    )
}

fn grep_footer(uri: &str, shown_lines: usize) -> String {
    format!(
        "[first {} matching lines shown — output truncated to fit budget; continue: {}]",
        shown_lines,
        grep_continue_target(uri, shown_lines),
    )
}

// ---------------------------------------------------------------------------
// Enriched header suffix grammar.
// ---------------------------------------------------------------------------

fn line_unit(meta: &SegmentMeta) -> bool {
    matches!(meta.natural_unit, NaturalUnit::Line | NaturalUnit::File)
}

/// The terse suffix projection for a segment, or `None` when no useful count
/// fits (directory, image, error, or an empty body).
fn segment_suffix(meta: &SegmentMeta, shown: usize) -> Option<String> {
    // A pushdown collection reports `N of M issues`/`messages` in its natural
    // unit. When the total is not cheaply knowable (messages), the suffix is
    // omitted and the header stays bare.
    if matches!(meta.natural_unit, NaturalUnit::Record | NaturalUnit::Turn) {
        let total = meta.total_units?;
        let noun = meta.unit_noun.as_deref()?;
        return Some(format!("{shown} of {total} {noun}"));
    }
    match meta.kind {
        SegmentKind::File | SegmentKind::Web | SegmentKind::Resource => {
            let total = meta.total_units?;
            if total == 0 {
                return None;
            }
            Some(format!(
                "lines {}\u{2013}{} of {}",
                meta.offset + 1,
                meta.offset + shown,
                total
            ))
        }
        SegmentKind::Grep => {
            let matches = meta.match_count?;
            match meta.file_count {
                Some(files) => Some(format!("{matches} matches in {files} files")),
                None => Some(format!("{matches} matches")),
            }
        }
        SegmentKind::Glob => Some(format!("{} files", meta.file_count?)),
        SegmentKind::Directory | SegmentKind::Image | SegmentKind::Error => None,
    }
}

fn header(meta: &SegmentMeta, shown: usize, truncated: bool) -> String {
    match segment_suffix(meta, shown) {
        Some(suffix) => format!("=== {} [{}] ===", meta.uri, suffix),
        None if truncated => format!("=== {} [truncated] ===", meta.uri),
        None => format!("=== {} ===", meta.uri),
    }
}

// ---------------------------------------------------------------------------
// Segment rendering.
// ---------------------------------------------------------------------------

/// A segment rendered to its final composed text block plus finalized metadata.
pub struct RenderedSegment {
    pub text: String,
    pub meta: SegmentMeta,
    pub images: Vec<ImageBlock>,
    pub affordance: Option<Affordance>,
}

fn safe_char_boundary(text: &str, mut byte: usize) -> usize {
    if byte >= text.len() {
        return text.len();
    }
    while byte > 0 && !text.is_char_boundary(byte) {
        byte -= 1;
    }
    byte
}

/// The numbering prefix length of a file-text line (`"{:>6}\t"` and wider), or 0
/// for un-numbered bodies. Used to derive a text-relative `char_offset`.
fn numbering_prefix_len(line: &str) -> usize {
    line.find('\t').map(|tab| tab + 1).unwrap_or(0)
}

struct BodyCut {
    body: String,
    shown_lines: usize,
    budget_truncated: bool,
    char_offset: Option<usize>,
}

/// Cut `body` to `budget` chars on a line boundary. When even the first line
/// alone exceeds the budget, fall back to a character-truncated prefix of that
/// line and report a text-relative `char_offset` so the continue URI advances.
fn cut_body(body: &str, budget: usize, numbered: bool, logical_lines: Option<usize>) -> BodyCut {
    if body.len() <= budget {
        return BodyCut {
            body: body.to_string(),
            shown_lines: logical_lines.unwrap_or_else(|| body.lines().count()),
            budget_truncated: false,
            char_offset: None,
        };
    }
    // Keep only whole lines that fit. When at least one newline precedes the
    // budget, cut there; the first line then fits and no char fallback is needed.
    let safe_end = safe_char_boundary(body, budget);
    if let Some(newline) = body[..safe_end].rfind('\n') {
        let cut = &body[..newline];
        return BodyCut {
            body: cut.to_string(),
            shown_lines: cut.lines().count().max(1),
            budget_truncated: true,
            char_offset: None,
        };
    }
    // No complete line fits: the first line alone exceeds the budget. Character
    // fallback — show a prefix and report a text-relative char_offset.
    let first_line = body.lines().next().unwrap_or(body);
    let prefix_len = if numbered {
        numbering_prefix_len(first_line)
    } else {
        0
    };
    // Keep at least one text character so the continue URI strictly advances.
    let min_end = safe_char_boundary(first_line, (prefix_len + 1).min(first_line.len()));
    let mut end = safe_char_boundary(first_line, budget.min(first_line.len()));
    if end < min_end {
        end = min_end;
    }
    let shown = &first_line[..end];
    let char_offset = shown
        .get(prefix_len..)
        .map(|t| t.chars().count())
        .unwrap_or(0);
    BodyCut {
        body: shown.to_string(),
        shown_lines: 1,
        budget_truncated: true,
        char_offset: Some(char_offset.max(1)),
    }
}

/// Render one produced segment to its final text block under `budget` characters,
/// emitting the enriched header, body, an always-valid continue footer, and any
/// issue history. Updates `meta.shown_units`, `truncated`, and `char_continuation`.
pub fn render_segment(seg: ReadSegment, budget: usize) -> RenderedSegment {
    if matches!(
        seg.meta.natural_unit,
        NaturalUnit::Record | NaturalUnit::Turn
    ) {
        return render_record_segment(seg, budget);
    }
    let ReadSegment {
        body,
        affordance,
        images,
        history,
        mut meta,
    } = seg;

    let numbered = matches!(meta.kind, SegmentKind::File);
    let history_suffix = history
        .as_deref()
        .filter(|h| !h.is_empty())
        .map(|h| format!("\n\n{h}"))
        .unwrap_or_default();

    let logical_full_lines = (meta.shown_units > 0).then_some(meta.shown_units);
    let full_lines = logical_full_lines.unwrap_or_else(|| body.lines().count());
    // A line/file window that ends before EOF needs a footer even when the
    // budget was not the limiting factor.
    let window_short = line_unit(&meta)
        && meta
            .total_units
            .is_some_and(|total| meta.offset + full_lines < total);

    let mut content_budget = budget;
    loop {
        let header_estimate = header(&meta, full_lines, true).len();
        let body_budget = content_budget.saturating_sub(header_estimate + 1 + history_suffix.len());
        let cut = cut_body(&body, body_budget, numbered, logical_full_lines);
        let truncated = cut.budget_truncated || window_short || cut.char_offset.is_some();

        let head = header(&meta, cut.shown_lines, truncated);
        let mut candidate = if cut.body.is_empty() {
            head.clone()
        } else {
            format!("{head}\n{}", cut.body)
        };

        let footer = match meta.natural_unit {
            NaturalUnit::Match => {
                if cut.budget_truncated {
                    Some(grep_footer(&meta.uri, cut.shown_lines))
                } else {
                    None
                }
            }
            // Record/turn pages are rendered by render_record_segment, never here.
            NaturalUnit::Record | NaturalUnit::Turn => None,
            NaturalUnit::Line | NaturalUnit::File => {
                let total = meta.total_units.unwrap_or(meta.offset + cut.shown_lines);
                if cut.budget_truncated
                    || cut.char_offset.is_some()
                    || meta.offset + cut.shown_lines < total
                {
                    Some(line_footer(
                        &meta.uri,
                        meta.offset,
                        cut.shown_lines,
                        total,
                        meta.limit,
                        cut.budget_truncated,
                        cut.char_offset,
                    ))
                } else {
                    None
                }
            }
        };
        if let Some(footer) = &footer {
            candidate.push('\n');
            candidate.push_str(footer);
        }
        candidate.push_str(&history_suffix);

        if candidate.len() <= budget || content_budget == 0 || cut.shown_lines == 0 {
            meta.shown_units = cut.shown_lines;
            meta.truncated = truncated;
            meta.char_continuation = cut.char_offset.is_some();
            return RenderedSegment {
                text: candidate,
                meta,
                images,
                affordance,
            };
        }
        content_budget = content_budget.saturating_sub(candidate.len() - budget + 1);
    }
}

/// Render one segment's standalone section text (header + body + footer) — the
/// exact bytes it contributes between separators in a composed batch. The budget
/// is effectively unbounded, so a section that was not truncated in its original
/// batch renders byte-identically here. Shared by archival's write side (the
/// needle it searches for in the stored composed result) and reconstruction (the
/// fill it splices back into a hybrid-read skeleton placeholder), exactly as
/// [`assemble`] is shared between the live and reconstructed composition.
pub(crate) fn render_section_text(seg: ReadSegment) -> String {
    render_segment(seg, usize::MAX).text
}

/// Render a `Record` collection page. The body is already item-windowed by the
/// producer, so the item counts (`shown`/`total`) come from `meta`, never from
/// body line counts. Budget truncation on line boundaries is only a safety net;
/// the agent's `limit` is what bounds the page. The continue footer advances
/// `offset` by the items the producer emitted.
fn render_record_segment(seg: ReadSegment, budget: usize) -> RenderedSegment {
    let ReadSegment {
        body,
        affordance,
        images,
        history,
        mut meta,
    } = seg;

    let history_suffix = history
        .as_deref()
        .filter(|h| !h.is_empty())
        .map(|h| format!("\n\n{h}"))
        .unwrap_or_default();

    let shown = meta.shown_units;
    let more = meta
        .total_units
        .is_some_and(|total| meta.offset + shown < total);

    let header_estimate = header(&meta, shown, true).len();
    let body_budget = budget.saturating_sub(header_estimate + 1 + history_suffix.len());
    let cut = cut_body(&body, body_budget, false, None);
    let truncated = cut.budget_truncated || more;

    let head = header(&meta, shown, truncated);
    let mut text = if cut.body.is_empty() {
        head
    } else {
        format!("{head}\n{}", cut.body)
    };

    if more {
        if let (Some(total), Some(noun)) = (meta.total_units, meta.unit_noun.as_deref()) {
            text.push('\n');
            text.push_str(&record_footer(&meta.uri, meta.offset, shown, total, noun));
        }
    }
    text.push_str(&history_suffix);

    meta.truncated = truncated;
    RenderedSegment {
        text,
        meta,
        images,
        affordance,
    }
}

// ---------------------------------------------------------------------------
// Batch assembly.
// ---------------------------------------------------------------------------

/// Estimated untruncated length of a segment's text block, for fair-share
/// budgeting: header (with suffix slack), body, a footer allowance, and history.
pub(crate) fn estimate_len(segment: &ReadSegment) -> usize {
    let header = format!("=== {} ===", segment.meta.uri).len() + 28;
    let footer = 220;
    let history = segment.history.as_ref().map(|h| h.len() + 2).unwrap_or(0);
    header + usize::from(!segment.body.is_empty()) + segment.body.len() + footer + history
}

/// Compose produced segments into one envelope under a single budget. Affordances
/// are deduped by `(kind, block)` and appended once after all content, so
/// truncation can never cut them.
///
/// Shared by the live `read_batch` dispatch ([`super::batch`]) and archival
/// reconstruction ([`crate::archival::reconstruct`]): both feed a `Vec<ReadSegment>`
/// through this one composer so a reconstructed read reproduces the live envelope
/// byte-for-byte.
pub(crate) fn assemble(segments: Vec<ReadSegment>) -> ReadBatchEnvelope {
    let lengths: Vec<usize> = segments.iter().map(estimate_len).collect();
    let separator_budget = segments.len().saturating_sub(1);
    let total_budget = READ_BATCH_CHAR_BUDGET.saturating_sub(separator_budget);
    let budgets = fair_share_budgets(&lengths, total_budget);

    let mut text = String::new();
    let mut images = Vec::new();
    let mut metas: Vec<SegmentMeta> = Vec::new();
    let mut affordances: Vec<Affordance> = Vec::new();

    for (segment, budget) in segments.into_iter().zip(budgets) {
        let rendered = render_segment(segment, budget);
        if !text.is_empty() {
            text.push('\n');
        }
        text.push_str(&rendered.text);
        images.extend(rendered.images);
        metas.push(rendered.meta);
        if let Some(affordance) = rendered.affordance {
            if !affordances.iter().any(|existing| {
                existing.kind == affordance.kind && existing.block == affordance.block
            }) {
                affordances.push(affordance);
            }
        }
    }

    for affordance in affordances {
        text.push_str("\n\n");
        text.push_str(&affordance.block);
    }

    ReadBatchEnvelope {
        text,
        images,
        segments: metas,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file_seg(uri: &str, numbered_body: &str, total: usize, offset: usize) -> ReadSegment {
        let mut meta = SegmentMeta::new(uri, SegmentKind::File, NaturalUnit::Line);
        meta.total_units = Some(total);
        meta.offset = offset;
        ReadSegment::text(numbered_body, meta)
    }

    #[test]
    fn full_small_read_emits_line_suffix_and_no_footer() {
        let body = "     1\ta\n     2\tb\n     3\tc";
        let r = render_segment(file_seg("file:x.rs", body, 3, 0), 10_000);
        assert!(r
            .text
            .starts_with("=== file:x.rs [lines 1\u{2013}3 of 3] ==="));
        assert!(!r.text.contains("continue:"));
        assert!(!r.meta.truncated);
    }

    #[test]
    fn window_before_eof_emits_footer() {
        let body = "     1\ta\n     2\tb";
        let r = render_segment(file_seg("file:x.rs", body, 9, 0), 10_000);
        assert!(r
            .text
            .contains("[lines 1\u{2013}2 of 9 — continue: file:x.rs?offset=2]"));
    }

    #[test]
    fn budget_truncates_on_line_boundary_with_footer() {
        let mut body = String::new();
        for i in 1..=200 {
            body.push_str(&format!("{:>6}\t{}\n", i, "x".repeat(80)));
        }
        let body = body.trim_end().to_string();
        let r = render_segment(file_seg("file:big.rs", &body, 200, 0), 1_500);
        assert!(r.text.len() <= 1_500);
        assert!(r.meta.truncated);
        assert!(r.text.contains("output truncated to fit budget"));
        // The continue URI advances past the shown lines.
        assert!(r.text.contains("offset="));
    }

    #[test]
    fn single_huge_line_uses_char_offset_and_advances() {
        // One numbered line far larger than the budget: the dead-end case.
        let line = format!("{:>6}\t{}", 1, "y".repeat(5_000));
        let r = render_segment(file_seg("file:huge.rs", &line, 1, 0), 500);
        assert!(r.text.len() <= 500);
        assert!(r.meta.char_continuation);
        assert!(r.text.contains("char_offset="));
        // The reported char_offset is > 0 so re-reading strictly advances.
        let marker = "char_offset=";
        let idx = r.text.find(marker).unwrap() + marker.len();
        let digits: String = r.text[idx..]
            .chars()
            .take_while(|c| c.is_ascii_digit())
            .collect();
        assert!(digits.parse::<usize>().unwrap() >= 1);
    }

    #[test]
    fn grep_suffix_and_directory_form() {
        let mut meta = SegmentMeta::new("file:src?grep=foo", SegmentKind::Grep, NaturalUnit::Match);
        meta.match_count = Some(42);
        meta.file_count = Some(7);
        let r = render_segment(
            ReadSegment::text("src/a.rs:1:foo\nsrc/b.rs:2:foo", meta),
            10_000,
        );
        assert!(r
            .text
            .starts_with("=== file:src?grep=foo [42 matches in 7 files] ==="));
    }

    #[test]
    fn grep_single_file_suffix_omits_files() {
        let mut meta =
            SegmentMeta::new("file:a.rs?grep=foo", SegmentKind::Grep, NaturalUnit::Match);
        meta.match_count = Some(3);
        meta.file_count = None;
        let r = render_segment(ReadSegment::text("a.rs:1:foo", meta), 10_000);
        assert!(r.text.starts_with("=== file:a.rs?grep=foo [3 matches] ==="));
    }

    #[test]
    fn glob_suffix_counts_files() {
        let mut meta = SegmentMeta::new(
            "file:src?glob=**/*.rs",
            SegmentKind::Glob,
            NaturalUnit::File,
        );
        meta.file_count = Some(12);
        meta.total_units = Some(12);
        let r = render_segment(ReadSegment::text("a.rs\nb.rs", meta), 10_000);
        assert!(r
            .text
            .starts_with("=== file:src?glob=**/*.rs [12 files] ==="));
    }

    #[test]
    fn empty_file_has_bare_header() {
        let r = render_segment(file_seg("file:empty.rs", "", 0, 0), 10_000);
        assert_eq!(r.text, "=== file:empty.rs ===");
    }

    #[test]
    fn glob_uri_with_brackets_is_not_mistaken_for_suffix() {
        // A glob URI legitimately contains brackets; the suffix is appended after
        // a space and the URI's own brackets stay intact.
        let mut meta = SegmentMeta::new(
            "file:src?glob=**/[abc].rs",
            SegmentKind::Glob,
            NaturalUnit::File,
        );
        meta.file_count = Some(2);
        let r = render_segment(ReadSegment::text("a.rs", meta), 10_000);
        assert!(r
            .text
            .starts_with("=== file:src?glob=**/[abc].rs [2 files] ==="));
    }

    fn record_seg(
        uri: &str,
        body: &str,
        total: Option<usize>,
        shown: usize,
        noun: Option<&str>,
        offset: usize,
    ) -> ReadSegment {
        let mut meta = SegmentMeta::new(uri, SegmentKind::Resource, NaturalUnit::Record);
        meta.total_units = total;
        meta.shown_units = shown;
        meta.unit_noun = noun.map(str::to_string);
        meta.offset = offset;
        ReadSegment::text(body, meta)
    }

    #[test]
    fn record_segment_emits_item_suffix_and_paging_footer() {
        let r = record_seg(
            "cairn://p/CAIRN/issues?limit=10",
            "- issue one\n- issue two",
            Some(142),
            10,
            Some("issues"),
            0,
        );
        let out = render_segment(r, 10_000);
        assert!(out
            .text
            .starts_with("=== cairn://p/CAIRN/issues?limit=10 [10 of 142 issues] ==="));
        // A further page exists: the continue advances offset by the shown items.
        assert!(out
            .text
            .contains("continue: cairn://p/CAIRN/issues?limit=10&offset=10"));
        assert!(out.meta.truncated);
    }

    #[test]
    fn record_segment_last_page_has_no_footer() {
        let r = record_seg(
            "cairn://p/CAIRN/issues?offset=140&limit=10",
            "- tail issue",
            Some(142),
            2,
            Some("issues"),
            140,
        );
        let out = render_segment(r, 10_000);
        assert!(out
            .text
            .starts_with("=== cairn://p/CAIRN/issues?offset=140&limit=10 [2 of 142 issues] ==="));
        assert!(!out.text.contains("continue:"));
    }

    #[test]
    fn record_segment_without_total_has_bare_header() {
        // Messages: total is not cheaply knowable, so the suffix and footer are
        // both omitted and the header stays bare.
        let r = record_seg(
            "cairn://p/CAIRN/1/messages?limit=5",
            "[ts] sender: hello",
            None,
            3,
            Some("messages"),
            0,
        );
        let out = render_segment(r, 10_000);
        assert_eq!(
            out.text,
            "=== cairn://p/CAIRN/1/messages?limit=5 ===\n[ts] sender: hello"
        );
    }

    #[test]
    fn turn_segment_emits_turn_suffix_and_paging_footer() {
        let mut meta = SegmentMeta::new(
            "cairn://p/CAIRN/1/1/builder/chat?offset=1&limit=1",
            SegmentKind::Resource,
            NaturalUnit::Turn,
        );
        meta.total_units = Some(3);
        meta.shown_units = 1;
        meta.unit_noun = Some("turns".to_string());
        meta.offset = 1;
        meta.limit = Some(1);

        let out = render_segment(ReadSegment::text("# chat\n\n## Turn 2\nbody", meta), 10_000);

        assert!(out.text.starts_with(
            "=== cairn://p/CAIRN/1/1/builder/chat?offset=1&limit=1 [1 of 3 turns] ==="
        ));
        assert!(out
            .text
            .contains("continue: cairn://p/CAIRN/1/1/builder/chat?limit=1&offset=2"));
    }

    #[test]
    fn window_text_lines_tails_on_negative_offset() {
        let w = window_text_lines("a\nb\nc\nd", Some(-2), None);
        assert_eq!(w.body, "c\nd");
        assert_eq!(w.total, 4);
        assert_eq!(w.offset, 2);
        assert_eq!(w.shown, 2);
    }

    #[test]
    fn window_text_lines_does_not_count_trailing_format_newlines() {
        let w = window_text_lines("# Question\n\n## Response\n\nanswer\n\n", None, None);
        assert_eq!(w.body, "# Question\n\n## Response\n\nanswer");
        assert_eq!(w.total, 5);
        assert_eq!(w.offset, 0);
        assert_eq!(w.shown, 5);
    }

    #[test]
    fn internal_blank_line_window_counts_as_one_line_and_terminates() {
        let w = window_text_lines("first\n\nthird", Some(1), Some(1));
        assert_eq!(w.body, "");
        assert_eq!(w.total, 3);
        assert_eq!(w.offset, 1);
        assert_eq!(w.shown, 1);

        let mut meta = SegmentMeta::new(
            "cairn://p/CAIRN/1/1/planner/questions/q-1?offset=1&limit=1",
            SegmentKind::Resource,
            NaturalUnit::Line,
        );
        meta.total_units = Some(w.total);
        meta.shown_units = w.shown;
        meta.offset = w.offset;
        meta.limit = Some(1);

        let out = render_segment(ReadSegment::text(w.body, meta), 10_000);

        assert_eq!(
            out.text,
            "=== cairn://p/CAIRN/1/1/planner/questions/q-1?offset=1&limit=1 [lines 2\u{2013}2 of 3] ===\n[lines 2\u{2013}2 of 3 — continue: cairn://p/CAIRN/1/1/planner/questions/q-1?offset=2&limit=1]"
        );
        assert_eq!(out.meta.shown_units, 1);
        assert!(out.meta.truncated);
    }
}
