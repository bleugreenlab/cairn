//! Shared view/render layer for batch reads, plus the archived-file render seam.
//!
//! Moved down from the mcp read handler into `storage` so `storage` carries no
//! upward edge into the mcp layer (it will later descend into `cairn-db`); the mcp read
//! layer re-exports it under the original `view` name so its consumers compile
//! unchanged. This module is the single source of truth for turning a produced
//! [`ReadSegment`] into its final
//! text block: budget truncation on line boundaries, the single-huge-line
//! character-fallback continuation (the dead-end fix), the always-valid continue
//! footer, and the enriched `=== uri [suffix] ===` header. The per-source line
//! extraction (numbered file text vs plain web/resource markdown) is thin and
//! lives with the producers; everything that was previously duplicated in the
//! cairn-cmd composer lives here.

use cairn_common::query::{encode_query_params, split_target_query, QueryParam};
use cairn_common::token_meter::TokenMeter;
use std::sync::OnceLock;

use cairn_common::read::{
    Affordance, ImageBlock, NaturalUnit, ReadBatchEnvelope, ReadSegment, SegmentKind, SegmentMeta,
};

/// Build an `Error`-kind segment carrying an inline failure message for one
/// target. A producer error never aborts a batch — it shows in place. Lives here
/// beside the view layer (moved down from the mcp read handler) so `storage`
/// reconstruction can build the same labeled-coordinate stub segments the live
/// read builds, without an upward edge into the mcp layer.
pub fn error_segment(uri: impl Into<String>, message: impl Into<String>) -> ReadSegment {
    ReadSegment::text(
        message.into(),
        SegmentMeta::new(uri, SegmentKind::Error, NaturalUnit::Line),
    )
}

/// Build a `Duplicate`-kind segment carrying the existing human-facing
/// suppression explanation. Suppression is not a producer failure and must stay
/// distinguishable in both structured metadata and composed text.
pub fn duplicate_segment(uri: impl Into<String>, message: impl Into<String>) -> ReadSegment {
    ReadSegment::text(
        message.into(),
        SegmentMeta::new(uri, SegmentKind::Duplicate, NaturalUnit::Line),
    )
}

/// The render seam for an archived read's file bytes.
///
/// Archival reconstruction lives in `storage` (it will descend into `cairn-db`),
/// but reproducing a read's rendered [`ReadSegment`] requires the mcp read
/// layer's byte renderers and in-process ast-grep/outline machinery, which sit
/// above `storage`. The mcp layer registers an implementation at `Orchestrator`
/// construction; reconstruction calls it through this registry. All boundary
/// types already live in [`cairn_common::read`], so the seam is clean both ways.
pub trait ArchivedFileRenderer: Send + Sync {
    fn produce_archived_file_segment(
        &self,
        target: &str,
        bytes: &[u8],
    ) -> Result<ReadSegment, String>;
}

static ARCHIVED_FILE_RENDERER: OnceLock<&'static dyn ArchivedFileRenderer> = OnceLock::new();

/// Register the archived-file renderer. Idempotent (first writer wins); the mcp
/// read layer calls this at every `Orchestrator` construction. A binary that
/// reconstructs archived reads WITHOUT registering a renderer degrades every
/// archived read to a labeled coordinate stub — never a panic.
pub fn set_archived_file_renderer(renderer: &'static dyn ArchivedFileRenderer) {
    let _ = ARCHIVED_FILE_RENDERER.set(renderer);
}

/// The registered archived-file renderer, or `None` when none has been
/// registered (reconstruction then degrades archived reads to stubs).
pub(crate) fn archived_file_renderer() -> Option<&'static dyn ArchivedFileRenderer> {
    ARCHIVED_FILE_RENDERER.get().copied()
}

/// Single backend token budget for one whole `read` batch.
pub const READ_BATCH_TOKEN_BUDGET: u32 = 20_000;

/// Maximum rendered bytes in a `read` envelope before the client gets a chance
/// to replace it with a persisted-output preview.
///
/// Measured with the Claude Agent SDK on 2026-08-22: 16.4 KB passed through and
/// 49.8 KB was persisted (with further failures at 50.8 and 51.7 KB). 45,000
/// bytes preserves margin below that empirical bracket and for JSON escaping in
/// the surrounding MCP payload. This is deliberately a byte count, not a Unicode
/// scalar count: the downstream boundary is serialized bytes.
pub const CLIENT_TOOL_RESULT_BYTE_CEILING: usize = 45_000;

const AFFORDANCE_ELISION: &str =
    "[cairn: affordance metadata omitted to keep this read below the 45,000-byte client ceiling]";
const HISTORY_ELISION: &str =
    "[cairn: issue history omitted to keep this read below the 45,000-byte client ceiling]";
const METADATA_ELISION: &str = "[cairn: affordance metadata and issue history omitted to keep this read below the 45,000-byte client ceiling]";

fn bounded_uri_label(uri: &str) -> String {
    const MAX_URI_BYTES: usize = 512;
    if uri.len() <= MAX_URI_BYTES {
        return uri.to_string();
    }
    let end = safe_char_boundary(uri, MAX_URI_BYTES);
    format!("{}…", &uri[..end])
}

#[cfg(any(test, feature = "test-utils"))]
pub(crate) struct TestByteMeter;

#[cfg(any(test, feature = "test-utils"))]
impl TokenMeter for TestByteMeter {
    fn count(&self, text: &str) -> u32 {
        text.len().try_into().unwrap_or(u32::MAX)
    }

    fn cut(&self, text: &str, budget: u32) -> cairn_common::token_meter::Cut {
        let byte_offset = safe_char_boundary(text, (budget as usize).min(text.len()));
        cairn_common::token_meter::Cut {
            byte_offset,
            tokens: byte_offset.try_into().unwrap_or(u32::MAX),
        }
    }
}

/// Resolve a possibly-negative offset against a total count (negative = tail).
fn resolve_offset(offset: Option<i64>, total: usize) -> usize {
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
fn fair_share_budgets(lengths: &[u32], total_budget: u32) -> Vec<u32> {
    if lengths.is_empty() {
        return Vec::new();
    }
    let mut budgets = vec![0; lengths.len()];
    let mut remaining_budget = total_budget;
    let mut remaining_count = lengths.len();
    let mut by_length: Vec<(usize, u32)> = lengths.iter().copied().enumerate().collect();
    by_length.sort_by_key(|(_, length)| *length);

    let mut index = 0;
    while index < by_length.len() && remaining_count > 0 {
        let divisor = u32::try_from(remaining_count).unwrap_or(u32::MAX);
        let floor = remaining_budget / divisor;
        let (original_index, length) = by_length[index];
        if length > floor {
            break;
        }
        budgets[original_index] = length;
        remaining_budget = remaining_budget.saturating_sub(length);
        remaining_count -= 1;
        index += 1;
    }
    let divisor = u32::try_from(remaining_count).unwrap_or(u32::MAX);
    if let Some(floor) = remaining_budget.checked_div(divisor) {
        let extra = remaining_budget % divisor;
        for (extra_index, (original_index, _)) in by_length[index..].iter().enumerate() {
            budgets[*original_index] = floor + u32::from((extra_index as u32) < extra);
        }
    }
    budgets
}

// ---------------------------------------------------------------------------
// Continue-URI helpers (ported from the cairn-cmd composer).
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
        let prior_chars = split_target_query(uri)
            .ok()
            .and_then(|split| {
                split
                    .params
                    .iter()
                    .rev()
                    .find(|param| param.key == "char_offset")
                    .and_then(|param| param.value.parse::<usize>().ok())
            })
            .unwrap_or(0);
        return target_with_query_param(&target, "char_offset", prior_chars + chars);
    }
    let target = target_without_query_param(uri, "char_offset");
    let target = target_with_query_param(&target, "offset", offset + shown_lines);
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

/// Continue footer for a grep whose `head_limit` window elided results. Shaped
/// like the line-window footer because it answers the same question — what did
/// I see, out of what, and how do I get the rest — and its continue widens the
/// window to the true total, so one read completes the enumeration.
fn grep_window_footer(uri: &str, shown: usize, total: usize, noun: &str) -> String {
    format!(
        "[{} 1\u{2013}{} of {} — continue: {}]",
        noun,
        shown,
        total,
        target_with_query_param(uri, "head_limit", total),
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
/// fits (directory, image, non-content state, or an empty body).
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
            // The counts describe the COMPLETE result, so the header answers
            // "how many are there".
            let counts = match (meta.match_count, meta.file_count) {
                (Some(matches), Some(files)) => format!("{matches} matches in {files} files"),
                (Some(matches), None) => format!("{matches} matches"),
                // A `files_with_matches` grep has no match dimension: its body is
                // the list of files that matched, so the file count is the honest
                // count. Without it the header falls silent and a hit reads like a
                // miss.
                (None, Some(files)) => format!("{files} files"),
                (None, None) => return None,
            };
            // A `head_limit` window makes the body a sample of that result, and
            // saying so here is the whole difference between a partial
            // enumeration and a wrong one.
            match (meta.grep_shown, meta.total_units, meta.unit_noun.as_deref()) {
                (Some(shown), Some(total), Some(noun)) => {
                    Some(format!("{counts}; {shown} of {total} {noun} shown"))
                }
                _ => Some(counts),
            }
        }
        SegmentKind::Glob => Some(format!("{} files", meta.file_count?)),
        SegmentKind::Directory
        | SegmentKind::Image
        | SegmentKind::Error
        | SegmentKind::Duplicate => None,
    }
}

/// The display label for an image read's durable reference: the target's final
/// path segment (`file:plots/fig.png` -> `fig.png`, `cairn:~/browser?screenshot`
/// -> `browser`), falling back to the whole identity when there is nothing
/// shorter to say.
fn image_label(uri: &str) -> &str {
    let identity = uri.split('?').next().unwrap_or(uri);
    let tail = identity
        .rsplit('/')
        .next()
        .filter(|segment| !segment.is_empty())
        .unwrap_or(identity);
    // A legacy hash-addressed image would otherwise be labeled with its own
    // 64-character content hash, which says nothing. Friendly image URIs need no
    // substitute: their own tail is a short ordinal.
    if tail.len() == 64 && tail.bytes().all(|b| b.is_ascii_hexdigit()) {
        return "image";
    }
    tail.strip_prefix("file:").unwrap_or(tail)
}

/// The durable-reference trailer for a segment's promoted images: one markdown
/// reference per stored image, appended after the body and any continue footer.
///
/// This trailer is the *only* part of an image read that survives into the
/// transcript. The base64 [`ImageBlock`] rides beside the text as a native
/// content block and reaches the agent alone; a recorded tool result keeps text.
/// Composing the reference here — in the one composer both the live batch and
/// archival reconstruction run — keeps the two byte-identical and means no
/// producer has to know about durable storage. A block with no `uri` (nothing was
/// promoted, or a legacy archived read reconstructed from bytes alone) renders
/// nothing, exactly as before.
fn image_reference_trailer(images: &[ImageBlock], target: &str) -> String {
    let label = image_label(target);
    images
        .iter()
        .filter_map(|image| image.uri.as_deref())
        .map(|uri| format!("\n![{label}]({uri})"))
        .collect()
}

fn header(meta: &SegmentMeta, shown: usize, truncated: bool) -> String {
    match meta.kind {
        SegmentKind::Error => return format!("=== {} [error] ===", meta.uri),
        SegmentKind::Duplicate => return format!("=== {} [duplicate] ===", meta.uri),
        _ => {}
    }
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
    meta: SegmentMeta,
    images: Vec<ImageBlock>,
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

/// Cut `body` to `budget` tokens on a line boundary. When even the first line
/// alone exceeds the budget, fall back to a token-truncated prefix of that
/// line and report a text-relative `char_offset` so the continue URI advances.
fn cut_body(
    body: &str,
    budget: u32,
    numbered: bool,
    logical_lines: Option<usize>,
    meter: &dyn TokenMeter,
) -> BodyCut {
    if meter.count(body) <= budget {
        return BodyCut {
            body: body.to_string(),
            shown_lines: logical_lines.unwrap_or_else(|| body.lines().count()),
            budget_truncated: false,
            char_offset: None,
        };
    }
    // Keep only whole lines that fit. When at least one newline precedes the
    // budget, cut there; the first line then fits and no char fallback is needed.
    let safe_end = safe_char_boundary(body, meter.cut(body, budget).byte_offset);
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
    let mut end = safe_char_boundary(first_line, meter.cut(first_line, budget).byte_offset);
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
pub fn render_segment(seg: ReadSegment, budget: u32, meter: &dyn TokenMeter) -> RenderedSegment {
    if matches!(
        seg.meta.natural_unit,
        NaturalUnit::Record | NaturalUnit::Turn
    ) {
        return render_record_segment(seg, budget, meter);
    }
    let ReadSegment {
        body,
        affordance: _,
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
    let image_trailer = image_reference_trailer(&images, &meta.uri);

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
        let fixed = format!(
            "{}\n{}{}",
            header(&meta, full_lines, true),
            image_trailer,
            history_suffix
        );
        let body_budget = content_budget.saturating_sub(meter.count(&fixed));
        let cut = cut_body(&body, body_budget, numbered, logical_full_lines, meter);
        // A grep window that elided matches is a truncated read like any other:
        // a structured consumer must be able to see that without re-deriving it.
        let truncated = cut.budget_truncated
            || window_short
            || cut.char_offset.is_some()
            || meta.grep_shown.is_some();

        let head = header(&meta, cut.shown_lines, truncated);
        let mut candidate = if cut.body.is_empty() {
            head.clone()
        } else {
            format!("{head}\n{}", cut.body)
        };

        let footer = match meta.natural_unit {
            NaturalUnit::Match => {
                match (meta.grep_shown, meta.total_units, meta.unit_noun.as_deref()) {
                    // The budget cut the body below even the window's own size, so
                    // its footer is the one that describes what is actually here.
                    _ if cut.budget_truncated => Some(grep_footer(&meta.uri, cut.shown_lines)),
                    (Some(shown), Some(total), Some(noun)) => {
                        Some(grep_window_footer(&meta.uri, shown, total, noun))
                    }
                    _ => None,
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
        candidate.push_str(&image_trailer);
        candidate.push_str(&history_suffix);

        let candidate_tokens = meter.count(&candidate);
        if candidate_tokens <= budget || content_budget == 0 || cut.shown_lines == 0 {
            meta.shown_units = cut.shown_lines;
            meta.truncated = truncated;
            meta.char_continuation = cut.char_offset.is_some();
            return RenderedSegment {
                text: candidate,
                meta,
                images,
            };
        }
        content_budget = content_budget.saturating_sub(candidate_tokens - budget + 1);
    }
}

/// Render one segment's standalone section text (header + body + footer) — the
/// exact bytes it contributes between separators in a composed batch. The budget
/// is effectively unbounded, so a section that was not truncated in its original
/// batch renders byte-identically here. Shared by archival's write side (the
/// needle it searches for in the stored composed result) and reconstruction (the
/// fill it splices back into a hybrid-read skeleton placeholder), exactly as
/// [`assemble`] is shared between the live and reconstructed composition.
pub(crate) fn render_section_text(seg: ReadSegment, meter: &dyn TokenMeter) -> String {
    render_segment(seg, u32::MAX, meter).text
}

/// Render a `Record` collection page. The body is already item-windowed by the
/// producer, so the item counts (`shown`/`total`) come from `meta`, never from
/// body line counts. Budget truncation on line boundaries is only a safety net;
/// the agent's `limit` is what bounds the page. The continue footer advances
/// `offset` by the items the producer emitted.
fn render_record_segment(seg: ReadSegment, budget: u32, meter: &dyn TokenMeter) -> RenderedSegment {
    let ReadSegment {
        body,
        affordance: _,
        images,
        history,
        mut meta,
    } = seg;

    let history_suffix = history
        .as_deref()
        .filter(|h| !h.is_empty())
        .map(|h| format!("\n\n{h}"))
        .unwrap_or_default();
    let image_trailer = image_reference_trailer(&images, &meta.uri);

    let produced_shown = meta.shown_units;
    let fixed = format!(
        "{}\n{}{}",
        header(&meta, produced_shown, true),
        image_trailer,
        history_suffix
    );
    let body_budget = budget.saturating_sub(meter.count(&fixed));
    let cut = cut_body(&body, body_budget, false, None, meter);
    let shown = match meta.record_prelude_lines {
        Some(prelude_lines) if cut.budget_truncated => cut
            .shown_lines
            .saturating_sub(prelude_lines)
            .min(produced_shown),
        _ => produced_shown,
    };
    let blocked_oversized_record = meta.record_prelude_lines.is_some()
        && cut.budget_truncated
        && shown == 0
        && (produced_shown > 0 || cut.char_offset.is_some());
    let more = !blocked_oversized_record
        && meta
            .total_units
            .is_some_and(|total| meta.offset + shown < total);
    let truncated = cut.budget_truncated || more;

    meta.shown_units = shown;
    let head = header(&meta, shown, truncated);
    let rendered_body = if blocked_oversized_record {
        serde_json::json!({
            "type": "error",
            "error": "record exceeds read budget; select fewer columns or shorter values",
            "offset": meta.offset,
        })
        .to_string()
    } else {
        cut.body
    };
    let mut text = if rendered_body.is_empty() {
        head
    } else {
        format!("{head}\n{rendered_body}")
    };

    if more {
        if let (Some(total), Some(noun)) = (meta.total_units, meta.unit_noun.as_deref()) {
            text.push('\n');
            text.push_str(&record_footer(&meta.uri, meta.offset, shown, total, noun));
        }
    }
    text.push_str(&image_trailer);
    text.push_str(&history_suffix);

    meta.truncated = truncated;
    RenderedSegment { text, meta, images }
}

/// Render a standalone segment under both the live token budget and the client
/// byte ceiling. This is the single-target counterpart to [`assemble`].
pub fn render_segment_bounded(seg: ReadSegment, meter: &dyn TokenMeter) -> RenderedSegment {
    if let Some(rendered) = render_segment_fitting(&seg, meter, None) {
        return rendered;
    }

    let mut without_history = seg.clone();
    without_history.history = None;
    if let Some(rendered) = render_segment_fitting(&without_history, meter, Some(HISTORY_ELISION)) {
        return rendered;
    }

    let uri = bounded_uri_label(&seg.meta.uri);
    let mut overflow = render_segment(without_history, 0, meter);
    overflow.text = format!(
        "Read target {uri} cannot fit below the 45,000-byte client ceiling because its required header exceeds the ceiling."
    );
    overflow.images.clear();
    overflow.meta.truncated = true;
    overflow
}

fn render_segment_fitting(
    seg: &ReadSegment,
    meter: &dyn TokenMeter,
    marker: Option<&str>,
) -> Option<RenderedSegment> {
    let finish = |mut rendered: RenderedSegment| {
        if let Some(marker) = marker {
            rendered.text.push_str("\n\n");
            rendered.text.push_str(marker);
        }
        (rendered.text.len() <= CLIENT_TOOL_RESULT_BYTE_CEILING).then_some(rendered)
    };

    let first = render_segment(seg.clone(), READ_BATCH_TOKEN_BUDGET, meter);
    if let Some(first) = finish(first) {
        return Some(first);
    }

    let mut low = 0;
    let mut high = READ_BATCH_TOKEN_BUDGET - 1;
    let mut best = None;
    while low <= high {
        let budget = low + (high - low) / 2;
        let candidate = render_segment(seg.clone(), budget, meter);
        if let Some(candidate) = finish(candidate) {
            best = Some(candidate);
            low = budget + 1;
        } else {
            if budget == 0 {
                break;
            }
            high = budget.saturating_sub(1);
        }
    }
    best
}

// ---------------------------------------------------------------------------
// Batch assembly.
// ---------------------------------------------------------------------------

/// Estimated untruncated length of a segment's text block, for fair-share
/// budgeting: header (with suffix slack), body, a footer allowance, and history.
fn estimate_len(segment: &ReadSegment, meter: &dyn TokenMeter) -> u32 {
    let history = segment.history.as_deref().unwrap_or_default();
    let images = image_reference_trailer(&segment.images, &segment.meta.uri);
    let estimate = format!(
        "{}\n{}\n{}{}{}",
        header(&segment.meta, segment.meta.shown_units, true),
        segment.body,
        "x".repeat(220),
        images,
        history
    );
    meter.count(&estimate)
}

/// Compose produced segments into one envelope under a single budget. Affordances
/// are deduped by `(kind, block)` and appended once after all content, so
/// truncation can never cut them.
///
/// Shared by the live `read_batch` dispatch ([`super::batch`]) and archival
/// reconstruction ([`crate::storage::events::reconstruct`]): both feed a `Vec<ReadSegment>`
/// through this one composer so a reconstructed read reproduces the live envelope
/// byte-for-byte.
pub fn assemble(segments: Vec<ReadSegment>, meter: &dyn TokenMeter) -> ReadBatchEnvelope {
    assemble_inner(segments, false, meter)
}

/// Compose a batch while retaining each final, post-budget segment body in the
/// envelope. The bodies exclude the generated `=== uri [suffix] ===` header but
/// include continuation footers and history exactly as rendered in `text`.
pub fn assemble_with_bodies(
    segments: Vec<ReadSegment>,
    meter: &dyn TokenMeter,
) -> ReadBatchEnvelope {
    assemble_inner(segments, true, meter)
}

/// Recompose archived reads written under the former character budget.
pub fn assemble_legacy_chars(
    segments: Vec<ReadSegment>,
    meter: &dyn TokenMeter,
) -> ReadBatchEnvelope {
    assemble_at_budget(segments, false, meter, 45_000)
}

fn assemble_inner(
    segments: Vec<ReadSegment>,
    include_bodies: bool,
    meter: &dyn TokenMeter,
) -> ReadBatchEnvelope {
    if let Some(envelope) = assemble_fitting(&segments, include_bodies, meter, None) {
        return envelope;
    }

    let mut without_affordances = segments.clone();
    for segment in &mut without_affordances {
        segment.affordance = None;
    }
    if let Some(mut envelope) = assemble_fitting(
        &without_affordances,
        include_bodies,
        meter,
        Some(AFFORDANCE_ELISION),
    ) {
        envelope.affordances.clear();
        return envelope;
    }

    let mut without_trailers = without_affordances;
    for segment in &mut without_trailers {
        segment.history = None;
    }
    if let Some(mut envelope) = assemble_fitting(
        &without_trailers,
        include_bodies,
        meter,
        Some(METADATA_ELISION),
    ) {
        envelope.affordances.clear();
        return envelope;
    }

    let largest_header = segments
        .iter()
        .max_by_key(|segment| header(&segment.meta, 0, true).len())
        .map(|segment| {
            (
                segment.meta.uri.as_str(),
                header(&segment.meta, 0, true).len(),
            )
        });
    let refusal = match largest_header {
        Some((uri, bytes)) if bytes > CLIENT_TOOL_RESULT_BYTE_CEILING => format!(
            "Read target {} cannot fit below the 45,000-byte client ceiling because its required header exceeds the ceiling.",
            bounded_uri_label(uri)
        ),
        Some((_, _)) => format!(
            "Read batch of {} targets cannot fit below the 45,000-byte client ceiling because the required headers for its targets exceed the ceiling on their own. Split the request into fewer targets.",
            segments.len()
        ),
        None => "Empty read batch cannot be rendered below the 45,000-byte client ceiling.".to_string(),
    };
    let mut overflow = assemble_at_budget(without_trailers, include_bodies, meter, 0);
    overflow.text = refusal.clone();
    overflow.images.clear();
    overflow.affordances.clear();
    for meta in &mut overflow.segments {
        meta.truncated = true;
    }
    if let Some(bodies) = &mut overflow.bodies {
        bodies.fill(String::new());
        if let Some(first) = bodies.first_mut() {
            *first = refusal;
        }
    }
    overflow
}

fn assemble_fitting(
    segments: &[ReadSegment],
    include_bodies: bool,
    meter: &dyn TokenMeter,
    marker: Option<&str>,
) -> Option<ReadBatchEnvelope> {
    let finish = |mut envelope: ReadBatchEnvelope| {
        if let Some(marker) = marker {
            envelope.text.push_str("\n\n");
            envelope.text.push_str(marker);
        }
        (envelope.text.len() <= CLIENT_TOOL_RESULT_BYTE_CEILING).then_some(envelope)
    };

    let first = assemble_at_budget(
        segments.to_vec(),
        include_bodies,
        meter,
        READ_BATCH_TOKEN_BUDGET,
    );
    if let Some(first) = finish(first) {
        return Some(first);
    }

    // Recompose at progressively tighter token budgets rather than slicing the
    // finished text. That keeps fair-share representation of every target and
    // lets the canonical renderer regenerate truthful headers and continuations.
    let mut low = 0;
    let mut high = READ_BATCH_TOKEN_BUDGET - 1;
    let mut best = None;
    while low <= high {
        let budget = low + (high - low) / 2;
        let candidate = assemble_at_budget(segments.to_vec(), include_bodies, meter, budget);
        if let Some(candidate) = finish(candidate) {
            best = Some(candidate);
            low = budget + 1;
        } else {
            if budget == 0 {
                break;
            }
            high = budget.saturating_sub(1);
        }
    }

    best
}

fn assemble_at_budget(
    segments: Vec<ReadSegment>,
    include_bodies: bool,
    meter: &dyn TokenMeter,
    batch_budget: u32,
) -> ReadBatchEnvelope {
    let lengths: Vec<u32> = segments
        .iter()
        .map(|segment| estimate_len(segment, meter))
        .collect();
    let separator_budget = meter.count(&"\n".repeat(segments.len().saturating_sub(1)));
    let structured_affordances: Vec<Affordance> = segments
        .iter()
        .filter_map(|segment| segment.affordance.clone())
        .collect();
    let mut affordances: Vec<Affordance> = Vec::new();
    for segment in &segments {
        if let Some(affordance) = &segment.affordance {
            if !affordances.iter().any(|existing| {
                existing.kind == affordance.kind && existing.block == affordance.block
            }) {
                affordances.push(affordance.clone());
            }
        }
    }
    let affordance_budget: u32 = affordances
        .iter()
        .map(|affordance| meter.count(&format!("\n\n{}", affordance.block)))
        .sum();
    let total_budget = batch_budget
        .saturating_sub(separator_budget)
        .saturating_sub(affordance_budget);
    let budgets = fair_share_budgets(&lengths, total_budget);

    let mut text = String::new();
    let mut images = Vec::new();
    let mut metas: Vec<SegmentMeta> = Vec::new();
    let mut bodies = include_bodies.then(|| Vec::with_capacity(segments.len()));
    for (segment, budget) in segments.into_iter().zip(budgets) {
        let rendered = render_segment(segment, budget, meter);
        if !text.is_empty() {
            text.push('\n');
        }
        text.push_str(&rendered.text);
        if let Some(bodies) = &mut bodies {
            let body = rendered
                .text
                .split_once('\n')
                .map_or("", |(_, body)| body)
                .to_string();
            bodies.push(body);
        }
        images.extend(rendered.images);
        metas.push(rendered.meta);
    }

    for affordance in affordances {
        text.push_str("\n\n");
        text.push_str(&affordance.block);
    }

    ReadBatchEnvelope {
        text,
        images,
        segments: metas,
        affordances: structured_affordances,
        bodies,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_common::token_meter::Cut;

    static BYTE_METER: TestByteMeter = TestByteMeter;

    fn render_segment(seg: ReadSegment, budget: usize) -> RenderedSegment {
        super::render_segment(seg, budget.try_into().unwrap(), &BYTE_METER)
    }

    fn file_seg(uri: &str, numbered_body: &str, total: usize, offset: usize) -> ReadSegment {
        let mut meta = SegmentMeta::new(uri, SegmentKind::File, NaturalUnit::Line);
        meta.total_units = Some(total);
        meta.offset = offset;
        ReadSegment::text(numbered_body, meta)
    }

    fn image_seg(uri: &str, stored: Option<&str>) -> ReadSegment {
        let mut seg = ReadSegment::text(
            String::new(),
            SegmentMeta::new(uri, SegmentKind::Image, NaturalUnit::Line),
        );
        let mut image = ImageBlock::inline("image/png", "b64");
        image.uri = stored.map(str::to_string);
        seg.images.push(image);
        seg
    }

    const STORED_IMAGE: &str =
        "cairn://p/CAIRN/images/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    #[test]
    fn promoted_image_renders_a_durable_reference_under_the_header() {
        // The base64 block reaches the agent as a native content block; this
        // reference is the only part of the read the transcript keeps, so it must
        // be in the composed text.
        let r = render_segment(image_seg("file:plots/fig.png", Some(STORED_IMAGE)), 10_000);
        assert_eq!(
            r.text,
            format!("=== file:plots/fig.png ===\n![fig.png]({STORED_IMAGE})")
        );
    }

    #[test]
    fn reading_a_stored_image_uri_is_not_labelled_with_its_hash() {
        let r = render_segment(image_seg(STORED_IMAGE, Some(STORED_IMAGE)), 10_000);
        assert!(r.text.ends_with(&format!("![image]({STORED_IMAGE})")));
    }

    #[test]
    fn unpromoted_image_renders_exactly_as_before() {
        // Archival reconstruction rebuilds blocks from bytes alone and can carry
        // no durable URI; those reads must still compose byte-for-byte as they
        // originally did.
        let r = render_segment(image_seg("file:logo.png", None), 10_000);
        assert_eq!(r.text, "=== file:logo.png ===");
    }

    #[test]
    fn image_reference_follows_a_body_and_its_continue_footer() {
        // A browser screenshot carries a status banner; the reference is a
        // trailer after everything the body and footer say, like issue history.
        let mut seg = image_seg("cairn:~/browser?screenshot", Some(STORED_IMAGE));
        seg.body = "Browser: https://example.com".to_string();
        seg.meta.kind = SegmentKind::Resource;
        seg.meta.total_units = Some(1);
        let r = render_segment(seg, 10_000);
        assert_eq!(
            r.text,
            format!(
                "=== cairn:~/browser?screenshot [lines 1\u{2013}1 of 1] ===\nBrowser: https://example.com\n![browser]({STORED_IMAGE})"
            )
        );
    }

    #[test]
    fn image_reference_survives_a_budget_that_truncates_the_body() {
        // The reference is budgeted like history: a tight budget cuts body lines,
        // never the one line that makes the image visible at all.
        let mut seg = image_seg("cairn:~/browser?screenshot", Some(STORED_IMAGE));
        seg.body = (1..=200)
            .map(|i| format!("line {i} {}", "x".repeat(80)))
            .collect::<Vec<_>>()
            .join("\n");
        seg.meta.kind = SegmentKind::Resource;
        seg.meta.total_units = Some(200);
        let r = render_segment(seg, 1_500);
        assert!(r.text.len() <= 1_500);
        assert!(r.meta.truncated);
        assert!(r.text.ends_with(&format!("![browser]({STORED_IMAGE})")));
    }

    /// A `head_limit` window that elided matches must be visible in the
    /// composed text alone — header and footer — because that text is all an
    /// agent reading an enumeration ever sees. The counts stay the whole
    /// result's; the window says how much of it is here; the continue widens
    /// `head_limit` to the true total so one more read finishes the job.
    #[test]
    fn a_windowed_grep_says_so_in_its_header_and_footer() {
        let uri = "file:src?grep=needle&head_limit=8";
        let mut meta = SegmentMeta::new(uri, SegmentKind::Grep, NaturalUnit::Match);
        meta.match_count = Some(23);
        meta.file_count = Some(6);
        meta.total_units = Some(23);
        meta.unit_noun = Some("matches".to_string());
        meta.grep_shown = Some(8);
        let r = render_segment(ReadSegment::text("src/a.rs:1:needle", meta), 10_000);

        assert!(
            r.text
                .starts_with("=== file:src?grep=needle&head_limit=8 [23 matches in 6 files; 8 of 23 matches shown] ==="),
            "{}",
            r.text
        );
        assert!(
            r.text.ends_with(
                "[matches 1\u{2013}8 of 23 — continue: file:src?grep=needle&head_limit=23]"
            ),
            "{}",
            r.text
        );
        // A partial read is a truncated read, for consumers that read metadata
        // rather than text.
        assert!(r.meta.truncated);
    }

    /// The same segment with nothing elided keeps the plain header and grows no
    /// footer: the marker means something precisely because a complete read does
    /// not carry it.
    #[test]
    fn a_complete_grep_carries_no_window_marker() {
        let mut meta = SegmentMeta::new(
            "file:src?grep=needle",
            SegmentKind::Grep,
            NaturalUnit::Match,
        );
        meta.match_count = Some(23);
        meta.file_count = Some(6);
        meta.total_units = Some(23);
        meta.unit_noun = Some("matches".to_string());
        let r = render_segment(ReadSegment::text("src/a.rs:1:needle", meta), 10_000);

        assert_eq!(
            r.text,
            "=== file:src?grep=needle [23 matches in 6 files] ===\nsrc/a.rs:1:needle"
        );
        assert!(!r.meta.truncated);
    }

    /// Under `files_with_matches` the window's unit is the file, and the footer
    /// has to name the unit it actually counted rather than saying "matches"
    /// over a body that carries none.
    #[test]
    fn a_windowed_file_list_reports_files_not_matches() {
        let uri = "file:src?grep=needle&output_mode=files_with_matches&head_limit=2";
        let mut meta = SegmentMeta::new(uri, SegmentKind::Grep, NaturalUnit::Match);
        meta.file_count = Some(6);
        meta.total_units = Some(6);
        meta.unit_noun = Some("files".to_string());
        meta.grep_shown = Some(2);
        let r = render_segment(ReadSegment::text("src/a.rs\nsrc/b.rs", meta), 10_000);

        assert!(
            r.text.contains("[6 files; 2 of 6 files shown]"),
            "{}",
            r.text
        );
        assert!(
            r.text.contains("[files 1\u{2013}2 of 6 — continue:"),
            "{}",
            r.text
        );
    }

    #[test]
    fn non_content_states_render_stable_header_markers() {
        let error = render_segment(error_segment("file:missing", "not found"), 10_000);
        assert_eq!(error.text, "=== file:missing [error] ===\nnot found");

        let duplicate = render_segment(
            duplicate_segment("file:same", "[duplicate call] unchanged"),
            10_000,
        );
        assert_eq!(
            duplicate.text,
            "=== file:same [duplicate] ===\n[duplicate call] unchanged"
        );

        let success = render_segment(file_seg("file:ok", "     1\tok", 1, 0), 10_000);
        assert!(!success.text.contains("[error]"));
        assert!(!success.text.contains("[duplicate]"));
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
    fn char_continuation_preserves_projection_and_accumulates_offset() {
        let target = line_continue_target(
            "cairn://p/CAIRN/1/1/builder/chat/raw?format=json&offset=0&char_offset=120",
            0,
            1,
            None,
            Some(80),
        );
        assert_eq!(
            target,
            "cairn://p/CAIRN/1/1/builder/chat/raw?format=json&offset=0&char_offset=200"
        );
    }

    #[test]
    fn completed_char_fragment_drops_char_offset_before_next_line() {
        let target = line_continue_target(
            "cairn://p/CAIRN/1/1/builder/chat/raw?format=json&offset=0&char_offset=200",
            0,
            1,
            Some(1),
            None,
        );
        assert_eq!(
            target,
            "cairn://p/CAIRN/1/1/builder/chat/raw?format=json&offset=1&limit=1"
        );
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
    fn line_framed_record_budget_keeps_truthful_row_continuation() {
        let schema = r#"{"type":"columns","columns":["value"]}"#;
        let row = |value: char| {
            format!(
                r#"{{"type":"row","values":["{}"]}}"#,
                value.to_string().repeat(180)
            )
        };
        let body = format!("{schema}\n{}\n{}\n{}", row('a'), row('b'), row('c'));
        let mut segment = record_seg(
            "cairn://db?sql=SELECT value&format=json&limit=3",
            &body,
            Some(3),
            3,
            Some("rows"),
            0,
        );
        segment.meta.limit = Some(3);
        segment.meta.record_prelude_lines = Some(1);

        let out = render_segment(segment, 500);

        assert!(out.meta.truncated);
        assert_eq!(out.meta.shown_units, 1);
        assert!(out
            .text
            .starts_with("=== cairn://db?sql=SELECT value&format=json&limit=3 [1 of 3 rows] ==="));
        assert!(out
            .text
            .contains("continue: cairn://db?sql=SELECT value&format=json&limit=3&offset=1"));
        assert!(out.text.lines().any(|line| line == schema));
        for line in out.text.lines().filter(|line| line.starts_with('{')) {
            serde_json::from_str::<serde_json::Value>(line).unwrap();
        }
    }

    #[test]
    fn line_framed_record_oversized_row_returns_parseable_error_without_loop() {
        let schema = r#"{"type":"columns","columns":["value"]}"#;
        let row = format!(r#"{{"type":"row","values":["{}"]}}"#, "x".repeat(2_000));
        let body = format!("{schema}\n{row}");
        let mut segment = record_seg(
            "cairn://db?sql=SELECT value&format=json&limit=1",
            &body,
            Some(1),
            1,
            Some("rows"),
            0,
        );
        segment.meta.record_prelude_lines = Some(1);

        let out = render_segment(segment, 300);

        assert!(out.meta.truncated);
        assert_eq!(out.meta.shown_units, 0);
        assert!(out
            .text
            .starts_with("=== cairn://db?sql=SELECT value&format=json&limit=1 [0 of 1 rows] ==="));
        assert!(!out.text.contains("continue:"));
        let error_line = out.text.lines().find(|line| line.starts_with('{')).unwrap();
        let error: serde_json::Value = serde_json::from_str(error_line).unwrap();
        assert_eq!(error["type"], "error");
        assert!(error["error"].as_str().unwrap().contains("read budget"));
    }

    #[test]
    fn line_framed_record_oversized_prelude_returns_parseable_error() {
        let schema = serde_json::json!({
            "type": "columns",
            "columns": ["x".repeat(2_000)],
        })
        .to_string();
        let mut segment = record_seg(
            "cairn://db?sql=SELECT value&format=json",
            &schema,
            Some(0),
            0,
            Some("rows"),
            0,
        );
        segment.meta.record_prelude_lines = Some(1);

        let out = render_segment(segment, 300);

        assert!(out.meta.truncated);
        assert_eq!(out.meta.shown_units, 0);
        assert!(!out.meta.char_continuation);
        assert!(out
            .text
            .starts_with("=== cairn://db?sql=SELECT value&format=json [0 of 0 rows] ==="));
        assert!(!out.text.contains("continue:"));
        let error_line = out.text.lines().find(|line| line.starts_with('{')).unwrap();
        let error: serde_json::Value = serde_json::from_str(error_line).unwrap();
        assert_eq!(error["type"], "error");
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

    struct DensityMeter;

    impl TokenMeter for DensityMeter {
        fn count(&self, text: &str) -> u32 {
            let dense = text.bytes().filter(|byte| *byte == b'!').count() as u32;
            let sparse = text.len() as u32 - dense;
            dense + sparse.div_ceil(4)
        }

        fn cut(&self, text: &str, budget: u32) -> Cut {
            let mut byte_offset = 0;
            let mut dense = 0_u32;
            let mut sparse = 0_u32;
            for (end, ch) in text
                .char_indices()
                .map(|(start, ch)| (start + ch.len_utf8(), ch))
            {
                if ch == '!' {
                    dense += 1;
                } else {
                    sparse += ch.len_utf8() as u32;
                }
                if dense + sparse.div_ceil(4) > budget {
                    break;
                }
                byte_offset = end;
            }
            Cut {
                byte_offset,
                tokens: self.count(&text[..byte_offset]),
            }
        }
    }

    #[test]
    fn equal_token_budgets_cut_different_byte_lengths_by_density() {
        let meter = DensityMeter;
        let dense = cut_body(&"!".repeat(100), 10, false, None, &meter);
        let sparse = cut_body(&"a".repeat(100), 10, false, None, &meter);

        assert_eq!(dense.body.len(), 10);
        assert_eq!(sparse.body.len(), 40);
        assert!(dense.char_offset.is_some());
        assert!(sparse.char_offset.is_some());
    }

    #[test]
    fn batch_budget_preserves_continue_footer_and_affordance() {
        let body = (1..=1_000)
            .map(|line| format!("{line}: {}", "x".repeat(80)))
            .collect::<Vec<_>>()
            .join("\n");
        let mut segment = file_seg("file:large", &body, 1_000, 0);
        segment.affordance = Some(Affordance {
            kind: SegmentKind::File,
            block: "## File affordance\nUse `offset` to continue.".to_string(),
            spec: None,
        });

        let envelope = assemble(vec![segment], &BYTE_METER);

        assert!(envelope.text.contains("output truncated to fit budget"));
        assert!(envelope.text.contains("continue: file:large?offset="));
        assert!(envelope
            .text
            .ends_with("## File affordance\nUse `offset` to continue."));
        assert!(BYTE_METER.count(&envelope.text) <= READ_BATCH_TOKEN_BUDGET);
    }

    #[test]
    fn token_budget_does_not_cross_client_tool_result_byte_ceiling() {
        let code = include_str!("../../../cairn-core/src/mcp/handlers/read/batch.rs");
        let prose = "Reads are being intercepted before the agent sees them. The pagination hint \
                     and affordance block must remain visible so the agent can continue safely.\n"
            .repeat(1_000);

        for (uri, specimen) in [("file:batch.rs", code), ("cairn:~/arc", prose.as_str())] {
            let body = specimen
                .lines()
                .enumerate()
                .map(|(index, line)| format!("{:>6}\t{line}", index + 1))
                .collect::<Vec<_>>()
                .join("\n");
            let mut segment = file_seg(uri, &body, body.lines().count(), 0);
            segment.affordance = Some(Affordance {
                kind: SegmentKind::File,
                block: "## Read affordance\nUse the `continue:` URI above to page.".to_string(),
                spec: None,
            });

            let envelope = assemble(vec![segment], &DensityMeter);

            assert!(
                envelope.text.len() <= CLIENT_TOOL_RESULT_BYTE_CEILING,
                "{uri} rendered {} bytes",
                envelope.text.len()
            );
            assert!(envelope.text.contains("continue:"), "{uri}");
            assert!(envelope
                .text
                .ends_with("## Read affordance\nUse the `continue:` URI above to page."));
        }
    }

    #[test]
    fn byte_ceiling_preserves_every_target_through_fair_share() {
        let affordance = Affordance {
            kind: SegmentKind::File,
            block: "## Read affordance\nUse the continuation URI.".to_string(),
            spec: None,
        };
        let segments = (0..3)
            .map(|index| {
                let body = (1..=1_000)
                    .map(|line| format!("{line}: {}", "payload ".repeat(20)))
                    .collect::<Vec<_>>()
                    .join("\n");
                let mut segment = file_seg(&format!("file:large-{index}.txt"), &body, 1_000, 0);
                segment.affordance = Some(affordance.clone());
                segment
            })
            .collect::<Vec<_>>();

        let uncapped = assemble_at_budget(
            segments.clone(),
            false,
            &DensityMeter,
            READ_BATCH_TOKEN_BUDGET,
        );
        let naive =
            &uncapped.text[..safe_char_boundary(&uncapped.text, CLIENT_TOOL_RESULT_BYTE_CEILING)];
        assert!(!naive.contains("=== file:large-2.txt"));

        let envelope = assemble(segments, &DensityMeter);
        assert!(envelope.text.len() <= CLIENT_TOOL_RESULT_BYTE_CEILING);
        for index in 0..3 {
            let header = format!("=== file:large-{index}.txt");
            assert_eq!(envelope.text.matches(&header).count(), 1, "{header}");
        }
        assert_eq!(envelope.text.matches("continue:").count(), 3);
        assert!(envelope
            .text
            .ends_with("## Read affordance\nUse the continuation URI."));
    }

    #[test]
    fn oversized_distinct_affordances_preserve_every_target() {
        let segments = (0..20)
            .map(|index| {
                let body = format!("content for target {index}");
                let mut segment = file_seg(&format!("file:target-{index}"), &body, 1, 0);
                segment.affordance = Some(Affordance {
                    kind: SegmentKind::File,
                    block: format!("affordance {index} {}", "x".repeat(2_400)),
                    spec: None,
                });
                segment
            })
            .collect::<Vec<_>>();

        let envelope = assemble(segments, &BYTE_METER);

        assert!(envelope.text.len() <= CLIENT_TOOL_RESULT_BYTE_CEILING);
        assert_eq!(envelope.segments.len(), 20);
        assert!(envelope.affordances.is_empty());
        assert!(envelope.text.contains(AFFORDANCE_ELISION));
        for index in 0..20 {
            assert!(envelope
                .text
                .contains(&format!("content for target {index}")));
        }
    }

    #[test]
    fn irreducible_standalone_history_is_omitted_in_band() {
        let mut segment = file_seg("file:large", "", 1, 0);
        segment.history = Some("x".repeat(CLIENT_TOOL_RESULT_BYTE_CEILING + 1));

        let rendered = render_segment_bounded(segment, &BYTE_METER);

        assert!(rendered.text.len() <= CLIENT_TOOL_RESULT_BYTE_CEILING);
        assert!(rendered.text.contains("=== file:large"));
        assert!(rendered.text.contains(HISTORY_ELISION));
        assert!(!rendered.text.contains(&"x".repeat(100)));
        assert_eq!(rendered.meta.uri, "file:large");
    }

    #[test]
    fn oversized_standalone_uri_returns_a_bounded_targeted_refusal() {
        let uri = format!("file:{}", "x".repeat(CLIENT_TOOL_RESULT_BYTE_CEILING + 1));
        let segment = file_seg(&uri, "content", 1, 0);

        let rendered = render_segment_bounded(segment, &BYTE_METER);

        assert!(rendered.text.len() <= CLIENT_TOOL_RESULT_BYTE_CEILING);
        assert!(rendered.text.starts_with("Read target file:"));
        assert!(rendered.text.contains("required header exceeds"));
        assert_eq!(rendered.meta.uri, uri);
        assert!(rendered.meta.truncated);
    }

    #[test]
    fn aggregate_header_refusal_keeps_segments_and_terminal_bodies_consistent() {
        let segments = (0..1_000)
            .map(|index| {
                file_seg(
                    &format!("file:target-{index}-{}", "x".repeat(40)),
                    "content",
                    1,
                    0,
                )
            })
            .collect::<Vec<_>>();

        let envelope = assemble_with_bodies(segments, &BYTE_METER);

        assert!(envelope.text.len() <= CLIENT_TOOL_RESULT_BYTE_CEILING);
        assert_eq!(
            envelope.text,
            "Read batch of 1000 targets cannot fit below the 45,000-byte client ceiling because the required headers for its targets exceed the ceiling on their own. Split the request into fewer targets."
        );
        assert_eq!(envelope.segments.len(), 1_000);
        let bodies = envelope.bodies.as_ref().unwrap();
        assert_eq!(bodies.len(), envelope.segments.len());
        assert_eq!(bodies[0], envelope.text);
        assert!(bodies[1..].iter().all(String::is_empty));
        assert!(envelope.images.is_empty());
        assert!(envelope.segments.iter().all(|meta| meta.truncated));
    }
}
