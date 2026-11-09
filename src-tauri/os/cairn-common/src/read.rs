//! Structured read segments shared by the cairn-cmd client and the cairn-core
//! backend.
//!
//! A `read` tool call resolves an array of targets and forwards them to the
//! backend as one `read_batch` callback. The backend classifies each target,
//! runs its producer to a [`ReadSegment`], windows and truncates it under one
//! shared budget, and assembles the whole batch into a [`ReadBatchEnvelope`].
//!
//! The envelope is the lossless *internal* contract over the cairn-cmd↔cairn-core
//! HTTP boundary: it carries the fully composed `=== uri [suffix] ===` text, any
//! image blocks, and per-segment metadata. Programmatic callers may opt into the
//! final post-budget segment bodies; they are omitted by default so agent-facing
//! reads keep their existing response size and serialization. The enriched header
//! suffix remains a terse, deterministic projection of each segment's metadata.

use serde::{Deserialize, Serialize};

/// The unit a segment windows by: a file/web read pages by line, a grep pages by
/// match, a glob `files_with_matches` read pages by file, a pushdown collection
/// (`/issues`, `/messages`) pages by record, and chat digests page by turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NaturalUnit {
    Line,
    Match,
    File,
    Record,
    Turn,
}

/// The producer family a segment came from. Drives header-suffix shape and
/// (later) affordance dedupe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SegmentKind {
    File,
    Directory,
    Glob,
    Grep,
    Web,
    Resource,
    Image,
    Error,
    Duplicate,
}

/// A base64-encoded image block, surfaced as a separate `CallToolResult` content
/// block by the CLI after the composed text.
///
/// The base64 payload reaches the agent only: a transcript keeps a tool result's
/// text, not its native content blocks. `uri` is the durable stored-image
/// reference the read path promoted these bytes
/// to, and it is what makes the same read visible to the *user* — the composer
/// renders it as a markdown reference in the segment text, which the transcript
/// does keep and the frontend resolves back out of the content store. `None` when
/// the bytes were not promoted (no authenticated run, or a storage refusal), in
/// which case no reference is rendered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImageBlock {
    pub mime_type: String,
    pub data: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,
}

impl ImageBlock {
    /// An un-promoted block carrying only the inline bytes. Producers build this;
    /// the read path fills `uri` in afterward when it can store the bytes.
    pub fn inline(mime_type: impl Into<String>, data: impl Into<String>) -> Self {
        Self {
            mime_type: mime_type.into(),
            data: data.into(),
            uri: None,
        }
    }

    /// A block that already has an address: the bytes were fetched *through* a
    /// stored-image URI, so that URI is what names them. Promotion skips a block
    /// carrying a `uri`, which is what keeps reading an image by its own address
    /// from minting a second reference to the same blob and then citing the
    /// reference the caller did not ask for.
    pub fn stored(
        mime_type: impl Into<String>,
        data: impl Into<String>,
        uri: impl Into<String>,
    ) -> Self {
        Self {
            mime_type: mime_type.into(),
            data: data.into(),
            uri: Some(uri.into()),
        }
    }
}

/// An opaque, post-truncation-appended, dedupe-by-kind affordance block.
///
/// In this slice file/web carry `None` and resources keep their affordance
/// embedded in [`ReadSegment::body`] (opaque). The dedupe + append machinery is
/// built and tested now; the resource child populates it for real.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Affordance {
    pub kind: SegmentKind,
    pub block: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spec: Option<AffordanceSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AffordanceSpec {
    pub kind: String,
    pub name: String,
    pub links: Vec<LinkSpec>,
    pub filters: Vec<FilterSpec>,
    pub actions: Vec<ActionSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinkSpec {
    pub label: String,
    pub uri_template: String,
    pub uri: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FilterSpec {
    pub key: String,
    pub values: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionSpec {
    pub label: String,
    pub mode: String,
    pub uri_template: String,
    pub uri: Option<String>,
    pub required: Vec<KeyInfo>,
    pub optional: Vec<KeyInfo>,
    pub example: String,
    /// Prose an action carries beyond its key list, appended verbatim after the
    /// rendered keys and before the example. Carries its own leading separator
    /// because the guidance is a continuation of the key sentence, not a new
    /// one (`\" with text, status, ...\"`, `\"; the slug is immutable\"`).
    /// A consumer that only builds a payload ignores it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guidance: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyInfo {
    pub key: String,
    pub ty: String,
    pub note: String,
    pub aliases: Vec<String>,
}

/// Per-segment metadata. Lossless: the composed header suffix is a terse
/// projection of these fields, but the full set rides the envelope for tests and
/// the frontend.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentMeta {
    pub uri: String,
    pub kind: SegmentKind,
    pub natural_unit: NaturalUnit,
    /// Total lines / total matches / total files / total turns, when known.
    pub total_units: Option<usize>,
    pub shown_units: usize,
    pub offset: usize,
    pub limit: Option<usize>,
    /// grep: real matches (not context lines) in the COMPLETE result, before
    /// any `head_limit` window — see [`Self::grep_shown`].
    pub match_count: Option<usize>,
    /// grep/glob: files. For grep, files in the complete result.
    pub file_count: Option<usize>,
    /// `Record`/`Turn` unit: the plural noun for the header suffix (`"issues"`,
    /// `"messages"`, `"turns"`). For a grep segment, the unit its `head_limit`
    /// window counts (`"matches"`, or `"files"` under
    /// `files_with_matches`/`count`). `None` for other segments.
    pub unit_noun: Option<String>,
    /// grep: results the body carries when a `head_limit` window elided some of
    /// `total_units`, counted in [`Self::unit_noun`]'s unit. `None` when the
    /// body is the complete result.
    ///
    /// A windowed enumeration that reports only its own size is
    /// indistinguishable from a complete one, which silently turns "every caller
    /// of this symbol" into "the first few callers of this symbol". This field
    /// is what makes the header say `8 of 23 matches shown` and what earns the
    /// continue footer.
    #[serde(default)]
    pub grep_shown: Option<usize>,
    /// For line-framed record bodies, the number of leading physical lines that
    /// are metadata rather than records. This lets budget truncation translate
    /// retained lines back into truthful record counts and continuation offsets.
    #[serde(default)]
    pub record_prelude_lines: Option<usize>,
    /// `Record` unit with no countable total (`total_units` is `None`): the
    /// producer saw records past this page. A store log or an event scan knows
    /// whether another page exists without knowing how many records remain,
    /// and this is what earns such a page its continuation.
    #[serde(default)]
    pub more_records: bool,
    /// For a read served from a named revision (`?branch=`), the preformatted
    /// label naming what was read: the coordinate, its short commit, the
    /// commit's age, and for the default branch how fresh the store's copy of
    /// origin is. Formatted by the producer at read time so header composition
    /// stays a pure function of this metadata, identical live and archived.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
    /// The URI this segment's `continue:` footer names, present exactly when
    /// the renderer emits a footer that names one. The footer is prose for a
    /// reader; this is the same target for a program, which must take it from
    /// here rather than recover it from the text: a verbatim `sql=` value can
    /// carry a newline, which splits the footer across physical lines.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation: Option<String>,
    pub truncated: bool,
    /// True when a sub-line character fallback was used (single-huge-line case).
    pub char_continuation: bool,
}

impl SegmentMeta {
    /// A metadata skeleton for `uri`/`kind` with everything else zeroed.
    pub fn new(uri: impl Into<String>, kind: SegmentKind, natural_unit: NaturalUnit) -> Self {
        Self {
            uri: uri.into(),
            kind,
            natural_unit,
            total_units: None,
            shown_units: 0,
            offset: 0,
            limit: None,
            match_count: None,
            file_count: None,
            unit_noun: None,
            grep_shown: None,
            record_prelude_lines: None,
            more_records: false,
            revision: None,
            continuation: None,
            truncated: false,
            char_continuation: false,
        }
    }
}

/// One produced segment before windowing/truncation.
///
/// `body` is the windowed, line-numbered (file) or grep-formatted content the
/// producer rendered; the view layer applies the budget, char fallback, footer,
/// and the enriched header.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadSegment {
    pub body: String,
    pub affordance: Option<Affordance>,
    pub images: Vec<ImageBlock>,
    /// Issue history appended after the body.
    pub history: Option<String>,
    pub meta: SegmentMeta,
}

impl ReadSegment {
    /// A text/body-only segment carrying `meta`, no images/affordance/history.
    pub fn text(body: impl Into<String>, meta: SegmentMeta) -> Self {
        Self {
            body: body.into(),
            affordance: None,
            images: Vec::new(),
            history: None,
            meta,
        }
    }
}

/// The `read_batch` callback result over the cairn-cmd↔cairn-core boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadBatchEnvelope {
    /// Fully composed `=== uri [suffix] ===` batch text.
    pub text: String,
    pub images: Vec<ImageBlock>,
    /// Lossless per-segment metadata; never surfaced as its own content block.
    pub segments: Vec<SegmentMeta>,
    /// Structured affordances in segment order. Unlike the rendered blocks,
    /// these are never deduplicated or replaced by session pointers.
    #[serde(default)]
    pub affordances: Vec<Affordance>,
    /// Final per-segment bodies, in the same order as `segments`. Present only
    /// when the `read_batch` caller opts in with `include_bodies: true`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bodies: Option<Vec<String>>,
}

/// The `run` callback result over the cairn-cmd↔cairn-core boundary.
///
/// Mirrors [`ReadBatchEnvelope`]: the composed batch text plus any image content
/// blocks an external MCP `tools/call` returned (e.g. `cairn://mcp/axon/look`
/// with `screenshot:true`). The transport edge lifts each image into its own
/// content block after the text, so an image-bearing tool result reaches the
/// agent instead of being flattened to a text placeholder.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunBatchEnvelope {
    /// Fully composed batch text (the `=== <header> ===` item bodies).
    pub text: String,
    pub images: Vec<ImageBlock>,
    /// The call was refused as malformed before anything ran, and `text` says
    /// why. It is not retryable as sent: the caller must change the call. The
    /// transport edges surface it as an input error (an MCP tool error, the
    /// SDK's `CairnInputError`) so a script can tell 'you sent the wrong shape'
    /// from 'the machine failed'.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub invalid_input: bool,
    /// Each item's typed result, in input order and `null` for an item that
    /// has none; absent when no item has one. Only a model door
    /// (`cairn://models/<verb>`) produces one today, and `text` carries the same
    /// reply whole. It is the typed form, so the SDKs read it instead of
    /// parsing composed text, and the agent-facing edges ignore it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Vec<serde_json::Value>>,
    /// The items whose output was too large to return to code, each replaced
    /// in `text` by a one-line refusal; absent when every item fit. Only a
    /// batch composed for code (not for a model's context) carries every item
    /// whole and so can set it; the SDKs raise it as a typed error, and the
    /// agent-facing edges never see it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub over_ceiling: Option<OverCeiling>,
}

/// The run items a code-facing batch refused to return (see
/// [`RunBatchEnvelope::over_ceiling`]): each one's output was past
/// `ceiling_bytes`, or would have carried the batch past
/// `batch_ceiling_bytes`, which is measured as the text crosses the wire
/// ([`crate::executor_protocol::relay_wire_len`]) so the response fits one
/// executor-link frame.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OverCeiling {
    pub ceiling_bytes: usize,
    pub batch_ceiling_bytes: usize,
    pub items: Vec<OverCeilingItem>,
}

/// One item refused under [`OverCeiling`]: its position in the batch, its
/// header, and the size of the output it produced.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OverCeilingItem {
    pub index: usize,
    pub header: String,
    pub bytes: usize,
}

/// Share of an elided body's budget kept from its head (the opening context: a
/// command echo, the first lines). The rest keeps the tail, because command
/// output is read end-first: error summaries, test failures, the last `&&`
/// segment and exit codes all sit at the end.
const ELIDED_HEAD_PERCENT: usize = 15;

/// Bytes held back inside a budget for the elision marker, so head, marker and
/// tail together never exceed it.
const ELISION_MARKER_RESERVE: usize = 96;

/// `body` cut to at most `budget` bytes for a model's context: a short head and
/// a long tail, with a `--- elided N lines / M chars; tail kept below ---`
/// marker between them stating what was dropped. A body within budget comes
/// back unchanged. The tail runs to the body's byte-exact end, so a trailing
/// note survives, and starts on a whole line, so it never opens on a fragment
/// that reads as corrupted output; the skipped partial line counts in the
/// marker. This is the one elision a model sees for text that code receives
/// whole: a capped `run` item, and a decision node's input.
pub fn elide_tail_biased(body: &str, budget: usize) -> String {
    if body.len() <= budget {
        return body.to_string();
    }
    let content_budget = budget.saturating_sub(ELISION_MARKER_RESERVE);
    let head_budget = content_budget * ELIDED_HEAD_PERCENT / 100;
    let tail_budget = content_budget.saturating_sub(head_budget);

    // Head: snap down to a line boundary for a clean cut (only ever shrinks it).
    let head_end = floor_char_boundary(body, head_budget);
    let head_end = body[..head_end]
        .rfind('\n')
        .map(|i| i + 1)
        .unwrap_or(head_end);

    // Tail: the last `tail_budget` bytes, snapped up to a char boundary. The end
    // of the body is always retained, so a trailing note survives.
    let raw_tail_start = body.len().saturating_sub(tail_budget);
    let mut tail_start = ceil_char_boundary(body, raw_tail_start).max(head_end);

    // Advance the tail start to the next line boundary so the tail begins on a
    // whole line. Only advance when a newline leaves real content after it;
    // otherwise the byte-exact end (and any trailing note riding it) would be
    // elided. The skipped partial line folds into the elided span below,
    // keeping the marker count accurate.
    if tail_start > head_end {
        if let Some(rel) = body[tail_start..].find('\n') {
            let line_start = tail_start + rel + 1;
            if line_start < body.len() {
                tail_start = line_start;
            }
        }
    }

    let elided = &body[head_end..tail_start];
    let elided_chars = elided.len();
    let elided_lines = elided.matches('\n').count();
    let head = &body[..head_end];
    let tail = &body[tail_start..];
    let head_sep = if head.is_empty() || head.ends_with('\n') {
        ""
    } else {
        "\n"
    };
    format!(
        "{head}{head_sep}--- elided {elided_lines} lines / {elided_chars} chars; tail kept below ---\n{tail}"
    )
}

/// Largest byte index `<= idx` that is a UTF-8 char boundary.
fn floor_char_boundary(s: &str, mut idx: usize) -> usize {
    if idx >= s.len() {
        return s.len();
    }
    while idx > 0 && !s.is_char_boundary(idx) {
        idx -= 1;
    }
    idx
}

/// Smallest byte index `>= idx` that is a UTF-8 char boundary.
fn ceil_char_boundary(s: &str, mut idx: usize) -> usize {
    while idx < s.len() && !s.is_char_boundary(idx) {
        idx += 1;
    }
    idx
}

#[cfg(test)]
mod tests {
    use super::*;

    // A trailing detached-terminal note rides in the tail and is never elided.
    #[test]
    fn elision_preserves_a_trailing_note() {
        let note = "Command still running; detached to \
            cairn://p/cairn/1632/1/builder/terminal/run-1 — readable and killable there.";
        let body = format!("{}\n\n{}", "p".repeat(50_000), note);
        let capped = elide_tail_biased(&body, 4_000);
        assert!(capped.len() <= 4_000);
        assert!(
            capped.ends_with(note),
            "the trailing note must survive the cap"
        );
        assert!(capped.contains("--- elided"));
    }

    // The elision marker reports the actual dropped span.
    #[test]
    fn elision_counts_are_accurate() {
        let body = (0..1_000)
            .map(|i| format!("line-{i}"))
            .collect::<Vec<_>>()
            .join("\n");
        let capped = elide_tail_biased(&body, 2_000);
        // Parse "--- elided N lines / M chars; ..."
        let marker = capped
            .lines()
            .find(|l| l.starts_with("--- elided"))
            .expect("marker present");
        let chars: usize = marker
            .split("/ ")
            .nth(1)
            .and_then(|s| s.split(' ').next())
            .and_then(|s| s.parse().ok())
            .expect("char count parses");
        // Reconstructed length: head + tail + elided chars == original length.
        let head_tail: usize = capped
            .lines()
            .filter(|l| !l.starts_with("--- elided"))
            .map(|l| l.len() + 1)
            .sum::<usize>()
            .saturating_sub(1);
        // Within a few bytes of the original (newline accounting around the marker).
        let reconstructed = head_tail + chars;
        assert!(
            reconstructed.abs_diff(body.len()) <= 4,
            "reconstructed {reconstructed} vs original {}",
            body.len()
        );
    }

    // The retained tail begins on a whole source line, never a mid-line
    // fragment that would read as corrupted output (e.g. `94548` for `194548`).
    #[test]
    fn elision_starts_the_tail_on_a_complete_line() {
        // Variable-width lines: a byte-exact tail start would land mid-line and
        // surface a partial number; the snap-to-line must avoid that.
        let lines: Vec<String> = (0..4_000)
            .map(|i| format!("{}-entry", 90_000 + i * 7))
            .collect();
        let body = lines.join("\n");
        let capped = elide_tail_biased(&body, 2_000);
        let tail = capped
            .split_once("tail kept below ---\n")
            .map(|(_, t)| t)
            .expect("elision marker present");
        let first_tail_line = tail.lines().next().expect("a tail line");
        assert!(
            lines.iter().any(|l| l == first_tail_line),
            "first tail line {first_tail_line:?} is not a complete source line"
        );
    }

    // Multibyte content is sliced on char boundaries (no panic).
    #[test]
    fn elision_is_multibyte_boundary_safe() {
        let body = "é".repeat(40_000); // 2 bytes each → 80_000 bytes
        let capped = elide_tail_biased(&body, 5_000);
        assert!(capped.len() <= 5_000);
        assert!(capped.contains("--- elided"));
    }
}
