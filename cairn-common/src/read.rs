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
//! image blocks, and the per-segment metadata used in tests and (later) by the
//! frontend. The agent- and frontend-facing channel is the composed `text`
//! itself; the enriched header suffix is a terse, deterministic projection of
//! each segment's metadata, parseable without scraping body text.

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
}

/// A base64-encoded image block, surfaced as a separate `CallToolResult` content
/// block by the CLI after the composed text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImageBlock {
    pub mime_type: String,
    pub data: String,
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
    /// grep: real matches (not context lines).
    pub match_count: Option<usize>,
    /// grep/glob: files.
    pub file_count: Option<usize>,
    /// `Record`/`Turn` unit: the plural noun for the header suffix (`"issues"`,
    /// `"messages"`, `"turns"`). `None` for other segments.
    pub unit_noun: Option<String>,
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
}
