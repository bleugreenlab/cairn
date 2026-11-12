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

use std::borrow::Cow;

use serde::{Deserialize, Serialize};

use crate::security::Sanitizer;

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

/// A base64-encoded image block, surfaced as its own `CallToolResult` content
/// block by the transport edge at the place in the text that [`Self::at`] names.
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
    /// Byte offset into the text this block travels with (an item body, a
    /// segment, an envelope's text) at which the image sits. `None` places it
    /// after all of that text, including anything appended to it later. Every
    /// edit to anchored text goes through [`AnchoredText`], which keeps the
    /// offsets true.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub at: Option<usize>,
}

impl ImageBlock {
    /// An un-promoted block carrying only the inline bytes. Producers build this;
    /// the read path fills `uri` in afterward when it can store the bytes.
    pub fn inline(mime_type: impl Into<String>, data: impl Into<String>) -> Self {
        Self {
            mime_type: mime_type.into(),
            data: data.into(),
            uri: None,
            at: None,
        }
    }

    /// This block placed at byte offset `at` of the text it travels with.
    pub fn placed_at(mut self, at: usize) -> Self {
        self.at = Some(at);
        self
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
            at: None,
        }
    }
}

/// One block of an [`AnchoredText`] in delivery order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderedBlock<'a> {
    Text(&'a str),
    Image(&'a ImageBlock),
}

/// Text and the images placed in it: the one owner of the [`ImageBlock::at`]
/// invariants. Every edit made after images are anchored goes through here, so
/// an offset always names the same place in the text it was given for, and
/// always falls on a char boundary at or before the end.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AnchoredText {
    text: String,
    images: Vec<ImageBlock>,
}

impl AnchoredText {
    /// `text` with `images` placed in it. An anchor past the end is clamped to
    /// the end and one inside a char is floored to its start.
    pub fn new(text: impl Into<String>, mut images: Vec<ImageBlock>) -> Self {
        let text = text.into();
        for image in &mut images {
            image.at = image.at.map(|at| floor_char_boundary(&text, at));
        }
        Self { text, images }
    }

    pub fn text(&self) -> &str {
        &self.text
    }

    pub fn images(&self) -> &[ImageBlock] {
        &self.images
    }

    /// The images, for filling in what they carry (a promoted `uri`). Their
    /// anchors are edited only by this type's own methods.
    pub fn images_mut(&mut self) -> &mut [ImageBlock] {
        &mut self.images
    }

    pub fn len(&self) -> usize {
        self.text.len()
    }

    pub fn is_empty(&self) -> bool {
        self.text.is_empty()
    }

    /// Append `s`. Trailing images stay after it; anchored ones stay put.
    pub fn push_str(&mut self, s: &str) {
        self.text.push_str(s);
    }

    /// Put `s` before all of the text, shifting every anchor past it.
    pub fn prepend(&mut self, s: &str) {
        self.insert_str(0, s);
    }

    /// Insert `s` at byte offset `at` (floored to a char boundary). An image
    /// anchored at or after `at` moves with the text that followed it.
    pub fn insert_str(&mut self, at: usize, s: &str) {
        let at = floor_char_boundary(&self.text, at);
        self.text.insert_str(at, s);
        for image in &mut self.images {
            if let Some(anchor) = image.at.as_mut() {
                if *anchor >= at {
                    *anchor += s.len();
                }
            }
        }
    }

    /// Insert `s` immediately before image `index`: after whatever precedes it,
    /// including images that share its anchor and come before it, and before
    /// the image itself and the images that share its anchor after it. Before
    /// a trailing image that is the end of the text.
    pub fn insert_before_image(&mut self, index: usize, s: &str) {
        let Some(at) = self.images.get(index).and_then(|image| image.at) else {
            self.push_str(s);
            return;
        };
        self.text.insert_str(at, s);
        for (other, image) in self.images.iter_mut().enumerate() {
            if let Some(anchor) = image.at.as_mut() {
                if *anchor > at || (*anchor == at && other >= index) {
                    *anchor += s.len();
                }
            }
        }
    }

    /// Cut the text to `len` bytes (floored to a char boundary). An anchor past
    /// the cut moves to the new end.
    pub fn truncate(&mut self, len: usize) {
        let len = floor_char_boundary(&self.text, len);
        self.text.truncate(len);
        for image in &mut self.images {
            if let Some(anchor) = image.at.as_mut() {
                *anchor = (*anchor).min(len);
            }
        }
    }

    /// Fix every trailing image at the current end of the text, so text
    /// appended later goes after it.
    pub fn pin_trailing(&mut self) {
        let len = self.text.len();
        for image in &mut self.images {
            image.at.get_or_insert(len);
        }
    }

    /// Append `other`'s text and images. Its anchors are rebased onto the end
    /// of this text, and its trailing images stay trailing.
    pub fn append(&mut self, other: AnchoredText) {
        let base = self.text.len();
        self.text.push_str(&other.text);
        self.images
            .extend(other.images.into_iter().map(|mut image| {
                image.at = image.at.map(|at| at + base);
                image
            }));
    }

    /// [`elide_tail_biased`] over the text. Images are never dropped: one in
    /// the head stays put, one in the tail moves with it, and one in the elided
    /// span moves to the start of the kept tail, just after the marker line.
    pub fn elide_tail_biased(&mut self, budget: usize) {
        let Some(cut) = elision_cut(&self.text, budget) else {
            return;
        };
        let (text, tail_offset) = cut.apply(&self.text);
        for image in &mut self.images {
            if let Some(anchor) = image.at.as_mut() {
                *anchor = if *anchor <= cut.head_end {
                    *anchor
                } else if *anchor >= cut.tail_start {
                    *anchor - cut.tail_start + tail_offset
                } else {
                    tail_offset
                };
            }
        }
        self.text = text;
    }

    /// Redact what `sanitizer` matches without moving any image off the text
    /// it was placed against. A redaction changes the text's length, so an
    /// offset taken before it would name the wrong byte afterward: an image
    /// would land mid-word, or before its own citation.
    ///
    /// The whole text is redacted once, which is what `sanitizer` records.
    /// When anything matched, the pieces between anchors are redacted again
    /// on a shadow pass, and the anchors move to the redacted piece
    /// boundaries. That holds only when the pieces join to the whole-text
    /// result. If they don't, a match straddled an anchor, and the images
    /// fall back to trailing rather than any credential going out split.
    pub fn sanitize(&mut self, sanitizer: &mut Sanitizer<'_>) {
        let whole = match sanitizer.text(&self.text) {
            Cow::Borrowed(_) => return,
            Cow::Owned(whole) => whole,
        };
        let mut cuts: Vec<usize> = self
            .images
            .iter()
            .filter_map(|image| image.at)
            .map(|at| floor_char_boundary(&self.text, at))
            .collect();
        cuts.sort_unstable();
        cuts.dedup();
        if !cuts.is_empty() {
            let mut shadow = sanitizer.shadow();
            let mut pieces = String::with_capacity(whole.len());
            let mut moved = Vec::with_capacity(cuts.len());
            let mut start = 0;
            for &cut in &cuts {
                pieces.push_str(&shadow.text(&self.text[start..cut]));
                moved.push(pieces.len());
                start = cut;
            }
            pieces.push_str(&shadow.text(&self.text[start..]));
            let keeps_places = pieces == whole;
            for image in &mut self.images {
                image.at = match image.at {
                    Some(at) if keeps_places => cuts
                        .binary_search(&floor_char_boundary(&self.text, at))
                        .ok()
                        .map(|cut| moved[cut]),
                    _ => None,
                };
            }
        }
        self.text = whole;
    }

    /// Where `image` sits for delivery: its anchor when text follows it, or
    /// `None` for an image that comes after all of the text.
    fn placement(&self, image: &ImageBlock) -> Option<usize> {
        image
            .at
            .map(|at| floor_char_boundary(&self.text, at))
            .filter(|&at| at < self.text.len())
    }

    /// The text and its images in delivery order, for a carrier. An image
    /// with no text after it carries `at: None`, so a text-then-image result
    /// serializes exactly as it did before images had anchors.
    pub fn into_parts(self) -> (String, Vec<ImageBlock>) {
        let Self { text, mut images } = self;
        // Pinned at the very end sorts before trailing, which is where it
        // stood: text appended later would have followed it.
        images.sort_by_key(|image| {
            image
                .at
                .map_or(usize::MAX, |at| floor_char_boundary(&text, at))
        });
        for image in &mut images {
            image.at = image
                .at
                .map(|at| floor_char_boundary(&text, at))
                .filter(|&at| at < text.len());
        }
        (text, images)
    }

    /// The text split around its images, in delivery order.
    ///
    /// With no images, or with every image after all of the text, this is
    /// exactly `[Text(text), images…]`, the shape every result had before
    /// images had anchors, and still one text block when the text is empty.
    ///
    /// Otherwise the text is split at each distinct anchor (images sharing one
    /// keep their order). Every split between two non-empty pieces consumes one
    /// newline beside it: the one ending the piece before if there is one,
    /// otherwise the one starting the piece after. Empty pieces are dropped.
    /// Model clients join a tool result's text parts with `\n` when they keep
    /// them as text, so the consumed newline is what keeps that join equal to
    /// [`Self::text`]. The join is exact when every anchor between two
    /// non-empty pieces has a newline beside it and the text between two image
    /// positions is never a lone newline. A split with no newline beside it
    /// joins with one newline more than the text has, and a lone newline
    /// between two images is consumed whole, so that join has one fewer.
    pub fn blocks(&self) -> Vec<OrderedBlock<'_>> {
        let len = self.text.len();
        let mut order: Vec<(Option<usize>, &ImageBlock)> = self
            .images
            .iter()
            .map(|image| (self.placement(image), image))
            .collect();
        order.sort_by_key(|(placement, _)| placement.unwrap_or(usize::MAX));
        let split = order.partition_point(|(placement, _)| placement.is_some());
        let (placed, trailing) = order.split_at(split);

        let mut blocks = Vec::with_capacity(2 * placed.len() + trailing.len() + 1);
        if placed.is_empty() {
            blocks.push(OrderedBlock::Text(&self.text));
        } else {
            // Raw pieces between distinct anchors, and the images after each.
            let mut pieces: Vec<(usize, usize)> = Vec::new();
            let mut groups: Vec<&[(Option<usize>, &ImageBlock)]> = Vec::new();
            let mut start = 0;
            let mut first = 0;
            while first < placed.len() {
                let anchor = placed[first].0;
                let end = first
                    + placed[first..]
                        .iter()
                        .take_while(|(placement, _)| *placement == anchor)
                        .count();
                let at = anchor.unwrap_or(len);
                pieces.push((start, at));
                groups.push(&placed[first..end]);
                start = at;
                first = end;
            }
            pieces.push((start, len));

            let bytes = self.text.as_bytes();
            for split in 0..groups.len() {
                let (before_start, before_end) = pieces[split];
                let (after_start, after_end) = pieces[split + 1];
                if before_start == before_end || after_start == after_end {
                    continue;
                }
                if bytes[before_end - 1] == b'\n' {
                    pieces[split].1 -= 1;
                } else if bytes[after_start] == b'\n' {
                    pieces[split + 1].0 += 1;
                }
            }

            for (index, &(start, end)) in pieces.iter().enumerate() {
                if start < end {
                    blocks.push(OrderedBlock::Text(&self.text[start..end]));
                }
                if let Some(group) = groups.get(index) {
                    blocks.extend(group.iter().map(|(_, image)| OrderedBlock::Image(image)));
                }
            }
        }
        blocks.extend(trailing.iter().map(|(_, image)| OrderedBlock::Image(image)));
        blocks
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
    /// How many of the envelope's `images` this segment contributed, in
    /// segment order, so a caller that splits a batch by segment can split its
    /// images the same way.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub image_count: usize,
}

fn is_zero(count: &usize) -> bool {
    *count == 0
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
            image_count: 0,
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
    /// The segments' images in segment order, each placed in `text` at its
    /// [`ImageBlock::at`]: after its own segment's text, or after everything
    /// for the last segment's.
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
/// blocks an item produced (an external MCP `tools/call` such as
/// `cairn://mcp/axon/look` with `screenshot:true`, or `cairn://models/image`).
/// The transport edge delivers each image as its own content block at the place
/// in the text its [`ImageBlock::at`] names ([`AnchoredText::blocks`]), so an
/// image-bearing result reaches the agent in the order its producer gave it
/// instead of being flattened to a text placeholder or hoisted past the text.
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
    match elision_cut(body, budget) {
        Some(cut) => cut.apply(body).0,
        None => body.to_string(),
    }
}

/// Where [`elide_tail_biased`] cuts a body: it keeps `..head_end` and
/// `tail_start..`, and drops what lies between.
struct ElisionCut {
    head_end: usize,
    tail_start: usize,
}

impl ElisionCut {
    /// The elided text, and the offset in it where the kept tail starts.
    fn apply(&self, body: &str) -> (String, usize) {
        let elided = &body[self.head_end..self.tail_start];
        let elided_chars = elided.len();
        let elided_lines = elided.matches('\n').count();
        let head = &body[..self.head_end];
        let tail = &body[self.tail_start..];
        let head_sep = if head.is_empty() || head.ends_with('\n') {
            ""
        } else {
            "\n"
        };
        let text = format!(
            "{head}{head_sep}--- elided {elided_lines} lines / {elided_chars} chars; tail kept below ---\n{tail}"
        );
        let tail_offset = text.len() - tail.len();
        (text, tail_offset)
    }
}

/// The cut for `body` under `budget`, or `None` when it fits whole.
fn elision_cut(body: &str, budget: usize) -> Option<ElisionCut> {
    if body.len() <= budget {
        return None;
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

    Some(ElisionCut {
        head_end,
        tail_start,
    })
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

    fn image(data: &str) -> ImageBlock {
        ImageBlock::inline("image/png", data)
    }

    /// Blocks as `T:<text>` / `I:<data>` labels, for order assertions.
    fn shape(anchored: &AnchoredText) -> Vec<String> {
        anchored
            .blocks()
            .into_iter()
            .map(|block| match block {
                OrderedBlock::Text(text) => format!("T:{text}"),
                OrderedBlock::Image(image) => format!("I:{}", image.data),
            })
            .collect()
    }

    /// The text blocks joined the way a model client joins tool-result text.
    fn joined(anchored: &AnchoredText) -> String {
        anchored
            .blocks()
            .into_iter()
            .filter_map(|block| match block {
                OrderedBlock::Text(text) => Some(text),
                OrderedBlock::Image(_) => None,
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn a_text_only_result_is_one_text_block_even_when_empty() {
        assert_eq!(shape(&AnchoredText::new("hello", vec![])), ["T:hello"]);
        assert_eq!(shape(&AnchoredText::new("", vec![])), ["T:"]);
    }

    #[test]
    fn trailing_images_keep_the_shape_results_had_before_anchors() {
        let anchored = AnchoredText::new("tree", vec![image("a"), image("b")]);
        assert_eq!(shape(&anchored), ["T:tree", "I:a", "I:b"]);
        // An image-only result still opens with its (empty) text block.
        let alone = AnchoredText::new("", vec![image("a")]);
        assert_eq!(shape(&alone), ["T:", "I:a"]);
        // An anchor at the very end is trailing too.
        let pinned = AnchoredText::new("tree", vec![image("a").placed_at(4)]);
        assert_eq!(shape(&pinned), ["T:tree", "I:a"]);
    }

    #[test]
    fn an_image_before_its_text_comes_first() {
        let anchored = AnchoredText::new("caption", vec![image("a").placed_at(0)]);
        assert_eq!(shape(&anchored), ["I:a", "T:caption"]);
        assert_eq!(joined(&anchored), "caption");
    }

    #[test]
    fn an_image_between_texts_splits_them_and_consumes_one_newline() {
        // Anchored at the start of the second part, as the gateway places it.
        let anchored = AnchoredText::new("before\nafter", vec![image("a").placed_at(7)]);
        assert_eq!(shape(&anchored), ["T:before", "I:a", "T:after"]);
        assert_eq!(joined(&anchored), anchored.text());

        // Anchored before the newline: the one starting the next piece goes.
        let anchored = AnchoredText::new("before\nafter", vec![image("a").placed_at(6)]);
        assert_eq!(shape(&anchored), ["T:before", "I:a", "T:after"]);
        assert_eq!(joined(&anchored), anchored.text());

        // A blank line keeps its other newline.
        let anchored = AnchoredText::new("before\n\nafter", vec![image("a").placed_at(7)]);
        assert_eq!(shape(&anchored), ["T:before", "I:a", "T:\nafter"]);
        assert_eq!(joined(&anchored), anchored.text());
    }

    #[test]
    fn images_sharing_an_anchor_keep_their_order() {
        let anchored = AnchoredText::new(
            "x\ny",
            vec![image("b").placed_at(2), image("t"), image("c").placed_at(2)],
        );
        assert_eq!(shape(&anchored), ["T:x", "I:b", "I:c", "T:y", "I:t"]);
    }

    #[test]
    fn the_join_is_off_by_one_newline_only_where_the_rule_says() {
        // No newline beside the anchor: the join gains one.
        let bare = AnchoredText::new("ab", vec![image("a").placed_at(1)]);
        assert_eq!(shape(&bare), ["T:a", "I:a", "T:b"]);
        assert_eq!(joined(&bare), "a\nb");
        // A lone newline between two images is consumed whole: one fewer.
        let lone = AnchoredText::new(
            "a\n\nb",
            vec![image("a").placed_at(2), image("b").placed_at(3)],
        );
        assert_eq!(shape(&lone), ["T:a", "I:a", "I:b", "T:b"]);
        assert_eq!(joined(&lone), "a\nb");
    }

    #[test]
    fn anchors_are_clamped_and_floored_to_char_boundaries() {
        let anchored = AnchoredText::new(
            "éx",
            vec![image("a").placed_at(1), image("b").placed_at(99)],
        );
        assert_eq!(anchored.images()[0].at, Some(0));
        assert_eq!(anchored.images()[1].at, Some(3));
        let (text, images) = anchored.into_parts();
        assert_eq!(text, "éx");
        assert_eq!(images[0].at, Some(0));
        // Past the end is after all of the text: trailing on the wire.
        assert_eq!(images[1].at, None);
    }

    #[test]
    fn into_parts_orders_images_by_place_with_trailing_last() {
        let anchored = AnchoredText::new(
            "one\ntwo\n",
            vec![image("t"), image("b").placed_at(4), image("a").placed_at(0)],
        );
        let (_, images) = anchored.into_parts();
        let order: Vec<_> = images.iter().map(|image| image.data.as_str()).collect();
        assert_eq!(order, ["a", "b", "t"]);
        assert_eq!(images[2].at, None);
    }

    #[test]
    fn edits_keep_every_anchor_on_the_text_it_named() {
        let mut anchored =
            AnchoredText::new("body\nmore", vec![image("a").placed_at(5), image("t")]);
        anchored.prepend("=== h ===\n");
        assert_eq!(anchored.images()[0].at, Some(15));
        assert!(anchored.text()[15..].starts_with("more"));
        anchored.push_str("\nnote");
        assert_eq!(anchored.images()[0].at, Some(15));
        assert_eq!(anchored.images()[1].at, None);
        anchored.insert_str(15, "X");
        assert!(anchored.text()[16..].starts_with("more"));
        anchored.truncate(12);
        assert_eq!(anchored.images()[0].at, Some(12));

        let mut pinned = AnchoredText::new("out", vec![image("t")]);
        pinned.pin_trailing();
        pinned.push_str("\nnext");
        assert_eq!(shape(&pinned), ["T:out", "I:t", "T:next"]);
    }

    #[test]
    fn append_rebases_the_other_text_and_keeps_its_trailing_images_trailing() {
        let mut batch = AnchoredText::new("first", vec![image("p")]);
        batch.pin_trailing();
        batch.push_str("\n");
        batch.append(AnchoredText::new(
            "x\ny",
            vec![image("a").placed_at(2), image("t")],
        ));
        assert_eq!(batch.text(), "first\nx\ny");
        assert_eq!(
            shape(&batch),
            ["T:first", "I:p", "T:x", "I:a", "T:y", "I:t"]
        );
        assert_eq!(joined(&batch), batch.text());
    }

    #[test]
    fn insert_before_image_lands_between_it_and_what_precedes_it() {
        let mut anchored = AnchoredText::new(
            "x\ny",
            vec![image("a").placed_at(2), image("b").placed_at(2)],
        );
        anchored.insert_before_image(0, "A\n");
        anchored.insert_before_image(1, "B\n");
        assert_eq!(anchored.text(), "x\nA\nB\ny");
        assert_eq!(shape(&anchored), ["T:x\nA", "I:a", "T:B", "I:b", "T:y"]);
        assert_eq!(joined(&anchored), anchored.text());

        let mut trailing = AnchoredText::new("x", vec![image("t")]);
        trailing.insert_before_image(0, "\ncite");
        assert_eq!(trailing.text(), "x\ncite");
    }

    #[test]
    fn elision_keeps_every_image_and_remaps_its_anchor() {
        let lines: Vec<String> = (0..2_000).map(|i| format!("line-{i}")).collect();
        let body = lines.join("\n");
        let head_line = body.find("line-1\n").unwrap();
        let middle_line = body.find("line-1000\n").unwrap();
        let tail_line = body.find("line-1999").unwrap();
        let mut anchored = AnchoredText::new(
            body.clone(),
            vec![
                image("head").placed_at(head_line),
                image("middle").placed_at(middle_line),
                image("tail").placed_at(tail_line),
                image("trailing"),
            ],
        );
        anchored.elide_tail_biased(4_000);
        let text = anchored.text();
        assert_eq!(
            text,
            elide_tail_biased(&body, 4_000),
            "text elides as before"
        );
        let at = |data: &str| {
            anchored
                .images()
                .iter()
                .find(|image| image.data == data)
                .and_then(|image| image.at)
        };
        assert!(text[at("head").unwrap()..].starts_with("line-1\n"));
        let marker_end =
            text.find("tail kept below ---\n").unwrap() + "tail kept below ---\n".len();
        assert_eq!(
            at("middle"),
            Some(marker_end),
            "an elided image moves to the tail's start"
        );
        assert!(text[at("tail").unwrap()..].starts_with("line-1999"));
        assert_eq!(at("trailing"), None);
        assert_eq!(anchored.images().len(), 4);
        assert_eq!(joined(&anchored), anchored.text());
    }

    const SECRET: &str = "sk-live-Qa9Zm2Xp7Lr4Wd3Ty8Uv5Kn0Hb6Jc1";

    fn sanitizer() -> (
        crate::security::registry::SecretRegistry,
        Sanitizer<'static>,
    ) {
        use crate::security::{SecretCategory, SecretId, SecretMaterial};
        let registry = crate::security::registry::SecretRegistry::new();
        registry
            .register(
                SecretId::new("test"),
                SecretCategory::CallbackCredential,
                "unit test",
                SecretMaterial::from_string(SECRET.to_string()),
            )
            .expect("registerable")
            .retain_for_process();
        let sanitizer = Sanitizer::with_snapshot(registry.snapshot(), None);
        (registry, sanitizer)
    }

    #[test]
    fn redaction_keeps_every_image_beside_the_text_it_was_placed_against() {
        let (_registry, mut sanitizer) = sanitizer();
        let text = format!("TOKEN={SECRET}\n=== look ===\ncite\nits description");
        let at = text.find("its description").unwrap();
        let mut anchored = AnchoredText::new(text, vec![image("a").placed_at(at), image("t")]);
        anchored.sanitize(&mut sanitizer);
        assert!(!anchored.text().contains(SECRET));
        assert!(
            sanitizer.detections().has_exact(),
            "the real pass records the match"
        );
        assert_eq!(
            shape(&anchored),
            [
                "T:TOKEN=[REDACTED]\n=== look ===\ncite",
                "I:a",
                "T:its description",
                "I:t"
            ]
        );
        assert_eq!(joined(&anchored), anchored.text());

        // A credential after the image moves nothing before it.
        let text = format!("cite\nits description {SECRET}");
        let mut anchored = AnchoredText::new(text, vec![image("a").placed_at(5)]);
        anchored.sanitize(&mut sanitizer);
        assert_eq!(anchored.images()[0].at, Some(5));
    }

    #[test]
    fn redaction_with_nothing_to_redact_leaves_everything_alone() {
        let (_registry, mut sanitizer) = sanitizer();
        let before = AnchoredText::new("cite\nits description", vec![image("a").placed_at(5)]);
        let mut after = before.clone();
        after.sanitize(&mut sanitizer);
        assert_eq!(after, before);
        assert!(sanitizer.detections().is_empty());
    }

    #[test]
    fn a_credential_straddling_an_anchor_is_redacted_whole_and_the_image_trails() {
        let (_registry, mut sanitizer) = sanitizer();
        // Only possible for an anchor with no newline beside it; the producers
        // never place one, but the fallback must not leak either half.
        let text = format!("key {SECRET} end");
        let mut anchored = AnchoredText::new(text, vec![image("a").placed_at(10)]);
        anchored.sanitize(&mut sanitizer);
        assert_eq!(anchored.text(), "key [REDACTED] end");
        assert_eq!(anchored.images()[0].at, None);
    }

    #[test]
    fn an_image_without_an_anchor_decodes_as_trailing() {
        let old = r#"{"mime_type":"image/png","data":"b64"}"#;
        let decoded: ImageBlock = serde_json::from_str(old).unwrap();
        assert_eq!(decoded, image("b64"));
        // And a trailing one serializes exactly as it did before anchors.
        assert_eq!(serde_json::to_string(&decoded).unwrap(), old);
        let placed = serde_json::to_string(&image("b64").placed_at(3)).unwrap();
        assert_eq!(
            serde_json::from_str::<ImageBlock>(&placed).unwrap().at,
            Some(3)
        );
    }
}
