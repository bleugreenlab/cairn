//! Output composition: per-item body capping, batch water-filling, the run
//! envelope, and the streamable-`tail` pipeline transform.

use super::types::ItemOutcome;
use crate::mcp::handlers::read::view;
use crate::mcp::types::McpCallbackRequest;
use cairn_common::executor_protocol::{relay_wire_len, RELAY_RESPONSE_BYTE_BUDGET};
use cairn_common::read::{
    AnchoredText, ImageBlock, OverCeiling, OverCeilingItem, RunBatchEnvelope,
};

/// What a batch composed for code may carry in total, measured as its item
/// bodies cost on the wire ([`relay_wire_len`]). A script's call usually
/// reaches the host through an executor's relay, whose response crosses the
/// link as one frame, so a batch is bounded below [`RELAY_RESPONSE_BYTE_BUDGET`]
/// with room left for item headers, refusal lines, a model door's `data`,
/// images, and reminders.
const WHOLE_RUN_BATCH_WIRE_BYTES: usize = 12 << 20;
const _: () = assert!(WHOLE_RUN_BATCH_WIRE_BYTES < RELAY_RESPONSE_BYTE_BUDGET);

/// Max bytes the capped items of a batch composed for a model's context
/// ([`RunOutputBudget::Context`]) share: command output is read end-first, and
/// this keeps it below the size at which an agent CLI moves a tool result out
/// of the conversation. An item carried whole ([`ItemOutcome::whole`], a model
/// door's reply) is outside it, so a batch holding one can compose past it; the
/// agent-facing edges pass the text through untouched either way. A batch
/// composed for code never applies it.
pub(crate) const MAX_RUN_RESULT_BYTES: usize = 45_000;

/// How much of each item a composed batch result may carry, chosen by whoever
/// the result is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RunOutputBudget {
    /// The result lands in a model's context: the capped items share
    /// [`MAX_RUN_RESULT_BYTES`], each tail-biased within its share.
    Context,
    /// The result lands in code that parses it: every item is carried whole,
    /// and one whose output is past `byte_ceiling`, or that would carry the
    /// batch's wire cost past `batch_wire_ceiling`, is refused by type
    /// ([`ComposedRun::over_ceiling`]) rather than cut.
    Whole {
        byte_ceiling: usize,
        batch_wire_ceiling: usize,
    },
}

/// The budget a `run` batch's output composes under. The 45k cap exists to
/// keep a batch result from flooding a model's context, so only a call a
/// model-facing edge stamped (the MCP tools, the HTTP-loop backend) composes
/// under it. Every other caller hands the text to code that parses it: an
/// `@cairn/sdk` script, a service's `ctx.cairn`, a `@cairn/harness` workflow,
/// a custom shell action. Those receive every item whole, up to
/// [`view::WHOLE_READ_BYTE_CEILING`] per item and [`WHOLE_RUN_BATCH_WIRE_BYTES`]
/// for the batch. An agent that runs one of them
/// inside its own `run` still sees only what that outer batch's cap lets
/// through, so the guard sits where text enters a model's context.
pub(super) fn run_budget_for(request: &McpCallbackRequest) -> RunOutputBudget {
    if request.is_model_facing() {
        RunOutputBudget::Context
    } else {
        RunOutputBudget::Whole {
            byte_ceiling: view::WHOLE_READ_BYTE_CEILING,
            batch_wire_ceiling: WHOLE_RUN_BATCH_WIRE_BYTES,
        }
    }
}

/// A composed batch result: its text, its items' images placed in that text,
/// and under [`RunOutputBudget::Whole`] the items refused for being past the
/// ceiling.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ComposedRun {
    pub(super) text: String,
    pub(super) images: Vec<ImageBlock>,
    pub(super) over_ceiling: Option<OverCeiling>,
}

impl ComposedRun {
    fn new(output: AnchoredText, over_ceiling: Option<OverCeiling>) -> Self {
        let (text, images) = output.into_parts();
        Self {
            text,
            images,
            over_ceiling,
        }
    }

    /// The text and images, for an edit that keeps the images in place.
    pub(super) fn output(&self) -> AnchoredText {
        AnchoredText::new(self.text.clone(), self.images.clone())
    }
}

/// Smallest per-item body budget worth showing. Below this an item is omitted
/// whole (the last-resort path) rather than reduced to a useless sliver. Sized
/// well above any trailing detached-terminal note, so the tail cap of a
/// non-omitted item never elides that note.
const MIN_ITEM_BODY_BUDGET: usize = 512;

/// Headroom held back from the batch cap for the trailing omission note and
/// inter-segment separators, so the composed result stays under the cap.
const OMISSION_NOTE_RESERVE: usize = 256;

/// Render one item's body block (no header) with its images in place:
/// tail-capped to `body_budget` unless the item is carried whole, with an empty
/// body shown as `(no output)`.
fn render_item_body(outcome: &ItemOutcome, body_budget: usize) -> AnchoredText {
    let mut body = whole_item_body(outcome);
    if !outcome.whole && !outcome.body.is_empty() {
        body.elide_tail_biased(body_budget);
    }
    body
}

/// One item's full body block (no header) with its images, `(no output)` when
/// empty.
fn whole_item_body(outcome: &ItemOutcome) -> AnchoredText {
    let body = if outcome.body.is_empty() {
        "(no output)".to_string()
    } else {
        outcome.body.clone()
    };
    AnchoredText::new(body, outcome.images.clone())
}

/// Add one labeled item to a batch under composition. An item that is not the
/// batch's last pins its trailing images to the end of its own text, so they
/// arrive before the next item's header rather than after the whole batch.
fn push_item(batch: &mut AnchoredText, header: &str, mut body: AnchoredText, last: bool) {
    if !last {
        body.pin_trailing();
    }
    body.prepend(&format!("=== {header} ===\n"));
    if !batch.is_empty() {
        batch.push_str("\n");
    }
    batch.append(body);
}

/// `bytes` as a ceiling is stated: whole MiB when it is one, else bytes.
fn ceiling_label(bytes: usize) -> String {
    const MIB: usize = 1 << 20;
    if bytes >= MIB && bytes.is_multiple_of(MIB) {
        format!("{} MiB", bytes / MIB)
    } else {
        format!("{bytes} bytes")
    }
}

/// Divide `total` across items by their `natural` (uncapped) sizes: items smaller
/// than their equal share take their full size and donate the surplus to larger
/// items (classic water-filling). Each returned allocation is at least the equal
/// division of whatever budget remained when that item was filled.
fn water_fill_budgets(natural: &[usize], total: usize) -> Vec<usize> {
    let n = natural.len();
    let mut alloc = vec![0usize; n];
    let mut order: Vec<usize> = (0..n).collect();
    order.sort_by_key(|&i| natural[i]);
    let mut remaining = total;
    let mut left = n;
    for &i in &order {
        let share = remaining / left.max(1);
        let give = natural[i].min(share);
        alloc[i] = give;
        remaining = remaining.saturating_sub(give);
        left -= 1;
    }
    alloc
}

/// Serialize the run result into a [`RunBatchEnvelope`] for the transport edge.
///
/// The composed `text` and any `images` ride together so the transport edge
/// delivers each image as its own content block at its place in the text,
/// mirroring the read-batch envelope.
/// Serializing a struct of string/array fields does not fail in practice; the
/// fallback returns the bare text so a run never errors on rendering.
pub(super) fn run_envelope(text: String, images: Vec<ImageBlock>) -> String {
    run_envelope_with_data(text, images, None, None)
}

/// [`run_envelope`] carrying each item's typed result ([`collect_run_data`])
/// and the items refused past the code-facing ceiling ([`ComposedRun`]).
pub(super) fn run_envelope_with_data(
    text: String,
    images: Vec<ImageBlock>,
    data: Option<Vec<serde_json::Value>>,
    over_ceiling: Option<OverCeiling>,
) -> String {
    encode_envelope(RunBatchEnvelope {
        text,
        images,
        data,
        invalid_input: false,
        over_ceiling,
    })
}

/// The envelope for a call refused as malformed before anything ran. Marked so
/// the transport edges surface it as an input error rather than a batch result.
pub(super) fn run_input_refusal(text: String) -> String {
    encode_envelope(RunBatchEnvelope {
        text,
        images: Vec::new(),
        invalid_input: true,
        data: None,
        over_ceiling: None,
    })
}

fn encode_envelope(envelope: RunBatchEnvelope) -> String {
    serde_json::to_string(&envelope).unwrap_or(envelope.text)
}

/// Every item's typed result in item order, `null` for an item without one;
/// `None` when no item has one, so an ordinary batch's envelope is unchanged.
pub(super) fn collect_run_data(outcomes: &mut [ItemOutcome]) -> Option<Vec<serde_json::Value>> {
    if outcomes.iter().all(|outcome| outcome.data.is_none()) {
        return None;
    }
    Some(
        outcomes
            .iter_mut()
            .map(|outcome| outcome.data.take().unwrap_or(serde_json::Value::Null))
            .collect(),
    )
}

/// Compose per-item outcomes into a single result under `budget`, which the
/// caller chooses for who the result is for ([`run_budget_for`]). It is never
/// read from the payload: a result's audience is a fact about the caller, not
/// a request a caller may make.
///
/// A single item renders its body directly with no header; several are
/// labeled `=== <header> ===` in input order, under either budget. Every item's
/// images arrive in place: inside its text where it anchored them, and after
/// its text (before the next item) where it did not. The last item's trailing
/// images stay trailing, so a lone text-then-image item composes exactly as
/// it did before images had places.
pub(super) fn compose_run_output(outcomes: &[ItemOutcome], budget: RunOutputBudget) -> ComposedRun {
    match budget {
        RunOutputBudget::Context => ComposedRun::new(compose_for_context(outcomes), None),
        RunOutputBudget::Whole {
            byte_ceiling,
            batch_wire_ceiling,
        } => compose_whole(outcomes, byte_ceiling, batch_wire_ceiling),
    }
}

/// [`RunOutputBudget::Whole`]: every item rendered in full, never water-filled,
/// elided, or omitted. An item past `byte_ceiling` is replaced by a one-line
/// refusal and listed in [`ComposedRun::over_ceiling`], so a script fails by
/// type rather than parsing cut text. An item already carried whole is outside
/// the per-item ceiling, as it is outside the context cap: a served read was
/// bounded by read itself, and a model door's reply is data.
///
/// Every item, whole ones included, counts toward `batch_wire_ceiling` in
/// input order. An item that would carry the batch past it is refused the same
/// way, so a batch's response always fits the executor link's frame and the
/// caller gets a typed refusal instead of a dropped link.
fn compose_whole(
    outcomes: &[ItemOutcome],
    byte_ceiling: usize,
    batch_wire_ceiling: usize,
) -> ComposedRun {
    let mut refused: Vec<OverCeilingItem> = Vec::new();
    let mut wire_spent = 0usize;
    let bodies: Vec<AnchoredText> = outcomes
        .iter()
        .enumerate()
        .map(|(index, outcome)| {
            let bytes = outcome.body.len();
            let refusal = if !outcome.whole && bytes > byte_ceiling {
                format!(
                    "`{}`: output of {bytes} bytes exceeds the {} ceiling for a result returned to code; write it to a file and read that instead.",
                    outcome.header,
                    ceiling_label(byte_ceiling),
                )
            } else {
                let wire = relay_wire_len(&outcome.body);
                if wire_spent + wire <= batch_wire_ceiling {
                    wire_spent += wire;
                    return whole_item_body(outcome);
                }
                format!(
                    "`{}`: output of {bytes} bytes would carry this batch past the {} a result returned to code may hold, measured as it crosses the wire; write it to a file and read that instead.",
                    outcome.header,
                    ceiling_label(batch_wire_ceiling),
                )
            };
            refused.push(OverCeilingItem {
                index,
                header: outcome.header.clone(),
                bytes,
            });
            // The refused body's places are gone, but its images still arrive,
            // after the line that stands in for it.
            let images = outcome
                .images
                .iter()
                .cloned()
                .map(|mut image| {
                    image.at = None;
                    image
                })
                .collect();
            AnchoredText::new(refusal, images)
        })
        .collect();
    let output = if outcomes.len() == 1 {
        bodies.into_iter().next().unwrap_or_default()
    } else {
        let last = outcomes.len() - 1;
        let mut batch = AnchoredText::default();
        for (index, (outcome, body)) in outcomes.iter().zip(bodies).enumerate() {
            push_item(&mut batch, &outcome.header, body, index == last);
        }
        batch
    };
    let over_ceiling = (!refused.is_empty()).then_some(OverCeiling {
        ceiling_bytes: byte_ceiling,
        batch_ceiling_bytes: batch_wire_ceiling,
        items: refused,
    });
    ComposedRun::new(output, over_ceiling)
}

/// [`RunOutputBudget::Context`]: `MAX_RUN_RESULT_BYTES` is divided fairly
/// across the capped items (water-filling) and each one's body is tail-capped
/// within its share, so every item surfaces meaningful output. Whole-item
/// omission, with a named note, remains only as a last resort for pathological
/// item counts where the per-item share falls below a usable floor.
///
/// An item carried whole ([`ItemOutcome::whole`]) is never capped, shared, or
/// omitted: it renders its full body and takes nothing from the other items'
/// budget. Without one, the composed result stays under `MAX_RUN_RESULT_BYTES`.
/// Images sit outside that budget: capping moves them but never drops them,
/// and an omitted item's images still arrive, where the item would have been.
fn compose_for_context(outcomes: &[ItemOutcome]) -> AnchoredText {
    if outcomes.len() == 1 {
        return render_item_body(&outcomes[0], MAX_RUN_RESULT_BYTES);
    }

    let headers: Vec<String> = outcomes
        .iter()
        .map(|o| format!("=== {} ===\n", o.header))
        .collect();
    // Fixed per-item cost that must always be present: the header line plus one
    // inter-segment separator.
    let overhead: Vec<usize> = headers.iter().map(|h| h.len() + 1).collect();
    let natural: Vec<usize> = outcomes
        .iter()
        .enumerate()
        .map(|(i, o)| {
            let body_len = if o.body.is_empty() {
                "(no output)".len()
            } else {
                o.body.len()
            };
            overhead[i] + body_len
        })
        .collect();

    let total_budget = MAX_RUN_RESULT_BYTES.saturating_sub(OMISSION_NOTE_RESERVE);
    let capped: Vec<usize> = (0..outcomes.len())
        .filter(|&i| !outcomes[i].whole)
        .collect();
    let shares = water_fill_budgets(
        &capped.iter().map(|&i| natural[i]).collect::<Vec<_>>(),
        total_budget,
    );
    // A whole item is allotted its natural size, so it always fits below.
    let mut alloc = natural.clone();
    for (&i, share) in capped.iter().zip(shares) {
        alloc[i] = share;
    }

    let last = outcomes.len() - 1;
    let mut text = AnchoredText::default();
    let mut omitted: Vec<String> = Vec::new();
    for (i, o) in outcomes.iter().enumerate() {
        let body_budget = alloc[i].saturating_sub(overhead[i]);
        let natural_body = natural[i] - overhead[i];
        // Last resort: an item that does not fit and whose share is below the
        // usable floor is omitted whole rather than shown as a sliver.
        if natural_body > body_budget && body_budget < MIN_ITEM_BODY_BUDGET {
            omitted.push(o.header.clone());
            let mut images = AnchoredText::new("", o.images.clone());
            if i != last {
                images.pin_trailing();
            }
            text.append(images);
            continue;
        }
        push_item(
            &mut text,
            &o.header,
            render_item_body(o, body_budget),
            i == last,
        );
    }

    if !omitted.is_empty() {
        if !text.is_empty() {
            text.push_str("\n\n");
        }
        text.push_str(&format!(
            "--- {} of {} items omitted (batch {}K char cap); re-run each scoped (grep/tail the command) or open a terminal resource: {} ---",
            omitted.len(),
            outcomes.len(),
            MAX_RUN_RESULT_BYTES / 1000,
            omitted.join(", "),
        ));
    }

    text
}

/// A recognized trailing `tail` pipeline stage whose only effect is to keep the
/// last N lines of stdout. A trailing `tail` buffers all upstream output until
/// EOF, so the live preview stays blank for the whole run; stripping it lets the
/// full output stream, and re-applying [`OutputTail::apply`] to the captured
/// stdout keeps the recorded result exactly what the agent asked for.
pub(super) struct OutputTail {
    /// Number of trailing lines to keep. GNU/BSD `tail` defaults to 10.
    lines: usize,
}

impl OutputTail {
    /// Keep the last `lines` lines of `stdout`, mirroring `tail -n <lines>`.
    /// Captured stdout is lines joined by `\n` (the reader strips the newlines),
    /// so this counts `\n` separators.
    pub(super) fn apply(&self, stdout: &str) -> String {
        if self.lines == 0 {
            return String::new();
        }
        let total_lines = stdout.bytes().filter(|&b| b == b'\n').count() + 1;
        if total_lines <= self.lines {
            return stdout.to_string();
        }
        // The last N lines begin just after the `drop`-th newline.
        let drop = total_lines - self.lines;
        let mut seen = 0usize;
        let mut start = 0usize;
        for (idx, byte) in stdout.bytes().enumerate() {
            if byte == b'\n' {
                seen += 1;
                if seen == drop {
                    start = idx + 1;
                    break;
                }
            }
        }
        stdout[start..].to_string()
    }
}

/// GNU/BSD `tail` keeps the last 10 lines when no count is given.
const DEFAULT_TAIL_LINES: usize = 10;

/// If `command` is a single pure pipeline ending in a `tail` stage whose sole
/// effect is "keep the last N lines of stdout", return the command with that
/// stage removed plus the [`OutputTail`] to re-apply to captured stdout. Returns
/// `None` — leave the command exactly as written — for anything not provably
/// equivalent to that.
///
/// The transformation is exact only because the command is a single pipeline:
/// every stage's stdout flows through to the final `tail`, and a fresh `bash -c`
/// runs without `pipefail`, so the pipeline exits with tail's success (0). Both
/// facts let the caller re-apply the line limit to the whole captured stdout and
/// mask the exit code to 0 with byte-for-byte fidelity. A top-level `;`, `||`,
/// background `&`, newline, or non-quiet `&&` prefix breaks that equivalence (the
/// tail would apply only to the final statement, or prefix stdout would be
/// included in our captured post-tail transform), and `tail -f`/`-c`/`-n +N`, a
/// file argument, or `|&` are not last-N-line stdin tails — all of these bail.
/// A quiet leading `cd … &&` chain is allowed because it only changes cwd before
/// the final pipeline. See [`head_is_pure_pipeline`] and [`parse_tail_stage`].
pub(super) fn strip_streamable_tail(command: &str) -> Option<(String, OutputTail)> {
    if let Some(stripped) = strip_single_pipeline_tail(command) {
        return Some(stripped);
    }

    // Common agent command shape: `cd crate && long command 2>&1 | tail -40`.
    // The setup side must be a quiet `cd` chain so applying the captured tail to
    // stdout after execution is still equivalent to the shell pipeline: the
    // prefix contributes no stdout of its own, and the final tail belongs only to
    // the right-hand pipeline.
    let (setup, pipeline) = split_last_top_level_and(command)?;
    if !setup_is_quiet_cd_chain(setup) {
        return None;
    }
    let (stripped_pipeline, tail) = strip_single_pipeline_tail(pipeline.trim_start())?;
    Some((
        format!("{} && {}", setup.trim_end(), stripped_pipeline),
        tail,
    ))
}

fn strip_single_pipeline_tail(command: &str) -> Option<(String, OutputTail)> {
    let (head, tail_segment) = split_last_top_level_pipe(command)?;
    let tail = parse_tail_stage(tail_segment)?;
    let head = head.trim_end();
    if head.is_empty() || !head_is_pure_pipeline(head) {
        return None;
    }
    Some((head.to_string(), tail))
}

/// Split `command` at its last top-level plain `|` (a real pipe: not `||`, not
/// `|&`, not `>|`, and not inside single/double quotes, backticks, or
/// `$(...)`/`(...)` nesting). Returns `(before, after)` with the pipe removed, or
/// `None` when there is no such pipe.
fn split_last_top_level_pipe(command: &str) -> Option<(&str, &str)> {
    let mut last_pipe: Option<usize> = None;
    scan_top_level(command, |bytes, i, c| {
        if c != b'|' {
            return ScanAction::Continue;
        }
        let next = bytes.get(i + 1).copied();
        let prev = i.checked_sub(1).map(|p| bytes[p]);
        // `||` (logical OR): consume both bytes so neither registers.
        if next == Some(b'|') {
            return ScanAction::SkipNext;
        }
        // `|&` (pipe stdout+stderr) combines streams — tail would see a
        // different byte stream than our stdout-only re-application. `>|` is a
        // force-clobber redirect, not a pipe.
        if next != Some(b'&') && prev != Some(b'>') {
            last_pipe = Some(i);
        }
        ScanAction::Continue
    });
    let idx = last_pipe?;
    Some((&command[..idx], &command[idx + 1..]))
}

fn split_last_top_level_and(command: &str) -> Option<(&str, &str)> {
    let mut last_and: Option<usize> = None;
    scan_top_level(command, |bytes, i, c| {
        if c == b'&' && bytes.get(i + 1).copied() == Some(b'&') {
            last_and = Some(i);
            return ScanAction::SkipNext;
        }
        ScanAction::Continue
    });
    let idx = last_and?;
    Some((&command[..idx], &command[idx + 2..]))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScanAction {
    Continue,
    SkipNext,
}

fn scan_top_level(command: &str, mut visit: impl FnMut(&[u8], usize, u8) -> ScanAction) {
    let bytes = command.as_bytes();
    let mut in_single = false;
    let mut in_double = false;
    let mut in_backtick = false;
    let mut depth: usize = 0;
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        if in_single {
            if c == b'\'' {
                in_single = false;
            }
            i += 1;
            continue;
        }
        if in_double {
            match c {
                b'\\' => {
                    i += 2;
                    continue;
                }
                b'"' => in_double = false,
                _ => {}
            }
            i += 1;
            continue;
        }
        if in_backtick {
            if c == b'`' {
                in_backtick = false;
            }
            i += 1;
            continue;
        }
        match c {
            b'\\' => {
                i += 2;
                continue;
            }
            b'\'' => in_single = true,
            b'"' => in_double = true,
            b'`' => in_backtick = true,
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            _ if depth == 0 && visit(bytes, i, c) == ScanAction::SkipNext => {
                i += 2;
                continue;
            }
            _ => {}
        }
        i += 1;
    }
}

/// A "pure pipeline" head: its only top-level control operators are single `|`
/// pipes, so every stage's stdout flows through to the final `tail`. A top-level
/// `;`, `&&`, `||`, background `&`, or newline breaks the equivalence that makes
/// stripping exact, so those return `false`. Redirection ampersands (`2>&1`,
/// `>&2`, `&>file`) are allowed — they are adjacent to `>`/`<`, never doubled.
fn head_is_pure_pipeline(head: &str) -> bool {
    let mut ok = true;
    scan_top_level(head, |bytes, i, c| {
        match c {
            b';' | b'\n' => ok = false,
            b'|' => {
                // `||` (logical OR) is not a pipe.
                if bytes.get(i + 1).copied() == Some(b'|') {
                    ok = false;
                    return ScanAction::SkipNext;
                }
            }
            b'&' => {
                // Allow redirection ampersands (`2>&1`, `>&2`, `&>file`); reject
                // background `&` and `&&`.
                let prev = i.checked_sub(1).map(|p| bytes[p]);
                let next = bytes.get(i + 1).copied();
                let is_redirection = prev == Some(b'>') || prev == Some(b'<') || next == Some(b'>');
                if !is_redirection {
                    ok = false;
                    if next == Some(b'&') {
                        return ScanAction::SkipNext;
                    }
                }
            }
            _ => {}
        }
        ScanAction::Continue
    });
    ok
}

fn setup_is_quiet_cd_chain(setup: &str) -> bool {
    let mut start = 0usize;
    let mut segments = Vec::new();
    scan_top_level(setup, |bytes, i, c| {
        if c == b'&' && bytes.get(i + 1).copied() == Some(b'&') {
            segments.push(&setup[start..i]);
            start = i + 2;
            return ScanAction::SkipNext;
        }
        ScanAction::Continue
    });
    segments.push(&setup[start..]);
    !segments.is_empty() && segments.into_iter().all(is_quiet_cd_command)
}

fn is_quiet_cd_command(segment: &str) -> bool {
    let mut toks = segment.split_whitespace();
    if toks.next() != Some("cd") {
        return false;
    }
    let Some(path) = toks.next() else {
        return false;
    };
    if toks.next().is_some() || path == "-" {
        return false;
    }
    !path.bytes().any(|b| {
        matches!(
            b,
            b'|' | b'&'
                | b';'
                | b'<'
                | b'>'
                | b'('
                | b')'
                | b'`'
                | b'$'
                | b'\''
                | b'"'
                | b'*'
                | b'?'
                | b'['
                | b'\\'
                | b'\n'
        )
    })
}

/// Parse a pipeline segment that must be exactly a `tail` invocation equivalent
/// to "keep the last N lines". Any shell metacharacter (a trailing operator,
/// redirection, subshell, substitution, quote, or glob riding along) or an
/// unrecognized flag yields `None`.
fn parse_tail_stage(segment: &str) -> Option<OutputTail> {
    let segment = segment.trim();
    if segment.bytes().any(|b| {
        matches!(
            b,
            b'|' | b'&'
                | b';'
                | b'<'
                | b'>'
                | b'('
                | b')'
                | b'`'
                | b'$'
                | b'\''
                | b'"'
                | b'*'
                | b'?'
                | b'['
                | b'\\'
        )
    }) {
        return None;
    }
    let mut toks = segment.split_whitespace();
    if toks.next()? != "tail" {
        return None;
    }
    let args: Vec<&str> = toks.collect();
    parse_tail_args(&args)
}

/// Parse `tail`'s arguments into a last-N-lines transform, or `None` for any form
/// that is not a plain stdin line tail (`-f`, `-c`, `-n +N`, a file argument, an
/// unrecognized flag).
fn parse_tail_args(args: &[&str]) -> Option<OutputTail> {
    if args.is_empty() {
        return Some(OutputTail {
            lines: DEFAULT_TAIL_LINES,
        });
    }
    let mut lines: Option<usize> = None;
    let mut i = 0;
    while i < args.len() {
        let a = args[i];
        if a == "-n" || a == "--lines" {
            lines = Some(parse_line_count(args.get(i + 1)?)?);
            i += 2;
            continue;
        }
        if let Some(v) = a.strip_prefix("--lines=") {
            lines = Some(parse_line_count(v)?);
            i += 1;
            continue;
        }
        if let Some(v) = a.strip_prefix("-n") {
            lines = Some(parse_line_count(v)?);
            i += 1;
            continue;
        }
        if let Some(v) = a.strip_prefix('-') {
            // Obsolescent `-N` count form. `-f`/`-c`/`-q`/... fail here because
            // they are not all-digits.
            lines = Some(parse_line_count(v)?);
            i += 1;
            continue;
        }
        // A bare positional is a file argument — tail would read the file, not the
        // pipe — so this is not a stdin tail.
        return None;
    }
    lines.map(|lines| OutputTail { lines })
}

/// Parse a `tail` line-count value: plain ASCII digits only. Rejects `+N` (from
/// line N), an empty value, and any non-numeric flag letter.
fn parse_line_count(value: &str) -> Option<usize> {
    if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    value.parse::<usize>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The batch as a model-facing caller receives it.
    fn context(outcomes: &[ItemOutcome]) -> String {
        let composed = compose_run_output(outcomes, RunOutputBudget::Context);
        assert_eq!(composed.over_ceiling, None);
        composed.text
    }

    fn outcome(header: &str, body: &str, succeeded: bool) -> ItemOutcome {
        ItemOutcome {
            header: header.to_string(),
            body: body.to_string(),
            succeeded,
            suspended: false,
            images: Vec::new(),
            tracked_modifications: None,
            data: None,
            whole: false,
        }
    }

    #[test]
    fn run_envelope_round_trips_text_and_images() {
        // The run result is serialized as a RunBatchEnvelope so the transport
        // edge can lift each image into its own content block. Round-trip an
        // image-bearing result to prove the carrier preserves both.
        let image = ImageBlock::inline("image/png", "b64");
        let json = run_envelope("=== look ===\nAX tree".to_string(), vec![image.clone()]);
        let parsed: RunBatchEnvelope = serde_json::from_str(&json).expect("valid envelope JSON");
        assert_eq!(parsed.text, "=== look ===\nAX tree");
        assert_eq!(parsed.images, vec![image]);
    }

    fn with_images(header: &str, body: &str, images: Vec<ImageBlock>) -> ItemOutcome {
        ItemOutcome {
            images,
            ..outcome(header, body, true)
        }
    }

    /// Where image `data` sits in a composed batch's text.
    fn image_at(composed: &ComposedRun, data: &str) -> Option<usize> {
        composed
            .images
            .iter()
            .find(|image| image.data == data)
            .unwrap_or_else(|| panic!("image {data} was dropped"))
            .at
    }

    #[test]
    fn images_arrive_in_item_order_each_after_its_own_item() {
        let outcomes = vec![
            with_images("look", "tree", vec![ImageBlock::inline("image/png", "a")]),
            // A text-only item contributes no images.
            ItemOutcome::failed("echo".to_string(), "out"),
            with_images("look2", "tree2", vec![ImageBlock::inline("image/png", "b")]),
        ];
        for budget in [
            RunOutputBudget::Context,
            RunOutputBudget::Whole {
                byte_ceiling: view::WHOLE_READ_BYTE_CEILING,
                batch_wire_ceiling: WHOLE_RUN_BATCH_WIRE_BYTES,
            },
        ] {
            let composed = compose_run_output(&outcomes, budget);
            let order: Vec<_> = composed.images.iter().map(|i| i.data.as_str()).collect();
            assert_eq!(order, ["a", "b"], "{budget:?}");
            // The first item's image comes before the next item's header,
            // not after the whole batch.
            let a = image_at(&composed, "a").expect("pinned to its item");
            assert_eq!(&composed.text[..a], "=== look ===\ntree", "{budget:?}");
            assert!(composed.text[a..].starts_with("\n=== echo ==="));
            // The last item's trailing image stays trailing.
            assert_eq!(image_at(&composed, "b"), None, "{budget:?}");
        }
    }

    /// Every place the composer gives an image has a newline beside it, so a
    /// model client that joins the delivered text blocks with `\n` reads back
    /// exactly the composed text. Checked over the shapes composition takes:
    /// labeling, pinning between items, an image before its text, elision of
    /// a middle, tail, and head anchor, omission, and refusal.
    #[test]
    fn every_composed_placement_joins_back_to_the_composed_text() {
        let png = |data: &str| ImageBlock::inline("image/png", data);
        let lines = |n: usize| {
            (0..n)
                .map(|i| format!("line-{i}"))
                .collect::<Vec<_>>()
                .join("\n")
        };
        let big = lines(9_000);
        let at_line = |line: &str| big.find(&format!("{line}\n")).unwrap();
        let batches: Vec<Vec<ItemOutcome>> = vec![
            vec![with_images("look", "caption", vec![png("a").placed_at(0)])],
            vec![with_images(
                "look",
                "before\nafter",
                vec![png("a").placed_at(7)],
            )],
            vec![
                with_images("look", "tree", vec![png("a")]),
                with_images("look2", "caption", vec![png("b").placed_at(0)]),
                with_images("look3", "x\ny", vec![png("c").placed_at(2), png("d")]),
            ],
            vec![
                with_images(
                    "flood",
                    &big,
                    vec![
                        png("head").placed_at(at_line("line-3")),
                        png("middle").placed_at(at_line("line-4000")),
                        png("tail").placed_at(at_line("line-8990")),
                        png("trailing"),
                    ],
                ),
                with_images("after", "done", vec![png("e")]),
            ],
            (0..400)
                .map(|i| {
                    with_images(
                        &format!("cmd-{i}"),
                        &"x".repeat(2_000),
                        vec![png(&format!("o{i}"))],
                    )
                })
                .collect(),
            vec![
                with_images("huge", &"h\n".repeat(1_024), vec![png("r").placed_at(10)]),
                outcome("small", "S", true),
            ],
        ];
        for outcomes in &batches {
            for budget in [
                RunOutputBudget::Context,
                RunOutputBudget::Whole {
                    byte_ceiling: 1_024,
                    batch_wire_ceiling: 1 << 20,
                },
            ] {
                let composed = compose_run_output(outcomes, budget);
                let images = outcomes.iter().map(|o| o.images.len()).sum::<usize>();
                assert_eq!(
                    composed.images.len(),
                    images,
                    "{budget:?}: no image is dropped"
                );
                let output = composed.output();
                let joined = output
                    .blocks()
                    .into_iter()
                    .filter_map(|block| match block {
                        cairn_common::read::OrderedBlock::Text(text) => Some(text),
                        cairn_common::read::OrderedBlock::Image(_) => None,
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                assert_eq!(joined, composed.text, "{budget:?}");
            }
        }
    }

    #[test]
    fn a_single_text_then_image_item_composes_as_it_always_did() {
        let image = ImageBlock::inline("image/png", "a");
        let composed = compose_run_output(
            &[with_images("look", "AX tree", vec![image.clone()])],
            RunOutputBudget::Context,
        );
        assert_eq!(composed.text, "AX tree");
        assert_eq!(composed.images, vec![image]);
    }

    #[test]
    fn an_anchored_image_keeps_its_place_under_the_item_header() {
        let composed = compose_run_output(
            &[
                with_images(
                    "cairn://mcp/stub/look",
                    "caption",
                    vec![ImageBlock::inline("image/png", "a").placed_at(0)],
                ),
                outcome("echo after", "after", true),
            ],
            RunOutputBudget::Context,
        );
        let a = image_at(&composed, "a").unwrap();
        assert_eq!(&composed.text[..a], "=== cairn://mcp/stub/look ===\n");
        assert!(composed.text[a..].starts_with("caption\n=== echo after ==="));
    }

    #[test]
    fn an_image_in_the_elided_middle_lands_at_the_start_of_the_tail() {
        let body = format!(
            "HEAD\n{}\nPICTURED\n{}\nTAIL",
            "m\n".repeat(30_000),
            "n\n".repeat(30_000)
        );
        let middle = body.find("PICTURED").unwrap();
        let composed = compose_run_output(
            &[with_images(
                "look",
                &body,
                vec![ImageBlock::inline("image/png", "a").placed_at(middle)],
            )],
            RunOutputBudget::Context,
        );
        assert!(composed.text.len() <= MAX_RUN_RESULT_BYTES);
        let a = image_at(&composed, "a").expect("still anchored");
        assert!(composed.text[..a].ends_with("tail kept below ---\n"));
    }

    #[test]
    fn an_omitted_items_images_still_arrive_where_it_would_have_been() {
        let big = "x".repeat(2_000);
        let mut outcomes: Vec<ItemOutcome> = (0..400)
            .map(|i| outcome(&format!("cmd-{i}"), &big, true))
            .collect();
        outcomes[200].images = vec![ImageBlock::inline("image/png", "a")];
        let composed = compose_run_output(&outcomes, RunOutputBudget::Context);
        assert!(composed.text.contains("items omitted"));
        assert!(composed.text.len() <= MAX_RUN_RESULT_BYTES);
        assert_eq!(composed.images.len(), 1);
        assert!(image_at(&composed, "a").is_some());
    }

    #[test]
    fn a_refused_items_images_arrive_after_its_refusal_line() {
        let outcomes = vec![
            with_images(
                "huge",
                &"h".repeat(2_048),
                vec![ImageBlock::inline("image/png", "a").placed_at(10)],
            ),
            outcome("small", "S", true),
        ];
        let composed = compose_run_output(
            &outcomes,
            RunOutputBudget::Whole {
                byte_ceiling: 1_024,
                batch_wire_ceiling: 1 << 20,
            },
        );
        let a = image_at(&composed, "a").unwrap();
        assert!(composed.text[..a].ends_with("read that instead."));
        assert!(composed.text[a..].starts_with("\n=== small ==="));
    }

    #[test]
    fn compose_single_returns_body_without_header() {
        let outcomes = vec![outcome("echo hi", "hi", true)];
        assert_eq!(context(&outcomes), "hi");
    }

    #[test]
    fn compose_single_empty_body_is_no_output() {
        let outcomes = vec![outcome("true", "", true)];
        assert_eq!(context(&outcomes), "(no output)");
    }

    #[test]
    fn compose_multi_labels_items_in_input_order() {
        let outcomes = vec![
            outcome("cmd a", "alpha", true),
            outcome("cmd b", "beta", true),
        ];
        let text = context(&outcomes);
        let ia = text.find("=== cmd a ===").unwrap();
        let ialpha = text.find("alpha").unwrap();
        let ib = text.find("=== cmd b ===").unwrap();
        let ibeta = text.find("beta").unwrap();
        assert!(ia < ialpha, "header a precedes its body");
        assert!(ialpha < ib, "item a precedes item b");
        assert!(ib < ibeta, "header b precedes its body");
    }

    #[test]
    fn compose_multi_inlines_empty_body_as_no_output() {
        let outcomes = vec![outcome("a", "", false), outcome("b", "out", true)];
        let text = context(&outcomes);
        assert!(text.contains("=== a ===\n(no output)"));
        assert!(text.contains("=== b ===\nout"));
    }

    // Regression for the original repro: a small first segment followed by a huge
    // second segment. Head-biased capping dropped the second entirely; fair
    // budgets must surface both, with the huge one tail-biased.
    #[test]
    fn compose_multi_fair_budget_surfaces_both_segments() {
        let huge = format!("HEADLINE-OF-DIFF\n{}\nTAIL-OF-DIFF", "d".repeat(100_000));
        let outcomes = vec![
            outcome("git diff --stat", "1 file changed", true),
            outcome("git diff", &huge, true),
        ];
        let text = context(&outcomes);
        // First (small) item surfaces whole.
        assert!(text.contains("=== git diff --stat ===\n1 file changed"));
        // Second (huge) item surfaces, tail-biased with an elision marker, and its
        // tail (the signal) is retained.
        assert!(text.contains("=== git diff ==="));
        assert!(text.contains("--- elided"));
        assert!(text.contains("TAIL-OF-DIFF"));
        // Nothing is omitted, and the result stays within the cap.
        assert!(!text.contains("items omitted"));
        assert!(text.len() <= MAX_RUN_RESULT_BYTES);
    }

    // A 3-item batch where item 1 floods the output still shows items 2 and 3.
    #[test]
    fn compose_multi_starved_item_does_not_omit_later_items() {
        let flood = "f".repeat(100_000);
        let outcomes = vec![
            outcome("flood", &flood, true),
            outcome("item-two", "SECOND-OUTPUT", true),
            outcome("item-three", "THIRD-OUTPUT", true),
        ];
        let text = context(&outcomes);
        assert!(text.contains("=== item-two ===\nSECOND-OUTPUT"));
        assert!(text.contains("=== item-three ===\nTHIRD-OUTPUT"));
        assert!(text.contains("--- elided"));
        assert!(!text.contains("items omitted"));
        assert!(text.len() <= MAX_RUN_RESULT_BYTES);
    }

    // Single-item path is tail-biased: a body that exceeds the cap keeps a head
    // and the byte-exact tail, with an elision marker between.
    #[test]
    fn compose_single_tail_biased_keeps_head_and_tail() {
        let body = format!(
            "HEAD-SENTINEL\n{}\nTAIL-SENTINEL",
            "m".repeat(MAX_RUN_RESULT_BYTES * 2)
        );
        let text = context(&[outcome("big", &body, true)]);
        assert!(text.starts_with("HEAD-SENTINEL"));
        assert!(text.ends_with("TAIL-SENTINEL"));
        assert!(text.contains("--- elided"));
        assert!(text.len() <= MAX_RUN_RESULT_BYTES);
    }

    fn whole(header: &str, body: &str) -> ItemOutcome {
        ItemOutcome {
            whole: true,
            ..outcome(header, body, true)
        }
    }

    // A model door's reply is data: past the cap it arrives byte for byte, with
    // no elision marker.
    #[test]
    fn compose_single_whole_item_is_never_capped() {
        let body = format!("[{}]", vec!["0.0123456789"; 20_000].join(","));
        assert!(body.len() > MAX_RUN_RESULT_BYTES * 4);
        assert_eq!(context(&[whole("embed", &body)]), body);
    }

    // Beside capped items, a whole item renders in full and takes nothing from
    // their shared budget, so they cap exactly as they would without it.
    #[test]
    fn compose_multi_whole_item_renders_in_full_outside_the_shared_budget() {
        let reply = "7".repeat(MAX_RUN_RESULT_BYTES * 2);
        let flood = format!("HEAD\n{}\nTAIL", "f".repeat(100_000));
        let alone = context(&[outcome("flood", &flood, true), outcome("b", "B", true)]);
        let text = context(&[
            outcome("flood", &flood, true),
            whole("cairn://models/embed", &reply),
            outcome("b", "B", true),
        ]);
        assert!(text.contains(&format!("=== cairn://models/embed ===\n{reply}\n")));
        assert!(!text.contains("items omitted"));
        let without_reply = text.replace(&format!("=== cairn://models/embed ===\n{reply}\n"), "");
        assert_eq!(without_reply, alone);
    }

    // Pathological item counts fall back to whole-item omission, with advice to
    // re-run scoped or use a terminal — never an offset-continuation footer.
    #[test]
    fn compose_multi_omits_only_as_last_resort() {
        let big = "x".repeat(2_000);
        let outcomes: Vec<ItemOutcome> = (0..400)
            .map(|i| outcome(&format!("cmd-{i}"), &big, true))
            .collect();
        let text = context(&outcomes);
        assert!(text.contains("items omitted"));
        assert!(text.contains("re-run each scoped"));
        assert!(!text.contains("Call again with offset"));
        assert!(text.len() <= MAX_RUN_RESULT_BYTES);
    }

    // The composed result never exceeds the batch cap, even when every item floods.
    #[test]
    fn compose_multi_never_exceeds_cap() {
        let flood = "z".repeat(60_000);
        let outcomes: Vec<ItemOutcome> = (0..8)
            .map(|i| outcome(&format!("c{i}"), &flood, true))
            .collect();
        let text = context(&outcomes);
        assert!(text.len() <= MAX_RUN_RESULT_BYTES);
    }

    // Water-filling: small items keep their full size; the large item absorbs the
    // donated surplus.
    #[test]
    fn water_fill_donates_surplus_to_large_item() {
        let natural = vec![100, 100, 100_000];
        let alloc = water_fill_budgets(&natural, 45_000);
        assert_eq!(alloc[0], 100, "small item keeps full size");
        assert_eq!(alloc[1], 100, "small item keeps full size");
        assert!(
            alloc[2] >= 44_000,
            "large item absorbs the surplus: {}",
            alloc[2]
        );
        assert!(alloc.iter().sum::<usize>() <= 45_000);
    }

    fn for_code(outcomes: &[ItemOutcome]) -> ComposedRun {
        compose_run_output(
            outcomes,
            RunOutputBudget::Whole {
                byte_ceiling: view::WHOLE_READ_BYTE_CEILING,
                batch_wire_ceiling: WHOLE_RUN_BATCH_WIRE_BYTES,
            },
        )
    }

    // A script parses what it receives, so a body past the context cap
    // arrives byte for byte rather than head-and-tail with an elision marker.
    #[test]
    fn whole_budget_carries_a_body_past_the_context_cap_unelided() {
        let body = format!("{{\"rows\":[{}]}}", vec!["\"r\""; 15_000].join(","));
        assert!(body.len() > 60_000);
        let composed = for_code(&[outcome("cairn://mcp/cairn-bugs/list_reports", &body, true)]);
        assert_eq!(composed.text, body);
        assert_eq!(composed.over_ceiling, None);
        assert!(context(&[outcome("x", &body, true)]).contains("--- elided"));
    }

    #[test]
    fn whole_budget_never_water_fills_or_omits_items() {
        let flood = "f".repeat(20_000);
        let outcomes: Vec<ItemOutcome> = (0..8)
            .map(|i| outcome(&format!("c{i}"), &flood, true))
            .chain([outcome("empty", "", true)])
            .collect();
        let composed = for_code(&outcomes);
        assert!(composed.text.len() > MAX_RUN_RESULT_BYTES);
        for i in 0..8 {
            assert!(composed.text.contains(&format!("=== c{i} ===\n{flood}")));
        }
        assert!(composed.text.ends_with("=== empty ===\n(no output)"));
        assert!(!composed.text.contains("--- elided"));
        assert!(!composed.text.contains("items omitted"));
        assert_eq!(composed.over_ceiling, None);
    }

    // Past the ceiling an item is refused by type, not cut, and the batch's
    // other items still arrive whole beside the refusal.
    #[test]
    fn whole_budget_refuses_an_item_past_the_ceiling_by_type() {
        let outcomes = vec![
            outcome("small", "S", true),
            outcome("huge", &"h".repeat(2_048), true),
            whole("cairn://models/embed", &"7".repeat(4_096)),
        ];
        let composed = compose_run_output(
            &outcomes,
            RunOutputBudget::Whole {
                byte_ceiling: 1_024,
                batch_wire_ceiling: 1 << 20,
            },
        );
        assert_eq!(
            composed.over_ceiling,
            Some(OverCeiling {
                ceiling_bytes: 1_024,
                batch_ceiling_bytes: 1 << 20,
                items: vec![OverCeilingItem {
                    index: 1,
                    header: "huge".to_string(),
                    bytes: 2_048,
                }],
            })
        );
        assert!(composed.text.contains("=== small ===\nS\n"));
        assert!(composed.text.contains(
            "=== huge ===\n`huge`: output of 2048 bytes exceeds the 1024 bytes ceiling for a result returned to code"
        ));
        assert!(!composed.text.contains(&"h".repeat(2_048)));
        // A model door's reply is carried whole, outside the ceiling.
        assert!(composed.text.ends_with(&"7".repeat(4_096)));
    }

    // Items that each fit can still add up past what one response frame
    // carries; the batch budget refuses the ones that would, counting each
    // body as it costs on the wire, and later items that fit still arrive.
    #[test]
    fn whole_budget_refuses_items_that_would_carry_the_batch_past_its_wire_budget() {
        let outcomes = vec![
            outcome("a", &"a".repeat(900), true),
            outcome("b", &"b".repeat(900), true),
            outcome("c", &"c".repeat(900), true),
            // 100 bytes raw, 400 once escaped twice into the frame.
            outcome("quotes", &"\"".repeat(100), true),
            whole("cairn://models/embed", &"7".repeat(300)),
            outcome("tiny", "t", true),
        ];
        let composed = compose_run_output(
            &outcomes,
            RunOutputBudget::Whole {
                byte_ceiling: 1_024,
                batch_wire_ceiling: 2_048,
            },
        );
        let refused = |index: usize, header: &str, bytes: usize| OverCeilingItem {
            index,
            header: header.to_string(),
            bytes,
        };
        assert_eq!(
            composed.over_ceiling,
            Some(OverCeiling {
                ceiling_bytes: 1_024,
                batch_ceiling_bytes: 2_048,
                items: vec![
                    refused(2, "c", 900),
                    refused(3, "quotes", 100),
                    refused(4, "cairn://models/embed", 300),
                ],
            })
        );
        assert!(composed
            .text
            .contains(&format!("=== a ===\n{}\n", "a".repeat(900))));
        assert!(composed
            .text
            .contains(&format!("=== b ===\n{}\n", "b".repeat(900))));
        assert!(composed.text.contains(
            "=== c ===\n`c`: output of 900 bytes would carry this batch past the 2048 bytes a result returned to code may hold"
        ));
        assert!(!composed.text.contains(&"\"".repeat(100)));
        assert!(composed.text.ends_with("=== tiny ===\nt"));
    }

    // The production batch budget leaves the response, escaped twice, inside
    // one link frame even when every byte is the costliest to escape.
    #[test]
    fn the_batch_wire_budget_fits_one_relay_frame() {
        let quotes = "\"".repeat(3 << 20);
        let outcomes = vec![outcome("one", &quotes, true), outcome("two", &quotes, true)];
        let composed = for_code(&outcomes);
        assert_eq!(
            composed.over_ceiling.as_ref().map(|over| over
                .items
                .iter()
                .map(|item| item.index)
                .collect::<Vec<_>>()),
            Some(vec![1])
        );
        let envelope =
            run_envelope_with_data(composed.text, Vec::new(), None, composed.over_ceiling);
        let frame = serde_json::to_string(&envelope).unwrap();
        assert!(
            frame.len() < RELAY_RESPONSE_BYTE_BUDGET,
            "{} bytes on the wire",
            frame.len()
        );
    }

    #[test]
    fn ceiling_label_names_whole_mebibytes() {
        assert_eq!(ceiling_label(view::WHOLE_READ_BYTE_CEILING), "8 MiB");
        assert_eq!(ceiling_label(1_024), "1024 bytes");
    }
    #[test]
    fn strip_streamable_tail_recognizes_last_n_line_forms() {
        let cases = [
            ("cargo build | tail -50", "cargo build", 50),
            ("cargo build | tail -n 50", "cargo build", 50),
            ("cargo build | tail -n50", "cargo build", 50),
            ("cargo build | tail --lines=50", "cargo build", 50),
            ("cargo build | tail --lines 50", "cargo build", 50),
            ("cargo build | tail", "cargo build", DEFAULT_TAIL_LINES),
            // A merged stderr redirect stays on the head side and streams live.
            ("cargo build 2>&1 | tail -20", "cargo build 2>&1", 20),
            // Common agent setup prefixes stay attached while the tail comes off
            // the right-hand pipeline.
            (
                "cd src-tauri && cargo test -p cairn-core --lib 'analytics::tests::' 2>&1 | tail -40",
                "cd src-tauri && cargo test -p cairn-core --lib 'analytics::tests::' 2>&1",
                40,
            ),
            // Multi-stage pipelines are fine: every stage still feeds tail.
            ("cat log | grep err | tail -5", "cat log | grep err", 5),
        ];
        for (command, head, lines) in cases {
            let (stripped, tail) = strip_streamable_tail(command)
                .unwrap_or_else(|| panic!("should strip trailing tail: {command}"));
            assert_eq!(stripped, head, "head mismatch for {command}");
            assert_eq!(tail.lines, lines, "line count mismatch for {command}");
        }
    }

    #[test]
    fn strip_streamable_tail_leaves_unrecognized_or_unsafe_forms_untouched() {
        let untouched = [
            "cargo build",                            // no pipe at all
            "cargo build | tail -f",                  // follow never reaches EOF
            "cargo build | tail -c 100",              // byte tail, not line tail
            "cargo build | tail -n +50",              // from line N, not last N
            "cargo build | tail -50 log.txt",         // reads a file, not the pipe
            "cargo build | tail -5 | grep x",         // tail is not the final stage
            "cargo build | tail -q",                  // unrecognized flag
            "cargo build | head -50",                 // head already streams
            "cargo build |& tail -5",                 // pipe-both combines stderr
            "cargo build || tail -5",                 // logical OR, not a pipe
            "(cargo build | tail -5)",                // pipe nested in a subshell
            "echo 'a | tail -5'",                     // pipe is inside quotes
            "echo setup && make | tail -20",          // non-quiet && prefix adds stdout
            "cd - && make | tail -20",                // `cd -` prints the new cwd
            "echo start ; make | tail -5",            // ; — tail sees only the last stmt
            "set -o pipefail; cargo build | tail -5", // pipefail changes exit semantics
            "make | tail -5 & echo done",             // trailing background operator
        ];
        for command in untouched {
            assert!(
                strip_streamable_tail(command).is_none(),
                "should leave untouched: {command}"
            );
        }
    }

    #[test]
    fn output_tail_keeps_last_n_lines() {
        let tail = OutputTail { lines: 2 };
        assert_eq!(tail.apply("a\nb\nc\nd"), "c\nd");
        // Fewer lines than the limit — unchanged.
        assert_eq!(tail.apply("only\none"), "only\none");
        assert_eq!(tail.apply("single"), "single");
        // Exactly the limit — unchanged.
        assert_eq!(tail.apply("x\ny"), "x\ny");
        // Empty stdout stays empty.
        assert_eq!(tail.apply(""), "");
    }

    #[test]
    fn output_tail_zero_lines_is_empty() {
        assert_eq!(OutputTail { lines: 0 }.apply("a\nb\nc"), "");
    }
}
