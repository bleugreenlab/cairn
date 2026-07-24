//! Output composition: per-item body capping, batch water-filling, the run
//! envelope, and the streamable-`tail` pipeline transform.

use super::types::ItemOutcome;
use cairn_common::read::{ImageBlock, RunBatchEnvelope};

/// Max chars in a composed batch result. Deliberately equal to the CLI's
/// `MAX_RESULT_CHARS`: `compose_run_output` keeps its result at or under this,
/// so the CLI's run-result guard passes a core-capped batch through untouched
/// (no double truncation, no second elision marker).
const MAX_RUN_RESULT_CHARS: usize = 45_000;

/// Percent of an item's char budget reserved for the head (leading context: the
/// command echo and opening lines). The remainder holds the tail. Command output
/// is read end-first — error summaries, test failures, the last `&&` segment, and
/// exit codes all live at the end — so the per-item cap is tail-biased.
const ITEM_HEAD_PERCENT: usize = 15;

/// Bytes reserved inside a per-item budget for the elision marker, so a capped
/// body (head + marker + tail) never exceeds its budget. This is what keeps the
/// composed batch within `MAX_RUN_RESULT_CHARS`, so the CLI passes it untouched.
const ELISION_MARKER_RESERVE: usize = 96;

/// Smallest per-item body budget worth showing. Below this an item is omitted
/// whole (the last-resort path) rather than reduced to a useless sliver. Sized
/// well above any trailing detached-terminal note, so the tail cap of a
/// non-omitted item never elides that note.
const MIN_ITEM_BODY_BUDGET: usize = 512;

/// Headroom held back from the batch cap for the trailing omission note and
/// inter-segment separators, so the composed result stays under the cap.
const OMISSION_NOTE_RESERVE: usize = 256;

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

/// Tail-biased head+tail cap for one command body. Keeps a small head (~15%, the
/// command's opening context) and a large tail (~85%, the signal), with an
/// elision marker between stating how many lines and chars were dropped. The tail
/// extends to the byte-exact end of the body, so a trailing note — e.g. the
/// detached-terminal pointer a promoted item appends — is always retained as long
/// as the item clears `MIN_ITEM_BODY_BUDGET`. Its start is snapped forward to the
/// next line boundary so the displayed tail begins on a whole line rather than a
/// mid-line fragment (which reads as corrupted output); the skipped partial line
/// folds into the elision count. The result never exceeds `budget`.
fn cap_item_body_tail_biased(body: &str, budget: usize) -> String {
    if body.len() <= budget {
        return body.to_string();
    }
    let content_budget = budget.saturating_sub(ELISION_MARKER_RESERVE);
    let head_budget = content_budget * ITEM_HEAD_PERCENT / 100;
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

    // Advance the tail start to the next line boundary so the displayed tail
    // begins on a whole line, not a mid-line fragment. Only advance when a
    // newline leaves real content after it — otherwise the byte-exact end (and
    // any trailing note riding it) would be elided. The skipped partial line
    // folds into the elided span below, keeping the marker count accurate.
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

/// Render one item's body block (no header): tail-capped to `body_budget`, with
/// an empty body shown as `(no output)`.
fn render_item_body(outcome: &ItemOutcome, body_budget: usize) -> String {
    if outcome.body.is_empty() {
        "(no output)".to_string()
    } else {
        cap_item_body_tail_biased(&outcome.body, body_budget)
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
/// The composed `text` and any `images` ride together so the CLI lifts each image
/// into its own content block after the text, mirroring the read-batch envelope.
/// Serializing a struct of string/array fields does not fail in practice; the
/// fallback returns the bare text so a run never errors on rendering.
pub(super) fn run_envelope(text: String, images: Vec<ImageBlock>) -> String {
    let envelope = RunBatchEnvelope { text, images };
    serde_json::to_string(&envelope).unwrap_or(envelope.text)
}

/// Collect every item's image blocks across the batch, in item order.
pub(super) fn collect_run_images(outcomes: Vec<ItemOutcome>) -> Vec<ImageBlock> {
    outcomes.into_iter().flat_map(|o| o.images).collect()
}

/// Compose per-item outcomes into a single result string, bounded by
/// `MAX_RUN_RESULT_CHARS`. A single item returns its (tail-capped) body directly
/// with no header. Multiple items are labeled `=== <header> ===` in input order;
/// the batch cap is divided fairly across them (water-filling) and each item's
/// body is tail-capped within its share, so every item surfaces meaningful
/// output. Whole-item omission, with a named note, remains only as a last resort
/// for pathological item counts where the per-item share falls below a usable
/// floor. Because each item's body is capped (head + marker + tail) within its
/// allocation, the composed result stays under `MAX_RUN_RESULT_CHARS` — the CLI's
/// equal cap then passes it through untouched (no double truncation).
pub(super) fn compose_run_output(outcomes: &[ItemOutcome]) -> String {
    if outcomes.len() == 1 {
        return render_item_body(&outcomes[0], MAX_RUN_RESULT_CHARS);
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

    let total_budget = MAX_RUN_RESULT_CHARS.saturating_sub(OMISSION_NOTE_RESERVE);
    let alloc = water_fill_budgets(&natural, total_budget);

    let mut segments: Vec<String> = Vec::with_capacity(outcomes.len());
    let mut omitted: Vec<String> = Vec::new();
    for (i, o) in outcomes.iter().enumerate() {
        let body_budget = alloc[i].saturating_sub(overhead[i]);
        let natural_body = natural[i] - overhead[i];
        // Last resort: an item that does not fit and whose share is below the
        // usable floor is omitted whole rather than shown as a sliver.
        if natural_body > body_budget && body_budget < MIN_ITEM_BODY_BUDGET {
            omitted.push(o.header.clone());
            continue;
        }
        segments.push(format!(
            "{}{}",
            headers[i],
            render_item_body(o, body_budget)
        ));
    }

    let mut text = segments.join("\n");
    if !omitted.is_empty() {
        if !text.is_empty() {
            text.push_str("\n\n");
        }
        text.push_str(&format!(
            "--- {} of {} items omitted (batch {}K char cap); re-run each scoped (grep/tail the command) or open a terminal resource: {} ---",
            omitted.len(),
            outcomes.len(),
            MAX_RUN_RESULT_CHARS / 1000,
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
            _ if depth == 0 => {
                if visit(bytes, i, c) == ScanAction::SkipNext {
                    i += 2;
                    continue;
                }
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

    fn outcome(header: &str, body: &str, succeeded: bool) -> ItemOutcome {
        ItemOutcome {
            header: header.to_string(),
            body: body.to_string(),
            succeeded,
            suspended: false,
            images: Vec::new(),
            promoted_terminal: None,
            tracked_modifications: None,
        }
    }

    #[test]
    fn run_envelope_round_trips_text_and_images() {
        // The run result is serialized as a RunBatchEnvelope so the transport
        // edge can lift each image into its own content block. Round-trip an
        // image-bearing result to prove the carrier preserves both.
        let image = ImageBlock {
            mime_type: "image/png".to_string(),
            data: "b64".to_string(),
        };
        let json = run_envelope("=== look ===\nAX tree".to_string(), vec![image.clone()]);
        let parsed: RunBatchEnvelope = serde_json::from_str(&json).expect("valid envelope JSON");
        assert_eq!(parsed.text, "=== look ===\nAX tree");
        assert_eq!(parsed.images, vec![image]);
    }

    #[test]
    fn collect_run_images_gathers_in_item_order() {
        let img_a = ImageBlock {
            mime_type: "image/png".to_string(),
            data: "a".to_string(),
        };
        let img_b = ImageBlock {
            mime_type: "image/png".to_string(),
            data: "b".to_string(),
        };
        let outcomes = vec![
            ItemOutcome {
                header: "look".to_string(),
                body: "tree".to_string(),
                succeeded: true,
                suspended: false,
                images: vec![img_a.clone()],
                promoted_terminal: None,
                tracked_modifications: None,
            },
            // A text-only item contributes no images and is skipped.
            ItemOutcome::failed("echo".to_string(), "out"),
            ItemOutcome {
                header: "look2".to_string(),
                body: "tree2".to_string(),
                succeeded: true,
                suspended: false,
                images: vec![img_b.clone()],
                promoted_terminal: None,
                tracked_modifications: None,
            },
        ];
        assert_eq!(collect_run_images(outcomes), vec![img_a, img_b]);
    }

    #[test]
    fn compose_single_returns_body_without_header() {
        let outcomes = vec![outcome("echo hi", "hi", true)];
        assert_eq!(compose_run_output(&outcomes), "hi");
    }

    #[test]
    fn compose_single_empty_body_is_no_output() {
        let outcomes = vec![outcome("true", "", true)];
        assert_eq!(compose_run_output(&outcomes), "(no output)");
    }

    #[test]
    fn compose_multi_labels_items_in_input_order() {
        let outcomes = vec![
            outcome("cmd a", "alpha", true),
            outcome("cmd b", "beta", true),
        ];
        let text = compose_run_output(&outcomes);
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
        let text = compose_run_output(&outcomes);
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
        let text = compose_run_output(&outcomes);
        // First (small) item surfaces whole.
        assert!(text.contains("=== git diff --stat ===\n1 file changed"));
        // Second (huge) item surfaces, tail-biased with an elision marker, and its
        // tail (the signal) is retained.
        assert!(text.contains("=== git diff ==="));
        assert!(text.contains("--- elided"));
        assert!(text.contains("TAIL-OF-DIFF"));
        // Nothing is omitted, and the result stays within the cap.
        assert!(!text.contains("items omitted"));
        assert!(text.len() <= MAX_RUN_RESULT_CHARS);
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
        let text = compose_run_output(&outcomes);
        assert!(text.contains("=== item-two ===\nSECOND-OUTPUT"));
        assert!(text.contains("=== item-three ===\nTHIRD-OUTPUT"));
        assert!(text.contains("--- elided"));
        assert!(!text.contains("items omitted"));
        assert!(text.len() <= MAX_RUN_RESULT_CHARS);
    }

    // Single-item path is tail-biased: a body that exceeds the cap keeps a head
    // and the byte-exact tail, with an elision marker between.
    #[test]
    fn compose_single_tail_biased_keeps_head_and_tail() {
        let body = format!(
            "HEAD-SENTINEL\n{}\nTAIL-SENTINEL",
            "m".repeat(MAX_RUN_RESULT_CHARS * 2)
        );
        let text = compose_run_output(&[outcome("big", &body, true)]);
        assert!(text.starts_with("HEAD-SENTINEL"));
        assert!(text.ends_with("TAIL-SENTINEL"));
        assert!(text.contains("--- elided"));
        assert!(text.len() <= MAX_RUN_RESULT_CHARS);
    }

    // A trailing detached-terminal note rides in the tail and is never elided.
    #[test]
    fn cap_item_body_preserves_trailing_promoted_note() {
        let note = "Command still running; detached to \
            cairn://p/CAIRN/1632/1/builder/terminal/run-1 — readable and killable there.";
        let body = format!("{}\n\n{}", "p".repeat(50_000), note);
        let capped = cap_item_body_tail_biased(&body, 4_000);
        assert!(capped.len() <= 4_000);
        assert!(capped.ends_with(note), "promoted note must survive the cap");
        assert!(capped.contains("--- elided"));
    }

    // The elision marker reports the actual dropped span.
    #[test]
    fn cap_item_body_elision_counts_are_accurate() {
        let body = (0..1_000)
            .map(|i| format!("line-{i}"))
            .collect::<Vec<_>>()
            .join("\n");
        let budget = 2_000;
        let capped = cap_item_body_tail_biased(&body, budget);
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
    fn cap_item_body_first_tail_line_is_complete() {
        // Variable-width lines: a byte-exact tail start would land mid-line and
        // surface a partial number; the snap-to-line fix must avoid that.
        let lines: Vec<String> = (0..4_000)
            .map(|i| format!("{}-entry", 90_000 + i * 7))
            .collect();
        let body = lines.join("\n");
        let capped = cap_item_body_tail_biased(&body, 2_000);
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
    fn cap_item_body_multibyte_boundary_safe() {
        let body = "é".repeat(40_000); // 2 bytes each → 80_000 bytes
        let capped = cap_item_body_tail_biased(&body, 5_000);
        assert!(capped.len() <= 5_000);
        assert!(capped.contains("--- elided"));
        // Round-trips as valid UTF-8 by construction (String), nothing to assert
        // beyond the absence of a panic above.
    }

    // Pathological item counts fall back to whole-item omission, with advice to
    // re-run scoped or use a terminal — never an offset-continuation footer.
    #[test]
    fn compose_multi_omits_only_as_last_resort() {
        let big = "x".repeat(2_000);
        let outcomes: Vec<ItemOutcome> = (0..400)
            .map(|i| outcome(&format!("cmd-{i}"), &big, true))
            .collect();
        let text = compose_run_output(&outcomes);
        assert!(text.contains("items omitted"));
        assert!(text.contains("re-run each scoped"));
        assert!(!text.contains("Call again with offset"));
        assert!(text.len() <= MAX_RUN_RESULT_CHARS);
    }

    // The composed result never exceeds the batch cap, even when every item floods.
    #[test]
    fn compose_multi_never_exceeds_cap() {
        let flood = "z".repeat(60_000);
        let outcomes: Vec<ItemOutcome> = (0..8)
            .map(|i| outcome(&format!("c{i}"), &flood, true))
            .collect();
        let text = compose_run_output(&outcomes);
        assert!(text.len() <= MAX_RUN_RESULT_CHARS);
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
