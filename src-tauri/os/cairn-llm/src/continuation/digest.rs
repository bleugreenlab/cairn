//! The two projections of a [`Transcript`]: the bounded digest a rebuilt
//! session is seeded with, and the verbatim text native resume re-sends.
//!
//! The digest is the text form of a flat-row tool model: turn-structured, one
//! row per target, a right-pinned count figure, no inline JSON bodies. The host
//! owns any header above it and the vocabulary of its own tools, which it
//! registers on a [`ToolRenderers`]; everything else renders through
//! [`generic_rows`].

use std::collections::HashMap;
use std::fmt;

use cairn_platform::clock::HostClock;
use serde_json::Value;

use super::transcript::{Entry, ToolCall, Transcript, Turn};

// Tool-input summary preview cap (the fallback row's label).
const TOOL_SUMMARY_PREVIEW: usize = 80;

// Digest truncation thresholds. User content gets a generous window (its first
// turn often replays the whole plan); assistant prose is the narrative and is
// cheap once tool noise is gone, so it truncates only at a high ceiling.
const USER_CONTENT_THRESHOLD: usize = 3000;
const USER_CONTENT_PREVIEW: usize = 1200;
const DIGEST_ASSISTANT_THRESHOLD: usize = 6000;
const DIGEST_ASSISTANT_PREVIEW: usize = 2000;
/// The width of a one-line error or injection preview.
pub const ERROR_LINE_PREVIEW: usize = 200;
/// The width of a tool row's label.
pub const ROW_LABEL_PREVIEW: usize = 120;

/// Rendering options for a digest. `latest`, `turn_offset`, and `turn_limit`
/// page whole turns; `unabridged` and `inline_diffs` are the reseed-fidelity
/// knobs, whose defaults keep the output the lean digest.
#[derive(Debug, Clone, Default)]
pub struct DigestOptions {
    /// Render newest turn first.
    pub latest: bool,
    /// The first rendered turn, in render order. Negative counts from the end.
    pub turn_offset: Option<i64>,
    pub turn_limit: Option<usize>,
    /// Render user and assistant prose whole rather than truncated: on reseed,
    /// misremembering the user's instructions or losing the assistant's
    /// reasoning trail is the worst failure mode.
    pub unabridged: bool,
    /// Let tool renderers inline change bodies beneath their rows.
    pub inline_diffs: bool,
}

/// What a tool renderer may consult besides the call.
#[derive(Debug, Clone, Copy, Default)]
pub struct RowContext {
    pub inline_diffs: bool,
}

/// One rendered digest row: `<bullet> <text> [metric] — <detail> <trailer>`,
/// optionally followed by an indented multi-line `body` block.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DigestRow {
    /// Renders the `✗` bullet instead of `·`.
    pub error: bool,
    pub text: String,
    pub metric: Option<String>,
    pub detail: Option<String>,
    pub trailer: Option<String>,
    /// Multi-line body rendered beneath the row line, indented into a markdown
    /// code block so `+`/`-` markers render safely.
    pub body: Option<String>,
}

impl DigestRow {
    pub fn render(&self) -> String {
        let bullet = if self.error { "✗" } else { "·" };
        let mut line = format!("{} {}", bullet, self.text.trim_end());
        if let Some(metric) = self.metric.as_deref().filter(|m| !m.is_empty()) {
            line.push(' ');
            line.push_str(metric);
        }
        if let Some(detail) = self.detail.as_deref().filter(|d| !d.is_empty()) {
            line.push_str(" — ");
            line.push_str(detail);
        }
        if let Some(trailer) = self.trailer.as_deref().filter(|t| !t.is_empty()) {
            line.push(' ');
            line.push_str(trailer);
        }
        if let Some(body) = self.body.as_deref().filter(|b| !b.is_empty()) {
            for body_line in body.lines() {
                line.push('\n');
                line.push_str("    ");
                line.push_str(body_line);
            }
        }
        line
    }
}

type RenderRows = dyn Fn(&ToolCall, &RowContext) -> Vec<DigestRow> + Send + Sync;

/// Per-tool row builders, keyed by the tool's name with any `mcp__<server>__`
/// prefix removed and case folded — so `mcp__cairn__read`, `Read`, and `read`
/// all reach the renderer registered as `read`. Unregistered tools render
/// through [`generic_rows`].
#[derive(Default)]
pub struct ToolRenderers {
    renderers: HashMap<String, Box<RenderRows>>,
}

impl fmt::Debug for ToolRenderers {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut names: Vec<&str> = self.renderers.keys().map(String::as_str).collect();
        names.sort_unstable();
        formatter
            .debug_struct("ToolRenderers")
            .field("registered", &names)
            .finish()
    }
}

impl ToolRenderers {
    /// The generic fallback only.
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(
        &mut self,
        name: &str,
        render: impl Fn(&ToolCall, &RowContext) -> Vec<DigestRow> + Send + Sync + 'static,
    ) -> &mut Self {
        self.renderers.insert(registry_key(name), Box::new(render));
        self
    }

    /// The rows one call renders as. An unpaired call (in flight or
    /// interrupted) has no result, so its otherwise-blank rows are marked
    /// `[pending]` whichever renderer built them.
    pub fn rows(&self, call: &ToolCall, ctx: &RowContext) -> Vec<DigestRow> {
        let mut rows = match self.renderers.get(&registry_key(&call.name)) {
            Some(render) => render(call, ctx),
            None => generic_rows(call),
        };
        if call.outcome.is_none() {
            for row in &mut rows {
                if !row.error && row.metric.is_none() {
                    row.metric = Some("[pending]".to_string());
                }
            }
        }
        rows
    }
}

fn registry_key(name: &str) -> String {
    cleaned_tool_name(name).to_ascii_lowercase()
}

/// The fallback row for any tool without a registered renderer: the tool's
/// name, a one-line summary of its input, a count figure from its output, and
/// its first output line when it failed. Public so a renderer can delegate to
/// it for shapes it does not special-case.
pub fn generic_rows(call: &ToolCall) -> Vec<DigestRow> {
    let verb = cleaned_tool_name(&call.name);
    let outcome = call.outcome.as_ref();
    let is_error = outcome.is_some_and(|o| o.is_error);
    let summary = summarize_tool_input(&call.name, &call.input).unwrap_or_default();
    let text = if summary.is_empty() {
        verb.to_string()
    } else {
        format!("{} {}", verb, summary)
    };
    let metric = outcome
        .filter(|o| !o.is_error)
        .and_then(|o| figure_for_tool_io(Some(&call.name), &o.content));
    vec![DigestRow {
        error: is_error,
        text,
        metric,
        detail: outcome
            .filter(|o| o.is_error)
            .map(|o| first_result_line(&o.content)),
        trailer: None,
        body: None,
    }]
}

/// The turn sections of a digest, each opening `\n## Turn …`. The caller owns
/// any header above them.
///
/// A heading names its clock, because it is the stamp most often compared
/// against a resume marker or an app log. It carries only the time while the
/// digest stays on one local day, and states the date on the first rendered
/// turn whose day differs from the stamp before it — seeded from the first
/// turn's own day, so a host header dated from that turn agrees with it. The
/// rule is "differs from the stamp before it", not "is later than", which is
/// why it holds under `latest` ordering too. A turn without a time renders
/// `## Turn N` alone.
pub fn render_digest(
    transcript: &Transcript,
    tools: &ToolRenderers,
    clock: &HostClock,
    opts: &DigestOptions,
) -> String {
    let turns = &transcript.turns;
    let mut out = String::new();
    let mut order: Vec<usize> = (0..turns.len()).collect();
    if opts.latest {
        order.reverse();
    }
    let start = match opts.turn_offset.unwrap_or(0) {
        raw if raw < 0 => turns.len().saturating_sub(raw.unsigned_abs() as usize),
        raw => (raw as usize).min(turns.len()),
    };
    let end = opts
        .turn_limit
        .map(|limit| (start + limit).min(order.len()))
        .unwrap_or(order.len());
    let order = &order[start..end];
    let mut previous_local_date = turns
        .first()
        .and_then(|turn| turn.started_at)
        .and_then(|at| clock.local_date(at));
    for &index in order {
        let turn = &turns[index];
        let label = turn
            .number
            .map_or_else(|| (index + 1).to_string(), |number| number.to_string());
        match turn.started_at {
            Some(at) => {
                let time = clock
                    .turn_stamp(at, previous_local_date)
                    .map(|(stamp, date)| {
                        previous_local_date = Some(date);
                        stamp
                    })
                    .unwrap_or_default();
                out.push_str(&format!("\n## Turn {} · {}\n", label, time));
            }
            None => out.push_str(&format!("\n## Turn {}\n", label)),
        }
        render_turn(&mut out, turn, tools, opts);
    }
    out
}

fn render_turn(out: &mut String, turn: &Turn, tools: &ToolRenderers, opts: &DigestOptions) {
    let link = turn.link.as_deref();
    let ctx = RowContext {
        inline_diffs: opts.inline_diffs,
    };
    let mut seen_user = false;
    for entry in &turn.entries {
        match entry {
            Entry::User(content) if content.is_empty() => {}
            Entry::User(content) => {
                // The turn's leading user entry is the user message; later ones
                // (and any system-reminder content) are injections —
                // load-bearing base-branch updates and direct messages.
                if seen_user || content.contains("<system-reminder>") {
                    out.push_str(&format!(
                        "⚠ {}\n",
                        single_line_preview(content, ERROR_LINE_PREVIEW)
                    ));
                } else {
                    seen_user = true;
                    if opts.unabridged {
                        push_verbatim(out, "**User:** ", content);
                    } else {
                        push_truncated(
                            out,
                            "**User:** ",
                            content,
                            USER_CONTENT_THRESHOLD,
                            USER_CONTENT_PREVIEW,
                            link,
                        );
                    }
                }
            }
            Entry::Assistant(content) if content.is_empty() => {}
            Entry::Assistant(content) => {
                if opts.unabridged {
                    push_verbatim(out, "**Assistant:** ", content);
                } else {
                    push_truncated(
                        out,
                        "**Assistant:** ",
                        content,
                        DIGEST_ASSISTANT_THRESHOLD,
                        DIGEST_ASSISTANT_PREVIEW,
                        link,
                    );
                }
            }
            // Reasoning does not survive into a seed: the thinking that produced
            // a conclusion is exactly what a rebuild gives up.
            Entry::Thinking(_) => {}
            Entry::Tool(call) => {
                for row in tools.rows(call, &ctx) {
                    out.push_str(&row.render());
                    out.push('\n');
                }
            }
            Entry::Error(message) => {
                let row = DigestRow {
                    error: true,
                    text: "error".to_string(),
                    detail: Some(single_line_preview(message, ERROR_LINE_PREVIEW)),
                    ..DigestRow::default()
                };
                out.push_str(&row.render());
                out.push('\n');
            }
            Entry::Marker(marker) => {
                let line = if opts.unabridged {
                    marker.full.as_ref().or(marker.skim.as_ref())
                } else {
                    marker.skim.as_ref()
                };
                if let Some(line) = line {
                    out.push_str(line);
                    out.push('\n');
                }
            }
        }
    }
}

/// Full-fidelity text of what native resume re-sends: every message, every
/// tool call's input, and every result, unelided. This is the baseline a cold
/// rebuild is priced against, so its length is what matters; its wording keeps
/// the `**Speaker:**` blocks of Cairn's full transcript projection.
///
/// Thinking and errors contribute nothing: providers do not re-send prior
/// reasoning as prefix, and an error is not conversation. An empty transcript
/// renders as the empty string.
pub fn render_verbatim(transcript: &Transcript) -> String {
    let mut out = String::new();
    for entry in transcript.turns.iter().flat_map(|turn| &turn.entries) {
        match entry {
            Entry::User(text) if !text.is_empty() => {
                out.push_str("**User:** ");
                out.push_str(text);
                out.push_str("\n\n");
            }
            Entry::Assistant(text) if !text.is_empty() => {
                out.push_str("**Assistant:** ");
                out.push_str(text);
                out.push_str("\n\n");
            }
            Entry::Tool(call) => {
                out.push_str(&format!("**Tool Call ({}):**\n", call.name));
                match &call.input {
                    Value::String(input) => out.push_str(input),
                    Value::Null => {}
                    input => out.push_str(&serde_json::to_string_pretty(input).unwrap_or_default()),
                }
                out.push_str("\n\n");
                if let Some(outcome) = &call.outcome {
                    out.push_str(&format!("**Tool Result ({}):**\n", call.name));
                    out.push_str(&outcome.content);
                    out.push_str("\n\n");
                }
            }
            Entry::Marker(marker) => {
                if let Some(verbatim) = &marker.verbatim {
                    out.push_str(verbatim);
                    out.push_str("\n\n");
                }
            }
            Entry::User(_) | Entry::Assistant(_) | Entry::Thinking(_) | Entry::Error(_) => {}
        }
    }
    out
}

/// [`render_verbatim`]'s length: the denominator of
/// [`super::price_cold_rebuild`].
pub fn verbatim_bytes(transcript: &Transcript) -> i64 {
    render_verbatim(transcript).len() as i64
}

fn push_verbatim(out: &mut String, prefix: &str, content: &str) {
    out.push_str(prefix);
    out.push_str(content.trim_end());
    out.push('\n');
}

fn push_truncated(
    out: &mut String,
    prefix: &str,
    content: &str,
    threshold: usize,
    preview: usize,
    link: Option<&str>,
) {
    let (display, truncated, _original_len) = truncate_with_info(content, threshold, preview);
    out.push_str(prefix);
    out.push_str(display.trim_end());
    out.push('\n');
    if truncated {
        let total = content.lines().count();
        let shown = display.lines().count().max(1);
        let remaining = total.saturating_sub(shown);
        match link {
            Some(link) => out.push_str(&format!("[+{} lines → {}]\n", remaining, link)),
            None => out.push_str(&format!("[+{} lines]\n", remaining)),
        }
    }
}

// ── Row helpers shared with host renderers ──────────────────────────────────────

/// A JSON value as display text: a string as is, anything else pretty-printed.
pub fn json_value_to_display(value: &Value) -> String {
    match value {
        Value::String(text) => text.to_string(),
        _ => serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string()),
    }
}

/// Truncate text and return `(display_text, was_truncated, original_len)`.
pub fn truncate_with_info(
    text: &str,
    threshold: usize,
    preview_len: usize,
) -> (String, bool, usize) {
    let original_len = text.len();
    if original_len <= threshold {
        (text.to_string(), false, original_len)
    } else {
        // Find a safe truncation point (don't break in middle of multi-byte char)
        let safe_len = text
            .char_indices()
            .take_while(|(i, _)| *i < preview_len)
            .last()
            .map(|(i, c)| i + c.len_utf8())
            .unwrap_or(0);
        (format!("{}...", &text[..safe_len]), true, original_len)
    }
}

/// A tool's name without its `mcp__<server>__` prefix.
pub fn cleaned_tool_name(tool_name: &str) -> &str {
    if tool_name.starts_with("mcp__") {
        tool_name.rsplit("__").next().unwrap_or(tool_name)
    } else {
        tool_name
    }
}

/// Whitespace collapsed to single spaces and capped at `max_len` bytes (on a
/// character boundary) with a trailing `...`.
pub fn single_line_preview(text: &str, max_len: usize) -> String {
    let collapsed = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let trimmed = collapsed.trim();
    if trimmed.is_empty() {
        return String::new();
    }

    let safe_len = trimmed
        .char_indices()
        .take_while(|(i, _)| *i < max_len)
        .last()
        .map(|(i, c)| i + c.len_utf8())
        .unwrap_or(trimmed.len());

    if trimmed.len() > safe_len {
        format!("{}...", &trimmed[..safe_len])
    } else {
        trimmed.to_string()
    }
}

/// A scalar input field as a one-line label; `None` for containers and blanks.
pub fn summarize_scalar(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => {
            let preview = single_line_preview(text, TOOL_SUMMARY_PREVIEW);
            if preview.is_empty() {
                None
            } else {
                Some(preview)
            }
        }
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(boolean) => Some(boolean.to_string()),
        _ => None,
    }
}

/// The fallback row's input summary: the field that names a call's target for
/// the tools both CLIs ship, else the first of a list of common target fields.
pub fn summarize_tool_input(tool_name: &str, input: &Value) -> Option<String> {
    let obj = input.as_object()?;
    let cleaned = cleaned_tool_name(tool_name);

    match cleaned {
        "Bash" | "bash" => obj
            .get("description")
            .or_else(|| obj.get("command"))
            .and_then(summarize_scalar),
        "Read" | "read" => obj
            .get("file_path")
            .or_else(|| obj.get("path"))
            .and_then(summarize_scalar),
        "Edit" | "edit" | "filechange" => {
            if let Some(changes) = obj.get("changes").and_then(|v| v.as_array()) {
                let count = changes.len();
                Some(format!(
                    "{} file{}",
                    count,
                    if count == 1 { "" } else { "s" }
                ))
            } else {
                obj.get("file_path").and_then(summarize_scalar)
            }
        }
        "Write" | "write" => obj.get("file_path").and_then(summarize_scalar),
        "Grep" | "grep" => obj
            .get("pattern")
            .and_then(summarize_scalar)
            .map(|pattern| format!("\"{}\"", pattern)),
        "Glob" | "glob" => obj.get("pattern").and_then(summarize_scalar),
        "task" | "Task" => obj.get("description").and_then(summarize_scalar),
        _ => obj
            .get("command")
            .or_else(|| obj.get("file_path"))
            .or_else(|| obj.get("path"))
            .or_else(|| obj.get("pattern"))
            .or_else(|| obj.get("query"))
            .or_else(|| obj.get("url"))
            .or_else(|| obj.get("title"))
            .or_else(|| obj.get("description"))
            .and_then(summarize_scalar),
    }
}

/// Lines in `text`, counting a non-empty text without a newline as one.
pub fn line_count(text: &str) -> usize {
    text.lines()
        .count()
        .max(if text.is_empty() { 0 } else { 1 })
}

/// `[N lines]` for a read's output.
pub fn figure_for_read_result(text: &str) -> Option<String> {
    let count = line_count(text);
    (count > 0).then(|| format!("[{} lines]", count))
}

/// `[exit N]` from a command's output: an exit-code line near its end, or an
/// `exit_code`/`exitCode`/`code` field when the output is JSON.
pub fn figure_for_run_result(text: &str) -> Option<String> {
    let patterns = ["Exit code:", "exit code", "exit status", "status:"];
    for line in text.lines().rev().take(20) {
        let lower = line.to_ascii_lowercase();
        if patterns.iter().any(|pattern| lower.contains(pattern)) {
            if let Some(code) = lower
                .split(|ch: char| !ch.is_ascii_digit())
                .find(|part| !part.is_empty())
            {
                return Some(format!("[exit {}]", code));
            }
        }
    }

    serde_json::from_str::<Value>(text).ok().and_then(|value| {
        value
            .get("exit_code")
            .or_else(|| value.get("exitCode"))
            .or_else(|| value.get("code"))
            .and_then(|code| code.as_i64())
            .map(|code| format!("[exit {}]", code))
    })
}

/// `(+A −D)` from a diff's `+`/`-` lines, skipping the `+++`/`---` headers.
pub fn count_prefixed_diff_lines(text: &str) -> Option<String> {
    let mut adds = 0usize;
    let mut dels = 0usize;
    for line in text.lines() {
        if line.starts_with("+++") || line.starts_with("---") {
            continue;
        }
        if line.starts_with('+') {
            adds += 1;
        } else if line.starts_with('-') {
            dels += 1;
        }
    }
    (adds > 0 || dels > 0).then(|| format!("(+{} −{})", adds, dels))
}

fn figure_for_tool_io(tool_name: Option<&str>, text: &str) -> Option<String> {
    match tool_name.map(cleaned_tool_name) {
        Some("Read" | "read") => figure_for_read_result(text),
        Some("Bash" | "bash" | "run" | "Run") => figure_for_run_result(text),
        Some("Edit" | "edit" | "Write" | "write" | "change" | "filechange") => {
            count_prefixed_diff_lines(text)
        }
        Some("Grep" | "grep") => {
            let count = line_count(text);
            (count > 0).then(|| format!("[{} matches]", count))
        }
        _ => None,
    }
}

/// The first non-blank line, trimmed.
pub fn first_nonempty_line(text: &str) -> Option<String> {
    text.lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .map(str::to_string)
}

/// A failed result's detail: its first non-blank line, previewed.
pub fn first_result_line(text: &str) -> String {
    first_nonempty_line(text)
        .map(|line| single_line_preview(&line, ERROR_LINE_PREVIEW))
        .unwrap_or_else(|| "(no output)".to_string())
}

#[cfg(test)]
mod tests {
    use super::super::transcript::{Marker, ToolOutcome, TranscriptBuilder};
    use super::*;
    use crate::session::events::{SessionEvent, TurnStatus};
    use serde_json::json;

    fn clock() -> HostClock {
        HostClock::fixed("America/Los_Angeles")
    }

    fn call(name: &str, input: Value, outcome: Option<(&str, bool)>) -> ToolCall {
        ToolCall {
            id: "id".to_string(),
            name: name.to_string(),
            input,
            outcome: outcome.map(|(content, is_error)| ToolOutcome {
                content: content.to_string(),
                is_error,
            }),
        }
    }

    fn one_turn(entries: Vec<Entry>) -> Transcript {
        Transcript {
            turns: vec![Turn {
                entries,
                ..Turn::default()
            }],
        }
    }

    #[test]
    fn dispatch_ignores_case_and_the_mcp_prefix_and_a_registration_wins() {
        let mut tools = ToolRenderers::new();
        tools.register("Read", |call, _| {
            vec![DigestRow {
                text: format!("custom {}", call.name),
                metric: Some("[1 line]".to_string()),
                ..DigestRow::default()
            }]
        });
        let ctx = RowContext::default();
        for name in ["read", "READ", "Read", "mcp__cairn__read", "mcp__x__ReAd"] {
            let rows = tools.rows(&call(name, json!({}), Some(("ok", false))), &ctx);
            assert_eq!(
                rows[0].text,
                format!("custom {name}"),
                "{name} missed the renderer"
            );
        }
        // An unregistered tool falls back to the generic row.
        let rows = tools.rows(
            &call(
                "mcp__x__lookup",
                json!({ "query": "parser" }),
                Some(("hit", false)),
            ),
            &ctx,
        );
        assert_eq!(
            rows,
            generic_rows(&call(
                "mcp__x__lookup",
                json!({ "query": "parser" }),
                Some(("hit", false))
            ))
        );
        assert_eq!(rows[0].render(), "· lookup parser");
    }

    #[test]
    fn an_unpaired_call_is_pending_whoever_renders_it() {
        let mut tools = ToolRenderers::new();
        tools.register("custom", |_, _| {
            vec![
                DigestRow {
                    text: "custom a".to_string(),
                    ..DigestRow::default()
                },
                DigestRow {
                    text: "custom b".to_string(),
                    metric: Some("[3 lines]".to_string()),
                    ..DigestRow::default()
                },
                DigestRow {
                    error: true,
                    text: "custom c".to_string(),
                    ..DigestRow::default()
                },
            ]
        });
        let ctx = RowContext::default();
        let rendered: Vec<String> = tools
            .rows(&call("custom", json!({}), None), &ctx)
            .iter()
            .map(DigestRow::render)
            .collect();
        assert_eq!(
            rendered,
            vec!["· custom a [pending]", "· custom b [3 lines]", "✗ custom c"]
        );
        assert_eq!(
            tools.rows(&call("Glob", json!({ "pattern": "*.rs" }), None), &ctx)[0].render(),
            "· Glob *.rs [pending]"
        );
        // A paired call is never pending.
        assert_eq!(
            tools.rows(&call("custom", json!({}), Some(("", false))), &ctx)[0].render(),
            "· custom a"
        );
    }

    #[test]
    fn a_marker_renders_the_field_each_projection_asks_for() {
        let marker = |skim: Option<&str>, full: Option<&str>, verbatim: Option<&str>| {
            Entry::Marker(Marker {
                skim: skim.map(str::to_string),
                full: full.map(str::to_string),
                verbatim: verbatim.map(str::to_string),
            })
        };
        let transcript = one_turn(vec![
            marker(Some("· [skim only]"), None, None),
            marker(Some("· [skim]"), Some("FULL BODY"), Some("VERBATIM BODY")),
            marker(None, None, Some("**System:** boundary")),
        ]);
        let tools = ToolRenderers::new();
        let lean = render_digest(&transcript, &tools, &clock(), &DigestOptions::default());
        assert_eq!(lean, "\n## Turn 1\n· [skim only]\n· [skim]\n");
        let full = render_digest(
            &transcript,
            &tools,
            &clock(),
            &DigestOptions {
                unabridged: true,
                ..DigestOptions::default()
            },
        );
        assert_eq!(full, "\n## Turn 1\n· [skim only]\nFULL BODY\n");
        assert_eq!(
            render_verbatim(&transcript),
            "VERBATIM BODY\n\n**System:** boundary\n\n"
        );
    }

    #[test]
    fn thinking_reaches_neither_projection() {
        let transcript = one_turn(vec![
            Entry::User("q".to_string()),
            Entry::Thinking("PRIVATE REASONING".to_string()),
            Entry::Assistant("a".to_string()),
        ]);
        let tools = ToolRenderers::new();
        for opts in [
            DigestOptions::default(),
            DigestOptions {
                unabridged: true,
                inline_diffs: true,
                ..DigestOptions::default()
            },
        ] {
            assert!(!render_digest(&transcript, &tools, &clock(), &opts).contains("PRIVATE"));
        }
        assert_eq!(
            render_verbatim(&transcript),
            "**User:** q\n\n**Assistant:** a\n\n"
        );
    }

    #[test]
    fn an_empty_transcript_weighs_nothing() {
        assert_eq!(render_verbatim(&Transcript::default()), "");
        assert_eq!(verbatim_bytes(&Transcript::default()), 0);
        assert_eq!(
            render_digest(
                &Transcript::default(),
                &ToolRenderers::new(),
                &clock(),
                &DigestOptions::default()
            ),
            ""
        );
    }

    #[test]
    fn the_verbatim_measure_counts_every_input_and_result_whole() {
        let transcript = one_turn(vec![
            Entry::Tool(call(
                "Bash",
                json!({ "command": "ls" }),
                Some(("a\nb", false)),
            )),
            Entry::Tool(call("raw", json!("string input"), None)),
            Entry::Error("not conversation".to_string()),
        ]);
        assert_eq!(
            render_verbatim(&transcript),
            "**Tool Call (Bash):**\n{\n  \"command\": \"ls\"\n}\n\n**Tool Result (Bash):**\na\nb\n\n**Tool Call (raw):**\nstring input\n\n"
        );
    }

    /// What a host holding only a session's event stream — triome's persisted
    /// JSONL — gets from this module, end to end: the stream round-trips through
    /// JSON, folds into a transcript, and renders a digest with no host
    /// vocabulary registered at all.
    #[test]
    fn a_session_event_stream_builds_a_transcript_and_digest() {
        let stream = vec![
            SessionEvent::Started {
                backend_session_id: Some("sess-1".to_string()),
                model: Some("opus".to_string()),
            },
            SessionEvent::ThinkingDelta {
                text: "The test is probably ".to_string(),
            },
            SessionEvent::ThinkingDelta {
                text: "order-dependent.".to_string(),
            },
            SessionEvent::TextDelta {
                text: "I'll run ".to_string(),
                parent_tool_use_id: None,
            },
            SessionEvent::TextDelta {
                text: "the tests.".to_string(),
                parent_tool_use_id: None,
            },
            SessionEvent::Text {
                text: "I'll run the tests first.".to_string(),
                parent_tool_use_id: None,
            },
            SessionEvent::ToolCall {
                id: "toolu_1".to_string(),
                name: "Bash".to_string(),
                input: json!({ "command": "cargo test -p parser", "description": "Run the parser tests" }),
                parent_tool_use_id: None,
            },
            SessionEvent::ToolCall {
                id: "item_2".to_string(),
                name: "commandExecution".to_string(),
                input: json!({ "type": "commandExecution", "id": "item_2", "command": "git status --short" }),
                parent_tool_use_id: None,
            },
            SessionEvent::ToolResult {
                tool_use_id: "toolu_1".to_string(),
                content: "test result: FAILED. 1 failed\nExit code: 101".to_string(),
                is_error: false,
            },
            SessionEvent::ToolResult {
                tool_use_id: "item_2".to_string(),
                content: " M src/parser.rs".to_string(),
                is_error: false,
            },
            SessionEvent::ToolCall {
                id: "toolu_3".to_string(),
                name: "Read".to_string(),
                input: json!({ "file_path": "/repo/src/missing.rs" }),
                parent_tool_use_id: None,
            },
            SessionEvent::ToolResult {
                tool_use_id: "toolu_3".to_string(),
                content: "File does not exist.".to_string(),
                is_error: true,
            },
            SessionEvent::Text {
                text: "The parser test fails on trailing commas.".to_string(),
                parent_tool_use_id: None,
            },
            SessionEvent::TurnComplete {
                status: TurnStatus::Completed,
                text: Some("The parser test fails on trailing commas.".to_string()),
                structured: None,
                usage: None,
                cost_usd: Some(0.12),
                error: None,
            },
        ];
        let second = vec![
            SessionEvent::TextDelta {
                text: "Pushing now".to_string(),
                parent_tool_use_id: None,
            },
            SessionEvent::Error {
                message: "Overloaded; retrying".to_string(),
                will_retry: true,
            },
            SessionEvent::Error {
                message: "rate limit reached for this account".to_string(),
                will_retry: false,
            },
        ];

        // triome's path: each event is persisted as a JSON line and read back.
        let reread = |events: Vec<SessionEvent>| -> Vec<SessionEvent> {
            events
                .iter()
                .map(|event| {
                    let line = serde_json::to_string(event).unwrap();
                    serde_json::from_str::<SessionEvent>(&line).unwrap()
                })
                .collect()
        };
        let mut builder = TranscriptBuilder::new();
        builder.user("The parser test is flaky. Find out why.", None);
        for event in reread(stream) {
            builder.event(&event, None);
        }
        builder.user("Fix it and push.", None);
        for event in reread(second) {
            builder.event(&event, None);
        }
        let transcript = builder.finish();

        let digest = render_digest(
            &transcript,
            &ToolRenderers::new(),
            &clock(),
            &DigestOptions::default(),
        );
        assert_eq!(
            digest,
            "\n## Turn 1\n\
             **User:** The parser test is flaky. Find out why.\n\
             **Assistant:** I'll run the tests first.\n\
             · Bash Run the parser tests [exit 101]\n\
             · commandExecution git status --short\n\
             ✗ Read /repo/src/missing.rs — File does not exist.\n\
             **Assistant:** The parser test fails on trailing commas.\n\
             \n## Turn 2\n\
             **User:** Fix it and push.\n\
             **Assistant:** Pushing now\n\
             ✗ error — rate limit reached for this account\n"
        );
        assert!(!digest.contains("order-dependent"), "thinking leaked");
        assert!(verbatim_bytes(&transcript) > digest.len() as i64);
    }
}
