//! Tool-result formatting: char-capping, augmentation-reminder assembly, and
//! change-report failure rendering, plus the shared `CallbackOutcome`.
use rmcp::model::{CallToolResult, ContentBlock};
use serde::Deserialize;

#[derive(Debug, Clone, Default)]
pub struct CallbackOutcome {
    pub result: String,
    /// `<system-reminder>` bodies from the callback response, in delivery order.
    /// Assembled into the model-visible text at the transport edge by
    /// [`assemble_reminders`]; never spliced into `result`, which stays
    /// parseable as data (e.g. the read-batch envelope, a change report).
    pub reminders: Vec<String>,
    pub transport_ok: bool,
    /// What went wrong when the callback did not deliver a host answer: the
    /// class the sink observed and how long it waited. Set by the sink at the
    /// point of failure, so nothing downstream has to recover the class from
    /// rendered text.
    pub failure: Option<CallbackFailure>,
}

/// The ways a callback can fail to deliver the host's answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallbackFailureKind {
    /// The HTTP client could not be built; nothing was sent.
    ClientBuild,
    /// The connection was never established; the host never saw the request.
    Connect,
    /// The client's own socket bound expired while waiting for the answer.
    Timeout,
    /// The connection was reset or closed by the other side mid-request.
    Reset,
    /// Writing the request body failed.
    RequestBody,
    /// Any other send failure.
    Request,
    /// The host (or a relay) answered with a non-2xx status.
    HttpStatus(u16),
    /// The answer's body could not be read.
    ResponseBody,
    /// The answer's body was not a callback response.
    ResponseParse,
}

impl CallbackFailureKind {
    /// The class word rendered in failure text and the transport-error header.
    pub fn word(self) -> &'static str {
        match self {
            Self::ClientBuild => "client_build",
            Self::Connect => "connect",
            Self::Timeout => "timeout",
            Self::Reset => "reset",
            Self::RequestBody => "request_body",
            Self::Request => "request",
            Self::HttpStatus(_) => "http_status",
            Self::ResponseBody => "response_body",
            Self::ResponseParse => "response_parse",
        }
    }
}

/// A failed callback: its class, and how long after sending the sink gave up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CallbackFailure {
    pub kind: CallbackFailureKind,
    pub elapsed: std::time::Duration,
}

/// Whether a `waitFor` whose callback failed could have armed its durable wait.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitArming {
    /// The host had not reached the point where a wait arms: either the
    /// request never arrived, or it failed inside the inline phase, and a
    /// dropped connection aborts the host's handling with it.
    NotArmed,
    /// The wait may have armed before the answer was lost.
    MaybeArmed,
}

/// Whether a failed `waitFor` callback could have armed its wait. A wait
/// writes its durable row only after
/// [`INLINE_DURABLE_WAIT_BUDGET`](cairn_common::callback_contract::INLINE_DURABLE_WAIT_BUDGET)
/// of inline waiting, so a failure sooner than that means nothing was armed.
pub fn wait_arming(failure: Option<&CallbackFailure>) -> WaitArming {
    use cairn_common::callback_contract::INLINE_DURABLE_WAIT_BUDGET;
    match failure {
        Some(CallbackFailure {
            kind: CallbackFailureKind::Connect | CallbackFailureKind::ClientBuild,
            ..
        }) => WaitArming::NotArmed,
        Some(failure) if failure.elapsed < INLINE_DURABLE_WAIT_BUDGET => WaitArming::NotArmed,
        _ => WaitArming::MaybeArmed,
    }
}

/// The opening of a `run` verdict for a callback that failed after grace.
const RUN_MAY_BE_HELD: &str = "The runner may still be holding this batch durably: if it is, this turn is parked and resumes with its result, and a reissue is refused while the batch is held or parked.";

/// What the batch behind a failed `run` callback may have done, and the safe
/// next move: the one line an agent needs instead of "the runner is down".
pub fn run_transport_verdict(
    failure: Option<&CallbackFailure>,
    is_wait: bool,
    commit_msg: Option<&str>,
) -> String {
    if is_wait {
        return match wait_arming(failure) {
            WaitArming::NotArmed => "The wait was NOT armed: the runner had not reached the point where a wait is recorded. Reissue it.".to_string(),
            WaitArming::MaybeArmed => "The wait may have been armed before the answer was lost. Reissue it; if the first call armed, the runner refuses the reissue and says so.".to_string(),
        };
    }
    let grace = std::time::Duration::from_millis(cairn_common::run_contract::RUN_GRACE_WINDOW_MS);
    let subject =
        commit_msg.map(|subject| truncate_chars(subject.lines().next().unwrap_or(subject), 120));
    match (failure, subject) {
        (
            Some(CallbackFailure {
                kind: CallbackFailureKind::Connect | CallbackFailureKind::ClientBuild,
                ..
            }),
            _,
        ) => "The batch never reached the runner; nothing ran. Reissue it.".to_string(),
        // Past grace the call may have been held or parked, but it may as
        // well have answered (a held call settles its row before the reply
        // leaves) or never been bound, so a refused reissue is the only proof
        // the batch is still running, and an unrefused one may run it twice.
        (Some(failure), Some(subject)) if failure.elapsed >= grace => format!(
            "{RUN_MAY_BE_HELD} If a reissue is not refused, the batch may have run and committed: read `cairn:@/vcs/log?limit=5` for `{subject}` before re-running."
        ),
        (Some(failure), None) if failure.elapsed >= grace => format!(
            "{RUN_MAY_BE_HELD} If a reissue is not refused, the batch may have run; it carried no commit_msg, so re-running it is safe if its other side effects are."
        ),
        (_, Some(subject)) => format!(
            "The batch may have run. Uncommitted edits are discarded either way. It carried commit_msg, so read `cairn:@/vcs/log?limit=5` for `{subject}` before re-running."
        ),
        (_, None) => "The batch may have run. Uncommitted edits are discarded either way, and it carried no commit_msg, so re-running it is safe if its other side effects are.".to_string(),
    }
}

#[derive(Debug, Deserialize)]
struct ChangeReportSummary {
    #[serde(default)]
    failures: Vec<ChangeFailureSummary>,
    #[serde(default)]
    applied: Vec<serde_json::Value>,
    #[serde(default)]
    commit: Option<ChangeCommitSummary>,
}

#[derive(Debug, Deserialize)]
struct ChangeFailureSummary {
    index: usize,
    target: String,
    mode: String,
    error: String,
}

#[derive(Debug, Deserialize)]
struct ChangeCommitSummary {
    status: String,
}

/// Max chars in a tool result (~20k tokens, safely under Claude Code's 25k token limit)
const MAX_RESULT_CHARS: usize = 45_000;

/// If text exceeds MAX_RESULT_CHARS, truncate at a line boundary and append continuation info.
/// Redact common secret patterns from a command string for safe logging.
pub fn redact_command(command: &str) -> String {
    use std::sync::LazyLock;

    static PATTERNS: LazyLock<Vec<regex::Regex>> = LazyLock::new(|| {
        vec![
            regex::Regex::new(r#"(?i)(Bearer\s+)[^\s'"]+"#).unwrap(),
            regex::Regex::new(r"\bsk[-_][a-zA-Z0-9._-]{8,}\b").unwrap(),
            regex::Regex::new(r"(?i)(export\s+[A-Z_]*(?:KEY|SECRET|TOKEN|PASSWORD)\s*=)\S+")
                .unwrap(),
            regex::Regex::new(r"(?i)(--password[= ])\S+").unwrap(),
        ]
    });

    let mut result = command.to_string();
    for pattern in PATTERNS.iter() {
        result = pattern
            .replace_all(&result, |caps: &regex::Captures| {
                if let Some(prefix) = caps.get(1) {
                    format!("{}[REDACTED]", prefix.as_str())
                } else {
                    "[REDACTED]".to_string()
                }
            })
            .to_string();
    }
    result
}

pub fn cap_text_result(text: &str, offset: usize) -> String {
    if text.len() <= MAX_RESULT_CHARS {
        return text.to_string();
    }

    // Find a safe byte position on a char boundary (avoids panic on multi-byte UTF-8)
    let mut safe_end = MAX_RESULT_CHARS;
    while safe_end > 0 && !text.is_char_boundary(safe_end) {
        safe_end -= 1;
    }

    // Find last newline before the safe limit
    let truncation_point = text[..safe_end].rfind('\n').unwrap_or(safe_end);

    let truncated = &text[..truncation_point];
    let lines_shown = truncated.lines().count();
    let total_lines = text.lines().count();
    let next_offset = offset + lines_shown;

    format!(
        "{}\n\n--- truncated (lines {}-{} of {}, {} of {} chars) ---\nCall again with offset={} to continue.",
        truncated,
        offset + 1,
        offset + lines_shown,
        total_lines,
        truncation_point,
        text.len(),
        next_offset
    )
}

/// Append each system-reminder body to `content`, wrapped in the
/// `<system-reminder>` envelope. This is the single assembly point for
/// augmentation reminders across every verb: the backend ships them as data on
/// the callback response (`CallbackResponse::reminders`) and the CLI renders
/// them into the model-visible text here, reproducing the legacy wrapping and
/// ordering byte-for-byte. Handler content therefore stays parseable as data
/// end-to-end — no JSON payload is ever followed by trailing reminder text.
pub fn assemble_reminders(mut content: String, reminders: &[String]) -> String {
    for body in reminders {
        content.push_str("\n\n<system-reminder>\n");
        content.push_str(body);
        content.push_str("\n</system-reminder>");
    }
    content
}

fn sanitize_callback_url(callback_url: &str) -> String {
    match url::Url::parse(callback_url) {
        Ok(mut parsed) => {
            let _ = parsed.set_username("");
            let _ = parsed.set_password(None);
            parsed.set_query(None);
            parsed.set_fragment(None);
            parsed.to_string()
        }
        Err(_) => callback_url.to_string(),
    }
}

fn callback_failure_class(outcome: &CallbackOutcome) -> String {
    match outcome.failure {
        Some(CallbackFailure {
            kind: CallbackFailureKind::HttpStatus(status),
            ..
        }) => format!("http_status {status}"),
        Some(failure) => failure.kind.word().to_string(),
        None => "unknown".to_string(),
    }
}

fn transport_error_message(outcome: &CallbackOutcome, callback_url: &str, tool: &str) -> String {
    format!(
        "MCP callback transport failed\nurl: {}\ntool: {}\nclass: {}\nerror: {}",
        sanitize_callback_url(callback_url),
        tool,
        callback_failure_class(outcome),
        outcome.result
    )
}

fn callback_transport_error_result(
    outcome: &CallbackOutcome,
    callback_url: &str,
    tool: &str,
) -> CallToolResult {
    let message = transport_error_message(outcome, callback_url, tool);
    CallToolResult::error(vec![ContentBlock::text(cap_text_result(&message, 0))])
}

/// A `run` callback that delivered no host answer, as a tool error carrying
/// the transport detail and the one verdict line for what the batch may have
/// done ([`run_transport_verdict`]).
pub fn run_transport_error_result(
    outcome: &CallbackOutcome,
    callback_url: &str,
    is_wait: bool,
    commit_msg: Option<&str>,
) -> CallToolResult {
    let message = format!(
        "{}\nverdict: {}",
        transport_error_message(outcome, callback_url, "run"),
        run_transport_verdict(outcome.failure.as_ref(), is_wait, commit_msg)
    );
    CallToolResult::error(vec![ContentBlock::text(cap_text_result(&message, 0))])
}

fn change_report_failure_reason(raw: &str) -> Option<String> {
    let report: ChangeReportSummary = serde_json::from_str(raw)
        .or_else(|_| {
            let first_chunk = raw.trim_start().split_once("\n\n").map(|(head, _)| head);
            first_chunk
                .ok_or_else(|| serde_json::Error::io(std::io::ErrorKind::InvalidData.into()))
                .and_then(serde_json::from_str)
        })
        .ok()?;
    if !report.failures.is_empty() {
        let total = report.applied.len() + report.failures.len();
        let failures = report
            .failures
            .iter()
            .map(|failure| {
                format!(
                    "[{}] {} {}: {}",
                    failure.index, failure.target, failure.mode, failure.error
                )
            })
            .collect::<Vec<_>>()
            .join("; ");
        return Some(format!(
            "{} of {} changes failed: {}",
            report.failures.len(),
            total,
            failures
        ));
    }

    let commit_status = report.commit?.status;
    if commit_status.eq_ignore_ascii_case("failed") {
        return Some("change commit failed".to_string());
    }

    None
}

/// Truncate to at most `max` characters on a char boundary. Response/error
/// bodies are arbitrary bytes; slicing by byte offset (`&s[..n]`) panics when a
/// multi-byte char straddles the cut, so clamp by chars instead.
pub fn truncate_chars(text: &str, max: usize) -> String {
    text.chars().take(max).collect()
}

/// Build the user-facing message for a non-2xx MCP callback response. HTTP 413
/// gets actionable guidance about the body-size limit; other statuses report
/// the status and the request size so the failure is diagnosable.
pub fn http_status_error_message(status: u16, reason: &str, request_bytes: usize) -> String {
    let mut message = format!(
        "Cairn's MCP callback gateway returned HTTP {status} {reason}. Request body was {request_bytes} bytes."
    );
    if status == 413 {
        message.push_str(
            " The payload exceeds the callback body-size limit; split the change into smaller \
             writes (fewer files per call, or a smaller content/patch).",
        );
    }
    message
}

pub fn change_callback_result(
    outcome: CallbackOutcome,
    callback_url: &str,
    tool: &str,
) -> CallToolResult {
    if !outcome.transport_ok {
        return callback_transport_error_result(&outcome, callback_url, tool);
    }

    // Parse the change report from the clean handler content first, then
    // assemble augmentation reminders into the model-visible text.
    let failure = change_report_failure_reason(&outcome.result);
    let body = assemble_reminders(outcome.result, &outcome.reminders);
    match failure {
        Some(reason) => {
            let text = format!("{}\n\n{}", reason, body);
            CallToolResult::error(vec![ContentBlock::text(cap_text_result(&text, 0))])
        }
        None => CallToolResult::success(vec![ContentBlock::text(cap_text_result(&body, 0))]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::get_text;
    use cairn_common::protocol::CallbackResponse;
    use cairn_common::read::{ReadBatchEnvelope, RunBatchEnvelope};

    #[test]
    fn http_status_error_message_explains_413() {
        let msg = http_status_error_message(413, "Payload Too Large", 5_000_000);
        assert!(msg.contains("413"));
        assert!(msg.contains("5000000 bytes"));
        assert!(msg.contains("body-size limit"));
    }

    #[test]
    fn http_status_error_message_reports_other_statuses() {
        let msg = http_status_error_message(500, "Internal Server Error", 42);
        assert!(msg.contains("500"));
        assert!(msg.contains("42 bytes"));
        assert!(!msg.contains("body-size limit"));
    }

    #[test]
    fn truncate_chars_does_not_panic_on_multibyte_boundary() {
        // A run of multi-byte chars whose byte length exceeds the limit but whose
        // char count is under it: byte slicing would panic mid-codepoint.
        let body = "é".repeat(400); // 400 chars, 800 bytes
        let truncated = truncate_chars(&body, 500);
        assert_eq!(truncated.chars().count(), 400);

        let long = "€".repeat(1000); // 1000 chars, 3000 bytes
        let clamped = truncate_chars(&long, 200);
        assert_eq!(clamped.chars().count(), 200);
    }

    #[test]
    fn test_cap_text_result_short_text_unchanged() {
        let text = "line 1\nline 2\nline 3";
        let result = cap_text_result(text, 0);
        assert_eq!(result, text);
    }

    #[test]
    fn test_cap_text_result_truncates_at_line_boundary() {
        // Build text that exceeds MAX_RESULT_CHARS
        let line = "x".repeat(100);
        let lines: Vec<&str> =
            std::iter::repeat_n(line.as_str(), MAX_RESULT_CHARS / 100 + 100).collect();
        let text = lines.join("\n");
        assert!(text.len() > MAX_RESULT_CHARS);

        let result = cap_text_result(&text, 0);

        // Should be truncated
        assert!(result.len() < text.len());
        // Should contain continuation hint
        assert!(result.contains("--- truncated"));
        assert!(result.contains("Call again with offset="));
        // The truncated content should end at a line boundary (before the hint)
        let content_part = result.split("\n\n--- truncated").next().unwrap();
        // Each line is 100 chars, so content should be made of complete lines
        assert!(content_part.lines().all(|l| l.len() == 100));
    }

    #[test]
    fn test_cap_text_result_with_nonzero_offset() {
        let line = "x".repeat(100);
        let lines: Vec<&str> =
            std::iter::repeat_n(line.as_str(), MAX_RESULT_CHARS / 100 + 100).collect();
        let text = lines.join("\n");

        let result = cap_text_result(&text, 50);
        // Continuation hint should show offset-based line numbers
        assert!(result.contains("lines 51-"));
        assert!(result.contains("Call again with offset="));
    }

    #[test]
    fn test_cap_text_result_no_newlines() {
        // A single long line with no newlines should still truncate
        let text = "x".repeat(MAX_RESULT_CHARS + 1000);
        let result = cap_text_result(&text, 0);
        assert!(result.contains("--- truncated"));
        // Should truncate at MAX_RESULT_CHARS since no newline found
        assert!(result.starts_with(&"x".repeat(MAX_RESULT_CHARS)));
    }

    #[test]
    fn test_cap_text_result_multibyte_utf8_boundary() {
        // Build text where MAX_RESULT_CHARS falls inside a multi-byte char.
        // U+00E9 (é) is 2 bytes in UTF-8 (0xC3 0xA9).
        // Fill with ASCII up to one byte before the limit, then a 2-byte char
        // so the limit lands on the second byte of the é.
        let prefix = "a".repeat(MAX_RESULT_CHARS - 1);
        // é adds 2 bytes → total = MAX_RESULT_CHARS + 1 bytes, triggers truncation
        // and MAX_RESULT_CHARS falls on byte index inside the é
        let text = format!("{}é{}", prefix, "b".repeat(2000));
        assert!(text.len() > MAX_RESULT_CHARS);

        // Before the fix this would panic with "byte index is not a char boundary"
        let result = cap_text_result(&text, 0);
        assert!(result.contains("--- truncated"));
        // The é should be excluded (boundary rounds down), so content is just the prefix
        assert!(result.starts_with(&prefix));
    }

    #[test]
    fn test_cap_text_result_multibyte_utf8_with_newlines() {
        // Mix multi-byte chars with newlines to test both boundary + line truncation
        // Each line: 99 ASCII chars + é (2 bytes) + newline = 102 bytes per line
        let line = format!("{}é", "z".repeat(99));
        assert_eq!(line.len(), 101); // 99 + 2 bytes for é
        let num_lines = MAX_RESULT_CHARS / 101 + 100;
        let text = std::iter::repeat_n(line.as_str(), num_lines)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(text.len() > MAX_RESULT_CHARS);

        let result = cap_text_result(&text, 0);
        assert!(result.contains("--- truncated"));
        // Content before the truncation marker should end at a complete line
        let content_part = result.split("\n\n--- truncated").next().unwrap();
        for line in content_part.lines() {
            assert!(line.ends_with('é'), "Line should be intact: {:?}", line);
        }
    }

    fn failure(kind: CallbackFailureKind, secs: u64) -> CallbackFailure {
        CallbackFailure {
            kind,
            elapsed: std::time::Duration::from_secs(secs),
        }
    }

    #[test]
    fn the_class_comes_from_the_typed_failure() {
        let cases = [
            (CallbackFailureKind::ClientBuild, "client_build"),
            (CallbackFailureKind::Connect, "connect"),
            (CallbackFailureKind::Timeout, "timeout"),
            (CallbackFailureKind::Reset, "reset"),
            (CallbackFailureKind::RequestBody, "request_body"),
            (CallbackFailureKind::Request, "request"),
            (CallbackFailureKind::HttpStatus(504), "http_status 504"),
            (CallbackFailureKind::ResponseBody, "response_body"),
            (CallbackFailureKind::ResponseParse, "response_parse"),
        ];
        for (kind, class) in cases {
            let outcome = CallbackOutcome {
                // The text no longer decides the class.
                result: "Error calling Tauri: anything".to_string(),
                failure: Some(failure(kind, 1)),
                ..Default::default()
            };
            assert_eq!(callback_failure_class(&outcome), class);
        }
        assert_eq!(
            callback_failure_class(&CallbackOutcome::default()),
            "unknown"
        );
    }

    #[test]
    fn a_wait_is_not_armed_unless_it_could_have_passed_the_inline_phase() {
        use CallbackFailureKind::*;
        let cases = [
            (Some(failure(Connect, 300)), WaitArming::NotArmed),
            (Some(failure(ClientBuild, 0)), WaitArming::NotArmed),
            (Some(failure(Reset, 1)), WaitArming::NotArmed),
            (Some(failure(Timeout, 44)), WaitArming::NotArmed),
            (Some(failure(Reset, 45)), WaitArming::MaybeArmed),
            (Some(failure(Timeout, 195)), WaitArming::MaybeArmed),
            (Some(failure(HttpStatus(504), 240)), WaitArming::MaybeArmed),
            (None, WaitArming::MaybeArmed),
        ];
        for (failure, expected) in cases {
            assert_eq!(wait_arming(failure.as_ref()), expected, "{failure:?}");
        }
    }

    #[test]
    fn the_run_verdict_names_what_the_batch_may_have_done() {
        use CallbackFailureKind::*;
        let never = run_transport_verdict(Some(&failure(Connect, 0)), false, Some("x"));
        assert!(
            never.contains("never reached the runner; nothing ran"),
            "{never}"
        );
        let early =
            run_transport_verdict(Some(&failure(Reset, 30)), false, Some("fix parser\n\nbody"));
        assert!(early.contains("may have run"), "{early}");
        assert!(
            early.contains("cairn:@/vcs/log?limit=5") && early.contains("`fix parser`"),
            "{early}"
        );
        let early_no_commit = run_transport_verdict(Some(&failure(Reset, 30)), false, None);
        assert!(!early_no_commit.contains("vcs/log"), "{early_no_commit}");
        // Past grace the batch may be held, but it may as well have answered
        // or never been bound, so a committing batch keeps its vcs/log advice
        // and a reissue is promised refused only while the batch is held.
        let late = run_transport_verdict(Some(&failure(Timeout, 195)), false, Some("fix parser"));
        assert!(
            late.contains("may still be holding this batch durably"),
            "{late}"
        );
        assert!(
            late.contains("refused while the batch is held or parked"),
            "{late}"
        );
        assert!(
            late.contains("If a reissue is not refused")
                && late.contains("cairn:@/vcs/log?limit=5")
                && late.contains("`fix parser`"),
            "{late}"
        );
        assert!(!late.contains("Reissuing is safe"), "{late}");
        let late_no_commit = run_transport_verdict(Some(&failure(Timeout, 195)), false, None);
        assert!(
            late_no_commit.contains("may still be holding"),
            "{late_no_commit}"
        );
        assert!(!late_no_commit.contains("vcs/log"), "{late_no_commit}");
        let not_armed = run_transport_verdict(Some(&failure(Reset, 2)), true, None);
        assert!(not_armed.contains("NOT armed"), "{not_armed}");
        let maybe = run_transport_verdict(Some(&failure(Reset, 60)), true, None);
        assert!(maybe.contains("refuses the reissue"), "{maybe}");
        // Every verdict is distinct: the next move differs for each.
        let all = [
            never,
            early,
            early_no_commit,
            late,
            late_no_commit,
            not_armed,
            maybe,
        ];
        for (i, a) in all.iter().enumerate() {
            for b in &all[i + 1..] {
                assert_ne!(a, b);
            }
        }
    }

    #[test]
    fn test_change_transport_failure_becomes_tool_error() {
        let outcome = CallbackOutcome {
            result: "Error calling Tauri: request: connection refused".to_string(),
            transport_ok: false,
            failure: Some(failure(CallbackFailureKind::Request, 1)),
            ..Default::default()
        };

        let result = change_callback_result(
            outcome,
            "http://user:secret@127.0.0.1:3847/api/mcp?token=secret",
            "write",
        );

        assert!(result.is_error.unwrap_or(false));
        let text = get_text(&result);
        assert!(text.contains("MCP callback transport failed"));
        assert!(text.contains("tool: write"));
        assert!(text.contains("class: request"));
        assert!(text.contains("http://127.0.0.1:3847/api/mcp"));
        assert!(!text.contains("user:secret"));
        assert!(!text.contains("token=secret"));
    }

    #[test]
    fn test_change_report_failure_becomes_tool_error() {
        // Mirrors the real failure shape: empty/null/false fields are omitted,
        // but genuine `failures` and `partial_success: true` are still present.
        let report = serde_json::json!({
            "applied": [],
            "failures": [{
                "index": 1,
                "target": "src/lib.rs",
                "mode": "patch",
                "kind": "file",
                "error": "old_string not found"
            }],
            "partial_success": true
        })
        .to_string();
        let outcome = CallbackOutcome {
            result: report,
            transport_ok: true,
            ..Default::default()
        };

        let result = change_callback_result(outcome, "http://127.0.0.1:3847/api/mcp", "write");

        assert!(result.is_error.unwrap_or(false));
        let text = get_text(&result);
        assert!(text.contains("1 of 1 changes failed: [1] src/lib.rs patch"));
        assert!(text.contains("old_string not found"));
        assert!(text.contains("partial_success"));
    }

    #[test]
    fn test_change_report_failure_before_blocking_result_becomes_tool_error() {
        let report = serde_json::json!({
            "applied": [],
            "failures": [{
                "index": 0,
                "target": "cairn://p/MISSING/messages",
                "mode": "append",
                "kind": "resource",
                "error": "No project found"
            }]
        })
        .to_string();
        let outcome = CallbackOutcome {
            result: format!("{report}\n\nTask append target node not found"),
            transport_ok: true,
            ..Default::default()
        };

        let result = change_callback_result(outcome, "http://127.0.0.1:3847/api/mcp", "write");

        assert!(result.is_error.unwrap_or(false));
        let text = get_text(&result);
        assert!(text.contains("1 of 1 changes failed: [0] cairn://p/MISSING/messages append"));
        assert!(text.contains("Task append target node not found"));
    }

    #[test]
    fn test_change_commit_failure_becomes_tool_error() {
        // Mirrors the real shape: `failure`, `partial_success`, and
        // `transactional` are omitted; the failing `commit` carries the signal.
        let report = serde_json::json!({
            "applied": [{
                "index": 0,
                "target": "src/lib.rs",
                "mode": "patch",
                "kind": "file",
                "summary": "updated"
            }],
            "commit": {
                "status": "failed",
                "sha": null,
                "pr_number": null,
                "message": "git commit failed"
            }
        })
        .to_string();
        let outcome = CallbackOutcome {
            result: report,
            transport_ok: true,
            ..Default::default()
        };

        let result = change_callback_result(outcome, "http://127.0.0.1:3847/api/mcp", "write");

        assert!(result.is_error.unwrap_or(false));
        let text = get_text(&result);
        assert!(text.contains("change commit failed"));
        assert!(text.contains("git commit failed"));
    }

    #[test]
    fn test_successful_change_report_remains_success() {
        // Mirrors a clean commit success: only `applied` and `commit` remain.
        let report = serde_json::json!({
            "applied": [],
            "commit": {
                "status": "committed",
                "sha": "abc1234",
                "pr_number": null,
                "message": null
            }
        })
        .to_string();
        let outcome = CallbackOutcome {
            result: report,
            transport_ok: true,
            ..Default::default()
        };

        let result = change_callback_result(outcome, "http://127.0.0.1:3847/api/mcp", "write");

        assert!(!result.is_error.unwrap_or(false));
        assert!(get_text(&result).contains("committed"));
    }
    #[test]
    fn read_envelope_parses_clean_with_reminders_as_data() {
        // CAIRN-1590 regression: the callback response carries the read-batch
        // envelope JSON in `result` and the system-reminder bodies separately
        // in `reminders`. The JSON parses cleanly (no trailing text to split)
        // and the assembled model-visible text contains every reminder.
        let envelope = cairn_common::read::ReadBatchEnvelope {
            text: "=== file:a.rs [lines 1\u{2013}1 of 1] ===\n     1\ta".to_string(),
            images: vec![],
            segments: vec![],
            affordances: vec![],
            bodies: None,
        };
        let response = CallbackResponse {
            result: serde_json::to_string(&envelope).unwrap(),
            artifact_uri: None,
            reminders: vec![
                "[Direct message from planner] hello mid-turn".to_string(),
                "The worktree has uncommitted changes. Commit them.".to_string(),
            ],
        };

        // Content is pure JSON: it deserializes as the envelope with no leftover.
        let parsed: ReadBatchEnvelope = serde_json::from_str(&response.result)
            .expect("handler content stays parseable as the bare envelope");
        assert_eq!(parsed.text, envelope.text);

        // Assembly appends every reminder, each wrapped in a system-reminder block.
        let assembled = assemble_reminders(parsed.text, &response.reminders);
        assert!(assembled.contains("[Direct message from planner] hello mid-turn"));
        assert!(assembled.contains("The worktree has uncommitted changes"));
        assert_eq!(assembled.matches("<system-reminder>").count(), 2);
    }

    #[test]
    fn run_envelope_image_becomes_a_content_block() {
        // The run tool parses a RunBatchEnvelope and lifts each image into its
        // own content block after the text, so an image-bearing MCP tool result
        // (e.g. an Axon look screenshot) reaches the agent. Exercise that lift.
        let envelope = RunBatchEnvelope {
            text: "=== look ===\nAX tree".to_string(),
            images: vec![cairn_common::read::ImageBlock::inline("image/png", "b64")],
            invalid_input: false,
            data: None,
        };
        let serialized = serde_json::to_string(&envelope).unwrap();
        let parsed: RunBatchEnvelope = serde_json::from_str(&serialized).unwrap();

        let mut blocks: Vec<ContentBlock> = vec![ContentBlock::text(parsed.text)];
        for image in parsed.images {
            blocks.push(ContentBlock::image(image.data, image.mime_type));
        }
        assert_eq!(blocks.len(), 2);
        let image_block = blocks[1].as_image().expect("second block is an image");
        assert_eq!(image_block.mime_type, "image/png");
        assert_eq!(image_block.data, "b64");
    }

    #[test]
    fn assemble_reminders_is_noop_without_reminders() {
        let content = "plain result".to_string();
        assert_eq!(assemble_reminders(content.clone(), &[]), content);
    }

    #[test]
    fn assemble_reminders_wraps_each_body_in_delivery_order() {
        let out = assemble_reminders(
            "body".to_string(),
            &["first".to_string(), "second".to_string()],
        );
        assert_eq!(
            out,
            "body\n\n<system-reminder>\nfirst\n</system-reminder>\n\n<system-reminder>\nsecond\n</system-reminder>"
        );
    }
}
