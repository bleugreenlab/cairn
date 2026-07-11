//! Tool-result formatting: char-capping, augmentation-reminder assembly, and
//! change-report failure rendering, plus the shared `CallbackOutcome`.
use rmcp::model::{CallToolResult, Content};
use serde::Deserialize;

#[derive(Debug, Clone, Default)]
pub(crate) struct CallbackOutcome {
    pub(crate) result: String,
    /// `<system-reminder>` bodies from the callback response, in delivery order.
    /// Assembled into the model-visible text at the transport edge by
    /// [`assemble_reminders`]; never spliced into `result`, which stays
    /// parseable as data (e.g. the read-batch envelope, a change report).
    pub(crate) reminders: Vec<String>,
    pub(crate) transport_ok: bool,
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
pub(crate) fn redact_command(command: &str) -> String {
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

pub(crate) fn cap_text_result(text: &str, offset: usize) -> String {
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

/// Final guard for `run` results. Core's `compose_run_output` already caps batch
/// output to its `MAX_RUN_RESULT_CHARS` (deliberately equal to `MAX_RESULT_CHARS`)
/// with tail-biased per-item capping, so a well-formed core result is at or under
/// the cap and passes through untouched. This is a defensive backstop only: an
/// over-cap result is tail-biased here too — keeping the end (error summaries, the
/// last `&&` segment) rather than the head — and never carries the `offset=`
/// continuation footer that `read` uses. A finished command's output is not
/// re-readable, so the actionable advice is to re-run scoped or use a terminal.
pub(crate) fn cap_run_result(text: &str) -> String {
    if text.len() <= MAX_RESULT_CHARS {
        return text.to_string();
    }
    const ADVICE: &str =
        "--- run output exceeded the char cap; showing the tail. Re-run scoped (grep/tail the command) or use a terminal resource for full output. ---\n";
    // Keep the last bytes, leaving room for the advice prefix so the whole result
    // stays within the cap. Snap up to a char boundary to avoid splitting UTF-8.
    let tail_budget = MAX_RESULT_CHARS.saturating_sub(ADVICE.len());
    let mut start = text.len().saturating_sub(tail_budget);
    while start < text.len() && !text.is_char_boundary(start) {
        start += 1;
    }
    format!("{ADVICE}{}", &text[start..])
}

/// Append each system-reminder body to `content`, wrapped in the
/// `<system-reminder>` envelope. This is the single assembly point for
/// augmentation reminders across every verb: the backend ships them as data on
/// the callback response (`CallbackResponse::reminders`) and the CLI renders
/// them into the model-visible text here, reproducing the legacy wrapping and
/// ordering byte-for-byte. Handler content therefore stays parseable as data
/// end-to-end — no JSON payload is ever followed by trailing reminder text.
pub(crate) fn assemble_reminders(mut content: String, reminders: &[String]) -> String {
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

fn callback_failure_class(result: &str) -> &'static str {
    if result.starts_with("Error calling Tauri:") {
        "request"
    } else if result.starts_with("Error parsing response:") {
        "response_parse"
    } else if result.starts_with("Error reading response body:") {
        "response_body"
    } else if result.starts_with("Error building HTTP client:") {
        "client_build"
    } else {
        "http_status"
    }
}

fn callback_transport_error_result(
    outcome: &CallbackOutcome,
    callback_url: &str,
    tool: &str,
) -> CallToolResult {
    let message = format!(
        "MCP callback transport failed\nurl: {}\ntool: {}\nclass: {}\nerror: {}",
        sanitize_callback_url(callback_url),
        tool,
        callback_failure_class(&outcome.result),
        outcome.result
    );
    CallToolResult::error(vec![Content::text(cap_text_result(&message, 0))])
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
pub(crate) fn truncate_chars(text: &str, max: usize) -> String {
    text.chars().take(max).collect()
}

/// Build the user-facing message for a non-2xx MCP callback response. HTTP 413
/// gets actionable guidance about the body-size limit; other statuses report
/// the status and the request size so the failure is diagnosable.
pub(crate) fn http_status_error_message(status: u16, reason: &str, request_bytes: usize) -> String {
    let mut message = format!(
        "MCP callback returned HTTP {status} {reason}. Request body was {request_bytes} bytes."
    );
    if status == 413 {
        message.push_str(
            " The payload exceeds the callback body-size limit; split the change into smaller \
             writes (fewer files per call, or a smaller content/patch).",
        );
    }
    message
}

pub(crate) fn change_callback_result(
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
            CallToolResult::error(vec![Content::text(cap_text_result(&text, 0))])
        }
        None => CallToolResult::success(vec![Content::text(cap_text_result(&body, 0))]),
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

    #[test]
    fn test_cap_run_result_passthrough_under_cap() {
        // Core caps run output at MAX_RUN_RESULT_CHARS == MAX_RESULT_CHARS, so a
        // well-formed result passes through unchanged.
        let text = "=== git diff ===\nsome output\n--- elided 10 lines / 200 chars; tail kept below ---\ntail";
        assert_eq!(cap_run_result(text), text);
    }

    #[test]
    fn test_cap_run_result_tail_biased_no_offset_footer() {
        // Defensive backstop: an over-cap result is tail-biased, never carrying the
        // read-style offset continuation footer.
        let text = format!(
            "HEAD-START\n{}\nTAIL-SIGNAL",
            "x".repeat(MAX_RESULT_CHARS * 2)
        );
        let result = cap_run_result(&text);
        assert!(result.len() <= MAX_RESULT_CHARS);
        assert!(
            result.ends_with("TAIL-SIGNAL"),
            "keeps the tail, not the head"
        );
        assert!(!result.contains("Call again with offset"));
        assert!(result.contains("Re-run scoped"));
    }

    #[test]
    fn test_cap_run_result_multibyte_boundary_safe() {
        // A long multibyte body must not split a char when tail-slicing.
        let text = "é".repeat(MAX_RESULT_CHARS); // 2 bytes each → 2x the cap
        let result = cap_run_result(&text);
        assert!(result.len() <= MAX_RESULT_CHARS);
        assert!(!result.contains("Call again with offset"));
    }

    #[test]
    fn test_change_transport_failure_becomes_tool_error() {
        let outcome = CallbackOutcome {
            result: "Error calling Tauri: connection refused".to_string(),
            transport_ok: false,
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
            images: vec![cairn_common::read::ImageBlock {
                mime_type: "image/png".to_string(),
                data: "b64".to_string(),
            }],
        };
        let serialized = serde_json::to_string(&envelope).unwrap();
        let parsed: RunBatchEnvelope = serde_json::from_str(&serialized).unwrap();

        let mut blocks: Vec<Content> = vec![Content::text(parsed.text)];
        for image in parsed.images {
            blocks.push(Content::image(image.data, image.mime_type));
        }
        assert_eq!(blocks.len(), 2);
        let image_block = blocks[1].raw.as_image().expect("second block is an image");
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
