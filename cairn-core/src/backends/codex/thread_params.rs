use super::CODEX_BACKEND_NAME;
use crate::backends::run_state::run_backend_db;
use crate::models::{Fence, Model};
use crate::storage::{LocalDb, RowExt};
use cairn_db::turso::params;
use serde_json::Value;
use std::sync::Arc;

const RESUME_FALLBACK_TRANSCRIPT_CHARS: usize = 24_000;

// Codex's `baseInstructions` and `developerInstructions` payloads are derived by
// slicing the assembled prompt segments in `backend_impl` (via
// `base_instructions_from_segments` / `developer_instructions_from_segments`),
// so sent bytes equal the persisted `system:prompt` segments by construction.

pub(super) fn codex_sandbox_mode(fence: Fence) -> &'static str {
    match fence {
        // Ask/deny agents get Codex's workspace-write sandbox; Cairn's verb
        // handlers provide the user-facing fence decision.
        Fence::Ask | Fence::Deny => "workspace-write",
        Fence::Allow => "danger-full-access",
    }
}

// Codex thread-start takes more parameters than clippy's default threshold;
// they are distinct primitive knobs, so a params struct would add indirection
// without improving clarity.
#[allow(clippy::too_many_arguments)]
pub(super) fn build_thread_start_params(
    cwd: &str,
    approval_policy: &str,
    sandbox_mode: &str,
    model: Option<&str>,
    reasoning_effort: Option<&str>,
    service_tier: Option<&str>,
    base_instructions: &str,
    developer_instructions: Option<&str>,
) -> Value {
    let mut params = serde_json::json!({
        "model": model.unwrap_or(Model::GPT_5_6_LUNA),
        "cwd": cwd,
        "approvalPolicy": approval_policy,
        "sandbox": sandbox_mode,
        "serviceName": "cairn",
        "baseInstructions": base_instructions,
    });
    if let Some(model) = model {
        params["model"] = serde_json::json!(model);
    }
    if let Some(effort) = reasoning_effort {
        params["reasoningEffort"] = serde_json::json!(effort);
    }
    if let Some(tier) = service_tier {
        params["serviceTier"] = serde_json::json!(tier);
    }
    if let Some(instructions) = developer_instructions {
        params["developerInstructions"] = serde_json::json!(instructions);
    }
    params
}

#[allow(clippy::too_many_arguments)]
pub(super) fn build_thread_resume_params(
    thread_id: &str,
    cwd: &str,
    approval_policy: &str,
    sandbox_mode: &str,
    model: Option<&str>,
    reasoning_effort: Option<&str>,
    service_tier: Option<&str>,
    base_instructions: &str,
    developer_instructions: Option<&str>,
) -> Value {
    let mut params = serde_json::json!({
        "threadId": thread_id,
        "cwd": cwd,
        "approvalPolicy": approval_policy,
        "sandbox": sandbox_mode,
        "serviceName": "cairn",
        "baseInstructions": base_instructions,
    });
    if let Some(model) = model {
        params["model"] = serde_json::json!(model);
    }
    if let Some(effort) = reasoning_effort {
        params["reasoningEffort"] = serde_json::json!(effort);
    }
    if let Some(tier) = service_tier {
        params["serviceTier"] = serde_json::json!(tier);
    }
    if let Some(instructions) = developer_instructions {
        params["developerInstructions"] = serde_json::json!(instructions);
    }
    params
}

#[allow(clippy::too_many_arguments)]
pub(super) fn build_thread_fork_params(
    thread_id: &str,
    cwd: &str,
    approval_policy: &str,
    sandbox_mode: &str,
    model: Option<&str>,
    reasoning_effort: Option<&str>,
    service_tier: Option<&str>,
    base_instructions: &str,
    developer_instructions: Option<&str>,
) -> Value {
    build_thread_resume_params(
        thread_id,
        cwd,
        approval_policy,
        sandbox_mode,
        model,
        reasoning_effort,
        service_tier,
        base_instructions,
        developer_instructions,
    )
}

pub(super) fn is_missing_rollout_error(err: &str, thread_id: &str) -> bool {
    err.contains("no rollout found for thread id")
        && (thread_id.is_empty() || err.contains(thread_id))
}

pub(super) fn build_resume_fallback_prompt(
    run_db: &Arc<LocalDb>,
    stale_session_id: &str,
    latest_user_message: &str,
) -> Option<String> {
    let db = run_db.clone();
    let stale_session_id = stale_session_id.to_string();
    let event_rows: Vec<(String, i32, String, String)> =
        run_backend_db(CODEX_BACKEND_NAME, async move {
            db.read(|conn| {
                let stale_session_id = stale_session_id.clone();
                Box::pin(async move {
                    // Live-only reader (no archival reconstruction): this runs
                    // mid-session to rebuild Codex thread params from the active
                    // session's events, before teardown makes anything
                    // archivable. It never sees a gitcoord/zstd stub.
                    let mut rows = conn
                        .query(
                            "SELECT run_id, sequence, event_type, data
                         FROM events
                         WHERE session_id = ?1
                         ORDER BY created_at ASC, sequence ASC",
                            params![stale_session_id.as_str()],
                        )
                        .await?;
                    let mut event_rows = Vec::new();
                    while let Some(row) = rows.next().await? {
                        event_rows.push((
                            row.text(0)?,
                            row.i64(1)? as i32,
                            row.text(2)?,
                            row.text(3)?,
                        ));
                    }
                    Ok(event_rows)
                })
            })
            .await
            .map_err(|e| e.to_string())
        })
        .ok()?;

    if event_rows.is_empty() {
        return None;
    }

    let transcript = crate::transcripts::format_transcript_full(&event_rows);
    let transcript = trim_resume_transcript(&transcript, RESUME_FALLBACK_TRANSCRIPT_CHARS);

    Some(format_resume_fallback_prompt(
        &transcript,
        latest_user_message,
    ))
}

/// Wrap a prior-session transcript and the latest reply into the fresh-thread
/// fallback prompt. Pure so the ordering invariant (prior context precedes the
/// latest message) is unit-testable without a database.
pub(super) fn format_resume_fallback_prompt(transcript: &str, latest_user_message: &str) -> String {
    format!(
        "The previous Codex thread could not be resumed, so you are continuing from a transcript snapshot instead.\n\n\
## Prior Transcript\n\n{}\n\n\
## Latest User Message\n\n{}",
        transcript, latest_user_message
    )
}

pub(super) fn trim_resume_transcript(transcript: &str, max_chars: usize) -> String {
    if transcript.chars().count() <= max_chars {
        return transcript.to_string();
    }

    let tail: String = transcript
        .chars()
        .rev()
        .take(max_chars)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    format!("[Earlier transcript omitted]\n\n{}", tail)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn thread_start_params_send_base_and_developer_instructions_separately() {
        let params = build_thread_start_params(
            "/tmp/worktree",
            "on-request",
            "workspace-write",
            Some("gpt-test"),
            Some("medium"),
            Some("priority"),
            "BASE",
            Some("agent content"),
        );

        assert_eq!(params["baseInstructions"], "BASE");
        assert_eq!(params["developerInstructions"], "agent content");
        assert_eq!(params["serviceTier"], "priority");
    }

    #[test]
    fn thread_resume_params_send_base_and_developer_instructions_separately() {
        let params = build_thread_resume_params(
            "thread-1",
            "/tmp/worktree",
            "on-request",
            "workspace-write",
            Some("gpt-test"),
            Some("medium"),
            None,
            "BASE",
            Some("agent content"),
        );

        assert_eq!(params["baseInstructions"], "BASE");
        assert_eq!(params["developerInstructions"], "agent content");
    }

    #[test]
    fn resume_fallback_prompt_includes_prior_context_and_latest_reply() {
        // The fresh-thread fallback must carry prior user and assistant context
        // plus the latest reply, in that order — not just system instructions
        // (CAIRN-2598).
        let transcript = "User: earlier question\nAssistant: earlier answer";
        let prompt = format_resume_fallback_prompt(transcript, "the new reply");
        assert!(prompt.contains("earlier question"));
        assert!(prompt.contains("earlier answer"));
        assert!(prompt.contains("the new reply"));
        assert!(prompt.contains("## Prior Transcript"));
        assert!(prompt.contains("## Latest User Message"));
        // Prior transcript precedes the latest reply.
        assert!(prompt.find("earlier answer").unwrap() < prompt.find("the new reply").unwrap());
    }

    #[test]
    fn trim_resume_transcript_keeps_tail_when_over_limit() {
        let trimmed = trim_resume_transcript("AAAABBBBCCCC", 4);
        assert!(trimmed.starts_with("[Earlier transcript omitted]"));
        assert!(trimmed.contains("CCCC"));
        assert!(!trimmed.contains("AAAA"));
    }

    #[test]
    fn trim_resume_transcript_untouched_within_limit() {
        assert_eq!(trim_resume_transcript("short", 100), "short");
    }

    #[test]
    fn empty_developer_instructions_are_omitted() {
        let developer: Option<&str> = None;
        let params = build_thread_start_params(
            "/tmp/worktree",
            "on-request",
            "workspace-write",
            Some("gpt-test"),
            Some("medium"),
            None,
            "BASE",
            developer,
        );

        assert_eq!(params["baseInstructions"], "BASE");
        assert!(params.get("developerInstructions").is_none());
    }
}
