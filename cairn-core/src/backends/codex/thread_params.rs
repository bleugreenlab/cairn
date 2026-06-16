use super::CODEX_BACKEND_NAME;
use crate::backends::run_state::run_backend_db;
use crate::models::{Fence, Model};
use crate::orchestrator::Orchestrator;
use crate::storage::RowExt;
use serde_json::Value;
use turso::params;

const RESUME_FALLBACK_TRANSCRIPT_CHARS: usize = 24_000;

pub(super) fn build_base_instructions(workspace_instructions: Option<&str>) -> String {
    let mut combined = crate::system_prompt::CODEX_SYSTEM_PROMPT.to_string();
    combined.push_str("\n\n");
    combined.push_str(&crate::system_prompt::cairn_system_prompt());
    if let Some(workspace_content) =
        workspace_instructions.filter(|content| !content.trim().is_empty())
    {
        combined.push_str("\n\n## Workspace Instructions\n\n");
        combined.push_str(workspace_content.trim());
    }
    combined
}

/// The developer-instructions payload sent to Codex. Deliberately NOT trimmed:
/// the persisted `system:prompt` event records this exact content as labeled
/// segments (`assemble_prompt_segments` uses it untrimmed), and archival's
/// verify-then-discard depends on sent and persisted bytes being identical.
/// Trimming here would silently diverge them the moment agent content carries
/// edge whitespace, and the only symptom would be the prompt dedup quietly
/// falling back to whole-event zstd at teardown.
pub(super) fn build_developer_instructions(system_prompt: Option<&str>) -> Option<String> {
    system_prompt
        .filter(|sp| !sp.trim().is_empty())
        .map(str::to_string)
}

pub(super) fn codex_sandbox_mode(fence: Fence) -> &'static str {
    match fence {
        // Ask/deny agents get Codex's workspace-write sandbox; Cairn's verb
        // handlers provide the user-facing fence decision.
        Fence::Ask | Fence::Deny => "workspace-write",
        Fence::Allow => "danger-full-access",
    }
}

pub(super) fn build_thread_start_params(
    cwd: &str,
    approval_policy: &str,
    sandbox_mode: &str,
    model: Option<&str>,
    reasoning_effort: Option<&str>,
    base_instructions: &str,
    developer_instructions: Option<&str>,
) -> Value {
    let mut params = serde_json::json!({
        "model": model.unwrap_or(Model::GPT_5_4_MINI),
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
        base_instructions,
        developer_instructions,
    )
}

pub(super) fn is_missing_rollout_error(err: &str, thread_id: &str) -> bool {
    err.contains("no rollout found for thread id")
        && (thread_id.is_empty() || err.contains(thread_id))
}

pub(super) fn build_resume_fallback_prompt(
    orch: &Orchestrator,
    stale_session_id: &str,
    latest_user_message: &str,
) -> Option<String> {
    let db = orch.db.local.clone();
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

    Some(format!(
        "The previous Codex thread could not be resumed, so you are continuing from a transcript snapshot instead.\n\n\
## Prior Transcript\n\n{}\n\n\
## Latest User Message\n\n{}",
        transcript, latest_user_message
    ))
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
    fn base_instructions_include_codex_base_and_cairn_prompt() {
        let base = build_base_instructions(None);

        assert!(base.contains(crate::system_prompt::CODEX_SYSTEM_PROMPT));
        assert!(base.contains(&crate::system_prompt::cairn_system_prompt()));
        assert!(!base.contains("agent content"));
        assert!(!base.contains("user request"));
    }

    #[test]
    fn base_instructions_include_workspace_instructions() {
        let base = build_base_instructions(Some("workspace doctrine"));

        assert!(base.contains("\n\n## Workspace Instructions\n\nworkspace doctrine"));
        assert!(!base.contains("agent content"));
    }

    #[test]
    fn thread_start_params_send_base_and_developer_instructions_separately() {
        let base = build_base_instructions(None);
        let developer = build_developer_instructions(Some("agent content"));
        let params = build_thread_start_params(
            "/tmp/worktree",
            "on-request",
            "workspace-write",
            Some("gpt-test"),
            Some("medium"),
            &base,
            developer.as_deref(),
        );

        assert_eq!(params["baseInstructions"], base);
        assert_eq!(params["developerInstructions"], "agent content");
    }

    #[test]
    fn thread_resume_params_send_base_and_developer_instructions_separately() {
        let base = build_base_instructions(None);
        let developer = build_developer_instructions(Some("agent content"));
        let params = build_thread_resume_params(
            "thread-1",
            "/tmp/worktree",
            "on-request",
            "workspace-write",
            Some("gpt-test"),
            Some("medium"),
            &base,
            developer.as_deref(),
        );

        assert_eq!(params["baseInstructions"], base);
        assert_eq!(params["developerInstructions"], "agent content");
    }

    #[test]
    fn empty_developer_instructions_are_omitted() {
        let base = build_base_instructions(None);
        let developer = build_developer_instructions(Some("  \n  "));
        let params = build_thread_start_params(
            "/tmp/worktree",
            "on-request",
            "workspace-write",
            Some("gpt-test"),
            Some("medium"),
            &base,
            developer.as_deref(),
        );

        assert_eq!(params["baseInstructions"], base);
        assert!(params.get("developerInstructions").is_none());
    }

    /// Sent bytes must equal persisted bytes: developer instructions pass
    /// through untrimmed so they stay byte-identical to the recorded
    /// `system:prompt` segments, which archival verifies at teardown.
    #[test]
    fn developer_instructions_preserve_edge_whitespace() {
        let developer = build_developer_instructions(Some("\nagent content\n"));
        assert_eq!(developer.as_deref(), Some("\nagent content\n"));
    }
}
