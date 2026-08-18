//! MCP request handlers organized by domain.
//!
//! Framework-agnostic handler logic. Both Tauri and cairn-server dispatch to these.

pub mod authority;
pub(crate) mod branch;
pub mod bug_report;
pub(crate) mod check_run;
pub mod comments_artifacts;
pub(crate) mod durable_images;
pub(crate) mod durable_suspend;
pub mod executions;
pub mod fence;
pub mod fetch_web;
pub mod issue_resources;
pub mod issues;
pub(crate) mod mcp_continuation;
pub mod mcp_resources;
pub mod messages;
pub(crate) mod owned_wait;
pub mod pdf;
pub mod permission;
pub mod planning;
pub mod read;
pub mod repl;
pub mod resources;
pub mod run;
pub(crate) mod run_context;
pub mod search;
pub(crate) mod search_translate;
pub mod search_web;
pub mod skills_resources;
pub mod slug;
pub(crate) mod target;
pub mod terminal;
pub(crate) mod tool_use_correlation;
pub mod watch;
pub mod web;
pub mod workflows;
pub mod write;

/// Payload for `agent-attention` events.
pub(crate) struct AttentionEvent<'a> {
    pub(crate) attention_type: &'a str,
    pub(crate) project_key: &'a str,
    /// Canonical home URI for the job that needs attention.
    pub(crate) home_uri: Option<&'a str>,
    pub(crate) tool_name: Option<&'a str>,
}

/// Emit an `agent-attention` event to notify the frontend that user attention is needed.
///
/// Used for: ask_user prompts, permission requests, job completed/failed.
pub(crate) fn emit_attention(emitter: &dyn crate::services::EventEmitter, event: &AttentionEvent) {
    let _ = emitter.emit(
        "agent-attention",
        serde_json::json!({
            "type": event.attention_type,
            "projectKey": event.project_key,
            "homeUri": event.home_uri,
            "toolName": event.tool_name,
        }),
    );
}

/// Authenticated identity and project coordinate for one agent run.
#[derive(Clone)]
pub struct RunContext {
    pub run_id: String,
    pub job_id: String,
    pub exec_seq: Option<i32>, // Monotonic execution sequence number (for URIs)
    pub issue_id: Option<String>, // Null for project-level runs
    pub issue_number: Option<i32>, // Issue number for building issue keys (e.g., 123 for CAIRN-123)
    pub project_id: String,
    pub project_key: String,
    pub job_name: Option<String>, // Human-readable job name from execution snapshot (e.g., "builder-1")
    pub agent_config_id: Option<String>, // "workflow" identifies harness-backed workflow runs
}

impl RunContext {
    /// Get the issue key (e.g., "CAIRN-123") or None for project-level runs
    #[allow(dead_code)]
    pub fn issue_key(&self) -> Option<String> {
        self.issue_number
            .map(|num| format!("{}/{}", self.project_key, num))
    }
}

/// Minimal project context for external tools (no active run required)
#[derive(Debug)]
pub struct ProjectContext {
    project_id: String,
    project_key: String,
}

fn parse_payload<T>(request: &crate::mcp::types::McpCallbackRequest) -> Result<T, String>
where
    T: serde::de::DeserializeOwned,
{
    serde_json::from_value(request.payload.clone()).map_err(|e| format!("Invalid payload: {e}"))
}

/// Strip a shell launcher wrapper and return the semantic inner command when possible.
pub(crate) fn unwrap_shell_launcher(cmd: &str) -> String {
    let trimmed = cmd.trim();
    let prefixes = [
        "/bin/zsh -lc ",
        "/bin/bash -lc ",
        "/bin/sh -lc ",
        "zsh -lc ",
        "bash -lc ",
        "sh -lc ",
        "/bin/zsh -c ",
        "/bin/bash -c ",
        "/bin/sh -c ",
        "zsh -c ",
        "bash -c ",
        "sh -c ",
    ];

    for prefix in prefixes {
        if let Some(rest) = trimmed.strip_prefix(prefix) {
            let rest = rest.trim();
            if rest.len() >= 2 {
                if let Some(inner) = rest
                    .strip_prefix('"')
                    .and_then(|s| s.strip_suffix('"'))
                    .map(|s| {
                        let mut out = String::with_capacity(s.len());
                        let mut chars = s.chars();
                        while let Some(ch) = chars.next() {
                            if ch == '\\' {
                                if let Some(next) = chars.next() {
                                    match next {
                                        '\\' | '"' | '$' | '`' => out.push(next),
                                        '\n' => {}
                                        other => {
                                            out.push('\\');
                                            out.push(other);
                                        }
                                    }
                                } else {
                                    out.push('\\');
                                }
                            } else {
                                out.push(ch);
                            }
                        }
                        out
                    })
                {
                    return inner;
                }

                if let Some(inner) = rest
                    .strip_prefix('\'')
                    .and_then(|s| s.strip_suffix('\''))
                    .map(ToOwned::to_owned)
                {
                    return inner;
                }
            }

            return rest.to_string();
        }
    }

    trimmed.to_string()
}

/// Normalize command for matching: strip shell launchers, trim, collapse whitespace.
pub(crate) fn normalize_command(cmd: &str) -> String {
    unwrap_shell_launcher(cmd)
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// The agent-facing report for a commit that landed on the branch but did not
/// finish publishing.
///
/// One message for both commit verbs, because a `run` carrying a `commit_msg` and
/// a file-touching `write` climb the same publication ladder, so an agent hitting
/// this needs the same answer from either. They used to disagree: `write` said not
/// to re-send, while `run` said to "retry a batch carrying the same commit_msg to
/// republish" — which does not work, and which the `git-workflow` skill inherited
/// and taught.
///
/// A retry is wrong for two independent reasons. The commit is already on the
/// branch, so a re-run that reproduces the same content has no working-tree delta
/// to seal and therefore no publication path at all; and a byte-identical `write`
/// redelivery is answered from the replay ledger with this same finalized failure
/// (see [`write::replay`]). What does publish it is *any* later successful commit,
/// because publication moves the branch ref to the branch's current head, which
/// already contains this commit. So the recovery is to carry on rather than to
/// manufacture another commit, and a publication that keeps failing is a defect to
/// surface rather than to out-wait.
///
/// The failure is deliberately not attributed to one rung. `sealed locally;
/// unpublished` covers the jj-to-git export and the origin push alike, so the ref
/// outside the store may already be correct while only origin is stale.
pub(crate) fn unpublished_commit_message(sha: &str, error: &str) -> String {
    format!(
        "Commit {sha} was sealed locally but remains unpublished: {error}. The commit is on your \
         branch in Cairn's store; what did not complete is a later rung of publication — the \
         branch ref outside the store, the push to origin, or both — so a reviewer, a \
         coordinator, a child branch cut from yours, or a `git rev-parse` may still see the \
         previous tip. Do not re-send this call; it already applied. Your next successful commit \
         publishes the branch at its current head and carries this commit with it. If publication \
         keeps failing, surface it rather than making another commit to force it."
    )
}

#[cfg(test)]
mod publication_failure_wording {
    use super::unpublished_commit_message;

    /// Both commit verbs share this message, and it must never prescribe a retry:
    /// the commit is already on the branch, so a reproduced batch has no delta to
    /// seal, and a redelivered `write` replays this same failure. The `run` verb
    /// once said "retry a batch carrying the same commit_msg to republish", which
    /// stranded branches while promising a recovery that could not fire.
    #[test]
    fn unpublished_guidance_never_prescribes_a_retry() {
        let message = unpublished_commit_message("abc123", "origin push rejected");
        assert!(message.contains("abc123"), "{message}");
        assert!(message.contains("origin push rejected"), "{message}");
        let lower = message.to_ascii_lowercase();
        assert!(!lower.contains("retry"), "{message}");
        assert!(!lower.contains("republish"), "{message}");
        assert!(!lower.contains("re-send this write"), "{message}");
        // The mechanism that does publish it, and the escalation when it will not.
        assert!(lower.contains("next successful commit"), "{message}");
        assert!(lower.contains("surface it"), "{message}");
    }

    /// The status covers the export and the push alike, so the message must not
    /// claim only the ref outside the store failed: origin alone can be stale.
    #[test]
    fn unpublished_guidance_attributes_the_failure_to_either_later_rung() {
        let message = unpublished_commit_message("abc123", "export failed").to_ascii_lowercase();
        assert!(
            message.contains("branch ref outside the store"),
            "{message}"
        );
        assert!(message.contains("origin"), "{message}");
        assert!(message.contains("or both"), "{message}");
    }
}

#[cfg(test)]
mod suspension_markers {
    use super::{
        owned_wait::WAIT_SUSPENDED_MARKER,
        planning::{PROMPT_SUSPENDED_MARKER, PROMPT_SUSPENDED_OWNED_LOOP_MARKER},
    };
    use crate::{
        execution::delegation::runtime::{
            DELEGATED_TASKS_SUSPENDED_PARENT_SUFFIX, DELEGATED_TASKS_SUSPENDED_SUFFIX,
        },
        mcp::handlers::run::{RUN_BATCH_SUSPENDED_MARKER, RUN_ITEM_SUSPENDED_MARKER},
        orchestrator::lifecycle::USER_STOP_TOOL_RESULT,
    };

    /// Every hand-off marker a suspension can put in front of an agent, so a new
    /// suspension client cannot introduce one that reads like a refusal. The list
    /// is also what the transcript mirrors in `suspensionHandoff.ts`; its parity
    /// test reads these same constants out of the source.
    const HANDOFF_MARKERS: &[&str] = &[
        WAIT_SUSPENDED_MARKER,
        PROMPT_SUSPENDED_MARKER,
        PROMPT_SUSPENDED_OWNED_LOOP_MARKER,
        RUN_BATCH_SUSPENDED_MARKER,
        RUN_ITEM_SUSPENDED_MARKER,
        DELEGATED_TASKS_SUSPENDED_SUFFIX,
        DELEGATED_TASKS_SUSPENDED_PARENT_SUFFIX,
    ];

    /// The vocabulary of a refusal. A suspension marker carrying any of it hands
    /// the agent the CLI's own "the user doesn't want to proceed" reading of a
    /// routine pause, which agents have acted on (CAIRN-3162).
    ///
    /// Naming the user is NOT refusal framing, which is why the word itself is not
    /// banned: a blocking question's marker says the run resumes when the user
    /// answers, and that is exactly the fact the agent needs.
    const REFUSAL_VOCABULARY: &[&str] = &[
        "reject",
        "declin",
        "interrupt",
        "does not want",
        "doesn't want",
        "cancel",
        "denied",
    ];

    #[test]
    fn cairn_suspensions_are_not_framed_as_user_rejections() {
        for marker in HANDOFF_MARKERS {
            let marker = marker.to_ascii_lowercase();
            assert!(marker.contains("suspend"), "{marker}");
            for refusal in REFUSAL_VOCABULARY {
                assert!(!marker.contains(refusal), "{marker} contains {refusal}");
            }
        }
    }

    /// Every marker also says what happens next, so a parked agent is never left
    /// to infer whether anything is still coming.
    #[test]
    fn every_suspension_marker_says_the_call_continues() {
        for marker in HANDOFF_MARKERS {
            let marker = marker.to_ascii_lowercase();
            assert!(
                marker.contains("resume") || marker.contains("pending"),
                "{marker}"
            );
        }
    }

    #[test]
    fn genuine_user_stop_remains_explicit_and_distinct() {
        let result = USER_STOP_TOOL_RESULT.to_ascii_lowercase();
        assert!(result.contains("user stop"));
        assert!(result.contains("interrupt"));
        for marker in HANDOFF_MARKERS {
            assert_ne!(&USER_STOP_TOOL_RESULT, marker);
        }
    }
}
