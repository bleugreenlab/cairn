//! Shared run-handler data types: batch payload shapes, the resolved-item spec,
//! per-item outcomes, the promoted-terminal marker, and streaming payloads.

use crate::config::mcp_servers::McpServerConfig;
use cairn_common::read::ImageBlock;
use serde::{Deserialize, Serialize};

/// Payload for run tool from MCP server.
///
/// `run` is an ordered batch of invocations. Each item is either a shell
/// command (`command`) or a `cairn://` skill-script target (`target`).
/// Items run in parallel by default; `sequential` opts into ordered execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunPayload {
    pub commands: Vec<RunItem>,
    /// Run items in input order instead of concurrently. Default false.
    #[serde(default)]
    pub sequential: Option<bool>,
    /// In sequential mode, abort remaining items after a failure. Default true.
    #[serde(default)]
    pub stop_on_error: Option<bool>,
    /// Commit all worktree changes once after the whole batch succeeds. Required
    /// when a successful worktree-bound run dirties the worktree; `^` amends.
    /// Without it, a run that dirties the worktree is restored to HEAD (see
    /// `run_commit_barrier`).
    #[serde(default)]
    pub commit_msg: Option<String>,
    /// Run the batch in the live checkout holding this branch/ref. Branch-scoped
    /// runs never commit, refuse initially dirty checkouts, and warn if tracked
    /// changes appear during the run.
    #[serde(default)]
    pub branch: Option<String>,
}

/// A single run invocation: exactly one of `command` (shell) or `target`
/// (a `cairn://skills/<id>/scripts/<name>` URI).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunItem {
    #[serde(default)]
    pub command: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub timeout: Option<u32>,
    #[serde(default)]
    pub target: Option<String>,
    #[serde(default)]
    pub payload: Option<RunItemPayload>,
}

/// Structured args for a run item's target.
///
/// `args` carries positional arguments for a skill-script target. `args_json`
/// carries the named-argument object for an MCP tool call
/// (`cairn://mcp/<server>/<tool>`), passed through to the server's `tools/call`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RunItemPayload {
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub args_json: Option<serde_json::Value>,
}

/// A resolved, ready-to-execute run item.
pub(super) enum RunSpec {
    /// A shell command run through `bash -c` (or `cmd /c` on Windows).
    Shell {
        command: String,
        timeout: Option<u32>,
    },
    /// A resolved skill script: interpreter program + full argv (script path
    /// included), executed directly (not through a shell).
    Script {
        program: String,
        args: Vec<String>,
        timeout: Option<u32>,
    },
    /// A proxied external MCP `tools/call` (`cairn://mcp/<server>/<tool>`).
    /// Not a process exec — dispatched through the host `McpGateway`. Boxed
    /// because its config payload is far larger than the other variants.
    McpCall(Box<McpCallSpec>),
}

/// Resolved payload for a proxied MCP `tools/call`.
pub(super) struct McpCallSpec {
    pub(super) credential_key: String,
    pub(super) tool: String,
    pub(super) args: serde_json::Value,
    pub(super) config: McpServerConfig,
    pub(super) timeout: Option<u32>,
}

/// Per-item execution result used to compose the batch output.
pub(super) struct ItemOutcome {
    /// Header shown above this item's output (`=== <header> ===`): the command
    /// or the skill-script target URI.
    pub(super) header: String,
    pub(super) body: String,
    pub(super) succeeded: bool,
    /// The item durably suspended on a worktree-fence approval; the whole batch
    /// re-runs on resume. Carried so `handle_run` can surface the suspend marker.
    pub(super) suspended: bool,
    /// Image content blocks this item produced. Only an external MCP `tools/call`
    /// returns them today; collected across the batch into the run envelope so the
    /// transport edge delivers them as real image content blocks (read-path mirror).
    pub(super) images: Vec<ImageBlock>,
}

impl ItemOutcome {
    /// A failed item carrying `header` and `body`, with no image blocks — the
    /// common shape for every error / denial / spawn-failure path.
    pub(super) fn failed(header: String, body: impl Into<String>) -> Self {
        Self {
            header,
            body: body.into(),
            succeeded: false,
            suspended: false,
            images: Vec::new(),
        }
    }
}

/// A timed-out `run` item that was promoted to a durable terminal session.
pub(crate) struct PromotedTerminal {
    /// The terminal's auto-allocated slug (`run-<n>`).
    #[allow(dead_code)]
    pub(crate) slug: String,
    /// Canonical terminal URI the agent can read and kill.
    pub(crate) uri: String,
    /// Whether the current job was subscribed to the terminal exit wake during
    /// promotion. Kept separate from promotion success so a wake persistence
    /// failure never strands the still-running process behind an unreported URI.
    pub(crate) wake_subscribed: bool,
}

/// Event payload for real-time run output streaming.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunOutputPayload {
    pub run_id: String,
    pub tool_use_id: String,
    pub chunk: String,
    pub stream: String, // "stdout" or "stderr"
}

/// Event payload for run command completion.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunCompletePayload {
    pub run_id: String,
    pub tool_use_id: String,
    pub exit_code: Option<i32>,
    pub timed_out: bool,
}

/// Event payload for the live `when:write` check status line: a full snapshot of
/// the planned checklist, re-emitted on every state transition. Snapshots (not
/// deltas) keep the frontend stateless and self-healing — the latest snapshot for
/// a `tool_use_id` wins, and a missed event never leaves the line inconsistent.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckStatusPayload {
    pub run_id: String,
    /// The committing call id (base, no `:check-` suffix). The frontend derives
    /// each check's live-tail stream key as `"{toolUseId}:check-{index}"`.
    pub tool_use_id: String,
    pub checks: Vec<CheckStatusEntry>,
}

/// One check's live status within a [`CheckStatusPayload`] snapshot.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckStatusEntry {
    /// Stable index; the frontend derives `"{toolUseId}:check-{index}"` from it to
    /// find this check's live output tail.
    pub index: usize,
    pub name: String,
    /// `"pending"` | `"running"` | `"passed"` | `"failed"`.
    pub state: String,
    /// The same annotation vocabulary as the final tool-result summary
    /// (`summary_annotation`): `"12 tests"` | `"4.1s"` | `"cached"` |
    /// `"2 of 40 failed, exit 101"` … `None` while pending or running.
    pub annotation: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_payload_deserializes_batch_shape() {
        let payload: RunPayload = serde_json::from_value(serde_json::json!({
            "commands": [
                { "command": "echo hi", "description": "greet" },
                { "target": "cairn://skills/ui/scripts/check.sh", "payload": { "args": ["--fast"] } }
            ],
            "sequential": true,
            "commit_msg": "done"
        }))
        .unwrap();
        assert_eq!(payload.commands.len(), 2);
        assert_eq!(payload.sequential, Some(true));
        assert_eq!(payload.commit_msg.as_deref(), Some("done"));
        assert_eq!(payload.commands[0].command.as_deref(), Some("echo hi"));
        assert_eq!(
            payload.commands[1].target.as_deref(),
            Some("cairn://skills/ui/scripts/check.sh")
        );
        assert_eq!(
            payload.commands[1].payload.as_ref().unwrap().args,
            vec!["--fast".to_string()]
        );
    }
}
