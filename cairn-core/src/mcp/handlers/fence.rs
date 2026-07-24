//! Worktree fence: the single primitive that gates out-of-worktree file reads,
//! file writes, and shell commands for a fenced agent.
//!
//! All three verb handlers (`read`/`write`/`run`) detect a crossing and call
//! [`raise_fence`]. The fence consults the agent's [`Fence`] policy:
//!
//! - `Allow` — the crossing proceeds (no DB row, no prompt).
//! - `Deny` — the crossing is rejected immediately (headless/noninteractive runs).
//! - `Ask` — a session grant short-circuits to allow; otherwise the request
//!   suspends on the shared [`super::permission::await_permission_decision`]
//!   primitive (durable suspend, no auto-deny) and is answerable via the UI or
//!   the `permissions` resource.
//!
//! Shell `run` crossings are no longer detected by parsing the command string.
//! Each command Cairn spawns on the agent's behalf runs under a kernel
//! filesystem sandbox ([`crate::services::sandbox`]); a blocked operation is
//! reported back as a [`crate::services::sandbox::SandboxDenial`], and the `run`
//! handler turns that authoritative kernel denial into a [`Crossing`] and calls
//! [`raise_fence`]. This replaces the old best-effort `classify_shell_command`
//! string heuristic (which a subshell, `exec`, or env-indirection could evade)
//! with OS enforcement. The `read` and `write` handlers detect crossings by path
//! resolution, on the *same* boundary the `run` sandbox enforces: a `read` is
//! gated only when its path is in the sensitive denylist (reads are otherwise
//! broad, matching `run`); a `write` is gated when its target escapes the
//! worktree. See `docs/worktree-fence.md`.

use std::path::Path;

use crate::mcp::types::McpCallbackRequest;
use crate::models::Fence;
use crate::orchestrator::Orchestrator;

use super::permission::{await_permission_decision, resolve_fence_policy, PermissionWait};

/// Resolve the canonical run and its fence policy for a verb request, looking
/// the run up by id or, failing that, by cwd. Returns `None` when there is no
/// active run or the cwd is unknown — no fence applies.
pub(crate) async fn resolve_run_fence(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
) -> Option<(String, Fence)> {
    let run_id = match request.run_id.clone() {
        Some(id) => id,
        None => {
            super::run_context::lookup_run(&orch.db.local, request)
                .await
                .ok()?
                .run_id
        }
    };
    let fence = resolve_fence_policy(orch, Some(&run_id)).await?;
    Some((run_id, fence))
}

/// What kind of boundary crossing was detected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrossingKind {
    ReadOutsideWorktree,
    WriteOutsideWorktree,
    ShellEscape,
}

impl CrossingKind {
    /// Stable tag stored in the request `tool_input` (so a legacy tool prompt's
    /// `tool_input` never parses as a crossing by accident).
    fn tag(self) -> &'static str {
        match self {
            CrossingKind::ReadOutsideWorktree => "read_outside_worktree",
            CrossingKind::WriteOutsideWorktree => "write_outside_worktree",
            CrossingKind::ShellEscape => "shell_escape",
        }
    }
}

/// A detected boundary crossing awaiting a fence decision.
#[derive(Debug, Clone)]
pub struct Crossing {
    kind: CrossingKind,
    /// The verb that produced it: "read" | "write" | "run".
    verb: &'static str,
    /// Canonical key for session-grant matching (path or normalized command).
    pub descriptor: String,
    /// Human-readable summary for the UI and the deny message.
    summary: String,
}

impl Crossing {
    /// A read of a sensitive denylisted path (credential store, private key).
    /// Reads are otherwise broad; this is the only read the fence gates, kept
    /// consistent with the `run`-verb OS sandbox's read denylist.
    pub fn read_denied(path: &Path) -> Self {
        let descriptor = path.display().to_string();
        Crossing {
            kind: CrossingKind::ReadOutsideWorktree,
            verb: "read",
            summary: format!("read a sensitive denied path: {descriptor}"),
            descriptor,
        }
    }

    pub(crate) fn write_outside(path: &Path) -> Self {
        let descriptor = path.display().to_string();
        Crossing {
            kind: CrossingKind::WriteOutsideWorktree,
            verb: "write",
            summary: format!("write a file outside the worktree: {descriptor}"),
            descriptor,
        }
    }

    /// Shell crossing from an out-of-worktree path token. The descriptor is the
    /// resolved path, so a session grant generalizes across commands touching it
    /// (parity with read/write crossings) rather than keying on the exact
    /// command bytes.
    pub fn shell_path(resolved: &Path, token: &str) -> Self {
        Crossing {
            kind: CrossingKind::ShellEscape,
            verb: "run",
            descriptor: resolved.display().to_string(),
            summary: format!("command references a path outside the worktree: {token}"),
        }
    }

    /// Shell crossing with no path (privilege escalation). The descriptor is the
    /// normalized command.
    pub(crate) fn shell_command(summary: String, command: &str) -> Self {
        Crossing {
            kind: CrossingKind::ShellEscape,
            verb: "run",
            descriptor: normalize_command_for_descriptor(command),
            summary,
        }
    }
}

/// The fence's verdict for a crossing.
#[derive(Debug)]
pub enum FenceDecision {
    /// Proceed with the crossing.
    Allow,
    /// Reject with this reason.
    Deny(String),
    /// The run durably suspended; the verb handler returns a suspend marker and
    /// the run re-drives the verb on resume.
    Suspended,
}

/// Adjudicate a detected crossing under the agent's escape policy.
///
/// `run_id` is the canonical run the verb is executing under (resolved by the
/// caller). `request` is the originating verb request, embedded in the stored
/// `tool_input` so the slow-path resume can re-dispatch it verbatim.
pub async fn raise_fence(
    orch: &Orchestrator,
    run_id: &str,
    fence: Fence,
    request: &McpCallbackRequest,
    crossing: Crossing,
) -> FenceDecision {
    match fence {
        Fence::Allow => FenceDecision::Allow,
        Fence::Deny => FenceDecision::Deny(format!(
            "Denied by agent fence policy (fence: deny): {}",
            crossing.summary
        )),
        Fence::Ask => {
            // A session grant for this descriptor short-circuits to allow.
            if let Ok(allowed) = orch.session_allowed_crossings.lock() {
                if allowed.contains(&crossing.descriptor) {
                    return FenceDecision::Allow;
                }
            }

            // Stable tool_use id so the slow-path resume attaches the synthetic
            // result to the verb call the agent is waiting on.
            let tool_use_id = request
                .tool_use_id
                .clone()
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

            // Embed the originating request (with the resolved run_id) so resume
            // can re-dispatch the exact verb call.
            let mut embedded = request.clone();
            embedded.run_id = Some(run_id.to_string());
            embedded.tool_use_id = Some(tool_use_id.clone());

            let tool_input = serde_json::json!({
                "kind": crossing.kind.tag(),
                "verb": crossing.verb,
                "descriptor": crossing.descriptor,
                "summary": crossing.summary,
                "request": embedded,
            });

            match await_permission_decision(orch, run_id, &tool_use_id, crossing.verb, &tool_input)
                .await
            {
                PermissionWait::Decided(response) => {
                    if response_is_allow(&response) {
                        FenceDecision::Allow
                    } else {
                        FenceDecision::Deny(format!(
                            "Denied by worktree fence: {}",
                            crossing.summary
                        ))
                    }
                }
                PermissionWait::Suspended => FenceDecision::Suspended,
            }
        }
    }
}

fn response_is_allow(response_json: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(response_json)
        .ok()
        .and_then(|value| {
            value
                .get("behavior")
                .and_then(|b| b.as_str())
                .map(|b| b == "allow")
        })
        .unwrap_or(false)
}

/// Collapse a command to a stable descriptor for session-grant matching.
fn normalize_command_for_descriptor(command: &str) -> String {
    super::normalize_command(command)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn shell_path_crossing_keys_on_resolved_path() {
        let c = Crossing::shell_path(Path::new("/etc/hosts"), "/etc/hosts");
        assert_eq!(c.kind, CrossingKind::ShellEscape);
        assert_eq!(c.verb, "run");
        assert_eq!(c.descriptor, "/etc/hosts");
    }

    #[test]
    fn shell_command_crossing_normalizes_descriptor() {
        let c = Crossing::shell_command("blocked".to_string(), "sudo   rm  -rf /");
        assert_eq!(c.kind, CrossingKind::ShellEscape);
        assert_eq!(
            c.descriptor,
            normalize_command_for_descriptor("sudo rm -rf /")
        );
    }

    #[test]
    fn read_and_write_crossings_describe_paths() {
        assert_eq!(Crossing::read_denied(Path::new("/x")).descriptor, "/x");
        assert_eq!(Crossing::write_outside(Path::new("/y")).descriptor, "/y");
    }
}
