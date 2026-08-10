//! Logical namespace fence: the single primitive that gates sensitive host reads,
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
//! logical project namespace. See `docs/worktree-fence.md`.

use std::path::Path;

use crate::mcp::types::McpCallbackRequest;
use crate::models::Fence;
use crate::orchestrator::Orchestrator;

use super::permission::{await_permission_decision, resolve_fence_policy, PermissionWait};

/// Resolve the canonical run and its fence policy for a verb request, looking
/// the run up exclusively by its authenticated id. Returns `None` when the
/// request has no resolvable run identity.
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
pub(crate) enum CrossingKind {
    SensitiveHostRead,
    ExternalHostWrite,
    /// A shell command referencing a resolved path outside the projection.
    ShellPathCrossing,
    /// A shell command the kernel blocked without reporting which path it
    /// touched. Separated from [`CrossingKind::ShellPathCrossing`] because
    /// allowing it re-executes with no sandbox rather than widening a named
    /// path.
    ShellCommandEscape,
}

impl CrossingKind {
    /// Stable tag stored in the request `tool_input` (so a legacy tool prompt's
    /// `tool_input` never parses as a crossing by accident).
    pub(crate) fn tag(self) -> &'static str {
        match self {
            CrossingKind::SensitiveHostRead => "sensitive_host_read",
            CrossingKind::ExternalHostWrite => "external_host_write",
            CrossingKind::ShellPathCrossing => "shell_path_crossing",
            CrossingKind::ShellCommandEscape => "shell_command_escape",
        }
    }

    /// Every kind, so a tag can be resolved back to one. A new variant that is
    /// not listed here fails [`every_kind_is_registered`], which is the test
    /// that keeps [`tag_is_path_scoped`] from silently defaulting it.
    const ALL: [CrossingKind; 4] = [
        CrossingKind::SensitiveHostRead,
        CrossingKind::ExternalHostWrite,
        CrossingKind::ShellPathCrossing,
        CrossingKind::ShellCommandEscape,
    ];

    /// Whether this kind's descriptor is a resolved host path, and therefore
    /// whether allowing it widens one named path rather than removing the
    /// sandbox.
    ///
    /// Exhaustive on purpose: adding a variant is a compile error here, which
    /// forces the question "does allowing this remove containment?" to be
    /// answered rather than inherited.
    fn is_path_scoped(self) -> bool {
        match self {
            CrossingKind::SensitiveHostRead
            | CrossingKind::ExternalHostWrite
            | CrossingKind::ShellPathCrossing => true,
            CrossingKind::ShellCommandEscape => false,
        }
    }
}

/// Whether a **stored** crossing tag is one of the known path-scoped kinds.
///
/// Deliberately a positive test against a closed set, so an unrecognized tag is
/// treated as an escape. That covers two cases that would otherwise fail open,
/// in the one direction a security gate may fail:
///
/// - a row written by a build BEFORE the path/command split, which tagged both
///   shell crossings `shell_escape`. Pending prompts are durable and nothing
///   expires them, so an agent suspended on a command crossing across an
///   upgrade would otherwise resume able to self-approve its own escape. A
///   descriptor-shape heuristic was considered and rejected: a normalized
///   command can begin with `/` (`/bin/cat /etc/shadow`), so it would read as a
///   path in exactly the case that matters. This over-restricts a legacy
///   path-scoped shell crossing to an operator for the life of that one pending
///   row, which is the acceptable direction;
/// - a kind added later whose author did not think about this gate.
pub(crate) fn tag_is_path_scoped(tag: &str) -> bool {
    CrossingKind::ALL
        .iter()
        .any(|kind| kind.tag() == tag && kind.is_path_scoped())
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
            kind: CrossingKind::SensitiveHostRead,
            verb: "read",
            summary: format!("read a sensitive denied path: {descriptor}"),
            descriptor,
        }
    }

    pub(crate) fn write_outside(path: &Path) -> Self {
        let descriptor = path.display().to_string();
        Crossing {
            kind: CrossingKind::ExternalHostWrite,
            verb: "write",
            summary: format!("write an external host path: {descriptor}"),
            descriptor,
        }
    }

    /// Shell crossing from a path token outside the admitted executor projection. The descriptor is the
    /// resolved path, so a session grant generalizes across commands touching it
    /// (parity with read/write crossings) rather than keying on the exact
    /// command bytes.
    pub fn shell_path(resolved: &Path, token: &str) -> Self {
        Crossing {
            kind: CrossingKind::ShellPathCrossing,
            verb: "run",
            descriptor: resolved.display().to_string(),
            summary: format!("command references an external host path: {token}"),
        }
    }

    /// Shell crossing with no recovered path. The descriptor is the normalized
    /// command.
    ///
    /// Its own kind, because allowing it is a different act from allowing the
    /// others. They widen one named path and the sandbox is still constructed;
    /// this one re-executes an agent-authored command with **no sandbox at
    /// all**, because there is no path to widen. That distinction has to be
    /// visible in the stored prompt rather than inferred later from a
    /// descriptor's shape, so the resolver can require an operator for it.
    pub(crate) fn shell_command(summary: String, command: &str) -> Self {
        Crossing {
            kind: CrossingKind::ShellCommandEscape,
            verb: "run",
            descriptor: normalize_command_for_descriptor(command),
            summary,
        }
    }

    /// The exact `tool_input` [`raise_fence`] would store for this crossing.
    ///
    /// Exists so a resolution test can seed a REAL stored prompt rather than a
    /// hand-written JSON blob that could drift from what the fence actually
    /// writes — which is how a test comes to assert something about a shape
    /// that no longer occurs.
    #[cfg(test)]
    pub(crate) fn stored_tool_input_for_test(&self) -> String {
        serde_json::json!({
            "kind": self.kind.tag(),
            "verb": self.verb,
            "descriptor": self.descriptor,
            "summary": self.summary,
            "request": McpCallbackRequest {
                thread_id: None,
                cwd: "/wt".to_string(),
                run_id: Some("run-1".to_string()),
                tool: self.verb.to_string(),
                tool_use_id: Some("tool-1".to_string()),
                payload: serde_json::json!({}),
            },
        })
        .to_string()
    }

    /// The host path this crossing names, or `None` for a command-scoped one.
    fn host_path(&self) -> Option<&Path> {
        self.kind
            .is_path_scoped()
            .then(|| Path::new(self.descriptor.as_str()))
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
    // A crossing that names a protected host path is refused outright, ahead of
    // the fence policy and ahead of the session-grant short circuit.
    //
    // This lives here rather than at a caller because there are two structurally
    // identical adjudication sites -- the in-runner one in `run/process.rs` and
    // the executor-relayed one in `fleet/mod.rs` -- and a rule attached to
    // either can be missed by the other, or by a third added later. Both reach
    // this function, so this is the invariant's real home.
    //
    // It does not consult `fence`, for the same reason
    // `authorization::protected` does not: containment decides whether a process
    // may cross a filesystem boundary, while this decides whether the
    // workspace's own capability set -- or the credential that approves changes
    // to it -- may be reached at all.
    if let Some(path) = crossing.host_path() {
        if let Some(refusal) =
            crate::authorization::protected::denied_path_refusal(&orch.config_dir, path)
        {
            return FenceDecision::Deny(refusal.to_string());
        }
    }

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
                            "Denied by logical namespace fence: {}",
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

    /// `tag_is_path_scoped` resolves a stored tag through `ALL`, so a variant
    /// missing from that list would silently be treated as an escape. Cheap to
    /// state, and the alternative is a gate that quietly over-restricts.
    #[test]
    fn every_kind_is_registered() {
        for kind in [
            CrossingKind::SensitiveHostRead,
            CrossingKind::ExternalHostWrite,
            CrossingKind::ShellPathCrossing,
            CrossingKind::ShellCommandEscape,
        ] {
            assert!(
                CrossingKind::ALL.contains(&kind),
                "{kind:?} is missing from CrossingKind::ALL"
            );
            assert_eq!(
                tag_is_path_scoped(kind.tag()),
                kind.is_path_scoped(),
                "{kind:?} resolves to a different answer through its stored tag"
            );
        }
    }

    /// The gate fails closed on a tag it does not recognize — a row written
    /// before the path/command kinds were split, and anything a later change
    /// adds without considering it.
    #[test]
    fn an_unrecognized_tag_is_not_path_scoped() {
        for legacy in ["shell_escape", "something_added_later", ""] {
            assert!(
                !tag_is_path_scoped(legacy),
                "'{legacy}' must not be treated as a path-scoped crossing"
            );
        }
    }

    #[test]
    fn shell_path_crossing_keys_on_resolved_path() {
        let c = Crossing::shell_path(Path::new("/etc/hosts"), "/etc/hosts");
        assert_eq!(c.kind, CrossingKind::ShellPathCrossing);
        assert_eq!(c.verb, "run");
        assert_eq!(c.descriptor, "/etc/hosts");
    }

    /// A command-scoped escape is its own kind, and that is load-bearing: the
    /// resolver reads the stored kind to decide whether allowing this crossing
    /// requires an operator, because allowing it re-executes with no sandbox
    /// where a path-scoped crossing only widens the path it names.
    #[test]
    fn shell_command_crossing_normalizes_descriptor() {
        let c = Crossing::shell_command("blocked".to_string(), "sudo   rm  -rf /");
        assert_eq!(c.kind, CrossingKind::ShellCommandEscape);
        assert_ne!(
            c.kind,
            Crossing::shell_path(Path::new("/etc/hosts"), "/etc/hosts").kind,
            "a command escape and a path crossing must not share a kind"
        );
        assert!(c.host_path().is_none(), "it names no path to widen");
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
