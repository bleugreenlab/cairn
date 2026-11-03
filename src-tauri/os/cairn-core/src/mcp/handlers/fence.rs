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

use super::permission::{await_permission_decision, fence_policy_for_run, Handoff, PermissionWait};

mod command_refusal;

/// Typed result of resolving the fence attached to a request. Ambient means no
/// authenticated identity was supplied; unavailable means one was supplied but
/// its context or policy could not be recovered.
pub(crate) enum RunFenceResolution {
    Ambient,
    Resolved(String, Fence),
    Unavailable(String),
}

pub(crate) async fn resolve_run_fence(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
) -> RunFenceResolution {
    let Some(run_id) = request.run_id.clone().filter(|id| !id.is_empty()) else {
        return RunFenceResolution::Ambient;
    };
    if let Err(error) = super::run_context::lookup_run_routed(&orch.db, request).await {
        return RunFenceResolution::Unavailable(format!(
            "permission context unavailable: authenticated run is missing or expired ({error})"
        ));
    }
    match fence_policy_for_run(orch, &run_id).await {
        Ok(fence) => RunFenceResolution::Resolved(run_id, fence),
        Err(reason) => RunFenceResolution::Unavailable(format!(
            "permission context unavailable: authenticated run has no resolvable fence policy: {reason}"
        )),
    }
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
    /// The agent's command exactly as written, for every `run` crossing
    /// whether path- or command-scoped. Consulted only for the instructive
    /// refusal, never for grant matching. Kept beside the descriptor because
    /// normalizing collapses the newlines that separate a script's commands.
    command: Option<String>,
    /// The output's own denial line, for a crossing whose path was not
    /// recovered. The agent's code wrote it, so like `command` it is consulted
    /// only for the instructive refusal, never for grant matching.
    reported: Option<String>,
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
            command: None,
            reported: None,
        }
    }

    pub(crate) fn write_outside(path: &Path) -> Self {
        let descriptor = path.display().to_string();
        Crossing {
            kind: CrossingKind::ExternalHostWrite,
            verb: "write",
            summary: format!("write an external host path: {descriptor}"),
            descriptor,
            command: None,
            reported: None,
        }
    }

    /// A kernel denial of `path` inside the item that ran `command`.
    ///
    /// The descriptor is the path, so a session grant generalizes across
    /// commands touching it (parity with read/write crossings) rather than
    /// keying on the exact command bytes. The command is still carried: a
    /// recovered path does not change whether the item's blocked step has an
    /// ordinary in-sandbox route, so the instructive refusal must see it.
    pub fn shell_path(path: &Path, command: &str) -> Self {
        let descriptor = path.display().to_string();
        Crossing {
            kind: CrossingKind::ShellPathCrossing,
            verb: "run",
            summary: format!("command references an external host path: {descriptor}"),
            descriptor,
            command: Some(command.to_string()),
            reported: None,
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
            command: Some(command.to_string()),
            reported: None,
        }
    }

    /// A kernel denial whose path was not recovered, inside the item that ran
    /// `command` (shown to the approver as `label`).
    ///
    /// Says only what is known: something the item ran was denied. The denial is
    /// usually a descendant's (the tool a script or `sh -c` started), so naming
    /// the item as what was blocked would blame a wrapper that started fine.
    /// `reported` is the output's own denial line, quoted for whoever answers;
    /// the command wrote it, so it stays in the summary and never reaches the
    /// descriptor a grant is scoped by. It may still select a refusal (see
    /// [`Crossing::route_refusal`]), because a refusal only narrows.
    pub(crate) fn unattributed_denial(label: &str, command: &str, reported: Option<&str>) -> Self {
        let mut summary = format!(
            "the sandbox denied an access made inside this item, and the denied path could not be recovered: {label}"
        );
        if let Some(line) = reported {
            summary.push_str(&format!("\nthe item's output reports (unverified): {line}"));
        }
        Crossing {
            reported: reported.map(str::to_string),
            ..Self::shell_command(summary, command)
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
            "request": McpCallbackRequest { answer_bound_ms: None,
                thread_id: None,
                cwd: "/wt".to_string(),
                run_id: Some("run-1".to_string()),
                tool: self.verb.to_string(),
                tool_use_id: Some("tool-1".to_string()),
                payload: serde_json::json!({}),
                origin: None,
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

    /// The instructive refusal for a crossing whose blocked step has an
    /// ordinary route inside the sandbox, or `None`.
    ///
    /// The command text is read first, because it can name the exact git
    /// subcommand. A git call made from an inline interpreter or a script is
    /// not in that text, so the denied path is read next. When the sandbox
    /// recovered it, a `run` crossing's descriptor is that path, and a
    /// git-internal one must be refused rather than prompted for, because an
    /// allow would open git's index or refs to direct writes. When it did not,
    /// the output's reported denial line stands in for it. That line is
    /// unverified, which is why it may only choose a refusal: a refusal gives
    /// the agent less than the escape prompt it replaces, so a forged line can
    /// cost its author an answer but never widen access.
    ///
    /// A write under Cairn's scratch root is refused the same way, after the
    /// git refusals: by the path a `run` or `write` crossing names, else by the
    /// reported line. Nothing there is a workspace, and an allow would open a
    /// sibling of the job's own scratch dir that nothing owns or reclaims.
    fn route_refusal(&self) -> Option<String> {
        use crate::services::sandbox::scratch_refusal;
        let written_path = matches!(
            self.kind,
            CrossingKind::ShellPathCrossing | CrossingKind::ExternalHostWrite
        )
        .then(|| self.host_path())
        .flatten();
        self.command
            .as_deref()
            .and_then(command_refusal::denied_command_refusal)
            .or_else(|| {
                matches!(self.kind, CrossingKind::ShellPathCrossing)
                    .then(|| self.host_path())
                    .flatten()
                    .and_then(command_refusal::recovered_git_write_refusal)
            })
            .or_else(|| {
                self.reported
                    .as_deref()
                    .and_then(command_refusal::reported_git_write_refusal)
            })
            .or_else(|| {
                written_path
                    .filter(|path| scratch_refusal::scratch_root_containing(path).is_some())
                    .map(scratch_refusal::scratch_root_refusal)
            })
            .or_else(|| {
                self.reported
                    .as_deref()
                    .and_then(scratch_refusal::reported_scratch_root_write)
                    .map(|path| scratch_refusal::scratch_root_refusal(&path))
            })
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
    /// The prompt boundary could not establish an answerable request. Execution
    /// remains closed, but this is infrastructure failure rather than denial.
    Unavailable(String),
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
    raise_fence_with_handoff(orch, run_id, fence, request, crossing, Handoff::Unwatched).await
}

/// [`raise_fence`] for a crossing relayed from an executor batch, whose
/// callback can close before the inline wait ends. A closed handoff withdraws
/// the request rather than parking the turn.
pub(crate) async fn raise_fence_with_handoff(
    orch: &Orchestrator,
    run_id: &str,
    fence: Fence,
    request: &McpCallbackRequest,
    crossing: Crossing,
    handoff: Handoff,
) -> FenceDecision {
    if let Some(decision) = decide_without_asking(orch, fence, &crossing) {
        return decision;
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

    match await_permission_decision(
        orch,
        run_id,
        &tool_use_id,
        crossing.verb,
        &tool_input,
        handoff,
    )
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
        PermissionWait::Unavailable(error) => FenceDecision::Unavailable(error),
        PermissionWait::Withdrawn(cause) => FenceDecision::Unavailable(format!(
            "the permission for this crossing was withdrawn: {}",
            cause.sentence()
        )),
    }
}

/// The decision a crossing gets without asking anyone: refused, allowed by the
/// fence policy, or allowed by a session grant. `None` means only a prompt can
/// answer it, and nothing has been recorded yet, so a caller that must never
/// ask (a `run` item served as a read) can decline the crossing here instead.
pub(crate) fn decide_without_asking(
    orch: &Orchestrator,
    fence: Fence,
    crossing: &Crossing,
) -> Option<FenceDecision> {
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
            return Some(FenceDecision::Deny(refusal.to_string()));
        }
    }

    // A blocked command whose blocked step has an ordinary route inside the
    // sandbox (a git index or ref write, named by the command, the recovered
    // path, or the output's denial line; see `command_refusal`) is answered with that
    // route instead of a prompt, so no operator is asked a question
    // whose only answers are harmful or futile. Unlike the protected-path
    // refusal above it leaves an `allow` fence alone: an agent trusted to
    // escape is spending nobody's attention, and its command does what it
    // says once unsandboxed.
    match fence {
        Fence::Allow => Some(FenceDecision::Allow),
        Fence::Deny => Some(FenceDecision::Deny(
            crossing.route_refusal().unwrap_or_else(|| {
                format!(
                    "Denied by agent fence policy (fence: deny): {}",
                    crossing.summary
                )
            }),
        )),
        Fence::Ask => {
            if let Some(refusal) = crossing.route_refusal() {
                return Some(FenceDecision::Deny(refusal));
            }
            // A session grant for this descriptor short-circuits to allow.
            if let Ok(allowed) = orch.session_allowed_crossings.lock() {
                if allowed.contains(&crossing.descriptor) {
                    return Some(FenceDecision::Allow);
                }
            }
            None
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
        let c = Crossing::shell_path(Path::new("/etc/hosts"), "cat /etc/hosts");
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
            Crossing::shell_path(Path::new("/etc/hosts"), "cat /etc/hosts").kind,
            "a command escape and a path crossing must not share a kind"
        );
        assert!(c.host_path().is_none(), "it names no path to widen");
        assert_eq!(
            c.descriptor,
            normalize_command_for_descriptor("sudo rm -rf /")
        );
    }

    #[test]
    fn an_unattributed_denial_names_no_blocked_command_and_quotes_output_unverified() {
        let reported = "error: cannot open '/Users/x/.ssh/known_hosts': Permission denied";
        let c = Crossing::unattributed_denial("uv run -", "uv run -", Some(reported));
        assert_eq!(c.kind, CrossingKind::ShellCommandEscape);
        assert!(!c.summary.contains("blocked"), "{}", c.summary);
        assert!(
            c.summary.contains("could not be recovered: uv run -"),
            "{}",
            c.summary
        );
        assert!(
            c.summary.contains(&format!("(unverified): {reported}")),
            "{}",
            c.summary
        );
        // Output text never scopes a grant.
        assert_eq!(c.descriptor, normalize_command_for_descriptor("uv run -"));
        assert!(c.host_path().is_none());

        let bare = Crossing::unattributed_denial("bun -e", "bun -e", None);
        assert!(!bare.summary.contains("unverified"), "{}", bare.summary);
    }

    /// posts/518: an inline `uv run -` script's git was denied its worktree
    /// index lock, the path was not recovered, and the operator got an escape
    /// prompt no answer could help. The output's denial line now selects the
    /// git-writer refusal, and still scopes nothing.
    #[tokio::test]
    async fn an_unattributed_git_internal_write_is_refused_with_its_route_and_scopes_nothing() {
        let reported = "fatal: Unable to create '/home/dev/.cairn-exec/managed-objects/e2e056b6025ecec5f1bbfb1d6fc1026e8a3b75fa93873322d535a125c6742df7/repository.git/worktrees/slot-6/index.lock': Permission denied";
        let crossing = || Crossing::unattributed_denial("uv run -", "uv run -", Some(reported));

        // Nothing a grant could be scoped by comes from the text.
        let c = crossing();
        assert_eq!(c.kind, CrossingKind::ShellCommandEscape);
        assert_eq!(c.descriptor, normalize_command_for_descriptor("uv run -"));
        assert!(c.host_path().is_none());

        let orch = super::super::permission::tests::test_orchestrator().await;
        for fence in [Fence::Ask, Fence::Deny] {
            match raise_fence(&orch, "run-1", fence, &run_request(), crossing()).await {
                FenceDecision::Deny(refusal) => {
                    assert!(
                        refusal.contains("git's staging records or refs"),
                        "{refusal}"
                    );
                    assert!(refusal.contains("unverified"), "{refusal}");
                    assert!(refusal.contains("plain `mv`"), "{refusal}");
                    assert!(
                        refusal.contains("`commit_msg` on the `run` or `write`"),
                        "{refusal}"
                    );
                    assert!(!refusal.contains("slot-6"), "{refusal}");
                }
                other => panic!("{fence:?}: expected the git-writer refusal, got {other:?}"),
            }
        }
        assert_eq!(
            permission_rows(&orch).await,
            0,
            "the refusal must not put an escape prompt on anyone's desk"
        );

        // An agent trusted to escape is left alone, as for a named git writer.
        assert!(matches!(
            raise_fence(&orch, "run-1", Fence::Allow, &run_request(), crossing()).await,
            FenceDecision::Allow
        ));
    }

    /// An unattributed denial whose output names anything other than a git
    /// directory's own records keeps today's escape prompt.
    #[test]
    fn an_unattributed_denial_elsewhere_keeps_the_escape_prompt() {
        for reported in [
            "error: cannot open '/Users/x/.ssh/known_hosts': Permission denied",
            "fatal: Unable to create '/home/dev/project/build/index.lock': Permission denied",
        ] {
            let c = Crossing::unattributed_denial("uv run -", "uv run -", Some(reported));
            assert_eq!(c.kind, CrossingKind::ShellCommandEscape);
            assert_eq!(c.route_refusal(), None, "{reported}");
        }
    }

    async fn permission_rows(orch: &Orchestrator) -> i64 {
        use crate::storage::RowExt;
        orch.db
            .local
            .query_one("SELECT COUNT(*) FROM permission_requests", (), |row| {
                row.i64(0)
            })
            .await
            .unwrap()
    }

    /// The command refusal lives in `raise_fence`, the one function that turns
    /// a crossing into a decision. The in-runner adjudication site in
    /// `run/process.rs` and the executor-relayed one in `fleet/mod.rs` both
    /// reach it, and a third cannot adjudicate without it, so this is where the
    /// "no operator card" property is pinned for all of them (the relayed path
    /// is also exercised end to end in `fleet::handoff_tests`).
    #[tokio::test]
    async fn a_blocked_git_index_write_is_refused_with_its_route_and_raises_no_permission() {
        let orch = super::super::permission::tests::test_orchestrator().await;
        let request = McpCallbackRequest {
            answer_bound_ms: None,
            thread_id: None,
            cwd: "/wt".to_string(),
            run_id: Some("run-1".to_string()),
            tool: "run".to_string(),
            tool_use_id: Some("tool-1".to_string()),
            payload: serde_json::json!({}),
            origin: None,
        };
        let crossing = || {
            Crossing::shell_command(
                "command blocked by the executor sandbox".to_string(),
                "cd src && git mv old.rs new.rs && cargo check",
            )
        };

        for fence in [Fence::Ask, Fence::Deny] {
            match raise_fence(&orch, "run-1", fence, &request, crossing()).await {
                FenceDecision::Deny(refusal) => {
                    assert!(refusal.contains("runs `git mv`"), "{refusal}");
                    assert!(refusal.contains("plain `mv`"), "{refusal}");
                }
                other => panic!("{fence:?}: expected the instructive refusal, got {other:?}"),
            }
        }
        assert_eq!(
            permission_rows(&orch).await,
            0,
            "the refusal must not spend a permission on anyone's desk"
        );

        // An agent trusted to escape is left alone.
        assert!(matches!(
            raise_fence(&orch, "run-1", Fence::Allow, &request, crossing()).await,
            FenceDecision::Allow
        ));
    }

    fn run_request() -> McpCallbackRequest {
        McpCallbackRequest {
            answer_bound_ms: None,
            thread_id: None,
            cwd: "/wt".to_string(),
            run_id: Some("run-1".to_string()),
            tool: "run".to_string(),
            tool_use_id: Some("tool-1".to_string()),
            payload: serde_json::json!({}),
            origin: None,
        }
    }

    /// Once the sandbox recovers the denied path (an index or ref lock in the
    /// cell's shared repository), the crossing is path-scoped, but the item's
    /// blocked step still has its in-sandbox route. The refusal must answer it
    /// rather than an "external host path" prompt that names a git internal.
    #[tokio::test]
    async fn a_git_writer_denied_on_a_recovered_path_is_refused_with_its_route() {
        let orch = super::super::permission::tests::test_orchestrator().await;
        let request = run_request();
        let command = "git mv a b && git status --short";
        for path in [
            "/Users/dev/.cairn-exec/managed-objects/m/repository.git/worktrees/slot-652/index.lock",
            "/Users/dev/.cairn-exec/managed-objects/m/repository.git/refs/heads/main.lock",
        ] {
            let crossing = Crossing::shell_path(Path::new(path), command);
            // What a grant would be scoped by is unchanged: the path.
            assert_eq!(crossing.kind, CrossingKind::ShellPathCrossing);
            assert_eq!(crossing.descriptor, path);

            for fence in [Fence::Ask, Fence::Deny] {
                match raise_fence(&orch, "run-1", fence, &request, crossing.clone()).await {
                    FenceDecision::Deny(refusal) => {
                        assert!(refusal.contains("runs `git mv`"), "{refusal}");
                        assert!(refusal.contains("plain `mv`"), "{refusal}");
                    }
                    other => {
                        panic!("{fence:?} {path}: expected the instructive refusal, got {other:?}")
                    }
                }
            }
        }
        assert_eq!(permission_rows(&orch).await, 0);
    }

    /// An inline interpreter's git call is not in its command text, but on
    /// macOS the sandbox recovers the path it was denied. A path-scoped allow
    /// would hand the item git's index, so the recovered path selects the
    /// git-writer refusal instead, stated as the sandbox's own record.
    #[tokio::test]
    async fn a_recovered_git_internal_path_from_a_non_git_command_is_refused_with_its_route() {
        let orch = super::super::permission::tests::test_orchestrator().await;
        let path =
            "/Users/dev/.cairn-exec/managed-objects/m/repository.git/worktrees/slot-6/index.lock";
        for command in ["uv run -", "./scripts/rename.sh"] {
            let crossing = Crossing::shell_path(Path::new(path), command);
            assert_eq!(crossing.kind, CrossingKind::ShellPathCrossing);

            for fence in [Fence::Ask, Fence::Deny] {
                match raise_fence(&orch, "run-1", fence, &run_request(), crossing.clone()).await {
                    FenceDecision::Deny(refusal) => {
                        assert!(
                            refusal.contains("git's staging records or refs"),
                            "{refusal}"
                        );
                        assert!(refusal.contains("The sandbox recorded"), "{refusal}");
                        assert!(!refusal.contains("unverified"), "{refusal}");
                        assert!(refusal.contains("plain `mv`"), "{refusal}");
                        assert!(
                            refusal.contains("`commit_msg` on the `run` or `write`"),
                            "{refusal}"
                        );
                        assert!(!refusal.contains("slot-6"), "{refusal}");
                    }
                    other => panic!(
                        "{fence:?} {command}: expected the git-writer refusal, got {other:?}"
                    ),
                }
            }
        }
        assert_eq!(
            permission_rows(&orch).await,
            0,
            "no path-scoped crossing may reach anyone's desk"
        );
    }

    /// A recovered path outside a git directory's own records, including a
    /// file merely named `index.lock`, keeps the path-scoped prompt.
    #[test]
    fn a_recovered_path_elsewhere_keeps_the_path_prompt() {
        for path in ["/outside/x", "/home/dev/project/build/index.lock"] {
            let c = Crossing::shell_path(Path::new(path), "uv run -");
            assert_eq!(c.kind, CrossingKind::ShellPathCrossing);
            assert_eq!(c.route_refusal(), None, "{path}");
        }
    }

    /// A path denial whose command has no in-sandbox route still raises the
    /// path-scoped prompt it always did.
    #[tokio::test]
    async fn a_path_denial_for_an_unrecognized_command_still_prompts() {
        let orch = super::super::permission::tests::test_orchestrator().await;
        // The stored prompt references its run.
        orch.db
            .local
            .execute_script(
                "
                INSERT INTO workspaces(id, name, created_at, updated_at) VALUES ('ws','Workspace',1,1);
                INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
                 VALUES ('project-1','ws','Project','prj','/tmp/project',1,1);
                INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
                 VALUES ('issue-1','project-1',1,'Issue','active','active','none',1,1);
                INSERT INTO runs(id, issue_id, project_id, status, created_at, updated_at)
                 VALUES ('run-1','issue-1','project-1','live',1,1);
                "
                .to_string(),
            )
            .await
            .unwrap();
        let task = {
            let orch = orch.clone();
            tokio::spawn(async move {
                raise_fence(
                    &orch,
                    "run-1",
                    Fence::Ask,
                    &run_request(),
                    Crossing::shell_path(Path::new("/outside/x"), "touch /outside/x"),
                )
                .await
            })
        };
        for _ in 0..400 {
            if permission_rows(&orch).await > 0 || task.is_finished() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        if task.is_finished() {
            match task.await.unwrap() {
                FenceDecision::Suspended => {}
                other => panic!("the fence answered without waiting on a prompt: {other:?}"),
            }
        } else {
            task.abort();
        }
        assert_eq!(permission_rows(&orch).await, 1);

        use crate::storage::RowExt;
        let tool_input = orch
            .db
            .local
            .query_one("SELECT tool_input FROM permission_requests", (), |row| {
                row.text(0)
            })
            .await
            .unwrap();
        let stored: serde_json::Value = serde_json::from_str(&tool_input).unwrap();
        assert_eq!(stored["kind"], CrossingKind::ShellPathCrossing.tag());
        assert_eq!(stored["descriptor"], "/outside/x");
    }

    /// A write under Cairn's scratch root, however it reached the fence, is
    /// answered with the scratch refusal under `ask` and `deny`, ahead of any
    /// session grant, and puts nothing on anyone's desk; `allow` is unfenced.
    #[tokio::test]
    async fn a_write_under_the_scratch_root_is_refused_by_name_on_every_surface() {
        use crate::services::sandbox::scratch_refusal::scratch_root_refusal;
        let root = cairn_common::scratch::scratch_root();
        let sibling = root.join("triome-45-measure");
        let reported = format!(
            "mkdir: cannot create directory '{}': Permission denied",
            root.join("x").display()
        );
        type Case = (&'static str, Box<dyn Fn() -> Crossing>, String);
        let crossings: Vec<Case> = vec![
            (
                "recovered run path",
                Box::new({
                    let sibling = sibling.clone();
                    move || Crossing::shell_path(&sibling, "uv run -")
                }),
                scratch_root_refusal(&sibling),
            ),
            (
                "write verb",
                Box::new({
                    let sibling = sibling.join("notes.md");
                    move || Crossing::write_outside(&sibling)
                }),
                scratch_root_refusal(&sibling.join("notes.md")),
            ),
            (
                "reported Linux line",
                Box::new({
                    let reported = reported.clone();
                    move || Crossing::unattributed_denial("sh -c", "mkdir x", Some(&reported))
                }),
                scratch_root_refusal(&root.join("x")),
            ),
        ];

        let orch = super::super::permission::tests::test_orchestrator().await;
        // A grant cannot open the root either: the refusal precedes the check.
        orch.session_allowed_crossings
            .lock()
            .unwrap()
            .insert(sibling.display().to_string());
        for (surface, crossing, expected) in &crossings {
            for fence in [Fence::Ask, Fence::Deny] {
                match raise_fence(&orch, "run-1", fence, &run_request(), crossing()).await {
                    FenceDecision::Deny(refusal) => assert_eq!(&refusal, expected, "{surface}"),
                    other => {
                        panic!("{surface} {fence:?}: expected the scratch refusal, got {other:?}")
                    }
                }
            }
            assert!(matches!(
                raise_fence(&orch, "run-1", Fence::Allow, &run_request(), crossing()).await,
                FenceDecision::Allow
            ));
        }
        assert_eq!(permission_rows(&orch).await, 0);
    }

    /// A remote executor's scratch root is refused by name too: an approval
    /// would rerun the item with no fence, which would make the root
    /// approvable there while it is refused here.
    #[tokio::test]
    async fn a_remote_executors_scratch_root_is_refused_by_name_too() {
        let remote = Path::new("/home/dev/.cairn-executor/scratch/lab");
        let orch = super::super::permission::tests::test_orchestrator().await;
        for fence in [Fence::Ask, Fence::Deny] {
            match raise_fence(
                &orch,
                "run-1",
                fence,
                &run_request(),
                Crossing::shell_path(remote, "mkdir lab"),
            )
            .await
            {
                FenceDecision::Deny(refusal) => {
                    assert!(
                        refusal.contains("(`/home/dev/.cairn-executor/scratch`)"),
                        "{refusal}"
                    );
                }
                other => panic!("{fence:?}: expected the scratch refusal, got {other:?}"),
            }
        }
        assert_eq!(permission_rows(&orch).await, 0);
    }

    /// A git-internal path keeps its own route even beneath the scratch root,
    /// and a path outside the root keeps the path prompt.
    #[test]
    fn the_scratch_refusal_follows_the_git_refusals_and_only_covers_the_root() {
        let root = cairn_common::scratch::scratch_root();
        let git = root.join("lab/repo/.git/refs/heads/main.lock");
        let refusal = Crossing::shell_path(&git, "uv run -")
            .route_refusal()
            .unwrap();
        assert!(
            refusal.contains("git's staging records or refs"),
            "{refusal}"
        );

        let tmpdir = cairn_common::paths::cairn_home().join("tmp/n1/tree/x");
        assert_eq!(
            Crossing::shell_path(&tmpdir, "uv run -").route_refusal(),
            None
        );
        // A read is never a scratch-root write.
        assert_eq!(Crossing::read_denied(&root.join("x")).route_refusal(), None);

        // A project's own `.cairn/` folder is project content: a crossing into
        // another checkout's `.cairn/scratch` keeps today's path prompt. (The
        // node's own worktree is writable, so it never reaches this tier.)
        let project = Path::new("/home/dev/projects/app/.cairn/scratch/x");
        assert_eq!(
            Crossing::shell_path(project, "uv run -").route_refusal(),
            None
        );
        // A dev instance's home is a Cairn home, and the refusal names its root.
        let dev = Path::new("/home/dev/.cairn-dev/scratch/x");
        let refusal = Crossing::shell_path(dev, "uv run -")
            .route_refusal()
            .unwrap();
        assert!(
            refusal.contains("(`/home/dev/.cairn-dev/scratch`)"),
            "{refusal}"
        );
    }

    /// The sentence's own words, that is: the two paths it quotes are wherever
    /// this process's home is, and a test home may sit under a directory with
    /// any name.
    #[test]
    fn the_scratch_refusal_carries_no_substrate_vocabulary() {
        let root = cairn_common::scratch::scratch_root();
        let lab = root.join("lab");
        let refusal = crate::services::sandbox::scratch_refusal::scratch_root_refusal(&lab)
            .replace(&lab.display().to_string(), "<path>")
            .replace(&root.display().to_string(), "<root>");
        crate::system_prompt::assert_no_substrate_vocabulary("scratch-root refusal", &refusal);
    }

    #[test]
    fn read_and_write_crossings_describe_paths() {
        assert_eq!(Crossing::read_denied(Path::new("/x")).descriptor, "/x");
        assert_eq!(Crossing::write_outside(Path::new("/y")).descriptor, "/y");
    }
}
