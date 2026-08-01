//! OS sandbox policy construction for a run, including the read-only
//! live-checkout regime and the jj workspace-refresh carveout.

use crate::mcp::handlers::normalize_command;
use crate::models::Fence;
use crate::orchestrator::Orchestrator;
use crate::services::sandbox;

/// Build the OS sandbox policy for a run, or `None` when no confinement applies.
///
/// The fence dial is the only gate on whether a profile exists at all: `allow`,
/// or a spawn with no agent run behind it, is unconfined everywhere. What the
/// dial does not decide is the profile's *shape*, which follows the checkout:
/// - **Executor checkout cwd**: a fenced spawn confines writes to the cell.
/// - **User live-checkout cwd** (triage, project terminals): a fenced spawn keeps
///   the checkout readable but kernel-denies every write into it, and no session
///   grant, declared check, or accepted command re-opens it.
///
fn is_safe_jj_workspace_refresh_status_command(command: &str) -> bool {
    let segments: Vec<&str> = command.split("&&").map(str::trim).collect();
    if segments.len() != 2 {
        return false;
    }

    let refresh: Vec<&str> = segments[0].split_whitespace().collect();
    let status: Vec<&str> = segments[1].split_whitespace().collect();

    matches!(refresh.as_slice(), ["jj", "workspace", "update-stale"])
        && matches!(status.as_slice(), ["jj", "st" | "status"])
}

fn jj_workspace_repo_dir(worktree: &std::path::Path) -> Option<std::path::PathBuf> {
    let pointer_path = worktree.join(".jj").join("repo");
    let pointer = std::fs::read_to_string(&pointer_path).ok()?;
    let pointer = pointer.trim();
    if pointer.is_empty() {
        return None;
    }

    let raw_repo_dir = std::path::Path::new(pointer);
    let repo_dir = if raw_repo_dir.is_absolute() {
        raw_repo_dir.to_path_buf()
    } else {
        pointer_path.parent()?.join(raw_repo_dir)
    };

    Some(repo_dir.canonicalize().unwrap_or(repo_dir))
}

fn apply_safe_jj_workspace_refresh_status_carveout(
    policy: &mut sandbox::SandboxPolicy,
    checkout: &std::path::Path,
    command_for_grant: Option<&str>,
) {
    let Some(command) = command_for_grant else {
        return;
    };
    if !is_safe_jj_workspace_refresh_status_command(command) {
        return;
    }
    let Some(repo_dir) = jj_workspace_repo_dir(checkout) else {
        return;
    };

    // `jj workspace update-stale && jj st` is Cairn's own workspace-refresh
    // probe. Non-colocated jj workspaces keep the repo metadata behind a
    // `.jj/repo` pointer outside the worktree, so allow that exact metadata path
    // without turning the rest of the command into an unconfined run.
    policy.writable_extra.push(repo_dir);
}

/// Which checkout a process is about to run in.
///
/// This is structural, not inferable from the path. A job's execution home is an
/// ordinary detached Git checkout carrying no `.jj` markers — on disk it is
/// indistinguishable from the project's live checkout — yet one is the agent's
/// own writable home and the other is read-only for agents, non-negotiably. A
/// caller that knows which it holds says so; only the ambient host path, whose
/// cwd is either the agent's jj residence or the live checkout, may infer it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum RunCheckout {
    /// The agent's own checkout: its jj residence, or its execution home.
    AgentOwned,
    /// The project's live checkout, read-only for agents.
    ProjectLive,
}

impl RunCheckout {
    /// Classify a cwd that is known to be either the agent's jj residence or the
    /// project's live checkout. An execution home is neither, so a surface that
    /// runs in one must state its class instead of calling this.
    pub(crate) fn infer(cwd: &str) -> Self {
        if crate::jj::is_jj_dir(std::path::Path::new(cwd)) {
            Self::AgentOwned
        } else {
            Self::ProjectLive
        }
    }

    pub(crate) fn is_project_live(self) -> bool {
        matches!(self, Self::ProjectLive)
    }
}

/// Build the OS sandbox policy for a run, or `None` when no confinement applies.
pub(crate) async fn build_run_sandbox_policy(
    orch: &Orchestrator,
    cwd: &str,
    checkout_kind: RunCheckout,
    run_id: Option<&str>,
    project_id: Option<&str>,
    command_for_grant: Option<&str>,
) -> Option<(sandbox::SandboxPolicy, Fence)> {
    use crate::mcp::handlers::permission::resolve_fence_policy;

    // One gate, every surface: the agent's dial decides whether Cairn applies any
    // policy at all. `allow` means nothing of Cairn's blocks the spawn, including
    // on the project's live checkout — the operator runs every agent at `allow`
    // deliberately, and a confinement the dial cannot switch off is the dial
    // lying. A spawn with no resolvable run identity is nobody's agent operation
    // (an operator's own project terminal), so there is likewise nothing to fence.
    let fence = resolve_fence_policy(orch, run_id).await?;
    if !sandbox::sandbox_applies(fence) {
        return None;
    }

    // The checkout decides the fenced profile's shape. The live checkout is
    // read-only for a fenced agent and non-grantable with it: run_one routes a
    // denial there to a plain explanation rather than a fence prompt, since no
    // grant can publish through someone else's working tree.
    let readonly_non_worktree = checkout_kind.is_project_live();

    if !sandbox::is_available() {
        log::warn!("OS sandbox unavailable on this host; running command unconfined (cwd={cwd})");
        return None;
    }

    let granted: Vec<String> = orch
        .session_allowed_crossings
        .lock()
        .ok()
        .map(|s| s.iter().cloned().collect())
        .unwrap_or_default();

    // A command-scoped session grant escalates: skip the sandbox so the approved
    // command (shell command, or a skill script's program) runs with full reach
    // without re-tripping the fence. Keyed identically to the crossing descriptor
    // raised in `run_one`. Executor-only: the project's live checkout is read-only
    // and non-negotiable, so a command grant must never re-open it to writes.
    if !readonly_non_worktree {
        if let Some(cmd) = command_for_grant {
            if granted.contains(&normalize_command(cmd)) {
                return None;
            }
        }
    }

    // Project-declared check/test commands are trusted, not risky mutations: run
    // them with host permissions (no fence prompt, no idle-hang), matching the
    // turn-end cadence which already runs these exact commands unconfined.
    // Executor-checkout only — the live checkout stays read-only.
    //
    // The trust source is the CANONICAL main checkout, not the agent-mutable
    // worktree: the `checks` contract and package.json `scripts` are resolved from
    // the project's live main checkout (worktree used only as a fallback when the
    // project repo is unresolved). This is deliberately the OPPOSITE of the check
    // cadences, which bind their contract to the commit they evaluate: here the
    // question is not "which checks does this tree declare" but "has the project
    // sanctioned this command to run unconfined", and only the canonical checkout
    // can answer that. This runs host-side (not in the fenced agent subprocess),
    // so reading the main checkout is not a fence crossing, and a branch cannot
    // self-grant an unconfined run by committing its own check or package script.
    // See `crate::config::check_exemption` and docs/worktree-fence.md.
    if !readonly_non_worktree {
        if let (Some(cmd), Some(pid)) = (command_for_grant, project_id) {
            let main_repo = crate::projects::crud::resolve_local_repo_path_and_key(&orch.db, pid)
                .await
                .ok()
                .and_then(|(path, _key)| path);
            let source = main_repo
                .as_deref()
                .map(std::path::Path::new)
                .unwrap_or_else(|| std::path::Path::new(cwd));
            let checks = crate::config::project_settings::load_checks(source).unwrap_or_default();
            let scripts = crate::config::check_exemption::load_project_scripts(source);
            if crate::config::check_exemption::is_exempt_check_command(cmd, &checks, &scripts) {
                log::info!(
                    "check-command exemption: running declared check/test unconfined (cwd={cwd})"
                );
                return None;
            }
        }
    }

    let deny_read = orch.sandbox_deny_read();

    // User-owned acceptance is the single trust decision for an exact command
    // declared by this project. Accepted executor commands skip policy creation
    // entirely, restoring the user's normal shell state and credential access.
    // Live checkouts retain their structural read-only wrapper.
    let accepted_command = match (command_for_grant, project_id) {
        (Some(command), Some(project_id)) => {
            let accepted = crate::config::settings::load_accepted_fence_commands(&orch.config_dir)
                .remove(project_id)
                .unwrap_or_default();
            let project_terminals =
                crate::config::project_settings::load_terminal_commands(std::path::Path::new(cwd));
            crate::config::dev_commands::resolve_carveouts(command, &project_terminals, &accepted)
                .unconfined
        }
        _ => false,
    };
    if accepted_command && !readonly_non_worktree {
        log::info!(
            "accepted dev command matched project shortcut; launching unfenced (command={:?}, cwd={cwd})",
            command_for_grant.unwrap_or_default()
        );
        return None;
    }

    // A user live-checkout cwd is read-only (dropped from the writable set) but
    // readable; an executor checkout remains writable within its cell boundary. Session path
    // grants flow into either policy, but `for_readonly_checkout` drops any grant
    // that lies within (or contains) the checkout, so a grant can never re-open it.
    let checkout = std::path::Path::new(cwd);
    let mut policy = if readonly_non_worktree {
        sandbox::SandboxPolicy::for_readonly_checkout(checkout, &granted, deny_read)
    } else {
        sandbox::SandboxPolicy::for_run(checkout, &granted, deny_read)
    };
    if !readonly_non_worktree {
        apply_safe_jj_workspace_refresh_status_carveout(&mut policy, checkout, command_for_grant);
    }

    Some((policy, fence))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn safe_jj_workspace_refresh_status_command_accepts_exact_status_forms() {
        assert!(is_safe_jj_workspace_refresh_status_command(
            "jj workspace update-stale && jj st"
        ));
        assert!(is_safe_jj_workspace_refresh_status_command(
            "  jj   workspace   update-stale   &&   jj   status  "
        ));
    }

    #[test]
    fn safe_jj_workspace_refresh_status_command_rejects_extra_shell_work() {
        assert!(!is_safe_jj_workspace_refresh_status_command(
            "jj workspace update-stale && jj st && touch outside"
        ));
        assert!(!is_safe_jj_workspace_refresh_status_command(
            "jj workspace update-stale; touch outside && jj st"
        ));
        assert!(!is_safe_jj_workspace_refresh_status_command(
            "jj workspace update-stale && jj log"
        ));
    }

    #[test]
    fn jj_workspace_repo_dir_resolves_relative_pointer() {
        let dir = tempfile::tempdir().unwrap();
        let worktree = dir.path().join("ws");
        let jj_dir = worktree.join(".jj");
        let store_repo = dir.path().join("store").join(".jj").join("repo");
        std::fs::create_dir_all(&jj_dir).unwrap();
        std::fs::create_dir_all(&store_repo).unwrap();
        std::fs::write(jj_dir.join("repo"), "../../store/.jj/repo\n").unwrap();

        assert_eq!(
            jj_workspace_repo_dir(&worktree).unwrap(),
            store_repo.canonicalize().unwrap()
        );
    }

    #[test]
    fn safe_jj_workspace_refresh_status_carveout_grants_only_repo_pointer_target() {
        let dir = tempfile::tempdir().unwrap();
        let worktree = dir.path().join("ws");
        let jj_dir = worktree.join(".jj");
        let store_repo = dir.path().join("store").join(".jj").join("repo");
        std::fs::create_dir_all(&jj_dir).unwrap();
        std::fs::create_dir_all(&store_repo).unwrap();
        std::fs::write(jj_dir.join("repo"), "../../store/.jj/repo\n").unwrap();

        let mut policy = sandbox::SandboxPolicy {
            worktree: worktree.clone(),
            writable_extra: vec![],
            deny_read: vec![],
            writable_regex: vec![],
            worktree_writable: true,
        };
        apply_safe_jj_workspace_refresh_status_carveout(
            &mut policy,
            &worktree,
            Some("jj workspace update-stale && jj st"),
        );

        assert_eq!(
            policy.writable_extra,
            vec![store_repo.canonicalize().unwrap()]
        );
    }

    #[test]
    fn safe_jj_workspace_refresh_status_carveout_rejects_other_commands() {
        let dir = tempfile::tempdir().unwrap();
        let worktree = dir.path().join("ws");
        let jj_dir = worktree.join(".jj");
        let store_repo = dir.path().join("store").join(".jj").join("repo");
        std::fs::create_dir_all(&jj_dir).unwrap();
        std::fs::create_dir_all(&store_repo).unwrap();
        std::fs::write(jj_dir.join("repo"), "../../store/.jj/repo\n").unwrap();

        let mut policy = sandbox::SandboxPolicy {
            worktree: worktree.clone(),
            writable_extra: vec![],
            deny_read: vec![],
            writable_regex: vec![],
            worktree_writable: true,
        };
        apply_safe_jj_workspace_refresh_status_carveout(
            &mut policy,
            &worktree,
            Some("jj workspace update-stale && jj st && touch outside"),
        );

        assert!(policy.writable_extra.is_empty());
    }
}
