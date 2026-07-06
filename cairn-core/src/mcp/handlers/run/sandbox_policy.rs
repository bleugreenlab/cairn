//! OS sandbox policy construction for a run, including the read-only
//! live-checkout regime and the jj workspace-refresh carveout.

use crate::mcp::handlers::normalize_command;
use crate::models::Fence;
use crate::orchestrator::Orchestrator;
use crate::services::sandbox;

/// Build the OS sandbox policy for a run, or `None` when no confinement applies.
///
/// Two regimes:
/// - **Worktree cwd**: gated on the fence — `allow` (or no run context) runs
///   unconfined; `ask`/`deny` confine writes to the worktree.
/// - **Non-worktree cwd** (the project's live checkout, for example triage or
///   read-only analysis): a read-only-checkout policy applies STRUCTURALLY, regardless of
///   fence, so a stray write into the live checkout is kernel-denied while the
///   checkout stays readable.
///
/// The unconfined escape hatches — an already-granted command and an accepted
/// dev-command carveout with no declared scopes — are WORKTREE-ONLY: a
/// non-worktree run never returns `None` for them, because the live checkout is
/// read-only and non-grantable. The only `None` for a non-worktree cwd is a host
/// with no sandbox primitive, where the restored dirt-detection warning covers
/// the gap. Carveout write-globs still apply on a non-worktree cwd, but any glob
/// that would write the checkout itself is dropped.
///
/// Whether a writable carveout scope `glob` would permit a write into `checkout`
/// (the project's live checkout). Used to refuse a dev-command scope that would
/// defeat the read-only-checkout guarantee on a non-worktree run.
///
/// A scope can only ever write at or below its **non-wildcard literal prefix**
/// (everything from the first `*` on merely widens the match within that prefix).
/// So the scope re-opens checkout writes iff that prefix sits inside the checkout
/// — including a nested subtree like `{checkout}/target/**`, which a root/child
/// regex probe would miss — or is an ancestor whose subpath grant would include
/// the checkout. Concrete (wildcard-free) scopes fall out of the same check with
/// the whole scope as the prefix. Fail-closed: an empty prefix (a leading-`*`
/// glob) counts as covering.
fn glob_covers_checkout(glob: &str, checkout: &std::path::Path) -> bool {
    let checkout = checkout
        .canonicalize()
        .unwrap_or_else(|_| checkout.to_path_buf());
    let literal_prefix = match glob.find('*') {
        Some(i) => &glob[..i],
        None => glob,
    };
    let prefix = std::path::Path::new(literal_prefix);
    let prefix = prefix
        .canonicalize()
        .unwrap_or_else(|_| prefix.to_path_buf());
    prefix.starts_with(&checkout) || checkout.starts_with(&prefix)
}

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

/// Build the OS sandbox policy for a run, or `None` when no confinement applies.
pub(crate) async fn build_run_sandbox_policy(
    orch: &Orchestrator,
    cwd: &str,
    run_id: Option<&str>,
    project_id: Option<&str>,
    command_for_grant: Option<&str>,
    branch_scoped_run: bool,
) -> Option<(sandbox::SandboxPolicy, Fence)> {
    use crate::mcp::handlers::permission::resolve_fence_policy;

    // The project's live checkout (a non-jj cwd: triage / read-only analysis)
    // is read-only for agents, non-negotiable, so the read-only-checkout sandbox
    // applies STRUCTURALLY regardless of fence; a request without an execution
    // snapshot would otherwise be fully unconfined. A real worktree
    // keeps the fence gate (ask/deny confine; allow runs free).
    let non_worktree = !crate::jj::is_jj_dir(std::path::Path::new(cwd));
    let readonly_non_worktree = non_worktree && !branch_scoped_run;
    let fence = if readonly_non_worktree {
        // Read-only and non-grantable: `Deny` makes run_one never route a denial
        // through the fence (no prompt, no session grant). Detection of the block
        // itself is enabled separately in execute_process so the agent still gets
        // the clear read-only-checkout message.
        Fence::Deny
    } else {
        let fence = resolve_fence_policy(orch, run_id).await?;
        if !sandbox::sandbox_applies(fence) {
            return None;
        }
        fence
    };

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
    // raised in `run_one`. WORKTREE-ONLY: the project's live checkout is read-only
    // and non-negotiable, so a command grant (earned in some worktree run, since
    // the session set is shared) must never re-open the checkout to writes.
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
    // Worktree-only — the live checkout stays read-only.
    //
    // The trust source is the CANONICAL main checkout, not the agent-mutable
    // worktree: the `checks` contract and package.json `scripts` are resolved from
    // the project's live main checkout (worktree used only as a fallback when the
    // project repo is unresolved), mirroring the check cadences'
    // `load_live_project_checks`. This runs host-side (not in the fenced agent
    // subprocess), so reading the main checkout is not a fence crossing, and a
    // branch cannot self-grant an unconfined run by committing its own check or
    // package script. See `crate::config::check_exemption` and
    // docs/worktree-fence.md.
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

    // Durable dev-command fence carveouts: a project terminal command the user
    // has accepted as a fence-crosser pre-ordains the crossings it needs (e.g.
    // Cairn's `bun run dev:instance` writing its per-instance home outside the
    // worktree), so an approved launch does not park on a fence prompt. The
    // declaration lives in repo config but only takes effect once the user has
    // accepted that command (stored per project in workspace settings), so a
    // cloned repo can declare a fence-crosser but cannot grant itself the
    // crossing. See `crate::config::dev_commands` and `docs/worktree-fence.md`.
    let carveouts = match (command_for_grant, project_id) {
        (Some(cmd), Some(pid)) => {
            let accepted = crate::config::settings::load_accepted_fence_commands(&orch.config_dir)
                .remove(pid)
                .unwrap_or_default();
            if accepted.is_empty() {
                Default::default()
            } else {
                let project_terminals = crate::config::project_settings::load_terminal_commands(
                    std::path::Path::new(cwd),
                );
                let templates = crate::config::settings::build_service_templates(
                    &orch.config_dir,
                    Some(std::path::PathBuf::from(cwd)),
                );
                crate::config::dev_commands::resolve_carveouts(
                    cmd,
                    &project_terminals,
                    &accepted,
                    &deny_read,
                    &templates,
                )
            }
        }
        _ => Default::default(),
    };

    // An accepted command with no declared scopes crosses fully: skip the sandbox
    // like a command-scoped session grant does. WORKTREE-ONLY for the same reason
    // — on the live checkout an unconfined carveout still resolves to a read-only
    // checkout policy; nothing re-opens the checkout to writes.
    if carveouts.unconfined && !readonly_non_worktree {
        log::info!("dev-command carveout: running accepted command unconfined (cwd={cwd})");
        return None;
    }
    if !carveouts.dropped_sensitive.is_empty() {
        log::warn!(
            "dev-command carveout dropped {} declared scope(s) that touch the read denylist \
             (a repo-committed terminal command cannot grant a secret-store crossing): {:?}",
            carveouts.dropped_sensitive.len(),
            carveouts.dropped_sensitive,
        );
    }

    // Non-worktree cwd: the live checkout is read-only (dropped from the writable
    // set) but readable; a worktree cwd keeps the worktree writable. Session path
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
    // Writable carveout scopes are globs: realize them as macOS SBPL regex grants
    // (matching the build-service mechanism), and additionally as a concrete
    // writable subpath for any wildcard-free scope so the Linux landlock path
    // (which does not translate regex grants) still honors simple carveouts. On a
    // non-worktree cwd, refuse any scope that would write the read-only live
    // checkout — the read-only guarantee outranks a dev-command's declared scope.
    for glob in &carveouts.write_globs {
        if readonly_non_worktree && glob_covers_checkout(glob, checkout) {
            log::warn!(
                "dev-command carveout scope {glob:?} would write the read-only live checkout; \
                 dropping it (cwd={cwd})"
            );
            continue;
        }
        policy.writable_regex.push(sandbox::glob_to_regex(glob));
        if !glob.contains('*') {
            policy.writable_extra.push(std::path::PathBuf::from(glob));
        }
    }

    Some((policy, fence))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn branch_scoped_checkout_policy_is_writable_while_plain_live_checkout_is_readonly() {
        let checkout = std::path::Path::new("/project/live");
        let readonly = sandbox::SandboxPolicy::for_readonly_checkout(checkout, &[], vec![]);
        let branch_scoped = sandbox::SandboxPolicy::for_run(checkout, &[], vec![]);

        assert!(!readonly.worktree_writable);
        assert!(branch_scoped.worktree_writable);
    }
    #[test]
    fn glob_covers_checkout_drops_checkout_scopes_keeps_external() {
        // A non-worktree run must drop any carveout scope that could write the
        // read-only live checkout, while keeping scopes safely outside it.
        let checkout = std::path::Path::new("/project/live");
        // Wildcard scopes that reach into the checkout are covered.
        assert!(glob_covers_checkout("/project/live/**", checkout));
        // A broader ancestor wildcard that also matches the checkout.
        assert!(glob_covers_checkout("/project/**", checkout));
        // A nested subtree wildcard INSIDE the checkout (the case a root/child
        // regex probe missed): `target/`, `node_modules/`, etc.
        assert!(glob_covers_checkout("/project/live/target/**", checkout));
        assert!(glob_covers_checkout(
            "/project/live/node_modules/**",
            checkout
        ));
        // Concrete scope inside the checkout.
        assert!(glob_covers_checkout("/project/live/target", checkout));
        // Concrete ancestor whose subpath grant would include the checkout.
        assert!(glob_covers_checkout("/project", checkout));
        // The checkout itself.
        assert!(glob_covers_checkout("/project/live", checkout));
        // Safely-outside scopes are not covered (they stay writable).
        assert!(!glob_covers_checkout("/other/**", checkout));
        assert!(!glob_covers_checkout("/home/u/.cairn-dev/**", checkout));
        assert!(!glob_covers_checkout("/scratch/ok", checkout));
    }
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
