use super::*;

/// Create a worktree for a job using the orchestrator's service traits.
#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_worktree_for_job(
    orch: &Orchestrator,
    repo_path: &str,
    worktree_path: &Path,
    branch: &str,
    base_ref: &str,
    identity: &crate::jj::WorkspaceIdentity,
    allow_retry_cleanup: bool,
    job_id: &str,
    issue_id: Option<String>,
    sink: &setup_progress::SetupSink,
    cancel: &Arc<AtomicBool>,
    child_slot: &Arc<Mutex<Option<Box<dyn crate::services::ChildProcess>>>>,
) -> Result<(), crate::git::worktree::SetupError> {
    let settings = load_project_settings(Path::new(repo_path));
    let populate_config = settings.populate_config();
    let setup_commands = settings.setup_commands.unwrap_or_default();

    let git = &*orch.services.git;
    let fs = &*orch.services.fs;
    let process = &*orch.services.process;
    let repo = Path::new(repo_path);

    let cleanup = || {
        // A jj workspace is .jj-only and not registered as a git worktree, so
        // `git worktree remove` would fail and strand both the directory and the
        // shared-store registration. Forget it from the store (handles a
        // partially-created workspace too) and remove the dir.
        let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
        let store = crate::jj::project_store_dir(&orch.config_dir, repo);
        let _ = crate::jj::forget_workspace_name(&jj, &store, &identity.workspace_name);
        let _ = std::fs::remove_dir_all(worktree_path);
    };

    // 1. Create worktree
    setup_progress::emit(
        sink,
        job_id,
        issue_id.clone(),
        "status",
        Some("worktree"),
        None,
        Some(format!(
            "[info] Creating worktree at {}",
            worktree_path.display()
        )),
    );
    // Provision one shared jj store (a Cairn-managed jj repo backed by the
    // project's existing .git, so the user's checkout is never touched) and add
    // this job's working dir as a jj workspace off it. jj is the only substrate.
    // See docs/jj-migration.md.
    let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let store = crate::jj::project_store_dir(&orch.config_dir, repo);
    crate::jj::ensure_project_store(&jj, &store, repo).map_err(|message| {
        crate::git::worktree::SetupError::Spawn {
            command: "jj git init --git-repo".to_string(),
            message,
        }
    })?;
    // Resolve the base to a revision jj can always find in the shared store, so a
    // local-only repo whose configured default branch has no matching ref (or an
    // unborn/empty repo) still provisions instead of failing
    // `Revision <x> doesn't exist`. See crate::jj::resolve_base_rev.
    let mut base_rev = crate::jj::resolve_base_rev(&jj, &store, base_ref, |r| {
        git.rev_parse(repo, vec![r.to_string()])
            .ok()
            .filter(|s| !s.is_empty())
    });

    let marker_matches = crate::jj::read_workspace_identity(worktree_path)
        .map(|marker| {
            marker.lineage_root_job_id == identity.lineage_root_job_id
                && marker.owner_job_id == identity.owner_job_id
                && marker.project_id == identity.project_id
                && marker.project_root == identity.project_root
                && marker.worktree_path == identity.worktree_path
                && marker.branch == identity.branch
                && marker.workspace_name == identity.workspace_name
        })
        .unwrap_or(false);
    let registration_exists =
        crate::jj::workspace_registered(&jj, &store, &identity.workspace_name);
    let bookmark_tip = crate::jj::bookmark_commit(&jj, &store, branch);
    let any_existing = worktree_path.exists() || registration_exists || bookmark_tip.is_some();
    if any_existing {
        let safe_retry = allow_retry_cleanup
            && crate::jj::workspace_retry_is_clean(&jj, worktree_path)
            && (marker_matches || registration_exists);
        if !safe_retry {
            return Err(crate::git::worktree::SetupError::Spawn {
                command: "managed workspace ownership check".to_string(),
                message: format!(
                    "workspace slot is occupied by another or unproven lineage: path={}, branch={}, workspace={}",
                    worktree_path.display(),
                    branch,
                    identity.workspace_name
                ),
            });
        }
        if marker_matches {
            if let Some(tip) = bookmark_tip {
                base_rev = tip;
            }
        }
        crate::jj::cleanup_workspace_retry(&jj, &store, worktree_path, &identity.workspace_name)
            .map_err(|message| crate::git::worktree::SetupError::Spawn {
                command: "jj workspace retry cleanup".to_string(),
                message,
            })?;
    }

    crate::jj::add_workspace(&jj, &store, worktree_path, branch, &base_rev, None).map_err(
        |message| crate::git::worktree::SetupError::Spawn {
            command: "jj workspace add".to_string(),
            message,
        },
    )?;
    // Record the integration base for in-fence check tooling (diff-vs-base
    // attribution). The base BRANCH is what the changed-file diff resolves
    // against — it auto-advances with the integration tip — while the resolved
    // SHA is a stable cache key. Auxiliary metadata: a write failure must not
    // fail provisioning. See scripts/lib/check-base.ts / docs/check-harness.md.
    if let Err(error) = crate::jj::write_base_marker(worktree_path, base_ref, &base_rev) {
        log::warn!("failed to write base marker for {branch}: {error}");
    }
    // Record the project's primary checkout so in-worktree dev tooling can
    // borrow machine-local artifacts (sidecar binaries) from it. Auxiliary
    // metadata like the markers above: never fails provisioning.
    if let Err(error) = crate::jj::write_project_root_marker(worktree_path, repo) {
        log::warn!("failed to write project root marker for {branch}: {error}");
    }
    crate::jj::write_workspace_identity(worktree_path, identity).map_err(|message| {
        crate::git::worktree::SetupError::Spawn {
            command: "write managed workspace identity".to_string(),
            message,
        }
    })?;
    setup_progress::emit(
        sink,
        job_id,
        issue_id.clone(),
        "status",
        Some("worktree"),
        None,
        Some(format!("[info] Worktree ready (branch {branch})")),
    );
    if cancel.load(Ordering::SeqCst) {
        cleanup();
        return Err(crate::git::worktree::SetupError::Cancelled);
    }

    // 2. Populate gitignored content per explicit rules
    if !populate_config.is_empty() {
        setup_progress::emit(
            sink,
            job_id,
            issue_id.clone(),
            "status",
            Some("populate"),
            None,
            Some("[info] Populating gitignored content".to_string()),
        );
        // Establish the jj-native exclude BEFORE populate copies files in: jj
        // auto-tracks a new file on the first snapshot after it appears, and a
        // later rule cannot un-track it, so explicitly-populated gitignored
        // content (e.g. .env, node_modules) must be kept out of
        // snapshot.auto-track up front. Best-effort here — the post-populate
        // backstop below is the real guarantee and fails setup loudly if any
        // populated path is still snapshot-visible.
        if let Err(e) = crate::jj::set_populate_auto_track(&jj, &store, &populate_config, &[]) {
            log::warn!("Failed to set snapshot.auto-track for populate excludes: {e}");
        }
        match crate::git::worktree::populate_worktree(
            git,
            fs,
            repo,
            worktree_path,
            &populate_config,
        ) {
            Ok(result) => {
                let line = format!(
                    "[info] Populating gitignored content ({} copied, {} symlinked, {} skipped, {} failed)",
                    result.copied, result.symlinked, result.skipped, result.failed
                );
                log::info!("{line}");
                setup_progress::emit(
                    sink,
                    job_id,
                    issue_id.clone(),
                    "status",
                    Some("populate"),
                    None,
                    Some(line),
                );
            }
            Err(e) => {
                let line = format!("[info] Worktree population failed (continuing): {e}");
                log::warn!("{line}");
                setup_progress::emit(
                    sink,
                    job_id,
                    issue_id.clone(),
                    "status",
                    Some("populate"),
                    None,
                    Some(line),
                );
            }
        }
        // Security backstop: explicitly-populated gitignored content must stay
        // UNCOMMITTED so a later run/write seal can never commit secrets or
        // build artifacts. At this point only populate has run (no setup
        // commands, no agent edits), so ANY path visible in `@` is populated
        // content that leaked past the auto-track exclude. Self-heal a
        // conservative glob-translation miss by adding the exact leaked paths to
        // auto-track and un-tracking them; fail setup loudly if anything still
        // leaks rather than provision a worktree where populated content could
        // be sealed.
        match crate::jj::working_copy_dirty_paths(&jj, worktree_path) {
            Ok(leaked) if !leaked.is_empty() => {
                log::warn!(
                    "populate exclude missed {} path(s); self-healing: {:?}",
                    leaked.len(),
                    leaked
                );
                let _ = crate::jj::set_populate_auto_track(&jj, &store, &populate_config, &leaked);
                let _ = crate::jj::untrack_paths(&jj, worktree_path, &leaked);
                let still = crate::jj::working_copy_dirty_paths(&jj, worktree_path)
                    .unwrap_or_else(|_| leaked.clone());
                if !still.is_empty() {
                    cleanup();
                    return Err(crate::git::worktree::SetupError::Spawn {
                        command: "populate exclude verification".to_string(),
                        message: format!(
                            "explicitly-populated gitignored content is still snapshot-visible \
                             and could be committed: {}. Refusing to provision the worktree.",
                            still.join(", ")
                        ),
                    });
                }
            }
            Ok(_) => {}
            Err(e) => {
                // Can't verify the security invariant — fail loud rather than
                // provision a worktree where populated content might be sealed.
                cleanup();
                return Err(crate::git::worktree::SetupError::Spawn {
                    command: "populate exclude verification".to_string(),
                    message: format!("could not verify populate excludes: {e}"),
                });
            }
        }
        if cancel.load(Ordering::SeqCst) {
            cleanup();
            return Err(crate::git::worktree::SetupError::Cancelled);
        }
    }

    // 3. Run setup commands
    if !setup_commands.is_empty() {
        if let Err(e) = crate::git::worktree::run_setup_commands_with_process_streaming(
            process,
            worktree_path,
            &setup_commands,
            sink,
            job_id,
            issue_id.clone(),
            cancel,
            child_slot,
        ) {
            log::error!("Setup commands failed, cleaning up worktree: {}", e);
            cleanup();
            return Err(e);
        }
    }

    Ok(())
}

/// The resolved worktree plan for an ephemeral call or workflow run — the pure
/// decision, split from the I/O that carries it out.
///
/// An Inherit call/workflow shares a worktree-backed parent's tree, but an
/// ambient (no-worktree) parent has none to inherit, so it mints its own
/// ephemeral worktree off the parent's base branch — the same trade the
/// child-task path already makes (CAIRN-2476). Both `calls.rs` and `workflow.rs`
/// resolve through this one function so the two paths cannot drift; the caller
/// performs the scratch mkdir / `jj workspace add` against the returned plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CallWorktreePlan {
    /// Inherit from a worktree-backed parent: share its worktree path.
    Share { path: String },
    /// Inherit from an ambient (no-worktree) parent: mint an ephemeral worktree
    /// off `base_ref`, owned by this job and reclaimed at terminalization.
    MintEphemeral { base_ref: String },
    /// `none`: a fresh scratch dir with no project-tree binding.
    Scratch,
}

/// Resolve the worktree plan for an ephemeral call/workflow from its mode and the
/// parent job's worktree/base. Pure and unit-testable; the mint I/O lives in the
/// caller. An ambient parent (NULL `worktree_path`) under Inherit mints off the
/// parent's base branch, falling back to `HEAD`.
pub(crate) fn resolve_call_worktree_plan(
    worktree: crate::execution::jobs::CallWorktree,
    parent_worktree_path: Option<&str>,
    parent_base_branch: Option<&str>,
) -> CallWorktreePlan {
    use crate::execution::jobs::CallWorktree;
    match worktree {
        CallWorktree::Inherit => match parent_worktree_path {
            Some(path) => CallWorktreePlan::Share {
                path: path.to_string(),
            },
            None => CallWorktreePlan::MintEphemeral {
                base_ref: parent_base_branch.unwrap_or("HEAD").to_string(),
            },
        },
        CallWorktree::None => CallWorktreePlan::Scratch,
    }
}

/// Mint a throwaway worktree for a task delegated by an ambient (no-worktree)
/// parent, off `base_ref`.
///
/// A task must never run in the user's live checkout, and an ambient parent has
/// no worktree to inherit — so its task gets its own isolated worktree here. The
/// delegated subgraph carries no PR machinery and this branch is discarded
/// unconditionally at teardown, so no task commit can ever land; the owning task
/// job is marked `owns_ephemeral_worktree` and the worktree is reclaimed the
/// moment that job terminalizes. Returns `(worktree_path, branch)`.
///
/// This mirrors the synchronous cost `prepare_job` already pays for a
/// worktree-backed node (jj workspace add + populate + setup), bounded by the
/// task's lifetime. The delegation-DAG path reaches the same outcome through
/// `prepare_job`'s `WorktreeMode::Own` branch; this helper is the synchronous
/// `create_child_task` path's counterpart, keeping both task paths identical.
pub(crate) fn ensure_ephemeral_task_worktree(
    orch: &Orchestrator,
    repo_path: &str,
    project_id: &str,
    job_id: &str,
    issue_id: Option<String>,
    base_ref: &str,
) -> Result<(String, String), String> {
    let worktrees_dir =
        crate::managed_worktrees::base_dir().ok_or("Could not find managed worktrees directory")?;
    // The job id is globally unique, so a directory keyed on it never collides
    // with another task's ephemeral worktree.
    let safe_id: String = job_id
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '-' })
        .collect();
    let wt_dir = format!("task-{safe_id}");
    let branch = format!("agent/{wt_dir}");
    let wt_path = worktrees_dir.join(&wt_dir);

    let identity = crate::jj::WorkspaceIdentity::new(
        job_id,
        job_id,
        project_id,
        PathBuf::from(repo_path),
        wt_path.clone(),
        branch.clone(),
        crate::jj::workspace_name_for_branch(&branch),
        base_ref,
    );
    let cancel = Arc::new(AtomicBool::new(false));
    let child_slot = Arc::new(Mutex::new(None));
    let sink = setup_progress::make_sink(orch, job_id, issue_id.clone());
    prepare_worktree_for_job(
        orch,
        repo_path,
        &wt_path,
        &branch,
        base_ref,
        &identity,
        true,
        job_id,
        issue_id,
        &sink,
        &cancel,
        &child_slot,
    )
    .map_err(|e| e.to_string())?;

    Ok((wt_path.to_string_lossy().to_string(), branch))
}

#[cfg(test)]
mod plan_tests {
    use super::*;
    use crate::execution::jobs::CallWorktree;

    #[test]
    fn inherit_from_worktree_backed_parent_shares() {
        // A worktree-backed parent shares its tree — no mint, no scratch.
        let plan = resolve_call_worktree_plan(
            CallWorktree::Inherit,
            Some("/work/parent-wt"),
            Some("agent/parent"),
        );
        assert_eq!(
            plan,
            CallWorktreePlan::Share {
                path: "/work/parent-wt".to_string()
            }
        );
    }

    #[test]
    fn inherit_from_ambient_parent_mints_off_base_branch() {
        // An ambient (no-worktree) parent mints an ephemeral worktree off its base.
        let plan = resolve_call_worktree_plan(CallWorktree::Inherit, None, Some("main"));
        assert_eq!(
            plan,
            CallWorktreePlan::MintEphemeral {
                base_ref: "main".to_string()
            }
        );
    }

    #[test]
    fn inherit_from_ambient_parent_without_base_falls_back_to_head() {
        let plan = resolve_call_worktree_plan(CallWorktree::Inherit, None, None);
        assert_eq!(
            plan,
            CallWorktreePlan::MintEphemeral {
                base_ref: "HEAD".to_string()
            }
        );
    }

    #[test]
    fn none_mode_is_scratch_regardless_of_parent() {
        // `none` never binds to the project tree, ambient parent or not.
        assert_eq!(
            resolve_call_worktree_plan(CallWorktree::None, None, Some("main")),
            CallWorktreePlan::Scratch
        );
        assert_eq!(
            resolve_call_worktree_plan(CallWorktree::None, Some("/work/parent-wt"), None),
            CallWorktreePlan::Scratch
        );
    }
}
