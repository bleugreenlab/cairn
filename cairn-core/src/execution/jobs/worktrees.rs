use super::*;

/// Emit a durable phase timing with the project store as its correlation key.
fn emit_phase_timing(
    sink: &setup_progress::SetupSink,
    job_id: &str,
    issue_id: Option<String>,
    phase: &str,
    elapsed: std::time::Duration,
    store: &Path,
) {
    let elapsed_ms = elapsed.as_millis().min(u128::from(u64::MAX)) as u64;
    log::info!(
        "managed workspace setup phase complete: job_id={job_id}, phase={phase}, elapsed_ms={elapsed_ms}, store={}",
        store.display()
    );
    setup_progress::emit_timing(
        sink,
        job_id,
        issue_id,
        phase,
        elapsed_ms,
        store.to_string_lossy().into_owned(),
    );
}

fn acquire_store_guard(
    orch: &Orchestrator,
    store: &Path,
    operation: String,
) -> crate::orchestrator::JjStoreGuard {
    run_db({
        let orch = orch.clone();
        let store = store.to_path_buf();
        async move { Ok(orch.acquire_jj_store_lock(&store, operation).await) }
    })
    .expect("jj store lock acquisition cannot fail")
}

/// Forget store registration under the store lock, then remove the directory
/// without retaining the project-wide lock across filesystem I/O.
fn cleanup_prepared_worktree(
    orch: &Orchestrator,
    jj: &crate::jj::JjEnv,
    store: &Path,
    worktree_path: &Path,
    workspace_name: &str,
    branch: &str,
    job_id: &str,
    issue_id: Option<String>,
    sink: &setup_progress::SetupSink,
) {
    let started = std::time::Instant::now();
    {
        let _guard = acquire_store_guard(
            orch,
            store,
            format!("workspace provisioning cleanup for {job_id}"),
        );
        let _ = crate::jj::forget_workspace_name(jj, store, workspace_name);
        // Workspace creation also creates the job bookmark. A failed setup must
        // not leave that coordinate occupying the next allocation attempt.
        let _ = jj.run(store, &["bookmark", "delete", branch], "jj bookmark delete");
    }
    if let Err(error) = std::fs::remove_dir_all(worktree_path) {
        if error.kind() != std::io::ErrorKind::NotFound {
            log::warn!(
                "failed to remove managed workspace {} during cleanup: {error}",
                worktree_path.display()
            );
        }
    }
    emit_phase_timing(sink, job_id, issue_id, "cleanup", started.elapsed(), store);
}

fn cleanup_owned_mutation_failure(cleanup_allowed: bool, cleanup: impl FnOnce()) {
    if cleanup_allowed {
        cleanup();
    }
}

/// Create and prepare a worktree. Each store-mutating phase acquires its own
/// short guard; population and arbitrary project setup commands never hold it.
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
    let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let store = crate::jj::project_store_dir(&orch.config_dir, repo);

    let cleanup = || {
        cleanup_prepared_worktree(
            orch,
            &jj,
            &store,
            worktree_path,
            &identity.workspace_name,
            branch,
            job_id,
            issue_id.clone(),
            sink,
        );
    };

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

    // Store initialization, retry ownership validation, workspace creation,
    // identity markers, and populate exclusions form the only provisioning
    // mutation critical section. Exclusions must exist before copied files do.
    let wait_started = std::time::Instant::now();
    let guard = acquire_store_guard(
        orch,
        &store,
        format!("workspace provisioning mutation for {job_id}"),
    );
    emit_phase_timing(
        sink,
        job_id,
        issue_id.clone(),
        "store-lock-wait",
        wait_started.elapsed(),
        &store,
    );
    let mutation_started = std::time::Instant::now();
    // Destructive cleanup is legal only after this invocation has either proven
    // retry ownership or successfully created the workspace. A collision refusal
    // must leave the unproven path, registration, and bookmark untouched.
    let cleanup_allowed = std::cell::Cell::new(false);
    let mutation_result = (|| {
        crate::jj::ensure_project_store(&jj, &store, repo).map_err(|message| {
            crate::git::worktree::SetupError::Spawn {
                command: "jj git init --git-repo".to_string(),
                message,
            }
        })?;
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
                        worktree_path.display(), branch, identity.workspace_name
                    ),
                });
            }
            if marker_matches {
                if let Some(tip) = bookmark_tip {
                    base_rev = tip;
                }
            }
            cleanup_allowed.set(true);
            crate::jj::cleanup_workspace_retry(
                &jj,
                &store,
                worktree_path,
                &identity.workspace_name,
            )
            .map_err(|message| crate::git::worktree::SetupError::Spawn {
                command: "jj workspace retry cleanup".to_string(),
                message,
            })?;
        }
        if let Err(message) =
            crate::jj::add_workspace(&jj, &store, worktree_path, branch, &base_rev, None)
        {
            // `add_workspace` spans the jj mutation and marker/bookmark writes.
            // The slot was proven empty above while this same store guard was
            // held, so any expected coordinate that appeared is evidence of a
            // partial creation by this invocation, not an earlier collision.
            let partially_created = worktree_path.exists()
                || crate::jj::workspace_registered(&jj, &store, &identity.workspace_name)
                || crate::jj::bookmark_commit(&jj, &store, branch).is_some();
            cleanup_allowed.set(partially_created);
            return Err(crate::git::worktree::SetupError::Spawn {
                command: "jj workspace add".to_string(),
                message,
            });
        }
        cleanup_allowed.set(true);
        if let Err(error) = crate::jj::write_base_marker(worktree_path, base_ref, &base_rev) {
            log::warn!("failed to write base marker for {branch}: {error}");
        }
        if let Err(error) = crate::jj::write_project_root_marker(worktree_path, repo) {
            log::warn!("failed to write project root marker for {branch}: {error}");
        }
        crate::jj::write_workspace_identity(worktree_path, identity).map_err(|message| {
            crate::git::worktree::SetupError::Spawn {
                command: "write managed workspace identity".to_string(),
                message,
            }
        })?;
        if !populate_config.is_empty() {
            crate::jj::set_populate_auto_track(&jj, &store, &populate_config, &[]).map_err(
                |message| crate::git::worktree::SetupError::Spawn {
                    command: "configure populate exclusions".to_string(),
                    message,
                },
            )?;
        }
        Ok(())
    })();
    drop(guard);
    emit_phase_timing(
        sink,
        job_id,
        issue_id.clone(),
        "workspace-mutation",
        mutation_started.elapsed(),
        &store,
    );
    if let Err(error) = mutation_result {
        cleanup_owned_mutation_failure(cleanup_allowed.get(), cleanup);
        return Err(error);
    }

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
        let populate_started = std::time::Instant::now();
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
            Err(error) => {
                let line = format!("[info] Worktree population failed (continuing): {error}");
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
        emit_phase_timing(
            sink,
            job_id,
            issue_id.clone(),
            "populate-discovery-copy",
            populate_started.elapsed(),
            &store,
        );

        let verification_started = std::time::Instant::now();
        let _guard = acquire_store_guard(
            orch,
            &store,
            format!("workspace populate verification for {job_id}"),
        );
        let verification = match crate::jj::working_copy_dirty_paths(&jj, worktree_path) {
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
                if still.is_empty() {
                    Ok(())
                } else {
                    Err(crate::git::worktree::SetupError::Spawn {
                        command: "populate exclude verification".to_string(),
                        message: format!(
                            "explicitly-populated gitignored content is still snapshot-visible and could be committed: {}. Refusing to provision the worktree.",
                            still.join(", ")
                        ),
                    })
                }
            }
            Ok(_) => Ok(()),
            Err(error) => Err(crate::git::worktree::SetupError::Spawn {
                command: "populate exclude verification".to_string(),
                message: format!("could not verify populate excludes: {error}"),
            }),
        };
        drop(_guard);
        emit_phase_timing(
            sink,
            job_id,
            issue_id.clone(),
            "populate-verification",
            verification_started.elapsed(),
            &store,
        );
        if let Err(error) = verification {
            cleanup();
            return Err(error);
        }
        if cancel.load(Ordering::SeqCst) {
            cleanup();
            return Err(crate::git::worktree::SetupError::Cancelled);
        }
    }

    let setup_started = std::time::Instant::now();
    if !setup_commands.is_empty() {
        if let Err(error) = crate::git::worktree::run_setup_commands_with_process_streaming(
            process,
            worktree_path,
            &setup_commands,
            sink,
            job_id,
            issue_id.clone(),
            cancel,
            child_slot,
        ) {
            emit_phase_timing(
                sink,
                job_id,
                issue_id.clone(),
                "setup-commands",
                setup_started.elapsed(),
                &store,
            );
            log::error!("Setup commands failed, cleaning up worktree: {error}");
            cleanup();
            return Err(error);
        }
    }
    emit_phase_timing(
        sink,
        job_id,
        issue_id,
        "setup-commands",
        setup_started.elapsed(),
        &store,
    );
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
    fn unproven_collision_never_runs_destructive_cleanup() {
        let cleanup_calls = std::cell::Cell::new(0);
        cleanup_owned_mutation_failure(false, || cleanup_calls.set(cleanup_calls.get() + 1));
        assert_eq!(cleanup_calls.get(), 0);
    }

    #[test]
    fn owned_post_creation_failure_runs_cleanup() {
        let cleanup_calls = std::cell::Cell::new(0);
        cleanup_owned_mutation_failure(true, || cleanup_calls.set(cleanup_calls.get() + 1));
        assert_eq!(cleanup_calls.get(), 1);
    }

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
