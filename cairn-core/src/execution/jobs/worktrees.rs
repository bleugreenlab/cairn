use super::*;

// ============================================================================
// Private helpers
// ============================================================================

fn spawn_seed_worktree_task(
    orch: &Orchestrator,
    worktree_path: PathBuf,
    populate_config: crate::config::project_settings::PopulateConfig,
    job_id: String,
    issue_id: Option<String>,
    sink: setup_progress::SetupSink,
    cancel: Arc<AtomicBool>,
) {
    let fs = orch.services.fs.clone();

    std::thread::spawn(move || {
        if cancel.load(Ordering::SeqCst) {
            log::info!(
                "Skipping background seed for job {job_id}: setup was cancelled before seed start"
            );
            return;
        }

        let result =
            crate::git::worktree::seed_worktree(&*fs, &worktree_path, &populate_config.seed);
        if cancel.load(Ordering::SeqCst) {
            log::info!(
                "Background seed for job {job_id} finished after setup cancellation; suppressing progress emit"
            );
            return;
        }

        match result {
            Ok(result) => {
                let line = format!(
                    "[info] Background seed complete ({} cloned, {} skipped, {} failed)",
                    result.cloned, result.skipped, result.failed
                );
                log::info!("{line}");
                setup_progress::emit(
                    &sink,
                    &job_id,
                    issue_id.clone(),
                    "status",
                    Some("populate"),
                    None,
                    Some(line),
                );
            }
            Err(e) => {
                let line = format!("[info] Background seed failed (continuing): {e}");
                log::warn!("{line}");
                setup_progress::emit(
                    &sink,
                    &job_id,
                    issue_id.clone(),
                    "status",
                    Some("populate"),
                    None,
                    Some(line),
                );
            }
        }
    });
}

/// Create a worktree for a job using the orchestrator's service traits.
#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_worktree_for_job(
    orch: &Orchestrator,
    repo_path: &str,
    worktree_path: &Path,
    branch: &str,
    base_ref: &str,
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
        let _ = crate::jj::forget_workspace(&jj, &store, branch);
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
    let base_rev = crate::jj::resolve_base_rev(&jj, &store, base_ref, |r| {
        git.rev_parse(repo, vec![r.to_string()])
            .ok()
            .filter(|s| !s.is_empty())
    });
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

    // 4. Warm-cache seed entries are deliberately off the startup-critical path.
    // The jj auto-track exclude was installed before population began, so these
    // build artifacts stay out of snapshots even if the agent seals while the
    // background clone is still running. The hard leak gate above is reserved for
    // synchronous copy/symlink population, where secrets such as .env can live.
    if !populate_config.seed.is_empty() {
        let line = format!(
            "[info] Seeding external worktree content in the background ({} entr{})",
            populate_config.seed.len(),
            if populate_config.seed.len() == 1 {
                "y"
            } else {
                "ies"
            }
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
        spawn_seed_worktree_task(
            orch,
            worktree_path.to_path_buf(),
            populate_config.clone(),
            job_id.to_string(),
            issue_id.clone(),
            sink.clone(),
            cancel.clone(),
        );
    }

    Ok(())
}
