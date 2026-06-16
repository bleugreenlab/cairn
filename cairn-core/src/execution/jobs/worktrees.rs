use super::*;

// ============================================================================
// Private helpers
// ============================================================================

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
        let _ =
            crate::git::worktree::remove_worktree_with_services(git, fs, repo, worktree_path, true);
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
    crate::git::worktree::create_worktree_with_services(
        git,
        fs,
        repo,
        worktree_path,
        branch,
        base_ref,
    )
    .map_err(|message| crate::git::worktree::SetupError::Spawn {
        command: "git worktree add".to_string(),
        message,
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

    // 4. Pre-warm language servers in the background so indexing is under way
    // before the agent's first `~/lsp` query. Best-effort and detached: cloning
    // the orchestrator shares the pooled LSP manager, a worktree with no language
    // markers (or no installed server) spawns nothing, and any failure is
    // confined to this thread.
    let prewarm = orch.clone();
    let prewarm_worktree = worktree_path.to_path_buf();
    std::thread::spawn(move || prewarm.lsp_prewarm(&prewarm_worktree));

    Ok(())
}
