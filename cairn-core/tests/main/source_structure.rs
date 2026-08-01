use std::path::PathBuf;

fn core_source(relative: &str) -> String {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    std::fs::read_to_string(root.join("src").join(relative))
        .unwrap_or_else(|error| panic!("failed to read core source {relative}: {error}"))
}

/// Every source and doc file the promotion ban walks: the whole Rust workspace
/// and the design docs, minus build output and the memory-triage archive (a
/// historical record of what agents once observed, not current canon).
fn banned_symbol_search_roots() -> Vec<PathBuf> {
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../..")
        .canonicalize()
        .expect("repository root resolves from the crate manifest");
    vec![repo.join("src-tauri"), repo.join("docs")]
}

fn walk_source_files(root: &std::path::Path, found: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if path.is_dir() {
            if matches!(name.as_ref(), "target" | "node_modules" | "memory-triage") {
                continue;
            }
            walk_source_files(&path, found);
        } else if matches!(
            path.extension().and_then(|ext| ext.to_str()),
            Some("rs" | "md")
        ) {
            found.push(path);
        }
    }
}

/// Promotion is retired: a `run` item that outlives its bound is killed, and a
/// batch that outlives its grace window suspends and resumes with its finished
/// result. Reclassifying a live child into a terminal is what made publication
/// depend on elapsed time — the same command with the same `commit_msg`
/// committed if it finished fast and was silently voided if it did not. These
/// symbols named that machinery; a reappearance is the defect returning, so the
/// ban is asserted structurally rather than left to review.
#[test]
fn timeout_promotion_never_returns() {
    const BANNED: &[&str] = &[
        "PromotedTerminalProcess",
        "promote_command_process",
        "promote_timeouts",
        "promote_on_timeout",
        "ActivatePromotedTerminal",
        "activate_promoted_executor_terminal",
        "build_promoted_terminal_uri",
        "subscribe_promoted_terminal_exit_wake",
        "command-promoted",
        "promoted_terminal",
    ];
    let this_file = PathBuf::from(file!())
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .expect("this test file has a name");

    let mut files = Vec::new();
    for root in banned_symbol_search_roots() {
        walk_source_files(&root, &mut files);
    }
    assert!(
        files.len() > 100,
        "the promotion ban walked too few files ({}) to be meaningful",
        files.len()
    );

    let mut offenders = Vec::new();
    for file in files {
        if file.file_name().is_some_and(|name| name == &*this_file) {
            continue;
        }
        let Ok(source) = std::fs::read_to_string(&file) else {
            continue;
        };
        for symbol in BANNED {
            if source.contains(symbol) {
                offenders.push(format!("{} contains {symbol}", file.display()));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "timeout promotion reappeared:\n{}",
        offenders.join("\n")
    );
}

/// `jj::seal` resolves a workspace's branch ownership by reading
/// `.jj/cairn-branch` AT CALL TIME. That is sound for a fixture which provisions a
/// workspace and seals it in the same breath, and unsound for the commit barrier,
/// whose seal runs after a batch that can write that very file: a batch which
/// deleted the marker would get a locally committed, UNPUBLISHED seal reported to
/// it as a successful commit on its branch, and one which planted a marker would
/// opt a checkout Cairn does not own into Cairn publication.
///
/// The VCS backend therefore captures ownership when it is constructed during
/// request preflight and passes it to `jj::seal_paths`. Nothing in the type system
/// separates the two entry points — `jj::seal` is `pub`, and reaching for it here
/// would compile and silently reintroduce the defect — so the production edge is
/// banned structurally rather than left to a doc comment and review.
#[test]
fn the_vcs_backend_never_resolves_seal_ownership_at_call_time() {
    // `jj::seal_paths(` does not contain `jj::seal(`, so the ban names the
    // marker-reading entry point exactly, with no carve-out for the file's own
    // fixtures: they pass their branch explicitly too.
    assert_absent("mcp/vcs.rs", &["jj::seal("]);
}

fn assert_absent(relative: &str, forbidden: &[&str]) {
    let source = core_source(relative);
    for needle in forbidden {
        assert!(
            !source.contains(needle),
            "production agent path {relative} must not contain obsolete edge {needle:?}"
        );
    }
}

#[test]
fn agent_job_preparation_has_no_checkout_lifecycle_edges() {
    assert_absent(
        "execution/jobs/lifecycle.rs",
        &[
            "add_workspace",
            "populate_worktree",
            "managed_worktree",
            "worktree_path",
            "owns_ephemeral_worktree",
            "restore_workspace_assignment",
        ],
    );
}

#[test]
fn authenticated_run_identity_never_resolves_from_cwd() {
    assert_absent(
        "mcp/handlers/run_context.rs",
        &[
            "lookup_run_by_cwd",
            "ORDER BY r.created_at DESC",
            "request.cwd",
        ],
    );
}

/// The canon invariant, pinned where it is decided. A search-shaped run item is
/// a read in run's clothing: it must be served *below* head resolution, so it
/// reads the same coordinate a read reads, and *above* both lease acquisition
/// and slot placement, so no served search can admit a cell. Ordering in that
/// function is the entire mechanism, and type checking cannot see it.
#[test]
fn search_interception_sits_below_head_resolution_and_above_admission() {
    let source = core_source("mcp/handlers/run/mod.rs");
    let at = |needle: &str| {
        source
            .find(needle)
            .unwrap_or_else(|| panic!("run dispatch no longer mentions {needle:?}"))
    };
    let head = at("resolve_current_for_read");
    let interception = at("try_run_search_batch");
    let lease = at("acquire_job_residency");
    let placement = at("submit_run_batch");

    assert!(
        head < interception,
        "a served search must read the resolved head coordinate, so interception belongs below head resolution"
    );
    assert!(
        interception < lease && interception < placement,
        "a served search must not be able to take a lease or admit a cell, so interception belongs above both"
    );
}

/// The other half of the same invariant: whatever the seam order says, the
/// serving path itself must have no way to schedule anything.
#[test]
fn served_searches_have_no_scheduling_edges() {
    assert_absent(
        "mcp/handlers/run/search.rs",
        &[
            "acquire_job_residency",
            "submit_run_batch",
            "CellRequest",
            "execution_residency",
            "WorktreeSearchPool",
        ],
    );
}

#[test]
fn logical_reads_cannot_acquire_or_refresh_materializations() {
    for relative in [
        "mcp/handlers/read/file.rs",
        "mcp/handlers/read/object_read.rs",
        "mcp/handlers/read/overlay.rs",
    ] {
        assert_absent(
            relative,
            &[
                "AcquireLease",
                "acquire_lease",
                "RefreshCheckout",
                "refresh_checkout",
                "WorktreeSearchPool",
            ],
        );
    }
}
