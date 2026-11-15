use super::*;

fn changed(path: &str) -> GraphFileChange {
    GraphFileChange {
        path: path.to_string(),
        previous_path: None,
        status: "modified".to_string(),
        additions: 1,
        deletions: 1,
    }
}

#[test]
fn base_bundle_selects_protocol_and_changed_rust_wire_paths() {
    let paths = executor_base_content_paths(
        &test_wire_policy(),
        &[
            changed("src-tauri/cairn-exec/src/main.rs"),
            changed("src-tauri/cairn-exec/Cargo.toml"),
            changed("src/unrelated.rs"),
        ],
    );
    assert!(paths.contains(EXECUTOR_PROTOCOL_SOURCE));
    assert!(paths.contains("src-tauri/cairn-exec/src/main.rs"));
    assert!(paths.contains("src-tauri/cairn-executor/src/main.rs"));
    assert!(!paths.contains("src-tauri/cairn-exec/Cargo.toml"));
    assert!(!paths.contains("src/unrelated.rs"));
}

/// The process env one manual run hands the fleet: the producer's item,
/// through the same wire builder every placement uses. The fleet turns each
/// `*_CONTENT` entry into a cell file (pinned by the fleet's own
/// `check_process_batch` test), and on a remote cell, with no base ref, those
/// files are the only base the script will ever see.
fn manual_process_env(check_name: &str, transport: Vec<(String, String)>) -> Vec<(String, String)> {
    let mut items = manual_check_wire(check_name, transport).items;
    assert_eq!(items.len(), 1);
    items.remove(0).process.env
}

/// A parked check has no admission deadline. However long its wait, it
/// can never become a `Capacity` failure that counts toward the
/// infrastructure breaker; only the requester's own cancellation removes
/// it from the queue, and a manual check's requester is a runner task, not
/// the CLI that asked for it.
#[test]
fn a_parked_check_never_ages_out_of_its_queue() {
    let wire = manual_check_wire("rust-tests", Vec::new());
    assert_eq!(wire.request.wait_horizon_unix_ms, u64::MAX);
    assert_eq!(wire.request.mutation_policy, MutationPolicy::PureVerdict);
}

/// One manual run's batch, through the same wire builder every placement
/// uses.
fn manual_check_wire(
    check_name: &str,
    transport: Vec<(String, String)>,
) -> crate::execution::wire::CheckBatchWire {
    let check: CheckCommand = serde_json::from_value(serde_json::json!({
        "command": "bun run check:executor-protocol",
    }))
    .unwrap();
    let item = PlannedCheckBatchItem {
        command: check.command.clone(),
        stream_id: "manual-stream".into(),
        ..manual_check_batch_item(
            check_name,
            "input",
            &check,
            &crate::execution::check_placement::resolve_check_placement(
                check_name,
                &check,
                None,
                |_| None,
            )
            .unwrap(),
            CheckResourceClass::Shared,
            0,
            InfraScopes::head_only("job", "commit"),
            transport,
        )
    };
    let request = PlannedCheckBatchRequest {
        project_id: "project".into(),
        repository: "/repo".into(),
        store_dir: PathBuf::from("/store"),
        sealed_commit: "commit".into(),
        requesting_job_id: "job".into(),
        owner: cairn_common::executor_protocol::CellOwnerRef {
            project_id: "project".into(),
            project_key: None,
            issue_number: None,
            job_id: Some("job".into()),
            execution_seq: None,
            node_kind: Some("manual-check".into()),
        },
        affinity_key: Some("job".into()),
        priority: CellPriority::ReviewCheck,
        env: Vec::new(),
        items: vec![item],
        run_context: None,
        mutation_policy: MutationPolicy::PureVerdict,
        status_board: None,
        on_group_complete: None,
        purpose: None,
        cell_groups: Default::default(),
        publication: None,
    };
    crate::execution::wire::build_check_batch_wire(
        &request,
        "request".into(),
        "attempt".into(),
        None,
        Vec::new(),
        Vec::new(),
    )
}

#[test]
fn manual_check_batch_ships_changed_files_and_base_content_to_its_cell() {
    let wire_path = "src-tauri/cairn-exec/src/main.rs";
    let bundle = format!("{{\"{EXECUTOR_PROTOCOL_SOURCE}\":\"base\"}}");
    let env = manual_process_env(
        EXECUTOR_PROTOCOL_CHECK,
        check_transport_env(&[changed(wire_path)], Some(bundle.clone())),
    );
    let value = |key: &str| {
        env.iter()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.clone())
    };
    assert_eq!(
        value("CAIRN_CHECK_CHANGED_FILES_CONTENT").as_deref(),
        Some(wire_path)
    );
    assert_eq!(value("CAIRN_CHECK_BASE_CONTENT_CONTENT"), Some(bundle));
}

/// A run whose head is its base (a `--branch main` baseline) still tells the
/// cell so, rather than leaving the script to ask a checkout that cannot say.
#[test]
fn manual_check_with_an_empty_diff_still_ships_the_changed_file_list() {
    let env = manual_process_env("rust-lint", check_transport_env(&[], None));
    assert_eq!(
        env,
        vec![(
            "CAIRN_CHECK_CHANGED_FILES_CONTENT".to_string(),
            String::new()
        )],
        "only the changed-file list rides here, and it rides even when empty"
    );
}

/// cairn/5268: codex-pin's `ensure-sidecar` step failed compiling cairn-orch,
/// a crate the suite never compiles, and that red was served as codex-pin's
/// verdict on trees without the break. The step's marker makes the run a red
/// that is never reusable, whatever the lane's key covers.
#[test]
fn a_failed_prerequisite_step_is_a_red_that_is_never_reused() {
    let command = "bun run codex:pin:check";
    let output = "$ bun run scripts/ensure-sidecar.ts\n\
             Source sidecars missing for x86_64-unknown-linux-gnu — building them first\n\
             error[E0603]: function `unix_time_ms` is private\n\
             \x20   --> cairn-orch/src/executor_supervisor.rs:1483:31\n\
             error: could not compile `cairn-orch` due to 1 previous error\n\
             CAIRN_PREREQUISITE_EXIT: sidecar build exited 101 on fedora\n\
             Cause (verbatim):\n\
             error[E0603]: function `unix_time_ms` is private\n\
             error: script \"codex:pin:check\" exited with code 101";
    let classification =
        classify_check_failure(command, Some(101), false, false, None, output).unwrap();
    assert_eq!(classification.kind, CheckFailureKind::Prerequisite);
    assert!(
        !classification.kind.is_infrastructure(),
        "the machine is not to blame"
    );
    assert!(
        classification.kind.is_verdict_about_the_code(),
        "the tree failed to build, so the run still reads as a red"
    );
    let reuse = check_reuse_decision(command, Some(classification.kind), None);
    assert!(!reuse.reusable);
    assert_eq!(
        reuse.reason.as_deref(),
        Some("a prerequisite step failed before the suite ran")
    );
    // Without the marker the same compile error is an ordinary red, which
    // the reuse decision admits: the marker is what keeps it out.
    let unmarked = output
        .lines()
        .take_while(|line| !line.starts_with("CAIRN_PREREQUISITE_EXIT"))
        .collect::<Vec<_>>()
        .join("\n");
    assert_eq!(
        classify_check_failure(command, Some(101), false, false, None, &unmarked),
        None
    );
    assert!(check_reuse_decision(command, None, None).reusable);
    assert_eq!(
        CheckFailureKind::from_stored(CheckFailureKind::Prerequisite.as_str()),
        Some(CheckFailureKind::Prerequisite)
    );
}

/// The protocol script's own "could not evaluate" line, as it reaches the
/// runner, is a no-verdict infrastructure exit that is never reusable.
#[test]
fn protocol_check_missing_inputs_classify_as_unreusable_infrastructure() {
    let output = "$ bun run scripts/check-executor-protocol.ts\n\
             CAIRN_INFRASTRUCTURE_EXIT: Executor protocol check could not evaluate: \
             no integration base revision could be resolved\n\
             error: script \"check:executor-protocol\" exited with code 75";
    let command = "bun run check:executor-protocol";
    let classification =
        classify_check_failure(command, Some(75), false, false, None, output).unwrap();
    assert_eq!(classification.kind, CheckFailureKind::Infrastructure);
    assert!(classification.kind.is_infrastructure());
    assert!(!check_reuse_decision(command, Some(classification.kind), None).reusable);
}

/// A passing run whose result handoff was refused is a non-verdict: never
/// the suite's red, never its green, and never reusable evidence.
#[test]
fn a_refused_result_handoff_classifies_as_unreusable_infrastructure() {
    let output = "        Summary [   2.104s] 17 tests run: 17 passed, 0 skipped\n\
             CAIRN_HANDOFF_FAILED_EXIT: tests: 17 passed, 0 failed (verdict as computed, exit 0); \
             result handoff refused: EACCES: permission denied at \
             /cells/slot-1/scratch/cairn-check-result-1/result.json\n\
             error: script \"test:rust\" exited with code 74";
    let command = "bun run test:rust:tiered";
    let classification =
        classify_check_failure(command, Some(74), false, false, None, output).unwrap();
    assert_eq!(classification.kind, CheckFailureKind::Infrastructure);
    assert!(!classification.kind.is_verdict_about_the_code());
    assert_eq!(classification.evidence_line, Some(1));
    assert!(!check_reuse_decision(command, Some(classification.kind), None).reusable);

    // The marker under another exit is not the handoff protocol.
    assert_eq!(
        classify_check_failure(command, Some(1), false, false, None, output),
        None
    );
}

/// A lint verdict over a tree with findings depends on its base, so it is
/// never served to another request by input key; a clean tree's is.
#[test]
fn only_a_clean_trees_eslint_verdict_is_reusable() {
    let eslint = |tree: Option<Vec<String>>| ParsedCheckResult {
        schema_version: 1,
        complete: true,
        selection: "unknown".to_string(),
        tests: Vec::new(),
        undeclared_skips: 0,
        parser: ESLINT_PARSER.to_string(),
        passed: 0,
        failed: 0,
        suite_failures: 0,
        skipped: 0,
        deferred: 0,
        failures: Vec::new(),
        report: None,
        phases: None,
        unverified: None,
        lint_findings: Some(crate::execution::check_parsers::LintFindings {
            tree,
            baseline: None,
        }),
        plan_audit: None,
        accounting: None,
        ingestion: None,
    };
    let command = "bun run check:lint";
    assert!(check_reuse_decision(command, None, Some(&eslint(Some(Vec::new())))).reusable);
    let red_base = check_reuse_decision(
        command,
        None,
        Some(&eslint(Some(vec!["eslint|r|f|m".to_string()]))),
    );
    assert!(
        !red_base.reusable,
        "a green over a red base is relative to it"
    );
    assert!(red_base.reason.unwrap().contains("depends on the base"));
    assert!(
        !check_reuse_decision(command, None, Some(&eslint(None))).reusable,
        "an unrecorded set cannot vouch for a clean tree"
    );
}

#[test]
fn only_the_protocol_comparison_needs_base_content() {
    assert!(needs_executor_base_content(EXECUTOR_PROTOCOL_CHECK));
    assert!(!needs_executor_base_content("rust-lint"));
}
