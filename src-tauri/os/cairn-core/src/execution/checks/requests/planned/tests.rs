use super::*;

fn request() -> PlannedCheckRequest {
    let plan = crate::execution::selection::CheckPlan {
        name: "fix".into(),
        applies: true,
        command: "echo ok".into(),
        scope: crate::execution::selection::CheckScope::Full,
        resource_class: crate::config::project_settings::CheckResourceClass::Shared,
        verdict_environment_names: vec!["SECRET".into()],
        verdict_platforms: vec!["macos".into()],
        verdict_runtime: Vec::new(),
        verdict_executor_ids: None,
        config_error: None,
    };
    let producer = CheckProducer::new(CheckConsumer::Write, "wave");
    let coordinate =
        crate::execution::cache::SealedCheckCoordinate::new("commit", "commit", "tree", "input");
    let recording = crate::execution::checks::CheckAdoptionRecordingPlan::new(
        &plan,
        &coordinate,
        "j",
        "diagnostic",
        &[],
        &producer,
    );
    PlannedCheckRequest {
        producer,
        project_id: "p".into(),
        repository: "/unused".into(),
        store_dir: PathBuf::from("/unused"),
        sealed_commit: "commit".into(),
        requesting_job_id: "j".into(),
        owner: CellOwnerRef {
            project_id: "p".into(),
            project_key: Some("proj".into()),
            issue_number: None,
            job_id: Some("j".into()),
            execution_seq: None,
            node_kind: None,
        },
        affinity_key: Some("j:fix".into()),
        priority: CellPriority::WriteCheck,
        env: vec![("SECRET".into(), "credential-must-not-persist".into())],
        item: PlannedCheckBatchItem {
            recording_plan: Some(recording),
            index: 0,
            name: plan.name.clone(),
            input_hash: "input".into(),
            resource_identity: crate::execution::wire::CheckResourceIdentityInput::Checkpoint {
                name: "fix".into(),
                command: plan.command.clone(),
            },
            command: plan.command.clone(),
            stream_id: "opaque".into(),
            env: vec![
                ("SECRET".into(), "credential-must-not-persist".into()),
                ("CAIRN_CHECK_CHANGED_FILES_CONTENT".into(), "file.rs".into()),
            ],
            verdict_environment_names: plan.verdict_environment_names,
            timeout_ms: 0,
            executor: None,
            verdict_platforms: plan.verdict_platforms,
            resource_class: plan.resource_class,
            infra_scopes: crate::execution::cache::InfraScopes::head_only("j", "commit"),
            avoid_executor_ids: Vec::new(),
        },
        run_context: None,
        status_board: None,
        on_complete: None,
        purpose: None,
        publication: None,
    }
}

fn returned() -> Returned {
    Returned {
        results: HashMap::from([(
            0,
            Ok(crate::mcp::handlers::run::CheckExecResult {
                exit_code: Some(0),
                output: "ok".into(),
                timed_out: false,
                duration_ms: Some(1),
                provenance: None,
                publication: None,
            }),
        )]),
        identity: Some(("physical".into(), "attempt".into())),
        delta: Some(crate::fleet::MutationDelta {
            base_commit: "commit".into(),
            delta_commit: "rewrite".into(),
            upload_receipt: None,
        }),
    }
}

async fn accepted(orch: &Orchestrator, key: &str, phase: WriteLanePhase) -> String {
    let request = request();
    let snapshot = Snapshot::capture(&request, phase).unwrap();
    let c = Context {
        write_lane: snapshot,
        returned: None,
        recorded: false,
        fold: None,
        final_recording: None,
        terminal: None,
    };
    let spec = CheckRequestSpec {
        consumer: CheckConsumer::Write,
        cadence: Some("write"),
        priority: CellPriority::WriteCheck,
        project_id: "p".into(),
        project_key: "proj".into(),
        suite: "fix".into(),
        revision: Some("commit".into()),
        subject_job_id: "j".into(),
        placement: None,
        retry: false,
        requester_ref: "wave".into(),
        requester_job_id: Some("j".into()),
    };
    let id = filing_id("p", key);
    accept_with_context(
        &orch.db.local,
        &spec,
        "fix",
        &Coordinate::Commit("commit".into()),
        Some(&id),
        Some(&serde_json::to_string(&c).unwrap()),
    )
    .await
    .unwrap();
    id
}

#[tokio::test]
async fn durable_write_phase_identity_is_a_resource_uuid_and_never_joins_another_phase() {
    let (_root, orch) = super::super::tests::row_fixture().await;
    let first = accepted(&orch, "wave:fixer", WriteLanePhase::Fixer).await;
    let convergence = accepted(&orch, "wave:convergence", WriteLanePhase::Convergence).await;
    assert!(uuid::Uuid::parse_str(&first).is_ok());
    assert_eq!(filing_id("rekeyed-project", "wave:fixer"), first);
    assert_ne!(first, convergence);
    let row = load_request_by_id(&orch.db.local, &first)
        .await
        .unwrap()
        .unwrap();
    settle(
        &orch,
        &orch.db.local,
        &row,
        CheckRequestAnswer::Cancelled {
            reason: "wave ended".into(),
        },
    )
    .await;
    assert_eq!(
        accepted(&orch, "wave:fixer", WriteLanePhase::Fixer).await,
        first
    );
    let count = orch
        .db
        .local
        .query_opt(
            "SELECT COUNT(*) FROM check_requests WHERE consumer='write'",
            (),
            |row| row.i64(0),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(count, 2);
}

#[tokio::test]
async fn returned_fixer_checkpoint_replays_delta_and_cannot_fold_a_convergence_phase() {
    let (_root, orch) = super::super::tests::row_fixture().await;
    let id = accepted(&orch, "wave:fixer", WriteLanePhase::Fixer).await;
    let serialized = orch
        .db
        .local
        .query_text(
            "SELECT context_json FROM check_requests WHERE id=?1",
            (id.clone(),),
        )
        .await
        .unwrap()
        .unwrap();
    assert!(!serialized.contains("credential-must-not-persist"));
    assert!(serialized.contains("CAIRN_CHECK_CHANGED_FILES_CONTENT"));
    checkpoint(&orch.db.local, &id, "returned", &returned())
        .await
        .unwrap();
    // Crash after durable result, before publication. No fleet exists to rerun it.
    drive_inner(&orch, orch.db.local.clone(), &id, None)
        .await
        .unwrap();
    let c = context(&orch.db.local, &id).await.unwrap().unwrap();
    let replay = c.returned.unwrap().outcome(&orch, &id, &c.write_lane);
    assert_eq!(replay.delta.unwrap().delta_commit, "rewrite");
    assert_eq!(replay.request.unwrap().request_id, "physical");
    assert_eq!(
        load_request_by_id(&orch.db.local, &id)
            .await
            .unwrap()
            .unwrap()
            .state,
        "accepted"
    );
    let fold = Ok(crate::mcp::handlers::run::PublishedSlotDelta {
        commit: "folded".into(),
        patch: "patch".into(),
        paths: vec!["file.rs".into()],
        landed_on_tested_base: true,
    });
    record_planned_fold(&orch, "p", "wave:fixer", &fold)
        .await
        .unwrap();
    record_planned_fold(&orch, "p", "wave:fixer", &fold)
        .await
        .unwrap();
    assert_eq!(
        load_planned_fold(&orch, "p", "wave:fixer")
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .commit,
        "folded"
    );
    let convergence = accepted(&orch, "wave:convergence", WriteLanePhase::Convergence).await;
    checkpoint(&orch.db.local, &convergence, "returned", &returned())
        .await
        .unwrap();
    assert!(record_planned_fold(&orch, "p", "wave:convergence", &fold)
        .await
        .unwrap_err()
        .contains("only the original fixer"));
    // Crash after fold checkpoint, before successor: replay still returns the
    // same physical mutation, and the caller consumes the existing fold.
    drive_inner(&orch, orch.db.local.clone(), &id, None)
        .await
        .unwrap();
    assert_eq!(
        context(&orch.db.local, &id)
            .await
            .unwrap()
            .unwrap()
            .returned
            .unwrap()
            .identity
            .unwrap()
            .0,
        "physical"
    );
}

#[tokio::test]
async fn a_recorded_checkpoint_finishes_settlement_without_executing_or_recording_again() {
    let (_root, orch) = super::super::tests::row_fixture().await;
    let id = accepted(&orch, "wave:fixer", WriteLanePhase::Fixer).await;
    checkpoint(&orch.db.local, &id, "returned", &returned())
        .await
        .unwrap();
    let terminal:crate::execution::checks::ManualConfiguredCheckResult=serde_json::from_value(serde_json::json!({
        "checkName":"fix","commitSha":"commit","treeHash":"tree","inputHash":"input","environmentFingerprint":"",
        "observationRef":null,"ranAt":null,"reusable":false,"notRecorded":"no verdict","disposition":"fresh",
        "passed":false,"verified":false,"unverifiedReason":null,"tier":null,"exitCode":null,"noVerdict":null,
        "outputTail":"","executor":null,"verdictPlatform":null,"runtime":null,"placementNote":null,"placementUnmet":null,"capture":null
    })).unwrap();
    checkpoint(&orch.db.local, &id, "terminal", &terminal)
        .await
        .unwrap();
    checkpoint(&orch.db.local, &id, "recorded", &true)
        .await
        .unwrap();
    // Crash after checkpoint, before row settlement.
    drive_inner(&orch, orch.db.local.clone(), &id, None)
        .await
        .unwrap();
    drive_inner(&orch, orch.db.local.clone(), &id, None)
        .await
        .unwrap();
    let row = load_request_by_id(&orch.db.local, &id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.state, "recorded");
    assert!(row.outcome_json.is_some());
    assert_eq!(
        orch.db
            .local
            .query_opt(
                "SELECT COUNT(*) FROM check_result_observations",
                (),
                |row| row.i64(0)
            )
            .await
            .unwrap()
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn granted_write_without_grant_evidence_is_not_replaced_or_declared_lost() {
    let (_root, orch) = super::super::tests::row_fixture().await;
    let id = accepted(&orch, "missing-grant", WriteLanePhase::Fixer).await;
    orch.db.local.execute("UPDATE check_requests SET state='running',request_id='missing-physical',executor='offline-host' WHERE id=?1",
        (id.clone(),)).await.unwrap();
    let error = drive_inner(&orch, orch.db.local.clone(), &id, None)
        .await
        .unwrap_err();
    assert!(error.contains("waiting to learn"), "{error}");
    let row = load_request_by_id(&orch.db.local, &id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.state, "running");
    assert!(context(&orch.db.local, &id)
        .await
        .unwrap()
        .unwrap()
        .returned
        .is_none());
    assert_eq!(
        orch.db
            .local
            .query_opt(
                "SELECT COUNT(*) FROM check_requests WHERE consumer='write'",
                (),
                |row| row.i64(0)
            )
            .await
            .unwrap()
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn legacy_unknown_phase_discards_its_delta_and_reuses_one_successor_obligation() {
    let (_root, orch) = super::super::tests::row_fixture().await;
    let id = accepted(&orch, "legacy-phase", WriteLanePhase::LegacyFixer).await;
    checkpoint(&orch.db.local, &id, "returned", &returned())
        .await
        .unwrap();
    assert!(record_planned_fold(
        &orch,
        "p",
        "legacy-phase",
        &Ok(crate::mcp::handlers::run::PublishedSlotDelta {
            commit: "wrong".into(),
            patch: String::new(),
            paths: Vec::new(),
            landed_on_tested_base: true,
        })
    )
    .await
    .is_err());
    // The durable successor snapshot already exists at this crash boundary.
    // Repeated retirement links it; it cannot produce a second fresh request.
    checkpoint(
        &orch.db.local,
        &id,
        "legacy_successor_wave",
        &"one-successor",
    )
    .await
    .unwrap();
    let row = load_request_by_id(&orch.db.local, &id)
        .await
        .unwrap()
        .unwrap();
    let c = context(&orch.db.local, &id).await.unwrap().unwrap();
    finish_legacy(&orch, &orch.db.local, &row, &c)
        .await
        .unwrap();
    finish_legacy(&orch, &orch.db.local, &row, &c)
        .await
        .unwrap();
    let settled = load_request_by_id(&orch.db.local, &id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(settled.state, "cancelled");
    let reason = settled.evidence.unwrap();
    assert!(reason.contains(LEGACY_PHASE_UNKNOWN));
    assert!(reason.contains("one-successor"));
    assert!(load_planned_fold(&orch, "p", "legacy-phase")
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        context(&orch.db.local, &id)
            .await
            .unwrap()
            .unwrap()
            .returned
            .unwrap()
            .delta
            .unwrap()
            .delta_commit,
        "rewrite"
    );
}

#[tokio::test]
async fn returned_delta_checkpoint_rejects_a_different_second_result() {
    let (_root, orch) = super::super::tests::row_fixture().await;
    let id = accepted(&orch, "immutable-return", WriteLanePhase::Fixer).await;
    let first = returned();
    checkpoint(&orch.db.local, &id, "returned", &first)
        .await
        .unwrap();
    checkpoint(&orch.db.local, &id, "returned", &first)
        .await
        .unwrap();
    let mut different = first;
    different.delta.as_mut().unwrap().delta_commit = "another-rewrite".into();
    assert!(checkpoint(&orch.db.local, &id, "returned", &different)
        .await
        .is_err());
    assert_eq!(
        context(&orch.db.local, &id)
            .await
            .unwrap()
            .unwrap()
            .returned
            .unwrap()
            .delta
            .unwrap()
            .delta_commit,
        "rewrite"
    );
}

#[tokio::test]
async fn write_request_settles_lost_only_from_its_granted_death_evidence() {
    let (_root, orch) = super::super::tests::row_fixture().await;
    let id = accepted(&orch, "death-evidence", WriteLanePhase::Fixer).await;
    let claim = crate::storage::check_lane_adoptions::Claim {
        request_id: "dead-physical".into(),
        attempt_id: "attempt".into(),
        project_id: "p".into(),
        check_names: vec!["fix".into()],
        platform: "macos".into(),
        commit_sha: "commit".into(),
        input_hashes: vec!["input".into()],
        recording_plans: Vec::new(),
        consumer: "write".into(),
        consumer_ref: id.clone(),
        executor_id: "executor".into(),
        machine: "dead-host".into(),
        granted_at_ms: 1,
        state: "lost".into(),
        stopped_at_ms: Some(2),
        evidence: Some("executor enrollment removed".into()),
        observation_id: None,
        updated_ms: 2,
    };
    crate::storage::check_lane_adoptions::insert(&orch.db.local, &claim)
        .await
        .unwrap();
    orch.db
        .local
        .execute(
            "UPDATE check_requests SET state='running',request_id='dead-physical' WHERE id=?1",
            (id.clone(),),
        )
        .await
        .unwrap();
    drive_inner(&orch, orch.db.local.clone(), &id, None)
        .await
        .unwrap();
    let row = load_request_by_id(&orch.db.local, &id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.state, "lost");
    let finalized = finalize_planned_check_request(&orch, "death-evidence", &request())
        .await
        .unwrap();
    assert!(finalized.results.is_empty());
    assert!(finalized.delta.is_none());
    assert!(row
        .evidence
        .unwrap()
        .contains("executor enrollment removed"));
    assert!(context(&orch.db.local, &id)
        .await
        .unwrap()
        .unwrap()
        .returned
        .is_none());
}

#[test]
fn coalesced_return_checkpoint_retains_the_physical_publication_not_subscriber_identity() {
    let mut result = returned().results.remove(&0).unwrap().unwrap();
    result.publication = Some(crate::fleet::PublicationCoordination::for_request((
        "leader".into(),
        "grant".into(),
    )));
    let outcome = PlannedCheckBatchOutcome {
        results: HashMap::from([(0, Ok(result))]),
        request: None,
        delta: None,
        store_dir: None,
    };
    let captured = Returned::capture(&outcome);
    assert_eq!(captured.identity, Some(("leader".into(), "grant".into())));
    let json = serde_json::to_string(&captured).unwrap();
    let recovered: Returned = serde_json::from_str(&json).unwrap();
    assert_eq!(recovered.identity, captured.identity);
}
