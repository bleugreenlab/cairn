//! Remote retained results must earn their acknowledgement through the recorder.
use super::*;
use crate::db::DbState;
use crate::execution::check_placement::EffectiveCheckPlacement;
use crate::execution::selection::CheckScope;
use crate::mcp::handlers::run::adopted::{adopt, Adoption};
use crate::services::testing::TestServicesBuilder;
use crate::storage::{check_lane_adoptions as claims, RowExt, SearchIndex};
use cairn_common::executor_protocol::{ExecutorMessage, ProcessBatchItemOutcome};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::mpsc;

const REMOTE: &str = "retained-check-remote";
const REQUEST: &str = "retained-check-request";
const ATTEMPT: &str = "retained-check-attempt";

pub(super) fn claim(state: &str, consumer: &str) -> claims::Claim {
    claims::Claim {
        request_id: REQUEST.into(),
        attempt_id: ATTEMPT.into(),
        project_id: "project-a".into(),
        check_names: vec!["smoke".into()],
        platform: "linux".into(),
        commit_sha: "original-commit".into(),
        input_hashes: vec!["original-input".into()],
        recording_plans: vec![recording_plan(
            "smoke",
            "original-input",
            "main:project-a",
            "main-cadence:project-a:smoke:linux",
        )],
        consumer: consumer.into(),
        consumer_ref: "main-cadence:project-a:smoke:linux".into(),
        executor_id: REMOTE.into(),
        machine: REMOTE.into(),
        granted_at_ms: 10,
        state: state.into(),
        stopped_at_ms: (state != "running").then_some(20),
        evidence: None,
        observation_id: None,
        updated_ms: chrono::Utc::now().timestamp_millis(),
    }
}

fn waiter(claim: &claims::Claim) -> claims::Waiter {
    claims::Waiter {
        request_id: claim.request_id.clone(),
        attempt_id: claim.attempt_id.clone(),
        check_name: claim.check_names[0].clone(),
        item_index: 0,
        consumer: claim.consumer.clone(),
        consumer_ref: claim.consumer_ref.clone(),
        state: claim.state.clone(),
        observation_id: None,
        evidence: None,
        updated_ms: claim.updated_ms,
    }
}

async fn runner(home: &Path, claim: &claims::Claim) -> Orchestrator {
    let local = Arc::new(
        LocalDb::open_migrated_for_test(home.join("runner.db"))
            .await
            .unwrap(),
    );
    claims::insert_grant(&local, claim, &[waiter(claim)])
        .await
        .unwrap();
    boot(home, local).await
}

pub(super) async fn boot(home: &Path, local: Arc<LocalDb>) -> Orchestrator {
    let search = Arc::new(SearchIndex::open_or_create(home.join("search")).unwrap());
    let orch = Orchestrator::builder(
        Arc::new(DbState::new(local, search)),
        Arc::new(TestServicesBuilder::new().build()),
        home.to_path_buf(),
    )
    .jj_binary_path(crate::jj::tests::jj_bin().unwrap_or_else(|| "jj".into()))
    .build();
    orch.fleet.await_restart_adoptions_loaded().await;
    orch
}

pub(super) fn result(orch: &Orchestrator) -> crate::fleet::UnboundResult {
    let environment = cairn_checkenv::verdict_environment_hash(Vec::<String>::new(), |_| None);
    let item = ProcessBatchItemOutcome {
        header: "smoke".into(),
        body: "remote smoke passed".into(),
        succeeded: true,
        suspended: false,
        images: vec![],
        exit_code: Some(0),
        timed_out: false,
        started_at_unix_ms: 10,
        finished_at_unix_ms: 30,
        duration_ms: 20,
        peak_rss_bytes: None,
        peak_process: None,
        cpu: None,
        granted_units: None,
        disk_delta_bytes: None,
        sandbox_denials: vec![],
        tracked_modifications: None,
        environment_fingerprint: "remote-environment".into(),
        verdict_platform: Some("linux".into()),
        verdict_arch: Some("x86_64".into()),
        toolchain_fingerprint: Some(check_toolchain_identity().into()),
        verdict_environment_hash: Some(environment.clone()),
        stop: None,
        build_jobs: None,
        unrecorded_cache_checkpoint: None,
        build_job_processes: None,
    };
    let mut metadata = orch.fleet.execution_meta_for_test();
    // Executors leave authenticated identity stamping to the runner link.
    metadata.executor_id.clear();
    metadata.executor_device_id.clear();
    metadata.executor_connection_generation = 1;
    metadata.environment_fingerprint = "remote-environment".into();
    metadata.verdict_platform = Some("linux".into());
    metadata.verdict_arch = Some("x86_64".into());
    metadata.toolchain_fingerprint = Some(check_toolchain_identity().into());
    metadata.verdict_environment_hash = Some(environment);
    crate::fleet::UnboundResult {
        executor_id: REMOTE.into(),
        machine: REMOTE.into(),
        request_id: REQUEST.into(),
        attempt_id: ATTEMPT.into(),
        outcome: CellOutcome::Completed {
            request_id: REQUEST.into(),
            attempt_id: ATTEMPT.into(),
            exit_code: Some(0),
            output: serde_json::to_string(&[item]).unwrap(),
            timed_out: false,
            metadata,
            mutation_delta: None,
            sandbox_denials: vec![],
            tracked_modifications: None,
            node_scratch: None,
        },
    }
}

fn contract(orch: &Orchestrator, home: &Path) -> CheckContractSnapshot {
    let configured: CheckCommand = serde_yaml::from_str("command: echo smoke\n").unwrap();
    CheckContractSnapshot {
        context: ManualCheckCacheContext {
            project_id: "project-a".into(),
            project_key: "PA".into(),
            job_id: "main:project-a".into(),
            commit_sha: "original-commit".into(),
            tree_hash: "original-tree".into(),
            input_hash: "original-input".into(),
            cacheable: true,
            entry: None,
        },
        sealed_coordinate: crate::execution::cache::SealedCheckCoordinate::new(
            "original-commit",
            "original-commit",
            "original-tree",
            "original-input",
        ),
        repository_path: home.to_string_lossy().into_owned(),
        change: BaseRelativeChange {
            jj: crate::jj::environment(&orch.jj_binary_path, &orch.config_dir),
            repository: home.to_path_buf(),
            base: "original-base".into(),
            head: "original-commit".into(),
            target: "main".into(),
            changed: vec![],
            base_entries: None,
            base_findings: None,
        },
        defined_by_commit: "original-commit".into(),
        configured_check: configured,
        placement: EffectiveCheckPlacement {
            selector: None,
            verdict_platforms: vec!["linux".into()],
            executor_name: None,
            note: None,
        },
        plan: CheckPlan {
            name: "smoke".into(),
            applies: true,
            command: "echo smoke".into(),
            scope: CheckScope::Full,
            resource_class: crate::config::project_settings::CheckResourceClass::Shared,
            verdict_environment_names: vec![],
            verdict_executor_ids: None,
            config_error: None,
            verdict_platforms: vec!["linux".into()],
            verdict_runtime: vec![],
        },
        timeout_ms: 1000,
        capture: None,
    }
}

fn producer() -> CheckRunProducer {
    CheckRunProducer {
        adopt: Some(AdoptedAttempt {
            request_id: REQUEST.into(),
            attempt_id: ATTEMPT.into(),
            executor_id: REMOTE.into(),
            item_index: 0,
            item_count: 1,
        }),
        tool_use_id: "main-cadence:project-a:smoke:linux".into(),
        node_kind: "main-check".into(),
        owner_job_id: None,
        purpose: Some(crate::fleet::purpose::PurposeKey::main_cadence(
            "project-a",
            "smoke",
            "linux",
        )),
    }
}

pub(super) fn assert_no_release(frames: &mut mpsc::UnboundedReceiver<ExecutorMessage>) {
    while let Ok(frame) = frames.try_recv() {
        assert!(
            !matches!(
                frame,
                ExecutorMessage::ResultAck { .. } | ExecutorMessage::Cancel { .. }
            ),
            "retained attempt released before a durable decision: {frame:?}"
        );
    }
}

pub(super) async fn project_db(home: &Path) -> Arc<LocalDb> {
    let db = Arc::new(
        LocalDb::open_migrated_for_test(home.join("project.db"))
            .await
            .unwrap(),
    );
    db.execute(
        "INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
        VALUES('project-a','default','A','PA','/fixture',1,1)",
        (),
    )
    .await
    .unwrap();
    db
}

async fn adopted_result_is_recorded_before_ack(state: &str) {
    let home = tempfile::tempdir().unwrap();
    let orch = runner(home.path(), &claim(state, "main_cadence")).await;
    let mut frames = orch.fleet.attach_unreported_machine_for_test(REMOTE);
    let retained = result(&orch);

    // The remote resend arrives while the project's recorder database is not open.
    assert!(!home.path().join("project.db").exists());
    orch.fleet.handle_executor_message(
        REMOTE,
        1,
        ExecutorMessage::Result {
            request_id: REQUEST.into(),
            attempt_id: ATTEMPT.into(),
            outcome: retained.outcome.clone(),
        },
    );
    assert_eq!(adopt(&orch, retained).await, Adoption::HeldForCheckLane);
    // Executors resend retained results before their opening account. An empty
    // account must not turn the already-held result into lost work.
    orch.fleet
        .apply_executor_snapshot(REMOTE, 1, Default::default(), Default::default());
    assert_no_release(&mut frames);

    let db = project_db(home.path()).await;
    let observed = Arc::new(AtomicUsize::new(0));
    let probe_count = observed.clone();
    let probe_db = db.clone();
    let local = orch.db.local.clone();
    orch.fleet
        .set_result_ack_probe_for_test(Arc::new(move |request, attempt| {
            assert_eq!((request, attempt), (REQUEST, ATTEMPT));
            // This runs synchronously BEFORE the frame is sent. A separate checkout
            // cannot see the writer's uncommitted row, unlike checking after recv().
            let (observations, cached, settled) = crate::storage::run_db_blocking(|| async {
                let observations = probe_db.query_opt(
                "SELECT COUNT(*) FROM check_result_observations WHERE project_id='project-a'
                 AND commit_sha='original-commit' AND check_name='smoke' AND verdict='passed'
                 AND executor_id='retained-check-remote'
                 AND executor_device_id='device-retained-check-remote'",
                (), |row| row.i64(0)).await.map_err(|e| e.to_string())?.unwrap();
                let cached = probe_db
                    .query_opt(
                        "SELECT COUNT(*) FROM check_result_cache WHERE project_id='project-a'
                 AND input_hash='original-input' AND check_name='smoke' AND passed=1",
                        (),
                        |row| row.i64(0),
                    )
                    .await
                    .map_err(|e| e.to_string())?
                    .unwrap();
                let settled = claims::lookup(&local, REQUEST, ATTEMPT)
                    .await
                    .map_err(|e| e.to_string())?
                    .unwrap();
                Ok((observations, cached, settled))
            })
            .unwrap();
            assert_eq!(
                observations, 1,
                "the observation must commit before ResultAck"
            );
            assert_eq!(cached, 1, "the verdict cache must commit before ResultAck");
            assert_eq!(settled.state, "recorded");
            assert!(settled.observation_id.is_some());
            probe_count.fetch_add(1, Ordering::SeqCst);
        }));
    let outcome = execute_check_contract(
        &orch,
        db.clone(),
        contract(&orch, home.path()),
        producer(),
        true,
        Arc::new(|_| {}),
    )
    .await
    .unwrap();
    assert!(outcome.observation_id.is_some(), "{outcome:?}");
    assert_no_release(&mut frames);
    let pending = claims::waiters(&orch.db.local, REQUEST, ATTEMPT)
        .await
        .unwrap();
    assert_eq!(pending[0].state, "recorded");
    orch.fleet
        .deliver_check_waiter(&pending[0], outcome.observation_id.as_deref().unwrap())
        .await
        .unwrap();
    assert_eq!(observed.load(Ordering::SeqCst), 1);
    let mut ack_count = 0;
    while let Ok(frame) = frames.try_recv() {
        match frame {
            ExecutorMessage::ResultAck {
                request_id,
                attempt_id,
            } => {
                assert_eq!(
                    (request_id.as_str(), attempt_id.as_str()),
                    (REQUEST, ATTEMPT)
                );
                ack_count += 1;
            }
            ExecutorMessage::Offer { .. }
            | ExecutorMessage::Granted { .. }
            | ExecutorMessage::Cancel { .. } => {
                panic!("adopting must not dispatch or cancel: {frame:?}")
            }
            _ => {}
        }
    }
    assert_eq!(ack_count, 1);
}

#[tokio::test]
async fn retained_check_before_project_open_records_row_and_cache_before_ack_frame() {
    adopted_result_is_recorded_before_ack("awaiting_adoption").await;
}

#[tokio::test]
async fn abrupt_running_claim_uses_the_same_held_result_recorder_and_ack_order() {
    adopted_result_is_recorded_before_ack("running").await;
}

#[tokio::test]
async fn noncadence_stray_result_commits_loss_before_ack_frame() {
    let home = tempfile::tempdir().unwrap();
    let orch = runner(home.path(), &claim("cancelled", "turn_end")).await;
    let mut frames = orch.fleet.attach_reported_machine_for_test(REMOTE, &[]);
    let local = orch.db.local.clone();
    let probes = Arc::new(AtomicUsize::new(0));
    let seen = probes.clone();
    orch.fleet
        .set_result_ack_probe_for_test(Arc::new(move |request, attempt| {
            let claim = crate::storage::run_db_blocking(|| async {
                claims::lookup(&local, request, attempt)
                    .await
                    .map_err(|e| e.to_string())
            })
            .unwrap()
            .unwrap();
            assert_eq!(claim.state, "lost");
            assert!(claim.evidence.unwrap().contains("turn_end"));
            seen.fetch_add(1, Ordering::SeqCst);
        }));
    assert_eq!(adopt(&orch, result(&orch)).await, Adoption::Unclaimed);
    assert_eq!(probes.load(Ordering::SeqCst), 1);
    assert!(matches!(
        frames.try_recv(),
        Ok(ExecutorMessage::ResultAck { .. })
    ));
    assert_no_release(&mut frames);
}

#[tokio::test]
async fn unreadable_claim_store_is_undecided_and_sends_no_ack() {
    let home = tempfile::tempdir().unwrap();
    let orch = runner(home.path(), &claim("running", "main_cadence")).await;
    let mut frames = orch.fleet.attach_reported_machine_for_test(REMOTE, &[]);
    // Schema sabotage is confined to this test's disposable database.
    orch.db
        .local
        .execute(
            "ALTER TABLE check_lane_adoptions RENAME TO unavailable_claims",
            (),
        )
        .await
        .unwrap();
    assert_eq!(adopt(&orch, result(&orch)).await, Adoption::Undecided);
    assert_no_release(&mut frames);
}

#[tokio::test]
async fn replay_after_observation_commit_before_claim_commit_reuses_the_original_observation() {
    let home = tempfile::tempdir().unwrap();
    let handle = uuid::Uuid::new_v4().to_string();
    let original_stream = format!("manual-check:original-run:handle={handle}:0");
    let mut physical = claim("running", "main_cadence");
    physical.recording_plans = vec![recording_plan(
        "smoke",
        "original-input",
        "main:project-a",
        &original_stream,
    )];
    let orch = runner(home.path(), &physical).await;
    let mut frames = orch.fleet.attach_unreported_machine_for_test(REMOTE);
    let retained = result(&orch);
    orch.fleet.handle_executor_message(
        REMOTE,
        1,
        ExecutorMessage::Result {
            request_id: REQUEST.into(),
            attempt_id: ATTEMPT.into(),
            outcome: retained.outcome,
        },
    );
    orch.fleet
        .apply_executor_snapshot(REMOTE, 1, Default::default(), Default::default());
    let db = project_db(home.path()).await;
    db.execute(
        "INSERT INTO manual_check_handles(handle,project_id,created_at) VALUES(?1,'project-a',1)",
        (handle.clone(),),
    )
    .await
    .unwrap();
    orch.db
        .local
        .execute_script(
            "CREATE TRIGGER fail_claim_commit BEFORE UPDATE OF state ON check_lane_adoption_waiters
         WHEN NEW.state='recorded' BEGIN
         SELECT RAISE(ABORT,'fixture crash after observation commit'); END;",
        )
        .await
        .unwrap();
    let (_retry_guard, mut retries) =
        observe_retry(format!("remote-check:{REQUEST}:{ATTEMPT}:smoke"));
    let first_orch = orch.clone();
    let first_db = db.clone();
    let first_contract = contract(&orch, home.path());
    let first = tokio::spawn(async move {
        execute_check_contract(
            &first_orch,
            first_db,
            first_contract,
            producer(),
            true,
            Arc::new(|_| {}),
        )
        .await
    });
    let failure = tokio::time::timeout(std::time::Duration::from_secs(20), retries.recv())
        .await
        .expect("the waiter commit must actually be rejected after observation commit")
        .expect("the retry listener stays installed");
    assert!(
        failure.contains("fixture crash after observation commit"),
        "{failure}"
    );
    assert!(
        !first.is_finished(),
        "persistence failures must retain the recorder for retry"
    );
    assert_no_release(&mut frames);
    let original_id = db
        .query_opt_text(
            "SELECT id FROM check_result_observations WHERE check_name='smoke'",
            (),
        )
        .await
        .unwrap()
        .expect("the recorder committed before the claim failure");
    let receipt = db
        .query_opt_text(
            "SELECT observation_id FROM manual_check_handles WHERE handle=?1",
            (handle.clone(),),
        )
        .await
        .unwrap();
    assert_eq!(receipt.as_deref(), Some(original_id.as_str()));
    db.execute(
        "UPDATE manual_check_handles SET response_json='{}' WHERE handle=?1",
        (handle.clone(),),
    )
    .await
    .unwrap();
    let persisted = claims::lookup(&orch.db.local, REQUEST, ATTEMPT)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(persisted.state, "recorded");
    first.abort();
    assert!(
        first.await.unwrap_err().is_cancelled(),
        "the first recorder is killed at the crash window"
    );
    assert_no_release(&mut frames);

    let local = orch.db.local.clone();
    local
        .execute("DROP TRIGGER fail_claim_commit", ())
        .await
        .unwrap();
    // A fresh fleet must discover the unfinished claim, not an in-memory waiter
    // or a cache-only short circuit from the previous recorder.
    let restarted_home = home.path().join("restarted");
    std::fs::create_dir_all(&restarted_home).unwrap();
    let restarted = boot(&restarted_home, local).await;
    let mut restarted_frames = restarted.fleet.attach_unreported_machine_for_test(REMOTE);
    restarted.fleet.handle_executor_message(
        REMOTE,
        1,
        ExecutorMessage::Result {
            request_id: REQUEST.into(),
            attempt_id: ATTEMPT.into(),
            outcome: result(&restarted).outcome,
        },
    );
    restarted
        .fleet
        .apply_executor_snapshot(REMOTE, 1, Default::default(), Default::default());
    assert_no_release(&mut restarted_frames);
    let probe_db = db.clone();
    let ack_count = Arc::new(AtomicUsize::new(0));
    let ack_seen = ack_count.clone();
    restarted
        .fleet
        .set_result_ack_probe_for_test(Arc::new(move |_, _| {
            let count = crate::storage::run_db_blocking(|| async {
                probe_db
                    .query_opt(
                        "SELECT COUNT(*) FROM check_result_observations",
                        (),
                        |row| row.i64(0),
                    )
                    .await
                    .map_err(|error| error.to_string())
            })
            .unwrap()
            .unwrap();
            assert_eq!(
                count, 1,
                "replay must not append a second immutable observation"
            );
            ack_seen.fetch_add(1, Ordering::SeqCst);
        }));
    // Replay the frozen original producer, including its settled receipt,
    // rather than substituting the current cadence caller's stream.
    let replay = execute_adopted_check_item(&restarted, db.clone(), &persisted, 0)
        .await
        .unwrap();
    assert_eq!(replay.observation_id.as_deref(), Some(original_id.as_str()));
    let receipt = db
        .query_opt(
            "SELECT observation_id,response_json FROM manual_check_handles WHERE handle=?1",
            (handle,),
            |row| Ok((row.text(0)?, row.text(1)?)),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        receipt,
        (original_id.clone(), "{}".into()),
        "replay must preserve the settled original receipt"
    );
    assert_no_release(&mut restarted_frames);
    let pending = claims::waiters(&restarted.db.local, REQUEST, ATTEMPT)
        .await
        .unwrap();
    restarted
        .fleet
        .deliver_check_waiter(&pending[0], &original_id)
        .await
        .unwrap();
    assert_eq!(ack_count.load(Ordering::SeqCst), 1);
}

fn recording_plan(name: &str, input: &str, job: &str, stream: &str) -> serde_json::Value {
    let plan = CheckPlan {
        name: name.into(),
        applies: true,
        command: format!("echo {name}"),
        scope: CheckScope::Full,
        resource_class: crate::config::project_settings::CheckResourceClass::Shared,
        verdict_environment_names: vec![],
        verdict_executor_ids: None,
        config_error: None,
        verdict_platforms: vec!["linux".into()],
        verdict_runtime: vec![],
    };
    let coordinate = crate::execution::cache::SealedCheckCoordinate::new(
        "original-commit",
        "original-commit",
        "original-tree",
        input,
    );
    serde_json::to_value(CheckAdoptionRecordingPlan::new(
        &plan,
        &coordinate,
        job,
        stream,
        &[],
    ))
    .unwrap()
}

pub(super) fn batch_item(
    name: &str,
    input: &str,
    stream: &str,
) -> crate::fleet::PureVerdictBatchItem {
    use cairn_common::executor_protocol::{ProcessBatchExecution, ProcessBatchItem};
    crate::fleet::PureVerdictBatchItem {
        result_identity: crate::execution::cache::CheckResultIdentity::new("p", name, input),
        process: ProcessBatchItem {
            header: name.into(),
            stream_id: stream.into(),
            execution: ProcessBatchExecution::NativeShell,
            program: String::new(),
            args: vec![format!("echo {name}")],
            env: vec![],
            stdin: None,
            timeout_ms: 1000,
            command_resource_identity: None,
            verdict_environment_names: vec![],
            silence_bound_ms: 0,
        },
    }
}

#[tokio::test]
#[serial_test::serial(jj)]
async fn a_rejected_second_observation_retains_the_entire_batch_until_the_store_recovers() {
    shared_manual_batch(true, false, false, false, false).await;
}

#[tokio::test]
#[serial_test::serial(jj)]
async fn cadence_adoption_mixed_withdrawn_and_active_consumers_finishes_all_items() {
    shared_manual_batch(false, true, false, false, false).await;
}

#[tokio::test]
#[serial_test::serial(jj)]
async fn cadence_adoption_hash_mismatch_commits_loss_before_releasing_retained_result() {
    shared_manual_batch(false, false, true, false, false).await;
}

#[tokio::test]
#[serial_test::serial(jj)]
async fn all_manual_consumers_withdrawn_before_result_keep_cancelled_receipts_when_cadence_adopts()
{
    shared_manual_batch(false, false, false, true, false).await;
}

#[tokio::test]
#[serial_test::serial(jj)]
async fn cadence_adoption_delivered_receipt_retries_physical_commit_before_ack() {
    shared_manual_batch(false, false, false, false, true).await;
}

async fn shared_manual_batch(
    reject_second: bool,
    withdrawn_first: bool,
    hash_mismatch: bool,
    cancelled_manual: bool,
    reject_finalization: bool,
) {
    use crate::jj::tests::{git, git_stdout, init_project, jj_bin};
    jj_bin().expect("this mandatory adoption regression requires the pinned jj tool");
    let home = tempfile::tempdir().unwrap();
    let repo = home.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    init_project(&repo);
    std::fs::create_dir_all(repo.join(".cairn")).unwrap();
    std::fs::write(repo.join(".cairn/config.yaml"),
        "checks:\n  first:\n    command: echo first\n    executor: {os: linux}\n    verdictPlatforms: [linux]\n    mainCadence:\n      platforms: [linux]\n      notify: testing\n  second:\n    command: echo second\n    executor: {os: linux}\n    verdictPlatforms: [linux]\n    mainCadence:\n      platforms: [linux]\n      notify: testing\n  third:\n    command: echo third {changedFiles}\n    fullCommand: echo third --all\n    executor: {os: linux}\n    verdictPlatforms: [linux]\n"
    ).unwrap();
    git(&repo, &["add", "-A"]);
    git(
        &repo,
        &["commit", "-q", "-m", "declare retained manual suites"],
    );
    let commit = git_stdout(&repo, &["rev-parse", "HEAD"]);
    let local = Arc::new(
        LocalDb::open_migrated_for_test(home.path().join("shared.db"))
            .await
            .unwrap(),
    );
    local
        .execute(
            "INSERT INTO projects(id,workspace_id,name,key,repo_path,default_branch,created_at,updated_at)
        VALUES('p','default','P','P',?1,'main',1,1)",
            (repo.to_string_lossy().into_owned(),),
        )
        .await
        .unwrap();
    local.execute_script(
        "INSERT INTO threads(id,project_id,name,status,attention,created_at,updated_at)
           VALUES('testing','p','testing','active','none',1,1);
         INSERT INTO jobs(id,thread_id,project_id,node_name,uri_segment,status,created_at,updated_at)
           VALUES('testing-job','testing','p','thread','thread','idle',1,1);"
    ).await.unwrap();
    let orch = boot(home.path(), local.clone()).await;
    let mut frames = orch.fleet.attach_reported_machine_for_test(REMOTE, &[]);
    let suites = ["first", "second", "third"];
    let mut contracts = Vec::new();
    for name in suites {
        contracts.push(
            resolve_check_contract_at_commit(
                &orch,
                local.clone(),
                CheckRunTarget {
                    job_id: "manual-job".into(),
                    project_id: "p".into(),
                    project_key: "P".into(),
                    repository_path: repo.to_string_lossy().into_owned(),
                    attribution_target: "main".into(),
                    attribution_commit: None,
                },
                commit.clone(),
                name,
                None,
            )
            .await
            .expect("the original manual contract is resolved from the real sealed commit"),
        );
    }
    let whole_third_input = contracts[2].context.input_hash.clone();
    let selective_command = "echo third selected.rs";
    contracts[2].plan.command = selective_command.into();
    contracts[2].plan.scope = CheckScope::Partial;
    let selected_input = check_result_key_for_executed_command(
        &contracts[2].configured_check,
        selective_command,
        &InputSelector::from_globs(&["**".into()]),
        None,
        &contracts[2].context.tree_hash,
        &check_platform_identity(),
        check_toolchain_identity(),
        CheckChange {
            name: "third",
            basis: contracts[2].change.basis(),
        },
    );
    assert_ne!(selected_input, whole_third_input);
    contracts[2].context.input_hash = selected_input.clone();
    contracts[2].sealed_coordinate = crate::execution::cache::SealedCheckCoordinate::new(
        &commit,
        &commit,
        &contracts[2].context.tree_hash,
        &selected_input,
    );
    let hashes = contracts
        .iter()
        .map(|c| c.context.input_hash.as_str())
        .collect::<Vec<_>>();
    let handles = (0..3)
        .map(|_| uuid::Uuid::new_v4().to_string())
        .collect::<Vec<_>>();
    let streams = handles
        .iter()
        .enumerate()
        .map(|(i, handle)| format!("manual-check:run:handle={handle}:{i}"))
        .collect::<Vec<_>>();
    for handle in &handles {
        local
            .execute(
                "INSERT INTO manual_check_handles(handle,project_id,created_at) VALUES(?1,'p',1)",
                (handle.clone(),),
            )
            .await
            .unwrap();
    }
    let mut physical = claim("running", "manual");
    physical.project_id = "p".into();
    physical.commit_sha = commit.clone();
    physical.check_names = suites.iter().map(|s| s.to_string()).collect();
    physical.input_hashes = hashes.iter().map(|s| s.to_string()).collect();
    physical.consumer_ref = handles[0].clone();
    physical.recording_plans = (0..3)
        .map(|i| {
            serde_json::to_value(CheckAdoptionRecordingPlan::new(
                &contracts[i].plan,
                &contracts[i].sealed_coordinate,
                "manual-job",
                &streams[i],
                &[],
            ))
            .unwrap()
        })
        .collect();
    orch.fleet
        .register_check_claim((REQUEST.into(), ATTEMPT.into()), physical);
    let request = |id: &str, attempt: &str, job: &str| {
        let mut request = orch.fleet.cell_request_for_test(id);
        request.attempt_id = attempt.into();
        request.base_commit = commit.clone();
        request.repository = RepositoryLocator::ScratchOnly {
            owner_id: "p".into(),
        };
        request.pinned_executor_id = Some(REMOTE.into());
        request.placement_work_class = crate::fleet::PlacementWorkClass::ReviewChecks;
        request.requesting_job_id = Some(job.into());
        request.wait_horizon_unix_ms = u64::MAX;
        request
    };
    let manual_request = request(REQUEST, ATTEMPT, "manual-job");
    let manual_items = (0..3)
        .map(|i| {
            let mut item = batch_item(suites[i], hashes[i], &streams[i]);
            item.process.args = vec![contracts[i].plan.command.clone()];
            item
        })
        .collect();
    let manual_orch = orch.clone();
    let mut manual = tokio::spawn(async move {
        manual_orch
            .fleet
            .submit_pure_verdict_batch(&manual_orch, manual_request, manual_items, None, None, None)
            .await
    });
    let entry_id = loop {
        if let ExecutorMessage::Offer { entry_id, .. } = next_submission_frame(
            &orch,
            &mut frames,
            &(REQUEST.into(), ATTEMPT.into()),
            "awaiting initial Offer",
            &mut manual,
        )
        .await
        {
            break entry_id;
        }
    };
    orch.fleet
        .handle_executor_message(REMOTE, 1, ExecutorMessage::Claim { entry_id });
    let granted = loop {
        if let ExecutorMessage::Granted {
            request,
            batch: Some(batch),
            ..
        } = next_submission_frame(
            &orch,
            &mut frames,
            &(REQUEST.into(), ATTEMPT.into()),
            "awaiting Granted after Claim",
            &mut manual,
        )
        .await
        {
            assert_eq!(
                batch.items.len(),
                3,
                "one physical grant contains all manual suites"
            );
            break request;
        }
    };
    assert_eq!(
        (granted.request_id.as_str(), granted.attempt_id.as_str()),
        (REQUEST, ATTEMPT)
    );
    assert!(
        claims::lookup(&local, REQUEST, ATTEMPT)
            .await
            .unwrap()
            .is_some(),
        "the claim is durable at grant"
    );

    let key = crate::fleet::purpose::PurposeKey::main_cadence("p", "second", "linux");
    let mut joined_template = claim("running", "main_cadence");
    joined_template.request_id = "cadence-join".into();
    joined_template.attempt_id = "cadence-attempt".into();
    joined_template.project_id = "p".into();
    joined_template.commit_sha = commit.clone();
    joined_template.check_names = vec!["second".into()];
    joined_template.input_hashes = vec![hashes[1].to_string()];
    joined_template.consumer_ref = key.to_string();
    joined_template.recording_plans = vec![serde_json::to_value(CheckAdoptionRecordingPlan::new(
        &contracts[1].plan,
        &contracts[1].sealed_coordinate,
        "main:p",
        "main-cadence:p:second:linux",
        &[],
    ))
    .unwrap()];
    orch.fleet.register_check_claim(
        ("cadence-join".into(), "cadence-attempt".into()),
        joined_template,
    );
    let cadence_request = request("cadence-join", "cadence-attempt", "main:p");
    let cadence_orch = orch.clone();
    let cadence_input = hashes[1].to_string();
    let cadence = tokio::spawn(async move {
        cadence_orch
            .fleet
            .submit_pure_verdict_batch(
                &cadence_orch,
                cadence_request,
                vec![batch_item(
                    "second",
                    &cadence_input,
                    "main-cadence:p:second:linux",
                )],
                None,
                None,
                Some(key),
            )
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            let count = local
                .query_opt(
                    "SELECT COUNT(*) FROM check_lane_adoption_waiters
                 WHERE request_id=?1 AND attempt_id=?2 AND consumer='main_cadence'",
                    (REQUEST.to_string(), ATTEMPT.to_string()),
                    |row| row.i64(0),
                )
                .await
                .unwrap()
                .unwrap();
            if count == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the late cadence join is durably attached to the existing physical grant");
    if withdrawn_first {
        claims::insert_waiter(
            &local,
            &claims::Waiter {
                request_id: REQUEST.into(),
                attempt_id: ATTEMPT.into(),
                check_name: "first".into(),
                item_index: 0,
                consumer: "main_cadence".into(),
                consumer_ref: crate::fleet::purpose::PurposeKey::main_cadence(
                    "p", "first", "linux",
                )
                .to_string(),
                state: "running".into(),
                observation_id: None,
                evidence: None,
                updated_ms: chrono::Utc::now().timestamp_millis(),
            },
        )
        .await
        .unwrap();
    }
    if cancelled_manual {
        let original = claims::waiters(&local, REQUEST, ATTEMPT).await.unwrap();
        let manuals = original
            .into_iter()
            .filter(|w| w.consumer == "manual")
            .collect::<Vec<_>>();
        assert_eq!(manuals.len(), 3);
        assert_eq!(
            orch.fleet
                .cancel_job_requests("manual-job", crate::fleet::CancelCause::OperatorRetracted,),
            3,
            "ending the manual job detaches all three real subscribers, not the cadence joiner"
        );
        tokio::time::timeout(std::time::Duration::from_secs(15), async {
            loop {
                let waiters = claims::waiters(&local, REQUEST, ATTEMPT).await.unwrap();
                if waiters
                    .iter()
                    .filter(|w| w.consumer == "manual")
                    .all(|w| w.state == "cancelled" && w.observation_id.is_none())
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("detached manual consumers must commit cancellation before the result");
        // The caller has already received its cancellation response; replaying
        // the physical result must not overwrite that user-visible settlement.
        for (index, handle) in handles.iter().enumerate() {
            local.execute(
                "UPDATE manual_check_handles SET response_json=?2,commit_sha=?3,tree_hash=?4,input_hash=?5
                 WHERE handle=?1 AND observation_id IS NULL",
                cairn_db::turso::params![
                    handle.clone(), r#"{"cancelled":true}"#.to_string(), commit.clone(),
                    contracts[index].context.tree_hash.clone(), hashes[index].to_string(),
                ],
            ).await.unwrap();
        }
        let before_result = claims::waiters(&local, REQUEST, ATTEMPT).await.unwrap();
        let manuals = before_result
            .iter()
            .filter(|w| w.consumer == "manual")
            .collect::<Vec<_>>();
        assert_eq!(manuals.len(), 3);
        assert!(
            manuals
                .iter()
                .all(|w| w.state == "cancelled" && w.observation_id.is_none()),
            "all manual consumers are already cancelled before recording: {manuals:?}"
        );
        assert!(before_result.iter().any(|w| w.consumer == "main_cadence"
            && matches!(w.state.as_str(), "running" | "awaiting_adoption" | "joined")));
        assert_eq!(
            local
                .query_opt("SELECT COUNT(*) FROM check_result_observations", (), |r| r
                    .i64(0))
                .await
                .unwrap(),
            Some(0)
        );
        assert!(orch
            .fleet
            .adoption_owner(&(REQUEST.into(), ATTEMPT.into()))
            .is_some());
    }
    assert_no_release(&mut frames);
    orch.fleet.begin_runner_shutdown();
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        let _ = manual.await.unwrap();
        let _ = cadence.await.unwrap();
    })
    .await
    .expect("shutdown releases old runner waiters without cancelling their remote grant");
    assert_no_release(&mut frames);

    // Faults are installed while no runner is writing: the restarted runner's
    // own adoption work holds write transactions, and DDL issued alongside it
    // waits out the pool's busy timeout. Neither trigger can fire before the
    // driver below, since nothing records an observation or finalizes the
    // physical claim until then.
    if reject_second {
        local.execute_script(
            "CREATE TRIGGER reject_second_observation BEFORE INSERT ON check_result_observations
             WHEN NEW.check_name='second' BEGIN
             SELECT RAISE(ABORT,'fixture rejects second observation'); END;"
        ).await.unwrap();
    }
    if reject_finalization {
        local.execute_script(
            "CREATE TRIGGER reject_physical_finalization BEFORE UPDATE OF state ON check_lane_adoptions
             WHEN NEW.state='recorded' BEGIN
             SELECT RAISE(ABORT,'fixture rejects physical finalization'); END;"
        ).await.unwrap();
    }
    let restarted_home = home.path().join("restarted-shared");
    std::fs::create_dir_all(&restarted_home).unwrap();
    let mut restarted = boot(&restarted_home, local.clone()).await;
    let mut restarted_frames = restarted.fleet.attach_unreported_machine_for_test(REMOTE);
    let mut retained = result(&restarted);
    if let CellOutcome::Completed { output, .. } = &mut retained.outcome {
        let specimen: Vec<ProcessBatchItemOutcome> = serde_json::from_str(output).unwrap();
        let items = suites
            .iter()
            .map(|name| {
                let mut item = specimen[0].clone();
                item.header = (*name).into();
                item.body = if *name == "third" {
                    "CAIRN_CHECK_RESULT_JSON={\"schemaVersion\":1,\"parser\":\"nextest\",\"complete\":true,\"selection\":\"partial\",\"undeclaredSkips\":0,\"tests\":[{\"id\":\"fixture selected::passes\",\"status\":\"passed\",\"attempts\":1,\"retried\":false,\"flaky\":false}]}".into()
                } else {
                    format!("{name} passed remotely")
                };
                item
            })
            .collect::<Vec<_>>();
        *output = serde_json::to_string(&items).unwrap();
    }
    let retained_outcome = retained.outcome;
    restarted.fleet.handle_executor_message(
        REMOTE,
        1,
        ExecutorMessage::Result {
            request_id: REQUEST.into(),
            attempt_id: ATTEMPT.into(),
            outcome: retained_outcome.clone(),
        },
    );
    restarted
        .fleet
        .apply_executor_snapshot(REMOTE, 1, Default::default(), Default::default());
    assert_no_release(&mut restarted_frames);
    if withdrawn_first {
        // Commit a real first-item observation, then withdraw that consumer.
        // A second boot must not turn its terminal receipt back into an adopter.
        let physical = claims::lookup(&local, REQUEST, ATTEMPT)
            .await
            .unwrap()
            .unwrap();
        let recorded = execute_adopted_check_item(&restarted, local.clone(), &physical, 0)
            .await
            .unwrap();
        assert!(recorded.observation_id.is_some());
        let first_purpose =
            crate::fleet::purpose::PurposeKey::main_cadence("p", "first", "linux").to_string();
        restarted
            .fleet
            .withdraw_check_consumer(
                &(REQUEST.into(), ATTEMPT.into()),
                "first",
                "main_cadence",
                &first_purpose,
                "first cadence consumer withdrew after its observation committed",
            )
            .await
            .unwrap();
        let withdrawn = claims::waiters(&local, REQUEST, ATTEMPT)
            .await
            .unwrap()
            .into_iter()
            .find(|w| w.consumer == "main_cadence" && w.check_name == "first")
            .unwrap();
        assert_eq!(withdrawn.state, "recorded_undelivered");
        assert_eq!(withdrawn.observation_id, recorded.observation_id);
        assert_no_release(&mut restarted_frames);
        restarted.fleet.begin_runner_shutdown();
        let again = home.path().join("restarted-with-withdrawn-consumer");
        std::fs::create_dir_all(&again).unwrap();
        restarted = boot(&again, local.clone()).await;
        restarted_frames = restarted.fleet.attach_unreported_machine_for_test(REMOTE);
        restarted.fleet.handle_executor_message(
            REMOTE,
            1,
            ExecutorMessage::Result {
                request_id: REQUEST.into(),
                attempt_id: ATTEMPT.into(),
                outcome: retained_outcome,
            },
        );
        restarted
            .fleet
            .apply_executor_snapshot(REMOTE, 1, Default::default(), Default::default());
        assert_no_release(&mut restarted_frames);
    }
    if hash_mismatch {
        let mut physical = claims::lookup(&local, REQUEST, ATTEMPT)
            .await
            .unwrap()
            .unwrap();
        physical.input_hashes[1] = "corrupted-granted-cadence-input".into();
        let probes = Arc::new(AtomicUsize::new(0));
        let seen = probes.clone();
        let store = local.clone();
        restarted.fleet.set_result_ack_probe_for_test(Arc::new(move |request, attempt| {
            assert_eq!((request, attempt), (REQUEST, ATTEMPT));
            crate::storage::run_db_blocking(|| async {
                let lost = claims::lookup(&store, REQUEST, ATTEMPT).await.map_err(|e| e.to_string())?.unwrap();
                assert_eq!(lost.state, "lost");
                let evidence = lost.evidence.unwrap();
                assert!(evidence.contains("corrupted-granted-cadence-input") && evidence.contains("differs from rebuilt"), "{evidence}");
                assert!(claims::waiters(&store, REQUEST, ATTEMPT).await.map_err(|e| e.to_string())?
                    .iter().all(|w| matches!(w.state.as_str(), "lost" | "cancelled")));
                assert_eq!(store.query_opt("SELECT COUNT(*) FROM check_result_observations WHERE project_id='p'", (), |r| r.i64(0))
                    .await.map_err(|e| e.to_string())?, Some(0));
                Ok(())
            }).unwrap();
            seen.fetch_add(1, Ordering::SeqCst);
        }));
        let waiters = claims::waiters(&local, REQUEST, ATTEMPT).await.unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            crate::execution::checks_main::adopt_check_batch(&restarted, &physical, waiters),
        )
        .await
        .expect("hash mismatch must durably settle and release the retained result");
        assert_eq!(probes.load(Ordering::SeqCst), 1);
        assert!(claims::open(&local)
            .await
            .unwrap()
            .iter()
            .all(|c| c.request_id != REQUEST));
        let mut ack = 0;
        while let Ok(frame) = restarted_frames.try_recv() {
            match frame {
                ExecutorMessage::ResultAck { .. } => ack += 1,
                ExecutorMessage::Offer { .. } | ExecutorMessage::Granted { .. } => {
                    panic!("mismatch dispatched new work")
                }
                _ => {}
            }
        }
        assert_eq!(ack, 1);
        return;
    }
    let probes = Arc::new(AtomicUsize::new(0));
    let probe_count = probes.clone();
    let probe_db = local.clone();
    restarted.fleet.set_result_ack_probe_for_test(Arc::new(move |request, attempt| {
        assert_eq!((request, attempt), (REQUEST, ATTEMPT));
        let (observations, cached, manual, cadence) = crate::storage::run_db_blocking(|| async {
            let count = |sql: &'static str| {
                let db = probe_db.clone();
                async move {
                    db.query_opt(sql, (), |row| row.i64(0)).await
                        .map_err(|e| e.to_string()).map(|n| n.unwrap())
                }
            };
            Ok((
                count("SELECT COUNT(*) FROM check_result_observations WHERE project_id='p'
                    AND verdict='passed'").await?,
                count("SELECT COUNT(*) FROM check_result_cache WHERE project_id='p' AND passed=1").await?,
                count(if cancelled_manual {
                    "SELECT COUNT(*) FROM check_lane_adoption_waiters WHERE consumer='manual'
                     AND state='cancelled' AND observation_id IS NOT NULL"
                } else {
                    "SELECT COUNT(*) FROM check_lane_adoption_waiters WHERE consumer='manual'
                     AND state='recorded_undelivered' AND observation_id IS NOT NULL"
                }).await?,
                count("SELECT COUNT(*) FROM check_lane_adoption_waiters WHERE consumer='main_cadence'
                    AND state='delivered' AND observation_id IS NOT NULL").await?,
            ))
        }).unwrap();
        assert_eq!((observations, cached, manual, cadence), (3, 3, 3, 1),
            "ResultAck must follow the third observation/cache and every consumer's durable disposition");
        if cancelled_manual {
            crate::storage::run_db_blocking(|| async {
                assert_eq!(probe_db.query_opt(
                    "SELECT COUNT(*) FROM manual_check_handles WHERE project_id='p' AND response_json=?1",
                    (r#"{"cancelled":true}"#.to_string(),), |row| row.i64(0),
                ).await.map_err(|e| e.to_string())?, Some(3),
                    "adoption must preserve all original manual cancellation responses");
                let waiters = claims::waiters(&probe_db, REQUEST, ATTEMPT).await.map_err(|e| e.to_string())?;
                for waiter in waiters.into_iter().filter(|w| w.consumer == "manual") {
                    assert_eq!(waiter.state, "cancelled");
                    assert_eq!(waiter.observation_id.as_deref(),
                        Some(format!("remote-check:{REQUEST}:{ATTEMPT}:{}", waiter.check_name).as_str()));
                    assert!(waiter.evidence.as_deref().is_some_and(|e| e.contains("live consumer dropped before its projection and delivery completed")),
                        "cancellation evidence must survive physical recording: {waiter:?}");
                }
                Ok(())
            }).unwrap();
        }
        probe_count.fetch_add(1, Ordering::SeqCst);
    }));
    let physical = claims::lookup(&local, REQUEST, ATTEMPT)
        .await
        .unwrap()
        .unwrap();
    let waiters = claims::waiters(&local, REQUEST, ATTEMPT).await.unwrap();
    let (_retry_guard, mut retries) =
        observe_retry(format!("remote-check:{REQUEST}:{ATTEMPT}:second"));
    let driver_orch = restarted.clone();
    let driver_claim = physical.clone();
    let driver = tokio::spawn(async move {
        crate::execution::checks_main::adopt_check_batch(&driver_orch, &driver_claim, waiters)
            .await;
    });
    if reject_finalization {
        let failure = tokio::time::timeout(std::time::Duration::from_secs(20), retries.recv())
            .await
            .expect("the physical finalization UPDATE must actually be rejected")
            .expect("the finalization retry listener remains installed");
        assert!(
            failure.contains("fixture rejects physical finalization"),
            "{failure}"
        );
        tokio::time::timeout(std::time::Duration::from_secs(20), async {
            loop {
                let consumers = claims::waiters(&local, REQUEST, ATTEMPT).await.unwrap();
                if consumers
                    .iter()
                    .any(|w| w.consumer == "main_cadence" && w.state == "delivered")
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the cadence receipt commits delivered before physical finalization is rejected");
        assert_ne!(
            claims::lookup(&local, REQUEST, ATTEMPT)
                .await
                .unwrap()
                .unwrap()
                .state,
            "recorded"
        );
        assert!(
            !driver.is_finished(),
            "a delivered receipt is not proof physical finalization succeeded"
        );
        assert_eq!(probes.load(Ordering::SeqCst), 0);
        assert_no_release(&mut restarted_frames);
        local
            .execute_script("DROP TRIGGER reject_physical_finalization;")
            .await
            .unwrap();
    }
    if reject_second {
        let failure = tokio::time::timeout(std::time::Duration::from_secs(20), retries.recv())
            .await
            .expect("the second observation insert must actually fail, not merely run slowly")
            .expect("the recording retry probe remains installed");
        assert!(
            failure.contains("fixture rejects second observation"),
            "{failure}"
        );
        let rows = local.query_all(
            "SELECT check_name FROM check_result_observations WHERE project_id='p' ORDER BY check_name",
            (), |row| row.text(0),
        ).await.unwrap();
        assert_eq!(rows, vec!["first".to_string()]);
        assert!(
            !driver.is_finished(),
            "a temporary observation-store refusal is not adoption loss"
        );
        assert_eq!(probes.load(Ordering::SeqCst), 0);
        assert_no_release(&mut restarted_frames);
        let pending = claims::lookup(&local, REQUEST, ATTEMPT)
            .await
            .unwrap()
            .unwrap();
        assert!(!matches!(
            pending.state.as_str(),
            "lost" | "cancelled" | "recorded"
        ));
        local
            .execute("DROP TRIGGER reject_second_observation", ())
            .await
            .unwrap();
    }
    tokio::time::timeout(std::time::Duration::from_secs(30), driver)
        .await
        .expect("the same physical batch finishes after observation storage recovers")
        .unwrap();
    assert_eq!(probes.load(Ordering::SeqCst), 1);
    if withdrawn_first {
        let withdrawn = claims::waiters(&local, REQUEST, ATTEMPT)
            .await
            .unwrap()
            .into_iter()
            .find(|w| w.consumer == "main_cadence" && w.check_name == "first")
            .unwrap();
        assert_eq!(withdrawn.state, "recorded_undelivered");
        assert!(withdrawn.observation_id.is_some());
        let outcomes = crate::execution::checks_main::last_outcomes(&restarted, "p");
        assert_eq!(
            outcomes
                .iter()
                .map(|o| o.check.as_str())
                .collect::<Vec<_>>(),
            vec!["second"],
            "only the active cadence suite reports; the withdrawn receipt stays terminal"
        );
    }
    let rows = local
        .query_all(
            "SELECT check_name,COUNT(*) FROM check_result_observations WHERE project_id='p'
         GROUP BY check_name ORDER BY check_name",
            (),
            |row| Ok((row.text(0)?, row.i64(1)?)),
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![
            ("first".into(), 1),
            ("second".into(), 1),
            ("third".into(), 1)
        ]
    );
    let selected = local.query_opt(
        "SELECT selection,input_hash FROM check_result_observations WHERE project_id='p' AND check_name='third'",
        (), |row| Ok((row.text(0)?, row.text(1)?)),
    ).await.unwrap().unwrap();
    assert_eq!(selected, ("partial".into(), selected_input));
    assert!(
        crate::execution::cache::get_check_result(local.clone(), "p", "third", &whole_third_input)
            .unwrap()
            .is_none(),
        "a selected sibling must not satisfy the cadence's whole-suite coordinate"
    );
    let restored: CheckAdoptionRecordingPlan =
        serde_json::from_value(physical.recording_plans[2].clone()).unwrap();
    assert_eq!(restored.plan.scope, CheckScope::Partial);
    assert_eq!(restored.plan.command, selective_command);
    let mut ack_count = 0;
    while let Ok(frame) = restarted_frames.try_recv() {
        match frame {
            ExecutorMessage::ResultAck { .. } => ack_count += 1,
            ExecutorMessage::Cancel { .. }
            | ExecutorMessage::Offer { .. }
            | ExecutorMessage::Granted { .. } => {
                panic!("adoption must not cancel or dispatch another physical batch: {frame:?}")
            }
            _ => {}
        }
    }
    assert_eq!(ack_count, 1);
}

/// A remote `when: write` batch whose physical result reached the runner, but
/// whose sole direct consumer was answered from the verdict cache instead: the
/// write wave filters cache misses before it submits, then the shared engine
/// resolves the cache again, and a hit leaves the parked batch outcome (and its
/// direct consumer guard) unused. The completed physical attempt must still
/// settle durably and be acknowledged, or the executor keeps it forever.
#[tokio::test]
async fn direct_write_check_answered_from_cache_still_settles_and_acks_its_physical_result() {
    direct_write_settlement(false, false).await;
}

#[tokio::test]
async fn direct_write_check_rejected_settlement_retains_result_until_retry() {
    direct_write_settlement(true, true).await;
}

async fn direct_write_settlement(withdraw_first: bool, reject_settlement: bool) {
    let home = tempfile::tempdir().unwrap();
    let local = Arc::new(
        LocalDb::open_migrated_for_test(home.path().join("runner.db"))
            .await
            .unwrap(),
    );
    local
        .execute(
            "INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
             VALUES('p','default','P','P','/fixture',1,1)",
            (),
        )
        .await
        .unwrap();
    let orch = boot(home.path(), local.clone()).await;
    // The test fleet's machines serve project `p`.
    let mut frames = orch.fleet.attach_reported_machine_for_test(REMOTE, &[]);
    let key: (String, String) = (REQUEST.into(), ATTEMPT.into());

    // What the write wave registers before submitting (checks.rs, the
    // non-pure-verdict arm of the planned batch submission).
    let mut template = claim("running", "write");
    template.consumer_ref = "write-job".into();
    template.project_id = "p".into();
    orch.fleet
        .register_check_claim(key.clone(), template.clone());
    let guard = orch
        .fleet
        .check_consumer_guard_for(&key, "smoke", &template);

    let mut request = orch.fleet.cell_request_for_test(REQUEST);
    request.attempt_id = ATTEMPT.into();
    request.project_id = "p".into();
    request.base_commit = "original-commit".into();
    request.repository = RepositoryLocator::ScratchOnly {
        owner_id: "p".into(),
    };
    request.pinned_executor_id = Some(REMOTE.into());
    request.mutation_policy = MutationPolicy::AllowDelta;
    request.requesting_job_id = Some("write-job".into());
    request.wait_horizon_unix_ms = u64::MAX;
    let submitting = orch.clone();
    let process = batch_item("smoke", "original-input", "write-checks:run:0").process;
    let mut submission = tokio::spawn(async move {
        submitting
            .fleet
            .submit_write_check_batch(&submitting, request, vec![process], None, None)
            .await
    });
    let next_frame = |frames: &mut mpsc::UnboundedReceiver<ExecutorMessage>| {
        let frame = frames.try_recv();
        frame.ok()
    };
    let entry_id = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            tokio::select! {
                frame = frames.recv() => if let Some(ExecutorMessage::Offer { entry_id, .. }) = frame { break entry_id; },
                ended = &mut submission => panic!("write submission ended before Offer: {:?}", ended.map(|e| format!("{:?}", e.outcome))),
            }
        }
    })
    .await
    .expect("the AllowDelta write batch is offered to the pinned remote");
    let generation = orch.fleet.link_generation(REMOTE).unwrap();
    orch.fleet
        .handle_executor_message(REMOTE, generation, ExecutorMessage::Claim { entry_id });
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            if let Some(ExecutorMessage::Granted {
                request,
                batch: Some(batch),
                ..
            }) = frames.recv().await
            {
                assert_eq!((request.request_id, request.attempt_id), key);
                assert_eq!(batch.items.len(), 1);
                break;
            }
        }
    })
    .await
    .expect("the remote claim is granted");
    let granted = claims::lookup(&local, REQUEST, ATTEMPT)
        .await
        .unwrap()
        .expect("a remote write grant is durably claimed");
    assert_eq!(granted.state, "running");
    let consumers = claims::waiters(&local, REQUEST, ATTEMPT).await.unwrap();
    assert_eq!(
        consumers.len(),
        1,
        "the direct write consumer is the sole consumer"
    );
    assert_eq!(
        (consumers[0].consumer.as_str(), consumers[0].state.as_str()),
        ("write", "running")
    );

    let acks = Arc::new(AtomicUsize::new(0));
    let probe_acks = acks.clone();
    let probe_db = local.clone();
    orch.fleet
        .set_result_ack_probe_for_test(Arc::new(move |request, attempt| {
            assert_eq!((request, attempt), (REQUEST, ATTEMPT));
            // Runs synchronously before the frame is sent, on a separate
            // checkout: only committed rows are visible here.
            let (settled, consumers) = crate::storage::run_db_blocking(|| async {
                let settled = claims::lookup(&probe_db, REQUEST, ATTEMPT)
                    .await
                    .map_err(|e| e.to_string())?
                    .expect("the physical row outlives its settlement");
                let consumers = claims::waiters(&probe_db, REQUEST, ATTEMPT)
                    .await
                    .map_err(|e| e.to_string())?;
                Ok((settled, consumers))
            })
            .unwrap();
            assert_eq!(
                settled.state, "cancelled",
                "ResultAck must follow the durable physical disposition: {settled:?}"
            );
            assert!(
                settled
                    .evidence
                    .as_deref()
                    .is_some_and(|e| e.contains("no live consumer")),
                "{settled:?}"
            );
            assert!(
                consumers
                    .iter()
                    .all(|w| w.state == "cancelled" && w.observation_id.is_none()),
                "{consumers:?}"
            );
            probe_acks.fetch_add(1, Ordering::SeqCst);
        }));

    if withdraw_first {
        drop(guard);
        submission.abort();
        assert!(matches!(submission.await, Err(error) if error.is_cancelled()));
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while claims::waiters(&local, REQUEST, ATTEMPT).await.unwrap()[0].state != "cancelled" {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            claims::lookup(&local, REQUEST, ATTEMPT)
                .await
                .unwrap()
                .unwrap()
                .state,
            "running"
        );
        assert_eq!(acks.load(Ordering::SeqCst), 0);
        while let Ok(frame) = frames.try_recv() {
            assert!(!matches!(frame, ExecutorMessage::ResultAck { .. }));
        }
        let mut retries = orch.fleet.direct_settlement_retries_for_test();
        if reject_settlement {
            local.execute_script("CREATE TRIGGER reject_direct_settlement BEFORE UPDATE OF state ON check_lane_adoptions WHEN NEW.state='cancelled' BEGIN SELECT RAISE(ABORT,'reject direct settlement'); END;").await.unwrap();
        }
        // Aborting the submission removed its queue binding. The retained result
        // must converge with bound ingest, not become generic no-adopter loss.
        orch.fleet.handle_executor_message(
            REMOTE,
            generation,
            ExecutorMessage::Result {
                request_id: REQUEST.into(),
                attempt_id: ATTEMPT.into(),
                outcome: result(&orch).outcome,
            },
        );
        if reject_settlement {
            let (retried, _) =
                tokio::time::timeout(std::time::Duration::from_secs(10), retries.recv())
                    .await
                    .unwrap()
                    .unwrap();
            assert_eq!(retried, key);
            assert_eq!(acks.load(Ordering::SeqCst), 0);
            assert!(orch.fleet.check_claim(&key).is_some());
            assert_no_release(&mut frames);
            local
                .execute_script("DROP TRIGGER reject_direct_settlement;")
                .await
                .unwrap();
        }
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                if let Some(ExecutorMessage::ResultAck {
                    request_id,
                    attempt_id,
                }) = frames.recv().await
                {
                    assert_eq!((request_id, attempt_id), key);
                    break;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(acks.load(Ordering::SeqCst), 1);
        orch.fleet.assert_check_settlement_drained_for_test(&key);
        assert!(claims::open(&local)
            .await
            .unwrap()
            .iter()
            .all(|claim| claim.request_id != REQUEST));
        return;
    }

    // The remote formatter succeeded and reports one typed item outcome.
    orch.fleet.handle_executor_message(
        REMOTE,
        generation,
        ExecutorMessage::Result {
            request_id: REQUEST.into(),
            attempt_id: ATTEMPT.into(),
            outcome: result(&orch).outcome,
        },
    );
    let executed = tokio::time::timeout(std::time::Duration::from_secs(15), submission)
        .await
        .expect("the write submission returns the remote result")
        .unwrap();
    assert!(
        matches!(
            executed.outcome,
            CellOutcome::Completed {
                exit_code: Some(0),
                ..
            }
        ),
        "{:?}",
        executed.outcome
    );
    assert_eq!(
        acks.load(Ordering::SeqCst),
        0,
        "a claimed grant defers its ack"
    );
    assert_no_release(&mut frames);

    // The wave parks the item with its direct guard (checks.rs, after
    // `submit_write_check_batch`), exactly as production does.
    let (mut items, delta) =
        split_write_check_batch_outcome(executed.outcome, &executed.account, 1);
    assert!(delta.is_none());
    for item in items.iter_mut() {
        let parked = item.as_mut().expect("the remote item outcome is usable");
        parked.publication = Some(
            crate::fleet::PublicationCoordination::for_request(key.clone())
                .with_consumer_guard(guard.clone()),
        );
    }
    drop(guard);
    let batched_results: Arc<
        std::sync::Mutex<HashMap<usize, Result<CheckExecResult, CheckExecutionFailure>>>,
    > = Arc::new(std::sync::Mutex::new(
        items.into_iter().enumerate().collect(),
    ));

    // The race: another lane recorded a reusable verdict for the same input
    // after the wave's miss filter and before the engine's cache phase.
    let configured: CheckCommand = serde_yaml::from_str("command: \"echo smoke\"\n").unwrap();
    let plan = CheckPlan {
        name: "smoke".into(),
        applies: true,
        command: "echo smoke".into(),
        scope: CheckScope::Full,
        resource_class: crate::config::project_settings::CheckResourceClass::Shared,
        verdict_environment_names: crate::execution::check_identity::verdict_environment_names(
            &configured,
        ),
        verdict_executor_ids: None,
        config_error: None,
        verdict_platforms: vec![crate::execution::check_identity::gating_platform().to_string()],
        verdict_runtime: crate::execution::check_identity::expected_verdict_runtime(
            &configured,
            check_toolchain_identity(),
        ),
    };
    record_fresh_check_observation(
        local.clone(),
        reusable_smoke_observation("original-input", plan_environment_fingerprint(&plan)),
    )
    .unwrap();
    let parked = batched_results.clone();
    let outcomes = run_planned_checks(
        local.clone(),
        "p",
        "original-tree",
        "write-job",
        &[(plan, "original-input".into())],
        "write-checks:run",
        CheckExecMode::Shared,
        Some(&orch),
        move |index, _command, _stream| {
            let parked = parked.clone();
            async move {
                parked.lock().unwrap().remove(&index).unwrap_or_else(|| {
                    Err(CheckExecutionFailure::from(
                        "no parked write outcome".to_string(),
                    ))
                })
            }
        },
        |_| {},
    )
    .await;
    assert!(
        outcomes[0].cached && outcomes[0].passed,
        "the engine must answer from the seeded reusable verdict"
    );
    assert!(outcomes[0].consumer_guard.is_none());
    assert!(
        batched_results.lock().unwrap().contains_key(&0),
        "the cache hit left the physical item outcome untaken"
    );
    // The wave ends; its parked outcomes (and their direct guard) drop unused.
    drop(batched_results);

    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let consumers = claims::waiters(&local, REQUEST, ATTEMPT).await.unwrap();
            if consumers[0].state == "cancelled" {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("dropping the last direct guard withdraws the sole consumer");
    let withdrawn = claims::waiters(&local, REQUEST, ATTEMPT).await.unwrap();
    assert!(withdrawn[0].observation_id.is_none());
    assert!(withdrawn[0]
        .evidence
        .as_deref()
        .is_some_and(|e| e.contains("live consumer dropped")));
    // Whatever settles the physical row may do so together with the
    // withdrawal; while it still reads `running`, nothing may have released
    // the executor's retained result.
    let physical = claims::lookup(&local, REQUEST, ATTEMPT)
        .await
        .unwrap()
        .unwrap();
    assert!(physical.observation_id.is_none());
    if physical.state == "running" {
        assert_eq!(acks.load(Ordering::SeqCst), 0);
        while let Some(frame) = next_frame(&mut frames) {
            assert!(
                !matches!(
                    frame,
                    ExecutorMessage::ResultAck { .. } | ExecutorMessage::Cancel { .. }
                ),
                "released before any durable settlement: {frame:?}"
            );
        }
    }

    // The completed physical attempt has no consumer left and its result is in
    // hand: it must settle durably and then be acknowledged exactly once.
    let acked = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if let Some(ExecutorMessage::ResultAck {
                request_id,
                attempt_id,
            }) = frames.recv().await
            {
                break (request_id, attempt_id);
            }
        }
    })
    .await;
    let physical = claims::lookup(&local, REQUEST, ATTEMPT).await.unwrap();
    assert!(
        acked.is_ok(),
        "no ResultAck: the completed remote write attempt was never settled after its sole \
         direct consumer withdrew; physical row = {:?}, in-memory claim open = {}, \
         consumers = {:?}",
        physical
            .as_ref()
            .map(|c| (&c.state, &c.observation_id, &c.evidence)),
        orch.fleet.check_claim(&key).is_some(),
        claims::waiters(&local, REQUEST, ATTEMPT)
            .await
            .unwrap()
            .iter()
            .map(|w| (&w.consumer, &w.state))
            .collect::<Vec<_>>(),
    );
    assert_eq!(acked.unwrap(), key);
    assert_eq!(physical.unwrap().state, "cancelled");
    assert_eq!(acks.load(Ordering::SeqCst), 1);
    // No debt remains in memory or in the open-claim scan a restart reads.
    orch.fleet.assert_check_settlement_drained_for_test(&key);
    assert!(claims::open(&local)
        .await
        .unwrap()
        .iter()
        .all(|claim| claim.request_id != REQUEST));
    assert!(
        !take_frames_contain_ack(&mut frames),
        "exactly one ResultAck"
    );
}

fn take_frames_contain_ack(frames: &mut mpsc::UnboundedReceiver<ExecutorMessage>) -> bool {
    let mut acked = false;
    while let Ok(frame) = frames.try_recv() {
        acked |= matches!(frame, ExecutorMessage::ResultAck { .. });
    }
    acked
}

fn reusable_smoke_observation(input_hash: &str, environment: String) -> FreshCheckObservationWrite {
    FreshCheckObservationWrite {
        manual_handle: None,
        publication_id: None,
        sealed_coordinate: crate::execution::cache::SealedCheckCoordinate::new(
            "commit-source",
            "commit-source",
            "tree-source",
            input_hash,
        ),
        id: "obs-smoke-review-lane".into(),
        public_handle: "handle-smoke-review-lane".into(),
        infra_reset_scopes: Vec::new(),
        nested_sandbox_lane: None,
        failure_signature: None,
        placement_facts: None,
        substrate_shape: None,
        selection: None,
        deferred_count: 0,
        plan_audit_state: None,
        plan_audit_note: None,
        audit_findings: Vec::new(),
        phases: None,
        execution_warmth: None,
        cell_origin: None,
        build_jobs: None,
        project_id: "p".into(),
        commit_sha: "commit-source".into(),
        defined_by_commit_sha: "commit-source".into(),
        tree_hash: "tree-source".into(),
        check_name: "smoke".into(),
        input_hash: input_hash.into(),
        environment_fingerprint: environment,
        exit_code: 0,
        verdict: "passed".into(),
        failure_kind: None,
        complete: true,
        reusable: true,
        non_reusable_reason: None,
        parser_version: crate::execution::check_identity::CHECK_PARSER_VERSION as i64,
        result_schema_version: crate::execution::check_identity::CHECK_RESULT_SCHEMA_VERSION as i64,
        ran_at: 100,
        duration_ms: 1,
        job_id: Some("review-job".into()),
        run_id: None,
        cadence: "write".into(),
        executor_id: None,
        executor_device_id: None,
        executor_connection_generation: None,
        executor_cell_id: None,
        executor_lease_epoch: None,
        executor_started_at_unix_ms: None,
        executor_finished_at_unix_ms: None,
        runner_build_id: Some(cairn_checkenv::implementation_identity().to_string()),
        toolchain_fingerprint: Some(check_toolchain_identity().to_string()),
        output_tail: "cached".into(),
        target_results_json: None,
        tests: Vec::new(),
        verdict_platform: Some(crate::execution::check_identity::gating_platform().to_string()),
        verdict_arch: Some(std::env::consts::ARCH.to_string()),
        verdict_environment_hash: Some(cairn_checkenv::verdict_environment_with_runtime(
            &cairn_checkenv::verdict_environment_hash(Vec::<String>::new(), |_| None),
            &cairn_checkenv::runtime_versions(
                crate::config::project_settings::RuntimeInstrument::ALL
                    .iter()
                    .map(|instrument| instrument.as_str()),
                Some(check_toolchain_identity()),
            ),
        )),
    }
}

type RetryListeners = std::collections::HashMap<String, mpsc::UnboundedSender<String>>;
static RECORDING_RETRIES: std::sync::LazyLock<std::sync::Mutex<RetryListeners>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashMap::new()));

pub(super) fn notify_observation_retry(id: &str, error: &str) {
    if let Some(listener) = RECORDING_RETRIES.lock().unwrap().get(id) {
        let _ = listener.send(error.to_string());
    }
}

pub(super) struct RetryListener(String);
impl Drop for RetryListener {
    fn drop(&mut self) {
        RECORDING_RETRIES.lock().unwrap().remove(&self.0);
    }
}

pub(super) fn observe_retry(id: String) -> (RetryListener, mpsc::UnboundedReceiver<String>) {
    let (sender, receiver) = mpsc::unbounded_channel();
    assert!(RECORDING_RETRIES
        .lock()
        .unwrap()
        .insert(id.clone(), sender)
        .is_none());
    (RetryListener(id), receiver)
}

#[tokio::test]
async fn consumer_identity_uses_receipt_attempt_and_stream_not_scheduling_priority() {
    let home = tempfile::tempdir().unwrap();
    let orch = runner(home.path(), &claim("running", "main_cadence")).await;
    orch.db
        .local
        .execute_script(
            "INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
         VALUES('project-a','default','A','PA','/fixture',1,1);
         INSERT INTO jobs(id,project_id,branch,status,created_at,updated_at)
         VALUES('consumer-job','project-a','main','running',1,1);
         INSERT INTO turn_end_check_attempts(id,job_id,state,created_at,updated_at)
         VALUES('review-attempt','consumer-job','running',1,1);
         UPDATE jobs SET turn_end_check_attempt_id='review-attempt' WHERE id='consumer-job';",
        )
        .await
        .unwrap();
    let handle = uuid::Uuid::new_v4().to_string();
    let manual_stream = format!("manual-check:run:handle={handle}:0");
    assert_eq!(
        check_consumer_identity(&orch, "consumer-job", &manual_stream, None)
            .await
            .unwrap(),
        ("manual", handle)
    );
    assert_eq!(
        check_consumer_identity(&orch, "consumer-job", "turn-checks:run:0", None)
            .await
            .unwrap(),
        ("turn_end", "review-attempt".into())
    );
    // Merge gates are scheduled at ReviewCheck priority but are not a turn-end
    // wave. The durable identity boundary deliberately accepts no priority.
    assert_eq!(
        check_consumer_identity(&orch, "consumer-job", "merge-gate:consumer-job:0", None)
            .await
            .unwrap(),
        ("write", "consumer-job".into())
    );
    let purpose = crate::fleet::purpose::PurposeKey::main_cadence("project-a", "smoke", "linux");
    assert_eq!(
        check_consumer_identity(&orch, "consumer-job", &manual_stream, Some(&purpose))
            .await
            .unwrap(),
        ("main_cadence", purpose.to_string())
    );
}

pub(super) async fn next_submission_frame(
    orch: &Orchestrator,
    frames: &mut mpsc::UnboundedReceiver<ExecutorMessage>,
    identity: &(String, String),
    boundary: &str,
    submission: &mut tokio::task::JoinHandle<Vec<crate::fleet::CoalescedDelivery>>,
) -> ExecutorMessage {
    let received = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        tokio::select! {
            frame = frames.recv() => frame.expect("remote fake link stays open"),
            result = submission => {
                let outcomes = result.expect("submission task must not panic before a protocol frame")
                    .into_iter().map(|item| match item {
                        Ok(item) => format!("{:?}", item.outcome),
                        Err(error) => format!("{error:?}"),
                    }).collect::<Vec<_>>();
                panic!("{boundary}: submission ended before the expected frame: {outcomes:?}");
            }
        }
    }).await;
    received.unwrap_or_else(|_| {
        panic!(
            "{boundary}: no frame; dispatch={:?}; health={}",
            orch.fleet.request_dispatch_phase(identity),
            serde_json::to_string(
                &orch
                    .fleet
                    .executor_health(chrono::Utc::now().timestamp_millis() as u64)
            )
            .unwrap(),
        )
    })
}
