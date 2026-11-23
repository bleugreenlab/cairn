//! A write wave driven end to end against a colocated fake executor:
//! [`await_write_check_wave`] over [`run_write_checks_after_seal_inner`] on a
//! real jj-coordinated project whose sealed write selects two declared fixers
//! and three verdicts, with no cached verdicts.
//!
//! What it pins is the request shape of the write cadence: each fixer is its
//! own change-returning request, chained on the previous fixer's fold; each
//! verdict is its own pure-verdict request on the commit the chain ended at,
//! all in flight at once, each recorded as its own request settles; and a
//! verdict that rewrites the tree without declaring `fixes` is reported, never
//! folded.

use super::*;
use crate::db::DbState;
use crate::jj::tests::{git, git_stdout, init_project, jj_bin};
use crate::mcp::handlers::branch::BranchResolution;
use crate::services::testing::TestServicesBuilder;
use crate::storage::SearchIndex;
use cairn_common::executor_protocol::{
    CellExecutionMeta, ExecutorMessage, ProcessBatchItemOutcome, TrackedModificationEvidence,
};
use std::time::Duration;
use tokio::sync::mpsc;

const JOB: &str = "job";
const BRANCH: &str = "agent/feature";
const VERDICTS: [&str; 3] = ["alpha", "beta", "gamma"];
/// The verdict whose result the machine holds until the test releases it.
const HELD: &str = "beta";
/// The verdict that writes to the tree without declaring `fixes`.
const UNDECLARED: &str = "gamma";
/// A deadlock guard, never an oracle: every wait below is for an event the
/// wave produces in well under a second.
const DEADLINE: Duration = Duration::from_secs(60);

fn commit(repo: &Path, files: &[(&str, &str)], message: &str) -> String {
    for (path, content) in files {
        let path = repo.join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
    }
    git(repo, &["add", "-A"]);
    git(repo, &["commit", "-q", "-m", message]);
    git_stdout(repo, &["rev-parse", "HEAD"])
}

/// Each fixer owns one file, so neither's fold moves the other's key; the
/// verdicts read everything.
fn contract() -> String {
    let mut config = String::from(
        "checks:\n  \
         fmt-a:\n    command: fix a.txt\n    when: write\n    fixes: true\n    verdictPlatforms: []\n    impact:\n      - a.txt\n  \
         fmt-b:\n    command: fix b.txt\n    when: write\n    fixes: true\n    impact:\n      - b.txt\n",
    );
    for verdict in VERDICTS {
        config.push_str(&format!(
            "  {verdict}:\n    command: echo {verdict}\n    when: write\n    impact:\n      - \"**\"\n"
        ));
    }
    config
}

/// What a fixer leaves its file as.
fn fixed(path: &str) -> String {
    format!("formatted {path}\n")
}

/// The delta a fixer of `path` returns on `base`: a commit off `base` with
/// the file in its fixed form, or `None` when `base` already has it, as a
/// real formatter returns no change on a formatted tree.
fn rewrite(repo: &Path, base: &str, path: &str) -> Option<String> {
    rewrite_as(repo, base, path, &fixed(path))
}

fn rewrite_as(repo: &Path, base: &str, path: &str, content: &str) -> Option<String> {
    let current = std::process::Command::new("git")
        .args(["show", &format!("{base}:{path}")])
        .current_dir(repo)
        .output()
        .unwrap();
    if current.status.success() && current.stdout == content.as_bytes() {
        return None;
    }
    let scratch = tempfile::tempdir().unwrap();
    let index = scratch.path().join("index");
    let run = |args: &[&str], stdin: Option<&str>| -> String {
        use std::io::Write;
        let mut child = std::process::Command::new("git")
            .args(args)
            .current_dir(repo)
            .env("GIT_INDEX_FILE", &index)
            .env("GIT_AUTHOR_NAME", "fixer")
            .env("GIT_AUTHOR_EMAIL", "fixer@example.com")
            .env("GIT_COMMITTER_NAME", "fixer")
            .env("GIT_COMMITTER_EMAIL", "fixer@example.com")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        if let Some(input) = stdin {
            child
                .stdin
                .take()
                .unwrap()
                .write_all(input.as_bytes())
                .unwrap();
        }
        let output = child.wait_with_output().unwrap();
        assert!(output.status.success(), "git {args:?}");
        String::from_utf8(output.stdout).unwrap().trim().to_string()
    };
    run(&["read-tree", base], None);
    let blob = run(&["hash-object", "-w", "--stdin"], Some(content));
    run(
        &[
            "update-index",
            "--add",
            "--cacheinfo",
            &format!("100644,{blob},{path}"),
        ],
        None,
    );
    let tree = run(&["write-tree"], None);
    Some(run(
        &["commit-tree", &tree, "-p", base, "-m", "fixer output"],
        None,
    ))
}

/// The colocated machine's answer to a one-item batch that exited zero.
fn answered(
    request: &CellRequest,
    header: &str,
    generation: u64,
    tracked_modifications: Option<TrackedModificationEvidence>,
    mutation_delta: Option<crate::fleet::MutationDelta>,
) -> CellOutcome {
    let item = ProcessBatchItemOutcome {
        header: header.into(),
        body: String::new(),
        succeeded: true,
        suspended: false,
        images: Vec::new(),
        exit_code: Some(0),
        timed_out: false,
        started_at_unix_ms: 1,
        finished_at_unix_ms: 2,
        duration_ms: 1,
        peak_rss_bytes: None,
        peak_process: None,
        cpu: None,
        granted_units: None,
        disk_delta_bytes: None,
        sandbox_denials: Vec::new(),
        tracked_modifications,
        environment_fingerprint: String::new(),
        verdict_platform: None,
        verdict_arch: None,
        toolchain_fingerprint: None,
        verdict_environment_hash: None,
        stop: None,
        build_jobs: None,
        unrecorded_cache_checkpoint: None,
        build_job_processes: None,
    };
    CellOutcome::Completed {
        request_id: request.request_id.clone(),
        attempt_id: request.attempt_id.clone(),
        exit_code: Some(0),
        output: serde_json::to_string(&[item]).unwrap(),
        timed_out: false,
        metadata: CellExecutionMeta {
            warmth: None,
            load_context: None,
            executor_id: crate::fleet::COLOCATED_EXECUTOR_ID.into(),
            executor_device_id: "local-device".into(),
            executor_connection_generation: generation,
            cell_id: "cell".into(),
            cell_epoch: 1,
            started_at_unix_ms: 1,
            finished_at_unix_ms: 2,
            duration_ms: Some(1),
            commands_exited_at_unix_ms: None,
            peak_rss_bytes: None,
            peak_physical_footprint_bytes: None,
            peak_process: None,
            cpu: None,
            allocation: None,
            disk_delta_bytes: None,
            measurement_quality: None,
            environment_fingerprint: String::new(),
            verdict_platform: None,
            verdict_arch: None,
            verdict_environment_hash: None,
            sandbox: None,
            toolchain_fingerprint: None,
            containment: None,
            stop: None,
            cell_origin: None,
            test_phase: None,
            build_job_processes: None,
        },
        mutation_delta: mutation_delta.map(Box::new),
        sandbox_denials: Vec::new(),
        tracked_modifications: None,
        node_scratch: None,
    }
}

/// What the machine saw of one granted request.
#[derive(Debug)]
struct Grant {
    suite: String,
    policy: MutationPolicy,
    base_commit: String,
    request_id: String,
    items: usize,
}

fn connect_machine(
    orch: &Orchestrator,
    repo: &Path,
    grants_tx: mpsc::UnboundedSender<Grant>,
    release_held: Arc<tokio::sync::Notify>,
) -> tokio::task::JoinHandle<mpsc::UnboundedReceiver<ExecutorMessage>> {
    let (sender, mut executor) = mpsc::unbounded_channel();
    let generation = orch.fleet.attach_announced_executor(sender);
    let fleet = orch.fleet.clone();
    let machine_repo = repo.to_path_buf();
    cairn_common::task::spawn(async move {
        let answer = |request: &CellRequest, outcome: CellOutcome| {
            fleet.handle_executor_message(
                crate::fleet::COLOCATED_EXECUTOR_ID,
                generation,
                ExecutorMessage::Result {
                    request_id: request.request_id.clone(),
                    attempt_id: request.attempt_id.clone(),
                    outcome,
                },
            );
        };
        let mut verdicts = Vec::new();
        while verdicts.len() < VERDICTS.len() {
            match executor.recv().await {
                Some(ExecutorMessage::Offer { entry_id, .. }) => {
                    fleet.handle_executor_message(
                        crate::fleet::COLOCATED_EXECUTOR_ID,
                        generation,
                        ExecutorMessage::Claim { entry_id },
                    );
                }
                Some(ExecutorMessage::Granted {
                    request,
                    batch: Some(batch),
                    ..
                }) => {
                    let suite = request
                        .affinity_key
                        .as_deref()
                        .and_then(|key| key.strip_prefix(&format!("{JOB}:")))
                        .expect("every write check carries a job:suite key")
                        .to_string();
                    grants_tx
                        .send(Grant {
                            suite: suite.clone(),
                            policy: request.mutation_policy.clone(),
                            base_commit: request.base_commit.clone(),
                            request_id: request.request_id.clone(),
                            items: batch.items.len(),
                        })
                        .unwrap();
                    let header = batch.items[0].header.clone();
                    if request.mutation_policy == MutationPolicy::AllowDelta {
                        let path = if suite == "fmt-a" { "a.txt" } else { "b.txt" };
                        let delta = rewrite(&machine_repo, &request.base_commit, path).map(
                            |delta_commit| crate::fleet::MutationDelta {
                                base_commit: request.base_commit.clone(),
                                delta_commit,
                                upload_receipt: None,
                            },
                        );
                        answer(
                            &request,
                            answered(&request, &header, generation, None, delta),
                        );
                    } else {
                        // A pure-verdict cell discards what its command
                        // wrote and reports it, as the executor does.
                        let tracked = (suite == UNDECLARED).then(|| TrackedModificationEvidence {
                            paths: vec!["a.txt".into()],
                            files_changed: 1,
                            lines_added: 1,
                            lines_deleted: 1,
                            index_restore_failure: None,
                        });
                        verdicts.push((
                            suite,
                            request.clone(),
                            answered(&request, &header, generation, tracked, None),
                        ));
                    }
                }
                Some(_) => continue,
                None => panic!("the wave's checks are offered to the colocated executor"),
            }
        }
        let mut held = None;
        for (suite, request, outcome) in verdicts {
            if suite == HELD {
                held = Some((request, outcome));
            } else {
                answer(&request, outcome);
            }
        }
        release_held.notified().await;
        let (request, outcome) = held.expect("beta was granted");
        answer(&request, outcome);
        // Kept open so the executor stays attached while the wave finishes.
        executor
    })
}

async fn assert_active_legacy_successor(
    orch: &Orchestrator,
    repo: &Path,
    run: &RunContext,
    original_request: &str,
    current_head: &str,
) -> String {
    let legacy_runner = Orchestrator::builder(
        orch.db.clone(),
        Arc::new(TestServicesBuilder::new().build()),
        orch.config_dir.clone(),
    )
    .jj_binary_path(orch.jj_binary_path.clone())
    .build();
    legacy_runner.fleet.await_restart_adoptions_loaded().await;
    let orch = &legacy_runner;
    let db = &orch.db.local;
    let json = db
        .query_text(
            "SELECT context_json FROM check_requests WHERE id=?1",
            (original_request.to_string(),),
        )
        .await
        .unwrap()
        .unwrap();
    let json: serde_json::Value = serde_json::from_str(&json).unwrap();
    let snapshot = &json["write_lane"];
    let mut item: PlannedCheckBatchItem = serde_json::from_value(snapshot["item"].clone()).unwrap();
    const REMOTE: &str = "legacy-wave-remote";
    // An AllowDelta request stays home unless its caller names a machine.
    item.executor = Some(cairn_common::executor_protocol::ExecutorSelector {
        name: Some(REMOTE.into()),
        ..Default::default()
    });
    let producer = CheckProducer::new(CheckConsumer::Write, JOB);
    item.recording_plan.as_mut().unwrap().producer = producer.clone();
    let old = PlannedCheckRequest {
        producer,
        project_id: "proj".into(),
        repository: repo.to_string_lossy().into_owned(),
        store_dir: crate::jj::project_store_dir(&orch.config_dir, repo),
        sealed_commit: snapshot["sealed_commit"].as_str().unwrap().into(),
        requesting_job_id: JOB.into(),
        owner: serde_json::from_value(snapshot["owner"].clone()).unwrap(),
        affinity_key: Some(check_affinity_key(JOB, "fmt-a")),
        priority: CellPriority::WriteCheck,
        env: Vec::new(),
        item,
        run_context: Some(run.clone()),
        status_board: None,
        on_complete: None,
        purpose: None,
        publication: None,
    };
    let mut machine = orch
        .fleet
        .attach_reported_project_machine_for_test(REMOTE, "proj", &[]);
    let generation = 1;
    let (reconnect_tx, reconnect_rx) = tokio::sync::oneshot::channel();
    let mut reconnect_rx = Some(reconnect_rx);
    let route = Arc::new(std::sync::Mutex::new((orch.fleet.clone(), generation)));
    let machine_route = route.clone();
    let machine_repo = repo.to_path_buf();
    let (grants_tx, mut grants) = mpsc::unbounded_channel();
    let (deltas_tx, mut deltas) = mpsc::unbounded_channel();
    let release = Arc::new(tokio::sync::Notify::new());
    let released = release.clone();
    let executor = cairn_common::task::spawn(async move {
        let mut count = 0;
        while let Some(message) = machine.recv().await {
            let (fleet, generation) = machine_route.lock().unwrap().clone();
            let executor_id = if count == 0 {
                REMOTE
            } else {
                crate::fleet::COLOCATED_EXECUTOR_ID
            };
            match message {
                ExecutorMessage::Offer { entry_id, .. } => {
                    fleet.handle_executor_message(
                        executor_id,
                        generation,
                        ExecutorMessage::Claim { entry_id },
                    );
                }
                ExecutorMessage::Granted {
                    request,
                    batch: Some(batch),
                    ..
                } => {
                    count += 1;
                    grants_tx
                        .send((
                            request.request_id.clone(),
                            request.base_commit.clone(),
                            batch.items[0].header.clone(),
                            batch.items.len(),
                        ))
                        .unwrap();
                    if count == 1 {
                        released.notified().await;
                        machine = reconnect_rx.take().unwrap().await.unwrap();
                    }
                    let (fleet, generation) = machine_route.lock().unwrap().clone();
                    let delta = if count == 1 {
                        rewrite(&machine_repo, &request.base_commit, "a.txt")
                    } else {
                        rewrite_as(
                            &machine_repo,
                            &request.base_commit,
                            "a.txt",
                            "upgrade formatter output\n",
                        )
                    }
                    .map(|delta_commit| crate::fleet::MutationDelta {
                        base_commit: request.base_commit.clone(),
                        delta_commit,
                        upload_receipt: None,
                    });
                    deltas_tx
                        .send(delta.as_ref().map(|delta| delta.delta_commit.clone()))
                        .unwrap();
                    let result_generation = if count == 1 { 1 } else { generation };
                    let mut outcome = answered(
                        &request,
                        &batch.items[0].header,
                        result_generation,
                        None,
                        delta,
                    );
                    if count == 1 {
                        if let CellOutcome::Completed { metadata, .. } = &mut outcome {
                            metadata.executor_id = REMOTE.into();
                            metadata.executor_device_id = format!("device-{REMOTE}");
                        }
                    }
                    fleet.handle_executor_message(
                        executor_id,
                        result_generation,
                        ExecutorMessage::Result {
                            request_id: request.request_id.clone(),
                            attempt_id: request.attempt_id.clone(),
                            outcome,
                        },
                    );
                }
                _ => {}
            }
        }
    });
    let raw_orch = orch.clone();
    let mut raw =
        cairn_common::task::spawn(async move { submit_planned_fix(&raw_orch, old).await });
    let old_grant = tokio::time::timeout(DEADLINE, async {
        tokio::select! {
            grant = grants.recv() => grant.expect("the fake remote stays attached"),
            result = &mut raw => {
                let diagnostic = match result {
                    Ok(Ok(outcome)) => format!("{:?}", outcome.results),
                    Ok(Err(error)) => error,
                    Err(error) => error.to_string(),
                };
                panic!("legacy submission ended before its first remote grant: {diagnostic}");
            }
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "legacy submission has no remote grant; health={}",
            serde_json::to_string(
                &orch
                    .fleet
                    .executor_health(chrono::Utc::now().timestamp_millis() as u64)
            )
            .unwrap()
        )
    });
    assert_eq!(old_grant.3, 1);
    let granted_claim = crate::storage::check_lane_adoptions::latest_for_request(db, &old_grant.0)
        .await
        .unwrap()
        .expect("the remote grant is journaled before shutdown");
    assert_eq!(granted_claim.executor_id, REMOTE);
    assert_eq!(granted_claim.state, "running");
    assert_eq!(granted_claim.consumer, "write");
    assert_eq!(granted_claim.consumer_ref, JOB);
    assert!(granted_claim.observation_id.is_none());
    let granted_waiters = crate::storage::check_lane_adoptions::waiters(
        db,
        &granted_claim.request_id,
        &granted_claim.attempt_id,
    )
    .await
    .unwrap();
    assert_eq!(granted_waiters.len(), 1);
    assert_eq!(granted_waiters[0].state, "running");
    assert_eq!(granted_waiters[0].consumer, "write");
    assert_eq!(granted_waiters[0].consumer_ref, JOB);
    assert!(granted_waiters[0].observation_id.is_none());
    // Shutdown leaves granted legacy custody intact; live caller cancellation
    // alone would intentionally withdraw it instead of simulating runner loss.
    orch.fleet.begin_runner_shutdown();
    let stopped = tokio::time::timeout(DEADLINE, raw)
        .await
        .expect("the old caller finishes when its runner stops")
        .expect("the old caller does not panic")
        .expect("shutdown reports the granted lane's custody");
    assert!(stopped.delta.is_none());
    assert_eq!(stopped.results.len(), 1);
    let attempt = match stopped.results.values().next().unwrap() {
        Err(CheckExecutionFailure::LeftRunning(attempt)) => attempt,
        other => panic!("shutdown must leave the old grant running, got {other:?}"),
    };
    assert_eq!(attempt.request_id, old_grant.0);
    assert_eq!(attempt.attempt_id, granted_claim.attempt_id);
    let stopped_request = stopped.request.as_ref().unwrap();
    assert_eq!(attempt.attempt_id, stopped_request.attempt_id);
    assert_eq!(attempt.executor_id, REMOTE);
    let claim = crate::storage::check_lane_adoptions::latest_for_request(db, &old_grant.0)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claim.state, "running");
    assert_eq!(claim.consumer, "write");
    assert_eq!(claim.consumer_ref, JOB);
    assert_eq!(claim.attempt_id, attempt.attempt_id);
    assert!(claim.observation_id.is_none());
    let waiters =
        crate::storage::check_lane_adoptions::waiters(db, &claim.request_id, &claim.attempt_id)
            .await
            .unwrap();
    assert_eq!(waiters.len(), 1);
    assert_eq!(waiters[0].state, "running");
    assert_eq!(waiters[0].consumer, "write");
    assert_eq!(waiters[0].consumer_ref, JOB);
    assert!(waiters[0].observation_id.is_none());
    let recovered = Orchestrator::builder(
        orch.db.clone(),
        Arc::new(TestServicesBuilder::new().build()),
        orch.config_dir.clone(),
    )
    .jj_binary_path(orch.jj_binary_path.clone())
    .build();
    recovered.fleet.await_restart_adoptions_loaded().await;
    let _remote_machine = recovered.fleet.attach_reported_project_machine_for_test(
        REMOTE,
        "proj",
        &[(&claim.request_id, &claim.attempt_id)],
    );
    // The fresh successor resolves the current untargeted contract and stays
    // colocated; the legacy retained result still belongs to REMOTE.
    let (sender, machine) = mpsc::unbounded_channel();
    let generation = recovered.fleet.attach_announced_executor(sender);
    reconnect_tx.send(machine).unwrap();
    *route.lock().unwrap() = (recovered.fleet.clone(), generation);
    let orch = &recovered;

    let before = db
        .query_opt("SELECT COUNT(*) FROM check_requests", (), |row| {
            Ok(row.get::<i64>(0)?)
        })
        .await
        .unwrap()
        .unwrap();
    // The old lane is genuinely granted and unanswered when the upgrade importer runs.
    requests::rearm_at_boot(orch).await;
    let imported = db
        .query_text(
            "SELECT id FROM check_requests WHERE request_id=?1",
            (old_grant.0.clone(),),
        )
        .await
        .expect("the imported-row lookup is readable");
    let imported = if let Some(imported) = imported {
        imported
    } else {
        let claim_now =
            crate::storage::check_lane_adoptions::latest_for_request(db, &old_grant.0).await;
        let waiters_now =
            crate::storage::check_lane_adoptions::waiters(db, &claim.request_id, &claim.attempt_id)
                .await;
        let store = crate::jj::project_store_dir(&orch.config_dir, repo);
        let contract = load_checks_contract_at_commit_detailed(repo, &claim.commit_sha).await;
        let store_contract =
            load_checks_contract_at_commit_detailed(&store, &claim.commit_sha).await;
        let recording_errors: Vec<_> = claim
            .recording_plans
            .iter()
            .map(|plan| {
                serde_json::from_value::<CheckAdoptionRecordingPlan>(plan.clone())
                    .err()
                    .map(|error| error.to_string())
            })
            .collect();
        panic!(
            "legacy importer left no row for granted request {}; claim={claim_now:?}; waiters={waiters_now:?}; immutable contract={contract:?}; object repository={repo:?}; coordinate-store contract={store_contract:?}; store={store:?}; recording decode errors={recording_errors:?}",
            old_grant.0,
        );
    };
    assert_ne!(imported, original_request);
    release.notify_one();
    let old_delta = tokio::time::timeout(DEADLINE, deltas.recv())
        .await
        .unwrap()
        .unwrap()
        .expect("the old granted fixer really returned a mutation delta");
    let fresh = tokio::time::timeout(DEADLINE, grants.recv())
        .await
        .expect("legacy recovery automatically dispatches its same-suite successor")
        .unwrap();
    assert_ne!(fresh.0, old_grant.0);
    assert_eq!(
        fresh.1, current_head,
        "the fresh lane judges the current sealed head"
    );
    assert_eq!(fresh.3, 1, "the successor remains a single-suite lane");
    tokio::time::timeout(DEADLINE,async {
        loop {
            let done=db.query_opt("SELECT COUNT(*) FROM check_requests old
                JOIN write_check_waves w ON w.id=json_extract(old.context_json,'$.legacy_successor_wave')
                WHERE old.id=?1 AND old.state='cancelled' AND w.completed=1",
                (imported.clone(),),|row|Ok(row.get::<i64>(0)?)).await.unwrap();
            if done==Some(1) {break;}
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("the imported row links its completed automatic successor");
    requests::rearm_at_boot(orch).await;
    let after = db
        .query_opt("SELECT COUNT(*) FROM check_requests", (), |row| {
            Ok(row.get::<i64>(0)?)
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        after - before,
        2,
        "one imported row and exactly one fresh same-suite row"
    );
    let legacy = db
        .query_opt(
            "SELECT evidence,json_extract(context_json,'$.fold'),
        json_extract(context_json,'$.legacy_successor_wave') FROM check_requests WHERE id=?1",
            (imported,),
            |row| {
                Ok((
                    row.get::<String>(0)?,
                    row.get::<Option<String>>(1)?,
                    row.get::<String>(2)?,
                ))
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert!(
        legacy.0.contains("old delta discarded unfolded"),
        "{}",
        legacy.0
    );
    let retained = db
        .query_opt_text(
            "SELECT json_extract(context_json,'$.returned.delta.deltaCommit')
        FROM check_requests WHERE request_id=?1",
            (old_grant.0,),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        retained, old_delta,
        "the imported row retains the exact old delta as discarded evidence"
    );
    assert!(
        legacy.1.is_none(),
        "unknown-phase old delta never receives a fold checkpoint"
    );
    assert_eq!(
        db.query_text(
            "SELECT suite FROM check_requests WHERE request_id=?1",
            (fresh.0.clone(),)
        )
        .await
        .unwrap()
        .as_deref(),
        Some("fmt-a"),
        "the canonical successor row names only the imported suite"
    );
    let landed = git_stdout(repo, &["rev-parse", &format!("refs/heads/{BRANCH}")]);
    assert_ne!(
        landed, current_head,
        "the fresh durable formatter publishes its new delta"
    );
    assert_eq!(
        git_stdout(repo, &["rev-parse", &format!("{landed}^")]),
        current_head,
        "exactly one new fold lands, solely from the fresh successor"
    );
    assert_eq!(
        git_stdout(repo, &["show", &format!("{landed}:a.txt")]),
        "upgrade formatter output"
    );
    assert!(
        grants.try_recv().is_err(),
        "rearming dispatches no second successor"
    );
    executor.abort();
    assert!(executor.await.unwrap_err().is_cancelled());
    landed
}

async fn assert_superseded_summary_restart(orch: &Orchestrator, config_dir: &Path, bin: &str) {
    let db = &orch.db.local;
    db.execute(
        "UPDATE write_check_waves SET delivered=1 WHERE id='write:job:tool-use'",
        (),
    )
    .await
    .unwrap();
    let now = chrono::Utc::now().timestamp_millis();
    for (tool, offset) in [("summary-old", 1), ("summary-new", 2)] {
        db.execute(
            "INSERT INTO write_check_waves(id,project_id,job_id,run_id,tool_use_id,driven_by,
            snapshot_json,summary,completed,created_at_ms)
            SELECT ?1,project_id,job_id,run_id,?2,driven_by,snapshot_json,?3,1,?4
            FROM write_check_waves WHERE id='write:job:tool-use'",
            cairn_db::turso::params![
                format!("write:job:{tool}"),
                tool,
                format!("Checks: ✓ {tool}"),
                now + offset
            ],
        )
        .await
        .unwrap();
    }
    let first = Orchestrator::builder(
        orch.db.clone(),
        Arc::new(TestServicesBuilder::new().build()),
        config_dir.to_path_buf(),
    )
    .jj_binary_path(bin.to_string())
    .build();
    let old = Arc::new(first.late_write_checks.open(JOB, "summary-old"));
    old.planned("old-commit");
    let newest = first.late_write_checks.open(JOB, "summary-new");
    newest.planned("new-commit");
    write_wave::deliver_late_write_check_summary(
        first.clone(),
        "run".into(),
        old.clone(),
        "Checks: ✓ summary-old".into(),
    )
    .await;
    assert_eq!(
        db.query_opt(
            "SELECT delivered FROM write_check_waves WHERE id='write:job:summary-old'",
            (),
            |row| Ok(row.get::<i64>(0)?)
        )
        .await
        .unwrap(),
        Some(0),
        "supersession is not a delivery receipt"
    );
    drop(old);
    drop(newest);
    drop(first);
    let second = Orchestrator::builder(
        orch.db.clone(),
        Arc::new(TestServicesBuilder::new().build()),
        config_dir.to_path_buf(),
    )
    .jj_binary_path(bin.to_string())
    .build();
    write_wave::rearm_at_boot(&second).await;
    tokio::time::timeout(DEADLINE,async {
        loop {
            if db.query_opt("SELECT COUNT(*) FROM write_check_waves WHERE tool_use_id IN ('summary-old','summary-new') AND delivered=1",
                (),|row|Ok(row.get::<i64>(0)?)).await.unwrap()==Some(2) { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("the newer durable summary receipts both waves");
    let content = db
        .query_text(
            "SELECT content FROM messages WHERE id='reconcile:write-checks:job:summary-new'",
            (),
        )
        .await
        .unwrap()
        .unwrap();
    assert!(
        content.contains("supersedes 1 earlier pending wave"),
        "{content}"
    );
    write_wave::rearm_at_boot(&second).await;
    assert_eq!(
        db.query_opt(
            "SELECT COUNT(*) FROM messages WHERE id LIKE 'reconcile:write-checks:job:summary-%'",
            (),
            |row| Ok(row.get::<i64>(0)?)
        )
        .await
        .unwrap(),
        Some(1),
        "restart delivers the newest summary once, retaining the folded notice"
    );
    // B already delivered a newer pass before restart. A's retained older
    // failure must not resurrect as a warning after B leaves the delivery queue.
    db.execute(
        "UPDATE write_check_waves SET delivered=0,superseded=0,
        summary='Checks: ✗ fmt-a',verdicts_json='[[\"fmt-a\",false]]'
        WHERE tool_use_id='summary-old'",
        (),
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE write_check_waves SET verdicts_json='[[\"fmt-a\",true]]'
        WHERE tool_use_id='summary-new'",
        (),
    )
    .await
    .unwrap();
    let third = Orchestrator::builder(
        orch.db.clone(),
        Arc::new(TestServicesBuilder::new().build()),
        config_dir.to_path_buf(),
    )
    .jj_binary_path(bin.to_string())
    .build();
    write_wave::rearm_at_boot(&third).await;
    tokio::time::timeout(DEADLINE, async {
        loop {
            if db
                .query_opt(
                    "SELECT delivered FROM write_check_waves WHERE tool_use_id='summary-old'",
                    (),
                    |row| Ok(row.get::<i64>(0)?),
                )
                .await
                .unwrap()
                == Some(1)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the delivered newer pass suppresses the retained older failure");
    assert_eq!(
        db.query_opt(
            "SELECT COUNT(*) FROM messages WHERE id LIKE 'reconcile:write-checks:job:summary-%'",
            (),
            |row| Ok(row.get::<i64>(0)?)
        )
        .await
        .unwrap(),
        Some(1),
        "neither the delivered newer summary nor the covered older failure is sent again"
    );
}

/// Observation rows: check name to the commits it was recorded at, with the
/// output tail of the last.
async fn observations(db: &LocalDb) -> BTreeMap<String, (Vec<String>, String)> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT check_name, commit_sha, output_tail FROM check_result_observations
                     ORDER BY ran_at",
                    (),
                )
                .await?;
            let mut found: BTreeMap<String, (Vec<String>, String)> = BTreeMap::new();
            while let Some(row) = rows.next().await? {
                let entry = found.entry(row.text(0)?).or_default();
                entry.0.push(row.text(1)?);
                entry.1 = row.text(2)?;
            }
            Ok::<_, crate::storage::DbError>(found)
        })
    })
    .await
    .unwrap()
}

/// Two fixers and three verdicts on one write. The fixers run as two chained
/// change-returning requests, the second on the first's fold, and their
/// folds land as two commits. The verdicts run as three single-suite
/// pure-verdict requests on the second fold, all in flight at once; alpha's
/// and gamma's verdicts are recorded while beta's is still out. Gamma
/// rewrote a file without declaring `fixes`: its rewrite is reported with its
/// verdict and never folded.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(jj)]
async fn fixers_chain_as_requests_and_each_verdict_is_its_own_request_recorded_as_it_settles() {
    let Some(bin) = jj_bin() else {
        return;
    };
    let home = tempfile::tempdir().unwrap();
    let project = tempfile::tempdir().unwrap();
    let repo = std::fs::canonicalize(project.path()).unwrap();
    init_project(&repo);
    let base = commit(
        &repo,
        &[
            (".cairn/config.yaml", contract().as_str()),
            ("a.txt", "a\n"),
            ("b.txt", "b\n"),
        ],
        "declare the write checks",
    );
    git(&repo, &["checkout", "-q", "-b", BRANCH]);
    let sealed = commit(
        &repo,
        &[("a.txt", "unformatted a\n"), ("b.txt", "unformatted b\n")],
        "the write",
    );
    git(&repo, &["checkout", "-q", "main"]);

    let config_dir = home.path().to_path_buf();
    let coordinates = {
        let jj = crate::jj::environment(&bin, &config_dir);
        let (config_dir, repo) = (config_dir.clone(), repo.clone());
        tokio::task::spawn_blocking(move || {
            crate::jj::coordinate_repository(&jj, &config_dir, &repo)
        })
        .await
        .unwrap()
        .unwrap()
    };
    let local = LocalDb::open_migrated_for_test(config_dir.join("checks.db"))
        .await
        .unwrap();
    let repo_path = repo.to_string_lossy().into_owned();
    local
        .execute(
            "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES ('ws','w',1,1)",
            (),
        )
        .await
        .unwrap();
    local
        .execute(
            "INSERT INTO projects(id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
                 VALUES ('proj','ws','P','p',?1,'main',1,1)",
            (repo_path.as_str(),),
        )
        .await
        .unwrap();
    local
        .execute_script(
            "INSERT INTO issues(id,project_id,number,title,status,progress,attention,created_at,updated_at)
               VALUES('issue','proj',7,'I','active','active','none',1,1);
             INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq)
               VALUES('exec','r','issue','proj','running',1,1);
             INSERT INTO jobs(id,project_id,issue_id,execution_id,recipe_node_id,status,node_name,uri_segment,branch,created_at,updated_at)
               VALUES('job','proj','issue','exec','node','running','builder','builder','agent/feature',1,1);
             INSERT INTO runs(id, issue_id, project_id, job_id, status, created_at, updated_at)
               VALUES('run', 'issue', 'proj', 'job', 'live', 1, 1);",
        )
        .await
        .unwrap();
    let search = Arc::new(SearchIndex::open_or_create(config_dir.join("search")).unwrap());
    let mut orch = Orchestrator::builder(
        Arc::new(DbState::new(Arc::new(local), search)),
        Arc::new(TestServicesBuilder::new().build()),
        config_dir.clone(),
    )
    .jj_binary_path(bin.clone())
    .build();

    // The colocated machine claims every offer and reports each grant. A
    // fixer is answered at once with its rewrite. The verdicts are held until
    // all three are granted; then every one but beta is answered, and beta
    // waits for the test.
    let (grants_tx, mut grants) = mpsc::unbounded_channel::<Grant>();
    let release_held = Arc::new(tokio::sync::Notify::new());
    let mut machine = connect_machine(&orch, &repo, grants_tx.clone(), release_held.clone());

    let run_context = RunContext {
        run_id: "run".into(),
        job_id: JOB.into(),
        exec_seq: Some(1),
        issue_id: Some("issue".into()),
        issue_number: Some(7),
        thread_id: None,
        project_id: "proj".into(),
        project_key: "p".into(),
        job_name: Some("builder".into()),
        agent_config_id: None,
    };
    let sealed_resolution = BranchResolution {
        project_id: "proj".into(),
        repository_path: coordinates,
        object_repository_path: repo.clone(),
        rev: BRANCH.into(),
        commit_id: sealed.clone(),
        default_commit_id: base,
        default_branch: "main".into(),
    };
    let (progress, progress_seen) = tokio::sync::watch::channel(WaveProgress::default());
    let wave_orch = orch.clone();
    let claims = orch.lane_claims.open_wave(JOB);
    let ticket = orch.late_write_checks.open(JOB, "tool-use");
    let ceiling = Duration::from_secs(600);
    let guard = WriteChecksInFlightGuard::new(&orch, JOB);
    write_wave::crash_after_raw_for_test("write:job:tool-use");
    write_wave::crash_after_fold_for_test("write:job:tool-use");
    let replay_run = run_context.clone();
    let replay_resolution = sealed_resolution.clone();
    let replay_cwd = repo_path.clone();
    let wave = cairn_common::task::spawn(async move {
        await_write_check_wave(
            guard,
            InlineWaitBounds {
                start: ceiling,
                total: ceiling,
            },
            progress_seen,
            async move {
                run_write_checks_after_seal_inner(
                    &wave_orch,
                    Some(&run_context),
                    &repo_path,
                    Some(&sealed_resolution),
                    progress,
                    claims,
                    &ticket,
                )
                .await
            },
            |_late: String| std::future::ready(()),
        )
        .await
    });

    // 1. Two fixers, two chained change-returning requests: fmt-a on the
    // sealed commit, fmt-b on the commit fmt-a's fold landed.
    let first = tokio::time::timeout(DEADLINE, grants.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.suite, "fmt-a");
    let interrupted = tokio::time::timeout(DEADLINE, wave)
        .await
        .expect("the coordinator stops at the crash boundary")
        .unwrap();
    assert!(
        interrupted.is_some(),
        "the supervisor reports the interrupted wave"
    );
    assert_eq!(
        orch.db
            .local
            .query_opt(
                "SELECT completed FROM write_check_waves WHERE id='write:job:tool-use'",
                (),
                |row| Ok(row.get::<i64>(0)?)
            )
            .await
            .unwrap(),
        Some(0),
        "a lost coordinator has not completed its durable wave"
    );
    assert_eq!(
        git_stdout(&repo, &["rev-parse", &format!("refs/heads/{BRANCH}")]),
        sealed,
        "the first crash retained the delta before any branch fold"
    );
    machine.abort();
    assert!(machine.await.unwrap_err().is_cancelled());
    let first_request = orch
        .db
        .local
        .query_text(
            "SELECT id FROM check_requests WHERE consumer='write' AND suite='fmt-a'",
            (),
        )
        .await
        .unwrap()
        .unwrap();
    requests::forget_driver_for_test(&first_request);
    orch = Orchestrator::builder(
        orch.db.clone(),
        Arc::new(TestServicesBuilder::new().build()),
        config_dir.clone(),
    )
    .jj_binary_path(bin.clone())
    .build();
    machine = connect_machine(&orch, &repo, grants_tx.clone(), release_held.clone());
    write_wave::crash_after_record_for_test("write:job:tool-use");
    let resume_orch = orch.clone();
    let resume_run = replay_run.clone();
    let resume_resolution = replay_resolution.clone();
    let resume_cwd = replay_cwd.clone();
    let wave = cairn_common::task::spawn(async move {
        let ticket = resume_orch.late_write_checks.open(JOB, "tool-use");
        let claims = resume_orch.lane_claims.open_wave(JOB);
        let (progress, _) = tokio::sync::watch::channel(WaveProgress::default());
        run_write_checks_after_seal_inner(
            &resume_orch,
            Some(&resume_run),
            &resume_cwd,
            Some(&resume_resolution),
            progress,
            claims,
            &ticket,
        )
        .await
    });
    assert!(
        tokio::time::timeout(DEADLINE, wave)
            .await
            .unwrap()
            .unwrap_err()
            .is_panic(),
        "the second runner stops after the fold checkpoint, before successor planning"
    );
    assert_eq!(
        observations(&orch.db.local).await.len(),
        0,
        "the fold checkpoint does not prematurely record a fixer verdict"
    );
    machine.abort();
    assert!(machine.await.unwrap_err().is_cancelled());
    requests::forget_driver_for_test(&first_request);
    orch = Orchestrator::builder(
        orch.db.clone(),
        Arc::new(TestServicesBuilder::new().build()),
        config_dir.clone(),
    )
    .jj_binary_path(bin.clone())
    .build();
    machine = connect_machine(&orch, &repo, grants_tx.clone(), release_held.clone());
    let resume_orch = orch.clone();
    let resume_run = replay_run.clone();
    let resume_resolution = replay_resolution.clone();
    let resume_cwd = replay_cwd.clone();
    let wave = cairn_common::task::spawn(async move {
        let ticket = resume_orch.late_write_checks.open(JOB, "tool-use");
        let claims = resume_orch.lane_claims.open_wave(JOB);
        let (progress, _) = tokio::sync::watch::channel(WaveProgress::default());
        run_write_checks_after_seal_inner(
            &resume_orch,
            Some(&resume_run),
            &resume_cwd,
            Some(&resume_resolution),
            progress,
            claims,
            &ticket,
        )
        .await
    });
    let second = tokio::time::timeout(DEADLINE, grants.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second.suite, "fmt-b");
    assert!(
        tokio::time::timeout(DEADLINE, wave)
            .await
            .unwrap()
            .unwrap_err()
            .is_panic(),
        "the second runner stops after final observations, before verifier successors"
    );
    assert_eq!(
        observations(&orch.db.local).await.len(),
        2,
        "each fixer has one final-fold observation at the second crash"
    );
    machine.abort();
    assert!(machine.await.unwrap_err().is_cancelled());
    orch = Orchestrator::builder(
        orch.db.clone(),
        Arc::new(TestServicesBuilder::new().build()),
        config_dir.clone(),
    )
    .jj_binary_path(bin.clone())
    .build();
    machine = connect_machine(&orch, &repo, grants_tx, release_held.clone());
    let resume_orch = orch.clone();
    let resume_run = replay_run.clone();
    let resume_resolution = replay_resolution.clone();
    let resume_cwd = replay_cwd.clone();
    let wave = cairn_common::task::spawn(async move {
        let ticket = resume_orch.late_write_checks.open(JOB, "tool-use");
        let claims = resume_orch.lane_claims.open_wave(JOB);
        let (progress, _) = tokio::sync::watch::channel(WaveProgress::default());
        run_write_checks_after_seal_inner(
            &resume_orch,
            Some(&resume_run),
            &resume_cwd,
            Some(&resume_resolution),
            progress,
            claims,
            &ticket,
        )
        .await
    });
    let mut seen = vec![first, second];
    for _ in 2..2 + VERDICTS.len() {
        let grant = tokio::time::timeout(DEADLINE, grants.recv())
            .await
            .unwrap_or_else(|_| panic!("every request is granted while beta is held; saw {seen:?}"))
            .expect("the machine reports its grants");
        seen.push(grant);
    }
    for grant in &seen {
        assert_eq!(grant.items, 1, "one suite, one batch item: {grant:?}");
    }
    let [fmt_a, fmt_b, verdict_grants @ ..] = seen.as_slice() else {
        unreachable!()
    };
    assert_eq!(
        (
            fmt_a.suite.as_str(),
            &fmt_a.policy,
            fmt_a.base_commit.as_str()
        ),
        ("fmt-a", &MutationPolicy::AllowDelta, sealed.as_str()),
        "the first fixer runs on the sealed commit"
    );
    assert_eq!(
        (fmt_b.suite.as_str(), &fmt_b.policy),
        ("fmt-b", &MutationPolicy::AllowDelta)
    );
    let fold_a = fmt_b.base_commit.clone();
    assert_ne!(fold_a, sealed, "the second fixer runs on the first's fold");
    assert_eq!(
        git_stdout(&repo, &["show", &format!("{fold_a}:a.txt")]),
        fixed("a.txt").trim(),
        "and sees its rewrite"
    );
    assert_eq!(
        git_stdout(&repo, &["rev-parse", &format!("{fold_a}^")]),
        sealed,
        "fmt-a's fold is a commit of its own on the sealed commit"
    );

    // 2. Three verdicts, three pure-verdict requests, all on fmt-b's fold and
    // all granted before any of them is answered.
    let fold_b = verdict_grants[0].base_commit.clone();
    assert_eq!(
        verdict_grants
            .iter()
            .map(|grant| grant.suite.as_str())
            .collect::<BTreeSet<_>>(),
        VERDICTS.into_iter().collect::<BTreeSet<_>>()
    );
    for grant in verdict_grants {
        assert_eq!(grant.policy, MutationPolicy::PureVerdict, "{grant:?}");
        assert_eq!(
            grant.base_commit, fold_b,
            "every verdict is about the tree that lands"
        );
    }
    assert_eq!(
        git_stdout(&repo, &["rev-parse", &format!("{fold_b}^")]),
        fold_a,
        "fmt-b's fold is a commit of its own on fmt-a's"
    );
    assert_eq!(
        seen.iter()
            .map(|grant| grant.request_id.as_str())
            .collect::<BTreeSet<_>>()
            .len(),
        seen.len(),
        "distinct requests: {seen:?}"
    );

    // 3. Alpha's and gamma's verdicts are recorded while beta's request is
    // still out, and the wave waits for beta.
    let db = orch.db.local.clone();
    let answered = tokio::time::timeout(DEADLINE, async {
        loop {
            let found = observations(&db).await;
            if found.contains_key("alpha") && found.contains_key(UNDECLARED) {
                break found;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("each answered verdict is recorded as its own request settles");
    assert!(
        !answered.contains_key(HELD),
        "beta has no verdict while its result is held: {answered:?}"
    );
    assert!(!wave.is_finished(), "the wave waits for beta");

    // Repeated boot scans cannot compete with the live wave for sequencing.
    write_wave::rearm_at_boot(&orch).await;
    write_wave::rearm_at_boot(&orch).await;
    assert!(!wave.is_finished());
    release_held.notify_one();
    let summary = tokio::time::timeout(DEADLINE, wave)
        .await
        .expect("the wave finishes once beta settles")
        .unwrap()
        .expect("the wave answers with a summary");
    let _executor = machine.await.unwrap();
    assert!(
        grants.try_recv().is_err(),
        "no check ran twice: neither fold invalidated the other fixer"
    );

    // 4. Every lane is recorded once, at the commit the chain ended at, and
    // the branch carries both fixers' rewrites and nothing of gamma's.
    let recorded = observations(&db).await;
    for name in ["fmt-a", "fmt-b", "alpha", "beta", "gamma"] {
        let (commits, _) = recorded
            .get(name)
            .unwrap_or_else(|| panic!("{name} is recorded: {recorded:?}"));
        assert_eq!(commits, &vec![fold_b.clone()], "{name}");
    }
    assert_eq!(
        git_stdout(&repo, &["rev-parse", &format!("refs/heads/{BRANCH}")]),
        fold_b,
        "two folds, and no third for gamma's undeclared rewrite"
    );
    for path in ["a.txt", "b.txt"] {
        assert_eq!(
            git_stdout(&repo, &["show", &format!("{fold_b}:{path}")]),
            fixed(path).trim()
        );
    }
    assert!(
        summary.contains(&format!(
            "fixed, {}, {}; 2 files",
            &fold_a[..12],
            &fold_b[..12]
        )),
        "{summary}"
    );
    for verdict in VERDICTS {
        assert!(
            summary.contains(&format!("\u{2713} {verdict}")),
            "{summary}"
        );
    }

    // 5. Gamma's rewrite is reported with its verdict, through the executor's
    // tracked-modification evidence, and was discarded.
    let (_, gamma_tail) = &recorded[UNDECLARED];
    assert!(
        gamma_tail.contains("note: check modified tracked paths: a.txt")
            && gamma_tail.contains("changes were discarded"),
        "{gamma_tail}"
    );
    // A coordinator lost after recording but before successor/delivery can replay
    // the original sealed wave even though the branch is now at its final fold.
    // Durable lane identities and fold checkpoints must make this a read-only replay.
    let row_ids = db
        .query_all(
            "SELECT id FROM check_requests WHERE consumer='write' ORDER BY id",
            (),
            |row| Ok(row.get::<String>(0)?),
        )
        .await
        .unwrap();
    assert_eq!(row_ids.len(), 5);
    db.execute(
        "UPDATE write_check_waves SET snapshot_json=json_set(snapshot_json,
        '$.run.project_id','historical-project','$.run.job_id','historical-job',
        '$.run.run_id','historical-run','$.logical.project_id','historical-project')
        WHERE id='write:job:tool-use'",
        (),
    )
    .await
    .unwrap();
    let mut moved_resolution = replay_resolution;
    moved_resolution.commit_id = fold_b.clone();
    let ticket = orch.late_write_checks.open(JOB, "tool-use");
    let claims = orch.lane_claims.open_wave(JOB);
    let (progress, _) = tokio::sync::watch::channel(WaveProgress::default());
    let replay = tokio::time::timeout(
        DEADLINE,
        run_write_checks_after_seal_inner(
            &orch,
            Some(&replay_run),
            &replay_cwd,
            Some(&moved_resolution),
            progress,
            claims,
            &ticket,
        ),
    )
    .await
    .expect("replay joins the settled original lanes");
    assert!(replay
        .as_deref()
        .is_some_and(|summary| summary.contains(&fold_a[..12]) && summary.contains(&fold_b[..12])));
    assert_eq!(
        observations(&db).await,
        recorded,
        "replay records no second verdict"
    );
    assert_eq!(
        db.query_all(
            "SELECT id FROM check_requests WHERE consumer='write' ORDER BY id",
            (),
            |row| Ok(row.get::<String>(0)?)
        )
        .await
        .unwrap(),
        row_ids,
        "replay files no second lane"
    );
    assert_eq!(
        git_stdout(&repo, &["rev-parse", &format!("refs/heads/{BRANCH}")]),
        fold_b,
        "replay folds neither delta twice"
    );
    assert!(grants.try_recv().is_err(), "replay executes nothing");
    let stored = db
        .query_text(
            "SELECT summary FROM write_check_waves WHERE id=?1",
            ("write:job:tool-use".to_string(),),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(Some(stored), replay);
    drop(ticket);
    assert_superseded_summary_restart(&orch, &config_dir, &bin).await;
    let fold_b =
        assert_active_legacy_successor(&orch, &repo, &replay_run, &first_request, &fold_b).await;
    let recorded = observations(&db).await;

    // A retired job cannot replay a publication, even when the crash left a raw
    // delta and no fold checkpoint. The row must say the historical outcome is
    // unknown rather than claim that the fold never happened.
    db.execute("UPDATE jobs SET status='cancelled' WHERE id='job'", ())
        .await
        .unwrap();
    db.execute("UPDATE write_check_waves SET completed=0,delivered=0,summary=NULL WHERE id='write:job:tool-use'",())
        .await.unwrap();
    db.execute("UPDATE check_requests SET state='running',outcome_json=NULL,observation_id=NULL,
        context_json=json_set(context_json,'$.recorded',json('false'),'$.final_recording',NULL,'$.terminal',NULL,'$.fold',NULL)
        WHERE id=?1",(first_request.clone(),)).await.unwrap();
    requests::forget_driver_for_test(&first_request);
    let ticket = orch.late_write_checks.open(JOB, "tool-use");
    let (progress, _) = tokio::sync::watch::channel(WaveProgress::default());
    let retired = tokio::time::timeout(
        DEADLINE,
        run_write_checks_after_seal_inner(
            &orch,
            Some(&replay_run),
            &replay_cwd,
            Some(&moved_resolution),
            progress,
            orch.lane_claims.open_wave(JOB),
            &ticket,
        ),
    )
    .await
    .expect("retirement is terminal, not an endless publication retry")
    .unwrap();
    assert!(
        retired.contains("job retired before the fold's outcome was recorded; not determined"),
        "{retired}"
    );
    let evidence = db
        .query_text(
            "SELECT evidence FROM check_requests WHERE id=?1",
            (first_request,),
        )
        .await
        .unwrap()
        .unwrap();
    assert!(
        evidence.contains("job retired before the fold's outcome was recorded; not determined"),
        "{evidence}"
    );
    assert_eq!(
        git_stdout(&repo, &["rev-parse", &format!("refs/heads/{BRANCH}")]),
        fold_b,
        "nothing is published after retirement"
    );
    assert_eq!(
        observations(&db).await,
        recorded,
        "retirement fabricates no final observation"
    );
    assert!(
        grants.try_recv().is_err(),
        "retirement grants no replacement execution"
    );
}
