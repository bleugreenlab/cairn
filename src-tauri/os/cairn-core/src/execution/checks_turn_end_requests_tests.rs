//! A turn-end review wave driven end to end against a colocated fake
//! executor: [`run_turn_end_checks`] on a real jj-coordinated project whose
//! head declares three `when: review` checks and has no cached verdicts.
//!
//! What it pins is the request shape of a wave: every missed suite is its own
//! single-item request with its own affinity key, each suite's verdict is
//! recorded when its own request settles (not when the slowest sibling does),
//! and the wave's one attention delivery waits for every request.

use super::*;
use crate::db::DbState;
use crate::fleet::{CellOutcome, CellRequest};
use crate::jj::tests::{git, git_stdout, init_project, jj_bin};
use crate::services::testing::TestServicesBuilder;
use crate::storage::{RowExt, SearchIndex};
use cairn_common::executor_protocol::{
    CellExecutionMeta, ExecutorMessage, ProcessBatchItemOutcome,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

const JOB: &str = "job";
const BRANCH: &str = "agent/feature";
const SUITES: [&str; 3] = ["alpha", "beta", "gamma"];
const HELD: &str = "beta";
/// A deadlock guard, never an oracle: every wait below is for an event the
/// wave produces in milliseconds.
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

fn review_contract() -> String {
    let mut config = String::from("checks:\n");
    for suite in SUITES {
        config.push_str(&format!(
            "  {suite}:\n    command: echo {suite}\n    when: review\n    impact:\n      - \"**\"\n"
        ));
    }
    config
}

/// The colocated machine's answer to a one-item batch: the item ran and
/// exited zero.
fn passed_batch(request: &CellRequest, header: &str, generation: u64) -> CellOutcome {
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
        tracked_modifications: None,
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
        mutation_delta: None,
        sandbox_denials: Vec::new(),
        tracked_modifications: None,
        node_scratch: None,
    }
}

/// What the machine saw of one granted request.
#[derive(Debug)]
struct Grant {
    affinity_key: Option<String>,
    request_id: String,
    items: usize,
}

/// Observation rows per check name.
async fn observations(db: &LocalDb) -> BTreeMap<String, i64> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT check_name, COUNT(*) FROM check_result_observations
                     GROUP BY check_name",
                    (),
                )
                .await?;
            let mut found = BTreeMap::new();
            while let Some(row) = rows.next().await? {
                found.insert(row.text(0)?, row.i64(1)?);
            }
            Ok::<_, crate::storage::DbError>(found)
        })
    })
    .await
    .unwrap()
}

/// The turn-check attention pushes written for the job: one per delivery.
async fn deliveries(db: &LocalDb) -> i64 {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT COUNT(*) FROM attention_pushes
                     WHERE recipient = ?1 AND key LIKE 'turn-checks:%'",
                    (JOB,),
                )
                .await?;
            rows.next().await?.unwrap().i64(0)
        })
    })
    .await
    .unwrap()
}

/// Three misses become three single-suite requests; alpha and gamma are
/// answered while beta's result is held. Each answered suite's verdict is
/// recorded while beta is still out, and the wave delivers once, only after
/// beta settles.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(jj)]
async fn each_missed_suite_is_its_own_request_recorded_as_it_settles_and_delivered_once() {
    let Some(bin) = jj_bin() else {
        return;
    };
    let home = tempfile::tempdir().unwrap();
    let project = tempfile::tempdir().unwrap();
    let repo = project.path().to_path_buf();
    init_project(&repo);
    commit(
        &repo,
        &[(".cairn/config.yaml", review_contract().as_str())],
        "declare three review checks",
    );
    git(&repo, &["checkout", "-q", "-b", BRANCH]);
    commit(&repo, &[("feature.rs", "feature\n")], "the change");
    git(&repo, &["checkout", "-q", "main"]);

    let config_dir = home.path().to_path_buf();
    {
        let jj = crate::jj::environment(&bin, &config_dir);
        let (config_dir, repo) = (config_dir.clone(), repo.clone());
        tokio::task::spawn_blocking(move || {
            crate::jj::coordinate_repository(&jj, &config_dir, &repo)
        })
        .await
        .unwrap()
        .unwrap();
    }
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
             INSERT INTO jobs(id,project_id,issue_id,execution_id,status,node_name,uri_segment,branch,created_at,updated_at)
               VALUES('job','proj','issue','exec','running','builder','builder','agent/feature',1,1);",
        )
        .await
        .unwrap();
    let search = Arc::new(SearchIndex::open_or_create(config_dir.join("search")).unwrap());
    let orch = Orchestrator::builder(
        Arc::new(DbState::new(Arc::new(local), search)),
        Arc::new(TestServicesBuilder::new().build()),
        config_dir.clone(),
    )
    .jj_binary_path(bin.clone())
    .build();

    // The colocated machine claims every offer and reports each grant. It
    // answers every suite at once except beta, whose result it holds until
    // the test releases it.
    let (sender, mut executor) = mpsc::unbounded_channel();
    let generation = orch.fleet.attach_executor(sender);
    let fleet = orch.fleet.clone();
    let (grants_tx, mut grants) = mpsc::unbounded_channel::<Grant>();
    let (release_held, held_released) = oneshot::channel::<()>();
    let machine = tokio::spawn(async move {
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
        let mut granted = 0;
        let mut held = None;
        while granted < SUITES.len() {
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
                    granted += 1;
                    grants_tx
                        .send(Grant {
                            affinity_key: request.affinity_key.clone(),
                            request_id: request.request_id.clone(),
                            items: batch.items.len(),
                        })
                        .unwrap();
                    let outcome = passed_batch(&request, &batch.items[0].header, generation);
                    let is_held = request
                        .affinity_key
                        .as_deref()
                        .is_some_and(|key| key.ends_with(&format!(":{HELD}")));
                    if is_held {
                        held = Some((request, outcome));
                    } else {
                        answer(&request, outcome);
                    }
                }
                Some(_) => continue,
                None => panic!("the wave's checks are offered to the colocated executor"),
            }
        }
        held_released.await.unwrap();
        let (request, outcome) = held.expect("beta was granted");
        answer(&request, outcome);
        // Kept open so the executor stays attached while the wave finishes.
        executor
    });

    let attempt = request_turn_end_attempt(&orch, JOB).expect("persist the attempt");
    transition_turn_end_attempt(&orch, JOB, &attempt, "claimed", None).expect("claim it");
    let slot = orch
        .try_begin_turn_end_checks(JOB, &attempt)
        .expect("the slot is free");
    let wave = tokio::spawn(run_turn_end_checks(orch.clone(), JOB.to_string(), slot));

    // 1. Three misses, three separate single-item requests.
    let mut seen = Vec::new();
    for _ in SUITES {
        let grant = tokio::time::timeout(DEADLINE, grants.recv())
            .await
            .unwrap_or_else(|_| {
                panic!("every missed suite is granted while beta is held; saw {seen:?}")
            })
            .expect("the machine reports its grants");
        seen.push(grant);
    }
    for grant in &seen {
        assert_eq!(grant.items, 1, "one suite, one batch item: {grant:?}");
    }
    let affinity: BTreeSet<_> = seen
        .iter()
        .map(|grant| grant.affinity_key.clone().expect("each suite has a key"))
        .collect();
    assert_eq!(
        affinity,
        SUITES
            .iter()
            .map(|suite| check_affinity_key(JOB, suite))
            .collect::<BTreeSet<_>>(),
    );
    assert_eq!(
        affinity,
        SUITES
            .iter()
            .map(|suite| format!("{JOB}:{suite}"))
            .collect::<BTreeSet<_>>(),
        "the key names the job and the suite"
    );
    let request_ids: BTreeSet<_> = seen.iter().map(|grant| grant.request_id.as_str()).collect();
    assert_eq!(
        request_ids.len(),
        SUITES.len(),
        "distinct requests: {seen:?}"
    );

    // 2. Alpha's and gamma's verdicts land while beta's request is still out.
    let db = orch.db.local.clone();
    let answered = tokio::time::timeout(DEADLINE, async {
        loop {
            let found = observations(&db).await;
            if found.contains_key("alpha") && found.contains_key("gamma") {
                break found;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("each answered suite is recorded as its own request settles");
    assert!(
        !answered.contains_key(HELD),
        "beta has no verdict while its result is held: {answered:?}"
    );
    assert!(!wave.is_finished(), "the wave waits for beta");
    // 3. Nothing is delivered while a request is still out.
    assert_eq!(
        deliveries(&db).await,
        0,
        "no delivery before the last request settles"
    );

    release_held.send(()).unwrap();
    tokio::time::timeout(DEADLINE, wave)
        .await
        .expect("the wave finishes once beta settles")
        .unwrap();
    let _executor = machine.await.unwrap();

    let recorded = observations(&db).await;
    assert_eq!(
        recorded,
        SUITES
            .iter()
            .map(|suite| (suite.to_string(), 1))
            .collect::<BTreeMap<_, _>>(),
        "each suite is recorded exactly once"
    );
    assert_eq!(
        deliveries(&db).await,
        1,
        "the wave delivers once, after its last request"
    );
    let current = current_turn_end_attempt(&db, JOB)
        .await
        .unwrap()
        .expect("the attempt is current");
    assert_eq!(current.id, attempt);
    assert_eq!(current.state, "completed", "{:?}", current.reason);
}
