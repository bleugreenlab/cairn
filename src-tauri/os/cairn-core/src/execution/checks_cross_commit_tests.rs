//! A content-identical later commit can publish first without becoming the physical source.
use super::adoption_tests::{assert_no_release, batch_item, boot, claim, observe_retry, result};
use super::*;
use crate::storage::{check_lane_adoptions as claims, RowExt};
use cairn_common::executor_protocol::ExecutorMessage;
use std::sync::atomic::{AtomicUsize, Ordering};

const REMOTE: &str = "retained-check-remote";
const REQUEST: &str = "cross-commit-physical";
const ATTEMPT: &str = "cross-commit-attempt";

#[tokio::test]
#[serial_test::serial(jj)]
async fn cross_commit_live_follower_publishes_physical_source_and_projects_both_manual_handles() {
    cross_commit(false, false).await;
}

#[tokio::test]
#[serial_test::serial(jj)]
async fn cross_commit_live_follower_publication_replays_after_observation_before_waiter_crash() {
    cross_commit(true, false).await;
}

async fn assert_projection(
    db: &Arc<LocalDb>,
    a: &str,
    b: &str,
    tree_b: &str,
    input: &str,
    handles: &[String],
) {
    let id = format!("remote-check:{REQUEST}:{ATTEMPT}:smoke");
    let sources = db
        .query_all(
            "SELECT id,commit_sha,defined_by_commit_sha,input_hash FROM check_result_observations
         WHERE project_id='p'",
            (),
            |row| Ok((row.text(0)?, row.text(1)?, row.text(2)?, row.text(3)?)),
        )
        .await
        .unwrap();
    assert_eq!(
        sources,
        vec![(id.clone(), a.into(), a.into(), input.into())],
        "the physical A grant, not the B publisher, owns the only immutable observation"
    );
    let alias = db.query_opt(
        "SELECT source_observation_id,defined_by_commit_sha,tree_hash,input_hash,disposition
         FROM check_result_commit_aliases WHERE project_id='p' AND commit_sha=?1 AND check_name='smoke'",
        (b.to_string(),), |row| Ok((row.text(0)?,row.text(1)?,row.text(2)?,row.text(3)?,row.text(4)?)),
    ).await.unwrap().expect("B needs its own audited cached alias");
    assert_eq!(
        alias,
        (
            id.clone(),
            b.into(),
            tree_b.into(),
            input.into(),
            "cached".into()
        )
    );
    for (handle, commit) in handles.iter().zip([a, b]) {
        let attached = db.query_opt(
            "SELECT observation_id,commit_sha,input_hash FROM manual_check_handles WHERE handle=?1",
            (handle.clone(),), |row| Ok((row.text(0)?,row.text(1)?,row.text(2)?)),
        ).await.unwrap().unwrap();
        assert_eq!(
            attached,
            (id.clone(), commit.into(), input.into()),
            "each manual handle must resolve to the exact physical source at its own commit"
        );
    }
}

#[tokio::test]
#[serial_test::serial(jj)]
async fn cross_commit_live_consumer_aborted_after_result_does_not_pin_physical_ack() {
    cross_commit(false, true).await;
}

async fn cross_commit(crash: bool, abort_consumer: bool) {
    use crate::jj::tests::{git, git_stdout, init_project, jj_bin};
    jj_bin().expect("cross-commit adoption requires the pinned jj tool");
    let home = tempfile::tempdir().unwrap();
    let repo = home.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    init_project(&repo);
    std::fs::create_dir_all(repo.join(".cairn")).unwrap();
    std::fs::write(repo.join("checked.txt"), "unchanged check input\n").unwrap();
    std::fs::write(repo.join(".cairn/config.yaml"),
        "checks:\n  smoke:\n    command: echo smoke\n    impact: [checked.txt]\n    executor: {os: linux}\n    verdictPlatforms: [linux]\n    mainCadence:\n      platforms: [linux]\n      notify: testing\n"
    ).unwrap();
    git(&repo, &["add", "-A"]);
    git(&repo, &["commit", "-q", "-m", "physical A"]);
    let a = git_stdout(&repo, &["rev-parse", "HEAD"]);
    std::fs::write(repo.join("README.md"), "documentation-only B\n").unwrap();
    git(&repo, &["add", "-A"]);
    git(&repo, &["commit", "-q", "-m", "documentation B"]);
    let b = git_stdout(&repo, &["rev-parse", "HEAD"]);
    assert_ne!(a, b);
    let db = Arc::new(
        LocalDb::open_migrated_for_test(home.path().join("runner.db"))
            .await
            .unwrap(),
    );
    db.execute(
        "INSERT INTO projects(id,workspace_id,name,key,repo_path,default_branch,created_at,updated_at)
         VALUES('p','default','P','P',?1,'main',1,1)",
        (repo.to_string_lossy().into_owned(),),
    ).await.unwrap();
    if crash {
        db.execute_script(
            "INSERT INTO threads(id,project_id,name,status,attention,created_at,updated_at)
             VALUES('testing','p','testing','active','none',1,1);
             INSERT INTO jobs(id,thread_id,project_id,node_name,uri_segment,status,created_at,updated_at)
             VALUES('testing-job','testing','p','thread','thread','idle',1,1);",
        )
        .await
        .unwrap();
    }
    let orch = boot(home.path(), db.clone()).await;
    let mut frames = orch.fleet.attach_reported_machine_for_test(REMOTE, &[]);
    let target = |job: &str| CheckRunTarget {
        job_id: job.into(),
        project_id: "p".into(),
        project_key: "P".into(),
        repository_path: repo.to_string_lossy().into_owned(),
        attribution_target: "main".into(),
        attribution_commit: None,
    };
    let ca = resolve_check_contract_at_commit(
        &orch,
        db.clone(),
        target("manual-a"),
        a.clone(),
        "smoke",
        None,
    )
    .await
    .unwrap();
    let cb = resolve_check_contract_at_commit(
        &orch,
        db.clone(),
        target("manual-b"),
        b.clone(),
        "smoke",
        None,
    )
    .await
    .unwrap();
    assert_ne!(ca.context.tree_hash, cb.context.tree_hash);
    assert_eq!(
        ca.context.input_hash, cb.context.input_hash,
        "the impact selector must really coalesce the doc-only commit, not a fabricated input key"
    );
    let input = ca.context.input_hash.clone();
    let tree_b = cb.context.tree_hash.clone();
    let handles = vec![
        uuid::Uuid::new_v4().to_string(),
        uuid::Uuid::new_v4().to_string(),
    ];
    let streams = handles
        .iter()
        .map(|h| format!("manual-check:run:handle={h}:0"))
        .collect::<Vec<_>>();
    for handle in &handles {
        db.execute(
            "INSERT INTO manual_check_handles(handle,project_id,created_at) VALUES(?1,'p',1)",
            (handle.clone(),),
        )
        .await
        .unwrap();
    }
    let mut physical = claim("running", "manual");
    physical.request_id = REQUEST.into();
    physical.attempt_id = ATTEMPT.into();
    physical.project_id = "p".into();
    physical.commit_sha = a.clone();
    physical.consumer_ref = handles[0].clone();
    physical.input_hashes = vec![input.clone()];
    physical.recording_plans = vec![serde_json::to_value(CheckAdoptionRecordingPlan::new(
        &ca.plan,
        &ca.sealed_coordinate,
        "manual-a",
        &streams[0],
        &[],
    ))
    .unwrap()];
    orch.fleet
        .register_check_claim((REQUEST.into(), ATTEMPT.into()), physical);
    let mut request = orch.fleet.cell_request_for_test(REQUEST);
    request.attempt_id = ATTEMPT.into();
    request.base_commit = a.clone();
    request.repository = RepositoryLocator::ScratchOnly {
        owner_id: "p".into(),
    };
    request.pinned_executor_id = Some(REMOTE.into());
    request.verdict_platforms = ca.plan.verdict_platforms.clone();
    request.placement_work_class = crate::fleet::PlacementWorkClass::ReviewChecks;
    request.requesting_job_id = Some("manual-a".into());
    request.wait_horizon_unix_ms = u64::MAX;
    let mut item = batch_item("smoke", &input, &streams[0]);
    item.process.args = vec![ca.plan.command.clone()];
    let ao = orch.clone();
    let mut raw_a = tokio::spawn(async move {
        ao.fleet
            .submit_pure_verdict_batch(&ao, request, vec![item], None, None, None)
            .await
    });
    let entry_id = loop {
        if let ExecutorMessage::Offer { entry_id, .. } =
            next_physical_frame(&orch, &mut frames, &mut raw_a, "Offer").await
        {
            break entry_id;
        }
    };
    orch.fleet
        .handle_executor_message(REMOTE, 1, ExecutorMessage::Claim { entry_id });
    loop {
        if let ExecutorMessage::Granted {
            request,
            batch: Some(batch),
            ..
        } = next_physical_frame(&orch, &mut frames, &mut raw_a, "Granted").await
        {
            assert_eq!(request.request_id, REQUEST);
            assert_eq!(request.base_commit, a);
            assert_eq!(batch.items.len(), 1);
            break;
        }
    }
    // Only cadence consumers have a boot adopter in this build. Admit a real
    // third consumer rather than pretending manual-only work survives shutdown.
    let raw_cadence = if crash {
        let key = crate::fleet::purpose::PurposeKey::main_cadence("p", "smoke", "linux");
        let mut cadence = claim("running", "main_cadence");
        cadence.request_id = "cross-commit-cadence".into();
        cadence.attempt_id = "cross-commit-cadence-attempt".into();
        cadence.project_id = "p".into();
        cadence.commit_sha = a.clone();
        cadence.input_hashes = vec![input.clone()];
        cadence.consumer_ref = key.to_string();
        cadence.recording_plans = vec![serde_json::to_value(CheckAdoptionRecordingPlan::new(
            &ca.plan,
            &ca.sealed_coordinate,
            "main:p",
            "main-cadence:p:smoke:linux",
            &[],
        ))
        .unwrap()];
        orch.fleet.register_check_claim(
            (cadence.request_id.clone(), cadence.attempt_id.clone()),
            cadence,
        );
        let mut request = orch.fleet.cell_request_for_test("cross-commit-cadence");
        request.attempt_id = "cross-commit-cadence-attempt".into();
        request.base_commit = a.clone();
        request.repository = RepositoryLocator::ScratchOnly {
            owner_id: "p".into(),
        };
        request.pinned_executor_id = Some(REMOTE.into());
        request.placement_work_class = crate::fleet::PlacementWorkClass::ReviewChecks;
        request.requesting_job_id = Some("main:p".into());
        request.wait_horizon_unix_ms = u64::MAX;
        let item = batch_item("smoke", &input, "main-cadence:p:smoke:linux");
        let co = orch.clone();
        let task = tokio::spawn(async move {
            co.fleet
                .submit_pure_verdict_batch(&co, request, vec![item], None, None, Some(key))
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(20), async {
            loop {
                let waiters = claims::waiters(&db, REQUEST, ATTEMPT).await.unwrap();
                if waiters.iter().any(|w| w.consumer == "main_cadence") {
                    break;
                }
                assert!(
                    !task.is_finished(),
                    "cadence must join the physical A grant"
                );
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the real cadence join provides shutdown and boot adoption ownership");
        Some(task)
    } else {
        None
    };
    let (_retry_guard, mut retries) =
        observe_retry(format!("remote-check:{REQUEST}:{ATTEMPT}:smoke"));
    if crash {
        db.execute_script(
            "CREATE TRIGGER cross_commit_crash BEFORE UPDATE OF state ON check_lane_adoption_waiters
             WHEN NEW.state='recorded' BEGIN SELECT RAISE(ABORT,'cross-commit crash after observation'); END;"
        ).await.unwrap();
    }
    if abort_consumer {
        db.execute_script(format!(
            "CREATE TRIGGER cross_commit_abort BEFORE UPDATE OF state ON check_lane_adoption_waiters
             WHEN NEW.state='delivered' AND NEW.consumer_ref='{}' BEGIN
             SELECT RAISE(ABORT,'cross-commit consumer held before delivery'); END;", handles[1],
        ))
        .await
        .unwrap();
    }
    let bo = orch.clone();
    let bd = db.clone();
    let bstream = streams[1].clone();
    let live_b = tokio::spawn(async move {
        execute_check_contract(
            &bo,
            bd,
            cb,
            CheckRunProducer {
                adopt: None,
                tool_use_id: bstream,
                node_kind: "manual-check".into(),
                owner_job_id: None,
                purpose: None,
            },
            true,
            Arc::new(|_| {}),
        )
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            let waiters = claims::waiters(&db, REQUEST, ATTEMPT).await.unwrap();
            if waiters.len() == if crash { 3 } else { 2 } {
                assert!(waiters
                    .iter()
                    .any(|w| w.consumer == "manual" && w.consumer_ref == handles[1]));
                break;
            }
            assert!(!live_b.is_finished(), "B must join the live A grant");
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("B durably joins the physical A grant across commits");
    let mut retained = result(&orch).outcome;
    if let CellOutcome::Completed {
        request_id,
        attempt_id,
        ..
    } = &mut retained
    {
        *request_id = REQUEST.into();
        *attempt_id = ATTEMPT.into();
    } else {
        panic!("fixture must produce a completed retained batch");
    }
    orch.fleet.handle_executor_message(
        REMOTE,
        1,
        ExecutorMessage::Result {
            request_id: REQUEST.into(),
            attempt_id: ATTEMPT.into(),
            outcome: retained.clone(),
        },
    );
    let mut deliveries = tokio::time::timeout(std::time::Duration::from_secs(20), raw_a)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(deliveries.len(), 1);
    let delivery = deliveries.remove(0);
    assert!(
        delivery.is_ok(),
        "the physical A result must really be delivered"
    );

    if crash || abort_consumer {
        let failure = tokio::time::timeout(std::time::Duration::from_secs(20), retries.recv())
            .await
            .expect("actual waiter write failure must follow the B publication")
            .unwrap();
        let expected = if crash {
            "cross-commit crash after observation"
        } else {
            "cross-commit consumer held before delivery"
        };
        assert!(failure.contains(expected), "{failure}");
        assert!(!live_b.is_finished());
    }
    if !crash {
        if abort_consumer {
            assert_projection(&db, &a, &b, &tree_b, &input, &handles).await;
            assert_no_release(&mut frames);
            let before = claims::waiters(&db, REQUEST, ATTEMPT).await.unwrap();
            assert_eq!(before.len(), 2);
            assert!(before.iter().all(|w| w.state == "recorded"));
            // This is ordinary caller cancellation, not runner shutdown. The
            // guard must withdraw just B without cancelling the shared result.
            live_b.abort();
            assert!(live_b.await.unwrap_err().is_cancelled());
            tokio::time::timeout(std::time::Duration::from_secs(20), async {
                loop {
                    let rows = claims::waiters(&db, REQUEST, ATTEMPT).await.unwrap();
                    if rows
                        .iter()
                        .any(|w| w.consumer_ref == handles[1] && w.state == "recorded_undelivered")
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("dropping B after Result must durably withdraw B's recorded consumer");
            assert_no_release(&mut frames);
        } else {
            let published = tokio::time::timeout(std::time::Duration::from_secs(20), live_b)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(
                published.observation_id.as_deref(),
                Some(format!("remote-check:{REQUEST}:{ATTEMPT}:smoke").as_str())
            );
        }
        assert_projection(&db, &a, &b, &tree_b, &input, &handles).await;
        assert_no_release(&mut frames);
        let probe = install_ack_probe(
            &orch,
            db.clone(),
            a.clone(),
            b.clone(),
            tree_b.clone(),
            input.clone(),
            handles.clone(),
            false,
            abort_consumer,
        );
        let executed = std::sync::Mutex::new(Some(check_result_from_delivery(delivery)));
        let outcomes = run_planned_checks_with_board(
            db.clone(),
            "p",
            CheckRunCommit {
                evaluated: &a,
                defined_by: &a,
            },
            &ca.context.tree_hash,
            "manual-a",
            &[(ca.plan.clone(), input.clone())],
            &[ca.sealed_coordinate],
            &[InfraScopes::head_only("manual-a", &a)],
            &streams[0],
            CheckExecMode::Shared,
            Some(&orch),
            None,
            &[],
            true,
            |_, _, _| {
                std::future::ready(
                    executed
                        .lock()
                        .unwrap()
                        .take()
                        .expect("one real A delivery"),
                )
            },
            |_| {},
        )
        .await;
        assert_eq!(outcomes.len(), 1);
        assert!(outcomes[0].passed);
        assert_eq!(
            outcomes[0].recorded.as_ref().unwrap().id,
            format!("remote-check:{REQUEST}:{ATTEMPT}:smoke")
        );
        assert_eq!(probe.load(Ordering::SeqCst), 1);
        assert_one_ack(&mut frames);
        return;
    }
    assert_projection(&db, &a, &b, &tree_b, &input, &handles).await;
    assert_no_release(&mut frames);
    // Stopping the runner before killing consumers preserves durable ownership;
    // ordinary consumer cancellation is deliberately not a runner crash.
    orch.fleet.begin_runner_shutdown();
    live_b.abort();
    assert!(live_b.await.unwrap_err().is_cancelled());
    drop(delivery);
    let cadence_delivery =
        tokio::time::timeout(std::time::Duration::from_secs(20), raw_cadence.unwrap())
            .await
            .unwrap()
            .unwrap();
    assert_eq!(cadence_delivery.len(), 1);
    assert!(cadence_delivery[0].is_ok());
    drop(cadence_delivery);
    assert_no_release(&mut frames);
    db.execute("DROP TRIGGER cross_commit_crash", ())
        .await
        .unwrap();
    let restarted_home = home.path().join("fresh-runner");
    std::fs::create_dir_all(&restarted_home).unwrap();
    let restarted = boot(&restarted_home, db.clone()).await;
    let mut replay_frames = restarted.fleet.attach_unreported_machine_for_test(REMOTE);
    restarted.fleet.handle_executor_message(
        REMOTE,
        1,
        ExecutorMessage::Result {
            request_id: REQUEST.into(),
            attempt_id: ATTEMPT.into(),
            outcome: retained,
        },
    );
    restarted
        .fleet
        .apply_executor_snapshot(REMOTE, 1, Default::default(), Default::default());
    assert_no_release(&mut replay_frames);
    let probe = install_ack_probe(
        &restarted,
        db.clone(),
        a.clone(),
        b.clone(),
        tree_b.clone(),
        input.clone(),
        handles.clone(),
        true,
        false,
    );
    let physical = claims::lookup(&db, REQUEST, ATTEMPT)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(physical.commit_sha, a);
    let waiters = claims::waiters(&db, REQUEST, ATTEMPT).await.unwrap();
    assert_eq!(waiters.len(), 3);
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        crate::execution::checks_main::adopt_check_batch(&restarted, &physical, waiters),
    )
    .await
    .expect("fresh replay must finish observation and cadence delivery before acknowledgement");
    assert_projection(&db, &a, &b, &tree_b, &input, &handles).await;
    assert_eq!(probe.load(Ordering::SeqCst), 1);
    assert_one_ack(&mut replay_frames);
}

#[allow(clippy::too_many_arguments)]
fn install_ack_probe(
    orch: &Orchestrator,
    db: Arc<LocalDb>,
    a: String,
    b: String,
    tree_b: String,
    input: String,
    handles: Vec<String>,
    replayed: bool,
    aborted: bool,
) -> Arc<AtomicUsize> {
    let count = Arc::new(AtomicUsize::new(0));
    let seen = count.clone();
    orch.fleet.set_result_ack_probe_for_test(Arc::new(move |request,attempt|{
        assert_eq!((request,attempt),(REQUEST,ATTEMPT));
        crate::storage::run_db_blocking(|| async {
            assert_projection(&db,&a,&b,&tree_b,&input,&handles).await;
            let rows=claims::waiters(&db,REQUEST,ATTEMPT).await.map_err(|e|e.to_string())?;
            assert_eq!(rows.len(),if replayed {3} else {2});
            for row in rows {
                assert_eq!(row.state,if (replayed && row.consumer != "main_cadence") || (aborted && row.consumer_ref == handles[1]) {"recorded_undelivered"} else {"delivered"},
                    "every consumer must have a durable disposition at the instant ResultAck is sent");
                assert_eq!(row.observation_id.as_deref(),
                    Some(format!("remote-check:{REQUEST}:{ATTEMPT}:smoke").as_str()));
            }
            Ok(())
        }).unwrap();
        seen.fetch_add(1,Ordering::SeqCst);
    }));
    count
}

fn assert_one_ack(frames: &mut tokio::sync::mpsc::UnboundedReceiver<ExecutorMessage>) {
    let mut acks = 0;
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
                acks += 1;
            }
            ExecutorMessage::Cancel { .. }
            | ExecutorMessage::Offer { .. }
            | ExecutorMessage::Granted { .. } => panic!(
                "coalescing/replay must never cancel or dispatch a second physical lane: {frame:?}"
            ),
            _ => {}
        }
    }
    assert_eq!(acks, 1);
}

async fn next_physical_frame(
    orch: &Orchestrator,
    frames: &mut tokio::sync::mpsc::UnboundedReceiver<ExecutorMessage>,
    submitting: &mut tokio::task::JoinHandle<Vec<crate::fleet::CoalescedDelivery>>,
    stage: &str,
) -> ExecutorMessage {
    super::adoption_tests::next_submission_frame(
        orch,
        frames,
        &(REQUEST.into(), ATTEMPT.into()),
        stage,
        submitting,
    )
    .await
}
