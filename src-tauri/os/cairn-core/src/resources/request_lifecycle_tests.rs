use super::*;
use crate::mcp::handlers::run::ledger::{self, tests as seed, Description, End};

const REQUEST: &str = "472b219b-3852-4a74-867a-df41db86dfa2";

async fn fixture(name: &str) -> LocalDb {
    let db = crate::storage::migrated_test_db(name).await;
    db.execute_script(
        "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p', 'default', 'P', 'prj', '/tmp/p', 1, 1);
         INSERT INTO jobs (id, project_id, status, node_name, uri_segment, created_at, updated_at) VALUES ('job', 'p', 'running', 'builder', 'builder', 1, 1);
         INSERT INTO sessions (id, job_id, status, backend_id, created_at, updated_at) VALUES ('sess', 'job', 'open', 'h', 1, 1);
         INSERT INTO runs (id, project_id, job_id, status, session_id, created_at, updated_at) VALUES ('run', 'p', 'job', 'live', 'sess', 1, 1);
         INSERT INTO turns (id, session_id, run_id, job_id, sequence, state, start_reason, created_at, updated_at) VALUES ('turn', 'sess', 'run', 'job', 1, 'yielded', 'initial', 1, 1);",
    )
    .await
    .unwrap();
    db
}

async fn journal(
    db: &LocalDb,
    sequence: i64,
    state: &str,
    executor: Option<&str>,
    decline: Option<(&str, &str)>,
    at_ms: i64,
) {
    db.execute(
        "INSERT INTO execution_queue (event_id, entry_id, sequence, runner_incarnation, request_id, attempt_id, project_id, state, executor_id, generation, decline_reason, detail, constraints_json, scoring_json, horizon_unix_ms, created_at) \
         VALUES (?1, 'entry-1', ?2, 'incarnation-1', ?3, 'attempt-1', 'p', ?4, ?5, ?6, ?7, ?8, '{}', '{}', 0, ?9)",
        params![
            format!("event-{sequence}"),
            sequence,
            REQUEST,
            state,
            executor,
            executor.map(|_| 3_i64),
            decline.map(|(reason, _)| reason),
            decline.map(|(_, detail)| detail),
            at_ms,
        ],
    )
    .await
    .unwrap();
}

fn context() -> Context {
    Context {
        now_ms: 10_000_000,
        incarnation: Some("incarnation-1".into()),
        machines: BTreeMap::from([("exec-1".to_string(), "bginf-sp1".to_string())]),
        ..Context::default()
    }
}

async fn lifecycle(db: &LocalDb, id: &str, context: &Context) -> RequestLifecycle {
    let resolution = resolve(db, db, Some(db), "p", id).await.unwrap();
    let ids = resolution.request_ids();
    assert_eq!(ids.len(), 1, "{id} names one request: {ids:?}");
    let request_id = &ids[0];
    let facts = gather(
        db,
        db,
        request_id,
        resolution.waits.get(request_id).map(String::as_str),
    )
    .await
    .unwrap();
    build(
        "prj",
        request_id,
        resolution.kinds_for(request_id),
        resolution.check_request.clone(),
        facts,
        context,
    )
}

#[tokio::test]
async fn a_parked_then_placed_batch_renders_every_stage_with_its_time() {
    let db = fixture("lifecycle-happy.db").await;
    seed::seed(&db, REQUEST, "toolu_happy", "job", "run", "p", 1_000_000).await;
    seed::seed_description(
        &db,
        REQUEST,
        &Description {
            label: Some("cargo test -p cairn-core".into()),
            item_count: 1,
            sequential: false,
            declared_timeout_ms: Some(600_000),
            verdict_branch: None,
            cwd: "/tmp/wt".into(),
            selector_json: Some(r#"{"name":"bginf-sp1"}"#.into()),
        },
    )
    .await;
    journal(&db, 1, "queued", None, None, 1_000_100).await;
    journal(
        &db,
        2,
        "queued",
        None,
        Some(("pressure", "bginf-sp1: measured CPU 94% is over its brake")),
        1_000_200,
    )
    .await;
    journal(&db, 3, "claimed", Some("exec-1"), None, 2_800_200).await;
    seed::seed_execution(
        &db,
        REQUEST,
        &ledger::Execution {
            attempt_id: "attempt-1".into(),
            executor_id: "exec-1".into(),
            machine: "bginf-sp1".into(),
            generation: Some(3),
            cell_id: Some("cell-7".into()),
            pid: Some(4242),
            started_ms: 2_800_300,
            cpu_milli: Some(1500),
            resident_bytes: Some(2 * 1024 * 1024),
            measured_ms: Some(2_830_000),
        },
    )
    .await;
    seed::seed_end(&db, REQUEST, End::Answered, None, 2_900_000).await;

    let lifecycle = lifecycle(&db, REQUEST, &context()).await;
    let text = render(&lifecycle);
    for expected in [
        "Matched as: run-batch request id.",
        "**delivered**: answered on its call at 1970-01-01 00:48:20Z (42 B of answer) (`run_batch_requests`)",
        "- Tool call: `toolu_happy`",
        "- Received: 1970-01-01 00:16:40Z",
        "- Label: cargo test -p cairn-core",
        "- Declared timeout: 10m",
        "held (pressure): bginf-sp1: measured CPU 94% is over its brake",
        "claimed by [bginf-sp1](cairn://executors/bginf-sp1) (link generation 3)",
        "- Waited 30m before a machine claimed it",
        "- Root pid: 4242",
        "- Cell: `cell-7`",
        "- Peak CPU: 1.5 cores; peak resident memory: 2.0 MiB",
    ] {
        assert!(text.contains(expected), "missing {expected:?} in:\n{text}");
    }
    let json = serde_json::to_value(&lifecycle).unwrap();
    assert_eq!(json["end"]["kind"], "delivered");
    assert_eq!(json["placement"].as_array().unwrap().len(), 3);
    assert_eq!(json["execution"]["pid"], 4242);
    assert_eq!(json["ids"]["toolUseId"], "toolu_happy");
    assert_eq!(json["unplacedMs"], 1_800_100);

    // Read both public surfaces from these same receipt/placement/execution rows.
    db.execute_script(
        "INSERT INTO threads (id, project_id, name, jurisdiction, created_at, updated_at)
         VALUES ('thread', 'p', 'builder', 'test', 1, 1);
         UPDATE jobs SET thread_id='thread', uri_segment='thread', node_name='thread' WHERE id='job';
         INSERT INTO run_batch_detached (request_id, job_id, run_id, label, commits, holds_branch, state, reason, created_ms, output, commit_id)
         VALUES ('472b219b-3852-4a74-867a-df41db86dfa2', 'job', 'run', 'cargo test', 1, 1, 'settled', 'call could not suspend', 1000000, 'batch output', 'abcdef1234567890');",
    ).await.unwrap();
    let db = std::sync::Arc::new(db);
    let search = std::sync::Arc::new(
        crate::storage::SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap(),
    );
    let orch = crate::orchestrator::OrchestratorBuilder::new(
        std::sync::Arc::new(crate::db::DbState::new(db.clone(), search)),
        std::sync::Arc::new(crate::services::testing::TestServicesBuilder::new().build()),
        tempfile::tempdir().unwrap().keep(),
    )
    .build();
    for state in ["parked", "running", "settled", "lost"] {
        db.execute(
            "UPDATE run_batch_requests SET \"end\"=NULL, ended_ms=NULL WHERE request_id=?1",
            (REQUEST,),
        )
        .await
        .unwrap();
        db.execute(
            "UPDATE run_batch_detached SET state=?1, waiting_on='waiting for room', lost_evidence='machine enrolment gone' WHERE request_id=?2",
            (state, REQUEST),
        ).await.unwrap();
        let request_page = read_request(&orch, "prj", REQUEST, false).await;
        let batch_page = super::super::node::read_node_batches(
            &orch,
            &db,
            "prj",
            0,
            0,
            "builder",
            None,
            Some(REQUEST),
        )
        .await;
        let expected = if matches!(state, "parked" | "running") {
            format!(
                "{request_page}{}",
                super::super::node::cancel_line("cairn://p/prj/builder", REQUEST)
            )
        } else {
            request_page.clone()
        };
        assert_eq!(batch_page, expected, "{state}");
        for stage in [
            "## Call",
            "## Batch",
            "## Placement",
            "## Execution",
            "## End",
            "## Delivery",
        ] {
            assert!(
                batch_page.contains(stage),
                "{state}: missing {stage}: {batch_page}"
            );
        }
        if matches!(state, "parked" | "running") {
            assert!(batch_page.contains("action:\"cancel\""), "{batch_page}");
        }
        if state == "settled" {
            assert!(
                batch_page.contains("## Output\n\nbatch output"),
                "{batch_page}"
            );
            assert!(batch_page.contains("commit abcdef123456"), "{batch_page}");
        }
    }
    db.execute_script(
        "INSERT INTO threads (id, project_id, name, jurisdiction, created_at, updated_at)
         VALUES ('other', 'p', 'other', 'test', 1, 1);
         INSERT INTO jobs (id, project_id, thread_id, status, node_name, uri_segment, created_at, updated_at)
         VALUES ('other-job', 'p', 'other', 'running', 'thread', 'thread', 1, 1);",
    ).await.unwrap();
    let redirect = super::super::node::read_node_batches(
        &orch,
        &db,
        "prj",
        0,
        0,
        "other",
        None,
        Some(REQUEST),
    )
    .await;
    assert!(
        redirect.contains("belongs to a different job"),
        "{redirect}"
    );
    assert!(
        redirect.contains(&format!("cairn://p/prj/builder/batches/{REQUEST}")),
        "{redirect}"
    );
    assert!(!redirect.contains("batch output"), "{redirect}");

    db.execute(
        "UPDATE run_batch_detached SET state='running', output=NULL WHERE request_id=?1",
        (REQUEST,),
    )
    .await
    .unwrap();
    {
        let live = crate::mcp::handlers::run::detached::LiveBatch::claim_for_test(REQUEST);
        live.begin_settling_for_test();
        crate::mcp::handlers::run::detached::note_publishing(
            REQUEST,
            &crate::fleet::ObservedPublication {
                attempt_id: "attempt-1".into(),
                delta_bytes: Some(1_000_000),
                since_ms: 2_900_000,
            },
        );
        let request_page = read_request(&orch, "prj", REQUEST, false).await;
        let batch_page = super::super::node::read_node_batches(
            &orch,
            &db,
            "prj",
            0,
            0,
            "builder",
            None,
            Some(REQUEST),
        )
        .await;
        assert!(batch_page.starts_with(&request_page), "{batch_page}");
        assert!(batch_page.contains("publishing"), "{batch_page}");
        assert!(
            batch_page.contains("branch releases when the result settles"),
            "{batch_page}"
        );
    }
    db.execute_script(format!(
        r#"INSERT INTO agent_waits (id, job_id, run_id, session_id, predecessor_turn_id, tool_use_id, condition_json, state, resolution_json, created_at)
           VALUES ('wait-delivery', 'job', 'run', 'sess', 'turn', 'toolu_happy', '{{"kind":"run_batch","request_id":"{REQUEST}","commits":true,"settlement":{{"late_cancel":"cancel arrived while publishing"}}}}', 'resolving', 'durable result', 1000);"#
    )).await.unwrap();
    let project_page = read_request(&orch, "prj", REQUEST, false).await;
    let batch_page = super::super::node::read_node_batches(
        &orch,
        &db,
        "prj",
        0,
        0,
        "builder",
        None,
        Some(REQUEST),
    )
    .await;
    assert_eq!(batch_page, project_page);
    assert!(
        batch_page.contains("result is being delivered"),
        "{batch_page}"
    );
    assert!(batch_page.contains("durable result"), "{batch_page}");
    assert!(!batch_page.contains("action:\"cancel\""), "{batch_page}");
    for has_detached in [true, false] {
        if !has_detached {
            db.execute(
                "DELETE FROM run_batch_detached WHERE request_id=?1",
                (REQUEST,),
            )
            .await
            .unwrap();
        }
        db.execute(
            "UPDATE agent_waits SET state='pending', resolution_json=NULL WHERE id='wait-delivery'",
            (),
        )
        .await
        .unwrap();
        let project_page = read_request(&orch, "prj", REQUEST, false).await;
        let batch_page = super::super::node::read_node_batches(
            &orch,
            &db,
            "prj",
            0,
            0,
            "builder",
            None,
            Some(REQUEST),
        )
        .await;
        assert_eq!(
            batch_page,
            format!(
                "{project_page}{}",
                super::super::node::cancel_line("cairn://p/prj/builder", REQUEST)
            )
        );
        assert!(
            batch_page.contains("Cancel: cancel arrived while publishing."),
            "{batch_page}"
        );
    }
}

#[tokio::test]
async fn a_call_whose_run_crashed_before_admission_reads_lost_with_the_runs_exit() {
    let db = fixture("lifecycle-crash.db").await;
    seed::seed(&db, REQUEST, "exec-fde657a4", "job", "run", "p", 1_000_000).await;
    seed::seed_end(
        &db,
        REQUEST,
        End::Dropped,
        Some("admission (waiting for room on bginf-sp1)"),
        1_044_000,
    )
    .await;
    db.execute(
        "UPDATE runs SET status='crashed', exit_reason='provider_silence_recovery', exited_at=1044 WHERE id='run'",
        (),
    )
    .await
    .unwrap();

    let lifecycle = lifecycle(&db, "exec-fde657a4", &context()).await;
    assert_eq!(lifecycle.end.kind, "lost");
    assert_eq!(
        lifecycle.end.sentence,
        "run `run` crashed: provider_silence_recovery at 1970-01-01 00:17:24Z; the batch was still in admission (waiting for room on bginf-sp1) and died with its call"
    );
    assert_eq!(lifecycle.matched_as, vec!["tool call id"]);
}

#[tokio::test]
async fn a_batch_whose_wait_was_abandoned_reads_still_running_and_quotes_the_wait() {
    let db = fixture("lifecycle-abandoned.db").await;
    db.execute_script(format!(
        r#"INSERT INTO agent_waits (id, job_id, run_id, session_id, predecessor_turn_id, tool_use_id, condition_json, state, resolution_json, created_at, resolved_at)
           VALUES ('wait-1', 'job', 'run', 'sess', 'turn', 'toolu_old', '{{"kind":"run_batch","request_id":"{REQUEST}","commits":false}}', 'cancelled', '{{"abandoned":"the turn was replaced"}}', 1000, 2000);
           INSERT INTO run_batch_detached (request_id, job_id, run_id, label, commits, holds_branch, state, reason, created_ms)
           VALUES ('{REQUEST}', 'job', 'run', 'cargo test', 0, 1, 'running', 'its wait was abandoned', 2000);"#
    ))
    .await
    .unwrap();

    let lifecycle = lifecycle(&db, "wait-1", &context()).await;
    assert_eq!(lifecycle.end.kind, "still_running");
    assert!(
        lifecycle
            .end
            .sentence
            .contains("a wait's end is not the batch's"),
        "{}",
        lifecycle.end.sentence
    );
    assert_eq!(
        lifecycle.end.quoted.as_deref(),
        Some(r#"{"abandoned":"the turn was replaced"}"#)
    );
    let text = render(&lifecycle);
    assert!(
        text.contains("> {\"abandoned\":\"the turn was replaced\"}"),
        "{text}"
    );
    assert!(
        text.contains("Its receipt predates the request ledger"),
        "{text}"
    );
}

#[tokio::test]
async fn an_unknown_id_says_so_and_lists_the_ids_it_accepts() {
    let db = fixture("lifecycle-unknown.db").await;
    let resolution = resolve(&db, &db, Some(&db), "p", "nothing-here")
        .await
        .unwrap();
    assert!(resolution.request_ids().is_empty());
    let text = unknown("nothing-here", false);
    assert!(text.starts_with("`nothing-here` names no request in this project."));
    for (kind, _) in ID_KINDS {
        assert!(text.contains(kind), "{kind} is listed:\n{text}");
    }
    let json: serde_json::Value = serde_json::from_str(&unknown("nothing-here", true)).unwrap();
    assert_eq!(
        json["acceptedIdKinds"].as_array().unwrap().len(),
        ID_KINDS.len()
    );
}

#[tokio::test]
async fn every_id_kind_reaches_the_same_request_and_a_run_lists_its_batches() {
    let db = fixture("lifecycle-ids.db").await;
    seed::seed(&db, REQUEST, "toolu_ids", "job", "run", "p", 1_000).await;
    db.execute_script(format!(
        r#"INSERT INTO agent_waits (id, job_id, run_id, session_id, predecessor_turn_id, tool_use_id, condition_json, state, created_at)
           VALUES ('wait-ids', 'job', 'run', 'sess', 'turn', 'toolu_ids', '{{"kind":"run_batch","request_id":"{REQUEST}"}}', 'resolved', 1000);"#
    ))
    .await
    .unwrap();
    journal(&db, 1, "queued", None, None, 1_100).await;
    for (id, kind) in [
        (REQUEST, "run-batch request id"),
        ("toolu_ids", "tool call id"),
        ("wait-ids", "wait id"),
        ("entry-1", "placement entry id"),
        ("attempt-1", "placement attempt id"),
        ("run", "run id"),
    ] {
        let resolution = resolve(&db, &db, Some(&db), "p", id).await.unwrap();
        assert_eq!(resolution.request_ids(), vec![REQUEST.to_string()], "{id}");
        assert!(
            resolution.kinds_for(REQUEST).contains(&kind),
            "{id} matched as {kind}"
        );
    }

    seed::seed(&db, "second-request", "toolu_two", "job", "run", "p", 2_000).await;
    let resolution = resolve(&db, &db, Some(&db), "p", "run").await.unwrap();
    assert_eq!(
        resolution.request_ids(),
        vec![REQUEST.to_string(), "second-request".to_string()]
    );
    let rows = summaries(&db, &resolution.request_ids()).await.unwrap();
    let text = disambiguation("prj", "run", &resolution, &rows, None, false);
    assert!(text.starts_with("`run` names 2 requests."), "{text}");
    assert!(
        text.contains(&lifecycle_uri("prj", "second-request")),
        "{text}"
    );
}

#[tokio::test]
async fn an_end_is_written_once() {
    let db = fixture("lifecycle-once.db").await;
    seed::seed(&db, REQUEST, "toolu_once", "job", "run", "p", 1_000).await;
    seed::seed_end(
        &db,
        REQUEST,
        End::Refused,
        Some("no machine matches `os: plan9`"),
        2_000,
    )
    .await;
    seed::seed_end(&db, REQUEST, End::Dropped, Some("received"), 3_000).await;
    let lifecycle = lifecycle(&db, REQUEST, &context()).await;
    assert_eq!(lifecycle.end.kind, "refused");
    assert_eq!(
        lifecycle.end.sentence,
        "refused at 1970-01-01 00:00:02Z: no machine matches `os: plan9`"
    );
}

#[tokio::test]
async fn an_open_call_from_an_earlier_runner_reads_lost_to_that_runners_end() {
    let db = fixture("lifecycle-restart.db").await;
    seed::seed(&db, REQUEST, "toolu_restart", "job", "run", "p", 1_000).await;
    db.execute(
        "INSERT INTO runner_events (id, runner_incarnation, kind, occurred_at_ms) VALUES ('ev-1', 'incarnation-2', 'runner_started', 5000)",
        (),
    )
    .await
    .unwrap();
    let restarted = Context {
        incarnation: Some("incarnation-2".into()),
        ..context()
    };
    let lifecycle = lifecycle(&db, REQUEST, &restarted).await;
    assert_eq!(lifecycle.end.kind, "lost");
    assert_eq!(
        lifecycle.end.sentence,
        "no end recorded: the runner that received it ended with its call still open; the runner restarted at 1970-01-01 00:00:05Z"
    );
    // The same row read by the runner that received it is simply open.
    let open = lifecycle_open(&db).await;
    assert_eq!(open, "open");
}

async fn lifecycle_open(db: &LocalDb) -> &'static str {
    lifecycle(db, REQUEST, &context()).await.end.kind
}

/// Every statement the lifecycle read runs, with how many text binds it takes.
const LOOKUPS: &[(&str, usize)] = &[
    ("SELECT request_id FROM run_batch_requests WHERE request_id=?1", 1),
    ("SELECT request_id FROM run_batch_detached WHERE request_id=?1", 1),
    ("SELECT request_id FROM run_batch_requests WHERE tool_use_id=?1", 1),
    ("SELECT request_id FROM run_batch_requests WHERE run_id=?1 ORDER BY received_ms", 1),
    ("SELECT id, condition_json FROM agent_waits WHERE tool_use_id=?1", 1),
    ("SELECT id, condition_json FROM agent_waits WHERE id=?1", 1),
    ("SELECT id, condition_json FROM agent_waits WHERE run_id=?1", 1),
    ("SELECT id, condition_json FROM agent_waits WHERE run_id=?1 AND tool_use_id=?2", 2),
    ("SELECT request_id FROM execution_queue WHERE entry_id=?1", 1),
    ("SELECT request_id FROM execution_queue WHERE attempt_id=?1", 1),
    ("SELECT request_id FROM execution_queue INDEXED BY idx_execution_queue_request WHERE request_id=?1 ORDER BY created_at, sequence", 1),
    ("SELECT id, suite, state, request_id FROM check_requests WHERE id=?1", 1),
    ("SELECT state FROM run_batch_adopted_outcomes WHERE request_id=?1", 1),
    ("SELECT status FROM runs WHERE id=?1", 1),
    ("SELECT request_id FROM run_batch_adopted_outcomes WHERE attempt_id=?1", 1),
    ("SELECT id, condition_json FROM agent_waits WHERE json_extract(condition_json, '$.request_id')=?1", 1),
    ("SELECT request_id FROM check_lane_adoptions WHERE request_id=?1", 1),
    ("SELECT request_id FROM check_lane_adoptions WHERE attempt_id=?1", 1),
    ("SELECT request_id FROM check_lane_adoptions WHERE observation_id=?1", 1),
    ("SELECT check_name FROM check_lane_adoption_waiters WHERE request_id=?1", 1),
    ("SELECT id FROM check_result_observations WHERE project_id=?1 AND public_handle=?2", 2),
    ("SELECT public_handle FROM check_result_observations WHERE id=?1", 1),
    ("SELECT occurred_at_ms FROM runner_events WHERE kind='runner_started' AND occurred_at_ms > ?1 ORDER BY occurred_at_ms LIMIT 1", 1),
];

#[tokio::test]
async fn every_lookup_searches_an_index() {
    let db = fixture("lifecycle-plans.db").await;
    for (sql, binds) in LOOKUPS {
        let plan = cairn_db::storage::query_plan(
            &db,
            sql,
            vec![cairn_db::turso::Value::Text("x".into()); *binds],
        )
        .await;
        println!("{sql}\n  {}", plan.replace('\n', "\n  "));
        assert!(
            plan.lines()
                .all(|step| !step.trim_start().starts_with("SCAN")),
            "{sql} scans:\n{plan}"
        );
    }
}

#[tokio::test]
async fn a_batch_older_than_the_ledger_resolves_through_the_rows_it_left() {
    let db = fixture("lifecycle-legacy.db").await;
    // Only a wait carries this request: its own id, its tool call and its
    // wait id all reach it, and so does the canonical link they lead to.
    db.execute_script(format!(
        r#"INSERT INTO agent_waits (id, job_id, run_id, session_id, predecessor_turn_id, tool_use_id, condition_json, state, created_at, resolved_at, successor_turn_id)
           VALUES ('wait-old', 'job', 'run', 'sess', 'turn', 'toolu_old', '{{"kind":"run_batch","request_id":"{REQUEST}"}}', 'resolved', 1000, 9000, NULL);"#
    ))
    .await
    .unwrap();
    for id in [REQUEST, "toolu_old", "wait-old"] {
        let lifecycle = lifecycle(&db, id, &context()).await;
        assert_eq!(lifecycle.ids.wait_id.as_deref(), Some("wait-old"), "{id}");
        assert_eq!(lifecycle.end.kind, "delivered", "{id}");
        assert_eq!(lifecycle.end.source, "agent_waits", "{id}");
    }

    // Only an adopted result carries this one: its request and attempt reach it.
    db.execute(
        "INSERT INTO run_batch_adopted_outcomes (request_id, attempt_id, job_id, executor_id, machine, outcome_json, state, received_ms, settled_ms) \
         VALUES ('adopted-request', 'adopted-attempt', 'job', 'exec-1', 'bginf-sp1', '{}', 'delivered', 5000, 6000)",
        (),
    )
    .await
    .unwrap();
    for id in ["adopted-request", "adopted-attempt"] {
        let lifecycle = lifecycle(&db, id, &context()).await;
        assert_eq!(lifecycle.request_id, "adopted-request", "{id}");
        assert_eq!(lifecycle.end.kind, "delivered", "{id}");
        assert_eq!(lifecycle.end.source, "run_batch_adopted_outcomes", "{id}");
    }
}

#[tokio::test]
async fn a_check_lane_reads_its_adoption_by_request_attempt_and_observation() {
    let db = fixture("lifecycle-lane.db").await;
    db.execute_script(
        "INSERT INTO check_lane_adoptions (request_id, attempt_id, project_id, check_names, platform, commit_sha, input_hashes, consumer, consumer_ref, executor_id, machine, granted_at_ms, state, stopped_at_ms, observation_id, updated_ms)
           VALUES ('lane-request', 'lane-attempt', 'p', 'rust-tests', 'linux', 'abc', '{}', 'turn_end', 'job', 'exec-1', 'bginf-sp1', 1000, 'recorded', 61000, 'obs-uuid', 61000);
         INSERT INTO check_lane_adoption_waiters (request_id, attempt_id, check_name, item_index, consumer, consumer_ref, state, updated_ms)
           VALUES ('lane-request', 'lane-attempt', 'rust-tests', 0, 'turn_end', 'job', 'delivered', 61000);",
    )
    .await
    .unwrap();
    for (id, kind) in [
        ("lane-request", "check lane request id"),
        ("lane-attempt", "check lane attempt id"),
        ("obs-uuid", "check observation id"),
    ] {
        let lifecycle = lifecycle(&db, id, &context()).await;
        assert_eq!(lifecycle.request_id, "lane-request", "{id}");
        assert!(
            lifecycle.matched_as.contains(&kind),
            "{id}: {:?}",
            lifecycle.matched_as
        );
        assert_eq!(lifecycle.end.kind, "delivered", "{id}");
        assert_eq!(
            lifecycle.end.sentence,
            "check lane on bginf-sp1 recorded observation `obs-uuid` at 1970-01-01 00:01:01Z"
        );
        let text = render(&lifecycle);
        assert!(
            text.contains("## Check lane (`check_lane_adoptions`)"),
            "{text}"
        );
        assert!(text.contains("Observation: UUID `obs-uuid`"), "{text}");
        assert!(
            text.contains("Waiter: rust-tests for turn_end `job`: delivered"),
            "{text}"
        );
    }
}
