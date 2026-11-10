use super::testing::publish_unit_for_test;
use super::thread_range::*;
use super::write::{pack_unit, PackUnit};
use crate::storage::{LocalDb, RowExt};

async fn fixture(name: &str) -> (LocalDb, ThreadOwner) {
    let db = crate::storage::migrated_test_db(name).await;
    db.execute_script(
        "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
         INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
           VALUES('p','w','Project','proj','/none',1,1);
         INSERT INTO threads(id,project_id,name,created_at,updated_at) VALUES('th','p','test-thread',1,1);
         INSERT INTO jobs(id,project_id,thread_id,uri_segment,status,current_session_id,created_at,updated_at)
           VALUES('owner','p','th','thread','running','s4',1,1);",
    ).await.unwrap();
    for i in 1..=4 {
        db.execute_script(format!(
            "INSERT INTO sessions(id,job_id,sequence,status,created_at,closed_at,updated_at)
               VALUES('s{i}','owner',{i},'{}',{i},{},1);
             INSERT INTO runs(id,job_id,project_id,session_id,status,created_at,updated_at)
               VALUES('r{i}','owner','p','s{i}','{}',{i},1);
             INSERT INTO turns(id,job_id,session_id,run_id,sequence,state,created_at,updated_at)
               VALUES('t{i}','owner','s{i}','r{i}',1,'complete',{i},1);
             INSERT INTO events(id,run_id,session_id,turn_id,sequence,event_type,data,created_at,timestamp)
               VALUES('e{i}','r{i}','s{i}','t{i}',1,'text','{{}}',{i},1);",
            if i == 4 { "open" } else { "replaced" },
            if i == 4 {
                "NULL".to_string()
            } else {
                (i * 100).to_string()
            },
            if i == 4 { "live" } else { "exited" },
        ))
        .await
        .unwrap();
    }
    db.execute_script(
        "INSERT INTO thread_compactions(job_id,generation,source_session_id,compacted_through_block,
             recency_start_block,source_bytes,candidate_bytes,trigger,created_at)
         VALUES('owner',1,'s2',5,6,100,10,'manual',900),
               ('owner',2,'s3',NULL,7,100,10,'manual',1),
               ('owner',3,'s4',99,100,100,10,'manual',999);",
    ).await.unwrap();
    let owner = ThreadOwner {
        job_id: "owner".into(),
        thread_name: "test-thread".into(),
    };
    (db, owner)
}

async fn build(db: &LocalDb, owner: &ThreadOwner) -> RangeBuild {
    build_ranges(db, owner, 1000, 10, 1000).await.unwrap()
}

async fn task(db: &LocalDb, status: &str) {
    db.execute_script(format!(
        "INSERT INTO jobs(id,project_id,parent_job_id,status,current_session_id,created_at,updated_at)
           VALUES('task','p','owner','{status}','task-s',150,1);
         INSERT INTO sessions(id,job_id,sequence,status,closed_at,created_at,updated_at)
           VALUES('task-s','task',1,'replaced',180,150,180);
         INSERT INTO runs(id,job_id,project_id,session_id,status,created_at,updated_at)
           VALUES('task-r','task','p','task-s','exited',150,180);
         INSERT INTO events(id,run_id,session_id,sequence,event_type,data,created_at,timestamp)
           VALUES('task-e','task-r','task-s',1,'text','{{}}',150,1);"
    )).await.unwrap();
}

#[tokio::test]
async fn boundary_is_generation_source_not_timestamp_or_block_and_pending_is_ignored() {
    let (db, owner) = fixture("thread-range-boundary.db").await;
    let page = owners(&db, "", 1).await.unwrap();
    assert_eq!(page[0].job_id, "owner");
    assert!(owners(&db, "owner", 1).await.unwrap().is_empty());
    let ranges = build(&db, &owner).await;
    let candidate = &ranges.candidates[0];
    assert_eq!(
        (
            candidate.range.first_sequence,
            candidate.range.last_sequence
        ),
        (1, 3)
    );
    assert_eq!(candidate.range.through_generation, 2);
    assert_eq!(candidate.resolved_at, 300);
    assert_eq!(candidate.event_rows, 3);
    assert_eq!(
        validate_candidate(&db, candidate, None).await.unwrap(),
        None
    );
    db.execute(
        "UPDATE thread_compactions SET compacted_through_block=42 WHERE generation=2",
        (),
    )
    .await
    .unwrap();
    assert_eq!(
        build(&db, &owner).await.candidates[0].range,
        candidate.range
    );
    db.execute("DELETE FROM thread_compactions WHERE generation<3", ())
        .await
        .unwrap();
    assert!(build(&db, &owner).await.candidates.is_empty());
}

#[tokio::test]
async fn closed_time_controls_window_and_live_straddle_holds_back_entire_suffix() {
    let (db, owner) = fixture("thread-range-straddle-live.db").await;
    let ranges = build_ranges(&db, &owner, 200, 10, 1000).await.unwrap();
    assert_eq!(ranges.candidates[0].range.last_sequence, 1);
    db.execute_script(
        "UPDATE turns SET run_id='r2' WHERE id='t4';
         UPDATE turns SET run_id='r1' WHERE id='t2';",
    )
    .await
    .unwrap();
    // r2 reaches s4, pulling s2 back; r1 then pulls s1 back as well.
    assert!(build(&db, &owner).await.candidates.is_empty());
}

#[tokio::test]
async fn connected_sessions_do_not_split_and_oversize_is_named() {
    let (db, owner) = fixture("thread-range-group.db").await;
    db.execute("UPDATE turns SET run_id='r1' WHERE id='t2'", ())
        .await
        .unwrap();
    let ranges = build_ranges(&db, &owner, 1000, 10, 30).await.unwrap();
    assert_eq!(ranges.candidates.len(), 2);
    assert_eq!(
        (
            ranges.candidates[0].range.first_sequence,
            ranges.candidates[0].range.last_sequence
        ),
        (1, 2)
    );
    assert_eq!(ranges.candidates[0].range.runs, vec!["r1", "r2"]);
    let ranges = build_ranges(&db, &owner, 1000, 40, 30).await.unwrap();
    assert!(ranges.candidates.is_empty());
    assert_eq!(ranges.skips.len(), 2);
    assert_eq!(ranges.skips[0].scope_id, "owner/1-2");
}

#[tokio::test]
async fn running_task_is_deferred_then_terminal_descendants_are_included_once() {
    let (db, owner) = fixture("thread-range-task.db").await;
    task(&db, "running").await;
    assert_eq!(
        build(&db, &owner).await.candidates[0].range.jobs,
        vec!["owner"]
    );
    db.execute("UPDATE jobs SET status='complete' WHERE id='task'", ())
        .await
        .unwrap();
    let candidate = build(&db, &owner).await.candidates.remove(0);
    assert_eq!(candidate.range.jobs, vec!["owner", "task"]);
    assert!(candidate.range.sessions.contains(&"task-s".into()));
    assert_eq!(
        validate_candidate(&db, &candidate, None).await.unwrap(),
        None
    );
    db.execute("UPDATE jobs SET status='running' WHERE id='task'", ())
        .await
        .unwrap();
    assert!(validate_candidate(&db, &candidate, None)
        .await
        .unwrap()
        .is_some());
    // A terminal grandchild is found through its still-running task parent.
    db.execute_script(
        "INSERT INTO jobs(id,project_id,parent_job_id,status,created_at,updated_at)
           VALUES('grandchild','p','task','cancelled',180,1);
         INSERT INTO runs(id,job_id,project_id,status,created_at,updated_at)
           VALUES('grand-r','grandchild','p','exited',180,1);",
    )
    .await
    .unwrap();
    let ranges = build(&db, &owner).await;
    assert!(ranges.candidates[0]
        .range
        .jobs
        .contains(&"grandchild".into()));
}

#[tokio::test]
async fn manifest_and_event_closure_are_checked_and_hot_rows_are_untouched() {
    let (db, owner) = fixture("thread-range-event.db").await;
    let candidate = build(&db, &owner).await.candidates.remove(0);
    let built = pack_unit(&db, &PackUnit::ThreadRange(candidate.range.clone()))
        .await
        .unwrap();
    assert_eq!(
        validate_candidate(&db, &candidate, Some(&built.manifest))
            .await
            .unwrap(),
        None
    );
    let mut bad = built.manifest.clone();
    bad.runs[0].session_ids.push("s4".into());
    assert!(validate_candidate(&db, &candidate, Some(&bad))
        .await
        .unwrap()
        .unwrap()
        .contains("manifest"));
    db.execute_script(
        "INSERT INTO events(id,run_id,session_id,sequence,event_type,data,created_at,timestamp)
           VALUES('stray','r4','s1',2,'text','{}',1,1);",
    )
    .await
    .unwrap();
    assert!(validate_candidate(&db, &candidate, Some(&built.manifest))
        .await
        .unwrap()
        .unwrap()
        .contains("gained run"));
    assert_eq!(
        db.query_one("SELECT COUNT(*) FROM events", (), |r| r.i64(0))
            .await
            .unwrap(),
        5
    );
}

#[tokio::test]
async fn reopened_task_invalidates_only_unevicted_scope_not_owner_live_tail() {
    let (db, owner) = fixture("thread-range-stale.db").await;
    task(&db, "complete").await;
    let candidate = build(&db, &owner).await.candidates.remove(0);
    let scope = candidate.range.scope_id();
    publish_unit_for_test(&db, &PackUnit::ThreadRange(candidate.range)).await;
    assert_eq!(
        unevicted_ranges(&db, "", 10).await.unwrap(),
        vec![scope.clone()]
    );
    assert!(!is_stale(&db, &scope).await.unwrap());
    assert!(build(&db, &owner).await.candidates.is_empty());
    db.execute_script(
        "INSERT INTO runs(id,job_id,project_id,session_id,status,created_at,updated_at)
           VALUES('reopened','task','p','task-s','live',999,999);",
    )
    .await
    .unwrap();
    assert!(is_stale(&db, &scope).await.unwrap());
    db.execute(
        "UPDATE history_tier SET evicted_at=123 WHERE scope_id=?1",
        (scope.clone(),),
    )
    .await
    .unwrap();
    assert!(!invalidate_unevicted(&db, &scope).await.unwrap());
    assert_eq!(
        db.query_one("SELECT COUNT(*) FROM history_tier_sessions", (), |r| r
            .i64(0))
            .await
            .unwrap(),
        4
    );
    db.execute(
        "UPDATE history_tier SET evicted_at=NULL WHERE scope_id=?1",
        (scope.clone(),),
    )
    .await
    .unwrap();
    assert!(invalidate_unevicted(&db, &scope).await.unwrap());
    assert!(unevicted_ranges(&db, "", 10).await.unwrap().is_empty());
    assert_eq!(
        db.query_one("SELECT COUNT(*) FROM history_tier_runs", (), |r| r.i64(0))
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        db.query_one("SELECT COUNT(*) FROM events", (), |r| r.i64(0))
            .await
            .unwrap(),
        5
    );
}

#[tokio::test]
async fn session_probes_seek_at_twenty_sessions_and_hundreds_of_runs() {
    let (db, _) = fixture("thread-range-query-plans.db").await;
    // Exercise the actual one-session SQL repeatedly at production cardinality.
    // There is intentionally no SQL list whose shape changes with the range size.
    for session in 0..20 {
        let id = format!("plan-s{session}");
        db.execute(
            "INSERT INTO sessions(id,job_id,status,sequence,created_at,updated_at) VALUES(?1,'owner','replaced',?2,1,1)",
            (id.clone(),100_i64+session),
        ).await.unwrap();
        for run in 0..20 {
            let run_id = format!("plan-r{session}-{run}");
            db.execute(
                "INSERT INTO runs(id,project_id,job_id,session_id,status,created_at,updated_at)
                 VALUES(?1,'p','owner',?2,'exited',1,1)",
                (run_id.clone(), id.clone()),
            )
            .await
            .unwrap();
            db.execute(
                "INSERT INTO events(id,run_id,session_id,sequence,event_type,data,created_at,timestamp)
                 VALUES(?1,?2,?3,1,'text','{}',1,1)",
                (format!("plan-e{session}-{run}"), run_id, id.clone()),
            )
            .await
            .unwrap();
        }
        for (sql, binds, index) in [
            (
                EVENT_CLOSURE_SQL,
                vec![
                    turso::Value::Text(id.clone()),
                    turso::Value::Integer(0),
                    turso::Value::Integer(512),
                ],
                "idx_events_session_id",
            ),
            (
                STALE_SESSION_RUN_SQL,
                vec![turso::Value::Text(id.clone())],
                "idx_runs_session_id_created_at",
            ),
            (
                STALE_SESSION_TURN_SQL,
                vec![turso::Value::Text(id.clone())],
                "idx_turns_session",
            ),
            (
                STALE_TASK_SQL,
                vec![turso::Value::Text("owner".into())],
                "idx_runs_job_id",
            ),
            (
                STALE_SESSION_EVENT_SQL,
                vec![turso::Value::Text(id)],
                "idx_events_session_id",
            ),
        ] {
            let plan = crate::storage::query_plan(&db, sql, binds).await;
            assert!(plan.contains(index), "{sql}: {plan}");
            assert!(
                !plan.contains("SCAN ")
                    && !plan.contains("TEMP B-TREE")
                    && !plan.contains("SORTER"),
                "{sql}: {plan}"
            );
        }
    }
}

#[tokio::test]
async fn a_deferred_task_joins_the_next_range_after_rotation() {
    let (db, owner) = fixture("thread-range-deferred.db").await;
    task(&db, "running").await;
    let first = build(&db, &owner).await.candidates.remove(0);
    publish_unit_for_test(&db, &PackUnit::ThreadRange(first.range)).await;
    db.execute_script(
        "UPDATE jobs SET status='complete' WHERE id='task';
         UPDATE runs SET status='exited' WHERE id='r4';
         UPDATE sessions SET status='replaced',closed_at=400 WHERE id='s4';
         INSERT INTO sessions(id,job_id,sequence,status,created_at,updated_at)
           VALUES('s5','owner',5,'open',500,500);
         UPDATE jobs SET current_session_id='s5' WHERE id='owner';",
    )
    .await
    .unwrap();
    let next = build(&db, &owner).await.candidates.remove(0);
    assert_eq!(
        (next.range.first_sequence, next.range.last_sequence),
        (4, 4)
    );
    assert!(next.range.jobs.contains(&"task".into()));
    assert_eq!(validate_candidate(&db, &next, None).await.unwrap(), None);
}

#[tokio::test]
async fn stale_event_only_session_association_is_detected() {
    let (db, owner) = fixture("thread-range-stale-event.db").await;
    let first = build(&db, &owner).await.candidates.remove(0);
    let scope = first.range.scope_id();
    publish_unit_for_test(&db, &PackUnit::ThreadRange(first.range)).await;
    assert!(!is_stale(&db, &scope).await.unwrap());
    db.execute_script(
        "INSERT INTO events(id,run_id,session_id,sequence,event_type,data,created_at,timestamp)
           VALUES('foreign-event','r4','s1',20,'text','{}',700,1);",
    )
    .await
    .unwrap();
    assert!(is_stale(&db, &scope).await.unwrap());
}

#[tokio::test]
async fn event_closure_reaches_beyond_first_keyset_page() {
    let (db, owner) = fixture("thread-range-event-page.db").await;
    let values = (2..=600)
        .map(|i| format!("('many-{i}','r1','s1',{i},'text','{{}}',1,1)"))
        .collect::<Vec<_>>()
        .join(",");
    db.execute_script(format!(
        "INSERT INTO events(id,run_id,session_id,sequence,event_type,data,created_at,timestamp) VALUES {values};"
    )).await.unwrap();
    let candidate = build_ranges(&db, &owner, 1000, 1, 10000)
        .await
        .unwrap()
        .candidates
        .remove(0);
    let built = pack_unit(&db, &PackUnit::ThreadRange(candidate.range.clone()))
        .await
        .unwrap();
    assert_eq!(
        validate_candidate(&db, &candidate, Some(&built.manifest))
            .await
            .unwrap(),
        None
    );
    db.execute_script(
        "INSERT INTO events(id,run_id,session_id,sequence,event_type,data,created_at,timestamp)
           VALUES('last-stray','r4','s1',601,'text','{}',1,1);",
    )
    .await
    .unwrap();
    assert!(validate_candidate(&db, &candidate, Some(&built.manifest))
        .await
        .unwrap()
        .unwrap()
        .contains("gained run"));
}

#[tokio::test]
async fn parent_walk_refuses_an_unbounded_tree() {
    let (db, owner) = fixture("thread-range-parent-bound.db").await;
    let mut parent = "owner".to_string();
    for i in 0..65 {
        let id = format!("deep-{i}");
        db.execute(
            "INSERT INTO jobs(id,project_id,parent_job_id,status,created_at,updated_at)
             VALUES(?1,'p',?2,'running',1,1)",
            (id.clone(), parent),
        )
        .await
        .unwrap();
        parent = id;
    }
    let result = build_ranges(&db, &owner, 1000, 10, 1000).await.unwrap();
    assert!(result.candidates.is_empty());
    assert_eq!(result.skips.len(), 1);
    assert!(result.skips[0].reason.contains("parent walk exceeded"));
}

#[tokio::test]
async fn event_only_straddles_participate_in_discovery_and_closure() {
    let (db, owner) = fixture("thread-event-only-straddle.db").await;
    db.execute_script(
        "INSERT INTO events(id,run_id,session_id,sequence,event_type,data,created_at,timestamp)
         VALUES('event-only','r4','s2',2,'text','{}',1,1);",
    )
    .await
    .unwrap();
    let held = build(&db, &owner).await;
    assert!(held.skips.is_empty());
    assert_eq!(held.candidates[0].range.sessions, ["s1"]);
    // The inverse edge matters too: a packed run must not name the live tail
    // solely through an event's session coordinate.
    db.execute_script(
        "DELETE FROM events WHERE id='event-only';
         INSERT INTO events(id,run_id,session_id,sequence,event_type,data,created_at,timestamp)
         VALUES('event-only-out','r2','s4',2,'text','{}',1,1);",
    )
    .await
    .unwrap();
    let held = build(&db, &owner).await;
    assert!(held.skips.is_empty());
    assert_eq!(held.candidates[0].range.sessions, ["s1"]);
}
