use super::*;
use crate::models::{
    DelegatedOutputContract, DelegatedOwnershipScope, DelegatedSessionStrategy, DelegationOrigin,
    OutputSchema,
};
use crate::storage::RowExt;

async fn db_with_execution(snapshot_packets: serde_json::Value) -> (tempfile::TempDir, LocalDb) {
    let dir = tempfile::tempdir().unwrap();
    let db = LocalDb::open_migrated_for_test(dir.path().join("packets.db"))
        .await
        .unwrap();
    db.execute_script(
        "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
         INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
           VALUES('p','w','P','proj','/tmp/p',1,1);
         INSERT INTO issues(id,project_id,number,title,status,progress,attention,created_at,updated_at)
           VALUES('i','p',1,'I','active','active','none',1,1);",
    )
    .await
    .unwrap();
    let snapshot = serde_json::json!({
        "recipe": {"id": "r", "name": "R", "description": null, "trigger": "manual",
                   "nodes": [], "edges": []},
        "agents": {}, "skills": {},
        "triggerContext": {"issueId": "i", "projectId": "p", "triggerType": "manual"},
        "delegatedPackets": snapshot_packets,
        "createdAt": 1
    });
    db.execute(
        "INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq,snapshot)
         VALUES('e','r','i','p','running',1,1,?1)",
        (snapshot.to_string(),),
    )
    .await
    .unwrap();
    (dir, db)
}

fn packet(id: &str, status: DelegatedStatus, job: Option<&str>) -> DelegatedWorkPacket {
    DelegatedWorkPacket {
        id: id.to_string(),
        parent_job_id: "j-parent".to_string(),
        parent_turn_id: Some("t-parent".to_string()),
        parent_tool_use_id: Some("tool-1".to_string()),
        origin: DelegationOrigin::TaskTool,
        title: format!("task {id}"),
        problem_statement: Some(format!("do {id}")),
        prompt_digest: None,
        agent_config_id: "Explore".to_string(),
        ownership: DelegatedOwnershipScope {
            cwd: "/tmp".to_string(),
            fence: None,
            sandbox: None,
            on_escape: None,
        },
        session: DelegatedSessionStrategy::default(),
        acceptance: vec!["Return it".to_string()],
        output_contract: DelegatedOutputContract {
            schema_type: OutputSchema::Preset("return".to_string()),
            tool_name: None,
            description: None,
        },
        status,
        materialized_node_ids: vec![],
        result_artifact_job_id: job.map(str::to_string),
        task_index: Some(0),
        tier_override: None,
        backend_preference: None,
        background: false,
        created_at: 7,
    }
}

fn inline(packets: &[DelegatedWorkPacket]) -> serde_json::Value {
    serde_json::to_value(packets).unwrap()
}

async fn read<T: Send + 'static>(
    db: &LocalDb,
    f: impl for<'a> Fn(&'a Connection) -> futures_boxed::Boxed<'a, T> + Send + Sync + 'static,
) -> T {
    let f = std::sync::Arc::new(f);
    db.read(move |conn| {
        let f = f.clone();
        Box::pin(async move { Ok(f(conn).await) })
    })
    .await
    .unwrap()
}

mod futures_boxed {
    pub type Boxed<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;
}

/// Another execution beside `e`. An issue holds one execution, so this one has
/// none, like a thread's host execution.
async fn add_execution(db: &LocalDb, id: &str, snapshot_packets: serde_json::Value) {
    let snapshot = serde_json::json!({
        "recipe": {"id": "r", "name": "R", "description": null, "trigger": "manual",
                   "nodes": [], "edges": []},
        "agents": {}, "skills": {},
        "triggerContext": {"projectId": "p", "triggerType": "manual"},
        "delegatedPackets": snapshot_packets,
        "createdAt": 1
    });
    db.execute(
        "INSERT INTO executions(id,recipe_id,project_id,status,started_at,snapshot)
         VALUES(?1,'r','p','running',1,?2)",
        (id.to_string(), snapshot.to_string()),
    )
    .await
    .unwrap();
}

/// Snapshots that still hold packets. An empty array is left in place: removing
/// it would rewrite the snapshot for nothing.
async fn inline_snapshots(db: &LocalDb) -> i64 {
    db.query_one(
        "SELECT COUNT(*) FROM executions \
         WHERE COALESCE(json_array_length(snapshot, '$.delegatedPackets'), 0) > 0",
        (),
        |row| row.i64(0),
    )
    .await
    .unwrap()
}

async fn row_count(db: &LocalDb) -> i64 {
    db.query_one("SELECT COUNT(*) FROM delegated_packets", (), |row| {
        row.i64(0)
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn a_packet_round_trips_and_keeps_its_prompt_only_while_pending() {
    let (_dir, db) = db_with_execution(serde_json::json!([])).await;
    let pending = packet("a", DelegatedStatus::Pending, None);
    insert_db(&db, "e", &pending).await.unwrap();
    let digested = DelegatedWorkPacket {
        prompt_digest: Some(prompt_digest("do a")),
        ..pending.clone()
    };
    assert_eq!(
        get_db(&db, "e", "a").await.unwrap(),
        Some(digested),
        "a stored packet gains its prompt's digest"
    );

    let change = PacketChange {
        status: Some(DelegatedStatus::Materialized),
        materialized_node_ids: Some(vec!["delegated-a-agent".to_string()]),
        result_artifact_job_id: Some("j-a".to_string()),
    };
    assert!(update_db(&db, "e", "a", &change).await.unwrap());
    let stored = get_db(&db, "e", "a").await.unwrap().unwrap();
    assert_eq!(stored.status, DelegatedStatus::Materialized);
    assert_eq!(stored.materialized_node_ids, vec!["delegated-a-agent"]);
    assert_eq!(stored.result_artifact_job_id.as_deref(), Some("j-a"));
    assert_eq!(
        stored.problem_statement, None,
        "the prompt leaves with pending"
    );
    assert_eq!(
        stored.prompt_digest.as_deref(),
        Some(prompt_digest("do a").as_str()),
        "the digest stays, so a retried spawn still matches"
    );

    // A child is named once.
    let rename = PacketChange {
        result_artifact_job_id: Some("j-other".to_string()),
        ..PacketChange::default()
    };
    update_db(&db, "e", "a", &rename).await.unwrap();
    assert_eq!(
        get_db(&db, "e", "a")
            .await
            .unwrap()
            .unwrap()
            .result_artifact_job_id
            .as_deref(),
        Some("j-a")
    );
    assert!(!update_db(&db, "e", "missing", &change).await.unwrap());
}

/// While the backfill is partway, every packet reads from wherever it is: a
/// table row shadows its inline copy, an inline-only packet is still found, and
/// order is the inline array's, with new packets after it.
#[tokio::test]
async fn reads_merge_inline_packets_with_rows_and_rows_win() {
    let inline_packets = [
        packet("a", DelegatedStatus::Running, Some("j-a")),
        packet("b", DelegatedStatus::Running, Some("j-b")),
    ];
    let (_dir, db) = db_with_execution(inline(&inline_packets)).await;

    let listed = list_db(&db, "e").await.unwrap();
    assert_eq!(listed, inline_packets.to_vec());

    // `a` settles: copied into the table at its index, changed there.
    let done = PacketChange {
        status: Some(DelegatedStatus::Completed),
        ..PacketChange::default()
    };
    assert!(update_db(&db, "e", "a", &done).await.unwrap());
    assert_eq!(row_count(&db).await, 1, "only the changed packet is copied");
    let fresh = packet("c", DelegatedStatus::Pending, None);
    insert_db(&db, "e", &fresh).await.unwrap();

    let listed = list_db(&db, "e").await.unwrap();
    let order: Vec<_> = listed
        .iter()
        .map(|p| (p.id.as_str(), p.status.clone()))
        .collect();
    assert_eq!(
        order,
        [
            ("a", DelegatedStatus::Completed),
            ("b", DelegatedStatus::Running),
            ("c", DelegatedStatus::Pending),
        ],
        "the row shadows the stale inline `a`; `b` is still read inline"
    );

    let by_job = |job: &'static str| {
        read(&db, move |conn| {
            Box::pin(async move { find_by_result_job(conn, "e", job).await.unwrap() })
        })
    };
    assert_eq!(
        by_job("j-a").await.unwrap().status,
        DelegatedStatus::Completed
    );
    assert_eq!(
        by_job("j-b").await.unwrap().status,
        DelegatedStatus::Running
    );
    assert!(by_job("j-none").await.is_none());
}

#[tokio::test]
async fn a_node_resolves_to_the_packet_that_materialized_it() {
    let mut legacy = packet("a", DelegatedStatus::Materialized, Some("j-a"));
    legacy.materialized_node_ids = vec!["delegated-a-agent".to_string()];
    let (_dir, db) = db_with_execution(inline(&[legacy])).await;
    let mut fresh = packet("ab", DelegatedStatus::Materialized, Some("j-ab"));
    // A prefix of another packet's node id must not match it.
    fresh.materialized_node_ids = vec!["delegated-a-agent-2".to_string()];
    insert_db(&db, "e", &fresh).await.unwrap();

    let by_node = |node: &'static str| {
        read(&db, move |conn| {
            Box::pin(async move { find_by_node(conn, "e", node).await.unwrap() })
        })
    };
    assert_eq!(by_node("delegated-a-agent").await.unwrap().id, "a");
    assert_eq!(by_node("delegated-a-agent-2").await.unwrap().id, "ab");
    assert!(by_node("authored-node").await.is_none());
}

/// The backfill moves every inline packet, keeps a row already changed in the
/// table, strips the key, and is a no-op the second time.
#[tokio::test]
async fn moving_inline_packets_strips_the_snapshot_and_keeps_newer_rows() {
    let inline_packets = [
        packet("a", DelegatedStatus::Running, Some("j-a")),
        packet("b", DelegatedStatus::Pending, None),
    ];
    let (_dir, db) = db_with_execution(inline(&inline_packets)).await;
    let done = PacketChange {
        status: Some(DelegatedStatus::Completed),
        ..PacketChange::default()
    };
    update_db(&db, "e", "a", &done).await.unwrap();

    assert_eq!(
        move_inline_packets(&db, "e").await.unwrap(),
        InlineMove::Moved(2)
    );
    let snapshot = db
        .query_one(
            "SELECT snapshot FROM executions WHERE id = 'e'",
            (),
            |row| row.text(0),
        )
        .await
        .unwrap();
    assert!(!snapshot.contains("delegatedPackets"), "{snapshot}");
    crate::snapshot_migrate::load(&snapshot).expect("the stripped snapshot still loads");

    let listed = list_db(&db, "e").await.unwrap();
    assert_eq!(
        listed[0].status,
        DelegatedStatus::Completed,
        "the newer row won"
    );
    assert_eq!(
        listed[1],
        DelegatedWorkPacket {
            prompt_digest: Some(prompt_digest("do b")),
            ..inline_packets[1].clone()
        },
        "a pending packet keeps its prompt and gains its digest"
    );
    assert_eq!(
        move_inline_packets(&db, "e").await.unwrap(),
        InlineMove::Nothing
    );
}

/// The point of the table: a status change on an execution with many large
/// packets writes one small row, not the snapshot.
#[tokio::test]
async fn a_status_change_writes_one_row_not_the_snapshot() {
    let (_dir, db) = db_with_execution(serde_json::json!([])).await;
    for index in 0..40 {
        let mut big = packet(&format!("p{index}"), DelegatedStatus::Pending, None);
        big.problem_statement = Some("x".repeat(256 * 1024));
        insert_db(&db, "e", &big).await.unwrap();
        let materialized = PacketChange {
            status: Some(DelegatedStatus::Materialized),
            result_artifact_job_id: Some(format!("j{index}")),
            ..PacketChange::default()
        };
        update_db(&db, "e", &format!("p{index}"), &materialized)
            .await
            .unwrap();
    }
    let before = db.logical_log_bytes();
    let done = PacketChange {
        status: Some(DelegatedStatus::Completed),
        ..PacketChange::default()
    };
    update_db(&db, "e", "p20", &done).await.unwrap();
    let written = db.logical_log_bytes().saturating_sub(before);
    assert!(
        written < 64 * 1024,
        "one packet transition wrote {written} bytes of log"
    );
}

#[tokio::test]
async fn the_backfill_moves_every_execution_once_and_then_stays_done() {
    let (_dir, db) = db_with_execution(inline(&[packet(
        "a",
        DelegatedStatus::Running,
        Some("j-a"),
    )]))
    .await;
    add_execution(
        &db,
        "e2",
        inline(&[
            packet("b", DelegatedStatus::Pending, None),
            packet("c", DelegatedStatus::Completed, Some("j-c")),
        ]),
    )
    .await;
    add_execution(&db, "e3", serde_json::json!([])).await;

    let report = run_backfill(&db).await.unwrap();

    assert_eq!(
        (report.executions, report.packets, report.completed),
        (2, 3, true),
        "{report:?}"
    );
    assert_eq!(row_count(&db).await, 3);
    assert_eq!(inline_snapshots(&db).await, 0, "every snapshot is stripped");
    let (moved, completed_at) = db
        .query_one(
            "SELECT moved, completed_at FROM delegated_packet_backfill_state WHERE id = 1",
            (),
            |row| Ok((row.i64(0)?, row.opt_i64(1)?)),
        )
        .await
        .unwrap();
    assert_eq!(moved, 3);
    assert!(completed_at.is_some(), "the walk records that it finished");
    assert_eq!(
        list_db(&db, "e2").await.unwrap()[1].status,
        DelegatedStatus::Completed
    );

    assert_eq!(
        run_backfill(&db).await.unwrap(),
        BackfillReport {
            completed: true,
            ..BackfillReport::default()
        },
        "a finished walk does nothing on a later start"
    );
}

/// A restart resumes at the cursor rather than rescanning, and a packet the
/// walk has not reached still reads through from its snapshot.
#[tokio::test]
async fn a_restarted_backfill_resumes_at_its_cursor() {
    let (_dir, db) = db_with_execution(inline(&[packet(
        "a",
        DelegatedStatus::Running,
        Some("j-a"),
    )]))
    .await;
    add_execution(
        &db,
        "e2",
        inline(&[packet("b", DelegatedStatus::Running, Some("j-b"))]),
    )
    .await;
    let first = db
        .query_one("SELECT rowid FROM executions WHERE id = 'e'", (), |row| {
            row.i64(0)
        })
        .await
        .unwrap();
    db.execute(
        "UPDATE delegated_packet_backfill_state SET cursor_rowid = ?1 WHERE id = 1",
        (first,),
    )
    .await
    .unwrap();

    let report = run_backfill(&db).await.unwrap();

    assert_eq!((report.executions, report.packets), (1, 1), "{report:?}");
    assert_eq!(inline_snapshots(&db).await, 1, "`e` is behind the cursor");
    assert_eq!(
        find_by_result_job_db(&db, "e", "j-a")
            .await
            .unwrap()
            .map(|packet| packet.id),
        Some("a".to_string()),
        "a packet still inline is still found"
    );
}
