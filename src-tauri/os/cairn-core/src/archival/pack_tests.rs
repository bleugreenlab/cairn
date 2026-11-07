use std::sync::Arc;

use async_trait::async_trait;
use cairn_db::turso::params;

use super::*;
use crate::storage::content_store::{ContentStoreError, InMemoryContentStore, ARCHIVE_MARKER};
use crate::storage::history_pack::catalog::tier_entry;
use crate::storage::history_pack::write::pack_execution;
use crate::storage::history_pack::HistoryPack;
use crate::storage::{migrated_test_db, query_plan, MigrationRunner, TURSO_MIGRATIONS};

const DAY: i64 = 86_400;
const NOW: i64 = 1_000 * DAY;
/// Resolved well past the 30-day window.
const OLD: i64 = NOW - 40 * DAY;

fn policy() -> PackPolicy {
    PackPolicy {
        window_secs: 30 * DAY,
        bytes_per_row: Some(1_000),
    }
}

/// Production's budget without the free-space floor, which a test host's
/// volume may or may not clear.
fn budget() -> PackBudget {
    PackBudget {
        floor_free_bytes: 0,
        floor_free_pct: 0,
        ..PackBudget::PRODUCTION
    }
}

async fn seed_project(db: &LocalDb) {
    db.execute_script(
        "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES ('ws','w',1,1);
         INSERT INTO projects(id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
           VALUES ('proj','ws','P','p','/nonexistent','main',1,1);",
    )
    .await
    .unwrap();
}

/// Issue `i{n}` with execution `x{n}`: one job, one run, two events, one tool
/// invocation.
async fn seed_execution(
    db: &LocalDb,
    n: i64,
    status: &str,
    completed_at: Option<i64>,
    run_status: &str,
) {
    let script = format!(
        "INSERT INTO issues(id, project_id, number, title, status, completed_at, created_at, updated_at)
           VALUES ('i{n}','proj',{n},'T','{status}',{completed},1,1);
         INSERT INTO executions(id, recipe_id, project_id, issue_id, status, started_at)
           VALUES ('x{n}','r','proj','i{n}','complete',1);
         INSERT INTO jobs(id, execution_id, project_id, issue_id, status, created_at, updated_at)
           VALUES ('j{n}','x{n}','proj','i{n}','complete',1,1);
         INSERT INTO runs(id, job_id, project_id, status, created_at, updated_at)
           VALUES ('r{n}','j{n}','proj','{run_status}',1,1);",
        completed = completed_at.map_or("NULL".to_string(), |at| at.to_string()),
    );
    db.execute_script(&script).await.unwrap();
    for (seq, event_type, data) in [
        (
            1,
            "user",
            serde_json::json!({"eventType": "user", "content": format!("task {n}")}),
        ),
        (
            2,
            "assistant",
            serde_json::json!({"eventType": "assistant", "content": "done"}),
        ),
    ] {
        db.execute(
            "INSERT INTO events(id, run_id, sequence, timestamp, event_type, data, created_at)
             VALUES (?1, ?2, ?3, ?3, ?4, ?5, ?3)",
            params![
                format!("e{n}-{seq}"),
                format!("r{n}"),
                seq,
                event_type,
                data.to_string()
            ],
        )
        .await
        .unwrap();
    }
    db.execute(
        "INSERT INTO tool_invocations(id, event_id, tool_use_id, run_id, ts, verb, tool_name,
                                      target_scheme, target_kind)
         VALUES (?1, ?2, 'tool-1', ?3, 1, 'read', 'read', 'file', 'rs')",
        params![format!("inv{n}"), format!("e{n}-2"), format!("r{n}")],
    )
    .await
    .unwrap();
}

async fn count(db: &LocalDb, sql: &str) -> i64 {
    db.query_one(sql, (), |row| row.i64(0)).await.unwrap()
}

async fn catalogued(db: &LocalDb) -> Vec<String> {
    db.query_all(
        "SELECT execution_id FROM history_tier ORDER BY execution_id",
        (),
        |row| row.text(0),
    )
    .await
    .unwrap()
}

fn skipped(report: &PackReport) -> Vec<(&str, &str)> {
    report
        .skipped
        .iter()
        .map(|p| (p.execution_id.as_str(), p.reason.as_str()))
        .collect()
}

/// A migrated database with `store` as its archive, for stores a test file
/// archive cannot stand in for.
async fn db_with_store(name: &str, store: Arc<dyn ContentStore>) -> LocalDb {
    let path = tempfile::tempdir().unwrap().keep().join(name);
    let db = LocalDb::open(&path).await.unwrap();
    MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
        .run(&db)
        .await
        .unwrap();
    db.set_private_content_store(store)
        .map_err(drop)
        .expect("a fresh handle has no archive");
    db
}

#[tokio::test]
async fn a_pass_packs_every_execution_past_the_window_and_deletes_nothing() {
    let db = migrated_test_db("pack-pass-all.db").await;
    seed_project(&db).await;
    for n in 1..=3 {
        seed_execution(&db, n, "merged", Some(OLD - n), "exited").await;
    }
    seed_execution(&db, 4, "closed", Some(NOW - 5 * DAY), "exited").await;
    seed_execution(&db, 5, "open", None, "exited").await;
    let events = count(&db, "SELECT COUNT(*) FROM events").await;
    let invocations = count(&db, "SELECT COUNT(*) FROM tool_invocations").await;

    let report = run_pack_pass_at(&db, NOW, policy(), budget()).await;

    assert_eq!(report.stopped, None, "{report}");
    assert_eq!(report.packed, 3, "{report}");
    assert!(
        report.skipped.is_empty() && report.failed.is_empty(),
        "{report}"
    );
    assert_eq!(report.event_rows, 6);
    assert_eq!(report.hot_estimate_bytes, 6_000);
    assert_eq!(report.remaining, Some(0));
    assert_eq!(catalogued(&db).await, ["x1", "x2", "x3"]);

    // Nothing is deleted and nothing is evicted, so every reader stays hot.
    assert_eq!(count(&db, "SELECT COUNT(*) FROM events").await, events);
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM tool_invocations").await,
        invocations
    );
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM history_tier WHERE evicted_at IS NOT NULL"
        )
        .await,
        0
    );
    assert!(!db.has_evicted_history());

    let store = db.content_store().unwrap().clone();
    for n in 1..=3 {
        let entry = tier_entry(&db, &format!("x{n}")).await.unwrap().unwrap();
        assert_eq!(entry.resolved_at, OLD - n);
        assert_eq!(entry.packed_at, NOW);
        assert_eq!(entry.evicted_at, None);
        assert_eq!(entry.hot_bytes_estimate, 2_000);
        assert_eq!(entry.row_counts.get("events"), Some(&2));
        assert_eq!(entry.row_counts.get("tool_invocations"), Some(&1));
        let bytes = store.get(&entry.content_hash).await.unwrap().unwrap();
        assert_eq!(bytes.len() as i64, entry.bytes);
        let pack = HistoryPack::open(bytes).unwrap();
        pack.verify().unwrap();
        assert_eq!(pack.run_events(&format!("r{n}")).unwrap().len(), 2);
    }

    // The walk finished, so the cursor is cleared for the next one.
    let saved = progress::load(&db, progress::PACK).await.unwrap();
    assert_eq!(saved, Progress::default());
    // And a second pass has nothing left to do.
    let again = run_pack_pass_at(&db, NOW, policy(), budget()).await;
    assert_eq!((again.packed, again.pages), (0, 0), "{again}");
}

#[tokio::test]
async fn an_execution_with_a_live_run_is_named_and_packed_once_it_ends() {
    let db = migrated_test_db("pack-pass-live.db").await;
    seed_project(&db).await;
    seed_execution(&db, 1, "merged", Some(OLD), "live").await;

    let report = run_pack_pass_at(&db, NOW, policy(), budget()).await;
    assert_eq!(skipped(&report), [("x1", "a run is still live")]);
    assert!(catalogued(&db).await.is_empty());
    assert!(report
        .to_string()
        .contains("skipped: x1 (a run is still live)"));

    db.execute("UPDATE runs SET status = 'exited' WHERE id = 'r1'", ())
        .await
        .unwrap();
    let report = run_pack_pass_at(&db, NOW, policy(), budget()).await;
    assert_eq!(report.packed, 1, "{report}");
}

#[tokio::test]
async fn an_issue_resolved_again_is_packed_again() {
    let db = migrated_test_db("pack-pass-reresolved.db").await;
    seed_project(&db).await;
    seed_execution(&db, 1, "merged", Some(OLD), "exited").await;
    assert_eq!(
        run_pack_pass_at(&db, NOW, policy(), budget()).await.packed,
        1
    );

    db.execute(
        "UPDATE issues SET completed_at = ?1 WHERE id = 'i1'",
        (OLD + DAY,),
    )
    .await
    .unwrap();
    let report = run_pack_pass_at(&db, NOW, policy(), budget()).await;
    assert_eq!(report.packed, 1, "{report}");
    let entry = tier_entry(&db, "x1").await.unwrap().unwrap();
    assert_eq!(entry.resolved_at, OLD + DAY);
}

#[tokio::test]
async fn a_pass_interrupted_after_its_put_converges() {
    let db = migrated_test_db("pack-pass-resume.db").await;
    seed_project(&db).await;
    seed_execution(&db, 1, "merged", Some(OLD), "exited").await;
    // A crash between the put and the catalog commit: the object is stored and
    // nothing names it.
    let built = pack_execution(&db, "x1").await.unwrap();
    let store = db.content_store().unwrap().clone();
    store.put(&built.content_hash, &built.bytes).await.unwrap();
    assert!(catalogued(&db).await.is_empty());

    let report = run_pack_pass_at(&db, NOW, policy(), budget()).await;
    assert_eq!(report.packed, 1, "{report}");
    let entry = tier_entry(&db, "x1").await.unwrap().unwrap();
    assert_eq!(
        entry.content_hash, built.content_hash,
        "the same history packs to the same object"
    );
}

/// Hands back every object with its last byte flipped.
struct CorruptingStore(InMemoryContentStore);

#[async_trait]
impl ContentStore for CorruptingStore {
    async fn put(&self, hash: &str, bytes: &[u8]) -> Result<(), ContentStoreError> {
        self.0.put(hash, bytes).await
    }

    async fn get(&self, hash: &str) -> Result<Option<Vec<u8>>, ContentStoreError> {
        Ok(self.0.get(hash).await?.map(|mut bytes| {
            if let Some(last) = bytes.last_mut() {
                *last ^= 0xff;
            }
            bytes
        }))
    }
}

#[tokio::test]
async fn a_pack_that_does_not_verify_is_named_and_never_catalogued() {
    let db = db_with_store(
        "pack-pass-corrupt.db",
        Arc::new(CorruptingStore(InMemoryContentStore::new())),
    )
    .await;
    seed_project(&db).await;
    seed_execution(&db, 1, "merged", Some(OLD), "exited").await;

    let report = run_pack_pass_at(&db, NOW, policy(), budget()).await;
    assert_eq!(report.stopped, None, "{report}");
    assert_eq!(report.packed, 0);
    assert_eq!(report.failed.len(), 1, "{report}");
    assert_eq!(report.failed[0].execution_id, "x1");
    assert!(catalogued(&db).await.is_empty());
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM history_tier_runs").await,
        0,
        "verification precedes every catalog write"
    );
}

#[tokio::test]
async fn an_offline_archive_halts_the_pass_before_anything_is_packed() {
    let db = migrated_test_db("pack-pass-offline.db").await;
    seed_project(&db).await;
    seed_execution(&db, 1, "merged", Some(OLD), "exited").await;
    let root = db
        .content_store()
        .unwrap()
        .local_root()
        .unwrap()
        .to_path_buf();
    std::fs::remove_file(root.join(ARCHIVE_MARKER)).unwrap();

    let report = run_pack_pass_at(&db, NOW, policy(), budget()).await;
    let stopped = report.stopped.clone().unwrap_or_default();
    assert!(stopped.contains("archive offline"), "{report}");
    assert!(report.halted);
    assert!(catalogued(&db).await.is_empty());
    assert_eq!(
        progress::load(&db, progress::PACK).await.unwrap(),
        Progress::default(),
        "the cursor stays where it was"
    );
}

#[tokio::test]
async fn a_volume_under_the_floor_stops_the_pass_and_says_where() {
    let db = migrated_test_db("pack-pass-floor.db").await;
    seed_project(&db).await;
    seed_execution(&db, 1, "merged", Some(OLD), "exited").await;
    let floor = PackBudget {
        floor_free_bytes: u64::MAX,
        ..budget()
    };

    let report = run_pack_pass_at(&db, NOW, policy(), floor).await;
    let stopped = report.stopped.clone().unwrap_or_default();
    assert!(
        stopped.contains("archive volume under its floor"),
        "{report}"
    );
    let root = db
        .content_store()
        .unwrap()
        .local_root()
        .unwrap()
        .display()
        .to_string();
    assert!(stopped.contains(&root), "{report}");
    assert!(!report.halted);
    assert_eq!(report.packed, 0);
    assert!(catalogued(&db).await.is_empty());
}

#[test]
fn the_floor_is_the_larger_of_its_share_and_its_minimum() {
    let budget = PackBudget::PRODUCTION;
    let gib = 1024 * 1024 * 1024;
    // A 500 GiB volume keeps the 100 GiB minimum; a 2 TiB one keeps 10%.
    assert_eq!(budget.floor_for(500 * gib), 100 * gib);
    assert_eq!(budget.floor_for(2048 * gib), 2048 * gib / 100 * 10);
}

#[tokio::test]
async fn an_oversized_execution_is_skipped_by_name_and_not_reread() {
    let db = migrated_test_db("pack-pass-oversize.db").await;
    seed_project(&db).await;
    seed_execution(&db, 1, "merged", Some(OLD), "exited").await;
    let tiny = PackBudget {
        max_pack_bytes: 1,
        ..budget()
    };

    let report = run_pack_pass_at(&db, NOW, policy(), tiny).await;
    assert_eq!(report.skipped.len(), 1, "{report}");
    assert_eq!(report.skipped[0].execution_id, "x1");
    assert!(report.skipped[0].reason.contains("pack bound"), "{report}");
    assert!(catalogued(&db).await.is_empty());

    // The skip stands for this resolution: the next pass names it again
    // without reading a page of it.
    let again = run_pack_pass_at(&db, NOW, policy(), tiny).await;
    assert_eq!(again.skipped, report.skipped);
    assert_eq!(again.pages, 0, "{again}");
}

#[tokio::test]
async fn a_spent_pass_budget_stops_without_moving_the_cursor() {
    let db = migrated_test_db("pack-pass-budget.db").await;
    seed_project(&db).await;
    seed_execution(&db, 1, "merged", Some(OLD), "exited").await;
    let spent = PackBudget {
        pass_time: Duration::ZERO,
        ..budget()
    };

    let report = run_pack_pass_at(&db, NOW, policy(), spent).await;
    let stopped = report.stopped.clone().unwrap_or_default();
    assert!(stopped.contains("pass budget"), "{report}");
    assert!(!report.halted);
    assert_eq!(report.packed, 0);
    assert_eq!(report.remaining, Some(1));
}

#[tokio::test]
async fn an_execution_limit_stops_the_pass_where_the_next_resumes() {
    let db = migrated_test_db("pack-pass-limit.db").await;
    seed_project(&db).await;
    for n in 1..=3 {
        seed_execution(&db, n, "merged", Some(OLD + n), "exited").await;
    }
    let two = PackBudget {
        max_executions: 2,
        ..budget()
    };
    let report = run_pack_pass_at(&db, NOW, policy(), two).await;
    assert_eq!(report.packed, 2, "{report}");
    assert!(report
        .stopped
        .as_deref()
        .is_some_and(|stop| stop.contains("limit of 2")));
    assert!(!report.halted);
    let report = run_pack_pass_at(&db, NOW, policy(), two).await;
    assert_eq!(report.packed, 1, "{report}");
    assert_eq!(catalogued(&db).await, ["x1", "x2", "x3"]);
}

#[tokio::test]
async fn the_cursor_resumes_mid_walk() {
    let db = migrated_test_db("pack-pass-cursor.db").await;
    seed_project(&db).await;
    for n in 1..=3 {
        seed_execution(&db, n, "merged", Some(OLD + n), "exited").await;
    }
    // As if a previous pass stopped after x2: x1 was skipped then, so it waits
    // for the walk to come round again.
    save_cursor(
        &db,
        Some(&Candidate {
            completed_at: OLD + 2,
            execution_id: "x2".into(),
        }),
    )
    .await
    .map_err(|stop| stop.reason)
    .unwrap();

    let report = run_pack_pass_at(&db, NOW, policy(), budget()).await;
    assert_eq!(report.packed, 1, "{report}");
    assert_eq!(catalogued(&db).await, ["x3"]);
    let report = run_pack_pass_at(&db, NOW, policy(), budget()).await;
    assert_eq!(catalogued(&db).await, ["x1", "x2", "x3"], "{report}");
}

#[test]
fn no_statement_of_the_pass_deletes() {
    // The pass writes catalog rows and its cursor and nothing else.
    let source = include_str!("pack.rs");
    for forbidden in ["DELETE", "evicted_at =", "UPDATE events"] {
        assert!(
            !source.contains(forbidden),
            "pack.rs must not contain {forbidden:?}"
        );
    }
}

#[test]
fn the_report_names_skips_and_why_it_stopped() {
    let report = PackReport {
        packed: 2,
        pack_bytes: 3 * 1024 * 1024,
        bytes_per_row: 2_323,
        skipped: (0..22)
            .map(|n| Passed {
                execution_id: format!("x{n}"),
                reason: "a run is still live".into(),
            })
            .collect(),
        largest: Some(("x9".into(), 2 * 1024 * 1024)),
        hold_max: Duration::from_millis(40),
        hold_max_execution: Some("x9".into()),
        remaining: Some(1_001),
        stopped: Some("archive volume under its floor".into()),
        ..PackReport::default()
    };
    let line = report.to_string();
    assert!(
        line.starts_with("2 executions packed (3.0 MiB packed"),
        "{line}"
    );
    assert!(line.contains("22 skipped"), "{line}");
    assert!(line.contains("held max 40 ms (x9)"), "{line}");
    assert!(line.contains("largest 2.0 MiB (x9)"), "{line}");
    assert!(
        line.contains("remaining more than 1000 candidates"),
        "{line}"
    );
    assert!(
        line.contains("x19 (a run is still live), and 2 more"),
        "{line}"
    );
    assert!(!line.contains("x20 ("), "{line}");
    assert!(
        line.ends_with("; stopped: archive volume under its floor"),
        "{line}"
    );
}

#[tokio::test]
async fn the_pass_statements_stay_on_their_indexes() {
    let db = migrated_test_db("pack-pass-plans.db").await;
    let candidates = query_plan(
        &db,
        CANDIDATES_SQL,
        vec![
            NOW.into(),
            i64::MIN.into(),
            String::new().into(),
            CANDIDATE_PAGE.into(),
        ],
    )
    .await;
    assert!(
        candidates.contains("SEARCH j USING INDEX idx_jobs_issue_id (issue_id=?)"),
        "{candidates}"
    );
    assert!(
        candidates
            .contains("SEARCH t USING INDEX sqlite_autoindex_history_tier_1 (execution_id=?)"),
        "{candidates}"
    );
    assert!(!candidates.contains("SCAN j"), "{candidates}");
    let live = query_plan(&db, LIVE_RUN_SQL, vec!["x1".to_string().into()]).await;
    assert!(
        live.contains("SEARCH j USING INDEX idx_jobs_execution_id (execution_id=?)"),
        "{live}"
    );
    assert!(
        live.contains("SEARCH r USING INDEX idx_runs_job_id (job_id=?)"),
        "{live}"
    );
    assert!(!live.contains("SCAN runs"), "{live}");
}
