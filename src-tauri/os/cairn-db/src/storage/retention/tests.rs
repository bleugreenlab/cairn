use std::collections::BTreeSet;

use tempfile::{tempdir, TempDir};

use super::*;
use crate::storage::{MigrationRunner, TEAM_MIGRATIONS};

const NOW: i64 = 1_800_000_000;
const HOUR: i64 = 3_600;

/// A budget that never ends a pass early and never folds.
const UNBOUNDED: EvictorBudget = EvictorBudget {
    pass_time: Duration::from_secs(600),
    pass_rows: usize::MAX,
    fold_at_log_bytes: u64::MAX,
    fold_work_target: None,
    folds_per_pass: 0,
};

#[test]
fn the_evictor_runs_unless_the_operator_turns_it_off() {
    for on in [None, Some(""), Some("on"), Some("1"), Some("typo")] {
        assert!(evictor_enabled(on), "{on:?}");
    }
    for off in ["off", "OFF", " off ", "0", "false", "no"] {
        assert!(!evictor_enabled(Some(off)), "{off:?}");
    }
}

async fn run(db: &LocalDb, budget: EvictorBudget, rules: &[Eviction]) -> PassReport {
    run_rules(db, NOW, budget, &mut EvictorState::default(), rules).await
}

async fn private_db() -> (TempDir, LocalDb) {
    let temp = tempdir().unwrap();
    let db = LocalDb::open_migrated_for_test(temp.path().join("retention.db"))
        .await
        .unwrap();
    (temp, db)
}

async fn team_db() -> (TempDir, LocalDb) {
    let temp = tempdir().unwrap();
    let db = LocalDb::open(temp.path().join("team.db")).await.unwrap();
    MigrationRunner::new(TEAM_MIGRATIONS.to_vec())
        .run(&db)
        .await
        .unwrap();
    (temp, db)
}

/// Every table `db` holds, as `sqlite_master` lists it, less the engine's own.
async fn table_names(db: &LocalDb) -> Vec<String> {
    db.query_all(
        "SELECT name FROM sqlite_master WHERE type = 'table' \
         AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__turso%'",
        (),
        |row| row.text(0),
    )
    .await
    .unwrap()
}

#[derive(Debug, Default, PartialEq, Eq)]
struct Audit {
    /// Tables a lineage creates that have no rule.
    unruled: Vec<String>,
    /// Rules for tables no lineage creates.
    stale: Vec<&'static str>,
    /// Tables with more than one rule.
    duplicated: Vec<&'static str>,
}

fn audit(tables: &BTreeSet<String>) -> Audit {
    let mut seen = BTreeSet::new();
    let mut audit = Audit::default();
    for entry in TABLE_RETENTION {
        if !seen.insert(entry.table) {
            audit.duplicated.push(entry.table);
        }
        if !tables.contains(entry.table) {
            audit.stale.push(entry.table);
        }
    }
    audit.unruled = tables
        .iter()
        .filter(|table| !seen.contains(table.as_str()))
        .cloned()
        .collect();
    audit
}

async fn both_lineages() -> BTreeSet<String> {
    let (_private_temp, private) = private_db().await;
    let (_team_temp, team) = team_db().await;
    let mut tables: BTreeSet<String> = table_names(&private).await.into_iter().collect();
    tables.extend(table_names(&team).await);
    tables
}

/// The registry is the retention census's denominator: a migration that adds a
/// table without a rule fails here, beside `team_schema_matches_private`.
#[tokio::test]
async fn every_table_has_a_retention_rule() {
    let tables = both_lineages().await;
    assert_eq!(
        audit(&tables),
        Audit::default(),
        "every table either lineage creates needs exactly one rule in TABLE_RETENTION \
         (storage/retention.rs), and every rule must name a real table"
    );
}

#[tokio::test]
async fn a_table_without_a_rule_fails_the_audit() {
    let (_temp, db) = private_db().await;
    db.execute_script("CREATE TABLE injected_without_a_rule (id INTEGER PRIMARY KEY)")
        .await
        .unwrap();
    let tables: BTreeSet<String> = table_names(&db).await.into_iter().collect();
    assert_eq!(audit(&tables).unruled, vec!["injected_without_a_rule"]);
}

#[test]
fn rules_name_registered_parents_owners_and_evictions() {
    let registered: BTreeSet<&str> = TABLE_RETENTION.iter().map(|entry| entry.table).collect();
    let is_owner =
        |owner: &str| owner.starts_with("cairn/") || owner.starts_with("cairn://p/cairn/");
    for entry in TABLE_RETENTION {
        let table = entry.table;
        assert!(entry.budget_mb > 0, "`{table}` needs a budget");
        match entry.rule {
            Retention::Keep(reason) => assert!(!reason.is_empty(), "`{table}` keeps for no reason"),
            Retention::Owned { parent } => assert!(
                registered.contains(parent),
                "`{table}` is owned by unregistered `{parent}`"
            ),
            Retention::Tiered { owner } | Retention::Undecided { owner, .. } => {
                assert!(
                    is_owner(owner),
                    "`{table}` names no issue or thread: {owner:?}"
                )
            }
            Retention::OwnedBy { owner, rule } => {
                assert!(
                    !owner.is_empty() && !rule.is_empty(),
                    "`{table}` owner unnamed"
                )
            }
            Retention::Evict(eviction) => assert!(
                eviction.tables().contains(&table),
                "`{table}` is evicted by {eviction:?}, which does not delete from it"
            ),
        }
    }
    for eviction in Eviction::ALL {
        for table in eviction.tables() {
            assert_eq!(
                retention_for(table).map(|entry| entry.rule),
                Some(Retention::Evict(eviction)),
                "{eviction:?} deletes from `{table}`, so its rule must say so"
            );
        }
    }
}

// --- Plan pins (cairn/5119): every statement the evictor runs seeks. ---

async fn query_plan(db: &LocalDb, sql: &str, binds: usize) -> Vec<String> {
    let sql = format!("EXPLAIN QUERY PLAN {sql}");
    db.read(move |conn| {
        Box::pin(async move {
            let bound = vec![Value::Integer(1); binds];
            let mut rows = conn.query(&sql, bound).await?;
            let mut plan = Vec::new();
            while let Some(row) = rows.next().await? {
                plan.push(row.text(3)?);
            }
            Ok(plan)
        })
    })
    .await
    .unwrap()
}

async fn assert_seeks(db: &LocalDb, sql: &str, binds: usize, using: &str) {
    let plan = query_plan(db, sql, binds).await;
    assert!(
        plan.iter().any(|line| line.contains(using)),
        "expected `{using}` in the plan of {sql}: {plan:#?}"
    );
    assert!(
        !plan.iter().any(|line| line.starts_with("SCAN")),
        "{sql} scans: {plan:#?}"
    );
}

#[tokio::test]
async fn every_selection_and_delete_seeks() {
    let (_temp, db) = private_db().await;
    const ROWID: &str = "USING INTEGER PRIMARY KEY";
    for walk in [OUTBOX_WALK, SEARCH_OUTBOX_WALK, STREAM_WALK, CODEMAP_WALK] {
        assert_seeks(&db, walk, 2, ROWID).await;
    }
    assert_seeks(
        &db,
        STREAM_CHUNKS,
        2,
        "idx_message_stream_chunks_stream_kind_index",
    )
    .await;
    assert_seeks(
        &db,
        CODEMAP_NEWEST,
        1,
        "idx_codemap_cache_project_computed_at",
    )
    .await;
    assert_seeks(&db, ARC_CHAINS, 2, "idx_artifacts_output_job_version").await;
    assert_seeks(&db, ARC_SUPERSEDED, 4, "idx_artifacts_output_job_version").await;
    assert_seeks(&db, ARC_PINNED, 1, "idx_job_artifact_inputs_producer").await;
    for table in [
        "effect_outbox",
        "search_outbox",
        "message_streams",
        "message_stream_chunks",
        "codemap_cache",
        "artifacts",
    ] {
        let delete = format!("DELETE FROM {table} WHERE rowid IN ({})", placeholders(3));
        assert_seeks(&db, &delete, 3, ROWID).await;
    }
    for statement in [ARC_UNLINK, ARC_DANGLING] {
        assert_seeks(
            &db,
            &format!("{statement} (?1, ?2)"),
            2,
            "idx_artifacts_parent_version",
        )
        .await;
    }
}

// --- Behaviour. ---

async fn seed_owners(db: &LocalDb) {
    db.execute_script(
        "INSERT INTO workspaces (id,name,created_at,updated_at) VALUES ('w','W',1,1);
         INSERT INTO projects (id,workspace_id,name,key,repo_path,created_at,updated_at)
             VALUES ('p','w','P','prj','/tmp/p',1,1);
         INSERT INTO issues (id,project_id,number,title,status,attention,created_at,updated_at)
             VALUES ('i','p',1,'T','active','none',1,1);
         INSERT INTO jobs (id,issue_id,project_id,status,uri_segment,node_name,created_at,updated_at)
             VALUES ('j','i','p','running','b','b',1,1);
         INSERT INTO jobs (id,issue_id,project_id,status,uri_segment,node_name,created_at,updated_at)
             VALUES ('consumer','i','p','running','c','c',1,1);
         INSERT INTO runs (id, job_id, status, created_at, updated_at) VALUES ('r','j','exited',1,1);",
    )
    .await
    .unwrap();
}

async fn ids(db: &LocalDb, sql: &'static str) -> Vec<String> {
    db.query_all(sql, (), |row| row.text(0)).await.unwrap()
}

async fn count(db: &LocalDb, sql: &'static str) -> i64 {
    db.query_one(sql, (), |row| row.i64(0)).await.unwrap()
}

async fn insert_outbox(db: &LocalDb, id: &str, state: &str, created_at: i64, updated_at: i64) {
    db.execute(
        "INSERT INTO effect_outbox (id, kind, dedupe_key, payload_json, state, created_at, updated_at)
         VALUES (?1, 'embed_event', ?1, '{}', ?2, ?3, ?4)",
        (id.to_string(), state.to_string(), created_at, updated_at),
    )
    .await
    .unwrap();
}

/// The cases the outbox's old `gc` covered, now through the paged evictor.
#[tokio::test]
async fn done_outbox_entries_go_after_a_day_and_nothing_else_does() {
    let (_temp, db) = private_db().await;
    let old = NOW - 3 * 24 * HOUR;
    insert_outbox(&db, "done-old", "done", old, NOW - 2 * 24 * HOUR).await;
    insert_outbox(&db, "done-recently", "done", old, NOW - HOUR).await;
    insert_outbox(&db, "pending-old", "pending", old, old).await;
    insert_outbox(&db, "failed-old", "failed", old, old).await;
    insert_outbox(&db, "done-new", "done", NOW - HOUR, NOW - HOUR).await;

    let report = run(&db, UNBOUNDED, &[Eviction::DoneOutbox]).await;

    assert_eq!(report.end, PassEnd::Complete);
    assert!(report.rules[0].finished);
    assert_eq!(report.rows(), 1);
    assert_eq!(
        ids(&db, "SELECT id FROM effect_outbox ORDER BY id").await,
        vec!["done-new", "done-recently", "failed-old", "pending-old"]
    );
}

#[tokio::test]
async fn a_backlog_is_deleted_in_pages_of_at_most_500_rows() {
    let (_temp, db) = private_db().await;
    let old = NOW - 3 * 24 * HOUR;
    db.write(move |conn| {
        Box::pin(async move {
            for index in 0..1_201 {
                conn.execute(
                    "INSERT INTO effect_outbox (id, kind, dedupe_key, payload_json, state, created_at, updated_at)
                     VALUES (?1, 'embed_event', ?1, '{}', 'done', ?2, ?2)",
                    (format!("e{index}"), old),
                )
                .await?;
            }
            Ok(())
        })
    })
    .await
    .unwrap();

    let report = run(&db, UNBOUNDED, &[Eviction::DoneOutbox]).await;

    let rule = &report.rules[0];
    assert_eq!((rule.rows, rule.pages, rule.max_page_rows), (1_201, 3, 500));
    assert_eq!(count(&db, "SELECT COUNT(*) FROM effect_outbox").await, 0);
}

#[tokio::test]
async fn a_pass_folds_a_grown_log_and_ends_at_its_fold_budget() {
    let (_temp, db) = private_db().await;
    insert_outbox(
        &db,
        "done-old",
        "done",
        NOW - 3 * 24 * HOUR,
        NOW - 2 * 24 * HOUR,
    )
    .await;
    let budget = EvictorBudget {
        fold_at_log_bytes: 0,
        folds_per_pass: 1,
        ..UNBOUNDED
    };

    let report = run(&db, budget, &[Eviction::DoneOutbox]).await;

    assert_eq!(report.folds.len(), 1);
    assert!(report.folds[0].succeeded, "{:?}", report.folds);
    assert_eq!(report.end, PassEnd::FoldBudget);
    assert_eq!(report.rows(), 1);
}

async fn insert_stream(
    db: &LocalDb,
    id: &'static str,
    status: &'static str,
    created_at: i64,
    finalized_at: Option<i64>,
    chunks: usize,
) {
    db.write(move |conn| {
        Box::pin(async move {
            conn.execute(
                "INSERT INTO message_streams (id, run_id, backend, sequence, status, created_at, updated_at, finalized_at)
                 VALUES (?1, 'r', 'claude', 0, ?2, ?3, ?3, ?4)",
                (id, status, created_at, finalized_at),
            )
            .await?;
            for index in 0..chunks {
                conn.execute(
                    "INSERT INTO message_stream_chunks (id, stream_id, kind, chunk_index, data, char_count, created_at)
                     VALUES (?1, ?2, 'content', ?3, 'x', 1, ?4)",
                    (format!("{id}-{index}"), id, index as i64, created_at),
                )
                .await?;
            }
            Ok(())
        })
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn terminal_streams_go_with_their_chunks_a_day_after_they_end() {
    let (_temp, db) = private_db().await;
    seed_owners(&db).await;
    let old = NOW - 3 * 24 * HOUR;
    insert_stream(&db, "finalized-old", "finalized", old, Some(old), 3).await;
    insert_stream(&db, "long-old", "finalized", old, Some(old), 1_100).await;
    insert_stream(&db, "open-old", "open", old, None, 2).await;
    insert_stream(&db, "aborted-old", "aborted", old, Some(old), 1).await;
    insert_stream(
        &db,
        "finalized-recently",
        "finalized",
        old,
        Some(NOW - HOUR),
        2,
    )
    .await;
    insert_stream(
        &db,
        "finalized-new",
        "finalized",
        NOW - HOUR,
        Some(NOW - HOUR),
        2,
    )
    .await;

    let report = run(&db, UNBOUNDED, &[Eviction::TerminalStreams]).await;

    let rule = &report.rules[0];
    assert!(rule.finished);
    assert_eq!(rule.rows, 3 + 1_100 + 1 + 3);
    assert!(rule.max_page_rows <= PAGE_ROWS, "{rule:?}");
    assert_eq!(
        ids(&db, "SELECT id FROM message_streams ORDER BY id").await,
        vec!["finalized-new", "finalized-recently", "open-old"]
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM message_stream_chunks").await,
        6
    );
}

async fn insert_codemap(
    db: &LocalDb,
    sha: &'static str,
    computed_at: i64,
    invalidated_at: Option<i64>,
) {
    db.execute(
        "INSERT INTO codemap_cache (project_id, base_commit_sha, computed_at, invalidated_at, file_count, edge_count, codemap_json)
         VALUES ('p', ?1, ?2, ?3, 0, 0, '{}')",
        (sha, computed_at, invalidated_at),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn invalidated_codemaps_go_except_the_projects_newest() {
    let (_temp, db) = private_db().await;
    seed_owners(&db).await;
    let old = NOW - 3 * 24 * HOUR;
    insert_codemap(&db, "invalidated-1", old, Some(old)).await;
    insert_codemap(&db, "invalidated-2", old + 1, Some(old)).await;
    insert_codemap(&db, "invalidated-recently", old + 2, Some(NOW - HOUR)).await;
    insert_codemap(&db, "current", old - 10, None).await;
    insert_codemap(&db, "invalidated-newest", old + 3, Some(old)).await;

    let report = run(&db, UNBOUNDED, &[Eviction::InvalidatedCodemaps]).await;

    assert!(report.rules[0].finished);
    assert_eq!(
        ids(
            &db,
            "SELECT base_commit_sha FROM codemap_cache ORDER BY base_commit_sha"
        )
        .await,
        vec!["current", "invalidated-newest", "invalidated-recently"]
    );
}

#[tokio::test]
async fn arc_versions_beyond_the_newest_five_go_unless_pinned() {
    let (_temp, db) = private_db().await;
    seed_owners(&db).await;
    db.write(|conn| {
        Box::pin(async move {
            for (kind, name) in [("arc", "arc"), ("plan", "plan")] {
                for version in 1..=8i64 {
                    let parent = (version > 1).then(|| format!("{name}-{}", version - 1));
                    conn.execute(
                        "INSERT INTO artifacts (id, job_id, artifact_type, data, version, parent_version_id, output_name, created_at, updated_at)
                         VALUES (?1, 'j', ?2, '{}', ?3, ?4, ?5, 1, 1)",
                        (format!("{name}-{version}"), kind, version, parent, name),
                    )
                    .await?;
                }
            }
            conn.execute(
                "INSERT INTO job_artifact_inputs (job_id, producer_job_id, output_name, version, updated_at)
                 VALUES ('consumer', 'j', 'arc', 2, 1)",
                (),
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();

    let report = run(&db, UNBOUNDED, &[Eviction::SupersededArcVersions]).await;

    assert!(report.rules[0].finished, "{report}");
    assert_eq!(report.rows(), 2);
    assert_eq!(
        ids(
            &db,
            "SELECT id FROM artifacts WHERE output_name = 'arc' ORDER BY version"
        )
        .await,
        vec!["arc-2", "arc-4", "arc-5", "arc-6", "arc-7", "arc-8"]
    );
    assert_eq!(
        ids(
            &db,
            "SELECT id FROM artifacts WHERE output_name = 'arc' AND parent_version_id IS NULL ORDER BY version"
        )
        .await,
        vec!["arc-2", "arc-4"],
        "the oldest kept versions no longer point at deleted ones"
    );
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM artifacts WHERE output_name = 'plan'"
        )
        .await,
        8
    );
}

/// The pinned engine checks a self-referencing foreign key by scanning the
/// table (this is why arc versions are deleted with enforcement off). If an
/// engine bump makes the check use the index, this fails, and the arc rule can
/// go back to an ordinary write.
#[tokio::test]
async fn the_engine_still_scans_for_self_referencing_children() {
    let (_temp, db) = private_db().await;
    let program = db
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query("EXPLAIN DELETE FROM artifacts WHERE rowid = 1", ())
                    .await?;
                let mut ops = Vec::new();
                while let Some(row) = rows.next().await? {
                    ops.push(format!("{} {}", row.text(1)?, row.text(7)?));
                }
                Ok(ops)
            })
        })
        .await
        .unwrap();
    assert!(
        program
            .iter()
            .any(|op| op.starts_with("Rewind") && op.contains("artifacts")),
        "the self-reference check no longer scans artifacts: {program:#?}"
    );
}

#[tokio::test]
async fn applied_search_outbox_entries_go_after_a_day() {
    let (_temp, db) = private_db().await;
    let old = NOW - 3 * 24 * HOUR;
    for (id, status, created_at) in [
        ("applied-old", "applied", old),
        ("pending-old", "pending", old),
        ("applied-new", "applied", NOW - HOUR),
    ] {
        db.execute(
            "INSERT INTO search_outbox (id, source_table, source_id, content_type, op, status, created_at)
             VALUES (?1, 'events', 'e', 'event', 'upsert', ?2, ?3)",
            (id, status, created_at),
        )
        .await
        .unwrap();
    }

    let report = run(&db, UNBOUNDED, &[Eviction::AppliedSearchOutbox]).await;

    assert!(report.rules[0].finished);
    assert_eq!(
        ids(&db, "SELECT id FROM search_outbox ORDER BY id").await,
        vec!["applied-new", "pending-old"]
    );
}

#[tokio::test]
async fn a_production_pass_over_an_empty_database_completes() {
    let (_temp, db) = private_db().await;
    let report = run_pass(
        &db,
        NOW,
        EvictorBudget::PRODUCTION,
        &mut EvictorState::default(),
    )
    .await;
    assert_eq!(report.end, PassEnd::Complete, "{report}");
    assert!(report
        .rules
        .iter()
        .all(|rule| rule.finished && rule.error.is_none()));
}

/// A rule's first page is folded alone; after that, a fold covers as many
/// pages as fit the work target at the measured cost, and a fold window never
/// spans two rules.
#[test]
fn folds_are_sized_by_the_measured_cost_of_each_rule() {
    let target = Duration::from_millis(500);
    let mut state = EvictorState::default();
    assert!(!state.fold_due(Eviction::DoneOutbox, target));
    state.deleted_page(Eviction::DoneOutbox);
    assert!(
        state.fold_due(Eviction::DoneOutbox, target),
        "unmeasured: one page"
    );

    state.folded(Duration::from_millis(100));
    assert_eq!(state.pages_per_fold(Eviction::DoneOutbox, target), 5);
    for _ in 0..4 {
        state.deleted_page(Eviction::DoneOutbox);
        assert!(!state.fold_due(Eviction::DoneOutbox, target));
    }
    state.deleted_page(Eviction::DoneOutbox);
    assert!(state.fold_due(Eviction::DoneOutbox, target));
    assert!(
        state.fold_due(Eviction::TerminalStreams, target),
        "another rule's pages are folded before it starts"
    );

    // A page that folds slower than the target is still folded one at a time.
    state.folded(Duration::from_millis(30_000));
    assert_eq!(state.pages_per_fold(Eviction::DoneOutbox, target), 1);
}

#[test]
fn quantiles_take_the_nearest_rank() {
    assert_eq!(quantile(&[], 0.5), 0);
    assert_eq!(quantile(&[5, 1, 3], 0.5), 3);
    assert_eq!(quantile(&[5, 1, 3], 0.99), 5);
}
