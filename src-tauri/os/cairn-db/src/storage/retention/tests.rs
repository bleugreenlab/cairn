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

async fn both_lineages() -> BTreeSet<String> {
    let (_private_temp, private) = private_db().await;
    let (_team_temp, team) = team_db().await;
    let mut tables: BTreeSet<String> = table_names(&private).await.unwrap().into_iter().collect();
    tables.extend(table_names(&team).await.unwrap());
    tables
}

/// The registry is the retention census's denominator: a migration that adds a
/// table without a rule fails here, beside `team_schema_matches_private`.
#[tokio::test]
async fn every_table_has_a_retention_rule() {
    let tables = both_lineages().await;
    assert_eq!(
        audit(&tables),
        RetentionAudit::default(),
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
    assert_eq!(
        audit_tables(&db).await.unwrap().unruled,
        vec!["injected_without_a_rule"]
    );
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
    for walk in [SETTLED_WAIT_WALK, UNSTAMPED_WAIT_WALK] {
        assert_seeks(&db, walk, 3, "idx_agent_waits_settled").await;
        let plan = query_plan(&db, walk, 3).await;
        assert!(
            !plan.iter().any(|line| line.contains("SORTER")),
            "{walk} sorts: {plan:#?}"
        );
    }
    assert_seeks(
        &db,
        HELD_TRANSFERRED_WAITS,
        1,
        "idx_permission_requests_status",
    )
    .await;
    assert_seeks(
        &db,
        WATCHDOG_WALK,
        3,
        "USING COVERING INDEX idx_codex_watchdog_lifecycle_retention",
    )
    .await;
    let plan = query_plan(&db, WATCHDOG_WALK, 3).await;
    assert!(
        !plan.iter().any(|line| line.contains("SORTER")),
        "{plan:#?}"
    );
    for table in [
        "codex_watchdog_lifecycle",
        "effect_outbox",
        "search_outbox",
        "message_streams",
        "message_stream_chunks",
        "codemap_cache",
        "artifacts",
        "agent_waits",
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

/// A pass capped at one fold ends after its first page. The next pass starts
/// at the rule after the one that spent the budget, so a cap on folds is never
/// a cap on which rules run.
#[tokio::test]
async fn a_capped_pass_rotates_so_every_rule_gets_a_page() {
    let (_temp, db) = private_db().await;
    seed_wait_owners(&db).await;
    insert_watchdog(&db, "watchdog-old", "phase_transition", NOW - 8 * DAY_S).await;
    let old = NOW - 3 * 24 * HOUR;
    insert_outbox(&db, "done-old", "done", old, NOW - 2 * 24 * HOUR).await;
    insert_stream(&db, "finalized-old", "finalized", old, Some(old), 2).await;
    let budget = EvictorBudget {
        fold_at_log_bytes: 0,
        folds_per_pass: 1,
        ..UNBOUNDED
    };
    let mut state = EvictorState::default();

    let served = |report: &PassReport| -> Vec<&'static str> {
        report
            .rules
            .iter()
            .filter(|rule| rule.rows > 0)
            .map(|rule| rule.eviction.name())
            .collect()
    };
    let first = run_pass(&db, NOW, budget, &mut state).await;
    assert_eq!(first.end, PassEnd::FoldBudget, "{first}");
    assert_eq!(served(&first), vec!["watchdog_lifecycle"], "{first}");
    assert_eq!(first.rules[0].eviction, Eviction::WatchdogLifecycle);

    let second = run_pass(&db, NOW, budget, &mut state).await;
    assert_eq!(second.end, PassEnd::FoldBudget, "{second}");
    assert_eq!(second.rules[0].eviction, Eviction::DoneOutbox, "{second}");
    assert_eq!(served(&second), vec!["done_outbox"], "{second}");

    let third = run_pass(&db, NOW, budget, &mut state).await;
    assert_eq!(third.end, PassEnd::FoldBudget, "{third}");
    assert_eq!(
        third.rules[0].eviction,
        Eviction::TerminalStreams,
        "{third}"
    );
    assert_eq!(served(&third), vec!["terminal_streams"], "{third}");
    assert_eq!(count(&db, "SELECT COUNT(*) FROM message_streams").await, 0);
}

/// The rotation's step on its own: a finished pass starts the next at the
/// first rule, one cut short mid-rule resumes that rule, and one whose first
/// rule spent the whole budget moves past it.
#[test]
fn the_next_pass_starts_where_this_one_was_cut_short() {
    let rule = |eviction| RuleReport {
        eviction,
        pages: 1,
        rows: 1,
        max_page_rows: 1,
        select_ms: Vec::new(),
        write_ms: Vec::new(),
        finished: false,
        error: None,
    };
    let report = |end, served: &[Eviction]| PassReport {
        rules: served.iter().map(|eviction| rule(*eviction)).collect(),
        folds: Vec::new(),
        log_peak_bytes: 0,
        duration: Duration::ZERO,
        end,
    };
    let mut rules = Eviction::ALL;
    rules.rotate_left(2);
    let mut state = EvictorState::default();

    state.advance(&rules, &report(PassEnd::Complete, &rules));
    assert_eq!(state.start, 0);
    state.advance(&rules, &report(PassEnd::FoldBudget, &rules[..3]));
    assert_eq!(state.start, 4, "resumes the rule it was cut short in");
    state.advance(&rules, &report(PassEnd::TimeBudget, &rules[..1]));
    assert_eq!(state.start, 3, "moves past a first rule that spent it all");
    let last = Eviction::ALL.len() - 1;
    state.advance(
        &Eviction::ALL[last..],
        &report(PassEnd::RowBudget, &Eviction::ALL[last..]),
    );
    assert_eq!(state.start, 0, "wraps past the end");
}

/// A fold before the pass deletes anything covers other writers' log and pages
/// carried over from the previous pass, so it clears them without teaching the
/// estimate. A fold over the pass's own pages does teach it.
#[tokio::test]
async fn a_settling_fold_does_not_train_the_estimate() {
    let (_temp, db) = private_db().await;
    let budget = EvictorBudget {
        fold_work_target: Some(Duration::from_millis(500)),
        folds_per_pass: 2,
        ..UNBOUNDED
    };

    let mut state = EvictorState::default();
    state.deleted_page(Eviction::DoneOutbox);
    let settling = run_rules(&db, NOW, budget, &mut state, &[Eviction::DoneOutbox]).await;
    assert_eq!(settling.folds.len(), 1, "{settling}");
    assert!(settling.folds[0].succeeded, "{settling}");
    assert!(state.unfolded.is_none());
    assert_eq!(state.fold_ms_per_page(Eviction::DoneOutbox), None);

    insert_outbox(
        &db,
        "done-old",
        "done",
        NOW - 3 * 24 * HOUR,
        NOW - 2 * 24 * HOUR,
    )
    .await;
    let trained = run_rules(
        &db,
        NOW,
        budget,
        &mut state,
        &[Eviction::DoneOutbox, Eviction::AppliedSearchOutbox],
    )
    .await;
    assert_eq!(trained.folds.len(), 1, "{trained}");
    assert_eq!(trained.rows(), 1, "{trained}");
    assert!(
        state.fold_ms_per_page(Eviction::DoneOutbox).is_some(),
        "a fold over the pass's own page trains its rule"
    );
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

const DAY_MS: i64 = 86_400_000;
const NOW_MS: i64 = NOW * 1_000;

async fn seed_wait_owners(db: &LocalDb) {
    seed_owners(db).await;
    db.execute_script(
        "INSERT INTO sessions (id, job_id, created_at, updated_at) VALUES ('s', 'j', 1, 1);
         INSERT INTO turns (id, session_id, run_id, job_id, sequence, state, created_at, updated_at)
             VALUES ('t', 's', 'r', 'j', 1, 'yielded', 1, 1);",
    )
    .await
    .unwrap();
}

/// A wait on its own call (`tool_use_id` = `id`), in milliseconds.
async fn insert_wait(
    db: &LocalDb,
    id: &str,
    state: &str,
    resolution: Option<&str>,
    created_at: i64,
    resolved_at: Option<i64>,
) {
    db.execute(
        "INSERT INTO agent_waits (id, job_id, run_id, session_id, predecessor_turn_id, tool_use_id,
             condition_json, state, resolution_json, created_at, resolved_at)
         VALUES (?1, 'j', 'r', 's', 't', ?1, '{}', ?2, ?3, ?4, ?5)",
        (
            id.to_string(),
            state.to_string(),
            resolution.map(str::to_string),
            created_at,
            resolved_at,
        ),
    )
    .await
    .unwrap();
}

async fn insert_permission(db: &LocalDb, tool_use_id: &str, status: &str) {
    db.execute(
        "INSERT INTO permission_requests (id, run_id, tool_use_id, tool_name, tool_input, status, created_at)
         VALUES (?1, 'r', ?1, 'Bash', '{}', ?2, 1)",
        (tool_use_id.to_string(), status.to_string()),
    )
    .await
    .unwrap();
}

async fn wait_ids(db: &LocalDb) -> Vec<String> {
    ids(db, "SELECT id FROM agent_waits ORDER BY id").await
}

#[tokio::test]
async fn settled_waits_go_a_week_after_they_settle_and_live_ones_never_do() {
    let (_temp, db) = private_db().await;
    seed_wait_owners(&db).await;
    let created = NOW_MS - 40 * DAY_MS;
    let done = Some(r#"{"outcome":"ok"}"#);
    insert_wait(
        &db,
        "resolved-8d",
        "resolved",
        done,
        created,
        Some(NOW_MS - 8 * DAY_MS),
    )
    .await;
    insert_wait(
        &db,
        "cancelled-8d",
        "cancelled",
        None,
        created,
        Some(NOW_MS - 8 * DAY_MS),
    )
    .await;
    insert_wait(
        &db,
        "resolved-6d",
        "resolved",
        done,
        created,
        Some(NOW_MS - 6 * DAY_MS),
    )
    .await;
    insert_wait(&db, "pending-old", "pending", None, created, None).await;
    insert_wait(&db, "resolving-old", "resolving", None, created, None).await;
    // A terminal row with no settle time is aged by creation.
    insert_wait(&db, "unstamped-old", "cancelled", None, created, None).await;
    insert_wait(
        &db,
        "unstamped-new",
        "cancelled",
        None,
        NOW_MS - DAY_MS,
        None,
    )
    .await;

    let report = run(&db, UNBOUNDED, &[Eviction::SettledWaits]).await;

    assert!(report.rules[0].finished, "{report}");
    assert_eq!(report.rows(), 3);
    assert_eq!(
        wait_ids(&db).await,
        vec![
            "pending-old",
            "resolved-6d",
            "resolving-old",
            "unstamped-new"
        ]
    );
}

/// Rows written before `kind` existed carry none, and are aged like any other.
#[tokio::test]
async fn a_settled_wait_without_a_kind_is_evicted_by_age() {
    let (_temp, db) = private_db().await;
    seed_wait_owners(&db).await;
    insert_wait(
        &db,
        "legacy",
        "resolved",
        None,
        1,
        Some(NOW_MS - 8 * DAY_MS),
    )
    .await;
    db.execute("UPDATE agent_waits SET kind = NULL", ())
        .await
        .unwrap();

    run(&db, UNBOUNDED, &[Eviction::SettledWaits]).await;

    assert!(wait_ids(&db).await.is_empty());
}

/// A run-batch wait a permission took over is reclaimed when the permission is
/// withdrawn, so it stays while the permission is pending, however old, and
/// goes by age once the permission is answered.
#[tokio::test]
async fn a_transferred_wait_stays_while_its_permission_is_pending() {
    let (_temp, db) = private_db().await;
    seed_wait_owners(&db).await;
    let settled = Some(NOW_MS - 30 * DAY_MS);
    let transferred = Some(TRANSFERRED_TO_INNER_WAIT);
    insert_wait(&db, "held", "resolved", transferred, 1, settled).await;
    insert_permission(&db, "held", "pending").await;
    insert_wait(&db, "answered", "resolved", transferred, 1, settled).await;
    insert_permission(&db, "answered", "allowed").await;
    insert_wait(&db, "orphaned", "resolved", transferred, 1, settled).await;
    // A pending permission holds only the transferred wait of its own call.
    insert_wait(&db, "other-call", "cancelled", None, 1, settled).await;
    insert_permission(&db, "other-call", "pending").await;

    run(&db, UNBOUNDED, &[Eviction::SettledWaits]).await;
    assert_eq!(wait_ids(&db).await, vec!["held"]);

    db.execute(
        "UPDATE permission_requests SET status = 'allowed' WHERE id = 'held'",
        (),
    )
    .await
    .unwrap();
    run(&db, UNBOUNDED, &[Eviction::SettledWaits]).await;
    assert!(wait_ids(&db).await.is_empty());
}

/// Held rows filling whole pages are stepped over, not re-read forever, and the
/// rows past them still go.
#[tokio::test]
async fn pages_full_of_held_waits_are_stepped_over() {
    let (_temp, db) = private_db().await;
    seed_wait_owners(&db).await;
    let held = 2 * WAIT_PAGE_ROWS + 3;
    db.write(move |conn| {
        Box::pin(async move {
            for index in 0..held + 300 {
                let id = format!("w{index:04}");
                let resolution = (index < held).then_some(TRANSFERRED_TO_INNER_WAIT);
                conn.execute(
                    "INSERT INTO agent_waits (id, job_id, run_id, session_id, predecessor_turn_id,
                         tool_use_id, condition_json, state, resolution_json, created_at, resolved_at)
                     VALUES (?1, 'j', 'r', 's', 't', ?1, '{}', 'resolved', ?2, 1, ?3)",
                    (id.clone(), resolution, NOW_MS - 30 * DAY_MS + index as i64),
                )
                .await?;
                if index < held {
                    conn.execute(
                        "INSERT INTO permission_requests (id, run_id, tool_use_id, tool_name, tool_input, status, created_at)
                         VALUES (?1, 'r', ?1, 'Bash', '{}', 'pending', 1)",
                        (id,),
                    )
                    .await?;
                }
            }
            Ok(())
        })
    })
    .await
    .unwrap();

    let report = run(&db, UNBOUNDED, &[Eviction::SettledWaits]).await;

    let rule = &report.rules[0];
    assert!(rule.finished, "{report}");
    assert_eq!(rule.rows, 300);
    assert!(rule.max_page_rows <= WAIT_PAGE_ROWS, "{rule:?}");
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM agent_waits").await,
        held as i64
    );
}

/// An MCP question belongs to its wait (`prompts.mcp_wait_id` cascades), so it
/// goes with an evicted wait; an ordinary prompt of the same run stays.
#[tokio::test]
async fn an_evicted_waits_mcp_prompts_go_with_it() {
    let (_temp, db) = private_db().await;
    seed_wait_owners(&db).await;
    insert_wait(&db, "old", "resolved", None, 1, Some(NOW_MS - 8 * DAY_MS)).await;
    db.execute_script(
        "INSERT INTO prompts (id, run_id, job_id, questions, created_at, mcp_wait_id)
             VALUES ('bound', 'r', 'j', '[]', 1, 'old');
         INSERT INTO prompts (id, run_id, job_id, questions, created_at)
             VALUES ('ordinary', 'r', 'j', '[]', 1);",
    )
    .await
    .unwrap();

    run(&db, UNBOUNDED, &[Eviction::SettledWaits]).await;

    assert!(wait_ids(&db).await.is_empty());
    assert_eq!(ids(&db, "SELECT id FROM prompts").await, vec!["ordinary"]);
}

/// A wait delete cascades into `prompts` (`mcp_wait_id`). The engine reaches
/// those children through an ordinary index on the key and scans the table for
/// a partial one, which every evicted wait would pay in full.
#[tokio::test]
async fn a_wait_delete_reaches_its_prompts_through_the_index() {
    let (_temp, db) = private_db().await;
    let program = db
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query("EXPLAIN DELETE FROM agent_waits WHERE rowid IN (1, 2)", ())
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
        program.iter().any(|op| op.contains("fk cascade delete")),
        "the delete no longer cascades: {program:#?}"
    );
    assert!(
        !program
            .iter()
            .any(|op| op.starts_with("Rewind") && op.contains("prompts")),
        "the cascade scans prompts: {program:#?}"
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
    assert!(
        report
            .rules
            .iter()
            .all(|rule| rule.finished && rule.error.is_none()),
        "{report}"
    );
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

fn watchdog_eviction() -> Eviction {
    match retention_for("codex_watchdog_lifecycle").unwrap().rule {
        Retention::Evict(rule) => rule,
        other => panic!("watchdog lifecycle must be evicted, got {other:?}"),
    }
}

async fn insert_watchdog(db: &LocalDb, id: &str, kind: &str, created_at: i64) {
    db.execute(
        "INSERT INTO codex_watchdog_lifecycle
            (id, run_id, session_id, provider_turn_id, generation, kind, created_at)
         VALUES (?1, 'r', 's', 'provider-turn', 'generation', ?2, ?3)",
        (id.to_string(), kind.to_string(), created_at),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn watchdog_lifecycle_uses_seven_days_for_noise_and_thirty_for_recovery() {
    let (_temp, db) = private_db().await;
    seed_wait_owners(&db).await;
    let recovery = [
        "expired",
        "heartbeat",
        "worker_panicked",
        "worker_exited_unexpectedly",
        "recovery_claimed",
        "successor_reserved",
        "successor_started",
        "successor_failed",
        "terminalization_succeeded",
        "terminalization_failed",
    ];
    // Append in age order, as the production ledger does.
    for (days, suffix) in [(31, "old"), (30, "boundary"), (8, "kept")] {
        for kind in recovery {
            insert_watchdog(&db, &format!("{kind}-{suffix}"), kind, NOW - days * DAY_S).await;
        }
    }
    for kind in ["phase_transition", "armed", "disarmed"] {
        for (days, suffix) in [(8, "old"), (7, "boundary"), (6, "kept")] {
            insert_watchdog(&db, &format!("{kind}-{suffix}"), kind, NOW - days * DAY_S).await;
        }
    }
    insert_watchdog(&db, "budget-window", "successor_reserved", NOW - 29 * 60).await;

    let report = run(&db, UNBOUNDED, &[watchdog_eviction()]).await;

    assert_eq!(report.end, PassEnd::Complete, "{report}");
    assert!(report.rules[0].finished, "{report}");
    assert_eq!(report.rows(), recovery.len() + 3, "{report}");
    let remaining = ids(&db, "SELECT id FROM codex_watchdog_lifecycle ORDER BY id").await;
    assert_eq!(remaining.len(), 2 * recovery.len() + 7);
    assert!(remaining.iter().all(|id| !id.ends_with("-old")));
    assert!(remaining.contains(&"budget-window".to_string()));
}

#[tokio::test]
async fn watchdog_lifecycle_handles_timestamp_reordering_within_a_page() {
    let (_temp, db) = private_db().await;
    seed_wait_owners(&db).await;
    let cutoff = NOW - 7 * DAY_S;
    insert_watchdog(&db, "new-first", "armed", cutoff + 1).await;
    insert_watchdog(&db, "old-later", "phase_transition", cutoff - 36).await;
    insert_watchdog(&db, "recovery-kept", "expired", cutoff - 36).await;

    let report = run(&db, UNBOUNDED, &[watchdog_eviction()]).await;

    assert_eq!(report.rows(), 1, "{report}");
    assert_eq!(
        ids(&db, "SELECT id FROM codex_watchdog_lifecycle ORDER BY id").await,
        vec!["new-first", "recovery-kept"],
    );
}

#[tokio::test]
async fn watchdog_lifecycle_backlog_is_deleted_in_bounded_pages_and_passes() {
    let (_temp, db) = private_db().await;
    seed_wait_owners(&db).await;
    let kept = PAGE_ROWS + 3;
    let expired = 3 * PAGE_ROWS + 17;
    db.write(move |conn| {
        Box::pin(async move {
            for index in 0..kept + expired {
                conn.execute(
                    "INSERT INTO codex_watchdog_lifecycle
                        (id, run_id, session_id, provider_turn_id, generation, kind, created_at)
                     VALUES (?1, 'r', 's', 'provider-turn', 'generation', ?2, ?3)",
                    (
                        format!("watchdog-{index}"),
                        if index < kept {
                            "expired"
                        } else {
                            "phase_transition"
                        },
                        NOW - 8 * DAY_S,
                    ),
                )
                .await?;
            }
            Ok(())
        })
    })
    .await
    .unwrap();

    let rule = watchdog_eviction();
    let budget = EvictorBudget {
        pass_rows: PAGE_ROWS,
        ..UNBOUNDED
    };
    let mut state = EvictorState::default();
    let mut deleted = 0;
    for pass in 0..4 {
        let report = run_rules(&db, NOW, budget, &mut state, &[rule]).await;
        assert_eq!(
            report.end,
            if pass < 3 {
                PassEnd::RowBudget
            } else {
                PassEnd::Complete
            },
            "{report}"
        );
        assert!(report.rules[0].error.is_none(), "{report}");
        assert!(report.rules[0].max_page_rows <= PAGE_ROWS, "{report}");
        deleted += report.rows();
    }
    assert_eq!(deleted, expired);
    assert_eq!(
        count(&db, "SELECT count(*) FROM codex_watchdog_lifecycle").await,
        kept as i64
    );
}

#[tokio::test]
async fn watchdog_lifecycle_finds_old_rows_behind_a_full_page_of_recent_insertions() {
    let (_temp, db) = private_db().await;
    seed_wait_owners(&db).await;
    db.write(|conn| {
        Box::pin(async move {
            for index in 0..PAGE_ROWS + 1 {
                conn.execute(
                    "INSERT INTO codex_watchdog_lifecycle
                        (id, run_id, session_id, provider_turn_id, generation, kind, created_at)
                     VALUES (?1, 'r', 's', 'provider-turn', 'generation', 'armed', ?2)",
                    (format!("recent-{index}"), NOW),
                )
                .await?;
            }
            Ok(())
        })
    })
    .await
    .unwrap();
    insert_watchdog(&db, "late-routine", "phase_transition", NOW - 8 * DAY_S).await;
    insert_watchdog(&db, "late-recovery", "expired", NOW - 31 * DAY_S).await;

    let report = run(&db, UNBOUNDED, &[watchdog_eviction()]).await;

    assert_eq!(report.end, PassEnd::Complete, "{report}");
    assert_eq!(report.rows(), 2, "{report}");
    assert_eq!(
        count(&db, "SELECT count(*) FROM codex_watchdog_lifecycle").await,
        (PAGE_ROWS + 1) as i64,
    );
}
