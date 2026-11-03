//! The reads that list held executions, measured where they hurt: beside a
//! large `executions` table whose rows each carry a big snapshot.
//!
//! Turso assembles a whole record when it fetches any column of it, so a step
//! that touches an `executions` row from the table reads that row's snapshot
//! (overflow pages included) even when the statement names only `id`. A held
//! read that scans the table, or seeks it by primary key, therefore costs every
//! snapshot it passes (cairn/5894).
//!
//! The tests here span the gate store, the census, the capacity queue and the
//! thread reorder, so they belong to none of those files.

use std::time::{Duration, Instant};

use cairn_db::sql::placeholders;
use cairn_db::turso::{params, Value as SqlValue};
use cairn_workflow::gates::store::{
    BLOCKING_CAPACITY_GATES_SQL, HELD_EXECUTIONS_SQL, WAITING_GATE_PREDICATES_SQL,
};
use serde_json::{json, Value};

use super::capacity::tests::add_thread;
use super::capacity::{
    capacity_predicate, fleet_load, queue_view, FLEET_SLOT_FACT, THREAD_FOCUS_FACT,
};
use super::census::census_for_executions_conn;
use super::reorder::order_thread_queue_conn;
use super::sweep::tests::{env, Env};
use crate::storage::{DrainBudget, RowExt};

/// The live database's average snapshot on a gated execution is about 210 KB.
const SNAPSHOT_BYTES: usize = 150 * 1024;
/// How many seeded executions a cap holds.
const CAP_HELD: usize = 20;
/// The first [`THREAD_CHILDREN`] cap-held executions are children of thread
/// `ta`, held on its focus cap as well as the fleet's.
const THREAD_CHILDREN: usize = 3;
/// Seeded issue numbers start here, clear of the sweep world's own issues.
const FIRST_NUMBER: usize = 1000;

const AUTHORED_GATE: &str =
    r#"{"fact":"check_verdict","check":"rust-tests","issue":"cairn://p/prj/1","verdict":"passed"}"#;

/// A seeded gate: (predicate, satisfied, outcome, queue key).
type SeededGate<'a> = (&'a str, bool, &'a str, Option<i64>);

/// A seeded execution: (id, status, lifecycle, released_at, gates, job status).
type SeededExecution<'a> = (
    String,
    &'a str,
    &'a str,
    Option<i64>,
    Vec<SeededGate<'a>>,
    Option<&'a str>,
);

/// A seeded gate row: (execution, gate seq, predicate, satisfied_at,
/// last_outcome, queue key).
type SeededGateRow<'a> = (
    &'a str,
    i64,
    &'a str,
    Option<i64>,
    Option<&'a str>,
    Option<i64>,
);

fn held_id(k: usize) -> String {
    format!("held-{k:04}")
}

/// `executions` rows, each with its own issue and a [`SNAPSHOT_BYTES`]
/// snapshot. The first [`CAP_HELD`] are cap-held, in key order; the rest mix
/// what a long-lived database accumulates: finished work with a job, released
/// gated work, superseded gated work, and a few still waiting on an authored
/// gate. Returns the cap-held ids in queue order.
async fn seed(env: &Env, executions: usize) -> Vec<String> {
    add_thread(env, "ta", "alpha").await;
    let snapshot = format!("{{\"recipe\":\"{}\"}}", "s".repeat(SNAPSHOT_BYTES));
    let fleet = capacity_predicate(FLEET_SLOT_FACT);
    let focus = capacity_predicate(THREAD_FOCUS_FACT);
    for chunk in (0..executions).collect::<Vec<_>>().chunks(25) {
        let chunk = chunk.to_vec();
        let (snapshot, fleet, focus) = (snapshot.clone(), fleet.clone(), focus.clone());
        env.db
            .write(move |conn| {
                let (chunk, snapshot) = (chunk.clone(), snapshot.clone());
                let (fleet, focus) = (fleet.clone(), focus.clone());
                Box::pin(async move {
                    for k in chunk {
                        let number = (FIRST_NUMBER + k) as i64;
                        let issue = format!("ci{number}");
                        let thread = (k < THREAD_CHILDREN).then_some("ta");
                        conn.execute(
                            "INSERT INTO issues(id, project_id, number, title, status, progress,
                                                attention, parent_thread_id, created_at, updated_at)
                               VALUES(?1, 'p', ?2, ?3, 'active', 'active', 'none', ?4, 1, 1)",
                            params![issue.as_str(), number, format!("Issue {number}"), thread],
                        )
                        .await?;
                        // (id, status, lifecycle, released_at, gates as
                        // (predicate, satisfied, outcome, queue key), job status)
                        let (id, status, lifecycle, released, gates, job): SeededExecution<'_> =
                            if k < CAP_HELD {
                                let key = Some(1_000_000 + k as i64);
                                let mut gates = vec![(fleet.as_str(), false, "no_evidence", key)];
                                if k < THREAD_CHILDREN {
                                    gates.push((focus.as_str(), false, "no_evidence", key));
                                }
                                (held_id(k), "held", "canonical", None, gates, None)
                            } else {
                                let id = format!("past-{k:04}");
                                match k % 4 {
                                    1 => (
                                        id,
                                        "complete",
                                        "canonical",
                                        Some(1),
                                        vec![(AUTHORED_GATE, true, "satisfied", None)],
                                        Some("complete"),
                                    ),
                                    2 => (
                                        id,
                                        "held",
                                        "superseded",
                                        None,
                                        vec![(AUTHORED_GATE, false, "no_evidence", None)],
                                        None,
                                    ),
                                    3 if k % 40 == 3 => (
                                        id,
                                        "held",
                                        "canonical",
                                        None,
                                        vec![(AUTHORED_GATE, false, "no_evidence", None)],
                                        None,
                                    ),
                                    _ => (
                                        id,
                                        "complete",
                                        "canonical",
                                        None,
                                        Vec::new(),
                                        Some("complete"),
                                    ),
                                }
                            };
                        conn.execute(
                            "INSERT INTO executions(id, recipe_id, issue_id, project_id, status,
                                                    started_at, seq, lifecycle, released_at,
                                                    snapshot)
                               VALUES(?1, 'recipe', ?2, 'p', ?3, 1, 1, ?4, ?5, ?6)",
                            params![
                                id.as_str(),
                                issue.as_str(),
                                status,
                                lifecycle,
                                released,
                                snapshot.as_str()
                            ],
                        )
                        .await?;
                        for (seq, (predicate, satisfied, outcome, key)) in gates.iter().enumerate()
                        {
                            conn.execute(
                                "INSERT INTO execution_gates(id, execution_id, seq, predicate_json,
                                                             satisfied_at, last_outcome, created_at,
                                                             queue_key)
                                   VALUES(?1, ?2, ?3, ?4, ?5, ?6, 1, ?7)",
                                params![
                                    format!("{id}-g{seq}"),
                                    id.as_str(),
                                    seq as i64,
                                    *predicate,
                                    satisfied.then_some(1i64),
                                    *outcome,
                                    *key
                                ],
                            )
                            .await?;
                        }
                        if let Some(job) = job {
                            conn.execute(
                                "INSERT INTO jobs(id, execution_id, project_id, issue_id, node_name,
                                                  status, created_at, updated_at)
                                   VALUES(?1, ?2, 'p', ?3, 'builder', ?4, 1, 1)",
                                params![format!("job-{id}"), id.as_str(), issue.as_str(), job],
                            )
                            .await?;
                        }
                    }
                    Ok(())
                })
            })
            .await
            .unwrap();
    }
    // Cairn disables the engine's automatic checkpoint, so until one runs
    // every seeded row is still a version in memory and no read pays for its
    // overflow pages. A live database's rows are on b-tree pages.
    let report = env
        .db
        .checkpoint(
            3,
            Duration::from_millis(50),
            DrainBudget::fixed(Duration::from_millis(50)),
        )
        .await
        .unwrap();
    assert_eq!(report.error, None, "the seeded rows are checkpointed");
    (0..CAP_HELD).map(held_id).collect()
}

/// The wall time of each held read over one seeded database.
#[derive(Debug)]
struct Legs {
    fleet_load: Duration,
    queue_rows: Duration,
    census: Duration,
    queue_view: Duration,
    held_listing: Duration,
    thread_order: Duration,
}

fn ms(duration: Duration) -> String {
    format!("{:.1} ms", duration.as_secs_f64() * 1000.0)
}

/// Seed `executions`, then run and time every held read, asserting what each
/// returns.
async fn measure(name: &str, executions: usize) -> Legs {
    let env = env(name).await;
    let started = Instant::now();
    let queued = seed(&env, executions).await;
    println!(
        "seeded {executions} executions x {} KB in {}",
        SNAPSHOT_BYTES / 1024,
        ms(started.elapsed())
    );

    let started = Instant::now();
    fleet_load(&env.orch).await.unwrap();
    let fleet_load = started.elapsed();

    let started = Instant::now();
    let rows: Vec<String> = env
        .db
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn.query(super::capacity::QUEUE_ROWS_SQL, ()).await?;
                let mut ids = Vec::new();
                while let Some(row) = rows.next().await? {
                    ids.push(row.text(0)?);
                }
                Ok(ids)
            })
        })
        .await
        .unwrap();
    let queue_rows = started.elapsed();
    let mut sorted = rows.clone();
    sorted.sort();
    assert_eq!(
        sorted, queued,
        "the queue rows are exactly the cap-held set"
    );

    let ids = queued.clone();
    let started = Instant::now();
    let censuses = env
        .db
        .read(move |conn| {
            let ids = ids.clone();
            Box::pin(async move { census_for_executions_conn(conn, &ids).await })
        })
        .await
        .unwrap();
    let census = started.elapsed();
    assert_eq!(
        censuses.len(),
        CAP_HELD,
        "every cap-held execution has a census"
    );

    let started = Instant::now();
    let view = queue_view(&env.orch).await.unwrap();
    let queue_view = started.elapsed();
    let listed: Vec<String> = view.held.iter().map(|q| q.execution_id.clone()).collect();
    // Two lanes, neither served: alpha's three children and the shared lane's
    // top-level rest take turns, alpha first by its head's key.
    let (alpha, shared) = queued.split_at(THREAD_CHILDREN);
    let mut rotation = Vec::new();
    for round in 0..shared.len().max(alpha.len()) {
        rotation.extend(alpha.get(round).cloned());
        rotation.extend(shared.get(round).cloned());
    }
    assert_eq!(
        listed, rotation,
        "the queue lists the cap-held set, in release order"
    );

    let started = Instant::now();
    let held = env
        .db
        .read(|conn| {
            Box::pin(
                async move { cairn_workflow::gates::store::list_held_executions_conn(conn).await },
            )
        })
        .await
        .unwrap();
    let held_listing = started.elapsed();
    let awaiting_authored = (CAP_HELD..executions).filter(|k| k % 40 == 3).count();
    assert_eq!(
        held.len(),
        CAP_HELD + awaiting_authored,
        "the sweep lists every live, gated, unreleased execution"
    );

    let order: Vec<Value> = vec![json!(FIRST_NUMBER + 2), json!(FIRST_NUMBER)];
    let started = Instant::now();
    let rotation = env
        .db
        .write(move |conn| {
            let order = order.clone();
            Box::pin(async move {
                order_thread_queue_conn(conn, "ta", "alpha", &order, "cairn://p/prj/ta").await
            })
        })
        .await
        .unwrap()
        .expect("the order names cap-held children of the thread");
    let thread_order = started.elapsed();
    let reordered: Vec<String> = rotation
        .queue
        .iter()
        .map(|queued| queued.execution_id.clone())
        .collect();
    assert_eq!(reordered, vec![held_id(2), held_id(0), held_id(1)]);

    let legs = Legs {
        fleet_load,
        queue_rows,
        census,
        queue_view,
        held_listing,
        thread_order,
    };
    println!(
        "{executions} executions x {} KB: fleet load {}, queue rows {}, census {}, \
         queue_view {}, held listing {}, thread order {}",
        SNAPSHOT_BYTES / 1024,
        ms(legs.fleet_load),
        ms(legs.queue_rows),
        ms(legs.census),
        ms(legs.queue_view),
        ms(legs.held_listing),
        ms(legs.thread_order),
    );
    legs
}

/// The queue, the census, the sweep's listing and a thread reorder all find
/// exactly the cap-held executions among many with large snapshots.
#[tokio::test]
async fn held_reads_find_the_queue_among_large_snapshots() {
    measure("held-reads-small.db", 200).await;
}

/// [`held_reads_find_the_queue_among_large_snapshots`] at the live database's
/// scale, for the timings. Run with `--run-ignored only --no-capture`.
#[tokio::test]
#[ignore = "seeds 300 MB of snapshots; run by hand for the timings"]
async fn held_reads_at_scale() {
    let legs = measure("held-reads-scale.db", 2_000).await;
    // Reading each execution's row, these took 215, 234 and 592 ms on a debug
    // build (11-02, Apple silicon); read from the index, 16, 14 and 56 ms. The
    // bounds sit between, so a regression to row reads fails here.
    for (leg, took) in [
        ("queue rows", legs.queue_rows),
        ("held listing", legs.held_listing),
    ] {
        assert!(took < Duration::from_millis(75), "{leg} took {}", ms(took));
    }
    assert!(
        legs.thread_order < Duration::from_millis(200),
        "ordering a thread queue of three holds the write lock for {}",
        ms(legs.thread_order)
    );
}

// -- plan pins ---------------------------------------------------------------

/// The plan of `sql` with `bound` text parameters, one node per line.
async fn plan(db: &crate::storage::LocalDb, sql: &str, bound: usize) -> Vec<String> {
    let sql = format!("EXPLAIN QUERY PLAN {sql}");
    db.read(move |conn| {
        let sql = sql.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(&sql, vec![SqlValue::Text("x".into()); bound])
                .await?;
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

/// Every in-scope statement, with how many parameters it binds.
fn pinned_statements() -> Vec<(&'static str, String, usize)> {
    vec![
        (
            "QUEUE_ROWS_SQL",
            super::capacity::QUEUE_ROWS_SQL.to_string(),
            0,
        ),
        (
            "CAPACITY_ROWS_SQL",
            super::capacity::CAPACITY_ROWS_SQL.to_string(),
            1,
        ),
        (
            "FLEET_JOBS_SQL",
            super::capacity::FLEET_JOBS_SQL.to_string(),
            1,
        ),
        (
            "RUNNER_DEVICE_SQL",
            super::capacity::RUNNER_DEVICE_SQL.to_string(),
            1,
        ),
        ("HELD_EXECUTIONS_SQL", HELD_EXECUTIONS_SQL.to_string(), 0),
        (
            "WAITING_GATE_PREDICATES_SQL",
            WAITING_GATE_PREDICATES_SQL.to_string(),
            1,
        ),
        (
            "BLOCKING_CAPACITY_GATES_SQL",
            BLOCKING_CAPACITY_GATES_SQL.to_string(),
            0,
        ),
        (
            "census executions",
            super::census::executions_census_sql(3),
            3,
        ),
        ("census issues", super::census::issues_gated_sql(3), 3),
        ("census project", super::census::project_gated_sql(), 1),
        (
            "THREAD_QUEUE_SQL",
            super::reorder::THREAD_QUEUE_SQL.to_string(),
            1,
        ),
        (
            "THREAD_HOLDS_SQL",
            super::reorder::THREAD_HOLDS_SQL.to_string(),
            1,
        ),
        (
            "AWAITS_AUTHORED_SQL",
            super::reorder::AWAITS_AUTHORED_SQL.to_string(),
            1,
        ),
    ]
}

/// Every held read reaches `executions` only through
/// `idx_executions_hold_facts`, by a seek: no plan step scans the table or the
/// index, or reads an execution's row (and so its snapshot) through the
/// primary key or an issue index. The planner picks this index unaided today,
/// as it has no statistics and favours a covering index; this is what notices
/// if it stops.
#[tokio::test]
async fn held_reads_never_read_an_execution_row() {
    let db = crate::storage::migrated_test_db("held-reads-plans.db").await;
    for (label, sql, bound) in pinned_statements() {
        let plan = plan(&db, &sql, bound).await;
        let executions: Vec<&String> = plan
            .iter()
            .filter(|line| {
                ["e", "successor", "executions"].iter().any(|alias| {
                    ["SEARCH ", "SCAN "].iter().any(|step| {
                        line.starts_with(&format!("{step}{alias} "))
                            || line.as_str() == format!("{step}{alias}")
                    })
                })
            })
            .collect();
        assert!(
            !executions.is_empty(),
            "{label}: reads executions: {plan:#?}"
        );
        for line in executions {
            assert!(
                line.starts_with("SEARCH ")
                    && line.contains("USING COVERING INDEX idx_executions_hold_facts (id=?)"),
                "{label}: `{line}` must seek idx_executions_hold_facts by id: {plan:#?}"
            );
        }
        for forbidden in [
            "SCAN executions",
            "sqlite_autoindex_executions_1",
            "idx_executions_issue_seq",
            "AUTOMATIC",
        ] {
            assert!(
                !plan.iter().any(|line| line.contains(forbidden)),
                "{label}: names `{forbidden}`: {plan:#?}"
            );
        }
    }
}

/// The one reorder read that still takes a row: the newest execution's
/// `status`, which no issue-keyed index carries. It walks
/// `idx_executions_issue_seq` down and stops at the first, so it reads one
/// row; a sorter would mean it read every execution on the issue.
#[tokio::test]
async fn the_latest_execution_status_reads_one_row() {
    let db = crate::storage::migrated_test_db("held-reads-latest.db").await;
    let plan = plan(&db, super::reorder::LATEST_EXECUTION_SQL, 1).await;
    assert_eq!(
        plan,
        vec!["SEARCH executions USING INDEX idx_executions_issue_seq (issue_id=?)".to_string()]
    );
}

// -- equivalence -------------------------------------------------------------

/// The queue's predicate as it was before its gate-list source, which a cap
/// holding the execution implies.
macro_rules! reference_cap_held_sql {
    () => {
        concat!(
            cairn_db::awaiting_release_sql!(),
            " AND EXISTS (SELECT 1 FROM execution_gates g WHERE g.execution_id = e.id
                   AND g.satisfied_at IS NULL AND g.last_outcome = 'no_evidence' AND ",
            cairn_db::capacity_gate_sql!(),
            ") AND NOT ",
            cairn_db::awaits_authored_gate_sql!()
        )
    };
}

const REFERENCE_QUEUE_ROWS_SQL: &str = concat!(
    "SELECT e.id, e.issue_id, e.seq, i.number, i.title, p.id, p.key, t.name, ",
    cairn_workflow::held_since_sql!(),
    ", ",
    cairn_workflow::fleet_order_key_sql!(),
    ", i.parent_thread_id, ",
    super::capacity::queue_pin_sql!(),
    "
       FROM executions e
       LEFT JOIN issues i ON i.id = e.issue_id
       LEFT JOIN projects p ON p.id = i.project_id
       LEFT JOIN threads t ON t.id = i.parent_thread_id
      WHERE ",
    reference_cap_held_sql!()
);

const REFERENCE_THREAD_QUEUE_SQL: &str = concat!(
    "SELECT e.id, i.number, i.title, ",
    cairn_workflow::held_since_sql!(),
    ", ",
    cairn_workflow::queue_key_sql!(),
    ", ",
    super::capacity::queue_pin_sql!(),
    ", ",
    cairn_workflow::fleet_order_key_sql!(),
    "
       FROM executions e JOIN issues i ON i.id = e.issue_id
      WHERE i.parent_thread_id = ?1 AND ",
    reference_cap_held_sql!(),
    " ORDER BY 7 DESC, 8, e.id"
);

const REFERENCE_HELD_EXECUTIONS_SQL: &str = concat!(
    "SELECT e.id, e.issue_id, i.number, e.seq, ",
    cairn_workflow::held_since_sql!(),
    ", ",
    cairn_workflow::queue_key_sql!(),
    "
       FROM executions e
       LEFT JOIN issues i ON i.id = e.issue_id
      WHERE ",
    cairn_db::awaiting_release_sql!(),
    " ORDER BY 6, i.number IS NULL, i.number, e.seq, e.id"
);

const REFERENCE_AWAITS_AUTHORED_SQL: &str = concat!(
    "SELECT 1 FROM executions e WHERE e.issue_id = ?1 AND ",
    cairn_db::awaiting_release_sql!(),
    " AND ",
    cairn_db::awaits_authored_gate_sql!(),
    " LIMIT 1"
);

/// A statement whose only change is a forced index: the hint chooses an access
/// path and cannot change a row, so its reference is the same text without it.
fn unhinted(sql: &str) -> String {
    let unhinted = sql.replace(" INDEXED BY idx_executions_hold_facts", "");
    assert_ne!(unhinted, sql, "the statement forces the hold-facts index");
    unhinted
}

fn reference_census_issues_sql(n: usize) -> String {
    unhinted(&super::census::census_rows_sql(&format!(
        "e.issue_id IN ({}) AND e.issue_id IS NOT NULL AND {}",
        placeholders(n),
        cairn_db::gated_unreleased_sql!()
    )))
}

fn reference_census_project_sql() -> String {
    unhinted(&super::census::census_rows_sql(&format!(
        "e.issue_id IN (SELECT id FROM issues
                         WHERE project_id = ?1 AND status NOT IN ('merged', 'closed'))
           AND e.issue_id IS NOT NULL AND {}",
        cairn_db::gated_unreleased_sql!()
    )))
}

const DEVICE: &str = "this-device";

/// Every shape a held read must tell apart, in one database. Each
/// execution's id names its shape.
async fn matrix() -> crate::storage::LocalDb {
    let db = crate::storage::migrated_test_db("held-reads-matrix.db").await;
    let (fleet, focus) = (
        capacity_predicate(FLEET_SLOT_FACT),
        capacity_predicate(THREAD_FOCUS_FACT),
    );
    let (fleet, focus) = (fleet.as_str(), focus.as_str());
    let mut sql = String::from(
        "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
           VALUES('p', 'default', 'P', 'prj', '/tmp/p', 1, 1);
         INSERT INTO threads(id, project_id, name, created_at, updated_at)
           VALUES('tm', 'p', 'matrix', 1, 1);",
    );
    // (issue number, status, resolution column, thread)
    for (number, status, resolved, thread) in [
        (1, "active", None, None),
        (2, "active", None, None),
        (3, "active", None, None),
        (4, "active", None, None),
        (5, "active", None, None),
        (6, "merged", Some("merged_at"), None),
        (7, "closed", Some("closed_at"), None),
        (9, "active", None, None),
        (10, "active", None, Some("tm")),
        (11, "active", None, Some("tm")),
        (12, "active", None, None),
        (13, "active", None, None),
        (14, "active", None, None),
    ] {
        let thread = thread.map_or("NULL".to_string(), |thread| format!("'{thread}'"));
        sql.push_str(&format!(
            "INSERT INTO issues(id, project_id, number, title, status, progress, attention,
                                parent_thread_id, created_at, updated_at)
               VALUES('i{number}', 'p', {number}, 'Issue {number}', '{status}', 'active',
                      'none', {thread}, 1, 1);"
        ));
        if let Some(column) = resolved {
            sql.push_str(&format!(
                "UPDATE issues SET {column} = 5 WHERE id = 'i{number}';"
            ));
        }
    }
    // (id, issue, status, seq, lifecycle, released_at, release override, device)
    for (id, issue, status, seq, lifecycle, released, release_override, device) in [
        (
            "e1-ungated-running",
            Some("i1"),
            "running",
            1,
            "canonical",
            None,
            None,
            None,
        ),
        (
            "e2-authored",
            Some("i2"),
            "held",
            1,
            "canonical",
            None,
            None,
            None,
        ),
        (
            "e3-all-satisfied",
            Some("i3"),
            "held",
            1,
            "canonical",
            None,
            None,
            None,
        ),
        (
            "e4-released",
            Some("i4"),
            "running",
            1,
            "canonical",
            Some(3),
            None,
            None,
        ),
        (
            "e5-superseded",
            Some("i5"),
            "held",
            1,
            "superseded",
            None,
            None,
            None,
        ),
        (
            "e5-successor",
            Some("i5"),
            "running",
            2,
            "canonical",
            None,
            None,
            None,
        ),
        (
            "e6-merged",
            Some("i6"),
            "held",
            1,
            "canonical",
            None,
            None,
            None,
        ),
        (
            "e7-closed",
            Some("i7"),
            "held",
            1,
            "canonical",
            None,
            None,
            None,
        ),
        (
            "e8-no-issue",
            None,
            "held",
            1,
            "canonical",
            None,
            None,
            None,
        ),
        (
            "e9-authored-and-cap",
            Some("i9"),
            "held",
            1,
            "canonical",
            None,
            None,
            None,
        ),
        (
            "e10-two-caps",
            Some("i10"),
            "held",
            1,
            "canonical",
            None,
            None,
            None,
        ),
        (
            "e11-dormant-cap",
            Some("i11"),
            "held",
            1,
            "canonical",
            None,
            None,
            None,
        ),
        (
            "e12-hand-released",
            Some("i12"),
            "running",
            1,
            "canonical",
            Some(4),
            Some(r#"{"principal":"operator","at":4}"#),
            None,
        ),
        (
            "e13-other-device",
            Some("i13"),
            "held",
            1,
            "canonical",
            None,
            None,
            Some("other"),
        ),
        (
            "e14-started",
            Some("i14"),
            "running",
            1,
            "canonical",
            None,
            None,
            Some(DEVICE),
        ),
    ] {
        let text = |value: Option<&str>| value.map_or("NULL".to_string(), |v| format!("'{v}'"));
        let released = released.map_or("NULL".to_string(), |at: i64| at.to_string());
        sql.push_str(&format!(
            "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq,
                                    lifecycle, released_at, release_override_json,
                                    runner_device_id, snapshot)
               VALUES('{id}', 'r', {}, 'p', '{status}', 1, {seq}, '{lifecycle}', {released},
                      {}, {}, '{{}}');",
            text(issue),
            text(release_override),
            text(device),
        ));
    }
    // (execution, gate seq, predicate, satisfied_at, last_outcome, queue key)
    let gates: Vec<SeededGateRow<'_>> = vec![
        (
            "e2-authored",
            0,
            AUTHORED_GATE,
            None,
            Some("no_evidence"),
            None,
        ),
        (
            "e3-all-satisfied",
            0,
            AUTHORED_GATE,
            Some(2),
            Some("satisfied"),
            None,
        ),
        (
            "e4-released",
            0,
            AUTHORED_GATE,
            Some(2),
            Some("satisfied"),
            None,
        ),
        (
            "e5-superseded",
            0,
            fleet,
            None,
            Some("no_evidence"),
            Some(5_000_000),
        ),
        (
            "e6-merged",
            0,
            fleet,
            None,
            Some("no_evidence"),
            Some(6_000_000),
        ),
        ("e7-closed", 0, AUTHORED_GATE, None, None, None),
        (
            "e8-no-issue",
            0,
            fleet,
            None,
            Some("no_evidence"),
            Some(8_000_000),
        ),
        (
            "e9-authored-and-cap",
            0,
            AUTHORED_GATE,
            None,
            Some("no_evidence"),
            None,
        ),
        (
            "e9-authored-and-cap",
            1,
            fleet,
            None,
            Some("no_evidence"),
            Some(9_000_000),
        ),
        (
            "e10-two-caps",
            0,
            fleet,
            None,
            Some("no_evidence"),
            Some(10_000_000),
        ),
        (
            "e10-two-caps",
            1,
            focus,
            None,
            Some("no_evidence"),
            Some(10_000_001),
        ),
        (
            "e11-dormant-cap",
            0,
            focus,
            None,
            Some("satisfied"),
            Some(11_000_000),
        ),
        (
            "e11-dormant-cap",
            1,
            fleet,
            None,
            Some("no_evidence"),
            Some(11_000_001),
        ),
        (
            "e13-other-device",
            0,
            fleet,
            None,
            Some("no_evidence"),
            Some(13_000_000),
        ),
    ];
    for (execution, seq, predicate, satisfied, outcome, key) in gates {
        let number = |value: Option<i64>| value.map_or("NULL".to_string(), |v| v.to_string());
        let outcome = outcome.map_or("NULL".to_string(), |v| format!("'{v}'"));
        sql.push_str(&format!(
            "INSERT INTO execution_gates(id, execution_id, seq, predicate_json, satisfied_at,
                                         last_outcome, created_at, queue_key)
               VALUES('{execution}-g{seq}', '{execution}', {seq}, '{predicate}', {}, {outcome},
                      {}, {});",
            number(satisfied),
            10 + seq,
            number(key),
        ));
    }
    sql.push_str(
        "INSERT INTO execution_supersessions(id, issue_id, superseded_execution_id,
             successor_execution_id, reason, actor_kind, created_at)
           VALUES('s5', 'i5', 'e5-superseded', 'e5-successor', 'successor_created', 'system', 3);
         INSERT INTO jobs(id, execution_id, project_id, issue_id, node_name, status,
                          created_at, updated_at)
           VALUES('j1', 'e1-ungated-running', 'p', 'i1', 'builder', 'running', 1, 1),
                 ('j4', 'e4-released', 'p', 'i4', 'builder', 'complete', 1, 1),
                 ('j12', 'e12-hand-released', 'p', 'i12', 'builder', 'pending', 1, 1),
                 ('j14', 'e14-started', 'p', 'i14', 'builder', 'running', 1, 1),
                 ('j5', 'e5-successor', 'p', 'i5', 'builder', 'running', 1, 1);",
    );
    db.execute_script(&sql).await.unwrap();
    db
}

type Rows = Vec<Vec<String>>;

async fn rows(db: &crate::storage::LocalDb, sql: &str, bound: Vec<SqlValue>) -> Rows {
    let sql = sql.to_string();
    db.read(move |conn| {
        let (sql, bound) = (sql.clone(), bound.clone());
        Box::pin(async move {
            let mut rows = conn.query(&sql, bound).await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                let mut values = Vec::new();
                for index in 0..row.column_count() {
                    values.push(format!("{:?}", row.get_value(index)?));
                }
                out.push(values);
            }
            Ok(out)
        })
    })
    .await
    .unwrap()
}

fn ids(rows: &Rows) -> Vec<String> {
    rows.iter().map(|row| row[0].clone()).collect()
}

/// Each rewritten statement returns exactly what its pre-change text returns
/// over every shape, in its order where it orders, and the shapes are the ones
/// that make the comparison mean something (the queue is not empty, the two
/// caps on one execution list it once).
#[tokio::test]
async fn rewritten_held_reads_return_what_they_returned() {
    let db = matrix().await;
    let text = |value: &str| SqlValue::Text(value.to_string());
    let all_issues: Vec<SqlValue> = (1..=14).map(|n| text(&format!("i{n}"))).collect();
    let all_executions: Vec<SqlValue> = [
        "e1-ungated-running",
        "e2-authored",
        "e3-all-satisfied",
        "e4-released",
        "e5-superseded",
        "e5-successor",
        "e6-merged",
        "e7-closed",
        "e8-no-issue",
        "e9-authored-and-cap",
        "e10-two-caps",
        "e11-dormant-cap",
        "e12-hand-released",
        "e13-other-device",
        "e14-started",
    ]
    .into_iter()
    .map(text)
    .collect();
    let executions_census = super::census::executions_census_sql(all_executions.len());

    // (label, rewritten, reference, bound, ordered)
    let mut cases: Vec<(String, String, String, Vec<SqlValue>, bool)> = vec![
        (
            "queue rows".into(),
            super::capacity::QUEUE_ROWS_SQL.into(),
            REFERENCE_QUEUE_ROWS_SQL.into(),
            vec![],
            false,
        ),
        (
            "thread queue".into(),
            super::reorder::THREAD_QUEUE_SQL.into(),
            REFERENCE_THREAD_QUEUE_SQL.into(),
            vec![text("tm")],
            true,
        ),
        (
            "held listing".into(),
            HELD_EXECUTIONS_SQL.into(),
            REFERENCE_HELD_EXECUTIONS_SQL.into(),
            vec![],
            true,
        ),
        (
            "waiting predicates".into(),
            WAITING_GATE_PREDICATES_SQL.into(),
            unhinted(WAITING_GATE_PREDICATES_SQL),
            vec![SqlValue::Null],
            false,
        ),
        (
            "waiting predicates, excluding".into(),
            WAITING_GATE_PREDICATES_SQL.into(),
            unhinted(WAITING_GATE_PREDICATES_SQL),
            vec![text("e2-authored")],
            false,
        ),
        (
            "blocking capacity gates".into(),
            BLOCKING_CAPACITY_GATES_SQL.into(),
            unhinted(BLOCKING_CAPACITY_GATES_SQL),
            vec![],
            true,
        ),
        (
            "capacity rows".into(),
            super::capacity::CAPACITY_ROWS_SQL.into(),
            unhinted(super::capacity::CAPACITY_ROWS_SQL),
            vec![text(DEVICE)],
            false,
        ),
        (
            "fleet jobs".into(),
            super::capacity::FLEET_JOBS_SQL.into(),
            unhinted(super::capacity::FLEET_JOBS_SQL),
            vec![text(DEVICE)],
            false,
        ),
        (
            "thread holds".into(),
            super::reorder::THREAD_HOLDS_SQL.into(),
            unhinted(super::reorder::THREAD_HOLDS_SQL),
            vec![text("tm")],
            true,
        ),
        (
            "census executions".into(),
            executions_census.clone(),
            unhinted(&executions_census),
            all_executions.clone(),
            true,
        ),
        (
            "census issues".into(),
            super::census::issues_gated_sql(all_issues.len()),
            reference_census_issues_sql(all_issues.len()),
            all_issues.clone(),
            true,
        ),
        (
            "census project".into(),
            super::census::project_gated_sql(),
            reference_census_project_sql(),
            vec![text("p")],
            true,
        ),
    ];
    for issue in &all_issues {
        cases.push((
            format!("awaits authored {issue:?}"),
            super::reorder::AWAITS_AUTHORED_SQL.into(),
            REFERENCE_AWAITS_AUTHORED_SQL.into(),
            vec![issue.clone()],
            true,
        ));
    }
    for execution in &all_executions {
        cases.push((
            format!("runner device {execution:?}"),
            super::capacity::RUNNER_DEVICE_SQL.into(),
            unhinted(super::capacity::RUNNER_DEVICE_SQL),
            vec![execution.clone()],
            true,
        ));
    }

    let mut results = std::collections::HashMap::new();
    for (label, rewritten, reference, bound, ordered) in cases {
        let (mut got, mut want) = (
            rows(&db, &rewritten, bound.clone()).await,
            rows(&db, &reference, bound).await,
        );
        if !ordered {
            got.sort();
            want.sort();
        }
        assert_eq!(
            got, want,
            "{label}: the rewrite returns the reference's rows"
        );
        results.insert(label, got);
    }

    let mut queue = ids(&results["queue rows"]);
    queue.sort();
    assert_eq!(
        queue,
        [
            "\"e10-two-caps\"",
            "\"e11-dormant-cap\"",
            "\"e13-other-device\"",
            "\"e8-no-issue\""
        ]
        .map(|id| format!("Text({id})")),
        "the queue is the cap-held set, the two-cap execution once"
    );
    assert_eq!(
        ids(&results["thread queue"]),
        ["\"e10-two-caps\"", "\"e11-dormant-cap\""].map(|id| format!("Text({id})"))
    );
    let mut held = ids(&results["held listing"]);
    held.sort();
    assert_eq!(
        held,
        [
            "\"e10-two-caps\"",
            "\"e11-dormant-cap\"",
            "\"e13-other-device\"",
            "\"e2-authored\"",
            "\"e3-all-satisfied\"",
            "\"e8-no-issue\"",
            "\"e9-authored-and-cap\"",
        ]
        .map(|id| format!("Text({id})")),
        "the sweep sees every live, gated, unreleased execution, all-satisfied included"
    );
    assert!(
        results["census executions"]
            .iter()
            .any(|row| row[0] == "Text(\"e5-superseded\")" && row[7] == "Integer(2)"),
        "the census names the superseded execution's successor: {:#?}",
        results["census executions"]
    );
    assert!(
        !results["fleet jobs"].is_empty(),
        "the fleet counts a started execution's working job"
    );
}
