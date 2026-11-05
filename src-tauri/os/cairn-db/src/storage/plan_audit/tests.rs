//! The classifier against real bytecode from the pinned engine, the two
//! registries against both lineages, and every trigger body.

use std::collections::{BTreeMap, BTreeSet};

use tempfile::{tempdir, TempDir};

use super::*;
use crate::storage::census::{payload_expr, quote_identifier, refuse_live_directory, Column};
use crate::storage::{LocalDb, MigrationRunner, RowExt, TURSO_MIGRATIONS};

async fn private_db() -> (TempDir, LocalDb) {
    let temp = tempdir().unwrap();
    let db = LocalDb::open_migrated_for_test(temp.path().join("plan-audit.db"))
        .await
        .unwrap();
    (temp, db)
}

async fn team_db() -> (TempDir, LocalDb) {
    let temp = tempdir().unwrap();
    let db = LocalDb::open(temp.path().join("plan-audit-team.db"))
        .await
        .unwrap();
    MigrationRunner::new(TEAM_MIGRATIONS.to_vec())
        .run(&db)
        .await
        .unwrap();
    (temp, db)
}

/// `EXPLAIN` `sql` on `db` and classify it against `registry`.
async fn audit(
    db: &LocalDb,
    sql: &str,
    registry: &'static [WideRowTable],
) -> (Listing, Classified) {
    let sql = sql.to_string();
    db.read(move |conn| {
        Box::pin(async move {
            let schema = SchemaView::load(conn, registry).await?;
            let listing = explain(conn, &sql).await?;
            let classified = classify(&listing, &schema);
            Ok((listing, classified))
        })
    })
    .await
    .unwrap()
}

fn subjects(classified: &Classified) -> Vec<String> {
    classified.findings.iter().map(Finding::subject).collect()
}

/// Run each `(sql, expected subjects)` case and report every mismatch at once,
/// each with its listing, so one run shows the whole picture.
async fn assert_cases(db: &LocalDb, registry: &'static [WideRowTable], cases: &[(&str, &[&str])]) {
    let mut failures = Vec::new();
    for (sql, expected) in cases {
        let (listing, classified) = audit(db, sql, registry).await;
        let got = subjects(&classified);
        if got != *expected {
            failures.push(format!(
                "{sql}\n  expected {expected:?}\n  got      {got:?}\n{}",
                listing.render()
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n\n"));
}

// --- 1. The bytecode parser. ---

fn instr(addr: i64, opcode: &str, p1: i64, comment: &str) -> Instr {
    Instr {
        addr,
        opcode: opcode.to_string(),
        p1,
        p2: 0,
        p3: 0,
        p4: String::new(),
        p5: 0,
        comment: comment.to_string(),
    }
}

#[test]
fn a_listing_splits_wherever_the_address_restarts() {
    let listing = Listing::new([
        instr(0, "Init", 0, "Start at 1"),
        instr(1, "Program", 0, "subprogram=trigger subprogram"),
        instr(2, "Halt", 0, ""),
        instr(0, "Init", 0, "Start at 1"),
        instr(1, "Halt", 0, ""),
        instr(0, "Init", 0, "Start at 1"),
        instr(1, "Halt", 0, ""),
    ]);
    assert_eq!(listing.main().len(), 3);
    assert_eq!(listing.subprograms().len(), 2);
    assert_eq!(listing.instrs().count(), 7);
}

/// A registry naming only the toy table, so the real wide tables are
/// ordinary here and every finding is about the toy.
const TOY: &[WideRowTable] = &[WideRowTable {
    table: "toy_wide",
    blob_columns: &["body"],
    reason: "the classifier's own fixture",
}];

async fn toy_db() -> (TempDir, LocalDb) {
    let (temp, db) = private_db().await;
    db.execute_batch(
        "CREATE TABLE toy_wide (id INTEGER PRIMARY KEY, name TEXT, body BLOB, late TEXT, tag TEXT);
         CREATE INDEX toy_wide_name ON toy_wide(name);
         CREATE UNIQUE INDEX toy_wide_tag ON toy_wide(tag);
         CREATE TABLE toy_log (id INTEGER PRIMARY KEY, n INTEGER);
         CREATE TRIGGER toy_wide_sweeps AFTER UPDATE OF name ON toy_wide
         BEGIN DELETE FROM toy_log WHERE n = 0; END;",
    )
    .await
    .unwrap();
    (temp, db)
}

/// Each way a statement reaches a wide row, and the near misses that must stay
/// clean: the cursor map, the subprogram boundary, an `UPDATE`'s own ephemeral
/// `Rewind`, a point seek on a unique index, and covering index walks.
#[tokio::test]
async fn the_classifier_names_each_way_a_wide_row_is_reached() {
    let (_temp, db) = toy_db().await;
    assert_cases(
        &db,
        TOY,
        &[
            // A full walk flags whatever it reads: every row's first page is paid.
            (
                "SELECT id FROM toy_wide WHERE name IS NULL OR late = ?",
                &["wide-scan on toy_wide"],
            ),
            (
                "SELECT id FROM toy_wide WHERE late = ?",
                &["wide-scan on toy_wide"],
            ),
            // A walk over the index that fetches each row is still a full walk.
            (
                "SELECT late FROM toy_wide ORDER BY name",
                &["wide-scan on toy_wide"],
            ),
            // A covering walk over the index never opens a row.
            ("SELECT name FROM toy_wide ORDER BY name", &[]),
            // `Last` with no step back reads one row.
            ("SELECT max(rowid) FROM toy_wide", &[]),
            // An index range that fetches the row and reads past the blob.
            (
                "SELECT late FROM toy_wide WHERE name = ?",
                &["range-past-blob on toy_wide.late"],
            ),
            // The same range reading only the index, or a column before the blob.
            ("SELECT id FROM toy_wide WHERE name = ?", &[]),
            ("SELECT name FROM toy_wide WHERE name > ?", &[]),
            // A rowid range reading past the blob.
            (
                "SELECT late FROM toy_wide WHERE id > ? AND id < ?",
                &["range-past-blob on toy_wide.late"],
            ),
            // Point reads: rowid, and equality on every column of a unique index.
            ("SELECT body FROM toy_wide WHERE rowid = ?", &[]),
            ("SELECT late FROM toy_wide WHERE id = ?", &[]),
            ("SELECT body, late FROM toy_wide WHERE tag = ?", &[]),
            // An UPDATE walks its own ephemeral scratch table, never the target.
            ("UPDATE toy_wide SET late = ? WHERE id = ?", &[]),
            // A keyed UPDATE rewrites one row, however its key is found.
            ("UPDATE toy_wide SET late = ? WHERE tag = ?", &[]),
            // Firing the trigger inlines its body, which walks toy_log.
            (
                "UPDATE toy_wide SET name = ? WHERE id = ?",
                &["trigger-scan on toy_log"],
            ),
        ],
    )
    .await;
}

// --- 2. The statements 5866, 5894, 5864 and 5876 fixed, before and after. ---

/// `due_wait_ids` before #4666 (cairn/5866): every predicate read from
/// `condition_json`, and `state` stored after it.
const DUE_WAIT_IDS_BEFORE_4666: &str = "SELECT id FROM agent_waits WHERE state='pending' AND created_at<=?1-?2 AND json_extract(condition_json,'$.kind')='mcp_continuation' AND (deadline_ms<=?1 OR json_type(condition_json,'$.state.pending_operation') NOT IN ('null') OR (json_type(condition_json,'$.state.pending_input_requests') NOT IN ('null') AND json_type(condition_json,'$.state.pending_prompt_id') IN ('null')) OR EXISTS (SELECT 1 FROM prompts WHERE prompts.mcp_wait_id=agent_waits.id AND prompts.response IS NOT NULL) OR (json_type(condition_json,'$.state.task') NOT IN ('null') AND COALESCE(json_extract(condition_json,'$.state.next_poll_at_ms'),0)<=?1)) ORDER BY COALESCE(json_extract(condition_json,'$.state.next_poll_at_ms'),deadline_ms,created_at),created_at LIMIT ?3";

/// `due_wait_ids` after #4666: schedule columns and a partial index. Gating
/// clean, with one report-only read: the correlated `EXISTS` takes
/// `pending_prompt_id` from the table row rather than from the covering
/// index, so each pending MCP wait in the range pays its `condition_json`.
/// The range is the pending set, nine rows on the 11-02 database.
const DUE_WAIT_IDS_AFTER_4666: &str = "SELECT id FROM agent_waits WHERE state='pending' AND kind='mcp_continuation' AND created_at<=?1-?2 AND (due_at_ms<=?1 OR (pending_prompt_id IS NOT NULL AND EXISTS (SELECT 1 FROM prompts WHERE prompts.id=agent_waits.pending_prompt_id AND prompts.mcp_wait_id=agent_waits.id AND prompts.response IS NOT NULL))) ORDER BY COALESCE(next_poll_at_ms,deadline_ms,created_at),created_at LIMIT ?3";

/// The one "held" predicate (cairn/5894), as the sweep's listing applies it.
/// Clean since #4689's covering `idx_executions_hold_facts` (0336); a walk of
/// every execution's snapshot without it.
const HELD_EXECUTIONS: &str = concat!(
    "SELECT e.id FROM executions e WHERE ",
    crate::awaiting_release_sql!()
);

/// The reaper's two `pending_launch_run_ids` statements (cairn/5864).
const PENDING_ORDINARY_LAUNCHES: &str = "SELECT p.run_id FROM prepared_ordinary_launches p JOIN runtime_admission_requests r ON r.request_id=p.request_id WHERE (r.state IN ('leased','starting') OR (r.state='queued' AND NOT EXISTS (SELECT 1 FROM team_ordinary_admission_authority a WHERE a.request_id=r.request_id)))";
const PENDING_RUNTIME_CALLS: &str = "SELECT c.run_id FROM prepared_runtime_calls c JOIN runtime_admission_requests r ON r.request_id=c.request_id WHERE r.state IN ('queued','leased','starting')";

/// The job chat-turn count before #4664 (cairn/5876): every event the job's
/// runs wrote, each opened for its `turn_id`.
const CHAT_TURNS_BEFORE_4664: &str = "SELECT COUNT(DISTINCT e.turn_id) FROM events e JOIN runs r ON e.run_id = r.id WHERE r.job_id = ?1 AND e.turn_id IS NOT NULL";

/// The count after #4664: the job's turns, probing `events` by `turn_id`.
const CHAT_TURNS_AFTER_4664: &str = "SELECT COUNT(*) FROM turns WHERE job_id = ?1 AND EXISTS (SELECT 1 FROM events e WHERE e.turn_id = turns.id)";

/// The statements the classifier is held to, with what it must say of each.
/// Their plans on a fresh database are also what the clone is compared to.
const FIXTURES: &[(&str, &[&str])] = &[
    (DUE_WAIT_IDS_BEFORE_4666, &["wide-scan on agent_waits"]),
    (
        DUE_WAIT_IDS_AFTER_4666,
        &["range-past-blob on agent_waits.pending_prompt_id"],
    ),
    (HELD_EXECUTIONS, &[]),
    (
        PENDING_ORDINARY_LAUNCHES,
        &["wide-scan on prepared_ordinary_launches"],
    ),
    (
        PENDING_RUNTIME_CALLS,
        &["wide-scan on runtime_admission_requests"],
    ),
    (
        CHAT_TURNS_BEFORE_4664,
        &["range-past-blob on events.turn_id"],
    ),
    (CHAT_TURNS_AFTER_4664, &[]),
    // Point reads of the widest row in the database.
    ("SELECT snapshot FROM executions WHERE rowid = ?", &[]),
    ("SELECT snapshot FROM executions WHERE id = ?", &[]),
    // A covering index search over the largest table.
    ("SELECT count(*) FROM events WHERE run_id = ?", &[]),
];

/// Red for exactly the statements the 11-02 fixes changed, and clean for what
/// they changed them to. The reaper's statements (5864) are not fixed on this
/// tree, so they are red here; when that lands, its own plan pins say what is
/// clean, and this test's expectations move with it.
#[tokio::test]
async fn the_eleven_two_statements_are_flagged_and_their_fixes_are_not() {
    let (_temp, db) = private_db().await;
    assert_cases(&db, WIDE_ROW_TABLES, FIXTURES).await;

    // The held predicate as it planned before #4689 gave it a covering index.
    db.execute_batch("DROP INDEX idx_executions_hold_facts")
        .await
        .unwrap();
    assert_cases(
        &db,
        WIDE_ROW_TABLES,
        &[(HELD_EXECUTIONS, &["wide-scan on executions"])],
    )
    .await;
}

// --- The measurements the performance thread reads. ---

/// `review_push`'s readiness probe before #4714 (cairn/5643).
const REVIEW_READINESS_BEFORE_4714: &str = "SELECT j.id FROM jobs j WHERE j.issue_id = ?1 AND (EXISTS (SELECT 1 FROM artifacts a WHERE a.job_id = j.id AND (a.artifact_type = 'create-pr' OR (a.artifact_type = 'plan' AND a.confirmed = 0)) AND NOT EXISTS (SELECT 1 FROM artifacts newer WHERE newer.job_id = a.job_id AND newer.output_name IS a.output_name AND newer.version > a.version)) OR EXISTS (SELECT 1 FROM merge_requests mr WHERE mr.issue_id = ?1 AND mr.source_branch = j.branch AND mr.status = 'open')) ORDER BY CASE WHEN EXISTS (SELECT 1 FROM artifacts a WHERE a.job_id = j.id AND a.artifact_type IN ('create-pr','plan')) THEN 0 ELSE 1 END, j.created_at DESC LIMIT 1";

/// The probe's first arm after #4714, naming the index 0335 added for it.
const REVIEW_READINESS_AFTER_4714: &str = "SELECT 1 FROM artifacts a INDEXED BY idx_artifacts_job_review WHERE a.job_id = ?1 AND (a.artifact_type = 'create-pr' OR (a.artifact_type = 'plan' AND a.confirmed = 0))";

/// A job's direct messages before #4726 (cairn/5930).
const JOB_DIRECTS_BEFORE_4726: &str = "SELECT id FROM messages WHERE channel_type = 'direct' AND (recipient_run_id IN (SELECT id FROM runs WHERE job_id = ?1) OR sender_run_id IN (SELECT id FROM runs WHERE job_id = ?1))";

/// One side of it after #4726: from the job's runs into the recipient index.
const JOB_DIRECTS_AFTER_4726: &str = "SELECT m.id FROM runs r JOIN messages m INDEXED BY idx_messages_recipient ON m.recipient_run_id = r.id WHERE r.job_id = ?1 AND m.channel_type = 'direct' AND m.recipient_run_id IS NOT NULL";

/// The semantic lane's turn-excerpt load before #4729 (cairn/5618), with the
/// long turn list that tips the planner onto the type index.
fn turn_events_before_4729(plus: &str) -> String {
    format!(
        "SELECT id, turn_id FROM events WHERE turn_id IN ({}) AND {plus}event_type IN (?, ?, ?)",
        vec!["?"; 100].join(", ")
    )
}

/// `low-cardinality-seek` names 5643's, 5930's and 5618's statements as they
/// planned before their fixes, and stays quiet on what replaced them and on a
/// statement for which the type-first index is the right one.
#[tokio::test]
async fn a_low_cardinality_seek_is_named_before_its_fix_and_not_after() {
    let (_temp, db) = private_db().await;
    let candidates_of = |sql: String| {
        let db = &db;
        async move {
            let (_, classified) = audit(db, &sql, WIDE_ROW_TABLES).await;
            let catalog_sql = sql.clone();
            let found = db
                .read(move |conn| {
                    let (sql, classified) = (catalog_sql.clone(), classified.clone());
                    Box::pin(async move {
                        let catalog = IndexCatalog::load(conn).await?;
                        Ok(low_cardinality_seeks(&classified, &catalog, &sql))
                    })
                })
                .await
                .unwrap();
            found
                .into_iter()
                .map(|c| {
                    format!(
                        "{} via {} binds {} ({})",
                        c.table, c.index, c.column, c.better_index
                    )
                })
                .collect::<Vec<_>>()
        }
    };

    for quiet in [
        REVIEW_READINESS_AFTER_4714.to_string(),
        JOB_DIRECTS_AFTER_4726.to_string(),
        turn_events_before_4729("+"),
        "SELECT id FROM artifacts WHERE artifact_type = ?1".to_string(),
        "SELECT id FROM messages WHERE channel_type = ?1".to_string(),
        // `home_id` leads another index, but the seek already binds it as the
        // sought index's own second column.
        "SELECT id FROM wake_conditions WHERE home_kind = ?1 AND home_id = ?2".to_string(),
    ] {
        assert_eq!(
            candidates_of(quiet.clone()).await,
            Vec::<String>::new(),
            "{quiet}"
        );
    }

    let events = candidates_of(turn_events_before_4729("")).await;
    assert_eq!(
        events,
        ["events via idx_events_type_run binds turn_id (idx_events_turn_id)"],
        "5618"
    );

    // The indexes 0331 and 0335 added are what the two fixes stand on.
    db.execute_batch("DROP INDEX idx_artifacts_job_review; DROP INDEX idx_messages_channel_seq;")
        .await
        .unwrap();
    let artifacts = candidates_of(REVIEW_READINESS_BEFORE_4714.to_string()).await;
    assert!(
        artifacts
            .iter()
            .any(|c| c.starts_with("artifacts via idx_artifacts_type binds job_id (")),
        "5643: {artifacts:?}"
    );
    let messages = candidates_of(JOB_DIRECTS_BEFORE_4726.to_string()).await;
    assert!(
        messages
            .iter()
            .any(|c| c.starts_with("messages via idx_messages_channel_created binds ")),
        "5930: {messages:?}"
    );

    // A bound column deep in the sought index narrows nothing across a gap.
    db.execute_batch(
        "CREATE TABLE toy_seek (id INTEGER PRIMARY KEY, kind TEXT, a TEXT, b TEXT);
         CREATE INDEX idx_toy_seek_kind_a_b ON toy_seek(kind, a, b);
         CREATE INDEX idx_toy_seek_b ON toy_seek(b);",
    )
    .await
    .unwrap();
    let seek = "SELECT id FROM toy_seek INDEXED BY idx_toy_seek_kind_a_b WHERE kind = ?1";
    assert_eq!(
        candidates_of(format!("{seek} AND b = ?2")).await,
        ["toy_seek via idx_toy_seek_kind_a_b binds b (idx_toy_seek_b)"],
    );
    assert_eq!(
        candidates_of(format!("{seek} AND a = ?2 AND b = ?3")).await,
        Vec::<String>::new(),
    );
}

/// A finding names the index it goes through, which is what the runner's shape
/// tags match on: `KNOWN_SHAPES` in `scripts/lib/plan-audit.ts` repeats these.
#[tokio::test]
async fn a_finding_names_the_index_it_goes_through() {
    let (_temp, db) = private_db().await;
    let index_of = |sql: String| {
        let db = &db;
        async move {
            let (listing, classified) = audit(db, &sql, WIDE_ROW_TABLES).await;
            (
                classified
                    .findings
                    .iter()
                    .map(|finding| (finding.table.clone(), finding.index.clone()))
                    .collect::<Vec<_>>(),
                listing.render(),
            )
        }
    };
    let (chat_turns, listing) = index_of(CHAT_TURNS_BEFORE_4664.to_string()).await;
    assert_eq!(
        chat_turns,
        [("events".to_string(), Some("idx_events_run_id".to_string()))],
        "5876:\n{listing}"
    );
    let (type_run, listing) =
        index_of(turn_events_before_4729("").replacen("id, turn_id", "data", 1)).await;
    assert_eq!(
        type_run,
        [(
            "events".to_string(),
            Some("idx_events_type_run".to_string())
        )],
        "5618:\n{listing}"
    );
    let (walk, listing) = index_of(
        "SELECT late_read FROM (SELECT data AS late_read FROM artifacts WHERE artifact_type = ?1)"
            .to_string(),
    )
    .await;
    assert_eq!(
        walk,
        [(
            "artifacts".to_string(),
            Some("idx_artifacts_type".to_string())
        )],
        "5643:\n{listing}"
    );
    let (table_walk, listing) = index_of(PENDING_ORDINARY_LAUNCHES.to_string()).await;
    assert_eq!(
        table_walk,
        [("prepared_ordinary_launches".to_string(), None)],
        "a table cursor has no index:\n{listing}"
    );
}

// --- 3. Every trigger body. ---

/// What fires a trigger.
#[derive(Debug)]
enum Firing {
    Insert,
    Delete,
    /// `UPDATE OF` these columns; empty for a bare `UPDATE`.
    Update(Vec<String>),
}

/// Read the event from `CREATE TRIGGER [IF NOT EXISTS] name [BEFORE|AFTER|
/// INSTEAD OF] event ON table`.
fn firing(sql: &str) -> Firing {
    let words: Vec<String> = sql
        .split_whitespace()
        .map(str::to_ascii_uppercase)
        .collect();
    let mut at = words.iter().position(|word| word == "TRIGGER").unwrap() + 1;
    if words[at] == "IF" {
        at += 3;
    }
    // Past the trigger's own name, which may itself spell an event.
    at += 1;
    let event = words[at..]
        .iter()
        .position(|word| matches!(word.as_str(), "INSERT" | "DELETE" | "UPDATE"))
        .map(|offset| at + offset)
        .unwrap();
    match words[event].as_str() {
        "INSERT" => Firing::Insert,
        "DELETE" => Firing::Delete,
        _ if words.get(event + 1).map(String::as_str) == Some("OF") => {
            let columns = words[event + 2..]
                .iter()
                .take_while(|word| *word != "ON")
                .flat_map(|word| word.split(','))
                .filter(|column| !column.is_empty())
                .map(str::to_ascii_lowercase)
                .collect();
            Firing::Update(columns)
        }
        _ => Firing::Update(Vec::new()),
    }
}

async fn columns_of(db: &LocalDb, table: &str) -> Vec<String> {
    db.query_all(format!("PRAGMA table_info({table})"), (), |row| row.text(1))
        .await
        .unwrap()
}

/// Statements that fire `firing` on `table`: an insert of every column, a
/// keyed delete, or a keyed self-assignment of each `UPDATE OF` column.
fn firing_statements(firing: &Firing, table: &str, columns: &[String]) -> Vec<String> {
    match firing {
        Firing::Insert => vec![format!(
            "INSERT INTO {table} ({}) VALUES ({})",
            columns.join(", "),
            vec!["?"; columns.len()].join(", ")
        )],
        Firing::Delete => vec![format!("DELETE FROM {table} WHERE rowid = ?")],
        Firing::Update(of) => {
            let set: Vec<&String> = if of.is_empty() {
                columns.iter().take(1).collect()
            } else {
                of.iter().collect()
            };
            set.into_iter()
                .map(|column| format!("UPDATE {table} SET {column} = {column} WHERE rowid = ?"))
                .collect()
        }
    }
}

/// `(name, table, sql)` of every trigger `db` holds.
async fn triggers(db: &LocalDb) -> Vec<(String, String, String)> {
    db.query_all(
        "SELECT name, tbl_name, sql FROM sqlite_master WHERE type = 'trigger' ORDER BY name",
        (),
        |row| Ok((row.text(0)?, row.text(1)?, row.text(2)?)),
    )
    .await
    .unwrap()
}

/// One subprogram walk a firing statement produced.
struct Scan {
    trigger: String,
    statement: String,
    finding: Finding,
}

impl Scan {
    fn render(&self) -> String {
        format!(
            "{}: {}\n  {}\n{}",
            self.trigger,
            self.statement,
            self.finding.subject(),
            self.finding.excerpt
        )
    }
}

/// Every `trigger-scan` any trigger's firing statement produces.
async fn trigger_scans(db: &LocalDb, on: Option<&str>) -> Vec<Scan> {
    let mut scans = Vec::new();
    for (name, table, sql) in triggers(db).await {
        if on.is_some_and(|only| only != table) {
            continue;
        }
        let columns = columns_of(db, &table).await;
        for statement in firing_statements(&firing(&sql), &table, &columns) {
            let (listing, classified) = audit(db, &statement, WIDE_ROW_TABLES).await;
            assert!(
                !listing.subprograms().is_empty(),
                "`{statement}` should fire `{name}`, but its listing has no subprogram:\n{}",
                listing.render()
            );
            for finding in classified.findings {
                if finding.rule == Rule::TriggerScan {
                    scans.push(Scan {
                        trigger: name.clone(),
                        statement: statement.clone(),
                        finding,
                    });
                }
            }
        }
    }
    scans
}

/// Every trigger in both lineages, explained through a statement that fires it
/// with its body inlined, walks no table. This covers 5907's class whether or
/// not any test runs the statement that fires the trigger. The foreign-key
/// cascades those statements fire are held to the same rule:
/// `storage::migrations::tests::every_foreign_key_child_column_leads_an_index`
/// keeps their child columns indexed at schema-write time, and this sweep
/// proves the engine seeks them.
#[tokio::test]
async fn every_trigger_body_seeks() {
    let (_private_temp, private) = private_db().await;
    let (_team_temp, team) = team_db().await;
    let mut walks = Vec::new();
    for (lineage, db) in [("private", &private), ("team", &team)] {
        for scan in trigger_scans(db, None).await {
            walks.push(format!("[{lineage}] {}", scan.render()));
        }
    }
    assert!(
        walks.is_empty(),
        "a trigger body or foreign-key action walks a table; key it to the row that fired \
         it (docs/database.md):\n\n{}",
        walks.join("\n\n")
    );
}

/// The zero-count delete 0177 shipped, reinstated, fails the sweep.
#[tokio::test]
async fn a_trigger_body_that_walks_a_table_fails_the_sweep() {
    let (_temp, db) = private_db().await;
    db.execute_batch(
        "CREATE TRIGGER injected_zero_count_delete AFTER UPDATE OF result_ts ON tool_invocations
         BEGIN DELETE FROM tool_analytics_hourly WHERE invocation_count = 0; END;",
    )
    .await
    .unwrap();
    let scans = trigger_scans(&db, Some("tool_invocations")).await;
    assert!(
        scans
            .iter()
            .any(|scan| scan.trigger == "injected_zero_count_delete"
                && scan.finding.table == "tool_analytics_hourly"
                && scan.finding.excerpt.starts_with("-- trigger subprogram")),
        "{}",
        scans
            .iter()
            .map(Scan::render)
            .collect::<Vec<_>>()
            .join("\n\n")
    );
}

#[test]
fn a_trigger_event_is_read_past_a_name_that_spells_one() {
    assert!(matches!(
        firing("CREATE TRIGGER search_events_delete AFTER DELETE ON events BEGIN SELECT 1; END"),
        Firing::Delete
    ));
    assert!(matches!(
        firing("CREATE TRIGGER IF NOT EXISTS update_log BEFORE INSERT ON t BEGIN SELECT 1; END"),
        Firing::Insert
    ));
    match firing("CREATE TRIGGER x BEFORE UPDATE OF is_error, result_ts ON t BEGIN SELECT 1; END") {
        Firing::Update(columns) => assert_eq!(columns, ["is_error", "result_ts"]),
        other => panic!("{other:?}"),
    }
}

// --- 4. The registries against both lineages. ---

/// Every table either lineage creates, with its columns.
async fn both_lineages() -> BTreeMap<String, BTreeSet<String>> {
    let (_private_temp, private) = private_db().await;
    let (_team_temp, team) = team_db().await;
    let mut tables: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for db in [&private, &team] {
        let names = db
            .query_all(
                "SELECT name FROM sqlite_master WHERE type = 'table' \
                 AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__turso%'",
                (),
                |row| row.text(0),
            )
            .await
            .unwrap();
        for name in names {
            let columns = columns_of(db, &name).await;
            tables.entry(name).or_default().extend(columns);
        }
    }
    tables
}

/// Registry entries naming a table or column no lineage has, and tables
/// listed twice.
fn wide_registry_gaps(
    registry: &[WideRowTable],
    tables: &BTreeMap<String, BTreeSet<String>>,
) -> Vec<String> {
    let mut gaps = Vec::new();
    let mut seen = BTreeSet::new();
    for entry in registry {
        if !seen.insert(entry.table) {
            gaps.push(format!("{} is listed twice", entry.table));
        }
        match tables.get(entry.table) {
            None => gaps.push(format!("{} is not a table", entry.table)),
            Some(columns) => gaps.extend(
                entry
                    .blob_columns
                    .iter()
                    .filter(|column| !columns.contains(**column))
                    .map(|column| format!("{}.{column} is not a column", entry.table)),
            ),
        }
    }
    gaps
}

#[tokio::test]
async fn every_wide_row_entry_names_a_real_column() {
    let tables = both_lineages().await;
    assert_eq!(
        wide_registry_gaps(WIDE_ROW_TABLES, &tables),
        Vec::<String>::new(),
        "WIDE_ROW_TABLES (storage/plan_audit/registry.rs) must name real tables and columns"
    );
    let stale = [
        WideRowTable {
            table: "no_such_table",
            blob_columns: &["body"],
            reason: "stale",
        },
        WideRowTable {
            table: "executions",
            blob_columns: &["no_such_column"],
            reason: "stale",
        },
    ];
    assert_eq!(
        wide_registry_gaps(&stale, &tables),
        [
            "no_such_table is not a table",
            "executions.no_such_column is not a column"
        ]
    );
}

#[derive(Debug, Default, PartialEq, Eq)]
struct TriggerAudit {
    /// Triggers a lineage creates that no entry declares.
    undeclared: Vec<String>,
    /// Entries naming a trigger no lineage creates, or the wrong table.
    stale: Vec<String>,
}

fn trigger_audit(
    registry: &[AuditedTrigger],
    triggers: &BTreeSet<(String, String)>,
) -> TriggerAudit {
    let declared: BTreeSet<(String, String)> = registry
        .iter()
        .map(|entry| (entry.trigger.to_string(), entry.table.to_string()))
        .collect();
    TriggerAudit {
        undeclared: triggers
            .difference(&declared)
            .map(|(name, table)| format!("{name} on {table}"))
            .collect(),
        stale: declared
            .difference(triggers)
            .map(|(name, table)| format!("{name} on {table}"))
            .collect(),
    }
}

async fn trigger_set(dbs: &[&LocalDb]) -> BTreeSet<(String, String)> {
    let mut set = BTreeSet::new();
    for db in dbs {
        set.extend(
            triggers(db)
                .await
                .into_iter()
                .map(|(name, table, _)| (name, table)),
        );
    }
    set
}

#[tokio::test]
async fn every_trigger_is_declared_and_every_declaration_exists() {
    let (_private_temp, private) = private_db().await;
    let (_team_temp, team) = team_db().await;
    let triggers = trigger_set(&[&private, &team]).await;
    assert_eq!(
        trigger_audit(AUDITED_TRIGGERS, &triggers),
        TriggerAudit::default(),
        "every trigger either lineage creates needs one AUDITED_TRIGGERS entry \
         (storage/plan_audit/registry.rs) saying why its body seeks, and every entry must exist"
    );
}

#[tokio::test]
async fn an_undeclared_trigger_and_a_stale_declaration_fail_the_audit() {
    let (_temp, db) = private_db().await;
    db.execute_batch(
        "CREATE TRIGGER injected_undeclared AFTER INSERT ON posts BEGIN SELECT 1; END;",
    )
    .await
    .unwrap();
    let mut registry = AUDITED_TRIGGERS.to_vec();
    registry.push(AuditedTrigger {
        trigger: "injected_stale",
        table: "posts",
        reason: "stale",
    });
    let audit = trigger_audit(&registry, &trigger_set(&[&db]).await);
    assert_eq!(audit.undeclared, ["injected_undeclared on posts"]);
    assert!(
        audit.stale.contains(&"injected_stale on posts".to_string()),
        "{audit:?}"
    );
}

// --- 5 and 6. Against a production clone: `#[ignore]`d, and run by hand. ---
//
// `cp -c` the live three-file set into a temp directory, `cp` it at once to a
// volume with room (never leave it on the internal one: a clone diverges at the
// fold rate), delete the temp copy, and run with `CAIRN_PLAN_AUDIT_CLONE` naming
// the copy's `cairn.db`.

const CLONE_ENV: &str = "CAIRN_PLAN_AUDIT_CLONE";

async fn open_clone() -> LocalDb {
    let path = std::path::PathBuf::from(
        std::env::var_os(CLONE_ENV)
            .unwrap_or_else(|| panic!("{CLONE_ENV} must name a clone's cairn.db")),
    );
    refuse_live_directory(&path).unwrap();
    LocalDb::open(&path).await.unwrap()
}

/// A listing with what is this database's own and not its plan's taken out:
/// root pages become the objects at them, and the schema cookie is blanked.
/// Anything else that differs between two databases is a different plan.
fn plan_shape(listing: &Listing, schema: &SchemaView) -> Vec<String> {
    let named = |root: i64| {
        schema
            .object_at_root(root)
            .map_or_else(|| format!("unknown root {root}"), |o| o.name().to_string())
    };
    listing
        .instrs()
        .map(|instr| {
            let (p2, p3, comment) = match instr.opcode.as_str() {
                "OpenRead" | "OpenWrite" => (
                    named(instr.p2),
                    instr.p3.to_string(),
                    instr
                        .comment
                        .replace(&format!("root={}", instr.p2), "root=<object>"),
                ),
                "Transaction" => (
                    instr.p2.to_string(),
                    "<cookie>".to_string(),
                    instr.comment.clone(),
                ),
                _ => (
                    instr.p2.to_string(),
                    instr.p3.to_string(),
                    instr.comment.clone(),
                ),
            };
            format!(
                "{} {} {} {p2} {p3} {} {} {comment}",
                instr.addr, instr.opcode, instr.p1, instr.p4, instr.p5
            )
        })
        .collect()
}

async fn plan_shape_on(db: &LocalDb, sql: &str) -> Vec<String> {
    let sql = sql.to_string();
    db.read(move |conn| {
        Box::pin(async move {
            let schema = SchemaView::load(conn, WIDE_ROW_TABLES).await?;
            Ok(plan_shape(&explain(conn, &sql).await?, &schema))
        })
    })
    .await
    .unwrap()
}

#[test]
fn plan_shape_ignores_root_pages_and_the_schema_cookie_only() {
    let shape_with = |root: i64, cookie: i64, p1: i64| {
        let schema = SchemaView::with_root_for_test(root, "events");
        let open = Instr {
            p2: root,
            ..instr(
                1,
                "OpenRead",
                p1,
                &format!("table=events, root={root}, iDb=0"),
            )
        };
        let transaction = Instr {
            p3: cookie,
            ..instr(2, "Transaction", 0, "iDb=0 tx_mode=Read")
        };
        plan_shape(&Listing::new([open, transaction]), &schema)
    };
    assert_eq!(shape_with(7, 100, 0), shape_with(9, 200, 0));
    assert_ne!(shape_with(7, 100, 0), shape_with(7, 100, 1));
}

/// Whether a fixture on a fresh database plans as it does on production's data.
/// The fast lane's fixtures, and every finding the suite's empty databases
/// report, stand on this: the engine keeps no statistics, so a plan should not
/// depend on what a table holds, only on the schema a database has reached.
///
/// Measured 2026-11-05 at turso 7b7f1595, on a clone taken at
/// 2026-11-05T08:24:23Z while the app ran: every fixture plans alike.
#[tokio::test]
#[ignore = "needs CAIRN_PLAN_AUDIT_CLONE naming a clone of the live database"]
async fn an_empty_database_plans_as_the_clone_does() {
    let clone = open_clone().await;
    MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
        .run(&clone)
        .await
        .unwrap();
    let (_temp, fresh) = private_db().await;
    let mut differences = Vec::new();
    for (sql, _) in FIXTURES {
        let on_clone = plan_shape_on(&clone, sql).await;
        let on_fresh = plan_shape_on(&fresh, sql).await;
        if on_clone != on_fresh {
            differences.push(format!(
                "{sql}\n--- clone\n{}\n--- fresh\n{}",
                on_clone.join("\n"),
                on_fresh.join("\n")
            ));
        }
    }
    println!("{} fixtures compared", FIXTURES.len());
    assert!(differences.is_empty(), "{}", differences.join("\n\n"));
}

/// How many of a table's newest rows the census reads.
const CENSUS_WINDOW: i64 = 2_000;
/// A table with this many rows and this average payload belongs in the list.
const WIDE_MIN_ROWS: i64 = 100;
const WIDE_MIN_AVERAGE_BYTES: i64 = 1_024;

/// One table's newest rows, measured.
#[derive(Debug, Clone, PartialEq, Eq)]
struct TablePayload {
    table: String,
    rows: i64,
    bytes: i64,
}

impl TablePayload {
    fn is_wide(&self) -> bool {
        self.rows >= WIDE_MIN_ROWS && self.bytes / self.rows.max(1) >= WIDE_MIN_AVERAGE_BYTES
    }
}

/// `(tables that measure wide and are not listed, listed tables that do not)`.
fn wide_list_gaps(
    measured: &[TablePayload],
    registry: &[WideRowTable],
) -> (Vec<String>, Vec<String>) {
    let listed = |table: &str| registry.iter().any(|entry| entry.table == table);
    let describe = |t: &TablePayload| {
        format!(
            "{}: {} rows, {} bytes each",
            t.table,
            t.rows,
            t.bytes / t.rows.max(1)
        )
    };
    (
        measured
            .iter()
            .filter(|t| t.is_wide() && !listed(&t.table))
            .map(describe)
            .collect(),
        measured
            .iter()
            .filter(|t| !t.is_wide() && listed(&t.table))
            .map(describe)
            .collect(),
    )
}

#[test]
fn the_wide_list_gaps_name_each_direction() {
    let table = |table: &str, rows, bytes| TablePayload {
        table: table.to_string(),
        rows,
        bytes,
    };
    let measured = [
        table("unlisted_wide", 100, 102_400),
        table("unlisted_narrow", 100, 102_399),
        table("unlisted_few", 99, 1_000_000),
        table("listed_wide", 2_000, 4_000_000),
        table("listed_narrow", 2_000, 2_000),
    ];
    let registry = [
        WideRowTable {
            table: "listed_wide",
            blob_columns: &[],
            reason: "",
        },
        WideRowTable {
            table: "listed_narrow",
            blob_columns: &[],
            reason: "",
        },
    ];
    assert_eq!(
        wide_list_gaps(&measured, &registry),
        (
            vec!["unlisted_wide: 100 rows, 1024 bytes each".to_string()],
            vec!["listed_narrow: 2000 rows, 1 bytes each".to_string()],
        )
    );
}

/// `WIDE_ROW_TABLES` against what production's tables hold: a table that
/// measures wide and is not listed fails. A listed table under the threshold is
/// reported and does not fail, because a table can be listed for its worst rows.
///
/// Measured 2026-11-05 on the same clone as the test above, this fails: 25 tables
/// are over the threshold and not listed. cairn/6149 decides the list and the
/// threshold.
#[tokio::test]
#[ignore = "needs CAIRN_PLAN_AUDIT_CLONE naming a clone of the live database"]
async fn wide_row_list_matches_the_clone() {
    let db = open_clone().await;
    let tables = db
        .query_all(
            "SELECT name FROM sqlite_master WHERE type = 'table' \
             AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__turso%' ORDER BY name",
            (),
            |row| row.text(0),
        )
        .await
        .unwrap();
    let mut measured = Vec::new();
    let mut unmeasured = Vec::new();
    for table in tables {
        let columns = db
            .query_all(
                format!("SELECT name, type FROM pragma_table_info('{table}')"),
                (),
                |row| Ok(Column::new(row.text(0)?, &row.text(1)?)),
            )
            .await
            .unwrap();
        let sql = format!(
            "SELECT count(*), coalesce(sum({}), 0) FROM \
             (SELECT * FROM {} ORDER BY rowid DESC LIMIT {CENSUS_WINDOW})",
            payload_expr(&columns),
            quote_identifier(&table)
        );
        match db
            .query_all(sql, (), |row| Ok((row.i64(0)?, row.i64(1)?)))
            .await
        {
            Ok(rows) => {
                let (rows, bytes) = rows.first().copied().unwrap_or((0, 0));
                measured.push(TablePayload { table, rows, bytes });
            }
            Err(error) => unmeasured.push(format!("{table}: {error}")),
        }
    }
    let (missing, under) = wide_list_gaps(&measured, WIDE_ROW_TABLES);
    println!(
        "{} tables measured over their newest {CENSUS_WINDOW} rows",
        measured.len()
    );
    println!("listed, under the threshold: {under:#?}");
    println!("not measured: {unmeasured:#?}");
    assert!(
        missing.is_empty(),
        "these tables measure wide and are not in WIDE_ROW_TABLES: {missing:#?}"
    );
}

// --- The allowlist production and the runner share. ---

/// Why each entry is not one the list can hold: a rule that does not gate, an
/// unknown issue form, a malformed hash, a file that is not there, or a second
/// entry for the same identity.
fn allowlist_defects(entries: &[AllowEntry], src_tauri: &std::path::Path) -> Vec<String> {
    let mut defects = Vec::new();
    let mut seen = BTreeSet::new();
    for entry in entries {
        let name = format!(
            "{} {} {} {}",
            entry.file, entry.rule, entry.table, entry.statement
        );
        if ![Rule::WideScan, Rule::TriggerScan]
            .iter()
            .any(|rule| rule.as_str() == entry.rule)
        {
            defects.push(format!("{name}: rule is not a gating rule"));
        }
        let issue = entry.issue.strip_prefix("cairn://p/cairn/");
        if !issue.is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit())) {
            defects.push(format!("{name}: issue {} is not canonical", entry.issue));
        }
        if entry.statement.len() != 16
            || !entry
                .statement
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            defects.push(format!("{name}: statement is not 16 lowercase hex"));
        }
        if !src_tauri.join(&entry.file).is_file() {
            defects.push(format!("{name}: no such file under src-tauri/"));
        }
        if entry.reason.trim().is_empty() {
            defects.push(format!("{name}: no reason"));
        }
        if !seen.insert(name.clone()) {
            defects.push(format!("{name}: listed twice"));
        }
    }
    defects
}

fn src_tauri_dir() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// The checked-in file parses as the compiled-in list, and holds only entries
/// that can match a finding.
#[test]
fn the_checked_in_allowlist_is_well_formed() {
    let text = include_str!("../../../plan-audit-allowlist.toml");
    let entries = parse_allowlist(text).expect("plan-audit-allowlist.toml must parse");
    assert_eq!(allowlist(), entries.as_slice());
    assert_eq!(
        allowlist_defects(&entries, &src_tauri_dir()),
        Vec::<String>::new()
    );
}

/// The runner's parser takes flat `key = "value"` lines and no more, because a
/// missing TOML library would cost the lane its verdict. Anything else parses
/// here and not there, so it is refused here too.
#[test]
fn the_checked_in_allowlist_stays_in_the_runners_shape() {
    let text = include_str!("../../../plan-audit-allowlist.toml");
    for (index, line) in text.lines().enumerate() {
        let line = line.trim();
        let flat = line.is_empty()
            || line.starts_with('#')
            || line == "[[allow]]"
            || (line.ends_with('"') && line.split_once(" = \"").is_some());
        assert!(flat, "plan-audit-allowlist.toml:{}: {line}", index + 1);
    }
}

/// Each defect the well-formedness check exists to catch, so its pass means
/// something.
#[test]
fn a_malformed_allowlist_entry_is_named() {
    let entries = parse_allowlist(
        r#"
[[allow]]
file = "os/cairn-db/src/no/such/file.rs"
rule = "range-past-blob"
table = "events"
statement = "NOT-HEX"
kind = "debt"
issue = "CAIRN-1234"
reason = " "
"#,
    )
    .unwrap();
    let defects = allowlist_defects(&entries, &src_tauri_dir());
    for expected in [
        "not a gating rule",
        "not canonical",
        "16 lowercase hex",
        "no such file",
        "no reason",
    ] {
        assert!(
            defects.iter().any(|defect| defect.ends_with(expected)
                || defect.contains(&format!("{expected} "))
                || defect.contains(expected)),
            "{expected}: {defects:?}"
        );
    }
    for (text, what) in [
        ("[[allow]]\nfile = \"x\"\n", "a missing key"),
        (
            "[[allow]]\nfile = \"x\"\nrule = \"wide-scan\"\ntable = \"t\"\nstatement = \"s\"\nkind = \"later\"\nissue = \"i\"\nreason = \"r\"\n",
            "an unknown kind",
        ),
        (
            "[[allow]]\nfile = \"x\"\nrule = \"wide-scan\"\ntable = \"t\"\nstatement = \"s\"\nkind = \"debt\"\nissue = \"i\"\nreason = \"r\"\nline = \"9\"\n",
            "an unknown key",
        ),
    ] {
        assert!(parse_allowlist(text).is_err(), "{what} parsed");
    }
}

/// `Location::file()` is what an entry's `file` is compared to, and nothing else
/// pins its form. A build that changed it would make every entry match nothing.
#[test]
fn a_call_site_reads_relative_to_the_workspace_root() {
    assert!(file!().starts_with("os/cairn-db/"), "{}", file!());
    assert_eq!(std::panic::Location::caller().file(), file!());
}

/// An accepted gating finding logs below WARN, an unaccepted one at WARN, and a
/// report-only one at INFO either way. Identity is every field: a different
/// hash, file, rule or table at the same site does not match.
#[test]
fn only_an_unaccepted_gating_finding_warns() {
    let hash = "0123456789abcdef";
    let entries = parse_allowlist(&format!(
        r#"
[[allow]]
file = "os/cairn-db/src/lib.rs"
rule = "wide-scan"
table = "events"
statement = "{hash}"
kind = "deliberate"
issue = "cairn://p/cairn/5989"
reason = "a bulk scan by design"
"#
    ))
    .unwrap();
    let file = "os/cairn-db/src/lib.rs";
    let accepted = entry_for(&entries, file, Rule::WideScan, "events", hash);
    assert_eq!(accepted.unwrap().kind, AllowKind::Deliberate);
    assert_eq!(
        finding_level(Rule::WideScan, accepted),
        log::Level::Info,
        "an accepted gating finding logs below WARN"
    );
    assert_eq!(finding_level(Rule::WideScan, None), log::Level::Warn);
    assert_eq!(finding_level(Rule::TriggerScan, None), log::Level::Warn);
    assert_eq!(finding_level(Rule::RangePastBlob, None), log::Level::Info);
    for miss in [
        entry_for(&entries, file, Rule::WideScan, "events", "fedcba9876543210"),
        entry_for(&entries, "os/x.rs", Rule::WideScan, "events", hash),
        entry_for(&entries, file, Rule::TriggerScan, "events", hash),
        entry_for(&entries, file, Rule::WideScan, "artifacts", hash),
    ] {
        assert_eq!(miss, None);
    }
}

// --- The hook: first sighting, once, and the opt-outs. ---

/// What this process has reported from `file:line`.
fn reported_at(site: &std::panic::Location<'_>) -> Vec<String> {
    let prefix = format!("{}:{} ", site.file(), site.line());
    REPORTED
        .lock()
        .unwrap()
        .iter()
        .filter(|line| line.starts_with(&prefix))
        .map(|line| line[prefix.len()..].to_string())
        .collect()
}

/// A flagged statement is explained the first time its call site issues it and
/// reported once, however often it runs after.
#[tokio::test]
async fn a_statement_is_explained_once_per_call_site() {
    let (_temp, db) = private_db().await;
    let mut site = None;
    for _ in 0..3 {
        site = Some(std::panic::Location::caller());
        db.query_all(PENDING_ORDINARY_LAUNCHES, (), |row| row.text(0))
            .await
            .unwrap();
    }
    // `Location::caller()` outside a `#[track_caller]` fn names its own line,
    // so the statement's site is the line after it.
    let site = site.unwrap();
    let reported: Vec<String> = REPORTED
        .lock()
        .unwrap()
        .iter()
        .filter(|line| line.starts_with(&format!("{}:{} ", site.file(), site.line() + 1)))
        .cloned()
        .collect();
    assert_eq!(
        reported.len(),
        1,
        "one report for three runs of one statement: {reported:?}"
    );
    assert!(reported[0].ends_with("wide-scan on prepared_ordinary_launches"));
}

/// The same statement through a tracked connection is seen at that call; an
/// exempt call, or one through the raw connection, is not.
#[tokio::test]
async fn a_tracked_statement_is_seen_and_an_exempt_or_raw_one_is_not() {
    let (_temp, db) = private_db().await;
    let tracked = db
        .read(|conn| {
            Box::pin(async move {
                let site = std::panic::Location::caller();
                let mut rows = conn.query(PENDING_RUNTIME_CALLS, ()).await?;
                while rows.next().await?.is_some() {}
                Ok(site)
            })
        })
        .await
        .unwrap();
    let exempt = db
        .read(|conn| {
            Box::pin(async move {
                conn.exempt_from_plan_audit();
                let site = std::panic::Location::caller();
                let mut rows = conn.query(PENDING_RUNTIME_CALLS, ()).await?;
                while rows.next().await?.is_some() {}
                Ok(site)
            })
        })
        .await
        .unwrap();
    let raw = db
        .read(|conn| {
            Box::pin(async move {
                let raw: &crate::turso::Connection = conn;
                let site = std::panic::Location::caller();
                let mut rows = raw.query(PENDING_RUNTIME_CALLS, ()).await?;
                while rows.next().await?.is_some() {}
                Ok(site)
            })
        })
        .await
        .unwrap();
    let next_line = |site: &'static std::panic::Location<'static>| {
        let prefix = format!("{}:{} ", site.file(), site.line() + 1);
        REPORTED
            .lock()
            .unwrap()
            .iter()
            .filter(|line| line.starts_with(&prefix))
            .count()
    };
    assert_eq!(next_line(tracked), 1, "{:?}", REPORTED.lock().unwrap());
    assert_eq!(next_line(exempt), 0, "an exempt call was explained");
    assert_eq!(
        next_line(raw),
        0,
        "a raw-connection statement was explained"
    );
}

/// A single-value helper reached with a tracked connection is attributed to
/// its caller's line, not its own: `#[track_caller]` carries the line through
/// `QueryConn` to `TrackedConnection::query`.
#[tokio::test]
async fn a_helper_statement_is_attributed_to_the_helpers_caller() {
    let (_temp, db) = private_db().await;
    let site = db
        .read(|conn| {
            Box::pin(async move {
                let site = std::panic::Location::caller();
                crate::storage::query_opt_text_conn(conn, PENDING_ORDINARY_LAUNCHES, ()).await?;
                Ok(site)
            })
        })
        .await
        .unwrap();
    let prefix = format!("{}:{} ", site.file(), site.line() + 1);
    let reported = REPORTED.lock().unwrap().clone();
    assert!(
        reported.iter().any(|line| line.starts_with(&prefix)
            && line.ends_with("wide-scan on prepared_ordinary_launches")),
        "expected a report at {prefix}: {reported:?}"
    );
}

#[tokio::test]
async fn a_multi_row_helper_is_audited_at_its_caller() {
    use crate::storage::{QueryConn, QueryRows};

    #[track_caller]
    fn forward<'a, C: QueryConn + ?Sized>(
        conn: &'a C,
        sql: &'a str,
    ) -> impl std::future::Future<Output = crate::storage::DbResult<QueryRows>> + Send + 'a {
        conn.query_rows(sql, ())
    }

    let (_temp, db) = private_db().await;
    let conn = db.connect().await.unwrap();
    let line = line!() + 1;
    let mut rows = forward(&conn, PENDING_ORDINARY_LAUNCHES).await.unwrap();
    while rows.next().await.unwrap().is_some() {}
    let prefix = format!("{}:{line} ", file!());
    assert!(
        REPORTED
            .lock()
            .unwrap()
            .iter()
            .any(|report| report.starts_with(&prefix)
                && report.ends_with("wide-scan on prepared_ordinary_launches")),
        "expected a report at {prefix}"
    );
}

#[tokio::test]
async fn fixed_sql_helpers_name_their_statement_instead_of_a_raw_borrow() {
    let (_temp, db) = private_db().await;
    let conn = db.connect().await.unwrap();
    assert!(crate::lineage::thread_status_conn(&conn, "missing")
        .await
        .unwrap()
        .is_none());
    let held = db.overdue_holders(std::time::Duration::ZERO, std::time::Duration::ZERO);
    assert_eq!(held.len(), 1);
    assert!(held[0].statement.contains("lineage.rs:"), "{held:?}");
    assert!(
        held[0].statement.contains("SELECT status FROM threads"),
        "{held:?}"
    );
    assert!(!held[0].statement.contains("raw connection"), "{held:?}");
}

/// A database that has not reached a lineage head plans its own toy schema,
/// not production's, and is not audited.
#[tokio::test]
async fn a_database_short_of_a_lineage_head_is_not_audited() {
    let temp = tempdir().unwrap();
    let db = LocalDb::open(temp.path().join("toy.db")).await.unwrap();
    db.execute_batch(
        "CREATE TABLE executions (id TEXT PRIMARY KEY, snapshot TEXT, released_at INTEGER)",
    )
    .await
    .unwrap();
    let site = std::panic::Location::caller();
    db.query_all(
        "SELECT id FROM executions WHERE released_at IS NULL",
        (),
        |row| row.text(0),
    )
    .await
    .unwrap();
    let prefix = format!("{}:{} ", site.file(), site.line() + 1);
    assert!(
        !REPORTED
            .lock()
            .unwrap()
            .iter()
            .any(|line| line.starts_with(&prefix)),
        "{:?}",
        reported_at(site)
    );
}

/// The seen-set keys on the call site and the SQL, and stops explaining a
/// call site past eight variants.
#[test]
fn a_call_site_is_explained_for_at_most_eight_variants() {
    let audit = PlanAudit::new();
    let site = std::panic::Location::caller();
    let fresh: Vec<bool> = (0..10)
        .map(|n| {
            audit
                .first_sighting(site, &format!("SELECT {n} FROM jobs"))
                .is_some()
        })
        .collect();
    assert_eq!(fresh.iter().filter(|fresh| **fresh).count(), 8);
    assert!(audit.first_sighting(site, "SELECT 0 FROM jobs").is_none());
    let elsewhere = std::panic::Location::caller();
    assert!(audit
        .first_sighting(elsewhere, "SELECT 0 FROM jobs")
        .is_some());
    for skipped in [
        "BEGIN",
        "COMMIT",
        "PRAGMA foreign_keys = ON",
        "EXPLAIN SELECT 1",
        "CREATE INDEX i ON t(a)",
    ] {
        assert!(
            audit.first_sighting(elsewhere, skipped).is_none(),
            "{skipped}"
        );
    }
}

/// What the audit adds to a statement it has already explained, in
/// nanoseconds: the eligibility load, the prefix test, the hash of the SQL and
/// the set lookup. `cargo test --release -p cairn-db --lib -- --ignored
/// --nocapture first_sighting_cost_probe`. The engine-level
/// `statement_overhead_probe` cannot resolve this on a loaded host, where its
/// own noise is microseconds.
#[test]
#[ignore = "measurement probe; run explicitly in release"]
fn first_sighting_cost_probe() {
    const CALLS: u32 = 2_000_000;
    let audit = PlanAudit::new();
    let site = std::panic::Location::caller();
    let short = "SELECT 1";
    let long = PENDING_ORDINARY_LAUNCHES;
    for sql in [short, long] {
        assert!(audit.first_sighting(site, sql).is_some());
        let mut best = u128::MAX;
        for _round in 0..5 {
            let started = std::time::Instant::now();
            for _ in 0..CALLS {
                std::hint::black_box(audit.first_sighting(site, std::hint::black_box(sql)));
            }
            best = best.min(started.elapsed().as_nanos() / u128::from(CALLS));
        }
        println!(
            "first_sighting_cost_probe: {}-byte statement, seen: {best}ns/call",
            sql.len()
        );
    }
}
