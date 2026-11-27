use super::*;
use crate::db::DbState;
use crate::execution::teardown::{reconcile_resolved_resources, settle_issue_resources};
use crate::orchestrator::Orchestrator;
use crate::services::testing::TestServicesBuilder;
use crate::storage::SearchIndex;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

const FIXTURE: &str = "
    INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
    INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
      VALUES('p','w','P','p','',1,1);
    INSERT INTO issues(id,project_id,number,title,status,created_at,updated_at)
      VALUES('open','p',1,'O','active',1,1),('done','p',2,'D','merged',1,1);
    INSERT INTO jobs(id,project_id,issue_id,parent_job_id,branch,base_branch,status,created_at,updated_at)
      VALUES('open-job','p','open',NULL,'agent/open','main','running',1,1),
            ('done-job','p','done',NULL,'agent/done','main','complete',1,1),
            ('child','p',NULL,'done-job',NULL,NULL,'running',1,1),
            ('dependent','p','open',NULL,'agent/dependent','agent/done','running',1,1);";

type Entry = (String, String, Option<String>, String, Option<i64>);

async fn entries(db: &LocalDb) -> Vec<Entry> {
    db.query_all(
        "SELECT kind,subject_id,run_id,resolution,resolution_generation
         FROM resolution_cleanups ORDER BY kind,subject_id",
        (),
        |row| {
            Ok((
                row.text(0)?,
                row.text(1)?,
                row.opt_text(2)?,
                row.text(3)?,
                row.opt_i64(4)?,
            ))
        },
    )
    .await
    .unwrap()
}

fn issue_entry(issue: &str, resolution: &str, generation: i64) -> Entry {
    (
        "issue".into(),
        issue.into(),
        None,
        resolution.into(),
        Some(generation),
    )
}

fn calls_entry(turn: &str, run: &str, resolution: &str) -> Entry {
    (
        "calls".into(),
        turn.into(),
        Some(run.into()),
        resolution.into(),
        None,
    )
}

async fn fixture(name: &str) -> LocalDb {
    let db = crate::storage::migrated_test_db(name).await;
    db.execute_script(FIXTURE).await.unwrap();
    db
}

fn orchestrator(db: LocalDb, root: &Path) -> Orchestrator {
    let search = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
    Orchestrator::builder(
        Arc::new(DbState::new(Arc::new(db), search)),
        Arc::new(TestServicesBuilder::new().build()),
        root.to_path_buf(),
    )
    .build()
}

async fn record(db: &LocalDb, issue: &str) {
    let issue = issue.to_string();
    db.write(|conn| {
        let issue = issue.clone();
        Box::pin(async move { record_issue(conn, &issue, Resolution::Merged).await })
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn resolve_records_the_issue_and_reopen_retires_it_but_keeps_its_calls() {
    let db = fixture("cleanups-resolve.db").await;
    db.execute_script(
        "INSERT INTO sessions(id,job_id,backend,status,created_at,updated_at)
           VALUES('s','open-job','claude','open',1,1);
         INSERT INTO runs(id,project_id,job_id,status,created_at,updated_at)
           VALUES('r','p','open-job','live',1,1);
         INSERT INTO turns(id,session_id,job_id,run_id,sequence,state,start_reason,created_at,updated_at)
           VALUES('t','s','open-job','r',1,'running','initial',1,1);",
    )
    .await
    .unwrap();
    crate::issues::crud::resolve(
        &db,
        &crate::services::RealClock,
        "open",
        Resolution::Merged,
        "device",
    )
    .await
    .unwrap();
    assert!(entries(&db)
        .await
        .contains(&issue_entry("open", "merged", 0)));
    db.execute(
        "UPDATE turns SET state='cancelled', end_reason='issue_merged' WHERE id='t'",
        (),
    )
    .await
    .unwrap();
    crate::issues::crud::unresolve(&db, &crate::services::RealClock, "open")
        .await
        .unwrap();
    assert_eq!(
        entries(&db).await,
        vec![calls_entry("t", "r", "merged")],
        "reopen retires the issue's entry; its cancelled turn's calls are still owed"
    );
}

#[tokio::test]
async fn an_issue_entry_rolls_back_with_its_transaction() {
    let db = fixture("cleanups-rollback.db").await;
    let failed = db
        .write(|conn| {
            Box::pin(async move {
                record_issue(conn, "done", Resolution::Merged).await?;
                Err::<(), _>(crate::storage::DbError::internal(
                    "the resolution write failed".to_string(),
                ))
            })
        })
        .await;
    assert!(failed.is_err());
    assert!(entries(&db).await.is_empty());
    record(&db, "open").await;
    assert!(
        entries(&db).await.is_empty(),
        "an issue that does not read resolved is owed nothing"
    );
}

#[tokio::test]
async fn only_a_resolution_cancel_records_a_turn_s_calls() {
    let db = fixture("cleanups-trigger.db").await;
    db.execute_script(
        "INSERT INTO sessions(id,job_id,backend,status,created_at,updated_at)
           VALUES('s','open-job','claude','open',1,1);
         INSERT INTO runs(id,project_id,job_id,status,created_at,updated_at)
           VALUES('r','p','open-job','live',1,1);
         INSERT INTO turns(id,session_id,job_id,run_id,sequence,state,start_reason,created_at,updated_at)
           VALUES('user','s','open-job','r',1,'running','initial',1,1),
                 ('closed','s','open-job','r',2,'running','initial',1,1),
                 ('runless','s','open-job',NULL,3,'pending','initial',1,1);
         UPDATE turns SET state='cancelled', end_reason='user_stop' WHERE id='user';
         UPDATE turns SET state='cancelled', end_reason='issue_closed' WHERE id IN ('closed','runless');",
    )
    .await
    .unwrap();
    assert_eq!(
        entries(&db).await,
        vec![calls_entry("closed", "r", "closed")]
    );
    clear_calls(&db, "closed").await.unwrap();
    db.execute_script(
        "UPDATE turns SET end_reason='issue_closed', updated_at=5 WHERE id='closed';",
    )
    .await
    .unwrap();
    assert!(
        entries(&db).await.is_empty(),
        "rewriting a turn already cancelled for the same reason records nothing again"
    );
}

#[tokio::test]
async fn settlement_retires_the_entry_only_when_nothing_is_left() {
    let root = tempfile::tempdir().unwrap();
    let db = fixture("cleanups-settle.db").await;
    db.execute(
        "INSERT INTO runs(id,project_id,job_id,status,created_at,updated_at)
         VALUES('child-run','p','child','live',1,1)",
        (),
    )
    .await
    .unwrap();
    record(&db, "done").await;
    let orch = orchestrator(db, root.path());
    let db = &orch.db.local;
    settle_issue_resources(&orch, db, "done", Resolution::Merged).await;
    assert_eq!(
        entries(db).await,
        vec![issue_entry("done", "merged", 0)],
        "a descendant's agent process is still owed its stop"
    );
    db.execute("UPDATE runs SET status='exited' WHERE id='child-run'", ())
        .await
        .unwrap();
    settle_issue_resources(&orch, db, "done", Resolution::Merged).await;
    assert!(entries(db).await.is_empty());

    record(db, "done").await;
    let earlier = issue_enrolment(db, "done").await.unwrap().unwrap();
    db.execute(
        "UPDATE issues SET resolution_generation=3 WHERE id='done'",
        (),
    )
    .await
    .unwrap();
    record(db, "done").await;
    clear_issue(db, "done", &earlier).await.unwrap();
    assert_eq!(
        entries(db).await,
        vec![issue_entry("done", "merged", 3)],
        "a clear for an earlier enrolment keeps the later one"
    );
}

/// An acquisition that enrols the issue while a settlement is under way must
/// survive that settlement's clear, within one resolution generation. The
/// trigger re-enrols the issue in the middle of the real settlement path,
/// after its enrolment was read and before it clears, as a check request
/// accepted for the resolved owner at that moment would.
#[tokio::test]
async fn an_enrolment_written_during_settlement_survives_it() {
    let root = tempfile::tempdir().unwrap();
    let db = fixture("cleanups-interleave.db").await;
    db.execute_script(
        "INSERT INTO terminal_outputs(id,terminal_id,job_id,project_id,slug,terminal_uri,expiry_basis,capture,created_at,updated_at)
           VALUES('output','gone','done-job','p','watch','cairn://p/p/2/1/builder/terminal/watch','resolution','complete',1,1);
         CREATE TRIGGER enrol_mid_settlement AFTER UPDATE ON terminal_outputs
         BEGIN
           UPDATE resolution_cleanups SET token='enrolled-during-settlement'
            WHERE kind='issue' AND subject_id='done';
         END;",
    )
    .await
    .unwrap();
    record(&db, "done").await;
    let orch = orchestrator(db, root.path());
    let db = &orch.db.local;
    let first = settle_issue_resources(&orch, db, "done", Resolution::Merged).await;
    assert!(
        !first
            .iter()
            .any(|resource| resource.outcome
                == crate::issues::status::SettlementOutcome::CleanupPending),
        "nothing is pending, so only the token can keep the entry: {first:?}"
    );
    assert_eq!(
        issue_enrolment(db, "done").await.unwrap().as_deref(),
        Some("enrolled-during-settlement"),
        "the settlement cleared only the enrolment it started from"
    );
    db.execute_script("DROP TRIGGER enrol_mid_settlement;")
        .await
        .unwrap();
    settle_issue_resources(&orch, db, "done", Resolution::Merged).await;
    assert!(
        entries(db).await.is_empty(),
        "the next settlement, starting from that enrolment, retires it"
    );
}

#[tokio::test]
async fn the_sweep_retires_a_calls_entry_once_its_calls_are_answered() {
    let root = tempfile::tempdir().unwrap();
    let db = fixture("cleanups-calls.db").await;
    db.execute_script(
        "INSERT INTO sessions(id,job_id,backend,status,created_at,updated_at)
           VALUES('s','done-job','claude','closed',1,1);
         INSERT INTO runs(id,project_id,job_id,session_id,status,created_at,updated_at)
           VALUES('r','p','done-job','s','exited',1,1);
         INSERT INTO turns(id,session_id,job_id,run_id,sequence,state,start_reason,created_at,updated_at)
           VALUES('quiet','s','done-job','r',1,'running','initial',1,1),
                 ('open-call','s','done-job','r',2,'running','initial',1,1);",
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO events(id,run_id,turn_id,sequence,timestamp,event_type,data,created_at)
         VALUES('call','r','open-call',1,1,'assistant',?1,1)",
        (serde_json::json!({
            "eventType":"assistant", "sessionId":"s", "isError":false,
            "toolUses":[{"id":"read-1","name":"read","input":{"paths":["file:a"]}}]
        })
        .to_string(),),
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE turns SET state='cancelled', end_reason='issue_merged' WHERE run_id='r'",
        (),
    )
    .await
    .unwrap();
    assert_eq!(entries(&db).await.len(), 2);
    let orch = orchestrator(db, root.path());
    reconcile_resolved_resources(&orch).await;
    let db = &orch.db.local;
    assert!(entries(db).await.is_empty());
    assert!(
        crate::orchestrator::lifecycle::open_call_names(db, "r", "open-call")
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn an_idle_sweep_is_one_read_per_database() {
    let root = tempfile::tempdir().unwrap();
    let orch = orchestrator(fixture("cleanups-idle.db").await, root.path());
    // Scoped to this task: the orchestrator's own background work shares the
    // handle, so a handle-wide counter would measure it too.
    let ((), reads) = orch
        .db
        .local
        .own_reads(reconcile_resolved_resources(&orch))
        .await;
    assert_eq!(reads.statements, 1);
}

#[tokio::test]
async fn an_acquisition_records_only_a_resolved_owner() {
    let db = fixture("cleanups-acquire.db").await;
    assert_eq!(note_acquisition(&db, "open-job").await.unwrap(), 0);
    assert!(entries(&db).await.is_empty());
    assert_eq!(note_acquisition(&db, "child").await.unwrap(), 1);
    assert_eq!(entries(&db).await, vec![issue_entry("done", "merged", 0)]);
    db.execute("DELETE FROM resolution_cleanups", ())
        .await
        .unwrap();
    assert_eq!(
        note_acquisition(&db, "dependent").await.unwrap(),
        1,
        "work cut from a resolved issue's branch settles under that issue"
    );
    db.execute("DELETE FROM resolution_cleanups", ())
        .await
        .unwrap();
    db.write(|conn| Box::pin(async move { note_acquisition_conn(conn, "child").await }))
        .await
        .unwrap();
    assert_eq!(entries(&db).await, vec![issue_entry("done", "merged", 0)]);
}

#[tokio::test]
async fn the_line_600_owner_query_seeks_issues() {
    let db = fixture("cleanups-plan.db").await;
    let plan = db
        .query_all(
            format!(
                "EXPLAIN QUERY PLAN {}",
                crate::execution::teardown::RESOLVED_RESOURCE_ISSUES_SQL
            ),
            ("child".to_string(),),
            |row| row.text(3),
        )
        .await
        .unwrap();
    assert!(
        !plan
            .iter()
            .any(|step| step.contains("SCAN issues") || step.contains("SCAN i")),
        "{plan:#?}"
    );
}

#[tokio::test]
async fn a_turn_s_recorded_calls_seek_its_own_events() {
    let db = fixture("cleanups-calls-plan.db").await;
    let plan = db
        .query_all(
            format!(
                "EXPLAIN QUERY PLAN {}",
                crate::orchestrator::lifecycle::RECORDED_CALLS_SQL
            ),
            ("r".to_string(), "t".to_string()),
            |row| row.text(3),
        )
        .await
        .unwrap();
    assert!(
        plan.iter().any(|step| step.contains("idx_events_turn_id")),
        "{plan:#?}"
    );
}

/// A migration's SQL by name, whether the landing has numbered it yet or not.
fn migration_sql(name: &str) -> String {
    let directory = workspace_crates()
        .parent()
        .unwrap()
        .join("turso_migrations");
    let file = std::fs::read_dir(&directory)
        .unwrap()
        .flatten()
        .map(|entry| entry.path())
        .find(|path| {
            path.file_stem()
                .and_then(|stem| stem.to_str())
                .and_then(|stem| stem.split_once('_'))
                .is_some_and(|(_, rest)| rest == name)
        })
        .unwrap_or_else(|| panic!("no migration named {name}"));
    std::fs::read_to_string(file).unwrap()
}

#[tokio::test]
async fn the_backfill_owes_what_held_evidence_and_cancelled_turns_say() {
    let db = fixture("cleanups-backfill.db").await;
    db.execute_script(
        "INSERT INTO issues(id,project_id,number,title,status,created_at,updated_at)
           VALUES('term','p',3,'T','closed',1,1),('idle','p',4,'I','merged',1,1);
         INSERT INTO jobs(id,project_id,issue_id,branch,base_branch,status,created_at,updated_at)
           VALUES('term-job','p','term','agent/term','main','complete',1,1),
                 ('idle-job','p','idle','agent/idle','main','complete',1,1);
         INSERT INTO sessions(id,job_id,backend,status,created_at,updated_at)
           VALUES('s','done-job','claude','closed',1,1);
         INSERT INTO runs(id,project_id,job_id,status,created_at,updated_at)
           VALUES('child-run','p','child','live',1,1),('old','p','done-job','exited',1,1),
                 ('idle-run','p','idle-job','exited',1,1);
         INSERT INTO turns(id,session_id,job_id,run_id,sequence,state,start_reason,end_reason,created_at,updated_at)
           VALUES('cut','s','done-job','old',1,'cancelled','initial','issue_merged',1,1),
                 ('stopped','s','done-job','old',2,'cancelled','initial','user_stop',1,1);
         INSERT INTO job_terminals(id,job_id,project_id,session_id,command,status,created_at,slug)
           VALUES('watch','term-job','p','sess','sleep 100','running',1,'watch');
         DELETE FROM resolution_cleanups;",
    )
    .await
    .unwrap();
    let shared = migration_sql("resolution_cleanups");
    let backfill = shared
        .split_once("\nEND;")
        .expect("the backfill follows the trigger")
        .1;
    db.execute_script(backfill).await.unwrap();
    db.execute_script(migration_sql("resolution_cleanups_host_local"))
        .await
        .unwrap();
    assert_eq!(
        entries(&db).await,
        vec![
            calls_entry("cut", "old", "merged"),
            issue_entry("done", "merged", 0),
            issue_entry("term", "closed", 0),
        ],
        "a delegated child's live run owes its source issue, a live terminal its own; \
         an issue holding nothing owes nothing"
    );
}

// --- every resource a resolved owner can acquire is noted --------------------

/// Tables settlement reads to find what a resolved owner still holds.
const SETTLED_TABLES: &[&str] = &[
    "runs",
    "job_terminals",
    "terminal_outputs",
    "check_requests",
    "run_batch_detached",
];

/// How a production insert's owner is checked against resolved issues.
enum Noted {
    /// The inserting function calls `resolution_cleanups::note_*` itself.
    Here,
    /// The named function in the same file, its only production caller, does.
    By(&'static str),
    /// The row cannot be a resource of an owner resolved before it, and why.
    Never(&'static str),
}

struct Site {
    file: &'static str,
    function: &'static str,
    noted: Noted,
}

const SITES: &[Site] = &[
    Site {
        file: "cairn-core/src/execution/checks/requests.rs",
        function: "accept_with_context",
        noted: Noted::Here,
    },
    Site {
        file: "cairn-core/src/mcp/handlers/run/detached.rs",
        function: "insert",
        noted: Noted::Here,
    },
    Site {
        file: "cairn-core/src/mcp/handlers/run/detached.rs",
        function: "insert_converted",
        noted: Noted::Here,
    },
    Site {
        file: "cairn-core/src/mcp/handlers/terminal.rs",
        function: "insert_terminal_resource",
        noted: Noted::By("spawn_terminal_session"),
    },
    Site {
        file: "cairn-core/src/terminal_output.rs",
        function: "attach_in",
        noted: Noted::Never(
            "a capture record is attached to a terminal row in that row's own transaction \
             (insert_terminal_resource, which is noted) or to a row re-adopted after a \
             restart, whose environment is noted when its executor re-advertises it",
        ),
    },
    Site {
        file: "cairn-core/src/execution/jobs/persistence.rs",
        function: "insert_run",
        noted: Noted::Here,
    },
    Site {
        file: "cairn-core/src/execution/jobs/persistence.rs",
        function: "insert_child_job_session_run",
        noted: Noted::Here,
    },
    Site {
        file: "cairn-core/src/execution/jobs/calls.rs",
        function: "insert_restart_run_rows",
        noted: Noted::Here,
    },
    Site {
        file: "cairn-core/src/project_services/rows.rs",
        function: "mint_run",
        noted: Noted::Here,
    },
    Site {
        file: "cairn-core/src/execution/jobs/mod.rs",
        function: "hold_start_for_reservation",
        noted: Noted::Never(CRASHED_RUN),
    },
    Site {
        file: "cairn-core/src/execution/jobs/mod.rs",
        function: "mark_job_start_failed",
        noted: Noted::Never(CRASHED_RUN),
    },
    Site {
        file: "cairn-core/src/orchestrator/wakes/provider_limits.rs",
        function: "settle_supply_held_start",
        noted: Noted::Never(CRASHED_RUN),
    },
];

const CRASHED_RUN: &str = "the run is inserted already `crashed`: it records a start that was \
     refused, and no process ever runs under it";

fn workspace_crates() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cairn-core sits in the workspace's crate directory")
        .to_path_buf()
}

fn is_test_path(relative: &Path) -> bool {
    relative.components().any(|component| {
        matches!(
            component.as_os_str().to_str(),
            Some("tests" | "examples" | "benches" | "target")
        )
    }) || relative
        .file_stem()
        .and_then(|stem| stem.to_str())
        .is_some_and(|stem| {
            matches!(stem, "tests" | "testing" | "test_support" | "testsupport")
                || stem.ends_with("_tests")
                || stem.starts_with("test_")
        })
}

fn rust_files(root: &Path, directory: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(directory) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let relative = path.strip_prefix(root).unwrap_or(&path).to_path_buf();
        if path.is_dir() {
            if !matches!(
                entry.file_name().to_str(),
                Some("target" | "node_modules" | ".git")
            ) {
                rust_files(root, &path, out);
            }
        } else if path.extension().is_some_and(|ext| ext == "rs") && !is_test_path(&relative) {
            out.push(path);
        }
    }
}

fn is_test_attribute(line: &str) -> bool {
    let line = line.trim();
    line == "#[cfg(test)]"
        || line.starts_with("#[cfg(any(test")
        || line.starts_with("#[cfg(all(test")
        || line == "#[test]"
        || line.starts_with("#[tokio::test")
}

/// Lexer state carried across lines, so a brace inside a string, a raw string,
/// a comment or a character literal never counts toward an item's extent.
#[derive(Default)]
struct Lexer {
    string: Option<Option<usize>>,
    block_comments: usize,
}

impl Lexer {
    /// The line's `{` minus its `}` outside strings and comments, and whether
    /// it opened any.
    fn brace_delta(&mut self, line: &str) -> (i64, bool) {
        let chars = line.chars().collect::<Vec<_>>();
        let mut delta = 0;
        let mut opened = false;
        let mut at = 0;
        while at < chars.len() {
            let c = chars[at];
            let next = chars.get(at + 1).copied();
            if self.block_comments > 0 {
                if c == '*' && next == Some('/') {
                    self.block_comments -= 1;
                    at += 1;
                } else if c == '/' && next == Some('*') {
                    self.block_comments += 1;
                    at += 1;
                }
            } else if let Some(raw) = self.string {
                match raw {
                    None if c == '\\' => at += 1,
                    None if c == '"' => self.string = None,
                    Some(hashes)
                        if c == '"'
                            && chars[at + 1..].iter().take_while(|h| **h == '#').count()
                                >= hashes =>
                    {
                        self.string = None;
                        at += hashes;
                    }
                    _ => {}
                }
            } else if c == '/' && next == Some('/') {
                break;
            } else if c == '/' && next == Some('*') {
                self.block_comments += 1;
                at += 1;
            } else if c == '"' {
                self.string = Some(None);
            } else if c == 'r'
                && (next == Some('"') || next == Some('#'))
                && !chars[..at]
                    .last()
                    .is_some_and(|p| p.is_alphanumeric() || *p == '_')
            {
                let hashes = chars[at + 1..].iter().take_while(|h| **h == '#').count();
                if chars.get(at + 1 + hashes) == Some(&'"') {
                    self.string = Some(Some(hashes));
                    at += 1 + hashes;
                }
            } else if c == '\'' {
                if next == Some('\\') {
                    at += chars[at + 2..]
                        .iter()
                        .position(|q| *q == '\'')
                        .map_or(0, |end| end + 2);
                } else if chars.get(at + 2) == Some(&'\'') {
                    at += 2;
                }
            } else if c == '{' {
                delta += 1;
                opened = true;
            } else if c == '}' {
                delta -= 1;
            }
            at += 1;
        }
        (delta, opened)
    }
}

/// `source`'s lines with every test-only item blanked (so numbering holds),
/// and the files of the out-of-line test modules it declares.
fn production_lines(source: &str, file: &Path) -> (Vec<String>, Vec<PathBuf>) {
    let mut lines = source.lines().map(str::to_string).collect::<Vec<_>>();
    let mut children = Vec::new();
    let mut index = 0;
    while index < lines.len() {
        if !is_test_attribute(&lines[index]) {
            index += 1;
            continue;
        }
        let mut item = index + 1;
        let mut path_attribute = None;
        while item < lines.len() {
            let text = lines[item].trim();
            if let Some(rest) = text.strip_prefix("#[path = \"") {
                path_attribute = rest.split('"').next().map(str::to_string);
            }
            if !text.is_empty() && !text.starts_with("#[") {
                break;
            }
            item += 1;
        }
        if item >= lines.len() {
            break;
        }
        let text = lines[item].trim().to_string();
        let mut end = item;
        if text.ends_with(';') && !text.contains('{') {
            if let Some(name) = text
                .trim_end_matches(';')
                .split_whitespace()
                .skip_while(|word| *word != "mod")
                .nth(1)
            {
                let parent = file.parent().unwrap();
                let stem = file
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .unwrap_or("");
                let directory = if matches!(stem, "mod" | "lib" | "main") {
                    parent.to_path_buf()
                } else {
                    parent.join(stem)
                };
                match &path_attribute {
                    Some(path) => children.push(parent.join(path)),
                    None => {
                        children.push(directory.join(format!("{name}.rs")));
                        children.push(directory.join(name).join("mod.rs"));
                    }
                }
            }
        } else {
            let mut lexer = Lexer::default();
            let mut depth = 0_i64;
            let mut opened = false;
            for (at, line) in lines.iter().enumerate().skip(item) {
                let (delta, opens) = lexer.brace_delta(line);
                opened |= opens;
                depth += delta;
                end = at;
                if opened && depth <= 0 {
                    break;
                }
            }
        }
        for line in &mut lines[index..=end] {
            line.clear();
        }
        index = end + 1;
    }
    (lines, children)
}

/// A resolved owner has no reconciler to rediscover it: a resource it acquires
/// is found only because the acquiring site recorded the owner as owed a
/// settlement. Every production insert into a table settlement reads must be
/// a listed site that notes its owner, or say why its row cannot need it.
#[test]
fn every_settled_resource_insert_notes_its_owner() {
    let root = workspace_crates();
    let mut files = Vec::new();
    rust_files(&root, &root, &mut files);
    assert!(files.len() > 100, "the scan found the workspace's sources");
    let mut sources = Vec::new();
    let mut test_modules = HashSet::new();
    for file in files {
        let Ok(source) = std::fs::read_to_string(&file) else {
            continue;
        };
        let (lines, children) = production_lines(&source, &file);
        test_modules.extend(children);
        sources.push((file, lines));
    }
    let insert =
        regex::Regex::new(r#"(?i)\bINSERT\s+(?:OR\s+\w+\s+)?INTO\s+"?([a-z_]+)"?[\s(]"#).unwrap();
    let function = regex::Regex::new(r"\bfn\s+([A-Za-z_][A-Za-z0-9_]*)").unwrap();
    let mut used = vec![false; SITES.len()];
    let mut refused = Vec::new();
    for (file, lines) in &sources {
        if test_modules.contains(file) {
            continue;
        }
        let relative = file
            .strip_prefix(&root)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        let mut current = String::new();
        let mut noting = HashSet::new();
        let mut found = Vec::new();
        for (at, line) in lines.iter().enumerate() {
            if line.trim_start().starts_with("//") {
                continue;
            }
            if let Some(name) = function.captures(line) {
                current = name[1].to_string();
            }
            if line.contains("resolution_cleanups::note") {
                noting.insert(current.clone());
            }
            let window = lines[at..(at + 3).min(lines.len())].join(" ");
            for capture in insert.captures_iter(&window) {
                let table = capture[1].to_ascii_lowercase();
                if capture.get(0).unwrap().start() < line.len()
                    && SETTLED_TABLES.contains(&table.as_str())
                {
                    found.push((at + 1, current.clone(), table));
                }
            }
        }
        let by_function: HashMap<&str, usize> = SITES
            .iter()
            .enumerate()
            .filter(|(_, site)| site.file == relative)
            .map(|(index, site)| (site.function, index))
            .collect();
        for (line, function, table) in found {
            let Some(&index) = by_function.get(function.as_str()) else {
                refused.push(format!(
                    "{relative}:{line} fn {function} inserts into {table} and is not a listed site"
                ));
                continue;
            };
            used[index] = true;
            let noter = match SITES[index].noted {
                Noted::Here => Some(function.as_str()),
                Noted::By(caller) => Some(caller),
                Noted::Never(reason) => {
                    if reason.trim().is_empty() {
                        refused.push(format!(
                            "{relative} fn {function} is exempt without a written reason"
                        ));
                    }
                    None
                }
            };
            if let Some(noter) = noter {
                if !noting.contains(noter) {
                    refused.push(format!(
                        "{relative}:{line} fn {function} inserts into {table}, but fn {noter} \
                         never calls resolution_cleanups::note_*"
                    ));
                }
            }
        }
    }
    for (site, used) in SITES.iter().zip(used) {
        if !used {
            refused.push(format!(
                "{} fn {} is listed but inserts into no settled table any more; delete its entry",
                site.file, site.function
            ));
        }
    }
    assert!(
        refused.is_empty(),
        "A production insert into a table settlement reads must note its owner against \
         resolved issues (execution::resolution_cleanups::note_acquisition*), or be listed \
         in SITES with the reason its row can never need it:\n{}",
        refused.join("\n")
    );
}
