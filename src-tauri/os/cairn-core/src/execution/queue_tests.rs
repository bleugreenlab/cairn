//! The run queue's authority, as cairn/7646 states its acceptance: an active
//! entry holds its seat wherever it is and is never cut; a dormant entry holds
//! none and keeps its place; work that wants a seat takes the next free one
//! by its place; a launch with no seat is held, not started; park removes an
//! entry and unpark restores it; the same stored list and facts always decide
//! the same seats.

use std::collections::{HashMap, HashSet};

use cairn_workflow::queue::QueueCaps;

use super::*;

const CAPS: QueueCaps = QueueCaps {
    global: 2,
    per_thread: 1,
};

fn fact(issue: &str, thread: Option<&str>, joinable: bool, since: i64) -> ExecutionFact {
    ExecutionFact {
        issue_id: issue.into(),
        thread_id: thread.map(str::to_owned),
        joinable,
        since,
        activity: Activity::Wanting,
    }
}

/// Executions `<id>` on issues `issue-<id>`, all live, in the given threads
/// and doing the given things.
fn facts(spec: &[(&str, Option<&str>, Activity)]) -> HashMap<String, ExecutionFact> {
    spec.iter()
        .enumerate()
        .map(|(at, (id, thread, activity))| {
            let mut fact = fact(&format!("issue-{id}"), *thread, true, at as i64);
            fact.activity = *activity;
            ((*id).to_string(), fact)
        })
        .collect()
}

fn known(facts: &HashMap<String, ExecutionFact>) -> HashSet<String> {
    facts.keys().cloned().collect()
}

fn joined(facts: &HashMap<String, ExecutionFact>) -> Vec<StoredRow> {
    plan_membership(Vec::new(), facts, &known(facts)).rows
}

fn seated(rows: &[StoredRow], facts: &HashMap<String, ExecutionFact>) -> Vec<String> {
    seated_asking(rows, facts, None)
}

fn seated_asking(
    rows: &[StoredRow],
    facts: &HashMap<String, ExecutionFact>,
    asking: Option<&str>,
) -> Vec<String> {
    decide(rows, facts, CAPS, asking)
        .items
        .into_iter()
        .filter(|item| item.runs)
        .map(|item| item.execution_id)
        .collect()
}

fn order(rows: &[StoredRow]) -> Vec<&str> {
    rows.iter()
        .filter(|row| !row.parked)
        .map(|row| row.execution_id.as_str())
        .collect()
}

use Activity::{Active, Dormant, Wanting};

#[test]
fn dormant_entries_ahead_hold_no_seat_and_held_work_behind_them_runs() {
    // Tonight's shape: old entries waiting on a review, a person or a
    // finished check fill the top of the list; work that is due sits below.
    let facts = facts(&[
        ("july-review", None, Dormant),
        ("july-check", None, Dormant),
        ("question", Some("R"), Dormant),
        ("cairn-a", Some("C"), Wanting),
        ("cairn-b", None, Wanting),
        ("cairn-c", None, Wanting),
    ]);
    let rows = joined(&facts);
    assert_eq!(seated(&rows, &facts), ["cairn-a", "cairn-b"]);
    let state = decide(&rows, &facts, CAPS, None);
    assert_eq!(state.item("july-review").unwrap().position, Some(1));
    assert_eq!(
        state.item("cairn-c").unwrap().queued_label(),
        "Queued · 1st in line"
    );
}

#[test]
fn an_entry_waiting_on_its_running_check_keeps_its_seat() {
    let mut facts = facts(&[
        ("checking", None, Active),
        ("other", None, Active),
        ("due", None, Wanting),
    ]);
    let rows = joined(&facts);
    assert_eq!(seated(&rows, &facts), ["checking", "other"]);
    // Its verdict lands and wakes it: its launch asks while it still reads
    // active (the attempt is live until the woken turn exists), so it is
    // seated and never held behind `due`.
    assert_eq!(
        seated_asking(&rows, &facts, Some("checking")),
        ["checking", "other"]
    );
    // Once it hands off and nothing it waits on moves, its seat is freed.
    facts.get_mut("checking").unwrap().activity = Dormant;
    assert_eq!(seated(&rows, &facts), ["other", "due"]);
}

#[test]
fn a_woken_dormant_entry_takes_the_next_free_seat_by_its_place() {
    let facts = facts(&[
        ("old", None, Dormant),
        ("running", None, Active),
        ("filed-later", None, Wanting),
    ]);
    let rows = joined(&facts);
    assert_eq!(seated(&rows, &facts), ["running", "filed-later"]);
    // A person replies to `old`: its launch asks for a seat. Its resumed work
    // outranks the later filing, which has not started, so it takes the seat.
    let asked = decide(&rows, &facts, CAPS, Some("old"));
    assert!(asked.item("old").unwrap().runs);
    assert_eq!(asked.item("filed-later").unwrap().in_line, Some(1));
    // Once the later filing is running, nothing running is cut for `old`: it
    // is held as first in line for the next free seat.
    let mut started = facts.clone();
    started.get_mut("filed-later").unwrap().activity = Active;
    let held = decide(&rows, &started, CAPS, Some("old"));
    let old = held.item("old").unwrap();
    assert!(!old.runs);
    assert_eq!(old.queued_label(), "Queued · 1st in line");
}

#[test]
fn a_move_to_the_top_waits_for_a_free_seat_and_cuts_nothing() {
    let facts = facts(&[
        ("a1", Some("A"), Active),
        ("b1", Some("B"), Active),
        ("c1", Some("C"), Wanting),
    ]);
    let mut rows = joined(&facts);
    assert!(plan_move(&mut rows, "c1", 1));
    assert_eq!(order(&rows), ["c1", "a1", "b1"]);
    assert_eq!(seated(&rows, &facts), ["a1", "b1"]);
    assert_eq!(
        decide(&rows, &facts, CAPS, None)
            .item("c1")
            .unwrap()
            .in_line,
        Some(1)
    );
}

#[test]
fn park_removes_an_entry_and_unpark_returns_it_to_the_end() {
    let facts = facts(&[
        ("a1", Some("A"), Wanting),
        ("b1", Some("B"), Wanting),
        ("c1", Some("C"), Wanting),
    ]);
    let mut rows = joined(&facts);
    assert!(plan_park(&mut rows, "a1", true));
    assert_eq!(order(&rows), ["b1", "c1"]);
    assert_eq!(seated(&rows, &facts), ["b1", "c1"]);
    let state = decide(&rows, &facts, CAPS, Some("a1"));
    let a1 = state.item("a1").unwrap();
    assert!(a1.parked && a1.position.is_none() && !a1.runs);
    // A parked execution is never added back by membership.
    let again = plan_membership(rows.clone(), &facts, &known(&facts));
    assert_eq!(order(&again.rows), ["b1", "c1"]);
    assert!(
        !plan_move(&mut rows, "a1", 1),
        "a parked entry has no place to move"
    );
    assert!(plan_park(&mut rows, "a1", false));
    assert_eq!(
        order(&rows),
        ["b1", "c1", "a1"],
        "unpark returns it to the end"
    );
}

#[test]
fn membership_follows_liveness_and_an_unknown_row_keeps_its_place() {
    let mut facts = facts(&[("a1", Some("A"), Wanting), ("b1", Some("B"), Wanting)]);
    let rows = joined(&facts);
    // b1's issue merges: its database knows it and no longer reads it live.
    facts.remove("b1");
    let mut known_ids = known(&facts);
    known_ids.insert("b1".into());
    let left = plan_membership(rows.clone(), &facts, &known_ids);
    assert_eq!(left.dropped, ["b1"]);
    assert_eq!(order(&left.rows), ["a1"]);
    // A team database that is offline knows nothing: the row stays, but is
    // never seated.
    let offline = plan_membership(rows, &facts, &known(&facts));
    assert!(offline.dropped.is_empty());
    assert_eq!(order(&offline.rows), ["a1", "b1"]);
    assert_eq!(seated_asking(&offline.rows, &facts, Some("b1")), ["a1"]);
}

#[test]
fn joiners_append_in_order_and_a_rerun_takes_its_issues_place() {
    let mut facts = facts(&[("a1", Some("A"), Wanting), ("b1", Some("B"), Wanting)]);
    let rows = joined(&facts);
    facts.insert("z".into(), fact("issue-z", None, true, 100));
    // A rerun of a1's issue files a new execution.
    facts.insert("a1-rerun".into(), fact("issue-a1", Some("A"), true, 50));
    let grown = plan_membership(rows, &facts, &known(&facts));
    assert_eq!(order(&grown.rows), ["a1", "a1-rerun", "b1", "z"]);
    // A held (`after:`-gated) execution is not joinable and does not join.
    facts.insert("held".into(), fact("issue-held", None, false, 0));
    let held = plan_membership(grown.rows, &facts, &known(&facts));
    assert!(!order(&held.rows).contains(&"held"));
}

#[test]
fn the_stored_list_and_facts_alone_reproduce_the_seats() {
    let facts = facts(&[
        ("a1", Some("A"), Wanting),
        ("a2", Some("A"), Active),
        ("b1", Some("B"), Dormant),
        ("u", None, Wanting),
    ]);
    let mut rows = joined(&facts);
    assert!(plan_move(&mut rows, "u", 1));
    let before = seated(&rows, &facts);
    // A restart reads back exactly what was stored: same rows, same ranks.
    let reread = plan_membership(rows.clone(), &facts, &known(&facts));
    assert!(!reread.changed, "a reread writes nothing");
    assert_eq!(seated(&reread.rows, &facts), before);
    assert_eq!(before, ["u", "a2"]);
}

#[test]
fn a_move_clamps_to_the_list() {
    let facts = facts(&[
        ("a", None, Wanting),
        ("b", None, Wanting),
        ("c", None, Wanting),
    ]);
    let mut rows = joined(&facts);
    assert!(plan_move(&mut rows, "a", 99));
    assert_eq!(order(&rows), ["b", "c", "a"]);
    assert!(plan_move(&mut rows, "a", 0));
    assert_eq!(order(&rows), ["a", "b", "c"]);
    assert!(!plan_move(&mut rows, "missing", 1));
}

/// The rule the queue derived on every read until cairn/7675 stored its
/// facts, kept here, in tests only, as the oracle every stored value is
/// checked against. Production has no copy: `cairn_db::stored_status::jobs`
/// is the one definition.
const ORACLE_ACTIVITY_SQL: &str = "CASE WHEN EXISTS (
        SELECT 1 FROM jobs aj LEFT JOIN turns ht ON ht.id = aj.current_turn_id
         WHERE aj.execution_id = e.id AND aj.status IN ('running', 'pending') AND (
               ht.state IN ('pending', 'running')
            OR (ht.state = 'complete' AND ht.start_reason = 'dependency_unblock'
                AND ht.run_id IS NULL AND ht.end_reason IS NULL)
            OR (aj.status = 'running' AND aj.current_turn_id IS NULL)
            OR EXISTS (SELECT 1 FROM agent_waits aw WHERE aw.job_id = aj.id
                         AND (aw.state = 'resolving'
                              OR (aw.state = 'pending'
                                  AND COALESCE(aw.kind, json_extract(aw.condition_json, '$.kind'))
                                      NOT IN ('operator_request'))))
            OR EXISTS (SELECT 1 FROM provider_limit_holds ah WHERE ah.job_id = aj.id
                         AND ah.state = 'active' AND ah.boundary_source <> 'queue'
                         AND ah.retry_not_before IS NOT NULL AND ah.resume_gate IS NULL)))
      OR EXISTS (
        SELECT 1 FROM jobs cj
         WHERE cj.execution_id = e.id AND (
               EXISTS (SELECT 1 FROM turn_end_check_attempts ta WHERE ta.id = cj.turn_end_check_attempt_id
                         AND ta.state IN ('requested', 'claimed', 'runtime_started', 'deferred', 'submitted'))
            OR EXISTS (SELECT 1 FROM check_requests cr WHERE cr.requester_job_id = cj.id
                         AND cr.state IN ('accepted', 'queued', 'granted', 'running'))))
     THEN 'active'
     WHEN EXISTS (
        SELECT 1 FROM jobs wj JOIN provider_limit_holds wh ON wh.job_id = wj.id
         WHERE wj.execution_id = e.id AND wh.state = 'active' AND wh.boundary_source = 'queue')
     THEN 'wanting'
     ELSE 'dormant' END";

pub(crate) async fn oracle(db: &LocalDb, execution: &str) -> String {
    db.query_opt_text(
        format!("SELECT {ORACLE_ACTIVITY_SQL} FROM executions e WHERE e.id = ?1"),
        (execution.to_string(),),
    )
    .await
    .unwrap()
    .unwrap()
}

/// Each stored fact of `job` beside the value its source rows give it, as
/// `(fact, stored, derived)`. The derivations are the oracle for the facts.
const JOB_FACTS_SQL: &str = "SELECT j.head_turn,
        CASE WHEN t.state IN ('pending', 'running') THEN 'running'
             WHEN t.state = 'complete' AND t.start_reason = 'dependency_unblock'
                  AND t.run_id IS NULL AND t.end_reason IS NULL THEN 'resume_owed'
             ELSE 'idle' END,
        COALESCE(j.turn_end_attempt_state, '-'),
        COALESCE((SELECT ta.state FROM turn_end_check_attempts ta WHERE ta.id = j.turn_end_check_attempt_id), '-'),
        CAST(j.open_cairn_waits AS TEXT),
        CAST((SELECT COUNT(*) FROM agent_waits aw WHERE aw.job_id = j.id
               AND (aw.state = 'resolving' OR (aw.state = 'pending' AND aw.kind NOT IN ('operator_request')))) AS TEXT),
        j.hold_standing,
        COALESCE((SELECT CASE WHEN h.boundary_source = 'queue' THEN 'queue'
                              WHEN h.retry_not_before IS NOT NULL AND h.resume_gate IS NULL THEN 'timer'
                              ELSE 'other' END
                    FROM provider_limit_holds h WHERE h.job_id = j.id AND h.state = 'active'), 'none'),
        CAST(j.open_check_requests AS TEXT),
        CAST((SELECT COUNT(*) FROM check_requests cr WHERE cr.requester_job_id = j.id
               AND cr.state IN ('accepted', 'queued', 'granted', 'running')) AS TEXT)
   FROM jobs j LEFT JOIN turns t ON t.id = j.current_turn_id WHERE j.id = ?1";

/// Assert every stored fact of every job of `execution` equals the value its
/// source rows give it.
pub(crate) async fn assert_job_facts(db: &LocalDb, execution: &str, step: &str) {
    let jobs = db
        .query_all(
            "SELECT id FROM jobs WHERE execution_id = ?1",
            (execution.to_string(),),
            |row| row.text(0),
        )
        .await
        .unwrap();
    for job in jobs {
        let pairs = db
            .query_one(JOB_FACTS_SQL, (job.clone(),), |row| {
                (0..5)
                    .map(|i| Ok((row.text(2 * i)?, row.text(2 * i + 1)?)))
                    .collect::<cairn_db::storage::DbResult<Vec<_>>>()
            })
            .await
            .unwrap();
        for (fact, (stored, derived)) in [
            "head_turn",
            "turn_end_attempt_state",
            "open_cairn_waits",
            "hold_standing",
            "open_check_requests",
        ]
        .iter()
        .zip(pairs)
        {
            assert_eq!(stored, derived, "{fact} of {job} after {step}");
        }
    }
}

/// Assert that after `step` the queue reads `expected` for `execution`, that
/// the read-time rule agrees, and that every stored fact of its jobs equals
/// its source rows. Writer tests call this right after a production write,
/// with nothing of their own written in between.
pub(crate) async fn assert_activity(db: &LocalDb, execution: &str, expected: &str, step: &str) {
    assert_eq!(oracle(db, execution).await, expected, "rule after {step}");
    assert_eq!(stored(db, execution).await, expected, "stored after {step}");
    assert_job_facts(db, execution, step).await;
}

/// What the queue reads for `execution`.
pub(crate) async fn stored(db: &LocalDb, execution: &str) -> String {
    let doing = db
        .read(|conn| {
            Box::pin(
                async move { cairn_db::stored_status::jobs::non_dormant_executions(conn).await },
            )
        })
        .await
        .unwrap();
    doing
        .get(execution)
        .copied()
        .unwrap_or(Activity::Dormant)
        .as_str()
        .to_string()
}

/// The stored-status migration's backfill statements.
fn backfill_sql() -> &'static str {
    let migration = cairn_db::storage::TURSO_MIGRATIONS
        .iter()
        .find(|migration| migration.name() == "job_stored_status")
        .expect("the stored-status migration is registered");
    let sql = migration.sql();
    &sql[sql
        .find("UPDATE jobs SET head_turn")
        .expect("the migration carries its backfill")..]
}

/// Give raw-seeded rows their stored facts (fixtures only; see
/// `cairn_db::stored_status::jobs::seed_from_sources_for_test`).
pub(crate) async fn seed_stored_status(db: &LocalDb) {
    cairn_db::stored_status::jobs::seed_from_sources_for_test(db)
        .await
        .unwrap();
}

pub(crate) async fn seed_project(db: &LocalDb) {
    db.execute_script(
        "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
         INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
           VALUES('p','w','P','p','/tmp/p',1,1);",
    )
    .await
    .unwrap();
}

/// Execution `id` on issue `i-{id}`, number `n`.
pub(crate) async fn seed_execution(db: &LocalDb, n: usize, id: &str) {
    db.execute_script(format!(
        "INSERT INTO issues(id,project_id,number,title,status,created_at,updated_at)
           VALUES('i-{id}','p',{n},'I','active',1,1);
         INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq)
           VALUES('{id}','recipe-default','i-{id}','p','running',1,1);"
    ))
    .await
    .unwrap();
}

/// Job `job` of `execution` in `status`, with session `s-{job}` and live run
/// `r-{job}`, and no turn.
pub(crate) async fn seed_job(db: &LocalDb, execution: &str, job: &str, status: &str) {
    db.execute_script(format!(
        "INSERT INTO jobs(id,execution_id,issue_id,project_id,status,created_at,updated_at)
           VALUES('{job}','{execution}',(SELECT issue_id FROM executions WHERE id='{execution}'),'p','{status}',1,1);
         INSERT INTO sessions(id,job_id,backend,backend_id,status,sequence,created_at,updated_at)
           VALUES('s-{job}','{job}','claude','native-{job}','open',1,1,1);
         INSERT INTO runs(id,job_id,session_id,status,created_at,updated_at)
           VALUES('r-{job}','{job}','s-{job}','live',1,1);"
    ))
    .await
    .unwrap();
}

/// One execution with a single job in one shape, and what it is doing.
struct Case {
    id: &'static str,
    status: &'static str,
    turn: Option<&'static str>,
    wait: Option<&'static str>,
    hold: Option<&'static str>,
    attempt: Option<&'static str>,
    request: Option<&'static str>,
    /// A claimed `dependency_unblock` marker heads the job, over a yielded
    /// anchor, with this SQL `end_reason`.
    marker: Option<&'static str>,
    expected: &'static str,
}

const fn case(id: &'static str, status: &'static str, expected: &'static str) -> Case {
    Case {
        id,
        status,
        turn: None,
        wait: None,
        hold: None,
        attempt: None,
        request: None,
        marker: None,
        expected,
    }
}

/// Every arm of the rule, each on and off, including the closed-job arms the
/// read-time rule never had a test for.
fn cases() -> Vec<Case> {
    vec![
        Case {
            turn: Some("running"),
            ..case("turn-running", "running", "active")
        },
        Case {
            turn: Some("pending"),
            ..case("turn-due", "pending", "active")
        },
        case("between-turns", "running", "active"),
        case("pending-no-turn", "pending", "dormant"),
        Case {
            turn: Some("yielded"),
            wait: Some("checks"),
            ..case("waits-on-check", "running", "active")
        },
        Case {
            turn: Some("yielded"),
            wait: Some("duration"),
            ..case("waits-on-timer", "running", "active")
        },
        Case {
            turn: Some("yielded"),
            wait: Some("operator_request"),
            ..case("asks-operator", "running", "dormant")
        },
        Case {
            turn: Some("yielded"),
            wait: Some("checks"),
            ..case("closed-waits", "complete", "dormant")
        },
        Case {
            turn: Some("yielded"),
            ..case("question", "running", "dormant")
        },
        Case {
            turn: Some("interrupted"),
            ..case("crashed", "running", "dormant")
        },
        Case {
            turn: Some("running"),
            ..case("idle-job", "idle", "dormant")
        },
        Case {
            turn: Some("yielded"),
            hold: Some("queue"),
            ..case("held", "pending", "wanting")
        },
        Case {
            turn: Some("complete"),
            hold: Some("queue"),
            ..case("closed-held", "complete", "wanting")
        },
        Case {
            turn: Some("yielded"),
            hold: Some("rate_limit_event"),
            ..case("limit-reset", "running", "active")
        },
        Case {
            turn: Some("yielded"),
            hold: Some("rate_limit_event"),
            ..case("closed-limit", "complete", "dormant")
        },
        Case {
            turn: Some("yielded"),
            hold: Some("unknown"),
            ..case("limit-unknown", "running", "dormant")
        },
        Case {
            turn: Some("complete"),
            attempt: Some("claimed"),
            ..case("closed-attempt", "complete", "active")
        },
        Case {
            turn: Some("complete"),
            attempt: Some("completed"),
            ..case("attempt-done", "complete", "dormant")
        },
        Case {
            turn: Some("complete"),
            request: Some("queued"),
            ..case("closed-request", "complete", "active")
        },
        Case {
            turn: Some("complete"),
            request: Some("recorded"),
            ..case("request-done", "complete", "dormant")
        },
        // A parent parked on blocking tasks whose last task settled: its
        // job's head is the claimed `dependency_unblock` marker. Its resume
        // is a turn due until it launches; once a launch is refused the
        // marker says so and pins no seat.
        Case {
            marker: Some("NULL"),
            ..case("unblock-owed", "running", "active")
        },
        Case {
            marker: Some("'launch_refused'"),
            ..case("unblock-refused", "running", "dormant")
        },
        Case {
            marker: Some("NULL"),
            ..case("unblock-closed", "complete", "dormant")
        },
    ]
}

/// Seed every case's rows with raw inserts, as an older database holds them,
/// leaving each job's stored activity at its default.
async fn seed_cases(db: &LocalDb, cases: &[Case]) {
    seed_project(db).await;
    for (n, c) in cases.iter().enumerate() {
        let (id, job) = (c.id, format!("j-{}", c.id));
        seed_execution(db, n, id).await;
        seed_job(db, id, &job, c.status).await;
        if let Some(state) = c.turn {
            db.execute_script(format!(
                "INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,start_reason,created_at,updated_at)
                   VALUES('t-{id}','s-{job}','r-{job}','{job}',1,'{state}','initial',1,1);
                 UPDATE jobs SET current_turn_id='t-{id}' WHERE id='{job}';"
            ))
            .await
            .unwrap();
        }
        if let Some(end_reason) = c.marker {
            db.execute_script(format!(
                "INSERT INTO turns(id,session_id,job_id,sequence,state,yield_reason,start_reason,created_at,updated_at)
                   VALUES('a-{id}','s-{job}','{job}',1,'yielded','dependency_wait','initial',1,1);
                 INSERT INTO turns(id,session_id,run_id,job_id,sequence,predecessor_id,state,start_reason,end_reason,created_at,updated_at)
                   VALUES('t-{id}','s-{job}',NULL,'{job}',2,'a-{id}','complete','dependency_unblock',{end_reason},1,1);
                 UPDATE jobs SET current_turn_id='t-{id}' WHERE id='{job}';"
            ))
            .await
            .unwrap();
        }
        if let Some(kind) = c.wait {
            db.execute_script(format!(
                "INSERT INTO agent_waits(id,job_id,run_id,session_id,predecessor_turn_id,tool_use_id,condition_json,state,created_at,kind)
                   VALUES('w-{id}','{job}','r-{job}','s-{job}','t-{id}','call','{{\"kind\":\"{kind}\"}}','pending',1,'{kind}');"
            ))
            .await
            .unwrap();
        }
        if let Some(source) = c.hold {
            let reset = if source == "rate_limit_event" {
                "99"
            } else {
                "NULL"
            };
            db.execute_script(format!(
                "INSERT INTO provider_limit_holds(id,job_id,run_id,turn_id,backend,boundary_source,retry_not_before,created_at,updated_at)
                   VALUES('h-{id}','{job}','r-{job}','t-{id}','claude','{source}',{reset},1,1);"
            ))
            .await
            .unwrap();
        }
        if let Some(state) = c.attempt {
            db.execute_script(format!(
                "INSERT INTO turn_end_check_attempts(id,job_id,state,created_at,updated_at)
                   VALUES('a-{id}','{job}','{state}',1,1);
                 UPDATE jobs SET turn_end_check_attempt_id='a-{id}' WHERE id='{job}';"
            ))
            .await
            .unwrap();
        }
        if let Some(state) = c.request {
            db.execute_script(format!(
                "INSERT INTO check_requests(id,project_id,suite,consumer,requester_ref,requester_job_id,priority,state,accepted_at_ms,state_since_ms,updated_ms)
                   VALUES('cr-{id}','p','rust','requested','node','{job}','review_check','{state}',1,1,1);"
            ))
            .await
            .unwrap();
        }
    }
}

/// Every shape an existing database can hold: the migration's backfill gives
/// each job its facts from its source rows, and the queue's `CASE` over those
/// facts reads what the read-time rule derived, through the partial index.
#[tokio::test(flavor = "current_thread")]
async fn the_stored_facts_read_as_the_read_time_rule_derived() {
    let db = crate::storage::migrated_test_db("run-queue-backfill.db").await;
    let cases = cases();
    seed_cases(&db, &cases).await;
    db.execute_script(backfill_sql()).await.unwrap();
    for c in &cases {
        assert_activity(&db, c.id, c.expected, c.id).await;
    }
}

/// The read names the index's predicate verbatim, so the planner uses it.
#[test]
fn the_partial_index_is_the_queues_predicate() {
    let migration = cairn_db::storage::TURSO_MIGRATIONS
        .iter()
        .find(|migration| migration.name() == "job_stored_status")
        .unwrap();
    assert!(migration.sql().contains(&format!(
        "CREATE INDEX idx_jobs_queue_activity ON jobs(execution_id) WHERE {};",
        cairn_db::stored_status::jobs::QUEUE_LISTED_SQL
    )));
}

/// Execution `e{n}` with job `j{n}` in `status`, its session and run, and a
/// current turn `t{n}` in `turn`, each job's activity settled.
pub(crate) async fn seed_turn_job(db: &LocalDb, n: usize, status: &str, turn: &str) {
    let (execution, job) = (format!("e{n}"), format!("j{n}"));
    seed_execution(db, n, &execution).await;
    seed_job(db, &execution, &job, status).await;
    db.execute_script(format!(
        "INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,start_reason,created_at,updated_at)
           VALUES('t{n}','s-{job}','r-{job}','{job}',1,'{turn}','initial',1,1);
         UPDATE jobs SET current_turn_id='t{n}' WHERE id='{job}';"
    ))
    .await
    .unwrap();
    seed_stored_status(db).await;
}

/// The run reaper: a host restart yields a running turn that holds a `run`
/// call open (its inline wait keeps it active), and an in-session reap
/// interrupts one and cancels that wait (dormant).
#[tokio::test(flavor = "current_thread")]
async fn the_reapers_settlements_store_the_activity_they_leave() {
    use cairn_session::runs::queries::{settle_stranded_turns, StaleRunSettlement};
    let db = crate::storage::migrated_test_db("run-queue-reaper.db").await;
    seed_project(&db).await;
    for n in [1, 2] {
        seed_turn_job(&db, n, "running", "running").await;
        db.execute_script(format!(
            "INSERT INTO agent_waits(id,job_id,run_id,session_id,predecessor_turn_id,tool_use_id,condition_json,state,created_at,kind,hold)
               VALUES('w{n}','j{n}','r-j{n}','s-j{n}','t{n}','call','{{\"kind\":\"checks\"}}','pending',1,'checks','inline');"
        ))
        .await
        .unwrap();
        seed_stored_status(&db).await;
        assert_activity(&db, &format!("e{n}"), "active", "seeding").await;
    }
    for (n, settlement, expected) in [
        (1, StaleRunSettlement::HostRestart, "active"),
        (2, StaleRunSettlement::Reaped, "dormant"),
    ] {
        let run = format!("r-j{n}");
        db.write(move |conn| {
            let run = run.clone();
            Box::pin(async move { settle_stranded_turns(conn, &run, settlement, 50).await })
        })
        .await
        .unwrap();
        assert_activity(&db, &format!("e{n}"), expected, &format!("{settlement:?}")).await;
    }
}

/// A failed write leaves the stored facts as they were: each setter runs in
/// its writer's transaction, so it rolls back with it.
#[tokio::test(flavor = "current_thread")]
async fn a_rolled_back_write_leaves_the_stored_activity_unchanged() {
    let db = crate::storage::migrated_test_db("run-queue-rollback.db").await;
    seed_project(&db).await;
    seed_execution(&db, 1, "e").await;
    seed_job(&db, "e", "j", "pending").await;
    seed_stored_status(&db).await;
    assert_eq!(stored(&db, "e").await, "dormant");
    let failed = db
        .write(|conn| {
            Box::pin(async move {
                cairn_workflow::advancement::recompute::write_job_status(
                    conn,
                    "j",
                    &crate::models::JobStatus::Pending,
                    &crate::models::JobStatus::Running,
                )
                .await?;
                conn.execute("INSERT INTO no_such_table VALUES (1)", ())
                    .await?;
                Ok(())
            })
        })
        .await;
    assert!(failed.is_err());
    assert_eq!(stored(&db, "e").await, "dormant");
    assert_eq!(oracle(&db, "e").await, "dormant");
}

/// Drive a seeded random sequence of production writes over a few executions
/// and check the stored activity against the read-time rule after every one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stored_activity_tracks_the_rule_through_a_random_sequence_of_writes() {
    use crate::mcp::handlers::durable_suspend;
    use crate::models::{JobStatus, TurnStartReason, TurnState, TurnYieldReason};

    let orch = crate::resources::references::test_orchestrator(
        crate::storage::migrated_test_db("run-queue-sequence.db").await,
    );
    let db = orch.db.local.clone();
    let emitter = orch.services.emitter.clone();
    seed_project(&db).await;
    let executions = ["e0", "e1", "e2"];
    let mut jobs = Vec::new();
    for (n, execution) in executions.iter().enumerate() {
        seed_execution(&db, n, execution).await;
        for k in 0..2 {
            let job = format!("{execution}-j{k}");
            seed_job(&db, execution, &job, "running").await;
            cairn_workflow::turns::create_initial_turn(
                &db,
                &format!("t-{job}-0"),
                &format!("s-{job}"),
                &job,
                emitter.as_ref(),
            )
            .await
            .unwrap();
            let kind = if k == 0 { "checks" } else { "operator_request" };
            db.execute_script(format!(
                "INSERT INTO agent_waits(id,job_id,run_id,session_id,predecessor_turn_id,tool_use_id,condition_json,state,created_at,kind)
                   VALUES('w-{job}','{job}','r-{job}','s-{job}','t-{job}-0','call-{job}','{{\"kind\":\"{kind}\"}}','pending',1,'{kind}');"
            ))
            .await
            .unwrap();
            seed_stored_status(&db).await;
            jobs.push((execution.to_string(), job));
        }
    }
    for execution in executions {
        assert_eq!(stored(&db, execution).await, oracle(&db, execution).await);
    }

    let mut seed: u64 = 0x7675_5eed;
    let mut next = move |n: u64| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed % n
    };
    let mut attempts: HashMap<String, String> = HashMap::new();
    let mut turns = 0;
    for step in 0..240 {
        let (_, job) = jobs[next(jobs.len() as u64) as usize].clone();
        let action = next(9);
        let (run, session) = (format!("r-{job}"), format!("s-{job}"));
        let head = db
            .query_opt(
                "SELECT t.id, t.state FROM jobs j JOIN turns t ON t.id = j.current_turn_id WHERE j.id = ?1",
                (job.clone(),),
                |row| Ok((row.text(0)?, row.text(1)?)),
            )
            .await
            .unwrap();
        let now = 1_000 + step as i64;
        match action {
            0 => {
                let from: JobStatus = db
                    .query_opt_text("SELECT status FROM jobs WHERE id = ?1", (job.clone(),))
                    .await
                    .unwrap()
                    .unwrap()
                    .parse()
                    .unwrap();
                let to = [
                    JobStatus::Pending,
                    JobStatus::Running,
                    JobStatus::Complete,
                    JobStatus::Idle,
                    JobStatus::Cancelled,
                ][next(5) as usize]
                    .clone();
                let job = job.clone();
                db.write(move |conn| {
                    let (job, from, to) = (job.clone(), from.clone(), to.clone());
                    Box::pin(async move {
                        cairn_workflow::advancement::recompute::write_job_status(
                            conn, &job, &from, &to,
                        )
                        .await
                    })
                })
                .await
                .unwrap();
            }
            1 => {
                if let Some((head, state)) = &head {
                    let state: TurnState = state.parse().unwrap();
                    if state.is_terminal() {
                        turns += 1;
                        let _ = cairn_workflow::turns::create_successor_turn(
                            &db,
                            &format!("t-{job}-{turns}"),
                            &session,
                            &job,
                            head,
                            TurnStartReason::FollowUp,
                            emitter.as_ref(),
                        )
                        .await;
                    }
                }
            }
            2 => {
                if let Some((head, _)) = &head {
                    let _ = cairn_workflow::transitions::turn::start_turn(
                        &db,
                        head,
                        &run,
                        emitter.as_ref(),
                    )
                    .await;
                }
            }
            3 => {
                if let Some((head, _)) = &head {
                    if next(2) == 0 {
                        let _ = cairn_workflow::transitions::turn::apply_turn_outcome(
                            &db,
                            head,
                            TurnState::Complete,
                            None,
                            emitter.as_ref(),
                        )
                        .await;
                    } else {
                        let head = head.clone();
                        db.write(move |conn| {
                            let head = head.clone();
                            Box::pin(async move {
                                cairn_workflow::transitions::turn::yield_running_turn_conn(
                                    conn,
                                    &head,
                                    TurnYieldReason::Wait,
                                    now,
                                )
                                .await
                                .map(drop)
                            })
                        })
                        .await
                        .unwrap();
                    }
                }
            }
            4 => {
                if next(2) == 0 {
                    let _ = crate::orchestrator::wakes::arm_queue_hold(
                        &db,
                        &job,
                        crate::orchestrator::wakes::HoldKind::Launch,
                        None,
                        now,
                    )
                    .await;
                } else {
                    crate::orchestrator::wakes::cancel_provider_limit_holds_for_job(&db, &job)
                        .await
                        .unwrap();
                }
            }
            5 => {
                let (run, call, which) = (run.clone(), format!("call-{job}"), next(3));
                db.write(move |conn| {
                    let (run, call) = (run.clone(), call.clone());
                    Box::pin(async move {
                        match which {
                            0 => durable_suspend::relinquish_to_inner_wait(conn, &run, &call)
                                .await
                                .map(drop),
                            1 => durable_suspend::reclaim_from_inner_wait(conn, &run, &call)
                                .await
                                .map(drop),
                            _ => {
                                durable_suspend::cancel_run_waits_conn(conn, &run, "test", now)
                                    .await
                            }
                        }
                    })
                })
                .await
                .unwrap();
            }
            _ => {
                let key = job.clone();
                let (orch, job, current) = (orch.clone(), job.clone(), attempts.get(&job).cloned());
                let which = next(4);
                let requested = tokio::task::spawn_blocking(move || {
                    use crate::execution::checks_turn_end as attempt;
                    match (current, which) {
                        (None, _) | (_, 0) => attempt::request_turn_end_attempt(&orch, &job).ok(),
                        (Some(id), 1) => {
                            let _ = attempt::abandon_turn_end_attempt(&orch, &job, &id, "test");
                            None
                        }
                        (Some(id), _) => {
                            let state =
                                ["claimed", "runtime_started", "completed", "failed"][step % 4];
                            let _ =
                                attempt::transition_turn_end_attempt(&orch, &job, &id, state, None);
                            None
                        }
                    }
                })
                .await
                .unwrap();
                if let Some(id) = requested {
                    attempts.insert(key, id);
                }
            }
        }
        for execution in executions {
            let context = format!("step {step}: action {action} on {job}");
            assert_eq!(
                stored(&db, execution).await,
                oracle(&db, execution).await,
                "{context}"
            );
            assert_job_facts(&db, execution, &context).await;
        }
    }
}

/// Counts are deltas with no reconciler, so each writer must move them only
/// by what its own write changed: a repeated settle, a cancel after
/// resolution, and a wait that never transferred move nothing. Deleting an
/// issue's jobs takes their facts with them and leaves every other job's
/// facts exact; that is the one production path that deletes the runs,
/// sessions and turns waits and holds cascade from (the `source_structure`
/// scan holds that).
#[tokio::test(flavor = "current_thread")]
async fn counts_survive_double_settles_cancels_after_resolution_and_job_deletion() {
    use crate::mcp::handlers::durable_suspend;
    let db = crate::storage::migrated_test_db("run-queue-counts.db").await;
    seed_project(&db).await;
    for n in [1, 2] {
        seed_turn_job(&db, n, "running", "yielded").await;
        db.execute_script(format!(
            "INSERT INTO agent_waits(id,job_id,run_id,session_id,predecessor_turn_id,tool_use_id,condition_json,state,created_at,kind)
               VALUES('w{n}','j{n}','r-j{n}','s-j{n}','t{n}','call','{{\"kind\":\"checks\"}}','pending',1,'checks');
             INSERT INTO provider_limit_holds(id,job_id,run_id,turn_id,backend,boundary_source,kind,created_at,updated_at)
               VALUES('h{n}','j{n}','r-j{n}','t{n}','claude','queue','resume',1,1);
             INSERT INTO check_requests(id,project_id,suite,consumer,requester_ref,requester_job_id,priority,state,accepted_at_ms,state_since_ms,updated_ms)
               VALUES('cr{n}','p','rust','requested','node','j{n}','review_check','queued',1,1,1);"
        ))
        .await
        .unwrap();
    }
    seed_stored_status(&db).await;
    assert_activity(&db, "e1", "active", "seeding").await;

    let wait_write = |which: u8| {
        let db = &db;
        async move {
            db.write(move |conn| {
                Box::pin(async move {
                    match which {
                        0 => durable_suspend::relinquish_to_inner_wait(conn, "r-j1", "call")
                            .await
                            .map(drop),
                        1 => durable_suspend::reclaim_from_inner_wait(conn, "r-j1", "call")
                            .await
                            .map(drop),
                        _ => durable_suspend::cancel_run_waits_conn(conn, "r-j1", "test", 5).await,
                    }
                })
            })
            .await
            .unwrap();
        }
    };
    for (step, which) in [
        ("transfer", 0),
        ("transfer again", 0),
        ("reclaim", 1),
        ("reclaim again", 1),
        ("cancel", 2),
        ("cancel again", 2),
        ("reclaim after cancel", 1),
        ("transfer after cancel", 0),
    ] {
        wait_write(which).await;
        assert_job_facts(&db, "e1", step).await;
    }
    for step in ["cancel holds", "cancel holds again"] {
        crate::orchestrator::wakes::cancel_provider_limit_holds_for_job(&db, "j1")
            .await
            .unwrap();
        assert_job_facts(&db, "e1", step).await;
    }

    db.write(|conn| {
        Box::pin(async move {
            crate::jobs::cleanup::delete_owned_jobs(
                conn,
                crate::jobs::cleanup::JobOwner::Issue,
                "i-e2",
            )
            .await
        })
    })
    .await
    .unwrap();
    assert_eq!(
        db.query_opt_i64("SELECT COUNT(*) FROM jobs WHERE id = 'j2'", ())
            .await
            .unwrap(),
        Some(0)
    );
    assert_activity(&db, "e2", "dormant", "deleting the issue's jobs").await;
    // e1 keeps its open check request, so it stays active.
    assert_activity(&db, "e1", "active", "another issue's jobs deleted").await;
}

/// A parent that an earlier build held behind the queue the moment its last
/// task settled (a queue resume hold resting on its `dependency_unblock`
/// marker) is seated by the next settle, ahead of wanting work and past a full
/// cap, and so has its held resume released; it never reads as queued.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn settle_seats_a_parent_held_behind_the_queue_on_its_unblock_marker() {
    let orch = crate::resources::references::test_orchestrator(
        crate::storage::migrated_test_db("run-queue-unblock-held.db").await,
    );
    orch.update_settings(crate::models::UpdateSettings {
        max_concurrent_executions: Some(1),
        ..Default::default()
    })
    .unwrap();
    let db = orch.db.local.clone();
    seed_project(&db).await;
    // `busy` fills the one seat; `due` wants one and is ahead of `parent`.
    for (n, id, job_status, turn_state) in [
        (1, "busy", "running", "running"),
        (2, "due", "pending", "complete"),
        (3, "parent", "running", "yielded"),
    ] {
        db.execute_script(format!(
            "INSERT INTO issues(id,project_id,number,title,status,created_at,updated_at)
               VALUES('i-{id}','p',{n},'I','active',1,1);
             INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq)
               VALUES('{id}','recipe-default','i-{id}','p','running',{n},1);
             INSERT INTO jobs(id,execution_id,project_id,status,created_at,updated_at)
               VALUES('j-{id}','{id}','p','{job_status}',1,1);
             INSERT INTO turns(id,session_id,job_id,sequence,state,start_reason,created_at,updated_at)
               VALUES('t-{id}','s-{id}','j-{id}',1,'{turn_state}','initial',1,1);
             UPDATE jobs SET current_turn_id='t-{id}' WHERE id='j-{id}';"
        ))
        .await
        .unwrap();
    }
    db.execute_script(
        "INSERT INTO turns(id,session_id,run_id,job_id,sequence,predecessor_id,state,start_reason,created_at,updated_at)
           VALUES('marker','s-parent',NULL,'j-parent',2,'t-parent','complete','dependency_unblock',2,2);
         UPDATE jobs SET current_turn_id='marker' WHERE id='j-parent';
         INSERT INTO provider_limit_holds(id,job_id,turn_id,backend,boundary_source,kind,created_at,updated_at)
           VALUES('h-due','j-due','t-due','claude','queue','launch',1,1),
                 ('h-parent','j-parent','marker','claude','queue','resume',2,2);",
    )
    .await
    .unwrap();
    seed_stored_status(&db).await;

    let state = settle(&orch).await.unwrap();
    let parent = state.item("parent").unwrap();
    assert_eq!(parent.activity, Activity::Active);
    assert!(parent.runs, "the owed resume holds its seat");
    assert_eq!(recorded_state(parent), None, "never reads as queued");
    assert!(!state.runs("due"), "nothing else takes the full cap");
    assert_eq!(
        db.query_opt_text("SELECT queue_state FROM executions WHERE id = 'parent'", ())
            .await
            .unwrap(),
        None
    );
}

/// The launch seam every wake goes through: with the one seat taken by running
/// work, a dormant execution's launch is held as queued, unless it carries the
/// operator's own message, which runs the addressed agent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_launch_with_no_seat_is_held_unless_it_carries_the_operators_message() {
    let orch = crate::resources::references::test_orchestrator(
        crate::storage::migrated_test_db("run-queue-launch.db").await,
    );
    orch.update_settings(crate::models::UpdateSettings {
        max_concurrent_executions: Some(1),
        ..Default::default()
    })
    .unwrap();
    let db = orch.db.local.clone();
    db.execute_script(
        "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
         INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
           VALUES('p','w','P','p','/tmp/p',1,1);",
    )
    .await
    .unwrap();
    for (n, id, turn_state) in [(1, "busy", "running"), (2, "idle", "complete")] {
        db.execute_script(format!(
            "INSERT INTO issues(id,project_id,number,title,status,created_at,updated_at)
               VALUES('i-{id}','p',{n},'I','active',1,1);
             INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq)
               VALUES('{id}','recipe-default','i-{id}','p','running',{n},1);
             INSERT INTO jobs(id,execution_id,project_id,status,created_at,updated_at)
               VALUES('j-{id}','{id}','p','running',1,1);
             INSERT INTO sessions(id,job_id,backend,backend_id,status,sequence,created_at,updated_at)
               VALUES('s-{id}','j-{id}','claude','native-{id}','open',1,1,1);
             INSERT INTO runs(id,job_id,session_id,status,created_at,updated_at)
               VALUES('r-{id}','j-{id}','s-{id}','live',1,1);
             INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,start_reason,created_at,updated_at)
               VALUES('t-{id}','s-{id}','r-{id}','j-{id}',1,'{turn_state}','initial',1,1);
             UPDATE jobs SET current_turn_id='t-{id}' WHERE id='j-{id}';"
        ))
        .await
        .unwrap();
    }
    // Raw inserts are not writers: give the seeded rows their stored facts.
    seed_stored_status(&db).await;
    let refusal = crate::orchestrator::wakes::queue_job_refusal(&orch, &db, "j-idle")
        .await
        .unwrap();
    assert_eq!(refusal.as_deref(), Some("Queued · 1st in line"));
    assert_eq!(
        crate::orchestrator::wakes::queue_job_refusal(&orch, &db, "j-busy")
            .await
            .unwrap(),
        None,
        "active work keeps its seat"
    );
    db.execute_script(
        "INSERT INTO queued_messages(id,job_id,content,delivery,created_at,kind)
           VALUES('m','j-idle','?','queue',2,'operator');",
    )
    .await
    .unwrap();
    assert_eq!(
        crate::orchestrator::wakes::queue_job_refusal(&orch, &db, "j-idle")
            .await
            .unwrap(),
        None,
        "the operator's own message runs the addressed agent"
    );
}
