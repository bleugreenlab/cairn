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

/// The classification reads stored facts. Each case is one execution whose
/// single job is in the named state; the `waitFor` duration arm is the one the
/// operator may flip, and flipping it is a change to `person_wait_kinds!`.
#[tokio::test(flavor = "current_thread")]
async fn activity_is_read_from_stored_facts() {
    let db = crate::storage::migrated_test_db("run-queue-activity.db").await;
    db.execute_script(
        "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
         INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
           VALUES('p','w','P','p','/tmp/p',1,1);",
    )
    .await
    .unwrap();
    let cases = [
        ("turn-running", "running", "running", None, None, "active"),
        (
            "waits-on-check",
            "running",
            "yielded",
            Some("checks"),
            None,
            "active",
        ),
        (
            "waits-on-timer",
            "running",
            "yielded",
            Some("duration"),
            None,
            "active",
        ),
        (
            "asks-operator",
            "running",
            "yielded",
            Some("operator_request"),
            None,
            "dormant",
        ),
        ("question", "running", "yielded", None, None, "dormant"),
        ("crashed", "running", "interrupted", None, None, "dormant"),
        ("held", "pending", "yielded", None, Some("queue"), "wanting"),
        (
            "limit-reset",
            "running",
            "yielded",
            None,
            Some("rate_limit_event"),
            "active",
        ),
    ];
    for (n, (id, job_status, turn_state, wait, hold, _)) in cases.iter().enumerate() {
        let sql = format!(
            "INSERT INTO issues(id,project_id,number,title,status,created_at,updated_at)
               VALUES('i-{id}','p',{n},'I','active',1,1);
             INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq)
               VALUES('{id}','recipe-default','i-{id}','p','running',1,1);
             INSERT INTO jobs(id,execution_id,project_id,status,created_at,updated_at)
               VALUES('j-{id}','{id}','p','{job_status}',1,1);
             INSERT INTO sessions(id,job_id,backend,backend_id,status,sequence,created_at,updated_at)
               VALUES('s-{id}','j-{id}','claude','native-{id}','open',1,1,1);
             INSERT INTO runs(id,job_id,session_id,status,created_at,updated_at)
               VALUES('r-{id}','j-{id}','s-{id}','live',1,1);
             INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,start_reason,created_at,updated_at)
               VALUES('t-{id}','s-{id}','r-{id}','j-{id}',1,'{turn_state}','initial',1,1);
             UPDATE jobs SET current_turn_id='t-{id}' WHERE id='j-{id}';"
        );
        db.execute_script(&sql).await.unwrap();
        if let Some(kind) = wait {
            db.execute_script(format!(
                "INSERT INTO agent_waits(id,job_id,run_id,session_id,predecessor_turn_id,tool_use_id,condition_json,state,created_at,kind)
                   VALUES('w-{id}','j-{id}','r-{id}','s-{id}','t-{id}','call','{{\"kind\":\"{kind}\"}}','pending',1,'{kind}');"
            ))
            .await
            .unwrap();
        }
        if let Some(source) = hold {
            let reset = if *source == "queue" { "NULL" } else { "99" };
            db.execute_script(format!(
                "INSERT INTO provider_limit_holds(id,job_id,run_id,turn_id,backend,boundary_source,retry_not_before,created_at,updated_at)
                   VALUES('h-{id}','j-{id}','r-{id}','t-{id}','claude','{source}',{reset},1,1);"
            ))
            .await
            .unwrap();
        }
    }
    for (id, .., expected) in cases {
        let sql = format!(
            "SELECT {} FROM executions e WHERE e.id = '{id}'",
            activity_sql!()
        );
        let activity = db.query_opt_text(&sql, ()).await.unwrap();
        assert_eq!(activity.as_deref(), Some(expected), "{id}");
    }
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
