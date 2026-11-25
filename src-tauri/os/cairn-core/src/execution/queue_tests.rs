//! The run queue's authority, as the issue states its acceptance: an entry
//! runs iff inside both lines; a waiting entry holds its place; a move to the
//! top starts it and stops what it pushes below the line, which runs again
//! when it rises; park removes waiting and running entries alike and unpark
//! restores; the same stored list always decides the same running set.

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
    }
}

/// Executions `e<n>` on issues `i<n>`, in thread `thread`, all live.
fn facts(spec: &[(&str, Option<&str>)]) -> HashMap<String, ExecutionFact> {
    spec.iter()
        .enumerate()
        .map(|(at, (id, thread))| {
            (
                (*id).to_string(),
                fact(&format!("issue-{id}"), *thread, true, at as i64),
            )
        })
        .collect()
}

fn known(facts: &HashMap<String, ExecutionFact>) -> HashSet<String> {
    facts.keys().cloned().collect()
}

fn joined(facts: &HashMap<String, ExecutionFact>) -> Vec<StoredRow> {
    plan_membership(Vec::new(), facts, &known(facts)).rows
}

fn running(rows: &[StoredRow], facts: &HashMap<String, ExecutionFact>) -> Vec<String> {
    decide(rows, facts, CAPS)
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

#[test]
fn an_entry_runs_only_inside_both_lines() {
    let facts = facts(&[
        ("a1", Some("A")),
        ("a2", Some("A")),
        ("b1", Some("B")),
        ("c1", Some("C")),
    ]);
    let rows = joined(&facts);
    assert_eq!(order(&rows), ["a1", "a2", "b1", "c1"]);
    assert_eq!(running(&rows, &facts), ["a1", "b1"]);
}

#[test]
fn a_waiting_or_finished_entry_holds_its_place() {
    let mut facts = facts(&[("a1", Some("A")), ("b1", Some("B")), ("c1", Some("C"))]);
    let rows = joined(&facts);
    // a1 finishes its recipe and waits on review: no longer joinable, still
    // live, so it keeps its row and its place.
    facts.get_mut("a1").unwrap().joinable = false;
    let kept = plan_membership(rows, &facts, &known(&facts));
    assert!(!kept.changed);
    assert_eq!(running(&kept.rows, &facts), ["a1", "b1"]);
}

#[test]
fn a_move_to_the_top_starts_it_and_stops_the_one_displaced() {
    let facts = facts(&[("a1", Some("A")), ("b1", Some("B")), ("c1", Some("C"))]);
    let mut rows = joined(&facts);
    assert_eq!(running(&rows, &facts), ["a1", "b1"]);
    assert!(plan_move(&mut rows, "c1", 1));
    assert_eq!(order(&rows), ["c1", "a1", "b1"]);
    assert_eq!(running(&rows, &facts), ["c1", "a1"], "b1 is displaced");
    let b1 = decide(&rows, &facts, CAPS);
    assert_eq!(
        b1.item("b1").unwrap().position,
        Some(3),
        "displaced, still queued"
    );
    assert!(plan_move(&mut rows, "c1", 3));
    assert_eq!(
        running(&rows, &facts),
        ["a1", "b1"],
        "b1 runs again once back above the line"
    );
}

#[test]
fn park_removes_waiting_and_running_entries_and_unpark_restores() {
    let facts = facts(&[("a1", Some("A")), ("b1", Some("B")), ("c1", Some("C"))]);
    let mut rows = joined(&facts);
    assert!(plan_park(&mut rows, "a1", true), "a running entry");
    assert!(plan_park(&mut rows, "c1", true), "a waiting entry");
    assert_eq!(order(&rows), ["b1"]);
    let state = decide(&rows, &facts, CAPS);
    assert_eq!(running(&rows, &facts), ["b1"]);
    assert!(state.item("a1").unwrap().parked && state.item("a1").unwrap().position.is_none());
    // A parked execution is never added back by membership.
    let again = plan_membership(rows.clone(), &facts, &known(&facts));
    assert_eq!(order(&again.rows), ["b1"]);
    assert!(
        !plan_move(&mut rows, "a1", 1),
        "a parked entry has no place to move"
    );
    assert!(plan_park(&mut rows, "a1", false));
    assert_eq!(order(&rows), ["b1", "a1"], "unpark returns it to the end");
    assert_eq!(running(&rows, &facts), ["b1", "a1"]);
}

#[test]
fn membership_follows_liveness_and_an_unknown_row_keeps_its_place() {
    let mut facts = facts(&[("a1", Some("A")), ("b1", Some("B"))]);
    let rows = joined(&facts);
    // b1's issue merges: its database knows it and no longer reads it live.
    facts.remove("b1");
    let mut known_ids = known(&facts);
    known_ids.insert("b1".into());
    let left = plan_membership(rows.clone(), &facts, &known_ids);
    assert_eq!(left.dropped, ["b1"]);
    assert_eq!(order(&left.rows), ["a1"]);
    // A team database that is offline knows nothing: the row stays, but is
    // passed over by both lines.
    let offline = plan_membership(rows, &facts, &known(&facts));
    assert!(offline.dropped.is_empty());
    assert_eq!(order(&offline.rows), ["a1", "b1"]);
    assert_eq!(running(&offline.rows, &facts), ["a1"]);
}

#[test]
fn joiners_append_in_order_and_a_rerun_takes_its_issues_place() {
    let mut facts = facts(&[("a1", Some("A")), ("b1", Some("B"))]);
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
fn the_stored_list_alone_reproduces_the_running_set() {
    let facts = facts(&[
        ("a1", Some("A")),
        ("a2", Some("A")),
        ("b1", Some("B")),
        ("u", None),
    ]);
    let mut rows = joined(&facts);
    assert!(plan_move(&mut rows, "u", 1));
    assert!(plan_park(&mut rows, "b1", true));
    let before = running(&rows, &facts);
    // A restart reads back exactly what was stored: same rows, same ranks.
    let reread = plan_membership(rows.clone(), &facts, &known(&facts));
    assert!(!reread.changed, "a reread writes nothing");
    assert_eq!(running(&reread.rows, &facts), before);
    assert_eq!(before, ["u", "a1"]);
}

#[test]
fn a_move_clamps_to_the_list() {
    let facts = facts(&[("a", None), ("b", None), ("c", None)]);
    let mut rows = joined(&facts);
    assert!(plan_move(&mut rows, "a", 99));
    assert_eq!(order(&rows), ["b", "c", "a"]);
    assert!(plan_move(&mut rows, "a", 0));
    assert_eq!(order(&rows), ["a", "b", "c"]);
    assert!(!plan_move(&mut rows, "missing", 1));
}
