//! The execution queue's rotation end to end (cairn/5943): filings onto a full
//! fleet, one slot freed at a time, and the sweeps' releases read back against
//! the order the queue page numbers.

use std::sync::Arc;

use serde_json::{json, Value};

use super::capacity::tests::{add_issue, add_started, add_thread, caps, full_fleet, is_held};
use super::capacity::{fleet_order, queue_view, QueueSnapshot, Start};
use super::reorder::order_thread_queue_conn;
use super::reorder::tests::operator_move;
use super::sweep::tests::{attach_team, count, env, execution, file, now, restarted, Env};
use super::sweep::{evaluate_execution, sweep, sweep_at};
use crate::execution::recipe::{LaunchIntent, QueuePlace};
use crate::storage::{LocalDb, RowExt};

const ALPHA: i32 = 400;
const BETA: i32 = 500;
const GAMMA: i32 = 600;

/// A team replica's rows route by a `{team}~{uuid}` id.
const TEAM_PROJECT: &str = "teamgate~00000000-0000-4000-8000-0000000000a1";
const TEAM_THREAD: &str = "teamgate~00000000-0000-4000-8000-0000000000b1";

/// The lanes' executions, each in filing order.
struct Lanes {
    env: Env,
    a: Vec<String>,
    b: Vec<String>,
    c: Vec<String>,
}

async fn file_children(env: &Env, thread: &str, base: i32, count: i32) -> Vec<String> {
    let mut ids = Vec::new();
    for n in 1..=count {
        add_issue(env, base + n, Some(thread)).await;
        file(env, base + n, json!([])).await.unwrap();
        assert!(is_held(env, base + n).await, "{} holds", base + n);
        ids.push(execution(env, base + n).await.0);
    }
    ids
}

/// A full fleet, then alpha files `sizes.0` children, beta `sizes.1` and
/// gamma `sizes.2`, in that order: every lane's head is older than the next
/// lane's, so first-in-first-out would drain alpha first. The thread focus
/// cap is `focus`. One pass numbers the places.
async fn lanes(name: &str, sizes: (i32, i32, i32), focus: i32) -> Lanes {
    let env = env(name).await;
    caps(&env, 6, focus);
    full_fleet(&env).await;
    for (id, name) in [("ta", "alpha"), ("tb", "beta"), ("tc", "gamma")] {
        add_thread(&env, id, name).await;
    }
    let a = file_children(&env, "ta", ALPHA, sizes.0).await;
    let b = file_children(&env, "tb", BETA, sizes.1).await;
    let c = file_children(&env, "tc", GAMMA, sizes.2).await;
    sweep_at(&env.orch, now()).await.unwrap();
    Lanes { env, a, b, c }
}

async fn view_ids(env: &Env) -> Vec<String> {
    queue_view(&env.orch)
        .await
        .unwrap()
        .held
        .into_iter()
        .map(|held| held.execution_id)
        .collect()
}

/// End every job of `execution_id` in each of `dbs`: its fleet slot frees, and
/// with no PR it stops counting as an open child.
async fn end_jobs(dbs: &[&Arc<LocalDb>], execution_id: &str) {
    for db in dbs {
        db.execute(
            "UPDATE jobs SET status = 'complete' WHERE execution_id = ?1",
            (execution_id.to_string(),),
        )
        .await
        .unwrap();
    }
}

/// Free one fleet slot by ending `running`'s jobs, sweep, and return the one
/// execution the sweep released.
async fn release_one(env: &Env, dbs: &[&Arc<LocalDb>], running: &str) -> String {
    end_jobs(dbs, running).await;
    let report = sweep_at(&env.orch, now()).await.unwrap();
    assert_eq!(
        report.released.len(),
        1,
        "one slot, one release: {report:?}"
    );
    report.released[0].clone()
}

async fn order(env: &Env, thread: &str, name: &str, entries: Value) -> Result<(), Vec<String>> {
    let entries = entries.as_array().unwrap().clone();
    let (thread, name) = (thread.to_string(), name.to_string());
    env.db
        .write(move |conn| {
            let (entries, thread, name) = (entries.clone(), thread.clone(), name.clone());
            Box::pin(async move {
                Ok(
                    order_thread_queue_conn(conn, &thread, &name, &entries, &name)
                        .await?
                        .map(|_| ()),
                )
            })
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn three_threads_filing_ten_three_and_one_release_in_rotation() {
    let Lanes { env, a, b, c } = lanes("rotation-ten-three-one.db", (10, 3, 1), 50).await;
    let mut expected = vec![
        a[0].clone(),
        b[0].clone(),
        c[0].clone(),
        a[1].clone(),
        b[1].clone(),
        a[2].clone(),
        b[2].clone(),
    ];
    expected.extend(a[3..].iter().cloned());
    assert_eq!(
        view_ids(&env).await,
        expected,
        "the page numbers the rotation"
    );

    let mut running = "se100".to_string();
    let mut released = Vec::new();
    for _ in 0..expected.len() {
        running = release_one(&env, &[&env.db], &running).await;
        released.push(running.clone());
    }
    assert_eq!(released, expected);
}

/// Alpha sits at its focus cap: its head is skipped, beta and gamma take the
/// slots, and alpha keeps its rank, so the moment its thread frees, its head
/// takes the next slot ahead of beta's and gamma's next.
#[tokio::test]
async fn a_thread_at_its_focus_cap_is_skipped_and_keeps_its_turn() {
    let env = env("rotation-focus-cap.db").await;
    full_fleet(&env).await;
    for (id, name) in [("ta", "alpha"), ("tb", "beta"), ("tc", "gamma")] {
        add_thread(&env, id, name).await;
    }
    add_started(&env, 150, Some("ta"), "running").await;
    caps(&env, 7, 1);
    let a = file_children(&env, "ta", ALPHA, 2).await;
    let b = file_children(&env, "tb", BETA, 2).await;
    let c = file_children(&env, "tc", GAMMA, 2).await;
    sweep_at(&env.orch, now()).await.unwrap();

    let first = release_one(&env, &[&env.db], "se100").await;
    assert_eq!(first, b[0], "alpha's head is at its cap, beta's goes");
    let second = release_one(&env, &[&env.db], &first).await;
    assert_eq!(second, c[0], "alpha still capped, gamma never served");
    assert!(is_held(&env, ALPHA + 1).await);

    // Alpha's open child ends: its thread frees, and so does a fleet slot.
    let third = release_one(&env, &[&env.db], "se150").await;
    assert_eq!(third, a[0], "alpha kept its rank: never served");
    assert!(is_held(&env, BETA + 2).await && is_held(&env, GAMMA + 2).await);
}

/// A thread's own `order` and a head filing change only that thread's
/// sequence: every other lane's rows keep their positions.
#[tokio::test]
async fn a_threads_own_order_holds_inside_its_lane() {
    let Lanes { env, a, b, c } = lanes("rotation-own-order.db", (5, 2, 1), 50).await;
    let positions = |view: &[String], ids: &[String]| -> Vec<usize> {
        ids.iter()
            .map(|id| view.iter().position(|listed| listed == id).unwrap())
            .collect()
    };
    let before = view_ids(&env).await;

    order(&env, "ta", "alpha", json!([ALPHA + 4, ALPHA + 2]))
        .await
        .unwrap();
    let after = view_ids(&env).await;
    assert_eq!(positions(&after, &b), positions(&before, &b));
    assert_eq!(positions(&after, &c), positions(&before, &c));
    let alpha: Vec<&String> = after.iter().filter(|id| a.contains(id)).collect();
    assert_eq!(
        alpha[..2],
        [&a[3], &a[1]],
        "alpha's own order leads its lane"
    );

    add_issue(&env, ALPHA + 6, Some("ta")).await;
    let filing = crate::mcp::handlers::executions::resolve_filing(
        &env.orch,
        "prj",
        ALPHA + 6,
        Some("memory-triage"),
        Default::default(),
        None,
    )
    .await
    .unwrap();
    crate::mcp::handlers::executions::start_execution_from_collection(
        &env.orch,
        "prj",
        ALPHA + 6,
        filing,
        None,
        None,
        None,
        LaunchIntent::NewAttempt,
        Vec::new(),
        QueuePlace::Head {
            placed_by: "alpha".to_string(),
        },
    )
    .await
    .unwrap();
    let head = execution(&env, ALPHA + 6).await.0;
    sweep_at(&env.orch, now()).await.unwrap();
    let filed = view_ids(&env).await;
    assert_eq!(positions(&filed, &b), positions(&before, &b));
    assert_eq!(positions(&filed, &c), positions(&before, &c));
    assert_eq!(
        filed.iter().find(|id| a.contains(id) || **id == head),
        Some(&head),
        "a head filing leads its lane"
    );
}

/// A move to 5 freezes the four rows ahead of it, and the sweeps release the
/// five in exactly the page's order. A later move to 1 is the next release.
/// An order naming a frozen row says so; one naming the placed row says whose
/// place it is.
#[tokio::test]
async fn an_operator_pinned_row_releases_at_its_pinned_position() {
    let Lanes { env, a, .. } = lanes("rotation-pinned.db", (8, 3, 1), 50).await;
    let moved = operator_move(&env.orch, &a[6], 5).await.unwrap();
    assert_eq!(moved.position, 5);
    let page = view_ids(&env).await;
    assert_eq!(page[4], a[6]);
    assert_eq!(page[0], a[0]);

    let refused = order(&env, "ta", "alpha", json!([ALPHA + 1]))
        .await
        .unwrap_err();
    assert_eq!(
        refused,
        vec![format!(
            "{} is held at its place ahead of an operator move until it starts",
            ALPHA + 1
        )]
    );
    let refused = order(&env, "ta", "alpha", json!([ALPHA + 7]))
        .await
        .unwrap_err();
    assert_eq!(
        refused,
        vec![format!("{}'s place is the operator's", ALPHA + 7)]
    );

    let mut running = "se100".to_string();
    let mut released = Vec::new();
    for _ in 0..5 {
        running = release_one(&env, &[&env.db], &running).await;
        released.push(running.clone());
    }
    assert_eq!(released, page[..5]);

    let fourth = view_ids(&env).await[3].clone();
    operator_move(&env.orch, &fourth, 1).await.unwrap();
    assert_eq!(view_ids(&env).await[0], fourth);
    assert_eq!(release_one(&env, &[&env.db], &running).await, fourth);
}

/// Pins, three lane kinds (threads, a parent issue's children, top-level
/// issues) and two databases.
async fn mixed(name: &str) -> (Env, Arc<LocalDb>) {
    let env = env(&format!("{name}.db")).await;
    caps(&env, 6, 50);
    full_fleet(&env).await;
    // A team project routes by its ids' `teamgate~` prefix, so its filings
    // land in the replica.
    let team = attach_team(&env, &format!("{name}-team.db")).await;
    team.execute_script(format!(
        "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
           VALUES('{TEAM_PROJECT}', 'default', 'Team queue', 'tq', '/tmp/team', 1, 1);
         INSERT INTO threads(id, project_id, name, created_at, updated_at)
           VALUES('{TEAM_THREAD}', '{TEAM_PROJECT}', 'teamwork', 1, 1);
         INSERT INTO issues(id, project_id, number, title, status, progress, attention,
                            parent_thread_id, created_at, updated_at)
           VALUES('teamgate~00000000-0000-4000-8000-000000000011', '{TEAM_PROJECT}', 11,
                  'Team 11', 'active', 'active', 'none', '{TEAM_THREAD}', 1, 1),
                 ('teamgate~00000000-0000-4000-8000-000000000012', '{TEAM_PROJECT}', 12,
                  'Team 12', 'active', 'active', 'none', '{TEAM_THREAD}', 1, 1);"
    ))
    .await
    .unwrap();
    env.orch
        .db
        .set_route("tq", Some("teamgate".to_string()))
        .await;
    add_thread(&env, "ta", "alpha").await;
    file_children(&env, "ta", ALPHA, 3).await;
    for number in [1, 2] {
        file(&env, number, json!([])).await.unwrap();
    }
    for number in [701, 702] {
        add_issue(&env, number, None).await;
        file(&env, number, json!([])).await.unwrap();
    }
    for number in [11, 12] {
        let filing = crate::mcp::handlers::executions::resolve_filing(
            &env.orch,
            "tq",
            number,
            Some("memory-triage"),
            Default::default(),
            None,
        )
        .await
        .unwrap();
        crate::mcp::handlers::executions::start_execution_from_collection(
            &env.orch,
            "tq",
            number,
            filing,
            None,
            None,
            None,
            LaunchIntent::NewAttempt,
            Vec::new(),
            QueuePlace::Tail,
        )
        .await
        .unwrap();
    }
    sweep_at(&env.orch, now()).await.unwrap();
    let page = view_ids(&env).await;
    assert_eq!(page.len(), 9, "{page:?}");
    operator_move(&env.orch, &page[6], 3).await.unwrap();
    (env, team)
}

#[tokio::test]
async fn the_queue_page_numbering_is_the_release_sequence() {
    let (env, team) = mixed("rotation-numbering").await;
    let page = view_ids(&env).await;
    let mut running = "se100".to_string();
    let mut released = Vec::new();
    for _ in 0..page.len() {
        let next = view_ids(&env).await[0].clone();
        running = release_one(&env, &[&env.db, &team], &running).await;
        assert_eq!(running, next, "the page's first is the next release");
        released.push(running.clone());
    }
    assert_eq!(released, page);
}

#[tokio::test]
async fn the_snapshot_and_the_page_read_one_order() {
    let (env, _team) = mixed("rotation-one-order").await;
    let page = fleet_order(&env.orch).await.unwrap();
    let ids: Vec<String> = page
        .iter()
        .map(|entry| entry.queued.execution_id.clone())
        .collect();
    let starts: Vec<Start<'_>> = page
        .iter()
        .map(|entry| Start {
            db: &entry.db,
            issue_id: entry.queued.issue_id.as_deref(),
            execution_id: Some(&entry.queued.execution_id),
        })
        .collect();
    let snapshot = QueueSnapshot::load(&env.orch, &starts).await.unwrap();
    assert_eq!(snapshot.order(), ids);
    assert!(page.iter().any(|entry| entry.pinned), "the fixture pins");
    let lanes: std::collections::BTreeSet<String> =
        page.iter().map(|entry| entry.lane.clone()).collect();
    assert_eq!(
        lanes.into_iter().collect::<Vec<_>>(),
        vec![
            "issue:ip".to_string(),
            "shared".to_string(),
            "thread:ta".to_string(),
            format!("thread:{TEAM_THREAD}"),
        ]
    );
    assert!(
        page.iter().any(|entry| entry.db_index == 1),
        "the fixture spans two databases"
    );
}

/// The turn A1 spent is in the database, so after a restart B1, never served,
/// is next, though A2's key is older and an order held in memory would pick
/// A2.
#[tokio::test]
async fn rotation_survives_a_restart() {
    let Lanes { env, a, b, .. } = lanes("rotation-restart.db", (2, 1, 0), 50).await;
    let first = release_one(&env, &[&env.db], "se100").await;
    assert_eq!(first, a[0]);

    let env = restarted(env);
    caps(&env, 6, 50);
    let older = count(
        &env,
        &format!(
            "SELECT (SELECT queue_key FROM execution_gates WHERE execution_id = '{}'
                       AND json_extract(predicate_json, '$.fact') = 'fleet_slot')
                  < (SELECT queue_key FROM execution_gates WHERE execution_id = '{}'
                       AND json_extract(predicate_json, '$.fact') = 'fleet_slot')",
            a[1], b[0]
        ),
    )
    .await;
    assert_eq!(older, 1, "A2's key is older than B1's");
    assert_eq!(release_one(&env, &[&env.db], &first).await, b[0]);
}

/// Several free slots, four sweeps and two evaluations at once: each lane's
/// head is released once, with one job set, and the turn stamps are distinct
/// and increase in release order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn racing_sweeps_release_each_rotation_turn_once() {
    let Lanes { env, a, b, c } = lanes("rotation-racing.db", (3, 2, 1), 50).await;
    for job in ["se100", "se101", "se102"] {
        end_jobs(&[&env.db], job).await;
    }
    let mut handles = Vec::new();
    for _ in 0..4 {
        let orch = env.orch.clone();
        handles.push(tokio::spawn(
            async move { sweep(&orch).await.unwrap().released },
        ));
    }
    for _ in 0..2 {
        let (orch, id) = (env.orch.clone(), b[0].clone());
        handles.push(tokio::spawn(async move {
            if evaluate_execution(&orch, &id).await.unwrap().released {
                vec![id]
            } else {
                Vec::new()
            }
        }));
    }
    let mut released = Vec::new();
    for handle in handles {
        released.extend(handle.await.unwrap());
    }
    released.sort();
    let mut heads = vec![a[0].clone(), b[0].clone(), c[0].clone()];
    heads.sort();
    assert_eq!(released, heads, "each head once, nothing else");
    for id in &heads {
        assert_eq!(
            count(
                &env,
                &format!(
                    "SELECT COUNT(*) - COUNT(DISTINCT recipe_node_id) FROM jobs
                      WHERE execution_id = '{id}'"
                )
            )
            .await,
            0,
            "one job set"
        );
    }
    let turns = env
        .db
        .query_all(
            "SELECT lane, turn FROM queue_lane_turns ORDER BY turn",
            (),
            |row| Ok((row.text(0)?, row.i64(1)?)),
        )
        .await
        .unwrap();
    let lanes: Vec<&str> = turns.iter().map(|(lane, _)| lane.as_str()).collect();
    assert_eq!(lanes, vec!["thread:ta", "thread:tb", "thread:tc"]);
    assert!(
        turns.windows(2).all(|pair| pair[0].1 < pair[1].1),
        "{turns:?}"
    );
}
