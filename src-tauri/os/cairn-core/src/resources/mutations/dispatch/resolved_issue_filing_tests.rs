//! Filing an execution on a merged or closed issue is refused before anything
//! is written, by the same resolved-issue clause the gate sweep's release
//! predicate excludes by (`cairn_db::resolved_issue_sql!`). An execution held
//! BEFORE its issue resolved is untouched: the census still reads it as
//! `won't start`.
//!
use std::collections::BTreeSet;

use serde_json::json;

use super::thread_adoption_tests::{add_issue, request};
use crate::execution::gates::sweep::tests::{count, env, execution, Env};
use crate::mcp::types::{ChangeItem, ChangeMode};

async fn file(env: &Env, number: i32, dry_run: bool) -> Result<String, String> {
    let item = ChangeItem {
        target: format!("cairn://p/prj/{number}/executions"),
        mode: ChangeMode::Append,
        payload: Some(
            json!({"recipe": "memory-triage", "after":[{"fact":"clock","notBefore":"2099-01-01T00:00:00Z"}]}),
        ),
    };
    crate::resources::mutations::dispatch_resource_change(
        &env.orch,
        &request(None),
        0,
        &item,
        dry_run,
    )
    .await
    .map(|change| change.summary)
    .map_err(|failure| failure.error)
}

async fn resolve(env: &Env, number: i32, column: &str, status: &str) {
    env.db
        .execute_script(format!(
            "UPDATE issues SET status = '{status}', {column} = 5 WHERE id = 'si{number}';"
        ))
        .await
        .unwrap();
}

/// `(executions, gate rows)` of issue `number`, and every gate row anywhere.
async fn written(env: &Env, number: i32) -> (i64, i64, i64) {
    (
        count(
            env,
            &format!("SELECT COUNT(*) FROM executions WHERE issue_id = 'si{number}'"),
        )
        .await,
        count(
            env,
            &format!(
                "SELECT COUNT(*) FROM execution_gates g JOIN executions e ON e.id = g.execution_id \
                 WHERE e.issue_id = 'si{number}'"
            ),
        )
        .await,
        count(env, "SELECT COUNT(*) FROM execution_gates").await,
    )
}

/// The defect: on main both filings were accepted and answered `Queued`. Now
/// each is refused, preview and filing alike, naming the resolution, the
/// continuity route and the old head, and nothing is written.
#[tokio::test]
async fn filing_on_a_merged_or_closed_issue_is_refused_and_writes_nothing() {
    for (column, resolution) in [("closed_at", "closed"), ("merged_at", "merged")] {
        let env = env(&format!("file-on-{resolution}.db")).await;
        // An issue whose first execution minted a branch, and one with none.
        add_issue(&env, 310, None).await;
        env.db.execute_script("INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq) VALUES('se310','recipe','si310','p','complete',1,1); INSERT INTO jobs(id,execution_id,issue_id,project_id,node_name,status,created_at,updated_at) VALUES('sj310','se310','si310','p','builder','complete',1,1);").await.unwrap();
        env.db
            .execute_script(
                "UPDATE jobs SET branch = 'agent/prj-310-builder-0', uri_segment = 'builder'
                  WHERE id = 'sj310';",
            )
            .await
            .unwrap();
        add_issue(&env, 311, None).await;
        resolve(&env, 310, column, resolution).await;
        resolve(&env, 311, column, resolution).await;

        for (number, head) in [
            (
                310,
                "Start from its head: branch `agent/prj-310-builder-0` (node \
                 cairn://p/prj/310/1/builder).",
            ),
            (311, "Start from its head: <node URI, branch or commit>."),
        ] {
            let before = written(&env, number).await;
            for dry_run in [true, false] {
                let refusal = file(&env, number, dry_run)
                    .await
                    .expect_err("a filing on a resolved issue is refused");
                assert!(
                    refusal.starts_with(&format!(
                        "prj/{number} is {resolution}: an execution filed on it would never start"
                    )),
                    "{refusal}"
                );
                assert!(
                    refusal.contains("target:\"cairn://p/prj/issues\",mode:\"append\"")
                        && refusal.contains("execution:true")
                        && refusal.contains(&format!("Continues cairn://p/prj/{number}.")),
                    "the route is a new issue: {refusal}"
                );
                assert!(refusal.contains(head), "names the old head: {refusal}");
                assert!(refusal.ends_with("Nothing was filed."), "{refusal}");
                assert_eq!(
                    written(&env, number).await,
                    before,
                    "{resolution} {number} dry_run={dry_run}: no execution or gate row"
                );
            }
        }
    }
}

/// Unchanged: an execution filed while its issue was open and held there
/// reads `won't start` once the issue closes, and the next filing is refused
/// rather than queued beside it.
#[tokio::test]
async fn an_execution_held_before_its_issue_closed_still_reads_wont_start() {
    let env = env("held-then-closed.db").await;
    add_issue(&env, 320, None).await;

    let filed = file(&env, 320, false).await.unwrap();
    assert!(filed.contains("HELD"), "{filed}");
    let (execution_id, ..) = execution(&env, 320).await;

    resolve(&env, 320, "closed_at", "closed").await;
    assert_eq!(
        env.db
            .read(|conn| {
                let id = execution_id.clone();
                Box::pin(async move {
                    Ok(crate::execution::gates::census::census_for_executions_conn(
                        conn,
                        std::slice::from_ref(&id),
                    )
                    .await?
                    .remove(&id)
                    .unwrap()
                    .summary)
                })
            })
            .await
            .unwrap(),
        "won't start: prj/320 closed"
    );

    let before = written(&env, 320).await;
    assert!(file(&env, 320, false).await.is_err());
    assert_eq!(written(&env, 320).await, before);
}

/// Filing and release read one clause, so they agree row for row: a gated,
/// unreleased execution is held exactly when filing on its issue is allowed.
/// The status-only row shows both read the resolution timestamps, not
/// `status`.
#[tokio::test]
async fn filing_and_release_agree_on_which_issues_are_resolved() {
    let env = env("filing-release-agree.db").await;
    let states = [
        (330, "status = 'active'"),
        (331, "merged_at = 5"),
        (332, "closed_at = 5"),
        (333, "merged_at = 5, closed_at = 6"),
        (334, "status = 'closed'"),
    ];
    for (number, set) in states {
        add_issue(&env, number, None).await;
        env.db
            .execute_script(format!(
                "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
                   VALUES('ge{number}', 'recipe', 'si{number}', 'p', 'held', 1, 1);
                 INSERT INTO execution_gates(id, execution_id, seq, predicate_json, created_at)
                   VALUES('gg{number}', 'ge{number}', 0, '{{\"fact\":\"clock\"}}', 1);
                 UPDATE issues SET {set} WHERE id = 'si{number}';"
            ))
            .await
            .unwrap();
    }

    let held: BTreeSet<i64> = env
        .db
        .read(|conn| {
            Box::pin(
                async move { cairn_workflow::gates::store::list_held_executions_conn(conn).await },
            )
        })
        .await
        .unwrap()
        .into_iter()
        .filter_map(|held| held.issue_number)
        .collect();
    let mut fileable = BTreeSet::new();
    for (number, _) in states {
        if crate::mcp::handlers::executions::refuse_filing_on_resolved_issue(
            &env.orch, "prj", number,
        )
        .await
        .is_ok()
        {
            fileable.insert(i64::from(number));
        }
    }
    assert_eq!(held, BTreeSet::from([330, 334]));
    assert_eq!(
        fileable, held,
        "filing allows exactly the issues release would start"
    );
}
