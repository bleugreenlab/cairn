//! Admission of new PR-producing work, separate from running-work capacity.
//! Call under the capacity admission lock, even for a release's own launch.

use crate::storage::{DbResult, RowExt, TrackedConnection};
use cairn_db::turso::params;

/// Unresolved children with open PRs, in stable issue-number order.
pub async fn children_conn(
    conn: &TrackedConnection,
    thread_id: &str,
) -> DbResult<Vec<(i64, String)>> {
    let mut rows = conn
        .query(
            concat!(
                "SELECT i.number, i.id FROM issues i WHERE i.parent_thread_id = ?1 AND ",
                cairn_db::thread_open_pr_child_sql!(),
                " ORDER BY i.number"
            ),
            params![thread_id],
        )
        .await?;
    let mut children = Vec::new();
    while let Some(row) = rows.next().await? {
        children.push((row.i64(0)?, row.text(1)?));
    }
    Ok(children)
}

/// A refusal reason only for a PR-producing launch without an existing PR.
/// Operator Start now is checked by the caller; ordinary fresh release is not
/// an exemption. Resolving a hold repeats this read rather than latching it.
/// This bounds launches against already-open PRs, not reserved future PRs:
/// builders admitted below the bound can later publish enough PRs to exceed it.
/// `known` is the thread's facts as a pass already read them; its open-PR
/// children stand in for a fresh read only when it is this job's thread.
pub async fn launch_refusal_conn(
    conn: &TrackedConnection,
    job_id: &str,
    bound: i32,
    known: Option<&super::capacity::ThreadFacts>,
) -> DbResult<Option<String>> {
    let mut rows = conn
        .query(
            "SELECT i.id, t.id, t.name, j.recipe_node_id, j.execution_id
           FROM jobs j JOIN issues i ON i.id = j.issue_id
           JOIN threads t ON t.id = i.parent_thread_id
          WHERE j.id = ?1 AND i.merged_at IS NULL AND i.closed_at IS NULL",
            params![job_id],
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };
    let issue_id = row.text(0)?;
    let thread_id = row.text(1)?;
    let thread_name = row.text(2)?;
    let node_id = row.opt_text(3)?;
    let execution_id = row.opt_text(4)?;
    drop(rows);
    let children = match known.filter(|facts| facts.thread_id == thread_id) {
        Some(facts) => facts.open_prs.clone(),
        None => children_conn(conn, &thread_id).await?,
    };
    if children.iter().any(|(_, id)| id == &issue_id) || children.len() < bound.max(1) as usize {
        return Ok(None);
    }
    let (Some(node_id), Some(execution_id)) = (node_id, execution_id) else {
        return Ok(None);
    };
    if !cairn_workflow::inputs::node_ships_a_pr_conn(conn, &node_id, &execution_id).await? {
        return Ok(None);
    }
    let numbers = children
        .iter()
        .map(|(number, _)| number.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    Ok(Some(format!(
        "{thread_name} has {}/{} open PRs: {numbers}",
        children.len(),
        bound.max(1)
    )))
}

/// Live thread-policy holds on an execution (or one hold id), in release order.
/// Used by Start now without confusing job holds with unreleased gate rows.
pub async fn thread_holds(
    db: &crate::storage::LocalDb,
    execution_or_hold: &str,
) -> DbResult<Vec<(String, String)>> {
    let id = execution_or_hold.to_owned();
    db.read(move |conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    concat!(
            "SELECT supply_hold.id, COALESCE(supply_hold.observed_text, supply_hold.boundary_source)
             FROM provider_limit_holds supply_hold JOIN jobs j ON j.id=supply_hold.job_id
             JOIN executions e ON e.id=j.execution_id JOIN issues i ON i.id=e.issue_id
             WHERE (e.id=?1 OR supply_hold.id=?1)
               AND supply_hold.boundary_source IN ('thread_focus','thread_open_prs')
               AND i.merged_at IS NULL AND i.closed_at IS NULL AND ",
            cairn_db::live_execution_sql!(), " AND ",
            cairn_db::reentry_hold_owns_job_sql!(),
            " ORDER BY COALESCE(supply_hold.queue_key,supply_hold.created_at*1000000), supply_hold.id"
        ),
                    params![id],
                )
                .await?;
            let mut holds = Vec::new();
            while let Some(row) = rows.next().await? {
                holds.push((row.text(0)?, row.text(1)?));
            }
            Ok(holds)
        })
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::LocalDb;
    use serde_json::json;

    async fn refusal(db: &LocalDb, job: &str) -> Option<String> {
        let job = job.to_owned();
        db.read(move |conn| Box::pin(async move { launch_refusal_conn(conn, &job, 2, None).await }))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn acceptance_d_counts_unresolved_pr_children_and_rechecks_after_merge() {
        let db = crate::storage::migrated_test_db("thread-open-prs.db").await;
        let snapshot = json!({
            "recipe": {
                "id":"r", "name":"R", "trigger":"manual",
                "nodes":[
                    {"id":"builder","name":"Builder","nodeType":"agent","position":{"x":0,"y":0}},
                    {"id":"planner","name":"Planner","nodeType":"agent","position":{"x":0,"y":0}},
                    {"id":"pr","name":"PR","nodeType":"pr","position":{"x":0,"y":1}}
                ],
                "edges":[{"id":"out","edgeType":"context","sourceNodeId":"builder",
                    "sourceHandle":"context-out","targetNodeId":"pr","targetHandle":"context-in"}]
            },
            "agents":{}, "skills":{}, "tools":{},
            "triggerContext":{"projectId":"p","triggerType":"manual"}, "createdAt":0
        })
        .to_string();
        db.execute_script(
            "INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
               VALUES('p','default','P','p','/tmp/p',1,1);
             INSERT INTO threads(id,project_id,name,created_at,updated_at)
               VALUES('th','p','harness',1,1);",
        )
        .await
        .unwrap();
        for number in 1..=5 {
            db.execute_script(format!(
                "INSERT INTO issues(id,project_id,number,title,status,parent_thread_id,created_at,updated_at)
                   VALUES('i{number}','p',{number},'Child','active','th',1,1);
                 INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq)
                   VALUES('e{number}','r','i{number}','p','running',1,1);
                 INSERT INTO jobs(id,execution_id,recipe_node_id,project_id,issue_id,status,created_at,updated_at)
                   VALUES('j{number}','e{number}','builder','p','i{number}','pending',1,1);"
            )).await.unwrap();
            db.execute(
                "UPDATE executions SET snapshot=?1 WHERE id=?2",
                params![snapshot.clone(), format!("e{number}")],
            )
            .await
            .unwrap();
        }
        for number in [1, 2, 4, 5] {
            db.execute_script(format!(
                "INSERT INTO merge_requests(id,job_id,project_id,issue_id,title,source_branch,target_branch,status,opened_at,updated_at)
                   VALUES('mr{number}','j{number}','p','i{number}','PR','b','main','open',1,1);"
            )).await.unwrap();
        }
        db.execute_script(
            "UPDATE issues SET merged_at=5 WHERE id='i4';
             UPDATE issues SET closed_at=5 WHERE id='i5';
             INSERT INTO jobs(id,execution_id,project_id,issue_id,status,created_at,updated_at)
               VALUES('j2-other','e2','p','i2','complete',1,1);
             INSERT INTO merge_requests(id,job_id,project_id,issue_id,title,source_branch,target_branch,status,opened_at,updated_at)
               VALUES('mr2-other','j2-other','p','i2','Other PR','other','main','open',1,1);",
        )
        .await
        .unwrap();
        assert_eq!(
            refusal(&db, "j3").await.as_deref(),
            Some("harness has 2/2 open PRs: 1, 2")
        );
        assert_eq!(
            refusal(&db, "j1").await,
            None,
            "existing PR fixes are admitted"
        );
        db.execute_script("UPDATE jobs SET recipe_node_id='planner' WHERE id='j3';")
            .await
            .unwrap();
        assert_eq!(refusal(&db, "j3").await, None, "planners are admitted");
        db.execute_script(
            "UPDATE jobs SET recipe_node_id='builder' WHERE id='j3';
                           UPDATE merge_requests SET status='merged' WHERE id='mr1';",
        )
        .await
        .unwrap();
        assert_eq!(refusal(&db, "j3").await, None, "merge frees the bound");
        let children = db
            .read(|conn| Box::pin(async move { children_conn(conn, "th").await }))
            .await
            .unwrap();
        assert_eq!(children, vec![(2, "i2".into())]);
        db.execute_script(
            "INSERT INTO provider_limit_holds(id,job_id,backend,boundary_source,observed_text,kind,created_at,updated_at)
               VALUES('hold3','j3','claude','thread_open_prs','PR bound','launch',1,1);"
        ).await.unwrap();
        let holds = thread_holds(&db, "e3").await.unwrap();
        assert_eq!(holds, vec![("hold3".into(), "PR bound".into())]);
        assert_eq!(thread_holds(&db, "hold3").await.unwrap(), holds);
        db.execute_script("UPDATE jobs SET status='running' WHERE id='j3';")
            .await
            .unwrap();
        assert!(
            thread_holds(&db, "e3").await.unwrap().is_empty(),
            "a stale hold cannot authorize Start now"
        );
    }
}
