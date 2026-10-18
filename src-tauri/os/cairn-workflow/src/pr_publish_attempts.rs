//! PR-only publication attempts and revision-gated retries.
//! A retry keeps the action row (and URI); a manual retry starts a new bounded
//! cycle without deleting the terminal causes of earlier cycles.
use crate::advancement::{load_nodes_from_execution, run_advancement_db};
use crate::effects::types::{EffectContext, EffectSource, WorkflowEffect};
use crate::runtime::{WorkflowOwner, WorkflowRuntime};
use crate::storage::{DbResult, LocalDb, RowExt};
use cairn_db::turso::{params, Connection};

pub const PR_PUBLISH_MAX_ATTEMPTS: i64 = 5;

#[derive(Debug, Clone)]
pub struct PublishAttemptRow {
    pub id: String,
    pub action_run_id: String,
    pub cycle: i64,
    pub attempt: i64,
    pub input_revision: Option<String>,
    pub outcome: Option<String>,
    pub detail: Option<String>,
    pub started_at: i64,
    pub settled_at: Option<i64>,
}

pub fn is_pr_publish_retry_eligible(
    current_revision: Option<&str>,
    last_revision: Option<&str>,
    attempts: i64,
    cap: i64,
) -> bool {
    attempts < cap && matches!((current_revision, last_revision), (Some(a), Some(b)) if a != b)
}

pub async fn list_publish_attempts(
    db: &LocalDb,
    id: &str,
) -> Result<Vec<PublishAttemptRow>, String> {
    db.query_all(
        "SELECT id, action_run_id, cycle, attempt, input_revision, outcome, detail, started_at, settled_at
         FROM pr_publish_attempts WHERE action_run_id = ?1 ORDER BY cycle DESC, attempt DESC",
        (id.to_owned(),),
        |r| Ok(PublishAttemptRow { id:r.text(0)?, action_run_id:r.text(1)?, cycle:r.i64(2)?,
            attempt:r.i64(3)?, input_revision:r.opt_text(4)?, outcome:r.opt_text(5)?,
            detail:r.opt_text(6)?, started_at:r.i64(7)?, settled_at:r.opt_i64(8)? }),
    ).await.map_err(|e| e.to_string())
}

pub async fn latest_publish_attempt(
    db: &LocalDb,
    id: &str,
) -> Result<Option<PublishAttemptRow>, String> {
    Ok(list_publish_attempts(db, id).await?.into_iter().next())
}

pub async fn publish_attempt_count(db: &LocalDb, id: &str) -> Result<i64, String> {
    Ok(latest_publish_attempt(db, id)
        .await?
        .map_or(0, |a| a.attempt))
}

pub async fn settle_publish_attempt_conn(
    conn: &Connection,
    id: &str,
    outcome: &str,
    detail: &str,
) -> DbResult<()> {
    conn.execute(
        "UPDATE pr_publish_attempts SET outcome = ?2, detail = ?3, settled_at = ?4
         WHERE id = (SELECT id FROM pr_publish_attempts WHERE action_run_id = ?1 ORDER BY cycle DESC, attempt DESC LIMIT 1)
         AND outcome IS NULL",
        params![id, outcome, detail, chrono::Utc::now().timestamp()],
    ).await?;
    Ok(())
}

pub async fn settle_publish_attempt(
    db: &LocalDb,
    id: &str,
    outcome: &str,
    detail: &str,
) -> Result<(), String> {
    db.write(|conn| {
        let (id, outcome, detail) = (id.to_owned(), outcome.to_owned(), detail.to_owned());
        Box::pin(async move { settle_publish_attempt_conn(conn, &id, &outcome, &detail).await })
    })
    .await
    .map_err(|e| e.to_string())
}

pub(crate) async fn insert_attempt(conn: &Connection, a: &PublishAttemptRow) -> DbResult<()> {
    conn.execute(
        "INSERT INTO pr_publish_attempts(id,action_run_id,cycle,attempt,input_revision,started_at)
        VALUES(?1,?2,?3,?4,?5,?6)",
        params![
            a.id.as_str(),
            a.action_run_id.as_str(),
            a.cycle,
            a.attempt,
            a.input_revision.as_deref(),
            a.started_at
        ],
    )
    .await?;
    Ok(())
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RearmedPrAction {
    pub action_run_id: String,
    pub execution_id: String,
    pub node_id: String,
    pub attempt_id: String,
    pub created_at_millis: i64,
}
impl RearmedPrAction {
    pub fn effect(&self) -> WorkflowEffect {
        WorkflowEffect::ExecuteAction {
            action_run_id: self.action_run_id.clone(),
            execution_id: self.execution_id.clone(),
            node_id: self.node_id.clone(),
            ctx: EffectContext {
                job_id: None,
                run_id: None,
                execution_id: Some(self.execution_id.clone()),
                source: EffectSource::DagAdvancement,
            },
        }
    }
}

/// Persist before claiming the owning row: private and team databases cannot
/// share a transaction. An unclaimed intent is harmless on replay because its
/// attempt id must match the journal. A successful claim always has an intent.
async fn persist_dispatch(
    runtime: &WorkflowRuntime,
    action: &RearmedPrAction,
) -> Result<(), String> {
    crate::effects::outbox::insert_pending_with_payload_async(
        &runtime.private_db,
        "pr_publish",
        &action.attempt_id,
        &serde_json::to_string(action).map_err(|e| e.to_string())?,
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(())
}

async fn action_fields(db: &LocalDb, id: &str) -> Result<(String, String, String, String), String> {
    db.query_one(
        "SELECT execution_id,recipe_node_id,status,action_config_id FROM action_runs WHERE id = ?1",
        (id.to_owned(),),
        |r| Ok((r.text(0)?, r.text(1)?, r.text(2)?, r.text(3)?)),
    )
    .await
    .map_err(|e| e.to_string())
}

async fn is_live_conn(conn: &Connection, execution: &str) -> DbResult<bool> {
    let mut rows = conn
        .query(
            concat!(
                "SELECT COUNT(*) FROM executions e WHERE e.id = ?1 AND ",
                cairn_db::live_execution_sql!()
            ),
            (execution,),
        )
        .await?;
    Ok(rows
        .next()
        .await?
        .is_some_and(|r| r.i64(0).unwrap_or(0) > 0))
}

pub async fn validate_pr_publish_retry(runtime: &WorkflowRuntime, id: &str) -> Result<(), String> {
    let db = runtime.database(WorkflowOwner::ActionRun(id))?;
    let (execution, node, status, kind) = action_fields(&db, id).await?;
    let nodes = load_nodes_from_execution(db.clone(), &execution)?;
    if kind != "builtin:pr" || !nodes.iter().any(|n| n.id == node && n.node_type == "pr") {
        return Err("Only a PR action node can retry publication".into());
    }
    if status == "blocked" || status == "complete" {
        return Err("This PR is already published; use refresh instead of retry".into());
    }
    if status != "failed" {
        return Err("Only a failed PR action can retry publication".into());
    }
    let live = db
        .read(|conn| {
            let execution = execution.clone();
            Box::pin(async move { is_live_conn(conn, &execution).await })
        })
        .await
        .map_err(|e| e.to_string())?;
    if !live {
        return Err("Cannot retry a superseded execution or a resolved issue".into());
    }
    Ok(())
}

fn new_attempt(
    id: &str,
    revision: Option<String>,
    last: Option<&PublishAttemptRow>,
    manual: bool,
) -> PublishAttemptRow {
    PublishAttemptRow {
        id: cairn_common::ids::mint_child(id),
        action_run_id: id.to_owned(),
        cycle: last.map_or(1, |a| a.cycle + i64::from(manual)),
        attempt: if manual {
            1
        } else {
            last.map_or(1, |a| a.attempt + 1)
        },
        input_revision: revision,
        outcome: None,
        detail: None,
        started_at: chrono::Utc::now().timestamp(),
        settled_at: None,
    }
}

/// One conditional claim is shared by manual and revision-triggered retries.
async fn rearm(
    runtime: &WorkflowRuntime,
    id: &str,
    manual: bool,
) -> Result<Option<RearmedPrAction>, String> {
    if let Err(error) = validate_pr_publish_retry(runtime, id).await {
        if manual {
            return Err(error);
        }
        return Ok(None);
    }
    let db = runtime.database(WorkflowOwner::ActionRun(id))?;
    let (execution, node, _, _) = action_fields(&db, id).await?;
    let last = latest_publish_attempt(&db, id).await?;
    let revision = runtime.host.pr_publish_input_revision(id)?;
    if !manual
        && !is_pr_publish_retry_eligible(
            revision.as_deref(),
            last.as_ref().and_then(|a| a.input_revision.as_deref()),
            last.as_ref().map_or(0, |a| a.attempt),
            PR_PUBLISH_MAX_ATTEMPTS,
        )
    {
        return Ok(None);
    }
    let attempt = new_attempt(id, revision, last.as_ref(), manual);
    let dispatch = RearmedPrAction {
        action_run_id: id.into(),
        execution_id: execution.clone(),
        node_id: node,
        attempt_id: attempt.id.clone(),
        created_at_millis: chrono::Utc::now().timestamp_millis(),
    };
    persist_dispatch(runtime, &dispatch).await?;
    let claimed = db.write(|conn| {
        let (id, execution, attempt, expected) = (id.to_owned(),execution.clone(),attempt.clone(),last.as_ref().map(|a| a.id.clone()));
        Box::pin(async move {
            if !is_live_conn(conn,&execution).await? { return Ok(false); }
            let affected = conn.execute(
                "UPDATE action_runs SET status='pending',error_message=NULL,completed_at=NULL,output=NULL,started_at=?2
                 WHERE id=?1 AND status='failed' AND action_config_id='builtin:pr'
                 AND COALESCE((SELECT id FROM pr_publish_attempts WHERE action_run_id=?1 ORDER BY cycle DESC,attempt DESC LIMIT 1),'')=COALESCE(?3,'')",
                params![id.as_str(),attempt.started_at,expected.as_deref()],
            ).await?;
            if affected == 0 { return Ok(false); }
            insert_attempt(conn,&attempt).await?;
            Ok(true)
        })
    }).await;
    let claimed = match claimed {
        Ok(claimed) => claimed,
        Err(error) => {
            finish_dispatch_intent(runtime, &dispatch.attempt_id).await?;
            return Err(error.to_string());
        }
    };
    if !claimed {
        finish_dispatch_intent(runtime, &dispatch.attempt_id).await?;
        return Ok(None);
    }
    if let Err(error) = crate::advancement::recompute_execution_jobs(runtime, &execution) {
        log::warn!("Could not refresh execution projection after PR retry claim: {error}");
    }
    let _ = runtime.emitter.emit(
        "db-change",
        serde_json::json!({"table":"action_runs","action":"update"}),
    );
    Ok(Some(dispatch))
}

pub fn rearm_failed_pr_actions(
    runtime: &WorkflowRuntime,
    execution: &str,
) -> Result<Vec<RearmedPrAction>, String> {
    let runtime = runtime.clone();
    let execution = execution.to_owned();
    run_advancement_db(async move {
        let db = runtime.database(WorkflowOwner::Execution(&execution))?;
        let nodes = load_nodes_from_execution(db.clone(), &execution)?;
        let ids = db.query_all("SELECT id,recipe_node_id FROM action_runs WHERE execution_id=?1 AND status='failed' AND action_config_id='builtin:pr'",
            (execution,),|r| Ok((r.text(0)?,r.text(1)?))).await.map_err(|e|e.to_string())?;
        let mut out = Vec::new();
        for (id, node) in ids {
            if !nodes.iter().any(|n| n.id == node && n.node_type == "pr") {
                continue;
            }
            // A concurrent pass may have won the claim since enumeration.
            if validate_pr_publish_retry(&runtime, &id).await.is_err() {
                continue;
            }
            if let Some(action) = rearm(&runtime, &id, false).await? {
                out.push(action);
            }
        }
        Ok(out)
    })
}

pub async fn retry_pr_publish(runtime: &WorkflowRuntime, id: &str) -> Result<(), String> {
    let dispatch = rearm(runtime, id, true)
        .await?
        .ok_or("PR retry was already claimed")?;
    if let Some(tx) = &runtime.effect_tx {
        let _ = tx.send(dispatch.effect());
    }
    Ok(())
}

/// The intent precedes the action row. The caller inserts this baseline in the
/// same transaction as the row, so no visible PR action lacks a journal.
pub(crate) fn prepare_initial_publish_attempt(
    runtime: &WorkflowRuntime,
    id: &str,
    execution: &str,
    node: &str,
) -> Result<PublishAttemptRow, String> {
    let attempt = new_attempt(id, None, None, false);
    let dispatch = RearmedPrAction {
        action_run_id: id.to_owned(),
        execution_id: execution.to_owned(),
        node_id: node.to_owned(),
        attempt_id: attempt.id.clone(),
        created_at_millis: chrono::Utc::now().timestamp_millis(),
    };
    let runtime = runtime.clone();
    run_advancement_db(async move { persist_dispatch(&runtime, &dispatch).await })?;
    Ok(attempt)
}

/// Resolve only an initial, not-yet-dispatched baseline. Unknown revisions stay
/// conservative; failing to resolve a revision must not strand the durable action.
pub(crate) fn capture_initial_publish_revision(runtime: &WorkflowRuntime, id: &str) {
    let result = (|| {
        let revision = runtime.host.pr_publish_input_revision(id)?;
        let db = runtime.database(WorkflowOwner::ActionRun(id))?;
        let id = id.to_owned();
        run_advancement_db(async move {
            db.execute("UPDATE pr_publish_attempts SET input_revision=?2 WHERE action_run_id=?1 AND cycle=1 AND attempt=1 AND input_revision IS NULL AND dispatched_at IS NULL AND outcome IS NULL",
                params![id,revision]).await.map_err(|e|e.to_string())?;
            Ok(())
        })
    })();
    if let Err(error) = result {
        log::warn!("Could not capture initial PR publication revision: {error}");
    }
}

/// Only the winner of pending -> running may cross the host boundary. Legacy
/// actions without a PR journal retain their existing execution semantics.
pub async fn claim_dispatch(runtime: &WorkflowRuntime, id: &str) -> Result<bool, String> {
    let db = runtime.database(WorkflowOwner::ActionRun(id))?;
    if latest_publish_attempt(&db, id).await?.is_some_and(|a| {
        a.cycle == 1 && a.attempt == 1 && a.input_revision.is_none() && a.outcome.is_none()
    }) {
        capture_initial_publish_revision(runtime, id);
    }
    db.write(|conn| {
        let id = id.to_owned();
        Box::pin(async move {
            let mut rows = conn.query("SELECT COUNT(*) FROM pr_publish_attempts WHERE action_run_id=?1",(id.as_str(),)).await?;
            if rows.next().await?.unwrap().i64(0)? == 0 { return Ok(true); }
            let mut rows = conn.query("SELECT execution_id FROM action_runs WHERE id=?1",(id.as_str(),)).await?;
            let Some(row) = rows.next().await? else {return Ok(false);};
            if !is_live_conn(conn,&row.text(0)?).await? {return Ok(false);}
            let claimed = conn.execute("UPDATE action_runs SET status='running' WHERE id=?1 AND status='pending'",(id.as_str(),)).await? == 1;
            if claimed {
                conn.execute("UPDATE pr_publish_attempts SET dispatched_at=?2 WHERE action_run_id=?1 AND outcome IS NULL",
                    params![id.as_str(),chrono::Utc::now().timestamp_millis()]).await?;
            }
            Ok(claimed)
        })
    }).await.map_err(|e| e.to_string())
}

/// Startup-only replay. Generation checks discard orphaned intents and earlier
/// cycles. A running attempt belonged to the previous process and is reclaimable.
pub async fn replay_dispatch(
    runtime: &WorkflowRuntime,
    payload: &str,
    boot_at_millis: i64,
) -> Result<Option<WorkflowEffect>, String> {
    let action: RearmedPrAction =
        serde_json::from_str(payload).map_err(|e| format!("Invalid PR dispatch: {e}"))?;
    // Startup runs after ingress opens. An intent created by this host may
    // still be between outbox persistence and the owning-database claim.
    if action.created_at_millis >= boot_at_millis {
        return Ok(None);
    }
    let db = runtime.database(WorkflowOwner::ActionRun(&action.action_run_id))?;
    let last = latest_publish_attempt(&db, &action.action_run_id).await?;
    if last
        .as_ref()
        .is_none_or(|a| a.id != action.attempt_id || a.outcome.is_some())
    {
        finish_dispatch_intent(runtime, &action.attempt_id).await?;
        return Ok(None);
    }
    let replay = db.write(|conn| {
        let action = action.clone();
        Box::pin(async move {
            if !is_live_conn(conn,&action.execution_id).await? {return Ok((false,true));}
            let claimed = conn.execute(
                "UPDATE action_runs SET status='pending' WHERE id=?1 AND
                 (SELECT id FROM pr_publish_attempts WHERE action_run_id=?1 ORDER BY cycle DESC,attempt DESC LIMIT 1)=?2 AND
                 (status='pending' OR (status='running' AND
                 (SELECT dispatched_at FROM pr_publish_attempts WHERE id=?2) < ?3))",
                params![action.action_run_id.as_str(),action.attempt_id,boot_at_millis],
            ).await? == 1;
            let mut rows = conn.query("SELECT status FROM action_runs WHERE id=?1",(action.action_run_id,)).await?;
            let terminal = rows.next().await?.map(|r|r.text(0)).transpose()?.is_none_or(|s| s != "pending" && s != "running");
            Ok((claimed,terminal))
        })
    }).await.map_err(|e|e.to_string())?;
    if replay.1 {
        finish_dispatch_intent(runtime, &action.attempt_id).await?;
    }
    // A running claim created by this host is not replayed or acknowledged.
    // Its original live delivery still owns the intent until settlement.
    Ok(replay.0.then(|| action.effect()))
}

pub(crate) async fn finish_dispatch_intent(
    runtime: &WorkflowRuntime,
    attempt_id: &str,
) -> Result<(), String> {
    crate::effects::outbox::mark_done_by_key_async(&runtime.private_db, "pr_publish", attempt_id)
        .await
        .map_err(|e| e.to_string())
}

pub async fn acknowledge_settled_dispatch(
    runtime: &WorkflowRuntime,
    id: &str,
) -> Result<(), String> {
    let db = runtime.database(WorkflowOwner::ActionRun(id))?;
    if let Some(last) = latest_publish_attempt(&db, id).await? {
        if last.outcome.is_some() {
            finish_dispatch_intent(runtime, &last.id).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn pr_publish_retry_requires_known_changed_revision_below_cap() {
        assert!(is_pr_publish_retry_eligible(Some("new"), Some("old"), 1, 5));
        assert!(!is_pr_publish_retry_eligible(
            Some("same"),
            Some("same"),
            1,
            5
        ));
        assert!(!is_pr_publish_retry_eligible(
            Some("new"),
            Some("old"),
            5,
            5
        ));
        assert!(!is_pr_publish_retry_eligible(None, Some("old"), 1, 5));
        assert!(!is_pr_publish_retry_eligible(Some("new"), None, 1, 5));
        assert!(!is_pr_publish_retry_eligible(None, None, 0, 5));
    }
}
