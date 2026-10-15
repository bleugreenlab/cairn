use crate::storage::{DbError, LocalDb, RowExt};
use cairn_db::turso::params;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    sync::{Mutex, OnceLock},
};
use tokio::sync::mpsc;

use crate::runtime::AttentionRuntime;
use cairn_domain::DeliveryUrgency;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WakeCondition {
    pub id: String,
    pub home_kind: String,
    pub home_id: String,
    pub sql: String,
    pub reason: String,
    pub once: bool,
    pub state: String,
    pub last_outcome: Option<String>,
    pub last_error: Option<String>,
    pub last_fired_at: Option<i64>,
    pub last_evaluated_at: Option<i64>,
    pub created_by: String,
    pub created_at: i64,
    pub updated_at: i64,
}

static NOTIFY: OnceLock<Mutex<Option<mpsc::UnboundedSender<String>>>> = OnceLock::new();

fn notifier() -> &'static Mutex<Option<mpsc::UnboundedSender<String>>> {
    NOTIFY.get_or_init(|| Mutex::new(None))
}

/// Announce that a write to `thread_id`'s store has committed.
///
/// Every committed write to a thread database must call this, whatever surface
/// performed it. A condition is arbitrary read-only SQL over the whole store,
/// including the app-contract tables a thread never writes with SQL, so the
/// obligation belongs to the act of committing rather than to any one write
/// path. A commit that skips the call is indistinguishable from one that never
/// happened to an armed condition, and nothing at the call site shows it.
pub fn notify_thread_db_written(thread_id: &str) {
    if let Ok(sender) = notifier().lock() {
        if let Some(sender) = sender.as_ref() {
            let _ = sender.send(thread_id.to_string());
        }
    }
}

pub fn install_evaluator(runtime: AttentionRuntime) {
    let (tx, mut rx) = mpsc::unbounded_channel();
    if let Ok(mut sender) = notifier().lock() {
        *sender = Some(tx);
    }
    tokio::spawn(async move {
        while let Some(first) = rx.recv().await {
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            let mut threads = HashSet::from([first]);
            while let Ok(thread_id) = rx.try_recv() {
                threads.insert(thread_id);
            }
            for thread_id in threads {
                if let Err(error) = evaluate_thread_conditions(&runtime, &thread_id).await {
                    tracing::error!(%thread_id, %error, "failed to evaluate thread wake conditions");
                }
            }
        }
    });
}

pub async fn evaluate_thread_conditions(
    runtime: &AttentionRuntime,
    thread_id: &str,
) -> Result<Vec<(String, ConditionOutcome, bool)>, String> {
    let conditions = load_active_wake_conditions(&runtime.private_db, thread_id).await?;
    let mut results = Vec::with_capacity(conditions.len());
    for condition in conditions {
        let outcome = runtime.host.evaluate_condition(&condition).await;
        let fired = if should_fire(&condition, &outcome) {
            fire_condition(runtime, &condition, &outcome).await?
        } else {
            record_quiet_outcome(&runtime.private_db, &condition, &outcome).await?;
            false
        };
        results.push((condition.id, outcome, fired));
    }
    Ok(results)
}

async fn record_quiet_outcome(
    db: &LocalDb,
    condition: &WakeCondition,
    outcome: &ConditionOutcome,
) -> Result<(), String> {
    let id = condition.id.clone();
    let condition_last_outcome = condition.last_outcome.clone();
    let condition_last_error = condition.last_error.clone();
    let outcome_name = outcome.name().to_string();
    let error = outcome.error().map(str::to_string);
    db.write(move |conn| {
        let id = id.clone();
        let outcome_name = outcome_name.clone();
        let error = error.clone();
        let condition_last_outcome = condition_last_outcome.clone();
        let condition_last_error = condition_last_error.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE wake_conditions
             SET last_outcome = ?1, last_error = ?2,
                 last_evaluated_at = unixepoch(), updated_at = unixepoch()
             WHERE id = ?3 AND state = 'active'
               AND last_outcome IS ?4 AND last_error IS ?5",
                params![
                    outcome_name,
                    error,
                    id,
                    condition_last_outcome,
                    condition_last_error
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|error| error.to_string())
}

async fn fire_condition(
    runtime: &AttentionRuntime,
    condition: &WakeCondition,
    outcome: &ConditionOutcome,
) -> Result<bool, String> {
    let condition = condition.clone();
    let outcome = outcome.clone();
    let host = runtime.host.clone();
    let result = runtime.private_db.write(move |conn| {
        let host = host.clone();
        let condition = condition.clone();
        let outcome = outcome.clone();
        Box::pin(async move {
        let mut rows = conn.query(
            "SELECT state, last_outcome, last_error FROM wake_conditions WHERE id = ?1 AND home_kind = 'thread' AND home_id = ?2",
            params![condition.id.as_str(), condition.home_id.as_str()],
        ).await?;
        let Some(row) = rows.next().await? else { return Ok(None) };
        let state = row.text(0)?;
        let last_outcome = row.opt_text(1)?;
        let last_error = row.opt_text(2)?;
        drop(rows);
        if state != "active"
            || last_outcome != condition.last_outcome
            || last_error != condition.last_error
        {
            return Ok(None);
        }
        let mut thread = conn.query("SELECT status FROM threads WHERE id = ?1", params![condition.home_id.as_str()]).await?;
        let status = thread.next().await?.map(|row| row.text(0)).transpose()?;
        drop(thread);
        if status.as_deref() != Some("active") {
            conn.execute("UPDATE wake_conditions SET last_evaluated_at = unixepoch(), updated_at = unixepoch() WHERE id = ?1", params![condition.id.as_str()]).await?;
            return Ok(None);
        }
        let job_id = host.ensure_thread_session(conn, &condition.home_id).await?;
        let mut runs = conn.query("SELECT id FROM runs WHERE job_id = ?1 ORDER BY created_at DESC LIMIT 1", params![job_id.as_str()]).await?;
        let run_id = runs.next().await?.map(|row| row.text(0)).transpose()?;
        drop(runs);
        let Some(run_id) = run_id else {
            conn.execute("UPDATE wake_conditions SET last_evaluated_at = unixepoch(), updated_at = unixepoch() WHERE id = ?1", params![condition.id.as_str()]).await?;
            return Ok(None);
        };
        let content = match &outcome {
            ConditionOutcome::Matched { rows, shown, total } => format!(
                "{}\n\nCondition `{}` matched — {} rows matched, {} shown:\n{}",
                condition.reason,
                condition.id,
                total.map_or_else(|| "more than the display limit".into(), |n| n.to_string()),
                shown,
                rows
            ),
            ConditionOutcome::Error(error) => format!(
                "{}\n\nCondition `{}` could not be evaluated: {}\nIt is still armed and will report again only if the error changes.",
                condition.reason, condition.id, error
            ),
            ConditionOutcome::Empty => return Ok(None),
        };
        let key = format!("condition:{}", condition.id);
        crate::direct::insert_system_direct_push_with_key_conn(
            conn, &job_id, &run_id, &content, DeliveryUrgency::Steer, Some(&key),
        ).await?;
        conn.execute(
            "UPDATE wake_conditions SET last_outcome = ?1, last_error = ?2, last_fired_at = unixepoch(), last_evaluated_at = unixepoch(), state = CASE WHEN once = 1 THEN 'spent' ELSE state END, updated_at = unixepoch() WHERE id = ?3",
            params![outcome.name(), outcome.error(), condition.id.as_str()],
        ).await?;
        Ok(Some(job_id))
    })}).await.map_err(|error| error.to_string())?;
    if let Some(job_id) = result {
        runtime.host.nudge(&job_id, DeliveryUrgency::Steer)?;
        Ok(true)
    } else {
        Ok(false)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConditionOutcome {
    Matched {
        rows: String,
        shown: usize,
        total: Option<usize>,
    },
    Empty,
    Error(String),
}

impl ConditionOutcome {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Matched { .. } => "matched",
            Self::Empty => "empty",
            Self::Error(_) => "error",
        }
    }

    pub fn error(&self) -> Option<&str> {
        match self {
            Self::Error(error) => Some(error),
            _ => None,
        }
    }
}

/// Decide whether an observed outcome is news relative to the persisted edge
/// cursor. The cursor is advanced only after a wake is successfully delivered.
fn should_fire(condition: &WakeCondition, outcome: &ConditionOutcome) -> bool {
    match outcome {
        ConditionOutcome::Matched { .. } => condition.last_outcome.as_deref() != Some("matched"),
        ConditionOutcome::Empty => false,
        ConditionOutcome::Error(error) => {
            condition.last_outcome.as_deref() != Some("error")
                || condition.last_error.as_deref() != Some(error.as_str())
        }
    }
}

const SELECT_CONDITION: &str = "SELECT id, home_kind, home_id, sql, reason, once, state, last_outcome, last_error, last_fired_at, last_evaluated_at, created_by, created_at, updated_at FROM wake_conditions";

fn condition_from_row(row: &cairn_db::turso::Row) -> Result<WakeCondition, DbError> {
    Ok(WakeCondition {
        id: row.text(0)?,
        home_kind: row.text(1)?,
        home_id: row.text(2)?,
        sql: row.text(3)?,
        reason: row.text(4)?,
        once: row.i64(5)? != 0,
        state: row.text(6)?,
        last_outcome: row.opt_text(7)?,
        last_error: row.opt_text(8)?,
        last_fired_at: row.opt_i64(9)?,
        last_evaluated_at: row.opt_i64(10)?,
        created_by: row.text(11)?,
        created_at: row.i64(12)?,
        updated_at: row.i64(13)?,
    })
}

pub async fn create_wake_condition(
    db: &LocalDb,
    thread_id: &str,
    sql: &str,
    reason: &str,
    once: bool,
    created_by: &str,
) -> Result<WakeCondition, String> {
    if sql.trim().is_empty() {
        return Err("Condition SQL cannot be empty".into());
    }
    if reason.trim().is_empty() {
        return Err("Condition reason cannot be empty".into());
    }
    if !matches!(created_by, "agent" | "user" | "system") {
        return Err("Condition creator must be agent, user, or system".into());
    }
    let condition = WakeCondition {
        id: uuid::Uuid::new_v4().to_string(),
        home_kind: "thread".into(),
        home_id: thread_id.to_string(),
        sql: sql.trim().to_string(),
        reason: reason.trim().to_string(),
        once,
        state: "active".into(),
        last_outcome: None,
        last_error: None,
        last_fired_at: None,
        last_evaluated_at: None,
        created_by: created_by.to_string(),
        created_at: chrono::Utc::now().timestamp(),
        updated_at: chrono::Utc::now().timestamp(),
    };
    let saved = condition.clone();
    db.write(move |conn| {
        let saved = saved.clone();
        Box::pin(async move {
            let mut rows = conn.query("SELECT status FROM threads WHERE id = ?1", params![saved.home_id.as_str()]).await?;
            let status = rows.next().await?.map(|row| row.text(0)).transpose()?;
            drop(rows);
            if status.as_deref() != Some("active") {
                return Err(DbError::Internal(format!("thread is not active: {}", saved.home_id)));
            }
            conn.execute(
                "INSERT INTO wake_conditions (id, home_kind, home_id, sql, reason, once, state, created_by, created_at, updated_at) VALUES (?1, 'thread', ?2, ?3, ?4, ?5, 'active', ?6, ?7, ?7)",
                params![saved.id.as_str(), saved.home_id.as_str(), saved.sql.as_str(), saved.reason.as_str(), i64::from(saved.once), saved.created_by.as_str(), saved.created_at],
            ).await?;
            Ok(saved)
        })
    }).await.map_err(|error| error.to_string())
}

async fn list_conditions(
    db: &LocalDb,
    thread_id: &str,
    active_only: bool,
) -> Result<Vec<WakeCondition>, String> {
    let thread_id = thread_id.to_string();
    db.read(move |conn| {
        let thread_id = thread_id.clone();
        Box::pin(async move {
            let active = if active_only { " AND state = 'active'" } else { "" };
            let mut rows = conn.query(&format!("{SELECT_CONDITION} WHERE home_kind = 'thread' AND home_id = ?1{active} ORDER BY created_at, id"), params![thread_id.as_str()]).await?;
            let mut conditions = Vec::new();
            while let Some(row) = rows.next().await? { conditions.push(condition_from_row(&row)?); }
            Ok(conditions)
        })
    }).await.map_err(|error| error.to_string())
}

pub async fn list_wake_conditions(
    db: &LocalDb,
    thread_id: &str,
) -> Result<Vec<WakeCondition>, String> {
    list_conditions(db, thread_id, false).await
}

pub async fn load_active_wake_conditions(
    db: &LocalDb,
    thread_id: &str,
) -> Result<Vec<WakeCondition>, String> {
    list_conditions(db, thread_id, true).await
}

async fn set_condition_state(
    db: &LocalDb,
    thread_id: &str,
    id: &str,
    state: &str,
    reset_edge: bool,
) -> Result<bool, String> {
    let (thread_id, id, state) = (thread_id.to_string(), id.to_string(), state.to_string());
    db.write(move |conn| {
        let (thread_id, id, state) = (thread_id.clone(), id.clone(), state.clone());
        Box::pin(async move {
            let sql = if reset_edge {
                "UPDATE wake_conditions SET state = ?1, last_outcome = NULL, last_error = NULL, updated_at = unixepoch() WHERE id = ?2 AND home_kind = 'thread' AND home_id = ?3"
            } else {
                "UPDATE wake_conditions SET state = ?1, updated_at = unixepoch() WHERE id = ?2 AND home_kind = 'thread' AND home_id = ?3"
            };
            Ok(conn.execute(sql, params![state.as_str(), id.as_str(), thread_id.as_str()]).await? > 0)
        })
    }).await.map_err(|error| error.to_string())
}

pub async fn mute_wake_condition(db: &LocalDb, thread_id: &str, id: &str) -> Result<bool, String> {
    set_condition_state(db, thread_id, id, "muted", false).await
}

/// Unmuting is a fresh arm: stale edge state from before the mute must not
/// suppress a condition that became true while nobody was listening.
pub async fn unmute_wake_condition(
    db: &LocalDb,
    thread_id: &str,
    id: &str,
) -> Result<bool, String> {
    set_condition_state(db, thread_id, id, "active", true).await
}

pub async fn delete_wake_condition(
    db: &LocalDb,
    thread_id: &str,
    id: &str,
) -> Result<bool, String> {
    let (thread_id, id) = (thread_id.to_string(), id.to_string());
    db.write(move |conn| {
        let (thread_id, id) = (thread_id.clone(), id.clone());
        Box::pin(async move {
            Ok(conn.execute("DELETE FROM wake_conditions WHERE id = ?1 AND home_kind = 'thread' AND home_id = ?2", params![id.as_str(), thread_id.as_str()]).await? > 0)
        })
    }).await.map_err(|error| error.to_string())
}

pub async fn delete_thread_wake_conditions(db: &LocalDb, thread_id: &str) -> Result<u64, String> {
    let thread_id = thread_id.to_string();
    db.write(move |conn| {
        let thread_id = thread_id.clone();
        Box::pin(async move {
            conn.execute(
                "DELETE FROM wake_conditions WHERE home_kind = 'thread' AND home_id = ?1",
                params![thread_id.as_str()],
            )
            .await
            .map_err(Into::into)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn fixture() -> (LocalDb, WakeCondition) {
        let db = crate::storage::migrated_test_db("wake-conditions.db").await;
        db.execute_script("INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1); INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES('p','w','P','P','/tmp',1,1); INSERT INTO threads(id, project_id, name, status, attention, created_at, updated_at) VALUES('thread','p','general','active','none',1,1);").await.unwrap();
        let condition = create_wake_condition(&db, "thread", "SELECT 1", "ready", false, "agent")
            .await
            .unwrap();
        (db, condition)
    }

    #[tokio::test]
    async fn persists_and_scopes_conditions() {
        let (db, condition) = fixture().await;
        assert_eq!(
            list_wake_conditions(&db, "thread").await.unwrap(),
            vec![condition]
        );
        assert!(list_wake_conditions(&db, "other").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn unmute_resets_the_persisted_edge() {
        let (db, condition) = fixture().await;
        db.execute_script(&format!(
            "UPDATE wake_conditions SET last_outcome = 'matched' WHERE id = '{}'",
            condition.id
        ))
        .await
        .unwrap();
        mute_wake_condition(&db, "thread", &condition.id)
            .await
            .unwrap();
        unmute_wake_condition(&db, "thread", &condition.id)
            .await
            .unwrap();
        let loaded = list_wake_conditions(&db, "thread")
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(loaded.state, "active");
        assert_eq!(loaded.last_outcome, None);
        assert!(should_fire(
            &loaded,
            &ConditionOutcome::Matched {
                rows: String::new(),
                shown: 1,
                total: Some(1)
            }
        ));
    }

    #[tokio::test]
    async fn stale_quiet_evaluation_cannot_overwrite_a_newer_fired_edge() {
        let (db, condition) = fixture().await;
        db.execute_script(&format!(
            "UPDATE wake_conditions SET last_outcome = 'matched', last_fired_at = 2 WHERE id = '{}'",
            condition.id
        ))
        .await
        .unwrap();
        record_quiet_outcome(&db, &condition, &ConditionOutcome::Empty)
            .await
            .unwrap();
        let loaded = list_wake_conditions(&db, "thread")
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(loaded.last_outcome.as_deref(), Some("matched"));
        assert_eq!(loaded.last_fired_at, Some(2));
    }

    #[test]
    fn edge_decision_suppresses_repeated_matches_and_identical_errors() {
        let mut condition = WakeCondition {
            id: "c".into(),
            home_kind: "thread".into(),
            home_id: "t".into(),
            sql: "SELECT 1".into(),
            reason: "r".into(),
            once: false,
            state: "active".into(),
            last_outcome: None,
            last_error: None,
            last_fired_at: None,
            last_evaluated_at: None,
            created_by: "agent".into(),
            created_at: 1,
            updated_at: 1,
        };
        let matched = ConditionOutcome::Matched {
            rows: String::new(),
            shown: 1,
            total: Some(1),
        };
        assert!(should_fire(&condition, &matched));
        condition.last_outcome = Some("matched".into());
        assert!(!should_fire(&condition, &matched));
        assert!(!should_fire(&condition, &ConditionOutcome::Empty));
        condition.last_outcome = Some("error".into());
        condition.last_error = Some("missing table".into());
        assert!(!should_fire(
            &condition,
            &ConditionOutcome::Error("missing table".into())
        ));
        assert!(should_fire(
            &condition,
            &ConditionOutcome::Error("syntax error".into())
        ));
    }
}
