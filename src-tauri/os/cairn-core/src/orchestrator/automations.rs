//! Durable schedules for headless thread procedure runs.

use crate::orchestrator::wakes::{due_occurrence, next_occurrence};
use crate::storage::{DbError, LocalDb, RowExt};
use cairn_db::turso::params;
use serde::{Deserialize, Serialize};

use crate::models::DeliveryUrgency;
use crate::orchestrator::Orchestrator;

const MIN_EVERY_MS: i64 = 5 * 60 * 1_000;
const MAX_EVERY_MS: i64 = 30 * 24 * 60 * 60 * 1_000;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadAutomation {
    pub id: String,
    pub thread_id: String,
    pub procedure_name: String,
    pub args: Vec<String>,
    pub every_ms: i64,
    pub anchor_at: i64,
    pub state: String,
    pub last_fired_occurrence_at: Option<i64>,
    pub last_evaluated_at: Option<i64>,
    pub last_run_id: Option<String>,
    pub last_status: Option<String>,
    pub last_error: Option<String>,
    pub consecutive_failures: i64,
    pub created_by: String,
    pub created_at: i64,
    pub updated_at: i64,
}

/// Claim and execute one due occurrence. The claim is persisted before any
/// executor work so concurrent scheduler hosts and restarts cannot double-fire.
pub async fn fire_thread_automation(
    orch: &Orchestrator,
    automation: &ThreadAutomation,
    now: i64,
) -> Result<bool, String> {
    let Some(due) = automation.due_at(now) else {
        return Ok(false);
    };
    let automation_id = automation.id.clone();
    let thread_id = automation.thread_id.clone();
    let claimed = orch.db.local.write(move |conn| {
        let automation_id = automation_id.clone();
        let thread_id = thread_id.clone();
        Box::pin(async move {
        let mut rows = conn.query(
            "SELECT a.state, a.last_fired_occurrence_at, t.status FROM thread_automations a JOIN threads t ON t.id=a.thread_id WHERE a.id=?1 AND a.thread_id=?2",
            params![automation_id.as_str(), thread_id.as_str()],
        ).await?;
        let Some(row) = rows.next().await? else { return Ok(false) };
        let state = row.text(0)?;
        let last = row.opt_i64(1)?;
        let thread_status = row.text(2)?;
        drop(rows);
        if state != "active" || last.is_some_and(|last| last >= due) { return Ok(false) }
        if thread_status != "active" {
            conn.execute(
                "UPDATE thread_automations SET state='muted', last_fired_occurrence_at=?1, last_evaluated_at=?2, last_status='failed', last_error='Owning thread is closed', consecutive_failures=consecutive_failures+1, updated_at=?2 WHERE id=?3",
                params![due, now, automation_id.as_str()],
            ).await?;
            return Ok(false);
        }
        let changed = conn.execute(
            "UPDATE thread_automations SET last_fired_occurrence_at=?1, last_evaluated_at=?2, updated_at=?2 WHERE id=?3 AND state='active' AND (last_fired_occurrence_at IS NULL OR last_fired_occurrence_at < ?1)",
            params![due, now, automation_id.as_str()],
        ).await?;
        Ok(changed == 1)
    })}).await.map_err(|error| error.to_string())?;
    if !claimed {
        return Ok(false);
    }

    let (job_id, run_id) = match ensure_automation_run(orch, automation, now).await {
        Ok(run) => run,
        Err(error) => {
            settle_automation(orch, automation, due, None, Some(&error), now).await?;
            return Ok(true);
        }
    };
    let result = execute_procedure(orch, automation, due, &job_id, &run_id).await;
    settle_automation(
        orch,
        automation,
        due,
        Some(&run_id),
        result.as_ref().err().map(String::as_str),
        now,
    )
    .await?;
    Ok(true)
}

/// Mint the run an automation fire executes under.
///
/// The batch runs AS its thread: "procedures execute with the invoking thread's
/// identity". The fence and the OS sandbox both resolve through run → job →
/// execution → snapshot → `agents[agent_config_id]`, so the automation job
/// carries the session's host execution and the session's own agent. A job that
/// named an agent no snapshot holds, or no execution at all, resolved to no
/// fence and every fire was refused before a command ran. The pointers are
/// re-stamped on every fire rather than only at creation, which also repairs
/// automation jobs minted before this held, and follows the session onto a new
/// host if it ever acquires one.
async fn ensure_automation_run(
    orch: &Orchestrator,
    automation: &ThreadAutomation,
    now: i64,
) -> Result<(String, String), String> {
    let thread_id = automation.thread_id.clone();
    let session_job = orch
        .db
        .local
        .write(move |conn| {
            let thread_id = thread_id.clone();
            Box::pin(async move {
                crate::threads::ensure_thread_session_conn(conn, &thread_id, None).await
            })
        })
        .await
        .map_err(|error| error.to_string())?;
    let host_execution = crate::execution::jobs::ensure_job_agent_snapshot(orch, &session_job)
        .await?
        .ok_or_else(|| format!("thread session {session_job} has no host execution"))?;

    let thread_id = automation.thread_id.clone();
    orch.db.local.write(move |conn| {
        let thread_id = thread_id.clone();
        let session_job = session_job.clone();
        let host_execution = host_execution.clone();
        Box::pin(async move {
        let mut session = conn.query("SELECT agent_config_id FROM jobs WHERE id=?1", params![session_job.as_str()]).await?;
        let agent_config_id = session.next().await?.ok_or_else(|| DbError::Internal(format!("thread session job not found: {session_job}")))?.opt_text(0)?;
        drop(session);
        let mut rows = conn.query(
            "SELECT id FROM jobs WHERE parent_job_id=?1 AND uri_segment='automations' ORDER BY created_at DESC LIMIT 1",
            params![session_job.as_str()],
        ).await?;
        let existing = rows.next().await?.map(|row| row.text(0)).transpose()?;
        drop(rows);
        let job_id = match existing {
            Some(job_id) => {
                conn.execute(
                    "UPDATE jobs SET execution_id=?1, agent_config_id=?2, updated_at=?3 WHERE id=?4",
                    params![host_execution.as_str(), agent_config_id.as_deref(), now, job_id.as_str()],
                ).await?;
                job_id
            }
            None => {
                let job_id = cairn_common::ids::mint_child(&thread_id);
                let mut project = conn.query("SELECT project_id FROM threads WHERE id=?1", params![thread_id.as_str()]).await?;
                let project_id = project.next().await?.ok_or_else(|| DbError::Internal(format!("thread not found: {thread_id}")))?.text(0)?;
                drop(project);
                conn.execute(
                    "INSERT INTO jobs (id, thread_id, project_id, parent_job_id, execution_id, status, agent_config_id, node_name, uri_segment, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, 'idle', ?6, 'automations', 'automations', ?7, ?7)",
                    params![job_id.as_str(), thread_id.as_str(), project_id.as_str(), session_job.as_str(), host_execution.as_str(), agent_config_id.as_deref(), now],
                ).await?;
                job_id
            }
        };
        let run_id = uuid::Uuid::new_v4().to_string();
        conn.execute(
            "INSERT INTO runs (id, project_id, job_id, status, started_at, created_at, updated_at, start_mode) SELECT ?1, project_id, id, 'live', ?2, ?2, ?2, 'automation' FROM jobs WHERE id=?3",
            params![run_id.as_str(), now, job_id.as_str()],
        ).await?;
        Ok((job_id, run_id))
    })}).await.map_err(|error| error.to_string())
}

async fn execute_procedure(
    orch: &Orchestrator,
    automation: &ThreadAutomation,
    due: i64,
    job_id: &str,
    run_id: &str,
) -> Result<String, String> {
    let payload = serde_json::json!({
        "commands": [{
            "target": format!("cairn:~/procedures/{}", automation.procedure_name),
            "payload": { "args": automation.args },
        }]
    });
    let request = crate::mcp::types::McpCallbackRequest {
        cwd: crate::scratch::ensure_job_scratch_dir(job_id, None)
            .to_string_lossy()
            .into_owned(),
        run_id: Some(run_id.to_string()),
        tool: "run".into(),
        payload,
        tool_use_id: Some(format!("automation:{}:{due}", automation.id)),
        thread_id: None,
    };
    let encoded = crate::mcp::handlers::run::handle_run(orch, &request).await;
    let envelope: cairn_common::read::RunBatchEnvelope = serde_json::from_str(&encoded)
        .map_err(|error| format!("Automation executor returned an invalid result: {error}"))?;
    if let Some(code) = envelope.text.lines().rev().find_map(|line| {
        line.strip_prefix("Exit code: ")
            .and_then(|value| value.trim().parse::<i32>().ok())
            .filter(|code| *code != 0)
    }) {
        return Err(format!(
            "Procedure failed with exit code {code}: {}",
            envelope.text
        ));
    }
    if crate::mcp::handlers::run::envelope_reports_run_failure(&envelope.text) {
        return Err(envelope.text);
    }
    Ok(envelope.text)
}

/// Bound on how much of a failure the streak notice quotes. A procedure failure
/// carries its whole batch envelope; the automation resource keeps all of it.
const NOTICE_ERROR_CHARS: usize = 1_500;

/// Record a fire's outcome, and tell the owning thread when a failure streak
/// BEGINS.
///
/// Exactly one notice per streak: an automation fails on a fixed cadence, so a
/// notice per fire wakes an idle thread every interval to relearn one fact. The
/// first failure after a success (or ever) is announced; later ones only update
/// the automation row the notice links to, and the next success ends the streak
/// so a later failure is news again.
async fn settle_automation(
    orch: &Orchestrator,
    automation: &ThreadAutomation,
    due: i64,
    run_id: Option<&str>,
    error: Option<&str>,
    now: i64,
) -> Result<(), String> {
    let automation_id = automation.id.clone();
    let automation = automation.clone();
    let run_id = run_id.map(str::to_owned);
    let error = error.map(str::to_owned);
    let nudge = orch.db.local.write(move |conn| {
        let automation = automation.clone();
        let run_id = run_id.clone();
        let error = error.clone();
        Box::pin(async move {
        let (status, run_status) = if error.is_some() { ("failed", "failed") } else { ("ok", "completed") };
        if let Some(run_id) = run_id.as_deref() {
            conn.execute("UPDATE runs SET status=?1, error_message=?2, exited_at=?3, updated_at=?3 WHERE id=?4", params![run_status, error.as_deref(), now, run_id]).await?;
        }
        let mut prior = conn.query("SELECT consecutive_failures FROM thread_automations WHERE id=?1", params![automation.id.as_str()]).await?;
        let prior_failures = prior.next().await?.map(|row| row.i64(0)).transpose()?.unwrap_or(0);
        drop(prior);
        conn.execute(
            "UPDATE thread_automations SET last_run_id=COALESCE(?1, last_run_id), last_status=?2, last_error=?3, consecutive_failures=CASE WHEN ?3 IS NULL THEN 0 ELSE consecutive_failures+1 END, updated_at=?4 WHERE id=?5",
            params![run_id.as_deref(), status, error.as_deref(), now, automation.id.as_str()],
        ).await?;
        let Some(error) = error else { return Ok(None) };
        if prior_failures > 0 { return Ok(None) }
        let session_job = crate::threads::ensure_thread_session_conn(conn, &automation.thread_id, None).await?;
        let mut rows = conn.query("SELECT id FROM runs WHERE job_id=?1 ORDER BY created_at DESC, id DESC LIMIT 1", params![session_job.as_str()]).await?;
        let recipient_run = rows.next().await?.map(|row| row.text(0)).transpose()?;
        drop(rows);
        let Some(recipient_run) = recipient_run else { return Ok(None) };
        let mut coordinate = conn.query("SELECT p.key, t.name FROM threads t JOIN projects p ON p.id=t.project_id WHERE t.id=?1", params![automation.thread_id.as_str()]).await?;
        let status_uri = match coordinate.next().await? {
            Some(row) => cairn_common::uri::CairnResource::ThreadAutomation { project: row.text(0)?, name: row.text(1)?, id: automation.id.clone() }.to_uri(),
            None => format!("automation {}", automation.id),
        };
        drop(coordinate);
        let mut quoted: String = error.chars().take(NOTICE_ERROR_CHARS).collect();
        if error.chars().nth(NOTICE_ERROR_CHARS).is_some() { quoted.push('…'); }
        let run_suffix = run_id.as_deref().map(|id| format!("\n\nRun: {id}")).unwrap_or_default();
        let content = format!(
            "Automation `{}` running procedure `{}` started failing at occurrence {}: {}\n\nThis is the only notice for this failure streak: later failures update {} without waking you again, and the next success ends the streak.{}",
            automation.id, automation.procedure_name, due, quoted, status_uri, run_suffix
        );
        let key = format!("automation:{}", automation.id);
        crate::messages::delivery::insert_system_direct_push_with_key_conn(conn, &session_job, &recipient_run, &content, DeliveryUrgency::Steer, Some(&key)).await?;
        Ok(Some(session_job))
    })}).await.map_err(|error| error.to_string())?;
    // The outcome and the notice are durable by now, and the pending push is what
    // the thread's next idle drain delivers. A wake that cannot be issued this
    // instant is logged rather than turned into a failed fire, which would make
    // the scheduler cool down as if nothing had been recorded.
    if let Some(job_id) = nudge {
        if let Err(error) =
            crate::messages::delivery::nudge_job_for_urgency(orch, &job_id, DeliveryUrgency::Steer)
        {
            log::warn!(
                "Recorded automation '{}' failure notice but could not wake thread job {job_id}: {error}",
                automation_id
            );
        }
    }
    Ok(())
}

impl ThreadAutomation {
    pub fn due_at(&self, now: i64) -> Option<i64> {
        due_occurrence(self.anchor_at, self.every_ms, now)
            .filter(|due| self.last_fired_occurrence_at.is_none_or(|last| *due > last))
    }

    pub fn next_due_at(&self, now: i64) -> Option<i64> {
        next_occurrence(self.anchor_at, self.every_ms, now)
    }
}

const SELECT: &str = "SELECT id, thread_id, procedure_name, args_json, every_ms, anchor_at, state, last_fired_occurrence_at, last_evaluated_at, last_run_id, last_status, last_error, consecutive_failures, created_by, created_at, updated_at FROM thread_automations";

fn from_row(row: &cairn_db::turso::Row) -> Result<ThreadAutomation, DbError> {
    let args_json = row.opt_text(3)?.unwrap_or_else(|| "[]".into());
    let args = serde_json::from_str(&args_json)
        .map_err(|error| DbError::Internal(format!("invalid automation args_json: {error}")))?;
    Ok(ThreadAutomation {
        id: row.text(0)?,
        thread_id: row.text(1)?,
        procedure_name: row.text(2)?,
        args,
        every_ms: row.i64(4)?,
        anchor_at: row.i64(5)?,
        state: row.text(6)?,
        last_fired_occurrence_at: row.opt_i64(7)?,
        last_evaluated_at: row.opt_i64(8)?,
        last_run_id: row.opt_text(9)?,
        last_status: row.opt_text(10)?,
        last_error: row.opt_text(11)?,
        consecutive_failures: row.i64(12)?,
        created_by: row.text(13)?,
        created_at: row.i64(14)?,
        updated_at: row.i64(15)?,
    })
}

fn validate(every_ms: i64, procedure_name: &str, created_by: Option<&str>) -> Result<(), String> {
    if !(MIN_EVERY_MS..=MAX_EVERY_MS).contains(&every_ms) {
        return Err("Automation interval must be between 5 minutes and 30 days".into());
    }
    if procedure_name.is_empty() {
        return Err("Automation procedure name cannot be empty".into());
    }
    if created_by.is_some_and(|value| !matches!(value, "agent" | "user" | "system")) {
        return Err("Automation creator must be agent, user, or system".into());
    }
    Ok(())
}

pub async fn list_thread_automations(
    db: &LocalDb,
    thread_id: &str,
) -> Result<Vec<ThreadAutomation>, String> {
    let thread_id = thread_id.to_string();
    db.read(move |conn| {
        let thread_id = thread_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    &format!("{SELECT} WHERE thread_id = ?1 ORDER BY created_at, id"),
                    params![thread_id.as_str()],
                )
                .await?;
            let mut values = Vec::new();
            while let Some(row) = rows.next().await? {
                values.push(from_row(&row)?);
            }
            Ok(values)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

pub async fn get_thread_automation(
    db: &LocalDb,
    thread_id: &str,
    id: &str,
) -> Result<Option<ThreadAutomation>, String> {
    let (thread_id, id) = (thread_id.to_string(), id.to_string());
    db.read(move |conn| {
        let thread_id = thread_id.clone();
        let id = id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    &format!("{SELECT} WHERE thread_id = ?1 AND id = ?2"),
                    params![thread_id.as_str(), id.as_str()],
                )
                .await?;
            rows.next().await?.map(|row| from_row(&row)).transpose()
        })
    })
    .await
    .map_err(|error| error.to_string())
}

pub async fn create_thread_automation(
    db: &LocalDb,
    thread_id: &str,
    procedure_name: &str,
    args: Vec<String>,
    every_ms: i64,
    created_by: &str,
) -> Result<ThreadAutomation, String> {
    validate(every_ms, procedure_name, Some(created_by))?;
    let id = uuid::Uuid::new_v4().to_string();
    let (thread_id, procedure_name, created_by) = (
        thread_id.to_string(),
        procedure_name.to_string(),
        created_by.to_string(),
    );
    let args_json = serde_json::to_string(&args).map_err(|error| error.to_string())?;
    let created_at = chrono::Utc::now().timestamp();
    let anchor_at = created_at + (every_ms + 999) / 1_000;
    let saved_id = id.clone();
    let saved_thread_id = thread_id.clone();
    db.write(move |conn| {
        let thread_id = thread_id.clone();
        let procedure_name = procedure_name.clone();
        let created_by = created_by.clone();
        let args_json = args_json.clone();
        let saved_id = saved_id.clone();
        Box::pin(async move {
        let mut rows = conn.query("SELECT status FROM threads WHERE id = ?1", params![thread_id.as_str()]).await?;
        let status = rows.next().await?.map(|row| row.text(0)).transpose()?;
        drop(rows);
        if status.as_deref() != Some("active") { return Err(DbError::Internal(format!("thread is not active: {thread_id}"))); }
        conn.execute("INSERT INTO thread_automations (id, thread_id, procedure_name, args_json, every_ms, anchor_at, created_by, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8)", params![saved_id.as_str(), thread_id.as_str(), procedure_name.as_str(), args_json.as_str(), every_ms, anchor_at, created_by.as_str(), created_at]).await?;
        Ok(())
        })
    }).await.map_err(|error| error.to_string())?;
    get_thread_automation(db, &saved_thread_id, &id)
        .await?
        .ok_or_else(|| "created automation disappeared".into())
}

pub async fn update_thread_automation(
    db: &LocalDb,
    thread_id: &str,
    id: &str,
    every_ms: Option<i64>,
    args: Option<Vec<String>>,
    state: Option<&str>,
) -> Result<Option<ThreadAutomation>, String> {
    if let Some(value) = every_ms {
        validate(value, "unchanged", None)?;
    }
    if state.is_some_and(|value| !matches!(value, "active" | "muted")) {
        return Err("Automation state must be active or muted".into());
    }
    let current = get_thread_automation(db, thread_id, id).await?;
    let Some(current) = current else {
        return Ok(None);
    };
    let new_every = every_ms.unwrap_or(current.every_ms);
    let new_args = args.unwrap_or(current.args);
    let new_state = state.unwrap_or(&current.state).to_string();
    let now = chrono::Utc::now().timestamp();
    let new_anchor = every_ms
        .map(|value| now + (value + 999) / 1_000)
        .unwrap_or(current.anchor_at);
    let args_json = serde_json::to_string(&new_args).map_err(|error| error.to_string())?;
    let (thread_id, id) = (thread_id.to_string(), id.to_string());
    db.write(move |conn| {
        let args_json = args_json.clone();
        let new_state = new_state.clone();
        let thread_id = thread_id.clone();
        let id = id.clone();
        Box::pin(async move {
        conn.execute("UPDATE thread_automations SET args_json=?1, every_ms=?2, anchor_at=?3, state=?4, last_fired_occurrence_at=CASE WHEN ?5 THEN NULL ELSE last_fired_occurrence_at END, updated_at=?6 WHERE thread_id=?7 AND id=?8", params![args_json.as_str(), new_every, new_anchor, new_state.as_str(), every_ms.is_some(), now, thread_id.as_str(), id.as_str()]).await?;
        Ok(())
        })
    }).await.map_err(|error| error.to_string())?;
    get_thread_automation(db, &current.thread_id, &current.id).await
}

pub async fn delete_thread_automation(
    db: &LocalDb,
    thread_id: &str,
    id: &str,
) -> Result<bool, String> {
    let (thread_id, id) = (thread_id.to_string(), id.to_string());
    db.write(move |conn| {
        let thread_id = thread_id.clone();
        let id = id.clone();
        Box::pin(async move {
            Ok(conn
                .execute(
                    "DELETE FROM thread_automations WHERE thread_id=?1 AND id=?2",
                    params![thread_id.as_str(), id.as_str()],
                )
                .await?
                > 0)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

pub async fn load_active_thread_automations(db: &LocalDb) -> Result<Vec<ThreadAutomation>, String> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    &format!("{SELECT} WHERE state = 'active' ORDER BY anchor_at, id"),
                    (),
                )
                .await?;
            let mut values = Vec::new();
            while let Some(row) = rows.next().await? {
                values.push(from_row(&row)?);
            }
            Ok(values)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::SearchIndex;
    use std::sync::Arc;

    #[tokio::test]
    async fn settlement_persists_success_and_failure_through_db_write() {
        let root = tempfile::tempdir().unwrap().keep();
        let db = Arc::new(crate::storage::migrated_test_db("automation-settlement.db").await);
        db.execute_script("INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1); INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','P','P','/tmp',1,1); INSERT INTO threads(id,project_id,name,status,attention,created_at,updated_at) VALUES('t','p','general','active','none',1,1); INSERT INTO jobs(id,thread_id,project_id,status,agent_config_id,node_name,uri_segment,created_at,updated_at) VALUES('j','t','p','idle','automation','automations','automations',1,1); INSERT INTO runs(id,project_id,job_id,status,created_at,updated_at) VALUES('success','p','j','live',1,1),('failure','p','j','live',2,2); INSERT INTO thread_automations(id,thread_id,procedure_name,args_json,every_ms,anchor_at,created_by,created_at,updated_at) VALUES('a','t','collect','[]',60000,60,'agent',1,1);").await.unwrap();
        let search = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(db.clone(), search)),
            Arc::new(TestServicesBuilder::new().build()),
            root,
        )
        .build();
        let automation = get_thread_automation(&db, "t", "a").await.unwrap().unwrap();

        settle_automation(&orch, &automation, 60, Some("success"), None, 10)
            .await
            .unwrap();
        assert_eq!(
            db.query_text("SELECT status FROM runs WHERE id='success'", ())
                .await
                .unwrap()
                .as_deref(),
            Some("completed")
        );

        settle_automation(&orch, &automation, 120, Some("failure"), Some("boom"), 20)
            .await
            .unwrap();
        assert_eq!(
            db.query_text("SELECT status FROM runs WHERE id='failure'", ())
                .await
                .unwrap()
                .as_deref(),
            Some("failed")
        );
        let settled = get_thread_automation(&db, "t", "a").await.unwrap().unwrap();
        assert_eq!(settled.last_run_id.as_deref(), Some("failure"));
        assert_eq!(settled.last_status.as_deref(), Some("failed"));
        assert_eq!(settled.last_error.as_deref(), Some("boom"));
        assert_eq!(settled.consecutive_failures, 1);
    }

    async fn count(db: &LocalDb, sql: &'static str) -> i64 {
        db.query_text(sql, ())
            .await
            .unwrap()
            .and_then(|value| value.parse().ok())
            .unwrap()
    }

    /// A five-minute automation that breaks keeps failing every five minutes.
    /// The owning thread hears about that once, when it starts, and again only
    /// once a success has ended the streak and a new one begins.
    #[tokio::test]
    async fn a_failure_streak_raises_exactly_one_attention_fact_for_the_owning_thread() {
        let root = tempfile::tempdir().unwrap().keep();
        let db = Arc::new(crate::storage::migrated_test_db("automation-streak.db").await);
        // The session holds a running turn, so the notice is recorded without
        // the nudge relaunching an agent process this test has no spawner for.
        db.execute_script(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','P','P','/tmp',1,1);
             INSERT INTO threads(id,project_id,name,status,attention,created_at,updated_at) VALUES('t','p','desk','active','none',1,1);
             INSERT INTO jobs(id,thread_id,project_id,status,agent_config_id,current_session_id,node_name,uri_segment,created_at,updated_at) VALUES('session','t','p','running','thread','sess','thread','thread',1,1);
             INSERT INTO sessions(id,job_id,backend,status,sequence,created_at,updated_at) VALUES('sess','session','claude','open',1,1,1);
             INSERT INTO runs(id,project_id,job_id,status,created_at,updated_at) VALUES('session-run','p','session','live',1,1);
             INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,created_at,updated_at) VALUES('turn','sess','session-run','session',1,'running',1,1);
             INSERT INTO thread_automations(id,thread_id,procedure_name,args_json,every_ms,anchor_at,created_by,created_at,updated_at) VALUES('a','t','collect','[]',300000,60,'agent',1,1);",
        )
        .await
        .unwrap();
        let search = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(db.clone(), search)),
            Arc::new(TestServicesBuilder::new().build()),
            root,
        )
        .build();
        let automation = get_thread_automation(&db, "t", "a").await.unwrap().unwrap();
        const NOTICES: &str = "SELECT CAST(COUNT(*) AS TEXT) FROM messages WHERE recipient_run_id='session-run' AND content LIKE 'Automation `a`%'";
        const PENDING: &str = "SELECT CAST(COUNT(*) AS TEXT) FROM attention_pushes WHERE recipient='session' AND key='automation:a' AND delivered_event_id IS NULL";

        for (due, now) in [(60, 61), (360, 361), (660, 661)] {
            settle_automation(&orch, &automation, due, None, Some("boom"), now)
                .await
                .unwrap();
        }
        assert_eq!(count(&db, NOTICES).await, 1, "one notice per streak");
        assert_eq!(count(&db, PENDING).await, 1);
        let notice = db
            .query_text(
                "SELECT content FROM messages WHERE content LIKE 'Automation `a`%'",
                (),
            )
            .await
            .unwrap()
            .unwrap();
        assert!(
            notice.contains("started failing at occurrence 60: boom"),
            "{notice}"
        );
        assert!(notice.contains("/desk/automations/a"), "{notice}");
        assert_eq!(
            get_thread_automation(&db, "t", "a")
                .await
                .unwrap()
                .unwrap()
                .consecutive_failures,
            3
        );

        // Delivered or not, the streak has been announced: coalescing by key
        // only absorbs repeats while the push is still pending, which is not
        // what holds the next failure back.
        db.execute(
            "UPDATE attention_pushes SET delivered_event_id='delivered'",
            (),
        )
        .await
        .unwrap();
        settle_automation(&orch, &automation, 960, None, Some("boom"), 961)
            .await
            .unwrap();
        assert_eq!(count(&db, NOTICES).await, 1);
        assert_eq!(count(&db, PENDING).await, 0);

        settle_automation(&orch, &automation, 1260, None, None, 1261)
            .await
            .unwrap();
        settle_automation(&orch, &automation, 1560, None, Some("again"), 1561)
            .await
            .unwrap();
        assert_eq!(count(&db, NOTICES).await, 2, "a new streak is news again");
        assert_eq!(count(&db, PENDING).await, 1);
    }

    #[tokio::test]
    async fn automation_crud_preserves_args_and_anchors_first_fire() {
        let db = crate::storage::migrated_test_db("thread-automations.db").await;
        db.execute_script("INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1); INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','P','P','/tmp',1,1); INSERT INTO threads(id,project_id,name,status,attention,created_at,updated_at) VALUES('t','p','general','active','none',1,1);").await.unwrap();
        let created =
            create_thread_automation(&db, "t", "collect", vec!["a".into()], MIN_EVERY_MS, "agent")
                .await
                .unwrap();
        assert_eq!(created.args, vec!["a"]);
        assert_eq!(created.due_at(created.created_at), None);
        assert_eq!(created.due_at(created.anchor_at), Some(created.anchor_at));
        let updated = update_thread_automation(
            &db,
            "t",
            &created.id,
            None,
            Some(vec!["b".into()]),
            Some("muted"),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(updated.args, vec!["b"]);
        assert_eq!(updated.state, "muted");
        assert!(delete_thread_automation(&db, "t", &created.id)
            .await
            .unwrap());
    }
}
