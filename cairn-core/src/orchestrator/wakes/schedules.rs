//! Durable, anchored wake schedules for first-class threads.

use crate::messages::queued::DeliveryUrgency;
use crate::orchestrator::Orchestrator;
use crate::storage::{DbError, LocalDb, RowExt};
use cairn_db::turso::params;
use serde::{Deserialize, Serialize};

const MIN_EVERY_MS: i64 = 5 * 60 * 1_000;
const MAX_EVERY_MS: i64 = 30 * 24 * 60 * 60 * 1_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WakeSchedule {
    pub id: String,
    pub home_kind: String,
    pub home_id: String,
    pub every_ms: i64,
    pub anchor_at: i64,
    pub reason: String,
    pub state: String,
    pub last_fired_occurrence_at: Option<i64>,
    pub last_evaluated_at: Option<i64>,
    pub created_by: String,
    pub created_at: i64,
    pub updated_at: i64,
}

/// Resolve a wakes resource's current session job to its durable thread home.
/// Recurring schedules deliberately refuse ordinary execution and task jobs.
pub async fn thread_home_for_job(db: &LocalDb, job_id: &str) -> Result<String, String> {
    let job_id = job_id.to_string();
    db.read(move |conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
        let mut rows = conn.query(
            "SELECT thread_id FROM jobs WHERE id = ?1",
            params![job_id.as_str()],
        ).await?;
        rows.next().await?
            .map(|row| row.opt_text(0))
            .transpose()?
            .flatten()
            .ok_or_else(|| DbError::Internal(
                "Recurring schedules are only supported on thread homes; create or use a thread and write its cairn:~/wakes resource".into()
            ))
        })
    }).await.map_err(|error| error.to_string())
}

impl WakeSchedule {
    pub fn next_due_at(&self, now: i64) -> Option<i64> {
        next_occurrence(self.anchor_at, self.every_ms, now)
    }
    pub fn due_at(&self, now: i64) -> Option<i64> {
        due_occurrence(self.anchor_at, self.every_ms, now)
            .filter(|due| self.last_fired_occurrence_at.is_none_or(|last| *due > last))
    }
}

/// Largest anchored occurrence at or before now.
pub fn due_occurrence(anchor_at: i64, every_ms: i64, now: i64) -> Option<i64> {
    if every_ms <= 0 || now < anchor_at {
        return None;
    }
    let elapsed_ms = i128::from(now - anchor_at) * 1_000;
    let steps = elapsed_ms / i128::from(every_ms);
    let occurrence_ms = i128::from(anchor_at) * 1_000 + steps * i128::from(every_ms);
    i64::try_from(occurrence_ms / 1_000).ok()
}

/// Smallest anchored occurrence strictly after now.
pub fn next_occurrence(anchor_at: i64, every_ms: i64, now: i64) -> Option<i64> {
    if every_ms <= 0 {
        return None;
    }
    if now < anchor_at {
        return Some(anchor_at);
    }
    let elapsed_ms = i128::from(now - anchor_at) * 1_000;
    let steps = elapsed_ms / i128::from(every_ms) + 1;
    let occurrence_ms = i128::from(anchor_at) * 1_000 + steps * i128::from(every_ms);
    i64::try_from((occurrence_ms + 999) / 1_000).ok()
}

fn schedule_from_row(row: &cairn_db::turso::Row) -> Result<WakeSchedule, DbError> {
    Ok(WakeSchedule {
        id: row.text(0)?,
        home_kind: row.text(1)?,
        home_id: row.text(2)?,
        every_ms: row.i64(3)?,
        anchor_at: row.i64(4)?,
        reason: row.text(5)?,
        state: row.text(6)?,
        last_fired_occurrence_at: row.opt_i64(7)?,
        last_evaluated_at: row.opt_i64(8)?,
        created_by: row.text(9)?,
        created_at: row.i64(10)?,
        updated_at: row.i64(11)?,
    })
}

const SELECT_SCHEDULE: &str = "SELECT id, home_kind, home_id, every_ms, anchor_at, reason, state, last_fired_occurrence_at, last_evaluated_at, created_by, created_at, updated_at FROM wake_schedules";

pub async fn list_wake_schedules(
    db: &LocalDb,
    thread_id: &str,
) -> Result<Vec<WakeSchedule>, String> {
    let thread_id = thread_id.to_string();
    db.read(move |conn| {
        let thread_id = thread_id.clone();
        Box::pin(async move {
        let mut rows = conn.query(
            &format!("{SELECT_SCHEDULE} WHERE home_kind = 'thread' AND home_id = ?1 ORDER BY created_at, id"),
            params![thread_id.as_str()],
        ).await?;
        let mut schedules = Vec::new();
        while let Some(row) = rows.next().await? { schedules.push(schedule_from_row(&row)?); }
        Ok(schedules)
        })
    }).await.map_err(|error| error.to_string())
}

pub async fn create_wake_schedule(
    db: &LocalDb,
    thread_id: &str,
    every_ms: i64,
    reason: &str,
    created_by: &str,
) -> Result<WakeSchedule, String> {
    if !(MIN_EVERY_MS..=MAX_EVERY_MS).contains(&every_ms) {
        return Err("Schedule interval must be between 5 minutes and 30 days".into());
    }
    if reason.trim().is_empty() {
        return Err("Schedule reason cannot be empty".into());
    }
    if !matches!(created_by, "agent" | "user" | "system") {
        return Err("Schedule creator must be agent, user, or system".into());
    }
    let id = uuid::Uuid::new_v4().to_string();
    let thread_id = thread_id.to_string();
    let reason = reason.trim().to_string();
    let created_by = created_by.to_string();
    let now = chrono::Utc::now().timestamp();
    let saved_id = id.clone();
    db.write(move |conn| {
        let saved_id = saved_id.clone();
        let thread_id = thread_id.clone();
        let reason = reason.clone();
        let created_by = created_by.clone();
        Box::pin(async move {
        let mut rows = conn.query("SELECT status FROM threads WHERE id = ?1", params![thread_id.as_str()]).await?;
        let status = rows.next().await?.map(|row| row.text(0)).transpose()?;
        drop(rows);
        if status.as_deref() != Some("active") {
            return Err(DbError::Internal(format!("thread is not active: {thread_id}")));
        }
        conn.execute(
            "INSERT INTO wake_schedules (id, home_kind, home_id, every_ms, anchor_at, reason, state, created_by, created_at, updated_at) VALUES (?1, 'thread', ?2, ?3, ?4, ?5, 'active', ?6, ?4, ?4)",
            params![saved_id.as_str(), thread_id.as_str(), every_ms, now, reason.as_str(), created_by.as_str()],
        ).await?;
        Ok(WakeSchedule {
            id: saved_id, home_kind: "thread".into(), home_id: thread_id,
            every_ms, anchor_at: now, reason, state: "active".into(),
            last_fired_occurrence_at: None, last_evaluated_at: None,
            created_by, created_at: now, updated_at: now,
        })
        })
    }).await.map_err(|error| error.to_string())
}

async fn set_schedule_state(
    db: &LocalDb,
    thread_id: &str,
    schedule_id: &str,
    state: &str,
) -> Result<bool, String> {
    let (thread_id, schedule_id, state) = (
        thread_id.to_string(),
        schedule_id.to_string(),
        state.to_string(),
    );
    db.write(move |conn| {
        let thread_id = thread_id.clone();
        let schedule_id = schedule_id.clone();
        let state = state.clone();
        Box::pin(async move {
        Ok(conn.execute(
            "UPDATE wake_schedules SET state = ?1, updated_at = unixepoch() WHERE id = ?2 AND home_kind = 'thread' AND home_id = ?3",
            params![state.as_str(), schedule_id.as_str(), thread_id.as_str()],
        ).await? > 0)
        })
    }).await.map_err(|error| error.to_string())
}

pub async fn mute_wake_schedule(db: &LocalDb, thread_id: &str, id: &str) -> Result<bool, String> {
    set_schedule_state(db, thread_id, id, "muted").await
}
pub async fn unmute_wake_schedule(db: &LocalDb, thread_id: &str, id: &str) -> Result<bool, String> {
    let (thread_id, id) = (thread_id.to_string(), id.to_string());
    let now = chrono::Utc::now().timestamp();
    db.write(move |conn| {
        let thread_id = thread_id.clone();
        let id = id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT anchor_at, every_ms FROM wake_schedules
             WHERE id = ?1 AND home_kind = 'thread' AND home_id = ?2",
                    params![id.as_str(), thread_id.as_str()],
                )
                .await?;
            let occurrence = rows
                .next()
                .await?
                .map(|row| {
                    due_occurrence(row.i64(0)?, row.i64(1)?, now).ok_or_else(|| {
                        DbError::Internal("schedule has no current occurrence".into())
                    })
                })
                .transpose()?;
            drop(rows);
            let Some(occurrence) = occurrence else {
                return Ok(false);
            };
            Ok(conn
                .execute(
                    "UPDATE wake_schedules SET state = 'active',
                    last_fired_occurrence_at = MAX(COALESCE(last_fired_occurrence_at, ?1), ?1),
                    updated_at = ?2
             WHERE id = ?3 AND home_kind = 'thread' AND home_id = ?4",
                    params![occurrence, now, id.as_str(), thread_id.as_str()],
                )
                .await?
                > 0)
        })
    })
    .await
    .map_err(|error| error.to_string())
}
pub async fn delete_wake_schedule(db: &LocalDb, thread_id: &str, id: &str) -> Result<bool, String> {
    let (thread_id, id) = (thread_id.to_string(), id.to_string());
    db.write(move |conn| {
        let thread_id = thread_id.clone();
        let id = id.clone();
        Box::pin(async move {
            Ok(conn.execute(
            "DELETE FROM wake_schedules WHERE id = ?1 AND home_kind = 'thread' AND home_id = ?2",
            params![id.as_str(), thread_id.as_str()],
        ).await? > 0)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

pub async fn load_active_wake_schedules(db: &LocalDb) -> Result<Vec<WakeSchedule>, String> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    &format!("{SELECT_SCHEDULE} WHERE state = 'active' ORDER BY anchor_at, id"),
                    (),
                )
                .await?;
            let mut schedules = Vec::new();
            while let Some(row) = rows.next().await? {
                schedules.push(schedule_from_row(&row)?);
            }
            Ok(schedules)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// Atomically enqueue one due occurrence and advance its cursor.
pub async fn fire_wake_schedule(
    orch: &Orchestrator,
    schedule: &WakeSchedule,
    now: i64,
) -> Result<bool, String> {
    let Some(due) = schedule.due_at(now) else {
        return Ok(false);
    };
    let schedule_id = schedule.id.clone();
    let thread_id = schedule.home_id.clone();
    let reason = schedule.reason.clone();
    let every_ms = schedule.every_ms;
    let outcome = orch.db.local.write(move |conn| {
        let schedule_id = schedule_id.clone();
        let thread_id = thread_id.clone();
        let reason = reason.clone();
        Box::pin(async move {
        let mut current = conn.query(
            "SELECT state, last_fired_occurrence_at FROM wake_schedules WHERE id = ?1 AND home_kind = 'thread' AND home_id = ?2",
            params![schedule_id.as_str(), thread_id.as_str()],
        ).await?;
        let Some(row) = current.next().await? else { return Ok(None) };
        let state = row.text(0)?;
        let last = row.opt_i64(1)?;
        drop(current);
        if state != "active" || last.is_some_and(|last| last >= due) { return Ok(None); }
        let mut thread = conn.query("SELECT status FROM threads WHERE id = ?1", params![thread_id.as_str()]).await?;
        let status = thread.next().await?.map(|row| row.text(0)).transpose()?;
        drop(thread);
        if status.as_deref() != Some("active") {
            conn.execute(
                "UPDATE wake_schedules SET last_fired_occurrence_at = ?1, last_evaluated_at = ?2, updated_at = ?2 WHERE id = ?3",
                params![due, now, schedule_id.as_str()],
            ).await?;
            return Ok(None);
        }
        let job_id = crate::threads::ensure_thread_session_conn(conn, &thread_id, None).await?;
        let mut runs = conn.query(
            "SELECT id FROM runs WHERE job_id = ?1 ORDER BY created_at DESC LIMIT 1",
            params![job_id.as_str()],
        ).await?;
        let run_id = runs.next().await?.map(|row| row.text(0)).transpose()?;
        drop(runs);
        let Some(run_id) = run_id else {
            conn.execute(
                "UPDATE wake_schedules SET last_evaluated_at = ?1, updated_at = ?1 WHERE id = ?2",
                params![now, schedule_id.as_str()],
            ).await?;
            return Ok(None);
        };
        let skipped = last
            .map(|last| ((due - last) * 1_000 / every_ms).saturating_sub(1))
            .unwrap_or(0);
        let content = if skipped > 0 {
            format!(
                "{reason}\n\n{skipped} scheduled occurrence(s) were skipped while this thread was unavailable."
            )
        } else {
            reason.clone()
        };
        let push_key = format!("schedule:{schedule_id}");
        crate::messages::delivery::insert_system_direct_push_with_key_conn(
            conn, &job_id, &run_id, &content, DeliveryUrgency::Steer, Some(&push_key),
        ).await?;
        conn.execute(
            "UPDATE wake_schedules SET last_fired_occurrence_at = ?1, last_evaluated_at = ?2, updated_at = ?2 WHERE id = ?3",
            params![due, now, schedule_id.as_str()],
        ).await?;
        Ok(Some(job_id))
        })
    }).await.map_err(|error| error.to_string())?;
    if let Some(job_id) = outcome {
        crate::messages::delivery::nudge_job_for_urgency(orch, &job_id, DeliveryUrgency::Steer)?;
        Ok(true)
    } else {
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn anchored_occurrences_do_not_drift_when_evaluated_late() {
        assert_eq!(due_occurrence(100, 10_000, 137), Some(130));
        assert_eq!(next_occurrence(100, 10_000, 137), Some(140));
    }
    #[test]
    fn exact_boundary_is_due_and_next_is_strictly_later() {
        assert_eq!(due_occurrence(100, 10_000, 130), Some(130));
        assert_eq!(next_occurrence(100, 10_000, 130), Some(140));
    }
    #[test]
    fn downtime_collapses_to_latest_occurrence() {
        assert_eq!(due_occurrence(0, 6 * 60 * 60 * 1_000, 86_401), Some(86_400));
    }
    #[test]
    fn cursor_suppresses_old_occurrences_after_unmute() {
        let schedule = WakeSchedule {
            id: "s".into(),
            home_kind: "thread".into(),
            home_id: "t".into(),
            every_ms: 10_000,
            anchor_at: 100,
            reason: "reason".into(),
            state: "active".into(),
            last_fired_occurrence_at: Some(130),
            last_evaluated_at: None,
            created_by: "agent".into(),
            created_at: 100,
            updated_at: 100,
        };
        assert_eq!(schedule.due_at(139), None);
        assert_eq!(schedule.due_at(140), Some(140));
    }
}
