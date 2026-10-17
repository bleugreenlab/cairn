//! Provider-owned pauses. The yielded head owns the wait; the row owns its clock.

use cairn_common::ids;
use cairn_db::turso::{params, Connection, Row};
use serde::{Deserialize, Serialize};

use crate::execution::jobs::{continue_job_impl, ResumeContext};
use crate::orchestrator::Orchestrator;
use crate::storage::{DbError, DbResult, LocalDb, RowExt};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderLimitHold {
    pub id: String,
    pub job_id: String,
    pub run_id: String,
    pub turn_id: String,
    pub backend: String,
    pub account_id: Option<String>,
    pub retry_not_before: Option<i64>,
    pub boundary_source: String,
    pub observed_text: Option<String>,
    pub observed_count: i64,
    pub launch_attempts: i64,
    pub state: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub resolved_at: Option<i64>,
    /// Why automatic resolution is withheld whatever the boundary says.
    pub resume_gate: Option<String>,
    /// The account whose reset the boundary is, when not this hold's own.
    pub boundary_account_id: Option<String>,
}

/// The limit landed while a tool call had no recorded result. Resuming could run
/// that call a second time, so only an explicit continue resumes the work.
pub const RESUME_GATE_TOOL_IN_FLIGHT: &str = "tool_in_flight";

impl ProviderLimitHold {
    /// Stable FNV-1a jitter, independent of process hash seeds and restarts.
    /// The jitter spreads jobs released by one shared reset; a switch to another
    /// account has no shared instant to spread, so it is due as declared. A gated
    /// hold keeps its boundary for the record but is never due.
    pub fn next_due_at(&self) -> Option<i64> {
        if self.state != "active" || self.resume_gate.is_some() {
            return None;
        }
        if self.boundary_source == "account_switch" {
            return self.retry_not_before;
        }
        let hash = self.id.bytes().fold(0xcbf29ce484222325_u64, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
        });
        self.retry_not_before?.checked_add((hash % 121) as i64)
    }

    pub fn due_at(&self, now: i64) -> Option<i64> {
        self.next_due_at().filter(|due| *due <= now)
    }
}

#[derive(Clone, Debug)]
pub struct ProviderLimitObservation {
    pub job_id: String,
    pub run_id: String,
    pub turn_id: String,
    pub backend: String,
    pub account_id: Option<String>,
    pub retry_not_before: Option<i64>,
    pub boundary_source: String,
    pub observed_text: Option<String>,
    /// [`RESUME_GATE_TOOL_IN_FLIGHT`] when a tool call had no recorded result.
    pub resume_gate: Option<&'static str>,
    /// The account whose reset `retry_not_before` is, when it is not this
    /// hold's own: the earliest any account the policy may select can serve.
    /// `boundary_source` then still says how this hold's own limit was seen.
    pub boundary_account_id: Option<String>,
    pub now: i64,
}

const COLUMNS: &str = "id, job_id, run_id, turn_id, backend, account_id, retry_not_before, boundary_source, observed_text, observed_count, launch_attempts, state, created_at, updated_at, resolved_at, resume_gate, boundary_account_id";

fn from_row(row: &Row) -> DbResult<ProviderLimitHold> {
    Ok(ProviderLimitHold {
        id: row.text(0)?,
        job_id: row.text(1)?,
        run_id: row.text(2)?,
        turn_id: row.text(3)?,
        backend: row.text(4)?,
        account_id: row.opt_text(5)?,
        retry_not_before: row.opt_i64(6)?,
        boundary_source: row.text(7)?,
        observed_text: row.opt_text(8)?,
        observed_count: row.i64(9)?,
        launch_attempts: row.i64(10)?,
        state: row.text(11)?,
        created_at: row.i64(12)?,
        updated_at: row.i64(13)?,
        resolved_at: row.opt_i64(14)?,
        resume_gate: row.opt_text(15)?,
        boundary_account_id: row.opt_text(16)?,
    })
}

async fn load_conn(conn: &Connection, id: &str) -> DbResult<Option<ProviderLimitHold>> {
    let mut rows = conn
        .query(
            &format!("SELECT {COLUMNS} FROM provider_limit_holds WHERE id=?1"),
            (id,),
        )
        .await?;
    rows.next().await?.as_ref().map(from_row).transpose()
}

/// Yield the provider-refused head and record the hold that owns it in one
/// owning-database transaction. Both land or neither does, so a provider-yielded
/// head never exists without an active hold to resolve it. A repeated observation
/// of a head that is already provider-yielded coalesces into its hold; a stale
/// observation never acquires ownership of a newer operator turn.
pub async fn yield_and_arm_provider_limit_hold(
    db: &LocalDb,
    observation: ProviderLimitObservation,
) -> Result<ProviderLimitHold, String> {
    let timerless = matches!(
        observation.boundary_source.as_str(),
        "unknown" | "credits_depleted"
    );
    if !matches!(
        observation.boundary_source.as_str(),
        "rate_limit_event" | "result_text" | "account_switch" | "credits_depleted" | "unknown"
    ) || (observation.boundary_account_id.is_none()
        && observation.retry_not_before.is_none() != timerless)
        || (observation.boundary_account_id.is_some() && observation.retry_not_before.is_none())
    {
        return Err("provider limit boundary and source disagree".into());
    }
    if observation
        .resume_gate
        .is_some_and(|gate| gate != RESUME_GATE_TOOL_IN_FLIGHT)
    {
        return Err("unknown provider limit resume gate".into());
    }
    db.write(move |conn| {
        let observation = observation.clone();
        Box::pin(async move {
            let o = observation;
            crate::transitions::turn::yield_running_turn_conn(
                conn,
                &o.turn_id,
                crate::models::TurnYieldReason::ProviderLimit,
                o.now,
            )
            .await?;
            // Whether this observation yielded the turn or an earlier one did, the
            // head must now be exactly this provider-yielded turn.
            let mut rows = conn.query(
                "SELECT 1 FROM jobs j JOIN turns t ON t.id=j.current_turn_id
                 JOIN runs r ON r.id=?3 AND r.job_id=j.id
                 WHERE j.id=?1 AND t.id=?2 AND t.job_id=j.id
                   AND t.state='yielded' AND t.yield_reason='provider_limit'",
                params![o.job_id.as_str(), o.turn_id.as_str(), o.run_id.as_str()],
            ).await?;
            if rows.next().await?.is_none() {
                return Err(DbError::internal("provider limit no longer owns the yielded job head".to_string()));
            }
            drop(rows);
            conn.execute(
                "UPDATE provider_limit_holds SET state='superseded', updated_at=?1
                 WHERE job_id=?2 AND state='active' AND turn_id<>?3",
                params![o.now, o.job_id.as_str(), o.turn_id.as_str()],
            ).await?;
            let mut rows = conn.query(
                "SELECT id FROM provider_limit_holds WHERE job_id=?1 AND state='active'",
                (o.job_id.as_str(),),
            ).await?;
            let existing = rows.next().await?.map(|row| row.text(0)).transpose()?;
            drop(rows);
            let id = if let Some(id) = existing {
                // NULL observations do not erase a previously declared boundary,
                // and no later observation lifts a resume gate.
                let later = "?1 IS NOT NULL AND (retry_not_before IS NULL OR ?1 > retry_not_before)";
                conn.execute(
                    &format!("UPDATE provider_limit_holds SET
                       boundary_source=CASE WHEN {later} THEN ?2 ELSE boundary_source END,
                       boundary_account_id=CASE WHEN {later} THEN ?7 ELSE boundary_account_id END,
                       retry_not_before=CASE WHEN retry_not_before IS NULL THEN ?1 WHEN ?1 IS NULL THEN retry_not_before ELSE MAX(retry_not_before, ?1) END,
                       resume_gate=COALESCE(resume_gate, ?6),
                       observed_text=?3, observed_count=observed_count+1, updated_at=?4
                     WHERE id=?5 AND state='active'"),
                    params![o.retry_not_before, o.boundary_source, o.observed_text, o.now, id.as_str(), o.resume_gate, o.boundary_account_id],
                ).await?;
                id
            } else {
                let id = ids::mint_child(&o.job_id);
                conn.execute(
                    "INSERT INTO provider_limit_holds
                     (id,job_id,run_id,turn_id,backend,account_id,retry_not_before,boundary_source,observed_text,created_at,updated_at,resume_gate,boundary_account_id)
                     VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?10,?11,?12)",
                    params![id.as_str(), o.job_id, o.run_id, o.turn_id, o.backend, o.account_id,
                        o.retry_not_before, o.boundary_source, o.observed_text, o.now, o.resume_gate,
                        o.boundary_account_id],
                ).await?;
                id
            };
            load_conn(conn, &id).await?.ok_or_else(|| DbError::internal("armed hold disappeared".to_string()))
        })
    }).await.map_err(|error| error.to_string())
}

/// Retire every active hold whose provider-yielded turn is no longer its job's
/// head. A hold exists to own that head; once an operator continues past it, the
/// hold has nothing left to resolve. A timed hold would learn this at its claim,
/// but a hold with no timer, or with automatic resume withheld, is never claimed.
pub async fn supersede_orphaned_provider_limit_holds(
    db: &LocalDb,
    now: i64,
) -> Result<u64, String> {
    db.execute(
        "UPDATE provider_limit_holds SET state='superseded', updated_at=?1
         WHERE state='active' AND NOT EXISTS (
           SELECT 1 FROM jobs j JOIN turns t ON t.id=j.current_turn_id
           WHERE j.id=provider_limit_holds.job_id AND t.id=provider_limit_holds.turn_id
             AND t.state='yielded' AND t.yield_reason='provider_limit')",
        params![now],
    )
    .await
    .map_err(|error| error.to_string())
}

pub async fn load_active_provider_limit_holds(
    db: &LocalDb,
) -> Result<Vec<ProviderLimitHold>, String> {
    db.query_all(
        &format!("SELECT {COLUMNS} FROM provider_limit_holds WHERE state='active'"),
        (),
        from_row,
    )
    .await
    .map_err(|error| error.to_string())
}

pub async fn cancel_provider_limit_holds_for_job(db: &LocalDb, job_id: &str) -> Result<(), String> {
    db.execute(
        "UPDATE provider_limit_holds SET state='cancelled', updated_at=?1 WHERE job_id=?2 AND state='active'",
        params![chrono::Utc::now().timestamp(), job_id],
    ).await.map(|_| ()).map_err(|error| error.to_string())
}

async fn set_state_conn(conn: &Connection, id: &str, state: &str, now: i64) -> DbResult<()> {
    conn.execute(
        "UPDATE provider_limit_holds SET state=?1, updated_at=?2 WHERE id=?3 AND state='active'",
        params![state, now, id],
    )
    .await?;
    Ok(())
}

/// Atomically consume a due hold and publish its sole pending successor.
async fn claim(
    db: &LocalDb,
    id: &str,
    now: i64,
) -> Result<Option<(ProviderLimitHold, String, String)>, String> {
    let id = id.to_string();
    db.write(move |conn| {
        let id = id.clone();
        Box::pin(async move {
            let Some(hold) = load_conn(conn, &id).await? else { return Ok(None) };
            if hold.due_at(now).is_none() { return Ok(None) }
            let mut rows = conn.query(
                "SELECT t.session_id, j.status, i.merged_at, i.closed_at, th.status
                 FROM jobs j JOIN turns t ON t.id=j.current_turn_id
                 LEFT JOIN executions e ON e.id=j.execution_id
                 LEFT JOIN issues i ON i.id=COALESCE(j.issue_id,e.issue_id)
                 LEFT JOIN threads th ON th.id=j.thread_id
                 WHERE j.id=?1 AND t.id=?2 AND t.state='yielded' AND t.yield_reason='provider_limit'",
                params![hold.job_id.as_str(), hold.turn_id.as_str()],
            ).await?;
            let Some(row) = rows.next().await? else {
                set_state_conn(conn, &id, "superseded", now).await?;
                return Ok(None)
            };
            let session_id = row.text(0)?;
            let ineligible = row.text(1)? == "cancelled" || row.opt_i64(2)?.is_some()
                || row.opt_i64(3)?.is_some()
                || row.opt_text(4)?.is_some_and(|status| status != "active");
            drop(rows);
            if ineligible {
                set_state_conn(conn, &id, "cancelled", now).await?;
                return Ok(None);
            }
            // Recheck inside the transaction as well as at the outer admission
            // edge: execution replacement can race the scheduler.
            if crate::execution::canonical::fence_for_job_conn(conn, &hold.job_id).await?.is_superseded() {
                set_state_conn(conn, &id, "superseded", now).await?;
                return Ok(None);
            }
            let successor = ids::mint_child(&hold.job_id);
            conn.execute(
                "INSERT INTO turns (id,session_id,job_id,sequence,predecessor_id,state,start_reason,created_at,updated_at)
                 SELECT ?1,?2,?3,COALESCE(MAX(sequence),0)+1,?4,'pending','wait_resolved',?5,?5 FROM turns WHERE session_id=?2",
                params![successor.as_str(), session_id.as_str(), hold.job_id.as_str(), hold.turn_id.as_str(), now],
            ).await?;
            let changed = conn.execute(
                "UPDATE jobs SET current_turn_id=?1, updated_at=?2 WHERE id=?3 AND current_turn_id=?4",
                params![successor.as_str(), now, hold.job_id.as_str(), hold.turn_id.as_str()],
            ).await?;
            if changed != 1 {
                return Err(DbError::internal("provider hold lost its head during claim".to_string()));
            }
            conn.execute(
                "UPDATE provider_limit_holds SET state='resolved', resolved_at=?1, updated_at=?1 WHERE id=?2",
                params![now, id],
            ).await?;
            Ok(Some((hold, successor, session_id)))
        })
    }).await.map_err(|error| error.to_string())
}

/// Restore only an undriven successor still owned by this claim. Never rewind
/// an operator's newer head or a successor that actually began doing work.
async fn restore_after_launch_failure(
    db: &LocalDb,
    hold: &ProviderLimitHold,
    successor: &str,
    now: i64,
) -> Result<bool, String> {
    let hold = hold.clone();
    let successor = successor.to_string();
    db.write(move |conn| {
        let hold = hold.clone();
        let successor = successor.clone();
        Box::pin(async move {
            let changed = conn.execute(
                "UPDATE turns SET state='cancelled', end_reason='launch_refused', ended_at=?1, updated_at=?1, predecessor_id=NULL
                 WHERE id=?2 AND (state='pending' OR (state='cancelled' AND end_reason='launch_refused'))
                   AND id=(SELECT current_turn_id FROM jobs WHERE id=?3)
                   AND EXISTS (SELECT 1 FROM turns WHERE id=?4 AND state='yielded' AND yield_reason='provider_limit')
                   AND EXISTS (SELECT 1 FROM provider_limit_holds WHERE id=?5 AND state='resolved')",
                params![now, successor.as_str(), hold.job_id.as_str(), hold.turn_id.as_str(), hold.id.as_str()],
            ).await?;
            if changed == 0 { return Ok(false) }
            // Detach the unserved launch from the successor chain so an explicit
            // Continue can still create a successor from the restored head.
            conn.execute(
                "UPDATE jobs SET current_turn_id=?1, updated_at=?2 WHERE id=?3 AND current_turn_id=?4",
                params![hold.turn_id, now, hold.job_id, successor],
            ).await?;
            conn.execute(
                "UPDATE provider_limit_holds SET state='active', resolved_at=NULL,
                   launch_attempts=launch_attempts+1,
                   retry_not_before=CASE WHEN launch_attempts+1 >= 3 THEN NULL ELSE ?1 END,
                   updated_at=?2 WHERE id=?3 AND state='resolved'",
                params![now.saturating_add(300), now, hold.id],
            ).await?;
            Ok(true)
        })
    }).await.map_err(|error| error.to_string())
}

/// Cold admission can fail after the resolver returned: queued launches belong
/// to the admission worker, not the original call stack. Recover its pending
/// provider successor before generic failure projection can settle the DAG.
pub(crate) async fn restore_provider_hold_after_admission_failure(
    orch: &Orchestrator,
    db: &LocalDb,
    successor: &str,
    now: i64,
) -> Result<bool, String> {
    let successor_id = successor.to_string();
    let hold = db.read(move |conn| {
        let successor_id = successor_id.clone();
        Box::pin(async move {
            let mut rows = conn.query(
                "SELECT h.id FROM provider_limit_holds h
                 JOIN turns t ON t.predecessor_id=h.turn_id AND t.job_id=h.job_id
                 WHERE t.id=?1 AND t.start_reason='wait_resolved' AND h.state='resolved'
                   AND (t.state='pending' OR (t.state='cancelled' AND t.end_reason='launch_refused'))
                 ORDER BY h.created_at DESC LIMIT 1",
                (successor_id.as_str(),),
            ).await?;
            let id = rows.next().await?.map(|row| row.text(0)).transpose()?;
            drop(rows);
            match id {
                Some(id) => load_conn(conn, &id).await,
                None => Ok(None),
            }
        })
    }).await.map_err(|error| error.to_string())?;
    let Some(hold) = hold else { return Ok(false) };
    if restore_after_launch_failure(db, &hold, successor, now).await? {
        crate::execution::advancement::recompute_job(orch, &hold.job_id)?;
    }
    // A newer head is authoritative too: this stale launch must not fail it.
    Ok(true)
}

/// Whether a due hold's work has an account to run on. A boundary is a reading
/// taken when the hold was armed; by the time it comes due, the account it
/// named may have been limited again. Launching anyway would only spend one of
/// the hold's launch attempts on a refusal, so a hold whose launch routing would
/// refuse re-arms to when a subscription the policy may select next can serve,
/// or to no timer when none declares a time, and launches nothing. Whether the
/// launch would be refused is asked of launch routing itself, so a healthy pin
/// or an allowed API-key fallback launches exactly as a continue would.
async fn hold_can_launch(
    orch: &Orchestrator,
    db: &LocalDb,
    hold: &ProviderLimitHold,
    now: i64,
) -> Result<bool, String> {
    use crate::credentials::{RoutedAvailability, RoutedProvider};
    let Some(provider) = RoutedProvider::for_backend(&hold.backend) else {
        return Ok(true);
    };
    let job = hold.job_id.clone();
    let Some((project_id, model, pinned)) = db
        .query_opt(
            "SELECT COALESCE(j.project_id, i.project_id), j.model, s.account_id FROM jobs j
               LEFT JOIN issues i ON i.id = j.issue_id
               LEFT JOIN sessions s ON s.id = j.current_session_id
              WHERE j.id = ?1",
            (job,),
            |row| Ok((row.opt_text(0)?, row.opt_text(1)?, row.opt_text(2)?)),
        )
        .await
        .map_err(|error| error.to_string())?
    else {
        return Ok(true);
    };
    let snapshot =
        crate::orchestrator::session::snapshot_account_override_for_job(orch, &hold.job_id)?;
    if crate::orchestrator::session::routed_session_can_launch(
        orch,
        provider,
        pinned.as_deref(),
        project_id.as_deref(),
        model.as_deref(),
        snapshot.as_deref(),
    ) {
        return Ok(true);
    }
    let explicit = snapshot.or_else(|| {
        crate::orchestrator::session::routed_project_override(orch, provider, project_id.as_deref())
    });
    let (at, account) = match orch.routed_availability(
        provider,
        project_id.as_deref(),
        explicit.as_deref(),
        model.as_deref(),
        now,
    ) {
        RoutedAvailability::Now(_) | RoutedAvailability::NoCandidates => return Ok(true),
        RoutedAvailability::At(at, _) if at <= now => return Ok(true),
        RoutedAvailability::At(at, account) => (Some(at), Some(account)),
        RoutedAvailability::Unknown => (None, None),
    };
    let account = account.filter(|account| hold.account_id.as_deref() != Some(account.as_str()));
    log::info!(
        "Provider hold {} came due with no account able to serve; re-armed to {at:?}",
        hold.id
    );
    let id = hold.id.clone();
    db.write(move |conn| {
        let id = id.clone();
        let account = account.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE provider_limit_holds SET retry_not_before=?1, boundary_account_id=?2,
                   updated_at=?3
                 WHERE id=?4 AND state='active'",
                params![at, account, now, id.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|error| error.to_string())?;
    Ok(false)
}

/// Resolve through owned-wait admission, never through an attention push or the
/// model-capacity retry path (both would mint a competing successor).
pub async fn resolve_provider_limit_hold(
    orch: &Orchestrator,
    hold: &ProviderLimitHold,
    now: i64,
) -> Result<bool, String> {
    let db = crate::execution::routing::owning_db_for_job(&orch.db, &hold.job_id)
        .await
        .map_err(|error| error.to_string())?;
    if hold.due_at(now).is_none() {
        return Ok(false);
    }
    if crate::execution::canonical::job_refusal(&db, &hold.job_id).is_some() {
        let id = hold.id.clone();
        db.write(move |conn| {
            let id = id.clone();
            Box::pin(async move { set_state_conn(conn, &id, "superseded", now).await })
        })
        .await
        .map_err(|error| error.to_string())?;
        return Ok(false);
    }
    if !hold_can_launch(orch, &db, hold, now).await? {
        return Ok(false);
    }
    let Some((hold, successor, session)) = claim(&db, &hold.id, now).await? else {
        return Ok(false);
    };
    let note = if hold.boundary_source == "account_switch" {
        "Continuing the unfinished work on the replacement account."
    } else {
        "The provider usage limit has reset. Continuing the unfinished work."
    };
    if let Err(error) = crate::messages::transcript::insert_system_message_sync(
        orch,
        &hold.run_id,
        Some(&session),
        Some(&hold.turn_id),
        note,
        serde_json::json!({"provider": hold.backend, "kind": "provider_limit_resolved", "holdId": hold.id}),
    ) {
        log::warn!(
            "Failed to record provider hold resolution {}: {error}",
            hold.id
        );
    }
    let result = continue_job_impl(
        orch,
        &hold.job_id,
        None,
        None,
        Some(ResumeContext {
            preclaimed_successor_turn_id: Some(successor.clone()),
            suppress_user_event: true,
            suppress_self_suspend_note: true,
            unwind_preclaimed_start_as: Some(crate::models::TurnEndReason::LaunchRefused),
            ..Default::default()
        }),
    );
    if let Err(error) = result {
        restore_after_launch_failure(&db, &hold, &successor, now).await?;
        crate::execution::advancement::recompute_job(orch, &hold.job_id)?;
        return Err(format!("Provider hold {} resume failed: {error}", hold.id));
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jitter_is_stable_bounded_and_unknown_never_due() {
        let mut hold = ProviderLimitHold {
            id: "stable-hold".into(),
            job_id: "j".into(),
            run_id: "r".into(),
            turn_id: "t".into(),
            backend: "claude".into(),
            account_id: None,
            retry_not_before: Some(1000),
            boundary_source: "result_text".into(),
            observed_text: None,
            observed_count: 1,
            launch_attempts: 0,
            state: "active".into(),
            created_at: 1,
            updated_at: 1,
            resolved_at: None,
            resume_gate: None,
            boundary_account_id: None,
        };
        let due = hold.next_due_at().unwrap();
        assert!((1000..=1120).contains(&due));
        assert_eq!(hold.clone().next_due_at(), Some(due));
        assert_eq!(hold.due_at(due - 1), None);
        assert_eq!(hold.due_at(due), Some(due));
        hold.retry_not_before = None;
        assert_eq!(hold.due_at(i64::MAX), None);
        hold.retry_not_before = Some(1000);
        hold.boundary_source = "account_switch".into();
        assert_eq!(
            hold.next_due_at(),
            Some(1000),
            "an account switch is not jittered"
        );
        hold.resume_gate = Some(RESUME_GATE_TOOL_IN_FLIGHT.into());
        assert_eq!(
            hold.due_at(i64::MAX),
            None,
            "a gated hold is never due, whatever its boundary"
        );
    }
}
