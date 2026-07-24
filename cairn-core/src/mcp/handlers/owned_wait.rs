use super::permission::{
    emit_successor_turn_events, ensure_wait_resolved_successor, yield_turn_for_host, WaitSuccessor,
};
use super::run::{TerminalWaitEvent, WaitDuration, WaitFor};
use super::tool_use_correlation::resolve_tool_use_id;
use crate::execution::jobs::{continue_job_impl, ResumeContext};
use crate::mcp::types::McpCallbackRequest;
use crate::models::{TurnState, TurnYieldReason};
use crate::orchestrator::Orchestrator;
use crate::storage::{DbError, LocalDb, RowExt};
use cairn_db::turso::params;
use std::{sync::Arc, time::Duration};

const INLINE_BUDGET: Duration = Duration::from_secs(45);
const MAX_MS: u64 = 7 * 24 * 60 * 60 * 1000;
// In-process poll for a racing continuation to START the predecessor's successor
// it just created. There is deliberately NO fixed cutoff: a healthy continuation
// can legitimately take a while before `start_turn` (cold process spawn, DB
// contention), so the resolver rechecks with exponential backoff (capped) for as
// long as the host lives. Shutdown drops this task, handing ownership to startup
// reconciliation.
const COLLISION_POLL_INITIAL: Duration = Duration::from_millis(50);
const COLLISION_POLL_MAX: Duration = Duration::from_secs(2);

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Condition {
    Duration,
    Terminal {
        uri: String,
        slug: String,
        on: TerminalWaitEvent,
        phrase: Option<String>,
    },
}

async fn resolve_wait_tool_use_id(
    db: &LocalDb,
    run_id: &str,
    turn_id: &str,
    wait: &WaitFor,
) -> Option<String> {
    let expected_wait = serde_json::to_value(wait).ok()?;
    resolve_tool_use_id(db, run_id, Some(turn_id), |name, input| {
        if name != "run" && !name.ends_with("__run") {
            return false;
        }
        let Some(commands) = input.get("commands").and_then(|value| value.as_array()) else {
            return false;
        };
        let [item] = commands.as_slice() else {
            return false;
        };
        let Some(item) = item.as_object() else {
            return false;
        };
        item.len() == 1 && item.get("waitFor") == Some(&expected_wait)
    })
    .await
}
#[derive(Clone)]
struct Record {
    id: String,
    job_id: String,
    run_id: String,
    session_id: String,
    turn_id: String,
    tool_use_id: String,
    condition: Condition,
    deadline: Option<i64>,
    created: i64,
}

pub(crate) async fn handle_owned_wait(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    wait: &WaitFor,
) -> String {
    let (ctx, db) = match super::run_context::lookup_run_routed(&orch.db, request).await {
        Ok(value) => value,
        Err(error) => return error,
    };
    let turn_id = match orch.process_state.get_current_turn_id(&ctx.run_id) {
        Some(value) => value,
        None => return "waitFor requires an active turn".into(),
    };
    let tool_use_id = match request.tool_use_id.clone() {
        Some(value) => value,
        None => match resolve_wait_tool_use_id(&db, &ctx.run_id, &turn_id, wait).await {
            Some(value) => value,
            None => {
                return "Could not correlate waitFor with its originating run tool call; retry the wait from the active assistant turn.".into()
            }
        },
    };
    let created = chrono::Utc::now().timestamp_millis();
    let (condition, deadline) = match normalize(orch, request, wait, created).await {
        Ok(value) => value,
        Err(error) => return error,
    };
    let session_id = match run_session(&db, &ctx.run_id).await {
        Ok(Some(value)) => value,
        Ok(None) => return "waitFor requires an active session".into(),
        Err(error) => return error,
    };
    let record = Record {
        id: cairn_common::ids::mint_child(&ctx.run_id),
        job_id: ctx.job_id,
        run_id: ctx.run_id,
        session_id,
        turn_id,
        tool_use_id,
        condition,
        deadline,
        created,
    };

    // The fast path remains wholly inside the live predecessor turn. Only a
    // budget expiry establishes durable suspension; the slow trigger reuses the
    // absolute deadline or level-triggered terminal state.
    match tokio::time::timeout(
        INLINE_BUDGET,
        trigger(orch.clone(), db.clone(), record.clone()),
    )
    .await
    {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => error,
        Err(_) => {
            // Durable path. Establish suspension BEFORE arming the trigger: persist
            // the wait row and yield the turn, mark the in-memory turn yielded, then
            // park the warm process. Only then spawn the resolver. Arming first let
            // an already-elapsed duration resolve and resume the agent, which the
            // park below would then re-suspend — the duration-specific "never
            // resumed" race (CAIRN-2970).
            if let Err(error) = insert(&db, &record).await {
                return error;
            }
            orch.process_state
                .yield_for_host(&record.run_id, &record.turn_id);
            if let Err(error) = crate::orchestrator::lifecycle::suspend_run_for_durable_wait(
                orch,
                &record.run_id,
                "owned_wait_suspended",
            ) {
                // Parking failed: leave the pending row for startup reconciliation
                // rather than launching a resolver against a still-active
                // predecessor.
                log::warn!(
                    "owned wait suspend failed for run {}: {error}",
                    record.run_id
                );
                return format!("Failed to suspend waitFor: {error}");
            }
            let (owned_orch, owned_db, owned_record) = (orch.clone(), db.clone(), record.clone());
            tokio::spawn(async move {
                let result =
                    match trigger(owned_orch.clone(), owned_db.clone(), owned_record.clone()).await
                    {
                        Ok(result) => result,
                        Err(error) => {
                            serde_json::json!({"outcome":"error","error":error}).to_string()
                        }
                    };
                if let Err(error) =
                    resolve_slow(&owned_orch, &owned_db, &owned_record, &result, false).await
                {
                    log::warn!("owned wait resolution failed: {error}");
                }
            });
            "Wait suspended; the same run call will resume when its condition fires.".into()
        }
    }
}

async fn normalize(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    wait: &WaitFor,
    now: i64,
) -> Result<(Condition, Option<i64>), String> {
    match wait {
        WaitFor::Duration { duration } => {
            let ms = parse_duration(duration)?;
            Ok((Condition::Duration, Some(now + ms as i64)))
        }
        WaitFor::Terminal {
            reference,
            on,
            phrase,
            ..
        } => {
            let uri = if reference.starts_with("cairn:~/") {
                let home = super::run_context::lookup_home_uri_routed(&orch.db, request).await?;
                format!("{}{}", home.trim_end_matches('/'), &reference[7..])
            } else {
                reference.clone()
            };
            let slug = uri
                .rsplit_once("/terminal/")
                .map(|v| v.1)
                .filter(|v| !v.is_empty() && !v.contains('/'))
                .ok_or("terminal ref must be a canonical terminal URI")?
                .to_string();
            match on {
                TerminalWaitEvent::Exit if phrase.is_some() => {
                    return Err("terminal exit wait does not accept phrase".into())
                }
                TerminalWaitEvent::Output
                    if phrase.as_deref().map(str::trim).unwrap_or("").is_empty() =>
                {
                    return Err("terminal output wait requires phrase".into())
                }
                _ => {}
            }
            Ok((
                Condition::Terminal {
                    uri,
                    slug,
                    on: on.clone(),
                    phrase: phrase.clone(),
                },
                None,
            ))
        }
    }
}
fn parse_duration(v: &WaitDuration) -> Result<u64, String> {
    let ms = match v {
        WaitDuration::Milliseconds(v) => *v,
        WaitDuration::Human(v) => {
            let p = v
                .find(|c: char| !c.is_ascii_digit())
                .ok_or("duration needs ms, s, m, h, or d")?;
            let n: u64 = v[..p].parse().map_err(|_| "invalid duration")?;
            let f = match &v[p..] {
                "ms" => 1,
                "s" => 1000,
                "m" => 60000,
                "h" => 3600000,
                "d" => 86400000,
                _ => return Err("duration needs ms, s, m, h, or d".into()),
            };
            n.checked_mul(f).ok_or("duration too large")?
        }
    };
    if ms == 0 || ms > MAX_MS {
        return Err("duration must be between 1ms and 7d".into());
    }
    Ok(ms)
}
async fn run_session(db: &LocalDb, id: &str) -> Result<Option<String>, String> {
    let id = id.to_string();
    db.read(|c| {
        let id = id.clone();
        Box::pin(async move {
            let mut r = c
                .query("SELECT session_id FROM runs WHERE id=?1", params![id])
                .await?;
            r.next()
                .await?
                .map(|x| x.opt_text(0))
                .transpose()
                .map(Option::flatten)
        })
    })
    .await
    .map_err(|e| e.to_string())
}
async fn insert(db: &LocalDb, r: &Record) -> Result<(), String> {
    let r = r.clone();
    db.write(|c|{let r=r.clone();Box::pin(async move{let json=serde_json::to_string(&r.condition).map_err(|e|DbError::internal(e.to_string()))?;c.execute("INSERT INTO agent_waits(id,job_id,run_id,session_id,predecessor_turn_id,tool_use_id,condition_json,deadline_ms,state,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,'pending',?9)",params![r.id.as_str(),r.job_id.as_str(),r.run_id.as_str(),r.session_id.as_str(),r.turn_id.as_str(),r.tool_use_id.as_str(),json,r.deadline,r.created]).await?;yield_turn_for_host(c,&r.turn_id,TurnYieldReason::Wait).await.map_err(|e|DbError::internal(e.to_string()))?;Ok(())})}).await.map_err(|e|format!("Failed to establish waitFor: {e}"))
}
async fn trigger(orch: Orchestrator, db: Arc<LocalDb>, r: Record) -> Result<String, String> {
    match &r.condition {
        Condition::Duration => {
            let deadline = r.deadline.unwrap();
            let left = deadline - chrono::Utc::now().timestamp_millis();
            if left > 0 {
                tokio::time::sleep(Duration::from_millis(left as u64)).await
            }
            Ok(serde_json::json!({"outcome":"elapsed","elapsedMs":chrono::Utc::now().timestamp_millis()-r.created,"deadlineMs":deadline}).to_string())
        }
        Condition::Terminal {
            uri,
            slug,
            on,
            phrase,
        } => loop {
            if let Some(v) =
                terminal(&orch, &db, &r.job_id, uri, slug, on, phrase.as_deref()).await?
            {
                return Ok(v);
            }
            tokio::time::sleep(Duration::from_millis(100)).await
        },
    }
}
async fn terminal(
    orch: &Orchestrator,
    db: &LocalDb,
    job: &str,
    uri: &str,
    slug: &str,
    on: &TerminalWaitEvent,
    phrase: Option<&str>,
) -> Result<Option<String>, String> {
    let (j, s) = (job.to_string(), slug.to_string());
    let row=db.read(|c|{let (j,s)=(j.clone(),s.clone());Box::pin(async move{let mut r=c.query("SELECT session_id,status,exit_code,output_tail FROM job_terminals WHERE job_id=?1 AND slug=?2",params![j,s]).await?;r.next().await?.map(|x|Ok((x.text(0)?,x.text(1)?,x.opt_i64(2)?,x.opt_text(3)?))).transpose()})}).await.map_err(|e|e.to_string())?;
    let Some((sid, status, code, tail)) = row else {
        return Err(format!("Terminal not found: {uri}"));
    };
    let exited = status == "exited";
    if matches!(on, TerminalWaitEvent::Exit) && exited {
        return Ok(Some(
            serde_json::json!({"outcome":"exited","terminal":uri,"exitCode":code,"excerpt":tail})
                .to_string(),
        ));
    }
    if matches!(on, TerminalWaitEvent::Output) {
        let live = orch
            .pty_state
            .sessions
            .lock()
            .ok()
            .and_then(|m| m.get(&sid).cloned())
            .and_then(|s| s.lock().ok()?.output_buffer.clone())
            .and_then(|b| {
                let bytes: Vec<u8> = b.lock().ok()?.iter().copied().collect();
                Some(String::from_utf8_lossy(&bytes).to_string())
            })
            .or(tail.clone())
            .unwrap_or_default();
        if let Some(p) = phrase {
            if let Some(ex) = crate::services::scan_for_phrase("", &live, p).matched_excerpt {
                return Ok(Some(serde_json::json!({"outcome":"matched","terminal":uri,"phrase":p,"exitCode":code,"excerpt":ex}).to_string()));
            }
        }
        if exited {
            return Ok(Some(serde_json::json!({"outcome":"terminal_exited","terminal":uri,"phrase":phrase,"exitCode":code,"excerpt":tail}).to_string()));
        }
    }
    Ok(None)
}
async fn resolve_slow(
    orch: &Orchestrator,
    db: &LocalDb,
    r: &Record,
    result: &str,
    replay: bool,
) -> Result<(), String> {
    // First-writer-wins claim: pending -> resolving. The row count distinguishes
    // the live resolver that won the transition from a duplicate that lost it; a
    // startup replay of an existing `resolving` row re-drives idempotently. A crash
    // from here on leaves a `resolving` row that replay re-drives idempotently
    // instead of losing the resume behind a `resolved` row.
    let (id, out) = (r.id.clone(), result.to_string());
    let claimed = db
        .write(|c| {
            let (id, out) = (id.clone(), out.clone());
            Box::pin(async move {
                let changed = c
                    .execute(
                        "UPDATE agent_waits SET state='resolving',resolution_json=?2 WHERE id=?1 AND state='pending'",
                        params![id, out],
                    )
                    .await?;
                Ok(changed)
            })
        })
        .await
        .map_err(|e| e.to_string())?;

    let Some((state, stored_result, stored_successor)) = load_resolution(db, &r.id).await? else {
        return Err(format!("owned wait disappeared: {}", r.id));
    };
    match state.as_str() {
        // User Stop cancelled the wait, or a prior resolution already finished it.
        "cancelled" | "resolved" => return Ok(()),
        "resolving" => {}
        other => return Err(format!("owned wait has invalid resolution state: {other}")),
    }
    // A live resolver that did not win the pending -> resolving transition defers to
    // the winner. Only a startup replay legitimately re-drives a `resolving` row.
    if !replay && claimed == 0 {
        return Ok(());
    }
    let result = stored_result.as_deref().unwrap_or(result);

    // Resolve the wait's own WaitResolved successor by explicit identity, record it,
    // deliver the synthetic result once, and drive exactly one continuation. A
    // racing continuation that already claimed the predecessor's single successor
    // yields a collision: defer to it rather than hijacking a foreign turn.
    let mut backoff = COLLISION_POLL_INITIAL;
    loop {
        match ensure_wait_successor(orch, db, r, stored_successor.clone()).await? {
            WaitSuccessorOutcome::Ready { turn_id, state } => {
                persist_successor(db, &r.id, &turn_id).await?;
                // Deliver the synthetic tool result to the predecessor exactly once.
                store_result_once(orch, db, r, result).await?;
                // Drive continuation only while the successor still awaits its start.
                // A successor already started (or interrupted by a crash mid-resume)
                // means the resume was already delivered once; replay must not launch
                // a second one — crash recovery owns an interrupted successor here.
                if state == TurnState::Pending {
                    continue_job_impl(
                        orch,
                        &r.job_id,
                        Some(result),
                        None,
                        Some(ResumeContext {
                            suppress_user_event: true,
                            preclaimed_successor_turn_id: Some(turn_id),
                            ..Default::default()
                        }),
                    )?;
                }
                break;
            }
            WaitSuccessorOutcome::Collision {
                state: TurnState::Pending,
                ..
            } => {
                // The predecessor's single successor was created by another
                // continuation that has NOT started it yet, so the run is still parked
                // — it has not resumed. Recheck (with backoff) until that continuation
                // starts it (running) or terminalizes it, rather than falsely
                // resolving now (which would strand the parked agent) or giving up on
                // an arbitrary cutoff a slow-but-healthy continuation could exceed.
                // Host shutdown drops this task; startup reconciliation re-drives the
                // still-`resolving` row.
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(COLLISION_POLL_MAX);
            }
            WaitSuccessorOutcome::Collision {
                turn_id,
                start_reason,
                state,
            } => {
                // A running or already-finished continuation resumed the run through
                // the predecessor's successor (e.g. a user steer mid-wait): the run
                // resumed exactly once via that path. Record the awaited result
                // durably and resolve without a second, competing resume. Live
                // delivery into an already-active turn is precluded by
                // one-active-turn, so the result is preserved in the transcript.
                log::warn!(
                    "owned wait {} resolved into an existing {start_reason} successor {turn_id} (state {state}); recorded result without a second resume",
                    r.id
                );
                store_result_once(orch, db, r, result).await?;
                break;
            }
        }
    }

    // Mark resolved (idempotent CAS on the resolving state).
    let id = r.id.clone();
    db.write(|c|{let id=id.clone();Box::pin(async move{c.execute("UPDATE agent_waits SET state='resolved',resolved_at=?2 WHERE id=?1 AND state='resolving'",params![id,chrono::Utc::now().timestamp_millis()]).await?;Ok(())})}).await.map_err(|e|e.to_string())?;
    Ok(())
}

/// Resolution of this wait's successor turn.
enum WaitSuccessorOutcome {
    /// The wait's own `WaitResolved` successor and its current state. A `Pending`
    /// state still awaits its start (drive the continuation); any other state means
    /// the resume was already delivered (replay must not re-drive it).
    Ready { turn_id: String, state: TurnState },
    /// A racing continuation already owns the predecessor's single successor; the
    /// wait must not hijack that foreign turn. `state` decides the handling — a
    /// pending foreign turn means the run has not resumed yet.
    Collision {
        turn_id: String,
        start_reason: String,
        state: TurnState,
    },
}

/// Resolve this wait's `WaitResolved` successor by explicit identity (never by
/// predecessor alone): reuse the wait's own successor on replay, create it pending
/// when absent, and report a collision when a foreign continuation already claimed
/// the predecessor's single successor.
async fn ensure_wait_successor(
    orch: &Orchestrator,
    db: &LocalDb,
    r: &Record,
    stored_successor: Option<String>,
) -> Result<WaitSuccessorOutcome, String> {
    let outcome = ensure_wait_resolved_successor(db, &r.run_id, &r.turn_id)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("owned wait successor context missing for run {}", r.run_id))?;
    match outcome {
        WaitSuccessor::Ready(update) => {
            // Emits the insert db-change only when this attempt created the turn.
            emit_successor_turn_events(db, &*orch.services.emitter, &update).await;
            if let Some(stored) = stored_successor.as_deref() {
                if stored != update.turn_id {
                    log::warn!(
                        "owned wait {} recorded successor {stored} but resolved {}",
                        r.id,
                        update.turn_id
                    );
                }
            }
            let state = load_turn_state(db, &update.turn_id).await?;
            Ok(WaitSuccessorOutcome::Ready {
                turn_id: update.turn_id,
                state,
            })
        }
        WaitSuccessor::Collision {
            turn_id,
            start_reason,
            state,
        } => Ok(WaitSuccessorOutcome::Collision {
            turn_id,
            start_reason,
            state,
        }),
    }
}

async fn load_turn_state(db: &LocalDb, turn_id: &str) -> Result<TurnState, String> {
    let turn_id = turn_id.to_string();
    db.read(|c| {
        let turn_id = turn_id.clone();
        Box::pin(async move {
            let mut rows = c
                .query(
                    "SELECT state FROM turns WHERE id=?1 LIMIT 1",
                    params![turn_id.clone()],
                )
                .await?;
            let state = rows
                .next()
                .await?
                .ok_or_else(|| DbError::Row(format!("turn not found: {turn_id}")))?
                .text(0)?;
            state.parse::<TurnState>().map_err(DbError::Row)
        })
    })
    .await
    .map_err(|e| e.to_string())
}

async fn load_resolution(
    db: &LocalDb,
    id: &str,
) -> Result<Option<(String, Option<String>, Option<String>)>, String> {
    let id = id.to_string();
    db.read(|c| {
        let id = id.clone();
        Box::pin(async move {
            let mut rows = c
                .query(
                    "SELECT state,resolution_json,successor_turn_id FROM agent_waits WHERE id=?1",
                    params![id],
                )
                .await?;
            rows.next()
                .await?
                .map(|row| Ok((row.text(0)?, row.opt_text(1)?, row.opt_text(2)?)))
                .transpose()
        })
    })
    .await
    .map_err(|e| e.to_string())
}

async fn persist_successor(db: &LocalDb, id: &str, successor: &str) -> Result<(), String> {
    let (id, successor) = (id.to_string(), successor.to_string());
    db.write(|c|{let (id,successor)=(id.clone(),successor.clone());Box::pin(async move{c.execute("UPDATE agent_waits SET successor_turn_id=COALESCE(successor_turn_id,?2) WHERE id=?1 AND state='resolving'",params![id,successor]).await?;Ok(())})}).await.map_err(|e|e.to_string())
}

async fn store_result_once(
    orch: &Orchestrator,
    db: &LocalDb,
    r: &Record,
    result: &str,
) -> Result<(), String> {
    let id = r.id.clone();
    let already_stored = db
        .read(|c| {
            let id = id.clone();
            Box::pin(async move {
                let mut rows = c
                    .query(
                        "SELECT result_stored_at FROM agent_waits WHERE id=?1",
                        params![id],
                    )
                    .await?;
                Ok(rows
                    .next()
                    .await?
                    .and_then(|row| row.opt_i64(0).ok().flatten())
                    .is_some())
            })
        })
        .await
        .map_err(|e| e.to_string())?;
    if already_stored {
        return Ok(());
    }
    crate::execution::jobs::store_tool_result_event_with_turn(
        orch,
        &r.run_id,
        &r.session_id,
        &r.tool_use_id,
        result,
        false,
        chrono::Utc::now().timestamp() as i32,
        Some(&r.turn_id),
    )?;
    let id = r.id.clone();
    db.write(|c| {
        let id = id.clone();
        Box::pin(async move {
            c.execute(
                "UPDATE agent_waits SET result_stored_at=COALESCE(result_stored_at,?2) WHERE id=?1",
                params![id, chrono::Utc::now().timestamp_millis()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|e| e.to_string())
}
pub(crate) async fn reconcile_owned_waits(orch: &Orchestrator) {
    for db in orch.db.all_dbs().await {
        let records = db.read(|conn| Box::pin(async move {
            let mut rows = conn.query(
                "SELECT id,job_id,run_id,session_id,predecessor_turn_id,tool_use_id,condition_json,deadline_ms,created_at,state,resolution_json FROM agent_waits WHERE state IN ('pending','resolving')",
                (),
            ).await?;
            let mut found = Vec::new();
            while let Some(row) = rows.next().await? {
                let condition_json = row.text(6)?;
                let condition = serde_json::from_str::<Condition>(&condition_json)
                    .map_err(|error| DbError::internal(error.to_string()))?;
                found.push((Record {
                    id: row.text(0)?, job_id: row.text(1)?, run_id: row.text(2)?,
                    session_id: row.text(3)?, turn_id: row.text(4)?, tool_use_id: row.text(5)?,
                    condition, deadline: row.opt_i64(7)?, created: row.i64(8)?,
                }, row.text(9)?, row.opt_text(10)?));
            }
            Ok(found)
        })).await;
        let Ok(records) = records else {
            log::warn!("failed to load pending owned waits during startup");
            continue;
        };
        for (record, state, stored_result) in records {
            let (owned_orch, owned_db) = (orch.clone(), db.clone());
            tokio::spawn(async move {
                let result = if state == "resolving" {
                    stored_result.unwrap_or_else(|| serde_json::json!({"outcome":"error","error":"wait resolution payload missing"}).to_string())
                } else {
                    match trigger(owned_orch.clone(), owned_db.clone(), record.clone()).await {
                        Ok(result) => result,
                        Err(error) => {
                            serde_json::json!({"outcome":"error","error":error}).to_string()
                        }
                    }
                };
                if let Err(error) =
                    resolve_slow(&owned_orch, &owned_db, &record, &result, true).await
                {
                    log::warn!("startup owned wait resolution failed: {error}");
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{MigrationRunner, SearchIndex, TURSO_MIGRATIONS};

    async fn test_orchestrator() -> Orchestrator {
        let root = tempfile::tempdir().unwrap().keep();
        let local = LocalDb::open(root.join("test.db")).await.unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&local)
            .await
            .unwrap();
        for sql in [
            "INSERT INTO workspaces (id,name,created_at,updated_at) VALUES ('w','W',1,1)",
            "INSERT INTO projects (id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES ('p','w','P','PRJ','/tmp/p',1,1)",
            "INSERT INTO issues (id,project_id,number,title,status,created_at,updated_at) VALUES ('i','p',1,'T','active',1,1)",
            "INSERT INTO executions (id,recipe_id,issue_id,project_id,status,started_at,seq) VALUES ('e','recipe','i','p','running',1,1)",
            "INSERT INTO jobs (id,execution_id,issue_id,project_id,node_name,status,created_at,updated_at,uri_segment) VALUES ('job-1','e','i','p','Builder','running',1,1,'builder')",
            "INSERT INTO runs (id,issue_id,project_id,job_id,status,created_at,updated_at) VALUES ('run-1','i','p','job-1','live',1,1)",
            "INSERT INTO turns (id,session_id,run_id,job_id,sequence,state,created_at,updated_at) VALUES ('old-turn','session-1','run-1','job-1',1,'completed',1,1)",
            "INSERT INTO turns (id,session_id,run_id,job_id,sequence,state,created_at,updated_at) VALUES ('current-turn','session-1','run-1','job-1',2,'active',2,2)",
        ] {
            local.execute(sql, ()).await.unwrap();
        }
        let search = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
        Orchestrator::builder(
            Arc::new(DbState::new(Arc::new(local), search)),
            Arc::new(TestServicesBuilder::new().build()),
            root,
        )
        .build()
    }

    async fn insert_assistant_event(
        db: &LocalDb,
        id: &str,
        run_id: &str,
        turn_id: &str,
        sequence: i64,
        data: serde_json::Value,
    ) {
        let id = id.to_string();
        let run_id = run_id.to_string();
        let turn_id = turn_id.to_string();
        let data = data.to_string();
        db.write(|conn| {
            let (id, run_id, turn_id, data) = (id.clone(), run_id.clone(), turn_id.clone(), data.clone());
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO events(id,run_id,turn_id,sequence,timestamp,event_type,data,created_at) VALUES(?1,?2,?3,?4,1,'assistant',?5,1)",
                    params![id, run_id, turn_id, sequence, data],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn callback_wait_resolves_provider_id_from_persisted_run_event() {
        let orch = test_orchestrator().await;
        let wait = WaitFor::Terminal {
            kind: super::super::run::TerminalWaitKind::Terminal,
            reference: "cairn:~/terminal/tests".into(),
            on: TerminalWaitEvent::Output,
            phrase: Some("ready".into()),
        };
        insert_assistant_event(
            &orch.db.local,
            "event-1",
            "run-1",
            "current-turn",
            1,
            serde_json::json!({"toolUses":[{
                "toolUseId":"provider-run-id",
                "name":"mcp__cairn__run",
                "input":{"commands":[{"waitFor":serde_json::to_value(&wait).unwrap()}]}
            }]}),
        )
        .await;

        assert_eq!(
            resolve_wait_tool_use_id(&orch.db.local, "run-1", "current-turn", &wait).await,
            Some("provider-run-id".into())
        );
    }

    #[tokio::test]
    async fn uncorrelatable_callback_does_not_fabricate_or_insert_wait_identity() {
        // `agent_waits` is provided by the standard migrations that
        // `test_orchestrator` runs; no manual table creation is needed.
        let orch = test_orchestrator().await;
        let wait = WaitFor::Duration {
            duration: WaitDuration::Human("3m".into()),
        };
        assert_eq!(
            resolve_wait_tool_use_id(&orch.db.local, "run-1", "current-turn", &wait).await,
            None
        );
        let count: i64 = orch
            .db
            .local
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn.query("SELECT COUNT(*) FROM agent_waits", ()).await?;
                    rows.next().await?.unwrap().i64(0)
                })
            })
            .await
            .unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn identical_prior_turn_wait_is_ignored_until_current_event_arrives() {
        let orch = test_orchestrator().await;
        let wait = WaitFor::Duration {
            duration: WaitDuration::Human("3m".into()),
        };
        let event = |id: &str| {
            serde_json::json!({"toolUses":[{
                "id":id,
                "name":"run",
                "input":{"commands":[{"waitFor":serde_json::to_value(&wait).unwrap()}]}
            }]})
        };
        insert_assistant_event(
            &orch.db.local,
            "old-event",
            "run-1",
            "old-turn",
            1,
            event("old-provider-id"),
        )
        .await;

        let db = orch.db.local.clone();
        let current_event = event("current-provider-id");
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            insert_assistant_event(
                &db,
                "current-event",
                "run-1",
                "current-turn",
                2,
                current_event,
            )
            .await;
        });

        assert_eq!(
            resolve_wait_tool_use_id(&orch.db.local, "run-1", "current-turn", &wait).await,
            Some("current-provider-id".into())
        );
    }

    fn record(condition: Condition, deadline: Option<i64>) -> Record {
        Record {
            id: "wait-1".into(),
            job_id: "job-1".into(),
            run_id: "run-1".into(),
            session_id: "session-1".into(),
            turn_id: "turn-1".into(),
            tool_use_id: "tool-1".into(),
            condition,
            deadline,
            created: chrono::Utc::now().timestamp_millis(),
        }
    }
    #[test]
    fn duration_bounds() {
        assert_eq!(
            parse_duration(&WaitDuration::Human("3m".into())).unwrap(),
            180000
        );
        assert!(parse_duration(&WaitDuration::Human("0s".into())).is_err());
        assert!(parse_duration(&WaitDuration::Human("8d".into())).is_err());
    }

    #[tokio::test]
    async fn elapsed_duration_returns_without_creating_durable_state() {
        let orch = test_orchestrator().await;
        let now = chrono::Utc::now().timestamp_millis();
        let result = trigger(
            orch.clone(),
            orch.db.local.clone(),
            record(Condition::Duration, Some(now)),
        )
        .await
        .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&result).unwrap()["outcome"],
            "elapsed"
        );
    }

    #[tokio::test]
    async fn output_wait_fires_on_exit_before_phrase() {
        let orch = test_orchestrator().await;
        orch.db
            .local
            .execute(
                "INSERT INTO job_terminals (id, job_id, session_id, command, status, exit_code, created_at, exited_at, slug, output_tail)
                 VALUES ('t1','job-1','sess-t1','sleep 1','exited',3,1,50,'tests','boom crash exit')",
                (),
            )
            .await
            .unwrap();
        let result = trigger(
            orch.clone(),
            orch.db.local.clone(),
            record(
                Condition::Terminal {
                    uri: "cairn://p/PRJ/1/1/builder/terminal/tests".into(),
                    slug: "tests".into(),
                    on: TerminalWaitEvent::Output,
                    phrase: Some("ready".into()),
                },
                None,
            ),
        )
        .await
        .unwrap();
        let value: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(
            value["outcome"], "terminal_exited",
            "an output/phrase wait must resolve when the terminal exits before the phrase"
        );
    }

    #[tokio::test]
    async fn missing_terminal_fails_before_any_durable_wait_exists() {
        let orch = test_orchestrator().await;
        let error = trigger(
            orch.clone(),
            orch.db.local.clone(),
            record(
                Condition::Terminal {
                    uri: "cairn://p/PRJ/1/1/builder/terminal/missing".into(),
                    slug: "missing".into(),
                    on: TerminalWaitEvent::Exit,
                    phrase: None,
                },
                None,
            ),
        )
        .await
        .unwrap_err();
        assert!(error.contains("Terminal not found"));
    }

    // ---- Durable resolution path -------------------------------------------

    /// Full continue-ready seed: an open session, a live run, and a `running`
    /// predecessor turn wired as the job's current turn. `insert` yields the
    /// predecessor and creates the pending wait row before resolution runs.
    async fn durable_env() -> (Orchestrator, Record, Condition) {
        let root = tempfile::tempdir().unwrap().keep();
        let local = LocalDb::open(root.join("test.db")).await.unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&local)
            .await
            .unwrap();
        for sql in [
            "INSERT INTO workspaces (id,name,created_at,updated_at) VALUES ('w','W',1,1)",
            "INSERT INTO projects (id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES ('p','w','P','PRJ','/tmp/p',1,1)",
            "INSERT INTO issues (id,project_id,number,title,status,created_at,updated_at) VALUES ('i','p',1,'T','active',1,1)",
            "INSERT INTO executions (id,recipe_id,issue_id,project_id,status,started_at,seq) VALUES ('e','recipe','i','p','running',1,1)",
            "INSERT INTO jobs (id,execution_id,issue_id,project_id,node_name,status,created_at,updated_at,uri_segment) VALUES ('job-1','e','i','p','Builder','running',1,1,'builder')",
            "INSERT INTO sessions (id,job_id,status,backend_id,created_at,updated_at) VALUES ('session-1','job-1','open','handle-1',1,1)",
            "INSERT INTO runs (id,issue_id,project_id,job_id,status,session_id,created_at,updated_at) VALUES ('run-1','i','p','job-1','live','session-1',1,1)",
            "INSERT INTO turns (id,session_id,run_id,job_id,sequence,state,start_reason,created_at,updated_at) VALUES ('pred-turn','session-1','run-1','job-1',1,'running','initial',1,1)",
            // current_session_id / current_turn_id carry FKs, so wire them only
            // after the session and turn rows exist.
            "UPDATE jobs SET current_session_id='session-1', current_turn_id='pred-turn' WHERE id='job-1'",
        ] {
            local.execute(sql, ()).await.unwrap();
        }
        let search = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(Arc::new(local), search)),
            Arc::new(TestServicesBuilder::new().build()),
            root,
        )
        .build();
        let now = chrono::Utc::now().timestamp_millis();
        let record = Record {
            id: "wait-1".into(),
            job_id: "job-1".into(),
            run_id: "run-1".into(),
            session_id: "session-1".into(),
            turn_id: "pred-turn".into(),
            tool_use_id: "tool-1".into(),
            condition: Condition::Duration,
            deadline: Some(now - 1000),
            created: now,
        };
        (orch, record, Condition::Duration)
    }

    /// Register an in-memory warm process for `run-1`/`session-1` so
    /// `continue_job_impl` reuses it instead of spawning a real CLI.
    fn register_warm(orch: &Orchestrator) {
        let mut processes = orch.process_state.processes.lock().unwrap();
        let child = Arc::new(std::sync::Mutex::new(None));
        let stdin = Arc::new(std::sync::Mutex::new(Some(
            crate::agent_process::process::wrap_plain_stdin(Box::new(Vec::<u8>::new())),
        )));
        let mut handle = crate::agent_process::process::RunHandle::new(
            child,
            stdin,
            Some("session-1".to_string()),
            Some("job-1".to_string()),
        );
        handle.transition_to_warm();
        processes.register("run-1".to_string(), handle);
    }

    async fn wait_state(orch: &Orchestrator) -> String {
        orch.db
            .local
            .query_one("SELECT state FROM agent_waits WHERE id='wait-1'", (), |r| {
                r.text(0)
            })
            .await
            .unwrap()
    }

    async fn wait_resolved_turns(orch: &Orchestrator) -> Vec<(String, String)> {
        orch.db
            .local
            .read(|c| {
                Box::pin(async move {
                    let mut rows = c
                        .query(
                            "SELECT id,state FROM turns WHERE start_reason='wait_resolved' ORDER BY sequence",
                            (),
                        )
                        .await?;
                    let mut v = Vec::new();
                    while let Some(r) = rows.next().await? {
                        v.push((r.text(0)?, r.text(1)?));
                    }
                    Ok(v)
                })
            })
            .await
            .unwrap()
    }

    async fn tool_result_count(orch: &Orchestrator) -> i64 {
        orch.db
            .local
            .query_one(
                "SELECT COUNT(*) FROM events WHERE event_type='tool_result'",
                (),
                |r| r.i64(0),
            )
            .await
            .unwrap()
    }

    async fn turn_state_of(orch: &Orchestrator, id: &str) -> String {
        let id = id.to_string();
        orch.db
            .local
            .query_one("SELECT state FROM turns WHERE id=?1", (id,), |r| r.text(0))
            .await
            .unwrap()
    }

    const ELAPSED: &str = r#"{"outcome":"elapsed","elapsedMs":1,"deadlineMs":1}"#;

    #[tokio::test]
    async fn durable_duration_resolves_once_with_single_successor_and_resumes_warm() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        register_warm(&orch);

        resolve_slow(&orch, &orch.db.local, &record, ELAPSED, false)
            .await
            .unwrap();

        // Exactly one WaitResolved successor, and it is running (started on resume).
        let successors = wait_resolved_turns(&orch).await;
        assert_eq!(successors.len(), 1, "exactly one WaitResolved successor");
        assert_eq!(successors[0].1, "running", "successor started on resume");
        // The synthetic result was delivered once; the predecessor stays yielded.
        assert_eq!(tool_result_count(&orch).await, 1);
        assert_eq!(turn_state_of(&orch, "pred-turn").await, "yielded");
        // The wait row reached resolved.
        assert_eq!(wait_state(&orch).await, "resolved");
        // The warm process was resumed onto the successor, not left parked.
        assert_eq!(
            orch.process_state.get_current_turn_id("run-1").as_deref(),
            Some(successors[0].0.as_str()),
            "warm process resumed onto the successor turn"
        );
    }

    #[tokio::test]
    async fn duplicate_resolution_creates_no_second_successor_or_result() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        register_warm(&orch);

        resolve_slow(&orch, &orch.db.local, &record, ELAPSED, false)
            .await
            .unwrap();
        // A duplicate live delivery on the already-resolved row is a clean no-op.
        resolve_slow(&orch, &orch.db.local, &record, ELAPSED, false)
            .await
            .unwrap();

        assert_eq!(wait_resolved_turns(&orch).await.len(), 1);
        assert_eq!(tool_result_count(&orch).await, 1);
        assert_eq!(wait_state(&orch).await, "resolved");
    }

    #[tokio::test]
    async fn resolving_replay_reuses_persisted_successor_without_second_continuation() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        register_warm(&orch);

        resolve_slow(&orch, &orch.db.local, &record, ELAPSED, false)
            .await
            .unwrap();
        let successor_id = wait_resolved_turns(&orch).await[0].0.clone();

        // Simulate a crash after the continuation but before the resolved CAS: the
        // row is stuck at `resolving` with its successor persisted, and startup
        // replay re-drives it.
        orch.db
            .local
            .execute(
                "UPDATE agent_waits SET state='resolving' WHERE id='wait-1'",
                (),
            )
            .await
            .unwrap();

        resolve_slow(&orch, &orch.db.local, &record, ELAPSED, true)
            .await
            .unwrap();

        // No second successor, no second tool result — the already-running
        // successor short-circuits the continuation.
        let successors = wait_resolved_turns(&orch).await;
        assert_eq!(successors.len(), 1);
        assert_eq!(successors[0].0, successor_id);
        assert_eq!(tool_result_count(&orch).await, 1);
        assert_eq!(wait_state(&orch).await, "resolved");
    }

    #[tokio::test]
    async fn terminal_condition_durable_path_resolves_and_resumes() {
        let (orch, mut record, _) = durable_env().await;
        record.condition = Condition::Terminal {
            uri: "cairn://p/PRJ/1/1/builder/terminal/tests".into(),
            slug: "tests".into(),
            on: TerminalWaitEvent::Exit,
            phrase: None,
        };
        record.deadline = None;
        insert(&orch.db.local, &record).await.unwrap();
        register_warm(&orch);

        let exited = r#"{"outcome":"exited","terminal":"t","exitCode":0}"#;
        resolve_slow(&orch, &orch.db.local, &record, exited, false)
            .await
            .unwrap();

        let successors = wait_resolved_turns(&orch).await;
        assert_eq!(successors.len(), 1);
        assert_eq!(successors[0].1, "running");
        assert_eq!(wait_state(&orch).await, "resolved");
        assert_eq!(
            orch.process_state.get_current_turn_id("run-1").as_deref(),
            Some(successors[0].0.as_str())
        );
    }

    /// Insert a foreign (non-wait_resolved) successor of the predecessor in the
    /// given state, as if a racing continuation had claimed the predecessor's
    /// single successor mid-wait.
    async fn insert_foreign_successor(orch: &Orchestrator, state: &str) {
        let state = state.to_string();
        orch.db
            .local
            .execute(
                "INSERT INTO turns (id,session_id,run_id,job_id,sequence,predecessor_id,state,start_reason,created_at,updated_at) VALUES ('foreign','session-1','run-1','job-1',2,'pred-turn',?1,'follow_up',2,2)",
                (state,),
            )
            .await
            .unwrap();
        orch.db
            .local
            .execute(
                "UPDATE jobs SET current_turn_id='foreign' WHERE id='job-1'",
                (),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn collision_with_running_continuation_records_result_and_resolves() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        // A continuation already RESUMED the run through the predecessor's successor.
        insert_foreign_successor(&orch, "running").await;
        register_warm(&orch);

        resolve_slow(&orch, &orch.db.local, &record, ELAPSED, false)
            .await
            .unwrap();

        // The run has resumed exactly once (via the foreign turn); the wait records
        // its result and resolves without creating or hijacking a turn.
        assert!(
            wait_resolved_turns(&orch).await.is_empty(),
            "no WaitResolved turn created on collision"
        );
        assert_eq!(
            turn_state_of(&orch, "foreign").await,
            "running",
            "foreign successor is left untouched"
        );
        assert_eq!(tool_result_count(&orch).await, 1);
        assert_eq!(wait_state(&orch).await, "resolved");
        let successor: Option<String> = orch
            .db
            .local
            .query_one(
                "SELECT successor_turn_id FROM agent_waits WHERE id='wait-1'",
                (),
                |r| r.opt_text(0),
            )
            .await
            .unwrap();
        assert_eq!(
            successor, None,
            "foreign turn is not recorded as the successor"
        );
    }

    #[tokio::test]
    async fn collision_resolves_in_process_once_foreign_successor_starts() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        // The racing continuation has created the successor but not started it yet.
        insert_foreign_successor(&orch, "pending").await;
        register_warm(&orch);

        // It starts the foreign successor shortly after the resolver first observes
        // it pending — as a warm reuse would, within the poll budget.
        let db = orch.db.local.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(80)).await;
            db.execute("UPDATE turns SET state='running' WHERE id='foreign'", ())
                .await
                .unwrap();
        });

        // The SAME live resolver — no restart — polls the transition and resolves.
        resolve_slow(&orch, &orch.db.local, &record, ELAPSED, false)
            .await
            .unwrap();

        assert!(wait_resolved_turns(&orch).await.is_empty());
        assert_eq!(turn_state_of(&orch, "foreign").await, "running");
        assert_eq!(tool_result_count(&orch).await, 1);
        assert_eq!(
            wait_state(&orch).await,
            "resolved",
            "in-process poll resolves the wait once the foreign turn starts"
        );
    }

    #[tokio::test]
    async fn collision_polls_while_unstarted_then_reconciliation_resolves_after_restart() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        // A racing continuation created the predecessor's successor but has NOT
        // started it — the run is still parked, so the wait must not falsely resolve.
        insert_foreign_successor(&orch, "pending").await;
        register_warm(&orch);

        // While the foreign successor stays pending, the resolver keeps polling and
        // never completes (no false resolve, no arbitrary give-up cutoff). Dropping
        // the future when the timeout fires models the resolver task being cancelled
        // by host shutdown.
        let polling = tokio::time::timeout(
            Duration::from_millis(250),
            resolve_slow(&orch, &orch.db.local, &record, ELAPSED, false),
        )
        .await;
        assert!(
            polling.is_err(),
            "resolver keeps waiting on an unstarted foreign successor instead of resolving or giving up"
        );
        assert_eq!(
            wait_state(&orch).await,
            "resolving",
            "wait stays resolving (durable ownership) while the run has not resumed"
        );
        assert!(wait_resolved_turns(&orch).await.is_empty());
        assert_eq!(tool_result_count(&orch).await, 0, "no result stored yet");

        // After restart, the racing continuation has started the foreign turn; startup
        // reconciliation re-drives the still-`resolving` row and resolves it.
        orch.db
            .local
            .execute("UPDATE turns SET state='running' WHERE id='foreign'", ())
            .await
            .unwrap();
        resolve_slow(&orch, &orch.db.local, &record, ELAPSED, true)
            .await
            .unwrap();

        assert!(wait_resolved_turns(&orch).await.is_empty());
        assert_eq!(turn_state_of(&orch, "foreign").await, "running");
        assert_eq!(tool_result_count(&orch).await, 1);
        assert_eq!(wait_state(&orch).await, "resolved");
    }

    #[tokio::test]
    async fn self_suspend_preserves_pending_wait_while_user_stop_cancels() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        register_warm(&orch);

        // A durable self-suspend must NOT cancel its own wait row.
        crate::orchestrator::lifecycle::suspend_run_for_durable_wait(
            &orch,
            "run-1",
            "owned_wait_suspended",
        )
        .unwrap();
        assert_eq!(
            wait_state(&orch).await,
            "pending",
            "self-suspend must preserve the pending wait"
        );
        assert!(
            orch.process_state
                .processes
                .lock()
                .unwrap()
                .get("run-1")
                .is_some(),
            "self-suspend keeps the process warm for resume"
        );

        // An external stop DOES cancel the wait.
        crate::orchestrator::lifecycle::stop_active_turn_for_run(&orch, "run-1", true);
        assert_eq!(
            wait_state(&orch).await,
            "cancelled",
            "external stop cancels the wait"
        );
    }
}
