//! Provider-process lifetime enforcement, independent of memory-pressure trimming.

use super::stop::{teardown_process, TerminalPolicy};
use super::{finalize_run, set_exit_reason};
use crate::agent_process::process::WarmProcess;
use crate::db::DbState;
use crate::execution::routing::routing_db_for_id;
use crate::models::RunStatus;
use crate::orchestrator::Orchestrator;
use crate::storage::{run_db_blocking, LocalDb, RowExt};
use cairn_models::continuation::{
    prefix_cache_state, prompt_cache_horizon_secs, PrefixCacheState, ResumeBackend,
};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
struct LifetimeFacts {
    session_id: String,
    session_status: String,
    backend: String,
    job_id: String,
    job_status: String,
    has_unsettled_turn: bool,
    last_provider_request_at: Option<i64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StopCause {
    SessionClosed,
    JobCancelled,
    IdlePastHorizon,
}

impl StopCause {
    fn exit_reason(self) -> &'static str {
        match self {
            Self::SessionClosed => "session_closed",
            Self::JobCancelled => "job_cancelled",
            Self::IdlePastHorizon => "cache_horizon_idle",
        }
    }
}

fn lifetime_verdict(facts: &LifetimeFacts, process_idle_secs: u64, now: i64) -> Option<StopCause> {
    // A pending delivery or a durable wait owns the turn even when its process
    // is parked idle. This guard outranks closure and cancellation too.
    if facts.has_unsettled_turn {
        return None;
    }
    if facts.session_status != "open" {
        return Some(StopCause::SessionClosed);
    }
    if facts.job_status == "cancelled" {
        return Some(StopCause::JobCancelled);
    }
    let backend = ResumeBackend::from_key(&facts.backend);
    match prefix_cache_state(backend, facts.last_provider_request_at, now) {
        PrefixCacheState::Cold => Some(StopCause::IdlePastHorizon),
        PrefixCacheState::Unknown
            if prompt_cache_horizon_secs(backend)
                .is_some_and(|horizon| process_idle_secs >= horizon as u64) =>
        {
            Some(StopCause::IdlePastHorizon)
        }
        _ => None,
    }
}

/// Read each owning database once. An unresolved route, absent row, or failed
/// lookup supplies no verdict: unknown state never authorizes a stop.
async fn load_lifetime_facts(
    dbs: &DbState,
    warm: &[WarmProcess],
) -> HashMap<String, LifetimeFacts> {
    let mut groups: Vec<(Arc<LocalDb>, Vec<String>)> = Vec::new();
    for process in warm {
        let db = match routing_db_for_id(dbs, &process.run_id).await {
            Ok(db) => db,
            Err(error) => {
                log::warn!(
                    "Process lifetime sweep: cannot route run {}: {error}",
                    process.run_id
                );
                continue;
            }
        };
        if let Some((_, runs)) = groups.iter_mut().find(|(owner, _)| Arc::ptr_eq(owner, &db)) {
            runs.push(process.run_id.clone());
        } else {
            groups.push((db, vec![process.run_id.clone()]));
        }
    }

    let mut facts = HashMap::new();
    for (db, runs) in groups {
        let ids = serde_json::to_string(&runs).expect("run ids serialize");
        let result = db
            .query_all(
                "WITH candidates AS (
                SELECT r.id, r.session_id, r.job_id FROM runs r
                WHERE r.id IN (SELECT value FROM json_each(?1))
             ), last_requests AS (
                SELECT e.session_id, MAX(e.created_at) AS requested_at FROM events e
                WHERE e.session_id IN (SELECT session_id FROM candidates)
                  AND e.parent_tool_use_id IS NULL
                  AND (e.input_tokens IS NOT NULL OR e.output_tokens IS NOT NULL
                    OR e.cache_read_tokens IS NOT NULL OR e.cache_create_tokens IS NOT NULL)
                GROUP BY e.session_id
             )
             SELECT r.id, s.id, s.status, s.backend, j.id, j.status,
                    EXISTS(SELECT 1 FROM turns t WHERE t.job_id = j.id
                           AND t.state IN ('pending', 'running', 'yielded')),
                    p.requested_at
             FROM candidates r
             JOIN sessions s ON s.id = r.session_id
             JOIN jobs j ON j.id = r.job_id
             LEFT JOIN last_requests p ON p.session_id = s.id",
                (ids,),
                |row| {
                    Ok((
                        row.text(0)?,
                        LifetimeFacts {
                            session_id: row.text(1)?,
                            session_status: row.text(2)?,
                            backend: row.text(3)?,
                            job_id: row.text(4)?,
                            job_status: row.text(5)?,
                            has_unsettled_turn: row.i64(6)? != 0,
                            last_provider_request_at: row.opt_i64(7)?,
                        },
                    ))
                },
            )
            .await;
        match result {
            Ok(rows) => facts.extend(rows),
            Err(error) => log::warn!(
                "Process lifetime sweep: skipping {} runs after facts lookup failed: {error}",
                runs.len()
            ),
        }
    }
    facts
}

/// Enforce session lifetime and provider cache horizons for registered idle
/// processes only. Background terminals belong to the job, not its provider.
pub(crate) fn sweep_process_lifetimes(orch: &Orchestrator) {
    let warm = orch.process_state.warm_processes();
    let inspected = warm.len();
    let dbs = orch.db.clone();
    let candidates = warm.clone();
    let facts = run_db_blocking(move || async move {
        Ok::<_, String>(load_lifetime_facts(&dbs, &candidates).await)
    })
    .unwrap_or_else(|error| {
        log::warn!("Process lifetime sweep: failed to load facts: {error}");
        HashMap::new()
    });
    let now = orch.services.clock.now();
    let mut session_closed = 0;
    let mut job_cancelled = 0;
    let mut cache_horizon_idle = 0;
    for process in warm {
        let Some(facts) = facts.get(&process.run_id) else {
            continue;
        };
        if lifetime_verdict(facts, process.seconds_since_activity, now).is_none() {
            continue;
        }
        let launch_lock = orch.job_launch_lock(&facts.job_id);
        let Ok(guard) = launch_lock.try_lock() else {
            continue;
        };
        if !orch.process_state.is_warm(&process.run_id) {
            continue;
        }
        // A delivery can finish or yield between the batch read and this lock,
        // leaving the process idle again. Neither its durable turn ownership nor
        // its cache age may be inferred from the old snapshot.
        let dbs = orch.db.clone();
        let candidate = process.clone();
        let refreshed = run_db_blocking(move || async move {
            Ok::<_, String>(
                load_lifetime_facts(&dbs, std::slice::from_ref(&candidate))
                    .await
                    .remove(&candidate.run_id),
            )
        });
        let facts = match refreshed {
            Ok(Some(facts)) => facts,
            Ok(None) => continue,
            Err(error) => {
                log::warn!(
                    "Process lifetime sweep: cannot revalidate {}: {error}",
                    process.run_id
                );
                continue;
            }
        };
        let idle_secs = orch
            .process_state
            .processes
            .lock()
            .ok()
            .and_then(|processes| {
                processes.get(&process.run_id).and_then(|handle| {
                    handle
                        .is_warm()
                        .then(|| handle.last_activity.elapsed().as_secs())
                })
            });
        let Some(cause) = idle_secs.and_then(|idle| lifetime_verdict(&facts, idle, now)) else {
            continue;
        };
        if let Err(error) = teardown_process(orch, &process.run_id, TerminalPolicy::Keep) {
            log::warn!(
                "Process lifetime sweep: could not stop run {} on session {}: {error}",
                process.run_id,
                facts.session_id
            );
            continue;
        }
        // Finalization can re-derive the job and launch its next turn. The launch
        // lock is not reentrant; only physical teardown belongs under it.
        drop(guard);
        match cause {
            StopCause::SessionClosed => session_closed += 1,
            StopCause::JobCancelled => job_cancelled += 1,
            StopCause::IdlePastHorizon => cache_horizon_idle += 1,
        }
        if let Err(error) = set_exit_reason(orch, &process.run_id, cause.exit_reason()) {
            log::error!(
                "Process lifetime sweep: failed to record exit for {}: {error}",
                process.run_id
            );
        }
        // The physical process is already gone. A failed metadata write must
        // not leave its run live for the reaper to misclassify as a crash;
        // not every provider finalizes an idle run from its EOF callback.
        finalize_run(orch, &process.run_id, RunStatus::Exited);
    }
    // An all-zero tick is the positive control that the sweep is alive.
    log::info!(
        "Process lifetime sweep: inspected={inspected}, session_closed={session_closed}, job_cancelled={job_cancelled}, cache_horizon_idle={cache_horizon_idle}"
    );
}
