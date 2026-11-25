//! The run queue: the one authority on whether an issue execution runs.
//!
//! Each runner keeps one ordered list (`run_queue`, in its private database)
//! of every issue execution it wants to run, across all of its projects. An
//! execution joins at the end when it becomes runnable (filed with no `after:`
//! gate, or its last gate released) and leaves only when it stops being live:
//! superseded, withdrawn, or its issue merged or closed. Starting to run, and
//! finishing its recipe, do not remove it.
//!
//! Two lines are drawn over that one list ([`cairn_workflow::queue::selection`]):
//! an entry runs if and only if it is among the first `per_thread` entries of
//! its thread's slice and, among the entries that pass that, among the first
//! `global`. Position is the only input: an entry waiting on a check, a review,
//! a person, or a provider limit holds its place exactly as a running one does.
//!
//! Park is the only way out short of resolution: a parked row stays stored
//! (so nothing re-adds it) but takes no position. Unpark returns it to the end.
//! Moving an entry is the only way to change what runs; there is no override.
//!
//! Every change to the list ends in [`settle`], which reads the list, decides,
//! and acts on the difference: jobs of an admitted execution waiting on the
//! queue are resumed, and running jobs of an execution below the line are
//! stopped at their next tool boundary, resumably. The same pass runs at
//! startup and on the gate sweep's cadence, so a restart reproduces the
//! running set from the stored list alone.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};

use cairn_db::turso::params;
use cairn_workflow::queue::{selection, QueueCaps, QueueEntry};
use serde::Serialize;

use crate::orchestrator::Orchestrator;
use crate::storage::{DbResult, LocalDb, RowExt, TrackedConnection};

/// Serializes every read-decide-write of the list on this runner. Held only
/// while the list is read and rewritten, never while acting on a decision.
static QUEUE_LOCK: LazyLock<tokio::sync::Mutex<()>> = LazyLock::new(|| tokio::sync::Mutex::new(()));

/// One stored row of the list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredRow {
    pub execution_id: String,
    pub issue_id: String,
    pub rank: i64,
    pub parked: bool,
}

/// What the owning database says about one execution on this runner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionFact {
    pub issue_id: String,
    pub thread_id: Option<String>,
    /// Runnable and not yet in the list: it joins at the end.
    pub joinable: bool,
    /// When it became runnable, which orders a batch of joiners.
    pub since: i64,
}

/// Where an entry is, and whether it runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueItem {
    pub execution_id: String,
    pub issue_id: String,
    pub thread_id: Option<String>,
    /// One-based place in the list; `None` when parked.
    pub position: Option<usize>,
    /// Inside both lines.
    pub runs: bool,
    pub parked: bool,
}

/// The list as one decision saw it: unparked entries in order, then parked.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueState {
    pub items: Vec<QueueItem>,
    pub global_cap: usize,
    pub thread_cap: usize,
}

impl QueueState {
    /// Whether `execution_id` is inside both lines.
    pub fn runs(&self, execution_id: &str) -> bool {
        self.items
            .iter()
            .any(|item| item.runs && item.execution_id == execution_id)
    }

    pub fn item(&self, execution_id: &str) -> Option<&QueueItem> {
        self.items
            .iter()
            .find(|item| item.execution_id == execution_id)
    }
}

/// The list after membership is brought up to date: rows to keep in order,
/// rows to drop, and rows to add.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Membership {
    /// Unparked rows in list order, then parked rows.
    pub rows: Vec<StoredRow>,
    pub dropped: Vec<String>,
    pub changed: bool,
}

/// Bring stored rows up to date with the executions' facts. A row whose
/// execution reads as not live (`absent` from `facts` but `known` to an open
/// database) leaves; a row no open database knows of stays, so an offline team
/// database never costs an entry its place. A joinable execution with no row
/// joins at the end, oldest first, or directly behind its issue's existing
/// row so a rerun of an issue inherits the place its old execution leaves.
pub fn plan_membership(
    stored: Vec<StoredRow>,
    facts: &HashMap<String, ExecutionFact>,
    known: &HashSet<String>,
) -> Membership {
    let mut out = Membership::default();
    let mut kept: Vec<StoredRow> = Vec::with_capacity(stored.len());
    for row in stored {
        if !facts.contains_key(&row.execution_id) && known.contains(&row.execution_id) {
            out.dropped.push(row.execution_id);
            out.changed = true;
        } else {
            kept.push(row);
        }
    }
    kept.sort_by(|a, b| (a.rank, &a.execution_id).cmp(&(b.rank, &b.execution_id)));
    let present: HashSet<String> = kept.iter().map(|row| row.execution_id.clone()).collect();
    let mut joiners: Vec<(&String, &ExecutionFact)> = facts
        .iter()
        .filter(|(id, fact)| fact.joinable && !present.contains(*id))
        .collect();
    joiners.sort_by(|(a, fa), (b, fb)| (fa.since, *a).cmp(&(fb.since, *b)));
    for (id, fact) in joiners {
        let row = StoredRow {
            execution_id: id.clone(),
            issue_id: fact.issue_id.clone(),
            rank: 0,
            parked: false,
        };
        match kept
            .iter()
            .rposition(|other| other.issue_id == fact.issue_id && !other.parked)
        {
            Some(at) => kept.insert(at + 1, row),
            None => kept.push(row),
        }
        out.changed = true;
    }
    let (mut unparked, parked): (Vec<_>, Vec<_>) = kept.into_iter().partition(|row| !row.parked);
    unparked.extend(parked);
    for (index, row) in unparked.iter_mut().enumerate() {
        let rank = index as i64 + 1;
        if row.rank != rank {
            row.rank = rank;
            out.changed = true;
        }
    }
    out.rows = unparked;
    out
}

/// Decide the list: a parked row takes no position, and a row whose execution
/// no open database knows cannot run here, so it is passed over by both lines.
pub fn decide(
    rows: &[StoredRow],
    facts: &HashMap<String, ExecutionFact>,
    caps: QueueCaps,
) -> QueueState {
    let listed: Vec<&StoredRow> = rows.iter().filter(|row| !row.parked).collect();
    let entries: Vec<QueueEntry> = listed
        .iter()
        .filter(|row| facts.contains_key(&row.execution_id))
        .map(|row| QueueEntry {
            execution_id: row.execution_id.clone(),
            thread_id: facts[&row.execution_id].thread_id.clone(),
        })
        .collect();
    let runs: HashSet<&str> = entries
        .iter()
        .zip(selection(&entries, caps))
        .filter(|(_, decision)| decision.admitted)
        .map(|(entry, _)| entry.execution_id.as_str())
        .collect();
    let mut items: Vec<QueueItem> = listed
        .iter()
        .enumerate()
        .map(|(index, row)| QueueItem {
            execution_id: row.execution_id.clone(),
            issue_id: row.issue_id.clone(),
            thread_id: facts
                .get(&row.execution_id)
                .and_then(|fact| fact.thread_id.clone()),
            position: Some(index + 1),
            runs: runs.contains(row.execution_id.as_str()),
            parked: false,
        })
        .collect();
    items.extend(rows.iter().filter(|row| row.parked).map(|row| {
        QueueItem {
            execution_id: row.execution_id.clone(),
            issue_id: row.issue_id.clone(),
            thread_id: facts
                .get(&row.execution_id)
                .and_then(|fact| fact.thread_id.clone()),
            position: None,
            runs: false,
            parked: true,
        }
    }));
    QueueState {
        items,
        global_cap: caps.global,
        thread_cap: caps.per_thread,
    }
}

/// Move one unparked row to one-based `position` (clamped to the list).
pub fn plan_move(rows: &mut Vec<StoredRow>, execution_id: &str, position: usize) -> bool {
    let Some(from) = rows
        .iter()
        .position(|row| row.execution_id == execution_id && !row.parked)
    else {
        return false;
    };
    let row = rows.remove(from);
    let listed = rows.iter().filter(|row| !row.parked).count();
    let to = position.saturating_sub(1).min(listed);
    rows.insert(to, row);
    renumber(rows);
    true
}

/// Park or unpark one row. Unparking returns it to the end of the list.
pub fn plan_park(rows: &mut Vec<StoredRow>, execution_id: &str, parked: bool) -> bool {
    let Some(from) = rows.iter().position(|row| row.execution_id == execution_id) else {
        return false;
    };
    if rows[from].parked == parked {
        return true;
    }
    let mut row = rows.remove(from);
    row.parked = parked;
    let listed = rows.iter().filter(|row| !row.parked).count();
    rows.insert(if parked { rows.len() } else { listed }, row);
    renumber(rows);
    true
}

fn renumber(rows: &mut [StoredRow]) {
    for (index, row) in rows.iter_mut().enumerate() {
        row.rank = index as i64 + 1;
    }
}

/// The caps as settings hold them. Zero is not a setting: both clamp to one.
pub fn caps(orch: &Orchestrator) -> QueueCaps {
    let settings = orch.get_settings();
    QueueCaps {
        global: settings.max_concurrent_executions.max(1) as usize,
        per_thread: settings.max_open_children_per_thread.max(1) as usize,
    }
}

// ---------------------------------------------------------------------------
// Storage
// ---------------------------------------------------------------------------

const STORED_ROWS_SQL: &str =
    "SELECT execution_id, issue_id, rank, parked_at IS NOT NULL FROM run_queue ORDER BY rank, execution_id";

/// Every live execution on this runner (`?1`) in one database: its issue, its
/// thread, and whether it is runnable now. `held` is an execution waiting on
/// an authored `after:` gate; it is not in the list until released.
const LIVE_EXECUTIONS_SQL: &str = concat!(
    "SELECT e.id, e.issue_id, i.parent_thread_id,
            CASE WHEN e.status IN ('running', 'paused') AND NOT (",
    cairn_db::gated_unreleased_sql!(),
    ") THEN 1 ELSE 0 END,
            COALESCE(e.released_at, e.started_at), e.queue_state
       FROM executions e INDEXED BY idx_executions_run_queue JOIN issues i ON i.id = e.issue_id
      WHERE e.lifecycle IN ('canonical', 'pending')
        AND COALESCE(e.runner_device_id, ?1) = ?1 AND ",
    cairn_db::live_execution_sql!()
);

async fn stored_rows_conn(conn: &TrackedConnection) -> DbResult<Vec<StoredRow>> {
    let mut rows = conn.query(STORED_ROWS_SQL, ()).await?;
    let mut out = Vec::new();
    while let Some(row) = rows.next().await? {
        out.push(StoredRow {
            execution_id: row.text(0)?,
            issue_id: row.text(1)?,
            rank: row.i64(2)?,
            parked: row.i64(3)? != 0,
        });
    }
    Ok(out)
}

async fn stored_rows(db: &LocalDb) -> DbResult<Vec<StoredRow>> {
    db.read(|conn| Box::pin(async move { stored_rows_conn(conn).await }))
        .await
}

async fn write_rows(
    db: &LocalDb,
    rows: Vec<StoredRow>,
    dropped: Vec<String>,
    now: i64,
) -> DbResult<()> {
    db.write(move |conn| {
        let (rows, dropped) = (rows.clone(), dropped.clone());
        Box::pin(async move {
            for id in &dropped {
                conn.execute(
                    "DELETE FROM run_queue WHERE execution_id = ?1",
                    params![id.as_str()],
                )
                .await?;
            }
            for row in &rows {
                conn.execute(
                    "INSERT INTO run_queue(execution_id, issue_id, rank, parked_at, added_at)
                     VALUES (?1, ?2, ?3, CASE WHEN ?4 THEN ?5 END, ?5)
                     ON CONFLICT(execution_id) DO UPDATE SET
                       rank = excluded.rank,
                       parked_at = CASE WHEN ?4 THEN COALESCE(run_queue.parked_at, ?5) END",
                    params![
                        row.execution_id.as_str(),
                        row.issue_id.as_str(),
                        row.rank,
                        i64::from(row.parked),
                        now
                    ],
                )
                .await?;
            }
            Ok(())
        })
    })
    .await
}

/// Where each live execution lives, and the queue state its row last
/// recorded (`executions.queue_state`).
type Located = HashMap<String, (Arc<LocalDb>, Option<String>)>;

/// Every live execution on this runner, across every open database, and
/// which executions those databases know of at all.
async fn execution_facts(
    orch: &Orchestrator,
    stored: &[StoredRow],
) -> Result<(HashMap<String, ExecutionFact>, HashSet<String>, Located), String> {
    let device = orch.anon_device_manager.device_id();
    let mut facts = HashMap::new();
    let mut known = HashSet::new();
    let mut located = Located::new();
    let ids: Vec<String> = stored.iter().map(|row| row.execution_id.clone()).collect();
    for db in orch.db.all_dbs().await {
        let (device, ids) = (device.clone(), ids.clone());
        let (live, present) = db
            .read(move |conn| {
                let (device, ids) = (device.clone(), ids.clone());
                Box::pin(async move {
                    let mut rows = conn
                        .query(LIVE_EXECUTIONS_SQL, params![device.as_str()])
                        .await?;
                    let mut live = Vec::new();
                    while let Some(row) = rows.next().await? {
                        live.push((
                            row.text(0)?,
                            ExecutionFact {
                                issue_id: row.text(1)?,
                                thread_id: row.opt_text(2)?,
                                joinable: row.i64(3)? != 0,
                                since: row.opt_i64(4)?.unwrap_or(0),
                            },
                            row.opt_text(5)?,
                        ));
                    }
                    drop(rows);
                    let mut present = Vec::new();
                    for chunk in ids.chunks(cairn_db::sql::IN_LIST_CHUNK) {
                        let sql = format!(
                            "SELECT id FROM executions INDEXED BY idx_executions_hold_facts WHERE id IN ({})",
                            cairn_db::sql::placeholders(chunk.len())
                        );
                        let values: Vec<cairn_db::turso::Value> = chunk
                            .iter()
                            .map(|id| cairn_db::turso::Value::Text(id.clone()))
                            .collect();
                        let mut rows = conn.query(&sql, values).await?;
                        while let Some(row) = rows.next().await? {
                            present.push(row.text(0)?);
                        }
                    }
                    Ok((live, present))
                })
            })
            .await
            .map_err(|error| format!("failed to read the run queue's executions: {error}"))?;
        known.extend(present);
        for (id, fact, queue_state) in live {
            known.insert(id.clone());
            located.insert(id.clone(), (db.clone(), queue_state));
            facts.insert(id, fact);
        }
    }
    Ok((facts, known, located))
}

/// The queue state an issue's status projection reads from its execution:
/// `queued` below the line, `parked` when parked, none when it runs.
fn recorded_state(item: &QueueItem) -> Option<&'static str> {
    match (item.parked, item.runs) {
        (true, _) => Some("parked"),
        (false, false) => Some("queued"),
        (false, true) => None,
    }
}

/// Record each execution's queue state where its issue's status is projected,
/// and re-project that status, only where it changed. The queue is the one
/// writer of `executions.queue_state`.
async fn record_states(state: &QueueState, located: &Located) {
    for item in &state.items {
        let Some((db, recorded)) = located.get(&item.execution_id) else {
            continue;
        };
        let want = recorded_state(item);
        if recorded.as_deref() == want {
            continue;
        }
        let (execution, issue, want) = (
            item.execution_id.clone(),
            item.issue_id.clone(),
            want.map(str::to_owned),
        );
        let written = db
            .write(move |conn| {
                let (execution, issue, want) = (execution.clone(), issue.clone(), want.clone());
                Box::pin(async move {
                    conn.execute(
                        "UPDATE executions SET queue_state = ?2 WHERE id = ?1",
                        params![execution.as_str(), want.as_deref()],
                    )
                    .await?;
                    cairn_workflow::transitions::outcome::recompute_issue_status_conn(conn, &issue)
                        .await?;
                    Ok(())
                })
            })
            .await;
        if let Err(error) = written {
            log::warn!(
                "run queue could not record the state of execution {}: {error}",
                item.execution_id
            );
        }
    }
}

/// Read the list, bring its membership up to date, apply `change` to it, store
/// it, and decide it. `change` returns false when its subject is not in the
/// list, which fails the call without writing.
async fn under_lock(
    orch: &Orchestrator,
    change: impl FnOnce(&mut Vec<StoredRow>) -> Result<(), String>,
) -> Result<(QueueState, Located), String> {
    let _guard = QUEUE_LOCK.lock().await;
    let local = orch.db.local.clone();
    let stored = stored_rows(&local)
        .await
        .map_err(|error| format!("failed to read the run queue: {error}"))?;
    let before = stored.clone();
    let (facts, known, located) = execution_facts(orch, &stored).await?;
    let Membership {
        mut rows, dropped, ..
    } = plan_membership(stored, &facts, &known);
    change(&mut rows)?;
    if rows != before || !dropped.is_empty() {
        write_rows(&local, rows.clone(), dropped, orch.services.clock.now())
            .await
            .map_err(|error| format!("failed to store the run queue: {error}"))?;
    }
    Ok((decide(&rows, &facts, caps(orch)), located))
}

/// The list as it stands, membership brought up to date.
pub async fn read(orch: &Orchestrator) -> Result<QueueState, String> {
    Ok(under_lock(orch, |_| Ok(())).await?.0)
}

/// Whether `execution_id` runs now. Every path that would start or continue
/// an issue execution's work asks this, and nothing else.
pub async fn runs(orch: &Orchestrator, execution_id: &str) -> Result<bool, String> {
    Ok(read(orch).await?.runs(execution_id))
}

fn missing(execution_id: &str) -> String {
    format!("Execution {execution_id} is not in this runner's queue.")
}

/// Move an entry to one-based `position`, then act on what that changes.
pub async fn move_to(
    orch: &Orchestrator,
    execution_id: &str,
    position: usize,
) -> Result<QueueState, String> {
    let (state, located) = under_lock(orch, |rows| {
        if rows
            .iter()
            .any(|row| row.execution_id == execution_id && row.parked)
        {
            return Err(format!(
                "Execution {execution_id} is parked; unpark it to put it back in the queue."
            ));
        }
        plan_move(rows, execution_id, position)
            .then_some(())
            .ok_or_else(|| missing(execution_id))
    })
    .await?;
    act(orch, &state, &located).await;
    Ok(state)
}

/// Park (`true`) or unpark an entry, then act on what that changes.
pub async fn set_parked(
    orch: &Orchestrator,
    execution_id: &str,
    parked: bool,
) -> Result<QueueState, String> {
    let (state, located) = under_lock(orch, |rows| {
        plan_park(rows, execution_id, parked)
            .then_some(())
            .ok_or_else(|| missing(execution_id))
    })
    .await?;
    act(orch, &state, &located).await;
    Ok(state)
}

/// Read and decide the list and act on it: the pass every change ends in,
/// and the startup and periodic backstop.
pub async fn settle(orch: &Orchestrator) -> Result<QueueState, String> {
    let (state, located) = under_lock(orch, |_| Ok(())).await?;
    act(orch, &state, &located).await;
    Ok(state)
}

/// Make the running set and the issues' statuses match the decision.
/// Idempotent: an admitted execution with nothing waiting, a displaced one
/// already stopped, and an unchanged state are no-ops.
async fn act(orch: &Orchestrator, state: &QueueState, located: &Located) {
    record_states(state, located).await;
    for item in &state.items {
        let result = if item.runs {
            crate::orchestrator::wakes::resume_queue_held(orch, &item.execution_id).await
        } else {
            crate::orchestrator::lifecycle::displace_execution(orch, &item.execution_id).await
        };
        if let Err(error) = result {
            log::warn!(
                "run queue could not {} execution {}: {error}",
                if item.runs { "resume" } else { "stop" },
                item.execution_id
            );
        }
    }
}

#[cfg(test)]
#[path = "queue_tests.rs"]
mod tests;
