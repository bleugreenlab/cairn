//! Canonical whole-session eligibility for thread history. These routines only
//! read hot history; invalidation removes unevicted catalog entries, never history.
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use super::write::ThreadRange;
use super::Manifest;
use crate::storage::{DbResult, LocalDb, RowExt};

const MAX_TASKS: usize = 4096;
const MAX_DEPTH: usize = 64;
const EVENT_PAGE: i64 = 512;

#[derive(Clone, Debug)]
pub struct ThreadOwner {
    pub job_id: String,
    pub thread_name: String,
}

#[derive(Clone, Debug)]
pub struct RangeCandidate {
    pub range: ThreadRange,
    pub thread_name: String,
    pub resolved_at: i64,
    pub event_rows: u64,
    pub estimated_bytes: u64,
}

#[derive(Clone, Debug)]
pub struct RangeSkip {
    pub scope_id: String,
    pub reason: String,
}

#[derive(Default, Debug)]
pub struct RangeBuild {
    pub candidates: Vec<RangeCandidate>,
    pub skips: Vec<RangeSkip>,
}

/// Stable job-id cursor, independent of the execution source's cursor.
pub async fn owners(db: &LocalDb, after_job_id: &str, limit: usize) -> DbResult<Vec<ThreadOwner>> {
    db.query_all(
        "SELECT j.id, t.name FROM jobs j JOIN threads t ON t.id = j.thread_id
         WHERE j.id > ?1 AND j.parent_job_id IS NULL AND j.uri_segment = 'thread'
         ORDER BY j.id LIMIT ?2",
        (after_job_id.to_string(), limit as i64),
        |r| {
            Ok(ThreadOwner {
                job_id: r.text(0)?,
                thread_name: r.text(1)?,
            })
        },
    )
    .await
}

#[derive(Clone, Debug)]
struct Session {
    id: String,
    sequence: i64,
    closed: Option<i64>,
    eligible: bool,
}

#[derive(Clone, Debug)]
struct Run {
    id: String,
    sessions: BTreeSet<String>,
    eligible: bool,
    rows: u64,
}

fn terminal_job(status: &str) -> bool {
    matches!(status, "complete" | "failed" | "cancelled")
}

async fn mapped(db: &LocalDb, table: &str, column: &str, id: &str) -> DbResult<bool> {
    Ok(db
        .query_opt_i64(
            format!(
                "SELECT 1 FROM {table} m JOIN history_tier h ON h.scope_id = m.scope_id
                 WHERE m.{column} = ?1 LIMIT 1"
            ),
            (id.to_string(),),
        )
        .await?
        .is_some())
}

async fn read_run(db: &LocalDb, id: &str) -> DbResult<Option<Run>> {
    let Some((status, own)) = db
        .query_opt(
            "SELECT status, session_id FROM runs WHERE id = ?1",
            (id.to_string(),),
            |r| Ok((r.text(0)?, r.opt_text(1)?)),
        )
        .await?
    else {
        return Ok(None);
    };
    let mut sessions: BTreeSet<String> = own.into_iter().collect();
    sessions.extend(
        db.query_all(
            "SELECT session_id FROM turns WHERE run_id = ?1",
            (id.to_string(),),
            |r| r.text(0),
        )
        .await?,
    );
    let mut rows = 0;
    for (session, count) in db
        .query_all(
            "SELECT session_id, COUNT(*) FROM events WHERE run_id = ?1 GROUP BY session_id",
            (id.to_string(),),
            |r| Ok((r.opt_text(0)?, r.i64(1)?)),
        )
        .await?
    {
        sessions.extend(session);
        rows += count as u64;
    }
    Ok(Some(Run {
        id: id.to_string(),
        sessions,
        eligible: !matches!(status.as_str(), "starting" | "live")
            && !mapped(db, "history_tier_runs", "run_id", id).await?,
        rows,
    }))
}

async fn session_runs(db: &LocalDb, session: &str) -> DbResult<BTreeSet<String>> {
    let mut ids: BTreeSet<String> = db
        .query_all(
            "SELECT id FROM runs WHERE session_id = ?1",
            (session.to_string(),),
            |r| r.text(0),
        )
        .await?
        .into_iter()
        .collect();
    ids.extend(
        db.query_all(
            "SELECT run_id FROM turns WHERE session_id = ?1 AND run_id IS NOT NULL",
            (session.to_string(),),
            |r| r.text(0),
        )
        .await?,
    );
    // Event ownership can survive even when neither the run nor a turn names
    // this session. Discover it here; post-build closure remains a backstop.
    let mut after = 0;
    loop {
        let page = db
            .query_all(
                EVENT_CLOSURE_SQL,
                (session.to_string(), after, EVENT_PAGE),
                |r| Ok((r.i64(0)?, r.opt_text(1)?)),
            )
            .await?;
        if page.is_empty() {
            break;
        }
        after = page.last().unwrap().0;
        ids.extend(page.iter().filter_map(|(_, run)| run.clone()));
        if page.len() < EVENT_PAGE as usize {
            break;
        }
    }
    Ok(ids)
}

#[derive(Debug)]
struct Task {
    id: String,
    created: i64,
    runs: Vec<Run>,
    sessions: Vec<String>,
}

/// Walk every parent (even nonterminal parents), but never an unbounded graph.
/// Exhausting the bound refuses this owner rather than silently losing children.
async fn tasks(db: &LocalDb, owner: &str) -> DbResult<Option<Vec<Task>>> {
    let mut result = Vec::new();
    let mut queue = vec![(owner.to_string(), 0)];
    let mut seen = HashSet::from([owner.to_string()]);
    while let Some((parent, depth)) = queue.pop() {
        let children = db
            .query_all(
                "SELECT id, status, created_at FROM jobs WHERE parent_job_id = ?1 LIMIT ?2",
                (parent, MAX_TASKS as i64 + 1),
                |r| Ok((r.text(0)?, r.text(1)?, r.i64(2)?)),
            )
            .await?;
        for (id, status, created) in children {
            if !seen.insert(id.clone()) {
                continue;
            }
            if seen.len() > MAX_TASKS || depth >= MAX_DEPTH {
                return Ok(None);
            }
            queue.push((id.clone(), depth + 1));
            if !terminal_job(&status) {
                continue;
            }
            let session_rows = db
                .query_all(
                    "SELECT id FROM sessions WHERE job_id = ?1",
                    (id.clone(),),
                    |r| r.text(0),
                )
                .await?;
            let mut sessions = Vec::new();
            let mut eligible = true;
            for session in session_rows {
                if mapped(db, "history_tier_sessions", "session_id", &session).await? {
                    eligible = false;
                    break;
                }
                sessions.push(session);
            }
            if !eligible {
                continue;
            }
            let ids = db
                .query_all(
                    "SELECT id FROM runs WHERE job_id = ?1",
                    (id.clone(),),
                    |r| r.text(0),
                )
                .await?;
            let mut runs = Vec::new();
            for run_id in ids {
                let Some(run) = read_run(db, &run_id).await? else {
                    eligible = false;
                    break;
                };
                if !run.eligible || !run.sessions.iter().all(|s| sessions.contains(s)) {
                    eligible = false;
                    break;
                }
                runs.push(run);
            }
            if eligible && (!runs.is_empty() || !sessions.is_empty()) {
                result.push(Task {
                    id,
                    created,
                    runs,
                    sessions,
                });
            }
        }
    }
    result.sort_by(|a, b| (a.created, &a.id).cmp(&(b.created, &b.id)));
    Ok(Some(result))
}

/// Cut by the newest *applied generation's source session*, never by compaction
/// timestamps or turn block indexes. Cutoff is an epoch-second hot-window bound.
pub async fn build_ranges(
    db: &LocalDb,
    owner: &ThreadOwner,
    cutoff: i64,
    bytes_per_row: u64,
    max_pack_bytes: u64,
) -> DbResult<RangeBuild> {
    let boundary = db
        .query_opt(
            "SELECT c.generation, s.sequence FROM thread_compactions c
         JOIN jobs j ON j.id = c.job_id
         JOIN sessions s ON s.id = c.source_session_id AND s.job_id = j.id
         WHERE c.job_id = ?1 AND c.source_session_id <> j.current_session_id
         ORDER BY c.generation DESC LIMIT 1",
            (owner.job_id.clone(),),
            |r| Ok((r.i64(0)?, r.i64(1)?)),
        )
        .await?;
    let Some((generation, boundary)) = boundary else {
        return Ok(RangeBuild::default());
    };
    let mut sessions = db
        .query_all(
            "SELECT s.id, s.sequence, s.closed_at,
                s.status NOT IN ('open','active') AND s.id <> COALESCE(j.current_session_id,'')
         FROM sessions s JOIN jobs j ON j.id = s.job_id WHERE s.job_id = ?1",
            (owner.job_id.clone(),),
            |r| {
                Ok(Session {
                    id: r.text(0)?,
                    sequence: r.i64(1)?,
                    closed: r.opt_i64(2)?,
                    eligible: r.i64(3)? != 0,
                })
            },
        )
        .await?;
    sessions.sort_by(|a, b| (a.sequence, &a.id).cmp(&(b.sequence, &b.id)));
    let mut remaining = Vec::new();
    for mut session in sessions {
        if session.sequence > boundary {
            break;
        }
        if mapped(db, "history_tier_sessions", "session_id", &session.id).await? {
            continue;
        }
        session.eligible &= session.closed.is_some_and(|t| t < cutoff);
        if !session.eligible {
            break;
        }
        remaining.push(session);
    }
    let positions: HashMap<String, usize> = remaining
        .iter()
        .enumerate()
        .map(|(i, s)| (s.id.clone(), i))
        .collect();
    let mut runs = BTreeMap::new();
    let mut end = remaining.len();
    for (i, session) in remaining.iter().enumerate() {
        for id in session_runs(db, &session.id).await? {
            if !runs.contains_key(&id) {
                if let Some(run) = read_run(db, &id).await? {
                    runs.insert(id.clone(), run);
                } else {
                    end = end.min(i);
                    continue;
                }
            }
            let run = &runs[&id];
            if !run.eligible || !run.sessions.iter().all(|s| positions.contains_key(s)) {
                end = end.min(i);
            }
        }
    }
    // An invalid suffix can pull earlier sessions back through any number of straddles.
    loop {
        let old = end;
        for run in runs.values() {
            let indexes: Vec<usize> = run
                .sessions
                .iter()
                .filter_map(|s| positions.get(s).copied())
                .collect();
            if indexes.iter().any(|i| *i >= end) {
                if let Some(first) = indexes.iter().min() {
                    end = end.min(*first);
                }
            }
        }
        if old == end {
            break;
        }
    }
    remaining.truncate(end);
    let mut result = RangeBuild::default();
    if remaining.is_empty() {
        return Ok(result);
    }
    let Some(mut task_pool) = tasks(db, &owner.job_id).await? else {
        result.skips.push(RangeSkip {
            scope_id: format!("{}/tasks", owner.job_id),
            reason: "thread task parent walk exceeded its bound".into(),
        });
        return Ok(result);
    };
    let mut start = 0;
    let target = max_pack_bytes / 2;
    let mut pending: Option<RangeCandidate> = None;
    while start < remaining.len() {
        // Connected sessions occupy an interval: unrelated sessions between a
        // straddle's ends stay with that component, preserving consecutive ranges.
        let mut stop = start + 1;
        loop {
            let old = stop;
            for run in runs.values() {
                let indexes: Vec<usize> = run
                    .sessions
                    .iter()
                    .filter_map(|s| positions.get(s).copied())
                    .collect();
                if indexes.iter().any(|i| *i >= start && *i < stop) {
                    if let Some(last) = indexes.iter().max() {
                        stop = stop.max(last + 1);
                    }
                }
            }
            if old == stop {
                break;
            }
        }
        let slice = &remaining[start..stop];
        let session_set: HashSet<&str> = slice.iter().map(|s| s.id.as_str()).collect();
        let component_runs: Vec<&Run> = runs
            .values()
            .filter(|r| r.sessions.iter().any(|s| session_set.contains(s.as_str())))
            .collect();
        let event_rows: u64 = component_runs.iter().map(|r| r.rows).sum();
        let resolved_at = slice.last().unwrap().closed.unwrap();
        let mut component = RangeCandidate {
            range: ThreadRange {
                owner_job_id: owner.job_id.clone(),
                first_sequence: slice[0].sequence,
                last_sequence: slice.last().unwrap().sequence,
                through_generation: generation,
                jobs: vec![owner.job_id.clone()],
                sessions: slice.iter().map(|s| s.id.clone()).collect(),
                runs: component_runs.iter().map(|r| r.id.clone()).collect(),
            },
            thread_name: owner.thread_name.clone(),
            resolved_at,
            event_rows,
            estimated_bytes: event_rows.saturating_mul(bytes_per_row),
        };
        if component.estimated_bytes > max_pack_bytes {
            if let Some(range) = pending.take() {
                result.candidates.push(range);
            }
            result.skips.push(RangeSkip {
                scope_id: component.range.scope_id(),
                reason: "connected sessions exceed maximum pack bytes".into(),
            });
            start = stop;
            continue;
        }
        // Tasks are indivisible too. A large task is named and deferred without
        // preventing its owner's closed session history from packing.
        let mut keep = Vec::new();
        for task in task_pool {
            if task.created > resolved_at {
                keep.push(task);
                continue;
            }
            let rows: u64 = task.runs.iter().map(|r| r.rows).sum();
            if rows
                .saturating_mul(bytes_per_row)
                .saturating_add(component.estimated_bytes)
                > max_pack_bytes
            {
                let scope_id = format!("task/{}", task.id);
                if !result.skips.iter().any(|skip| skip.scope_id == scope_id) {
                    result.skips.push(RangeSkip {
                        scope_id,
                        reason: "task plus connected sessions exceeds maximum pack bytes".into(),
                    });
                }
                keep.push(task);
                continue;
            }
            component.range.jobs.push(task.id);
            component.range.sessions.extend(task.sessions);
            component
                .range
                .runs
                .extend(task.runs.into_iter().map(|r| r.id));
            component.event_rows += rows;
            component.estimated_bytes = component.event_rows.saturating_mul(bytes_per_row);
        }
        task_pool = keep;
        if pending.as_ref().is_some_and(|p| {
            p.estimated_bytes.saturating_add(component.estimated_bytes) > target
                || p.range.last_sequence + 1 != component.range.first_sequence
        }) {
            result.candidates.push(pending.take().unwrap());
        }
        if let Some(p) = pending.as_mut() {
            p.range.last_sequence = component.range.last_sequence;
            p.range.sessions.extend(component.range.sessions);
            p.range.runs.extend(component.range.runs);
            p.range
                .jobs
                .extend(component.range.jobs.into_iter().skip(1));
            p.resolved_at = component.resolved_at;
            p.event_rows += component.event_rows;
            p.estimated_bytes = p.event_rows.saturating_mul(bytes_per_row);
        } else {
            pending = Some(component);
        }
        start = stop;
    }
    if let Some(range) = pending {
        result.candidates.push(range);
    }
    Ok(result)
}

/// Session-keyed bounded pages avoid both bound IN lists and DISTINCT sorters.
pub(super) const EVENT_CLOSURE_SQL: &str =
    "SELECT rowid, run_id FROM events WHERE session_id = ?1 AND rowid > ?2 ORDER BY rowid LIMIT ?3";

/// Recheck under the archival permit, and again with the built manifest before
/// publication. Returns a standing-skip reason, or None when the range is closed.
pub async fn validate_candidate(
    db: &LocalDb,
    candidate: &RangeCandidate,
    manifest: Option<&Manifest>,
) -> DbResult<Option<String>> {
    let range = &candidate.range;
    let sessions: HashSet<&str> = range.sessions.iter().map(String::as_str).collect();
    let run_ids: HashSet<&str> = range.runs.iter().map(String::as_str).collect();
    for job in range.jobs.iter().filter(|j| *j != &range.owner_job_id) {
        let status = db
            .query_text("SELECT status FROM jobs WHERE id = ?1", (job.clone(),))
            .await?;
        if !status.as_deref().is_some_and(terminal_job) {
            return Ok(Some(format!("task {job} is no longer terminal")));
        }
        let ids = db
            .query_all("SELECT id FROM runs WHERE job_id=?1", (job.clone(),), |r| {
                r.text(0)
            })
            .await?;
        if ids.iter().any(|id| !run_ids.contains(id.as_str())) {
            return Ok(Some(format!("task {job} gained a run")));
        }
    }
    for session in &range.sessions {
        let status = db.query_opt(
            "SELECT s.status, j.current_session_id, s.job_id FROM sessions s JOIN jobs j ON j.id=s.job_id WHERE s.id=?1",
            (session.clone(),), |r| Ok((r.text(0)?,r.opt_text(1)?,r.text(2)?)),
        ).await?;
        if status.is_none_or(|(status, current, job)| {
            job == range.owner_job_id
                && (status == "open" || status == "active" || current.as_deref() == Some(session))
        }) {
            return Ok(Some(format!("session {session} is no longer closed")));
        }
        if mapped(db, "history_tier_sessions", "session_id", session).await? {
            return Ok(Some(format!("session {session} is already mapped")));
        }
        for run in session_runs(db, session).await? {
            if !run_ids.contains(run.as_str()) {
                return Ok(Some(format!("session {session} gained run {run}")));
            }
        }
    }
    for id in &range.runs {
        let Some(run) = read_run(db, id).await? else {
            return Ok(Some(format!("run {id} disappeared")));
        };
        if !run.eligible || !run.sessions.iter().all(|s| sessions.contains(s.as_str())) {
            return Ok(Some(format!(
                "run {id} is no longer terminal, unmapped and closed"
            )));
        }
    }
    if let Some(manifest) = manifest {
        let manifest_sessions: HashSet<&str> =
            manifest.sessions.iter().map(|s| s.id.as_str()).collect();
        let manifest_runs: HashSet<&str> = manifest.runs.iter().map(|r| r.id.as_str()).collect();
        if manifest_sessions != sessions
            || manifest_runs != run_ids
            || manifest
                .runs
                .iter()
                .any(|r| r.session_ids.iter().any(|s| !sessions.contains(s.as_str())))
        {
            return Ok(Some(
                "manifest run/session closure differs from range".into(),
            ));
        }
        for session in &range.sessions {
            let mut after = 0;
            loop {
                let rows = db
                    .query_all(
                        EVENT_CLOSURE_SQL,
                        (session.clone(), after, EVENT_PAGE),
                        |r| Ok((r.i64(0)?, r.opt_text(1)?)),
                    )
                    .await?;
                if rows.is_empty() {
                    break;
                }
                for (rowid, run) in &rows {
                    if !run.as_deref().is_some_and(|id| manifest_runs.contains(id)) {
                        return Ok(Some(format!(
                            "session {session} has event {rowid} outside manifest runs"
                        )));
                    }
                }
                after = rows.last().unwrap().0;
                if rows.len() < EVENT_PAGE as usize {
                    break;
                }
            }
        }
    }
    Ok(None)
}

pub async fn unevicted_ranges(
    db: &LocalDb,
    after_scope_id: &str,
    limit: usize,
) -> DbResult<Vec<String>> {
    db.query_all(
        "SELECT scope_id FROM history_tier WHERE scope_id > ?1 AND kind='thread_range'
         AND evicted_at IS NULL ORDER BY scope_id LIMIT ?2",
        (after_scope_id.to_string(), limit as i64),
        |r| r.text(0),
    )
    .await
}

pub(super) const STALE_SESSION_RUN_SQL: &str = "SELECT 1 FROM runs r WHERE r.session_id = ?1
     AND NOT EXISTS (SELECT 1 FROM history_tier_runs m JOIN history_tier h ON h.scope_id=m.scope_id
                     WHERE m.run_id=r.id) LIMIT 1";
pub(super) const STALE_SESSION_TURN_SQL: &str =
    "SELECT 1 FROM turns t WHERE t.session_id = ?1 AND t.run_id IS NOT NULL
     AND NOT EXISTS (SELECT 1 FROM history_tier_runs m JOIN history_tier h ON h.scope_id=m.scope_id
                     WHERE m.run_id=t.run_id) LIMIT 1";
pub(super) const STALE_SESSION_EVENT_SQL: &str = "SELECT 1 FROM events e WHERE e.session_id = ?1
     AND NOT EXISTS (SELECT 1 FROM history_tier_runs m JOIN history_tier h ON h.scope_id=m.scope_id
                     WHERE m.run_id=e.run_id) LIMIT 1";
pub(super) const STALE_TASK_SQL: &str = "SELECT 1 FROM runs r WHERE r.job_id = ?1
     AND NOT EXISTS (SELECT 1 FROM history_tier_runs m JOIN history_tier h ON h.scope_id=m.scope_id
                     WHERE m.run_id=r.id) LIMIT 1";

/// Owner probes are restricted to packed sessions: its live tail is expected.
/// Task probes cover the whole job, so reopening cannot leave a stale pack.
/// This detects unmapped runs, not late appends to an already mapped run.
pub async fn is_stale(db: &LocalDb, scope_id: &str) -> DbResult<bool> {
    let Some(owner) = db.query_text(
        "SELECT owner_id FROM history_tier WHERE scope_id=?1 AND kind='thread_range' AND evicted_at IS NULL",
        (scope_id.to_string(),),
    ).await? else { return Ok(false); };
    let sessions = db
        .query_all(
            "SELECT session_id FROM history_tier_sessions WHERE scope_id=?1",
            (scope_id.to_string(),),
            |r| r.text(0),
        )
        .await?;
    let mut task_jobs = BTreeSet::new();
    for session in sessions {
        for sql in [
            STALE_SESSION_RUN_SQL,
            STALE_SESSION_TURN_SQL,
            STALE_SESSION_EVENT_SQL,
        ] {
            if db.query_opt_i64(sql, (session.clone(),)).await?.is_some() {
                return Ok(true);
            }
        }
        if let Some(job) = db
            .query_text("SELECT job_id FROM sessions WHERE id=?1", (session,))
            .await?
        {
            if job != owner {
                task_jobs.insert(job);
            }
        }
    }
    // A task with no session still has run mappings.
    let jobs = db.query_all(
        "SELECT r.job_id FROM history_tier_runs m JOIN runs r ON r.id=m.run_id WHERE m.scope_id=?1",
        (scope_id.to_string(),), |r| r.opt_text(0),
    ).await?;
    task_jobs.extend(jobs.into_iter().flatten().filter(|j| j != &owner));
    for job in task_jobs {
        let status = db
            .query_text("SELECT status FROM jobs WHERE id=?1", (job.clone(),))
            .await?;
        if !status.as_deref().is_some_and(terminal_job)
            || db.query_opt_i64(STALE_TASK_SQL, (job,)).await?.is_some()
        {
            return Ok(true);
        }
    }
    Ok(false)
}

/// The guard and mapping removal share a retrying transaction. Evicted scopes
/// and their mappings are immutable. Unreferenced store objects remain for GC.
pub async fn invalidate_unevicted(db: &LocalDb, scope_id: &str) -> DbResult<bool> {
    let id = scope_id.to_string();
    db.write(move |conn| {
        let id=id.clone();
        Box::pin(async move {
            let dropped = conn.execute(
                "DELETE FROM history_tier WHERE scope_id=?1 AND kind='thread_range' AND evicted_at IS NULL",
                (id.as_str(),),
            ).await?;
            if dropped > 0 {
                for table in ["history_tier_runs","history_tier_sessions"] {
                    conn.execute(format!("DELETE FROM {table} WHERE scope_id=?1"),(id.as_str(),)).await?;
                }
            }
            Ok(dropped > 0)
        })
    }).await
}
