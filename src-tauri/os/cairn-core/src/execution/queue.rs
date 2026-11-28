//! The run queue: the one authority on whether an issue execution runs.
//!
//! Each runner keeps one ordered list (`run_queue`, in its private database)
//! of every issue execution it wants to run, across all of its projects. An
//! execution joins at the end when it becomes runnable (filed with no `after:`
//! gate, or its last gate released) and leaves only when it stops being live:
//! superseded, withdrawn, or its issue merged or closed. Starting to run, and
//! finishing its recipe, do not remove it.
//!
//! Seats are given over that one list ([`cairn_workflow::queue::selection`])
//! by what each entry is doing: the greatest activity among its jobs, a fixed
//! `CASE` over facts each job row stores
//! ([`cairn_db::stored_status::jobs::QUEUE_ACTIVITY_SQL`]). Each fact is an
//! explicit value its acting site writes in its own transaction, and the
//! queue reads only active and wanting jobs, through a partial index on that
//! predicate, so a pass costs the work in flight rather than the number of
//! live executions:
//!
//! - **Active**: agent work is in flight. A turn is running or due, or the
//!   execution waits on something Cairn is doing now (its own check lanes or
//!   check requests, a run batch, a terminal, a tool call, a timer, a provider
//!   limit with a known reset). It holds a seat, through the wait, wherever it
//!   is in the list, so it never has to win one back and is never cut. A
//!   parent parked on blocking tasks is due from the moment its last task
//!   settles: the claimed `dependency_unblock` marker heading its job, with no
//!   run and no `end_reason`, is its resume owed. A resume that cannot launch
//!   stamps the marker's `end_reason`, so a stalled marker pins no seat.
//! - **Wanting**: work is due and the queue holds it (a queue hold). It takes
//!   a free seat by its place in the list.
//! - **Dormant**: nothing it waits on is moving: a person (a review, a
//!   question, a permission, an approval, an operator request), or nothing at
//!   all. It holds no seat and keeps its place. When a person acts on it its
//!   launch asks [`admit`], and it competes for the next free seat as wanting.
//!
//! No timer and no age enter the decision. The two caps bound the seats
//! globally and per thread; active work counts against them even past them,
//! and only wanting work waits for room.
//!
//! Park is the only way out short of resolution: a parked row stays stored
//! (so nothing re-adds it) but takes no position, and parking stops its
//! running work at the next tool boundary. Unpark returns it to the end.
//!
//! Every change to the list ends in [`settle`], which reads the list, decides,
//! and resumes the held work of every seated execution. The same pass runs at
//! startup and on the gate sweep's cadence, so a restart reproduces the seats
//! from the stored list and the stored facts alone.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};

use cairn_db::turso::params;
pub use cairn_workflow::queue::Activity;
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
    pub activity: Activity,
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
    /// Holds a seat.
    pub runs: bool,
    pub parked: bool,
    pub activity: Activity,
    /// Wanting and given no seat: its place among those waiting for one.
    pub in_line: Option<usize>,
}

impl QueueItem {
    /// What a launch held for this entry says: `Queued · 2nd in line`.
    pub fn queued_label(&self) -> String {
        match self.in_line {
            Some(place) => format!("Queued · {} in line", ordinal(place)),
            None => "Queued".into(),
        }
    }
}

fn ordinal(n: usize) -> String {
    let suffix = match (n % 10, n % 100) {
        (_, 11..=13) => "th",
        (1, _) => "st",
        (2, _) => "nd",
        (3, _) => "rd",
        _ => "th",
    };
    format!("{n}{suffix}")
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
    /// Whether `execution_id` holds a seat.
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
/// no open database knows cannot run here, so it is never seated. `asking` is
/// an execution whose launch is asking for a seat: it competes as wanting
/// even before anything is held for it.
pub fn decide(
    rows: &[StoredRow],
    facts: &HashMap<String, ExecutionFact>,
    caps: QueueCaps,
    asking: Option<&str>,
) -> QueueState {
    let activity = |id: &str| {
        let stored = facts
            .get(id)
            .map_or(Activity::Dormant, |fact| fact.activity);
        if asking == Some(id) && stored == Activity::Dormant {
            Activity::Wanting
        } else {
            stored
        }
    };
    let listed: Vec<&StoredRow> = rows.iter().filter(|row| !row.parked).collect();
    let entries: Vec<QueueEntry> = listed
        .iter()
        .filter(|row| facts.contains_key(&row.execution_id))
        .map(|row| QueueEntry {
            execution_id: row.execution_id.clone(),
            thread_id: facts[&row.execution_id].thread_id.clone(),
            activity: activity(&row.execution_id),
        })
        .collect();
    let decisions: HashMap<&str, cairn_workflow::queue::QueueDecision> = entries
        .iter()
        .zip(selection(&entries, caps))
        .map(|(entry, decision)| (entry.execution_id.as_str(), decision))
        .collect();
    let mut items: Vec<QueueItem> = listed
        .iter()
        .enumerate()
        .map(|(index, row)| {
            let decision = decisions.get(row.execution_id.as_str());
            QueueItem {
                execution_id: row.execution_id.clone(),
                issue_id: row.issue_id.clone(),
                thread_id: facts
                    .get(&row.execution_id)
                    .and_then(|fact| fact.thread_id.clone()),
                position: Some(index + 1),
                runs: decision.is_some_and(|decision| decision.admitted),
                parked: false,
                activity: activity(&row.execution_id),
                in_line: decision.and_then(|decision| decision.in_line),
            }
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
            activity: facts
                .get(&row.execution_id)
                .map_or(Activity::Dormant, |fact| fact.activity),
            in_line: None,
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

/// The live executions on this runner (`?1`) in one database that `$source`
/// names: each one's issue, its thread, and whether it is runnable now. `held`
/// is an execution waiting on an authored `after:` gate; it is not in the list
/// until released. What each is doing is read beside it from its jobs' stored
/// activity. `$source` is the last conjunct, so a bound list can follow it.
/// `CROSS JOIN` keeps the executions driving the issue lookup.
macro_rules! live_executions_sql {
    ($index:literal, $source:expr) => {
        concat!(
            "SELECT e.id, e.issue_id, i.parent_thread_id,
                    CASE WHEN ",
            cairn_db::runnable_execution_status_sql!(),
            " AND NOT (",
            cairn_db::gated_unreleased_sql!(),
            ") THEN 1 ELSE 0 END,
                    COALESCE(e.released_at, e.started_at), e.queue_state
               FROM executions e ",
            $index,
            " CROSS JOIN issues i ON i.id = e.issue_id
              WHERE e.lifecycle IN ('canonical', 'pending')
                AND COALESCE(e.runner_device_id, ?1) = ?1 AND ",
            cairn_db::live_execution_sql!(),
            " AND ",
            $source
        )
    };
}

/// The facts read touches only executions that can matter to the list. The
/// facts are consulted for a row the list already holds (to keep or drop it,
/// decide it, and record its state) and for a joiner, which must be runnable.
/// Any other live execution (complete, failed, cancelled or held, and not
/// listed) is never looked at, so reading it would only walk history. A
/// complete or failed execution on an unresolved issue does stay listed, since
/// its jobs can run again in place; it is read by its row's id.
///
/// The runnable half scans the partial `idx_executions_live`, whose predicate
/// the bound spells, so it costs the executions running now.
const RUNNABLE_EXECUTIONS_SQL: &str = live_executions_sql!(
    "INDEXED BY idx_executions_live",
    cairn_db::runnable_execution_status_sql!()
);

/// The listed half: the list's rows, by id, closed by [`listed_ids`]. No
/// `INDEXED BY`: the pinned Turso serves a forced index only through an
/// equality, and turns an `IN` list under the hint into a scan of the whole
/// index (docs/database.md). Unhinted, each id seeks the covering
/// `idx_executions_hold_facts`.
const LISTED_EXECUTIONS_SQL: &str = live_executions_sql!("", "e.id IN ");

/// Which of the listed ids this database knows at all, live or not, closed by
/// [`listed_ids`]. Unhinted for the same reason as [`LISTED_EXECUTIONS_SQL`].
const PRESENT_EXECUTIONS_SQL: &str = "SELECT e.id FROM executions e WHERE e.id IN ";

/// The bound id list that closes [`LISTED_EXECUTIONS_SQL`] or
/// [`PRESENT_EXECUTIONS_SQL`]: `count` values numbered from `?2`, after the
/// runner's `?1`.
fn listed_ids(count: usize) -> String {
    let markers: Vec<String> = (2..count + 2).map(|n| format!("?{n}")).collect();
    format!("({})", markers.join(", "))
}

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

/// One live execution's fact and the queue state its row last recorded.
type LiveRow = (ExecutionFact, Option<String>);

/// Read one statement's live executions into `live`. A runnable listed
/// execution comes back from both halves; it is the same row both times.
async fn read_live(
    conn: &TrackedConnection,
    sql: &str,
    values: Vec<cairn_db::turso::Value>,
    live: &mut HashMap<String, LiveRow>,
) -> DbResult<()> {
    let mut rows = conn.query(sql, values).await?;
    while let Some(row) = rows.next().await? {
        live.insert(
            row.text(0)?,
            (
                ExecutionFact {
                    issue_id: row.text(1)?,
                    thread_id: row.opt_text(2)?,
                    joinable: row.i64(3)? != 0,
                    since: row.opt_i64(4)?.unwrap_or(0),
                    activity: Activity::Dormant,
                },
                row.opt_text(5)?,
            ),
        );
    }
    Ok(())
}

/// The live executions on this runner the list can need, across every open
/// database (see [`RUNNABLE_EXECUTIONS_SQL`]), and which of the listed
/// executions those databases know of at all.
async fn execution_facts(
    orch: &Orchestrator,
    stored: &[StoredRow],
) -> Result<(HashMap<String, ExecutionFact>, HashSet<String>, Located), String> {
    use cairn_db::turso::Value;
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
                    let mut live = HashMap::new();
                    read_live(
                        conn,
                        RUNNABLE_EXECUTIONS_SQL,
                        vec![Value::Text(device.clone())],
                        &mut live,
                    )
                    .await?;
                    let mut present = Vec::new();
                    for chunk in ids.chunks(cairn_db::sql::IN_LIST_CHUNK) {
                        let values: Vec<Value> = std::iter::once(&device)
                            .chain(chunk)
                            .map(|value| Value::Text(value.clone()))
                            .collect();
                        let list = listed_ids(chunk.len());
                        read_live(
                            conn,
                            &format!("{LISTED_EXECUTIONS_SQL}{list}"),
                            values.clone(),
                            &mut live,
                        )
                        .await?;
                        let mut rows = conn
                            .query(&format!("{PRESENT_EXECUTIONS_SQL}{list}"), values)
                            .await?;
                        while let Some(row) = rows.next().await? {
                            present.push(row.text(0)?);
                        }
                    }
                    let doing = cairn_db::stored_status::jobs::non_dormant_executions(conn).await?;
                    for (id, (fact, _)) in &mut live {
                        if let Some(activity) = doing.get(id.as_str()) {
                            fact.activity = *activity;
                        }
                    }
                    Ok((live, present))
                })
            })
            .await
            .map_err(|error| format!("failed to read the run queue's executions: {error}"))?;
        known.extend(present);
        for (id, (fact, queue_state)) in live {
            known.insert(id.clone());
            located.insert(id.clone(), (db.clone(), queue_state));
            facts.insert(id, fact);
        }
    }
    Ok((facts, known, located))
}

/// The queue state an issue's status projection reads from its execution:
/// `queued` while its due work waits for a seat, `parked` when parked, none
/// when it is seated or dormant (a dormant entry reads as what it waits on).
fn recorded_state(item: &QueueItem) -> Option<&'static str> {
    match (item.parked, item.runs, item.activity) {
        (true, _, _) => Some("parked"),
        (false, false, Activity::Wanting) => Some("queued"),
        _ => None,
    }
}

/// Record each execution's queue state where its issue's status is projected,
/// and re-project that status, only where it changed. The queue is the one
/// writer of `executions.queue_state`. Returns how many it wrote.
async fn record_states(state: &QueueState, located: &Located) -> usize {
    let mut records = 0;
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
        match written {
            Ok(()) => records += 1,
            Err(error) => log::warn!(
                "run queue could not record the state of execution {}: {error}",
                item.execution_id
            ),
        }
    }
    records
}

/// Where one settle spent its time, for the gate sweep's pass line.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SettleTimings {
    /// Waiting for [`QUEUE_LOCK`].
    pub wait_ms: u64,
    /// Holding it: reading, deciding and storing the list.
    pub lock_ms: u64,
    /// The part of `lock_ms` spent reading the executions' facts.
    pub facts_ms: u64,
    /// Acting on the decision, after the lock is released.
    pub act_ms: u64,
    /// Queue states recorded by the act.
    pub act_records: usize,
    /// Seated executions whose held work the act resumed.
    pub act_resumes: usize,
    /// The part of `act_ms` spent resuming them.
    pub act_resume_ms: u64,
}

fn elapsed_ms(since: std::time::Instant) -> u64 {
    u64::try_from(since.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// What one locked pass found and how long it took.
struct Pass {
    state: QueueState,
    located: Located,
    timings: SettleTimings,
}

/// Read the list, bring its membership up to date, apply `change` to it, store
/// it, and decide it, `asking` competing as wanting. `change` returns false
/// when its subject is not in the list, which fails the call without writing.
/// `caller` names the pass in its DEBUG timing line.
async fn under_lock(
    orch: &Orchestrator,
    caller: &'static str,
    asking: Option<&str>,
    change: impl FnOnce(&mut Vec<StoredRow>) -> Result<(), String>,
) -> Result<Pass, String> {
    let waited = std::time::Instant::now();
    let _guard = QUEUE_LOCK.lock().await;
    let wait_ms = elapsed_ms(waited);
    let locked = std::time::Instant::now();
    let local = orch.db.local.clone();
    let stored = stored_rows(&local)
        .await
        .map_err(|error| format!("failed to read the run queue: {error}"))?;
    let before = stored.clone();
    let facts_started = std::time::Instant::now();
    let (facts, known, located) = execution_facts(orch, &stored).await?;
    let facts_ms = elapsed_ms(facts_started);
    let Membership {
        mut rows, dropped, ..
    } = plan_membership(stored, &facts, &known);
    change(&mut rows)?;
    if rows != before || !dropped.is_empty() {
        write_rows(&local, rows.clone(), dropped, orch.services.clock.now())
            .await
            .map_err(|error| format!("failed to store the run queue: {error}"))?;
    }
    let state = decide(&rows, &facts, caps(orch), asking);
    let lock_ms = elapsed_ms(locked);
    log::debug!(
        "run queue pass caller={caller} wait_ms={wait_ms} lock_ms={lock_ms} facts_ms={facts_ms} live={}",
        facts.len()
    );
    Ok(Pass {
        state,
        located,
        timings: SettleTimings {
            wait_ms,
            lock_ms,
            facts_ms,
            ..SettleTimings::default()
        },
    })
}

/// The list as it stands, membership brought up to date.
pub async fn read(orch: &Orchestrator) -> Result<QueueState, String> {
    Ok(under_lock(orch, "read", None, |_| Ok(())).await?.state)
}

/// The decision a launch of `execution_id`'s work gets: seated already, or
/// given a free seat by its place as wanting work. Every path that would start
/// or continue an issue execution's work asks this, and nothing else.
pub async fn admit(orch: &Orchestrator, execution_id: &str) -> Result<QueueState, String> {
    Ok(under_lock(orch, "admit", Some(execution_id), |_| Ok(()))
        .await?
        .state)
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
    let Pass { state, located, .. } = under_lock(orch, "move", None, |rows| {
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

/// Park (`true`) or unpark an entry, then act on what that changes. Parking
/// stops the execution's running work at its next tool boundary, resumably;
/// it is the one change that stops work the queue already seated.
pub async fn set_parked(
    orch: &Orchestrator,
    execution_id: &str,
    parked: bool,
) -> Result<QueueState, String> {
    let Pass { state, located, .. } = under_lock(orch, "park", None, |rows| {
        plan_park(rows, execution_id, parked)
            .then_some(())
            .ok_or_else(|| missing(execution_id))
    })
    .await?;
    act(orch, &state, &located).await;
    if parked {
        if let Err(error) =
            crate::orchestrator::lifecycle::displace_execution(orch, execution_id).await
        {
            log::warn!("run queue could not stop parked execution {execution_id}: {error}");
        }
    }
    Ok(state)
}

/// Read and decide the list and act on it: the pass every change ends in,
/// and the startup and periodic backstop.
pub async fn settle(orch: &Orchestrator) -> Result<QueueState, String> {
    Ok(settle_timed(orch).await?.0)
}

/// [`settle`], with where its time went.
pub async fn settle_timed(orch: &Orchestrator) -> Result<(QueueState, SettleTimings), String> {
    let Pass {
        state,
        located,
        mut timings,
    } = under_lock(orch, "settle", None, |_| Ok(())).await?;
    let acted = std::time::Instant::now();
    let (records, resumes, resume_ms) = act(orch, &state, &located).await;
    timings.act_ms = elapsed_ms(acted);
    timings.act_records = records;
    timings.act_resumes = resumes;
    timings.act_resume_ms = resume_ms;
    Ok((state, timings))
}

/// Make the issues' statuses match the decision and resume the held work of
/// every seated execution. Nothing seated is ever stopped here: active work
/// holds its seat, and an unseated entry has no work running. Idempotent: a
/// seated execution with nothing held and an unchanged state are no-ops.
/// Returns the states recorded, the executions resumed, and the time resuming
/// took.
async fn act(orch: &Orchestrator, state: &QueueState, located: &Located) -> (usize, usize, u64) {
    let records = record_states(state, located).await;
    let resuming = std::time::Instant::now();
    let mut resumes = 0;
    for item in state.items.iter().filter(|item| item.runs) {
        resumes += 1;
        if let Err(error) =
            crate::orchestrator::wakes::resume_queue_held(orch, &item.execution_id).await
        {
            log::warn!(
                "run queue could not resume execution {}: {error}",
                item.execution_id
            );
        }
    }
    (records, resumes, elapsed_ms(resuming))
}

#[cfg(test)]
#[path = "queue_tests.rs"]
pub(crate) mod tests;
