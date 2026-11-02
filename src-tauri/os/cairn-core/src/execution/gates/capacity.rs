//! The execution queue: two capacity caps that decide whether a new execution
//! starts now or holds (cairn/5802; tenets 1288 and 1350).
//!
//! ## One queue, not a second scheduler
//!
//! A cap hold is a GATE ROW of a second kind beside the authored `after:`
//! gates: `{"fact":"fleet_slot"}` or `{"fact":"thread_focus"}`, with its live
//! reason in `execution_gates.detail`. Being a gate row, a cap-held execution is
//! held by the one definition every surface already reads
//! (`cairn_db::awaiting_release_sql!`): it has no jobs, so it takes no slot and
//! no cell and placement never sees it, and it is released by the same guarded,
//! exactly-once `release_conn` the sweep uses for every other gate.
//!
//! Capacity gates differ from authored ones in three ways. Agents cannot author
//! them (`authoring` refuses the facts by name). They are LEVEL conditions that
//! are never latched in advance: a free slot can be taken again, so they are
//! re-decided inside the release transaction itself, under one process-wide
//! admission lock ([`decide`]). And only the operator's Start now skips them.
//!
//! ## The two caps count different things
//!
//! - **Fleet slot** protects machines. It counts executions in progress on THIS
//!   runner's fleet: started, with a job that is working (pending placement, or
//!   running and not parked off the machine), not waiting on the operator.
//!   Sub-agent tasks count as part of their execution; threads never count.
//! - **Thread focus** protects a thread's attention and keeps PRs from going
//!   stale. It counts the thread's direct child issues that have started and
//!   are not yet merged or closed, a child whose PR waits for review included.
//!
//! Neither is a load bound. The caps gate only STARTS (filing and release);
//! placement and per-machine admission never consult them, and nothing here
//! ever parks, stops, or re-holds running work. An answered question, a granted
//! permission, a wake: none of them is a start, so none of them queues.
//!
//! ## Why a lock
//!
//! The fleet count spans every open database, and a per-database write
//! transaction cannot serialize it. Every capped filing and every release takes
//! [`Orchestrator::admission_lock`](crate::orchestrator::Orchestrator), decides,
//! and writes before letting go, so two admissions never interleave. Outside
//! the lock a count can only fall (a completion) or rise through the operator's
//! own starts, which are allowed over the cap, so a stale count is only ever
//! conservative. Exactly-once still rests on the guarded `released_at` update.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, LazyLock, Mutex};

use cairn_db::turso::params;
use cairn_workflow::gates::store::{
    list_blocking_capacity_gates_conn, set_gate_detail_conn, CAPACITY_GATE_SEQ_BASE,
};

use crate::models::TurnYieldReason;
use crate::orchestrator::Orchestrator;
use crate::storage::{DbResult, LocalDb, RowExt};

/// The fleet cap's gate fact.
pub const FLEET_SLOT_FACT: &str = "fleet_slot";
/// The thread focus cap's gate fact. It names no thread: the thread is the
/// held issue's CURRENT parent, so a reparent needs no rewrite.
pub const THREAD_FOCUS_FACT: &str = "thread_focus";
/// Every capacity fact. `cairn_db::capacity_gate_sql!` spells the same set in
/// SQL; a test holds the two together.
pub const CAPACITY_FACTS: &[&str] = &[FLEET_SLOT_FACT, THREAD_FOCUS_FACT];

/// The stored predicate of a capacity gate.
pub fn capacity_predicate(fact: &str) -> String {
    serde_json::json!({ "fact": fact }).to_string()
}

/// The capacity fact a stored predicate names, or `None` for an authored gate.
pub fn capacity_fact(predicate_json: &str) -> Option<&'static str> {
    let value: serde_json::Value = serde_json::from_str(predicate_json).ok()?;
    let fact = value.get("fact")?.as_str()?;
    CAPACITY_FACTS.iter().copied().find(|known| *known == fact)
}

/// One cap that holds an execution right now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CapBlock {
    Fleet {
        in_progress: usize,
        limit: usize,
        /// Of `in_progress`, how many are still waiting for a machine, read
        /// from the placement queue. Said so the operator deciding on Start
        /// now knows the fleet may be stuck on placement, not on work.
        waiting_for_machine: usize,
    },
    Thread {
        thread_name: String,
        limit: usize,
        /// The open started children holding it, by issue number.
        open_children: Vec<i64>,
    },
}

impl CapBlock {
    pub fn fact(&self) -> &'static str {
        match self {
            Self::Fleet { .. } => FLEET_SLOT_FACT,
            Self::Thread { .. } => THREAD_FOCUS_FACT,
        }
    }

    /// How an operator start records skipping this cap.
    pub fn skipped_label(&self) -> &'static str {
        match self {
            Self::Fleet { .. } => "fleet",
            Self::Thread { .. } => "thread",
        }
    }

    pub(crate) fn seq(&self) -> i64 {
        match self {
            Self::Fleet { .. } => CAPACITY_GATE_SEQ_BASE,
            Self::Thread { .. } => CAPACITY_GATE_SEQ_BASE + 1,
        }
    }

    /// The reason, without its place in line.
    pub fn reason(&self) -> String {
        match self {
            Self::Fleet {
                in_progress,
                limit,
                waiting_for_machine,
            } => {
                let placing = match waiting_for_machine {
                    0 => String::new(),
                    n => format!(", {n} of them waiting for a machine"),
                };
                format!("waiting for a fleet slot: {in_progress} of {limit} in progress{placing}")
            }
            Self::Thread {
                thread_name,
                open_children,
                ..
            } => {
                let numbers: Vec<String> = open_children.iter().map(i64::to_string).collect();
                let noun = if open_children.len() == 1 {
                    "child"
                } else {
                    "children"
                };
                format!(
                    "{thread_name} has {} open {noun}: {}",
                    open_children.len(),
                    numbers.join(", ")
                )
            }
        }
    }
}

/// Every cap holding one execution, as decided under the admission lock.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Admission {
    pub blocks: Vec<CapBlock>,
}

impl Admission {
    pub fn is_clear(&self) -> bool {
        self.blocks.is_empty()
    }

    pub fn reasons(&self) -> Vec<String> {
        self.blocks.iter().map(CapBlock::reason).collect()
    }
}

/// Hold the admission lock across a decision and the write that acts on it.
pub(crate) async fn admission_lock(orch: &Orchestrator) -> tokio::sync::OwnedMutexGuard<()> {
    orch.admission_lock.clone().lock_owned().await
}

/// `"1st"`, `"2nd"`, ...
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

const PLACE_SUFFIX: &str = " in line)";

/// A reason with its place in line.
pub fn with_place(reason: &str, place: usize) -> String {
    format!("{reason} ({}{PLACE_SUFFIX}", ordinal(place))
}

/// The place-in-line suffix of a stored reason, `" (2nd in line)"`, if any.
pub(crate) fn place_suffix(detail: &str) -> Option<&str> {
    if !detail.ends_with(PLACE_SUFFIX) {
        return None;
    }
    detail.rfind(" (").map(|at| &detail[at..])
}

fn without_place(detail: &str) -> &str {
    match place_suffix(detail) {
        Some(suffix) => &detail[..detail.len() - suffix.len()],
        None => detail,
    }
}

/// A fresh reason carried onto a row that already has a place, so re-deciding
/// a hold does not churn its row between sweeps.
pub(crate) fn carry_place(existing: Option<&str>, reason: &str) -> String {
    match existing.and_then(place_suffix) {
        Some(suffix) => format!("{reason}{suffix}"),
        None => reason.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Counting
// ---------------------------------------------------------------------------

/// Whether a job uses a machine right now, judged from its current turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MachineUse {
    Uses,
    Free,
}

/// The pending durable wait a yielded `wait` turn is parked on.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ParkedWait {
    /// `agent_waits.condition_json`'s `kind`.
    pub kind: Option<String>,
    /// For a `checks` wait: the watched node belongs to the waiting job's own
    /// issue, so the lanes it waits on are this execution's machine work.
    pub watches_own_issue: bool,
    /// Any of the job's pending waits occupies a machine (a `run` batch, a
    /// terminal, checks on its own issue's node). One turn can park several
    /// calls at once, so the newest wait alone does not decide it.
    pub any_machine_wait: bool,
}

/// Whether a job's current turn uses a machine. `Err` names a yield this
/// classification does not know, which then does NOT count: a missed slot
/// over-releases by one (per-machine admission absorbs that), while a held
/// slot can deadlock the fleet. The yield reasons are exhaustive over
/// [`TurnYieldReason`], so a new variant fails to compile here until someone
/// decides it.
///
/// A turn parked on its own child work frees its slot: a coordinator parked on
/// the children it filed (a `waitFor` duration, or checks on a child's node)
/// would otherwise hold a slot those capped children need, and six of them
/// would deadlock the fleet. Delegated sub-agent tasks are jobs of the same
/// execution, so a `dependency_wait` parent is counted through them.
pub(crate) fn job_machine_use(
    turn_state: Option<&str>,
    yield_reason: Option<&str>,
    wait: &ParkedWait,
) -> Result<MachineUse, String> {
    if turn_state != Some("yielded") {
        return Ok(MachineUse::Uses);
    }
    let Some(raw) = yield_reason else {
        return Err("yielded without a reason".to_string());
    };
    let reason: TurnYieldReason = raw.parse().map_err(|_| raw.to_string())?;
    Ok(match reason {
        // Waiting on the operator; `job_awaits_operator_sql!` usually catches
        // these first.
        TurnYieldReason::UserInput | TurnYieldReason::Permission => MachineUse::Free,
        // Its delegated children count on their own job rows.
        TurnYieldReason::DependencyWait => MachineUse::Free,
        // Waiting on the operator or on provider capacity, not on a machine.
        TurnYieldReason::SafetyPause | TurnYieldReason::ProviderLimit => MachineUse::Free,
        TurnYieldReason::Wait => return wait_machine_use(wait),
    })
}

fn wait_machine_use(wait: &ParkedWait) -> Result<MachineUse, String> {
    if wait.any_machine_wait {
        return Ok(MachineUse::Uses);
    }
    match wait.kind.as_deref() {
        // A coordinator waiting on its children's progress.
        Some("duration") => Ok(MachineUse::Free),
        Some("checks") if wait.watches_own_issue => Ok(MachineUse::Uses),
        Some("checks") => Ok(MachineUse::Free),
        // A process, or a `run` batch parked past its grace window: both
        // occupy a machine.
        Some("terminal") | Some("run_batch") => Ok(MachineUse::Uses),
        // An MCP tool call runs on the gateway host, not on an executor.
        Some("mcp_continuation") => Ok(MachineUse::Free),
        Some(other) => Err(format!("wait:{other}")),
        None => Err("wait:none".to_string()),
    }
}

/// `capacity_unclassified_yield{reason}`: how often [`decide`] met a parked
/// turn it could not classify, by reason. Process-local, surfaced on the
/// queue view, so a new yield kind that should count is found by name rather
/// than by a fleet that quietly runs one over.
static UNCLASSIFIED_YIELDS: LazyLock<Mutex<BTreeMap<String, u64>>> =
    LazyLock::new(|| Mutex::new(BTreeMap::new()));

fn note_unclassified(reason: &str) {
    let Ok(mut counts) = UNCLASSIFIED_YIELDS.lock() else {
        return;
    };
    let count = counts.entry(reason.to_string()).or_insert(0);
    *count += 1;
    if *count == 1 {
        log::warn!(
            "capacity_unclassified_yield{{reason={reason}}}: a parked turn the fleet cap cannot \
             classify; it is not counted toward the fleet"
        );
    }
}

/// Every unclassified parked-turn reason seen by this process, with its count.
pub fn unclassified_yield_counts() -> BTreeMap<String, u64> {
    UNCLASSIFIED_YIELDS
        .lock()
        .map(|counts| counts.clone())
        .unwrap_or_default()
}

/// Candidate working jobs of executions the fleet may count, on this device.
const FLEET_JOBS_SQL: &str = concat!(
    "SELECT e.id, j.id, j.issue_id, t.state, t.yield_reason,
            (SELECT json_extract(aw.condition_json, '$.kind') FROM agent_waits aw
              WHERE aw.job_id = j.id AND aw.state IN ('pending', 'resolving')
              ORDER BY aw.created_at DESC LIMIT 1),
            (SELECT watched.issue_id FROM agent_waits aw
               JOIN jobs watched ON watched.id = json_extract(aw.condition_json, '$.job_id')
              WHERE aw.job_id = j.id AND aw.state IN ('pending', 'resolving')
              ORDER BY aw.created_at DESC LIMIT 1),
            EXISTS (SELECT 1 FROM agent_waits aw
               LEFT JOIN jobs watched ON watched.id = json_extract(aw.condition_json, '$.job_id')
              WHERE aw.job_id = j.id AND aw.state IN ('pending', 'resolving')
                AND (json_extract(aw.condition_json, '$.kind') IN ('terminal', 'run_batch')
                     OR (json_extract(aw.condition_json, '$.kind') = 'checks'
                         AND watched.issue_id = j.issue_id)))
       FROM executions e
       JOIN jobs j ON j.execution_id = e.id
       LEFT JOIN turns t ON t.id = j.current_turn_id
      WHERE COALESCE(e.runner_device_id, ?1) = ?1 AND ",
    cairn_db::fleet_counted_execution_sql!(),
    " AND ",
    cairn_db::fleet_working_job_sql!()
);

struct FleetJobRow {
    execution_id: String,
    job_id: String,
    issue_id: Option<String>,
    turn_state: Option<String>,
    yield_reason: Option<String>,
    wait_kind: Option<String>,
    watched_issue_id: Option<String>,
    any_machine_wait: bool,
}

async fn fleet_job_rows(db: &LocalDb, device: &str) -> DbResult<Vec<FleetJobRow>> {
    let device = device.to_string();
    db.read(move |conn| {
        let device = device.clone();
        Box::pin(async move {
            let mut rows = conn.query(FLEET_JOBS_SQL, params![device.as_str()]).await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                out.push(FleetJobRow {
                    execution_id: row.text(0)?,
                    job_id: row.text(1)?,
                    issue_id: row.opt_text(2)?,
                    turn_state: row.opt_text(3)?,
                    yield_reason: row.opt_text(4)?,
                    wait_kind: row.opt_text(5)?,
                    watched_issue_id: row.opt_text(6)?,
                    any_machine_wait: row.i64(7)? != 0,
                });
            }
            Ok(out)
        })
    })
    .await
}

/// The fleet as the cap sees it: executions in progress on this runner, and
/// how many of them are still waiting for a machine.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FleetLoad {
    pub in_progress: usize,
    pub waiting_for_machine: usize,
}

/// Count executions in progress across every open database.
pub async fn fleet_load(orch: &Orchestrator) -> Result<FleetLoad, String> {
    let device = orch.anon_device_manager.device_id();
    let waiting_jobs = orch.fleet.jobs_waiting_for_a_machine();
    let mut working: HashMap<String, bool> = HashMap::new();
    for db in orch.db.all_dbs().await {
        let rows = fleet_job_rows(&db, &device)
            .await
            .map_err(|error| format!("failed to count the fleet: {error}"))?;
        for row in rows {
            let wait = ParkedWait {
                watches_own_issue: row.wait_kind.as_deref() == Some("checks")
                    && row.watched_issue_id.is_some()
                    && row.watched_issue_id == row.issue_id,
                kind: row.wait_kind,
                any_machine_wait: row.any_machine_wait,
            };
            let uses = match job_machine_use(
                row.turn_state.as_deref(),
                row.yield_reason.as_deref(),
                &wait,
            ) {
                Ok(uses) => uses,
                Err(reason) => {
                    note_unclassified(&reason);
                    MachineUse::Free
                }
            };
            if uses == MachineUse::Uses {
                let placing = waiting_jobs.contains(&row.job_id);
                let entry = working.entry(row.execution_id).or_insert(false);
                *entry |= placing;
            }
        }
    }
    Ok(FleetLoad {
        in_progress: working.len(),
        waiting_for_machine: working.values().filter(|placing| **placing).count(),
    })
}

/// The issue's thread, as the focus cap reads it.
struct ThreadFocus {
    thread_id: String,
    thread_name: String,
    limit: usize,
    open_children: Vec<(i64, String)>,
}

const THREAD_OF_ISSUE_SQL: &str = "SELECT t.id, t.name, t.max_open_children
       FROM issues i JOIN threads t ON t.id = i.parent_thread_id
      WHERE i.id = ?1";

const OPEN_THREAD_CHILDREN_SQL: &str = concat!(
    "SELECT i.number, i.id FROM issues i WHERE i.parent_thread_id = ?1 AND ",
    cairn_db::open_thread_child_sql!(),
    " ORDER BY i.number"
);

async fn thread_focus(
    db: &LocalDb,
    issue_id: &str,
    default_limit: i32,
) -> DbResult<Option<ThreadFocus>> {
    let issue_id = issue_id.to_string();
    db.read(move |conn| {
        let issue_id = issue_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(THREAD_OF_ISSUE_SQL, params![issue_id.as_str()])
                .await?;
            let Some(row) = rows.next().await? else {
                return Ok(None);
            };
            let thread_id = row.text(0)?;
            let thread_name = row.text(1)?;
            let limit = row.opt_i64(2)?.map(|limit| limit as i32);
            drop(rows);
            let mut rows = conn
                .query(OPEN_THREAD_CHILDREN_SQL, params![thread_id.as_str()])
                .await?;
            let mut open_children = Vec::new();
            while let Some(row) = rows.next().await? {
                open_children.push((row.i64(0)?, row.text(1)?));
            }
            Ok(Some(ThreadFocus {
                thread_id,
                thread_name,
                limit: limit.unwrap_or(default_limit).max(1) as usize,
                open_children,
            }))
        })
    })
    .await
}

/// Its own place in line on `fact`: when it first held on that cap, or `None`
/// for a start that has not queued on it (a new filing, or one whose `after:`
/// gates just cleared), which then queues behind everything already held.
const OWN_PLACE_SQL: &str = "SELECT MIN(created_at) FROM execution_gates
      WHERE execution_id = ?1 AND satisfied_at IS NULL
        AND json_extract(predicate_json, '$.fact') = ?2";

/// Fleet holds of this device's other executions that are ahead of `?2`.
const FLEET_AHEAD_SQL: &str = concat!(
    "SELECT COUNT(*) FROM execution_gates g JOIN executions e ON e.id = g.execution_id
      WHERE json_extract(g.predicate_json, '$.fact') = 'fleet_slot'
        AND g.satisfied_at IS NULL AND g.last_outcome = 'no_evidence'
        AND e.id IS NOT ?1 AND g.created_at < ?2
        AND COALESCE(e.runner_device_id, ?3) = ?3 AND ",
    cairn_db::awaiting_release_sql!(),
    " AND NOT ",
    cairn_db::awaits_authored_gate_sql!()
);

/// Focus holds of the same thread's other children that are ahead of `?2`.
const THREAD_AHEAD_SQL: &str = concat!(
    "SELECT COUNT(*) FROM execution_gates g JOIN executions e ON e.id = g.execution_id
       JOIN issues held_child ON held_child.id = e.issue_id
      WHERE json_extract(g.predicate_json, '$.fact') = 'thread_focus'
        AND g.satisfied_at IS NULL AND g.last_outcome = 'no_evidence'
        AND e.id IS NOT ?1 AND g.created_at < ?2
        AND held_child.parent_thread_id = ?3 AND ",
    cairn_db::awaiting_release_sql!(),
    " AND NOT ",
    cairn_db::awaits_authored_gate_sql!()
);

async fn own_place(db: &LocalDb, execution_id: Option<&str>, fact: &str) -> DbResult<i64> {
    let Some(execution_id) = execution_id else {
        return Ok(i64::MAX);
    };
    let (execution_id, fact) = (execution_id.to_string(), fact.to_string());
    let place = db
        .read(move |conn| {
            let (execution_id, fact) = (execution_id.clone(), fact.clone());
            Box::pin(async move {
                crate::storage::query_opt_i64_conn(
                    conn,
                    OWN_PLACE_SQL,
                    params![execution_id.as_str(), fact.as_str()],
                )
                .await
            })
        })
        .await?;
    Ok(place.unwrap_or(i64::MAX))
}

async fn count_ahead(
    db: &LocalDb,
    sql: &'static str,
    execution_id: Option<&str>,
    place: i64,
    scope: &str,
) -> DbResult<i64> {
    let (execution_id, scope) = (execution_id.map(str::to_string), scope.to_string());
    let ahead = db
        .read(move |conn| {
            let (execution_id, scope) = (execution_id.clone(), scope.clone());
            Box::pin(async move {
                crate::storage::query_opt_i64_conn(
                    conn,
                    sql,
                    params![execution_id.as_deref(), place, scope.as_str()],
                )
                .await
            })
        })
        .await?;
    Ok(ahead.unwrap_or(0))
}

/// Decide which caps hold a start on `issue_id` right now. Call it holding
/// [`admission_lock`], and act on it before letting go.
///
/// `db` is the database that owns the issue. `execution_id` is the held
/// execution being released, or `None` for a new filing.
///
/// A cap holds when it is full, AND when anything is already held on it ahead
/// of this start: a slot that frees between sweeps goes to the oldest held
/// execution, never to a fresh filing that happened to land first. A start
/// that re-runs an issue already counted as an open child of its thread opens
/// no new child, so the thread cap does not hold it; nor does it when the
/// issue has no thread.
pub async fn decide(
    orch: &Orchestrator,
    db: &LocalDb,
    issue_id: Option<&str>,
    execution_id: Option<&str>,
) -> Result<Admission, String> {
    let settings = orch.get_settings();
    let mut blocks = Vec::new();
    let counting = |error: crate::storage::DbError| format!("failed to read the queue: {error}");

    let limit = settings.max_concurrent_executions.max(1) as usize;
    let fleet = fleet_load(orch).await?;
    let place = own_place(db, execution_id, FLEET_SLOT_FACT)
        .await
        .map_err(counting)?;
    let device = orch.anon_device_manager.device_id();
    let mut fleet_ahead = 0;
    for queue_db in orch.db.all_dbs().await {
        fleet_ahead += count_ahead(&queue_db, FLEET_AHEAD_SQL, execution_id, place, &device)
            .await
            .map_err(counting)?;
    }
    if fleet.in_progress >= limit || fleet_ahead > 0 {
        blocks.push(CapBlock::Fleet {
            in_progress: fleet.in_progress,
            limit,
            waiting_for_machine: fleet.waiting_for_machine,
        });
    }

    if let Some(issue_id) = issue_id {
        let focus = thread_focus(db, issue_id, settings.max_open_children_per_thread)
            .await
            .map_err(|error| format!("failed to count the thread's open children: {error}"))?;
        if let Some(focus) = focus {
            let already_open = focus.open_children.iter().any(|(_, id)| id == issue_id);
            let place = own_place(db, execution_id, THREAD_FOCUS_FACT)
                .await
                .map_err(counting)?;
            let ahead = count_ahead(db, THREAD_AHEAD_SQL, execution_id, place, &focus.thread_id)
                .await
                .map_err(counting)?;
            if !already_open && (focus.open_children.len() >= focus.limit || ahead > 0) {
                blocks.push(CapBlock::Thread {
                    thread_name: focus.thread_name,
                    limit: focus.limit,
                    open_children: focus
                        .open_children
                        .into_iter()
                        .map(|(number, _)| number)
                        .collect(),
                });
            }
        }
    }
    Ok(Admission { blocks })
}

// ---------------------------------------------------------------------------
// Place in line
// ---------------------------------------------------------------------------

/// Rewrite every blocking capacity gate's place in line: the fleet's in one
/// order across every database, each thread's within the thread. Oldest held
/// first, by when the execution first held on that cap. A row is written only
/// when its text changes. Returns the executions whose census moved.
pub async fn refresh_places(orch: &Orchestrator) -> Result<Vec<String>, String> {
    struct Blocking {
        db: Arc<LocalDb>,
        gate_id: String,
        execution_id: String,
        fact: &'static str,
        group: Option<(usize, String)>,
        detail: String,
        created_at: i64,
    }
    let mut blocking = Vec::new();
    for (index, db) in orch.db.all_dbs().await.into_iter().enumerate() {
        let gates = db
            .read(|conn| {
                Box::pin(async move {
                    let gates = list_blocking_capacity_gates_conn(conn).await?;
                    let mut threads = HashMap::new();
                    for gate in &gates {
                        if let Some(issue_id) = gate.issue_id.as_deref() {
                            if threads.contains_key(issue_id) {
                                continue;
                            }
                            let thread = crate::storage::query_opt_text_conn(
                                conn,
                                "SELECT parent_thread_id FROM issues WHERE id = ?1",
                                params![issue_id],
                            )
                            .await?;
                            threads.insert(issue_id.to_string(), thread);
                        }
                    }
                    Ok((gates, threads))
                })
            })
            .await
            .map_err(|error| format!("failed to list held capacity gates: {error}"))?;
        let (gates, threads) = gates;
        for gate in gates {
            let Some(fact) = capacity_fact(&gate.predicate_json) else {
                continue;
            };
            let group = gate
                .issue_id
                .as_ref()
                .and_then(|issue_id| threads.get(issue_id).cloned().flatten())
                .map(|thread| (index, thread));
            blocking.push(Blocking {
                db: db.clone(),
                gate_id: gate.gate_id,
                execution_id: gate.execution_id,
                fact,
                group,
                detail: gate.detail.unwrap_or_default(),
                created_at: gate.created_at,
            });
        }
    }
    blocking.sort_by(|a, b| {
        (a.created_at, &a.execution_id, &a.gate_id).cmp(&(
            b.created_at,
            &b.execution_id,
            &b.gate_id,
        ))
    });

    let mut places: HashMap<(&'static str, Option<(usize, String)>), usize> = HashMap::new();
    let mut moved = HashSet::new();
    for gate in &blocking {
        let key = match gate.fact {
            THREAD_FOCUS_FACT => (gate.fact, gate.group.clone()),
            _ => (gate.fact, None),
        };
        let place = places.entry(key).or_insert(0);
        *place += 1;
        let rendered = with_place(without_place(&gate.detail), *place);
        if rendered == gate.detail {
            continue;
        }
        let gate_id = gate.gate_id.clone();
        gate.db
            .write(|conn| {
                let (gate_id, rendered) = (gate_id.clone(), rendered.clone());
                Box::pin(async move { set_gate_detail_conn(conn, &gate_id, &rendered).await })
            })
            .await
            .map_err(|error| format!("failed to rewrite a place in line: {error}"))?;
        if moved.insert(gate.execution_id.clone()) {
            let change = crate::notify::execution_gates_db_change_for_execution(
                &gate.db,
                &gate.execution_id,
                "update",
            )
            .await;
            let _ = orch.services.emitter.emit("db-change", change);
        }
    }
    Ok(moved.into_iter().collect())
}

// ---------------------------------------------------------------------------
// The queue view
// ---------------------------------------------------------------------------

/// One held execution as the queue view lists it.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueuedExecution {
    pub execution_id: String,
    pub project_id: Option<String>,
    pub project_key: Option<String>,
    pub issue_id: Option<String>,
    pub issue_number: Option<i64>,
    pub issue_title: Option<String>,
    pub thread_name: Option<String>,
    pub seq: Option<i64>,
    pub held_since: i64,
    pub census: Option<crate::models::ExecutionGateCensus>,
}

/// One active thread's focus cap, as the queue view lists it.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadFocusView {
    pub project_id: String,
    pub project_key: String,
    pub thread_id: String,
    pub name: String,
    /// The thread's own limit, or `None` when it inherits the workspace's.
    pub max_open_children: Option<i64>,
    pub open_children: usize,
}

/// The whole execution queue: the fleet's load, every held execution in
/// release order, and each active thread's focus cap.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueView {
    pub in_progress: usize,
    pub limit: usize,
    pub waiting_for_machine: usize,
    pub max_open_children_per_thread: usize,
    pub held: Vec<QueuedExecution>,
    pub threads: Vec<ThreadFocusView>,
    /// `capacity_unclassified_yield{reason}` counts, by reason.
    pub unclassified_yields: BTreeMap<String, u64>,
}

const QUEUE_ROWS_SQL: &str = concat!(
    "SELECT e.id, e.issue_id, e.seq, i.number, i.title, p.id, p.key, t.name, ",
    cairn_workflow::held_since_sql!(),
    "
       FROM executions e
       LEFT JOIN issues i ON i.id = e.issue_id
       LEFT JOIN projects p ON p.id = i.project_id
       LEFT JOIN threads t ON t.id = i.parent_thread_id
      WHERE ",
    cairn_db::awaiting_release_sql!()
);

const THREAD_FOCUS_ROWS_SQL: &str = concat!(
    "SELECT t.project_id, p.key, t.id, t.name, t.max_open_children,
            (SELECT COUNT(*) FROM issues i WHERE i.parent_thread_id = t.id AND ",
    cairn_db::open_thread_child_sql!(),
    ")
       FROM threads t JOIN projects p ON p.id = t.project_id
      WHERE t.status = 'active'
      ORDER BY p.key, t.name"
);

/// Every held execution across every database, in release order, with the
/// fleet's load and each active thread's focus cap.
pub async fn queue_view(orch: &Orchestrator) -> Result<QueueView, String> {
    let settings = orch.get_settings();
    let fleet = fleet_load(orch).await?;
    let mut held = Vec::new();
    let mut threads = Vec::new();
    for db in orch.db.all_dbs().await {
        let rows = db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn.query(THREAD_FOCUS_ROWS_SQL, ()).await?;
                    let mut out = Vec::new();
                    while let Some(row) = rows.next().await? {
                        out.push(ThreadFocusView {
                            project_id: row.text(0)?,
                            project_key: row.text(1)?,
                            thread_id: row.text(2)?,
                            name: row.text(3)?,
                            max_open_children: row.opt_i64(4)?,
                            open_children: row.i64(5)?.max(0) as usize,
                        });
                    }
                    Ok(out)
                })
            })
            .await
            .map_err(|error| format!("failed to list threads' focus caps: {error}"))?;
        threads.extend(rows);
    }
    for db in orch.db.all_dbs().await {
        let rows = db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn.query(QUEUE_ROWS_SQL, ()).await?;
                    let mut out = Vec::new();
                    while let Some(row) = rows.next().await? {
                        out.push(QueuedExecution {
                            execution_id: row.text(0)?,
                            issue_id: row.opt_text(1)?,
                            seq: row.opt_i64(2)?,
                            issue_number: row.opt_i64(3)?,
                            issue_title: row.opt_text(4)?,
                            project_id: row.opt_text(5)?,
                            project_key: row.opt_text(6)?,
                            thread_name: row.opt_text(7)?,
                            held_since: row.opt_i64(8)?.unwrap_or(0),
                            census: None,
                        });
                    }
                    drop(rows);
                    let ids: Vec<String> = out.iter().map(|q| q.execution_id.clone()).collect();
                    let mut censuses =
                        super::census::census_for_executions_conn(conn, &ids).await?;
                    for queued in &mut out {
                        queued.census = censuses.remove(&queued.execution_id);
                    }
                    Ok(out)
                })
            })
            .await
            .map_err(|error| format!("failed to list the execution queue: {error}"))?;
        held.extend(rows);
    }
    held.sort_by(|a, b| {
        (a.held_since, a.issue_number, a.seq, &a.execution_id).cmp(&(
            b.held_since,
            b.issue_number,
            b.seq,
            &b.execution_id,
        ))
    });
    Ok(QueueView {
        in_progress: fleet.in_progress,
        limit: settings.max_concurrent_executions.max(1) as usize,
        waiting_for_machine: fleet.waiting_for_machine,
        max_open_children_per_thread: settings.max_open_children_per_thread.max(1) as usize,
        held,
        threads,
        unclassified_yields: unclassified_yield_counts(),
    })
}

/// Set (or clear, with `None`) one thread's own limit on open started
/// children, and tell the queue its focus cap may have moved.
pub async fn set_thread_max_open_children(
    db: &LocalDb,
    project_id: &str,
    thread: &str,
    value: Option<i64>,
) -> Result<(), String> {
    if value.is_some_and(|value| value < 1) {
        return Err("a thread's limit on open children is at least 1".to_string());
    }
    let (project_id, thread) = (project_id.to_string(), thread.to_string());
    let updated = db
        .write(move |conn| {
            let (project_id, thread) = (project_id.clone(), thread.clone());
            Box::pin(async move {
                Ok(conn
                    .execute(
                        "UPDATE threads SET max_open_children = ?3
                          WHERE project_id = ?1 AND (id = ?2 OR name = ?2)",
                        params![project_id.as_str(), thread.as_str(), value],
                    )
                    .await?)
            })
        })
        .await
        .map_err(|error| error.to_string())?;
    if updated == 0 {
        return Err("no such thread in this project".to_string());
    }
    super::sweep::notify_gate_evidence_changed();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::gates::sweep::tests::{
        advanced, count, env, execution, file, now, resolve_issue, Env,
    };
    use crate::execution::gates::sweep::{
        evaluate_execution, release, sweep, sweep_at, OverrideScope, ReleaseCause, ReleaseOutcome,
    };
    use crate::execution::recipe::{LaunchIntent, Starter};
    use crate::mcp::handlers::executions::{patch_execution_gates, GatePatch};
    use cairn_common::protocol::CallbackRequest;
    use serde_json::{json, Value};

    // -- the test world ----------------------------------------------------

    fn caps(env: &Env, fleet: i32, children: i32) {
        env.orch
            .update_settings(crate::models::UpdateSettings {
                max_concurrent_executions: Some(fleet),
                max_open_children_per_thread: Some(children),
                ..Default::default()
            })
            .unwrap();
    }

    async fn add_thread(env: &Env, id: &str, name: &str) {
        env.db
            .execute_script(&format!(
                "INSERT INTO threads(id, project_id, name, created_at, updated_at)
                   VALUES('{id}', 'p', '{name}', 1, 1);"
            ))
            .await
            .unwrap();
    }

    /// An issue with no execution: a backlog child when `thread` is given.
    async fn add_issue(env: &Env, number: i32, thread: Option<&str>) {
        let thread = thread
            .map(|thread| format!("'{thread}'"))
            .unwrap_or_else(|| "NULL".to_string());
        env.db
            .execute_script(&format!(
                "INSERT INTO issues(id, project_id, number, title, status, progress, attention,
                                    parent_thread_id, created_at, updated_at)
                   VALUES('si{number}', 'p', {number}, 'Issue {number}', 'backlog', 'backlog',
                          'none', {thread}, 1, 1);"
            ))
            .await
            .unwrap();
    }

    /// An issue with a started execution whose one job `sj{number}` has
    /// `status`: what the fleet and the thread see as work already under way.
    async fn add_started(env: &Env, number: i32, thread: Option<&str>, status: &str) {
        add_issue(env, number, thread).await;
        env.db
            .execute_script(&format!(
                "UPDATE issues SET status = 'active', progress = 'active' WHERE id = 'si{number}';
                 INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
                   VALUES('se{number}', 'recipe', 'si{number}', 'p', 'running', 1, 1);
                 INSERT INTO jobs(id, execution_id, project_id, issue_id, node_name, status,
                                  created_at, updated_at)
                   VALUES('sj{number}', 'se{number}', 'p', 'si{number}', 'builder', '{status}', 1, 1);"
            ))
            .await
            .unwrap();
    }

    /// Give job `sj{number}` a session, a run, and a current turn in `state`.
    async fn turn(env: &Env, number: i32, state: &str, yield_reason: Option<&str>) {
        let reason = yield_reason
            .map(|reason| format!("'{reason}'"))
            .unwrap_or_else(|| "NULL".to_string());
        env.db
            .execute_script(&format!(
                "INSERT INTO sessions(id, job_id, status, backend_id, created_at, updated_at)
                   VALUES('ss{number}', 'sj{number}', 'open', 'backend', 1, 1);
                 INSERT INTO runs(id, project_id, job_id, issue_id, status, session_id, created_at, updated_at)
                   VALUES('sr{number}', 'p', 'sj{number}', 'si{number}', 'live', 'ss{number}', 1, 1);
                 INSERT INTO turns(id, session_id, run_id, job_id, sequence, state, start_reason,
                                   yield_reason, created_at, updated_at)
                   VALUES('st{number}', 'ss{number}', 'sr{number}', 'sj{number}', 1, '{state}',
                          'initial', {reason}, 1, 1);
                 UPDATE jobs SET current_turn_id = 'st{number}' WHERE id = 'sj{number}';"
            ))
            .await
            .unwrap();
    }

    /// Park job `sj{number}` on a durable wait whose condition is `condition`.
    async fn park_on(env: &Env, number: i32, condition: Value) {
        turn(env, number, "yielded", Some("wait")).await;
        env.db
            .execute_script(&format!(
                "INSERT INTO agent_waits(id, job_id, run_id, session_id, predecessor_turn_id,
                                         tool_use_id, condition_json, state, created_at)
                   VALUES('sw{number}', 'sj{number}', 'sr{number}', 'ss{number}', 'st{number}',
                          'tool', '{condition}', 'pending', 1);"
            ))
            .await
            .unwrap();
    }

    async fn fleet(env: &Env) -> usize {
        fleet_load(&env.orch).await.unwrap().in_progress
    }

    async fn open_children(env: &Env, issue_number: i32) -> Vec<i64> {
        thread_focus(&env.db, &format!("si{issue_number}"), 4)
            .await
            .unwrap()
            .map(|focus| focus.open_children.into_iter().map(|(n, _)| n).collect())
            .unwrap_or_default()
    }

    async fn summary(env: &Env, execution_id: &str) -> String {
        let id = execution_id.to_string();
        env.db
            .read(move |conn| {
                let id = id.clone();
                Box::pin(async move {
                    crate::execution::gates::census::census_for_executions_conn(conn, &[id]).await
                })
            })
            .await
            .unwrap()
            .remove(execution_id)
            .map(|census| census.summary)
            .unwrap_or_default()
    }

    async fn is_held(env: &Env, number: i32) -> bool {
        let (_, status, released_at, jobs) = execution(env, number).await;
        status == "held" && released_at.is_none() && jobs == 0
    }

    async fn jobs_of(env: &Env, execution_id: &str) -> i64 {
        count(
            env,
            &format!("SELECT COUNT(*) FROM jobs WHERE execution_id = '{execution_id}'"),
        )
        .await
    }

    async fn gate_rows(env: &Env, execution_id: &str) -> i64 {
        count(
            env,
            &format!("SELECT COUNT(*) FROM execution_gates WHERE execution_id = '{execution_id}'"),
        )
        .await
    }

    /// Five started executions beside the parent issue's live coordinator: a
    /// full fleet at the default cap of 6.
    async fn full_fleet(env: &Env) {
        for number in 100..105 {
            add_started(env, number, None, "running").await;
        }
        assert_eq!(fleet(env).await, 6);
    }

    fn agent_request() -> CallbackRequest {
        CallbackRequest {
            answer_bound_ms: None,
            cwd: "/".to_string(),
            run_id: None,
            tool: "write".to_string(),
            payload: Value::Null,
            tool_use_id: None,
            thread_id: None,
            origin: None,
        }
    }

    fn operator() -> Value {
        serde_json::to_value(cairn_common::identity::PrincipalRef::Machine {
            device_id: "operator-desk".to_string(),
        })
        .unwrap()
    }

    /// The operator's own start, as the composer's invoke makes it.
    fn operator_start(env: &Env, number: i32) -> String {
        let execution = crate::execution::recipe::start_recipe_execution_impl(
            &env.orch,
            &format!("si{number}"),
            Some("memory-triage"),
            "p",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            crate::models::TriggerType::Manual,
            LaunchIntent::NewAttempt,
            Starter::Operator,
        )
        .unwrap();
        assert_eq!(execution.status, crate::models::ExecutionStatus::Running);
        crate::execution::recipe::create_jobs_for_execution(&env.orch, &execution.id).unwrap();
        execution.id
    }

    // -- one test per acceptance rule ---------------------------------------

    #[tokio::test]
    async fn a_seventh_execution_holds_for_a_fleet_slot_and_releases_once_when_one_completes() {
        let mut env = env("cap-fleet-seventh.db").await;
        full_fleet(&env).await;

        let filed = file(&env, 1, json!([])).await.unwrap();
        assert!(
            filed.contains("HELD: waiting for a fleet slot: 6 of 6 in progress"),
            "the filing names the cap: {filed}"
        );
        assert!(filed.contains("only the operator's Start now"), "{filed}");
        assert!(is_held(&env, 1).await, "held: no jobs, no slot");
        let (id, ..) = execution(&env, 1).await;
        assert_eq!(
            summary(&env, &id).await,
            "held, waiting for a fleet slot: 6 of 6 in progress (1st in line)"
        );
        assert!(
            advanced(&mut env).is_empty(),
            "a held filing never advances"
        );
        assert!(sweep_at(&env.orch, now())
            .await
            .unwrap()
            .released
            .is_empty());

        env.db
            .execute("UPDATE jobs SET status = 'complete' WHERE id = 'sj100'", ())
            .await
            .unwrap();
        let report = sweep_at(&env.orch, now()).await.unwrap();
        assert_eq!(report.released, vec![id.clone()], "released once");
        let jobs = jobs_of(&env, &id).await;
        assert!(jobs > 0, "release minted its jobs");
        assert_eq!(advanced(&mut env), vec![id.clone()]);
        assert_eq!(
            count(
                &env,
                &format!(
                    "SELECT COUNT(*) FROM execution_gates
                      WHERE execution_id = '{id}' AND satisfied_at IS NOT NULL"
                )
            )
            .await,
            1,
            "its fleet hold latched on release"
        );

        assert!(sweep_at(&env.orch, now())
            .await
            .unwrap()
            .released
            .is_empty());
        assert_eq!(jobs_of(&env, &id).await, jobs, "no second set of jobs");
        assert_eq!(fleet(&env).await, 6, "its pending jobs hold the freed slot");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_completion_racing_a_sweep_releases_once() {
        let mut env = env("cap-fleet-race.db").await;
        full_fleet(&env).await;
        file(&env, 1, json!([])).await.unwrap();
        let (id, ..) = execution(&env, 1).await;
        assert!(is_held(&env, 1).await);
        env.db
            .execute("UPDATE jobs SET status = 'complete' WHERE id = 'sj100'", ())
            .await
            .unwrap();

        let mut sweeps = Vec::new();
        for _ in 0..4 {
            let orch = env.orch.clone();
            sweeps.push(tokio::spawn(async move {
                sweep(&orch).await.unwrap().released.len()
            }));
        }
        let mut evaluations = Vec::new();
        for _ in 0..2 {
            let (orch, id) = (env.orch.clone(), id.clone());
            evaluations.push(tokio::spawn(async move {
                usize::from(evaluate_execution(&orch, &id).await.unwrap().released)
            }));
        }
        let mut released = 0;
        for handle in sweeps.into_iter().chain(evaluations) {
            released += handle.await.unwrap();
        }
        assert_eq!(released, 1, "exactly one caller owns the release");
        assert_eq!(
            count(
                &env,
                &format!(
                    "SELECT COUNT(*) - COUNT(DISTINCT recipe_node_id) FROM jobs
                      WHERE execution_id = '{id}'"
                )
            )
            .await,
            0,
            "one job set"
        );
        assert_eq!(advanced(&mut env), vec![id]);
    }

    #[tokio::test]
    async fn waiting_on_the_operator_frees_the_fleet_slot_but_not_the_thread_count() {
        let env = env("cap-operator-waits.db").await;
        add_thread(&env, "th", "harness").await;
        // A PR or artifact awaiting review leaves its job blocked.
        add_started(&env, 100, Some("th"), "blocked").await;
        // An unanswered question on the current turn.
        add_started(&env, 101, Some("th"), "running").await;
        turn(&env, 101, "yielded", Some("user_input")).await;
        env.db
            .execute_script(
                "INSERT INTO prompts(id, run_id, job_id, turn_id, questions, uri_segment, created_at)
                   VALUES('q101', 'sr101', 'sj101', 'st101', '[]', 'q-1', 1);",
            )
            .await
            .unwrap();
        // A pending permission, on a turn still marked running.
        add_started(&env, 102, Some("th"), "running").await;
        turn(&env, 102, "running", None).await;
        env.db
            .execute_script(
                "INSERT INTO permission_requests(id, run_id, job_id, turn_id, tool_use_id, tool_name,
                                                 tool_input, status, uri_segment, created_at)
                   VALUES('pr102', 'sr102', 'sj102', 'st102', 'tu102', 'Bash', '{}', 'pending',
                          'p-1', 1);",
            )
            .await
            .unwrap();
        add_started(&env, 103, None, "running").await;
        // A planner blocked on plan approval, its builder pending behind it:
        // the whole execution waits on the operator.
        add_started(&env, 106, None, "blocked").await;
        env.db
            .execute_script(
                "INSERT INTO jobs(id, execution_id, project_id, issue_id, node_name, status,
                                  created_at, updated_at)
                   VALUES('sj106b', 'se106', 'p', 'si106', 'builder', 'pending', 1, 1);",
            )
            .await
            .unwrap();

        // A planner asking through a prompt row that names only its run, its
        // builder pending behind it.
        add_started(&env, 107, None, "running").await;
        turn(&env, 107, "yielded", Some("user_input")).await;
        env.db
            .execute_script(
                "INSERT INTO prompts(id, run_id, turn_id, questions, uri_segment, created_at)
                   VALUES('q107', 'sr107', 'st107', '[]', 'q-1', 1);
                 INSERT INTO jobs(id, execution_id, project_id, issue_id, node_name, status,
                                  created_at, updated_at)
                   VALUES('sj107b', 'se107', 'p', 'si107', 'builder', 'pending', 1, 1);",
            )
            .await
            .unwrap();

        assert_eq!(
            fleet(&env).await,
            2,
            "only the coordinator and 103 use a machine; review, a question, a permission, and a \
             builder parked behind a planner waiting on the operator do not"
        );
        add_issue(&env, 104, Some("th")).await;
        assert_eq!(
            open_children(&env, 104).await,
            vec![100, 101, 102],
            "each still counts as an open child of its thread"
        );
    }

    #[tokio::test]
    async fn a_fifth_started_child_holds_naming_the_four_and_releases_once_on_merge() {
        let mut env = env("cap-thread-fifth.db").await;
        caps(&env, 50, 4);
        add_thread(&env, "th", "harness").await;
        for number in 100..104 {
            add_started(&env, number, Some("th"), "running").await;
        }
        // Neither a backlog child, a sub-agent task, nor a grandchild counts.
        add_issue(&env, 104, Some("th")).await;
        env.db
            .execute_script(
                "INSERT INTO jobs(id, execution_id, project_id, issue_id, node_name, status,
                                  parent_job_id, created_at, updated_at)
                   VALUES('task100', 'se100', 'p', 'si100', 'task', 'running', 'sj100', 1, 1);",
            )
            .await
            .unwrap();
        add_started(&env, 107, None, "running").await;
        env.db
            .execute(
                "UPDATE issues SET parent_issue_id = 'si100' WHERE id = 'si107'",
                (),
            )
            .await
            .unwrap();
        add_issue(&env, 105, Some("th")).await;

        let filed = file(&env, 105, json!([])).await.unwrap();
        assert!(
            filed.contains("HELD: harness has 4 open children: 100, 101, 102, 103"),
            "the thread reason names the four: {filed}"
        );
        let (id, ..) = execution(&env, 105).await;
        assert_eq!(
            summary(&env, &id).await,
            "held, harness has 4 open children: 100, 101, 102, 103 (1st in line)"
        );

        resolve_issue(&env, 100, "merged").await;
        let report = sweep_at(&env.orch, now()).await.unwrap();
        assert_eq!(
            report.released,
            vec![id.clone()],
            "released once on the merge"
        );
        assert_eq!(advanced(&mut env), vec![id]);
        assert!(sweep_at(&env.orch, now())
            .await
            .unwrap()
            .released
            .is_empty());
    }

    #[tokio::test]
    async fn an_operator_start_over_both_caps_starts_counts_and_is_never_held() {
        let env = env("cap-operator-start.db").await;
        add_thread(&env, "th", "harness").await;
        for number in 100..104 {
            add_started(&env, number, Some("th"), "running").await;
        }
        add_started(&env, 104, None, "running").await;
        assert_eq!(fleet(&env).await, 6);
        add_issue(&env, 105, Some("th")).await;

        let id = operator_start(&env, 105);
        assert_eq!(gate_rows(&env, &id).await, 0, "never held");
        assert_eq!(fleet(&env).await, 7, "it counts toward the fleet");
        assert_eq!(
            open_children(&env, 105).await,
            vec![100, 101, 102, 103, 105],
            "and toward its thread"
        );

        // A capped start behind it now holds on both.
        add_issue(&env, 106, Some("th")).await;
        file(&env, 106, json!([])).await.unwrap();
        let (held, ..) = execution(&env, 106).await;
        assert_eq!(
            summary(&env, &held).await,
            "held, waiting for a fleet slot: 7 of 6 in progress (1st in line); harness has 5 open \
             children: 100, 101, 102, 103, 105 (1st in line)",
            "both reasons show"
        );

        // Later sweeps and a lowered cap never re-hold it.
        sweep_at(&env.orch, now()).await.unwrap();
        caps(&env, 1, 1);
        sweep_at(&env.orch, now()).await.unwrap();
        assert_eq!(gate_rows(&env, &id).await, 0);
        let (_, status, ..) = execution(&env, 105).await;
        assert_ne!(status, "held");
    }

    #[tokio::test]
    async fn answering_a_question_or_granting_a_permission_resumes_without_a_hold() {
        let env = env("cap-answer-resumes.db").await;
        full_fleet(&env).await;
        turn(&env, 103, "yielded", Some("user_input")).await;
        env.db
            .execute_script(
                "INSERT INTO prompts(id, run_id, job_id, turn_id, questions, uri_segment, created_at)
                   VALUES('q103', 'sr103', 'sj103', 'st103', '[]', 'q-1', 1);",
            )
            .await
            .unwrap();
        turn(&env, 104, "yielded", Some("permission")).await;
        env.db
            .execute_script(
                "INSERT INTO permission_requests(id, run_id, job_id, turn_id, tool_use_id, tool_name,
                                                 tool_input, status, uri_segment, created_at)
                   VALUES('pr104', 'sr104', 'sj104', 'st104', 'tu104', 'Bash', '{}', 'pending',
                          'p-1', 1);",
            )
            .await
            .unwrap();
        assert_eq!(fleet(&env).await, 4, "both wait on the operator");
        // Two capped starts fill the slots they freed.
        add_issue(&env, 1001, None).await;
        add_issue(&env, 1002, None).await;
        for number in [1001, 1002] {
            file(&env, number, json!([])).await.unwrap();
            assert!(!is_held(&env, number).await);
        }
        assert_eq!(fleet(&env).await, 6);

        // The operator answers and grants: the turns resume, taking the fleet
        // over its cap. Neither is a start, so neither queues.
        env.db
            .execute_script(
                "UPDATE prompts SET response = 'yes', answered_at = 2 WHERE id = 'q103';
                 UPDATE permission_requests SET status = 'approved', responded_at = 2 WHERE id = 'pr104';
                 UPDATE turns SET state = 'running', yield_reason = NULL WHERE id IN ('st103', 'st104');",
            )
            .await
            .unwrap();
        assert_eq!(fleet(&env).await, 8);
        sweep_at(&env.orch, now()).await.unwrap();
        for number in [103, 104] {
            assert_eq!(gate_rows(&env, &format!("se{number}")).await, 0);
            assert_eq!(
                count(
                    &env,
                    &format!(
                        "SELECT COUNT(*) FROM jobs WHERE id = 'sj{number}' AND status = 'running'"
                    )
                )
                .await,
                1,
                "its job resumed"
            );
        }
    }

    #[tokio::test]
    async fn a_reparented_child_leaves_the_old_threads_count_and_joins_the_new_one() {
        let mut env = env("cap-reparent.db").await;
        caps(&env, 50, 4);
        add_thread(&env, "th", "harness").await;
        add_thread(&env, "tx", "execution").await;
        for number in 100..104 {
            add_started(&env, number, Some("th"), "running").await;
        }
        for number in 110..113 {
            add_started(&env, number, Some("tx"), "running").await;
        }
        add_issue(&env, 105, Some("th")).await;
        file(&env, 105, json!([])).await.unwrap();
        assert!(is_held(&env, 105).await);
        let (id, ..) = execution(&env, 105).await;

        env.db
            .execute(
                "UPDATE issues SET parent_thread_id = 'tx' WHERE id = 'si103'",
                (),
            )
            .await
            .unwrap();
        let report = sweep_at(&env.orch, now()).await.unwrap();
        assert_eq!(
            report.released,
            vec![id.clone()],
            "the old thread is under its cap"
        );
        assert_eq!(advanced(&mut env), vec![id]);

        add_issue(&env, 113, Some("tx")).await;
        let filed = file(&env, 113, json!([])).await.unwrap();
        assert!(
            filed.contains("execution has 4 open children: 103, 110, 111, 112"),
            "the new thread counts it: {filed}"
        );
    }

    #[tokio::test]
    async fn lowering_a_cap_below_the_count_stops_nothing() {
        let env = env("cap-lowered.db").await;
        add_thread(&env, "th", "harness").await;
        for number in 100..104 {
            add_started(&env, number, Some("th"), "running").await;
        }
        add_started(&env, 104, None, "running").await;
        caps(&env, 1, 1);
        sweep_at(&env.orch, now()).await.unwrap();
        assert_eq!(
            count(
                &env,
                "SELECT COUNT(*) FROM jobs WHERE id LIKE 'sj%' AND status = 'running'"
            )
            .await,
            5,
            "every running job keeps running"
        );
        assert_eq!(
            count(&env, "SELECT COUNT(*) FROM execution_gates").await,
            0,
            "nothing running gains a hold"
        );
        assert_eq!(
            count(
                &env,
                "SELECT COUNT(*) FROM executions WHERE status = 'held'"
            )
            .await,
            0
        );
    }

    #[tokio::test]
    async fn start_now_on_a_held_execution_starts_it_and_records_the_operator() {
        let mut env = env("cap-start-now.db").await;
        full_fleet(&env).await;
        file(&env, 1, json!([])).await.unwrap();
        let (id, ..) = execution(&env, 1).await;

        // An agent's early release skips no cap.
        let reply = patch_execution_gates(
            &env.orch,
            &agent_request(),
            "prj",
            1,
            1,
            GatePatch::Release,
            false,
        )
        .await
        .unwrap();
        assert!(
            reply.contains("still held by the execution queue: waiting for a fleet slot"),
            "{reply}"
        );
        assert!(
            reply.contains("only the operator's Start now skips a cap"),
            "{reply}"
        );
        assert!(is_held(&env, 1).await);
        assert!(advanced(&mut env).is_empty());

        let outcome = release(
            &env.orch,
            &id,
            ReleaseCause::Override {
                principal: operator(),
                scope: OverrideScope::Everything,
            },
        )
        .await
        .unwrap();
        assert_eq!(outcome, ReleaseOutcome::Released);
        assert!(jobs_of(&env, &id).await > 0);
        assert_eq!(advanced(&mut env), vec![id.clone()]);
        let recorded: Value = serde_json::from_str(
            &env.db
                .query_opt_text(
                    "SELECT release_override_json FROM executions WHERE id = ?1",
                    (id.clone(),),
                )
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(recorded["operator"], json!(true));
        assert_eq!(recorded["skipped"], json!(["fleet"]));
        assert_eq!(
            summary(&env, &id).await,
            "started now by the operator (machine operator-desk), skipping the fleet cap"
        );
    }

    #[tokio::test]
    async fn oldest_held_first() {
        let env = env("cap-oldest-first.db").await;
        full_fleet(&env).await;
        add_issue(&env, 1003, None).await;
        add_issue(&env, 1002, None).await;
        file(&env, 1003, json!([])).await.unwrap();
        file(&env, 1002, json!([])).await.unwrap();
        let (older, ..) = execution(&env, 1003).await;
        let (younger, ..) = execution(&env, 1002).await;
        // 1003 has waited longer, though its number is higher.
        env.db
            .execute(
                "UPDATE execution_gates SET created_at = created_at - 100 WHERE execution_id = ?1",
                (older.clone(),),
            )
            .await
            .unwrap();
        crate::execution::gates::sweep::sweep_at(&env.orch, now())
            .await
            .unwrap();
        assert!(summary(&env, &older).await.ends_with("(1st in line)"));
        assert!(summary(&env, &younger).await.ends_with("(2nd in line)"));

        env.db
            .execute("UPDATE jobs SET status = 'complete' WHERE id = 'sj100'", ())
            .await
            .unwrap();
        let report = sweep_at(&env.orch, now()).await.unwrap();
        assert_eq!(
            report.released,
            vec![older],
            "one slot, the oldest takes it"
        );
        assert!(summary(&env, &younger).await.ends_with("(1st in line)"));
    }

    /// An execution that waited on its `after:` gates takes its place in the
    /// queue when they clear, behind what is already held, however long ago
    /// it was filed; and the sweep releases in the order the places say.
    #[tokio::test]
    async fn an_authored_wait_that_clears_queues_behind_what_is_already_held() {
        let mut env = env("cap-authored-clears-behind.db").await;
        full_fleet(&env).await;
        let merged_1 = json!([{"fact": "issue_resolution", "issue": "cairn://p/prj/1", "resolution": "merged"}]);
        file(&env, 2, merged_1).await.unwrap();
        let (waited, ..) = execution(&env, 2).await;
        env.db
            .execute(
                "UPDATE execution_gates SET created_at = created_at - 100000 WHERE execution_id = ?1",
                (waited.clone(),),
            )
            .await
            .unwrap();
        file(&env, 3, json!([])).await.unwrap();
        let (queued, ..) = execution(&env, 3).await;
        env.db
            .execute(
                "UPDATE execution_gates SET created_at = created_at - 50 WHERE execution_id = ?1",
                (queued.clone(),),
            )
            .await
            .unwrap();

        resolve_issue(&env, 1, "merged").await;
        assert!(sweep_at(&env.orch, now())
            .await
            .unwrap()
            .released
            .is_empty());
        assert!(summary(&env, &queued).await.ends_with("(1st in line)"));
        assert!(
            summary(&env, &waited).await.ends_with("(2nd in line)"),
            "{}",
            summary(&env, &waited).await
        );

        env.db
            .execute("UPDATE jobs SET status = 'complete' WHERE id = 'sj100'", ())
            .await
            .unwrap();
        let report = sweep_at(&env.orch, now()).await.unwrap();
        assert_eq!(
            report.released,
            vec![queued.clone()],
            "the place in line is the release order"
        );
        assert_eq!(advanced(&mut env), vec![queued]);
        assert!(summary(&env, &waited).await.ends_with("(1st in line)"));
    }

    /// A cap-held execution that an agent patches onto a new `after:` gate
    /// waits on that gate, not in the queue: it holds no place and is ahead of
    /// nobody, so a freed slot goes to a fresh start instead of stalling the
    /// fleet behind a gate that may never clear.
    #[tokio::test]
    async fn a_patched_in_wait_gives_up_its_place_and_blocks_no_one() {
        let env = env("cap-patched-wait.db").await;
        full_fleet(&env).await;
        file(&env, 1, json!([])).await.unwrap();
        let (patched, ..) = execution(&env, 1).await;
        env.db
            .execute(
                "UPDATE execution_gates SET created_at = created_at - 100 WHERE execution_id = ?1",
                (patched.clone(),),
            )
            .await
            .unwrap();
        let after = super::super::authoring::parse_after(
            Some(&json!([{"fact": "issue_resolution", "issue": "cairn://p/prj/2", "resolution": "merged"}])),
            "payload.after",
        )
        .unwrap();
        patch_execution_gates(
            &env.orch,
            &agent_request(),
            "prj",
            1,
            1,
            GatePatch::Replace(after),
            false,
        )
        .await
        .unwrap();
        let waiting = summary(&env, &patched).await;
        assert!(
            waiting.contains("waiting on issue cairn://p/prj/2 merged")
                && !waiting.contains("in line"),
            "waits on its gate, with no place in line: {waiting}"
        );

        env.db
            .execute("UPDATE jobs SET status = 'complete' WHERE id = 'sj100'", ())
            .await
            .unwrap();
        add_issue(&env, 1002, None).await;
        let filed = file(&env, 1002, json!([])).await.unwrap();
        assert!(
            filed.starts_with("Started execution"),
            "nothing stalls the fleet: {filed}"
        );
        sweep_at(&env.orch, now()).await.unwrap();
        assert!(!summary(&env, &patched).await.contains("in line"));
    }

    /// A slot that frees between sweeps goes to the oldest held execution,
    /// not to a filing that lands first.
    #[tokio::test]
    async fn a_fresh_start_does_not_jump_the_fleet_queue() {
        let env = env("cap-fresh-fleet.db").await;
        full_fleet(&env).await;
        file(&env, 1, json!([])).await.unwrap();
        let (older, ..) = execution(&env, 1).await;
        env.db
            .execute(
                "UPDATE execution_gates SET created_at = created_at - 100 WHERE execution_id = ?1",
                (older.clone(),),
            )
            .await
            .unwrap();
        env.db
            .execute("UPDATE jobs SET status = 'complete' WHERE id = 'sj100'", ())
            .await
            .unwrap();

        add_issue(&env, 1002, None).await;
        let filed = file(&env, 1002, json!([])).await.unwrap();
        assert!(
            filed.contains("HELD: waiting for a fleet slot: 5 of 6 in progress"),
            "a free slot is the oldest held execution's: {filed}"
        );
        let report = sweep_at(&env.orch, now()).await.unwrap();
        assert_eq!(report.released, vec![older]);
        assert!(is_held(&env, 1002).await);
    }

    #[tokio::test]
    async fn a_fresh_child_does_not_jump_its_threads_queue() {
        let env = env("cap-fresh-thread.db").await;
        caps(&env, 50, 4);
        add_thread(&env, "th", "harness").await;
        for number in 100..104 {
            add_started(&env, number, Some("th"), "running").await;
        }
        add_issue(&env, 105, Some("th")).await;
        add_issue(&env, 106, Some("th")).await;
        file(&env, 105, json!([])).await.unwrap();
        let (older, ..) = execution(&env, 105).await;
        env.db
            .execute(
                "UPDATE execution_gates SET created_at = created_at - 100 WHERE execution_id = ?1",
                (older.clone(),),
            )
            .await
            .unwrap();
        resolve_issue(&env, 100, "merged").await;

        let filed = file(&env, 106, json!([])).await.unwrap();
        assert!(
            filed.contains("HELD: harness has 3 open children: 101, 102, 103"),
            "{filed}"
        );
        let report = sweep_at(&env.orch, now()).await.unwrap();
        assert_eq!(report.released, vec![older]);
        assert!(is_held(&env, 106).await);
    }

    #[tokio::test]
    async fn a_thread_held_execution_does_not_block_another_threads_start() {
        let env = env("cap-thread-blocks-only-itself.db").await;
        add_thread(&env, "th", "harness").await;
        for number in 100..104 {
            add_started(&env, number, Some("th"), "running").await;
        }
        add_started(&env, 104, None, "running").await;
        add_issue(&env, 105, Some("th")).await;
        add_issue(&env, 1002, None).await;
        file(&env, 105, json!([])).await.unwrap();
        file(&env, 1002, json!([])).await.unwrap();
        let (thread_held, ..) = execution(&env, 105).await;
        let (fleet_held, ..) = execution(&env, 1002).await;
        env.db
            .execute(
                "UPDATE execution_gates SET created_at = created_at - 100 WHERE execution_id = ?1",
                (thread_held.clone(),),
            )
            .await
            .unwrap();

        env.db
            .execute("UPDATE jobs SET status = 'complete' WHERE id = 'sj104'", ())
            .await
            .unwrap();
        let report = sweep_at(&env.orch, now()).await.unwrap();
        assert_eq!(
            report.released,
            vec![fleet_held],
            "the older execution is still held by its own thread"
        );
        assert_eq!(
            summary(&env, &thread_held).await,
            "held, harness has 4 open children: 100, 101, 102, 103 (1st in line)"
        );
    }

    #[tokio::test]
    async fn coordinators_parked_on_their_children_do_not_deadlock_the_fleet() {
        let mut env = env("cap-coordinators.db").await;
        env.db
            .execute("UPDATE jobs SET status = 'idle' WHERE id = 'coord'", ())
            .await
            .unwrap();
        add_thread(&env, "th", "harness").await;
        for number in 100..106 {
            add_started(&env, number, Some("th"), "running").await;
        }
        assert_eq!(fleet(&env).await, 6);
        // Each coordinator files a child while it works: all six hold.
        let mut children = Vec::new();
        for number in 200..206 {
            add_issue(&env, number, None).await;
            env.db
                .execute(
                    "UPDATE issues SET parent_issue_id = ?1 WHERE id = ?2",
                    (format!("si{}", number - 100), format!("si{number}")),
                )
                .await
                .unwrap();
            file(&env, number, json!([])).await.unwrap();
            assert!(is_held(&env, number).await);
            children.push(execution(&env, number).await.0);
        }
        add_started(&env, 300, None, "idle").await;

        // Then each parks on its children: a waitFor duration, or checks on
        // another issue's node.
        for number in 100..103 {
            park_on(&env, number, json!({"kind": "duration"})).await;
        }
        for number in 103..106 {
            park_on(
                &env,
                number,
                json!({"kind": "checks", "uri": "cairn://p/prj/300/1/builder", "job_id": "sj300"}),
            )
            .await;
        }
        assert_eq!(fleet(&env).await, 0, "no parked coordinator holds a slot");

        let mut report = sweep_at(&env.orch, now()).await.unwrap();
        report.released.sort();
        children.sort();
        assert_eq!(report.released, children, "every child starts: no deadlock");
        assert_eq!(advanced(&mut env).len(), 6);
        add_issue(&env, 106, Some("th")).await;
        assert_eq!(
            open_children(&env, 106).await,
            vec![100, 101, 102, 103, 104, 105],
            "each parked coordinator is still one open child of its thread"
        );
    }

    #[tokio::test]
    async fn machine_waits_still_hold_a_fleet_slot() {
        let env = env("cap-machine-waits.db").await;
        env.db
            .execute("UPDATE jobs SET status = 'idle' WHERE id = 'coord'", ())
            .await
            .unwrap();
        for number in 100..106 {
            add_started(&env, number, None, "running").await;
        }
        park_on(
            &env,
            100,
            json!({"kind": "terminal", "uri": "u", "slug": "dev", "on": "exit"}),
        )
        .await;
        park_on(
            &env,
            101,
            json!({"kind": "run_batch", "request_id": "req", "commits": false}),
        )
        .await;
        park_on(
            &env,
            102,
            json!({"kind": "checks", "uri": "cairn://p/prj/102/1/builder", "job_id": "sj102"}),
        )
        .await;
        // A parent parked on its delegated task counts once, through the task.
        turn(&env, 103, "yielded", Some("dependency_wait")).await;
        env.db
            .execute_script(
                "INSERT INTO jobs(id, execution_id, project_id, issue_id, node_name, status,
                                  parent_job_id, created_at, updated_at)
                   VALUES('task103', 'se103', 'p', 'si103', 'task', 'running', 'sj103', 1, 1);",
            )
            .await
            .unwrap();
        // With no running task, a dependency wait uses no machine.
        turn(&env, 104, "yielded", Some("dependency_wait")).await;
        // A thread's host execution never counts, its task jobs included.
        add_thread(&env, "tz", "host").await;
        env.db
            .execute("UPDATE jobs SET thread_id = 'tz' WHERE id = 'sj105'", ())
            .await
            .unwrap();

        assert_eq!(
            fleet(&env).await,
            4,
            "a terminal, a parked run batch, checks on its own node, and a delegated task"
        );
    }

    #[tokio::test]
    async fn rerunning_an_open_child_is_not_a_new_child() {
        let env = env("cap-rerun-open-child.db").await;
        caps(&env, 50, 4);
        add_thread(&env, "th", "harness").await;
        for number in 100..104 {
            add_started(&env, number, Some("th"), "running").await;
        }
        let filed = file(&env, 103, json!([])).await.unwrap();
        assert!(filed.starts_with("Started execution"), "{filed}");
    }

    #[tokio::test]
    async fn a_cap_held_execution_is_not_a_notification() {
        let env = env("cap-not-a-notification.db").await;
        full_fleet(&env).await;
        file(&env, 1, json!([])).await.unwrap();
        assert!(is_held(&env, 1).await);
        let (id, ..) = execution(&env, 1).await;
        let entries = crate::notifications::list_operator_notifications(
            &env.orch,
            crate::notifications::NotificationView::All,
            Some("p"),
        )
        .await
        .unwrap();
        let rendered = format!("{entries:?}");
        assert!(
            !rendered.contains(&id) && !rendered.contains("fleet slot"),
            "a held execution starts on its own, so it waits on nobody: {rendered}"
        );
    }

    // -- guards --------------------------------------------------------------

    #[test]
    fn the_capacity_facts_are_the_ones_the_sql_names() {
        let sql = cairn_db::capacity_gate_sql!();
        for fact in CAPACITY_FACTS {
            assert!(
                sql.contains(&format!("'{fact}'")),
                "{fact} missing from {sql}"
            );
            assert!(
                cairn_db::awaits_authored_gate_sql!().contains(&format!("'{fact}'")),
                "{fact} missing from awaits_authored_gate_sql!"
            );
        }
        assert_eq!(sql.matches('\'').count(), 2 + CAPACITY_FACTS.len() * 2);
        assert_eq!(
            capacity_fact(&capacity_predicate(FLEET_SLOT_FACT)),
            Some(FLEET_SLOT_FACT)
        );
        assert_eq!(capacity_fact(r#"{"fact":"clock"}"#), None);
    }

    #[test]
    fn parse_after_refuses_the_capacity_facts() {
        for fact in CAPACITY_FACTS {
            let error = super::super::authoring::parse_after(
                Some(&json!([{ "fact": fact }])),
                "payload.after",
            )
            .unwrap_err();
            assert!(error.contains("system-filed"), "{error}");
            assert!(error.contains("Start now"), "{error}");
        }
    }

    /// Adding a `TurnYieldReason` variant fails to compile in
    /// [`job_machine_use`] until someone decides whether it uses a machine;
    /// this asserts every variant that exists today has a decision.
    #[test]
    fn every_yield_reason_is_classified() {
        use TurnYieldReason::*;
        let all = [
            UserInput,
            Permission,
            DependencyWait,
            Wait,
            SafetyPause,
            ProviderLimit,
        ];
        for reason in all {
            let wait = ParkedWait {
                kind: Some("duration".to_string()),
                ..ParkedWait::default()
            };
            assert!(
                job_machine_use(Some("yielded"), Some(&reason.to_string()), &wait).is_ok(),
                "{reason} is unclassified"
            );
        }
        for kind in [
            "duration",
            "checks",
            "terminal",
            "run_batch",
            "mcp_continuation",
        ] {
            let wait = ParkedWait {
                kind: Some(kind.to_string()),
                ..ParkedWait::default()
            };
            assert!(
                job_machine_use(Some("yielded"), Some("wait"), &wait).is_ok(),
                "wait:{kind}"
            );
        }
        assert!(job_machine_use(
            Some("yielded"),
            Some("wait"),
            &ParkedWait {
                kind: Some("something_new".to_string()),
                ..ParkedWait::default()
            }
        )
        .is_err());
        assert_eq!(
            job_machine_use(
                Some("yielded"),
                Some("wait"),
                &ParkedWait {
                    kind: Some("duration".to_string()),
                    any_machine_wait: true,
                    ..ParkedWait::default()
                }
            ),
            Ok(MachineUse::Uses),
            "a newer duration wait does not hide a parked run batch beside it"
        );
        assert!(job_machine_use(
            Some("yielded"),
            Some("a_new_reason"),
            &ParkedWait::default()
        )
        .is_err());
        assert_eq!(
            job_machine_use(Some("running"), None, &ParkedWait::default()),
            Ok(MachineUse::Uses)
        );
    }

    /// The caps gate starts only. Placement and per-machine admission must
    /// never read them, or the fleet cap would quietly become a load bound
    /// (tenets 1309, 290).
    #[test]
    fn placement_and_admission_never_read_the_caps() {
        let sources = [
            (
                "fleet/placement.rs",
                include_str!("../../fleet/placement.rs"),
            ),
            ("fleet/queue.rs", include_str!("../../fleet/queue.rs")),
            (
                "execution/jobs/ordinary_admission.rs",
                include_str!("../jobs/ordinary_admission.rs"),
            ),
        ];
        for (path, source) in sources {
            for needle in [
                FLEET_SLOT_FACT,
                THREAD_FOCUS_FACT,
                "max_concurrent_executions",
                "maxConcurrentExecutions",
                "max_open_children",
                "maxOpenChildrenPerThread",
                "gates::capacity",
            ] {
                assert!(!source.contains(needle), "{path} reads `{needle}`");
            }
        }
    }

    #[test]
    fn reasons_and_places_read_as_the_operator_sees_them() {
        let fleet = CapBlock::Fleet {
            in_progress: 6,
            limit: 6,
            waiting_for_machine: 2,
        };
        assert_eq!(
            with_place(&fleet.reason(), 2),
            "waiting for a fleet slot: 6 of 6 in progress, 2 of them waiting for a machine (2nd in line)"
        );
        let thread = CapBlock::Thread {
            thread_name: "harness".to_string(),
            limit: 4,
            open_children: vec![5783, 5790, 5791, 5795],
        };
        assert_eq!(
            with_place(&thread.reason(), 1),
            "harness has 4 open children: 5783, 5790, 5791, 5795 (1st in line)"
        );
        assert_eq!(ordinal(11), "11th");
        assert_eq!(ordinal(23), "23rd");
        let placed = with_place("reason", 3);
        assert_eq!(
            carry_place(Some(&placed), "new reason"),
            "new reason (3rd in line)"
        );
        assert_eq!(without_place(&placed), "reason");
    }
}
