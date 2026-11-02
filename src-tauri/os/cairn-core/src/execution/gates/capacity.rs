//! The execution queue: two capacity caps that decide whether a new execution
//! starts now or holds (cairn/5802, cairn/5830; tenets 1288 and 1355).
//!
//! ## One queue, not a second scheduler
//!
//! A cap hold is a GATE ROW of a second kind beside the authored `after:`
//! gates: `{"fact":"fleet_slot"}` or `{"fact":"thread_focus"}`, with its facts
//! (a [`CapacityHold`], place in line included) as JSON in
//! `execution_gates.detail`. Those facts become words in one place, the gate
//! census, so every surface reads the same `Queued · 3rd · 39/30 running`.
//! The sweep numbers the places; a filing never does, so a just-filed hold
//! reads `Queued · 39/30 running` until the next pass.
//!
//! Being a gate row, a cap-held execution is held by the one definition every surface already reads
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
//!   stale. It counts the thread's direct child issues, not merged or closed,
//!   that have live work or an open PR (`cairn_db::open_thread_child_sql!`):
//!   a child whose PR waits for review counts, and one whose executions all
//!   ended with no PR does not. The limit is the workspace's
//!   `maxOpenChildrenPerThread`; a thread has no limit of its own.
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
//! the [`AdmissionLock`], decides, and writes before letting go, so two
//! admissions never interleave. Outside the lock a count can only fall (a
//! completion) or rise through the operator's own starts, which are allowed
//! over the cap, so a stale count is only ever conservative. Exactly-once
//! still rests on the guarded `released_at` update.
//!
//! The lock is cheap to hold and never waited on forever. A decision is one
//! read of the queue ([`QueueSnapshot`]) and a pure function over it, so a
//! sweep pass decides every candidate under ONE acquisition, oldest first,
//! and writes only what changed. Display values (counts and places in line)
//! are rewritten after the pass, outside the lock. An acquisition that waits
//! past [`ADMISSION_WAIT_BOUND`] names the holder and fails retryably.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, PoisonError};
use std::time::{Duration, Instant};

use cairn_db::turso::params;
use cairn_workflow::gates::store::{
    list_blocking_capacity_gates_conn, set_gate_detail_conn, CAPACITY_GATE_SEQ_BASE,
};
use tracing::Instrument;

use crate::models::{CapacityHold, TurnYieldReason};
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

/// Which gate row a [`CapacityHold`] is, and how an operator start names it.
pub trait CapacityHoldExt {
    /// The capacity fact of its gate row.
    fn fact(&self) -> &'static str;
    /// How an operator start records skipping this cap.
    fn skipped_label(&self) -> &'static str;
    /// Its gate row's seq: the fleet's sorts before the thread's.
    fn seq(&self) -> i64;
}

impl CapacityHoldExt for CapacityHold {
    fn fact(&self) -> &'static str {
        match self {
            Self::Fleet { .. } => FLEET_SLOT_FACT,
            Self::Thread { .. } => THREAD_FOCUS_FACT,
        }
    }

    fn skipped_label(&self) -> &'static str {
        match self {
            Self::Fleet { .. } => "fleet",
            Self::Thread { .. } => "thread",
        }
    }

    fn seq(&self) -> i64 {
        match self {
            Self::Fleet { .. } => CAPACITY_GATE_SEQ_BASE,
            Self::Thread { .. } => CAPACITY_GATE_SEQ_BASE + 1,
        }
    }
}

/// A hold as `execution_gates.detail` stores it.
pub fn encode_hold(hold: &CapacityHold) -> String {
    // Infallible for this type: strings and integers only, no map keys.
    serde_json::to_string(hold).unwrap_or_default()
}

/// The hold a capacity row of `fact` stores, or `None` when its `detail`
/// carries no facts for that cap (cleared, or written before facts were).
pub fn decode_hold(fact: &str, detail: &str) -> Option<CapacityHold> {
    let hold: CapacityHold = serde_json::from_str(detail).ok()?;
    (hold.fact() == fact).then_some(hold)
}

/// The sentence an agent-facing reply adds after a queued line. An agent needs
/// to know that no write of its own skips the queue; the operator's surfaces
/// never carry it.
pub const ONLY_START_NOW_SKIPS_THE_QUEUE: &str = "Only the operator's Start now skips the queue.";

/// Every cap holding one execution, as decided under the admission lock. Each
/// hold's `place` is `None`: the queue numbers it once it is stored.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Admission {
    pub blocks: Vec<CapacityHold>,
}

impl Admission {
    pub fn is_clear(&self) -> bool {
        self.blocks.is_empty()
    }
}

// ---------------------------------------------------------------------------
// The admission lock
// ---------------------------------------------------------------------------

/// Where an admission-lock acquisition comes from: what its timing, a
/// timed-out wait, and the holder slot name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AdmissionSite {
    /// A capped filing deciding its caps before its insert.
    Filing,
    /// A sweep pass's release phase: every candidate, one acquisition.
    SweepPass,
    /// One execution's release: Start now, a creation-time evaluation, an
    /// early release.
    Release,
    /// A gate patch removing the last authored gate, which releases.
    GatePatch,
}

impl AdmissionSite {
    const ALL: [AdmissionSite; 4] = [
        AdmissionSite::Filing,
        AdmissionSite::SweepPass,
        AdmissionSite::Release,
        AdmissionSite::GatePatch,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            AdmissionSite::Filing => "filing",
            AdmissionSite::SweepPass => "sweep_pass",
            AdmissionSite::Release => "release",
            AdmissionSite::GatePatch => "gate_patch",
        }
    }

    fn index(self) -> usize {
        Self::ALL
            .iter()
            .position(|site| *site == self)
            .unwrap_or_default()
    }

    #[cfg(test)]
    pub(crate) const ALL_FOR_TESTS: [AdmissionSite; 4] = Self::ALL;
}

impl std::fmt::Display for AdmissionSite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// How long an acquisition waits before it gives up, names the holder, and
/// fails retryably.
pub const ADMISSION_WAIT_BOUND: Duration = Duration::from_secs(30);

/// A wait for, or a hold of, the lock this long is logged at any log level:
/// the profiler span alone is off below verbose.
const ADMISSION_SLOW: Duration = Duration::from_millis(250);

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// An acquisition that waited out its bound. Nothing was decided or written,
/// so the operation that wanted the lock can be retried as it stands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionBusy {
    pub waiter: AdmissionSite,
    pub waited: Duration,
    /// Who held the lock when the wait gave up, and for how long.
    pub holder: Option<AdmissionHolder>,
}

/// Who holds the admission lock: its site, what it is admitting (an issue or
/// execution id, or a sweep pass's candidate count), and for how long. A
/// holder that never lets go is a future on no thread's stack, so this is how
/// it names itself to whoever times out waiting for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionHolder {
    pub site: AdmissionSite,
    pub subject: String,
    pub held: Duration,
}

impl std::fmt::Display for AdmissionHolder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "a {} ({}) for {} ms",
            self.site,
            self.subject,
            millis(self.held)
        )
    }
}

impl std::fmt::Display for AdmissionBusy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "the execution queue is busy: this {} waited {} ms for the admission lock",
            self.waiter,
            millis(self.waited)
        )?;
        match &self.holder {
            Some(holder) => write!(f, ", held by {holder}")?,
            None => f.write_str(", which was just let go")?,
        }
        f.write_str(". Nothing was written; retry.")
    }
}

type HolderSlot = Arc<Mutex<Option<(AdmissionSite, String, Instant)>>>;

/// The execution queue's admission lock: every capped filing and every gate
/// release decides its caps and writes under it, so two admissions never
/// interleave over a count that spans databases.
///
/// Lock order: this lock, then the owning database's write. Never take it
/// while holding a write transaction, and never hold it across a thread join
/// or a `block_on`: the holder is a future that needs a runtime worker to
/// finish, and a worker blocked waiting for it is one fewer worker to run it
/// (cairn/5844).
///
/// Bounded and attributed: an acquisition waits at most
/// [`ADMISSION_WAIT_BOUND`], the current holder's site, subject and age are
/// readable without the lock, and each acquisition is one `admission_lock`
/// profiler span carrying its `wait_ms` and `held_ms`.
pub struct AdmissionLock {
    mutex: Arc<tokio::sync::Mutex<()>>,
    holder: HolderSlot,
    wait_bound_ms: AtomicU64,
    acquisitions: [AtomicU64; 4],
}

impl Default for AdmissionLock {
    fn default() -> Self {
        Self {
            mutex: Arc::new(tokio::sync::Mutex::new(())),
            holder: Arc::new(Mutex::new(None)),
            wait_bound_ms: AtomicU64::new(millis(ADMISSION_WAIT_BOUND)),
            acquisitions: Default::default(),
        }
    }
}

impl AdmissionLock {
    /// Take the lock for `site`, admitting `subject` (an issue or execution
    /// id, or a pass's candidate count), or say who holds it once the bound
    /// runs out.
    pub async fn acquire(
        &self,
        site: AdmissionSite,
        subject: impl Into<String>,
    ) -> Result<AdmissionGuard, AdmissionBusy> {
        let subject = subject.into();
        let span = tracing::info_span!(
            target: "profiler",
            "admission_lock",
            site = site.as_str(),
            subject = subject.as_str(),
            wait_ms = tracing::field::Empty,
            held_ms = tracing::field::Empty,
        );
        let bound = Duration::from_millis(self.wait_bound_ms.load(Ordering::Relaxed));
        let started = Instant::now();
        let acquired = tokio::time::timeout(bound, self.mutex.clone().lock_owned()).await;
        let waited = started.elapsed();
        span.record("wait_ms", millis(waited));
        let Ok(guard) = acquired else {
            let busy = AdmissionBusy {
                waiter: site,
                waited,
                holder: self.holder(),
            };
            let (holder, holder_subject, held_for) =
                busy.holder.as_ref().map_or(("none", "", 0), |holder| {
                    (
                        holder.site.as_str(),
                        holder.subject.as_str(),
                        millis(holder.held),
                    )
                });
            log::warn!(
                "admission lock wait timed out waiter={site} waiter_subject={subject} \
                 holder={holder} holder_subject={holder_subject} held_for_ms={held_for}"
            );
            return Err(busy);
        };
        if waited >= ADMISSION_SLOW {
            log::info!(
                "admission lock acquired after a slow wait site={site} subject={subject} \
                 wait_ms={}",
                millis(waited)
            );
        }
        let acquired_at = Instant::now();
        *self.holder.lock().unwrap_or_else(PoisonError::into_inner) =
            Some((site, subject.clone(), acquired_at));
        self.acquisitions[site.index()].fetch_add(1, Ordering::Relaxed);
        Ok(AdmissionGuard {
            _guard: guard,
            holder: self.holder.clone(),
            site,
            subject,
            acquired_at,
            span,
        })
    }

    /// The current holder: its site, subject, and how long it has held the
    /// lock.
    pub fn holder(&self) -> Option<AdmissionHolder> {
        self.holder
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .map(|(site, subject, since)| AdmissionHolder {
                site: *site,
                subject: subject.clone(),
                held: since.elapsed(),
            })
    }

    /// How many times `site` has taken the lock.
    pub fn acquisitions(&self, site: AdmissionSite) -> u64 {
        self.acquisitions[site.index()].load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn set_wait_bound(&self, bound: Duration) {
        self.wait_bound_ms.store(millis(bound), Ordering::Relaxed);
    }
}

/// The admission lock, held. Dropping it clears the holder slot before the
/// lock is let go, and closes the acquisition's span with its `held_ms`.
pub struct AdmissionGuard {
    _guard: tokio::sync::OwnedMutexGuard<()>,
    holder: HolderSlot,
    site: AdmissionSite,
    subject: String,
    acquired_at: Instant,
    span: tracing::Span,
}

impl Drop for AdmissionGuard {
    fn drop(&mut self) {
        let held = self.acquired_at.elapsed();
        *self.holder.lock().unwrap_or_else(PoisonError::into_inner) = None;
        self.span.record("held_ms", millis(held));
        if held >= ADMISSION_SLOW {
            log::info!(
                "admission lock held long site={} subject={} held_ms={}",
                self.site,
                self.subject,
                millis(held)
            );
        }
    }
}

/// A fresh decision stored over a row that already has a place, carrying that
/// place so re-deciding a hold does not churn its row between sweeps.
pub(crate) fn carry_place(fact: &str, prior: Option<&str>, hold: &CapacityHold) -> String {
    let mut hold = hold.clone();
    hold.set_place(
        prior
            .and_then(|detail| decode_hold(fact, detail))
            .and_then(|prior| prior.place()),
    );
    encode_hold(&hold)
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

const THREAD_OF_ISSUE_SQL: &str = "SELECT t.id, t.name, i.number
       FROM issues i JOIN threads t ON t.id = i.parent_thread_id
      WHERE i.id = ?1";

const OPEN_THREAD_CHILDREN_SQL: &str = concat!(
    "SELECT i.number, i.id FROM issues i WHERE i.parent_thread_id = ?1 AND ",
    cairn_db::open_thread_child_sql!(),
    " ORDER BY i.number"
);

/// The thread an issue is a child of, as the focus cap reads it.
#[derive(Debug, Clone)]
struct IssueThread {
    thread_id: String,
    thread_name: String,
    number: i64,
}

async fn thread_of_issue_conn(
    conn: &cairn_db::turso::Connection,
    issue_id: &str,
) -> DbResult<Option<IssueThread>> {
    let mut rows = conn.query(THREAD_OF_ISSUE_SQL, params![issue_id]).await?;
    match rows.next().await? {
        Some(row) => Ok(Some(IssueThread {
            thread_id: row.text(0)?,
            thread_name: row.text(1)?,
            number: row.i64(2)?,
        })),
        None => Ok(None),
    }
}

/// A thread's open children, by issue number: `(number, issue id)`.
async fn open_thread_children_conn(
    conn: &cairn_db::turso::Connection,
    thread_id: &str,
) -> DbResult<Vec<(i64, String)>> {
    let mut rows = conn
        .query(OPEN_THREAD_CHILDREN_SQL, params![thread_id])
        .await?;
    let mut open = Vec::new();
    while let Some(row) = rows.next().await? {
        open.push((row.i64(0)?, row.text(1)?));
    }
    Ok(open)
}

/// Every unsatisfied capacity row in one database, with what the queue needs
/// to know about its execution: whether it runs on this device (`?1`),
/// whether it is in line at all (awaiting release, and not waiting on an
/// authored gate of its own), and its issue's thread.
const CAPACITY_ROWS_SQL: &str = concat!(
    "SELECT g.execution_id, json_extract(g.predicate_json, '$.fact'), g.created_at,
            g.last_outcome,
            CASE WHEN COALESCE(e.runner_device_id, ?1) = ?1 THEN 1 ELSE 0 END,
            CASE WHEN ",
    cairn_db::awaiting_release_sql!(),
    " AND NOT ",
    cairn_db::awaits_authored_gate_sql!(),
    " THEN 1 ELSE 0 END,
            i.parent_thread_id
       FROM execution_gates g
       JOIN executions e ON e.id = g.execution_id
       LEFT JOIN issues i ON i.id = e.issue_id
      WHERE g.satisfied_at IS NULL AND ",
    cairn_db::capacity_gate_sql!()
);

struct CapacityRow {
    execution_id: String,
    fact: &'static str,
    created_at: i64,
    /// `last_outcome = 'no_evidence'`: the cap holds it now. A cleared row
    /// reads `satisfied` and only keeps the execution's place.
    blocking: bool,
    on_device: bool,
    in_line: bool,
    thread_id: Option<String>,
}

async fn capacity_rows(db: &LocalDb, device: &str) -> DbResult<Vec<CapacityRow>> {
    let device = device.to_string();
    db.read(move |conn| {
        let device = device.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(CAPACITY_ROWS_SQL, params![device.as_str()])
                .await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                let named = row.opt_text(1)?;
                let Some(fact) = named
                    .as_deref()
                    .and_then(|named| CAPACITY_FACTS.iter().copied().find(|fact| *fact == named))
                else {
                    continue;
                };
                out.push(CapacityRow {
                    execution_id: row.text(0)?,
                    fact,
                    created_at: row.i64(2)?,
                    blocking: row.opt_text(3)?.as_deref() == Some("no_evidence"),
                    on_device: row.i64(4)? != 0,
                    in_line: row.i64(5)? != 0,
                    thread_id: row.opt_text(6)?,
                });
            }
            Ok(out)
        })
    })
    .await
}

/// One start to decide: the held execution being released, or `None` for a
/// new filing, on `issue_id` in `db`, the database that owns the issue.
pub(crate) struct Start<'a> {
    pub db: &'a LocalDb,
    pub issue_id: Option<&'a str>,
    pub execution_id: Option<&'a str>,
}

/// An execution's oldest unsatisfied row on one cap: its place in line, and
/// whether the cap holds it now.
#[derive(Debug, Clone, Copy)]
struct StoredHold {
    since: i64,
    blocking: bool,
}

/// The execution queue as one read sees it: the fleet's load, every
/// capacity row, and the threads of the starts being decided. Read once per
/// decision under the admission lock, so a sweep pass decides every candidate
/// against one read and keeps it current as it releases and holds, instead
/// of reading the queue again per execution.
pub(crate) struct QueueSnapshot {
    fleet_limit: usize,
    thread_limit: usize,
    load: FleetLoad,
    stored: HashMap<(String, &'static str), StoredHold>,
    /// Fleet holds in line on this device: execution -> since.
    fleet_line: HashMap<String, i64>,
    /// Thread holds in line, per thread: execution -> since.
    thread_lines: HashMap<String, HashMap<String, i64>>,
    issues: HashMap<String, IssueThread>,
    open_children: HashMap<String, Vec<(i64, String)>>,
    /// Whether a start's execution runs on this device's fleet.
    on_device: HashMap<String, bool>,
}

impl QueueSnapshot {
    /// Read the queue for `starts`: `2D + G` read transactions for `D` open
    /// databases and `G` databases the starts live in, however many starts.
    pub(crate) async fn load(orch: &Orchestrator, starts: &[Start<'_>]) -> Result<Self, String> {
        Self::read(orch, starts)
            .instrument(tracing::info_span!(
                target: "profiler",
                "admission_decide",
                starts = starts.len(),
            ))
            .await
    }

    async fn read(orch: &Orchestrator, starts: &[Start<'_>]) -> Result<Self, String> {
        let reading = |error: crate::storage::DbError| format!("failed to read the queue: {error}");
        let settings = orch.get_settings();
        let device = orch.anon_device_manager.device_id();
        let mut snapshot = Self {
            fleet_limit: settings.max_concurrent_executions.max(1) as usize,
            thread_limit: settings.max_open_children_per_thread.max(1) as usize,
            load: fleet_load(orch).await?,
            stored: HashMap::new(),
            fleet_line: HashMap::new(),
            thread_lines: HashMap::new(),
            issues: HashMap::new(),
            open_children: HashMap::new(),
            on_device: HashMap::new(),
        };
        for db in orch.db.all_dbs().await {
            for row in capacity_rows(&db, &device).await.map_err(reading)? {
                snapshot.add_row(row);
            }
        }

        // Each database the starts live in: one read for their devices,
        // their issues' threads, and those threads' open children.
        let mut groups: Vec<(&LocalDb, Vec<(Option<String>, Option<String>)>)> = Vec::new();
        for start in starts {
            let wanted = (
                start.execution_id.map(str::to_string),
                start.issue_id.map(str::to_string),
            );
            match groups
                .iter_mut()
                .find(|(db, _)| std::ptr::eq(*db, start.db))
            {
                Some((_, group)) => group.push(wanted),
                None => groups.push((start.db, vec![wanted])),
            }
        }
        for (db, wanted) in groups {
            let known: HashSet<String> = snapshot.open_children.keys().cloned().collect();
            let (devices, issues, children) = db
                .read(move |conn| {
                    let (wanted, mut known) = (wanted.clone(), known.clone());
                    Box::pin(async move {
                        let (mut devices, mut issues, mut children) =
                            (Vec::new(), Vec::new(), Vec::new());
                        for (execution_id, issue_id) in &wanted {
                            if let Some(execution_id) = execution_id {
                                let runner = crate::storage::query_opt_text_conn(
                                    conn,
                                    "SELECT runner_device_id FROM executions WHERE id = ?1",
                                    params![execution_id.as_str()],
                                )
                                .await?;
                                devices.push((execution_id.clone(), runner));
                            }
                            let Some(issue_id) = issue_id else { continue };
                            let Some(thread) = thread_of_issue_conn(conn, issue_id).await? else {
                                continue;
                            };
                            if known.insert(thread.thread_id.clone()) {
                                children.push((
                                    thread.thread_id.clone(),
                                    open_thread_children_conn(conn, &thread.thread_id).await?,
                                ));
                            }
                            issues.push((issue_id.clone(), thread));
                        }
                        Ok((devices, issues, children))
                    })
                })
                .await
                .map_err(reading)?;
            for (execution_id, runner) in devices {
                let on_device = runner.as_deref().is_none_or(|runner| runner == device);
                snapshot.on_device.insert(execution_id, on_device);
            }
            snapshot.issues.extend(issues);
            snapshot.open_children.extend(children);
        }
        Ok(snapshot)
    }

    fn add_row(&mut self, row: CapacityRow) {
        let key = (row.execution_id.clone(), row.fact);
        let stored = self.stored.entry(key).or_insert(StoredHold {
            since: row.created_at,
            blocking: row.blocking,
        });
        if row.created_at < stored.since {
            *stored = StoredHold {
                since: row.created_at,
                blocking: row.blocking,
            };
        }
        if !(row.blocking && row.in_line) {
            return;
        }
        match row.fact {
            FLEET_SLOT_FACT if row.on_device => {
                self.fleet_line.insert(row.execution_id, row.created_at);
            }
            THREAD_FOCUS_FACT => {
                if let Some(thread_id) = row.thread_id {
                    self.thread_lines
                        .entry(thread_id)
                        .or_default()
                        .insert(row.execution_id, row.created_at);
                }
            }
            _ => {}
        }
    }

    /// A start's place in line on `fact`: when it first held on that cap, or
    /// last of all for one that has not queued on it (a new filing, or one
    /// whose `after:` gates just cleared).
    fn place(&self, start: &Start<'_>, fact: &'static str) -> i64 {
        start
            .execution_id
            .and_then(|id| self.stored.get(&(id.to_string(), fact)))
            .map_or(i64::MAX, |stored| stored.since)
    }

    /// Which caps hold `start` now.
    ///
    /// A cap holds when it is full, AND when anything is already held on it
    /// ahead of this start: a slot that frees between sweeps goes to the
    /// oldest held execution, never to a fresh filing that happened to land
    /// first. A start that re-runs an issue already counted as an open child
    /// of its thread opens no new child, so the thread cap does not hold it;
    /// nor does it when the issue has no thread.
    pub(crate) fn decide(&self, start: &Start<'_>) -> Admission {
        let ahead = |line: &HashMap<String, i64>, place: i64| {
            line.iter()
                .filter(|(id, since)| Some(id.as_str()) != start.execution_id && **since < place)
                .count()
        };
        let mut blocks = Vec::new();
        if self.load.in_progress >= self.fleet_limit
            || ahead(&self.fleet_line, self.place(start, FLEET_SLOT_FACT)) > 0
        {
            blocks.push(CapacityHold::Fleet {
                running: self.load.in_progress,
                limit: self.fleet_limit,
                unplaced: self.load.waiting_for_machine,
                place: None,
            });
        }
        let thread = start.issue_id.and_then(|issue_id| {
            let thread = self.issues.get(issue_id)?;
            Some((issue_id, thread))
        });
        if let Some((issue_id, thread)) = thread {
            let open = self
                .open_children
                .get(&thread.thread_id)
                .map(Vec::as_slice)
                .unwrap_or_default();
            let already_open = open.iter().any(|(_, id)| id == issue_id);
            let ahead = self
                .thread_lines
                .get(&thread.thread_id)
                .map_or(0, |line| ahead(line, self.place(start, THREAD_FOCUS_FACT)));
            if !already_open && (open.len() >= self.thread_limit || ahead > 0) {
                blocks.push(CapacityHold::Thread {
                    thread: thread.thread_name.clone(),
                    open: open.iter().map(|(number, _)| *number).collect(),
                    limit: self.thread_limit,
                    place: None,
                });
            }
        }
        Admission { blocks }
    }

    /// Whether holding `start` on `admission` would change nothing stored: it
    /// is held, and exactly the caps that hold it already have blocking rows.
    /// Such a start costs its pass no write; its facts are refreshed with the
    /// rest of the queue's after the pass.
    pub(crate) fn holds_as_stored(&self, start: &Start<'_>, admission: &Admission) -> bool {
        let Some(execution_id) = start.execution_id else {
            return false;
        };
        !admission.is_clear()
            && CAPACITY_FACTS.iter().all(|fact| {
                let wanted = admission.blocks.iter().any(|block| block.fact() == *fact);
                let stored = self
                    .stored
                    .get(&(execution_id.to_string(), *fact))
                    .is_some_and(|stored| stored.blocking);
                wanted == stored
            })
    }

    /// `start` was released: it takes a fleet slot, opens its thread's child
    /// if that was not open yet, and leaves every line.
    pub(crate) fn record_release(&mut self, start: &Start<'_>) {
        // The fleet counts this device's executions only: releasing one that
        // runs on another device takes no slot here.
        let on_device = start
            .execution_id
            .and_then(|id| self.on_device.get(id))
            .copied()
            .unwrap_or(true);
        if on_device {
            self.load.in_progress += 1;
        }
        if let Some(issue_id) = start.issue_id {
            if let Some(thread) = self.issues.get(issue_id) {
                let open = self
                    .open_children
                    .entry(thread.thread_id.clone())
                    .or_default();
                if !open.iter().any(|(_, id)| id == issue_id) {
                    open.push((thread.number, issue_id.to_string()));
                    open.sort();
                }
            }
        }
        if let Some(execution_id) = start.execution_id {
            self.fleet_line.remove(execution_id);
            for line in self.thread_lines.values_mut() {
                line.remove(execution_id);
            }
            for fact in CAPACITY_FACTS {
                self.stored.remove(&(execution_id.to_string(), *fact));
            }
        }
    }

    /// `start` was held on `blocks` as of `now`: it stands in line on each of
    /// them, keeping its place where it had one, and leaves the others.
    pub(crate) fn record_hold(&mut self, start: &Start<'_>, blocks: &[CapacityHold], now: i64) {
        let Some(execution_id) = start.execution_id else {
            return;
        };
        let thread_id = start
            .issue_id
            .and_then(|issue_id| self.issues.get(issue_id))
            .map(|thread| thread.thread_id.clone());
        let on_device = self.on_device.get(execution_id).copied().unwrap_or(true);
        for fact in CAPACITY_FACTS {
            let key = (execution_id.to_string(), *fact);
            let holds = blocks.iter().any(|block| block.fact() == *fact);
            let since = match self.stored.get_mut(&key) {
                Some(stored) => {
                    stored.blocking = holds;
                    stored.since
                }
                None if holds => {
                    self.stored.insert(
                        key,
                        StoredHold {
                            since: now,
                            blocking: true,
                        },
                    );
                    now
                }
                None => continue,
            };
            let line = match *fact {
                FLEET_SLOT_FACT if on_device => Some(&mut self.fleet_line),
                THREAD_FOCUS_FACT => thread_id
                    .as_ref()
                    .map(|thread_id| self.thread_lines.entry(thread_id.clone()).or_default()),
                _ => None,
            };
            if let Some(line) = line {
                if holds {
                    line.insert(execution_id.to_string(), since);
                } else {
                    line.remove(execution_id);
                }
            }
        }
    }

    /// `hold` with this read's counts: the fleet's load and limit, or the
    /// thread's name, open children and limit when this read knows the thread.
    fn refresh_counts(&self, hold: &mut CapacityHold, thread_id: Option<&str>) {
        match hold {
            CapacityHold::Fleet {
                running,
                limit,
                unplaced,
                ..
            } => {
                *running = self.load.in_progress;
                *limit = self.fleet_limit;
                *unplaced = self.load.waiting_for_machine;
            }
            CapacityHold::Thread {
                thread,
                open,
                limit,
                ..
            } => {
                let Some(thread_id) = thread_id else { return };
                let Some(children) = self.open_children.get(thread_id) else {
                    return;
                };
                if let Some(known) = self
                    .issues
                    .values()
                    .find(|known| known.thread_id == thread_id)
                {
                    thread.clone_from(&known.thread_name);
                }
                *open = children.iter().map(|(number, _)| *number).collect();
                *limit = self.thread_limit;
            }
        }
    }
}

/// Decide which caps hold a start on `issue_id` right now, over a fresh read
/// of the queue. Call it holding the [`AdmissionLock`], and act on it before
/// letting go.
///
/// `db` is the database that owns the issue. `execution_id` is the held
/// execution being released, or `None` for a new filing.
pub async fn decide(
    orch: &Orchestrator,
    db: &LocalDb,
    issue_id: Option<&str>,
    execution_id: Option<&str>,
) -> Result<Admission, String> {
    let start = Start {
        db,
        issue_id,
        execution_id,
    };
    let snapshot = QueueSnapshot::load(orch, std::slice::from_ref(&start)).await?;
    Ok(snapshot.decide(&start))
}

// ---------------------------------------------------------------------------
// Place in line
// ---------------------------------------------------------------------------

/// Rewrite every blocking capacity gate's place in line: the fleet's in one
/// order across every database, each thread's within the thread. Oldest held
/// first, by when the execution first held on that cap. With the `queue` a
/// sweep pass decided against, as it stood once the pass was done, the counts
/// each hold shows are rewritten too, so a hold the pass left as it was does
/// not have to be written under the admission lock to stay current. Neither
/// takes the lock: these are display values, never admission inputs. A row is
/// written only when what it renders changes; a row with no facts yet keeps
/// its place in the count but is left for the next decision to write. Returns
/// the executions whose census moved.
pub async fn refresh_places(
    orch: &Orchestrator,
    queue: Option<&QueueSnapshot>,
) -> Result<Vec<String>, String> {
    struct Blocking {
        db: Arc<LocalDb>,
        gate_id: String,
        execution_id: String,
        fact: &'static str,
        group: Option<(usize, String)>,
        detail: Option<String>,
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
                detail: gate.detail,
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
        let Some(mut hold) = gate
            .detail
            .as_deref()
            .and_then(|detail| decode_hold(gate.fact, detail))
        else {
            continue;
        };
        hold.set_place(Some(*place));
        if let Some(queue) = queue {
            let thread = gate.group.as_ref().map(|(_, thread)| thread.as_str());
            queue.refresh_counts(&mut hold, thread);
        }
        let rendered = encode_hold(&hold);
        if gate.detail.as_deref() == Some(rendered.as_str()) {
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

/// One cap-held execution as the queue view lists it.
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

/// The whole execution queue: the fleet's load and every execution a CAP
/// holds, in release order. One waiting on its own `after:` gates is not in
/// the queue, so it is not listed.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueView {
    pub in_progress: usize,
    pub limit: usize,
    pub waiting_for_machine: usize,
    /// How many executions the caps hold: `held.len()`.
    pub queued: usize,
    pub held: Vec<QueuedExecution>,
    /// `capacity_unclassified_yield{reason}` counts, by reason.
    pub unclassified_yields: BTreeMap<String, u64>,
}

/// Held executions a cap holds right now: a blocking capacity gate (the same
/// `no_evidence` row [`FLEET_AHEAD_SQL`] counts as ahead), and no unmet
/// authored gate of their own.
const QUEUE_ROWS_SQL: &str = concat!(
    "SELECT e.id, e.issue_id, e.seq, i.number, i.title, p.id, p.key, t.name, ",
    cairn_workflow::held_since_sql!(),
    "
       FROM executions e
       LEFT JOIN issues i ON i.id = e.issue_id
       LEFT JOIN projects p ON p.id = i.project_id
       LEFT JOIN threads t ON t.id = i.parent_thread_id
      WHERE ",
    cairn_db::awaiting_release_sql!(),
    " AND EXISTS (SELECT 1 FROM execution_gates g WHERE g.execution_id = e.id
                   AND g.satisfied_at IS NULL AND g.last_outcome = 'no_evidence' AND ",
    cairn_db::capacity_gate_sql!(),
    ") AND NOT ",
    cairn_db::awaits_authored_gate_sql!()
);

/// Every cap-held execution across every database, in release order, with
/// the fleet's load.
pub async fn queue_view(orch: &Orchestrator) -> Result<QueueView, String> {
    let settings = orch.get_settings();
    let fleet = fleet_load(orch).await?;
    let mut held = Vec::new();
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
        queued: held.len(),
        held,
        unclassified_yields: unclassified_yield_counts(),
    })
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

    /// A PR on issue `number`, owned by its job `sj{number}`.
    async fn merge_request(env: &Env, number: i32, status: &str) {
        env.db
            .execute_script(&format!(
                "INSERT INTO merge_requests(id, job_id, project_id, issue_id, title, source_branch,
                                            target_branch, status, opened_at, updated_at)
                   VALUES('smr{number}', 'sj{number}', 'p', 'si{number}', 'PR', 'b', 'main',
                          '{status}', 1, 1);"
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
        let issue_id = format!("si{issue_number}");
        env.db
            .read(move |conn| {
                let issue_id = issue_id.clone();
                Box::pin(async move {
                    let Some(thread) = thread_of_issue_conn(conn, &issue_id).await? else {
                        return Ok(Vec::new());
                    };
                    Ok(open_thread_children_conn(conn, &thread.thread_id)
                        .await?
                        .into_iter()
                        .map(|(number, _)| number)
                        .collect())
                })
            })
            .await
            .unwrap()
    }

    async fn census_of(
        env: &Env,
        execution_id: &str,
    ) -> Option<crate::models::ExecutionGateCensus> {
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
    }

    async fn summary(env: &Env, execution_id: &str) -> String {
        census_of(env, execution_id)
            .await
            .map(|census| census.summary)
            .unwrap_or_default()
    }

    /// A queued execution's hover detail.
    async fn detail(env: &Env, execution_id: &str) -> String {
        census_of(env, execution_id)
            .await
            .and_then(|census| census.detail)
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
    async fn operator_start(env: &Env, number: i32) -> String {
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
        .await
        .unwrap();
        assert_eq!(execution.status, crate::models::ExecutionStatus::Running);
        crate::execution::recipe::create_jobs_for_execution(&env.orch, &execution.id)
            .await
            .unwrap();
        execution.id
    }

    // -- one test per acceptance rule ---------------------------------------

    #[tokio::test]
    async fn a_seventh_execution_holds_for_a_fleet_slot_and_releases_once_when_one_completes() {
        let mut env = env("cap-fleet-seventh.db").await;
        full_fleet(&env).await;

        let filed = file(&env, 1, json!([])).await.unwrap();
        assert!(
            filed.contains("(recipe 'memory-triage'): Queued · 6/6 running."),
            "the filing names the cap, not yet its place: {filed}"
        );
        assert!(
            filed.ends_with("Only the operator's Start now skips the queue."),
            "{filed}"
        );
        assert!(is_held(&env, 1).await, "held: no jobs, no slot");
        let (id, ..) = execution(&env, 1).await;
        assert_eq!(
            summary(&env, &id).await,
            "Queued · 6/6 running",
            "the filing path numbers no place"
        );
        assert_eq!(detail(&env, &id).await, "Fleet");
        assert!(
            advanced(&mut env).is_empty(),
            "a held filing never advances"
        );
        assert!(sweep_at(&env.orch, now())
            .await
            .unwrap()
            .released
            .is_empty());
        assert_eq!(
            summary(&env, &id).await,
            "Queued · 1st · 6/6 running",
            "the sweep numbers it"
        );
        assert_eq!(detail(&env, &id).await, "Fleet · 1st");

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
        // An artifact awaiting review leaves its job blocked.
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
            filed.contains("Queued · harness 4/4 open"),
            "the thread cap holds it: {filed}"
        );
        let (id, ..) = execution(&env, 105).await;
        assert_eq!(summary(&env, &id).await, "Queued · harness 4/4 open");
        assert_eq!(
            detail(&env, &id).await,
            "harness · 100, 101, 102, 103",
            "the hover names the four"
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

        let id = operator_start(&env, 105).await;
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
            "Queued · 7/6 running · harness 5/4 open",
            "both caps show"
        );
        assert_eq!(
            detail(&env, &held).await,
            "Fleet\nharness · 100, 101, 102, 103, 105"
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
        assert!(filed.contains("Queued · execution 4/4 open"), "{filed}");
        let (held, ..) = execution(&env, 113).await;
        assert_eq!(
            detail(&env, &held).await,
            "execution · 103, 110, 111, 112",
            "the new thread counts it"
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
            reply.contains("still Queued · 1st · 6/6 running."),
            "{reply}"
        );
        assert!(
            reply.ends_with("Only the operator's Start now skips the queue."),
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
        assert_eq!(summary(&env, &older).await, "Queued · 1st · 6/6 running");
        assert_eq!(summary(&env, &younger).await, "Queued · 2nd · 6/6 running");

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
        assert!(summary(&env, &younger).await.starts_with("Queued · 1st · "));
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
        assert!(summary(&env, &queued).await.starts_with("Queued · 1st · "));
        assert!(
            summary(&env, &waited).await.starts_with("Queued · 2nd · "),
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
        assert!(summary(&env, &waited).await.starts_with("Queued · 1st · "));
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
                && !waiting.contains("Queued"),
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
        assert!(!summary(&env, &patched).await.contains("Queued"));
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
            filed.contains("Queued · 5/6 running"),
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
        assert!(filed.contains("Queued · harness 3/4 open"), "{filed}");
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
            "Queued · 1st · harness 4/4 open"
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

    /// A child whose executions all ended with no PR gave up its place, so
    /// rerunning it opens a new child the cap may hold.
    #[tokio::test]
    async fn rerunning_a_child_that_stopped_counting_is_a_new_child() {
        let env = env("cap-rerun-ended-child.db").await;
        caps(&env, 50, 4);
        add_thread(&env, "th", "harness").await;
        for number in 100..104 {
            add_started(&env, number, Some("th"), "running").await;
        }
        add_started(&env, 104, Some("th"), "failed").await;
        assert_eq!(open_children(&env, 104).await, vec![100, 101, 102, 103]);
        let filed = file(&env, 104, json!([])).await.unwrap();
        assert!(filed.contains("Queued · harness 4/4 open"), "{filed}");
        assert!(is_held(&env, 104).await);
    }

    #[tokio::test]
    async fn a_child_failed_at_preparation_without_a_pr_does_not_count() {
        let env = env("cap-preparation-failed.db").await;
        caps(&env, 50, 4);
        add_thread(&env, "th", "harness").await;
        for number in 100..103 {
            add_started(&env, number, Some("th"), "running").await;
        }
        // cairn/4741's shape, as `mark_job_start_failed` leaves it: a failed
        // job whose run ended at preparation.
        add_started(&env, 103, Some("th"), "failed").await;
        env.db
            .execute_script(
                "INSERT INTO runs(id, project_id, job_id, issue_id, status, exit_reason,
                                  created_at, updated_at)
                   VALUES('sr103', 'p', 'sj103', 'si103', 'crashed', 'job_preparation_failed', 1, 1);",
            )
            .await
            .unwrap();
        add_issue(&env, 104, Some("th")).await;
        assert_eq!(open_children(&env, 104).await, vec![100, 101, 102]);
        let filed = file(&env, 104, json!([])).await.unwrap();
        assert!(filed.starts_with("Started execution"), "{filed}");
    }

    #[tokio::test]
    async fn a_failed_child_with_an_open_pr_counts() {
        let env = env("cap-failed-with-pr.db").await;
        caps(&env, 50, 4);
        add_thread(&env, "th", "harness").await;
        for number in 100..103 {
            add_started(&env, number, Some("th"), "running").await;
        }
        add_started(&env, 103, Some("th"), "failed").await;
        merge_request(&env, 103, "open").await;
        add_issue(&env, 104, Some("th")).await;
        assert_eq!(open_children(&env, 104).await, vec![100, 101, 102, 103]);
        let filed = file(&env, 104, json!([])).await.unwrap();
        assert!(filed.contains("Queued · harness 4/4 open"), "{filed}");

        // Closing the PR ends it.
        env.db
            .execute(
                "UPDATE merge_requests SET status = 'closed' WHERE id = 'smr103'",
                (),
            )
            .await
            .unwrap();
        let (id, ..) = execution(&env, 104).await;
        assert_eq!(sweep_at(&env.orch, now()).await.unwrap().released, vec![id]);
    }

    #[tokio::test]
    async fn a_pr_awaiting_review_counts() {
        let env = env("cap-pr-awaiting-review.db").await;
        caps(&env, 50, 4);
        add_thread(&env, "th", "harness").await;
        for number in 100..103 {
            add_started(&env, number, Some("th"), "running").await;
        }
        add_started(&env, 103, Some("th"), "blocked").await;
        merge_request(&env, 103, "open").await;
        add_issue(&env, 104, Some("th")).await;
        assert_eq!(open_children(&env, 104).await, vec![100, 101, 102, 103]);
        let filed = file(&env, 104, json!([])).await.unwrap();
        assert!(filed.contains("Queued · harness 4/4 open"), "{filed}");
    }

    /// A sibling that fails while blocked (on review, say) ends its live work;
    /// the transition nudges the queue, and one sweep releases the held start.
    #[tokio::test]
    async fn a_held_thread_start_releases_when_a_siblings_execution_fails_and_ends() {
        let mut env = env("cap-sibling-fails.db").await;
        caps(&env, 50, 2);
        add_thread(&env, "th", "harness").await;
        add_started(&env, 100, Some("th"), "blocked").await;
        add_started(&env, 101, Some("th"), "blocked").await;
        add_issue(&env, 102, Some("th")).await;
        file(&env, 102, json!([])).await.unwrap();
        assert!(is_held(&env, 102).await);
        let (id, ..) = execution(&env, 102).await;

        let hints = cairn_workflow::gates::hint::sent_on_this_thread();
        env.db
            .write(|conn| {
                Box::pin(async move {
                    cairn_workflow::advancement::recompute::write_job_status(
                        conn,
                        "sj100",
                        &crate::models::JobStatus::Blocked,
                        &crate::models::JobStatus::Failed,
                    )
                    .await
                })
            })
            .await
            .unwrap();
        assert!(
            cairn_workflow::gates::hint::sent_on_this_thread() > hints,
            "ending the job nudges the queue"
        );
        assert_eq!(open_children(&env, 102).await, vec![101]);

        let report = sweep_at(&env.orch, now()).await.unwrap();
        assert_eq!(report.released, vec![id.clone()], "released once");
        assert_eq!(advanced(&mut env), vec![id]);
        assert!(sweep_at(&env.orch, now())
            .await
            .unwrap()
            .released
            .is_empty());
    }

    /// The queue page lists what a CAP holds, and nothing that waits on its own
    /// `after:` gates or whose cap has cleared and only awaits the release.
    #[tokio::test]
    async fn the_queue_lists_only_what_a_cap_holds() {
        let env = env("cap-queue-view.db").await;
        full_fleet(&env).await;
        file(&env, 1, json!([])).await.unwrap();
        let (capped, ..) = execution(&env, 1).await;
        file(
            &env,
            2,
            json!([{"fact": "issue_resolution", "issue": "cairn://p/prj/5", "resolution": "merged"}]),
        )
        .await
        .unwrap();
        assert!(is_held(&env, 2).await, "held on its own gate");
        file(&env, 3, json!([])).await.unwrap();
        let (cleared, ..) = execution(&env, 3).await;
        env.db
            .execute(
                "UPDATE execution_gates SET last_outcome = 'satisfied', detail = NULL
                  WHERE execution_id = ?1",
                (cleared.clone(),),
            )
            .await
            .unwrap();

        let view = queue_view(&env.orch).await.unwrap();
        let listed: Vec<&str> = view
            .held
            .iter()
            .map(|held| held.execution_id.as_str())
            .collect();
        assert_eq!(listed, vec![capped.as_str()]);
        assert_eq!(view.queued, 1);
        assert_eq!((view.in_progress, view.limit), (6, 6));
        let census = view.held[0].census.as_ref().unwrap();
        assert!(census.queued);
        // Both filings share the test clock's instant, so which is numbered
        // first is a tie broken by id; `oldest_held_first` covers the order.
        assert!(
            census.summary.starts_with("Queued · ") && census.summary.ends_with(" · 6/6 running"),
            "{}",
            census.summary
        );
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
            !rendered.contains(&id) && !rendered.contains("Queued"),
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
                "maxOpenChildrenPerThread",
                "gates::capacity",
            ] {
                assert!(!source.contains(needle), "{path} reads `{needle}`");
            }
        }
    }

    /// A hold's stored form round-trips, and a re-decided hold keeps the place
    /// its row already had.
    #[test]
    fn a_stored_hold_keeps_its_place_across_decisions() {
        let decided = CapacityHold::Fleet {
            running: 6,
            limit: 6,
            unplaced: 2,
            place: None,
        };
        let stored = encode_hold(&CapacityHold::Fleet {
            running: 7,
            limit: 6,
            unplaced: 0,
            place: Some(3),
        });
        assert_eq!(
            stored,
            r#"{"cap":"fleet","running":7,"limit":6,"unplaced":0,"place":3}"#
        );
        let carried = carry_place(FLEET_SLOT_FACT, Some(&stored), &decided);
        assert_eq!(
            decode_hold(FLEET_SLOT_FACT, &carried),
            Some(CapacityHold::Fleet {
                running: 6,
                limit: 6,
                unplaced: 2,
                place: Some(3),
            })
        );
        assert_eq!(
            decode_hold(
                FLEET_SLOT_FACT,
                &carry_place(FLEET_SLOT_FACT, None, &decided)
            ),
            Some(decided.clone()),
            "a first hold has no place yet"
        );
        assert_eq!(
            decode_hold(THREAD_FOCUS_FACT, &stored),
            None,
            "facts for another cap are not this row's"
        );
        assert_eq!(
            decode_hold(
                FLEET_SLOT_FACT,
                "waiting for a fleet slot: 6 of 6 in progress"
            ),
            None,
            "old prose is unknown facts"
        );
        let thread = CapacityHold::Thread {
            thread: "harness".to_string(),
            open: vec![5783, 5790],
            limit: 2,
            place: Some(1),
        };
        assert_eq!(
            decode_hold(THREAD_FOCUS_FACT, &encode_hold(&thread)),
            Some(thread)
        );
    }
}
