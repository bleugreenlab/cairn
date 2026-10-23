//! The private ordinary-session lane's budget: what it admits against, what it
//! does when the head of its queue cannot fit, and how surfaces read that back.
//!
//! ## Memory is the brake
//!
//! Agent CLIs are long-lived, network-bound processes whose real cost to the
//! host is resident memory. So every promotion pass measures the host with the
//! warm GC's own probe and admits the next queued session only while
//! `available - in_flight - estimate` stays at or above the reserve the warm GC
//! keeps (`max(2 GiB, 10% of RAM)`, [`memory_reserve`]). Using the GC's reserve
//! rather than a separate floor matters: a lower admission floor would make
//! every admission past it cause the next GC sweep to evict a warm process.
//!
//! There is deliberately no CPU brake here. Sessions spend their time waiting
//! on the network; CPU pressure belongs to build children on executors, and
//! braking sessions on it would make an interactive resume wait on build load.
//!
//! `private_dedicated_process_capacity` survives only as an explicit operator
//! ceiling. It is never a default, and it is read per pass, so changing it takes
//! effect without a restart.
//!
//! ## A blocked head makes room
//!
//! A warm idle process and a durably parked one (a session self-suspended on an
//! `agent_waits` row) both stay resident and keep their `active` unit: a park
//! is resumable by design, and its memory is real and measured as such. Neither
//! is GC-protected, so queued demand can reclaim them. When the head is blocked
//! and nothing in the lane is mid-spawn (so the probe is not about to change
//! underneath the decision), the promoter asks the warm GC for room on the
//! head's behalf: enough memory for the head's estimate, or the one least
//! relevant warm process holding a unit in the lane under an explicit ceiling.
//! The evicted process's release re-triggers promotion, and an evicted park
//! resumes later through the same queue.
//!
//! ## Boot ramp
//!
//! A restart resumes every session the old process was serving at once: the
//! watchdog reconcile and the owned-wait reconcile each hand the queue a wave of
//! launches within seconds, and memory, the only brake above, has room for all
//! of them. What the host cannot absorb is their warm-up. A Claude process is
//! expensive between its spawn and its first output (that interval grew from
//! under a second to 12-22 s in a 20-resume wave), and enough of them at once
//! convoys the runtime every UI command waits on.
//!
//! So for [`BOOT_RAMP_HORIZON`] after boot the promoter keeps at most
//! [`BOOT_RAMP_WINDOW`] spawns warming. A spawn warms from its lease until its
//! process's first output, its launch ending (its admission row leaving
//! `leased`/`starting`/`active`), or [`BOOT_RAMP_WARMING_TIMEOUT`]; a backend
//! that does not report first output stops warming once its process registers.
//! Each release promotes again, so the window refills at the pace warm-up
//! actually takes rather than on a clock. Launches the window holds stay
//! durably `queued`, which is what the reaper and every surface already read.
//!
//! This is a warm-up window, not a resident-session cap. It never counts a
//! process past its first output, it ends with the horizon, and memory remains
//! the only bound on how many sessions are resident. Urgent work (an operator
//! waiting on the launch) is never held, though it counts as warming. The
//! window and horizon are constants: a knob an operator had to set to recover
//! from a convoy would be situational state.

use super::{run_db, run_db_borrowed};
use crate::agent_process::gc::SpawnRoom;
use crate::orchestrator::Orchestrator;
use crate::resume_timing::ResumeTimingEvent;
use crate::storage::{
    HostMemoryEnvelope, LaneBudget, LaneShortfall, RowExt, RuntimeResourceCapacity, SpawnWindow,
};
use cairn_session::agent_process::gc::{memory_reserve, DEFAULT_SPAWN_RSS};
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub(crate) const WORKSPACE: &str = "private-runtime";

/// How long a memory-blocked lane waits before asking the GC for room again.
/// Eviction kills are asynchronous, so the probe can still read low right after
/// one; without this a second pass would evict again for the same shortfall.
/// Matches the process sweep interval, which re-measures and retries.
const MEMORY_EVICTION_COOLDOWN: Duration = Duration::from_secs(30);

/// How long after boot the promoter bounds spawn warm-up. See the module doc.
pub(crate) const BOOT_RAMP_HORIZON: Duration = Duration::from_secs(10 * 60);
/// How many spawns may be warming at once while the boot ramp is on.
pub(crate) const BOOT_RAMP_WINDOW: u32 = 4;
/// A spawn with no output this long after its lease stops counting as warming,
/// so one wedged process cannot hold the window shut.
pub(crate) const BOOT_RAMP_WARMING_TIMEOUT: Duration = Duration::from_secs(120);

/// What one promotion pass admitted against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PrivateLaneBudget {
    /// Explicit operator ceiling on resident units; `None` means memory alone.
    pub ceiling: Option<u32>,
    /// Measured host envelope; `None` when the host cannot be measured, which
    /// admits on memory (unmeasured is unpressured).
    pub memory: Option<HostMemoryEnvelope>,
    pub measured_at: i64,
}

impl PrivateLaneBudget {
    pub fn capacity(&self) -> RuntimeResourceCapacity {
        RuntimeResourceCapacity {
            resident_process_units: self.ceiling.unwrap_or(u32::MAX),
            logical_stream_units: u32::MAX,
        }
    }
}

/// The host's system memory through the warm GC's probe when warm retention is
/// on, so both measure the same way; the OS probe otherwise.
fn host_memory(orch: &Orchestrator) -> Option<crate::agent_process::memory::SystemMemory> {
    match orch.warm_gc.as_ref() {
        Some(gc) => gc.system_memory(),
        None => cairn_common::memory::system_memory(),
    }
}

/// Measure the budget for one promotion pass: the operator's optional ceiling
/// and the host memory envelope, with the moment it was measured.
pub(crate) fn private_lane_budget(orch: &Orchestrator) -> PrivateLaneBudget {
    PrivateLaneBudget {
        ceiling: orch.get_settings().private_dedicated_process_capacity,
        memory: host_memory(orch).map(|system| HostMemoryEnvelope {
            available_bytes: system.available,
            reserve_bytes: memory_reserve(system.total),
            unmeasured_claim_bytes: DEFAULT_SPAWN_RSS,
        }),
        measured_at: chrono::Utc::now().timestamp(),
    }
}

#[derive(Debug, Clone)]
struct LaneDecision {
    budget: PrivateLaneBudget,
    shortfall: Option<LaneShortfall>,
}

/// One spawn the boot ramp counts as warming.
struct WarmingSpawn {
    lane: String,
    request_id: String,
    leased_at: Instant,
    /// Its backend reports the process's first output (Claude's reader does).
    /// Otherwise the spawn stops warming once its process registers.
    reports_first_output: bool,
}

/// This boot's warm-up window. In memory only: the ramp is per boot, and a
/// restart starts a new one.
#[derive(Default)]
struct BootRamp {
    /// Keyed by run id.
    warming: HashMap<String, WarmingSpawn>,
    /// Queue heads the ramp held, with when it first held each.
    held_since: HashMap<String, Instant>,
    held: u64,
    longest_hold: Duration,
    ended: bool,
}

/// The last decision each private lane's promoter made. Surfaces read this
/// rather than probing, so what they display is what was decided.
#[derive(Default)]
pub struct PrivateLaneLedger {
    decisions: Mutex<HashMap<String, LaneDecision>>,
    memory_evictions: Mutex<HashMap<String, Instant>>,
    boot_ramp: Mutex<BootRamp>,
    /// Serializes, across every pass on every private lane, a ramp pass's
    /// window read, the lease it decides, and the warming registration that
    /// lease implies. Without it, concurrent passes (a restart's enqueue burst
    /// fires one each) all read the same count and lease past the window.
    ramp_admission: Mutex<()>,
}

impl PrivateLaneLedger {
    /// Hold the ramp's admission critical section. The caller drops it before
    /// starting the leased launch, so a slow spawn never serializes the lane.
    pub(crate) fn lock_ramp_admission(&self) -> std::sync::MutexGuard<'_, ()> {
        self.ramp_admission
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn ramp(&self) -> Option<std::sync::MutexGuard<'_, BootRamp>> {
        self.boot_ramp.lock().ok()
    }

    /// The window as it stands: how many spawns are warming against how many
    /// may.
    pub(crate) fn spawn_window(&self) -> SpawnWindow {
        SpawnWindow {
            warming: self.ramp().map_or(0, |ramp| ramp.warming.len() as u64),
            window: BOOT_RAMP_WINDOW,
        }
    }

    /// Count `run_id`'s spawn as warming from its lease.
    pub(crate) fn begin_warming(&self, run_id: &str, lane: &str, request_id: &str, urgent: bool) {
        let Some(mut ramp) = self.ramp() else {
            return;
        };
        let now = Instant::now();
        if let Some(since) = ramp.held_since.remove(request_id) {
            ramp.held += 1;
            ramp.longest_hold = ramp.longest_hold.max(now.duration_since(since));
        }
        ramp.warming.insert(
            run_id.to_string(),
            WarmingSpawn {
                lane: lane.to_string(),
                request_id: request_id.to_string(),
                leased_at: now,
                reports_first_output: false,
            },
        );
        let warming = ramp.warming.len();
        drop(ramp);
        let mut event = ResumeTimingEvent::new("boot_ramp_lease");
        event.run_id = Some(run_id);
        event.count = Some(warming);
        event.mode = Some(if urgent { "urgent" } else { "ordinary" });
        event.emit();
    }

    /// `run_id`'s backend will report its process's first output, so it keeps
    /// warming past registration until it does.
    pub(crate) fn expect_first_output(&self, run_id: &str) {
        if let Some(spawn) = self
            .ramp()
            .as_mut()
            .and_then(|ramp| ramp.warming.get_mut(run_id))
        {
            spawn.reports_first_output = true;
        }
    }

    /// Stop counting `run_id` as warming, recording why (`first_output`,
    /// `active`, `ended` or `timeout`). Returns its lane, for the caller to
    /// promote what the freed slot admits, or `None` if it was not warming.
    pub(crate) fn release_warming(&self, run_id: &str, mode: &str) -> Option<String> {
        let spawn = self.ramp()?.warming.remove(run_id)?;
        let mut event = ResumeTimingEvent::new("boot_ramp_release").elapsed(spawn.leased_at);
        event.run_id = Some(run_id);
        event.mode = Some(mode);
        event.emit();
        Some(spawn.lane)
    }

    /// Its process registered: a spawn whose backend reports no first output
    /// has nothing further to wait for.
    fn release_unless_awaiting_output(&self, run_id: &str) {
        let awaiting = self.ramp().is_some_and(|ramp| {
            ramp.warming
                .get(run_id)
                .is_some_and(|spawn| spawn.reports_first_output)
        });
        if !awaiting {
            self.release_warming(run_id, "active");
        }
    }

    fn note_ramp_hold(&self, request_id: &str) {
        if let Some(mut ramp) = self.ramp() {
            ramp.held_since
                .entry(request_id.to_string())
                .or_insert_with(Instant::now);
        }
    }

    /// Warming spawns as `(run_id, request_id, leased_at)`.
    fn warming_spawns(&self) -> Vec<(String, String, Instant)> {
        self.ramp().map_or_else(Vec::new, |ramp| {
            ramp.warming
                .iter()
                .map(|(run, spawn)| (run.clone(), spawn.request_id.clone(), spawn.leased_at))
                .collect()
        })
    }

    /// Close the ramp once, logging what it did.
    fn end_boot_ramp(&self) {
        let Some(mut ramp) = self.ramp() else {
            return;
        };
        if ramp.ended {
            return;
        }
        ramp.ended = true;
        let still_warming = ramp.warming.len();
        ramp.warming.clear();
        ramp.held_since.clear();
        log::info!(
            "boot ramp ended: it held {} background launch(es), the longest for {}s; {still_warming} spawn(s) were still warming",
            ramp.held,
            ramp.longest_hold.as_secs()
        );
    }

    #[cfg(test)]
    pub(crate) fn warming_runs(&self) -> HashSet<String> {
        self.ramp()
            .map(|ramp| ramp.warming.keys().cloned().collect())
            .unwrap_or_default()
    }

    #[cfg(test)]
    pub(crate) fn backdate_warming(&self, run_id: &str, by: Duration) {
        if let Some(spawn) = self
            .ramp()
            .as_mut()
            .and_then(|ramp| ramp.warming.get_mut(run_id))
        {
            spawn.leased_at = spawn.leased_at.checked_sub(by).expect("backdated instant");
        }
    }

    pub(crate) fn record(
        &self,
        lane: &str,
        budget: PrivateLaneBudget,
        shortfall: Option<LaneShortfall>,
    ) {
        if let Ok(mut decisions) = self.decisions.lock() {
            decisions.insert(lane.to_string(), LaneDecision { budget, shortfall });
        }
    }

    fn decision(&self, lane: &str) -> Option<LaneDecision> {
        self.decisions.lock().ok()?.get(lane).cloned()
    }

    /// Claim the lane's memory-eviction slot unless one was used recently.
    fn claim_memory_eviction(&self, lane: &str, now: Instant) -> bool {
        let Ok(mut evictions) = self.memory_evictions.lock() else {
            return false;
        };
        if evictions
            .get(lane)
            .is_some_and(|last| now.duration_since(*last) < MEMORY_EVICTION_COOLDOWN)
        {
            return false;
        }
        evictions.insert(lane.to_string(), now);
        true
    }
}

/// Whether this boot is still inside its ramp horizon.
pub(crate) fn boot_ramp_active(orch: &Orchestrator) -> bool {
    chrono::Utc::now().timestamp()
        < orch
            .boot_at
            .saturating_add(BOOT_RAMP_HORIZON.as_secs() as i64)
}

/// The spawn window this promotion pass leases against, or `None` once the
/// boot ramp is over. Spawns that stopped warming without saying so (their
/// launch ended, or they timed out) are released first, so the window reads
/// only spawns that are still warming.
pub(super) fn boot_ramp_window(orch: &Orchestrator) -> Option<SpawnWindow> {
    if !boot_ramp_active(orch) {
        orch.private_lanes.end_boot_ramp();
        return None;
    }
    let now = Instant::now();
    for (run_id, request_id, leased_at) in orch.private_lanes.warming_spawns() {
        if now.duration_since(leased_at) >= BOOT_RAMP_WARMING_TIMEOUT {
            log::warn!(
                "boot ramp: run {run_id} produced no output {}s after its lease; it no longer counts as warming",
                now.duration_since(leased_at).as_secs()
            );
            orch.private_lanes.release_warming(&run_id, "timeout");
            continue;
        }
        // An unreadable row proves nothing; the timeout still bounds it.
        let launch_live = run_db({
            let db = orch.db.local.clone();
            async move {
                crate::storage::get_admission_request(&db, &request_id)
                    .await
                    .map_err(|error| error.to_string())
            }
        })
        .map(|request| {
            request.is_some_and(|request| {
                matches!(request.state.as_str(), "leased" | "starting" | "active")
            })
        });
        if launch_live == Ok(false) {
            orch.private_lanes.release_warming(&run_id, "ended");
        }
    }
    Some(orch.private_lanes.spawn_window())
}

/// Settle a warming spawn once its start returns: its slot frees at once if
/// the start left no process that will report first output.
pub(super) fn settle_warming_start(
    orch: &Orchestrator,
    run_id: &str,
    started: &Result<bool, String>,
) {
    match started {
        Ok(true) => orch.private_lanes.release_unless_awaiting_output(run_id),
        Ok(false) | Err(_) => {
            orch.private_lanes.release_warming(run_id, "ended");
        }
    }
}

/// Record that the ramp is holding `head_request_id`, for the end-of-ramp log.
pub(super) fn note_ramp_hold(orch: &Orchestrator, head_request_id: &str) {
    orch.private_lanes.note_ramp_hold(head_request_id);
}

/// `run_id`'s backend reports its process's first output; the ramp keeps it
/// warming until then.
pub(crate) fn spawn_reports_first_output(orch: &Orchestrator, run_id: &str) {
    orch.private_lanes.expect_first_output(run_id);
}

/// `run_id`'s process produced its first output: it is no longer warming, and
/// its slot admits the next background launch.
pub(crate) fn spawn_produced_first_output(orch: &Orchestrator, run_id: &str) {
    if let Some(lane) = orch.private_lanes.release_warming(run_id, "first_output") {
        super::ordinary_admission::promote_private_lane(orch, &lane);
    }
}

/// Ask the warm GC for room on behalf of a blocked queue head. The caller has
/// established that nothing in the lane is mid-spawn.
pub(super) fn make_room_for_head(orch: &Orchestrator, lane: &str, shortfall: &LaneShortfall) {
    let evicted = match shortfall {
        LaneShortfall::Memory {
            head_estimate_bytes,
            ..
        } => {
            if !orch
                .private_lanes
                .claim_memory_eviction(lane, Instant::now())
            {
                return;
            }
            orch.collect_warm(SpawnRoom::Bytes(*head_estimate_bytes))
        }
        LaneShortfall::Units { .. } => {
            let holders = match lane_unit_holders(orch, lane) {
                Ok(holders) => holders,
                Err(error) => {
                    log::warn!(
                        "ordinary admission lane {lane} could not list unit holders: {error}"
                    );
                    return;
                }
            };
            orch.evict_one_warm_unit(&holders).into_iter().collect()
        }
        // Evicting frees memory or a unit, never a warm-up slot.
        LaneShortfall::Streams { .. } | LaneShortfall::Ramp { .. } => Vec::new(),
    };
    if !evicted.is_empty() {
        log::info!(
            "ordinary admission lane {lane} evicted {} warm process(es) for its queue head ({shortfall:?})",
            evicted.len()
        );
    }
}

/// Runs holding an `active` unit in `lane`: the only processes whose eviction
/// frees a unit the head can use.
fn lane_unit_holders(orch: &Orchestrator, lane: &str) -> Result<HashSet<String>, String> {
    let db = orch.db.local.clone();
    let lane = lane.to_string();
    run_db(async move {
        db.query_all(
            "SELECT launch_key FROM runtime_admission_requests WHERE workspace_key=?1 AND resource_lane=?2 AND state='active'",
            (WORKSPACE.to_string(), lane),
            |row| row.text(0),
        )
        .await
        .map(|keys| {
            keys.into_iter()
                .filter_map(|key| key.strip_prefix("ordinary:").map(str::to_string))
                .collect()
        })
        .map_err(|error| error.to_string())
    })
}

/// Every private lane holding a non-terminal admission row, with the budget
/// its promoter last decided against and its current occupancy.
pub(crate) async fn private_lane_budgets(
    orch: &Orchestrator,
) -> crate::storage::DbResult<Vec<LaneBudget>> {
    let occupancy = crate::storage::lane_occupancy(&orch.db.local, WORKSPACE).await?;
    let current_ceiling = orch.get_settings().private_dedicated_process_capacity;
    // Warm-up is one host's, so every lane reports the same window.
    let boot_ramp = boot_ramp_active(orch).then(|| orch.private_lanes.spawn_window());
    // Memory is one host, so every lane's headroom nets out spawns in flight on
    // all of them, as the lease does.
    let host_in_flight = occupancy
        .iter()
        .map(|lane| lane.in_flight_bytes(DEFAULT_SPAWN_RSS))
        .fold(0_u64, u64::saturating_add);
    Ok(occupancy
        .into_iter()
        .map(|lane| {
            let decision = orch.private_lanes.decision(&lane.lane);
            let memory = decision
                .as_ref()
                .and_then(|decision| decision.budget.memory);
            let unmeasured =
                memory.map_or(DEFAULT_SPAWN_RSS, |memory| memory.unmeasured_claim_bytes);
            LaneBudget {
                in_flight_bytes: host_in_flight,
                ceiling: decision
                    .as_ref()
                    .map_or(current_ceiling, |decision| decision.budget.ceiling),
                memory,
                measured_at: decision
                    .as_ref()
                    .map(|decision| decision.budget.measured_at),
                per_process_bytes: lane.typical_claim_bytes.unwrap_or(unmeasured),
                // A recorded shortfall describes a head; with nothing queued
                // there is no head left to describe.
                shortfall: decision
                    .and_then(|decision| decision.shortfall)
                    .filter(|_| lane.queued > 0),
                lane: lane.lane,
                active_units: lane.active_units,
                queued: lane.queued,
                boot_ramp,
            }
        })
        .collect())
}

fn gib(bytes: u64) -> String {
    format!("{:.1} GiB", bytes as f64 / (1u64 << 30) as f64)
}

/// One lane's budget in one line, shared by every surface that displays it so
/// they all describe the same decision in the same words.
pub(crate) fn describe_lane(budget: &LaneBudget, now: i64) -> String {
    let mut line = format!(
        "{}: {} active, {} queued",
        budget.lane, budget.active_units, budget.queued
    );
    line.push_str(&match budget.ceiling {
        Some(ceiling) => format!(", operator ceiling {ceiling}"),
        None => ", no unit ceiling".to_string(),
    });
    if let Some(ramp) = budget.boot_ramp {
        line.push_str(&format!(
            ", boot ramp: {} of {} warming",
            ramp.warming, ramp.window
        ));
    }
    match (budget.memory, budget.headroom_bytes(), budget.measured_at) {
        (Some(memory), Some(headroom), _) => {
            line.push_str(&format!(
                "; memory headroom {} above a {} reserve",
                gib(headroom),
                gib(memory.reserve_bytes)
            ));
            if budget.in_flight_bytes > 0 {
                line.push_str(&format!(
                    " ({} held for spawns in flight)",
                    gib(budget.in_flight_bytes)
                ));
            }
        }
        (_, _, Some(_)) => {
            line.push_str("; host memory unmeasured, so memory does not brake admission")
        }
        (_, _, None) => line.push_str("; no admission pass has measured memory yet"),
    }
    if let Some(measured_at) = budget.measured_at {
        line.push_str(&format!(
            ", measured {}s ago",
            now.saturating_sub(measured_at).max(0)
        ));
    }
    line
}

/// Why a lane's queue head is waiting, as a clause.
pub(crate) fn describe_shortfall(shortfall: &LaneShortfall) -> String {
    match shortfall {
        LaneShortfall::Memory {
            available_bytes,
            reserve_bytes,
            in_flight_bytes,
            head_estimate_bytes,
        } => format!(
            "it needs about {} and only {} is free above the {} reserve",
            gib(*head_estimate_bytes),
            gib(available_bytes
                .saturating_sub(*reserve_bytes)
                .saturating_sub(*in_flight_bytes)),
            gib(*reserve_bytes)
        ),
        LaneShortfall::Units { active, ceiling } => {
            format!(
                "the operator ceiling of {ceiling} resident processes is full ({active} active)"
            )
        }
        LaneShortfall::Streams { active, ceiling } => {
            format!("the stream ceiling of {ceiling} is full ({active} active)")
        }
        LaneShortfall::Ramp { warming, window } => format!(
            "the boot ramp starts background launches only while fewer than {window} spawns are warming up ({warming} are)"
        ),
    }
}

/// Render a job's in-flight ordinary launch from its admission projection, or
/// `None` when the launch is not waiting on admission.
pub(crate) fn render_launch_line(
    projection: &crate::storage::OrdinaryAdmissionProjection,
    now: i64,
) -> Option<String> {
    use crate::storage::RuntimeAdmissionState as State;
    let lane = projection.lane_budget.as_ref();
    let lane_name = lane.map_or("the team workspace", |budget| budget.lane.as_str());
    match projection.state {
        State::Queued => {
            let mut line = match (projection.position, projection.behind_count) {
                (Some(position), Some(behind)) => format!(
                    "Launch: queued for a session slot on {lane_name}, position {position} of {}",
                    position + behind
                ),
                _ => format!("Launch: queued for a session slot on {lane_name}"),
            };
            line.push_str(".\n");
            if let Some(budget) = lane {
                line.push_str(&format!("Lane: {}.\n", describe_lane(budget, now)));
                if let (Some(1), Some(shortfall)) = (projection.position, &budget.shortfall) {
                    line.push_str(&format!(
                        "Waiting because {}.\n",
                        describe_shortfall(shortfall)
                    ));
                }
            }
            Some(line)
        }
        State::Leased | State::Starting => Some(format!(
            "Launch: admitted on {lane_name}; its process is starting.\n"
        )),
        _ => None,
    }
}

/// The launch line for `job_id`, if its latest launch is in ordinary admission.
pub(crate) async fn describe_job_launch(orch: &Orchestrator, job_id: &str) -> Option<String> {
    let now = chrono::Utc::now().timestamp();
    let budgets = private_lane_budgets(orch).await.ok()?;
    let local = crate::storage::ordinary_admission_projections(&orch.db.local, now, &budgets)
        .await
        .ok()?
        .into_iter()
        .find(|projection| projection.job_id == job_id)?;
    let projection = if local.workspace_key.starts_with("team:") {
        // A team mirror stays queued locally after launch and carries only local
        // rank. Postgres says where the request really is, team-wide.
        let client = super::team_admission_lifecycle::team_client(orch).ok()?;
        let status = tokio::time::timeout(Duration::from_secs(5), client.status(&local.request_id))
            .await
            .ok()?;
        crate::runtime_admission::authoritative_team_projection(local, status)?
    } else {
        local
    };
    render_launch_line(&projection, now)
}

/// Promote every private lane that holds admission rows. The process sweep runs
/// this so memory freed by anything other than an agent session (a finished
/// build, a closed browser) still wakes the queue, and so each lane's recorded
/// budget stays fresh for the surfaces that read it.
pub(crate) fn promote_private_lanes_blocking(orch: &Orchestrator) {
    let db = orch.db.local.clone();
    let lanes = match run_db(async move {
        db.query_all(
            "SELECT DISTINCT resource_lane FROM runtime_admission_requests WHERE workspace_key=?1 AND state IN ('queued','leased','starting','active')",
            (WORKSPACE.to_string(),),
            |row| row.text(0),
        )
        .await
        .map_err(|error| error.to_string())
    }) {
        Ok(lanes) => lanes,
        Err(error) => {
            log::warn!("ordinary admission could not list private lanes: {error}");
            return;
        }
    };
    for lane in lanes {
        if let Err(error) = super::ordinary_admission::promote_private_lane_blocking(orch, &lane) {
            log::warn!("ordinary admission lane {lane} stopped promoting: {error}");
        }
    }
}

/// Cancel every queued ordinary request whose run already reached a terminal
/// status or no longer exists. Settlement withdraws the row in the same write,
/// so this backstops rows left by an older build and team runs, whose run lives
/// in a replica the local admission row cannot share a transaction with.
/// Returns how many rows were withdrawn.
pub(crate) fn withdraw_orphaned_ordinary_admissions(orch: &Orchestrator) -> usize {
    let queued = match run_db_borrowed(async {
        crate::storage::list_queued_ordinary_launches(&orch.db.local)
            .await
            .map_err(|error| error.to_string())
    }) {
        Ok(queued) => queued,
        Err(error) => {
            log::warn!("ordinary admission could not list queued launches: {error}");
            return 0;
        }
    };
    let mut withdrawn = 0;
    for launch in queued {
        // An unreachable owning database proves nothing about the run.
        if run_settled(orch, &launch.run_id) != Some(true) {
            continue;
        }
        let retired = if launch.workspace_key.starts_with("team:") {
            // Postgres first: the local mirror is the obligation to retry, so it
            // is retired only once the authority has let go of the request.
            use super::team_admission_lifecycle::{withdraw_settled_team_request, TeamWithdrawal};
            match withdraw_settled_team_request(orch, &launch.request_id) {
                Ok(TeamWithdrawal::Withdrawn) => true,
                Ok(TeamWithdrawal::Pending) => false,
                Err(error) => {
                    log::warn!(
                        "team ordinary admission {} stays queued for retry: {error}",
                        launch.request_id
                    );
                    false
                }
            }
        } else {
            // A private run lives in the local database beside its admission
            // row, so withdrawing the row and returning the wake its turn was
            // to carry are one write: the cancelled row is the last trace this
            // sweep would ever look at, so nothing could retry a revert left
            // for later.
            matches!(
                run_db_borrowed(async {
                    orch.db
                        .local
                        .write(|conn| {
                            let run_id = launch.run_id.clone();
                            Box::pin(async move {
                                let withdrawn = crate::storage::withdraw_queued_ordinary_admission(
                                    conn,
                                    &run_id,
                                    "run settled before admission",
                                    chrono::Utc::now().timestamp(),
                                )
                                .await?;
                                if let Some(withdrawn) = &withdrawn {
                                    crate::orchestrator::attention_push::return_withdrawn_delivery_conn(
                                        conn, withdrawn,
                                    )
                                    .await;
                                }
                                Ok(withdrawn.is_some())
                            })
                        })
                        .await
                        .map_err(|error| error.to_string())
                }),
                Ok(true)
            )
        };
        if retired {
            withdrawn += 1;
        }
    }
    if withdrawn > 0 {
        log::info!("ordinary admission withdrew {withdrawn} queued request(s) whose run had already settled");
    }
    withdrawn
}

/// Whether `run_id` has reached a terminal status or no longer exists. `None`
/// when its owning database cannot be read, which proves nothing either way.
pub(super) fn run_settled(orch: &Orchestrator, run_id: &str) -> Option<bool> {
    let status = run_db_borrowed(async {
        let owning = crate::execution::routing::owning_db_for_run(&orch.db, run_id)
            .await
            .map_err(|error| error.to_string())?;
        owning
            .query_opt_text("SELECT status FROM runs WHERE id=?1", (run_id.to_string(),))
            .await
            .map_err(|error| error.to_string())
    })
    .ok()?;
    Some(!status.is_some_and(|status| matches!(status.as_str(), "starting" | "live")))
}

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: u64 = 1 << 30;

    fn budget(ceiling: Option<u32>, shortfall: Option<LaneShortfall>) -> LaneBudget {
        LaneBudget {
            lane: "claude-cli".into(),
            active_units: 9,
            queued: 2,
            in_flight_bytes: 0,
            ceiling,
            memory: Some(HostMemoryEnvelope {
                available_bytes: 54 * GIB,
                reserve_bytes: 12 * GIB + GIB * 8 / 10,
                unmeasured_claim_bytes: GIB,
            }),
            measured_at: Some(100),
            per_process_bytes: GIB,
            shortfall,
            boot_ramp: None,
        }
    }

    #[test]
    fn a_lane_line_names_the_boot_ramp_while_it_holds_background_launches() {
        let mut ramped = budget(
            None,
            Some(LaneShortfall::Ramp {
                warming: 4,
                window: 4,
            }),
        );
        ramped.boot_ramp = Some(SpawnWindow {
            warming: 4,
            window: 4,
        });
        assert!(
            describe_lane(&ramped, 112).contains(", no unit ceiling, boot ramp: 4 of 4 warming;")
        );
        assert_eq!(
            describe_shortfall(ramped.shortfall.as_ref().unwrap()),
            "the boot ramp starts background launches only while fewer than 4 spawns are warming up (4 are)"
        );
    }

    #[test]
    fn a_lane_line_names_occupancy_ceiling_headroom_and_reserve() {
        assert_eq!(
            describe_lane(&budget(None, None), 112),
            "claude-cli: 9 active, 2 queued, no unit ceiling; memory headroom 41.2 GiB above a 12.8 GiB reserve, measured 12s ago"
        );
        assert!(describe_lane(&budget(Some(4), None), 112).contains(", operator ceiling 4;"));
        let mut unmeasured = budget(None, None);
        unmeasured.memory = None;
        assert!(describe_lane(&unmeasured, 112)
            .contains("host memory unmeasured, so memory does not brake admission"));
    }

    #[test]
    fn a_queued_launch_names_its_position_lane_and_why_the_head_waits() {
        let projection = |position, shortfall| crate::storage::OrdinaryAdmissionProjection {
            workspace_key: WORKSPACE.into(),
            request_id: "req".into(),
            job_id: "job".into(),
            run_id: "run".into(),
            state: crate::storage::RuntimeAdmissionState::Queued,
            reason: None,
            position: Some(position),
            behind_count: Some(3 - position),
            enqueue_age_ms: 0,
            runtime_class: crate::storage::PreparedRuntimeClass::DedicatedProcess,
            effective_claim: None,
            effective_capacity: None,
            lane_budget: Some(budget(Some(9), shortfall)),
            runtime_host: None,
            host_constraint: crate::storage::PreparedRuntimeHostRequirement::None,
        };
        let head = render_launch_line(
            &projection(
                1,
                Some(LaneShortfall::Units {
                    active: 9,
                    ceiling: 9,
                }),
            ),
            112,
        )
        .unwrap();
        assert!(
            head.starts_with("Launch: queued for a session slot on claude-cli, position 1 of 3.\nLane: claude-cli: 9 active"),
            "{head}"
        );
        assert!(
            head.ends_with("Waiting because the operator ceiling of 9 resident processes is full (9 active).\n"),
            "{head}"
        );
        // Only the head is waiting on the shortfall; the rest wait on the head.
        let behind = render_launch_line(
            &projection(
                2,
                Some(LaneShortfall::Units {
                    active: 9,
                    ceiling: 9,
                }),
            ),
            112,
        )
        .unwrap();
        assert!(!behind.contains("Waiting because"), "{behind}");
    }
}
