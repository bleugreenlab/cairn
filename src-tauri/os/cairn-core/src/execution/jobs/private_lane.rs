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
//!
//! ## One pass per lane
//!
//! Promotion is triggered from many places (an enqueue, a release, a finalize,
//! the pipe floor recovering, the process sweep), each of which used to start
//! a pass of its own. Passes on one lane all claim through the lane's one
//! cursor row, so parallel passes only conflict and park writers. Every pass
//! therefore enters the lane's [`LaneSingleFlight`] first: a trigger that finds
//! a pass running marks it to run once more and returns at once, and the
//! running pass checks that mark under the same lock it ends under, so a
//! trigger is never lost between the queue draining and the pass ending.
//! However many triggers land during a pass, exactly one more pass runs.

use super::{run_db, run_db_borrowed};
use crate::agent_process::gc::SpawnRoom;
use crate::orchestrator::Orchestrator;
use crate::resume_timing::ResumeTimingEvent;
use crate::storage::{
    HostMemoryEnvelope, LaneBudget, LaneShortfall, RowExt, RuntimeResourceCapacity, SpawnWindow,
};
use cairn_session::agent_process::gc::{memory_reserve, DEFAULT_SPAWN_RSS};
use std::collections::{HashMap, HashSet};
use std::sync::{Condvar, Mutex};
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
/// on, so both measure the same way; the orchestrator's host probe otherwise.
fn host_memory(orch: &Orchestrator) -> Option<crate::agent_process::memory::SystemMemory> {
    match orch.warm_gc.as_ref() {
        Some(gc) => gc.system_memory(),
        None => orch.host_memory.system_memory(),
    }
}

/// The host memory envelope every process lane on this runner admits against,
/// measured now: ordinary sessions and ephemeral calls share it, so the host
/// has one budget and one reserve. `None` when the host cannot be measured.
pub(crate) fn host_memory_envelope(orch: &Orchestrator) -> Option<HostMemoryEnvelope> {
    host_memory(orch).map(|system| HostMemoryEnvelope {
        available_bytes: system.available,
        reserve_bytes: memory_reserve(system.total),
        unmeasured_claim_bytes: DEFAULT_SPAWN_RSS,
    })
}

/// Measure the budget for one promotion pass: the operator's optional ceiling
/// and the host memory envelope, with the moment it was measured.
pub(crate) fn private_lane_budget(orch: &Orchestrator) -> PrivateLaneBudget {
    PrivateLaneBudget {
        ceiling: orch.get_settings().private_dedicated_process_capacity,
        memory: host_memory_envelope(orch),
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

/// The last decision each of this host's process lanes made: the private
/// ordinary-session lanes and the ephemeral-call lanes that share their memory
/// envelope. Surfaces read this rather than probing, so what they display is
/// what was decided.
#[derive(Default)]
pub struct PrivateLaneLedger {
    /// Keyed by `(workspace, lane)`: a lane name alone is ambiguous, since
    /// `claude-cli` is a lane of both the ordinary-session workspace and the
    /// call workspace.
    decisions: Mutex<HashMap<(String, String), LaneDecision>>,
    memory_evictions: Mutex<HashMap<String, Instant>>,
    boot_ramp: Mutex<BootRamp>,
    /// Serializes, across every pass on every private lane, a ramp pass's
    /// window read, the lease it decides, and the warming registration that
    /// lease implies. Without it, concurrent passes (a restart's enqueue burst
    /// fires one each) all read the same count and lease past the window.
    ramp_admission: Mutex<()>,
    /// Which lanes have a promotion pass running. See [`LaneSingleFlight`].
    passes: LaneSingleFlight,
}

/// At most one promotion pass per `(workspace, lane)` at a time, with every
/// trigger that arrives during a pass folded into one more run of it.
#[derive(Default)]
pub(crate) struct LaneSingleFlight {
    /// A lane is present while a pass on it runs; its value is whether a
    /// trigger arrived since the pass last checked.
    running: Mutex<HashMap<(String, String), bool>>,
    /// Signalled whenever a pass ends.
    ended: Condvar,
}

impl LaneSingleFlight {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<(String, String), bool>> {
        self.running
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Start a pass on the lane, or, when one is already running, mark it to
    /// run again and return `None`: the running pass covers this trigger.
    pub(crate) fn enter(&self, workspace: &str, lane: &str) -> Option<LanePass<'_>> {
        let key = (workspace.to_string(), lane.to_string());
        let mut running = self.lock();
        if let Some(rerun) = running.get_mut(&key) {
            *rerun = true;
            return None;
        }
        running.insert(key.clone(), false);
        Some(LanePass {
            flight: self,
            key,
            ended: false,
        })
    }

    /// Wait until no pass runs on any lane, or `limit` passes.
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn wait_idle(&self, limit: Duration) {
        let running = self.lock();
        let _ = self
            .ended
            .wait_timeout_while(running, limit, |running| !running.is_empty());
    }
}

/// A running promotion pass. Dropping it ends the pass, so one that unwinds or
/// returns early never leaves its lane marked running.
pub(crate) struct LanePass<'a> {
    flight: &'a LaneSingleFlight,
    key: (String, String),
    ended: bool,
}

impl LanePass<'_> {
    /// Called when the pass has drained what it can. `true` when a trigger
    /// arrived meanwhile: the mark is cleared and the caller runs once more.
    /// `false` ends the pass, under the same lock as the check, so a trigger
    /// either lands before it (and is served) or finds no pass and starts one.
    pub(crate) fn again(&mut self) -> bool {
        let mut running = self.flight.lock();
        if let Some(rerun) = running.get_mut(&self.key) {
            if *rerun {
                *rerun = false;
                return true;
            }
        }
        running.remove(&self.key);
        self.ended = true;
        drop(running);
        self.flight.ended.notify_all();
        false
    }
}

impl Drop for LanePass<'_> {
    fn drop(&mut self) {
        if !self.ended {
            self.flight.lock().remove(&self.key);
            self.flight.ended.notify_all();
        }
    }
}

impl PrivateLaneLedger {
    /// Enter the lane's single-flight promotion. See [`LaneSingleFlight::enter`].
    pub(crate) fn enter_pass(&self, workspace: &str, lane: &str) -> Option<LanePass<'_>> {
        self.passes.enter(workspace, lane)
    }

    /// Wait for every running promotion pass to end, so a test that drives
    /// promotion sees the outcome of a pass a background trigger was running.
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn wait_for_passes(&self) {
        self.passes.wait_idle(Duration::from_secs(30));
    }

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
        workspace: &str,
        lane: &str,
        budget: PrivateLaneBudget,
        shortfall: Option<LaneShortfall>,
    ) {
        if let Ok(mut decisions) = self.decisions.lock() {
            decisions.insert(
                (workspace.to_string(), lane.to_string()),
                LaneDecision { budget, shortfall },
            );
        }
    }

    fn decision(&self, workspace: &str, lane: &str) -> Option<LaneDecision> {
        self.decisions
            .lock()
            .ok()?
            .get(&(workspace.to_string(), lane.to_string()))
            .cloned()
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
        // Evicting frees memory or a unit, never a warm-up slot or readiness.
        // Nor, as far as anything has measured, pipe capacity: no eviction is
        // credited with relieving the fresh-pipe floor until an experiment
        // shows idle sessions hold the budget (cairn/6023).
        LaneShortfall::Streams { .. }
        | LaneShortfall::Ramp { .. }
        | LaneShortfall::StartupRecovery
        | LaneShortfall::PipeHealth { .. } => Vec::new(),
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
    private_lane_budgets_from(
        &orch.db.local,
        &orch.private_lanes,
        orch.get_settings().private_dedicated_process_capacity,
        orch.boot_at,
    )
    .await
}

pub(crate) async fn private_lane_budgets_from(
    db: &crate::storage::LocalDb,
    ledger: &PrivateLaneLedger,
    current_ceiling: Option<u32>,
    boot_at: i64,
) -> crate::storage::DbResult<Vec<LaneBudget>> {
    let occupancy = crate::storage::lane_occupancy(db, WORKSPACE).await?;
    let boot_ramp = (chrono::Utc::now().timestamp()
        < boot_at.saturating_add(BOOT_RAMP_HORIZON.as_secs() as i64))
    .then(|| ledger.spawn_window());
    // Memory is one host, so every lane's headroom nets out spawns in flight on
    // all of them, ephemeral calls included, as the lease does.
    let host_in_flight = crate::storage::host_in_flight_bytes(db, DEFAULT_SPAWN_RSS).await?;
    Ok(occupancy
        .into_iter()
        .map(|lane| {
            let decision = ledger.decision(WORKSPACE, &lane.lane);
            lane_budget(lane, decision, host_in_flight, current_ceiling, boot_ramp)
        })
        .collect())
}

/// One lane's occupancy joined with the decision its promoter last recorded.
/// `current_ceiling` stands in for the bound when no pass has decided yet.
fn lane_budget(
    lane: crate::storage::LaneOccupancy,
    decision: Option<LaneDecision>,
    host_in_flight: u64,
    current_ceiling: Option<u32>,
    boot_ramp: Option<SpawnWindow>,
) -> LaneBudget {
    let memory = decision
        .as_ref()
        .and_then(|decision| decision.budget.memory);
    let unmeasured = memory.map_or(DEFAULT_SPAWN_RSS, |memory| memory.unmeasured_claim_bytes);
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
        // A recorded shortfall describes a head; with nothing queued there is
        // no head left to describe.
        shortfall: decision
            .and_then(|decision| decision.shortfall)
            .filter(|_| lane.queued > 0),
        lane: lane.lane,
        active_units: lane.active_units,
        queued: lane.queued,
        boot_ramp,
    }
}

/// An ephemeral-call lane's occupancy and the last decision its promoter
/// recorded. The call promoter records only a lease or a hold, never an empty
/// queue, so a recorded decision with no shortfall is an admission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CallLaneBudget {
    /// `ceiling` is the count bound the last lease applied: the fan-out cap on
    /// a measured host, the unmeasured-host bound otherwise. `shortfall` is
    /// why the head was held, while it still waits.
    pub budget: LaneBudget,
    /// The last pass leased the lane's head.
    pub admitted: bool,
}

/// Every ephemeral-call lane holding a non-terminal admission row, with the
/// last decision its promoter recorded. Read from the ledger the lease writes,
/// never a fresh probe, and never part of the provider snapshot.
pub(crate) async fn call_lane_budgets_from(
    db: &crate::storage::LocalDb,
    ledger: &PrivateLaneLedger,
) -> crate::storage::DbResult<Vec<CallLaneBudget>> {
    let workspace = crate::storage::CALL_ADMISSION_WORKSPACE;
    let occupancy = crate::storage::lane_occupancy(db, workspace).await?;
    let host_in_flight = crate::storage::host_in_flight_bytes(db, DEFAULT_SPAWN_RSS).await?;
    Ok(occupancy
        .into_iter()
        .map(|lane| {
            let decision = ledger.decision(workspace, &lane.lane);
            let admitted = decision
                .as_ref()
                .is_some_and(|decision| decision.shortfall.is_none());
            CallLaneBudget {
                budget: lane_budget(lane, decision, host_in_flight, None, None),
                admitted,
            }
        })
        .collect())
}

pub(crate) async fn call_lane_budgets(
    orch: &Orchestrator,
) -> crate::storage::DbResult<Vec<CallLaneBudget>> {
    call_lane_budgets_from(&orch.db.local, &orch.private_lanes).await
}

fn gib(bytes: u64) -> String {
    format!("{:.1} GiB", bytes as f64 / (1u64 << 30) as f64)
}

/// One lane's budget in one line, shared by every surface that displays it so
/// they all describe the same decision in the same words.
pub(crate) fn describe_lane(budget: &LaneBudget, now: i64) -> String {
    let bound = match budget.ceiling {
        Some(ceiling) => format!(", operator ceiling {ceiling}"),
        None => ", no unit ceiling".to_string(),
    };
    lane_line(&budget.lane, &bound, budget, now)
}

/// An ephemeral-call lane's budget in the same shape as [`describe_lane`],
/// named apart from the ordinary-session lane that shares its lane name.
pub(crate) fn describe_call_lane(budget: &LaneBudget, now: i64) -> String {
    let bound = match (budget.measured_at, budget.ceiling, budget.memory) {
        // No pass has decided yet, so the bound it would apply is not known.
        (None, _, _) => String::new(),
        (Some(_), Some(cap), Some(_)) => format!(", fan-out cap {cap}"),
        (Some(_), Some(bound), None) => format!(", at most {bound} while memory is unmeasured"),
        (Some(_), None, _) => ", no count bound".to_string(),
    };
    lane_line(
        &format!("{} ephemeral calls", budget.lane),
        &bound,
        budget,
        now,
    )
}

fn lane_line(name: &str, bound: &str, budget: &LaneBudget, now: i64) -> String {
    let mut line = format!(
        "{name}: {} active, {} queued",
        budget.active_units, budget.queued
    );
    line.push_str(bound);
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
            interactive_room_bytes,
        } => {
            let mut clause = format!(
                "it needs about {} and only {} is free above the {} reserve",
                gib(*head_estimate_bytes),
                gib(available_bytes
                    .saturating_sub(*reserve_bytes)
                    .saturating_sub(*in_flight_bytes)),
                gib(*reserve_bytes)
            );
            if *interactive_room_bytes > 0 {
                clause.push_str(&format!(
                    ", of which it leaves {} for ordinary sessions",
                    gib(*interactive_room_bytes)
                ));
            }
            clause
        }
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
        LaneShortfall::StartupRecovery => {
            "the host is still finishing startup recovery, and its agent callback gateway refuses calls until it does".to_string()
        }
        LaneShortfall::PipeHealth { assessment } => format!(
            "this host refuses new processes, whatever memory is free ({})",
            assessment.describe()
        ),
    }
}

/// Render a job's in-flight ordinary launch from its admission projection, or
/// `None` when the launch is not waiting on admission.
pub(crate) fn render_launch_line(
    projection: &crate::storage::ProviderAdmissionRecord,
) -> Option<String> {
    use crate::storage::RuntimeAdmissionState as State;
    let lane_name = &projection.resource_lane;
    match projection.state {
        Some(State::Queued) => {
            let mut line = match (projection.position, projection.behind_count) {
                (Some(position), Some(behind)) => format!(
                    "Launch: queued for a session slot on {lane_name}, position {position} of {}",
                    position + behind
                ),
                _ => format!("Launch: queued for a session slot on {lane_name}"),
            };
            line.push_str(".\n");
            if projection.reason
                == Some(crate::storage::RuntimeAdmissionReason::WaitingForStartupRecovery)
            {
                line.push_str(&format!(
                    "Waiting because {}.\n",
                    describe_shortfall(&LaneShortfall::StartupRecovery)
                ));
            }
            Some(line)
        }
        Some(State::Leased | State::Starting) => Some(format!(
            "Launch: admitted on {lane_name}; its process is starting.\n"
        )),
        _ => None,
    }
}

/// The launch line for `job_id`, if its latest launch is in ordinary admission.
pub(crate) async fn describe_job_launch(orch: &Orchestrator, job_id: &str) -> Option<String> {
    let published = crate::runtime_admission::published_provider_admission(orch)
        .await
        .ok()?;
    let projection = published
        .snapshot
        .records
        .iter()
        .find(|record| record.owner.job_id.as_deref() == Some(job_id))?;
    if projection.state.is_none() {
        return Some(
            "Launch: provider admission authority unavailable; live state is unknown.\n".into(),
        );
    }
    render_launch_line(projection)
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

/// [`promote_private_lanes_blocking`] in the background. A finished ephemeral
/// call frees memory, and ordinary sessions are first to it.
pub(crate) fn promote_private_lanes(orch: &Orchestrator) {
    let promoter = orch.clone();
    orch.spawn_blocking_detached(move || promote_private_lanes_blocking(&promoter));
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
            orch.db.local.provider_admission_changed();
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

    #[test]
    fn concurrent_triggers_on_a_lane_run_one_pass_and_exactly_one_more() {
        let flight = LaneSingleFlight::default();
        let mut pass = flight.enter("w", "lane").expect("the first trigger runs");
        for _ in 0..10 {
            assert!(flight.enter("w", "lane").is_none());
        }
        // Another lane, and the same lane name in another workspace, run beside it.
        let other = flight.enter("w", "other").expect("lanes are independent");
        let call = flight
            .enter("calls", "lane")
            .expect("workspaces are independent");
        drop((other, call));
        assert!(pass.again(), "triggers during the pass earn one more run");
        assert!(!pass.again(), "and only one");
        assert!(
            flight.enter("w", "lane").is_some(),
            "the ended pass freed the lane"
        );
    }

    #[test]
    fn a_dropped_pass_frees_its_lane() {
        let flight = LaneSingleFlight::default();
        let pass = flight.enter("w", "lane").unwrap();
        assert!(flight.enter("w", "lane").is_none());
        drop(pass);
        assert!(flight.enter("w", "lane").is_some());
    }

    /// A trigger racing the pass's final check is never lost: either the pass
    /// sees its mark and runs again, or the trigger finds no pass and runs
    /// its own. Every trigger's work is covered by a pass that starts after it.
    #[test]
    fn a_trigger_racing_the_end_of_a_pass_is_never_lost() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::{Arc, Barrier};
        for _ in 0..2_000 {
            let flight = Arc::new(LaneSingleFlight::default());
            let served = Arc::new(AtomicUsize::new(0));
            let barrier = Arc::new(Barrier::new(2));
            let mut pass = flight.enter("w", "lane").unwrap();
            let trigger = {
                let (flight, served, barrier) = (flight.clone(), served.clone(), barrier.clone());
                std::thread::spawn(move || {
                    barrier.wait();
                    if let Some(mut own) = flight.enter("w", "lane") {
                        served.fetch_add(1, Ordering::SeqCst);
                        while own.again() {}
                    }
                })
            };
            barrier.wait();
            while pass.again() {
                served.fetch_add(1, Ordering::SeqCst);
            }
            trigger.join().unwrap();
            assert_eq!(
                served.load(Ordering::SeqCst),
                1,
                "the trigger was served once"
            );
        }
    }

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
    fn a_batch_shortfall_names_the_room_it_leaves_for_ordinary_sessions() {
        let shortfall = LaneShortfall::Memory {
            available_bytes: 14 * GIB,
            reserve_bytes: 12 * GIB,
            in_flight_bytes: 0,
            head_estimate_bytes: GIB / 2,
            interactive_room_bytes: GIB,
        };
        assert_eq!(
            describe_shortfall(&shortfall),
            "it needs about 0.5 GiB and only 2.0 GiB is free above the 12.0 GiB reserve, of which it leaves 1.0 GiB for ordinary sessions"
        );
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
    fn a_queued_launch_names_its_position_and_lane_without_host_telemetry() {
        let projection = |position, reason| crate::storage::ProviderAdmissionRecord {
            authority_key: "private".into(),
            workspace_id: WORKSPACE.into(),
            resource_lane: "claude-cli".into(),
            request_id: "req".into(),
            owner: Default::default(),
            enqueue_sequence: Some(position),
            enqueued_at_unix_ms: Some(0),
            lane_order: Some(position),
            state: Some(crate::storage::RuntimeAdmissionState::Queued),
            reason,
            position: Some(position),
            behind_count: Some(3 - position),
            runtime_class: crate::storage::PreparedRuntimeClass::DedicatedProcess,
            effective_claim: None,
            effective_capacity: None,
            runtime_host: None,
            host_constraint: Some(crate::storage::PreparedRuntimeHostRequirement::None),
            session_id: None,
            model: None,
            account_id: None,
            provider: None,
        };
        assert_eq!(
            render_launch_line(&projection(1, None)).unwrap(),
            "Launch: queued for a session slot on claude-cli, position 1 of 3.\n"
        );
        assert_eq!(
            render_launch_line(&projection(2, None)).unwrap(),
            "Launch: queued for a session slot on claude-cli, position 2 of 3.\n"
        );
        assert_eq!(
            render_launch_line(&projection(
                2,
                Some(crate::storage::RuntimeAdmissionReason::WaitingForStartupRecovery),
            ))
            .unwrap(),
            "Launch: queued for a session slot on claude-cli, position 2 of 3.\nWaiting because the host is still finishing startup recovery, and its agent callback gateway refuses calls until it does.\n"
        );
    }
}
