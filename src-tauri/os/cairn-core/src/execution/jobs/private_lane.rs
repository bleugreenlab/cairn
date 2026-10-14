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

use super::{run_db, run_db_borrowed};
use crate::agent_process::gc::SpawnRoom;
use crate::orchestrator::Orchestrator;
use crate::storage::{
    HostMemoryEnvelope, LaneBudget, LaneShortfall, RowExt, RuntimeResourceCapacity,
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

/// The last decision each private lane's promoter made. Surfaces read this
/// rather than probing, so what they display is what was decided.
#[derive(Default)]
pub struct PrivateLaneLedger {
    decisions: Mutex<HashMap<String, LaneDecision>>,
    memory_evictions: Mutex<HashMap<String, Instant>>,
}

impl PrivateLaneLedger {
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
        LaneShortfall::Streams { .. } => Vec::new(),
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
            matches!(
                run_db_borrowed(async {
                    crate::storage::cancel_queued_admission(
                        &orch.db.local,
                        &launch.request_id,
                        Some("run settled before admission"),
                        chrono::Utc::now().timestamp(),
                    )
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
        }
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
