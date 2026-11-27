//! The runner host's fresh-pipe floor (cairn/6023).
//!
//! Executors gate their own hosts; this gates the one an executor cannot see:
//! the runner's, where every Claude CLI session, dedicated Codex app-server and
//! process-backed call is spawned. The evidence and policy are the shared ones
//! in [`cairn_common::pipe_health`], sampled here on the runner's own clock.
//!
//! While the floor refuses, a launch that would add a process waits queued
//! with a named shortfall (`LaneShortfall::PipeHealth`), whatever memory says.
//! A warm continuation, a stream on a resident server, and a stateless HTTP
//! call add no process and are untouched. When the floor clears, by measured
//! recovery or by evidence expiring into a named gap, every process lane is
//! promoted again, so nothing waits on an unrelated process exit.
//!
//! The floor is not an operator notification: it clears on measurement and
//! waits on nothing the operator can do. Where it shows is the executor page,
//! which renders each engagement from its row.
//!
//! Each engagement is a row (`pipe_floor_engagements`, cairn/6440): a
//! descriptor census taken as the floor engages ([`census`]), then a bounded
//! release of idle sessions through the canonical Keep-policy retire
//! ([`release`]) that keeps releasing until the bar admission reopens on
//! holds, and whose verdict says whether it did. A `not_recovered` verdict disables releases for the build
//! that reached it; every engagement still takes its census. All of it runs
//! on a detached task: the sampler only mints the engagement's id, because
//! its next sample is what detects recovery.

mod census;
mod release;

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cairn_common::executor_protocol::LOCAL_EXECUTOR_NAME;
use cairn_common::pipe_health::{PipeCapacity, PipeHealthAssessment};
use cairn_procmem::identity::StartKey;
use cairn_procmem::{KernelRead, ProcessIdentity};

use self::census::{AdmissionCounts, Attribution, SessionProcess, SessionState};
use self::release::{Candidate, FloorNow, ReleaseRecord, ReleaseWorld, RetireAttempt};
use crate::agent_process::process::WarmProcess;
use crate::orchestrator::lifecycle::{
    retire_unowned_idle, run_sessions, NotRetired, RetireGrounds,
};
use crate::orchestrator::Orchestrator;
use crate::storage::pipe_floor::{self as store, PipeFloorEngagementOpen, Reclamation};
use crate::storage::run_db_blocking;

/// How long the census may take before it reports itself partial.
const CENSUS_DEADLINE: Duration = Duration::from_secs(3);
/// How long a released session's leader has to exit before the experiment
/// stops on an unconfirmed release.
const EXIT_WAIT: Duration = Duration::from_secs(5);

impl Orchestrator {
    /// The runner host's fresh-pipe floor when it refuses new processes now,
    /// assessed at the moment of asking. `None` admits, including under a
    /// named gap (never sampled, not applicable, or stale past the hold).
    pub(crate) fn process_floor_refusal(&self) -> Option<PipeHealthAssessment> {
        let assessment = self.pipe_health.assess_now();
        (!assessment.admits()).then_some(assessment)
    }

    /// The floor's answer now, admitting or not, for the surfaces that show it.
    pub(crate) fn process_floor(&self) -> PipeHealthAssessment {
        self.pipe_health.assess_now()
    }

    /// Start sampling this host. An engagement is recorded and its census
    /// and release pass run detached; a clearance promotes every process lane
    /// at once.
    pub(crate) fn spawn_pipe_floor(&self) {
        let floor = Arc::new(FloorRecord::default());
        // Hashing the runner's executable is unbounded disk I/O: do it now, off
        // the sampler, so no engagement waits on it.
        self.spawn_blocking_detached(|| {
            runner_build_id();
        });
        let orch = self.clone();
        let started = self.pipe_health.spawn_sampler(move |transition| {
            if transition.engaged() {
                log::warn!(
                    "runner host fresh-pipe floor engaged: {}",
                    transition.to.describe()
                );
            } else {
                log::info!(
                    "runner host fresh-pipe floor changed from {} ({}): {}",
                    transition.from.label(),
                    transition.clearance().unwrap_or("still held"),
                    transition.to.describe()
                );
            }
            if transition.engaged() {
                let open = PipeFloorEngagementOpen {
                    id: uuid::Uuid::new_v4().to_string(),
                    host: LOCAL_EXECUTOR_NAME.into(),
                    // Resolved by the detached writers: the sampler never
                    // reads the executable.
                    build_id: String::new(),
                    opened_at_ms: unix_ms() as i64,
                    opening_reading_bytes: transition
                        .to
                        .capacity
                        .map(|capacity| capacity.accepted_bytes as i64),
                    opening_at_least: transition
                        .to
                        .capacity
                        .is_some_and(|capacity| capacity.at_least),
                    opening_assessment: transition.to.describe(),
                };
                *floor.open.lock().unwrap_or_else(|e| e.into_inner()) = Some(open.clone());
                let (orch, floor) = (orch.clone(), floor.clone());
                orch.clone()
                    .spawn_blocking_detached(move || engage(&orch, &floor, open));
            }
            if let Some(clearance) = transition.clearance() {
                let open = floor.open.lock().unwrap_or_else(|e| e.into_inner()).take();
                if let Some(mut open) = open {
                    let (db, clearance) = (orch.db.local.clone(), clearance.to_string());
                    let cleared_at = unix_ms() as i64;
                    orch.spawn_blocking_detached(move || {
                        open.build_id = runner_build_id();
                        let written = run_db_blocking(move || async move {
                            store::clear_engagement(&db, &open, cleared_at, &clearance)
                                .await
                                .map_err(|error| error.to_string())
                        });
                        if let Err(error) = written {
                            log::warn!("could not record the pipe floor's clearance: {error}");
                        }
                    });
                }
            }
            if transition.cleared() {
                let promoter = orch.clone();
                orch.spawn_blocking_detached(move || {
                    crate::execution::jobs::promote_private_lanes_blocking(&promoter);
                    crate::execution::jobs::promote_call_lanes(&promoter);
                });
            }
        });
        if let Err(error) = started {
            log::warn!("could not start the runner's pipe-health sampler; the floor stays unmeasured: {error}");
        }
    }
}

impl Orchestrator {
    /// This host's latest floor engagement as the executor page shows it: its
    /// census, each release and the verdict, and whether releases are on.
    /// Reads rows only; nothing here probes.
    pub(crate) async fn pipe_floor_record(&self) -> String {
        let db = &self.db.local;
        let latest = store::latest_engagement(db, LOCAL_EXECUTOR_NAME).await;
        let disabled =
            store::reclamation_disabled_by(db, LOCAL_EXECUTOR_NAME, &runner_build_id()).await;
        match (latest, disabled) {
            (Ok(latest), Ok(disabled)) => render_engagement(latest.as_ref(), disabled.as_ref()),
            (Err(error), _) | (_, Err(error)) => {
                format!("  - Pipe-floor engagements: could not be read: {error}\n")
            }
        }
    }
}

fn at(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .map(|time| time.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        .unwrap_or_else(|| ms.to_string())
}

fn bytes(value: Option<i64>, at_least: bool) -> String {
    match value {
        Some(value) if at_least => format!("at least {value} B"),
        Some(value) => format!("{value} B"),
        None => "no reading".into(),
    }
}

/// The engagement lines under a host's `New-work admission (fresh pipes)`.
pub(crate) fn render_engagement(
    latest: Option<&(store::PipeFloorEngagement, Vec<store::PipeFloorRelease>)>,
    disabled_by: Option<&store::PipeFloorEngagement>,
) -> String {
    let mut out = String::new();
    let switch = match disabled_by {
        Some(by) => format!(
            "disabled for this build after engagement {}: {}",
            by.open.id,
            by.reclamation_detail.as_deref().unwrap_or("not recovered")
        ),
        None => format!(
            "enabled (bounded: {} per engagement, {} per {} s)",
            release::MAX_RELEASES_PER_ENGAGEMENT,
            release::MAX_RELEASES_PER_WINDOW,
            release::RELEASE_WINDOW_MS / 1_000
        ),
    };
    out.push_str(&format!("  - Pipe-driven warm reclamation: {switch}\n"));
    let Some((engagement, releases)) = latest else {
        out.push_str("  - Floor engagements: never engaged on this host\n");
        return out;
    };
    let open = &engagement.open;
    let state = match (&engagement.cleared_at_ms, &engagement.clearance) {
        (Some(cleared), Some(clearance)) => format!("cleared {} by {clearance}", at(*cleared)),
        _ => "still engaged".into(),
    };
    out.push_str(&format!(
        "  - Latest engagement `{}`: engaged {} at {}; {state}\n",
        open.id,
        at(open.opened_at_ms),
        bytes(open.opening_reading_bytes, open.opening_at_least)
    ));
    match engagement
        .census_json
        .as_deref()
        .map(serde_json::from_str::<census::CensusSummary>)
    {
        Some(Ok(summary)) if summary.supported => {
            out.push_str(&format!(
                "    - Census: {} pipe descriptors across {} processes{}, taken in {} ms; agent \
                 processes {} active, {} idle, {} protected, {} pooled Codex threads\n",
                summary.pipe_fds,
                summary.processes,
                if summary.partial {
                    format!(" (partial; {} unreadable)", summary.unreadable)
                } else {
                    String::new()
                },
                summary.took_ms,
                summary.admission.active,
                summary.admission.idle,
                summary.admission.protected,
                summary.admission.pooled_codex_threads
            ));
            let families: Vec<String> = summary
                .families
                .iter()
                .map(|family| {
                    format!(
                        "{} {} ({} processes)",
                        family.family, family.pipe_fds, family.processes
                    )
                })
                .collect();
            out.push_str(&format!("    - By family: {}\n", families.join(", ")));
            let sessions: Vec<String> = summary
                .sessions
                .iter()
                .filter(|session| session.pipe_fds > 0)
                .map(|session| {
                    format!(
                        "run `{}` ({}, {:?}{}) {}",
                        session.run_id,
                        session.family,
                        session.state,
                        session
                            .job_id
                            .as_deref()
                            .map(|job| format!(", job `{job}`"))
                            .unwrap_or_default(),
                        session.pipe_fds
                    )
                })
                .collect();
            if !sessions.is_empty() {
                out.push_str(&format!("    - By session: {}\n", sessions.join("; ")));
            }
        }
        Some(Ok(_)) => out.push_str("    - Census: not supported on this platform\n"),
        Some(Err(error)) => out.push_str(&format!("    - Census: unreadable ({error})\n")),
        None => out.push_str("    - Census: not recorded yet\n"),
    }
    for release in releases {
        let delta = release
            .census_delta_json
            .as_deref()
            .and_then(|json| serde_json::from_str::<Vec<census::FamilyDelta>>(json).ok())
            .and_then(|deltas| deltas.into_iter().next())
            .map(|delta| {
                format!(
                    "; largest move {} {}→{}",
                    delta.family, delta.before, delta.after
                )
            })
            .unwrap_or_default();
        let survivors = release
            .surviving_descendants_json
            .as_deref()
            .and_then(|json| serde_json::from_str::<Vec<release::SurvivingProcess>>(json).ok())
            .filter(|survivors| !survivors.is_empty())
            .map(|survivors| {
                format!(
                    "; {} descendant(s) still alive holding {} pipe descriptors",
                    survivors.len(),
                    survivors
                        .iter()
                        .map(|survivor| u64::from(survivor.pipe_fds))
                        .sum::<u64>()
                )
            })
            .unwrap_or_default();
        out.push_str(&format!(
            "    - Release {}: run `{}` ({}): {} → {}, leader exit {}{delta}{survivors}\n",
            release.seq,
            release.run_id,
            release.backend.as_deref().unwrap_or("unknown backend"),
            bytes(release.before_bytes, false),
            bytes(release.after_bytes, release.after_at_least),
            if release.leader_exit_confirmed {
                "confirmed"
            } else {
                "not confirmed"
            }
        ));
    }
    out.push_str(&format!(
        "    - Release verdict: {}{}\n",
        engagement.reclamation.replace('_', " "),
        engagement
            .reclamation_detail
            .as_deref()
            .map(|detail| format!("; {detail}"))
            .unwrap_or_default()
    ));
    out
}

/// What the floor's detached work shares across engagements.
#[derive(Default)]
struct FloorRecord {
    /// The open engagement, for its clearance.
    open: Mutex<Option<PipeFloorEngagementOpen>>,
    /// Release times, so the per-window bound spans engagements. Held for a
    /// whole pass, so passes run one at a time: an engagement that opens
    /// while an earlier pass is still ending waits for it rather than
    /// releasing nothing, since the floor re-engages one sample after it
    /// clears when queued launches take the room.
    window: Mutex<VecDeque<u64>>,
}

fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

/// The running runner's build: a `not_recovered` verdict disables releases
/// only for the build that reached it.
fn runner_build_id() -> String {
    static BUILD: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    BUILD
        .get_or_init(|| {
            cairn_common::build_identity::current_executable_build_id()
                .unwrap_or_else(|error| format!("unknown ({error})"))
        })
        .clone()
}

fn store_write<F, Fut>(what: &str, write: F)
where
    F: FnOnce() -> Fut + Send,
    Fut: std::future::Future<Output = crate::storage::DbResult<()>>,
{
    if let Err(error) =
        run_db_blocking(move || async move { write().await.map_err(|error| error.to_string()) })
    {
        log::warn!("pipe floor: could not record {what}: {error}");
    }
}

/// Persist an engagement, take its census, and run (or skip) its release
/// pass. Runs on a detached blocking task.
fn engage(orch: &Orchestrator, floor: &FloorRecord, mut open: PipeFloorEngagementOpen) {
    open.build_id = runner_build_id();
    let db = orch.db.local.clone();
    let id = open.id.clone();
    store_write("an engagement", || {
        let (db, open) = (db.clone(), open.clone());
        async move { store::open_engagement(&db, &open).await }
    });
    let census = take_census(orch);
    match &census {
        Ok((attribution, admission)) => {
            let summary = attribution.summary(*admission);
            let json = serde_json::to_string(&summary).unwrap_or_default();
            store_write("a census", || {
                let (db, id) = (db.clone(), id.clone());
                async move { store::record_census(&db, &id, &json, unix_ms() as i64).await }
            });
        }
        Err(error) => {
            log::warn!("pipe floor engagement {id}: the descriptor census failed: {error}")
        }
    }
    let (state, detail) = release_pass(orch, floor, &open, census.ok().map(|(a, _)| a));
    store_write("a reclamation verdict", || {
        let (db, id, detail) = (db.clone(), id.clone(), detail.clone());
        async move { store::set_reclamation(&db, &id, state, Some(&detail)).await }
    });
}

/// Decide whether this engagement releases anything, and run the pass if so.
fn release_pass(
    orch: &Orchestrator,
    floor: &FloorRecord,
    open: &PipeFloorEngagementOpen,
    census: Option<Attribution>,
) -> (Reclamation, String) {
    let mut window = floor.window.lock().unwrap_or_else(|e| e.into_inner());
    let db = orch.db.local.clone();
    let (host, build) = (open.host.clone(), open.build_id.clone());
    let disabled = run_db_blocking(move || async move {
        store::reclamation_disabled_by(&db, &host, &build)
            .await
            .map_err(|error| error.to_string())
    });
    match disabled {
        Ok(Some(by)) => {
            return (
                Reclamation::Skipped,
                format!(
                    "disabled after engagement {}: {}",
                    by.open.id,
                    by.reclamation_detail.unwrap_or_default()
                ),
            )
        }
        Ok(None) => {}
        Err(error) => {
            return (
                Reclamation::Skipped,
                format!("could not read whether releases are disabled: {error}"),
            )
        }
    }
    let Some(census) = census else {
        return (
            Reclamation::Skipped,
            "no census to measure releases against".into(),
        );
    };
    store_write("a running pass", || {
        let (db, id) = (orch.db.local.clone(), open.id.clone());
        async move { store::set_reclamation(&db, &id, Reclamation::Running, None).await }
    });
    let mut world = LiveWorld {
        orch,
        engagement_id: open.id.clone(),
    };
    let result = release::run_release_pass(&mut world, census, &mut window);
    (result.verdict, result.detail)
}

/// The descriptor census attributed to this runner's registered sessions,
/// with session admission's counts.
fn take_census(orch: &Orchestrator) -> Result<(Attribution, AdmissionCounts), String> {
    let registered = orch.process_state.registered_processes();
    let sessions_by_run = run_sessions(
        orch,
        registered
            .iter()
            .map(|process| process.run_id.clone())
            .collect(),
    );
    let sessions: Vec<SessionProcess> = registered
        .iter()
        .map(|process| SessionProcess {
            run_id: process.run_id.clone(),
            job_id: process.job_id.clone(),
            session_id: sessions_by_run
                .get(&process.run_id)
                .map(|(session, _)| session.clone()),
            family: census::backend_family(process.backend.as_deref()),
            state: if process.pooled {
                SessionState::Pooled
            } else if process.warm {
                SessionState::Idle
            } else {
                SessionState::Active
            },
            leader: process.leader.as_ref().and_then(leader_identity),
        })
        .collect();
    let fds = cairn_procmem::fd_census(Instant::now() + CENSUS_DEADLINE);
    let attribution = census::attribute(&fds, &sessions, std::process::id());
    let warm = orch.process_state.warm_processes();
    let protected = match orch.warm_gc.clone() {
        Some(gc) => {
            let dbs = orch.db.clone();
            let probed = warm.clone();
            run_db_blocking(move || async move {
                Ok::<_, String>(gc.protected_count(&dbs, &probed).await)
            })?
        }
        None => 0,
    };
    let admission = AdmissionCounts {
        active: orch.process_state.active_process_count(),
        idle: warm.len(),
        protected,
        pooled_codex_threads: registered.iter().filter(|process| process.pooled).count(),
    };
    Ok((attribution, admission))
}

fn leader_identity(
    anchor: &cairn_common::process_identity::ProcessAnchor,
) -> Option<ProcessIdentity> {
    Some(ProcessIdentity {
        pid: anchor.pid,
        start_time: StartKey::parse(&anchor.start_token)?,
    })
}

/// The release experiment against this runner.
struct LiveWorld<'a> {
    orch: &'a Orchestrator,
    engagement_id: String,
}

impl ReleaseWorld for LiveWorld<'_> {
    fn floor(&mut self) -> FloorNow {
        let assessment = self.orch.pipe_health.assess_now();
        FloorNow {
            cleared: assessment.admits(),
            phase: assessment.hold_phase(),
        }
    }

    fn probe(&mut self) -> Result<PipeCapacity, String> {
        let sample = cairn_common::pipe_health::sample_fresh_pipe(unix_ms());
        match sample.value() {
            Some(capacity) => Ok(*capacity),
            None => Err(sample
                .gap()
                .map_or("no reading".to_string(), |gap| gap.describe().to_string())),
        }
    }

    fn census(&mut self) -> Result<Attribution, String> {
        take_census(self.orch).map(|(attribution, _)| attribution)
    }

    fn candidates(&mut self) -> Vec<Candidate> {
        let Some(gc) = self.orch.warm_gc.clone() else {
            return Vec::new();
        };
        // Pooled Codex threads are a measurability filter applied after the
        // owner test, not a second definition of owned: their teardown frees
        // no process of their own, so there is no leader exit to await.
        let registered: Vec<_> = self
            .orch
            .process_state
            .registered_processes()
            .into_iter()
            .filter(|process| process.warm && !process.pooled)
            .filter_map(|process| {
                let leader = process.leader.as_ref().and_then(leader_identity)?;
                Some((process, leader))
            })
            .collect();
        let warm: Vec<WarmProcess> = registered
            .iter()
            .map(|(process, leader)| WarmProcess {
                run_id: process.run_id.clone(),
                pid: Some(leader.pid),
                seconds_since_activity: 0,
                job_id: process.job_id.clone(),
            })
            .collect();
        let seconds: std::collections::HashMap<String, u64> = self
            .orch
            .process_state
            .warm_processes()
            .into_iter()
            .map(|process| (process.run_id, process.seconds_since_activity))
            .collect();
        let warm: Vec<WarmProcess> = warm
            .into_iter()
            .map(|mut process| {
                process.seconds_since_activity =
                    seconds.get(&process.run_id).copied().unwrap_or_default();
                process
            })
            .collect();
        let dbs = self.orch.db.clone();
        let order =
            run_db_blocking(
                move || async move { Ok::<_, String>(gc.release_order(&dbs, &warm).await) },
            )
            .unwrap_or_default();
        order
            .into_iter()
            .filter_map(|run_id| {
                let (process, leader) = registered
                    .iter()
                    .find(|(process, _)| process.run_id == run_id)?;
                Some(Candidate {
                    run_id,
                    job_id: process.job_id.clone(),
                    session_id: None,
                    family: census::backend_family(process.backend.as_deref()),
                    leader: *leader,
                })
            })
            .collect()
    }

    fn retire(&mut self, candidate: &Candidate) -> RetireAttempt {
        match retire_unowned_idle(self.orch, &candidate.run_id, RetireGrounds::PipePressure) {
            Ok(_) => RetireAttempt::Released,
            Err(NotRetired::TeardownFailed(error)) => RetireAttempt::Failed(error),
            Err(other) => RetireAttempt::NotReleasable(other.describe()),
        }
    }

    fn await_exit(&mut self, leader: ProcessIdentity) -> bool {
        let deadline = Instant::now() + EXIT_WAIT;
        loop {
            match cairn_procmem::platform::read_kernel_state(leader.pid) {
                KernelRead::Gone => return true,
                KernelRead::Live(process) if process.start != leader.start_time => return true,
                _ => {}
            }
            if Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    fn active_sessions(&mut self) -> usize {
        self.orch.process_state.active_process_count()
    }

    fn wait(&mut self, ms: u64) {
        std::thread::sleep(Duration::from_millis(ms));
    }

    fn now_ms(&mut self) -> u64 {
        unix_ms()
    }

    fn record(&mut self, release: &ReleaseRecord) {
        let row = store::PipeFloorRelease {
            engagement_id: self.engagement_id.clone(),
            seq: release.seq as i64,
            run_id: release.candidate.run_id.clone(),
            session_id: release.candidate.session_id.clone(),
            job_id: release.candidate.job_id.clone(),
            backend: Some(release.candidate.family.clone()),
            released_at_ms: release.released_at_ms as i64,
            leader_exit_confirmed: release.leader_exit_confirmed,
            before_bytes: Some(release.before.accepted_bytes as i64),
            after_bytes: release.after.map(|after| after.accepted_bytes as i64),
            after_at_least: release.after.is_some_and(|after| after.at_least),
            census_delta_json: serde_json::to_string(&release.family_delta).ok(),
            surviving_descendants_json: serde_json::to_string(&release.surviving_descendants).ok(),
            outcome: release.outcome.clone(),
        };
        let db = self.orch.db.local.clone();
        store_write("a release", || async move {
            store::insert_release(&db, &row).await
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_page_shows_the_engagement_census_releases_and_verdict() {
        let open = PipeFloorEngagementOpen {
            id: "e1".into(),
            host: "local".into(),
            build_id: "b1".into(),
            opened_at_ms: 0,
            opening_reading_bytes: Some(512),
            opening_at_least: false,
            opening_assessment: "pipe health: degraded".into(),
        };
        let summary = census::CensusSummary {
            supported: true,
            processes: 152,
            pipe_fds: 754,
            took_ms: 40,
            families: vec![census::FamilyCount {
                family: "claude-cli".into(),
                processes: 30,
                pipe_fds: 162,
            }],
            admission: census::AdmissionCounts {
                active: 2,
                idle: 9,
                protected: 1,
                pooled_codex_threads: 0,
            },
            ..census::CensusSummary::default()
        };
        let engagement = store::PipeFloorEngagement {
            open,
            cleared_at_ms: None,
            clearance: None,
            census_json: Some(serde_json::to_string(&summary).unwrap()),
            census_at_ms: Some(10),
            reclamation: "not_recovered".into(),
            reclamation_detail: Some("3 releases, 512→512 B".into()),
        };
        let release = store::PipeFloorRelease {
            engagement_id: "e1".into(),
            seq: 1,
            run_id: "run-a".into(),
            session_id: None,
            job_id: None,
            backend: Some("claude-cli".into()),
            released_at_ms: 20,
            leader_exit_confirmed: true,
            before_bytes: Some(512),
            after_bytes: Some(512),
            after_at_least: false,
            census_delta_json: Some(r#"[{"family":"claude-cli","before":162,"after":150}]"#.into()),
            surviving_descendants_json: Some(r#"[{"pid":7,"family":"node","pipe_fds":6}]"#.into()),
            outcome: "released".into(),
        };
        let page = render_engagement(Some(&(engagement.clone(), vec![release])), None);
        for expected in [
            "Pipe-driven warm reclamation: enabled (bounded: 10 per engagement, 3 per 15 s)",
            "Latest engagement `e1`",
            "at 512 B; still engaged",
            "754 pipe descriptors across 152 processes",
            "2 active, 9 idle, 1 protected",
            "claude-cli 162 (30 processes)",
            "Release 1: run `run-a` (claude-cli): 512 B → 512 B, leader exit confirmed",
            "largest move claude-cli 162→150",
            "1 descendant(s) still alive holding 6 pipe descriptors",
            "Release verdict: not recovered; 3 releases, 512→512 B",
        ] {
            assert!(page.contains(expected), "{expected}\n{page}");
        }
        let disabled = render_engagement(None, Some(&engagement));
        assert!(disabled.contains("disabled for this build after engagement e1: 3 releases"));
        assert!(disabled.contains("never engaged"));
    }
}
