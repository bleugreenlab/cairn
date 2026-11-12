//! The history pack pass: resolved executions and whole closed thread-session
//! ranges before their applied compaction boundary, while the app runs.
//!
//! It deletes nothing. Hot rows stay authoritative and `history_tier.evicted_at`
//! stays null, so every reader keeps reading hot (see `HistoryReader`); the
//! catalog rows it writes are what eviction (C2) will act on.
//!
//! ## Shape
//!
//! Candidates are executions whose issue is `merged` or `closed` and resolved
//! more than the window ago, with no catalog row from this resolution, walked
//! in `(completed_at, execution_id)` order a page at a time. The position is
//! saved in `archival_progress` after each one and cleared when the walk ends,
//! so the next pass resumes mid-walk, and a finished walk starts over and
//! retries what it skipped. An issue reopened and resolved again has a later
//! `completed_at` than its catalog row's `resolved_at`, so it is packed again.
//!
//! For each execution, the pass:
//!
//! 1. Checks the store answers, then checks the archive volume against the
//!    free-space floor (at least [`PackBudget::floor_free_pct`] of the volume
//!    and [`PackBudget::floor_free_bytes`]) and stops the pass below it.
//!    Reconstruction degrades to a stub when the store fails, so packing
//!    against an offline archive would preserve stubs; and an archive whose
//!    volume is not mounted has no free space of its own to measure.
//! 2. Takes the archival permit and holds it for this execution only, so a
//!    teardown waits behind at most one execution.
//! 3. Skips the execution if any of its runs is still live.
//! 4. Reads it a keyset page at a time, each page its own short read
//!    (`HistoryPacker::read_next`), and does the CPU work of each page
//!    (reconstruction, the quarantine gate, encoding, compression) on the
//!    blocking pool inside an `archival/pack` runtime region. An execution
//!    whose pack grows past [`PackBudget::max_pack_bytes`], or with one row
//!    too large for a frame, is skipped by name, never truncated.
//! 5. Checks the store again, assembles the object on the blocking pool, and
//!    publishes it: put, read back, verify, then commit the catalog rows.
//!
//! Between executions it yields and folds the logical log by the shared rule:
//! past [`crate::storage::FOLD_AT_LOG_BYTES`] and no checkpoint in the engine.
//! The pass stops once it has run for [`PackBudget::pass_time`].
//!
//! Each packed execution logs one line; each pass logs a [`PackReport`].

use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use cairn_db::turso::params;

use super::pacing::{acquire_gate, fold_if_due, FoldTally};
use super::progress::{self, Progress};
use crate::storage::history_pack::thread_range::{self, RangeCandidate};
#[path = "pack_threads.rs"]
mod threads;
use crate::storage::content_store::ContentStore;
use crate::storage::history_pack::catalog::{publish_to, PublishError, PublishFacts};
use crate::storage::history_pack::write::{BuiltPack, HistoryPacker, PackBuildError, PackUnit};
use crate::storage::history_pack::{PackError, FRAME_RAW_BYTES};
use crate::storage::{DbResult, LocalDb, RowExt, FOLD_AT_LOG_BYTES};

/// How long after boot the first pass waits: clear of the boot convoy and of
/// the offload's first pass. The two serialize on the archival permit anyway.
pub const PACK_FIRST_PASS_DELAY: Duration = Duration::from_secs(30 * 60);

/// The environment variable an operator sets to stop the pass without a
/// rebuild. Read once, when the archival maintenance loop is spawned; see
/// [`super::switch_enabled`] for the values that turn it off.
pub const PACK_ENV: &str = "CAIRN_HISTORY_PACK";

/// The largest pack the pass writes. A pack is assembled whole in memory
/// before it is stored, so this bounds the pass's footprint.
pub const PACK_MAX_BYTES: u64 = 1024 * 1024 * 1024;

/// The estimate of one `events` row's hot cost when neither the settings nor a
/// census give one: the 2026-10-30 census calibration.
pub const FALLBACK_BYTES_PER_EVENT_ROW: u64 = 2_323;

/// Candidate executions read per page.
const CANDIDATE_PAGE: i64 = 50;

/// The most candidates the remaining-work probe counts.
const REMAINING_PROBE: usize = 1_000;

/// The most skipped or failed executions a summary names.
const NAMES_REPORTED: usize = 20;

/// How long one pass may run, how large a pack may be, and when it stops for
/// space.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PackBudget {
    pub pass_time: Duration,
    pub max_pack_bytes: u64,
    pub fold_at_log_bytes: u64,
    /// The archive volume must keep at least this many bytes free...
    pub floor_free_bytes: u64,
    /// ...and at least this percentage of its capacity.
    pub floor_free_pct: u64,
    /// The most scopes (execution or thread range) one pass packs. Unbounded in production; a
    /// rehearsal sets it to measure a fixed sample.
    pub max_executions: usize,
}

impl PackBudget {
    /// The floor follows from the pass's own bounds. Between two floor checks
    /// the pass writes at most one pack (at most [`PACK_MAX_BYTES`], staged
    /// under the store's `tmp/` and renamed into place) and folds a log of
    /// [`FOLD_AT_LOG_BYTES`], on a volume the database may share. Four packs'
    /// worth covers that with room to spare for whatever else writes to the
    /// volume between checks; 1% keeps the pass proportionally clear of a
    /// large volume's last few percent, where filesystems slow down. On a
    /// 1.86 TB disk that is 18.6 GiB, the scale of the executor's own critical
    /// line (16 GiB), not a share sized to exceed what a busy host has free.
    pub const PRODUCTION: Self = Self {
        pass_time: Duration::from_secs(30 * 60),
        max_pack_bytes: PACK_MAX_BYTES,
        fold_at_log_bytes: FOLD_AT_LOG_BYTES,
        floor_free_bytes: 4 * PACK_MAX_BYTES,
        floor_free_pct: 1,
        max_executions: usize::MAX,
    };

    /// The free bytes a volume of `total_bytes` must keep.
    pub fn floor_for(&self, total_bytes: u64) -> u64 {
        self.floor_free_bytes
            .max(total_bytes / 100 * self.floor_free_pct)
    }
}

/// Which executions are old enough to pack, and how their hot cost is
/// estimated. Built from the `archive` settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PackPolicy {
    /// How long after resolution an execution stays out of the pass.
    pub window_secs: i64,
    /// An explicit per-row estimate; otherwise the latest census, otherwise
    /// [`FALLBACK_BYTES_PER_EVENT_ROW`].
    pub bytes_per_row: Option<u64>,
}

/// One scope passed over, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Passed {
    /// The catalog scope: an execution's id is its scope id.
    pub scope_id: String,
    pub reason: String,
}

/// Work and elapsed budget charged to one candidate source.
#[derive(Debug, Default, Clone)]
pub struct SourceReport {
    pub packed: usize,
    pub skipped: usize,
    pub failed: usize,
    pub remaining: Option<usize>,
    pub duration: Duration,
}

/// What one pass did.
#[derive(Debug, Default, Clone)]
pub struct PackReport {
    pub executions: SourceReport,
    pub threads: SourceReport,
    /// Unevicted ranges examined for newly arrived or reopened work.
    pub staleness_probes: usize,
    pub stale_ranges: usize,
    pub packed: usize,
    pub pack_bytes: u64,
    /// The stored size of the packed executions' events, as read.
    pub hot_bytes: u64,
    /// Their estimated hot cost: event rows times the per-row estimate.
    pub hot_estimate_bytes: u64,
    pub event_rows: u64,
    pub bytes_per_row: u64,
    /// Passed over this pass and retried by a later one: a live run, a pack
    /// over the size bound, a row too large for a frame.
    pub skipped: Vec<Passed>,
    /// Could not be packed for a reason a later pass may not share: a build
    /// or verification failure. Nothing was catalogued for them.
    pub failed: Vec<Passed>,
    pub pages: u64,
    /// Each catalog statement's duration, in milliseconds.
    pub write_ms: Vec<u64>,
    pub gate_wait: Duration,
    pub gate_wait_max: Duration,
    /// The longest the pass held the archival permit, and for which scope.
    pub hold_max: Duration,
    pub hold_max_scope: Option<String>,
    /// The largest pack written, and for which scope.
    pub largest: Option<(String, u64)>,
    pub fold: FoldTally,
    pub remaining: Option<usize>,
    /// Why the pass ended before its walk did.
    pub stopped: Option<String>,
    /// Whether that was a failure (the store, the database) rather than the
    /// pass's own budget or floor.
    pub halted: bool,
    pub duration: Duration,
}

fn mib(bytes: u64) -> String {
    format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0))
}

fn names(passed: &[Passed]) -> String {
    let mut out = passed
        .iter()
        .take(NAMES_REPORTED)
        .map(|p| format!("{} ({})", p.scope_id, p.reason))
        .collect::<Vec<_>>()
        .join(", ");
    if passed.len() > NAMES_REPORTED {
        out.push_str(&format!(", and {} more", passed.len() - NAMES_REPORTED));
    }
    out
}

impl PackReport {
    fn write_percentile(&self, pct: usize) -> u64 {
        if self.write_ms.is_empty() {
            return 0;
        }
        let mut sorted = self.write_ms.clone();
        sorted.sort_unstable();
        sorted[(sorted.len() - 1) * pct / 100]
    }
}

impl fmt::Display for PackReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} scopes packed ({} packed vs {} hot, estimate {} at {} B/row, {} events), \
             {} skipped, {} failed in {} pages; catalog writes p50 {} ms, max {} ms; \
             gate wait {} ms (max {} ms), held max {} ms",
            self.packed,
            mib(self.pack_bytes),
            mib(self.hot_bytes),
            mib(self.hot_estimate_bytes),
            self.bytes_per_row,
            self.event_rows,
            self.skipped.len(),
            self.failed.len(),
            self.pages,
            self.write_percentile(50),
            self.write_percentile(100),
            self.gate_wait.as_millis(),
            self.gate_wait_max.as_millis(),
            self.hold_max.as_millis(),
        )?;
        if let Some(scope) = &self.hold_max_scope {
            write!(f, " ({scope})")?;
        }
        if let Some((scope, bytes)) = &self.largest {
            write!(f, "; largest {} ({scope})", mib(*bytes))?;
        }
        write!(
            f,
            "; {} folds (max {} ms, {} skipped, {} failed); log peak {}; remaining {} candidates; {:.1} s",
            self.fold.folds,
            self.fold.max.as_millis(),
            self.fold.skipped,
            self.fold.failed,
            mib(self.fold.log_peak_bytes),
            match self.remaining {
                Some(n) if n > REMAINING_PROBE => format!("more than {REMAINING_PROBE}"),
                Some(n) => n.to_string(),
                None => "unknown".to_string(),
            },
            self.duration.as_secs_f64(),
        )?;
        for (name, source) in [
            ("executions", &self.executions),
            ("thread ranges", &self.threads),
        ] {
            write!(
                f,
                "; {name}: {} packed, {} skipped, {} failed, remaining {}, budget {:.1} s",
                source.packed,
                source.skipped,
                source.failed,
                source
                    .remaining
                    .map_or("unknown".into(), |n| if n > REMAINING_PROBE {
                        format!("more than {REMAINING_PROBE}")
                    } else {
                        n.to_string()
                    }),
                source.duration.as_secs_f64()
            )?;
        }
        write!(
            f,
            "; staleness probes {}, invalidated {}",
            self.staleness_probes, self.stale_ranges
        )?;
        if !self.skipped.is_empty() {
            write!(f, "; skipped: {}", names(&self.skipped))?;
        }
        if !self.failed.is_empty() {
            write!(f, "; failed: {}", names(&self.failed))?;
        }
        if let Some(stopped) = &self.stopped {
            write!(f, "; stopped: {stopped}")?;
        }
        Ok(())
    }
}

/// One lock per database (keyed by [`LocalDb::instance`]) serializing its
/// passes, so two never walk and advance the same cursor at once.
static PASS_LOCKS: LazyLock<std::sync::Mutex<HashMap<u64, Arc<tokio::sync::Mutex<()>>>>> =
    LazyLock::new(Default::default);

/// Scopes whose skip will recur until they change: a pack over the size bound,
/// a row too large for a frame. Keyed by database and scope id, with the
/// resolution the skip was for, so a later resolution is tried again. Kept for
/// the life of the process: a restart tries each once more.
type StandingSkips = HashMap<(u64, String), (i64, String)>;
static STANDING_SKIPS: LazyLock<std::sync::Mutex<StandingSkips>> = LazyLock::new(Default::default);

fn pass_lock(db: &LocalDb) -> Arc<tokio::sync::Mutex<()>> {
    PASS_LOCKS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .entry(db.instance())
        .or_default()
        .clone()
}

fn standing_skip(db: &LocalDb, scope_id: &str, resolved_at: i64) -> Option<String> {
    STANDING_SKIPS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(&(db.instance(), scope_id.to_string()))
        .filter(|(skipped_at, _)| *skipped_at == resolved_at)
        .map(|(_, reason)| reason.clone())
}

fn remember_skip(db: &LocalDb, scope_id: &str, resolved_at: i64, reason: &str) {
    STANDING_SKIPS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(
            (db.instance(), scope_id.to_string()),
            (resolved_at, reason.to_string()),
        );
}

/// An execution old enough to pack.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Candidate {
    completed_at: i64,
    execution_id: String,
}

/// Why one execution was not packed.
enum ExecutionError {
    /// Passed over; a later pass retries it. `standing` skips recur until the
    /// execution changes, so this process does not read it again.
    Skip { reason: String, standing: bool },
    /// Its pack could not be built or verified. Nothing was catalogued.
    Fail(String),
    /// The store or the database failed: end the pass.
    Halt(String),
}

/// Run one pack pass now.
pub async fn run_pack_pass(db: &LocalDb, policy: PackPolicy, budget: PackBudget) -> PackReport {
    run_pack_pass_at(db, chrono::Utc::now().timestamp(), policy, budget).await
}

/// One pack pass at an explicit clock.
pub async fn run_pack_pass_at(
    db: &LocalDb,
    now: i64,
    policy: PackPolicy,
    budget: PackBudget,
) -> PackReport {
    let started = Instant::now();
    let mut report = PackReport::default();
    report.fold.observe(db);
    let cutoff = now.saturating_sub(policy.window_secs);
    let lock = pass_lock(db);
    let _serialized = lock.lock().await;
    match (db.team_content_store(), db.content_store().cloned()) {
        (Some(_), _) => {
            report.stopped = Some("a team replica has no history tier".into());
        }
        (None, None) => {
            report.stopped = Some("this database has no archive store".into());
            report.halted = true;
        }
        (None, Some(store)) => {
            report.bytes_per_row = bytes_per_row(db, policy).await;
            if let Err(stop) = walk(
                db,
                store.as_ref(),
                now,
                cutoff,
                budget,
                started,
                &mut report,
            )
            .await
            {
                report.stopped = Some(stop.reason);
                report.halted = stop.halted;
            }
        }
    }
    report.executions.remaining = probe_remaining(db, cutoff).await;
    report.threads.remaining = threads::probe_remaining(
        db,
        cutoff,
        budget,
        report.bytes_per_row,
        report.stopped.is_none(),
    )
    .await;
    report.remaining = report
        .executions
        .remaining
        .zip(report.threads.remaining)
        .map(|(executions, threads)| executions.saturating_add(threads));
    report.duration = started.elapsed();
    report
}

struct Stop {
    reason: String,
    halted: bool,
}

impl Stop {
    fn halt(reason: impl Into<String>) -> Self {
        Stop {
            reason: reason.into(),
            halted: true,
        }
    }

    fn budget(reason: impl Into<String>) -> Self {
        Stop {
            reason: reason.into(),
            halted: false,
        }
    }
}

/// The per-row hot estimate: the setting, else the latest census, else the
/// fallback.
async fn bytes_per_row(db: &LocalDb, policy: PackPolicy) -> u64 {
    if let Some(explicit) = policy.bytes_per_row {
        return explicit;
    }
    match crate::storage::census::latest_bytes_per_event_row(db).await {
        Ok(Some((per_row, _))) if per_row.is_finite() && per_row > 0.0 => per_row.round() as u64,
        _ => FALLBACK_BYTES_PER_EVENT_ROW,
    }
}

/// Executions lead, but may use only two thirds before threads get a turn.
/// Once threads finish their walk, executions may spend the unused reservation.
async fn walk(
    db: &LocalDb,
    store: &dyn ContentStore,
    now: i64,
    cutoff: i64,
    budget: PackBudget,
    started: Instant,
    report: &mut PackReport,
) -> Result<(), Stop> {
    check_archive(store, budget).await?;
    let phase = Instant::now();
    let result = walk_executions(
        db,
        store,
        now,
        cutoff,
        budget,
        started + execution_deadline(budget.pass_time),
        report,
    )
    .await;
    report.executions.duration += phase.elapsed();
    let executions_done = result?;
    let phase = Instant::now();
    let result = threads::walk(db, store, now, cutoff, budget, started, report).await;
    report.threads.duration += phase.elapsed();
    let threads_done = result?;
    let mut executions_done = executions_done;
    if !executions_done && threads_done && started.elapsed() < budget.pass_time {
        let phase = Instant::now();
        let result = walk_executions(
            db,
            store,
            now,
            cutoff,
            budget,
            started + budget.pass_time,
            report,
        )
        .await;
        report.executions.duration += phase.elapsed();
        executions_done = result?;
    }
    if !executions_done || !threads_done {
        return Err(Stop::budget(format!(
            "pass budget of {} s spent; both cursors resume next pass",
            budget.pass_time.as_secs()
        )));
    }
    Ok(())
}

fn execution_deadline(pass_time: Duration) -> Duration {
    pass_time - pass_time / 3
}

async fn walk_executions(
    db: &LocalDb,
    store: &dyn ContentStore,
    now: i64,
    cutoff: i64,
    budget: PackBudget,
    deadline: Instant,
    report: &mut PackReport,
) -> Result<bool, Stop> {
    let saved = progress::load(db, progress::PACK)
        .await
        .map_err(|error| Stop::halt(format!("loading the pack cursor: {error}")))?;
    let mut after = match (saved.watermark, saved.cursor) {
        (Some(completed_at), Some(execution_id)) => Some(Candidate {
            completed_at,
            execution_id,
        }),
        _ => None,
    };
    loop {
        let page = load_candidates(db, cutoff, after.as_ref())
            .await
            .map_err(|error| Stop::halt(format!("loading pack candidates: {error}")))?;
        if page.is_empty() {
            // The end: start over next pass, so a skipped execution is retried.
            save_cursor(db, None).await?;
            return Ok(true);
        }
        for candidate in page {
            if Instant::now() >= deadline {
                return Ok(false);
            }
            check_archive(store, budget).await?;
            let before = (report.packed, report.skipped.len(), report.failed.len());
            pack_candidate(db, store, now, budget, &candidate, report).await?;
            report.executions.packed += report.packed - before.0;
            report.executions.skipped += report.skipped.len() - before.1;
            report.executions.failed += report.failed.len() - before.2;
            save_cursor(db, Some(&candidate)).await?;
            after = Some(candidate);
            tokio::task::yield_now().await;
            fold_if_due(
                db,
                budget.fold_at_log_bytes,
                &mut report.fold,
                "history pack",
            )
            .await;
            if report.packed >= budget.max_executions {
                return Err(Stop::budget(format!(
                    "the limit of {} executions is reached",
                    budget.max_executions
                )));
            }
        }
    }
}

/// Pack one candidate and account for it. Only a halt ends the walk.
async fn pack_candidate(
    db: &LocalDb,
    store: &dyn ContentStore,
    now: i64,
    budget: PackBudget,
    candidate: &Candidate,
    report: &mut PackReport,
) -> Result<(), Stop> {
    pack_unit_candidate(
        db,
        store,
        now,
        budget,
        &WorkUnit {
            unit: PackUnit::Execution(candidate.execution_id.clone()),
            resolved_at: candidate.completed_at,
            thread: None,
            label: format!("execution {}", candidate.execution_id),
        },
        report,
    )
    .await
}

struct WorkUnit {
    unit: PackUnit,
    resolved_at: i64,
    thread: Option<RangeCandidate>,
    label: String,
}

/// Both sources use the same gate, builder, publication and accounting.
async fn pack_unit_candidate(
    db: &LocalDb,
    store: &dyn ContentStore,
    now: i64,
    budget: PackBudget,
    work: &WorkUnit,
    report: &mut PackReport,
) -> Result<(), Stop> {
    let execution_id = work.unit.scope().id;
    let resolved_at = work.resolved_at;
    let label = &work.label;
    if let Some(reason) = standing_skip(db, &execution_id, resolved_at) {
        report.skipped.push(Passed {
            scope_id: execution_id,
            reason,
        });
        return Ok(());
    }
    let mut gate_wait = Duration::ZERO;
    let permit = acquire_gate(&mut gate_wait).await.map_err(Stop::halt)?;
    let held_since = Instant::now();
    let outcome = pack_unit(db, store, now, budget, work, report).await;
    drop(permit);
    let held = held_since.elapsed();
    report.gate_wait += gate_wait;
    report.gate_wait_max = report.gate_wait_max.max(gate_wait);
    if held > report.hold_max {
        report.hold_max = held;
        report.hold_max_scope = Some(execution_id.clone());
    }
    match outcome {
        Ok(packed) => {
            log::info!(
                "history pack: {label} {} runs, {} events: {} packed vs {} hot (estimate {} at {} B/row) in {} ms, {} pages, gate wait {} ms, held {} ms",
                packed.runs,
                packed.event_rows,
                mib(packed.bytes),
                mib(packed.hot_bytes),
                mib(packed.hot_estimate),
                report.bytes_per_row,
                packed.took.as_millis(),
                packed.pages,
                gate_wait.as_millis(),
                held.as_millis(),
            );
            report.packed += 1;
            report.pack_bytes += packed.bytes;
            report.hot_bytes += packed.hot_bytes;
            report.hot_estimate_bytes += packed.hot_estimate;
            report.event_rows += packed.event_rows;
            if report
                .largest
                .as_ref()
                .is_none_or(|(_, largest)| packed.bytes > *largest)
            {
                report.largest = Some((execution_id, packed.bytes));
            }
            Ok(())
        }
        Err(ExecutionError::Skip { reason, standing }) => {
            log::info!("history pack: {label} skipped: {reason}");
            if standing {
                remember_skip(db, &execution_id, resolved_at, &reason);
            }
            report.skipped.push(Passed {
                scope_id: execution_id,
                reason,
            });
            Ok(())
        }
        Err(ExecutionError::Fail(reason)) => {
            log::warn!("history pack: {label} not packed: {reason}");
            report.failed.push(Passed {
                scope_id: execution_id,
                reason,
            });
            Ok(())
        }
        Err(ExecutionError::Halt(reason)) => Err(Stop::halt(format!("{execution_id}: {reason}"))),
    }
}

/// What packing one execution produced.
struct Packed {
    runs: usize,
    event_rows: u64,
    bytes: u64,
    hot_bytes: u64,
    hot_estimate: u64,
    pages: u64,
    took: Duration,
}

/// Build, verify and catalog one execution's pack. Called holding the
/// archival permit.
async fn pack_unit(
    db: &LocalDb,
    store: &dyn ContentStore,
    now: i64,
    budget: PackBudget,
    work: &WorkUnit,
    report: &mut PackReport,
) -> Result<Packed, ExecutionError> {
    let started = Instant::now();
    let unit = &work.unit;
    if let Some(candidate) = &work.thread {
        validate_range(db, candidate, None).await?;
    }
    if let PackUnit::Execution(execution_id) = unit {
        if has_live_run(db, execution_id)
            .await
            .map_err(|error| ExecutionError::Halt(format!("checking for live runs: {error}")))?
        {
            return Err(ExecutionError::Skip {
                reason: "a run is still live".into(),
                standing: false,
            });
        }
    }
    ensure_online(store).await?;
    let mut packer = match HistoryPacker::start(db, unit).await {
        Ok(packer) => packer,
        Err(PackBuildError::NotFound(_)) => {
            return Err(ExecutionError::Skip {
                reason: "its scope owner row is gone".into(),
                standing: false,
            })
        }
        Err(PackBuildError::Db(error)) => {
            return Err(ExecutionError::Halt(format!(
                "reading its skeleton: {error}"
            )))
        }
        Err(PackBuildError::Pack(error)) => return Err(pack_error(error)),
    };
    let mut pages = 0u64;
    while let Some(page) = packer
        .read_next(db)
        .await
        .map_err(|error| ExecutionError::Halt(format!("reading a page: {error}")))?
    {
        pages += 1;
        let (returned, absorbed) = tokio::task::spawn_blocking(move || {
            let _region = cairn_platform::runtime_regions::enter("archival/pack");
            let absorbed = packer.absorb(page);
            (packer, absorbed)
        })
        .await
        .map_err(|error| ExecutionError::Fail(format!("the page task failed: {error}")))?;
        packer = returned;
        absorbed.map_err(pack_error)?;
        if packer.held_bytes() > budget.max_pack_bytes {
            return Err(oversize(budget, packer.held_bytes(), pages));
        }
        report.fold.observe(db);
        tokio::task::yield_now().await;
    }
    report.pages += pages;
    // Reconstruction stubs a body it cannot fetch, so an archive that went
    // offline during the reads leaves stubs that must not be packed.
    ensure_online(store).await?;
    let runs = packer.run_count();
    let built: BuiltPack = tokio::task::spawn_blocking(move || {
        let _region = cairn_platform::runtime_regions::enter("archival/pack");
        packer.finish()
    })
    .await
    .map_err(|error| ExecutionError::Fail(format!("the assembly task failed: {error}")))?
    .map_err(pack_error)?;
    let bytes = built.bytes.len() as u64;
    if bytes > budget.max_pack_bytes {
        return Err(oversize(budget, bytes, pages));
    }
    if let Some(candidate) = &work.thread {
        validate_range(db, candidate, Some(&built.manifest)).await?;
    }
    let event_rows = built.stats.event_rows();
    let hot_estimate = event_rows.saturating_mul(report.bytes_per_row);
    let published = publish_to(
        db,
        store,
        &built,
        PublishFacts {
            hot_bytes_estimate: i64::try_from(hot_estimate).unwrap_or(i64::MAX),
            resolved_at: work.resolved_at,
            packed_at: now,
        },
    )
    .await
    .map_err(publish_error)?;
    report.write_ms.extend(
        published
            .catalog_writes
            .iter()
            .map(|write| write.as_millis() as u64),
    );
    Ok(Packed {
        runs,
        event_rows,
        bytes,
        hot_bytes: built.stats.hot_event_bytes,
        hot_estimate,
        pages,
        took: started.elapsed(),
    })
}

async fn validate_range(
    db: &LocalDb,
    candidate: &RangeCandidate,
    manifest: Option<&crate::storage::history_pack::Manifest>,
) -> Result<(), ExecutionError> {
    match thread_range::validate_candidate(db, candidate, manifest).await {
        Ok(None) => Ok(()),
        Ok(Some(reason)) => Err(ExecutionError::Skip {
            reason,
            standing: manifest.is_some(),
        }),
        Err(error) => Err(ExecutionError::Halt(format!(
            "checking thread range closure: {error}"
        ))),
    }
}

fn oversize(budget: PackBudget, bytes: u64, pages: u64) -> ExecutionError {
    ExecutionError::Skip {
        reason: format!(
            "over the {} pack bound ({} after {pages} pages)",
            mib(budget.max_pack_bytes),
            mib(bytes)
        ),
        standing: true,
    }
}

/// A row too large for any frame is a standing skip; any other pack error is
/// a failure of this attempt; an offline archive ends the pass.
fn pack_error(error: PackError) -> ExecutionError {
    match error {
        PackError::Offline { .. } => ExecutionError::Halt(error.to_string()),
        PackError::Corrupt(message) if message.contains("exceeds FRAME_RAW_BYTES") => {
            ExecutionError::Skip {
                reason: format!(
                    "a row is larger than one frame ({} of JSONL): {message}",
                    mib(FRAME_RAW_BYTES as u64)
                ),
                standing: true,
            }
        }
        other => ExecutionError::Fail(other.to_string()),
    }
}

fn publish_error(error: PublishError) -> ExecutionError {
    match error {
        PublishError::Evicted { .. } | PublishError::Claimed { .. } => ExecutionError::Skip {
            reason: error.to_string(),
            standing: false,
        },
        PublishError::Pack(PackError::Offline { .. })
        | PublishError::NoStore
        | PublishError::Db(_) => ExecutionError::Halt(error.to_string()),
        PublishError::ReadBackMissing | PublishError::Mismatch(_) | PublishError::Pack(_) => {
            ExecutionError::Fail(error.to_string())
        }
    }
}

/// A key no object has. A file store answers `Offline` for it when its root has
/// lost its marker, and `None` otherwise.
const PROBE_KEY: &str = "0000000000000000000000000000000000000000000000000000000000000000";

async fn ensure_online(store: &dyn ContentStore) -> Result<(), ExecutionError> {
    store
        .get(PROBE_KEY)
        .await
        .map(drop)
        .map_err(|error| ExecutionError::Halt(error.to_string()))
}

/// Halt the pass if the archive is offline, and stop it if the archive's
/// volume is under the floor. Liveness comes first: `volume::space` answers
/// for the nearest existing ancestor, so a root on an unmounted volume would
/// otherwise be measured against whichever disk holds its mount point.
async fn check_archive(store: &dyn ContentStore, budget: PackBudget) -> Result<(), Stop> {
    store
        .get(PROBE_KEY)
        .await
        .map_err(|error| Stop::halt(error.to_string()))?;
    let Some(root) = store.local_root() else {
        return Ok(());
    };
    let root: PathBuf = root.to_path_buf();
    let probed = root.clone();
    let space = tokio::task::spawn_blocking(move || {
        // The store answered, but the root can still go between the probe
        // and here; never let the ancestor rule measure another disk.
        if !probed.is_dir() {
            return Ok(None);
        }
        cairn_common::volume::space(&probed).map(Some)
    })
    .await
    .map_err(|error| Stop::halt(format!("the free-space probe failed: {error}")))?
    .map_err(|error| Stop::halt(format!("reading free space at {}: {error}", root.display())))?;
    match space {
        Some(space) => floor_verdict(&root, space, budget),
        None => Err(Stop::halt(format!("archive offline: {}", root.display()))),
    }
}

/// Stop the pass if `space` is under the floor. Never writes toward a full
/// disk: on a volume this full the pass stops at once, and the reason says
/// where the archive is so an operator can move it.
fn floor_verdict(
    root: &Path,
    space: cairn_common::volume::VolumeSpace,
    budget: PackBudget,
) -> Result<(), Stop> {
    let floor = budget.floor_for(space.total_bytes);
    if space.free_bytes < floor {
        return Err(Stop::budget(format!(
            "archive volume under its floor: {} free of {} at {}, the pass keeps {} free (the larger of {}% and {}); \
             nothing more is packed until the volume has room or archive.path names another volume",
            gib(space.free_bytes),
            gib(space.total_bytes),
            root.display(),
            gib(floor),
            budget.floor_free_pct,
            gib(budget.floor_free_bytes),
        )));
    }
    Ok(())
}

fn gib(bytes: u64) -> String {
    format!("{:.1} GiB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
}

async fn save_cursor(db: &LocalDb, at: Option<&Candidate>) -> Result<(), Stop> {
    progress::save(
        db,
        progress::PACK,
        &Progress {
            watermark: at.map(|candidate| candidate.completed_at),
            cursor: at.map(|candidate| candidate.execution_id.clone()),
        },
    )
    .await
    .map_err(|error| Stop::halt(format!("saving the pack cursor: {error}")))
}

/// Executions resolved before `cutoff` with no catalog row from this
/// resolution. `issues` is scanned and sorted (it is small, and this is the
/// archival sweep's shape), then each issue's jobs are found through
/// `idx_jobs_issue_id` and the catalog is probed by primary key: an execution
/// scope's id is the execution's, and a thread range's (`<job>/<n>-<m>`) can
/// never equal one.
pub(crate) const CANDIDATES_SQL: &str = "SELECT DISTINCT i.completed_at, j.execution_id
     FROM issues i JOIN jobs j ON j.issue_id = i.id
     WHERE i.status IN ('merged', 'closed')
       AND i.completed_at IS NOT NULL
       AND i.completed_at < ?1
       AND (i.completed_at > ?2 OR (i.completed_at = ?2 AND j.execution_id > ?3))
       AND j.execution_id IS NOT NULL
       AND NOT EXISTS (
           SELECT 1 FROM history_tier t
           WHERE t.scope_id = j.execution_id
             AND (t.resolved_at >= i.completed_at OR t.evicted_at IS NOT NULL))
     ORDER BY i.completed_at ASC, j.execution_id ASC
     LIMIT ?4";

async fn load_candidates(
    db: &LocalDb,
    cutoff: i64,
    after: Option<&Candidate>,
) -> DbResult<Vec<Candidate>> {
    let (after_at, after_execution) = match after {
        Some(candidate) => (candidate.completed_at, candidate.execution_id.clone()),
        None => (i64::MIN, String::new()),
    };
    db.query_all(
        CANDIDATES_SQL,
        params![cutoff, after_at, after_execution, CANDIDATE_PAGE],
        |row| {
            Ok(Candidate {
                completed_at: row.i64(0)?,
                execution_id: row.text(1)?,
            })
        },
    )
    .await
}

/// Candidates left, counted to one past [`REMAINING_PROBE`] rather than by
/// `COUNT(*)`. Counts what this pass skipped too: they are still unpacked.
async fn probe_remaining(db: &LocalDb, cutoff: i64) -> Option<usize> {
    db.query_all(
        CANDIDATES_SQL,
        params![cutoff, i64::MIN, "", (REMAINING_PROBE + 1) as i64],
        |row| row.text(1),
    )
    .await
    .ok()
    .map(|rows| rows.len())
}

/// Driven from `jobs` by execution, each job's runs probed through
/// `idx_runs_job_id`. As a join the planner scans `runs` instead.
pub(crate) const LIVE_RUN_SQL: &str = "SELECT 1 FROM jobs j
     WHERE j.execution_id = ?1
       AND EXISTS (SELECT 1 FROM runs r WHERE r.job_id = j.id AND r.status IN ('starting', 'live'))
     LIMIT 1";

async fn has_live_run(db: &LocalDb, execution_id: &str) -> DbResult<bool> {
    Ok(db
        .query_opt(LIVE_RUN_SQL, (execution_id.to_string(),), |row| row.i64(0))
        .await?
        .is_some())
}

#[cfg(test)]
#[path = "pack_tests.rs"]
mod tests;
