//! One measured record per root merge attempt.
//!
//! `merge_pr_for_job` opens a [`MergeAttempt`] for every call and runs the whole
//! call inside that attempt's task-local scope. The places that do the work
//! report into whichever attempt is in scope, as the work happens:
//!
//! - the canonical branch preparer (`base_advance::preparation::prepare_branch`)
//!   reports every preparation by branch, its outcome and whether it replayed;
//! - the wake funnel (`messages::delivery`'s `nudge`) reports every wake request
//!   with its outcome;
//! - the landing times its own phases with [`time`], whose timer adds on drop,
//!   so a phase that returns an error still records how long it ran. Timers
//!   may nest: each instant belongs to the innermost open timer only, so a
//!   step timed inside a wider phase (the writer admission inside a fold) is
//!   carved out of it rather than counted twice.
//!
//! Outside an attempt's scope every hook is a no-op. The scope belongs to the
//! task, so two merges running at once never share a collector, and nothing is
//! process-global except the build identity.
//!
//! What a record can and cannot see:
//!
//! - Only work done on the attempt's own task. A task the merge spawns does not
//!   inherit the scope. The detached downstream base-advance follow-up is
//!   wrapped by [`downstream`] in its own `merge_downstream` record carrying the
//!   same attempt id; other detached work is not attributed.
//! - Branch work is counted only where it passes through the canonical
//!   preparer, so the counters are `sibling_preparations`, not "siblings
//!   touched", and the record says `branch_write_coverage: "preparer_only"`.
//!   Other bookmark writers are not yet reported: a fold's flatten re-points
//!   rider bookmarks, jj's automatic descendant rebase moves bookmarks on
//!   commits the fold rewrote. A zero here is therefore not proof that no
//!   other branch moved.
//! - The landing's own compare-and-swap writers (`advance_landing_bookmarks`,
//!   the origin take of the target) also report each accepted bookmark move by
//!   name, and name a move whose outcome they could not observe. These
//!   `retained_bookmark_writes` are the moves the store keeps, not all branch
//!   work, and are evidence rather than a coverage claim. The landing's fold
//!   reports nothing itself: every fold is set aside under its guard before
//!   the landing is installed, so its moves never persist, and the install is
//!   the reported write. The test
//!   `every_bookmark_a_landing_keeps_is_moved_by_a_reporting_writer` pins
//!   these writers and keeps the retired rewrite engine out of the landing.
//!   The same pin over the native VCS candidate path (the squash and install
//!   bodies) belongs to cairn-vcs's own source contract. Until both pass and
//!   a runtime merge shows the record, `branch_write_coverage` stays
//!   `preparer_only`; it may then name the verified retained writers. A writer
//!   whose outcome is unknown stays in `unobserved_bookmark_writes`. Re-pointing child jobs' declared base after a merge is a database update,
//!   not branch work, and is deliberately not counted.
//! - A dispatched continuation is a request, not a resume that ran: the runtime
//!   claims it later (`resume_queue_claimed`). The record therefore names the
//!   jobs it asked to resume and reports `resumes_triggered` as `"unobserved"`;
//!   joining those jobs to their claims is the log reader's work.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use crate::messages::delivery::NudgeOutcome;

tokio::task_local! {
    static CURRENT: Arc<MergeAttempt>;
}

/// The most sibling branch names one record lists; the count is always exact.
const LISTED_BRANCHES: usize = 20;

/// The most job ids one record lists as asked to resume. Past it the record
/// says it is truncated; the count is always exact.
const LISTED_RESUME_JOBS: usize = 500;

/// The most characters of a failure a record carries.
const ERROR_CHARS: usize = 300;

/// A timed part of a merge. Each instant is attributed to at most one phase
/// (the innermost open timer), so their sum never exceeds the attempt's total;
/// the remainder is reported as `unattributed_ms`. On a default-branch landing
/// that remainder is only the short stretches between named phases; on other
/// routes (a child landing, the approval ceremony's human wait) it is larger.
///
/// `total_ms` of a root record runs from the first statement of
/// `merge_pr_for_job` (before its context resolution and commit barrier) to
/// the moment its result is known, after the attempt is journaled. Whatever a
/// caller does around that call (a UI command's own wrapper) is outside it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Phase {
    /// Reading origin's copy of the landing target, off the store lock.
    Fetch,
    /// The canonical preparer carrying the source (or target) onto its base.
    /// Timed by the preparer itself; landings do not time it.
    Prepare,
    /// Building the prospective merge result under the store lock.
    Build,
    /// Validating the merge result off the store lock.
    Validate,
    /// The network leg: push plus read-back, off the store lock.
    Publish,
    /// Recording the landed merge: postcondition, allocations, resolution,
    /// journal.
    Bookkeeping,
    /// Telling GitHub the pull request merged, once the merge is recorded.
    Settle,
    /// Waiting for the store writer lock at the landing's own acquisitions.
    /// The preparer's waits are inside `Prepare`.
    LockWait,
    /// The local fold of the validated landing, under the store lock, less
    /// the writer admission it runs (`Admission`).
    Fold,
    /// Everything before the landing starts: the merge context, the commit
    /// barrier, the base-merge policy gate and its journal event.
    Gates,
    /// Waiting for the landing's reservation of its source and target, and
    /// leasing the store's objects against maintenance.
    ReservationWait,
    /// The origin take of the landing target, from the store lock being
    /// acquired to the take returning: the fence read, the target's write
    /// permit and the fast-forward itself.
    Take,
    /// Judging the configuration the merge result would put on the default
    /// branch.
    ConfigPreflight,
    /// Under the store lock before the fold: re-checking that the prepared
    /// coordinate still holds, and binding identifiers when the landing binds.
    Recheck,
    /// Admitting the landing's writers of its source and target
    /// (`admit_landing_writers`) at every call site, the ones inside the
    /// build and the fold included.
    Admission,
    /// Pinning the folded landing and setting the fold aside, so the store
    /// names the prepared target and source until the landing is installed.
    SetAside,
    /// Installing the landing in the store by compare-and-swap of the two
    /// bookmarks, once origin accepted it (or locally).
    Install,
    /// Recording origin's new value in the store after an accepted push.
    RecordPublication,
}

impl Phase {
    const ALL: [Phase; 18] = [
        Phase::Fetch,
        Phase::Prepare,
        Phase::Build,
        Phase::Validate,
        Phase::Publish,
        Phase::Bookkeeping,
        Phase::Settle,
        Phase::LockWait,
        Phase::Fold,
        Phase::Gates,
        Phase::ReservationWait,
        Phase::Take,
        Phase::ConfigPreflight,
        Phase::Recheck,
        Phase::Admission,
        Phase::SetAside,
        Phase::Install,
        Phase::RecordPublication,
    ];

    fn key(self) -> &'static str {
        match self {
            Phase::Fetch => "fetch_ms",
            Phase::Prepare => "prepare_ms",
            Phase::Build => "build_ms",
            Phase::Validate => "validate_ms",
            Phase::Publish => "publish_ms",
            Phase::Bookkeeping => "bookkeeping_ms",
            Phase::Settle => "settle_ms",
            Phase::LockWait => "lock_wait_ms",
            Phase::Fold => "fold_ms",
            Phase::Gates => "gates_ms",
            Phase::ReservationWait => "reservation_wait_ms",
            Phase::Take => "take_ms",
            Phase::ConfigPreflight => "config_preflight_ms",
            Phase::Recheck => "recheck_ms",
            Phase::Admission => "admission_ms",
            Phase::SetAside => "set_aside_ms",
            Phase::Install => "install_ms",
            Phase::RecordPublication => "record_publication_ms",
        }
    }

    fn index(self) -> usize {
        self as usize
    }
}

/// What one call of the canonical preparer did.
#[derive(Debug, Clone, Copy)]
pub(crate) enum PreparationNote<'a> {
    Prepared {
        head_before: &'a str,
        head_after: &'a str,
        base_commit: &'a str,
        /// Whether the preparer replayed the branch onto a newer base.
        advanced: bool,
    },
    Refused {
        kind: &'static str,
    },
}

#[derive(Debug, Default)]
struct State {
    job_id: String,
    attempt_id: Option<String>,
    merge_request_id: Option<String>,
    pr_number: Option<i32>,
    source: Option<String>,
    target: Option<String>,
    route: Option<&'static str>,
    phases: [Option<Duration>; Phase::ALL.len()],
    /// The open phase timers, innermost last. Only the innermost accrues.
    open: Vec<OpenPhase>,
    next_timer: u64,
    landing_attempts: u32,
    source_preparations: u32,
    replays_issued: u32,
    last_refusal: Option<&'static str>,
    source_head_before: Option<String>,
    source_head_after: Option<String>,
    target_before: Option<String>,
    target_after: Option<String>,
    siblings: BTreeSet<String>,
    sibling_replays: u32,
    /// Branch work reported before the attempt knew its source and target, so
    /// it could be attributed to neither.
    unattributed_branch_work: u32,
    wakes: BTreeMap<&'static str, u32>,
    resume_requested_jobs: BTreeSet<String>,
    /// Bookmark moves a canonical writer reported as accepted, in order.
    bookmark_writes: Vec<BookmarkWrite>,
    /// Bookmarks a writer may have moved but whose outcome it could not
    /// report: never counted as unmoved.
    unobserved_writes: BTreeSet<String>,
}

#[derive(Debug)]
struct OpenPhase {
    timer: u64,
    phase: Phase,
    /// When this timer last became the innermost one.
    since: Instant,
}

#[derive(Debug)]
struct BookmarkWrite {
    branch: String,
    role: &'static str,
    before: String,
    after: String,
}

/// The most bookmark writes one record lists; the per-role counts are exact.
const LISTED_WRITES: usize = 50;

impl State {
    fn add_phase(&mut self, phase: Phase, elapsed: Duration) {
        let slot = &mut self.phases[phase.index()];
        *slot = Some(slot.unwrap_or_default() + elapsed);
    }

    /// Open a timer for `phase` at `now`, pausing the one it nests in.
    fn open_phase(&mut self, phase: Phase, now: Instant) -> u64 {
        if let Some(outer) = self.open.last_mut() {
            let (outer_phase, ran) = (outer.phase, now.saturating_duration_since(outer.since));
            outer.since = now;
            self.add_phase(outer_phase, ran);
        }
        self.next_timer += 1;
        self.open.push(OpenPhase {
            timer: self.next_timer,
            phase,
            since: now,
        });
        self.next_timer
    }

    /// Close `timer` at `now`. The innermost timer is credited since it last
    /// became innermost and the one it nested in resumes; a timer closed
    /// while another is nested in it is credited nothing more, since that
    /// stretch was the nested timer's.
    fn close_phase(&mut self, timer: u64, now: Instant) {
        let Some(at) = self.open.iter().rposition(|open| open.timer == timer) else {
            return;
        };
        let closed = self.open.remove(at);
        if at < self.open.len() {
            return;
        }
        self.add_phase(closed.phase, now.saturating_duration_since(closed.since));
        if let Some(outer) = self.open.last_mut() {
            outer.since = now;
        }
    }

    /// Which of the landing's own branches `branch` is, if the attempt knows.
    fn role(&self, branch: &str) -> Option<Role> {
        let source = self.source.as_deref()?;
        Some(if branch == source {
            Role::Source
        } else if self.target.as_deref() == Some(branch) {
            Role::Target
        } else {
            Role::Sibling
        })
    }

    fn role_label(&self, branch: &str) -> &'static str {
        match self.role(branch) {
            Some(Role::Source) => "source",
            Some(Role::Target) => "target",
            Some(Role::Sibling) => "sibling",
            None => "unattributed",
        }
    }

    fn writes_by(&self, role: &str) -> usize {
        self.bookmark_writes
            .iter()
            .filter(|write| write.role == role)
            .count()
    }

    fn branch_work(&mut self, branch: &str, advanced: bool) -> Option<Role> {
        let role = self.role(branch);
        match role {
            Some(Role::Source | Role::Target) => {
                if advanced {
                    self.replays_issued += 1;
                }
            }
            Some(Role::Sibling) => {
                self.siblings.insert(branch.to_string());
                if advanced {
                    self.sibling_replays += 1;
                }
            }
            None => self.unattributed_branch_work += 1,
        }
        role
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    Source,
    Target,
    Sibling,
}

/// The collector for one root merge attempt, or for the downstream follow-up
/// one attempt spawned.
#[derive(Debug)]
pub(crate) struct MergeAttempt {
    record: &'static str,
    started: Instant,
    state: Mutex<State>,
}

impl MergeAttempt {
    /// A new attempt for `merge_pr_for_job(job_id)`.
    pub(crate) fn begin(job_id: &str) -> Arc<Self> {
        Arc::new(Self {
            record: "merge_attempt",
            started: Instant::now(),
            state: Mutex::new(State {
                job_id: job_id.to_string(),
                ..State::default()
            }),
        })
    }

    /// The collector for work `parent` spawned off its own task: the same
    /// identity, its own counts and clock.
    fn downstream_of(parent: &MergeAttempt) -> Arc<Self> {
        let identity = parent.with(|state| State {
            job_id: state.job_id.clone(),
            attempt_id: state.attempt_id.clone(),
            merge_request_id: state.merge_request_id.clone(),
            pr_number: state.pr_number,
            source: state.source.clone(),
            target: state.target.clone(),
            route: state.route,
            ..State::default()
        });
        Arc::new(Self {
            record: "merge_downstream",
            started: Instant::now(),
            state: Mutex::new(identity),
        })
    }

    fn with<T>(&self, f: impl FnOnce(&mut State) -> T) -> T {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        f(&mut state)
    }

    /// The record as it stands, with `outcome` as the attempt's result.
    pub(crate) fn record(&self, outcome: Result<(), &str>) -> serde_json::Value {
        let total = self.started.elapsed();
        self.with(|state| {
            let attributed: Duration = state.phases.iter().flatten().sum();
            let mut record = serde_json::json!({
                "record": self.record,
                "job_id": state.job_id,
                "attempt_id": state.attempt_id,
                "merge_request_id": state.merge_request_id,
                "pr_number": state.pr_number,
                "source": state.source,
                "target": state.target,
                "route": state.route,
                "outcome": if outcome.is_ok() { "succeeded" } else { "failed" },
                "error": outcome.err().map(|error| error.chars().take(ERROR_CHARS).collect::<String>()),
                "target_before": state.target_before,
                "target_after": state.target_after,
                "source_head_before": state.source_head_before,
                "source_head_after": state.source_head_after,
                "total_ms": millis(total),
                "unattributed_ms": millis(total.saturating_sub(attributed)),
                "landing_attempts": state.landing_attempts,
                "source_preparations": state.source_preparations,
                "replays_issued": state.replays_issued,
                "last_preparation_refusal": state.last_refusal,
                "sibling_preparations": state.siblings.len(),
                "sibling_preparation_branches": state.siblings.iter().take(LISTED_BRANCHES).collect::<Vec<_>>(),
                "sibling_replays": state.sibling_replays,
                // Only the canonical preparer reports branch work so far. A bookmark
                // another writer moves (a fold's automatic descendant rebase, a
                // re-pointed rider, an origin import) is not in these counts.
                "branch_write_coverage": "preparer_only",
                "retained_bookmark_writes": state.bookmark_writes.iter().take(LISTED_WRITES).map(|write| serde_json::json!({
                    "branch": write.branch,
                    "role": write.role,
                    "before": write.before,
                    "after": write.after,
                })).collect::<Vec<_>>(),
                "retained_bookmark_writes_truncated": state.bookmark_writes.len() > LISTED_WRITES,
                "retained_source_bookmark_writes": state.writes_by("source"),
                "retained_target_bookmark_writes": state.writes_by("target"),
                "retained_sibling_bookmark_writes": state.writes_by("sibling"),
                "unobserved_bookmark_writes": state.unobserved_writes,
                "unattributed_branch_work": state.unattributed_branch_work,
                "wake_requests": state.wakes.values().sum::<u32>(),
                "wakes": state.wakes,
                "resume_requested_job_count": state.resume_requested_jobs.len(),
                "resume_requested_jobs": state.resume_requested_jobs.iter().take(LISTED_RESUME_JOBS).collect::<Vec<_>>(),
                "resume_requested_jobs_truncated": state.resume_requested_jobs.len() > LISTED_RESUME_JOBS,
                "resumes_triggered": "unobserved",
            });
            // A phase the attempt never reached is null, not zero.
            for phase in Phase::ALL {
                record[phase.key()] = state.phases[phase.index()].map(millis).into();
            }
            record
        })
    }

    /// How long `phase` has accrued so far, below the record's millisecond
    /// rounding.
    #[cfg(test)]
    pub(crate) fn phase_total(&self, phase: Phase) -> Option<Duration> {
        self.with(|state| state.phases[phase.index()])
    }

    /// Emit the root attempt's record once its result is known.
    pub(crate) fn finish(&self, result: &Result<String, String>) {
        emit(self.record(result.as_ref().map(|_| ()).map_err(String::as_str)));
    }
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// The running executable's content identity, computed once per process. It
/// hashes the whole binary, so it is only ever computed off the async runtime.
pub(crate) fn build_id() -> &'static str {
    static BUILD: OnceLock<String> = OnceLock::new();
    BUILD.get_or_init(|| {
        cairn_common::build_identity::current_executable_build_id()
            .unwrap_or_else(|error| format!("unavailable:{error}"))
    })
}

/// Log `record` as one INFO line, `<record kind> <json>`, stamped with the
/// build and boot it ran on. Off the merge's own path: the first record of a
/// process pays for hashing the executable on a blocking worker.
fn emit(record: serde_json::Value) {
    match tokio::runtime::Handle::try_current() {
        Ok(runtime) => drop(runtime.spawn_blocking(move || log_record(record))),
        Err(_) => log_record(record),
    }
}

fn log_record(mut record: serde_json::Value) {
    record["build"] = build_id().into();
    record["boot"] = cairn_common::build_identity::process_boot_generation().into();
    record["emitted_at"] = chrono::Utc::now().to_rfc3339().into();
    let kind = record["record"]
        .as_str()
        .unwrap_or("merge_attempt")
        .to_string();
    log::info!("{kind} {record}");
}

fn current() -> Option<Arc<MergeAttempt>> {
    CURRENT.try_with(Arc::clone).ok()
}

fn update(f: impl FnOnce(&mut State)) {
    if let Some(attempt) = current() {
        attempt.with(f);
    }
}

/// Run `future` as `attempt`: everything it does on its own task reports there.
pub(crate) async fn measured<F: Future>(attempt: Arc<MergeAttempt>, future: F) -> F::Output {
    CURRENT.scope(attempt, future).await
}

/// Wrap a future the current attempt is about to spawn, so the work it does
/// is recorded as this attempt's `merge_downstream` record when it finishes.
/// Outside an attempt it is `future` unchanged. Call it on the attempt's task,
/// before spawning.
///
/// The record is emitted whenever the follow-up finishes, including when it did
/// nothing, so a missing `merge_downstream` line means it never ran or died.
pub(crate) fn downstream<F>(future: F) -> impl Future<Output = Result<(), String>> + Send + 'static
where
    F: Future<Output = Result<(), String>> + Send + 'static,
{
    let parent = current();
    async move {
        let Some(parent) = parent else {
            return future.await;
        };
        let child = MergeAttempt::downstream_of(&parent);
        let output = CURRENT.scope(Arc::clone(&child), future).await;
        emit(child.record(output.as_ref().map(|_| ()).map_err(String::as_str)));
        output
    }
}

/// Name the pull request this attempt merges, once it is resolved.
pub(crate) fn identify(merge_request_id: &str, pr_number: Option<i32>, source: &str, target: &str) {
    update(|state| {
        state.merge_request_id = Some(merge_request_id.to_string());
        state.pr_number = pr_number;
        state.source = Some(source.to_string());
        state.target = Some(target.to_string());
    });
}

/// The merge attempt journal id (`merge_attempt_events.attempt_id`).
pub(crate) fn journaled(attempt_id: &str) {
    update(|state| state.attempt_id = Some(attempt_id.to_string()));
}

/// How the merge lands: `store` (the store fold, no remote), `origin` (the
/// store fold published by a leased push), either with an `-approval` suffix
/// when it lands behind the human approval ceremony, or `child` (into an
/// integration branch).
pub(crate) fn route(route: &'static str) {
    update(|state| state.route = Some(route));
}

/// The commit the target holds once the merge is recorded.
pub(crate) fn landed(commit: &str) {
    update(|state| state.target_after = Some(commit.to_string()));
}

/// One more build-validate-land round of the landing loop.
pub(crate) fn landing_attempt() {
    update(|state| state.landing_attempts += 1);
}

/// A timer that adds its elapsed time to `phase` of the attempt in scope when
/// dropped, including on an early error return. Bind it to a named variable
/// (`let _publish = time(..)`): `let _ = time(..)` drops it at once. A timer
/// opened while another is open nests in it: the outer one is paused until the
/// inner one closes. Open it on the attempt's own task, and never run two
/// timed futures of one attempt concurrently: their time would be split
/// between them by nesting order, never double-counted but misattributed.
pub(crate) fn time(phase: Phase) -> PhaseTimer {
    let attempt = current();
    let timer = attempt.as_ref().map_or(0, |attempt| {
        attempt.with(|state| state.open_phase(phase, Instant::now()))
    });
    PhaseTimer { attempt, timer }
}

#[must_use = "a phase timer measures until it is dropped"]
pub(crate) struct PhaseTimer {
    attempt: Option<Arc<MergeAttempt>>,
    timer: u64,
}

impl Drop for PhaseTimer {
    fn drop(&mut self) {
        if let Some(attempt) = self.attempt.take() {
            let now = Instant::now();
            attempt.with(|state| state.close_phase(self.timer, now));
        }
    }
}

/// The canonical preparer's report of one preparation of `branch`.
pub(crate) fn note_preparation(branch: &str, elapsed: Duration, note: PreparationNote<'_>) {
    update(|state| {
        let advanced = matches!(note, PreparationNote::Prepared { advanced: true, .. });
        let role = state.branch_work(branch, advanced);
        if !matches!(role, Some(Role::Source | Role::Target)) {
            return;
        }
        state.add_phase(Phase::Prepare, elapsed);
        if role != Some(Role::Source) {
            return;
        }
        state.source_preparations += 1;
        match note {
            PreparationNote::Prepared {
                head_before,
                head_after,
                base_commit,
                ..
            } => {
                if state.source_head_before.is_none() {
                    state.source_head_before = Some(head_before.to_string());
                }
                state.source_head_after = Some(head_after.to_string());
                state.target_before = Some(base_commit.to_string());
            }
            PreparationNote::Refused { kind } => state.last_refusal = Some(kind),
        }
    });
}

/// A canonical bookmark writer's report of moves it made and the store
/// accepted, as `(bookmark, before, after)`. Report only accepted outcomes;
/// call it on the attempt's own task (inline under `blocking_phase` is fine, a
/// `spawn_blocking` worker is not: the collector is not visible there).
pub(crate) fn note_bookmark_writes(moves: &[(&str, &str, &str)]) {
    update(|state| {
        for (branch, before, after) in moves {
            let role = state.role_label(branch);
            state.bookmark_writes.push(BookmarkWrite {
                branch: branch.to_string(),
                role,
                before: before.to_string(),
                after: after.to_string(),
            });
        }
    });
}

/// A writer that may have moved `branches` but could not report the outcome
/// (a failure after its first mutation, a worker that did not finish).
pub(crate) fn note_unobserved_writes(branches: &[&str]) {
    update(|state| {
        state
            .unobserved_writes
            .extend(branches.iter().map(|branch| branch.to_string()));
    });
}

/// The wake funnel's report of one wake request for `job_id`.
pub(crate) fn note_wake(job_id: &str, outcome: &Result<NudgeOutcome, String>) {
    update(|state| {
        let kind = match outcome {
            Ok(NudgeOutcome::NotWaking) => "not_waking",
            Ok(NudgeOutcome::Refused(_)) => "refused",
            Ok(NudgeOutcome::QueuedForActiveTurn) => "queued_for_active_turn",
            Ok(NudgeOutcome::Interrupted) => "interrupted",
            Ok(NudgeOutcome::Resumed) => "resume_dispatched",
            Ok(NudgeOutcome::SupersededWait { .. }) => "superseded_wait",
            Ok(NudgeOutcome::Held(_)) => "held",
            Ok(NudgeOutcome::Declined(_)) => "declined",
            Err(_) => "failed",
        };
        *state.wakes.entry(kind).or_default() += 1;
        if matches!(
            outcome,
            Ok(NudgeOutcome::Resumed
                | NudgeOutcome::Interrupted
                | NudgeOutcome::SupersededWait { .. })
        ) {
            state.resume_requested_jobs.insert(job_id.to_string());
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn prepared<'a>(
        head_before: &'a str,
        head_after: &'a str,
        base: &'a str,
        advanced: bool,
    ) -> PreparationNote<'a> {
        PreparationNote::Prepared {
            head_before,
            head_after,
            base_commit: base,
            advanced,
        }
    }

    #[tokio::test]
    async fn counts_come_from_the_reported_preparations_by_branch() {
        let attempt = MergeAttempt::begin("job-1");
        measured(Arc::clone(&attempt), async {
            identify("mr-1", Some(42), "feature", "main");
            journaled("attempt-1");
            route("origin");
            landing_attempt();
            note_preparation(
                "feature",
                Duration::from_millis(5),
                prepared("h0", "h1", "b1", true),
            );
            landing_attempt();
            note_preparation(
                "feature",
                Duration::from_millis(5),
                prepared("h1", "h1", "b1", false),
            );
            note_preparation(
                "sibling",
                Duration::from_millis(50),
                prepared("s0", "s1", "b1", true),
            );
            note_preparation(
                "other",
                Duration::from_millis(50),
                PreparationNote::Refused { kind: "busy" },
            );
            landed("m1");
        })
        .await;

        let record = attempt.record(Ok(()));
        assert_eq!(record["record"], "merge_attempt");
        assert_eq!(record["pr_number"], 42);
        assert_eq!(record["merge_request_id"], "mr-1");
        assert_eq!(record["attempt_id"], "attempt-1");
        assert_eq!(record["route"], "origin");
        assert_eq!(record["outcome"], "succeeded");
        assert_eq!(record["landing_attempts"], 2);
        assert_eq!(record["source_preparations"], 2);
        assert_eq!(record["replays_issued"], 1);
        assert_eq!(record["source_head_before"], "h0");
        assert_eq!(record["source_head_after"], "h1");
        assert_eq!(record["target_before"], "b1");
        assert_eq!(record["target_after"], "m1");
        assert_eq!(record["sibling_preparations"], 2);
        assert_eq!(
            record["sibling_preparation_branches"],
            serde_json::json!(["other", "sibling"])
        );
        assert_eq!(record["sibling_replays"], 1);
        // Only the landing's own preparations are its prepare phase.
        assert_eq!(record["prepare_ms"], 10);
    }

    #[tokio::test]
    async fn nothing_reported_is_nothing_counted_and_unreached_phases_are_null() {
        let attempt = MergeAttempt::begin("job-1");
        measured(Arc::clone(&attempt), async {
            identify("mr-1", None, "feature", "main");
        })
        .await;
        let record = attempt.record(Ok(()));
        assert_eq!(record["sibling_preparations"], 0);
        assert_eq!(record["replays_issued"], 0);
        assert_eq!(record["wake_requests"], 0);
        assert!(record["pr_number"].is_null());
        assert!(record["publish_ms"].is_null());
        assert!(record["target_before"].is_null());
        assert_eq!(record["resumes_triggered"], "unobserved");
    }

    #[tokio::test]
    async fn a_failed_attempt_keeps_the_phases_it_ran_and_its_refusal() {
        async fn land() -> Result<String, String> {
            let _validate = time(Phase::Validate);
            std::thread::sleep(Duration::from_millis(3));
            note_preparation(
                "feature",
                Duration::from_millis(7),
                PreparationNote::Refused { kind: "conflict" },
            );
            Err("Refusing to merge: conflict".to_string())
        }
        let attempt = MergeAttempt::begin("job-1");
        let result = measured(Arc::clone(&attempt), async {
            identify("mr-1", Some(7), "feature", "main");
            land().await
        })
        .await;
        let record = attempt.record(result.as_ref().map(|_| ()).map_err(String::as_str));
        assert_eq!(record["outcome"], "failed");
        assert_eq!(record["error"], "Refusing to merge: conflict");
        assert_eq!(record["pr_number"], 7);
        assert!(record["validate_ms"].as_u64().unwrap() >= 3);
        assert_eq!(record["prepare_ms"], 7);
        assert_eq!(record["last_preparation_refusal"], "conflict");
        assert_eq!(record["replays_issued"], 0);
    }

    /// A timer opened inside another carves its time out of the outer one,
    /// so nested phases are never counted twice and the outer phase resumes
    /// when the inner one closes.
    #[tokio::test]
    async fn a_nested_phase_is_carved_out_of_the_phase_it_runs_in() {
        let ms = Duration::from_millis;
        let t0 = Instant::now();
        let mut state = State::default();
        let fold = state.open_phase(Phase::Fold, t0);
        let admission = state.open_phase(Phase::Admission, t0 + ms(20));
        state.close_phase(admission, t0 + ms(50));
        state.close_phase(fold, t0 + ms(70));
        assert_eq!(state.phases[Phase::Admission.index()], Some(ms(30)));
        assert_eq!(
            state.phases[Phase::Fold.index()],
            Some(ms(40)),
            "the fold keeps only its own time, not the admission's"
        );

        // An outer timer closed while an inner one is open is credited
        // nothing for that stretch, and the inner one keeps accruing.
        let outer = state.open_phase(Phase::Recheck, t0 + ms(100));
        let inner = state.open_phase(Phase::Install, t0 + ms(105));
        state.close_phase(outer, t0 + ms(115));
        state.close_phase(inner, t0 + ms(125));
        assert_eq!(state.phases[Phase::Recheck.index()], Some(ms(5)));
        assert_eq!(state.phases[Phase::Install.index()], Some(ms(20)));
        assert!(state.open.is_empty());

        // Through the task-local hooks, the sum never exceeds the total.
        let attempt = MergeAttempt::begin("job-1");
        measured(Arc::clone(&attempt), async {
            let _fold = time(Phase::Fold);
            let _admission = time(Phase::Admission);
            std::thread::sleep(ms(5));
        })
        .await;
        assert!(attempt.phase_total(Phase::Admission).unwrap() >= ms(5));
        let record = attempt.record(Ok(()));
        let attributed: u64 = Phase::ALL
            .iter()
            .filter_map(|phase| record[phase.key()].as_u64())
            .sum();
        assert!(attributed <= record["total_ms"].as_u64().unwrap());
        assert!(record["take_ms"].is_null(), "a phase never reached is null");
    }

    #[tokio::test]
    async fn wake_requests_are_counted_by_outcome_and_resumes_stay_unobserved() {
        let attempt = MergeAttempt::begin("job-1");
        measured(Arc::clone(&attempt), async {
            identify("mr-1", Some(1), "feature", "main");
            note_wake("owner", &Ok(NudgeOutcome::Resumed));
            note_wake("parent", &Ok(NudgeOutcome::QueuedForActiveTurn));
            note_wake("gone", &Err("failed to route".to_string()));
        })
        .await;
        let record = attempt.record(Ok(()));
        assert_eq!(record["wake_requests"], 3);
        assert_eq!(record["wakes"]["resume_dispatched"], 1);
        assert_eq!(record["wakes"]["queued_for_active_turn"], 1);
        assert_eq!(record["wakes"]["failed"], 1);
        assert_eq!(
            record["resume_requested_jobs"],
            serde_json::json!(["owner"])
        );
        assert_eq!(record["resumes_triggered"], "unobserved");
    }

    #[tokio::test]
    async fn concurrent_attempts_and_unscoped_work_never_mix() {
        let (a, b) = (MergeAttempt::begin("a"), MergeAttempt::begin("b"));
        // Outside any attempt every hook is a no-op.
        note_preparation("x", Duration::ZERO, prepared("0", "1", "b", true));
        note_wake("x", &Ok(NudgeOutcome::Resumed));
        let first = measured(Arc::clone(&a), async {
            identify("mr-a", None, "a", "main");
            tokio::task::yield_now().await;
            note_preparation("a", Duration::ZERO, prepared("0", "1", "m", true));
        });
        let second = measured(Arc::clone(&b), async {
            identify("mr-b", None, "b", "main");
            tokio::task::yield_now().await;
            note_preparation("a", Duration::ZERO, prepared("0", "1", "m", true));
            note_wake("x", &Ok(NudgeOutcome::Resumed));
        });
        tokio::join!(first, second);
        let (a, b) = (a.record(Ok(())), b.record(Ok(())));
        assert_eq!(
            (
                a["replays_issued"].as_u64(),
                a["sibling_preparations"].as_u64()
            ),
            (Some(1), Some(0))
        );
        assert_eq!(a["wake_requests"], 0);
        // `a` is a sibling of `b`'s landing.
        assert_eq!(
            (
                b["replays_issued"].as_u64(),
                b["sibling_preparations"].as_u64()
            ),
            (Some(0), Some(1))
        );
        assert_eq!(b["wake_requests"], 1);
    }

    #[tokio::test]
    async fn accepted_bookmark_writes_are_listed_by_role_and_unobserved_ones_named() {
        let attempt = MergeAttempt::begin("job-1");
        measured(Arc::clone(&attempt), async {
            identify("mr-1", Some(9), "feature", "main");
            note_bookmark_writes(&[("main", "b0", "m1"), ("feature", "h1", "m1")]);
            note_bookmark_writes(&[("child", "c0", "c1")]);
            note_unobserved_writes(&["main"]);
        })
        .await;
        let record = attempt.record(Ok(()));
        assert_eq!(record["retained_target_bookmark_writes"], 1);
        assert_eq!(record["retained_source_bookmark_writes"], 1);
        assert_eq!(record["retained_sibling_bookmark_writes"], 1);
        assert_eq!(record["retained_bookmark_writes"][2]["branch"], "child");
        assert_eq!(record["retained_bookmark_writes"][2]["role"], "sibling");
        assert_eq!(record["retained_bookmark_writes"][0]["before"], "b0");
        assert_eq!(
            record["unobserved_bookmark_writes"],
            serde_json::json!(["main"])
        );
        // Named reports are recorded, but coverage is not claimed from them.
        assert_eq!(record["branch_write_coverage"], "preparer_only");
    }

    /// The landing's own sources, inline test modules cut off.
    fn landing_sources() -> [(&'static str, &'static str); 5] {
        fn production(source: &'static str) -> &'static str {
            source
                .split("\n#[cfg(test)]\nmod tests")
                .next()
                .unwrap_or(source)
        }
        [
            (
                "default_landing.rs",
                production(include_str!("default_landing.rs")),
            ),
            (
                "child_landing.rs",
                production(include_str!("child_landing.rs")),
            ),
            (
                "store_merge/mod.rs",
                production(include_str!("store_merge/mod.rs")),
            ),
            ("merge.rs", production(include_str!("merge.rs"))),
            ("resolution.rs", production(include_str!("resolution.rs"))),
        ]
    }

    /// The text of the item starting at `signature`, up to its closing brace
    /// at column zero.
    fn body_of<'a>(source: &'a str, signature: &str) -> &'a str {
        let start = source
            .find(signature)
            .unwrap_or_else(|| panic!("`{signature}` is gone"));
        let end = source[start..]
            .find("\n}\n")
            .map_or(source.len(), |end| start + end);
        &source[start..end]
    }

    /// The record's bookmark-write counts are only as complete as the writers
    /// that report into it. This pins that every bookmark a landing keeps is
    /// moved by a reporting writer, and that the retired engine, which moved
    /// bookmarks no landing named, stays out of the landing.
    #[test]
    fn every_bookmark_a_landing_keeps_is_moved_by_a_reporting_writer() {
        const RETIRED: &[&str] = &[
            "flatten_branch_recovery(",
            "heal_conflicted_ancestry(",
            "reconcile_siblings(",
            "rebase_branch_onto(",
            "hide_superseded_history(",
            "rebase_descendants",
            "\"rebase\",",
            "\"abandon\",",
        ];
        for (file, source) in landing_sources() {
            for retired in RETIRED {
                assert!(
                    !source.contains(retired),
                    "{file} calls the retired `{retired}`"
                );
            }
        }

        // Landings move bookmarks by compare-and-swap only in
        // `advance_landing_bookmarks`, which reports what it moved and what it
        // may have moved.
        let (_, default_landing) = landing_sources()[0];
        let advance = body_of(default_landing, "fn advance_landing_bookmarks(");
        assert!(advance.contains("note_bookmark_writes("));
        assert!(advance.contains("note_unobserved_writes("));
        for (file, source) in landing_sources() {
            let inside = if file == "default_landing.rs" {
                advance.matches("\"bookmark\",").count()
            } else {
                0
            };
            assert_eq!(
                source.matches("\"bookmark\",").count(),
                inside,
                "{file} runs `jj bookmark` outside advance_landing_bookmarks"
            );
        }

        // The origin take moves only the target, and reports it.
        let take = body_of(default_landing, "async fn judge_origin_target(");
        assert!(take.contains("fast_forward_branch_from_origin("));
        assert!(take.contains("note_bookmark_writes("));
        assert!(take.contains("note_unobserved_writes("));

        // A fold's own bookmark moves are transient: every fold is set aside
        // under the same guard before the landing is installed, so the fold
        // needs no report of its own and the install's report is the move the
        // store keeps.
        let mut folds = 0;
        for (at, _) in default_landing.match_indices("fold_validated_default(") {
            folds += 1;
            let rest = &default_landing[at..];
            let installed = ["advance_landing_bookmarks(", "publish_released("]
                .iter()
                .filter_map(|writer| rest.find(writer))
                .min()
                .expect("every fold is installed by a reporting writer");
            let set_aside = ["undo_own_operations(", "restore()"]
                .iter()
                .filter_map(|undo| rest.find(undo))
                .min()
                .expect("every fold is set aside");
            assert!(
                set_aside < installed,
                "a fold in default_landing.rs is installed without being set aside first"
            );
        }
        assert_eq!(
            folds, 2,
            "a new fold call site needs the same set-aside proof"
        );

        // The canonical preparer reports every preparation.
        let preparer = include_str!("../../orchestrator/base_advance/preparation.rs");
        assert!(
            body_of(preparer, "pub(crate) async fn prepare_branch(").contains("note_preparation(")
        );
    }

    #[test]
    fn branch_work_before_identification_is_unattributed_not_sibling() {
        let attempt = MergeAttempt::begin("job-1");
        attempt.with(|state| {
            state.branch_work("feature", true);
        });
        let record = attempt.record(Err("context did not resolve"));
        assert_eq!(record["sibling_preparations"], 0);
        assert_eq!(record["unattributed_branch_work"], 1);
    }
}
