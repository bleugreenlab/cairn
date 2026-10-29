use crate::config::project_settings::{
    CheckCommand, CheckPolicy, CheckWhen, ChecksContract, UnsupportedCheck,
};
use crate::execution::cache::{
    list_check_results_for_job, observed_commit_for_job, resolve_job_logical_head,
    CheckResultCacheEntry, HeadVerdictKeys, OwnedHeadVerdictKeys,
};
use crate::execution::check_parsers::{
    error_anchored_tail, extract_running_tests, format_failure_excerpt, format_failure_names,
    is_test_runner_parser, report_excerpt, ParsedCheckResult, VerdictFacts, MAX_FAILURE_NAMES,
    RUST_LINT_PARSER,
};
use crate::execution::checks::{
    check_input_hashes, head_verdicts, head_verdicts_conn, load_checks_contract_at_commit,
    CheckFailureKind,
};
use crate::execution::checks_turn_end::{
    read_turn_end_log_tail, resolve_job_coords, turn_end_check_started, JobCoords, WavePlan,
};
use crate::execution::inputs::{
    any_check_declares_inputs, ResolvedInputs, TreeBlobs, TreeSnapshot,
};
use crate::execution::selection::plan_checks;
use crate::jj::{logical_changed_files, logical_tree_hash, own_patch_against, OwnPatch};
use crate::orchestrator::generation_cache::GenerationCache;
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// In-process materialization of the exact settled check planner projection.
///
/// The generation is the cache's correctness key: every mutation of a job's
/// logical head/tree, live base, contract, results, runtime attempt, or lifecycle
/// advances it. The shared [`GenerationCache`] closes the
/// invalidation-during-compute race: a reader whose miss finishes after an
/// invalidation retries against the new generation instead of returning stale
/// work.
///
/// Two things are layered on top of the primitive here: a job-to-project map, so
/// a check result recorded for a project can invalidate every job of that project
/// without the caller having to enumerate them; and a memo of the checks
/// contract each commit declares.
///
/// While a wave is in flight the same cache serves the live projection through
/// [`LIVE_CHECK_POLL_WINDOW`], so every observer of one job shares one
/// computation per window. Submission and settlement of a wave advance the
/// job's generation, which is what keeps one cache correct across the switch.
pub(crate) struct NodeCheckStatusCache {
    cache: GenerationCache<Vec<NodeCheckStatus>>,
    projects: Mutex<HashMap<String, String>>,
    contracts: Mutex<HashMap<String, Arc<ChecksContract>>>,
    settled_projection_reads: AtomicU64,
    live_window_reads: AtomicU64,
}

/// How long one live projection serves every observer of a job while a check
/// wave runs.
///
/// A running lane's verdicts and output tail move continuously and nothing
/// announces each move, so the live projection cannot be fenced by invalidation
/// alone. One second matches the 1 Hz tail poll the surfaces run, so each
/// observer still sees a new tail every tick, now from one computation per job
/// however many panes, windows, or query keys are watching it.
const LIVE_CHECK_POLL_WINDOW: Duration = Duration::from_secs(1);

/// How many commits' contracts the memo holds before it starts over. A job's
/// head moves a commit at a time, so this is many jobs' worth of heads.
const CONTRACT_MEMO_LIMIT: usize = 256;

/// A status computation slower than this logs where its time went.
const SLOW_STATUS_LOG_THRESHOLD: Duration = Duration::from_secs(1);

/// What one recorded check exit establishes about the tree it ran on.
///
/// The single answer to "is this green verified?": the `/checks` board, the
/// manual check reply, the channel state card, and the `pinBump` merge
/// precondition all classify through [`verdict_evidence`], so a lane can never
/// count as passed on one surface and unverified on another.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VerdictEvidence {
    Failed,
    Verified,
    Unverified(UnverifiedReason),
}

/// Why a successful exit vouches for nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UnverifiedReason {
    /// The lint never ran to completion (nothing selected, cargo missing,
    /// clippy died without a verdict).
    IncompleteLint,
    EmptySelection,
    AllSkipped(usize),
    IncompleteEnumeration,
    NoTestsRan,
    NoStructuredResult,
}

impl std::fmt::Display for UnverifiedReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IncompleteLint => f.write_str("the lint did not run to completion"),
            Self::EmptySelection => f.write_str("no tests matched the selection"),
            Self::AllSkipped(skipped) => write!(f, "no tests ran; {skipped} skipped"),
            Self::IncompleteEnumeration => f.write_str("test enumeration was incomplete"),
            Self::NoTestsRan => f.write_str("no tests ran"),
            Self::NoStructuredResult => {
                f.write_str("the command produced no structured coverage result")
            }
        }
    }
}

/// Classify a recorded exit. Exit zero establishes only that nothing
/// contradicted the command: a test runner that ran nothing, an incomplete
/// enumeration, or no canonical artifact at all establishes no verified green.
/// A non-test parse (tsc, a complete lint) vouches for itself.
pub(crate) fn verdict_evidence(
    passed: bool,
    parsed: Option<&ParsedCheckResult>,
) -> VerdictEvidence {
    verdict_evidence_from_facts(
        passed,
        parsed.map(ParsedCheckResult::verdict_facts).as_ref(),
    )
}

/// [`verdict_evidence`] over only the fields it reads, for a reader that
/// projected them from storage instead of parsing the whole result.
pub(crate) fn verdict_evidence_from_facts(
    passed: bool,
    parsed: Option<&VerdictFacts<'_>>,
) -> VerdictEvidence {
    if !passed {
        return VerdictEvidence::Failed;
    }
    let Some(parsed) = parsed else {
        return VerdictEvidence::Unverified(UnverifiedReason::NoStructuredResult);
    };
    let reason = if parsed.is_incomplete_lint() {
        UnverifiedReason::IncompleteLint
    } else if !parsed.is_test_runner() {
        return VerdictEvidence::Verified;
    } else if parsed.selection == "empty" {
        UnverifiedReason::EmptySelection
    } else if parsed.tests_run() == 0 && parsed.skipped > 0 {
        UnverifiedReason::AllSkipped(parsed.skipped)
    } else if !parsed.complete {
        UnverifiedReason::IncompleteEnumeration
    } else if parsed.tests_run() == 0 {
        UnverifiedReason::NoTestsRan
    } else {
        return VerdictEvidence::Verified;
    };
    VerdictEvidence::Unverified(reason)
}

pub(crate) fn check_state_from_evidence(
    passed: bool,
    parsed: Option<&ParsedCheckResult>,
) -> NodeCheckState {
    check_state_from_facts(
        passed,
        parsed.map(ParsedCheckResult::verdict_facts).as_ref(),
    )
}

pub(crate) fn check_state_from_facts(
    passed: bool,
    parsed: Option<&VerdictFacts<'_>>,
) -> NodeCheckState {
    match verdict_evidence_from_facts(passed, parsed) {
        VerdictEvidence::Failed => NodeCheckState::Failed,
        VerdictEvidence::Verified => NodeCheckState::Passed,
        VerdictEvidence::Unverified(_) => NodeCheckState::Unverified,
    }
}

/// Whether an applicable lane holds a verdict, and what it holds instead when
/// it does not.
///
/// A different question from [`VerdictEvidence`], which asks whether a GREEN is
/// verified. Here a red is as complete as a green: the question is whether the
/// head was judged at all, not how it came out. A result with no structured
/// output is the command's exit code, which counts as a verdict (a script
/// check, or a runner whose output Cairn does not parse); for a test runner
/// that is an interim until its output is parsed or it writes a result file.
/// A green whose own structured result says nothing
/// was selected, or that the run was cut short, is not a verdict. Any recorded
/// failure kind is a run that ended without a verdict, except a selection that
/// matched nothing, which is an empty one.
///
/// Emptiness splits in two. A static check whose result says its selection was
/// empty (a lint whose cone holds no crate the change touched) had no
/// selectable work: not a verdict and never a green (cairn/5325), but not a
/// missing answer either, because there was nothing in its domain to judge. A test
/// runner's empty selection over a change its impact matched is the cairn/5325
/// defect (impact names files the selection maps to no test), and stays
/// `Empty`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LaneExistence {
    /// A red, or a green its own output does not contradict.
    Complete,
    /// No row at this head, and no wave in flight that would produce one.
    Absent,
    /// No row yet, but a wave has queued (`running: false`) or started it.
    Pending { running: bool },
    /// The last run at this head ended without a verdict: the substrate
    /// failed, the run was killed, or the failure could not be classified.
    /// `kind` is the stored failure kind.
    Died { kind: String },
    /// Cairn stopped running the check after this many consecutive
    /// infrastructure failures.
    Breaker { after: i64 },
    /// A test runner selected nothing to judge for a change its impact matched.
    Empty,
    /// The run started judging and did not finish.
    Incomplete(UnverifiedReason),
    /// A static check found nothing in its domain in this change: not
    /// applicable by content.
    NoSelectableWork,
}

impl LaneExistence {
    /// The one word every surface names this state by.
    pub(crate) fn label(&self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::Absent => "absent",
            Self::Pending { running: false } => "pending",
            Self::Pending { running: true } => "running",
            Self::Died { .. } => "died",
            Self::Breaker { .. } => "breaker",
            Self::Empty => "empty",
            Self::Incomplete(_) => "incomplete",
            Self::NoSelectableWork => "not applicable",
        }
    }

    /// Whether this state answers the lane from the change's own content: a
    /// verdict, or a static check with nothing in its domain. Every other state
    /// is an attempt that ended without an answer, and such an attempt never
    /// displaces an answer recorded for the same inputs ([`select_lane_row`]).
    pub(crate) fn answers_the_lane(&self) -> bool {
        matches!(self, Self::Complete | Self::NoSelectableWork)
    }

    /// The state in a few words, carrying its cause where it has one.
    pub(crate) fn describe(&self) -> String {
        match self {
            Self::Died { kind } => format!("died: {}", kind.replace('_', " ")),
            Self::Breaker { after } => {
                format!("suppressed after {after} infrastructure failures")
            }
            Self::Empty => "the selection matched no tests".to_string(),
            Self::Incomplete(reason) => reason.to_string(),
            other => other.label().to_string(),
        }
    }
}

/// [`LaneExistence`] of one recorded row: whether it exited zero, its stored
/// failure kind, the infrastructure streak when the breaker tripped on it, and
/// its structured result's facts.
pub(crate) fn row_existence(
    passed: bool,
    failure_kind: Option<&str>,
    breaker_after: Option<i64>,
    facts: Option<&VerdictFacts<'_>>,
) -> LaneExistence {
    if let Some(after) = breaker_after {
        return LaneExistence::Breaker { after };
    }
    if let Some(kind) = failure_kind {
        return if CheckFailureKind::from_stored(kind) == Some(CheckFailureKind::Selection) {
            LaneExistence::Empty
        } else {
            LaneExistence::Died {
                kind: kind.to_string(),
            }
        };
    }
    if passed && facts.is_some_and(|facts| !facts.is_test_runner() && facts.selection == "empty") {
        return LaneExistence::NoSelectableWork;
    }
    match verdict_evidence_from_facts(passed, facts) {
        VerdictEvidence::Failed
        | VerdictEvidence::Verified
        | VerdictEvidence::Unverified(UnverifiedReason::NoStructuredResult) => {
            LaneExistence::Complete
        }
        VerdictEvidence::Unverified(
            UnverifiedReason::EmptySelection
            | UnverifiedReason::NoTestsRan
            | UnverifiedReason::AllSkipped(_),
        ) => LaneExistence::Empty,
        VerdictEvidence::Unverified(
            reason @ (UnverifiedReason::IncompleteEnumeration | UnverifiedReason::IncompleteLint),
        ) => LaneExistence::Incomplete(reason),
    }
}

/// [`LaneExistence`] of one stored row: the classification every reader that
/// chooses a lane's row, or renders one, consults.
pub(crate) fn entry_existence(row: &CheckResultCacheEntry) -> LaneExistence {
    let parsed = row
        .target_results_json
        .as_deref()
        .and_then(|json| serde_json::from_str::<ParsedCheckResult>(json).ok());
    row_existence(
        row.passed,
        row.failure_kind.as_deref(),
        (row.infra_failure_streak >= crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND)
            .then_some(row.infra_failure_streak),
        parsed
            .as_ref()
            .map(ParsedCheckResult::verdict_facts)
            .as_ref(),
    )
}

/// The row a lane is shown by, and the newer attempt it outranked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LaneRow {
    pub(crate) row: CheckResultCacheEntry,
    /// A newer attempt at the same inputs that ended without a verdict, kept
    /// so a surface can say it happened without letting it stand for the lane.
    pub(crate) newer_attempt: Option<CheckResultCacheEntry>,
}

impl From<CheckResultCacheEntry> for LaneRow {
    fn from(row: CheckResultCacheEntry) -> Self {
        Self {
            row,
            newer_attempt: None,
        }
    }
}

/// The row that represents one lane, chosen from every row recorded for it at
/// one set of inputs (one input hash), which arrive newest first.
///
/// Answer quality outranks recency. A row that answers the lane
/// ([`LaneExistence::answers_the_lane`]) is shown over any row that does not,
/// however much newer: a run that could not fetch the commit, was killed, or
/// selected nothing says nothing about inputs another run already judged, so it
/// must not turn a recorded pass red, or a red into silence. Within one class
/// the newest row wins, and arrival order breaks ties. Only when no row answers
/// does the newest attempt stand alone, rendered as itself.
pub(crate) fn select_lane_row(
    rows: impl IntoIterator<Item = CheckResultCacheEntry>,
) -> Option<LaneRow> {
    let mut best: Option<(bool, CheckResultCacheEntry)> = None;
    let mut newest_attempt: Option<CheckResultCacheEntry> = None;
    for row in rows {
        let answers = entry_existence(&row).answers_the_lane();
        if !answers
            && newest_attempt
                .as_ref()
                .is_none_or(|attempt| attempt.ran_at < row.ran_at)
        {
            newest_attempt = Some(row.clone());
        }
        let outranks = best.as_ref().is_none_or(|(best_answers, best_row)| {
            (answers, row.ran_at) > (*best_answers, best_row.ran_at)
        });
        if outranks {
            best = Some((answers, row));
        }
    }
    let (answers, row) = best?;
    let newer_attempt = newest_attempt.filter(|attempt| answers && attempt.ran_at > row.ran_at);
    Some(LaneRow { row, newer_attempt })
}

/// The sentence a lane carries beside its answer when a newer attempt at the
/// same inputs ended without one.
pub(crate) fn newer_attempt_note(lane: &LaneRow) -> Option<String> {
    let attempt = lane.newer_attempt.as_ref()?;
    let answer = match entry_existence(&lane.row) {
        LaneExistence::NoSelectableWork => "not applicable",
        _ if lane.row.passed => "passed",
        _ => "failed",
    };
    let at = crate::clock::stamp_millis(lane.row.ran_at)
        .map(|stamp| format!(" at {stamp}"))
        .unwrap_or_default();
    Some(format!(
        "latest attempt: no verdict ({}); last verdict: {answer}{at}",
        entry_existence(attempt).describe()
    ))
}

/// [`LaneExistence`] of one lane of the node projection, or `None` when the
/// lane has nothing to say about the head: the impact gate excluded it, it runs
/// only against a merge result at landing, or this build cannot run it.
///
/// A lane with no row is `Pending` only while a wave of its own cadence is in
/// flight; with nothing in flight, nothing is coming, and it is `Absent`.
pub(crate) fn lane_existence(
    status: &NodeCheckStatus,
    cadence: WaveCadence,
) -> Option<LaneExistence> {
    match status.state {
        NodeCheckState::NotApplicable | NodeCheckState::Unsupported => None,
        NodeCheckState::Running => Some(LaneExistence::Pending { running: true }),
        NodeCheckState::Pending => Some(if cadence.covers(&status.when) {
            LaneExistence::Pending { running: false }
        } else {
            LaneExistence::Absent
        }),
        NodeCheckState::Passed | NodeCheckState::Failed | NodeCheckState::Unverified => {
            let facts = status.parser.as_deref().map(|parser| VerdictFacts {
                parser: std::borrow::Cow::Borrowed(parser),
                selection: std::borrow::Cow::Borrowed(
                    status.selection.as_deref().unwrap_or("unknown"),
                ),
                complete: status.complete.unwrap_or(false),
                passed: status.passed.unwrap_or(0),
                failed: status.failed.unwrap_or(0),
                skipped: status.skipped.unwrap_or(0),
            });
            Some(row_existence(
                status.state != NodeCheckState::Failed,
                status.failure_kind.as_deref(),
                status.suppressed_after,
                facts.as_ref(),
            ))
        }
    }
}

impl Default for NodeCheckStatusCache {
    fn default() -> Self {
        Self {
            cache: GenerationCache::new("node check projection"),
            projects: Mutex::new(HashMap::new()),
            contracts: Mutex::new(HashMap::new()),
            settled_projection_reads: AtomicU64::new(0),
            live_window_reads: AtomicU64::new(0),
        }
    }
}

impl NodeCheckStatusCache {
    fn record_read_branch(&self, live: bool) {
        let counter = if live {
            &self.live_window_reads
        } else {
            &self.settled_projection_reads
        };
        let count = counter.fetch_add(1, AtomicOrdering::Relaxed) + 1;
        log::debug!(
            "node check projection branch={} count={count}",
            if live {
                "live-window"
            } else {
                "settled-projection"
            }
        );
    }

    /// The checks contract `commit` declares, read once per commit.
    ///
    /// A commit is immutable and so is the contract it declares, so the memo
    /// needs no invalidation. A read that finds no contract is not remembered:
    /// absence can be a read that failed.
    async fn contract_at_commit(
        &self,
        object_repository: &Path,
        commit: &str,
    ) -> Option<Arc<ChecksContract>> {
        if let Some(contract) = self
            .contracts
            .lock()
            .ok()
            .and_then(|memo| memo.get(commit).cloned())
        {
            return Some(contract);
        }
        let contract = Arc::new(
            load_checks_contract_at_commit(object_repository, commit)
                .await?
                .contract,
        );
        if let Ok(mut memo) = self.contracts.lock() {
            if memo.len() >= CONTRACT_MEMO_LIMIT {
                memo.clear();
            }
            memo.insert(commit.to_string(), contract.clone());
        }
        Some(contract)
    }

    fn bind_project(&self, job_id: &str, project_id: &str) {
        if let Ok(mut projects) = self.projects.lock() {
            projects.insert(job_id.to_string(), project_id.to_string());
        }
    }

    pub(crate) fn invalidate_project_results(&self, project_id: &str) {
        let jobs = self
            .projects
            .lock()
            .map(|projects| {
                projects
                    .iter()
                    .filter(|(_, cached_project)| cached_project.as_str() == project_id)
                    .map(|(job_id, _)| job_id.clone())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        for job_id in jobs {
            self.invalidate(&job_id, "project-check-result");
        }
    }

    pub(crate) fn invalidate(&self, job_id: &str, reason: &'static str) {
        self.cache.invalidate(job_id, reason);
    }

    async fn get_or_compute<F, Fut>(&self, job_id: &str, compute: F) -> Option<Vec<NodeCheckStatus>>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Option<Vec<NodeCheckStatus>>>,
    {
        self.cache.get_or_compute(job_id, compute).await
    }

    async fn get_or_compute_live<F, Fut>(
        &self,
        job_id: &str,
        compute: F,
    ) -> Option<Vec<NodeCheckStatus>>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Option<Vec<NodeCheckStatus>>>,
    {
        self.cache
            .get_or_compute_within(job_id, LIVE_CHECK_POLL_WINDOW, compute)
            .await
    }
}

/// Which cadences have a wave in flight for a job.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct WaveCadence {
    review: bool,
    write: bool,
}

impl WaveCadence {
    pub(crate) fn of(orch: &Orchestrator, job_id: &str) -> Self {
        Self {
            review: orch.turn_end_checks_in_flight(job_id),
            write: orch.write_checks_in_flight(job_id),
        }
    }

    fn any(self) -> bool {
        self.review || self.write
    }

    /// Whether a wave that would run a lane of cadence `when` is in flight.
    pub(crate) fn covers(self, when: &str) -> bool {
        (self.review && when == CheckWhen::Review.as_str())
            || (self.write && when == CheckWhen::Write.as_str())
    }
}

#[cfg(test)]
impl WaveCadence {
    pub(crate) fn reviewing_for_test() -> Self {
        Self {
            review: true,
            write: false,
        }
    }

    pub(crate) fn writing_for_test() -> Self {
        Self {
            review: false,
            write: true,
        }
    }
}

/// A lane with nothing but its identity and state, for tests that build the
/// projection's output directly.
#[cfg(test)]
pub(crate) fn test_status(
    name: &str,
    policy: &str,
    when: &str,
    state: NodeCheckState,
) -> NodeCheckStatus {
    NodeCheckStatus {
        job_id: "job".to_string(),
        request_id: None,
        name: name.to_string(),
        state,
        policy: policy.to_string(),
        when: when.to_string(),
        cached: None,
        duration_ms: None,
        ran_at: None,
        passed: None,
        failed: None,
        skipped: None,
        deferred: None,
        parser: None,
        complete: None,
        selection: None,
        suite_failures: None,
        failure_names: Vec::new(),
        output_tail: None,
        failure_kind: None,
        suppressed_after: None,
        executor: None,
        verified_on: None,
        notice: None,
    }
}

/// Wall-clock time per phase of one status computation, logged when the whole
/// computation is slow, so a slow poll in the profiler can be attributed to
/// the step that cost it.
struct PhaseClock {
    started: Instant,
    lap_started: Instant,
    phases: Vec<(&'static str, Duration)>,
}

impl PhaseClock {
    fn start() -> Self {
        let now = Instant::now();
        Self {
            started: now,
            lap_started: now,
            phases: Vec::new(),
        }
    }

    /// Close the phase that began at the previous lap.
    fn lap(&mut self, phase: &'static str) {
        let now = Instant::now();
        self.phases.push((phase, now - self.lap_started));
        self.lap_started = now;
    }

    /// Record a phase measured inside another, such as a step of a blocking
    /// task, without moving the lap boundary.
    fn record(&mut self, phase: &'static str, elapsed: Duration) {
        self.phases.push((phase, elapsed));
    }

    fn breakdown(&self) -> String {
        self.phases
            .iter()
            .map(|(phase, elapsed)| format!("{phase}_ms={}", elapsed.as_millis()))
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn report(&self, job_id: &str, branch: &'static str) {
        let total = self.started.elapsed();
        if total < SLOW_STATUS_LOG_THRESHOLD {
            return;
        }
        log::info!(
            "node check status slow job={job_id} branch={branch} total_ms={} {}",
            total.as_millis(),
            self.breakdown()
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeCheckStatus {
    /// Stable owner used to join this lane to the canonical build-fabric row.
    pub(crate) job_id: String,
    /// The live fabric request for this lane, while one has been submitted.
    pub(crate) request_id: Option<String>,
    pub(crate) name: String,
    pub(crate) state: NodeCheckState,
    pub(crate) policy: String,
    pub(crate) when: String,
    pub(crate) cached: Option<bool>,
    pub(crate) duration_ms: Option<i64>,
    pub(crate) ran_at: Option<i64>,
    pub(crate) passed: Option<usize>,
    pub(crate) failed: Option<usize>,
    pub(crate) skipped: Option<usize>,
    /// Tests a fast-tier lane terminated at its per-test budget. Never part of
    /// `passed`; rendered beside it so a fast green is never a bare one.
    pub(crate) deferred: Option<usize>,
    /// Structured runner coverage metadata. Absence means the command supplied no
    /// canonical evidence that its successful exit verified anything.
    pub(crate) parser: Option<String>,
    pub(crate) complete: Option<bool>,
    pub(crate) selection: Option<String>,
    /// Files that failed as a whole without any test in them failing — a vitest
    /// file that could not be COLLECTED. Such a file runs no test, so folding it
    /// into `failed` renders "0 of 881 failed": a red check pointing at nothing.
    /// `None` for legacy rows and runners with no separate collection phase.
    pub(crate) suite_failures: Option<usize>,
    pub(crate) failure_names: Vec<String>,
    pub(crate) output_tail: Option<String>,
    /// Terminal classification of a FAILING check, so a surface renders the real
    /// death or evidence domain rather than an opaque red. `None` for a pass, an
    /// ordinary non-zero exit, and legacy rows.
    pub(crate) failure_kind: Option<String>,
    /// When set, Cairn has STOPPED executing this check for these inputs after
    /// this many consecutive infrastructure failures. The row it is rendered from
    /// is the last real attempt, so without this a suppressed check would read as
    /// an ordinary infrastructure red that is still being retried.
    pub(crate) suppressed_after: Option<i64>,
    /// The public name of the machine whose run is this lane's verdict (the
    /// source machine, for a reused one), so a mis-placed lane is visible
    /// without reading its log.
    pub(crate) executor: Option<String>,
    /// Set when the lane's verdict was recorded against an earlier head of this
    /// branch than the node's current one: a head the branch was replayed off
    /// by a base advance that kept its own patch. The verdict is about the same
    /// change, on the base that head was on.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) verified_on: Option<VerifiedHead>,
    /// A sentence every surface shows beside the lane: why an
    /// [`NodeCheckState::Unsupported`] lane does not run, or that a newer
    /// attempt at this lane's inputs ended without a verdict and the answer
    /// shown is the one recorded before it ([`newer_attempt_note`]).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) notice: Option<String>,
}

/// The head, and the fork point it was on, that a lane's verdict was recorded
/// against when that head is not the node's current one.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VerifiedHead {
    pub(crate) head: String,
    pub(crate) base: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub enum NodeCheckState {
    Passed,
    /// The command did not fail, but supplied no evidence that anything ran.
    Unverified,
    Failed,
    Running,
    Pending,
    NotApplicable,
    /// Declared by the contract, but in terms this build does not understand,
    /// so it never runs here. Not a verdict, and not "does not apply": it
    /// would apply on a newer build.
    Unsupported,
}

/// The lanes for the checks a contract declares that this build cannot run.
fn unsupported_lanes<'a>(
    job_id: &str,
    unsupported: &'a BTreeMap<String, UnsupportedCheck>,
) -> impl Iterator<Item = NodeCheckStatus> + 'a {
    let job_id = job_id.to_string();
    unsupported.iter().map(move |(name, check)| {
        // As declared, which may itself be the word this build lacks.
        let declared = |field: &str, default: &str| {
            check
                .declared
                .get(field)
                .and_then(serde_yaml::Value::as_str)
                .unwrap_or(default)
                .to_string()
        };
        NodeCheckStatus {
            job_id: job_id.clone(),
            request_id: None,
            name: name.clone(),
            state: NodeCheckState::Unsupported,
            policy: declared("policy", CheckPolicy::Advisory.as_str()),
            when: declared("when", CheckWhen::Write.as_str()),
            cached: None,
            duration_ms: None,
            ran_at: None,
            passed: None,
            failed: None,
            skipped: None,
            deferred: None,
            parser: None,
            complete: None,
            selection: None,
            suite_failures: None,
            failure_names: Vec::new(),
            output_tail: None,
            failure_kind: None,
            suppressed_after: None,
            executor: None,
            verified_on: None,
            notice: Some(check.notice.clone()),
        }
    })
}

/// `statuses` with the contract's unsupported lanes among them, in name order.
fn with_unsupported_lanes(
    mut statuses: Vec<NodeCheckStatus>,
    job_id: &str,
    unsupported: &BTreeMap<String, UnsupportedCheck>,
) -> Vec<NodeCheckStatus> {
    if unsupported.is_empty() {
        return statuses;
    }
    statuses.extend(unsupported_lanes(job_id, unsupported));
    statuses.sort_by(|a, b| a.name.cmp(&b.name));
    statuses
}

pub async fn node_check_statuses(
    orch: &Orchestrator,
    job_id: &str,
) -> Option<Vec<NodeCheckStatus>> {
    // Both branches go through one cache. While a wave runs, its lanes move
    // with nothing to announce each move, so the live branch is bounded by age
    // rather than fenced by invalidation; either way every concurrent observer
    // of the job joins one computation.
    let live = WaveCadence::of(orch, job_id).any();
    orch.node_check_status_cache.record_read_branch(live);
    let compute = || compute_node_check_statuses(orch, job_id);
    if live {
        orch.node_check_status_cache
            .get_or_compute_live(job_id, compute)
            .await
    } else {
        orch.node_check_status_cache
            .get_or_compute(job_id, compute)
            .await
    }
}

async fn compute_node_check_statuses(
    orch: &Orchestrator,
    job_id: &str,
) -> Option<Vec<NodeCheckStatus>> {
    let mut clock = PhaseClock::start();
    // Route to the database that owns this job (team replica or private DB); the
    // job coordinates and cached check results for a team node live in its
    // replica. A closed replica yields no statuses rather than a wrong read
    // against the private DB.
    let db = crate::execution::routing::owning_db_for_job(&orch.db, job_id)
        .await
        .ok()?;
    clock.lap("owning_db");
    let coords = resolve_job_coords(&db, job_id).await.ok().flatten()?;
    clock.lap("coords");
    orch.node_check_status_cache
        .bind_project(job_id, &coords.project_id);
    let cadence = WaveCadence::of(orch, job_id);
    // A resolved node's branch is retired, or about to be, so neither of the
    // branches below can resolve its head. Its lanes are answered from the last
    // plan a wave recorded instead. An unreadable answer falls through to them,
    // which fail the same transient way they always have.
    let retired = retired_node_in(&db, job_id)
        .await
        .map_err(|error| log::debug!("node {job_id} retirement unreadable: {error}"))
        .ok()
        .flatten();
    clock.lap("retirement");
    let (branch, statuses) = if let Some(retired) = retired {
        (
            "retired",
            retired_statuses(orch, job_id, &db, &coords, &retired, cadence).await,
        )
    } else if cadence.any() {
        (
            "live",
            live_statuses(orch, job_id, &db, &coords, cadence, &mut clock).await,
        )
    } else {
        (
            "settled",
            settled_statuses(orch, job_id, &db, &coords, cadence, &mut clock).await,
        )
    };
    clock.report(job_id, branch);
    statuses
}

/// The projection while a wave is in flight.
///
/// It reads only what the wave published when it planned: its sealed commit,
/// the contract that commit declares, its tree, and every check's input hash at
/// it. So it resolves no repository, loads no jj workspace, computes no fork
/// point, and reads no config. Keying by the wave's sealed commit rather than
/// the job's live head is also what keeps the key consistent with the wave's
/// tree hash when the branch moves on mid-wave.
async fn live_statuses(
    orch: &Orchestrator,
    job_id: &str,
    db: &Arc<LocalDb>,
    coords: &JobCoords,
    cadence: WaveCadence,
    clock: &mut PhaseClock,
) -> Option<Vec<NodeCheckStatus>> {
    let Some(status) = orch.turn_end_check_runtime_status(job_id) else {
        // Planning has not published yet, or only the write cadence is running.
        // Every lane stays pending rather than showing stale rows from an
        // earlier run of this long-lived job, which needs only the check names:
        // the contract at the node's logical head.
        let logical_head = resolve_job_logical_head(orch, job_id).await.ok()?;
        clock.lap("logical_head");
        let contract = orch
            .node_check_status_cache
            .contract_at_commit(Path::new(&coords.repository_path), &logical_head)
            .await?;
        clock.lap("contract");
        let statuses = assemble_statuses(
            orch,
            job_id,
            &contract.checks,
            LaneVerdicts::default(),
            None,
            &orch.recent_turn_end_check_request_ids(job_id),
            cadence,
        );
        return Some(with_unsupported_lanes(
            statuses,
            job_id,
            &contract.unsupported,
        ));
    };
    let plan = &status.plan;
    if plan.checks.is_empty() {
        return Some(unsupported_lanes(job_id, &plan.unsupported).collect());
    }

    let rows = plan_verdict_rows(db, &coords.project_id, job_id, plan).await;
    clock.lap("head_verdicts");
    // Only a base replay that kept the branch's patch moves the head without
    // withdrawing the wave, so a wave pinned to an earlier head is answering
    // for the same change on the base that head was on, and its lanes say which
    // head that was. Reading the node's head is the one VCS step this branch
    // takes, and only when there is a verdict to label; the live window bounds
    // it to once per poll tick per job.
    let mut verified_on = HashMap::new();
    if !rows.is_empty() {
        let head = resolve_job_logical_head(orch, job_id).await.ok();
        clock.lap("logical_head");
        if head.is_some_and(|head| head != plan.head) {
            let verified = VerifiedHead {
                head: plan.head.clone(),
                base: plan.base.clone(),
            };
            verified_on = rows
                .iter()
                .map(|lane| (lane.row.check_name.clone(), verified.clone()))
                .collect();
        }
    }
    let statuses = assemble_statuses(
        orch,
        job_id,
        &plan.checks,
        LaneVerdicts {
            rows: rows_by_name(rows),
            verified_on,
        },
        Some(&plan.applicable_names),
        &status.request_ids,
        cadence,
    );
    Some(with_unsupported_lanes(statuses, job_id, &plan.unsupported))
}

/// The verdict rows that answer a plan's lanes: each check's row at the plan's
/// tree, or the reusable verdict for its input hash. A pure database read, which
/// is what lets both the live and the retired projection serve it without
/// touching the repository.
async fn plan_verdict_rows(
    db: &Arc<LocalDb>,
    project_id: &str,
    job_id: &str,
    plan: &Arc<WavePlan>,
) -> Vec<LaneRow> {
    let project_id = project_id.to_string();
    let read_plan = plan.clone();
    let head = OwnedHeadVerdictKeys {
        tree_hash: plan.tree_hash.clone(),
        input_hashes: plan.input_hashes.clone(),
        job_id: job_id.to_string(),
        commit_sha: plan.head.clone(),
    };
    db.read(move |conn| {
        Box::pin(async move {
            head_verdicts_conn(conn, &project_id, &read_plan.checks, head.keys()).await
        })
    })
    .await
    .map_err(|error| log::debug!("node {job_id} check verdicts unreadable: {error}"))
    .map(|rows| attribute_reuse(rows, &plan.tree_hash))
    .unwrap_or_default()
}

/// A node whose issue has resolved. Its branch is retired, or is about to be,
/// and nothing will move its lanes again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RetiredNode {
    /// The issue's terminal status: `merged` or `closed`.
    pub(crate) outcome: String,
    /// The last plan a review wave recorded for this node. Its head is the last
    /// one the node's lanes were planned against, which for a merged node is the
    /// head the merge was judged on. `None` when no wave ever recorded one.
    pub(crate) plan: Option<Arc<WavePlan>>,
}

impl RetiredNode {
    pub(crate) fn head(&self) -> Option<&str> {
        self.plan.as_deref().map(|plan| plan.head.as_str())
    }
}

/// Whether `job_id` belongs to a resolved issue, and if so the last plan its
/// lanes were recorded against.
///
/// Retirement is read from the issue rather than inferred from an unresolvable
/// branch: a branch that fails to resolve is just as often a busy repository,
/// and answering that from a recorded plan would show a live node stale lanes.
/// A job with no branch of its own never had lanes, and is never retired.
pub(crate) async fn retired_node_in(
    db: &LocalDb,
    job_id: &str,
) -> Result<Option<RetiredNode>, String> {
    let row = db
        .query_opt(
            "SELECT i.status,
                    (SELECT a.plan_json FROM turn_end_check_attempts a
                      WHERE a.job_id = j.id AND a.plan_json IS NOT NULL
                      ORDER BY a.created_at DESC, a.id DESC LIMIT 1)
               FROM jobs j JOIN issues i ON i.id = j.issue_id
              WHERE j.id = ?1 AND i.status IN ('merged', 'closed')
                AND j.branch IS NOT NULL AND j.branch <> ''",
            (job_id.to_string(),),
            |row| Ok((row.text(0)?, row.opt_text(1)?)),
        )
        .await
        .map_err(|error| error.to_string())?;
    let Some((outcome, plan_json)) = row else {
        return Ok(None);
    };
    let plan = plan_json.and_then(|json| {
        serde_json::from_str::<WavePlan>(&json)
            .map_err(|error| {
                log::warn!("node {job_id}: recorded wave plan is unreadable ({error})")
            })
            .ok()
            .map(Arc::new)
    });
    Ok(Some(RetiredNode { outcome, plan }))
}

/// [`retired_node_in`] against the database that owns the job.
pub(crate) async fn node_retirement(orch: &Orchestrator, job_id: &str) -> Option<RetiredNode> {
    let db = crate::execution::routing::owning_db_for_job(&orch.db, job_id)
        .await
        .ok()?;
    retired_node_in(&db, job_id)
        .await
        .map_err(|error| log::debug!("node {job_id} retirement unreadable: {error}"))
        .ok()
        .flatten()
}

/// The projection for a retired node: its last recorded plan's lanes, keyed
/// exactly as the live projection keys them, so a wave that was still running
/// at the merge and recorded afterwards is shown too. It reads nothing but the
/// database. `None` when no wave ever recorded a plan, which callers tell apart
/// from an unreadable node through [`node_retirement`].
async fn retired_statuses(
    orch: &Orchestrator,
    job_id: &str,
    db: &Arc<LocalDb>,
    coords: &JobCoords,
    retired: &RetiredNode,
    cadence: WaveCadence,
) -> Option<Vec<NodeCheckStatus>> {
    let plan = retired.plan.as_ref()?;
    let rows = plan_verdict_rows(db, &coords.project_id, job_id, plan).await;
    let statuses = assemble_statuses(
        orch,
        job_id,
        &plan.checks,
        LaneVerdicts {
            rows: rows_by_name(rows),
            verified_on: HashMap::new(),
        },
        Some(&plan.applicable_names),
        &orch.recent_turn_end_check_request_ids(job_id),
        cadence,
    );
    Some(with_unsupported_lanes(statuses, job_id, &plan.unsupported))
}

/// The exact settled projection: what applies to this head and which verdict
/// answers each check, resolved against the same sealed tree the runners key by.
/// It is computed once per generation, so its VCS work is paid once per
/// change rather than once per poll.
async fn settled_statuses(
    orch: &Orchestrator,
    job_id: &str,
    db: &Arc<LocalDb>,
    coords: &JobCoords,
    cadence: WaveCadence,
    clock: &mut PhaseClock,
) -> Option<Vec<NodeCheckStatus>> {
    let project_root = std::path::PathBuf::from(&coords.repository_path);
    let logical_repository = {
        let jj_binary_path = orch.jj_binary_path.clone();
        let config_dir = orch.config_dir.clone();
        let project_repo = project_root.clone();
        tokio::task::spawn_blocking(move || {
            let jj = crate::jj::environment(&jj_binary_path, &config_dir);
            crate::jj::coordinate_repository(&jj, &config_dir, &project_repo)
        })
        .await
        .ok()?
        .ok()?
    };
    clock.lap("coordinate_repository");
    let logical_head = resolve_job_logical_head(orch, job_id).await.ok()?;
    clock.lap("logical_head");
    // The same live fork point the review cadence gates on. Reading the
    // recorded `jobs.base_commit` here would show a node checks that its own
    // suite never planned, because that row does not follow a base advance.
    let live_range =
        crate::diff::live_job_branch_range(db, job_id, &orch.jj_binary_path, &orch.config_dir)
            .await
            .map_err(|error| {
                log::debug!("node {job_id} check status: no live base coordinate ({error})");
            })
            .ok()
            .flatten();
    clock.lap("live_range");
    // The status a node renders must be the suite its own cadences plan, so the
    // contract comes from the node's logical head — the same commit the cadences
    // read it from. Reading the project checkout here would show a node checks
    // that its own suite never selected (CAIRN-3333).
    let contract = orch
        .node_check_status_cache
        .contract_at_commit(&project_root, &logical_head)
        .await?;
    clock.lap("contract");
    if contract.checks.is_empty() {
        return Some(unsupported_lanes(job_id, &contract.unsupported).collect());
    }

    // Status resolution waits on jj, cargo metadata, and the synchronous cache
    // bridge. Routing and config loading above stay async; the complete status
    // snapshot below belongs on the blocking pool so rendering `/checks` cannot
    // park a runtime worker.
    let status_db = db.clone();
    let status_job_id = job_id.to_string();
    let status_project_id = coords.project_id.clone();
    let status_contract = contract.clone();
    let status_jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let (rows_by_name, applicable_names, verified_on, verdicts_elapsed) =
        tokio::task::spawn_blocking(move || {
            let checks = &status_contract.checks;
            // The projection must answer both "does this check apply" and "which
            // verdict answers it" exactly as the runners do, so it resolves each
            // check's inputs against the same sealed tree they key by, once.
            let entries = if any_check_declares_inputs(checks.values()) {
                crate::jj::tree_entries(&status_jj, &logical_repository, &logical_head).ok()
            } else {
                None
            };
            let blobs = TreeBlobs {
                jj: &status_jj,
                repository: &logical_repository,
            };
            let snapshot = TreeSnapshot::new(entries.as_deref(), &blobs);
            let inputs = ResolvedInputs::resolve(checks, &status_contract.extra_inputs, &snapshot);
            let mut verdicts_elapsed = Duration::ZERO;
            let head_tree = logical_tree_hash(&status_jj, &logical_repository, &logical_head).ok();
            let live_rows = head_tree.as_deref().and_then(|tree_hash| {
                let input_hashes =
                    check_input_hashes(checks, &inputs, entries.as_deref(), tree_hash);
                let verdicts_started = Instant::now();
                let rows = head_verdicts(
                    status_db.clone(),
                    &status_project_id,
                    checks,
                    HeadVerdictKeys {
                        tree_hash,
                        input_hashes: &input_hashes,
                        job_id: &status_job_id,
                        commit_sha: &logical_head,
                    },
                );
                verdicts_elapsed = verdicts_started.elapsed();
                rows.ok().map(|rows| attribute_reuse(rows, tree_hash))
            });
            let mut rows = live_rows
                .or_else(|| list_check_results_for_job(status_db.clone(), &status_job_id).ok())
                .unwrap_or_default();
            let changed = live_range.as_ref().and_then(|range| {
                logical_changed_files(&status_jj, &logical_repository, &range.base, &logical_head)
                    .inspect_err(|error| {
                        log::debug!(
                            "check status: changed files of {}..{logical_head} unreadable: {error}",
                            range.base
                        );
                    })
                    .ok()
            });
            let applicable_names = changed.as_ref().map(|changed| {
                plan_checks(checks, &inputs, changed, &project_root)
                    .into_iter()
                    .filter(|plan| plan.applies)
                    .map(|plan| plan.name)
                    .collect::<HashSet<_>>()
            });
            // An applicable lane with no verdict at this head may still hold one
            // this job recorded on an earlier head carrying the same change, when
            // base advances replayed the branch while its wave ran.
            let mut verified_on = HashMap::new();
            if let (Some(head_tree), Some(range), Some(applicable)) = (
                head_tree.as_deref(),
                live_range.as_ref(),
                applicable_names.as_ref(),
            ) {
                let missing: HashSet<String> = applicable
                    .iter()
                    .filter(|name| !rows.iter().any(|lane| &lane.row.check_name == *name))
                    .cloned()
                    .collect();
                if !missing.is_empty() {
                    let job_rows = list_check_results_for_job(status_db.clone(), &status_job_id)
                        .unwrap_or_default()
                        .into_iter()
                        .map(|lane| lane.row)
                        .collect();
                    for (row, verified) in carry_forward_unchanged_patch(
                        &missing,
                        &logical_head,
                        head_tree,
                        job_rows,
                        |row| {
                            observed_commit_for_job(status_db.clone(), &status_job_id, row)
                                .ok()
                                .flatten()
                        },
                        |commit| {
                            own_patch_against(
                                &status_jj,
                                &logical_repository,
                                commit,
                                &range.target_head,
                            )
                            .ok()
                        },
                    ) {
                        verified_on.insert(row.check_name.clone(), verified);
                        rows.push(row.into());
                    }
                }
            }
            (
                rows_by_name(rows),
                applicable_names,
                verified_on,
                verdicts_elapsed,
            )
        })
        .await
        .ok()?;
    clock.lap("blocking");
    clock.record("blocking.head_verdicts", verdicts_elapsed);

    let statuses = assemble_statuses(
        orch,
        job_id,
        &contract.checks,
        LaneVerdicts {
            rows: rows_by_name,
            verified_on,
        },
        applicable_names.as_ref(),
        &orch.recent_turn_end_check_request_ids(job_id),
        cadence,
    );
    Some(with_unsupported_lanes(
        statuses,
        job_id,
        &contract.unsupported,
    ))
}

fn rows_by_name(rows: Vec<LaneRow>) -> HashMap<String, LaneRow> {
    rows.into_iter()
        .map(|lane| (lane.row.check_name.clone(), lane))
        .collect()
}

/// What answered each lane: its verdict row by check name, and, for a verdict
/// carried from an earlier head, the head it was recorded on.
#[derive(Default)]
struct LaneVerdicts {
    rows: HashMap<String, LaneRow>,
    verified_on: HashMap<String, VerifiedHead>,
}

/// One status row per configured check, in name order, from what the branch
/// resolved: the verdicts that answered its lanes, which checks apply (`None`
/// when that could not be decided), and the fabric request each lane was
/// submitted as.
fn assemble_statuses(
    orch: &Orchestrator,
    job_id: &str,
    checks: &HashMap<String, CheckCommand>,
    verdicts: LaneVerdicts,
    applicable_names: Option<&HashSet<String>>,
    request_ids: &HashMap<String, String>,
    cadence: WaveCadence,
) -> Vec<NodeCheckStatus> {
    let LaneVerdicts {
        rows: rows_by_name,
        verified_on,
    } = verdicts;
    let mut names = checks.keys().cloned().collect::<Vec<_>>();
    names.sort();
    names
        .into_iter()
        .map(|name| {
            let check = checks.get(&name).expect("name came from checks map");

            // A review check does not apply when the impact gate excluded it
            // from this tree's plan; it will never run, so it is neither
            // running nor pending.
            let not_applicable = applicable_names.is_some_and(|names| !names.contains(&name));

            // `LaneEvidence::Recorded` outranks everything below, and this
            // returns before the `started` probe so a lane that already holds
            // a verdict never pays for a filesystem check it cannot use.
            if let Some(lane) = rows_by_name.get(&name) {
                let row = &lane.row;
                let mut status = status_from_row(
                    job_id,
                    request_ids.get(&name).cloned(),
                    &name,
                    check.policy,
                    check.when,
                    row,
                    row.executor_id
                        .as_deref()
                        .map(|id| orch.fleet.executor_name_for_text(id)),
                );
                status.verified_on = verified_on.get(&name).cloned();
                status.notice = newer_attempt_note(lane);
                return status;
            }

            // Turn-end review checks run CONCURRENTLY in isolated COW clones
            // (or sequentially in the shared worktree on the clone-unavailable
            // fallback), each into its OWN log file created the instant it
            // starts. Existence of that file — not a non-empty tail — is the
            // RUNNING signal, so a silent-but-active check is not mistaken for a
            // queued one; under isolation several checks read as running at
            // once, while a not-yet-started (or fallback-queued) check has no
            // file yet and stays pending. The tail is read separately and is
            // None while a running check has yet to emit.
            let started = (cadence.review && check.when == CheckWhen::Review
                || cadence.write && check.when == CheckWhen::Write)
                && !not_applicable
                && (cadence.write || turn_end_check_started(orch, job_id, &name));

            let state = match lane_evidence(false, not_applicable, started) {
                LaneEvidence::NotApplicable => NodeCheckState::NotApplicable,
                LaneEvidence::Running => NodeCheckState::Running,
                // `Recorded` is unreachable here: the row arm returned above.
                LaneEvidence::Recorded | LaneEvidence::Pending => NodeCheckState::Pending,
            };

            // Only the actively-running check carries a live tail (and it may
            // still be None before its first line); pending and not-applicable
            // rows have none.
            let output_tail = if started {
                read_turn_end_log_tail(orch, job_id, &name)
            } else {
                None
            };

            NodeCheckStatus {
                job_id: job_id.to_string(),
                request_id: request_ids.get(&name).cloned(),
                name,
                state,
                policy: check.policy.as_str().to_string(),
                when: check.when.as_str().to_string(),
                cached: None,
                duration_ms: None,
                ran_at: None,
                passed: None,
                failed: None,
                skipped: None,
                deferred: None,
                parser: None,
                complete: None,
                selection: None,
                suite_failures: None,
                failure_names: Vec::new(),
                output_tail,
                failure_kind: None,
                suppressed_after: None,
                executor: None,
                verified_on: None,
                notice: None,
            }
        })
        .collect()
}

/// Verdicts this job recorded on an earlier head whose own patch is the current
/// head's, for the `missing` lanes: those the current head applies but holds no
/// verdict for.
///
/// A base advance that replays a branch without changing its patch does not
/// withdraw the review wave running on it, so that wave records against the
/// pre-replay head, whose tree (and usually its lanes' input hashes) the current
/// head does not share. The verdict is still about the change the branch claims,
/// so the lane carries it, naming the head and base it was recorded on. A head
/// whose patch differs (a commit since, a conflict resolution, a replay that
/// altered a hunk) carries nothing, and neither does anything unreadable.
///
/// `commit_of` names the commit a row was observed on; `patch_of` reads a head's
/// own patch against the integration target. Each head's patch is read once.
fn carry_forward_unchanged_patch(
    missing: &HashSet<String>,
    head: &str,
    head_tree: &str,
    job_rows: Vec<CheckResultCacheEntry>,
    mut commit_of: impl FnMut(&CheckResultCacheEntry) -> Option<String>,
    mut patch_of: impl FnMut(&str) -> Option<OwnPatch>,
) -> Vec<(CheckResultCacheEntry, VerifiedHead)> {
    let mut patches: HashMap<String, Option<OwnPatch>> = HashMap::new();
    let mut carried = Vec::new();
    for row in job_rows {
        if !missing.contains(&row.check_name) || row.tree_hash == head_tree {
            continue;
        }
        let Some(commit) = commit_of(&row).filter(|commit| commit != head) else {
            continue;
        };
        if !patches.contains_key(head) {
            patches.insert(head.to_string(), patch_of(head));
        }
        if !patches.contains_key(&commit) {
            let patch = patch_of(&commit);
            patches.insert(commit.clone(), patch);
        }
        let (Some(Some(current)), Some(Some(earlier))) = (patches.get(head), patches.get(&commit))
        else {
            continue;
        };
        if current.patch == earlier.patch {
            let verified = VerifiedHead {
                head: commit,
                base: earlier.base.clone(),
            };
            carried.push((row, verified));
        }
    }
    carried
}

/// Mark every row reached from another tree as `cached`.
///
/// A row from another tree reaches a head only as the verdict reuse would serve
/// for the check's input hash ([`head_verdicts`]). That verdict is true of this
/// head — the check's inputs are byte-identical — but it was not produced here,
/// and the surface
/// must say so: a reused verdict that rendered as fresh would tell a reader the
/// head was exercised when it was not. The stored flag cannot say this on its
/// own, because the source row of a reuse is a fresh write (`cached = 0`) and the
/// reuse itself records only a commit alias.
fn attribute_reuse(mut rows: Vec<LaneRow>, head_tree_hash: &str) -> Vec<LaneRow> {
    for lane in &mut rows {
        if lane.row.tree_hash != head_tree_hash {
            lane.row.cached = Some(true);
        }
    }
    rows
}

/// What a lane knows about one check, in the order the answers outrank each
/// other.
///
/// The one ordering decision worth stating is `Recorded` over `NotApplicable`.
/// It matters in exactly one situation: a check the impact gate would NOT have
/// selected for this node's diff, whose verdict some other evaluation of the
/// IDENTICAL tree already recorded. A whole-tree default-branch attestation is
/// the case that actually happens — it plans over the entire tree, so it records
/// verdicts for checks no particular diff would select.
///
/// The verdict wins. `NotApplicable` asserts that a check will never run and has
/// nothing to say about this head, and that is simply false once evidence keyed
/// to this head exists. Lanes are read by the check's input hash at this head,
/// so such a verdict is true of this head by construction: same inputs, same
/// result. The impact gate decides what to RUN; it does not decide what is true.
///
/// The consequence is deliberate and should not surprise a reader of `/checks`:
/// a lane can carry a real verdict — including a red — for a check this node's
/// own change would not have selected, and a settle-wait will report it. That is
/// a fact about the tree the node is sitting on, which is the question a lane
/// answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LaneEvidence {
    /// A verdict recorded against this node's sealed tree.
    Recorded,
    /// The impact gate excluded this check from the tree's plan.
    NotApplicable,
    /// A wave has started this check and it has not returned.
    Running,
    /// Selected, with nothing back yet.
    Pending,
}

pub(crate) fn lane_evidence(has_row: bool, not_applicable: bool, started: bool) -> LaneEvidence {
    if has_row {
        LaneEvidence::Recorded
    } else if not_applicable {
        LaneEvidence::NotApplicable
    } else if started {
        LaneEvidence::Running
    } else {
        LaneEvidence::Pending
    }
}

fn status_from_row(
    job_id: &str,
    request_id: Option<String>,
    name: &str,
    policy: CheckPolicy,
    when: CheckWhen,
    row: &CheckResultCacheEntry,
    executor: Option<String>,
) -> NodeCheckStatus {
    let parsed = row
        .target_results_json
        .as_deref()
        .and_then(|s| serde_json::from_str::<ParsedCheckResult>(s).ok());
    let failure_kind = row
        .failure_kind
        .as_deref()
        .and_then(CheckFailureKind::from_stored);
    let mut failure_names: Vec<String> = parsed
        .as_ref()
        .map(|p| p.failures.iter().map(|f| f.name.clone()).collect())
        .unwrap_or_default();
    // A timeout has no failing tests to name; surface the tests still running
    // when it was killed (nextest SLOW lines) so the wake detail answers "what
    // was it doing when it died?".
    if failure_kind == Some(CheckFailureKind::TimedOut) && failure_names.is_empty() {
        failure_names = extract_running_tests(&row.output_tail);
    }
    let output_tail = if row.passed {
        None
    } else {
        Some(format_failure_excerpt(
            parsed.as_ref(),
            row.output_tail.trim_end(),
        ))
        .filter(|s| !s.trim().is_empty())
    };
    // A lane never offered to placement holds no verdict either way, so it reads
    // as unverified rather than as a red, and it has no run to time.
    let attempted_nothing = failure_kind.is_some_and(CheckFailureKind::attempted_nothing);
    NodeCheckStatus {
        job_id: job_id.to_string(),
        request_id,
        name: name.to_string(),
        state: if attempted_nothing {
            NodeCheckState::Unverified
        } else {
            check_state_from_evidence(row.passed, parsed.as_ref())
        },
        policy: policy.as_str().to_string(),
        when: when.as_str().to_string(),
        cached: row.cached,
        duration_ms: (!attempted_nothing).then_some(row.duration_ms),
        ran_at: Some(row.ran_at),
        passed: parsed.as_ref().map(|p| p.passed),
        failed: parsed.as_ref().map(|p| p.failed),
        skipped: parsed.as_ref().map(|p| p.skipped),
        deferred: parsed.as_ref().map(|p| p.deferred),
        parser: parsed.as_ref().map(|p| p.parser.clone()),
        complete: parsed.as_ref().map(|p| p.complete),
        selection: parsed.as_ref().map(|p| p.selection.clone()),
        suite_failures: parsed.as_ref().map(|p| p.suite_failures),
        failure_names,
        output_tail,
        failure_kind: row.failure_kind.clone(),
        suppressed_after: (row.infra_failure_streak
            >= crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND)
            .then_some(row.infra_failure_streak),
        executor,
        verified_on: None,
        notice: None,
    }
}

/// The fast-tier label for a lane whose stored parse declared `selection: fast`.
fn status_fast_tier_label(status: &NodeCheckStatus) -> Option<String> {
    (status.selection.as_deref() == Some(crate::execution::check_parsers::FAST_TIER_SELECTION))
        .then(|| {
            crate::execution::check_parsers::fast_tier_label(
                status.passed.unwrap_or(0) + status.failed.unwrap_or(0),
                status.deferred.unwrap_or(0),
                status.skipped.unwrap_or(0),
            )
        })
}

pub(crate) fn format_status_annotation(status: &NodeCheckStatus) -> Option<String> {
    // Checked ahead of suppression: a head row that was bounded earlier and is
    // now not attempted at all says the newer, more useful thing.
    if status.failure_kind.as_deref() == Some(CheckFailureKind::NotPlaceable.as_str()) {
        return Some(not_placeable_annotation(status.output_tail.as_deref()));
    }
    if status.failure_kind.as_deref() == Some(CheckFailureKind::ConfigUnreadable.as_str()) {
        return Some(UNREADABLE_CONFIG_ANNOTATION.to_string());
    }
    // A suppressed check produced no verdict at this tree — Cairn declined to run
    // it. Every arm below describes a verdict, so saying any of them here would
    // dress the absence of a result up as one.
    if status.suppressed_after.is_some() {
        return Some("suppressed".to_string());
    }
    if status.state == NodeCheckState::Unsupported {
        return status.notice.clone();
    }
    let mut parts = Vec::new();
    match status.state {
        NodeCheckState::Unverified => {
            let reason = match (
                status.selection.as_deref(),
                status.complete,
                status.skipped.unwrap_or(0),
            ) {
                _ if status.parser.as_deref() == Some(RUST_LINT_PARSER) => {
                    "the lint did not run to completion".to_string()
                }
                (Some("empty"), _, _) => "no tests matched the change".to_string(),
                (_, _, skipped) if skipped > 0 => format!("no tests ran, {skipped} skipped"),
                (Some("full" | "filtered"), Some(false), _) => {
                    "test enumeration incomplete".to_string()
                }
                (Some("full" | "filtered"), _, _) => "no tests ran".to_string(),
                _ => "no structured coverage result".to_string(),
            };
            parts.push(format!("unverified — {reason}"));
        }
        NodeCheckState::Passed => {
            // Only a test runner's tallies are test counts. A lint or typecheck
            // parse carries zeros there, which would read as "no tests matched".
            let test_counts = match (status.parser.as_deref(), status.passed, status.failed) {
                (Some(parser), Some(passed), Some(failed)) if is_test_runner_parser(parser) => {
                    Some((passed, failed))
                }
                _ => None,
            };
            if let Some(label) = status_fast_tier_label(status) {
                // A fast-tier green covers the change's own tests and every
                // test that fit the budget, never the whole suite; its label
                // says so in place of the bare test count.
                parts.push(label);
            } else if let Some((passed, failed)) = test_counts {
                let total = passed + failed;
                // Mirrors `execution::checks::summary_annotation`: a skipped test
                // never disappears into a green, and a suite that skipped itself
                // entirely reads differently from one whose selector matched
                // nothing (CAIRN-3164).
                let skipped = status.skipped.unwrap_or(0);
                match (total, skipped) {
                    (0, 0) => parts.push("no tests matched the change".to_string()),
                    (0, skipped) => parts.push(format!("no tests ran, {skipped} skipped")),
                    (total, 0) => parts.push(format!("{total} tests")),
                    (total, skipped) => parts.push(format!("{total} tests, {skipped} skipped")),
                }
                if status.selection.as_deref() == Some("filtered") {
                    parts.push("filtered selection".to_string());
                }
            } else if let Some(ms) = status.duration_ms {
                parts.push(format_check_duration(ms));
            }
        }
        NodeCheckState::Failed => {
            if let Some(annotation) = failed_annotation(
                status
                    .failure_kind
                    .as_deref()
                    .and_then(CheckFailureKind::from_stored),
                status.duration_ms,
                status.passed,
                status.failed,
                status.suite_failures,
                &status.failure_names,
            ) {
                parts.push(annotation);
            }
            // A red fast-tier lane still names what it did not judge.
            if let Some(label) = status_fast_tier_label(status) {
                parts.push(label);
            }
        }
        _ => {}
    }
    if let Some(machine) = &status.executor {
        parts.push(format!("on {machine}"));
    }
    if status.cached == Some(true) {
        parts.push("cached".to_string());
    }
    if let Some(verified) = &status.verified_on {
        parts.push(format!(
            "verified on earlier head {} over base {}; base since advanced, diff unchanged",
            short_commit(&verified.head),
            short_commit(&verified.base)
        ));
    }
    if let Some(notice) = &status.notice {
        parts.push(notice.clone());
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join(", "))
    }
}

fn short_commit(commit: &str) -> &str {
    &commit[..commit.len().min(12)]
}

/// The line a not-placeable lane renders as: the fleet's first sentence of why,
/// and when it will run. The machine inventory after it stays in the stored row.
fn not_placeable_annotation(output_tail: Option<&str>) -> String {
    let why = output_tail
        .and_then(|tail| tail.split(". ").next())
        .map(|sentence| sentence.trim().trim_end_matches('.'))
        .filter(|sentence| !sentence.is_empty())
        .unwrap_or("no machine in the fleet can run it");
    format!("not placeable — {why}; runs when a machine that satisfies it attaches")
}

/// The line a lane renders as while the project's config does not load in
/// this build. Why it does not load is in the stored row, and in the refusal
/// every batch in the project gets.
const UNREADABLE_CONFIG_ANNOTATION: &str =
    "not run — this build cannot read the project's .cairn/config.yaml; runs once it loads";

fn row_kind_is(row: &CheckResultCacheEntry, kind: CheckFailureKind) -> bool {
    row.failure_kind.as_deref() == Some(kind.as_str())
}

fn row_attempted_nothing(row: &CheckResultCacheEntry) -> bool {
    row.failure_kind
        .as_deref()
        .and_then(CheckFailureKind::from_stored)
        .is_some_and(CheckFailureKind::attempted_nothing)
}

/// Failing lanes, and lanes without a verdict, that get their own detail line in
/// a wake body before the rest are rolled up by name. A wake is read at the top
/// of a resume prompt: it names the reds and stops, and `/checks` holds the
/// complete listing.
const WAKE_DETAILED_FAILURES: usize = 4;
/// Trailing chars of a failing lane's output kept when the lane named no failing
/// test. Without it such a lane — a compiler, a linter, a build — would say only
/// that it failed, which is the thing this body exists to stop doing.
const WAKE_EXCERPT_CHARS: usize = 600;
/// Check names listed in a wake's green roll-up before it collapses to a count.
const WAKE_ROLLUP_NAMES: usize = 12;

/// The body a delivered `turn-checks:` attention push carries: the verdicts
/// themselves, so a woken agent reads WHAT failed instead of a pointer to where
/// the answer lives. `None` when this job has recorded no verdict at all, which
/// leaves the caller its reference-only line.
///
/// Deliberately NOT [`node_check_statuses`]. That resolves the live tree hash,
/// the base fork point and the whole impact plan (jj, plus `cargo metadata` for
/// target expansion) — seconds of work on a busy repository, taken while resume
/// assembly holds the launch lock. A wake speaks about verdicts already recorded,
/// so it reads exactly those: one indexed query for the rows at each check's
/// latest inputs, chosen by [`select_lane_row`]. The consequence is that a lane's
/// last recorded verdict may predate this node's current tree; the URI in the
/// wake header is the live surface.
pub(crate) async fn check_wake_body(orch: &Orchestrator, job_id: &str) -> Option<String> {
    let db = crate::execution::routing::owning_db_for_job(&orch.db, job_id)
        .await
        .ok()?;
    match crate::execution::cache::latest_check_results_for_job(&db, job_id).await {
        Ok(lanes) => format_check_wake_body(&lanes),
        Err(error) => {
            log::debug!("check wake body for job {job_id}: {error}");
            None
        }
    }
}

/// Pure renderer for [`check_wake_body`], so the shape a wake speaks in is
/// unit-tested without a database.
///
/// Every lane lands in exactly one bucket by [`entry_existence`], the same
/// classification the turn-end checks section and the merge gate use: a
/// verdict is passing or failing, and a lane that ended without one (killed,
/// suppressed, an empty selection, a substrate failure) is listed as itself
/// and counted in neither. The headline's failing count is therefore only ever
/// reds about the change, and the passing list only ever greens that judged it.
pub(crate) fn format_check_wake_body(lanes: &[LaneRow]) -> Option<String> {
    if lanes.is_empty() {
        return None;
    }
    let mut lanes: Vec<&LaneRow> = lanes.iter().collect();
    lanes.sort_by(|left, right| left.row.check_name.cmp(&right.row.check_name));
    let mut failing: Vec<&CheckResultCacheEntry> = Vec::new();
    let mut no_verdict: Vec<(&CheckResultCacheEntry, LaneExistence)> = Vec::new();
    let mut passing: Vec<String> = Vec::new();
    let mut not_applicable: Vec<String> = Vec::new();
    let mut unplaceable: Vec<String> = Vec::new();
    let mut unreadable: Vec<String> = Vec::new();
    for lane in &lanes {
        let row = &lane.row;
        // Never offered to placement: listed by why, apart from every bucket
        // that counts a run.
        if row_attempted_nothing(row) {
            if row_kind_is(row, CheckFailureKind::ConfigUnreadable) {
                unreadable.push(row.check_name.clone());
            } else {
                unplaceable.push(row.check_name.clone());
            }
            continue;
        }
        match entry_existence(row) {
            LaneExistence::Complete if row.passed => passing.push(row.check_name.clone()),
            LaneExistence::Complete => failing.push(row),
            LaneExistence::NoSelectableWork => not_applicable.push(row.check_name.clone()),
            existence => no_verdict.push((row, existence)),
        }
    }
    let judged = passing.len() + failing.len() + no_verdict.len();
    let clean = failing.is_empty() && no_verdict.is_empty();
    let mut out = match (failing.len(), no_verdict.len()) {
        (0, 0) if passing.is_empty() => "Project checks on this node: no lane holds a verdict.".to_string(),
        (0, 0) => format!(
            "Project checks on this node: all {} passing ({}).",
            passing.len(),
            join_capped(&passing, WAKE_ROLLUP_NAMES)
        ),
        (0, silent) => format!(
            "Project checks on this node: none failing; {silent} of {judged} without a verdict.\n"
        ),
        (failed, 0) => format!("Project checks on this node: {failed} of {judged} failing.\n"),
        (failed, silent) => format!(
            "Project checks on this node: {failed} of {judged} failing, {silent} without a verdict.\n"
        ),
    };
    for row in failing.iter().take(WAKE_DETAILED_FAILURES) {
        out.push_str(&format!(
            "\n- \u{2717} {} \u{2014} {}",
            row.check_name,
            wake_failure_detail(row)
        ));
    }
    if failing.len() > WAKE_DETAILED_FAILURES {
        let rest: Vec<String> = failing
            .iter()
            .skip(WAKE_DETAILED_FAILURES)
            .map(|row| row.check_name.clone())
            .collect();
        out.push_str(&format!(
            "\n- \u{2717} also failing: {}",
            join_capped(&rest, WAKE_ROLLUP_NAMES)
        ));
    }
    for (row, existence) in no_verdict.iter().take(WAKE_DETAILED_FAILURES) {
        out.push_str(&format!(
            "\n- \u{2298} {} \u{2014} {}",
            row.check_name,
            wake_no_verdict_detail(row, existence)
        ));
    }
    if no_verdict.len() > WAKE_DETAILED_FAILURES {
        let rest: Vec<String> = no_verdict
            .iter()
            .skip(WAKE_DETAILED_FAILURES)
            .map(|(row, _)| row.check_name.clone())
            .collect();
        out.push_str(&format!(
            "\n- \u{2298} also without a verdict: {}",
            join_capped(&rest, WAKE_ROLLUP_NAMES)
        ));
    }
    if !clean && !passing.is_empty() {
        out.push_str(&format!(
            "\n- \u{2713} passing: {}",
            join_capped(&passing, WAKE_ROLLUP_NAMES)
        ));
    }
    if !not_applicable.is_empty() {
        out.push_str(&format!(
            "\n- not applicable (nothing in their domain changed): {}",
            join_capped(&not_applicable, WAKE_ROLLUP_NAMES)
        ));
    }
    for (names, why) in [
        (
            &unplaceable,
            "not placeable (no machine in the fleet can run them yet)",
        ),
        (
            &unreadable,
            "not run (this build cannot read the project's .cairn/config.yaml)",
        ),
    ] {
        if !names.is_empty() {
            out.push_str(&format!(
                "\n- ⚠ {why}: {}",
                join_capped(names, WAKE_ROLLUP_NAMES)
            ));
        }
    }
    for lane in &lanes {
        if let Some(note) = newer_attempt_note(lane) {
            out.push_str(&format!("\n- {}: {note}", lane.row.check_name));
        }
    }
    Some(out)
}

/// What a lane that ended without a verdict says for itself in a wake body:
/// what kept it from judging the change. Never a verdict sentence, which would
/// send the reader after a failure the change did not cause.
fn wake_no_verdict_detail(row: &CheckResultCacheEntry, existence: &LaneExistence) -> String {
    match existence {
        LaneExistence::Breaker { after } => {
            format!("not run \u{2014} suppressed after {after} infrastructure failures")
        }
        LaneExistence::Died { .. } => {
            format!(
                "died without a verdict \u{2014} {}",
                wake_failure_detail(row)
            )
        }
        LaneExistence::Empty => {
            let parsed = row
                .target_results_json
                .as_deref()
                .and_then(|json| serde_json::from_str::<ParsedCheckResult>(json).ok());
            let reason = match verdict_evidence(row.passed, parsed.as_ref()) {
                VerdictEvidence::Unverified(reason) => reason.to_string(),
                _ => CheckFailureKind::Selection.describe(row.duration_ms),
            };
            format!("no verdict \u{2014} {reason}")
        }
        other => format!("no verdict \u{2014} {}", other.describe()),
    }
}

/// What one failing lane, or one that died, says for itself in a wake body: the
/// sentence for how it ended, then the concrete failures — named tests when the
/// runner named them, and the tail of the output when it did not.
fn wake_failure_detail(row: &CheckResultCacheEntry) -> String {
    let parsed = row
        .target_results_json
        .as_deref()
        .and_then(|json| serde_json::from_str::<ParsedCheckResult>(json).ok());
    let kind = row
        .failure_kind
        .as_deref()
        .and_then(CheckFailureKind::from_stored);
    let mut names: Vec<String> = parsed
        .as_ref()
        .map(|p| p.failures.iter().map(|f| f.name.clone()).collect())
        .unwrap_or_default();
    if kind == Some(CheckFailureKind::TimedOut) && names.is_empty() {
        names = extract_running_tests(&row.output_tail);
    }
    let annotation = failed_annotation(
        kind,
        Some(row.duration_ms),
        parsed.as_ref().map(|p| p.passed),
        parsed.as_ref().map(|p| p.failed),
        parsed.as_ref().map(|p| p.suite_failures),
        &names,
    )
    .unwrap_or_else(|| format!("exit {}", row.exit_code));

    // Named failures ARE the answer to "what broke", and a timeout has already
    // spent its names on the still-running list. A check's own report follows
    // the names, since it carries the verdict headers and pre-existing count the
    // names alone do not. The output tail is the fallback for a lane that named
    // nothing at all.
    let named = kind != Some(CheckFailureKind::TimedOut) && !names.is_empty();
    let header = if named {
        format!("{annotation}: {}", join_capped(&names, MAX_FAILURE_NAMES))
    } else {
        annotation
    };
    let report = parsed
        .as_ref()
        .and_then(|p| p.report.as_deref())
        .filter(|report| !report.trim().is_empty());
    let excerpt = match report {
        Some(report) => report_excerpt(report, row.output_tail.trim_end(), WAKE_EXCERPT_CHARS),
        None if named => return header,
        None => {
            let excerpt = format_failure_excerpt(parsed.as_ref(), row.output_tail.trim_end());
            error_anchored_tail(excerpt.trim_end(), WAKE_EXCERPT_CHARS)
        }
    };
    if excerpt.trim().is_empty() {
        return header;
    }
    let indented = excerpt
        .lines()
        .map(|line| format!("    {line}"))
        .collect::<Vec<_>>()
        .join("\n");
    format!("{header}\n{indented}")
}

/// The sentence a FAILING lane renders as, written from recorded facts alone.
///
/// Shared by the `/checks` lane annotation and the attention wake body so a red
/// is described the same way wherever it is read: the wake that rouses the author
/// and the listing they open next must not disagree about what happened.
/// `None` when the row carries neither a classified death nor a test tally — the
/// caller then says what it can (an exit code) rather than inventing a verdict.
///
/// `names` are the failing test names, or — for a timeout, which has none — the
/// tests still running when the check was killed.
pub(crate) fn failed_annotation(
    failure_kind: Option<CheckFailureKind>,
    duration_ms: Option<i64>,
    passed: Option<usize>,
    failed: Option<usize>,
    suite_failures: Option<usize>,
    names: &[String],
) -> Option<String> {
    if let Some(kind) = failure_kind {
        // A classified death renders AS itself ("timed out after 30m",
        // "failed to spawn"), never a bare "N of M failed" the agent
        // would chase into tests that never failed.
        let mut s = if kind == CheckFailureKind::RunnerError {
            match passed.unwrap_or(0) {
                0 => "test runner failed before reporting tests".to_string(),
                passed => format!(
                    "test runner failed after {passed} tests passed with no assertion failures"
                ),
            }
        } else {
            kind.describe(duration_ms.unwrap_or(0))
        };
        if kind == CheckFailureKind::TimedOut && !names.is_empty() {
            s.push_str(&format!(
                "; still running: {}",
                join_capped(names, MAX_FAILURE_NAMES)
            ));
        }
        return Some(s);
    }
    let (Some(failed), Some(passed)) = (failed, passed) else {
        return None;
    };
    // Mirrors `execution::checks::summary_annotation`: a file that failed to
    // collect ran no test, so it is counted as itself rather than folded into an
    // assertion tally that would read "0 of 881 failed".
    let suites = suite_failures.unwrap_or(0);
    let mut segments = Vec::new();
    if failed == 0 && passed == 0 && suites == 0 {
        return Some("check failed before reporting test failures — unverified".to_string());
    }
    if failed > 0 {
        segments.push(format!("{failed} of {} failed", failed + passed));
    }
    if suites > 0 {
        let noun = if suites == 1 { "suite" } else { "suites" };
        segments.push(format!("{suites} {noun} failed to load"));
    }
    Some(segments.join(", "))
}

/// Comma-join check or test names, capped so a wide fan-out doesn't flood a line.
fn join_capped(names: &[String], max: usize) -> String {
    let shown: Vec<&str> = names.iter().take(max).map(String::as_str).collect();
    let more = names.len().saturating_sub(shown.len());
    if more > 0 {
        format!("{}, +{more} more", shown.join(", "))
    } else {
        shown.join(", ")
    }
}

fn format_check_duration(ms: i64) -> String {
    if ms >= 1000 {
        format!("{:.1}s", ms as f64 / 1000.0)
    } else {
        format!("{ms}ms")
    }
}

pub(crate) fn formatted_failure_names(status: &NodeCheckStatus) -> Option<String> {
    let parsed = ParsedCheckResult {
        schema_version: 1,
        complete: false,
        selection: "unknown".to_string(),
        tests: vec![],
        undeclared_skips: 0,
        parser: "node-status".to_string(),
        passed: status.passed.unwrap_or(0),
        failed: status.failed.unwrap_or(status.failure_names.len()),
        skipped: status.skipped.unwrap_or(0),
        suite_failures: status.suite_failures.unwrap_or(0),
        deferred: 0,
        report: None,
        failures: status
            .failure_names
            .iter()
            .map(|name| crate::execution::check_parsers::CheckFailure {
                name: name.clone(),
                message: None,
            })
            .collect(),
    };
    format_failure_names(&parsed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[test]
    fn a_check_this_build_cannot_run_is_an_unsupported_lane_with_its_notice() {
        let mut older = crate::models::CheckVocabulary::this_build();
        older.when.retain(|word| word != "landing");
        let declared: serde_yaml::Value =
            serde_yaml::from_str("command: cargo check\nwhen: landing\npolicy: gate\n").unwrap();
        let crate::models::DeclaredCheck::Unsupported(unsupported) =
            crate::models::read_declared_check(declared, &older).unwrap()
        else {
            panic!("`when: landing` is unsupported without the variant");
        };
        let set = BTreeMap::from([("landing-compile".to_string(), unsupported)]);

        let lanes = with_unsupported_lanes(Vec::new(), "job", &set);
        assert_eq!(lanes.len(), 1);
        let lane = &lanes[0];
        assert_eq!(lane.state, NodeCheckState::Unsupported);
        assert_eq!(lane.when, "landing");
        assert_eq!(lane.policy, "gate");
        let notice = format_status_annotation(lane).unwrap();
        assert!(
            notice.starts_with(
                "needs a newer Cairn: `when: landing` is not understood by this build"
            ),
            "{notice}"
        );
        let json = serde_json::to_value(lane).unwrap();
        assert_eq!(json["state"], "unsupported");
        assert_eq!(json["notice"], serde_json::Value::String(notice));
    }

    /// A wave records its plan as JSON, and both the live and the retired
    /// projection list the plan's unsupported checks. The recorded form must
    /// keep each declared entry, which the lane reads `when` and `policy` from,
    /// and a plan recorded before the field existed must still read.
    #[test]
    fn a_recorded_wave_plan_keeps_the_checks_this_build_cannot_run() {
        let mut older = crate::models::CheckVocabulary::this_build();
        older.when.retain(|word| word != "landing");
        let declared: serde_yaml::Value =
            serde_yaml::from_str("command: cargo check\nwhen: landing\npolicy: gate\n").unwrap();
        let crate::models::DeclaredCheck::Unsupported(unsupported) =
            crate::models::read_declared_check(declared, &older).unwrap()
        else {
            panic!("`when: landing` is unsupported without the variant");
        };
        let plan = WavePlan {
            head: "head".to_string(),
            base: "base".to_string(),
            tree_hash: "tree".to_string(),
            checks: Default::default(),
            applicable_names: Default::default(),
            input_hashes: Default::default(),
            unsupported: BTreeMap::from([("landing-compile".to_string(), unsupported)]),
        };

        let recorded: WavePlan =
            serde_json::from_str(&serde_json::to_string(&plan).unwrap()).unwrap();
        assert_eq!(recorded, plan);
        let lanes: Vec<_> = unsupported_lanes("job", &recorded.unsupported).collect();
        assert_eq!(lanes.len(), 1);
        assert_eq!(lanes[0].state, NodeCheckState::Unsupported);
        assert_eq!(
            (lanes[0].when.as_str(), lanes[0].policy.as_str()),
            ("landing", "gate")
        );

        let before_the_field: WavePlan = serde_json::from_str(
            r#"{"head":"h","base":"b","treeHash":"t","checks":{},"applicableNames":[],"inputHashes":{}}"#,
        )
        .unwrap();
        assert!(before_the_field.unsupported.is_empty());
    }

    #[test]
    fn node_check_read_branches_are_counted_independently() {
        let cache = NodeCheckStatusCache::default();
        cache.record_read_branch(false);
        cache.record_read_branch(false);
        cache.record_read_branch(true);

        assert_eq!(
            cache.settled_projection_reads.load(AtomicOrdering::Relaxed),
            2
        );
        assert_eq!(cache.live_window_reads.load(AtomicOrdering::Relaxed), 1);
    }

    /// Every observer polling a running wave at once shares one computation:
    /// the fleet overlay, the PR view, and each window's own query key all
    /// resolve to the same job, which is where the fan-out is cut.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_live_polls_for_one_job_compute_once() {
        let cache = Arc::new(NodeCheckStatusCache::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(tokio::sync::Barrier::new(11));
        let mut readers = Vec::new();
        for _ in 0..11 {
            let cache = cache.clone();
            let calls = calls.clone();
            let barrier = barrier.clone();
            readers.push(tokio::spawn(async move {
                barrier.wait().await;
                cache
                    .get_or_compute_live("job", || {
                        let calls = calls.clone();
                        async move {
                            calls.fetch_add(1, Ordering::SeqCst);
                            tokio::time::sleep(Duration::from_millis(20)).await;
                            Some(Vec::new())
                        }
                    })
                    .await
            }));
        }
        for reader in readers {
            assert_eq!(reader.await.unwrap(), Some(Vec::new()));
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // Settlement advances the generation, so the next read, live or not,
        // never serves the live value.
        cache.invalidate("job", "check-settlement");
        cache
            .get_or_compute("job", || {
                let calls = calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Some(Vec::new())
                }
            })
            .await;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn the_phase_breakdown_names_each_phase_in_order() {
        let mut clock = PhaseClock::start();
        clock.lap("owning_db");
        clock.record("blocking.head_verdicts", Duration::from_millis(42));
        let breakdown = clock.breakdown();
        assert!(breakdown.starts_with("owning_db_ms="), "{breakdown}");
        assert!(
            breakdown.ends_with(" blocking.head_verdicts_ms=42"),
            "{breakdown}"
        );
    }

    #[tokio::test]
    async fn settled_projection_coalesces_concurrent_misses_and_reuses_warm_value() {
        let cache = Arc::new(NodeCheckStatusCache::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let mut readers = Vec::new();
        for _ in 0..16 {
            let cache = cache.clone();
            let calls = calls.clone();
            readers.push(tokio::spawn(async move {
                cache
                    .get_or_compute("job", || {
                        let calls = calls.clone();
                        async move {
                            calls.fetch_add(1, Ordering::SeqCst);
                            tokio::task::yield_now().await;
                            Some(Vec::new())
                        }
                    })
                    .await
            }));
        }
        for reader in readers {
            assert_eq!(reader.await.unwrap(), Some(Vec::new()));
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        assert_eq!(
            cache
                .get_or_compute("job", || async {
                    panic!("warm projection recomputed");
                })
                .await,
            Some(Vec::new())
        );
    }

    #[tokio::test]
    async fn invalidation_during_compute_retries_new_generation() {
        let cache = Arc::new(NodeCheckStatusCache::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let reader = {
            let cache = cache.clone();
            let calls = calls.clone();
            let entered = entered.clone();
            let release = release.clone();
            tokio::spawn(async move {
                cache
                    .get_or_compute("job", || {
                        let calls = calls.clone();
                        let entered = entered.clone();
                        let release = release.clone();
                        async move {
                            let call = calls.fetch_add(1, Ordering::SeqCst);
                            if call == 0 {
                                entered.notify_one();
                                release.notified().await;
                            }
                            Some(Vec::new())
                        }
                    })
                    .await
            })
        };
        entered.notified().await;
        cache.invalidate("job", "test-boundary");
        release.notify_one();
        assert_eq!(reader.await.unwrap(), Some(Vec::new()));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn invalidation_is_scoped_to_one_job() {
        let cache = NodeCheckStatusCache::default();
        let calls = AtomicUsize::new(0);
        for job in ["a", "b"] {
            cache
                .get_or_compute(job, || async {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Some(Vec::new())
                })
                .await;
        }
        cache.invalidate("a", "lifecycle");
        for job in ["a", "b"] {
            cache
                .get_or_compute(job, || async {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Some(Vec::new())
                })
                .await;
        }
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn project_result_invalidates_every_bound_job_but_not_other_projects() {
        let cache = NodeCheckStatusCache::default();
        let calls = AtomicUsize::new(0);
        for (job, project) in [("a", "project"), ("b", "project"), ("c", "other")] {
            cache.bind_project(job, project);
            cache
                .get_or_compute(job, || async {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Some(Vec::new())
                })
                .await;
        }
        cache.invalidate_project_results("project");
        for job in ["a", "b", "c"] {
            cache
                .get_or_compute(job, || async {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Some(Vec::new())
                })
                .await;
        }
        assert_eq!(calls.load(Ordering::SeqCst), 5);
    }

    fn verdict_row(name: &str, passed: bool) -> CheckResultCacheEntry {
        CheckResultCacheEntry {
            project_id: "project-a".to_string(),
            tree_hash: "tree".to_string(),
            input_hash: format!("ih-{name}"),
            check_name: name.to_string(),
            exit_code: if passed { 0 } else { 1 },
            passed,
            output_tail: String::new(),
            duration_ms: 1_000,
            ran_at: 1,
            target_results_json: None,
            job_id: Some("job".to_string()),
            cached: Some(false),
            failure_kind: None,
            infra_failure_streak: 0,
            executor_id: None,
            executor_device_id: None,
            executor_connection_generation: None,
            executor_cell_id: None,
            executor_lease_epoch: None,
            executor_started_at_unix_ms: None,
            executor_finished_at_unix_ms: None,
            toolchain_fingerprint: None,
            defined_by_commit_sha: Some("commit-a".to_string()),
        }
    }

    fn own_patch(base: &str, patch: &str) -> crate::jj::OwnPatch {
        crate::jj::OwnPatch {
            base: base.to_string(),
            patch: patch.to_string(),
        }
    }

    /// A wave kept across base replays records on the pre-replay head. A lane
    /// the current head has no verdict for carries that verdict exactly when the
    /// earlier head's own patch is the current head's, naming that head and the
    /// base it was on; a head whose patch differs carries nothing, and a lane
    /// that already holds a verdict here, or a row from this very tree, is left
    /// alone.
    #[test]
    fn a_verdict_carries_forward_only_from_a_head_with_the_same_patch() {
        let row = |name: &str, tree: &str| CheckResultCacheEntry {
            tree_hash: tree.to_string(),
            ..verdict_row(name, name != "red")
        };
        let rows = vec![
            row("red", "tree-replayed-off"),
            row("green", "tree-replayed-off"),
            row("edited", "tree-before-commit"),
            row("answered", "tree-replayed-off"),
            row("here", "tree-head"),
        ];
        let missing: HashSet<String> = ["red", "green", "edited", "here"]
            .into_iter()
            .map(String::from)
            .collect();
        let mut patch_reads = Vec::new();
        let carried = carry_forward_unchanged_patch(
            &missing,
            "head",
            "tree-head",
            rows,
            |row| {
                Some(match row.tree_hash.as_str() {
                    "tree-replayed-off" => "replayed-off".to_string(),
                    "tree-before-commit" => "before-commit".to_string(),
                    _ => "head".to_string(),
                })
            },
            |commit| {
                patch_reads.push(commit.to_string());
                Some(match commit {
                    "head" => own_patch("main-now", "+change\n"),
                    "replayed-off" => own_patch("main-then", "+change\n"),
                    _ => own_patch("main-then", "+partial\n"),
                })
            },
        );
        let names: Vec<_> = carried
            .iter()
            .map(|(row, verified)| (row.check_name.as_str(), verified.clone()))
            .collect();
        let verified = VerifiedHead {
            head: "replayed-off".to_string(),
            base: "main-then".to_string(),
        };
        assert_eq!(
            names,
            vec![("red", verified.clone()), ("green", verified)],
            "a red carries as faithfully as a green"
        );
        patch_reads.sort();
        assert_eq!(
            patch_reads,
            vec!["before-commit", "head", "replayed-off"],
            "each head's patch is read once"
        );
    }

    #[test]
    fn an_unreadable_patch_carries_nothing() {
        let missing: HashSet<String> = ["rust".to_string()].into_iter().collect();
        let carried = carry_forward_unchanged_patch(
            &missing,
            "head",
            "tree-head",
            vec![verdict_row("rust", true)],
            |_| Some("earlier".to_string()),
            |commit| (commit == "earlier").then(|| own_patch("main", "+x\n")),
        );
        assert!(carried.is_empty());
    }

    fn parsed_json(passed: usize, failures: &[&str]) -> String {
        serde_json::to_string(&ParsedCheckResult {
            schema_version: 1,
            complete: true,
            selection: "full".to_string(),
            tests: vec![],
            undeclared_skips: 0,
            parser: "nextest".to_string(),
            passed,
            failed: failures.len(),
            skipped: 0,
            suite_failures: 0,
            deferred: 0,
            report: None,
            failures: failures
                .iter()
                .map(|name| crate::execution::check_parsers::CheckFailure {
                    name: name.to_string(),
                    message: None,
                })
                .collect(),
        })
        .unwrap()
    }

    /// A wake body over rows that each answer their own lane.
    fn wake(rows: impl IntoIterator<Item = CheckResultCacheEntry>) -> Option<String> {
        let lanes: Vec<LaneRow> = rows.into_iter().map(LaneRow::from).collect();
        format_check_wake_body(&lanes)
    }

    /// A row that ended without a verdict, recorded `ran_at` after the epoch.
    fn died_row(name: &str, kind: CheckFailureKind, ran_at: i64) -> CheckResultCacheEntry {
        CheckResultCacheEntry {
            exit_code: 143,
            failure_kind: Some(kind.as_str().to_string()),
            output_tail: "Cairn: check process was terminated by SIGTERM (exit 143)".to_string(),
            ran_at,
            ..verdict_row(name, false)
        }
    }

    /// A green whose own result says the selection matched nothing.
    fn empty_selection_row(name: &str) -> CheckResultCacheEntry {
        let mut parsed: ParsedCheckResult = serde_json::from_str(&parsed_json(0, &[])).unwrap();
        parsed.selection = "empty".to_string();
        CheckResultCacheEntry {
            target_results_json: Some(serde_json::to_string(&parsed).unwrap()),
            ..verdict_row(name, true)
        }
    }

    /// The ruling on cairn/4650: for one lane at one set of inputs, an answer
    /// outranks any attempt that produced none, in either order of arrival, and
    /// the attempt is kept beside it only when it came after.
    #[test]
    fn a_verdict_outranks_a_newer_attempt_without_one_in_either_order() {
        let pass = CheckResultCacheEntry {
            ran_at: 100,
            ..verdict_row("frontend-tests", true)
        };
        let attempt = died_row("frontend-tests", CheckFailureKind::Infrastructure, 200);

        let lane = select_lane_row([attempt.clone(), pass.clone()]).unwrap();
        assert_eq!(lane.row, pass);
        assert_eq!(lane.newer_attempt, Some(attempt.clone()));
        let note = newer_attempt_note(&lane).unwrap();
        assert!(
            note.starts_with(
                "latest attempt: no verdict (died: infrastructure); last verdict: passed"
            ),
            "{note}"
        );

        let earlier_attempt = CheckResultCacheEntry {
            ran_at: 50,
            ..attempt.clone()
        };
        let lane = select_lane_row([pass.clone(), earlier_attempt]).unwrap();
        assert_eq!(lane.row, pass);
        assert_eq!(lane.newer_attempt, None, "an older attempt is not news");

        let red = CheckResultCacheEntry {
            ran_at: 100,
            ..verdict_row("frontend-tests", false)
        };
        let lane = select_lane_row([attempt.clone(), red.clone()]).unwrap();
        assert_eq!(lane.row, red, "a red is an answer too");

        let lane = select_lane_row([attempt.clone()]).unwrap();
        assert_eq!(
            lane.row, attempt,
            "with no answer, the attempt stands as itself"
        );
        assert_eq!(lane.newer_attempt, None);

        let newer_pass = CheckResultCacheEntry {
            ran_at: 300,
            ..verdict_row("frontend-tests", true)
        };
        let lane = select_lane_row([newer_pass.clone(), red]).unwrap();
        assert_eq!(lane.row, newer_pass, "between answers, the newer wins");
    }

    /// Specimen 1 (cairn/4604): a suppressed lane opened a wake with "2 of 5
    /// failing". It holds no verdict, so it is counted in neither bucket.
    #[test]
    fn a_suppressed_lane_is_not_counted_as_failing() {
        let mut suppressed = verdict_row("executor-windows-check", false);
        suppressed.failure_kind = Some("infrastructure".to_string());
        suppressed.infra_failure_streak = crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND;

        let body = wake([suppressed, verdict_row("lint", true)]).unwrap();

        assert!(
            body.starts_with(
                "Project checks on this node: none failing; 1 of 2 without a verdict."
            ),
            "{body}"
        );
        assert!(
            body.contains("⊘ executor-windows-check — not run — suppressed after"),
            "{body}"
        );
        assert!(!body.contains('✗'), "{body}");
        assert!(body.contains("✓ passing: lint"), "{body}");
    }

    /// Specimen 2 (cairn/4609): the summary listed an empty-selection lane
    /// under passing while `/checks` called it unverified. It judged nothing,
    /// so it is never a green.
    #[test]
    fn an_empty_selection_is_never_listed_as_passing() {
        let body = wake([
            empty_selection_row("rust-tests"),
            verdict_row("rust-fmt", true),
        ])
        .unwrap();

        assert!(
            body.contains("⊘ rust-tests — no verdict — no tests matched the selection"),
            "{body}"
        );
        assert!(body.contains("✓ passing: rust-fmt"), "{body}");
        assert!(!body.contains("passing: rust-fmt, rust-tests"), "{body}");
        assert!(!body.contains("all 2 passing"), "{body}");
    }

    /// Specimen 4: a run the OOM killer stopped is a death, not a verdict, and
    /// beside a real red the headline counts only the red.
    #[test]
    fn a_killed_run_renders_as_a_death_and_only_reds_are_counted() {
        let body = wake([
            died_row("rust-tests", CheckFailureKind::Killed, 1),
            verdict_row("typecheck", false),
            verdict_row("lint", true),
        ])
        .unwrap();

        assert!(
            body.starts_with("Project checks on this node: 1 of 3 failing, 1 without a verdict."),
            "{body}"
        );
        assert!(
            body.contains("⊘ rust-tests — died without a verdict — killed (signal)"),
            "{body}"
        );
        assert!(!body.contains("✗ rust-tests"), "{body}");
        assert!(body.contains("✗ typecheck"), "{body}");
    }

    /// cairn://posts/453: the lane keeps its pass, and the wake says a newer
    /// attempt produced no verdict instead of waking the author over it.
    #[test]
    fn a_wake_notes_a_newer_attempt_beside_the_verdict_it_kept() {
        let pass = CheckResultCacheEntry {
            ran_at: 100,
            ..verdict_row("frontend-tests", true)
        };
        let lane = select_lane_row([
            died_row("frontend-tests", CheckFailureKind::Infrastructure, 200),
            pass,
        ])
        .unwrap();

        let body = format_check_wake_body(&[lane]).unwrap();

        assert!(
            body.starts_with("Project checks on this node: all 1 passing (frontend-tests)."),
            "{body}"
        );
        assert!(
            body.contains("- frontend-tests: latest attempt: no verdict (died: infrastructure)"),
            "{body}"
        );
    }

    /// The whole point of the wake body: an agent roused by a red check learns
    /// WHICH lane went red and WHAT failed in it, without spending a turn
    /// reading somewhere else to find out.
    #[test]
    fn a_checks_wake_names_the_failing_lane_and_its_tests() {
        let mut failing = verdict_row("rust-tests", false);
        failing.target_results_json =
            Some(parsed_json(64, &["cairn_core a::b", "cairn_core c::d"]));
        let rows = vec![
            failing,
            verdict_row("rust-fmt", true),
            verdict_row("lint", true),
        ];

        let body = wake(rows).unwrap();

        assert!(
            body.starts_with("Project checks on this node: 1 of 3 failing."),
            "{body}"
        );
        assert!(
            body.contains("✗ rust-tests — 2 of 66 failed: cairn_core a::b, cairn_core c::d"),
            "{body}"
        );
        assert!(body.contains("✓ passing: lint, rust-fmt"), "{body}");
    }

    /// A compiler or linter names no test, so without its output tail the lane
    /// would say only that it failed — the pointer-shaped wake this replaced.
    #[test]
    fn a_lane_that_named_no_failure_carries_its_output_tail() {
        let mut failing = verdict_row("typecheck", false);
        failing.output_tail =
            "src/a.ts(3,1): error TS2322: Type 'string' is not assignable.".to_string();

        let body = wake([failing]).unwrap();

        assert!(body.contains("✗ typecheck — exit 1"), "{body}");
        assert!(
            body.contains("    src/a.ts(3,1): error TS2322"),
            "the tail is indented under its lane: {body}"
        );
    }

    /// Green rides along passively, so it costs no turn — but it still says what
    /// it covered rather than sending the reader to a resource.
    #[test]
    fn a_clean_run_says_so_in_one_line() {
        let body = wake([verdict_row("lint", true), verdict_row("api", true)]).unwrap();

        assert_eq!(
            body,
            "Project checks on this node: all 2 passing (api, lint)."
        );
        assert!(
            wake([]).is_none(),
            "no recorded verdict leaves the caller its reference-only line"
        );
    }

    /// The row `record_unplaceable_check` writes for `executor-windows-check`
    /// against a fleet with no Windows machine.
    fn not_placeable_row() -> CheckResultCacheEntry {
        let mut row = verdict_row("executor-windows-check", false);
        row.exit_code = -1;
        row.duration_ms = 0;
        row.failure_kind = Some(CheckFailureKind::NotPlaceable.as_str().to_string());
        row.output_tail = "No machine enrolled in this fleet, attached or not, satisfies \
             toolchains rust on windows. Known machines: bgsrv-ub (linux; rust). Not attempted: \
             it runs on the first wave after a machine that satisfies it attaches."
            .to_string();
        row
    }

    /// cairn/4982: a lane no machine can take is not a red. It reads as
    /// unverified with the fleet's own reason, carries no run duration, and a
    /// wake lists it apart from the failing lanes rather than among them.
    #[test]
    fn a_not_placeable_lane_renders_as_unverified_and_stays_out_of_the_reds() {
        let (state, annotation) = annotate(&not_placeable_row());
        assert_eq!(state, NodeCheckState::Unverified);
        assert_eq!(
            annotation,
            "not placeable — No machine enrolled in this fleet, attached or not, satisfies \
             toolchains rust on windows; runs when a machine that satisfies it attaches"
        );

        let body = wake([not_placeable_row(), verdict_row("lint", true)]).unwrap();
        assert_eq!(
            body,
            "Project checks on this node: all 1 passing (lint).\n- ⚠ not placeable (no machine \
             in the fleet can run them yet): executor-windows-check"
        );

        let body = wake([
            not_placeable_row(),
            verdict_row("lint", true),
            verdict_row("typecheck", false),
        ])
        .unwrap();
        assert!(
            body.starts_with("Project checks on this node: 1 of 2 failing."),
            "{body}"
        );
        assert!(!body.contains("✗ executor-windows-check"), "{body}");
    }

    /// A suppressed lane produced no verdict at all; the wake must not dress the
    /// last real attempt up as this run's result.
    #[test]
    fn a_suppressed_lane_says_it_was_not_run() {
        let mut row = verdict_row("rust-tests", false);
        row.failure_kind = Some("infrastructure".to_string());
        row.infra_failure_streak = crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND;

        let body = wake([row]).unwrap();

        assert!(body.contains("not run — suppressed after"), "{body}");
    }

    /// The precedence CAIRN-3823 made reachable again, recorded so the next
    /// reader inherits a decision rather than an accident.
    ///
    /// While verdict rows were invisible, a check the impact gate excluded could
    /// only ever render `not applicable`. Now that they are visible, a whole-tree
    /// default-branch attestation can leave a verdict at a tree for a check no
    /// particular diff would select — and a node sitting on that identical tree
    /// shows it, red included, rather than claiming the check has nothing to say.
    #[test]
    fn a_recorded_verdict_outranks_the_impact_gate() {
        assert_eq!(
            lane_evidence(true, true, false),
            LaneEvidence::Recorded,
            "evidence about this exact tree outranks a plan that would not have run it"
        );
        assert_eq!(
            lane_evidence(true, false, true),
            LaneEvidence::Recorded,
            "a returned verdict outranks a lane still reading as started"
        );
        assert_eq!(
            lane_evidence(false, true, false),
            LaneEvidence::NotApplicable
        );
        assert_eq!(lane_evidence(false, false, true), LaneEvidence::Running);
        assert_eq!(lane_evidence(false, false, false), LaneEvidence::Pending);
    }

    /// The one classification every surface shares, case by case.
    #[test]
    fn verdict_evidence_classifies_each_shape_of_exit() {
        let nextest = |passed: usize, skipped: usize, complete: bool, selection: &str| {
            let mut parsed: ParsedCheckResult =
                serde_json::from_str(&parsed_json(passed, &[])).unwrap();
            parsed.parser = "nextest".into();
            parsed.skipped = skipped;
            parsed.complete = complete;
            parsed.selection = selection.into();
            parsed
        };
        assert_eq!(verdict_evidence(false, None), VerdictEvidence::Failed);
        assert_eq!(
            verdict_evidence(true, None),
            VerdictEvidence::Unverified(UnverifiedReason::NoStructuredResult)
        );
        assert_eq!(
            verdict_evidence(true, Some(&nextest(3, 0, true, "full"))),
            VerdictEvidence::Verified
        );
        assert_eq!(
            verdict_evidence(true, Some(&nextest(0, 0, true, "empty"))),
            VerdictEvidence::Unverified(UnverifiedReason::EmptySelection)
        );
        assert_eq!(
            verdict_evidence(true, Some(&nextest(0, 4, true, "full"))),
            VerdictEvidence::Unverified(UnverifiedReason::AllSkipped(4))
        );
        assert_eq!(
            verdict_evidence(true, Some(&nextest(3, 0, false, "full"))),
            VerdictEvidence::Unverified(UnverifiedReason::IncompleteEnumeration)
        );
        assert_eq!(
            verdict_evidence(true, Some(&nextest(0, 0, true, "full"))),
            VerdictEvidence::Unverified(UnverifiedReason::NoTestsRan)
        );
        assert_eq!(
            UnverifiedReason::AllSkipped(4).to_string(),
            "no tests ran; 4 skipped"
        );
    }

    #[test]
    fn successful_test_runs_without_executed_tests_are_unverified() {
        for (selection, skipped, expected) in
            [("empty", 0, "no tests matched"), ("full", 8, "8 skipped")]
        {
            let mut row = verdict_row("rust-tests", true);
            let mut parsed: ParsedCheckResult = serde_json::from_str(&parsed_json(0, &[])).unwrap();
            parsed.selection = selection.to_string();
            parsed.skipped = skipped;
            row.target_results_json = Some(serde_json::to_string(&parsed).unwrap());

            let status = status_from_row(
                "job",
                None,
                "rust-tests",
                CheckPolicy::Advisory,
                CheckWhen::Review,
                &row,
                None,
            );
            assert_eq!(status.state, NodeCheckState::Unverified);
            assert!(
                format_status_annotation(&status)
                    .unwrap()
                    .contains(expected),
                "{status:?}"
            );
        }
    }

    #[test]
    fn a_success_without_a_canonical_result_is_unverified_but_tsc_is_verified() {
        let row = verdict_row("lint", true);
        let status = status_from_row(
            "job",
            None,
            "lint",
            CheckPolicy::Advisory,
            CheckWhen::Review,
            &row,
            None,
        );
        assert_eq!(status.state, NodeCheckState::Unverified);

        let mut tsc = verdict_row("typecheck", true);
        let parsed = ParsedCheckResult {
            schema_version: 1,
            complete: true,
            selection: "full".into(),
            tests: vec![],
            undeclared_skips: 0,
            parser: "tsc".into(),
            passed: 0,
            failed: 0,
            suite_failures: 0,
            deferred: 0,
            report: None,
            skipped: 0,
            failures: vec![],
        };
        tsc.target_results_json = Some(serde_json::to_string(&parsed).unwrap());
        let status = status_from_row(
            "job",
            None,
            "typecheck",
            CheckPolicy::Advisory,
            CheckWhen::Review,
            &tsc,
            None,
        );
        assert_eq!(status.state, NodeCheckState::Passed);
    }

    fn rust_lint_row(passed: bool, complete: bool, failures: &[&str]) -> CheckResultCacheEntry {
        let mut row = verdict_row("rust-lint", passed);
        row.target_results_json = Some(
            serde_json::to_string(&ParsedCheckResult {
                schema_version: 1,
                complete,
                selection: "unknown".to_string(),
                tests: vec![],
                undeclared_skips: 0,
                parser: RUST_LINT_PARSER.to_string(),
                passed: 0,
                failed: failures.len(),
                skipped: 0,
                suite_failures: 0,
                deferred: 0,
                report: Some("✓ clippy: no new findings in your diff".to_string()),
                failures: failures
                    .iter()
                    .map(|name| crate::execution::check_parsers::CheckFailure {
                        name: name.to_string(),
                        message: None,
                    })
                    .collect(),
            })
            .unwrap(),
        );
        row
    }

    fn annotate(row: &CheckResultCacheEntry) -> (NodeCheckState, String) {
        let status = status_from_row(
            "job",
            None,
            &row.check_name,
            CheckPolicy::Advisory,
            CheckWhen::Review,
            row,
            None,
        );
        let annotation = format_status_annotation(&status).unwrap_or_default();
        (status.state, annotation)
    }

    /// The cairn/4820 shape: a passing rust-lint row once rendered
    /// `[unverified] unverified — no structured coverage result` because it had
    /// no parse. With a complete result it is a plain pass, and its zero test
    /// tallies never read as "no tests matched".
    #[test]
    fn a_complete_rust_lint_pass_reads_as_a_pass() {
        let (state, annotation) = annotate(&rust_lint_row(true, true, &[]));
        assert_eq!(state, NodeCheckState::Passed);
        assert_eq!(annotation, "1.0s");
    }

    #[test]
    fn an_incomplete_rust_lint_exit_zero_is_unverified_and_says_why() {
        let (state, annotation) = annotate(&rust_lint_row(true, false, &[]));
        assert_eq!(state, NodeCheckState::Unverified);
        assert_eq!(
            annotation,
            "unverified — the lint did not run to completion"
        );
        assert!(!annotation.contains("no structured coverage result"));

        let (state, _) = annotate(&rust_lint_row(false, false, &[]));
        assert_eq!(state, NodeCheckState::Failed, "a red stays red");
    }

    /// tsc's `complete: false` means "not a test enumeration", so a passing
    /// typecheck stays verified, and its pass reads as a duration.
    #[test]
    fn a_passing_typecheck_stays_verified_and_reads_as_its_duration() {
        let mut row = verdict_row("typecheck", true);
        row.target_results_json = Some(
            r#"{"parser":"tsc","complete":false,"passed":0,"failed":0,"skipped":0,"failures":[]}"#
                .to_string(),
        );
        let (state, annotation) = annotate(&row);
        assert_eq!(state, NodeCheckState::Passed);
        assert_eq!(annotation, "1.0s");
    }

    #[test]
    fn a_rust_lint_red_wake_names_its_sites() {
        let body = wake([rust_lint_row(
            false,
            true,
            &["a.rs:1 rustfmt", "b.rs:2 clippy::needless_clone"],
        )])
        .unwrap();
        assert!(
            body.contains(
                "✗ rust-lint — 2 of 2 failed: a.rs:1 rustfmt, b.rs:2 clippy::needless_clone"
            ),
            "{body}"
        );
    }

    const LINT_REPORT: &str = "❌ clippy: 1 new finding(s) in your diff\n  b.rs:2  clippy::needless_clone  redundant clone\n  ℹ 15 pre-existing finding(s) on the base (not gating)\n❌ rustfmt: formatting drift in 1 file(s) — run `bun run fmt` to fix.\n  a.rs:1";

    /// A rust-lint red whose stored excerpt is `lead` (a classification, or
    /// nothing), the report, then compile chatter, as the lane composes it.
    fn rust_lint_red_with_report(lead: &str) -> CheckResultCacheEntry {
        let mut row = rust_lint_row(
            false,
            true,
            &["b.rs:2 clippy::needless_clone", "a.rs:1 rustfmt"],
        );
        let mut parsed: ParsedCheckResult =
            serde_json::from_str(row.target_results_json.as_deref().unwrap()).unwrap();
        parsed.report = Some(LINT_REPORT.to_string());
        row.target_results_json = Some(serde_json::to_string(&parsed).unwrap());
        row.output_tail = format!(
            "{lead}{LINT_REPORT}\n\nFinal output:\n    Checking crate-7 v0.1.0\nerror: script \"check:rust\" exited with code 1"
        );
        row
    }

    /// Every renderer shows the lint's own verdict headers and pre-existing
    /// count, not just the failure names, and none of the chatter after it.
    #[test]
    fn a_rust_lint_red_renders_its_report_on_checks_and_the_wake() {
        let row = rust_lint_red_with_report("");
        let status = status_from_row(
            "job",
            None,
            "rust-lint",
            CheckPolicy::Advisory,
            CheckWhen::Review,
            &row,
            None,
        );
        assert_eq!(status.output_tail.as_deref(), Some(LINT_REPORT));

        let body = wake([row]).unwrap();
        assert!(
            body.contains(
                "✗ rust-lint — 2 of 2 failed: b.rs:2 clippy::needless_clone, a.rs:1 rustfmt"
            ),
            "{body}"
        );
        assert!(
            body.contains("❌ clippy: 1 new finding(s) in your diff"),
            "{body}"
        );
        assert!(body.contains("15 pre-existing"), "{body}");
        assert!(!body.contains("Checking crate-7"), "{body}");
    }

    /// A classified rust-lint death keeps its classification and evidence ahead
    /// of the report, as the stored excerpt orders them.
    #[test]
    fn a_classified_rust_lint_red_keeps_its_evidence_ahead_of_the_report() {
        let lead = "sccache corruption\nEvidence:\nmemory map must have a non-zero length\n\n";
        let row = rust_lint_red_with_report(lead);
        let status = status_from_row(
            "job",
            None,
            "rust-lint",
            CheckPolicy::Advisory,
            CheckWhen::Review,
            &row,
            None,
        );
        let excerpt = status.output_tail.unwrap();
        assert!(
            excerpt.starts_with("sccache corruption\nEvidence:"),
            "{excerpt}"
        );
        assert!(excerpt.ends_with("  a.rs:1"), "{excerpt}");
        assert!(!excerpt.contains("Final output:"), "{excerpt}");
    }

    #[test]
    fn status_annotation_renders_counts_duration_and_cached() {
        let mut status = NodeCheckStatus {
            job_id: "job".to_string(),
            request_id: None,
            name: "rust".to_string(),
            state: NodeCheckState::Passed,
            policy: "advisory".to_string(),
            when: "write".to_string(),
            cached: Some(true),
            duration_ms: Some(4100),
            ran_at: Some(1),
            passed: Some(12),
            failed: Some(0),
            skipped: Some(1),
            deferred: None,
            parser: Some("nextest".into()),
            complete: Some(true),
            selection: Some("full".into()),
            suite_failures: None,
            failure_names: Vec::new(),
            output_tail: None,
            failure_kind: None,
            suppressed_after: None,
            executor: None,
            verified_on: None,
            notice: None,
        };
        assert_eq!(
            format_status_annotation(&status).as_deref(),
            Some("12 tests, 1 skipped, cached")
        );
        status.executor = Some("fedora".into());
        assert_eq!(
            format_status_annotation(&status).as_deref(),
            Some("12 tests, 1 skipped, on fedora, cached"),
            "a lane names the machine its verdict came from"
        );
        status.executor = None;
        status.skipped = Some(0);
        assert_eq!(
            format_status_annotation(&status).as_deref(),
            Some("12 tests, cached")
        );
        status.passed = None;
        status.failed = None;
        status.cached = Some(false);
        assert_eq!(format_status_annotation(&status).as_deref(), Some("4.1s"));
        status.verified_on = Some(VerifiedHead {
            head: "0123456789abcdef0123".into(),
            base: "fedcba9876543210fedc".into(),
        });
        assert_eq!(
            format_status_annotation(&status).as_deref(),
            Some(
                "4.1s, verified on earlier head 0123456789ab over base fedcba987654; \
                 base since advanced, diff unchanged"
            ),
            "a lane verified on an earlier head names that head and its base"
        );
    }

    /// A fast-tier green is never a bare one: the label names how many tests it
    /// ran and how many it deferred, so its population cannot merge with a
    /// whole-suite green's. A red fast lane says the same beside its failures.
    #[test]
    fn a_fast_tier_lane_renders_its_population_green_or_red() {
        let mut status = NodeCheckStatus {
            job_id: "job".to_string(),
            request_id: None,
            name: "rust-tests".to_string(),
            state: NodeCheckState::Passed,
            policy: "advisory".to_string(),
            when: "review".to_string(),
            cached: Some(false),
            duration_ms: Some(90_000),
            ran_at: Some(1),
            passed: Some(8_501),
            failed: Some(0),
            skipped: Some(0),
            deferred: Some(3_596),
            parser: Some("nextest".into()),
            complete: Some(true),
            selection: Some("fast".into()),
            suite_failures: None,
            failure_names: Vec::new(),
            output_tail: None,
            failure_kind: None,
            suppressed_after: None,
            executor: None,
            verified_on: None,
            notice: None,
        };
        assert_eq!(
            format_status_annotation(&status).as_deref(),
            Some("fast tier: 8501 ran, 3596 deferred")
        );

        status.state = NodeCheckState::Failed;
        status.passed = Some(8_499);
        status.failed = Some(2);
        status.failure_names = vec!["c t::a".into(), "c t::b".into()];
        assert_eq!(
            format_status_annotation(&status).as_deref(),
            Some("2 of 8501 failed, fast tier: 8501 ran, 3596 deferred")
        );
    }

    /// A verdict reached from another tree is reused, never fresh, on the head
    /// that renders it, green or red. The death-vs-lie seam depends on this:
    /// a reused verdict that read as fresh would claim the head was exercised.
    #[test]
    fn a_verdict_from_another_tree_renders_cached_and_one_from_this_tree_does_not() {
        let mut foreign_green = verdict_row("rust-lint", true);
        foreign_green.tree_hash = "tree-source".to_string();
        let mut foreign_red = verdict_row("lint", false);
        foreign_red.tree_hash = "tree-source".to_string();
        let mut own = verdict_row("rust-tests", true);
        own.tree_hash = "tree-head".to_string();

        let rows: Vec<CheckResultCacheEntry> = attribute_reuse(
            vec![foreign_green.into(), foreign_red.into(), own.into()],
            "tree-head",
        )
        .into_iter()
        .map(|lane| lane.row)
        .collect();
        let annotate = |row: &CheckResultCacheEntry| {
            format_status_annotation(&status_from_row(
                "job",
                None,
                &row.check_name,
                CheckPolicy::Advisory,
                CheckWhen::Review,
                row,
                None,
            ))
            .unwrap_or_default()
        };
        assert!(
            annotate(&rows[0]).ends_with("cached"),
            "{}",
            annotate(&rows[0])
        );
        assert_eq!(rows[1].cached, Some(true));
        assert!(
            annotate(&rows[1]).contains("cached"),
            "{}",
            annotate(&rows[1])
        );
        assert_eq!(
            rows[2].cached,
            Some(false),
            "this head's own run stays fresh"
        );
        assert!(!annotate(&rows[2]).contains("cached"));
    }

    #[test]
    fn status_annotation_separates_a_self_skipped_suite_from_a_zero_selection() {
        let mut status = NodeCheckStatus {
            job_id: "job".to_string(),
            request_id: None,
            name: "rust-full".to_string(),
            state: NodeCheckState::Passed,
            policy: "advisory".to_string(),
            when: "review".to_string(),
            cached: Some(false),
            duration_ms: Some(600_000),
            ran_at: Some(1),
            passed: Some(0),
            failed: Some(0),
            skipped: Some(12),
            deferred: None,
            parser: Some("nextest".into()),
            complete: Some(true),
            selection: Some("full".into()),
            suite_failures: None,
            failure_names: Vec::new(),
            output_tail: None,
            failure_kind: None,
            suppressed_after: None,
            executor: None,
            verified_on: None,
            notice: None,
        };
        assert_eq!(
            format_status_annotation(&status).as_deref(),
            Some("no tests ran, 12 skipped")
        );
        status.skipped = Some(0);
        assert_eq!(
            format_status_annotation(&status).as_deref(),
            Some("no tests matched the change")
        );
    }

    /// The whole point of `suite_failures`, at the surface that outlives the run:
    /// a check read back out of `check_result_cache` must not render the zero
    /// tally the immediate summary already refuses to render.
    #[test]
    fn status_annotation_names_a_cached_suite_collection_failure() {
        let mut status = NodeCheckStatus {
            job_id: "job".to_string(),
            request_id: None,
            name: "frontend-partial".to_string(),
            state: NodeCheckState::Failed,
            policy: "advisory".to_string(),
            when: "write".to_string(),
            cached: Some(true),
            duration_ms: Some(30_000),
            ran_at: Some(1),
            passed: Some(881),
            failed: Some(0),
            skipped: Some(0),
            deferred: None,
            parser: Some("nextest".into()),
            complete: Some(true),
            selection: Some("full".into()),
            suite_failures: Some(1),
            failure_names: vec!["src/components/FileTabView.test.tsx".to_string()],
            output_tail: Some("Cannot find module".to_string()),
            failure_kind: None,
            suppressed_after: None,
            executor: None,
            verified_on: None,
            notice: None,
        };
        assert_eq!(
            format_status_annotation(&status).as_deref(),
            Some("1 suite failed to load, cached")
        );

        // Both kinds at once: neither number may absorb the other.
        status.passed = Some(38);
        status.failed = Some(2);
        status.suite_failures = Some(3);
        status.cached = Some(false);
        assert_eq!(
            format_status_annotation(&status).as_deref(),
            Some("2 of 40 failed, 3 suites failed to load")
        );

        // A legacy row (no suite count stored) still reads exactly as before.
        status.suite_failures = None;
        assert_eq!(
            format_status_annotation(&status).as_deref(),
            Some("2 of 40 failed")
        );
    }

    #[test]
    fn failed_annotation_renders_timeout_with_still_running_tests() {
        let status = NodeCheckStatus {
            job_id: "job".to_string(),
            request_id: None,
            name: "rust-full".to_string(),
            state: NodeCheckState::Failed,
            policy: "advisory".to_string(),
            when: "review".to_string(),
            cached: None,
            duration_ms: Some(1_800_000),
            ran_at: Some(1),
            passed: None,
            failed: None,
            skipped: None,
            deferred: None,
            parser: None,
            complete: None,
            selection: None,
            suite_failures: None,
            failure_names: vec!["mycrate mod::hangs".to_string()],
            output_tail: Some("...".to_string()),
            failure_kind: Some("timed_out".to_string()),
            suppressed_after: None,
            executor: None,
            verified_on: None,
            notice: None,
        };
        assert_eq!(
            format_status_annotation(&status).as_deref(),
            Some("timed out after 30m; still running: mycrate mod::hangs")
        );
    }

    #[test]
    fn environment_and_unclassified_failures_render_without_retry_or_code_ownership_claims() {
        assert_eq!(
            failed_annotation(None, Some(1), Some(0), Some(0), Some(0), &[]).as_deref(),
            Some("check failed before reporting test failures — unverified")
        );
        assert_eq!(
            failed_annotation(
                Some(CheckFailureKind::Environment),
                Some(1),
                None,
                None,
                None,
                &[],
            )
            .as_deref(),
            Some("environment cannot build this crate cone on this machine — unverified")
        );
        assert_eq!(
            failed_annotation(
                Some(CheckFailureKind::Unclassified),
                Some(1),
                None,
                None,
                None,
                &[],
            )
            .as_deref(),
            Some("failure could not be classified — needs investigation")
        );
        assert_eq!(
            failed_annotation(
                Some(CheckFailureKind::Selection),
                Some(1),
                None,
                None,
                None,
                &[],
            )
            .as_deref(),
            Some("test selection matched no tests")
        );
    }

    /// A suppressed check must not borrow the vocabulary of a verdict. The row it
    /// renders from is a real infrastructure failure, so without the counter it
    /// would read as "infrastructure/toolchain failure" — true of the last
    /// attempt, and misleading about the present, where nothing is being retried.
    #[test]
    fn suppressed_annotation_says_not_run_rather_than_naming_a_verdict() {
        let mut status = NodeCheckStatus {
            job_id: "job".to_string(),
            request_id: None,
            name: "rust-full".to_string(),
            state: NodeCheckState::Failed,
            policy: "advisory".to_string(),
            when: "review".to_string(),
            cached: Some(true),
            duration_ms: Some(6),
            ran_at: Some(1),
            passed: None,
            failed: None,
            skipped: None,
            deferred: None,
            parser: None,
            complete: None,
            selection: None,
            suite_failures: None,
            failure_names: Vec::new(),
            output_tail: Some("sccache: server startup failed".to_string()),
            failure_kind: Some("infrastructure".to_string()),
            suppressed_after: Some(3),
            executor: None,
            verified_on: None,
            notice: None,
        };
        assert_eq!(
            format_status_annotation(&status).as_deref(),
            Some("suppressed")
        );

        // Below the bound the same row still reads as the retried failure it is,
        // reuse suffix and all.
        status.suppressed_after = None;
        assert_eq!(
            format_status_annotation(&status).as_deref(),
            Some("infrastructure/toolchain failure, cached")
        );
    }

    /// The counter is what makes a row suppressed, and only at or past the bound.
    #[test]
    fn a_row_becomes_suppressed_only_at_the_bound() {
        let mut row = CheckResultCacheEntry {
            project_id: "project-a".to_string(),
            tree_hash: "tree".to_string(),
            input_hash: "ih".to_string(),
            check_name: "rust".to_string(),
            exit_code: 1,
            passed: false,
            output_tail: "sccache died".to_string(),
            duration_ms: 1,
            ran_at: 1,
            target_results_json: None,
            job_id: None,
            cached: None,
            failure_kind: Some("infrastructure".to_string()),
            infra_failure_streak: crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND - 1,
            executor_id: None,
            executor_device_id: None,
            executor_connection_generation: None,
            executor_cell_id: None,
            executor_lease_epoch: None,
            executor_started_at_unix_ms: None,
            executor_finished_at_unix_ms: None,
            toolchain_fingerprint: None,
            defined_by_commit_sha: Some("commit-a".to_string()),
        };
        assert_eq!(
            status_from_row(
                "job",
                None,
                "rust",
                CheckPolicy::Advisory,
                CheckWhen::Review,
                &row,
                None
            )
            .suppressed_after,
            None
        );

        row.infra_failure_streak = crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND;
        assert_eq!(
            status_from_row(
                "job",
                None,
                "rust",
                CheckPolicy::Advisory,
                CheckWhen::Review,
                &row,
                None
            )
            .suppressed_after,
            Some(crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND)
        );
    }

    #[test]
    fn failed_annotation_renders_spawn_error() {
        let status = NodeCheckStatus {
            job_id: "job".to_string(),
            request_id: None,
            name: "rust-lint".to_string(),
            state: NodeCheckState::Failed,
            policy: "advisory".to_string(),
            when: "review".to_string(),
            cached: None,
            duration_ms: Some(6),
            ran_at: Some(1),
            passed: None,
            failed: None,
            skipped: None,
            deferred: None,
            parser: None,
            complete: None,
            selection: None,
            suite_failures: None,
            failure_names: Vec::new(),
            output_tail: Some("Failed to spawn command".to_string()),
            failure_kind: Some("spawn_error".to_string()),
            suppressed_after: None,
            executor: None,
            verified_on: None,
            notice: None,
        };
        assert_eq!(
            format_status_annotation(&status).as_deref(),
            Some("failed to spawn")
        );
    }

    fn lane(name: &str, state: NodeCheckState) -> NodeCheckStatus {
        test_status(name, "advisory", "review", state)
    }

    fn with_tests(mut lane: NodeCheckStatus, selection: &str, passed: usize) -> NodeCheckStatus {
        lane.parser = Some("nextest".into());
        lane.selection = Some(selection.into());
        lane.complete = Some(true);
        lane.passed = Some(passed);
        lane.failed = Some(0);
        lane.skipped = Some(0);
        lane
    }

    fn rust_lint(selection: &str, complete: bool) -> NodeCheckStatus {
        let mut lane = lane("rust-lint", NodeCheckState::Unverified);
        lane.parser = Some("rust-lint".into());
        lane.selection = Some(selection.into());
        lane.complete = Some(complete);
        lane.passed = Some(0);
        lane.failed = Some(0);
        lane.skipped = Some(0);
        lane
    }

    #[test]
    fn every_lane_state_maps_to_its_existence() {
        let idle = WaveCadence::default();
        let reviewing = WaveCadence::reviewing_for_test();
        let died = |kind: &str, after: Option<i64>| {
            let mut lane = lane("rust-tests", NodeCheckState::Failed);
            lane.failure_kind = Some(kind.into());
            lane.suppressed_after = after;
            lane
        };
        let incomplete = {
            let mut lane = with_tests(lane("rust-tests", NodeCheckState::Unverified), "full", 4);
            lane.complete = Some(false);
            lane
        };
        let cases: Vec<(NodeCheckStatus, WaveCadence, Option<LaneExistence>)> = vec![
            (
                with_tests(lane("rust-tests", NodeCheckState::Passed), "full", 3),
                idle,
                Some(LaneExistence::Complete),
            ),
            (
                lane("rust-tests", NodeCheckState::Failed),
                idle,
                Some(LaneExistence::Complete),
            ),
            // A script check's green carries no structured result: its exit
            // code is the verdict.
            (
                lane("migrations", NodeCheckState::Unverified),
                idle,
                Some(LaneExistence::Complete),
            ),
            (
                lane("rust-tests", NodeCheckState::Pending),
                idle,
                Some(LaneExistence::Absent),
            ),
            (
                lane("rust-tests", NodeCheckState::Pending),
                reviewing,
                Some(LaneExistence::Pending { running: false }),
            ),
            (
                lane("rust-tests", NodeCheckState::Running),
                reviewing,
                Some(LaneExistence::Pending { running: true }),
            ),
            (
                died("infrastructure", None),
                idle,
                Some(LaneExistence::Died {
                    kind: "infrastructure".into(),
                }),
            ),
            (
                died("infrastructure", Some(3)),
                idle,
                Some(LaneExistence::Breaker { after: 3 }),
            ),
            (
                with_tests(lane("rust-tests", NodeCheckState::Unverified), "empty", 0),
                idle,
                Some(LaneExistence::Empty),
            ),
            (
                incomplete,
                idle,
                Some(LaneExistence::Incomplete(
                    UnverifiedReason::IncompleteEnumeration,
                )),
            ),
            // A static check whose cone held nothing the change touched had no
            // selectable work; one that stopped before measuring decided nothing.
            (
                rust_lint("empty", false),
                idle,
                Some(LaneExistence::NoSelectableWork),
            ),
            (
                rust_lint("unknown", false),
                idle,
                Some(LaneExistence::Incomplete(UnverifiedReason::IncompleteLint)),
            ),
            (
                lane("rust-tests", NodeCheckState::NotApplicable),
                idle,
                None,
            ),
            (lane("next-check", NodeCheckState::Unsupported), idle, None),
        ];
        for (status, cadence, expected) in cases {
            let shown = format!("{status:?}");
            assert_eq!(lane_existence(&status, cadence), expected, "{shown}");
        }
    }

    /// A write-cadence wave in flight does not make a review lane pending.
    #[test]
    fn only_a_wave_of_the_lanes_own_cadence_makes_it_pending() {
        assert_eq!(
            lane_existence(
                &lane("rust-tests", NodeCheckState::Pending),
                WaveCadence::writing_for_test()
            ),
            Some(LaneExistence::Absent)
        );
    }
}
