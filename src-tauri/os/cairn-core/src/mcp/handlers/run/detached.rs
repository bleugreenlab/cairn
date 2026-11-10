//! A `run` batch that keeps running after its call has answered.
//!
//! A call that cannot suspend (its carrier cannot resume it in place, or the
//! project store stayed busy past its answer deadline) used to cancel its batch
//! so the call could answer in time. The executor was healthy; the batch died
//! only because Cairn could not record a wait. Now the call answers with a
//! handle instead and the batch runs to its own end. Its life from that moment
//! is one durable row, `run_batch_detached`, written before the call answers:
//!
//! - its result settles the row and, in the same write, queues Cairn's own
//!   notice for the job ([`settle_and_deliver`]), never a message from anyone;
//! - the node reads it at `cairn:~/batches/<request_id>`;
//! - an identical reissue is refused while it runs;
//! - the replay mutation boundary holds the branch while it runs unsettled, and
//!   its ending releases that hold through [`end`], the one place a detached
//!   batch reaches `base_advance::on_branch_hold_released`.
//!
//! A batch no machine has been handed yet is `parked`: its row names what it
//! waits for and holds no branch, and it becomes `running` when it is
//! admitted ([`mark_admitted`]). A parked row never claims work that exists
//! nowhere.
//!
//! A runner restart does not settle it. Only the batch's report, or evidence
//! that it can no longer report, does.

use cairn_db::turso::params;

use crate::orchestrator::Orchestrator;
use crate::storage::{DbResult, LocalDb, RowExt};

/// Where a detached batch stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DetachedState {
    /// No machine has been handed it yet; `waiting_on` says why.
    Parked,
    /// It has not reported, and nothing shows it cannot.
    Running,
    /// Its result arrived.
    Settled,
    /// Evidence shows it can no longer report; its outcome is unknown.
    Lost,
}

impl DetachedState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Parked => "parked",
            Self::Running => "running",
            Self::Settled => "settled",
            Self::Lost => "lost",
        }
    }

    fn parse(raw: &str) -> Self {
        match raw {
            "parked" => Self::Parked,
            "settled" => Self::Settled,
            "lost" => Self::Lost,
            _ => Self::Running,
        }
    }
}

/// One `run_batch_detached` row.
#[derive(Clone, Debug)]
pub(crate) struct DetachedBatch {
    pub(crate) request_id: String,
    pub(crate) job_id: String,
    pub(crate) run_id: String,
    pub(crate) label: Option<String>,
    /// The batch's normalized payload, compared by the reissue refusal.
    pub(crate) identity: Option<String>,
    pub(crate) commits: bool,
    /// Whether it can write the branch or runs in the node's residence. A
    /// verdict-only or host batch never holds the replay boundary.
    pub(crate) holds_branch: bool,
    pub(crate) state: DetachedState,
    pub(crate) verdict: Option<String>,
    pub(crate) output: Option<String>,
    pub(crate) commit_id: Option<String>,
    /// Why its call could not suspend.
    pub(crate) reason: String,
    pub(crate) lost_evidence: Option<String>,
    pub(crate) created_ms: i64,
    pub(crate) settled_ms: Option<i64>,
    pub(crate) notified_ms: Option<i64>,
    /// What a later runner process needs to settle and publish the batch from
    /// a result it adopts (see [`super::adopted`]).
    pub(crate) settlement: Option<super::PersistedSettlement>,
    /// What a parked batch waits for, in a sentence.
    pub(crate) waiting_on: Option<String>,
}

const COLUMNS: &str = "request_id,job_id,run_id,label,identity,commits,holds_branch,state,verdict,output,commit_id,reason,lost_evidence,created_ms,settled_ms,notified_ms,settlement_json,waiting_on";

fn from_row(row: &cairn_db::turso::Row) -> DbResult<DetachedBatch> {
    Ok(DetachedBatch {
        request_id: row.text(0)?,
        job_id: row.text(1)?,
        run_id: row.text(2)?,
        label: row.opt_text(3)?,
        identity: row.opt_text(4)?,
        commits: row.i64(5)? != 0,
        holds_branch: row.i64(6)? != 0,
        state: DetachedState::parse(&row.text(7)?),
        verdict: row.opt_text(8)?,
        output: row.opt_text(9)?,
        commit_id: row.opt_text(10)?,
        reason: row.text(11)?,
        lost_evidence: row.opt_text(12)?,
        created_ms: row.i64(13)?,
        settled_ms: row.opt_i64(14)?,
        notified_ms: row.opt_i64(15)?,
        // A row this build cannot read keeps its batch settleable, with the
        // precise report in place of the commit.
        settlement: row
            .opt_text(16)?
            .and_then(|json| serde_json::from_str(&json).ok()),
        waiting_on: row.opt_text(17)?,
    })
}

/// Request ids whose detached batch has a resolver waiting on it in this
/// process. A `running` row outside this set was left by an earlier runner
/// incarnation: nothing here will hear its answer, so only its executor's
/// report or evidence that it can no longer report settles it.
static LIVE_RESOLVERS: std::sync::LazyLock<std::sync::Mutex<std::collections::HashSet<String>>> =
    std::sync::LazyLock::new(Default::default);

/// Marks one detached batch as awaited in this process for as long as it lives.
pub(crate) struct LiveResolver(String);

impl LiveResolver {
    pub(crate) fn claim(request_id: &str) -> Self {
        LIVE_RESOLVERS
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(request_id.to_string());
        Self(request_id.to_string())
    }
}

impl Drop for LiveResolver {
    fn drop(&mut self) {
        LIVE_RESOLVERS
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&self.0);
    }
}

/// The batches whose task is still running in this process, from preparation
/// through publication, by request id. It covers what the fleet registry does
/// not: a batch between placement attempts, or settling after its process
/// ended. Each entry is the batch's own control, so a cancel addressed by
/// request id reaches the one batch it names ([`super::cancel`]).
static LIVE_BATCHES: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<String, std::sync::Arc<super::BatchControl>>>,
> = std::sync::LazyLock::new(Default::default);

/// Marks one batch's task as running for as long as it lives.
pub(crate) struct LiveBatch(String);

impl LiveBatch {
    pub(super) fn claim(control: &std::sync::Arc<super::BatchControl>) -> Self {
        let request_id = control.request_id.clone();
        LIVE_BATCHES
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(request_id.clone(), control.clone());
        PLACED_HERE
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(request_id.clone());
        Self(request_id)
    }

    /// A live batch under `request_id` with nothing behind it, for a test
    /// outside this module that needs only the registry to say it is alive.
    #[cfg(test)]
    pub(crate) fn claim_for_test(request_id: &str) -> Self {
        Self::claim(&super::BatchControl::with_request_id(request_id, false))
    }
}

/// The control of a batch whose task is running in this process.
pub(super) fn live_control(request_id: &str) -> Option<std::sync::Arc<super::BatchControl>> {
    LIVE_BATCHES
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(request_id)
        .cloned()
}

/// Who a live batch belongs to: its job, and that job's home URI (the
/// node or task whose `batches/<request_id>` addresses it).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BatchOwner {
    pub(crate) job_id: String,
    pub(crate) home: Option<String>,
}

/// The owner of a batch whose task is running in this process, once its run
/// context has resolved.
pub(crate) fn live_owner(request_id: &str) -> Option<BatchOwner> {
    live_control(request_id)?.owner()
}

/// Where a batch whose task runs in this process stands now, in the terms a
/// reader deciding whether to wait for it needs.
pub(crate) fn live_progress(orch: &Orchestrator, request_id: &str) -> Option<BatchProgress> {
    Some(live_control(request_id)?.progress(&orch.fleet))
}

/// Item `index` of a live batch has started. Reached from the host item loop
/// and from a machine's per-item report; a request with no live batch here (a
/// check lane, a batch an earlier runner placed) is ignored.
pub(crate) fn note_item_started(request_id: &str, index: usize) {
    if let Some(control) = live_control(request_id) {
        control.item_started(index, chrono::Utc::now().timestamp_millis());
    }
}

/// Item `index` of a live batch has finished.
pub(crate) fn note_item_finished(request_id: &str, index: usize) {
    if let Some(control) = live_control(request_id) {
        control.item_finished(index);
    }
}

/// The longest item header a progress line quotes; a shell item's header is
/// its whole command line.
const PROGRESS_HEADER_CHARS: usize = 80;

/// Where a live batch stands, for the rebase hold and the batch's own page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum BatchProgress {
    /// No machine has it: `waiting` is what it waits for.
    Queued { waiting: String },
    /// One item is running: item `index` (0-based) of `total`, since
    /// `since_ms` (Unix milliseconds).
    Running {
        index: usize,
        total: usize,
        header: String,
        since_ms: i64,
    },
    /// Several items of a parallel batch are running, the earliest since
    /// `since_ms`.
    Parallel {
        running: usize,
        total: usize,
        since_ms: i64,
    },
    /// A machine has it and has not reported an item running.
    Placed { on: String },
    /// Its task is alive here but no machine holds it: between placement
    /// attempts.
    InRunner,
    /// Settlement has begun: it is publishing its result, and any commit it
    /// carries is being written.
    Settling,
}

impl BatchProgress {
    /// One clause, as `now_ms` sees it: ``now running item 3 of 5 (`cargo
    /// nextest run`) for 14m 2s``.
    pub(crate) fn describe(&self, now_ms: i64) -> String {
        let elapsed = |since: i64| {
            crate::execution::checks_status::human_duration(
                now_ms.saturating_sub(since).max(0) as u64
            )
        };
        match self {
            Self::Queued { waiting } => format!("queued for a machine: {waiting}"),
            Self::Running {
                index,
                total,
                header,
                since_ms,
            } => {
                let mut quoted: String = header.chars().take(PROGRESS_HEADER_CHARS).collect();
                if header.chars().count() > PROGRESS_HEADER_CHARS {
                    quoted.push('…');
                }
                format!(
                    "now running item {} of {total} (`{quoted}`) for {}",
                    index + 1,
                    elapsed(*since_ms)
                )
            }
            Self::Parallel {
                running,
                total,
                since_ms,
            } => format!(
                "now running {running} of its {total} items in parallel, the first for {}",
                elapsed(*since_ms)
            ),
            Self::Placed { on } => {
                format!("placed on `{on}`, which has not yet reported an item running")
            }
            Self::InRunner => {
                "between placement attempts; no machine holds it right now".to_string()
            }
            Self::Settling => {
                "publishing its result now; a commit it carries is being written".to_string()
            }
        }
    }
}

/// Every request this runner process started a batch task for. For one of
/// these, the task and fleet registries are complete, so their silence is
/// verified death; for any other, only its machines' reports speak.
static PLACED_HERE: std::sync::LazyLock<std::sync::Mutex<std::collections::HashSet<String>>> =
    std::sync::LazyLock::new(Default::default);

pub(crate) fn placed_here(request_id: &str) -> bool {
    PLACED_HERE
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .contains(request_id)
}

impl Drop for LiveBatch {
    fn drop(&mut self) {
        LIVE_BATCHES
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&self.0);
    }
}

/// Whether a batch placed by this runner incarnation is still alive: its task
/// is running here, or the fleet still holds its request. For a request this
/// incarnation placed, `false` is verified death.
pub(crate) fn batch_is_live(orch: &Orchestrator, request_id: &str) -> bool {
    LIVE_BATCHES
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .contains_key(request_id)
        || orch.is_cell_request_in_flight(request_id)
}

/// Whether a resolver in this process is waiting on the batch's answer.
pub(crate) fn has_live_resolver(request_id: &str) -> bool {
    LIVE_RESOLVERS
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .contains(request_id)
}

/// Where an unsettled batch actually stands. A parked row says it itself
/// (`waiting_on`); for a running one the row says only that nothing has
/// settled it, so this asks the fleet's queue whether a machine has it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum LiveState {
    /// Not yet handed to the fleet; `waiting` is what it waits for.
    Parked { waiting: String },
    /// A machine claimed its attempt and may be running it.
    Placed { on: String },
    /// In the placement queue, and no machine has claimed it.
    Unplaced { wait: String },
    /// Its task is alive here but no machine holds it right now: it is
    /// between placement attempts, or is publishing its result.
    InRunner,
    /// Nothing in this runner holds it; a batch an earlier runner placed is
    /// settled from its machine's report or from evidence it cannot report.
    Untracked,
}

impl LiveState {
    /// The state as a running row's `State:` line and the batch list state it.
    fn headline(&self) -> String {
        match self {
            Self::Parked { waiting } => format!("parked, {waiting}"),
            Self::Placed { on } => format!("running on `{on}`"),
            Self::Unplaced { .. } => "queued, not yet placed on any machine".to_string(),
            Self::InRunner => "in this runner, held by no machine right now".to_string(),
            Self::Untracked => "running".to_string(),
        }
    }

    /// One sentence for a reader deciding whether to wait.
    fn sentence(&self) -> String {
        match self {
            Self::Parked { waiting } => format!("it has not started: it is {waiting}, and it holds no branch until a machine has it"),
            Self::Placed { on } => format!("it is running on `{on}`"),
            Self::Unplaced { wait } => format!("no machine has taken it: it is {wait}"),
            Self::InRunner => "it is between placement attempts or publishing its result; no machine holds it right now".to_string(),
            Self::Untracked => "this runner holds no task for it, so it settles from its machine's own report or from evidence that it cannot report".to_string(),
        }
    }
}

/// Where an unsettled detached batch stands now: its own row for a parked
/// one, and the fleet's queue and this runner's task registry for a running
/// one.
pub(crate) fn live_state(orch: &Orchestrator, batch: &DetachedBatch) -> LiveState {
    let request_id = batch.request_id.as_str();
    if batch.state == DetachedState::Parked {
        return LiveState::Parked {
            waiting: batch
                .waiting_on
                .clone()
                .unwrap_or_else(|| "waiting to be placed".to_string()),
        };
    }
    if let Some(executor_id) = orch.fleet.claimed_executor_of(request_id) {
        return LiveState::Placed {
            on: orch.fleet.executor_name_for_text(&executor_id),
        };
    }
    if let Some(wait) = orch.fleet.unplaced_wait(request_id) {
        return LiveState::Unplaced { wait };
    }
    if batch_is_live(orch, request_id) {
        return LiveState::InRunner;
    }
    LiveState::Untracked
}

/// The caller-home-relative handle, resolved beneath the owning node or task.
pub(crate) fn handle_uri(request_id: &str) -> String {
    format!("cairn:~/batches/{request_id}")
}

/// What a call that detached answers with.
pub(crate) fn detach_answer_text(request_id: &str, reason: &str) -> String {
    format!(
        "This batch is still running. Cairn could not suspend this call ({reason}), so the call answers now instead. Nothing was cancelled. When it settles, Cairn delivers its result to this job as a notice, and you can read it any time at `{}`. Do not re-run it.",
        handle_uri(request_id)
    )
}

/// What a call that detached a batch no machine has yet answers with.
pub(crate) fn detach_answer_parked_text(request_id: &str, reason: &str, waiting: &str) -> String {
    format!(
        "This batch has not started yet: it is {waiting}. Cairn could not suspend this call ({reason}), so the call answers now instead. Nothing was cancelled, and it holds no branch until it starts. When it settles, Cairn delivers its result to this job as a notice, and `{}` shows where it stands. Do not re-run it.",
        handle_uri(request_id)
    )
}

/// What a reissue of a batch still unsettled detached is told: which request
/// it is, where that request stands now, and how to wait on it.
pub(crate) fn reissue_refusal_text(request_id: &str, live: &LiveState) -> String {
    format!(
        "This batch is already in flight as request `{request_id}`, detached from the call that issued it: {}. Nothing new was started. Wait on that request instead of re-running it: its result reaches this job as a notice when it settles, and `{}` shows where it stands now.",
        live.sentence(),
        handle_uri(request_id)
    )
}

fn label_of(batch: &DetachedBatch) -> &str {
    batch.label.as_deref().unwrap_or("unnamed")
}

/// How the notice and the handle state what the batch committed. A batch with
/// no `commit_msg` cannot have committed, and says so plainly.
fn commit_phrase(batch: &DetachedBatch) -> String {
    match (batch.commits, batch.commit_id.as_deref()) {
        (true, Some(commit)) => format!("commit {}", &commit[..commit.len().min(12)]),
        _ => "committed nothing".to_string(),
    }
}

/// Cairn's sentence about a detached batch, as its job receives it. It is the
/// whole notice: the full output stays behind the handle.
pub(crate) fn notice_sentence(batch: &DetachedBatch) -> String {
    let handle = handle_uri(&batch.request_id);
    match batch.state {
        DetachedState::Lost => format!(
            "Your run batch '{}' ({handle}) can no longer report ({}); its outcome is unknown, and the handle shows what was recorded.",
            label_of(batch),
            batch.lost_evidence.as_deref().unwrap_or("no evidence was recorded")
        ),
        _ => format!(
            "Your run batch '{}' ({handle}) finished: {}, {}.",
            label_of(batch),
            batch.verdict.as_deref().unwrap_or("finished"),
            commit_phrase(batch)
        ),
    }
}

/// The handle's read: state, verdict, label, what it committed, and its output.
/// `live` is where a running batch stands now; other states ignore it.
pub(crate) fn render(batch: &DetachedBatch, live: &LiveState) -> String {
    let state = match batch.state {
        DetachedState::Running => live.headline(),
        other => other.as_str().to_string(),
    };
    let mut out = format!(
        "# Run batch `{}`\n\nLabel: {}\nState: {state}\n",
        batch.request_id,
        label_of(batch),
    );
    out.push_str(&format!("Detached because: {}\n", batch.reason));
    match batch.state {
        DetachedState::Parked => {
            out.push_str(&format!(
                "Waiting: it is {}. No machine has it yet, and it holds no branch until one does.\n",
                batch.waiting_on.as_deref().unwrap_or("waiting to be placed")
            ));
            out.push_str(
                "\nIts result reaches this job as a notice when it settles. Do not re-run it.\n",
            );
        }
        DetachedState::Running => {
            out.push_str(&format!("Now: {}.\n", live.sentence()));
            out.push_str(if batch.commits {
                "Commits: not yet; it carries a commit_msg, so it may commit when it settles.\n"
            } else {
                "Commits: none; it has no commit_msg, so it cannot commit.\n"
            });
            out.push_str(
                "\nIts result reaches this job as a notice when it settles. Do not re-run it.\n",
            );
        }
        DetachedState::Settled => {
            out.push_str(&format!(
                "Verdict: {}\nCommits: {}\n",
                batch.verdict.as_deref().unwrap_or("finished"),
                commit_phrase(batch)
            ));
            out.push_str(&format!(
                "\n## Output\n\n{}\n",
                batch.output.as_deref().unwrap_or("(no output)")
            ));
        }
        DetachedState::Lost => {
            out.push_str(&format!(
                "Lost because: {}\nIts outcome is unknown. {}\n",
                batch
                    .lost_evidence
                    .as_deref()
                    .unwrap_or("no evidence was recorded"),
                if batch.commits {
                    "It carried a commit_msg; read `cairn:@/vcs/log?limit=5` before re-running it."
                } else {
                    "It had no commit_msg, so it committed nothing."
                }
            ));
        }
    }
    out
}

/// One line for the node's batch list.
pub(crate) fn summary_line(batch: &DetachedBatch, home: &str, live: &LiveState) -> String {
    let state = match batch.state {
        DetachedState::Parked => format!(
            "parked, {}",
            batch
                .waiting_on
                .as_deref()
                .unwrap_or("waiting to be placed")
        ),
        DetachedState::Running => live.headline(),
        DetachedState::Settled => format!(
            "{}, {}",
            batch.verdict.as_deref().unwrap_or("finished"),
            commit_phrase(batch)
        ),
        DetachedState::Lost => "lost".to_string(),
    };
    format!(
        "`{home}/batches/{}` '{}': {state}",
        batch.request_id,
        label_of(batch)
    )
}

/// The verdict a settled batch's composed text states, when no settlement
/// recorded one: a failure of the run itself names itself by its opening.
pub(crate) fn verdict_from_text(text: &str) -> String {
    use super::{RUN_CANCELLED, RUN_NOT_EXECUTED, RUN_NOT_PUBLISHED, RUN_OUTCOME_UNKNOWN};
    if text.starts_with(RUN_NOT_EXECUTED) {
        "it could not execute".into()
    } else if text.starts_with(RUN_NOT_PUBLISHED) {
        "its commands ran but their changes could not be published".into()
    } else if text.starts_with(RUN_OUTCOME_UNKNOWN) {
        "its outcome is unknown".into()
    } else if text.starts_with(RUN_CANCELLED) {
        "it was cancelled".into()
    } else {
        "finished".into()
    }
}

/// Turn a run-batch wait whose batch outlives the wait into a detached batch:
/// `running`, carrying the wait's identity and branch scope, and settled later
/// by the wait's resolver or by its machine's report. Called inside the same
/// transaction as the wait's abandonment or diversion, and ahead of a
/// restart's resolution of it, so there is never a moment when a live batch
/// has no unsettled row. A wait that names no run batch writes nothing.
pub(crate) async fn insert_converted(
    c: &cairn_db::storage::TrackedConnection,
    job_id: &str,
    run_id: &str,
    condition: &crate::mcp::handlers::durable_suspend::Condition,
    reason: &str,
    now_ms: i64,
) -> DbResult<()> {
    let crate::mcp::handlers::durable_suspend::Condition::RunBatch {
        request_id,
        commits,
        label,
        identity,
        settlement,
        ..
    } = condition
    else {
        return Ok(());
    };
    let settlement = settlement
        .as_ref()
        .and_then(|settlement| serde_json::to_string(settlement).ok());
    c.execute(
        "INSERT OR IGNORE INTO run_batch_detached(request_id,job_id,run_id,label,identity,commits,holds_branch,state,reason,created_ms,settlement_json) VALUES(?1,?2,?3,?4,?5,?6,?7,'running',?8,?9,?10)",
        params![
            request_id.as_str(),
            job_id,
            run_id,
            label.as_deref(),
            identity.as_deref(),
            *commits as i64,
            condition.holds_branch() as i64,
            reason,
            now_ms,
            settlement
        ],
    )
    .await?;
    Ok(())
}

/// What a completion a machine still lists says about a batch whose runner
/// restarted before it reported, when the result itself never reached this
/// runner: its machine evicted it past its retention bound, or delivered it to
/// a runner process that did not live to record it.
pub(crate) fn completion_result(
    completion: &cairn_common::executor_protocol::CellCompletion,
    commits: bool,
) -> (String, String) {
    use cairn_common::executor_protocol::CellCompletionVerdict;
    let verdict = match completion.verdict {
        CellCompletionVerdict::Succeeded => "succeeded",
        CellCompletionVerdict::Failed => "failed",
        CellCompletionVerdict::TimedOut => "timed out",
        CellCompletionVerdict::Cancelled => "it was cancelled",
        CellCompletionVerdict::Unavailable => "it could not execute",
    };
    let mut output = String::from(
        "Cairn restarted before this batch reported, so its full output was not recovered; its machine kept only the completion.",
    );
    if let Some(eviction) = completion.result_evicted.as_ref() {
        output.push_str(&format!(
            " Its machine dropped the full result, past the {} MiB it retains of results Cairn has not yet received.",
            eviction.bound_bytes / (1024 * 1024)
        ));
        if let Some(delta) = eviction.delta_commit.as_deref() {
            output.push_str(&format!(
                " The commit it sealed ({}) was dropped with it.",
                &delta[..delta.len().min(12)]
            ));
        }
    }
    if commits {
        output.push_str(" Its changes were not published: publication happens when Cairn receives the result, and it never did. Re-run it with its commit_msg if the work still matters.");
    }
    if let Some(tail) = completion.failure_output_tail.as_deref() {
        output.push_str("\n\nThe tail its machine kept:\n");
        output.push_str(tail);
    }
    (verdict.to_string(), output)
}

/// Settle the running rows whose resolver can no longer hear them, on evidence
/// only:
///
/// - a batch this runner process placed whose task, fleet request, and
///   resolver are all gone ended here without its result reaching anyone;
/// - a batch an earlier process placed is settled from its machines' own
///   reports once every enrolled machine has reported on its current link
///   ([`crate::fleet::Fleet::request_report`]): finished with the completion a
///   machine lists, or lost when none holds or finished it.
///
/// A timer, a restart, attachment, or a registry miss alone settles nothing:
/// until the evidence arrives the row stands, and so does the branch hold it
/// makes. Each pass also wakes jobs whose result notice is still waiting, so a
/// wake lost to a crash after settlement is made good.
pub(crate) async fn sweep_unreported(orch: &Orchestrator) -> usize {
    use crate::fleet::RequestReport;
    let mut settled = 0;
    for db in orch.db.all_dbs().await {
        if let Ok(rows) = all_running(&db).await {
            for batch in rows {
                let id = batch.request_id.as_str();
                if has_live_resolver(id) || batch_is_live(orch, id) {
                    continue;
                }
                // An adopted result settles the row through its driver, and
                // its commit publishes there (`run::adopted`).
                if !placed_here(id) {
                    match super::adopted::has_open_adoption(&db, id).await {
                        Ok(false) => {}
                        Ok(true) => continue,
                        Err(error) => {
                            log::warn!("could not read whether run batch {id} has an adopted result: {error}");
                            continue;
                        }
                    }
                }
                let placed_on = batch
                    .settlement
                    .as_ref()
                    .and_then(|settlement| settlement.placed_on.as_ref());
                // A parked batch was never handed to any machine, so once the
                // task that would have placed it is gone nothing ever will.
                let finished = if batch.state == DetachedState::Parked {
                    mark_lost(
                        orch,
                        &db,
                        id,
                        &format!(
                            "it was never placed: it was still {} when the runner task that would have placed it ended",
                            batch.waiting_on.as_deref().unwrap_or("waiting to be placed")
                        ),
                    )
                    .await
                } else if placed_here(id) {
                    mark_lost(
                        orch,
                        &db,
                        id,
                        "its batch ended in this runner without its result reaching Cairn",
                    )
                    .await
                } else if placed_on.is_some_and(super::PlacedOn::colocated) {
                    mark_lost(
                        orch,
                        &db,
                        id,
                        "it ran on `local`, this machine's own executor, which stops with Cairn when Cairn restarts",
                    )
                    .await
                } else {
                    match orch.fleet.request_report(id) {
                        RequestReport::Unknown | RequestReport::Held { .. } => continue,
                        RequestReport::Completed(completion) => {
                            let (verdict, output) = completion_result(&completion, batch.commits);
                            end(
                                orch,
                                &db,
                                id,
                                Finish::Settled {
                                    verdict: &verdict,
                                    output: &output,
                                    commit_id: None,
                                },
                                true,
                            )
                            .await
                        }
                        RequestReport::Gone => mark_lost(
                            orch,
                            &db,
                            id,
                            "every machine has reported its work since Cairn restarted, and none holds or finished it",
                        )
                        .await,
                    }
                };
                if let Ok(Some(batch)) = finished {
                    wake_job(orch, &batch.job_id, &batch.request_id).await;
                    settled += 1;
                }
            }
        }
        for job_id in jobs_with_waiting_notices(&db).await {
            wake_job(orch, &job_id, "a waiting result notice").await;
        }
    }
    settled
}

/// Jobs with a run-batch result notice still queued.
async fn jobs_with_waiting_notices(db: &LocalDb) -> Vec<String> {
    let cutoff =
        chrono::Utc::now().timestamp() - crate::orchestrator::wakes::NOTICE_DELIVERY_HORIZON_SECS;
    db.query_all(
        "SELECT DISTINCT job_id FROM suppressed_wakes WHERE source_kind='run_batch' AND delivered_at IS NULL AND created_at > ?1",
        params![cutoff],
        |row| row.text(0),
    )
    .await
    .unwrap_or_default()
}

pub(crate) async fn insert(db: &LocalDb, batch: &DetachedBatch) -> Result<(), String> {
    let batch = batch.clone();
    db.write(|c| {
        let b = batch.clone();
        Box::pin(async move {
            c.execute(
                &format!("INSERT INTO run_batch_detached({COLUMNS}) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18)"),
                params![
                    b.request_id.as_str(),
                    b.job_id.as_str(),
                    b.run_id.as_str(),
                    b.label.as_deref(),
                    b.identity.as_deref(),
                    b.commits as i64,
                    b.holds_branch as i64,
                    b.state.as_str(),
                    b.verdict.as_deref(),
                    b.output.as_deref(),
                    b.commit_id.as_deref(),
                    b.reason.as_str(),
                    b.lost_evidence.as_deref(),
                    b.created_ms,
                    b.settled_ms,
                    b.notified_ms,
                    b.settlement
                        .as_ref()
                        .and_then(|settlement| serde_json::to_string(settlement).ok()),
                    b.waiting_on.as_deref()
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|error| format!("record detached run batch: {error}"))
}

pub(crate) async fn load(db: &LocalDb, request_id: &str) -> Result<Option<DetachedBatch>, String> {
    db.query_opt(
        format!("SELECT {COLUMNS} FROM run_batch_detached WHERE request_id=?1"),
        params![request_id.to_string()],
        from_row,
    )
    .await
    .map_err(|error| error.to_string())
}

/// Every detached batch of one job, newest first.
pub(crate) async fn for_job(db: &LocalDb, job_id: &str) -> Result<Vec<DetachedBatch>, String> {
    db.query_all(
        format!(
            "SELECT {COLUMNS} FROM run_batch_detached WHERE job_id=?1 ORDER BY created_ms DESC"
        ),
        params![job_id.to_string()],
        from_row,
    )
    .await
    .map_err(|error| error.to_string())
}

/// A parked batch has been handed to a machine: it runs now, and holds its
/// branch when `holds_branch`. `false` when the row was not parked.
pub(crate) async fn mark_admitted(
    db: &LocalDb,
    request_id: &str,
    holds_branch: bool,
) -> Result<bool, String> {
    let id = request_id.to_string();
    db.write(|c| {
        let id = id.clone();
        Box::pin(async move {
            let changed = c
                .execute(
                    "UPDATE run_batch_detached SET state='running',holds_branch=?2,waiting_on=NULL WHERE request_id=?1 AND state='parked'",
                    params![id, holds_branch as i64],
                )
                .await?;
            Ok(changed > 0)
        })
    })
    .await
    .map_err(|error| format!("admit detached run batch: {error}"))
}

/// Restate what a parked batch waits for.
pub(crate) async fn note_waiting(db: &LocalDb, request_id: &str, waiting: &str) {
    let (id, waiting) = (request_id.to_string(), waiting.to_string());
    let updated = db
        .write(|c| {
            let (id, waiting) = (id.clone(), waiting.clone());
            Box::pin(async move {
                c.execute(
                    "UPDATE run_batch_detached SET waiting_on=?2 WHERE request_id=?1 AND state='parked'",
                    params![id, waiting],
                )
                .await?;
                Ok(())
            })
        })
        .await;
    if let Err(error) = updated {
        log::warn!("could not restate what parked run batch {request_id} waits for: {error}");
    }
}

/// The job's batches still unsettled detached: parked or running.
pub(crate) async fn running_for_job(
    db: &LocalDb,
    job_id: &str,
) -> Result<Vec<DetachedBatch>, String> {
    db.query_all(
        format!("SELECT {COLUMNS} FROM run_batch_detached WHERE job_id=?1 AND state IN ('parked','running')"),
        params![job_id.to_string()],
        from_row,
    )
    .await
    .map_err(|error| error.to_string())
}

/// Every batch still unsettled detached in this database: parked or running.
pub(crate) async fn all_running(db: &LocalDb) -> Result<Vec<DetachedBatch>, String> {
    db.query_all(
        format!("SELECT {COLUMNS} FROM run_batch_detached WHERE state IN ('parked','running')"),
        (),
        from_row,
    )
    .await
    .map_err(|error| error.to_string())
}

/// How a running batch ends.
enum Finish<'a> {
    Settled {
        verdict: &'a str,
        output: &'a str,
        commit_id: Option<&'a str>,
    },
    Lost {
        evidence: &'a str,
    },
}

/// Queue Cairn's notice for a finished batch on the connection settling it.
/// Keyed by the request, so a batch is queued once however often it is
/// reported.
pub(crate) async fn queue_notice(
    c: &cairn_db::storage::TrackedConnection,
    job_id: &str,
    request_id: &str,
    sentence: &str,
    now_secs: i64,
) -> DbResult<()> {
    c.execute(
        "INSERT OR IGNORE INTO suppressed_wakes (id, subscription_id, job_id, source_kind, source_ref, fact_kind, occurrences, latest_detail_uri, content, created_at, updated_at, delivered_at) VALUES (?1, NULL, ?2, ?3, ?4, ?5, 1, NULL, ?6, ?7, ?7, NULL)",
        params![
            crate::orchestrator::wakes::run_batch_notice_id(request_id),
            job_id,
            crate::orchestrator::wakes::SOURCE_KIND_RUN_BATCH,
            request_id,
            crate::orchestrator::wakes::FACT_KIND_MESSAGE,
            sentence,
            now_secs
        ],
    )
    .await?;
    Ok(())
}

/// End a parked or running batch, and, when `notify`, queue its notice in the same
/// transaction: a finished batch is never left without the notice its job is
/// owed, whatever crashes after. `None` when it was not running, so something
/// else already finished it and this call queues nothing.
async fn finish(
    db: &LocalDb,
    request_id: &str,
    finish: Finish<'_>,
    notify: bool,
) -> Result<Option<DetachedBatch>, String> {
    let (state, verdict, output, commit, evidence) = match finish {
        Finish::Settled {
            verdict,
            output,
            commit_id,
        } => (
            "settled",
            Some(verdict.to_string()),
            Some(output.to_string()),
            commit_id.map(str::to_string),
            None,
        ),
        Finish::Lost { evidence } => ("lost", None, None, None, Some(evidence.to_string())),
    };
    let id = request_id.to_string();
    db.write(|c| {
        let (id, verdict, output, commit, evidence) = (
            id.clone(),
            verdict.clone(),
            output.clone(),
            commit.clone(),
            evidence.clone(),
        );
        Box::pin(async move {
            let now = chrono::Utc::now().timestamp_millis();
            let changed = c
                .execute(
                    "UPDATE run_batch_detached SET state=?2,verdict=?3,output=?4,commit_id=?5,lost_evidence=?6,settled_ms=?7,waiting_on=NULL WHERE request_id=?1 AND state IN ('parked','running')",
                    params![id.clone(), state, verdict, output, commit, evidence, now],
                )
                .await?;
            if changed == 0 {
                return Ok(None);
            }
            let mut rows = c
                .query(
                    &format!("SELECT {COLUMNS} FROM run_batch_detached WHERE request_id=?1"),
                    params![id.clone()],
                )
                .await?;
            let mut batch = match rows.next().await? {
                Some(row) => from_row(&row)?,
                None => return Ok(None),
            };
            drop(rows);
            if notify {
                queue_notice(c, &batch.job_id, &id, &notice_sentence(&batch), now / 1000)
                    .await?;
                c.execute(
                    "UPDATE run_batch_detached SET notified_ms=?2 WHERE request_id=?1",
                    params![id, now],
                )
                .await?;
                batch.notified_ms = Some(now);
            }
            Ok(Some(batch))
        })
    })
    .await
    .map_err(|error| format!("finish detached run batch: {error}"))
}

/// [`finish`] a running batch, then release the branch hold it made. Every
/// way a detached batch ends (its answer settling it with a notice or inline,
/// a completion its machine lists, or evidence that it was lost) comes through
/// here, so its hold is released exactly once: only the call whose write
/// actually moved the row off `parked` or `running` releases it, and a batch that never
/// held the branch releases nothing. The release is spawned, so it never runs
/// under whatever guard, reservation or permit the caller holds; it does
/// nothing while anything else still holds the branch.
async fn end(
    orch: &Orchestrator,
    db: &LocalDb,
    request_id: &str,
    how: Finish<'_>,
    notify: bool,
) -> Result<Option<DetachedBatch>, String> {
    let ended = finish(db, request_id, how, notify).await?;
    if let Some(batch) = ended.as_ref().filter(|batch| batch.holds_branch) {
        crate::orchestrator::base_advance::on_branch_hold_released(orch, &batch.job_id);
    }
    Ok(ended)
}

/// Settle a running batch as lost, naming the evidence that it can no longer
/// report, and queue the notice that says so. `None` when it was not running.
pub(crate) async fn mark_lost(
    orch: &Orchestrator,
    db: &LocalDb,
    request_id: &str,
    evidence: &str,
) -> Result<Option<DetachedBatch>, String> {
    end(orch, db, request_id, Finish::Lost { evidence }, true).await
}

/// Wake a job whose batch result notice is queued, if it is idle. A job busy
/// with a turn takes the notice at its next tool boundary; one suspended on
/// its own work takes it when that resumes.
async fn wake_job(orch: &Orchestrator, job_id: &str, what: &str) {
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "suppressed_wakes", "action": "insert"}),
    );
    let (owned, job) = (orch.clone(), job_id.to_string());
    let nudged = tokio::task::spawn_blocking(move || {
        crate::messages::delivery::nudge_job_for_urgency(
            &owned,
            &job,
            crate::messages::queued::DeliveryUrgency::Queue,
        )
    })
    .await;
    let short = &job_id[..job_id.len().min(8)];
    match nudged {
        Ok(Ok(outcome)) => log::debug!("woke job {short} for {what}: {outcome:?}"),
        Ok(Err(error)) => log::warn!("could not wake job {short} for {what}: {error}"),
        Err(error) => log::warn!("waking job {short} for {what} failed: {error}"),
    }
}

/// Settle a running batch with its answer, queue its notice in the same
/// write, and wake its job. This is the one path a batch's result takes to a
/// job whose call can no longer receive it. Reports whether this call settled
/// it.
pub(crate) async fn settle_and_deliver(
    orch: &Orchestrator,
    db: &LocalDb,
    request_id: &str,
    verdict: &str,
    output: &str,
) -> bool {
    let commit = super::run_commit_sha(output);
    let settled = Finish::Settled {
        verdict,
        output,
        commit_id: commit.as_deref(),
    };
    match end(orch, db, request_id, settled, true).await {
        Ok(Some(batch)) => {
            wake_job(orch, &batch.job_id, &batch.request_id).await;
            true
        }
        Ok(None) => false,
        Err(error) => {
            log::warn!("detached run batch {request_id} could not be settled: {error}");
            false
        }
    }
}

/// Settle a running batch whose call received the answer inline, so no notice
/// is owed. Used when the row was written ahead of need, or when a wait
/// converted at abandonment was answered on its socket after all.
pub(crate) async fn settle_answered_inline(
    orch: &Orchestrator,
    db: &LocalDb,
    request_id: &str,
    verdict: &str,
    output: &str,
) {
    let commit = super::run_commit_sha(output);
    let settled = Finish::Settled {
        verdict,
        output,
        commit_id: commit.as_deref(),
    };
    if let Err(error) = end(orch, db, request_id, settled, false).await {
        log::warn!("detached run batch {request_id} could not be settled: {error}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batch(state: DetachedState, commits: bool, commit_id: Option<&str>) -> DetachedBatch {
        DetachedBatch {
            request_id: "req-1".into(),
            job_id: "job".into(),
            run_id: "run".into(),
            label: Some("cargo test".into()),
            identity: None,
            commits,
            holds_branch: true,
            state,
            verdict: Some("succeeded".into()),
            output: Some("ok".into()),
            commit_id: commit_id.map(str::to_string),
            reason: "the project store was busy past its answer deadline".into(),
            lost_evidence: Some("its machine's enrolment is gone".into()),
            created_ms: 1,
            settled_ms: None,
            notified_ms: None,
            settlement: None,
            waiting_on: None,
        }
    }

    /// An orchestrator over [`db_with_job`], for the ending paths that release
    /// a branch hold.
    async fn orch_with_job() -> Orchestrator {
        let search = std::sync::Arc::new(
            crate::storage::SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep())
                .unwrap(),
        );
        crate::orchestrator::OrchestratorBuilder::new(
            std::sync::Arc::new(crate::db::DbState::new(
                std::sync::Arc::new(db_with_job().await),
                search,
            )),
            std::sync::Arc::new(crate::services::testing::TestServicesBuilder::new().build()),
            tempfile::tempdir().unwrap().keep(),
        )
        .build()
    }

    async fn db_with_job() -> LocalDb {
        let db = crate::storage::migrated_test_db("detached.db").await;
        for sql in [
            "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1)",
            "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES('p','w','P','p','/tmp',1,1)",
            "INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at) VALUES('i','p',1,'I','active','active','none',1,1)",
            "INSERT INTO jobs(id, project_id, issue_id, status, created_at, updated_at) VALUES('job','p','i','running',1,1)",
        ] {
            db.execute(sql, ()).await.unwrap();
        }
        db
    }

    fn running(request_id: &str) -> DetachedBatch {
        DetachedBatch {
            request_id: request_id.into(),
            verdict: None,
            output: None,
            ..batch(DetachedState::Running, false, None)
        }
    }

    async fn notices(db: &LocalDb) -> Vec<String> {
        crate::messages::side_channel::peek_pending_side_channel_for_job_async(db, "job")
            .await
            .unwrap()
            .iter()
            .map(|notice| notice.render())
            .collect()
    }

    /// The crash boundary: the notice a finished batch owes its job is written
    /// by the same transaction that finishes it, so no crash or failed wake
    /// after that write can leave a settled row with no notice. A second report
    /// finds nothing to finish and queues nothing; an inline answer owes none.
    #[tokio::test(flavor = "current_thread")]
    async fn finishing_a_batch_queues_its_notice_in_the_same_write() {
        let orch = orch_with_job().await;
        let db: &LocalDb = &orch.db.local;
        insert(db, &running("req-1")).await.unwrap();
        let settled = finish(
            db,
            "req-1",
            Finish::Settled {
                verdict: "succeeded",
                output: "ok",
                commit_id: None,
            },
            true,
        )
        .await
        .unwrap()
        .expect("it was running");
        assert!(settled.notified_ms.is_some());
        assert_eq!(notices(db).await, vec![notice_sentence(&settled)]);

        assert!(finish(db, "req-1", Finish::Lost { evidence: "x" }, true)
            .await
            .unwrap()
            .is_none());
        assert_eq!(notices(db).await.len(), 1);

        insert(db, &running("req-2")).await.unwrap();
        settle_answered_inline(&orch, db, "req-2", "succeeded", "inline").await;
        assert_eq!(
            load(db, "req-2").await.unwrap().unwrap().state,
            DetachedState::Settled
        );
        assert_eq!(
            notices(db).await.len(),
            1,
            "an inline answer owes no notice"
        );

        insert(db, &running("req-3")).await.unwrap();
        let lost = mark_lost(&orch, db, "req-3", "its machine's enrolment is gone")
            .await
            .unwrap()
            .unwrap();
        assert!(notices(db).await.contains(&notice_sentence(&lost)));
    }

    /// A batch a machine finished while Cairn was down settles from the
    /// completion it lists, and says what was lost with the old process.
    #[test]
    fn a_listed_completion_settles_with_what_survived() {
        let completion: cairn_common::executor_protocol::CellCompletion =
            serde_json::from_value(serde_json::json!({
                "requestId": "req-1",
                "attemptId": "a",
                "commandClass": "other",
                "command": "cargo test",
                "priority": "reviewCheck",
                "queuedAtUnixMs": 1,
                "finishedAtUnixMs": 2,
                "durationMs": 1,
                "verdict": "failed",
                "failureOutputTail": "test x ... FAILED",
                "servedAtUnixMs": 2
            }))
            .unwrap();
        let (verdict, output) = completion_result(&completion, true);
        assert_eq!(verdict, "failed");
        assert!(output.contains("full output was not recovered"), "{output}");
        assert!(output.contains("changes were not published"), "{output}");
        assert!(output.ends_with("test x ... FAILED"), "{output}");
    }

    #[test]
    fn the_notice_names_the_handle_the_verdict_and_what_it_committed() {
        assert_eq!(
            notice_sentence(&batch(DetachedState::Settled, false, None)),
            "Your run batch 'cargo test' (cairn:~/batches/req-1) finished: succeeded, committed nothing."
        );
        assert_eq!(
            notice_sentence(&batch(
                DetachedState::Settled,
                true,
                Some("0123456789abcdef0123")
            )),
            "Your run batch 'cargo test' (cairn:~/batches/req-1) finished: succeeded, commit 0123456789ab."
        );
        assert_eq!(
            notice_sentence(&batch(DetachedState::Lost, true, None)),
            "Your run batch 'cargo test' (cairn:~/batches/req-1) can no longer report (its machine's enrolment is gone); its outcome is unknown, and the handle shows what was recorded."
        );
    }

    #[test]
    fn a_commitless_batch_says_it_cannot_have_committed() {
        let running = render(
            &batch(DetachedState::Running, false, None),
            &LiveState::Untracked,
        );
        assert!(
            running.contains("it has no commit_msg, so it cannot commit"),
            "{running}"
        );
        let settled = render(
            &batch(DetachedState::Settled, true, Some("abcdef1234567")),
            &LiveState::Untracked,
        );
        assert!(
            settled.contains("Commits: commit abcdef123456"),
            "{settled}"
        );
        assert!(settled.contains("## Output\n\nok"), "{settled}");
    }

    /// A running row no machine has taken reads queued with what it waits on,
    /// never `running`; a parked one reads its own wait; and the reissue
    /// refusal names the request, that wait, and the handle to wait on.
    #[test]
    fn an_unsettled_row_reads_where_its_request_stands() {
        let queued = LiveState::Unplaced {
            wait: "waiting to be placed since 2026-11-07T03:46:44Z (360s); `local` declined it (ProjectProvisioning): project p is provisioning for request holder".into(),
        };
        let row = running("req-1");
        let read = render(&row, &queued);
        assert!(
            read.contains("State: queued, not yet placed on any machine"),
            "{read}"
        );
        assert!(!read.contains("State: running"), "{read}");
        assert!(read.contains("provisioning for request holder"), "{read}");
        assert!(summary_line(&row, "cairn://p/x/1/1/builder", &queued)
            .ends_with("queued, not yet placed on any machine"));
        let refusal = reissue_refusal_text("req-1", &queued);
        assert!(refusal.contains("request `req-1`"), "{refusal}");
        assert!(refusal.contains("ProjectProvisioning"), "{refusal}");
        assert!(refusal.contains("cairn:~/batches/req-1"), "{refusal}");
        assert!(render(
            &row,
            &LiveState::Placed {
                on: "bgsrv-ub".into()
            }
        )
        .contains("State: running on `bgsrv-ub`"));
        let parked = LiveState::Parked {
            waiting: "waiting for the job's residency on bgsrv-mac: running `batch-1`".into(),
        };
        let refusal = reissue_refusal_text("req-2", &parked);
        assert!(refusal.contains("request `req-2`"), "{refusal}");
        assert!(refusal.contains("running `batch-1`"), "{refusal}");
        assert!(refusal.contains("holds no branch"), "{refusal}");
    }

    #[test]
    fn the_detach_answer_says_nothing_was_cancelled_and_names_the_handle() {
        assert_eq!(
            detach_answer_text("req-1", "the project store was busy past its answer deadline"),
            "This batch is still running. Cairn could not suspend this call (the project store was busy past its answer deadline), so the call answers now instead. Nothing was cancelled. When it settles, Cairn delivers its result to this job as a notice, and you can read it any time at `cairn:~/batches/req-1`. Do not re-run it."
        );
        assert!(!crate::mcp::handlers::run::envelope_reports_run_failure(
            &detach_answer_text("req-1", "x")
        ));
    }

    /// The ending functions, enumerated from the source: a detached row leaves
    /// `parked` or `running` in exactly one production statement, inside
    /// [`finish`]; the only other state write is [`mark_admitted`] moving a
    /// parked row to `running`, which ends nothing;
    /// `finish` is called only by [`end`]; `end` is the only caller of the
    /// branch-hold release; and every way a batch ends reaches `end`. No other
    /// production module of the run path, or the durable-suspend resolution
    /// that converts waits into these rows, writes a row's state. A new ending
    /// that bypassed `end` would leave the branch held until some unrelated
    /// release, so it fails here.
    #[test]
    fn every_detached_ending_releases_its_branch_hold_through_end() {
        // A source with its top-level `#[cfg(test)]` modules removed. Their
        // bodies are indented, so each ends at the first column-0 brace.
        fn production(source: &str) -> String {
            let mut kept = String::new();
            let mut rest = source;
            while let Some(start) = rest.find("#[cfg(test)]\nmod ") {
                kept.push_str(&rest[..start]);
                let module = &rest[start..];
                let close = module.find("\n}\n").map_or(module.len(), |at| at + 3);
                rest = &module[close..];
            }
            kept.push_str(rest);
            kept
        }
        fn body<'a>(source: &'a str, signature: &str) -> &'a str {
            let start = source
                .find(signature)
                .unwrap_or_else(|| panic!("`{signature}` is not in detached.rs"));
            let rest = &source[start + signature.len()..];
            let close = rest.find("\n}\n").expect("a top-level body ends");
            &rest[..close]
        }
        let own = production(include_str!("detached.rs"));
        let state_write = "UPDATE run_batch_detached SET state";
        assert_eq!(own.matches(state_write).count(), 2);
        assert!(body(&own, "async fn finish(").contains(state_write));
        let admission = body(&own, "pub(crate) async fn mark_admitted(");
        assert!(
            admission.contains("SET state='running',") && admission.contains("AND state='parked'"),
            "the one state write outside `finish` only admits a parked row"
        );
        let release = "base_advance::on_branch_hold_released";
        assert!(body(&own, "async fn end(").contains(release));
        assert_eq!(
            own.matches(&format!("{release}(")).count(),
            1,
            "only `end` releases a detached batch's hold"
        );
        // The definition and the one call, in `end`.
        assert_eq!(
            own.matches("finish(").count(),
            2,
            "`finish` is called only by `end`"
        );
        assert!(body(&own, "async fn end(").contains("finish(db,"));
        for ending in [
            "pub(crate) async fn mark_lost(",
            "pub(crate) async fn settle_and_deliver(",
            "pub(crate) async fn settle_answered_inline(",
            "pub(crate) async fn sweep_unreported(",
        ] {
            let ending_body = body(&own, ending);
            assert!(
                ending_body.contains(" end(") || ending_body.contains("mark_lost("),
                "{ending} must end through `end`"
            );
            assert!(!ending_body.contains("finish("), "{ending} bypasses `end`");
        }
        for (name, other) in [
            ("run/mod.rs", production(include_str!("mod.rs"))),
            (
                "durable_suspend.rs",
                production(include_str!("../durable_suspend.rs")),
            ),
            ("run/adopted.rs", production(include_str!("adopted.rs"))),
        ] {
            assert!(
                !other.contains("UPDATE run_batch_detached"),
                "{name} writes a detached row's state outside `finish`"
            );
        }
    }
}
