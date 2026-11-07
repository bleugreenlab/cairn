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
//! - the replay mutation boundary holds the branch while it is unsettled.
//!
//! A runner restart does not settle it. Only the batch's report, or evidence
//! that it can no longer report, does.

use cairn_db::turso::params;

use crate::orchestrator::Orchestrator;
use crate::storage::{DbResult, LocalDb, RowExt};

/// Where a detached batch stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DetachedState {
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
            Self::Running => "running",
            Self::Settled => "settled",
            Self::Lost => "lost",
        }
    }

    fn parse(raw: &str) -> Self {
        match raw {
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
}

const COLUMNS: &str = "request_id,job_id,run_id,label,identity,commits,holds_branch,state,verdict,output,commit_id,reason,lost_evidence,created_ms,settled_ms,notified_ms,settlement_json";

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

/// Request ids whose batch task is still running in this process, from
/// preparation through publication. It covers what the fleet registry does
/// not: a batch between placement attempts, or settling after its process
/// ended.
static LIVE_BATCHES: std::sync::LazyLock<std::sync::Mutex<std::collections::HashSet<String>>> =
    std::sync::LazyLock::new(Default::default);

/// Marks one batch's task as running for as long as it lives.
pub(crate) struct LiveBatch(String);

impl LiveBatch {
    pub(crate) fn claim(request_id: &str) -> Self {
        LIVE_BATCHES
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(request_id.to_string());
        PLACED_HERE
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(request_id.to_string());
        Self(request_id.to_string())
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
        .contains(request_id)
        || orch.is_cell_request_in_flight(request_id)
}

/// Whether a resolver in this process is waiting on the batch's answer.
pub(crate) fn has_live_resolver(request_id: &str) -> bool {
    LIVE_RESOLVERS
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .contains(request_id)
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

/// What a reissue of a batch that is still running detached is told.
pub(crate) fn reissue_refusal_text(request_id: &str) -> String {
    format!(
        "This batch is already running, detached from the call that started it; its result reaches this job as a notice when it settles, and `{}` shows it now. Nothing new was started.",
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
pub(crate) fn render(batch: &DetachedBatch) -> String {
    let mut out = format!(
        "# Run batch `{}`\n\nLabel: {}\nState: {}\n",
        batch.request_id,
        label_of(batch),
        batch.state.as_str()
    );
    out.push_str(&format!("Detached because: {}\n", batch.reason));
    match batch.state {
        DetachedState::Running => {
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
pub(crate) fn summary_line(batch: &DetachedBatch, home: &str) -> String {
    let state = match batch.state {
        DetachedState::Running => "running".to_string(),
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
                let finished = if placed_here(id) {
                    mark_lost(
                        &db,
                        id,
                        "its batch ended in this runner without its result reaching Cairn",
                    )
                    .await
                } else if placed_on.is_some_and(super::PlacedOn::colocated) {
                    mark_lost(
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
                            finish(
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
                &format!("INSERT INTO run_batch_detached({COLUMNS}) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17)"),
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
                        .and_then(|settlement| serde_json::to_string(settlement).ok())
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

/// The job's batches still running detached.
pub(crate) async fn running_for_job(
    db: &LocalDb,
    job_id: &str,
) -> Result<Vec<DetachedBatch>, String> {
    db.query_all(
        format!("SELECT {COLUMNS} FROM run_batch_detached WHERE job_id=?1 AND state='running'"),
        params![job_id.to_string()],
        from_row,
    )
    .await
    .map_err(|error| error.to_string())
}

/// Every batch still running detached in this database.
pub(crate) async fn all_running(db: &LocalDb) -> Result<Vec<DetachedBatch>, String> {
    db.query_all(
        format!("SELECT {COLUMNS} FROM run_batch_detached WHERE state='running'"),
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

/// End a running batch, and, when `notify`, queue its notice in the same
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
                    "UPDATE run_batch_detached SET state=?2,verdict=?3,output=?4,commit_id=?5,lost_evidence=?6,settled_ms=?7 WHERE request_id=?1 AND state='running'",
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

/// Settle a running batch as lost, naming the evidence that it can no longer
/// report, and queue the notice that says so. `None` when it was not running.
pub(crate) async fn mark_lost(
    db: &LocalDb,
    request_id: &str,
    evidence: &str,
) -> Result<Option<DetachedBatch>, String> {
    finish(db, request_id, Finish::Lost { evidence }, true).await
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
    match finish(db, request_id, settled, true).await {
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
    if let Err(error) = finish(db, request_id, settled, false).await {
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
        }
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
        let db = db_with_job().await;
        insert(&db, &running("req-1")).await.unwrap();
        let settled = finish(
            &db,
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
        assert_eq!(notices(&db).await, vec![notice_sentence(&settled)]);

        assert!(finish(&db, "req-1", Finish::Lost { evidence: "x" }, true)
            .await
            .unwrap()
            .is_none());
        assert_eq!(notices(&db).await.len(), 1);

        insert(&db, &running("req-2")).await.unwrap();
        settle_answered_inline(&db, "req-2", "succeeded", "inline").await;
        assert_eq!(
            load(&db, "req-2").await.unwrap().unwrap().state,
            DetachedState::Settled
        );
        assert_eq!(
            notices(&db).await.len(),
            1,
            "an inline answer owes no notice"
        );

        insert(&db, &running("req-3")).await.unwrap();
        let lost = mark_lost(&db, "req-3", "its machine's enrolment is gone")
            .await
            .unwrap()
            .unwrap();
        assert!(notices(&db).await.contains(&notice_sentence(&lost)));
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
        let running = render(&batch(DetachedState::Running, false, None));
        assert!(
            running.contains("it has no commit_msg, so it cannot commit"),
            "{running}"
        );
        let settled = render(&batch(DetachedState::Settled, true, Some("abcdef1234567")));
        assert!(
            settled.contains("Commits: commit abcdef123456"),
            "{settled}"
        );
        assert!(settled.contains("## Output\n\nok"), "{settled}");
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
}
