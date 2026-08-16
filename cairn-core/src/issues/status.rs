use crate::issues::crud;
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};
use crate::transitions::Resolution;
use cairn_db::turso::params;

/// Who is driving a status resolution. This gates exactly one check: the
/// `merged`-with-an-open-PR redirect. An agent has a better lever available
/// (patch the PR's create-pr artifact with `action:"merge"`), so it is pointed
/// at that lever; a person marking an issue merged in the UI has no such lever
/// and is not redirected.
///
/// Live work is deliberately NOT gated on the actor. Stopping it is a
/// confirmation both callers give the same way — see [`Confirmation`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolutionActor {
    /// A person acting through the UI.
    User,
    /// An agent/recipe via the MCP write path — refuse a `merged` resolution
    /// while a PR is open and name the real merge lever (CAIRN-2287).
    Agent,
}

/// Whether the caller has acknowledged that resolving this issue will stop the
/// work still live on it.
///
/// Live work makes a terminal resolution a confirmation, never a wall
/// (CAIRN-3212). The first unconfirmed attempt is refused with the live work
/// enumerated and the confirming key named; a confirmed attempt stops that work
/// and resolves. Deliberateness lives in the explicit key rather than in the
/// species of caller: the MCP write path sends `payload.confirm`, and the UI
/// sends the same flag once its dialog is accepted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Confirmation {
    /// Not confirmed. Live work refuses the resolution and names the key.
    Absent,
    /// Confirmed. Live work is stopped, then the issue resolves.
    Given,
}

impl Confirmation {
    /// Lift a caller's boolean confirm flag into the typed confirmation.
    pub fn from_flag(confirmed: bool) -> Self {
        if confirmed {
            Self::Given
        } else {
            Self::Absent
        }
    }
}

/// A job still live on an issue when a terminal resolution is requested.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveJob {
    pub id: String,
    /// What the work is, in the reader's vocabulary: the node's name, else the
    /// agent it runs, else a generic label.
    pub name: String,
    /// Stored job status: `running`, `idle`, `pending`, or `blocked`.
    pub status: String,
    /// True when the job belongs to another issue but branches off this one — a
    /// reviewer or follow-up started from this issue's branch.
    pub from_dependent_issue: bool,
}

impl LiveJob {
    /// Whether the job has actually started. A started job holds a session that
    /// must be stopped; an unstarted one is a DAG entry that is simply cancelled.
    pub fn is_started(&self) -> bool {
        matches!(self.status.as_str(), "running" | "idle")
    }

    /// One plain-language line naming the work and its state, for a refusal or a
    /// confirmation dialog. No DAG or job-state vocabulary beyond what the
    /// reader can act on.
    pub fn summary(&self) -> String {
        let state = match self.status.as_str() {
            "running" => "running now",
            "idle" => "open between turns",
            "blocked" => "waiting for an approval",
            "pending" => "queued, never started",
            other => other,
        };
        if self.from_dependent_issue {
            format!("{} ({state}, on a branch off this issue)", self.name)
        } else {
            format!("{} ({state})", self.name)
        }
    }
}

/// Why a status update did not apply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolutionRefusal {
    /// The issue still has live work and the caller has not confirmed stopping
    /// it. Carries the work itself so a caller can enumerate it rather than
    /// re-deriving it.
    NeedsConfirmation {
        /// The resolution that was requested (`merged` or `closed`).
        status: String,
        live_work: Vec<LiveJob>,
    },
    /// Refused for a reason a confirmation cannot clear: an open PR on a
    /// `merged` resolution, a status that is not settable, or a storage failure.
    Rejected(String),
}

impl std::fmt::Display for ResolutionRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rejected(message) => write!(f, "{message}"),
            Self::NeedsConfirmation { status, live_work } => {
                let listed: Vec<String> = live_work.iter().map(LiveJob::summary).collect();
                write!(
                    f,
                    "This issue still has live work: {}. Marking it {status} stops that work — anything running is stopped so it can be resumed later, and anything that never started is cancelled. Send the same write again with confirm: true to go ahead.",
                    listed.join(", ")
                )
            }
        }
    }
}

impl From<String> for ResolutionRefusal {
    fn from(message: String) -> Self {
        Self::Rejected(message)
    }
}

/// Whether a status resolution may proceed, and what it would have to stop.
///
/// Returns the live work a confirmed resolution will stop — empty when the
/// status is not a resolution, or when nothing is live. Every refusal a
/// resolution can raise is raised here and nowhere else.
///
/// Two callers share it. [`update_status`] calls it as its own gate. The MCP
/// issue-patch handler calls it *before* applying any other field of the same
/// write, so a refused combined patch (`{title, status}`) leaves the title
/// unchanged too — a refusal that says it changed nothing has to be true of the
/// whole write, not just the resolution.
pub async fn check_resolution(
    orch: &Orchestrator,
    id: &str,
    status: &str,
    actor: ResolutionActor,
    confirmation: Confirmation,
) -> Result<Vec<LiveJob>, ResolutionRefusal> {
    match status {
        // A resolution: keep checking below.
        "merged" | "closed" => {}
        // Unresolving has no live work to stop.
        "backlog" => return Ok(Vec::new()),
        // Every other status is derived from execution state. This is checked
        // here, not only where the status is applied, so that the invariant
        // holds for the whole surface: a caller that pre-checks gets the same
        // refusal it would have gotten later, before it has written anything.
        _ => return Err(unsettable_status_refusal(status)),
    }

    // `merged` while a PR is still OPEN would record a resolution WITHOUT
    // merging — stranding the branch's commits and auto-closing the PR
    // unmerged. Refuse and name the real merge lever (CAIRN-2287). Merging
    // through the PR resolves the issue itself, so an agent never needs the
    // status patch to merge.
    if status == "merged" && matches!(actor, ResolutionActor::Agent) {
        if let Some((project_key, number)) = open_merge_request_for_issue(orch, id).await? {
            return Err(ResolutionRefusal::Rejected(format!(
                "Refusing to mark {project_key}/{number} merged: it still has an OPEN pull request. Setting status=merged records a resolution WITHOUT merging the PR — it strands the branch's commits and auto-closes the PR unmerged. Merge through the PR instead:\n  write({{changes:[{{target:\"cairn://p/{project_key}/{number}/1/builder/create-pr\",mode:\"patch\",payload:{{action:\"merge\"}}}}]}})\nThat merge resolves this issue for you. If the PR was already merged externally, refresh it instead with payload:{{action:\"refresh\"}}."
            )));
        }
    }

    // Live work on the issue is a confirmation, not a blocker (CAIRN-3212).
    // Unconfirmed, the refusal enumerates the work and names the key.
    // content→execution boundary (CAIRN-2181): this queries runs/jobs, which
    // stay private until CAIRN-2182.
    let live_work = live_work_for_issue(orch, id).await?;
    if !live_work.is_empty() && matches!(confirmation, Confirmation::Absent) {
        return Err(ResolutionRefusal::NeedsConfirmation {
            status: status.to_string(),
            live_work,
        });
    }
    Ok(live_work)
}

pub async fn update_status(
    orch: &Orchestrator,
    id: &str,
    status: &str,
    actor: ResolutionActor,
    confirmation: Confirmation,
) -> Result<(), ResolutionRefusal> {
    let is_terminal_state = status == "merged" || status == "closed";

    // Every refusal comes from here, before anything is written.
    check_resolution(orch, id, status, actor, confirmation).await?;

    // The issue row lives in its owning database — the private DB for a local
    // project, the team replica for a team project (CAIRN-2181). resolve/unresolve
    // mutate that row (and close sessions that, for a team project, are empty in
    // the replica), so they must target the same DB the row is read from.
    let owning_db = crud::owning_db_for_issue(&orch.db, id)
        .await
        .map_err(|e| e.to_string())?;

    match status {
        // A confirmed resolution runs the cascade, which stops the live work
        // before the rows move — and only proceeds if that succeeded, so a
        // failed stop leaves the issue open rather than closing it over work
        // that is still running.
        "merged" => {
            resolve_terminal(
                orch,
                &owning_db,
                id,
                Resolution::Merged,
                StopFailure::Refuses,
            )
            .await?
        }
        "closed" => {
            resolve_terminal(
                orch,
                &owning_db,
                id,
                Resolution::Closed,
                StopFailure::Refuses,
            )
            .await?
        }
        "backlog" => crud::unresolve(&owning_db, &*orch.services.clock, id)
            .await
            .map_err(|e| e.to_string())?,
        // Unreachable in practice: `check_resolution` above refuses an
        // unsettable status before anything is written. Kept as the exhaustive
        // arm, sharing the one message so the two can never disagree.
        _ => return Err(unsettable_status_refusal(status)),
    }

    if is_terminal_state {
        crate::execution::advancement::release_dependent_executions(orch, id).await?;
    }

    let issue = crud::get(&owning_db, id)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("Issue not found after status update: {id}"))?;
    if is_terminal_state {
        let discord = crate::config::settings::load_settings(&orch.config_dir)
            .channels
            .discord;
        if discord.enabled {
            let guild_id = discord
                .guild_id
                .parse::<u64>()
                .map_err(|_| "Discord guild ID must be an unsigned integer".to_string())?;
            let project = crate::projects::crud::get_db(&owning_db, &issue.project_id)
                .await
                .map_err(|error| error.to_string())?
                .ok_or_else(|| {
                    format!("Project not found for terminal issue: {}", issue.project_id)
                })?;
            let target = format!("cairn://p/{}/{}", project.key, issue.number);
            if crate::channels::discord_surfaces::request_issue_lock(
                &owning_db,
                guild_id,
                &target,
                chrono::Utc::now().timestamp(),
            )
            .await?
            {
                crate::channels::wake_discord_surfaces();
            }
        }
    }
    let _ = orch.services.emitter.emit(
        "db-change",
        crate::notify::issue_db_change(&issue, "update"),
    );
    orch.invalidate_sidebar_active_issues();

    orch.wake_for_issue(id).await;

    if is_terminal_state {
        let orch_clone = orch.clone();
        let issue_id = id.to_string();
        let status_label = status.to_string();
        // A merged resolution recorded here did NOT run a fold (that is the PR
        // action's job), so teardown must preserve any branch whose commits have
        // not landed — the KMCP data-loss guard (CAIRN-2287). A closed resolution
        // is an explicit discard and deletes branches as before.
        let reason = if status == "merged" {
            crate::execution::teardown::TeardownReason::Merged
        } else {
            crate::execution::teardown::TeardownReason::Discarded
        };

        tokio::spawn(async move {
            log::info!(
                "Starting background worktree teardown for issue {} (status: {})",
                issue_id,
                status_label
            );

            if let Err(error) = crate::execution::teardown::cleanup_issue_jobs(
                &orch_clone,
                crate::execution::teardown::TeardownScope::Issue(issue_id),
                reason,
            )
            .await
            {
                log::warn!("Worktree teardown for issue failed: {}", error);
            }
        });
    }

    Ok(())
}

/// How a failed stop is handled when an issue is going terminal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StopFailure {
    /// Nothing has been recorded yet, so a stop that fails refuses the whole
    /// resolution: the issue stays open rather than resolving over work that is
    /// still running, and the caller can simply send the same write again.
    Refuses,
    /// The resolution already happened somewhere Cairn cannot take back — a PR
    /// merged on GitHub, a fold landed in the shared store — so the record has
    /// to follow it. A failed stop is logged and left to the postcondition,
    /// which settles whatever the stop could not.
    Escalates,
}

/// Resolve an issue into a terminal status, running the cascade that makes the
/// resolution true of the issue's *work* and not only of its row.
///
/// This is the one door into a terminal resolution, and every caller comes
/// through it: the status patch from the agent write path and the UI, the PR
/// merge and PR close, and the recipe `close_issue` action. The cascade is what
/// separates a resolved issue from a resolved record. CAIRN-3241 merged while
/// its builder was mid-batch, and because the merge path resolved the row
/// directly, the builder went on running test suites against a merged issue
/// while the same resolution had already closed its session out from under it —
/// leaving an operator who tried to intervene with a session that refused to
/// continue and runs that would not stop.
///
/// The order is the substance:
///
/// 1. **Stop the live work first**, through [`stop_live_work`]: the canonical
///    node stop for anything started (parked warm and resumable, cascading to
///    child runs and cancelling the executor's in-flight batches) and a cancel
///    for anything that never started. Stopping before the rows move is what
///    keeps a session from being closed over a running turn.
/// 2. Resolve the issue row and close its sessions.
/// 3. Drop the warm processes those sessions held, so their capacity frees.
/// 4. Quit any in-flight turn-end review suite (CAIRN-2648).
/// 5. Check the postcondition and settle anything that violates it.
///
/// It deliberately does NOT tear down worktrees, release dependent executions,
/// or emit — those differ per caller and stay with them. Teardown in particular
/// must run *after* this: killing an agent's terminals while the agent itself is
/// still running only invites it to open new ones, which is why the live
/// specimen still had a terminal running post-merge.
pub(crate) async fn resolve_terminal(
    orch: &Orchestrator,
    owning_db: &LocalDb,
    issue_id: &str,
    resolution: Resolution,
    on_stop_failure: StopFailure,
) -> Result<(), ResolutionRefusal> {
    let live_work = live_work_for_issue(orch, issue_id).await?;
    if !live_work.is_empty() {
        if let Err(refusal) = stop_live_work(orch, issue_id, &live_work).await {
            match on_stop_failure {
                StopFailure::Refuses => return Err(refusal),
                StopFailure::Escalates => log::warn!(
                    "Terminal resolution of issue {issue_id} could not stop all of its work cleanly ({refusal}); the resolution has already landed elsewhere, so recording it anyway and settling what is left"
                ),
            }
        }
    }

    if let Err(error) =
        crate::execution::teardown::release_issue_dev_instances(orch, issue_id).await
    {
        match on_stop_failure {
            StopFailure::Refuses => {
                return Err(ResolutionRefusal::Rejected(format!(
                    "Refusing terminal resolution because dev-instance teardown was not verified: {error}"
                )))
            }
            StopFailure::Escalates => log::warn!(
                "Terminal resolution of issue {issue_id} could not verify dev-instance teardown ({error}); the resolution already landed elsewhere, so recording it and leaving startup reconciliation to retry"
            ),
        }
    }

    let closed_sessions = crud::resolve(owning_db, &*orch.services.clock, issue_id, resolution)
        .await
        .map_err(|e| ResolutionRefusal::Rejected(e.to_string()))?;

    for session_id in &closed_sessions {
        if let Some(run_id) = orch.process_state.remove_by_session(session_id) {
            log::info!(
                "Evicted warm process {} for closed session {}",
                &run_id[..run_id.len().min(8)],
                &session_id[..session_id.len().min(8)]
            );
        }
    }

    // The issue is resolved — quit any in-flight turn-end review suite so it does
    // not keep running against a merged/closed issue (CAIRN-2648).
    crate::execution::checks_turn_end::cancel_turn_end_checks_for_issue(orch, owning_db, issue_id)
        .await;

    settle_work_left_on_closed_sessions(orch, owning_db, &closed_sessions).await;

    Ok(())
}

/// The postcondition of [`resolve_terminal`]: no session it closed may still
/// have a turn in flight.
///
/// [`stop_live_work`] is the mechanism and it works forwards, from the `jobs`
/// table; this is the check and it works backwards, from the sessions that were
/// actually closed. That closes the two gaps enumeration cannot: a job whose
/// stop failed, and a turn that began between the enumeration and the close.
/// A closed session whose runs outlive it is the state CAIRN-3253 makes
/// unreachable, so anything found here goes through the same canonical stop and
/// has its routed batches cancelled.
async fn settle_work_left_on_closed_sessions(
    orch: &Orchestrator,
    db: &LocalDb,
    closed_sessions: &[String],
) {
    for job_id in jobs_with_live_turns(db, closed_sessions).await {
        log::warn!(
            "Job {job_id} still held a turn in flight after a terminal resolution closed its session; stopping it now"
        );
        if let Err(error) = crate::orchestrator::lifecycle::stop_job(orch, &job_id) {
            log::warn!("Failed to stop job {job_id} after its session closed: {error}");
        }
        // The run-scoped branch of `stop_job` cancels the executor's in-flight
        // batches itself; the branch for a job with no live run to attach to does
        // not, so cancel explicitly here as well. Both are idempotent.
        let cancelled = orch.fleet.cancel_job_requests(&job_id);
        if cancelled > 0 {
            log::info!(
                "Cancelled {cancelled} in-flight batch(es) for job {job_id} after its session closed"
            );
        }
    }
}

/// Jobs holding a live (`pending`/`running`/`yielded`) turn on any of
/// `session_ids`. A read failure logs and contributes nothing: this is a
/// postcondition sweep, and failing to run it must not fail a resolution that
/// has already been recorded.
async fn jobs_with_live_turns(db: &LocalDb, session_ids: &[String]) -> Vec<String> {
    let mut job_ids: Vec<String> = Vec::new();
    for session_id in session_ids {
        let found = db
            .read(|conn| {
                let session_id = session_id.clone();
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT DISTINCT job_id
                             FROM turns
                             WHERE session_id = ?1
                               AND job_id IS NOT NULL
                               AND state IN ('pending', 'running', 'yielded')",
                            params![session_id.as_str()],
                        )
                        .await?;
                    let mut ids = Vec::new();
                    while let Some(row) = rows.next().await? {
                        ids.push(row.text(0)?);
                    }
                    Ok(ids)
                })
            })
            .await;
        match found {
            Ok(ids) => {
                for id in ids {
                    if !job_ids.contains(&id) {
                        job_ids.push(id);
                    }
                }
            }
            Err(error) => {
                log::warn!("Failed to check for live turns on closed session {session_id}: {error}")
            }
        }
    }
    job_ids
}

/// The `(project_key, issue_number)` of an issue's OPEN merge request, or `None`
/// when it has no unresolved PR. Used to refuse an agent `status:"merged"` and
/// point it at the real merge lever. Reads `orch.db.local`, where all
/// `merge_requests` access lives.
async fn open_merge_request_for_issue(
    orch: &Orchestrator,
    issue_id: &str,
) -> Result<Option<(String, i32)>, String> {
    let issue_id = issue_id.to_string();
    orch.db
        .local
        .read(|conn| {
            let issue_id = issue_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT p.key, i.number
                         FROM merge_requests mr
                         JOIN issues i ON mr.issue_id = i.id
                         JOIN projects p ON i.project_id = p.id
                         WHERE mr.issue_id = ?1
                           AND mr.status NOT IN ('merged', 'closed')
                         LIMIT 1",
                        params![issue_id.as_str()],
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => Ok(Some((row.text(0)?, row.i64(1)? as i32))),
                    None => Ok(None),
                }
            })
        })
        .await
        .map_err(|e| format!("Failed to check for an open merge request: {e}"))
}

/// Stop everything still live on an issue heading into a terminal state.
///
/// Started work goes through the canonical node stop
/// ([`crate::orchestrator::lifecycle::stop_job`]) — the same path the Stop
/// button takes, which parks the process warm, leaves the session resumable,
/// and cascades to child runs. Work that never started (a `pending`/`blocked`
/// DAG entry) is cancelled exactly as a removed snapshot node is archived: the
/// row flips to the sticky `cancelled` status with its transcript intact, in one
/// transaction so the queued set moves together or not at all.
///
/// This fails closed. Any job that cannot be stopped — a stop that errors, a
/// cancel transaction that fails, or a queued row that did not actually reach
/// `cancelled` — refuses the resolution, so a transient storage or interrupt
/// failure can never leave live work running against a closed issue. Both steps
/// are idempotent (stopping a stopped job and cancelling a cancelled one are
/// no-ops), so the caller can simply send the confirmed write again.
///
/// The cancel is verified by re-reading the rows; the stop deliberately is not.
/// A warm park leaves the job claimed and its process warm on purpose, so there
/// is no row-level post-condition to assert without contradicting the stop
/// semantics this reuses.
async fn stop_live_work(
    orch: &Orchestrator,
    issue_id: &str,
    live_work: &[LiveJob],
) -> Result<(), ResolutionRefusal> {
    let (started, queued): (Vec<&LiveJob>, Vec<&LiveJob>) =
        live_work.iter().partition(|job| job.is_started());
    let mut failures: Vec<String> = Vec::new();

    for job in started {
        if let Err(error) = crate::orchestrator::lifecycle::stop_job(orch, &job.id) {
            log::warn!(
                "Failed to stop job {} for terminal issue {issue_id}: {error}",
                job.id
            );
            failures.push(format!("{} could not be stopped ({error})", job.name));
        }
    }

    if !queued.is_empty() {
        let job_ids: Vec<String> = queued.iter().map(|job| job.id.clone()).collect();
        let now = chrono::Utc::now().timestamp();
        let cancelled = orch
            .db
            .local
            .write(|conn| {
                let job_ids = job_ids.clone();
                Box::pin(async move {
                    for job_id in &job_ids {
                        crate::execution::advancement::cancel_job_conn(conn, job_id, now).await?;
                    }
                    Ok(())
                })
            })
            .await;
        match cancelled {
            Err(error) => {
                log::warn!("Failed to cancel queued jobs for terminal issue {issue_id}: {error}");
                for job in &queued {
                    failures.push(format!("{} could not be cancelled ({error})", job.name));
                }
            }
            Ok(()) => {
                // The write reporting success is not the same as the rows having
                // moved: a job that vanished between the enumeration and the
                // cancel updates zero rows and reports Ok.
                for job in &queued {
                    if !job_is_cancelled(orch, &job.id).await {
                        failures.push(format!("{} did not reach a cancelled state", job.name));
                    }
                }
            }
        }
    }

    if failures.is_empty() {
        return Ok(());
    }
    Err(ResolutionRefusal::Rejected(format!(
        "This issue was left unresolved because its live work could not be stopped: {}. Nothing was resolved and nothing was torn down; send the same write again once that clears.",
        failures.join("; ")
    )))
}

/// The refusal for a status a caller cannot set by hand — `active`, `waiting`,
/// and the rest are derived from execution state.
fn unsettable_status_refusal(status: &str) -> ResolutionRefusal {
    ResolutionRefusal::Rejected(format!(
        "Cannot manually set status to '{status}' because it is derived from execution state"
    ))
}

/// Whether a job row now reads `cancelled`. A missing row answers `false`: the
/// cancel had nothing to move, which is exactly the case the caller must catch.
async fn job_is_cancelled(orch: &Orchestrator, job_id: &str) -> bool {
    let job_id = job_id.to_string();
    orch.db
        .local
        .read(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT status FROM jobs WHERE id = ?1",
                        params![job_id.as_str()],
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => Ok(row.text(0)? == "cancelled"),
                    None => Ok(false),
                }
            })
        })
        .await
        .unwrap_or(false)
}

/// The work still live on `issue_id`: its own non-terminal jobs plus jobs of
/// other issues based on its branch (reviewers, follow-ups). An empty vec means
/// resolving the issue strands nothing.
///
/// `cancelled` counts as terminal alongside `complete` and `failed` — an
/// archived node is not live work, and counting it kept already-dispositioned
/// issues from resolving.
///
/// Three consumers share this one enumeration: the confirmation refusal in
/// [`update_status`], the work [`stop_live_work`] stops once confirmed, and the
/// recipe merge/close guard in `execution::actions`, which stays a hard refusal
/// so a coordinator never resolves a child issue out from under a reviewer it
/// is about to read. Recipe action nodes are not job rows and the implementation
/// job is `complete` by merge time, so the action performing a resolution never
/// counts itself here.
pub async fn live_work_for_issue(
    orch: &Orchestrator,
    issue_id: &str,
) -> Result<Vec<LiveJob>, String> {
    let issue_id = issue_id.to_string();
    orch.db
        .local
        .read(|conn| {
            let issue_id = issue_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT j.id,
                                COALESCE(NULLIF(j.node_name, ''), NULLIF(j.agent_config_id, ''), 'agent work'),
                                j.status,
                                CASE WHEN j.issue_id = ?1 THEN 0 ELSE 1 END AS from_dependent
                         FROM jobs j
                         WHERE j.status NOT IN ('complete', 'failed', 'cancelled')
                           AND (
                               j.issue_id = ?1
                               OR (
                                   j.issue_id IS NOT NULL
                                   AND j.issue_id <> ?1
                                   AND j.base_branch IN (
                                       SELECT DISTINCT parent.branch
                                       FROM jobs parent
                                       WHERE parent.issue_id = ?1
                                         AND parent.branch IS NOT NULL
                                   )
                               )
                           )
                         ORDER BY from_dependent, j.created_at",
                        params![issue_id.as_str()],
                    )
                    .await?;
                let mut live_work = Vec::new();
                while let Some(row) = rows.next().await? {
                    live_work.push(LiveJob {
                        id: row.text(0)?,
                        name: row.text(1)?,
                        status: row.text(2)?,
                        from_dependent_issue: row.i64(3)? == 1,
                    });
                }
                Ok(live_work)
            })
        })
        .await
        .map_err(|e| format!("Failed to check live work for issue: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::SearchIndex;
    use std::sync::Arc;

    /// An orchestrator over a migrated database with one project, one issue, and
    /// one queued job on it.
    async fn orch_with_queued_job() -> Orchestrator {
        let db = crate::storage::migrated_test_db("issue-status-stop.db").await;
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w', 'W', 1, 1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES('p-1', 'w', 'Proj', 'proj', '/tmp/proj', 1, 1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
             VALUES('issue-1', 'p-1', 1, 'Issue', 'active', 'idle', 'none', 1, 1);
            INSERT INTO jobs(id, project_id, issue_id, node_name, status, created_at, updated_at)
             VALUES('job-queued', 'p-1', 'issue-1', 'reviewer', 'pending', 1, 1);
            ",
        )
        .await
        .unwrap();

        let search =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        OrchestratorBuilder::new(
            Arc::new(DbState::new(Arc::new(db), search)),
            Arc::new(TestServicesBuilder::new().build()),
            tempfile::tempdir().unwrap().keep(),
        )
        .build()
    }

    fn queued(id: &str, name: &str) -> LiveJob {
        LiveJob {
            id: id.to_string(),
            name: name.to_string(),
            status: "pending".to_string(),
            from_dependent_issue: false,
        }
    }

    /// The preflight is total over statuses, not just over resolutions. A caller
    /// that checks before writing must get the same refusal it would get later,
    /// or a combined `{title, status}` write applies the title and then refuses.
    #[tokio::test]
    async fn an_unsettable_status_is_refused_by_the_preflight() {
        let orch = orch_with_queued_job().await;

        for status in ["active", "waiting", "frobnicate"] {
            let refusal = check_resolution(
                &orch,
                "issue-1",
                status,
                ResolutionActor::Agent,
                Confirmation::Given,
            )
            .await
            .expect_err("a derived status is not settable");
            assert!(
                refusal.to_string().contains("derived from execution state"),
                "{refusal}"
            );
        }

        // Unresolving stays allowed and has no live work to stop.
        assert!(check_resolution(
            &orch,
            "issue-1",
            "backlog",
            ResolutionActor::Agent,
            Confirmation::Absent,
        )
        .await
        .expect("backlog is settable")
        .is_empty());
    }

    #[tokio::test]
    async fn stopping_queued_work_cancels_it() {
        let orch = orch_with_queued_job().await;

        stop_live_work(&orch, "issue-1", &[queued("job-queued", "reviewer")])
            .await
            .expect("a real queued job cancels");

        assert!(job_is_cancelled(&orch, "job-queued").await);
    }

    /// A cancel that moves no rows reports `Ok` from the database. Trusting that
    /// would resolve the issue over work the caller was told had been stopped,
    /// so the rows are re-read and a job that did not move refuses the whole
    /// resolution.
    #[tokio::test]
    async fn work_that_does_not_reach_cancelled_refuses_the_resolution() {
        let orch = orch_with_queued_job().await;

        let refusal = stop_live_work(
            &orch,
            "issue-1",
            &[
                queued("job-queued", "reviewer"),
                queued("job-gone", "planner"),
            ],
        )
        .await
        .expect_err("work that did not stop must refuse");

        let text = refusal.to_string();
        assert!(
            text.contains("planner"),
            "names the work that did not stop: {text}"
        );
        assert!(
            text.contains("left unresolved"),
            "says the issue was not resolved: {text}"
        );
    }

    /// The refusal is the resolution's gate, not a report after the fact: a
    /// confirmed close whose work cannot be stopped leaves the issue open.
    #[tokio::test]
    async fn a_stop_failure_leaves_the_issue_unresolved() {
        let orch = orch_with_queued_job().await;
        // Enumerate first, then delete the row out from under the cancel — the
        // race the verification exists to catch.
        let live_work = live_work_for_issue(&orch, "issue-1").await.unwrap();
        assert_eq!(live_work.len(), 1);
        orch.db
            .local
            .execute("DELETE FROM jobs WHERE id = 'job-queued'", ())
            .await
            .unwrap();

        let refusal = stop_live_work(&orch, "issue-1", &live_work)
            .await
            .expect_err("a vanished job cannot be confirmed stopped");
        assert!(matches!(refusal, ResolutionRefusal::Rejected(_)));

        let status = orch
            .db
            .local
            .query_opt_text(
                "SELECT status FROM issues WHERE id = ?1",
                params!["issue-1"],
            )
            .await
            .unwrap();
        assert_eq!(
            status.as_deref(),
            Some("active"),
            "the issue stays open when its work could not be stopped"
        );
    }
}
