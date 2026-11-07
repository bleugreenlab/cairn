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
                "Refusing to mark {project_key}/{number} merged: it still has an OPEN pull request. Setting status=merged records a resolution WITHOUT merging the PR — it strands the branch's commits and auto-closes the PR unmerged. Merge through the PR instead:\n  write({{changes:[{{target:\"cairn://p/{project_key}/{number}/1/builder/create-pr\",mode:\"patch\",payload:{{action:\"merge\"}}}}]}})\nThat merge resolves this issue for you. If the PR was already merged on GitHub, refresh it instead with payload:{{action:\"refresh\"}}: a refresh that finds GitHub's merge records it and resolves this issue as merged."
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
        "backlog" => {
            crud::unresolve(&owning_db, &*orch.services.clock, id)
                .await
                .map_err(|e| e.to_string())?;
            // Reopening lifts resolution-cancelled jobs; let job and execution
            // views refetch.
            let mut changes =
                crate::notify::job_db_changes_for_issue(&owning_db, id, "update").await;
            changes.extend(
                crate::notify::execution_db_changes_for_issue(&owning_db, id, "update").await,
            );
            for change in changes {
                let _ = orch.services.emitter.emit("db-change", change);
            }
        }
        // Unreachable in practice: `check_resolution` above refuses an
        // unsettable status before anything is written. Kept as the exhaustive
        // arm, sharing the one message so the two can never disagree.
        _ => return Err(unsettable_status_refusal(status)),
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

    // A terminal resolution was announced by `resolve_terminal` on the edge it
    // crossed; waking here as well would emit the same `Resolved` fact twice.
    if !is_terminal_state {
        orch.wake_for_issue(id).await;
    }

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
/// 6. Re-derive every job's projection, so each settles to the outcome it
///    recorded (Complete or Failed) or to Cancelled — never Running, Pending,
///    Idle, or Blocked on a resolved issue.
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
        if let Err(refusal) = stop_live_work(orch, issue_id, &live_work, resolution).await {
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

    // Recording the resolution revokes publication to the issue's branches, so
    // it is recorded under the project store's lock: every seal and bookmark
    // advance asks `mcp::handlers::branch::publication_authority` under that
    // lock, and a write either finishes before this edge or is refused after
    // it. Only the row write sits inside the hold; stopping processes above and
    // teardown afterwards never do.
    let revocation = match revocation_store_lock(orch, owning_db, issue_id).await {
        Ok(revocation) => revocation,
        // Not recorded: an unordered resolution is the state this hold exists
        // to prevent. The resolution's own retries (the confirmed write sent
        // again, the next PR refresh observing the merge) record it once the
        // project can be read.
        Err(error) => {
            return Err(ResolutionRefusal::Rejected(format!(
                "The resolution was not recorded because its project could not be read to order it \
                 against in-flight commits ({error}). Send the same write again."
            )))
        }
    };
    let resolved = crud::resolve(
        owning_db,
        &*orch.services.clock,
        issue_id,
        resolution,
        &orch.anon_device_manager.device_id(),
    )
    .await;
    drop(revocation);
    let crud::ResolveOutcome {
        closed_sessions,
        became_terminal,
    } = resolved.map_err(|e| ResolutionRefusal::Rejected(e.to_string()))?;

    crate::orchestrator::lifecycle::stop_processes_for_sessions(orch, &closed_sessions);

    // The issue is resolved — quit any in-flight turn-end review suite so it does
    // not keep running against a merged/closed issue (CAIRN-2648).
    crate::execution::checks_turn_end::cancel_turn_end_checks_for_issue(orch, owning_db, issue_id)
        .await;

    settle_work_left_on_closed_sessions(orch, owning_db, &closed_sessions).await;

    settle_job_projection_for_issue(orch, owning_db, issue_id).await;

    if became_terminal {
        announce_resolution(orch, owning_db, issue_id).await;
    }

    // Every terminal resolution comes through here — the status patch, a PR
    // merge or close, the recipe action — so this is where `issue_resolution`
    // and `pr_state` gates hear about it.
    crate::execution::gates::sweep::notify_gate_evidence_changed();
    orch.notify_tenet_evidence();

    Ok(())
}

/// How many times the project lookup behind the revocation hold is tried
/// before the resolution is refused unrecorded.
const REVOCATION_LOOKUP_ATTEMPTS: u32 = 3;

/// The store-lock hold that orders a resolution against publication.
///
/// `Ok(None)` only when a successful lookup proves the issue's project has no
/// local repository, so no store this runner publishes to. A lookup that
/// fails proves nothing: it is retried, then returned as an error, and the
/// caller records nothing. Otherwise this waits for the lock however long a
/// publication holds it, or finds this task already holding it.
async fn revocation_store_lock(
    orch: &Orchestrator,
    db: &LocalDb,
    issue_id: &str,
) -> Result<Option<crate::orchestrator::EdgeHold>, String> {
    revocation_store_lock_with(orch, issue_id, move || async move {
        db.query_opt_text(
            "SELECT p.repo_path FROM issues i JOIN projects p ON p.id = i.project_id
              WHERE i.id = ?1",
            params![issue_id],
        )
        .await
        .map_err(|error| error.to_string())
    })
    .await
}

async fn revocation_store_lock_with<F, Fut>(
    orch: &Orchestrator,
    issue_id: &str,
    lookup: F,
) -> Result<Option<crate::orchestrator::EdgeHold>, String>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<Option<String>, String>>,
{
    let mut attempt = 1;
    let repo_path = loop {
        match lookup().await {
            Ok(path) => break path,
            Err(error) if attempt >= REVOCATION_LOOKUP_ATTEMPTS => {
                return Err(format!(
                    "looking up the project of issue {issue_id}: {error}"
                ))
            }
            Err(error) => {
                log::warn!(
                    "Terminal resolution of issue {issue_id}: project lookup failed (attempt \
                     {attempt}), retrying: {error}"
                );
                tokio::time::sleep(std::time::Duration::from_millis(50 * u64::from(attempt))).await;
                attempt += 1;
            }
        }
    };
    let Some(repo_path) = repo_path.filter(|path| !path.trim().is_empty()) else {
        return Ok(None);
    };
    let store = crate::jj::project_store_dir(&orch.config_dir, std::path::Path::new(&repo_path));
    Ok(Some(
        orch.acquire_jj_store_lock_for_edge(
            &store,
            format!("terminal resolution of issue {issue_id}"),
        )
        .await,
    ))
}

/// Announce the resolution [`resolve_terminal`] just recorded to the issue's
/// watchers — its parent coordinator among them.
///
/// The recompute sweep announces only the terminal edges it crosses itself, so
/// that a sweep over an old merged issue never replays the merge. A resolution
/// through this door crosses the edge in `crud::resolve`, before any recompute
/// runs, which leaves the sweep nothing to announce: the door that crossed the
/// edge has to. Runs after the job projection settles, so a watcher roused by
/// it reads the issue's work already at rest.
async fn announce_resolution(orch: &Orchestrator, db: &LocalDb, issue_id: &str) {
    let ctx = match crate::orchestrator::attention::read_issue_for_attention(db, issue_id).await {
        Ok(ctx) => ctx,
        Err(error) => {
            log::warn!("Could not announce the resolution of issue {issue_id}: {error}");
            return;
        }
    };
    // Reopened in between: there is no resolution left to announce.
    if !ctx.status.is_terminal() {
        return;
    }
    orch.emit_attention_event(crate::orchestrator::AttentionEvent {
        issue_id: issue_id.to_string(),
        issue_uri: ctx.issue_uri(),
        fact: crate::orchestrator::AttentionFact::Resolved {
            final_status: ctx.status.clone(),
        },
        attention: ctx.attention,
        status: ctx.status,
        updated_at: ctx.updated_at,
        route_provenance: None,
    });
}

/// Re-derive every job on a just-resolved issue, so none rests Running,
/// Pending, Idle, or Blocked on it.
///
/// The stop above cancels what was live, but a job that was neither live nor
/// queued — a builder parked Complete, a planner Blocked at its gate, a
/// long-running coordinator Idle — still holds whatever status it had. This is
/// the same recompute every other fact site uses, not a direct write: on a
/// resolved issue it settles each job to its recorded outcome or Cancelled,
/// quietly (`ChangeCause::IssueResolved`). Failures are logged and do not
/// refuse, like the postcondition sweep: the resolution has been recorded, and
/// the boot repair settles whatever this misses.
async fn settle_job_projection_for_issue(orch: &Orchestrator, db: &LocalDb, issue_id: &str) {
    let found = db
        .read(|conn| {
            let issue_id = issue_id.to_string();
            Box::pin(async move {
                let mut executions = Vec::new();
                let mut rows = conn
                    .query(
                        "SELECT id FROM executions WHERE issue_id = ?1",
                        params![issue_id.as_str()],
                    )
                    .await?;
                while let Some(row) = rows.next().await? {
                    executions.push(row.text(0)?);
                }
                drop(rows);
                let mut execution_less = Vec::new();
                let mut rows = conn
                    .query(
                        "SELECT id FROM jobs
                         WHERE issue_id = ?1 AND execution_id IS NULL AND thread_id IS NULL",
                        params![issue_id.as_str()],
                    )
                    .await?;
                while let Some(row) = rows.next().await? {
                    execution_less.push(row.text(0)?);
                }
                Ok((executions, execution_less))
            })
        })
        .await;
    let (executions, execution_less) = match found {
        Ok(found) => found,
        Err(error) => {
            log::warn!("Failed to list jobs to settle on resolved issue {issue_id}: {error}");
            return;
        }
    };
    for execution_id in &executions {
        if let Err(error) =
            crate::execution::advancement::recompute_execution_jobs(orch, execution_id)
        {
            log::warn!(
                "Failed to settle jobs of execution {execution_id} on resolved issue {issue_id}: {error}"
            );
        }
    }
    for job_id in &execution_less {
        if let Err(error) = crate::execution::advancement::recompute_job(orch, job_id) {
            log::warn!("Failed to settle job {job_id} on resolved issue {issue_id}: {error}");
        }
    }
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
        let cancelled = orch
            .fleet
            .cancel_job_requests(&job_id, crate::fleet::CancelCause::SessionClosed);
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
/// and cascades to child runs, then is archived because a terminal issue cannot
/// resume it. Work that never started (a `pending`/`blocked`
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
/// Archival is verified by re-reading every row. A terminal resolution first
/// uses warm stop as the process-level interrupt mechanism, then deliberately
/// replaces its resumable row state with the sticky cancelled state.
///
/// Every archived job's sessions close with `resolution`'s reason, the jobs of
/// dependent issues included: the resolution is what ended their work, and the
/// reason is what tells the operator that reopening the issue picks it back up.
async fn stop_live_work(
    orch: &Orchestrator,
    issue_id: &str,
    live_work: &[LiveJob],
    resolution: Resolution,
) -> Result<(), ResolutionRefusal> {
    stop_live_work_with(orch, issue_id, live_work, resolution, |orch, job_id| {
        crate::orchestrator::lifecycle::stop_job(orch, job_id)
    })
    .await
}

async fn stop_live_work_with(
    orch: &Orchestrator,
    issue_id: &str,
    live_work: &[LiveJob],
    resolution: Resolution,
    stop_started: impl Fn(&Orchestrator, &str) -> Result<(), String>,
) -> Result<(), ResolutionRefusal> {
    let (started, queued): (Vec<&LiveJob>, Vec<&LiveJob>) =
        live_work.iter().partition(|job| job.is_started());
    let mut failures: Vec<String> = Vec::new();
    let mut stopped: Vec<&LiveJob> = Vec::new();

    for job in &started {
        match stop_started(orch, &job.id) {
            Ok(()) => stopped.push(job),
            Err(error) => {
                log::warn!(
                    "Failed to stop job {} for terminal issue {issue_id}: {error}",
                    job.id
                );
                failures.push(format!("{} could not be stopped ({error})", job.name));
            }
        }
    }

    // A normal Stop parks a job warm and leaves its row running/idle so it can
    // resume. That is wrong at this lifecycle boundary: merged and closed issues
    // cannot resume work. Once the process is stopped, archive every job in one
    // transaction so no Builder remains falsely running on a terminal issue.
    let jobs_to_cancel: Vec<&LiveJob> = stopped.into_iter().chain(queued).collect();
    if !jobs_to_cancel.is_empty() {
        let job_ids: Vec<String> = jobs_to_cancel.iter().map(|job| job.id.clone()).collect();
        let now = chrono::Utc::now().timestamp();
        let cancelled = orch
            .db
            .local
            .write(|conn| {
                let job_ids = job_ids.clone();
                Box::pin(async move {
                    for job_id in &job_ids {
                        crate::execution::advancement::cancel_job_conn(
                            conn,
                            job_id,
                            now,
                            resolution.into(),
                        )
                        .await?;
                    }
                    Ok(())
                })
            })
            .await;
        match cancelled {
            Err(error) => {
                log::warn!("Failed to cancel queued jobs for terminal issue {issue_id}: {error}");
                for job in &jobs_to_cancel {
                    failures.push(format!("{} could not be cancelled ({error})", job.name));
                }
            }
            Ok(()) => {
                // The write reporting success is not the same as the rows having
                // moved: a job that vanished between the enumeration and the
                // cancel updates zero rows and reports Ok.
                for job in &jobs_to_cancel {
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
        "This issue was left unresolved because its live work could not be stopped: {}. Nothing was resolved; work that stopped successfully may already be archived. Send the same write again once the remaining work can stop.",
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

    /// The ordering the publication authority depends on. A publisher holds the
    /// project store lock and has passed its authority check; a resolution that
    /// arrives now must queue behind it rather than record itself, and the
    /// publisher's next check after the release is refused. No timeout lets the
    /// resolution through while the hold lasts.
    #[tokio::test]
    async fn a_resolution_waits_for_a_publication_holding_the_store_lock() {
        use crate::mcp::handlers::branch::{publication_authority_in, PublicationAuthority};
        let orch = orch_with_queued_job().await;
        let store =
            crate::jj::project_store_dir(&orch.config_dir, std::path::Path::new("/tmp/proj"));
        let publisher = orch
            .acquire_jj_store_lock(
                &store,
                crate::orchestrator::StoreLockClass::Foreground,
                "publisher past its authority check",
            )
            .await;
        assert_eq!(
            publication_authority_in(&orch.db.local, "job-queued", None, None).await,
            PublicationAuthority::Authorized
        );

        let resolver = orch.clone();
        let resolving = tokio::spawn(async move {
            let db = resolver.db.local.clone();
            resolve_terminal(
                &resolver,
                &db,
                "issue-1",
                Resolution::Merged,
                StopFailure::Escalates,
            )
            .await
        });
        let mut queued = false;
        for _ in 0..10_000 {
            tokio::task::yield_now().await;
            queued = orch.store_lock_status(&store).is_some_and(|status| {
                status
                    .waiters
                    .iter()
                    .any(|waiter| waiter.operation.starts_with("terminal resolution"))
            });
            if queued {
                break;
            }
        }
        assert!(queued, "the resolution queues on the store lock");
        let status = orch
            .db
            .local
            .query_opt_text("SELECT status FROM issues WHERE id = 'issue-1'", ())
            .await
            .unwrap();
        assert_ne!(
            status.as_deref(),
            Some("merged"),
            "nothing is recorded while the publication holds the lock"
        );
        assert!(!resolving.is_finished());

        drop(publisher);
        resolving.await.unwrap().unwrap();
        assert!(matches!(
            publication_authority_in(&orch.db.local, "job-queued", None, None).await,
            PublicationAuthority::Retired(_)
        ));
    }

    /// A failed project lookup is not evidence that there is no store: it is
    /// retried, and only a lookup that keeps failing refuses, before anything is
    /// recorded. A lookup that fails once and then succeeds takes the hold.
    #[tokio::test]
    async fn a_failed_project_lookup_never_lets_a_resolution_through_unordered() {
        let orch = orch_with_queued_job().await;
        let counter = std::sync::atomic::AtomicU32::new(0);
        let calls = &counter;
        let flaky = move || async move {
            if calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                Err("database is busy".to_string())
            } else {
                Ok(Some("/tmp/proj".to_string()))
            }
        };
        let hold = revocation_store_lock_with(&orch, "issue-1", flaky)
            .await
            .expect("the retry reads the project");
        assert!(matches!(
            hold,
            Some(crate::orchestrator::EdgeHold::Acquired(_))
        ));
        drop(hold);
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);

        let failing = || async { Err::<Option<String>, _>("database is busy".to_string()) };
        let error = revocation_store_lock_with(&orch, "issue-1", failing)
            .await
            .err()
            .expect("a lookup that keeps failing is an error, not 'no store'");
        assert!(error.contains("database is busy"), "{error}");

        let absent = || async { Ok::<_, String>(None) };
        assert!(revocation_store_lock_with(&orch, "issue-1", absent)
            .await
            .unwrap()
            .is_none());
    }

    /// A reopen lifts the work a resolution stopped and continues it in a new
    /// session on the same job, so the job and issue read live again. A delta
    /// the old session prepared before the resolution must still be refused;
    /// the successor session's own work is not.
    #[tokio::test]
    async fn a_reopen_does_not_reauthorize_the_session_the_resolution_closed() {
        use crate::mcp::handlers::branch::{
            publication_authority_in, PublicationAuthority, RetirementCause,
        };
        let orch = orch_with_queued_job().await;
        let db = orch.db.local.clone();
        db.execute_script(
            "
            INSERT INTO jobs(id, project_id, issue_id, node_name, status, branch, current_session_id, created_at, updated_at)
             VALUES('job-live', 'p-1', 'issue-1', 'builder', 'complete', 'agent/proj-1-builder-0', 's-old', 1, 1);
            INSERT INTO sessions(id, job_id, backend, status, created_at, updated_at)
             VALUES('s-old', 'job-live', 'claude', 'open', 1, 1);
            INSERT INTO runs(id, issue_id, project_id, job_id, status, session_id, created_at, updated_at)
             VALUES('r-old', 'issue-1', 'p-1', 'job-live', 'live', 's-old', 1, 1);
            ",
        )
        .await
        .unwrap();

        crud::resolve(
            &db,
            &*orch.services.clock,
            "issue-1",
            Resolution::Merged,
            "device",
        )
        .await
        .unwrap();
        crud::unresolve(&db, &*orch.services.clock, "issue-1")
            .await
            .unwrap();

        let PublicationAuthority::Retired(retired) =
            publication_authority_in(&db, "job-live", Some("r-old"), None).await
        else {
            panic!("the session the resolution closed is still refused after the reopen");
        };
        assert_eq!(retired.cause, RetirementCause::SessionClosed);
        assert_eq!(retired.branch, "agent/proj-1-builder-0");

        let successor = db
            .query_opt_text(
                "SELECT current_session_id FROM jobs WHERE id = 'job-live'",
                (),
            )
            .await
            .unwrap()
            .expect("the reopen continues the job in a session");
        assert_ne!(successor, "s-old");
        db.execute(
            "INSERT INTO runs(id, issue_id, project_id, job_id, status, session_id, created_at, updated_at)
             VALUES('r-new', 'issue-1', 'p-1', 'job-live', 'live', ?1, 2, 2)",
            (successor.as_str(),),
        )
        .await
        .unwrap();
        assert_eq!(
            publication_authority_in(&db, "job-live", Some("r-new"), None).await,
            PublicationAuthority::Authorized,
            "the reopened issue's own work publishes"
        );
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

        stop_live_work(
            &orch,
            "issue-1",
            &[queued("job-queued", "reviewer")],
            Resolution::Closed,
        )
        .await
        .expect("a real queued job cancels");

        assert!(job_is_cancelled(&orch, "job-queued").await);
    }

    #[tokio::test]
    async fn a_started_job_is_not_archived_when_its_stop_fails() {
        let orch = orch_with_queued_job().await;
        orch.db
            .local
            .execute(
                "INSERT INTO jobs(id, project_id, issue_id, node_name, status, created_at, updated_at)
                 VALUES('job-running', 'p-1', 'issue-1', 'builder', 'running', 2, 2)",
                (),
            )
            .await
            .unwrap();
        let running = LiveJob {
            id: "job-running".into(),
            name: "builder".into(),
            status: "running".into(),
            from_dependent_issue: false,
        };

        let refusal =
            stop_live_work_with(&orch, "issue-1", &[running], Resolution::Closed, |_, _| {
                Err("interrupt unavailable".into())
            })
            .await
            .expect_err("failed process interruption refuses terminal resolution");

        assert!(refusal.to_string().contains("builder"));
        assert_eq!(
            orch.db
                .local
                .query_opt_text("SELECT status FROM jobs WHERE id = 'job-running'", ())
                .await
                .unwrap()
                .as_deref(),
            Some("running"),
            "a process that may still be running keeps a live job row"
        );
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
            Resolution::Closed,
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

        let refusal = stop_live_work(&orch, "issue-1", &live_work, Resolution::Closed)
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

/// Resolution settles every job's projection, and reopening lifts exactly the
/// jobs the resolution cancelled.
#[cfg(test)]
mod projection_tests {
    use super::*;
    use crate::db::DbState;
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::SearchIndex;
    use std::sync::Arc;

    fn snapshot_json() -> String {
        serde_json::json!({
            "recipe": {
                "id": "r", "name": "R", "description": null, "trigger": "manual",
                "nodes": [
                    {"id": "builder", "nodeType": "agent", "name": "Builder", "position": {"x": 0.0, "y": 0.0}},
                    {"id": "planner", "nodeType": "agent", "name": "Planner", "position": {"x": 0.0, "y": 0.0}},
                    {"id": "coordinator", "nodeType": "agent", "name": "Coordinator", "position": {"x": 0.0, "y": 0.0}},
                    {"id": "followup", "nodeType": "agent", "name": "Followup", "position": {"x": 0.0, "y": 0.0}},
                    {"id": "qa", "nodeType": "agent", "name": "QA", "position": {"x": 0.0, "y": 0.0}},
                    {"id": "reviewed", "nodeType": "agent", "name": "Reviewed", "position": {"x": 0.0, "y": 0.0}},
                    {"id": "pr", "nodeType": "pr", "name": "PR", "position": {"x": 1.0, "y": 0.0}}
                ],
                "edges": []
            },
            "agents": {}, "skills": {},
            "triggerContext": {"issueId": "issue-t", "projectId": "p-t", "triggerType": "manual"},
            "delegatedPackets": [], "createdAt": 1
        })
        .to_string()
    }

    /// An active issue whose canonical execution holds:
    /// - an interrupted builder (no outcome turn);
    /// - a planner cached complete;
    /// - an idle coordinator whose work turn completed, with no unmet contract;
    /// - a job running a follow-up turn after an earlier completed one;
    /// - the builder's first attempt, archived by a restart (its node remains,
    ///   beside the newer builder job) after a completed turn;
    /// - a job archived by removing its node;
    /// - a failed job;
    /// - a complete job whose memory review was sent and whose review turn was
    ///   cancelled, so no finalizer will close its session;
    ///
    /// plus a superseded earlier execution whose builder the supersession froze.
    async fn orch_with_execution() -> Orchestrator {
        let db = crate::storage::migrated_test_db("issue-status-projection.db").await;
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w', 'W', 1, 1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES('p-t', 'w', 'Proj', 'projt', '/tmp/projt', 1, 1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
             VALUES('issue-t', 'p-t', 1, 'Issue', 'active', 'active', 'none', 1, 1);
            ",
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq, lifecycle, snapshot)
             VALUES ('e-old', 'r', 'issue-t', 'p-t', 'cancelled', 1, 1, 'superseded', ?1),
                    ('e-t', 'r', 'issue-t', 'p-t', 'running', 2, 2, 'canonical', ?1)",
            params![snapshot_json()],
        )
        .await
        .unwrap();
        db.execute_script(
            "
            INSERT INTO jobs(id, execution_id, recipe_node_id, issue_id, project_id, node_name, uri_segment, status, current_session_id, created_at, updated_at)
             VALUES('builder', 'e-t', 'builder', 'issue-t', 'p-t', 'builder', 'builder', 'running', 's-builder', 1, 1),
                   ('planner', 'e-t', 'planner', 'issue-t', 'p-t', 'planner', 'planner', 'complete', 's-planner', 1, 1),
                   ('coordinator', 'e-t', 'coordinator', 'issue-t', 'p-t', 'coordinator', 'coordinator', 'idle', 's-coordinator', 1, 1),
                   ('followup', 'e-t', 'followup', 'issue-t', 'p-t', 'followup', 'followup', 'running', 's-followup', 1, 1),
                   ('builder-attempt-1', 'e-t', 'builder', 'issue-t', 'p-t', 'builder', 'builder-1', 'cancelled', 's-attempt-1', 0, 0),
                   ('archived', 'e-t', 'removed', 'issue-t', 'p-t', 'removed', 'removed', 'cancelled', NULL, 1, 1),
                   ('old-builder', 'e-old', 'builder', 'issue-t', 'p-t', 'builder', 'builder', 'cancelled', NULL, 1, 1),
                   ('qa', 'e-t', 'qa', 'issue-t', 'p-t', 'qa', 'qa', 'failed', 's-qa', 1, 1);
            INSERT INTO jobs(id, execution_id, recipe_node_id, issue_id, project_id, node_name, uri_segment, status, current_session_id, memory_review_state, created_at, updated_at)
             VALUES('reviewed', 'e-t', 'reviewed', 'issue-t', 'p-t', 'reviewed', 'reviewed', 'complete', 's-reviewed', 'sent', 1, 1);
            INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
             VALUES('s-builder', 'builder', 'claude', 'open', 1, 1, 1),
                   ('s-planner', 'planner', 'claude', 'open', 1, 1, 1),
                   ('s-coordinator', 'coordinator', 'claude', 'open', 1, 1, 1),
                   ('s-followup', 'followup', 'claude', 'open', 1, 1, 1),
                   ('s-attempt-1', 'builder-attempt-1', 'claude', 'closed', 1, 0, 0),
                   ('s-qa', 'qa', 'claude', 'open', 1, 1, 1),
                   ('s-reviewed', 'reviewed', 'claude', 'open', 1, 1, 1);
            INSERT INTO turns(id, session_id, job_id, sequence, state, start_reason, created_at, ended_at, updated_at)
             VALUES('t-builder', 's-builder', 'builder', 1, 'interrupted', 'initial', 1, 2, 2),
                   ('t-planner', 's-planner', 'planner', 1, 'complete', 'initial', 1, 2, 2),
                   ('t-coordinator', 's-coordinator', 'coordinator', 1, 'complete', 'initial', 1, 2, 2),
                   ('t-followup-1', 's-followup', 'followup', 1, 'complete', 'initial', 1, 2, 2),
                   ('t-attempt-1', 's-attempt-1', 'builder-attempt-1', 1, 'complete', 'initial', 0, 0, 0),
                   ('t-qa', 's-qa', 'qa', 1, 'failed', 'initial', 1, 2, 2),
                   ('t-reviewed', 's-reviewed', 'reviewed', 1, 'complete', 'initial', 1, 2, 2),
                   ('t-reviewed-review', 's-reviewed', 'reviewed', 2, 'cancelled', 'memory_review', 3, 4, 4);
            INSERT INTO turns(id, session_id, job_id, sequence, state, start_reason, created_at, updated_at)
             VALUES('t-followup-2', 's-followup', 'followup', 2, 'running', 'follow_up', 3, 3);
            ",
        )
        .await
        .unwrap();
        // The builder's provider conversation, which a reopen's successor forks.
        db.execute(
            "UPDATE sessions SET backend_id = 'native-builder' WHERE id = 's-builder'",
            (),
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

    async fn text(orch: &Orchestrator, sql: &str, id: &str) -> Option<String> {
        orch.db
            .local
            .query_opt_text(sql, params![id])
            .await
            .unwrap()
    }

    async fn unsettled_jobs(orch: &Orchestrator) -> i64 {
        orch.db
            .local
            .query_opt_i64(
                "SELECT COUNT(*) FROM jobs WHERE issue_id = 'issue-t'
                   AND status IN ('running', 'pending', 'idle', 'blocked')",
                (),
            )
            .await
            .unwrap()
            .unwrap_or(0)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_terminal_resolution_settles_every_job() {
        for resolution in [Resolution::Merged, Resolution::Closed] {
            let orch = orch_with_execution().await;
            let db = orch.db.local.clone();
            resolve_terminal(&orch, &db, "issue-t", resolution, StopFailure::Refuses)
                .await
                .expect("the resolution lands");

            assert_eq!(
                text(&orch, "SELECT status FROM jobs WHERE id = ?1", "builder")
                    .await
                    .as_deref(),
                Some("cancelled"),
                "{resolution:?}"
            );
            assert_eq!(
                text(&orch, "SELECT status FROM jobs WHERE id = ?1", "planner")
                    .await
                    .as_deref(),
                Some("complete"),
                "{resolution:?}"
            );
            for (job, expected, why) in [
                ("coordinator", "complete", "an idle coordinator whose work completed settles Complete, not the stop's archive"),
                ("followup", "complete", "a follow-up interrupted by the stop leaves the earlier completion standing"),
                ("builder-attempt-1", "cancelled", "a restart archive is never re-derived"),
            ] {
                assert_eq!(
                    text(&orch, "SELECT status FROM jobs WHERE id = ?1", job)
                        .await
                        .as_deref(),
                    Some(expected),
                    "{resolution:?}: {why}"
                );
            }
            for session in ["s-builder", "s-planner"] {
                assert_eq!(
                    text(&orch, "SELECT status FROM sessions WHERE id = ?1", session)
                        .await
                        .as_deref(),
                    Some("closed"),
                    "{resolution:?} {session}"
                );
            }
            // The resolution closes every job's sessions with its own reason,
            // whatever the job's status, which is what tells the operator that
            // reopening the issue picks the work back up. A sent memory review
            // whose turn has ended holds nothing open.
            let reason = match resolution {
                Resolution::Merged => "issue_merged",
                Resolution::Closed => "issue_closed",
            };
            for session in [
                "s-builder",
                "s-coordinator",
                "s-followup",
                "s-planner",
                "s-qa",
                "s-reviewed",
            ] {
                assert_eq!(
                    text(
                        &orch,
                        "SELECT terminal_reason FROM sessions WHERE id = ?1",
                        session
                    )
                    .await
                    .as_deref(),
                    Some(reason),
                    "{resolution:?} {session}"
                );
                assert!(
                    orch.db
                        .local
                        .query_opt_i64(
                            "SELECT closed_at FROM sessions WHERE id = ?1",
                            params![session],
                        )
                        .await
                        .unwrap()
                        .is_some(),
                    "{resolution:?} {session} records when it closed"
                );
            }
            assert_eq!(unsettled_jobs(&orch).await, 0, "{resolution:?}");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reopening_lifts_only_what_the_resolution_cancelled() {
        let orch = orch_with_execution().await;
        let db = orch.db.local.clone();
        resolve_terminal(
            &orch,
            &db,
            "issue-t",
            Resolution::Merged,
            StopFailure::Refuses,
        )
        .await
        .expect("the resolution lands");
        assert_eq!(
            text(&orch, "SELECT status FROM jobs WHERE id = ?1", "builder")
                .await
                .as_deref(),
            Some("cancelled")
        );

        update_status(
            &orch,
            "issue-t",
            "backlog",
            ResolutionActor::User,
            Confirmation::Absent,
        )
        .await
        .expect("reopening is always allowed");

        assert_eq!(
            text(&orch, "SELECT status FROM jobs WHERE id = ?1", "builder")
                .await
                .as_deref(),
            Some("pending"),
            "an interrupted builder rests resumable again, exactly as on an active issue"
        );
        assert_eq!(
            text(
                &orch,
                "SELECT completed_at FROM jobs WHERE id = ?1",
                "builder"
            )
            .await,
            None,
            "a lifted job is no longer completed"
        );
        assert_eq!(
            text(&orch, "SELECT status FROM jobs WHERE id = ?1", "planner")
                .await
                .as_deref(),
            Some("complete")
        );
        assert_eq!(
            text(&orch, "SELECT status FROM jobs WHERE id = ?1", "archived")
                .await
                .as_deref(),
            Some("cancelled"),
            "a job archived by node removal stays archived"
        );
        assert_eq!(
            text(
                &orch,
                "SELECT status FROM jobs WHERE id = ?1",
                "old-builder"
            )
            .await
            .as_deref(),
            Some("cancelled"),
            "a superseded execution's job stays frozen"
        );
        assert_eq!(
            text(
                &orch,
                "SELECT status FROM jobs WHERE id = ?1",
                "builder-attempt-1"
            )
            .await
            .as_deref(),
            Some("cancelled"),
            "a restart archive stays archived"
        );

        // The lifted builder, the complete planner, the failed job and the
        // reviewed one each continue on a fresh open successor of the session
        // the merge closed; the closed one keeps the merge's reason as history.
        for (job, closed) in [
            ("builder", "s-builder"),
            ("planner", "s-planner"),
            ("qa", "s-qa"),
            ("reviewed", "s-reviewed"),
        ] {
            let current = text(
                &orch,
                "SELECT current_session_id FROM jobs WHERE id = ?1",
                job,
            )
            .await
            .expect("the job points at a session");
            assert_ne!(current, closed, "{job} moved off its closed session");
            assert_eq!(
                text(
                    &orch,
                    "SELECT status || ':' || parent_session_id FROM sessions WHERE id = ?1",
                    &current
                )
                .await
                .as_deref(),
                Some(format!("open:{closed}").as_str()),
                "{job}'s successor is open and descends from the closed session"
            );
            assert_eq!(
                text(
                    &orch,
                    "SELECT status || ':' || terminal_reason || ':' || replaced_by_id
                       FROM sessions WHERE id = ?1",
                    closed
                )
                .await
                .as_deref(),
                Some(format!("closed:issue_merged:{current}").as_str()),
                "{job}'s closed session keeps its reason"
            );
        }
        for (job, session) in [
            ("archived", None),
            ("old-builder", None),
            ("builder-attempt-1", Some("s-attempt-1")),
        ] {
            assert_eq!(
                text(
                    &orch,
                    "SELECT current_session_id FROM jobs WHERE id = ?1",
                    job
                )
                .await
                .as_deref(),
                session,
                "{job} is not continued"
            );
        }

        // The builder's next launch forks the conversation the merge closed,
        // and the planner's session takes a reply.
        let builder_session = text(
            &orch,
            "SELECT current_session_id FROM jobs WHERE id = ?1",
            "builder",
        )
        .await
        .unwrap();
        let (prepared, start, _) =
            crate::execution::jobs::prepare_session_for_test(db.clone(), "builder", 10)
                .await
                .expect("the builder's session prepares");
        assert_eq!(prepared, builder_session);
        assert!(
            matches!(
                &start,
                crate::backends::SessionStart::Fork { session_id, source_backend_id }
                    if session_id == &builder_session && source_backend_id == "native-builder"
            ),
            "{start:?}"
        );
        for job in ["planner", "qa", "reviewed"] {
            assert!(
                !crate::execution::jobs::current_session_refuses_turns(&db, job).await,
                "{job}'s current session takes a turn"
            );
        }
    }

    /// The migration converges the open sessions older builds left on
    /// cancelled jobs, with the reason the job's issue still records, and
    /// leaves every other job's sessions alone.
    #[tokio::test]
    async fn the_migration_closes_the_sessions_of_cancelled_jobs() {
        use crate::storage::{MigrationRunner, TURSO_MIGRATIONS};
        let dir = tempfile::tempdir().unwrap();
        let db = LocalDb::open(dir.path().join("cancelled-sessions.db"))
            .await
            .unwrap();
        let before = TURSO_MIGRATIONS
            .iter()
            .take_while(|migration| migration.name() != "close_sessions_of_cancelled_jobs")
            .copied()
            .collect::<Vec<_>>();
        assert!(
            before.len() < TURSO_MIGRATIONS.len(),
            "the migration must be in the lineage"
        );
        MigrationRunner::new(before).run(&db).await.unwrap();
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w', 'W', 1, 1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES('p', 'w', 'P', 'p', '/tmp/p', 1, 1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
             VALUES('merged', 'p', 1, 'M', 'merged', 'merged', 'none', 1, 1),
                   ('closed', 'p', 2, 'C', 'closed', 'closed', 'none', 1, 1),
                   ('open', 'p', 3, 'O', 'active', 'active', 'none', 1, 1);
            INSERT INTO jobs(id, project_id, issue_id, status, completed_at, created_at, updated_at)
             VALUES('on-merged', 'p', 'merged', 'cancelled', 50, 1, 1),
                   ('on-closed', 'p', 'closed', 'cancelled', 60, 1, 1),
                   ('no-issue', 'p', NULL, 'cancelled', NULL, 1, 1),
                   ('two-open', 'p', 'merged', 'cancelled', 70, 1, 1),
                   ('live', 'p', 'open', 'running', NULL, 1, 1),
                   ('done', 'p', 'merged', 'complete', 80, 1, 1);
            INSERT INTO sessions(id, job_id, backend, status, terminal_reason, sequence, created_at, updated_at)
             VALUES('s-merged', 'on-merged', 'claude', 'open', NULL, 1, 1, 1),
                   ('s-closed', 'on-closed', 'claude', 'open', NULL, 1, 1, 1),
                   ('s-no-issue', 'no-issue', 'claude', 'open', NULL, 1, 1, 1),
                   ('s-two-a', 'two-open', 'claude', 'open', NULL, 1, 1, 1),
                   ('s-two-b', 'two-open', 'claude', 'open', NULL, 2, 1, 1),
                   ('s-earlier', 'on-merged', 'claude', 'closed', 'rotated_reseed', 1, 1, 1),
                   ('s-live', 'live', 'claude', 'open', NULL, 1, 1, 1),
                   ('s-done', 'done', 'claude', 'open', NULL, 1, 1, 1);
            ",
        )
        .await
        .unwrap();

        // Up to the migration that closes every resolved issue's sessions, so
        // this one is seen alone.
        MigrationRunner::new(
            TURSO_MIGRATIONS
                .iter()
                .take_while(|migration| migration.name() != "close_sessions_on_resolved_issues")
                .copied()
                .collect::<Vec<_>>(),
        )
        .run(&db)
        .await
        .unwrap();

        for (session, expected) in [
            ("s-merged", "closed:issue_merged:50"),
            ("s-closed", "closed:issue_closed:60"),
            ("s-two-a", "closed:issue_merged:70"),
            ("s-two-b", "closed:issue_merged:70"),
            ("s-earlier", "closed:rotated_reseed:"),
            ("s-live", "open::"),
            ("s-done", "open::"),
        ] {
            assert_eq!(
                db.query_opt_text(
                    "SELECT status || ':' || COALESCE(terminal_reason, '') || ':' || COALESCE(closed_at, '')
                       FROM sessions WHERE id = ?1",
                    params![session],
                )
                .await
                .unwrap()
                .as_deref(),
                Some(expected),
                "{session}"
            );
        }
        assert_eq!(
            db.query_opt_text(
                "SELECT status || ':' || terminal_reason || ':' || (closed_at IS NOT NULL)
                   FROM sessions WHERE id = 's-no-issue'",
                (),
            )
            .await
            .unwrap()
            .as_deref(),
            Some("closed:job_cancelled:1"),
            "a job with no issue closes as job_cancelled, stamped with the migration's time"
        );
    }

    /// The migration converges the open sessions older builds left on the jobs
    /// of resolved issues, whatever the job's status and whatever its memory
    /// review state, stamped with when the issue resolved, and leaves an active
    /// issue's sessions and already-closed sessions alone.
    #[tokio::test]
    async fn the_migration_closes_the_sessions_of_resolved_issues() {
        use crate::storage::{MigrationRunner, TURSO_MIGRATIONS};
        let dir = tempfile::tempdir().unwrap();
        let db = LocalDb::open(dir.path().join("resolved-sessions.db"))
            .await
            .unwrap();
        let before = TURSO_MIGRATIONS
            .iter()
            .take_while(|migration| migration.name() != "close_sessions_on_resolved_issues")
            .copied()
            .collect::<Vec<_>>();
        assert!(
            before.len() < TURSO_MIGRATIONS.len(),
            "the migration must be in the lineage"
        );
        MigrationRunner::new(before).run(&db).await.unwrap();
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w', 'W', 1, 1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES('p', 'w', 'P', 'p', '/tmp/p', 1, 1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, merged_at, closed_at, completed_at, created_at, updated_at)
             VALUES('merged', 'p', 1, 'M', 'merged', 'merged', 'none', 100, NULL, 101, 1, 1),
                   ('closed', 'p', 2, 'C', 'closed', 'closed', 'none', NULL, 200, 201, 1, 1),
                   ('merged-bare', 'p', 3, 'MB', 'merged', 'merged', 'none', NULL, NULL, 300, 1, 1),
                   ('closed-unstamped', 'p', 4, 'CU', 'closed', 'closed', 'none', NULL, NULL, NULL, 1, 1),
                   ('open', 'p', 5, 'O', 'active', 'active', 'none', NULL, NULL, NULL, 1, 1);
            INSERT INTO jobs(id, project_id, issue_id, status, memory_review_state, created_at, updated_at)
             VALUES('complete', 'p', 'merged', 'complete', NULL, 1, 1),
                   ('failed', 'p', 'closed', 'failed', NULL, 1, 1),
                   ('pending', 'p', 'merged', 'pending', NULL, 1, 1),
                   ('reviewing', 'p', 'merged', 'complete', 'sent', 1, 1),
                   ('bare', 'p', 'merged-bare', 'complete', NULL, 1, 1),
                   ('unstamped', 'p', 'closed-unstamped', 'failed', NULL, 1, 1),
                   ('live', 'p', 'open', 'running', NULL, 1, 1);
            INSERT INTO sessions(id, job_id, backend, status, terminal_reason, closed_at, sequence, created_at, updated_at)
             VALUES('s-complete', 'complete', 'claude', 'open', NULL, NULL, 1, 1, 1),
                   ('s-failed', 'failed', 'claude', 'open', NULL, NULL, 1, 1, 1),
                   ('s-pending', 'pending', 'claude', 'open', NULL, NULL, 1, 1, 1),
                   ('s-reviewing', 'reviewing', 'claude', 'open', NULL, NULL, 1, 1, 1),
                   ('s-bare', 'bare', 'claude', 'open', NULL, NULL, 1, 1, 1),
                   ('s-unstamped', 'unstamped', 'claude', 'open', NULL, NULL, 1, 1, 1),
                   ('s-earlier', 'complete', 'claude', 'closed', 'rotated_reseed', 5, 0, 1, 1),
                   ('s-live', 'live', 'claude', 'open', NULL, NULL, 1, 1, 1);
            INSERT INTO turns(id, session_id, job_id, sequence, state, start_reason, created_at, updated_at)
             VALUES('t-review', 's-reviewing', 'reviewing', 1, 'yielded', 'memory_review', 1, 1);
            ",
        )
        .await
        .unwrap();

        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();

        for (session, expected, why) in [
            ("s-complete", "closed:issue_merged:100", "a complete job"),
            ("s-failed", "closed:issue_closed:200", "a failed job"),
            ("s-pending", "closed:issue_merged:100", "a pending stray"),
            (
                "s-reviewing",
                "closed:issue_merged:100",
                "no review turn is live while migrations run",
            ),
            (
                "s-bare",
                "closed:issue_merged:300",
                "falls back to the issue's completion time",
            ),
            ("s-earlier", "closed:rotated_reseed:5", "already closed"),
            ("s-live", "open::", "an active issue's work"),
        ] {
            assert_eq!(
                db.query_opt_text(
                    "SELECT status || ':' || COALESCE(terminal_reason, '') || ':' || COALESCE(closed_at, '')
                       FROM sessions WHERE id = ?1",
                    params![session],
                )
                .await
                .unwrap()
                .as_deref(),
                Some(expected),
                "{session}: {why}"
            );
        }
        assert_eq!(
            db.query_opt_text(
                "SELECT status || ':' || terminal_reason || ':' || (closed_at IS NOT NULL)
                   FROM sessions WHERE id = 's-unstamped'",
                (),
            )
            .await
            .unwrap()
            .as_deref(),
            Some("closed:issue_closed:1"),
            "an issue with no resolution time closes stamped with the migration's time"
        );
    }

    struct FixedClock(i64);

    impl crate::services::Clock for FixedClock {
        fn now(&self) -> i64 {
            self.0
        }
        fn now_u64(&self) -> u64 {
            self.0 as u64
        }
    }

    /// `delivered_event_id:retired_at:retirement_reason` of one push.
    async fn push_state(db: &LocalDb, id: &str) -> String {
        db.query_opt_text(
            "SELECT COALESCE(delivered_event_id, '') || ':' || COALESCE(retired_at, '') || ':'
                    || COALESCE(retirement_reason, '')
               FROM attention_pushes WHERE id = ?1",
            params![id.to_string()],
        )
        .await
        .unwrap()
        .unwrap_or_else(|| panic!("push {id} is missing"))
    }

    async fn ids(db: &LocalDb, sql: &str) -> Vec<String> {
        let sql = sql.to_string();
        db.read(move |conn| {
            let sql = sql.clone();
            Box::pin(async move {
                let mut rows = conn.query(&sql, ()).await?;
                let mut out = Vec::new();
                while let Some(row) = rows.next().await? {
                    out.push(row.text(0)?);
                }
                Ok(out)
            })
        })
        .await
        .unwrap()
    }

    /// A resolution retires the pending review pushes naming its issue in the
    /// same write, and nothing else: a delivered review, another issue's review,
    /// and a question on the same issue are untouched. A reopen leaves the
    /// retired row retired, and the reopened issue's next review is a new row.
    #[tokio::test]
    async fn a_resolution_retires_its_issues_pending_review_pushes() {
        use cairn_attention::push::{push, Boundary, Wake};
        let db = crate::storage::migrated_test_db("resolution-retires-reviews.db").await;
        // The project key is stored capitalized; push keys are canonical.
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w', 'W', 1, 1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES('p', 'w', 'P', 'Proj', '/tmp/p', 1, 1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
             VALUES('one', 'p', 1, 'One', 'active', 'active', 'none', 1, 1),
                   ('two', 'p', 2, 'Two', 'active', 'active', 'none', 1, 1);
            INSERT INTO jobs(id, project_id, status, created_at, updated_at)
             VALUES('watcher', 'p', 'running', 1, 1),
                   ('other', 'p', 'running', 1, 1);
            INSERT INTO attention_pushes(id, recipient, content_ref, wake, boundary, key, created_at, delivered_event_id)
             VALUES('pending', 'watcher', 'cairn://p/proj/1/1/builder/create-pr', 'wake', 'event', 'review:cairn://p/proj/1', 1, NULL),
                   ('delivered', 'other', 'cairn://p/proj/1/1/builder/create-pr', 'wake', 'event', 'review:cairn://p/proj/1', 1, 'ev-1'),
                   ('sibling', 'watcher', 'cairn://p/proj/2/1/builder/create-pr', 'wake', 'event', 'review:cairn://p/proj/2', 1, NULL),
                   ('question', 'watcher', 'cairn://p/proj/1/1/builder/questions/q', 'wake', 'event', 'question:cairn://p/proj/1/1/builder/questions/q', 1, NULL);
            ",
        )
        .await
        .unwrap();

        crud::resolve(&db, &FixedClock(500), "one", Resolution::Merged, "device")
            .await
            .unwrap();

        for (id, expected, why) in [
            (
                "pending",
                ":500:review_resolved",
                "the resolved issue's pending review",
            ),
            (
                "delivered",
                "ev-1::",
                "a delivered review is history, not queue",
            ),
            ("sibling", "::", "another issue's review"),
            (
                "question",
                "::",
                "an ask on a resolved issue is suspended, not retired",
            ),
        ] {
            assert_eq!(push_state(&db, id).await, expected, "{id}: {why}");
        }

        crud::unresolve(&db, &FixedClock(600), "one").await.unwrap();
        assert_eq!(
            push_state(&db, "pending").await,
            ":500:review_resolved",
            "a reopen does not resurrect the retired push"
        );

        let (fresh, _) = push(
            &db,
            "watcher",
            "cairn://p/Proj/1/1/builder/create-pr",
            Wake::Wake,
            Boundary::Event,
            "review:cairn://p/Proj/1",
        )
        .await
        .unwrap();
        assert_ne!(fresh, "pending", "the next review is a new row");
        assert_eq!(push_state(&db, &fresh).await, "::");
        assert_eq!(push_state(&db, "pending").await, ":500:review_resolved");
    }

    /// The migration retires exactly the rows the live-database audit selects —
    /// pending `review:` pushes on merged or closed issues — and leaves every
    /// other row as it was.
    #[tokio::test]
    async fn the_migration_retires_review_pushes_of_resolved_issues() {
        use crate::storage::{MigrationRunner, TURSO_MIGRATIONS};
        for name in [
            "retire_review_pushes_of_resolved_issues",
            "retire_late_reviews_of_resolved_issues",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let db = LocalDb::open(dir.path().join("resolved-review-pushes.db"))
                .await
                .unwrap();
            let before = TURSO_MIGRATIONS
                .iter()
                .take_while(|migration| migration.name() != name)
                .copied()
                .collect::<Vec<_>>();
            assert!(
                before.len() < TURSO_MIGRATIONS.len(),
                "the migration must be in the lineage"
            );
            MigrationRunner::new(before).run(&db).await.unwrap();
            db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w', 'W', 1, 1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES('p', 'w', 'P', 'p', '/tmp/p', 1, 1),
                   ('q', 'w', 'Q', 'q', '/tmp/q', 1, 1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
             VALUES('merged', 'p', 1, 'M', 'merged', 'merged', 'none', 1, 1),
                   ('closed', 'p', 2, 'C', 'closed', 'closed', 'none', 1, 1),
                   ('active', 'p', 3, 'A', 'active', 'active', 'none', 1, 1),
                   ('failed', 'p', 4, 'F', 'failed', 'failed', 'none', 1, 1),
                   ('other-project', 'q', 1, 'Q', 'active', 'active', 'none', 1, 1);
            INSERT INTO jobs(id, project_id, status, created_at, updated_at)
             VALUES('watcher', 'p', 'running', 1, 1),
                   ('other', 'p', 'running', 1, 1);
            INSERT INTO attention_pushes(id, recipient, content_ref, wake, boundary, key, created_at, delivered_event_id, retired_at, retirement_reason)
             VALUES('r-merged', 'watcher', 'cairn://p/p/1', 'wake', 'event', 'review:cairn://p/p/1', 1, NULL, NULL, NULL),
                   ('r-closed', 'watcher', 'cairn://p/p/2', 'wake', 'event', 'review:cairn://p/p/2', 1, NULL, NULL, NULL),
                   ('r-active', 'watcher', 'cairn://p/p/3', 'wake', 'event', 'review:cairn://p/p/3', 1, NULL, NULL, NULL),
                   ('r-failed', 'watcher', 'cairn://p/p/4', 'wake', 'event', 'review:cairn://p/p/4', 1, NULL, NULL, NULL),
                   ('r-other-project', 'watcher', 'cairn://p/q/1', 'wake', 'event', 'review:cairn://p/q/1', 1, NULL, NULL, NULL),
                   ('r-delivered', 'other', 'cairn://p/p/1', 'wake', 'event', 'review:cairn://p/p/1', 1, 'ev-1', NULL, NULL),
                   ('r-retired', 'other', 'cairn://p/p/2', 'wake', 'event', 'review:cairn://p/p/2', 1, NULL, 5, 'review_resolved'),
                   ('q-merged', 'watcher', 'cairn://p/p/1/1/builder/questions/q', 'wake', 'event', 'question:cairn://p/p/1/1/builder/questions/q', 1, NULL, NULL, NULL),
                   ('resolved-merged', 'watcher', 'cairn://p/p/1', 'wake', 'event', 'resolved:cairn://p/p/1', 1, NULL, NULL, NULL);
            ",
        )
        .await
        .unwrap();

            const AUDIT: &str = "SELECT p.id FROM attention_pushes p
            JOIN issues i ON p.key = 'review:cairn://p/' || (SELECT key FROM projects WHERE id = i.project_id) || '/' || i.number
           WHERE p.delivered_event_id IS NULL AND p.retired_at IS NULL AND i.status IN ('merged', 'closed')
           ORDER BY p.id";
            let selected = ids(&db, AUDIT).await;
            assert_eq!(selected, vec!["r-closed", "r-merged"]);

            MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
                .run(&db)
                .await
                .unwrap();

            assert_eq!(
                ids(
                    &db,
                    "SELECT id FROM attention_pushes
                  WHERE retirement_reason = 'review_resolved' AND retired_at IS NOT NULL
                    AND id <> 'r-retired' ORDER BY id"
                )
                .await,
                selected,
                "exactly the audited rows retire"
            );
            assert!(ids(&db, AUDIT).await.is_empty());
            for (id, expected) in [
                ("r-active", "::"),
                ("r-failed", "::"),
                ("r-other-project", "::"),
                ("r-delivered", "ev-1::"),
                ("r-retired", ":5:review_resolved"),
                ("q-merged", "::"),
                ("resolved-merged", "::"),
            ] {
                assert_eq!(push_state(&db, id).await, expected, "{id}");
            }
            assert_eq!(
                ids(&db, "SELECT id FROM attention_pushes").await.len(),
                9,
                "nothing is deleted"
            );
        }
    }
}
