//! PR-node resolution: the merge / close lifecycle (`resolve_pr_node`), the
//! `merge_requests` status transitions, and downstream execution advancement.

use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};
use crate::transitions::Resolution;
use cairn_db::turso::params;

use super::context::{
    db_error, resolve_merge_mr_context_for_job, MergeMrContext, PrNodeResolution,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrResolutionAttribution {
    pub actor_kind: &'static str,
    pub actor_identity: Option<String>,
    pub surface: &'static str,
}

pub struct StoredPrResolutionAttribution {
    pub action: String,
    pub actor_kind: String,
    pub actor_identity: Option<String>,
    pub surface: String,
    pub created_at: i64,
    pub lane_snapshot: String,
}

impl PrResolutionAttribution {
    pub fn operator_ui() -> Self {
        Self {
            actor_kind: "operator-ui",
            actor_identity: None,
            surface: "operator-ui",
        }
    }
    pub fn operator_cli() -> Self {
        Self {
            actor_kind: "operator-cli",
            actor_identity: None,
            surface: "operator-cli",
        }
    }
    pub fn agent(uri: String) -> Self {
        Self {
            actor_kind: "agent",
            actor_identity: Some(uri),
            surface: "agent-resource-write",
        }
    }
    pub fn internal(path: &'static str, identity: impl Into<String>) -> Self {
        Self {
            actor_kind: "internal",
            actor_identity: Some(identity.into()),
            surface: path,
        }
    }
    /// A merge or close Cairn did not perform, seen on GitHub by a refresh.
    /// Nobody inside Cairn acted, so there is no principal to name.
    fn github_observation() -> Self {
        Self {
            actor_kind: "unjournaled",
            actor_identity: None,
            surface: GITHUB_OBSERVATION_SURFACE,
        }
    }
}

/// The surface a refresh journals when it observes a resolution Cairn did not
/// perform.
const GITHUB_OBSERVATION_SURFACE: &str = "github-refresh-observation";

/// Who a journaled resolution names, as the PR artifact and the issue summary
/// both render it.
pub fn resolution_actor_label(
    actor_kind: Option<&str>,
    actor_identity: Option<&str>,
    surface: Option<&str>,
) -> String {
    match (actor_kind, actor_identity) {
        (Some("operator-ui"), _) => "operator (UI)".to_string(),
        (Some("operator-cli"), _) => "operator (CLI)".to_string(),
        (Some("unjournaled"), _) if surface == Some(GITHUB_OBSERVATION_SURFACE) => {
            "someone outside Cairn (observed on GitHub)".to_string()
        }
        (_, Some(identity)) => identity.to_string(),
        (Some("unjournaled"), _) | (None, _) => {
            "merge performed by an unjournaled path".to_string()
        }
        (Some(kind), _) => kind.to_string(),
    }
}

async fn lane_snapshot_for_job(orch: &Orchestrator, db: &LocalDb, job_id: &str) -> String {
    let job_id = job_id.to_string();
    let execution = db.query_opt(
        "SELECT json_object('executionStatus', e.status, 'executionSnapshot', json(e.snapshot), 'jobs', COALESCE((SELECT json_group_array(json_object('node', j2.uri_segment, 'status', j2.status)) FROM jobs j2 WHERE j2.execution_id = e.id), '[]')) FROM jobs j LEFT JOIN executions e ON e.id = j.execution_id WHERE j.id = ?1",
        (job_id.clone(),),
        |row| row.text(0),
    ).await.ok().flatten().and_then(|value| serde_json::from_str::<serde_json::Value>(&value).ok()).unwrap_or_else(|| serde_json::json!({"state":"unavailable"}));
    let checks = crate::execution::checks_status::node_check_statuses(orch, &job_id)
        .await
        .and_then(|statuses| serde_json::to_value(statuses).ok())
        .unwrap_or_else(|| serde_json::json!([]));
    serde_json::json!({ "execution": execution, "systematicChecks": checks }).to_string()
}

pub async fn latest_resolution_attribution(
    db: &LocalDb,
    mr_id: &str,
) -> Option<StoredPrResolutionAttribution> {
    let mr_id = mr_id.to_string();
    // A Cairn merge can land on GitHub and be observed by a refresh before its
    // own resolution journals, inside the same second. The principal that acted
    // outranks the observation of its act.
    db.query_opt("SELECT action, actor_kind, actor_identity, surface, created_at, lane_snapshot FROM pr_resolution_attributions WHERE merge_request_id = ?1 ORDER BY created_at DESC, CASE WHEN actor_kind = 'unjournaled' THEN 1 ELSE 0 END LIMIT 1", (mr_id,), |row| Ok(StoredPrResolutionAttribution { action: row.text(0)?, actor_kind: row.text(1)?, actor_identity: row.opt_text(2)?, surface: row.text(3)?, created_at: row.i64(4)?, lane_snapshot: row.text(5)? })).await.ok().flatten()
}

/// Move a `merge_requests` row to its terminal status, returning whether this
/// call moved it.
///
/// The conditional update is the claim on the resolution. A refresh that saw
/// GitHub's merge and Cairn's own merge path can both reach one row, as can two
/// readers that shared one refresh; only the call that moves the row runs the
/// terminal cascade. A closed row may still become merged, because GitHub can
/// merge a pull request that was closed and reopened. Nothing moves a merged row.
async fn claim_merge_request_resolution(
    db: &LocalDb,
    mr_id: &str,
    resolution: PrNodeResolution,
    merged_commit: Option<&str>,
    now: i64,
) -> Result<bool, String> {
    let moved = match resolution {
        PrNodeResolution::Merge => {
            db.execute(
                "UPDATE merge_requests
                 SET status = 'merged', merged_at = ?1, updated_at = ?1,
                     merged_commit = COALESCE(?2, merged_commit)
                 WHERE id = ?3 AND status IN ('open', 'closed')",
                params![now, merged_commit, mr_id],
            )
            .await
        }
        PrNodeResolution::Close => {
            db.execute(
                "UPDATE merge_requests
                 SET status = 'closed', closed_at = ?1, updated_at = ?1
                 WHERE id = ?2 AND status = 'open'",
                params![now, mr_id],
            )
            .await
        }
    }
    .map_err(|e| db_error("Failed to update merge request", e))?;
    Ok(moved > 0)
}

/// Whether an observed resolution of `mr_id` speaks for its issue: the issue is
/// still unresolved, and no other pull request of the issue is open or newer.
///
/// A person who closed the issue keeps that decision, and a pull request that a
/// newer one superseded does not close the issue the newer one serves.
async fn observation_resolves_issue(
    db: &LocalDb,
    issue_id: &str,
    mr_id: &str,
) -> Result<bool, String> {
    let Some(status) = db
        .query_opt_text(
            "SELECT i.status FROM issues i
              WHERE i.id = ?1
                AND NOT EXISTS (
                    SELECT 1 FROM merge_requests other
                     WHERE other.issue_id = i.id AND other.id <> ?2
                       AND (other.status = 'open'
                            OR other.opened_at > (SELECT opened_at FROM merge_requests WHERE id = ?2))
                )",
            params![issue_id, mr_id],
        )
        .await
        .map_err(|e| db_error("Failed to load the issue an observed resolution belongs to", e))?
    else {
        return Ok(false);
    };
    Ok(status
        .parse::<crate::models::IssueStatus>()
        .is_ok_and(|status| !status.is_terminal()))
}

/// Resolve the PR's issue through the one terminal cascade.
///
/// The PR has already been merged or closed for real by the time this runs — on
/// GitHub, or as a fold landed in the shared store — so the resolution cannot be
/// refused back out; [`StopFailure::Escalates`] says so. Everything else is the
/// same cascade the status-patch path runs: the issue's live work is stopped
/// through the canonical node stop before the rows move, so no session is closed
/// over a running turn and no batch outlives the issue that owns it.
async fn resolve_issue_for_pr(
    orch: &Orchestrator,
    db: &LocalDb,
    issue_id: &str,
    resolution: Resolution,
) -> Result<(), String> {
    crate::issues::status::resolve_terminal(
        orch,
        db,
        issue_id,
        resolution,
        crate::issues::status::StopFailure::Escalates,
    )
    .await
    .map_err(|refusal| refusal.to_string())
}

/// Who brought a pull request to its terminal state.
enum Resolver {
    /// A Cairn action that performed the merge or close. `None` is a boundary
    /// that did not say whom it acted for, which is journaled loudly.
    Performed(Option<PrResolutionAttribution>),
    /// A refresh that saw GitHub report a merge or close Cairn did not perform.
    Observed,
}

/// Resolve a `pr` node by its owner id (`merge_requests.job_id`): the producing
/// `pr` action_run (CAIRN-1220) or a legacy `create_pr` producing job.
///
/// `merged_commit` is the commit a merge wrote to the target branch, when the
/// caller knows it.
pub async fn resolve_pr_node(
    orch: &Orchestrator,
    owner_id: &str,
    resolution: PrNodeResolution,
    attribution: Option<PrResolutionAttribution>,
    merged_commit: Option<&str>,
) -> Result<(), String> {
    resolve_pr_node_as(
        orch,
        owner_id,
        resolution,
        merged_commit,
        Resolver::Performed(attribution),
    )
    .await
    .map(|_| ())
}

/// Resolve a pull request that GitHub reports merged or closed by someone other
/// than Cairn: the operator in GitHub's web UI, an integration agent using the
/// GitHub CLI, a teammate.
///
/// It is the resolution a Cairn merge or close runs. The row moves, the `pr`
/// node's terminal cascade runs, and the observation is journaled once as the
/// resolution's provenance. It differs in one respect: a Cairn action resolves
/// the issue because it acted on the issue's behalf, while an observation only
/// knows the pull request moved, so it resolves the issue only when
/// [`observation_resolves_issue`] says the pull request speaks for it.
///
/// Returns whether this call resolved the row; `false` means it already was.
pub(super) async fn resolve_observed_pr_node(
    orch: &Orchestrator,
    owner_id: &str,
    resolution: PrNodeResolution,
    merged_commit: Option<&str>,
) -> Result<bool, String> {
    resolve_pr_node_as(
        orch,
        owner_id,
        resolution,
        merged_commit,
        Resolver::Observed,
    )
    .await
}

async fn resolve_pr_node_as(
    orch: &Orchestrator,
    owner_id: &str,
    resolution: PrNodeResolution,
    merged_commit: Option<&str>,
    resolver: Resolver,
) -> Result<bool, String> {
    // Route to the database that owns this PR's producing job (team replica or
    // private DB). Every row this resolution reads or writes — the merge_requests
    // status transition, the issue resolution, the pr action_run completion, and
    // the producing-execution advance — lives in that database. This runs AFTER
    // the merge/close already hit GitHub or the local jj fold, so reading the
    // private DB for a team job would error with “No merge request found” and
    // strand the team replica's PR (and its blocked action run) unresolved.
    let db = crate::execution::routing::routing_db_for_id(&orch.db, owner_id)
        .await
        .map_err(|e| e.to_string())?;
    let merge_context = resolve_merge_mr_context_for_job(&db, owner_id).await?;
    let mr_id = merge_context.mr.mr_id.clone();
    let now = chrono::Utc::now().timestamp();
    let lane_snapshot = lane_snapshot_for_job(orch, &db, owner_id).await;
    let performed = matches!(resolver, Resolver::Performed(_));
    let claimed =
        claim_merge_request_resolution(&db, &mr_id, resolution, merged_commit, now).await?;
    let attribution = match resolver {
        Resolver::Performed(attribution) => Some(attribution.unwrap_or_else(|| {
            log::error!(
                "merge performed by an unjournaled path: owner={owner_id} resolution={resolution:?}"
            );
            PrResolutionAttribution {
                actor_kind: "unjournaled",
                actor_identity: None,
                surface: "unjournaled-path",
            }
        })),
        // Only the call that resolved the row journals the observation, so a
        // resolution observed by many readers is recorded once.
        Resolver::Observed => claimed.then(PrResolutionAttribution::github_observation),
    };
    if let Some(attribution) = attribution {
        db.execute(
            "INSERT INTO pr_resolution_attributions (id, merge_request_id, action, actor_kind, actor_identity, surface, lane_snapshot, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![uuid::Uuid::new_v4().to_string(), mr_id.as_str(), if matches!(resolution, PrNodeResolution::Merge) { "merge" } else { "close" }, attribution.actor_kind, attribution.actor_identity.as_deref(), attribution.surface, lane_snapshot.as_str(), now],
        ).await.map_err(|e| db_error("Failed to journal PR resolution attribution", e))?;
    }

    if !claimed {
        if !performed {
            return Ok(false);
        }
        // Cairn performed this merge or close, but the row was already resolved:
        // by a refresh that saw this same merge land on GitHub while this call
        // was in flight, or by an earlier resolution. The cascade ran then. What
        // this call adds is who acted and, for a merge, the commit it produced.
        if let (PrNodeResolution::Merge, Some(commit)) = (resolution, merged_commit) {
            db.execute(
                "UPDATE merge_requests SET merged_commit = ?1
                 WHERE id = ?2 AND status = 'merged' AND merged_commit IS NULL",
                params![commit, mr_id.as_str()],
            )
            .await
            .map_err(|e| db_error("Failed to record the merged commit", e))?;
        }
        announce_pr_resolution(orch, &merge_context);
        return Ok(false);
    }

    let issue_to_resolve = match merge_context.issue_id.as_deref() {
        Some(issue_id) if performed => Some(issue_id),
        Some(issue_id) => observation_resolves_issue(&db, issue_id, &mr_id)
            .await?
            .then_some(issue_id),
        None => None,
    };
    // Resolving the PR resolves its issue, and resolving an issue runs the
    // terminal cascade: the issue's live work is stopped, its queued work is
    // cancelled, its sessions close, and any in-flight turn-end review suite is
    // quit (CAIRN-2648, CAIRN-3253).
    if let Some(issue_id) = issue_to_resolve {
        let issue_resolution = match resolution {
            PrNodeResolution::Merge => Resolution::Merged,
            PrNodeResolution::Close => Resolution::Closed,
        };
        resolve_issue_for_pr(orch, &db, issue_id, issue_resolution).await?;
    }

    if let Some(issue_id) = issue_to_resolve {
        if merge_context.has_triage_batch {
            let result = match resolution {
                PrNodeResolution::Merge => {
                    crate::memories::db::resolve_triage_batch_on_merge(&db, issue_id).await
                }
                PrNodeResolution::Close => {
                    crate::memories::db::revert_triage_batch_on_close(&db, issue_id).await
                }
            };
            match result {
                Ok(ids) if !ids.is_empty() => log::info!(
                    "Resolved {} canon memory triage row(s) for issue {} via {:?}",
                    ids.len(),
                    issue_id,
                    resolution
                ),
                Ok(_) => {}
                Err(error) => log::warn!(
                    "Memory triage {:?} reconciliation failed for issue {}: {}",
                    resolution,
                    issue_id,
                    error
                ),
            }
        }

        if matches!(resolution, PrNodeResolution::Close) {
            match crate::memories::db::discard_draft_memories_for_closed_issue(&db, issue_id).await
            {
                Ok(ids) if !ids.is_empty() => log::info!(
                    "Discarded {} draft memory row(s) for closed issue {}",
                    ids.len(),
                    issue_id
                ),
                Ok(_) => {}
                Err(error) => log::warn!(
                    "Failed to discard draft memories for closed issue {}: {}",
                    issue_id,
                    error
                ),
            }
        }
    }

    let port = match resolution {
        PrNodeResolution::Merge => "merge",
        PrNodeResolution::Close => "close",
    };
    crate::pr_data::ports::fire_pr_node_port_for_owner(&db, owner_id, port).await?;
    // A first-class `pr` action_run was `Blocked` while the PR was open;
    // resolution completes it so the execution can finish. The durable MR owner
    // is the producing job, so the action_run is found by parent_job_id. Legacy
    // `create_pr` PRs have no action_run to flip; a single-job recompute covers
    // that path. See CAIRN-1220.
    match complete_pr_action_run_if_owner(&db, owner_id, now).await? {
        Some(execution_id) => {
            crate::execution::advancement::recompute_execution_jobs(orch, &execution_id)?
        }
        None => crate::execution::advancement::recompute_job(orch, owner_id)?,
    }
    if let Err(e) = advance_producing_execution_after_pr_resolution(orch, &mr_id).await {
        log::warn!(
            "Failed to advance producing execution after PR resolution: {}",
            e
        );
    }

    if matches!(resolution, PrNodeResolution::Merge) {
        if let Some(issue_id) = issue_to_resolve.map(str::to_string) {
            let orch_for_memories = orch.clone();
            tokio::spawn(async move {
                let started = std::time::Instant::now();
                match crate::memories::commands::confirm_and_spawn_drafts_for_merged_issue(
                    orch_for_memories,
                    &issue_id,
                )
                .await
                {
                    Ok(spawned) if !spawned.is_empty() => log::info!(
                        "Confirmed draft memories for merged issue {} and spawned {} triage issue(s) in {:?}",
                        issue_id,
                        spawned.len(),
                        started.elapsed()
                    ),
                    Ok(_) => log::info!(
                        "Confirmed draft memories for merged issue {} with no triage issue spawned in {:?}",
                        issue_id,
                        started.elapsed()
                    ),
                    Err(error) => log::warn!(
                        "Failed to confirm draft memories for merged issue {}: {}",
                        issue_id,
                        error
                    ),
                }
            });
        }

        // Downstream sibling/coordinator reconciliation advances *other* in-flight
        // branches (siblings auto-rebase onto the advanced tip; a Coordinator on
        // the integration branch has its `@` re-parented). It has no bearing on
        // whether THIS PR is merged, is best-effort end to end, and is re-fired
        // idempotently by the GitHub `push` webhook (guarded by the before/after
        // commit-id snapshot in `reconcile_base_advance`). So spawn it off the
        // synchronous merge path: the resolution — and the "merging" button —
        // returns immediately, and the reconcile lands in the background.
        let orch = orch.clone();
        let owner_id = owner_id.to_string();
        tokio::spawn(async move {
            let started = std::time::Instant::now();
            if let Err(e) = crate::orchestrator::base_advance::notify_downstream_of_base_advance(
                &orch, &owner_id,
            )
            .await
            {
                log::warn!("Failed to notify downstream jobs of base advance: {}", e);
            }
            log::info!(
                "Deferred downstream base-advance reconcile for owner {} took {:?}",
                owner_id,
                started.elapsed()
            );
        });
    }

    announce_pr_resolution(orch, &merge_context);
    Ok(true)
}

/// Merged/closed is the settled terminal state the PR snapshot caches without
/// an expiry, so every write that moves a resolved row advances the generation
/// and tells the row's readers.
fn announce_pr_resolution(orch: &Orchestrator, merge_context: &MergeMrContext) {
    orch.invalidate_pr_refresh(&merge_context.mr.job_id, "pull-request-resolved");
    let _ = orch.services.emitter.emit(
        "db-change",
        crate::notify::merge_request_db_change_ids(
            "update",
            Some(&merge_context.mr.mr_id),
            Some(&merge_context.mr.job_id),
            merge_context.issue_id.as_deref(),
            &merge_context.project_id,
        ),
    );
    if let Some(issue_id) = merge_context.issue_id.as_deref() {
        let _ = orch.services.emitter.emit(
            "db-change",
            crate::notify::issue_db_change_ids("update", issue_id, Some(&merge_context.project_id)),
        );
    }
}

/// If `owner_id` is a producing job with a blocked `pr` action_run child, mark
/// that action_run `complete` and return its execution_id. Also accepts an
/// action_run id for unrepaired historical rows. Returns `None` for legacy
/// `create_pr` jobs with no first-class PR action.
async fn complete_pr_action_run_if_owner(
    db: &LocalDb,
    owner_id: &str,
    now: i64,
) -> Result<Option<String>, String> {
    let owner_id = owner_id.to_string();
    db.write(|conn| {
        let owner_id = owner_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, execution_id
                     FROM (
                         SELECT id, execution_id, 0 AS priority
                         FROM action_runs
                         WHERE parent_job_id = ?1
                         UNION ALL
                         SELECT id, execution_id, 1 AS priority
                         FROM action_runs
                         WHERE id = ?1
                     )
                     ORDER BY priority
                     LIMIT 1",
                    params![owner_id.as_str()],
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Ok(None);
            };
            let action_run_id = row.text(0)?;
            let execution_id = row.text(1)?;
            drop(rows);
            conn.execute(
                "UPDATE action_runs SET status = 'complete', completed_at = ?1 WHERE id = ?2",
                params![now, action_run_id.as_str()],
            )
            .await?;
            Ok(Some(execution_id))
        })
    })
    .await
    .map_err(|e| db_error("Failed to complete PR action_run", e))
}

/// After a `merge_requests` row transitions to merged or closed, enqueue an
/// `AdvanceDag` for the execution that produced the PR so any downstream node
/// gated on PR-merged-ness wakes. No-op when the row has no linked job or the
/// job has no `execution_id` (manually-attached or non-recipe PRs).
///
/// Idempotency: callers may invoke this more than once for the same `mr_id`
/// without harm. Each call enqueues a fresh outbox entry and sends a follow-on
/// `AdvanceDag`; `reduce_dag` is idempotent, so a duplicate advancement is
/// cheap and strictly safer than dropping one.
///
/// Errors are swallowed at the call sites — webhook handlers must not return
/// errors that GitHub will retry, and the durable outbox row is the recovery
/// mechanism if the in-memory send is lost.
pub async fn advance_producing_execution_after_pr_resolution(
    orch: &Orchestrator,
    mr_id: &str,
) -> Result<(), String> {
    let mr_id_owned = mr_id.to_string();
    // Route to the owning database (team replica or private DB); the PR's
    // producing job/action_run and execution live there. Best-effort: a routing
    // failure logs and no-ops, matching the swallow-errors contract below.
    let db = match crate::execution::routing::routing_db_for_id(&orch.db, mr_id).await {
        Ok(db) => db,
        Err(e) => {
            log::warn!(
                "Failed to route producing-execution lookup for mr {}: {}",
                mr_id,
                e
            );
            return Ok(());
        }
    };
    let execution_id = match db
        .query_opt_text(
            // The PR's owner (`merge_requests.job_id`) is either a producing
            // job (legacy `create_pr`) or a producing `pr` action_run
            // (CAIRN-1220); resolve the execution from whichever it is.
            "SELECT COALESCE(j.execution_id, ar.execution_id)
             FROM merge_requests mr
             LEFT JOIN jobs j ON mr.job_id = j.id
             LEFT JOIN action_runs ar ON mr.job_id = ar.id
             WHERE mr.id = ?1
             LIMIT 1",
            params![mr_id_owned.as_str()],
        )
        .await
    {
        Ok(value) => value,
        Err(e) => {
            log::warn!(
                "Failed to look up producing execution for mr {}: {}",
                mr_id,
                e
            );
            return Ok(());
        }
    };

    let Some(execution_id) = execution_id else {
        log::debug!(
            "No producing execution for mr {} — skipping DAG advance",
            mr_id
        );
        return Ok(());
    };

    if let Err(error) = crate::workflow_host::runtime(orch)
        .enqueue_advance(&execution_id)
        .await
    {
        log::warn!(
            "Failed to enqueue advance_dag outbox entry for execution {execution_id}: {error}"
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pr_data::actions::test_support::{
        migrated_db, seed_pr_node_merge_request_for_artifact_job, test_orchestrator,
    };
    use crate::services::testing::MockGitClient;

    /// Another job on the PR node fixture's issue. `recipe_node_id` stays NULL so
    /// it is extra work on the issue rather than a second claimant of a snapshot
    /// node.
    async fn seed_issue_job(db: &LocalDb, job_id: &str, node_name: &str, status: &str) {
        let job_id = job_id.to_string();
        let node_name = node_name.to_string();
        let status = status.to_string();
        db.execute(
            "INSERT INTO jobs (id, execution_id, node_name, status, issue_id, project_id, created_at, updated_at)
             VALUES (?1, 'exec-pr-node', ?2, ?3, 'issue-pr-node', 'proj-pr-node', 2, 2)",
            params![job_id.as_str(), node_name.as_str(), status.as_str()],
        )
        .await
        .unwrap();
    }

    /// Give an existing job the session, run, and running turn a live agent holds
    /// — the shape the canonical stop acts on.
    async fn attach_live_session(db: &LocalDb, job_id: &str) {
        let job_id = job_id.to_string();
        db.write(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let session_id = format!("session-{job_id}");
                let run_id = format!("run-{job_id}");
                let turn_id = format!("turn-{job_id}");
                conn.execute(
                    "INSERT INTO sessions (id, job_id, status, created_at, updated_at)
                     VALUES (?1, ?2, 'open', 1, 1)",
                    params![session_id.as_str(), job_id.as_str()],
                )
                .await?;
                conn.execute(
                    "INSERT INTO runs (id, project_id, job_id, chat_id, status, session_id, created_at, updated_at, start_mode)
                     VALUES (?1, 'proj-pr-node', ?2, NULL, 'live', ?3, 1, 1, 'resume')",
                    params![run_id.as_str(), job_id.as_str(), session_id.as_str()],
                )
                .await?;
                conn.execute(
                    "INSERT INTO turns (id, session_id, run_id, job_id, sequence, state, created_at, updated_at)
                     VALUES (?1, ?2, ?3, ?4, 1, 'running', 1, 1)",
                    params![
                        turn_id.as_str(),
                        session_id.as_str(),
                        run_id.as_str(),
                        job_id.as_str()
                    ],
                )
                .await?;
                conn.execute(
                    "UPDATE jobs SET current_session_id = ?1, current_turn_id = ?2 WHERE id = ?3",
                    params![session_id.as_str(), turn_id.as_str(), job_id.as_str()],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    async fn scalar(db: &LocalDb, sql: &str, id: &str) -> Option<String> {
        db.query_opt_text(sql, params![id]).await.unwrap()
    }

    async fn live_run_count(db: &LocalDb) -> i64 {
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT COUNT(*) FROM runs WHERE status IN ('starting', 'live')",
                        (),
                    )
                    .await?;
                rows.next().await?.expect("count row").i64(0)
            })
        })
        .await
        .unwrap()
    }

    async fn seed_parent_coordinator(db: &LocalDb, muted: bool) {
        db.execute_script(
            "INSERT INTO issues
               (id, project_id, number, title, status, progress, attention, created_at, updated_at)
             VALUES
               ('parent-issue', 'proj-pr-node', 1, 'Parent thread', 'active', 'active', 'none', 1, 1);
             UPDATE issues SET parent_issue_id = 'parent-issue' WHERE id = 'issue-pr-node';
             UPDATE merge_requests SET github_pr_number = 3023 WHERE id = 'mr-pr-node';
             INSERT INTO jobs
               (id, project_id, issue_id, status, current_session_id, created_at, updated_at)
             VALUES
               ('parent-coordinator', 'proj-pr-node', 'parent-issue', 'running', 'parent-session', 2, 2);",
        )
        .await
        .unwrap();
        if muted {
            db.execute(
                "INSERT INTO wake_subscriptions
                   (id, job_id, source_kind, source_ref, fact_kinds_json, state,
                    created_by, created_at, updated_at, one_shot)
                 VALUES
                   ('muted-child', 'parent-coordinator', 'issue', 'cairn://p/proj/3',
                    '[\"resolved\"]', 'muted', 'agent', 2, 2, 0)",
                (),
            )
            .await
            .unwrap();
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn operator_ui_merge_notifies_parent_and_mute_downgrades_to_digest() {
        use crate::orchestrator::attention_delivery::render_push_resolved;
        use crate::orchestrator::attention_push::{list_pending, Wake};

        for muted in [false, true] {
            let db = migrated_db().await;
            seed_pr_node_merge_request_for_artifact_job(&db).await;
            seed_parent_coordinator(&db, muted).await;
            let orch = test_orchestrator(db, MockGitClient::new());

            resolve_pr_node(
                &orch,
                "builder-job",
                PrNodeResolution::Merge,
                Some(PrResolutionAttribution::operator_ui()),
                None,
            )
            .await
            .expect("the operator UI merge resolves the child");

            let pushes = list_pending(&orch.db.local, "parent-coordinator")
                .await
                .unwrap();
            assert_eq!(pushes.len(), 1, "muted={muted}");
            assert_eq!(
                pushes[0].wake,
                if muted { Wake::Passive } else { Wake::Wake },
                "muted={muted}"
            );
            let rendered = render_push_resolved(&orch, &pushes[0]).await;
            assert!(rendered.contains("proj-3 \"Issue\" merged"), "{rendered}");
            assert!(
                rendered.contains("PR #3023, by operator (UI)"),
                "{rendered}"
            );
        }
    }

    /// The close door announces exactly as the merge door does. Neither the
    /// UI close nor the recipe `close_issue` action calls anything after the
    /// cascade, so the announcement has to come from the cascade itself.
    #[tokio::test(flavor = "current_thread")]
    async fn operator_ui_close_notifies_parent() {
        use crate::orchestrator::attention_delivery::render_push_resolved;
        use crate::orchestrator::attention_push::{list_pending, Wake};

        let db = migrated_db().await;
        seed_pr_node_merge_request_for_artifact_job(&db).await;
        seed_parent_coordinator(&db, false).await;
        let orch = test_orchestrator(db, MockGitClient::new());

        resolve_pr_node(
            &orch,
            "builder-job",
            PrNodeResolution::Close,
            Some(PrResolutionAttribution::operator_ui()),
            None,
        )
        .await
        .expect("the operator UI close resolves the child");

        let pushes = list_pending(&orch.db.local, "parent-coordinator")
            .await
            .unwrap();
        assert_eq!(pushes.len(), 1);
        assert_eq!(pushes[0].wake, Wake::Wake);
        let rendered = render_push_resolved(&orch, &pushes[0]).await;
        assert!(rendered.contains("proj-3 \"Issue\" closed"), "{rendered}");
    }

    /// Resolving an issue that is already terminal crosses no edge, so it is
    /// not a second announcement — even though it restamps the resolution
    /// instant the push fingerprint is keyed to, which is exactly what would
    /// let a delivered notice fire again.
    #[tokio::test(flavor = "current_thread")]
    async fn resolving_an_already_resolved_issue_announces_nothing_new() {
        use crate::orchestrator::attention_push::list_pending;

        let db = migrated_db().await;
        seed_pr_node_merge_request_for_artifact_job(&db).await;
        seed_parent_coordinator(&db, false).await;
        let orch = test_orchestrator(db, MockGitClient::new());

        resolve_pr_node(
            &orch,
            "builder-job",
            PrNodeResolution::Merge,
            Some(PrResolutionAttribution::operator_ui()),
            None,
        )
        .await
        .expect("the merge resolves the child");
        assert_eq!(
            list_pending(&orch.db.local, "parent-coordinator")
                .await
                .unwrap()
                .len(),
            1
        );
        orch.db
            .local
            .execute(
                "UPDATE attention_pushes SET delivered_event_id = 'delivered'
                  WHERE recipient = 'parent-coordinator'",
                (),
            )
            .await
            .unwrap();

        crate::issues::status::resolve_terminal(
            &orch,
            &orch.db.local,
            "issue-pr-node",
            Resolution::Merged,
            crate::issues::status::StopFailure::Escalates,
        )
        .await
        .expect("a repeat resolution lands");

        assert!(
            list_pending(&orch.db.local, "parent-coordinator")
                .await
                .unwrap()
                .is_empty(),
            "the coordinator already heard about this resolution"
        );
    }

    /// The PR merge is the door CAIRN-3241 came through: the PR merged, the issue
    /// row flipped, and the builder went on running test suites against a merged
    /// issue — because this path resolved the issue directly instead of running
    /// the cascade the confirmed-close path already had. It runs the same one now.
    #[tokio::test(flavor = "current_thread")]
    async fn merging_a_pr_stops_the_issues_live_work() {
        let db = migrated_db().await;
        seed_pr_node_merge_request_for_artifact_job(&db).await;
        seed_issue_job(&db, "live-builder", "builder", "running").await;
        attach_live_session(&db, "live-builder").await;
        seed_issue_job(&db, "queued-reviewer", "reviewer", "pending").await;
        let orch = test_orchestrator(db, MockGitClient::new());
        let db = &orch.db.local;

        resolve_pr_node(
            &orch,
            "builder-job",
            PrNodeResolution::Merge,
            Some(PrResolutionAttribution::agent(
                "cairn://p/prj/1/1/thread".to_string(),
            )),
            None,
        )
        .await
        .expect("the merge resolution lands");

        let attribution = latest_resolution_attribution(db, "mr-pr-node")
            .await
            .expect("the resolution journals attribution");
        assert_eq!(attribution.actor_kind, "agent");
        assert_eq!(
            attribution.actor_identity.as_deref(),
            Some("cairn://p/prj/1/1/thread")
        );
        assert!(attribution.lane_snapshot.contains("executionStatus"));

        assert_eq!(
            scalar(
                db,
                "SELECT status FROM issues WHERE id = ?1",
                "issue-pr-node"
            )
            .await,
            Some("merged".to_string())
        );

        // The running builder went through the canonical node stop: its turn is
        // interrupted and its run settled as a stop, not left executing.
        assert_eq!(
            scalar(
                db,
                "SELECT state FROM turns WHERE id = ?1",
                "turn-live-builder"
            )
            .await,
            Some("interrupted".to_string())
        );
        assert_eq!(
            scalar(
                db,
                "SELECT status FROM runs WHERE id = ?1",
                "run-live-builder"
            )
            .await,
            Some("exited".to_string())
        );
        assert_eq!(
            scalar(
                db,
                "SELECT exit_reason FROM runs WHERE id = ?1",
                "run-live-builder"
            )
            .await,
            Some("user_stop".to_string())
        );

        // Work that never started is cancelled rather than left queued against a
        // merged issue.
        assert_eq!(
            scalar(
                db,
                "SELECT status FROM jobs WHERE id = ?1",
                "queued-reviewer"
            )
            .await,
            Some("cancelled".to_string())
        );

        assert_eq!(
            scalar(db, "SELECT status FROM jobs WHERE id = ?1", "live-builder").await,
            Some("cancelled".to_string()),
            "terminal resolution archives the stopped builder rather than leaving it running"
        );

        // And the session closes over stopped work, not running work.
        assert_eq!(
            scalar(
                db,
                "SELECT status FROM sessions WHERE id = ?1",
                "session-live-builder"
            )
            .await,
            Some("closed".to_string())
        );
        assert_eq!(
            live_run_count(db).await,
            0,
            "no run keeps executing against a merged issue"
        );
    }

    /// The half-dead state must be unrepresentable: a session that refuses to
    /// continue while its own runs keep going.
    ///
    /// The cascade's enumeration reads the `jobs` table, so a job that is not
    /// itself live work slips past it — here one already marked `complete` while
    /// still holding an open session and a running turn, which is also what a
    /// turn started between the enumeration and the close looks like. The
    /// postcondition is what closes that gap.
    #[tokio::test(flavor = "current_thread")]
    async fn a_closed_session_is_never_left_with_a_turn_in_flight() {
        let db = migrated_db().await;
        seed_pr_node_merge_request_for_artifact_job(&db).await;
        // `builder-job` is `complete`, so it is not enumerated as live work.
        attach_live_session(&db, "builder-job").await;
        let orch = test_orchestrator(db, MockGitClient::new());
        let db = &orch.db.local;

        resolve_pr_node(&orch, "builder-job", PrNodeResolution::Merge, None, None)
            .await
            .expect("the merge resolution lands");

        let phantom = latest_resolution_attribution(db, "mr-pr-node")
            .await
            .expect("an unattributed boundary is journaled loudly");
        assert_eq!(phantom.action, "merge");
        assert_eq!(phantom.actor_kind, "unjournaled");
        assert_eq!(phantom.surface, "unjournaled-path");

        assert_eq!(
            scalar(
                db,
                "SELECT status FROM sessions WHERE id = ?1",
                "session-builder-job"
            )
            .await,
            Some("closed".to_string()),
            "the resolution closes the session"
        );
        assert_eq!(
            scalar(
                db,
                "SELECT state FROM turns WHERE id = ?1",
                "turn-builder-job"
            )
            .await,
            Some("interrupted".to_string()),
            "and nothing it closed is left with a turn in flight"
        );
        assert_eq!(live_run_count(db).await, 0);
    }

    /// The PR door reaches the projection settlement: once the merge lands, no
    /// job on the issue rests running, pending, idle, or blocked — including
    /// the builder the postcondition stopped, which before settlement re-derived
    /// Pending against the merged issue.
    #[tokio::test(flavor = "current_thread")]
    async fn merging_a_pr_leaves_no_job_unsettled() {
        let db = migrated_db().await;
        seed_pr_node_merge_request_for_artifact_job(&db).await;
        attach_live_session(&db, "builder-job").await;
        seed_issue_job(&db, "queued-reviewer", "reviewer", "pending").await;
        let orch = test_orchestrator(db, MockGitClient::new());
        let db = &orch.db.local;

        resolve_pr_node(&orch, "builder-job", PrNodeResolution::Merge, None, None)
            .await
            .expect("the merge resolution lands");

        let unsettled = db
            .query_opt_i64(
                "SELECT COUNT(*) FROM jobs WHERE issue_id = 'issue-pr-node'
                   AND status IN ('running', 'pending', 'idle', 'blocked')",
                (),
            )
            .await
            .unwrap()
            .unwrap_or(0);
        assert_eq!(unsettled, 0, "a merged issue holds no unsettled job");
    }

    async fn observation_count(db: &LocalDb) -> i64 {
        db.query_opt_i64(
            "SELECT COUNT(*) FROM pr_resolution_attributions
              WHERE merge_request_id = 'mr-pr-node' AND actor_kind = 'unjournaled'",
            (),
        )
        .await
        .unwrap()
        .unwrap_or(0)
    }

    /// A merge made on GitHub by someone other than Cairn resolves exactly as a
    /// Cairn merge does: the row, its merge commit, the issue, and the `pr`
    /// node's blocked action run. The observation is journaled once, and seeing
    /// it again changes nothing.
    #[tokio::test(flavor = "current_thread")]
    async fn an_observed_merge_resolves_the_row_and_the_pr_node_once() {
        let db = migrated_db().await;
        seed_pr_node_merge_request_for_artifact_job(&db).await;
        let orch = test_orchestrator(db, MockGitClient::new());
        let db = &orch.db.local;

        let resolved = resolve_observed_pr_node(
            &orch,
            "builder-job",
            PrNodeResolution::Merge,
            Some("b77e7657"),
        )
        .await
        .expect("the observed merge resolves");
        assert!(resolved, "the first observation resolves the row");

        let row = |column: &'static str| {
            let sql = format!("SELECT {column} FROM merge_requests WHERE id = ?1");
            async move { scalar(db, &sql, "mr-pr-node").await }
        };
        assert_eq!(row("status").await.as_deref(), Some("merged"));
        assert_eq!(row("merged_commit").await.as_deref(), Some("b77e7657"));
        let merged_at = row("CAST(merged_at AS TEXT)").await;
        assert!(merged_at.is_some(), "merged_at is recorded");
        assert_eq!(
            scalar(
                db,
                "SELECT status FROM issues WHERE id = ?1",
                "issue-pr-node"
            )
            .await
            .as_deref(),
            Some("merged")
        );
        assert_eq!(
            scalar(
                db,
                "SELECT status FROM action_runs WHERE id = ?1",
                "pr-action-run"
            )
            .await
            .as_deref(),
            Some("complete"),
            "the pr node's blocked action run completes"
        );
        let observed = latest_resolution_attribution(db, "mr-pr-node")
            .await
            .expect("the observation is journaled");
        assert_eq!(observed.action, "merge");
        assert_eq!(observed.actor_kind, "unjournaled");
        assert_eq!(observed.surface, "github-refresh-observation");
        assert_eq!(observation_count(db).await, 1);

        let again = resolve_observed_pr_node(
            &orch,
            "builder-job",
            PrNodeResolution::Merge,
            Some("b77e7657"),
        )
        .await
        .expect("a repeat observation is harmless");
        assert!(!again, "a resolved row is not resolved twice");
        assert_eq!(observation_count(db).await, 1, "journaled once");
        assert_eq!(row("CAST(merged_at AS TEXT)").await, merged_at);
    }

    /// GitHub closing a pull request says nothing about an issue somebody has
    /// already resolved, so an observed close never rewrites a merged issue.
    #[tokio::test(flavor = "current_thread")]
    async fn an_observed_close_leaves_an_already_resolved_issue_alone() {
        let db = migrated_db().await;
        seed_pr_node_merge_request_for_artifact_job(&db).await;
        let orch = test_orchestrator(db, MockGitClient::new());
        let db = &orch.db.local;
        crate::issues::status::resolve_terminal(
            &orch,
            db,
            "issue-pr-node",
            Resolution::Merged,
            crate::issues::status::StopFailure::Escalates,
        )
        .await
        .expect("the issue resolves merged");

        assert!(
            resolve_observed_pr_node(&orch, "builder-job", PrNodeResolution::Close, None)
                .await
                .unwrap()
        );

        assert_eq!(
            scalar(
                db,
                "SELECT status FROM merge_requests WHERE id = ?1",
                "mr-pr-node"
            )
            .await
            .as_deref(),
            Some("closed")
        );
        assert_eq!(
            scalar(
                db,
                "SELECT status FROM issues WHERE id = ?1",
                "issue-pr-node"
            )
            .await
            .as_deref(),
            Some("merged"),
            "the observed close does not overwrite the issue's resolution"
        );
        assert_eq!(
            scalar(
                db,
                "SELECT status FROM action_runs WHERE id = ?1",
                "pr-action-run"
            )
            .await
            .as_deref(),
            Some("complete"),
            "the pr node still resolves"
        );
    }

    /// A pull request a newer one superseded does not close the issue the newer
    /// one serves.
    #[tokio::test(flavor = "current_thread")]
    async fn an_observed_close_of_a_superseded_pull_request_keeps_the_issue_open() {
        let db = migrated_db().await;
        seed_pr_node_merge_request_for_artifact_job(&db).await;
        db.execute(
            "INSERT INTO merge_requests (id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at)
             VALUES ('mr-newer', 'builder-job-2', 'proj-pr-node', 'issue-pr-node', 'Retry', 'agent/proj-3-builder-1', 'main', 'open', 5, 5)",
            (),
        )
        .await
        .unwrap();
        let orch = test_orchestrator(db, MockGitClient::new());
        let db = &orch.db.local;

        assert!(
            resolve_observed_pr_node(&orch, "builder-job", PrNodeResolution::Close, None)
                .await
                .unwrap()
        );

        assert_eq!(
            scalar(
                db,
                "SELECT status FROM merge_requests WHERE id = ?1",
                "mr-pr-node"
            )
            .await
            .as_deref(),
            Some("closed")
        );
        let issue_status = scalar(
            db,
            "SELECT status FROM issues WHERE id = ?1",
            "issue-pr-node",
        )
        .await
        .and_then(|status| status.parse::<crate::models::IssueStatus>().ok())
        .expect("the issue has a status");
        assert!(
            !issue_status.is_terminal(),
            "the issue stays unresolved, got {issue_status}"
        );
    }

    /// Cairn's own GitHub merge can be observed by a refresh before it resolves
    /// the row itself. The observation then owns the cascade, and Cairn's late
    /// resolution records only what it knows better: who acted, and the commit.
    #[tokio::test(flavor = "current_thread")]
    async fn a_cairn_merge_overtaken_by_its_observation_records_its_actor_and_commit() {
        let db = migrated_db().await;
        seed_pr_node_merge_request_for_artifact_job(&db).await;
        let orch = test_orchestrator(db, MockGitClient::new());
        let db = &orch.db.local;

        assert!(
            resolve_observed_pr_node(&orch, "builder-job", PrNodeResolution::Merge, None)
                .await
                .unwrap()
        );
        let merged_at = scalar(
            db,
            "SELECT CAST(merged_at AS TEXT) FROM merge_requests WHERE id = ?1",
            "mr-pr-node",
        )
        .await;

        resolve_pr_node(
            &orch,
            "builder-job",
            PrNodeResolution::Merge,
            Some(PrResolutionAttribution::operator_ui()),
            Some("cafe1234"),
        )
        .await
        .expect("the late resolution lands");

        assert_eq!(
            scalar(
                db,
                "SELECT merged_commit FROM merge_requests WHERE id = ?1",
                "mr-pr-node"
            )
            .await
            .as_deref(),
            Some("cafe1234")
        );
        assert_eq!(
            scalar(
                db,
                "SELECT CAST(merged_at AS TEXT) FROM merge_requests WHERE id = ?1",
                "mr-pr-node"
            )
            .await,
            merged_at,
            "the row moved once"
        );
        let latest = latest_resolution_attribution(db, "mr-pr-node")
            .await
            .unwrap();
        assert_eq!(
            latest.actor_kind, "operator-ui",
            "the actor outranks the observation"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pr_resolution_completes_pr_action_run_for_producing_job_owner() {
        let db = migrated_db().await;
        seed_pr_node_merge_request_for_artifact_job(&db).await;

        let execution_id = complete_pr_action_run_if_owner(&db, "builder-job", 42)
            .await
            .unwrap()
            .expect("builder job resolves its pr action_run child");

        assert_eq!(execution_id, "exec-pr-node");
        let status = db
            .query_text(
                "SELECT status FROM action_runs WHERE id = 'pr-action-run'",
                (),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status, "complete");
    }

    /// The merge/close lifecycle transition itself must land in the owning replica.
    /// A team-prefixed producing job resolves (Close) against its injected replica
    /// — marking the team MR closed and never touching the empty private DB — the
    /// state-transition step that runs AFTER the irreversible GitHub/local work.
    #[tokio::test(flavor = "current_thread")]
    async fn resolve_pr_node_close_routes_to_owning_team_replica() {
        use crate::models::{
            ExecutionSnapshot, RecipeSnapshot, RecipeTrigger, TriggerContext, TriggerType,
        };
        use crate::pr_data::actions::test_support::test_orchestrator;
        use crate::services::testing::MockGitClient;
        use std::collections::HashMap;
        use std::sync::Arc;

        let orch = test_orchestrator(migrated_db().await, MockGitClient::new());
        let team = Arc::new(migrated_db().await);
        orch.db.insert_team_db_for_test("team1", team.clone()).await;

        let exec_id = "team1~00000000-0000-4000-8000-0000000000e0";
        let job_id = "team1~00000000-0000-4000-8000-0000000000e1";
        let issue_id = "team1~00000000-0000-4000-8000-0000000000e2";
        let mr_id = "team1~00000000-0000-4000-8000-0000000000e3";
        let snapshot = ExecutionSnapshot::new(
            RecipeSnapshot {
                id: "recipe-1".into(),
                name: "R".into(),
                description: None,
                trigger: RecipeTrigger::Manual,
                nodes: Vec::new(),
                edges: Vec::new(),
            },
            HashMap::new(),
            HashMap::new(),
            TriggerContext {
                issue_id: Some(issue_id.to_string()),
                project_id: "proj-t".to_string(),
                trigger_type: TriggerType::Manual,
                event_payload: None,
                initiated_via: None,
            },
        )
        .to_json()
        .unwrap();

        // Seed the full close fixture in the TEAM replica ONLY; the private DB
        // stays empty, so a resolution that read `orch.db.local` would error.
        {
            let exec_id = exec_id.to_string();
            let job_id = job_id.to_string();
            let issue_id = issue_id.to_string();
            let mr_id = mr_id.to_string();
            team.write(|conn| {
                let exec_id = exec_id.clone();
                let job_id = job_id.clone();
                let issue_id = issue_id.clone();
                let mr_id = mr_id.clone();
                let snapshot = snapshot.clone();
                Box::pin(async move {
                    conn.execute(
                        "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
                         VALUES ('proj-t', 'default', 'P', 'proj', '/repo', 'main', 1, 1)",
                        (),
                    )
                    .await?;
                    conn.execute(
                        "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
                         VALUES (?1, 'proj-t', 1, 'Issue', 'active', 1, 1)",
                        params![issue_id.as_str()],
                    )
                    .await?;
                    conn.execute(
                        "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, snapshot, started_at, seq)
                         VALUES (?1, 'recipe-1', ?2, 'proj-t', 'running', ?3, 1, 1)",
                        params![exec_id.as_str(), issue_id.as_str(), snapshot.as_str()],
                    )
                    .await?;
                    conn.execute(
                        "INSERT INTO jobs (id, execution_id, issue_id, project_id, status, branch, created_at, updated_at)
                         VALUES (?1, ?2, ?3, 'proj-t', 'complete', 'feature', 1, 1)",
                        params![job_id.as_str(), exec_id.as_str(), issue_id.as_str()],
                    )
                    .await?;
                    conn.execute(
                        "INSERT INTO merge_requests (id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at)
                         VALUES (?1, ?2, 'proj-t', ?3, 'Team PR', 'feature', 'main', 'open', 1, 1)",
                        params![mr_id.as_str(), job_id.as_str(), issue_id.as_str()],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();
        }

        resolve_pr_node(&orch, job_id, PrNodeResolution::Close, None, None)
            .await
            .expect("a team PR resolves its close transition against the injected replica");

        // The transition landed in the TEAM replica, not the (empty) private DB.
        let team_status = team
            .query_text(
                "SELECT status FROM merge_requests WHERE id = ?1",
                params![mr_id],
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(team_status, "closed");
        let private_status = orch
            .db
            .local
            .query_opt_text(
                "SELECT status FROM merge_requests WHERE id = ?1",
                params![mr_id],
            )
            .await
            .unwrap();
        assert!(
            private_status.is_none(),
            "the private DB must never receive the team PR's row"
        );
    }
}
