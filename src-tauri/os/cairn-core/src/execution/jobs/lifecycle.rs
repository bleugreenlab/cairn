use super::*;

use crate::sessions::queries::RotationReason;
use crate::threads::compaction::CompactionTrigger;
use crate::threads::ThreadCompaction;
use cairn_models::continuation::{
    decide_continue_action, prefix_cache_state, price_cold_rebuild, reseed_step,
    window_pressure_reached, CacheDecision, ColdRebuild, ContinueAction, PrefixCacheState,
    ReseedStep, ResumeBackend, COLD_RESEED_MIN_SAVINGS_RATIO,
};

// ============================================================================
// on_job_complete_impl
// ============================================================================

#[derive(Debug)]
enum PushJobCoordinate {
    Node {
        project: String,
        number: i32,
        exec_seq: i32,
        node: String,
    },
    Task {
        project: String,
        number: i32,
        exec_seq: i32,
        node: String,
        task: String,
    },
}

/// Deliver an already-prepared turn to a process that became warm after the
/// cold launch decision. The caller owns the job launch lock and the durable
/// turn/event; this is the same occupancy-before-stdin continuation boundary as
/// ordinary warm reuse, without allocating another run, turn, or launch claim.
pub(super) fn deliver_prepared_warm_continuation(
    orch: &Orchestrator,
    owning_db: &Arc<LocalDb>,
    run_id: &str,
    session_id: &str,
    turn_id: &str,
    project_id: &str,
    prompt: String,
) -> Result<(), String> {
    orch.process_state.transition_to_active(run_id);
    start_turn(orch, turn_id, run_id)?;
    orch.process_state
        .set_current_turn_id(run_id, Some(turn_id));

    let project_key = run_db({
        let db = owning_db.clone();
        let project_id = project_id.to_string();
        async move {
            db.query_opt_text("SELECT key FROM projects WHERE id = ?1", (project_id,))
                .await
                .map_err(|error| error.to_string())?
                .ok_or_else(|| "resume project authority is unavailable".to_string())
        }
    })?;
    let image_project_id = project_id.to_string();
    let message_content = crate::storage::run_db_blocking(move || async move {
        crate::agent_process::stdin::resolve_stable_images(
            &orch.db,
            &image_project_id,
            &project_key,
            prompt,
        )
        .await
    })?;
    crate::backends::stdin::send_user_message(
        &orch.process_state,
        run_id,
        &message_content,
        session_id,
        None,
    )?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum GoalReactivation {
    Operator {
        kind: &'static str,
        id: Option<String>,
    },
    OwnedConditionResolved(crate::agent_process::process::GoalPauseOwner),
}

/// Evidence from the authoritative jobs row. Only unowned pauses can use it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum JobRunnable {
    Attested,
    Unknown,
}

/// What the authoritative jobs row says about the provider goal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GoalReconciliation {
    Active,
    Paused(crate::agent_process::process::JobPauseState),
    Blocked,
    Complete,
}

impl GoalReactivation {
    pub(crate) fn operator_message(id: &str) -> Self {
        Self::Operator {
            kind: "operator_message",
            id: Some(id.to_string()),
        }
    }

    fn explicit_resume(id: Option<&str>) -> Self {
        Self::Operator {
            kind: "explicit_resume",
            id: id.map(str::to_string),
        }
    }
}

pub(crate) fn goal_pause_notice(cause: &crate::backends::stdin::GoalPauseCause) -> String {
    use crate::agent_process::process::GoalPause;
    use crate::backends::stdin::GoalPauseCause;
    match cause {
        GoalPauseCause::Unattributed => "[Goal state: paused, and Cairn cannot attribute this pause to a known cause. It lifts when this job is admitted to run again.]".to_string(),
        GoalPauseCause::Paused(GoalPause::JobState(state)) => format!(
            "[Goal state: paused because this job is {} and not runnable. It lifts when the job is admitted to run again; this delivery does not force that.]", state.label()),
        GoalPauseCause::Complete => "[Goal state: complete. This delivery does not restart the goal.]".to_string(),
        GoalPauseCause::Paused(GoalPause::OperatorHold) => "[Goal state: paused by an operator Stop. Autonomous continuation stays off until the operator resumes; this delivery does not lift it.]".to_string(),
        GoalPauseCause::Paused(GoalPause::SafetyBrake) => "[Goal state: paused by the repeat safety brake. It lifts on an explicit rearm.]".to_string(),
        GoalPauseCause::Paused(GoalPause::OperatorCompact) => "[Goal state: paused because the operator is compacting this session. This process is being replaced; nothing lifts this pause.]".to_string(),
        GoalPauseCause::Paused(GoalPause::Owned(owner)) => format!(
            "[Goal state: paused pending {} {}. It lifts automatically when that condition resolves; this delivery does not lift it.]",
            owner.kind(), owner.id(),
        ),
    }
}

#[derive(Debug, Clone)]
struct SessionBoundary {
    kind: &'static str,
    from_backend: Option<String>,
    to_backend: Option<String>,
    model: Option<String>,
    seed_bytes: Option<usize>,
    /// Why this continuation cost what it did, when a prompt-cache reading was
    /// part of choosing it. `None` on the ordinary warm continuation, which
    /// keeps that payload byte-identical to what it has always been.
    cache: Option<CacheDecision>,
    /// Who asked for this boundary, when it was not the system's own decision.
    /// Absent everywhere but an operator Compact, so every other payload stays
    /// byte-identical.
    initiated_by: Option<&'static str>,
    /// Why a replacement happened, when its kind alone does not say. `None`
    /// keeps every other boundary's payload byte-identical.
    reason: Option<&'static str>,
}

/// The boundary reason for a session replaced because its first provider
/// launch never confirmed.
const UNCONFIRMED_FIRST_LAUNCH_BOUNDARY_REASON: &str = "unconfirmed_first_launch";

impl SessionBoundary {
    fn provider_change(from: String, to: String, model: Option<&str>, seed_bytes: usize) -> Self {
        Self {
            kind: "provider_change",
            from_backend: Some(from),
            to_backend: Some(to),
            model: model.map(str::to_string),
            seed_bytes: Some(seed_bytes),
            cache: None,
            initiated_by: None,
            reason: None,
        }
    }

    fn digest_reseed(backend: String, seed_bytes: usize) -> Self {
        Self {
            kind: "digest_reseed",
            from_backend: Some(backend.clone()),
            to_backend: Some(backend),
            model: None,
            seed_bytes: Some(seed_bytes),
            cache: None,
            initiated_by: None,
            reason: None,
        }
    }

    /// Mark a boundary the operator asked for, as distinct from one a
    /// continuation decided on.
    fn initiated_by_operator(mut self) -> Self {
        self.initiated_by = Some("operator");
        self
    }

    fn native_resume(model: Option<&str>, backend: Option<&str>) -> Self {
        Self {
            kind: "native_resume_new_process",
            from_backend: backend.map(str::to_string),
            to_backend: backend.map(str::to_string),
            model: model.map(str::to_string),
            seed_bytes: None,
            cache: None,
            initiated_by: None,
            reason: None,
        }
    }

    fn fresh(model: Option<&str>, backend: Option<&str>) -> Self {
        Self {
            kind: "fresh",
            from_backend: None,
            to_backend: backend.map(str::to_string),
            model: model.map(str::to_string),
            seed_bytes: None,
            cache: None,
            initiated_by: None,
            reason: None,
        }
    }

    /// Attach the cache reading that chose this continuation. Taken as an
    /// `Option` so the caller threads one variable through both the rebuild and
    /// the native-resume construction sites, which is what keeps the label and
    /// the detail from being set by different branches.
    fn with_cache(mut self, cache: Option<CacheDecision>) -> Self {
        self.cache = cache;
        self
    }

    fn with_reason(mut self, reason: &'static str) -> Self {
        self.reason = Some(reason);
        self
    }

    fn raw(&self) -> serde_json::Value {
        let mut raw = serde_json::json!({
            "kind": self.kind,
            "fromBackend": self.from_backend,
            "toBackend": self.to_backend,
            "model": self.model,
            "seedBytes": self.seed_bytes,
        });
        if let (Some(initiated_by), Some(object)) = (self.initiated_by, raw.as_object_mut()) {
            object.insert("initiatedBy".to_string(), initiated_by.into());
        }
        if let (Some(reason), Some(object)) = (self.reason, raw.as_object_mut()) {
            object.insert("reason".to_string(), reason.into());
        }
        if let (Some(cache), Some(object)) = (self.cache, raw.as_object_mut()) {
            object.insert("decision".to_string(), cache.decision.into());
            object.insert("cacheState".to_string(), cache.cache_state.into());
            if let Some(skipped) = cache.reseed_skipped {
                object.insert("reseedSkipped".to_string(), skipped.into());
            }
            if let Some(bytes) = cache.prefix_bytes {
                object.insert("prefixBytes".to_string(), bytes.into());
            }
            if let Some(bytes) = cache.candidate_seed_bytes {
                object.insert("candidateSeedBytes".to_string(), bytes.into());
            }
            if let Some(threshold) = cache.threshold {
                object.insert("savingsThreshold".to_string(), threshold.into());
            }
        }
        raw
    }
}

fn adopt_successor_turn(
    db: std::sync::Arc<crate::storage::LocalDb>,
    turn_id: &str,
    session_id: &str,
) -> Result<(), String> {
    let turn_id = turn_id.to_string();
    let session_id = session_id.to_string();
    run_db(async move {
        let changed = db
            .execute(
                "UPDATE turns
                    SET session_id = ?1,
                        sequence = (SELECT COALESCE(MAX(sequence), 0) + 1 FROM turns WHERE session_id = ?1),
                        updated_at = ?2
                  WHERE id = ?3 AND state = 'pending'",
                cairn_db::turso::params![
                    session_id.clone(),
                    chrono::Utc::now().timestamp(),
                    turn_id.clone()
                ],
            )
            .await
            .map_err(|error| error.to_string())?;
        if changed == 1 {
            return Ok(());
        }
        let state = db
            .query_opt_text("SELECT state FROM turns WHERE id = ?1", (turn_id.clone(),))
            .await
            .map_err(|error| error.to_string())?
            .unwrap_or_else(|| "missing".to_string());
        Err(format!(
            "successor turn {turn_id} could not be adopted into session {session_id}: it was {state} before launch (the previous run may have exited while session rotation was in progress)"
        ))
    })
}

fn emit_resume_step(
    event_name: &'static str,
    started: std::time::Instant,
    resume_id: &str,
    job_id: &str,
) {
    let mut event = crate::resume_timing::ResumeTimingEvent::new(event_name);
    event.step_us = Some(started.elapsed().as_micros());
    event.resume_id = Some(resume_id);
    event.job_id = Some(job_id);
    event.emit();
}

/// Restores early queue claims when launch preparation fails before their
/// canonical user events are stored. Successfully persisted rows are removed
/// one by one so a partial event-storage failure never duplicates them.
struct ClaimedMessagesLaunchGuard<'a> {
    orch: &'a Orchestrator,
    db: std::sync::Arc<crate::storage::LocalDb>,
    pending_ids: Vec<String>,
}

impl ClaimedMessagesLaunchGuard<'_> {
    fn persisted(&mut self, id: &str) {
        self.pending_ids.retain(|pending| pending != id);
    }
}

impl Drop for ClaimedMessagesLaunchGuard<'_> {
    fn drop(&mut self) {
        if self.pending_ids.is_empty() {
            return;
        }
        if let Err(error) = crate::messages::queued::restore_claimed(&self.db, &self.pending_ids) {
            log::error!("failed to restore queued operator messages after launch failure: {error}");
        } else {
            log::warn!(
                "launch failed after queued operator messages were claimed; restored {} pending messages",
                self.pending_ids.len()
            );
        }
        let _ = self.orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "queued_messages", "action": "update"}),
        );
    }
}

/// Owns a newly-created successor until it is attached to a run. Any `?` in
/// launch preparation therefore terminalizes the pending head instead of
/// leaving the job permanently unsendable.
struct PendingLaunchTurnGuard<'a> {
    orch: &'a Orchestrator,
    db: std::sync::Arc<crate::storage::LocalDb>,
    turn_id: Option<String>,
    run_id: Option<String>,
    inserted_run_id: Option<String>,
    /// Set once this launch has written the job `running`. A launch refused
    /// after that point must not leave the job resting on a status only the
    /// launch asserted, so the drop re-derives it.
    wrote_running_job_id: Option<String>,
    /// A preclaimed successor this launch started, and the end reason its caller
    /// named for it (`ResumeContext::unwind_preclaimed_start_as`). A launch that
    /// fails before delivering it settles it `cancelled` here, while the job's
    /// launch lock is still held.
    unwind_preclaimed: Option<(String, String)>,
}

impl PendingLaunchTurnGuard<'_> {
    fn started(&mut self, run_id: &str) {
        if self.turn_id.is_some() {
            self.run_id = Some(run_id.to_string());
        }
    }

    /// The occupancy a launch put on the process for a preclaimed successor
    /// (owned-wait or permission) is the guard's to undo if the launch then
    /// fails: a process that never received its prompt must not read as serving
    /// the turn. The turn itself stays the caller's, unless the caller named an
    /// end reason to unwind it with; then the guard settles it too, so no lock
    /// holder ever observes a started-but-undelivered preclaimed turn.
    fn started_preclaimed(
        &mut self,
        run_id: &str,
        turn_id: &str,
        unwind_as: Option<&crate::models::TurnEndReason>,
    ) {
        self.run_id = Some(run_id.to_string());
        self.unwind_preclaimed = unwind_as.map(|reason| (turn_id.to_string(), reason.to_string()));
    }

    fn inserted_run(&mut self, run_id: &str) {
        self.inserted_run_id = Some(run_id.to_string());
    }

    fn wrote_running(&mut self, job_id: &str) {
        self.wrote_running_job_id = Some(job_id.to_string());
    }

    fn adopted(&mut self) {
        self.turn_id = None;
        self.run_id = None;
        self.inserted_run_id = None;
        self.wrote_running_job_id = None;
        self.unwind_preclaimed = None;
    }
}

impl Drop for PendingLaunchTurnGuard<'_> {
    fn drop(&mut self) {
        let turn_id = self.turn_id.take();
        if let Some(run_id) = self.run_id.take() {
            self.orch.process_state.transition_to_warm(&run_id);
        }
        let now = chrono::Utc::now().timestamp();
        if let Some((turn_id, end_reason)) = self.unwind_preclaimed.take() {
            let result = run_db({
                let db = self.db.clone();
                let turn_id = turn_id.clone();
                async move {
                    db.execute(
                        "UPDATE turns SET state = 'cancelled', end_reason = ?1, ended_at = ?2, updated_at = ?2 WHERE id = ?3 AND state = 'running'",
                        cairn_db::turso::params![end_reason.as_str(), now, turn_id.as_str()],
                    )
                    .await
                    .map_err(|error| error.to_string())
                }
            });
            if let Err(error) = result {
                log::error!(
                    "failed to settle preclaimed successor {turn_id} after launch failure: {error}"
                );
            }
            let _ = self.orch.services.emitter.emit(
                "db-change",
                serde_json::json!({"table": "turns", "action": "update", "turnId": turn_id}),
            );
        }
        if let Some(turn_id) = turn_id {
            let result = run_db({
                let db = self.db.clone();
                let turn_id = turn_id.clone();
                async move {
                    db.execute(
                    "UPDATE turns SET state = 'cancelled', end_reason = 'launch_refused', ended_at = ?1, updated_at = ?1 WHERE id = ?2 AND state IN ('pending', 'running')",
                    cairn_db::turso::params![now, turn_id.as_str()],
                )
                .await
                .map_err(|error| error.to_string())
                }
            });
            if let Err(error) = result {
                log::error!("failed to cancel pending successor after launch failure: {error}");
            }
            let _ = self.orch.services.emitter.emit(
                "db-change",
                serde_json::json!({"table": "turns", "action": "update", "turnId": turn_id}),
            );
        }
        if let Some(run_id) = self.inserted_run_id.take() {
            crate::orchestrator::lifecycle::release_and_stop_process(self.orch, &run_id);
            if let Err(error) = crate::orchestrator::lifecycle::set_exit_reason(
                self.orch,
                &run_id,
                "launch_attach_failed",
            ) {
                log::error!("failed to record launch attachment failure for run {run_id}: {error}");
            }
            crate::orchestrator::lifecycle::finalize_run(
                self.orch,
                &run_id,
                crate::models::RunStatus::Crashed,
            );
        }
        if let Some(job_id) = self.wrote_running_job_id.take() {
            if let Err(error) = crate::execution::advancement::recompute_job(self.orch, &job_id) {
                log::error!("failed to re-derive job {job_id} after a refused launch: {error}");
            }
        }
    }
}

/// The operator-facing refusal for a session that can never take another turn,
/// or `None` when it can.
///
/// A closed or failed session is not something the caller can retry past, so
/// the message names what WOULD work instead of reporting an internal session
/// state: an operator who reached this while trying to intervene on a merged
/// issue got only "Session cf7a2f00 is Closed and cannot be continued", which
/// told them nothing they could act on (CAIRN-3253).
///
/// The one closed session that can still take a turn is the legacy
/// already-terminal reconciliation, which a launch replaces with a fresh session.
fn closed_session_refusal(session: &Session) -> Option<String> {
    match session.status {
        SessionStatus::Open => None,
        SessionStatus::Closed
            if session.terminal_reason.as_deref()
                == Some(crate::orchestrator::lifecycle::ALREADY_TERMINAL_RECONCILED_REASON) =>
        {
            None
        }
        SessionStatus::Closed => Some(match session.terminal_reason.as_deref() {
            Some("issue_merged") => "This node's issue has been merged, which stopped its work and closed its session, so it cannot take another turn. To pick the work back up, reopen the issue (set its status to backlog) and start a new execution on it.".to_string(),
            Some("issue_closed") => "This node's issue has been closed, which stopped its work and closed its session, so it cannot take another turn. To pick the work back up, reopen the issue (set its status to backlog) and start a new execution on it.".to_string(),
            Some("node_removed") => "This node was removed from its execution, which closed its session, so it cannot take another turn. Add the node back to the execution to run it again.".to_string(),
            _ => "This node's session has been closed, so it cannot take another turn. Start a new execution on the issue to carry the work forward.".to_string(),
        }),
        SessionStatus::Failed => Some("This node's session ended in a failure, so it cannot take another turn. Start a new execution on the issue to carry the work forward.".to_string()),
    }
}

/// Whether a job's current session refuses every further turn. Read without
/// side effects so a delivery path can decline to nudge a job no launch could
/// serve. A read failure answers `false`: the launch funnel refuses on its own.
pub(crate) async fn current_session_refuses_turns(
    db: &crate::storage::LocalDb,
    job_id: &str,
) -> bool {
    let session_id = match db
        .query_opt_text(
            "SELECT current_session_id FROM jobs WHERE id = ?1",
            (job_id.to_string(),),
        )
        .await
    {
        Ok(Some(session_id)) => session_id,
        _ => return false,
    };
    let session = db
        .read(|conn| {
            let session_id = session_id.clone();
            Box::pin(async move { super::persistence::load_session_conn(conn, &session_id).await })
        })
        .await;
    matches!(session, Ok(Some(session)) if closed_session_refusal(&session).is_some())
}

fn push_job_coordinate(content_ref: &str) -> Option<PushJobCoordinate> {
    use cairn_common::uri::CairnResource;
    match cairn_common::uri::parse_uri(content_ref)? {
        CairnResource::Node {
            project,
            number,
            exec_seq,
            node_id,
        }
        | CairnResource::NodeChat {
            project,
            number,
            exec_seq,
            node_id,
        }
        | CairnResource::NodeChatRaw {
            project,
            number,
            exec_seq,
            node_id,
        }
        | CairnResource::NodeArtifact {
            project,
            number,
            exec_seq,
            node_id,
            ..
        }
        | CairnResource::NodeDiff {
            project,
            number,
            exec_seq,
            node_id,
        }
        | CairnResource::NodeTasks {
            project,
            number,
            exec_seq,
            node_id,
        }
        | CairnResource::NodeCalls {
            project,
            number,
            exec_seq,
            node_id,
        }
        | CairnResource::NodeWakes {
            project,
            number,
            exec_seq,
            node_id,
        }
        | CairnResource::NodeChecks {
            project,
            number,
            exec_seq,
            node_id,
        }
        | CairnResource::NodeQuestions {
            project,
            number,
            exec_seq,
            node_id,
        }
        | CairnResource::NodeQuestion {
            project,
            number,
            exec_seq,
            node_id,
            ..
        }
        | CairnResource::NodePermissions {
            project,
            number,
            exec_seq,
            node_id,
        }
        | CairnResource::NodePermission {
            project,
            number,
            exec_seq,
            node_id,
            ..
        }
        | CairnResource::NodeMessages {
            project,
            number,
            exec_seq,
            node_id,
        }
        | CairnResource::NodeProgress {
            project,
            number,
            exec_seq,
            node_id,
        }
        | CairnResource::NodeMemories {
            project,
            number,
            exec_seq,
            node_id,
        }
        | CairnResource::NodeSymbols {
            project,
            number,
            exec_seq,
            node_id,
            ..
        } => Some(PushJobCoordinate::Node {
            project,
            number,
            exec_seq,
            node: node_id,
        }),
        CairnResource::Task {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
        }
        | CairnResource::TaskChat {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
        }
        | CairnResource::TaskChatRaw {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
        }
        | CairnResource::TaskArtifact {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
            ..
        }
        | CairnResource::TaskChecks {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
        }
        | CairnResource::TaskPermissions {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
        }
        | CairnResource::TaskPermission {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
            ..
        }
        | CairnResource::TaskMessages {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
        } => Some(PushJobCoordinate::Task {
            project,
            number,
            exec_seq,
            node: node_id,
            task: task_name,
        }),
        CairnResource::JobTodos {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
        } => Some(match task_name {
            Some(task) => PushJobCoordinate::Task {
                project,
                number,
                exec_seq,
                node: node_id,
                task,
            },
            None => PushJobCoordinate::Node {
                project,
                number,
                exec_seq,
                node: node_id,
            },
        }),
        _ => None,
    }
}

async fn push_job_coordinate_exists(
    db: &LocalDb,
    coordinate: PushJobCoordinate,
) -> Result<bool, String> {
    db.read(|conn| {
        Box::pin(async move {
            let exists = match coordinate {
                PushJobCoordinate::Node {
                    project,
                    number,
                    exec_seq,
                    node,
                } => {
                    let mut rows = conn
                        .query(
                            "SELECT EXISTS(
                           SELECT 1 FROM jobs j
                           JOIN projects p ON p.id = j.project_id
                           JOIN issues i ON i.id = j.issue_id
                           JOIN executions e ON e.id = j.execution_id
                           WHERE p.key = ?1 AND i.number = ?2 AND e.seq = ?3
                             AND j.uri_segment = ?4
                             AND (j.parent_job_id IS NULL OR j.agent_config_id = 'workflow')
                         )",
                            params![project, number as i64, exec_seq as i64, node],
                        )
                        .await?;
                    rows.next()
                        .await?
                        .is_some_and(|row| row.i64(0).unwrap_or(0) != 0)
                }
                PushJobCoordinate::Task {
                    project,
                    number,
                    exec_seq,
                    node,
                    task,
                } => {
                    let mut rows = conn
                        .query(
                            "SELECT EXISTS(
                           SELECT 1 FROM jobs j
                           JOIN jobs parent ON parent.id = j.parent_job_id
                           JOIN projects p ON p.id = j.project_id
                           JOIN issues i ON i.id = j.issue_id
                           JOIN executions e ON e.id = j.execution_id
                           WHERE p.key = ?1 AND i.number = ?2 AND e.seq = ?3
                             AND parent.uri_segment = ?4 AND j.uri_segment = ?5
                             AND j.agent_config_id != 'workflow'
                         )",
                            params![project, number as i64, exec_seq as i64, node, task],
                        )
                        .await?;
                    rows.next()
                        .await?
                        .is_some_and(|row| row.i64(0).unwrap_or(0) != 0)
                }
            };
            Ok(exists)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

async fn retain_resolvable_pushes(
    db: &LocalDb,
    recipient: &str,
    pushes: Vec<crate::orchestrator::attention_push::Push>,
) -> Vec<crate::orchestrator::attention_push::Push> {
    use futures_util::{stream, StreamExt};

    let mut retained = Vec::with_capacity(pushes.len());
    let mut probed = stream::iter(pushes.into_iter().enumerate())
        .map(|(index, push)| async move {
            let exists = match push_job_coordinate(&push.content_ref) {
                Some(coordinate) => push_job_coordinate_exists(db, coordinate).await,
                None => Ok(true),
            };
            (index, push, exists)
        })
        .buffer_unordered(16)
        .collect::<Vec<_>>()
        .await;
    probed.sort_by_key(|(index, _, _)| *index);

    for (_, push, exists) in probed {
        match exists {
            Ok(true) => retained.push(push),
            Ok(false) => {
                match crate::orchestrator::attention_push::delete_pending_by_id(db, &push.id).await {
                    Ok(()) => log::warn!(
                        "dropping unresolved attention push: recipient={} id={} key={} content_ref={}",
                        recipient,
                        push.id,
                        push.key,
                        push.content_ref
                    ),
                    Err(error) => {
                        log::warn!(
                            "failed to delete unresolved attention push {}; delivering fail-open: {}",
                            push.id,
                            error
                        );
                        retained.push(push);
                    }
                }
            }
            Err(error) => {
                log::warn!(
                    "attention push coordinate probe failed for {} ({}); delivering fail-open: {}",
                    push.id,
                    push.content_ref,
                    error
                );
                retained.push(push);
            }
        }
    }
    retained
}

/// Called when a job finishes. Advances the execution DAG if applicable.
///
/// Only advances for jobs that are part of a recipe DAG (have both `execution_id`
/// and `recipe_node_id`). Rows missing either field are not runnable DAG jobs and
/// are ignored here.
pub async fn on_job_complete_impl(orch: &Orchestrator, job_id: &str) -> Result<Vec<Job>, String> {
    // Tear down any external MCP gateway connections this job opened
    // (cairn://mcp/... family). Connections are pooled per job id, so closing
    // here releases the per-session server processes (e.g. Playwright browsers).
    if let Some(gateway) = orch.mcp_gateway() {
        gateway.close_session(job_id).await;
    }

    let job_id = job_id.to_string();
    let db = crate::execution::routing::owning_db_for_job(&orch.db, &job_id)
        .await
        .map_err(|e| e.to_string())?;
    let (execution_id, recipe_node_id): (Option<String>, Option<String>) = run_db(async move {
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT execution_id, recipe_node_id FROM jobs WHERE id = ?1",
                        (job_id.as_str(),),
                    )
                    .await?;
                let row = rows
                    .next()
                    .await?
                    .ok_or_else(|| DbError::Row("job not found".to_string()))?;
                Ok((row.opt_text(0)?, row.opt_text(1)?))
            })
        })
        .await
        .map_err(|e| db_error("Job not found", e))
    })?
    .clone();
    match execution_id {
        Some(exec_id) if recipe_node_id.is_some() => {
            crate::execution::advancement::advance_execution_with_actions(orch, &exec_id).await
        }
        _ => Ok(vec![]), // Not a runnable DAG job, so there is no DAG to advance.
    }
}

// ============================================================================
// prepare_job
// ============================================================================

/// Where a job that inherits its parent's branch starts, resolved live-first and
/// never failing.
///
/// The store is the authority: the parent's bookmark is what the branch
/// currently means, and it is the only rung of this ladder that is a coordinate
/// rather than a memory of one.
///
/// Below it sits the parent's recorded `jobs.base_commit`. That row is
/// bookkeeping, not a coordinate (CAIRN-3224), and the standing operator ruling
/// forbids failing a spawn over substrate state — so it stays as a fallback, but
/// only when the store can still produce the commit it names. An unverified
/// recorded commit is the worse failure of the two: minting a job at a commit
/// the store cannot resolve does not fail here, it fails later inside
/// materialization, far from the row that caused it.
///
/// The floor is the job's own base branch, resolved live. `resolve_base_rev`
/// always yields something — down to jj's root commit on an unborn repository —
/// so a parent whose branch has vanished entirely degrades to a real coordinate
/// instead of refusing to start.
fn inherited_head<F>(
    jj: &crate::jj::JjEnv,
    store: &Path,
    branch: &str,
    recorded_base: Option<&str>,
    base_ref: &str,
    git_rev_parse: F,
) -> String
where
    F: Fn(&str) -> Option<String>,
{
    if let Some(live) = crate::jj::bookmark_commit(jj, store, branch) {
        return live;
    }
    if let Some(recorded) = recorded_base {
        if crate::jj::revset_resolves(jj, store, recorded) {
            log::warn!(
                "[prepare_job] inherited branch {branch} does not resolve in the store; starting \
                 from the parent's recorded base {recorded}, which the store can still produce"
            );
            return recorded.to_string();
        }
        log::warn!(
            "[prepare_job] inherited branch {branch} does not resolve in the store, and the \
             parent's recorded base {recorded} does not resolve either; falling back to \
             {base_ref}"
        );
    } else {
        log::warn!(
            "[prepare_job] inherited branch {branch} does not resolve in the store and the parent \
             has no recorded base; falling back to {base_ref}"
        );
    }
    crate::jj::resolve_base_rev(jj, store, base_ref, git_rev_parse)
}

/// The parent's durable coordinate, as an inheriting child needs to see it.
pub(crate) struct ParentCoordinate {
    /// The branch the child continues. Its absence preserves a deliberately
    /// branchless parent's base coordinate.
    pub branch: Option<String>,
    /// The parent's archival `jobs.base_commit`, the verified fallback rung of
    /// [`inherited_head`] — never the primary coordinate.
    pub recorded_base: Option<String>,
}

/// The job facts coordinate selection reads, without the rest of the row.
pub(crate) struct CoordinateRequest<'a> {
    pub job_id: &'a str,
    pub parent_job_id: Option<&'a str>,
    /// A branch already recorded on the row — pre-assigned lineage, honored
    /// instead of minting a fresh name.
    pub existing_branch: Option<&'a str>,
    /// The job's own base branch, the floor every non-inheriting job starts from.
    pub base_ref: &'a str,
    /// The committed head a fresh mint starts at INSTEAD of the base branch,
    /// resolved by [`crate::execution::continuity`] when an earlier execution of
    /// the same issue left reachable commits. `None` mints at the base, which is
    /// what a first execution and every branchless mode always do.
    pub adopt_head: Option<&'a str>,
}

/// Select the durable coordinate a job starts from, one outcome per branch mode.
///
/// `inherit` is the only mode that reads another job. A branch-bearing child
/// continues its parent's branch at its **live head**. When the parent is
/// deliberately branchless, the child preserves that mode and starts from the
/// live base ref recorded on its own row by the delegation edge.
///
/// Two grades of inheritance, and the difference is what happens when the store
/// cannot produce that head. Plain `inherit` degrades through
/// [`inherited_head`]'s ladder, which always yields a coordinate. With
/// `requires_parent_head` it refuses instead: nothing below the parent's live
/// bookmark is the parent's work, so a child seeded from a recorded base or from
/// the base branch is doing confidently wrong work rather than degraded work.
/// Delegated tasks take the strict grade (CAIRN-3309). Both refusals — no
/// lineage, and a branch the store cannot resolve — name the child, the parent,
/// and what is missing, so a broken edge is legible without opening this file.
///
/// `isolate` mints a child branch; `none` stays on the base coordinate with no
/// branch of its own. Both resolve base live, and neither consults a parent.
///
/// A mint is created at `request.adopt_head` when one is given and at the base
/// otherwise. That is the whole of adoption at this layer: which prior head is
/// the latest, whether carrying it is safe, and what the successor is told about
/// it all belong to [`crate::execution::continuity`], which resolves the answer
/// before this is called. The recorded base stays the base either way — see the
/// note at the mint itself.
///
/// The DB-shaped work is passed in: `load_parent` fetches the parent's
/// coordinate, and `mint_name` derives a fresh branch name (it is only called
/// when a mint actually needs one). That keeps the whole decision testable
/// against a real jj store without an orchestrator.
pub(crate) fn select_job_coordinate<P, M, F>(
    behavior: &crate::execution::step_behavior::StepBehavior,
    request: CoordinateRequest<'_>,
    jj: &crate::jj::JjEnv,
    store: &Path,
    load_parent: P,
    mint_name: M,
    git_rev_parse: F,
) -> Result<(Option<String>, Option<String>), String>
where
    P: FnOnce(&str) -> Result<ParentCoordinate, String>,
    M: FnOnce() -> Result<String, String>,
    F: Fn(&str) -> Option<String>,
{
    if behavior.inherits_branch {
        let job_id = request.job_id;
        let parent_job_id = request.parent_job_id.ok_or_else(|| {
            format!(
                "Job {job_id} inherits its parent's branch but has no parent_job_id: \
                 the delegation edge that created it never recorded lineage"
            )
        })?;
        let parent = load_parent(parent_job_id)?;
        let Some(branch) = parent.branch else {
            let base = crate::jj::resolve_base_rev(jj, store, request.base_ref, git_rev_parse);
            return Ok((None, Some(base)));
        };
        if behavior.requires_parent_head {
            let head = crate::jj::bookmark_commit(jj, store, &branch).ok_or_else(|| {
                format!(
                    "Job {job_id} cannot start: the branch {branch} it inherits from parent job \
                     {parent_job_id} does not resolve in the store, so its logical head cannot \
                     be seeded. Starting from the base branch would run this job against code \
                     its parent has already moved past."
                )
            })?;
            return Ok((Some(branch), Some(head)));
        }
        let head = inherited_head(
            jj,
            store,
            &branch,
            parent.recorded_base.as_deref(),
            request.base_ref,
            git_rev_parse,
        );
        return Ok((Some(branch), Some(head)));
    }

    let base = crate::jj::resolve_base_rev(jj, store, request.base_ref, git_rev_parse);
    if !behavior.mints_branch {
        return Ok((None, Some(base)));
    }

    let branch = match request.existing_branch {
        Some(branch) => branch.to_string(),
        None => mint_name()?,
    };
    // Where the new bookmark is CREATED, which is the base unless a prior
    // execution of this issue left committed work to carry forward.
    let mint_point = request.adopt_head.unwrap_or(base.as_str());
    match crate::jj::bookmark_commit(jj, store, &branch) {
        Some(existing) if existing != mint_point => {
            return Err(format!(
                "Branch {branch} already exists at a different commit"
            ));
        }
        Some(_) => {}
        None => crate::jj::create_bookmark_at(jj, store, &branch, mint_point)?,
    }
    // The recorded base stays the BASE even when the mint point is ahead of it.
    // `jobs.base_commit` is the coordinate the job's change is measured against
    // — its diff, its PR, its pack anchor — and adopted commits are part of the
    // change this job ships, not part of the ground it ships onto. Recording the
    // adopted head here instead would hide the inherited work from every one of
    // those surfaces and anchor the job on a commit no default branch reaches.
    Ok((Some(branch), Some(base)))
}

/// Ask [`crate::execution::continuity`] what this branch-minting job should do
/// about committed work earlier executions of the same issue left behind.
///
/// The three halves of the answer live in three places on purpose: the database
/// knows which prior jobs minted branches, the store knows where those branches
/// actually are now, and the policy — which head is the latest, what counts as
/// superseded, when there is no single answer — is pure and testable without
/// either. This function is only the seam that joins them.
fn resolve_continuation<F>(
    orch: &Orchestrator,
    jj: &crate::jj::JjEnv,
    store: &Path,
    job: &DbJob,
    base_ref: &str,
    git_rev_parse: F,
) -> Result<crate::execution::continuity::Continuation, String>
where
    F: Fn(&str) -> Option<String>,
{
    use crate::execution::continuity::{self, Continuation};

    let (Some(issue_id), Some(execution_id)) =
        (job.issue_id.as_deref(), job.execution_id.as_deref())
    else {
        // A project-level run belongs to no issue, so there is no series of
        // executions for it to be the successor in.
        return Ok(Continuation::NotApplicable);
    };

    let owning_db = run_db({
        let dbs = orch.db.clone();
        let job_id = job.id.clone();
        async move {
            crate::execution::routing::owning_db_for_job(&dbs, &job_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?
    .clone();

    // The mode is read BEFORE anything is short-circuited. A caller that named a
    // coordinate asked a question, and the only honest answers are the head it
    // asked for or a refusal that says why not — minting ordinary work at the
    // base and saying nothing is neither.
    let mode = run_db(load_execution_continuity(
        owning_db.clone(),
        execution_id.to_string(),
    ))?;
    let candidates = run_db({
        let db = owning_db.clone();
        let issue_id = issue_id.to_string();
        let execution_id = execution_id.to_string();
        async move {
            continuity::prior_branch_bearing_jobs(&db, &issue_id, &execution_id)
                .await
                .map_err(|e| db_error("Failed to enumerate prior executions", e))
        }
    })?;
    // Only an UNASKED question may be answered without consulting the store.
    // Every other mode with nothing enumerated still resolves to `Fresh` inside
    // `decide`; this path exists so a first execution does not pay for a base
    // resolution it has no use for.
    if candidates.is_empty() && !matches!(mode, crate::models::Continuity::From(_)) {
        return Ok(Continuation::Fresh { prior: Vec::new() });
    }

    let base = crate::jj::resolve_base_rev(jj, store, base_ref, git_rev_parse);
    let prior = continuity::resolve_heads(jj, store, candidates);
    let continuation = continuity::decide(&mode, &base, prior, |tip, dest| {
        crate::jj::revset_descends_from(jj, store, tip, dest)
    });
    log::info!(
        "[continuity] job {} (issue {issue_id}, execution {execution_id}) resolved {mode}: {}",
        job.id,
        match &continuation {
            Continuation::Adopt { adopted, .. } =>
                format!("adopting {} from {}", adopted.head, adopted.branch),
            Continuation::Fresh { prior } =>
                format!("nothing to carry ({} prior branches)", prior.len()),
            Continuation::Declined { prior } => format!("declining {} prior branches", prior.len()),
            Continuation::Undecided { candidates } =>
                format!("refusing: {} unrelated heads", candidates.len()),
            Continuation::Unresolved { coordinate, .. } =>
                format!("refusing: `{coordinate}` names nothing adoptable"),
            Continuation::NotApplicable => "not applicable".to_string(),
        }
    );
    Ok(continuation)
}

/// Where a job's work lives in the shared project store, as job preparation
/// resolved it, and what continuity decided about earlier executions' work.
pub(crate) struct StoreCoordinate {
    pub(crate) branch: Option<String>,
    pub(crate) base_commit: Option<String>,
    pub(crate) continuation: crate::execution::continuity::Continuation,
}

/// Everything job preparation does to the shared project store (the import
/// that syncs it with the project's git, the continuity read, and the branch
/// mint) as one critical section under the per-store lock.
///
/// The lock is what keeps the mint from being silently undone. A store writer
/// that fails partway (a base-advance rebase that conflicts, a merge fold, a
/// seal) rolls back by restoring the store's local view to the operation it
/// snapshotted, which rewinds every local bookmark at once. That restore is an
/// exact undo only when every local-view write since the snapshot was the lock
/// holder's own (see [`crate::jj::restore_operation`]). A mint made beside such
/// a writer is erased by its rollback, and the job then runs against a branch
/// that no longer exists: on cairn/4897 the builder's branch was created while
/// a conflicting sibling rebase held the lock, the rebase rolled back, and every
/// read, write, and command the session made afterwards failed to resolve it.
#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_store_coordinate<F, P, M>(
    orch: &Orchestrator,
    jj: &crate::jj::JjEnv,
    store: &Path,
    repo_path: &Path,
    job: &DbJob,
    behavior: &crate::execution::step_behavior::StepBehavior,
    git_rev_parse: F,
    load_parent: P,
    mint_name: M,
) -> Result<StoreCoordinate, String>
where
    F: Fn(&str) -> Option<String> + Clone,
    P: FnOnce(&str) -> Result<ParentCoordinate, String>,
    M: FnOnce() -> Result<String, String>,
{
    let _store_guard = run_db_borrowed(async {
        Ok(orch
            .acquire_jj_store_lock(store, format!("branch preparation for job {}", job.id))
            .await)
    })?;
    crate::jj::ensure_project_store(jj, store, repo_path)?;

    let base_ref = job.base_branch.as_deref().unwrap_or("HEAD");

    // Only a job about to MINT a branch has a continuity question: an inheriting
    // child already continues a live branch, and a job that carries a branch on
    // its row is a restart of itself rather than a successor.
    let continuation = if behavior.mints_branch && job.branch.is_none() {
        resolve_continuation(orch, jj, store, job, base_ref, git_rev_parse.clone())?
    } else {
        crate::execution::continuity::Continuation::NotApplicable
    };
    if let Some(refusal) = continuation.refusal() {
        return Err(refusal);
    }

    let (branch, base_commit) = select_job_coordinate(
        behavior,
        CoordinateRequest {
            job_id: &job.id,
            parent_job_id: job.parent_job_id.as_deref(),
            existing_branch: job.branch.as_deref(),
            base_ref,
            adopt_head: continuation.adopt_head(),
        },
        jj,
        store,
        load_parent,
        mint_name,
        git_rev_parse,
    )?;
    Ok(StoreCoordinate {
        branch,
        base_commit,
        continuation,
    })
}

/// Sync the shared store with the project's git under the per-store lock, for a
/// synchronous caller that needs nothing else from the store held.
///
/// The import writes local bookmarks for every local git ref that appeared or
/// moved. Unlocked, an import made inside another writer's rollback window is
/// rewound by that rollback, and the export that follows the restore then
/// deletes or rewinds the very git refs the import had just brought in.
pub(crate) fn import_project_store_locked(
    orch: &Orchestrator,
    jj: &crate::jj::JjEnv,
    store: &Path,
    repo_path: &Path,
    operation: &str,
) -> Result<(), String> {
    let _store_guard = run_db_borrowed(async {
        Ok(orch
            .acquire_jj_store_lock(store, format!("project store import: {operation}"))
            .await)
    })?;
    crate::jj::ensure_project_store(jj, store, repo_path)
}

/// Prepare a job for execution: resolve its durable branch coordinate, create the
/// run record and scratch residence, and store the initial user event. Returns a
/// [`PreparedJob`] with everything the host layer needs to
/// call `start_agent_session`.
///
/// The job status must already be set to `"running"` by the caller before this is
/// invoked (Tauri does this synchronously so the UI sees the change immediately).
pub fn prepare_job(orch: &Orchestrator, job_id: &str) -> Result<PreparedJob, String> {
    // The cold-start funnel inserts a run exactly the way the resume funnel does,
    // so it takes the same per-job launch lock (CAIRN-3283). Admission control
    // for a cold start is the durable compare-and-set claim on `jobs.status` in
    // the transport's `start_job_background`; this lock is what keeps that claim
    // from interleaving with a concurrent resume of the same job.
    let launch_lock = orch.job_launch_lock(job_id);
    let _launch_guard = launch_lock
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    log::info!("[prepare_job] resolving owner for job {job_id}");
    // Resolve the job's owning database ONCE (fail-closed) and thread it through
    // every run/session/turn/event write below: a team job's rows live wholly in
    // its synced replica, so prepare must read and write there, not the private DB.
    let owning_db = run_db({
        let dbs = orch.db.clone();
        let job_id = job_id.to_string();
        async move {
            crate::execution::routing::owning_db_for_job(&dbs, &job_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?
    .clone();
    // ---- Load job -------------------------------------------------------
    let job = run_db(load_job(
        owning_db.clone(),
        job_id.to_string(),
        "Job not found",
    ))?;
    // ---- Load project info ----------------------------------------------
    let (repo_path, project_key) = run_db(load_project_repo_and_key(
        orch.db.clone(),
        job.project_id.clone(),
    ))?;

    // ---- Execution seq (for job-activated event) ------------------------
    let exec_seq = match &job.execution_id {
        Some(exec_id) => run_db(load_execution_seq(owning_db.clone(), exec_id.clone()))?,
        None => None,
    };
    // ---- Display ID (issue number or sequential run counter) ------------
    let display_id = run_db(load_display_id(
        owning_db.clone(),
        job.project_id.clone(),
        job.issue_id.clone(),
    ))?;

    // ---- Determine node behavior ----------------------------------------
    let node_id = job
        .recipe_node_id
        .as_ref()
        .ok_or("Job has no recipe_node_id; standalone jobs are no longer runnable")?;
    let execution_id = job
        .execution_id
        .as_ref()
        .ok_or("Job has recipe node but no execution_id")?;

    let all_nodes = run_db(load_nodes_from_execution(
        owning_db.clone(),
        execution_id.clone(),
    ))?;
    let node_map: HashMap<&str, &DbRecipeNode> =
        all_nodes.iter().map(|n| (n.id.as_str(), n)).collect();

    let node = node_map
        .get(node_id.as_str())
        .ok_or_else(|| format!("Recipe node not found: {}", node_id))?;

    if node.node_type == "action" {
        return Err("Action nodes execute inline during DAG advancement".to_string());
    }
    if node.node_type == "checkpoint" {
        return Err("Checkpoint nodes wait for approval, not session start".to_string());
    }

    let behavior = resolve_node_behavior(node);
    let (mints_branch, inherits_branch, step_name): (bool, bool, String) = (
        behavior.mints_branch,
        behavior.inherits_branch,
        node.name.clone(),
    );
    log::info!(
        "[prepare_job] job {job_id}: behavior mints_branch={mints_branch} inherits_branch={inherits_branch} step={step_name}"
    );

    // ---- Virtual branch preparation -------------------------------------
    // Agent jobs own only durable store coordinates. No directory is allocated
    // here: the backend process resides in the job scratch directory and command
    // execution obtains disposable executor projections separately.
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(&repo_path));
    let git_rev_parse = |revision: &str| {
        orch.services
            .git
            .rev_parse(Path::new(&repo_path), vec![revision.to_string()])
            .ok()
            .filter(|sha| !sha.is_empty())
    };

    let StoreCoordinate {
        branch,
        base_commit,
        continuation,
    } = prepare_store_coordinate(
        orch,
        &jj,
        &store,
        Path::new(&repo_path),
        &job,
        &behavior,
        git_rev_parse,
        |parent_job_id| {
            let parent = run_db(load_job(
                owning_db.clone(),
                parent_job_id.to_string(),
                "Parent job not found",
            ))?;
            Ok(ParentCoordinate {
                branch: parent.branch,
                recorded_base: parent.base_commit,
            })
        },
        || {
            let seq = run_db(count_existing_branched_jobs(
                owning_db.clone(),
                job.issue_id.clone(),
                job.execution_id.clone(),
            ))?;
            let safe_step_name: String = step_name
                .to_lowercase()
                .chars()
                .map(|c| if c.is_alphanumeric() { c } else { '-' })
                .collect::<String>()
                .trim_matches('-')
                .to_string();
            Ok(format!(
                "agent/{project_key}-{display_id}-{safe_step_name}-{seq}"
            ))
        },
    )?;

    run_db(update_job_coordinate(
        owning_db.clone(),
        job_id.to_string(),
        branch,
        base_commit,
        chrono::Utc::now().timestamp() as i32,
    ))?;

    let _ = orch.services.emitter.emit(
        "job-activated",
        serde_json::json!({
            "jobId": job_id,
            "issueId": job.issue_id,
            "nodeName": job.node_name,
            "execSeq": exec_seq,
        }),
    );

    // ---- Reload job with its durable branch coordinate ------------------
    let job = run_db(load_job(
        owning_db.clone(),
        job_id.to_string(),
        "Job not found after branch preparation",
    ))?;

    // ---- Agent config ---------------------------------------------------
    let project_path = Some(PathBuf::from(repo_path.clone()));

    let agent_config = load_agent_config(orch, &job, project_path.as_deref())?;
    log::info!("[prepare_job] job {job_id}: loaded agent config");

    // ---- Create session + run record --------------------------------------
    let run_id = ids::mint_child(job_id);
    let now = chrono::Utc::now().timestamp() as i32;
    let status_str = RunStatus::Starting.to_string();

    // Ensure a Session record exists for this job and derive the first-start mode.
    let had_current_session = job.current_session_id.is_some();
    log::info!("[prepare_job] job {job_id}: preparing session");
    let (session_id, session_start, run_start_mode) = run_db(prepare_session(
        owning_db.clone(),
        job_id.to_string(),
        job.clone(),
        now,
    ))?;
    log::info!("[prepare_job] job {job_id}: session {session_id} prepared");
    if !had_current_session {
        // `prepare_session` backfills jobs.current_session_id for older/sessionless
        // jobs. The returned `start_job` value and earlier worktree invalidations
        // can both predate that write, so emit an explicit jobs change to refresh
        // chat chrome such as ContextUsage as soon as the session is known.
        let _ = orch.services.emitter.emit(
            "db-change",
            crate::notify::job_db_change_ids(
                "update",
                &job.id,
                job.issue_id.as_deref(),
                job.thread_id.as_deref(),
                job.execution_id.as_deref(),
                job.parent_job_id.as_deref(),
                job.parent_tool_use_id.as_deref(),
                &job.project_id,
            ),
        );
    }

    log::info!("[prepare_job] job {job_id}: inserting run {run_id}");
    let existing_active_count = run_db(insert_run(
        owning_db.clone(),
        RunInsert {
            run_id: run_id.clone(),
            issue_id: job.issue_id.clone(),
            project_id: Some(job.project_id.clone()),
            job_id: Some(job_id.to_string()),
            status: status_str.clone(),
            session_id: Some(session_id.clone()),
            started_at: None,
            created_at: now,
            updated_at: now,
            start_mode: Some(run_start_mode.clone()),
            warn_existing_active: true,
        },
    ))?;

    // Admission passes from the launch lock to the claim here, while the lock is
    // still held: from now until the transport registers the spawned process,
    // this job has a run and a turn but no serving process — the exact shape a
    // resume reads as idle (CAIRN-3283).
    let launch_claim = orch.claim_job_launch(job_id, &run_id);

    log::info!("[prepare_job] job {job_id}: run {run_id} inserted");
    if existing_active_count > 0 {
        log::warn!(
            "[prepare_job] Job {} already has {} active runs",
            job_id,
            existing_active_count
        );
    }

    let _ = orch.services.emitter.emit(
        "db-change",
        crate::notify::run_db_change_ids(
            "insert",
            &run_id,
            Some(job_id),
            job.issue_id.as_deref(),
            Some(&job.project_id),
        ),
    );

    // ---- Create initial turn ------------------------------------------------
    let turn_id = ids::mint_child(job_id);
    create_initial_turn(orch, &turn_id, &session_id, job_id)?;

    // ---- Resolve inputs + build prompt ----------------------------------
    let (resolved_inputs, artifact_schema_info) =
        run_db(resolve_inputs_and_schema(owning_db.clone(), job.clone()))?;

    let mut prompt = format_resolved_inputs(&resolved_inputs);
    // Provenance rides in the opening prompt rather than a side channel: it is
    // the one piece of context a successor must not be able to miss, and this is
    // the event a rebuilt or compacted session re-reads.
    if let Some(briefing) = continuation.briefing() {
        if !prompt.is_empty() {
            prompt.push_str("\n\n");
        }
        prompt.push_str(&briefing);
    }
    let prompt = prompt;

    // Provision the process residence independently of repository coordinates.
    // Session startup resolves the canonical home URI and registers the readable
    // scratch name; this early ensure guarantees a writable residence exists as
    // soon as preparation completes.
    crate::scratch::ensure_job_scratch_dir(job_id, None);

    let job_model = job.model.as_ref().map(Model::new);

    // ---- Store the launch prompt ----------------------------------------
    // Under the namespaced `user:launch` type, not `user`: this prompt is
    // composed from the issue's resolved inputs, so a watching parent must never
    // be shown its own issue description as something a person said (CAIRN-3408).
    let launch_event_id =
        store_launch_event_with_turn(orch, &run_id, &session_id, &prompt, now, Some(&turn_id))?;

    // ---- Emit system message for job start ------------------------------
    crate::messages::system::emit_job_event(
        orch,
        job_id,
        Some(&run_id),
        crate::messages::system::JobEvent::Started,
    );

    Ok(PreparedJob {
        job_id: job_id.to_string(),
        project_id: job.project_id,
        run_id,
        session_id,
        session_start,
        prompt,
        job_model,
        agent_config,
        artifact_schema_info,
        execution_id: job.execution_id,
        turn_id,
        launch_event_id,
        start_reason: TurnStartReason::Initial,
        launch_claim: Some(launch_claim),
    })
}

// ============================================================================
// continue_job_or_enqueue
// ============================================================================

/// User-facing continue: enqueue instead of resuming when a turn is already
/// active, so a stale composer send never 500s and never drops the message.
///
/// The chat composer decides send-vs-queue from a cached head-turn read; when a
/// turn goes active just after that read was taken, a plain Enter still routes to
/// the `continue_job` command (this path) rather than the queue. Left unguarded,
/// that reaches [`continue_job_impl`] with a `running`/`pending` head turn, trips
/// the active-turn guard, and returns an error the composer surfaces as a 500 —
/// dropping the typed text (CAIRN-2657).
///
/// Mirroring the direct-message guard ([`crate::messages::delivery::head_turn_active`]),
/// a message-bearing send against an active turn lands in the queue
/// ([`crate::messages::queued::Delivery::Queue`], delivered at turn end) instead.
/// The existing turn-end / flush-on-idle machinery already claims and delivers
/// `queue` rows, so no new delivery code is needed. Guarding here — the
/// authoritative layer — protects every caller of the command, not just the
/// composer. Internal Rust callers (prompt/permission answers, wakes, delegation
/// resume, advancement) keep calling [`continue_job_impl`] directly: they run at
/// genuine turn boundaries where no turn is active and must not be silently
/// converted into queued messages.
const FORCED_DIGEST_RESUME_TRIGGER: &str = "Continue from the conversation digest above.";

#[derive(Debug, Clone)]
enum ContinuationIntent {
    Normal,
    ForceDigestReseed,
    /// An operator's cold Compact. Everything live in the old session has
    /// already been torn down under the launch lock this continuation runs
    /// under; the forced reseed rotates with reason `operator_compact` and
    /// writes its boundary inside the rotation's own transaction.
    OperatorCompact(Option<OperatorBoundary>),
}

/// Where an operator Compact's `digest_reseed` boundary is written: the old
/// session's last run, at the turn the Compact cancelled.
#[derive(Debug, Clone)]
pub(crate) struct OperatorBoundary {
    pub(crate) run_id: String,
    pub(crate) turn_id: Option<String>,
}

/// Rotate the job onto a fresh session seeded from its digest and launch it,
/// for an operator Compact that has already torn the old session down.
///
/// The caller holds this job's launch lock and passes the guard in, so no
/// racing resume can reach the old session between the teardown and this
/// launch. Queued messages and attention pushes are claimed by this launch
/// like any other, so whatever the operator sent while the session was stuck
/// lands in the new session's first turn.
pub(crate) fn continue_job_launch_locked_for_operator_compact<'a>(
    orch: &Orchestrator,
    job_id: &str,
    identity_override: Option<crate::identity::UserIdentity>,
    boundary: Option<OperatorBoundary>,
    launch_guard: std::sync::MutexGuard<'a, ()>,
) -> Result<Run, String> {
    continue_job_launch_locked(
        orch,
        job_id,
        Some(FORCED_DIGEST_RESUME_TRIGGER),
        identity_override,
        Some(ResumeContext {
            suppress_user_event: true,
            suppress_self_suspend_note: true,
            supersede_pending_retry: true,
            ..Default::default()
        }),
        ContinuationIntent::OperatorCompact(boundary),
        ResumeTrace {
            id: &ids::mint_child(job_id),
            started: std::time::Instant::now(),
        },
        Some(launch_guard),
    )
}

pub fn resume_job_from_digest(
    orch: &Orchestrator,
    job_id: &str,
    identity_override: Option<crate::identity::UserIdentity>,
) -> Result<Run, String> {
    let owning_db = run_db({
        let dbs = orch.db.clone();
        let job_id = job_id.to_string();
        async move {
            crate::execution::routing::owning_db_for_job(&dbs, &job_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;
    if crate::messages::delivery::head_turn_active_sync(&owning_db, job_id) {
        return Err(
            "Resume from digest is available only after the current turn finishes.".to_string(),
        );
    }

    continue_job_impl_with_intent(
        orch,
        job_id,
        Some(FORCED_DIGEST_RESUME_TRIGGER),
        identity_override,
        Some(ResumeContext {
            suppress_user_event: true,
            suppress_self_suspend_note: true,
            supersede_pending_retry: true,
            ..Default::default()
        }),
        ContinuationIntent::ForceDigestReseed,
    )
}

/// Cairn's approximate-token convention: ~4 characters per token, the same ratio
/// the frontend's `estimateTokens` (`src/components/chat/tokenEstimate.ts`) uses,
/// so a size read in the UI is on the scale the rest of Cairn reports. The two
/// count characters slightly differently — Unicode scalar values here, UTF-16
/// code units there — which is immaterial at this precision.
///
/// No real tokenizer belongs at this boundary: the seed goes to whichever
/// provider the rebuilt session runs on, and each tokenizes it differently.
const SEED_CHARS_PER_TOKEN: usize = 4;

/// The size of the seed a forced digest resume would create right now.
///
/// The measured boundary is the seed *event*: the framing header plus the
/// composed body, which is exactly the string rotation delivers and stores. Two
/// things are deliberately outside it. The fixed continuation trigger appended at
/// delivery is its own event and a constant handful of tokens, so counting it
/// would add noise without changing a judgement. The agent's system prompt and
/// tool definitions are outside it because no reseed changes them: this number is
/// the projected size of the compacted seed, not the resumed session's context
/// occupancy.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DigestPreview {
    /// Estimated tokens in that seed content, at [`SEED_CHARS_PER_TOKEN`].
    pub estimated_tokens: i64,
    /// What a Compact pressed now would tear down first. All zero means the
    /// idle reseed; anything else is the cold path, and the confirmation names
    /// exactly these counts because they come from the same read the action
    /// makes.
    pub live_work: crate::execution::operator_compact::LiveWorkCounts,
}

impl DigestPreview {
    /// Measure one composed seed. Reachable across the crate so the seed
    /// composers' own test fixtures can assert on the number the menu shows,
    /// rather than on a second calculation that resembles it.
    pub(crate) fn of(seed_content: &str) -> Self {
        Self {
            estimated_tokens: seed_content.chars().count().div_ceil(SEED_CHARS_PER_TOKEN) as i64,
            live_work: Default::default(),
        }
    }
}

/// Compose — but never apply — the seed [`resume_job_from_digest`] would deliver,
/// and measure it.
///
/// Read-only in the strong sense: it runs the same [`compose_forced_reseed`] the
/// action runs and then throws the seed away, so the size an operator reads in
/// the tab menu cannot describe anything other than what pressing the action
/// would create. Both paths are previewable — a thread's compacted seed and an
/// ordinary node's full-fidelity digest — because composition is read-only on
/// both.
pub fn preview_job_digest(orch: &Orchestrator, job_id: &str) -> Result<DigestPreview, String> {
    let owning_db = run_db({
        let dbs = orch.db.clone();
        let job_id = job_id.to_string();
        async move {
            crate::execution::routing::owning_db_for_job(&dbs, &job_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;
    let job = run_db(load_job(
        owning_db.clone(),
        job_id.to_string(),
        "Job not found",
    ))?;
    let session_id = job
        .current_session_id
        .clone()
        .ok_or_else(|| "This job has no agent session to preview a digest for.".to_string())?;
    let session = run_db(load_session_optional(owning_db.clone(), session_id))?
        .ok_or_else(|| "This job's current session could not be loaded.".to_string())?;

    let mut preview =
        DigestPreview::of(compose_forced_reseed(orch, &owning_db, &job, &session)?.framed());
    preview.live_work =
        crate::execution::operator_compact::live_work_for_job(orch, job_id)?.counts();
    Ok(preview)
}

pub fn continue_job_or_enqueue(
    orch: &Orchestrator,
    job_id: &str,
    message: Option<&str>,
    identity_override: Option<crate::identity::UserIdentity>,
    request_id: Option<&str>,
) -> Result<Run, String> {
    continue_job_or_enqueue_inner(orch, job_id, message, identity_override, request_id, || {})
}

fn enqueue_continuation_message(
    db: &LocalDb,
    job_id: &str,
    content: &str,
    request_id: Option<&str>,
) -> Result<crate::messages::queued::QueuedMessage, String> {
    match request_id {
        Some(request_id) => crate::messages::queued::enqueue_idempotent(
            db,
            job_id,
            content,
            crate::messages::queued::Delivery::Queue,
            request_id,
        ),
        None => crate::messages::queued::enqueue(
            db,
            job_id,
            content,
            crate::messages::queued::Delivery::Queue,
        ),
    }
}

fn continue_job_or_enqueue_inner(
    orch: &Orchestrator,
    job_id: &str,
    message: Option<&str>,
    identity_override: Option<crate::identity::UserIdentity>,
    request_id: Option<&str>,
    before_resume: impl FnOnce(),
) -> Result<Run, String> {
    // Only a message-bearing send can be queued; a bare continue has nothing to
    // enqueue, so it falls straight through to the resume (and its guard).
    if let Some(text) = message.filter(|m| !m.trim().is_empty()) {
        // Message-bearing continues are compatibility ingress only. Persist first
        // and hand launch work to a background nudge, matching the canonical
        // enqueue command's acknowledgement boundary.
        let owning_db = run_db({
            let dbs = orch.db.clone();
            let job_id = job_id.to_string();
            async move {
                crate::execution::routing::owning_db_for_job(&dbs, &job_id)
                    .await
                    .map_err(|e| e.to_string())
            }
        })?;
        enqueue_continuation_message(&owning_db, job_id, text, request_id)?;
        let _ = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "queued_messages", "action": "update"}),
        );
        before_resume();

        let latest = crate::runs::queries::list_runs_for_job(owning_db, job_id)
            .map_err(|e| e.to_string())?
            .into_iter()
            .next()
            .ok_or_else(|| "queued continuation has no run row".to_string())?;
        let orch = orch.clone();
        let job_id = job_id.to_string();
        std::thread::spawn(move || {
            if let Err(error) = crate::messages::delivery::nudge_job_for_urgency(
                &orch,
                &job_id,
                crate::messages::queued::Delivery::Queue,
            ) {
                log::warn!("queued continuation nudge for job {job_id} failed: {error}");
            }
        });
        return Ok(latest);
    }
    continue_job_impl(
        orch,
        job_id,
        message,
        identity_override,
        Some(ResumeContext {
            supersede_pending_retry: true,
            goal_reactivation: Some(GoalReactivation::explicit_resume(request_id)),
            ..Default::default()
        }),
    )
}

#[cfg(any(test, feature = "test-utils"))]
pub fn continue_job_or_enqueue_with_before_resume_for_test(
    orch: &Orchestrator,
    job_id: &str,
    message: Option<&str>,
    request_id: &str,
    before_resume: impl FnOnce(),
) -> Result<Run, String> {
    continue_job_or_enqueue_inner(orch, job_id, message, None, Some(request_id), before_resume)
}

// ============================================================================
// continue_job_impl
// ============================================================================

/// Context for resuming a job after a durable suspend on the slow (>45s) path.
///
/// Its presence tells [`continue_job_impl`] not to store the resume message as a
/// visible user event: the result is already rendered in place as the
/// originating `write` call's synthetic tool_result. The message is still
/// forwarded to the agent so the model sees it in context.
#[derive(Debug, Clone, Default)]
pub struct ResumeContext {
    /// When true, skip storing the resume message as a `user` transcript event.
    pub(crate) suppress_user_event: bool,
    /// Suppress the self-suspend framing note when the hidden trigger belongs to
    /// another typed lifecycle action, such as manual digest resume.
    pub(crate) suppress_self_suspend_note: bool,
    /// Consume this already-claimed pending retry turn instead of creating a
    /// follow-up. Used only by best-effort automatic backend retries.
    pub(crate) preclaimed_retry_turn_id: Option<String>,
    /// User-facing continuation may take over an unstarted automatic retry.
    /// Reclassifying that pending head as a follow-up resets the retry budget
    /// and makes the sleeping timer's retry-head check fail.
    pub(crate) supersede_pending_retry: bool,
    /// Typed operator attention that may reactivate a paused goal. Passive
    /// bookkeeping resumes leave this absent.
    pub(crate) goal_reactivation: Option<GoalReactivation>,
    /// Reuse this pre-created, still-pending successor turn (an owned-wait
    /// resolution's `WaitResolved` turn, or a late permission answer's
    /// `permission_response` turn) instead of allocating a follow-up. It is
    /// started on the resumed run here — exactly once. Distinct from
    /// `preclaimed_retry_turn_id`, which carries automatic-retry claim semantics.
    pub(crate) preclaimed_successor_turn_id: Option<String>,
    /// If this launch starts `preclaimed_successor_turn_id` and then fails to
    /// deliver its prompt, settle that turn `cancelled` with this end reason
    /// before the job's launch lock is released, rather than leaving it
    /// `running` for the caller. Under the lock, a `running` preclaimed
    /// successor then always means some launch delivered it.
    pub(crate) unwind_preclaimed_start_as: Option<crate::models::TurnEndReason>,
}

/// Validate a pre-created successor turn (owned-wait `WaitResolved` or
/// `permission_response`) before `continue_job_impl`
/// reuses it: it must belong to this job and the resolved session and still be
/// pending (its start happens here). Rejecting a mismatch keeps a stale or
/// foreign turn from being driven as this run's successor (CAIRN-2970).
fn validate_preclaimed_successor(
    db: Arc<LocalDb>,
    job_id: &str,
    session_id: &str,
    turn_id: &str,
) -> Result<(), String> {
    let info = run_db({
        let db = db.clone();
        let turn_id = turn_id.to_string();
        async move {
            db.read(|conn| {
                let turn_id = turn_id.clone();
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT job_id, session_id, state FROM turns WHERE id = ?1 LIMIT 1",
                            (turn_id.as_str(),),
                        )
                        .await?;
                    match rows.next().await? {
                        Some(row) => Ok(Some((row.opt_text(0)?, row.text(1)?, row.text(2)?))),
                        None => Ok(None),
                    }
                })
            })
            .await
            .map_err(|e| e.to_string())
        }
    })?;
    let Some((turn_job, turn_session, state)) = info else {
        return Err(format!("preclaimed successor turn {turn_id} not found"));
    };
    if turn_job.as_deref() != Some(job_id) {
        return Err(format!(
            "preclaimed successor turn {turn_id} belongs to a different job"
        ));
    }
    if turn_session != session_id {
        return Err(format!(
            "preclaimed successor turn {turn_id} belongs to session {turn_session}, not {session_id}"
        ));
    }
    let state: TurnState = state
        .parse()
        .map_err(|e| format!("preclaimed successor turn {turn_id} has invalid state: {e}"))?;
    if state != TurnState::Pending {
        return Err(format!(
            "preclaimed successor turn {turn_id} is {state}, not pending"
        ));
    }
    Ok(())
}

pub(crate) fn continue_automatic_retry(
    orch: &Orchestrator,
    job_id: &str,
    retry_turn_id: &str,
) -> Result<Run, String> {
    continue_job_impl(
        orch,
        job_id,
        None,
        None,
        Some(ResumeContext {
            suppress_user_event: true,
            suppress_self_suspend_note: false,
            preclaimed_retry_turn_id: Some(retry_turn_id.to_string()),
            supersede_pending_retry: false,
            goal_reactivation: None,
            preclaimed_successor_turn_id: None,
            unwind_preclaimed_start_as: None,
        }),
    )
}

/// Continue an existing job with an optional follow-up message.
///
/// Reuses a warm process if one exists for the job's session, otherwise starts
/// a new Claude process with `--resume`.
pub fn continue_job_impl(
    orch: &Orchestrator,
    job_id: &str,
    message: Option<&str>,
    identity_override: Option<crate::identity::UserIdentity>,
    prompt_resume: Option<ResumeContext>,
) -> Result<Run, String> {
    continue_job_impl_with_intent(
        orch,
        job_id,
        message,
        identity_override,
        prompt_resume,
        ContinuationIntent::Normal,
    )
}

/// The launch funnel for an existing job, and the one place a resume is
/// admitted (CAIRN-3283).
///
/// Deciding a job is idle, inserting its run, allocating its turn, spawning the
/// backend process and registering that process are five separate steps with no
/// mutual exclusion between them. Two wake facts routed concurrently for one job
/// each evaluated the idle check before either had created a turn, so both fell
/// through, both inserted a run, both drove the same turn row, and both spawned a
/// process against the same session — two independent agent contexts racing every
/// side effect of one turn. The per-job launch lock makes that whole section
/// atomic; the recheck under it is what makes the serialization correct, since a
/// caller that blocked and then acquired must attach to the launch it waited on
/// rather than mint a second one.
///
/// Nothing is lost by attaching: [`continue_job_impl`] claims ALL of a job's
/// pending side-channel notices, queued messages and attention pushes, so the
/// launch already in flight carries the waiting caller's content too — and
/// anything persisted after that claim is swept by the turn-end flush.
fn continue_job_impl_with_intent(
    orch: &Orchestrator,
    job_id: &str,
    message: Option<&str>,
    identity_override: Option<crate::identity::UserIdentity>,
    prompt_resume: Option<ResumeContext>,
    continuation_intent: ContinuationIntent,
) -> Result<Run, String> {
    let resume_started = std::time::Instant::now();
    let resume_id = ids::mint_child(job_id);
    // A closed thread takes no new turns, and this is the boundary that decides
    // what a turn is. Everything else dormancy gates — a prompt, a wake, a queued
    // push — exists to reach this function, and the desktop composer reaches it
    // WITHOUT them: it holds the session job id and resumes it directly, so a gate
    // anywhere upstream would leave the app's own send path ungated and make the
    // read-only composer the enforcement rather than the courtesy it should be.
    // Taken before the launch lock so a refused resume touches nothing.
    let owning_db = run_db({
        let dbs = orch.db.clone();
        let job_id = job_id.to_string();
        async move {
            crate::execution::routing::owning_db_for_job(&dbs, &job_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;
    if crate::threads::is_dormant_thread_session_sync(&owning_db, job_id) {
        return Err(crate::threads::dormant_thread_refusal());
    }
    let launch_wait_started = std::time::Instant::now();
    let launch_lock = orch.job_launch_lock(job_id);
    // A poisoned lock means some earlier launch panicked. What it guards is
    // durable rows that the recheck below re-reads from scratch, so recover the
    // guard rather than refusing this job every resume from here on.
    let _launch_guard = launch_lock
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let mut event = crate::resume_timing::ResumeTimingEvent::new("launch_lock_acquired")
        .elapsed(launch_wait_started);
    event.resume_id = Some(&resume_id);
    event.job_id = Some(job_id);
    event.emit();

    // A pre-created retry or owned-wait successor turn is the caller's own
    // claimed turn (pending, validated below), not somebody else's launch.
    let owns_preclaimed_turn = prompt_resume.as_ref().is_some_and(|context| {
        context.preclaimed_retry_turn_id.is_some() || context.preclaimed_successor_turn_id.is_some()
    });
    if !owns_preclaimed_turn {
        if let Some(in_flight) = in_flight_launch(orch, job_id)? {
            // A resume carrying in-memory content has nowhere durable to leave
            // it, so refuse loudly instead of silently dropping the text. Every
            // such caller runs at a genuine turn boundary, where this branch
            // cannot fire unless something raced it.
            if message.is_some() {
                return Err(format!(
                    "Job {} is already running a turn (run {}); this resume was refused rather than started as a second concurrent session.",
                    &job_id[..job_id.len().min(8)],
                    &in_flight.id[..in_flight.id.len().min(8)]
                ));
            }
            log::info!(
                "Job {} already has a launch in flight (run {}); attaching to it instead of starting a second",
                &job_id[..job_id.len().min(8)],
                &in_flight.id[..in_flight.id.len().min(8)]
            );
            return Ok(in_flight);
        }
    }

    continue_job_launch_locked(
        orch,
        job_id,
        message,
        identity_override,
        prompt_resume,
        continuation_intent,
        ResumeTrace {
            id: &resume_id,
            started: resume_started,
        },
        Some(_launch_guard),
    )
}

/// The launch already in flight for this job, if there is one. Called only under
/// the per-job launch lock, before this resume has mutated anything.
///
/// A launch is in flight in either of two ways.
///
/// An admitted cold start holds a [`JobLaunchClaim`](crate::orchestrator::JobLaunchClaim)
/// from `prepare_job` until its process registers. Nothing observable is serving
/// a turn during that interval — that is the whole reason the claim exists — so
/// it is checked first, before any turn state.
///
/// Otherwise both halves must agree: the job's head turn is `running` in the
/// database AND the run driving it has a live in-process handle serving exactly
/// that turn. Requiring both is deliberate.
///
/// - A `pending` head turn is not in flight: it is a pre-created retry or
///   owned-wait successor whose owner is the caller.
/// - An active turn whose run has no live handle is not in flight either — that
///   is the stale turn [`reconcile_stale_active_turn_for_continue`] recovers, and
///   the incident's own job read as idle to both nudges precisely because a run
///   had sat in `starting` for two hours with no process behind it. Treating that
///   as in flight would convert a rare transient race into a permanently wedged
///   node.
fn in_flight_launch(orch: &Orchestrator, job_id: &str) -> Result<Option<Run>, String> {
    let owning_db = run_db({
        let dbs = orch.db.clone();
        let job_id = job_id.to_string();
        async move {
            crate::execution::routing::owning_db_for_job(&dbs, &job_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;

    // A cold start that has been admitted but whose process has not registered
    // yet owns this job's launch, even though nothing observable is serving a
    // turn for it. Checked before the turn state because that interval is
    // precisely where the turn state cannot tell the two apart.
    if let Some(run_id) = orch.claimed_launch_run(job_id) {
        return run_db(load_run(
            owning_db,
            run_id,
            "Run not found for claimed launch",
        ))
        .map(Some);
    }

    let Some(active) = load_active_turn_for_continue(owning_db.clone(), job_id.to_string())? else {
        return Ok(None);
    };
    if active.state != TurnState::Running {
        return Ok(None);
    }
    let Some(run_id) = active.run_id else {
        return Ok(None);
    };
    if orch.process_state.get_current_turn_id(&run_id).as_deref() != Some(active.turn_id.as_str()) {
        return Ok(None);
    }
    run_db(load_run(
        owning_db,
        run_id,
        "Run not found for in-flight turn",
    ))
    .map(Some)
}

/// Test seam for the launch recheck: the predicate is what makes serialization
/// correct, and its two failure modes (attaching to nothing, or refusing a
/// resume forever) are opposite and both severe.
#[cfg(any(test, feature = "test-utils"))]
pub fn in_flight_launch_for_test(
    orch: &Orchestrator,
    job_id: &str,
) -> Result<Option<String>, String> {
    in_flight_launch(orch, job_id).map(|run| run.map(|run| run.id))
}

/// Launch watchdog recovery while its caller holds this job's launch lock.
///
/// This deliberately skips the public funnel's second lock acquisition. The
/// recovery caller performs its claim-epoch and exact-head compare-and-set under
/// the same guard immediately before entering here.
pub(crate) fn continue_job_launch_locked_for_watchdog<'a>(
    orch: &Orchestrator,
    job_id: &str,
    prompt_resume: Option<ResumeContext>,
    launch_guard: std::sync::MutexGuard<'a, ()>,
) -> Result<WatchdogContinuation, String> {
    let run = continue_job_launch_locked(
        orch,
        job_id,
        None,
        None,
        prompt_resume,
        ContinuationIntent::Normal,
        ResumeTrace {
            id: &ids::mint_child(job_id),
            started: std::time::Instant::now(),
        },
        Some(launch_guard),
    )?;
    let turn_id = run_db({
        let dbs = orch.db.clone();
        let run_id = run.id.clone();
        async move {
            let db = crate::execution::routing::owning_db_for_run(&dbs, &run_id)
                .await
                .map_err(|error| error.to_string())?;
            db.query_opt_text(
                "SELECT turn_id FROM events WHERE run_id=?1 AND turn_id IS NOT NULL ORDER BY created_at DESC,id DESC LIMIT 1",
                (run_id,),
            )
            .await
            .map_err(|error| error.to_string())
        }
    })?
    .ok_or_else(|| format!("watchdog successor run {} has no current turn", run.id))?;
    let started = orch.process_state.has_process(&run.id) == Some(true)
        && orch.process_state.get_current_turn_id(&run.id).as_deref() == Some(turn_id.as_str());
    Ok(if started {
        WatchdogContinuation::Started { run, turn_id }
    } else {
        WatchdogContinuation::Queued { run, turn_id }
    })
}

#[derive(Debug)]
pub(crate) enum WatchdogContinuation {
    Started { run: Run, turn_id: String },
    Queued { run: Run, turn_id: String },
}

/// One resume's timing identity: the id every `ResumeTimingEvent` on this path
/// is stamped with, and the instant they all measure their elapsed time from.
/// They are two halves of one fact and are never used apart, so they travel as
/// one parameter.
#[derive(Clone, Copy)]
struct ResumeTrace<'a> {
    id: &'a str,
    started: std::time::Instant,
}

#[allow(clippy::too_many_arguments)]
fn continue_job_launch_locked(
    orch: &Orchestrator,
    job_id: &str,
    message: Option<&str>,
    identity_override: Option<crate::identity::UserIdentity>,
    prompt_resume: Option<ResumeContext>,
    continuation_intent: ContinuationIntent,
    trace: ResumeTrace<'_>,
    mut launch_guard: Option<std::sync::MutexGuard<'_, ()>>,
) -> Result<Run, String> {
    let ResumeTrace {
        id: resume_id,
        started: resume_started,
    } = trace;
    // Resolve the job's owning database ONCE (fail-closed): a team job resumes
    // against its synced replica, never the private DB.
    let owning_db = run_db({
        let dbs = orch.db.clone();
        let job_id = job_id.to_string();
        async move {
            crate::execution::routing::owning_db_for_job(&dbs, &job_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;
    let mut event = crate::resume_timing::ResumeTimingEvent::new("owning_database_resolved")
        .elapsed(resume_started);
    event.resume_id = Some(resume_id);
    event.job_id = Some(job_id);
    event.emit();

    // Claim operator-authored follow-ups before session preparation. Delivery is
    // the durable claim itself; provider/session setup must not hold the UI's
    // acknowledgement behind its database and process-startup work.
    let queued_messages = match crate::messages::queued::claim_all_for_job(&owning_db, job_id) {
        Ok(messages) => messages,
        Err(error) => {
            log::warn!(
                "failed to claim queued messages for resume prompt on job {}: {}",
                &job_id[..job_id.len().min(8)],
                error
            );
            Vec::new()
        }
    };
    let mut event = crate::resume_timing::ResumeTimingEvent::new("resume_queue_claimed")
        .elapsed(resume_started);
    event.resume_id = Some(resume_id);
    event.job_id = Some(job_id);
    event.count = Some(queued_messages.len());
    event.bytes = Some(
        queued_messages
            .iter()
            .map(|message| message.content.len())
            .sum(),
    );
    event.emit();
    let has_queued = !queued_messages.is_empty();
    let mut claimed_messages = ClaimedMessagesLaunchGuard {
        orch,
        db: owning_db.clone(),
        pending_ids: queued_messages
            .iter()
            .map(|message| message.id.clone())
            .collect(),
    };

    // ---- Load job -------------------------------------------------------
    let (mut job, project_id, issue_id, project_path) = run_db(load_job_context(
        owning_db.clone(),
        orch.db.clone(),
        job_id.to_string(),
    ))?;
    let preclaimed_retry_turn_id = prompt_resume
        .as_ref()
        .and_then(|context| context.preclaimed_retry_turn_id.clone());
    let preclaimed_successor_turn_id = prompt_resume
        .as_ref()
        .and_then(|context| context.preclaimed_successor_turn_id.clone());
    // An owned-wait resolution pre-creates its `WaitResolved` successor turn and
    // hands it here to reuse; that changes reconcile, reseed, and turn-selection
    // below (CAIRN-2970).
    let resuming_owned_wait = preclaimed_successor_turn_id.is_some();
    if let Some(retry_turn_id) = preclaimed_retry_turn_id.as_deref() {
        if job.parent_job_id.is_some()
            || job.recipe_node_id.is_some()
            || job.agent_config_id.as_deref() == Some("workflow")
            || !pending_retry_head_matches(owning_db.clone(), job_id, retry_turn_id)?
        {
            return Err("automatic retry was superseded or is ineligible".to_string());
        }
    }

    // CAIRN-2629: same device-ownership guard as the start/claim path — refuse to
    // resume an execution owned by another machine (its runner owns the lifecycle).
    // Fail-open on a read hiccup (deferred_owner returns None), and treat a NULL /
    // this-machine owner as "may proceed".
    if let Some(exec_id) = job.execution_id.clone() {
        let this_device = orch.anon_device_manager.device_id();
        let deferred = run_db({
            let owning_db = owning_db.clone();
            async move {
                Ok::<_, String>(
                    crate::execution::ownership::deferred_owner(&owning_db, &exec_id, &this_device)
                        .await,
                )
            }
        })?;
        if let Some(owner) = deferred {
            return Err(format!(
                "This execution is owned by device {owner}, not this machine; resume it there."
            ));
        }
    }

    // CAIRN-4665, enforcement point 1: a superseded execution takes no turns.
    //
    // Placed here because EVERY resume funnels through this function — the idle
    // flush, the urgency nudge, parent wakes, permission and question answers,
    // durable-suspend resolution, delegation completion, memory review, DAG
    // actions, and watchdog recovery (which reaches it through
    // `continue_job_launch_locked_for_watchdog`). Provider session recovery and
    // native goal continuation both terminate in that watchdog path, so they are
    // covered here rather than needing their own guard.
    //
    // Unlike the device guard immediately above, this one FAILS CLOSED: a read
    // error refuses rather than admits. A missed refusal is the incident; a
    // spurious refusal is a legible error an operator can act on.
    if let Some(refusal) = crate::execution::canonical::job_refusal(&owning_db, job_id) {
        log::warn!(
            "canonical_fence: refused resume of job {} — its execution no longer owns the issue",
            &job_id[..job_id.len().min(8)]
        );
        return Err(refusal);
    }

    let current_session_id = job.current_session_id.as_ref().ok_or_else(|| {
        if job.status == "blocked" {
            "This job has no agent session to resume. A command checkpoint is resolved by confirming it (an override that continues the workflow).".to_string()
        } else {
            "Job has no current session to resume".to_string()
        }
    })?;
    let safety_brake_rearmed = if prompt_resume.as_ref().is_some_and(|context| {
        matches!(
            context.goal_reactivation,
            Some(GoalReactivation::Operator { .. })
        )
    }) {
        crate::execution::jobs::rearm_codex_repeat_breaker(
            owning_db.clone(),
            job_id,
            current_session_id,
        )?
    } else {
        false
    };

    // A session that can never take another turn refuses here, before the
    // follow-up turn and the `running` transition below are written. Refusing
    // after them left a post-resolution push (a turn-end check verdict, a
    // base-advance failure) writing a merged issue's builder back to Running
    // over a cancelled turn that nothing re-derived.
    if let Some(session) = run_db(load_session_optional(
        owning_db.clone(),
        current_session_id.clone(),
    ))? {
        if let Some(refusal) = closed_session_refusal(&session) {
            return Err(refusal);
        }
    }

    let mut event = crate::resume_timing::ResumeTimingEvent::new("resume_session_selected")
        .elapsed(resume_started);
    event.resume_id = Some(resume_id);
    event.job_id = Some(job_id);
    event.session_id = Some(current_session_id);
    event.emit();

    // Publish the successor before any expensive or fallible session preparation.
    // Owned-wait and retry resumes already carry their own pending turn; every
    // ordinary resume creates the same canonical follow-up here instead of near
    // process spawn, so head-turn liveness is continuous across turnover.
    let user_initiated = has_queued
        || message.is_some()
        || prompt_resume
            .as_ref()
            .is_some_and(|context| context.preclaimed_retry_turn_id.is_none());
    let supersede_pending_retry = prompt_resume
        .as_ref()
        .is_some_and(|context| context.supersede_pending_retry);
    let precreated_turn_id =
        if preclaimed_successor_turn_id.is_none() && preclaimed_retry_turn_id.is_none() {
            Some(create_followup_turn(
                orch,
                current_session_id,
                job_id,
                user_initiated,
                supersede_pending_retry,
            )?)
        } else {
            None
        };
    let mut pending_launch_turn = PendingLaunchTurnGuard {
        orch,
        db: owning_db.clone(),
        turn_id: precreated_turn_id.clone(),
        run_id: None,
        inserted_run_id: None,
        wrote_running_job_id: None,
        unwind_preclaimed: None,
    };

    // ---- Transition job to Running if in terminal state -------------------
    // Any resume (including the post-completion memory review) makes the agent
    // active, so the job is Running. The confirm affordance is decoupled from
    // run state (it keys off the unconfirmed artifact), so a Running review does
    // not hide it (CAIRN-1576).
    let job_runnable = if job.status == "running" {
        JobRunnable::Attested
    } else {
        let step_started = std::time::Instant::now();
        let runnable = match transition_job_to_running(orch, job_id) {
            Ok(()) => {
                pending_launch_turn.wrote_running(job_id);
                JobRunnable::Attested
            }
            Err(e) => {
                log::warn!("Failed to transition job {} to running: {}", job_id, e);
                JobRunnable::Unknown
            }
        };
        emit_resume_step("resume_step_job_running", step_started, resume_id, job_id);
        runnable
    };

    // ---- Host agent snapshot ---------------------------------------------
    // A run's fence and sandbox policy resolve through job → execution →
    // snapshot → the job's own agent, so a job whose agent appears in no
    // snapshot cannot be granted anything. This is the one funnel a turn can
    // start from, which makes guaranteeing the snapshot here both the invariant
    // (nothing can start a turn around it) and the backfill (a thread that
    // predates this repairs itself on its next turn, with no migration). It must
    // precede `load_agent_config` so the turn runs on what the ensure just
    // wrote.
    //
    // This refuses the resume rather than logging and continuing. Launching
    // anyway would not be graceful degradation: `load_agent_config` falls back to
    // the live config files and would happily start the turn, which then refuses
    // every `run` batch with the unresolvable-fence error this exists to make
    // impossible — the failure moved out of the launch, where it is legible, and
    // into the agent's tool calls, where it is not. The ensure only fails when it
    // could not establish the invariant; a snapshot that already exists is
    // returned without consulting the agent files at all.
    let step_started = std::time::Instant::now();
    job.execution_id = run_db_borrowed(ensure_host_agent_snapshot(
        orch,
        &job,
        project_path.as_deref(),
        SnapshotFreshness::for_job(&job),
    ))?;
    emit_resume_step(
        "resume_step_agent_snapshot",
        step_started,
        resume_id,
        job_id,
    );

    let step_started = std::time::Instant::now();
    let agent_config = load_agent_config(orch, &job, project_path.as_deref())?;
    emit_resume_step("resume_step_agent_config", step_started, resume_id, job_id);
    crate::scratch::ensure_job_scratch_dir(job_id, None);

    let now = chrono::Utc::now().timestamp() as i32;

    // Desired backend for this turn, asked of the one resolution the spawn below
    // asks — `jobs.model` first, the agent's selection only where it names that
    // same model. Used to detect a model change that crosses providers so the
    // session can rotate. Resolving it any other way here would let the rotation
    // and the process disagree about which provider this turn runs on.
    //
    // This IS resolve-early, keyed to the durable fact: `jobs.model` is what the
    // job launched with, while the agent config is re-resolved against current
    // settings every turn. Preferring the config's selection let a workspace's
    // model settings move an ongoing session onto another provider without any
    // model edit (CAIRN-3798).
    let desired_backend = crate::backends::effective_backend_name(
        agent_config.as_ref().and_then(|ac| ac.selection.as_ref()),
        agent_config
            .as_ref()
            .and_then(|ac| ac.backend_preference.as_deref()),
        job.model.as_deref(),
    );

    // ---- Session identity check -----------------------------------------
    // Load Session and derive explicit continue semantics. When the requested
    // model now implies a different backend than the session was started on, the
    // prior backend's resume handle is invalid on the new backend, so rotate to a
    // fresh session on the new backend rather than resuming with a wrong handle.
    // This runs before the warm/cold split, so it covers cold continues too.
    // Cold-resume reseed decision (CAIRN-2534) is produced in the plain-resume
    // arm below and threaded into prompt assembly + seed-event storage.
    let mut reseed_outcome: Option<ReseedOutcome> = None;
    let mut resumes_provider_change = false;
    // Set when the rotation itself committed this continuation's boundary.
    let mut boundary_in_rotation = false;
    let (forced_reseed, operator_compact) = match &continuation_intent {
        ContinuationIntent::Normal => (false, None),
        ContinuationIntent::ForceDigestReseed => (true, None),
        ContinuationIntent::OperatorCompact(boundary) => (true, Some(boundary.clone())),
    };
    let step_started = std::time::Instant::now();
    let mut session_boundary: Option<SessionBoundary> = None;
    // A cold prefix that was priced and kept: recorded on whichever boundary the
    // continuation ends up producing.
    let mut cache_decision: Option<CacheDecision> = None;
    let (session_id, session_start, run_start_mode) = match run_db(load_session_optional(
        owning_db.clone(),
        current_session_id.clone(),
    ))? {
        Some(session) => match session.status {
            SessionStatus::Open => {
                if forced_reseed {
                    // The one operator-authorized replacement. Digest construction
                    // and rotation must both succeed; on failure the old session
                    // remains usable and nothing was discarded.
                    let (reason, rotation_boundary) = match operator_compact.as_ref() {
                        Some(boundary) => (RotationReason::OperatorCompact, boundary.as_ref()),
                        None => (RotationReason::Reseed, None),
                    };
                    let outcome = force_session_reseed(
                        orch,
                        &owning_db,
                        &job,
                        &session,
                        desired_backend.as_deref(),
                        reason,
                        rotation_boundary,
                    )?;
                    let session_start = crate::backends::SessionStart::New {
                        session_id: outcome.new_session_id.clone(),
                    };
                    let run_start_mode = run_start_mode(&session_start).to_string();
                    let new_id = outcome.new_session_id.clone();
                    if operator_compact.is_some() {
                        // Committed with the session close, or not at all.
                        boundary_in_rotation = true;
                    } else {
                        session_boundary = Some(SessionBoundary::digest_reseed(
                            session.backend.clone(),
                            outcome.seed_content.len(),
                        ));
                    }
                    reseed_outcome = Some(outcome);
                    (new_id, session_start, run_start_mode)
                } else {
                    // Cache economics are asked only of a cold process restart.
                    // A warm process holding the session is passed `Warm`
                    // regardless of the clock: rotating one means tearing down a
                    // live provider process mid-conversation, a much larger blast
                    // radius than this decision earns. It also means the freshness
                    // query runs only where it can change something.
                    //
                    // That leaves a real gap — a warm process idle past the
                    // horizon still pays cache creation on its full prefix on the
                    // next turn — which needs a warm-process teardown story
                    // before it can be closed safely (CAIRN-4757).
                    let prefix_cache = if orch
                        .process_state
                        .find_process_by_session(&session.id)
                        .is_some()
                    {
                        PrefixCacheState::Warm
                    } else {
                        prefix_cache_state(
                            ResumeBackend::from_key(&session.backend),
                            run_db(load_last_provider_request_for_session(
                                owning_db.clone(),
                                session.id.clone(),
                            ))
                            .ok()
                            .flatten(),
                            orch.services.clock.now(),
                        )
                    };
                    let unconfirmed_spawned_launch =
                        session_has_unconfirmed_spawned_launch(&owning_db, &session);
                    match decide_continue_action(
                        &session.backend,
                        session.backend_id.as_deref(),
                        desired_backend.as_deref(),
                        prefix_cache,
                        unconfirmed_spawned_launch,
                    ) {
                        ContinueAction::RotateToBackend(want) => {
                            log::info!(
                                "Backend change {} -> {} for job {}; rotating with transcript seed",
                                session.backend,
                                want,
                                job_id
                            );
                            let from_backend = session.backend.clone();
                            let outcome = force_session_reseed(
                                orch,
                                &owning_db,
                                &job,
                                &session,
                                Some(&want),
                                RotationReason::BackendChange,
                                None,
                            )?;
                            let session_start = crate::backends::SessionStart::New {
                                session_id: outcome.new_session_id.clone(),
                            };
                            let run_start_mode = run_start_mode(&session_start).to_string();
                            let new_id = outcome.new_session_id.clone();
                            session_boundary = Some(SessionBoundary::provider_change(
                                from_backend,
                                want,
                                job.model.as_deref(),
                                outcome.seed_content.len(),
                            ));
                            reseed_outcome = Some(outcome);
                            (new_id, session_start, run_start_mode)
                        }
                        ContinueAction::RotateUnconfirmedStart => {
                            // The provider already holds a conversation under this
                            // id from a launch that was rejected before confirming
                            // it, so a retry under the same id is refused. Rotate
                            // to a fresh id; the turn this continuation carries is
                            // adopted onto the successor below, exactly as a
                            // provider-change rotation's is.
                            log::info!(
                                "Session {} launched its provider but never confirmed it; rotating job {} onto a fresh session",
                                &session.id[..session.id.len().min(8)],
                                job_id
                            );
                            let rotation = rotate_unconfirmed_start(
                                orch,
                                &owning_db,
                                &job,
                                &session,
                                desired_backend.as_deref(),
                            )?;
                            let new_id = rotation.new_session_id;
                            let session_start = crate::backends::SessionStart::New {
                                session_id: new_id.clone(),
                            };
                            let run_start_mode = run_start_mode(&session_start).to_string();
                            session_boundary = Some(
                                SessionBoundary::digest_reseed(
                                    session.backend.clone(),
                                    rotation.seed.as_ref().map_or(0, String::len),
                                )
                                .with_reason(UNCONFIRMED_FIRST_LAUNCH_BOUNDARY_REASON),
                            );
                            reseed_outcome = rotation.seed.map(|seed_content| ReseedOutcome {
                                new_session_id: new_id.clone(),
                                seed_content,
                            });
                            (new_id, session_start, run_start_mode)
                        }
                        ContinueAction::RetryStart => {
                            // A handle-less open session is a provisional startup that
                            // never reached the backend handshake. Retry the fresh start
                            // under the same Cairn session identity so its transcript and
                            // pending turn remain attached rather than rotating again.
                            let session_start = crate::backends::SessionStart::New {
                                session_id: session.id.clone(),
                            };
                            if let Some(parent_id) = session.parent_session_id.as_deref() {
                                let db = owning_db.clone();
                                let parent_id = parent_id.to_string();
                                if let Ok(parent) = run_db(async move {
                                    crate::sessions::queries::get(db.as_ref(), &parent_id).await
                                }) {
                                    // A provider change seeds its successor here, at its
                                    // first launch. An operator Compact seeds its own
                                    // launch, and lands here only if that launch failed
                                    // after the rotation committed; recomposing is what
                                    // keeps the successor from starting with no context.
                                    if matches!(
                                        parent.terminal_reason.as_deref(),
                                        Some("provider_change" | "operator_compact")
                                    ) {
                                        let seed_content =
                                            compose_forced_reseed(orch, &owning_db, &job, &parent)?
                                                .framed()
                                                .to_string();
                                        reseed_outcome = Some(ReseedOutcome {
                                            new_session_id: session.id.clone(),
                                            seed_content,
                                        });
                                        resumes_provider_change = true;
                                    }
                                }
                            }
                            let run_start_mode = run_start_mode(&session_start).to_string();
                            (session.id.clone(), session_start, run_start_mode)
                        }
                        ContinueAction::Resume => {
                            // Native backend resume of the open session (e.g. a Codex
                            // session carrying its stored thread id) while its prefix
                            // is still cache-readable. Reseed is bypassed so a reply
                            // never restarts at the system prompt (CAIRN-2598), and
                            // at a tenth the price there is nothing to weigh it
                            // against. A cold prefix takes `MaybeReseed` instead, to
                            // be priced rather than rebuilt.
                            let session_start = resolve_continue_session_start(&session)?;
                            let run_start_mode = run_start_mode(&session_start).to_string();
                            (session.id.clone(), session_start, run_start_mode)
                        }
                        ContinueAction::MaybeReseed => {
                            // Rebuild-eligible open session. It rebuilds only if the
                            // provider's own accounting says its window is running
                            // out; otherwise this is a native resume of the same
                            // conversation, however long it has been waiting. Any
                            // failure in the attempt falls open to native resume with
                            // session state untouched.
                            //
                            // An owned-wait resume is the exception: it reuses a
                            // successor turn already bound to this session, so a
                            // reseed rotation would strand that turn. Take native
                            // resume instead (CAIRN-2970).
                            let now_secs = orch.services.clock.now();
                            let reseed = if resuming_owned_wait {
                                ReseedDecision::NativeResume(None)
                            } else {
                                attempt_session_reseed(
                                    orch,
                                    &owning_db,
                                    &job,
                                    &session,
                                    now_secs,
                                    desired_backend.as_deref(),
                                    prefix_cache,
                                )
                            };
                            match reseed {
                                ReseedDecision::Rebuilt(outcome, decision) => {
                                    let session_start = crate::backends::SessionStart::New {
                                        session_id: outcome.new_session_id.clone(),
                                    };
                                    let run_start_mode = run_start_mode(&session_start).to_string();
                                    let new_id = outcome.new_session_id.clone();
                                    reseed_outcome = Some(outcome);
                                    session_boundary = Some(
                                        SessionBoundary::digest_reseed(
                                            session.backend.clone(),
                                            reseed_outcome
                                                .as_ref()
                                                .map_or(0, |value| value.seed_content.len()),
                                        )
                                        .with_cache(decision),
                                    );
                                    (new_id, session_start, run_start_mode)
                                }
                                ReseedDecision::NativeResume(decision) => {
                                    // The boundary itself is built below, once the
                                    // process decision is known. Carrying the
                                    // reading forward rather than the conclusion is
                                    // what keeps its label and its detail set by
                                    // this same branch.
                                    cache_decision = decision;
                                    let session_start = resolve_continue_session_start(&session)?;
                                    let run_start_mode = run_start_mode(&session_start).to_string();
                                    (session.id.clone(), session_start, run_start_mode)
                                }
                            }
                        }
                    }
                }
            }
            // A closed or failed session cannot take another turn. Normally
            // refused before anything was written (see `closed_session_refusal`
            // above); this arm covers a session closed in between.
            SessionStatus::Closed => {
                // Legacy data only. Watchdog reconciliation used to close a
                // session whose turn it found already terminal, which replaced a
                // native conversation for bookkeeping; it no longer closes
                // anything, so no session minted after CAIRN-4618 can reach this
                // arm. Installs still carry rows that did, and they must still be
                // able to take a turn.
                if session.terminal_reason.as_deref()
                    == Some(crate::orchestrator::lifecycle::ALREADY_TERMINAL_RECONCILED_REASON)
                {
                    let new_session = run_db({
                        let db = owning_db.clone();
                        let session = session.clone();
                        let job_id = job_id.to_string();
                        let emitter = orch.services.emitter.clone();
                        let recovery_backend = desired_backend
                            .clone()
                            .unwrap_or_else(|| crate::backends::CLAUDE_FAMILY_BACKEND.to_string());
                        async move {
                            crate::sessions::queries::rotate_job_session_with_backend(
                                db.as_ref(),
                                &session,
                                &job_id,
                                &recovery_backend,
                                RotationReason::RecoveryReplacement,
                                None,
                                emitter.as_ref(),
                            )
                            .await
                        }
                    })?;
                    let session_start = crate::backends::SessionStart::New {
                        session_id: new_session.id.clone(),
                    };
                    let run_start_mode = run_start_mode(&session_start).to_string();
                    (new_session.id, session_start, run_start_mode)
                } else {
                    return Err(closed_session_refusal(&session).unwrap_or_default());
                }
            }
            SessionStatus::Failed => {
                return Err(closed_session_refusal(&session).unwrap_or_default());
            }
        },
        None => {
            // No Session record found — legacy data (e.g. old Codex thread_id).
            // Use session_id directly; it may itself be a resume handle.
            log::info!(
                "No Session record for {}, using as-is (legacy)",
                &current_session_id[..current_session_id.len().min(8)]
            );
            (
                current_session_id.clone(),
                crate::backends::SessionStart::Resume {
                    session_id: current_session_id.clone(),
                    backend_id: current_session_id.clone(),
                },
                "resume".to_string(),
            )
        }
    };
    if session_id.as_str() != current_session_id.as_str() {
        let turn_id = precreated_turn_id
            .as_deref()
            .or(preclaimed_successor_turn_id.as_deref())
            .or(preclaimed_retry_turn_id.as_deref())
            .ok_or_else(|| "session rotated without an adoptable successor turn".to_string())?;
        adopt_successor_turn(owning_db.clone(), turn_id, &session_id)?;
    }
    emit_resume_step(
        "resume_step_session_loaded",
        step_started,
        resume_id,
        job_id,
    );

    // ---- Find or create run ---------------------------------------------
    // A delayed retry is best-effort. Recheck its durable claim immediately
    // before allocating or waking a run so a superseding action wins cleanly.
    if let Some(retry_turn_id) = preclaimed_retry_turn_id.as_deref() {
        if !pending_retry_head_matches(owning_db.clone(), job_id, retry_turn_id)? {
            return Err("automatic retry was superseded before run launch".to_string());
        }
    }
    // A run row this host holds no process for is not a launch in flight; it is
    // a predecessor that never reached a terminal status, and left alone it
    // keeps answering `latest_run_for_job` (and therefore the whole delivery
    // ladder) on behalf of a process that does not exist (CAIRN-3291). Reap it
    // here, where the reaper's precondition is already met: the launch lock is
    // held, and this precedes the warm-reuse decision below, so every row is
    // still carrying whatever process it had when this resume arrived.
    let step_started = std::time::Instant::now();
    crate::runs::reap::reap_stale_runs_for_job(orch, &owning_db, job_id);
    emit_resume_step("resume_step_reap_runs", step_started, resume_id, job_id);
    // Reconcile a reusable process against the job's requested model *before*
    // deciding to reuse it. `jobs.model` is the source of truth: a model change
    // restarts the process (cold resume with the new model) so the persisted
    // model wins. Cross-backend changes were already handled by rotation above,
    // so any process found here is on the correct backend.
    //
    // The durable session pin is reconciled the same way: provider credentials
    // are frozen into a process at spawn, so a pin that moved (an explicit
    // continuation after depletion) restarts an idle process cold on the new
    // account rather than reusing the spent one.
    let desired_account_id = crate::storage::run_db_blocking({
        let db = owning_db.clone();
        let session_id = session_id.clone();
        move || async move { crate::sessions::queries::get(&db, &session_id).await }
    })?
    .account_id;
    let mut restarted_for_model_change = false;
    let existing_run_id = match orch.process_state.find_process_by_session(&session_id) {
        Some(found_run_id) => {
            let account_decision = ensure_reused_process_account(
                &orch.process_state,
                &found_run_id,
                desired_account_id.as_deref(),
            )?;
            let decision = match account_decision {
                ReuseDecision::Restart => ReuseDecision::Restart,
                ReuseDecision::Reuse => ensure_reused_process_model(
                    &orch.process_state,
                    &found_run_id,
                    job.model.as_deref(),
                )?,
            };
            match decision {
                ReuseDecision::Reuse => Some(found_run_id),
                ReuseDecision::Restart => {
                    log::info!(
                        "{} changed for session {}; evicting warm process {} and restarting",
                        if account_decision == ReuseDecision::Restart {
                            "Provider account"
                        } else {
                            "Model"
                        },
                        &session_id[..session_id.len().min(8)],
                        &found_run_id[..found_run_id.len().min(8)]
                    );
                    restarted_for_model_change = true;
                    // A model change ends this retained process lifetime. Release
                    // its generation-fenced provider claim before removing the
                    // handle that carries the only in-memory copy.
                    crate::orchestrator::lifecycle::release_and_stop_process(orch, &found_run_id);
                    None
                }
            }
        }
        None => None,
    };
    // The owned-wait successor IS the intended (pending) active turn; reconcile
    // would cancel it, and the yielded predecessor leaves nothing else stale.
    if existing_run_id.is_none() && precreated_turn_id.is_none() && !resuming_owned_wait {
        let _ = reconcile_stale_active_turn_for_continue(orch, job_id, &session_id)?;
    }
    let (run_id, is_process_reuse) = if let Some(existing_id) = existing_run_id {
        log::info!(
            "Found existing process for session {}, reusing run {}",
            &session_id[..session_id.len().min(8)],
            &existing_id[..existing_id.len().min(8)]
        );
        (existing_id, true)
    } else {
        let new_run_id = ids::mint_child(job_id);
        let status_str = RunStatus::Starting.to_string();
        let step_started = std::time::Instant::now();
        run_db(insert_run(
            owning_db.clone(),
            RunInsert {
                run_id: new_run_id.clone(),
                issue_id: issue_id.clone(),
                project_id: Some(project_id.clone()),
                job_id: Some(job_id.to_string()),
                status: status_str.clone(),
                session_id: Some(session_id.clone()),
                started_at: None,
                created_at: now,
                updated_at: now,
                start_mode: Some(run_start_mode.clone()),
                // Advisory only, and logged on the prepare path already: under
                // the per-job launch lock a pre-existing active run for this job
                // should now be impossible, so say so if one ever appears.
                warn_existing_active: true,
            },
        ))?;
        pending_launch_turn.inserted_run(&new_run_id);
        emit_resume_step("resume_step_run_inserted", step_started, resume_id, job_id);
        let _ = orch.services.emitter.emit(
            "db-change",
            crate::notify::run_db_change_ids(
                "insert",
                &new_run_id,
                Some(job_id),
                issue_id.as_deref(),
                Some(&project_id),
            ),
        );
        (new_run_id, false)
    };
    if session_boundary.is_none()
        && !is_process_reuse
        && !resumes_provider_change
        && !boundary_in_rotation
    {
        session_boundary = Some(
            match &session_start {
                crate::backends::SessionStart::Resume { .. }
                | crate::backends::SessionStart::Fork { .. } => {
                    SessionBoundary::native_resume(job.model.as_deref(), desired_backend.as_deref())
                }
                crate::backends::SessionStart::New { .. } => {
                    SessionBoundary::fresh(job.model.as_deref(), desired_backend.as_deref())
                }
            }
            .with_cache(cache_decision),
        );
    }
    let mut process_decision =
        crate::resume_timing::ResumeTimingEvent::new("resume_process_decision");
    process_decision.resume_id = Some(resume_id);
    process_decision.job_id = Some(job_id);
    process_decision.run_id = Some(&run_id);
    process_decision.session_id = Some(&session_id);
    process_decision.mode = Some(if is_process_reuse {
        "reuse"
    } else if restarted_for_model_change {
        "restart_model_change"
    } else {
        "cold_spawn"
    });
    process_decision.emit();

    // ---- Create successor turn for follow-up ----------------------------
    // A resume carrying user-authored content (an explicit message or a
    // prompt/permission answer) is a work turn, never the post-completion
    // memory-review reflection. The pending-queued-message case (a user steer
    // that arrives without an explicit message) is detected inside
    // `create_followup_turn` against the rows the claim below sweeps up.
    let turn_id = if let Some(successor_id) = preclaimed_successor_turn_id.as_deref() {
        // Reuse the pre-created owned-wait successor instead of allocating a
        // follow-up; it is started (pending -> running on this run) below like any
        // other pending successor.
        validate_preclaimed_successor(owning_db.clone(), job_id, &session_id, successor_id)?;
        successor_id.to_string()
    } else if let Some(retry_turn_id) = preclaimed_retry_turn_id {
        if !pending_retry_head_matches(owning_db.clone(), job_id, &retry_turn_id)? {
            return Err("automatic retry was superseded before launch".to_string());
        }
        retry_turn_id
    } else if let Some(turn_id) = precreated_turn_id.as_deref() {
        turn_id.to_string()
    } else {
        let step_started = std::time::Instant::now();
        let turn_id = create_followup_turn(
            orch,
            &session_id,
            job_id,
            user_initiated,
            supersede_pending_retry,
        )?;
        let mut event = crate::resume_timing::ResumeTimingEvent::new("resume_step_turn_created");
        event.step_us = Some(step_started.elapsed().as_micros());
        event.resume_id = Some(resume_id);
        event.job_id = Some(job_id);
        event.turn_id = Some(&turn_id);
        event.emit();
        turn_id
    };
    let retry_turn_prestarted = prompt_resume
        .as_ref()
        .is_some_and(|context| context.preclaimed_retry_turn_id.is_some());
    if retry_turn_prestarted && !claim_retry_turn_start(orch, &turn_id, &run_id)? {
        return Err("automatic retry was superseded before turn reservation".to_string());
    }

    // ---- Artifact schema ------------------------------------------------
    let step_started = std::time::Instant::now();
    let artifact_schema_info = run_db(find_job_downstream_artifact_schema(
        owning_db.clone(),
        job.clone(),
    ))?;
    emit_resume_step(
        "resume_step_artifact_schema",
        step_started,
        resume_id,
        job_id,
    );

    // When there is no explicit resume message but the user queued follow-ups,
    // the queued content *is* the user's prompt — don't lead with the generic
    // "Continue where you left off." placeholder (and don't store it as a "You"
    // event; each queued message is stored as its own event below).
    // CAIRN-1881: drain this job's pending attention pushes. Both rousing
    // (`wake`/`interrupt`) and `passive` ride-along pushes deliver on a resume
    // that is already happening; each is lazy-resolved so a push whose referent
    // already resolved is skipped. They are stamped delivered atomically with
    // their carrying event, persisted below once the prompt is assembled.
    let drained_pushes = {
        let db = owning_db.clone();
        let recipient = job_id.to_string();
        run_db(async move {
            let pushes = crate::orchestrator::attention_push::list_pending_live(&db, &recipient)
                .await
                .map_err(|e| e.to_string())?;
            Ok::<_, String>(retain_resolvable_pushes(&db, &recipient, pushes).await)
        })
        .unwrap_or_default()
    };
    let has_pushes = !drained_pushes.is_empty();
    // Keep resume assembly independent of full resource reads. Every push carries
    // its canonical URI and a compact summary; only bounded, targeted database
    // details are resolved for direct messages, catch-up, and terminal outcomes.
    let push_prompt = if has_pushes {
        let pushes = drained_pushes.clone();
        crate::storage::run_db_blocking(move || async move {
            Ok::<_, String>(
                crate::orchestrator::attention_delivery::render_pushes_resolved(orch, &pushes)
                    .await,
            )
        })
        .ok()
        .flatten()
    } else {
        None
    };
    let mut event =
        crate::resume_timing::ResumeTimingEvent::new("resume_pushes_ready").elapsed(resume_started);
    event.resume_id = Some(resume_id);
    event.job_id = Some(job_id);
    event.session_id = Some(&session_id);
    event.count = Some(drained_pushes.len());
    event.bytes = push_prompt.as_ref().map(String::len);
    event.emit();
    // Persist the same compact content the agent received alongside the wake-card
    // payload. The detail modal retains the summary and canonical resource links.
    let push_summary =
        if has_pushes {
            Some(
            crate::storage::run_db_blocking({
                let db = owning_db.clone();
                let pushes = drained_pushes.clone();
                let resolved = push_prompt.clone().unwrap_or_default();
                move || async move {
                    crate::orchestrator::attention_push::push_event_content_json_with_resolutions(
                        &db, &pushes, &resolved,
                    ).await.map_err(|error| error.to_string())
                }
            }).unwrap_or_else(|error| {
                log::warn!("Failed to resolve resume push receipts: {}", error);
                crate::orchestrator::attention_push::push_event_content_json(
                    &drained_pushes, push_prompt.as_deref().unwrap_or_default(),
                )
            }),
            )
        } else {
            None
        };
    let trigger = resolve_resume_trigger(message, has_queued, has_pushes);
    let base_prompt = resolve_skill_slash_command(orch, &trigger.message, project_path.as_deref());
    let side_channel_notices =
        match crate::messages::side_channel::claim_pending_side_channel_for_job(&owning_db, job_id)
        {
            Ok(notices) => notices,
            Err(error) => {
                log::warn!(
                    "failed to claim side-channel notices for resume prompt on job {}: {}",
                    &job_id[..job_id.len().min(8)],
                    error
                );
                Vec::new()
            }
        };
    if !side_channel_notices.is_empty() {
        crate::messages::transcript::insert_side_channel_events_sync(
            orch,
            &run_id,
            Some(&session_id),
            Some(&turn_id),
            &side_channel_notices,
        )?;
    }
    let queued_block = if has_queued {
        Some(
            queued_messages
                .iter()
                .map(|m| m.content.clone())
                .collect::<Vec<_>>()
                .join("\n\n"),
        )
    } else {
        None
    };
    let side_channel_block = if side_channel_notices.is_empty() {
        None
    } else {
        Some(crate::messages::transcript::render_side_channel_prompt_block(&side_channel_notices))
    };
    // A durable self-suspend resume injects the awaited result as the suspended
    // tool call's synthetic `tool_result` (this is exactly when
    // `suppress_user_event` is set). From the agent's side that call looks
    // interrupted mid-execution and then returns fine, which is disconcerting;
    // lead the forwarded prompt with a short note that names the pause as
    // deliberate. The note rides only in the live prompt, never a stored event,
    // so it frames this turn without cluttering the transcript.
    let suppress_user_event = prompt_resume
        .as_ref()
        .map(|ctx| ctx.suppress_user_event)
        .unwrap_or(false);
    let suppress_self_suspend_note = prompt_resume
        .as_ref()
        .is_some_and(|ctx| ctx.suppress_self_suspend_note);
    let artifact_handoff_note = artifact_handoff_resume_note(owning_db.clone(), job_id)?;
    let resume_note = match (
        (suppress_user_event && !suppress_self_suspend_note)
            .then_some(RESUME_AFTER_SELF_SUSPEND_NOTE),
        artifact_handoff_note.as_deref(),
    ) {
        (Some(self_suspend), Some(handoff)) => Some(format!("{self_suspend}\n\n{handoff}")),
        (Some(self_suspend), None) => Some(self_suspend.to_string()),
        (None, Some(handoff)) => Some(handoff.to_string()),
        (None, None) => None,
    };
    let prompt = assemble_resume_prompt(
        resume_note.as_deref(),
        queued_block,
        &base_prompt,
        side_channel_block,
        push_prompt.as_deref(),
    );
    // Reseed: the seed (header + prior-session digest) leads the reconstructed
    // context; the normal trigger tail follows. The seed is also stored as a
    // collapsible `user:seed` event below, while the trigger keeps its own
    // verbatim user event (CAIRN-2534).
    let prompt = apply_reseed_seed(prompt, reseed_outcome.as_ref());
    // This is the single outer turn-boundary stamp. Apply it after every producer,
    // including cold-resume reseeding, so all resumed input opens with the clock.
    let previous_turn_end = previous_turn_end_for_resume(owning_db.clone(), &turn_id)?;
    let prompt = prepend_resume_stamp(
        prompt,
        crate::clock::host(),
        chrono::Utc::now(),
        previous_turn_end,
    );
    let mut event =
        crate::resume_timing::ResumeTimingEvent::new("resume_prompt_ready").elapsed(resume_started);
    event.resume_id = Some(resume_id);
    event.job_id = Some(job_id);
    event.session_id = Some(&session_id);
    event.turn_id = Some(&turn_id);
    event.bytes = Some(prompt.len());
    event.emit();

    let job_model = job.model.as_ref().map(Model::new);

    // ---- Store user event -----------------------------------------------
    // Store the user's message so the UI displays what was actually sent.
    //
    // Slow-path prompt resumes skip this: the answer is already rendered as the
    // originating Question call's synthetic tool_result, so a separate "You"
    // block would duplicate it. The message is still forwarded to the agent
    // below for the model's context.
    //
    // `suppress_user_event` was resolved above (it also gates the resume note);
    // reuse it here.
    // Skip the default "You" event when the user supplied no explicit message and
    // the content is carried entirely by queued follow-ups (stored individually
    // below) — storing the empty placeholder would render a blank You block. A
    // resume with no operator content at all still stores its event, but as the
    // `user:continuation` marker rather than a "You" block.
    // CAIRN-1881: persist the carrying event for the drained pushes and stamp each
    // delivered by it, atomically (same transaction as the event INSERT). The
    // pushes already ride in the resume prompt above, so they are delivered
    // regardless of `suppress_user_event`; recovery redelivers only pushes whose
    // carrying event never durably landed.
    // Reseed: store the seed (header + digest) as a collapsible `user:seed`
    // event ahead of every other event in this turn, so the transcript renders a
    // divider followed by the verbatim trigger message rather than a giant seed
    // bubble, and future digests collapse the seed to one line (CAIRN-2534).
    if let Some(boundary) = &session_boundary {
        crate::messages::transcript::insert_session_boundary_sync(
            orch,
            &run_id,
            Some(&session_id),
            Some(&turn_id),
            boundary.raw(),
        )?;
    }
    if let Some(outcome) = &reseed_outcome {
        store_seed_event_with_turn(
            orch,
            &run_id,
            &session_id,
            &outcome.seed_content,
            now,
            Some(&turn_id),
        )?;
    }
    // Armed the moment the stamp lands, disarmed once a process holds the prompt
    // it stamped for; see `UndeliveredPushes`.
    let mut stamped_pushes = UndeliveredPushes {
        db: owning_db.clone(),
        carrying_event_id: None,
    };
    if let Some(text) = &push_summary {
        let push_ids: Vec<String> = drained_pushes.iter().map(|p| p.id.clone()).collect();
        stamped_pushes.carrying_event_id = Some(
            crate::execution::jobs::snapshots::store_attention_push_event(
                orch,
                &run_id,
                &session_id,
                text,
                &push_ids,
                now,
                Some(&turn_id),
            )?,
        );
    }
    // Queued follow-ups — including passive "quiet" notes that rode along without
    // waking the agent — were authored before the immediate resume message, so
    // they render first, matching the prompt order assembled above. Each shows as
    // its own "You" block and drops out of the pending strip.
    if has_queued {
        for queued in &queued_messages {
            let display_message = queued.content.clone();
            store_queued_user_event_with_turn(
                orch,
                &run_id,
                &session_id,
                &queued.id,
                &display_message,
                now,
                Some(&turn_id),
            )?;
            let mut event =
                crate::resume_timing::ResumeTimingEvent::new("queued_message_persisted");
            event.resume_id = Some(resume_id);
            event.queued_message_id = Some(&queued.id);
            event.job_id = Some(job_id);
            event.run_id = Some(&run_id);
            event.session_id = Some(&session_id);
            event.turn_id = Some(&turn_id);
            event.bytes = Some(queued.content.len());
            event.emit();
            claimed_messages.persisted(&queued.id);
        }
        let _ = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "queued_messages", "action": "update"}),
        );
    }
    let store_default_user_event = !suppress_user_event && !trigger.message.is_empty();
    if store_default_user_event {
        let display_message = trigger.message.clone();
        // A synthesized continuation is stored under its own namespaced type so
        // no surface can render it as something the operator said (CAIRN-3175).
        let store = if trigger.synthetic {
            store_continuation_event_with_turn
        } else {
            store_user_event_with_turn
        };
        store(
            orch,
            &run_id,
            &session_id,
            &display_message,
            now,
            Some(&turn_id),
        )?;
        if !trigger.synthetic {
            let mut event = crate::resume_timing::ResumeTimingEvent::new("user_message_persisted")
                .elapsed(resume_started);
            event.resume_id = Some(resume_id);
            event.job_id = Some(job_id);
            event.run_id = Some(&run_id);
            event.session_id = Some(&session_id);
            event.turn_id = Some(&turn_id);
            event.emit();
        }
    }

    // ---- Warm process or new session ------------------------------------
    if is_process_reuse {
        // Establish the turn FULLY before waking the agent (CAIRN-2123). A warm
        // agent commonly fires a tool call the instant it resumes; if the wake
        // (`send_user_message`) preceded turn establishment, that call could
        // land in the `Busy`-without-turn window and capture a NULL turn for any
        // worktree-fence crossing it raised — durably parking the run with no
        // way to resume it. Ordering occupancy (`transition_to_active` then
        // `ServingTurn` via `begin_turn`) and the persisted DB turn
        // (`start_turn` writes `jobs.current_turn_id`) ahead of the wake closes
        // that window: a tool call produced on resume always observes
        // `ServingTurn(turn)`, never `Busy`.
        orch.process_state.transition_to_active(&run_id);
        if !retry_turn_prestarted {
            start_turn(orch, &turn_id, &run_id)?;
        }
        pending_launch_turn.started(&run_id);
        if let Some(successor_id) = preclaimed_successor_turn_id.as_deref() {
            pending_launch_turn.started_preclaimed(
                &run_id,
                successor_id,
                prompt_resume
                    .as_ref()
                    .and_then(|context| context.unwind_preclaimed_start_as.as_ref()),
            );
        }
        orch.process_state
            .set_current_turn_id(&run_id, Some(&turn_id));

        // Run stays Live — no durable status change for warm reuse.
        let warm_prompt_started = std::time::Instant::now();
        let message_text = prompt.clone();
        let image_project_id = project_id.clone();
        let image_project_key = run_db({
            let db = owning_db.clone();
            let project_id = project_id.clone();
            async move {
                db.query_opt_text("SELECT key FROM projects WHERE id = ?1", (project_id,))
                    .await
                    .map_err(|error| error.to_string())?
                    .ok_or_else(|| "resume project authority is unavailable".to_string())
            }
        })?;
        let mut message_content = crate::storage::run_db_blocking(move || async move {
            crate::agent_process::stdin::resolve_stable_images(
                &orch.db,
                &image_project_id,
                &image_project_key,
                message_text,
            )
            .await
        })?;
        let mut prompt_event = crate::resume_timing::ResumeTimingEvent::new("warm_prompt_resolved");
        prompt_event.step_us = Some(warm_prompt_started.elapsed().as_micros());
        prompt_event.resume_id = Some(resume_id);
        prompt_event.job_id = Some(job_id);
        prompt_event.run_id = Some(&run_id);
        prompt_event.session_id = Some(&session_id);
        prompt_event.turn_id = Some(&turn_id);
        prompt_event.bytes = Some(message_content.text.len());
        prompt_event.emit();
        let stdin_started = std::time::Instant::now();
        // Queued messages have no author provenance. Only typed attention or
        // the condition's durable resolution can request a goal release.
        let reactivation = prompt_resume
            .as_ref()
            .and_then(|context| context.goal_reactivation.clone())
            .or_else(|| {
                drained_pushes.iter().find_map(|push| {
                    crate::orchestrator::attention_push::operator_message_id_from_key(&push.key)
                        .map(GoalReactivation::operator_message)
                })
            });
        if safety_brake_rearmed {
            crate::backends::stdin::rearm_goal_safety_brake(&orch.process_state, &run_id)?;
        }
        match crate::backends::stdin::reactivate_goal(
            &orch.process_state,
            &run_id,
            reactivation.as_ref(),
            job_runnable,
        )? {
            crate::backends::stdin::GoalReactivationResult::Lifted { kind, id } => {
                crate::messages::transcript::insert_goal_reactivated_sync(
                    orch,
                    &run_id,
                    &session_id,
                    &turn_id,
                    kind,
                    id.as_deref().unwrap_or(resume_id),
                )?;
            }
            crate::backends::stdin::GoalReactivationResult::StillPaused { cause } => {
                if let Some(trigger) = reactivation.as_ref() {
                    crate::messages::transcript::insert_goal_release_refused_sync(
                        orch,
                        &run_id,
                        &session_id,
                        &turn_id,
                        trigger,
                        &cause,
                    )?;
                }
                message_content.text =
                    format!("{}\n\n{}", goal_pause_notice(&cause), message_content.text);
            }
            crate::backends::stdin::GoalReactivationResult::Active => {}
        }
        crate::backends::stdin::send_user_message(
            &orch.process_state,
            &run_id,
            &message_content,
            &session_id,
            None,
        )?;
        pending_launch_turn.adopted();
        let mut stdin_event = crate::resume_timing::ResumeTimingEvent::new("warm_stdin_written");
        stdin_event.step_us = Some(stdin_started.elapsed().as_micros());
        stdin_event.resume_id = Some(resume_id);
        stdin_event.job_id = Some(job_id);
        stdin_event.run_id = Some(&run_id);
        stdin_event.session_id = Some(&session_id);
        stdin_event.turn_id = Some(&turn_id);
        stdin_event.bytes = Some(message_content.text.len());
        stdin_event.emit();
        // The warm agent now holds the prompt these pushes rode in on.
        stamped_pushes.delivered();
    } else {
        let launch_event_id = run_db({
            let db = owning_db.clone();
            let run = run_id.clone();
            let turn = turn_id.clone();
            async move {
                db.query_opt_text(
                    "SELECT id FROM events WHERE run_id=?1 AND turn_id=?2 ORDER BY created_at DESC,id DESC LIMIT 1",
                    (run, turn),
                )
                .await
                .map_err(|error| error.to_string())
            }
        })?
        .ok_or_else(|| "cold continuation has no durable launch event".to_string())?;
        // Durable admission is now the launch owner. It may remain queued, and
        // promotion reacquires this same lock before its final state recheck.
        drop(launch_guard.take());
        super::start_prepared_job_session(
            orch,
            PreparedJob {
                job_id: job_id.to_string(),
                project_id: project_id.clone(),
                run_id: run_id.clone(),
                session_id: session_id.clone(),
                session_start,
                prompt,
                job_model,
                agent_config,
                artifact_schema_info,
                execution_id: job.execution_id.clone(),
                turn_id: turn_id.clone(),
                launch_event_id,
                start_reason: TurnStartReason::FollowUp,
                launch_claim: None,
            },
            identity_override,
        )?;
        // Durable admission now owns the exact prompt and turn. Returning while
        // queued must not roll either back; promotion establishes the turn once
        // the process exists.
        stamped_pushes.delivered();
        pending_launch_turn.adopted();
    }

    // ---- Return run -----------------------------------------------------
    let run = run_db(load_run(
        owning_db.clone(),
        run_id.clone(),
        "Run not found after creation",
    ))?;
    Ok(run)
}

/// An attention delivery written by a launch that has not yet reached a process,
/// reverted if it never does.
///
/// The carrying event and its stamps have to precede the spawn — the pushes ride
/// in the prompt the spawn carries — so every launch opens a window in which a
/// delivery is durably recorded and no agent has received it. Held across that
/// window, this guard closes it: dropped still armed, by any `?` between the
/// stamp and the spawn, it reverts the delivery so the next resume redelivers.
/// Without it a refused launch spends the wake silently, and the node that was
/// owed it simply never wakes.
///
/// A guard rather than an error branch because the window is long and every step
/// in it can fail; `Drop` covers each one without asking a future edit to
/// remember.
struct UndeliveredPushes {
    db: Arc<LocalDb>,
    carrying_event_id: Option<String>,
}

impl UndeliveredPushes {
    /// A process now holds the prompt this delivery was written for.
    fn delivered(&mut self) {
        self.carrying_event_id = None;
    }
}

impl Drop for UndeliveredPushes {
    fn drop(&mut self) {
        let Some(event_id) = self.carrying_event_id.take() else {
            return;
        };
        let db = self.db.clone();
        match crate::storage::run_db_blocking(move || async move {
            crate::orchestrator::attention_push::revert_delivery(&db, &event_id)
                .await
                .map_err(|error| error.to_string())
        }) {
            Ok(restored) => log::warn!(
                "launch failed after recording an attention delivery; reverted it and returned {restored} push(es) to the pending queue for redelivery"
            ),
            // Nothing else will clear these rows, so say plainly that a wake was
            // lost rather than leaving it to be inferred from silence.
            Err(error) => log::error!(
                "launch failed after recording an attention delivery, and reverting it failed ({error}); those wakes will not be redelivered"
            ),
        }
    }
}

fn artifact_handoff_resume_note(
    owning_db: Arc<LocalDb>,
    job_id: &str,
) -> Result<Option<String>, String> {
    let job_id = job_id.to_string();
    run_db(async move {
        owning_db
            .query_opt(
                "SELECT a.output_name, a.artifact_type, a.version, a.confirmed
                   FROM jobs j
                   JOIN turns t ON t.id = j.current_turn_id
                   JOIN artifacts a ON a.job_id = j.id
                  WHERE j.id = ?1 AND t.end_reason = 'artifact_handoff'
                  ORDER BY a.created_at DESC, a.rowid DESC
                  LIMIT 1",
                (job_id,),
                |row| {
                    Ok((
                        row.opt_text(0)?,
                        row.text(1)?,
                        row.i64(2)? as i32,
                        row.i64(3)? != 0,
                    ))
                },
            )
            .await
            .map_err(|error| format!("Failed to resolve artifact handoff resume note: {error}"))
            .map(|artifact| {
                artifact.map(|(output_name, artifact_type, version, confirmed)| {
                    let name = output_name.unwrap_or(artifact_type);
                    let state = if confirmed {
                        "applied successfully"
                    } else {
                        "applied successfully and is awaiting user confirmation"
                    };
                    format!(
                        "Resuming after an artifact handoff. Your previous turn ended because you wrote your terminal output artifact (cairn:~/{name}, version {version}) — the write was {state} and the session paused for review. Any interruption notice in the prior transcript was Cairn ending the turn at that boundary, not a user abort. The message that resumes you follows."
                    )
                })
            })
    })
}

/// The immediate message a resume leads with, and whether Cairn wrote it.
struct ResumeTrigger {
    /// Text that becomes the resume prompt's immediate message. Empty when the
    /// content is carried entirely by queued follow-ups or attention pushes,
    /// which are assembled (and stored) as their own blocks.
    message: String,
    /// True when no operator content reached this resume at all, so `message` is
    /// [`SYNTHETIC_CONTINUATION_PROMPT`] — Cairn's own text. Every attribution
    /// decision downstream keys off this flag.
    synthetic: bool,
}

/// Decide what a resume leads with, given the caller's message and whether the
/// claims for this wake swept up queued follow-ups or attention pushes.
///
/// A blank message is no message: it reaches this seam only from a caller with
/// nothing to say, and treating it as operator content would store an empty
/// "You" block.
fn resolve_resume_trigger(
    message: Option<&str>,
    has_queued: bool,
    has_pushes: bool,
) -> ResumeTrigger {
    match message.filter(|text| !text.trim().is_empty()) {
        Some(operator) => ResumeTrigger {
            message: operator.to_string(),
            synthetic: false,
        },
        None if has_queued || has_pushes => ResumeTrigger {
            message: String::new(),
            synthetic: false,
        },
        None => ResumeTrigger {
            message: SYNTHETIC_CONTINUATION_PROMPT.to_string(),
            synthetic: true,
        },
    }
}

/// Prompt for a resume that carries no operator content at all.
///
/// Cairn resumes a job on its own for reasons the operator never authored — an
/// automatic retry, a flush that found nothing pending, a suspension whose
/// result arrives without a message. The wake still needs prompt text, so Cairn
/// writes it. The previous text ("Continue where you left off.") read as a terse
/// operator instruction, and agents obeyed it as one, planning around a
/// directive nobody gave (CAIRN-3175). This text names itself as substrate
/// instead: it says what happened, denies operator intent explicitly, and gives
/// the safe default for a turn that was already finished.
///
/// Sibling to [`RESUME_AFTER_SELF_SUSPEND_NOTE`], which corrects the adjacent
/// misattribution (CAIRN-3162): that note rebuts a *suspension* the CLI rendered
/// as a user rejection, while this text is the *prompt* for a resume nobody
/// asked for. A self-suspend resume with no message carries both.
const SYNTHETIC_CONTINUATION_PROMPT: &str = "Automatic resume: Cairn restarted this turn after the previous one ended without completing. No message accompanies it — nobody asked you to continue, and there is no new instruction here. Pick your own work back up from where it stopped; if it was already finished, close the turn with a short status rather than starting something new.";

/// Short note that leads the forwarded prompt on a durable self-suspend resume.
///
/// On the slow (>45s) path the awaited result is delivered as the suspended tool
/// call's synthetic `tool_result`, so to the resumed agent that call looks
/// interrupted mid-execution and then returns — a deliberate pause that reads
/// like a glitch. Naming it as an intentional suspend up front removes that
/// tension (CAIRN-2173).
const RESUME_AFTER_SELF_SUSPEND_NOTE: &str = "Resuming after an intentional self-suspend. Your run paused itself to wait on work it had already started — delegated work (a sub-agent task or a user question), an explicit wait, or one or more long-running command batches — and resumed now that the work finished. Cairn, not the user, parked those tool calls on purpose. If the CLI described any of them as rejected or interrupted by the user, that wording was a transport artifact; the real results follow, labeled by call when there is more than one.";

fn prepend_resume_stamp(
    prompt: String,
    clock: &crate::clock::HostClock,
    now: chrono::DateTime<chrono::Utc>,
    previous_turn_end: Option<i64>,
) -> String {
    format!(
        "{}\n\n{}",
        clock.resume_prefix(now, previous_turn_end),
        prompt
    )
}

fn previous_turn_end_for_resume(db: Arc<LocalDb>, turn_id: &str) -> Result<Option<i64>, String> {
    let turn_id = turn_id.to_string();
    run_db(async move {
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT predecessor.ended_at
                           FROM turns current
                           LEFT JOIN turns predecessor ON predecessor.id = current.predecessor_id
                          WHERE current.id = ?1
                          LIMIT 1",
                        (turn_id.as_str(),),
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => row.opt_i64(0),
                    None => Ok(None),
                }
            })
        })
        .await
        .map_err(|error| error.to_string())
    })
}

/// Assemble a resume prompt so notes that accumulated before this wake lead and
/// the immediate resume message follows them.
///
/// A `resume_note` (the self-suspend frame) leads everything when present.
/// Queued user follow-ups — including `passive` "quiet" notes that rode along
/// without waking the agent — were authored before the message that triggers
/// this resume, so they precede `base_prompt`. A quiet note "A" sent before a
/// waking message "B" is therefore delivered as "A\n\nB", matching the order the
/// user sent them rather than reversing it. Side-channel notices and resolved
/// attention pushes keep their established position after the immediate
/// message. Falls back to the generic continue placeholder when every part is
/// empty.
fn assemble_resume_prompt(
    resume_note: Option<&str>,
    queued_block: Option<String>,
    base_prompt: &str,
    side_channel_block: Option<String>,
    push_prompt: Option<&str>,
) -> String {
    let mut parts: Vec<String> = Vec::new();
    if let Some(note) = resume_note {
        if !note.is_empty() {
            parts.push(note.to_string());
        }
    }
    if let Some(q) = queued_block {
        if !q.is_empty() {
            parts.push(q);
        }
    }
    if !base_prompt.is_empty() {
        parts.push(base_prompt.to_string());
    }
    if let Some(s) = side_channel_block {
        if !s.is_empty() {
            parts.push(s);
        }
    }
    if let Some(p) = push_prompt {
        if !p.is_empty() {
            parts.push(p.to_string());
        }
    }
    if parts.is_empty() {
        SYNTHETIC_CONTINUATION_PROMPT.to_string()
    } else {
        parts.join("\n\n")
    }
}

// ============================================================================
// Session replacement
// ============================================================================
//
// The policy itself — which continuations may replace a conversation, and how a
// cold prefix is priced — is `cairn_models::continuation`, whose module doc states
// it. What lives here is Cairn's orchestration around it: composing seeds from
// stored transcripts, rotating sessions, and recording the boundary.

/// Header that leads a compacted THREAD seed, framing the three sections that
/// follow. A thread is never finished, so the framing points at what keeps it
/// cheap rather than apologizing for a reconstruction: the arc is the only part
/// of this context nothing can regenerate.
const THREAD_SEED_HEADER: &str = "This thread's session was rebuilt so it stays cheap to run indefinitely. Below: the arc you authored, a table of contents for history that has been compacted away, and the most recent turns verbatim. Every compacted chapter names what it was and where to read it in full — read one only when it bears on what you are doing now, because re-reading costs more than it saves unless the range is genuinely load-bearing. Re-read any file whose content still matters, and keep `cairn:~/arc` current: decisions, the paths you rejected and why, and the open questions are the only things here that cannot be rebuilt from the transcript.";

/// Header that leads the reseed seed prompt, framing the digest that follows.
const RESEED_SEED_HEADER: &str = "The prior events in this session are summarized below because the underlying agent session was reconstructed after a period of inactivity (tool-result bodies were elided). Re-read any files whose content is still relevant, and re-read your working documents (todos/plan/board) for the current plan state before acting.";

/// What a cold native resume makes the provider recreate, in bytes.
///
/// The one denominator both rebuild paths price against, so "what does native
/// resume send" has a single answer rather than two that can drift. `None` when
/// the job has no transcript to measure, which keeps the caller on native resume
/// rather than rebuilding against a number it does not have.
fn verbatim_prefix_bytes(owning_db: &Arc<LocalDb>, job_id: &str) -> Option<i64> {
    run_db({
        let db = owning_db.clone();
        let job_id = job_id.to_string();
        async move {
            Ok::<_, String>(crate::resources::reseed_verbatim_baseline_bytes(&db, &job_id).await)
        }
    })
    .ok()
    .flatten()
}

/// The seed content delivered and stored on a reseed: the header framing the
/// digest, then the digest itself (`header + digest`). The trigger is appended
/// separately at delivery and stored as its own verbatim `user` event.
fn build_reseed_seed_content(digest: &str) -> String {
    format!("{RESEED_SEED_HEADER}\n\n{digest}")
}

/// The seed content delivered and stored when a thread compacts. Same shape as
/// [`build_reseed_seed_content`] — header then body, trigger appended later —
/// so a compacted thread seed remains an ordinary `user:seed` everywhere
/// downstream.
fn build_thread_seed_content(composed: &str, idle_secs: Option<i64>) -> String {
    match idle_secs.filter(|seconds| *seconds >= 60) {
        // A rebuilt session has no felt continuity with the one before it: every
        // relative reference in the turns below ("just now", "still running")
        // was written against a clock the resumed agent cannot see. Naming the
        // gap once, at the top, is what lets it read them correctly.
        Some(seconds) => format!(
            "{THREAD_SEED_HEADER}\n\nYour last turn ended {} ago; everything below was written before that.\n\n{composed}",
            crate::clock::format_elapsed(seconds)
        ),
        None => format!("{THREAD_SEED_HEADER}\n\n{composed}"),
    }
}

/// Prepend the reseed seed to the trigger prompt when this resume reseeded;
/// identity otherwise. Keeps the seed-before-trigger ordering in one place.
fn apply_reseed_seed(prompt: String, reseed: Option<&ReseedOutcome>) -> String {
    match reseed {
        Some(outcome) => format!("{}\n\n{}", outcome.seed_content, prompt),
        None => prompt,
    }
}

/// A successful automatic rebuild: the fresh session that was rotated in, and
/// the seed (header + prior-session digest) to deliver and store.
struct ReseedOutcome {
    new_session_id: String,
    seed_content: String,
}

/// How a continuation resolved, and what reading chose it.
///
/// The refusal is decided deep inside the attempt while the `native_resume`
/// boundary is built far outside it, so declining has to carry its reason back
/// rather than collapsing to a bare `None`. That is what keeps the boundary's
/// label and its detail set by the same branch that chose the operation.
/// What was read about this continuation before anything was composed.
///
/// The three travel together because they are one snapshot of the same moment:
/// how full the window is, how long the session has been quiet, and whether the
/// provider still holds its prefix. Passing them as one value is what keeps the
/// thread and digest attempts deciding from the same reading rather than each
/// taking its own.
#[derive(Debug, Clone, Copy)]
struct ContinuationReadings {
    /// The provider's own accounting of the last request, `(used, window)`.
    occupancy: Option<(i64, i64)>,
    /// How long since this session's last event of any kind. Cosmetic: it tells
    /// a rebuilt agent how much clock its verbatim turns were written before,
    /// and reaches no decision.
    idle_secs: Option<i64>,
    /// Whether the provider still holds this session's prefix.
    prefix_cache: PrefixCacheState,
}

enum ReseedDecision {
    /// A fresh session was rotated in; the caller spawns it and stores the seed.
    /// `Some` names a rebuild that cache economics chose; `None` is a
    /// window-pressure rebuild, which the clock had no part in and whose
    /// boundary payload stays exactly what it has always been.
    Rebuilt(ReseedOutcome, Option<CacheDecision>),
    /// The conversation continues natively. `Some` names a cold prefix that was
    /// priced and kept; `None` is the ordinary warm continuation, which records
    /// nothing new.
    NativeResume(Option<CacheDecision>),
}

/// Attempt the two automatic rebuilds an open session can undergo: the model's
/// window is running out and something has to be given up (CAIRN-2534,
/// CAIRN-4618), or the provider's prompt cache went cold and the bounded seed
/// costs materially less than recreating the whole prefix (CAIRN-4757).
///
/// Fails open in every direction. A composition error, a missing measurement, or
/// a failed rotation all return [`ReseedDecision::NativeResume`] with session
/// state exactly as a native resume would have left it, because rotation is the
/// last and only persisted mutation.
///
/// How long the session has been idle still reaches no viability decision. It
/// enters here twice, and both are economic or cosmetic: `prefix_cache` prices
/// the continuation, and `idle_secs` tells a rebuilt agent how much clock its
/// verbatim turns were written before.
///
/// The digest is rendered from the prior history *before* any rotation or new
/// event, so it can never contain itself.
fn attempt_session_reseed(
    orch: &Orchestrator,
    owning_db: &Arc<LocalDb>,
    job: &DbJob,
    session: &Session,
    now: i64,
    target_backend: Option<&str>,
    prefix_cache: PrefixCacheState,
) -> ReseedDecision {
    let occupancy = session_occupancy(orch, &session.id);
    let readings = ContinuationReadings {
        occupancy,
        idle_secs: run_db(load_last_event_time_for_session(
            owning_db.clone(),
            session.id.clone(),
        ))
        .ok()
        .flatten()
        .map(|last_event_at| now.saturating_sub(last_event_at)),
        prefix_cache,
    };

    // A thread is never finished, so it does not reseed from its whole
    // transcript: it compacts, folding what its children settled into chapters.
    if thread_compaction_capability(owning_db, &job.id).is_enabled() {
        return attempt_thread_compaction(orch, owning_db, job, session, readings, target_backend);
    }

    // One threshold answers one question — what fraction of a window a session
    // may occupy before it gives something up — so an ordinary node reads the
    // same setting a thread does rather than carrying a second knob that would
    // drift from it.
    match reseed_step(
        occupancy,
        orch.get_settings().thread_compact_threshold,
        prefix_cache,
    ) {
        ReseedStep::Resume => return ReseedDecision::NativeResume(None),
        ReseedStep::PriceColdRebuild => {}
        ReseedStep::RebuildForWindow => {
            let Ok(outcome) = force_session_reseed(
                orch,
                owning_db,
                job,
                session,
                target_backend,
                RotationReason::Reseed,
                None,
            ) else {
                return ReseedDecision::NativeResume(None);
            };

            // `reseed_step` rebuilds for the window only on a reading, so a
            // rebuild always has one to report.
            let (used, window) = occupancy.unwrap_or_default();
            log::info!(
            "Window-pressure reseed for job {}: rotated session {} -> {} ({used}/{window} tokens)",
            &job.id[..job.id.len().min(8)],
            &session.id[..session.id.len().min(8)],
            &outcome.new_session_id[..outcome.new_session_id.len().min(8)],
        );
            // Window pressure, not economics: no cache reading participated, so the
            // boundary records none.
            return ReseedDecision::Rebuilt(outcome, None);
        }
    }

    // Past the horizon the two continuations differ only in what they send, so
    // both sides are measured before either is chosen. The baseline is what
    // native resume makes the provider recreate; the seed is what the rebuild
    // would send instead. Composing at all costs a transcript render, which is
    // why nothing above this line pays for it.
    let Some(prefix_bytes) = verbatim_prefix_bytes(owning_db, &job.id) else {
        return ReseedDecision::NativeResume(None);
    };

    let framed = match compose_forced_reseed(orch, owning_db, job, session) {
        Ok(composed) => composed.framed().to_string(),
        Err(error) => {
            log::warn!(
                "Cold-cache seed composition failed for job {}; resuming natively: {error}",
                &job.id[..job.id.len().min(8)]
            );
            return ReseedDecision::NativeResume(None);
        }
    };
    let seed_bytes = framed.len() as i64;

    let decision = match price_cold_rebuild(seed_bytes, prefix_bytes) {
        ColdRebuild::Pays(decision) => decision,
        ColdRebuild::Declined(decision) => {
            log::info!(
                "Cold prefix for job {} kept its native conversation: a rebuild would send \
             {seed_bytes} bytes against {prefix_bytes}, below the {COLD_RESEED_MIN_SAVINGS_RATIO} \
             savings floor",
                &job.id[..job.id.len().min(8)],
            );
            return ReseedDecision::NativeResume(Some(decision));
        }
    };

    match rotate_with_seed(
        orch,
        owning_db,
        job,
        session,
        framed,
        Rotation {
            reason: RotationReason::CacheExpiry,
            target_backend,
            boundary: None,
        },
    ) {
        Ok(outcome) => {
            log::info!(
                "Cold-cache reseed for job {}: rotated session {} -> {} \
                 ({prefix_bytes} bytes -> {seed_bytes} bytes)",
                &job.id[..job.id.len().min(8)],
                &session.id[..session.id.len().min(8)],
                &outcome.new_session_id[..outcome.new_session_id.len().min(8)],
            );
            ReseedDecision::Rebuilt(outcome, Some(decision))
        }
        Err(error) => {
            log::warn!(
                "Cold-cache reseed failed for job {}; resuming natively: {error}",
                &job.id[..job.id.len().min(8)]
            );
            ReseedDecision::NativeResume(None)
        }
    }
}

/// A forced reseed's seed, composed but not yet applied.
///
/// This is the whole read-only half of [`force_session_reseed`]: everything up to
/// the rotation that makes it durable. It exists as its own value so the size an
/// operator is shown before pressing the action and the seed the action actually
/// delivers are measurements of one construction, not two that can drift apart.
enum ComposedReseed {
    /// A thread compacts. The composition also carries the accounting its
    /// generation record needs, so applying it costs no second composition.
    Thread {
        seed: Box<crate::resources::ThreadSeed>,
        framed: String,
    },
    /// An ordinary node reseeds from its whole transcript at full fidelity.
    Digest { framed: String },
}

impl ComposedReseed {
    /// The seed content exactly as rotation would deliver and store it: framing
    /// header first, then the composed body.
    fn framed(&self) -> &str {
        match self {
            ComposedReseed::Thread { framed, .. } | ComposedReseed::Digest { framed } => framed,
        }
    }
}

/// Why a forced reseed could not be composed.
enum ComposeRefusal {
    /// An ordinary node with no transcript to digest. Distinct from a failure:
    /// there is nothing to carry, rather than something that could not be read.
    NoTranscript,
    Failed(String),
}

impl std::fmt::Display for ComposeRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ComposeRefusal::NoTranscript => f.write_str(
                "Cannot resume from digest because this job has no resumable transcript.",
            ),
            ComposeRefusal::Failed(error) => f.write_str(error),
        }
    }
}

impl From<ComposeRefusal> for String {
    fn from(refusal: ComposeRefusal) -> Self {
        refusal.to_string()
    }
}

/// Compose the seed a forced digest resume would deliver, without mutating
/// anything. Fails rather than returning a partial seed, exactly as the automatic
/// reseed paths do.
fn compose_forced_reseed(
    orch: &Orchestrator,
    owning_db: &Arc<LocalDb>,
    job: &DbJob,
    session: &Session,
) -> Result<ComposedReseed, ComposeRefusal> {
    // An operator forcing a digest resume on a thread gets the thread's own seed
    // shape; there is only one way to reconstruct a thread's context.
    if thread_compaction_capability(owning_db, &job.id).is_enabled() {
        let seed =
            compose_thread_seed_blocking(orch, owning_db, job).map_err(ComposeRefusal::Failed)?;
        let idle_secs = run_db(load_last_event_time_for_session(
            owning_db.clone(),
            session.id.clone(),
        ))
        .ok()
        .flatten()
        .map(|last_event_at| orch.services.clock.now().saturating_sub(last_event_at));
        let framed = build_thread_seed_content(&seed.content, idle_secs);
        return Ok(ComposedReseed::Thread {
            seed: Box::new(seed),
            framed,
        });
    }

    let digest = {
        let db = owning_db.clone();
        let job = job.clone();
        run_db(async move { crate::resources::render_reseed_digest(&db, &job).await })
            .map_err(ComposeRefusal::Failed)?
    };
    if digest.trim().is_empty() || digest == "No runs found for this node." {
        // The renderer drops rows it cannot read, so an empty rendering is not
        // evidence of an empty history. Only a direct count of the job's event
        // rows can say there is nothing to carry; anything else fails closed.
        return Err(empty_digest_refusal(job_event_rows_exist(
            owning_db, &job.id,
        )));
    }
    Ok(ComposedReseed::Digest {
        framed: build_reseed_seed_content(&digest),
    })
}

/// What an empty digest rendering means, given a direct read of whether the job
/// has any event rows at all. Only a successful read that found none is an
/// empty history; rows the renderer could not turn into content, or a read that
/// failed, are a failure to compose.
fn empty_digest_refusal(event_rows_exist: Result<bool, String>) -> ComposeRefusal {
    match event_rows_exist {
        Ok(false) => ComposeRefusal::NoTranscript,
        Ok(true) => ComposeRefusal::Failed(
            "Cannot resume from digest: this job's transcript rows could not be rendered."
                .to_string(),
        ),
        Err(error) => ComposeRefusal::Failed(format!(
            "Cannot resume from digest: could not read this job's transcript: {error}"
        )),
    }
}

/// Whether any of the job's runs carries an event row, read so a failure is an
/// error rather than an empty answer.
fn job_event_rows_exist(owning_db: &Arc<LocalDb>, job_id: &str) -> Result<bool, String> {
    let db = owning_db.clone();
    let job_id = job_id.to_string();
    run_db(async move {
        db.query_opt_i64(
            "SELECT 1 FROM events e JOIN runs r ON r.id = e.run_id WHERE r.job_id = ?1 LIMIT 1",
            (job_id,),
        )
        .await
        .map(|row| row.is_some())
        .map_err(|error| error.to_string())
    })
}

fn force_session_reseed(
    orch: &Orchestrator,
    owning_db: &Arc<LocalDb>,
    job: &DbJob,
    session: &Session,
    target_backend: Option<&str>,
    reason: RotationReason,
    boundary: Option<&OperatorBoundary>,
) -> Result<ReseedOutcome, String> {
    let rotation = Rotation {
        reason,
        target_backend,
        boundary,
    };
    // Construct the complete seed before any process or session mutation.
    let composed = compose_forced_reseed(orch, owning_db, job, session)?;
    apply_composed_reseed(orch, owning_db, job, session, composed, rotation)
}

/// Apply an already-composed forced reseed: compact a thread, or rotate an
/// ordinary node onto its digest.
fn apply_composed_reseed(
    orch: &Orchestrator,
    owning_db: &Arc<LocalDb>,
    job: &DbJob,
    session: &Session,
    composed: ComposedReseed,
    rotation: Rotation<'_>,
) -> Result<ReseedOutcome, String> {
    // A thread's compaction generation records why it was rebuilt. Every
    // forced reseed but an unconfirmed start is recorded as the operator's.
    let trigger = match rotation.reason {
        RotationReason::UnconfirmedStart => CompactionTrigger::UnconfirmedStart,
        _ => CompactionTrigger::Manual,
    };
    match composed {
        ComposedReseed::Thread { seed, framed } => apply_thread_compaction(
            orch,
            owning_db,
            job,
            session,
            DecidedCompaction {
                seed: *seed,
                framed,
                trigger,
                occupancy: None,
            },
            rotation,
        ),
        ComposedReseed::Digest { framed } => {
            rotate_with_seed(orch, owning_db, job, session, framed, rotation)
        }
    }
}

/// Stop the live process and rotate to a fresh session carrying `seed_content`.
///
/// The one mutating half of every reseed, thread or not. Rotation is the last
/// and only persisted step, so any failure before it leaves session state
/// exactly as a native resume would have.
///
/// An operator Compact's `boundary` is written by the rotation transaction
/// itself, measured from the seed it is about to deliver.
fn rotate_with_seed(
    orch: &Orchestrator,
    owning_db: &Arc<LocalDb>,
    job: &DbJob,
    session: &Session,
    seed_content: String,
    rotation: Rotation<'_>,
) -> Result<ReseedOutcome, String> {
    let new_session_id =
        rotate_job_onto_successor(orch, owning_db, job, session, rotation, seed_content.len())?;
    Ok(ReseedOutcome {
        new_session_id,
        seed_content,
    })
}

/// A successor for a session whose first launch never confirmed, and the seed
/// it starts from when there was anything to compose.
struct UnconfirmedStartRotation {
    new_session_id: String,
    seed: Option<String>,
}

/// Replace a session whose provider was launched under its id but never
/// confirmed it. A thread gets its thread seed and any other node its digest,
/// as a forced reseed would. A node with no transcript rotates without a seed:
/// the provider holds no conversation worth carrying, and the successor's fresh
/// launch assembles its first prompt exactly as a retry would have, under a new
/// id. Any other composition failure is returned with session state untouched.
fn rotate_unconfirmed_start(
    orch: &Orchestrator,
    owning_db: &Arc<LocalDb>,
    job: &DbJob,
    session: &Session,
    target_backend: Option<&str>,
) -> Result<UnconfirmedStartRotation, String> {
    let rotation = Rotation {
        reason: RotationReason::UnconfirmedStart,
        target_backend,
        boundary: None,
    };
    match compose_forced_reseed(orch, owning_db, job, session) {
        Ok(composed) => {
            let outcome = apply_composed_reseed(
                orch,
                owning_db,
                job,
                session,
                composed,
                rotation,
            )?;
            Ok(UnconfirmedStartRotation {
                new_session_id: outcome.new_session_id,
                seed: Some(outcome.seed_content),
            })
        }
        Err(ComposeRefusal::NoTranscript) => Ok(UnconfirmedStartRotation {
            new_session_id: rotate_job_onto_successor(
                orch,
                owning_db,
                job,
                session,
                rotation,
                0,
            )?,
            seed: None,
        }),
        Err(ComposeRefusal::Failed(error)) => Err(format!(
            "Could not compose a seed to replace session {}, whose first launch never confirmed: {error}",
            session.id
        )),
    }
}

/// Whether `session` carries the unconfirmed-launch mark on a backend whose
/// fresh start claims Cairn's session id. A read failure reads as unmarked,
/// which keeps today's same-id retry.
fn session_has_unconfirmed_spawned_launch(owning_db: &Arc<LocalDb>, session: &Session) -> bool {
    if session.backend_id.is_some()
        || !crate::backends::backend_for_name(Some(&session.backend))
            .fresh_start_claims_session_id()
    {
        return false;
    }
    let db = owning_db.clone();
    let session_id = session.id.clone();
    run_db(async move {
        crate::sessions::queries::session_has_unconfirmed_spawned_launch(&db, &session_id)
            .await
            .map_err(|error| error.to_string())
    })
    .unwrap_or_else(|error| {
        log::warn!(
            "Could not read the launch history of session {}; retrying its start: {error}",
            session.id
        );
        false
    })
}

/// Stop the session's live process, if any, and rotate the job onto a fresh
/// successor session. Returns the successor's id.
///
/// Every rotation cause enters here, carried by one [`Rotation`]. An operator
/// Compact's boundary is written by the rotation transaction itself, as a
/// `digest_reseed` of `seed_bytes`, the seed the successor is about to receive.
fn rotate_job_onto_successor(
    orch: &Orchestrator,
    owning_db: &Arc<LocalDb>,
    job: &DbJob,
    session: &Session,
    rotation: Rotation<'_>,
    seed_bytes: usize,
) -> Result<String, String> {
    let Rotation {
        reason,
        target_backend,
        boundary,
    } = rotation;
    let commit = boundary.map(|anchor| crate::sessions::queries::RotationCommit {
        model: None,
        boundary: Some(crate::sessions::queries::BoundaryCommit {
            run_id: anchor.run_id.clone(),
            turn_id: anchor.turn_id.clone(),
            payload: SessionBoundary::digest_reseed(session.backend.clone(), seed_bytes)
                .initiated_by_operator()
                .raw(),
        }),
    });
    if let Some(old_run) = orch.process_state.find_process_by_session(&session.id) {
        // Rotation ends the old physical process even though the logical job
        // continues. Release its provider-runtime claim before dropping the
        // retained handle, then let the successor acquire its own generation.
        crate::orchestrator::lifecycle::release_and_stop_process(orch, &old_run);
    }

    let new_session = run_db({
        let db = owning_db.clone();
        let session = session.clone();
        let job_id = job.id.clone();
        let emitter = orch.services.emitter.clone();
        // The successor inherits the source backend unless a switch was actually
        // requested — the same rule `rotate_session` applies. Defaulting to
        // Claude instead would move a Codex session onto Claude whenever the
        // caller had no desired backend to name (nothing to compare against),
        // which is reachable now that a cold Codex prefix can rebuild.
        let target_backend = target_backend.unwrap_or(&session.backend).to_string();
        async move {
            crate::sessions::queries::rotate_job_session_with_backend(
                db.as_ref(),
                &session,
                &job_id,
                &target_backend,
                reason,
                commit,
                emitter.as_ref(),
            )
            .await
            .map_err(|error| format!("Failed to rotate session onto a successor: {error}"))
        }
    })?;

    Ok(new_session.id)
}

/// How a reseed rotates its session: why, onto which backend, and, for an
/// operator Compact, the boundary the rotation transaction itself writes.
#[derive(Clone, Copy)]
struct Rotation<'a> {
    reason: RotationReason,
    /// `None` keeps the source session's backend.
    target_backend: Option<&'a str>,
    boundary: Option<&'a OperatorBoundary>,
}

/// Whether this job's session compacts as a thread. One resolution point, in
/// `crate::threads`; a failure to resolve reads as "not a thread", which keeps
/// the ordinary path.
fn thread_compaction_capability(owning_db: &Arc<LocalDb>, job_id: &str) -> ThreadCompaction {
    let db = owning_db.clone();
    let job_id = job_id.to_string();
    run_db(async move {
        Ok::<_, String>(crate::threads::compaction_capability_for_job(&db, &job_id).await)
    })
    .unwrap_or(ThreadCompaction::Disabled)
}

/// The session's live context occupancy, as the provider last reported it.
///
/// `None` — no snapshot yet, an unrecognized model, a backend catalog without a
/// window — leaves a warm session alone rather than guessing at a number this
/// lossy a decision turns on.
fn session_occupancy(orch: &Orchestrator, session_id: &str) -> Option<(i64, i64)> {
    let orch = orch.clone();
    let session_id = session_id.to_string();
    let state =
        run_db(async move { Ok::<_, String>(orch.get_context_token_state(&session_id).await) })
            .ok()
            .flatten()?;
    Some((state.used_tokens, state.context_window?))
}

fn compose_thread_seed_blocking(
    orch: &Orchestrator,
    owning_db: &Arc<LocalDb>,
    job: &DbJob,
) -> Result<crate::resources::ThreadSeed, String> {
    let db = owning_db.clone();
    let job_id = job.id.clone();
    let now = orch.services.clock.now();
    let settings = orch.get_settings();
    let snapshot_policy = match (job.execution_id.clone(), job.agent_config_id.clone()) {
        (Some(execution_id), Some(agent_config_id)) => run_db({
            let db = owning_db.clone();
            async move {
                Ok::<_, String>(
                    super::snapshots::load_thread_seed_policy(db, execution_id, agent_config_id)
                        .await,
                )
            }
        })
        .ok()
        .flatten(),
        _ => None,
    };
    let policy = crate::resources::SeedPolicy {
        budget_tokens: snapshot_policy
            .and_then(|(budget, _)| (budget > 0).then_some(budget))
            .unwrap_or(settings.seed_budget_tokens)
            .max(1) as usize,
        inline_eras: snapshot_policy
            .and_then(|(_, eras)| (eras >= 0).then_some(eras))
            .unwrap_or(settings.seed_eras)
            .max(0) as usize,
    };
    run_db(async move {
        crate::resources::compose_thread_seed_with_policy(&db, &job_id, now, policy).await
    })
}

/// Compose a thread's seed, decide whether this continuation compacts, and apply
/// it if so.
///
/// Composition is read-only and happens only once a reading has already decided
/// a rebuild is on the table — the window is running out, or the prefix has gone
/// cold. On the overwhelmingly common continuation both readings say no and this
/// costs one cached lookup, where it used to compose an entire thread seed on
/// every wake and throw it away.
///
/// The two triggers are guarded differently, because they are different
/// obligations. Capacity has to give something up whatever it costs, so it asks
/// only that the compaction replace *something*. Expiry has a choice, so it must
/// show the saving before it spends the thread's continuity on one.
fn attempt_thread_compaction(
    orch: &Orchestrator,
    owning_db: &Arc<LocalDb>,
    job: &DbJob,
    session: &Session,
    readings: ContinuationReadings,
    target_backend: Option<&str>,
) -> ReseedDecision {
    let ContinuationReadings {
        occupancy,
        idle_secs,
        prefix_cache,
    } = readings;
    let capacity = window_pressure_reached(occupancy, orch.get_settings().thread_compact_threshold);
    let cold = prefix_cache == PrefixCacheState::Cold;
    if !capacity && !cold {
        return ReseedDecision::NativeResume(None);
    }

    let seed = match compose_thread_seed_blocking(orch, owning_db, job) {
        Ok(seed) => seed,
        Err(error) => {
            // Fail open, exactly as the ordinary reseed attempt does: the old
            // session and its current pointer stay usable.
            log::warn!(
                "Thread seed composition failed for job {}; resuming natively: {error}",
                &job.id[..job.id.len().min(8)]
            );
            return ReseedDecision::NativeResume(None);
        }
    };

    let framed = build_thread_seed_content(&seed.content, idle_secs);
    let seed_bytes = framed.len() as i64;
    // A rotation that replaces nothing folds no history and spends the thread's
    // continuity for it, so it is worse than doing nothing whatever triggered it.
    // A continuity guard, not an economic one: no saving buys past it.
    if seed.source_bytes == 0 {
        return ReseedDecision::NativeResume(if capacity {
            None
        } else {
            Some(CacheDecision::refused_nothing_to_fold(prefix_cache))
        });
    }

    // Capacity carries no cache decision: the clock had no part in choosing it,
    // so its boundary payload stays exactly what it has always been.
    let (trigger, rotation_reason, cache_decision) = if capacity {
        (
            CompactionTrigger::Capacity,
            RotationReason::Compaction,
            None,
        )
    } else {
        // The denominator is what native resume actually re-sends, measured the
        // same way the digest path measures it. Deliberately NOT derived from
        // `source_bytes`: that field is the dropped turns rendered through the
        // seed's OWN digest projection, which already elides tool-result bodies,
        // so pricing against it would compare a seed with a seed and hide most
        // of what a compaction actually saves.
        let Some(prefix_bytes) = verbatim_prefix_bytes(owning_db, &job.id) else {
            return ReseedDecision::NativeResume(None);
        };
        let decision = match price_cold_rebuild(seed_bytes, prefix_bytes) {
            ColdRebuild::Pays(decision) => decision,
            ColdRebuild::Declined(decision) => {
                log::info!(
                    "Cold prefix for thread {} kept its native conversation: a compaction would \
                 send {seed_bytes} bytes against {prefix_bytes}, below the \
                 {COLD_RESEED_MIN_SAVINGS_RATIO} savings floor",
                    &job.id[..job.id.len().min(8)],
                );
                return ReseedDecision::NativeResume(Some(decision));
            }
        };
        (
            CompactionTrigger::Expiry,
            RotationReason::CacheExpiry,
            Some(decision),
        )
    };

    let decided = DecidedCompaction {
        seed,
        framed,
        trigger,
        occupancy,
    };
    match apply_thread_compaction(
        orch,
        owning_db,
        job,
        session,
        decided,
        Rotation {
            reason: rotation_reason,
            target_backend,
            boundary: None,
        },
    ) {
        Ok(outcome) => ReseedDecision::Rebuilt(outcome, cache_decision),
        Err(error) => {
            log::warn!(
                "Thread compaction failed for job {}; resuming natively: {error}",
                &job.id[..job.id.len().min(8)]
            );
            ReseedDecision::NativeResume(None)
        }
    }
}

/// A compaction that has been decided on: what to send, why, and the reading
/// that caused it. Travelling together is what keeps them consistent — the bytes
/// the generation records are the bytes rotation delivers, and the occupancy the
/// log reports is the occupancy the decision turned on.
struct DecidedCompaction {
    seed: crate::resources::ThreadSeed,
    /// The seed exactly as it will be delivered, header and bridge line applied.
    framed: String,
    trigger: CompactionTrigger,
    occupancy: Option<(i64, i64)>,
}

/// Persist the generation, then rotate.
///
/// Order matters: the marks a seed folded into chapters are consumed in the same
/// transaction that records the chapters, before any session moves, because the
/// seed must be on record by the time it can reach an agent. The generation
/// records the session it was composed from, and rotation is what makes it
/// applied — so a rotation that fails leaves it pending, its marks still
/// eligible and its chapters still unclaimed, and the next continuation decides
/// exactly as it would have if this attempt had never happened.
fn apply_thread_compaction(
    orch: &Orchestrator,
    owning_db: &Arc<LocalDb>,
    job: &DbJob,
    session: &Session,
    decided: DecidedCompaction,
    rotation: Rotation<'_>,
) -> Result<ReseedOutcome, String> {
    let DecidedCompaction {
        seed,
        framed,
        trigger,
        occupancy,
    } = decided;
    let seed_bytes = framed.len() as i64;
    let first_minted_seq = seed.eras_through_seq.unwrap_or(0) + 1;
    let minted_count = seed.minted_eras.len();
    let applied = crate::threads::compaction::AppliedCompaction {
        trigger,
        source_session_id: seed.source_session_id,
        entries: seed.entries,
        seed_bytes,
        source_bytes: seed.source_bytes,
        candidate_bytes: seed.candidate_bytes,
        compacted_through_block: seed.compacted_through_block,
        recency_start_block: seed.recency_start_block,
        consumed_child_issue_ids: seed.consumed_child_issue_ids,
        minted_eras: seed.minted_eras,
        arc_bytes: seed.arc_bytes,
        eras_through_seq: seed.eras_through_seq,
    };
    let now = orch.services.clock.now();
    run_db({
        let db = owning_db.clone();
        let job_id = job.id.clone();
        async move {
            crate::threads::compaction::persist_generation(&db, &job_id, &applied, now)
                .await
                .map_err(|error| format!("Failed to persist a thread compaction: {error}"))
        }
    })?;
    for offset in 0..minted_count {
        orch.enqueue_thread_era_summary(&job.id, (first_minted_seq + offset as i64) as i32);
    }

    let outcome = rotate_with_seed(orch, owning_db, job, session, framed, rotation)?;

    // Occupancy rides in the log line because it is what the warm decision turns
    // on; the byte counts describe what the rebuild did, never why it happened.
    let occupancy = match occupancy {
        Some((used, window)) if window > 0 => {
            format!("{used}/{window} tokens ({}%)", used * 100 / window)
        }
        _ => "occupancy unread".to_string(),
    };
    log::info!(
        "Thread compaction ({}) for job {}: {} chapters, {} bytes -> {} bytes, \
         framed seed {} bytes, {}, session {} -> {}",
        trigger.as_db(),
        &job.id[..job.id.len().min(8)],
        seed.new_entries,
        seed.source_bytes,
        seed.candidate_bytes,
        seed_bytes,
        occupancy,
        &session.id[..session.id.len().min(8)],
        &outcome.new_session_id[..outcome.new_session_id.len().min(8)],
    );

    Ok(outcome)
}

/// Outcome of reconciling a reusable process against the job's requested model.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ReuseDecision {
    /// The live process already matches (or can serve) the requested model — reuse it.
    Reuse,
    /// The live process cannot serve the requested model and must be restarted.
    Restart,
}

/// Reconcile a reusable process against the session's durable account pin.
///
/// A process's provider credentials are immutable after spawn, so a moved pin
/// can only be honoured by a cold restart, and only an idle child may be
/// evicted for one.
fn ensure_reused_process_account(
    process_state: &crate::agent_process::process::AgentProcessState,
    run_id: &str,
    desired_account: Option<&str>,
) -> Result<ReuseDecision, String> {
    if process_state.get_account_id(run_id).as_deref() == desired_account {
        return Ok(ReuseDecision::Reuse);
    }
    match process_state.get_occupancy(run_id) {
        Some(crate::agent_process::process::RunOccupancy::Idle) | None => {
            Ok(ReuseDecision::Restart)
        }
        Some(other) => Err(format!(
            "Cannot change provider account for run {}: process is busy ({:?})",
            &run_id[..run_id.len().min(8)],
            other
        )),
    }
}

/// Reconcile a reusable warm/active process against the job's requested model.
///
/// `jobs.model` and the model recorded on the live process (`RunHandle.model`,
/// set at startup) are the source of truth. By the time this runs, any
/// cross-backend change has already been handled by session rotation in
/// `continue_job_impl`, so a process found for the current session is guaranteed
/// to be on the right backend — only the model can differ here.
///
/// A model change resolves to `Restart` rather than an in-place `set_model`. A
/// live switch is not trusted: for Claude the accept/reject arrives later as an
/// async control response, so reusing the process immediately would risk running
/// the next turn on a stale model and recording a model the process never
/// confirmed. Restart (cold resume of the same session with the new model) is
/// deterministic. If a restart would be required but the process is not idle,
/// returns an error rather than killing an active or host-blocked turn.
fn ensure_reused_process_model(
    process_state: &crate::agent_process::process::AgentProcessState,
    run_id: &str,
    desired_model: Option<&str>,
) -> Result<ReuseDecision, String> {
    // No requested model → nothing to reconcile; reuse as-is.
    let Some(desired) = desired_model else {
        return Ok(ReuseDecision::Reuse);
    };

    // Already serving the requested model → reuse.
    if process_state.get_model(run_id).as_deref() == Some(desired) {
        return Ok(ReuseDecision::Reuse);
    }

    // Model diverged → restart deterministically, but refuse to tear down a
    // process that is mid-turn or host-blocked.
    match process_state.get_occupancy(run_id) {
        Some(crate::agent_process::process::RunOccupancy::Idle) | None => {
            log::info!(
                "Model changed to {} for run {}; restarting to apply it",
                desired,
                &run_id[..run_id.len().min(8)]
            );
            Ok(ReuseDecision::Restart)
        }
        Some(other) => Err(format!(
            "Cannot change model to {} for run {}: process is busy ({:?})",
            desired,
            &run_id[..run_id.len().min(8)],
            other
        )),
    }
}

struct ActiveTurnForContinue {
    turn_id: String,
    state: TurnState,
    run_id: Option<String>,
}

fn load_active_turn_for_continue(
    db: Arc<LocalDb>,
    job_id: String,
) -> Result<Option<ActiveTurnForContinue>, String> {
    run_db(async move {
        db.read(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT id, state, run_id
                         FROM turns
                         WHERE job_id = ?1
                           AND state IN ('pending', 'running')
                         ORDER BY sequence DESC
                         LIMIT 1",
                        (job_id.as_str(),),
                    )
                    .await?;
                rows.next()
                    .await?
                    .map(|row| {
                        let state = row.text(1)?.parse().map_err(db_internal)?;
                        Ok(ActiveTurnForContinue {
                            turn_id: row.text(0)?,
                            state,
                            run_id: row.opt_text(2)?,
                        })
                    })
                    .transpose()
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
}

fn mark_stale_active_turn_for_continue(
    db: Arc<LocalDb>,
    active: ActiveTurnForContinue,
) -> Result<(), String> {
    run_db(async move {
        db.write(|conn| {
            let turn_id = active.turn_id.clone();
            let state = active.state.clone();
            let run_id = active.run_id.clone();
            Box::pin(async move {
                let now = chrono::Utc::now().timestamp();
                let target_state = match &state {
                    TurnState::Running => TurnState::Interrupted,
                    TurnState::Pending => TurnState::Cancelled,
                    _ => return Ok(()),
                };

                conn.execute(
                    "UPDATE turns
                     SET state = ?1,
                         ended_at = ?2,
                         updated_at = ?2
                     WHERE id = ?3
                       AND state = ?4",
                    params![
                        target_state.to_string(),
                        now,
                        turn_id.as_str(),
                        state.to_string()
                    ],
                )
                .await?;

                if let Some(run_id) = run_id.as_deref() {
                    conn.execute(
                        "UPDATE runs
                         SET status = 'crashed',
                             exit_reason = 'stale_continue_recovery',
                             exited_at = ?1,
                             updated_at = ?1
                         WHERE id = ?2
                           AND status IN ('starting', 'live', 'running', 'idle')",
                        params![now, run_id],
                    )
                    .await?;
                }

                Ok(())
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
}

fn reconcile_stale_active_turn_for_continue(
    orch: &Orchestrator,
    job_id: &str,
    session_id: &str,
) -> Result<bool, String> {
    if orch
        .process_state
        .find_process_by_session(session_id)
        .is_some()
    {
        return Ok(false);
    }

    let owning_db = run_db({
        let dbs = orch.db.clone();
        let job_id = job_id.to_string();
        async move {
            crate::execution::routing::owning_db_for_job(&dbs, &job_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;
    let Some(active) = load_active_turn_for_continue(owning_db.clone(), job_id.to_string())? else {
        return Ok(false);
    };

    if active
        .run_id
        .as_deref()
        .and_then(|run_id| orch.process_state.get_current_turn_id(run_id))
        .is_some()
    {
        return Ok(false);
    }

    let turn_id = active.turn_id.clone();
    let run_id = active.run_id.clone();
    let state = active.state.clone();
    mark_stale_active_turn_for_continue(owning_db.clone(), active)?;
    log::warn!(
        "Recovered stale {} turn {} for job {} before continue",
        state,
        turn_id,
        job_id
    );
    let turn_change = run_db({
        let db = owning_db.clone();
        let turn_id = turn_id.clone();
        async move { Ok(crate::notify::turn_db_change_for_id(&db, &turn_id, "update").await) }
    })?;
    let _ = orch.services.emitter.emit("db-change", turn_change);
    if let Some(run_id) = run_id.as_deref() {
        let run_change = run_db({
            let db = owning_db.clone();
            let run_id = run_id.to_string();
            async move { Ok(crate::notify::run_db_change_for_id(&db, &run_id, "update").await) }
        })?;
        let _ = orch.services.emitter.emit("db-change", run_change);
    }
    Ok(true)
}

#[cfg(any(test, feature = "test-utils"))]
pub fn reconcile_stale_active_turn_for_continue_for_test(
    orch: &Orchestrator,
    job_id: &str,
    session_id: &str,
) -> Result<bool, String> {
    reconcile_stale_active_turn_for_continue(orch, job_id, session_id)
}

#[cfg(test)]
mod tests {
    use super::{
        apply_reseed_seed, assemble_resume_prompt, build_reseed_seed_content,
        ensure_reused_process_account, ensure_reused_process_model, prepend_resume_stamp,
        previous_turn_end_for_resume, push_job_coordinate, resolve_resume_trigger,
        PendingLaunchTurnGuard, PushJobCoordinate, ReseedDecision, ReseedOutcome, ReuseDecision,
        SessionBoundary, RESEED_SEED_HEADER, SYNTHETIC_CONTINUATION_PROMPT, THREAD_SEED_HEADER,
    };
    use crate::agent_process::process::{AgentProcessState, RunHandle, RunOccupancy};
    use crate::db::DbState;
    use crate::orchestrator::Orchestrator;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::migrated_test_db;
    use crate::storage::SearchIndex;
    use cairn_db::turso::params;
    use cairn_models::continuation::{
        decide_continue_action, prefix_cache_state, window_pressure_reached, ContinueAction,
        PrefixCacheState, ResumeBackend, COLD_RESEED_MIN_SAVINGS_RATIO,
    };
    use std::sync::{Arc, Mutex};

    #[test]
    fn session_boundary_payloads_use_the_transcript_contract() {
        let boundaries = [
            SessionBoundary::provider_change("codex".into(), "claude".into(), Some("fable"), 100),
            SessionBoundary::digest_reseed("claude".into(), 200),
            SessionBoundary::native_resume(Some("opus"), Some("claude")),
            SessionBoundary::fresh(Some("gpt-5.6-sol"), Some("codex")),
        ];
        let kinds: Vec<_> = boundaries
            .iter()
            .map(|boundary| boundary.raw()["kind"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(
            kinds,
            [
                "provider_change",
                "digest_reseed",
                "native_resume_new_process",
                "fresh",
            ]
        );
        assert_eq!(boundaries[0].raw()["model"], "fable");
        assert_eq!(boundaries[1].raw()["seedBytes"], 200);
        // A reason is written only where one was given, so every existing
        // boundary payload stays byte-identical.
        for boundary in &boundaries {
            assert!(boundary.raw().get("reason").is_none());
        }
        let unconfirmed = SessionBoundary::digest_reseed("claude".into(), 200)
            .with_reason(super::UNCONFIRMED_FIRST_LAUNCH_BOUNDARY_REASON);
        assert_eq!(unconfirmed.raw()["kind"], "digest_reseed");
        assert_eq!(unconfirmed.raw()["reason"], "unconfirmed_first_launch");
    }

    fn test_orchestrator(db: crate::storage::LocalDb) -> Orchestrator {
        let root = tempfile::tempdir().unwrap().keep();
        let state = Arc::new(DbState::new(
            Arc::new(db),
            Arc::new(SearchIndex::open_or_create(root.join("search.db")).unwrap()),
        ));
        Orchestrator::builder(state, Arc::new(TestServicesBuilder::new().build()), root).build()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn precreated_successor_is_live_during_prologue_and_cancelled_on_launch_failure() {
        let orch = test_orchestrator(migrated_test_db("precreated-successor-rollback").await);
        let db = orch.db.local.clone();
        db.execute_script(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
               VALUES('p','w','P','p','/tmp/p',1,1);
             INSERT INTO jobs(id,project_id,status,current_session_id,created_at,updated_at)
               VALUES('j','p','running','s',1,1);
             INSERT INTO sessions(id,job_id,backend,status,sequence,created_at,updated_at)
               VALUES('s','j','claude','open',1,1,1);
             INSERT INTO turns(id,session_id,job_id,sequence,state,start_reason,created_at,ended_at,updated_at)
               VALUES('predecessor','s','j',1,'complete','initial',1,2,2);
             UPDATE jobs SET current_turn_id='predecessor' WHERE id='j';",
        )
        .await
        .unwrap();

        let successor = super::create_followup_turn(&orch, "s", "j", true, false).unwrap();
        assert!(crate::messages::delivery::head_turn_active_sync(&db, "j"));
        let head = crate::turns::queries::get_head_turn(&db, "j")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(head.id, successor);
        assert_eq!(head.state, crate::models::TurnState::Pending);

        orch.process_state
            .register_process(
                "r".to_string(),
                RunHandle::test_handle(Some("s"), Some("j")),
            )
            .unwrap();
        orch.process_state.transition_to_active("r");
        orch.process_state.begin_turn("r", &successor);
        {
            let mut guard = PendingLaunchTurnGuard {
                orch: &orch,
                db: db.clone(),
                turn_id: Some(successor.clone()),
                run_id: None,
                inserted_run_id: None,
                wrote_running_job_id: None,
                unwind_preclaimed: None,
            };
            guard.started("r");
        }
        assert_eq!(
            orch.process_state.get_occupancy("r"),
            Some(RunOccupancy::Idle),
            "a failed launch must return the process to warm occupancy"
        );
        let head = crate::turns::queries::get_head_turn(&db, "j")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(head.id, successor);
        assert_eq!(head.state, crate::models::TurnState::Cancelled);
        assert!(!crate::messages::delivery::head_turn_active_sync(&db, "j"));
    }

    /// The cairn/4710/4742/4908 leak: a push arriving seconds after a merge
    /// nudged a `complete` builder whose session the merge had closed. The launch
    /// refused, but only after writing a follow-up turn and `running`. Refused
    /// now, it writes nothing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_closed_session_refuses_before_writing_a_turn_or_running() {
        let orch = test_orchestrator(migrated_test_db("closed-session-refusal").await);
        let db = orch.db.local.clone();
        db.execute_script(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','P','p','/tmp/p',1,1);
             INSERT INTO issues(id,project_id,number,title,status,progress,attention,merged_at,created_at,updated_at) VALUES('i','p',1,'I','merged','merged','none',1,1,1);
             INSERT INTO jobs(id,project_id,issue_id,status,current_session_id,created_at,updated_at) VALUES('j','p','i','complete','s',1,1);
             INSERT INTO sessions(id,job_id,backend,status,terminal_reason,sequence,created_at,updated_at) VALUES('s','j','claude','closed','issue_merged',1,1,1);
             INSERT INTO turns(id,session_id,job_id,sequence,state,start_reason,created_at,ended_at,updated_at) VALUES('t','s','j',1,'complete','initial',1,2,2);",
        )
        .await
        .unwrap();

        let error = super::continue_job_impl(&orch, "j", None, None, None)
            .expect_err("a closed session takes no turn");
        assert!(error.contains("has been merged"), "{error}");

        assert_eq!(
            db.query_opt_i64("SELECT COUNT(*) FROM turns WHERE job_id = 'j'", ())
                .await
                .unwrap(),
            Some(1),
            "no follow-up turn is written for a refused launch"
        );
        assert_eq!(
            db.query_opt_text("SELECT status FROM jobs WHERE id = 'j'", ())
                .await
                .unwrap()
                .as_deref(),
            Some("complete"),
            "a refused launch never writes the job running"
        );
    }

    /// cairn/4978: a Claude session whose first launch was rejected at init
    /// after the CLI had already created its conversation. Retrying under the
    /// same id is refused by the CLI forever ("Session ID ... is already in
    /// use"), so the next continuation must rotate to a fresh id and carry the
    /// pending input there.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_unconfirmed_spawned_session_rotates_on_its_next_continuation() {
        let orch = test_orchestrator(migrated_test_db("unconfirmed-start-rotation").await);
        let db = orch.db.local.clone();
        db.execute_script(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','P','p','/tmp/p',1,1);
             INSERT INTO issues(id,project_id,number,title,status,created_at,updated_at) VALUES('i','p',1,'I','active',1,1);
             INSERT INTO jobs(id,project_id,issue_id,status,node_name,uri_segment,current_session_id,model,created_at,updated_at)
               VALUES('j','p','i','running','builder','builder','sess-a','opus',1,1);
             INSERT INTO sessions(id,job_id,backend,backend_id,status,sequence,created_at,updated_at)
               VALUES('sess-a','j','claude',NULL,'open',24,1,1);
             INSERT INTO runs(id,job_id,session_id,status,started_at,exit_reason,error_message,start_mode,created_at,updated_at)
               VALUES('run-1','j','sess-a','crashed',2,'session_start_rejected','missing Cairn tools','fresh',1,3);
             INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,start_reason,created_at,ended_at,updated_at)
               VALUES('turn-1','sess-a','run-1','j',1,'interrupted','initial',1,3,3);
             INSERT INTO events(id,run_id,session_id,turn_id,sequence,timestamp,event_type,data,parent_tool_use_id,created_at)
               VALUES('e-1','run-1','sess-a','turn-1',1,2,'user','{\"content\":\"start the work\"}',NULL,2),
                     ('e-2','run-1','sess-a','turn-1',2,3,'error','{\"message\":\"missing Cairn tools\"}',NULL,3);",
        )
        .await
        .unwrap();

        let continued =
            super::continue_job_impl(&orch, "j", Some("carry this forward"), None, None);

        let successor = db
            .query_opt_text("SELECT current_session_id FROM jobs WHERE id = 'j'", ())
            .await
            .unwrap()
            .expect("the job points at a session");
        assert_ne!(
            successor, "sess-a",
            "the continuation relaunched the id the CLI refuses: {continued:?}"
        );
        assert_eq!(
            db.query_opt_text(
                "SELECT status || ':' || terminal_reason || ':' || replaced_by_id FROM sessions WHERE id = 'sess-a'",
                (),
            )
            .await
            .unwrap()
            .as_deref(),
            Some(format!("closed:rotated_unconfirmed_start:{successor}").as_str())
        );
        assert_eq!(
            db.query_opt_i64(
                "SELECT sequence FROM sessions WHERE id = ?1",
                params![successor.clone()],
            )
            .await
            .unwrap(),
            Some(25),
            "the successor is the next lineage generation"
        );

        let run = continued.expect("the continuation launched on the successor");
        assert_eq!(run.session_id.as_deref(), Some(successor.as_str()));
        let start: serde_json::Value = serde_json::from_str(
            &db.query_opt_text(
                "SELECT session_start_json FROM prepared_ordinary_launches WHERE run_id = ?1",
                params![run.id.clone()],
            )
            .await
            .unwrap()
            .expect("the launch was prepared"),
        )
        .unwrap();
        assert_eq!(
            serde_json::from_value::<crate::storage::PreparedSessionStart>(start).unwrap(),
            crate::storage::PreparedSessionStart::New {
                session_id: successor.clone()
            },
            "the successor launches fresh under its own id"
        );

        let boundary: serde_json::Value = serde_json::from_str(
            &db.query_opt_text(
                "SELECT data FROM events WHERE session_id = ?1 AND event_type = 'system:session_boundary'",
                params![successor.clone()],
            )
            .await
            .unwrap()
            .expect("the rotation left a boundary"),
        )
        .unwrap();
        assert_eq!(boundary["raw"]["kind"], "digest_reseed");
        assert_eq!(boundary["raw"]["reason"], "unconfirmed_first_launch");
        assert!(
            boundary["raw"]["seedBytes"].as_u64().unwrap_or(0) > 0,
            "the successor is seeded from the job's own transcript"
        );

        let carried = db
            .query_opt_i64(
                "SELECT COUNT(*) FROM events WHERE session_id = ?1 AND data LIKE '%carry this forward%'",
                params![successor.clone()],
            )
            .await
            .unwrap();
        assert_eq!(carried, Some(1), "the pending input reached the successor");
    }

    /// An empty digest rendering is not an empty history. Only a successful read
    /// that found no event rows may rotate a session without a seed.
    #[test]
    fn only_a_proven_empty_history_rotates_without_a_seed() {
        assert!(matches!(
            super::empty_digest_refusal(Ok(false)),
            super::ComposeRefusal::NoTranscript
        ));
        assert!(matches!(
            super::empty_digest_refusal(Ok(true)),
            super::ComposeRefusal::Failed(_)
        ));
        assert!(matches!(
            super::empty_digest_refusal(Err("database is locked".to_string())),
            super::ComposeRefusal::Failed(_)
        ));
    }

    /// A launch that spawned and crashed before writing a single event leaves
    /// nothing to carry: the successor launches unseeded under a fresh id, which
    /// is exactly what a same-id retry would have sent.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_unconfirmed_session_with_no_events_rotates_unseeded() {
        let orch = test_orchestrator(migrated_test_db("unconfirmed-start-unseeded").await);
        let db = orch.db.local.clone();
        db.execute_script(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','P','p','/tmp/p',1,1);
             INSERT INTO issues(id,project_id,number,title,status,created_at,updated_at) VALUES('i','p',1,'I','active',1,1);
             INSERT INTO jobs(id,project_id,issue_id,status,node_name,uri_segment,current_session_id,model,created_at,updated_at)
               VALUES('j','p','i','running','builder','builder','sess-a','opus',1,1);
             INSERT INTO sessions(id,job_id,backend,backend_id,status,sequence,created_at,updated_at)
               VALUES('sess-a','j','claude',NULL,'open',3,1,1);
             INSERT INTO runs(id,job_id,session_id,status,started_at,exit_reason,start_mode,created_at,updated_at)
               VALUES('run-1','j','sess-a','crashed',2,'crash','fresh',1,3);
             INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,start_reason,created_at,ended_at,updated_at)
               VALUES('turn-1','sess-a','run-1','j',1,'interrupted','initial',1,3,3);",
        )
        .await
        .unwrap();

        let run = super::continue_job_impl(&orch, "j", Some("carry this forward"), None, None)
            .expect("the continuation launches on a successor");
        let successor = run.session_id.clone().expect("the run names its session");
        assert_ne!(successor, "sess-a");
        assert_eq!(
            db.query_opt_text(
                "SELECT terminal_reason FROM sessions WHERE id = 'sess-a'",
                ()
            )
            .await
            .unwrap()
            .as_deref(),
            Some("rotated_unconfirmed_start")
        );
        let boundary: serde_json::Value = serde_json::from_str(
            &db.query_opt_text(
                "SELECT data FROM events WHERE session_id = ?1 AND event_type = 'system:session_boundary'",
                params![successor.clone()],
            )
            .await
            .unwrap()
            .expect("the rotation left a boundary"),
        )
        .unwrap();
        assert_eq!(boundary["raw"]["reason"], "unconfirmed_first_launch");
        assert_eq!(boundary["raw"]["seedBytes"], 0);
        assert_eq!(
            db.query_opt_i64(
                "SELECT COUNT(*) FROM events WHERE session_id = ?1 AND event_type = 'user:seed'",
                params![successor.clone()],
            )
            .await
            .unwrap(),
            Some(0),
            "nothing was composed, so nothing is seeded"
        );
    }

    /// The cairn/4840 shape: Claude is rejected at `init` on every launch. It goes
    /// live, waits until admission has begun its turn, and then dies the way the
    /// reader's init rejection settles a run — but only when the test releases
    /// it, so input can be queued while the launch is provably alive.
    struct DiesAtInitBackend {
        launches: Arc<Mutex<Vec<(String, std::time::Instant)>>>,
        released_deaths: Arc<(Mutex<usize>, std::sync::Condvar)>,
    }

    impl crate::backends::AgentBackend for DiesAtInitBackend {
        fn name(&self) -> &str {
            "Claude"
        }
        fn is_available(&self) -> Result<(), String> {
            Ok(())
        }
        fn discover_models(
            &self,
        ) -> Result<Vec<cairn_models::types::catalog::DiscoveredModel>, String> {
            Ok(Vec::new())
        }
        fn resolve_tools(&self, _: &[String], _: &[String]) -> crate::backends::ResolvedTools {
            crate::backends::ResolvedTools {
                allowed: Vec::new(),
                disallowed: Vec::new(),
            }
        }
        fn supports_resume(&self) -> bool {
            true
        }
        fn supports_warm_processes(&self) -> bool {
            true
        }
        fn start_session(
            &self,
            config: crate::backends::SessionConfig,
            orch: &Orchestrator,
        ) -> Result<(), String> {
            let session_id = config.session_start.session_id().to_string();
            let launch_index = {
                let mut launches = self.launches.lock().unwrap();
                launches.push((session_id.clone(), std::time::Instant::now()));
                launches.len()
            };
            let run_id = config.run_id.clone();
            orch.process_state
                .register_process(
                    run_id.clone(),
                    RunHandle::test_handle(Some(&session_id), Some("j")),
                )
                .map_err(|error| error.to_string())?;
            let db = orch.db.local.clone();
            let live_run = run_id.clone();
            crate::storage::run_db_blocking(move || async move {
                db.execute(
                    "UPDATE runs SET status = 'live', started_at = 2 WHERE id = ?1",
                    (live_run,),
                )
                .await
                .map(|_| ())
                .map_err(|error| error.to_string())
            })?;

            let orch = orch.clone();
            let released = self.released_deaths.clone();
            std::thread::spawn(move || {
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
                while !matches!(
                    orch.process_state.get_occupancy(&run_id),
                    Some(RunOccupancy::ServingTurn(_))
                ) && std::time::Instant::now() < deadline
                {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
                let (lock, ready) = &*released;
                let mut allowed = lock.lock().unwrap();
                while *allowed < launch_index {
                    allowed = ready.wait(allowed).unwrap();
                }
                drop(allowed);
                crate::orchestrator::lifecycle::release_and_stop_process(&orch, &run_id);
                if crate::orchestrator::lifecycle::record_run_failure_if_active(
                    &orch,
                    &run_id,
                    crate::backends::BackendFailure::StartupRejected.exit_reason(),
                    "init lacked Cairn's tools",
                )
                .unwrap_or(true)
                {
                    crate::orchestrator::lifecycle::finalize_run(
                        &orch,
                        &run_id,
                        crate::models::RunStatus::Crashed,
                    );
                }
            });
            Ok(())
        }
    }

    /// cairn/4978 #2: settling a Claude death within a second must not turn a
    /// provider that dies at init into a hot relaunch loop. `finalize_run`'s
    /// run-terminal idle flush is the one automatic continuation a crash can
    /// trigger, and it resumes only for waking input still pending. A launch
    /// claims the queue it carries, so each relaunch costs one new waking input:
    /// two inputs, two deaths, two launches, and then silence.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_provider_that_dies_at_init_relaunches_only_per_waking_input() {
        let orch = test_orchestrator(migrated_test_db("dies-at-init-spacing").await);
        let db = orch.db.local.clone();
        db.execute_script(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','P','p','/tmp/p',1,1);
             INSERT INTO issues(id,project_id,number,title,status,created_at,updated_at) VALUES('i','p',1,'I','active',1,1);
             INSERT INTO jobs(id,project_id,issue_id,status,node_name,uri_segment,current_session_id,model,created_at,updated_at)
               VALUES('j','p','i','running','builder','builder','sess-a','opus',1,1);
             INSERT INTO sessions(id,job_id,backend,backend_id,status,sequence,created_at,updated_at)
               VALUES('sess-a','j','claude',NULL,'open',1,1,1);
             INSERT INTO runs(id,job_id,session_id,status,started_at,start_mode,created_at,updated_at)
               VALUES('run-0','j','sess-a','exited',2,'fresh',1,3);
             INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,start_reason,created_at,ended_at,updated_at)
               VALUES('turn-0','sess-a','run-0','j',1,'complete','initial',1,3,3);
             INSERT INTO events(id,run_id,session_id,turn_id,sequence,timestamp,event_type,data,parent_tool_use_id,created_at)
               VALUES('e-0','run-0','sess-a','turn-0',1,2,'user','{\"content\":\"start the work\"}',NULL,2);",
        )
        .await
        .unwrap();
        let launches = Arc::new(Mutex::new(Vec::new()));
        let released = Arc::new((Mutex::new(0usize), std::sync::Condvar::new()));
        let _backend =
            crate::orchestrator::session::install_orchestrator_test_session_backend(&orch, {
                let launches = launches.clone();
                let released = released.clone();
                move || {
                    Box::new(DiesAtInitBackend {
                        launches: launches.clone(),
                        released_deaths: released.clone(),
                    })
                }
            });
        let release_death = |n: usize| {
            let (lock, ready) = &*released;
            *lock.lock().unwrap() = n;
            ready.notify_all();
        };
        let launch_count = || launches.lock().unwrap().len();
        async fn wait_for(what: &str, ok: impl Fn() -> bool) {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
            while !ok() {
                assert!(
                    std::time::Instant::now() < deadline,
                    "timed out waiting for {what}"
                );
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        }
        let serving = |orch: &Orchestrator| {
            orch.process_state
                .find_process_by_session(&launches.lock().unwrap().last().unwrap().0)
                .is_some_and(|run| {
                    matches!(
                        orch.process_state.get_occupancy(&run),
                        Some(RunOccupancy::ServingTurn(_))
                    )
                })
        };

        // Input 1 launches under the session's own id.
        let first = orch.clone();
        tokio::task::spawn_blocking(move || {
            super::continue_job_impl(&first, "j", Some("first input"), None, None)
        })
        .await
        .unwrap()
        .expect("launch 1 is admitted");
        wait_for("launch 1", || launch_count() == 1).await;
        wait_for("launch 1 to serve its turn", || serving(&orch)).await;

        // Input 2 arrives while launch 1 is alive, so it waits in the queue.
        super::enqueue_continuation_message(&db, "j", "second input", None).unwrap();
        crate::messages::delivery::nudge_job_for_urgency(
            &orch,
            "j",
            crate::messages::queued::Delivery::Queue,
        )
        .unwrap();
        assert_eq!(
            launch_count(),
            1,
            "a busy job queues instead of relaunching"
        );

        // Death 1 settles at once; its idle flush finds input 2 and relaunches,
        // under a fresh id because sess-a is now an unconfirmed spawned launch.
        let died = std::time::Instant::now();
        release_death(1);
        wait_for("launch 2", || launch_count() == 2).await;
        let relaunched_after = launches.lock().unwrap()[1].1.duration_since(died);
        wait_for("launch 2 to serve its turn", || serving(&orch)).await;

        // Death 2 has no pending input behind it, so nothing relaunches.
        release_death(2);
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        let launches = launches.lock().unwrap().clone();
        assert_eq!(
            launches.len(),
            2,
            "a provider that dies at init relaunched without new waking input: {launches:?}"
        );
        assert_eq!(launches[0].0, "sess-a");
        assert_ne!(
            launches[1].0, "sess-a",
            "the relaunch rotated off the taken id"
        );
        assert!(
            relaunched_after < std::time::Duration::from_secs(20),
            "the queued input was carried by the relaunch: {relaunched_after:?}"
        );
        assert_eq!(
            db.query_opt_i64(
                "SELECT COUNT(*) FROM runs WHERE job_id = 'j' AND status = 'crashed' AND exit_reason = 'session_start_rejected'",
                (),
            )
            .await
            .unwrap(),
            Some(2)
        );
        assert_eq!(
            db.query_opt_i64(
                "SELECT COUNT(*) FROM queued_messages WHERE job_id = 'j' AND delivered_at IS NULL",
                (),
            )
            .await
            .unwrap(),
            Some(0),
            "both inputs were carried by a launch"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn adopted_launch_keeps_precreated_successor_serving() {
        let orch = test_orchestrator(migrated_test_db("adopted-launch-occupancy").await);
        let db = orch.db.local.clone();
        orch.process_state
            .register_process(
                "r".to_string(),
                RunHandle::test_handle(Some("s"), Some("j")),
            )
            .unwrap();
        orch.process_state.transition_to_active("r");
        orch.process_state.begin_turn("r", "successor");

        {
            let mut guard = PendingLaunchTurnGuard {
                orch: &orch,
                db,
                turn_id: Some("successor".to_string()),
                run_id: None,
                inserted_run_id: None,
                wrote_running_job_id: None,
                unwind_preclaimed: None,
            };
            guard.started("r");
            guard.adopted();
        }

        assert_eq!(
            orch.process_state.get_occupancy("r"),
            Some(RunOccupancy::ServingTurn("successor".to_string())),
            "adopting the launch must disarm rollback for the active run"
        );
        assert_eq!(
            orch.process_state.get_current_turn_id("r").as_deref(),
            Some("successor"),
            "the provider's turn-start notification must still see the launch turn"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn exiting_old_run_does_not_fail_rotation_successor() {
        let orch = test_orchestrator(migrated_test_db("rotation-finalize-successor").await);
        let db = orch.db.local.clone();
        db.execute_script(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','P','p','/tmp/p',1,1);
             INSERT INTO jobs(id,project_id,status,current_session_id,created_at,updated_at) VALUES('j','p','running','s1',1,1);
             INSERT INTO sessions(id,job_id,backend,status,sequence,created_at,updated_at) VALUES('s1','j','claude','open',1,1,1),('s2','j','claude','open',2,2,2);
             INSERT INTO runs(id,job_id,project_id,session_id,status,created_at,updated_at) VALUES('r1','j','p','s1','running',1,1);
             INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,start_reason,created_at,started_at,ended_at,updated_at) VALUES('t1','s1','r1','j',1,'yielded','initial',1,1,1,1);
             UPDATE jobs SET current_turn_id='t1' WHERE id='j';",
        ).await.unwrap();
        // The head turn is terminal because that is the only shape a followup can
        // be created from: a non-terminal head is refused by `create_turn_conn`,
        // so a `running` t1 here would abort the test before it reached the
        // rotation it exists to check.
        let successor = super::create_followup_turn(&orch, "s1", "j", true, false).unwrap();
        crate::orchestrator::lifecycle::finalize_run(&orch, "r1", crate::models::RunStatus::Exited);
        assert_eq!(
            db.query_opt_text("SELECT state FROM turns WHERE id=?1", (successor.clone(),))
                .await
                .unwrap()
                .as_deref(),
            Some("pending")
        );
        super::adopt_successor_turn(db.clone(), &successor, "s2").unwrap();
        db.execute("INSERT INTO runs(id,job_id,project_id,session_id,status,created_at,updated_at) VALUES('r2','j','p','s2','starting',2,2)", ()).await.unwrap();
        super::start_turn(&orch, &successor, "r2").unwrap();
        assert_eq!(
            db.query_opt_text(
                "SELECT state || ':' || run_id || ':' || session_id FROM turns WHERE id=?1",
                (successor.clone(),)
            )
            .await
            .unwrap()
            .as_deref(),
            Some("running:r2:s2")
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn clean_exit_still_fails_pending_turn_owned_by_run() {
        let orch = test_orchestrator(migrated_test_db("owned-pending-finalize").await);
        let db = orch.db.local.clone();
        db.execute_script("INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1); INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','P','p','/tmp/p',1,1); INSERT INTO jobs(id,project_id,status,current_session_id,created_at,updated_at) VALUES('j','p','running','s',1,1); INSERT INTO sessions(id,job_id,backend,status,sequence,created_at,updated_at) VALUES('s','j','claude','open',1,1,1); INSERT INTO runs(id,job_id,project_id,session_id,status,created_at,updated_at) VALUES('r','j','p','s','running',1,1); INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,start_reason,created_at,updated_at) VALUES('t','s','r','j',1,'pending','initial',1,1); UPDATE jobs SET current_turn_id='t' WHERE id='j';").await.unwrap();
        crate::orchestrator::lifecycle::finalize_run(&orch, "r", crate::models::RunStatus::Exited);
        assert_eq!(
            db.query_opt_text("SELECT state FROM turns WHERE id='t'", ())
                .await
                .unwrap()
                .as_deref(),
            Some("failed")
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn launch_guard_crashes_inserted_run_when_start_cannot_attach_turn() {
        let orch = test_orchestrator(migrated_test_db("unattached-run-cleanup").await);
        let db = orch.db.local.clone();
        db.execute_script("INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1); INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','P','p','/tmp/p',1,1); INSERT INTO jobs(id,project_id,status,created_at,updated_at) VALUES('j','p','running',1,1); INSERT INTO runs(id,job_id,project_id,status,created_at,updated_at) VALUES('r','j','p','starting',1,1);").await.unwrap();
        orch.process_state
            .register_process("r".to_string(), RunHandle::test_handle(None, Some("j")))
            .unwrap();
        assert_eq!(orch.process_state.has_process("r"), Some(true));
        {
            let mut guard = PendingLaunchTurnGuard {
                orch: &orch,
                db: db.clone(),
                turn_id: None,
                run_id: None,
                inserted_run_id: None,
                wrote_running_job_id: None,
                unwind_preclaimed: None,
            };
            guard.inserted_run("r");
        }
        assert_eq!(orch.process_state.has_process("r"), Some(false));
        assert_eq!(
            db.query_opt_text(
                "SELECT status || ':' || exit_reason || ':' || (exited_at IS NOT NULL) FROM runs WHERE id='r'",
                (),
            )
            .await
            .unwrap()
            .as_deref(),
            Some("crashed:launch_attach_failed:1")
        );
    }

    #[test]
    fn wake_coordinate_parser_distinguishes_nested_tasks() {
        match push_job_coordinate("cairn://p/cairn/42/2/builder/task/review-rust/checks") {
            Some(PushJobCoordinate::Task {
                project,
                number,
                exec_seq,
                node,
                task,
            }) => {
                assert_eq!(
                    (
                        project.as_str(),
                        number,
                        exec_seq,
                        node.as_str(),
                        task.as_str()
                    ),
                    ("cairn", 42, 2, "builder", "review-rust")
                );
            }
            other => panic!("expected task coordinate, got {other:?}"),
        }
        assert!(push_job_coordinate("cairn://p/cairn/42").is_none());
        assert!(push_job_coordinate("not-a-cairn-uri").is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn manual_compaction_carries_channel_cursor_through_rotation() {
        let orch = test_orchestrator(migrated_test_db("manual-compaction-channel-cursor").await);
        let db = orch.db.local.clone();
        db.execute_script(
            "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES('p','w','P','proj','/tmp/p',1,1);
             INSERT INTO jobs(id, project_id, status, current_session_id, created_at, updated_at)
               VALUES('j','p','running','s1',1,1);
             INSERT INTO sessions(id, job_id, backend, status, sequence, channel_cursor_rowid, created_at, updated_at)
               VALUES('s1','j','claude','open',1,42,1,1);
             INSERT INTO runs(id, job_id, project_id, session_id, status, created_at, updated_at)
               VALUES('r1','j','p','s1','exited',1,1);
             INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data, created_at)
               VALUES('e1','r1','s1',1,1,'assistant','{\"type\":\"assistant\",\"content\":\"prior work\"}',1);",
        )
        .await
        .unwrap();

        let source = crate::sessions::queries::get(&db, "s1").await.unwrap();
        let job = db
            .read(|conn| {
                Box::pin(async move {
                    super::load_job_conn(conn, "j")
                        .await?
                        .ok_or_else(|| crate::storage::DbError::Row("job not found".to_string()))
                })
            })
            .await
            .unwrap();

        let outcome = super::force_session_reseed(
            &orch,
            &db,
            &job,
            &source,
            Some("claude"),
            crate::sessions::queries::RotationReason::Reseed,
            None,
        )
        .expect("the manual digest action composes history and rotates the session");
        let cursor = db
            .query_opt_i64(
                "SELECT channel_cursor_rowid FROM sessions WHERE id = ?1",
                params![outcome.new_session_id.as_str()],
            )
            .await
            .unwrap();
        assert_eq!(cursor, Some(42));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn resume_elapsed_uses_stored_predecessor_turn_end() {
        let db = Arc::new(migrated_test_db("resume-clock-predecessor").await);
        db.execute_script(
            "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES('p','w','P','p','/tmp/p',1,1);
             INSERT INTO jobs(id, project_id, status, current_session_id, created_at, updated_at)
               VALUES('j','p','running','s',1,1);
             INSERT INTO sessions(id, job_id, backend, status, created_at, updated_at)
               VALUES('s','j','codex','open',1,1);
             INSERT INTO turns(id, session_id, job_id, sequence, state, start_reason, created_at, ended_at, updated_at)
               VALUES('previous','s','j',1,'completed','initial',1,1000,1000);
             INSERT INTO turns(id, session_id, job_id, sequence, predecessor_id, state, start_reason, created_at, updated_at)
               VALUES('current','s','j',2,'previous','running','follow_up',1100,1100);",
        )
        .await
        .unwrap();

        assert_eq!(
            previous_turn_end_for_resume(db, "current").unwrap(),
            Some(1000)
        );
    }

    #[test]
    fn resumed_input_stamp_is_outermost_even_for_reseeded_context() {
        let clock = crate::clock::HostClock::fixed("America/Los_Angeles");
        let now = chrono::DateTime::from_timestamp(1_752_381_600, 0).unwrap();
        let reconstructed = apply_reseed_seed(
            "TRIGGER".to_string(),
            Some(&ReseedOutcome {
                new_session_id: "session-new".to_string(),
                seed_content: "SEED".to_string(),
            }),
        );
        let prompt =
            prepend_resume_stamp(reconstructed, &clock, now, Some(now.timestamp() - 192 * 60));
        assert_eq!(
            prompt,
            "[Sat 21:40 PDT — resumed after 3h 12m]\n\nSEED\n\nTRIGGER"
        );
    }

    /// Mixed provenance at the seam where it actually mixes (CAIRN-4608): the
    /// operator's own queued text and a passive child catch-up reach one prompt,
    /// and each keeps its own. The queued block IS the operator speaking to this
    /// node, carried verbatim with no attribution sentence wrapped around it; the
    /// push block carries the attribution the catch-up digest resolved for the
    /// child it is about. Separate sections, in that order, neither inside the
    /// other — which is what keeps a thread from reading a child's `^` as its own
    /// unanswered message.
    #[test]
    fn queued_operator_text_and_a_child_catchup_keep_their_own_provenance() {
        let child_block =
            "Attention update (passive): cairn://p/cairn/4602/1/builder/chat?offset=1\n\n\
             Passive child context — not addressed to you. The operator sent 1 message to \
             cairn://p/cairn/4602/1/builder, a child you watch; you have not caught up on them:\n  \
             • 2026-08-25 17:04 EDT — ^";
        let operator_text = "stop and re-read the issue";

        let prompt = assemble_resume_prompt(
            None,
            Some(operator_text.to_string()),
            "",
            None,
            Some(child_block),
        );

        assert_eq!(prompt, format!("{operator_text}\n\n{child_block}"));
        let child_at = prompt
            .find("Passive child context")
            .expect("the child block is carried");
        assert!(
            !prompt[..child_at].contains("not addressed to you"),
            "the operator's own words carry no child attribution: {prompt}"
        );
    }

    #[test]
    fn passive_note_precedes_immediate_resume_message() {
        // A quiet note "A" queued before a waking message "B" must deliver as
        // "A then B", matching send order — not reversed.
        let prompt = assemble_resume_prompt(None, Some("A".to_string()), "B", None, None);
        assert_eq!(prompt, "A\n\nB");
    }

    #[test]
    fn multiple_queued_notes_keep_order_before_message() {
        let prompt = assemble_resume_prompt(None, Some("A1\n\nA2".to_string()), "B", None, None);
        assert_eq!(prompt, "A1\n\nA2\n\nB");
    }

    #[test]
    fn resume_prompt_without_queued_is_just_the_message() {
        let prompt = assemble_resume_prompt(None, None, "B", None, None);
        assert_eq!(prompt, "B");
    }

    #[test]
    fn queued_only_resume_has_no_placeholder() {
        let prompt = assemble_resume_prompt(None, Some("A".to_string()), "", None, None);
        assert_eq!(prompt, "A");
    }

    #[test]
    fn empty_resume_falls_back_to_the_synthetic_continuation() {
        // Second construction site: every part came back empty, so the assembled
        // prompt is Cairn's own continuation text rather than a bare directive.
        let prompt = assemble_resume_prompt(None, None, "", None, None);
        assert_eq!(prompt, SYNTHETIC_CONTINUATION_PROMPT);
    }

    #[test]
    fn synthetic_continuation_never_speaks_as_the_operator() {
        // The text an agent reads must name itself as Cairn's own resume and deny
        // operator intent outright. The retired placeholder ("Continue where you
        // left off.") read as a terse instruction and was obeyed as one
        // (CAIRN-3175), so its exact shape is pinned out.
        let text = SYNTHETIC_CONTINUATION_PROMPT;
        assert!(
            text.starts_with("Automatic resume:"),
            "the continuation must announce itself before anything else: {text}"
        );
        assert!(
            text.contains("nobody asked you to continue"),
            "the continuation must deny operator intent explicitly: {text}"
        );
        assert_ne!(
            text, "Continue where you left off.",
            "the bare directive form is exactly what agents mistook for an instruction"
        );
    }

    #[test]
    fn resume_without_operator_content_is_synthetic() {
        // First construction site: no message, no queued follow-up, no attention
        // push — Cairn is waking the job on its own.
        let trigger = resolve_resume_trigger(None, false, false);
        assert!(trigger.synthetic);
        assert_eq!(trigger.message, SYNTHETIC_CONTINUATION_PROMPT);
    }

    #[test]
    fn blank_operator_message_is_no_message() {
        // A whitespace-only message carries no operator intent, so it resolves to
        // the synthetic continuation instead of an empty "You" block.
        let trigger = resolve_resume_trigger(Some("   \n"), false, false);
        assert!(trigger.synthetic);
        assert_eq!(trigger.message, SYNTHETIC_CONTINUATION_PROMPT);
    }

    #[test]
    fn operator_message_resume_keeps_its_attribution() {
        let trigger = resolve_resume_trigger(Some("ship the fix"), false, false);
        assert!(!trigger.synthetic);
        assert_eq!(trigger.message, "ship the fix");
    }

    #[test]
    fn queued_or_pushed_content_leaves_the_trigger_empty() {
        // Queued follow-ups and attention pushes are assembled and stored as their
        // own blocks, so the trigger contributes nothing — and is not synthetic
        // either, because real content did reach this resume.
        for (has_queued, has_pushes) in [(true, false), (false, true), (true, true)] {
            let trigger = resolve_resume_trigger(None, has_queued, has_pushes);
            assert!(trigger.message.is_empty());
            assert!(!trigger.synthetic);
        }
    }

    #[test]
    fn side_channel_and_push_follow_the_immediate_message() {
        // Queued notes lead; the immediate message, then side-channel notices and
        // resolved attention pushes, follow — the established position for those
        // blocks.
        let prompt = assemble_resume_prompt(
            None,
            Some("A".to_string()),
            "B",
            Some("side".to_string()),
            Some("push"),
        );
        assert_eq!(prompt, "A\n\nB\n\nside\n\npush");
    }

    #[test]
    fn self_suspend_note_leads_the_resume_prompt() {
        // On a durable self-suspend resume the note frames the whole turn, so it
        // leads even queued notes — the agent reads "this pause was deliberate"
        // before the awaited result that follows.
        let prompt = assemble_resume_prompt(
            Some("NOTE"),
            Some("A".to_string()),
            "B",
            Some("side".to_string()),
            Some("push"),
        );
        assert_eq!(prompt, "NOTE\n\nA\n\nB\n\nside\n\npush");
    }

    #[test]
    fn absent_self_suspend_note_is_omitted() {
        // A normal (non-suspended) resume passes no note and is unchanged.
        let prompt = assemble_resume_prompt(None, None, "B", None, None);
        assert_eq!(prompt, "B");
    }

    /// Seed a standing node holding one open Claude session on a native thread,
    /// with a warm process bound to it so a continuation reuses that process
    /// instead of spawning a provider the test environment has no account for.
    /// Reuse is the ordinary warm path and the one a standing thread takes.
    async fn standing_node(name: &str) -> (Orchestrator, Arc<crate::storage::LocalDb>) {
        let orch = test_orchestrator(migrated_test_db(name).await);
        let db = orch.db.local.clone();
        db.execute_script(
            "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES('p','w','P','p','/tmp/p',1,1);
             INSERT INTO jobs(id, project_id, status, node_name, uri_segment,
                              current_session_id, model, created_at, updated_at)
               VALUES('j','p','blocked','builder','builder','s1','opus',1,1);
             INSERT INTO sessions(id, job_id, backend, backend_id, status, sequence, created_at, updated_at)
               VALUES('s1','j','claude','native-thread-1','open',1,1,1);
             INSERT INTO runs(id, job_id, session_id, status, created_at, updated_at)
               VALUES('r1','j','s1','live',1,1);",
        )
        .await
        .unwrap();
        {
            let mut processes = orch.process_state.processes.lock().unwrap();
            let mut handle = RunHandle::new(
                Arc::new(Mutex::new(None)),
                Arc::new(Mutex::new(Some(
                    cairn_models::session::claude::ClaudeControl::boxed(Box::new(Vec::<u8>::new())),
                ))),
                Some("s1".to_string()),
                Some("j".to_string()),
            );
            handle.transition_to_warm();
            handle.model = Some("opus".to_string());
            handle.backend = Some("claude".to_string());
            processes.register("r1".to_string(), handle);
        }
        (orch, db)
    }

    /// A node with exactly one provider request on record, at `t = 0`. Later
    /// events are added by each test to say what does and does not refresh the
    /// prompt-cache horizon measured from it.
    async fn node_with_a_provider_request(
        name: &str,
    ) -> (Orchestrator, Arc<crate::storage::LocalDb>) {
        let orch = test_orchestrator(migrated_test_db(name).await);
        let db = orch.db.local.clone();
        db.execute_script(
            "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES('p','w','P','p','/tmp/p',1,1);
             INSERT INTO jobs(id, project_id, status, node_name, uri_segment,
                              current_session_id, model, created_at, updated_at)
               VALUES('j','p','blocked','builder','builder','s1','opus',1,1);
             INSERT INTO sessions(id, job_id, backend, backend_id, status, sequence, created_at, updated_at)
               VALUES('s1','j','claude','native-thread-1','open',1,1,1);
             INSERT INTO runs(id, job_id, session_id, status, created_at, updated_at)
               VALUES('r1','j','s1','live',1,1);
             INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                                parent_tool_use_id, created_at, input_tokens, output_tokens)
               VALUES('e-request','r1','s1',1,0,'assistant','{\"content\":\"working\"}',NULL,0,1200,300);",
        )
        .await
        .unwrap();
        (orch, db)
    }

    /// The prefix cache state this node's session is in at `now`, read the way
    /// the continuation path reads it — through the freshness query, not a value
    /// the test made up.
    async fn cache_state_at(db: &Arc<crate::storage::LocalDb>, now: i64) -> PrefixCacheState {
        prefix_cache_state(
            ResumeBackend::from_key("claude"),
            super::load_last_provider_request_for_session(db.clone(), "s1".to_string())
                .await
                .unwrap(),
            now,
        )
    }

    /// Acceptance 3. A busy node is not a node talking to its provider. None of
    /// this reached the API, so none of it may extend the horizon.
    #[tokio::test(flavor = "current_thread")]
    async fn a_tool_result_does_not_refresh_the_cache_horizon() {
        let (_orch, db) = node_with_a_provider_request("cache-horizon-local-activity").await;
        // Fifty-nine minutes in: a tool comes back, a system message lands, an
        // attention notification arrives. Every token column is NULL because
        // nothing here was a sampling request.
        db.execute_script(
            "INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                                parent_tool_use_id, created_at, input_tokens, cache_read_tokens,
                                cache_create_tokens, output_tokens)
               VALUES('e-tool','r1','s1',2,3540,'tool_result','{}',NULL,3540,NULL,NULL,NULL,NULL),
                     ('e-system','r1','s1',3,3540,'system','{}',NULL,3540,NULL,NULL,NULL,NULL),
                     ('e-attn','r1','s1',4,3540,'system:attention','{}',NULL,3540,NULL,NULL,NULL,NULL);",
        )
        .await
        .unwrap();

        assert_eq!(cache_state_at(&db, 3_540).await, PrefixCacheState::Warm);
        assert_eq!(
            cache_state_at(&db, 3_660).await,
            PrefixCacheState::Cold,
            "local activity extended a horizon only the provider can refresh"
        );
    }

    /// Acceptance 4. A request that the provider itself reported usage for — here
    /// a cache read, the cheap outcome the horizon exists to protect — moves the
    /// horizon forward.
    #[tokio::test(flavor = "current_thread")]
    async fn a_provider_request_recording_a_cache_read_refreshes_the_horizon() {
        let (_orch, db) = node_with_a_provider_request("cache-horizon-cache-read").await;
        db.execute_script(
            "INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                                parent_tool_use_id, created_at, input_tokens, cache_read_tokens,
                                cache_create_tokens, output_tokens)
               VALUES('e-read','r1','s1',2,3540,'assistant','{}',NULL,3540,40,182000,NULL,300);",
        )
        .await
        .unwrap();

        assert_eq!(
            cache_state_at(&db, 3_660).await,
            PrefixCacheState::Warm,
            "a request the provider served from cache did not refresh the horizon"
        );
        // And it is the new request the horizon now runs from, not the old one.
        assert_eq!(
            cache_state_at(&db, 3_540 + 3_600).await,
            PrefixCacheState::Cold
        );
    }

    /// A sub-agent's requests run against their own prefix, so they refresh their
    /// own cache and not the parent conversation's.
    #[tokio::test(flavor = "current_thread")]
    async fn a_subagent_request_does_not_refresh_the_parent_prefix() {
        let (_orch, db) = node_with_a_provider_request("cache-horizon-subagent").await;
        db.execute_script(
            "INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                                parent_tool_use_id, created_at, input_tokens, cache_read_tokens,
                                cache_create_tokens, output_tokens)
               VALUES('e-child','r1','s1',2,3540,'assistant','{}','tool-1',3540,900,0,4000,200);",
        )
        .await
        .unwrap();

        assert_eq!(
            cache_state_at(&db, 3_660).await,
            PrefixCacheState::Cold,
            "a sub-agent's own prefix was mistaken for the parent's"
        );
    }

    /// A thread with history worth folding: eight turns, the earlier ones
    /// carrying heavy tool output and naming a child that has since settled.
    ///
    /// `RECENCY_FLOOR_TURNS` keeps the last three verbatim, so turns 0..4 are
    /// foldable — which is what gives the composition a real `source_bytes` to
    /// price a compaction against.
    async fn thread_with_foldable_history(
        name: &str,
    ) -> (Orchestrator, Arc<crate::storage::LocalDb>) {
        let (orch, db) = node_with_a_provider_request(name).await;
        db.execute_script(
            "INSERT INTO threads(id, project_id, name, status, attention, created_at, updated_at)
               VALUES('t','p','general','active','none',1,1);
             UPDATE jobs SET thread_id = 't' WHERE id = 'j';",
        )
        .await
        .unwrap();

        let child_uri = "cairn://p/p/41";
        let body = "tool output a resumed conversation would re-send verbatim\n".repeat(120);
        for turn in 0..8i64 {
            let turn_id = format!("turn-{turn}");
            db.execute(
                "INSERT INTO turns(id, session_id, sequence, state, created_at, updated_at)
                 VALUES(?1, 's1', ?2, 'complete', ?2, ?2)",
                params![turn_id.clone(), turn + 1],
            )
            .await
            .unwrap();
            // The user turn names the child, so the fold reads as that child's
            // chapter rather than undifferentiated interstitial conversation.
            db.execute(
                "INSERT INTO events(id, run_id, session_id, turn_id, sequence, timestamp,
                                    event_type, data, parent_tool_use_id, created_at)
                 VALUES(?1, 'r1', 's1', ?2, ?3, 0, 'user', ?4, NULL, 0)",
                params![
                    format!("e-user-{turn}"),
                    turn_id.clone(),
                    turn * 10 + 100,
                    serde_json::json!({ "content": format!("check on {child_uri}") }).to_string(),
                ],
            )
            .await
            .unwrap();
            db.execute(
                "INSERT INTO events(id, run_id, session_id, turn_id, sequence, timestamp,
                                    event_type, data, parent_tool_use_id, created_at)
                 VALUES(?1, 'r1', 's1', ?2, ?3, 0, 'tool_result', ?4, NULL, 0)",
                params![
                    format!("e-tool-{turn}"),
                    turn_id,
                    turn * 10 + 101,
                    serde_json::json!({ "toolResult": body, "toolName": "run" }).to_string(),
                ],
            )
            .await
            .unwrap();
        }

        // The child settled, which is what made those turns compactable.
        crate::threads::compaction::mark_child_terminal(
            &db,
            "j",
            &crate::threads::compaction::ChildMark {
                child_issue_id: "child-41".to_string(),
                child_issue_uri: child_uri.to_string(),
                child_title: "Settled child".to_string(),
                final_status: "merged".to_string(),
                marked_at: 1,
            },
        )
        .await
        .unwrap();

        (orch, db)
    }

    /// The trigger the most recent compaction generation recorded.
    async fn compaction_trigger(db: &Arc<crate::storage::LocalDb>) -> Option<String> {
        db.query_opt(
            "SELECT trigger FROM thread_compactions WHERE job_id = 'j'
              ORDER BY generation DESC LIMIT 1",
            (),
            |row| crate::storage::RowExt::text(row, 0),
        )
        .await
        .unwrap()
    }

    /// Acceptance 2 on the thread path, end to end.
    ///
    /// This is the branch nothing else reaches: `attempt_thread_compaction`
    /// computes its own `prefix_bytes` from the seed's construction, selects
    /// `CompactionTrigger::Expiry`, persists a generation, and rotates with
    /// `RotationReason::CacheExpiry`. The digest path shares the guard but none
    /// of those effects.
    #[tokio::test(flavor = "current_thread")]
    async fn a_cold_thread_with_foldable_chapters_compacts() {
        use crate::sessions::queries::get as get_session;

        let (orch, db) = thread_with_foldable_history("cold-thread-compacts").await;
        let job = super::load_job(db.clone(), "j".to_string(), "cold thread compacts")
            .await
            .unwrap();
        let session = get_session(&db, "s1").await.unwrap();

        // The window is measured and nowhere near full, so capacity cannot be
        // what fires this — only the cold prefix can.
        let occupancy = super::session_occupancy(&orch, "s1");
        assert!(
            !window_pressure_reached(occupancy, orch.get_settings().thread_compact_threshold),
            "the fixture reached window pressure, so this would prove nothing: {occupancy:?}"
        );

        let now = 2 * 60 * 60;
        let prefix_cache = cache_state_at(&db, now).await;
        assert_eq!(prefix_cache, PrefixCacheState::Cold);

        let ReseedDecision::Rebuilt(outcome, Some(decision)) = super::attempt_session_reseed(
            &orch,
            &db,
            &job,
            &session,
            now,
            Some("claude"),
            prefix_cache,
        ) else {
            panic!("a cold thread with settled history to fold must compact, and say why");
        };

        // The seed replaced the folded turns rather than carrying them.
        assert!(outcome.seed_content.contains(THREAD_SEED_HEADER));
        assert_eq!(decision.decision, "digest_reseed");
        assert_eq!(decision.cache_state, "cold");
        assert!(decision.reseed_skipped.is_none());

        // A generation was persisted, and it names expiry — the trigger whose
        // production writer CAIRN-4618 removed and this restores.
        assert_eq!(compaction_trigger(&db).await.as_deref(), Some("expiry"));

        // The thread moved to a successor session, and the predecessor names the
        // cause separably from a capacity compaction.
        assert_ne!(outcome.new_session_id, "s1");
        let replaced = get_session(&db, "s1").await.unwrap();
        assert_eq!(
            replaced.terminal_reason.as_deref(),
            Some("rotated_cache_expiry")
        );
        assert_eq!(
            db.query_opt(
                "SELECT current_session_id FROM jobs WHERE id = 'j'",
                (),
                |row| crate::storage::RowExt::text(row, 0)
            )
            .await
            .unwrap()
            .as_deref(),
            Some(outcome.new_session_id.as_str())
        );

        // Acceptance 6: the boundary says a rebuild happened, with a real size.
        let raw = SessionBoundary::digest_reseed("claude".into(), outcome.seed_content.len())
            .with_cache(Some(decision))
            .raw();
        assert_eq!(raw["kind"], "digest_reseed");
        assert!(raw["seedBytes"].as_u64().unwrap() > 0);
        assert_eq!(raw["cacheState"], "cold");
        assert!(raw["reseedSkipped"].is_null());
    }

    /// What the node's session looks like after something ran: how many there
    /// are, and the id and native thread of the one the job is on.
    async fn session_state(db: &crate::storage::LocalDb) -> (i64, Option<String>, Option<String>) {
        use crate::storage::RowExt;
        db.query_opt(
            "SELECT (SELECT COUNT(*) FROM sessions WHERE job_id = 'j'),
                    j.current_session_id, s.backend_id
             FROM jobs j LEFT JOIN sessions s ON s.id = j.current_session_id
             WHERE j.id = 'j'",
            (),
            |row| Ok((row.i64(0)?, row.opt_text(1)?, row.opt_text(2)?)),
        )
        .await
        .unwrap()
        .unwrap()
    }

    /// An ordinary reply, driven all the way through the real launch path rather
    /// than through the rotation decision alone, so a rotation anywhere in
    /// `continue_job_impl` would show up here (CAIRN-4618).
    ///
    /// The reply is enqueued and nudged here rather than sent through
    /// `continue_job_or_enqueue`, because that ingress acknowledges at the
    /// enqueue boundary and hands the launch to a background nudge thread. Going
    /// through it would leave this test asserting against a launch that has not
    /// happened yet — every assertion below would pass vacuously. The nudge is
    /// the same one that thread runs, so the path under test is unchanged.
    #[tokio::test(flavor = "current_thread")]
    async fn an_ordinary_continuation_launch_leaves_the_session_alone() {
        let (orch, db) = standing_node("standing-node-reply").await;
        let before = session_state(&db).await;

        crate::messages::queued::enqueue(
            &db,
            "j",
            "carry on",
            crate::messages::queued::Delivery::Queue,
        )
        .unwrap();
        crate::messages::delivery::nudge_job_for_urgency(
            &orch,
            "j",
            crate::messages::queued::Delivery::Queue,
        )
        .unwrap();

        assert_eq!(session_state(&db).await, before);
        assert_eq!(before.1.as_deref(), Some("s1"));
        assert_eq!(before.2.as_deref(), Some("native-thread-1"));
        let launch_turn = orch
            .process_state
            .get_current_turn_id("r1")
            .expect("the resumed process keeps its launch turn");
        assert_eq!(
            orch.process_state.get_occupancy("r1"),
            Some(RunOccupancy::ServingTurn(launch_turn.clone())),
            "continue_job_impl must return with the resumed run active"
        );

        let request = crate::mcp::types::McpCallbackRequest {
            thread_id: None,
            cwd: "/tmp/p".to_string(),
            run_id: Some("r1".to_string()),
            tool: "ask_user".to_string(),
            payload: serde_json::Value::Null,
            tool_use_id: Some("question-call".to_string()),
        };
        let payload = crate::mcp::types::AskUserPayload {
            questions: vec![crate::mcp::types::Question {
                question: "Continue?".to_string(),
                header: None,
                options: vec![],
                multi_select: false,
            }],
        };
        crate::mcp::handlers::planning::ask_questions(
            &orch,
            &request,
            payload,
            true,
            Some("question-call"),
        )
        .await;

        assert_eq!(
            db.query_opt_text(
                "SELECT turn_id FROM prompts WHERE run_id='r1' AND answered_at IS NULL",
                (),
            )
            .await
            .unwrap()
            .as_deref(),
            Some(launch_turn.as_str()),
            "a question from the resumed run must bind to its launch turn"
        );
        let pending = crate::runs::queries::get_pending_prompt_for_job(db.clone(), "j")
            .unwrap()
            .expect("the pending question is discoverable through the job's current turn");
        assert_eq!(
            Some(pending.id.as_str()),
            db.query_opt_text(
                "SELECT id FROM prompts WHERE run_id='r1' AND turn_id=?1 AND answered_at IS NULL",
                (launch_turn.clone(),),
            )
            .await
            .unwrap()
            .as_deref(),
            "the prompt reached through the job's current turn is the one bound to the launch turn"
        );
        assert_eq!(
            pending.run_id, "r1",
            "the question reached this way belongs to the resumed run, because a prompt \
             carries its run rather than its turn"
        );
    }

    /// A passive wake: no user message, the suppressed-event shape the wake path
    /// uses. This drives `continue_job_launch_locked_for_watchdog`, which is not a
    /// test-only door — it is the exact entry point a watchdog successor and the
    /// stranded-jobs sweep launch through. So this is also the end-to-end proof
    /// that a recovery successor lands on the session it was recovering rather
    /// than a replacement, which is what `needs_fresh_session` used to guarantee
    /// it would not (CAIRN-4618).
    #[tokio::test(flavor = "current_thread")]
    async fn a_passive_wake_launch_leaves_the_session_alone() {
        use super::{continue_job_launch_locked_for_watchdog, ResumeContext};

        let (orch, db) = standing_node("standing-node-wake").await;
        let before = session_state(&db).await;
        let launch_lock = orch.job_launch_lock("j");
        let launch_guard = launch_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        continue_job_launch_locked_for_watchdog(
            &orch,
            "j",
            Some(ResumeContext {
                suppress_user_event: true,
                suppress_self_suspend_note: true,
                ..ResumeContext::default()
            }),
            launch_guard,
        )
        .unwrap();

        assert_eq!(session_state(&db).await, before);
        assert_eq!(before.1.as_deref(), Some("s1"));
        assert_eq!(before.2.as_deref(), Some("native-thread-1"));
    }

    /// The standing-thread canary (CAIRN-4618).
    ///
    /// A long-lived node takes turn after turn over days, going quiet between
    /// them. This drives the one function on the continuation path that is
    /// allowed to replace a session, thirty times, across a clock that advances
    /// two hours per cycle and a transcript that grows every cycle — and asserts
    /// that after all of it there is still exactly one session row, still open,
    /// still the job's current one, still carrying the same native `backend_id`.
    ///
    /// Each cycle records the wake's own provider request and then goes quiet for
    /// half an hour, which is what an ordinary standing thread does: it keeps
    /// talking to the provider, so its prefix never leaves the cache-readable
    /// window and there is nothing to price. Under the old staleness rule every
    /// one of those cycles was a rotation anyway — that is how the reference
    /// install's outlier reached 78 sessions on one job. The number to watch here
    /// is not that the assertions pass but that `sessions` stays at 1.
    ///
    /// Its sibling, `a_cold_prefix_is_priced_before_it_replaces_a_conversation`,
    /// covers the other half: what happens once the thread does go quiet past the
    /// horizon.
    ///
    /// The rotation that IS supposed to happen is the last step, written out
    /// explicitly: once the provider reports the window nearly full, the session
    /// is rebuilt. Expected rotations are test steps; there is no tolerated
    /// background churn for an idle session to hide in.
    #[tokio::test(flavor = "current_thread")]
    async fn a_standing_session_takes_thirty_continuations_without_rotating() {
        use crate::sessions::queries::get as get_session;
        use crate::storage::RowExt;

        let orch = test_orchestrator(migrated_test_db("standing-thread-canary").await);
        let db = orch.db.local.clone();
        db.execute_script(
            "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES('p','w','P','p','/tmp/p',1,1);
             INSERT INTO jobs(id, project_id, status, node_name, uri_segment,
                              current_session_id, model, created_at, updated_at)
               VALUES('j','p','blocked','builder','builder','s1','opus',1,1);
             INSERT INTO sessions(id, job_id, backend, backend_id, status, sequence, created_at, updated_at)
               VALUES('s1','j','claude','native-thread-1','open',1,1,1);
             INSERT INTO runs(id, job_id, session_id, status, created_at, updated_at)
               VALUES('r1','j','s1','live',1,1);",
        )
        .await
        .unwrap();

        // A warm process bound to this session, so each continuation reuses it
        // instead of spawning a provider the test environment has no account for.
        // Reuse is the ordinary warm path, and it is the path a standing thread
        // actually takes between wakes.
        {
            let mut processes = orch.process_state.processes.lock().unwrap();
            let mut handle = RunHandle::new(
                Arc::new(Mutex::new(None)),
                Arc::new(Mutex::new(Some(
                    cairn_models::session::claude::ClaudeControl::boxed(Box::new(Vec::<u8>::new())),
                ))),
                Some("s1".to_string()),
                Some("j".to_string()),
            );
            handle.transition_to_warm();
            handle.model = Some("opus".to_string());
            handle.backend = Some("claude".to_string());
            processes.register("r1".to_string(), handle);
        }

        let original = {
            let session = get_session(&db, "s1").await.unwrap();
            (session.id.clone(), session.backend_id.clone())
        };
        assert_eq!(original.1.as_deref(), Some("native-thread-1"));

        // A reading the provider actually took, comfortably below the threshold.
        // Without one, "no rebuild" would be the absence of evidence rather than a
        // decision, and the canary would pass on a session nobody could measure.
        let occupancy = |used: i64| crate::models::ContextTokenState {
            run_id: "r1".to_string(),
            session_id: Some("s1".to_string()),
            backend: "claude".to_string(),
            model: Some("opus".to_string()),
            used_tokens: used,
            context_window: Some(200_000),
            auto_compact_limit: None,
            reasoning_tokens: None,
            last_output_tokens: None,
            captured_at: 1,
        };
        orch.store_context_token_snapshot(occupancy(20_000));

        let job = super::load_job(db.clone(), "j".to_string(), "standing-thread canary")
            .await
            .unwrap();

        let identity = |db: Arc<crate::storage::LocalDb>| async move {
            let session = get_session(&db, "s1").await.unwrap();
            (session.id.clone(), session.backend_id.clone())
        };

        let mut now = 1_000_000_i64;
        for cycle in 0..30 {
            // The wake's own provider request, carrying the usage the provider
            // reported for it. Token columns are what mark it as provider
            // traffic, and provider traffic is the only thing that refreshes a
            // prompt cache.
            db.execute(
                "INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data, created_at, input_tokens, output_tokens)
                 VALUES(?1, 'r1', 's1', ?2, ?3, 'assistant', '{}', ?3, 1200, 300)",
                params![format!("e{cycle}"), cycle as i64, now],
            )
            .await
            .unwrap();

            let session = get_session(&db, "s1").await.unwrap();
            let prefix_cache = prefix_cache_state(
                ResumeBackend::from_key(&session.backend),
                super::load_last_provider_request_for_session(db.clone(), "s1".to_string())
                    .await
                    .unwrap(),
                now,
            );
            assert_eq!(
                prefix_cache,
                PrefixCacheState::Warm,
                "cycle {cycle} let a thread that just talked to the provider read as cold"
            );
            // How this session continues.
            assert_eq!(
                decide_continue_action(
                    &session.backend,
                    session.backend_id.as_deref(),
                    Some("claude"),
                    prefix_cache,
                    false
                ),
                ContinueAction::MaybeReseed,
                "cycle {cycle} changed how this session continues"
            );
            // And the only step on that path that can replace it.
            assert!(
                matches!(
                    super::attempt_session_reseed(
                        &orch,
                        &db,
                        &job,
                        &session,
                        now,
                        Some("claude"),
                        prefix_cache
                    ),
                    ReseedDecision::NativeResume(None)
                ),
                "cycle {cycle} rebuilt a session whose window was never measured as full"
            );

            assert_eq!(
                identity(db.clone()).await,
                original,
                "cycle {cycle} moved the session or its native thread"
            );

            // The node goes quiet for half an hour between turns — well inside
            // the hour Claude keeps the prefix cache-readable.
            now += 30 * 60;
        }

        let sessions = db
            .query_opt(
                "SELECT COUNT(*) FROM sessions WHERE job_id = 'j'",
                (),
                |row| row.i64(0),
            )
            .await
            .unwrap();
        assert_eq!(
            sessions,
            Some(1),
            "thirty continuations over seventy simulated hours minted a replacement conversation"
        );
        assert_eq!(
            db.query_opt(
                "SELECT current_session_id FROM jobs WHERE id = 'j'",
                (),
                |row| row.text(0)
            )
            .await
            .unwrap()
            .as_deref(),
            Some("s1")
        );

        // The expected rotation, as an explicit step. Nothing about the session
        // changed except the one thing that is allowed to move it: the provider
        // now reports the window nearly full.
        orch.store_context_token_snapshot(occupancy(190_000));
        let session = get_session(&db, "s1").await.unwrap();
        let ReseedDecision::Rebuilt(rebuilt, decision) = super::attempt_session_reseed(
            &orch,
            &db,
            &job,
            &session,
            now,
            Some("claude"),
            PrefixCacheState::Warm,
        ) else {
            panic!("a session at 95% of its window must give something up");
        };
        assert_ne!(rebuilt.new_session_id, "s1");
        // Window pressure chose this, so no cache reading is recorded on it. The
        // boundary a capacity rebuild emits is byte-identical to what it has
        // always been — a `cacheState` here would attribute the rebuild to an
        // economic decision that never happened.
        assert!(
            decision.is_none(),
            "a window-pressure rebuild recorded a cache decision: {decision:?}"
        );
        let raw = SessionBoundary::digest_reseed("claude".into(), rebuilt.seed_content.len())
            .with_cache(decision)
            .raw();
        assert_eq!(raw["kind"], "digest_reseed");
        for economics in [
            "decision",
            "cacheState",
            "reseedSkipped",
            "prefixBytes",
            "candidateSeedBytes",
            "savingsThreshold",
        ] {
            assert!(
                raw.get(economics).is_none(),
                "capacity boundary grew a `{economics}` field"
            );
        }
        assert_eq!(
            db.query_opt(
                "SELECT current_session_id FROM jobs WHERE id = 'j'",
                (),
                |row| row.text(0)
            )
            .await
            .unwrap()
            .as_deref(),
            Some(rebuilt.new_session_id.as_str()),
            "the measured rebuild did not become the job's session"
        );
    }

    /// A cold prefix on a session with almost nothing behind it. The rebuild
    /// would send MORE than the conversation it replaces — the framing header
    /// alone outweighs the transcript — so the conversation is kept, and the
    /// refusal is recorded with the numbers that produced it.
    ///
    /// This is the case CAIRN-4618 was right about, reached now by measurement
    /// rather than by refusing to look.
    #[tokio::test(flavor = "current_thread")]
    async fn a_cold_prefix_is_priced_before_it_replaces_a_conversation() {
        use crate::sessions::queries::get as get_session;
        use crate::storage::RowExt;

        let (orch, db) = node_with_a_provider_request("cold-prefix-priced").await;
        let job = super::load_job(db.clone(), "j".to_string(), "cold prefix")
            .await
            .unwrap();
        let session = get_session(&db, "s1").await.unwrap();

        // Two hours on, with no provider request since the first.
        let now = 2 * 60 * 60;
        let prefix_cache = cache_state_at(&db, now).await;
        assert_eq!(prefix_cache, PrefixCacheState::Cold);
        // A cold Claude session is sent to be priced rather than resumed blind.
        assert_eq!(
            decide_continue_action(
                &session.backend,
                session.backend_id.as_deref(),
                Some("claude"),
                prefix_cache,
                false
            ),
            ContinueAction::MaybeReseed
        );

        let ReseedDecision::NativeResume(Some(decision)) = super::attempt_session_reseed(
            &orch,
            &db,
            &job,
            &session,
            now,
            Some("claude"),
            prefix_cache,
        ) else {
            panic!("a rebuild that saves nothing must keep the conversation and say why");
        };

        assert_eq!(decision.decision, "native_resume");
        assert_eq!(decision.cache_state, "cold");
        assert_eq!(decision.reseed_skipped, Some("insufficient_savings"));
        assert_eq!(decision.threshold, Some(COLD_RESEED_MIN_SAVINGS_RATIO));
        assert!(
            decision.candidate_seed_bytes > decision.prefix_bytes,
            "the refusal must carry the measurement it refused on: {decision:?}"
        );

        // Nothing moved. Same session, same native handle, one row.
        let after = get_session(&db, "s1").await.unwrap();
        assert_eq!(after.backend_id.as_deref(), Some("native-thread-1"));
        assert_eq!(
            db.query_opt(
                "SELECT COUNT(*) FROM sessions WHERE job_id = 'j'",
                (),
                |row| row.i64(0)
            )
            .await
            .unwrap(),
            Some(1)
        );
        assert_eq!(
            db.query_opt(
                "SELECT current_session_id FROM jobs WHERE id = 'j'",
                (),
                |row| row.text(0)
            )
            .await
            .unwrap()
            .as_deref(),
            Some("s1")
        );

        // Acceptance 6: the boundary the caller builds from this decision says
        // native resume, and carries the refusal as detail rather than as a kind.
        let raw = SessionBoundary::native_resume(Some("opus"), Some("claude"))
            .with_cache(Some(decision))
            .raw();
        assert_eq!(raw["kind"], "native_resume_new_process");
        assert!(raw["seedBytes"].is_null());
        assert_eq!(raw["decision"], "native_resume");
        assert_eq!(raw["cacheState"], "cold");
        assert_eq!(raw["reseedSkipped"], "insufficient_savings");
        assert_eq!(raw["savingsThreshold"], 0.5);
    }

    /// A thread whose window is running out compacts for capacity, and records
    /// no cache reading for it — the same rule the ordinary node follows, on the
    /// path that has its own trigger and rotation reason.
    #[tokio::test(flavor = "current_thread")]
    async fn a_thread_compacting_for_capacity_records_no_cache_decision() {
        use crate::sessions::queries::get as get_session;

        let (orch, db) = thread_with_foldable_history("thread-capacity-no-cache-decision").await;
        orch.store_context_token_snapshot(crate::models::ContextTokenState {
            run_id: "r1".to_string(),
            session_id: Some("s1".to_string()),
            backend: "claude".to_string(),
            model: Some("opus".to_string()),
            used_tokens: 190_000,
            context_window: Some(200_000),
            auto_compact_limit: None,
            reasoning_tokens: None,
            last_output_tokens: None,
            captured_at: 1,
        });

        let job = super::load_job(db.clone(), "j".to_string(), "thread capacity")
            .await
            .unwrap();
        let session = get_session(&db, "s1").await.unwrap();

        // Warm prefix, full window: capacity is the only thing choosing this.
        let ReseedDecision::Rebuilt(outcome, decision) = super::attempt_session_reseed(
            &orch,
            &db,
            &job,
            &session,
            0,
            Some("claude"),
            PrefixCacheState::Warm,
        ) else {
            panic!("a thread at 95% of its window must give something up");
        };
        assert_ne!(outcome.new_session_id, "s1");
        assert!(
            decision.is_none(),
            "a capacity compaction recorded a cache decision: {decision:?}"
        );

        // And it is still a capacity compaction end to end, not an expiry one.
        assert_eq!(
            compaction_trigger(&db).await.as_deref(),
            Some("capacity"),
            "the generation named the wrong trigger"
        );
        assert_eq!(
            get_session(&db, "s1")
                .await
                .unwrap()
                .terminal_reason
                .as_deref(),
            Some("rotated_compaction")
        );
    }

    /// A thread pays for a rebuild in its own reasoning, and a thread with
    /// nothing settled to fold has nothing to show for it.
    ///
    /// This is refused on continuity rather than on price, and the distinction
    /// matters: on a tool-heavy transcript the seed's digest projection can be
    /// under half the verbatim prefix even when the composition folds *nothing*,
    /// so the ratio test alone would have rotated a thread that reclaimed no
    /// history at all. Hence a separate reason, and a separate assertion.
    #[tokio::test(flavor = "current_thread")]
    async fn a_cold_thread_with_nothing_foldable_keeps_its_conversation() {
        use crate::sessions::queries::get as get_session;

        let (orch, db) = node_with_a_provider_request("cold-thread-nothing-foldable").await;
        // Make this session a thread's, which is what routes it to compaction
        // rather than the whole-transcript digest.
        db.execute_script(
            "INSERT INTO threads(id, project_id, name, status, attention, created_at, updated_at)
               VALUES('t','p','general','active','none',1,1);
             UPDATE jobs SET thread_id = 't' WHERE id = 'j';",
        )
        .await
        .unwrap();
        assert!(
            super::thread_compaction_capability(&db, "j").is_enabled(),
            "the fixture no longer models a thread, so this asserts nothing"
        );

        let job = super::load_job(db.clone(), "j".to_string(), "cold thread")
            .await
            .unwrap();
        let session = get_session(&db, "s1").await.unwrap();
        let now = 2 * 60 * 60;
        let prefix_cache = cache_state_at(&db, now).await;
        assert_eq!(prefix_cache, PrefixCacheState::Cold);

        let ReseedDecision::NativeResume(Some(decision)) = super::attempt_session_reseed(
            &orch,
            &db,
            &job,
            &session,
            now,
            Some("claude"),
            prefix_cache,
        ) else {
            panic!("a thread with nothing settled to fold must keep its conversation and say why");
        };

        assert_eq!(decision.decision, "native_resume");
        assert_eq!(decision.cache_state, "cold");
        assert_eq!(decision.reseed_skipped, Some("nothing_to_fold"));
        assert!(
            decision.prefix_bytes.is_none() && decision.candidate_seed_bytes.is_none(),
            "a continuity refusal carried a price it was not decided on: {decision:?}"
        );

        // The thread stayed where it was, and no compaction generation was
        // written for an attempt that never rotated.
        assert!(compaction_trigger(&db).await.is_none());
        assert_eq!(
            get_session(&db, "s1").await.unwrap().status,
            crate::models::SessionStatus::Open
        );
        assert_eq!(
            db.query_opt(
                "SELECT current_session_id FROM jobs WHERE id = 'j'",
                (),
                |row| crate::storage::RowExt::text(row, 0)
            )
            .await
            .unwrap()
            .as_deref(),
            Some("s1")
        );
    }

    /// Acceptance 2, and the other side of the guard. A transcript whose bulk is
    /// tool output is exactly where the bounded digest wins: native resume makes
    /// the provider recreate every result body, while the digest carries one row
    /// per call.
    #[tokio::test(flavor = "current_thread")]
    async fn a_cold_prefix_rebuilds_when_the_seed_pays_for_itself() {
        use crate::sessions::queries::get as get_session;

        let (orch, db) = node_with_a_provider_request("cold-prefix-rebuilds").await;
        let body = "tool output line that a resumed conversation would re-send\n".repeat(400);
        let data = serde_json::json!({ "toolResult": body, "toolName": "run" }).to_string();
        for index in 0..8i64 {
            db.execute(
                "INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                                    parent_tool_use_id, created_at)
                 VALUES(?1, 'r1', 's1', ?2, 0, 'tool_result', ?3, NULL, 0)",
                params![format!("e-tool-{index}"), index + 10, data.clone()],
            )
            .await
            .unwrap();
        }

        // The measurement the decision turns on, taken here so this test fails
        // loudly if the digest ever stops eliding what it elides today.
        let prefix_bytes = crate::resources::reseed_verbatim_baseline_bytes(&db, "j")
            .await
            .expect("a job with a transcript has a verbatim baseline");
        assert!(
            prefix_bytes > 100_000,
            "the fixture no longer models a transcript worth pricing: {prefix_bytes}"
        );

        let job = super::load_job(db.clone(), "j".to_string(), "cold prefix rebuild")
            .await
            .unwrap();
        let session = get_session(&db, "s1").await.unwrap();
        let now = 2 * 60 * 60;
        let prefix_cache = cache_state_at(&db, now).await;
        assert_eq!(prefix_cache, PrefixCacheState::Cold);

        let ReseedDecision::Rebuilt(outcome, Some(decision)) = super::attempt_session_reseed(
            &orch,
            &db,
            &job,
            &session,
            now,
            Some("claude"),
            prefix_cache,
        ) else {
            panic!("a cold rebuild that more than halves the request must happen, and say so");
        };

        assert_ne!(outcome.new_session_id, "s1");
        assert!(!outcome.seed_content.is_empty());
        assert!(
            (outcome.seed_content.len() as i64) < prefix_bytes / 2,
            "the seed did not halve what native resume would send: {} vs {prefix_bytes}",
            outcome.seed_content.len()
        );
        assert_eq!(decision.decision, "digest_reseed");
        assert_eq!(decision.cache_state, "cold");
        assert!(decision.reseed_skipped.is_none());

        // The predecessor names the cause that ended it, separably from an
        // ordinary window-pressure reseed.
        let replaced = get_session(&db, "s1").await.unwrap();
        assert_eq!(
            replaced.terminal_reason.as_deref(),
            Some("rotated_cache_expiry")
        );

        // Acceptance 6: the boundary says what actually happened, with a seed size.
        let raw = SessionBoundary::digest_reseed("claude".into(), outcome.seed_content.len())
            .with_cache(Some(decision))
            .raw();
        assert_eq!(raw["kind"], "digest_reseed");
        assert!(raw["seedBytes"].as_u64().unwrap() > 0);
        assert_eq!(raw["cacheState"], "cold");
        assert!(raw["reseedSkipped"].is_null());
    }

    /// A backend Cairn has no published horizon for never expires, however long
    /// it waits — so it keeps exactly today's behaviour.
    #[tokio::test(flavor = "current_thread")]
    async fn an_unknown_backend_never_expires_on_a_borrowed_ttl() {
        let (_orch, db) = node_with_a_provider_request("cache-horizon-unknown-backend").await;
        let last = super::load_last_provider_request_for_session(db.clone(), "s1".to_string())
            .await
            .unwrap();
        let ten_days = 10 * 24 * 60 * 60;
        assert_eq!(
            prefix_cache_state(ResumeBackend::from_key("openrouter"), last, ten_days),
            PrefixCacheState::Unknown
        );
        assert_eq!(
            decide_continue_action(
                "openrouter",
                Some("sess"),
                Some("openrouter"),
                prefix_cache_state(ResumeBackend::from_key("openrouter"), last, ten_days),
                false
            ),
            ContinueAction::MaybeReseed,
            "an unpriced backend must reach the same arm it reaches today"
        );
    }

    #[test]
    fn reseed_seed_orders_header_digest_then_trigger() {
        // The seed content is HEADER + digest; applying it to the trigger yields
        // HEADER + digest + trigger, in that order (CAIRN-2534).
        let seed_content = build_reseed_seed_content("DIGEST_BODY");
        assert!(seed_content.starts_with(RESEED_SEED_HEADER));
        assert!(seed_content.contains("DIGEST_BODY"));

        let outcome = ReseedOutcome {
            new_session_id: "sess-new".to_string(),
            seed_content: seed_content.clone(),
        };
        let delivered = apply_reseed_seed("TRIGGER".to_string(), Some(&outcome));
        assert_eq!(delivered, format!("{seed_content}\n\nTRIGGER"));
        let header_at = delivered.find(RESEED_SEED_HEADER).unwrap();
        let digest_at = delivered.find("DIGEST_BODY").unwrap();
        let trigger_at = delivered.find("TRIGGER").unwrap();
        assert!(header_at < digest_at && digest_at < trigger_at);
    }

    #[test]
    fn the_projected_size_covers_the_framing_actually_delivered() {
        use super::{build_reseed_seed_content, build_thread_seed_content, DigestPreview};

        // The header is not decoration: it is part of the seed event the rebuilt
        // session receives, so a projection that measured only the composed body
        // would under-report every seed by the same hidden amount. Measuring the
        // framed string is what makes the number describe what gets delivered.
        let body = "COMPOSED_BODY";
        for framed in [
            build_thread_seed_content(body, None),
            build_reseed_seed_content(body),
        ] {
            let preview = DigestPreview::of(&framed);
            assert_eq!(
                preview.estimated_tokens,
                (framed.chars().count() as i64 + 3) / 4
            );
            assert!(
                preview.estimated_tokens > DigestPreview::of(body).estimated_tokens,
                "the framing header fell outside the measured seed: {framed}"
            );
        }

        // Sub-thousand seeds still round up rather than to zero: a short seed is
        // small, never absent.
        assert_eq!(DigestPreview::of("abc").estimated_tokens, 1);
        assert_eq!(DigestPreview::of("").estimated_tokens, 0);
    }

    #[test]
    fn a_thread_seed_carries_its_own_header_and_stays_a_seed() {
        // The thread seed is framed for a session that never ends, but it is the
        // same header-then-body shape, so it remains an ordinary `user:seed`
        // everywhere downstream.
        let content = super::build_thread_seed_content("## Arc\n\nCOMPOSED_BODY", None);
        assert!(content.starts_with(super::THREAD_SEED_HEADER));
        assert!(content.contains("COMPOSED_BODY"));

        // The bridge line names the gap the rebuilt session cannot feel, so the
        // relative references in the turns below stay readable. A sub-minute gap
        // is omitted, as it is at an ordinary turn boundary.
        let bridged = super::build_thread_seed_content("BODY", Some(3 * 3600 + 12 * 60));
        assert!(
            bridged.contains("Your last turn ended 3h 12m ago"),
            "the rebuild header lost its bridge line: {bridged}"
        );
        assert!(!super::build_thread_seed_content("BODY", Some(59)).contains("last turn ended"));
        assert!(
            !content.starts_with(RESEED_SEED_HEADER),
            "a thread must not be told its session was reconstructed after inactivity"
        );

        let outcome = ReseedOutcome {
            new_session_id: "sess-new".to_string(),
            seed_content: content.clone(),
        };
        assert_eq!(
            apply_reseed_seed("TRIGGER".to_string(), Some(&outcome)),
            format!("{content}\n\nTRIGGER")
        );
    }

    #[test]
    fn apply_reseed_seed_is_identity_without_reseed() {
        // A non-reseed resume delivers the trigger unchanged.
        assert_eq!(apply_reseed_seed("TRIGGER".to_string(), None), "TRIGGER");
    }

    /// Register a warm process with a recorded model and backend. The stdin is an
    /// in-memory writer so a live `send_set_model` (Claude) succeeds in tests.
    fn register_run(
        state: &AgentProcessState,
        run_id: &str,
        model: Option<&str>,
        backend: Option<&str>,
        account_id: Option<&str>,
    ) {
        let mut processes = state.processes.lock().unwrap();
        let child = Arc::new(Mutex::new(None));
        let stdin = Arc::new(Mutex::new(Some(
            cairn_models::session::claude::ClaudeControl::boxed(Box::new(Vec::<u8>::new())),
        )));
        let mut handle = RunHandle::new(child, stdin, Some(format!("sess-{run_id}")), None);
        handle.transition_to_warm();
        handle.model = model.map(|m| m.to_string());
        handle.backend = backend.map(|b| b.to_string());
        handle.account_id = account_id.map(str::to_string);
        processes.register(run_id.to_string(), handle);
    }

    #[test]
    fn matching_provider_account_reuses_warm_process() {
        let state = AgentProcessState::default();
        register_run(&state, "run-account", Some("opus"), None, Some("account-a"));
        assert_eq!(
            ensure_reused_process_account(&state, "run-account", Some("account-a")).unwrap(),
            ReuseDecision::Reuse
        );
    }

    #[test]
    fn changed_provider_account_restarts_only_when_idle() {
        let state = AgentProcessState::default();
        register_run(&state, "run-account", Some("opus"), None, Some("account-a"));
        assert_eq!(
            ensure_reused_process_account(&state, "run-account", Some("account-b")).unwrap(),
            ReuseDecision::Restart
        );
        state.begin_turn("run-account", "turn-1");
        let error =
            ensure_reused_process_account(&state, "run-account", Some("account-b")).unwrap_err();
        assert!(error.contains("busy"));
    }

    #[test]
    fn a_move_back_off_codex_rotates_too() {
        // The other direction of the same change, which used to be invisible.
        // `backend_for_model` answers None for every Claude model, so a caller
        // that passed the inference straight through supplied None here — read as
        // "nothing to compare" — and a Codex session with a native handle then
        // took the Resume arm below, handing a Codex thread id to the Claude CLI
        // the spawn actually starts. Spelling the model's provider is what makes
        // the check symmetric.
        assert_eq!(
            decide_continue_action(
                "codex",
                Some("thread-existing"),
                Some(crate::backends::resolved_backend_for_model("fable")),
                PrefixCacheState::Warm,
                false
            ),
            ContinueAction::RotateToBackend("claude".to_string())
        );
        // ...and the move onto Codex still rotates, from the same resolution.
        assert_eq!(
            decide_continue_action(
                "claude",
                Some("sess"),
                Some(crate::backends::resolved_backend_for_model("gpt-5.6-sol")),
                PrefixCacheState::Warm,
                false
            ),
            ContinueAction::RotateToBackend("codex".to_string())
        );
        // A session already on the provider its model resolves to is left alone.
        assert_eq!(
            decide_continue_action(
                "claude",
                Some("sess"),
                Some(crate::backends::resolved_backend_for_model("opus")),
                PrefixCacheState::Warm,
                false
            ),
            ContinueAction::MaybeReseed
        );
    }

    /// CAIRN-3798, at the boundary that produced it. An ongoing thread carries
    /// its model on `jobs.model`; the Thread agent's own default has since
    /// resolved to a Codex model. Resuming must leave the Claude session exactly
    /// where it is — the persisted model was never edited, and rotating it onto
    /// Codex both destroyed the conversation and handed Codex a model it rejects
    /// (HTTP 400 for `fable` on a ChatGPT account).
    #[test]
    fn an_agent_default_does_not_rotate_an_untouched_thread() {
        let thread_agent_default =
            crate::models::ModelSelection::new("codex", crate::models::Model::new("gpt-5.6-sol"));
        let desired = crate::backends::effective_backend_name(
            Some(&thread_agent_default),
            Some("codex"),
            Some("fable"),
        );
        assert_eq!(desired.as_deref(), Some("claude"));
        assert_eq!(
            decide_continue_action(
                "claude",
                Some("sess"),
                desired.as_deref(),
                PrefixCacheState::Warm,
                false
            ),
            ContinueAction::MaybeReseed,
            "an untouched fable thread must stay on the session it is running"
        );
        // And the spawn resolves the same pair: Claude, carrying `fable`.
        assert_eq!(
            crate::backends::backend_for_name(desired.as_deref()).name(),
            "Claude"
        );
    }

    /// The other half of the same rule: a model the operator actually chose in
    /// the thread's model menu still moves the session onto its provider. What
    /// changed is who may name the provider, not whether a switch takes effect.
    #[test]
    fn an_operator_model_switch_still_rotates() {
        let thread_agent_default =
            crate::models::ModelSelection::new("claude", crate::models::Model::new("fable"));
        let desired = crate::backends::effective_backend_name(
            Some(&thread_agent_default),
            None,
            Some("gpt-5.6-sol"),
        );
        assert_eq!(desired.as_deref(), Some("codex"));
        assert_eq!(
            decide_continue_action(
                "claude",
                Some("sess"),
                desired.as_deref(),
                PrefixCacheState::Warm,
                false
            ),
            ContinueAction::RotateToBackend("codex".to_string())
        );
    }

    #[test]
    fn every_model_resolves_to_a_spelled_provider() {
        use crate::backends::resolved_backend_for_model;
        for claude_model in ["sonnet", "opus", "haiku", "fable"] {
            assert_eq!(
                resolved_backend_for_model(claude_model),
                "claude",
                "{claude_model} runs on Claude, and the resolution must say so"
            );
        }
        assert_eq!(resolved_backend_for_model("gpt-5.6-sol"), "codex");
        assert_eq!(resolved_backend_for_model("codex-mini"), "codex");
        assert_eq!(resolved_backend_for_model("openrouter/auto"), "openrouter");
    }

    /// A prompt edit no longer has an input here to be decided from, and that is
    /// the point: an edited agent prompt reaches the existing conversation on its
    /// next turn because every backend reassembles the whole instruction stack on
    /// every spawn, resume included. The two backend-side halves of that claim
    /// are pinned where the arguments are actually built —
    /// `agent_process::args::a_resumed_session_still_receives_the_assembled_system_prompt`
    /// and `codex::thread_params::a_resumed_thread_carries_current_instructions`.
    #[test]
    fn a_prompt_edit_does_not_replace_a_live_conversation() {
        assert_eq!(
            decide_continue_action(
                "codex",
                Some("thread-x"),
                Some("codex"),
                PrefixCacheState::Warm,
                false
            ),
            ContinueAction::Resume
        );
        assert_eq!(
            decide_continue_action(
                "claude",
                Some("sess-x"),
                Some("claude"),
                PrefixCacheState::Warm,
                false
            ),
            ContinueAction::MaybeReseed
        );
    }

    #[test]
    fn matching_model_reuses_without_set_model() {
        let state = AgentProcessState::default();
        register_run(&state, "run-1", Some("opus"), None, None);
        let decision = ensure_reused_process_model(&state, "run-1", Some("opus")).unwrap();
        assert_eq!(decision, ReuseDecision::Reuse);
        assert_eq!(state.get_model("run-1"), Some("opus".to_string()));
    }

    #[test]
    fn no_desired_model_reuses() {
        let state = AgentProcessState::default();
        register_run(&state, "run-1", Some("opus"), None, None);
        let decision = ensure_reused_process_model(&state, "run-1", None).unwrap();
        assert_eq!(decision, ReuseDecision::Reuse);
    }

    #[test]
    fn changed_model_requests_restart_when_idle() {
        let state = AgentProcessState::default();
        register_run(&state, "run-1", Some("sonnet"), None, None);
        let decision = ensure_reused_process_model(&state, "run-1", Some("opus")).unwrap();
        assert_eq!(decision, ReuseDecision::Restart);
        // No unconfirmed in-place switch: tracked model is left unchanged; the
        // fresh process records the new model at startup.
        assert_eq!(state.get_model("run-1"), Some("sonnet".to_string()));
    }

    #[test]
    fn changed_model_requests_restart_for_any_backend() {
        // The helper no longer cares about backend (rotation handles cross-backend
        // upstream); a model change on a same-backend process always restarts.
        let state = AgentProcessState::default();
        register_run(&state, "run-1", Some("gpt-5.4-mini"), Some("codex"), None);
        let decision = ensure_reused_process_model(&state, "run-1", Some("gpt-5.4")).unwrap();
        assert_eq!(decision, ReuseDecision::Restart);
        assert_eq!(state.get_model("run-1"), Some("gpt-5.4-mini".to_string()));
    }

    #[test]
    fn changed_model_errors_when_busy() {
        let state = AgentProcessState::default();
        register_run(&state, "run-1", Some("opus"), None, None);
        // Mid-turn: a restart would kill active work → error instead.
        state.begin_turn("run-1", "turn-1");
        let err = ensure_reused_process_model(&state, "run-1", Some("sonnet")).unwrap_err();
        assert!(err.contains("busy"), "unexpected error: {err}");
    }

    // ---- The inherit ladder ---------------------------------------------
    //
    // Where a job that inherits its parent's branch starts is the one place a
    // recorded `jobs.base_commit` can still influence where work begins, so
    // every rung is pinned against a real jj store rather than reasoned about.
    // Two properties matter: a commit the store cannot produce is never chosen,
    // and no rung fails. A job minted at an unresolvable coordinate does not
    // fail here — it fails inside materialization, far from the row that
    // caused it.

    use super::inherited_head;
    use crate::jj::tests::{git, git_stdout, init_project, jj_bin};
    use crate::jj::{create_bookmark_at, ensure_project_store, JjEnv};
    use std::path::PathBuf;
    use tempfile::TempDir;

    /// A store holding two real commits on `main`, with the tempdirs that keep
    /// it alive.
    struct InheritFixture {
        _home: TempDir,
        _project: TempDir,
        jj: JjEnv,
        store: PathBuf,
        first: String,
        second: String,
    }

    /// `None` when jj is not resolvable on this machine, matching the skip
    /// convention the rest of the real-store suites use.
    fn inherit_fixture() -> Option<InheritFixture> {
        let bin = jj_bin()?;
        let home = TempDir::new().unwrap();
        let project = TempDir::new().unwrap();
        init_project(project.path());
        let first = git_stdout(project.path(), &["rev-parse", "HEAD"]);
        std::fs::write(project.path().join("second.rs"), "second\n").unwrap();
        git(project.path(), &["add", "-A"]);
        git(project.path(), &["commit", "-q", "-m", "second"]);
        let second = git_stdout(project.path(), &["rev-parse", "HEAD"]);

        let jj = crate::jj::environment(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, project.path()).unwrap();
        Some(InheritFixture {
            _home: home,
            _project: project,
            jj,
            store,
            first,
            second,
        })
    }

    /// A commit id that is well-formed and absent from the store — the shape a
    /// recorded row takes after its commit is abandoned or was never fetched.
    const ABSENT_COMMIT: &str = "0123456789abcdef0123456789abcdef01234567";

    /// Rung one: the store is the authority. A resolvable recorded base does not
    /// get a say while the branch itself still resolves.
    #[test]
    #[serial_test::serial(jj)]
    fn the_live_bookmark_outranks_a_resolvable_recorded_base() {
        let Some(fx) = inherit_fixture() else {
            eprintln!("skipping the_live_bookmark_outranks_a_resolvable_recorded_base: no jj");
            return;
        };
        create_bookmark_at(&fx.jj, &fx.store, "agent/parent", &fx.first).unwrap();

        let head = inherited_head(
            &fx.jj,
            &fx.store,
            "agent/parent",
            Some(&fx.second),
            "main",
            |_| None,
        );

        assert_eq!(
            head, fx.first,
            "the branch's own bookmark is where the branch is; the recorded row is not"
        );
    }

    /// Rung two: the recorded base is used, but only once the branch is gone AND
    /// the store can still produce the commit it names.
    #[test]
    #[serial_test::serial(jj)]
    fn a_resolvable_recorded_base_is_used_once_the_bookmark_is_gone() {
        let Some(fx) = inherit_fixture() else {
            eprintln!(
                "skipping a_resolvable_recorded_base_is_used_once_the_bookmark_is_gone: no jj"
            );
            return;
        };

        let head = inherited_head(
            &fx.jj,
            &fx.store,
            "agent/never-created",
            Some(&fx.first),
            "main",
            |_| None,
        );

        assert_eq!(
            head, fx.first,
            "a recorded commit the store can still produce is a legitimate degraded start"
        );
    }

    /// Rung three, and the whole point of the rung: a recorded commit the store
    /// cannot resolve is refused, not carried forward. This is the case that
    /// used to mint a job at a coordinate materialization would later fail on.
    #[test]
    #[serial_test::serial(jj)]
    fn an_unresolvable_recorded_base_is_refused_for_the_live_base_branch() {
        let Some(fx) = inherit_fixture() else {
            eprintln!("skipping an_unresolvable_recorded_base_is_refused: no jj");
            return;
        };
        let base_tip = fx.second.clone();

        let head = inherited_head(
            &fx.jj,
            &fx.store,
            "agent/never-created",
            Some(ABSENT_COMMIT),
            "main",
            |revision| (revision == "main").then(|| base_tip.clone()),
        );

        assert_ne!(
            head, ABSENT_COMMIT,
            "a commit the store cannot produce must never be handed to a job"
        );
        assert_eq!(
            head, fx.second,
            "the floor is the job's own base branch, resolved live"
        );
    }

    /// The same floor carries a job whose parent recorded nothing at all — the
    /// state every sub-agent task, call, and workflow child is in when its
    /// parent's row was never written.
    #[test]
    #[serial_test::serial(jj)]
    fn an_absent_recorded_base_falls_through_to_the_live_base_branch() {
        let Some(fx) = inherit_fixture() else {
            eprintln!("skipping an_absent_recorded_base_falls_through: no jj");
            return;
        };
        let base_tip = fx.second.clone();

        let head = inherited_head(
            &fx.jj,
            &fx.store,
            "agent/never-created",
            None,
            "main",
            |revision| (revision == "main").then(|| base_tip.clone()),
        );

        assert_eq!(head, fx.second);
    }

    // ---- Mode dispatch ---------------------------------------------------
    //
    // The ladder above answers "where does an inheriting job start?". These
    // answer the question one level up: which jobs ask it at all, and what
    // happens to a job that asks with no lineage to answer from.

    use super::{select_job_coordinate, CoordinateRequest, ParentCoordinate};
    use crate::execution::step_behavior::StepBehavior;

    /// Plain inheritance: degrades through the ladder when the parent's branch
    /// cannot be resolved. The strict grade the delegation edge uses is pinned
    /// beside the node that emits it.
    fn inherits() -> StepBehavior {
        StepBehavior {
            mints_branch: false,
            inherits_branch: true,
            requires_parent_head: false,
        }
    }

    fn child_request(parent_job_id: Option<&str>) -> CoordinateRequest<'_> {
        CoordinateRequest {
            adopt_head: None,
            job_id: "child-job",
            parent_job_id,
            existing_branch: None,
            base_ref: "main",
        }
    }

    /// A branchless mode is not "no opinion": it puts the job on its base
    /// branch. That is right for a node authored that way and wrong for a
    /// delegated task, which is the substance of CAIRN-3309.
    #[test]
    #[serial_test::serial(jj)]
    fn a_branchless_mode_stays_on_the_base_branch() {
        let Some(fx) = inherit_fixture() else {
            eprintln!("skipping a_branchless_mode_stays_on_the_base_branch: no jj");
            return;
        };
        create_bookmark_at(&fx.jj, &fx.store, "agent/parent", &fx.first).unwrap();
        let base_tip = fx.second.clone();

        let (branch, base_commit) = select_job_coordinate(
            &StepBehavior {
                mints_branch: false,
                inherits_branch: false,
                requires_parent_head: false,
            },
            child_request(Some("parent-job")),
            &fx.jj,
            &fx.store,
            |_| unreachable!("a branchless job never reads its parent's coordinate"),
            || unreachable!("a branchless job mints nothing"),
            move |revision| (revision == "main").then(|| base_tip.clone()),
        )
        .unwrap();

        assert_eq!(branch, None, "the job owns no branch");
        assert_eq!(
            base_commit.as_deref(),
            Some(fx.second.as_str()),
            "it starts at its base branch, regardless of where any parent's branch sits"
        );
    }

    /// A branchless parent is a real authored coordinate, not broken lineage.
    /// Its child remains branchless and resolves the parent's inherited base ref
    /// live rather than minting a writable branch.
    #[test]
    #[serial_test::serial(jj)]
    fn a_branchless_parent_preserves_the_base_coordinate() {
        let Some(fx) = inherit_fixture() else {
            eprintln!("skipping a_branchless_parent_preserves_the_base_coordinate: no jj");
            return;
        };
        let base_tip = fx.second.clone();

        let (branch, base_commit) = select_job_coordinate(
            &inherits(),
            child_request(Some("parent-job")),
            &fx.jj,
            &fx.store,
            |_| {
                Ok(ParentCoordinate {
                    branch: None,
                    recorded_base: Some(fx.first.clone()),
                })
            },
            || unreachable!("an inheriting job mints nothing"),
            move |revision| (revision == "main").then(|| base_tip.clone()),
        )
        .expect("a branchless parent delegates from its live base ref");

        assert_eq!(branch, None);
        assert_eq!(base_commit.as_deref(), Some(fx.second.as_str()));
    }

    /// The same refusal one step earlier: a job that inherits but was never
    /// re-parented has no edge to follow at all.
    #[test]
    #[serial_test::serial(jj)]
    fn missing_lineage_refuses_the_spawn() {
        let Some(fx) = inherit_fixture() else {
            eprintln!("skipping missing_lineage_refuses_the_spawn: no jj");
            return;
        };

        let error = select_job_coordinate(
            &inherits(),
            child_request(None),
            &fx.jj,
            &fx.store,
            |_| unreachable!("there is no parent to load"),
            || unreachable!("an inheriting job mints nothing"),
            |_| None,
        )
        .expect_err("an inheriting job with no parent_job_id must not fall through to base");

        assert!(error.contains("parent_job_id"), "{error}");
    }

    // ---- Continuity across executions -----------------------------------
    //
    // The specimen: execution 1 commits real work and then dies for a reason
    // that has nothing to do with that work, and execution 2 is started on the
    // same issue. These drive the actual composition `prepare_job` drives — a
    // migrated database, a real jj store, `resolve_continuation`, and
    // `select_job_coordinate` — because the failure they exist to prevent was
    // never in any one of those pieces. It was in what they did NOT say to each
    // other.

    use super::super::persistence::load_job;
    use super::import_project_store_locked;
    use super::prepare_store_coordinate;
    use super::resolve_continuation;
    use crate::db_records::DbJob;
    use crate::execution::continuity::Continuation;
    use crate::models::Continuity;

    /// A project whose `main` holds two commits and whose
    /// `agent/proj-1-builder-0` holds one more — the shape execution 1's death
    /// left behind — with the store that can see both.
    struct SuccessorFixture {
        _home: TempDir,
        project: TempDir,
        jj: JjEnv,
        store: PathBuf,
        base: String,
        dead_head: String,
    }

    const DEAD_BRANCH: &str = "agent/proj-1-builder-0";
    /// A file that exists ONLY in execution 1's commit, so "did the successor
    /// inherit it" is answered by the tree rather than by a commit id alone.
    const UNIQUE_FILE: &str = "packed_refs.rs";

    fn successor_fixture() -> Option<SuccessorFixture> {
        let bin = jj_bin()?;
        let home = TempDir::new().unwrap();
        let project = TempDir::new().unwrap();
        init_project(project.path());
        std::fs::write(project.path().join("second.rs"), "second\n").unwrap();
        git(project.path(), &["add", "-A"]);
        git(project.path(), &["commit", "-q", "-m", "second"]);
        let base = git_stdout(project.path(), &["rev-parse", "HEAD"]);

        // Execution 1's work: committed, compiling, and reachable.
        git(project.path(), &["checkout", "-q", "-b", DEAD_BRANCH]);
        std::fs::write(
            project.path().join(UNIQUE_FILE),
            "pub fn with_packed_refs_patience() {}\n",
        )
        .unwrap();
        git(project.path(), &["add", "-A"]);
        git(
            project.path(),
            &["commit", "-q", "-m", "shared packed-refs patience"],
        );
        let dead_head = git_stdout(project.path(), &["rev-parse", "HEAD"]);
        git(project.path(), &["checkout", "-q", "main"]);

        let jj = crate::jj::environment(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, project.path()).unwrap();
        assert_eq!(
            crate::jj::bookmark_commit(&jj, &store, DEAD_BRANCH).as_deref(),
            Some(dead_head.as_str()),
            "the fixture's premise is that the dead execution's branch is reachable"
        );
        Some(SuccessorFixture {
            _home: home,
            project,
            jj,
            store,
            base,
            dead_head,
        })
    }

    /// Seed the two executions. Execution 1's job is `failed` with its branch on
    /// the row; its session is deliberately left `open` with a NULL terminal
    /// reason, exactly as cairn/4602's was, so nothing here can quietly come to
    /// depend on a tidy death. Execution 2's job is the successor about to
    /// prepare. Returns its loaded row.
    async fn seed_successor(
        orch: &Orchestrator,
        repo_path: &str,
        continuity: Continuity,
        rejected: bool,
    ) -> DbJob {
        let snapshot = {
            let mut snapshot = crate::models::ExecutionSnapshot::new(
                crate::models::RecipeSnapshot {
                    id: "build".to_string(),
                    name: "Build".to_string(),
                    description: None,
                    trigger: crate::models::RecipeTrigger::Manual,
                    nodes: vec![],
                    edges: vec![],
                },
                Default::default(),
                Default::default(),
                crate::models::TriggerContext {
                    issue_id: Some("issue-1".to_string()),
                    project_id: "proj-1".to_string(),
                    trigger_type: crate::models::TriggerType::Manual,
                    event_payload: None,
                    initiated_via: None,
                },
            );
            snapshot.continuity = continuity;
            snapshot.to_json().unwrap()
        };
        let repo_path = repo_path.to_string();
        orch.db
            .local
            .write(move |conn| {
                let repo_path = repo_path.clone();
                let snapshot = snapshot.clone();
                Box::pin(async move {
                    conn.execute(
                        "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
                         VALUES ('proj-1','default','Proj','proj',?1,'main',1,1)",
                        params![repo_path.as_str()],
                    ).await?;
                    conn.execute(
                        "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
                         VALUES ('issue-1','proj-1',4602,'Retry me','active',1,1)",
                        (),
                    ).await?;
                    for (exec, seq) in [("exec-1", 1), ("exec-2", 2)] {
                        conn.execute(
                            // Prior attempts are `superseded`: an issue has at
                            // most one canonical execution, and these fixtures
                            // model a dead attempt whose successor is being
                            // prepared (CAIRN-4665).
                            "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot, lifecycle)
                             VALUES (?1,'build','issue-1','proj-1',?2,1,?3,?4,'superseded')",
                            params![
                                exec,
                                if seq == 1 { "failed" } else { "running" },
                                seq,
                                snapshot.as_str()
                            ],
                        ).await?;
                    }
                    conn.execute(
                        "INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, node_name, uri_segment, status, branch, created_at, updated_at)
                         VALUES ('job-dead','exec-1','builder-node','issue-1','proj-1','Builder','builder','failed',?1,1,1)",
                        params![DEAD_BRANCH],
                    ).await?;
                    conn.execute(
                        "INSERT INTO sessions (id, job_id, status, terminal_reason, sequence, created_at, updated_at)
                         VALUES ('sess-dead','job-dead','open',NULL,1,1,1)",
                        (),
                    ).await?;
                    conn.execute(
                        "INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, node_name, uri_segment, status, base_branch, created_at, updated_at)
                         VALUES ('job-next','exec-2','builder-node','issue-1','proj-1','Builder','builder','pending','main',1,1)",
                        (),
                    ).await?;
                    if rejected {
                        conn.execute(
                            "INSERT INTO merge_requests (id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at)
                             VALUES ('mr-1','job-dead','proj-1','issue-1','PR',?1,'main','closed',1,1)",
                            params![DEAD_BRANCH],
                        ).await?;
                    }
                    Ok(())
                })
            })
            .await
            .unwrap();

        load_job(
            orch.db.local.clone(),
            "job-next".to_string(),
            "successor job not found",
        )
        .await
        .unwrap()
    }

    fn mints() -> StepBehavior {
        StepBehavior {
            mints_branch: true,
            inherits_branch: false,
            requires_parent_head: false,
        }
    }

    /// The acceptance case. Execution 1 committed a unique change and died
    /// before publishing an artifact; execution 2's branch must open with that
    /// commit in its history rather than at the issue's base.
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn a_successor_inherits_the_dead_executions_committed_head() {
        let Some(fx) = successor_fixture() else {
            eprintln!("skipping a_successor_inherits_the_dead_executions_committed_head: no jj");
            return;
        };
        let repo = fx.project.path().to_string_lossy().to_string();
        let orch = test_orchestrator(migrated_test_db("continuity-adopt.db").await);
        let job = seed_successor(&orch, &repo, Continuity::Auto, false).await;

        let base = fx.base.clone();
        let rev_parse = move |revision: &str| (revision == "main").then(|| base.clone());
        let continuation =
            resolve_continuation(&orch, &fx.jj, &fx.store, &job, "main", rev_parse.clone())
                .unwrap();

        assert_eq!(
            continuation.adopt_head(),
            Some(fx.dead_head.as_str()),
            "the successor carries execution 1's committed head: {continuation:?}"
        );
        assert!(
            continuation.refusal().is_none(),
            "a single head needs no decision"
        );
        let briefing = continuation.briefing().expect("the successor is told");
        assert!(briefing.contains(&fx.dead_head[..12]), "{briefing}");

        let (branch, base_commit) = select_job_coordinate(
            &mints(),
            CoordinateRequest {
                job_id: "job-next",
                parent_job_id: None,
                existing_branch: None,
                base_ref: "main",
                adopt_head: continuation.adopt_head(),
            },
            &fx.jj,
            &fx.store,
            |_| unreachable!("a minting job reads no parent"),
            || Ok("agent/proj-4602-builder-1".to_string()),
            rev_parse,
        )
        .unwrap();

        let minted = branch.expect("the successor still mints its own branch");
        assert_ne!(
            minted, DEAD_BRANCH,
            "the dead execution's branch is left as evidence"
        );
        let head = crate::jj::bookmark_commit(&fx.jj, &fx.store, &minted).unwrap();
        assert_eq!(
            head, fx.dead_head,
            "the successor's initial HEAD is execution 1's committed head"
        );
        assert!(
            crate::jj::revset_descends_from(&fx.jj, &fx.store, &head, &fx.base),
            "and it still descends from the issue's base"
        );
        assert_eq!(
            base_commit.as_deref(),
            Some(fx.base.as_str()),
            "the recorded base stays the BASE: inherited commits are part of the change this \
             job ships, so its diff and PR must still measure against main"
        );
    }

    /// The intentional start-over. `clean` mints at the base, and the successor
    /// is told the prior work exists and was deliberately not carried — so
    /// "start clean" never means "start uninformed".
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn a_clean_successor_starts_at_base_and_is_told_what_it_left() {
        let Some(fx) = successor_fixture() else {
            eprintln!("skipping a_clean_successor_starts_at_base: no jj");
            return;
        };
        let repo = fx.project.path().to_string_lossy().to_string();
        let orch = test_orchestrator(migrated_test_db("continuity-clean.db").await);
        let job = seed_successor(&orch, &repo, Continuity::Clean, false).await;

        let base = fx.base.clone();
        let rev_parse = move |revision: &str| (revision == "main").then(|| base.clone());
        let continuation =
            resolve_continuation(&orch, &fx.jj, &fx.store, &job, "main", rev_parse.clone())
                .unwrap();

        assert!(matches!(continuation, Continuation::Declined { .. }));
        assert_eq!(continuation.adopt_head(), None);
        let briefing = continuation
            .briefing()
            .expect("declining is still announced");
        assert!(briefing.contains(DEAD_BRANCH), "{briefing}");
        assert!(briefing.contains("NOT inherited"), "{briefing}");

        let (branch, _) = select_job_coordinate(
            &mints(),
            CoordinateRequest {
                job_id: "job-next",
                parent_job_id: None,
                existing_branch: None,
                base_ref: "main",
                adopt_head: continuation.adopt_head(),
            },
            &fx.jj,
            &fx.store,
            |_| unreachable!(),
            || Ok("agent/proj-4602-builder-1".to_string()),
            rev_parse,
        )
        .unwrap();

        let head = crate::jj::bookmark_commit(&fx.jj, &fx.store, &branch.unwrap()).unwrap();
        assert_eq!(head, fx.base, "a clean successor starts at the base branch");
        assert!(
            !crate::jj::revset_descends_from(&fx.jj, &fx.store, &head, &fx.dead_head),
            "and does not contain the superseded work"
        );
    }

    /// A coordinate that names nothing refuses even when NOTHING was
    /// enumerated — the case an "is the candidate list empty?" fast path used to
    /// swallow before the mode was ever read, minting ordinary work at the base
    /// while the caller's exact adoption request went unanswered. A refusal here
    /// is what keeps the two possible outcomes to "the head you named" and "why
    /// not".
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn an_explicit_coordinate_refuses_when_nothing_was_enumerated() {
        let Some(fx) = successor_fixture() else {
            eprintln!("skipping an_explicit_coordinate_refuses_when_nothing_was_enumerated: no jj");
            return;
        };
        let repo = fx.project.path().to_string_lossy().to_string();
        let orch = test_orchestrator(migrated_test_db("continuity-empty-from.db").await);
        let job = seed_successor(
            &orch,
            &repo,
            Continuity::From("agent/never-existed".to_string()),
            false,
        )
        .await;
        // Execution 1 minted nothing, so there is no candidate to match. Its
        // session references the job, so that goes first.
        orch.db
            .local
            .execute_script(
                "DELETE FROM sessions WHERE job_id = 'job-dead';
                 DELETE FROM jobs WHERE id = 'job-dead';",
            )
            .await
            .unwrap();

        let base = fx.base.clone();
        let continuation = resolve_continuation(&orch, &fx.jj, &fx.store, &job, "main", move |r| {
            (r == "main").then(|| base.clone())
        })
        .unwrap();

        assert_eq!(continuation.adopt_head(), None);
        let refusal = continuation
            .refusal()
            .expect("an unanswerable coordinate must not mint at the base in silence");
        assert!(refusal.contains("agent/never-existed"), "{refusal}");
    }

    /// Work a reviewer closed is superseded, not retried. The successor starts
    /// at the base without anyone having to say so, and is still told the branch
    /// exists and why it was skipped.
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn rejected_work_is_not_reinstated_by_a_retry() {
        let Some(fx) = successor_fixture() else {
            eprintln!("skipping rejected_work_is_not_reinstated_by_a_retry: no jj");
            return;
        };
        let repo = fx.project.path().to_string_lossy().to_string();
        let orch = test_orchestrator(migrated_test_db("continuity-rejected.db").await);
        let job = seed_successor(&orch, &repo, Continuity::Auto, true).await;

        let base = fx.base.clone();
        let continuation = resolve_continuation(&orch, &fx.jj, &fx.store, &job, "main", move |r| {
            (r == "main").then(|| base.clone())
        })
        .unwrap();

        assert_eq!(continuation.adopt_head(), None);
        let briefing = continuation
            .briefing()
            .expect("a rejected branch is still named");
        assert!(briefing.contains("explicitly rejected"), "{briefing}");
    }

    /// The ladder has no failing rung. With the branch gone, nothing recorded,
    /// an unresolvable base ref, and a git that answers nothing, it still yields
    /// a coordinate — jj's always-present root commit. Refusing to start over
    /// substrate state is what the operator ruling forbids.
    #[test]
    #[serial_test::serial(jj)]
    fn the_ladder_yields_a_coordinate_even_when_every_rung_is_unresolvable() {
        let Some(fx) = inherit_fixture() else {
            eprintln!("skipping the_ladder_yields_a_coordinate_even_when_every_rung_fails: no jj");
            return;
        };

        let head = inherited_head(
            &fx.jj,
            &fx.store,
            "agent/never-created",
            Some(ABSENT_COMMIT),
            "no-such-base-branch",
            |_| None,
        );

        assert_eq!(
            head, "root()",
            "the ladder degrades to jj's root commit rather than failing the spawn"
        );
    }

    // ---- The mint and a locked writer's rollback ------------------------
    //
    // The specimen is cairn/4897. A base-advance rebase of a sibling branch took
    // the per-store lock, snapshotted the op log, rebased, hit a content
    // conflict, and rolled back by restoring that snapshot. A job prepared while
    // it ran created its own branch in the middle of that window, without the
    // lock, and the restore erased it.

    /// A mint that arrives while another writer holds the store lock waits for
    /// that writer to finish, including its rollback, so the branch it creates
    /// survives. Before the fix the mint ran inside the window and the restore
    /// removed it.
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn a_mint_during_a_locked_writers_rollback_survives_the_rollback() {
        let Some(fx) = successor_fixture() else {
            eprintln!("skipping a_mint_during_a_locked_writers_rollback: no jj");
            return;
        };
        let repo = fx.project.path().to_string_lossy().to_string();
        let orch = test_orchestrator(migrated_test_db("mint-under-rollback.db").await);
        let job = seed_successor(&orch, &repo, Continuity::Clean, false).await;
        const MINTED: &str = "agent/proj-4602-builder-1";
        const SIBLING: &str = "agent/proj-9-builder-0";

        // The sibling reconcile: lock, snapshot, a local-view write of its own.
        let reconcile = orch
            .acquire_jj_store_lock(&fx.store, "sibling reconcile (test)")
            .await;
        let snapshot = crate::jj::operation_id(&fx.jj, &fx.store).unwrap();
        crate::jj::create_bookmark_at(&fx.jj, &fx.store, SIBLING, &fx.base).unwrap();

        let base = fx.base.clone();
        let rev_parse = move |revision: &str| (revision == "main").then(|| base.clone());
        let prepared = std::thread::scope(|scope| {
            let preparing = scope.spawn(|| {
                prepare_store_coordinate(
                    &orch,
                    &fx.jj,
                    &fx.store,
                    fx.project.path(),
                    &job,
                    &mints(),
                    rev_parse,
                    |_| unreachable!("a minting job reads no parent"),
                    || Ok(MINTED.to_string()),
                )
            });

            // Give the preparation every chance to run ahead of the lock.
            std::thread::sleep(std::time::Duration::from_millis(500));
            assert_eq!(
                crate::jj::bookmark_commit(&fx.jj, &fx.store, MINTED),
                None,
                "the mint must wait for the writer holding the store lock"
            );

            // The rebase conflicted: roll back to the snapshot, then release.
            crate::jj::restore_operation(&fx.jj, &fx.store, &snapshot).unwrap();
            assert_eq!(
                crate::jj::bookmark_commit(&fx.jj, &fx.store, SIBLING),
                None,
                "the rollback undid the writer's own change"
            );
            drop(reconcile);

            preparing.join().expect("preparation thread panicked")
        })
        .expect("preparation succeeds once the lock is released");

        assert_eq!(prepared.branch.as_deref(), Some(MINTED));
        assert_eq!(
            crate::jj::bookmark_commit(&fx.jj, &fx.store, MINTED).as_deref(),
            Some(fx.base.as_str()),
            "the job's branch exists after the rollback, at the base it was minted from"
        );
    }

    /// The same race for a bare import. A git ref that appears in the project
    /// while a locked writer is mid-transaction is imported only after that
    /// writer's rollback, so the rollback's re-export cannot delete it from git.
    /// Unlocked, the import landed inside the window, the restore rewound the
    /// bookmark it created, and the export then removed the git ref itself.
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn an_import_during_a_locked_writers_rollback_keeps_the_imported_ref() {
        let Some(fx) = successor_fixture() else {
            eprintln!("skipping an_import_during_a_locked_writers_rollback: no jj");
            return;
        };
        let orch = test_orchestrator(migrated_test_db("import-under-rollback.db").await);
        const ARRIVED: &str = "feature/arrived-mid-rollback";

        let reconcile = orch
            .acquire_jj_store_lock(&fx.store, "sibling reconcile (test)")
            .await;
        let snapshot = crate::jj::operation_id(&fx.jj, &fx.store).unwrap();
        // A ref reaches the project's git while the writer holds the lock.
        git(fx.project.path(), &["branch", ARRIVED, &fx.base]);

        let imported = std::thread::scope(|scope| {
            let importing = scope.spawn(|| {
                import_project_store_locked(
                    &orch,
                    &fx.jj,
                    &fx.store,
                    fx.project.path(),
                    "test import",
                )
            });
            std::thread::sleep(std::time::Duration::from_millis(500));
            assert_eq!(
                crate::jj::bookmark_commit(&fx.jj, &fx.store, ARRIVED),
                None,
                "the import must wait for the writer holding the store lock"
            );
            crate::jj::restore_operation(&fx.jj, &fx.store, &snapshot).unwrap();
            drop(reconcile);
            importing.join().expect("import thread panicked")
        });
        imported.expect("the import succeeds once the lock is released");

        assert_eq!(
            git_stdout(
                fx.project.path(),
                &["rev-parse", &format!("refs/heads/{ARRIVED}")]
            ),
            fx.base,
            "the rollback's export must not have deleted the ref from git"
        );
        assert_eq!(
            crate::jj::bookmark_commit(&fx.jj, &fx.store, ARRIVED).as_deref(),
            Some(fx.base.as_str()),
            "the store holds the imported ref after the rollback"
        );
    }
}
