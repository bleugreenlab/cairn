//! Permission request handling for the worktree fence and Codex prompts.
//!
//! A pending request is stored in `permission_requests`, an event is emitted for
//! the frontend, and the run waits (blocking) for the user to approve or deny via
//! the Orchestrator's `permission_responses` broadcast channel. The worktree
//! fence routes its crossings through [`await_permission_decision`]; an allow
//! re-executes the originating verb with the grant in place. Codex tool prompts
//! resolve through the same store/respond machinery via
//! [`resolve_permission_request`].

use crate::backends::{stdin, AgentPermissions};
use crate::mcp::types::McpCallbackRequest;
use crate::models::{ExecutionSnapshot, Fence, TurnStartReason, TurnState, TurnYieldReason};
use crate::orchestrator::Orchestrator;
use crate::storage::{DbError, DbResult, LocalDb, RowExt};
use cairn_common::authorization::AuthorityLifetimeKind;
use cairn_common::identity::{
    AppearanceSnapshot, AppearanceTransport, PrincipalPosition, PrincipalRef,
};
use cairn_common::ids;
use cairn_db::turso::params;

use super::{emit_attention, AttentionEvent};

/// Inline budget the permission handlers wait synchronously before durably
/// suspending the run. Mirrors `planning::INLINE_PROMPT_WAIT_BUDGET`: long
/// enough that a human or automation answering promptly keeps the CLI process
/// warm, short enough to suspend before the surrounding callback budget
/// expires. There is no auto-deny — a fence that no one answers stays pending
/// and is answerable whenever via the UI or the `permissions` resource.
const INLINE_PERMISSION_WAIT_BUDGET: std::time::Duration = std::time::Duration::from_secs(45);

/// Outcome of [`await_permission_decision`].
pub(crate) enum PermissionWait {
    /// Answered within the inline budget; carries the response JSON verbatim
    /// (Claude/Codex permission schema for tool prompts, `{behavior}` for a
    /// fence crossing). The successor turn has been started.
    Decided(String),
    /// Inline budget expired or the channel closed; the run was durably
    /// suspended (no auto-deny). Resume continues from the real answer.
    Suspended,
    /// The prompt could not be persisted, so no answerable request exists. This
    /// fails the operation closed without pretending that a human denied it.
    Unavailable(String),
}

pub(super) async fn retire_outer_run_batch_waits(
    conn: &cairn_db::turso::Connection,
    run_id: &str,
    tool_use_id: &str,
) -> DbResult<()> {
    conn.execute(
        "UPDATE agent_waits SET state='resolved',resolution_json=?3,resolved_at=?4 \
         WHERE run_id=?1 AND tool_use_id=?2 AND state IN ('pending','resolving') \
         AND successor_turn_id IS NULL AND result_stored_at IS NULL",
        params![
            run_id,
            tool_use_id,
            r#"{"outcome":"transferred_to_permission"}"#,
            chrono::Utc::now().timestamp_millis()
        ],
    )
    .await?;
    Ok(())
}

pub(crate) fn recovered_permission_answer(
    decision: PermissionDecision,
    provenance: &crate::channels::ledger::AskResolution,
) -> Result<PermissionAnswer, String> {
    let transport =
        serde_json::from_value(serde_json::Value::String(provenance.winner_surface.clone()))
            .map_err(|error| {
                format!(
                    "invalid stored permission winner surface {:?}: {error}",
                    provenance.winner_surface
                )
            })?;
    let surface = AnswerSurface::from_transport(transport)?;
    let mut answer = PermissionAnswer::from_surface(decision, surface)
        .with_containment_scope(PermissionScope::Once);
    answer.provenance_resolution_id = Some(provenance.resolution_id.clone());
    if !provenance.winner_actor.is_empty() {
        answer = answer.with_actor(provenance.winner_actor.clone());
    }

    if transport == AppearanceTransport::ChannelReply
        && !provenance.winner_provider.is_empty()
        && !provenance.winner_conversation.is_empty()
    {
        answer = answer.with_channel(
            provenance.winner_provider.clone(),
            provenance.winner_conversation.clone(),
        );
    }
    Ok(answer)
}

/// Shared suspend primitive for permission requests — the legacy tool prompt and
/// the worktree fence both route through here.
///
/// Inserts a `permission_requests` row stamped with the owning `job_id` and a
/// stable per-node `uri_segment` (`perm-N`), yields the current turn, emits the
/// frontend events + attention fact, then waits a short inline budget on the
/// `permission_responses` broadcast. On a fast answer it starts the successor
/// turn and returns [`PermissionWait::Decided`] with the response JSON verbatim.
/// On expiry or channel close it durably suspends the run (no auto-deny) and
/// returns [`PermissionWait::Suspended`]; the answer arrives later via the UI or
/// the `permissions` resource and a successor turn resumes the run.
pub(crate) async fn await_permission_decision(
    orch: &Orchestrator,
    run_id: &str,
    tool_use_id: &str,
    tool_name: &str,
    tool_input: &serde_json::Value,
) -> PermissionWait {
    let services = &orch.services;
    // Intrinsic prefixing (CAIRN-2210): a team run's permission_request is FK'd to
    // a replica-resident run, so its id must carry the run's team prefix or the
    // response path's routing_db_for_id would fail-close. Inherit the run's scope.
    let request_id = ids::mint_child(run_id);
    let now = chrono::Utc::now().timestamp() as i32;
    // Capture the live turn from the process. During the warm-reuse race window
    // this can be None even though the job's turn is already DB-visible
    // (CAIRN-2123); the closure below falls back to jobs.current_turn_id for a
    // job-owned request so the stored row never carries turn_id = NULL when the
    // turn is recoverable. The resume-time fallback in record_permission_response
    // is the load-bearing recovery; this is defensive at capture time.
    let process_turn_id = orch.process_state.get_current_turn_id(run_id);
    let tool_input_json = serde_json::to_string(tool_input).unwrap_or_default();

    let (yielded_turn, perm_segment, current_turn_id) = match orch
        .db
        .local
        .write(|conn| {
            let request_id = request_id.clone();
            let run_id = run_id.to_string();
            let tool_use_id = tool_use_id.to_string();
            let tool_name = tool_name.to_string();
            let tool_input_json = tool_input_json.clone();
            let process_turn_id = process_turn_id.clone();
            Box::pin(async move {
                // Owning node job for this run; None when the run has no job.
                let job_id = {
                    let mut rows = conn
                        .query(
                            "SELECT job_id FROM runs WHERE id = ?1 LIMIT 1",
                            params![run_id.as_str()],
                        )
                        .await?;
                    match rows.next().await? {
                        Some(row) => row.opt_text(0)?,
                        None => None,
                    }
                };

                // CAIRN-2123: prefer the live process turn; when it is absent
                // (the warm-reuse `Busy`-without-turn window) fall back to the
                // job's persisted turn so a job-owned request never stores
                // turn_id = NULL while the turn is recoverable. Project-chat runs
                // (no owning job) legitimately have no turn and keep None.
                let current_turn_id = match process_turn_id {
                    Some(turn_id) => Some(turn_id),
                    None => match job_id.as_deref() {
                        Some(owning_job_id) => {
                            let mut turn_rows = conn
                                .query(
                                    "SELECT current_turn_id FROM jobs WHERE id = ?1 LIMIT 1",
                                    params![owning_job_id],
                                )
                                .await?;
                            match turn_rows.next().await? {
                                Some(turn_row) => turn_row.opt_text(0)?,
                                None => None,
                            }
                        }
                        None => None,
                    },
                };

                // Stable per-node ordinal: count this node's existing requests.
                // Only assigned when the request is owned by a job (addressable).
                let uri_segment = if job_id.is_some() {
                    let mut count_rows = conn
                        .query(
                            "SELECT COUNT(*) FROM permission_requests pr \
                             JOIN runs r ON pr.run_id = r.id \
                             WHERE r.job_id = (SELECT job_id FROM runs WHERE id = ?1)",
                            params![run_id.as_str()],
                        )
                        .await?;
                    let ordinal = count_rows
                        .next()
                        .await?
                        .and_then(|row| row.i64(0).ok())
                        .unwrap_or(0)
                        + 1;
                    Some(format!("perm-{}", ordinal))
                } else {
                    None
                };

                conn.execute(
                    "
                    INSERT INTO permission_requests (
                        id, run_id, job_id, tool_use_id, tool_name, tool_input,
                        status, created_at, turn_id, uri_segment
                    )
                    VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'pending', ?7, ?8, ?9)
                    ",
                    params![
                        request_id.as_str(),
                        run_id.as_str(),
                        job_id.as_deref(),
                        tool_use_id.as_str(),
                        tool_name.as_str(),
                        tool_input_json.as_str(),
                        now,
                        current_turn_id.as_deref(),
                        uri_segment.as_deref()
                    ],
                )
                .await?;

                // The permission row is now the durable owner of this exact tool
                // call. Retire an outer run-batch wait in the same transaction so
                // no serialization order can leave two continuation owners.
                retire_outer_run_batch_waits(conn, &run_id, &tool_use_id).await?;

                let yielded_turn = if let Some(ref turn_id) = current_turn_id {
                    match yield_turn_for_host(conn, turn_id, TurnYieldReason::Permission).await {
                        Ok(yielded) => yielded,
                        Err(e) => {
                            log::warn!("Failed to yield turn {} for permission: {}", turn_id, e);
                            false
                        }
                    }
                } else {
                    false
                };

                match issue_id_for_run_conn(conn, &run_id).await {
                    Ok(issue_id) => {
                        if let Some(ref job_id) = job_id {
                            if let Err(e) =
                                crate::transitions::outcome::recompute_job_owner_attention_conn(
                                    conn,
                                    job_id,
                                    issue_id.as_deref(),
                                )
                                .await
                            {
                                log::warn!("Failed to recompute owner attention for {job_id}: {e}");
                            }
                        }
                    }
                    Err(e) => log::warn!("Failed to look up issue for run {}: {}", run_id, e),
                }

                Ok((yielded_turn, uri_segment, current_turn_id))
            })
        })
        .await
    {
        Ok(values) => values,
        Err(e) => {
            return PermissionWait::Unavailable(format!(
                "permission service unavailable: failed to store request: {e}"
            ))
        }
    };

    // Mark process as awaiting host (NOT GC-safe while holding continuation)
    if let Some(ref turn_id) = current_turn_id {
        orch.process_state.yield_for_host(run_id, turn_id);
    }

    // Emit event for frontend
    let _ = services.emitter.emit(
        "permission-request",
        serde_json::json!({
            "requestId": request_id,
            "runId": run_id,
            "toolUseId": tool_use_id,
            "toolName": tool_name,
            "input": tool_input,
        }),
    );
    let _ = services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "permission_requests", "action": "insert"}),
    );
    if yielded_turn {
        if let Some(turn_id) = current_turn_id.as_deref() {
            let change =
                crate::notify::turn_db_change_for_id(&orch.db.local, turn_id, "update").await;
            let _ = services.emitter.emit("db-change", change);
        }
    }

    emit_permission_attention(
        orch,
        run_id,
        tool_name,
        tool_use_id,
        tool_input,
        perm_segment.as_deref(),
    )
    .await;

    // Subscribe to permission responses broadcast channel
    let mut rx = orch.permission_responses.subscribe();
    let request_id_clone = request_id.clone();

    // Short inline fast-path; on expiry/close the run durably suspends below.
    let result = tokio::time::timeout(INLINE_PERMISSION_WAIT_BUDGET, async {
        loop {
            match rx.recv().await {
                Ok((resp_request_id, response_json)) => {
                    if resp_request_id == request_id_clone {
                        return Ok(response_json);
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    return Err("Channel closed");
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
            }
        }
    })
    .await;

    match result {
        Ok(Ok(response)) => {
            // Create successor turn for the permission response.
            if let Some(ref pred_turn_id) = current_turn_id {
                match ensure_and_start_successor_turn(
                    &orch.db.local,
                    run_id,
                    pred_turn_id,
                    TurnStartReason::PermissionResponse,
                )
                .await
                {
                    Ok(Some(successor)) => {
                        emit_successor_turn_events(&orch.db.local, &*services.emitter, &successor)
                            .await;
                        orch.process_state
                            .set_current_turn_id(run_id, Some(&successor.turn_id));
                    }
                    Ok(None) => {}
                    Err(e) => log::warn!("Failed to ensure successor turn: {}", e),
                }
            }
            PermissionWait::Decided(response)
        }
        Ok(Err(msg)) => {
            log::warn!("Permission wait channel ended for run {}: {}", run_id, msg);
            crate::orchestrator::lifecycle::suspend_run_for_durable_wait_after_handoff(
                orch,
                run_id,
                "permission_wait_suspended",
            );
            PermissionWait::Suspended
        }
        Err(_) => {
            // Inline budget expired: durably suspend (no auto-deny). The request
            // stays pending and is answerable whenever via the UI or the
            // `permissions` resource; a successor turn resumes the run.
            crate::orchestrator::lifecycle::suspend_run_for_durable_wait_after_handoff(
                orch,
                run_id,
                "permission_wait_suspended",
            );
            PermissionWait::Suspended
        }
    }
}

/// Emit the attention fact for a pending permission request so the UI toast and
/// the `watch` long-poll learn it without a follow-up read. Best-effort: a
/// missing run context simply wakes the issue.
async fn emit_permission_attention(
    orch: &Orchestrator,
    run_id: &str,
    tool_name: &str,
    tool_use_id: &str,
    tool_input: &serde_json::Value,
    perm_segment: Option<&str>,
) {
    let request = McpCallbackRequest {
        thread_id: None,
        cwd: String::new(),
        run_id: Some(run_id.to_string()),
        tool: String::new(),
        payload: serde_json::Value::Null,
        tool_use_id: Some(tool_use_id.to_string()),
    };
    let Ok(ctx) = super::run_context::lookup_run(&orch.db.local, &request).await else {
        return;
    };
    let services = &orch.services;
    let home_uri = crate::jobs::queries::home_uri_for_job(&orch.db.local, &ctx.job_id)
        .await
        .ok()
        .flatten();
    emit_attention(
        &*services.emitter,
        &AttentionEvent {
            attention_type: "permission",
            project_key: &ctx.project_key,
            home_uri: home_uri.as_deref(),
            tool_name: Some(tool_name),
        },
    );
    if let (Some(issue_id), Some(issue_number), Some(node)) = (
        ctx.issue_id.as_deref(),
        ctx.issue_number,
        ctx.job_name.as_deref(),
    ) {
        let segment = crate::jobs::queries::node_uri_segment_for_job(&orch.db.local, &ctx.job_id)
            .await
            .unwrap_or_else(|| node.to_string());
        // Point the fact at the answerable permission segment
        // (`.../permissions/perm-N`) when we know it, so a handler (coordinator,
        // user, or programmatic driver) can go straight to the decision patch
        // with no enumeration read of the collection. A sub-agent task job nests
        // its permission under the parent node
        // (`.../{parent}/task/{task}/permissions/perm-N`) so the URI resolves; a
        // top-level node uses the flat node segment (issue #143). Fall back to
        // the node/task base URI only when no segment was assigned (no owning
        // job).
        let parent_segment =
            crate::jobs::queries::parent_uri_segment_for_job(&orch.db.local, &ctx.job_id).await;
        let detail_uri = match perm_segment {
            Some(perm) => match parent_segment.as_deref() {
                Some(parent) => cairn_common::uri::build_task_permission_uri(
                    &ctx.project_key,
                    issue_number,
                    ctx.exec_seq.unwrap_or(1),
                    parent,
                    &segment,
                    perm,
                ),
                None => cairn_common::uri::build_node_permission_uri(
                    &ctx.project_key,
                    issue_number,
                    ctx.exec_seq.unwrap_or(1),
                    &segment,
                    perm,
                ),
            },
            None => cairn_common::uri::build_job_base_uri(
                &ctx.project_key,
                issue_number,
                ctx.exec_seq.unwrap_or(1),
                &segment,
                parent_segment.as_deref(),
            ),
        };
        if let Ok(issue_ctx) =
            crate::orchestrator::attention::read_issue_for_attention(&orch.db.local, issue_id).await
        {
            // Push the permission to the issue's watchers (CAIRN-1887): a `wake`
            // + `event` push keyed `permission:{issue}`, ref'd to the permission
            // row, excluding the requesting node (self-suspended on its own
            // permission). The legacy emit below still drives `cairn watch` and
            // the desktop toast.
            let issue_uri = issue_ctx.issue_uri();
            match crate::orchestrator::attention_delivery::push_to_issue_watchers(
                &orch.db.local,
                &issue_uri,
                Some(ctx.job_id.as_str()),
                &detail_uri,
                crate::orchestrator::attention_push::Wake::Wake,
                crate::orchestrator::attention_push::Boundary::Event,
                &format!("permission:{detail_uri}"),
            )
            .await
            {
                // CAIRN-1889: actively resume each idle watcher of the permission
                // via the shared resume-ladder primitive (idle -> resume; busy or
                // self-suspended -> no-op).
                Ok(recipients) => {
                    orch.notifier.emit_change("attention_pushes");
                    for recipient in &recipients {
                        if let Err(e) = crate::messages::delivery::nudge_job_for_urgency(
                            orch,
                            recipient,
                            crate::messages::queued::DeliveryUrgency::Steer,
                        ) {
                            log::warn!("permission push wake failed: {}", e);
                        }
                    }
                }
                Err(e) => log::warn!("permission push creation failed: {}", e),
            }
            orch.emit_attention_event(crate::orchestrator::AttentionEvent {
                issue_id: issue_id.to_string(),
                issue_uri: issue_ctx.issue_uri(),
                fact: crate::orchestrator::AttentionFact::Permission {
                    detail_uri,
                    content: crate::orchestrator::attention::PermissionContent {
                        tool_name: tool_name.to_string(),
                        tool_use_id: tool_use_id.to_string(),
                        input: tool_input.clone(),
                    },
                },
                attention: issue_ctx.attention,
                status: issue_ctx.status,
                updated_at: issue_ctx.updated_at,
                route_provenance: None,
            });
        }
    } else if let Some(issue_id) = ctx.issue_id.as_deref() {
        orch.wake_for_issue(issue_id).await;
    }
}

pub fn allow_response(original_input: &serde_json::Value) -> String {
    serde_json::json!({
        "behavior": "allow",
        "updatedInput": original_input
    })
    .to_string()
}

pub fn deny_response(message: &str) -> String {
    serde_json::json!({
        "behavior": "deny",
        "message": message
    })
    .to_string()
}

/// Resolve the effective worktree-fence policy for a run.
///
/// Path: run → job → execution → snapshot → agent.fence, applying
/// [`Fence::default`] when the field is `None`. The run ID is routed to its
/// owning private or team database before reading the execution snapshot.
/// Returns `None` when the authenticated coordinate cannot resolve.
pub(crate) async fn resolve_fence_policy(
    orch: &Orchestrator,
    run_id: Option<&str>,
) -> Option<Fence> {
    let run_id = run_id?.to_string();
    let owning_db = crate::execution::routing::routing_db_for_id(&orch.db, &run_id)
        .await
        .ok()?;

    let (agent_config_id, execution_id) = owning_db
        .read(|conn| {
            let run_id = run_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "
                        SELECT j.agent_config_id, j.execution_id
                        FROM runs r
                        JOIN jobs j ON r.job_id = j.id
                        WHERE r.id = ?1
                        LIMIT 1
                        ",
                        params![run_id.as_str()],
                    )
                    .await?;
                rows.next()
                    .await?
                    .map(|row| Ok((row.opt_text(0)?, row.opt_text(1)?)))
                    .transpose()
            })
        })
        .await
        .ok()
        .flatten()?;

    // Look up the agent snapshot from the execution
    let execution_id = execution_id?;
    let agent_config_id = agent_config_id?;

    let snapshot_json = owning_db
        .query_opt_text(
            "SELECT snapshot FROM executions WHERE id = ?1 LIMIT 1",
            params![execution_id.as_str()],
        )
        .await
        .ok()
        .flatten()?;

    let snapshot = crate::config::snapshot_migrate::load(&snapshot_json).ok()?;
    let agent = snapshot.agents.get(&agent_config_id)?;
    Some(agent.fence.unwrap_or_default())
}

// ============================================================================
// Permission resolution (canonical core)
//
// Moved out of the Tauri command so the cairn-core resource dispatcher can
// resolve a permission without calling up into Tauri. Both the Tauri
// `respond_to_permission` command and the `permissions` resource patch are thin
// callers of `resolve_permission_request`.
// ============================================================================

/// The decision a user (or a `permissions` resource patch) made on a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermissionDecision {
    Allow,
    Deny,
}

/// Whether a **containment** allow applies once or for the rest of the session.
///
/// This is the fence's concept: a `Session` answer inserts a concrete host path
/// into an in-process grant set for as long as the app runs. It is NOT
/// [`cairn_common::authorization::AuthorityLifetime`], which is a journaled,
/// revocable binding of a named authority scope that survives a restart. The
/// two travel together on [`PermissionAnswer`] and must not be substituted for
/// each other.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermissionScope {
    Once,
    Session,
}

/// An answering surface that carries **no** authority-minting capability.
///
/// Every one of these is reachable without an operator: an agent can patch a
/// node's `permissions` resource, a channel reply arrives from whoever is in
/// the chat, and a remote intent is synced from another client. They are all
/// legal ways to *deny* or *cancel* any prompt, and legal ways to allow a
/// containment crossing, but none of them can approve an authority boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnswerSurface {
    /// A `write` to a node's `permissions` resource — reachable by an agent.
    ResourcePatch,
    /// A reply in an external chat channel.
    ChannelReply,
    /// A remote intent synced from another client.
    RemoteIntent,
    /// An authenticated user whose role is below owner/admin, answering through
    /// the invoke surface.
    ///
    /// They are a real, identified person and may answer anything a prompt
    /// actually asks: deny or cancel any of them, allow a containment crossing.
    /// What they do not carry is the capability to *mint authority*, so an
    /// authority allow is refused by the resolver like any other non-operator
    /// answer. Representing the role shortfall as a surface rather than as an
    /// error is what keeps it from leaking into containment: the check lives
    /// where authority is issued, not across the whole prompt surface.
    NonOperatorInvoke,
    /// An unauthenticated call to the local invoke surface: a runner with no
    /// JWT key, reached without the desktop operator credential.
    ///
    /// This is not an identity and must never be described as one. Anything on
    /// the machine that can open a loopback socket arrives here, including an
    /// agent, which holds the runner URL in its own environment. It answers
    /// containment prompts because local tooling and scripts legitimately do,
    /// and it is journaled as itself so an audit can tell it apart from an
    /// operator answer.
    LocalInvoke,
}

impl AnswerSurface {
    /// Stable issuer tag, recorded on anything this surface produces.
    pub fn as_str(self) -> &'static str {
        match self {
            AnswerSurface::ResourcePatch => "resource_patch",
            AnswerSurface::ChannelReply => "channel_reply",
            AnswerSurface::RemoteIntent => "remote_intent",
            AnswerSurface::NonOperatorInvoke => "non_operator_invoke",
            AnswerSurface::LocalInvoke => "local_invoke",
        }
    }

    /// Reconstruct a non-authoritative answering surface from durable
    /// appearance provenance. Authenticated transports are deliberately not
    /// recoverable from their label alone: recreating an operator answer
    /// requires the original verified [`OperatorApproval`].
    pub fn from_transport(transport: AppearanceTransport) -> Result<Self, String> {
        match transport {
            AppearanceTransport::ResourcePatch => Ok(Self::ResourcePatch),
            AppearanceTransport::ChannelReply => Ok(Self::ChannelReply),
            AppearanceTransport::RemoteIntent => Ok(Self::RemoteIntent),
            AppearanceTransport::NonOperatorInvoke => Ok(Self::NonOperatorInvoke),
            AppearanceTransport::LocalInvoke => Ok(Self::LocalInvoke),
            AppearanceTransport::AuthenticatedOperator
            | AppearanceTransport::AuthenticatedDesktop => Err(format!(
                "cannot reconstruct {transport:?} permission authority without verified operator approval"
            )),
        }
    }
}

impl From<AnswerSurface> for AppearanceTransport {
    fn from(value: AnswerSurface) -> Self {
        match value {
            AnswerSurface::ResourcePatch => Self::ResourcePatch,
            AnswerSurface::ChannelReply => Self::ChannelReply,
            AnswerSurface::RemoteIntent => Self::RemoteIntent,
            AnswerSurface::NonOperatorInvoke => Self::NonOperatorInvoke,
            AnswerSurface::LocalInvoke => Self::LocalInvoke,
        }
    }
}

/// How far an answer's claim to be the operator has actually been checked.
///
/// Closed on purpose, and the two variants are **not** equivalent. Adding one is
/// a deliberate security review, not a string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorTransport {
    /// A JWT-authenticated operator with an owner or admin role. The identity
    /// is a verified authentication result about a person.
    AuthenticatedOperator,
    /// The native desktop shell of this install, proven by the desktop operator
    /// credential (`~/.cairn/operator_auth_secret`).
    ///
    /// This is a verified fact about a *process*, not about a person: it says
    /// the answer came through the desktop app rather than from anything else
    /// that can reach loopback. On a default install with no JWT key that is
    /// exactly the distinction that was missing, and the identity it carries is
    /// this machine's device id, which is what the rest of the system already
    /// attributes local work to.
    ///
    /// What backs it is that the credential never enters an agent's
    /// environment, is refused to agent reads and writes by
    /// `authorization::protected`, and is denied to the executor sandbox. Those
    /// are the properties that make this variant mean anything; see
    /// `docs/authorization.md` for what they do and do not establish.
    AuthenticatedDesktop,
}

impl OperatorTransport {
    pub fn as_str(self) -> &'static str {
        match self {
            OperatorTransport::AuthenticatedOperator => "authenticated_operator",
            OperatorTransport::AuthenticatedDesktop => "authenticated_desktop",
        }
    }

    pub fn issuer(self) -> &'static str {
        match self {
            OperatorTransport::AuthenticatedOperator | OperatorTransport::AuthenticatedDesktop => {
                OPERATOR_PROMPT_ISSUER
            }
        }
    }
}

impl From<OperatorTransport> for AppearanceTransport {
    fn from(value: OperatorTransport) -> Self {
        match value {
            OperatorTransport::AuthenticatedOperator => Self::AuthenticatedOperator,
            OperatorTransport::AuthenticatedDesktop => Self::AuthenticatedDesktop,
        }
    }
}

/// Proof that an answer came from an **authenticated operator** over a trusted
/// transport. Holding one is the capability to mint an authority grant.
///
/// The fields are private and the only way to build one is
/// [`OperatorApproval::authenticated`], which takes an identity the caller
/// resolved from its own authentication context plus a closed transport tag.
/// Nothing an agent authors reaches it: not an `answered_by` string, not an
/// approver field in a payload, not the requesting agent's identity, not the
/// prompt's own contents. That is the point — "only an operator may expand
/// workspace authority" has to be a property of the type that crosses the
/// resolver boundary, not a string comparison somewhere inside it.
///
/// This is not a cryptographic capability, and in-process Rust cannot make it
/// one: any code in the workspace could call the constructor. What it does
/// guarantee is that doing so is a deliberate, greppable, reviewable act by a
/// caller that must first name an operator identity and a transport — where the
/// previous shape let any caller flip a `&'static str` field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorApproval {
    actor: PrincipalRef,
    appearance: AppearanceSnapshot,
}

impl OperatorApproval {
    pub fn authenticated(
        actor: PrincipalRef,
        appearance: AppearanceSnapshot,
    ) -> Result<Self, String> {
        actor
            .validate_at(PrincipalPosition::DecisionActor)
            .map_err(|error| error.to_string())?;
        appearance.validate().map_err(|error| error.to_string())?;
        if appearance.principal() != &actor {
            return Err("the approval actor must equal the appearance principal".to_string());
        }
        Ok(Self { actor, appearance })
    }

    pub fn actor(&self) -> &PrincipalRef {
        &self.actor
    }
    pub fn appearance(&self) -> &AppearanceSnapshot {
        &self.appearance
    }
}

/// Who answered a permission request.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Answerer {
    /// An authenticated operator. The only answerer that can mint authority.
    Operator(Box<OperatorApproval>),
    /// Any other surface. May deny or cancel anything, and may allow a
    /// containment crossing, but never mints an authority grant.
    Surface(AnswerSurface),
}

/// The issuer tag recorded for an operator answer.
pub const OPERATOR_PROMPT_ISSUER: &str = "operator_prompt";

/// What was answered on a permission request, and by whom.
///
/// One request row can be a fence crossing, a legacy tool prompt, or an
/// authority request, so the answer carries both lifetime concepts and the
/// resolver applies whichever the stored request actually is. Keeping them as
/// separate fields — rather than one overloaded enum — is what stops a
/// containment answer from being read as an authority grant.
///
/// The answerer is private and set at construction. Attribution is therefore
/// derived from the capability the caller actually held, never from a field a
/// caller could set to whatever it wanted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PermissionAnswer {
    pub decision: PermissionDecision,
    /// Containment reuse, for a fence crossing or legacy tool prompt.
    pub scope: PermissionScope,
    /// Authority grant lifetime, for an authority request. `None` falls back to
    /// a single use, which is the narrowest reading of an unspecified answer.
    pub lifetime: Option<AuthorityLifetimeKind>,
    /// Optional absolute expiry (unix seconds) for the minted grant.
    pub expires_at: Option<i64>,
    answerer: Answerer,
    authenticated_appearance: Option<(PrincipalRef, AppearanceSnapshot)>,
    /// Compatibility representation for admitted channel/resource principals.
    /// Structured as provider/namespace/id so it can be wrapped as
    /// `PrincipalRef::External` without a data migration.
    provenance_actor: Option<String>,
    provenance_provider: Option<String>,
    provenance_conversation: Option<String>,
    provenance_resolution_id: Option<String>,
}

impl PermissionAnswer {
    /// An answer from an authenticated operator.
    pub fn from_operator(decision: PermissionDecision, approval: OperatorApproval) -> Self {
        Self {
            decision,
            scope: PermissionScope::Once,
            lifetime: None,
            expires_at: None,
            answerer: Answerer::Operator(Box::new(approval)),
            authenticated_appearance: None,
            provenance_actor: None,
            provenance_provider: None,
            provenance_conversation: None,
            provenance_resolution_id: None,
        }
    }

    /// An answer from a surface that holds no operator capability.
    pub fn from_surface(decision: PermissionDecision, surface: AnswerSurface) -> Self {
        Self {
            decision,
            scope: PermissionScope::Once,
            lifetime: None,
            expires_at: None,
            answerer: Answerer::Surface(surface),
            authenticated_appearance: None,
            provenance_actor: None,
            provenance_provider: None,
            provenance_conversation: None,
            provenance_resolution_id: None,
        }
    }

    pub fn with_actor(mut self, actor: impl Into<String>) -> Self {
        self.provenance_actor = Some(actor.into());
        self
    }

    pub fn with_channel(
        mut self,
        provider: impl Into<String>,
        conversation: impl Into<String>,
    ) -> Self {
        self.provenance_provider = Some(provider.into());
        self.provenance_conversation = Some(conversation.into());
        self
    }

    #[cfg(test)]
    pub(crate) fn channel_provenance(&self) -> (Option<&str>, Option<&str>) {
        (
            self.provenance_provider.as_deref(),
            self.provenance_conversation.as_deref(),
        )
    }

    pub(crate) fn resolution_provenance(
        &self,
    ) -> Result<(String, AppearanceTransport, String, Option<String>), String> {
        let transport: AppearanceTransport = match &self.answerer {
            Answerer::Operator(approval) => approval.appearance().evidence().transport,
            Answerer::Surface(surface) => (*surface).into(),
        };
        let surface = serde_json::to_string(&transport)
            .map_err(|error| error.to_string())?
            .trim_matches('"')
            .to_string();
        let actor = self
            .decision_attribution()
            .map(|(actor, _)| serde_json::to_string(actor).map_err(|error| error.to_string()))
            .transpose()?
            .or_else(|| self.provenance_actor.clone());
        if matches!(
            transport,
            AppearanceTransport::ChannelReply | AppearanceTransport::ResourcePatch
        ) && actor.as_deref().is_none_or(|value| value.trim().is_empty())
        {
            return Err(format!(
                "{surface} resolution requires an authenticated actor"
            ));
        }
        Ok((
            self.provenance_resolution_id
                .clone()
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
            transport,
            surface,
            actor,
        ))
    }

    pub fn with_authenticated_appearance(
        mut self,
        actor: PrincipalRef,
        appearance: AppearanceSnapshot,
    ) -> Result<Self, String> {
        actor
            .validate_at(PrincipalPosition::DecisionActor)
            .map_err(|error| error.to_string())?;
        appearance.validate().map_err(|error| error.to_string())?;
        if appearance.principal() != &actor {
            return Err("the answer actor must equal the appearance principal".to_string());
        }
        self.authenticated_appearance = Some((actor, appearance));
        Ok(self)
    }

    /// Set the **containment** reuse scope (fence crossing / legacy tool prompt).
    pub fn with_containment_scope(mut self, scope: PermissionScope) -> Self {
        self.scope = scope;
        self
    }

    /// Set the **authority** grant lifetime an allow would mint.
    pub fn with_lifetime(mut self, lifetime: Option<AuthorityLifetimeKind>) -> Self {
        self.lifetime = lifetime;
        self
    }

    pub fn with_expiry(mut self, expires_at: Option<i64>) -> Self {
        self.expires_at = expires_at;
        self
    }

    /// The operator capability this answer carries, if any. An authority allow
    /// is accepted only when this is `Some`.
    pub fn operator_approval(&self) -> Option<&OperatorApproval> {
        match &self.answerer {
            Answerer::Operator(approval) => Some(approval),
            Answerer::Surface(_) => None,
        }
    }

    /// Stable issuer tag for the journal: the real answering surface, never a
    /// relabelling. An agent's resource patch is recorded as `resource_patch`
    /// even when it denies something an operator would also have denied.
    pub fn issuer(&self) -> &'static str {
        match &self.answerer {
            Answerer::Operator(_) => OPERATOR_PROMPT_ISSUER,
            Answerer::Surface(surface) => surface.as_str(),
        }
    }

    /// The approver a grant records. Only ever the authenticated operator
    /// identity — a non-operator answer has no approver, rather than an
    /// unverified name.
    pub fn decision_attribution(&self) -> Option<(&PrincipalRef, &AppearanceSnapshot)> {
        match &self.answerer {
            Answerer::Operator(approval) => Some((approval.actor(), approval.appearance())),
            Answerer::Surface(_) => self
                .authenticated_appearance
                .as_ref()
                .map(|(actor, appearance)| (actor, appearance)),
        }
    }

    fn grant_lifetime(&self) -> AuthorityLifetimeKind {
        self.lifetime.unwrap_or(AuthorityLifetimeKind::Once)
    }
}

/// Result of resolving a permission request.
#[derive(Debug)]
pub struct ResolveOutcome {
    /// The request had already been answered.
    pub duplicate: bool,
    /// Owning issue, for status recompute / wake by the caller.
    pub issue_id: Option<String>,
}

/// A `permission_requests` row, loaded for resolution.
#[derive(Debug, Clone)]
pub struct PermissionRequestRecord {
    pub id: String,
    pub run_id: String,
    pub tool_use_id: String,
    pub tool_name: String,
    pub tool_input: String,
    pub status: String,
}

impl PermissionRequestRecord {
    fn from_row(row: &cairn_db::turso::Row) -> DbResult<Self> {
        Ok(Self {
            id: row.text(0)?,
            run_id: row.text(1)?,
            tool_use_id: row.text(2)?,
            tool_name: row.text(3)?,
            tool_input: row.text(4)?,
            status: row.text(5)?,
        })
    }
}

/// Detail stored in a fence request's `tool_input`. Its presence distinguishes a
/// worktree-fence crossing (re-execute the verb on allow) from a legacy tool
/// prompt (the decision itself is the result). Carries the originating verb
/// request so the slow-path resume can re-dispatch it with the grant in place.
#[derive(Debug, Clone, serde::Deserialize)]
struct CrossingDetail {
    /// Crossing classification. Required so a legacy `tool_input` never parses
    /// as a crossing by accident, and read at resolution time to tell a
    /// path-scoped crossing from a command-scoped escape.
    kind: String,
    /// The verb to re-dispatch: "read" | "write" | "run" (legacy "change"
    /// still accepted for crossings suspended before the verb rename).
    verb: String,
    /// Canonical key inserted into `session_allowed_crossings` for a session grant.
    descriptor: String,
    /// Human-readable crossing summary (used in the deny message).
    summary: String,
    /// The originating verb request, re-dispatched verbatim on a slow-path allow.
    request: McpCallbackRequest,
    /// Origin marker for background terminal fence prompts, which should not
    /// resume/re-dispatch an agent turn when answered.
    #[serde(default)]
    origin: Option<String>,
}

/// Parse a stored `tool_input` as a fence crossing. Returns `None` for a legacy
/// tool prompt (whose `tool_input` lacks the verb/descriptor/request shape) and
/// for an authority request (which carries no `descriptor`, by design).
fn parse_crossing_detail(tool_input: &str) -> Option<CrossingDetail> {
    serde_json::from_str::<CrossingDetail>(tool_input).ok()
}

/// Test-visible probe proving the fence and authority stored shapes stay
/// mutually exclusive.
#[cfg(test)]
pub(crate) fn parses_as_crossing(tool_input: &str) -> Option<String> {
    parse_crossing_detail(tool_input).map(|detail| detail.descriptor)
}

impl CrossingDetail {
    fn is_terminal_origin(&self) -> bool {
        self.origin.as_deref() == Some("terminal")
    }

    /// Whether allowing this crossing re-executes the command **unsandboxed**.
    ///
    /// A path-scoped crossing widens one named path and the sandbox is still
    /// constructed around the re-execution. Anything else has no path to widen
    /// -- the kernel reported a blocked command without reporting what it
    /// touched -- so the only way to let it proceed is to run it with no sandbox
    /// at all.
    ///
    /// Asked as "is this a known path-scoped kind?" rather than "is this the
    /// command kind?", so an unrecognized tag is an escape. See
    /// [`fence::tag_is_path_scoped`] for the two ways an unrecognized tag
    /// arrives.
    fn is_unsandboxed_escape(&self) -> bool {
        !crate::mcp::handlers::fence::tag_is_path_scoped(&self.kind)
    }
}

/// Live-wait state captured while recording a permission answer. Public for the
/// suspend/resume regression tests, which call [`record_permission_response`]
/// directly to assert the NULL-turn predecessor recovery.
#[derive(Debug)]
pub struct PermissionResponseResume {
    pub run_id: String,
    pub session_id: Option<String>,
    pub issue_id: Option<String>,
    pub predecessor_turn_id: Option<String>,
    pub successor_turn_id: Option<String>,
    pub job_id: Option<String>,
    pub duplicate: bool,
}

/// Persist an "allow all" decision for the agent that owns a permission request.
///
/// The normal permission response schema only has per-request allow/deny. The
/// desktop's Allow all button additionally flips the requesting agent's worktree
/// fence in the execution snapshot to [`Fence::Allow`] and tells any live process
/// for that agent to switch permission mode. The database is resolved from the
/// request id so team executions update their synced replica rather than the
/// private database.
///
/// # Why this takes an [`OperatorApproval`]
///
/// Turning off an agent's containment fence is a capability expansion, and it is
/// the one that subsumes every other. `Fence::Allow` means
/// [`sandbox_applies`](crate::services::sandbox::sandbox_applies) is false, so
/// no sandbox policy is built at all: after this call the same live agent's next
/// shell item runs unconfined, no kernel denial is produced, and every refusal
/// keyed on a denial -- including the one protecting the desktop operator
/// credential -- has nothing to fire on. An agent that could call this without a
/// capability could therefore read the credential that approves authority, which
/// would make the authority boundary a function of a switch the agent controls.
///
/// So the capability is required by the signature rather than checked by a
/// caller, the same way [`PermissionAnswer::from_operator`] carries it across
/// the resolver boundary. A surface answering a containment prompt with a plain
/// `allow` is unaffected; only turning the fence off needs an operator.
pub async fn allow_all_for_request(
    orch: &Orchestrator,
    request_id: &str,
    approval: &OperatorApproval,
) -> Result<(), String> {
    // A team execution's executions/jobs rows live in its synced replica, so the
    // snapshot read+write and the active-job lookup must target the owning
    // database (CAIRN-2227). Fail-closed (CAIRN-2170 class): an `allowAll` whose
    // replica is not open errors rather than editing a stale private snapshot.
    let db = crate::execution::routing::owning_db_for_permission_request(&orch.db, request_id)
        .await
        .map_err(|e| e.to_string())?;

    // "Allow all" is a containment decision, and an authority prompt is not a
    // containment prompt. Flipping the fence here would answer a question the
    // operator was not asked -- they were shown a named capability to approve,
    // not a host path -- and it would leave the agent with an unlogged, blanket
    // escape as a side effect of approving one MCP server. Refuse rather than
    // silently doing the wrong half: the authority answer they want is a
    // lifetime on the prompt itself, which is journaled and revocable.
    if let Ok(record) = get_permission_request_record(&db, request_id).await {
        if super::authority::parse_authority_detail(&record.tool_input).is_some() {
            return Err(
                "'Allow all' turns off this agent's containment fence, which is a different \
                 decision from the authority this prompt is asking about. Approve the request \
                 itself with a lifetime instead; a standing approval is listable and revocable \
                 where a fence flip is neither."
                    .to_string(),
            );
        }
    }

    let target = load_permission_snapshot_target(&db, request_id).await?;
    let Some(mut snapshot) = target.snapshot else {
        return Err("Execution has no snapshot".to_string());
    };

    let agent = snapshot.agents.get_mut(&target.agent_id).ok_or_else(|| {
        format!(
            "Agent '{}' not found in execution snapshot",
            target.agent_id
        )
    })?;
    agent.fence = Some(Fence::Allow);
    let snapshot_json = snapshot.to_json()?;

    // A fence flip is neither journaled nor revocable (which is why an authority
    // prompt refuses it above), so the log line is the only record that it
    // happened and who did it. Names the operator and the transport that was
    // actually verified, never anything the payload claimed.
    log::info!(
        "permission {request_id}: operator {} ({}) turned off agent '{}' containment fence for execution {}",
        serde_json::to_string(approval.actor()).unwrap_or_else(|_| "<invalid actor>".to_string()),
        serde_json::to_string(&approval.appearance().evidence().transport).unwrap_or_else(|_| "<invalid transport>".to_string()),
        target.agent_id,
        target.execution_id,
    );

    update_execution_snapshot(&db, &target.execution_id, &snapshot_json).await?;
    // Process-state propagation is host-local (live processes are keyed by
    // session in memory), but the job-session lookup it drives reads the owning
    // database.
    propagate_fence_allow_to_processes(orch, &db, &target.execution_id, &target.agent_id).await;

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "executions", "action": "update"}),
    );

    Ok(())
}

struct PermissionSnapshotTarget {
    execution_id: String,
    agent_id: String,
    snapshot: Option<ExecutionSnapshot>,
}

async fn load_permission_snapshot_target(
    db: &LocalDb,
    request_id: &str,
) -> Result<PermissionSnapshotTarget, String> {
    let request_id = request_id.to_string();
    db.read(|conn| {
        let request_id = request_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "
                    SELECT j.execution_id, j.agent_config_id, e.snapshot
                    FROM permission_requests pr
                    JOIN runs r ON pr.run_id = r.id
                    JOIN jobs j ON COALESCE(pr.job_id, r.job_id) = j.id
                    JOIN executions e ON j.execution_id = e.id
                    WHERE pr.id = ?1
                    LIMIT 1
                    ",
                    params![request_id.as_str()],
                )
                .await?;
            let row = rows
                .next()
                .await?
                .ok_or_else(|| DbError::Row(format!("job not found for request: {request_id}")))?;
            let execution_id = row.text(0)?;
            let agent_id = row
                .opt_text(1)?
                .ok_or_else(|| DbError::Row("Job has no agent_config_id".to_string()))?;
            let snapshot = row
                .opt_text(2)?
                .map(|json| crate::config::snapshot_migrate::load(&json))
                .transpose()
                .map_err(DbError::Row)?;
            Ok(PermissionSnapshotTarget {
                execution_id,
                agent_id,
                snapshot,
            })
        })
    })
    .await
    .map_err(|e| format!("Failed to load permission snapshot target: {e}"))
}

async fn update_execution_snapshot(
    db: &LocalDb,
    execution_id: &str,
    snapshot_json: &str,
) -> Result<(), String> {
    db.execute(
        "UPDATE executions SET snapshot = ?1 WHERE id = ?2",
        params![snapshot_json, execution_id],
    )
    .await
    .map(|_| ())
    .map_err(|e| format!("Failed to update execution snapshot: {e}"))
}

async fn propagate_fence_allow_to_processes(
    orch: &Orchestrator,
    db: &LocalDb,
    execution_id: &str,
    agent_id: &str,
) {
    let rows = match load_agent_job_sessions(db, execution_id, agent_id).await {
        Ok(rows) => rows,
        Err(e) => {
            log::warn!("Failed to query jobs for fence allow propagation: {e}");
            return;
        }
    };

    let perms = AgentPermissions::new(Fence::Allow);
    let mode = perms.to_legacy_str();
    for (job_id, session_id) in rows {
        let Some(session_id) = session_id else {
            continue;
        };
        let Some(run_id) = orch.process_state.find_process_by_session(&session_id) else {
            continue;
        };
        if let Err(e) = stdin::send_set_permission_mode(&orch.process_state, &run_id, mode) {
            log::warn!(
                "Failed to propagate allow fence to job {}: {}",
                &job_id[..job_id.len().min(8)],
                e
            );
        }
    }
}

async fn load_agent_job_sessions(
    db: &LocalDb,
    execution_id: &str,
    agent_id: &str,
) -> Result<Vec<(String, Option<String>)>, String> {
    let execution_id = execution_id.to_string();
    let agent_id = agent_id.to_string();
    db.read(|conn| {
        let execution_id = execution_id.clone();
        let agent_id = agent_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, current_session_id
                     FROM jobs
                     WHERE execution_id = ?1
                       AND agent_config_id = ?2
                       AND status NOT IN ('complete', 'failed')",
                    params![execution_id.as_str(), agent_id.as_str()],
                )
                .await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                out.push((row.text(0)?, row.opt_text(1)?));
            }
            Ok(out)
        })
    })
    .await
    .map_err(|e| format!("Failed to load active job sessions: {e}"))
}

/// Resolve a pending permission request: record the answer, record any session
/// grant, broadcast (waking an inline-waiting handler), and — when the run
/// durably suspended past the inline budget — resume it.
///
/// For a worktree-fence crossing, an allow re-executes the originating verb with
/// the grant in place and attaches its real output as the synthetic tool_result;
/// a deny attaches an error result. For a legacy tool prompt, the response JSON
/// itself is the result. Callers map their UI/resource shapes to
/// `(decision, scope)` and call this. Session scope remains internal bookkeeping;
/// prompt response payloads only expose allow/deny behaviors.
pub async fn resolve_permission_request(
    orch: &Orchestrator,
    request_id: &str,
    answer: PermissionAnswer,
) -> Result<ResolveOutcome, String> {
    resolve_permission_request_impl(orch, request_id, answer, true).await
}

pub(crate) async fn resolve_permission_request_domain(
    orch: &Orchestrator,
    request_id: &str,
    answer: PermissionAnswer,
) -> Result<ResolveOutcome, String> {
    resolve_permission_request_impl(orch, request_id, answer, false).await
}

async fn resolve_permission_request_impl(
    orch: &Orchestrator,
    request_id: &str,
    answer: PermissionAnswer,
    use_gate: bool,
) -> Result<ResolveOutcome, String> {
    let (resolution_id, resolution_transport, resolution_surface, resolution_actor) =
        answer.resolution_provenance()?;
    let resolution_provider = answer.provenance_provider.clone();
    let resolution_conversation = answer.provenance_conversation.clone();
    let decision = answer.decision;
    let scope = answer.scope;
    let now = chrono::Utc::now().timestamp() as i32;
    // Resolve the owning database ONCE (fail-closed, CAIRN-2227): a team
    // execution's permission_requests/runs/turns/issue rows live WHOLLY in its
    // synced replica, so the response record, the successor turn, and the
    // issue-status recompute must all land there. Reading the private DB instead
    // returns no row — the answer errors `Permission request not found` and
    // returns before the resume, leaving the run parked at the approval gate.
    // The resume mechanics (store_tool_result_event_with_turn / continue_job_impl)
    // already self-route by run/job, so threading this handle covers the rest.
    let owning_db =
        crate::execution::routing::owning_db_for_permission_request(&orch.db, request_id)
            .await
            .map_err(|e| format!("Permission request not found: {}", e))?;
    let record = get_permission_request_record(&owning_db, request_id)
        .await
        .map_err(|e| format!("Permission request not found: {}", e))?;

    let crossing = parse_crossing_detail(&record.tool_input);
    let authority = super::authority::parse_authority_detail(&record.tool_input);

    // Authority issuance is an operator capability, decided here from the
    // STORED prompt type rather than from anything the answerer said about
    // itself. The requester cannot approve its own escalation: allowing an
    // authority request is what mints a workspace-wide grant, so an answer that
    // carries no operator capability is refused before the row is touched.
    //
    // Refusing here, ahead of `record_permission_response`, is the whole point.
    // A rejected self-approval must leave the request pending for the operator:
    // it resolves nothing, mints nothing, wakes nothing, and re-dispatches
    // nothing. Deny and cancel stay open to every surface — an agent recognizing
    // that it should not be doing something is behavior worth keeping — and so
    // does allowing a containment crossing, which is a different layer.
    if let Some(detail) = authority.as_ref() {
        if matches!(decision, PermissionDecision::Allow) && answer.operator_approval().is_none() {
            journal_permission_authority_refusal(orch, &record, detail, &answer).await;
            return Err(format!(
                "Refused: allowing this request mints a workspace authority grant, which only an \
                 authenticated operator can do. This answer came from '{}', which may deny or \
                 cancel the request but cannot approve it. The request is still pending for the \
                 operator (scope: {}).",
                answer.issuer(),
                detail.scope_shorthand(),
            ));
        }
    }

    // The same capability, one layer down. Allowing a COMMAND-scoped crossing
    // re-executes an agent-authored command with no sandbox at all, which is
    // the state in which every path-based protection stops being constructed --
    // including the one keeping this agent away from the credential that
    // approves authority. That is an escape, not a containment answer, and it
    // is the same act `allow_all_for_request` requires an operator for; the
    // difference is that this one lasts for a command and that one lasts for
    // the run.
    //
    // Path-scoped crossings stay open to every surface. They widen one named
    // path, the sandbox is still built around the re-execution, and an agent
    // recognizing that it needs one file outside its worktree is ordinary work.
    if let Some(detail) = crossing.as_ref() {
        if detail.is_unsandboxed_escape()
            && matches!(decision, PermissionDecision::Allow)
            && answer.operator_approval().is_none()
        {
            return Err(format!(
                "Refused: this prompt names no host path to widen -- either the sandbox blocked a \
                 command without reporting which path it touched, or the prompt was recorded by a \
                 build whose crossing kind ('{}') this one does not recognize. Either way the only \
                 way to let it proceed is to re-run the command with no sandbox at all, which is \
                 an escape rather than one crossing, and only an operator can approve that. This \
                 answer came from '{}', which may deny it, or allow a path-scoped crossing. The \
                 request is still pending for the operator.",
                detail.kind,
                answer.issuer(),
            ));
        }
    }

    let status = match decision {
        PermissionDecision::Allow => "allowed",
        PermissionDecision::Deny => "denied",
    };
    // The winning answer is recovered after an independently leased domain
    // action, including after process loss. Persist session scope in the claim;
    // keeping it only in this invocation would narrow a recovered allow to once.
    let behavior = match (decision, scope) {
        (PermissionDecision::Deny, _) => "deny",
        (PermissionDecision::Allow, PermissionScope::Once) => "allow",
        (PermissionDecision::Allow, PermissionScope::Session) => "allow_session",
    };

    if use_gate {
        let claim = crate::channels::ledger::claim_ask_resolution(
            &owning_db,
            request_id,
            behavior,
            resolution_transport,
            None,
            None,
            resolution_actor.as_deref(),
            "permission",
            request_id,
            chrono::Utc::now().timestamp_millis(),
        )
        .await?;
        let winner = match claim {
            crate::channels::ledger::AskClaim::Won(winner)
            | crate::channels::ledger::AskClaim::Existing(winner) => winner,
        };
        let (winning_decision, winning_scope) = match winner.answer.as_str() {
            "allow" => (PermissionDecision::Allow, PermissionScope::Once),
            "allow_session" => (PermissionDecision::Allow, PermissionScope::Session),
            "deny" => (PermissionDecision::Deny, PermissionScope::Once),
            value => return Err(format!("invalid stored permission winner answer {value:?}")),
        };
        let mut winning_answer = match recovered_permission_answer(winning_decision, &winner) {
            Ok(answer) => answer.with_containment_scope(winning_scope),
            Err(_error)
                if matches!(
                    resolution_transport,
                    AppearanceTransport::AuthenticatedOperator
                        | AppearanceTransport::AuthenticatedDesktop
                ) && answer.operator_approval().is_some()
                    && winner.winner_surface == resolution_surface
                    && winner.winner_actor == resolution_actor.as_deref().unwrap_or_default() =>
            {
                // A verified caller may carry the unpersistable operator
                // capability across a retry, but only for the exact transport
                // and actor that won durably. Stored labels alone still cannot
                // recreate authority, and the stored answer below remains the
                // decision that executes.
                answer
            }
            Err(error) => return Err(error),
        };
        winning_answer.decision = winning_decision;
        winning_answer.provenance_resolution_id = Some(winner.resolution_id.clone());
        let now = chrono::Utc::now().timestamp_millis();
        let Some(lease) =
            crate::channels::ledger::try_lease_ask_action(&owning_db, request_id, now, 60_000)
                .await?
        else {
            return Ok(ResolveOutcome {
                duplicate: true,
                issue_id: None,
            });
        };
        let result = Box::pin(resolve_permission_request_impl(
            orch,
            request_id,
            winning_answer,
            false,
        ))
        .await;
        return match result {
            Ok(outcome) => {
                crate::channels::ledger::finalize_ask_resolution(
                    &owning_db,
                    request_id,
                    &format!("✓ answered: {behavior}"),
                    chrono::Utc::now().timestamp_millis(),
                )
                .await?;
                Ok(outcome)
            }
            Err(error) => {
                crate::channels::ledger::release_ask_action(&owning_db, &lease, &error).await?;
                Err(error)
            }
        };
    }

    // An authority request answers with the same minimal `{behavior}` shape a
    // fence crossing does: both re-drive the originating verb, so the waiting
    // handler only needs to know allow from deny.
    let structured = crossing.is_some() || authority.is_some();
    let response_json = build_permission_response_json(&record, behavior, structured);

    let resume = record_permission_response(&owning_db, request_id, status, &response_json, now)
        .await
        .map_err(|e| e.to_string())?;

    if !resume.duplicate {
        owning_db
            .execute(
                "UPDATE permission_requests SET resolution_id = ?2, resolution_surface = ?3, resolution_provider = ?4, resolution_conversation = ?5, resolution_actor = ?6 WHERE id = ?1",
                params![request_id.to_string(), resolution_id.clone(), resolution_surface.clone(), resolution_provider.clone(), resolution_conversation.clone(), resolution_actor.clone()],
            )
            .await
            .map_err(|error| error.to_string())?;
    }

    if !resume.duplicate && matches!(decision, PermissionDecision::Deny) {
        if let Some(detail) = authority.as_ref() {
            journal_permission_authority_refusal(orch, &record, detail, &answer).await;
        }
    }

    // Mint the grant BEFORE any resume: the re-dispatched verb re-runs the same
    // authorization check, so the grant has to already be on disk for it to find
    // one. An approval that was not recorded is not an approval, so a minting
    // failure turns the allow into a refusal the agent actually sees, rather
    // than a silent re-prompt whose cause is only in the logs.
    //
    // Guarded on `duplicate` for the same reason the broadcast and the resume
    // below are: a request that was already answered has already had its
    // consequences. Without this, re-answering a completed authority prompt
    // mints another grant every time — and for a standing lifetime each one is
    // separately revocable, so revoking the grant an operator can see would
    // leave its twins live. `record_permission_response` reports the duplicate
    // rather than refusing, because a duplicate answer is an ordinary race (two
    // windows, an inline waiter and a slow-path answer), so the caller is the
    // one that has to know which effects are once-only.
    let mut mint_failure: Option<String> = None;
    if let Some(detail) = authority.as_ref() {
        if matches!(decision, PermissionDecision::Allow) && !resume.duplicate {
            if let Err(error) = mint_authority_grant(orch, &record, detail, &answer, &resume).await
            {
                log::warn!(
                    "permission {request_id}: could not record the authority grant: {error}"
                );
                mint_failure = Some(error);
            }
        }
    }

    // Containment session bookkeeping (only on allow + session). Authority
    // requests are excluded on purpose: their reuse is the journaled grant, not
    // an in-process path or tool-name set.
    if authority.is_none()
        && matches!(decision, PermissionDecision::Allow)
        && matches!(scope, PermissionScope::Session)
    {
        match crossing.as_ref() {
            Some(detail) => {
                if let Ok(mut allowed) = orch.session_allowed_crossings.lock() {
                    allowed.insert(detail.descriptor.clone());
                }
            }
            None => {
                if let Some(tool_name) = permission_request_granted_tool_name(&record) {
                    if let Ok(mut allowed) = orch.session_allowed_tools.lock() {
                        allowed.insert(tool_name);
                    }
                }
            }
        }
    }

    if let Some(issue_id) = resume.issue_id.as_deref() {
        if let Err(e) = recompute_issue_status_for_issue(&owning_db, issue_id).await {
            log::warn!("Failed to recompute issue status {}: {}", issue_id, e);
        }
        // Attention bookkeeping is host-local (CAIRN-2186), so wake stays on orch.
        orch.wake_for_issue(issue_id).await;
    }

    // Snapshot the live-wait state before broadcasting. The broadcast can wake
    // an inline waiter immediately; that waiter then transitions the process out
    // of `AwaitingHost` while this resolver is still running. If we checked after
    // the send, the slow-path guard could misread the inline wakeup as a durable
    // suspend and try to resume the same permission a second time.
    let inline_waiter_was_present = orch
        .process_state
        .is_awaiting_host(&resume.run_id, resume.predecessor_turn_id.as_deref());

    // Broadcast: wakes a handler still in its inline wait (fast path).
    //
    // A grant that failed to persist rides along as `grantError` rather than
    // flipping the answer to a deny. The operator DID allow it — recording the
    // answer as a refusal would be a lie in the durable row — but the waiter has
    // to learn that the approval did not take effect, or it would report
    // "requires operator approval" a moment after the operator approved.
    if !resume.duplicate {
        let broadcast_json = match mint_failure.as_deref() {
            None => response_json.clone(),
            Some(error) => with_grant_error(&response_json, error),
        };
        let _ = orch
            .permission_responses
            .send((request_id.to_string(), broadcast_json));
    }

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "permission_requests", "action": "update"}),
    );
    if let Some(turn_id) = resume.successor_turn_id.as_deref() {
        let change = crate::notify::turn_db_change_for_id(&owning_db, turn_id, "update").await;
        let _ = orch.services.emitter.emit("db-change", change);
    }

    // Slow path: no inline waiter was present before the answer was broadcast,
    // so the run durably suspended and must be resumed from the stored response.
    let should_resume =
        should_resume_permission_response(crossing.as_ref(), &resume, inline_waiter_was_present);
    if should_resume {
        let receipt = cairn_db::models::ResolutionReceipt {
            id: Some(resolution_id.clone()),
            surface: resolution_surface.clone(),
            provider: resolution_provider.clone(),
            conversation: resolution_conversation.clone(),
            actor: resolution_actor.clone(),
            resolved_at: now as i64 * 1000,
        };
        if let Err(e) = resume_suspended_permission(
            orch,
            &owning_db,
            &record,
            &resume,
            &response_json,
            crossing.as_ref(),
            authority.as_ref(),
            mint_failure.as_deref(),
            decision,
            scope,
            &receipt,
        )
        .await
        {
            log::warn!("Failed to resume after permission {}: {}", request_id, e);
        }
    } else if !resume.duplicate
        && !inline_waiter_was_present
        && resume.successor_turn_id.is_none()
        && !crossing
            .as_ref()
            .is_some_and(CrossingDetail::is_terminal_origin)
    {
        // Park-forever signature (CAIRN-2123): the run durably suspended (no
        // inline waiter), this is the first answer (not a duplicate), and yet no
        // successor turn was created — so the run has no turn to resume into. The
        // resume-time predecessor fallback should have recovered one; if this
        // fires, a permission was answered but its run stays parked. Logged at
        // warn so a recurrence is diagnosable from logs, not just a live DB peek.
        log::warn!(
            "permission {request_id}: durably suspended but no successor turn was created \
             (predecessor turn missing); run {} may stay parked. job_id={:?}",
            resume.run_id,
            resume.job_id
        );
    }

    Ok(ResolveOutcome {
        duplicate: resume.duplicate,
        issue_id: resume.issue_id,
    })
}

/// Resolve a permission request addressed by its node URI segment (the
/// `permissions/{segment}` resource patch path). Looks the request id up from
/// the node coordinates + segment, then delegates to
/// [`resolve_permission_request`].
#[allow(clippy::too_many_arguments)]
pub async fn answer_node_permission(
    orch: &Orchestrator,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_segment: &str,
    perm_segment: &str,
    answer: PermissionAnswer,
) -> Result<ResolveOutcome, String> {
    let request_id = lookup_permission_request_for_node(
        orch,
        project_key,
        number,
        exec_seq,
        node_segment,
        perm_segment,
    )
    .await?;
    resolve_permission_request(orch, &request_id, answer).await
}

async fn lookup_permission_request_for_node(
    orch: &Orchestrator,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_segment: &str,
    perm_segment: &str,
) -> Result<String, String> {
    let project_key = cairn_common::uri::canonical_project(project_key);
    let node_segment = node_segment.to_string();
    let perm_segment = perm_segment.to_string();
    orch.db
        .local
        .read(|conn| {
            let project_key = project_key.clone();
            let node_segment = node_segment.clone();
            let perm_segment = perm_segment.clone();
            Box::pin(async move {
                let mut project_rows = conn
                    .query(
                        "SELECT id FROM projects WHERE key = ?1 COLLATE NOCASE LIMIT 2",
                        params![project_key.as_str()],
                    )
                    .await?;
                let project_id = project_rows
                    .next()
                    .await?
                    .map(|row| row.text(0))
                    .transpose()?
                    .ok_or_else(|| {
                        DbError::Row(format!("project {project_key} not found"))
                    })?;
                if project_rows.next().await?.is_some() {
                    return Err(DbError::Row(format!(
                        "project key {project_key} is ambiguous"
                    )));
                }

                let mut rows = conn
                    .query(
                        "
                        SELECT pr.id
                        FROM permission_requests pr
                        JOIN runs r ON pr.run_id = r.id
                        JOIN jobs j ON COALESCE(pr.job_id, r.job_id) = j.id
                        JOIN executions e ON j.execution_id = e.id
                        JOIN issues i ON j.issue_id = i.id
                        WHERE i.project_id = ?1
                          AND i.number = ?2
                          AND e.seq = ?3
                          AND j.uri_segment = ?4
                          AND pr.uri_segment = ?5
                        LIMIT 1
                        ",
                        params![
                            project_id.as_str(),
                            number as i64,
                            exec_seq as i64,
                            node_segment.as_str(),
                            perm_segment.as_str()
                        ],
                    )
                    .await?;
                rows.next()
                    .await?
                    .map(|row| row.text(0))
                    .transpose()?
                    .ok_or_else(|| {
                        DbError::Row(format!(
                            "permission {perm_segment} not found for {project_key}/{number}/{exec_seq}/{node_segment}"
                        ))
                    })
            })
        })
        .await
        .map_err(|e| e.to_string())
}

/// Build the response JSON for a resolved request. A fence crossing gets a
/// minimal `{behavior}` the waiting verb handler parses; a legacy prompt gets
/// the exact Claude/Codex permission schema.
fn build_permission_response_json(
    record: &PermissionRequestRecord,
    behavior: &str,
    structured: bool,
) -> String {
    if structured {
        return match behavior {
            "deny" => serde_json::json!({"behavior": "deny"}).to_string(),
            _ => serde_json::json!({"behavior": "allow"}).to_string(),
        };
    }

    let is_codex_mcp_elicitation = is_codex_mcp_elicitation_request(record);
    let is_codex_request = record.tool_name.starts_with("codex/") || is_codex_mcp_elicitation;
    if is_codex_mcp_elicitation {
        build_codex_mcp_elicitation_response(&record.tool_input, behavior).to_string()
    } else if is_codex_request {
        build_codex_permission_response(&record.tool_name, &record.tool_input, behavior).to_string()
    } else if behavior == "deny" {
        deny_response("User denied")
    } else {
        let original_input = serde_json::from_str::<serde_json::Value>(&record.tool_input)
            .unwrap_or(serde_json::json!({}));
        allow_response(&original_input)
    }
}

/// Attach a grant-minting failure to a response the inline waiter will read.
/// Falls back to a minimal object if the response is not JSON, so the waiter
/// still learns the approval did not take effect.
fn with_grant_error(response_json: &str, error: &str) -> String {
    let mut value = serde_json::from_str::<serde_json::Value>(response_json)
        .unwrap_or_else(|_| serde_json::json!({"behavior": "allow"}));
    if let Some(object) = value.as_object_mut() {
        object.insert(
            GRANT_ERROR_KEY.to_string(),
            serde_json::Value::String(error.to_string()),
        );
    }
    value.to_string()
}

/// Key carrying a grant-minting failure on an otherwise-allowing response.
pub(crate) const GRANT_ERROR_KEY: &str = "grantError";

fn should_resume_permission_response(
    crossing: Option<&CrossingDetail>,
    resume: &PermissionResponseResume,
    inline_waiter_was_present: bool,
) -> bool {
    !crossing.is_some_and(CrossingDetail::is_terminal_origin)
        && !resume.duplicate
        && resume.successor_turn_id.is_some()
        && !inline_waiter_was_present
}

/// Resume a run that durably suspended on a permission request.
///
/// Stores a synthetic tool_result for the originating call's `tool_use_id` and
/// resumes the job. For a fence allow the result is the re-executed verb's real
/// output; for a fence deny it is an error; for a legacy prompt it is the
/// decision JSON (the prompt's own result; the backend re-runs the gated tool).
#[allow(clippy::too_many_arguments)]
async fn resume_suspended_permission(
    orch: &Orchestrator,
    owning_db: &LocalDb,
    record: &PermissionRequestRecord,
    resume: &PermissionResponseResume,
    response_json: &str,
    crossing: Option<&CrossingDetail>,
    authority: Option<&super::authority::AuthorityPromptDetail>,
    mint_failure: Option<&str>,
    decision: PermissionDecision,
    scope: PermissionScope,
    resolution: &cairn_db::models::ResolutionReceipt,
) -> Result<(), String> {
    let Some(session_id) = resume.session_id.as_deref() else {
        log::warn!(
            "permission {}: skipping durable resume — run {} has no session_id; \
             the run will not be resumed",
            record.id,
            resume.run_id
        );
        return Ok(());
    };
    let Some(job_id) = resume.job_id.as_deref() else {
        log::warn!(
            "permission {}: skipping durable resume — request on run {} has no owning \
             job; the run will not be resumed",
            record.id,
            resume.run_id
        );
        return Ok(());
    };

    // An authority allow re-drives the verb with the minted grant already on
    // disk, so the re-check finds it. Unlike a fence allow there is nothing
    // transient to insert: the grant IS the durable record, which is exactly
    // what makes it listable and revocable.
    if let Some(detail) = authority {
        // The operator allowed it but the grant did not persist, so there is no
        // authority to re-dispatch under. Say that plainly instead of letting
        // the re-check quietly re-prompt for the same thing.
        if let Some(error) = mint_failure {
            return finish_resume(
                orch,
                record,
                resume,
                session_id,
                job_id,
                (
                    format!(
                        "Approval could not be recorded, so it did not take effect: {error}. \
                         Nothing was changed; ask again."
                    ),
                    true,
                ),
                resolution,
            );
        }
        let (content, is_error) = match decision {
            PermissionDecision::Deny => (
                format!(
                    "Denied by operator: {} (scope: {})",
                    detail.authority.summary,
                    detail.scope_shorthand()
                ),
                true,
            ),
            PermissionDecision::Allow => {
                let result = re_dispatch_request(orch, &detail.verb, &detail.request).await;
                // Another ungranted boundary in the same batch re-suspended the
                // run on a fresh request; that request's answer drives the next
                // resume.
                if has_pending_permission_request(owning_db, &resume.run_id).await {
                    return Ok(());
                }
                (result, false)
            }
        };
        return finish_resume(
            orch,
            record,
            resume,
            session_id,
            job_id,
            (content, is_error),
            resolution,
        );
    }

    let (content, is_error) = match crossing {
        Some(detail) => match decision {
            PermissionDecision::Deny => (
                format!("Denied by worktree fence: {}", detail.summary),
                true,
            ),
            PermissionDecision::Allow => {
                // A `once` grant is not persisted; insert the descriptor
                // transiently so the re-dispatched verb passes the fence exactly
                // for this re-execution, then remove it. A `session` grant is
                // already persisted by the caller.
                let transient = matches!(scope, PermissionScope::Once);
                if transient {
                    if let Ok(mut allowed) = orch.session_allowed_crossings.lock() {
                        allowed.insert(detail.descriptor.clone());
                    }
                }
                let result = re_dispatch_verb(orch, detail).await;
                if transient {
                    if let Ok(mut allowed) = orch.session_allowed_crossings.lock() {
                        allowed.remove(&detail.descriptor);
                    }
                }
                // If the re-dispatched verb tripped ANOTHER ungranted crossing
                // it durably re-suspended the run on a fresh pending request.
                // That request's own answer drives the next resume — do not
                // attach the (suspend-marker) result or continue the run here,
                // which would hand the agent a misleading result and race the
                // new request's resume.
                if has_pending_permission_request(owning_db, &resume.run_id).await {
                    return Ok(());
                }
                (result, false)
            }
        },
        // Legacy/Codex prompt: the decision JSON is the prompt call's result.
        None => (response_json.to_string(), false),
    };

    finish_resume(
        orch,
        record,
        resume,
        session_id,
        job_id,
        (content, is_error),
        resolution,
    )
}

/// Attach the synthetic tool result to the call the agent is waiting on and
/// continue the job. Shared by the fence, authority, and legacy-prompt resume
/// paths so all three land the result identically.
fn finish_resume(
    orch: &Orchestrator,
    record: &PermissionRequestRecord,
    resume: &PermissionResponseResume,
    session_id: &str,
    job_id: &str,
    result: (String, bool),
    resolution: &cairn_db::models::ResolutionReceipt,
) -> Result<(), String> {
    let (content, is_error) = result;
    let now = chrono::Utc::now().timestamp() as i32;
    crate::execution::jobs::store_tool_result_event_with_resolution(
        orch,
        &resume.run_id,
        session_id,
        &record.tool_use_id,
        &content,
        is_error,
        now,
        resume.predecessor_turn_id.as_deref(),
        Some(resolution),
    )?;

    let prompt_resume = crate::execution::jobs::ResumeContext {
        suppress_user_event: true,
        ..Default::default()
    };
    crate::execution::jobs::continue_job_impl(
        orch,
        job_id,
        Some(&content),
        None,
        Some(prompt_resume),
    )
    .map_err(|e| format!("Failed to resume after permission: {}", e))?;
    Ok(())
}

/// Mint the grant an allowed authority request earns.
///
/// The anchors come from the request row and the resume state, never from the
/// answering surface: a `turn` grant anchors to the turn the run will CONTINUE
/// in (an approval that expired the instant the agent resumed would authorize
/// nothing), and a `session` grant anchors to the run's durable session so it
/// survives a runner restart.
async fn journal_permission_authority_refusal(
    orch: &Orchestrator,
    record: &PermissionRequestRecord,
    detail: &super::authority::AuthorityPromptDetail,
    answer: &PermissionAnswer,
) {
    let (decision_actor, appearance_snapshot) = answer
        .decision_attribution()
        .map(|(actor, snapshot)| (Some(actor.clone()), Some(snapshot.clone())))
        .unwrap_or((None, None));
    let event = cairn_db::storage::authority::NewAuthorizationEvent {
        scope: detail.authority.scope.clone(),
        mutation: detail.authority.mutation.as_str().to_string(),
        summary: detail.authority.summary.clone(),
        outcome: "forbidden".to_string(),
        reason: detail.reason.as_str().to_string(),
        principal: detail.principal.clone(),
        audience: detail.audience.clone(),
        run_id: detail.request.run_id.clone(),
        request_uri: Some(record.id.clone()),
        grant_id: None,
        decision_actor,
        appearance_snapshot,
        decided_at: chrono::Utc::now().timestamp(),
    };
    if let Err(error) = cairn_db::storage::authority::append_event(&orch.db.local, event).await {
        log::warn!(
            "permission {}: failed to journal refused authority decision: {error}",
            record.id
        );
    }
}

async fn mint_authority_grant(
    orch: &Orchestrator,
    record: &PermissionRequestRecord,
    detail: &super::authority::AuthorityPromptDetail,
    answer: &PermissionAnswer,
    resume: &PermissionResponseResume,
) -> Result<(), String> {
    // The operator approved exactly the mutation they were shown, so the grant
    // is narrowed to that mode: approving "reconfigure linear" must not silently
    // also approve removing it.
    let mut constraints = vec![
        cairn_common::authorization::AuthorityConstraint::MutationModes {
            modes: vec![detail.authority.mutation],
        },
    ];

    // An MCP write is additionally bound to the identity of the configuration
    // the prompt described, so the approval covers that server and not merely
    // its name. A prompt that reached the operator without one cannot be turned
    // into a grant at all: minting an unbound grant would either authorize an
    // arbitrary later command or (with the structural floor in
    // `AuthorityConstraintSet::covers`) authorize nothing while looking valid.
    if detail.authority.requires_mcp_config_binding() {
        let Some(fingerprint) = detail.authority.facts.mcp_config.clone() else {
            return Err(
                "this MCP approval does not name the configuration it would authorize".to_string(),
            );
        };
        constraints
            .push(cairn_common::authorization::AuthorityConstraint::McpConfig { fingerprint });
    }

    let (decision_actor, appearance_snapshot) = answer.decision_attribution().ok_or_else(|| {
        "an authority grant requires authenticated decision attribution".to_string()
    })?;
    decision_actor
        .validate_at(PrincipalPosition::DecisionActor)
        .map_err(|error| error.to_string())?;
    appearance_snapshot
        .validate()
        .map_err(|error| error.to_string())?;
    if appearance_snapshot.principal() != decision_actor {
        return Err("the durable grant actor must equal the appearance principal".to_string());
    }

    let issue = crate::authorization::GrantIssue {
        request: detail.authority.clone(),
        principal: detail.principal.clone(),
        audience: detail.audience.clone(),
        lifetime: answer.grant_lifetime(),
        request_id: Some(record.id.clone()),
        turn_id: resume
            .successor_turn_id
            .clone()
            .or_else(|| resume.predecessor_turn_id.clone()),
        session_id: resume.session_id.clone(),
        expires_at: answer.expires_at,
        // Both the issuer and the approver come from the capability the
        // answerer actually held, not from anything it claimed. Only an
        // authenticated operator reaches a mint at all (see the resolver's
        // authority check), so the approver here is always a real, trusted
        // identity rather than a name someone typed.
        provenance: cairn_common::authorization::AuthorityProvenance {
            issuer: answer.issuer().to_string(),
            decision_actor: Some(decision_actor.clone()),
            appearance_snapshot: Some(appearance_snapshot.clone()),
            approver: None,
            request_uri: Some(record.id.clone()),
            node_uri: detail.principal.node_uri.clone(),
            rationale: None,
        },
        constraints: cairn_common::authorization::AuthorityConstraintSet::new(constraints),
    };
    // Grants live in the private database on every path, so what enforcement
    // reads is what the grant list shows and what revocation reaches.
    //
    // For a team run this is correct because an answer is only ever executed by
    // the runner that OWNS the execution. A teammate's answer arrives as a
    // remote intent, and `team_remote_intents` gates on
    // `executions.runner_device_id` at three layers: the candidate scan joins on
    // it, the claim UPDATE carries an EXISTS on it, and `verify_owner` re-checks
    // after the claim to catch a reassignment landing in between. So the mint
    // always happens on the runner that will re-dispatch the write, which is the
    // one whose private database the re-check reads.
    //
    // Ownership CHANGING is not the hazard — it is a supported flow, and its
    // outcome is the conservative one: the old owner's grant stays on the old
    // machine and the run re-prompts on the new owner's, which is exactly right
    // for a per-install grant, since the operator who now owns the execution is
    // the one who should be asked. The hazard is an answer being executed
    // anywhere OTHER than the owning runner, which is what those three layers
    // prevent; defeating them would mint a grant on one machine while the run
    // re-prompts forever on another.
    crate::authorization::issue_grant(&orch.db.local, issue)
        .await
        .map(|_| ())
}

/// True if the run has a still-pending permission request — used to detect that
/// a slow-path re-dispatch tripped another crossing and re-suspended the run.
async fn has_pending_permission_request(db: &LocalDb, run_id: &str) -> bool {
    let run_id = run_id.to_string();
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT COUNT(*) FROM permission_requests WHERE run_id = ?1 AND status = 'pending'",
                    params![run_id.as_str()],
                )
                .await?;
            Ok(rows.next().await?.and_then(|row| row.i64(0).ok()).unwrap_or(0))
        })
    })
    .await
    .map(|count| count > 0)
    .unwrap_or(false)
}

/// Re-execute a fenced verb with the grant already in place. The verb's normal
/// output string becomes the synthetic tool_result the resumed agent sees.
///
/// The futures are boxed: a `write` that answers a `permissions` patch can
/// resolve a fenced `write` crossing, which re-enters `handle_write` here — an
/// indirectly recursive async fn that must be heap-allocated.
async fn re_dispatch_verb(orch: &Orchestrator, detail: &CrossingDetail) -> String {
    re_dispatch_request(orch, &detail.verb, &detail.request).await
}

/// Re-execute a verb from its stored request. Shared by the fence and authority
/// resume paths: once the grant (of either kind) is in place, both simply
/// re-drive the exact call the agent made.
async fn re_dispatch_request(
    orch: &Orchestrator,
    verb: &str,
    request: &McpCallbackRequest,
) -> String {
    match verb {
        "read" => Box::pin(crate::mcp::handlers::read::handle_read_file(orch, request)).await,
        // A fenced target inside a batch suspends the whole `read_batch` call; on
        // allow we re-run the batch (idempotent reads) so the approved target now
        // resolves alongside the rest.
        "read_batch" => {
            let read_cursors = std::sync::Mutex::new(std::collections::HashMap::new());
            Box::pin(crate::mcp::handlers::read::handle_read_batch(
                orch,
                request,
                &read_cursors,
            ))
            .await
        }
        // `write` is the current verb; `change` is the legacy name a crossing
        // suspended before the rename may still carry.
        "write" | "change" => {
            Box::pin(crate::mcp::handlers::write::handle_write(orch, request)).await
        }
        "run" => Box::pin(crate::mcp::handlers::run::handle_run(orch, request)).await,
        other => format!("Cannot re-execute unknown verb '{other}' after approval"),
    }
}

async fn get_permission_request_record(
    db: &LocalDb,
    request_id: &str,
) -> DbResult<PermissionRequestRecord> {
    let request_id = request_id.to_string();
    db.read(|conn| {
        let request_id = request_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "
                    SELECT id, run_id, tool_use_id, tool_name, tool_input, status
                    FROM permission_requests
                    WHERE id = ?1
                    LIMIT 1
                    ",
                    params![request_id.as_str()],
                )
                .await?;
            rows.next()
                .await?
                .map(|row| PermissionRequestRecord::from_row(&row))
                .transpose()?
                .ok_or_else(|| DbError::Row(format!("permission request not found: {request_id}")))
        })
    })
    .await
}

pub async fn record_permission_response(
    db: &LocalDb,
    request_id: &str,
    status: &str,
    response_json: &str,
    responded_at: i32,
) -> DbResult<PermissionResponseResume> {
    let request_id = request_id.to_string();
    let status = status.to_string();
    let response_json = response_json.to_string();
    let mut resume = db
        .write(|conn| {
            let request_id = request_id.clone();
            let status = status.clone();
            let response_json = response_json.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "
                        SELECT pr.run_id, r.issue_id, pr.turn_id, r.session_id, r.job_id, pr.status
                        FROM permission_requests pr
                        JOIN runs r ON pr.run_id = r.id
                        WHERE pr.id = ?1
                        LIMIT 1
                        ",
                        params![request_id.as_str()],
                    )
                    .await?;
                let row = rows.next().await?.ok_or_else(|| {
                    DbError::Row(format!("permission request not found: {request_id}"))
                })?;
                let run_id = row.text(0)?;
                let issue_id = row.opt_text(1)?;
                let session_id = row.opt_text(3)?;
                let job_id = row.opt_text(4)?;
                let current_status = row.text(5)?;
                // CAIRN-2123: a fence crossing raised during the warm-reuse
                // `Busy`-without-turn window stored `turn_id = NULL`. By the time
                // the answer arrives (past the inline budget) the job's turn is
                // long since persisted in `jobs.current_turn_id` and interrupted
                // to a terminal state by the durable suspend, so recover it as
                // the predecessor. Without this the successor turn is never
                // created, `should_resume_permission_response` stays false, and
                // the run parks forever (allow and deny both no-ops). A request
                // with no owning job legitimately has no turn and
                // must NOT be force-resumed, so the fallback is gated on job
                // ownership.
                let predecessor_turn_id = match row.opt_text(2)? {
                    Some(turn_id) => Some(turn_id),
                    None => match job_id.as_deref() {
                        Some(owning_job_id) => {
                            let mut turn_rows = conn
                                .query(
                                    "SELECT current_turn_id FROM jobs WHERE id = ?1 LIMIT 1",
                                    params![owning_job_id],
                                )
                                .await?;
                            let recovered = match turn_rows.next().await? {
                                Some(turn_row) => turn_row.opt_text(0)?,
                                None => None,
                            };
                            if recovered.is_some() {
                                log::warn!(
                                    "permission {request_id}: stored turn_id was NULL; \
                                     recovered predecessor from jobs.current_turn_id \
                                     (warm-reuse race, cairn-2123)"
                                );
                            }
                            recovered
                        }
                        None => None,
                    },
                };

                let duplicate = if current_status != "pending" {
                    true
                } else {
                    conn.execute(
                        "
                        UPDATE permission_requests
                        SET status = ?2, response = ?3, responded_at = ?4
                        WHERE id = ?1 AND status = 'pending'
                        ",
                        params![
                            request_id.as_str(),
                            status.as_str(),
                            response_json.as_str(),
                            responded_at
                        ],
                    )
                    .await?
                        == 0
                };

                Ok(PermissionResponseResume {
                    run_id,
                    session_id,
                    issue_id,
                    predecessor_turn_id,
                    successor_turn_id: None,
                    job_id,
                    duplicate,
                })
            })
        })
        .await?;

    if !resume.duplicate {
        if let Some(predecessor_turn_id) = resume.predecessor_turn_id.as_deref() {
            if let Some(successor) = ensure_and_start_successor_turn(
                db,
                &resume.run_id,
                predecessor_turn_id,
                TurnStartReason::PermissionResponse,
            )
            .await?
            {
                resume.successor_turn_id = Some(successor.turn_id);
            }
        }
    }

    Ok(resume)
}

fn is_codex_mcp_elicitation_request(record: &PermissionRequestRecord) -> bool {
    serde_json::from_str::<serde_json::Value>(&record.tool_input)
        .ok()
        .and_then(|params| {
            params
                .get("_meta")
                .and_then(|v| v.as_object())
                .and_then(|meta| meta.get("codex_approval_kind"))
                .and_then(|v| v.as_str())
                .map(|kind| kind == "mcp_tool_call")
        })
        .unwrap_or(false)
}

fn permission_request_granted_tool_name(record: &PermissionRequestRecord) -> Option<String> {
    if is_codex_mcp_elicitation_request(record) {
        serde_json::from_str::<serde_json::Value>(&record.tool_input)
            .ok()
            .and_then(|params| extract_codex_mcp_elicitation_cairn_tool_name(&params))
    } else {
        Some(record.tool_name.clone())
    }
}

fn extract_codex_mcp_elicitation_cairn_tool_name(params: &serde_json::Value) -> Option<String> {
    if params.get("serverName").and_then(|v| v.as_str()) != Some("cairn") {
        return None;
    }
    let tool_name = params
        .get("toolName")
        .and_then(|v| v.as_str())
        .or_else(|| {
            params
                .get("_meta")
                .and_then(|v| v.as_object())
                .and_then(|meta| meta.get("toolName").or_else(|| meta.get("tool_name")))
                .and_then(|v| v.as_str())
        })
        .or_else(|| {
            params
                .get("message")
                .and_then(|v| v.as_str())
                .and_then(|message| {
                    let marker = "run tool \"";
                    let start = message.find(marker)? + marker.len();
                    let tail = &message[start..];
                    let end = tail.find('"')?;
                    Some(&tail[..end])
                })
        })?;
    Some(format!("mcp__cairn__{tool_name}"))
}

fn build_codex_permission_response(
    tool_name: &str,
    tool_input_json: &str,
    behavior: &str,
) -> serde_json::Value {
    if tool_name == "codex/permissions" {
        let requested_permissions = serde_json::from_str::<serde_json::Value>(tool_input_json)
            .ok()
            .and_then(|input| input.get("permissions").cloned())
            .unwrap_or_else(|| serde_json::json!({}));
        let scope = "turn";
        let permissions = if behavior == "deny" {
            serde_json::json!({})
        } else {
            requested_permissions
        };
        serde_json::json!({ "permissions": permissions, "scope": scope })
    } else if tool_name == "codex/mcp_server_elicitation" {
        build_codex_mcp_elicitation_response(tool_input_json, behavior)
    } else {
        let decision = match behavior {
            "deny" => "decline",
            _ => "accept",
        };
        serde_json::json!({ "decision": decision })
    }
}

fn build_codex_mcp_elicitation_response(
    tool_input_json: &str,
    behavior: &str,
) -> serde_json::Value {
    let _ = tool_input_json;
    let meta = serde_json::Value::Null;
    let action = match behavior {
        "deny" => "decline",
        _ => "accept",
    };
    serde_json::json!({ "action": action, "content": null, "_meta": meta })
}

#[derive(Debug)]
pub struct SuccessorTurnUpdate {
    pub turn_id: String,
    inserted: bool,
    started: bool,
}

pub(super) async fn emit_successor_turn_events(
    db: &LocalDb,
    emitter: &dyn crate::services::EventEmitter,
    update: &SuccessorTurnUpdate,
) {
    if update.inserted {
        let change = crate::notify::turn_db_change_for_id(db, &update.turn_id, "insert").await;
        let _ = emitter.emit("db-change", change);
    }
    if update.started {
        let change = crate::notify::turn_db_change_for_id(db, &update.turn_id, "update").await;
        let _ = emitter.emit("db-change", change);
    }
}

pub(super) async fn get_issue_title(db: &LocalDb, issue_id: &str) -> Option<String> {
    let issue_id = issue_id.to_string();
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT title FROM issues WHERE id = ?1 LIMIT 1",
                    params![issue_id.as_str()],
                )
                .await?;
            crate::storage::next_text(&mut rows, 0).await
        })
    })
    .await
    .ok()
    .flatten()
}

pub(super) async fn issue_id_for_run(db: &LocalDb, run_id: &str) -> DbResult<Option<String>> {
    let run_id = run_id.to_string();
    db.read(|conn| Box::pin(async move { issue_id_for_run_conn(conn, &run_id).await }))
        .await
}

async fn issue_id_for_run_conn(
    conn: &cairn_db::turso::Connection,
    run_id: &str,
) -> DbResult<Option<String>> {
    let mut rows = conn
        .query(
            "SELECT issue_id FROM runs WHERE id = ?1 LIMIT 1",
            params![run_id],
        )
        .await?;
    rows.next()
        .await?
        .map(|row| row.opt_text(0))
        .transpose()
        .map(|value| value.flatten())
}

pub async fn recompute_issue_status_for_issue(db: &LocalDb, issue_id: &str) -> DbResult<()> {
    let issue_id = issue_id.to_string();
    db.write(|conn| {
        let issue_id = issue_id.clone();
        Box::pin(async move {
            crate::transitions::outcome::recompute_issue_status_conn(conn, &issue_id).await
        })
    })
    .await
}

pub(super) async fn yield_turn_for_host(
    conn: &cairn_db::turso::Connection,
    turn_id: &str,
    reason: TurnYieldReason,
) -> DbResult<bool> {
    let mut rows = conn
        .query(
            "SELECT state FROM turns WHERE id = ?1 LIMIT 1",
            params![turn_id],
        )
        .await?;
    let current = rows
        .next()
        .await?
        .ok_or_else(|| DbError::Row(format!("turn not found: {}", turn_id)))?
        .text(0)?;
    let state: TurnState = current.parse().map_err(DbError::Row)?;
    if state != TurnState::Running {
        return Ok(false);
    }

    let now = chrono::Utc::now().timestamp() as i32;
    let reason = reason.to_string();
    conn.execute(
        "
        UPDATE turns
        SET state = 'yielded',
            yield_reason = ?1,
            ended_at = ?2,
            updated_at = ?2
        WHERE id = ?3
        ",
        params![reason.as_str(), now, turn_id],
    )
    .await?;
    Ok(true)
}

pub async fn ensure_and_start_successor_turn(
    db: &LocalDb,
    run_id: &str,
    predecessor_turn_id: &str,
    start_reason: TurnStartReason,
) -> DbResult<Option<SuccessorTurnUpdate>> {
    let run_id = run_id.to_string();
    let predecessor_turn_id = predecessor_turn_id.to_string();
    db.write(|conn| {
        let run_id = run_id.clone();
        let predecessor_turn_id = predecessor_turn_id.clone();
        let start_reason = start_reason.clone();
        Box::pin(async move {
            let Some((job_id, session_id)) = run_turn_context(conn, &run_id).await? else {
                return Ok(None);
            };

            let mut update = ensure_successor_turn(
                conn,
                &session_id,
                &job_id,
                &predecessor_turn_id,
                start_reason,
            )
            .await?;
            update.started = start_turn_for_run(conn, &update.turn_id, &run_id).await?;
            Ok(Some(update))
        })
    })
    .await
}

/// Outcome of [`ensure_wait_resolved_successor`].
pub enum WaitSuccessor {
    /// The wait's own `WaitResolved` successor — either freshly created (pending,
    /// unstarted) or an existing one reused on replay.
    Ready(SuccessorTurnUpdate),
    /// The predecessor already has a DIFFERENT successor from a racing
    /// continuation (predecessor -> successor is 1:1). The wait must not hijack a
    /// foreign turn as its own; the caller decides what to do based on `state` (a
    /// pending foreign turn means the run has NOT resumed yet).
    Collision {
        turn_id: String,
        start_reason: String,
        state: TurnState,
    },
}

/// Resolve the owned wait's `WaitResolved` successor by explicit identity, WITHOUT
/// starting it. A yielded predecessor turn has at most one successor, so:
///
/// - an existing `wait_resolved` successor is the wait's own — reuse it (idempotent
///   replay);
/// - an existing successor with any other `start_reason` is a racing continuation
///   (e.g. a user steer that resumed the run mid-wait) — report a `Collision` so the
///   caller does not adopt it;
/// - no successor yet — create the wait's pending `WaitResolved` turn.
///
/// Matching on `start_reason` (not predecessor identity alone) is what keeps the
/// resolver from hijacking a foreign turn or silently skipping delivery against it
/// (CAIRN-2970).
pub async fn ensure_wait_resolved_successor(
    db: &LocalDb,
    run_id: &str,
    predecessor_turn_id: &str,
) -> DbResult<Option<WaitSuccessor>> {
    let run_id = run_id.to_string();
    let predecessor_turn_id = predecessor_turn_id.to_string();
    db.write(|conn| {
        let run_id = run_id.clone();
        let predecessor_turn_id = predecessor_turn_id.clone();
        Box::pin(async move {
            let Some((job_id, session_id)) = run_turn_context(conn, &run_id).await? else {
                return Ok(None);
            };
            let existing = {
                let mut rows = conn
                    .query(
                        "SELECT id, start_reason, state FROM turns WHERE predecessor_id = ?1 ORDER BY sequence ASC LIMIT 1",
                        params![predecessor_turn_id.clone()],
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => Some((row.text(0)?, row.text(1)?, row.text(2)?)),
                    None => None,
                }
            };
            if let Some((turn_id, start_reason, state)) = existing {
                if start_reason == TurnStartReason::WaitResolved.to_string() {
                    return Ok(Some(WaitSuccessor::Ready(SuccessorTurnUpdate {
                        turn_id,
                        inserted: false,
                        started: false,
                    })));
                }
                let state: TurnState = state.parse().map_err(DbError::Row)?;
                return Ok(Some(WaitSuccessor::Collision {
                    turn_id,
                    start_reason,
                    state,
                }));
            }
            let update = ensure_successor_turn(
                conn,
                &session_id,
                &job_id,
                &predecessor_turn_id,
                TurnStartReason::WaitResolved,
            )
            .await?;
            Ok(Some(WaitSuccessor::Ready(update)))
        })
    })
    .await
}

async fn run_turn_context(
    conn: &cairn_db::turso::Connection,
    run_id: &str,
) -> DbResult<Option<(String, String)>> {
    let mut rows = conn
        .query(
            "SELECT job_id, session_id FROM runs WHERE id = ?1 LIMIT 1",
            params![run_id],
        )
        .await?;
    rows.next()
        .await?
        .map(|row| {
            let job_id = row.opt_text(0)?;
            let session_id = row.opt_text(1)?;
            Ok(job_id.zip(session_id))
        })
        .transpose()
        .map(|value| value.flatten())
}

async fn ensure_successor_turn(
    conn: &cairn_db::turso::Connection,
    session_id: &str,
    job_id: &str,
    predecessor_turn_id: &str,
    start_reason: TurnStartReason,
) -> DbResult<SuccessorTurnUpdate> {
    let mut rows = conn
        .query(
            "
            SELECT id
            FROM turns
            WHERE predecessor_id = ?1
            ORDER BY sequence ASC
            LIMIT 1
            ",
            params![predecessor_turn_id],
        )
        .await?;
    if let Some(row) = rows.next().await? {
        return Ok(SuccessorTurnUpdate {
            turn_id: row.text(0)?,
            inserted: false,
            started: false,
        });
    }

    let predecessor_state = select_turn_state(conn, predecessor_turn_id).await?;
    if !predecessor_state.is_terminal() {
        return Err(DbError::Row(format!(
            "Predecessor turn {} is in non-terminal state {:?}",
            predecessor_turn_id, predecessor_state
        )));
    }

    let active_turn_count = count_active_job_turns(conn, job_id).await?;
    if active_turn_count > 0 {
        return Err(DbError::Row(format!(
            "Job {} already has an active turn (pending or running)",
            job_id
        )));
    }

    let sequence = next_turn_sequence(conn, session_id).await?;
    let turn_id = ids::mint_child(job_id);
    let start_reason = start_reason.to_string();
    let now = chrono::Utc::now().timestamp() as i32;

    conn.execute(
        "
        INSERT INTO turns (
            id, session_id, run_id, job_id, sequence,
            predecessor_id, state, yield_reason, start_reason,
            created_at, started_at, ended_at, updated_at
        )
        VALUES (?1, ?2, NULL, ?3, ?4, ?5, 'pending', NULL, ?6,
                ?7, NULL, NULL, ?7)
        ",
        params![
            turn_id.as_str(),
            session_id,
            job_id,
            sequence,
            predecessor_turn_id,
            start_reason.as_str(),
            now
        ],
    )
    .await?;
    conn.execute(
        "UPDATE jobs SET current_turn_id = ?1 WHERE id = ?2",
        params![turn_id.as_str(), job_id],
    )
    .await?;

    Ok(SuccessorTurnUpdate {
        turn_id,
        inserted: true,
        started: false,
    })
}

async fn select_turn_state(
    conn: &cairn_db::turso::Connection,
    turn_id: &str,
) -> DbResult<TurnState> {
    let mut rows = conn
        .query(
            "SELECT state FROM turns WHERE id = ?1 LIMIT 1",
            params![turn_id],
        )
        .await?;
    let state = rows
        .next()
        .await?
        .ok_or_else(|| DbError::Row(format!("Predecessor turn not found: {}", turn_id)))?
        .text(0)?;
    state.parse().map_err(DbError::Row)
}

async fn count_active_job_turns(conn: &cairn_db::turso::Connection, job_id: &str) -> DbResult<i64> {
    let mut rows = conn
        .query(
            "
            SELECT COUNT(*)
            FROM turns
            WHERE job_id = ?1
              AND state IN ('pending', 'running')
            ",
            params![job_id],
        )
        .await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| DbError::Row("missing active turn count".to_string()))?;
    row.i64(0)
}

async fn next_turn_sequence(conn: &cairn_db::turso::Connection, session_id: &str) -> DbResult<i64> {
    let mut rows = conn
        .query(
            "SELECT MAX(sequence) FROM turns WHERE session_id = ?1",
            params![session_id],
        )
        .await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| DbError::Row("missing turn sequence".to_string()))?;
    Ok(row.opt_i64(0)?.unwrap_or(0) + 1)
}

async fn start_turn_for_run(
    conn: &cairn_db::turso::Connection,
    turn_id: &str,
    run_id: &str,
) -> DbResult<bool> {
    let mut rows = conn
        .query(
            "SELECT state FROM turns WHERE id = ?1 LIMIT 1",
            params![turn_id],
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Err(DbError::Row(format!("Turn not found: {}", turn_id)));
    };
    let state: TurnState = row.text(0)?.parse().map_err(DbError::Row)?;
    if state != TurnState::Pending {
        return Ok(false);
    }

    let now = chrono::Utc::now().timestamp() as i32;
    conn.execute(
        "
        UPDATE turns
        SET state = 'running',
            run_id = ?1,
            started_at = ?2,
            updated_at = ?2
        WHERE id = ?3
        ",
        params![run_id, now, turn_id],
    )
    .await?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{MigrationRunner, SearchIndex, TURSO_MIGRATIONS};
    use std::sync::Arc;

    #[test]
    fn answer_and_operator_transports_map_exhaustively() {
        assert_eq!(
            AppearanceTransport::from(AnswerSurface::ResourcePatch),
            AppearanceTransport::ResourcePatch
        );
        assert_eq!(
            AppearanceTransport::from(AnswerSurface::ChannelReply),
            AppearanceTransport::ChannelReply
        );
        assert_eq!(
            AppearanceTransport::from(AnswerSurface::RemoteIntent),
            AppearanceTransport::RemoteIntent
        );
        assert_eq!(
            AppearanceTransport::from(AnswerSurface::NonOperatorInvoke),
            AppearanceTransport::NonOperatorInvoke
        );
        assert_eq!(
            AppearanceTransport::from(AnswerSurface::LocalInvoke),
            AppearanceTransport::LocalInvoke
        );
        assert_eq!(
            AppearanceTransport::from(OperatorTransport::AuthenticatedOperator),
            AppearanceTransport::AuthenticatedOperator
        );
        assert_eq!(
            AppearanceTransport::from(OperatorTransport::AuthenticatedDesktop),
            AppearanceTransport::AuthenticatedDesktop
        );
    }

    #[test]
    fn operator_approval_rejects_actor_snapshot_mismatch() {
        let approval = desktop_approval();
        let other = PrincipalRef::Machine {
            device_id: "other-device".to_string(),
        };
        assert!(OperatorApproval::authenticated(other, approval.appearance().clone()).is_err());
    }

    async fn test_orchestrator() -> Orchestrator {
        let root = tempfile::tempdir().unwrap().keep();
        let db_path = root.join("test.db");
        let local = LocalDb::open(db_path).await.unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&local)
            .await
            .unwrap();
        let search = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
        let db = Arc::new(DbState::new(Arc::new(local), search));
        Orchestrator::builder(db, Arc::new(TestServicesBuilder::new().build()), root).build()
    }

    async fn seed_allow_all_permission(orch: &Orchestrator) {
        let snapshot = serde_json::json!({
            "recipe": {
                "id": "recipe-1",
                "name": "Recipe",
                "description": null,
                "trigger": "manual",
                "nodes": [],
                "edges": []
            },
            "agents": {
                "agent-1": {
                    "id": "agent-1",
                    "name": "Agent",
                    "description": "Agent",
                    "prompt": "prompt",
                    "tools": [],
                    "disallowedTools": null,
                    "skills": null,
                    "fence": "ask"
                }
            },
            "skills": {},
            "triggerContext": {
                "issueId": "issue-1",
                "projectId": "project-1",
                "triggerType": "manual"
            },
            "delegatedPackets": [],
            "createdAt": 1
        })
        .to_string()
        .replace('\'', "''");
        orch.db
            .local
            .execute_script(format!(
                "
                INSERT INTO workspaces(id, name, created_at, updated_at) VALUES ('ws','Workspace',1,1);
                INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
                 VALUES ('project-1','ws','Project','prj','/tmp/project',1,1);
                INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
                 VALUES ('issue-1','project-1',1,'Issue','active','active','none',1,1);
                INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot)
                 VALUES ('execution-1','recipe-1','issue-1','project-1','running',1,1,'{snapshot}');
                INSERT INTO jobs(
                    id, execution_id, recipe_node_id, issue_id, project_id, status,
                    agent_config_id, node_name, uri_segment, created_at, updated_at
                ) VALUES (
                    'job-1','execution-1','node-1','issue-1','project-1','running',
                    'agent-1','Agent','agent',1,1
                );
                INSERT INTO runs(id, issue_id, project_id, job_id, status, created_at, updated_at)
                 VALUES ('run-1','issue-1','project-1','job-1','running',1,1);
                INSERT INTO permission_requests(
                    id, run_id, tool_use_id, tool_name, tool_input, status, created_at, job_id, uri_segment
                ) VALUES (
                    'perm-request-1','run-1','tool-1','read','{{}}','pending',1,'job-1','perm-1'
                );
                "
            ))
            .await
            .unwrap();
    }

    /// Overwrite the seeded prompt with a real crossing `tool_input`, exactly as
    /// `raise_fence` stores one.
    async fn store_crossing(orch: &Orchestrator, crossing: super::super::fence::Crossing) {
        let tool_input = crossing.stored_tool_input_for_test().replace('\'', "''");
        orch.db
            .local
            .execute_script(format!(
                "UPDATE permission_requests SET tool_input = '{tool_input}' \
                 WHERE id = 'perm-request-1'"
            ))
            .await
            .unwrap();
    }

    async fn perm_status(orch: &Orchestrator) -> Option<String> {
        orch.db
            .local
            .query_opt_text(
                "SELECT status FROM permission_requests WHERE id = 'perm-request-1'",
                (),
            )
            .await
            .unwrap()
    }

    /// The property, asserted from the agent's side rather than at a boundary.
    ///
    /// An agent answers through its own `permissions` resource, which is an
    /// [`AnswerSurface::ResourcePatch`]. It may allow a path-scoped crossing --
    /// that widens one named path with the sandbox still built around the
    /// re-execution. It may not allow a command-scoped one, because there is no
    /// path to widen and the only way to let it proceed is to re-run the
    /// agent-authored command with no sandbox at all, which is the state in
    /// which every path-based protection stops being constructed.
    ///
    /// This is deliberately not a test about the invoke boundary. Closing one
    /// switch there left two others open, because the boundary tests pinned the
    /// gate rather than the property.
    fn desktop_approval() -> OperatorApproval {
        use cairn_common::identity::{
            Address, AppearanceEvidence, CredentialRef, VerificationMethod, VerificationRecord,
            VerificationStatus, VerificationStrength,
        };
        let actor = PrincipalRef::Machine {
            device_id: "test-device".to_string(),
        };
        let verification = VerificationRecord::new(
            VerificationMethod::DesktopCredential,
            VerificationStatus::Verified,
            None,
            None,
            None,
            Some(CredentialRef::new("desktop_operator_credential").unwrap()),
            VerificationStrength::new("local_shared_secret").unwrap(),
            1,
        )
        .unwrap();
        let evidence = AppearanceEvidence::new(
            AppearanceTransport::AuthenticatedDesktop,
            Address::Desktop {
                device_id: "test-device".to_string(),
            },
            verification,
            1,
            None,
        )
        .unwrap();
        let snapshot = AppearanceSnapshot::new(actor.clone(), evidence, Vec::new(), None).unwrap();
        OperatorApproval::authenticated(actor, snapshot).unwrap()
    }

    #[tokio::test]
    async fn migrated_telegram_winner_is_executed_by_a_desktop_retry() {
        let root = tempfile::tempdir().unwrap().keep();
        let local = LocalDb::open(root.join("test.db")).await.unwrap();
        const CANONICALIZE_CHANNEL_ANSWERS: &str = "0190_canonicalize_channel_permission_answers";
        let cut = TURSO_MIGRATIONS
            .iter()
            .position(|migration| migration.name() == CANONICALIZE_CHANNEL_ANSWERS)
            .expect("the canonicalizing migration is registered");
        let canonicalize = TURSO_MIGRATIONS[cut];
        MigrationRunner::new(TURSO_MIGRATIONS[..cut].to_vec())
            .run(&local)
            .await
            .unwrap();
        let search = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
        let db = Arc::new(DbState::new(Arc::new(local), search));
        let orch =
            Orchestrator::builder(db, Arc::new(TestServicesBuilder::new().build()), root).build();
        seed_allow_all_permission(&orch).await;
        crate::channels::ledger::claim_ask_resolution(
            &orch.db.local,
            "perm-request-1",
            "Approve",
            AppearanceTransport::ChannelReply,
            Some("telegram"),
            Some("telegram:8771562567"),
            Some("8771562567"),
            "permission",
            "perm-request-1",
            10,
        )
        .await
        .unwrap();
        assert_eq!(
            MigrationRunner::new(vec![canonicalize])
                .run(&orch.db.local)
                .await
                .unwrap(),
            vec![CANONICALIZE_CHANNEL_ANSWERS.to_string()]
        );
        assert_eq!(
            crate::channels::ledger::resolution_for_action(&orch.db.local, "perm-request-1")
                .await
                .unwrap()
                .unwrap()
                .answer,
            "allow"
        );

        resolve_permission_request(
            &orch,
            "perm-request-1",
            PermissionAnswer::from_operator(PermissionDecision::Allow, desktop_approval()),
        )
        .await
        .expect("a later desktop response must execute the canonical channel winner");

        assert_eq!(perm_status(&orch).await.as_deref(), Some("allowed"));
        assert_eq!(
            orch.db
                .local
                .query_opt_text(
                    "SELECT resolution_provider FROM permission_requests WHERE id = 'perm-request-1'",
                    (),
                )
                .await
                .unwrap()
                .as_deref(),
            Some("telegram")
        );
    }

    #[tokio::test]
    async fn unknown_stored_channel_winner_fails_closed() {
        let orch = test_orchestrator().await;
        seed_allow_all_permission(&orch).await;
        crate::channels::ledger::claim_ask_resolution(
            &orch.db.local,
            "perm-request-1",
            "unknown",
            AppearanceTransport::ChannelReply,
            Some("telegram"),
            Some("telegram:1"),
            Some("1"),
            "permission",
            "perm-request-1",
            10,
        )
        .await
        .unwrap();

        let error = resolve_permission_request(
            &orch,
            "perm-request-1",
            PermissionAnswer::from_operator(PermissionDecision::Allow, desktop_approval()),
        )
        .await
        .expect_err("unknown historical answers must remain invalid");
        assert!(error.contains("invalid stored permission winner answer"));
        assert_eq!(perm_status(&orch).await.as_deref(), Some("pending"));
    }

    #[tokio::test]
    async fn persisted_resource_permission_claim_is_executed_once_by_direct_retry() {
        let orch = test_orchestrator().await;
        seed_allow_all_permission(&orch).await;
        let winner_actor = "cairn://p/PRJ/1/1/builder";
        crate::channels::ledger::claim_ask_resolution(
            &orch.db.local,
            "perm-request-1",
            "allow_session",
            AppearanceTransport::ResourcePatch,
            None,
            None,
            Some(winner_actor),
            "permission",
            "perm-request-1",
            10,
        )
        .await
        .unwrap();

        let retry = || {
            resolve_permission_request(
                &orch,
                "perm-request-1",
                PermissionAnswer::from_surface(
                    PermissionDecision::Deny,
                    AnswerSurface::ResourcePatch,
                )
                .with_actor("cairn://p/PRJ/1/1/retry"),
            )
        };
        let (first, second) = tokio::join!(retry(), retry());
        let outcomes = [first.unwrap(), second.unwrap()];
        assert_eq!(
            outcomes.iter().filter(|outcome| !outcome.duplicate).count(),
            1,
            "only the action-lease winner may execute the permission effect"
        );
        assert_eq!(perm_status(&orch).await.as_deref(), Some("allowed"));
        assert!(
            orch.session_allowed_tools.lock().unwrap().contains("read"),
            "a retry that executes a persisted session winner must install its session grant"
        );
        assert_eq!(
            orch.db
                .local
                .query_opt_text(
                    "SELECT resolution_actor FROM permission_requests WHERE id = 'perm-request-1'",
                    (),
                )
                .await
                .unwrap()
                .as_deref(),
            Some(winner_actor),
            "the persisted winner, not the retrying caller, owns provenance"
        );
        assert_eq!(
            orch.db
                .local
                .query_opt_text(
                    "SELECT CAST(attempt_count AS TEXT) FROM channel_ask_action WHERE action_ref = 'perm-request-1'",
                    (),
                )
                .await
                .unwrap()
                .as_deref(),
            Some("1")
        );
    }

    #[tokio::test]
    async fn persisted_authenticated_claim_accepts_only_matching_fresh_proof() {
        let orch = test_orchestrator().await;
        seed_allow_all_permission(&orch).await;
        let approval = desktop_approval();
        let winner_actor = serde_json::to_string(approval.actor()).unwrap();
        crate::channels::ledger::claim_ask_resolution(
            &orch.db.local,
            "perm-request-1",
            "allow",
            AppearanceTransport::AuthenticatedDesktop,
            None,
            None,
            Some(&winner_actor),
            "permission",
            "perm-request-1",
            10,
        )
        .await
        .unwrap();

        let retry = || {
            resolve_permission_request(
                &orch,
                "perm-request-1",
                PermissionAnswer::from_operator(PermissionDecision::Deny, desktop_approval()),
            )
        };
        let (first, second) = tokio::join!(retry(), retry());
        let outcomes = [first.unwrap(), second.unwrap()];
        assert_eq!(
            outcomes.iter().filter(|outcome| !outcome.duplicate).count(),
            1,
            "only one matching retry may execute the stored winner"
        );
        assert_eq!(
            perm_status(&orch).await.as_deref(),
            Some("allowed"),
            "the persisted decision, not the retry's decision, must execute"
        );
        assert_eq!(
            orch.db
                .local
                .query_opt_text(
                    "SELECT CAST(attempt_count AS TEXT) FROM channel_ask_action WHERE action_ref = 'perm-request-1'",
                    (),
                )
                .await
                .unwrap()
                .as_deref(),
            Some("1")
        );
    }

    #[tokio::test]
    async fn persisted_authenticated_claim_rejects_mismatched_actor_or_transport_before_lease() {
        for (winner_transport, winner_actor) in [
            (
                AppearanceTransport::AuthenticatedDesktop,
                serde_json::to_string(&PrincipalRef::Machine {
                    device_id: "other-device".to_string(),
                })
                .unwrap(),
            ),
            (
                AppearanceTransport::AuthenticatedOperator,
                serde_json::to_string(desktop_approval().actor()).unwrap(),
            ),
        ] {
            let orch = test_orchestrator().await;
            seed_allow_all_permission(&orch).await;
            crate::channels::ledger::claim_ask_resolution(
                &orch.db.local,
                "perm-request-1",
                "allow",
                winner_transport,
                None,
                None,
                Some(&winner_actor),
                "permission",
                "perm-request-1",
                10,
            )
            .await
            .unwrap();

            resolve_permission_request(
                &orch,
                "perm-request-1",
                PermissionAnswer::from_operator(PermissionDecision::Allow, desktop_approval()),
            )
            .await
            .expect_err("mismatched fresh proof must not recover authenticated authority");

            assert_eq!(perm_status(&orch).await.as_deref(), Some("pending"));
            assert_eq!(
                orch.db
                    .local
                    .query_opt_text(
                        "SELECT CAST(attempt_count AS TEXT) FROM channel_ask_action WHERE action_ref = 'perm-request-1'",
                        (),
                    )
                    .await
                    .unwrap()
                    .as_deref(),
                Some("0"),
                "a proof mismatch must fail before leasing or executing the effect"
            );
        }
    }

    fn agent_resource_answer(decision: PermissionDecision) -> PermissionAnswer {
        PermissionAnswer::from_surface(decision, AnswerSurface::ResourcePatch)
            .with_actor("cairn://p/prj/1/1/builder")
    }

    #[tokio::test]
    async fn an_agent_cannot_self_approve_an_unsandboxed_command_escape() {
        for scope in [PermissionScope::Once, PermissionScope::Session] {
            let orch = test_orchestrator().await;
            seed_allow_all_permission(&orch).await;
            store_crossing(
                &orch,
                super::super::fence::Crossing::shell_command(
                    "command blocked by the executor sandbox".to_string(),
                    "cat ~/.cairn/operator_auth_secret",
                ),
            )
            .await;

            let refusal = resolve_permission_request(
                &orch,
                "perm-request-1",
                agent_resource_answer(PermissionDecision::Allow).with_containment_scope(scope),
            )
            .await
            .expect_err("an agent must not be able to lift its own sandbox");
            assert!(
                refusal.contains("no sandbox at all"),
                "unexpected refusal: {refusal}"
            );

            assert_eq!(
                perm_status(&orch).await.as_deref(),
                Some("pending"),
                "a refused escape records nothing and stays answerable"
            );
            assert!(
                orch.session_allowed_crossings.lock().unwrap().is_empty(),
                "a refused escape must not leave a session grant behind"
            );
        }
    }

    /// The operator can still allow it — through the desktop proxy or an
    /// owner/admin JWT, both of which carry the capability.
    #[tokio::test]
    async fn an_operator_can_allow_an_unsandboxed_command_escape() {
        let orch = test_orchestrator().await;
        seed_allow_all_permission(&orch).await;
        store_crossing(
            &orch,
            super::super::fence::Crossing::shell_command(
                "command blocked by the executor sandbox".to_string(),
                "make install",
            ),
        )
        .await;

        let approval = desktop_approval();
        resolve_permission_request(
            &orch,
            "perm-request-1",
            PermissionAnswer::from_operator(PermissionDecision::Allow, approval),
        )
        .await
        .expect("an operator may approve an escape");

        assert_eq!(perm_status(&orch).await.as_deref(), Some("allowed"));
    }

    /// A path-scoped crossing stays self-approvable. Narrowing that too would
    /// park ordinary work at the fence for no gain: the sandbox is still built,
    /// and what is widened is the one path the prompt named.
    #[tokio::test]
    async fn an_agent_can_still_self_approve_a_path_scoped_crossing() {
        let orch = test_orchestrator().await;
        seed_allow_all_permission(&orch).await;
        store_crossing(
            &orch,
            super::super::fence::Crossing::shell_path(
                std::path::Path::new("/tmp/outside.txt"),
                "/tmp/outside.txt",
            ),
        )
        .await;

        resolve_permission_request(
            &orch,
            "perm-request-1",
            agent_resource_answer(PermissionDecision::Allow),
        )
        .await
        .expect("a path-scoped crossing stays answerable by the agent");

        assert_eq!(perm_status(&orch).await.as_deref(), Some("allowed"));
    }

    /// A prompt written before the path/command kinds were split is treated as
    /// an escape rather than as a crossing.
    ///
    /// Pending prompts are durable and nothing expires them, so an agent
    /// suspended on a command crossing when the app is upgraded resumes
    /// afterwards facing a row tagged with the old ambiguous kind. Reading that
    /// as path-scoped would hand it the exact escape this gate exists to stop,
    /// through the exact surface it holds, on the exact prompt it is waiting on.
    #[tokio::test]
    async fn a_legacy_crossing_tag_is_treated_as_an_escape() {
        // The descriptor deliberately begins with `/`: a shape heuristic would
        // read this as a path and fail open on precisely the dangerous case.
        let legacy = serde_json::json!({
            "kind": "shell_escape",
            "verb": "run",
            "descriptor": "/bin/cat /Users/x/.cairn/operator_auth_secret",
            "summary": "command blocked by the executor sandbox",
            "request": {
                "cwd": "/wt", "run_id": "run-1", "tool": "run",
                "tool_use_id": "tool-1", "payload": {}
            }
        })
        .to_string()
        .replace('\'', "''");

        let orch = test_orchestrator().await;
        seed_allow_all_permission(&orch).await;
        orch.db
            .local
            .execute_script(format!(
                "UPDATE permission_requests SET tool_input = '{legacy}' \
                 WHERE id = 'perm-request-1'"
            ))
            .await
            .unwrap();

        resolve_permission_request(
            &orch,
            "perm-request-1",
            agent_resource_answer(PermissionDecision::Allow),
        )
        .await
        .expect_err("a legacy command crossing must not be self-approvable");
        assert_eq!(perm_status(&orch).await.as_deref(), Some("pending"));
    }

    /// Denial stays open on an escape too: recognizing that something should not
    /// happen is not a capability.
    #[tokio::test]
    async fn an_agent_can_still_deny_an_unsandboxed_command_escape() {
        let orch = test_orchestrator().await;
        seed_allow_all_permission(&orch).await;
        store_crossing(
            &orch,
            super::super::fence::Crossing::shell_command(
                "command blocked by the executor sandbox".to_string(),
                "cat /etc/shadow",
            ),
        )
        .await;

        resolve_permission_request(
            &orch,
            "perm-request-1",
            agent_resource_answer(PermissionDecision::Deny),
        )
        .await
        .expect("deny stays open to every surface");

        assert_eq!(perm_status(&orch).await.as_deref(), Some("denied"));
    }

    fn codex_mcp_request_input() -> String {
        serde_json::json!({
            "serverName": "cairn",
            "message": "Allow the cairn MCP server to run tool \"read\"?",
            "mode": "form",
            "requestedSchema": { "type": "object", "properties": {} },
            "_meta": {
                "codex_approval_kind": "mcp_tool_call",
                "persist": ["session", "always"],
                "tool_params": { "path": "/tmp/file.txt" }
            }
        })
        .to_string()
    }

    #[tokio::test]
    async fn allow_all_for_request_sets_requesting_agent_fence_to_allow() {
        let orch = test_orchestrator().await;
        seed_allow_all_permission(&orch).await;

        let approval = desktop_approval();
        allow_all_for_request(&orch, "perm-request-1", &approval)
            .await
            .unwrap();

        let snapshot = orch
            .db
            .local
            .query_text(
                "SELECT snapshot FROM executions WHERE id = 'execution-1'",
                (),
            )
            .await
            .unwrap()
            .unwrap();
        let snapshot = crate::config::snapshot_migrate::load(&snapshot).unwrap();
        assert_eq!(snapshot.agents["agent-1"].fence, Some(Fence::Allow));
    }

    #[test]
    fn codex_command_execution_allow_uses_decision_shape() {
        let response = build_codex_permission_response("codex/command_execution", "{}", "allow");
        assert_eq!(response, serde_json::json!({ "decision": "accept" }));
    }

    #[test]
    fn codex_permissions_allow_uses_permissions_shape() {
        let response = build_codex_permission_response(
            "codex/permissions",
            r#"{"permissions":{"network":{"enabled":true},"fileSystem":{"read":["/tmp"],"write":null}}}"#,
            "allow",
        );
        assert_eq!(
            response,
            serde_json::json!({
                "permissions": {
                    "network": { "enabled": true },
                    "fileSystem": { "read": ["/tmp"], "write": null }
                },
                "scope": "turn"
            })
        );
    }

    #[test]
    fn codex_permissions_deny_grants_no_permissions() {
        let response = build_codex_permission_response(
            "codex/permissions",
            r#"{"permissions":{"network":{"enabled":true}}}"#,
            "deny",
        );
        assert_eq!(
            response,
            serde_json::json!({ "permissions": {}, "scope": "turn" })
        );
    }

    #[test]
    fn codex_mcp_elicitation_allow_uses_action_shape() {
        let response = build_codex_mcp_elicitation_response(&codex_mcp_request_input(), "allow");
        assert_eq!(
            response,
            serde_json::json!({ "action": "accept", "content": null, "_meta": null })
        );
    }

    #[test]
    fn codex_mcp_elicitation_deny_uses_decline_action_shape() {
        let response = build_codex_mcp_elicitation_response(&codex_mcp_request_input(), "deny");
        assert_eq!(
            response,
            serde_json::json!({ "action": "decline", "content": null, "_meta": null })
        );
    }

    #[test]
    fn fence_crossing_tool_input_parses_as_crossing() {
        let detail = serde_json::json!({
            "kind": "sensitive_host_read",
            "verb": "read",
            "descriptor": "/etc/hosts",
            "summary": "read a sensitive denied path: /etc/hosts",
            "request": {
                "cwd": "/scratch/run-1",
                "run_id": "r1",
                "tool": "read",
                "payload": {"path": "file:/etc/hosts"},
                "tool_use_id": "tu1"
            }
        })
        .to_string();
        let parsed = parse_crossing_detail(&detail).expect("fence detail should parse");
        assert_eq!(parsed.verb, "read");
        assert_eq!(parsed.descriptor, "/etc/hosts");
        assert_eq!(parsed.request.tool, "read");
    }

    #[test]
    fn permission_response_does_not_cold_resume_when_inline_waiter_was_present() {
        let resume = PermissionResponseResume {
            run_id: "run-1".to_string(),
            session_id: Some("session-1".to_string()),
            issue_id: None,
            predecessor_turn_id: Some("turn-1".to_string()),
            successor_turn_id: Some("turn-2".to_string()),
            job_id: Some("job-1".to_string()),
            duplicate: false,
        };

        assert!(should_resume_permission_response(None, &resume, false));
        assert!(!should_resume_permission_response(None, &resume, true));
    }

    #[test]
    fn terminal_origin_permission_response_never_resumes_agent_turn() {
        // `shell_command_escape`, matching what this fixture actually is: the
        // descriptor is a normalized command, not a resolved path. It read
        // `shell_escape` before the two kinds were split, which is a shape the
        // system no longer writes.
        let detail = serde_json::json!({
            "kind": "shell_command_escape",
            "verb": "run",
            "descriptor": "ps aux",
            "summary": "command blocked by the worktree sandbox: ps aux",
            "origin": "terminal",
            "request": {
                "cwd": "/scratch/run-1",
                "run_id": "r1",
                "tool": "run",
                "payload": {"commands": [{"command": "ps aux"}]},
                "tool_use_id": "tu1"
            }
        })
        .to_string();
        let crossing = parse_crossing_detail(&detail).expect("terminal crossing should parse");
        let resume = PermissionResponseResume {
            run_id: "run-1".to_string(),
            session_id: Some("session-1".to_string()),
            issue_id: None,
            predecessor_turn_id: Some("turn-1".to_string()),
            successor_turn_id: Some("turn-2".to_string()),
            job_id: Some("job-1".to_string()),
            duplicate: false,
        };

        assert!(!should_resume_permission_response(
            Some(&crossing),
            &resume,
            false
        ));
    }

    #[test]
    fn grant_error_rides_on_the_response_without_rewriting_the_answer() {
        let augmented = with_grant_error(r#"{"behavior":"allow"}"#, "disk full");
        let value: serde_json::Value = serde_json::from_str(&augmented).unwrap();
        // The recorded decision stays truthful; only the failure is added.
        assert_eq!(value["behavior"], "allow");
        assert_eq!(value[GRANT_ERROR_KEY], "disk full");
    }

    #[test]
    fn grant_error_survives_a_response_that_is_not_json() {
        let augmented = with_grant_error("not json at all", "disk full");
        let value: serde_json::Value = serde_json::from_str(&augmented).unwrap();
        assert_eq!(value[GRANT_ERROR_KEY], "disk full");
    }

    #[test]
    fn legacy_tool_input_does_not_parse_as_crossing() {
        // A plain tool prompt payload lacks verb/descriptor/request.
        assert!(parse_crossing_detail(r#"{"path":"/tmp/x"}"#).is_none());
        assert!(
            parse_crossing_detail(r#"{"_meta":{"codex_approval_kind":"mcp_tool_call"}}"#).is_none()
        );
    }
}
