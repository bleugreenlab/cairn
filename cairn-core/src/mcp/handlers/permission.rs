//! Permission request handling for the worktree fence and Codex prompts.
//!
//! A pending request is stored in `permission_requests`, an event is emitted for
//! the frontend, and the run waits (blocking) for the user to approve or deny via
//! the Orchestrator's `permission_responses` broadcast channel. The worktree
//! fence routes its crossings through [`await_permission_decision`]; an allow
//! re-executes the originating verb with the grant in place. Codex tool prompts
//! resolve through the same store/respond machinery via
//! [`resolve_permission_request`].

use crate::mcp::types::McpCallbackRequest;
use crate::models::{Fence, TurnStartReason, TurnState, TurnYieldReason};
use crate::orchestrator::Orchestrator;
use crate::storage::{DbError, DbResult, LocalDb, RowExt};
use turso::params;

use super::{emit_attention, AttentionEvent};

/// Inline budget the permission handlers wait synchronously before durably
/// suspending the run. Mirrors `planning::INLINE_PROMPT_WAIT_BUDGET`: long
/// enough that a human or manager answering promptly keeps the CLI process
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
    let request_id = uuid::Uuid::new_v4().to_string();
    let now = chrono::Utc::now().timestamp() as i32;
    let current_turn_id = orch.process_state.get_current_turn_id(run_id);
    let tool_input_json = serde_json::to_string(tool_input).unwrap_or_default();

    let (yielded_turn, perm_segment) = match orch
        .db
        .local
        .write(|conn| {
            let request_id = request_id.clone();
            let run_id = run_id.to_string();
            let tool_use_id = tool_use_id.to_string();
            let tool_name = tool_name.to_string();
            let tool_input_json = tool_input_json.clone();
            let current_turn_id = current_turn_id.clone();
            Box::pin(async move {
                // Owning node job for this run; None for project-chat runs.
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
                    Ok(Some(issue_id)) => {
                        if let Err(e) = crate::transitions::outcome::recompute_issue_status_conn(
                            conn, &issue_id,
                        )
                        .await
                        {
                            log::warn!("Failed to recompute issue status {}: {}", issue_id, e);
                        }
                    }
                    Ok(None) => {}
                    Err(e) => log::warn!("Failed to look up issue for run {}: {}", run_id, e),
                }

                Ok((yielded_turn, uri_segment))
            })
        })
        .await
    {
        Ok(pair) => pair,
        Err(e) => {
            return PermissionWait::Decided(deny_response(&format!(
                "Failed to store request: {}",
                e
            )))
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
        let _ = services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "turns", "action": "update"}),
        );
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
                        emit_successor_turn_events(&*services.emitter, &successor);
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
            let _ = crate::orchestrator::lifecycle::suspend_run_for_durable_wait(
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
            let _ = crate::orchestrator::lifecycle::suspend_run_for_durable_wait(
                orch,
                run_id,
                "permission_wait_suspended",
            );
            PermissionWait::Suspended
        }
    }
}

/// Create a permission request for background work (agent terminals) without
/// yielding or suspending the current turn. The returned request id is answered
/// through the normal permission response broadcast.
pub(crate) async fn create_background_permission_request(
    orch: &Orchestrator,
    run_id: &str,
    tool_use_id: &str,
    tool_name: &str,
    tool_input: &serde_json::Value,
) -> Result<String, String> {
    let request_id = uuid::Uuid::new_v4().to_string();
    let now = chrono::Utc::now().timestamp() as i32;
    let tool_input_json = serde_json::to_string(tool_input).unwrap_or_default();

    let perm_segment = orch
        .db
        .local
        .write(|conn| {
            let request_id = request_id.clone();
            let run_id = run_id.to_string();
            let tool_use_id = tool_use_id.to_string();
            let tool_name = tool_name.to_string();
            let tool_input_json = tool_input_json.clone();
            Box::pin(async move {
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
                    VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'pending', ?7, NULL, ?8)
                    ",
                    params![
                        request_id.as_str(),
                        run_id.as_str(),
                        job_id.as_deref(),
                        tool_use_id.as_str(),
                        tool_name.as_str(),
                        tool_input_json.as_str(),
                        now,
                        uri_segment.as_deref()
                    ],
                )
                .await?;

                Ok(uri_segment)
            })
        })
        .await
        .map_err(|e| format!("Failed to store request: {e}"))?;

    let _ = orch.services.emitter.emit(
        "permission-request",
        serde_json::json!({
            "requestId": request_id,
            "runId": run_id,
            "toolUseId": tool_use_id,
            "toolName": tool_name,
            "input": tool_input,
        }),
    );
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "permission_requests", "action": "insert"}),
    );

    emit_permission_attention(
        orch,
        run_id,
        tool_name,
        tool_use_id,
        tool_input,
        perm_segment.as_deref(),
    )
    .await;

    Ok(request_id)
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
    let issue_title = match ctx.issue_id.as_deref() {
        Some(issue_id) => get_issue_title(&orch.db.local, issue_id).await,
        None => None,
    };
    emit_attention(
        &*services.emitter,
        &AttentionEvent {
            attention_type: "permission",
            project_key: &ctx.project_key,
            issue_number: ctx.issue_number,
            issue_title: issue_title.as_deref(),
            node_name: ctx.job_name.as_deref(),
            exec_seq: ctx.exec_seq,
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
                &format!("permission:{issue_uri}"),
            )
            .await
            {
                // CAIRN-1889: actively resume each idle watcher of the permission
                // via the shared resume-ladder primitive (idle -> resume; busy or
                // self-suspended -> no-op).
                Ok(recipients) => {
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
/// [`Fence::default`] when the field is `None`. Returns `None` when the run has
/// no execution snapshot (e.g. project chat) — in which case no fence applies,
/// preserving the unconfined default.
pub(crate) async fn resolve_fence_policy(
    orch: &Orchestrator,
    run_id: Option<&str>,
) -> Option<Fence> {
    let run_id = run_id?.to_string();

    let (agent_config_id, execution_id) = orch
        .db
        .local
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

    let snapshot_json = orch
        .db
        .local
        .query_opt_text(
            "SELECT snapshot FROM executions WHERE id = ?1 LIMIT 1",
            params![execution_id.as_str()],
        )
        .await
        .ok()
        .flatten()?;

    let snapshot = crate::models::ExecutionSnapshot::from_json(&snapshot_json).ok()?;
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

/// Whether an allow applies once or for the rest of the session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermissionScope {
    Once,
    Session,
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
    fn from_row(row: &turso::Row) -> DbResult<Self> {
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
    /// Crossing classification (read/write/shell). Unused at resolution time but
    /// required so a legacy `tool_input` never parses as a crossing by accident.
    #[allow(dead_code)]
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
/// tool prompt (whose `tool_input` lacks the verb/descriptor/request shape).
fn parse_crossing_detail(tool_input: &str) -> Option<CrossingDetail> {
    serde_json::from_str::<CrossingDetail>(tool_input).ok()
}

impl CrossingDetail {
    fn is_terminal_origin(&self) -> bool {
        self.origin.as_deref() == Some("terminal")
    }
}

#[derive(Debug)]
struct PermissionResponseResume {
    run_id: String,
    session_id: Option<String>,
    issue_id: Option<String>,
    predecessor_turn_id: Option<String>,
    successor_turn_id: Option<String>,
    job_id: Option<String>,
    duplicate: bool,
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
    decision: PermissionDecision,
    scope: PermissionScope,
) -> Result<ResolveOutcome, String> {
    let now = chrono::Utc::now().timestamp() as i32;
    let record = get_permission_request_record(&orch.db.local, request_id)
        .await
        .map_err(|e| format!("Permission request not found: {}", e))?;

    let crossing = parse_crossing_detail(&record.tool_input);
    let status = match decision {
        PermissionDecision::Allow => "allowed",
        PermissionDecision::Deny => "denied",
    };
    let behavior = match (decision, scope) {
        (PermissionDecision::Deny, _) => "deny",
        (PermissionDecision::Allow, PermissionScope::Once) => "allow",
        (PermissionDecision::Allow, PermissionScope::Session) => "allow",
    };

    let response_json = build_permission_response_json(&record, behavior, crossing.as_ref());

    let resume =
        record_permission_response(&orch.db.local, request_id, status, &response_json, now)
            .await
            .map_err(|e| e.to_string())?;

    // Session grant bookkeeping (only on allow + session).
    if matches!(decision, PermissionDecision::Allow) && matches!(scope, PermissionScope::Session) {
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
        if let Err(e) = recompute_issue_status_for_issue(&orch.db.local, issue_id).await {
            log::warn!("Failed to recompute issue status {}: {}", issue_id, e);
        }
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
    if !resume.duplicate {
        let _ = orch
            .permission_responses
            .send((request_id.to_string(), response_json.clone()));
    }

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "permission_requests", "action": "update"}),
    );
    if resume.successor_turn_id.is_some() {
        let _ = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "turns", "action": "update"}),
        );
    }

    // Slow path: no inline waiter was present before the answer was broadcast,
    // so the run durably suspended and must be resumed from the stored response.
    let should_resume =
        should_resume_permission_response(crossing.as_ref(), &resume, inline_waiter_was_present);
    if should_resume {
        if let Err(e) = resume_suspended_permission(
            orch,
            &record,
            &resume,
            &response_json,
            crossing.as_ref(),
            decision,
            scope,
        )
        .await
        {
            log::warn!("Failed to resume after permission {}: {}", request_id, e);
        }
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
    decision: PermissionDecision,
    scope: PermissionScope,
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
    resolve_permission_request(orch, &request_id, decision, scope).await
}

async fn lookup_permission_request_for_node(
    orch: &Orchestrator,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_segment: &str,
    perm_segment: &str,
) -> Result<String, String> {
    let project_key = project_key.to_string();
    let node_segment = node_segment.to_string();
    let perm_segment = perm_segment.to_string();
    orch.db
        .local
        .read(|conn| {
            let project_key = project_key.clone();
            let node_segment = node_segment.clone();
            let perm_segment = perm_segment.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "
                        SELECT pr.id
                        FROM permission_requests pr
                        JOIN runs r ON pr.run_id = r.id
                        JOIN jobs j ON COALESCE(pr.job_id, r.job_id) = j.id
                        JOIN executions e ON j.execution_id = e.id
                        JOIN issues i ON j.issue_id = i.id
                        JOIN projects p ON i.project_id = p.id
                        WHERE p.key = ?1
                          AND i.number = ?2
                          AND e.seq = ?3
                          AND j.uri_segment = ?4
                          AND pr.uri_segment = ?5
                        LIMIT 1
                        ",
                        params![
                            project_key.as_str(),
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
                            "permission {perm_segment} not found for {project_key}-{number}/{exec_seq}/{node_segment}"
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
    crossing: Option<&CrossingDetail>,
) -> String {
    if crossing.is_some() {
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
    record: &PermissionRequestRecord,
    resume: &PermissionResponseResume,
    response_json: &str,
    crossing: Option<&CrossingDetail>,
    decision: PermissionDecision,
    scope: PermissionScope,
) -> Result<(), String> {
    let Some(session_id) = resume.session_id.as_deref() else {
        return Ok(());
    };
    let Some(job_id) = resume.job_id.as_deref() else {
        return Ok(());
    };
    let now = chrono::Utc::now().timestamp() as i32;

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
                if has_pending_permission_request(&orch.db.local, &resume.run_id).await {
                    return Ok(());
                }
                (result, false)
            }
        },
        // Legacy/Codex prompt: the decision JSON is the prompt call's result.
        None => (response_json.to_string(), false),
    };

    crate::execution::jobs::store_tool_result_event_with_turn(
        orch,
        &resume.run_id,
        session_id,
        &record.tool_use_id,
        &content,
        is_error,
        now,
        resume.predecessor_turn_id.as_deref(),
    )?;

    let prompt_resume = crate::execution::jobs::ResumeContext {
        suppress_user_event: true,
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
/// resolve a fenced `write` crossing, which re-enters `handle_change` here — an
/// indirectly recursive async fn that must be heap-allocated.
async fn re_dispatch_verb(orch: &Orchestrator, detail: &CrossingDetail) -> String {
    match detail.verb.as_str() {
        "read" => {
            Box::pin(crate::mcp::handlers::files::handle_read_file(
                orch,
                &detail.request,
            ))
            .await
        }
        // A fenced target inside a batch suspends the whole `read_batch` call; on
        // allow we re-run the batch (idempotent reads) so the approved target now
        // resolves alongside the rest.
        "read_batch" => {
            let read_cursors = std::sync::Mutex::new(std::collections::HashMap::new());
            Box::pin(crate::mcp::handlers::read::handle_read_batch(
                orch,
                &detail.request,
                &read_cursors,
            ))
            .await
        }
        // `write` is the current verb; `change` is the legacy name a crossing
        // suspended before the rename may still carry.
        "write" | "change" => {
            Box::pin(crate::mcp::handlers::files::handle_change(
                orch,
                &detail.request,
            ))
            .await
        }
        "run" => {
            Box::pin(crate::mcp::handlers::bash::handle_run(
                orch,
                &detail.request,
            ))
            .await
        }
        other => format!(
            "Cannot re-execute unknown verb '{}' after fence allow",
            other
        ),
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

async fn record_permission_response(
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
                let predecessor_turn_id = row.opt_text(2)?;
                let session_id = row.opt_text(3)?;
                let job_id = row.opt_text(4)?;
                let current_status = row.text(5)?;

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

pub(super) fn emit_successor_turn_events(
    emitter: &dyn crate::services::EventEmitter,
    update: &SuccessorTurnUpdate,
) {
    if update.inserted {
        let _ = emitter.emit(
            "db-change",
            serde_json::json!({"table": "turns", "action": "insert"}),
        );
    }
    if update.started {
        let _ = emitter.emit(
            "db-change",
            serde_json::json!({"table": "turns", "action": "update"}),
        );
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

async fn issue_id_for_run_conn(conn: &turso::Connection, run_id: &str) -> DbResult<Option<String>> {
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
    conn: &turso::Connection,
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

async fn run_turn_context(
    conn: &turso::Connection,
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
    conn: &turso::Connection,
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
    let turn_id = uuid::Uuid::new_v4().to_string();
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

async fn select_turn_state(conn: &turso::Connection, turn_id: &str) -> DbResult<TurnState> {
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

async fn count_active_job_turns(conn: &turso::Connection, job_id: &str) -> DbResult<i64> {
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

async fn next_turn_sequence(conn: &turso::Connection, session_id: &str) -> DbResult<i64> {
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
    conn: &turso::Connection,
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
            "kind": "read_outside_worktree",
            "verb": "read",
            "descriptor": "/etc/hosts",
            "summary": "read a file outside the worktree: /etc/hosts",
            "request": {
                "cwd": "/wt",
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
        let detail = serde_json::json!({
            "kind": "shell_escape",
            "verb": "run",
            "descriptor": "ps aux",
            "summary": "command blocked by the worktree sandbox: ps aux",
            "origin": "terminal",
            "request": {
                "cwd": "/wt",
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
    fn legacy_tool_input_does_not_parse_as_crossing() {
        // A plain tool prompt payload lacks verb/descriptor/request.
        assert!(parse_crossing_detail(r#"{"path":"/tmp/x"}"#).is_none());
        assert!(
            parse_crossing_detail(r#"{"_meta":{"codex_approval_kind":"mcp_tool_call"}}"#).is_none()
        );
    }
}
