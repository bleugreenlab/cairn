//! Implementation-related MCP handlers.
//!
//! Handles: add_comment, and artifact writes/patches submitted through the
//! `write` verb (the replacement for the deleted dynamic `return` tool).

use crate::mcp::types::{AddCommentPayload, McpCallbackRequest};
use crate::models::ConfirmPolicy;
use crate::orchestrator::Orchestrator;
use crate::storage::{DbError, DbResult, RowExt};
use cairn_common::ids;
use cairn_db::turso::params;

// ============================================================================
// Handlers
// ============================================================================

pub async fn append_issue_comment(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    project_key: &str,
    issue_number: i32,
    content: &str,
) -> Result<String, String> {
    append_issue_comment_canonical(
        orch,
        request,
        project_key,
        issue_number,
        content,
        "agent",
        None,
    )
    .await
}

pub async fn append_issue_comment_for_remote_intent(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    project_key: &str,
    issue_number: i32,
    content: &str,
    intent_id: &str,
) -> Result<String, String> {
    append_issue_comment_canonical(
        orch,
        request,
        project_key,
        issue_number,
        content,
        "user",
        Some(intent_id),
    )
    .await
}

async fn append_issue_comment_canonical(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    project_key: &str,
    issue_number: i32,
    content: &str,
    source: &str,
    intent_id: Option<&str>,
) -> Result<String, String> {
    let services = &orch.services;
    let project_key_upper = project_key.to_uppercase();
    let (_comment_id, issue_id, _now) = append_issue_comment_db(
        orch,
        &project_key_upper,
        issue_number,
        content.to_string(),
        source,
        intent_id,
    )
    .await?;

    let _ = services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "comments", "action": "insert"}),
    );

    let exclude_job_id = super::run_context::lookup_run(&orch.db.local, request)
        .await
        .ok()
        .map(|ctx| ctx.job_id);
    if let Err(error) =
        crate::messages::side_channel::record_issue_comment_side_channel_for_intent_async(
            orch,
            &issue_id,
            source,
            content,
            exclude_job_id.as_deref(),
            intent_id,
        )
        .await
    {
        if intent_id.is_some() {
            return Err(format!(
                "Failed to record issue comment side-channel notices: {error}"
            ));
        }
        log::warn!("Failed to record issue comment side-channel notices: {error}");
    }

    Ok(format!(
        "Appended comment to issue {}-{}",
        project_key_upper, issue_number
    ))
}

/// Handle add_comment tool call - adds a comment to the issue associated with this run
pub async fn handle_add_comment(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let payload: AddCommentPayload = match super::parse_payload(request) {
        Ok(payload) => payload,
        Err(error) => return error,
    };

    log::info!(
        "add_comment for cwd={}, {} chars",
        request.cwd,
        payload.content.len()
    );

    let ctx = match super::run_context::lookup_run(&orch.db.local, request).await {
        Ok(ctx) => ctx,
        Err(e) => return e,
    };
    let issue_number = match ctx.issue_number {
        Some(number) => number,
        None => {
            return "Cannot add comment: project-level jobs don't have an associated issue"
                .to_string();
        }
    };
    let project_key = ctx.project_key.clone();
    match append_issue_comment(orch, request, &project_key, issue_number, &payload.content).await {
        Ok(result) => result,
        Err(error) => error,
    }
}

/// Validate `payload` against a resolved JSON Schema, returning a descriptive
/// error naming the failing fields. Now that the artifact schema is no longer a
/// visible tool input, this is where the agent gets corrective feedback for a
/// malformed write.
pub(crate) fn validate_against_schema(
    schema: &serde_json::Value,
    payload: &serde_json::Value,
) -> Result<(), String> {
    let validator =
        jsonschema::validator_for(schema).map_err(|e| format!("Invalid artifact schema: {e}"))?;
    let errors: Vec<String> = validator
        .iter_errors(payload)
        .map(|e| {
            let path = e.instance_path.to_string();
            if path.is_empty() {
                e.to_string()
            } else {
                format!("{e} (at {path})")
            }
        })
        .collect();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "Artifact does not match its schema:\n- {}",
            errors.join("\n- ")
        ))
    }
}

/// Resolve the artifact schema *shape* for a node (its own schema, else inherited
/// from a downstream context-edge target). `None` means no schema to validate
/// against.
async fn resolve_artifact_schema(
    orch: &Orchestrator,
    node_id: &str,
    execution_id: &str,
) -> Option<crate::models::OutputSchemaInfo> {
    let node_id = node_id.to_string();
    let execution_id = execution_id.to_string();
    let Ok(db) = crate::execution::routing::owning_db_for_execution(&orch.db, &execution_id).await
    else {
        return None;
    };
    db.read(|conn| {
        let node_id = node_id.clone();
        let execution_id = execution_id.clone();
        Box::pin(async move {
            crate::execution::jobs::find_downstream_artifact_schema_conn(
                conn,
                &node_id,
                &execution_id,
            )
            .await
        })
    })
    .await
    .ok()
    .flatten()
}

/// Enumerate the node's `context-self` living-doc targets (name + schema) so a
/// ctx-self write can be routed to its own schema. Empty for non-recipe jobs.
async fn resolve_ctx_self_schemas(
    orch: &Orchestrator,
    node_id: &str,
    execution_id: &str,
) -> Vec<crate::models::OutputSchemaInfo> {
    let node_id = node_id.to_string();
    let execution_id = execution_id.to_string();
    let Ok(db) = crate::execution::routing::owning_db_for_execution(&orch.db, &execution_id).await
    else {
        return Vec::new();
    };
    db.read(|conn| {
        let node_id = node_id.clone();
        let execution_id = execution_id.clone();
        Box::pin(async move {
            crate::execution::jobs::resolve_ctx_self_schemas_conn(conn, &node_id, &execution_id)
                .await
        })
    })
    .await
    .unwrap_or_default()
}

/// The resolved artifact contract for an addressed name: which schema a write
/// validates against, plus the terminal-interrupt and confirm inputs. Resolving
/// this once keeps the read-side affordance (which schema the agent is told to
/// write) and the write-side validation (which schema is enforced) from drifting.
pub(crate) struct ResolvedArtifactContract {
    /// Confirm policy governing whether the written artifact auto-confirms.
    pub confirm_policy: ConfirmPolicy,
    /// Schema the addressed write validates against (`None` = no schema).
    pub validation_schema: Option<crate::models::OutputSchema>,
    /// Whether the addressed name is a `context-self` living doc.
    pub is_ctx_self: bool,
    /// Canonical name of the terminal (context-out) artifact, if any.
    pub terminal_name: Option<String>,
    /// Whether the terminal contract carries a schema.
    pub terminal_has_schema: bool,
}

/// Resolve the terminal (context-out) contract and the ctx-self living-doc
/// targets for a job, then route by the addressed artifact name.
///
/// The terminal contract is the schema of the node's single `context-out` edge
/// target (an ArtifactNode or a `pr`/action input port). It drives the confirm
/// policy, the arming decision, and the job-completion gate. A `pr` consumer
/// auto-confirms — the PR lifecycle is the gate (CAIRN-1219). Task jobs have no
/// recipe node: they validate against the `return` contract every child task is
/// started with.
///
/// A `context-self` living doc validates against its OWN schema, always
/// auto-confirms, and NEVER arms the terminal interrupt or satisfies the output
/// contract (repeated create+patch across the run is normal). Anything else
/// takes the terminal contract.
/// Load and parse a node-less job's persisted `DelegatedOutputContract`
/// (CAIRN-2481). `None` when the column is NULL (a child task) or the job is
/// absent, so the caller falls through to the fixed `return` contract.
async fn load_job_output_contract(
    orch: &Orchestrator,
    job_id: &str,
) -> Option<crate::models::DelegatedOutputContract> {
    let db = crate::execution::routing::owning_db_for_job(&orch.db, job_id)
        .await
        .ok()?;
    let json = db
        .query_opt_text(
            "SELECT output_contract FROM jobs WHERE id = ?1",
            (job_id.to_string(),),
        )
        .await
        .ok()
        .flatten()?;
    serde_json::from_str(&json).ok()
}

pub(crate) async fn resolve_artifact_contract(
    orch: &Orchestrator,
    job_id: &str,
    task_name: Option<&str>,
    artifact_name: Option<&str>,
) -> ResolvedArtifactContract {
    let (node_id, execution_id) = job_node_execution(orch, job_id)
        .await
        .ok()
        .flatten()
        .unwrap_or_default();

    let mut terminal_policy = ConfirmPolicy::Auto;
    let mut terminal_schema: Option<crate::models::OutputSchema> = None;
    let mut terminal_name: Option<String> = None;
    let mut self_targets: Vec<crate::models::OutputSchemaInfo> = Vec::new();
    if let (Some(node_id), Some(execution_id)) = (node_id.as_deref(), execution_id.as_deref()) {
        if let Some(info) = resolve_artifact_schema(orch, node_id, execution_id).await {
            terminal_policy = info.confirm_policy;
            terminal_name = info.artifact_name;
            terminal_schema = Some(info.schema);
        }
        self_targets = resolve_ctx_self_schemas(orch, node_id, execution_id).await;
    } else if let Some(contract) = load_job_output_contract(orch, job_id).await {
        // A node-less run (an ephemeral call) carries its resolved output
        // contract on the job row, so prompt and validation read one source
        // (CAIRN-2481). This takes precedence over the fixed `return` fallback.
        terminal_name = Some(contract.artifact_name());
        terminal_schema = Some(contract.schema_type);
    } else if task_name.is_some() {
        terminal_name = Some("result".to_string());
        terminal_schema = Some(crate::models::OutputSchema::Preset("return".to_string()));
    }

    let ctx_self_target = artifact_name.and_then(|name| {
        self_targets
            .iter()
            .find(|t| t.artifact_name.as_deref() == Some(name))
    });
    let is_ctx_self = ctx_self_target.is_some();
    let terminal_has_schema = terminal_schema.is_some();
    let (confirm_policy, validation_schema) = match ctx_self_target {
        Some(target) => (ConfirmPolicy::Auto, Some(target.schema.clone())),
        None => (terminal_policy, terminal_schema),
    };

    ResolvedArtifactContract {
        confirm_policy,
        validation_schema,
        is_ctx_self,
        terminal_name,
        terminal_has_schema,
    }
}

/// Store (or patch) a node/task artifact submitted through the `write` verb.
///
/// ## Lifecycle design
///
/// This replaces the dynamic `return` tool. The schema is resolved and validated
/// server-side; `confirmed` is set from the producing node's declared
/// `confirm_policy` (`auto` -> confirmed, `user` -> unconfirmed); and the job is
/// recomputed so its status re-derives (Complete, or Blocked under `user`
/// policy). A fresh `create` of the declared output artifact arms the producing
/// run for a boundary interrupt after the tool result reaches native history.
///
/// `is_patch` merges the payload over the latest artifact (validating the full
/// merged object); `create` stores the payload as-is.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn write_artifact_change(
    orch: &Orchestrator,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    task_name: Option<&str>,
    artifact_name: Option<&str>,
    payload: &serde_json::Value,
    is_patch: bool,
) -> Result<String, String> {
    let services = &orch.services;
    // Resolve the project's owning database (a team project's jobs/artifacts live
    // wholly in its synced replica). The agent writing the artifact runs on the
    // member's machine where that replica is open; if it is not, the job lookup
    // below finds no row and the write errors rather than landing in private.
    let db = orch.db.for_project(project_key).await;

    // 1. Resolve the owning job from the URI coordinates.
    let job_id = crate::resources::resolve_todos_job_id(
        &db,
        project_key,
        number,
        exec_seq,
        node_name,
        task_name,
    )
    .await?;

    // 2. Resolve the terminal (context-out) contract and the ctx-self living-doc
    //    targets from the execution snapshot, then route the write by the
    //    addressed name.
    let contract = resolve_artifact_contract(orch, &job_id, task_name, artifact_name).await;
    let policy = contract.confirm_policy;
    let validation_schema = contract.validation_schema;

    // Only a fresh create of the terminal (context-out) artifact arms the boundary
    // interrupt; a ctx-self write never does.
    let should_arm_terminal_interrupt = !contract.is_ctx_self
        && should_arm_output_artifact_interrupt(
            is_patch,
            artifact_name,
            contract.terminal_name.as_deref(),
            contract.terminal_has_schema,
        );

    // A `patch` resolves against the latest artifact's data; a `create`
    // replaces. Validation and storage operate on the full resulting object so a
    // partial patch (e.g. `{content}` against a plan) can't drop required fields
    // like `title`. File-style text replacement payloads are operations over a
    // prose field, not metadata to store on the artifact.
    // The addressed name (`cairn:~/<name>`) is the artifact's identity and the
    // key of its version chain. The resolved contract type drives the
    // `artifact_type` column and is the fallback identity when the caller used
    // the generic `/artifact` alias (`artifact_name` is `None`). Resolving once
    // keeps the patch-base load and the store on the same `(job_id, output_name)`
    // chain. For a single-artifact node the addressed name equals the resolved
    // type, so identity is unchanged.
    let (artifact_type, output_name) =
        resolve_artifact_identity(orch, &job_id, artifact_name).await;

    let latest = load_latest_artifact(orch, &job_id, &output_name).await;
    let prior_confirmed = latest.as_ref().map(|(_, confirmed)| *confirmed);

    let effective_payload = match (is_patch, &latest) {
        (true, Some((base, _))) => apply_artifact_patch(base.clone(), payload)?,
        (true, None) if is_text_replacement_patch(payload) => {
            return Err(
                "Artifact text replacement patch requires an existing artifact to edit".to_string(),
            );
        }
        // create, or field-merge patch with no prior artifact — the payload stands alone.
        _ => payload.clone(),
    };

    if let Some(schema) = &validation_schema {
        let schema_value =
            crate::output_schemas::resolve_output_schema(orch.schema_dir.as_deref(), schema)
                .map_err(|e| format!("Failed to resolve artifact schema: {e}"))?;
        validate_against_schema(&schema_value, &effective_payload)?;
    }

    // 3. Confirmation is a one-shot job-progress trigger, not a per-version gate.
    //    A fresh create sets it from the node's policy (auto -> confirmed, user
    //    -> awaits the human). A patch is a pure edit: it never re-arms the gate.
    //    Once any version is confirmed, later writes keep it confirmed; a patch
    //    before confirmation inherits the still-unconfirmed state (so the
    //    request-changes revision stays Blocked until the human confirms).
    let confirmed = resolve_confirmed(is_patch, prior_confirmed, policy);

    // Before storing a create-pr artifact version: sweep any unsealed work onto the
    // branch, refuse an empty-delta (+0/-0) PR, and strictly publish the current
    // bookmark. Keyed on the written artifact's own name/type, the same signal
    // `create_pr_artifact_details` syncs on, so a ctx-self write under a create-pr
    // terminal never triggers it. The reserved `action:` merge/close/refresh paths
    // return earlier and never reach here.
    let is_create_pr_write = artifact_name == Some("create-pr")
        || output_name.as_str() == "create-pr"
        || artifact_type.as_str() == "create-pr";
    if is_create_pr_write {
        let job = crate::jobs::queries::get_job(&db, &job_id)
            .await
            .map_err(|e| {
                format!(
                    "create-pr artifact was not written because job `{job_id}` could not be \
                     loaded for bookmark publication: {e}"
                )
            })?;
        let (worktree, branch) = require_create_pr_branch_metadata(
            &job_id,
            job.worktree_path.as_deref(),
            job.branch.as_deref(),
        )?;
        sweep_gate_and_publish_create_pr_branch(
            orch,
            &job.project_id,
            &job_id,
            worktree,
            branch,
            job.base_branch.as_deref(),
        )
        .await?;
    }

    let data_json = serde_json::to_string(&effective_payload)
        .map_err(|e| format!("Failed to serialize artifact: {e}"))?;
    let stored = store_artifact(
        orch,
        &job_id,
        &artifact_type,
        &output_name,
        data_json,
        confirmed,
    )
    .await?;

    let synced_pr_metadata = if let Some((title, body)) = create_pr_artifact_details(
        &effective_payload,
        artifact_name,
        stored.output_name.as_deref(),
        &stored.artifact_type,
    ) {
        crate::pr_data::actions::sync_create_pr_artifact_for_job(
            orch,
            &job_id,
            &title,
            body.as_deref(),
        )
        .await?
    } else {
        false
    };

    log::info!(
        "Artifact written via change: id={}, job_id={}, type={}, version={}, confirmed={}, synced_pr_metadata={}",
        stored.artifact_id,
        job_id,
        stored.artifact_type,
        stored.version,
        confirmed,
        synced_pr_metadata
    );

    let artifact = crate::models::Artifact {
        id: stored.artifact_id.clone(),
        job_id: Some(job_id.clone()),
        artifact_type: stored.artifact_type.clone(),
        schema_version: 1,
        data: effective_payload.clone(),
        version: stored.version,
        parent_version_id: stored.parent_version_id.clone(),
        output_name: stored.output_name.clone(),
        created_at: stored.created_at as i64,
        updated_at: stored.updated_at as i64,
        seen_at: None,
        confirmed,
    };
    orch.notifier.artifact(&artifact);

    let _ = services.emitter.emit(
        "artifact-submitted",
        serde_json::json!({
            "artifact_id": stored.artifact_id,
            "job_id": job_id,
            "artifact_type": stored.artifact_type,
            "version": stored.version,
        }),
    );

    // Build the canonical artifact URI and embed its prose for corpus recall.
    let artifact_uri = if let Some(task_name) = task_name {
        cairn_common::uri::build_task_artifact_uri_named(
            project_key,
            number,
            exec_seq,
            node_name,
            task_name,
            artifact_name,
        )
    } else {
        cairn_common::uri::build_node_artifact_uri_named(
            project_key,
            number,
            exec_seq,
            node_name,
            artifact_name,
        )
    };
    let text = crate::embeddings::artifact_embed_text(&stored.artifact_type, &effective_payload)
        .unwrap_or_default();
    orch.enqueue_resource_embed(&artifact_uri, text);

    // 4. Typed attention emit: the artifact landing IS the actionable fact, so
    //    emit BEFORE the recompute. If recompute moves the issue to terminal
    //    and fires its own `Resolved` event, a watcher that returns on the
    //    first match still gets the artifact metadata first — the resolution
    //    is the downstream consequence, not the user-facing fact. The dedupe
    //    cache prevents the recompute's Resolved from drowning out the
    //    artifact write for the same issue within the dedupe window.
    if let Ok((issue_id, issue_ctx)) =
        crate::orchestrator::attention::lookup_issue_for_attention_by_key(&db, project_key, number)
            .await
    {
        let title = effective_payload
            .get("title")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        let summary = effective_payload
            .get("summary")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        let output_name = artifact_name.unwrap_or("document").to_string();
        orch.emit_attention_event(crate::orchestrator::AttentionEvent {
            issue_id,
            issue_uri: issue_ctx.issue_uri(),
            fact: crate::orchestrator::AttentionFact::ArtifactWritten {
                detail_uri: artifact_uri.clone(),
                content: crate::orchestrator::attention::ArtifactSummary {
                    output_name,
                    version: stored.version,
                    confirmed,
                    title,
                    summary,
                    artifact_type: stored.artifact_type.clone(),
                },
            },
            attention: issue_ctx.attention,
            status: issue_ctx.status,
            updated_at: issue_ctx.updated_at,
        });
    }

    // 5. Recompute the job so its status re-derives. Runs after the typed emit
    //    so the artifact's content reaches the watcher before any downstream
    //    Resolved. Draft memories are reviewed from the idle hook after the
    //    terminal artifact/tool boundary warms the process.
    if let Err(e) = crate::execution::advancement::recompute_job(orch, &job_id) {
        log::warn!("recompute_job after artifact write failed for {job_id}: {e}");
    }

    if should_arm_terminal_interrupt {
        arm_terminal_interrupt_for_job(orch, &job_id);
    }

    let verb = if is_patch { "patched" } else { "wrote" };
    Ok(format!(
        "Artifact {} ({}, type: {}, version {}){}{}{}",
        verb,
        artifact_uri,
        stored.artifact_type,
        stored.version,
        if confirmed {
            ""
        } else {
            " — awaiting user confirmation"
        },
        if synced_pr_metadata {
            " — synced PR title/body"
        } else {
            ""
        },
        terminal_handoff_suffix(should_arm_terminal_interrupt)
    ))
}

/// Server-side capture of a schema-constrained ephemeral call's NATIVE
/// structured output as its return artifact (CAIRN-2505).
///
/// A node-less call constrained to a JSON Schema (Claude `--json-schema`,
/// OpenRouter `response_format`, Codex per-turn `outputSchema`) produces its
/// result through the provider's structured-output mechanism rather than by the
/// model choosing to `write cairn:~/return`. When that constrained result comes
/// back, store it as the call's return artifact directly — validated against the
/// SAME contract the model's own write would validate against — so cheap/fast
/// tiers reliably yield a schema-valid artifact.
///
/// Returns `Ok(true)` when it stored the artifact, `Ok(false)` when there is
/// nothing to capture (the run is not a schema-bearing call, or the artifact
/// already exists — e.g. the model also wrote it). A schema-validation failure is
/// returned as `Err` and NEVER stored: silent provider non-conformance surfaces
/// as a loud failure (the call resolves to null), never corrupt data.
pub(crate) async fn capture_call_structured_output(
    orch: &Orchestrator,
    run_id: &str,
    value: serde_json::Value,
) -> Result<bool, String> {
    // run_id -> job_id (a call's run may live in a team replica).
    let owning = crate::execution::routing::owning_db_for_run(&orch.db, run_id)
        .await
        .map_err(|e| e.to_string())?;
    let Some(job_id) = owning
        .query_opt_text(
            "SELECT job_id FROM runs WHERE id = ?1",
            (run_id.to_string(),),
        )
        .await
        .map_err(|e| e.to_string())?
    else {
        return Ok(false);
    };

    // Only node-less calls carry a persisted output contract; every other run is
    // left to its normal artifact-write flow (unchanged).
    let Some(contract) = load_job_output_contract(orch, &job_id).await else {
        return Ok(false);
    };
    let output_name = contract.artifact_name();

    // If the model already wrote the artifact, don't overwrite it.
    if load_latest_artifact(orch, &job_id, &output_name)
        .await
        .is_some()
    {
        return Ok(false);
    }

    // Validate against the resolved contract schema. A failure is loud (the
    // artifact is never stored), so silent non-conformance can't corrupt data.
    let schema_value = crate::output_schemas::resolve_output_schema(
        orch.schema_dir.as_deref(),
        &contract.schema_type,
    )
    .map_err(|e| format!("Failed to resolve call output schema: {e}"))?;
    validate_against_schema(&schema_value, &value)?;

    let data_json = serde_json::to_string(&value)
        .map_err(|e| format!("Failed to serialize captured structured output: {e}"))?;

    // A call has no review gate, so its captured artifact auto-confirms.
    let stored = store_artifact(orch, &job_id, &output_name, &output_name, data_json, true).await?;

    let artifact = crate::models::Artifact {
        id: stored.artifact_id.clone(),
        job_id: Some(job_id.clone()),
        artifact_type: stored.artifact_type.clone(),
        schema_version: 1,
        data: value,
        version: stored.version,
        parent_version_id: stored.parent_version_id.clone(),
        output_name: stored.output_name.clone(),
        created_at: stored.created_at as i64,
        updated_at: stored.updated_at as i64,
        seen_at: None,
        confirmed: true,
    };
    orch.notifier.artifact(&artifact);
    let _ = orch.services.emitter.emit(
        "artifact-submitted",
        serde_json::json!({
            "artifact_id": stored.artifact_id,
            "job_id": job_id,
            "artifact_type": stored.artifact_type,
            "version": stored.version,
        }),
    );

    // Re-derive the call job's status now that its terminal artifact exists.
    if let Err(e) = crate::execution::advancement::recompute_job(orch, &job_id) {
        log::warn!("recompute_job after captured call output failed for {job_id}: {e}");
    }

    log::info!(
        "Captured native structured output as call artifact: run={run_id} job={job_id} type={output_name} v{}",
        stored.version
    );
    Ok(true)
}

struct StoredArtifact {
    artifact_id: String,
    artifact_type: String,
    version: i32,
    parent_version_id: Option<String>,
    output_name: Option<String>,
    created_at: i32,
    updated_at: i32,
}

async fn append_issue_comment_db(
    orch: &Orchestrator,
    project_key_upper: &str,
    issue_number: i32,
    content: String,
    source: &str,
    intent_id: Option<&str>,
) -> Result<(String, String, i32), String> {
    let project_key_upper = project_key_upper.to_string();
    let source = source.to_string();
    let stable_comment_id = intent_id.map(|id| format!("remote-intent-comment:{id}"));
    // The comment row lives in the database that owns the project (CAIRN-2181):
    // a team project's issues/comments live in its team replica. Edit/delete
    // already route via `for_project`; append must match or an appended comment
    // lands in the wrong DB and vanishes from the team-replica read view.
    let owning_db = orch.db.for_project(&project_key_upper).await;
    owning_db
        .write(|conn| {
            let project_key_upper = project_key_upper.clone();
            let content = content.clone();
            let source = source.clone();
            let stable_comment_id = stable_comment_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "
                        SELECT i.id
                        FROM issues i
                        JOIN projects p ON i.project_id = p.id
                        WHERE p.key = ?1 AND i.number = ?2
                        LIMIT 1
                        ",
                        params![project_key_upper.as_str(), issue_number],
                    )
                    .await?;
                let row = rows.next().await?.ok_or_else(|| {
                    DbError::Row(format!(
                        "Issue {}-{} not found",
                        project_key_upper, issue_number
                    ))
                })?;
                let issue_id = row.text(0)?;
                let comment_id = stable_comment_id.unwrap_or_else(|| ids::mint_child(&issue_id));
                let now = chrono::Utc::now().timestamp() as i32;
                let seq = crate::issues::comments::next_issue_comment_seq(conn, &issue_id).await?;

                conn.execute(
                    "
                    INSERT OR IGNORE INTO comments (id, issue_id, content, source, created_at, seq)
                    VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                    ",
                    params![
                        comment_id.as_str(),
                        issue_id.as_str(),
                        content.as_str(),
                        source.as_str(),
                        now,
                        seq
                    ],
                )
                .await?;

                let mut existing = conn
                    .query(
                        "SELECT issue_id, content, source, created_at FROM comments WHERE id=?1",
                        params![comment_id.as_str()],
                    )
                    .await?;
                let existing = existing.next().await?.ok_or_else(|| {
                    DbError::Row("comment missing after idempotent insert".into())
                })?;
                if existing.text(0)? != issue_id
                    || existing.text(1)? != content
                    || existing.text(2)? != source
                {
                    return Err(DbError::Row(format!(
                        "comment identity {comment_id} already exists with different content"
                    )));
                }
                let durable_created_at = existing.i64(3)? as i32;
                Ok((comment_id, issue_id, durable_created_at))
            })
        })
        .await
        .map_err(|e| {
            if e.to_string().contains("not found") {
                e.to_string()
            } else {
                format!("Failed to insert comment: {e}")
            }
        })
}

/// Resolve a written artifact version's `confirmed` flag. Confirmation is a
/// one-shot job-progress trigger, not a per-version gate:
/// - once any version is confirmed, later writes keep it confirmed;
/// - a patch before confirmation inherits the unconfirmed state (a
///   request-changes revision stays Blocked until the human confirms);
/// - a fresh create with no prior (or an unconfirmed prior) takes the node's
///   policy (`auto` -> confirmed, `user` -> awaits the human).
fn resolve_confirmed(is_patch: bool, prior_confirmed: Option<bool>, policy: ConfirmPolicy) -> bool {
    match (is_patch, prior_confirmed) {
        (_, Some(true)) => true,
        (true, Some(false)) => false,
        _ => matches!(policy, ConfirmPolicy::Auto),
    }
}

fn terminal_handoff_suffix(should_arm_terminal_interrupt: bool) -> &'static str {
    if should_arm_terminal_interrupt {
        " — applied; this turn now ends for review (an intentional handoff by Cairn — any interruption notice that follows is the turn boundary, not a user abort)"
    } else {
        ""
    }
}

fn output_artifact_name_matches(artifact_name: Option<&str>, required_name: Option<&str>) -> bool {
    match required_name {
        Some(required) => artifact_name.unwrap_or("document") == required,
        // Mirror recompute's unnamed-contract fallback: any artifact can satisfy
        // an output contract only when there is a schema but no required name.
        None => true,
    }
}

fn should_arm_output_artifact_interrupt(
    is_patch: bool,
    artifact_name: Option<&str>,
    required_name: Option<&str>,
    has_output_contract: bool,
) -> bool {
    has_output_contract && !is_patch && output_artifact_name_matches(artifact_name, required_name)
}

/// Sweep unsealed work onto the PR branch, refuse an empty-delta PR, and publish
/// the current bookmark before a create-pr artifact write is stored. Runs only for
/// a content create/patch of a create-pr artifact (the `action:`
/// merge/close/refresh paths return earlier).
///
/// Three moves, serialized on the per-store jj lock like every other Cairn jj
/// store writer:
/// 1. **Sweep.** If the working copy carries unsealed work, seal it via the normal
///    [`crate::mcp::vcs::JjBackend::seal_all`] path, advancing the bookmark and
///    preserving the seal path's best-effort publication contract.
/// 2. **Gate.** Compute the branch's delta versus its base over the store. If it is
///    STILL empty after the sweep, refuse the artifact write with a diagnostic
///    naming the branch and base, catching a +0/-0 PR at its source.
/// 3. **Publish.** Unconditionally push the current bookmark to origin and propagate
///    any failure so artifact storage cannot report success against a stale PR head.
///
/// Skips entirely when the worktree no longer exists on disk (post-merge title
/// edits and the like): there is nothing to sweep, gate, or publish.
async fn sweep_gate_and_publish_create_pr_branch(
    orch: &Orchestrator,
    project_id: &str,
    job_id: &str,
    worktree_path: &str,
    branch: &str,
    base_branch: Option<&str>,
) -> Result<(), String> {
    let worktree = std::path::Path::new(worktree_path);
    if !crate::jj::is_jj_dir(worktree) {
        return Ok(());
    }
    let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let backend = crate::mcp::vcs::JjBackend::new(crate::jj::JjEnv::resolve(
        &orch.jj_binary_path,
        &orch.config_dir,
    ));

    // Serialize on the per-store lock, keyed off the workspace's own project-root
    // marker (the same store `resolve_store_lock` targets). Best-effort resolution:
    // a worktree missing its marker seals without the guard, matching the
    // resolve_store_lock None fallback.
    let project_root = crate::jj::read_project_root_marker(worktree);
    let store = project_root
        .as_ref()
        .map(|root| crate::jj::project_store_dir(&orch.config_dir, root));
    let (before_tip, after_tip) = {
        let _guard = match store.as_deref() {
            Some(store) => Some(
                orch.acquire_jj_store_lock(store, "create-pr branch publication")
                    .await,
            ),
            None => None,
        };
        let before_tip = store
            .as_ref()
            .and_then(|store| crate::jj::bookmark_commit(&jj, store, branch));
        sweep_gate_and_publish_create_pr_branch_locked(
            &jj,
            &backend,
            worktree,
            branch,
            base_branch,
        )?;
        let after_tip = store
            .as_ref()
            .and_then(|store| crate::jj::bookmark_commit(&jj, store, branch));
        (before_tip, after_tip)
    };

    if before_tip != after_tip {
        let project_root = project_root.ok_or_else(|| {
            "create-pr advanced the managed bookmark but its project-root marker was missing; downstream reconciliation could not be routed".to_string()
        })?;
        let new_tip = after_tip
            .ok_or_else(|| format!("create-pr publication removed managed bookmark `{branch}`"))?;
        crate::orchestrator::base_advance::reconcile_managed_branch_advance(
            orch,
            project_id,
            &project_root.to_string_lossy(),
            branch,
            &new_tip,
            Some(job_id),
        )
        .await
        .map_err(|error| {
            format!(
                "create-pr sealed and published `{branch}`, but downstream workspace reconciliation failed: {error}"
            )
        })?;
    }
    Ok(())
}

fn require_create_pr_branch_metadata<'a>(
    job_id: &str,
    worktree_path: Option<&'a str>,
    branch: Option<&'a str>,
) -> Result<(&'a str, &'a str), String> {
    let worktree_path = worktree_path.filter(|value| !value.trim().is_empty());
    let branch = branch.filter(|value| !value.trim().is_empty());
    match (worktree_path, branch) {
        (Some(worktree_path), Some(branch)) => Ok((worktree_path, branch)),
        _ => Err(format!(
            "create-pr artifact was not written because job `{job_id}` has no live worktree and \
             branch metadata to publish"
        )),
    }
}

fn sweep_gate_and_publish_create_pr_branch_locked(
    jj: &crate::jj::JjEnv,
    backend: &crate::mcp::vcs::JjBackend,
    worktree: &std::path::Path,
    branch: &str,
    base_branch: Option<&str>,
) -> Result<(), String> {
    // 1. Sweep unsealed work onto the bookmark. An unreadable working-copy state
    //    is not clean: fail before publication so unsealed edits cannot be omitted.
    let is_dirty = crate::jj::is_working_copy_dirty(jj, worktree).map_err(|e| {
        format!(
            "create-pr artifact was not written because branch `{branch}` working-copy state \
             could not be inspected before publication: {e}"
        )
    })?;
    if is_dirty {
        crate::mcp::vcs::WorktreeVcs::seal_all(backend, worktree, "seal pending work for PR", None)
            .map_err(|e| {
                format!("create-pr: failed to seal pending work before opening the PR: {e}")
            })?;
    }

    // 2. Gate on a non-empty delta vs base. `node_changed_files` measures the
    //    node's own commits (`base..@`) over the live graph. `None` means the base
    //    could not be resolved — do NOT refuse on an unprovable delta; only a
    //    positively-empty delta is the refusal.
    let base_rev = crate::jj::read_base_marker(worktree)
        .map(|(_, rev)| rev)
        .filter(|rev| !rev.is_empty());
    if let Some(changes) =
        crate::jj::node_changed_files(jj, worktree, base_branch, base_rev.as_deref())
    {
        if changes.is_empty() {
            let base_name = base_branch.unwrap_or("the base branch");
            return Err(format!(
                "create-pr refused: branch `{branch}` has an empty delta versus its base \
                 `{base_name}` — there is nothing to open a PR from. Seal your work first, then \
                 retry; inspect the delta with `jj diff --from {base_name} --to @`."
            ));
        }
    }

    // 3. Publish even when the workspace was already clean: a rebase or re-anchor
    //    can advance the shared-store bookmark without leaving anything to seal.
    crate::jj::push_to_origin(jj, worktree, branch).map_err(|e| {
        format!(
            "create-pr artifact was not written because bookmark `{branch}` could not be \
             published to origin: {e}"
        )
    })
}

fn create_pr_artifact_details(
    payload: &serde_json::Value,
    artifact_name: Option<&str>,
    stored_output_name: Option<&str>,
    stored_artifact_type: &str,
) -> Option<(String, Option<String>)> {
    // Keyed on the written artifact's OWN name/type, never on the terminal
    // contract name: a ctx-self write under a create-pr-terminal node must not
    // sync the PR.
    let is_create_pr = artifact_name == Some("create-pr")
        || stored_output_name == Some("create-pr")
        || stored_artifact_type == "create-pr";
    if !is_create_pr {
        return None;
    }

    let title = payload.get("title")?.as_str()?.to_string();
    let body = payload
        .get("body")
        .and_then(|value| value.as_str())
        .map(str::to_string);
    Some((title, body))
}

fn active_run_id_for_job(orch: &Orchestrator, job_id: &str) -> Option<String> {
    let Ok(processes) = orch.process_state.processes.lock() else {
        return None;
    };
    let run_id = processes
        .iter()
        .find(|(_, process)| process.job_id.as_deref() == Some(job_id))
        .map(|(run_id, _)| run_id.clone());
    run_id
}

fn arm_terminal_interrupt_for_job(orch: &Orchestrator, job_id: &str) {
    let run_id = active_run_id_for_job(orch, job_id);

    if let Some(run_id) = run_id {
        if !orch.process_state.arm_terminal_tool(&run_id) {
            log::warn!(
                "Failed to arm terminal interrupt for output artifact job {} run {}",
                job_id,
                run_id
            );
        }
    } else {
        log::warn!(
            "No active run found to arm terminal interrupt for output artifact job {}",
            job_id
        );
    }
}

/// Apply a structured artifact patch.
///
/// Supported patch forms:
/// - field merge patch: shallow merge artifact fields over the prior object;
/// - text replacement patch: `{old_string,new_string,replace_all?,field?}` edits
///   an existing top-level string field (`field`, else `content`, else `body`).
fn apply_artifact_patch(
    base: serde_json::Value,
    patch: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    if is_text_replacement_patch(patch) {
        apply_artifact_text_replacement(base, patch)
    } else {
        Ok(merge_artifact_payload(base, patch))
    }
}

fn is_text_replacement_patch(patch: &serde_json::Value) -> bool {
    patch
        .as_object()
        .is_some_and(|obj| obj.contains_key("old_string") || obj.contains_key("new_string"))
}

fn apply_artifact_text_replacement(
    base: serde_json::Value,
    patch: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let mut base_obj = base.as_object().cloned().ok_or_else(|| {
        "Artifact text replacement patch requires an existing object artifact".to_string()
    })?;
    let patch_obj = patch
        .as_object()
        .ok_or_else(|| "Artifact text replacement patch payload must be an object".to_string())?;

    reject_mixed_artifact_text_patch_keys(patch_obj)?;

    let old = patch_obj
        .get("old_string")
        .and_then(|value| value.as_str())
        .ok_or_else(|| "Artifact text replacement patch requires string old_string".to_string())?;
    let new = patch_obj
        .get("new_string")
        .and_then(|value| value.as_str())
        .ok_or_else(|| "Artifact text replacement patch requires string new_string".to_string())?;
    let replace_all = patch_obj
        .get("replace_all")
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    let field = resolve_artifact_text_field(&base_obj, patch_obj)?;

    let current = base_obj
        .get(&field)
        .and_then(|value| value.as_str())
        .ok_or_else(|| {
            format!("Artifact text replacement field `{field}` is missing or not a string")
        })?;

    let updated = replace_artifact_text(current, old, new, replace_all)?;
    base_obj.insert(field, serde_json::Value::String(updated));
    Ok(serde_json::Value::Object(base_obj))
}

fn reject_mixed_artifact_text_patch_keys(
    patch_obj: &serde_json::Map<String, serde_json::Value>,
) -> Result<(), String> {
    const HELPER_KEYS: &[&str] = &["old_string", "new_string", "replace_all", "field"];
    let extra_keys: Vec<&str> = patch_obj
        .keys()
        .map(String::as_str)
        .filter(|key| !HELPER_KEYS.contains(key))
        .collect();
    if extra_keys.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "Artifact text replacement patch cannot be mixed with artifact field updates ({}) because those fields would be ambiguous; submit a separate field merge patch",
            extra_keys.join(", ")
        ))
    }
}

fn resolve_artifact_text_field(
    base_obj: &serde_json::Map<String, serde_json::Value>,
    patch_obj: &serde_json::Map<String, serde_json::Value>,
) -> Result<String, String> {
    if let Some(field) = patch_obj.get("field") {
        let field = field.as_str().ok_or_else(|| {
            "Artifact text replacement field selector must be a string".to_string()
        })?;
        if base_obj
            .get(field)
            .and_then(|value| value.as_str())
            .is_none()
        {
            return Err(format!(
                "Artifact text replacement field `{field}` is missing or not a string"
            ));
        }
        return Ok(field.to_string());
    }

    for candidate in ["content", "body"] {
        if base_obj
            .get(candidate)
            .and_then(|value| value.as_str())
            .is_some()
        {
            return Ok(candidate.to_string());
        }
    }

    Err("Artifact text replacement requires `field` or a string `content`/`body` field".to_string())
}

pub(crate) fn replace_artifact_text(
    current: &str,
    old: &str,
    new: &str,
    replace_all: bool,
) -> Result<String, String> {
    if let Some(anchors) = crate::mcp::wildcard::parse_wildcard(old) {
        return crate::mcp::wildcard::apply_wildcard_edit(current, &anchors, new)
            .map(|(result, _)| result)
            .map_err(|e| format!("Wildcard edit failed: {e}"));
    }

    let literal_old = crate::mcp::wildcard::unescape_literal(old);
    if !current.contains(literal_old.as_str()) {
        return Err(
            crate::mcp::handlers::write::file_mutations::literal_not_found_diagnostic(
                &literal_old,
                new,
            ),
        );
    }

    let matches = current.matches(literal_old.as_str()).count();
    if matches > 1 && !replace_all {
        return Err(format!(
            "old_string matched {matches} times; use replace_all:true or make old_string more specific"
        ));
    }

    if replace_all {
        Ok(current.replace(literal_old.as_str(), new))
    } else {
        Ok(current.replacen(literal_old.as_str(), new, 1))
    }
}

/// Merge a `patch` payload's keys over the existing artifact object (shallow,
/// per-key). A non-object base (or non-object patch) yields the patch as-is.
fn merge_artifact_payload(base: serde_json::Value, patch: &serde_json::Value) -> serde_json::Value {
    match base {
        serde_json::Value::Object(mut base_obj) => {
            if let Some(obj) = patch.as_object() {
                for (key, value) in obj {
                    base_obj.insert(key.clone(), value.clone());
                }
            }
            serde_json::Value::Object(base_obj)
        }
        _ => patch.clone(),
    }
}

/// Load the latest artifact version's parsed `data` and `confirmed` flag for a
/// job. The patch path merges a partial payload over the data; the `confirmed`
/// flag carries the one-shot confirmation across edits.
async fn load_latest_artifact(
    orch: &Orchestrator,
    job_id: &str,
    output_name: &str,
) -> Option<(serde_json::Value, bool)> {
    let job_id = job_id.to_string();
    let output_name = output_name.to_string();
    let Ok(db) = crate::execution::routing::owning_db_for_job(&orch.db, &job_id).await else {
        return None;
    };
    let row: Option<(String, i64)> = db
        .read(|conn| {
            let job_id = job_id.clone();
            let output_name = output_name.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT data, confirmed FROM artifacts WHERE job_id = ?1 AND output_name = ?2 ORDER BY version DESC LIMIT 1",
                        params![job_id.as_str(), output_name.as_str()],
                    )
                    .await?;
                rows.next()
                    .await?
                    .map(|row| Ok::<_, DbError>((row.text(0)?, row.i64(1)?)))
                    .transpose()
            })
        })
        .await
        .ok()
        .flatten();
    row.and_then(|(data, confirmed)| {
        serde_json::from_str(&data)
            .ok()
            .map(|value| (value, confirmed != 0))
    })
}

/// Resolve an artifact write's stored identity and schema type.
///
/// Resolve a written artifact's stored identity `(artifact_type, output_name)`.
///
/// The addressed `cairn:~/<name>` is the artifact's identity: the name keys its
/// per-name version chain (`output_name`) and labels the row (`artifact_type`).
/// When the caller used the generic `/artifact` alias (no name), fall back to the
/// node's terminal contract name.
async fn resolve_artifact_identity(
    orch: &Orchestrator,
    job_id: &str,
    artifact_name: Option<&str>,
) -> (String, String) {
    if let Some(name) = artifact_name {
        return (name.to_string(), name.to_string());
    }
    let fallback = resolve_terminal_artifact_name(orch, job_id)
        .await
        .unwrap_or_else(|| "document".to_string());
    (fallback.clone(), fallback)
}

/// The node's terminal (context-out) artifact name, for the generic `/artifact`
/// alias fallback.
async fn resolve_terminal_artifact_name(orch: &Orchestrator, job_id: &str) -> Option<String> {
    let (node_id, execution_id) = job_node_execution(orch, job_id).await.ok().flatten()?;
    let node_id = node_id?;
    let execution_id = execution_id?;
    resolve_artifact_schema(orch, &node_id, &execution_id)
        .await
        .and_then(|info| info.artifact_name)
}

async fn store_artifact(
    orch: &Orchestrator,
    job_id: &str,
    artifact_type: &str,
    output_name: &str,
    data_json: String,
    confirmed: bool,
) -> Result<StoredArtifact, String> {
    let job_id = job_id.to_string();
    let artifact_type = artifact_type.to_string();
    let output_name = output_name.to_string();
    let db = crate::execution::routing::owning_db_for_job(&orch.db, &job_id).await?;
    db.write(|conn| {
        let job_id = job_id.clone();
        let data_json = data_json.clone();
        let artifact_type = artifact_type.clone();
        let output_name = output_name.clone();
        Box::pin(async move {
            insert_next_artifact_version(
                conn,
                &job_id,
                &artifact_type,
                &output_name,
                &data_json,
                confirmed,
            )
            .await
        })
    })
    .await
    .map_err(|e| format!("Failed to store artifact: {}", e))
}

/// Insert the next version in an artifact's `(job_id, output_name)` chain.
///
/// Each distinct addressed name a node writes owns an independent version chain:
/// the parent/version lookup is scoped to `output_name`, so a write to name A
/// never bumps name B's version and links its parent only within its own chain.
/// A node that only ever writes one name increments exactly as a per-job chain
/// did.
async fn insert_next_artifact_version(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
    artifact_type: &str,
    output_name: &str,
    data_json: &str,
    confirmed: bool,
) -> Result<StoredArtifact, DbError> {
    let mut rows = conn
        .query(
            "
            SELECT id, version
            FROM artifacts
            WHERE job_id = ?1 AND output_name = ?2
            ORDER BY version DESC
            LIMIT 1
            ",
            params![job_id, output_name],
        )
        .await?;
    let existing = rows
        .next()
        .await?
        .map(|row| Ok::<_, DbError>((row.text(0)?, row.i64(1)? as i32)))
        .transpose()?;

    let (parent_version_id, version) = match existing {
        Some((parent_id, parent_version)) => (Some(parent_id), parent_version + 1),
        None => (None, 1),
    };
    let now = chrono::Utc::now().timestamp() as i32;
    let artifact_id = ids::mint_child(job_id);

    conn.execute(
        "
        INSERT INTO artifacts (
            id, job_id, artifact_type, schema_version, data, version,
            parent_version_id, output_name, confirmed, created_at, updated_at
        )
        VALUES (?1, ?2, ?3, 1, ?4, ?5, ?6, ?7, ?8, ?9, ?9)
        ",
        params![
            artifact_id.as_str(),
            job_id,
            artifact_type,
            data_json,
            version,
            parent_version_id.as_deref(),
            output_name,
            if confirmed { 1 } else { 0 },
            now
        ],
    )
    .await?;

    Ok(StoredArtifact {
        artifact_id,
        artifact_type: artifact_type.to_string(),
        version,
        parent_version_id,
        output_name: Some(output_name.to_string()),
        created_at: now,
        updated_at: now,
    })
}

async fn job_node_execution(
    orch: &Orchestrator,
    job_id: &str,
) -> DbResult<Option<(Option<String>, Option<String>)>> {
    let job_id = job_id.to_string();
    let db = crate::execution::routing::owning_db_for_job(&orch.db, &job_id)
        .await
        .map_err(|e| DbError::internal(e.to_string()))?;
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT recipe_node_id, execution_id FROM jobs WHERE id = ?1",
                    (job_id.as_str(),),
                )
                .await?;
            rows.next()
                .await?
                .map(|row| Ok((row.opt_text(0)?, row.opt_text(1)?)))
                .transpose()
        })
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::{
        apply_artifact_patch, merge_artifact_payload, require_create_pr_branch_metadata,
        resolve_confirmed, should_arm_output_artifact_interrupt,
        sweep_gate_and_publish_create_pr_branch_locked, terminal_handoff_suffix,
        validate_against_schema,
    };
    use crate::models::ConfirmPolicy;
    use serde_json::json;
    use std::path::{Path, PathBuf};
    use tempfile::TempDir;

    fn jj_bin() -> Option<String> {
        let bin = std::env::var("CAIRN_JJ_BIN")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| "jj".to_string());
        crate::env::command(&bin)
            .arg("--version")
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false)
            .then_some(bin)
    }

    fn git(repo: &Path, args: &[&str]) {
        assert!(
            crate::env::git()
                .args(args)
                .current_dir(repo)
                .status()
                .unwrap()
                .success(),
            "git {args:?} failed"
        );
    }

    fn git_stdout(repo: &Path, args: &[&str]) -> String {
        let output = crate::env::git()
            .args(args)
            .current_dir(repo)
            .output()
            .unwrap();
        assert!(output.status.success(), "git {args:?} failed");
        String::from_utf8_lossy(&output.stdout).trim().to_string()
    }

    struct CreatePrJjFixture {
        _home: TempDir,
        origin: TempDir,
        project: TempDir,
        _worktrees: TempDir,
        jj: crate::jj::JjEnv,
        backend: crate::mcp::vcs::JjBackend,
        store: PathBuf,
        worktree: PathBuf,
        branch: &'static str,
        remote_head_before_advance: String,
    }

    impl CreatePrJjFixture {
        fn new(bin: &str) -> Self {
            let home = TempDir::new().unwrap();
            let origin = TempDir::new().unwrap();
            let project = TempDir::new().unwrap();
            let worktrees = TempDir::new().unwrap();

            git(origin.path(), &["init", "-q", "--bare", "-b", "main"]);
            git(project.path(), &["init", "-q", "-b", "main"]);
            git(project.path(), &["config", "user.email", "p@e.com"]);
            git(project.path(), &["config", "user.name", "P"]);
            std::fs::write(project.path().join("shared.rs"), "base\n").unwrap();
            git(project.path(), &["add", "-A"]);
            git(project.path(), &["commit", "-q", "-m", "base"]);
            git(
                project.path(),
                &["remote", "add", "origin", &origin.path().to_string_lossy()],
            );
            git(project.path(), &["push", "-q", "origin", "main"]);

            let jj = crate::jj::JjEnv::resolve(bin, home.path());
            let store = home.path().join("jj-stores").join("project");
            crate::jj::ensure_project_store(&jj, &store, project.path()).unwrap();
            let branch = "agent/CAIRN-2679-builder-1";
            let worktree = worktrees.path().join("builder");
            crate::jj::add_workspace(&jj, &store, &worktree, branch, "main", None).unwrap();

            std::fs::write(worktree.join("feature.rs"), "first\n").unwrap();
            crate::jj::seal(&jj, &worktree, "initial feature", None).unwrap();
            crate::jj::push_to_origin(&jj, &worktree, branch).unwrap();
            let remote_head_before_advance = git_stdout(origin.path(), &["rev-parse", branch]);

            // Advance the bookmark locally without pushing. `seal` leaves a fresh,
            // clean working-copy commit while the bare origin remains stale.
            std::fs::write(worktree.join("feature.rs"), "second\n").unwrap();
            crate::jj::seal(&jj, &worktree, "rewrite feature", None).unwrap();
            assert!(!crate::jj::is_working_copy_dirty(&jj, &worktree).unwrap());
            let local_head = crate::jj::bookmark_commit(&jj, &store, branch).unwrap();
            assert_ne!(remote_head_before_advance, local_head);
            assert_eq!(
                git_stdout(origin.path(), &["rev-parse", branch]),
                remote_head_before_advance
            );

            let backend =
                crate::mcp::vcs::JjBackend::new(crate::jj::JjEnv::resolve(bin, home.path()));
            Self {
                _home: home,
                origin,
                project,
                _worktrees: worktrees,
                jj,
                backend,
                store,
                worktree,
                branch,
                remote_head_before_advance,
            }
        }

        fn local_head(&self) -> String {
            crate::jj::bookmark_commit(&self.jj, &self.store, self.branch).unwrap()
        }
    }

    #[test]
    #[serial_test::serial(jj)]
    fn create_pr_preparation_publishes_clean_locally_advanced_bookmark() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping create_pr_preparation_publishes_clean_locally_advanced_bookmark: jj not resolvable");
            return;
        };
        let fixture = CreatePrJjFixture::new(&bin);

        sweep_gate_and_publish_create_pr_branch_locked(
            &fixture.jj,
            &fixture.backend,
            &fixture.worktree,
            fixture.branch,
            Some("main"),
        )
        .unwrap();

        assert_eq!(
            git_stdout(fixture.origin.path(), &["rev-parse", fixture.branch]),
            fixture.local_head()
        );
    }

    #[test]
    #[serial_test::serial(jj)]
    fn create_pr_preparation_fails_when_clean_bookmark_cannot_be_published() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping create_pr_preparation_fails_when_clean_bookmark_cannot_be_published: jj not resolvable");
            return;
        };
        let fixture = CreatePrJjFixture::new(&bin);
        let unavailable_origin = fixture.project.path().join("unavailable-origin");
        git(
            fixture.project.path(),
            &[
                "remote",
                "set-url",
                "origin",
                &unavailable_origin.to_string_lossy(),
            ],
        );

        let error = sweep_gate_and_publish_create_pr_branch_locked(
            &fixture.jj,
            &fixture.backend,
            &fixture.worktree,
            fixture.branch,
            Some("main"),
        )
        .unwrap_err();

        assert!(
            error.contains("create-pr artifact was not written"),
            "{error}"
        );
        assert!(error.contains(fixture.branch), "{error}");
        assert!(
            error.contains("could not be published to origin"),
            "{error}"
        );
        assert_eq!(
            git_stdout(fixture.origin.path(), &["rev-parse", fixture.branch]),
            fixture.remote_head_before_advance
        );
    }

    #[test]
    #[serial_test::serial(jj)]
    fn create_pr_preparation_fails_when_working_copy_state_is_unreadable() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping create_pr_preparation_fails_when_working_copy_state_is_unreadable: jj not resolvable");
            return;
        };
        let fixture = CreatePrJjFixture::new(&bin);
        crate::jj::forget_workspace(&fixture.jj, &fixture.store, fixture.branch).unwrap();

        let error = sweep_gate_and_publish_create_pr_branch_locked(
            &fixture.jj,
            &fixture.backend,
            &fixture.worktree,
            fixture.branch,
            Some("main"),
        )
        .unwrap_err();

        assert!(
            error.contains("working-copy state could not be inspected before publication"),
            "{error}"
        );
        assert_eq!(
            git_stdout(fixture.origin.path(), &["rev-parse", fixture.branch]),
            fixture.remote_head_before_advance
        );
    }

    #[test]
    fn create_pr_branch_metadata_is_required_before_storage() {
        let missing_worktree =
            require_create_pr_branch_metadata("job-1", None, Some("agent/CAIRN-1-builder-0"))
                .unwrap_err();
        assert!(missing_worktree.contains("artifact was not written"));
        assert!(missing_worktree.contains("job-1"));

        let missing_branch =
            require_create_pr_branch_metadata("job-2", Some("/tmp/worktree"), None).unwrap_err();
        assert!(missing_branch.contains("artifact was not written"));
        assert!(missing_branch.contains("job-2"));

        assert_eq!(
            require_create_pr_branch_metadata(
                "job-3",
                Some("/tmp/worktree"),
                Some("agent/CAIRN-3-builder-0")
            )
            .unwrap(),
            ("/tmp/worktree", "agent/CAIRN-3-builder-0")
        );
    }

    #[test]
    fn persisted_output_contract_drives_validation() {
        // CAIRN-2481: a call persists its resolved DelegatedOutputContract on the
        // job row as JSON; resolve_artifact_contract parses it back and uses
        // schema_type as the validation schema. This pins that a conforming
        // return payload is accepted and a violating one is rejected — the exact
        // resolve_output_schema + validate_against_schema path the write handler
        // runs, keyed off the persisted contract rather than the `return`
        // fallback.
        use crate::models::{DelegatedOutputContract, OutputSchema};
        let contract = DelegatedOutputContract {
            schema_type: OutputSchema::Custom(json!({
                "type": "object",
                "properties": {"score": {"type": "number"}},
                "required": ["score"],
                "additionalProperties": false
            })),
            tool_name: None,
            description: None,
        };
        let stored = serde_json::to_string(&contract).unwrap();
        let parsed: DelegatedOutputContract = serde_json::from_str(&stored).unwrap();
        // A custom inline schema still writes to the canonical `return` artifact.
        assert_eq!(parsed.artifact_name(), "return");
        let schema =
            crate::output_schemas::resolve_output_schema(None, &parsed.schema_type).unwrap();
        assert!(validate_against_schema(&schema, &json!({"score": 7})).is_ok());
        assert!(validate_against_schema(&schema, &json!({"nope": true})).is_err());
    }

    #[test]
    fn create_takes_policy() {
        assert!(!resolve_confirmed(false, None, ConfirmPolicy::User));
        assert!(resolve_confirmed(false, None, ConfirmPolicy::Auto));
    }

    #[test]
    fn request_changes_revision_stays_unconfirmed() {
        // user-policy patch over the still-unconfirmed version stays Blocked.
        assert!(!resolve_confirmed(true, Some(false), ConfirmPolicy::User));
    }

    #[test]
    fn edit_after_confirmation_never_rearms_the_gate() {
        // The one-shot guarantee: once confirmed, any later write keeps it
        // confirmed regardless of policy or mode.
        assert!(resolve_confirmed(true, Some(true), ConfirmPolicy::User));
        assert!(resolve_confirmed(false, Some(true), ConfirmPolicy::User));
        assert!(resolve_confirmed(true, Some(true), ConfirmPolicy::Auto));
    }

    #[test]
    fn create_pr_artifact_details_detects_stored_create_pr_name() {
        // The generic `/artifact` alias resolves its identity to the terminal
        // name, so a create-pr write is stored under `output_name = create-pr`.
        let payload = json!({"title":"Updated PR", "body":"Fresh description"});
        let details =
            super::create_pr_artifact_details(&payload, None, Some("create-pr"), "create-pr");
        assert_eq!(
            details,
            Some((
                "Updated PR".to_string(),
                Some("Fresh description".to_string())
            ))
        );
    }

    #[test]
    fn create_pr_artifact_details_ignores_non_pr_artifacts() {
        let payload = json!({"title":"Plan", "body":"Not a PR"});
        assert!(
            super::create_pr_artifact_details(&payload, Some("plan"), Some("plan"), "plan")
                .is_none()
        );
    }

    #[test]
    fn create_pr_artifact_details_ignores_ctx_self_under_pr_terminal() {
        // A ctx-self living doc (`notes`) written by a node whose terminal is
        // create-pr must NOT sync the PR: keying is on the written artifact's own
        // name/type, never the terminal contract name.
        let payload = json!({"title":"Notes", "body":"scratch"});
        assert!(
            super::create_pr_artifact_details(&payload, Some("notes"), Some("notes"), "notes")
                .is_none()
        );
    }

    #[test]
    fn terminal_handoff_suffix_only_describes_armed_writes() {
        assert!(terminal_handoff_suffix(true).contains("intentional handoff by Cairn"));
        assert_eq!(terminal_handoff_suffix(false), "");
    }
    #[test]
    fn output_artifact_interrupt_arms_only_matching_creates() {
        assert!(should_arm_output_artifact_interrupt(
            false,
            Some("create-pr"),
            Some("create-pr"),
            true
        ));
        assert!(!should_arm_output_artifact_interrupt(
            true,
            Some("create-pr"),
            Some("create-pr"),
            true
        ));
        assert!(!should_arm_output_artifact_interrupt(
            false,
            Some("notes"),
            Some("create-pr"),
            true
        ));
        assert!(!should_arm_output_artifact_interrupt(
            false,
            Some("notes"),
            Some("result"),
            true
        ));
        assert!(should_arm_output_artifact_interrupt(
            false,
            Some("result"),
            Some("result"),
            true
        ));
        assert!(!should_arm_output_artifact_interrupt(
            false,
            Some("create-pr"),
            Some("create-pr"),
            false
        ));
        assert!(should_arm_output_artifact_interrupt(
            false,
            Some("checkpoint"),
            None,
            true
        ));
        assert!(should_arm_output_artifact_interrupt(
            false,
            None,
            Some("document"),
            true
        ));
    }

    #[test]
    fn patch_merges_over_base_keeping_unedited_fields() {
        let base = json!({"title": "Original", "content": "old", "summary": "s"});
        let merged = apply_artifact_patch(base, &json!({"content": "new"})).unwrap();
        // Edited field updated; required + untouched fields preserved.
        assert_eq!(merged["title"], "Original");
        assert_eq!(merged["content"], "new");
        assert_eq!(merged["summary"], "s");
    }

    #[test]
    fn text_patch_updates_content_and_discards_helper_keys() {
        let base = json!({"title": "Original", "content": "old text", "summary": "s"});
        let patched = apply_artifact_patch(
            base,
            &json!({"old_string": "old", "new_string": "new", "replace_all": true}),
        )
        .unwrap();

        assert_eq!(patched["content"], "new text");
        assert!(patched.get("old_string").is_none());
        assert!(patched.get("new_string").is_none());
        assert!(patched.get("replace_all").is_none());
        assert!(patched.get("field").is_none());
    }

    #[test]
    fn text_patch_can_target_explicit_summary_field() {
        let base = json!({"title": "Original", "content": "unchanged", "summary": "stale summary"});
        let patched = apply_artifact_patch(
            base,
            &json!({"old_string": "stale", "new_string": "fresh", "field": "summary"}),
        )
        .unwrap();

        assert_eq!(patched["content"], "unchanged");
        assert_eq!(patched["summary"], "fresh summary");
    }

    #[test]
    fn text_patch_defaults_to_content_before_body() {
        let base = json!({"title": "Original", "content": "stale content", "body": "stale body"});
        let patched =
            apply_artifact_patch(base, &json!({"old_string": "stale", "new_string": "fresh"}))
                .unwrap();

        assert_eq!(patched["content"], "fresh content");
        assert_eq!(patched["body"], "stale body");
    }

    #[test]
    fn text_patch_rejects_missing_old_string() {
        let base = json!({"title": "Original", "content": "current text"});
        let error = apply_artifact_patch(
            base,
            &json!({"old_string": "missing", "new_string": "fresh"}),
        )
        .unwrap_err();

        assert!(error.contains("old_string not found"), "{error}");
    }

    #[test]
    fn text_patch_rejects_duplicate_match_without_replace_all() {
        let base = json!({"title": "Original", "content": "dup dup"});
        let error = apply_artifact_patch(base, &json!({"old_string": "dup", "new_string": "new"}))
            .unwrap_err();

        assert!(error.contains("matched 2 times"), "{error}");
        assert!(error.contains("replace_all:true"), "{error}");
    }

    #[test]
    fn text_patch_replace_all_updates_every_match() {
        let base = json!({"title": "Original", "content": "dup dup"});
        let patched = apply_artifact_patch(
            base,
            &json!({"old_string": "dup", "new_string": "new", "replace_all": true}),
        )
        .unwrap();

        assert_eq!(patched["content"], "new new");
    }

    #[test]
    fn text_patch_rejects_mixed_field_updates() {
        let base =
            json!({"title": "Original", "content": "stale content", "summary": "old summary"});
        let error = apply_artifact_patch(
            base,
            &json!({"old_string": "stale", "new_string": "fresh", "summary": "new summary"}),
        )
        .unwrap_err();

        assert!(error.contains("cannot be mixed"), "{error}");
        assert!(error.contains("summary"), "{error}");
    }

    #[test]
    fn strict_plan_schema_rejects_unknown_field_merge_keys() {
        let schema: serde_json::Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../resources/schemas/plan.json"
        )))
        .unwrap();
        let base = json!({"title": "Original", "content": "old", "summary": "s"});
        let merged = apply_artifact_patch(base, &json!({"old_string": "old", "new_string": "new"}));
        assert!(merged.is_ok());

        let inert_field_merge = merge_artifact_payload(
            json!({"title": "Original", "content": "old", "summary": "s"}),
            &json!({"unexpected": "metadata"}),
        );
        let error = validate_against_schema(&schema, &inert_field_merge).unwrap_err();
        assert!(error.contains("unexpected"), "{error}");
    }

    #[test]
    fn transformed_text_patch_validates_against_plan_schema() {
        let schema: serde_json::Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../resources/schemas/plan.json"
        )))
        .unwrap();
        let base = json!({"title": "Original", "content": "old", "summary": "s"});
        let patched =
            apply_artifact_patch(base, &json!({"old_string": "old", "new_string": "new"})).unwrap();

        validate_against_schema(&schema, &patched).unwrap();
        assert_eq!(
            patched,
            json!({"title": "Original", "content": "new", "summary": "s"})
        );
    }

    #[test]
    fn patch_adds_new_keys() {
        let merged = merge_artifact_payload(json!({"title": "t"}), &json!({"content": "c"}));
        assert_eq!(merged["title"], "t");
        assert_eq!(merged["content"], "c");
    }

    #[test]
    fn non_object_base_yields_patch() {
        let merged = merge_artifact_payload(json!("scalar"), &json!({"content": "c"}));
        assert_eq!(merged, json!({"content": "c"}));
    }

    // --- Per-(job, output_name) version chains (CAIRN-1942) ---

    async fn seed_artifact_job(db: &crate::storage::LocalDb) -> String {
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
                     VALUES ('p-art', 'default', 'Art', 'ART', '/tmp/art', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO jobs (id, project_id, status, created_at, updated_at)
                     VALUES ('job-art', 'p-art', 'running', 1, 1)",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
        "job-art".to_string()
    }

    /// Store a fresh version addressing `name` (used as both identity and type),
    /// exercising the same per-name chain logic `store_artifact` drives.
    async fn store_named_version(
        db: &crate::storage::LocalDb,
        job_id: &str,
        name: &str,
        data: &str,
    ) -> super::StoredArtifact {
        let job_id = job_id.to_string();
        let name = name.to_string();
        let data = data.to_string();
        db.write(|conn| {
            let job_id = job_id.clone();
            let name = name.clone();
            let data = data.clone();
            Box::pin(async move {
                super::insert_next_artifact_version(conn, &job_id, &name, &name, &data, false).await
            })
        })
        .await
        .unwrap()
    }

    async fn latest_version_for_name(
        db: &crate::storage::LocalDb,
        job_id: &str,
        name: &str,
    ) -> Option<i64> {
        use crate::storage::RowExt;
        let job_id = job_id.to_string();
        let name = name.to_string();
        db.read(|conn| {
            let job_id = job_id.clone();
            let name = name.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT version FROM artifacts WHERE job_id = ?1 AND output_name = ?2 ORDER BY version DESC LIMIT 1",
                        (job_id.as_str(), name.as_str()),
                    )
                    .await?;
                rows.next().await?.map(|row| row.i64(0)).transpose()
            })
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn version_chains_are_independent_per_output_name() {
        let db = crate::storage::migrated_test_db("artifact-chains.turso.db").await;
        let job = seed_artifact_job(&db).await;

        assert_eq!(
            store_named_version(&db, &job, "plan", "{}").await.version,
            1
        );
        assert_eq!(
            store_named_version(&db, &job, "notes", "{}").await.version,
            1
        );
        assert_eq!(
            store_named_version(&db, &job, "plan", "{}").await.version,
            2
        );
        assert_eq!(
            store_named_version(&db, &job, "notes", "{}").await.version,
            2
        );
        assert_eq!(
            store_named_version(&db, &job, "plan", "{}").await.version,
            3
        );

        // Each name advances on its own; a write to one never bumps the other.
        assert_eq!(latest_version_for_name(&db, &job, "plan").await, Some(3));
        assert_eq!(latest_version_for_name(&db, &job, "notes").await, Some(2));
    }

    #[tokio::test]
    async fn parent_links_stay_within_one_name_chain() {
        let db = crate::storage::migrated_test_db("artifact-parents.turso.db").await;
        let job = seed_artifact_job(&db).await;

        let plan_v1 = store_named_version(&db, &job, "plan", "{}").await;
        assert!(plan_v1.parent_version_id.is_none());
        // An unrelated name interleaves; it begins its own chain at v1.
        let notes_v1 = store_named_version(&db, &job, "notes", "{}").await;
        assert!(notes_v1.parent_version_id.is_none());
        // plan v2 parents to plan v1, never to the interleaved notes write.
        let plan_v2 = store_named_version(&db, &job, "plan", "{}").await;
        assert_eq!(plan_v2.version, 2);
        assert_eq!(
            plan_v2.parent_version_id.as_deref(),
            Some(plan_v1.artifact_id.as_str())
        );
    }

    #[tokio::test]
    async fn single_name_chain_increments_like_before() {
        let db = crate::storage::migrated_test_db("artifact-single.turso.db").await;
        let job = seed_artifact_job(&db).await;

        let v1 = store_named_version(&db, &job, "plan", "{}").await;
        let v2 = store_named_version(&db, &job, "plan", "{}").await;
        let v3 = store_named_version(&db, &job, "plan", "{}").await;
        assert_eq!((v1.version, v2.version, v3.version), (1, 2, 3));
        assert_eq!(
            v3.parent_version_id.as_deref(),
            Some(v2.artifact_id.as_str())
        );
    }
}

#[cfg(test)]
mod capture_tests {
    //! Server-side capture of a schema-constrained call's native structured
    //! output as its return artifact (CAIRN-2505).
    use crate::db::DbState;
    use crate::orchestrator::{Orchestrator, OrchestratorBuilder};
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{LocalDb, MigrationRunner, RowExt, SearchIndex, TURSO_MIGRATIONS};
    use cairn_db::turso::params;
    use serde_json::json;
    use std::sync::Arc;

    async fn orch_with_db() -> (Orchestrator, Arc<LocalDb>) {
        let local = Arc::new(
            LocalDb::open(tempfile::tempdir().unwrap().keep().join("capture.db"))
                .await
                .unwrap(),
        );
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&local)
            .await
            .unwrap();
        let search =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        let db = Arc::new(DbState::new(local.clone(), search));
        let orch = OrchestratorBuilder::new(
            db,
            Arc::new(TestServicesBuilder::new().build()),
            tempfile::tempdir().unwrap().keep(),
        )
        .build();
        (orch, local)
    }

    /// A call contract whose custom schema requires a numeric `score`.
    fn score_contract() -> String {
        serde_json::to_string(&crate::models::DelegatedOutputContract {
            schema_type: crate::models::OutputSchema::Custom(json!({
                "type": "object",
                "properties": {"score": {"type": "number"}},
                "required": ["score"],
                "additionalProperties": false
            })),
            tool_name: None,
            description: None,
        })
        .unwrap()
    }

    /// Seed a node-less call (job carrying a persisted `output_contract` + NULL
    /// recipe_node_id) plus its run. A NULL `contract` models a non-call run.
    async fn seed_call(db: &LocalDb, job_id: &str, run_id: &str, contract: Option<String>) {
        let job_id = job_id.to_string();
        let run_id = run_id.to_string();
        db.write(|conn| {
            let job_id = job_id.clone();
            let run_id = run_id.clone();
            let contract = contract.clone();
            Box::pin(async move {
                conn.execute("INSERT OR IGNORE INTO workspaces (id, name, created_at, updated_at) VALUES ('w','W',1,1)", ()).await?;
                conn.execute("INSERT OR IGNORE INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p','w','P','PRJ','/tmp/p',1,1)", ()).await?;
                conn.execute("INSERT OR IGNORE INTO issues (id, project_id, number, title, status, created_at, updated_at) VALUES ('i','p',7,'T','active',1,1)", ()).await?;
                conn.execute("INSERT OR IGNORE INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq) VALUES ('e','recipe','i','p','running',1,1)", ()).await?;
                conn.execute(
                    "INSERT INTO jobs (id, execution_id, issue_id, project_id, status, agent_config_id, output_contract, created_at, updated_at) VALUES (?1,'e','i','p','running','web-researcher',?2,1,1)",
                    params![job_id.as_str(), contract.as_deref()],
                ).await?;
                conn.execute(
                    "INSERT INTO runs (id, job_id, issue_id, status, created_at, updated_at) VALUES (?1,?2,'i','live',1,1)",
                    params![run_id.as_str(), job_id.as_str()],
                ).await?;
                Ok(())
            })
        }).await.unwrap();
    }

    async fn artifact_count(db: &LocalDb, job_id: &str) -> i64 {
        let job_id = job_id.to_string();
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT COUNT(*) FROM artifacts WHERE job_id = ?1",
                        (job_id.as_str(),),
                    )
                    .await?;
                let row = rows.next().await?.unwrap();
                row.i64(0)
            })
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn captures_conforming_output_as_return_artifact() {
        let (orch, db) = orch_with_db().await;
        seed_call(&db, "j-call", "r-call", Some(score_contract())).await;

        assert!(
            super::capture_call_structured_output(&orch, "r-call", json!({"score": 7}))
                .await
                .unwrap(),
            "a fresh conforming result is captured"
        );
        assert_eq!(artifact_count(&db, "j-call").await, 1);

        // Idempotent: once the artifact exists, capture is a no-op (the model's
        // own write, if any, is never clobbered and no duplicate is stored).
        assert!(
            !super::capture_call_structured_output(&orch, "r-call", json!({"score": 9}))
                .await
                .unwrap(),
            "capture is a no-op once the artifact exists"
        );
        assert_eq!(artifact_count(&db, "j-call").await, 1);
    }

    #[tokio::test]
    async fn rejects_non_conforming_output_without_storing() {
        let (orch, db) = orch_with_db().await;
        seed_call(&db, "j-bad", "r-bad", Some(score_contract())).await;

        // Silent provider non-conformance surfaces as a loud failure, never data.
        assert!(
            super::capture_call_structured_output(&orch, "r-bad", json!({"nope": true}))
                .await
                .is_err()
        );
        assert_eq!(artifact_count(&db, "j-bad").await, 0);
    }

    #[tokio::test]
    async fn no_op_when_run_carries_no_output_contract() {
        let (orch, db) = orch_with_db().await;
        seed_call(&db, "j-node", "r-node", None).await;

        // A run with no persisted output contract (a node, not a call) is left
        // to its own artifact-write flow — unchanged.
        assert!(
            !super::capture_call_structured_output(&orch, "r-node", json!({"score": 7}))
                .await
                .unwrap()
        );
        assert_eq!(artifact_count(&db, "j-node").await, 0);
    }
}
