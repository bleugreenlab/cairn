//! Workflow run-target invocation (CAIRN-2487).
//!
//! A `run` item whose `target` is a workflow URI is a delegation, not a
//! subprocess: `handle_run` detects it here, resolves the workflow package,
//! validates the named args against the manifest's declared schema, and hands
//! off to [`crate::execution::delegation::spawn_workflow_packets`], which starts
//! the workflow node under the caller and suspends the caller until it finalizes.

use crate::config::workflows::{get_workflow, WorkflowBranchMode};
use crate::execution::delegation::{spawn_workflow_packets, SpawnWorkflowPacketsInput};
use crate::execution::jobs::CallBranchPolicy;
use crate::mcp::handlers::comments_artifacts::validate_against_schema;
use crate::mcp::handlers::run::{claim_batch_tool_use_id, RunItem};
use crate::mcp::handlers::skills_resources::{current_run_project, project_path_by_key};
use crate::mcp::handlers::tool_use_correlation::Claim;
use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;
use cairn_common::uri::CairnResource;

/// If any run item targets a workflow URI, return its `(project, workflow_id)`.
/// `project` is `Some` only for an explicit `cairn://p/<project>/workflows/<id>`.
pub(crate) fn detect_workflow_target(commands: &[RunItem]) -> Option<(Option<String>, String)> {
    for item in commands {
        if let Some(target) = item.target.as_deref() {
            match cairn_common::uri::parse_uri(target) {
                Some(CairnResource::Workflow { workflow_id }) => return Some((None, workflow_id)),
                Some(CairnResource::ProjectWorkflow {
                    project,
                    workflow_id,
                }) => return Some((Some(project), workflow_id)),
                _ => {}
            }
        }
    }
    None
}

/// Resolve, validate, and invoke a workflow run target. Returns the callback
/// result text (a suspend acknowledgement, a background URI, or an error).
pub(crate) async fn invoke_workflow(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    project: Option<String>,
    workflow_id: String,
    item: &RunItem,
) -> String {
    let project_path = match project.as_deref() {
        Some(key) => match project_path_by_key(orch, key).await {
            Ok(path) => Some(path),
            Err(e) => return format!("Project `{key}` not found: {e}"),
        },
        None => current_run_project(orch, request)
            .await
            .and_then(|(_, path)| path),
    };

    let workflow = match get_workflow(&orch.config_dir, &workflow_id, project_path.as_deref()) {
        Ok(Some(workflow)) => workflow,
        Ok(None) => return format!("Workflow not found: {workflow_id}"),
        Err(e) => return format!("Error loading workflow `{workflow_id}`: {e}"),
    };

    // Validate the named args against the manifest's declared schema, at
    // invocation time, before the workflow node is created.
    let args_value = item
        .payload
        .as_ref()
        .and_then(|p| p.args_json.clone())
        .unwrap_or_else(|| serde_json::json!({}));
    if !args_value.is_object() {
        return "Workflow args_json must be a JSON object.".to_string();
    }
    if let Some(schema) = workflow.args_schema.as_ref() {
        if let Err(e) = validate_against_schema(schema, &args_value) {
            return format!("Workflow `{workflow_id}` args validation failed.\n\n{e}");
        }
    }
    let args_json = args_value.to_string();

    let background = item.background.unwrap_or(false);
    let parent_tool_use_id = invoking_run_call(orch, request).await;

    let response = spawn_workflow_packets(
        orch,
        SpawnWorkflowPacketsInput {
            run_id: request.run_id.as_deref(),
            workflow_id: &workflow_id,
            script_path: workflow.script_path.clone(),
            output_schema: workflow.output.clone(),
            args_json,
            // The manifest declares whether this workflow inherits the caller's
            // durable branch coordinate. Neither mode creates a checkout.
            branch_policy: match workflow.branch {
                WorkflowBranchMode::None => CallBranchPolicy::None,
                WorkflowBranchMode::Inherit => CallBranchPolicy::Inherit,
            },
            parent_tool_use_id: parent_tool_use_id.as_deref(),
            background,
        },
    )
    .await;

    response.result
}

/// The provider `run` call this workflow invocation came from, which the
/// completion resume binds the workflow's synthetic `tool_result` to.
///
/// The MCP `tools/call` transport carries no provider tool-use id, so
/// `cairn-cmd` sends `tool_use_id: None` for every tool; only the Cairn-native
/// tool loop (`backends::http_loop`), which dispatches tools itself, populates
/// it. For every MCP-hosted agent the id therefore has to be correlated from the
/// current turn's transcript, exactly as a suspending run batch, a `waitFor`,
/// and a blocking task/question append do.
///
/// `None` is a real answer, not a failure to paper over. Inventing an id is
/// strictly worse than having none: the resume writes an answer addressed to a
/// call that does not exist, and — because a delivery was produced — suppresses
/// the visible transcript event that would otherwise have carried the result, so
/// the workflow's output disappears from the rendered transcript entirely
/// (CAIRN-3230). With `None` the caller still resumes with the result, rendered
/// as a visible continuation event.
async fn invoking_run_call(orch: &Orchestrator, request: &McpCallbackRequest) -> Option<String> {
    if let Some(id) = request.tool_use_id.clone() {
        return Some(id);
    }
    let (ctx, db) = crate::mcp::handlers::run_context::lookup_run_routed(&orch.db, request)
        .await
        .ok()?;
    let turn_id = orch.process_state.get_current_turn_id(&ctx.run_id)?;
    // A workflow target is enforced to be the sole item in its batch, so the
    // recorded call's identity is as crisp as a `waitFor`'s. The claim is still
    // exclusive and refuses a tie: this id will be used to WRITE a tool result,
    // and two indistinguishable calls cannot be told apart at this boundary.
    match claim_batch_tool_use_id(&db, &ctx.run_id, &turn_id, &request.payload).await {
        Claim::One(id) => Some(id),
        Claim::None => {
            log::warn!(
                "workflow invocation for run {} found no unanswered `run` call of its own in the transcript; its result will resume as a visible continuation event rather than under the call",
                ctx.run_id
            );
            None
        }
        Claim::Ambiguous(count) => {
            log::warn!(
                "workflow invocation for run {} matches {count} indistinguishable open `run` calls, so it cannot claim one without risking another call's answer; its result will resume as a visible continuation event",
                ctx.run_id
            );
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{LocalDb, MigrationRunner, SearchIndex, TURSO_MIGRATIONS};
    use serde_json::{json, Value};
    use std::sync::Arc;

    const RUN_ID: &str = "wf-caller-run";
    const TURN_ID: &str = "wf-caller-turn";

    /// A live caller node with an open turn: the state a workflow invocation
    /// arrives in, and the minimum a transcript claim needs to resolve.
    async fn seeded_caller() -> (Orchestrator, Arc<LocalDb>) {
        let local = Arc::new(
            LocalDb::open(tempfile::tempdir().unwrap().keep().join("workflows.db"))
                .await
                .unwrap(),
        );
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&local)
            .await
            .unwrap();
        for sql in [
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w','W',1,1)",
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p','w','P','prj','/tmp/p',1,1)",
            "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at) VALUES ('i','p',1,'T','active',1,1)",
            "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq) VALUES ('e','recipe','i','p','running',1,1)",
            "INSERT INTO jobs (id, execution_id, issue_id, project_id, node_name, agent_config_id, status, uri_segment, created_at, updated_at) VALUES ('j','e','i','p','Builder','agent-1','running','builder',1,1)",
            "INSERT INTO sessions (id, job_id, status, created_at, updated_at) VALUES ('s','j','open',1,1)",
        ] {
            local.execute(sql, ()).await.unwrap();
        }
        local
            .execute(
                "INSERT INTO runs (id, project_id, issue_id, job_id, session_id, status, created_at, updated_at) VALUES (?1,'p','i','j','s','live',1,1)",
                (RUN_ID,),
            )
            .await
            .unwrap();
        local
            .execute(
                "INSERT INTO turns (id, session_id, run_id, job_id, sequence, state, start_reason, created_at, updated_at) VALUES (?1,'s',?2,'j',1,'running','initial',1,1)",
                (TURN_ID, RUN_ID),
            )
            .await
            .unwrap();
        local
            .execute(
                "UPDATE jobs SET current_session_id = 's', current_turn_id = ?1 WHERE id = 'j'",
                (TURN_ID,),
            )
            .await
            .unwrap();

        let search =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        let orch = OrchestratorBuilder::new(
            Arc::new(DbState::new(local.clone(), search)),
            Arc::new(TestServicesBuilder::new().build()),
            tempfile::tempdir().unwrap().keep(),
        )
        .build();

        // The live process carrying the turn. `get_current_turn_id` reads it, and
        // without it the correlation bails before ever consulting the transcript
        // — which would make every "refuses to claim" assertion below pass for
        // the wrong reason.
        {
            let mut processes = orch.process_state.processes.lock().unwrap();
            processes.register(
                RUN_ID.to_string(),
                crate::agent_process::process::RunHandle::new(
                    Arc::new(std::sync::Mutex::new(None)),
                    Arc::new(std::sync::Mutex::new(None)),
                    Some("s".to_string()),
                    Some("j".to_string()),
                ),
            );
        }
        assert!(
            orch.process_state.begin_turn(RUN_ID, TURN_ID),
            "the seeded run must carry an open turn"
        );
        assert_eq!(
            orch.process_state.get_current_turn_id(RUN_ID).as_deref(),
            Some(TURN_ID)
        );
        (orch, local)
    }

    /// The batch an agent sends to start a workflow. The recorded tool call and
    /// the callback carry the same one — that is what makes them correlatable.
    fn workflow_batch() -> Value {
        json!({
            "commands": [{
                "target": "cairn://workflows/deep-research",
                "description": "Deep research: uniform-interface design lineage",
                "payload": {"args_json": {"maxSources": 12, "question": "why"}}
            }]
        })
    }

    /// Record the assistant event carrying this turn's `run` tool call(s), the
    /// way a live transcript does.
    async fn record_run_calls(db: &LocalDb, calls: &[(&str, Value)]) {
        let tool_uses: Vec<Value> = calls
            .iter()
            .map(|(id, input)| json!({"id": id, "name": "mcp__cairn__run", "input": input}))
            .collect();
        db.execute(
            "INSERT INTO events(id,run_id,turn_id,sequence,timestamp,event_type,data,created_at)
             VALUES('ev',?1,?2,1,1,'assistant',?3,1)",
            (
                RUN_ID,
                TURN_ID,
                json!({"toolUses": tool_uses}).to_string().as_str(),
            ),
        )
        .await
        .unwrap();
    }

    /// A callback exactly as the MCP transport delivers one: `tool_use_id: None`,
    /// because `tools/call` carries no provider tool-use id. This is the only
    /// shape production ever produces for an MCP-hosted agent.
    fn mcp_request(payload: Value) -> McpCallbackRequest {
        McpCallbackRequest {
            thread_id: None,
            cwd: "/tmp/wt".to_string(),
            run_id: Some(RUN_ID.to_string()),
            tool: "run".to_string(),
            payload,
            tool_use_id: None,
        }
    }

    /// The whole defect in one assertion: over the MCP transport the invocation
    /// must name the provider call the agent actually made. Before CAIRN-3230 it
    /// minted a UUID here, and the completion then wrote its `tool_result`
    /// against an id no assistant event ever carried.
    #[tokio::test]
    async fn an_mcp_hosted_invocation_claims_the_run_call_that_issued_it() {
        let (orch, db) = seeded_caller().await;
        record_run_calls(&db, &[("toolu_workflow", workflow_batch())]).await;

        assert_eq!(
            invoking_run_call(&orch, &mcp_request(workflow_batch())).await,
            Some("toolu_workflow".to_string())
        );
    }

    /// A sibling `run` call in the same assistant event is not this invocation's,
    /// so the claim must pick by contents rather than by recency.
    #[tokio::test]
    async fn a_sibling_run_call_in_the_same_event_is_not_claimed() {
        let (orch, db) = seeded_caller().await;
        record_run_calls(
            &db,
            &[
                ("toolu_workflow", workflow_batch()),
                (
                    "toolu_sibling",
                    json!({"commands": [{"command": "bun run check:rust"}]}),
                ),
            ],
        )
        .await;

        assert_eq!(
            invoking_run_call(&orch, &mcp_request(workflow_batch())).await,
            Some("toolu_workflow".to_string())
        );
    }

    /// When no recorded call matches, the honest answer is `None`. A synthetic id
    /// would be delivered to a call that does not exist and would suppress the
    /// visible event carrying the result; `None` keeps the result visible.
    #[tokio::test]
    async fn an_uncorrelatable_invocation_yields_no_id_rather_than_a_synthetic_one() {
        let (orch, db) = seeded_caller().await;
        record_run_calls(
            &db,
            &[(
                "toolu_unrelated",
                json!({"commands": [{"command": "echo hi"}]}),
            )],
        )
        .await;

        assert_eq!(
            invoking_run_call(&orch, &mcp_request(workflow_batch())).await,
            None
        );
    }

    /// Two byte-identical workflow invocations in one assistant event cannot be
    /// told apart at this boundary, so neither is claimed: guessing would answer
    /// one call with the other's workflow result.
    #[tokio::test]
    async fn indistinguishable_concurrent_invocations_are_refused_not_guessed() {
        let (orch, db) = seeded_caller().await;
        record_run_calls(
            &db,
            &[
                ("toolu_first", workflow_batch()),
                ("toolu_second", workflow_batch()),
            ],
        )
        .await;

        assert_eq!(
            invoking_run_call(&orch, &mcp_request(workflow_batch())).await,
            None
        );
    }

    /// The other transport: the Cairn-native tool loop dispatches tools itself, so
    /// it knows the provider id and supplies it. That id is authoritative and is
    /// used without consulting the transcript at all.
    #[tokio::test]
    async fn the_native_tool_loops_own_id_is_used_verbatim() {
        let (orch, _db) = seeded_caller().await;
        let request = McpCallbackRequest {
            tool_use_id: Some("toolu_native_loop".to_string()),
            ..mcp_request(workflow_batch())
        };

        assert_eq!(
            invoking_run_call(&orch, &request).await,
            Some("toolu_native_loop".to_string())
        );
    }
}
