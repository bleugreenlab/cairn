//! Workflow run-target invocation (CAIRN-2487).
//!
//! A `run` item whose `target` is a workflow URI is a delegation, not a
//! subprocess: `handle_run` detects it here, resolves the workflow package,
//! validates the named args against the manifest's declared schema, and hands
//! off to [`crate::execution::delegation::spawn_workflow_packets`], which starts
//! the workflow node under the caller and suspends the caller until it finalizes.

use crate::config::workflows::get_workflow;
use crate::execution::delegation::{spawn_workflow_packets, SpawnWorkflowPacketsInput};
use crate::execution::jobs::CallWorktree;
use crate::mcp::handlers::comments_artifacts::validate_against_schema;
use crate::mcp::handlers::run::RunItem;
use crate::mcp::handlers::skills_resources::{current_run_project, project_path_by_key};
use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;
use cairn_common::uri::CairnResource;
use uuid::Uuid;

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
    let group_id = request
        .tool_use_id
        .clone()
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    let response = spawn_workflow_packets(
        orch,
        SpawnWorkflowPacketsInput {
            run_id: request.run_id.as_deref(),
            cwd: &request.cwd,
            workflow_id: &workflow_id,
            script_path: workflow.script_path.clone(),
            output_schema: workflow.output.clone(),
            args_json,
            // A workflow runs in its own scratch dir with no caller-tree binding.
            worktree: CallWorktree::None,
            group_id: &group_id,
            parent_tool_use_id: request.tool_use_id.as_deref(),
            background,
        },
    )
    .await;

    response.result
}
