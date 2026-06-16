//! Action resource mutations (create / patch / delete via `write`).
//!
//! DB-backed via `action_configs::queries`, so this mirrors the memories
//! mutations rather than the file-backed recipes/agents. Workspace actions use
//! the `default` workspace id (matching the actions read collection); project
//! actions resolve their project id from the URI. Built-in actions are
//! read-only — the underlying queries reject patch/delete on them.

use crate::action_configs::queries as action_queries;
use crate::mcp::handlers::run_context;
use crate::models::{CreateActionConfig, UpdateActionConfig};
use crate::orchestrator::Orchestrator;

/// Workspace id used for workspace-scoped action definitions (matches the read side).
const WORKSPACE_ID: &str = "default";

fn parse_schema(
    payload: &serde_json::Value,
    key: &str,
    alias: &str,
) -> Result<Option<serde_json::Value>, String> {
    match super::payload_value(payload, key, &[alias]) {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(value @ serde_json::Value::Object(_)) => Ok(Some(value.clone())),
        Some(_) => Err(format!("payload.{key} must be a JSON object")),
    }
}

fn emit_change(orch: &Orchestrator, action: &str) {
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "action_configs", "action": action}),
    );
}

pub(super) async fn apply_action_create(
    orch: &Orchestrator,
    payload: &serde_json::Value,
    explicit_project: Option<&str>,
) -> Result<String, String> {
    let name = super::payload_trimmed_non_empty_str(payload, "name", &[])
        .ok_or("payload.name is required and must be a non-empty string")?
        .to_string();
    let command_template =
        super::payload_trimmed_non_empty_str(payload, "commandTemplate", &["command_template"])
            .ok_or("payload.commandTemplate is required and must be a non-empty string")?
            .to_string();
    let description = payload
        .get("description")
        .and_then(|v| v.as_str())
        .map(ToOwned::to_owned);
    let input_schema = parse_schema(payload, "inputSchema", "input_schema")?;
    let output_schema = parse_schema(payload, "outputSchema", "output_schema")?;

    let (workspace_id, project_id) = match explicit_project {
        Some(project) => (
            None,
            Some(run_context::project_id_by_key(&orch.db.local, project).await?),
        ),
        None => (Some(WORKSPACE_ID.to_string()), None),
    };

    let input = CreateActionConfig {
        name,
        description,
        command_template,
        input_schema,
        output_schema,
        workspace_id,
        project_id,
    };
    let action = action_queries::create_action_config(&orch.db.local, input).await?;
    emit_change(orch, "insert");
    Ok(format!("Created action '{}' ({})", action.name, action.id))
}

async fn require_scoped(
    orch: &Orchestrator,
    action_id: &str,
    explicit_project: Option<&str>,
) -> Result<(), String> {
    let action = action_queries::get_action_config(&orch.db.local, action_id)
        .await?
        .ok_or_else(|| not_found(action_id, explicit_project))?;
    let in_scope = match explicit_project {
        Some(_) => action.project_id.is_some(),
        None => true,
    };
    if !in_scope {
        return Err(not_found(action_id, explicit_project));
    }
    Ok(())
}

pub(super) async fn apply_action_patch(
    orch: &Orchestrator,
    payload: &serde_json::Value,
    action_id: &str,
    explicit_project: Option<&str>,
) -> Result<String, String> {
    require_scoped(orch, action_id, explicit_project).await?;

    let input = UpdateActionConfig {
        name: super::payload_str(payload, "name", &[]).map(ToOwned::to_owned),
        description: super::payload_str(payload, "description", &[]).map(ToOwned::to_owned),
        command_template: super::payload_str(payload, "commandTemplate", &["command_template"])
            .map(ToOwned::to_owned),
        input_schema: parse_schema(payload, "inputSchema", "input_schema")?,
        output_schema: parse_schema(payload, "outputSchema", "output_schema")?,
    };
    action_queries::update_action_config(&orch.db.local, action_id, input).await?;
    emit_change(orch, "update");
    Ok(format!("Updated action '{action_id}'"))
}

pub(super) async fn apply_action_delete(
    orch: &Orchestrator,
    action_id: &str,
    explicit_project: Option<&str>,
) -> Result<String, String> {
    require_scoped(orch, action_id, explicit_project).await?;
    action_queries::delete_action_config(&orch.db.local, action_id).await?;
    emit_change(orch, "delete");
    Ok(format!("Deleted action '{action_id}'"))
}

fn not_found(action_id: &str, explicit_project: Option<&str>) -> String {
    match explicit_project {
        Some(project) => format!(
            "Action not found in project {}: {action_id}",
            project.to_uppercase()
        ),
        None => format!("Action not found: {action_id}"),
    }
}
