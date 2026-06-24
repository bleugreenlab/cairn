//! Action resource reads: the `cairn://actions` collection and single actions.
//!
//! Action definitions are DB-backed (`action_configs`), so this mirrors the
//! memories read surface rather than the file-backed recipes/agents. The
//! contextual collection lists user-defined workspace + current-project actions
//! (built-in PR/issue actions are internal and excluded). Workspace actions use
//! the `default` workspace id, matching what `change create` writes.

use crate::action_configs::queries as action_queries;
use crate::mcp::handlers::run_context;
use crate::mcp::types::McpCallbackRequest;
use crate::models::ActionConfig;
use crate::orchestrator::Orchestrator;
use cairn_common::uri::{build_action_uri, build_project_action_uri};

/// Workspace id used for workspace-scoped action definitions.
pub(crate) const WORKSPACE_ID: &str = "default";

fn scope_label(action: &ActionConfig) -> &'static str {
    if action.project_id.is_some() {
        "project"
    } else {
        "workspace"
    }
}

fn action_link(action: &ActionConfig, project_key: Option<&str>) -> String {
    match (action.project_id.as_deref(), project_key) {
        (Some(_), Some(key)) => build_project_action_uri(key, &action.id),
        _ => build_action_uri(&action.id),
    }
}

/// Render the actions collection (workspace + current/explicit project).
pub(crate) async fn read_actions_collection(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    explicit_project: Option<&str>,
) -> String {
    let (project_id, project_key): (Option<String>, Option<String>) = match explicit_project {
        Some(project) => match run_context::project_id_by_key(&orch.db.local, project).await {
            Ok(id) => (Some(id), Some(project.to_uppercase())),
            Err(e) => return e,
        },
        None => match run_context::lookup_run(&orch.db.local, request).await {
            Ok(ctx) => (Some(ctx.project_id), Some(ctx.project_key)),
            Err(_) => (None, None),
        },
    };

    // Agent-facing effective set: workspace actions inherited, project actions
    // shadowing by name, per-project disabled names removed. Without a project
    // context, only workspace actions are visible.
    let actions: Vec<ActionConfig> = match project_id.as_deref() {
        Some(project_id) => {
            match action_queries::list_action_configs_for_context(&orch.db.local, project_id, false)
                .await
            {
                Ok(list) => list,
                Err(e) => return format!("Error listing actions: {e}"),
            }
        }
        None => match action_queries::list_action_configs(
            &orch.db.local,
            Some(WORKSPACE_ID),
            None,
            false,
        )
        .await
        {
            Ok(list) => list,
            Err(e) => return format!("Error listing actions: {e}"),
        },
    };

    let header = match project_key.as_deref() {
        Some(key) => format!("# Actions — {key} context\n\n"),
        None => "# Actions — workspace\n\n".to_string(),
    };
    let mut out = header;
    out.push_str(&format!("{} action(s)\n\n", actions.len()));

    if actions.is_empty() {
        out.push_str("No actions found.\n\n");
    } else {
        for action in &actions {
            out.push_str(&format!(
                "- [{}]({}) [{}] — {}\n",
                action.name,
                action_link(action, project_key.as_deref()),
                scope_label(action),
                action.description,
            ));
        }
        out.push('\n');
    }

    out
}

/// Render a single action: name, description, command template, schemas, actions.
pub(crate) async fn read_action(
    orch: &Orchestrator,
    action_id: &str,
    explicit_project: Option<&str>,
) -> String {
    let action = match action_queries::get_action_config(&orch.db.local, action_id).await {
        Ok(Some(action)) => action,
        Ok(None) => return not_found(action_id, explicit_project),
        Err(e) => return format!("Error loading action: {e}"),
    };

    // Explicit project scope is project-only.
    if explicit_project.is_some() && action.project_id.is_none() {
        return not_found(action_id, explicit_project);
    }

    let mut out = format!(
        "# Action `{}` — {}\n\n[{}]{}\n\n",
        action.id,
        action.name,
        scope_label(&action),
        if action.is_builtin {
            " [built-in, read-only]"
        } else {
            ""
        },
    );
    if !action.description.is_empty() {
        out.push_str(&format!("{}\n\n", action.description));
    }
    if let Some(template) = action.command_template.as_deref().filter(|t| !t.is_empty()) {
        out.push_str("## command template\n");
        out.push_str("```\n");
        out.push_str(template);
        out.push_str("\n```\n\n");
    }
    if let Some(schema) = &action.input_schema {
        out.push_str("## input schema\n");
        out.push_str("```json\n");
        out.push_str(&serde_json::to_string_pretty(schema).unwrap_or_default());
        out.push_str("\n```\n\n");
    }

    out
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
