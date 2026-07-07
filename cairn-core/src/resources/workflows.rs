//! Workflow read surface (CAIRN-2487).
//!
//! Mirrors the recipes/agents read surface, file-backed via
//! `config::workflows`. The contextual collection lists workspace +
//! current-project workflows (project shadows workspace by id); an explicit
//! project collection is project-only. Read-only: workflows are invoked as a
//! `run` target, not mutated through resource writes.

use std::collections::BTreeMap;
use std::path::PathBuf;

use cairn_db::models::OutputSchema;

use crate::config::workflows::{self as config_workflows, FileWorkflow};
use crate::config::ConfigResult;
use crate::mcp::handlers::skills_resources::{current_run_project, project_path_by_key};
use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;
use cairn_common::uri::{build_project_workflow_uri, build_workflow_uri};

fn scope_label(workflow: &FileWorkflow) -> &'static str {
    if workflow.is_project_scoped {
        "project"
    } else {
        "workspace"
    }
}

/// Canonical URI for a workflow: project-scoped when it lives in a project.
fn workflow_link(workflow: &FileWorkflow, project_key: Option<&str>) -> String {
    if workflow.is_project_scoped {
        match project_key {
            Some(project) => build_project_workflow_uri(project, &workflow.id),
            None => build_workflow_uri(&workflow.id),
        }
    } else {
        build_workflow_uri(&workflow.id)
    }
}

/// Resolve the project key + repo path for the requested scope. Mirrors agents.
async fn resolve_scope(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    explicit_project: Option<&str>,
) -> Result<(Option<String>, Option<PathBuf>), String> {
    if let Some(project) = explicit_project {
        let path = project_path_by_key(orch, project).await?;
        Ok((Some(project.to_uppercase()), Some(path)))
    } else {
        match current_run_project(orch, request).await {
            Some((key, path)) => Ok((Some(key), path)),
            None => Ok((None, None)),
        }
    }
}

/// Render the workflows collection (project shadows workspace by id).
pub(crate) async fn read_workflows_collection(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    explicit_project: Option<&str>,
) -> String {
    let (project_key, project_path) = match resolve_scope(orch, request, explicit_project).await {
        Ok(scope) => scope,
        Err(e) => return e,
    };

    let workflows =
        match config_workflows::list_workflows(&orch.config_dir, project_path.as_deref()) {
            Ok(workflows) => workflows,
            Err(e) => return format!("Error listing workflows: {e}"),
        };

    let mut by_id: BTreeMap<String, FileWorkflow> = BTreeMap::new();
    for result in workflows {
        if let ConfigResult::Ok(workflow) = result {
            // config_root_subdirs yields project first, so keep the first
            // occurrence for each id to let project workflows shadow workspace.
            by_id.entry(workflow.id.clone()).or_insert(workflow);
        }
    }

    let header = match project_key.as_deref() {
        Some(key) => format!("# Workflows — {key} context\n\n"),
        None => "# Workflows — workspace\n\n".to_string(),
    };
    let mut out = header;
    out.push_str(&format!("{} workflow(s)\n\n", by_id.len()));

    if by_id.is_empty() {
        out.push_str("No workflows found.\n\n");
    } else {
        for workflow in by_id.values() {
            out.push_str(&format!(
                "- [{}]({}) [{}] — {}\n",
                workflow.id,
                workflow_link(workflow, project_key.as_deref()),
                scope_label(workflow),
                workflow.name,
            ));
        }
        out.push('\n');
    }

    out
}

/// Render a single workflow: id, name, description, script, args, output.
pub(crate) async fn read_workflow(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    workflow_id: &str,
    explicit_project: Option<&str>,
) -> String {
    let (_, project_path) = match resolve_scope(orch, request, explicit_project).await {
        Ok(scope) => scope,
        Err(e) => return e,
    };

    let workflow = match config_workflows::get_workflow(
        &orch.config_dir,
        workflow_id,
        project_path.as_deref(),
    ) {
        Ok(Some(workflow)) => workflow,
        Ok(None) => return not_found(workflow_id, explicit_project),
        Err(e) => return format!("Error loading workflow: {e}"),
    };

    // Explicit project scope is project-only: never resolve a shared workspace
    // workflow behind an explicit project URI.
    if explicit_project.is_some() && !workflow.is_project_scoped {
        return not_found(workflow_id, explicit_project);
    }

    let mut out = format!(
        "# Workflow `{}` — {}\n\n[{}]\n\n",
        workflow.id,
        workflow.name,
        scope_label(&workflow),
    );
    if !workflow.description.is_empty() {
        out.push_str(&format!("{}\n\n", workflow.description));
    }
    out.push_str(&format!("- script: {}\n", workflow.script));
    match &workflow.output {
        Some(OutputSchema::Preset(name)) => out.push_str(&format!("- output: preset `{name}`\n")),
        Some(OutputSchema::Custom(_)) => out.push_str("- output: inline JSON Schema\n"),
        None => out.push_str("- output: return (default)\n"),
    }
    out.push('\n');

    if let Some(schema) = &workflow.args_schema {
        out.push_str("## args schema\n\n```json\n");
        out.push_str(&serde_json::to_string_pretty(schema).unwrap_or_else(|_| schema.to_string()));
        out.push_str("\n```\n\n");
    } else {
        out.push_str("## args schema\n\nThis workflow declares no args.\n\n");
    }

    out
}

fn not_found(workflow_id: &str, explicit_project: Option<&str>) -> String {
    match explicit_project {
        Some(project) => format!(
            "Workflow not found in project {}: {workflow_id}",
            project.to_uppercase()
        ),
        None => format!("Workflow not found: {workflow_id}"),
    }
}
