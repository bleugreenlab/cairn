//! Execution snapshot builder.
//!
//! Builds self-contained execution snapshots from the current recipe/agent/skill
//! configuration. The snapshot captures everything needed to run an execution
//! so that changes to config files don't affect running executions.

use diesel::prelude::*;
use diesel::SqliteConnection;
use std::collections::HashMap;
use std::path::Path;

use crate::config::presets::{load_effective_presets, resolve_agent_snapshot_with_seed_backend};
use crate::config::{
    self, agents as config_agents, skills as config_skills, tools as config_tools, ConfigResult,
};
use crate::models::{
    ExecutionSnapshot, RecipeNodeType, RecipeSnapshot, SkillSnapshot, ToolSnapshot, TriggerContext,
    TriggerType,
};
use crate::schema::projects;

/// Build an execution snapshot from the current file-based config state.
///
/// This captures the complete recipe configuration at execution time, including:
/// - The recipe with all nodes and edges
/// - All agents referenced by agent nodes
/// - All skills (for agent access)
///
/// The snapshot is self-contained and doesn't depend on the original
/// recipe/agent/skill records existing.
#[allow(clippy::too_many_arguments)]
pub fn build_execution_snapshot(
    conn: &mut SqliteConnection,
    config_dir: &Path,
    recipe_id: &str,
    issue_id: Option<&str>,
    project_id: &str,
    trigger_type: TriggerType,
    event_payload: Option<serde_json::Value>,
    backend_seed: Option<&str>,
) -> Result<ExecutionSnapshot, String> {
    // Get project path for file-based config lookup
    let project_path: Option<std::path::PathBuf> = projects::table
        .find(project_id)
        .select(projects::repo_path)
        .first::<String>(conn)
        .ok()
        .map(std::path::PathBuf::from);

    // Load the full recipe from files (files are the source of truth)
    let recipe = config::get_recipe_from_files(config_dir, project_path.as_deref(), recipe_id)?;

    log::info!(
        "build_execution_snapshot: loaded recipe '{}' with {} nodes, {} edges",
        recipe.name,
        recipe.nodes.len(),
        recipe.edges.len()
    );

    // Build recipe snapshot from the loaded recipe
    let recipe_snapshot = RecipeSnapshot {
        id: recipe.id.clone(),
        name: recipe.name,
        description: recipe.description,
        trigger: recipe.trigger,
        nodes: recipe.nodes.clone(),
        edges: recipe.edges,
    };

    log::info!(
        "build_execution_snapshot: created snapshot with {} nodes",
        recipe_snapshot.nodes.len()
    );

    // Find all agent IDs referenced by agent nodes
    let agent_ids: Vec<String> = recipe
        .nodes
        .iter()
        .filter(|n| n.node_type == RecipeNodeType::Agent)
        .filter_map(|n| {
            n.agent_config
                .as_ref()
                .and_then(|cfg| cfg.agent_config_id.clone())
        })
        .collect();

    // Load presets config (workspace + project merged)
    let presets = load_effective_presets(config_dir, project_path.as_deref());

    // Load agents and skills from files
    let mut agents: HashMap<String, crate::models::AgentSnapshot> = HashMap::new();
    let mut skills: HashMap<String, SkillSnapshot> = HashMap::new();

    // Pre-load all skills and tools into maps for quick lookup
    let all_skills = load_all_skills(config_dir, project_path.as_deref());
    let all_tools = load_all_tools(config_dir, project_path.as_deref());

    for agent_id in agent_ids {
        // Skip if already loaded (in case same agent is used multiple times)
        if agents.contains_key(&agent_id) {
            continue;
        }

        // Load agent from files
        if let Ok(Some(file_agent)) =
            config_agents::get_agent(config_dir, &agent_id, project_path.as_deref())
        {
            // For file-based agents, skills are available globally (no per-agent attachment)
            // Include all skills in the snapshot for agent access
            for (skill_id, skill_snapshot) in &all_skills {
                if !skills.contains_key(skill_id) {
                    skills.insert(skill_id.clone(), skill_snapshot.clone());
                }
            }

            let snapshot = resolve_agent_snapshot_with_seed_backend(
                &file_agent,
                None,
                backend_seed,
                &presets,
            )?;

            agents.insert(agent_id.clone(), snapshot);
        }
    }

    // Build trigger context
    let trigger_context = TriggerContext {
        issue_id: issue_id.map(|s| s.to_string()),
        project_id: project_id.to_string(),
        trigger_type,
        event_payload,
    };

    // Create the snapshot
    let mut snapshot =
        ExecutionSnapshot::new(recipe_snapshot, agents, skills, all_tools, trigger_context);
    snapshot.presets = Some((&presets).into());

    Ok(snapshot)
}

/// Load all skills from files into a map
fn load_all_skills(
    config_dir: &Path,
    project_path: Option<&Path>,
) -> HashMap<String, SkillSnapshot> {
    let mut skills = HashMap::new();

    if let Ok(file_skills) = config_skills::list_skills(config_dir, project_path) {
        for result in file_skills {
            if let ConfigResult::Ok(skill) = result {
                skills.insert(
                    skill.id.clone(),
                    SkillSnapshot {
                        id: skill.id,
                        name: skill.name,
                        description: skill.description,
                        prompt: skill.prompt,
                        allowed_tools: skill.allowed_tools,
                    },
                );
            }
        }
    }

    skills
}

/// Load all custom tools from files into a map
fn load_all_tools(config_dir: &Path, project_path: Option<&Path>) -> HashMap<String, ToolSnapshot> {
    let mut tools = HashMap::new();

    if let Ok(file_tools) = config_tools::list_tools(config_dir, project_path) {
        for result in file_tools {
            if let ConfigResult::Ok(tool) = result {
                tools.insert(
                    tool.id.clone(),
                    ToolSnapshot {
                        id: tool.id,
                        name: tool.name,
                        description: tool.description,
                        input_schema: tool.input_schema,
                        code: tool.code,
                        required_tools: tool.required_tools,
                    },
                );
            }
        }
    }

    tools
}
