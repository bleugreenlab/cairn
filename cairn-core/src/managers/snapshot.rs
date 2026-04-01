//! Manager execution snapshots.
//!
//! Managers get execution records for config storage (tier, prompt, tools, skills).
//! The execution provides a self-contained snapshot that survives config file changes.

use diesel::prelude::*;
use diesel::SqliteConnection;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

use crate::config::presets::{
    load_effective_presets, resolve_agent_snapshot, resolve_snapshot_agent_runtime, PresetsConfig,
};
use crate::config::{
    agents as config_agents, skills as config_skills, tools as config_tools, ConfigResult,
};
use crate::diesel_models::NewExecution;
use crate::models::{
    AgentNodeConfig, ApprovalPolicy, ExecutionSnapshot, FilesystemScope, Model, NodePosition,
    RecipeNode, RecipeNodeType, RecipeSnapshot, RecipeTrigger, SkillSnapshot, ToolSnapshot,
    TriggerContext, TriggerType,
};
use crate::schema::{executions, jobs, managers, projects};

/// Updates to apply to a manager's snapshot config.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ManagerConfigUpdate {
    #[serde(alias = "model")]
    pub tier: Option<Option<Model>>,
    #[serde(rename = "backend", alias = "backendPreference")]
    pub backend_preference: Option<Option<String>>,
    pub prompt: Option<String>,
    pub tools: Option<Vec<String>>,
    pub disallowed_tools: Option<Vec<String>>,
    pub skills: Option<Vec<String>>,
    pub approval_policy: Option<ApprovalPolicy>,
    pub filesystem_scope: Option<FilesystemScope>,
}

/// Build an execution snapshot for a manager.
///
/// Creates a self-contained snapshot with the agent config, all skills, and tools.
pub fn build_manager_snapshot(
    config_dir: &Path,
    project_path: Option<&Path>,
    project_id: &str,
    agent_config_id: &str,
    model_override: Option<&Model>,
) -> Result<ExecutionSnapshot, String> {
    // Load agent from files
    let file_agent = config_agents::get_agent(config_dir, agent_config_id, project_path)?
        .ok_or_else(|| format!("Agent config '{}' not found", agent_config_id))?;

    // Load presets and resolve agent snapshot
    let presets = load_effective_presets(config_dir, project_path);
    let tier_override = model_override.map(|m: &Model| m.as_str());
    let agent_snapshot = resolve_agent_snapshot(&file_agent, tier_override, &presets)?;

    // Load all skills and tools
    let all_skills = load_all_skills(config_dir, project_path);
    let all_tools = load_all_tools(config_dir, project_path);

    let mut agents = HashMap::new();
    agents.insert(agent_config_id.to_string(), agent_snapshot);

    // Build synthetic recipe snapshot (manager = single agent node)
    let recipe_snapshot = RecipeSnapshot {
        id: "__manager".to_string(),
        name: "Manager".to_string(),
        description: None,
        trigger: RecipeTrigger::Manual,
        nodes: vec![RecipeNode {
            id: "__manager_node".to_string(),
            node_type: RecipeNodeType::Agent,
            name: "Manager".to_string(),
            position: NodePosition { x: 0.0, y: 0.0 },
            parent_id: None,
            trigger_config: None,
            agent_config: Some(AgentNodeConfig {
                agent_config_id: Some(agent_config_id.to_string()),
                checkpoint: None,
                output_schema: None,
                git_config: None,
            }),
            action_config: None,
            checkpoint_config: None,
            artifact_config: None,
            condition_config: None,
            context_config: None,
        }],
        edges: vec![],
    };

    let trigger_context = TriggerContext {
        issue_id: None,
        project_id: project_id.to_string(),
        trigger_type: TriggerType::Manual,

        event_payload: None,
    };

    let mut snapshot = ExecutionSnapshot::new(
        recipe_snapshot,
        agents,
        all_skills,
        all_tools,
        trigger_context,
    );
    snapshot.presets = Some((&presets).into());

    Ok(snapshot)
}

/// Create an execution record for a manager and return the execution ID.
pub fn create_manager_execution(
    conn: &mut SqliteConnection,
    config_dir: &Path,
    project_id: &str,
    agent_config_id: &str,
    model: Option<&Model>,
) -> Result<String, String> {
    let project_path: Option<std::path::PathBuf> = projects::table
        .find(project_id)
        .select(projects::repo_path)
        .first::<String>(conn)
        .ok()
        .map(std::path::PathBuf::from);

    let snapshot = build_manager_snapshot(
        config_dir,
        project_path.as_deref(),
        project_id,
        agent_config_id,
        model,
    )?;

    let snapshot_json = snapshot.to_json()?;
    let execution_id = uuid::Uuid::new_v4().to_string();
    let now = chrono::Utc::now().timestamp() as i32;

    let new_execution = NewExecution {
        id: &execution_id,
        recipe_id: "__manager",
        issue_id: None,
        project_id: Some(project_id),
        status: "running",
        started_at: now,
        completed_at: None,
        snapshot: Some(&snapshot_json),
        seq: None,
        initiator_sub: None,
        initiator_auth_mode: None,
        initiator_org_id: None,
        triggered_by: "manual",
    };

    diesel::insert_into(executions::table)
        .values(&new_execution)
        .execute(conn)
        .map_err(|e| format!("Failed to create manager execution: {}", e))?;

    Ok(execution_id)
}

/// Update the agent config within a manager's execution snapshot.
pub fn update_manager_snapshot(
    conn: &mut SqliteConnection,
    config_dir: &Path,
    execution_id: &str,
    agent_config_id: &str,
    updates: ManagerConfigUpdate,
) -> Result<(), String> {
    log::info!(
        "update_manager_snapshot: exec={}, agent={}, has_prompt={}, has_tier={}, has_tools={}, has_skills={}",
        execution_id, agent_config_id,
        updates.prompt.is_some(), updates.tier.is_some(),
        updates.tools.is_some(), updates.skills.is_some()
    );
    // Load existing snapshot
    let snapshot_json: Option<String> = executions::table
        .find(execution_id)
        .select(executions::snapshot)
        .first(conn)
        .optional()
        .map_err(|e| format!("Failed to load execution: {}", e))?
        .flatten();

    let snapshot_json = snapshot_json.ok_or_else(|| "Execution has no snapshot".to_string())?;

    let mut snapshot: ExecutionSnapshot = serde_json::from_str(&snapshot_json)
        .map_err(|e| format!("Failed to parse snapshot: {}", e))?;

    // Get the agent entry
    let agent = snapshot
        .agents
        .get_mut(agent_config_id)
        .ok_or_else(|| format!("Agent '{}' not found in snapshot", agent_config_id))?;

    let runtime_changed = updates.tier.is_some() || updates.backend_preference.is_some();

    // Apply updates
    if let Some(tier) = updates.tier {
        agent.tier = tier;
    }
    if let Some(backend_preference) = updates.backend_preference {
        agent.backend_preference = backend_preference;
    }
    if let Some(prompt) = updates.prompt {
        agent.prompt = prompt;
    }
    if let Some(tools) = updates.tools {
        agent.tools = tools;
    }
    if let Some(disallowed_tools) = updates.disallowed_tools {
        agent.disallowed_tools = Some(disallowed_tools);
    }
    if let Some(skills) = updates.skills {
        agent.skills = Some(skills);
    }
    if let Some(approval_policy) = updates.approval_policy {
        agent.approval_policy = Some(approval_policy);
    }
    if let Some(filesystem_scope) = updates.filesystem_scope {
        agent.filesystem_scope = Some(filesystem_scope);
    }

    let model_str = if runtime_changed {
        let project_id = snapshot.trigger_context.project_id.clone();
        let project_path: Option<std::path::PathBuf> = projects::table
            .find(&project_id)
            .select(projects::repo_path)
            .first::<String>(conn)
            .ok()
            .map(std::path::PathBuf::from);
        let presets = snapshot
            .presets
            .as_ref()
            .map(PresetsConfig::from)
            .unwrap_or_else(|| load_effective_presets(config_dir, project_path.as_deref()));
        resolve_snapshot_agent_runtime(agent, &presets)?;
        let model = agent
            .model
            .clone()
            .ok_or_else(|| "Resolved snapshot agent missing model".to_string())?;
        Some(Some(model.to_string()))
    } else {
        None
    };

    // Save back
    let updated_json = serde_json::to_string(&snapshot)
        .map_err(|e| format!("Failed to serialize snapshot: {}", e))?;

    diesel::update(executions::table.find(execution_id))
        .set(executions::snapshot.eq(&updated_json))
        .execute(conn)
        .map_err(|e| format!("Failed to update execution snapshot: {}", e))?;

    // If runtime changed, also update the linked job's concrete model
    if let Some(model_str) = model_str {
        diesel::update(jobs::table.filter(jobs::execution_id.eq(execution_id)))
            .set(jobs::model.eq(model_str))
            .execute(conn)
            .map_err(|e| format!("Failed to update job model: {}", e))?;
    }

    Ok(())
}

/// Ensure a manager has an execution record, creating one if needed.
///
/// Used for lazy migration of existing managers that were created before
/// execution-backed config storage was added.
pub fn ensure_manager_execution(
    conn: &mut SqliteConnection,
    config_dir: &Path,
    manager_id: &str,
    project_id: &str,
    agent_config_id: &str,
    model: Option<&Model>,
) -> Result<String, String> {
    let execution_id =
        create_manager_execution(conn, config_dir, project_id, agent_config_id, model)?;

    // Update manager.execution_id
    diesel::update(managers::table.find(manager_id))
        .set(managers::execution_id.eq(&execution_id))
        .execute(conn)
        .map_err(|e| format!("Failed to update manager execution_id: {}", e))?;

    // Update the manager's job to reference this execution
    let job_id: Option<String> = managers::table
        .find(manager_id)
        .select(managers::job_id)
        .first::<Option<String>>(conn)
        .map_err(|e| format!("Manager not found: {}", e))?;

    if let Some(jid) = job_id {
        diesel::update(jobs::table.find(&jid))
            .set(jobs::execution_id.eq(&execution_id))
            .execute(conn)
            .map_err(|e| format!("Failed to update job execution_id: {}", e))?;
    }

    Ok(execution_id)
}

/// Load all skills from files into a map.
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

/// Load all custom tools from files into a map.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::AgentSnapshot;
    use crate::services::testing::MockClock;
    use crate::test_utils::{create_test_project, test_diesel_conn};
    use tempfile::tempdir;

    #[test]
    fn test_create_manager_execution() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");

        // create_manager_execution requires a config dir with an agent file.
        // In test, the config dir won't have agents, so this should return an error.
        let config_dir = std::env::temp_dir().join("cairn_test_snapshot");
        let result = create_manager_execution(&mut conn, &config_dir, &project_id, "manager", None);

        // Expected to fail since there's no agent file in the temp dir
        assert!(result.is_err());
    }

    #[test]
    fn test_update_manager_snapshot() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");
        let execution_id = insert_test_execution(&mut conn, &project_id, "manager");

        // Update the prompt
        let config_dir = tempdir().unwrap();
        update_manager_snapshot(
            &mut conn,
            config_dir.path(),
            &execution_id,
            "manager",
            ManagerConfigUpdate {
                prompt: Some("Updated prompt".to_string()),
                ..Default::default()
            },
        )
        .unwrap();

        // Verify
        let updated_json: Option<String> = executions::table
            .find(&execution_id)
            .select(executions::snapshot)
            .first(&mut conn)
            .unwrap();

        let updated: ExecutionSnapshot = serde_json::from_str(&updated_json.unwrap()).unwrap();
        assert_eq!(
            updated.agents.get("manager").unwrap().prompt,
            "Updated prompt"
        );
    }

    #[test]
    fn test_update_manager_snapshot_tier() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TEST");
        let execution_id = insert_test_execution(&mut conn, &project_id, "manager");

        // Clear the authoring tier and ensure the snapshot still has a concrete runtime model.
        let config_dir = tempdir().unwrap();
        update_manager_snapshot(
            &mut conn,
            config_dir.path(),
            &execution_id,
            "manager",
            ManagerConfigUpdate {
                tier: Some(None),
                ..Default::default()
            },
        )
        .unwrap();

        let updated_json: Option<String> = executions::table
            .find(&execution_id)
            .select(executions::snapshot)
            .first(&mut conn)
            .unwrap();

        let updated: ExecutionSnapshot = serde_json::from_str(&updated_json.unwrap()).unwrap();
        let agent = updated.agents.get("manager").unwrap();
        assert_eq!(agent.tier, None);
        assert_eq!(agent.model, Some(Model::new(Model::SONNET)));
    }

    /// Helper: insert a fake execution with a single-agent snapshot and return the execution ID.
    fn insert_test_execution(
        conn: &mut diesel::SqliteConnection,
        project_id: &str,
        agent_id: &str,
    ) -> String {
        let execution_id = uuid::Uuid::new_v4().to_string();
        let snapshot = ExecutionSnapshot {
            recipe: RecipeSnapshot {
                id: "__manager".to_string(),
                name: "Manager".to_string(),
                description: None,
                trigger: RecipeTrigger::Manual,
                nodes: vec![],
                edges: vec![],
            },
            agents: HashMap::from([(
                agent_id.to_string(),
                AgentSnapshot {
                    id: agent_id.to_string(),
                    name: "Manager".to_string(),
                    description: "Test".to_string(),
                    prompt: "Original prompt".to_string(),
                    tools: vec!["Read".to_string()],
                    tier: Some(Model::new(Model::SONNET)),
                    backend_preference: None,
                    model: Some(Model::new(Model::SONNET)),
                    disallowed_tools: None,
                    skills: None,
                    approval_policy: None,
                    filesystem_scope: None,
                    resolved_backend: None,
                    extras: None,
                },
            )]),
            skills: HashMap::new(),
            tools: HashMap::new(),
            trigger_context: TriggerContext {
                issue_id: None,
                project_id: project_id.to_string(),
                trigger_type: TriggerType::Manual,

                event_payload: None,
            },
            presets: None,
            delegated_packets: vec![],
            created_at: 1234567890,
        };

        let snapshot_json = snapshot.to_json().unwrap();
        let now = chrono::Utc::now().timestamp() as i32;

        diesel::insert_into(executions::table)
            .values(&NewExecution {
                id: &execution_id,
                recipe_id: "__manager",
                issue_id: None,
                project_id: Some(project_id),
                status: "running",
                started_at: now,
                completed_at: None,
                snapshot: Some(&snapshot_json),
                seq: None,
                initiator_sub: None,
                initiator_auth_mode: None,
                initiator_org_id: None,
                triggered_by: "manual",
            })
            .execute(conn)
            .unwrap();

        execution_id
    }

    #[test]
    fn test_update_snapshot_tools_and_skills() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TEST");
        let exec_id = insert_test_execution(&mut conn, &project_id, "manager");

        let config_dir = tempdir().unwrap();
        update_manager_snapshot(
            &mut conn,
            config_dir.path(),
            &exec_id,
            "manager",
            ManagerConfigUpdate {
                tools: Some(vec!["Read".into(), "Write".into(), "Bash".into()]),
                skills: Some(vec!["testing".into(), "debugging".into()]),
                ..Default::default()
            },
        )
        .unwrap();

        let json: String = executions::table
            .find(&exec_id)
            .select(executions::snapshot)
            .first::<Option<String>>(&mut conn)
            .unwrap()
            .unwrap();
        let snap: ExecutionSnapshot = serde_json::from_str(&json).unwrap();
        let agent = snap.agents.get("manager").unwrap();

        assert_eq!(agent.tools, vec!["Read", "Write", "Bash"]);
        assert_eq!(
            agent.skills,
            Some(vec!["testing".into(), "debugging".into()])
        );
        // Untouched fields should be preserved
        assert_eq!(agent.prompt, "Original prompt");
        assert_eq!(agent.tier, Some(Model::new(Model::SONNET)));
        assert_eq!(agent.model, Some(Model::new(Model::SONNET)));
    }

    #[test]
    fn test_update_snapshot_disallowed_tools_and_permission_mode() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TEST");
        let exec_id = insert_test_execution(&mut conn, &project_id, "manager");
        let config_dir = tempdir().unwrap();

        update_manager_snapshot(
            &mut conn,
            config_dir.path(),
            &exec_id,
            "manager",
            ManagerConfigUpdate {
                disallowed_tools: Some(vec!["Write".into(), "Bash".into()]),
                approval_policy: Some(ApprovalPolicy::Ask),
                filesystem_scope: Some(FilesystemScope::CwdOnly),
                ..Default::default()
            },
        )
        .unwrap();

        let json: String = executions::table
            .find(&exec_id)
            .select(executions::snapshot)
            .first::<Option<String>>(&mut conn)
            .unwrap()
            .unwrap();
        let snap: ExecutionSnapshot = serde_json::from_str(&json).unwrap();
        let agent = snap.agents.get("manager").unwrap();

        assert_eq!(
            agent.disallowed_tools,
            Some(vec!["Write".into(), "Bash".into()])
        );
        assert_eq!(agent.approval_policy, Some(ApprovalPolicy::Ask));
        assert_eq!(agent.filesystem_scope, Some(FilesystemScope::CwdOnly));
    }

    #[test]
    fn test_update_snapshot_tier_change_propagates_to_job() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TEST");
        let exec_id = insert_test_execution(&mut conn, &project_id, "manager");

        // Create a job linked to this execution
        let issue_id = crate::test_utils::create_test_issue(&mut conn, &project_id, "Temp");
        let job_id = crate::test_utils::create_test_job(
            &mut conn,
            &issue_id,
            &project_id,
            "manager",
            "running",
            None,
        );
        diesel::update(jobs::table.find(&job_id))
            .set(jobs::execution_id.eq(&exec_id))
            .execute(&mut conn)
            .unwrap();

        // Update tier in snapshot
        let config_dir = tempdir().unwrap();
        update_manager_snapshot(
            &mut conn,
            config_dir.path(),
            &exec_id,
            "manager",
            ManagerConfigUpdate {
                tier: Some(Some(Model::new(Model::OPUS))),
                ..Default::default()
            },
        )
        .unwrap();

        // Job model should be updated too
        let job_model: Option<String> = jobs::table
            .find(&job_id)
            .select(jobs::model)
            .first(&mut conn)
            .unwrap();
        assert_eq!(job_model, Some(Model::new(Model::OPUS).to_string()));
    }

    #[test]
    fn test_update_snapshot_nonexistent_execution() {
        let mut conn = test_diesel_conn();
        create_test_project(&mut conn, "Test", "TEST");
        let config_dir = tempdir().unwrap();

        let result = update_manager_snapshot(
            &mut conn,
            config_dir.path(),
            "nonexistent-exec-id",
            "manager",
            ManagerConfigUpdate {
                prompt: Some("Updated".into()),
                ..Default::default()
            },
        );

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("no snapshot"));
    }

    #[test]
    fn test_update_snapshot_wrong_agent_id() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TEST");
        let exec_id = insert_test_execution(&mut conn, &project_id, "manager");
        let config_dir = tempdir().unwrap();

        let result = update_manager_snapshot(
            &mut conn,
            config_dir.path(),
            &exec_id,
            "nonexistent-agent",
            ManagerConfigUpdate {
                prompt: Some("Updated".into()),
                ..Default::default()
            },
        );

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not found in snapshot"));
    }

    #[test]
    fn test_ensure_manager_execution_links_manager_and_job() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TEST");

        // Create a manager via crud
        let mut mock_clock = MockClock::new();
        mock_clock.expect_now().returning(|| 1700000000);

        let manager = crate::managers::crud::create(
            &mut conn,
            &mock_clock,
            crate::models::CreateManager {
                project_id: project_id.clone(),
                home_project_id: None,
                scope_kind: None,
                name: "Test Manager".into(),
                branch: "mgr/test".into(),
                description: None,
                agent_config_id: Some("manager".into()),
                tier: None,
                parent_manager_id: None,
            },
        )
        .unwrap();

        // Manager starts with no execution_id
        assert!(manager.execution_id.is_none());

        // Set up a config dir with a minimal agent config
        let config_dir = tempfile::tempdir().unwrap();
        let agents_dir = config_dir.path().join("agents");
        std::fs::create_dir_all(&agents_dir).unwrap();
        std::fs::write(
            agents_dir.join("manager.md"),
            "---\nname: Manager\ndescription: Test manager\ntools:\n  - Read\n  - Write\n---\n\nYou are a test manager.",
        )
        .unwrap();

        let exec_id = ensure_manager_execution(
            &mut conn,
            config_dir.path(),
            &manager.id,
            &project_id,
            "manager",
            None,
        )
        .unwrap();

        // Manager should now have execution_id
        let updated_manager = crate::managers::crud::get(&mut conn, &manager.id)
            .unwrap()
            .unwrap();
        assert_eq!(updated_manager.execution_id, Some(exec_id.clone()));

        // The manager's job should also reference the execution
        let job_exec_id: Option<String> = jobs::table
            .find(manager.job_id.as_ref().unwrap())
            .select(jobs::execution_id)
            .first(&mut conn)
            .unwrap();
        assert_eq!(job_exec_id, Some(exec_id.clone()));

        // Execution should exist with a valid snapshot
        let snap_json: Option<String> = executions::table
            .find(&exec_id)
            .select(executions::snapshot)
            .first(&mut conn)
            .unwrap();
        assert!(snap_json.is_some());
        let snap: ExecutionSnapshot = serde_json::from_str(&snap_json.unwrap()).unwrap();
        assert!(snap.agents.contains_key("manager"));
        assert_eq!(
            snap.agents.get("manager").unwrap().prompt,
            "You are a test manager."
        );
    }

    #[test]
    fn test_manager_config_update_serde_from_frontend() {
        let json = r#"{"tier":"claude/lg","prompt":"New prompt","skills":["testing"]}"#;
        let update: ManagerConfigUpdate = serde_json::from_str(json).unwrap();

        assert!(update.tier.is_some());
        assert!(update.tier.unwrap().is_some());
        assert_eq!(update.prompt, Some("New prompt".into()));
        assert!(update.tools.is_none());
        assert_eq!(update.skills, Some(vec!["testing".into()]));

        // Legacy `model` still aliases to tier for backward compatibility.
        let json_with_model = r#"{"model":"claude/md"}"#;
        let update: ManagerConfigUpdate = serde_json::from_str(json_with_model).unwrap();
        assert_eq!(update.tier.unwrap().unwrap(), Model::new("claude/md"));

        // All fields absent = no changes
        let empty: ManagerConfigUpdate = serde_json::from_str("{}").unwrap();
        assert!(empty.tier.is_none());
        assert!(empty.prompt.is_none());
        assert!(empty.tools.is_none());
        assert!(empty.skills.is_none());
    }
}
