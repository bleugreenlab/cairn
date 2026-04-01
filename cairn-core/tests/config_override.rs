//! Tests for entity override and promote operations.
//!
//! Verifies the claims made by the create_*_override and promote_*_to_workspace
//! methods on Orchestrator: scope resolution, guard conditions, and file placement.

mod common;

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use cairn_core::internal::agent_process::process::AgentProcessState;
use cairn_core::internal::db::DbState;
use cairn_core::internal::mcp::McpAuthState;
use cairn_core::internal::orchestrator::Orchestrator;
use cairn_core::internal::services::testing::TestServicesBuilder;
use cairn_core::internal::services::PtyState;
use tempfile::tempdir;

/// Build a minimal Orchestrator backed by an in-memory DB and temp directories.
///
/// Returns (orchestrator, config_dir, project_dir, project_id).
fn test_orchestrator() -> (Orchestrator, PathBuf, PathBuf, String) {
    let temp = tempdir().unwrap();
    let config_dir = temp.path().join("config");
    let project_dir = temp.path().join("project");
    std::fs::create_dir_all(&config_dir).unwrap();
    std::fs::create_dir_all(&project_dir).unwrap();

    let mut conn = common::test_conn();

    // Insert a project whose repo_path points to our temp project_dir
    let project_id = uuid::Uuid::new_v4().to_string();
    let now = chrono::Utc::now().timestamp() as i32;
    let project_path_str = project_dir.to_str().unwrap().to_string();
    diesel::sql_query(
        "INSERT INTO projects (id, workspace_id, name, key, repo_path, docs_enabled, default_branch, next_issue_number, created_at, updated_at)
         VALUES (?, 'default', 'Test', 'TST', ?, 1, 'main', 1, ?, ?)"
    )
    .bind::<diesel::sql_types::Text, _>(&project_id)
    .bind::<diesel::sql_types::Text, _>(&project_path_str)
    .bind::<diesel::sql_types::Integer, _>(now)
    .bind::<diesel::sql_types::Integer, _>(now)
    .execute(&mut conn)
    .expect("insert project");

    use diesel::RunQueryDsl;

    let services = TestServicesBuilder::new().build();
    let (perm_tx, _) = tokio::sync::broadcast::channel(1);
    let (run_tx, _) = tokio::sync::broadcast::channel(1);

    let db = Arc::new(DbState {
        conn: Mutex::new(conn),
    });
    let services = Arc::new(services);
    let orch = Orchestrator::builder(db, services, config_dir.clone())
        .process_state(Arc::new(AgentProcessState::default()))
        .mcp_auth(Arc::new(McpAuthState::new(config_dir.clone())))
        .pty_state(Arc::new(PtyState::default()))
        .permission_responses(perm_tx)
        .run_completions(run_tx)
        .mcp_binary_path(String::new())
        .build();
    // Leak temp so it isn't cleaned up while orch holds paths
    std::mem::forget(temp);

    (orch, config_dir, project_dir, project_id)
}

// ---------------------------------------------------------------------------
// Agent override & promote
// ---------------------------------------------------------------------------

fn write_agent(dir: &std::path::Path, id: &str, name: &str) {
    let agents_dir = dir.join("agents");
    std::fs::create_dir_all(&agents_dir).unwrap();
    let content = format!(
        "---\nname: {name}\ndescription: test agent\ntools:\n  - Read\n---\n\nYou are {name}.\n"
    );
    std::fs::write(agents_dir.join(format!("{id}.md")), content).unwrap();
}

fn write_project_agent(project_dir: &std::path::Path, id: &str, name: &str) {
    let agents_dir = project_dir.join(".cairn").join("agents");
    std::fs::create_dir_all(&agents_dir).unwrap();
    let content = format!(
        "---\nname: {name}\ndescription: project agent\ntools:\n  - Read\n---\n\nYou are {name}.\n"
    );
    std::fs::write(agents_dir.join(format!("{id}.md")), content).unwrap();
}

#[test]
fn create_agent_override_from_workspace() {
    let (orch, config_dir, project_dir, project_id) = test_orchestrator();

    write_agent(&config_dir, "my-agent", "My Agent");

    let result = orch.create_agent_override("my-agent", &project_id).unwrap();
    assert_eq!(result.id, "my-agent");
    assert_eq!(result.name, "My Agent");
    assert_eq!(result.project_id, Some(project_id.clone()));

    // File should exist in project
    let project_file = project_dir.join(".cairn/agents/my-agent.md");
    assert!(project_file.exists(), "override file should be created");
}

#[test]
fn create_agent_override_rejects_duplicate() {
    let (orch, config_dir, project_dir, project_id) = test_orchestrator();

    write_agent(&config_dir, "my-agent", "My Agent");
    write_project_agent(&project_dir, "my-agent", "Already There");

    let result = orch.create_agent_override("my-agent", &project_id);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("already exists"));
}

#[test]
fn create_agent_override_from_project_scope() {
    let (orch, _config_dir, project_dir, project_id) = test_orchestrator();

    // Source is a project-scoped agent (no workspace version exists).
    // The old fork_agent_config would reject this; create_agent_override should resolve it.
    write_project_agent(&project_dir, "proj-only", "Project Only Agent");

    // Creating an override in the same project should fail (already exists)
    let result = orch.create_agent_override("proj-only", &project_id);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("already exists"));
}

#[test]
fn promote_agent_to_workspace_success() {
    let (orch, config_dir, project_dir, project_id) = test_orchestrator();

    // Create a project-only agent (no workspace version)
    write_project_agent(&project_dir, "proj-agent", "Project Agent");

    let result = orch
        .promote_agent_to_workspace("proj-agent", &project_id)
        .unwrap();
    assert_eq!(result.id, "proj-agent");
    assert_eq!(result.workspace_id, Some("default".to_string()));
    assert_eq!(result.project_id, None);

    // Workspace file should exist
    let ws_file = config_dir.join("agents/proj-agent.md");
    assert!(ws_file.exists(), "workspace file should be created");

    // Original project file should still exist
    let proj_file = project_dir.join(".cairn/agents/proj-agent.md");
    assert!(proj_file.exists(), "project file should remain");
}

#[test]
fn promote_agent_rejects_non_project_scoped() {
    let (orch, config_dir, _project_dir, project_id) = test_orchestrator();

    // Workspace-scoped agent
    write_agent(&config_dir, "ws-agent", "WS Agent");

    let result = orch.promote_agent_to_workspace("ws-agent", &project_id);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("not project-scoped"));
}

#[test]
fn promote_agent_rejects_if_workspace_exists() {
    let (orch, config_dir, project_dir, project_id) = test_orchestrator();

    write_agent(&config_dir, "dual-agent", "WS Version");
    write_project_agent(&project_dir, "dual-agent", "Project Version");

    let result = orch.promote_agent_to_workspace("dual-agent", &project_id);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("already exists at workspace"));
}

// ---------------------------------------------------------------------------
// Skill override & promote
// ---------------------------------------------------------------------------

fn write_skill(dir: &std::path::Path, id: &str, name: &str) {
    let skill_dir = dir.join("skills").join(id);
    std::fs::create_dir_all(&skill_dir).unwrap();
    let content =
        format!("---\nname: {id}\ndescription: test skill\nmetadata:\n  display-name: {name}\n---\n\nSkill content for {name}.\n");
    std::fs::write(skill_dir.join("SKILL.md"), content).unwrap();
}

fn write_project_skill(project_dir: &std::path::Path, id: &str, name: &str) {
    let skill_dir = project_dir.join(".cairn").join("skills").join(id);
    std::fs::create_dir_all(&skill_dir).unwrap();
    let content = format!(
        "---\nname: {id}\ndescription: project skill\nmetadata:\n  display-name: {name}\n---\n\nSkill content for {name}.\n"
    );
    std::fs::write(skill_dir.join("SKILL.md"), content).unwrap();
}

#[test]
fn create_skill_override_from_workspace() {
    let (orch, config_dir, project_dir, project_id) = test_orchestrator();

    write_skill(&config_dir, "my-skill", "My Skill");

    let result = orch.create_skill_override("my-skill", &project_id).unwrap();
    assert_eq!(result.id, "my-skill");
    assert_eq!(result.project_id, Some(project_id.clone()));

    let project_file = project_dir.join(".cairn/skills/my-skill/SKILL.md");
    assert!(project_file.exists());
}

#[test]
fn create_skill_override_rejects_duplicate() {
    let (orch, config_dir, project_dir, project_id) = test_orchestrator();

    write_skill(&config_dir, "my-skill", "My Skill");
    write_project_skill(&project_dir, "my-skill", "Already There");

    let result = orch.create_skill_override("my-skill", &project_id);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("already exists"));
}

#[test]
fn promote_skill_to_workspace_success() {
    let (orch, config_dir, project_dir, project_id) = test_orchestrator();

    write_project_skill(&project_dir, "proj-skill", "Project Skill");

    let result = orch
        .promote_skill_to_workspace("proj-skill", &project_id)
        .unwrap();
    assert_eq!(result.id, "proj-skill");
    assert_eq!(result.workspace_id, Some("default".to_string()));

    let ws_file = config_dir.join("skills/proj-skill/SKILL.md");
    assert!(ws_file.exists());
}

#[test]
fn promote_skill_rejects_non_project_scoped() {
    let (orch, config_dir, _project_dir, project_id) = test_orchestrator();

    write_skill(&config_dir, "ws-skill", "WS Skill");

    let result = orch.promote_skill_to_workspace("ws-skill", &project_id);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("not project-scoped"));
}

#[test]
fn promote_skill_rejects_if_workspace_exists() {
    let (orch, config_dir, project_dir, project_id) = test_orchestrator();

    write_skill(&config_dir, "dual-skill", "WS Version");
    write_project_skill(&project_dir, "dual-skill", "Project Version");

    let result = orch.promote_skill_to_workspace("dual-skill", &project_id);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("already exists at workspace"));
}

// ---------------------------------------------------------------------------
// Recipe override & promote
// ---------------------------------------------------------------------------

fn write_recipe(dir: &std::path::Path, id: &str, name: &str) {
    let recipes_dir = dir.join("recipes");
    std::fs::create_dir_all(&recipes_dir).unwrap();
    let content = format!(
        "cairnVersion: 1\nname: {name}\ndescription: test recipe\ntrigger: issue\ncontext: issue\nnodes:\n  - id: t1\n    type: trigger\n    name: Trigger\n    position: 0@0\nedges: []\n"
    );
    std::fs::write(recipes_dir.join(format!("{id}.yaml")), content).unwrap();
}

fn write_project_recipe(project_dir: &std::path::Path, id: &str, name: &str) {
    let recipes_dir = project_dir.join(".cairn").join("recipes");
    std::fs::create_dir_all(&recipes_dir).unwrap();
    let content = format!(
        "cairnVersion: 1\nname: {name}\ndescription: project recipe\ntrigger: issue\ncontext: issue\nnodes:\n  - id: t1\n    type: trigger\n    name: Trigger\n    position: 0@0\nedges: []\n"
    );
    std::fs::write(recipes_dir.join(format!("{id}.yaml")), content).unwrap();
}

#[test]
fn create_recipe_override_from_workspace() {
    let (orch, config_dir, project_dir, project_id) = test_orchestrator();

    write_recipe(&config_dir, "my-recipe", "My Recipe");

    let result = orch
        .create_recipe_override("my-recipe", &project_id)
        .unwrap();
    assert_eq!(result.id, "my-recipe");
    assert_eq!(result.project_id, Some(project_id.clone()));

    let project_file = project_dir.join(".cairn/recipes/my-recipe.yaml");
    assert!(project_file.exists());
}

#[test]
fn create_recipe_override_rejects_duplicate() {
    let (orch, config_dir, project_dir, project_id) = test_orchestrator();

    write_recipe(&config_dir, "my-recipe", "My Recipe");
    write_project_recipe(&project_dir, "my-recipe", "Already There");

    let result = orch.create_recipe_override("my-recipe", &project_id);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("already exists"));
}

#[test]
fn promote_recipe_to_workspace_success() {
    let (orch, config_dir, project_dir, project_id) = test_orchestrator();

    write_project_recipe(&project_dir, "proj-recipe", "Project Recipe");

    let result = orch
        .promote_recipe_to_workspace("proj-recipe", &project_id)
        .unwrap();
    assert_eq!(result.id, "proj-recipe");
    assert_eq!(result.workspace_id, Some("default".to_string()));

    let ws_file = config_dir.join("recipes/proj-recipe.yaml");
    assert!(ws_file.exists());
}

#[test]
fn promote_recipe_rejects_non_project_scoped() {
    let (orch, config_dir, _project_dir, project_id) = test_orchestrator();

    write_recipe(&config_dir, "ws-recipe", "WS Recipe");

    let result = orch.promote_recipe_to_workspace("ws-recipe", &project_id);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("not project-scoped"));
}

#[test]
fn promote_recipe_rejects_if_workspace_exists() {
    let (orch, config_dir, project_dir, project_id) = test_orchestrator();

    write_recipe(&config_dir, "dual-recipe", "WS Version");
    write_project_recipe(&project_dir, "dual-recipe", "Project Version");

    let result = orch.promote_recipe_to_workspace("dual-recipe", &project_id);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("already exists at workspace"));
}
