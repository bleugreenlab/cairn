//! Tests for entity override and promote operations.

mod common;

use std::path::PathBuf;
use std::sync::Arc;

use cairn_core::internal::db::DbState;
use cairn_core::internal::orchestrator::Orchestrator;
use cairn_core::internal::services::testing::TestServicesBuilder;
use cairn_core::internal::services::RealClock;
use cairn_core::internal::storage::SearchIndex;
use cairn_core::models::{AgentConfig, CreateProject, Recipe, SkillConfig};
use cairn_core::projects::crud as project_crud;
use tempfile::{tempdir, TempDir};

struct TestOrchestrator {
    orch: Orchestrator,
    _db_temp: TempDir,
    _temp: TempDir,
    config_dir: PathBuf,
    project_dir: PathBuf,
    project_id: String,
}

async fn test_config_orchestrator() -> TestOrchestrator {
    let temp = tempdir().unwrap();
    let config_dir = temp.path().join("config");
    let project_dir = temp.path().join("project");
    let search_dir = temp.path().join("search");
    std::fs::create_dir_all(&config_dir).unwrap();
    std::fs::create_dir_all(&project_dir).unwrap();

    let (db_temp, db) = common::migrated_db().await;
    let local = Arc::new(db);
    let search_index = Arc::new(SearchIndex::open_or_create(&search_dir).unwrap());
    let db_state = Arc::new(DbState::new(local.clone(), search_index));
    let services = Arc::new(TestServicesBuilder::new().build());

    let project_id = uuid::Uuid::new_v4().to_string();
    project_crud::create_db(
        &local,
        &RealClock,
        &CreateProject {
            id: Some(project_id.clone()),
            name: "Test".to_string(),
            key: "TST".to_string(),
            repo_path: project_dir.to_string_lossy().to_string(),
            remote_url: None,
            server_id: None,
        },
    )
    .await
    .unwrap();

    let orch = Orchestrator::builder(db_state, services, config_dir.clone())
        .mcp_binary_path(String::new())
        .build();

    TestOrchestrator {
        orch,
        _db_temp: db_temp,
        _temp: temp,
        config_dir,
        project_dir,
        project_id,
    }
}

fn assert_err_contains<T>(result: Result<T, String>, expected: &str) {
    match result {
        Ok(_) => panic!("expected error containing {expected:?}"),
        Err(err) => assert!(err.contains(expected), "expected {expected:?} in {err:?}"),
    }
}

#[derive(Clone, Copy, Debug)]
enum ConfigKind {
    Agent,
    Skill,
    Recipe,
}

#[derive(Clone, Copy)]
enum ConfigScope {
    Workspace,
    Project,
}

struct ConfigResourceResult {
    id: String,
    name: String,
    workspace_id: Option<String>,
    project_id: Option<String>,
}

trait IntoConfigResourceResult {
    fn into_config_resource_result(self) -> ConfigResourceResult;
}

macro_rules! impl_config_resource_result {
    ($ty:ty) => {
        impl IntoConfigResourceResult for $ty {
            fn into_config_resource_result(self) -> ConfigResourceResult {
                ConfigResourceResult {
                    id: self.id,
                    name: self.name,
                    workspace_id: self.workspace_id,
                    project_id: self.project_id,
                }
            }
        }
    };
}

impl_config_resource_result!(AgentConfig);
impl_config_resource_result!(SkillConfig);
impl_config_resource_result!(Recipe);

fn normalize_config_result<T>(result: Result<T, String>) -> Result<ConfigResourceResult, String>
where
    T: IntoConfigResourceResult,
{
    result.map(IntoConfigResourceResult::into_config_resource_result)
}

impl ConfigKind {
    fn write_workspace(self, ctx: &TestOrchestrator, id: &str, name: &str) {
        self.write(ctx, ConfigScope::Workspace, id, name);
    }

    fn write_project(self, ctx: &TestOrchestrator, id: &str, name: &str) {
        self.write(ctx, ConfigScope::Project, id, name);
    }

    fn create_override(
        self,
        ctx: &TestOrchestrator,
        id: &str,
    ) -> Result<ConfigResourceResult, String> {
        match self {
            ConfigKind::Agent => {
                normalize_config_result(ctx.orch.create_agent_override(id, &ctx.project_id))
            }
            ConfigKind::Skill => {
                normalize_config_result(ctx.orch.create_skill_override(id, &ctx.project_id))
            }
            ConfigKind::Recipe => {
                normalize_config_result(ctx.orch.create_recipe_override(id, &ctx.project_id))
            }
        }
    }

    fn promote_to_workspace(
        self,
        ctx: &TestOrchestrator,
        id: &str,
    ) -> Result<ConfigResourceResult, String> {
        match self {
            ConfigKind::Agent => {
                normalize_config_result(ctx.orch.promote_agent_to_workspace(id, &ctx.project_id))
            }
            ConfigKind::Skill => {
                normalize_config_result(ctx.orch.promote_skill_to_workspace(id, &ctx.project_id))
            }
            ConfigKind::Recipe => {
                normalize_config_result(ctx.orch.promote_recipe_to_workspace(id, &ctx.project_id))
            }
        }
    }

    fn workspace_path(self, ctx: &TestOrchestrator, id: &str) -> PathBuf {
        self.path(ctx, ConfigScope::Workspace, id)
    }

    fn project_path(self, ctx: &TestOrchestrator, id: &str) -> PathBuf {
        self.path(ctx, ConfigScope::Project, id)
    }

    fn write(self, ctx: &TestOrchestrator, scope: ConfigScope, id: &str, name: &str) {
        let path = self.path(ctx, scope, id);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, self.content(scope, id, name)).unwrap();
    }

    fn path(self, ctx: &TestOrchestrator, scope: ConfigScope, id: &str) -> PathBuf {
        let root = match scope {
            ConfigScope::Workspace => ctx.config_dir.clone(),
            ConfigScope::Project => ctx.project_dir.join(".cairn"),
        };

        match self {
            ConfigKind::Agent => root.join(format!("agents/{id}.md")),
            ConfigKind::Skill => root.join(format!("skills/{id}/SKILL.md")),
            ConfigKind::Recipe => root.join(format!("recipes/{id}.yaml")),
        }
    }

    fn content(self, scope: ConfigScope, id: &str, name: &str) -> String {
        match self {
            ConfigKind::Agent => {
                let description = match scope {
                    ConfigScope::Workspace => "test agent",
                    ConfigScope::Project => "project agent",
                };
                format!(
                    "---\nname: {name}\ndescription: {description}\ntools:\n  - Read\n---\n\nYou are {name}.\n"
                )
            }
            ConfigKind::Skill => {
                let description = match scope {
                    ConfigScope::Workspace => "test skill",
                    ConfigScope::Project => "project skill",
                };
                format!(
                    "---\nname: {id}\ndescription: {description}\nmetadata:\n  display-name: {name}\n---\n\nSkill content for {name}.\n"
                )
            }
            ConfigKind::Recipe => {
                let description = match scope {
                    ConfigScope::Workspace => "test recipe",
                    ConfigScope::Project => "project recipe",
                };
                format!(
                    "cairnVersion: 1\nname: {name}\ndescription: {description}\ntrigger: issue\ncontext: issue\nnodes:\n  - id: t1\n    type: trigger\n    name: Trigger\n    position: 0@0\nedges: []\n"
                )
            }
        }
    }
}

async fn assert_create_override_from_workspace(
    kind: ConfigKind,
    id: &str,
    workspace_name: &str,
    expected_result_name: Option<&str>,
) {
    let ctx = test_config_orchestrator().await;

    kind.write_workspace(&ctx, id, workspace_name);

    let result = kind.create_override(&ctx, id).unwrap();
    assert_eq!(result.id, id, "{kind:?}");
    if let Some(expected_name) = expected_result_name {
        assert_eq!(result.name, expected_name, "{kind:?}");
    }
    assert_eq!(result.project_id, Some(ctx.project_id.clone()), "{kind:?}");
    assert!(kind.project_path(&ctx, id).exists(), "{kind:?}");
}

async fn assert_create_override_rejects_duplicate(
    kind: ConfigKind,
    id: &str,
    workspace_name: &str,
) {
    let ctx = test_config_orchestrator().await;

    kind.write_workspace(&ctx, id, workspace_name);
    kind.write_project(&ctx, id, "Already There");

    assert_err_contains(kind.create_override(&ctx, id), "already exists");
}

async fn assert_promote_to_workspace_success(
    kind: ConfigKind,
    id: &str,
    project_name: &str,
    expect_project_file: bool,
) {
    let ctx = test_config_orchestrator().await;

    kind.write_project(&ctx, id, project_name);

    let result = kind.promote_to_workspace(&ctx, id).unwrap();
    assert_eq!(result.id, id, "{kind:?}");
    assert_eq!(result.workspace_id, Some("default".to_string()), "{kind:?}");
    assert!(kind.workspace_path(&ctx, id).exists(), "{kind:?}");
    if expect_project_file {
        assert!(kind.project_path(&ctx, id).exists(), "{kind:?}");
    }
}

async fn assert_promote_rejects_non_project_scoped(
    kind: ConfigKind,
    id: &str,
    workspace_name: &str,
) {
    let ctx = test_config_orchestrator().await;

    kind.write_workspace(&ctx, id, workspace_name);

    assert_err_contains(kind.promote_to_workspace(&ctx, id), "not project-scoped");
}

async fn assert_promote_rejects_if_workspace_exists(
    kind: ConfigKind,
    id: &str,
    workspace_name: &str,
) {
    let ctx = test_config_orchestrator().await;

    kind.write_workspace(&ctx, id, workspace_name);
    kind.write_project(&ctx, id, "Project Version");

    assert_err_contains(
        kind.promote_to_workspace(&ctx, id),
        "already exists at workspace",
    );
}

#[tokio::test]
async fn create_agent_override_from_workspace() {
    assert_create_override_from_workspace(
        ConfigKind::Agent,
        "my-agent",
        "My Agent",
        Some("My Agent"),
    )
    .await;
}

#[tokio::test]
async fn create_agent_override_rejects_duplicate() {
    assert_create_override_rejects_duplicate(ConfigKind::Agent, "my-agent", "My Agent").await;
}

#[tokio::test]
async fn create_agent_override_from_project_scope_rejects_duplicate() {
    let ctx = test_config_orchestrator().await;

    ConfigKind::Agent.write_project(&ctx, "proj-only", "Project Only Agent");

    let result = ctx.orch.create_agent_override("proj-only", &ctx.project_id);
    assert_err_contains(result, "already exists");
}

#[tokio::test]
async fn promote_agent_to_workspace_success() {
    assert_promote_to_workspace_success(ConfigKind::Agent, "proj-agent", "Project Agent", true)
        .await;
}

#[tokio::test]
async fn promote_agent_rejects_non_project_scoped() {
    assert_promote_rejects_non_project_scoped(ConfigKind::Agent, "ws-agent", "WS Agent").await;
}

#[tokio::test]
async fn promote_agent_rejects_if_workspace_exists() {
    assert_promote_rejects_if_workspace_exists(ConfigKind::Agent, "dual-agent", "WS Version").await;
}

#[tokio::test]
async fn create_skill_override_from_workspace() {
    assert_create_override_from_workspace(ConfigKind::Skill, "my-skill", "My Skill", None).await;
}

#[tokio::test]
async fn create_skill_override_rejects_duplicate() {
    assert_create_override_rejects_duplicate(ConfigKind::Skill, "my-skill", "My Skill").await;
}

#[tokio::test]
async fn promote_skill_to_workspace_success() {
    assert_promote_to_workspace_success(ConfigKind::Skill, "proj-skill", "Project Skill", false)
        .await;
}

#[tokio::test]
async fn promote_skill_rejects_non_project_scoped() {
    assert_promote_rejects_non_project_scoped(ConfigKind::Skill, "ws-skill", "WS Skill").await;
}

#[tokio::test]
async fn promote_skill_rejects_if_workspace_exists() {
    assert_promote_rejects_if_workspace_exists(ConfigKind::Skill, "dual-skill", "WS Version").await;
}

#[tokio::test]
async fn create_recipe_override_from_workspace() {
    assert_create_override_from_workspace(ConfigKind::Recipe, "my-recipe", "My Recipe", None).await;
}

#[tokio::test]
async fn create_recipe_override_rejects_duplicate() {
    assert_create_override_rejects_duplicate(ConfigKind::Recipe, "my-recipe", "My Recipe").await;
}

#[tokio::test]
async fn promote_recipe_to_workspace_success() {
    assert_promote_to_workspace_success(ConfigKind::Recipe, "proj-recipe", "Project Recipe", false)
        .await;
}

#[tokio::test]
async fn promote_recipe_rejects_non_project_scoped() {
    assert_promote_rejects_non_project_scoped(ConfigKind::Recipe, "ws-recipe", "WS Recipe").await;
}

#[tokio::test]
async fn promote_recipe_rejects_if_workspace_exists() {
    assert_promote_rejects_if_workspace_exists(ConfigKind::Recipe, "dual-recipe", "WS Version")
        .await;
}
