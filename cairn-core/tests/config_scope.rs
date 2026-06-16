//! Tests for config scope mutation: copying agents, skills, and recipes
//! between workspace and project scope through the canonical copy primitive.

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

    fn copy(
        self,
        ctx: &TestOrchestrator,
        source_id: &str,
        source_project_id: Option<&str>,
        target_id: &str,
        target_project_id: Option<&str>,
    ) -> Result<ConfigResourceResult, String> {
        match self {
            ConfigKind::Agent => normalize_config_result(ctx.orch.copy_agent_config(
                source_id,
                source_project_id,
                target_id,
                target_project_id,
            )),
            ConfigKind::Skill => normalize_config_result(ctx.orch.copy_skill_config(
                source_id,
                source_project_id,
                target_id,
                target_project_id,
            )),
            ConfigKind::Recipe => normalize_config_result(ctx.orch.copy_recipe_config(
                source_id,
                source_project_id,
                target_id,
                target_project_id,
            )),
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

/// copy-from-existing (behavior #3): a workspace source copies into the project
/// under a NEW id as an independent additive artifact, and under the SAME id as
/// a project shadow. The source stays put in both cases (hard copy, no move).
async fn assert_copy_from_workspace_source(kind: ConfigKind) {
    let ctx = test_config_orchestrator().await;
    kind.write_workspace(&ctx, "src", "Source");

    let additive = kind
        .copy(&ctx, "src", None, "src-copy", Some(&ctx.project_id))
        .unwrap();
    assert_eq!(additive.id, "src-copy", "{kind:?}");
    assert_eq!(additive.name, "Source", "{kind:?}");
    assert!(additive.workspace_id.is_none(), "{kind:?}");
    assert_eq!(additive.project_id, Some(ctx.project_id.clone()), "{kind:?}");
    assert!(kind.project_path(&ctx, "src-copy").exists(), "{kind:?}");
    assert!(
        kind.workspace_path(&ctx, "src").exists(),
        "source remains after copy {kind:?}"
    );

    let shadow = kind
        .copy(&ctx, "src", None, "src", Some(&ctx.project_id))
        .unwrap();
    assert_eq!(shadow.id, "src", "{kind:?}");
    assert_eq!(shadow.project_id, Some(ctx.project_id.clone()), "{kind:?}");
    assert!(kind.project_path(&ctx, "src").exists(), "{kind:?}");
}

/// The target_exists guard: copying onto an id that already exists at the target
/// scope errors rather than clobbering it.
async fn assert_copy_rejects_existing_target(kind: ConfigKind) {
    let ctx = test_config_orchestrator().await;
    kind.write_workspace(&ctx, "src", "Source");
    kind.write_workspace(&ctx, "taken", "Taken");

    assert_err_contains(kind.copy(&ctx, "src", None, "taken", None), "already exists");
}

#[tokio::test]
async fn copy_agent_from_workspace_additive_and_shadow() {
    assert_copy_from_workspace_source(ConfigKind::Agent).await;
}

#[tokio::test]
async fn copy_skill_from_workspace_additive_and_shadow() {
    assert_copy_from_workspace_source(ConfigKind::Skill).await;
}

#[tokio::test]
async fn copy_recipe_from_workspace_additive_and_shadow() {
    assert_copy_from_workspace_source(ConfigKind::Recipe).await;
}

#[tokio::test]
async fn copy_agent_rejects_existing_target() {
    assert_copy_rejects_existing_target(ConfigKind::Agent).await;
}

#[tokio::test]
async fn copy_skill_rejects_existing_target() {
    assert_copy_rejects_existing_target(ConfigKind::Skill).await;
}

#[tokio::test]
async fn copy_recipe_rejects_existing_target() {
    assert_copy_rejects_existing_target(ConfigKind::Recipe).await;
}
