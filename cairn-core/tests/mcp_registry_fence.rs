//! Integration coverage for the worktree fence on `write cairn://mcp` at
//! workspace scope.
//!
//! A workspace-scope registry write edits `~/.cairn/settings.yaml`, outside the
//! agent's worktree. It must route through the SAME worktree fence as any
//! out-of-worktree file write — this proves the property unit tests cannot:
//! under `Sandbox::Worktree` + `OnEscape::Deny` the write returns a deny report
//! AND leaves `settings.yaml` byte-unchanged (deny changes nothing), and under
//! `OnEscape::Allow` the write actually lands.

mod common;

use std::collections::HashMap;
use std::sync::Arc;

use cairn_core::internal::mcp::handlers::files::handle_change;
use cairn_core::internal::mcp::types::McpCallbackRequest;
use cairn_core::internal::storage::LocalDb;
use cairn_core::models::{
    AgentSnapshot, ExecutionSnapshot, Fence, LegacyOnEscape, LegacySandbox, RecipeSnapshot,
    RecipeTrigger, TriggerContext, TriggerType,
};
use common::orchestrator;
use serde_json::json;
use tempfile::TempDir;
use turso::params;

fn settings_path(temp: &TempDir) -> std::path::PathBuf {
    temp.path().join("config").join("settings.yaml")
}

/// An agent snapshot carrying only the fence-relevant policy; everything else is
/// minimal filler.
fn agent_snapshot(sandbox: LegacySandbox, on_escape: LegacyOnEscape) -> AgentSnapshot {
    AgentSnapshot {
        id: "agent-1".to_string(),
        name: "Builder".to_string(),
        description: String::new(),
        prompt: String::new(),
        tools: Vec::new(),
        tier: None,
        backend_preference: None,
        selection: None,
        model: None,
        disallowed_tools: None,
        skills: None,
        fence: Some(Fence::from_legacy(Some(sandbox), Some(on_escape))),
        sandbox: None,
        on_escape: None,
        resolved_backend: None,
        extras: None,
    }
}

/// Seed run → job → execution so `resolve_fence_policy` resolves the given
/// `(sandbox, on_escape)` for `run-1`. The run id is `run-1`.
async fn seed_run(
    db: &LocalDb,
    project_id: &str,
    sandbox: LegacySandbox,
    on_escape: LegacyOnEscape,
) {
    let mut agents = HashMap::new();
    agents.insert("agent-1".to_string(), agent_snapshot(sandbox, on_escape));
    let snapshot = ExecutionSnapshot::new(
        RecipeSnapshot {
            id: "recipe-1".to_string(),
            name: "Fence test".to_string(),
            description: None,
            trigger: RecipeTrigger::Manual,
            nodes: Vec::new(),
            edges: Vec::new(),
        },
        agents,
        HashMap::new(),
        TriggerContext {
            issue_id: Some("issue-1".to_string()),
            project_id: project_id.to_string(),
            trigger_type: TriggerType::Manual,
            event_payload: None,
            initiated_via: None,
        },
    )
    .to_json()
    .unwrap();

    let project_id = project_id.to_string();
    db.write(move |conn| {
        let project_id = project_id.clone();
        let snapshot = snapshot.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
                 VALUES ('issue-1', ?1, 1, 'Fence', 'active', 1, 1)",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot, triggered_by)
                 VALUES ('exec-1', 'recipe-1', 'issue-1', ?1, 'running', 1, 1, ?2, 'manual')",
                params![project_id.as_str(), snapshot.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO jobs(id, execution_id, agent_config_id, issue_id, project_id, node_name, status, uri_segment, created_at, updated_at)
                 VALUES ('job-1', 'exec-1', 'agent-1', 'issue-1', ?1, 'builder', 'running', 'builder', 1, 1)",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO runs(id, project_id, issue_id, job_id, status, backend, created_at, updated_at, start_mode)
                 VALUES ('run-1', ?1, 'issue-1', 'job-1', 'live', 'claude', 1, 1, 'resume')",
                params![project_id.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

fn create_request() -> McpCallbackRequest {
    McpCallbackRequest {
        cwd: "/wt".to_string(),
        run_id: Some("run-1".to_string()),
        tool: "write".to_string(),
        payload: json!({
            "changes": [{
                "target": "cairn://mcp",
                "mode": "create",
                "payload": { "name": "axon", "command": "npx", "args": ["axon-mcp"] }
            }]
        }),
        tool_use_id: Some("toolu-mcp".to_string()),
    }
}

/// Deny is the must-have property: a worktree-sandboxed `onEscape: deny` agent's
/// workspace registry write is rejected AND `settings.yaml` is byte-unchanged.
#[tokio::test]
async fn workspace_mcp_create_denied_by_fence_changes_nothing() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let project_id = common::create_project(&db, "MRF").await;
    seed_run(
        &db,
        &project_id,
        LegacySandbox::Worktree,
        LegacyOnEscape::Deny,
    )
    .await;
    let orch = orchestrator(&temp, db);

    // A pre-existing settings.yaml whose exact bytes must survive a denied write.
    let path = settings_path(&temp);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let original = "# Cairn Workspace Settings\nbranchPrefix: keep-me\n";
    std::fs::write(&path, original).unwrap();

    let result = handle_change(&orch, &create_request()).await;

    // The write is rejected through the worktree fence (the onEscape: deny path).
    assert!(
        result.contains("Denied by agent fence policy") || result.contains("fence: deny"),
        "expected a fence deny report, got: {result}"
    );
    // Deny changes nothing: settings.yaml is byte-identical, no server added.
    let after = std::fs::read_to_string(&path).unwrap();
    assert_eq!(after, original, "denied write must not touch settings.yaml");
    assert!(!after.contains("axon"), "server must not have been written");
}

/// Allow is the complementary check: the same write under `onEscape: allow`
/// actually lands the server in settings.yaml.
#[tokio::test]
async fn workspace_mcp_create_allowed_lands_server() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let project_id = common::create_project(&db, "MRF").await;
    seed_run(
        &db,
        &project_id,
        LegacySandbox::Worktree,
        LegacyOnEscape::Allow,
    )
    .await;
    let orch = orchestrator(&temp, db);

    let path = settings_path(&temp);
    assert!(!path.exists(), "precondition: no settings file yet");

    let result = handle_change(&orch, &create_request()).await;
    assert!(
        result.contains("Added workspace MCP server 'axon'") || result.contains("\"applied\""),
        "expected a success report, got: {result}"
    );
    let written = std::fs::read_to_string(&path).expect("settings.yaml should now exist");
    assert!(
        written.contains("axon"),
        "server should be in settings.yaml"
    );
    assert!(
        written.contains("npx"),
        "command should be in settings.yaml"
    );
}
