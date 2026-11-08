//! Resource-URI read + write parity for recipes, agents, and actions.
//!
//! Recipes and agents are file-backed (`~/.cairn/...` workspace,
//! `<repo>/.cairn/...` project); actions are DB-backed (`action_configs`). Each
//! kind is exercised over its `cairn://` URI: read collection + item, create /
//! patch / delete via `write`, and project-vs-workspace scope resolution.

use crate::common;

use std::sync::Arc;

use crate::common::{
    change_resource as change, config_dir, confirm_project_library, orchestrator,
    project_resource_fixture, read_resource as read, register_project_library,
    resource_orchestrator_fixture, trust_project_library, untrusted_refusal,
};
use cairn_core::internal::storage::{LocalDb, RowExt};
use cairn_db::turso::params;
use serde_json::json;

const RECIPE_YAML: &str = "cairnVersion: 1\nname: Deploy Flow\ndescription: A deploy recipe\ntrigger: issue\ncontext: issue\n\nnodes:\n  - id: trigger-1\n    type: trigger\n    name: Trigger\n    position: 0@0\n\nedges: []\n";

const RECIPE_YAML_V2: &str = "cairnVersion: 1\nname: Deploy Flow\ndescription: An UPDATED deploy recipe\ntrigger: issue\ncontext: issue\n\nnodes:\n  - id: trigger-1\n    type: trigger\n    name: Trigger\n    position: 0@0\n\nedges: []\n";

async fn action_id_by_name(db: &LocalDb, name: &str) -> Option<String> {
    let name = name.to_string();
    db.read(|conn| {
        let name = name.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id FROM action_configs WHERE name = ?1",
                    params![name.as_str()],
                )
                .await?;
            rows.next().await?.map(|r| r.text(0)).transpose()
        })
    })
    .await
    .ok()
    .flatten()
}

// ============================================================================
// Recipes (file-backed)
// ============================================================================

#[tokio::test]
async fn recipe_workspace_create_read_patch_delete_roundtrip() {
    let (temp, db) = common::migrated_db().await;
    let orch = orchestrator(&temp, Arc::new(db));
    let cfg = config_dir(&temp);

    let created = change(
        &orch,
        json!([{ "target": "cairn://recipes", "mode": "create", "payload": { "content": RECIPE_YAML } }]),
    )
    .await;
    assert!(created.contains("\"applied\""), "create: {created}");
    let file = cfg.join("recipes/deploy-flow.yaml");
    assert!(file.exists(), "recipe file should exist: {created}");

    let item = read(&orch, "cairn://recipes/deploy-flow").await;
    assert!(item.contains("Deploy Flow"), "read: {item}");

    let patched = change(
        &orch,
        json!([{ "target": "cairn://recipes/deploy-flow", "mode": "patch", "payload": { "content": RECIPE_YAML_V2 } }]),
    )
    .await;
    assert!(patched.contains("\"applied\""), "patch: {patched}");
    let body = std::fs::read_to_string(&file).unwrap();
    assert!(body.contains("An UPDATED deploy recipe"), "body: {body}");

    let deleted = change(
        &orch,
        json!([{ "target": "cairn://recipes/deploy-flow", "mode": "delete" }]),
    )
    .await;
    assert!(deleted.contains("\"applied\""), "delete: {deleted}");
    assert!(!file.exists(), "recipe file should be gone");
}

#[tokio::test]
async fn recipe_create_rejects_malformed_yaml() {
    let (temp, db) = common::migrated_db().await;
    let orch = orchestrator(&temp, Arc::new(db));

    // Missing required fields (trigger/nodes/edges) -> from_yaml fails.
    let bad = change(
        &orch,
        json!([{ "target": "cairn://recipes", "mode": "create", "payload": { "content": "cairnVersion: 1\nname: Broken\n" } }]),
    )
    .await;
    assert!(!bad.contains("\"applied\":[{"), "should not apply: {bad}");
    assert!(
        bad.contains("parse error") || bad.contains("Invalid recipe"),
        "bad: {bad}"
    );
}

#[tokio::test]
async fn recipe_project_scope_roundtrip_and_guard() {
    let (temp, _db, orch, repo) = project_resource_fixture("proj").await;
    trust_project_library(&orch, "project-proj").await;

    let created = change(
        &orch,
        json!([{ "target": "cairn://p/proj/recipes", "mode": "create", "payload": { "content": RECIPE_YAML } }]),
    )
    .await;
    assert!(created.contains("\"applied\""), "create: {created}");
    let file = repo.join(".cairn/recipes/deploy-flow.yaml");
    assert!(file.exists(), "project recipe file should exist");
    confirm_project_library(&orch, "project-proj").await;

    let item = read(&orch, "cairn://p/proj/recipes/deploy-flow").await;
    assert!(item.contains("Origin: @"), "read: {item}");
    assert!(item.contains("A deploy recipe"), "read: {item}");

    // A workspace-only recipe must not resolve behind an explicit project URI.
    change(
        &orch,
        json!([{ "target": "cairn://recipes", "mode": "create", "payload": { "content": RECIPE_YAML, "id": "ws-only" } }]),
    )
    .await;
    let guarded = change(
        &orch,
        json!([{ "target": "cairn://p/proj/recipes/ws-only", "mode": "delete" }]),
    )
    .await;
    assert!(
        guarded.contains("not found in project proj"),
        "guarded: {guarded}"
    );
    assert!(
        config_dir(&temp).join("recipes/ws-only.yaml").exists(),
        "workspace recipe untouched"
    );
}

#[tokio::test]
async fn recipe_project_scope_create_while_untrusted_says_not_in_force() {
    let (_temp, _db, orch, repo) = project_resource_fixture("proj").await;
    register_project_library(&orch).await;
    let not_in_force = format!(
        "Not in force: {}",
        untrusted_refusal(
            "recipe",
            "deploy-flow",
            "proj",
            ".cairn/recipes/deploy-flow.yaml"
        )
    );

    let created = change(
        &orch,
        json!([{ "target": "cairn://p/proj/recipes", "mode": "create", "payload": { "content": RECIPE_YAML } }]),
    )
    .await;
    assert!(created.contains("\"applied\""), "create: {created}");
    let file = repo.join(".cairn/recipes/deploy-flow.yaml");
    assert!(file.exists(), "the untrusted create still writes");
    assert!(created.contains(&not_in_force), "create: {created}");

    let item = read(&orch, "cairn://p/proj/recipes/deploy-flow").await;
    assert!(
        item.contains(&untrusted_refusal(
            "recipe",
            "deploy-flow",
            "proj",
            ".cairn/recipes/deploy-flow.yaml"
        )),
        "read: {item}"
    );
    assert!(
        !item.contains("A deploy recipe"),
        "read shows no content: {item}"
    );

    let listing = read(&orch, "cairn://p/proj/recipes").await;
    let (_, skipped) = listing
        .split_once("## Not in force")
        .unwrap_or_else(|| panic!("listing: {listing}"));
    assert!(
        skipped.contains("deploy-flow") && skipped.contains("untrusted: skipped"),
        "listing: {listing}"
    );

    let patched = change(
        &orch,
        json!([{ "target": "cairn://p/proj/recipes/deploy-flow", "mode": "patch", "payload": { "content": RECIPE_YAML_V2 } }]),
    )
    .await;
    assert!(patched.contains("\"applied\""), "patch: {patched}");
    assert!(patched.contains(&not_in_force), "patch: {patched}");
    let body = std::fs::read_to_string(&file).unwrap();
    assert!(body.contains("An UPDATED deploy recipe"), "body: {body}");
}

// ============================================================================
// Agents (file-backed)
// ============================================================================

#[tokio::test]
async fn agent_workspace_create_read_patch_delete_roundtrip() {
    let (temp, db) = common::migrated_db().await;
    let orch = orchestrator(&temp, Arc::new(db));
    let cfg = config_dir(&temp);

    let created = change(
        &orch,
        json!([{
            "target": "cairn://agents",
            "mode": "create",
            "payload": {
                "name": "Deploy Helper",
                "description": "Helps with deploys",
                "prompt": "# Deploy\n\nDo the thing.",
                "tools": ["Read", "Grep"],
                "models": ["md"]
            }
        }]),
    )
    .await;
    assert!(created.contains("\"applied\""), "create: {created}");
    let file = cfg.join("agents/deploy-helper.md");
    assert!(file.exists(), "agent file should exist: {created}");
    let written = std::fs::read_to_string(&file).unwrap();
    assert!(
        written.contains("models:") && written.contains("md"),
        "allotment: {written}"
    );

    let item = read(&orch, "cairn://agents/deploy-helper").await;
    assert!(item.contains("Helps with deploys"), "read: {item}");
    assert!(item.contains("Read, Grep"), "tools: {item}");

    let patched = change(
        &orch,
        json!([{ "target": "cairn://agents/deploy-helper", "mode": "patch", "payload": { "prompt": "# Deploy\n\nDo it carefully." } }]),
    )
    .await;
    assert!(patched.contains("\"applied\""), "patch: {patched}");
    let body = std::fs::read_to_string(&file).unwrap();
    assert!(body.contains("Do it carefully."), "body: {body}");

    let deleted = change(
        &orch,
        json!([{ "target": "cairn://agents/deploy-helper", "mode": "delete" }]),
    )
    .await;
    assert!(deleted.contains("\"applied\""), "delete: {deleted}");
    assert!(!file.exists(), "agent file should be gone");
}

#[tokio::test]
async fn agent_create_rejects_missing_tools() {
    let (temp, db) = common::migrated_db().await;
    let orch = orchestrator(&temp, Arc::new(db));

    let missing = change(
        &orch,
        json!([{
            "target": "cairn://agents",
            "mode": "create",
            "payload": { "name": "No Tools", "description": "d", "prompt": "p" }
        }]),
    )
    .await;
    assert!(missing.contains("tools"), "missing tools: {missing}");
}

#[tokio::test]
async fn agent_project_scope_does_not_bleed_into_workspace() {
    let (temp, _db, orch, _repo) = project_resource_fixture("proj").await;
    let cfg = config_dir(&temp);

    change(
        &orch,
        json!([{
            "target": "cairn://agents",
            "mode": "create",
            "payload": { "name": "Shared", "description": "ws", "prompt": "p", "tools": ["Read"] }
        }]),
    )
    .await;

    let read_proj = read(&orch, "cairn://p/proj/agents/shared").await;
    assert!(
        read_proj.contains("not found in project proj"),
        "read_proj: {read_proj}"
    );

    let patched = change(
        &orch,
        json!([{ "target": "cairn://p/proj/agents/shared", "mode": "patch", "payload": { "description": "hijacked" } }]),
    )
    .await;
    assert!(
        patched.contains("not found in project proj"),
        "patched: {patched}"
    );
    let body = std::fs::read_to_string(cfg.join("agents/shared.md")).unwrap();
    assert!(body.contains("ws"), "workspace agent untouched: {body}");
}

#[tokio::test]
async fn agent_project_scope_create_read_roundtrip() {
    let (_temp, _db, orch, repo) = project_resource_fixture("proj").await;
    trust_project_library(&orch, "project-proj").await;

    let created = change(
        &orch,
        json!([{
            "target": "cairn://p/proj/agents",
            "mode": "create",
            "payload": { "name": "Builder Helper", "description": "proj", "prompt": "p", "tools": ["Read"] }
        }]),
    )
    .await;
    assert!(created.contains("\"applied\""), "create: {created}");
    assert!(
        repo.join(".cairn/agents/builder-helper.md").exists(),
        "project agent file"
    );
    // Tools are authority: in a trusted project the new agent still waits on
    // the operator's confirmation, and the create says so.
    assert!(
        created.contains("Not in force: ")
            && created.contains("whose tools the operator has not confirmed"),
        "create: {created}"
    );
    confirm_project_library(&orch, "project-proj").await;

    let item = read(&orch, "cairn://p/proj/agents/builder-helper").await;
    assert!(item.contains("Origin: @"), "read: {item}");
    assert!(item.contains("- tools: Read"), "read: {item}");

    // Rewording is not authority: the agent stays in force with no note.
    let reworded = change(
        &orch,
        json!([{ "target": "cairn://p/proj/agents/builder-helper", "mode": "patch", "payload": { "prompt": "p, carefully" } }]),
    )
    .await;
    assert!(reworded.contains("\"applied\""), "reword: {reworded}");
    assert!(!reworded.contains("Not in force"), "reword: {reworded}");

    // Widening a confirmed grant writes, and says the change needs the
    // operator's re-confirmation before it is used.
    let widened = change(
        &orch,
        json!([{ "target": "cairn://p/proj/agents/builder-helper", "mode": "patch", "payload": { "tools": ["Read", "Bash"] } }]),
    )
    .await;
    assert!(widened.contains("\"applied\""), "widen: {widened}");
    assert!(
        widened.contains("Not in force: ")
            && widened.contains("whose tools changed since the operator confirmed them")
            && widened.contains(cairn_core::config::library::TRUST_SETTINGS_PLACE),
        "widen: {widened}"
    );
}

#[tokio::test]
async fn agent_project_scope_create_while_untrusted_says_not_in_force() {
    let (_temp, _db, orch, repo) = project_resource_fixture("proj").await;
    register_project_library(&orch).await;
    let refusal = untrusted_refusal(
        "agent",
        "builder-helper",
        "proj",
        ".cairn/agents/builder-helper.md",
    );
    let not_in_force = format!("Not in force: {refusal}");

    let created = change(
        &orch,
        json!([{
            "target": "cairn://p/proj/agents",
            "mode": "create",
            "payload": { "name": "Builder Helper", "description": "proj", "prompt": "Untrusted helper prompt", "tools": ["Read"] }
        }]),
    )
    .await;
    assert!(created.contains("\"applied\""), "create: {created}");
    let file = repo.join(".cairn/agents/builder-helper.md");
    assert!(file.exists(), "the untrusted create still writes");
    assert!(created.contains(&not_in_force), "create: {created}");

    let item = read(&orch, "cairn://p/proj/agents/builder-helper").await;
    assert!(item.contains(&refusal), "read: {item}");
    assert!(
        !item.contains("Untrusted helper prompt"),
        "read shows no content: {item}"
    );

    let listing = read(&orch, "cairn://p/proj/agents").await;
    let (_, skipped) = listing
        .split_once("## Not in force")
        .unwrap_or_else(|| panic!("listing: {listing}"));
    assert!(
        skipped.contains("builder-helper") && skipped.contains("untrusted: skipped"),
        "listing: {listing}"
    );

    let patched = change(
        &orch,
        json!([{ "target": "cairn://p/proj/agents/builder-helper", "mode": "patch", "payload": { "description": "patched while untrusted" } }]),
    )
    .await;
    assert!(patched.contains("\"applied\""), "patch: {patched}");
    assert!(patched.contains(&not_in_force), "patch: {patched}");
    let body = std::fs::read_to_string(&file).unwrap();
    assert!(body.contains("patched while untrusted"), "body: {body}");
}

// ============================================================================
// Actions (DB-backed)
// ============================================================================

#[tokio::test]
async fn action_workspace_create_read_patch_delete_roundtrip() {
    let (_temp, db, orch) = resource_orchestrator_fixture().await;

    let created = change(
        &orch,
        json!([{
            "target": "cairn://actions",
            "mode": "create",
            "payload": { "name": "Make PR", "description": "opens a PR", "commandTemplate": "gh pr create --title {{title:string}}" }
        }]),
    )
    .await;
    assert!(created.contains("\"applied\""), "create: {created}");

    let id = action_id_by_name(&db, "Make PR").await.expect("action id");
    let uri = format!("cairn://actions/{id}");

    let item = read(&orch, &uri).await;
    assert!(item.contains("opens a PR"), "read: {item}");
    assert!(item.contains("gh pr create"), "template: {item}");

    let collection = read(&orch, "cairn://actions").await;
    assert!(collection.contains("Make PR"), "collection: {collection}");

    let patched = change(
        &orch,
        json!([{ "target": &uri, "mode": "patch", "payload": { "description": "opens a pull request" } }]),
    )
    .await;
    assert!(patched.contains("\"applied\""), "patch: {patched}");
    let item2 = read(&orch, &uri).await;
    assert!(item2.contains("opens a pull request"), "read2: {item2}");

    let deleted = change(&orch, json!([{ "target": &uri, "mode": "delete" }])).await;
    assert!(deleted.contains("\"applied\""), "delete: {deleted}");
    let gone = read(&orch, &uri).await;
    assert!(gone.contains("Action not found"), "gone: {gone}");
}

#[tokio::test]
async fn action_create_rejects_missing_command() {
    let (temp, db) = common::migrated_db().await;
    let orch = orchestrator(&temp, Arc::new(db));

    let missing = change(
        &orch,
        json!([{ "target": "cairn://actions", "mode": "create", "payload": { "name": "No Command" } }]),
    )
    .await;
    assert!(missing.contains("commandTemplate"), "missing: {missing}");
}

#[tokio::test]
async fn action_project_scope_roundtrip_and_guard() {
    let (_temp, db, orch, _repo) = project_resource_fixture("proj").await;

    let created = change(
        &orch,
        json!([{
            "target": "cairn://p/proj/actions",
            "mode": "create",
            "payload": { "name": "Proj Action", "commandTemplate": "echo {{msg:string}}" }
        }]),
    )
    .await;
    assert!(created.contains("\"applied\""), "create: {created}");
    let id = action_id_by_name(&db, "Proj Action")
        .await
        .expect("action id");

    let item = read(&orch, &format!("cairn://p/proj/actions/{id}")).await;
    assert!(item.contains("[project]"), "read: {item}");

    // A workspace action must not resolve behind an explicit project URI.
    change(
        &orch,
        json!([{ "target": "cairn://actions", "mode": "create", "payload": { "name": "WS Action", "commandTemplate": "echo hi" } }]),
    )
    .await;
    let ws_id = action_id_by_name(&db, "WS Action")
        .await
        .expect("ws action id");
    let guarded = read(&orch, &format!("cairn://p/proj/actions/{ws_id}")).await;
    assert!(
        guarded.contains("not found in project proj"),
        "guarded: {guarded}"
    );
}
