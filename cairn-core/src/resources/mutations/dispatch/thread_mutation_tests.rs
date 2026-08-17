//! The THREAD resource's own write path, which the crud-level tests do not
//! reach: they exercise `threads::crud::update` directly, while an agent's
//! `write` arrives here as a `ChangeItem` and takes a different arm.

use super::*;
use crate::db::DbState;
use crate::models::CreateThread;
use crate::orchestrator::OrchestratorBuilder;
use crate::services::testing::TestServicesBuilder;
use crate::storage::{LocalDb, MigrationRunner, RowExt, SearchIndex, TURSO_MIGRATIONS};
use std::sync::Arc;

async fn seeded_orch() -> Orchestrator {
    let local = LocalDb::open(tempfile::tempdir().unwrap().keep().join("t.db"))
        .await
        .unwrap();
    MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
        .run(&local)
        .await
        .unwrap();
    local
        .execute_script(
            "INSERT INTO workspaces (id,name,created_at,updated_at) VALUES ('w','W',1,1);
             INSERT INTO projects (id,workspace_id,name,key,repo_path,created_at,updated_at)
             VALUES ('p','w','P','prj','/tmp/p',1,1);",
        )
        .await
        .unwrap();
    let search =
        Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
    let db = Arc::new(DbState::new(Arc::new(local), search));
    OrchestratorBuilder::new(
        db,
        Arc::new(TestServicesBuilder::new().build()),
        tempfile::tempdir().unwrap().keep(),
    )
    .build()
}

fn request() -> McpCallbackRequest {
    McpCallbackRequest {
        thread_id: None,
        cwd: "/tmp".to_string(),
        run_id: None,
        tool: "change".to_string(),
        payload: serde_json::json!({}),
        tool_use_id: None,
    }
}

fn patch(payload: serde_json::Value) -> ChangeItem {
    ChangeItem {
        target: "cairn://p/prj/roadmap".to_string(),
        mode: ChangeMode::Patch,
        payload: Some(payload),
    }
}

async fn apply(orch: &Orchestrator, item: &ChangeItem) -> ResourceMutationResult<String> {
    dispatch_resource_change(orch, &request(), 0, item, false)
        .await
        .map(|change| change.summary)
}

async fn seed_thread(orch: &Orchestrator) -> String {
    crate::threads::crud::create(
        &orch.db.local,
        CreateThread {
            project_id: "p".into(),
            name: Some("roadmap".into()),
            jurisdiction: None,
            definition: None,
            migrated_from_number: None,
            model: None,
        },
    )
    .await
    .unwrap()
    .id
}

async fn session_model(orch: &Orchestrator, thread_id: &str) -> Option<String> {
    orch.db
        .local
        .query_one(
            &format!(
                "SELECT j.model FROM jobs j WHERE j.thread_id = ?1 AND {}",
                crate::threads::SESSION_JOB_SHAPE
            ),
            cairn_db::turso::params![thread_id],
            |row| row.opt_text(0),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn patch_re_models_the_thread_session() {
    let orch = seeded_orch().await;
    let thread_id = seed_thread(&orch).await;

    apply(&orch, &patch(serde_json::json!({ "model": "gpt-5.6-sol" })))
        .await
        .unwrap();

    assert_eq!(
        session_model(&orch, &thread_id).await.as_deref(),
        Some("gpt-5.6-sol"),
        "an agent's write re-models the thread the same way the desktop menu does"
    );
}

#[tokio::test]
async fn patch_leaves_the_rest_of_the_thread_alone_when_only_the_model_moves() {
    let orch = seeded_orch().await;
    let thread_id = seed_thread(&orch).await;

    apply(&orch, &patch(serde_json::json!({ "model": "fable" })))
        .await
        .unwrap();

    let thread = crate::threads::crud::get(&orch.db.local, &thread_id)
        .await
        .unwrap()
        .expect("the thread");
    assert_eq!(thread.name, "roadmap");
    assert_eq!(thread.status, cairn_db::models::ThreadStatus::Active);
}

#[tokio::test]
async fn an_empty_model_is_refused_rather_than_silently_clearing_one() {
    let orch = seeded_orch().await;
    let thread_id = seed_thread(&orch).await;

    let refused = apply(&orch, &patch(serde_json::json!({ "model": "   " }))).await;
    assert!(refused.is_err(), "a blank model names nothing");
    assert_eq!(session_model(&orch, &thread_id).await, None);

    let refused = apply(&orch, &patch(serde_json::json!({ "model": 5 }))).await;
    assert!(refused.is_err(), "a non-string model names nothing");
}

#[tokio::test]
async fn the_patch_contract_advertises_every_key_the_arm_accepts() {
    // The affordance block is how an agent discovers this at all, so an accepted
    // key that the contract omits is unreachable in practice.
    let patch_spec = cairn_common::contract::mutation_spec(
        cairn_common::contract::ResourceKind::Thread,
        ChangeMode::Patch,
    )
    .expect("a thread patch spec");
    for key in ["jurisdiction", "status", "definition", "name", "model"] {
        assert!(
            patch_spec.optional.iter().any(|spec| spec.key == key),
            "patch accepts `{key}` but the contract never mentions it"
        );
    }
    assert!(
        !patch_spec.optional.iter().any(|spec| spec.key == "title"),
        "a thread has one identifier, so the contract must not offer a title"
    );
}

/// A payload still naming a `title` is REFUSED, not quietly stripped. An agent
/// that keeps sending the retired key and gets a silent success has no way to
/// learn it retired, and the discarded value is the second identifier returning
/// by another door.
#[tokio::test]
async fn a_retired_title_is_refused_on_both_create_and_patch() {
    let orch = seeded_orch().await;
    let thread_id = seed_thread(&orch).await;

    let refused = apply(
        &orch,
        &patch(serde_json::json!({ "title": "Roadmap", "jurisdiction": "own it" })),
    )
    .await
    .expect_err("a title names a field that no longer exists");
    assert!(
        refused.error.contains("one identifier"),
        "the refusal has to say what replaced it, got: {}",
        refused.error
    );
    let thread = crate::threads::crud::get(&orch.db.local, &thread_id)
        .await
        .unwrap()
        .expect("the thread");
    assert_eq!(
        thread.jurisdiction, None,
        "a refused patch applies none of its keys"
    );

    let create = ChangeItem {
        target: "cairn://p/prj/threads".to_string(),
        mode: ChangeMode::Append,
        payload: Some(serde_json::json!({ "name": "planning", "title": "Planning" })),
    };
    assert!(
        apply(&orch, &create).await.is_err(),
        "creation refuses the retired key too"
    );
    assert!(
        crate::threads::crud::list(&orch.db.local, "p")
            .await
            .unwrap()
            .iter()
            .all(|thread| thread.name != "planning"),
        "a refused create leaves no thread behind"
    );
}

/// Closing and reopening through the resource patch reaches the same persisted
/// state the desktop command reaches, because both build the same
/// `crud::update` operation. Two entry points, one implementation — which is
/// the whole reason the direct-SQL status path was removed.
#[tokio::test]
async fn closing_and_reopening_through_the_resource_matches_the_desktop_update() {
    let orch = seeded_orch().await;
    let thread_id = seed_thread(&orch).await;

    apply(&orch, &patch(serde_json::json!({ "status": "closed" })))
        .await
        .unwrap();
    let via_resource = crate::threads::crud::get(&orch.db.local, &thread_id)
        .await
        .unwrap()
        .expect("the thread");
    assert_eq!(via_resource.status, cairn_db::models::ThreadStatus::Closed);

    // Reopen and re-close, this time through the command's own path.
    let update = |status| crate::models::UpdateThread {
        id: thread_id.clone(),
        name: None,
        jurisdiction: None,
        definition: None,
        status: Some(status),
        model: None,
    };
    crate::threads::crud::update(
        &orch.db.local,
        update(cairn_db::models::ThreadStatus::Active),
    )
    .await
    .unwrap();
    let via_command = crate::threads::crud::update(
        &orch.db.local,
        update(cairn_db::models::ThreadStatus::Closed),
    )
    .await
    .unwrap();

    assert_eq!(
        (
            via_command.status,
            via_command.name,
            via_command.jurisdiction
        ),
        (
            via_resource.status,
            via_resource.name,
            via_resource.jurisdiction
        ),
        "the agent-side patch and the desktop command land on one end state"
    );
}

/// Name-based resolution must keep finding a closed thread, or a thread could be
/// closed and then never reopened, renamed, or deleted from the resource side.
#[tokio::test]
async fn a_closed_thread_is_still_addressable_by_name() {
    let orch = seeded_orch().await;
    let thread_id = seed_thread(&orch).await;
    apply(&orch, &patch(serde_json::json!({ "status": "closed" })))
        .await
        .unwrap();

    apply(&orch, &patch(serde_json::json!({ "status": "active" })))
        .await
        .expect("a closed thread resolves by name so it can be reopened");

    assert_eq!(
        crate::threads::crud::get(&orch.db.local, &thread_id)
            .await
            .unwrap()
            .expect("the thread")
            .status,
        cairn_db::models::ThreadStatus::Active
    );
}

#[tokio::test]
async fn an_unknown_status_is_refused_and_changes_nothing() {
    let orch = seeded_orch().await;
    let thread_id = seed_thread(&orch).await;

    let refused = apply(
        &orch,
        &patch(serde_json::json!({ "status": "archived", "jurisdiction": "own it" })),
    )
    .await
    .expect_err("the vocabulary is exactly active and closed");
    assert!(
        refused.error.contains("active or closed"),
        "the refusal names the whole vocabulary, got: {}",
        refused.error
    );

    let thread = crate::threads::crud::get(&orch.db.local, &thread_id)
        .await
        .unwrap()
        .expect("the thread");
    assert_eq!(thread.status, cairn_db::models::ThreadStatus::Active);
    assert_eq!(
        thread.jurisdiction, None,
        "a refused patch applies none of its keys"
    );
}
