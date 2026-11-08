//! The THREAD resource's own write path, which the crud-level tests do not
//! reach: they exercise `threads::crud::update` directly, while an agent's
//! `write` arrives here as a `ChangeItem` and takes a different arm.

use super::*;
use crate::db::DbState;
use crate::models::CreateThread;
use crate::orchestrator::OrchestratorBuilder;
use crate::services::testing::{CapturingEmitter, TestServicesBuilder};
use crate::storage::{LocalDb, RowExt, SearchIndex};
use cairn_models::types::catalog::{claude_models, ProviderModelCatalog};
use std::sync::Arc;

async fn seeded_orch() -> Orchestrator {
    seeded_orch_with_emitter().await.0
}

/// The retired surfaces refuse every write with the service shape that
/// replaced them (tenet 1313).
#[tokio::test]
async fn procedure_and_automation_writes_are_refused_with_the_service_shape() {
    let orch = seeded_orch().await;
    seed_thread(&orch).await;
    for (target, mode, payload) in [
        (
            "cairn://p/prj/roadmap/procedures",
            ChangeMode::Create,
            Some(
                serde_json::json!({"name": "collect", "body": {"commands": [{"command": "echo ok"}]}}),
            ),
        ),
        (
            "cairn://p/prj/roadmap/procedures/collect",
            ChangeMode::Delete,
            None,
        ),
        (
            "cairn://p/prj/roadmap/automations",
            ChangeMode::Create,
            Some(serde_json::json!({"procedure": "collect", "every": "5m"})),
        ),
        (
            "cairn://p/prj/roadmap/automations/a-1",
            ChangeMode::Patch,
            Some(serde_json::json!({"state": "muted"})),
        ),
    ] {
        let item = ChangeItem {
            target: target.into(),
            mode,
            payload,
        };
        let error = apply(&orch, &item).await.unwrap_err();
        assert!(
            error.error.contains("tenet 1313"),
            "{target}: {}",
            error.error
        );
        assert!(
            error.error.contains("manifest"),
            "{target}: {}",
            error.error
        );
    }
}

async fn apply_dry_run(orch: &Orchestrator, item: &ChangeItem) -> ResourceMutationResult<String> {
    dispatch_resource_change(orch, &request(), 0, item, true)
        .await
        .map(|change| change.summary)
}

#[tokio::test]
async fn agent_thread_mutations_emit_scoped_db_changes_and_dry_runs_stay_silent() {
    let (orch, emitter) = seeded_orch_with_emitter().await;
    let thread_id = seed_thread(&orch).await;

    apply_dry_run(&orch, &patch(serde_json::json!({ "name": "preview" })))
        .await
        .unwrap();
    assert!(emitter.events_named("db-change").is_empty());

    apply(&orch, &patch(serde_json::json!({ "name": "north-star" })))
        .await
        .unwrap();
    let events = emitter.events_named("db-change");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["table"], "threads");
    assert_eq!(events[0]["action"], "update");
    assert_eq!(events[0]["threadId"], thread_id);
    assert_eq!(events[0]["projectId"], "p");
    assert_eq!(events[0]["name"], "north-star");
    assert_eq!(events[0]["previousName"], "roadmap");

    let second = ChangeItem {
        target: "cairn://p/prj/north-star".into(),
        mode: ChangeMode::Patch,
        payload: Some(serde_json::json!({ "name": "planning-board" })),
    };
    apply(&orch, &second).await.unwrap();
    let transitions = emitter.events_named("db-change");
    assert_eq!(transitions[1]["previousName"], "north-star");
    assert_eq!(transitions[1]["name"], "planning-board");

    let create = ChangeItem {
        target: "cairn://p/prj/threads".into(),
        mode: ChangeMode::Append,
        payload: Some(serde_json::json!({ "name": "planning" })),
    };
    apply(&orch, &create).await.unwrap();
    let created = emitter.events_named("db-change");
    assert_eq!(created.len(), 3);
    assert_eq!(created[2]["action"], "insert");
    assert_eq!(created[2]["name"], "planning");

    let delete = ChangeItem {
        target: "cairn://p/prj/planning".into(),
        mode: ChangeMode::Delete,
        payload: None,
    };
    apply(&orch, &delete).await.unwrap();
    let deleted = emitter.events_named("db-change");
    assert_eq!(deleted.len(), 4);
    assert_eq!(deleted[3]["action"], "delete");
    assert_eq!(deleted[3]["name"], "planning");
}

#[tokio::test]
async fn thread_creation_resource_refuses_model_keys_without_persisting() {
    let (orch, emitter) = seeded_orch_with_emitter().await;
    for selection in [
        serde_json::json!({"model": "gpt-5.5"}),
        serde_json::json!({"model": {"backend": "codex", "model": "gpt-5.5"}}),
        serde_json::json!({"backend": "codex"}),
    ] {
        let mut payload = selection.as_object().unwrap().clone();
        payload.insert("name".into(), serde_json::json!("refused"));
        let item = ChangeItem {
            target: "cairn://p/prj/threads".into(),
            mode: ChangeMode::Append,
            payload: Some(serde_json::Value::Object(payload)),
        };
        let refusal = apply(&orch, &item).await.unwrap_err();
        assert!(
            refusal.error.contains("Unknown payload key"),
            "{}",
            refusal.error
        );
        assert!(crate::threads::crud::list(&orch.db.local, "p")
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            orch.db
                .local
                .query_opt_i64("SELECT count(*) FROM jobs WHERE project_id = 'p'", (),)
                .await
                .unwrap(),
            Some(0)
        );
        assert!(emitter.events_named("db-change").is_empty());
    }
}

async fn seeded_orch_with_emitter() -> (Orchestrator, Arc<CapturingEmitter>) {
    let local = LocalDb::open_migrated_for_test(tempfile::tempdir().unwrap().keep().join("t.db"))
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
    let emitter = Arc::new(CapturingEmitter::new());
    let orch = OrchestratorBuilder::new(
        db,
        Arc::new(
            TestServicesBuilder::new()
                .with_emitter(emitter.clone())
                .build(),
        ),
        tempfile::tempdir().unwrap().keep(),
    )
    .build();
    orch.update_settings(crate::models::UpdateSettings {
        enabled_providers: Some(vec!["claude".into(), "codex".into()]),
        ..Default::default()
    })
    .unwrap();
    let mut codex = claude_models().remove(0);
    codex.id = "gpt-5.6-sol".into();
    codex.model = "gpt-5.6-sol".into();
    codex.display_name = "gpt-5.6-sol".into();
    for (backend, models) in [("claude", claude_models()), ("codex", vec![codex])] {
        orch.seed_model_catalog_for_test(ProviderModelCatalog {
            backend: backend.into(),
            models,
            options: Vec::new(),
            refreshed_at: Some(1),
            error: None,
        });
    }
    (orch, emitter)
}

fn request() -> McpCallbackRequest {
    McpCallbackRequest {
        answer_bound_ms: None,
        thread_id: None,
        cwd: "/tmp".to_string(),
        run_id: None,
        tool: "change".to_string(),
        payload: serde_json::json!({}),
        tool_use_id: None,
        origin: None,
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
            format!(
                "SELECT j.model FROM jobs j WHERE j.thread_id = ?1 AND {}",
                crate::threads::SESSION_JOB_SHAPE
            ),
            cairn_db::turso::params![thread_id],
            |row| row.opt_text(0),
        )
        .await
        .unwrap()
}

async fn current_session(orch: &Orchestrator, thread_id: &str) -> (String, String) {
    orch.db
        .local
        .query_one(
            format!(
                "SELECT s.id, s.backend FROM sessions s
                 JOIN jobs j ON j.current_session_id = s.id
                 WHERE j.thread_id = ?1 AND {}",
                crate::threads::SESSION_JOB_SHAPE
            ),
            cairn_db::turso::params![thread_id],
            |row| Ok((row.text(0)?, row.text(1)?)),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn same_provider_thread_model_patch_keeps_the_native_session() {
    let orch = seeded_orch().await;
    let thread_id = seed_thread(&orch).await;
    let before = current_session(&orch, &thread_id).await;

    apply(&orch, &patch(serde_json::json!({ "model": "fable" })))
        .await
        .unwrap();

    assert_eq!(
        session_model(&orch, &thread_id).await.as_deref(),
        Some("fable"),
        "the edit updates the job's launch model"
    );
    assert_eq!(current_session(&orch, &thread_id).await, before);
}

#[tokio::test]
async fn cross_provider_thread_model_patch_rotates_at_edit_time() {
    let orch = seeded_orch().await;
    let thread_id = seed_thread(&orch).await;
    let job_id = session_job_id(&orch).await;
    let (source_id, _) = current_session(&orch, &thread_id).await;
    orch.db
        .local
        .execute(
            "UPDATE sessions SET backend = 'codex' WHERE id = ?1",
            (source_id.clone(),),
        )
        .await
        .unwrap();
    orch.db
        .local
        .execute(
            "UPDATE jobs SET model = 'gpt-5.6-sol' WHERE id = ?1",
            (job_id.clone(),),
        )
        .await
        .unwrap();
    orch.db
        .local
        .execute(
            "INSERT INTO runs (id, job_id, project_id, status, created_at, updated_at)
             VALUES ('provider-change-run', ?1, 'p', 'live', 1, 1)",
            (job_id.clone(),),
        )
        .await
        .unwrap();

    // A task this session spawned carries the thread's id too. The edit must
    // address the session job by its shape, never "a job of this thread", or a
    // completed task would be re-modelled by an edit that has nothing to do with
    // the model it actually ran on.
    orch.db
        .local
        .execute(
            "INSERT INTO jobs (id, thread_id, parent_job_id, project_id, node_name,
                               uri_segment, status, model, created_at, updated_at)
             VALUES ('decoy-task', ?1, ?2, 'p', 'Survey', 'survey', 'complete', 'sonnet', 9, 9)",
            (thread_id.clone(), job_id.clone()),
        )
        .await
        .unwrap();

    apply(&orch, &patch(serde_json::json!({ "model": "fable" })))
        .await
        .unwrap();

    assert_eq!(
        orch.db
            .local
            .query_text("SELECT model FROM jobs WHERE id = 'decoy-task'", ())
            .await
            .unwrap()
            .as_deref(),
        Some("sonnet"),
        "a spawned task keeps the model it ran with"
    );

    let (successor_id, successor_backend) = current_session(&orch, &thread_id).await;
    assert_ne!(
        successor_id, source_id,
        "the edit eagerly mints a successor"
    );
    assert_eq!(successor_backend, "claude");
    assert_eq!(
        orch.db
            .local
            .query_one(
                "SELECT status, terminal_reason, replaced_by_id FROM sessions WHERE id = ?1",
                (source_id,),
                |row| Ok((row.text(0)?, row.opt_text(1)?, row.opt_text(2)?)),
            )
            .await
            .unwrap(),
        (
            "closed".into(),
            Some("provider_change".into()),
            Some(successor_id.clone())
        )
    );
    assert_eq!(
        orch.db
            .local
            .query_text(
                "SELECT current_session_id FROM jobs WHERE id = ?1",
                (job_id,)
            )
            .await
            .unwrap()
            .as_deref(),
        Some(successor_id.as_str())
    );
    assert_eq!(
        orch.db
            .local
            .query_one(
                "SELECT status, exit_reason FROM runs WHERE id = 'provider-change-run'",
                (),
                |row| Ok((row.text(0)?, row.opt_text(1)?)),
            )
            .await
            .unwrap(),
        ("exited".into(), Some("provider_change".into()))
    );
    let boundary = orch
        .db
        .local
        .query_text(
            "SELECT data FROM events WHERE run_id = 'provider-change-run' AND event_type = 'system:session_boundary'",
            (),
        )
        .await
        .unwrap()
        .expect("provider-change boundary");
    // The typed detail rides in the event's `raw`, which is where every
    // transcript reader — the row model included — looks for it.
    let boundary: serde_json::Value = serde_json::from_str(&boundary).unwrap();
    assert_eq!(boundary["raw"]["kind"], "provider_change");
    assert_eq!(boundary["raw"]["fromBackend"], "codex");
    assert_eq!(boundary["raw"]["toBackend"], "claude");
    assert_eq!(boundary["raw"]["model"], "fable");
}

async fn session_job_id(orch: &Orchestrator) -> String {
    orch.db
        .local
        .read(|conn| {
            Box::pin(async move {
                crate::threads::session_job_id_by_name_conn(conn, "prj", "roadmap").await
            })
        })
        .await
        .unwrap()
        .expect("thread session job")
}

#[tokio::test]
async fn thread_stop_interrupts_live_run_and_dry_run_is_silent() {
    let orch = seeded_orch().await;
    seed_thread(&orch).await;
    let job_id = session_job_id(&orch).await;
    orch.db.local.execute(
        "INSERT INTO runs (id, job_id, project_id, status, created_at, updated_at) VALUES ('thread-live-run', ?1, 'p', 'live', 1, 1)",
        (job_id.clone(),),
    ).await.unwrap();
    let item = patch(serde_json::json!({"action": "stop"}));

    let preview = apply_dry_run(&orch, &item).await.unwrap();
    assert!(preview.contains("Would stop") && preview.contains("thread-live-run"));
    assert_eq!(
        orch.db
            .local
            .query_text("SELECT status FROM runs WHERE id = 'thread-live-run'", ())
            .await
            .unwrap()
            .as_deref(),
        Some("live")
    );

    let summary = apply(&orch, &item).await.unwrap();
    assert!(summary.contains("Stopped") && summary.contains("thread-live-run"));
    assert_ne!(
        orch.db
            .local
            .query_text("SELECT status FROM runs WHERE id = 'thread-live-run'", ())
            .await
            .unwrap()
            .as_deref(),
        Some("live")
    );
}

#[tokio::test]
async fn thread_stop_cancels_runless_pending_turn_and_idles_job() {
    let orch = seeded_orch().await;
    seed_thread(&orch).await;
    let job_id = session_job_id(&orch).await;
    let session_id = orch
        .db
        .local
        .query_text(
            "SELECT current_session_id FROM jobs WHERE id = ?1",
            (job_id.clone(),),
        )
        .await
        .unwrap()
        .expect("thread session");
    let snapshot = serde_json::json!({
        "recipe": {
            "id": "host-thread", "name": "Session Host", "description": null,
            "trigger": "manual", "nodes": [], "edges": []
        },
        "agents": {},
        "skills": {},
        "triggerContext": { "issueId": null, "projectId": "p", "triggerType": "manual" },
        "delegatedPackets": [],
        "createdAt": 1
    })
    .to_string();
    orch.db.local.execute(
        "INSERT INTO executions (id, recipe_id, project_id, status, started_at, snapshot, triggered_by)
         VALUES ('thread-host-execution', 'host-thread', 'p', 'running', 1, ?1, 'manual')",
        (snapshot,),
    ).await.unwrap();
    orch.db
        .local
        .execute(
            "UPDATE jobs SET execution_id = 'thread-host-execution' WHERE id = ?1",
            (job_id.clone(),),
        )
        .await
        .unwrap();
    orch.db.local.execute(
        "INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, created_at, updated_at) VALUES ('thread-pending-turn', ?1, ?2, 1, 'pending', 'follow_up', 1, 1)",
        cairn_db::turso::params![session_id, job_id.clone()],
    ).await.unwrap();
    orch.db.local.execute(
        "UPDATE jobs SET status = 'running', current_turn_id = 'thread-pending-turn' WHERE id = ?1",
        (job_id.clone(),),
    ).await.unwrap();

    crate::execution::advancement::recompute_job(&orch, &job_id).unwrap();
    assert_eq!(
        orch.db
            .local
            .query_text("SELECT status FROM jobs WHERE id = ?1", (job_id.clone(),))
            .await
            .unwrap()
            .as_deref(),
        Some("running"),
        "a passive thread host with a live pending turn is running"
    );

    let summary = apply(&orch, &patch(serde_json::json!({"action": "stop"})))
        .await
        .unwrap();
    assert!(summary.contains("job is now idle"), "got: {summary}");
    assert_eq!(
        orch.db
            .local
            .query_text(
                "SELECT state FROM turns WHERE id = 'thread-pending-turn'",
                ()
            )
            .await
            .unwrap()
            .as_deref(),
        Some("cancelled")
    );
    assert_eq!(
        orch.db
            .local
            .query_text("SELECT status FROM jobs WHERE id = ?1", (job_id.clone(),))
            .await
            .unwrap()
            .as_deref(),
        Some("idle")
    );

    orch.db
        .local
        .execute(
            "UPDATE jobs SET status = 'complete' WHERE id = ?1",
            (job_id.clone(),),
        )
        .await
        .unwrap();
    let direct = orch
        .db
        .local
        .read({
            let job_id = job_id.clone();
            move |conn| {
                let job_id = job_id.clone();
                Box::pin(async move {
                    crate::execution::advancement::recompute_job_status_conn(conn, &job_id).await
                })
            }
        })
        .await
        .unwrap();
    assert_eq!(direct, cairn_db::models::JobStatus::Complete);

    crate::execution::advancement::recompute_job(&orch, &job_id).unwrap();
    assert_eq!(
        orch.db
            .local
            .query_text("SELECT status FROM jobs WHERE id = ?1", (job_id,))
            .await
            .unwrap()
            .as_deref(),
        Some("complete"),
        "a closed thread's terminal status is sticky in the execution sweep"
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
    let patch_spec =
        cairn_common::contract::mutation_spec(cairn_uri::ResourceKind::Thread, ChangeMode::Patch)
            .expect("a thread patch spec");
    for key in [
        "action",
        "jurisdiction",
        "status",
        "definition",
        "name",
        "model",
        "executor",
    ] {
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
            via_command.thread.status,
            via_command.thread.name,
            via_command.thread.jurisdiction
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

async fn thread_overview(orch: &Orchestrator) -> String {
    crate::resources::threads::read_thread(&orch.db.local, "prj", "roadmap", &[], 3).await
}

/// A thread's placement default (cairn/5920): the patch echoes the line, the
/// overview shows it, and `executor:null` takes it away again.
#[tokio::test]
async fn a_thread_placement_default_round_trips_through_patch_and_overview() {
    let orch = seeded_orch().await;
    seed_thread(&orch).await;

    let set = apply(
        &orch,
        &patch(serde_json::json!({ "executor": { "os": "linux" } })),
    )
    .await
    .unwrap();
    assert!(
        set.starts_with("Placement default: os linux (set ") && set.ends_with(" by operator)."),
        "{set}"
    );
    let overview = thread_overview(&orch).await;
    assert!(
        overview.contains("Placement default: os linux (set "),
        "{overview}"
    );

    // A rename leaves the default where it was: it is the thread's, by id.
    let renamed = apply(
        &orch,
        &patch(serde_json::json!({ "name": "north-star", "executor": { "os": "macos" } })),
    )
    .await
    .unwrap();
    assert!(
        renamed.contains("Patched thread cairn://p/prj/north-star"),
        "{renamed}"
    );
    assert!(renamed.contains("Placement default: os macos"), "{renamed}");
    let back = ChangeItem {
        target: "cairn://p/prj/north-star".into(),
        mode: ChangeMode::Patch,
        payload: Some(serde_json::json!({ "name": "roadmap" })),
    };
    apply(&orch, &back).await.unwrap();
    assert!(thread_overview(&orch)
        .await
        .contains("Placement default: os macos"));

    let cleared = apply(&orch, &patch(serde_json::json!({ "executor": null })))
        .await
        .unwrap();
    assert_eq!(cleared, "Placement default: none (cleared).");
    assert!(!thread_overview(&orch).await.contains("Placement default"));
}

/// The patch takes the selector rules a per-call `executor` does, and a name
/// is refused at set time when no machine answers to it.
#[tokio::test]
async fn a_placement_default_naming_nothing_is_refused_with_the_machines_that_exist() {
    let orch = seeded_orch().await;
    seed_thread(&orch).await;
    let ghost = apply(
        &orch,
        &patch(serde_json::json!({ "executor": { "name": "ghost" } })),
    )
    .await
    .unwrap_err();
    assert!(
        ghost.error.contains("no executor is named ghost"),
        "{}",
        ghost.error
    );
    assert!(ghost.error.contains("cairn://executors"), "{}", ghost.error);
    let both = apply(
        &orch,
        &patch(serde_json::json!({ "executor": { "name": "bbs", "os": "linux" } })),
    )
    .await
    .unwrap_err();
    assert!(both.error.contains("never both"), "{}", both.error);
    let empty = apply(&orch, &patch(serde_json::json!({ "executor": {} })))
        .await
        .unwrap_err();
    assert!(empty.error.contains("executor:null"), "{}", empty.error);
    assert!(!thread_overview(&orch).await.contains("Placement default"));
}

/// A thread's task has a default of its own, on its own job, and never shares
/// the thread's.
#[tokio::test]
async fn a_thread_tasks_placement_default_is_its_own() {
    let orch = seeded_orch().await;
    let thread_id = seed_thread(&orch).await;
    let session = crate::threads::ensure_thread_session(&orch.db.local, &thread_id)
        .await
        .unwrap();
    orch.db
        .local
        .execute(
            "INSERT INTO jobs (id, parent_job_id, project_id, status, node_name, uri_segment, created_at, updated_at) VALUES ('j-probe', ?1, 'p', 'running', 'probe', 'probe', 3, 3)",
            (session.as_str(),),
        )
        .await
        .unwrap();
    apply(
        &orch,
        &patch(serde_json::json!({ "executor": { "os": "linux" } })),
    )
    .await
    .unwrap();
    let task = ChangeItem {
        target: "cairn://p/prj/roadmap/task/probe".into(),
        mode: ChangeMode::Patch,
        payload: Some(serde_json::json!({ "executor": { "os": "macos" } })),
    };
    let set = apply(&orch, &task).await.unwrap();
    assert!(set.starts_with("Placement default: os macos"), "{set}");
    use crate::fleet::placement_default::{load, DefaultOwner};
    let task_default = load(&orch.db.local, &DefaultOwner::Job("j-probe".into()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(task_default.selector.os.as_deref(), Some("macos"));
    let thread_default = load(&orch.db.local, &DefaultOwner::Thread(thread_id.clone()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(thread_default.selector.os.as_deref(), Some("linux"));
    // The session job resolves to its thread's row, not a row of its own.
    assert_eq!(
        DefaultOwner::for_job(&orch.db.local, &session)
            .await
            .unwrap(),
        DefaultOwner::Thread(thread_id)
    );
}

/// A node patch says one thing: an action or a placement default, never both,
/// refused at the contract before anything resolves.
#[tokio::test]
async fn a_node_patch_carrying_both_action_and_executor_is_refused() {
    let orch = seeded_orch().await;
    let item = ChangeItem {
        target: "cairn://p/prj/1/1/builder".into(),
        mode: ChangeMode::Patch,
        payload: Some(serde_json::json!({ "action": "stop", "executor": { "os": "linux" } })),
    };
    let refused = apply(&orch, &item).await.unwrap_err();
    assert!(
        refused.error.contains("Unknown payload key"),
        "{}",
        refused.error
    );
    assert!(
        refused
            .error
            .contains("set or clear the node's placement default")
            || refused.error.contains("stop a running node"),
        "{}",
        refused.error
    );
}

/// An `executor` beside other keys is validated with them: a default naming
/// nothing refuses the whole patch, so the rename it rode with never happens.
#[tokio::test]
async fn a_refused_placement_default_applies_none_of_the_patch() {
    let orch = seeded_orch().await;
    let thread_id = seed_thread(&orch).await;
    for executor in [
        serde_json::json!({ "name": "ghost" }),
        serde_json::json!({ "name": "bbs", "os": "linux" }),
    ] {
        apply(
            &orch,
            &patch(serde_json::json!({ "name": "north-star", "executor": executor })),
        )
        .await
        .expect_err("an invalid default refuses the patch");
        let thread = crate::threads::crud::get(&orch.db.local, &thread_id)
            .await
            .unwrap()
            .expect("the thread still exists");
        assert_eq!(thread.name, "roadmap", "the rename must not have applied");
    }
}
