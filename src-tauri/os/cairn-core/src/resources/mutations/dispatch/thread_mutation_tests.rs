//! The THREAD resource's own write path, which the crud-level tests do not
//! reach: they exercise `threads::crud::update` directly, while an agent's
//! `write` arrives here as a `ChangeItem` and takes a different arm.

use super::*;
use crate::db::DbState;
use crate::models::CreateThread;
use crate::orchestrator::OrchestratorBuilder;
use crate::services::testing::{CapturingEmitter, TestServicesBuilder};
use crate::storage::{LocalDb, RowExt, SearchIndex};
use std::sync::Arc;

async fn seeded_orch() -> Orchestrator {
    seeded_orch_with_emitter().await.0
}

#[tokio::test]
async fn procedure_and_automation_resources_mutate_through_their_landed_contracts() {
    let orch = seeded_orch().await;
    let thread_id = seed_thread(&orch).await;
    assert!(crate::threads::procedures::list(&thread_id)
        .await
        .unwrap()
        .is_empty());

    let procedure = ChangeItem {
        target: "cairn://p/prj/roadmap/procedures".into(),
        mode: ChangeMode::Create,
        payload: Some(serde_json::json!({
            "name": "collect",
            "description": "Collect health",
            "body": {"commands": [{"command": "echo ok"}]}
        })),
    };
    apply(&orch, &procedure).await.unwrap();
    let stored = crate::threads::procedures::get(&thread_id, "collect")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.description.as_deref(), Some("Collect health"));

    let automation = ChangeItem {
        target: "cairn://p/prj/roadmap/automations".into(),
        mode: ChangeMode::Create,
        payload: Some(
            serde_json::json!({"procedure": "collect", "every": "5m", "args": ["health"]}),
        ),
    };
    apply(&orch, &automation).await.unwrap();
    let automations =
        crate::orchestrator::automations::list_thread_automations(&orch.db.local, &thread_id)
            .await
            .unwrap();
    assert_eq!(automations.len(), 1);
    assert_eq!(automations[0].args, vec!["health"]);

    let refused = ChangeItem {
        target: "cairn://p/prj/roadmap/procedures/collect".into(),
        mode: ChangeMode::Delete,
        payload: None,
    };
    let error = apply(&orch, &refused).await.unwrap_err();
    assert!(error.error.contains("automations reference it"));
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
    (orch, emitter)
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

async fn current_session(orch: &Orchestrator, thread_id: &str) -> (String, String) {
    orch.db
        .local
        .query_one(
            &format!(
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
    let patch_spec = cairn_common::contract::mutation_spec(
        cairn_common::contract::ResourceKind::Thread,
        ChangeMode::Patch,
    )
    .expect("a thread patch spec");
    for key in [
        "action",
        "jurisdiction",
        "status",
        "definition",
        "name",
        "model",
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
