//! The camp resources' write path, as an agent's `write` reaches it: an
//! authenticated thread run pitches, retitles, joins and leaves camps.

use super::*;
use crate::db::DbState;
use crate::models::{CampRole, CreateThread};
use crate::orchestrator::OrchestratorBuilder;
use crate::services::testing::{CapturingEmitter, TestServicesBuilder};
use crate::storage::{LocalDb, SearchIndex};
use std::sync::Arc;

async fn orch_with_project() -> (Orchestrator, Arc<CapturingEmitter>) {
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

/// A thread with its session and a live run on it, as an agent holds one.
async fn thread_run(orch: &Orchestrator, name: &str) -> (String, String) {
    let thread_id = crate::threads::crud::create(
        &orch.db.local,
        CreateThread {
            project_id: "p".into(),
            name: Some(name.into()),
            jurisdiction: None,
            definition: None,
            migrated_from_number: None,
            model: None,
        },
    )
    .await
    .unwrap()
    .id;
    let session = crate::threads::ensure_thread_session(&orch.db.local, &thread_id)
        .await
        .unwrap();
    let run_id = format!("run-{name}");
    orch.db
        .local
        .execute(
            "INSERT INTO runs(id, project_id, job_id, status, created_at, updated_at)
             VALUES (?1, 'p', ?2, 'live', 1, 1)",
            (run_id.clone(), session),
        )
        .await
        .unwrap();
    (thread_id, run_id)
}

async fn act(
    orch: &Orchestrator,
    run_id: Option<&str>,
    target: &str,
    mode: ChangeMode,
    payload: Option<serde_json::Value>,
) -> Result<String, String> {
    let request = McpCallbackRequest {
        answer_bound_ms: None,
        thread_id: None,
        cwd: "/tmp".to_string(),
        run_id: run_id.map(str::to_string),
        tool: "change".to_string(),
        payload: serde_json::json!({}),
        tool_use_id: None,
        origin: None,
    };
    let item = ChangeItem {
        target: target.into(),
        mode,
        payload,
    };
    dispatch_resource_change(orch, &request, 0, &item, false)
        .await
        .map(|change| change.summary)
        .map_err(|failure| failure.error)
}

#[tokio::test]
async fn threads_pitch_join_retitle_and_leave_camps_through_their_resources() {
    let (orch, emitter) = orch_with_project().await;
    // Without a general thread, base camp's lead falls to the oldest active
    // thread, and threads created in the same second tie-break on random ids.
    let (general, _) = thread_run(&orch, "general").await;
    let (devops, devops_run) = thread_run(&orch, "devops").await;
    let (execution, exec_run) = thread_run(&orch, "execution").await;
    let base = crate::camps::ensure_project_base_camp(&orch.db.local, "p")
        .await
        .unwrap();
    assert_eq!(base.lead_thread_id, general);

    let pitched = act(
        &orch,
        Some(&devops_run),
        "cairn://p/prj/camps",
        ChangeMode::Append,
        Some(serde_json::json!({"slug": "rebuild", "title": "Rebuild as an action"})),
    )
    .await
    .unwrap();
    assert!(pitched.contains("cairn://p/prj/camps/rebuild"), "{pitched}");
    assert!(emitter
        .events_named("db-change")
        .iter()
        .any(|event| event["table"] == "camps"));

    let joined = act(
        &orch,
        Some(&exec_run),
        "cairn://p/prj/camps/rebuild/participants",
        ChangeMode::Append,
        Some(serde_json::json!({"level": "wake", "reason": "the interface changes placement"})),
    )
    .await
    .unwrap();
    assert!(joined.starts_with("Joined"), "{joined}");

    let confirm = act(
        &orch,
        Some(&exec_run),
        "cairn://p/prj/camps/rebuild/participants",
        ChangeMode::Append,
        Some(serde_json::json!({"level": "confirm", "reason": "veto"})),
    )
    .await
    .unwrap_err();
    assert!(confirm.contains("assigned by the camp's lead"), "{confirm}");

    let not_lead = act(
        &orch,
        Some(&exec_run),
        "cairn://p/prj/camps/rebuild",
        ChangeMode::Patch,
        Some(serde_json::json!({"title": "Mine now"})),
    )
    .await
    .unwrap_err();
    assert!(not_lead.contains("lead"), "{not_lead}");
    let retitled = act(
        &orch,
        Some(&devops_run),
        "cairn://p/prj/camps/rebuild",
        ChangeMode::Patch,
        Some(serde_json::json!({"title": "Rebuild, phase 2"})),
    )
    .await
    .unwrap();
    assert!(retitled.starts_with("Retitled"), "{retitled}");

    let no_run = act(
        &orch,
        None,
        "cairn://p/prj/camps/rebuild/participants",
        ChangeMode::Append,
        Some(serde_json::json!({"level": "watch", "reason": "curious"})),
    )
    .await
    .unwrap_err();
    assert!(no_run.contains("authenticated run"), "{no_run}");

    let leave_base = act(
        &orch,
        Some(&exec_run),
        "cairn://p/prj/camps/base/participants",
        ChangeMode::Delete,
        None,
    )
    .await
    .unwrap_err();
    assert!(leave_base.contains("cannot be removed"), "{leave_base}");
    let opted_up = act(
        &orch,
        Some(&exec_run),
        "cairn://p/prj/camps/base/participants",
        ChangeMode::Append,
        Some(serde_json::json!({"level": "wake", "reason": "rebuild notices change routing"})),
    )
    .await
    .unwrap();
    assert!(opted_up.contains("from watch to wake"), "{opted_up}");

    let left = act(
        &orch,
        Some(&exec_run),
        "cairn://p/prj/camps/rebuild/participants",
        ChangeMode::Delete,
        Some(serde_json::json!({"reason": "agreed"})),
    )
    .await
    .unwrap();
    assert!(left.starts_with("Left"), "{left}");

    let rebuild = orch
        .db
        .local
        .camp_by_slug("p", "rebuild")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rebuild.title, "Rebuild, phase 2");
    assert_eq!(rebuild.lead_thread_id, devops);
    let participants = orch.db.local.camp_participants(&rebuild).await.unwrap();
    assert_eq!(participants.len(), 1);
    assert_eq!(participants[0].role, CampRole::Lead);
    let execution_camps = orch.db.local.camps_for_thread(&execution).await.unwrap();
    assert_eq!(
        execution_camps.len(),
        1,
        "execution is left only in base camp"
    );
    assert_eq!(execution_camps[0].1, CampRole::Wake);
}

#[tokio::test]
async fn issues_join_a_camp_by_reference_only_from_its_participants() {
    let (orch, _) = orch_with_project().await;
    let (_, devops_run) = thread_run(&orch, "devops").await;
    let (_, stranger_run) = thread_run(&orch, "stranger").await;
    crate::camps::ensure_project_base_camp(&orch.db.local, "p")
        .await
        .unwrap();
    act(
        &orch,
        Some(&devops_run),
        "cairn://p/prj/camps",
        ChangeMode::Append,
        Some(serde_json::json!({"slug": "rebuild", "title": "Rebuild"})),
    )
    .await
    .unwrap();

    let refused = act(
        &orch,
        Some(&stranger_run),
        "cairn://p/prj/issues",
        ChangeMode::Append,
        Some(serde_json::json!({"title": "Sneak in", "camp": "cairn://p/prj/camps/rebuild"})),
    )
    .await
    .unwrap_err();
    assert!(refused.contains("does not take part"), "{refused}");
    assert_eq!(
        orch.db
            .local
            .query_opt_i64("SELECT COUNT(*) FROM issues WHERE title = 'Sneak in'", ())
            .await
            .unwrap(),
        Some(0),
        "a refused join leaves no issue behind"
    );

    let filed = act(
        &orch,
        Some(&devops_run),
        "cairn://p/prj/issues",
        ChangeMode::Append,
        Some(serde_json::json!({"title": "Quiesce", "camp": "cairn://p/prj/camps/rebuild"})),
    )
    .await
    .unwrap();
    assert!(
        filed.contains("Joined camp cairn://p/prj/camps/rebuild"),
        "{filed}"
    );
    let number = orch
        .db
        .local
        .query_opt_i64("SELECT number FROM issues WHERE title = 'Quiesce'", ())
        .await
        .unwrap()
        .unwrap();
    let camp = orch
        .db
        .local
        .camp_by_slug("p", "rebuild")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        orch.db
            .local
            .query_opt_text("SELECT camp_id FROM issues WHERE title = 'Quiesce'", ())
            .await
            .unwrap(),
        Some(camp.id.clone())
    );

    let taken_out = act(
        &orch,
        Some(&devops_run),
        &format!("cairn://p/prj/{number}"),
        ChangeMode::Patch,
        Some(serde_json::json!({"camp": null})),
    )
    .await
    .unwrap();
    assert!(
        taken_out.contains("Took the issue out of camp"),
        "{taken_out}"
    );
    assert_eq!(
        orch.db
            .local
            .query_opt_text("SELECT camp_id FROM issues WHERE title = 'Quiesce'", ())
            .await
            .unwrap(),
        None
    );
    let journal = orch
        .db
        .local
        .query_all(
            "SELECT kind FROM camp_events WHERE camp_id = ?1 AND kind LIKE 'issue_%' ORDER BY id",
            (camp.id.clone(),),
            |row| crate::storage::RowExt::text(row, 0),
        )
        .await
        .unwrap();
    assert_eq!(journal, ["issue_joined", "issue_left"]);
}

async fn issue_title(orch: &Orchestrator, number: i64) -> String {
    orch.db
        .local
        .query_opt_text("SELECT title FROM issues WHERE number = ?1", (number,))
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn a_refused_camp_move_leaves_the_whole_issue_unchanged() {
    let (orch, _) = orch_with_project().await;
    let (_, devops_run) = thread_run(&orch, "devops").await;
    let (_, exec_run) = thread_run(&orch, "execution").await;
    let (_, stranger_run) = thread_run(&orch, "stranger").await;
    crate::camps::ensure_project_base_camp(&orch.db.local, "p")
        .await
        .unwrap();
    for (run, slug) in [(&devops_run, "rebuild"), (&exec_run, "other")] {
        act(
            &orch,
            Some(run),
            "cairn://p/prj/camps",
            ChangeMode::Append,
            Some(serde_json::json!({"slug": slug, "title": slug})),
        )
        .await
        .unwrap();
    }
    act(
        &orch,
        Some(&devops_run),
        "cairn://p/prj/issues",
        ChangeMode::Append,
        Some(serde_json::json!({"title": "Quiesce", "camp": "cairn://p/prj/camps/rebuild"})),
    )
    .await
    .unwrap();
    let number = orch
        .db
        .local
        .query_opt_i64("SELECT number FROM issues WHERE title = 'Quiesce'", ())
        .await
        .unwrap()
        .unwrap();
    let target = format!("cairn://p/prj/{number}");

    let moved = act(
        &orch,
        Some(&exec_run),
        &target,
        ChangeMode::Patch,
        Some(serde_json::json!({"title": "changed", "camp": "cairn://p/prj/camps/other"})),
    )
    .await
    .unwrap_err();
    assert!(moved.contains("already joined"), "{moved}");
    assert_eq!(issue_title(&orch, number).await, "Quiesce");

    let taken = act(
        &orch,
        Some(&stranger_run),
        &target,
        ChangeMode::Patch,
        Some(serde_json::json!({"title": "changed", "camp": null})),
    )
    .await
    .unwrap_err();
    assert!(taken.contains("taking part"), "{taken}");
    assert_eq!(issue_title(&orch, number).await, "Quiesce");
}

#[tokio::test]
async fn a_camp_lead_hands_over_before_it_can_close() {
    let (orch, _) = orch_with_project().await;
    let (general, general_run) = thread_run(&orch, "general").await;
    let (devops, devops_run) = thread_run(&orch, "devops").await;
    let base = crate::camps::ensure_project_base_camp(&orch.db.local, "p")
        .await
        .unwrap();
    assert_eq!(
        base.lead_thread_id, general,
        "an active general thread leads base"
    );

    let close = |id: String| crate::models::UpdateThread {
        id,
        name: None,
        jurisdiction: None,
        definition: None,
        status: Some(crate::models::ThreadStatus::Closed),
        model: None,
    };
    let refused = crate::threads::crud::update(&orch.db.local, close(general.clone()))
        .await
        .unwrap_err()
        .to_string();
    assert!(refused.contains("leads camp `base`"), "{refused}");

    let not_offered = act(
        &orch,
        Some(&devops_run),
        "cairn://p/prj/camps/base",
        ChangeMode::Patch,
        Some(serde_json::json!({"lead": "cairn://p/prj/devops"})),
    )
    .await
    .unwrap_err();
    assert!(not_offered.contains("has not offered"), "{not_offered}");
    act(
        &orch,
        Some(&general_run),
        "cairn://p/prj/camps/base",
        ChangeMode::Patch,
        Some(serde_json::json!({"lead": "cairn://p/prj/devops"})),
    )
    .await
    .unwrap();
    let took = act(
        &orch,
        Some(&devops_run),
        "cairn://p/prj/camps/base",
        ChangeMode::Patch,
        Some(serde_json::json!({"lead": "cairn://p/prj/devops"})),
    )
    .await
    .unwrap();
    assert!(took.starts_with("Took the lead"), "{took}");
    assert_eq!(
        orch.db
            .local
            .camp_by_slug("p", "base")
            .await
            .unwrap()
            .unwrap()
            .lead_thread_id,
        devops
    );
    crate::threads::crud::update(&orch.db.local, close(general))
        .await
        .expect("a former lead closes");
}
