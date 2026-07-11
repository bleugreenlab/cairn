//! End-to-end coordinator sleep/wake coverage across subscription lookup, durable
//! push creation, the idle-job nudge, and resumed-prompt delivery.

use crate::common;

use std::collections::HashMap;
use std::sync::Arc;

use cairn_core::internal::db::DbState;
use cairn_core::internal::orchestrator::attention_push::list_pending;
use cairn_core::internal::orchestrator::lifecycle::{evaluate_review_readiness, finalize_run};
use cairn_core::internal::orchestrator::Orchestrator;
use cairn_core::internal::services::testing::{
    MockGitClient, RecordingProcessSpawner, TestServicesBuilder,
};
use cairn_core::internal::services::GitOutput;
use cairn_core::internal::storage::{LocalDb, RowExt, SearchIndex};
use cairn_core::models::{
    ExecutionSnapshot, IssueStatus, NodePosition, RecipeNode, RecipeNodeType, RecipeSnapshot,
    RecipeTrigger, RunStatus, TriggerContext, TriggerType,
};
use cairn_db::turso::params;
use tempfile::TempDir;

const CHILD_URI: &str = "cairn://p/COORD/2";

fn orchestrator(temp: &TempDir, db: Arc<LocalDb>) -> (Orchestrator, RecordingProcessSpawner) {
    let search_index = Arc::new(SearchIndex::open_or_create(temp.path().join("search")).unwrap());
    let db_state = Arc::new(DbState::new(db, search_index));
    let recorder = RecordingProcessSpawner::new();
    let mut git = MockGitClient::new();
    git.expect_run().times(0..).returning(|_, _| {
        Ok(GitOutput {
            success: true,
            stdout: String::new(),
            stderr: String::new(),
        })
    });
    let services = Arc::new(
        TestServicesBuilder::new()
            .with_process(recorder.clone())
            .with_git(git)
            .build(),
    );
    let config_dir = temp.path().join("config");
    std::fs::create_dir_all(config_dir.join("agents")).unwrap();
    std::fs::create_dir_all(config_dir.join("recipes")).unwrap();
    (
        Orchestrator::builder(db_state, services, config_dir).build(),
        recorder,
    )
}

async fn seed_coordinator_and_child(db: &LocalDb, root: &std::path::Path) {
    let root = root.to_string_lossy().to_string();
    let snapshot = ExecutionSnapshot::new(
        RecipeSnapshot {
            id: "coordinator".to_string(),
            name: "Coordinator".to_string(),
            description: None,
            trigger: RecipeTrigger::Manual,
            nodes: Vec::new(),
            edges: Vec::new(),
        },
        HashMap::new(),
        HashMap::new(),
        TriggerContext {
            issue_id: Some("parent".to_string()),
            project_id: "project".to_string(),
            trigger_type: TriggerType::Manual,
            event_payload: None,
            initiated_via: None,
        },
    )
    .to_json()
    .unwrap()
    .replace('\'', "''");
    let child_snapshot = ExecutionSnapshot::new(
        RecipeSnapshot {
            id: "build".to_string(),
            name: "Build".to_string(),
            description: None,
            trigger: RecipeTrigger::Manual,
            nodes: vec![RecipeNode {
                id: "builder".to_string(),
                node_type: RecipeNodeType::Agent,
                name: "Builder".to_string(),
                position: NodePosition { x: 0.0, y: 0.0 },
                parent_id: None,
                trigger_config: None,
                agent_config: None,
                action_config: None,
                checkpoint_config: None,
                artifact_config: None,
                condition_config: None,
                context_config: None,
            }],
            edges: Vec::new(),
        },
        HashMap::new(),
        HashMap::new(),
        TriggerContext {
            issue_id: Some("child".to_string()),
            project_id: "project".to_string(),
            trigger_type: TriggerType::Manual,
            event_payload: None,
            initiated_via: None,
        },
    )
    .to_json()
    .unwrap()
    .replace('\'', "''");
    db.execute_script(&format!(
        "
        INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
          VALUES('project','default','Coordinator','COORD','{root}',1,1);
        INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
          VALUES('parent','project',1,'Parent','active','active','none',1,1);
        INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot)
          VALUES('parent-exec','coordinator','parent','project','running',1,1,'{snapshot}');
        INSERT INTO jobs(id, execution_id, project_id, issue_id, status,
                         uri_segment, node_name, worktree_path, current_session_id, created_at, updated_at)
          VALUES('coordinator','parent-exec','project','parent','complete',
                 'coordinator','Coordinator','{root}','coord-session',1,1);
        INSERT INTO sessions(id, job_id, backend, backend_id, status, sequence, created_at, updated_at)
          VALUES('coord-session','coordinator','claude','claude-session','open',1,2000000000,2000000000);
        INSERT INTO runs(id, project_id, issue_id, job_id, session_id, status, created_at, updated_at, start_mode)
          VALUES('coord-run','project','parent','coordinator','coord-session','complete',1,1,'resume');
        INSERT INTO turns(id, session_id, run_id, job_id, sequence, state, start_reason, created_at, updated_at)
          VALUES('coord-turn','coord-session','coord-run','coordinator',1,'complete','initial',1,1);
        UPDATE jobs SET current_turn_id='coord-turn' WHERE id='coordinator';

        INSERT INTO issues(id, project_id, number, title, status, progress, attention,
                           parent_issue_id, parent_job_id, created_at, updated_at)
          VALUES('child','project',2,'Child','active','waiting','none',
                 'parent','coordinator',1,1);
        INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot)
          VALUES('child-exec','build','child','project','running',1,1,'{child_snapshot}');
        INSERT INTO jobs(id, execution_id, recipe_node_id, project_id, issue_id, status,
                         uri_segment, node_name, branch, worktree_path, created_at, updated_at)
          VALUES('child-builder','child-exec','builder','project','child','complete',
                 'builder','Builder','child-branch','{root}',1,1);
        INSERT INTO runs(id, project_id, issue_id, job_id, status, created_at, updated_at)
          VALUES('child-run','project','child','child-builder','complete',1,1);
        INSERT INTO turns(id, session_id, run_id, job_id, sequence, state, start_reason, created_at, updated_at)
          VALUES('child-turn','child-session','child-run','child-builder',1,'complete','initial',1,1);
        INSERT INTO merge_requests(id, job_id, project_id, issue_id, title, source_branch,
                                   target_branch, status, head_sha, opened_at, updated_at)
          VALUES('child-pr','child-builder','project','child','Child PR','child-branch',
                 'main','open','child-head',1,1);
        INSERT INTO wake_subscriptions(id, job_id, source_kind, source_ref, state,
                                       created_by, created_at, updated_at, one_shot)
          VALUES('child-sub','coordinator','issue','{CHILD_URI}','active','system',1,1,0);
        "
    ))
    .await
    .unwrap();
}

async fn push_rows(db: &LocalDb, key: &str) -> Vec<(String, String, Option<String>)> {
    let key = key.to_string();
    db.read(|conn| {
        let key = key.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT recipient, wake, delivered_event_id FROM attention_pushes
                     WHERE key=?1 ORDER BY created_at, id",
                    params![key.as_str()],
                )
                .await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                out.push((row.text(0)?, row.text(1)?, row.opt_text(2)?));
            }
            Ok(out)
        })
    })
    .await
    .unwrap()
}

async fn attention_event_body(db: &LocalDb) -> String {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT data FROM events WHERE event_type='attention:briefing'
                     ORDER BY created_at DESC, sequence DESC LIMIT 1",
                    (),
                )
                .await?;
            rows.next().await?.map(|row| row.text(0)).transpose()
        })
    })
    .await
    .unwrap()
    .expect("resumed coordinator should persist the delivered attention briefing")
}

#[tokio::test(flavor = "current_thread")]
async fn settled_child_review_wakes_idle_coordinator_while_checks_are_running() {
    let (temp, db) = common::migrated_db().await;
    seed_coordinator_and_child(&db, temp.path()).await;
    let db = Arc::new(db);
    let (orch, recorder) = orchestrator(&temp, db.clone());

    assert!(orch.try_begin_turn_end_checks("child-builder").is_some());
    evaluate_review_readiness(&orch, "child").await;

    let rows = push_rows(&db, &format!("review:{CHILD_URI}")).await;
    let resumed_run = cairn_core::messages::delivery::latest_run_for_job(&db, "coordinator")
        .expect("the coordinator should have a resumed run");
    assert_ne!(
        resumed_run, "coord-run",
        "the nudge must create the coordinator's successor run"
    );
    assert_eq!(rows.len(), 1, "one fingerprinted review wake is canonical");
    assert_eq!(rows[0].0, "coordinator");
    assert_eq!(rows[0].1, "wake");
    assert!(
        rows[0].2.is_some(),
        "the idle resume must drain and stamp the push"
    );
    assert_eq!(
        recorder.spawn_count(),
        1,
        "the idle coordinator must be resumed"
    );

    let body = attention_event_body(&db).await;
    assert!(body.contains("Work product ready for review"), "{body}");
    assert!(body.contains(CHILD_URI), "{body}");

    orch.end_turn_end_checks("child-builder");
    evaluate_review_readiness(&orch, "child").await;
    assert_eq!(
        push_rows(&db, &format!("review:{CHILD_URI}")).await.len(),
        1,
        "later check completion must not duplicate an unchanged review"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn failed_child_finalize_wakes_once_and_delivers_failure_to_idle_coordinator() {
    let (temp, db) = common::migrated_db().await;
    seed_coordinator_and_child(&db, temp.path()).await;
    db.execute_script(
        "
        INSERT INTO sessions(id, job_id, backend, backend_id, status, sequence, created_at, updated_at)
          VALUES('child-session','child-builder','claude','child-backend','open',1,1,1);
        DELETE FROM merge_requests WHERE id='child-pr';
        UPDATE issues
          SET status='failed', progress='failed', attention='none', updated_at=2
          WHERE id='child';
        UPDATE executions SET status='failed' WHERE id='child-exec';
        UPDATE jobs
          SET status='running', current_session_id='child-session',
              current_turn_id='child-turn'
          WHERE id='child-builder';
        UPDATE runs
          SET status='live', session_id='child-session'
          WHERE id='child-run';
        UPDATE turns SET state='pending' WHERE id='child-turn';
        ",
    )
    .await
    .unwrap();
    let db = Arc::new(db);
    let (orch, recorder) = orchestrator(&temp, db.clone());

    // This is the production failed-start sequence: finalize marks the turn/run failed,
    // recompute emits Resolved, then the turn-end edge observes the same terminal
    // issue and emits Resolved again. Durable fingerprint dedupe must collapse the
    // second emit even though the first wake was synchronously delivered.
    finalize_run(&orch, "child-run", RunStatus::Crashed);

    assert_eq!(
        common::scalar_text_by_id(&db, "SELECT status FROM issues WHERE id=?1", "child").await,
        Some(IssueStatus::Failed.to_string())
    );
    let rows = push_rows(&db, &format!("resolved:{CHILD_URI}")).await;
    assert_eq!(
        rows.len(),
        1,
        "recompute plus turn-end must create one resolution row"
    );
    assert_eq!(rows[0].1, "wake");
    assert!(rows[0].2.is_some());
    assert_eq!(
        recorder.spawn_count(),
        1,
        "failed resolution must resume the parent exactly once"
    );
    assert!(list_pending(&db, "child-builder").await.unwrap().is_empty());

    let body = attention_event_body(&db).await;
    assert!(body.contains("Issue Failed"), "{body}");
    assert!(body.contains("retry or delegate a fix"), "{body}");
    assert!(!body.contains("Successfully"), "{body}");
    assert!(body.contains(CHILD_URI), "{body}");
}
