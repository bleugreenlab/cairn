//! Shared integration test helpers.

#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use cairn_core::internal::db::DbState;
use cairn_core::internal::mcp::handlers::files::handle_change;
use cairn_core::internal::mcp::handlers::issue_resources::handle_read_issue_resource;
use cairn_core::internal::mcp::types::McpCallbackRequest;
use cairn_core::internal::orchestrator::Orchestrator;
use cairn_core::internal::services::testing::TestServicesBuilder;
use cairn_core::internal::storage::{
    DbError, DbResult, LocalDb, MigrationRunner, RowExt, SearchIndex, TURSO_MIGRATIONS,
};
use serde_json::{json, Value};
use tempfile::{tempdir, TempDir};
use turso::params;

pub async fn migrated_db() -> (TempDir, LocalDb) {
    let temp = tempdir().unwrap();
    let db = LocalDb::open(temp.path().join("cairn.turso.db"))
        .await
        .unwrap();
    MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
        .run(&db)
        .await
        .unwrap();
    (temp, db)
}

pub fn orchestrator(temp: &TempDir, db: Arc<LocalDb>) -> Orchestrator {
    let search_index = Arc::new(SearchIndex::open_or_create(temp.path().join("search")).unwrap());
    let db_state = Arc::new(DbState::new(db, search_index));
    let services = Arc::new(TestServicesBuilder::new().build());
    Orchestrator::builder(db_state, services, temp.path().join("config")).build()
}

pub fn config_dir(temp: &TempDir) -> PathBuf {
    temp.path().join("config")
}

pub async fn resource_orchestrator_fixture() -> (TempDir, Arc<LocalDb>, Orchestrator) {
    let (temp, db) = migrated_db().await;
    let db = Arc::new(db);
    let orch = orchestrator(&temp, db.clone());
    (temp, db, orch)
}

pub async fn insert_project_with_repo(db: &LocalDb, key: &str, repo_path: &Path) -> String {
    let id = format!("project-{key}");
    let key = key.to_string();
    let repo = repo_path.to_string_lossy().to_string();
    let id_ret = id.clone();
    db.execute(
        "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
         VALUES (?1, 'default', 'Test Project', ?2, ?3, 1, 1)",
        params![id.as_str(), key.as_str(), repo.as_str()],
    )
    .await
    .unwrap();
    id_ret
}

pub async fn project_resource_fixture(
    project_key: &str,
) -> (TempDir, Arc<LocalDb>, Orchestrator, PathBuf) {
    let (temp, db) = migrated_db().await;
    let db = Arc::new(db);
    let repo = temp.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    insert_project_with_repo(&db, project_key, &repo).await;
    let orch = orchestrator(&temp, db.clone());
    (temp, db, orch, repo)
}

pub async fn read_resource(orch: &Orchestrator, uri: impl AsRef<str>) -> String {
    let request = McpCallbackRequest {
        cwd: String::new(),
        run_id: None,
        tool: "read_issue_resource".to_string(),
        payload: json!({ "uri": uri.as_ref() }),
        tool_use_id: None,
    };
    handle_read_issue_resource(orch, &request).await
}

pub async fn change_resource(orch: &Orchestrator, changes: Value) -> String {
    let request = McpCallbackRequest {
        cwd: String::new(),
        run_id: None,
        tool: "write".to_string(),
        payload: json!({ "changes": changes }),
        tool_use_id: None,
    };
    handle_change(orch, &request).await
}

pub async fn test_orchestrator() -> (tempfile::TempDir, Orchestrator) {
    let temp = tempfile::tempdir().unwrap();
    let (_db_temp, db) = migrated_db().await;
    let orch = orchestrator(&temp, Arc::new(db));
    (temp, orch)
}

pub async fn query_i64(db: &LocalDb, sql: &'static str) -> DbResult<i64> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn.query(sql, ()).await?;
            let row = rows
                .next()
                .await?
                .ok_or_else(|| DbError::Row("missing integer row".to_string()))?;
            row.i64(0)
        })
    })
    .await
}

pub async fn scalar_text_by_id(db: &LocalDb, sql: &'static str, id: &str) -> Option<String> {
    let id = id.to_string();
    db.read(|conn| {
        let id = id.clone();
        Box::pin(async move {
            let mut rows = conn.query(sql, params![id.as_str()]).await?;
            rows.next().await?.map(|row| row.opt_text(0)).transpose()
        })
    })
    .await
    .unwrap()
    .flatten()
}

pub async fn scalar_i64_by_id(db: &LocalDb, sql: &'static str, id: &str) -> i64 {
    let id = id.to_string();
    db.read(|conn| {
        let id = id.clone();
        Box::pin(async move {
            let mut rows = conn.query(sql, params![id.as_str()]).await?;
            let row = rows.next().await?.expect("count row");
            row.i64(0)
        })
    })
    .await
    .unwrap()
}

pub async fn execute(db: &LocalDb, sql: &'static str) -> DbResult<()> {
    db.write(|conn| {
        Box::pin(async move {
            conn.execute(sql, ()).await?;
            Ok(())
        })
    })
    .await
}

pub async fn create_project(db: &LocalDb, key: &str) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    let key = key.to_string();
    let now = chrono::Utc::now().timestamp();

    db.execute(
        "
        INSERT INTO projects (
            id, workspace_id, name, key, repo_path, created_at, updated_at
        )
        VALUES (?1, 'default', 'Test Project', ?2, '/tmp/test-repo', ?3, ?4)
        ",
        params![id.as_str(), key.as_str(), now, now],
    )
    .await
    .unwrap();

    id
}

pub async fn create_job(
    db: &LocalDb,
    parent_job_id: Option<&str>,
    task_index: Option<i64>,
) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    let project_key = format!("JT{}", &id[..8]);
    let project_id = create_project(db, &project_key).await;
    let parent_job_id = parent_job_id.map(str::to_string);
    let task_index = task_index.unwrap_or(0);
    let now = chrono::Utc::now().timestamp();

    db.write(|conn| {
        let id = id.clone();
        let project_id = project_id.clone();
        let parent_job_id = parent_job_id.clone();
        Box::pin(async move {
            conn.execute(
                "
                INSERT INTO jobs (
                    id, project_id, status, parent_job_id, task_index, created_at, updated_at
                )
                VALUES (?1, ?2, 'running', ?3, ?4, ?5, ?6)
                ",
                params![
                    id.as_str(),
                    project_id.as_str(),
                    parent_job_id.as_deref(),
                    task_index,
                    now,
                    now
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();

    id
}

/// True when this test process is itself confined by a Cairn worktree fence
/// (`CAIRN_SANDBOXED=1`, set on every fenced `run`). Integration tests that
/// spawn a real `sandbox-exec` or depend on an undisturbed subprocess lifecycle
/// cannot run nested inside the agent fence, so they call this to skip rather
/// than fail. Unfenced CI still runs them for real coverage.
pub fn skip_if_fenced(test: &str) -> bool {
    if std::env::var_os("CAIRN_SANDBOXED").is_some() {
        eprintln!("skipping {test}: cannot run nested inside a Cairn worktree fence");
        true
    } else {
        false
    }
}
