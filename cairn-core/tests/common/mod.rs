//! Shared integration test helpers.

#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use cairn_core::internal::db::DbState;
use cairn_core::internal::jj::{self, JjEnv};
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
/// The jj binary for the test, or `None` to self-skip when jj is unavailable.
/// jj-backed integration tests gate on this and print a skip note rather than
/// fail when jj cannot be resolved (honoring `CAIRN_JJ_BIN` when set).
pub fn jj_bin() -> Option<String> {
    let bin = std::env::var("CAIRN_JJ_BIN")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "jj".to_string());
    Command::new(&bin)
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
        .then_some(bin)
}

/// The git `HEAD` commit sha of `repo`, used to base a jj workspace on the
/// project repo's current tip.
pub fn head_sha(repo: &Path) -> String {
    let out = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(repo)
        .output()
        .unwrap();
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// Provision a NON-COLOCATED `.jj` workspace at `ws` over a shared store backed
/// by `project_repo`, mirroring production worktree provisioning. The workspace
/// is created with `jj workspace add` and carries no `.git`, so tests exercise
/// the real jj/git-shelling seams rather than a colocated checkout that would
/// mask them. `config_dir` is the orchestrator's config dir, so a handler's
/// `JjEnv` resolves the same store and config.
pub fn provision_jj_workspace(config_dir: &Path, project_repo: &Path, ws: &Path, branch: &str) {
    let jj = JjEnv::resolve("jj", config_dir);
    let store = jj::project_store_dir(config_dir, project_repo);
    jj::ensure_project_store(&jj, &store, project_repo).unwrap();
    let base = head_sha(project_repo);
    jj::add_workspace(&jj, &store, ws, branch, &base, None).unwrap();
}

/// Drive `primary_ws` into the OP-LOG stale state that reproduces the commit
/// barrier's data-loss bug. A sibling workspace over the same store seals a
/// commit, then the primary branch is rebased onto it from the store
/// (`--ignore-working-copy`, WITHOUT an `update_stale`) — rewriting the primary
/// workspace's own `@` out from under it. This is the production shape where BOTH
/// the seal and the restore are blocked by staleness, distinct from a mere
/// bookmark advance (which leaves `jj restore` working). The primary workspace
/// must already exist (via [`provision_jj_workspace`]); the sibling is created
/// next to it, coupled only through the shared store.
pub fn stale_sibling_advance(
    config_dir: &Path,
    project_repo: &Path,
    primary_ws: &Path,
    primary_branch: &str,
) {
    let jj = JjEnv::resolve("jj", config_dir);
    let store = jj::project_store_dir(config_dir, project_repo);
    let sibling_ws = primary_ws.parent().unwrap().join("sibling-advance-ws");
    let sibling_branch = "agent/SIB-advance-0";
    let base = head_sha(project_repo);
    jj::add_workspace(&jj, &store, &sibling_ws, sibling_branch, &base, None).unwrap();
    std::fs::write(sibling_ws.join("sibling-advance.txt"), "sibling advance\n").unwrap();
    jj::seal(&jj, &sibling_ws, "sibling advance", None).unwrap();
    // Rebase the whole primary branch (its working-copy commit included) onto the
    // sibling tip from the store. Rewriting `@` from outside is what makes the
    // primary workspace stale; we deliberately skip the `update_stale` that
    // `advance_workspace_onto` would normally run, leaving it stale for the test.
    jj::rebase_branch_onto(&jj, &store, primary_branch, sibling_branch).unwrap();
}

pub fn skip_if_fenced(test: &str) -> bool {
    if std::env::var_os("CAIRN_SANDBOXED").is_some() {
        eprintln!("skipping {test}: cannot run nested inside a Cairn worktree fence");
        true
    } else {
        false
    }
}
