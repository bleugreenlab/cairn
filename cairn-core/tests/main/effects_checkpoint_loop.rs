//! Integration tests for the command-checkpoint <-> agent auto-fix loop.
//!
//! Drives a real execution DAG (agent -> checkpoint(command) -> agent) through
//! `advance_execution_with_actions` against a real temp jj agent workspace, so
//! the checkpoint command actually runs and the worktree HEAD SHA gate is
//! exercised end to end: fail -> Blocked + recorded run, fix+seal -> re-arm ->
//! re-run -> pass -> downstream advances; plus the no-progress and hard-cap
//! termination paths and the manual Re-run entry.
//!
//! The worktree is a NON-colocated `.jj` workspace (the production shape), not a
//! git worktree: CAIRN-1970 ported the checkpoint cache's head/dirty reads off
//! git porcelain to `jj log -r @-` / `jj diff`, so a git-only worktree never
//! advances the head the re-arm gate reads. A real agent's "fix and commit" is
//! modeled by writing the file and sealing (`jj::seal`), which advances `@-`.

use crate::common;

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use cairn_core::internal::db::DbState;
use cairn_core::internal::execution::advancement::{
    advance_execution_with_actions, rerun_checkpoint_job,
};
use cairn_core::internal::jj::{self, JjEnv};
use cairn_core::internal::orchestrator::Orchestrator;
use cairn_core::internal::services::testing::TestServicesBuilder;
use cairn_core::internal::storage::{LocalDb, RowExt, SearchIndex};
use cairn_db::turso::params;
use serde_json::{json, Value};
use tempfile::{tempdir, TempDir};

const EXEC_ID: &str = "exec-1";
const BUILDER_JOB: &str = "builder";
const CHECKPOINT_JOB: &str = "ci";
const DONE_JOB: &str = "done";

struct Ctx {
    orch: Orchestrator,
    db: Arc<LocalDb>,
    project_id: String,
    config_dir: PathBuf,
    _db_temp: TempDir,
    _temp: TempDir,
}

async fn ctx() -> Ctx {
    let temp = tempdir().unwrap();
    let config_dir = temp.path().join("config");
    let (db_temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let project_id = common::create_project(&db, "CKL").await;
    let search_index = Arc::new(SearchIndex::open_or_create(temp.path().join("search")).unwrap());
    let db_state = Arc::new(DbState::new(db.clone(), search_index));
    let services = Arc::new(TestServicesBuilder::new().build());
    let orch = Orchestrator::builder(db_state, services, config_dir.clone()).build();
    Ctx {
        orch,
        db,
        project_id,
        config_dir,
        _db_temp: db_temp,
        _temp: temp,
    }
}

struct CheckpointLoop {
    c: Ctx,
    worktree: Worktree,
    path: String,
    sha: String,
}

impl CheckpointLoop {
    /// Write a file and seal it, advancing the workspace `@-` exactly as a real
    /// agent's fix-and-commit does. Returns the new head commit id.
    fn commit_file(&self, name: &str) -> String {
        self.worktree.commit_file(name)
    }
}

async fn checkpoint_loop(command: &str) -> Option<CheckpointLoop> {
    checkpoint_loop_with_status(command, "pending", true).await
}

async fn blocked_checkpoint_loop(command: &str) -> Option<CheckpointLoop> {
    checkpoint_loop_with_status(command, "blocked", true).await
}

async fn blocked_checkpoint_without_downstream(command: &str) -> Option<CheckpointLoop> {
    checkpoint_loop_with_status(command, "blocked", false).await
}

async fn checkpoint_loop_with_status(
    command: &str,
    checkpoint_status: &str,
    include_downstream: bool,
) -> Option<CheckpointLoop> {
    let c = ctx().await;
    let worktree = Worktree::try_new(&c.config_dir)?;
    let path = worktree.path();
    let sha = worktree.head();
    insert_execution(&c.db, EXEC_ID, &snapshot(command)).await;
    insert_job(
        &c.db,
        BUILDER_JOB,
        &c.project_id,
        EXEC_ID,
        BUILDER_JOB,
        "complete",
        None,
        Some(&path),
        None,
        "Builder",
    )
    .await;
    insert_complete_turn(&c.db, BUILDER_JOB).await;
    insert_job(
        &c.db,
        CHECKPOINT_JOB,
        &c.project_id,
        EXEC_ID,
        CHECKPOINT_JOB,
        checkpoint_status,
        Some(BUILDER_JOB),
        None,
        None,
        "CI",
    )
    .await;

    if include_downstream {
        insert_job(
            &c.db,
            DONE_JOB,
            &c.project_id,
            EXEC_ID,
            DONE_JOB,
            "pending",
            None,
            None,
            None,
            "Done",
        )
        .await;
    }

    Some(CheckpointLoop {
        c,
        worktree,
        path,
        sha,
    })
}

async fn advance_execution(h: &CheckpointLoop) {
    advance_execution_with_actions(&h.c.orch, EXEC_ID)
        .await
        .unwrap();
}

async fn assert_job_status(h: &CheckpointLoop, job_id: &str, status: &str) {
    assert_eq!(job_status(&h.c.db, job_id).await, status);
}

async fn assert_checkpoint(h: &CheckpointLoop, status: &str, run_count_expected: i64) {
    assert_job_status(h, CHECKPOINT_JOB, status).await;
    assert_eq!(run_count(&h.c.db, CHECKPOINT_JOB).await, run_count_expected);
}

async fn assert_latest_checkpoint_passed(h: &CheckpointLoop, passed: bool) {
    assert_eq!(latest_passed(&h.c.db, CHECKPOINT_JOB).await, Some(passed));
}

// ── jj worktree harness ──────────────────────────────────────────────────────
//
// A real agent worktree is a NON-colocated `.jj` workspace over a shared store
// (no `.git` in the workspace dir). The re-arm SHA gate reads the head via
// `jj log -r @-`, so the harness must advance that head by sealing — a git-only
// worktree (the pre-CAIRN-1970 harness) never moves it and the checkpoint stays
// blocked. A colocated `jj git init --colocate` repo would carry a `.git` and
// mask exactly this git-vs-jj mismatch, so `try_new` asserts there is none.

/// Initialize a throwaway project git repo with one commit, the store's base.
fn init_git_repo(repo: &Path) {
    let git = |args: &[&str]| {
        let out = Command::new("git")
            .args(args)
            .current_dir(repo)
            .output()
            .unwrap_or_else(|e| panic!("git {args:?} failed to spawn: {e}"));
        assert!(
            out.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    };
    git(&["init", "-q", "-b", "main"]);
    git(&["config", "user.email", "t@t.test"]);
    git(&["config", "user.name", "Test"]);
    std::fs::write(repo.join("seed.txt"), "seed").unwrap();
    git(&["add", "."]);
    git(&["commit", "-q", "-m", "init"]);
}

/// A non-colocated jj agent workspace over a shared store, mirroring production
/// worktree provisioning. The head advances only via [`Worktree::commit_file`]
/// (a real agent seal), the same `jj log -r @-` the production gate reads.
struct Worktree {
    _project: TempDir,
    dir: TempDir,
    config_dir: PathBuf,
}

impl Worktree {
    /// Provision the workspace, or `None` to self-skip when jj is unavailable.
    /// `config_dir` MUST be the orchestrator's config dir so the checkpoint
    /// gate's `JjEnv` resolves the same store.
    fn try_new(config_dir: &Path) -> Option<Self> {
        common::jj_bin()?;
        let project = tempdir().unwrap();
        init_git_repo(project.path());
        let dir = tempdir().unwrap();
        common::provision_jj_workspace(
            config_dir,
            project.path(),
            dir.path(),
            "agent/CKL-1-builder-0",
        );
        // Non-colocated: a `.git` here would mask the git-vs-jj head mismatch
        // this harness exists to exercise.
        assert!(
            !dir.path().join(".git").exists(),
            "jj workspace must be non-colocated (no .git)"
        );
        Some(Self {
            _project: project,
            dir,
            config_dir: config_dir.to_path_buf(),
        })
    }

    fn jj(&self) -> JjEnv {
        JjEnv::resolve("jj", &self.config_dir)
    }

    fn path(&self) -> String {
        self.dir.path().to_string_lossy().to_string()
    }

    /// The workspace head (`jj log -r @-`), the representation the production
    /// checkpoint gate reads.
    fn head(&self) -> String {
        jj::head_commit(&self.jj(), self.dir.path()).unwrap()
    }

    /// Write a file and seal it, advancing `@-` to a new commit exactly as a
    /// real agent seal does. Returns the new head commit id.
    fn commit_file(&self, name: &str) -> String {
        std::fs::write(self.dir.path().join(name), "x").unwrap();
        jj::seal(&self.jj(), self.dir.path(), "fix", None).unwrap();
        self.head()
    }
}

/// agent(builder) --control--> checkpoint(ci, command) --control--> agent(done)
fn snapshot(command: &str) -> Value {
    json!({
        "recipe": {
            "id": "recipe-1",
            "name": "checkpoint-loop",
            "trigger": "manual",
            "nodes": [
                {
                    "id": "builder",
                    "name": "Builder",
                    "nodeType": "agent",
                    "position": { "x": 0.0, "y": 0.0 },
                    "agentConfig": { "agentConfigId": "test-agent" }
                },
                {
                    "id": "ci",
                    "name": "CI",
                    "nodeType": "checkpoint",
                    "position": { "x": 0.0, "y": 1.0 },
                    "checkpointConfig": { "command": command }
                },
                {
                    "id": "done",
                    "name": "Done",
                    "nodeType": "agent",
                    "position": { "x": 0.0, "y": 2.0 },
                    "agentConfig": { "agentConfigId": "test-agent" }
                }
            ],
            "edges": [
                {
                    "id": "e1", "edgeType": "control",
                    "sourceNodeId": "builder", "sourceHandle": "control-out",
                    "targetNodeId": "ci", "targetHandle": "control-in"
                },
                {
                    "id": "e2", "edgeType": "control",
                    "sourceNodeId": "ci", "sourceHandle": "control-out",
                    "targetNodeId": "done", "targetHandle": "control-in"
                }
            ]
        },
        "agents": {},
        "skills": {},
        "tools": {},
        "triggerContext": { "projectId": "test-project", "triggerType": "manual" },
        "createdAt": 0
    })
}

async fn insert_execution(db: &LocalDb, exec_id: &str, snapshot: &Value) {
    let exec_id = exec_id.to_string();
    let snapshot = serde_json::to_string(snapshot).unwrap();
    let now = chrono::Utc::now().timestamp();
    db.execute(
        "INSERT INTO executions (id, recipe_id, status, started_at, seq, snapshot)
         VALUES (?1, 'recipe-1', 'running', ?2, 1, ?3)",
        params![exec_id.as_str(), now, snapshot.as_str()],
    )
    .await
    .unwrap();
}

#[allow(clippy::too_many_arguments)]
async fn insert_job(
    db: &LocalDb,
    id: &str,
    project_id: &str,
    exec_id: &str,
    node_id: &str,
    status: &str,
    parent_job_id: Option<&str>,
    worktree_path: Option<&str>,
    session_id: Option<&str>,
    node_name: &str,
) {
    let (id, project_id, exec_id, node_id, status, node_name) = (
        id.to_string(),
        project_id.to_string(),
        exec_id.to_string(),
        node_id.to_string(),
        status.to_string(),
        node_name.to_string(),
    );
    let parent_job_id = parent_job_id.map(str::to_string);
    let worktree_path = worktree_path.map(str::to_string);
    let session_id = session_id.map(str::to_string);
    let now = chrono::Utc::now().timestamp();
    db.write(move |conn| {
        let (id, project_id, exec_id, node_id, status, node_name) = (
            id.clone(),
            project_id.clone(),
            exec_id.clone(),
            node_id.clone(),
            status.clone(),
            node_name.clone(),
        );
        let parent_job_id = parent_job_id.clone();
        let worktree_path = worktree_path.clone();
        let session_id = session_id.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO jobs (
                    id, execution_id, recipe_node_id, parent_job_id, worktree_path,
                    current_session_id, status, project_id, node_name, created_at, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?10)",
                params![
                    id.as_str(),
                    exec_id.as_str(),
                    node_id.as_str(),
                    parent_job_id.as_deref(),
                    worktree_path.as_deref(),
                    session_id.as_deref(),
                    status.as_str(),
                    project_id.as_str(),
                    node_name.as_str(),
                    now,
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

/// Give a job a completed turn so the projection derives Complete (a cached
/// `complete` status with no supporting turn fact is re-derived to Pending by the
/// recompute sweep). Models a finished upstream agent.
async fn insert_complete_turn(db: &LocalDb, job_id: &str) {
    let job_id = job_id.to_string();
    let now = chrono::Utc::now().timestamp();
    db.write(move |conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason,
                                    created_at, started_at, updated_at)
                 VALUES (?1, ?2, ?3, 1, 'complete', 'initial', ?4, ?4, ?4)",
                params![
                    uuid::Uuid::new_v4().to_string().as_str(),
                    format!("session-{job_id}").as_str(),
                    job_id.as_str(),
                    now
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

async fn job_status(db: &LocalDb, id: &str) -> String {
    let id = id.to_string();
    db.read(move |conn| {
        let id = id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT status FROM jobs WHERE id = ?1",
                    params![id.as_str()],
                )
                .await?;
            rows.next().await?.unwrap().text(0)
        })
    })
    .await
    .unwrap()
}

async fn run_count(db: &LocalDb, job_id: &str) -> i64 {
    let job_id = job_id.to_string();
    db.read(move |conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT COUNT(*) FROM checkpoint_runs WHERE job_id = ?1",
                    params![job_id.as_str()],
                )
                .await?;
            rows.next().await?.unwrap().i64(0)
        })
    })
    .await
    .unwrap()
}

async fn latest_passed(db: &LocalDb, job_id: &str) -> Option<bool> {
    let job_id = job_id.to_string();
    db.read(move |conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT passed FROM checkpoint_runs WHERE job_id = ?1 ORDER BY ran_at DESC, attempt DESC LIMIT 1",
                    params![job_id.as_str()],
                )
                .await?;
            Ok(rows.next().await?.map(|r| r.i64(0)).transpose()?.map(|v| v != 0))
        })
    })
    .await
    .unwrap()
}

async fn insert_failed_run(db: &LocalDb, job_id: &str, attempt: i64, sha: &str) {
    let (job_id, sha) = (job_id.to_string(), sha.to_string());
    let now = chrono::Utc::now().timestamp();
    db.write(move |conn| {
        let (job_id, sha) = (job_id.clone(), sha.clone());
        Box::pin(async move {
            conn.execute(
                "INSERT INTO checkpoint_runs (id, job_id, attempt, command, commit_sha, exit_code, passed, ran_at)
                 VALUES (?1, ?2, ?3, 'exit 1', ?4, 1, 0, ?5)",
                params![uuid::Uuid::new_v4().to_string().as_str(), job_id.as_str(), attempt, sha.as_str(), now],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

// ── Tests ──────────────────────────────────────────────────────────────────

/// Pass-first: the command passes on the first run -> checkpoint Complete, and a
/// subsequent advance claims the downstream node. (PR-review #3 gap: no prior
/// checkpoint integration coverage.)
#[tokio::test]
async fn pass_first_completes_and_downstream_advances() {
    let Some(h) = checkpoint_loop("exit 0").await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };

    advance_execution(&h).await;
    assert_checkpoint(&h, "complete", 1).await;
    assert_latest_checkpoint_passed(&h, true).await;

    // Next advance claims the downstream agent node.
    advance_execution(&h).await;
    assert_job_status(&h, DONE_JOB, "running").await;
}

/// A failing command blocks the checkpoint and records the run. With a
/// sessionless upstream there is nothing to wake, so it stays Blocked.
#[tokio::test]
async fn fail_blocks_and_records_run() {
    let Some(h) = checkpoint_loop("test -f fixed.txt").await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };

    advance_execution(&h).await;
    assert_checkpoint(&h, "blocked", 1).await;
    assert_latest_checkpoint_passed(&h, false).await;
    assert_job_status(&h, DONE_JOB, "pending").await;
}

/// Full cycle: fail -> Blocked, fix+commit (new HEAD) -> re-arm re-runs against
/// the new worktree -> pass -> Complete -> downstream advances.
#[tokio::test]
async fn fix_commit_rearms_reruns_and_passes() {
    let Some(h) = checkpoint_loop("test -f fixed.txt").await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };

    // 1. First run fails -> Blocked.
    advance_execution(&h).await;
    assert_checkpoint(&h, "blocked", 1).await;

    // 2. Agent "fixes" and seals -> new HEAD.
    h.commit_file("fixed.txt");

    // 3. Advance: re-arm re-pends the checkpoint, it re-runs and passes.
    advance_execution(&h).await;
    assert_checkpoint(&h, "complete", 2).await;
    assert_latest_checkpoint_passed(&h, true).await;

    // 4. Downstream advances.
    advance_execution(&h).await;
    assert_job_status(&h, DONE_JOB, "running").await;
}

/// No-progress termination: the agent goes idle without committing (HEAD
/// unchanged), so the re-arm pass is a no-op and the checkpoint stays Blocked.
#[tokio::test]
async fn no_new_commit_does_not_rerun() {
    let Some(h) = checkpoint_loop("test -f fixed.txt").await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };

    advance_execution(&h).await;
    assert_checkpoint(&h, "blocked", 1).await;

    // No commit -> HEAD unchanged -> re-arm is a no-op.
    advance_execution(&h).await;
    assert_checkpoint(&h, "blocked", 1).await;
}

/// Hard cap: once the attempt cap is reached, a new commit no longer re-arms the
/// checkpoint — it stays Blocked for manual resolution.
#[tokio::test]
async fn attempt_cap_stops_rearm() {
    let Some(h) = blocked_checkpoint_without_downstream("test -f fixed.txt").await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };

    // Pre-seed CHECKPOINT_MAX_ATTEMPTS (5) failed runs at the original SHA.
    for attempt in 1..=5 {
        insert_failed_run(&h.c.db, CHECKPOINT_JOB, attempt, &h.sha).await;
    }

    // A new commit would normally re-arm, but the cap is exhausted.
    h.commit_file("fixed.txt");
    advance_execution(&h).await;
    assert_job_status(&h, CHECKPOINT_JOB, "blocked").await;
    assert_eq!(
        run_count(&h.c.db, CHECKPOINT_JOB).await,
        5,
        "no new run past the cap"
    );
}

/// Manual Re-run: bypasses the SHA gate, resets the attempt cycle, re-runs and
/// (with the fix present) passes -> Complete.
#[tokio::test]
async fn manual_rerun_resets_and_passes() {
    let Some(h) = blocked_checkpoint_loop("test -f fixed.txt").await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };

    // A prior failed run exists at the original SHA.
    insert_failed_run(&h.c.db, CHECKPOINT_JOB, 1, &h.sha).await;

    // The worktree is already fixed (no new commit needed — manual bypasses SHA).
    std::fs::write(format!("{}/fixed.txt", h.path), "x").unwrap();

    rerun_checkpoint_job(&h.c.orch, CHECKPOINT_JOB)
        .await
        .unwrap();
    assert_job_status(&h, CHECKPOINT_JOB, "complete").await;
    // History was reset, so only the fresh passing run remains.
    assert_eq!(run_count(&h.c.db, CHECKPOINT_JOB).await, 1);
    assert_latest_checkpoint_passed(&h, true).await;
}

/// Re-run rejects a job that is not a Blocked command checkpoint.
#[tokio::test]
async fn rerun_rejects_non_checkpoint() {
    let c = ctx().await;
    let Some(wt) = Worktree::try_new(&c.config_dir) else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    let path = wt.path();
    insert_execution(&c.db, EXEC_ID, &snapshot("exit 0")).await;
    // An agent job with a session is not a command checkpoint.
    insert_job(
        &c.db,
        BUILDER_JOB,
        &c.project_id,
        EXEC_ID,
        BUILDER_JOB,
        "blocked",
        None,
        Some(&path),
        Some("sess-1"),
        "Builder",
    )
    .await;

    let err = rerun_checkpoint_job(&c.orch, BUILDER_JOB)
        .await
        .unwrap_err();
    assert!(
        err.contains("command checkpoint"),
        "unexpected error: {err}"
    );
}
