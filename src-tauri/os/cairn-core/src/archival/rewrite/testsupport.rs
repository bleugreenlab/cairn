use super::*;
use cairn_db::turso::params;

pub(crate) use crate::storage::event_fixture::{
    assistant_read, assistant_run, assistant_text, assistant_thinking, assistant_tool,
    assistant_write, assistant_write_dup_tool_input, render_targets as rendered, system_event,
    system_init_claude, system_init_codex, system_prompt, tool_result as read_result, user_text,
    write_result,
};
pub(crate) use crate::storage::events::reconstruct::{reconstruct_events, STUB_PREFIX};
pub(crate) use crate::storage::events::testutil::{commit_all, git, init_repo, write_file};
pub(crate) use crate::storage::migrated_test_db;

#[allow(clippy::too_many_arguments)]
pub(crate) async fn insert_event(
    db: &LocalDb,
    id: &str,
    run_id: &str,
    seq: i64,
    created_at: i64,
    event_type: &str,
    data: &str,
) {
    let id = id.to_string();
    let run_id = run_id.to_string();
    let event_type = event_type.to_string();
    let data = data.to_string();
    db.write(move |conn| {
        let (id, run_id, event_type, data) =
            (id.clone(), run_id.clone(), event_type.clone(), data.clone());
        Box::pin(async move {
            conn.execute(
                "INSERT INTO events (id, run_id, sequence, timestamp, event_type, data, created_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    id.as_str(),
                    run_id.as_str(),
                    seq,
                    created_at,
                    event_type.as_str(),
                    data.as_str(),
                    created_at
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

/// Seed the workspace → project → execution `exec` → job `job` → run `run`
/// chain over `repo_path`. With `with_task`, a child task job and run share the
/// execution.
pub(crate) async fn seed_chain(db: &LocalDb, repo_path: &str, with_task: bool) {
    // Every archival roundtrip test seeds this chain, so it is the shared setup
    // point that registers the archived-file renderer reconstruction sources
    // through the registry.
    crate::mcp::handlers::read::register_archived_file_renderer();
    let repo_path = repo_path.to_string();
    db.write(move |conn| {
        let repo_path = repo_path.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES ('ws','w',1,1)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO projects(id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
                 VALUES ('proj','ws','p','p',?1,'main',1,1)",
                (repo_path.as_str(),),
            )
            .await?;
            conn.execute(
                "INSERT INTO executions(id, recipe_id, status, started_at) VALUES ('exec','r','running',1)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO jobs(id, execution_id, project_id, status, branch, created_at, updated_at)
                 VALUES ('job','exec','proj','complete','work',1,1)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO runs(id, job_id, project_id, status, created_at, updated_at)
                 VALUES ('run','job','proj','exited',1,1)",
                (),
            )
            .await?;
            if with_task {
                conn.execute(
                    "INSERT INTO jobs(id, execution_id, project_id, parent_job_id, status, branch,
                         created_at, updated_at)
                     VALUES ('taskjob','exec','proj','job','complete','work',2,2)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO runs(id, job_id, project_id, status, created_at, updated_at)
                     VALUES ('taskrun','taskjob','proj','exited',2,2)",
                    (),
                )
                .await?;
            }
            Ok(())
        })
    })
    .await
    .unwrap();
}

pub(crate) async fn load_events(db: &LocalDb) -> Vec<Event> {
    db.query_all(
        format!(
            "SELECT {EVENT_COLUMNS} FROM events
             WHERE run_id IN (
                 SELECT r.id FROM runs r JOIN jobs j ON r.job_id = j.id
                 WHERE j.execution_id = 'exec'
             )
             ORDER BY created_at ASC, sequence ASC"
        ),
        (),
        event_from_row,
    )
    .await
    .unwrap()
}

pub(crate) fn tool_result_of(event: &Event) -> String {
    let value: Value = serde_json::from_str(&event.data).unwrap();
    value["toolResult"].as_str().unwrap().to_string()
}

/// Archive execution `exec`, with or without a range.
pub(crate) async fn archive(
    db: &LocalDb,
    range: Option<ArchiveRange>,
) -> Result<ArchiveSummary, String> {
    archive_ranges(db, range.into_iter().collect()).await
}

/// Archive execution `exec` across several branch ranges.
pub(crate) async fn archive_ranges(
    db: &LocalDb,
    ranges: Vec<ArchiveRange>,
) -> Result<ArchiveSummary, String> {
    archive_execution(
        db,
        ArchiveTarget {
            execution_id: "exec".to_string(),
            ranges,
        },
    )
    .await
}

/// The range a branch occupies against `main`, resolved with plain git exactly
/// as `live_branch_range` resolves it from the jj store.
pub(crate) fn branch_range(repo: &Path, branch: &str) -> ArchiveRange {
    let tip = git(repo, &["rev-parse", branch]).trim().to_string();
    let target_head = git(repo, &["rev-parse", "main"]).trim().to_string();
    let base = git(repo, &["merge-base", branch, "main"])
        .trim()
        .to_string();
    ArchiveRange {
        branch: branch.to_string(),
        repo_path: repo.to_path_buf(),
        base,
        tip,
        durable_head: target_head.clone(),
        target_head,
    }
}

pub(crate) struct Fixture {
    _dir: tempfile::TempDir,
    pub(crate) repo: PathBuf,
    /// `main`, and the fork point of `work`.
    pub(crate) anchor: String,
    /// The work commit a write reports.
    pub(crate) w1: String,
    /// An out-of-band commit on `work` after W1, reported by no event.
    pub(crate) drift: String,
}

impl Fixture {
    pub(crate) fn range(&self) -> ArchiveRange {
        branch_range(&self.repo, "work")
    }
}

/// One repository: `main` holds the anchor; `work` forks from it with the work
/// commit W1 and then an out-of-band drift commit.
pub(crate) fn build_fixture() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let repo = dir.path().to_path_buf();
    init_repo(&repo);
    write_file(&repo, "a.txt", b"alpha\nbeta\ngamma\ndelta\n");
    write_file(&repo, "dir/b.txt", b"unchanged-keep\n");
    let anchor = commit_all(&repo, "base");
    git(&repo, &["checkout", "-q", "-b", "work"]);
    write_file(&repo, "a.txt", b"ALPHA\nbeta\ngamma\ndelta\nepsilon\n");
    write_file(&repo, "c.txt", b"new file\n");
    let w1 = commit_all(&repo, "work");
    write_file(&repo, "a.txt", b"DRIFT\nbeta\ngamma\ndelta\nepsilon\n");
    let drift = commit_all(&repo, "drift");
    Fixture {
        _dir: dir,
        repo,
        anchor,
        w1,
        drift,
    }
}

pub(crate) fn short(repo: &Path, sha: &str) -> String {
    git(repo, &["rev-parse", "--short", sha]).trim().to_string()
}

pub(crate) async fn blob_count(db: &LocalDb) -> i64 {
    db.query_one("SELECT COUNT(*) FROM archival_blobs", (), |row| row.i64(0))
        .await
        .unwrap()
}

pub(crate) async fn storage_mode(db: &LocalDb, id: &str) -> Option<String> {
    db.query_one(
        "SELECT storage_mode FROM events WHERE id = ?1",
        (id.to_string(),),
        |row| row.opt_text(0),
    )
    .await
    .unwrap()
}

pub(crate) async fn content_commit(db: &LocalDb, id: &str) -> Option<String> {
    db.query_one(
        "SELECT content_commit FROM events WHERE id = ?1",
        (id.to_string(),),
        |row| row.opt_text(0),
    )
    .await
    .unwrap()
}

pub(crate) async fn has_history(db: &LocalDb) -> bool {
    db.query_one(
        "SELECT COUNT(*) FROM execution_history WHERE execution_id = 'exec'",
        (),
        |row| row.i64(0),
    )
    .await
    .unwrap()
        > 0
}
