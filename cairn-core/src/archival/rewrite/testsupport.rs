use super::*;
use turso::params;

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

/// Seed the workspace → project → execution → jobs → runs chain. `repo_path`
/// is the canonical repo (ObjectStore ODB); `worktree_path` is the live
/// worktree (git + pack). A child task job + run share the worktree.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn seed_chain(
    db: &LocalDb,
    repo_path: &str,
    worktree_path: &str,
    base_commit: Option<&str>,
    pack_anchor: Option<&str>,
    with_task: bool,
) {
    let repo_path = repo_path.to_string();
    let worktree_path = worktree_path.to_string();
    let base_commit = base_commit.map(str::to_string);
    let pack_anchor = pack_anchor.map(str::to_string);
    db.write(move |conn| {
            let repo_path = repo_path.clone();
            let worktree_path = worktree_path.clone();
            let base_commit = base_commit.clone();
            let pack_anchor = pack_anchor.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES ('ws','w',1,1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO projects(id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
                     VALUES ('proj','ws','p','P',?1,'main',1,1)",
                    (repo_path.as_str(),),
                )
                .await?;
                conn.execute(
                    "INSERT INTO executions(id, recipe_id, status, started_at) VALUES ('exec','r','running',1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO jobs(id, execution_id, project_id, status, worktree_path,
                         base_commit, pack_anchor, created_at, updated_at)
                     VALUES ('job','exec','proj','complete',?1,?2,?3,1,1)",
                    params![
                        worktree_path.as_str(),
                        base_commit.as_deref(),
                        pack_anchor.as_deref()
                    ],
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
                        "INSERT INTO jobs(id, execution_id, project_id, parent_job_id, status,
                             worktree_path, base_commit, pack_anchor, created_at, updated_at)
                         VALUES ('taskjob','exec','proj','job','complete',?1,?2,?3,2,2)",
                        params![
                            worktree_path.as_str(),
                            base_commit.as_deref(),
                            pack_anchor.as_deref()
                        ],
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
    db.read(|conn| {
        Box::pin(async move {
            let sql = format!(
                "SELECT {EVENT_COLUMNS} FROM events
                     WHERE run_id IN (
                         SELECT r.id FROM runs r JOIN jobs j ON r.job_id = j.id
                         WHERE j.execution_id = 'exec'
                     )
                     ORDER BY created_at ASC, sequence ASC"
            );
            let mut rows = conn.query(&sql, ()).await?;
            let mut events = Vec::new();
            while let Some(row) = rows.next().await? {
                events.push(event_from_row(&row)?);
            }
            DbResult::Ok(events)
        })
    })
    .await
    .unwrap()
}

pub(crate) fn tool_result_of(event: &Event) -> String {
    let value: Value = serde_json::from_str(&event.data).unwrap();
    value["toolResult"].as_str().unwrap().to_string()
}

pub(crate) struct Fixture {
    _origin_dir: tempfile::TempDir,
    _clone_dir: tempfile::TempDir,
    pub(crate) origin: PathBuf,
    pub(crate) clone: PathBuf,
    pub(crate) anchor: String,
    pub(crate) w1: String,
}

/// origin holds main at the anchor; the clone makes the work commit (W1) and
/// a later out-of-band "drift" commit, so W1's objects live only in the range
/// pack and the anchor resolves from the repo layer.
pub(crate) fn build_fixture() -> Fixture {
    let origin_dir = tempfile::tempdir().unwrap();
    let origin = origin_dir.path().to_path_buf();
    init_repo(&origin);
    write_file(&origin, "a.txt", b"alpha\nbeta\ngamma\ndelta\n");
    write_file(&origin, "dir/b.txt", b"unchanged-keep\n");
    let anchor = commit_all(&origin, "base");

    let clone_dir = tempfile::tempdir().unwrap();
    let clone = clone_dir.path().to_path_buf();
    git(
        &origin,
        &[
            "clone",
            "-q",
            origin.to_str().unwrap(),
            clone.to_str().unwrap(),
        ],
    );
    git(&clone, &["checkout", "-q", "-b", "work"]);
    write_file(&clone, "a.txt", b"ALPHA\nbeta\ngamma\ndelta\nepsilon\n");
    write_file(&clone, "c.txt", b"new file\n");
    let w1 = commit_all(&clone, "work");
    // Out-of-band drift commit: not reported through any event result.
    write_file(&clone, "a.txt", b"DRIFT\nbeta\ngamma\ndelta\nepsilon\n");
    commit_all(&clone, "drift");

    Fixture {
        _origin_dir: origin_dir,
        _clone_dir: clone_dir,
        origin,
        clone,
        anchor,
        w1,
    }
}

pub(crate) fn short(worktree: &Path, sha: &str) -> String {
    git(worktree, &["rev-parse", "--short", sha])
        .trim()
        .to_string()
}

pub(crate) async fn blob_count(db: &LocalDb) -> i64 {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query("SELECT COUNT(*) FROM archival_blobs", ())
                .await?;
            rows.next().await?.unwrap().i64(0)
        })
    })
    .await
    .unwrap()
}
