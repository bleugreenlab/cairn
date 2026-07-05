//! Shared DB/git-fixture scaffolding for the archival-reconstruction tests.
//!
//! Promoted out of `reconstruct`'s private `#[cfg(test)] mod tests` so both the
//! renderer-agnostic tests that stay there and the byte-exact archived-read tests
//! relocated to the mcp read layer (`mcp::handlers::read::archived_reconstruct_tests`)
//! seed the same runs -> jobs -> projects chain, build the same range-pack git
//! fixture, and make the same events — from one copy that cannot drift.

use std::path::PathBuf;

use turso::params;

use crate::models::Event;
use crate::storage::event_fixture::read_stub;
use crate::storage::events::testutil::{commit_all, git, init_repo, write_file};
use crate::storage::{build_execution_pack, LocalDb, MigrationRunner, TURSO_MIGRATIONS};

pub async fn migrated_db() -> LocalDb {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.keep().join("cairn-archival-reconstruct.db");
    let db = LocalDb::open(path).await.unwrap();
    MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
        .run(&db)
        .await
        .unwrap();
    db
}

/// Seed the runs -> jobs -> projects chain plus an `execution_history` row so
/// `reconstruct_events` can resolve coordinates for run `run` / execution
/// `exec`. `pack` is the range pack bytes, or `None` for an empty range.
pub async fn seed_chain(db: &LocalDb, repo_path: &str, pack: Option<(Vec<u8>, Vec<u8>)>) {
    let repo_path = repo_path.to_string();
    db.write(move |conn| {
        let repo_path = repo_path.clone();
        let pack = pack.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES ('ws','w',1,1)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
                     VALUES ('proj','ws','p','P',?1,1,1)",
                (repo_path.as_str(),),
            )
            .await?;
            conn.execute(
                "INSERT INTO executions(id, recipe_id, status, started_at) VALUES ('exec','r','running',1)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO jobs(id, execution_id, project_id, status, created_at, updated_at)
                     VALUES ('job','exec','proj','complete',1,1)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO runs(id, job_id, status, created_at, updated_at)
                     VALUES ('run','job','exited',1,1)",
                (),
            )
            .await?;
            match pack {
                Some((pack, idx)) => {
                    conn.execute(
                        "INSERT INTO execution_history(execution_id, base_sha, tip_sha, pack, pack_idx)
                             VALUES ('exec','base','tip',?1,?2)",
                        params![pack, idx],
                    )
                    .await?;
                }
                None => {
                    conn.execute(
                        "INSERT INTO execution_history(execution_id, base_sha, tip_sha, pack, pack_idx)
                             VALUES ('exec','base','tip',NULL,NULL)",
                        (),
                    )
                    .await?;
                }
            }
            Ok(())
        })
    })
    .await
    .unwrap();
}

pub async fn seed_team_chain(
    db: &LocalDb,
    synced_repo_path: &str,
    pack: Option<(Vec<u8>, Vec<u8>)>,
) {
    let synced_repo_path = synced_repo_path.to_string();
    db.write(move |conn| {
        let synced_repo_path = synced_repo_path.clone();
        let pack = pack.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES ('ws','w',1,1)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
                     VALUES ('teamABC123~00000000-0000-4000-8000-000000000001','ws','p','P',?1,1,1)",
                (synced_repo_path.as_str(),),
            )
            .await?;
            conn.execute(
                "INSERT INTO executions(id, recipe_id, status, started_at) VALUES ('exec','r','running',1)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO jobs(id, execution_id, project_id, status, created_at, updated_at)
                     VALUES ('job','exec','teamABC123~00000000-0000-4000-8000-000000000001','complete',1,1)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO runs(id, job_id, status, created_at, updated_at)
                     VALUES ('run','job','exited',1,1)",
                (),
            )
            .await?;
            if let Some((pack, idx)) = pack {
                conn.execute(
                    "INSERT INTO execution_history(execution_id, base_sha, tip_sha, pack, pack_idx)
                         VALUES ('exec','base','tip',?1,?2)",
                    params![pack, idx],
                )
                .await?;
            }
            Ok(())
        })
    })
    .await
    .unwrap();
}

#[allow(clippy::too_many_arguments)]
pub fn make_event(
    id: &str,
    event_type: &str,
    data: &str,
    storage_mode: Option<&str>,
    content_commit: Option<&str>,
    data_blob: Option<Vec<u8>>,
    codec: Option<&str>,
) -> Event {
    Event {
        id: id.to_string(),
        run_id: "run".to_string(),
        session_id: None,
        sequence: 1,
        timestamp: 1,
        event_type: event_type.to_string(),
        data: data.to_string(),
        parent_tool_use_id: None,
        created_at: 1,
        input_tokens: None,
        cache_read_tokens: None,
        cache_create_tokens: None,
        output_tokens: None,
        turn_id: None,
        thinking_tokens: None,
        cost_usd: None,
        storage_mode: storage_mode.map(str::to_string),
        content_commit: content_commit.map(str::to_string),
        content_change_id: None,
        content_render_sha: None,
        data_blob,
        codec: codec.map(str::to_string),
        read_segments: None,
    }
}

pub struct Fixture {
    _origin_dir: tempfile::TempDir,
    _clone_dir: tempfile::TempDir,
    pub origin: PathBuf,
    pub clone: PathBuf,
    pub anchor: String,
    pub tip: String,
    pub pack: Vec<u8>,
    pub idx: Vec<u8>,
}

/// origin holds `main` at the anchor; a clone makes a branch commit (tip)
/// whose new objects live only in the range pack — so a tip read mixes
/// pack-layer (commit/tree/modified blob) and repo-layer (unchanged blob)
/// resolution.
pub fn build_fixture() -> Fixture {
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
    let tip = commit_all(&clone, "work");

    let (pack, idx) = build_execution_pack(&clone, &tip, &anchor, "main")
        .unwrap()
        .unwrap();

    Fixture {
        _origin_dir: origin_dir,
        _clone_dir: clone_dir,
        origin,
        clone,
        anchor,
        tip,
        pack,
        idx,
    }
}

pub fn read_event(content_commit: &str, paths: &[&str]) -> Event {
    let data = read_stub("t1", paths);
    make_event(
        "read-ev",
        "tool_result",
        &data,
        Some("gitcoord"),
        Some(content_commit),
        None,
        None,
    )
}

pub fn tool_result(event: &Event) -> String {
    let value: serde_json::Value = serde_json::from_str(&event.data).unwrap();
    value["toolResult"].as_str().unwrap().to_string()
}
