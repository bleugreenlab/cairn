//! Which scratch a job's work uses, and the rows that are its authority.
//!
//! A scratch belongs to a NODE: the owner URI is the job's home URI with any
//! `/task/...` suffix stripped, found by walking `jobs.parent_job_id` up to the
//! first ancestor that is not a task. A thread gives `cairn://p/rs/document`,
//! an issue node `cairn://p/cairn/5253/1/planner`. A planner and a builder are
//! separate nodes, and a new execution (`/2/...`) is a new node.
//!
//! The rows live in the runner's own database, because the store they describe
//! is a directory on this runner and the replicas are its executors.

use cairn_common::executor_protocol::NodeScratchGrant;
use cairn_db::turso::params;

use crate::orchestrator::Orchestrator;
use crate::storage::{DbResult, LocalDb, RowExt};

/// How far up `parent_job_id` a task may nest before resolution gives up.
const MAX_TASK_DEPTH: usize = 16;

/// One node's scratch, as its authority row holds it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeScratch {
    pub id: String,
    pub project_id: String,
    pub owner_uri: String,
    pub thread_id: Option<String>,
    pub issue_id: Option<String>,
    pub head_commit: Option<String>,
    pub head_bytes: u64,
    pub head_seq: u64,
    pub written_bytes: u64,
    pub head_executor_id: Option<String>,
    pub closed_at: Option<i64>,
}

impl NodeScratch {
    pub fn grant(&self, ceiling_bytes: u64) -> NodeScratchGrant {
        NodeScratchGrant {
            scratch_id: self.id.clone(),
            owner_uri: self.owner_uri.clone(),
            head_commit: self.head_commit.clone(),
            head_seq: self.head_seq,
            ceiling_bytes,
        }
    }
}

/// The node a job's scratch belongs to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScratchOwner {
    pub job_id: String,
    pub uri: String,
    pub project_id: String,
    pub thread_id: Option<String>,
    pub issue_id: Option<String>,
}

const COLUMNS: &str = "id, project_id, owner_uri, thread_id, issue_id, head_commit, head_bytes,
     written_bytes, head_executor_id, closed_at, head_seq";

fn from_row(row: &cairn_db::turso::Row) -> DbResult<NodeScratch> {
    Ok(NodeScratch {
        id: row.text(0)?,
        project_id: row.text(1)?,
        owner_uri: row.text(2)?,
        thread_id: row.opt_text(3)?,
        issue_id: row.opt_text(4)?,
        head_commit: row.opt_text(5)?,
        head_bytes: row.i64(6)?.max(0) as u64,
        written_bytes: row.i64(7)?.max(0) as u64,
        head_executor_id: row.opt_text(8)?,
        closed_at: row.opt_i64(9)?,
        head_seq: row.i64(10)?.max(0) as u64,
    })
}

/// Strip a `/task/...` suffix: what is left is the node itself.
pub fn node_uri(uri: &str) -> &str {
    uri.split_once("/task/").map_or(uri, |(node, _)| node)
}

/// The node `job_id` belongs to, through the database its run routes to.
pub async fn owner_for_job(db: &LocalDb, job_id: &str) -> Result<Option<ScratchOwner>, String> {
    let mut current = job_id.to_owned();
    for _ in 0..MAX_TASK_DEPTH {
        let Some(uri) = crate::jobs::queries::home_uri_for_job(db, &current)
            .await
            .map_err(|error| error.to_string())?
        else {
            return Ok(None);
        };
        let row = db
            .query_opt(
                "SELECT parent_job_id, project_id, thread_id, issue_id FROM jobs WHERE id = ?1",
                (current.clone(),),
                |row| {
                    Ok((
                        row.opt_text(0)?,
                        row.opt_text(1)?,
                        row.opt_text(2)?,
                        row.opt_text(3)?,
                    ))
                },
            )
            .await
            .map_err(|error| error.to_string())?;
        let Some((parent, project_id, thread_id, issue_id)) = row else {
            return Ok(None);
        };
        match parent {
            Some(parent) if uri.contains("/task/") => current = parent,
            _ => {
                let Some(project_id) = project_id else {
                    return Ok(None);
                };
                return Ok(Some(ScratchOwner {
                    job_id: current,
                    uri: node_uri(&uri).to_owned(),
                    project_id,
                    thread_id,
                    issue_id,
                }));
            }
        }
    }
    Err(format!(
        "job {job_id} nests tasks deeper than {MAX_TASK_DEPTH}; refusing to guess its node"
    ))
}

/// The open scratch of `owner`, created on first use.
pub async fn open_for_owner(local: &LocalDb, owner: &ScratchOwner) -> DbResult<NodeScratch> {
    let id = uuid::Uuid::new_v4().to_string();
    let now = chrono::Utc::now().timestamp_millis();
    // One statement, so two first batches racing for the same node create one
    // row; the partial unique index backs it.
    local
        .execute(
            "INSERT INTO node_scratches(id, project_id, owner_uri, thread_id, issue_id, created_at)
             SELECT ?1, ?2, ?3, ?4, ?5, ?6
             WHERE NOT EXISTS (
                 SELECT 1 FROM node_scratches WHERE owner_uri = ?3 AND closed_at IS NULL)",
            params![
                id.as_str(),
                owner.project_id.as_str(),
                owner.uri.as_str(),
                owner.thread_id.as_deref(),
                owner.issue_id.as_deref(),
                now
            ],
        )
        .await?;
    local
        .query_opt(
            format!(
                "SELECT {COLUMNS} FROM node_scratches
                 WHERE owner_uri = ?1 AND closed_at IS NULL LIMIT 1"
            ),
            (owner.uri.clone(),),
            from_row,
        )
        .await?
        .ok_or_else(|| crate::storage::DbError::Row("node scratch vanished after insert".into()))
}

pub async fn load(local: &LocalDb, scratch_id: &str) -> DbResult<Option<NodeScratch>> {
    local
        .query_opt(
            format!("SELECT {COLUMNS} FROM node_scratches WHERE id = ?1"),
            (scratch_id.to_owned(),),
            from_row,
        )
        .await
}

/// Open scratches of an issue's nodes, or a thread's.
pub async fn open_for(
    local: &LocalDb,
    issue_id: Option<&str>,
    thread_id: Option<&str>,
) -> DbResult<Vec<NodeScratch>> {
    local
        .query_all(
            format!(
                "SELECT {COLUMNS} FROM node_scratches
                 WHERE closed_at IS NULL
                   AND ((?1 IS NOT NULL AND issue_id = ?1) OR (?2 IS NOT NULL AND thread_id = ?2))"
            ),
            (issue_id.map(str::to_owned), thread_id.map(str::to_owned)),
            from_row,
        )
        .await
}

/// Every scratch that has not closed.
pub async fn open_all(local: &LocalDb) -> DbResult<Vec<NodeScratch>> {
    local
        .query_all(
            format!("SELECT {COLUMNS} FROM node_scratches WHERE closed_at IS NULL"),
            (),
            from_row,
        )
        .await
}

/// Whether the node an issue or a thread stands for has ended, which is when
/// everything it owns on disk ends with it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OwnerFate {
    Live,
    /// Ended, and why, in words a log line can carry.
    Ended(&'static str),
    /// The owner's database could not be asked: a team replica that is not
    /// open, or a failed query. Never read as ended.
    Unknown(String),
}

/// The fate of the node `issue_id` (or, with no issue, `thread_id`) owns.
///
/// An issue decides when there is one: a job working an issue answers to the
/// issue even when a thread parents it. A job tied to neither answers to
/// nothing that ends, so it reads as live.
pub async fn owner_fate(
    dbs: &crate::db::DbState,
    issue_id: Option<&str>,
    thread_id: Option<&str>,
) -> OwnerFate {
    let (id, sql, missing, resolved): (&str, &str, &'static str, &[(&str, &'static str)]) =
        match (issue_id, thread_id) {
            (Some(issue), _) => (
                issue,
                "SELECT status FROM issues WHERE id = ?1",
                "its issue no longer exists",
                &[
                    ("merged", "its issue merged"),
                    ("closed", "its issue closed"),
                ],
            ),
            (None, Some(thread)) => (
                thread,
                "SELECT status FROM threads WHERE id = ?1",
                "its thread no longer exists",
                &[("closed", "its thread closed")],
            ),
            (None, None) => return OwnerFate::Live,
        };
    let db = match crate::execution::routing::routing_db_for_id(dbs, id).await {
        Ok(db) => db,
        Err(error) => return OwnerFate::Unknown(error.to_string()),
    };
    match db.query_opt(sql, (id.to_owned(),), |row| row.text(0)).await {
        Ok(None) => OwnerFate::Ended(missing),
        Ok(Some(status)) => resolved
            .iter()
            .find(|(ended, _)| *ended == status)
            .map_or(OwnerFate::Live, |(_, why)| OwnerFate::Ended(why)),
        Err(error) => OwnerFate::Unknown(error.to_string()),
    }
}

pub async fn open_in_project(local: &LocalDb, project_id: &str) -> DbResult<Vec<NodeScratch>> {
    local
        .query_all(
            format!(
                "SELECT {COLUMNS} FROM node_scratches WHERE closed_at IS NULL AND project_id = ?1"
            ),
            (project_id.to_owned(),),
            from_row,
        )
        .await
}

/// The project's configured ceiling, read from its `.cairn/config.yaml`.
pub async fn ceiling_bytes(orch: &Orchestrator, project_id: &str) -> u64 {
    // The runner's own projects table names every project's checkout on this
    // machine, team-routed or not (the object routes resolve it the same way).
    let repo_path = orch
        .db
        .local
        .query_text(
            "SELECT repo_path FROM projects WHERE id = ?1",
            (project_id.to_owned(),),
        )
        .await
        .ok()
        .flatten()
        .filter(|path| !path.is_empty());
    let settings = repo_path.map(|path| {
        crate::config::project_settings::load_project_settings_read_only(std::path::Path::new(
            &path,
        ))
    });
    cairn_config::project_settings::ScratchSettings::ceiling_bytes(
        settings
            .as_ref()
            .and_then(|settings| settings.scratch.as_ref()),
    )
}

/// A job's scratch, resolved: the authority row, the project's ceiling, and
/// the job that owns the node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedScratch {
    pub scratch: NodeScratch,
    pub ceiling_bytes: u64,
    pub owner_job_id: String,
}

/// Resolve the scratch a job's work uses, creating its row on first use.
///
/// `job_db` is the database the job's run routes to; the scratch rows are in
/// the runner's own. `Ok(None)` for a job that belongs to no node (it has no
/// home URI), which runs with a private `$TMPDIR` as before.
pub async fn resolve_scratch(
    orch: &Orchestrator,
    job_db: &LocalDb,
    job_id: &str,
) -> Result<Option<ResolvedScratch>, String> {
    let Some(owner) = owner_for_job(job_db, job_id).await? else {
        return Ok(None);
    };
    let scratch = open_for_owner(&orch.db.local, &owner)
        .await
        .map_err(|error| format!("open the node scratch of {}: {error}", owner.uri))?;
    let ceiling_bytes = ceiling_bytes(orch, &owner.project_id).await;
    Ok(Some(ResolvedScratch {
        scratch,
        ceiling_bytes,
        owner_job_id: owner.job_id,
    }))
}

/// Advance the head by compare-and-swap. `false` when the head is no longer
/// `expected` or the scratch has closed.
pub async fn advance_head(
    local: &LocalDb,
    scratch_id: &str,
    expected: Option<&str>,
    head: &str,
    head_bytes: u64,
    added_bytes: u64,
    executor_id: &str,
) -> DbResult<bool> {
    let updated = local
        .execute(
            "UPDATE node_scratches
             SET head_commit = ?3, head_bytes = ?4, written_bytes = written_bytes + ?5,
                 head_executor_id = ?6, head_seq = head_seq + 1
             WHERE id = ?1 AND closed_at IS NULL
               AND ((?2 IS NULL AND head_commit IS NULL) OR head_commit = ?2)",
            params![
                scratch_id,
                expected,
                head,
                head_bytes as i64,
                added_bytes as i64,
                executor_id
            ],
        )
        .await?;
    Ok(updated == 1)
}

/// One machine's copy of a scratch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Replica {
    pub scratch_id: String,
    pub executor_id: String,
    pub synced_commit: Option<String>,
    pub synced_written_bytes: u64,
    pub dirty: bool,
    pub refused_bytes: Option<u64>,
}

pub async fn record_replica(local: &LocalDb, replica: &Replica) -> DbResult<()> {
    local
        .execute(
            "INSERT INTO node_scratch_replicas(scratch_id, executor_id, synced_commit,
                 synced_written_bytes, dirty, refused_bytes, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(scratch_id, executor_id) DO UPDATE SET
                 synced_commit = excluded.synced_commit,
                 synced_written_bytes = excluded.synced_written_bytes,
                 dirty = excluded.dirty,
                 refused_bytes = excluded.refused_bytes,
                 updated_at = excluded.updated_at",
            params![
                replica.scratch_id.as_str(),
                replica.executor_id.as_str(),
                replica.synced_commit.as_deref(),
                replica.synced_written_bytes as i64,
                i64::from(replica.dirty),
                replica.refused_bytes.map(|bytes| bytes as i64),
                chrono::Utc::now().timestamp_millis()
            ],
        )
        .await
        .map(|_| ())
}

/// Mark a replica as holding writes a refusal kept off the head, leaving the
/// commit it last synced alone.
pub async fn record_refusal(
    local: &LocalDb,
    scratch_id: &str,
    executor_id: &str,
    refused_bytes: u64,
) -> DbResult<()> {
    local
        .execute(
            "INSERT INTO node_scratch_replicas(scratch_id, executor_id, dirty, refused_bytes,
                 updated_at)
             VALUES (?1, ?2, 1, ?3, ?4)
             ON CONFLICT(scratch_id, executor_id) DO UPDATE SET
                 dirty = 1, refused_bytes = excluded.refused_bytes,
                 updated_at = excluded.updated_at",
            params![
                scratch_id,
                executor_id,
                refused_bytes as i64,
                chrono::Utc::now().timestamp_millis()
            ],
        )
        .await
        .map(|_| ())
}

pub async fn replicas(local: &LocalDb, scratch_id: &str) -> DbResult<Vec<Replica>> {
    local
        .query_all(
            "SELECT scratch_id, executor_id, synced_commit, synced_written_bytes, dirty,
                    refused_bytes
             FROM node_scratch_replicas WHERE scratch_id = ?1",
            (scratch_id.to_owned(),),
            |row| {
                Ok(Replica {
                    scratch_id: row.text(0)?,
                    executor_id: row.text(1)?,
                    synced_commit: row.opt_text(2)?,
                    synced_written_bytes: row.i64(3)?.max(0) as u64,
                    dirty: row.i64(4)? != 0,
                    refused_bytes: row.opt_i64(5)?.map(|bytes| bytes.max(0) as u64),
                })
            },
        )
        .await
}

pub async fn forget_replica(local: &LocalDb, scratch_id: &str, executor_id: &str) -> DbResult<()> {
    local
        .execute(
            "DELETE FROM node_scratch_replicas WHERE scratch_id = ?1 AND executor_id = ?2",
            params![scratch_id, executor_id],
        )
        .await
        .map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn db() -> (tempfile::TempDir, LocalDb) {
        let temp = tempfile::tempdir().unwrap();
        let db = LocalDb::open_migrated_for_test(temp.path().join("db.turso.db"))
            .await
            .unwrap();
        (temp, db)
    }

    async fn seed(db: &LocalDb) {
        db.execute_script(
            "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('p', 'default', 'P', 'nsc', '/tmp/nsc', 1, 1);
             INSERT INTO issues(id, project_id, number, title, status, attention, created_at, updated_at)
               VALUES ('i', 'p', 7, 'Issue', 'active', 'none', 1, 1);
             INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot, triggered_by)
               VALUES ('e', 'r', 'i', 'p', 'running', 1, 1, '{}', 'manual');
             INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, status, uri_segment, created_at, updated_at)
               VALUES ('planner', 'e', 'i', 'p', 'planner', 'running', 'planner', 1, 1),
                      ('builder', 'e', 'i', 'p', 'builder', 'running', 'builder', 1, 1);
             INSERT INTO jobs(id, execution_id, issue_id, project_id, parent_job_id, node_name, status, uri_segment, created_at, updated_at)
               VALUES ('task', 'e', 'i', 'p', 'builder', 'review', 'running', 'review', 1, 1),
                      ('nested', 'e', 'i', 'p', 'task', 'deep', 'running', 'deep', 1, 1);
             INSERT INTO threads(id, project_id, name, status, attention, created_at, updated_at)
               VALUES ('t', 'p', 'document', 'active', 'none', 1, 1);
             INSERT INTO jobs(id, project_id, thread_id, node_name, status, uri_segment, created_at, updated_at)
               VALUES ('session', 'p', 't', 'Thread', 'running', 'thread', 1, 1);
             INSERT INTO jobs(id, project_id, thread_id, parent_job_id, node_name, status, uri_segment, created_at, updated_at)
               VALUES ('thread-task', 'p', 't', 'session', 'Design', 'running', 'design', 1, 1);",
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn tasks_resolve_to_their_node_and_nodes_stay_apart() {
        let (_temp, db) = db().await;
        seed(&db).await;
        let uri = |owner: Option<ScratchOwner>| owner.expect("an owner").uri;
        assert_eq!(
            uri(owner_for_job(&db, "builder").await.unwrap()),
            "cairn://p/nsc/7/1/builder"
        );
        assert_eq!(
            uri(owner_for_job(&db, "task").await.unwrap()),
            "cairn://p/nsc/7/1/builder",
            "a task shares its parent's scratch"
        );
        assert_eq!(
            uri(owner_for_job(&db, "nested").await.unwrap()),
            "cairn://p/nsc/7/1/builder",
            "a nested task resolves to the node, not to the task above it"
        );
        assert_eq!(
            uri(owner_for_job(&db, "planner").await.unwrap()),
            "cairn://p/nsc/7/1/planner",
            "planner and builder are separate nodes"
        );
        assert_eq!(
            uri(owner_for_job(&db, "session").await.unwrap()),
            "cairn://p/nsc/document"
        );
        let thread_task = owner_for_job(&db, "thread-task").await.unwrap().unwrap();
        assert_eq!(thread_task.uri, "cairn://p/nsc/document");
        assert_eq!(thread_task.thread_id.as_deref(), Some("t"));

        let builder = open_for_owner(&db, &owner_for_job(&db, "builder").await.unwrap().unwrap())
            .await
            .unwrap();
        let via_task = open_for_owner(&db, &owner_for_job(&db, "nested").await.unwrap().unwrap())
            .await
            .unwrap();
        let planner = open_for_owner(&db, &owner_for_job(&db, "planner").await.unwrap().unwrap())
            .await
            .unwrap();
        assert_eq!(builder.id, via_task.id);
        assert_ne!(builder.id, planner.id);
        assert_eq!(builder.issue_id.as_deref(), Some("i"));
    }

    #[tokio::test]
    async fn the_head_advances_only_by_compare_and_swap() {
        let (_temp, db) = db().await;
        seed(&db).await;
        let owner = owner_for_job(&db, "builder").await.unwrap().unwrap();
        let scratch = open_for_owner(&db, &owner).await.unwrap();
        let first = "a".repeat(40);
        assert!(advance_head(&db, &scratch.id, None, &first, 10, 10, "m")
            .await
            .unwrap());
        assert!(
            !advance_head(&db, &scratch.id, None, &"b".repeat(40), 5, 5, "m")
                .await
                .unwrap(),
            "a writer that read a stale head must not win"
        );
        assert!(
            advance_head(&db, &scratch.id, Some(&first), &"c".repeat(40), 4, 3, "n")
                .await
                .unwrap()
        );
        let row = load(&db, &scratch.id).await.unwrap().unwrap();
        assert_eq!(row.head_commit, Some("c".repeat(40)));
        assert_eq!((row.head_bytes, row.written_bytes), (4, 13));
        assert_eq!(row.head_seq, 2, "every advance moves the sequence");
        assert_eq!(row.head_executor_id.as_deref(), Some("n"));
    }
}
