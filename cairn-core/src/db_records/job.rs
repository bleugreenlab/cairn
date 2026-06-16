//! Job models for database records (replaces Timeline Node)

use crate::storage::{DbResult, RowExt};

/// Canonical column projection for rows mapped by [`db_job_from_row`].
///
/// Keep this list in exactly the same order as the positional reads in
/// `db_job_from_row`; all jobs queries should import this constant rather than
/// spelling out their own projection.
pub const JOB_COLUMNS: &str = "id, execution_id, recipe_node_id, parent_job_id,
    worktree_path, branch, base_commit, current_session_id, resume_session_id, status,
    agent_config_id, issue_id, project_id, task_description, created_at, updated_at,
    completed_at, parent_tool_use_id, task_index, started_at, model, node_name,
    base_branch, current_turn_id, uri_segment, pack_anchor";

#[derive(Debug, Clone)]
pub struct DbJob {
    pub id: String,
    pub execution_id: Option<String>,
    pub recipe_node_id: Option<String>,
    pub parent_job_id: Option<String>,
    pub worktree_path: Option<String>,
    pub branch: Option<String>,
    pub base_commit: Option<String>,
    /// Nearest durable ancestor commit (reachable from the project default
    /// branch), captured alongside `base_commit` at worktree creation. NULL
    /// when unresolvable.
    pub pack_anchor: Option<String>,
    pub current_session_id: Option<String>,
    pub resume_session_id: Option<String>,
    pub status: String,
    pub agent_config_id: Option<String>,
    pub issue_id: Option<String>,
    pub project_id: String,
    pub task_description: Option<String>,
    pub created_at: i32,
    pub updated_at: i32,
    pub completed_at: Option<i32>,
    pub parent_tool_use_id: Option<String>,
    pub task_index: Option<i32>,
    pub started_at: Option<i32>,
    pub model: Option<String>,
    pub node_name: Option<String>,
    pub base_branch: Option<String>,
    pub current_turn_id: Option<String>,
    pub uri_segment: Option<String>,
}

pub fn db_job_from_row(row: &turso::Row) -> DbResult<DbJob> {
    Ok(DbJob {
        id: row.text(0)?,
        execution_id: row.opt_text(1)?,
        recipe_node_id: row.opt_text(2)?,
        parent_job_id: row.opt_text(3)?,
        worktree_path: row.opt_text(4)?,
        branch: row.opt_text(5)?,
        base_commit: row.opt_text(6)?,
        current_session_id: row.opt_text(7)?,
        resume_session_id: row.opt_text(8)?,
        status: row.text(9)?,
        agent_config_id: row.opt_text(10)?,
        issue_id: row.opt_text(11)?,
        project_id: row.text(12)?,
        task_description: row.opt_text(13)?,
        created_at: row.i64(14)? as i32,
        updated_at: row.i64(15)? as i32,
        completed_at: row.opt_i64(16)?.map(|value| value as i32),
        parent_tool_use_id: row.opt_text(17)?,
        task_index: row.opt_i64(18)?.map(|value| value as i32),
        started_at: row.opt_i64(19)?.map(|value| value as i32),
        model: row.opt_text(20)?,
        node_name: row.opt_text(21)?,
        base_branch: row.opt_text(22)?,
        current_turn_id: row.opt_text(23)?,
        uri_segment: row.opt_text(24)?,
        pack_anchor: row.opt_text(25)?,
    })
}

/// Load the live (non-cancelled) job for a recipe node within an execution,
/// preferring the newest attempt.
///
/// Restart-node archives a node's prior job as `cancelled` and creates a fresh
/// one (see `execution::advancement::restart_node`), so a single recipe node can
/// own several job rows at once. Every per-node *job* lookup must resolve to the
/// live attempt, never the cancelled archive, or downstream readiness and input
/// resolution read stale state after a restart. This is the one canonical
/// node→job lookup; callers that previously spelled their own
/// `WHERE execution_id = ? AND recipe_node_id = ? LIMIT 1` should route here.
///
/// Cascade reads that key on `status = 'failed'` (e.g. `upstream_failed_conn`)
/// are already safe — a cancelled row never matches — and intentionally stay as
/// they are.
pub async fn load_live_job_by_execution_node_conn(
    conn: &turso::Connection,
    execution_id: &str,
    recipe_node_id: &str,
) -> DbResult<Option<DbJob>> {
    let sql = format!(
        "SELECT {JOB_COLUMNS}
         FROM jobs
         WHERE execution_id = ?1 AND recipe_node_id = ?2 AND status <> 'cancelled'
         ORDER BY created_at DESC
         LIMIT 1"
    );
    let mut rows = conn
        .query(&sql, turso::params![execution_id, recipe_node_id])
        .await?;
    rows.next()
        .await?
        .map(|row| db_job_from_row(&row))
        .transpose()
}

#[derive(Debug)]
pub struct NewJob<'a> {
    pub id: &'a str,
    pub execution_id: Option<&'a str>,
    pub recipe_node_id: Option<&'a str>,
    pub parent_job_id: Option<&'a str>,
    pub worktree_path: Option<&'a str>,
    pub branch: Option<&'a str>,
    pub base_commit: Option<&'a str>,
    pub pack_anchor: Option<&'a str>,
    pub current_session_id: Option<&'a str>,
    pub resume_session_id: Option<&'a str>,
    pub status: &'a str,
    pub agent_config_id: Option<&'a str>,
    pub issue_id: Option<&'a str>,
    pub project_id: &'a str,
    pub task_description: Option<&'a str>,
    pub created_at: i32,
    pub updated_at: i32,
    pub completed_at: Option<i32>,
    pub parent_tool_use_id: Option<&'a str>,
    pub task_index: Option<i32>,
    pub started_at: Option<i32>,
    pub model: Option<&'a str>,
    pub node_name: Option<&'a str>,
    pub base_branch: Option<&'a str>,
    pub current_turn_id: Option<&'a str>,
    pub uri_segment: Option<&'a str>,
}

#[derive(Debug, Default)]
pub struct UpdateJobChangeset<'a> {
    pub worktree_path: Option<Option<&'a str>>,
    pub branch: Option<Option<&'a str>>,
    pub base_commit: Option<Option<&'a str>>,
    pub pack_anchor: Option<Option<&'a str>>,
    pub current_session_id: Option<Option<&'a str>>,
    pub resume_session_id: Option<Option<&'a str>>,
    pub status: Option<&'a str>,
    pub updated_at: Option<i32>,
    pub completed_at: Option<Option<i32>>,
    pub started_at: Option<Option<i32>>,
    pub model: Option<Option<&'a str>>,
}
