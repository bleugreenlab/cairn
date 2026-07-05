//! Todo management — CRUD operations for the `todos` table.
//!
//! Todos are addressed by their owning job's `todos` URI and mutated through the
//! `write` tool: `replace` rewrites the full list (DELETE + INSERT), `append`
//! adds items after the current tail, and `patch` updates matched items by id.

use crate::models::TodoItem;
use crate::storage::{DbError, DbResult, LocalDb, RowExt};
use cairn_common::ids;
use cairn_db::turso::params;

/// Compact one-line-per-todo rendering (`[id] content - status`) returned in
/// `write` results so the post-mutation state — including freshly assigned
/// ids — is visible without a follow-up read. Empty list renders `(no todos)`.
pub fn format_todos_compact(todos: &[TodoItem]) -> String {
    if todos.is_empty() {
        return "(no todos)".to_string();
    }
    todos
        .iter()
        .map(|t| format!("[{}] {} - {}", t.id, t.content, t.status))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Replace all todos for a job.
pub async fn replace_todos(
    db: &LocalDb,
    job_id: &str,
    items: &[TodoWriteItem],
) -> Result<Vec<TodoItem>, String> {
    let job_id = job_id.to_string();
    let items = items.to_vec();
    db.write(|conn| {
        let job_id = job_id.clone();
        let items = items.clone();
        Box::pin(async move {
            let now = chrono::Utc::now().timestamp();

            conn.execute("DELETE FROM todos WHERE job_id = ?1", (job_id.as_str(),))
                .await?;

            let mut result = Vec::with_capacity(items.len());
            for (position, item) in items.iter().enumerate() {
                let id = ids::mint_child(&job_id);
                // Default to the 1-based position so the id matches how the
                // list reads and is trivially guessable (item 1, 2, 3…). The
                // `todos` table is already scoped by `job_id`, so there's
                // nothing to namespace against — a bare number is enough.
                let default_id = (position + 1).to_string();
                let todo_id = item.id.as_deref().unwrap_or(&default_id);

                conn.execute(
                    "
                    INSERT INTO todos (
                        id, job_id, todo_id, content, status, priority,
                        active_form, position, created_at, updated_at
                    )
                    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                    ",
                    params![
                        id.as_str(),
                        job_id.as_str(),
                        todo_id,
                        item.content.as_str(),
                        item.status.as_str(),
                        item.priority.as_deref(),
                        item.active_form.as_deref(),
                        position as i64,
                        now,
                        now
                    ],
                )
                .await?;

                result.push(TodoItem {
                    id: todo_id.to_string(),
                    content: item.content.clone(),
                    status: item.status.clone(),
                    priority: item.priority.clone(),
                    active_form: item.active_form.clone(),
                });
            }

            Ok(result)
        })
    })
    .await
    .map_err(|e| format!("Failed to replace todos: {e}"))
}

/// Append todos to the end of a job's list, continuing the `position` sequence.
pub async fn append_todos(
    db: &LocalDb,
    job_id: &str,
    items: &[TodoWriteItem],
) -> Result<Vec<TodoItem>, String> {
    let job_id = job_id.to_string();
    let items = items.to_vec();
    db.write(|conn| {
        let job_id = job_id.clone();
        let items = items.clone();
        Box::pin(async move {
            let now = chrono::Utc::now().timestamp();

            // Find the current tail position (NULL when the job has no todos yet).
            let mut rows = conn
                .query(
                    "SELECT COALESCE(MAX(position), -1) FROM todos WHERE job_id = ?1",
                    (job_id.as_str(),),
                )
                .await?;
            let mut next_position = match rows.next().await? {
                Some(row) => row.i64(0)? + 1,
                None => 0,
            };

            for item in items.iter() {
                let id = ids::mint_child(&job_id);
                // 1-based position id; see `replace_todos` for rationale.
                let default_id = (next_position + 1).to_string();
                let todo_id = item.id.as_deref().unwrap_or(&default_id);

                conn.execute(
                    "
                    INSERT INTO todos (
                        id, job_id, todo_id, content, status, priority,
                        active_form, position, created_at, updated_at
                    )
                    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                    ",
                    params![
                        id.as_str(),
                        job_id.as_str(),
                        todo_id,
                        item.content.as_str(),
                        item.status.as_str(),
                        item.priority.as_deref(),
                        item.active_form.as_deref(),
                        next_position,
                        now,
                        now
                    ],
                )
                .await?;

                next_position += 1;
            }

            get_todos_for_job_conn(conn, &job_id).await
        })
    })
    .await
    .map_err(|e| format!("Failed to append todos: {e}"))
}

/// Apply partial updates to existing todos, matched by `todo_id`.
///
/// Only the fields present on each update are written; the rest are left intact.
/// Returns an error naming any `todo_id` that does not exist for the job.
pub async fn update_todos(
    db: &LocalDb,
    job_id: &str,
    updates: &[TodoUpdateItem],
) -> Result<Vec<TodoItem>, String> {
    let job_id = job_id.to_string();
    let updates = updates.to_vec();
    db.write(|conn| {
        let job_id = job_id.clone();
        let updates = updates.clone();
        Box::pin(async move {
            let now = chrono::Utc::now().timestamp();

            for update in updates.iter() {
                let mut rows = conn
                    .query(
                        "SELECT 1 FROM todos WHERE job_id = ?1 AND todo_id = ?2 LIMIT 1",
                        params![job_id.as_str(), update.id.as_str()],
                    )
                    .await?;
                if rows.next().await?.is_none() {
                    return Err(DbError::internal(format!(
                        "No todo with id '{}' for this job",
                        update.id
                    )));
                }

                if let Some(content) = update.content.as_deref() {
                    conn.execute(
                        "UPDATE todos SET content = ?1, updated_at = ?2 WHERE job_id = ?3 AND todo_id = ?4",
                        params![content, now, job_id.as_str(), update.id.as_str()],
                    )
                    .await?;
                }
                if let Some(status) = update.status.as_deref() {
                    conn.execute(
                        "UPDATE todos SET status = ?1, updated_at = ?2 WHERE job_id = ?3 AND todo_id = ?4",
                        params![status, now, job_id.as_str(), update.id.as_str()],
                    )
                    .await?;
                }
                if let Some(priority) = update.priority.as_deref() {
                    conn.execute(
                        "UPDATE todos SET priority = ?1, updated_at = ?2 WHERE job_id = ?3 AND todo_id = ?4",
                        params![priority, now, job_id.as_str(), update.id.as_str()],
                    )
                    .await?;
                }
                if let Some(active_form) = update.active_form.as_deref() {
                    conn.execute(
                        "UPDATE todos SET active_form = ?1, updated_at = ?2 WHERE job_id = ?3 AND todo_id = ?4",
                        params![active_form, now, job_id.as_str(), update.id.as_str()],
                    )
                    .await?;
                }
            }

            get_todos_for_job_conn(conn, &job_id).await
        })
    })
    .await
    .map_err(|e| format!("Failed to update todos: {e}"))
}

/// Get all todos for a job, ordered by position.
pub async fn get_todos_for_job(db: &LocalDb, job_id: &str) -> Result<Vec<TodoItem>, String> {
    let job_id = job_id.to_string();
    db.read(|conn| Box::pin(async move { get_todos_for_job_conn(conn, &job_id).await }))
        .await
        .map_err(|e| format!("Failed to load todos: {e}"))
}

async fn get_todos_for_job_conn(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
) -> DbResult<Vec<TodoItem>> {
    let mut rows = conn
        .query(
            "
            SELECT todo_id, content, status, priority, active_form
            FROM todos
            WHERE job_id = ?1
            ORDER BY position ASC
            ",
            (job_id,),
        )
        .await?;

    let mut todos = Vec::new();
    while let Some(row) = rows.next().await? {
        todos.push(TodoItem {
            id: row.text(0)?,
            content: row.text(1)?,
            status: row.text(2)?,
            priority: row.opt_text(3)?,
            active_form: row.opt_text(4)?,
        });
    }
    Ok(todos)
}

/// Get todos for all child jobs of a parent job.
pub async fn get_todos_for_children(
    db: &LocalDb,
    parent_job_id: &str,
) -> Result<Vec<(String, Vec<TodoItem>)>, String> {
    let parent_job_id = parent_job_id.to_string();
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "
                    SELECT id
                    FROM jobs
                    WHERE parent_job_id = ?1
                    ORDER BY task_index ASC
                    ",
                    (parent_job_id.as_str(),),
                )
                .await?;

            let mut results = Vec::new();
            while let Some(row) = rows.next().await? {
                let child_id = row.text(0)?;
                let todos = get_todos_for_job_conn(conn, &child_id).await?;
                if !todos.is_empty() {
                    results.push((child_id, todos));
                }
            }
            Ok(results)
        })
    })
    .await
    .map_err(|e| format!("Failed to load child todos: {e}"))
}

/// Get current todos for every job in an issue.
pub async fn get_todos_for_issue(
    db: &LocalDb,
    issue_id: &str,
) -> Result<Vec<(String, Vec<TodoItem>)>, String> {
    let issue_id = issue_id.to_string();
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id FROM jobs WHERE issue_id = ?1",
                    (issue_id.as_str(),),
                )
                .await?;

            let mut results = Vec::new();
            while let Some(row) = rows.next().await? {
                let job_id = row.text(0)?;
                let todos = get_todos_for_job_conn(conn, &job_id).await?;
                if !todos.is_empty() {
                    results.push((job_id, todos));
                }
            }
            Ok(results)
        })
    })
    .await
    .map_err(|e| format!("Failed to load issue todos: {e}"))
}

/// Mark all in-progress todos as completed for a job.
pub async fn finalize_todos(db: &LocalDb, job_id: &str) -> Result<(), String> {
    let job_id = job_id.to_string();
    db.write(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let now = chrono::Utc::now().timestamp();
            conn.execute(
                "
                UPDATE todos
                SET status = 'completed', updated_at = ?1
                WHERE job_id = ?2 AND status = 'in_progress'
                ",
                params![now, job_id.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|e| format!("Failed to finalize todos: {e}"))
}

/// Get todo progress string like "3/5 todos" for a job.
pub async fn get_todo_progress(db: &LocalDb, job_id: &str) -> Option<String> {
    let todos = get_todos_for_job(db, job_id).await.ok()?;
    if todos.is_empty() {
        return None;
    }

    let completed = todos.iter().filter(|t| t.status == "completed").count();
    Some(format!("{}/{} todos", completed, todos.len()))
}

/// Accepted keys for one `TodoWriteItem`, surfaced verbatim in a rejection when
/// an append/replace item is mis-keyed (e.g. `title` instead of `content`) so
/// the agent learns the real shape without a discovery round-trip. Keep in sync
/// with the struct fields below.
pub const TODO_WRITE_ITEM_KEYS: &str =
    "content (required), status (required), id, priority, activeForm";

/// Accepted keys for one `TodoUpdateItem` (a partial update matched by `id`).
/// Keep in sync with the struct fields below.
pub const TODO_UPDATE_ITEM_KEYS: &str = "id (required), content, status, priority, activeForm";

/// Input item for replace/append (before DB insertion).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TodoWriteItem {
    /// Agent-assigned ID
    pub id: Option<String>,
    pub content: String,
    pub status: String,
    pub priority: Option<String>,
    pub active_form: Option<String>,
}

/// Partial update for an existing todo, matched by `id` (the agent-assigned
/// `todo_id`). Absent fields are left unchanged.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TodoUpdateItem {
    pub id: String,
    #[serde(default)]
    pub content: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub priority: Option<String>,
    #[serde(default)]
    pub active_form: Option<String>,
}
