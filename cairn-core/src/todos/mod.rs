//! Todo management — CRUD operations for the `todos` table.
//!
//! Each `todo_write` call replaces all todos for the calling job
//! (DELETE + INSERT), matching Claude's native TodoWrite semantics
//! where each call sends the full list.

use crate::diesel_models::{DbTodo, NewTodo};
use crate::models::TodoItem;
use crate::schema::todos;
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;

/// Replace all todos for a job with the given list.
/// This is the core write operation — DELETE existing + INSERT new.
pub fn replace_todos(
    conn: &mut SqliteConnection,
    job_id: &str,
    items: &[TodoWriteItem],
) -> Result<Vec<TodoItem>, String> {
    let now = chrono::Utc::now().timestamp() as i32;

    // Delete existing todos for this job
    diesel::delete(todos::table.filter(todos::job_id.eq(job_id)))
        .execute(conn)
        .map_err(|e| format!("Failed to delete existing todos: {}", e))?;

    // Insert new todos
    let mut result = Vec::with_capacity(items.len());
    for (position, item) in items.iter().enumerate() {
        let id = uuid::Uuid::new_v4().to_string();
        let default_id = format!("todo-{}", position);
        let todo_id = item.id.as_deref().unwrap_or(&default_id);

        let new_todo = NewTodo {
            id: &id,
            job_id,
            todo_id,
            content: &item.content,
            status: &item.status,
            priority: item.priority.as_deref(),
            active_form: item.active_form.as_deref(),
            position: position as i32,
            created_at: now,
            updated_at: now,
        };

        diesel::insert_into(todos::table)
            .values(&new_todo)
            .execute(conn)
            .map_err(|e| format!("Failed to insert todo: {}", e))?;

        result.push(TodoItem {
            id: todo_id.to_string(),
            content: item.content.clone(),
            status: item.status.clone(),
            priority: item.priority.clone(),
            active_form: item.active_form.clone(),
        });
    }

    Ok(result)
}

/// Get all todos for a job, ordered by position.
pub fn get_todos_for_job(
    conn: &mut SqliteConnection,
    job_id: &str,
) -> Result<Vec<TodoItem>, String> {
    let db_todos: Vec<DbTodo> = todos::table
        .filter(todos::job_id.eq(job_id))
        .order(todos::position.asc())
        .load(conn)
        .map_err(|e| format!("Failed to load todos: {}", e))?;

    Ok(db_todos.into_iter().map(TodoItem::from).collect())
}

/// Get todos for all child jobs of a parent job.
/// Returns (job_id, todos) pairs for each child that has todos.
pub fn get_todos_for_children(
    conn: &mut SqliteConnection,
    parent_job_id: &str,
) -> Result<Vec<(String, Vec<TodoItem>)>, String> {
    use crate::schema::jobs;

    let child_job_ids: Vec<String> = jobs::table
        .filter(jobs::parent_job_id.eq(parent_job_id))
        .select(jobs::id)
        .order(jobs::task_index.asc())
        .load(conn)
        .map_err(|e| format!("Failed to load child jobs: {}", e))?;

    let mut results = Vec::new();
    for child_id in child_job_ids {
        let todos = get_todos_for_job(conn, &child_id)?;
        if !todos.is_empty() {
            results.push((child_id, todos));
        }
    }

    Ok(results)
}

/// Mark all in_progress todos as completed for a job.
/// Called during finalization when a job completes.
pub fn finalize_todos(conn: &mut SqliteConnection, job_id: &str) -> Result<(), String> {
    let now = chrono::Utc::now().timestamp() as i32;
    diesel::update(
        todos::table
            .filter(todos::job_id.eq(job_id))
            .filter(todos::status.eq("in_progress")),
    )
    .set((todos::status.eq("completed"), todos::updated_at.eq(now)))
    .execute(conn)
    .map_err(|e| format!("Failed to finalize todos: {}", e))?;

    Ok(())
}

/// Get todo progress string like "3/5 todos" for a job.
pub fn get_todo_progress(conn: &mut SqliteConnection, job_id: &str) -> Option<String> {
    let db_todos: Vec<DbTodo> = todos::table
        .filter(todos::job_id.eq(job_id))
        .load(conn)
        .ok()?;

    if db_todos.is_empty() {
        return None;
    }

    let completed = db_todos.iter().filter(|t| t.status == "completed").count();
    Some(format!("{}/{} todos", completed, db_todos.len()))
}

/// Input item from the MCP tool call (before DB insertion).
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
