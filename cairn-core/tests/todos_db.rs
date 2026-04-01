//! Integration tests for cairn_core::todos module.
//!
//! Tests todo CRUD operations against a real in-memory SQLite database.

mod common;

use cairn_core::todos::{
    finalize_todos, get_todo_progress, get_todos_for_children, get_todos_for_job, replace_todos,
    TodoWriteItem,
};

// ============================================================================
// Helpers
// ============================================================================

fn create_test_job(conn: &mut diesel::SqliteConnection, parent_job_id: Option<&str>) -> String {
    create_test_job_with_index(conn, parent_job_id, None)
}

/// Counter for unique project keys within a single test connection.
static PROJECT_COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

fn create_test_job_with_index(
    conn: &mut diesel::SqliteConnection,
    parent_job_id: Option<&str>,
    task_index: Option<i32>,
) -> String {
    use diesel::prelude::*;
    let id = uuid::Uuid::new_v4().to_string();
    let now = chrono::Utc::now().timestamp() as i32;

    let n = PROJECT_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let key = format!("TD{}", n);
    let project_id = common::create_test_project(conn, "TodoTest", &key);

    let task_index_val = task_index.unwrap_or(0);
    let parent_val = parent_job_id.unwrap_or("");
    let has_parent = parent_job_id.is_some();

    if has_parent {
        diesel::sql_query(
            "INSERT INTO jobs (id, project_id, status, parent_job_id, task_index, created_at, updated_at)
             VALUES (?, ?, 'running', ?, ?, ?, ?)",
        )
        .bind::<diesel::sql_types::Text, _>(&id)
        .bind::<diesel::sql_types::Text, _>(&project_id)
        .bind::<diesel::sql_types::Text, _>(parent_val)
        .bind::<diesel::sql_types::Integer, _>(task_index_val)
        .bind::<diesel::sql_types::Integer, _>(now)
        .bind::<diesel::sql_types::Integer, _>(now)
        .execute(conn)
        .expect("Failed to create test job");
    } else {
        diesel::sql_query(
            "INSERT INTO jobs (id, project_id, status, task_index, created_at, updated_at)
             VALUES (?, ?, 'running', ?, ?, ?)",
        )
        .bind::<diesel::sql_types::Text, _>(&id)
        .bind::<diesel::sql_types::Text, _>(&project_id)
        .bind::<diesel::sql_types::Integer, _>(task_index_val)
        .bind::<diesel::sql_types::Integer, _>(now)
        .bind::<diesel::sql_types::Integer, _>(now)
        .execute(conn)
        .expect("Failed to create test job");
    }

    id
}

fn make_item(content: &str, status: &str) -> TodoWriteItem {
    TodoWriteItem {
        id: None,
        content: content.to_string(),
        status: status.to_string(),
        priority: None,
        active_form: None,
    }
}

fn make_item_full(
    id: Option<&str>,
    content: &str,
    status: &str,
    priority: Option<&str>,
    active_form: Option<&str>,
) -> TodoWriteItem {
    TodoWriteItem {
        id: id.map(|s| s.to_string()),
        content: content.to_string(),
        status: status.to_string(),
        priority: priority.map(|s| s.to_string()),
        active_form: active_form.map(|s| s.to_string()),
    }
}

// ============================================================================
// replace_todos
// ============================================================================

#[test]
fn replace_todos_inserts_and_returns_items() {
    let mut conn = common::test_conn();
    let job_id = create_test_job(&mut conn, None);

    let items = vec![
        make_item("Set up database", "completed"),
        make_item("Write tests", "in_progress"),
        make_item("Deploy", "pending"),
    ];

    let result = replace_todos(&mut conn, &job_id, &items).unwrap();
    assert_eq!(result.len(), 3);
    assert_eq!(result[0].content, "Set up database");
    assert_eq!(result[0].status, "completed");
    assert_eq!(result[1].content, "Write tests");
    assert_eq!(result[1].status, "in_progress");
    assert_eq!(result[2].content, "Deploy");
    assert_eq!(result[2].status, "pending");
}

#[test]
fn replace_todos_generates_default_id_when_none() {
    let mut conn = common::test_conn();
    let job_id = create_test_job(&mut conn, None);

    let items = vec![
        make_item("First", "pending"),
        make_item("Second", "pending"),
    ];
    let result = replace_todos(&mut conn, &job_id, &items).unwrap();

    assert_eq!(result[0].id, "todo-0");
    assert_eq!(result[1].id, "todo-1");
}

#[test]
fn replace_todos_uses_provided_id() {
    let mut conn = common::test_conn();
    let job_id = create_test_job(&mut conn, None);

    let items = vec![make_item_full(
        Some("setup-db"),
        "Set up database",
        "pending",
        None,
        None,
    )];
    let result = replace_todos(&mut conn, &job_id, &items).unwrap();

    assert_eq!(result[0].id, "setup-db");
}

#[test]
fn replace_todos_deletes_previous_items() {
    let mut conn = common::test_conn();
    let job_id = create_test_job(&mut conn, None);

    // First write: 3 items
    let items1 = vec![
        make_item("A", "pending"),
        make_item("B", "pending"),
        make_item("C", "pending"),
    ];
    replace_todos(&mut conn, &job_id, &items1).unwrap();

    // Second write: 1 item — should replace all
    let items2 = vec![make_item("Only", "in_progress")];
    replace_todos(&mut conn, &job_id, &items2).unwrap();

    let loaded = get_todos_for_job(&mut conn, &job_id).unwrap();
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].content, "Only");
}

#[test]
fn replace_todos_with_empty_list_clears_all() {
    let mut conn = common::test_conn();
    let job_id = create_test_job(&mut conn, None);

    replace_todos(&mut conn, &job_id, &[make_item("X", "pending")]).unwrap();
    replace_todos(&mut conn, &job_id, &[]).unwrap();

    let loaded = get_todos_for_job(&mut conn, &job_id).unwrap();
    assert!(loaded.is_empty());
}

#[test]
fn replace_todos_preserves_priority_and_active_form() {
    let mut conn = common::test_conn();
    let job_id = create_test_job(&mut conn, None);

    let items = vec![make_item_full(
        Some("deploy"),
        "Deploy to prod",
        "in_progress",
        Some("high"),
        Some("Deploying to prod"),
    )];
    let result = replace_todos(&mut conn, &job_id, &items).unwrap();

    assert_eq!(result[0].priority.as_deref(), Some("high"));
    assert_eq!(result[0].active_form.as_deref(), Some("Deploying to prod"));

    // Verify roundtrip through DB
    let loaded = get_todos_for_job(&mut conn, &job_id).unwrap();
    assert_eq!(loaded[0].priority.as_deref(), Some("high"));
    assert_eq!(loaded[0].active_form.as_deref(), Some("Deploying to prod"));
}

#[test]
fn replace_todos_isolates_between_jobs() {
    let mut conn = common::test_conn();
    let job1 = create_test_job(&mut conn, None);
    let job2 = create_test_job(&mut conn, None);

    replace_todos(&mut conn, &job1, &[make_item("Job1 task", "pending")]).unwrap();
    replace_todos(&mut conn, &job2, &[make_item("Job2 task", "completed")]).unwrap();

    let todos1 = get_todos_for_job(&mut conn, &job1).unwrap();
    let todos2 = get_todos_for_job(&mut conn, &job2).unwrap();

    assert_eq!(todos1.len(), 1);
    assert_eq!(todos1[0].content, "Job1 task");
    assert_eq!(todos2.len(), 1);
    assert_eq!(todos2[0].content, "Job2 task");

    // Replacing job1's todos doesn't affect job2
    replace_todos(&mut conn, &job1, &[]).unwrap();
    let todos2_after = get_todos_for_job(&mut conn, &job2).unwrap();
    assert_eq!(todos2_after.len(), 1);
}

// ============================================================================
// get_todos_for_job
// ============================================================================

#[test]
fn get_todos_for_job_returns_empty_when_none() {
    let mut conn = common::test_conn();
    let job_id = create_test_job(&mut conn, None);

    let result = get_todos_for_job(&mut conn, &job_id).unwrap();
    assert!(result.is_empty());
}

#[test]
fn get_todos_for_job_preserves_position_order() {
    let mut conn = common::test_conn();
    let job_id = create_test_job(&mut conn, None);

    let items = vec![
        make_item("First", "pending"),
        make_item("Second", "in_progress"),
        make_item("Third", "completed"),
    ];
    replace_todos(&mut conn, &job_id, &items).unwrap();

    let loaded = get_todos_for_job(&mut conn, &job_id).unwrap();
    assert_eq!(loaded[0].content, "First");
    assert_eq!(loaded[1].content, "Second");
    assert_eq!(loaded[2].content, "Third");
}

// ============================================================================
// get_todos_for_children
// ============================================================================

#[test]
fn get_todos_for_children_collects_child_todos() {
    let mut conn = common::test_conn();
    let parent_id = create_test_job(&mut conn, None);
    let child1 = create_test_job_with_index(&mut conn, Some(&parent_id), Some(0));
    let child2 = create_test_job_with_index(&mut conn, Some(&parent_id), Some(1));

    replace_todos(&mut conn, &child1, &[make_item("Child1 task", "completed")]).unwrap();
    replace_todos(&mut conn, &child2, &[make_item("Child2 task", "pending")]).unwrap();

    let results = get_todos_for_children(&mut conn, &parent_id).unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0, child1);
    assert_eq!(results[0].1[0].content, "Child1 task");
    assert_eq!(results[1].0, child2);
    assert_eq!(results[1].1[0].content, "Child2 task");
}

#[test]
fn get_todos_for_children_skips_empty_children() {
    let mut conn = common::test_conn();
    let parent_id = create_test_job(&mut conn, None);
    let child1 = create_test_job_with_index(&mut conn, Some(&parent_id), Some(0));
    let _child2 = create_test_job_with_index(&mut conn, Some(&parent_id), Some(1));

    // Only child1 has todos
    replace_todos(&mut conn, &child1, &[make_item("Only child", "pending")]).unwrap();

    let results = get_todos_for_children(&mut conn, &parent_id).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, child1);
}

#[test]
fn get_todos_for_children_returns_empty_when_no_children() {
    let mut conn = common::test_conn();
    let parent_id = create_test_job(&mut conn, None);

    let results = get_todos_for_children(&mut conn, &parent_id).unwrap();
    assert!(results.is_empty());
}

// ============================================================================
// finalize_todos
// ============================================================================

#[test]
fn finalize_todos_marks_in_progress_as_completed() {
    let mut conn = common::test_conn();
    let job_id = create_test_job(&mut conn, None);

    let items = vec![
        make_item("Done", "completed"),
        make_item("Working", "in_progress"),
        make_item("Not started", "pending"),
    ];
    replace_todos(&mut conn, &job_id, &items).unwrap();

    finalize_todos(&mut conn, &job_id).unwrap();

    let loaded = get_todos_for_job(&mut conn, &job_id).unwrap();
    assert_eq!(loaded[0].status, "completed"); // was already completed
    assert_eq!(loaded[1].status, "completed"); // was in_progress → completed
    assert_eq!(loaded[2].status, "pending"); // pending stays pending
}

#[test]
fn finalize_todos_noop_when_no_in_progress() {
    let mut conn = common::test_conn();
    let job_id = create_test_job(&mut conn, None);

    let items = vec![
        make_item("Done", "completed"),
        make_item("Waiting", "pending"),
    ];
    replace_todos(&mut conn, &job_id, &items).unwrap();

    finalize_todos(&mut conn, &job_id).unwrap();

    let loaded = get_todos_for_job(&mut conn, &job_id).unwrap();
    assert_eq!(loaded[0].status, "completed");
    assert_eq!(loaded[1].status, "pending");
}

#[test]
fn finalize_todos_noop_for_empty_list() {
    let mut conn = common::test_conn();
    let job_id = create_test_job(&mut conn, None);

    // No error on empty
    finalize_todos(&mut conn, &job_id).unwrap();
}

// ============================================================================
// get_todo_progress
// ============================================================================

#[test]
fn get_todo_progress_returns_none_when_empty() {
    let mut conn = common::test_conn();
    let job_id = create_test_job(&mut conn, None);

    assert!(get_todo_progress(&mut conn, &job_id).is_none());
}

#[test]
fn get_todo_progress_formats_correctly() {
    let mut conn = common::test_conn();
    let job_id = create_test_job(&mut conn, None);

    let items = vec![
        make_item("A", "completed"),
        make_item("B", "completed"),
        make_item("C", "in_progress"),
        make_item("D", "pending"),
        make_item("E", "pending"),
    ];
    replace_todos(&mut conn, &job_id, &items).unwrap();

    let progress = get_todo_progress(&mut conn, &job_id).unwrap();
    assert_eq!(progress, "2/5 todos");
}

#[test]
fn get_todo_progress_all_completed() {
    let mut conn = common::test_conn();
    let job_id = create_test_job(&mut conn, None);

    let items = vec![make_item("A", "completed"), make_item("B", "completed")];
    replace_todos(&mut conn, &job_id, &items).unwrap();

    let progress = get_todo_progress(&mut conn, &job_id).unwrap();
    assert_eq!(progress, "2/2 todos");
}

#[test]
fn get_todo_progress_none_completed() {
    let mut conn = common::test_conn();
    let job_id = create_test_job(&mut conn, None);

    let items = vec![make_item("A", "pending"), make_item("B", "in_progress")];
    replace_todos(&mut conn, &job_id, &items).unwrap();

    let progress = get_todo_progress(&mut conn, &job_id).unwrap();
    assert_eq!(progress, "0/2 todos");
}

// ============================================================================
// TodoItem From<DbTodo> (maps todo_id, not internal id)
// ============================================================================

#[test]
fn todo_item_from_db_uses_todo_id_not_internal_id() {
    let mut conn = common::test_conn();
    let job_id = create_test_job(&mut conn, None);

    let items = vec![make_item_full(
        Some("my-custom-id"),
        "Custom task",
        "pending",
        Some("high"),
        Some("Working on custom task"),
    )];
    replace_todos(&mut conn, &job_id, &items).unwrap();

    let loaded = get_todos_for_job(&mut conn, &job_id).unwrap();
    // The id should be the todo_id ("my-custom-id"), not the internal UUID
    assert_eq!(loaded[0].id, "my-custom-id");
    assert_eq!(loaded[0].content, "Custom task");
    assert_eq!(loaded[0].status, "pending");
    assert_eq!(loaded[0].priority.as_deref(), Some("high"));
    assert_eq!(
        loaded[0].active_form.as_deref(),
        Some("Working on custom task")
    );
}
