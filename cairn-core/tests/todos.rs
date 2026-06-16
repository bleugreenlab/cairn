//! Integration tests for cairn_core::todos.

mod common;

use cairn_core::todos::{
    append_todos, finalize_todos, format_todos_compact, get_todo_progress, get_todos_for_children,
    get_todos_for_job, replace_todos, update_todos, TodoUpdateItem, TodoWriteItem,
};

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
        id: id.map(str::to_string),
        content: content.to_string(),
        status: status.to_string(),
        priority: priority.map(str::to_string),
        active_form: active_form.map(str::to_string),
    }
}

#[tokio::test]
async fn replace_todos_inserts_and_returns_items() {
    let (_temp, db) = common::migrated_db().await;
    let job_id = common::create_job(&db, None, None).await;

    let items = vec![
        make_item("Set up database", "completed"),
        make_item("Write tests", "in_progress"),
        make_item("Deploy", "pending"),
    ];

    let result = replace_todos(&db, &job_id, &items).await.unwrap();
    assert_eq!(result.len(), 3);
    assert_eq!(result[0].content, "Set up database");
    assert_eq!(result[0].status, "completed");
    assert_eq!(result[1].content, "Write tests");
    assert_eq!(result[1].status, "in_progress");
    assert_eq!(result[2].content, "Deploy");
    assert_eq!(result[2].status, "pending");
}

#[tokio::test]
async fn replace_todos_generates_default_id_when_none() {
    let (_temp, db) = common::migrated_db().await;
    let job_id = common::create_job(&db, None, None).await;

    let result = replace_todos(
        &db,
        &job_id,
        &[
            make_item("First", "pending"),
            make_item("Second", "pending"),
        ],
    )
    .await
    .unwrap();

    assert_eq!(result[0].id, "1");
    assert_eq!(result[1].id, "2");
}

#[tokio::test]
async fn replace_todos_uses_provided_id() {
    let (_temp, db) = common::migrated_db().await;
    let job_id = common::create_job(&db, None, None).await;

    let result = replace_todos(
        &db,
        &job_id,
        &[make_item_full(
            Some("setup-db"),
            "Set up database",
            "pending",
            None,
            None,
        )],
    )
    .await
    .unwrap();

    assert_eq!(result[0].id, "setup-db");
}

#[tokio::test]
async fn replace_todos_deletes_previous_items() {
    let (_temp, db) = common::migrated_db().await;
    let job_id = common::create_job(&db, None, None).await;

    replace_todos(
        &db,
        &job_id,
        &[
            make_item("A", "pending"),
            make_item("B", "pending"),
            make_item("C", "pending"),
        ],
    )
    .await
    .unwrap();
    replace_todos(&db, &job_id, &[make_item("Only", "in_progress")])
        .await
        .unwrap();

    let loaded = get_todos_for_job(&db, &job_id).await.unwrap();
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].content, "Only");
}

#[tokio::test]
async fn replace_todos_with_empty_list_clears_all() {
    let (_temp, db) = common::migrated_db().await;
    let job_id = common::create_job(&db, None, None).await;

    replace_todos(&db, &job_id, &[make_item("X", "pending")])
        .await
        .unwrap();
    replace_todos(&db, &job_id, &[]).await.unwrap();

    let loaded = get_todos_for_job(&db, &job_id).await.unwrap();
    assert!(loaded.is_empty());
}

#[tokio::test]
async fn replace_todos_preserves_priority_and_active_form() {
    let (_temp, db) = common::migrated_db().await;
    let job_id = common::create_job(&db, None, None).await;

    let result = replace_todos(
        &db,
        &job_id,
        &[make_item_full(
            Some("deploy"),
            "Deploy to prod",
            "in_progress",
            Some("high"),
            Some("Deploying to prod"),
        )],
    )
    .await
    .unwrap();

    assert_eq!(result[0].priority.as_deref(), Some("high"));
    assert_eq!(result[0].active_form.as_deref(), Some("Deploying to prod"));

    let loaded = get_todos_for_job(&db, &job_id).await.unwrap();
    assert_eq!(loaded[0].priority.as_deref(), Some("high"));
    assert_eq!(loaded[0].active_form.as_deref(), Some("Deploying to prod"));
}

#[tokio::test]
async fn replace_todos_isolates_between_jobs() {
    let (_temp, db) = common::migrated_db().await;
    let job1 = common::create_job(&db, None, None).await;
    let job2 = common::create_job(&db, None, None).await;

    replace_todos(&db, &job1, &[make_item("Job1 task", "pending")])
        .await
        .unwrap();
    replace_todos(&db, &job2, &[make_item("Job2 task", "completed")])
        .await
        .unwrap();

    let todos1 = get_todos_for_job(&db, &job1).await.unwrap();
    let todos2 = get_todos_for_job(&db, &job2).await.unwrap();

    assert_eq!(todos1.len(), 1);
    assert_eq!(todos1[0].content, "Job1 task");
    assert_eq!(todos2.len(), 1);
    assert_eq!(todos2[0].content, "Job2 task");

    replace_todos(&db, &job1, &[]).await.unwrap();
    let todos2_after = get_todos_for_job(&db, &job2).await.unwrap();
    assert_eq!(todos2_after.len(), 1);
}

#[tokio::test]
async fn get_todos_for_job_returns_empty_when_none() {
    let (_temp, db) = common::migrated_db().await;
    let job_id = common::create_job(&db, None, None).await;

    let result = get_todos_for_job(&db, &job_id).await.unwrap();
    assert!(result.is_empty());
}

#[tokio::test]
async fn get_todos_for_job_preserves_position_order() {
    let (_temp, db) = common::migrated_db().await;
    let job_id = common::create_job(&db, None, None).await;

    let items = vec![
        make_item("First", "pending"),
        make_item("Second", "in_progress"),
        make_item("Third", "completed"),
    ];
    replace_todos(&db, &job_id, &items).await.unwrap();

    let loaded = get_todos_for_job(&db, &job_id).await.unwrap();
    assert_eq!(loaded[0].content, "First");
    assert_eq!(loaded[1].content, "Second");
    assert_eq!(loaded[2].content, "Third");
}

#[tokio::test]
async fn get_todos_for_children_collects_child_todos() {
    let (_temp, db) = common::migrated_db().await;
    let parent_id = common::create_job(&db, None, None).await;
    let child1 = common::create_job(&db, Some(&parent_id), Some(0)).await;
    let child2 = common::create_job(&db, Some(&parent_id), Some(1)).await;

    replace_todos(&db, &child1, &[make_item("Child1 task", "completed")])
        .await
        .unwrap();
    replace_todos(&db, &child2, &[make_item("Child2 task", "pending")])
        .await
        .unwrap();

    let results = get_todos_for_children(&db, &parent_id).await.unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0, child1);
    assert_eq!(results[0].1[0].content, "Child1 task");
    assert_eq!(results[1].0, child2);
    assert_eq!(results[1].1[0].content, "Child2 task");
}

#[tokio::test]
async fn get_todos_for_children_skips_empty_children() {
    let (_temp, db) = common::migrated_db().await;
    let parent_id = common::create_job(&db, None, None).await;
    let child1 = common::create_job(&db, Some(&parent_id), Some(0)).await;
    let _child2 = common::create_job(&db, Some(&parent_id), Some(1)).await;

    replace_todos(&db, &child1, &[make_item("Only child", "pending")])
        .await
        .unwrap();

    let results = get_todos_for_children(&db, &parent_id).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, child1);
}

#[tokio::test]
async fn get_todos_for_children_returns_empty_when_no_children() {
    let (_temp, db) = common::migrated_db().await;
    let parent_id = common::create_job(&db, None, None).await;

    let results = get_todos_for_children(&db, &parent_id).await.unwrap();
    assert!(results.is_empty());
}

#[tokio::test]
async fn finalize_todos_marks_in_progress_as_completed() {
    let (_temp, db) = common::migrated_db().await;
    let job_id = common::create_job(&db, None, None).await;

    replace_todos(
        &db,
        &job_id,
        &[
            make_item("Done", "completed"),
            make_item("Working", "in_progress"),
            make_item("Not started", "pending"),
        ],
    )
    .await
    .unwrap();

    finalize_todos(&db, &job_id).await.unwrap();

    let loaded = get_todos_for_job(&db, &job_id).await.unwrap();
    assert_eq!(loaded[0].status, "completed");
    assert_eq!(loaded[1].status, "completed");
    assert_eq!(loaded[2].status, "pending");
}

#[tokio::test]
async fn finalize_todos_noop_when_no_in_progress() {
    let (_temp, db) = common::migrated_db().await;
    let job_id = common::create_job(&db, None, None).await;

    replace_todos(
        &db,
        &job_id,
        &[
            make_item("Done", "completed"),
            make_item("Waiting", "pending"),
        ],
    )
    .await
    .unwrap();

    finalize_todos(&db, &job_id).await.unwrap();

    let loaded = get_todos_for_job(&db, &job_id).await.unwrap();
    assert_eq!(loaded[0].status, "completed");
    assert_eq!(loaded[1].status, "pending");
}

#[tokio::test]
async fn finalize_todos_noop_for_empty_list() {
    let (_temp, db) = common::migrated_db().await;
    let job_id = common::create_job(&db, None, None).await;

    finalize_todos(&db, &job_id).await.unwrap();
}

#[tokio::test]
async fn get_todo_progress_returns_none_when_empty() {
    let (_temp, db) = common::migrated_db().await;
    let job_id = common::create_job(&db, None, None).await;

    assert!(get_todo_progress(&db, &job_id).await.is_none());
}

#[tokio::test]
async fn get_todo_progress_formats_correctly() {
    let (_temp, db) = common::migrated_db().await;
    let job_id = common::create_job(&db, None, None).await;

    replace_todos(
        &db,
        &job_id,
        &[
            make_item("A", "completed"),
            make_item("B", "completed"),
            make_item("C", "in_progress"),
            make_item("D", "pending"),
            make_item("E", "pending"),
        ],
    )
    .await
    .unwrap();

    let progress = get_todo_progress(&db, &job_id).await.unwrap();
    assert_eq!(progress, "2/5 todos");
}

#[tokio::test]
async fn get_todo_progress_all_completed() {
    let (_temp, db) = common::migrated_db().await;
    let job_id = common::create_job(&db, None, None).await;

    replace_todos(
        &db,
        &job_id,
        &[make_item("A", "completed"), make_item("B", "completed")],
    )
    .await
    .unwrap();

    let progress = get_todo_progress(&db, &job_id).await.unwrap();
    assert_eq!(progress, "2/2 todos");
}

#[tokio::test]
async fn get_todo_progress_none_completed() {
    let (_temp, db) = common::migrated_db().await;
    let job_id = common::create_job(&db, None, None).await;

    replace_todos(
        &db,
        &job_id,
        &[make_item("A", "pending"), make_item("B", "in_progress")],
    )
    .await
    .unwrap();

    let progress = get_todo_progress(&db, &job_id).await.unwrap();
    assert_eq!(progress, "0/2 todos");
}

#[tokio::test]
async fn append_todos_continues_position_after_existing() {
    let (_temp, db) = common::migrated_db().await;
    let job_id = common::create_job(&db, None, None).await;

    replace_todos(
        &db,
        &job_id,
        &[
            make_item("First", "completed"),
            make_item("Second", "pending"),
        ],
    )
    .await
    .unwrap();

    let result = append_todos(
        &db,
        &job_id,
        &[
            make_item("Third", "pending"),
            make_item("Fourth", "pending"),
        ],
    )
    .await
    .unwrap();

    // Returns the full list in position order.
    assert_eq!(result.len(), 4);
    assert_eq!(result[0].content, "First");
    assert_eq!(result[1].content, "Second");
    assert_eq!(result[2].content, "Third");
    assert_eq!(result[3].content, "Fourth");
    // Default ids continue the 1-based position sequence rather than restarting.
    assert_eq!(result[2].id, "3");
    assert_eq!(result[3].id, "4");
}

#[tokio::test]
async fn compact_state_shows_id_content_and_status() {
    let (_temp, db) = common::migrated_db().await;
    let job_id = common::create_job(&db, None, None).await;

    let todos = replace_todos(
        &db,
        &job_id,
        &[
            make_item("First", "in_progress"),
            make_item("Second", "pending"),
        ],
    )
    .await
    .unwrap();

    assert_eq!(
        format_todos_compact(&todos),
        "[1] First - in_progress\n[2] Second - pending"
    );
}

#[test]
fn compact_state_of_empty_list() {
    assert_eq!(format_todos_compact(&[]), "(no todos)");
}

#[tokio::test]
async fn append_todos_into_empty_starts_at_one() {
    let (_temp, db) = common::migrated_db().await;
    let job_id = common::create_job(&db, None, None).await;

    let result = append_todos(&db, &job_id, &[make_item("Only", "pending")])
        .await
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].id, "1");
}

#[tokio::test]
async fn update_todos_patches_only_provided_fields_by_id() {
    let (_temp, db) = common::migrated_db().await;
    let job_id = common::create_job(&db, None, None).await;

    replace_todos(
        &db,
        &job_id,
        &[
            make_item_full(Some("a"), "Task A", "pending", Some("low"), None),
            make_item_full(Some("b"), "Task B", "pending", None, None),
        ],
    )
    .await
    .unwrap();

    let updated = update_todos(
        &db,
        &job_id,
        &[TodoUpdateItem {
            id: "a".to_string(),
            content: None,
            status: Some("completed".to_string()),
            priority: None,
            active_form: None,
        }],
    )
    .await
    .unwrap();

    // Only the status of "a" changed; content, priority, and "b" are untouched.
    assert_eq!(updated[0].id, "a");
    assert_eq!(updated[0].status, "completed");
    assert_eq!(updated[0].content, "Task A");
    assert_eq!(updated[0].priority.as_deref(), Some("low"));
    assert_eq!(updated[1].id, "b");
    assert_eq!(updated[1].status, "pending");
}

#[tokio::test]
async fn update_todos_errors_on_unknown_id() {
    let (_temp, db) = common::migrated_db().await;
    let job_id = common::create_job(&db, None, None).await;

    replace_todos(
        &db,
        &job_id,
        &[make_item_full(Some("a"), "Task A", "pending", None, None)],
    )
    .await
    .unwrap();

    let err = update_todos(
        &db,
        &job_id,
        &[TodoUpdateItem {
            id: "missing".to_string(),
            content: None,
            status: Some("completed".to_string()),
            priority: None,
            active_form: None,
        }],
    )
    .await
    .unwrap_err();
    assert!(
        err.contains("missing"),
        "error should name the unknown id: {err}"
    );
}

#[tokio::test]
async fn todo_item_from_db_uses_todo_id_not_internal_id() {
    let (_temp, db) = common::migrated_db().await;
    let job_id = common::create_job(&db, None, None).await;

    replace_todos(
        &db,
        &job_id,
        &[make_item_full(
            Some("my-custom-id"),
            "Custom task",
            "pending",
            Some("high"),
            Some("Working on custom task"),
        )],
    )
    .await
    .unwrap();

    let loaded = get_todos_for_job(&db, &job_id).await.unwrap();
    assert_eq!(loaded[0].id, "my-custom-id");
    assert_eq!(loaded[0].content, "Custom task");
    assert_eq!(loaded[0].status, "pending");
    assert_eq!(loaded[0].priority.as_deref(), Some("high"));
    assert_eq!(
        loaded[0].active_form.as_deref(),
        Some("Working on custom task")
    );
}
