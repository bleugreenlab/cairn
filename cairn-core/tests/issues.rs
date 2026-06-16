mod common;

use cairn_core::internal::services::Clock;
use cairn_core::internal::storage::{LocalDb, RowExt};
use cairn_core::issues::{comments, crud};
use cairn_core::models::{
    CommentSource, CreateComment, CreateIssue, Issue, IssueProgress, IssueStatus, UpdateIssue,
};
use cairn_core::transitions::Resolution;
use turso::params;

struct FixedClock(i64);

impl Clock for FixedClock {
    fn now(&self) -> i64 {
        self.0
    }

    fn now_u64(&self) -> u64 {
        self.0 as u64
    }
}

fn create_issue_input(project_id: &str, title: &str) -> CreateIssue {
    CreateIssue {
        project_id: project_id.to_string(),
        title: title.to_string(),
        description: Some("description".to_string()),
        backend_override: None,
        label_ids: None,
    }
}

async fn project_fixture(key: &str) -> (tempfile::TempDir, LocalDb, String) {
    let (temp, db) = common::migrated_db().await;
    let project_id = common::create_project(&db, key).await;
    (temp, db, project_id)
}

async fn create_issue_at(db: &LocalDb, project_id: &str, title: &str, now: i64) -> Issue {
    crud::create(db, &FixedClock(now), create_issue_input(project_id, title))
        .await
        .unwrap()
}

async fn count_where(db: &LocalDb, sql: &'static str, id: &str) -> i64 {
    let id = id.to_string();
    db.read(|conn| {
        let id = id.clone();
        Box::pin(async move {
            let mut rows = conn.query(sql, params![id.as_str()]).await?;
            rows.next()
                .await?
                .map(|row| row.i64(0))
                .transpose()
                .map(|value| value.unwrap_or(0))
        })
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn create_list_and_get_issues_use_project_numbering() {
    let (_temp, db, project_id) = project_fixture("ISS").await;

    let first = create_issue_at(&db, &project_id, "First", 100).await;
    let second = create_issue_at(&db, &project_id, "Second", 200).await;

    assert_eq!(first.number, 1);
    assert_eq!(first.created_at, 100);
    assert_eq!(second.number, 2);

    let loaded = crud::get(&db, &first.id).await.unwrap().unwrap();
    assert_eq!(loaded.title, "First");
    assert_eq!(loaded.status, IssueStatus::Backlog);

    let issues = crud::list(&db, &project_id).await.unwrap();
    assert_eq!(
        issues
            .iter()
            .map(|issue| issue.id.as_str())
            .collect::<Vec<_>>(),
        vec![second.id.as_str(), first.id.as_str()]
    );
}

#[tokio::test]
async fn update_dismiss_restore_and_backend_override_round_trip() {
    let (_temp, db, project_id) = project_fixture("IUP").await;
    let issue = create_issue_at(&db, &project_id, "Original", 100).await;

    let updated = crud::update(
        &db,
        &FixedClock(200),
        UpdateIssue {
            id: issue.id.clone(),
            title: Some("Updated".to_string()),
            description: Some("new description".to_string()),
            backend_override: Some(Some("gpt-5.3-codex".to_string())),
            depends_on: None,
            label_ids: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(updated.title, "Updated");
    assert_eq!(updated.description, "new description");
    assert_eq!(updated.backend_override.as_deref(), Some("gpt-5.3-codex"));
    assert_eq!(updated.updated_at, 200);

    let cleared = crud::update(
        &db,
        &FixedClock(300),
        UpdateIssue {
            id: issue.id.clone(),
            title: None,
            description: None,
            backend_override: Some(None),
            depends_on: None,
            label_ids: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(cleared.title, "Updated");
    assert_eq!(cleared.backend_override, None);

    crud::dismiss(&db, &FixedClock(400), &issue.id)
        .await
        .unwrap();
    assert_eq!(
        crud::get(&db, &issue.id)
            .await
            .unwrap()
            .unwrap()
            .dismissed_at,
        Some(400)
    );

    crud::restore(&db, &FixedClock(500), &issue.id)
        .await
        .unwrap();
    assert_eq!(
        crud::get(&db, &issue.id)
            .await
            .unwrap()
            .unwrap()
            .dismissed_at,
        None
    );
}

#[tokio::test]
async fn resolve_and_unresolve_update_issue_and_close_open_sessions() {
    let (_temp, db, project_id) = project_fixture("RES").await;
    let issue = create_issue_at(&db, &project_id, "Issue", 100).await;

    db.write(|conn| {
        let project_id = project_id.clone();
        let issue_id = issue.id.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO jobs(id, project_id, issue_id, status, created_at, updated_at)
                 VALUES ('job-1', ?1, ?2, 'running', 1, 1)",
                params![project_id.as_str(), issue_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
                 VALUES ('session-1', 'job-1', 'codex', 'open', 1, 1, 1)",
                (),
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();

    let closed = crud::resolve(&db, &FixedClock(600), &issue.id, Resolution::Closed)
        .await
        .unwrap();
    assert_eq!(closed, vec!["session-1".to_string()]);

    let closed_issue = crud::get(&db, &issue.id).await.unwrap().unwrap();
    assert_eq!(closed_issue.status, IssueStatus::Closed);
    assert_eq!(closed_issue.progress, IssueProgress::Closed);
    assert_eq!(closed_issue.closed_at, Some(600));

    let session_status = count_where(
        &db,
        "SELECT COUNT(*) FROM sessions WHERE id = ?1 AND status = 'closed' AND terminal_reason = 'issue_closed'",
        "session-1",
    )
    .await;
    assert_eq!(session_status, 1);

    crud::unresolve(&db, &FixedClock(700), &issue.id)
        .await
        .unwrap();
    let reopened = crud::get(&db, &issue.id).await.unwrap().unwrap();
    assert_eq!(reopened.closed_at, None);
    assert_eq!(reopened.completed_at, None);
}

#[tokio::test]
async fn comments_create_list_update_and_delete() {
    let (_temp, db, project_id) = project_fixture("COM").await;
    let issue = create_issue_at(&db, &project_id, "Issue", 100).await;

    let first = comments::create(
        &db,
        &FixedClock(200),
        CreateComment {
            issue_id: issue.id.clone(),
            content: "first".to_string(),
            source: CommentSource::User,
        },
    )
    .await
    .unwrap();
    let second = comments::create(
        &db,
        &FixedClock(300),
        CreateComment {
            issue_id: issue.id.clone(),
            content: "second".to_string(),
            source: CommentSource::Agent,
        },
    )
    .await
    .unwrap();

    let listed = comments::list(&db, &issue.id).await.unwrap();
    assert_eq!(
        listed
            .iter()
            .map(|comment| comment.id.as_str())
            .collect::<Vec<_>>(),
        vec![first.id.as_str(), second.id.as_str()]
    );

    let updated = comments::update(&db, &first.id, "edited").await.unwrap();
    assert_eq!(updated.content, "edited");
    assert_eq!(updated.source, CommentSource::User);

    comments::delete(&db, &first.id).await.unwrap();
    let remaining = comments::list(&db, &issue.id).await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].id, second.id);
}

#[tokio::test]
async fn delete_db_removes_issue_owned_records() {
    let (_temp, db, project_id) = project_fixture("DEL").await;
    let issue = create_issue_at(&db, &project_id, "Issue", 100).await;

    db.write(|conn| {
        let project_id = project_id.clone();
        let issue_id = issue.id.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO jobs(id, project_id, issue_id, status, created_at, updated_at)
                 VALUES ('job-1', ?1, ?2, 'running', 1, 1)",
                params![project_id.as_str(), issue_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO runs(id, project_id, issue_id, job_id, status, created_at, updated_at)
                 VALUES ('run-1', ?1, ?2, 'job-1', 'live', 1, 1)",
                params![project_id.as_str(), issue_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO events(id, run_id, sequence, timestamp, event_type, data, created_at)
                 VALUES ('event-1', 'run-1', 1, 1, 'assistant', '{}', 1)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO prompts(id, run_id, questions, created_at)
                 VALUES ('prompt-1', 'run-1', '[]', 1)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO comments(id, issue_id, content, source, created_at)
                 VALUES ('comment-1', ?1, 'comment', 'user', 1)",
                params![issue_id.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();

    crud::delete_db(&db, &issue.id).await.unwrap();
    assert_eq!(
        count_where(&db, "SELECT COUNT(*) FROM issues WHERE id = ?1", &issue.id).await,
        0
    );
    assert_eq!(
        count_where(
            &db,
            "SELECT COUNT(*) FROM jobs WHERE issue_id = ?1",
            &issue.id
        )
        .await,
        0
    );
    assert_eq!(
        count_where(
            &db,
            "SELECT COUNT(*) FROM runs WHERE issue_id = ?1",
            &issue.id
        )
        .await,
        0
    );
    assert_eq!(
        count_where(
            &db,
            "SELECT COUNT(*) FROM comments WHERE issue_id = ?1",
            &issue.id
        )
        .await,
        0
    );
}

#[tokio::test]
async fn update_dependencies_replaces_and_hydrates_issue_models() {
    let (_temp, db, project_id) = project_fixture("DEP").await;
    let blocker = create_issue_at(&db, &project_id, "Blocker", 100).await;
    let dependent = create_issue_at(&db, &project_id, "Dependent", 200).await;

    let updated = crud::update(
        &db,
        &FixedClock(300),
        UpdateIssue {
            id: dependent.id.clone(),
            title: None,
            description: None,
            backend_override: None,
            depends_on: Some(vec![format!("cairn://p/DEP/{}", blocker.number)]),
            label_ids: None,
        },
    )
    .await
    .unwrap();

    assert_eq!(updated.depends_on, vec!["cairn://p/DEP/1"]);
    assert_eq!(updated.updated_at, 300);
    let loaded = crud::get(&db, &dependent.id).await.unwrap().unwrap();
    assert_eq!(loaded.depends_on, vec!["cairn://p/DEP/1"]);
    let listed = crud::list(&db, &project_id).await.unwrap();
    assert_eq!(
        listed
            .iter()
            .find(|issue| issue.id == dependent.id)
            .unwrap()
            .depends_on,
        vec!["cairn://p/DEP/1"]
    );

    let cleared = crud::update(
        &db,
        &FixedClock(400),
        UpdateIssue {
            id: dependent.id.clone(),
            title: None,
            description: None,
            backend_override: None,
            depends_on: Some(Vec::new()),
            label_ids: None,
        },
    )
    .await
    .unwrap();

    assert!(cleared.depends_on.is_empty());
    assert!(crud::get(&db, &dependent.id)
        .await
        .unwrap()
        .unwrap()
        .depends_on
        .is_empty());
}

#[tokio::test]
async fn failed_dependency_update_keeps_existing_dependencies() {
    let (_temp, db, project_id) = project_fixture("BAD").await;
    let blocker = create_issue_at(&db, &project_id, "Blocker", 100).await;
    let dependent = create_issue_at(&db, &project_id, "Dependent", 200).await;

    crud::update(
        &db,
        &FixedClock(300),
        UpdateIssue {
            id: dependent.id.clone(),
            title: None,
            description: None,
            backend_override: None,
            depends_on: Some(vec![format!("cairn://p/BAD/{}", blocker.number)]),
            label_ids: None,
        },
    )
    .await
    .unwrap();

    let error = crud::update(
        &db,
        &FixedClock(400),
        UpdateIssue {
            id: dependent.id.clone(),
            title: None,
            description: None,
            backend_override: None,
            depends_on: Some(vec!["not-an-issue-uri".to_string()]),
            label_ids: None,
        },
    )
    .await
    .unwrap_err()
    .to_string();

    assert!(error.contains("dependency URI must be a canonical issue URI"));
    assert_eq!(
        crud::get(&db, &dependent.id)
            .await
            .unwrap()
            .unwrap()
            .depends_on,
        vec!["cairn://p/BAD/1"]
    );
}
