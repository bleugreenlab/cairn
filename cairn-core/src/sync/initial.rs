//! Initial sync — dump all local state to cloud on first connection or resync.

use crate::db::DbState;
use crate::issues::relations;
use crate::storage::{DbError, LocalDb, RowExt};

use super::message::*;
use super::sender::SyncSender;

/// Statistics from an initial sync operation.
#[derive(Debug, Default)]
pub struct SyncStats {
    pub projects: usize,
    pub issues: usize,
    pub jobs: usize,
    pub runs: usize,
    pub comments: usize,
    pub artifacts: usize,
}

impl std::fmt::Display for SyncStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "projects={}, issues={}, jobs={}, runs={}, comments={}, artifacts={}",
            self.projects, self.issues, self.jobs, self.runs, self.comments, self.artifacts
        )
    }
}

/// Perform initial sync: dump all local state to cloud.
/// Runs on first device connection or when cloud requests resync.
///
/// Sends supported non-event entities through the SyncSender channel.
pub async fn initial_sync(db: &DbState, sender: &SyncSender) -> Result<SyncStats, String> {
    let mut stats = SyncStats::default();

    // Sync projects (directly from Db model — no filesystem access needed)
    {
        let rows = load_projects(&db.local).await?;
        stats.projects = rows.len();
        for row in rows {
            sender.send(SyncMessage::Project(row));
        }
    }

    // Sync issues
    {
        let rows = load_issues(&db.local).await?;
        stats.issues = rows.len();
        for row in rows {
            sender.send(SyncMessage::Issue(row));
        }
    }

    // Sync jobs
    {
        let rows = load_jobs(&db.local).await?;
        stats.jobs = rows.len();
        for row in rows {
            sender.send(SyncMessage::Job(row));
        }
    }

    // Sync runs
    {
        let rows = load_runs(&db.local).await?;
        stats.runs = rows.len();
        for row in rows {
            sender.send(SyncMessage::Run(row));
        }
    }

    // Sync comments
    {
        let rows = load_comments(&db.local).await?;
        stats.comments = rows.len();
        for row in rows {
            sender.send(SyncMessage::Comment(row));
        }
    }

    // Sync artifacts
    {
        let rows = load_artifacts(&db.local).await?;
        stats.artifacts = rows.len();
        for row in rows {
            sender.send(SyncMessage::Artifact(row));
        }
    }

    log::info!("Initial sync complete: {}", stats);
    Ok(stats)
}

async fn load_projects(db: &LocalDb) -> Result<Vec<SyncProject>, String> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, key, name, repo_path, created_at, updated_at
                     FROM projects
                     WHERE COALESCE(is_workspace, 0) = 0
                     ORDER BY created_at ASC, id ASC",
                    (),
                )
                .await?;
            let mut projects = Vec::new();
            while let Some(row) = rows.next().await? {
                projects.push(SyncProject {
                    id: row.text(0)?,
                    key: row.text(1)?,
                    name: row.text(2)?,
                    path: Some(row.text(3)?),
                    created_at: Some(row.i64(4)?),
                    updated_at: Some(row.i64(5)?),
                });
            }
            Ok(projects)
        })
    })
    .await
    .map_err(|error| format!("Failed to load projects: {error}"))
}

async fn load_issues(db: &LocalDb) -> Result<Vec<SyncIssue>, String> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT i.id, i.project_id, i.number, i.title, i.description, i.status, i.priority,
                            i.model, i.created_at, i.updated_at, i.completed_at,
                            i.merged_at, i.closed_at, i.parent_issue_id
                     FROM issues i
                     INNER JOIN projects p ON p.id = i.project_id
                     WHERE COALESCE(p.is_workspace, 0) = 0
                     ORDER BY i.created_at ASC, i.id ASC",
                    (),
                )
                .await?;
            let mut issues = Vec::new();
            while let Some(row) = rows.next().await? {
                issues.push(SyncIssue {
                    id: row.text(0)?,
                    project_id: row.text(1)?,
                    number: required_i32(row.i64(2)?, "issues.number")?,
                    title: row.text(3)?,
                    description: row.opt_text(4)?,
                    status: row.text(5)?,
                    priority: optional_i32(row.opt_i64(6)?, "issues.priority")?.unwrap_or(0),
                    model: row.opt_text(7)?,
                    created_at: Some(row.i64(8)?),
                    updated_at: Some(row.i64(9)?),
                    completed_at: row.opt_i64(10)?,
                    merged_at: row.opt_i64(11)?,
                    closed_at: row.opt_i64(12)?,
                    parent_issue_id: row.opt_text(13)?,
                    depends_on: relations::list_dependency_uris(conn, &row.text(0)?)
                        .await
                        .unwrap_or_default(),
                });
            }
            Ok(issues)
        })
    })
    .await
    .map_err(|error| format!("Failed to load issues: {error}"))
}

async fn load_jobs(db: &LocalDb) -> Result<Vec<SyncJob>, String> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT j.id, j.issue_id, j.project_id, j.execution_id, j.node_name,
                            j.task_description, j.status, j.model, j.branch, j.created_at,
                            j.updated_at, j.started_at, j.completed_at
                     FROM jobs j
                     LEFT JOIN projects project_ref ON project_ref.id = j.project_id
                     LEFT JOIN issues i ON i.id = j.issue_id
                     LEFT JOIN projects issue_project ON issue_project.id = i.project_id
                     WHERE COALESCE(project_ref.is_workspace, issue_project.is_workspace, 0) = 0
                     ORDER BY j.created_at ASC, j.id ASC",
                    (),
                )
                .await?;
            let mut jobs = Vec::new();
            while let Some(row) = rows.next().await? {
                jobs.push(SyncJob {
                    id: row.text(0)?,
                    issue_id: row.opt_text(1)?,
                    project_id: row.opt_text(2)?,
                    execution_id: row.opt_text(3)?,
                    node_name: row.opt_text(4)?,
                    task_description: row.opt_text(5)?,
                    status: row.opt_text(6)?,
                    model: row.opt_text(7)?,
                    branch: row.opt_text(8)?,
                    created_at: Some(row.i64(9)?),
                    updated_at: Some(row.i64(10)?),
                    started_at: row.opt_i64(11)?,
                    completed_at: row.opt_i64(12)?,
                });
            }
            Ok(jobs)
        })
    })
    .await
    .map_err(|error| format!("Failed to load jobs: {error}"))
}

async fn load_runs(db: &LocalDb) -> Result<Vec<SyncRun>, String> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT r.id, r.job_id, r.issue_id, r.status, r.backend, r.exit_reason,
                            r.error_message, r.started_at, r.exited_at, r.created_at
                     FROM runs r
                     LEFT JOIN projects run_project ON run_project.id = r.project_id
                     LEFT JOIN jobs j ON j.id = r.job_id
                     LEFT JOIN projects job_project ON job_project.id = j.project_id
                     LEFT JOIN issues i ON i.id = COALESCE(r.issue_id, j.issue_id)
                     LEFT JOIN projects issue_project ON issue_project.id = i.project_id
                     WHERE COALESCE(run_project.is_workspace, job_project.is_workspace, issue_project.is_workspace, 0) = 0
                     ORDER BY r.created_at ASC, r.id ASC",
                    (),
                )
                .await?;
            let mut runs = Vec::new();
            while let Some(row) = rows.next().await? {
                runs.push(SyncRun {
                    id: row.text(0)?,
                    job_id: row.opt_text(1)?,
                    issue_id: row.opt_text(2)?,
                    status: row.opt_text(3)?,
                    backend: row.opt_text(4)?,
                    exit_reason: row.opt_text(5)?,
                    error_message: row.opt_text(6)?,
                    started_at: row.opt_i64(7)?,
                    exited_at: row.opt_i64(8)?,
                    created_at: Some(row.i64(9)?),
                });
            }
            Ok(runs)
        })
    })
    .await
    .map_err(|error| format!("Failed to load runs: {error}"))
}

async fn load_comments(db: &LocalDb) -> Result<Vec<SyncComment>, String> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT c.id, c.issue_id, c.content, c.source, c.created_at
                     FROM comments c
                     INNER JOIN issues i ON i.id = c.issue_id
                     INNER JOIN projects p ON p.id = i.project_id
                     WHERE COALESCE(p.is_workspace, 0) = 0
                     ORDER BY c.created_at ASC, c.id ASC",
                    (),
                )
                .await?;
            let mut comments = Vec::new();
            while let Some(row) = rows.next().await? {
                comments.push(SyncComment {
                    id: row.text(0)?,
                    issue_id: row.text(1)?,
                    content: row.text(2)?,
                    source: Some(row.text(3)?),
                    created_at: Some(row.i64(4)?),
                });
            }
            Ok(comments)
        })
    })
    .await
    .map_err(|error| format!("Failed to load comments: {error}"))
}

async fn load_artifacts(db: &LocalDb) -> Result<Vec<SyncArtifact>, String> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT a.id, a.job_id, a.data, a.version, a.updated_at
                     FROM artifacts a
                     LEFT JOIN jobs j ON j.id = a.job_id
                     LEFT JOIN projects job_project ON job_project.id = j.project_id
                     LEFT JOIN issues i ON i.id = j.issue_id
                     LEFT JOIN projects issue_project ON issue_project.id = i.project_id
                     WHERE COALESCE(job_project.is_workspace, issue_project.is_workspace, 0) = 0
                     ORDER BY a.created_at ASC, a.id ASC",
                    (),
                )
                .await?;
            let mut artifacts = Vec::new();
            while let Some(row) = rows.next().await? {
                artifacts.push(SyncArtifact {
                    id: row.text(0)?,
                    job_id: row.opt_text(1)?,
                    data: Some(row.text(2)?),
                    version: optional_i32(row.opt_i64(3)?, "artifacts.version")?,
                    updated_at: Some(row.i64(4)?),
                });
            }
            Ok(artifacts)
        })
    })
    .await
    .map_err(|error| format!("Failed to load artifacts: {error}"))
}

fn required_i32(value: i64, column: &str) -> Result<i32, DbError> {
    i32::try_from(value)
        .map_err(|_| DbError::Row(format!("{column} value {value} is outside i32 range")))
}

fn optional_i32(value: Option<i64>, column: &str) -> Result<Option<i32>, DbError> {
    value.map(|value| required_i32(value, column)).transpose()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::sync::mpsc;

    use crate::db::DbState;
    use crate::storage::{MigrationRunner, SearchIndex, TURSO_MIGRATIONS};

    struct TestDb {
        db: DbState,
        _temp_dir: TempDir,
    }

    async fn test_db() -> TestDb {
        let temp_dir = tempfile::tempdir().unwrap();
        let local = Arc::new(
            LocalDb::open(temp_dir.path().join("cairn.db"))
                .await
                .unwrap(),
        );
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&local)
            .await
            .unwrap();
        let search_index =
            Arc::new(SearchIndex::open_or_create(temp_dir.path().join("search")).unwrap());
        TestDb {
            db: DbState::new(local, search_index),
            _temp_dir: temp_dir,
        }
    }

    async fn insert_sync_fixture(db: &LocalDb, include_event: bool) {
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT OR IGNORE INTO workspaces(id, name, created_at, updated_at)
                     VALUES ('default', 'Default', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
                     VALUES ('project-1', 'default', 'Project', 'PROJ', '/tmp/project', 10, 11)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO issues(
                        id, project_id, number, title, description, status, priority,
                        model, created_at, updated_at, completed_at,
                        merged_at, closed_at
                     )
                     VALUES (
                        'issue-1', 'project-1', 7, 'Issue 1', 'Issue description',
                        'in_progress', 2, 'claude-sonnet', 20, 21, NULL, NULL, NULL
                     )",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO jobs(
                        id, issue_id, project_id, task_description, status, model, branch,
                        node_name, created_at, updated_at, started_at, completed_at, uri_segment
                     )
                     VALUES (
                        'job-1', 'issue-1', 'project-1', 'Do the work', 'running',
                        'claude-sonnet', 'feature/turso', 'builder', 30, 31, 32, NULL, 'builder'
                     )",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO runs(
                        id, issue_id, project_id, job_id, status, backend, exit_reason,
                        error_message, started_at, exited_at, created_at, updated_at
                     )
                     VALUES (
                        'run-1', 'issue-1', 'project-1', 'job-1', 'running', 'claude',
                        NULL, NULL, 40, NULL, 41, 42
                     )",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO comments(id, issue_id, content, source, created_at)
                     VALUES ('comment-1', 'issue-1', 'Test comment', 'user', 50)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO artifacts(
                        id, job_id, artifact_type, data, version, created_at, updated_at
                     )
                     VALUES ('artifact-1', 'job-1', 'text', '{\"ok\":true}', 3, 60, 61)",
                    (),
                )
                .await?;

                conn.execute(
                    "INSERT INTO projects(
                        id, workspace_id, name, key, repo_path, created_at, updated_at, hidden, is_workspace
                     )
                     VALUES ('workspace', 'default', 'Workspace', 'CANON', '/tmp/cairn-home', 12, 13, 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
                     VALUES ('workspace-issue', 'workspace', 1, 'Workspace Issue', 'open', 22, 23)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO jobs(
                        id, issue_id, project_id, task_description, status, node_name, created_at, updated_at, uri_segment
                     )
                     VALUES (
                        'workspace-job', 'workspace-issue', 'workspace', 'Workspace work',
                        'running', 'workspace-builder', 34, 35, 'workspace-builder'
                     )",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO runs(id, issue_id, project_id, job_id, status, created_at, updated_at)
                     VALUES ('workspace-run', 'workspace-issue', 'workspace', 'workspace-job', 'running', 44, 45)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO comments(id, issue_id, content, source, created_at)
                     VALUES ('workspace-comment', 'workspace-issue', 'Workspace comment', 'user', 52)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO artifacts(id, job_id, artifact_type, data, version, created_at, updated_at)
                     VALUES ('workspace-artifact', 'workspace-job', 'text', '{}', 1, 62, 63)",
                    (),
                )
                .await?;

                if include_event {
                    conn.execute(
                        "INSERT INTO events(
                            id, run_id, session_id, sequence, timestamp, event_type, data,
                            created_at, input_tokens, cache_read_tokens, output_tokens, turn_id
                         )
                         VALUES (
                            'event-1', 'run-1', 'session-1', 1, 70, 'user',
                            '{\"type\":\"user\",\"content\":\"hello\"}', 71,
                            11, 12, 13, NULL
                         )",
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

    #[tokio::test]
    async fn initial_sync_empty_db() {
        let test = test_db().await;
        let (tx, mut rx) = mpsc::unbounded_channel();
        let sender = SyncSender::new(tx);

        let stats = initial_sync(&test.db, &sender).await.unwrap();
        assert_eq!(stats.projects, 0);
        assert_eq!(stats.issues, 0);
        assert_eq!(stats.jobs, 0);
        assert_eq!(stats.runs, 0);
        assert_eq!(stats.comments, 0);
        assert_eq!(stats.artifacts, 0);

        // No messages should have been sent
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn initial_sync_sends_all_non_event_entities() {
        let test = test_db().await;
        let (tx, mut rx) = mpsc::unbounded_channel();
        let sender = SyncSender::new(tx);

        insert_sync_fixture(&test.db.local, true).await;

        let stats = initial_sync(&test.db, &sender).await.unwrap();
        assert_eq!(stats.projects, 1);
        assert_eq!(stats.issues, 1);
        assert_eq!(stats.jobs, 1);
        assert_eq!(stats.runs, 1);
        assert_eq!(stats.comments, 1);
        assert_eq!(stats.artifacts, 1);

        let mut messages = Vec::new();
        while let Ok(message) = rx.try_recv() {
            messages.push(message);
        }
        assert_eq!(messages.len(), 6);

        let project = messages
            .iter()
            .find_map(|message| match message {
                SyncMessage::Project(project) => Some(project),
                _ => None,
            })
            .unwrap();
        assert_eq!(project.id, "project-1");
        assert_eq!(project.path.as_deref(), Some("/tmp/project"));

        let issue = messages
            .iter()
            .find_map(|message| match message {
                SyncMessage::Issue(issue) => Some(issue),
                _ => None,
            })
            .unwrap();
        assert_eq!(issue.number, 7);
        assert_eq!(issue.priority, 2);
        assert_eq!(issue.model.as_deref(), Some("claude-sonnet"));

        let job = messages
            .iter()
            .find_map(|message| match message {
                SyncMessage::Job(job) => Some(job),
                _ => None,
            })
            .unwrap();
        assert_eq!(job.project_id.as_deref(), Some("project-1"));
        assert_eq!(job.issue_id.as_deref(), Some("issue-1"));
        assert_eq!(job.node_name.as_deref(), Some("builder"));

        let run = messages
            .iter()
            .find_map(|message| match message {
                SyncMessage::Run(run) => Some(run),
                _ => None,
            })
            .unwrap();
        assert_eq!(run.job_id.as_deref(), Some("job-1"));
        assert_eq!(run.backend.as_deref(), Some("claude"));

        let comment = messages
            .iter()
            .find_map(|message| match message {
                SyncMessage::Comment(comment) => Some(comment),
                _ => None,
            })
            .unwrap();
        assert_eq!(comment.source.as_deref(), Some("user"));

        let artifact = messages
            .iter()
            .find_map(|message| match message {
                SyncMessage::Artifact(artifact) => Some(artifact),
                _ => None,
            })
            .unwrap();
        assert_eq!(artifact.version, Some(3));
        assert_eq!(artifact.data.as_deref(), Some("{\"ok\":true}"));
        assert!(!messages.iter().any(|message| match message {
            SyncMessage::Project(project) => project.id == "workspace",
            SyncMessage::Issue(issue) => issue.project_id == "workspace",
            SyncMessage::Job(job) => job.project_id.as_deref() == Some("workspace"),
            SyncMessage::Run(run) => run.job_id.as_deref() == Some("workspace-job"),
            SyncMessage::Comment(comment) => comment.issue_id == "workspace-issue",
            SyncMessage::Artifact(artifact) => artifact.job_id.as_deref() == Some("workspace-job"),
            _ => false,
        }));
        assert!(!messages
            .iter()
            .any(|message| matches!(message, SyncMessage::Event(_))));
    }

    #[test]
    fn sync_stats_display() {
        let stats = SyncStats {
            projects: 3,
            issues: 10,
            jobs: 5,
            runs: 20,
            comments: 7,
            artifacts: 2,
        };
        let s = format!("{}", stats);
        assert!(s.contains("projects=3"));
        assert!(s.contains("issues=10"));
        assert!(s.contains("artifacts=2"));
    }
}
