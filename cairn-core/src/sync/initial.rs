//! Initial sync — dump all local state to cloud on first connection or resync.

use crate::db::DbState;

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
    pub events: usize,
}

impl std::fmt::Display for SyncStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "projects={}, issues={}, jobs={}, runs={}, comments={}, artifacts={}, events={}",
            self.projects,
            self.issues,
            self.jobs,
            self.runs,
            self.comments,
            self.artifacts,
            self.events
        )
    }
}

/// Perform initial sync: dump all local state to cloud.
/// Runs on first device connection or when cloud requests resync.
///
/// Sends entities through the SyncSender channel (which batches and sends).
/// Skips events (too large) — only recent events would be synced separately.
pub fn initial_sync(db: &DbState, sender: &SyncSender) -> Result<SyncStats, String> {
    use crate::schema::*;
    use diesel::prelude::*;

    let mut conn = db
        .conn
        .lock()
        .map_err(|e| format!("Failed to lock DB: {}", e))?;
    let mut stats = SyncStats::default();

    // Sync projects (directly from Db model — no filesystem access needed)
    {
        let rows: Vec<crate::diesel_models::DbProject> = projects::table
            .load(&mut *conn)
            .map_err(|e| format!("Failed to load projects: {}", e))?;

        for row in &rows {
            sender.send(SyncMessage::Project(SyncProject {
                id: row.id.clone(),
                key: row.key.clone(),
                name: row.name.clone(),
                path: Some(row.repo_path.clone()),
                created_at: Some(row.created_at as i64),
                updated_at: Some(row.updated_at as i64),
            }));
        }
        stats.projects = rows.len();
    }

    // Sync issues
    {
        let rows: Vec<crate::diesel_models::DbIssue> = issues::table
            .load(&mut *conn)
            .map_err(|e| format!("Failed to load issues: {}", e))?;

        for row in &rows {
            sender.send(SyncMessage::Issue(SyncIssue {
                id: row.id.clone(),
                project_id: row.project_id.clone(),
                number: row.number,
                title: row.title.clone(),
                description: row.description.clone(),
                status: row.status.clone(),
                priority: row.priority.unwrap_or(0),
                model: row.model.clone(),
                manager_id: row.manager_id.clone(),
                created_at: Some(row.created_at as i64),
                updated_at: Some(row.updated_at as i64),
                completed_at: row.completed_at.map(|t| t as i64),
                merged_at: row.merged_at.map(|t| t as i64),
                closed_at: row.closed_at.map(|t| t as i64),
            }));
        }
        stats.issues = rows.len();
    }

    // Sync jobs
    {
        let rows: Vec<crate::diesel_models::DbJob> = jobs::table
            .load(&mut *conn)
            .map_err(|e| format!("Failed to load jobs: {}", e))?;

        for row in &rows {
            sender.send(SyncMessage::Job(SyncJob {
                id: row.id.clone(),
                issue_id: row.issue_id.clone(),
                project_id: Some(row.project_id.clone()),
                execution_id: row.execution_id.clone(),
                node_name: row.node_name.clone(),
                task_description: row.task_description.clone(),
                status: Some(row.status.clone()),
                model: row.model.clone(),
                branch: row.branch.clone(),
                created_at: Some(row.created_at as i64),
                updated_at: Some(row.updated_at as i64),
                started_at: row.started_at.map(|t| t as i64),
                completed_at: row.completed_at.map(|t| t as i64),
            }));
        }
        stats.jobs = rows.len();
    }

    // Sync runs
    {
        let rows: Vec<crate::diesel_models::DbRun> = runs::table
            .load(&mut *conn)
            .map_err(|e| format!("Failed to load runs: {}", e))?;

        for row in &rows {
            sender.send(SyncMessage::Run(SyncRun {
                id: row.id.clone(),
                job_id: row.job_id.clone(),
                issue_id: row.issue_id.clone(),
                status: row.status.clone(),
                backend: row.backend.clone(),
                exit_reason: row.exit_reason.clone(),
                error_message: row.error_message.clone(),
                started_at: row.started_at.map(|t| t as i64),
                exited_at: row.exited_at.map(|t| t as i64),
                created_at: Some(row.created_at as i64),
            }));
        }
        stats.runs = rows.len();
    }

    // Sync comments
    {
        let rows: Vec<crate::diesel_models::DbComment> = comments::table
            .load(&mut *conn)
            .map_err(|e| format!("Failed to load comments: {}", e))?;

        for row in &rows {
            sender.send(SyncMessage::Comment(SyncComment {
                id: row.id.clone(),
                issue_id: row.issue_id.clone(),
                content: row.content.clone(),
                source: Some(row.source.clone()),
                created_at: Some(row.created_at as i64),
            }));
        }
        stats.comments = rows.len();
    }

    // Sync artifacts
    {
        let rows: Vec<crate::diesel_models::DbArtifact> = artifacts::table
            .load(&mut *conn)
            .map_err(|e| format!("Failed to load artifacts: {}", e))?;

        for row in &rows {
            sender.send(SyncMessage::Artifact(SyncArtifact {
                id: row.id.clone(),
                job_id: row.job_id.clone(),
                data: Some(row.data.clone()),
                version: Some(row.version),
                updated_at: Some(row.updated_at as i64),
            }));
        }
        stats.artifacts = rows.len();
    }

    // Events are backfilled separately via backfill_events() which runs async,
    // sends direct HTTP batches, and doesn't hold the DB lock continuously.

    log::info!("Initial sync complete: {}", stats);
    Ok(stats)
}

/// Backfill events to cloud via direct HTTP POST, bypassing the sync channel.
///
/// Events can number in the hundreds of thousands. Pumping them through the
/// unbounded mpsc channel would spike memory and stall the batch drain loop.
/// Instead, this function pages through the DB, releases the lock between pages,
/// and POSTs each page directly to `/remote/sync`.
pub async fn backfill_events(
    db: &DbState,
    jwt_provider: std::sync::Arc<dyn Fn() -> Option<String> + Send + Sync>,
) -> Result<usize, String> {
    use crate::schema::events;
    use diesel::prelude::*;

    let client = reqwest::Client::new();
    let api_url = crate::api::ApiConfig::default().base_url;
    let page_size: i64 = 500;
    let mut offset: i64 = 0;
    let mut total: usize = 0;

    loop {
        // Lock DB briefly to load one page
        let rows: Vec<crate::diesel_models::DbEvent> = {
            let mut conn = db.conn.lock().map_err(|e| format!("DB lock: {}", e))?;
            events::table
                .order(events::created_at.asc())
                .limit(page_size)
                .offset(offset)
                .load(&mut *conn)
                .map_err(|e| format!("Failed to load events: {}", e))?
        };
        // DB lock released here

        let count = rows.len();
        if count == 0 {
            break;
        }

        // Build wire-format messages for this page
        let messages: Vec<serde_json::Value> = rows
            .iter()
            .map(|row| {
                let data = SyncEvent {
                    id: row.id.clone(),
                    run_id: row.run_id.clone(),
                    session_id: row.session_id.clone(),
                    sequence: Some(row.sequence),
                    event_type: row.event_type.clone(),
                    data: Some(row.data.clone()),
                    input_tokens: row.input_tokens,
                    output_tokens: row.output_tokens,
                    cache_read_tokens: row.cache_read_tokens,
                    created_at: Some(row.created_at as i64),
                    turn_id: row.turn_id.clone(),
                };
                serde_json::json!({
                    "table": "events", "action": "upsert", "id": data.id, "data": data
                })
            })
            .collect();

        let jwt = jwt_provider().ok_or("No JWT available for event backfill")?;
        let payload = serde_json::json!({ "messages": messages });

        let resp = client
            .post(format!("{}/remote/sync", api_url))
            .bearer_auth(&jwt)
            .json(&payload)
            .send()
            .await
            .map_err(|e| format!("Event backfill HTTP failed: {}", e))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("Event backfill API returned {}: {}", status, body));
        }

        total += count;
        log::debug!("Event backfill: sent {} events (total {})", count, total);

        if count < page_size as usize {
            break;
        }
        offset += page_size;

        // Yield between pages to avoid starving other tasks
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    log::info!("Event backfill complete: {} events", total);
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use diesel::prelude::*;
    use std::sync::Mutex;
    use tokio::sync::mpsc;

    use crate::db::DbState;
    use crate::diesel_models::NewComment;
    use crate::schema::comments;
    use crate::test_utils::{
        create_test_issue, create_test_job, create_test_project, test_diesel_conn,
    };

    fn test_db() -> DbState {
        DbState {
            conn: Mutex::new(test_diesel_conn()),
        }
    }

    #[test]
    fn initial_sync_empty_db() {
        let db = test_db();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let sender = SyncSender::new(tx);

        let stats = initial_sync(&db, &sender).unwrap();
        assert_eq!(stats.projects, 0);
        assert_eq!(stats.issues, 0);
        assert_eq!(stats.jobs, 0);
        assert_eq!(stats.runs, 0);
        assert_eq!(stats.comments, 0);
        assert_eq!(stats.artifacts, 0);
        assert_eq!(stats.events, 0);

        // No messages should have been sent
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn initial_sync_sends_all_entities() {
        let db = test_db();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let sender = SyncSender::new(tx);

        // Populate DB
        {
            let mut conn = db.conn.lock().unwrap();
            let proj_id = create_test_project(&mut conn, "Test", "TST");
            let issue_id = create_test_issue(&mut conn, &proj_id, "Issue 1");
            create_test_job(&mut conn, &issue_id, &proj_id, "chat", "running", None);

            // Add a comment
            let now = chrono::Utc::now().timestamp() as i32;
            diesel::insert_into(comments::table)
                .values(&NewComment {
                    id: "comment-1",
                    issue_id: &issue_id,
                    content: "Test comment",
                    source: "user",
                    created_at: now,
                })
                .execute(&mut *conn)
                .unwrap();
        }

        let stats = initial_sync(&db, &sender).unwrap();
        assert_eq!(stats.projects, 1);
        assert_eq!(stats.issues, 1);
        assert_eq!(stats.jobs, 1);
        assert_eq!(stats.comments, 1);
        // Events are backfilled separately, not via initial_sync
        assert_eq!(stats.events, 0);

        // Verify messages arrived on channel
        let mut message_count = 0;
        while rx.try_recv().is_ok() {
            message_count += 1;
        }
        // 1 project + 1 issue + 1 job + 0 runs + 1 comment + 0 artifacts = 4
        assert_eq!(message_count, 4);
    }

    #[tokio::test]
    async fn backfill_events_empty_db_returns_zero() {
        let db = test_db();
        let jwt_provider = std::sync::Arc::new(|| Some("fake-jwt".to_string()))
            as std::sync::Arc<dyn Fn() -> Option<String> + Send + Sync>;

        let result = backfill_events(&db, jwt_provider).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);
    }

    #[tokio::test]
    async fn backfill_events_no_jwt_returns_error() {
        use crate::diesel_models::{NewEvent, NewRun};
        use crate::schema::{events, runs};

        let db = test_db();

        // Insert prerequisite data: project → issue → job → run → event
        {
            let mut conn = db.conn.lock().unwrap();
            let proj_id = create_test_project(&mut conn, "Test", "TST");
            let issue_id = create_test_issue(&mut conn, &proj_id, "Issue 1");
            let job_id = create_test_job(&mut conn, &issue_id, &proj_id, "chat", "running", None);
            let now = chrono::Utc::now().timestamp() as i32;

            diesel::insert_into(runs::table)
                .values(&NewRun {
                    id: "run-1",
                    issue_id: Some(&issue_id),
                    project_id: Some(&proj_id),
                    job_id: Some(&job_id),
                    status: Some("live"),
                    session_id: None,
                    error_message: None,
                    started_at: Some(now),
                    exited_at: None,
                    created_at: now,
                    updated_at: now,
                    backend: None,
                    exit_reason: None,
                    start_mode: None,
                    chat_id: None,
                })
                .execute(&mut *conn)
                .unwrap();

            diesel::insert_into(events::table)
                .values(&NewEvent {
                    id: "evt-1",
                    run_id: "run-1",
                    session_id: Some("sess-1"),
                    sequence: 1,
                    timestamp: now,
                    event_type: "user",
                    data: r#"{"type":"user","content":"hello"}"#,
                    parent_tool_use_id: None,
                    created_at: now,
                    input_tokens: None,
                    cache_read_tokens: None,
                    cache_create_tokens: None,
                    output_tokens: None,
                    turn_id: None,
                })
                .execute(&mut *conn)
                .unwrap();
        }

        // JWT provider returns None — should error after loading events
        let jwt_provider = std::sync::Arc::new(|| None)
            as std::sync::Arc<dyn Fn() -> Option<String> + Send + Sync>;

        let result = backfill_events(&db, jwt_provider).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("No JWT available"));
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
            events: 500,
        };
        let s = format!("{}", stats);
        assert!(s.contains("projects=3"));
        assert!(s.contains("issues=10"));
        assert!(s.contains("artifacts=2"));
        assert!(s.contains("events=500"));
    }
}
