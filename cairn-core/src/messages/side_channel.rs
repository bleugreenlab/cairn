//! Durable, non-waking notices from user messages to child issue agents.

use serde::{Deserialize, Serialize};
use turso::params;

use crate::orchestrator::Orchestrator;
use crate::storage::{run_db_blocking, LocalDb, RowExt};

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum SideChannelOrigin {
    #[default]
    UserChild,
    IssueComment {
        source: String,
    },
    IssueMessage {
        source: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SideChannelNotice {
    pub id: String,
    pub parent_job_id: String,
    pub child_uri: String,
    pub content: String,
    #[serde(default)]
    pub origin: SideChannelOrigin,
    pub created_at: i64,
    pub delivered_at: Option<i64>,
}

impl SideChannelNotice {
    pub fn render(&self) -> String {
        match &self.origin {
            SideChannelOrigin::UserChild => format!(
                "[Side-channel] the user messaged your child {}:\n{}",
                self.child_uri, self.content
            ),
            SideChannelOrigin::IssueComment { source } => {
                let who = if source == "user" {
                    "the user"
                } else {
                    "an agent"
                };
                format!(
                    "[Side-channel] {who} commented on this issue {}:\n{}",
                    self.child_uri, self.content
                )
            }
            SideChannelOrigin::IssueMessage { source } => {
                let who = if source == "user" {
                    "the user"
                } else {
                    "an agent"
                };
                format!(
                    "[Side-channel] {who} posted a message on this issue {}:\n{}",
                    self.child_uri, self.content
                )
            }
        }
    }

    pub fn channel_type(&self) -> &'static str {
        match &self.origin {
            SideChannelOrigin::UserChild => "child_side_channel",
            SideChannelOrigin::IssueComment { .. } => "issue_comment",
            SideChannelOrigin::IssueMessage { .. } => "issue_message",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IssueSideChannelKind {
    Comment,
    Message,
}

impl IssueSideChannelKind {
    fn origin(self, source: String) -> SideChannelOrigin {
        match self {
            Self::Comment => SideChannelOrigin::IssueComment { source },
            Self::Message => SideChannelOrigin::IssueMessage { source },
        }
    }

    async fn record(
        self,
        db: &LocalDb,
        job_id: &str,
        issue_uri: &str,
        rendered: &str,
    ) -> Result<(), String> {
        match self {
            Self::Comment => {
                crate::orchestrator::wakes::record_live_comment_side_channel_message(
                    db, job_id, issue_uri, rendered,
                )
                .await?;
            }
            Self::Message => {
                crate::orchestrator::wakes::record_live_issue_message_side_channel_message(
                    db, job_id, issue_uri, rendered,
                )
                .await?;
            }
        }
        Ok(())
    }
}

fn parse_issue_notice_content(
    rendered: String,
    child_uri: &str,
    user_prefix: &str,
    agent_prefix: &str,
) -> (String, String) {
    let user_prefix = format!("[Side-channel] the user {user_prefix} {child_uri}:\n");
    let agent_prefix = format!("[Side-channel] an agent {agent_prefix} {child_uri}:\n");
    if let Some(content) = rendered.strip_prefix(&user_prefix) {
        ("user".to_string(), content.to_string())
    } else if let Some(content) = rendered.strip_prefix(&agent_prefix) {
        ("agent".to_string(), content.to_string())
    } else {
        ("agent".to_string(), rendered)
    }
}

fn notice_from_wake(wake: crate::orchestrator::wakes::SuppressedWake) -> SideChannelNotice {
    let rendered = wake.content.unwrap_or_default();
    let child_uri = wake
        .latest_detail_uri
        .or(wake.source_ref)
        .unwrap_or_else(|| "unknown child".to_string());
    let (origin, content) = if wake.source_kind == "issue_comment" {
        let (source, content) = parse_issue_notice_content(
            rendered,
            &child_uri,
            "commented on this issue",
            "commented on this issue",
        );
        (SideChannelOrigin::IssueComment { source }, content)
    } else if wake.source_kind == "issue_message" {
        let (source, content) = parse_issue_notice_content(
            rendered,
            &child_uri,
            "posted a message on this issue",
            "posted a message on this issue",
        );
        (SideChannelOrigin::IssueMessage { source }, content)
    } else {
        let prefix = format!("[Side-channel] the user messaged your child {child_uri}:\n");
        (
            SideChannelOrigin::UserChild,
            rendered
                .strip_prefix(&prefix)
                .unwrap_or(&rendered)
                .to_string(),
        )
    };
    SideChannelNotice {
        id: wake.id,
        parent_job_id: wake.job_id,
        child_uri,
        content,
        origin,
        created_at: wake.created_at,
        delivered_at: wake.delivered_at,
    }
}

pub fn record_user_child_side_channel(
    orch: &Orchestrator,
    child_issue_id: &str,
    child_uri: &str,
    content: &str,
) -> Result<Option<SideChannelNotice>, String> {
    let child_issue_id = child_issue_id.to_string();
    let child_uri = child_uri.to_string();
    let content = content.to_string();
    run_db_blocking(move || async move {
        record_user_child_side_channel_async(orch, &child_issue_id, &child_uri, &content).await
    })
}

pub async fn record_user_child_side_channel_async(
    orch: &Orchestrator,
    child_issue_id: &str,
    child_uri: &str,
    content: &str,
) -> Result<Option<SideChannelNotice>, String> {
    let Some(parent_job_id) =
        crate::orchestrator::parent_wake::load_parent_job(&orch.db.local, child_issue_id)?
    else {
        return Ok(None);
    };

    let child_issue_uri = match issue_uri_for_issue_id(&orch.db.local, child_issue_id).await? {
        Some(uri) => uri,
        None => child_uri.to_string(),
    };

    // CAIRN-1647: the parent's copy of a user→child message is now a durable
    // `message` attention item (request-response). It opens with
    // `responded:false`, folds into the parent's next briefing carrying the
    // current handling state, and bumps with the child's response at the
    // child's turn end — so the coordinator never acts on a message-only copy
    // the child already handled (the CAIRN-1663 phantom-issue fix). The child
    // still receives the message live through its own delivery path.
    let _ = parent_job_id; // presence already confirmed above
    crate::orchestrator::attention_delivery::record_child_message(
        orch,
        &child_issue_uri,
        child_issue_id,
        child_uri,
        "user",
        content,
    );
    Ok(None)
}

pub fn record_issue_comment_side_channel(
    orch: &Orchestrator,
    issue_id: &str,
    source: &str,
    content: &str,
    exclude_job_id: Option<&str>,
) -> Result<usize, String> {
    let issue_id = issue_id.to_string();
    let source = source.to_string();
    let content = content.to_string();
    let exclude_job_id = exclude_job_id.map(ToString::to_string);
    run_db_blocking(move || async move {
        record_issue_comment_side_channel_async(
            orch,
            &issue_id,
            &source,
            &content,
            exclude_job_id.as_deref(),
        )
        .await
    })
}

pub async fn record_issue_comment_side_channel_async(
    orch: &Orchestrator,
    issue_id: &str,
    source: &str,
    content: &str,
    exclude_job_id: Option<&str>,
) -> Result<usize, String> {
    record_issue_side_channel_async(
        orch,
        issue_id,
        source,
        content,
        exclude_job_id,
        IssueSideChannelKind::Comment,
    )
    .await
}

async fn record_issue_side_channel_async(
    orch: &Orchestrator,
    issue_id: &str,
    source: &str,
    content: &str,
    exclude_job_id: Option<&str>,
    kind: IssueSideChannelKind,
) -> Result<usize, String> {
    let Some(issue_uri) = issue_uri_for_issue_id(&orch.db.local, issue_id).await? else {
        return Ok(0);
    };

    let job_ids = crate::jobs::queries::active_agent_job_ids_for_issue(&orch.db.local, issue_id)
        .await
        .map_err(|error| error.to_string())?;

    let normalized_source = if source == "user" { "user" } else { "agent" };
    let mut delivered = 0usize;
    for job_id in job_ids {
        if exclude_job_id == Some(job_id.as_str()) {
            continue;
        }
        let notice = SideChannelNotice {
            id: String::new(),
            parent_job_id: job_id.clone(),
            child_uri: issue_uri.clone(),
            content: content.to_string(),
            origin: kind.origin(normalized_source.to_string()),
            created_at: 0,
            delivered_at: None,
        };
        kind.record(&orch.db.local, &job_id, &issue_uri, &notice.render())
            .await?;
        delivered += 1;
    }

    if delivered > 0 {
        let _ = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "suppressed_wakes", "action": "insert"}),
        );
    }
    Ok(delivered)
}

pub fn record_issue_message_side_channel(
    orch: &Orchestrator,
    issue_id: &str,
    source: &str,
    content: &str,
    exclude_job_id: Option<&str>,
) -> Result<usize, String> {
    let issue_id = issue_id.to_string();
    let source = source.to_string();
    let content = content.to_string();
    let exclude_job_id = exclude_job_id.map(ToString::to_string);
    run_db_blocking(move || async move {
        record_issue_message_side_channel_async(
            orch,
            &issue_id,
            &source,
            &content,
            exclude_job_id.as_deref(),
        )
        .await
    })
}

pub async fn record_issue_message_side_channel_async(
    orch: &Orchestrator,
    issue_id: &str,
    source: &str,
    content: &str,
    exclude_job_id: Option<&str>,
) -> Result<usize, String> {
    record_issue_side_channel_async(
        orch,
        issue_id,
        source,
        content,
        exclude_job_id,
        IssueSideChannelKind::Message,
    )
    .await
}

pub async fn record_issue_message_side_channel_by_issue_number(
    orch: &Orchestrator,
    project_key: &str,
    issue_number: i32,
    source: &str,
    content: &str,
    exclude_job_id: Option<&str>,
) -> Result<usize, String> {
    let Some(issue_id) = issue_id_for_key_number(&orch.db.local, project_key, issue_number).await?
    else {
        return Ok(0);
    };
    record_issue_message_side_channel_async(orch, &issue_id, source, content, exclude_job_id).await
}

pub async fn record_user_child_side_channel_by_issue_number(
    orch: &Orchestrator,
    project_key: &str,
    issue_number: i32,
    child_uri: &str,
    content: &str,
) -> Result<Option<SideChannelNotice>, String> {
    let Some(child_issue_id) =
        issue_id_for_key_number(&orch.db.local, project_key, issue_number).await?
    else {
        return Ok(None);
    };
    record_user_child_side_channel_async(orch, &child_issue_id, child_uri, content).await
}

pub fn record_user_child_side_channel_for_job(
    orch: &Orchestrator,
    child_job_id: &str,
    content: &str,
) -> Result<Option<SideChannelNotice>, String> {
    let child_job_id = child_job_id.to_string();
    let content = content.to_string();
    run_db_blocking(move || async move {
        let Some((child_issue_id, child_uri)) =
            child_issue_and_uri_for_job(&orch.db.local, &child_job_id).await?
        else {
            return Ok(None);
        };
        record_user_child_side_channel_async(orch, &child_issue_id, &child_uri, &content).await
    })
}

/// Non-stamping read of the pending side-channel notices for `job_id`.
///
/// Mirrors the SELECT in [`claim_pending_side_channel_for_job`] but does NOT
/// stamp `delivered_at`, so the flush-on-idle path can decide whether to resume
/// the parent job before committing to delivery. Delivery is stamped via
/// [`claim_pending_side_channel_for_job`] only after the resume succeeds.
pub fn peek_pending_side_channel_for_job(
    db: &LocalDb,
    job_id: &str,
) -> Result<Vec<SideChannelNotice>, String> {
    let job_id = job_id.to_string();
    run_db_blocking(
        move || async move { peek_pending_side_channel_for_job_async(db, &job_id).await },
    )
}

pub async fn peek_pending_side_channel_for_job_async(
    db: &LocalDb,
    job_id: &str,
) -> Result<Vec<SideChannelNotice>, String> {
    crate::orchestrator::wakes::peek_pending_live_side_channel_for_job(db, job_id)
        .await
        .map(|rows| rows.into_iter().map(notice_from_wake).collect())
}

pub fn claim_pending_side_channel_for_job(
    db: &LocalDb,
    job_id: &str,
) -> Result<Vec<SideChannelNotice>, String> {
    let job_id = job_id.to_string();
    run_db_blocking(
        move || async move { claim_pending_side_channel_for_job_async(db, &job_id).await },
    )
}

pub async fn claim_pending_side_channel_for_job_async(
    db: &LocalDb,
    job_id: &str,
) -> Result<Vec<SideChannelNotice>, String> {
    crate::orchestrator::wakes::claim_pending_live_side_channel_for_job_async(db, job_id)
        .await
        .map(|rows| rows.into_iter().map(notice_from_wake).collect())
}

pub async fn job_id_for_run(db: &LocalDb, run_id: &str) -> Option<String> {
    let run_id = run_id.to_string();
    db.read(|conn| {
        let run_id = run_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT job_id FROM runs WHERE id = ?1 LIMIT 1",
                    params![run_id.as_str()],
                )
                .await?;
            crate::storage::next_opt_text(&mut rows, 0).await
        })
    })
    .await
    .ok()
    .flatten()
}

async fn issue_uri_for_issue_id(db: &LocalDb, issue_id: &str) -> Result<Option<String>, String> {
    let issue_id = issue_id.to_string();
    let resolved: Option<(String, i32)> = db
        .read(|conn| {
            let issue_id = issue_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT p.key, i.number
                         FROM issues i
                         JOIN projects p ON i.project_id = p.id
                         WHERE i.id = ?1
                         LIMIT 1",
                        params![issue_id.as_str()],
                    )
                    .await?;
                rows.next()
                    .await?
                    .map(|row| Ok((row.text(0)?, row.i64(1)? as i32)))
                    .transpose()
            })
        })
        .await
        .map_err(|error| error.to_string())?;
    Ok(resolved
        .map(|(project_key, number)| cairn_common::uri::build_issue_uri(&project_key, number)))
}

async fn issue_id_for_key_number(
    db: &LocalDb,
    project_key: &str,
    issue_number: i32,
) -> Result<Option<String>, String> {
    let project_key = project_key.to_uppercase();
    db.read(|conn| {
        let project_key = project_key.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT i.id
                     FROM issues i
                     JOIN projects p ON i.project_id = p.id
                     WHERE p.key = ?1 AND i.number = ?2
                     LIMIT 1",
                    params![project_key.as_str(), issue_number],
                )
                .await?;
            crate::storage::next_text(&mut rows, 0).await
        })
    })
    .await
    .map_err(|error| error.to_string())
}

async fn child_issue_and_uri_for_job(
    db: &LocalDb,
    child_job_id: &str,
) -> Result<Option<(String, String)>, String> {
    let child_job_id = child_job_id.to_string();
    let resolved: Option<(String, String, i32, i32)> = db
        .read(|conn| {
            let child_job_id = child_job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT i.id, p.key, i.number, e.seq
                         FROM jobs j
                         JOIN issues i ON j.issue_id = i.id
                         JOIN projects p ON i.project_id = p.id
                         JOIN executions e ON j.execution_id = e.id
                         WHERE j.id = ?1
                         LIMIT 1",
                        params![child_job_id.as_str()],
                    )
                    .await?;
                rows.next()
                    .await?
                    .map(|row| {
                        Ok((
                            row.text(0)?,
                            row.text(1)?,
                            row.i64(2)? as i32,
                            row.i64(3)? as i32,
                        ))
                    })
                    .transpose()
            })
        })
        .await
        .map_err(|error| error.to_string())?;

    let Some((issue_id, project_key, issue_number, exec_seq)) = resolved else {
        return Ok(None);
    };
    let Some(segment) = crate::jobs::queries::node_uri_segment_for_job(db, &child_job_id).await
    else {
        return Ok(None);
    };
    let parent_segment = crate::jobs::queries::parent_uri_segment_for_job(db, &child_job_id).await;
    let child_uri = cairn_common::uri::build_job_base_uri(
        &project_key,
        issue_number,
        exec_seq,
        &segment,
        parent_segment.as_deref(),
    );
    Ok(Some((issue_id, child_uri)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::db::DbState;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{LocalDb, SearchIndex};
    use tempfile::tempdir;

    async fn migrated_db() -> LocalDb {
        crate::storage::migrated_test_db("side-channel.db").await
    }

    fn test_orchestrator(db: LocalDb) -> Orchestrator {
        let temp = tempdir().unwrap();
        let config_dir = temp.keep();
        let index_path = config_dir.join("search-index.db");
        let db_state = Arc::new(DbState::new(
            Arc::new(db),
            Arc::new(SearchIndex::open_or_create(index_path).unwrap()),
        ));
        let services = Arc::new(TestServicesBuilder::new().build());
        Orchestrator::builder(db_state, services, config_dir).build()
    }

    async fn seed_parent_job(db: &LocalDb, job_id: &str) {
        let job_id = job_id.to_string();
        db.write(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                conn.execute("INSERT OR IGNORE INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1)", ()).await?;
                conn.execute("INSERT OR IGNORE INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES('p','w','P','P','/tmp',1,1)", ()).await?;
                conn.execute("INSERT OR IGNORE INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at) VALUES('i','p',1,'I','active','active','none',1,1)", ()).await?;
                conn.execute("INSERT OR IGNORE INTO jobs(id, project_id, issue_id, status, current_session_id, created_at, updated_at) VALUES(?1,'p','i','complete','s',1,1)", params![job_id.as_str()]).await?;
                Ok(())
            })
        }).await.unwrap();
    }

    async fn seed_issue_job_run(db: &LocalDb, job_id: &str, run_id: &str, run_status: &str) {
        let job_id = job_id.to_string();
        let run_id = run_id.to_string();
        let run_status = run_status.to_string();
        db.write(|conn| {
            let job_id = job_id.clone();
            let run_id = run_id.clone();
            let run_status = run_status.clone();
            Box::pin(async move {
                conn.execute("INSERT OR IGNORE INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('proj-1', 'default', 'Project', 'PROJ', '/tmp/repo', 1, 1)", ()).await?;
                conn.execute("INSERT OR IGNORE INTO issues (id, project_id, number, title, status, created_at, updated_at) VALUES ('issue-1', 'proj-1', 42, 'Issue', 'active', 1, 1)", ()).await?;
                conn.execute("INSERT OR IGNORE INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq) VALUES ('exec-1', 'recipe-default', 'issue-1', 'proj-1', 'running', 1, 1)", ()).await?;
                conn.execute("INSERT OR IGNORE INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, node_name, uri_segment, status, created_at, updated_at) VALUES (?1, 'exec-1', 'builder', 'issue-1', 'proj-1', ?1, ?1, 'running', 1, 1)", params![job_id.as_str()]).await?;
                conn.execute("INSERT INTO runs (id, project_id, issue_id, job_id, status, created_at, updated_at) VALUES (?1, 'proj-1', 'issue-1', ?2, ?3, 1, 1)", params![run_id.as_str(), job_id.as_str(), run_status.as_str()]).await?;
                Ok(())
            })
        }).await.unwrap();
    }

    // Seed a pending UserChild message-bearing side-channel row directly and
    // return the equivalent notice. The production user→child recorder moved to
    // the attention ledger (CAIRN-1647); this exercises the generic claim/peek
    // machinery, still used by the issue comment / issue-message side channels.
    async fn insert_notice_for_test(
        db: &LocalDb,
        parent_job_id: &str,
        child_uri: &str,
        content: &str,
    ) -> SideChannelNotice {
        seed_parent_job(db, parent_job_id).await;
        let rendered =
            format!("[Side-channel] the user messaged your child {child_uri}:\n{content}");
        let id = uuid::Uuid::new_v4().to_string();
        {
            let id = id.clone();
            let parent = parent_job_id.to_string();
            let child = child_uri.to_string();
            let rendered = rendered.clone();
            db.write(move |conn| {
                let id = id.clone();
                let parent = parent.clone();
                let child = child.clone();
                let rendered = rendered.clone();
                Box::pin(async move {
                    conn.execute(
                        "INSERT INTO suppressed_wakes
                           (id, subscription_id, job_id, source_kind, source_ref, fact_kind,
                            occurrences, latest_detail_uri, content, created_at, updated_at, delivered_at)
                         VALUES (?1, NULL, ?2, 'issue', ?3, 'message', 1, ?3, ?4, 1, 1, NULL)",
                        params![id.as_str(), parent.as_str(), child.as_str(), rendered.as_str()],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();
        }
        let wake = crate::orchestrator::wakes::SuppressedWake {
            id,
            subscription_id: None,
            job_id: parent_job_id.to_string(),
            source_kind: "issue".to_string(),
            source_ref: Some(child_uri.to_string()),
            fact_kind: Some("message".to_string()),
            occurrences: 1,
            latest_detail_uri: Some(child_uri.to_string()),
            content: Some(rendered),
            created_at: 1,
            updated_at: 1,
            delivered_at: None,
        };
        notice_from_wake(wake)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn active_agent_job_ids_for_issue_returns_live_and_starting_deduped() {
        let db = migrated_db().await;
        seed_issue_job_run(&db, "job-live", "run-live", "live").await;
        seed_issue_job_run(&db, "job-starting", "run-starting", "starting").await;
        seed_issue_job_run(&db, "job-complete", "run-complete", "complete").await;
        seed_issue_job_run(&db, "job-live", "run-live-2", "live").await;

        let job_ids = crate::jobs::queries::active_agent_job_ids_for_issue(&db, "issue-1")
            .await
            .unwrap();
        assert_eq!(
            job_ids,
            vec!["job-live".to_string(), "job-starting".to_string()]
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn record_issue_comment_side_channel_delivers_to_active_jobs() {
        let db = migrated_db().await;
        seed_issue_job_run(&db, "job-a", "run-a", "live").await;
        seed_issue_job_run(&db, "job-b", "run-b", "starting").await;
        let orch = test_orchestrator(db);

        let delivered = record_issue_comment_side_channel_async(
            &orch,
            "issue-1",
            "user",
            "please consider this",
            None,
        )
        .await
        .unwrap();
        assert_eq!(delivered, 2);

        for job_id in ["job-a", "job-b"] {
            let claimed = claim_pending_side_channel_for_job_async(&orch.db.local, job_id)
                .await
                .unwrap();
            assert_eq!(claimed.len(), 1);
            assert_eq!(
                claimed[0].render(),
                "[Side-channel] the user commented on this issue cairn://p/PROJ/42:\nplease consider this"
            );
            assert_eq!(claimed[0].channel_type(), "issue_comment");
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn record_issue_message_side_channel_delivers_and_excludes_author() {
        let db = migrated_db().await;
        seed_issue_job_run(&db, "job-a", "run-a", "live").await;
        seed_issue_job_run(&db, "job-b", "run-b", "live").await;
        let orch = test_orchestrator(db);

        let delivered = record_issue_message_side_channel_async(
            &orch,
            "issue-1",
            "agent",
            "message for siblings",
            Some("job-a"),
        )
        .await
        .unwrap();
        assert_eq!(delivered, 1);

        assert!(
            claim_pending_side_channel_for_job_async(&orch.db.local, "job-a")
                .await
                .unwrap()
                .is_empty()
        );
        let claimed = claim_pending_side_channel_for_job_async(&orch.db.local, "job-b")
            .await
            .unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(
            claimed[0].render(),
            "[Side-channel] an agent posted a message on this issue cairn://p/PROJ/42:\nmessage for siblings"
        );
        assert_eq!(claimed[0].channel_type(), "issue_message");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn issue_message_notice_round_trips_origin_and_bare_content() {
        let db = migrated_db().await;
        seed_parent_job(&db, "job-a").await;
        let issue_uri = "cairn://p/PROJ/42";
        let rendered = "[Side-channel] the user posted a message on this issue cairn://p/PROJ/42:\nmessage body";
        let wake = crate::orchestrator::wakes::record_live_issue_message_side_channel_message(
            &db, "job-a", issue_uri, rendered,
        )
        .await
        .unwrap();

        let notice = notice_from_wake(wake);
        assert_eq!(
            notice.origin,
            SideChannelOrigin::IssueMessage {
                source: "user".to_string()
            }
        );
        assert_eq!(notice.content, "message body");
        assert_eq!(notice.render(), rendered);
        assert_eq!(notice.channel_type(), "issue_message");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn record_issue_comment_side_channel_no_active_jobs_is_noop() {
        let db = migrated_db().await;
        seed_issue_job_run(&db, "job-done", "run-done", "complete").await;
        let orch = test_orchestrator(db);

        let delivered = record_issue_comment_side_channel_async(
            &orch,
            "issue-1",
            "user",
            "no one is running",
            None,
        )
        .await
        .unwrap();
        assert_eq!(delivered, 0);
        let claimed = claim_pending_side_channel_for_job_async(&orch.db.local, "job-done")
            .await
            .unwrap();
        assert!(claimed.is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn record_issue_comment_side_channel_excludes_authoring_job() {
        let db = migrated_db().await;
        seed_issue_job_run(&db, "job-a", "run-a", "live").await;
        seed_issue_job_run(&db, "job-b", "run-b", "live").await;
        seed_issue_job_run(&db, "job-c", "run-c", "live").await;
        let orch = test_orchestrator(db);

        let delivered = record_issue_comment_side_channel_async(
            &orch,
            "issue-1",
            "agent",
            "sibling update",
            Some("job-b"),
        )
        .await
        .unwrap();
        assert_eq!(delivered, 2);

        assert_eq!(
            claim_pending_side_channel_for_job_async(&orch.db.local, "job-b")
                .await
                .unwrap()
                .len(),
            0
        );
        for job_id in ["job-a", "job-c"] {
            let claimed = claim_pending_side_channel_for_job_async(&orch.db.local, job_id)
                .await
                .unwrap();
            assert_eq!(claimed.len(), 1);
            assert_eq!(
                claimed[0].render(),
                "[Side-channel] an agent commented on this issue cairn://p/PROJ/42:\nsibling update"
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn issue_comment_notice_round_trips_origin_and_bare_content() {
        let db = migrated_db().await;
        seed_parent_job(&db, "job-a").await;
        let issue_uri = "cairn://p/PROJ/42";
        let rendered =
            "[Side-channel] the user commented on this issue cairn://p/PROJ/42:\ncomment body";
        let wake = crate::orchestrator::wakes::record_live_comment_side_channel_message(
            &db, "job-a", issue_uri, rendered,
        )
        .await
        .unwrap();

        let notice = notice_from_wake(wake);
        assert_eq!(
            notice.origin,
            SideChannelOrigin::IssueComment {
                source: "user".to_string()
            }
        );
        assert_eq!(notice.content, "comment body");
        assert_eq!(notice.render(), rendered);
        assert_eq!(notice.channel_type(), "issue_comment");

        let child =
            insert_notice_for_test(&db, "job-child", "cairn://p/PROJ/42/1/child", "child body")
                .await;
        assert_eq!(child.origin, SideChannelOrigin::UserChild);
        assert_eq!(child.content, "child body");
        assert_eq!(child.channel_type(), "child_side_channel");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn claim_pending_side_channel_marks_delivered_and_filters_by_parent_job() {
        let db = migrated_db().await;
        let first = insert_notice_for_test(&db, "parent-a", "cairn://p/P/1/1/child", "one").await;
        let second = insert_notice_for_test(&db, "parent-a", "cairn://p/P/2/1/child", "two").await;
        insert_notice_for_test(&db, "parent-b", "cairn://p/P/3/1/child", "three").await;

        let claimed = claim_pending_side_channel_for_job_async(&db, "parent-a")
            .await
            .unwrap();
        assert_eq!(claimed.len(), 2);
        assert!(claimed.iter().all(|notice| notice.delivered_at.is_some()));
        let ids: std::collections::HashSet<&str> =
            claimed.iter().map(|notice| notice.id.as_str()).collect();
        assert!(ids.contains(first.id.as_str()));
        assert!(ids.contains(second.id.as_str()));

        let again = claim_pending_side_channel_for_job_async(&db, "parent-a")
            .await
            .unwrap();
        assert!(again.is_empty());

        let other = claim_pending_side_channel_for_job_async(&db, "parent-b")
            .await
            .unwrap();
        assert_eq!(other.len(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn peek_pending_side_channel_does_not_stamp_delivered() {
        let db = migrated_db().await;
        insert_notice_for_test(&db, "parent-a", "cairn://p/P/1/1/child", "queued").await;

        // Peek returns the pending notice without stamping it...
        let peeked = peek_pending_side_channel_for_job_async(&db, "parent-a")
            .await
            .unwrap();
        assert_eq!(peeked.len(), 1);
        assert!(peeked.iter().all(|notice| notice.delivered_at.is_none()));

        // ...so a subsequent claim still finds and stamps it.
        let claimed = claim_pending_side_channel_for_job_async(&db, "parent-a")
            .await
            .unwrap();
        assert_eq!(
            claimed.len(),
            1,
            "peek must not stamp delivered_at, so the notice is still claimable"
        );
    }
}
