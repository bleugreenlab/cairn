use cairn_db::turso::params;

use crate::storage::{DbResult, LocalDb};

use super::types::*;

pub async fn record_live_comment_side_channel_message(
    db: &LocalDb,
    job_id: &str,
    issue_uri: &str,
    rendered: &str,
) -> Result<SuppressedWake, String> {
    record_live_comment_side_channel_message_with_key(db, job_id, issue_uri, rendered, None).await
}

pub(crate) async fn record_live_comment_side_channel_message_with_key(
    db: &LocalDb,
    job_id: &str,
    issue_uri: &str,
    rendered: &str,
    stable_id: Option<&str>,
) -> Result<SuppressedWake, String> {
    insert_message_wake_row(
        db,
        stable_id,
        None,
        job_id,
        SOURCE_KIND_ISSUE_COMMENT,
        Some(issue_uri),
        FACT_KIND_MESSAGE,
        Some(issue_uri),
        rendered,
    )
    .await
}

pub(crate) async fn record_live_issue_message_side_channel_message(
    db: &LocalDb,
    job_id: &str,
    issue_uri: &str,
    rendered: &str,
) -> Result<SuppressedWake, String> {
    insert_message_wake_row(
        db,
        None,
        None,
        job_id,
        SOURCE_KIND_ISSUE_MESSAGE,
        Some(issue_uri),
        FACT_KIND_MESSAGE,
        Some(issue_uri),
        rendered,
    )
    .await
}

pub(crate) async fn peek_pending_live_side_channel_for_job(
    db: &LocalDb,
    job_id: &str,
) -> Result<Vec<SuppressedWake>, String> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move { select_pending_live_side_channel(conn, &job_id).await })
    })
    .await
    .map_err(|error| format!("Failed to peek pending side-channel wake messages: {error}"))
}

pub(crate) async fn claim_pending_live_side_channel_for_job_async(
    db: &LocalDb,
    job_id: &str,
) -> Result<Vec<SuppressedWake>, String> {
    let job_id = job_id.to_string();
    let now = chrono::Utc::now().timestamp();
    db.write(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut notices = select_pending_live_side_channel(conn, &job_id).await?;
            if !notices.is_empty() {
                conn.execute(
                    "UPDATE suppressed_wakes
                     SET delivered_at = ?1, updated_at = ?1
                     WHERE job_id = ?2 AND delivered_at IS NULL
                       AND subscription_id IS NULL AND content IS NOT NULL
                       AND fact_kind = 'message'",
                    params![now, job_id.as_str()],
                )
                .await?;
                for notice in &mut notices {
                    notice.delivered_at = Some(now);
                    notice.updated_at = now;
                }
            }
            Ok(notices)
        })
    })
    .await
    .map_err(|error| format!("Failed to claim pending side-channel wake messages: {error}"))
}

async fn select_pending_live_side_channel(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
) -> DbResult<Vec<SuppressedWake>> {
    let mut rows = conn
        .query(
            "SELECT id, subscription_id, job_id, source_kind, source_ref, fact_kind,
                    occurrences, latest_detail_uri, content, created_at, updated_at, delivered_at
             FROM suppressed_wakes
             WHERE job_id = ?1 AND delivered_at IS NULL
               AND subscription_id IS NULL AND content IS NOT NULL
               AND fact_kind = 'message'
             ORDER BY created_at ASC, id ASC",
            params![job_id],
        )
        .await?;
    let mut notices = Vec::new();
    while let Some(row) = rows.next().await? {
        notices.push(suppressed_from_row(&row)?);
    }
    Ok(notices)
}

#[allow(clippy::too_many_arguments)]
async fn insert_message_wake_row(
    db: &LocalDb,
    stable_id: Option<&str>,
    subscription_id: Option<&str>,
    job_id: &str,
    source_kind: &str,
    source_ref: Option<&str>,
    fact_kind: &str,
    detail_uri: Option<&str>,
    content: &str,
) -> Result<SuppressedWake, String> {
    let id = stable_id
        .map(ToString::to_string)
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let now = chrono::Utc::now().timestamp();
    let subscription_id = subscription_id.map(ToString::to_string);
    let job_id = job_id.to_string();
    let source_kind = source_kind.to_string();
    let source_ref = source_ref.map(ToString::to_string);
    let fact_kind = fact_kind.to_string();
    let detail_uri = detail_uri.map(ToString::to_string);
    let content = content.to_string();
    db.write(|conn| {
        let id = id.clone();
        let subscription_id = subscription_id.clone();
        let job_id = job_id.clone();
        let source_kind = source_kind.clone();
        let source_ref = source_ref.clone();
        let fact_kind = fact_kind.clone();
        let detail_uri = detail_uri.clone();
        let content = content.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT OR IGNORE INTO suppressed_wakes
                 (id, subscription_id, job_id, source_kind, source_ref, fact_kind,
                  occurrences, latest_detail_uri, content, created_at, updated_at, delivered_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1, ?7, ?8, ?9, ?9, NULL)",
                params![
                    id.as_str(),
                    subscription_id.as_deref(),
                    job_id.as_str(),
                    source_kind.as_str(),
                    source_ref.as_deref(),
                    fact_kind.as_str(),
                    detail_uri.as_deref(),
                    content.as_str(),
                    now
                ],
            )
            .await?;
            let mut rows = conn
                .query(
                    "SELECT id, subscription_id, job_id, source_kind, source_ref, fact_kind,
                            occurrences, latest_detail_uri, content, created_at, updated_at, delivered_at
                     FROM suppressed_wakes WHERE id = ?1",
                    params![id.as_str()],
                )
                .await?;
            let row = rows
                .next()
                .await?
                .ok_or_else(|| crate::storage::DbError::Row("missing wake message".to_string()))?;
            suppressed_from_row(&row)
        })
    })
    .await
    .map_err(|error| format!("Failed to record wake message: {error}"))
}
