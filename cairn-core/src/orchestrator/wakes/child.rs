use turso::params;

use crate::storage::{LocalDb, RowExt};

use super::store::seed_scope;
use super::types::*;

pub async fn seed_default_child_subscription_for_parent_job(
    db: &LocalDb,
    parent_job_id: &str,
    issue_uri: &str,
) -> Result<WakeSubscription, String> {
    let kinds = DEFAULT_CHILD_FACT_KINDS
        .iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>();
    let source = WakeSource::Issue {
        reference: issue_uri.to_string(),
    };
    let scope = WakeScope::new(source, Some(kinds));
    seed_scope(db, parent_job_id, &scope, "system").await
}

pub async fn seed_default_child_subscription_for_issue(
    db: &LocalDb,
    child_issue_id: &str,
    issue_uri: &str,
) -> Result<Option<WakeSubscription>, String> {
    let Some(parent_job_id) = parent_job_for_child_issue_async(db, child_issue_id).await? else {
        return Ok(None);
    };
    seed_default_child_subscription_for_parent_job(db, &parent_job_id, issue_uri)
        .await
        .map(Some)
}

pub async fn reconcile_default_child_subscription_for_issue(
    db: &LocalDb,
    child_issue_id: &str,
    issue_uri: &str,
) -> Result<Option<WakeSubscription>, String> {
    let parent_job_id = parent_job_for_child_issue_async(db, child_issue_id).await?;
    remove_stale_default_child_subscriptions(db, issue_uri, parent_job_id.as_deref()).await?;
    match parent_job_id {
        Some(parent_job_id) => {
            seed_default_child_subscription_for_parent_job(db, &parent_job_id, issue_uri)
                .await
                .map(Some)
        }
        None => Ok(None),
    }
}

async fn remove_stale_default_child_subscriptions(
    db: &LocalDb,
    issue_uri: &str,
    current_parent_job_id: Option<&str>,
) -> Result<usize, String> {
    let issue_uri = issue_uri.to_string();
    let current_parent_job_id = current_parent_job_id.map(ToString::to_string);
    let default_fact_kinds = default_child_fact_kinds_json();
    db.write(|conn| {
        let issue_uri = issue_uri.clone();
        let current_parent_job_id = current_parent_job_id.clone();
        let default_fact_kinds = default_fact_kinds.clone();
        Box::pin(async move {
            let changed = match current_parent_job_id.as_deref() {
                Some(parent_job_id) => {
                    conn.execute(
                        "DELETE FROM wake_subscriptions
                         WHERE created_by = 'system'
                           AND source_kind = 'issue'
                           AND source_ref = ?1
                           AND one_shot = 0
                           AND COALESCE(fact_kinds_json, '') = ?2
                           AND job_id != ?3",
                        params![
                            issue_uri.as_str(),
                            default_fact_kinds.as_str(),
                            parent_job_id
                        ],
                    )
                    .await?
                }
                None => {
                    conn.execute(
                        "DELETE FROM wake_subscriptions
                         WHERE created_by = 'system'
                           AND source_kind = 'issue'
                           AND source_ref = ?1
                           AND one_shot = 0
                           AND COALESCE(fact_kinds_json, '') = ?2",
                        params![issue_uri.as_str(), default_fact_kinds.as_str()],
                    )
                    .await?
                }
            };
            Ok(changed as usize)
        })
    })
    .await
    .map_err(|error| format!("Failed to reconcile default child wake subscriptions: {error}"))
}

fn default_child_fact_kinds_json() -> String {
    let kinds = DEFAULT_CHILD_FACT_KINDS
        .iter()
        .map(|kind| (*kind).to_string())
        .collect::<Vec<_>>();
    fact_kinds_json(Some(&kinds)).unwrap_or_else(|| "[]".to_string())
}

async fn parent_job_for_child_issue_async(
    db: &LocalDb,
    child_issue_id: &str,
) -> Result<Option<String>, String> {
    let child_issue_id = child_issue_id.to_string();
    db.read(|conn| {
        let child_issue_id = child_issue_id.clone();
        Box::pin(async move {
            let mut issue_rows = conn
                .query(
                    "SELECT parent_job_id, parent_issue_id FROM issues WHERE id = ?1 LIMIT 1",
                    params![child_issue_id.as_str()],
                )
                .await?;
            let Some(issue_row) = issue_rows.next().await? else {
                return Ok(None);
            };
            let spawning_job_id = issue_row.opt_text(0)?;
            let parent_issue_id = issue_row.opt_text(1)?;
            drop(issue_rows);

            if let Some(job_id) = spawning_job_id {
                let mut rows = conn
                    .query(
                        "SELECT id FROM jobs
                         WHERE id = ?1 AND status != 'failed' AND current_session_id IS NOT NULL
                         LIMIT 1",
                        params![job_id.as_str()],
                    )
                    .await?;
                if let Some(row) = rows.next().await? {
                    return row.text(0).map(Some);
                }
            }

            let Some(parent_issue_id) = parent_issue_id else {
                return Ok(None);
            };
            let mut job_rows = conn
                .query(
                    "SELECT id
                     FROM jobs
                     WHERE issue_id = ?1
                       AND parent_job_id IS NULL
                       AND status != 'failed'
                       AND current_session_id IS NOT NULL
                     ORDER BY created_at DESC
                     LIMIT 1",
                    params![parent_issue_id.as_str()],
                )
                .await?;
            crate::storage::next_text(&mut job_rows, 0).await
        })
    })
    .await
    .map_err(|error| format!("Failed to resolve parent wake job: {error}"))
}
