use crate::models::{Check, MergeableState, PrCache, PrDataSummary, PrState, WebhookEvent};
use crate::storage::{DbResult, LocalDb, RowExt};
use turso::params;

const PR_COLUMNS: &str = "id, job_id, github_pr_number, github_pr_url, title, body, github_state,
    status, github_review, github_mergeable, additions, deletions, checks_status, checks_json,
    github_fetched_at, updated_at, source_branch, target_branch";

fn pr_cache_from_row(row: &turso::Row) -> DbResult<PrCache> {
    let checks: Vec<Check> = row
        .opt_text(13)?
        .and_then(|json| serde_json::from_str(&json).ok())
        .unwrap_or_default();
    let status = row.text(7)?;

    Ok(PrCache {
        id: row.text(0)?,
        job_id: row.opt_text(1)?,
        pr_number: row.opt_i64(2)?.unwrap_or(0) as i32,
        pr_url: row.opt_text(3)?.unwrap_or_default(),
        title: row.opt_text(4)?,
        body: row.opt_text(5)?,
        state: row
            .opt_text(6)?
            .and_then(|s| s.parse().ok())
            .unwrap_or(match status.as_str() {
                "merged" => PrState::Merged,
                "closed" => PrState::Closed,
                _ => PrState::Open,
            }),
        is_draft: false,
        review_decision: row.opt_text(8)?.and_then(|s| s.parse().ok()),
        mergeable: row
            .opt_text(9)?
            .and_then(|s| s.parse().ok())
            .unwrap_or(MergeableState::Unknown),
        additions: row.opt_i64(10)?.map(|value| value as i32),
        deletions: row.opt_i64(11)?.map(|value| value as i32),
        checks_status: row.opt_text(12)?.and_then(|s| s.parse().ok()),
        checks,
        fetched_at: row.opt_i64(14)?.unwrap_or(0),
        updated_at: row.i64(15)?,
        is_local: row.opt_i64(2)?.is_none(),
        source_branch: row.opt_text(16)?,
        target_branch: row.opt_text(17)?,
    })
}

pub async fn get_by_job_id(db: &LocalDb, job_id: &str) -> Result<Option<PrCache>, String> {
    let job_id = job_id.to_string();
    db.query_opt(
        format!("SELECT {PR_COLUMNS} FROM merge_requests WHERE job_id = ?1 LIMIT 1"),
        params![job_id.as_str()],
        pr_cache_from_row,
    )
    .await
    .map_err(|e| format!("Failed to load PR by job: {e}"))
}

pub async fn get_by_id(db: &LocalDb, id: &str) -> Result<Option<PrCache>, String> {
    let id = id.to_string();
    db.query_opt(
        format!("SELECT {PR_COLUMNS} FROM merge_requests WHERE id = ?1 LIMIT 1"),
        params![id.as_str()],
        pr_cache_from_row,
    )
    .await
    .map_err(|e| format!("Failed to load PR by id: {e}"))
}

pub async fn get_summaries_for_jobs(
    db: &LocalDb,
    job_ids: &[String],
) -> Result<Vec<PrDataSummary>, String> {
    if job_ids.is_empty() {
        return Ok(vec![]);
    }

    let mut summaries = Vec::new();
    for job_id in job_ids {
        if let Some(summary) = pr_summary_for_job(db, job_id, job_id).await? {
            summaries.push(summary);
        }
    }
    Ok(summaries)
}

pub async fn get_summaries_for_action_runs(
    db: &LocalDb,
    action_run_ids: &[String],
) -> Result<Vec<PrDataSummary>, String> {
    if action_run_ids.is_empty() {
        return Ok(vec![]);
    }

    let mut summaries = Vec::new();
    for action_run_id in action_run_ids {
        // A first-class `pr` node links its `merge_requests` row to the
        // action_run's own id (CAIRN-1220); the legacy `create_pr` action
        // links it to the producing builder job (`parent_job_id`). Resolve
        // the owner that actually owns the MR: own id first, then parent.
        let mut owner_ids = vec![action_run_id.clone()];
        if let Some(parent_job_id) = parent_job_id_for_action_run(db, action_run_id).await? {
            owner_ids.push(parent_job_id);
        }

        for owner_id in owner_ids {
            if let Some(summary) = pr_summary_for_job(db, &owner_id, action_run_id).await? {
                summaries.push(summary);
                break;
            }
        }
    }
    Ok(summaries)
}

pub async fn get_by_action_run_id(
    db: &LocalDb,
    action_run_id: &str,
) -> Result<Option<PrCache>, String> {
    if let Some(pr) = pr_cache_for_job(db, action_run_id).await? {
        return Ok(Some(pr));
    }

    let Some(job_id) = parent_job_id_for_action_run(db, action_run_id).await? else {
        return Ok(None);
    };

    pr_cache_for_job(db, &job_id).await
}

async fn pr_cache_for_job(db: &LocalDb, job_id: &str) -> Result<Option<PrCache>, String> {
    let job_id = job_id.to_string();
    db.query_opt(
        format!("SELECT {PR_COLUMNS} FROM merge_requests WHERE job_id = ?1 LIMIT 1"),
        params![job_id.as_str()],
        pr_cache_from_row,
    )
    .await
    .map_err(|e| format!("Failed to load PR details: {e}"))
}

async fn parent_job_id_for_action_run(
    db: &LocalDb,
    action_run_id: &str,
) -> Result<Option<String>, String> {
    let action_run_id = action_run_id.to_string();
    db.query_opt(
        "SELECT parent_job_id FROM action_runs WHERE id = ?1",
        params![action_run_id.as_str()],
        |row| row.opt_text(0),
    )
    .await
    .map(Option::flatten)
    .map_err(|e| format!("Failed to load action run parent: {e}"))
}

async fn pr_summary_for_job(
    db: &LocalDb,
    job_id: &str,
    action_run_id: &str,
) -> Result<Option<PrDataSummary>, String> {
    let job_id = job_id.to_string();
    let action_run_id = action_run_id.to_string();
    db.query_opt(
        "SELECT id, github_pr_number, github_pr_url, status
         FROM merge_requests
         WHERE job_id = ?1
         LIMIT 1",
        params![job_id.as_str()],
        move |row| {
            Ok(PrDataSummary {
                id: row.text(0)?,
                action_run_id: Some(action_run_id.clone()),
                pr_number: row.opt_i64(1)?.unwrap_or(0) as i32,
                pr_url: row.opt_text(2)?.unwrap_or_default(),
                pr_status: row.text(3)?,
            })
        },
    )
    .await
    .map_err(|e| format!("Failed to load PR summary: {e}"))
}

pub async fn get_project_merge_requests(
    db: &LocalDb,
    project_id: &str,
) -> Result<Vec<crate::models::ProjectPrEntry>, String> {
    let project_id = project_id.to_string();
    db.query_all(
        "SELECT mr.id, mr.job_id, mr.status, mr.title, mr.additions, mr.deletions,
                mr.checks_status, mr.github_review, mr.github_pr_number,
                mr.github_pr_url, mr.opened_at, mr.updated_at, mr.issue_id,
                i.number, i.title, j.execution_id
         FROM merge_requests mr
         LEFT JOIN issues i ON mr.issue_id = i.id
         LEFT JOIN jobs j ON j.id = mr.job_id
         WHERE mr.project_id = ?1
         ORDER BY mr.updated_at DESC",
        params![project_id.as_str()],
        |row| {
            Ok(crate::models::ProjectPrEntry {
                id: row.text(0)?,
                action_run_id: row.text(1)?,
                pr_status: row.text(2)?,
                title: row.opt_text(3)?,
                additions: row.opt_i64(4)?.map(|value| value as i32),
                deletions: row.opt_i64(5)?.map(|value| value as i32),
                checks_status: row.opt_text(6)?,
                review_decision: row.opt_text(7)?,
                pr_number: row.opt_i64(8)?.unwrap_or(0) as i32,
                pr_url: row.opt_text(9)?.unwrap_or_default(),
                opened_at: Some(row.i64(10)?),
                updated_at: row.i64(11)?,
                issue_id: row.opt_text(12)?,
                issue_number: row.opt_i64(13)?.map(|value| value as i32),
                issue_title: row.opt_text(14)?,
                execution_id: row.opt_text(15)?.unwrap_or_default(),
                is_draft: false,
            })
        },
    )
    .await
    .map_err(|e| format!("Failed to load project PRs: {e}"))
}

pub async fn get_webhook_events(db: &LocalDb, pr_number: i64) -> Result<Vec<WebhookEvent>, String> {
    db.query_all(
        "SELECT id, event_type, action, repo_full_name, pr_number,
                payload_summary, processed_at
         FROM webhook_events
         WHERE pr_number = ?1
         ORDER BY processed_at DESC
         LIMIT 100",
        params![pr_number],
        webhook_event_from_row,
    )
    .await
    .map_err(|e| format!("Failed to load webhook events: {e}"))
}

pub async fn get_webhook_events_for_job(
    db: &LocalDb,
    job_id: &str,
) -> Result<Vec<WebhookEvent>, String> {
    let Some(pr_number) = db
        .query_opt(
            "SELECT github_pr_number FROM merge_requests WHERE job_id = ?1",
            params![job_id],
            |row| row.opt_i64(0),
        )
        .await
        .map_err(|e| format!("Failed to load webhook events for job: {e}"))?
        .flatten()
    else {
        return Ok(Vec::new());
    };

    get_webhook_events(db, pr_number)
        .await
        .map_err(|e| format!("Failed to load webhook events for job: {e}"))
}

fn webhook_event_from_row(row: &turso::Row) -> DbResult<WebhookEvent> {
    Ok(WebhookEvent {
        id: row.text(0)?,
        event_type: row.text(1)?,
        action: row.text(2)?,
        repo_full_name: row.text(3)?,
        pr_number: row.opt_i64(4)?,
        payload_summary: row.text(5)?,
        processed_at: row.i64(6)?,
    })
}
