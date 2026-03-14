use crate::diesel_models::{DbPrData, DbWebhookEvent};
use crate::models::PrDataSummary;
use crate::models::WebhookEvent;
use crate::models::{Check, PrCache};
use crate::schema::{action_runs, pr_data, webhook_events};
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;

/// Convert DbPrData to PrCache (the frontend model)
fn db_pr_data_to_pr_cache(db: DbPrData) -> PrCache {
    let checks: Vec<Check> = db
        .checks_json
        .and_then(|json| serde_json::from_str(&json).ok())
        .unwrap_or_default();

    PrCache {
        id: db.id,
        job_id: None,
        pr_number: db.pr_number,
        pr_url: db.pr_url,
        title: db.title,
        body: db.body,
        state: db.state.and_then(|s| s.parse().ok()).unwrap_or_default(),
        is_draft: db.is_draft.map(|v| v != 0).unwrap_or(false),
        review_decision: db.review_decision.and_then(|s| s.parse().ok()),
        mergeable: db
            .mergeable
            .and_then(|s| s.parse().ok())
            .unwrap_or_default(),
        additions: db.additions,
        deletions: db.deletions,
        checks_status: db.checks_status.and_then(|s| s.parse().ok()),
        checks,
        fetched_at: db.fetched_at.map(|t| t as i64).unwrap_or(0),
        updated_at: db.updated_at as i64,
    }
}

fn db_webhook_to_model(e: DbWebhookEvent) -> WebhookEvent {
    WebhookEvent {
        id: e.id,
        event_type: e.event_type,
        action: e.action,
        repo_full_name: e.repo_full_name,
        pr_number: e.pr_number.map(|n| n as i64),
        payload_summary: e.payload_summary,
        processed_at: e.processed_at as i64,
    }
}

pub fn get_pr_data_for_action_runs(
    conn: &mut SqliteConnection,
    action_run_ids: &[String],
) -> Result<Vec<PrDataSummary>, String> {
    if action_run_ids.is_empty() {
        return Ok(vec![]);
    }
    let results: Vec<DbPrData> = pr_data::table
        .filter(pr_data::action_run_id.eq_any(action_run_ids))
        .load(conn)
        .map_err(|e| e.to_string())?;
    Ok(results
        .into_iter()
        .map(|d| PrDataSummary {
            id: d.id,
            action_run_id: d.action_run_id,
            pr_number: d.pr_number,
            pr_url: d.pr_url,
            pr_status: d.pr_status,
        })
        .collect())
}

pub fn get_pr_details(
    conn: &mut SqliteConnection,
    action_run_id: &str,
) -> Result<Option<PrCache>, String> {
    let db_pr: Option<DbPrData> = pr_data::table
        .filter(pr_data::action_run_id.eq(action_run_id))
        .first(conn)
        .optional()
        .map_err(|e| e.to_string())?;
    Ok(db_pr.map(db_pr_data_to_pr_cache))
}

pub fn get_webhook_events(
    conn: &mut SqliteConnection,
    pr_number: i64,
) -> Result<Vec<WebhookEvent>, String> {
    let events = webhook_events::table
        .filter(webhook_events::pr_number.eq(pr_number as i32))
        .order(webhook_events::processed_at.desc())
        .limit(100)
        .load::<DbWebhookEvent>(conn)
        .map_err(|e| e.to_string())?;
    Ok(events.into_iter().map(db_webhook_to_model).collect())
}

pub fn get_webhook_events_for_implementation(
    conn: &mut SqliteConnection,
    implementation_id: &str,
) -> Result<Vec<WebhookEvent>, String> {
    let pr_number: Option<i32> = pr_data::table
        .inner_join(action_runs::table.on(pr_data::action_run_id.eq(action_runs::id.nullable())))
        .filter(action_runs::parent_job_id.eq(implementation_id))
        .select(pr_data::pr_number)
        .first(conn)
        .ok();
    match pr_number {
        Some(pr_num) => {
            let events = webhook_events::table
                .filter(webhook_events::pr_number.eq(pr_num))
                .order(webhook_events::processed_at.desc())
                .limit(100)
                .load::<DbWebhookEvent>(conn)
                .map_err(|e| e.to_string())?;
            Ok(events.into_iter().map(db_webhook_to_model).collect())
        }
        None => Ok(vec![]),
    }
}
