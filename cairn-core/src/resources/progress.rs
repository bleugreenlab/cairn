//! CAIRN-2499: read renderer for a workflow node's durable progress timeline.
//!
//! Renders the `workflow_progress` entries a workflow's `phase()`/`log()` verbs
//! appended as a chronological markdown timeline. This is the agent/CLI view of
//! `cairn://.../{node}/progress`; the monitoring panel reads the same rows
//! through the typed `get_workflow_monitor` query instead. `since` (epoch
//! seconds) and `limit` (tail count) mirror the `/messages` projections.

use cairn_common::query::QueryParam;

use crate::storage::LocalDb;
use crate::workflow_progress::{self, ProgressEntry};

pub(super) async fn read_node_progress(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    params: &[QueryParam],
) -> String {
    let mut since: Option<i64> = None;
    let mut limit: Option<usize> = None;
    for param in params {
        match param.key.as_str() {
            "since" => match param.value.parse::<i64>() {
                Ok(value) => since = Some(value),
                Err(_) => {
                    return format!("Invalid `since`: {} (expected epoch seconds)", param.value)
                }
            },
            "limit" => match param.value.parse::<usize>() {
                Ok(value) => limit = Some(value),
                Err(_) => {
                    return format!(
                        "Invalid `limit`: {} (expected a non-negative integer)",
                        param.value
                    )
                }
            },
            other => {
                return format!(
                    "Unknown query param `{other}` for progress (accepts: since, limit)"
                )
            }
        }
    }

    let job_id =
        match super::node::resolve_todos_job_id(db, project_key, number, exec_seq, node_name, None)
            .await
        {
            Ok(job_id) => job_id,
            Err(error) => return error,
        };

    let entries = match workflow_progress::list_entries(db, &job_id).await {
        Ok(entries) => entries,
        Err(error) => return format!("Failed to query workflow progress: {error}"),
    };

    let filtered: Vec<&ProgressEntry> = entries
        .iter()
        .filter(|entry| match since {
            Some(threshold) => entry.created_at >= threshold,
            None => true,
        })
        .collect();
    // `limit` keeps the most recent N (tail), matching the messages stream.
    let shown: &[&ProgressEntry] = match limit {
        Some(n) if filtered.len() > n => &filtered[filtered.len() - n..],
        _ => &filtered,
    };

    let label = format!("{}-{}/{}/{}", project_key, number, exec_seq, node_name);
    let mut out = format!(
        "# Workflow progress \u{2014} {}\n\n{} entr{}\n\n",
        label,
        shown.len(),
        if shown.len() == 1 { "y" } else { "ies" }
    );
    for entry in shown {
        let ts = chrono::DateTime::from_timestamp(entry.created_at, 0)
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
            .unwrap_or_else(|| entry.created_at.to_string());
        match entry.kind.as_str() {
            "phase" => out.push_str(&format!("[{ts}] \u{25b8} phase: {}\n", entry.text)),
            _ => out.push_str(&format!("[{ts}]   {}\n", entry.text)),
        }
    }
    out
}
