//! Label resource reads.

use cairn_db::turso::params;
use cairn_uri::build_label_uri;

use super::common::{read_resource, storage_error};
use crate::labels::crud::{get_label_conn, list_labels_conn, DEFAULT_WORKSPACE_ID};
use crate::storage::{LocalDb, RowExt};

async fn usage_count(conn: &cairn_db::storage::TrackedConnection, label_id: &str) -> i64 {
    match conn
        .query(
            "SELECT COUNT(*) FROM issue_labels WHERE label_id = ?1",
            params![label_id],
        )
        .await
    {
        Ok(mut rows) => rows
            .next()
            .await
            .ok()
            .flatten()
            .and_then(|row| row.i64(0).ok())
            .unwrap_or(0),
        Err(_) => 0,
    }
}

pub(super) async fn read_labels(db: &LocalDb) -> String {
    let labels = match read_resource(db, |conn| {
        Box::pin(async move {
            let labels = list_labels_conn(conn, DEFAULT_WORKSPACE_ID)
                .await
                .map_err(|error| storage_error("Failed to load labels", error))?;
            let mut counted = Vec::with_capacity(labels.len());
            for label in labels {
                let count = usage_count(conn, &label.id).await;
                counted.push((label, count));
            }
            Ok(counted)
        })
    })
    .await
    {
        Ok(labels) => labels,
        Err(error) => return error,
    };

    let mut out = "# Labels — workspace\n\n".to_string();
    if labels.is_empty() {
        out.push_str("No labels defined.\n\n");
    } else {
        for (label, count) in labels {
            out.push_str(&format!(
                "- [{}]({}) — {} issue(s) — `{}`\n",
                label.name,
                build_label_uri(&label.id),
                count,
                label.color
            ));
        }
        out.push('\n');
    }
    out
}

pub(super) async fn read_label(db: &LocalDb, label_id: &str) -> String {
    let requested = label_id.to_owned();
    let (label, count) = match read_resource(db, move |conn| {
        Box::pin(async move {
            let label = get_label_conn(conn, &requested)
                .await
                .map_err(|_| format!("Label not found: {requested}"))?;
            let count = usage_count(conn, &label.id).await;
            Ok((label, count))
        })
    })
    .await
    {
        Ok(found) => found,
        Err(error) => return error,
    };
    let mut out = format!("# Label `{}`\n\n", label.id);
    out.push_str(&format!("Name: {}\n", label.name));
    out.push_str(&format!("Color: `{}`\n", label.color));
    out.push_str(&format!("Issues: {}\n\n", count));
    out
}
