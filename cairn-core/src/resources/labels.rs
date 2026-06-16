//! Label resource reads.

use cairn_common::uri::build_label_uri;
use turso::params;

use super::common::{connect_for_read, storage_error};
use crate::labels::crud::{get_label_conn, list_labels_conn, DEFAULT_WORKSPACE_ID};
use crate::storage::{LocalDb, RowExt};

async fn usage_count(conn: &turso::Connection, label_id: &str) -> i64 {
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
    let conn = match connect_for_read(db).await {
        Ok(conn) => conn,
        Err(error) => return error,
    };
    let labels = match list_labels_conn(&conn, DEFAULT_WORKSPACE_ID).await {
        Ok(labels) => labels,
        Err(error) => return storage_error("Failed to load labels", error),
    };

    let mut out = "# Labels — workspace\n\n".to_string();
    if labels.is_empty() {
        out.push_str("No labels defined.\n\n");
    } else {
        for label in labels {
            let count = usage_count(&conn, &label.id).await;
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
    let conn = match connect_for_read(db).await {
        Ok(conn) => conn,
        Err(error) => return error,
    };
    let label = match get_label_conn(&conn, label_id).await {
        Ok(label) => label,
        Err(_) => return format!("Label not found: {label_id}"),
    };
    let count = usage_count(&conn, &label.id).await;
    let mut out = format!("# Label `{}`\n\n", label.id);
    out.push_str(&format!("Name: {}\n", label.name));
    out.push_str(&format!("Color: `{}`\n", label.color));
    out.push_str(&format!("Issues: {}\n\n", count));
    out
}
