use std::collections::HashSet;

use turso::params;

use crate::labels::crud::{label_from_row, list_labels_conn, DEFAULT_WORKSPACE_ID};
use crate::models::Label;
use crate::storage::{DbResult, RowExt};

pub async fn resolve_label_ref(
    conn: &turso::Connection,
    workspace_id: &str,
    label_ref: &str,
) -> Result<Label, String> {
    let value = label_ref.trim();
    if value.is_empty() {
        return Err("label references must be non-empty strings".to_string());
    }
    let mut rows = conn
        .query(
            "SELECT id, workspace_id, name, color, created_at, updated_at
             FROM labels
             WHERE workspace_id = ?1 AND (id = ?2 OR name = ?2 COLLATE NOCASE)
             ORDER BY id ASC
             LIMIT 1",
            params![workspace_id, value],
        )
        .await
        .map_err(|error| error.to_string())?;
    if let Some(row) = rows.next().await.map_err(|error| error.to_string())? {
        return label_from_row(&row).map_err(|error| error.to_string());
    }

    let labels = list_labels_conn(conn, workspace_id)
        .await
        .map_err(|error| error.to_string())?;
    let available = if labels.is_empty() {
        "none".to_string()
    } else {
        labels
            .iter()
            .map(|label| label.id.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    };
    Err(format!(
        "Unknown label '{value}'. Available: {available}. Create it: write({{changes:[{{target:\"cairn://labels\",mode:\"create\",payload:{{name:\"{value}\"}}}}]}})"
    ))
}

pub async fn list_labels_for_issue(
    conn: &turso::Connection,
    issue_id: &str,
) -> DbResult<Vec<Label>> {
    let mut rows = conn
        .query(
            "SELECT l.id, l.workspace_id, l.name, l.color, l.created_at, l.updated_at
             FROM issue_labels il
             JOIN labels l ON l.id = il.label_id
             WHERE il.issue_id = ?1
             ORDER BY LOWER(l.name) ASC",
            params![issue_id],
        )
        .await?;
    let mut labels = Vec::new();
    while let Some(row) = rows.next().await? {
        labels.push(label_from_row(&row)?);
    }
    Ok(labels)
}

pub async fn replace_issue_labels(
    conn: &turso::Connection,
    issue_id: &str,
    refs: &[String],
    now: i64,
) -> Result<Vec<String>, String> {
    let mut label_ids = Vec::with_capacity(refs.len());
    let mut seen = HashSet::new();
    for label_ref in refs {
        let label = resolve_label_ref(conn, DEFAULT_WORKSPACE_ID, label_ref).await?;
        if seen.insert(label.id.clone()) {
            label_ids.push(label.id);
        }
    }

    conn.execute(
        "DELETE FROM issue_labels WHERE issue_id = ?1",
        params![issue_id],
    )
    .await
    .map_err(|error| error.to_string())?;
    for label_id in &label_ids {
        conn.execute(
            "INSERT INTO issue_labels (issue_id, label_id, created_at) VALUES (?1, ?2, ?3)",
            params![issue_id, label_id.as_str(), now],
        )
        .await
        .map_err(|error| error.to_string())?;
    }
    Ok(label_ids)
}

pub async fn list_issue_ids_for_label(
    conn: &turso::Connection,
    label_ref: &str,
) -> Result<Vec<String>, String> {
    let label = resolve_label_ref(conn, DEFAULT_WORKSPACE_ID, label_ref).await?;
    let mut rows = conn
        .query(
            "SELECT DISTINCT issue_id FROM issue_labels WHERE label_id = ?1 ORDER BY issue_id ASC",
            params![label.id.as_str()],
        )
        .await
        .map_err(|error| error.to_string())?;
    let mut issue_ids = Vec::new();
    while let Some(row) = rows.next().await.map_err(|error| error.to_string())? {
        issue_ids.push(row.text(0).map_err(|error| error.to_string())?);
    }
    Ok(issue_ids)
}

pub async fn hydrate_labels(
    conn: &turso::Connection,
    issue: &mut crate::models::Issue,
) -> DbResult<()> {
    issue.labels = list_labels_for_issue(conn, &issue.id).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::labels::crud::{create_label_conn, DEFAULT_WORKSPACE_ID};
    use crate::models::CreateLabel;
    use crate::storage::{LocalDb, MigrationRunner, TURSO_MIGRATIONS};
    use tempfile::tempdir;

    async fn test_db() -> LocalDb {
        let temp = tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("labels-attach.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db
    }

    async fn seed_issue(conn: &turso::Connection, issue_id: &str, number: i32) {
        conn.execute(
            "INSERT OR IGNORE INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-labels', 'default', 'Labels', 'LBL', '/tmp/lbl', 1, 1)",
            (),
        )
        .await
        .unwrap();
        conn.execute(
            "INSERT INTO issues (id, project_id, number, title, description, status, progress, attention, priority, created_at, updated_at) VALUES (?1, 'p-labels', ?2, 'Issue', '', 'backlog', 'backlog', 'none', 0, 1, 1)",
            params![issue_id, number],
        )
        .await
        .unwrap();
    }

    async fn seed_label(conn: &turso::Connection, name: &str) -> String {
        create_label_conn(
            conn,
            DEFAULT_WORKSPACE_ID,
            CreateLabel {
                name: name.to_string(),
                color: None,
            },
            2,
        )
        .await
        .unwrap()
        .id
    }

    #[tokio::test]
    async fn resolves_label_by_id_and_name() {
        let db = test_db().await;
        db.write(|conn| {
            Box::pin(async move {
                let id = seed_label(conn, "Needs Review").await;
                assert_eq!(
                    resolve_label_ref(conn, DEFAULT_WORKSPACE_ID, &id)
                        .await
                        .unwrap()
                        .id,
                    id
                );
                assert_eq!(
                    resolve_label_ref(conn, DEFAULT_WORKSPACE_ID, "needs review")
                        .await
                        .unwrap()
                        .id,
                    id
                );
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn replace_issue_labels_dedupes_and_clears() {
        let db = test_db().await;
        db.write(|conn| {
            Box::pin(async move {
                seed_issue(conn, "i-one", 1).await;
                seed_label(conn, "Bug").await;
                seed_label(conn, "UI").await;

                let replaced = replace_issue_labels(
                    conn,
                    "i-one",
                    &["bug".to_string(), "UI".to_string(), "Bug".to_string()],
                    3,
                )
                .await
                .unwrap();
                assert_eq!(replaced, vec!["bug", "ui"]);
                assert_eq!(list_labels_for_issue(conn, "i-one").await.unwrap().len(), 2);

                replace_issue_labels(conn, "i-one", &[], 4).await.unwrap();
                assert!(list_labels_for_issue(conn, "i-one")
                    .await
                    .unwrap()
                    .is_empty());
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn unknown_label_error_lists_available_and_create_action() {
        let db = test_db().await;
        db.write(|conn| {
            Box::pin(async move {
                seed_label(conn, "Bug").await;
                seed_label(conn, "UI").await;
                let error = resolve_label_ref(conn, DEFAULT_WORKSPACE_ID, "frontend")
                    .await
                    .unwrap_err();
                assert!(error.contains("Unknown label 'frontend'"));
                assert!(error.contains("Available: bug, ui"));
                assert!(error.contains("target:\"cairn://labels\""));
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn list_issue_ids_for_label_returns_reverse_lookup() {
        let db = test_db().await;
        db.write(|conn| {
            Box::pin(async move {
                seed_issue(conn, "i-one", 1).await;
                seed_issue(conn, "i-two", 2).await;
                seed_label(conn, "Bug").await;
                replace_issue_labels(conn, "i-one", &["bug".to_string()], 3)
                    .await
                    .unwrap();
                replace_issue_labels(conn, "i-two", &["Bug".to_string()], 3)
                    .await
                    .unwrap();
                assert_eq!(
                    list_issue_ids_for_label(conn, "bug").await.unwrap(),
                    vec!["i-one".to_string(), "i-two".to_string()]
                );
                Ok(())
            })
        })
        .await
        .unwrap();
    }
}
