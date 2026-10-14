use std::collections::{HashMap, HashSet};

use cairn_db::turso::params;

use crate::labels::crud::{
    create_label_conn, label_from_row, label_from_row_at, slugify, DEFAULT_WORKSPACE_ID,
};
use crate::models::{CreateLabel, Label};
use crate::storage::{DbResult, RowExt};

/// Find the label a reference names: its id, its display name
/// case-insensitively, or the slug those words produce. The slug arm is what
/// keeps prose and slug spellings of the same label ("Needs Review",
/// "needs-review") on one row instead of minting near-duplicates.
async fn find_label_ref(
    conn: &cairn_db::turso::Connection,
    workspace_id: &str,
    value: &str,
) -> Result<Option<Label>, String> {
    // `slugify` falls back to the literal "label" for a reference with no
    // alphanumerics, which would bind such a reference to an unrelated label
    // that happens to own that id. Only offer the slug arm when the reference
    // genuinely derives one.
    let slug = slugify(value);
    let slug = value
        .chars()
        .any(|ch| ch.is_ascii_alphanumeric())
        .then_some(slug.as_str());
    let mut rows = conn
        .query(
            "SELECT id, workspace_id, name, color, created_at, updated_at
             FROM labels
             WHERE workspace_id = ?1 AND (id = ?2 OR name = ?2 COLLATE NOCASE OR id = ?3)
             ORDER BY CASE
                 WHEN id = ?2 THEN 0
                 WHEN name = ?2 COLLATE NOCASE THEN 1
                 ELSE 2
             END, id ASC
             LIMIT 1",
            params![workspace_id, value, slug],
        )
        .await
        .map_err(|error| error.to_string())?;
    rows.next()
        .await
        .map_err(|error| error.to_string())?
        .map(|row| label_from_row(&row).map_err(|error| error.to_string()))
        .transpose()
}

/// Resolve a label reference, creating the label when nothing matches.
///
/// A label is a descriptor coined in the same breath as the issue it describes,
/// so a name the vocabulary has not seen yet is a new label rather than a failed
/// write. Returns the label and whether this call minted it.
async fn resolve_or_create_label_ref(
    conn: &cairn_db::turso::Connection,
    workspace_id: &str,
    label_ref: &str,
    now: i64,
) -> Result<(Label, bool), String> {
    let value = label_ref.trim();
    if value.is_empty() {
        return Err("label references must be non-empty strings".to_string());
    }
    if let Some(label) = find_label_ref(conn, workspace_id, value).await? {
        return Ok((label, false));
    }
    let label = create_label_conn(
        conn,
        workspace_id,
        CreateLabel {
            name: value.to_string(),
            color: None,
        },
        now,
    )
    .await?;
    Ok((label, true))
}

pub(crate) async fn list_labels_for_issue(
    conn: &cairn_db::turso::Connection,
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

pub(crate) async fn list_labels_for_issues(
    conn: &cairn_db::turso::Connection,
    issue_ids: &[String],
) -> DbResult<HashMap<String, Vec<Label>>> {
    if issue_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let issue_ids_json = serde_json::to_string(issue_ids).map_err(|error| {
        crate::storage::DbError::internal(format!("failed to serialize issue ids: {error}"))
    })?;
    let mut rows = conn
        .query(
            "SELECT il.issue_id,
                    l.id, l.workspace_id, l.name, l.color, l.created_at, l.updated_at
             FROM issue_labels il
             JOIN labels l ON l.id = il.label_id
             WHERE il.issue_id IN (SELECT value FROM json_each(?1))
             ORDER BY il.issue_id ASC, LOWER(l.name) ASC",
            params![issue_ids_json],
        )
        .await?;
    let mut labels = HashMap::<String, Vec<Label>>::new();
    while let Some(row) = rows.next().await? {
        let issue_id = row.text(0)?;
        labels
            .entry(issue_id)
            .or_default()
            .push(label_from_row_at(&row, 1)?);
    }
    Ok(labels)
}

/// Replace an issue's labels with `refs`, creating any label the workspace
/// vocabulary does not have yet.
///
/// Returns the labels minted along the way so callers that notify the UI can
/// refresh the vocabulary, not just the issue's chips.
pub(crate) async fn replace_issue_labels(
    conn: &cairn_db::turso::Connection,
    issue_id: &str,
    refs: &[String],
    now: i64,
) -> Result<Vec<Label>, String> {
    let mut label_ids = Vec::with_capacity(refs.len());
    let mut created = Vec::new();
    let mut seen = HashSet::new();
    for label_ref in refs {
        let (label, was_created) =
            resolve_or_create_label_ref(conn, DEFAULT_WORKSPACE_ID, label_ref, now).await?;
        if was_created {
            created.push(label.clone());
        }
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
    Ok(created)
}

/// Add labels without disturbing labels already attached to the issue.
pub(crate) async fn add_issue_labels(
    conn: &cairn_db::turso::Connection,
    issue_id: &str,
    refs: &[String],
    now: i64,
) -> Result<Vec<Label>, String> {
    let mut combined = list_labels_for_issue(conn, issue_id)
        .await
        .map_err(|error| error.to_string())?
        .into_iter()
        .map(|label| label.id)
        .collect::<Vec<_>>();
    combined.extend_from_slice(refs);
    replace_issue_labels(conn, issue_id, &combined, now).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::labels::crud::{create_label_conn, list_labels_conn, DEFAULT_WORKSPACE_ID};
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

    async fn seed_issue(conn: &cairn_db::turso::Connection, issue_id: &str, number: i32) {
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

    async fn seed_label(conn: &cairn_db::turso::Connection, name: &str) -> String {
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

    async fn label_ids_for_issue(
        conn: &cairn_db::turso::Connection,
        issue_id: &str,
    ) -> Vec<String> {
        list_labels_for_issue(conn, issue_id)
            .await
            .unwrap()
            .into_iter()
            .map(|label| label.id)
            .collect()
    }

    #[tokio::test]
    async fn resolves_label_by_id_name_and_slug() {
        let db = test_db().await;
        db.write(|conn| {
            Box::pin(async move {
                let id = seed_label(conn, "Needs Review").await;
                // A slug-only label: its id is the `slugify` fallback, so an
                // unslugifiable reference must not resolve to it.
                seed_label(conn, "label").await;
                for reference in [id.as_str(), "needs review", "Needs-Review"] {
                    let (label, created) =
                        resolve_or_create_label_ref(conn, DEFAULT_WORKSPACE_ID, reference, 3)
                            .await
                            .unwrap();
                    assert_eq!(label.id, id, "reference '{reference}'");
                    assert!(!created, "reference '{reference}' should reuse the label");
                }

                let (label, created) =
                    resolve_or_create_label_ref(conn, DEFAULT_WORKSPACE_ID, "🎉", 3)
                        .await
                        .unwrap();
                assert!(created, "an unslugifiable reference is its own label");
                assert_eq!(label.name, "🎉");
                assert_eq!(label.id, "label-2");
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn rejects_blank_label_refs_without_creating_anything() {
        let db = test_db().await;
        db.write(|conn| {
            Box::pin(async move {
                let error = resolve_or_create_label_ref(conn, DEFAULT_WORKSPACE_ID, "   ", 3)
                    .await
                    .unwrap_err();
                assert!(error.contains("non-empty"));
                assert!(list_labels_conn(conn, DEFAULT_WORKSPACE_ID)
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
    async fn replace_issue_labels_dedupes_and_clears() {
        let db = test_db().await;
        db.write(|conn| {
            Box::pin(async move {
                seed_issue(conn, "i-one", 1).await;
                seed_label(conn, "Bug").await;
                seed_label(conn, "UI").await;

                let created = replace_issue_labels(
                    conn,
                    "i-one",
                    &["bug".to_string(), "UI".to_string(), "Bug".to_string()],
                    3,
                )
                .await
                .unwrap();
                assert!(created.is_empty());
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
    async fn attaching_an_unknown_label_creates_it() {
        let db = test_db().await;
        db.write(|conn| {
            Box::pin(async move {
                seed_issue(conn, "i-one", 1).await;
                seed_label(conn, "Bug").await;

                let created = replace_issue_labels(
                    conn,
                    "i-one",
                    &["bug".to_string(), "execution-fabric".to_string()],
                    3,
                )
                .await
                .unwrap();
                assert_eq!(created.len(), 1);
                assert_eq!(created[0].id, "execution-fabric");
                assert_eq!(created[0].name, "execution-fabric");
                assert_eq!(
                    label_ids_for_issue(conn, "i-one").await,
                    vec!["bug".to_string(), "execution-fabric".to_string()]
                );

                // The same label named as prose resolves to the row the first
                // attach minted instead of minting a near-duplicate.
                let created_again =
                    replace_issue_labels(conn, "i-one", &["Execution Fabric".to_string()], 4)
                        .await
                        .unwrap();
                assert!(created_again.is_empty());
                assert_eq!(
                    label_ids_for_issue(conn, "i-one").await,
                    vec!["execution-fabric".to_string()]
                );
                assert_eq!(
                    list_labels_conn(conn, DEFAULT_WORKSPACE_ID)
                        .await
                        .unwrap()
                        .len(),
                    2
                );
                Ok(())
            })
        })
        .await
        .unwrap();
    }
}
