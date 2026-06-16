use turso::params;

use crate::models::{CreateLabel, Label, UpdateLabel};
use crate::storage::{DbError, DbResult, LocalDb, RowExt};

pub const DEFAULT_WORKSPACE_ID: &str = "default";
pub const PALETTE: &[&str] = &[
    "#6B8F71", "#B7791F", "#8B5CF6", "#0F766E", "#BE5A38", "#64748B", "#A16207", "#7C3AED",
    "#15803D", "#B45309", "#0369A1", "#BE123C",
];

pub(crate) fn label_from_row(row: &turso::Row) -> DbResult<Label> {
    Ok(Label {
        id: row.text(0)?,
        workspace_id: row.text(1)?,
        name: row.text(2)?,
        color: row.text(3)?,
        created_at: row.i64(4)?,
        updated_at: row.i64(5)?,
    })
}

pub fn slugify(text: &str) -> String {
    const MAX_LEN: usize = 48;
    let mut slug = String::new();
    let mut prev_dash = false;
    for ch in text.chars() {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch.to_ascii_lowercase());
            prev_dash = false;
        } else if !slug.is_empty() && !prev_dash {
            slug.push('-');
            prev_dash = true;
        }
        if slug.len() >= MAX_LEN {
            break;
        }
    }
    while slug.ends_with('-') {
        slug.pop();
    }
    if slug.is_empty() {
        slug.push_str("label");
    }
    slug
}

pub fn default_color_for(name: &str) -> String {
    let mut hash: usize = 0;
    for byte in name.as_bytes() {
        hash = hash.wrapping_mul(31).wrapping_add(*byte as usize);
    }
    PALETTE[hash % PALETTE.len()].to_string()
}

pub fn validate_color(color: &str) -> Result<String, String> {
    let value = color.trim();
    if value.len() == 7
        && value.starts_with('#')
        && value[1..].chars().all(|ch| ch.is_ascii_hexdigit())
    {
        Ok(format!("#{}", value[1..].to_ascii_uppercase()))
    } else {
        Err(format!("invalid label color '{color}'; expected #RRGGBB"))
    }
}

async fn label_id_exists_conn(conn: &turso::Connection, id: &str) -> DbResult<bool> {
    let mut rows = conn
        .query("SELECT 1 FROM labels WHERE id = ?1 LIMIT 1", params![id])
        .await?;
    Ok(rows.next().await?.is_some())
}

async fn unique_label_id_conn(conn: &turso::Connection, base: &str) -> DbResult<String> {
    let mut candidate = base.to_string();
    let mut n = 2;
    while label_id_exists_conn(conn, &candidate).await? {
        candidate = format!("{base}-{n}");
        n += 1;
    }
    Ok(candidate)
}

pub async fn get_label_conn(conn: &turso::Connection, label_id: &str) -> DbResult<Label> {
    let mut rows = conn
        .query(
            "SELECT id, workspace_id, name, color, created_at, updated_at FROM labels WHERE id = ?1 LIMIT 1",
            params![label_id],
        )
        .await?;
    rows.next()
        .await?
        .map(|row| label_from_row(&row))
        .transpose()?
        .ok_or_else(|| DbError::Row(format!("Label not found: {label_id}")))
}

pub async fn list_labels_conn(
    conn: &turso::Connection,
    workspace_id: &str,
) -> DbResult<Vec<Label>> {
    let mut rows = conn
        .query(
            "SELECT id, workspace_id, name, color, created_at, updated_at FROM labels WHERE workspace_id = ?1 ORDER BY LOWER(name) ASC",
            params![workspace_id],
        )
        .await?;
    let mut labels = Vec::new();
    while let Some(row) = rows.next().await? {
        labels.push(label_from_row(&row)?);
    }
    Ok(labels)
}

pub async fn create_label_conn(
    conn: &turso::Connection,
    workspace_id: &str,
    input: CreateLabel,
    now: i64,
) -> Result<Label, String> {
    let name = input.name.trim();
    if name.is_empty() {
        return Err("payload.name is required and must be a non-empty string".to_string());
    }
    let color = match input.color {
        Some(color) => validate_color(&color)?,
        None => default_color_for(name),
    };
    let id = unique_label_id_conn(conn, &slugify(name))
        .await
        .map_err(|error| error.to_string())?;
    conn.execute(
        "INSERT INTO labels (id, workspace_id, name, color, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?5)",
        params![id.as_str(), workspace_id, name, color.as_str(), now],
    )
    .await
    .map_err(|error| {
        if error.to_string().to_ascii_lowercase().contains("unique") {
            format!("label name already exists: {name}")
        } else {
            error.to_string()
        }
    })?;
    get_label_conn(conn, &id)
        .await
        .map_err(|error| error.to_string())
}

pub async fn update_label_conn(
    conn: &turso::Connection,
    label_id: &str,
    input: UpdateLabel,
    now: i64,
) -> Result<Label, String> {
    let name = match input.name {
        Some(name) => {
            let trimmed = name.trim().to_string();
            if trimmed.is_empty() {
                return Err("payload.name must be a non-empty string".to_string());
            }
            Some(trimmed)
        }
        None => None,
    };
    let color = match input.color {
        Some(color) => Some(validate_color(&color)?),
        None => None,
    };
    conn.execute(
        "UPDATE labels SET name = COALESCE(?1, name), color = COALESCE(?2, color), updated_at = ?3 WHERE id = ?4",
        params![name.as_deref(), color.as_deref(), now, label_id],
    )
    .await
    .map_err(|error| {
        if error.to_string().to_ascii_lowercase().contains("unique") {
            format!("label name already exists: {}", name.as_deref().unwrap_or(label_id))
        } else {
            error.to_string()
        }
    })?;
    get_label_conn(conn, label_id)
        .await
        .map_err(|error| error.to_string())
}

pub async fn delete_label_conn(conn: &turso::Connection, label_id: &str) -> DbResult<()> {
    conn.execute("DELETE FROM labels WHERE id = ?1", params![label_id])
        .await?;
    Ok(())
}

pub async fn list_labels(db: &LocalDb) -> Result<Vec<Label>, String> {
    db.read(|conn| Box::pin(async move { list_labels_conn(conn, DEFAULT_WORKSPACE_ID).await }))
        .await
        .map_err(|error| error.to_string())
}

pub async fn create_label(db: &LocalDb, input: CreateLabel) -> Result<Label, String> {
    let now = chrono::Utc::now().timestamp();
    db.write(|conn| {
        let input = input.clone();
        Box::pin(async move {
            create_label_conn(conn, DEFAULT_WORKSPACE_ID, input, now)
                .await
                .map_err(DbError::Row)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

pub async fn update_label(
    db: &LocalDb,
    label_id: &str,
    input: UpdateLabel,
) -> Result<Label, String> {
    let now = chrono::Utc::now().timestamp();
    let label_id = label_id.to_string();
    db.write(|conn| {
        let label_id = label_id.clone();
        let input = input.clone();
        Box::pin(async move {
            update_label_conn(conn, &label_id, input, now)
                .await
                .map_err(DbError::Row)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

pub async fn delete_label(db: &LocalDb, label_id: &str) -> Result<(), String> {
    let label_id = label_id.to_string();
    db.write(|conn| {
        let label_id = label_id.clone();
        Box::pin(async move { delete_label_conn(conn, &label_id).await })
    })
    .await
    .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{LocalDb, MigrationRunner, TURSO_MIGRATIONS};
    use tempfile::tempdir;

    async fn test_db() -> LocalDb {
        let temp = tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("labels-crud.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db
    }

    async fn seed_issue(conn: &turso::Connection, issue_id: &str) {
        conn.execute(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-labels', 'default', 'Labels', 'LBL', '/tmp/lbl', 1, 1)",
            (),
        )
        .await
        .unwrap();
        conn.execute(
            "INSERT INTO issues (id, project_id, number, title, description, status, progress, attention, priority, created_at, updated_at) VALUES (?1, 'p-labels', 1, 'Issue', '', 'backlog', 'backlog', 'none', 0, 1, 1)",
            params![issue_id],
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn create_assigns_slug_and_default_color() {
        let db = test_db().await;
        let label = create_label(
            &db,
            CreateLabel {
                name: "Needs Review".to_string(),
                color: None,
            },
        )
        .await
        .unwrap();

        assert_eq!(label.id, "needs-review");
        assert_eq!(label.name, "Needs Review");
        assert!(PALETTE.contains(&label.color.as_str()));
    }

    #[tokio::test]
    async fn rejects_duplicate_names_and_invalid_colors() {
        let db = test_db().await;
        create_label(
            &db,
            CreateLabel {
                name: "Bug".to_string(),
                color: Some("#ff00aa".to_string()),
            },
        )
        .await
        .unwrap();

        let duplicate = create_label(
            &db,
            CreateLabel {
                name: "bug".to_string(),
                color: None,
            },
        )
        .await
        .unwrap_err();
        assert!(duplicate.contains("label name already exists"));

        let invalid = create_label(
            &db,
            CreateLabel {
                name: "Invalid".to_string(),
                color: Some("red".to_string()),
            },
        )
        .await
        .unwrap_err();
        assert!(invalid.contains("expected #RRGGBB"));
    }

    #[tokio::test]
    async fn update_renames_without_changing_id() {
        let db = test_db().await;
        let label = create_label(
            &db,
            CreateLabel {
                name: "Needs Review".to_string(),
                color: None,
            },
        )
        .await
        .unwrap();

        let updated = update_label(
            &db,
            &label.id,
            UpdateLabel {
                name: Some("Ready".to_string()),
                color: Some("#123abc".to_string()),
            },
        )
        .await
        .unwrap();

        assert_eq!(updated.id, "needs-review");
        assert_eq!(updated.name, "Ready");
        assert_eq!(updated.color, "#123ABC");
    }

    #[tokio::test]
    async fn delete_cascades_issue_labels() {
        let db = test_db().await;
        db.write(|conn| {
            Box::pin(async move {
                seed_issue(conn, "i-labeled").await;
                create_label_conn(
                    conn,
                    DEFAULT_WORKSPACE_ID,
                    CreateLabel {
                        name: "Bug".to_string(),
                        color: None,
                    },
                    2,
                )
                .await
                .unwrap();
                conn.execute(
                    "INSERT INTO issue_labels (issue_id, label_id, created_at) VALUES ('i-labeled', 'bug', 3)",
                    (),
                )
                .await
                .unwrap();
                delete_label_conn(conn, "bug").await.unwrap();
                let mut rows = conn
                    .query("SELECT 1 FROM issue_labels WHERE issue_id = 'i-labeled'", ())
                    .await
                    .unwrap();
                assert!(rows.next().await.unwrap().is_none());
                Ok(())
            })
        })
        .await
        .unwrap();
    }
}
