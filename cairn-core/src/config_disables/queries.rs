//! Queries over the `config_disables` table.

use std::collections::HashSet;

use cairn_db::turso::params;

use crate::storage::{LocalDb, RowExt};

/// One inherited artifact a project has disabled.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DisabledConfig {
    /// One of `recipe` | `agent` | `skill` | `action`.
    pub entity_type: String,
    /// The shadow key: id for file types, name for actions.
    pub config_key: String,
}

/// Disable an inherited workspace artifact for one project. Idempotent.
pub async fn disable_config(
    db: &LocalDb,
    project_id: &str,
    entity_type: &str,
    config_key: &str,
) -> Result<(), String> {
    let now = chrono::Utc::now().timestamp();
    let project_id = project_id.to_string();
    let entity_type = entity_type.to_string();
    let config_key = config_key.to_string();
    db.write(|conn| {
        let project_id = project_id.clone();
        let entity_type = entity_type.clone();
        let config_key = config_key.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT OR IGNORE INTO config_disables
                     (project_id, entity_type, config_key, created_at)
                 VALUES (?1, ?2, ?3, ?4)",
                params![
                    project_id.as_str(),
                    entity_type.as_str(),
                    config_key.as_str(),
                    now
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|e| format!("Failed to disable config: {e}"))
}

/// Re-enable a previously disabled inherited artifact for one project. Idempotent.
pub async fn enable_config(
    db: &LocalDb,
    project_id: &str,
    entity_type: &str,
    config_key: &str,
) -> Result<(), String> {
    let project_id = project_id.to_string();
    let entity_type = entity_type.to_string();
    let config_key = config_key.to_string();
    db.write(|conn| {
        let project_id = project_id.clone();
        let entity_type = entity_type.clone();
        let config_key = config_key.clone();
        Box::pin(async move {
            conn.execute(
                "DELETE FROM config_disables
                 WHERE project_id = ?1 AND entity_type = ?2 AND config_key = ?3",
                params![
                    project_id.as_str(),
                    entity_type.as_str(),
                    config_key.as_str()
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|e| format!("Failed to enable config: {e}"))
}

/// Move legacy skill/recipe/agent disables into the repository-owned project policy.
/// Rows are deleted only after the config save succeeds; action rows are untouched.
pub async fn migrate_contextual_disables(
    db: &LocalDb,
    project_id: &str,
    project_path: &std::path::Path,
) -> Result<usize, String> {
    let rows = list_disabled_configs(db, project_id).await?;
    let contextual: Vec<_> = rows
        .into_iter()
        .filter(|row| matches!(row.entity_type.as_str(), "skill" | "recipe" | "agent"))
        .collect();
    if contextual.is_empty() {
        return Ok(0);
    }

    let mut settings = crate::config::project_settings::load_project_settings(project_path);
    let policy = settings.contextual_packages.get_or_insert_default();
    for row in &contextual {
        let kind = match row.entity_type.as_str() {
            "skill" => crate::config::contextual_packages::ContextualPackageKind::Skill,
            "recipe" => crate::config::contextual_packages::ContextualPackageKind::Recipe,
            "agent" => crate::config::contextual_packages::ContextualPackageKind::Agent,
            _ => unreachable!(),
        };
        policy.disable(crate::config::contextual_packages::ContextualPackageRef {
            kind,
            id: row.config_key.clone(),
        });
    }
    policy.normalize()?;
    crate::config::project_settings::save_project_settings(project_path, &settings)?;

    for row in &contextual {
        enable_config(db, project_id, &row.entity_type, &row.config_key).await?;
    }
    Ok(contextual.len())
}

/// The set of disabled shadow keys for one (project, entity_type) — the hot path
/// for resolution filters.
pub async fn list_disabled_keys(
    db: &LocalDb,
    project_id: &str,
    entity_type: &str,
) -> Result<HashSet<String>, String> {
    let project_id = project_id.to_string();
    let entity_type = entity_type.to_string();
    let keys = db
        .query_all(
            "SELECT config_key FROM config_disables
             WHERE project_id = ?1 AND entity_type = ?2",
            params![project_id.as_str(), entity_type.as_str()],
            |row| row.text(0),
        )
        .await
        .map_err(|e| format!("Failed to list disabled keys: {e}"))?;
    Ok(keys.into_iter().collect())
}

/// Every disabled artifact for a project, across all entity types — for the
/// settings UI, which must *render* inherited items as disabled even though the
/// agent-facing resolution paths *hide* them.
pub async fn list_disabled_configs(
    db: &LocalDb,
    project_id: &str,
) -> Result<Vec<DisabledConfig>, String> {
    let project_id = project_id.to_string();
    db.query_all(
        "SELECT entity_type, config_key FROM config_disables
         WHERE project_id = ?1
         ORDER BY entity_type ASC, config_key ASC",
        params![project_id.as_str()],
        |row| {
            Ok(DisabledConfig {
                entity_type: row.text(0)?,
                config_key: row.text(1)?,
            })
        },
    )
    .await
    .map_err(|e| format!("Failed to list disabled configs: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{LocalDb, MigrationRunner, TURSO_MIGRATIONS};

    async fn db() -> LocalDb {
        let db = LocalDb::open(tempfile::tempdir().unwrap().keep().join("disables.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db
    }

    #[tokio::test]
    async fn disable_enable_roundtrip_is_scoped_and_idempotent() {
        let db = db().await;
        disable_config(&db, "proj", "skill", "ui").await.unwrap();
        // Idempotent: a second disable does not error or duplicate.
        disable_config(&db, "proj", "skill", "ui").await.unwrap();

        let keys = list_disabled_keys(&db, "proj", "skill").await.unwrap();
        assert!(keys.contains("ui"));
        assert_eq!(keys.len(), 1);

        // A different project does not see the disable.
        assert!(list_disabled_keys(&db, "other", "skill")
            .await
            .unwrap()
            .is_empty());
        // A different entity type in the same project does not see it.
        assert!(list_disabled_keys(&db, "proj", "agent")
            .await
            .unwrap()
            .is_empty());

        enable_config(&db, "proj", "skill", "ui").await.unwrap();
        assert!(list_disabled_keys(&db, "proj", "skill")
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn migration_moves_contextual_rows_and_preserves_actions() {
        let db = db().await;
        let project = tempfile::tempdir().unwrap();
        disable_config(&db, "p", "skill", "ui").await.unwrap();
        disable_config(&db, "p", "agent", "review").await.unwrap();
        disable_config(&db, "p", "action", "deploy").await.unwrap();

        assert_eq!(
            migrate_contextual_disables(&db, "p", project.path())
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            migrate_contextual_disables(&db, "p", project.path())
                .await
                .unwrap(),
            0
        );

        let settings = crate::config::project_settings::load_project_settings(project.path());
        let disabled = settings.contextual_packages.unwrap().disabled;
        assert_eq!(disabled.len(), 2);
        let remaining = list_disabled_configs(&db, "p").await.unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].entity_type, "action");
    }

    #[tokio::test]
    async fn list_disabled_configs_returns_all_entity_types_for_project() {
        let db = db().await;
        disable_config(&db, "p", "skill", "ui").await.unwrap();
        disable_config(&db, "p", "action", "deploy").await.unwrap();
        disable_config(&db, "other", "skill", "x").await.unwrap();

        let all = list_disabled_configs(&db, "p").await.unwrap();
        assert_eq!(all.len(), 2);
        assert!(all
            .iter()
            .any(|d| d.entity_type == "skill" && d.config_key == "ui"));
        assert!(all
            .iter()
            .any(|d| d.entity_type == "action" && d.config_key == "deploy"));
    }
}
