//! Action configuration database queries.

use turso::params;
use uuid::Uuid;

use crate::models::{
    generate_input_schema, parse_template, ActionConfig, CreateActionConfig, UpdateActionConfig,
};
use crate::storage::{DbError, DbResult, LocalDb, RowExt};

const ACTION_CONFIG_COLUMNS: &str = "
    id, name, description, command_template, input_schema, output_schema,
    is_builtin, workspace_id, project_id, created_at, updated_at, tool_name, tool_description
";

fn parse_json_option(value: Option<String>, label: &str) -> DbResult<Option<serde_json::Value>> {
    value
        .as_deref()
        .map(serde_json::from_str)
        .transpose()
        .map_err(|error| DbError::Row(format!("invalid {label} JSON: {error}")))
}

fn action_config_from_row(row: &turso::Row) -> DbResult<ActionConfig> {
    let input_schema = parse_json_option(row.opt_text(4)?, "input_schema")?;
    let output_schema = parse_json_option(row.opt_text(5)?, "output_schema")?;

    Ok(ActionConfig {
        id: row.text(0)?,
        name: row.text(1)?,
        description: row.text(2)?,
        command_template: row.opt_text(3)?,
        input_schema,
        output_schema,
        is_builtin: row.i64(6)? != 0,
        workspace_id: row.opt_text(7)?,
        project_id: row.opt_text(8)?,
        created_at: row.i64(9)?,
        updated_at: row.i64(10)?,
        tool_name: row.opt_text(11)?,
        tool_description: row.opt_text(12)?,
    })
}

async fn load_action_config_conn(
    conn: &turso::Connection,
    id: &str,
) -> DbResult<Option<ActionConfig>> {
    let mut rows = conn
        .query(
            &format!("SELECT {ACTION_CONFIG_COLUMNS} FROM action_configs WHERE id = ?1"),
            params![id],
        )
        .await?;
    rows.next()
        .await?
        .map(|row| action_config_from_row(&row))
        .transpose()
}

pub async fn list_action_configs(
    db: &LocalDb,
    workspace_id: Option<&str>,
    project_id: Option<&str>,
    include_builtins: bool,
) -> Result<Vec<ActionConfig>, String> {
    let workspace_id = workspace_id.map(str::to_string);
    let project_id = project_id.map(str::to_string);
    db.query_all(
        format!(
            "
            SELECT {ACTION_CONFIG_COLUMNS}
            FROM action_configs
            WHERE (?1 IS NULL OR workspace_id = ?1)
              AND (?2 IS NULL OR project_id = ?2)
              AND (?3 = 1 OR is_builtin = 0)
            ORDER BY name ASC
            "
        ),
        params![
            workspace_id.as_deref(),
            project_id.as_deref(),
            if include_builtins { 1_i64 } else { 0_i64 }
        ],
        action_config_from_row,
    )
    .await
    .map_err(|e| format!("Failed to query action_configs: {e}"))
}

/// Resolve the effective action set for a project: workspace actions inherited,
/// project actions shadowing by name, then per-project disabled names removed.
///
/// This is the action analog of the file-type `list_for_context` shadowing. The
/// raw [`list_action_configs`] stays a literal per-scope query for the editor;
/// callers that want the agent-facing effective set use this resolver.
pub async fn list_action_configs_for_context(
    db: &LocalDb,
    project_id: &str,
    include_builtins: bool,
) -> Result<Vec<ActionConfig>, String> {
    use std::collections::HashMap;

    let mut by_name: HashMap<String, ActionConfig> = HashMap::new();
    for action in list_action_configs(db, Some("default"), None, include_builtins).await? {
        by_name.insert(action.name.clone(), action);
    }
    // Project actions shadow workspace actions of the same name.
    for action in list_action_configs(db, None, Some(project_id), include_builtins).await? {
        by_name.insert(action.name.clone(), action);
    }

    let disabled = crate::config_disables::list_disabled_keys(db, project_id, "action").await?;

    let mut actions: Vec<ActionConfig> = by_name
        .into_values()
        .filter(|action| !disabled.contains(&action.name))
        .collect();
    actions.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(actions)
}

/// Copy an existing action (from any scope) into a target scope under a chosen
/// name. Same name as an inherited workspace action + project target shadows it;
/// a new name is additive. Hard copy: a fresh UUID, no link to the source.
pub async fn copy_action_config(
    db: &LocalDb,
    source_id: &str,
    target_name: &str,
    target_project_id: Option<&str>,
) -> Result<ActionConfig, String> {
    let source = get_action_config(db, source_id)
        .await?
        .ok_or_else(|| format!("Action config not found: {source_id}"))?;

    let now = chrono::Utc::now().timestamp();
    let id = Uuid::new_v4().to_string();
    let (workspace_id, project_id) = match target_project_id {
        Some(pid) => (None, Some(pid.to_string())),
        None => (Some("default".to_string()), None),
    };

    let input_schema_str = source
        .input_schema
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|e| format!("Failed to serialize input_schema: {e}"))?;
    let output_schema_str = source
        .output_schema
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|e| format!("Failed to serialize output_schema: {e}"))?;

    let target_name = target_name.to_string();
    db.write(|conn| {
        let id = id.clone();
        let target_name = target_name.clone();
        let description = source.description.clone();
        let command_template = source.command_template.clone();
        let input_schema_str = input_schema_str.clone();
        let output_schema_str = output_schema_str.clone();
        let tool_name = source.tool_name.clone();
        let tool_description = source.tool_description.clone();
        let workspace_id = workspace_id.clone();
        let project_id = project_id.clone();
        Box::pin(async move {
            conn.execute(
                "
                INSERT INTO action_configs (
                    id, name, description, command_template, input_schema, output_schema,
                    is_builtin, workspace_id, project_id, created_at, updated_at,
                    tool_name, tool_description
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0, ?7, ?8, ?9, ?10, ?11, ?12)
                ",
                params![
                    id.as_str(),
                    target_name.as_str(),
                    description.as_str(),
                    command_template.as_deref(),
                    input_schema_str.as_deref(),
                    output_schema_str.as_deref(),
                    workspace_id.as_deref(),
                    project_id.as_deref(),
                    now,
                    now,
                    tool_name.as_deref(),
                    tool_description.as_deref()
                ],
            )
            .await?;
            load_action_config_conn(conn, &id)
                .await?
                .ok_or_else(|| DbError::internal(format!("copied action config missing: {id}")))
        })
    })
    .await
    .map_err(|e| format!("Failed to copy action_config: {e}"))
}

pub async fn get_action_config(db: &LocalDb, id: &str) -> Result<Option<ActionConfig>, String> {
    let id = id.to_string();
    db.read(|conn| Box::pin(async move { load_action_config_conn(conn, &id).await }))
        .await
        .map_err(|e| format!("Failed to query action_config: {e}"))
}

pub async fn create_action_config(
    db: &LocalDb,
    input: CreateActionConfig,
) -> Result<ActionConfig, String> {
    if input.workspace_id.is_none() == input.project_id.is_none() {
        return Err("Exactly one of workspace_id or project_id must be set".to_string());
    }

    let now = chrono::Utc::now().timestamp();
    let id = Uuid::new_v4().to_string();
    let input_schema = if input.input_schema.is_some() {
        input.input_schema.clone()
    } else {
        let vars = parse_template(&input.command_template);
        if vars.is_empty() {
            None
        } else {
            Some(generate_input_schema(&vars))
        }
    };

    let input_schema_str = input_schema
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|e| format!("Failed to serialize input_schema: {e}"))?;

    let output_schema_str = input
        .output_schema
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|e| format!("Failed to serialize output_schema: {e}"))?;

    db.write(|conn| {
        let id = id.clone();
        let input = input.clone();
        let input_schema_str = input_schema_str.clone();
        let output_schema_str = output_schema_str.clone();
        Box::pin(async move {
            conn.execute(
                "
                INSERT INTO action_configs (
                    id, name, description, command_template, input_schema, output_schema,
                    is_builtin, workspace_id, project_id, created_at, updated_at,
                    tool_name, tool_description
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0, ?7, ?8, ?9, ?10, NULL, NULL)
                ",
                params![
                    id.as_str(),
                    input.name.as_str(),
                    input.description.as_deref().unwrap_or(""),
                    input.command_template.as_str(),
                    input_schema_str.as_deref(),
                    output_schema_str.as_deref(),
                    input.workspace_id.as_deref(),
                    input.project_id.as_deref(),
                    now,
                    now
                ],
            )
            .await?;
            load_action_config_conn(conn, &id)
                .await?
                .ok_or_else(|| DbError::internal(format!("created action config missing: {id}")))
        })
    })
    .await
    .map_err(|e| format!("Failed to insert action_config: {e}"))
}

pub async fn update_action_config(
    db: &LocalDb,
    id: &str,
    input: UpdateActionConfig,
) -> Result<ActionConfig, String> {
    let now = chrono::Utc::now().timestamp();
    let id = id.to_string();

    let input_schema_str = if let Some(ref template) = input.command_template {
        if input.input_schema.is_none() {
            let vars = parse_template(template);
            if vars.is_empty() {
                None
            } else {
                Some(
                    serde_json::to_string(&generate_input_schema(&vars))
                        .map_err(|e| format!("Failed to serialize input_schema: {e}"))?,
                )
            }
        } else {
            input
                .input_schema
                .as_ref()
                .map(serde_json::to_string)
                .transpose()
                .map_err(|e| format!("Failed to serialize input_schema: {e}"))?
        }
    } else {
        input
            .input_schema
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| format!("Failed to serialize input_schema: {e}"))?
    };

    let output_schema_str = input
        .output_schema
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|e| format!("Failed to serialize output_schema: {e}"))?;

    db.write(|conn| {
        let id = id.clone();
        let input = input.clone();
        let input_schema_str = input_schema_str.clone();
        let output_schema_str = output_schema_str.clone();
        Box::pin(async move {
            let existing = load_action_config_conn(conn, &id)
                .await?
                .ok_or_else(|| DbError::internal(format!("Action config not found: {id}")))?;

            if existing.is_builtin {
                return Err(DbError::internal(
                    "Cannot update built-in action configurations",
                ));
            }

            conn.execute(
                "
                UPDATE action_configs
                SET name = COALESCE(?1, name),
                    description = COALESCE(?2, description),
                    command_template = COALESCE(?3, command_template),
                    input_schema = COALESCE(?4, input_schema),
                    output_schema = COALESCE(?5, output_schema),
                    updated_at = ?6
                WHERE id = ?7
                ",
                params![
                    input.name.as_deref(),
                    input.description.as_deref(),
                    input.command_template.as_deref(),
                    input_schema_str.as_deref(),
                    output_schema_str.as_deref(),
                    now,
                    id.as_str()
                ],
            )
            .await?;

            load_action_config_conn(conn, &id)
                .await?
                .ok_or_else(|| DbError::internal(format!("updated action config missing: {id}")))
        })
    })
    .await
    .map_err(|e| format!("Failed to update action_config: {e}"))
}

pub async fn delete_action_config(db: &LocalDb, id: &str) -> Result<(), String> {
    let id = id.to_string();
    db.write(|conn| {
        let id = id.clone();
        Box::pin(async move {
            let existing = load_action_config_conn(conn, &id)
                .await?
                .ok_or_else(|| DbError::internal(format!("Action config not found: {id}")))?;

            if existing.is_builtin {
                return Err(DbError::internal(
                    "Cannot delete built-in action configurations",
                ));
            }

            conn.execute(
                "DELETE FROM action_configs WHERE id = ?1",
                params![id.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|e| format!("Failed to delete action_config: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::CreateActionConfig;
    use crate::storage::{LocalDb, MigrationRunner, TURSO_MIGRATIONS};

    async fn db() -> LocalDb {
        let db = LocalDb::open(tempfile::tempdir().unwrap().keep().join("actions.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db
    }

    async fn seed_project(db: &LocalDb, id: &str) {
        let id = id.to_string();
        db.write(|conn| {
            let id = id.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at, is_workspace)
                     VALUES (?1, 'default', ?1, ?1, '/tmp/proj', 1, 1, 0)",
                    (id.as_str(),),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    fn input(name: &str, template: &str, project_id: Option<&str>) -> CreateActionConfig {
        CreateActionConfig {
            name: name.to_string(),
            description: Some(format!("desc:{name}:{}", project_id.unwrap_or("ws"))),
            command_template: template.to_string(),
            input_schema: None,
            output_schema: None,
            workspace_id: project_id.is_none().then(|| "default".to_string()),
            project_id: project_id.map(str::to_string),
        }
    }

    #[tokio::test]
    async fn project_action_shadows_workspace_by_name() {
        let db = db().await;
        seed_project(&db, "p1").await;
        create_action_config(&db, input("deploy", "echo ws", None))
            .await
            .unwrap();
        create_action_config(&db, input("deploy", "echo proj", Some("p1")))
            .await
            .unwrap();
        create_action_config(&db, input("build", "echo build", None))
            .await
            .unwrap();

        let ctx = list_action_configs_for_context(&db, "p1", false)
            .await
            .unwrap();
        // Inherited workspace `build` plus the single shadowed `deploy`.
        assert_eq!(ctx.len(), 2);
        let deploy: Vec<_> = ctx.iter().filter(|a| a.name == "deploy").collect();
        assert_eq!(deploy.len(), 1);
        assert_eq!(deploy[0].command_template.as_deref(), Some("echo proj"));
    }

    #[tokio::test]
    async fn disable_hides_workspace_action_for_one_project_only() {
        let db = db().await;
        create_action_config(&db, input("deploy", "echo ws", None))
            .await
            .unwrap();
        crate::config_disables::disable_config(&db, "p1", "action", "deploy")
            .await
            .unwrap();

        let p1 = list_action_configs_for_context(&db, "p1", false)
            .await
            .unwrap();
        assert!(!p1.iter().any(|a| a.name == "deploy"));

        let p2 = list_action_configs_for_context(&db, "p2", false)
            .await
            .unwrap();
        assert!(p2.iter().any(|a| a.name == "deploy"));
    }

    #[tokio::test]
    async fn copy_action_new_name_is_additive_same_name_into_project_shadows() {
        let db = db().await;
        seed_project(&db, "p1").await;
        let ws = create_action_config(&db, input("deploy", "echo ws", None))
            .await
            .unwrap();

        // New name in workspace: an independent, additive action with a fresh id.
        let additive = copy_action_config(&db, &ws.id, "deploy-staging", None)
            .await
            .unwrap();
        assert_eq!(additive.name, "deploy-staging");
        assert_ne!(additive.id, ws.id);
        assert_eq!(additive.command_template.as_deref(), Some("echo ws"));
        assert!(!additive.is_builtin);

        // Same name into a project: shadows the inherited workspace action.
        let shadow = copy_action_config(&db, &ws.id, "deploy", Some("p1"))
            .await
            .unwrap();
        assert_eq!(shadow.project_id.as_deref(), Some("p1"));
        let ctx = list_action_configs_for_context(&db, "p1", false)
            .await
            .unwrap();
        assert_eq!(ctx.iter().filter(|a| a.name == "deploy").count(), 1);
    }
}
