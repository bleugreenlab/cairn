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
