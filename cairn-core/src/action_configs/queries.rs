//! Action configuration database queries.

use diesel::prelude::*;

use crate::diesel_models::{DbActionConfig, NewActionConfig, UpdateActionConfigChangeset};
use crate::models::{
    generate_input_schema, parse_template, ActionConfig, CreateActionConfig, UpdateActionConfig,
};
use crate::schema::action_configs;

/// List action configurations with optional scope and builtin filters.
pub fn list_action_configs(
    conn: &mut SqliteConnection,
    workspace_id: Option<&str>,
    project_id: Option<&str>,
    include_builtins: bool,
) -> Result<Vec<ActionConfig>, String> {
    let mut query = action_configs::table.into_boxed();

    if let Some(wid) = workspace_id {
        query = query.filter(action_configs::workspace_id.eq(wid));
    }

    if let Some(pid) = project_id {
        query = query.filter(action_configs::project_id.eq(pid));
    }

    if !include_builtins {
        query = query.filter(action_configs::is_builtin.eq(0));
    }

    let results: Vec<DbActionConfig> = query
        .order(action_configs::name.asc())
        .load(conn)
        .map_err(|e| format!("Failed to query action_configs: {}", e))?;

    results.into_iter().map(ActionConfig::try_from).collect()
}

/// Get a single action configuration by ID.
pub fn get_action_config(
    conn: &mut SqliteConnection,
    id: &str,
) -> Result<Option<ActionConfig>, String> {
    let result: Option<DbActionConfig> = action_configs::table
        .find(id)
        .first(conn)
        .optional()
        .map_err(|e| format!("Failed to query action_config: {}", e))?;

    result.map(ActionConfig::try_from).transpose()
}

/// Create a new action configuration.
pub fn create_action_config(
    conn: &mut SqliteConnection,
    input: CreateActionConfig,
) -> Result<ActionConfig, String> {
    if input.workspace_id.is_none() == input.project_id.is_none() {
        return Err("Exactly one of workspace_id or project_id must be set".to_string());
    }

    let now = chrono::Utc::now().timestamp() as i32;
    let id = uuid::Uuid::new_v4().to_string();

    let input_schema = if input.input_schema.is_some() {
        input.input_schema
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
        .map_err(|e| format!("Failed to serialize input_schema: {}", e))?;

    let output_schema_str = input
        .output_schema
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|e| format!("Failed to serialize output_schema: {}", e))?;

    let new_action = NewActionConfig {
        id: &id,
        name: &input.name,
        description: input.description.as_deref().unwrap_or(""),
        command_template: Some(&input.command_template),
        input_schema: input_schema_str.as_deref(),
        output_schema: output_schema_str.as_deref(),
        is_builtin: 0,
        workspace_id: input.workspace_id.as_deref(),
        project_id: input.project_id.as_deref(),
        created_at: now,
        updated_at: now,
        tool_name: None,
        tool_description: None,
    };

    diesel::insert_into(action_configs::table)
        .values(&new_action)
        .execute(conn)
        .map_err(|e| format!("Failed to insert action_config: {}", e))?;

    let created: DbActionConfig = action_configs::table
        .find(&id)
        .first(conn)
        .map_err(|e| format!("Failed to fetch created action_config: {}", e))?;

    ActionConfig::try_from(created)
}

/// Update an existing action configuration.
pub fn update_action_config(
    conn: &mut SqliteConnection,
    id: &str,
    input: UpdateActionConfig,
) -> Result<ActionConfig, String> {
    let existing: DbActionConfig = action_configs::table
        .find(id)
        .first(conn)
        .map_err(|e| format!("Action config not found: {}", e))?;

    if existing.is_builtin != 0 {
        return Err("Cannot update built-in action configurations".to_string());
    }

    let now = chrono::Utc::now().timestamp() as i32;

    let input_schema_str = if let Some(ref template) = input.command_template {
        if input.input_schema.is_none() {
            let vars = parse_template(template);
            if vars.is_empty() {
                None
            } else {
                Some(
                    serde_json::to_string(&generate_input_schema(&vars))
                        .map_err(|e| format!("Failed to serialize input_schema: {}", e))?,
                )
            }
        } else {
            input
                .input_schema
                .as_ref()
                .map(serde_json::to_string)
                .transpose()
                .map_err(|e| format!("Failed to serialize input_schema: {}", e))?
        }
    } else {
        input
            .input_schema
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| format!("Failed to serialize input_schema: {}", e))?
    };

    let output_schema_str = input
        .output_schema
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|e| format!("Failed to serialize output_schema: {}", e))?;

    let changeset = UpdateActionConfigChangeset {
        name: input.name.as_deref(),
        description: input.description.as_deref(),
        command_template: input.command_template.as_deref().map(Some),
        input_schema: input_schema_str.as_deref().map(Some),
        output_schema: output_schema_str.as_deref().map(Some),
        updated_at: Some(now),
    };

    diesel::update(action_configs::table.find(id))
        .set(&changeset)
        .execute(conn)
        .map_err(|e| format!("Failed to update action_config: {}", e))?;

    let updated: DbActionConfig = action_configs::table
        .find(id)
        .first(conn)
        .map_err(|e| format!("Failed to fetch updated action_config: {}", e))?;

    ActionConfig::try_from(updated)
}

/// Delete an action configuration.
pub fn delete_action_config(conn: &mut SqliteConnection, id: &str) -> Result<(), String> {
    let existing: DbActionConfig = action_configs::table
        .find(id)
        .first(conn)
        .map_err(|e| format!("Action config not found: {}", e))?;

    if existing.is_builtin != 0 {
        return Err("Cannot delete built-in action configurations".to_string());
    }

    diesel::delete(action_configs::table.find(id))
        .execute(conn)
        .map_err(|e| format!("Failed to delete action_config: {}", e))?;

    Ok(())
}
