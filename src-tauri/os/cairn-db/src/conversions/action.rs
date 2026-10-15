//! Database record conversion into domain values.
use cairn_domain::*;

/// Convert from database model to domain model
impl TryFrom<crate::db_records::DbActionConfig> for ActionConfig {
    type Error = String;

    fn try_from(db: crate::db_records::DbActionConfig) -> Result<Self, Self::Error> {
        let input_schema = db
            .input_schema
            .as_ref()
            .map(|s| serde_json::from_str(s))
            .transpose()
            .map_err(|e| format!("Invalid input_schema JSON: {}", e))?;

        let output_schema = db
            .output_schema
            .as_ref()
            .map(|s| serde_json::from_str(s))
            .transpose()
            .map_err(|e| format!("Invalid output_schema JSON: {}", e))?;

        Ok(ActionConfig {
            id: db.id,
            name: db.name,
            description: db.description,
            command_template: db.command_template,
            input_schema,
            output_schema,
            is_builtin: db.is_builtin != 0,
            workspace_id: db.workspace_id,
            project_id: db.project_id,
            created_at: db.created_at as i64,
            updated_at: db.updated_at as i64,
            tool_name: db.tool_name,
            tool_description: db.tool_description,
        })
    }
}

/// Convert from database model to domain model
impl TryFrom<crate::db_records::DbActionRun> for ActionRun {
    type Error = String;

    fn try_from(db: crate::db_records::DbActionRun) -> Result<Self, Self::Error> {
        let status: ActionRunStatus = db
            .status
            .parse()
            .map_err(|e: String| format!("Invalid action run status: {}", e))?;

        Ok(ActionRun {
            id: db.id,
            execution_id: db.execution_id,
            recipe_node_id: db.recipe_node_id,
            action_config_id: db.action_config_id,
            issue_id: db.issue_id,
            project_id: db.project_id,
            status,
            inputs: db.inputs,
            output: db.output,
            error_message: db.error_message,
            started_at: db.started_at.map(|t| t as i64),
            completed_at: db.completed_at.map(|t| t as i64),
            created_at: db.created_at as i64,
            parent_job_id: db.parent_job_id,
            uri_segment: db.uri_segment,
        })
    }
}
