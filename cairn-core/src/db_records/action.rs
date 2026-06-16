//! Action config and action run models for database records

// ============================================================================
// Action Config models
// ============================================================================

#[derive(Debug)]
pub struct DbActionConfig {
    pub id: String,
    pub name: String,
    pub description: String,
    pub command_template: Option<String>,
    pub input_schema: Option<String>,
    pub output_schema: Option<String>,
    pub is_builtin: i32,
    pub workspace_id: Option<String>,
    pub project_id: Option<String>,
    pub created_at: i32,
    pub updated_at: i32,
    pub tool_name: Option<String>,
    pub tool_description: Option<String>,
}

#[derive(Debug)]
pub struct NewActionConfig<'a> {
    pub id: &'a str,
    pub name: &'a str,
    pub description: &'a str,
    pub command_template: Option<&'a str>,
    pub input_schema: Option<&'a str>,
    pub output_schema: Option<&'a str>,
    pub is_builtin: i32,
    pub workspace_id: Option<&'a str>,
    pub project_id: Option<&'a str>,
    pub created_at: i32,
    pub updated_at: i32,
    pub tool_name: Option<&'a str>,
    pub tool_description: Option<&'a str>,
}

#[derive(Debug, Default)]
pub struct UpdateActionConfigChangeset<'a> {
    pub name: Option<&'a str>,
    pub description: Option<&'a str>,
    pub command_template: Option<Option<&'a str>>,
    pub input_schema: Option<Option<&'a str>>,
    pub output_schema: Option<Option<&'a str>>,
    pub updated_at: Option<i32>,
}

// ============================================================================
// Action Run models
// ============================================================================

#[derive(Debug, Clone)]
pub struct DbActionRun {
    pub id: String,
    pub execution_id: String,
    pub recipe_node_id: String,
    pub action_config_id: String,
    pub issue_id: Option<String>,
    pub project_id: String,
    pub status: String,
    pub inputs: Option<String>,
    pub output: Option<String>,
    pub error_message: Option<String>,
    pub started_at: Option<i32>,
    pub completed_at: Option<i32>,
    pub created_at: i32,
    pub parent_job_id: Option<String>,
    pub uri_segment: Option<String>,
}

#[derive(Debug)]
pub struct NewActionRun<'a> {
    pub id: &'a str,
    pub execution_id: &'a str,
    pub recipe_node_id: &'a str,
    pub action_config_id: &'a str,
    pub issue_id: Option<&'a str>,
    pub project_id: &'a str,
    pub status: &'a str,
    pub inputs: Option<&'a str>,
    pub output: Option<&'a str>,
    pub error_message: Option<&'a str>,
    pub started_at: Option<i32>,
    pub completed_at: Option<i32>,
    pub created_at: i32,
    pub parent_job_id: Option<&'a str>,
    pub uri_segment: Option<&'a str>,
}

#[derive(Debug, Default)]
pub struct UpdateActionRunChangeset<'a> {
    pub status: Option<&'a str>,
    pub output: Option<Option<&'a str>>,
    pub error_message: Option<Option<&'a str>>,
    pub started_at: Option<Option<i32>>,
    pub completed_at: Option<Option<i32>>,
}
