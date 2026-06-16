//! Action configuration types for reusable action definitions.

use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::LazyLock;

/// Variable type for template parsing
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum VariableType {
    String,
    #[serde(rename = "string[]")]
    StringArray,
    Number,
    Boolean,
}

impl std::fmt::Display for VariableType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VariableType::String => write!(f, "string"),
            VariableType::StringArray => write!(f, "string[]"),
            VariableType::Number => write!(f, "number"),
            VariableType::Boolean => write!(f, "boolean"),
        }
    }
}

impl std::str::FromStr for VariableType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "string" => Ok(VariableType::String),
            "string[]" => Ok(VariableType::StringArray),
            "number" => Ok(VariableType::Number),
            "boolean" | "bool" => Ok(VariableType::Boolean),
            _ => Err(format!("Unknown variable type: {}", s)),
        }
    }
}

/// A variable extracted from a command template
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TemplateVariable {
    pub name: String,
    pub var_type: VariableType,
    pub required: bool,
}

/// Action configuration - a reusable action definition
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActionConfig {
    pub id: String,
    pub name: String,
    pub description: String,
    pub command_template: Option<String>,
    pub input_schema: Option<serde_json::Value>,
    pub output_schema: Option<serde_json::Value>,
    pub is_builtin: bool,
    pub workspace_id: Option<String>,
    pub project_id: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub tool_name: Option<String>,
    pub tool_description: Option<String>,
}

/// Input for creating an action config
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateActionConfig {
    pub name: String,
    pub description: Option<String>,
    pub command_template: String,
    pub input_schema: Option<serde_json::Value>,
    pub output_schema: Option<serde_json::Value>,
    pub workspace_id: Option<String>,
    pub project_id: Option<String>,
}

/// Input for updating an action config
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateActionConfig {
    pub name: Option<String>,
    pub description: Option<String>,
    pub command_template: Option<String>,
    pub input_schema: Option<serde_json::Value>,
    pub output_schema: Option<serde_json::Value>,
}

// Regex for parsing template variables: {{name:type}} or {{?name:type}}
static TEMPLATE_VAR_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\{\{(\?)?([a-zA-Z_][a-zA-Z0-9_]*):([a-zA-Z\[\]]+)\}\}").unwrap());

/// Parse a command template to extract variables.
///
/// Template syntax:
/// - `{{name:type}}` - required variable
/// - `{{?name:type}}` - optional variable
/// - Types: string, string[], number, boolean
///
/// Example: `gh pr create --title {{title:string}} --body {{?body:string}}`
pub fn parse_template(template: &str) -> Vec<TemplateVariable> {
    TEMPLATE_VAR_REGEX
        .captures_iter(template)
        .map(|cap| {
            let optional = cap.get(1).is_some();
            let name = cap.get(2).unwrap().as_str().to_string();
            let type_str = cap.get(3).unwrap().as_str();
            let var_type = type_str.parse().unwrap_or(VariableType::String);

            TemplateVariable {
                name,
                var_type,
                required: !optional,
            }
        })
        .collect()
}

/// Generate a JSON Schema from template variables.
pub fn generate_input_schema(vars: &[TemplateVariable]) -> serde_json::Value {
    let mut properties = serde_json::Map::new();
    let mut required = Vec::new();

    for var in vars {
        let type_def = match var.var_type {
            VariableType::String => serde_json::json!({"type": "string"}),
            VariableType::StringArray => {
                serde_json::json!({"type": "array", "items": {"type": "string"}})
            }
            VariableType::Number => serde_json::json!({"type": "number"}),
            VariableType::Boolean => serde_json::json!({"type": "boolean"}),
        };
        properties.insert(var.name.clone(), type_def);

        if var.required {
            required.push(serde_json::Value::String(var.name.clone()));
        }
    }

    serde_json::json!({
        "type": "object",
        "properties": properties,
        "required": required
    })
}

/// Interpolate a command template with input values.
///
/// Replaces `{{name:type}}` and `{{?name:type}}` with actual values.
/// For arrays, joins with spaces. For optional missing values, removes the placeholder.
pub fn interpolate_template(template: &str, inputs: &HashMap<String, serde_json::Value>) -> String {
    TEMPLATE_VAR_REGEX
        .replace_all(template, |caps: &regex::Captures| {
            let name = caps.get(2).unwrap().as_str();

            match inputs.get(name) {
                Some(value) => match value {
                    serde_json::Value::String(s) => shell_escape(s),
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::Bool(b) => b.to_string(),
                    serde_json::Value::Array(arr) => arr
                        .iter()
                        .filter_map(|v| v.as_str())
                        .map(shell_escape)
                        .collect::<Vec<_>>()
                        .join(" "),
                    _ => String::new(),
                },
                None => String::new(),
            }
        })
        .to_string()
}

/// Escape a string for safe shell usage.
fn shell_escape(s: &str) -> String {
    // If the string contains special characters, wrap in single quotes
    // and escape any single quotes within
    if s.chars()
        .any(|c| c.is_whitespace() || "\"'`$\\!&|;()<>".contains(c))
    {
        format!("'{}'", s.replace('\'', "'\\''"))
    } else {
        s.to_string()
    }
}

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

/// Action run status
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ActionRunStatus {
    Pending,
    Running,
    /// The action ran but is holding the DAG open pending external resolution
    /// (e.g. a `pr` node: the PR is open and awaits merge/close). Drives the
    /// `NeedsApproval` attention the same way a blocked job does, but without
    /// being a job. Resolves to `Complete` (or `Failed`) when the wait ends.
    Blocked,
    Complete,
    Failed,
}

impl std::fmt::Display for ActionRunStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ActionRunStatus::Pending => write!(f, "pending"),
            ActionRunStatus::Running => write!(f, "running"),
            ActionRunStatus::Blocked => write!(f, "blocked"),
            ActionRunStatus::Complete => write!(f, "complete"),
            ActionRunStatus::Failed => write!(f, "failed"),
        }
    }
}

impl std::str::FromStr for ActionRunStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "pending" => Ok(ActionRunStatus::Pending),
            "running" => Ok(ActionRunStatus::Running),
            "blocked" => Ok(ActionRunStatus::Blocked),
            "complete" => Ok(ActionRunStatus::Complete),
            "failed" => Ok(ActionRunStatus::Failed),
            _ => Err(format!("Unknown action run status: {}", s)),
        }
    }
}

/// An action run - tracks execution of an action node
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActionRun {
    pub id: String,
    pub execution_id: String,
    pub recipe_node_id: String,
    pub action_config_id: String,
    pub issue_id: Option<String>,
    pub project_id: String,
    pub status: ActionRunStatus,
    pub inputs: Option<String>,
    pub output: Option<String>,
    pub error_message: Option<String>,
    pub started_at: Option<i64>,
    pub completed_at: Option<i64>,
    pub created_at: i64,
    pub parent_job_id: Option<String>,
    /// Stable URI segment used to address this action node via cairn:// (e.g.
    /// `pr`). Allocated at insert time, deduped across jobs + action_runs for
    /// the execution. `None` only for rows that predate the column and could
    /// not be backfilled.
    pub uri_segment: Option<String>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_template_simple() {
        let template = "gh pr create --title {{title:string}}";
        let vars = parse_template(template);
        assert_eq!(vars.len(), 1);
        assert_eq!(vars[0].name, "title");
        assert_eq!(vars[0].var_type, VariableType::String);
        assert!(vars[0].required);
    }

    #[test]
    fn test_parse_template_optional() {
        let template = "echo {{?message:string}}";
        let vars = parse_template(template);
        assert_eq!(vars.len(), 1);
        assert!(!vars[0].required);
    }

    #[test]
    fn test_parse_template_multiple() {
        let template = "gh pr create --title {{title:string}} --body {{?body:string}}";
        let vars = parse_template(template);
        assert_eq!(vars.len(), 2);
        assert!(vars[0].required);
        assert!(!vars[1].required);
    }

    #[test]
    fn test_parse_template_array() {
        let template = "echo {{items:string[]}}";
        let vars = parse_template(template);
        assert_eq!(vars[0].var_type, VariableType::StringArray);
    }

    #[test]
    fn test_generate_input_schema() {
        let vars = vec![
            TemplateVariable {
                name: "title".to_string(),
                var_type: VariableType::String,
                required: true,
            },
            TemplateVariable {
                name: "body".to_string(),
                var_type: VariableType::String,
                required: false,
            },
        ];
        let schema = generate_input_schema(&vars);
        assert_eq!(schema["type"], "object");
        assert!(schema["properties"]["title"].is_object());
        assert_eq!(schema["required"], serde_json::json!(["title"]));
    }

    #[test]
    fn test_interpolate_template() {
        let template = "gh pr create --title {{title:string}}";
        let mut inputs = HashMap::new();
        inputs.insert(
            "title".to_string(),
            serde_json::Value::String("Fix bug".to_string()),
        );
        let result = interpolate_template(template, &inputs);
        assert_eq!(result, "gh pr create --title 'Fix bug'");
    }

    #[test]
    fn test_interpolate_template_array() {
        let template = "echo {{items:string[]}}";
        let mut inputs = HashMap::new();
        inputs.insert(
            "items".to_string(),
            serde_json::json!(["one", "two", "three"]),
        );
        let result = interpolate_template(template, &inputs);
        assert_eq!(result, "echo one two three");
    }
}
