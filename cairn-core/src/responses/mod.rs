//! Pure Response definition parsing, template rendering, and output validation.

use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};
use std::time::Duration;

use crate::backends::{
    backend_for_name, CompletionError, CompletionMessage, CompletionRequest, CompletionRole,
};
use crate::models::{Model, Preset, PresetOptionValue};
use crate::orchestrator::Orchestrator;
use crate::storage::NewResponseInvocation;

#[derive(Debug, Clone)]
pub enum ResponseCaller {
    Internal {
        label: String,
    },
    Agent {
        label: Option<String>,
        run_id: String,
        project_id: Option<String>,
        project_path: Option<std::path::PathBuf>,
    },
    Workflow {
        label: Option<String>,
        run_id: String,
        project_id: Option<String>,
        project_path: Option<std::path::PathBuf>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResponseOutcome {
    pub seq: i64,
    pub text: String,
    pub parsed: Option<Value>,
    pub model: String,
    pub backend: String,
    pub latency_ms: u64,
    pub cost: Option<f64>,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ResponseError {
    #[error("unknown response '{0}'")]
    UnknownResponse(String),
    #[error("missing required variable '{0}'")]
    MissingVariable(String),
    #[error("undeclared variable '{0}'")]
    UndeclaredVariable(String),
    #[error("response backend is unavailable: {0}")]
    BackendUnavailable(String),
    #[error("response timed out")]
    Timeout,
    #[error("response output violated its contract: {0}")]
    ContractViolation(String),
    #[error("response failed: {0}")]
    Upstream(String),
}

impl ResponseCaller {
    fn fields(
        &self,
    ) -> (
        &'static str,
        Option<&str>,
        Option<&str>,
        Option<&str>,
        Option<&std::path::Path>,
    ) {
        match self {
            Self::Internal { label } => ("internal", Some(label.as_str()), None, None, None),
            Self::Agent {
                label,
                run_id,
                project_id,
                project_path,
            } => (
                "agent",
                label.as_deref(),
                Some(run_id.as_str()),
                project_id.as_deref(),
                project_path.as_deref(),
            ),
            Self::Workflow {
                label,
                run_id,
                project_id,
                project_path,
            } => (
                "workflow",
                label.as_deref(),
                Some(run_id.as_str()),
                project_id.as_deref(),
                project_path.as_deref(),
            ),
        }
    }
}

/// Invoke one named, tool-free model completion. This is the sole invocation
/// path used by Rust internals, the run adapter, and workflows.
pub async fn invoke(
    orch: &Orchestrator,
    id: &str,
    args: &Value,
    caller: ResponseCaller,
) -> Result<ResponseOutcome, ResponseError> {
    let (caller_kind, caller_label, caller_run_id, project_id, project_path) = caller.fields();
    let file = crate::config::responses::get_response(&orch.config_dir, id, project_path)
        .map_err(ResponseError::Upstream)?
        .ok_or_else(|| ResponseError::UnknownResponse(id.to_string()))?;
    let definition_project_id = if file.is_project_scoped {
        Some(
            project_id
                .ok_or_else(|| {
                    ResponseError::Upstream(
                        "project-scoped response invoked without a project id".into(),
                    )
                })?
                .to_string(),
        )
    } else {
        None
    };
    let scope_key = definition_project_id
        .as_ref()
        .map(|id| format!("project:{id}"))
        .unwrap_or_else(|| "workspace".into());
    let rendered = file
        .definition
        .render(args)
        .map_err(classify_render_error)?;
    let (model, extras, backend_name) = if let Some(model) = &file.definition.model {
        let preset = Preset {
            model: Model::new(model),
            options: file.definition.options.clone(),
        };
        (
            preset.model.clone(),
            preset.to_extras(),
            file.definition
                .backend
                .clone()
                .expect("validated exact pin"),
        )
    } else {
        let presets =
            crate::config::presets::load_effective_presets(&orch.config_dir, project_path);
        let tier = file.definition.tier.as_deref().unwrap_or("sm");
        let preset = crate::config::presets::resolve_preset(tier, &presets)
            .map_err(ResponseError::Upstream)?;
        (preset.model, preset.extras, preset.backend)
    };
    backend_for_name(Some(&backend_name))
        .is_available()
        .map_err(|e| ResponseError::BackendUnavailable(e.to_string()))?;

    let output_schema = match &file.definition.output {
        OutputContract::Named(name) if name == "text" => None,
        OutputContract::Named(name) => Some(
            crate::output_schemas::resolve_output_schema(
                None,
                &crate::models::OutputSchema::Preset(name.clone()),
            )
            .map_err(ResponseError::Upstream)?,
        ),
        OutputContract::Schema(schema) => Some(schema.clone()),
    };
    let mut messages = Vec::with_capacity(file.definition.examples.len() * 2 + 1);
    for example in &file.definition.examples {
        let prompt = file
            .definition
            .render(&Value::Object(example.input.clone()))
            .map_err(classify_render_error)?;
        messages.push(CompletionMessage {
            role: CompletionRole::User,
            content: prompt,
        });
        messages.push(CompletionMessage {
            role: CompletionRole::Assistant,
            content: example
                .output
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| example.output.to_string()),
        });
    }
    messages.push(CompletionMessage {
        role: CompletionRole::User,
        content: rendered.clone(),
    });
    let mut request = CompletionRequest {
        system: None,
        messages,
        model: model.to_string(),
        extras: serde_json::to_value(extras).unwrap_or(Value::Null),
        output_schema: output_schema.clone(),
        timeout: file.definition.timeout,
    };

    let mut terminal_error = None;
    let mut accepted = None;
    for attempt in 0..=1 {
        let request_for_attempt = request.clone();
        let backend_for_attempt = backend_name.clone();
        let orch_for_attempt = orch.clone();
        let completion = match tokio::task::spawn_blocking(move || {
            backend_for_name(Some(&backend_for_attempt))
                .complete(request_for_attempt, &orch_for_attempt)
        })
        .await
        {
            Ok(completion) => completion,
            Err(error) => {
                terminal_error = Some(ResponseError::Upstream(format!(
                    "completion task failed: {error}"
                )));
                break;
            }
        };
        match completion {
            Ok(outcome) => match file
                .definition
                .validate_output(&outcome.text, output_schema.as_ref())
            {
                Ok(validated) => {
                    let parsed = match validated {
                        ValidatedOutput::Text(_) => None,
                        ValidatedOutput::Json(v) => Some(v),
                    };
                    accepted = Some((outcome, parsed));
                    break;
                }
                Err(error) if attempt == 0 => {
                    request.messages.push(CompletionMessage {
                        role: CompletionRole::Assistant,
                        content: outcome.text,
                    });
                    request.messages.push(CompletionMessage {
                        role: CompletionRole::User,
                        content: format!("Your previous answer violated the required output contract: {error}. Return a corrected answer only."),
                    });
                    terminal_error = Some(ResponseError::ContractViolation(error))
                }
                Err(error) => terminal_error = Some(ResponseError::ContractViolation(error)),
            },
            Err(error) => {
                terminal_error = Some(map_completion_error(error));
                break;
            }
        }
    }

    let now = chrono::Utc::now().timestamp_millis();
    let args_json = serde_json::to_string(args).ok();
    match accepted {
        Some((outcome, parsed)) => {
            let record = crate::storage::insert_response_invocation(
                &orch.db.local,
                NewResponseInvocation {
                    response_id: id.to_string(),
                    scope_key: scope_key.clone(),
                    project_id: definition_project_id.clone(),
                    caller_kind: caller_kind.into(),
                    caller_label: caller_label.map(str::to_owned),
                    caller_run_id: caller_run_id.map(str::to_owned),
                    rendered_prompt: rendered,
                    args_json,
                    status: "ok".into(),
                    output_text: Some(outcome.text.clone()),
                    error: None,
                    model: Some(outcome.model.clone()),
                    backend: Some(backend_name),
                    latency_ms: Some(outcome.latency_ms as i64),
                    input_tokens: outcome.tokens.input.map(|v| v as i64),
                    output_tokens: outcome.tokens.output.map(|v| v as i64),
                    cost: outcome.cost,
                    created_at: now,
                },
            )
            .await
            .map_err(|error| {
                ResponseError::Upstream(format!("failed to persist response history: {error}"))
            })?;
            let result = ResponseOutcome {
                seq: record.seq,
                text: outcome.text,
                parsed,
                model: record
                    .model
                    .expect("successful invocation records its model"),
                backend: record
                    .backend
                    .expect("successful invocation records its backend"),
                latency_ms: record.latency_ms.unwrap_or_default() as u64,
                cost: record.cost,
            };
            Ok(result)
        }
        None => {
            let error = terminal_error
                .unwrap_or_else(|| ResponseError::Upstream("completion produced no result".into()));
            crate::storage::insert_response_invocation(
                &orch.db.local,
                NewResponseInvocation {
                    response_id: id.to_string(),
                    scope_key,
                    project_id: definition_project_id,
                    caller_kind: caller_kind.into(),
                    caller_label: caller_label.map(str::to_owned),
                    caller_run_id: caller_run_id.map(str::to_owned),
                    rendered_prompt: rendered,
                    args_json,
                    status: "failed".into(),
                    output_text: None,
                    error: Some(error.to_string()),
                    model: Some(model.to_string()),
                    backend: Some(backend_name),
                    latency_ms: None,
                    input_tokens: None,
                    output_tokens: None,
                    cost: None,
                    created_at: now,
                },
            )
            .await
            .map_err(|storage_error| {
                ResponseError::Upstream(format!(
                    "{error}; additionally failed to persist response history: {storage_error}"
                ))
            })?;
            Err(error)
        }
    }
}

fn classify_render_error(error: String) -> ResponseError {
    if let Some(name) = error
        .strip_prefix("Missing required variable '")
        .and_then(|s| s.strip_suffix('\''))
    {
        ResponseError::MissingVariable(name.into())
    } else if let Some(name) = error
        .strip_prefix("Undeclared variable '")
        .and_then(|s| s.strip_suffix('\''))
    {
        ResponseError::UndeclaredVariable(name.into())
    } else {
        ResponseError::Upstream(error)
    }
}

fn map_completion_error(error: CompletionError) -> ResponseError {
    match error {
        CompletionError::BackendUnavailable => {
            ResponseError::BackendUnavailable("one-shot completion is unsupported".into())
        }
        CompletionError::Timeout => ResponseError::Timeout,
        other => ResponseError::Upstream(other.to_string()),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ResponseDefinition {
    pub name: String,
    pub description: String,
    #[serde(default)]
    pub tier: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub backend: Option<String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub options: HashMap<String, PresetOptionValue>,
    #[serde(default)]
    pub variables: Vec<ResponseVariable>,
    #[serde(default)]
    pub output: OutputContract,
    #[serde(default = "default_timeout", with = "duration_seconds")]
    pub timeout: Duration,
    #[serde(default)]
    pub examples: Vec<ResponseExample>,
    #[serde(skip)]
    pub template: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ResponseVariable {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub default: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResponseExample {
    pub input: Map<String, Value>,
    pub output: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum OutputContract {
    Named(String),
    Schema(Value),
}

impl Default for OutputContract {
    fn default() -> Self {
        Self::Named("text".into())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ValidatedOutput {
    Text(String),
    Json(Value),
}

fn default_timeout() -> Duration {
    Duration::from_secs(15)
}

mod duration_seconds {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S: Serializer>(value: &Duration, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&format!("{}s", value.as_secs()))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
        let raw = String::deserialize(deserializer)?;
        let seconds = raw
            .strip_suffix('s')
            .ok_or_else(|| {
                serde::de::Error::custom("timeout must be expressed in seconds, for example 15s")
            })?
            .parse::<u64>()
            .map_err(serde::de::Error::custom)?;
        Ok(Duration::from_secs(seconds))
    }
}

pub fn parse_definition(markdown: &str) -> Result<ResponseDefinition, String> {
    let (frontmatter, template) = crate::markdown_frontmatter::split_yaml_frontmatter(markdown)?;
    let mut definition: ResponseDefinition = serde_yaml::from_str(frontmatter)
        .map_err(|e| format!("Failed to parse frontmatter: {e}"))?;
    definition.template = template;
    definition.validate()?;
    Ok(definition)
}

impl ResponseDefinition {
    pub fn validate(&self) -> Result<(), String> {
        if self.name.trim().is_empty() {
            return Err("Response name cannot be empty".into());
        }
        if self.description.trim().is_empty() {
            return Err("Response description cannot be empty".into());
        }
        if self.template.trim().is_empty() {
            return Err("Response template cannot be empty".into());
        }
        if self.model.is_some() && self.tier.is_some() {
            return Err("Response must declare either 'model' or 'tier', not both".into());
        }
        if self.model.is_some() && self.backend.is_none() {
            return Err("Response with an exact 'model' pin must declare 'backend'".into());
        }
        if self.model.is_none() && self.backend.is_some() {
            return Err("Response 'backend' is only valid with an exact 'model' pin".into());
        }

        let mut declared = HashSet::new();
        for variable in &self.variables {
            if variable.name.is_empty()
                || !variable
                    .name
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_')
            {
                return Err(format!("Invalid variable name '{}'", variable.name));
            }
            if !declared.insert(variable.name.as_str()) {
                return Err(format!("Duplicate variable '{}'", variable.name));
            }
            if variable.required && variable.default.is_some() {
                return Err(format!(
                    "Required variable '{}' cannot have a default",
                    variable.name
                ));
            }
        }
        for referenced in template_variables(&self.template) {
            if !declared.contains(referenced.as_str()) {
                return Err(format!(
                    "Template references undeclared variable '{referenced}'"
                ));
            }
        }
        for example in &self.examples {
            self.render(&Value::Object(example.input.clone()))?;
        }
        if let OutputContract::Named(name) = &self.output {
            if name != "text" && !crate::output_schemas::is_preset_schema(name) {
                return Err(format!("Unknown output contract '{name}'"));
            }
        } else if let OutputContract::Schema(schema) = &self.output {
            jsonschema::validator_for(schema)
                .map_err(|e| format!("Invalid output JSON Schema: {e}"))?;
        }
        Ok(())
    }

    pub fn render(&self, args: &Value) -> Result<String, String> {
        let args = args
            .as_object()
            .ok_or_else(|| "Response arguments must be a JSON object".to_string())?;
        let variables: HashMap<&str, &ResponseVariable> = self
            .variables
            .iter()
            .map(|v| (v.name.as_str(), v))
            .collect();
        for name in args.keys() {
            if !variables.contains_key(name.as_str()) {
                return Err(format!("Undeclared variable '{name}'"));
            }
        }
        let mut values = HashMap::new();
        for variable in &self.variables {
            match args.get(&variable.name).or(variable.default.as_ref()) {
                Some(value) => {
                    values.insert(variable.name.as_str(), render_value(value));
                }
                None if variable.required => {
                    return Err(format!("Missing required variable '{}'", variable.name))
                }
                None => {
                    values.insert(variable.name.as_str(), String::new());
                }
            }
        }
        Ok(template_regex()
            .replace_all(&self.template, |captures: &regex::Captures<'_>| {
                values
                    .get(captures.get(1).unwrap().as_str())
                    .cloned()
                    .unwrap_or_default()
            })
            .into_owned())
    }

    pub fn validate_output(
        &self,
        text: &str,
        preset_schema: Option<&Value>,
    ) -> Result<ValidatedOutput, String> {
        let schema = match &self.output {
            OutputContract::Named(name) if name == "text" => {
                return Ok(ValidatedOutput::Text(text.to_string()))
            }
            OutputContract::Named(_) => {
                preset_schema.ok_or_else(|| "Preset output schema was not resolved".to_string())?
            }
            OutputContract::Schema(schema) => schema,
        };
        let value: Value =
            serde_json::from_str(text).map_err(|e| format!("Output is not valid JSON: {e}"))?;
        let validator = jsonschema::validator_for(schema)
            .map_err(|e| format!("Invalid output JSON Schema: {e}"))?;
        validator
            .validate(&value)
            .map_err(|e| format!("Output contract violation: {e}"))?;
        Ok(ValidatedOutput::Json(value))
    }
}

fn template_regex() -> Regex {
    Regex::new(r"\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}").unwrap()
}
fn template_variables(template: &str) -> Vec<String> {
    template_regex()
        .captures_iter(template)
        .map(|c| c[1].to_string())
        .collect()
}
fn render_value(value: &Value) -> String {
    value
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn definition() -> ResponseDefinition {
        parse_definition("---\nname: Test\ndescription: test response\nvariables:\n  - name: required\n    required: true\n  - name: optional\n    default: fallback\noutput: text\n---\n{{required}} / {{ optional }}").unwrap()
    }

    #[test]
    fn renders_substitutions_and_defaults() {
        assert_eq!(
            definition().render(&json!({"required": "value"})).unwrap(),
            "value / fallback"
        );
    }

    #[test]
    fn rejects_missing_and_undeclared_arguments() {
        assert!(definition()
            .render(&json!({}))
            .unwrap_err()
            .contains("Missing required"));
        assert!(definition()
            .render(&json!({"required": "ok", "extra": true}))
            .unwrap_err()
            .contains("Undeclared"));
    }

    #[test]
    fn rejects_undeclared_template_variable() {
        let error =
            parse_definition("---\nname: Test\ndescription: test\n---\n{{missing}}").unwrap_err();
        assert!(error.contains("undeclared variable 'missing'"));
    }

    #[test]
    fn validates_text_and_json_schema_outputs() {
        assert_eq!(
            definition().validate_output("plain", None).unwrap(),
            ValidatedOutput::Text("plain".into())
        );
        let mut structured = definition();
        structured.output = OutputContract::Schema(
            json!({"type":"object","required":["answer"],"properties":{"answer":{"type":"string"}}}),
        );
        assert!(matches!(
            structured.validate_output(r#"{"answer":"yes"}"#, None),
            Ok(ValidatedOutput::Json(_))
        ));
        assert!(structured
            .validate_output(r#"{"answer":1}"#, None)
            .unwrap_err()
            .contains("violation"));
    }

    #[test]
    fn validates_exact_model_pins_and_defaults_to_sm_tier() {
        let defaulted = definition();
        assert_eq!(defaulted.tier.as_deref().unwrap_or("sm"), "sm");
        assert!(parse_definition(
            "---\nname: Test\ndescription: test\nmodel: exact\ntier: sm\nbackend: openrouter\n---\nprompt"
        ).unwrap_err().contains("either 'model' or 'tier'"));
        assert!(
            parse_definition("---\nname: Test\ndescription: test\nmodel: exact\n---\nprompt")
                .unwrap_err()
                .contains("must declare 'backend'")
        );
        let pinned = parse_definition(
            "---\nname: Test\ndescription: test\nmodel: exact\nbackend: openrouter\noptions:\n  reasoningEffort: low\n---\nprompt"
        ).unwrap();
        let preset = Preset {
            model: Model::new(pinned.model.unwrap()),
            options: pinned.options,
        };
        assert_eq!(preset.to_extras().reasoning_effort.as_deref(), Some("low"));
    }
}
