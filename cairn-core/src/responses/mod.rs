//! Pure Response definition parsing, template rendering, and output validation.

use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
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

fn saved_discovery_selection(
    backend: &str,
    model: &str,
    discovery_error: Option<&str>,
) -> Option<ResponseModelSelection> {
    discovery_error.map(|reason| ResponseModelSelection {
        kind: "model".into(),
        value: model.into(),
        label: format!("Exact · {model}"),
        backend: backend.into(),
        model: model.into(),
        legacy_values: Vec::new(),
        runnable: false,
        unavailable_reason: Some(format!("model discovery failed: {reason}")),
    })
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ResponseModelCapabilities {
    pub backends: Vec<ResponseBackendCapability>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ResponseBackendCapability {
    pub backend: String,
    pub label: String,
    pub runnable: bool,
    pub unavailable_reason: Option<String>,
    pub selections: Vec<ResponseModelSelection>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ResponseModelSelection {
    pub kind: String,
    pub value: String,
    pub label: String,
    pub backend: String,
    pub model: String,
    pub legacy_values: Vec<String>,
    pub runnable: bool,
    pub unavailable_reason: Option<String>,
}

/// Backend-owned response capability projected with the effective presets and
/// discovered models that can be authored in this scope.
pub fn model_capabilities(
    orch: &Orchestrator,
    project_id: Option<&str>,
    project_path: Option<&std::path::Path>,
    saved: Option<&ResponseDefinition>,
) -> ResponseModelCapabilities {
    let presets = crate::config::presets::load_effective_presets(&orch.config_dir, project_path);
    let catalog = orch.get_model_catalog();
    let mut backends = Vec::new();
    for backend_name in crate::backends::KNOWN_BACKENDS.iter().copied() {
        let backend = backend_for_name(Some(backend_name));
        let availability = backend.response_completion_availability(orch, project_id);
        let mut selections = Vec::new();
        if let Some(backend_presets) = presets.backends.get(backend_name) {
            for tier in &presets.tiers {
                if let Some(preset) = backend_presets.get(tier) {
                    let model_availability = backend.response_model_availability(
                        orch,
                        project_id,
                        preset.model.as_str(),
                    );
                    let legacy_values = crate::config::presets::resolve_preset(tier, &presets)
                        .ok()
                        .filter(|resolved| resolved.backend == backend_name)
                        .map(|_| vec![tier.clone()])
                        .unwrap_or_default();
                    selections.push(ResponseModelSelection {
                        kind: "tier".into(),
                        value: format!("{backend_name}/{tier}"),
                        label: format!("{} · {}", tier.to_uppercase(), preset.model),
                        backend: backend_name.into(),
                        model: preset.model.to_string(),
                        legacy_values,
                        runnable: availability.is_ok() && model_availability.is_ok(),
                        unavailable_reason: model_availability.err(),
                    });
                }
            }
        }
        if let Some(entry) = catalog
            .iter()
            .find(|entry| entry.backend == backend_name && entry.error.is_none())
        {
            for model in entry.models.iter().filter(|model| !model.hidden) {
                let model_availability =
                    backend.response_model_availability(orch, project_id, &model.model);
                selections.push(ResponseModelSelection {
                    kind: "model".into(),
                    value: model.model.clone(),
                    label: format!("Exact · {}", model.display_name),
                    backend: backend_name.into(),
                    model: model.model.clone(),
                    legacy_values: Vec::new(),
                    runnable: availability.is_ok() && model_availability.is_ok(),
                    unavailable_reason: model_availability.err(),
                });
            }
        }
        if let Some(saved_model) = saved
            .filter(|definition| definition.backend.as_deref() == Some(backend_name))
            .and_then(|definition| definition.model.as_deref())
        {
            let already_represented = selections
                .iter()
                .any(|selection| selection.kind == "model" && selection.model == saved_model);
            let discovery_error = catalog
                .iter()
                .find(|entry| entry.backend == backend_name)
                .and_then(|entry| entry.error.as_deref());
            if !already_represented {
                if let Some(selection) =
                    saved_discovery_selection(backend_name, saved_model, discovery_error)
                {
                    selections.push(selection);
                }
            }
        }
        backends.push(ResponseBackendCapability {
            backend: backend_name.into(),
            label: backend.name().into(),
            runnable: availability.is_ok(),
            unavailable_reason: availability.err(),
            selections,
        });
    }

    ResponseModelCapabilities { backends }
}

pub fn validate_runnable_model(
    orch: &Orchestrator,
    definition: &ResponseDefinition,
    project_id: Option<&str>,
    project_path: Option<&std::path::Path>,
) -> Result<(), String> {
    let capabilities = model_capabilities(orch, project_id, project_path, Some(definition));
    if is_offered_model_selection(definition, &capabilities) {
        Ok(())
    } else {
        Err("Model configuration is not a runnable selection".into())
    }
}

fn is_offered_model_selection(
    definition: &ResponseDefinition,
    capabilities: &ResponseModelCapabilities,
) -> bool {
    if let Some(model) = &definition.model {
        let backend = definition.backend.as_deref().unwrap_or_default();
        capabilities.backends.iter().any(|candidate| {
            candidate.backend == backend
                && candidate.runnable
                && candidate.selections.iter().any(|selection| {
                    selection.runnable && selection.kind == "model" && selection.model == *model
                })
        })
    } else {
        let tier = definition.tier.as_deref().unwrap_or("sm");
        capabilities.backends.iter().any(|candidate| {
            candidate.runnable
                && candidate.selections.iter().any(|selection| {
                    selection.runnable
                        && selection.kind == "tier"
                        && (selection.value == tier
                            || selection.legacy_values.iter().any(|legacy| legacy == tier))
                })
        })
    }
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

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ResponseTestOutcome {
    pub text: String,
    pub parsed: Option<Value>,
    pub model: String,
    pub backend: String,
    pub latency_ms: u64,
    pub cost: Option<f64>,
    #[serde(skip)]
    input_tokens: Option<u64>,
    #[serde(skip)]
    output_tokens: Option<u64>,
}

struct CompletionAttempt {
    rendered: String,
    model: Model,
    backend: String,
    result: Result<ResponseTestOutcome, ResponseError>,
}

/// Complete a supplied response definition without loading or persisting config,
/// response history, route state, or any other invocation record.
pub async fn test_definition(
    orch: &Orchestrator,
    definition: &ResponseDefinition,
    args: &Value,
    project_id: Option<&str>,
    project_path: Option<&std::path::Path>,
) -> Result<ResponseTestOutcome, ResponseError> {
    definition.validate().map_err(ResponseError::Upstream)?;
    validate_runnable_model(orch, definition, project_id, project_path)
        .map_err(ResponseError::BackendUnavailable)?;
    complete_definition(orch, definition, args, project_id, project_path)
        .await?
        .result
}

/// Invoke one named, tool-free model completion. Named invocations journal their
/// outcome after running through the same one-shot pipeline used by draft tests.
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
    let attempt =
        complete_definition(orch, &file.definition, args, project_id, project_path).await?;
    let now = chrono::Utc::now().timestamp_millis();
    let args_json = serde_json::to_string(args).ok();

    match attempt.result {
        Ok(outcome) => {
            let record = crate::storage::insert_response_invocation(
                &orch.db.local,
                NewResponseInvocation {
                    response_id: id.to_string(),
                    scope_key,
                    project_id: definition_project_id,
                    caller_kind: caller_kind.into(),
                    caller_label: caller_label.map(str::to_owned),
                    caller_run_id: caller_run_id.map(str::to_owned),
                    rendered_prompt: attempt.rendered,
                    args_json,
                    status: "ok".into(),
                    output_text: Some(outcome.text.clone()),
                    error: None,
                    model: Some(outcome.model.clone()),
                    backend: Some(outcome.backend.clone()),
                    latency_ms: Some(outcome.latency_ms as i64),
                    input_tokens: outcome.input_tokens.map(|value| value as i64),
                    output_tokens: outcome.output_tokens.map(|value| value as i64),
                    cost: outcome.cost,
                    created_at: now,
                },
            )
            .await
            .map_err(|error| {
                ResponseError::Upstream(format!("failed to persist response history: {error}"))
            })?;
            Ok(ResponseOutcome {
                seq: record.seq,
                text: outcome.text,
                parsed: outcome.parsed,
                model: outcome.model,
                backend: outcome.backend,
                latency_ms: outcome.latency_ms,
                cost: outcome.cost,
            })
        }
        Err(error) => {
            crate::storage::insert_response_invocation(
                &orch.db.local,
                NewResponseInvocation {
                    response_id: id.to_string(),
                    scope_key,
                    project_id: definition_project_id,
                    caller_kind: caller_kind.into(),
                    caller_label: caller_label.map(str::to_owned),
                    caller_run_id: caller_run_id.map(str::to_owned),
                    rendered_prompt: attempt.rendered,
                    args_json,
                    status: "failed".into(),
                    output_text: None,
                    error: Some(error.to_string()),
                    model: Some(attempt.model.to_string()),
                    backend: Some(attempt.backend),
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

async fn complete_definition(
    orch: &Orchestrator,
    definition: &ResponseDefinition,
    args: &Value,
    project_id: Option<&str>,
    project_path: Option<&std::path::Path>,
) -> Result<CompletionAttempt, ResponseError> {
    complete_definition_with_executor(
        orch,
        definition,
        args,
        project_id,
        project_path,
        Arc::new(|backend_name, request, orch| {
            let backend = backend_for_name(Some(&backend_name));
            backend
                .response_completion_availability(orch, request.project_id.as_deref())
                .map_err(ResponseError::BackendUnavailable)?;
            backend
                .response_model_availability(orch, request.project_id.as_deref(), &request.model)
                .map_err(ResponseError::BackendUnavailable)?;
            backend
                .complete(request, orch)
                .map_err(map_completion_error)
        }),
    )
    .await
}

type CompletionExecutor = Arc<
    dyn Fn(
            String,
            CompletionRequest,
            &Orchestrator,
        ) -> Result<crate::backends::CompletionOutcome, ResponseError>
        + Send
        + Sync,
>;

async fn complete_definition_with_executor(
    orch: &Orchestrator,
    definition: &ResponseDefinition,
    args: &Value,
    project_id: Option<&str>,
    project_path: Option<&std::path::Path>,
    executor: CompletionExecutor,
) -> Result<CompletionAttempt, ResponseError> {
    let rendered = definition.render(args).map_err(classify_render_error)?;
    let (model, extras, backend_name) = if let Some(model) = &definition.model {
        let preset = Preset {
            model: Model::new(model),
            options: definition.options.clone(),
        };
        (
            preset.model.clone(),
            preset.to_extras(),
            definition.backend.clone().expect("validated exact pin"),
        )
    } else {
        let presets =
            crate::config::presets::load_effective_presets(&orch.config_dir, project_path);
        let tier = definition.tier.as_deref().unwrap_or("sm");
        let preset = crate::config::presets::resolve_preset(tier, &presets)
            .map_err(ResponseError::Upstream)?;
        (preset.model, preset.extras, preset.backend)
    };
    let output_schema = match &definition.output {
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
    let mut messages = Vec::with_capacity(definition.examples.len() * 2 + 1);
    for example in &definition.examples {
        let prompt = definition
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
        project_id: project_id.map(str::to_string),
        extras: serde_json::to_value(extras).unwrap_or(Value::Null),
        output_schema: output_schema.clone(),
        timeout: definition.timeout,
    };
    let mut terminal_error = None;
    let mut accepted = None;
    for attempt in 0..=1 {
        let request_for_attempt = request.clone();
        let backend_for_attempt = backend_name.clone();
        let orch_for_attempt = orch.clone();
        let executor_for_attempt = executor.clone();
        let completion = match tokio::task::spawn_blocking(move || {
            executor_for_attempt(backend_for_attempt, request_for_attempt, &orch_for_attempt)
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
            Ok(outcome) => {
                match definition.validate_output(&outcome.text, output_schema.as_ref()) {
                    Ok(validated) => {
                        let parsed = match validated {
                            ValidatedOutput::Text(_) => None,
                            ValidatedOutput::Json(value) => Some(value),
                        };
                        accepted = Some(ResponseTestOutcome {
                            text: outcome.text,
                            parsed,
                            model: outcome.model,
                            backend: backend_name.clone(),
                            latency_ms: outcome.latency_ms,
                            cost: outcome.cost,
                            input_tokens: outcome.tokens.input,
                            output_tokens: outcome.tokens.output,
                        });
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
                        terminal_error = Some(ResponseError::ContractViolation(error));
                    }
                    Err(error) => terminal_error = Some(ResponseError::ContractViolation(error)),
                }
            }
            Err(error) => {
                terminal_error = Some(error);
                break;
            }
        }
    }
    Ok(CompletionAttempt {
        rendered,
        model,
        backend: backend_name,
        result: accepted.ok_or_else(|| {
            terminal_error
                .unwrap_or_else(|| ResponseError::Upstream("completion produced no result".into()))
        }),
    })
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub values: Vec<Value>,
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
            if let Some(default) = &variable.default {
                if !variable.values.is_empty() && !variable.values.contains(default) {
                    return Err(format!(
                        "Variable '{}' default is not one of its declared values",
                        variable.name
                    ));
                }
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
                    if !variable.values.is_empty() && !variable.values.contains(value) {
                        return Err(format!(
                            "Variable '{}' must be one of its declared values",
                            variable.name
                        ));
                    }
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
    use crate::backends::{CompletionOutcome, CompletionTokens};
    use crate::db::DbState;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{LocalDb, MigrationRunner, SearchIndex, TURSO_MIGRATIONS};
    use serde_json::json;
    use std::sync::Mutex;

    async fn test_orchestrator() -> Orchestrator {
        let temp = tempfile::tempdir().unwrap().keep();
        let local = LocalDb::open(temp.join("responses.db")).await.unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&local)
            .await
            .unwrap();
        let search = Arc::new(SearchIndex::open_or_create(temp.join("search")).unwrap());
        Orchestrator::builder(
            Arc::new(DbState::new(Arc::new(local), search)),
            Arc::new(TestServicesBuilder::new().build()),
            temp.join("config"),
        )
        .build()
    }

    #[tokio::test]
    async fn capabilities_keep_canonical_tiers_when_providers_are_unavailable() {
        let orch = test_orchestrator().await;
        let capabilities = model_capabilities(&orch, None, None, None);
        let selections: Vec<_> = capabilities
            .backends
            .iter()
            .flat_map(|backend| backend.selections.iter())
            .collect();

        assert!(selections
            .iter()
            .any(|selection| selection.value == "openrouter/md"));
        assert!(selections.iter().any(|selection| {
            selection.kind == "tier" && selection.legacy_values.iter().any(|value| value == "md")
        }));
        assert!(capabilities
            .backends
            .iter()
            .filter(|backend| !backend.runnable)
            .flat_map(|backend| backend.selections.iter())
            .all(|selection| !selection.runnable));
    }

    #[test]
    fn discovery_failure_represents_saved_exact_pin_as_recovery_only() {
        let selection =
            saved_discovery_selection("openrouter", "saved/model", Some("catalog offline"))
                .unwrap();
        assert_eq!(selection.value, "saved/model");
        assert!(!selection.runnable);
        assert_eq!(
            selection.unavailable_reason.as_deref(),
            Some("model discovery failed: catalog offline")
        );
        assert!(saved_discovery_selection("openrouter", "removed/model", None).is_none());
    }

    fn definition() -> ResponseDefinition {
        parse_definition("---\nname: Test\ndescription: test response\nvariables:\n  - name: required\n    required: true\n  - name: optional\n    default: fallback\noutput: text\n---\n{{required}} / {{ optional }}").unwrap()
    }

    #[test]
    fn omits_absent_variable_defaults_when_serializing() {
        let serialized = serde_yaml::to_string(&definition()).unwrap();

        assert!(!serialized.contains("default: null"));
        assert!(serialized.contains("default: fallback"));
    }

    #[tokio::test]
    async fn draft_completion_uses_declared_request_without_persisting_history() {
        let orch = test_orchestrator().await;
        let captured = Arc::new(Mutex::new(None));
        let captured_for_backend = captured.clone();
        let definition = parse_definition(
            "---\nname: Draft\ndescription: draft response\nmodel: test-model\nbackend: openrouter\ntimeout: 7s\nvariables:\n  - name: subject\n    required: true\noutput:\n  type: object\n  required: [answer]\n  properties:\n    answer: { type: string }\n---\nDraft {{subject}}",
        )
        .unwrap();

        let attempt = complete_definition_with_executor(
            &orch,
            &definition,
            &json!({"subject": "reply"}),
            None,
            None,
            Arc::new(move |backend, request, _orch| {
                *captured_for_backend.lock().unwrap() = Some((backend, request));
                Ok(CompletionOutcome {
                    text: r#"{"answer":"done"}"#.into(),
                    parsed: None,
                    model: "test-model".into(),
                    tokens: CompletionTokens::default(),
                    cost: None,
                    latency_ms: 3,
                })
            }),
        )
        .await
        .unwrap();

        let outcome = attempt.result.unwrap();
        assert_eq!(outcome.text, r#"{"answer":"done"}"#);
        assert_eq!(outcome.parsed, Some(json!({"answer": "done"})));
        let (backend, request) = captured.lock().unwrap().take().unwrap();
        assert_eq!(backend, "openrouter");
        assert_eq!(request.model, "test-model");
        assert_eq!(request.timeout, Duration::from_secs(7));
        assert_eq!(request.messages.last().unwrap().content, "Draft reply");
        assert_eq!(
            request.output_schema,
            Some(json!({
                "type": "object",
                "required": ["answer"],
                "properties": {"answer": {"type": "string"}}
            }))
        );

        assert!(crate::storage::list_response_invocations(
            &orch.db.local,
            "workspace",
            "draft",
            10,
        )
        .await
        .unwrap()
        .is_empty());
        assert!(
            crate::storage::list_route_firings(&orch.db.local, "workspace", "draft", 10,)
                .await
                .unwrap()
                .is_empty()
        );
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
    fn validates_declared_variable_values() {
        let response = parse_definition(
            "---\nname: Test\ndescription: test\nvariables:\n  - name: surface\n    default: imessage\n    values: [imessage, discord]\n---\n{{surface}}",
        )
        .unwrap();
        assert_eq!(response.render(&json!({})).unwrap(), "imessage");
        assert_eq!(
            response.render(&json!({"surface": "discord"})).unwrap(),
            "discord"
        );
        assert!(response
            .render(&json!({"surface": "email"}))
            .unwrap_err()
            .contains("declared values"));
        assert!(parse_definition(
            "---\nname: Test\ndescription: test\nvariables:\n  - name: surface\n    default: email\n    values: [imessage, discord]\n---\n{{surface}}",
        )
        .unwrap_err()
        .contains("default is not one"));
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

    #[test]
    fn closed_model_validation_accepts_only_offered_selection_identities() {
        let capabilities = ResponseModelCapabilities {
            backends: vec![ResponseBackendCapability {
                backend: "openrouter".into(),
                label: "OpenRouter".into(),
                runnable: true,
                unavailable_reason: None,
                selections: vec![
                    ResponseModelSelection {
                        kind: "tier".into(),
                        value: "openrouter/sm".into(),
                        label: "SM".into(),
                        backend: "openrouter".into(),
                        model: "openrouter/auto".into(),
                        legacy_values: vec!["sm".into()],
                        runnable: true,
                        unavailable_reason: None,
                    },
                    ResponseModelSelection {
                        kind: "model".into(),
                        value: "openrouter/auto".into(),
                        label: "Exact".into(),
                        backend: "openrouter".into(),
                        model: "openrouter/auto".into(),
                        legacy_values: vec![],
                        runnable: true,
                        unavailable_reason: None,
                    },
                ],
            }],
        };
        let mut authored = definition();
        authored.tier = Some("sm".into());
        assert!(is_offered_model_selection(&authored, &capabilities));
        authored.tier = Some("ghost/sm".into());
        assert!(!is_offered_model_selection(&authored, &capabilities));
        authored.tier = None;
        authored.backend = Some("openrouter".into());
        authored.model = Some("openrouter/auto".into());
        assert!(is_offered_model_selection(&authored, &capabilities));
        let mut unavailable = capabilities.clone();
        unavailable.backends[0].selections[1].runnable = false;
        assert!(!is_offered_model_selection(&authored, &unavailable));
    }
}
