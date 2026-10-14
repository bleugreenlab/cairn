//! Workflow AI conditions, answered as the built-in response
//! `builtin:workflow-condition`.
//!
//! A condition is an ordinary response: it resolves presets and accounts in the
//! execution's project, completes through the backend's facet on a
//! Cairn-selected account, and is journaled in response history. With no
//! account able to serve the selection it refuses; there is no fallback.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use cairn_llm::types::catalog::ProviderModelCatalog;
use cairn_workflow::runtime::AiConditionRequest;
use serde_json::json;

use crate::config::presets::{parse_tier_ref, PresetsConfig};
use crate::orchestrator::Orchestrator;
use crate::responses::{
    OutputContract, ResponseCaller, ResponseDefinition, ResponseError, ResponseVariable,
};

pub(crate) const RESPONSE_ID: &str = "builtin:workflow-condition";

/// Answer an AI condition's question with the model's raw text.
pub(crate) async fn complete_condition(
    orch: &Orchestrator,
    request: AiConditionRequest,
) -> Result<String, String> {
    let (project_id, project_path) = execution_scope(orch, &request.execution_id).await?;
    let presets =
        crate::config::presets::load_effective_presets(&orch.config_dir, project_path.as_deref());
    let selection = ConditionModel::select(
        request.model.as_deref(),
        &presets,
        &orch.get_model_catalog(),
    );
    let args = json!({
        "question": request.question,
        "ports": request.ports.join(", "),
        "context": request.context,
    });
    let caller = ResponseCaller::Condition {
        label: format!(
            "condition '{}' in execution {}",
            request.node_name, request.execution_id
        ),
        project_id,
        project_path,
    };
    crate::responses::invoke_definition(orch, &definition(&selection), &args, RESPONSE_ID, caller)
        .await
        .map(|outcome| outcome.text)
        .map_err(|error| refusal(&request.node_name, &selection, error))
}

/// The execution's project and that project's checkout, so presets and
/// accounts resolve per project as for any other response.
async fn execution_scope(
    orch: &Orchestrator,
    execution_id: &str,
) -> Result<(Option<String>, Option<PathBuf>), String> {
    let db = crate::execution::routing::owning_db_for_execution(&orch.db, execution_id)
        .await
        .map_err(|error| error.to_string())?;
    let project_id = db
        .query_opt_text(
            "SELECT project_id FROM executions WHERE id = ?1",
            (execution_id.to_string(),),
        )
        .await
        .map_err(|error| format!("failed to read execution {execution_id}: {error}"))?;
    let project_path = match &project_id {
        Some(project_id) => crate::config::get_project_path(&orch.db.local, project_id)
            .await
            .ok(),
        None => None,
    };
    Ok((project_id, project_path))
}

/// The model an authored condition asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ConditionModel {
    /// A tier reference (`sm`, or `backend/tier`) resolved through presets.
    Tier(String),
    /// An exact model pinned to the backend that serves it.
    Exact { model: String, backend: String },
}

impl ConditionModel {
    /// Unset follows the workspace's `sm` tier. A known tier, or a known
    /// backend qualifying a known tier, is a tier reference. Anything else is
    /// an exact model: `openai/gpt-…` is an OpenRouter model, not a tier of a
    /// backend named `openai`.
    fn select(
        model: Option<&str>,
        presets: &PresetsConfig,
        catalog: &[ProviderModelCatalog],
    ) -> Self {
        let Some(model) = model.map(str::trim).filter(|model| !model.is_empty()) else {
            return Self::Tier("sm".into());
        };
        let is_tier = match parse_tier_ref(model) {
            (Some(backend), tier) => {
                presets.backends.contains_key(backend) && presets.tiers.iter().any(|t| t == tier)
            }
            (None, tier) => presets.tiers.iter().any(|t| t == tier),
        };
        if is_tier {
            return Self::Tier(model.into());
        }
        let backend = catalog
            .iter()
            .find(|entry| entry.models.iter().any(|listed| listed.model == model))
            .map(|entry| entry.backend.clone())
            .unwrap_or_else(|| {
                crate::backends::backend_for_model(model)
                    .unwrap_or("claude")
                    .to_string()
            });
        Self::Exact {
            model: model.into(),
            backend,
        }
    }

    fn describe(&self) -> String {
        match self {
            Self::Tier(tier) => format!("the '{tier}' tier"),
            Self::Exact { model, backend } => format!("{model} on {backend}"),
        }
    }
}

fn definition(selection: &ConditionModel) -> ResponseDefinition {
    let (tier, model, backend) = match selection {
        ConditionModel::Tier(tier) => (Some(tier.clone()), None, None),
        ConditionModel::Exact { model, backend } => {
            (None, Some(model.clone()), Some(backend.clone()))
        }
    };
    let variable = |name: &str| ResponseVariable {
        name: name.into(),
        description: None,
        required: true,
        default: None,
        values: vec![],
    };
    ResponseDefinition {
        name: "Workflow condition".into(),
        description: "Route a workflow AI condition node to one of its ports".into(),
        tier,
        model,
        backend,
        options: HashMap::new(),
        variables: vec![variable("question"), variable("ports"), variable("context")],
        output: OutputContract::Named("text".into()),
        // A CLI-served one-shot pays a process start the 15s response default
        // leaves little room for.
        timeout: Duration::from_secs(60),
        examples: vec![],
        template: "Answer with EXACTLY one word from this list: {{ports}}\n\nContext:\n{{context}}\n\nQuestion: {{question}}\n\nAnswer:".into(),
    }
}

fn refusal(node_name: &str, selection: &ConditionModel, error: ResponseError) -> String {
    match error {
        ResponseError::BackendUnavailable(reason) => {
            let mut message = format!(
                "AI condition '{node_name}' can't run: no configured account can serve {} ({reason}).",
                selection.describe()
            );
            if !reason.contains("Settings → Providers") {
                message.push_str(" Connect an account in Settings → Providers.");
            }
            message
        }
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn presets() -> PresetsConfig {
        crate::config::presets::default_presets_config(None)
    }

    fn catalog(backend: &str, model: &str) -> Vec<ProviderModelCatalog> {
        vec![ProviderModelCatalog {
            backend: backend.into(),
            models: vec![serde_json::from_value(
                json!({"id": model, "model": model, "displayName": model}),
            )
            .unwrap()],
            options: vec![],
            refreshed_at: None,
            error: None,
        }]
    }

    #[test]
    fn built_in_definition_is_valid_for_every_selection() {
        for selection in [
            ConditionModel::Tier("sm".into()),
            ConditionModel::Exact {
                model: "haiku".into(),
                backend: "claude".into(),
            },
        ] {
            definition(&selection).validate().unwrap();
        }
    }

    #[test]
    fn the_prompt_is_the_condition_prompt() {
        let rendered = definition(&ConditionModel::Tier("sm".into()))
            .render(&json!({"question": "Q?", "ports": "yes, no", "context": "{{ports}}"}))
            .unwrap();
        assert_eq!(
            rendered,
            "Answer with EXACTLY one word from this list: yes, no\n\nContext:\n{{ports}}\n\nQuestion: Q?\n\nAnswer:"
        );
    }

    #[test]
    fn unset_model_follows_the_sm_tier() {
        assert_eq!(
            ConditionModel::select(None, &presets(), &[]),
            ConditionModel::Tier("sm".into())
        );
        assert_eq!(
            ConditionModel::select(Some("  "), &presets(), &[]),
            ConditionModel::Tier("sm".into())
        );
    }

    #[test]
    fn tier_references_stay_tiers() {
        for tier in ["md", "codex/sm", "claude/lg"] {
            assert_eq!(
                ConditionModel::select(Some(tier), &presets(), &[]),
                ConditionModel::Tier(tier.into()),
                "{tier}"
            );
        }
    }

    #[test]
    fn exact_models_pin_the_backend_that_serves_them() {
        assert_eq!(
            ConditionModel::select(Some("gpt-5.4-mini"), &presets(), &[]),
            ConditionModel::Exact {
                model: "gpt-5.4-mini".into(),
                backend: "codex".into()
            }
        );
        assert_eq!(
            ConditionModel::select(Some("haiku"), &presets(), &[]),
            ConditionModel::Exact {
                model: "haiku".into(),
                backend: "claude".into()
            }
        );
        // A slash does not make a tier when the prefix is not a backend.
        assert_eq!(
            ConditionModel::select(Some("openai/gpt-6-astra"), &presets(), &[]),
            ConditionModel::Exact {
                model: "openai/gpt-6-astra".into(),
                backend: "openrouter".into()
            }
        );
        // The catalog's listing wins over name inference.
        assert_eq!(
            ConditionModel::select(Some("qwen3:8b"), &presets(), &catalog("ollama", "qwen3:8b")),
            ConditionModel::Exact {
                model: "qwen3:8b".into(),
                backend: "ollama".into()
            }
        );
    }

    #[test]
    fn refusal_names_the_selection_and_where_to_fix_it() {
        let message = refusal(
            "db?",
            &ConditionModel::Tier("sm".into()),
            ResponseError::BackendUnavailable("no Claude account".into()),
        );
        assert_eq!(
            message,
            "AI condition 'db?' can't run: no configured account can serve the 'sm' tier (no Claude account). Connect an account in Settings → Providers."
        );
        let already_instructive = refusal(
            "db?",
            &ConditionModel::Tier("sm".into()),
            ResponseError::BackendUnavailable("connect one in Settings → Providers".into()),
        );
        assert_eq!(
            already_instructive.matches("Settings → Providers").count(),
            1
        );
        assert_eq!(
            refusal(
                "db?",
                &ConditionModel::Tier("sm".into()),
                ResponseError::Timeout
            ),
            "response timed out"
        );
    }
}
