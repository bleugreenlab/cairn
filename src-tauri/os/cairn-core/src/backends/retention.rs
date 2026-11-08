//! Zero data retention at the provider boundary.
//!
//! [`admit`] is the one place a call's zero-data-retention requirement is
//! decided: once per call, after the offer is chosen and before anything is
//! sent. The launch gate (`launch.rs`) runs it for every completion and
//! session, and the decide and embed doors run it before they build a route,
//! so no request reaches a provider without it. What it admitted rides in the
//! sealed [`super::Admitted`] proof, which a backend needs to send anything at
//! all; OpenRouter turns it into `provider.zdr` on the request.
//!
//! Whether a provider can meet the requirement is that backend's own answer
//! ([`super::AgentBackend::zero_data_retention`]), unless the operator
//! declared the provider zero-retention in `zeroDataRetentionProviders`.
//! A provider that cannot meet it is refused naming the offers that can.

use cairn_models::interface::retention::{
    zdr_required_by, RequiredBy, Requirements, ZdrBasis, ZeroDataRetention,
};
use cairn_models::interface::{ModelScope, Models};
use cairn_models::types::catalog::VerbKind;

use super::{backend_for_name, AgentBackend};
use crate::orchestrator::Orchestrator;

/// How many offers a refusal names as the ones that can serve with zero
/// retention.
const ALTERNATIVES_NAMED: usize = 3;

/// How a backend meets a zero-data-retention requirement for one model.
#[derive(Debug, Clone, PartialEq)]
pub struct ZdrMet {
    pub basis: ZdrBasis,
    /// The basis as a sentence the reply carries.
    pub detail: String,
    /// Endpoint slugs the request must route around to keep the guarantee
    /// (OpenRouter's Safety Retention endpoints for the model).
    pub exclude: Vec<String>,
}

/// A requirement as admitted for one call: what the reply states, and what
/// the request must carry to keep it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ZdrAdmission {
    pub(crate) report: ZeroDataRetention,
    pub(crate) exclude: Vec<String>,
}

/// The providers the operator declared zero-retention.
fn declared(orch: &Orchestrator) -> Vec<String> {
    crate::config::settings::load_zero_data_retention_providers(&orch.config_dir)
}

/// Whether `model` on `provider` keeps no data, by declaration or by its
/// backend's own answer.
fn meets(
    orch: &Orchestrator,
    declared: &[String],
    backend: &dyn AgentBackend,
    provider: &str,
    model: &str,
    project_id: Option<&str>,
) -> Result<ZdrMet, String> {
    if declared.iter().any(|key| key == provider) {
        return Ok(ZdrMet {
            basis: ZdrBasis::OperatorDeclaration,
            detail: format!(
                "the operator declared {provider} zero-retention in `zeroDataRetentionProviders`"
            ),
            exclude: Vec::new(),
        });
    }
    backend.zero_data_retention(orch, project_id, provider, model)
}

fn required_by_text(required_by: RequiredBy) -> &'static str {
    match required_by {
        RequiredBy::Call => "the call asked for it",
        RequiredBy::WorkspaceOpenRouterSetting => {
            "the workspace's OpenRouter setting requires it of every OpenRouter call"
        }
    }
}

/// Decide `requirements` for a call of `model` on `provider` (served by
/// `backend`) for `verb`: `Ok(None)` when nothing requires zero retention of
/// it, the admission when the provider can keep it, and otherwise the
/// refusal, which names the offers serving `verb` that could.
pub(crate) fn admit(
    orch: &Orchestrator,
    provider: &str,
    backend: &dyn AgentBackend,
    model: &str,
    requirements: &Requirements,
    project_id: Option<&str>,
    verb: VerbKind,
) -> Result<Option<ZdrAdmission>, String> {
    let floor = orch.get_settings().openrouter_routing.zero_data_retention;
    let Some(required_by) = zdr_required_by(requirements, provider, floor) else {
        return Ok(None);
    };
    let declared = declared(orch);
    match meets(orch, &declared, backend, provider, model, project_id) {
        Ok(met) => Ok(Some(ZdrAdmission {
            report: ZeroDataRetention {
                required_by,
                satisfied_by: met.basis,
                detail: met.detail,
            },
            exclude: met.exclude,
        })),
        Err(why) => {
            let able = alternatives(orch, &declared, project_id, verb, provider, model);
            let alternatives = if able.is_empty() {
                "No offer in scope can serve it with zero retention.".to_string()
            } else {
                format!("Offers that can serve it with zero retention: {able}.")
            };
            Err(format!(
                "Zero data retention is required of this call ({}), and {model} on {provider} cannot keep it: {why}. Nothing was sent. {alternatives} An operator whose agreement with a provider covers zero retention can declare it by listing the provider's key in `zeroDataRetentionProviders` in settings.yaml.",
                required_by_text(required_by)
            ))
        }
    }
}

/// The offers serving `verb` in scope that can serve with zero retention,
/// named `provider/model`, less the one refused: at most
/// [`ALTERNATIVES_NAMED`], then how many more.
fn alternatives(
    orch: &Orchestrator,
    declared: &[String],
    project_id: Option<&str>,
    verb: VerbKind,
    refused_provider: &str,
    refused_model: &str,
) -> String {
    let Ok(catalog) = orch.catalog(&ModelScope {
        project_id: project_id.map(str::to_string),
    }) else {
        return String::new();
    };
    let able: Vec<String> = catalog
        .offers_serving(verb)
        .into_iter()
        .filter(|offer| !(offer.provider == refused_provider && offer.model == refused_model))
        .filter(|offer| {
            let backend = backend_for_name(Some(&offer.provider));
            meets(
                orch,
                declared,
                backend.as_ref(),
                &offer.provider,
                &offer.model,
                project_id,
            )
            .is_ok()
        })
        .map(|offer| format!("{}/{}", offer.provider, offer.model))
        .collect();
    let mut named = able
        .iter()
        .take(ALTERNATIVES_NAMED)
        .cloned()
        .collect::<Vec<_>>()
        .join(", ");
    if able.len() > ALTERNATIVES_NAMED {
        named.push_str(&format!(" and {} more", able.len() - ALTERNATIVES_NAMED));
    }
    named
}

/// The refusal of every backend that keeps no checkable guarantee.
pub(crate) fn no_guarantee(name: &str) -> String {
    format!("{name} offers no zero-data-retention guarantee Cairn can check")
}

/// A custom endpoint's answer: met when its host is this machine or a private
/// network, since then the operator runs it; refused for a public host, which
/// may as well be a third party's hosted API.
pub(crate) fn private_endpoint(name: &str, base_url: &str) -> Result<ZdrMet, String> {
    let host = reqwest::Url::parse(base_url)
        .ok()
        .and_then(|url| url.host_str().map(str::to_string));
    match host {
        Some(host) if cairn_models::interface::retention::is_private_host(&host) => Ok(ZdrMet {
            basis: ZdrBasis::PrivateHost,
            detail: format!("{name} is served from {host}, a loopback or private-network host"),
            exclude: Vec::new(),
        }),
        Some(host) => Err(format!(
            "{name} is served from {host}, a public host Cairn cannot tell from a third party's hosted API"
        )),
        None => Err(format!("{name}'s base URL `{base_url}` names no host")),
    }
}

/// What tests of every verb share: OpenRouter's ZDR list as a script, and a
/// workspace with OpenRouter installed on a key.
#[cfg(test)]
pub(crate) mod fixtures {
    use std::sync::Arc;

    use cairn_models::openrouter::{ZdrDirectory, ZdrEndpoint, ZdrSource};

    use crate::credentials::{ApiProvider, ProviderAuth};
    use crate::models::UpdateSettings;
    use crate::orchestrator::Orchestrator;

    /// A ZDR list read from a script, or an unreachable one.
    struct Scripted(Result<Vec<ZdrEndpoint>, String>);

    impl ZdrSource for Scripted {
        fn fetch(&self) -> Result<Vec<ZdrEndpoint>, String> {
            self.0.clone()
        }
    }

    /// One row of the list: `(model, provider, tag, safety_retention)`.
    pub(crate) type Row<'a> = (&'a str, &'a str, &'a str, bool);

    /// `orch` reads `rows` as OpenRouter's ZDR list.
    pub(crate) fn zdr_list(orch: &mut Orchestrator, rows: &[Row<'_>]) {
        let endpoints = rows
            .iter()
            .map(|(model, provider, tag, safety_retention)| ZdrEndpoint {
                model_id: model.to_string(),
                provider_name: provider.to_string(),
                tag: Some(tag.to_string()),
                safety_retention: *safety_retention,
            })
            .collect();
        set_source(orch, Ok(endpoints));
    }

    /// `orch` cannot read OpenRouter's ZDR list.
    pub(crate) fn unreadable_zdr_list(orch: &mut Orchestrator) {
        set_source(orch, Err("OpenRouter is unreachable".to_string()));
    }

    fn set_source(orch: &mut Orchestrator, source: Result<Vec<ZdrEndpoint>, String>) {
        let runtime = crate::orchestrator::models::ModelRuntime {
            openrouter_zdr: Arc::new(ZdrDirectory::new(Arc::new(Scripted(source)))),
            ..orch.models_runtime().clone()
        };
        orch.set_models_runtime_for_test(runtime);
    }

    /// Install OpenRouter in `orch` on the key `sk-or-test`.
    pub(crate) fn install_openrouter(orch: &Orchestrator) {
        let mut enabled = orch.enabled_providers();
        enabled.push("openrouter".to_string());
        orch.update_settings(UpdateSettings {
            enabled_providers: Some(enabled),
            ..Default::default()
        })
        .unwrap();
        orch.add_account(
            ApiProvider::OpenRouter,
            "OpenRouter".into(),
            ProviderAuth::ApiKey {
                value: "sk-or-test".into(),
            },
            None,
        )
        .unwrap();
    }

    /// Turn the workspace's OpenRouter zero-retention setting on.
    pub(crate) fn require_zdr_of_openrouter(orch: &Orchestrator) {
        orch.update_settings(UpdateSettings {
            openrouter_routing: Some(crate::models::OpenRouterRouting {
                zero_data_retention: true,
                sort: None,
            }),
            ..Default::default()
        })
        .unwrap();
    }

    /// Declare `providers` zero-retention, as an operator does in
    /// settings.yaml.
    pub(crate) fn declare_zero_retention(orch: &Orchestrator, providers: &[&str]) {
        let path = crate::config::settings::get_settings_path(&orch.config_dir);
        let mut text = std::fs::read_to_string(&path).unwrap_or_default();
        text.push_str("\nzeroDataRetentionProviders:\n");
        for provider in providers {
            text.push_str(&format!("  - \"{provider}\"\n"));
        }
        std::fs::write(&path, text).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_custom_endpoint_keeps_zero_retention_only_on_a_private_host() {
        let met = private_endpoint("Studio", "http://192.168.1.20:8000/v1").unwrap();
        assert_eq!(met.basis, ZdrBasis::PrivateHost);
        assert!(met.detail.contains("192.168.1.20"), "{}", met.detail);
        assert!(private_endpoint("Box", "http://gpu.lan/v1").is_ok());
        assert!(private_endpoint("Me", "http://localhost:11434").is_ok());
        let refused = private_endpoint("Together", "https://api.together.xyz/v1").unwrap_err();
        assert!(refused.contains("public host"), "{refused}");
        assert!(private_endpoint("Broken", "not a url")
            .unwrap_err()
            .contains("names no host"));
    }
}
