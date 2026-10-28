//! TypeSafe: Jev's System One endpoint on the workspace's own TypeSafe key.
//!
//! TypeSafe serves `decide` and nothing else. Its one endpoint,
//! `POST https://api.typesafe.ai/v1/systemone`, answers typed questions with
//! calibrated probabilities and generates no text, so there is no agent turn,
//! no response and no embedding to offer; starting either is refused with that
//! reason, as a custom `openai-decisions` provider refuses it.
//!
//! The same Jev is reachable through OpenRouter's Decisions API. The direct
//! account is a provider of its own because it differs where it matters: the
//! versioned ids (`jev-1.14.0`), TypeSafe's own aliases, which move
//! independently of OpenRouter's, and the account's own rate limits and
//! data-handling terms.
//!
//! # Models
//!
//! The ids are declared here rather than trusted to discovery, because
//! TypeSafe's `GET /v1/models` lists only aliases while accepting every
//! versioned id (<https://docs.typesafe.ai/models#listing-models>). The
//! listing is read only to confirm the key works and to pick up an alias this
//! build does not know. Without a key nothing is declared and nothing is
//! offered.
//!
//! # Context limits
//!
//! A request must fit two limits, 64k tokens for the state and every question
//! together and 32k for the state and any one question. The
//! `TypeSafe-Danger-Zone-Long-Context: true` header raises both to 262k, at
//! the same price but slower and less accurate, so Cairn never sends it. Cairn
//! has no Jev tokenizer to check the limits beforehand; TypeSafe refuses an
//! over-long request with `max_tokens_exceeded`, and the System One wire turns
//! that into a refusal carrying [`CONTEXT_LIMITS`].

#[cfg(test)]
pub(crate) mod tests;

use std::time::Duration;

use reqwest::header::{HeaderValue, AUTHORIZATION};
use serde::Deserialize;

use crate::backends::{AgentBackend, ResolvedTools, SessionConfig};
use crate::credentials::{ApiProvider, ProviderAuth};
use crate::orchestrator::Orchestrator;
use cairn_models::interface::decide::DecisionRoute;
use cairn_models::interface::SelectedOffer;
use cairn_models::types::catalog::{
    DiscoveredModel, Modalities, Modality, ModelPricing, Price, ServedVerb, Verb, Wire,
};
use cairn_models::types::usage::{Charge, Meter};
use cairn_models::wire::decisions::http::DecisionsEndpoint;

pub(crate) use cairn_models::types::catalog::TYPESAFE_BACKEND_KEY;
pub(crate) const TYPESAFE_BACKEND_NAME: &str = "TypeSafe";

/// The root every TypeSafe API path hangs off.
const TYPESAFE_API_BASE: &str = "https://api.typesafe.ai/v1";

/// Bounds the model listing, read on a catalog refresh.
const LISTING_TIMEOUT: Duration = Duration::from_secs(20);

/// Jev's per-request context, the window a decision is read with. TypeSafe
/// documents it as 64k.
const CONTEXT_WINDOW: i64 = 64_000;

/// USD per input token: TypeSafe's $42 per billion. Output is free.
const INPUT_USD_PER_TOKEN: &str = "0.000000042";

/// What a request longer than Jev's context is told, beside TypeSafe's own
/// sentence.
pub(crate) const CONTEXT_LIMITS: &str = "TypeSafe's Jev reads at most 64k tokens per request (the input and every question together) and 32k per question (the input and any one question). Shorten the input or split the questions across decisions. The TypeSafe-Danger-Zone-Long-Context: true header raises both limits to 262k, more slowly and less accurately; Cairn does not send it.";

/// Why TypeSafe cannot run an agent or a response.
const DECIDES_ONLY: &str = "TypeSafe answers decisions only (System One); it cannot run an agent or a response. Choose a chat provider for this.";

/// The ids Cairn declares, each with its name and what it is.
const DECLARED: [(&str, &str, &str); 4] = [
    (
        "jev-1.14.0",
        "Jev 1.14",
        "Jev 1.14, released 2026-10-14. A versioned id: its answers never move under you.",
    ),
    (
        "jev-1.13.0",
        "Jev 1.13",
        "Jev 1.13. A versioned id: its answers never move under you.",
    ),
    (
        "jev-latest",
        "Jev (latest)",
        "TypeSafe's alias for its most recent stable Jev release. It moves when a new release ships; each answer names the version that gave it.",
    ),
    (
        "jev-preview",
        "Jev (preview)",
        "TypeSafe's alias for its most recent Jev release, official or not. It moves ahead of jev-latest when a preview build ships.",
    ),
];

#[derive(Debug, Clone, Copy)]
pub struct TypeSafeBackend;

pub(crate) fn typesafe_api_key(orch: &Orchestrator) -> Option<String> {
    orch.get_credential_store().and_then(|store| {
        store
            .accounts_for_provider(ApiProvider::TypeSafe, None)
            .into_iter()
            .find(|account| account.serves_backend(TYPESAFE_BACKEND_KEY))
            .and_then(|account| match &account.auth {
                ProviderAuth::ApiKey { value } => Some(value.clone()),
                _ => None,
            })
    })
}

/// What every TypeSafe model serves: `decide` over System One.
fn decide() -> Vec<ServedVerb> {
    vec![ServedVerb {
        verb: Verb::Decide,
        wire: Wire::SystemOne,
    }]
}

/// A Jev model as the catalog carries it. `priced` is false for an alias the
/// listing named that this build does not know: its price is not stated
/// anywhere Cairn has read, so none is claimed.
fn jev(id: &str, name: &str, description: Option<String>, priced: bool) -> DiscoveredModel {
    DiscoveredModel {
        id: id.to_string(),
        model: id.to_string(),
        display_name: name.to_string(),
        description,
        hidden: false,
        // The alias TypeSafe's own SDKs default to.
        is_default: id == "jev-latest",
        default_reasoning_effort: None,
        supported_reasoning_efforts: Vec::new(),
        context_window: priced.then_some(CONTEXT_WINDOW),
        canonical_slug: None,
        serving_account_ids: Vec::new(),
        pricing: priced.then(|| ModelPricing {
            prices: vec![
                Price {
                    meter: Meter::tokens(Charge::Input),
                    usd: INPUT_USD_PER_TOKEN.to_string(),
                },
                Price {
                    meter: Meter::tokens(Charge::Output),
                    usd: "0".to_string(),
                },
            ],
            unplaced: Default::default(),
        }),
        supported_parameters: Vec::new(),
        router: false,
        modalities: Some(Modalities {
            input: vec![Modality::Text],
            output: vec![Modality::from("decisions")],
        }),
        serves: Some(decide()),
        tokenizer: None,
    }
}

/// The models Cairn declares TypeSafe serves.
pub(crate) fn declared_models() -> Vec<DiscoveredModel> {
    DECLARED
        .iter()
        .map(|(id, name, description)| jev(id, name, Some(description.to_string()), true))
        .collect()
}

#[derive(Debug, Deserialize)]
struct ListingResponse {
    models: Vec<ListedModel>,
}

#[derive(Debug, Deserialize)]
struct ListedModel {
    name: String,
    #[serde(default)]
    description: Option<String>,
}

/// The names a `GET /v1/models` body lists, with their descriptions.
fn decode_listing(body: &str) -> Result<Vec<ListedModel>, String> {
    serde_json::from_str::<ListingResponse>(body)
        .map(|listing| listing.models)
        .map_err(|error| format!("unreadable TypeSafe model listing: {error}: {body}"))
}

/// The catalog from the listing a key got back, or `None` when there is no
/// key: nothing, so nothing is offered. A failed listing keeps the declared
/// models and says why it failed (a rejected key reads here first).
pub(crate) fn assemble(
    listing: Option<Result<String, String>>,
) -> (Vec<DiscoveredModel>, Option<String>) {
    let Some(listing) = listing else {
        return (Vec::new(), None);
    };
    let mut models = declared_models();
    let listed = match listing.and_then(|body| decode_listing(&body)) {
        Ok(listed) => listed,
        Err(error) => return (models, Some(error)),
    };
    for entry in listed {
        if models.iter().any(|model| model.id == entry.name) {
            continue;
        }
        models.push(jev(&entry.name, &entry.name, entry.description, false));
    }
    (models, None)
}

/// One authenticated `GET /v1/models`, as its body or why it failed.
fn list_models(api_key: &str) -> Result<String, String> {
    let bearer = HeaderValue::from_str(&format!("Bearer {api_key}"))
        .map_err(|error| format!("invalid TypeSafe API key header: {error}"))?;
    let response = reqwest::blocking::Client::builder()
        .timeout(LISTING_TIMEOUT)
        .build()
        .map_err(|error| format!("TypeSafe model listing client failed: {error}"))?
        .get(format!("{TYPESAFE_API_BASE}/models"))
        .header(AUTHORIZATION, bearer)
        .send()
        .map_err(|error| format!("TypeSafe model listing request failed: {error}"))?;
    let status = response.status();
    let body = response
        .text()
        .map_err(|error| format!("TypeSafe model listing body failed: {error}"))?;
    if status.is_success() {
        return Ok(body);
    }
    let detail = cairn_models::wire::upstream_error_detail(&body);
    Err(if status == reqwest::StatusCode::UNAUTHORIZED {
        format!("TypeSafe rejected the API key (HTTP 401): {detail}")
    } else {
        format!(
            "TypeSafe model listing returned HTTP {}: {detail}",
            status.as_u16()
        )
    })
}

/// TypeSafe's catalog on this workspace's key: the declared models, and any
/// alias the listing adds. No key, no models.
pub(crate) fn discover_catalog_blocking(
    orch: &Orchestrator,
) -> (Vec<DiscoveredModel>, Option<String>) {
    assemble(typesafe_api_key(orch).map(|key| list_models(&key)))
}

/// Where a TypeSafe decision is posted, on `api_key`. The long-context header
/// is deliberately absent (see the module's *Context limits*).
pub(crate) fn decision_endpoint(api_key: &str) -> DecisionsEndpoint {
    DecisionsEndpoint {
        provider_name: TYPESAFE_BACKEND_NAME.to_string(),
        backend_key: TYPESAFE_BACKEND_KEY.to_string(),
        url: format!("{TYPESAFE_API_BASE}/systemone"),
        headers: vec![
            ("content-type".to_string(), "application/json".to_string()),
            ("authorization".to_string(), format!("Bearer {api_key}")),
        ],
        context_limits: Some(CONTEXT_LIMITS.to_string()),
    }
}

impl AgentBackend for TypeSafeBackend {
    fn name(&self) -> &str {
        TYPESAFE_BACKEND_NAME
    }

    fn is_available(&self) -> Result<(), String> {
        // Whether it can answer depends on a configured key, which only the
        // orchestrator can see; this stays optimistic so Settings shows the
        // provider and its setup requirement.
        Ok(())
    }

    fn discover_models(&self) -> Result<Vec<DiscoveredModel>, String> {
        // The listing needs the workspace's key, so the orchestrator routes
        // TypeSafe through `discover_catalog_blocking` instead.
        Err("TypeSafe discovery requires the workspace's API key".to_string())
    }

    /// `decide` over System One, for any id the workspace names (TypeSafe
    /// accepts every versioned id, listed or not), once there is a key to ask
    /// with. Without one, nothing.
    fn serves(&self, orch: &Orchestrator, _project_id: Option<&str>) -> Vec<ServedVerb> {
        if typesafe_api_key(orch).is_some() {
            decide()
        } else {
            Vec::new()
        }
    }

    fn decision_route(
        &self,
        orch: &Orchestrator,
        _project_id: Option<&str>,
        offer: &SelectedOffer,
    ) -> Result<DecisionRoute, String> {
        match offer.served.wire {
            Wire::SystemOne => {
                let api_key = typesafe_api_key(orch)
                    .ok_or_else(|| "TypeSafe needs an API key to answer a decision".to_string())?;
                Ok(DecisionRoute::SystemOne(decision_endpoint(&api_key)))
            }
            wire => Err(format!("TypeSafe answers no decision over {wire}")),
        }
    }

    fn response_completion_availability(
        &self,
        _orch: &Orchestrator,
        _project_id: Option<&str>,
    ) -> Result<(), String> {
        Err(DECIDES_ONLY.to_string())
    }

    fn resolve_tools(
        &self,
        _agent_tools: &[String],
        _agent_disallowed: &[String],
    ) -> ResolvedTools {
        ResolvedTools {
            allowed: Vec::new(),
            disallowed: Vec::new(),
        }
    }

    fn start_session(
        &self,
        _config: SessionConfig,
        _orch: &Orchestrator,
        _admitted: crate::backends::Admitted,
    ) -> Result<(), String> {
        Err(DECIDES_ONLY.to_string())
    }

    fn supports_resume(&self) -> bool {
        false
    }

    fn supports_warm_processes(&self) -> bool {
        false
    }

    fn runtime_launch_capability(
        &self,
        _launch: &crate::backends::RuntimeLaunch,
    ) -> Result<crate::backends::RuntimeLaunchCapability, String> {
        Ok(crate::backends::stateless_http_runtime_capability(
            "typesafe-http",
        ))
    }

    fn call_batch_capability(&self) -> crate::backends::CallBatchCapability {
        crate::backends::CallBatchCapability {
            shape: crate::backends::CallBatchShape::InProcess,
            max_concurrency: None,
        }
    }
}
