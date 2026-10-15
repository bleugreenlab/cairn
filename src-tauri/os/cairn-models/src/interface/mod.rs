//! The facet's interface: what the rest of Cairn, and later the served HTTP
//! API, may ask the cairn-models facet.
//!
//! The read side is [`Models`]: which providers and models exist and how tiers
//! bind to them ([`Catalog`]), which accounts serve them, how much of each
//! provider is left, and which sessions are live. The `cairn://models/*`
//! resources render these answers; the served HTTP API will be a second door
//! onto the same trait.
//!
//! The launch side is [`complete`] (an HTTP completion over one of the three
//! protocol families) and [`session`] (a CLI session opened on a host and
//! driven through its control).
//!
//! Every answer reuses the facet's existing vocabulary: accounts are
//! [`AccountInfo`], usage is [`ProviderPoolSnapshot`], providers are
//! [`ProviderDescriptor`], discovered models are [`ProviderModelCatalog`]. The
//! types defined here exist only where nothing did before: the composed
//! catalog, its catalog-id view, the session view, and the requests refusal.

pub mod complete;
pub mod decide;
pub mod session;

use std::collections::BTreeMap;
use std::fmt;

use cairn_credentials::AccountInfo;
use cairn_domain::decision::MeasuredBy;
use cairn_models_types::catalog::{
    DiscoveredModelPricing, ProviderDescriptor, ProviderModelCatalog,
};
use cairn_models_types::pool::ProviderPoolSnapshot;
use serde::Serialize;
use serde_json::Value;

/// Whose view of the facet a question is asked from.
///
/// `None` is the workspace, which is what Settings → Providers shows. A project
/// adds its private accounts and its tier overrides.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ModelScope {
    pub project_id: Option<String>,
}

impl ModelScope {
    pub fn workspace() -> Self {
        Self::default()
    }

    pub fn project(project_id: impl Into<String>) -> Self {
        Self {
            project_id: Some(project_id.into()),
        }
    }
}

/// Every provider this workspace can name, with its models and tier bindings.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Catalog {
    /// Installed providers in the workspace's own order, then every supported
    /// provider it has not installed (the Add-provider list).
    pub providers: Vec<CatalogProvider>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogProvider {
    pub descriptor: ProviderDescriptor,
    /// Whether this workspace installed the provider. Only installed providers
    /// carry models and tier bindings.
    pub installed: bool,
    /// What discovery last found. `None` means discovery never ran; a failed
    /// discovery carries its error and last refresh time inside.
    pub models: Option<ProviderModelCatalog>,
    /// The tiers bound on this provider, in the workspace's tier order.
    pub tiers: Vec<TierBinding>,
    /// How this provider's offers answer a decision: natively over the
    /// Decisions wire, from chat logprobs, or (`None`) not at all.
    pub decision: Option<MeasuredBy>,
    /// Models on this provider observed unable to answer a decision despite
    /// its method (a chat server that returned no logprobs, or a model that
    /// reasons before it answers), each with why. They offer no decision.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub decision_unsupported: BTreeMap<String, String>,
}

impl CatalogProvider {
    /// How `model` on this provider answers a decision, or `None`.
    pub fn decision_for(&self, model: &str) -> Option<MeasuredBy> {
        self.decision
            .filter(|_| !self.decision_unsupported.contains_key(model))
    }
}

/// One tier as it resolves on one provider.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TierBinding {
    pub tier: String,
    pub model: String,
    /// The preset's options (reasoning effort and the like), as JSON because
    /// the facet cannot name the configuration crate's value type.
    pub options: BTreeMap<String, Value>,
    /// Whether an unqualified reference to this tier lands on this provider.
    pub is_tier_default: bool,
}

/// One model across every provider that offers it.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogModel {
    /// The provider's canonical slug when it reports one, otherwise
    /// `{provider}/{model}`.
    pub catalog_id: String,
    pub display_name: String,
    pub offers: Vec<CatalogOffer>,
    /// Tier references bound to this model (`claude/lg`), then native ids that
    /// differ from the catalog id.
    pub aliases: Vec<String>,
}

/// One provider's offer of a model.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogOffer {
    pub provider: String,
    /// The id the provider itself is sent.
    pub model: String,
    pub context_window: Option<i64>,
    pub pricing: Option<DiscoveredModelPricing>,
    /// How this offer can answer a decision, or `None`. Whether a given
    /// answer was measured natively is recorded on the answer itself, since
    /// tier resolution picks the offer at run time.
    pub decision: Option<MeasuredBy>,
}

impl Catalog {
    /// The installed providers, in order.
    pub fn installed(&self) -> impl Iterator<Item = &CatalogProvider> {
        self.providers.iter().filter(|provider| provider.installed)
    }

    /// The catalog-id view: every selectable model on an installed provider,
    /// grouped by catalog id.
    ///
    /// Nothing is merged by guess. Two providers' offers share an entry only
    /// when both report the same canonical slug; the Claude CLI's `opus` stays
    /// `claude/opus`, because the CLI, not Cairn, decides which version it
    /// means. A model a tier is bound to but discovery has not reported still
    /// appears, so every binding is visible here. Hidden models (ones the
    /// provider cannot serve) are left out.
    pub fn models(&self) -> Vec<CatalogModel> {
        let mut out: Vec<CatalogModel> = Vec::new();
        let mut place =
            |catalog_id: String, display_name: &str, offer: CatalogOffer, aliases: Vec<String>| {
                let entry = match out.iter_mut().position(|m| m.catalog_id == catalog_id) {
                    Some(index) => &mut out[index],
                    None => {
                        out.push(CatalogModel {
                            catalog_id,
                            display_name: display_name.to_string(),
                            offers: Vec::new(),
                            aliases: Vec::new(),
                        });
                        out.last_mut().expect("just pushed")
                    }
                };
                if !entry
                    .offers
                    .iter()
                    .any(|o| o.provider == offer.provider && o.model == offer.model)
                {
                    entry.offers.push(offer);
                }
                for alias in aliases {
                    if alias != entry.catalog_id && !entry.aliases.contains(&alias) {
                        entry.aliases.push(alias);
                    }
                }
            };

        for provider in self.installed() {
            let key = provider.descriptor.key.as_ref();
            let tier_refs = |native: &[&str]| -> Vec<String> {
                provider
                    .tiers
                    .iter()
                    .filter(|binding| native.contains(&binding.model.as_str()))
                    .map(|binding| format!("{key}/{}", binding.tier))
                    .collect()
            };
            let discovered = provider
                .models
                .as_ref()
                .map(|catalog| catalog.models.as_slice())
                .unwrap_or_default();
            for model in discovered.iter().filter(|model| !model.hidden) {
                let catalog_id = model
                    .canonical_slug
                    .clone()
                    .filter(|slug| !slug.is_empty())
                    .unwrap_or_else(|| format!("{key}/{}", model.model));
                let mut aliases = tier_refs(&[model.model.as_str(), model.id.as_str()]);
                aliases.push(model.model.clone());
                place(
                    catalog_id,
                    &model.display_name,
                    CatalogOffer {
                        provider: key.to_string(),
                        model: model.model.clone(),
                        context_window: model.context_window,
                        pricing: model.pricing.clone(),
                        decision: provider.decision_for(&model.model),
                    },
                    aliases,
                );
            }
            for binding in &provider.tiers {
                let known = discovered
                    .iter()
                    .any(|model| model.model == binding.model || model.id == binding.model);
                if known {
                    continue;
                }
                place(
                    format!("{key}/{}", binding.model),
                    &binding.model,
                    CatalogOffer {
                        provider: key.to_string(),
                        model: binding.model.clone(),
                        context_window: None,
                        pricing: None,
                        decision: provider.decision_for(&binding.model),
                    },
                    tier_refs(&[binding.model.as_str()]),
                );
            }
        }
        out
    }

    /// Every offer that can answer a decision: native offers first, then
    /// logprobs offers, each in the workspace's provider order.
    pub fn decision_offers(&self) -> Vec<decide::DecisionOffer> {
        let mut offers: Vec<decide::DecisionOffer> = self
            .models()
            .into_iter()
            .flat_map(|model| model.offers)
            .filter_map(|offer| {
                Some(decide::DecisionOffer {
                    method: offer.decision?,
                    provider: offer.provider,
                    model: offer.model,
                })
            })
            .collect();
        // Stable: provider order survives within each method.
        offers.sort_by_key(|offer| offer.method != MeasuredBy::Native);
        offers
    }

    /// The offer a decision asking for `requested` is served by.
    ///
    /// `requested` is what an author wrote. Unset takes the first
    /// decision-capable offer ([`Self::decision_offers`]). A tier (`sm`, or
    /// `provider/sm`) takes that tier's binding on a decision-capable
    /// provider, the tier's default provider first. Anything else is an exact
    /// model, matched by native id, catalog id, or alias. A selection that
    /// lands only on providers that cannot decide is refused, never served by
    /// something that can.
    pub fn decision_offer(
        &self,
        requested: Option<&str>,
    ) -> Result<decide::DecisionOffer, decide::NoDecisionOffer> {
        let requested = requested.map(str::trim).filter(|value| !value.is_empty());
        let refuse = |detail: Option<String>| decide::NoDecisionOffer {
            requested: requested.map(str::to_string),
            detail,
        };
        let Some(requested) = requested else {
            return self.decision_offers().into_iter().next().ok_or_else(|| {
                // Say why offers that would have decided did not.
                let unable: Vec<String> = self
                    .installed()
                    .filter(|provider| provider.decision.is_some())
                    .flat_map(|provider| {
                        provider
                            .decision_unsupported
                            .iter()
                            .map(move |(model, why)| {
                                format!("{model} on {} {why}", provider.descriptor.key)
                            })
                    })
                    .collect();
                refuse((!unable.is_empty()).then(|| {
                    format!(
                        "every offer that could was seen unable ({})",
                        unable.join("; ")
                    )
                }))
            });
        };

        let (qualifier, tier) = match requested.split_once('/') {
            Some((provider, tier))
                if self
                    .installed()
                    .any(|candidate| candidate.descriptor.key.as_ref() == provider) =>
            {
                (Some(provider), tier)
            }
            _ => (None, requested),
        };
        let mut bindings: Vec<(&CatalogProvider, &TierBinding)> = self
            .installed()
            .filter(|provider| qualifier.is_none_or(|key| provider.descriptor.key.as_ref() == key))
            .flat_map(|provider| {
                provider
                    .tiers
                    .iter()
                    .filter(|binding| binding.tier == tier)
                    .map(move |binding| (provider, binding))
            })
            .collect();
        if !bindings.is_empty() {
            bindings.sort_by_key(|(provider, binding)| {
                (
                    provider.decision != Some(MeasuredBy::Native),
                    !binding.is_tier_default,
                )
            });
            return bindings
                .iter()
                .find_map(|(provider, binding)| {
                    Some(decide::DecisionOffer {
                        provider: provider.descriptor.key.to_string(),
                        model: binding.model.clone(),
                        method: provider.decision_for(&binding.model)?,
                    })
                })
                .ok_or_else(|| {
                    refuse(Some(format!(
                        "the '{requested}' tier is bound only on {}, which cannot answer decisions{}",
                        bindings
                            .iter()
                            .map(|(provider, _)| provider.descriptor.key.as_ref())
                            .collect::<Vec<_>>()
                            .join(", "),
                        self.observed_unable(bindings.iter().map(|(provider, binding)| {
                            (provider.descriptor.key.as_ref(), binding.model.as_str())
                        }))
                    )))
                });
        }

        let matching: Vec<CatalogOffer> = self
            .models()
            .into_iter()
            .filter(|model| {
                model.catalog_id == requested
                    || model.aliases.iter().any(|alias| alias == requested)
                    || model.offers.iter().any(|offer| offer.model == requested)
            })
            .flat_map(|model| model.offers)
            .collect();
        if matching.is_empty() {
            return Err(refuse(Some(format!(
                "no installed provider offers '{requested}'"
            ))));
        }
        let mut capable: Vec<decide::DecisionOffer> = matching
            .iter()
            .filter_map(|offer| {
                Some(decide::DecisionOffer {
                    provider: offer.provider.clone(),
                    model: offer.model.clone(),
                    method: offer.decision?,
                })
            })
            .collect();
        capable.sort_by_key(|offer| offer.method != MeasuredBy::Native);
        capable.into_iter().next().ok_or_else(|| {
            let mut providers: Vec<&str> = matching
                .iter()
                .map(|offer| offer.provider.as_str())
                .collect();
            providers.dedup();
            refuse(Some(format!(
                "'{requested}' is offered only by {}, which cannot answer decisions{}",
                providers.join(", "),
                self.observed_unable(
                    matching
                        .iter()
                        .map(|offer| (offer.provider.as_str(), offer.model.as_str()))
                )
            )))
        })
    }

    /// Why the named offers were observed unable to decide, as a clause to
    /// append to a refusal; empty when none was.
    fn observed_unable<'a>(&self, offers: impl Iterator<Item = (&'a str, &'a str)>) -> String {
        let reasons: Vec<String> = offers
            .filter_map(|(key, model)| {
                let provider = self
                    .providers
                    .iter()
                    .find(|provider| provider.descriptor.key.as_ref() == key)?;
                let why = provider.decision_unsupported.get(model)?;
                Some(format!("{model} on {key} {why}"))
            })
            .collect();
        if reasons.is_empty() {
            String::new()
        } else {
            format!(" ({})", reasons.join("; "))
        }
    }
}

/// A live model session: one process (or in-process turn loop) holding a
/// conversation with a provider.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionView {
    /// Cairn's session id, once the run has one.
    pub session_id: Option<String>,
    pub run_id: String,
    /// The node this session belongs to, when the run resolves to one.
    pub node_uri: Option<String>,
    pub provider: String,
    pub model: Option<String>,
    /// The account the session is pinned to, as its launch chose it.
    pub account_id: Option<String>,
    /// The provider's own id for the conversation (the Claude session, the
    /// Codex thread), once known.
    pub backend_session_id: Option<String>,
    pub state: SessionState,
    /// The machine the session runs on. `"local"` (this runner) for every
    /// session today; sessions hosted on an executor will name it here.
    pub host: String,
    /// The OS process id, when the session is a local process.
    pub pid: Option<u32>,
    pub started_at: Option<i64>,
}

impl SessionView {
    /// The id this session is addressed by: its session id, or its run id
    /// before a session exists.
    pub fn key(&self) -> &str {
        self.session_id.as_deref().unwrap_or(&self.run_id)
    }

    /// Whether `id` addresses this session (by session id or run id).
    pub fn is_addressed_by(&self, id: &str) -> bool {
        self.session_id.as_deref() == Some(id) || self.run_id == id
    }
}

/// The host every session runs on until sessions move onto executors.
pub const LOCAL_HOST: &str = "local";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SessionState {
    /// Starting, serving a turn, or holding one open for the host.
    Busy,
    /// Live and idle: parked between turns, resumable without a new process.
    Warm,
}

/// Per-request usage rows do not exist yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestsUnavailable;

impl fmt::Display for RequestsUnavailable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(
            "Per-request usage rows are not recorded yet. What exists today is the provider \
             card: pooled windows and per-account usage at `cairn://models/usage`, and each \
             account's windows at `cairn://models/accounts`. Per-request rows arrive with the \
             provider-payload capture work tracked in the llm thread (cairn://p/cairn/llm).",
        )
    }
}

impl std::error::Error for RequestsUnavailable {}

/// What the facet answers, read side.
///
/// Permissions are not facet flags: each door authorizes its callers the way
/// its existing equivalents do (a session stop carries a node stop's
/// authority), and broader rights become `cairn://grants` scopes.
pub trait Models: Send + Sync {
    /// Providers, their discovered models, and how tiers bind to them.
    fn catalog(&self, scope: &ModelScope) -> Result<Catalog, String>;

    /// The accounts visible in `scope`, without any secret value.
    fn accounts(&self, scope: &ModelScope) -> Result<Vec<AccountInfo>, String>;

    /// Pooled capacity per installed provider (or the one asked for), as
    /// already measured. Never probes.
    fn usage(
        &self,
        scope: &ModelScope,
        provider: Option<&str>,
    ) -> Result<Vec<ProviderPoolSnapshot>, String>;

    /// Per-request usage rows.
    fn requests(&self, _scope: &ModelScope) -> Result<Vec<Value>, RequestsUnavailable> {
        Err(RequestsUnavailable)
    }

    /// Every live session on this host.
    fn sessions(&self) -> Vec<SessionView>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_models_types::catalog::{claude_models, descriptor, DiscoveredModel};

    fn model(id: &str, slug: Option<&str>) -> DiscoveredModel {
        let mut model = claude_models().remove(0);
        model.id = id.to_string();
        model.model = id.to_string();
        model.display_name = id.to_string();
        model.canonical_slug = slug.map(str::to_string);
        model.context_window = Some(128_000);
        model
    }

    fn binding(tier: &str, model: &str) -> TierBinding {
        TierBinding {
            tier: tier.to_string(),
            model: model.to_string(),
            options: BTreeMap::new(),
            is_tier_default: true,
        }
    }

    fn provider(
        key: &str,
        models: Option<Vec<DiscoveredModel>>,
        tiers: Vec<TierBinding>,
    ) -> CatalogProvider {
        CatalogProvider {
            descriptor: descriptor(key).expect("shipped").clone(),
            installed: true,
            models: models.map(|models| ProviderModelCatalog {
                backend: key.to_string(),
                models,
                options: Vec::new(),
                refreshed_at: Some(1),
                error: None,
            }),
            tiers,
            decision: None,
            decision_unsupported: BTreeMap::new(),
        }
    }

    fn deciding(mut provider: CatalogProvider, method: MeasuredBy) -> CatalogProvider {
        provider.decision = Some(method);
        provider
    }

    fn custom(account: &str, models: &[&str], tiers: Vec<TierBinding>) -> CatalogProvider {
        let key = cairn_models_types::catalog::custom_key(account);
        CatalogProvider {
            descriptor: cairn_models_types::catalog::custom_descriptor(
                &cairn_models_types::catalog::CustomProvider {
                    account_id: account.to_string(),
                    display_name: account.to_string(),
                },
            ),
            installed: true,
            models: Some(ProviderModelCatalog {
                backend: key,
                models: models.iter().map(|id| model(id, None)).collect(),
                options: Vec::new(),
                refreshed_at: Some(1),
                error: None,
            }),
            tiers,
            decision: None,
            decision_unsupported: BTreeMap::new(),
        }
    }

    fn subscription_only() -> Catalog {
        Catalog {
            providers: vec![provider(
                "claude",
                Some(claude_models()),
                vec![binding("sm", "haiku"), binding("lg", "opus")],
            )],
        }
    }

    #[test]
    fn offers_state_their_providers_decision_method() {
        let catalog = Catalog {
            providers: vec![
                provider("claude", Some(claude_models()), vec![]),
                deciding(
                    custom("acc_sparks", &["gemma4-26b"], vec![]),
                    MeasuredBy::Logprobs,
                ),
            ],
        };
        let models = catalog.models();
        assert_eq!(find(&models, "claude/opus").offers[0].decision, None);
        let gemma = find(&models, "custom:acc_sparks/gemma4-26b");
        assert_eq!(gemma.offers[0].decision, Some(MeasuredBy::Logprobs));
        let json = serde_json::to_value(&gemma.offers[0]).unwrap();
        assert_eq!(json["decision"], "logprobs");
    }

    #[test]
    fn an_unset_decision_model_takes_a_native_offer_before_a_logprobs_one() {
        let catalog = Catalog {
            providers: vec![
                deciding(
                    custom("acc_sparks", &["gemma4-26b"], vec![]),
                    MeasuredBy::Logprobs,
                ),
                deciding(
                    custom("acc_gateway", &["typesafe-ai/jev"], vec![]),
                    MeasuredBy::Native,
                ),
            ],
        };
        assert_eq!(
            catalog.decision_offer(None).unwrap(),
            decide::DecisionOffer {
                provider: "custom:acc_gateway".into(),
                model: "typesafe-ai/jev".into(),
                method: MeasuredBy::Native,
            }
        );
        assert_eq!(
            catalog.decision_offer(Some("  ")).unwrap().provider,
            "custom:acc_gateway"
        );
    }

    #[test]
    fn a_subscription_only_workspace_refuses_naming_the_fixes() {
        let refusal = subscription_only().decision_offer(None).unwrap_err();
        let text = refusal.to_string();
        assert!(
            text.starts_with("No decision-capable model is configured."),
            "{text}"
        );
        assert!(text.contains("Decisions API key"), "{text}");
        assert!(text.contains("returns logprobs"), "{text}");
        assert!(text.contains("Settings → Providers"), "{text}");
    }

    #[test]
    fn a_tier_resolves_only_on_providers_that_can_decide() {
        let mut catalog = subscription_only();
        let refusal = catalog.decision_offer(Some("sm")).unwrap_err();
        assert_eq!(
            refusal.detail.as_deref(),
            Some("the 'sm' tier is bound only on claude, which cannot answer decisions")
        );

        catalog.providers.push(deciding(
            custom("acc_sparks", &["qwen3-4b"], vec![binding("sm", "qwen3-4b")]),
            MeasuredBy::Logprobs,
        ));
        assert_eq!(
            catalog.decision_offer(Some("sm")).unwrap(),
            decide::DecisionOffer {
                provider: "custom:acc_sparks".into(),
                model: "qwen3-4b".into(),
                method: MeasuredBy::Logprobs,
            }
        );
        // A qualified tier names its provider and nothing else.
        assert!(catalog.decision_offer(Some("claude/sm")).is_err());
        assert_eq!(
            catalog
                .decision_offer(Some("custom:acc_sparks/sm"))
                .unwrap()
                .model,
            "qwen3-4b"
        );
    }

    #[test]
    fn an_exact_model_is_refused_when_only_non_deciding_providers_offer_it() {
        let mut catalog = subscription_only();
        let refusal = catalog.decision_offer(Some("opus")).unwrap_err();
        assert_eq!(refusal.requested.as_deref(), Some("opus"));
        assert_eq!(
            refusal.detail.as_deref(),
            Some("'opus' is offered only by claude, which cannot answer decisions")
        );
        assert!(refusal
            .to_string()
            .starts_with("No decision-capable model serves 'opus': "));

        let unknown = catalog.decision_offer(Some("nothing-like-it")).unwrap_err();
        assert_eq!(
            unknown.detail.as_deref(),
            Some("no installed provider offers 'nothing-like-it'")
        );

        catalog.providers.push(deciding(
            custom("acc_gateway", &["typesafe-ai/jev"], vec![]),
            MeasuredBy::Native,
        ));
        assert_eq!(
            catalog
                .decision_offer(Some("typesafe-ai/jev"))
                .unwrap()
                .method,
            MeasuredBy::Native
        );
    }

    #[test]
    fn a_model_observed_unable_to_decide_is_not_offered_and_the_refusal_says_why() {
        let mut sparks = deciding(
            custom(
                "acc_sparks",
                &["qwen3", "gemma4"],
                vec![binding("sm", "qwen3")],
            ),
            MeasuredBy::Logprobs,
        );
        sparks
            .decision_unsupported
            .insert("qwen3".into(), "returned no logprobs".into());
        let catalog = Catalog {
            providers: vec![sparks],
        };
        // The other model on the same provider still decides.
        let offers = catalog.decision_offers();
        assert_eq!(offers.len(), 1);
        assert_eq!(offers[0].model, "gemma4");
        assert_eq!(catalog.decision_offer(None).unwrap().model, "gemma4");

        let mut only_qwen = catalog.clone();
        only_qwen.providers[0].decision_unsupported.insert(
            "gemma4".into(),
            "began 'The' instead of an answer label".into(),
        );
        let refusal = only_qwen.decision_offer(None).unwrap_err();
        assert_eq!(
            refusal.detail.as_deref(),
            Some("every offer that could was seen unable (gemma4 on custom:acc_sparks began 'The' instead of an answer label; qwen3 on custom:acc_sparks returned no logprobs)")
        );

        for requested in ["qwen3", "sm"] {
            let refusal = catalog.decision_offer(Some(requested)).unwrap_err();
            let detail = refusal.detail.unwrap();
            assert!(
                detail.ends_with("(qwen3 on custom:acc_sparks returned no logprobs)"),
                "{detail}"
            );
        }
    }

    fn find<'a>(models: &'a [CatalogModel], id: &str) -> &'a CatalogModel {
        models
            .iter()
            .find(|model| model.catalog_id == id)
            .unwrap_or_else(|| panic!("no catalog entry {id}"))
    }

    #[test]
    fn offers_sharing_a_canonical_slug_group_under_it() {
        let catalog = Catalog {
            providers: vec![
                provider(
                    "openrouter",
                    Some(vec![model("z-ai/glm-4.6", Some("z-ai/glm-4.6"))]),
                    vec![],
                ),
                provider(
                    "opencode-go",
                    Some(vec![model("glm-4.6", Some("z-ai/glm-4.6"))]),
                    vec![binding("md", "glm-4.6")],
                ),
            ],
        };
        let models = catalog.models();
        assert_eq!(models.len(), 1);
        let glm = find(&models, "z-ai/glm-4.6");
        let providers: Vec<&str> = glm.offers.iter().map(|o| o.provider.as_str()).collect();
        assert_eq!(providers, vec!["openrouter", "opencode-go"]);
        // The tier ref and the native id that differs from the slug.
        assert_eq!(glm.aliases, vec!["opencode-go/md", "glm-4.6"]);
    }

    #[test]
    fn a_model_without_a_slug_is_qualified_by_its_provider_and_never_merged() {
        let catalog = Catalog {
            providers: vec![
                provider(
                    "claude",
                    Some(claude_models()),
                    vec![binding("lg", "opus"), binding("md", "sonnet")],
                ),
                provider("ollama", Some(vec![model("opus", None)]), vec![]),
            ],
        };
        let models = catalog.models();
        let opus = find(&models, "claude/opus");
        assert_eq!(opus.offers.len(), 1);
        assert_eq!(opus.offers[0].context_window, Some(1_000_000));
        assert_eq!(opus.aliases, vec!["claude/lg", "opus"]);
        // Same native id on another provider, no shared slug: its own entry.
        let local = find(&models, "ollama/opus");
        assert_eq!(local.offers[0].provider, "ollama");
        assert_eq!(local.aliases, vec!["opus"]);
    }

    #[test]
    fn a_bound_model_discovery_has_not_reported_still_appears() {
        let catalog = Catalog {
            providers: vec![provider("codex", None, vec![binding("lg", "gpt-5.6-sol")])],
        };
        let models = catalog.models();
        let sol = find(&models, "codex/gpt-5.6-sol");
        assert_eq!(sol.offers[0].context_window, None);
        assert_eq!(sol.aliases, vec!["codex/lg"]);
    }

    #[test]
    fn uninstalled_providers_and_hidden_models_offer_nothing() {
        let mut hidden = model("preview", None);
        hidden.hidden = true;
        let mut absent = provider("openrouter", Some(vec![model("x/y", None)]), vec![]);
        absent.installed = false;
        let catalog = Catalog {
            providers: vec![provider("ollama", Some(vec![hidden]), vec![]), absent],
        };
        assert!(catalog.models().is_empty());
    }

    #[test]
    fn a_session_is_addressed_by_session_id_or_run_id() {
        let view = SessionView {
            session_id: Some("s1".into()),
            run_id: "r1".into(),
            node_uri: None,
            provider: "claude".into(),
            model: None,
            account_id: None,
            backend_session_id: Some("thread-1".into()),
            state: SessionState::Warm,
            host: LOCAL_HOST.into(),
            pid: None,
            started_at: None,
        };
        assert_eq!(view.key(), "s1");
        let json = serde_json::to_value(&view).unwrap();
        assert_eq!(json["host"], "local");
        assert!(json.get("pid").is_some());
        assert_eq!(json["backendSessionId"], "thread-1");
        assert!(view.is_addressed_by("s1") && view.is_addressed_by("r1"));
        assert!(!view.is_addressed_by("r2"));
    }

    #[test]
    fn the_requests_refusal_points_at_what_exists() {
        let text = RequestsUnavailable.to_string();
        assert!(text.contains("cairn://models/usage"));
        assert!(text.contains("cairn://models/accounts"));
        assert!(text.contains("cairn://p/cairn/llm"));
    }
}
