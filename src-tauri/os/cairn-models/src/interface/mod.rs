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
//! protocol families), [`session`] (a CLI session opened on a host and
//! driven through its control), [`decide`] (typed questions answered with
//! measured probabilities), [`image`], and [`speak`] (a text stream in, an
//! audio stream out).
//!
//! Every answer reuses the facet's existing vocabulary: accounts are
//! [`AccountInfo`], usage is [`ProviderPoolSnapshot`], providers are
//! [`ProviderDescriptor`], discovered models are [`ProviderModelCatalog`]. The
//! types defined here exist only where nothing did before: the composed
//! catalog, its catalog-id view, the session view, and the requests refusal.

pub mod complete;
pub mod decide;
pub mod image;
pub mod session;
pub mod speak;

use std::collections::BTreeMap;
use std::fmt;

use cairn_credentials::AccountInfo;
use cairn_domain::decision::MeasuredBy;
use cairn_models_types::catalog::{
    DiscoveredModel, Modalities, ModelPricing, ProviderDescriptor, ProviderModelCatalog,
    ServedVerb, Verb, VerbKind,
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
    /// What this provider serves for any model it routes, each verb with the
    /// wire that carries it. A model whose discovery says what it serves
    /// ([`DiscoveredModel::serves`]) answers for itself; every other model,
    /// including one a tier is bound to that discovery has not reported,
    /// serves this.
    pub serves: Vec<ServedVerb>,
    /// Verbs models on this provider were observed unable to serve despite
    /// what they declare (a chat server that returned no logprobs, or a model
    /// that reasons before it answers), each with why. Those offers do not
    /// serve that verb.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub unable: Vec<Unable>,
}

/// A model observed unable to serve a verb it declares.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Unable {
    pub model: String,
    pub verb: VerbKind,
    /// Why, as a clause that reads after the model's name.
    pub why: String,
}

impl CatalogProvider {
    /// What `model` on this provider serves: its own declaration when
    /// discovery made one, otherwise the provider's, minus every verb it was
    /// observed unable to serve.
    pub fn serves_for(&self, model: &str) -> Vec<ServedVerb> {
        let declared = self
            .discovered(model)
            .and_then(|discovered| discovered.serves.as_deref());
        self.serves_declared(model, declared)
    }

    /// Whether `model` here serves `verb`: the one answer every gate and
    /// picker asks.
    pub fn serves(&self, model: &str, verb: VerbKind) -> bool {
        best(&self.serves_for(model), verb).is_some()
    }

    /// The verb `model` serves over its best wire, or a refusal naming the
    /// model and what it does serve.
    ///
    /// Unlike [`Catalog::offer_for`], a model discovery never listed is not
    /// refused for that alone: it serves what the provider serves for any
    /// model, which is how a Claude alias or a freshly released id launches.
    pub fn admit(&self, model: &str, verb: VerbKind) -> Result<ServedVerb, NoOffer> {
        let serves = self.serves_for(model);
        if let Some(served) = best(&serves, verb) {
            return Ok(served);
        }
        let key = self.descriptor.key.as_ref();
        let detail = if let Some(why) = self.unable_to(model, verb) {
            format!("'{model}' on {key} {why}")
        } else if self.discovered(model).is_none() && best(&self.serves, verb).is_none() {
            format!(
                "{key} has not listed '{model}' and serves nothing for a model it has not \
                 listed; refresh its catalog in Settings → Providers, or pick a listed model"
            )
        } else if serves.is_empty() {
            format!("'{model}' on {key} serves nothing Cairn runs")
        } else {
            format!(
                "'{model}' on {key} serves only {}",
                kinds(&serves)
                    .iter()
                    .map(|kind| kind.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        Err(NoOffer {
            verb,
            requested: Some(model.to_string()),
            detail: Some(detail),
        })
    }

    /// [`Self::serves_for`] resolved for every model this provider can name
    /// (each discovered model, hidden ones included; each model a tier binds;
    /// each model an [`Unable`] names), for hosts that must not re-derive it.
    pub fn resolved_serves(&self) -> ResolvedServes {
        let discovered = self
            .models
            .as_ref()
            .map(|catalog| catalog.models.as_slice())
            .unwrap_or_default();
        let named = discovered
            .iter()
            .flat_map(|model| [model.model.as_str(), model.id.as_str()])
            .chain(self.tiers.iter().map(|binding| binding.model.as_str()))
            .chain(self.unable.iter().map(|unable| unable.model.as_str()));
        let mut models = BTreeMap::new();
        for model in named {
            if !models.contains_key(model) {
                models.insert(model.to_string(), kinds(&self.serves_for(model)));
            }
        }
        ResolvedServes { models }
    }

    /// Why `model` was observed unable to serve `verb`, when it was.
    pub fn unable_to(&self, model: &str, verb: VerbKind) -> Option<&str> {
        self.unable
            .iter()
            .find(|unable| unable.model == model && unable.verb == verb)
            .map(|unable| unable.why.as_str())
    }

    fn serves_declared(&self, model: &str, declared: Option<&[ServedVerb]>) -> Vec<ServedVerb> {
        declared
            .unwrap_or(&self.serves)
            .iter()
            .filter(|served| self.unable_to(model, served.verb.kind()).is_none())
            .cloned()
            .collect()
    }

    fn discovered(&self, model: &str) -> Option<&DiscoveredModel> {
        self.models
            .as_ref()?
            .models
            .iter()
            .find(|discovered| discovered.model == model || discovered.id == model)
    }

    /// Whether `account` (when one is asked for) serves `model` here. A model
    /// that names no serving accounts is served by every account of its
    /// provider.
    fn account_serves(&self, model: &str, account: Option<&str>) -> bool {
        let Some(account) = account else {
            return true;
        };
        self.discovered(model).is_none_or(|discovered| {
            discovered.serving_account_ids.is_empty()
                || discovered
                    .serving_account_ids
                    .iter()
                    .any(|id| id == account)
        })
    }
}

/// What each model a provider can name serves, by verb: [`CatalogProvider::serves_for`]
/// already resolved, so a host reads the answer instead of re-deriving the
/// rule.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedServes {
    pub models: BTreeMap<String, Vec<VerbKind>>,
}

/// The distinct verbs `serves` serves, in the order it lists them.
fn kinds(serves: &[ServedVerb]) -> Vec<VerbKind> {
    let mut kinds = Vec::new();
    for served in serves {
        let kind = served.verb.kind();
        if !kinds.contains(&kind) {
            kinds.push(kind);
        }
    }
    kinds
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
    /// What the model takes in and gives out, when its provider said.
    pub modalities: Option<Modalities>,
    pub pricing: Option<ModelPricing>,
    /// What this offer serves, each verb with its wire. Whether a given
    /// decision was measured natively is recorded on the answer itself, since
    /// tier resolution picks the offer at run time.
    pub serves: Vec<ServedVerb>,
    /// The accounts that serve this offer. Empty means every account of its
    /// provider.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub serving_account_ids: Vec<String>,
}

impl CatalogOffer {
    /// This offer as the server of `verb`, over its best wire for it, when it
    /// serves it.
    pub fn selected(&self, verb: VerbKind) -> Option<SelectedOffer> {
        Some(SelectedOffer {
            provider: self.provider.clone(),
            model: self.model.clone(),
            served: best(&self.serves, verb)?,
        })
    }
}

/// What a caller asks the catalog for: an offer serving `verb`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OfferQuery<'a> {
    pub verb: VerbKind,
    /// The model or tier an author wrote. Unset, or blank, takes the best
    /// offer.
    pub requested: Option<&'a str>,
    /// Only this provider's offers.
    pub provider: Option<&'a str>,
    /// Only offers this account serves. A custom provider's key already is
    /// its account; this narrows providers whose models are served per
    /// account (Ollama hosts, Actual targets).
    pub account: Option<&'a str>,
}

impl<'a> OfferQuery<'a> {
    /// Any offer serving `verb`.
    pub fn new(verb: VerbKind) -> Self {
        Self {
            verb,
            requested: None,
            provider: None,
            account: None,
        }
    }

    fn admits_provider(&self, key: &str) -> bool {
        self.provider.is_none_or(|provider| provider == key)
    }

    fn admits_offer(&self, offer: &CatalogOffer) -> bool {
        self.admits_provider(&offer.provider)
            && self.account.is_none_or(|account| {
                offer.serving_account_ids.is_empty()
                    || offer.serving_account_ids.iter().any(|id| id == account)
            })
    }
}

/// The offer a verb will be served by, as the catalog chose it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectedOffer {
    /// The provider's backend key.
    pub provider: String,
    /// The id the provider is sent.
    pub model: String,
    /// The verb it serves and the wire that carries it.
    pub served: ServedVerb,
}

impl SelectedOffer {
    /// How this offer measures a decision, when what it serves is `decide`.
    pub fn measured_by(&self) -> Option<MeasuredBy> {
        match self.served.verb {
            Verb::Decide => decide::measured_by(self.served.wire),
            Verb::Complete | Verb::Session | Verb::Image(_) | Verb::Speak(_) => None,
        }
    }
}

/// Nothing in scope serves a verb, or nothing serves it for what was asked.
/// The text names the fixes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NoOffer {
    pub verb: VerbKind,
    /// The model or tier the caller asked for, when it asked for one.
    pub requested: Option<String>,
    /// Why the request found nothing, when there is more to say than that
    /// nothing is configured.
    pub detail: Option<String>,
}

impl fmt::Display for NoOffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let capable = capable(self.verb);
        match (&self.requested, &self.detail) {
            (Some(requested), _) => write!(f, "No {capable} model serves '{requested}'")?,
            // Offers exist, but none of them could take this request.
            (None, Some(_)) => write!(f, "No {capable} model is available")?,
            (None, None) => write!(f, "No {capable} model is configured")?,
        }
        if let Some(detail) = &self.detail {
            write!(f, ": {detail}")?;
        }
        write!(f, ". {}", fixes(self.verb))
    }
}

impl std::error::Error for NoOffer {}

/// The fixes a completion or session refusal names.
const CONVERSATION_FIXES: &str =
    "To fix it, install or configure a provider in Settings → Providers.";

/// The adjective a refusal gives a model that serves `verb`.
fn capable(verb: VerbKind) -> &'static str {
    match verb {
        VerbKind::Complete => "completion-capable",
        VerbKind::Session => "session-capable",
        VerbKind::Decide => "decision-capable",
        VerbKind::Image => "image-capable",
        VerbKind::Speak => "speech-capable",
    }
}

/// What a refusal says of providers that do not serve `verb`.
fn cannot(verb: VerbKind) -> &'static str {
    match verb {
        VerbKind::Complete => "cannot serve completions",
        VerbKind::Session => "cannot run sessions",
        VerbKind::Decide => "cannot answer decisions",
        VerbKind::Image => "cannot generate images",
        VerbKind::Speak => "cannot speak",
    }
}

/// The fixes every refusal of `verb` names.
pub fn fixes(verb: VerbKind) -> &'static str {
    match verb {
        VerbKind::Complete | VerbKind::Session => CONVERSATION_FIXES,
        VerbKind::Decide => decide::DECISION_FIXES,
        VerbKind::Image => image::IMAGE_FIXES,
        VerbKind::Speak => speak::SPEECH_FIXES,
    }
}

/// How offers serving the same verb rank, lower first. A decision measured
/// natively comes before one read from logprobs, and speech that streams its
/// input natively before speech cut into segments; every other verb ranks
/// flat, which leaves the workspace's provider order to decide.
fn preference(served: &ServedVerb) -> u8 {
    match &served.verb {
        Verb::Decide if decide::measured_by(served.wire) == Some(MeasuredBy::Native) => 0,
        Verb::Decide => 1,
        Verb::Speak(capabilities) => match capabilities.input {
            speak::InputStreaming::Native => 0,
            speak::InputStreaming::Segmented => 1,
        },
        Verb::Complete | Verb::Session | Verb::Image(_) => 0,
    }
}

/// The best way `serves` serves `verb`, when it does: the first of the best
/// [`preference`].
fn best(serves: &[ServedVerb], verb: VerbKind) -> Option<ServedVerb> {
    serves
        .iter()
        .filter(|served| served.verb.kind() == verb)
        .min_by_key(|served| preference(served))
        .cloned()
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
                        modalities: model.modalities.clone(),
                        pricing: model.pricing.clone(),
                        serves: provider.serves_declared(&model.model, model.serves.as_deref()),
                        serving_account_ids: model.serving_account_ids.clone(),
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
                        modalities: None,
                        pricing: None,
                        serves: provider.serves_declared(&binding.model, None),
                        serving_account_ids: Vec::new(),
                    },
                    tier_refs(&[binding.model.as_str()]),
                );
            }
        }
        out
    }

    /// Every offer serving `verb`, best first: by its wire's preference, then
    /// in the workspace's provider order.
    pub fn offers_serving(&self, verb: VerbKind) -> Vec<SelectedOffer> {
        self.candidates(&OfferQuery::new(verb))
    }

    /// The offer that serves `query.verb` for what was asked.
    ///
    /// `requested` is what an author wrote. Unset takes the best offer
    /// serving the verb ([`Self::offers_serving`]). A tier (`sm`, or
    /// `provider/sm`) takes that tier's binding on a provider that serves the
    /// verb, the best wire first and then the tier's default provider.
    /// Anything else is an exact model, matched by native id, catalog id, or
    /// alias. A selection that lands only on offers that do not serve the
    /// verb is refused, never served by something that does. `provider` and
    /// `account` narrow every branch.
    pub fn offer_for(&self, query: OfferQuery<'_>) -> Result<SelectedOffer, NoOffer> {
        self.ranked(&query).map(|mut offers| offers.remove(0))
    }

    /// The best offer for `query` that `admit` accepts, with what `admit`
    /// returned for it: the request-aware walk a verb with per-request
    /// capabilities (an image's mask, a voice's streaming) selects with.
    ///
    /// It walks the same ranked offers [`Self::offer_for`] would pick from. A
    /// refusal names, three at most, each offer that declined and why; when a
    /// model or tier was asked for, it also names up to three offers of the
    /// verb that would admit the request.
    pub fn admitting_offer<A, E: fmt::Display>(
        &self,
        query: OfferQuery<'_>,
        admit: impl Fn(&SelectedOffer) -> Result<A, E>,
    ) -> Result<(SelectedOffer, A), NoOffer> {
        let ranked = self.ranked(&query)?;
        let mut declined = Vec::new();
        for offer in ranked {
            match admit(&offer) {
                Ok(admitted) => return Ok((offer, admitted)),
                Err(why) => declined.push(format!("{} on {}: {why}", offer.model, offer.provider)),
            }
        }
        let mut detail = declined
            .iter()
            .take(3)
            .cloned()
            .collect::<Vec<_>>()
            .join("; ");
        if declined.len() > 3 {
            detail.push_str(&format!("; and {} more", declined.len() - 3));
        }
        let requested = query
            .requested
            .map(str::trim)
            .filter(|value| !value.is_empty());
        if requested.is_some() {
            let would: Vec<String> = self
                .candidates(&OfferQuery {
                    requested: None,
                    ..query
                })
                .into_iter()
                .filter(|offer| admit(offer).is_ok())
                .take(3)
                .map(|offer| format!("{}/{}", offer.provider, offer.model))
                .collect();
            if !would.is_empty() {
                detail.push_str(&format!("; offers that would: {}", would.join(", ")));
            }
        }
        Err(NoOffer {
            verb: query.verb,
            requested: requested.map(str::to_string),
            detail: Some(detail),
        })
    }

    /// Every offer that serves `query.verb` for what was asked, best first;
    /// never empty. This is [`Self::offer_for`]'s selection, kept whole so a
    /// request-aware walk can move past an offer that cannot take it.
    fn ranked(&self, query: &OfferQuery<'_>) -> Result<Vec<SelectedOffer>, NoOffer> {
        let query = *query;
        let verb = query.verb;
        let requested = query
            .requested
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let refuse = |detail: Option<String>| NoOffer {
            verb,
            requested: requested.map(str::to_string),
            detail,
        };
        let Some(requested) = requested else {
            return Some(self.candidates(&query))
                .filter(|candidates| !candidates.is_empty())
                .ok_or_else(|| {
                    // Say why offers that would have served it did not.
                    let unable: Vec<String> = self
                        .installed()
                        .filter(|provider| query.admits_provider(&provider.descriptor.key))
                        .flat_map(|provider| {
                            let mut unable: Vec<&Unable> = provider
                                .unable
                                .iter()
                                .filter(|unable| unable.verb == verb)
                                .collect();
                            unable.sort_by(|a, b| a.model.cmp(&b.model));
                            unable.into_iter().map(move |unable| {
                                format!(
                                    "{} on {} {}",
                                    unable.model, provider.descriptor.key, unable.why
                                )
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
        let bindings: Vec<(&CatalogProvider, &TierBinding)> = self
            .installed()
            .filter(|provider| {
                let key = provider.descriptor.key.as_ref();
                query.admits_provider(key) && qualifier.is_none_or(|qualifier| qualifier == key)
            })
            .flat_map(|provider| {
                provider
                    .tiers
                    .iter()
                    .filter(|binding| binding.tier == tier)
                    .map(move |binding| (provider, binding))
            })
            .collect();
        if !bindings.is_empty() {
            let mut picks: Vec<(SelectedOffer, bool)> = bindings
                .iter()
                .filter(|(provider, binding)| {
                    provider.account_serves(&binding.model, query.account)
                })
                .filter_map(|(provider, binding)| {
                    let offer = SelectedOffer {
                        provider: provider.descriptor.key.to_string(),
                        model: binding.model.clone(),
                        served: best(&provider.serves_for(&binding.model), verb)?,
                    };
                    Some((offer, binding.is_tier_default))
                })
                .collect();
            // Stable: provider order survives within each rank.
            picks.sort_by_key(|(offer, is_tier_default)| {
                (preference(&offer.served), !is_tier_default)
            });
            return Some(
                picks
                    .into_iter()
                    .map(|(offer, _)| offer)
                    .collect::<Vec<_>>(),
            )
            .filter(|picks| !picks.is_empty())
            .ok_or_else(|| {
                refuse(Some(format!(
                    "the '{requested}' tier is bound only on {}, which {}{}",
                    bindings
                        .iter()
                        .map(|(provider, _)| provider.descriptor.key.as_ref())
                        .collect::<Vec<_>>()
                        .join(", "),
                    cannot(verb),
                    self.observed_unable(
                        verb,
                        bindings.iter().map(|(provider, binding)| {
                            (provider.descriptor.key.as_ref(), binding.model.as_str())
                        })
                    )
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
            .filter(|offer| query.admits_offer(offer))
            .collect();
        if matching.is_empty() {
            return Err(refuse(Some(format!(
                "no installed provider offers '{requested}'"
            ))));
        }
        let mut capable: Vec<SelectedOffer> = matching
            .iter()
            .filter_map(|offer| offer.selected(verb))
            .collect();
        capable.sort_by_key(|offer| preference(&offer.served));
        Some(capable)
            .filter(|capable| !capable.is_empty())
            .ok_or_else(|| {
                let mut providers: Vec<&str> = matching
                    .iter()
                    .map(|offer| offer.provider.as_str())
                    .collect();
                providers.dedup();
                refuse(Some(format!(
                    "'{requested}' is offered only by {}, which {}{}",
                    providers.join(", "),
                    cannot(verb),
                    self.observed_unable(
                        verb,
                        matching
                            .iter()
                            .map(|offer| (offer.provider.as_str(), offer.model.as_str()))
                    )
                )))
            })
    }

    /// Every offer `query` admits that serves its verb, best first.
    fn candidates(&self, query: &OfferQuery<'_>) -> Vec<SelectedOffer> {
        let mut offers: Vec<SelectedOffer> = self
            .models()
            .into_iter()
            .flat_map(|model| model.offers)
            .filter(|offer| query.admits_offer(offer))
            .filter_map(|offer| offer.selected(query.verb))
            .collect();
        // Stable: provider order survives within each rank.
        offers.sort_by_key(|offer| preference(&offer.served));
        offers
    }

    /// Why the named offers were observed unable to serve `verb`, as a clause
    /// to append to a refusal; empty when none was.
    fn observed_unable<'a>(
        &self,
        verb: VerbKind,
        offers: impl Iterator<Item = (&'a str, &'a str)>,
    ) -> String {
        let reasons: Vec<String> = offers
            .filter_map(|(key, model)| {
                let provider = self
                    .providers
                    .iter()
                    .find(|provider| provider.descriptor.key.as_ref() == key)?;
                let why = provider.unable_to(model, verb)?;
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
    use cairn_models_types::catalog::{claude_models, descriptor, DiscoveredModel, Wire};

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
            serves: Vec::new(),
            unable: Vec::new(),
        }
    }

    fn decision_wire(method: MeasuredBy) -> Wire {
        match method {
            MeasuredBy::Native => Wire::OpenAiDecisions,
            MeasuredBy::Logprobs => Wire::OpenAiChatCompletions,
        }
    }

    fn deciding(mut provider: CatalogProvider, method: MeasuredBy) -> CatalogProvider {
        provider.serves = vec![ServedVerb {
            verb: Verb::Decide,
            wire: decision_wire(method),
        }];
        provider
    }

    fn conversing(mut provider: CatalogProvider) -> CatalogProvider {
        provider.serves = ServedVerb::conversation(Wire::OpenAiChatCompletions);
        provider
    }

    fn decide(catalog: &Catalog, requested: Option<&str>) -> Result<SelectedOffer, NoOffer> {
        catalog.offer_for(OfferQuery {
            requested,
            ..OfferQuery::new(VerbKind::Decide)
        })
    }

    fn decision(provider: &str, model: &str, method: MeasuredBy) -> SelectedOffer {
        SelectedOffer {
            provider: provider.into(),
            model: model.into(),
            served: ServedVerb {
                verb: Verb::Decide,
                wire: decision_wire(method),
            },
        }
    }

    fn unable(model: &str, why: &str) -> Unable {
        Unable {
            model: model.into(),
            verb: VerbKind::Decide,
            why: why.into(),
        }
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
            serves: Vec::new(),
            unable: Vec::new(),
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
    fn offers_state_what_their_provider_serves() {
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
        assert_eq!(
            find(&models, "claude/opus").offers[0].selected(VerbKind::Decide),
            None
        );
        let gemma = find(&models, "custom:acc_sparks/gemma4-26b");
        assert_eq!(
            gemma.offers[0]
                .selected(VerbKind::Decide)
                .and_then(|offer| offer.measured_by()),
            Some(MeasuredBy::Logprobs)
        );
        let json = serde_json::to_value(&gemma.offers[0]).unwrap();
        assert_eq!(
            json["serves"],
            serde_json::json!([{ "verb": "decide", "wire": "openai-chat" }])
        );
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
            decide(&catalog, None).unwrap(),
            decision("custom:acc_gateway", "typesafe-ai/jev", MeasuredBy::Native)
        );
        assert_eq!(
            decide(&catalog, Some("  ")).unwrap().provider,
            "custom:acc_gateway"
        );
    }

    #[test]
    fn a_subscription_only_workspace_refuses_naming_the_fixes() {
        let refusal = decide(&subscription_only(), None).unwrap_err();
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
        let refusal = decide(&catalog, Some("sm")).unwrap_err();
        assert_eq!(
            refusal.detail.as_deref(),
            Some("the 'sm' tier is bound only on claude, which cannot answer decisions")
        );

        catalog.providers.push(deciding(
            custom("acc_sparks", &["qwen3-4b"], vec![binding("sm", "qwen3-4b")]),
            MeasuredBy::Logprobs,
        ));
        assert_eq!(
            decide(&catalog, Some("sm")).unwrap(),
            decision("custom:acc_sparks", "qwen3-4b", MeasuredBy::Logprobs)
        );
        // A qualified tier names its provider and nothing else.
        assert!(decide(&catalog, Some("claude/sm")).is_err());
        assert_eq!(
            decide(&catalog, Some("custom:acc_sparks/sm"))
                .unwrap()
                .model,
            "qwen3-4b"
        );
    }

    #[test]
    fn an_exact_model_is_refused_when_only_non_deciding_providers_offer_it() {
        let mut catalog = subscription_only();
        let refusal = decide(&catalog, Some("opus")).unwrap_err();
        assert_eq!(refusal.requested.as_deref(), Some("opus"));
        assert_eq!(
            refusal.detail.as_deref(),
            Some("'opus' is offered only by claude, which cannot answer decisions")
        );
        assert!(refusal
            .to_string()
            .starts_with("No decision-capable model serves 'opus': "));

        let unknown = decide(&catalog, Some("nothing-like-it")).unwrap_err();
        assert_eq!(
            unknown.detail.as_deref(),
            Some("no installed provider offers 'nothing-like-it'")
        );

        catalog.providers.push(deciding(
            custom("acc_gateway", &["typesafe-ai/jev"], vec![]),
            MeasuredBy::Native,
        ));
        assert_eq!(
            decide(&catalog, Some("typesafe-ai/jev"))
                .unwrap()
                .measured_by(),
            Some(MeasuredBy::Native)
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
        sparks.unable.push(unable("qwen3", "returned no logprobs"));
        let catalog = Catalog {
            providers: vec![sparks],
        };
        // The other model on the same provider still decides.
        let offers = catalog.offers_serving(VerbKind::Decide);
        assert_eq!(offers.len(), 1);
        assert_eq!(offers[0].model, "gemma4");
        assert_eq!(decide(&catalog, None).unwrap().model, "gemma4");

        let mut only_qwen = catalog.clone();
        only_qwen.providers[0]
            .unable
            .push(unable("gemma4", "began 'The' instead of an answer label"));
        let refusal = decide(&only_qwen, None).unwrap_err();
        assert_eq!(
            refusal.detail.as_deref(),
            Some("every offer that could was seen unable (gemma4 on custom:acc_sparks began 'The' instead of an answer label; qwen3 on custom:acc_sparks returned no logprobs)")
        );

        for requested in ["qwen3", "sm"] {
            let refusal = decide(&catalog, Some(requested)).unwrap_err();
            let detail = refusal.detail.unwrap();
            assert!(
                detail.ends_with("(qwen3 on custom:acc_sparks returned no logprobs)"),
                "{detail}"
            );
        }
    }

    #[test]
    fn a_models_own_declaration_stands_in_for_its_providers() {
        // An image model that outputs no text serves nothing Cairn has, while
        // a model discovery made no claim about serves what its provider does.
        let mut image = model("google/image-only", None);
        image.serves = Some(Vec::new());
        let chat = model("openai/gpt", None);
        let mut responses = model("openai/codex", None);
        responses.serves = Some(ServedVerb::conversation(Wire::OpenAiResponses));
        let catalog = Catalog {
            providers: vec![conversing(provider(
                "openrouter",
                Some(vec![image, chat, responses]),
                vec![],
            ))],
        };
        let offers = catalog.offers_serving(VerbKind::Complete);
        assert_eq!(
            offers
                .iter()
                .map(|offer| (offer.model.as_str(), offer.served.wire))
                .collect::<Vec<_>>(),
            [
                ("openai/gpt", Wire::OpenAiChatCompletions),
                ("openai/codex", Wire::OpenAiResponses),
            ]
        );
        let refusal = catalog
            .offer_for(OfferQuery {
                requested: Some("google/image-only"),
                ..OfferQuery::new(VerbKind::Session)
            })
            .unwrap_err();
        assert_eq!(
            refusal.detail.as_deref(),
            Some("'google/image-only' is offered only by openrouter, which cannot run sessions")
        );
    }

    #[test]
    fn provider_and_account_narrow_every_selection() {
        let mut on_low = model("qwen3", None);
        on_low.serving_account_ids = vec!["low".into()];
        let catalog = Catalog {
            providers: vec![
                conversing(provider(
                    "ollama",
                    Some(vec![on_low]),
                    vec![binding("sm", "qwen3")],
                )),
                conversing(provider(
                    "openrouter",
                    Some(vec![model("qwen3", None)]),
                    vec![],
                )),
            ],
        };
        let complete = |provider: Option<&'static str>,
                        account: Option<&'static str>,
                        requested: Option<&'static str>| {
            catalog.offer_for(OfferQuery {
                verb: VerbKind::Complete,
                requested,
                provider,
                account,
            })
        };
        assert_eq!(
            complete(None, None, Some("qwen3")).unwrap().provider,
            "ollama"
        );
        assert_eq!(
            complete(Some("openrouter"), None, Some("qwen3"))
                .unwrap()
                .provider,
            "openrouter"
        );
        assert_eq!(
            complete(Some("ollama"), Some("low"), Some("sm"))
                .unwrap()
                .model,
            "qwen3"
        );
        assert!(complete(Some("ollama"), Some("high"), Some("qwen3")).is_err());
        assert!(complete(Some("ollama"), Some("high"), Some("sm")).is_err());
        // An offer that names no serving account is served by every account.
        assert_eq!(
            complete(None, Some("high"), None).unwrap().provider,
            "openrouter"
        );
    }

    #[test]
    fn the_request_aware_walk_moves_past_offers_that_decline_and_names_them() {
        let catalog = Catalog {
            providers: vec![conversing(provider(
                "openrouter",
                Some(vec![model("a", None), model("b", None), model("c", None)]),
                vec![],
            ))],
        };
        let query = OfferQuery::new(VerbKind::Complete);
        let (offer, admitted) = catalog
            .admitting_offer(query, |offer| {
                if offer.model == "b" {
                    Ok(7)
                } else {
                    Err("too small")
                }
            })
            .unwrap();
        assert_eq!((offer.model.as_str(), admitted), ("b", 7));

        let refusal = catalog
            .admitting_offer(query, |_| Err::<(), _>("too small"))
            .unwrap_err();
        assert_eq!(
            refusal.to_string(),
            "No completion-capable model is available: a on openrouter: too small; b on openrouter: too small; c on openrouter: too small. To fix it, install or configure a provider in Settings → Providers."
        );

        let pinned = catalog
            .admitting_offer(
                OfferQuery {
                    requested: Some("a"),
                    ..query
                },
                |offer| {
                    if offer.model == "a" {
                        Err("too small")
                    } else {
                        Ok(())
                    }
                },
            )
            .unwrap_err();
        assert_eq!(
            pinned.detail.as_deref(),
            Some("a on openrouter: too small; offers that would: openrouter/b, openrouter/c")
        );
        // An unknown model is refused as it always was.
        assert!(catalog
            .admitting_offer(
                OfferQuery {
                    requested: Some("zzz"),
                    ..query
                },
                |_| Ok::<_, &str>(())
            )
            .unwrap_err()
            .detail
            .unwrap()
            .contains("no installed provider offers 'zzz'"));
    }

    #[test]
    fn a_verb_nothing_serves_refuses_with_its_own_fixes() {
        let refusal = subscription_only()
            .offer_for(OfferQuery::new(VerbKind::Session))
            .unwrap_err();
        assert_eq!(
            refusal.to_string(),
            "No session-capable model is configured. To fix it, install or configure a provider in Settings → Providers."
        );
        let refusal = subscription_only()
            .offer_for(OfferQuery {
                requested: Some("opus"),
                ..OfferQuery::new(VerbKind::Complete)
            })
            .unwrap_err();
        assert!(
            refusal
                .to_string()
                .starts_with("No completion-capable model serves 'opus': 'opus' is offered only by claude, which cannot serve completions. To fix it"),
            "{refusal}"
        );
    }

    fn image_only(model: &str) -> DiscoveredModel {
        let mut image = crate::image::codex::discovered_model();
        image.id = model.to_string();
        image.model = model.to_string();
        image
    }

    fn unable_to_complete(model: &str, why: &str) -> Unable {
        Unable {
            model: model.into(),
            verb: VerbKind::Complete,
            why: why.into(),
        }
    }

    #[test]
    fn a_model_serves_its_declaration_or_its_providers_minus_what_it_could_not() {
        let mut nothing = model("embed-ish", None);
        nothing.serves = Some(Vec::new());
        let mut codex = conversing(provider(
            "codex",
            Some(vec![
                image_only("gpt-image-2"),
                model("gpt-5.5", None),
                nothing,
            ]),
            vec![],
        ));
        assert!(codex.serves("gpt-image-2", VerbKind::Image));
        assert!(!codex.serves("gpt-image-2", VerbKind::Complete));
        assert!(!codex.serves("gpt-image-2", VerbKind::Session));
        // No declaration: the provider-wide fallback.
        assert!(codex.serves("gpt-5.5", VerbKind::Complete));
        assert!(codex.serves("never-listed", VerbKind::Session));
        assert!(!codex.serves("embed-ish", VerbKind::Complete));

        codex
            .unable
            .push(unable_to_complete("gpt-5.5", "reasons before it answers"));
        assert!(!codex.serves("gpt-5.5", VerbKind::Complete));
        assert!(codex.serves("gpt-5.5", VerbKind::Session));

        // OpenCode Go declares nothing provider-wide: a model whose discovery
        // made no claim serves nothing.
        let go = provider(
            "opencode-go",
            Some(vec![model("unpublished", None)]),
            vec![],
        );
        assert!(!go.serves("unpublished", VerbKind::Session));
        assert!(!go.serves("unpublished", VerbKind::Complete));
    }

    #[test]
    fn admit_names_the_model_and_what_it_does_serve() {
        let mut nothing = model("embed-ish", None);
        nothing.serves = Some(Vec::new());
        let mut codex = conversing(provider(
            "codex",
            Some(vec![
                image_only("gpt-image-2"),
                model("gpt-5.5", None),
                nothing,
            ]),
            vec![],
        ));
        codex
            .unable
            .push(unable_to_complete("gpt-5.5", "reasons before it answers"));

        assert_eq!(
            codex.admit("gpt-5.5", VerbKind::Session).unwrap().verb,
            Verb::Session
        );
        assert_eq!(
            codex.admit("gpt-image-2", VerbKind::Session).unwrap_err().to_string(),
            "No session-capable model serves 'gpt-image-2': 'gpt-image-2' on codex serves only image. To fix it, install or configure a provider in Settings → Providers."
        );
        assert_eq!(
            codex
                .admit("embed-ish", VerbKind::Complete)
                .unwrap_err()
                .detail
                .as_deref(),
            Some("'embed-ish' on codex serves nothing Cairn runs")
        );
        assert_eq!(
            codex
                .admit("gpt-5.5", VerbKind::Complete)
                .unwrap_err()
                .detail
                .as_deref(),
            Some("'gpt-5.5' on codex reasons before it answers")
        );

        let go = provider("opencode-go", Some(vec![model("listed", None)]), vec![]);
        let refusal = go.admit("glm-9", VerbKind::Session).unwrap_err();
        assert_eq!(refusal.requested.as_deref(), Some("glm-9"));
        assert_eq!(
            refusal.detail.as_deref(),
            Some("opencode-go has not listed 'glm-9' and serves nothing for a model it has not listed; refresh its catalog in Settings → Providers, or pick a listed model")
        );
        // Listed without a declaration: it serves nothing, said plainly.
        assert_eq!(
            go.admit("listed", VerbKind::Session)
                .unwrap_err()
                .detail
                .as_deref(),
            Some("'listed' on opencode-go serves nothing Cairn runs")
        );
    }

    #[test]
    fn resolved_serves_agrees_with_serves_for_on_every_model_the_provider_names() {
        let mut hidden = model("preview", None);
        hidden.hidden = true;
        let mut codex = conversing(provider(
            "codex",
            Some(vec![
                image_only("gpt-image-2"),
                model("gpt-5.5", None),
                hidden,
            ]),
            vec![binding("lg", "gpt-5.6-sol"), binding("sm", "gpt-image-2")],
        ));
        codex.unable.push(unable_to_complete(
            "o-reasoner",
            "reasons before it answers",
        ));
        let mut published = model("kimi-k3", None);
        published.serves = Some(ServedVerb::conversation(Wire::OpenAiChatCompletions));
        let go = provider(
            "opencode-go",
            Some(vec![model("unpublished", None), published]),
            vec![],
        );

        for provider in [&codex, &go] {
            let resolved = provider.resolved_serves();
            for (model, verbs) in &resolved.models {
                assert_eq!(verbs, &kinds(&provider.serves_for(model)), "{model}");
            }
        }
        let codex_resolved = codex.resolved_serves().models;
        assert_eq!(
            codex_resolved
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            [
                "gpt-5.5",
                "gpt-5.6-sol",
                "gpt-image-2",
                "o-reasoner",
                "preview"
            ]
        );
        assert_eq!(codex_resolved["gpt-image-2"], [VerbKind::Image]);
        assert_eq!(codex_resolved["o-reasoner"], [VerbKind::Session]);
        let go_resolved = go.resolved_serves().models;
        assert_eq!(go_resolved["unpublished"], Vec::<VerbKind>::new());
        assert_eq!(
            go_resolved["kimi-k3"],
            [VerbKind::Complete, VerbKind::Session]
        );
        assert_eq!(
            serde_json::to_value(go.resolved_serves()).unwrap(),
            serde_json::json!({ "models": { "kimi-k3": ["complete", "session"], "unpublished": [] } })
        );
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

    /// A custom provider whose models each serve `speak` over `wire`, with
    /// the capabilities the facet declares for them.
    fn speaking(account: &str, wire: Wire, models: &[&str]) -> CatalogProvider {
        let mut provider = custom(account, models, vec![]);
        for model in &mut provider.models.as_mut().expect("discovered").models {
            model.serves = Some(vec![ServedVerb {
                verb: Verb::Speak(speak::capabilities(wire, &model.model).expect("speaks")),
                wire,
            }]);
        }
        provider
    }

    fn speech_catalog() -> Catalog {
        Catalog {
            providers: vec![
                provider("claude", Some(claude_models()), vec![]),
                speaking("acc_kokoro", Wire::OpenAiSpeech, &["kokoro"]),
                speaking(
                    "acc_openai",
                    Wire::OpenAiSpeech,
                    &["tts-1", "gpt-4o-mini-tts"],
                ),
                speaking(
                    "acc_eleven",
                    Wire::ElevenLabsStreamInput,
                    &["eleven_flash_v2_5"],
                ),
            ],
        }
    }

    #[test]
    fn speech_with_nothing_to_serve_it_names_the_speech_fixes() {
        let refusal = subscription_only()
            .offer_for(OfferQuery::new(VerbKind::Speak))
            .unwrap_err();
        let text = refusal.to_string();
        assert!(
            text.starts_with("No speech-capable model is configured. To fix it"),
            "{text}"
        );
        assert!(
            text.contains("ElevenLabs") && text.contains("Kokoro-FastAPI"),
            "{text}"
        );
        let refusal = subscription_only()
            .offer_for(OfferQuery {
                requested: Some("opus"),
                ..OfferQuery::new(VerbKind::Speak)
            })
            .unwrap_err();
        assert_eq!(
            refusal.detail.as_deref(),
            Some("'opus' is offered only by claude, which cannot speak")
        );
    }

    #[test]
    fn speech_that_streams_input_natively_ranks_first() {
        let offers: Vec<(String, String)> = speech_catalog()
            .offers_serving(VerbKind::Speak)
            .into_iter()
            .map(|offer| (offer.provider, offer.model))
            .collect();
        assert_eq!(
            offers,
            vec![
                ("custom:acc_eleven".into(), "eleven_flash_v2_5".into()),
                ("custom:acc_kokoro".into(), "kokoro".into()),
                ("custom:acc_openai".into(), "tts-1".into()),
                ("custom:acc_openai".into(), "gpt-4o-mini-tts".into()),
            ]
        );
    }

    #[test]
    fn speech_selection_walks_to_the_first_offer_that_admits_the_request() {
        let catalog = speech_catalog();
        let plain = speak::SpeechRequest::new("alloy");
        let (offer, format) = catalog
            .speech_offer(OfferQuery::new(VerbKind::Speak), &plain)
            .unwrap();
        assert_eq!(offer.model, "eleven_flash_v2_5");
        assert_eq!(format.to_string(), "mp3_44100_128");

        let mut steered = speak::SpeechRequest::new("alloy");
        steered.instructions = Some("wry".into());
        let (offer, _) = catalog
            .speech_offer(OfferQuery::new(VerbKind::Speak), &steered)
            .unwrap();
        assert_eq!(
            (offer.provider.as_str(), offer.model.as_str()),
            ("custom:acc_openai", "gpt-4o-mini-tts")
        );

        // A pinned model that cannot take the request names one that would.
        let refusal = catalog
            .speech_offer(
                OfferQuery {
                    requested: Some("tts-1"),
                    ..OfferQuery::new(VerbKind::Speak)
                },
                &steered,
            )
            .unwrap_err();
        let text = refusal.to_string();
        assert!(
            text.starts_with("No speech-capable model serves 'tts-1': tts-1 on custom:acc_openai: this offer takes no delivery instructions"),
            "{text}"
        );
        assert!(
            text.contains("; offers that would: custom:acc_openai/gpt-4o-mini-tts. To fix it"),
            "{text}"
        );

        // Nothing admits it: every candidate's reason, three at most.
        let mut impossible = speak::SpeechRequest::new("alloy");
        impossible.speed = Some(9.0);
        let text = catalog
            .speech_offer(OfferQuery::new(VerbKind::Speak), &impossible)
            .unwrap_err()
            .to_string();
        assert!(
            text.starts_with("No speech-capable model is available: eleven_flash_v2_5 on custom:acc_eleven: speed 9 is outside"),
            "{text}"
        );
        assert!(text.contains("; and 1 more."), "{text}");
    }
}
