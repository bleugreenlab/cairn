//! The facet's interface: what the rest of Cairn, and later the served HTTP
//! API, may ask the cairn-models facet.
//!
//! The read side is [`Models`]: which providers and models exist
//! ([`Catalog`]), which accounts serve them, how much of each
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

mod alias;
pub use alias::{AliasResolution, ALIAS_RESOLUTION_TTL_SECS};

pub mod complete;
pub mod decide;
pub mod effort;
pub mod embed;
pub mod image;
pub mod relay;
pub mod retention;
pub mod session;
pub mod speak;

use std::fmt;

use cairn_credentials::AccountInfo;
use cairn_domain::decision::MeasuredBy;
pub use cairn_models_types::benchmarks::BenchmarkPlacement;
use cairn_models_types::catalog::{
    DiscoveredModel, Modalities, ModelPricing, ProviderDescriptor, ProviderModelCatalog,
    ServedVerb, Verb, VerbKind,
};
use cairn_models_types::pool::ProviderPoolSnapshot;
use serde::Serialize;

/// Whose view of the facet a question is asked from.
///
/// `None` is the workspace, which is what Settings → Providers shows. A project
/// adds its private accounts.
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

/// Every provider this workspace can name, with its models.
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
    /// carry models.
    pub installed: bool,
    /// What discovery last found. `None` means discovery never ran; a failed
    /// discovery carries its error and last refresh time inside.
    pub models: Option<ProviderModelCatalog>,
    /// What this provider serves for any model it routes, each verb with the
    /// wire that carries it. A model whose discovery says what it serves
    /// ([`DiscoveredModel::serves`]) answers for itself; every other model,
    /// including one named on the ladder that discovery has not reported,
    /// serves this.
    pub serves: Vec<ServedVerb>,
    /// Verbs models on this provider were observed unable to serve despite
    /// what they declare (a chat server that returned no logprobs, or a model
    /// that reasons before it answers), each with why. Those offers do not
    /// serve that verb.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub unable: Vec<Unable>,
    /// Session observations of the Claude CLI's moving aliases.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub alias_resolutions: Vec<AliasResolution>,
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
            ambiguous: false,
        })
    }

    /// This provider as a [`CatalogListing`] lists it: discovery's models,
    /// narrowed to those that serve `serves` when it is set, each with what it
    /// serves resolved by [`Self::serves_for`].
    pub fn listed(&self, serves: Option<VerbKind>) -> ListedProvider {
        let catalog = self.models.as_ref().map(|found| ProviderModelCatalog {
            backend: found.backend.clone(),
            models: found
                .models
                .iter()
                .filter(|model| {
                    serves.is_none_or(|verb| !model.hidden && self.serves(&model.model, verb))
                })
                .map(|model| DiscoveredModel {
                    serves: Some(self.serves_for(&model.model)),
                    availability: serves
                        .and_then(|verb| model.availability_by_verb.get(&verb).cloned())
                        .or_else(|| model.availability.clone()),
                    ..model.clone()
                })
                .collect(),
            options: found.options.clone(),
            refreshed_at: found.refreshed_at,
            error: found.error.clone(),
        });
        ListedProvider {
            descriptor: self.descriptor.clone(),
            installed: self.installed,
            serves: self.serves.clone(),
            unable: self.unable.clone(),
            discovered: self.models.as_ref().map_or(0, |found| found.models.len()),
            catalog,
        }
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
}

/// The catalog as every host lists it. `cairn://models/catalog` renders it and
/// the app's settings pages read it through one typed call, so an agent and the
/// operator are never shown different models.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogListing {
    /// Every verb the facet serves, in [`VerbKind::ALL`] order, so a host can
    /// show a place for each without naming them itself.
    pub verbs: Vec<VerbKind>,
    /// The verb this listing is narrowed to, when it is.
    pub serves: Option<VerbKind>,
    /// Where a request for `serves` that names no model lands
    /// ([`Catalog::offer_for`] with nothing requested), when anything serves
    /// it. Read from what is already known; nothing is probed.
    pub selected: Option<ListedOffer>,
    /// Every provider, in [`Catalog::providers`] order.
    pub providers: Vec<ListedProvider>,
}

/// One provider's model, as a listing names it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListedOffer {
    pub provider: String,
    pub model: String,
}

/// One provider in a [`CatalogListing`].
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListedProvider {
    pub descriptor: ProviderDescriptor,
    pub installed: bool,
    /// What the provider serves for a model whose discovery declares nothing.
    pub serves: Vec<ServedVerb>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub unable: Vec<Unable>,
    /// What discovery last found, narrowed to the models that serve the
    /// listing's verb (never a hidden one) when the listing is narrowed. Every
    /// model's `serves` is its resolved answer ([`CatalogProvider::serves_for`]),
    /// never absent, so a host reads it instead of re-deriving the rule.
    /// `None` means discovery never ran.
    pub catalog: Option<ProviderModelCatalog>,
    /// How many models discovery found, before any narrowing.
    pub discovered: usize,
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

/// One model across every provider that offers it.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogModel {
    /// The canonical model id when known, otherwise `{provider}/{model}`.
    pub catalog_id: String,
    pub display_name: String,
    pub offers: Vec<CatalogOffer>,
    /// Provider spellings (native ids and slugs) that differ from the catalog id.
    pub aliases: Vec<String>,
}

/// One provider's offer of a model.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogOffer {
    /// Placement's live answer at the time the catalog was read.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub availability: Option<cairn_models_types::catalog::ModelAvailability>,
    #[serde(skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub availability_by_verb:
        std::collections::BTreeMap<VerbKind, cairn_models_types::catalog::ModelAvailability>,
    pub provider: String,
    /// The id the provider itself is sent.
    pub model: String,
    /// The canonical slug reported by this provider, when nonempty.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slug: Option<String>,
    pub context_window: Option<i64>,
    /// What the model takes in and gives out, when its provider said.
    pub modalities: Option<Modalities>,
    pub pricing: Option<ModelPricing>,
    /// What this offer serves, each verb with its wire. Whether a given
    /// decision was measured natively is recorded on the answer itself, since
    /// selection picks the offer at run time.
    pub serves: Vec<ServedVerb>,
    /// The accounts that serve this offer. Empty means every account of its
    /// provider.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub serving_account_ids: Vec<String>,
    /// The provider-independent model this offer is
    /// ([`cairn_models_types::identity`]), when Cairn can tell without
    /// guessing. Offers with the same identity share a catalog entry.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canonical: Option<String>,
    /// Session provenance for a moving alias, including missing or stale evidence.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identity_note: Option<String>,
}

impl CatalogOffer {
    /// The selected route for this verb, from a freshly acquired catalog.
    /// An absent value makes no readiness or latency promise.
    pub fn availability_for(
        &self,
        verb: VerbKind,
    ) -> Option<&cairn_models_types::catalog::ModelAvailability> {
        self.availability_by_verb.get(&verb)
    }

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
    /// The model an author wrote, or a selection chose. Unset, or blank,
    /// takes the best offer.
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

    /// What `provider` and `account` narrow offers to, as a refusal names it,
    /// when either is set.
    fn narrowing(&self) -> Option<String> {
        match (self.provider, self.account) {
            (Some(provider), Some(account)) => {
                Some(format!("provider {provider} and account {account}"))
            }
            (Some(provider), None) => Some(format!("provider {provider}")),
            (None, Some(account)) => Some(format!("account {account}")),
            (None, None) => None,
        }
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
            Verb::Complete | Verb::Session | Verb::Image(_) | Verb::Speak(_) | Verb::Embed(_) => {
                None
            }
        }
    }
}

/// Nothing in scope serves a verb, or nothing serves it for what was asked.
/// The text names the fixes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NoOffer {
    pub verb: VerbKind,
    /// The model the caller asked for, when it asked for one.
    pub requested: Option<String>,
    /// Why the request found nothing, when there is more to say than that
    /// nothing is configured.
    pub detail: Option<String>,
    /// The name reached offers on more than one provider, and `detail` names
    /// the qualified form of each: the fix is to name one, not to install
    /// anything.
    pub ambiguous: bool,
}

impl fmt::Display for NoOffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let (true, Some(detail)) = (self.ambiguous, &self.detail) {
            return write!(f, "{detail}.");
        }
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
        VerbKind::Embed => "embedding-capable",
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
        VerbKind::Embed => "cannot embed",
        VerbKind::Image => "cannot generate images",
        VerbKind::Speak => "cannot speak",
    }
}

/// The fixes every refusal of `verb` names.
pub fn fixes(verb: VerbKind) -> &'static str {
    match verb {
        VerbKind::Complete | VerbKind::Session => CONVERSATION_FIXES,
        VerbKind::Decide => decide::DECISION_FIXES,
        VerbKind::Embed => embed::EMBED_FIXES,
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
        Verb::Complete | Verb::Session | Verb::Image(_) | Verb::Embed(_) => 0,
    }
}

/// The tier of [`matching_offers`] a name resolved at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tier {
    /// `provider/native-id`.
    Qualified,
    /// A provider's native id or reported slug.
    Spelled,
    /// A canonical id no provider spells.
    Canonical,
}

/// Resolve the most specific spelling before applying any capability or grant
/// filters: a qualified native id, a provider's native id or slug, then a
/// canonical id. A spelling reaches only the offers spelled that way.
fn matching_offers(
    models: &[CatalogModel],
    requested: &str,
    qualifier: Option<&str>,
    native: &str,
) -> (Tier, Vec<CatalogOffer>) {
    let names_offer = |offer: &CatalogOffer| {
        qualifier.is_some_and(|key| offer.provider == key && offer.model == native)
    };
    let spells =
        |offer: &CatalogOffer| offer.model == requested || offer.slug.as_deref() == Some(requested);
    let offers = || models.iter().flat_map(|model| &model.offers);
    let tier = if offers().any(names_offer) {
        Tier::Qualified
    } else if offers().any(spells) {
        Tier::Spelled
    } else {
        Tier::Canonical
    };
    let matching = models
        .iter()
        .flat_map(|model| {
            model.offers.iter().filter(move |offer| match tier {
                Tier::Qualified => names_offer(offer),
                Tier::Spelled => spells(offer),
                Tier::Canonical => model.catalog_id == requested,
            })
        })
        .cloned()
        .collect();
    (tier, matching)
}

/// Offers named by the shared three tiers, narrowed by the query, then by
/// capability. A more general tier never substitutes for an incapable offer.
///
/// A provider's spelling names the model that provider offers. When the
/// query's provider or account excludes every offer so spelled, the name
/// still means that model, so it reaches the model's other offers the query
/// admits. A qualified name names one provider, and is never widened.
fn exact_offers(
    models: &[CatalogModel],
    query: &OfferQuery<'_>,
    requested: &str,
    qualifier: Option<&str>,
    native: &str,
) -> (Vec<CatalogOffer>, Vec<SelectedOffer>) {
    let (tier, named) = matching_offers(models, requested, qualifier, native);
    let admitted = |offers: &mut dyn Iterator<Item = &CatalogOffer>| -> Vec<CatalogOffer> {
        offers
            .filter(|offer| query.admits_offer(offer))
            .cloned()
            .collect()
    };
    let mut matching = admitted(&mut named.iter());
    if matching.is_empty() && tier == Tier::Spelled {
        matching = admitted(
            &mut models
                .iter()
                .filter(|model| model.offers.iter().any(|offer| named.contains(offer)))
                .flat_map(|model| &model.offers),
        );
    }
    let mut capable: Vec<SelectedOffer> = matching
        .iter()
        .filter_map(|offer| offer.selected(query.verb))
        .collect();
    capable.sort_by_key(|offer| preference(&offer.served));
    (matching, capable)
}

/// Why `requested` names no one offer, when the offers it reaches for a verb
/// are on more than one provider: each provider with the qualified names that
/// reach its offers alone. Only offers that serve the verb count, since one
/// that cannot is no choice.
fn ambiguity(requested: &str, capable: &[SelectedOffer]) -> Option<String> {
    let mut providers: Vec<&str> = capable
        .iter()
        .map(|offer| offer.provider.as_str())
        .collect();
    providers.sort_unstable();
    providers.dedup();
    if providers.len() < 2 {
        return None;
    }
    let named: Vec<String> = providers
        .iter()
        .map(|provider| {
            let mut names: Vec<String> = capable
                .iter()
                .filter(|offer| offer.provider == *provider)
                .map(|offer| format!("{provider}/{}", offer.model))
                .collect();
            names.dedup();
            format!("{provider} ({})", names.join(", "))
        })
        .collect();
    let (last, rest) = named.split_last()?;
    Some(format!(
        "'{requested}' is offered by {} and {last}: name one",
        rest.join(", ")
    ))
}

/// The refusal of `requested` when the offers serving `verb` it reaches,
/// `capable`, sit on more than one provider: the one text every door that
/// resolves a name refuses an ambiguous one with.
pub fn ambiguous(verb: VerbKind, requested: &str, capable: &[SelectedOffer]) -> Option<NoOffer> {
    ambiguity(requested, capable).map(|detail| NoOffer {
        verb,
        requested: Some(requested.to_string()),
        detail: Some(detail),
        ambiguous: true,
    })
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

    /// Every provider with its models, narrowed to those serving `serves` when
    /// it is set: the one listing every host reads.
    pub fn listing(&self, serves: Option<VerbKind>) -> CatalogListing {
        CatalogListing {
            verbs: VerbKind::ALL.to_vec(),
            serves,
            selected: serves
                .and_then(|verb| self.offer_for(OfferQuery::new(verb)).ok())
                .map(|offer| ListedOffer {
                    provider: offer.provider,
                    model: offer.model,
                }),
            providers: self
                .providers
                .iter()
                .map(|provider| provider.listed(serves))
                .collect(),
        }
    }

    /// The catalog-id view: every selectable model on an installed provider,
    /// grouped by catalog id.
    ///
    /// Nothing is merged by guess. Two providers' offers share an entry only
    /// when identity resolves both to one canonical id. An unidentified offer
    /// stays under its provider-qualified native id. Moving aliases group only
    /// when their provider supplies current identity evidence. Hidden models
    /// (ones the provider cannot serve) are left out.
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
            let discovered = provider
                .models
                .as_ref()
                .map(|catalog| catalog.models.as_slice())
                .unwrap_or_default();
            for model in discovered.iter().filter(|model| !model.hidden) {
                let canonical = provider.canonical(&model.model);
                let catalog_id = canonical
                    .clone()
                    .unwrap_or_else(|| format!("{key}/{}", model.model));
                let slug = model.canonical_slug.clone().filter(|slug| !slug.is_empty());
                let mut aliases = vec![model.model.clone()];
                aliases.extend(slug.iter().cloned());
                place(
                    catalog_id,
                    &model.display_name,
                    CatalogOffer {
                        availability: model.availability.clone(),
                        availability_by_verb: model.availability_by_verb.clone(),
                        provider: key.to_string(),
                        model: model.model.clone(),
                        slug,
                        context_window: model.context_window,
                        modalities: model.modalities.clone(),
                        pricing: model.pricing.clone(),
                        serves: provider.serves_declared(&model.model, model.serves.as_deref()),
                        serving_account_ids: model.serving_account_ids.clone(),
                        canonical,
                        identity_note: provider.identity_note(&model.model),
                    },
                    aliases,
                );
            }
        }
        out
    }

    /// Where licensed public benchmarks place `model` on `provider`, when its
    /// canonical id is known and the shipped fit placed it
    /// ([`cairn_models_types::benchmarks`]). An input to the ladder, never a
    /// stored level.
    pub fn benchmark(&self, provider: &str, model: &str) -> Option<BenchmarkPlacement> {
        let canonical = match self.providers.iter().find(|p| p.descriptor.key == provider) {
            Some(found) => found.canonical(model),
            None => {
                cairn_models_types::identity::for_offer(provider, model, None).map(str::to_string)
            }
        }?;
        cairn_models_types::benchmarks::shipped()
            .model(&canonical)
            .cloned()
    }

    /// Offers reached by a qualified native id, then a provider's spelling,
    /// then a canonical id, without filtering by verb. Pools are workspace
    /// selection vocabulary, never catalog names.
    pub fn offers_named(&self, name: &str) -> Vec<CatalogOffer> {
        let name = name.trim();
        if name.is_empty() {
            return Vec::new();
        }
        let (qualifier, native) = self.qualified_by(name);
        matching_offers(&self.models(), name, qualifier, native).1
    }

    /// The installed provider whose key is `key`.
    pub fn provider(&self, key: &str) -> Option<&CatalogProvider> {
        self.installed()
            .find(|provider| provider.descriptor.key.as_ref() == key)
    }

    /// Every offer serving `verb`, best first: by its wire's preference, then
    /// in the workspace's provider order.
    pub fn offers_serving(&self, verb: VerbKind) -> Vec<SelectedOffer> {
        self.candidates(&OfferQuery::new(verb))
    }

    /// The offer that serves `query.verb` for what was asked.
    ///
    /// `requested` is what an author wrote or a selection chose. Unset takes
    /// the best offer serving the verb ([`Self::offers_serving`]). Anything
    /// else is an exact model, matched by native id, catalog id, alias, or
    /// `provider/native-id`. A selection that lands only on offers that do not
    /// serve the verb is refused, never served by something that does, and an
    /// unqualified one that lands on offers serving the verb on more than one
    /// provider is refused naming each `provider/native-id`, never decided
    /// for the caller. `provider` and `account` narrow every branch; a
    /// provider's spelling they exclude reaches the same model's offers they
    /// admit instead ([`exact_offers`]).
    pub fn offer_for(&self, query: OfferQuery<'_>) -> Result<SelectedOffer, NoOffer> {
        self.ranked(&query).map(|mut offers| offers.remove(0))
    }

    /// The one offer an edit pinning `query.requested` to an agent names,
    /// spelled as its provider spells it: what a stored launch fact must be,
    /// since a launch hands it to the provider verbatim.
    ///
    /// The name resolves as [`Self::offer_for`] resolves an exact model, with
    /// the differences an edit needs. A name is required, since an edit never
    /// takes the best offer. A `provider/native-id` name selects that
    /// provider's offer even when `query.provider` names another: the
    /// qualifier is what the editor wrote, `query.provider` only the provider
    /// the edit was made under. A name that reaches no offer exactly reaches
    /// the one it matches ignoring ASCII case, so `Opus` pins Claude's `opus`;
    /// a name several offers match only by case is refused naming each. And a
    /// name nothing offers is refused naming what the provider `query` narrows
    /// to does offer for the verb.
    pub fn pinned_offer(&self, query: OfferQuery<'_>) -> Result<SelectedOffer, NoOffer> {
        let verb = query.verb;
        let Some(requested) = query
            .requested
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            return Err(NoOffer {
                verb,
                requested: None,
                detail: Some("an edit names the model it pins".to_string()),
                ambiguous: false,
            });
        };
        let models = self.models();
        let (qualifier, native) = self.qualified_by(requested);
        let exact = OfferQuery {
            requested: Some(requested),
            provider: match qualifier {
                Some(key)
                    if models
                        .iter()
                        .flat_map(|model| &model.offers)
                        .any(|offer| offer.provider == key && offer.model == native) =>
                {
                    Some(key)
                }
                _ => query.provider,
            },
            ..query
        };
        if !exact_offers(&models, &exact, requested, qualifier, native)
            .0
            .is_empty()
        {
            return self.offer_for(exact);
        }

        // Every spelling that matches ignoring case, each of which matches
        // exactly. A qualified spelling selects its own provider, so only the
        // bare ones are narrowed.
        let mut spellings: Vec<String> = Vec::new();
        {
            let mut spell = |name: &str| {
                if name.eq_ignore_ascii_case(requested)
                    && !spellings.iter().any(|seen| seen == name)
                {
                    spellings.push(name.to_string());
                }
            };
            for model in &models {
                for offer in &model.offers {
                    spell(&format!("{}/{}", offer.provider, offer.model));
                }
                let admitted: Vec<&CatalogOffer> = model
                    .offers
                    .iter()
                    .filter(|offer| query.admits_offer(offer))
                    .collect();
                if admitted.is_empty() {
                    continue;
                }
                spell(&model.catalog_id);
                for alias in &model.aliases {
                    spell(alias);
                }
                for offer in admitted {
                    spell(&offer.model);
                    if let Some(slug) = &offer.slug {
                        spell(slug);
                    }
                }
            }
        }

        let mut pinned: Vec<SelectedOffer> = Vec::new();
        let mut refused: Option<NoOffer> = None;
        for spelling in &spellings {
            match self.pinned_offer(OfferQuery {
                requested: Some(spelling),
                ..query
            }) {
                Ok(offer) => {
                    if !pinned.contains(&offer) {
                        pinned.push(offer);
                    }
                }
                // That spelling already names more than one provider's offer:
                // refused naming the qualified form of each, under the name
                // the editor wrote.
                Err(refusal) if refusal.ambiguous => {
                    let spelled = OfferQuery {
                        requested: Some(spelling),
                        ..query
                    };
                    let (qualifier, native) = self.qualified_by(spelling);
                    let (_, capable) = exact_offers(&models, &spelled, spelling, qualifier, native);
                    return Err(NoOffer {
                        requested: Some(requested.to_string()),
                        detail: ambiguity(requested, &capable).or(refusal.detail),
                        ..refusal
                    });
                }
                Err(refusal) => {
                    refused.get_or_insert(refusal);
                }
            }
        }
        match pinned.len() {
            0 => Err(refused.unwrap_or_else(|| self.unoffered(&exact, requested))),
            1 => Ok(pinned.remove(0)),
            _ => {
                let detail = ambiguity(requested, &pinned).unwrap_or_else(|| {
                    let named: Vec<String> = pinned
                        .iter()
                        .map(|offer| format!("{}/{}", offer.provider, offer.model))
                        .collect();
                    format!(
                        "'{requested}' matches {} only by case: name one",
                        named.join(" and ")
                    )
                });
                Err(NoOffer {
                    verb,
                    requested: Some(requested.to_string()),
                    detail: Some(detail),
                    ambiguous: true,
                })
            }
        }
    }

    /// The refusal of a pin nothing offers, naming what the provider `query`
    /// narrows to offers for its verb, so the editor sees what to write.
    fn unoffered(&self, query: &OfferQuery<'_>, requested: &str) -> NoOffer {
        /// Enough to recognise the model meant; a large provider's full list
        /// is the catalog's to show.
        const SHOWN: usize = 12;
        let mut detail = format!("no installed provider offers '{requested}'");
        let provider = self.qualified_by(requested).0.or(query.provider);
        if let Some(provider) = provider {
            let offers = self.candidates(&OfferQuery {
                requested: None,
                provider: Some(provider),
                ..*query
            });
            let names = self.request_names(&offers);
            if names.is_empty()
                && self.installed().any(|known| {
                    known.descriptor.key == provider
                        && known.models.as_ref().is_none_or(|catalog| {
                            catalog.models.is_empty()
                                || catalog.refreshed_at.is_none()
                                // Codex declares an image offer independently of its
                                // live conversation inventory. It cannot make an early,
                                // empty session listing count as discovered.
                                || (provider == "codex"
                                    && matches!(query.verb, VerbKind::Session | VerbKind::Complete)
                                    && !catalog.models.iter().any(|model| {
                                        best(
                                            model.serves.as_deref().unwrap_or(&known.serves),
                                            query.verb,
                                        )
                                        .is_some()
                                    }))
                        })
                })
            {
                detail.push_str(&format!(
                    "; {provider}'s catalog is not discovered yet; wait for discovery or retry in Settings → Providers"
                ));
            }
            if !names.is_empty() {
                detail.push_str(&format!(
                    "; {provider} offers {}",
                    names[..names.len().min(SHOWN)].join(", ")
                ));
                if names.len() > SHOWN {
                    detail.push_str(&format!(
                        " and {} more (cairn://models/catalog lists them)",
                        names.len() - SHOWN
                    ));
                }
            }
        }
        NoOffer {
            verb: query.verb,
            requested: Some(requested.to_string()),
            detail: Some(detail),
            ambiguous: false,
        }
    }

    /// The best offer for `query` that `admit` accepts, with what `admit`
    /// returned for it: the request-aware walk a verb with per-request
    /// capabilities (an image's mask, a voice's streaming) selects with.
    ///
    /// It walks the same ranked offers [`Self::offer_for`] would pick from. A
    /// refusal names, three at most, each offer that declined and why; when a
    /// model was asked for, it also names up to three offers of the
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
            ambiguous: false,
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
            ambiguous: false,
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

        let (qualifier, native) = self.qualified_by(requested);

        let models = self.models();
        let (matching, capable) = exact_offers(&models, &query, requested, qualifier, native);
        if matching.is_empty() {
            // Name what the name reached outside the narrowing, so the caller
            // sees which provider spells it that way.
            let mut outside: Vec<String> = matching_offers(&models, requested, qualifier, native)
                .1
                .iter()
                .map(|offer| format!("{}/{}", offer.provider, offer.model))
                .collect();
            outside.dedup();
            return Err(refuse(Some(match query.narrowing() {
                Some(narrowing) if !outside.is_empty() => format!(
                    "'{requested}' names {}, outside {narrowing}, and that model has no offer within it",
                    outside.join(" and ")
                ),
                _ => format!("no installed provider offers '{requested}'"),
            })));
        }
        if capable.is_empty() {
            // Each provider, with the spelling it uses where that is not the
            // name asked for.
            let mut providers: Vec<String> = Vec::new();
            for offer in &matching {
                let named = if offer.model == native || offer.slug.as_deref() == Some(requested) {
                    offer.provider.clone()
                } else {
                    format!("{} (as '{}')", offer.provider, offer.model)
                };
                if !providers.contains(&named) {
                    providers.push(named);
                }
            }
            return Err(refuse(Some(format!(
                "'{requested}' is offered only by {}, which {}{}",
                providers.join(", "),
                cannot(verb),
                self.observed_unable(
                    verb,
                    matching
                        .iter()
                        .map(|offer| (offer.provider.as_str(), offer.model.as_str()))
                )
            ))));
        }
        // An exact model is never chosen between providers: the same name on
        // two providers may be two servings (another quantization, another
        // pooling), whose answers and vectors differ. Ranking them is model
        // selection, which belongs to allotments.
        if let Some(refusal) = ambiguous(verb, requested, &capable) {
            return Err(refusal);
        }
        Ok(capable)
    }

    /// `requested` split at the installed provider key before its first `/`:
    /// that key and the rest, or no key and all of it.
    fn qualified_by<'r>(&self, requested: &'r str) -> (Option<&'r str>, &'r str) {
        match requested.split_once('/') {
            Some((provider, rest))
                if self
                    .installed()
                    .any(|candidate| candidate.descriptor.key.as_ref() == provider) =>
            {
                (Some(provider), rest)
            }
            _ => (None, requested),
        }
    }

    /// The name a caller writes to reach each of `offers` for the verb it
    /// serves: its native id where that reaches it and nothing else, else
    /// `provider/native-id`. Listings name offers this way, so the name a
    /// reader copies is never one the doors refuse as ambiguous.
    ///
    /// The native id is resolved as [`Self::offer_for`] resolves it, prefix
    /// split included: OpenRouter's `typesafe/jev-1.14` starts with the
    /// TypeSafe provider's key, yet names OpenRouter's offer bare unless
    /// TypeSafe itself offers a `jev-1.14`.
    pub fn request_names(&self, offers: &[SelectedOffer]) -> Vec<String> {
        let models = self.models();
        offers
            .iter()
            .map(|offer| {
                let name = offer.model.as_str();
                let (qualifier, native) = self.qualified_by(name);
                let query = OfferQuery::new(offer.served.verb.kind());
                let (_, capable) = exact_offers(&models, &query, name, qualifier, native);
                let alone = ambiguity(name, &capable).is_none()
                    && capable.first().is_some_and(|first| {
                        first.provider == offer.provider && first.model == offer.model
                    });
                if alone {
                    offer.model.clone()
                } else {
                    format!("{}/{}", offer.provider, offer.model)
                }
            })
            .collect()
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
        // Canonical groups interleave providers, so their flattened order is
        // not selection priority. Recover discovery order before wire ranking.
        let order: std::collections::HashMap<_, _> = self
            .installed()
            .enumerate()
            .flat_map(|(provider_index, provider)| {
                let key = provider.descriptor.key.as_ref();
                provider
                    .models
                    .as_ref()
                    .into_iter()
                    .flat_map(|catalog| catalog.models.iter().enumerate())
                    .map(move |(model_index, model)| {
                        ((key, model.model.as_str()), (provider_index, model_index))
                    })
            })
            .collect();
        offers.sort_by_key(|offer| {
            (
                preference(&offer.served),
                order[&(offer.provider.as_str(), offer.model.as_str())],
            )
        });
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
    /// That process's CPU priority as the OS reports it when read, so what
    /// the launch applied can be checked without `ps`.
    pub cpu_priority: Option<cairn_platform::priority::CpuPriority>,
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

/// Which per-request rows to read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestsQuery {
    /// One client, by name or id.
    pub client: Option<String>,
    /// How far back, in hours.
    pub hours: u32,
    pub limit: usize,
}

impl Default for RequestsQuery {
    fn default() -> Self {
        Self {
            client: None,
            hours: 24,
            limit: 200,
        }
    }
}

/// One request an outside client made through the served API. Never holds a
/// prompt or a reply.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestRecord {
    pub id: String,
    /// Unix milliseconds.
    pub at: i64,
    /// `chat`, `completions`, `responses` or `embeddings`.
    pub door: String,
    pub client_id: Option<String>,
    /// The client's name now, when it still has a record.
    pub client: Option<String>,
    /// The name the client sent.
    pub model_asked: Option<String>,
    /// The model it resolved to.
    pub model: Option<String>,
    pub provider: Option<String>,
    pub account_id: Option<String>,
    pub status: i64,
    pub stream: bool,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub cached_tokens: Option<i64>,
    pub cost: Option<f64>,
    pub latency_ms: i64,
    /// How long it waited for a local model to load.
    pub wait_ms: Option<i64>,
    /// `answered`, `refused`, `upstream_error` or `client_closed`.
    pub outcome: String,
    /// The standing grant that allowed a remote provider.
    pub grant_id: Option<String>,
    pub detail: Option<String>,
}

/// What the facet answers, read side.
///
/// Permissions are not facet flags: each door authorizes its callers the way
/// its existing equivalents do (a session stop carries a node stop's
/// authority), and broader rights become `cairn://grants` scopes.
pub trait Models: Send + Sync {
    /// Providers and their discovered models.
    fn catalog(&self, scope: &ModelScope) -> Result<Catalog, String>;

    /// The catalog as hosts list it, narrowed to the models serving `serves`
    /// when it is set ([`Catalog::listing`]).
    fn catalog_listing(
        &self,
        scope: &ModelScope,
        serves: Option<VerbKind>,
    ) -> Result<CatalogListing, String> {
        Ok(self.catalog(scope)?.listing(serves))
    }

    /// The accounts visible in `scope`, without any secret value.
    fn accounts(&self, scope: &ModelScope) -> Result<Vec<AccountInfo>, String>;

    /// Pooled capacity per installed provider (or the one asked for), as
    /// already measured. Never probes.
    fn usage(
        &self,
        scope: &ModelScope,
        provider: Option<&str>,
    ) -> Result<Vec<ProviderPoolSnapshot>, String>;

    /// The served API's per-request rows, newest first.
    fn requests(&self, query: &RequestsQuery) -> Result<Vec<RequestRecord>, String>;

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

    fn provider(key: &str, models: Option<Vec<DiscoveredModel>>) -> CatalogProvider {
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
            serves: Vec::new(),
            unable: Vec::new(),
            alias_resolutions: Vec::new(),
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

    fn custom(account: &str, models: &[&str]) -> CatalogProvider {
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
            serves: Vec::new(),
            unable: Vec::new(),
            alias_resolutions: Vec::new(),
        }
    }

    #[test]
    fn a_name_reaches_offers_by_catalog_id_native_id_or_qualifier() {
        let catalog = Catalog {
            providers: vec![
                provider(
                    "openrouter",
                    Some(vec![model(
                        "deepseek/deepseek-v4-flash",
                        Some("deepseek/deepseek-v4-flash-20260901"),
                    )]),
                ),
                custom("acc_hub", &["deepseek/deepseek-v4-flash"]),
            ],
        };
        let providers = |name: &str| {
            catalog
                .offers_named(name)
                .into_iter()
                .map(|offer| offer.provider)
                .collect::<Vec<_>>()
        };
        // The native id reaches both, though their catalog ids differ.
        assert_eq!(
            providers("deepseek/deepseek-v4-flash"),
            ["openrouter", "custom:acc_hub"]
        );
        assert_eq!(
            providers("deepseek/deepseek-v4-flash-20260901"),
            ["openrouter"]
        );
        assert_eq!(
            providers("openrouter/deepseek/deepseek-v4-flash"),
            ["openrouter"]
        );
        assert!(providers("big").is_empty());
        assert!(providers("openrouter/big").is_empty());
        assert!(providers(" ").is_empty());
    }

    fn subscription_only() -> Catalog {
        Catalog {
            providers: vec![provider("claude", Some(claude_models()))],
        }
    }

    #[test]
    fn offers_state_what_their_provider_serves() {
        let catalog = Catalog {
            providers: vec![
                provider("claude", Some(claude_models())),
                deciding(custom("acc_sparks", &["gemma4-26b"]), MeasuredBy::Logprobs),
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
                deciding(custom("acc_sparks", &["gemma4-26b"]), MeasuredBy::Logprobs),
                deciding(
                    custom("acc_gateway", &["typesafe-ai/jev"]),
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

    /// An OpenRouter decision model is named the way a refusal names it,
    /// `provider/native-id`, though its native id holds a slash and its
    /// catalog id is independent of that spelling; that names only this provider's
    /// offer.
    #[test]
    fn a_provider_qualified_native_id_selects_that_providers_offer() {
        let mut jev = model("typesafe/jev-1.14", Some("typesafe/jev-1.14-20261014"));
        jev.serves = Some(vec![ServedVerb {
            verb: Verb::Decide,
            wire: Wire::SystemOne,
        }]);
        let catalog = Catalog {
            providers: vec![
                deciding(
                    custom("acc_gateway", &["typesafe/jev-1.14"]),
                    MeasuredBy::Native,
                ),
                provider("openrouter", Some(vec![jev])),
            ],
        };
        let selected = decide(&catalog, Some("openrouter/typesafe/jev-1.14")).unwrap();
        assert_eq!(
            selected,
            SelectedOffer {
                provider: "openrouter".into(),
                model: "typesafe/jev-1.14".into(),
                served: ServedVerb {
                    verb: Verb::Decide,
                    wire: Wire::SystemOne,
                },
            }
        );
        assert_eq!(selected.measured_by(), Some(MeasuredBy::Native));
        // The canonical slug still selects it; the bare native id is offered
        // by both providers, so it names neither.
        assert_eq!(
            decide(&catalog, Some("typesafe/jev-1.14-20261014"))
                .unwrap()
                .provider,
            "openrouter"
        );
        let refusal = decide(&catalog, Some("typesafe/jev-1.14")).unwrap_err();
        assert_eq!(
            refusal.detail.as_deref(),
            Some("'typesafe/jev-1.14' is offered by custom:acc_gateway (custom:acc_gateway/typesafe/jev-1.14) and openrouter (openrouter/typesafe/jev-1.14): name one")
        );
        assert_eq!(
            decide(&catalog, Some("custom:acc_gateway/typesafe/jev-1.14"))
                .unwrap()
                .provider,
            "custom:acc_gateway"
        );
        assert!(decide(&catalog, Some("openrouter/typesafe/jev-9")).is_err());
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

    /// A pool name is not a model: the catalog knows only models, and a
    /// qualified name reaches only that provider's offer.
    #[test]
    fn a_pool_name_reaches_no_offer_and_a_qualified_model_only_its_provider() {
        let mut catalog = subscription_only();
        catalog.providers.push(deciding(
            custom("acc_sparks", &["qwen3-4b"]),
            MeasuredBy::Logprobs,
        ));
        assert_eq!(
            decide(&catalog, Some("sm")).unwrap_err().detail.as_deref(),
            Some("no installed provider offers 'sm'")
        );
        assert!(decide(&catalog, Some("claude/qwen3-4b")).is_err());
        assert_eq!(
            decide(&catalog, Some("custom:acc_sparks/qwen3-4b")).unwrap(),
            decision("custom:acc_sparks", "qwen3-4b", MeasuredBy::Logprobs)
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
            custom("acc_gateway", &["typesafe-ai/jev"]),
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
            custom("acc_sparks", &["qwen3", "gemma4"]),
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

        let detail = decide(&catalog, Some("qwen3")).unwrap_err().detail.unwrap();
        assert!(
            detail.ends_with("(qwen3 on custom:acc_sparks returned no logprobs)"),
            "{detail}"
        );
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
                conversing(provider("ollama", Some(vec![on_low]))),
                conversing(provider("openrouter", Some(vec![model("qwen3", None)]))),
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
        // Both offer `qwen3`, so the bare id names neither; a qualifier or the
        // provider narrowing picks one.
        assert!(complete(None, None, Some("qwen3")).unwrap_err().ambiguous);
        assert_eq!(
            complete(None, None, Some("ollama/qwen3")).unwrap().provider,
            "ollama"
        );
        assert_eq!(
            complete(Some("openrouter"), None, Some("qwen3"))
                .unwrap()
                .provider,
            "openrouter"
        );
        assert_eq!(
            complete(Some("ollama"), Some("low"), Some("qwen3"))
                .unwrap()
                .model,
            "qwen3"
        );
        assert!(complete(Some("ollama"), Some("high"), Some("qwen3")).is_err());
        // An offer that names no serving account is served by every account.
        assert_eq!(
            complete(None, Some("high"), None).unwrap().provider,
            "openrouter"
        );
    }

    const QWEN_EMBED: &str = "Qwen/Qwen3-Embedding-4B";
    const QWEN_SLUG: &str = "qwen/qwen3-embedding-4b";

    /// A model whose own declaration is that it embeds, and nothing else.
    fn embedder(id: &str, slug: Option<&str>) -> DiscoveredModel {
        let mut embedder = model(id, slug);
        embedder.serves = Some(vec![ServedVerb {
            verb: Verb::Embed(
                cairn_models_types::embedding::EmbeddingCapabilities::cairn_gateway(),
            ),
            wire: Wire::OpenAiEmbeddings,
        }]);
        embedder
    }

    /// The live workspace's two Qwen3-Embedding-4B offers: OpenRouter's, and
    /// Cairn inference's under its real name, with a shared upstream spelling.
    /// The identity table does not list this model, so the offers stay separate.
    fn two_embedders() -> Catalog {
        Catalog {
            providers: vec![
                provider(
                    "openrouter",
                    Some(vec![
                        embedder(QWEN_SLUG, Some(QWEN_SLUG)),
                        embedder(
                            "openai/text-embedding-3-small",
                            Some("openai/text-embedding-3-small"),
                        ),
                    ]),
                ),
                provider(
                    "inference",
                    Some(vec![embedder(QWEN_EMBED, Some(QWEN_SLUG))]),
                ),
            ],
        }
    }

    fn embed_offer(catalog: &Catalog, requested: &str) -> Result<SelectedOffer, NoOffer> {
        catalog.offer_for(OfferQuery {
            requested: Some(requested),
            ..OfferQuery::new(VerbKind::Embed)
        })
    }

    #[test]
    fn an_unqualified_model_two_providers_offer_is_refused_naming_each() {
        let catalog = two_embedders();
        assert_eq!(
            catalog.models().len(),
            3,
            "unidentified Qwen offers remain separate despite their shared spelling"
        );
        assert_eq!(
            embed_offer(&catalog, QWEN_EMBED).unwrap().provider,
            "inference"
        );
        {
            let requested = QWEN_SLUG;
            let refusal = embed_offer(&catalog, requested).unwrap_err();
            assert!(refusal.ambiguous, "{requested}");
            assert_eq!(
                refusal.to_string(),
                format!("'{requested}' is offered by inference (inference/Qwen/Qwen3-Embedding-4B) and openrouter (openrouter/qwen/qwen3-embedding-4b): name one.")
            );
            // The request-aware walk refuses it the same way.
            assert!(
                catalog
                    .admitting_offer(
                        OfferQuery {
                            requested: Some(requested),
                            ..OfferQuery::new(VerbKind::Embed)
                        },
                        |_| Ok::<_, &str>(())
                    )
                    .unwrap_err()
                    .ambiguous
            );
        }

        // Each qualified form selects its provider's offer.
        let local = embed_offer(&catalog, "inference/Qwen/Qwen3-Embedding-4B").unwrap();
        assert_eq!(
            (local.provider.as_str(), local.model.as_str()),
            ("inference", QWEN_EMBED)
        );
        let remote = embed_offer(&catalog, "openrouter/qwen/qwen3-embedding-4b").unwrap();
        assert_eq!(
            (remote.provider.as_str(), remote.model.as_str()),
            ("openrouter", QWEN_SLUG)
        );
        // So does narrowing to one provider.
        let narrowed = catalog
            .offer_for(OfferQuery {
                requested: Some(QWEN_EMBED),
                provider: Some("inference"),
                ..OfferQuery::new(VerbKind::Embed)
            })
            .unwrap();
        assert_eq!(narrowed.provider, "inference");

        // A model one provider offers still resolves unqualified.
        assert_eq!(
            embed_offer(&catalog, "openai/text-embedding-3-small")
                .unwrap()
                .provider,
            "openrouter"
        );
    }

    fn pin(
        catalog: &Catalog,
        requested: &str,
        provider: Option<&str>,
    ) -> Result<(String, String), NoOffer> {
        catalog
            .pinned_offer(OfferQuery {
                requested: Some(requested),
                provider,
                ..OfferQuery::new(VerbKind::Session)
            })
            .map(|offer| (offer.provider, offer.model))
    }

    /// A Claude subscription beside one chat model two providers offer, merged
    /// by canonical identity: OpenRouter's lowercase id and Cairn inference's
    /// real name.
    fn pinnable() -> Catalog {
        const QWEN_CHAT: &str = "qwen/qwen3.6-35b-a3b";
        Catalog {
            providers: vec![
                conversing(provider("claude", Some(claude_models()))),
                conversing(provider(
                    "openrouter",
                    Some(vec![model(QWEN_CHAT, Some(QWEN_CHAT))]),
                )),
                conversing(provider(
                    "inference",
                    Some(vec![model("Qwen/Qwen3.6-35B-A3B", Some(QWEN_CHAT))]),
                )),
            ],
        }
    }

    #[test]
    fn a_pin_is_stored_as_its_provider_spells_it() {
        let catalog = pinnable();
        let claude = |model: &str| Ok(("claude".to_string(), model.to_string()));
        // Display case reaches the alias the CLI is sent.
        assert_eq!(pin(&catalog, "Opus", Some("claude")), claude("opus"));
        assert_eq!(pin(&catalog, " SONNET ", None), claude("sonnet"));
        assert_eq!(pin(&catalog, "Claude/Haiku", None), claude("haiku"));
        assert_eq!(pin(&catalog, "opus", Some("claude")), claude("opus"));

        // A qualifier selects its provider's offer and stores the native id,
        // even when the edit was made under another provider.
        assert_eq!(
            pin(
                &catalog,
                "openrouter/qwen/qwen3.6-35b-a3b",
                Some("inference")
            ),
            Ok(("openrouter".into(), "qwen/qwen3.6-35b-a3b".into()))
        );
        assert_eq!(
            pin(&catalog, "inference/Qwen/Qwen3.6-35B-A3B", None),
            Ok(("inference".into(), "Qwen/Qwen3.6-35B-A3B".into()))
        );
        // A bare id is narrowed by the provider the edit was made under.
        assert_eq!(
            pin(&catalog, "Qwen/Qwen3.6-35B-A3B", Some("inference")),
            Ok(("inference".into(), "Qwen/Qwen3.6-35B-A3B".into()))
        );
    }

    #[test]
    fn a_pin_two_providers_offer_is_refused_naming_each() {
        let catalog = pinnable();
        // The exact real name is only inference's spelling; the shared
        // lowercase slug reaches both providers.
        assert_eq!(
            pin(&catalog, "Qwen/Qwen3.6-35B-A3B", None),
            Ok(("inference".into(), "Qwen/Qwen3.6-35B-A3B".into()))
        );
        let expected = "'qwen/qwen3.6-35b-a3b' is offered by inference (inference/Qwen/Qwen3.6-35B-A3B) and openrouter (openrouter/qwen/qwen3.6-35b-a3b): name one.";
        let refusal = pin(&catalog, "qwen/qwen3.6-35b-a3b", None).unwrap_err();
        assert!(refusal.ambiguous);
        assert_eq!(refusal.to_string(), expected);
        // Reached only by case, it is refused the same way, under the name
        // the editor wrote.
        let refusal = pin(&catalog, "QWEN/QWEN3.6-35B-A3B", None).unwrap_err();
        assert!(refusal.ambiguous);
        assert_eq!(
            refusal.to_string(),
            expected.replacen("qwen/qwen3.6-35b-a3b", "QWEN/QWEN3.6-35B-A3B", 1)
        );

        // Two models that differ only by case are two models.
        let cased = Catalog {
            providers: vec![
                conversing(custom("acc_a", &["Jev-1"])),
                conversing(custom("acc_b", &["jev-1"])),
            ],
        };
        let refusal = pin(&cased, "JEV-1", None).unwrap_err();
        assert!(refusal.ambiguous);
        assert_eq!(
            refusal.detail.as_deref(),
            Some("'JEV-1' is offered by custom:acc_a (custom:acc_a/Jev-1) and custom:acc_b (custom:acc_b/jev-1): name one")
        );
        assert_eq!(
            pin(&cased, "jev-1", None),
            Ok(("custom:acc_b".into(), "jev-1".into()))
        );
    }

    #[test]
    fn a_pin_waits_for_discovery_instead_of_accepting_an_unlisted_model() {
        for models in [
            None,
            Some(Vec::new()),
            Some(vec![crate::image::codex::discovered_model()]),
        ] {
            // An early listing used to be cached as a successful empty answer.
            for refreshed_at in [None, Some(1)] {
                let mut codex = conversing(provider("codex", models.clone()));
                if let Some(catalog) = codex.models.as_mut() {
                    catalog.refreshed_at = refreshed_at;
                }
                let catalog = Catalog {
                    providers: vec![codex],
                };
                for (name, provider) in [("gpt-5.5", Some("codex")), ("codex/gpt-5.5", None)] {
                    let refusal = pin(&catalog, name, provider).unwrap_err().to_string();
                    assert!(
                        refusal.contains("codex's catalog is not discovered yet"),
                        "{refusal}"
                    );
                }
            }
        }
    }

    #[test]
    fn a_pin_nothing_offers_is_refused_naming_the_providers_offers() {
        let catalog = pinnable();
        let refusal = pin(&catalog, "opus-9", Some("claude")).unwrap_err();
        assert!(!refusal.ambiguous);
        let qualified = pin(&catalog, "claude/opus-9", Some("openrouter")).unwrap_err();
        assert!(qualified
            .to_string()
            .contains("claude offers haiku, sonnet, opus, fable"));
        assert_eq!(
            refusal.to_string(),
            "No session-capable model serves 'opus-9': no installed provider offers 'opus-9'; claude offers haiku, sonnet, opus, fable. To fix it, install or configure a provider in Settings → Providers."
        );
        assert_eq!(
            pin(&catalog, "opus-9", None).unwrap_err().detail.as_deref(),
            Some("no installed provider offers 'opus-9'")
        );
        // A provider that does not offer the name refuses it, though another
        // provider does.
        assert!(pin(&catalog, "opus", Some("openrouter")).is_err());
        assert!(pin(&catalog, "  ", Some("claude")).is_err());
    }

    /// An offer that cannot serve the verb is no choice, so a name another
    /// provider offers only for some other verb is not ambiguous for this one.
    #[test]
    fn only_offers_serving_the_verb_make_a_name_ambiguous() {
        let mut catalog = two_embedders();
        let inference = &mut catalog.providers[1].models.as_mut().unwrap().models[0];
        inference
            .serves
            .as_mut()
            .unwrap()
            .extend(ServedVerb::conversation(Wire::OpenAiChatCompletions));
        let offer = catalog
            .offer_for(OfferQuery {
                requested: Some(QWEN_SLUG),
                ..OfferQuery::new(VerbKind::Complete)
            })
            .unwrap();
        assert_eq!(
            (offer.provider.as_str(), offer.model.as_str()),
            ("inference", QWEN_EMBED)
        );
        assert!(embed_offer(&catalog, QWEN_SLUG).unwrap_err().ambiguous);
    }

    /// Listings name each offer as the doors take it: bare where that reaches
    /// it alone, qualified where another provider offers the same model.
    #[test]
    fn listings_qualify_a_model_more_than_one_provider_offers() {
        let catalog = two_embedders();
        let offers = catalog.offers_serving(VerbKind::Embed);
        let names = catalog.request_names(&offers);
        assert_eq!(
            names,
            [
                "openrouter/qwen/qwen3-embedding-4b",
                "openai/text-embedding-3-small",
                "Qwen/Qwen3-Embedding-4B",
            ]
        );
        for (offer, name) in offers.iter().zip(&names) {
            assert_eq!(&embed_offer(&catalog, name).unwrap(), offer, "{name}");
        }
    }

    /// A native id that begins with another installed provider's key is still
    /// named bare when the door takes it bare: only an offer that provider
    /// actually has under the rest of the id claims the bare name.
    #[test]
    fn listings_name_bare_a_native_id_prefixed_by_another_providers_key() {
        let mut catalog = Catalog {
            providers: vec![
                deciding(
                    provider(
                        "openrouter",
                        Some(vec![model(
                            "typesafe/jev-1.14",
                            Some("typesafe/jev-1.14-20261014"),
                        )]),
                    ),
                    MeasuredBy::Native,
                ),
                deciding(
                    provider("typesafe", Some(vec![model("jev-1.14.0", None)])),
                    MeasuredBy::Native,
                ),
            ],
        };
        let offers = catalog.offers_serving(VerbKind::Decide);
        let names = catalog.request_names(&offers);
        assert_eq!(names, ["typesafe/jev-1.14", "jev-1.14.0"]);
        for (offer, name) in offers.iter().zip(&names) {
            assert_eq!(&decide(&catalog, Some(name)).unwrap(), offer, "{name}");
        }

        // Once TypeSafe offers a `jev-1.14`, the bare id is its offer, so
        // OpenRouter's must be qualified to be reached.
        catalog.providers[1].models.as_mut().unwrap().models = vec![model("jev-1.14", None)];
        let offers = catalog.offers_serving(VerbKind::Decide);
        let names = catalog.request_names(&offers);
        assert_eq!(names, ["openrouter/typesafe/jev-1.14", "jev-1.14"]);
        for (offer, name) in offers.iter().zip(&names) {
            assert_eq!(&decide(&catalog, Some(name)).unwrap(), offer, "{name}");
        }
    }

    #[test]
    fn the_request_aware_walk_moves_past_offers_that_decline_and_names_them() {
        let catalog = Catalog {
            providers: vec![conversing(provider(
                "openrouter",
                Some(vec![model("a", None), model("b", None), model("c", None)]),
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
        let go = provider("opencode-go", Some(vec![model("unpublished", None)]));
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

        let go = provider("opencode-go", Some(vec![model("listed", None)]));
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
    fn a_listing_resolves_what_every_model_serves_and_narrows_by_verb() {
        let mut hidden = model("preview", None);
        hidden.hidden = true;
        let mut codex = conversing(provider(
            "codex",
            Some(vec![
                image_only("gpt-image-2"),
                model("gpt-5.5", None),
                hidden,
            ]),
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
        );

        let catalog = Catalog {
            providers: vec![codex, go],
        };

        // Unnarrowed: every discovered model, hidden ones included, each
        // answering for itself exactly as the gate does.
        let all = catalog.listing(None);
        assert_eq!(all.verbs, VerbKind::ALL);
        assert_eq!(all.selected, None);
        for (listed, provider) in all.providers.iter().zip(&catalog.providers) {
            let models = &listed.catalog.as_ref().unwrap().models;
            assert_eq!(models.len(), listed.discovered);
            for model in models {
                assert_eq!(
                    model.serves.as_deref(),
                    Some(provider.serves_for(&model.model).as_slice()),
                    "{}",
                    model.model
                );
            }
        }
        let names = |listing: &CatalogListing, key: &str| -> Vec<String> {
            listing
                .providers
                .iter()
                .find(|provider| provider.descriptor.key == key)
                .and_then(|provider| provider.catalog.as_ref())
                .map(|catalog| catalog.models.iter().map(|m| m.model.clone()).collect())
                .unwrap_or_default()
        };
        assert_eq!(names(&all, "codex"), ["gpt-image-2", "gpt-5.5", "preview"]);
        assert_eq!(names(&all, "opencode-go"), ["unpublished", "kimi-k3"]);

        // Narrowed: exactly the models the gate admits for the verb, never a
        // hidden one, and where an unnamed request lands.
        for verb in VerbKind::ALL {
            let listing = catalog.listing(Some(verb));
            for provider in &catalog.providers {
                let expected: Vec<String> = provider
                    .models
                    .as_ref()
                    .unwrap()
                    .models
                    .iter()
                    .filter(|model| !model.hidden && provider.serves(&model.model, verb))
                    .map(|model| model.model.clone())
                    .collect();
                assert_eq!(
                    names(&listing, &provider.descriptor.key),
                    expected,
                    "{verb}"
                );
            }
        }
        let session = catalog.listing(Some(VerbKind::Session));
        assert_eq!(names(&session, "codex"), ["gpt-5.5"]);
        assert_eq!(names(&session, "opencode-go"), ["kimi-k3"]);
        assert_eq!(
            session.selected,
            Some(ListedOffer {
                provider: "codex".into(),
                model: "gpt-5.5".into()
            })
        );
        let image = catalog.listing(Some(VerbKind::Image));
        assert_eq!(names(&image, "codex"), ["gpt-image-2"]);
        assert_eq!(catalog.listing(Some(VerbKind::Speak)).selected, None);
    }

    fn find<'a>(models: &'a [CatalogModel], id: &str) -> &'a CatalogModel {
        models
            .iter()
            .find(|model| model.catalog_id == id)
            .unwrap_or_else(|| panic!("no catalog entry {id}"))
    }

    #[test]
    fn offers_resolving_to_one_canonical_id_group_under_it() {
        let catalog = glm_catalog();
        let models = catalog.models();
        assert_eq!(models.len(), 1);
        let glm = find(&models, "glm-5.2");
        let providers: Vec<&str> = glm.offers.iter().map(|o| o.provider.as_str()).collect();
        assert_eq!(providers, vec!["openrouter", "opencode-go"]);
        assert_eq!(glm.aliases, vec!["z-ai/glm-5.2", "z-ai/glm-5.2-20261001"]);
    }

    fn glm_catalog() -> Catalog {
        Catalog {
            providers: vec![
                conversing(provider(
                    "openrouter",
                    Some(vec![model("z-ai/glm-5.2", Some("z-ai/glm-5.2-20261001"))]),
                )),
                conversing(provider("opencode-go", Some(vec![model("glm-5.2", None)]))),
            ],
        }
    }

    #[test]
    fn canonical_groups_do_not_change_unset_selection_priority() {
        let mut catalog = Catalog {
            providers: vec![
                conversing(provider(
                    "openrouter",
                    Some(vec![model("gpt-5.5", None), model("gpt-5.4", None)]),
                )),
                conversing(provider("opencode-go", Some(vec![model("gpt-5.5", None)]))),
            ],
        };
        assert_eq!(catalog.models().len(), 2);
        let query = OfferQuery::new(VerbKind::Complete);
        let offers = catalog.offers_serving(VerbKind::Complete);
        assert_eq!(
            offers
                .iter()
                .map(|offer| (offer.provider.as_str(), offer.model.as_str()))
                .collect::<Vec<_>>(),
            [
                ("openrouter", "gpt-5.5"),
                ("openrouter", "gpt-5.4"),
                ("opencode-go", "gpt-5.5"),
            ]
        );
        let (offer, ()) = catalog
            .admitting_offer(query, |offer| {
                if offer.provider == "openrouter" && offer.model == "gpt-5.5" {
                    Err("request exceeds this offer")
                } else {
                    Ok(())
                }
            })
            .unwrap();
        assert_eq!(
            (offer.provider.as_str(), offer.model.as_str()),
            ("openrouter", "gpt-5.4")
        );
        catalog.providers[0].models.as_mut().unwrap().models[0].serves = Some(vec![]);
        let offer = catalog.offer_for(query).unwrap();
        assert_eq!(
            (offer.provider.as_str(), offer.model.as_str()),
            ("openrouter", "gpt-5.4")
        );
    }

    #[test]
    fn provider_spellings_win_over_the_canonical_group() {
        let catalog = glm_catalog();
        for (name, expected) in [
            ("glm-5.2", "opencode-go"),
            ("z-ai/glm-5.2", "openrouter"),
            ("z-ai/glm-5.2-20261001", "openrouter"),
            ("opencode-go/glm-5.2", "opencode-go"),
        ] {
            let offer = catalog
                .offer_for(OfferQuery {
                    requested: Some(name),
                    ..OfferQuery::new(VerbKind::Complete)
                })
                .unwrap();
            assert_eq!(offer.provider, expected, "{name}");
            assert_eq!(catalog.offers_named(name).len(), 1, "{name}");
        }
        assert_eq!(
            catalog.request_names(&catalog.offers_serving(VerbKind::Complete)),
            ["z-ai/glm-5.2", "glm-5.2"]
        );
        // Case-insensitive edits include the provider's slug as a spelling.
        assert_eq!(
            pin(&catalog, "Z-AI/GLM-5.2-20261001", None).unwrap(),
            ("openrouter".into(), "z-ai/glm-5.2".into())
        );
        let mut incapable = catalog;
        incapable.providers[1].serves.clear();
        assert!(incapable
            .offer_for(OfferQuery {
                requested: Some("glm-5.2"),
                ..OfferQuery::new(VerbKind::Complete)
            })
            .is_err());
    }

    #[test]
    fn a_spelling_its_provider_is_filtered_out_of_resolves_in_its_model() {
        let complete =
            |catalog: &Catalog, name: &str, provider: Option<&str>, account: Option<&str>| {
                catalog.offer_for(OfferQuery {
                    verb: VerbKind::Complete,
                    requested: Some(name),
                    provider,
                    account,
                })
            };
        let catalog = glm_catalog();
        // Each provider's spelling, under the other provider, reaches that
        // provider's offer of the same model.
        for (name, provider, expected) in [
            ("glm-5.2", "openrouter", "z-ai/glm-5.2"),
            ("z-ai/glm-5.2", "opencode-go", "glm-5.2"),
            ("z-ai/glm-5.2-20261001", "opencode-go", "glm-5.2"),
        ] {
            let offer = complete(&catalog, name, Some(provider), None).unwrap();
            assert_eq!(
                (offer.provider.as_str(), offer.model.as_str()),
                (provider, expected),
                "{name}"
            );
        }
        // A qualifier names one provider, and a filter excluding it is never
        // widened past it.
        assert_eq!(
            complete(&catalog, "opencode-go/glm-5.2", Some("openrouter"), None)
                .unwrap_err()
                .detail
                .as_deref(),
            Some("'opencode-go/glm-5.2' names opencode-go/glm-5.2, outside provider openrouter, and that model has no offer within it")
        );

        // Nothing of the model within the filter: the refusal names the
        // spelling's provider.
        assert_eq!(
            complete(&catalog, "glm-5.2", Some("ollama"), None)
                .unwrap_err()
                .detail
                .as_deref(),
            Some("'glm-5.2' names opencode-go/glm-5.2, outside provider ollama, and that model has no offer within it")
        );
        // An offer within it that cannot serve the verb is named with the
        // spelling its provider uses.
        let mut incapable = glm_catalog();
        incapable.providers[0].serves.clear();
        assert_eq!(
            complete(&incapable, "glm-5.2", Some("openrouter"), None)
                .unwrap_err()
                .detail
                .as_deref(),
            Some("'glm-5.2' is offered only by openrouter (as 'z-ai/glm-5.2'), which cannot serve completions")
        );

        // Several providers within the filter: refused naming each, as any
        // name reaching two providers is.
        let mut catalog = glm_catalog();
        catalog.providers[1].models.as_mut().unwrap().models[0].serving_account_ids =
            vec!["go".into()];
        catalog.providers.push(conversing(provider(
            "ollama",
            Some(vec![model("z-ai/glm-5.2", None)]),
        )));
        assert_eq!(catalog.models().len(), 1);
        assert_eq!(
            complete(&catalog, "glm-5.2", None, Some("go"))
                .unwrap()
                .provider,
            "opencode-go"
        );
        let refusal = complete(&catalog, "glm-5.2", None, Some("elsewhere")).unwrap_err();
        assert!(refusal.ambiguous);
        assert_eq!(
            refusal.to_string(),
            "'glm-5.2' is offered by ollama (ollama/z-ai/glm-5.2) and openrouter (openrouter/z-ai/glm-5.2): name one."
        );
    }

    #[test]
    fn a_canonical_name_requires_one_capable_provider() {
        let mut catalog = Catalog {
            providers: vec![conversing(provider(
                "openrouter",
                Some(vec![model("anthropic/claude-opus-4.6", None)]),
            ))],
        };
        let query = OfferQuery {
            requested: Some("claude-opus-4.6"),
            ..OfferQuery::new(VerbKind::Complete)
        };
        assert_eq!(catalog.offer_for(query).unwrap().provider, "openrouter");
        catalog.providers.push(conversing(provider(
            "opencode-go",
            Some(vec![model("anthropic/claude-opus-4.6", None)]),
        )));
        assert_eq!(catalog.models().len(), 1);
        assert_eq!(catalog.offers_named("claude-opus-4.6").len(), 2);
        let refusal = catalog.offer_for(query).unwrap_err();
        assert!(refusal.ambiguous);
        let detail = refusal.detail.unwrap();
        for provider in ["openrouter", "opencode-go"] {
            assert!(detail.contains(&format!("{provider}/anthropic/claude-opus-4.6")));
            assert_eq!(
                catalog
                    .offer_for(OfferQuery {
                        provider: Some(provider),
                        ..query
                    })
                    .unwrap()
                    .provider,
                provider
            );
        }
        assert_eq!(
            catalog.request_names(&catalog.offers_serving(VerbKind::Complete)),
            [
                "openrouter/anthropic/claude-opus-4.6",
                "opencode-go/anthropic/claude-opus-4.6"
            ]
        );
        catalog.providers[1].serves.clear();
        assert_eq!(catalog.offer_for(query).unwrap().provider, "openrouter");
    }

    #[test]
    fn a_model_without_identity_is_qualified_by_its_provider_and_never_merged() {
        let catalog = Catalog {
            providers: vec![
                conversing(provider("claude", Some(claude_models()))),
                conversing(provider("ollama", Some(vec![model("opus", None)]))),
            ],
        };
        let models = catalog.models();
        let opus = find(&models, "claude/opus");
        assert_eq!(opus.offers.len(), 1);
        assert_eq!(opus.offers[0].context_window, Some(1_000_000));
        assert_eq!(opus.aliases, vec!["opus"]);
        // Same native id on another provider, no shared slug: its own entry.
        let local = find(&models, "ollama/opus");
        assert_eq!(local.offers[0].provider, "ollama");
        assert_eq!(local.aliases, vec!["opus"]);
        assert!(pin(&catalog, "opus", None).unwrap_err().ambiguous);
    }

    #[test]
    fn an_unknown_shared_slug_does_not_merge_offers() {
        let catalog = Catalog {
            providers: vec![
                provider(
                    "openrouter",
                    Some(vec![model("unknown-a", Some("unknown/model"))]),
                ),
                provider(
                    "ollama",
                    Some(vec![model("unknown-b", Some("unknown/model"))]),
                ),
            ],
        };
        let models = catalog.models();
        assert_eq!(models.len(), 2);
        assert_eq!(
            find(&models, "openrouter/unknown-a").offers[0].canonical,
            None
        );
        assert_eq!(find(&models, "ollama/unknown-b").offers[0].canonical, None);
        // A shared spelling still reaches both, independently of grouping.
        assert_eq!(catalog.offers_named("unknown/model").len(), 2);
    }

    #[test]
    fn offers_group_under_their_canonical_id() {
        let catalog = Catalog {
            providers: vec![
                provider("claude", Some(claude_models())),
                provider(
                    "openrouter",
                    Some(vec![model(
                        "anthropic/claude-opus-4.6",
                        Some("anthropic/claude-4.6-opus-20260205"),
                    )]),
                ),
                provider("opencode-go", Some(vec![model("kimi-k2.5", None)])),
            ],
        };
        let models = catalog.models();
        let opus = find(&models, "claude-opus-4.6");
        assert_eq!(opus.offers[0].canonical.as_deref(), Some("claude-opus-4.6"));
        let kimi = find(&models, "kimi-k2.5");
        assert_eq!(kimi.offers[0].canonical.as_deref(), Some("kimi-k2.5"));
        // The CLI's alias names no version, so it has no canonical id.
        assert_eq!(find(&models, "claude/opus").offers[0].canonical, None);
        assert_eq!(
            catalog.providers[1]
                .canonical("anthropic/claude-opus-4.6")
                .as_deref(),
            Some("claude-opus-4.6")
        );
        assert_eq!(catalog.providers[0].canonical("opus"), None);
    }

    #[test]
    fn the_benchmarks_place_a_model_through_its_canonical_id() {
        let catalog = Catalog {
            providers: vec![
                provider("claude", Some(claude_models())),
                provider(
                    "openrouter",
                    Some(vec![model("anthropic/claude-opus-4.6", None)]),
                ),
            ],
        };
        let placed = catalog
            .benchmark("openrouter", "anthropic/claude-opus-4.6")
            .expect("placed");
        assert_eq!(placed.canonical, "claude-opus-4.6");
        assert!(placed.summary().starts_with("benchmarks: "));
        // A provider the catalog does not list still resolves by the rule.
        assert_eq!(
            catalog.benchmark("codex", "gpt-5.5").map(|p| p.canonical),
            Some("gpt-5.5".to_string())
        );
        // The CLI's alias names no version, so the benchmarks cannot place it.
        assert_eq!(catalog.benchmark("claude", "opus"), None);
    }

    #[test]
    fn uninstalled_providers_and_hidden_models_offer_nothing() {
        let mut hidden = model("preview", None);
        hidden.hidden = true;
        let mut absent = provider("openrouter", Some(vec![model("x/y", None)]));
        absent.installed = false;
        let catalog = Catalog {
            providers: vec![provider("ollama", Some(vec![hidden])), absent],
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
            cpu_priority: Some(cairn_platform::priority::CpuPriority::Nice(5)),
            started_at: None,
        };
        assert_eq!(view.key(), "s1");
        assert_eq!(
            serde_json::to_value(&view).unwrap()["cpuPriority"],
            serde_json::json!({"kind": "nice", "value": 5})
        );
        let json = serde_json::to_value(&view).unwrap();
        assert_eq!(json["host"], "local");
        assert!(json.get("pid").is_some());
        assert_eq!(json["backendSessionId"], "thread-1");
        assert!(view.is_addressed_by("s1") && view.is_addressed_by("r1"));
        assert!(!view.is_addressed_by("r2"));
    }

    /// A custom provider whose models each serve `speak` over `wire`, with
    /// the capabilities the facet declares for them.
    fn speaking(account: &str, wire: Wire, models: &[&str]) -> CatalogProvider {
        let mut provider = custom(account, models);
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
                provider("claude", Some(claude_models())),
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
