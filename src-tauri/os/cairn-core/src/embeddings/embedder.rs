//! The one door cairn-core embeds through, and the health gate in front of it.
//!
//! Every consumer (the turn sweep, search's query, the embed worker's events,
//! resources and owner centroids, tenet evidence) embeds through [`Embed`].
//! One selected offer answers for all of them, every text is held to one
//! budget ([`super::EMBED_TEXT_BUDGET`]), and one gate decides whether a
//! request is sent at all.
//!
//! ## The gate
//!
//! A provider that fails is left alone: the wait starts at [`FIRST_PAUSE`],
//! doubles with each failed probe up to [`MAX_PAUSE`], and is never shorter
//! than the provider's own `Retry-After`. While it waits, every consumer is
//! told [`EmbedFailure::Paused`] and nothing is sent. Consumers failing
//! together count as one failure, because a failure inside an open wait does
//! not double it again. The first failure of an outage logs one warning on the
//! [`HEALTH_TARGET`] target, and the first success after it logs one line, so
//! an outage reads as two log lines rather than one per attempt. Retrying a
//! transient failure inside one call is the facet's job (`cairn_models::embed`);
//! the gate only paces calls.
//!
//! Two outcomes say nothing about the provider and leave the gate alone. A
//! request refused as written ([`EmbedFailure::Rejected`]) is about its input.
//! An offer this device holds no credential for ([`EmbedFailure::Unconfigured`])
//! is an install that never signed in, which is not an outage: it is silent.
//!
//! The gate never falls back to another offer. Vectors from two models are not
//! comparable, so a second model would be a second corpus.
//!
//! The gate's state lives in memory, in one [`EmbedGate`] the orchestrator
//! holds, and [`Embed::health`] is the only way to read it.

use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use cairn_models::interface::complete::CompletionError;
use cairn_models::interface::embed::{EmbedError, EmbedRequest, Embeddings, Width};
use cairn_models::types::embedding::{EmbeddingCapabilities, PurposeHandling};

pub use cairn_models::interface::embed::Purpose;

use super::{truncate_chars, EMBED_TEXT_BUDGET};

/// The log target the gate's outage and recovery lines are written to.
pub const HEALTH_TARGET: &str = "embeddings::health";
/// The wait after an outage's first failure.
const FIRST_PAUSE: Duration = Duration::from_secs(30);
/// The longest wait between probes of a provider that keeps failing.
const MAX_PAUSE: Duration = Duration::from_secs(30 * 60);

/// Which stored corpus a request's vectors join. Each asks for its own width.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Corpus {
    /// Per-turn vectors behind search's semantic lane.
    Turns,
    /// Recall vectors (issues, skills, memories, artifacts, tenets, owner
    /// centroids) and the event vectors folded into them.
    Resources,
}

impl Corpus {
    /// The width this corpus asks for: a preference, so an offer that cannot
    /// return it answers at its native width and that is what is stored.
    pub fn width(self) -> Width {
        Width::Preferred(match self {
            Self::Turns => 256,
            Self::Resources => 1536,
        })
    }
}

/// What a corpus's vectors are stored as: the model, and the width when it
/// is known before asking (`None` when the offer does not state its native
/// width).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorKey {
    pub model: String,
    pub dims: Option<u32>,
}

/// The vector space every stored vector shares: the offer that makes them and
/// the width each corpus is stored at (`None` when the offer does not state
/// it before asking). Recorded in `embedding_state`; see [`super::space`].
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EmbedSpace {
    pub provider: String,
    pub model: String,
    pub turn_dims: Option<u32>,
    pub resource_dims: Option<u32>,
}

impl EmbedSpace {
    /// The key `corpus`'s vectors are stored under in this space.
    pub fn key(&self, corpus: Corpus) -> VectorKey {
        VectorKey {
            model: self.model.clone(),
            dims: match corpus {
                Corpus::Turns => self.turn_dims,
                Corpus::Resources => self.resource_dims,
            },
        }
    }
}

/// One vector per text, in input order, and what they are stored under.
#[derive(Debug, Clone, PartialEq)]
pub struct Embedded {
    pub vectors: Vec<Vec<f32>>,
    pub model: String,
    /// The width measured on the reply, equal for every vector.
    pub dims: u32,
}

/// Why no vectors came back.
#[derive(Debug, Clone, PartialEq)]
pub enum EmbedFailure {
    /// The gate is closed after a failure; nothing was sent.
    Paused { retry_at: SystemTime },
    /// The provider or the offer failed. The gate is now closed, and has
    /// logged the outage once.
    Failed(String),
    /// The request was refused as written (no texts, an empty text). It says
    /// nothing about the provider, so nothing is paused.
    Rejected(String),
    /// The selected offer has no credential on this device. Nothing was sent
    /// and nothing is logged.
    Unconfigured,
}

impl EmbedFailure {
    /// Whether this is about the provider rather than the request: a caller
    /// that counts attempts against its input must not count these.
    pub fn is_provider_outcome(&self) -> bool {
        !matches!(self, Self::Rejected(_))
    }
}

impl fmt::Display for EmbedFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Paused { retry_at } => write!(
                f,
                "embedding is paused after a provider failure until {}",
                rfc3339(*retry_at)
            ),
            Self::Failed(error) => f.write_str(error),
            Self::Rejected(error) => write!(f, "the embedding request was refused: {error}"),
            Self::Unconfigured => f.write_str("no embedding provider is configured on this device"),
        }
    }
}

/// The gate's state, as a status read shows it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EmbedHealth {
    /// Whether the selected offer has what it needs to be called. False is
    /// "not configured", never "unavailable".
    pub configured: bool,
    /// When the current outage began.
    pub unavailable_since: Option<SystemTime>,
    pub last_error: Option<String>,
    /// When the next request may be sent.
    pub retry_at: Option<SystemTime>,
}

/// The door every embedding consumer uses.
#[async_trait]
pub trait Embed: Send + Sync {
    /// Embed `texts` into `corpus`, each capped at the shared budget.
    async fn embed(
        &self,
        corpus: Corpus,
        purpose: Purpose,
        texts: Vec<String>,
    ) -> Result<Embedded, EmbedFailure>;
    /// What `corpus` is stored as now; `None` when no offer is selected.
    fn active(&self, corpus: Corpus) -> Option<VectorKey>;
    /// The space the selected offer embeds into; `None` when the selection
    /// does not resolve to an offer (provider absent, no credential).
    fn space(&self) -> Option<EmbedSpace>;
    /// Whether a request would be sent now: configured, and not paused.
    fn ready(&self) -> bool;
    fn health(&self) -> EmbedHealth;
    /// Forget any outage: the selection changed, so the old provider's
    /// failures say nothing about the new one.
    fn reset_health(&self);
}

/// The selected offer, as the gate needs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectedEmbedOffer {
    /// The catalog provider serving it (`cairn`, `ollama`, `custom-…`).
    pub provider: String,
    /// The id stored vectors are labelled with.
    pub model: String,
    pub capabilities: EmbeddingCapabilities,
}

/// Where the gate's requests go: the one selected embedding offer.
pub trait EmbedSource: Send + Sync {
    /// The selection as an operator would name it (`cairn/cohere-embed-v4`).
    fn selection(&self) -> String;
    /// Whether this device holds what the selected offer needs to be called.
    fn configured(&self) -> bool;
    /// The selected offer, when the selection resolves to one.
    fn offer(&self) -> Option<SelectedEmbedOffer>;
    /// Embed `request` on the selected offer. Blocking: it posts.
    fn embed(&self, request: &EmbedRequest) -> Result<Embeddings, EmbedError>;
}

#[derive(Debug, Default)]
struct HealthState {
    /// Failed probes in this outage; the wait doubles with each.
    failures: u32,
    unavailable_since: Option<SystemTime>,
    last_error: Option<String>,
    retry_at: Option<SystemTime>,
}

/// A change worth one log line.
#[derive(Debug, Clone, PartialEq)]
enum Transition {
    Unavailable {
        since: SystemTime,
        error: String,
        retry_at: SystemTime,
    },
    Recovered {
        since: SystemTime,
        after: Duration,
    },
}

/// The pause/backoff state every consumer shares. One per orchestrator.
#[derive(Debug, Default)]
pub struct EmbedGate {
    state: Mutex<HealthState>,
}

impl EmbedGate {
    fn state(&self) -> std::sync::MutexGuard<'_, HealthState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// When the gate is closed at `now`, the instant it opens.
    fn closed_until(&self, now: SystemTime) -> Option<SystemTime> {
        self.state().retry_at.filter(|at| *at > now)
    }

    /// Record a failure at `now`. A failure inside an open wait (another
    /// consumer's request, sent before the first failure closed the gate)
    /// does not double it again.
    fn fail(
        &self,
        now: SystemTime,
        error: &str,
        retry_after: Option<Duration>,
    ) -> (SystemTime, Option<Transition>) {
        let mut state = self.state();
        let waiting = state.retry_at.is_some_and(|at| at > now);
        if !waiting {
            state.failures = state.failures.saturating_add(1);
        }
        let doublings = state.failures.saturating_sub(1).min(16);
        let backoff = FIRST_PAUSE.saturating_mul(1 << doublings).min(MAX_PAUSE);
        let next = now + backoff.max(retry_after.unwrap_or_default());
        let retry_at = state.retry_at.filter(|at| *at > next).unwrap_or(next);
        state.retry_at = Some(retry_at);
        state.last_error = Some(error.to_string());
        let transition = state.unavailable_since.is_none().then(|| {
            state.unavailable_since = Some(now);
            Transition::Unavailable {
                since: now,
                error: error.to_string(),
                retry_at,
            }
        });
        (retry_at, transition)
    }

    /// Record a success at `now`, clearing any outage.
    fn succeed(&self, now: SystemTime) -> Option<Transition> {
        let mut state = self.state();
        let since = state.unavailable_since;
        *state = HealthState::default();
        since.map(|since| Transition::Recovered {
            since,
            after: now.duration_since(since).unwrap_or_default(),
        })
    }

    fn health(&self, configured: bool) -> EmbedHealth {
        let state = self.state();
        EmbedHealth {
            configured,
            unavailable_since: state.unavailable_since,
            last_error: state.last_error.clone(),
            retry_at: state.retry_at,
        }
    }

    /// Clear the gate in place, open and with no outage on record. Silent:
    /// the outage belonged to a selection that is no longer in use.
    pub fn reset(&self) {
        *self.state() = HealthState::default();
    }
}

/// [`Embed`] over the facet's `embed` verb, through one [`EmbedSource`],
/// behind one [`EmbedGate`].
pub struct FacetEmbedder {
    source: Arc<dyn EmbedSource>,
    gate: Arc<EmbedGate>,
}

impl FacetEmbedder {
    pub fn new(source: Arc<dyn EmbedSource>, gate: Arc<EmbedGate>) -> Self {
        Self { source, gate }
    }
}

/// Record how a request went on the gate, log any change of state, and turn
/// the facet's answer into the door's. Runs on the request's own blocking
/// task, so a caller that stops waiting (search's deadline) still leaves the
/// gate knowing.
fn settle(
    source: &dyn EmbedSource,
    gate: &EmbedGate,
    result: Result<Embeddings, EmbedError>,
) -> Result<Embedded, EmbedFailure> {
    let now = SystemTime::now();
    match result {
        Ok(embeddings) => {
            if let Some(transition) = gate.succeed(now) {
                report(&source.selection(), transition);
            }
            Ok(Embedded {
                vectors: embeddings.vectors,
                model: embeddings.model,
                dims: embeddings.dims,
            })
        }
        Err(EmbedError::InvalidRequest(error)) => Err(EmbedFailure::Rejected(error)),
        Err(error) => {
            let text = error.to_string();
            let (retry_at, transition) = gate.fail(now, &text, error.retry_after());
            match transition {
                Some(transition) => report(&source.selection(), transition),
                None => log::debug!(
                    target: HEALTH_TARGET,
                    "embeddings: {} still unavailable: {text}; next attempt {}",
                    source.selection(),
                    rfc3339(retry_at)
                ),
            }
            Err(EmbedFailure::Failed(text))
        }
    }
}

/// The one log line a change of state is worth.
fn report(selection: &str, transition: Transition) {
    {
        match transition {
            Transition::Unavailable {
                since,
                error,
                retry_at,
            } => log::warn!(
                target: HEALTH_TARGET,
                "embeddings: {selection} unavailable since {}: {error}; next attempt {}",
                rfc3339(since),
                rfc3339(retry_at)
            ),
            Transition::Recovered { since, after } => log::info!(
                target: HEALTH_TARGET,
                "embeddings: {selection} available again after {} (unavailable since {})",
                span(after),
                rfc3339(since)
            ),
        }
    }
}

/// The most one text may carry, in bytes, on `capabilities` for `purpose`:
/// the shared budget, or the offer's own per-request character bound when it
/// is smaller, less the instruction prefix the facet puts in front of it.
///
/// A text over the offer's bound is refused by the provider every time it is
/// sent, which would wedge a newest-first sweep on it; cutting it here is
/// what keeps a smaller offer usable. Bytes bound characters from above, so a
/// byte budget under a character bound always fits.
fn text_budget(capabilities: Option<&EmbeddingCapabilities>, purpose: Purpose) -> usize {
    let Some(capabilities) = capabilities else {
        return EMBED_TEXT_BUDGET;
    };
    let bound = capabilities
        .limits
        .max_chars
        .map_or(EMBED_TEXT_BUDGET, |chars| {
            EMBED_TEXT_BUDGET.min(chars as usize)
        });
    let prefix = match &capabilities.purpose {
        PurposeHandling::Prefix { document, query } => match purpose {
            Purpose::Document => document.len(),
            Purpose::Query => query.len(),
        },
        PurposeHandling::Native | PurposeHandling::Ignored => 0,
    };
    bound.saturating_sub(prefix).max(1)
}

/// `texts` as they are sent: each cut to `budget` bytes on a character
/// boundary. No texts, or a text with nothing but whitespace, is refused here.
fn capped(texts: Vec<String>, budget: usize) -> Result<Vec<String>, EmbedFailure> {
    if texts.is_empty() {
        return Err(EmbedFailure::Rejected("there are no texts to embed".into()));
    }
    texts
        .into_iter()
        .enumerate()
        .map(|(index, text)| {
            if text.trim().is_empty() {
                return Err(EmbedFailure::Rejected(format!("text {index} is empty")));
            }
            Ok(match truncate_chars(&text, budget) {
                cut if cut.len() == text.len() => text,
                cut => cut.to_string(),
            })
        })
        .collect()
}

#[async_trait]
impl Embed for FacetEmbedder {
    async fn embed(
        &self,
        corpus: Corpus,
        purpose: Purpose,
        texts: Vec<String>,
    ) -> Result<Embedded, EmbedFailure> {
        let offer = self.source.offer();
        let texts = capped(
            texts,
            text_budget(offer.as_ref().map(|offer| &offer.capabilities), purpose),
        )?;
        if !self.source.configured() {
            return Err(EmbedFailure::Unconfigured);
        }
        if let Some(retry_at) = self.gate.closed_until(SystemTime::now()) {
            return Err(EmbedFailure::Paused { retry_at });
        }
        let request = EmbedRequest {
            texts,
            purpose,
            width: corpus.width(),
        };
        let (source, gate) = (self.source.clone(), self.gate.clone());
        match tokio::task::spawn_blocking(move || {
            let result = source.embed(&request);
            settle(source.as_ref(), &gate, result)
        })
        .await
        {
            Ok(settled) => settled,
            // The request panicked before it could settle; record it here.
            Err(error) => settle(
                self.source.as_ref(),
                &self.gate,
                Err(EmbedError::Completion(CompletionError::Upstream(format!(
                    "the embedding task failed: {error}"
                )))),
            ),
        }
    }

    fn active(&self, corpus: Corpus) -> Option<VectorKey> {
        self.source.offer().map(|offer| VectorKey {
            dims: corpus.width().on(&offer.capabilities),
            model: offer.model,
        })
    }

    fn space(&self) -> Option<EmbedSpace> {
        self.source.offer().map(|offer| EmbedSpace {
            turn_dims: Corpus::Turns.width().on(&offer.capabilities),
            resource_dims: Corpus::Resources.width().on(&offer.capabilities),
            provider: offer.provider,
            model: offer.model,
        })
    }

    fn ready(&self) -> bool {
        self.source.configured() && self.gate.closed_until(SystemTime::now()).is_none()
    }

    fn health(&self) -> EmbedHealth {
        self.gate.health(self.source.configured())
    }

    fn reset_health(&self) {
        self.gate.reset();
    }
}

/// The Cairn gateway reached with a device token held by the caller, outside
/// any workspace: the door offline tools working on a copy of a database
/// embed through. Its gate is its own.
#[cfg(feature = "internal-api")]
pub fn gateway_embedder(device_token: String) -> Arc<dyn Embed> {
    let api = crate::api::ApiConfig::default();
    Arc::new(FacetEmbedder::new(
        Arc::new(DeviceTokenGateway {
            route: cairn_models::embed::gateway::route_for(&api.base_url, &device_token),
        }),
        Arc::new(EmbedGate::default()),
    ))
}

#[cfg(feature = "internal-api")]
struct DeviceTokenGateway {
    route: cairn_models::interface::embed::EmbedRoute,
}

#[cfg(feature = "internal-api")]
impl EmbedSource for DeviceTokenGateway {
    fn selection(&self) -> String {
        use cairn_models::types::embedding::{CAIRN_GATEWAY_EMBED_MODEL, CAIRN_GATEWAY_KEY};
        format!("{CAIRN_GATEWAY_KEY}/{CAIRN_GATEWAY_EMBED_MODEL}")
    }

    fn configured(&self) -> bool {
        true
    }

    fn offer(&self) -> Option<SelectedEmbedOffer> {
        Some(SelectedEmbedOffer {
            provider: cairn_models::types::embedding::CAIRN_GATEWAY_KEY.to_string(),
            model: cairn_models::types::embedding::CAIRN_GATEWAY_EMBED_MODEL.to_string(),
            capabilities: EmbeddingCapabilities::cairn_gateway(),
        })
    }

    fn embed(&self, request: &EmbedRequest) -> Result<Embeddings, EmbedError> {
        let capabilities = EmbeddingCapabilities::cairn_gateway();
        cairn_models::embed::embed(
            &cairn_models::complete::HttpComplete,
            cairn_models::interface::embed::EmbedCall {
                route: &self.route,
                model: cairn_models::types::embedding::CAIRN_GATEWAY_EMBED_MODEL,
                capabilities: &capabilities,
                request,
                timeout: cairn_models::interface::embed::EMBED_TIMEOUT,
            },
        )
    }
}

fn rfc3339(at: SystemTime) -> String {
    chrono::DateTime::<chrono::Utc>::from(at).to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

/// A duration as an operator reads it: `11h 2m`, `4m 10s`, `30s`.
fn span(duration: Duration) -> String {
    let seconds = duration.as_secs();
    let (hours, minutes, seconds) = (seconds / 3600, seconds / 60 % 60, seconds % 60);
    match (hours, minutes) {
        (0, 0) => format!("{seconds}s"),
        (0, _) => format!("{minutes}m {seconds}s"),
        _ => format!("{hours}h {minutes}m"),
    }
}

/// An [`Embed`] for tests: deterministic vectors for one `(model, dims)`, a
/// scriptable failure per call, and a record of every request.
#[cfg(test)]
pub(crate) mod stub {
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;

    use cairn_models::types::usage::MeteredUsage;

    use super::*;

    /// An [`EmbedSource`] answering from a script and recording every request
    /// it was sent: drives a real [`FacetEmbedder`], gate and text budget
    /// included. Its vectors are the gateway's model, three wide.
    pub(crate) struct ScriptedSource {
        selection: String,
        pub configured: AtomicBool,
        /// How long each request takes before it answers.
        pub delay: Mutex<Duration>,
        /// What the offer declares; the gateway's unless a test says otherwise.
        pub capabilities: Mutex<EmbeddingCapabilities>,
        replies: Mutex<VecDeque<Result<(), EmbedError>>>,
        sent: Mutex<Vec<EmbedRequest>>,
    }

    impl ScriptedSource {
        /// `selection` names the test, so its log lines are its own.
        pub(crate) fn new(selection: &str) -> Arc<Self> {
            Arc::new(Self {
                selection: selection.to_string(),
                configured: AtomicBool::new(true),
                delay: Mutex::new(Duration::ZERO),
                capabilities: Mutex::new(EmbeddingCapabilities::cairn_gateway()),
                replies: Mutex::new(VecDeque::new()),
                sent: Mutex::new(Vec::new()),
            })
        }

        pub(crate) fn answer(&self, replies: Vec<Result<(), EmbedError>>) {
            self.replies.lock().unwrap().extend(replies);
        }

        pub(crate) fn sent(&self) -> Vec<EmbedRequest> {
            self.sent.lock().unwrap().clone()
        }

        /// A real embedder over this source, behind a gate of its own.
        pub(crate) fn embedder(self: &Arc<Self>) -> FacetEmbedder {
            FacetEmbedder::new(self.clone(), Arc::new(EmbedGate::default()))
        }
    }

    impl EmbedSource for ScriptedSource {
        fn selection(&self) -> String {
            self.selection.clone()
        }
        fn configured(&self) -> bool {
            self.configured.load(Ordering::SeqCst)
        }
        fn offer(&self) -> Option<SelectedEmbedOffer> {
            Some(SelectedEmbedOffer {
                provider: "cairn".into(),
                model: "cohere-embed-v4".into(),
                capabilities: self.capabilities.lock().unwrap().clone(),
            })
        }
        fn embed(&self, request: &EmbedRequest) -> Result<Embeddings, EmbedError> {
            self.sent.lock().unwrap().push(request.clone());
            std::thread::sleep(*self.delay.lock().unwrap());
            self.replies
                .lock()
                .unwrap()
                .pop_front()
                .expect("a request the script did not expect")?;
            Ok(Embeddings {
                vectors: request.texts.iter().map(|_| vec![0.5; 3]).collect(),
                provider: "cairn".into(),
                model: "cohere-embed-v4".into(),
                dims: 3,
                usage: MeteredUsage::default(),
            })
        }
    }

    /// One request the stub received.
    #[derive(Debug, Clone, PartialEq)]
    pub(crate) struct Call {
        pub corpus: Corpus,
        pub purpose: Purpose,
        pub texts: Vec<String>,
    }

    /// The offer a [`StubEmbed`] answers as.
    #[derive(Debug, Clone)]
    struct StubOffer {
        provider: String,
        model: String,
        dims: u32,
    }

    pub(crate) struct StubEmbed {
        /// The selected offer; `None` is a selection that does not resolve.
        offer: Mutex<Option<StubOffer>>,
        /// Answers for the next calls, in order; `None` succeeds.
        script: Mutex<VecDeque<Option<EmbedFailure>>>,
        calls: Mutex<Vec<Call>>,
        paused: Mutex<bool>,
        configured: AtomicBool,
    }

    impl StubEmbed {
        pub(crate) fn new(model: &str, dims: u32) -> Self {
            Self {
                offer: Mutex::new(Some(StubOffer {
                    provider: "cairn".to_string(),
                    model: model.to_string(),
                    dims,
                })),
                script: Mutex::new(VecDeque::new()),
                calls: Mutex::new(Vec::new()),
                paused: Mutex::new(false),
                configured: AtomicBool::new(true),
            }
        }

        /// Switch the selected offer, as a settings change would.
        pub(crate) fn set_space(&self, provider: &str, model: &str, dims: u32) {
            *self.offer.lock().unwrap() = Some(StubOffer {
                provider: provider.to_string(),
                model: model.to_string(),
                dims,
            });
        }

        /// Make the selection unresolvable: a provider gone, a token missing.
        pub(crate) fn set_unresolved(&self) {
            *self.offer.lock().unwrap() = None;
        }

        pub(crate) fn dims(&self) -> u32 {
            self.selected().map_or(0, |offer| offer.dims)
        }

        fn selected(&self) -> Option<StubOffer> {
            self.offer.lock().unwrap().clone()
        }

        /// A stub on an install with no embedding credential.
        pub(crate) fn unconfigured() -> Self {
            let stub = Self::cohere();
            stub.configured.store(false, Ordering::SeqCst);
            stub
        }

        /// A stub labelled as the Cairn gateway's vectors are.
        pub(crate) fn cohere() -> Self {
            Self::new("cohere-embed-v4", 1536)
        }

        /// Answer the next calls with these outcomes, in order.
        pub(crate) fn script(&self, outcomes: Vec<Option<EmbedFailure>>) {
            self.script.lock().unwrap().extend(outcomes);
        }

        /// Close or open the gate: while paused, `ready` is false and every
        /// call answers `Paused` without being recorded.
        pub(crate) fn set_paused(&self, paused: bool) {
            *self.paused.lock().unwrap() = paused;
        }

        pub(crate) fn calls(&self) -> Vec<Call> {
            self.calls.lock().unwrap().clone()
        }

        /// The vector a text embeds to: its byte length, then ones.
        pub(crate) fn vector(&self, text: &str) -> Vec<f32> {
            let mut vector = vec![1.0; self.dims() as usize];
            if let Some(first) = vector.first_mut() {
                *first = text.len() as f32;
            }
            vector
        }
    }

    #[async_trait]
    impl Embed for StubEmbed {
        async fn embed(
            &self,
            corpus: Corpus,
            purpose: Purpose,
            texts: Vec<String>,
        ) -> Result<Embedded, EmbedFailure> {
            if !self.configured.load(Ordering::SeqCst) {
                return Err(EmbedFailure::Unconfigured);
            }
            if *self.paused.lock().unwrap() {
                return Err(EmbedFailure::Paused {
                    retry_at: SystemTime::now() + FIRST_PAUSE,
                });
            }
            let texts = capped(texts, EMBED_TEXT_BUDGET)?;
            let Some(offer) = self.selected() else {
                return Err(EmbedFailure::Failed("no embedding offer resolves".into()));
            };
            self.calls.lock().unwrap().push(Call {
                corpus,
                purpose,
                texts: texts.clone(),
            });
            if let Some(Some(failure)) = self.script.lock().unwrap().pop_front() {
                return Err(failure);
            }
            Ok(Embedded {
                vectors: texts.iter().map(|text| self.vector(text)).collect(),
                model: offer.model,
                dims: offer.dims,
            })
        }

        fn active(&self, _corpus: Corpus) -> Option<VectorKey> {
            self.selected().map(|offer| VectorKey {
                model: offer.model,
                dims: Some(offer.dims),
            })
        }

        fn space(&self) -> Option<EmbedSpace> {
            self.selected().map(|offer| EmbedSpace {
                provider: offer.provider,
                model: offer.model,
                turn_dims: Some(offer.dims),
                resource_dims: Some(offer.dims),
            })
        }

        fn reset_health(&self) {
            *self.paused.lock().unwrap() = false;
        }

        fn ready(&self) -> bool {
            self.configured.load(Ordering::SeqCst) && !*self.paused.lock().unwrap()
        }

        fn health(&self) -> EmbedHealth {
            let paused = *self.paused.lock().unwrap();
            EmbedHealth {
                configured: self.configured.load(Ordering::SeqCst),
                retry_at: paused.then(|| SystemTime::now() + FIRST_PAUSE),
                ..EmbedHealth::default()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use cairn_models::types::embedding::EmbeddingCapabilities;

    use super::*;
    use crate::test_support::{capture_log, captured_log};

    use super::stub::ScriptedSource as Scripted;

    fn http(status: u16, retry_after: Option<u64>) -> EmbedError {
        EmbedError::Completion(CompletionError::Http {
            status,
            message: format!("Cairn gateway embed returned HTTP {status}: x"),
            retry_after: retry_after.map(Duration::from_secs),
        })
    }

    fn gated(source: &Arc<Scripted>) -> (FacetEmbedder, Arc<EmbedGate>) {
        let gate = Arc::new(EmbedGate::default());
        (FacetEmbedder::new(source.clone(), gate.clone()), gate)
    }

    async fn embed_one(embedder: &FacetEmbedder) -> Result<Embedded, EmbedFailure> {
        embedder
            .embed(Corpus::Resources, Purpose::Document, vec!["x".into()])
            .await
    }

    /// Let the next request through as if its wait had elapsed.
    fn elapse(gate: &EmbedGate) {
        gate.state().retry_at = Some(SystemTime::UNIX_EPOCH);
    }

    fn lines(selection: &str) -> Vec<String> {
        captured_log(HEALTH_TARGET)
            .into_iter()
            .filter(|line| line.contains(&format!("embeddings: {selection} ")))
            .collect()
    }

    fn wait(health: &EmbedHealth) -> Duration {
        health
            .retry_at
            .unwrap()
            .duration_since(health.unavailable_since.unwrap())
            .unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_outage_pauses_every_consumer_and_logs_once_each_way() {
        capture_log(HEALTH_TARGET);
        let source = Scripted::new("gate-outage");
        let (embedder, gate) = gated(&source);
        source.answer(vec![Err(http(500, None))]);

        let first = embed_one(&embedder).await.unwrap_err();
        assert!(matches!(&first, EmbedFailure::Failed(text) if text.contains("HTTP 500")));
        let health = embedder.health();
        assert!(health.configured);
        assert!(health.unavailable_since.is_some());
        assert!(health.last_error.unwrap().contains("HTTP 500"));
        assert!(!embedder.ready());

        // Before the wait is over, nothing is sent.
        for _ in 0..3 {
            assert!(matches!(
                embed_one(&embedder).await,
                Err(EmbedFailure::Paused { .. })
            ));
        }
        assert_eq!(source.sent().len(), 1);

        // Probes that keep failing log nothing more.
        for _ in 0..3 {
            elapse(&gate);
            source.answer(vec![Err(http(500, None))]);
            embed_one(&embedder).await.unwrap_err();
        }
        let logged = lines("gate-outage");
        assert_eq!(logged.len(), 1, "{logged:?}");
        assert!(logged[0].starts_with("embeddings: gate-outage unavailable since "));
        assert!(logged[0].contains("HTTP 500"));
        assert!(logged[0].contains("; next attempt "));

        // The first success clears the outage and says so once.
        elapse(&gate);
        source.answer(vec![Ok(()), Ok(())]);
        let embedded = embed_one(&embedder).await.unwrap();
        assert_eq!(
            (embedded.model.as_str(), embedded.dims),
            ("cohere-embed-v4", 3)
        );
        embed_one(&embedder).await.unwrap();
        assert_eq!(
            embedder.health(),
            EmbedHealth {
                configured: true,
                ..EmbedHealth::default()
            }
        );
        assert!(embedder.ready());
        let logged = lines("gate-outage");
        assert_eq!(logged.len(), 2, "{logged:?}");
        assert!(logged[1].starts_with("embeddings: gate-outage available again after "));
        assert!(logged[1].contains("(unavailable since "));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_caller_that_stops_waiting_still_leaves_the_gate_knowing() {
        capture_log(HEALTH_TARGET);
        let source = Scripted::new("gate-cancelled");
        *source.delay.lock().unwrap() = Duration::from_millis(300);
        source.answer(vec![Err(http(500, None))]);
        let (embedder, _) = gated(&source);

        // Search's deadline: the caller drops the call while it is in flight.
        let cut = tokio::time::timeout(Duration::from_millis(50), embed_one(&embedder)).await;
        assert!(cut.is_err(), "the call outlasted the deadline");
        assert_eq!(embedder.health().unavailable_since, None, "still in flight");

        // The request finishes on its own and settles the gate.
        for _ in 0..100 {
            if embedder.health().unavailable_since.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let health = embedder.health();
        assert!(health.unavailable_since.is_some());
        assert!(health.last_error.unwrap().contains("HTTP 500"));
        assert!(!embedder.ready());
        assert_eq!(lines("gate-cancelled").len(), 1);
    }

    #[test]
    fn the_wait_doubles_to_a_thirty_minute_cap() {
        let gate = EmbedGate::default();
        let now = SystemTime::now();
        let waits: Vec<u64> = (0..8)
            .map(|_| {
                elapse(&gate);
                let (retry_at, _) = gate.fail(now, "HTTP 503", None);
                retry_at.duration_since(now).unwrap().as_secs()
            })
            .collect();
        assert_eq!(waits, [30, 60, 120, 240, 480, 960, 1800, 1800]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn retry_after_is_the_shortest_wait() {
        let source = Scripted::new("gate-retry-after");
        let (embedder, _) = gated(&source);
        // The gateway's throttle asks for 60 s; the facet hands it back
        // rather than sleeping on it, and the gate honours it.
        source.answer(vec![Err(http(429, Some(600)))]);
        embed_one(&embedder).await.unwrap_err();
        assert!(wait(&embedder.health()) >= Duration::from_secs(600));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn consumers_failing_together_count_once() {
        let source = Scripted::new("gate-concurrent");
        let gate = Arc::new(EmbedGate::default());
        let now = SystemTime::now();
        // Three consumers each sent before the first failure closed the gate.
        for _ in 0..3 {
            gate.fail(now, "HTTP 500", None);
        }
        assert_eq!(gate.state().failures, 1);
        let embedder = FacetEmbedder::new(source, gate);
        assert_eq!(wait(&embedder.health()), FIRST_PAUSE);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_rejected_request_pauses_nothing() {
        let source = Scripted::new("gate-rejected");
        let (embedder, _) = gated(&source);
        source.answer(vec![Err(EmbedError::InvalidRequest(
            "text 0 is too long".into(),
        ))]);
        assert_eq!(
            embed_one(&embedder).await,
            Err(EmbedFailure::Rejected("text 0 is too long".into()))
        );
        // An empty text is refused before anything is sent.
        assert!(matches!(
            embedder
                .embed(
                    Corpus::Turns,
                    Purpose::Query,
                    vec!["ok".into(), "  ".into()]
                )
                .await,
            Err(EmbedFailure::Rejected(_))
        ));
        assert_eq!(source.sent().len(), 1);
        assert!(embedder.ready());
        assert_eq!(embedder.health().unavailable_since, None);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_unconfigured_offer_is_silent_and_never_an_outage() {
        capture_log(HEALTH_TARGET);
        let source = Scripted::new("gate-unconfigured");
        source.configured.store(false, Ordering::SeqCst);
        let (embedder, _) = gated(&source);
        assert!(!embedder.ready());
        assert_eq!(embed_one(&embedder).await, Err(EmbedFailure::Unconfigured));
        assert!(source.sent().is_empty());
        let health = embedder.health();
        assert!(!health.configured);
        assert_eq!(health.unavailable_since, None);
        assert_eq!(health.retry_at, None);
        assert!(lines("gate-unconfigured").is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn every_text_is_capped_on_a_character_boundary() {
        let source = Scripted::new("gate-caps");
        let (embedder, _) = gated(&source);
        source.answer(vec![Ok(())]);
        // A three-byte character straddles the budget.
        let long = format!(
            "{}✓{}",
            "a".repeat(EMBED_TEXT_BUDGET - 1),
            "b".repeat(50_000)
        );
        embedder
            .embed(
                Corpus::Resources,
                Purpose::Document,
                vec![long, "short".into()],
            )
            .await
            .unwrap();
        let sent = &source.sent()[0];
        assert_eq!(sent.texts[0], "a".repeat(EMBED_TEXT_BUDGET - 1));
        assert_eq!(sent.texts[1], "short");
        assert_eq!(sent.width, Width::Preferred(1536));
    }

    #[test]
    fn each_corpus_is_stored_at_the_width_the_offer_returns() {
        let gateway = EmbeddingCapabilities::cairn_gateway();
        assert_eq!(Corpus::Turns.width().on(&gateway), Some(1536));
        assert_eq!(Corpus::Resources.width().on(&gateway), Some(1536));
        let matryoshka = EmbeddingCapabilities {
            native_dims: Some(768),
            widths: cairn_models::types::embedding::EmbeddingWidths::UpTo { min: 64 },
            ..gateway
        };
        assert_eq!(Corpus::Turns.width().on(&matryoshka), Some(256));
        assert_eq!(Corpus::Resources.width().on(&matryoshka), Some(768));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_text_is_cut_to_the_offers_own_bound_at_the_door() {
        let source = Scripted::new("gate-offer-bound");
        *source.capabilities.lock().unwrap() = EmbeddingCapabilities {
            purpose: PurposeHandling::Prefix {
                document: "search_document: ".into(),
                query: "q: ".into(),
            },
            limits: cairn_models::types::embedding::BatchLimits {
                max_chars: Some(1000),
                ..Default::default()
            },
            ..EmbeddingCapabilities::cairn_gateway()
        };
        let (embedder, _) = gated(&source);
        source.answer(vec![Ok(()), Ok(())]);
        embedder
            .embed(Corpus::Turns, Purpose::Document, vec!["a".repeat(5000)])
            .await
            .unwrap();
        embedder
            .embed(Corpus::Turns, Purpose::Query, vec!["b".repeat(5000)])
            .await
            .unwrap();
        let sent = source.sent();
        assert_eq!(sent[0].texts[0].len(), 1000 - "search_document: ".len());
        assert_eq!(sent[1].texts[0].len(), 1000 - "q: ".len());
        // The gateway itself is bounded only by the shared budget.
        assert_eq!(
            text_budget(
                Some(&EmbeddingCapabilities::cairn_gateway()),
                Purpose::Document
            ),
            EMBED_TEXT_BUDGET
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_reset_gate_is_open_and_remembers_no_outage() {
        let source = Scripted::new("gate-reset");
        let (embedder, gate) = gated(&source);
        source.answer(vec![Err(http(500, None))]);
        embed_one(&embedder).await.unwrap_err();
        assert!(!embedder.ready());
        gate.reset();
        assert!(embedder.ready());
        assert_eq!(
            embedder.health(),
            EmbedHealth {
                configured: true,
                ..EmbedHealth::default()
            }
        );
        // The next failure is the first of a new outage.
        source.answer(vec![Err(http(500, None))]);
        embed_one(&embedder).await.unwrap_err();
        assert_eq!(wait(&embedder.health()), FIRST_PAUSE);
    }

    #[test]
    fn the_space_is_the_offer_with_each_corpus_width() {
        let source = Scripted::new("gate-space");
        let embedder = source.embedder();
        assert_eq!(
            embedder.space(),
            Some(EmbedSpace {
                provider: "cairn".into(),
                model: "cohere-embed-v4".into(),
                turn_dims: Some(1536),
                resource_dims: Some(1536),
            })
        );
    }

    #[test]
    fn spans_read_as_an_operator_writes_them() {
        assert_eq!(span(Duration::from_secs(30)), "30s");
        assert_eq!(span(Duration::from_secs(250)), "4m 10s");
        assert_eq!(span(Duration::from_secs(11 * 3600 + 125)), "11h 2m");
    }
}
