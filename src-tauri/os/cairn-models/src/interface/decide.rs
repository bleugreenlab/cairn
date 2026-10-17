//! The facet's decision verb: typed questions answered with measured
//! probabilities, sub-second, generating nothing.
//!
//! `decide` is its own verb rather than a Response with a typed contract
//! because its defining property is cost: a Decisions call is an input-only
//! request answered in hundreds of milliseconds, and routing it through the
//! Response path (a process per call, a prompt-text journal row) would throw
//! that away. It shares `complete`'s substrate instead: the same [`Complete`]
//! carries its POST, and the same [`super::Catalog`] chooses its offer
//! ([`super::Catalog::offer_for`] asked for [`VerbKind::Decide`]).
//!
//! An offer declares that it serves `decide` over a wire; how its answers are
//! measured follows from that wire ([`measured_by`]) and is never declared
//! beside it.
//!
//! [`VerbKind::Decide`]: cairn_models_types::catalog::VerbKind::Decide
//!
//! Questions and answers are `cairn_domain::decision` types; the facet defines
//! no mirror of them. What it adds is how an answer is measured: natively over
//! the Decisions wire, or from a chat model's next-token logprobs over a closed
//! label set. A chat model asked to report its own probabilities is not a
//! route: the number would not be a measurement.
//!
//! [`Complete`]: super::complete::Complete

use std::fmt;
use std::time::Duration;

use cairn_domain::decision::{DecisionQuestion, MeasuredBy};
use cairn_models_types::catalog::Wire;

use super::complete::{CompletionError, CompletionWire};
use super::NoOffer;
use crate::wire::decisions::http::DecisionsEndpoint;
use crate::wire::openai_compat::http::Endpoint;

/// How a decision is measured, and the deployment that measures it.
#[derive(Debug, Clone)]
pub enum DecisionRoute {
    /// A decision model over the Decisions wire.
    Native(DecisionsEndpoint),
    /// A chat-completions endpoint that serves next-token logprobs.
    Logprobs(Endpoint),
}

impl DecisionRoute {
    /// The completion route a decision on this deployment is sent over.
    pub fn completion_wire(&self) -> CompletionWire<'_> {
        match self {
            Self::Native(endpoint) => CompletionWire::Decisions(endpoint),
            Self::Logprobs(endpoint) => CompletionWire::Chat(endpoint),
        }
    }

    /// How this route measures: by the wire it is sent over, through the one
    /// wire-to-method mapping ([`measured_by`]).
    pub fn measured_by(&self) -> MeasuredBy {
        measured_by(self.completion_wire().wire())
            .expect("a decision route is sent over a wire that measures decisions")
    }

    pub fn backend_key(&self) -> &str {
        match self {
            Self::Native(endpoint) => &endpoint.backend_key,
            Self::Logprobs(endpoint) => &endpoint.backend_key,
        }
    }

    pub fn provider_name(&self) -> &str {
        match self {
            Self::Native(endpoint) => &endpoint.provider_name,
            Self::Logprobs(endpoint) => &endpoint.provider_name,
        }
    }
}

/// How a decision served over `wire` is measured, or `None` for a wire that
/// cannot measure one.
pub fn measured_by(wire: Wire) -> Option<MeasuredBy> {
    match wire {
        Wire::OpenAiDecisions => Some(MeasuredBy::Native),
        Wire::OpenAiChatCompletions => Some(MeasuredBy::Logprobs),
        Wire::AnthropicMessages
        | Wire::OpenAiResponses
        | Wire::ClaudeStreamJson
        | Wire::CodexAppServer => None,
    }
}

/// One decision request.
pub struct DecisionCall<'a> {
    pub route: &'a DecisionRoute,
    /// The id the provider is sent.
    pub model: &'a str,
    /// The text every question is asked about.
    pub input: &'a str,
    pub questions: &'a [DecisionQuestion],
    /// Bounds each provider request.
    pub timeout: Duration,
}

/// The fixes every no-offer refusal of a decision names.
pub const DECISION_FIXES: &str =
    "A decision is answered by a decision model or by a local model's \
logprobs, never by a chat model reporting its own probabilities. To fix it, add a custom provider \
in Settings → Providers: either a Decisions API key (OpenAI at https://api.openai.com/v1, the \
Vercel AI Gateway at https://ai-gateway.vercel.sh/v1, or TypeSafe Jev) with the protocol set to \
Decisions, or an OpenAI-compatible server that returns logprobs (for example vLLM) with the \
protocol set to OpenAI chat.";

/// Why a decision produced no answers. A refused question is not an error:
/// it is an answer ([`cairn_domain::decision::DecisionAnswer::Refusal`]).
#[derive(Debug, PartialEq)]
pub enum DecideError {
    NoOffer(NoOffer),
    /// The questions cannot be asked as written.
    InvalidQuestions(Vec<String>),
    /// A logprobs route's endpoint returned no logprobs, so it cannot measure
    /// a decision. Nothing is fabricated in their place.
    NoLogprobs {
        provider: String,
        model: String,
    },
    /// A logprobs route's model generated something other than an answer
    /// label (typically the start of its reasoning), so its logprobs at that
    /// position do not measure an answer.
    NotAnswering {
        provider: String,
        model: String,
        /// The token it generated instead.
        began: String,
    },
    /// A question this route cannot measure.
    Unmeasurable(String),
    Completion(CompletionError),
}

impl fmt::Display for DecideError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoOffer(refusal) => refusal.fmt(f),
            Self::InvalidQuestions(problems) => {
                write!(f, "the decision cannot be asked: {}", problems.join("; "))
            }
            Self::NoLogprobs { provider, model } => write!(
                f,
                "{provider} returned no logprobs for {model}, so it cannot answer a decision. Enable logprobs on that server (vLLM serves them by default), or use a Decisions provider. {DECISION_FIXES}"
            ),
            Self::NotAnswering {
                provider,
                model,
                began,
            } => write!(
                f,
                "{model} on {provider} began '{began}' instead of an answer label, so its logprobs do not measure an answer. A logprobs decision needs a model that answers at once: switch its reasoning off on the server (Cairn asks with reasoning_effort none, which Ollama and vLLM 0.21 or later honour), choose a model without a reasoning channel, or use a Decisions provider. {DECISION_FIXES}"
            ),
            Self::Unmeasurable(text) => f.write_str(text),
            Self::Completion(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for DecideError {}

impl DecideError {
    /// Why the model this ran on cannot answer decisions at all, as a short
    /// clause, when that is what failed. `None` for a failure that says
    /// nothing about the model (a transport error, a bad request).
    pub fn incapability(&self) -> Option<String> {
        match self {
            Self::NoLogprobs { .. } => Some("returned no logprobs".to_string()),
            Self::NotAnswering { began, .. } => {
                Some(format!("began '{began}' instead of an answer label"))
            }
            _ => None,
        }
    }
}

impl From<CompletionError> for DecideError {
    fn from(error: CompletionError) -> Self {
        Self::Completion(error)
    }
}

impl From<NoOffer> for DecideError {
    fn from(refusal: NoOffer) -> Self {
        Self::NoOffer(refusal)
    }
}
