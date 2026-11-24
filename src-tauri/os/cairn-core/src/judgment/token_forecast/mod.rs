//! The production token forecast: an issue at filing in, billable-token
//! p50/p90 with provenance out, or an explicit `unavailable` or
//! `not_applicable` (cairn/7356). cairn/6268's queue placement is the
//! consumer; the contract is [`contract`], and nothing here decides capacity,
//! windows or admission.
//!
//! # The path
//!
//! cairn/6271 measured it (`bench/token-forecast/`, `FINDINGS.md`): two
//! option-independent score questions about the filing text (effort and
//! scope, [`question`]) asked through `decide` on a zero-retention model
//! ([`answerer`]); their mean expected score falls in one of five equal-count
//! bins whose p50/p80/p90 of ln billable tokens were fit on resolved
//! Anthropic-filed issues, with p80 and p90 raised by split-conformal shifts
//! taken on a later window ([`calibration`], [`fit`]). On 6271's test window
//! that gave MdALE 0.78 against the global median's 1.16, with p90 coverage
//! 0.94.
//!
//! # Seams
//!
//! [`TokenForecaster`] is the whole contract. The production implementation,
//! [`DecideScoreForecaster`], is built from a [`ScoreAnswerer`] (text in,
//! score out) and a [`ForecastStore`] (the active calibration, and where
//! labels go). An answerer that replaces decide (cairn/6284's embedding head)
//! replaces only the [`ScoreAnswerer`]; one that predicts tokens directly
//! replaces the whole [`TokenForecaster`]. Calibrations are keyed by answerer
//! and question version, so a new answerer has none until it is refit and
//! forecasts `no_active_calibration` rather than mis-scaled numbers.
//!
//! # Where the artifacts live
//!
//! Labels and calibrations are rows in the private app database
//! (`token_forecast_labels`, `token_forecast_calibrations`, readable through
//! `cairn://db`). Every answered filing call stores a label; the background
//! [`worker`] backfills labels for resolved issues and refits weekly from the
//! database alone ([`population`], [`backfill`]). Replay reports are
//! aggregate-only, in `bench/token-forecast/reports/production-replay.md`.

pub mod answerer;
pub mod applicability;
pub mod backfill;
pub mod calibration;
pub mod contract;
pub mod fit;
pub mod population;
pub mod question;
pub mod store;
pub mod worker;

#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;

use self::applicability::Applicability;
use self::calibration::Calibration;
use self::contract::{
    AnswererProvenance, ApplicabilityFlags, Forecast, ForecastOutcome, ForecastParts,
    ForecastRequest, UnavailableReason, ANSWERER_KIND_DECIDE_SCORE,
};
use self::store::{LabelRecord, LabelSource, LabelStatus, TextSource};
use crate::orchestrator::Orchestrator;

/// The decide-score answerer's version. Bumped with the model or anything
/// about how it is asked, which leaves production without an active
/// calibration until a refit.
pub const ANSWERER_VERSION: &str = "decide_score.jev-1.14.v1";
/// The model cairn/6271 measured and every calibration of this answerer is
/// fit on.
pub const DECIDE_MODEL: &str = "typesafe/jev-1.14";
pub const DECIDE_PROVIDER: &str = "openrouter";

/// The forecast seam cairn/6268 consumes.
#[async_trait]
pub trait TokenForecaster: Send + Sync {
    async fn forecast(&self, request: ForecastRequest) -> ForecastOutcome;
}

/// What an answerer is shown: the scrubbed filing text, and the decide input
/// rendered from it.
#[derive(Debug, Clone, PartialEq)]
pub struct AnswerInput {
    pub text: String,
    pub rendered: String,
}

impl AnswerInput {
    pub fn new(scrubbed_text: String) -> Self {
        Self {
            rendered: question::render_input(&scrubbed_text),
            text: scrubbed_text,
        }
    }
}

/// An answerer's reading of one filing.
#[derive(Debug, Clone, PartialEq)]
pub struct ScoreAnswer {
    pub effort: f64,
    pub scope: f64,
    /// The calibrated quantity: the mean of `effort` and `scope`.
    pub score: f64,
    /// Each question's level probabilities as answered.
    pub distributions: serde_json::Value,
    pub provenance: AnswerProvenance,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AnswerProvenance {
    pub model: String,
    pub provider: String,
    pub measured_by: String,
    pub zdr_satisfied_by: String,
    pub cost_usd: Option<f64>,
    pub latency_ms: u64,
}

/// Why an answerer produced no score.
#[derive(Debug, Clone, PartialEq)]
pub enum AnswerError {
    /// Reached and failed; asking again may work.
    Failed(String),
    /// Nothing configured can answer as asked (zero retention unmet, no
    /// offer, context exceeded).
    Unanswerable(String),
    /// The model declined or left a question unmeasured. The call was still
    /// made, so its cost is carried when the reply reported one.
    Refused {
        reason: String,
        cost_usd: Option<f64>,
    },
}

/// Text in, score out: the part of the forecast a cheaper answerer replaces.
#[async_trait]
pub trait ScoreAnswerer: Send + Sync {
    /// The version calibrations of this answerer are keyed by.
    fn version(&self) -> &str;
    async fn answer(
        &self,
        input: &AnswerInput,
        deadline: Instant,
    ) -> Result<ScoreAnswer, AnswerError>;
}

/// The active calibration, and where answered labels go.
#[async_trait]
pub trait ForecastStore: Send + Sync {
    async fn active_calibration(
        &self,
        answerer_version: &str,
        question_version: &str,
    ) -> Result<Option<Calibration>, String>;
    async fn record_label(&self, label: LabelRecord) -> Result<(), String>;
}

/// sha256 over everything that determines a forecast's input.
pub fn input_hash(
    rendered: &str,
    question_version: &str,
    maker: &str,
    model: Option<&str>,
    recipe: Option<&str>,
    role: Option<&str>,
) -> String {
    let parts = serde_json::json!([rendered, question_version, maker, model, recipe, role]);
    question::sha256_hex(&parts.to_string())
}

/// The production forecaster: applicability, scrub, active calibration,
/// one bounded answer, lookup.
pub struct DecideScoreForecaster {
    answerer: Arc<dyn ScoreAnswerer>,
    store: Arc<dyn ForecastStore>,
}

impl DecideScoreForecaster {
    pub fn new(answerer: Arc<dyn ScoreAnswerer>, store: Arc<dyn ForecastStore>) -> Self {
        Self { answerer, store }
    }
}

#[async_trait]
impl TokenForecaster for DecideScoreForecaster {
    async fn forecast(&self, request: ForecastRequest) -> ForecastOutcome {
        use UnavailableReason::*;
        let maker = match applicability::check(&request) {
            Applicability::Applies { maker } => maker,
            Applicability::NotApplicable(refusal) => {
                return ForecastOutcome::NotApplicable(refusal)
            }
        };
        let text = match question::scrubbed_filing_text(&request.title, &request.description) {
            Ok(text) => text,
            Err(refused) => {
                return ForecastOutcome::unavailable(ScrubUnavailable, refused.to_string())
            }
        };
        // The deadline bounds everything after this point, the store reads
        // included: the filing path never waits on the forecast past it.
        let remaining = request.deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return ForecastOutcome::unavailable(DeadlineExceeded, "the deadline had passed");
        }
        match tokio::time::timeout(remaining, self.calibrated_answer(&request, maker, text)).await {
            Ok(outcome) => outcome,
            Err(_) => ForecastOutcome::unavailable(
                DeadlineExceeded,
                format!("no forecast within {} ms", remaining.as_millis()),
            ),
        }
    }
}

impl DecideScoreForecaster {
    async fn calibrated_answer(
        &self,
        request: &ForecastRequest,
        maker: String,
        text: String,
    ) -> ForecastOutcome {
        use UnavailableReason::*;
        let answerer_version = self.answerer.version().to_string();
        let question_version = question::question_version();
        // Read before any model call, so an uncalibrated answerer spends nothing.
        let calibration = match self
            .store
            .active_calibration(&answerer_version, question_version)
            .await
        {
            Ok(Some(calibration)) => calibration,
            Ok(None) => {
                return ForecastOutcome::unavailable(
                    NoActiveCalibration,
                    format!(
                        "no calibration is active for answerer {answerer_version}, questions {question_version}"
                    ),
                )
            }
            Err(error) => return ForecastOutcome::unavailable(NoActiveCalibration, error),
        };
        let input = AnswerInput::new(text);
        let input_hash = input_hash(
            &input.rendered,
            question_version,
            &maker,
            request.filing_model.as_deref(),
            request.recipe.as_deref(),
            request.first_step_role.as_deref(),
        );
        let answer = match self.answerer.answer(&input, request.deadline).await {
            Err(AnswerError::Failed(error)) => {
                return ForecastOutcome::unavailable(AnswererFailed, error)
            }
            Err(AnswerError::Unanswerable(error)) => {
                return ForecastOutcome::unavailable(AnswererUnanswerable, error)
            }
            Err(AnswerError::Refused { reason, .. }) => {
                return ForecastOutcome::unavailable(Refused, reason)
            }
            Ok(answer) => answer,
        };
        let label = LabelRecord {
            issue_id: request.issue.id.clone(),
            answerer_version: answerer_version.clone(),
            question_version: question_version.to_string(),
            status: LabelStatus::Labelled,
            skip_reason: None,
            answer: Some(answer.clone()),
            text_hash: Some(question::sha256_hex(&input.text)),
            text_source: Some(TextSource::Filing),
            filing_maker: Some(maker.clone()),
            filing_maker_method: Some("filing".to_string()),
            source: LabelSource::Filing,
        };
        // The answer is paid for, so its label is kept even when the store is
        // slow: written off the caller's path, finishing after the forecast
        // returns if it must.
        let store = self.store.clone();
        let uri = request.issue.uri.clone();
        cairn_common::task::spawn(async move {
            if let Err(error) = store.record_label(label).await {
                log::warn!("token forecast: the label for {uri} was not stored: {error}");
            }
        });
        available(&calibration, &answer, &answerer_version, input_hash)
    }
}

fn available(
    calibration: &Calibration,
    answer: &ScoreAnswer,
    answerer_version: &str,
    input_hash: String,
) -> ForecastOutcome {
    let Some(looked) = calibration.lookup(answer.score) else {
        return ForecastOutcome::unavailable(
            UnavailableReason::NoActiveCalibration,
            format!(
                "calibration {} has no bin for score {}",
                calibration.version, answer.score
            ),
        );
    };
    let parts = ForecastParts {
        answerer: AnswererProvenance {
            kind: ANSWERER_KIND_DECIDE_SCORE.to_string(),
            version: answerer_version.to_string(),
            model: answer.provenance.model.clone(),
            provider: answer.provenance.provider.clone(),
            question_version: calibration.question_version.clone(),
            measured_by: answer.provenance.measured_by.clone(),
            zdr_satisfied_by: answer.provenance.zdr_satisfied_by.clone(),
            cost_usd: answer.provenance.cost_usd,
            latency_ms: answer.provenance.latency_ms,
        },
        calibration: calibration.provenance(),
        flags: ApplicabilityFlags {
            p90_soft: looked.soft_basis.is_some(),
            p90_soft_basis: looked.soft_basis,
        },
        input_hash,
    };
    match Forecast::from_ln(looked.quantiles[0], looked.quantiles[2], parts) {
        Ok(forecast) => ForecastOutcome::Available(Box::new(forecast)),
        Err(error) => ForecastOutcome::unavailable(
            UnavailableReason::NoActiveCalibration,
            format!(
                "calibration {} gave no valid forecast: {error}",
                calibration.version
            ),
        ),
    }
}

/// The production forecaster for `orch`: decide on the private database's
/// active calibration.
pub(crate) fn production(orch: &Orchestrator) -> Arc<dyn TokenForecaster> {
    Arc::new(DecideScoreForecaster::new(
        Arc::new(answerer::DecideAnswerer::new(orch.clone())),
        Arc::new(store::DbForecastStore::new(
            orch.db.local.clone(),
            orch.services.clock.clone(),
        )),
    ))
}
