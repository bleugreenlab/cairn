//! The decide-score answerer: cairn/6271's two score questions asked through
//! the models facet's `decide`, on the one model the calibration was fit on.
//!
//! The model is a constant tied to [`super::ANSWERER_VERSION`], not the
//! workspace `decisionModel`, because a calibration is valid only for the
//! model it was fit on. Every call requires zero data retention and a reply
//! that does not say how it was kept is refused (tenet 1329). The call goes
//! through `Orchestrator::decide_named`, the crate-internal door behind
//! `run cairn://models/decide`.

use std::time::Instant;

use async_trait::async_trait;
use cairn_models::interface::retention::Requirements;
use serde_json::{json, Value};

use super::question::{decision_questions, expected_score, question_names};
use super::{
    AnswerError, AnswerInput, AnswerProvenance, ScoreAnswer, ScoreAnswerer, ANSWERER_VERSION,
    DECIDE_MODEL, DECIDE_PROVIDER,
};
use crate::models::decision::{Decision, DecisionAnswer};
use crate::orchestrator::{DecideRequest, DecisionFailure, Orchestrator};

pub struct DecideAnswerer {
    orch: Orchestrator,
}

impl DecideAnswerer {
    pub fn new(orch: Orchestrator) -> Self {
        Self { orch }
    }
}

#[async_trait]
impl ScoreAnswerer for DecideAnswerer {
    fn version(&self) -> &str {
        ANSWERER_VERSION
    }

    /// The caller bounds the call by `deadline`; dropping this future
    /// releases it at once (see `Orchestrator::decide_named`).
    async fn answer(
        &self,
        input: &AnswerInput,
        _deadline: Instant,
    ) -> Result<ScoreAnswer, AnswerError> {
        let request = DecideRequest {
            model: Some(DECIDE_MODEL.to_string()),
            provider: Some(DECIDE_PROVIDER.to_string()),
            input: input.rendered.clone(),
            questions: decision_questions(),
            requirements: Requirements::zero_data_retention(),
        };
        let outcome =
            self.orch
                .decide_named(None, request)
                .await
                .map_err(|failure| match failure {
                    DecisionFailure::Unanswerable(text) => AnswerError::Unanswerable(text),
                    DecisionFailure::Failed(text) => AnswerError::Failed(text),
                })?;
        let Some(zdr) = outcome.zero_data_retention.as_ref() else {
            return Err(AnswerError::Unanswerable(
                "the decide reply did not say how zero data retention was kept".to_string(),
            ));
        };
        let zdr_satisfied_by = serde_json::to_value(zdr.satisfied_by)
            .ok()
            .and_then(|value| value.as_str().map(str::to_string))
            .unwrap_or_else(|| format!("{:?}", zdr.satisfied_by));
        score_answer(
            &outcome.decision,
            AnswerProvenance {
                model: outcome.offer.model.clone(),
                provider: outcome.offer.provider.clone(),
                measured_by: outcome.offer.measured_by.as_str().to_string(),
                zdr_satisfied_by,
                cost_usd: outcome.decision.cost_usd(),
                latency_ms: outcome.latency_ms,
            },
        )
    }
}

/// The score of a decision: the mean of the questions' expected scores
/// (`score.py`: `(effort + scope) / 2`). A refused or unmeasured question
/// refuses the whole answer, as the bench dropped such labels.
pub fn score_answer(
    decision: &Decision,
    provenance: AnswerProvenance,
) -> Result<ScoreAnswer, AnswerError> {
    let refused = |reason: String| AnswerError::Refused {
        reason,
        cost_usd: provenance.cost_usd,
    };
    let mut expected = Vec::new();
    let mut distributions = serde_json::Map::new();
    for name in question_names() {
        match decision.answer(name) {
            Some(DecisionAnswer::Score { probabilities, .. }) => {
                let Some(value) = expected_score(probabilities) else {
                    return Err(refused(format!(
                        "{name} came back with no level probabilities"
                    )));
                };
                expected.push(value);
                distributions.insert(
                    name.to_string(),
                    serde_json::to_value(probabilities).unwrap_or(Value::Null),
                );
            }
            Some(DecisionAnswer::Refusal { reason, .. }) => {
                return Err(refused(format!(
                    "{name} was refused: {}",
                    reason.as_deref().unwrap_or("no reason given")
                )));
            }
            Some(_) => {
                return Err(refused(format!("{name} was not answered as a score")));
            }
            None => {
                return Err(refused(format!("{name} was not answered")));
            }
        }
    }
    let [effort, scope] = expected[..] else {
        return Err(refused(
            "the questions are not effort and scope".to_string(),
        ));
    };
    Ok(ScoreAnswer {
        effort,
        scope,
        score: (effort + scope) / 2.0,
        distributions: json!(distributions),
        provenance,
    })
}
