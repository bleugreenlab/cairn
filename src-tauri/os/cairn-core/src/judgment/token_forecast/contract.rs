//! The forecast contract cairn/6268's queue placement consumes, as agreed in
//! the section "Forecast contract: external prerequisite, not silently
//! complete" of its plan. Changes to these shapes are agreed with harness on
//! cairn/7356 first.
//!
//! A forecast is one of three outcomes. `available` carries billable-token
//! p50/p90 with the target it measures, the answerer and calibration that
//! produced it, applicability flags and the hash of what the answerer saw.
//! `unavailable` says why nothing was forecast and whether asking again can
//! help. `not_applicable` says the issue is outside what the forecaster was
//! validated on (maker, role, or empty text).

use std::ops::Deref;
use std::time::Instant;

use serde::{Deserialize, Serialize};

/// The id of the one target a forecast measures.
pub const TARGET_ID: &str = "issue_billable_primary_maker.v1";

/// The target in words, the definition `bench/token-forecast/README.md`
/// measured.
pub const TARGET_DEFINITION: &str = "billable tokens (input + cache_create + cache_read + output, thinking excluded) of the issue's realised primary maker, over every execution and sub-agent job, through resolution";

/// One valid example of each outcome, as serialized: the fixture a consumer
/// tests its storage and rendering against instead of writing its own.
#[cfg(any(test, feature = "test-utils"))]
pub const VALID_OUTCOMES_FIXTURE: &str = include_str!("fixtures/forecast_outcomes.json");

/// The answerer kind of the decide-score path.
pub const ANSWERER_KIND_DECIDE_SCORE: &str = "decide_score";

/// The issue a forecast is about: its row id and canonical URI.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IssueRef {
    pub id: String,
    pub uri: String,
}

/// What the consumer knows at filing.
#[derive(Debug, Clone)]
pub struct ForecastRequest {
    pub issue: IssueRef,
    pub title: String,
    pub description: String,
    /// The consumer's hash of the filing title and description.
    pub text_hash: String,
    /// The maker selected for the filing-time agent, when known.
    pub filing_maker: Option<String>,
    /// The model selected for the filing-time agent; the maker is derived
    /// from it when `filing_maker` is absent.
    pub filing_model: Option<String>,
    pub recipe: Option<String>,
    /// The agent the recipe's first step runs.
    pub first_step_role: Option<String>,
    /// When the consumer took its snapshot (unix seconds).
    pub snapshot_at: i64,
    /// The forecast answers by this instant or is `unavailable`.
    pub deadline: Instant,
}

/// One forecast.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum ForecastOutcome {
    Available(Box<Forecast>),
    Unavailable(Unavailable),
    NotApplicable(NotApplicable),
}

impl ForecastOutcome {
    pub fn unavailable(reason: UnavailableReason, detail: impl Into<String>) -> Self {
        Self::Unavailable(Unavailable::new(reason, detail))
    }

    pub fn not_applicable(reason: NotApplicableReason, detail: impl Into<String>) -> Self {
        Self::NotApplicable(NotApplicable {
            reason,
            detail: detail.into(),
        })
    }
}

/// What a forecast measures.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TargetDef {
    pub id: String,
    pub definition: String,
}

impl TargetDef {
    pub fn current() -> Self {
        Self {
            id: TARGET_ID.to_string(),
            definition: TARGET_DEFINITION.to_string(),
        }
    }
}

/// Who answered, with what, and what it cost.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AnswererProvenance {
    pub kind: String,
    pub version: String,
    pub model: String,
    pub provider: String,
    pub question_version: String,
    pub measured_by: String,
    /// How zero data retention was kept (tenet 1329).
    pub zdr_satisfied_by: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_usd: Option<f64>,
    pub latency_ms: u64,
}

/// The calibration a score was mapped through.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CalibrationProvenance {
    pub version: String,
    pub as_of: i64,
    /// Issues created in this window and resolved before its end fit the
    /// score bins (unix seconds, `[from, to)`).
    pub table_window: [i64; 2],
    /// Issues created in this window took the conformal shift.
    pub shift_window: [i64; 2],
    pub fit_n: u64,
    pub shift_n: u64,
    /// The filing-maker group the calibration was fit on.
    pub group: String,
}

/// Why a p90 should be read as soft. A forecast carries one basis; when
/// several hold, it names the first in this order (agreed with harness and
/// judgment on cairn/7356): `largest_actuals_heldout_coverage_below_0_85`,
/// because it covers the whole calibration, then `top_score_bin`, then
/// `bin_heldout_coverage_below_0_85`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SoftBasis {
    /// The largest score bin, where 6271 measured p90 coverage of 0.77.
    TopScoreBin,
    /// The bin's held-out shifted p90 coverage was below 0.85 at fit time.
    #[serde(rename = "bin_heldout_coverage_below_0_85")]
    BinHeldoutCoverageBelow085,
    /// The largest fifth of shift-window issues by actual tokens had
    /// held-out shifted p90 coverage below 0.85 at fit time. Actual size is
    /// unknowable at filing and the score does not isolate those issues, so
    /// every p90 of that calibration is soft.
    #[serde(rename = "largest_actuals_heldout_coverage_below_0_85")]
    LargestActualsHeldoutCoverageBelow085,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplicabilityFlags {
    pub p90_soft: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub p90_soft_basis: Option<SoftBasis>,
}

/// Every field of an available forecast. Read through [`Forecast`], which
/// is only built with its invariants held.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ForecastFields {
    pub p50_tokens: u64,
    pub p90_tokens: u64,
    pub target: TargetDef,
    pub answerer: AnswererProvenance,
    pub calibration: CalibrationProvenance,
    pub flags: ApplicabilityFlags,
    /// sha256 of the scrubbed rendered input, the question version, the
    /// maker, the model, the recipe and the role.
    pub input_hash: String,
}

/// An available forecast: p90 >= p50, both whole nonnegative token counts,
/// for the current target. Deserializing checks the same invariants.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "ForecastFields", into = "ForecastFields")]
pub struct Forecast(ForecastFields);

/// Everything of a forecast but its quantiles.
#[derive(Debug, Clone, PartialEq)]
pub struct ForecastParts {
    pub answerer: AnswererProvenance,
    pub calibration: CalibrationProvenance,
    pub flags: ApplicabilityFlags,
    pub input_hash: String,
}

impl Forecast {
    /// From quantiles on the natural-log scale the calibration is fit on.
    /// The billable target is plain `ln(tokens)`, not `ln(1 + tokens)`, so a
    /// quantile converts back as `exp(v)`, rounded to whole tokens.
    pub fn from_ln(p50_ln: f64, p90_ln: f64, parts: ForecastParts) -> Result<Self, String> {
        let tokens = |name: &str, ln: f64| -> Result<u64, String> {
            let value = ln.exp().round();
            if !value.is_finite() || value < 0.0 || value >= u64::MAX as f64 {
                return Err(format!("{name} of ln {ln} is not a finite token count"));
            }
            Ok(value as u64)
        };
        Self::try_from(ForecastFields {
            p50_tokens: tokens("p50", p50_ln)?,
            p90_tokens: tokens("p90", p90_ln)?,
            target: TargetDef::current(),
            answerer: parts.answerer,
            calibration: parts.calibration,
            flags: parts.flags,
            input_hash: parts.input_hash,
        })
    }
}

impl TryFrom<ForecastFields> for Forecast {
    type Error = String;

    fn try_from(fields: ForecastFields) -> Result<Self, String> {
        if fields.p90_tokens < fields.p50_tokens {
            return Err(format!(
                "p90 ({}) is below p50 ({})",
                fields.p90_tokens, fields.p50_tokens
            ));
        }
        if fields.target.id != TARGET_ID {
            return Err(format!("target {} is not {TARGET_ID}", fields.target.id));
        }
        if fields.flags.p90_soft != fields.flags.p90_soft_basis.is_some() {
            return Err("p90_soft and its basis disagree".to_string());
        }
        Ok(Self(fields))
    }
}

impl From<Forecast> for ForecastFields {
    fn from(forecast: Forecast) -> Self {
        forecast.0
    }
}

impl Deref for Forecast {
    type Target = ForecastFields;

    fn deref(&self) -> &ForecastFields {
        &self.0
    }
}

/// Why nothing was forecast.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnavailableReason {
    /// No calibration is active for the answerer and question version.
    NoActiveCalibration,
    DeadlineExceeded,
    /// The answerer was reached and something went wrong.
    AnswererFailed,
    /// Nothing configured can answer as asked: zero data retention unmet, no
    /// offer, or the input exceeds the model's context.
    AnswererUnanswerable,
    /// The model declined a question.
    Refused,
    /// The filing text could not be scrubbed of secrets, so nothing was sent.
    ScrubUnavailable,
}

impl UnavailableReason {
    /// Whether asking again, unchanged, can produce a forecast.
    pub fn retryable(self) -> bool {
        matches!(
            self,
            Self::NoActiveCalibration | Self::DeadlineExceeded | Self::AnswererFailed
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Unavailable {
    pub reason: UnavailableReason,
    pub retryable: bool,
    pub detail: String,
}

impl Unavailable {
    pub fn new(reason: UnavailableReason, detail: impl Into<String>) -> Self {
        Self {
            reason,
            retryable: reason.retryable(),
            detail: detail.into(),
        }
    }
}

/// Why the issue is outside what the forecaster was validated on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NotApplicableReason {
    /// Validated on Anthropic-filed issues only (cairn/6271).
    MakerNotAnthropic,
    MakerUnknown,
    /// A coordinator's own tokens measured 0.47 p90 coverage (cairn/6271).
    CoordinatorRole,
    EmptyText,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotApplicable {
    pub reason: NotApplicableReason,
    pub detail: String,
}
