//! Whether an issue is one the forecaster was validated on, decided before
//! any model call. cairn/6271 validated the decide-score path on issues filed
//! with an Anthropic model, and found a coordinator's own tokens unpredictable
//! from its text (p90 coverage 0.47); judgment's 11-06 steer leaves both out.
//! Whether an issue will fan out is unknowable at filing, so only the
//! first-step role is checked.
//!
//! The recipe is carried for provenance only and is never a feature: the
//! questions name no recipe, model, tier or project (tenet 1328).

use std::sync::LazyLock;

use regex::Regex;

use super::contract::{ForecastRequest, NotApplicable, NotApplicableReason};

pub const ANTHROPIC: &str = "anthropic";
pub const OPENAI: &str = "openai";
const COORDINATOR: &str = "coordinator";

/// Name prefixes of open-weight families, checked on the model's leaf name
/// and then on its vendor segment (`hf.co/<org>/<model>`, `~deepseek/...`).
const FAMILY_PREFIXES: &[(&str, &str)] = &[
    ("deepseek", "deepseek"),
    ("dsv", "deepseek"),
    ("qwen", "qwen"),
    ("gemma", "google"),
    ("gemini", "google"),
    ("glm", "zhipu"),
    ("z-ai", "zhipu"),
    ("kimi", "moonshot"),
    ("moonshot", "moonshot"),
];

/// The maker of a model, from its name: the module's one name-to-maker rule,
/// ported from `bench/token-forecast/extract.py::maker` so production and the
/// bench group issues the same way. A bare maker name maps to itself.
pub fn maker(model: Option<&str>) -> Option<String> {
    static OPENAI_LEAF: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"^(gpt|codex|o\d)\b").expect("valid regex"));
    static ANTHROPIC_LEAF: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"opus|sonnet|haiku|fable|mythos|claude").expect("valid regex")
    });
    let lowered = model.unwrap_or("").trim().to_lowercase();
    let m = lowered.trim_start_matches('~');
    if m.is_empty() {
        return None;
    }
    if m == ANTHROPIC || m == OPENAI {
        return Some(m.to_string());
    }
    let leaf = m.rsplit('/').next().unwrap_or(m);
    if OPENAI_LEAF.is_match(leaf) || leaf.starts_with("gpt-") {
        return Some(OPENAI.to_string());
    }
    if ANTHROPIC_LEAF.is_match(leaf) {
        return Some(ANTHROPIC.to_string());
    }
    let vendor = m.split('/').next().unwrap_or(m);
    for name in [leaf, vendor] {
        for (prefix, family) in FAMILY_PREFIXES {
            if name.starts_with(prefix) {
                return Some((*family).to_string());
            }
        }
    }
    if m.starts_with("custom:") {
        return Some("custom".to_string());
    }
    Some(if m.contains('/') {
        vendor.to_string()
    } else {
        leaf.split(['-', '.', ':'])
            .next()
            .unwrap_or(leaf)
            .to_string()
    })
}

/// The filing-time maker of a request: its stated maker, else its model's.
pub fn filing_maker(request: &ForecastRequest) -> Option<String> {
    maker(request.filing_maker.as_deref()).or_else(|| maker(request.filing_model.as_deref()))
}

/// The applicability verdict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Applicability {
    /// The issue is in scope; its filing maker.
    Applies {
        maker: String,
    },
    NotApplicable(NotApplicable),
}

pub fn check(request: &ForecastRequest) -> Applicability {
    let refuse =
        |reason, detail: String| Applicability::NotApplicable(NotApplicable { reason, detail });
    let Some(maker) = filing_maker(request) else {
        return refuse(
            NotApplicableReason::MakerUnknown,
            "no filing maker or model was given".to_string(),
        );
    };
    if maker != ANTHROPIC {
        return refuse(
            NotApplicableReason::MakerNotAnthropic,
            format!("filed with a {maker} model; the forecaster is validated on Anthropic-filed issues only"),
        );
    }
    if request
        .first_step_role
        .as_deref()
        .is_some_and(|role| role.trim().eq_ignore_ascii_case(COORDINATOR))
    {
        return refuse(
            NotApplicableReason::CoordinatorRole,
            "a coordinator's own tokens are not predictable from its text".to_string(),
        );
    }
    if request.title.trim().is_empty() && request.description.trim().is_empty() {
        return refuse(
            NotApplicableReason::EmptyText,
            "the issue has no title or description".to_string(),
        );
    }
    Applicability::Applies { maker }
}
