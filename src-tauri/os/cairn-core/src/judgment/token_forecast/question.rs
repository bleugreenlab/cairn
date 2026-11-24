//! The questions the decide-score answerer asks, exactly as cairn/6271
//! measured them.
//!
//! `size.v1.json` is a copy of `bench/token-forecast/questions/size.v1.json`.
//! Its version is `label_decide.ts`'s `QUESTION_VERSION`: the first 12 hex of
//! sha256 over `JSON.stringify({preamble, questions})`, each question reduced
//! to `{name, type, instructions, levels}`. A calibration is valid only for
//! the version it was fit on, so changing a word here changes the version and
//! leaves production with no active calibration until a refit, never with a
//! mis-scaled one.

use std::sync::LazyLock;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::models::decision::{DecisionQuestion, OptionProbability, QuestionKind, ScoreLevel};
use crate::security::prose_scrub::{scrub_prose, ScrubRefused};

const SPEC_JSON: &str = include_str!("size.v1.json");

/// About 6k tokens of description: enough for any filing, bounded for decide.
pub const DESCRIPTION_CHAR_CAP: usize = 24_000;

#[derive(Debug, Deserialize)]
struct Spec {
    preamble: String,
    questions: Vec<SpecQuestion>,
}

/// Field order is the bench's `{name, type, instructions, levels}`; the
/// version hashes this serialization.
#[derive(Debug, Clone, Deserialize, Serialize)]
struct SpecQuestion {
    name: String,
    #[serde(rename = "type")]
    kind: String,
    instructions: String,
    levels: Vec<SpecLevel>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct SpecLevel {
    label: String,
    description: String,
}

#[derive(Serialize)]
struct VersionView<'a> {
    preamble: &'a str,
    questions: &'a [SpecQuestion],
}

static SPEC: LazyLock<Spec> =
    LazyLock::new(|| serde_json::from_str(SPEC_JSON).expect("size.v1.json is valid"));

static VERSION: LazyLock<String> = LazyLock::new(|| {
    let view = VersionView {
        preamble: &SPEC.preamble,
        questions: &SPEC.questions,
    };
    let text = serde_json::to_string(&view).expect("the question spec serializes");
    sha256_hex(&text)[..12].to_string()
});

pub(crate) fn sha256_hex(text: &str) -> String {
    format!("{:x}", Sha256::digest(text.as_bytes()))
}

/// The version of the questions as asked.
pub fn question_version() -> &'static str {
    &VERSION
}

pub fn preamble() -> &'static str {
    &SPEC.preamble
}

/// The questions as the facet's `decide` takes them.
pub fn decision_questions() -> Vec<DecisionQuestion> {
    SPEC.questions
        .iter()
        .map(
            |SpecQuestion {
                 name,
                 instructions,
                 levels,
                 ..
             }| DecisionQuestion {
                name: name.clone(),
                instructions: instructions.clone(),
                kind: QuestionKind::Score {
                    levels: levels
                        .iter()
                        .map(|level| ScoreLevel {
                            label: level.label.clone(),
                            description: Some(level.description.clone()),
                        })
                        .collect(),
                },
            },
        )
        .collect()
}

pub fn question_names() -> Vec<&'static str> {
    SPEC.questions.iter().map(|q| q.name.as_str()).collect()
}

/// The filing text as `bench/token-forecast/features.py::filing_text`
/// renders it, before scrubbing: the title, then the description capped at
/// [`DESCRIPTION_CHAR_CAP`] characters.
pub fn render_filing_text(title: &str, description: &str) -> String {
    let description = description.trim();
    let description = if description.chars().count() > DESCRIPTION_CHAR_CAP {
        let kept: String = description.chars().take(DESCRIPTION_CHAR_CAP).collect();
        format!("{kept}\n[... truncated]")
    } else if description.is_empty() {
        "(empty)".to_string()
    } else {
        description.to_string()
    };
    format!("Title: {}\n\nDescription:\n{description}", title.trim())
}

/// The filing text, scrubbed of secrets: the only text an answerer sees.
pub fn scrubbed_filing_text(title: &str, description: &str) -> Result<String, ScrubRefused> {
    scrub_prose(&render_filing_text(title, description))
}

/// The decide input: the preamble, a blank line, the filing text.
pub fn render_input(text: &str) -> String {
    format!("{}\n\n{text}", preamble())
}

/// A score answer's expected value: each level's value times its
/// probability, renormalised over the levels answered (`label_decide.ts`
/// `expectedScore`). `None` when no level carried probability.
pub fn expected_score(probabilities: &[OptionProbability]) -> Option<f64> {
    let mut mass = 0.0;
    let mut total = 0.0;
    for p in probabilities {
        let Ok(value) = p.value.trim().parse::<f64>() else {
            continue;
        };
        if !value.is_finite() || !p.probability.is_finite() {
            continue;
        }
        mass += p.probability;
        total += value * p.probability;
    }
    (mass > 0.0).then(|| total / mass)
}
