//! The Decisions request and reply bodies, as the provider spells them.

use serde::{Deserialize, Serialize};

/// One question in a request.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum WireQuestion {
    Predicate {
        name: String,
        instructions: String,
    },
    Choice {
        name: String,
        instructions: String,
        choices: Vec<WireChoice>,
    },
    Score {
        name: String,
        instructions: String,
        levels: Vec<WireLevel>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct WireChoice {
    pub value: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct WireLevel {
    pub label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// The reply: one answer per question, in question order.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct DecisionsResponse {
    #[serde(default)]
    pub model: Option<String>,
    pub answers: Vec<WireAnswer>,
    #[serde(default)]
    pub usage: Option<DecisionsUsage>,
}

/// Decisions bill input only; `output_tokens` is reported as 0.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct DecisionsUsage {
    #[serde(default)]
    pub input_tokens: Option<i64>,
    #[serde(default)]
    pub output_tokens: Option<i64>,
    #[serde(default)]
    pub total_tokens: Option<i64>,
}

/// One answer as the provider reports it.
///
/// `Refusal` is a variant, not a missing probability: a declined question
/// arrives as `{"type": "refusal", "name": ...}` in its position, and a
/// decoder that defaulted it to probability 0 would turn "won't say" into
/// "no".
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum WireAnswer {
    Predicate {
        #[serde(default)]
        name: Option<String>,
        /// The probability that the answer is yes.
        probability: f64,
    },
    Choice {
        #[serde(default)]
        name: Option<String>,
        choice: WireValue,
        #[serde(default)]
        confidence: Option<f64>,
        #[serde(default)]
        probabilities: Vec<WireChoiceProbability>,
    },
    Score {
        #[serde(default)]
        name: Option<String>,
        /// The probability-weighted mean of the level indexes, from 0.
        score: f64,
        #[serde(default)]
        confidence: Option<f64>,
        #[serde(default)]
        probabilities: Vec<WireLevelProbability>,
    },
    Refusal {
        #[serde(default)]
        name: Option<String>,
    },
}

/// A choice value: the wire allows strings and booleans.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum WireValue {
    Text(String),
    Flag(bool),
}

impl WireValue {
    pub fn as_text(&self) -> String {
        match self {
            Self::Text(text) => text.clone(),
            Self::Flag(flag) => flag.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct WireChoiceProbability {
    pub value: WireValue,
    pub probability: f64,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct WireLevelProbability {
    /// The level's index, from 0.
    pub value: i64,
    #[serde(default)]
    pub label: Option<String>,
    pub probability: f64,
}
