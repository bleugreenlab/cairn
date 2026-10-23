//! The System One reply body, as TypeSafe and OpenRouter spell it.

use std::collections::BTreeMap;

use serde::{Deserialize, Deserializer};
use serde_json::Value;

/// The reply: one answer per question, keyed by the name the request gave it.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct SystemOneResponse {
    /// The model that answered, which may differ from the alias asked for
    /// (`jev-latest` answers as `jev-1.13.0`).
    #[serde(default)]
    pub model: Option<String>,
    pub answers: BTreeMap<String, SystemOneAnswer>,
    #[serde(default)]
    pub usage: Option<SystemOneUsage>,
}

/// System One bills input only. TypeSafe reports `output_tokens` but does
/// not charge them; OpenRouter adds what it charged as `cost`, in USD.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct SystemOneUsage {
    #[serde(default)]
    pub input_tokens: Option<i64>,
    #[serde(default)]
    pub output_tokens: Option<i64>,
    #[serde(default)]
    pub cost: Option<f64>,
}

/// One answer as the provider reports it.
#[derive(Debug, Clone, PartialEq)]
pub enum SystemOneAnswer {
    /// The probability that the answer is yes.
    Noul { noul: f64 },
    Choice {
        choice: String,
        confidence: Option<f64>,
        /// Every option's probability, keyed by option value.
        probabilities: BTreeMap<String, f64>,
    },
    Score {
        /// The probability-weighted mean of the level indexes, from 0.
        score: f64,
        confidence: Option<f64>,
        /// Every level's probability, keyed by its index as a string.
        probabilities: BTreeMap<String, f64>,
        /// Each level index mapped back to the criterion it was sent as.
        legend: BTreeMap<String, Value>,
    },
    /// Anything else: an unknown `type` (a refusal, a `box`), or a known one
    /// missing a field. Kept verbatim so it can be reported, and never read as
    /// an answer.
    Unrecognised(Value),
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum Recognised {
    Noul {
        noul: f64,
    },
    Choice {
        choice: String,
        #[serde(default)]
        confidence: Option<f64>,
        #[serde(default)]
        probabilities: BTreeMap<String, f64>,
    },
    Score {
        score: f64,
        #[serde(default)]
        confidence: Option<f64>,
        #[serde(default)]
        probabilities: BTreeMap<String, f64>,
        #[serde(default)]
        legend: BTreeMap<String, Value>,
    },
}

impl<'de> Deserialize<'de> for SystemOneAnswer {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        Ok(match Recognised::deserialize(&value) {
            Ok(Recognised::Noul { noul }) => Self::Noul { noul },
            Ok(Recognised::Choice {
                choice,
                confidence,
                probabilities,
            }) => Self::Choice {
                choice,
                confidence,
                probabilities,
            },
            Ok(Recognised::Score {
                score,
                confidence,
                probabilities,
                legend,
            }) => Self::Score {
                score,
                confidence,
                probabilities,
                legend,
            },
            Err(_) => Self::Unrecognised(value),
        })
    }
}
