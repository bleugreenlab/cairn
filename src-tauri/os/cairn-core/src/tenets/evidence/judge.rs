//! The judgement: does one concluded piece of work support, contradict, or not
//! bear on each of a few candidate tenets?

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use serde_json::{json, Map, Value};

use crate::responses::{
    OutputContract, ResponseDefinition, ResponseExample, ResponseOutcome, ResponseVariable,
};
use crate::tenets::{TenetOrigin, TenetView};

pub const RESPONSE_ID: &str = "builtin:tenet-evidence";
pub const CALLER_LABEL: &str = "tenet evidence";

const TEMPLATE: &str = "You check whether one piece of concluded work bears on standing project decisions called tenets. A tenet is a decision the operator approved: the chosen option is the tenet, and the other options were explicitly rejected.

Judge each tenet independently against the concluded work:
- supports: the work followed the chosen option or relied on it.
- contradicts: the work did what the tenet rules out. It adopted a rejected alternative, reversed or bypassed the decision, or concluded the decision was wrong.
- neither: the work does not bear on the tenet, or only shares its topic or vocabulary. When unsure, answer neither.

probability is how sure you are that your verdict is right, from 0 to 1. Return exactly one entry for every tenet number listed.

## Concluded work
{{event}}

## Tenets
{{tenets}}";

/// The output schema. Its root is an object: strict providers refuse an array
/// root.
pub fn schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["verdicts"],
        "properties": {
            "verdicts": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["tenet", "verdict", "probability"],
                    "properties": {
                        "tenet": {"type": "integer"},
                        "verdict": {"type": "string", "enum": ["supports", "contradicts", "neither"]},
                        "probability": {"type": "number", "minimum": 0, "maximum": 1}
                    }
                }
            }
        }
    })
}

fn variable(name: &str) -> ResponseVariable {
    ResponseVariable {
        name: name.into(),
        description: None,
        required: true,
        default: None,
        values: vec![],
    }
}

fn example() -> ResponseExample {
    let mut input = Map::new();
    input.insert(
        "event".into(),
        json!("Issue cairn://p/demo/41 \u{2014} Upload executor build artifacts\nResolution: merged\n\nExecutors now push finished build artifacts straight to the S3 bucket with a presigned URL, bypassing the artifact store, because the store's chunked upload was slow.")
    );
    input.insert(
        "tenets".into(),
        json!("### Tenet 1\nQuestion: How do executors publish build artifacts?\nChosen: Executor artifacts publish through the content store\nRejected: Direct S3 upload from the executor; Ship artifacts inside the result message\n\n### Tenet 2\nQuestion: Where do build caches live?\nChosen: Build caches stay on the executor's local disk\nRejected: none recorded")
    );
    ResponseExample {
        input,
        output: json!({"verdicts": [
            {"tenet": 1, "verdict": "contradicts", "probability": 0.9},
            {"tenet": 2, "verdict": "neither", "probability": 0.85}
        ]}),
    }
}

pub fn definition() -> ResponseDefinition {
    ResponseDefinition {
        name: "Tenet evidence".into(),
        description: "Judge whether concluded work supports or contradicts standing tenets".into(),
        tier: Some("sm".into()),
        model: None,
        backend: None,
        options: HashMap::new(),
        variables: vec![variable("event"), variable("tenets")],
        output: OutputContract::Schema(schema()),
        timeout: Duration::from_secs(60),
        examples: vec![example()],
        template: TEMPLATE.into(),
    }
}

/// One tenet as the judge reads it, numbered from 1 in candidate order.
pub fn tenet_block(number: usize, view: &TenetView) -> String {
    let text = view.text.as_deref().unwrap_or(&view.row.question);
    let mut block = format!("### Tenet {number}\n");
    // A migrated ruling has no question distinct from its text.
    let distinct_question =
        view.row.origin != TenetOrigin::MigratedRuling && view.row.question.trim() != text.trim();
    if distinct_question {
        block.push_str(&format!("Question: {}\n", view.row.question));
    }
    block.push_str(&format!("Chosen: {text}\n"));
    if view.rejected.is_empty() {
        block.push_str("Rejected: none recorded");
    } else {
        block.push_str(&format!("Rejected: {}", view.rejected.join("; ")));
    }
    block
}

/// The judge's arguments for one event against its candidates.
pub fn args(event_text: &str, candidates: &[&TenetView]) -> Value {
    let tenets = candidates
        .iter()
        .enumerate()
        .map(|(index, view)| tenet_block(index + 1, view))
        .collect::<Vec<_>>()
        .join("\n\n");
    json!({"event": event_text, "tenets": tenets})
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sign {
    Supports,
    Contradicts,
    Neither,
}

impl Sign {
    pub fn as_str(self) -> &'static str {
        match self {
            Sign::Supports => "supports",
            Sign::Contradicts => "contradicts",
            Sign::Neither => "neither",
        }
    }
}

/// One verdict about one candidate, by its 0-based candidate index.
#[derive(Debug, Clone, PartialEq)]
pub struct Verdict {
    pub candidate: usize,
    pub sign: Sign,
    pub probability: f64,
}

/// The verdicts in a judge's output, one per candidate. An entry naming a
/// number that is not a candidate is dropped, the first entry for a candidate
/// wins, and a candidate with no entry reads `neither`.
pub fn parse(outcome: &ResponseOutcome, candidates: usize) -> Vec<Verdict> {
    let parsed = outcome
        .parsed
        .clone()
        .or_else(|| serde_json::from_str(&outcome.text).ok())
        .unwrap_or(Value::Null);
    let entries = parsed
        .get("verdicts")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut seen = HashSet::new();
    let mut verdicts: Vec<Option<Verdict>> = vec![None; candidates];
    for entry in entries {
        let Some(number) = entry.get("tenet").and_then(Value::as_u64) else {
            continue;
        };
        let Some(candidate) = (number as usize).checked_sub(1).filter(|i| *i < candidates) else {
            continue;
        };
        if !seen.insert(candidate) {
            continue;
        }
        let sign = match entry.get("verdict").and_then(Value::as_str) {
            Some("supports") => Sign::Supports,
            Some("contradicts") => Sign::Contradicts,
            _ => Sign::Neither,
        };
        let probability = entry
            .get("probability")
            .and_then(Value::as_f64)
            .unwrap_or(0.0)
            .clamp(0.0, 1.0);
        verdicts[candidate] = Some(Verdict {
            candidate,
            sign,
            probability,
        });
    }
    verdicts
        .into_iter()
        .enumerate()
        .map(|(candidate, verdict)| {
            verdict.unwrap_or(Verdict {
                candidate,
                sign: Sign::Neither,
                probability: 0.0,
            })
        })
        .collect()
}
