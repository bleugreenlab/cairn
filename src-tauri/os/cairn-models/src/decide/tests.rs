use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::Duration;

use cairn_domain::decision::{
    DecisionAnswer, DecisionOption, DecisionQuestion, MeasuredBy, QuestionKind, ScoreLevel,
};
use serde_json::{json, Value};

use super::decide;
use crate::interface::complete::{
    Complete, CompletionCall, CompletionError, CompletionReply, CompletionWire, Delivery,
};
use crate::interface::decide::{DecideError, DecisionCall, DecisionRoute};
use crate::wire::decisions::http::DecisionsEndpoint;
use crate::wire::decisions::wire::DecisionsResponse;
use crate::wire::openai_compat::http::Endpoint;
use crate::wire::openai_compat::wire::ChatResponse;

/// A `Complete` that answers from a script and records every request.
#[derive(Default)]
struct Scripted {
    replies: Mutex<VecDeque<CompletionReply>>,
    requests: Mutex<Vec<(&'static str, Value)>>,
}

impl Scripted {
    fn answering(replies: Vec<CompletionReply>) -> Self {
        Self {
            replies: Mutex::new(replies.into()),
            ..Self::default()
        }
    }

    fn requests(&self) -> Vec<(&'static str, Value)> {
        self.requests.lock().unwrap().clone()
    }
}

impl Complete for Scripted {
    fn complete(&self, call: CompletionCall<'_>) -> Result<CompletionReply, CompletionError> {
        assert!(
            matches!(call.delivery, Delivery::Whole { .. }),
            "a decision is never streamed"
        );
        self.requests
            .lock()
            .unwrap()
            .push((call.wire.family(), call.body));
        Ok(self
            .replies
            .lock()
            .unwrap()
            .pop_front()
            .expect("an unscripted request"))
    }
}

fn native_route() -> DecisionRoute {
    DecisionRoute::Native(DecisionsEndpoint {
        provider_name: "Gateway".into(),
        backend_key: "custom:acc_gateway".into(),
        url: "http://gateway/v1/decisions".into(),
        headers: vec![],
    })
}

fn logprobs_route() -> DecisionRoute {
    DecisionRoute::Logprobs(Endpoint {
        provider_name: "Sparks".into(),
        backend_key: "custom:acc_sparks".into(),
        chat_url: "http://sparks/v1/chat/completions".into(),
        headers: vec![],
    })
}

fn choice(name: &str, values: &[&str]) -> DecisionQuestion {
    DecisionQuestion {
        name: name.into(),
        instructions: format!("{name}?"),
        kind: QuestionKind::Choice {
            options: values
                .iter()
                .map(|value| DecisionOption {
                    value: value.to_string(),
                    description: None,
                })
                .collect(),
        },
    }
}

fn predicate(name: &str) -> DecisionQuestion {
    DecisionQuestion {
        name: name.into(),
        instructions: format!("{name}?"),
        kind: QuestionKind::Predicate,
    }
}

fn score(name: &str, labels: &[&str]) -> DecisionQuestion {
    DecisionQuestion {
        name: name.into(),
        instructions: format!("{name}?"),
        kind: QuestionKind::Score {
            levels: labels
                .iter()
                .map(|label| ScoreLevel {
                    label: label.to_string(),
                    description: None,
                })
                .collect(),
        },
    }
}

fn call<'a>(route: &'a DecisionRoute, questions: &'a [DecisionQuestion]) -> DecisionCall<'a> {
    DecisionCall {
        route,
        model: "m",
        input: "The package arrived broken.",
        questions,
        timeout: Duration::from_secs(5),
    }
}

fn decisions_reply(value: Value) -> CompletionReply {
    CompletionReply::Decisions(serde_json::from_value::<DecisionsResponse>(value).unwrap())
}

fn chat_reply(text: &str) -> CompletionReply {
    CompletionReply::Chat(serde_json::from_str::<ChatResponse>(text).unwrap())
}

#[test]
fn native_answers_keep_their_probabilities_and_say_how_they_were_measured() {
    let complete = Scripted::answering(vec![decisions_reply(json!({
        "model": "openai/gpt-6-luna-decisions",
        "answers": [
            {"type": "predicate", "name": "damaged", "probability": 0.95},
            {"type": "choice", "name": "queue", "choice": "shipping", "confidence": 0.62,
             "probabilities": [{"value": "billing", "probability": 0.19},
                               {"value": "shipping", "probability": 0.81}]},
            {"type": "score", "name": "urgency", "score": 1.3, "confidence": 0.31,
             "probabilities": [{"value": 0, "label": "low", "probability": 0.08},
                               {"value": 1, "label": "medium", "probability": 0.54},
                               {"value": 2, "label": "high", "probability": 0.38}]}
        ],
        "usage": {"input_tokens": 96, "output_tokens": 0, "total_tokens": 96}
    }))]);
    let route = native_route();
    let questions = [
        predicate("damaged"),
        choice("queue", &["billing", "shipping"]),
        score("urgency", &["low", "medium", "high"]),
    ];
    let decision = decide(&complete, call(&route, &questions)).unwrap();

    assert_eq!(decision.provider, "custom:acc_gateway");
    assert_eq!(decision.model, "openai/gpt-6-luna-decisions");
    assert_eq!(decision.input_tokens, Some(96));
    assert!(decision
        .answers
        .iter()
        .all(|answer| answer.measured_by() == MeasuredBy::Native));
    assert_eq!(decision.answers[0].pick(), Some("yes"));
    assert_eq!(decision.answers[1].pick(), Some("shipping"));
    // A reported confidence is the provider's, kept as sent.
    assert_eq!(decision.answers[1].confidence(), Some(0.62));
    let DecisionAnswer::Score { score, pick, .. } = &decision.answers[2] else {
        panic!("expected a score");
    };
    assert_eq!((*score, pick.as_str()), (1.3, "medium"));
    assert_eq!(decision.answers[2].confidence(), Some(0.31));

    let requests = complete.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].0, "decisions");
    assert_eq!(
        requests[0].1["questions"][1]["choices"][0]["value"],
        "billing"
    );
    assert_eq!(requests[0].1["questions"][2]["levels"][2]["label"], "high");
}

/// The decoder pin: a refused question is a refusal in its position, never a
/// pick and never a probability of 0, while its siblings answer normally.
#[test]
fn a_native_refusal_stays_a_refusal_beside_normal_answers() {
    let complete = Scripted::answering(vec![decisions_reply(json!({
        "model": "typesafe-ai/jev",
        "answers": [
            {"type": "refusal", "name": "damaged"},
            {"type": "choice", "name": "queue", "choice": "billing"}
        ],
        "usage": {"input_tokens": 40, "output_tokens": 0}
    }))]);
    let route = native_route();
    let questions = [
        predicate("damaged"),
        choice("queue", &["billing", "shipping"]),
    ];
    let decision = decide(&complete, call(&route, &questions)).unwrap();
    assert_eq!(
        decision.answers[0],
        DecisionAnswer::Refusal {
            name: "damaged".into(),
            measured_by: MeasuredBy::Native,
            reason: None,
        }
    );
    assert_eq!(decision.answers[0].pick(), None);
    assert_eq!(decision.answers[0].confidence(), None);
    // Confidence is optional on the wire and stays absent when not reported.
    assert_eq!(decision.answers[1].pick(), Some("billing"));
    assert_eq!(decision.answers[1].confidence(), None);
}

#[test]
fn a_native_pick_outside_the_options_is_an_invalid_reply() {
    let complete = Scripted::answering(vec![decisions_reply(json!({
        "answers": [{"type": "choice", "name": "queue", "choice": "legal"}]
    }))]);
    let route = native_route();
    let questions = [choice("queue", &["billing", "shipping"])];
    let error = decide(&complete, call(&route, &questions)).unwrap_err();
    assert!(
        error.to_string().contains("not one of its options"),
        "{error}"
    );
}

#[test]
fn a_native_answer_of_the_wrong_kind_or_count_is_an_invalid_reply() {
    let route = native_route();
    let questions = [choice("queue", &["billing", "shipping"])];
    let complete = Scripted::answering(vec![decisions_reply(json!({
        "answers": [{"type": "predicate", "name": "queue", "probability": 0.4}]
    }))]);
    let error = decide(&complete, call(&route, &questions)).unwrap_err();
    assert!(
        error.to_string().contains("is a choice question"),
        "{error}"
    );

    let complete = Scripted::answering(vec![decisions_reply(json!({"answers": []}))]);
    let error = decide(&complete, call(&route, &questions)).unwrap_err();
    assert!(
        error.to_string().contains("asked 1 questions and got 0"),
        "{error}"
    );
}

const VLLM_CHOICE: &str = include_str!("testdata/vllm_logprobs_choice.json");

#[test]
fn logprobs_answers_renormalize_the_label_mass_of_a_vllm_reply() {
    let complete = Scripted::answering(vec![chat_reply(VLLM_CHOICE)]);
    let route = logprobs_route();
    let questions = [choice("queue", &["billing", "shipping"])];
    let decision = decide(&complete, call(&route, &questions)).unwrap();

    assert_eq!(decision.provider, "custom:acc_sparks");
    assert_eq!(decision.model, "gemma4-26b");
    assert_eq!(decision.input_tokens, Some(87));
    let DecisionAnswer::Choice {
        measured_by,
        pick,
        confidence,
        probabilities,
        ..
    } = &decision.answers[0]
    else {
        panic!("expected a choice: {:?}", decision.answers[0]);
    };
    assert_eq!(*measured_by, MeasuredBy::Logprobs);
    assert_eq!(pick, "shipping");
    // `B` and ` B` both spell the second label; `a` spells the first. The
    // non-label tokens (`**`, `The`) carry no mass.
    let b = (-0.06122_f64).exp() + (-7.1083_f64).exp();
    let a = (-2.8541_f64).exp() + (-10.002_f64).exp();
    let shipping = b / (a + b);
    assert!((probabilities[1].probability - shipping).abs() < 1e-12);
    // Two options: confidence is |2p - 1|, not the pick's probability.
    assert!((confidence.unwrap() - (2.0 * shipping - 1.0)).abs() < 1e-12);
    assert!((probabilities[0].probability + probabilities[1].probability - 1.0).abs() < 1e-12);
    assert_eq!(probabilities[0].value, "billing");

    let requests = complete.requests();
    assert_eq!(requests[0].0, "chat");
    let body = &requests[0].1;
    assert_eq!(body["logprobs"], true);
    assert_eq!(body["top_logprobs"], 20);
    assert_eq!(body["max_tokens"], 1);
    assert_eq!(body["stream"], false);
    let prompt = body["messages"][1]["content"].as_str().unwrap();
    assert!(prompt.contains("A. billing\nB. shipping"), "{prompt}");
}

fn vllm_reply_with_top(tokens: &[(&str, f64)]) -> CompletionReply {
    let top: Vec<Value> = tokens
        .iter()
        .map(|(token, logprob)| json!({"token": token, "logprob": logprob}))
        .collect();
    chat_reply(
        &json!({
            "model": "qwen3",
            "choices": [{
                "message": {"role": "assistant", "content": tokens[0].0},
                "logprobs": {"content": [{"token": tokens[0].0, "logprob": tokens[0].1,
                                            "top_logprobs": top}]}
            }],
            "usage": {"prompt_tokens": 10, "completion_tokens": 1}
        })
        .to_string(),
    )
}

#[test]
fn logprobs_measure_predicates_and_scores_one_request_per_question() {
    let complete = Scripted::answering(vec![
        vllm_reply_with_top(&[("Yes", -0.2), (" yes", -3.0), ("No", -2.0)]),
        vllm_reply_with_top(&[("C", -0.1), ("B", -2.5), ("Hmm", -4.0)]),
    ]);
    let route = logprobs_route();
    let questions = [
        predicate("urgent"),
        score("severity", &["low", "mid", "high"]),
    ];
    let decision = decide(&complete, call(&route, &questions)).unwrap();
    assert_eq!(complete.requests().len(), 2);
    assert_eq!(decision.input_tokens, Some(20));

    let DecisionAnswer::Predicate {
        probability_yes,
        measured_by,
        ..
    } = &decision.answers[0]
    else {
        panic!("expected a predicate");
    };
    let yes = (-0.2_f64).exp() + (-3.0_f64).exp();
    let no = (-2.0_f64).exp();
    assert!((probability_yes - yes / (yes + no)).abs() < 1e-12);
    assert_eq!(*measured_by, MeasuredBy::Logprobs);

    let DecisionAnswer::Score { score, pick, .. } = &decision.answers[1] else {
        panic!("expected a score");
    };
    assert_eq!(pick, "high");
    let c = (-0.1_f64).exp();
    let b = (-2.5_f64).exp();
    assert!((score - (b + 2.0 * c) / (b + c)).abs() < 1e-12);
}

/// The confidence each route reports for `probabilities` over `question`: the
/// native route with the provider's `confidence` omitted, and the logprobs
/// route with one label per nonzero probability. An option with probability 0
/// is left out of both replies, as a provider or a server may do.
fn confidence_by_route(question: &DecisionQuestion, probabilities: &[f64]) -> (f64, f64) {
    let values = question.answer_values();
    let native_answer = match &question.kind {
        QuestionKind::Choice { .. } => {
            let pick = probabilities
                .iter()
                .enumerate()
                .max_by(|a, b| a.1.total_cmp(b.1))
                .map(|(index, _)| values[index])
                .unwrap();
            json!({"type": "choice", "name": question.name, "choice": pick,
                   "probabilities": values.iter().zip(probabilities)
                       .filter(|(_, p)| **p > 0.0)
                       .map(|(value, p)| json!({"value": value, "probability": p}))
                       .collect::<Vec<_>>()})
        }
        QuestionKind::Score { .. } => {
            json!({"type": "score", "name": question.name,
                   "score": probabilities.iter().enumerate().map(|(i, p)| i as f64 * p).sum::<f64>(),
                   "probabilities": probabilities.iter().enumerate()
                       .filter(|(_, p)| **p > 0.0)
                       .map(|(i, p)| json!({"value": i, "probability": p}))
                       .collect::<Vec<_>>()})
        }
        QuestionKind::Predicate => unreachable!("a predicate carries no confidence field"),
    };
    let complete = Scripted::answering(vec![decisions_reply(json!({"answers": [native_answer]}))]);
    let route = native_route();
    let native = decide(&complete, call(&route, std::slice::from_ref(question))).unwrap();

    // The generated token must be the top label, so it leads the list.
    let mut labelled: Vec<(String, f64)> = probabilities
        .iter()
        .enumerate()
        .filter(|(_, p)| **p > 0.0)
        .map(|(index, p)| (char::from(b'A' + index as u8).to_string(), p.ln()))
        .collect();
    labelled.sort_by(|a, b| b.1.total_cmp(&a.1));
    let top: Vec<(&str, f64)> = labelled.iter().map(|(t, l)| (t.as_str(), *l)).collect();
    let complete = Scripted::answering(vec![vllm_reply_with_top(&top)]);
    let route = logprobs_route();
    let logprobs = decide(&complete, call(&route, std::slice::from_ref(question))).unwrap();

    (
        native.answers[0].confidence().expect("native fallback"),
        logprobs.answers[0].confidence().expect("logprobs"),
    )
}

/// Both routes compute confidence by the one shared formula, over the
/// question's declared options and levels, and land on the providers' own
/// published values (OpenAI's Decisions guide: 0.93 and 0.55).
#[test]
fn both_routes_compute_the_providers_confidence_from_identical_probabilities() {
    let department = choice("department", &["billing", "technical", "shipping", "other"]);
    let severity = score("severity", &["cosmetic", "workaround", "blocked"]);
    for (question, probabilities, published) in [
        (&department, vec![0.95, 0.02, 0.01, 0.02], Some(0.93)),
        (&severity, vec![0.1, 0.7, 0.2], Some(0.55)),
        // Two of four options returned: the even split is still a quarter,
        // so confidence is (0.6 - 0.25) / 0.75, not the two-option 0.2.
        (&department, vec![0.6, 0.4, 0.0, 0.0], None),
        // A level the reply leaves out is 0, still a level away.
        (&severity, vec![0.5, 0.0, 0.5], None),
        // A tied peak: both routes pick the first, so both report 0, where
        // picking the second would report 0.1.
        (&severity, vec![0.4, 0.4, 0.2], None),
    ] {
        let (native, logprobs) = confidence_by_route(question, &probabilities);
        assert!(
            (native - logprobs).abs() < 1e-9,
            "{probabilities:?}: native {native}, logprobs {logprobs}"
        );
        if let Some(published) = published {
            assert_eq!(
                (native * 100.0).round() / 100.0,
                published,
                "{probabilities:?}"
            );
        }
    }
    let (native, _) = confidence_by_route(&department, &[0.6, 0.4, 0.0, 0.0]);
    assert!((native - 0.35 / 0.75).abs() < 1e-9, "{native}");
    let (native, _) = confidence_by_route(&severity, &[0.5, 0.0, 0.5]);
    assert!(native.abs() < 1e-9, "{native}");
    let (native, _) = confidence_by_route(&severity, &[0.4, 0.4, 0.2]);
    assert!(native.abs() < 1e-9, "{native}");
}

/// A native reply may list levels in any order; the pick and its confidence
/// come from the declared order, so a tie still breaks to the first level.
#[test]
fn a_native_score_breaks_a_tie_by_declared_level_whatever_the_wire_order() {
    let complete = Scripted::answering(vec![decisions_reply(json!({"answers": [
        {"type": "score", "name": "severity", "score": 0.8,
         "probabilities": [{"value": 2, "probability": 0.2},
                           {"value": 1, "probability": 0.4},
                           {"value": 0, "probability": 0.4}]}
    ]}))]);
    let route = native_route();
    let questions = [score("severity", &["low", "mid", "high"])];
    let decision = decide(&complete, call(&route, &questions)).unwrap();
    assert_eq!(decision.answers[0].pick(), Some("low"));
    assert!(decision.answers[0].confidence().unwrap().abs() < 1e-9);
}

/// Never fabricate: a reply without logprobs means the offer cannot decide.
#[test]
fn a_reply_without_logprobs_refuses_instead_of_answering() {
    for reply in [
        json!({"model": "m", "choices": [{"message": {"role": "assistant", "content": "A"}}]}),
        json!({"model": "m", "choices": [{"message": {"role": "assistant", "content": "A"},
                                          "logprobs": null}]}),
        // The chosen token alone is not a distribution.
        json!({"model": "m", "choices": [{"message": {"role": "assistant", "content": "A"},
               "logprobs": {"content": [{"token": "A", "logprob": -0.1, "top_logprobs": []}]}}]}),
    ] {
        let complete = Scripted::answering(vec![chat_reply(&reply.to_string())]);
        let route = logprobs_route();
        let questions = [choice("queue", &["billing", "shipping"])];
        let error = decide(&complete, call(&route, &questions)).unwrap_err();
        assert_eq!(
            error,
            DecideError::NoLogprobs {
                provider: "Sparks".into(),
                model: "m".into()
            }
        );
        assert!(
            error.to_string().contains("Settings → Providers"),
            "{error}"
        );
    }
}

/// A thinking model on Ollama generates the start of its reasoning, and label
/// `A` sits far down its alternatives (logprob -16) with `B` absent.
/// Renormalizing that tail once reported `yes` at confidence 1.0 for an issue
/// whose answer is plainly `no`; the reply is refused instead.
const OLLAMA_REASONING_FIRST: &str = include_str!("testdata/ollama_reasoning_first.json");

#[test]
fn a_reasoning_token_is_not_an_answer_even_with_a_label_in_its_tail() {
    let complete = Scripted::answering(vec![chat_reply(OLLAMA_REASONING_FIRST)]);
    let route = logprobs_route();
    let questions = [choice("route", &["yes", "no"])];
    let error = decide(&complete, call(&route, &questions)).unwrap_err();
    assert_eq!(
        error,
        DecideError::NotAnswering {
            provider: "Sparks".into(),
            model: "m".into(),
            began: "The".into(),
        }
    );
    assert!(
        error.to_string().contains("reasoning_effort none"),
        "{error}"
    );
    let body = &complete.requests()[0].1;
    assert_eq!(body["reasoning_effort"], "none");
    assert_eq!(body["chat_template_kwargs"]["enable_thinking"], false);
}

#[test]
fn a_generated_token_that_is_no_label_is_not_an_answer() {
    // gpt-oss on Ollama always opens a harmony channel first.
    let complete = Scripted::answering(vec![vllm_reply_with_top(&[
        ("<|channel|>", -0.0001),
        ("B", -9.0),
        ("A", -12.0),
    ])]);
    let route = logprobs_route();
    let questions = [choice("queue", &["billing", "shipping"])];
    let error = decide(&complete, call(&route, &questions)).unwrap_err();
    assert!(
        matches!(&error, DecideError::NotAnswering { began, .. } if began == "<|channel|>"),
        "{error}"
    );
}

#[test]
fn more_options_than_logprobs_can_see_is_unmeasurable() {
    let values: Vec<String> = (0..21).map(|index| format!("o{index}")).collect();
    let values: Vec<&str> = values.iter().map(String::as_str).collect();
    let route = logprobs_route();
    let questions = [choice("wide", &values)];
    let complete = Scripted::default();
    let error = decide(&complete, call(&route, &questions)).unwrap_err();
    assert!(
        matches!(&error, DecideError::Unmeasurable(text) if text.contains("at most 20")),
        "{error}"
    );
    assert!(complete.requests().is_empty());
}

#[test]
fn malformed_questions_are_refused_before_any_request() {
    let route = native_route();
    let questions = [predicate("same"), predicate("same")];
    let complete = Scripted::default();
    let error = decide(&complete, call(&route, &questions)).unwrap_err();
    assert!(
        matches!(&error, DecideError::InvalidQuestions(problems) if problems[0].contains("used twice")),
        "{error}"
    );
    assert!(complete.requests().is_empty());
}

#[test]
fn the_routes_name_their_method_and_family() {
    use cairn_models_types::catalog::Wire;
    assert_eq!(native_route().measured_by(), MeasuredBy::Native);
    assert_eq!(logprobs_route().measured_by(), MeasuredBy::Logprobs);
    assert_eq!(
        native_route().completion_wire().wire(),
        Wire::OpenAiDecisions
    );
    assert_eq!(
        logprobs_route().completion_wire().wire(),
        Wire::OpenAiChatCompletions
    );
    let route = native_route();
    let DecisionRoute::Native(endpoint) = &route else {
        unreachable!()
    };
    assert_eq!(CompletionWire::Decisions(endpoint).family(), "decisions");
}
