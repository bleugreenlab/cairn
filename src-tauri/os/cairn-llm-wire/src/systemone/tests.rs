//! The System One request and reply, pinned against the two reference pages:
//! TypeSafe's (https://docs.typesafe.ai/api/system-one, with the Choice and
//! Score primitives at https://docs.typesafe.ai/primitives/choice and
//! /primitives/score) and OpenRouter's
//! (https://openrouter.ai/docs/api/api-reference/alphadecisions/submit-a-decisions-questions-and-answers-request).

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::Duration;

use serde_json::{json, Value};

use super::http::{build_body, post_systemone, read};
use super::wire::*;
use crate::decisions::http::DecisionsEndpoint;
use crate::decisions::wire::{WireChoice, WireLevel, WireQuestion};
use crate::CompletionError;

/// TypeSafe's documented request, verbatim.
#[test]
fn the_request_body_is_typesafes_documented_noul_request() {
    let body = build_body(
        "jev-latest",
        json!(
            "I was charged twice for my subscription this month. Please refund the extra charge."
        ),
        &[WireQuestion::Predicate {
            name: "billing".into(),
            instructions: "Is this message about billing?".into(),
        }],
    );
    assert_eq!(
        body,
        json!({
          "model": "jev-latest",
          "state": "I was charged twice for my subscription this month. Please refund the extra charge.",
          "questions": {
            "billing": {
              "type": "noul",
              "instructions": "Is this message about billing?"
            }
          }
        })
    );
}

/// OpenRouter's documented request, verbatim, except that its `noul` carries
/// `criteria` for true and false: Cairn's predicate has no criteria to send.
#[test]
fn the_request_body_is_openrouters_documented_request() {
    let body = build_body(
        "typesafe/jev-1.13",
        json!({
          "customer_tier": "enterprise",
          "ticket": "My checkout page shows a blank screen after I click Pay. I have tried two browsers."
        }),
        &[
            WireQuestion::Predicate {
                name: "is_bug".into(),
                instructions: "Is the customer reporting a software defect?".into(),
            },
            WireQuestion::Choice {
                name: "team".into(),
                instructions: "Which team should own this ticket?".into(),
                choices: [
                    ("account", "Login, permissions, or profile issues."),
                    (
                        "frontend",
                        "Rendering, layout, or browser compatibility issues.",
                    ),
                    (
                        "payments",
                        "Checkout, billing, or payment processing issues.",
                    ),
                ]
                .into_iter()
                .map(|(value, description)| WireChoice {
                    value: value.into(),
                    description: Some(description.into()),
                })
                .collect(),
            },
            WireQuestion::Score {
                name: "urgency".into(),
                instructions: "How urgent is this ticket?".into(),
                levels: [
                    "Can wait for the next release",
                    "Should be fixed this week",
                    "Blocking revenue right now",
                ]
                .into_iter()
                .map(|label| WireLevel {
                    label: label.into(),
                    description: None,
                })
                .collect(),
            },
        ],
    );
    assert_eq!(
        body,
        json!({
          "model": "typesafe/jev-1.13",
          "questions": {
            "is_bug": {
              "instructions": "Is the customer reporting a software defect?",
              "type": "noul"
            },
            "team": {
              "criteria": {
                "account": "Login, permissions, or profile issues.",
                "frontend": "Rendering, layout, or browser compatibility issues.",
                "payments": "Checkout, billing, or payment processing issues."
              },
              "instructions": "Which team should own this ticket?",
              "type": "choice"
            },
            "urgency": {
              "criteria": [
                "Can wait for the next release",
                "Should be fixed this week",
                "Blocking revenue right now"
              ],
              "instructions": "How urgent is this ticket?",
              "type": "score"
            }
          },
          "state": {
            "customer_tier": "enterprise",
            "ticket": "My checkout page shows a blank screen after I click Pay. I have tried two browsers."
          }
        })
    );
}

/// An option with no description is sent as `null`, as TypeSafe's `tone`
/// example does; a level with one carries its label in front.
#[test]
fn undescribed_options_are_null_and_described_levels_keep_their_label() {
    let body = build_body(
        "m",
        json!("s"),
        &[
            WireQuestion::Choice {
                name: "tone".into(),
                instructions: "What is the customer's tone?".into(),
                choices: ["calm", "frustrated", "angry"]
                    .into_iter()
                    .map(|value| WireChoice {
                        value: value.into(),
                        description: None,
                    })
                    .collect(),
            },
            WireQuestion::Score {
                name: "severity".into(),
                instructions: "How severe?".into(),
                levels: vec![
                    WireLevel {
                        label: "low".into(),
                        description: Some("Cosmetic; no impact to functionality".into()),
                    },
                    WireLevel {
                        label: "high".into(),
                        description: None,
                    },
                ],
            },
        ],
    );
    assert_eq!(
        body["questions"]["tone"]["criteria"],
        json!({"calm": null, "frustrated": null, "angry": null})
    );
    assert_eq!(
        body["questions"]["severity"]["criteria"],
        json!(["low: Cosmetic; no impact to functionality", "high"])
    );
}

/// TypeSafe's documented reply, verbatim.
const TYPESAFE_NOUL_REPLY: &str = r#"{
  "model": "jev-latest",
  "answers": {
    "billing": {
      "type": "noul",
      "noul": 0.98
    }
  },
  "usage": {
    "input_tokens": 120,
    "output_tokens": 12
  }
}"#;

/// OpenRouter's documented reply, verbatim.
const OPENROUTER_REPLY: &str = r#"{
  "answers": {
    "is_bug": {
      "noul": 0.96,
      "type": "noul"
    },
    "team": {
      "choice": "payments",
      "confidence": 0.75,
      "probabilities": {
        "account": 0,
        "frontend": 0.16,
        "payments": 0.84
      },
      "type": "choice"
    },
    "urgency": {
      "confidence": 0.99,
      "legend": {
        "0": "Can wait for the next release",
        "1": "Should be fixed this week",
        "2": "Blocking revenue right now"
      },
      "probabilities": {
        "0": 0,
        "1": 0.01,
        "2": 0.99
      },
      "score": 1.99,
      "type": "score"
    }
  },
  "id": "gen-dec-1789738314-X5e5eKGQdvR9rblyX250",
  "model": "typesafe/jev-1.13-20260917",
  "provider": "TypeSafe",
  "usage": {
    "cost": 0.000019992,
    "input_tokens": 476,
    "output_tokens": 70
  }
}"#;

fn probabilities(entries: &[(&str, f64)]) -> BTreeMap<String, f64> {
    entries
        .iter()
        .map(|(key, probability)| (key.to_string(), *probability))
        .collect()
}

#[test]
fn typesafes_documented_reply_decodes() {
    let reply = read(TYPESAFE_NOUL_REPLY).unwrap();
    assert_eq!(reply.model.as_deref(), Some("jev-latest"));
    assert_eq!(
        reply.answers["billing"],
        SystemOneAnswer::Noul { noul: 0.98 }
    );
    assert_eq!(
        reply.usage,
        Some(SystemOneUsage {
            input_tokens: Some(120),
            output_tokens: Some(12),
            cost: None,
        })
    );
}

#[test]
fn openrouters_documented_reply_decodes_answer_for_answer() {
    let reply = read(OPENROUTER_REPLY).unwrap();
    assert_eq!(reply.model.as_deref(), Some("typesafe/jev-1.13-20260917"));
    assert_eq!(
        reply.answers["is_bug"],
        SystemOneAnswer::Noul { noul: 0.96 }
    );
    assert_eq!(
        reply.answers["team"],
        SystemOneAnswer::Choice {
            choice: "payments".into(),
            confidence: Some(0.75),
            probabilities: probabilities(&[
                ("account", 0.0),
                ("frontend", 0.16),
                ("payments", 0.84)
            ]),
        }
    );
    let SystemOneAnswer::Score {
        score,
        confidence,
        probabilities: by_level,
        legend,
    } = &reply.answers["urgency"]
    else {
        panic!("expected a score: {:?}", reply.answers["urgency"]);
    };
    assert_eq!(*score, 1.99);
    assert_eq!(*confidence, Some(0.99));
    assert_eq!(
        by_level,
        &probabilities(&[("0", 0.0), ("1", 0.01), ("2", 0.99)])
    );
    assert_eq!(legend["1"], json!("Should be fixed this week"));
    assert_eq!(
        reply.usage,
        Some(SystemOneUsage {
            input_tokens: Some(476),
            output_tokens: Some(70),
            cost: Some(0.000019992),
        })
    );
}

/// A structured level (TypeSafe's `bug_severity` with examples) comes back in
/// the legend as the object it was sent as.
#[test]
fn a_structured_legend_entry_is_kept_as_sent() {
    let reply = read(
        r#"{"model":"jev-1.13.0","answers":{"bug_severity":{"type":"score","score":1.09,"confidence":0.87,
            "legend":{"0":{"what":"Cosmetic; no impact to functionality","examples":["typo in a label"]}},
            "probabilities":{"0":0.0,"1":0.91,"2":0.09}}},"usage":{"input_tokens":379,"output_tokens":18}}"#,
    )
    .unwrap();
    let SystemOneAnswer::Score { legend, .. } = &reply.answers["bug_severity"] else {
        panic!("expected a score");
    };
    assert_eq!(legend["0"]["what"], "Cosmetic; no impact to functionality");
}

/// Neither reference shows a refusal, so anything that is not a recognised
/// answer is kept verbatim for the facet to read as one, rather than failing
/// the whole reply or being coerced into a pick.
#[test]
fn an_answer_of_no_recognised_shape_is_kept_verbatim_not_guessed() {
    let reply = read(
        r#"{"model":"m","answers":{
            "declined":{"type":"refusal","reason":"unsafe"},
            "located":{"type":"box","left":0.1,"top":0.2,"right":0.3,"bottom":0.4},
            "half":{"type":"noul"},
            "fine":{"type":"noul","noul":0.5}
        },"usage":{"input_tokens":3,"output_tokens":0}}"#,
    )
    .unwrap();
    assert_eq!(
        reply.answers["declined"],
        SystemOneAnswer::Unrecognised(json!({"type": "refusal", "reason": "unsafe"}))
    );
    assert!(matches!(
        reply.answers["located"],
        SystemOneAnswer::Unrecognised(_)
    ));
    // A noul with no probability is not a noul of 0.
    assert_eq!(
        reply.answers["half"],
        SystemOneAnswer::Unrecognised(json!({"type": "noul"}))
    );
    assert_eq!(reply.answers["fine"], SystemOneAnswer::Noul { noul: 0.5 });
}

#[test]
fn an_unreadable_envelope_is_an_invalid_reply() {
    let error = read(r#"{"answers":[{"type":"noul","noul":0.5}]}"#).unwrap_err();
    assert!(
        matches!(&error, CompletionError::InvalidResponse(text) if text.contains("System One")),
        "{error:?}"
    );
}

/// Serve one request with `status` and `reply`, and hand back the request
/// exactly as it crossed the socket.
fn serve_once(
    status: &'static str,
    reply: &'static str,
) -> (String, std::sync::mpsc::Receiver<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (sender, received) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let request = read_request(&mut stream);
        respond(&mut stream, status, reply);
        let _ = sender.send(request);
    });
    (format!("http://{address}/api/alpha/decisions"), received)
}

/// Serve `script`'s replies in order, one per request, and a 500 to any
/// request past its end. Each request's JSON body is sent before its reply,
/// so every body is on the receiver by the time the call returns.
fn serve_script(script: Vec<(&'static str, String)>) -> (String, std::sync::mpsc::Receiver<Value>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (sender, received) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for index in 0.. {
            let Ok((mut stream, _)) = listener.accept() else {
                return;
            };
            let request = read_request(&mut stream);
            let body = serde_json::from_str(&request[request.find("\r\n\r\n").unwrap() + 4..])
                .unwrap_or(Value::Null);
            if sender.send(body).is_err() {
                return;
            }
            let (status, reply) = script.get(index).cloned().unwrap_or((
                "500 Internal Server Error",
                r#"{"error":{"message":"an unscripted request"}}"#.into(),
            ));
            respond(&mut stream, status, &reply);
        }
    });
    (format!("http://{address}/api/alpha/decisions"), received)
}

fn read_request(stream: &mut TcpStream) -> String {
    let mut raw = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        let read = stream.read(&mut chunk).unwrap();
        raw.extend_from_slice(&chunk[..read]);
        let text = String::from_utf8_lossy(&raw).to_string();
        let Some(head_end) = text.find("\r\n\r\n") else {
            continue;
        };
        let length = text[..head_end]
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
                    .flatten()
            })
            .unwrap_or(0);
        if raw.len() >= head_end + 4 + length || read == 0 {
            break text;
        }
    }
}

fn respond(stream: &mut TcpStream, status: &str, reply: &str) {
    let _ = write!(
        stream,
        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{reply}",
        reply.len()
    );
}

fn endpoint(url: String) -> DecisionsEndpoint {
    DecisionsEndpoint {
        provider_name: "OpenRouter".into(),
        backend_key: "openrouter".into(),
        url,
        headers: vec![
            ("content-type".into(), "application/json".into()),
            ("authorization".into(), "Bearer sk-or-test".into()),
        ],
        context_limits: None,
    }
}

#[test]
fn the_post_goes_to_the_endpoint_with_the_bearer_key_and_reads_the_reply() {
    let (url, request) = serve_once("200 OK", OPENROUTER_REPLY);
    let body = build_body(
        "typesafe/jev-1.14",
        json!("text"),
        &[WireQuestion::Predicate {
            name: "is_bug".into(),
            instructions: "?".into(),
        }],
    );
    let reply = post_systemone(&endpoint(url), body, Duration::from_secs(5)).unwrap();
    assert_eq!(
        reply.answers["is_bug"],
        SystemOneAnswer::Noul { noul: 0.96 }
    );
    let request = request.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(
        request.starts_with("POST /api/alpha/decisions "),
        "{request}"
    );
    assert!(
        request
            .to_lowercase()
            .contains("authorization: bearer sk-or-test"),
        "{request}"
    );
    let sent: Value =
        serde_json::from_str(&request[request.find("\r\n\r\n").unwrap() + 4..]).unwrap();
    assert_eq!(sent["questions"]["is_bug"]["type"], "noul");
    assert_eq!(sent["state"], "text");
}

/// OpenRouter's documented error body reaches the caller as its sentence.
#[test]
fn a_provider_error_carries_its_own_sentence() {
    let (url, _) = serve_once(
        "402 Payment Required",
        r#"{"error":{"code":402,"message":"Insufficient credits. Add more using https://openrouter.ai/credits"}}"#,
    );
    let error = post_systemone(&endpoint(url), json!({}), Duration::from_secs(5)).unwrap_err();
    assert_eq!(
        error,
        CompletionError::Upstream(
            "OpenRouter decision returned HTTP 402: Insufficient credits. Add more using https://openrouter.ai/credits"
                .into()
        )
    );
}

/// TypeSafe's documented overflow (HTTP 400 with `max_tokens_exceeded`) is
/// its own typed error, never retried and never read as a refused question,
/// and it says what the endpoint's limits are and what lifts them.
#[test]
fn a_request_longer_than_the_context_is_refused_with_the_endpoints_limits() {
    let (url, _) = serve_once(
        "400 Bad Request",
        r#"{"detail":"max_tokens_exceeded: the request is longer than the model's context length"}"#,
    );
    let endpoint = DecisionsEndpoint {
        provider_name: "TypeSafe".into(),
        context_limits: Some("Jev reads at most 64k tokens per request.".into()),
        ..endpoint(url)
    };
    let error =
        post_systemone(&endpoint, predicates(&["a", "b"]), Duration::from_secs(5)).unwrap_err();
    assert_eq!(
        error,
        CompletionError::ContextExceeded(
            "TypeSafe refused the decision as longer than the model's context (max_tokens_exceeded: the request is longer than the model's context length). Jev reads at most 64k tokens per request."
                .into()
        )
    );
    assert!(!error.is_transient());
}

/// Any other 400 is the provider's sentence, as before.
#[test]
fn another_bad_request_is_not_a_context_refusal() {
    let (url, _) = serve_once("400 Bad Request", r#"{"detail":"too much nesting"}"#);
    let error = post_systemone(&endpoint(url), json!({}), Duration::from_secs(5)).unwrap_err();
    assert_eq!(
        error,
        CompletionError::Upstream("OpenRouter decision returned HTTP 400: too much nesting".into())
    );
}

fn predicates(names: &[&str]) -> Value {
    build_body(
        "openai/gpt-6-luna-decisions",
        json!("state"),
        &names
            .iter()
            .map(|name| WireQuestion::Predicate {
                name: name.to_string(),
                instructions: format!("{name}?"),
            })
            .collect::<Vec<_>>(),
    )
}

/// OpenRouter's whole-request failure when its OpenAI model declines `name`.
fn refusing(name: &str) -> (&'static str, String) {
    refusing_as("OpenAI", name)
}

fn refusing_as(upstream: &str, name: &str) -> (&'static str, String) {
    (
        "502 Bad Gateway",
        json!({"error": {
            "code": 502,
            "message": format!("{upstream} refused to answer question \"{name}\"")
        }})
        .to_string(),
    )
}

fn answering(names: &[&str], input_tokens: i64, cost: f64) -> (&'static str, String) {
    let answers: serde_json::Map<String, Value> = names
        .iter()
        .map(|name| (name.to_string(), json!({"type": "noul", "noul": 0.7})))
        .collect();
    (
        "200 OK",
        json!({
            "model": "openai/gpt-6-luna-decisions-20261001",
            "answers": answers,
            "usage": {"input_tokens": input_tokens, "output_tokens": 0, "cost": cost}
        })
        .to_string(),
    )
}

fn asked(body: &Value) -> Vec<String> {
    body["questions"]
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect()
}

fn refused(reason: &str) -> SystemOneAnswer {
    SystemOneAnswer::Refused {
        reason: reason.into(),
    }
}

/// A whole-request 502 naming one question is that question's refusal: the
/// rest are asked again without it, and the reply answers all three.
#[test]
fn a_refused_question_is_dropped_and_the_rest_asked_again() {
    let (status, mut refusal) = refusing("b");
    // Should a refusal body ever report usage, it is counted too.
    let mut parsed: Value = serde_json::from_str(&refusal).unwrap();
    parsed["usage"] = json!({"input_tokens": 100, "cost": 0.25});
    refusal = parsed.to_string();
    let (url, requests) = serve_script(vec![(status, refusal), answering(&["a", "c"], 40, 0.5)]);
    let reply = post_systemone(
        &endpoint(url),
        predicates(&["a", "b", "c"]),
        Duration::from_secs(5),
    )
    .unwrap();
    let requests: Vec<Value> = requests.try_iter().collect();
    assert_eq!(requests.len(), 2);
    assert_eq!(asked(&requests[0]), ["a", "b", "c"]);
    assert_eq!(asked(&requests[1]), ["a", "c"]);
    assert_eq!(requests[1]["model"], requests[0]["model"]);
    assert_eq!(requests[1]["state"], requests[0]["state"]);
    assert_eq!(reply.answers["a"], SystemOneAnswer::Noul { noul: 0.7 });
    assert_eq!(reply.answers["c"], SystemOneAnswer::Noul { noul: 0.7 });
    assert_eq!(
        reply.answers["b"],
        refused("OpenAI refused to answer question \"b\"")
    );
    assert_eq!(
        reply.model.as_deref(),
        Some("openai/gpt-6-luna-decisions-20261001")
    );
    assert_eq!(
        reply.usage,
        Some(SystemOneUsage {
            input_tokens: Some(140),
            output_tokens: Some(0),
            cost: Some(0.75),
        })
    );
}

/// The match starts at `refused to answer question`, so an upstream other
/// than OpenAI naming itself is read the same way.
#[test]
fn two_refusals_are_both_spliced_and_the_third_post_answers() {
    let (url, requests) = serve_script(vec![
        refusing("b"),
        refusing_as("Anthropic", "c"),
        answering(&["a", "d"], 30, 0.1),
    ]);
    let reply = post_systemone(
        &endpoint(url),
        predicates(&["a", "b", "c", "d"]),
        Duration::from_secs(5),
    )
    .unwrap();
    let requests: Vec<Value> = requests.try_iter().collect();
    assert_eq!(requests.len(), 3);
    assert_eq!(asked(&requests[1]), ["a", "c", "d"]);
    assert_eq!(asked(&requests[2]), ["a", "d"]);
    assert_eq!(
        reply.answers.keys().collect::<Vec<_>>(),
        ["a", "b", "c", "d"]
    );
    assert_eq!(
        reply.answers["b"],
        refused("OpenAI refused to answer question \"b\"")
    );
    assert_eq!(
        reply.answers["c"],
        refused("Anthropic refused to answer question \"c\"")
    );
}

/// A model that keeps refusing fails the call loudly, naming every refused
/// question, rather than costing a round trip per question.
#[test]
fn a_fourth_refusal_fails_the_call_naming_every_refused_question() {
    let (url, requests) = serve_script(vec![
        refusing("a"),
        refusing("b"),
        refusing("c"),
        refusing("d"),
        answering(&["e"], 1, 0.0),
    ]);
    let error = post_systemone(
        &endpoint(url),
        predicates(&["a", "b", "c", "d", "e"]),
        Duration::from_secs(5),
    )
    .unwrap_err();
    assert_eq!(requests.try_iter().count(), 4);
    assert_eq!(
        error,
        CompletionError::Upstream(
            r#"OpenRouter refused 4 questions in one decision, more than the 3 a call asks around: "a", "b", "c", "d""#
                .into()
        )
    );
}

/// A refusal naming a question that was not asked is not one to act on.
#[test]
fn a_refusal_naming_a_question_not_asked_is_an_ordinary_upstream_error() {
    let (url, requests) = serve_script(vec![refusing("z"), answering(&["a"], 1, 0.0)]);
    let error =
        post_systemone(&endpoint(url), predicates(&["a"]), Duration::from_secs(5)).unwrap_err();
    assert_eq!(requests.try_iter().count(), 1);
    assert_eq!(
        error,
        CompletionError::Upstream(
            r#"OpenRouter decision returned HTTP 502: OpenAI refused to answer question "z""#
                .into()
        )
    );
}

#[test]
fn a_502_that_is_not_a_refusal_is_an_ordinary_upstream_error() {
    let (url, requests) = serve_script(vec![
        (
            "502 Bad Gateway",
            r#"{"error":{"code":502,"message":"Upstream provider unavailable for question \"a\""}}"#
                .into(),
        ),
        answering(&["a"], 1, 0.0),
    ]);
    let error =
        post_systemone(&endpoint(url), predicates(&["a"]), Duration::from_secs(5)).unwrap_err();
    assert_eq!(requests.try_iter().count(), 1);
    assert_eq!(
        error,
        CompletionError::Upstream(
            r#"OpenRouter decision returned HTTP 502: Upstream provider unavailable for question "a""#
                .into()
        )
    );
}

/// Refusing the only question left leaves nothing to ask: no empty request
/// is posted, and the reply carries only the refusal.
#[test]
fn refusing_the_last_question_posts_nothing_more() {
    let (url, requests) = serve_script(vec![refusing("a"), answering(&[], 1, 0.0)]);
    let reply = post_systemone(&endpoint(url), predicates(&["a"]), Duration::from_secs(5)).unwrap();
    assert_eq!(requests.try_iter().count(), 1);
    assert_eq!(
        reply,
        SystemOneResponse {
            model: None,
            answers: [(
                "a".to_string(),
                refused("OpenAI refused to answer question \"a\"")
            )]
            .into(),
            usage: None,
        }
    );
}
