//! The System One request and reply, pinned against the two reference pages:
//! TypeSafe's (https://docs.typesafe.ai/api/system-one, with the Choice and
//! Score primitives at https://docs.typesafe.ai/primitives/choice and
//! /primitives/score) and OpenRouter's
//! (https://openrouter.ai/docs/api/api-reference/alphadecisions/submit-a-decisions-questions-and-answers-request).

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::TcpListener;
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
        let mut raw = Vec::new();
        let mut chunk = [0u8; 8192];
        let request = loop {
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
        };
        let _ = write!(
            stream,
            "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{reply}",
            reply.len()
        );
        let _ = sender.send(request);
    });
    (format!("http://{address}/api/alpha/decisions"), received)
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
