//! The Decisions request and reply, pinned against the documented shape
//! (https://vercel.com/docs/ai-gateway/sdks-and-apis/openai-decisions, which
//! states it implements the OpenAI Decisions API's request and response).

use std::io::{Read, Write};
use std::net::TcpListener;
use std::time::Duration;

use serde_json::json;

use super::http::{build_body, post_decisions, read, DecisionsEndpoint};
use super::wire::*;
use crate::CompletionError;

fn documented_questions() -> Vec<WireQuestion> {
    vec![
        WireQuestion::Predicate {
            name: "damaged".into(),
            instructions: "Does the customer report a damaged item?".into(),
        },
        WireQuestion::Choice {
            name: "queue".into(),
            instructions: "Which team should handle this ticket?".into(),
            choices: vec![
                WireChoice {
                    value: "billing".into(),
                    description: Some("Refunds and payments".into()),
                },
                WireChoice {
                    value: "shipping".into(),
                    description: Some("Delivery and damaged items".into()),
                },
            ],
        },
        WireQuestion::Score {
            name: "urgency".into(),
            instructions: "How urgent is this ticket?".into(),
            levels: ["low", "medium", "high"]
                .into_iter()
                .map(|label| WireLevel {
                    label: label.into(),
                    description: None,
                })
                .collect(),
        },
    ]
}

#[test]
fn the_request_body_is_the_documented_request() {
    let body = build_body(
        "openai/gpt-6-luna-decisions",
        "The package arrived with a broken screen. I want my money back.",
        &documented_questions(),
    );
    assert_eq!(
        body,
        json!({
            "model": "openai/gpt-6-luna-decisions",
            "input": "The package arrived with a broken screen. I want my money back.",
            "questions": [
                {
                    "type": "predicate",
                    "name": "damaged",
                    "instructions": "Does the customer report a damaged item?"
                },
                {
                    "type": "choice",
                    "name": "queue",
                    "instructions": "Which team should handle this ticket?",
                    "choices": [
                        { "value": "billing", "description": "Refunds and payments" },
                        { "value": "shipping", "description": "Delivery and damaged items" }
                    ]
                },
                {
                    "type": "score",
                    "name": "urgency",
                    "instructions": "How urgent is this ticket?",
                    "levels": [{ "label": "low" }, { "label": "medium" }, { "label": "high" }]
                }
            ]
        })
    );
}

/// The documented reply, verbatim, including the gateway's metadata (which
/// the decoder must tolerate) and `output_tokens: 0`.
const DOCUMENTED_REPLY: &str = r#"{
  "model": "openai/gpt-6-luna-decisions",
  "answers": [
    { "type": "predicate", "name": "damaged", "probability": 0.95 },
    {
      "type": "choice",
      "name": "queue",
      "choice": "shipping",
      "confidence": 0.62,
      "probabilities": [
        { "value": "billing", "probability": 0.19 },
        { "value": "shipping", "probability": 0.81 }
      ]
    },
    {
      "type": "score",
      "name": "urgency",
      "score": 1.3,
      "confidence": 0.31,
      "probabilities": [
        { "value": 0, "label": "low", "probability": 0.08 },
        { "value": 1, "label": "medium", "probability": 0.54 },
        { "value": 2, "label": "high", "probability": 0.38 }
      ]
    }
  ],
  "usage": { "input_tokens": 96, "output_tokens": 0, "total_tokens": 96 },
  "provider_metadata": {
    "gateway": {
      "routing": { "originalModelId": "openai/gpt-6-luna-decisions", "finalProvider": "openai" },
      "cost": "0.0000096",
      "generationId": "gen_..."
    }
  }
}"#;

#[test]
fn the_documented_reply_decodes_answer_for_answer() {
    let reply = read(DOCUMENTED_REPLY).unwrap();
    assert_eq!(reply.model.as_deref(), Some("openai/gpt-6-luna-decisions"));
    assert_eq!(
        reply.usage,
        Some(DecisionsUsage {
            input_tokens: Some(96),
            output_tokens: Some(0),
            total_tokens: Some(96),
        })
    );
    assert_eq!(
        reply.answers[0],
        WireAnswer::Predicate {
            name: Some("damaged".into()),
            probability: 0.95,
        }
    );
    let WireAnswer::Choice {
        choice,
        confidence,
        probabilities,
        ..
    } = &reply.answers[1]
    else {
        panic!("expected a choice: {:?}", reply.answers[1]);
    };
    assert_eq!(choice.as_text(), "shipping");
    assert_eq!(*confidence, Some(0.62));
    assert_eq!(probabilities.len(), 2);
    let WireAnswer::Score {
        score,
        probabilities,
        ..
    } = &reply.answers[2]
    else {
        panic!("expected a score: {:?}", reply.answers[2]);
    };
    assert_eq!(*score, 1.3);
    assert_eq!(probabilities[1].label.as_deref(), Some("medium"));
    assert_eq!(probabilities[1].value, 1);
}

/// A declined question arrives as `{"type":"refusal","name":...}` in its
/// position, beside normally answered questions, and decodes as a refusal:
/// not as a predicate or choice with probability 0.
#[test]
fn a_refused_question_decodes_as_a_refusal_in_its_position() {
    let reply = read(
        r#"{
          "model": "typesafe-ai/jev",
          "answers": [
            { "type": "refusal", "name": "damaged" },
            { "type": "choice", "name": "queue", "choice": "billing" }
          ],
          "usage": { "input_tokens": 40, "output_tokens": 0 }
        }"#,
    )
    .unwrap();
    assert_eq!(
        reply.answers[0],
        WireAnswer::Refusal {
            name: Some("damaged".into())
        }
    );
    // `confidence` and `probabilities` are optional on a choice.
    assert_eq!(
        reply.answers[1],
        WireAnswer::Choice {
            name: Some("queue".into()),
            choice: WireValue::Text("billing".into()),
            confidence: None,
            probabilities: vec![],
        }
    );
    assert_eq!(reply.usage.unwrap().output_tokens, Some(0));
}

#[test]
fn boolean_choice_values_and_unnamed_questions_decode() {
    let reply = read(
        r#"{"answers":[{"type":"choice","name":null,"choice":true,
            "probabilities":[{"value":true,"probability":0.7},{"value":false,"probability":0.3}]}]}"#,
    )
    .unwrap();
    let WireAnswer::Choice {
        name,
        choice,
        probabilities,
        ..
    } = &reply.answers[0]
    else {
        panic!("expected a choice");
    };
    assert_eq!(name, &None);
    assert_eq!(choice.as_text(), "true");
    assert_eq!(probabilities[1].value.as_text(), "false");
}

#[test]
fn an_unknown_answer_type_is_an_invalid_reply_not_a_guess() {
    let error = read(r#"{"answers":[{"type":"ranking","name":"x"}]}"#).unwrap_err();
    assert!(
        matches!(&error, CompletionError::InvalidResponse(text) if text.contains("ranking")),
        "{error:?}"
    );
}

/// Serve one request with `status` and `reply`, and hand back the request
/// line, headers, and body exactly as they crossed the socket.
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
    (format!("http://{address}/v1/decisions"), received)
}

fn endpoint(url: String) -> DecisionsEndpoint {
    DecisionsEndpoint {
        provider_name: "Gateway".into(),
        backend_key: "custom:acc_gateway".into(),
        url,
        headers: vec![
            ("content-type".into(), "application/json".into()),
            ("authorization".into(), "Bearer sk-test".into()),
        ],
    }
}

#[test]
fn the_post_goes_to_the_decisions_path_with_the_bearer_key() {
    let (url, request) = serve_once(
        "200 OK",
        r#"{"model":"m","answers":[{"type":"refusal","name":"q"}],"usage":{"input_tokens":3,"output_tokens":0}}"#,
    );
    let body = build_body(
        "m",
        "text",
        &[WireQuestion::Predicate {
            name: "q".into(),
            instructions: "?".into(),
        }],
    );
    let reply = post_decisions(&endpoint(url), body, Duration::from_secs(5)).unwrap();
    assert_eq!(
        reply.answers,
        vec![WireAnswer::Refusal {
            name: Some("q".into())
        }]
    );
    let request = request.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(request.starts_with("POST /v1/decisions "), "{request}");
    assert!(
        request
            .to_lowercase()
            .contains("authorization: bearer sk-test"),
        "{request}"
    );
    let sent: serde_json::Value =
        serde_json::from_str(&request[request.find("\r\n\r\n").unwrap() + 4..]).unwrap();
    assert_eq!(sent["questions"][0]["type"], "predicate");
}

#[test]
fn a_provider_error_carries_its_own_sentence() {
    let (url, _) = serve_once(
        "400 Bad Request",
        r#"{"error":{"message":"Question names must be unique","type":"invalid_request_error","param":"questions[1].name"}}"#,
    );
    let error = post_decisions(&endpoint(url), json!({}), Duration::from_secs(5)).unwrap_err();
    assert_eq!(
        error,
        CompletionError::Upstream(
            "Gateway decision returned HTTP 400: Question names must be unique".into()
        )
    );
}

#[test]
fn the_endpoint_never_prints_its_key() {
    let printed = format!("{:?}", endpoint("http://x/v1/decisions".into()));
    assert!(!printed.contains("sk-test"), "{printed}");
    assert!(printed.contains("authorization: <redacted>"), "{printed}");
}
