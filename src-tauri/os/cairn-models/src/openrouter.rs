//! How a request identifies itself to OpenRouter: its key, and the app it is
//! attributed to on OpenRouter's rankings. Every OpenRouter call the facet or
//! its hosts make (chat completions, images) sends these headers.

/// The app a request is attributed to. A host that is not Cairn names
/// itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attribution {
    /// Sent as `http-referer`.
    pub referer: String,
    /// Sent as `x-openrouter-title`.
    pub title: String,
}

impl Attribution {
    pub fn cairn() -> Self {
        Self {
            referer: "https://cairn.computer".into(),
            title: "Cairn".into(),
        }
    }
}

/// The headers an OpenRouter request carries.
pub fn headers(api_key: &str, attribution: &Attribution) -> Vec<(String, String)> {
    vec![
        ("authorization".to_string(), format!("Bearer {api_key}")),
        ("content-type".to_string(), "application/json".to_string()),
        ("http-referer".to_string(), attribution.referer.clone()),
        ("x-openrouter-title".to_string(), attribution.title.clone()),
    ]
}

/// A cost-policy cutoff, not a tokenizer claim: below this many text characters
/// no explicit cache is requested, even if tools or images make the wire large.
/// Roughly 4K tokens of ordinary English; the provider enforces its own minimum.
const CACHE_MIN_TEXT_CHARS: usize = 16_384;

/// Makers/routes documented as needing explicit breakpoints. Keep this small:
/// unknown and implicit-cache routes must not acquire a cache-write premium.
/// Source: https://openrouter.ai/docs/guides/best-practices/prompt-caching
/// (Anthropic Claude / Alibaba Qwen, checked 2026-11-07).
fn explicit_cache_route(model: &str) -> bool {
    let model = model.split(':').next().unwrap_or(model);
    model.starts_with("anthropic/claude-")
        || matches!(
            model,
            "qwen/qwen3-max"
                | "qwen/qwen-plus"
                | "qwen/qwen3.6-plus"
                | "qwen/qwen3-coder-plus"
                | "qwen/qwen3-coder-flash"
        )
}

/// Preserve a cacheable static system prefix before the changing orientation.
/// Unknown/implicit routes retain their original wire shape. Keep a known
/// boundary even below the cutoff so a large dynamic tail cannot qualify the
/// entire system message as a stable cache prefix.
pub fn system_content(
    model: &str,
    system: &str,
    dynamic_tail: Option<&str>,
) -> crate::wire::openai_compat::wire::ChatContent {
    use crate::wire::openai_compat::wire::{ChatContent, ChatContentPart};
    if explicit_cache_route(model) {
        if let Some(tail) = dynamic_tail.filter(|s| !s.is_empty()) {
            if let Some(head) = system.strip_suffix(tail).filter(|head| !head.is_empty()) {
                return ChatContent::Parts(vec![
                    ChatContentPart::Text {
                        text: head.to_string(),
                    },
                    ChatContentPart::Text {
                        text: tail.to_string(),
                    },
                ]);
            }
        }
    }
    ChatContent::Text(system.to_string())
}

/// Put stable and rolling breakpoints on the *outgoing* body, never transcript
/// messages. Five-minute writes cost 1.25x input on these routes; one read at
/// 0.1x amortizes the premium, but a never-reused large prefix still costs more.
pub(crate) fn apply_prompt_caching(body: &mut serde_json::Value, session: bool) {
    use serde_json::Value;
    if !body
        .get("model")
        .and_then(Value::as_str)
        .is_some_and(explicit_cache_route)
    {
        return;
    }
    let Some(messages) = body.get_mut("messages").and_then(Value::as_array_mut) else {
        return;
    };
    let mut prefix_chars = 0usize;
    let mut stable = None;
    let mut rolling = None;
    for (index, message) in messages.iter().enumerate() {
        let Some(content) = message.get("content") else {
            continue;
        };
        prefix_chars = prefix_chars.saturating_add(text_chars(content));
        if prefix_chars < CACHE_MIN_TEXT_CHARS || !has_text(content) {
            continue;
        }
        match message.get("role").and_then(Value::as_str) {
            // The adapter splits static instructions from dynamic orientation.
            // Cache only the first text block, leaving its suffix uncached.
            Some("system" | "developer") if stable_text_chars(content) >= CACHE_MIN_TEXT_CHARS => {
                stable = Some(index)
            }
            Some("user" | "assistant" | "tool") => rolling = Some(index),
            _ => {}
        }
    }
    if let Some(index) = stable {
        mark_text(&mut messages[index]["content"], true);
    }
    // A completion can also have a large reusable user prefix without a system
    // prompt. With a stable system prefix, avoid paying to cache its unique
    // question. Sessions advance the tail each generation, including tool loops.
    if session || stable.is_none() {
        if let Some(index) = rolling {
            mark_text(&mut messages[index]["content"], false);
        }
    }
}

fn text_chars(content: &serde_json::Value) -> usize {
    use serde_json::Value;
    match content {
        Value::String(text) => text.chars().count(),
        Value::Array(parts) => parts
            .iter()
            .filter(|part| part["type"] == "text")
            .filter_map(|part| part["text"].as_str())
            .map(|text| text.chars().count())
            .sum(),
        _ => 0,
    }
}

fn has_text(content: &serde_json::Value) -> bool {
    content.as_str().is_some_and(|text| !text.is_empty())
        || content.as_array().is_some_and(|parts| {
            parts.iter().any(|part| {
                part["type"] == "text" && part["text"].as_str().is_some_and(|text| !text.is_empty())
            })
        })
}

fn stable_text_chars(content: &serde_json::Value) -> usize {
    if content.is_string() {
        return text_chars(content);
    }
    content
        .as_array()
        .and_then(|parts| parts.iter().find(|part| part["type"] == "text"))
        .and_then(|part| part["text"].as_str())
        .map(|text| text.chars().count())
        .unwrap_or(0)
}

fn mark_text(content: &mut serde_json::Value, first: bool) {
    use serde_json::json;
    if let Some(text) = content.as_str() {
        *content = json!([{"type": "text", "text": text}]);
    }
    if let Some(parts) = content.as_array_mut() {
        let index = if first {
            parts.iter().position(|part| part["type"] == "text")
        } else {
            parts.iter().rposition(|part| part["type"] == "text")
        };
        if let Some(index) = index {
            parts[index]["cache_control"] = json!({"type": "ephemeral"});
        }
    }
}

#[cfg(test)]
mod cache_tests {
    use super::*;
    use crate::interface::complete::{Complete, CompletionCall, CompletionWire};
    use crate::turn::{ChatFamily, ProtocolFamily};
    use crate::wire::openai_compat::{
        http::{ChatDialect, Endpoint},
        wire::ChatMessage,
    };
    use crate::wire::GenerationConfig;
    use serde_json::{json, Map, Value};
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::time::Duration;

    fn family(url: String, backend: &str) -> ChatFamily {
        ChatFamily {
            endpoint: Endpoint {
                provider_name: "test".into(),
                backend_key: backend.into(),
                chat_url: url,
                headers: vec![],
            },
            dialect: ChatDialect::default(),
            extras: Map::new(),
        }
    }

    // Capture the actual HTTP bytes, not just an intermediate typed message.
    fn sent_body(model: &str, system: &str, session: bool, backend: &str) -> Value {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut bytes = Vec::new();
            let (header_end, length) = loop {
                let mut buf = [0u8; 4096];
                let n = socket.read(&mut buf).unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buf[..n]);
                if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&bytes[..end]);
                    let length: usize = headers
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|value| value.trim().parse().unwrap())
                        })
                        .unwrap();
                    break (end + 4, length);
                }
            };
            while bytes.len() < header_end + length {
                let mut buf = [0u8; 4096];
                let n = socket.read(&mut buf).unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buf[..n]);
            }
            let body: Value =
                serde_json::from_slice(&bytes[header_end..header_end + length]).unwrap();
            let reply = r#"{"choices":[{"message":{"role":"assistant","content":"ok"}}]}"#;
            write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", reply.len(), reply).unwrap();
            body
        });
        let family = family(url, backend);
        let conversation = vec![
            ChatMessage::system(system.into()),
            ChatMessage::user("Answer briefly.".into()),
        ];
        let config = GenerationConfig::default();
        let body = if session {
            family.body(model, &conversation, &config)
        } else {
            family.request_body(&family.dialect, model, &conversation, &config, false)
        };
        // The server returns a whole reply for inspection of either request
        // shape; delivery decoding is covered by the transport's stream tests.
        crate::complete::HttpComplete
            .complete(CompletionCall::whole(
                CompletionWire::Chat(&family.endpoint),
                body,
                Duration::from_secs(5),
            ))
            .unwrap();
        server.join().unwrap()
    }

    #[test]
    fn cache_breakpoints_cross_the_wire_for_anthropic_both_doors() {
        for session in [false, true] {
            let body = sent_body(
                "anthropic/claude-haiku-5.5",
                &"x".repeat(CACHE_MIN_TEXT_CHARS),
                session,
                "openrouter",
            );
            assert_eq!(
                body["messages"][0]["content"][0]["cache_control"],
                json!({"type":"ephemeral"})
            );
            assert_eq!(body["stream"], session);
            let rolling = &body["messages"][1]["content"];
            if session {
                assert_eq!(rolling[0]["cache_control"], json!({"type":"ephemeral"}));
            } else {
                assert_eq!(rolling, "Answer briefly.");
            }
        }
    }

    #[test]
    fn implicit_unknown_short_and_other_backends_stay_unmodified_on_the_wire() {
        for session in [false, true] {
            for (model, system, backend) in [
                (
                    "deepseek/deepseek-v4-flash",
                    "x".repeat(CACHE_MIN_TEXT_CHARS),
                    "openrouter",
                ),
                (
                    "google/gemini-2.5-pro",
                    "x".repeat(CACHE_MIN_TEXT_CHARS),
                    "openrouter",
                ),
                (
                    "qwen/qwen3.5-plus-02-15",
                    "x".repeat(CACHE_MIN_TEXT_CHARS),
                    "openrouter",
                ),
                (
                    "anthropic/claude-haiku-5.5",
                    "x".repeat(CACHE_MIN_TEXT_CHARS - 16),
                    "openrouter",
                ),
                (
                    "anthropic/claude-haiku-5.5",
                    "x".repeat(CACHE_MIN_TEXT_CHARS),
                    "custom",
                ),
            ] {
                let body = sent_body(model, &system, session, backend);
                assert!(!body.to_string().contains("cache_control"), "{body}");
                assert_eq!(body["messages"][0]["content"], system);
            }
        }
    }

    #[test]
    fn rolling_prefix_moves_without_mutating_history_or_multimodal_parts() {
        let image = json!({"type":"image_url","image_url":{"url":"data:image/png;base64,AAAA"}});
        let mut body = json!({"model":"qwen/qwen3-coder-plus","messages":[
            {"role":"system","content":"instructions"},
            {"role":"user","content":[{"type":"text","text":"x".repeat(CACHE_MIN_TEXT_CHARS)},image.clone()]}
        ]});
        apply_prompt_caching(&mut body, true);
        assert_eq!(
            body["messages"][1]["content"][0]["cache_control"]["type"],
            "ephemeral"
        );
        assert_eq!(body["messages"][1]["content"][1], image);
        let first = body["messages"][1].clone();
        // Rebuild from stored history (without wire annotations), as the loop does.
        body["messages"][1]["content"][0]
            .as_object_mut()
            .unwrap()
            .remove("cache_control");
        body["messages"]
            .as_array_mut()
            .unwrap()
            .push(json!({"role":"tool","tool_call_id":"call1","content":"result"}));
        apply_prompt_caching(&mut body, true);
        assert_eq!(
            body["messages"][2]["content"][0]["cache_control"]["type"],
            "ephemeral"
        );
        assert_eq!(
            first["content"][0]["text"],
            body["messages"][1]["content"][0]["text"]
        );
        assert!(body["messages"][1]["content"][0]
            .get("cache_control")
            .is_none());
    }

    #[test]
    fn images_and_tools_do_not_make_a_short_text_cacheable() {
        let mut body = json!({"model":"anthropic/claude-haiku-5.5",
        "tools":[{"description":"x".repeat(CACHE_MIN_TEXT_CHARS)}],
        "messages":[{"role":"user","content":[
            {"type":"text","text":"Hi"},
            {"type":"image_url","image_url":{"url":"x".repeat(CACHE_MIN_TEXT_CHARS)}}
        ]}]});
        let before = body.clone();
        apply_prompt_caching(&mut body, true);
        assert_eq!(body, before);
    }
}

#[cfg(test)]
mod stable_prefix_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn dynamic_orientation_is_not_part_of_the_stable_breakpoint() {
        for tail in ["orientation one", "orientation two"] {
            let mut body = json!({"model":"anthropic/claude-haiku-5.5","messages":[
                {"role":"system","content":[
                    {"type":"text","text":"x".repeat(CACHE_MIN_TEXT_CHARS)},
                    {"type":"text","text":tail}]},
                {"role":"user","content":"question"}]});
            apply_prompt_caching(&mut body, true);
            assert_eq!(
                body["messages"][0]["content"][0]["cache_control"]["type"],
                "ephemeral"
            );
            assert!(body["messages"][0]["content"][1]
                .get("cache_control")
                .is_none());
            assert_eq!(body["messages"][0]["content"][1]["text"], tail);
        }
    }

    #[test]
    fn a_large_dynamic_tail_does_not_make_the_static_head_eligible() {
        let mut body = json!({"model":"anthropic/claude-haiku-5.5","messages":[
            {"role":"system","content":[
                {"type":"text","text":"small head"},
                {"type":"text","text":"x".repeat(CACHE_MIN_TEXT_CHARS)}]}]});
        let original = body.clone();
        apply_prompt_caching(&mut body, true);
        assert_eq!(body, original);
    }
}

#[cfg(test)]
mod system_content_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn only_cacheable_explicit_routes_split_orientation_without_changing_text() {
        for model in [
            "anthropic/claude-haiku-5.5",
            "qwen/qwen-plus",
            "deepseek/deepseek-v4-flash",
            "google/gemini-2.5-pro",
        ] {
            let head = "x".repeat(CACHE_MIN_TEXT_CHARS);
            let tail = "\n\nOrientation";
            let system = format!("{head}{tail}");
            let content = serde_json::to_value(system_content(model, &system, Some(tail))).unwrap();
            if explicit_cache_route(model) {
                assert_eq!(
                    content,
                    json!([
                        {"type":"text","text":head},
                        {"type":"text","text":tail}
                    ])
                );
            } else {
                assert_eq!(content, system);
            }
            let short =
                serde_json::to_value(system_content(model, "short tail", Some("tail"))).unwrap();
            if explicit_cache_route(model) {
                assert_eq!(
                    short,
                    json!([{"type":"text","text":"short "},{"type":"text","text":"tail"}])
                );
            } else {
                assert_eq!(short, "short tail");
            }
            assert_eq!(
                serde_json::to_value(system_content(model, &system, Some("not a suffix"))).unwrap(),
                system
            );
        }
    }
}

#[cfg(test)]
mod orientation_boundary_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn the_adapter_composition_never_marks_a_changing_tail_as_stable() {
        // Together these cross the cutoff; the static head alone does not.
        let head = "x".repeat(CACHE_MIN_TEXT_CHARS - 1);
        for tail in ["\norientation one", "\norientation two"] {
            let system = format!("{head}{tail}");
            let mut body = json!({"model":"anthropic/claude-haiku-5.5","messages":[
                {"role":"system","content": system_content("anthropic/claude-haiku-5.5", &system, Some(tail))}
            ]});
            apply_prompt_caching(&mut body, true);
            assert_eq!(body["messages"][0]["content"][0]["text"], head);
            assert_eq!(body["messages"][0]["content"][1]["text"], tail);
            assert!(!body.to_string().contains("cache_control"));
        }
    }
}
