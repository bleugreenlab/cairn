use super::http::build_provider_object;
use crate::agent_process::process::SessionControl;
use crate::backends::http_loop::HttpTurnControl;
use crate::models::{OpenRouterRouting, OpenRouterSort};
use serde_json::json;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[test]
fn provider_object_default_only_requires_parameters() {
    let provider = build_provider_object(&OpenRouterRouting::default());
    assert_eq!(provider, json!({ "require_parameters": true }));
}

#[test]
fn provider_object_includes_zdr_when_opted_in() {
    let provider = build_provider_object(&OpenRouterRouting {
        zero_data_retention: true,
        sort: None,
    });
    assert_eq!(provider["zdr"], json!(true));
    assert_eq!(provider["require_parameters"], json!(true));
    assert!(provider.get("sort").is_none());
}

#[test]
fn provider_object_maps_each_sort_variant() {
    for (variant, expected) in [
        (OpenRouterSort::Price, "price"),
        (OpenRouterSort::Throughput, "throughput"),
        (OpenRouterSort::Latency, "latency"),
    ] {
        let provider = build_provider_object(&OpenRouterRouting {
            zero_data_retention: false,
            sort: Some(variant),
        });
        assert_eq!(provider["sort"], json!(expected));
        assert!(provider.get("zdr").is_none());
    }
}

#[test]
fn provider_object_combines_zdr_and_sort() {
    let provider = build_provider_object(&OpenRouterRouting {
        zero_data_retention: true,
        sort: Some(OpenRouterSort::Latency),
    });
    assert_eq!(provider["require_parameters"], json!(true));
    assert_eq!(provider["zdr"], json!(true));
    assert_eq!(provider["sort"], json!("latency"));
}

#[test]
fn send_interrupt_sets_cancel_flag() {
    let cancel = Arc::new(AtomicBool::new(false));
    let mut control = HttpTurnControl {
        cancel: cancel.clone(),
        provider: "OpenRouter".to_string(),
    };
    SessionControl::interrupt(&mut control).expect("interrupt accepted");
    assert!(cancel.load(Ordering::SeqCst));
}

/// Byte-exact request bodies of OpenRouter's owned turn. See
/// `http_loop::scripted` for how they are recorded.
mod golden {
    use super::super::adapter::OpenRouterAdapter;
    use crate::backends::http_loop::scripted::{
        answer_schema, assert_golden, chat_text, record_turn, session_config,
    };
    use crate::models::{OpenRouterRouting, OpenRouterSort, UpdateSettings};

    #[tokio::test]
    async fn owned_turn_without_knobs() {
        let bodies = record_turn(
            session_config("openai/gpt-5"),
            vec![chat_text("done")],
            OpenRouterAdapter::new,
        )
        .await;
        assert_golden(
            &bodies,
            &[
                r#"{"messages":[{"content":"SYSTEM","role":"system"},{"content":"hi","role":"user"}],"model":"openai/gpt-5","provider":{"require_parameters":true},"stream":true,"tool_choice":"auto","tools":["read","write","run"]}"#,
            ],
        );
    }

    #[tokio::test]
    async fn owned_turn_with_effort_schema_and_routing() {
        let mut config = session_config("openai/gpt-5");
        config.reasoning_effort = Some("high".to_string());
        config.output_schema = Some(answer_schema());
        let bodies = record_turn(config, vec![chat_text(r#"{"answer":"done"}"#)], |orch| {
            orch.update_settings(UpdateSettings {
                openrouter_routing: Some(OpenRouterRouting {
                    zero_data_retention: true,
                    sort: Some(OpenRouterSort::Throughput),
                }),
                ..Default::default()
            })
            .unwrap();
            OpenRouterAdapter::new(orch)
        })
        .await;
        assert_golden(
            &bodies,
            &[
                r#"{"messages":[{"content":"SYSTEM","role":"system"},{"content":"hi","role":"user"}],"model":"openai/gpt-5","provider":{"require_parameters":true,"sort":"throughput","zdr":true},"reasoning":{"effort":"high"},"response_format":{"json_schema":{"name":"cairn_output","schema":{"properties":{"answer":{"type":"string"}},"required":["answer"],"type":"object"},"strict":true},"type":"json_schema"},"stream":true,"tool_choice":"auto","tools":["read","write","run"]}"#,
            ],
        );
    }
}
