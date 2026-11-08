use super::http::{provider_object, Request};
use crate::agent_process::process::SessionControl;
use crate::backends::http_loop::HttpTurnControl;
use crate::backends::retention::ZdrAdmission;
use crate::models::{OpenRouterRouting, OpenRouterSort};
use cairn_models::interface::retention::{RequiredBy, ZdrBasis, ZeroDataRetention};
use serde_json::json;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// A zero-retention requirement as the gate admits one, routing around
/// `exclude`.
pub(crate) fn admitted(exclude: &[&str]) -> ZdrAdmission {
    ZdrAdmission {
        report: ZeroDataRetention {
            required_by: RequiredBy::Call,
            satisfied_by: ZdrBasis::OpenRouterZdrRouting,
            detail: "test".into(),
        },
        exclude: exclude.iter().map(|tag| tag.to_string()).collect(),
    }
}

#[test]
fn a_chat_request_always_requires_parameters_and_a_whole_one_says_nothing_by_default() {
    let routing = OpenRouterRouting::default();
    assert_eq!(
        provider_object(&routing, None, Request::Chat),
        Some(json!({ "require_parameters": true }))
    );
    assert_eq!(provider_object(&routing, None, Request::Whole), None);
}

#[test]
fn an_admitted_requirement_sends_zdr_and_ignores_safety_retention_endpoints() {
    let routing = OpenRouterRouting::default();
    for request in [Request::Chat, Request::Whole] {
        let provider = provider_object(
            &routing,
            Some(&admitted(&["google-vertex/global"])),
            request,
        )
        .unwrap();
        assert_eq!(provider["zdr"], json!(true), "{request:?}");
        assert_eq!(provider["ignore"], json!(["google-vertex/global"]));
    }
    let provider = provider_object(&routing, Some(&admitted(&[])), Request::Whole).unwrap();
    assert_eq!(provider, json!({ "zdr": true }), "nothing to ignore");
}

#[test]
fn the_workspace_setting_sends_zdr_even_on_a_path_that_admitted_nothing() {
    let routing = OpenRouterRouting {
        zero_data_retention: true,
        sort: None,
    };
    for request in [Request::Chat, Request::Whole] {
        let provider = provider_object(&routing, None, request).unwrap();
        assert_eq!(provider["zdr"], json!(true), "{request:?}");
        assert!(provider.get("sort").is_none());
    }
}

#[test]
fn provider_object_maps_each_sort_variant() {
    for (variant, expected) in [
        (OpenRouterSort::Price, "price"),
        (OpenRouterSort::Throughput, "throughput"),
        (OpenRouterSort::Latency, "latency"),
    ] {
        let routing = OpenRouterRouting {
            zero_data_retention: false,
            sort: Some(variant),
        };
        for request in [Request::Chat, Request::Whole] {
            let provider = provider_object(&routing, None, request).unwrap();
            assert_eq!(provider["sort"], json!(expected));
            assert!(provider.get("zdr").is_none());
        }
    }
}

#[test]
fn provider_object_combines_zdr_and_sort() {
    let provider = provider_object(
        &OpenRouterRouting {
            zero_data_retention: true,
            sort: Some(OpenRouterSort::Latency),
        },
        None,
        Request::Chat,
    )
    .unwrap();
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
    use crate::backends::Admitted;
    use crate::models::{OpenRouterRouting, OpenRouterSort, UpdateSettings};

    #[tokio::test]
    async fn owned_turn_without_knobs() {
        let bodies = record_turn(
            session_config("openai/gpt-5"),
            vec![chat_text("done")],
            |orch| OpenRouterAdapter::new(orch, &Admitted::for_test(None)),
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
            OpenRouterAdapter::new(orch, &Admitted::for_test(None))
        })
        .await;
        assert_golden(
            &bodies,
            &[
                r#"{"messages":[{"content":"SYSTEM","role":"system"},{"content":"hi","role":"user"}],"model":"openai/gpt-5","provider":{"require_parameters":true,"sort":"throughput","zdr":true},"reasoning":{"effort":"high"},"response_format":{"json_schema":{"name":"cairn_output","schema":{"properties":{"answer":{"type":"string"}},"required":["answer"],"type":"object"},"strict":true},"type":"json_schema"},"stream":true,"tool_choice":"auto","tools":["read","write","run"]}"#,
            ],
        );
    }

    /// A session the launch gate admitted under a zero-retention requirement
    /// carries it on every turn, with the Safety Retention endpoints ignored.
    #[tokio::test]
    async fn owned_turn_under_an_admitted_zero_retention_requirement() {
        let bodies = record_turn(
            session_config("openai/gpt-5"),
            vec![chat_text("done")],
            |orch| {
                OpenRouterAdapter::new(
                    orch,
                    &Admitted::for_test(Some(super::admitted(&["azure/eu"]))),
                )
            },
        )
        .await;
        assert_golden(
            &bodies,
            &[
                r#"{"messages":[{"content":"SYSTEM","role":"system"},{"content":"hi","role":"user"}],"model":"openai/gpt-5","provider":{"ignore":["azure/eu"],"require_parameters":true,"zdr":true},"stream":true,"tool_choice":"auto","tools":["read","write","run"]}"#,
            ],
        );
    }
}
