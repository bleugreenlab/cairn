use super::http::build_body;
use super::models::{build_catalog, decode_published_metadata, decode_subscription_model_ids};
use super::{OpenCodeBackend, DEFAULT_MODEL, OPENCODE_BACKEND_KEY};
use crate::agent_process::stdin::MessageContent;
use crate::backends::openai_compat::wire::ChatMessage;
use crate::backends::{AgentBackend, DiscoveredModel, SessionConfig, SessionStart};
use crate::config::presets::default_presets_config;
use crate::models::Model;
use serde_json::json;

/// Shapes taken from the live endpoints on 2026-08-15. The subscription catalog
/// answers with bare ids; models.dev carries the metadata, including the
/// per-model SDK package that names the endpoint family serving it.
const SUBSCRIPTION_BODY: &str = r#"{
  "object": "list",
  "data": [
    {"id": "glm-5.2", "object": "model", "created": 1786821513, "owned_by": "opencode"},
    {"id": "minimax-m3", "object": "model", "created": 1786821513, "owned_by": "opencode"},
    {"id": "grok-4.5", "object": "model", "created": 1786821513, "owned_by": "opencode"},
    {"id": "hy3-preview", "object": "model", "created": 1786821513, "owned_by": "opencode"}
  ]
}"#;

const METADATA_BODY: &str = r#"{
  "anthropic": {"id": "anthropic", "models": {}},
  "opencode-go": {
    "id": "opencode-go",
    "name": "OpenCode Go",
    "api": "https://opencode.ai/zen/go/v1",
    "npm": "@ai-sdk/openai-compatible",
    "models": {
      "glm-5.2": {
        "id": "glm-5.2",
        "name": "GLM-5.2",
        "description": "Z.ai coding model",
        "tool_call": true,
        "reasoning": true,
        "temperature": true,
        "structured_output": true,
        "reasoning_options": [{"type": "effort", "values": ["high", "max"]}],
        "limit": {"context": 1000000, "output": 131072},
        "cost": {"input": 1.4, "output": 4.4, "cache_read": 0.26}
      },
      "minimax-m3": {
        "id": "minimax-m3",
        "name": "MiniMax-M3",
        "description": "MiniMax multimodal coding model",
        "tool_call": true,
        "limit": {"context": 1000000},
        "cost": {"input": 0.3, "output": 1.2},
        "provider": {"npm": "@ai-sdk/anthropic"}
      },
      "grok-4.5": {
        "id": "grok-4.5",
        "name": "Grok 4.5",
        "tool_call": true,
        "limit": {"context": 500000},
        "provider": {"npm": "@ai-sdk/openai"}
      },
      "some-future-model": {
        "id": "some-future-model",
        "name": "Future",
        "tool_call": true,
        "provider": {"npm": "@ai-sdk/google"}
      },
      "toolless-model": {
        "id": "toolless-model",
        "name": "Toolless",
        "description": "Completion-only model",
        "tool_call": false,
        "temperature": true,
        "limit": {"context": 128000}
      }
    }
  }
}"#;

fn catalog() -> Vec<DiscoveredModel> {
    let ids = decode_subscription_model_ids(SUBSCRIPTION_BODY).expect("subscription body decodes");
    let published = decode_published_metadata(METADATA_BODY).expect("metadata body decodes");
    build_catalog(&ids, &published)
}

fn entry<'a>(catalog: &'a [DiscoveredModel], id: &str) -> &'a DiscoveredModel {
    catalog
        .iter()
        .find(|model| model.id == id)
        .unwrap_or_else(|| panic!("{id} is present in the catalog"))
}

#[test]
fn the_subscription_decides_the_line_up_and_its_order() {
    let catalog = catalog();
    let ids: Vec<&str> = catalog.iter().map(|model| model.id.as_str()).collect();
    // Every id the subscription serves is accounted for, in its order — including
    // the one models.dev has no entry for.
    assert_eq!(
        ids,
        vec!["glm-5.2", "minimax-m3", "grok-4.5", "hy3-preview"]
    );
}

#[test]
fn a_chat_completions_model_is_offered_with_its_published_metadata() {
    let catalog = catalog();
    let glm = entry(&catalog, "glm-5.2");

    assert!(!glm.hidden, "a chat/completions model is selectable");
    assert_eq!(glm.display_name, "GLM-5.2");
    assert_eq!(glm.description.as_deref(), Some("Z.ai coding model"));
    assert_eq!(glm.context_window, Some(1_000_000));
    // The effort vocabulary is the model's own, not a ladder Cairn imposes.
    let efforts: Vec<&str> = glm
        .supported_reasoning_efforts
        .iter()
        .map(|effort| effort.reasoning_effort.as_str())
        .collect();
    assert_eq!(efforts, vec!["high", "max"]);
    // `tools` is load-bearing: an agent run is nothing but tool calls.
    assert!(glm.supported_parameters.contains(&"tools".to_string()));
    assert!(glm
        .supported_parameters
        .contains(&"structured_outputs".to_string()));
}

#[test]
fn published_price_per_million_becomes_price_per_token() {
    let catalog = catalog();
    let pricing = entry(&catalog, "glm-5.2")
        .pricing
        .as_ref()
        .expect("a priced model carries pricing");
    // $1.40 per million in, $4.40 out, $0.26 cached read — carried per token, and
    // parseable as a number rather than in scientific notation.
    let prompt: f64 = pricing
        .prompt
        .as_ref()
        .expect("input price")
        .parse()
        .expect("input price parses");
    let completion: f64 = pricing
        .completion
        .as_ref()
        .expect("output price")
        .parse()
        .expect("output price parses");
    let cache_read: f64 = pricing
        .input_cache_read
        .as_ref()
        .expect("cache read price")
        .parse()
        .expect("cache read price parses");
    assert!((prompt * 1_000_000.0 - 1.4).abs() < 1e-9);
    assert!((completion * 1_000_000.0 - 4.4).abs() < 1e-9);
    assert!((cache_read * 1_000_000.0 - 0.26).abs() < 1e-9);
}

#[test]
fn models_on_endpoints_cairn_cannot_speak_are_carried_but_not_offered() {
    let catalog = catalog();

    let minimax = entry(&catalog, "minimax-m3");
    assert!(minimax.hidden, "an Anthropic Messages model is not offered");
    let reason = minimax.description.as_deref().unwrap_or_default();
    assert!(
        reason.contains("Anthropic Messages"),
        "the entry names the endpoint family that serves it: {reason}"
    );
    assert!(
        reason.contains("MiniMax multimodal coding model"),
        "the published description is kept alongside the reason: {reason}"
    );

    let grok = entry(&catalog, "grok-4.5");
    assert!(grok.hidden, "an OpenAI Responses model is not offered");
    assert!(grok
        .description
        .as_deref()
        .unwrap_or_default()
        .contains("OpenAI Responses"));
}

#[test]
fn an_unrecognized_sdk_package_is_not_assumed_compatible() {
    // A package Cairn has no mapping for could be any protocol. Guessing
    // chat/completions would fail in the middle of a session instead of before
    // one starts, so the model is carried unselectable.
    let ids = vec!["some-future-model".to_string()];
    let published = decode_published_metadata(METADATA_BODY).expect("metadata decodes");
    let catalog = build_catalog(&ids, &published);
    assert!(catalog[0].hidden);
}

#[test]
fn a_subscription_id_with_no_published_metadata_is_accounted_for_not_dropped() {
    let catalog = catalog();
    let preview = entry(&catalog, "hy3-preview");
    assert!(preview.hidden);
    assert_eq!(preview.context_window, None);
    assert!(preview
        .description
        .as_deref()
        .unwrap_or_default()
        .contains("has not published its metadata"));
}

/// A chat/completions model whose published metadata says it cannot call tools.
fn toolless_model() -> DiscoveredModel {
    let published = decode_published_metadata(METADATA_BODY).expect("metadata decodes");
    build_catalog(&["toolless-model".to_string()], &published)
        .pop()
        .expect("the fixture yields one model")
}

#[test]
fn a_model_that_cannot_call_tools_stays_available_for_tool_free_completions() {
    let model = toolless_model();
    // Not hidden: `complete` sends no tools, so this model is genuinely usable
    // for one-shot completions. Hiding it would take that away in order to
    // prevent a different mistake.
    assert!(!model.hidden);
    assert_eq!(model.context_window, Some(128_000));
    assert!(!model.supported_parameters.contains(&"tools".to_string()));
    assert!(super::unservable_reason(&model, "toolless-model").is_none());
}

#[test]
fn a_model_that_cannot_call_tools_cannot_run_an_agent_session() {
    // An agent run is nothing but tool calls, so a session started here would
    // either be refused upstream or complete a turn while unable to act. It
    // fails before the run starts, saying what to do about it.
    let reason = super::session_blocker(&toolless_model(), "toolless-model")
        .expect("a model that cannot call tools cannot run a session");
    assert!(reason.contains("does not support tool calling"), "{reason}");
    assert!(reason.contains("Assign a tool-calling model"), "{reason}");
}

#[test]
fn a_tool_calling_model_on_a_served_endpoint_can_start_a_session() {
    let catalog = catalog();
    assert!(super::session_blocker(entry(&catalog, "glm-5.2"), "glm-5.2").is_none());
}

#[test]
fn a_session_on_an_unservable_model_is_refused_with_the_catalogs_own_reason() {
    let catalog = catalog();
    let reason = super::session_blocker(entry(&catalog, "minimax-m3"), "minimax-m3")
        .expect("a model Cairn cannot serve cannot run a session");
    assert!(reason.contains("Anthropic Messages"), "{reason}");
}

#[test]
fn metadata_without_the_provider_entry_fails_rather_than_reading_as_empty() {
    let error = decode_published_metadata(r#"{"anthropic": {"models": {}}}"#)
        .expect_err("an index with no opencode-go provider is a failure");
    assert!(error.contains("opencode-go"), "{error}");
}

fn config_with(
    reasoning_effort: Option<&str>,
    output_schema: Option<serde_json::Value>,
) -> SessionConfig {
    SessionConfig {
        run_id: "run-1".to_string(),
        working_dir: "/tmp".to_string(),
        project_id: "project".to_string(),
        project_key: "GO".to_string(),
        prompt: "hi".to_string(),
        message_content: MessageContent::text("hi"),
        system_prompt_content: None,
        system_prompt_dynamic_tail: None,
        model: None,
        session_start: SessionStart::New {
            session_id: "session-1".to_string(),
        },
        allowed_tools: Vec::new(),
        disallowed_tools: Vec::new(),
        mcp_config_json: "{}".to_string(),
        home_uri: "cairn://p/GO/1/1/builder".to_string(),
        max_thinking_tokens: None,
        reasoning_effort: reasoning_effort.map(str::to_string),
        service_tier: None,
        permissions: crate::backends::AgentPermissions::new(crate::models::Fence::Allow),
        bidirectional: false,
        identity: None,
        output_schema,
        ambient: false,
        is_ephemeral_call: false,
    }
}

#[test]
fn a_streamed_turn_asks_for_the_usage_that_meters_the_subscription() {
    let body = build_body(
        "glm-5.2",
        &[ChatMessage::user("hi".to_string())],
        &config_with(None, None),
    );
    assert_eq!(body["stream"], json!(true));
    // Without this an OpenAI-compatible stream reports no token counts at all,
    // and a dollar-metered subscription has nothing to account against.
    assert_eq!(body["stream_options"]["include_usage"], json!(true));
    assert_eq!(body["tool_choice"], json!("auto"));
    assert!(body["tools"]
        .as_array()
        .is_some_and(|tools| !tools.is_empty()));
    assert!(body.get("reasoning_effort").is_none());
}

#[test]
fn the_configured_effort_reaches_the_provider_as_written() {
    // Go's models publish different effort vocabularies, so Cairn passes the
    // configured value through rather than mapping it onto a common ladder.
    let body = build_body(
        "kimi-k3",
        &[ChatMessage::user("hi".to_string())],
        &config_with(Some("max"), None),
    );
    assert_eq!(body["reasoning_effort"], json!("max"));
}

#[test]
fn a_schema_constrained_run_carries_the_strict_response_format() {
    let schema = json!({"type": "object", "properties": {"answer": {"type": "string"}}});
    let body = build_body(
        "glm-5.2",
        &[ChatMessage::user("hi".to_string())],
        &config_with(None, Some(schema.clone())),
    );
    assert_eq!(
        body["response_format"]["json_schema"]["strict"],
        json!(true)
    );
    assert_eq!(body["response_format"]["json_schema"]["schema"], schema);
}

#[test]
fn the_backend_runs_its_agentic_loop_in_process() {
    let backend = OpenCodeBackend;
    assert_eq!(backend.name(), "OpenCode Go");
    assert!(backend.supports_resume());
    assert!(!backend.supports_warm_processes());
    assert!(matches!(
        backend.call_batch_capability().shape,
        crate::backends::CallBatchShape::InProcess
    ));
    assert!(matches!(
        backend.completion_shape(),
        crate::backends::CompletionShape::InProcess
    ));
}

#[test]
fn the_backend_factory_resolves_the_go_key() {
    let backend = crate::backends::backend_for_name(Some(OPENCODE_BACKEND_KEY));
    assert_eq!(backend.name(), "OpenCode Go");
}

#[test]
fn default_presets_address_go_models_through_tiers() {
    // A Go model id carries no provider in its name, so a tier ref is how a Go
    // model is named to the runtime at all. Every tier must therefore resolve.
    let config = default_presets_config(None);
    let presets = config
        .backends
        .get(OPENCODE_BACKEND_KEY)
        .expect("Go defines default presets");
    for tier in ["sm", "md", "lg"] {
        let preset = presets
            .get(tier)
            .unwrap_or_else(|| panic!("Go defines the {tier} tier"));
        let resolved = crate::config::presets::resolve_preset(
            &format!("{OPENCODE_BACKEND_KEY}/{tier}"),
            &config,
        )
        .expect("a qualified Go tier resolves");
        assert_eq!(resolved.backend, OPENCODE_BACKEND_KEY);
        assert_eq!(resolved.model, preset.model);
    }
    assert_eq!(presets["md"].model, Model::new(DEFAULT_MODEL));
}

#[test]
fn no_default_preset_points_at_a_region_gated_model() {
    // DeepSeek's Go models are the cheapest on offer but are served only from
    // China-hosted infrastructure behind a per-workspace opt-in, so a default
    // naming one fails for anyone who has not opted in.
    let config = default_presets_config(None);
    for preset in config.backends[OPENCODE_BACKEND_KEY].values() {
        assert!(
            !preset.model.as_str().starts_with("deepseek"),
            "{} is region-gated and cannot be a default",
            preset.model
        );
    }
}
