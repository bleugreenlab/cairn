//! The streaming chat-completions POST and its SSE reader: build the request
//! body (tools, provider routing, structured-output schema), stream deltas into
//! the live assistant stream and the response aggregator, observe cancel at line
//! boundaries, and detect in-band errors.

use super::OPENROUTER_BACKEND_KEY;
use crate::backends::openai_compat::http::{post_chat_completion_streaming, Endpoint};
use crate::backends::openai_compat::wire::{ChatMessage, ChatResponse};
use crate::backends::SessionConfig;
use crate::models::{OpenRouterRouting, OpenRouterSort};
use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;
use serde_json::{json, Value};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

const CHAT_URL: &str = "https://openrouter.ai/api/v1/chat/completions";
// Bound on a single streaming generation request. Each tool-loop iteration is
// one POST for one assistant response (the loop re-POSTs per tool round-trip),
// so this caps one generation, not a whole multi-iteration turn. A stalled
// upstream — including an over-limit request the provider hangs on — hits this
// and surfaces a clear error instead of hanging forever (the old `timeout(None)`
// waited indefinitely). Sized generously so a high-effort reasoning generation
// is never mistaken for a hang.

/// Build the OpenRouter `provider` object from routing settings.
///
/// `require_parameters` is always sent: tool schemas are unconditional, so this
/// only formalizes the existing effective behavior (route only to tool-capable
/// providers). `zdr`/`sort` are added only when the user opts in.
pub(super) fn build_provider_object(routing: &OpenRouterRouting) -> Value {
    let mut provider = json!({ "require_parameters": true });
    if routing.zero_data_retention {
        provider["zdr"] = json!(true);
    }
    if let Some(sort) = routing.sort {
        provider["sort"] = json!(match sort {
            OpenRouterSort::Price => "price",
            OpenRouterSort::Throughput => "throughput",
            OpenRouterSort::Latency => "latency",
        });
    }
    provider
}

#[allow(clippy::too_many_arguments)]
pub(super) fn post_chat_completion(
    orch: &Orchestrator,
    run_db: &Arc<LocalDb>,
    api_key: &str,
    model: &str,
    session_id: &str,
    messages: &[ChatMessage],
    config: &SessionConfig,
    run_id: &str,
    turn_id: Option<&str>,
    provider: &Value,
    cancel: &Arc<AtomicBool>,
) -> Result<ChatResponse, String> {
    let mut body = json!({
        "model": model,
        "messages": messages,
        "tools": crate::backends::openai_compat::tool_schemas(),
        "tool_choice": "auto",
        "stream": true,
    });
    if let Some(effort) = config.reasoning_effort.as_deref() {
        body["reasoning"] = json!({ "effort": effort });
    }
    body["provider"] = provider.clone();
    apply_output_schema(&mut body, config.output_schema.as_ref());
    let endpoint = Endpoint {
        provider_name: super::OPENROUTER_BACKEND_NAME,
        backend_key: OPENROUTER_BACKEND_KEY,
        chat_url: CHAT_URL.to_string(),
        headers: openrouter_headers(api_key)?,
        extra_body: None,
    };
    post_chat_completion_streaming(
        orch, run_db, &endpoint, body, run_id, session_id, turn_id, cancel,
    )
}

/// Inject the native structured-output constraint into an OpenRouter request
/// body for a schema-constrained call (CAIRN-2505). `response_format` json_schema
/// with `strict` demands conformance; `provider.require_parameters` routes ONLY
/// to providers that honor it, so a routed provider can't silently drop the
/// schema. Cairn's server-side validation of the stored artifact is the backstop
/// if one still does — non-conformance is a loud failure, never corrupt data. A
/// model with no schema-capable provider then fails the request loudly (an HTTP
/// error naming the model), which is the intended behavior. A no-op when the run
/// carries no output schema, leaving schema-less sessions bit-for-bit unchanged.
fn apply_output_schema(body: &mut Value, schema: Option<&Value>) {
    let Some(schema) = schema else {
        return;
    };
    body["response_format"] = json!({
        "type": "json_schema",
        "json_schema": {
            "name": "cairn_output",
            "strict": true,
            "schema": schema,
        }
    });
    match body.get_mut("provider").and_then(Value::as_object_mut) {
        Some(obj) => {
            obj.insert("require_parameters".to_string(), json!(true));
        }
        None => {
            body["provider"] = json!({ "require_parameters": true });
        }
    }
}

fn openrouter_headers(api_key: &str) -> Result<Vec<(String, String)>, String> {
    Ok(vec![
        ("authorization".to_string(), format!("Bearer {api_key}")),
        ("content-type".to_string(), "application/json".to_string()),
        (
            "http-referer".to_string(),
            "https://cairn.computer".to_string(),
        ),
        ("x-openrouter-title".to_string(), "Cairn".to_string()),
    ])
}

#[cfg(test)]
mod tests {
    use super::apply_output_schema;
    use serde_json::json;

    #[test]
    fn no_schema_leaves_body_unconstrained() {
        let mut body = json!({ "model": "m", "provider": { "sort": "throughput" } });
        apply_output_schema(&mut body, None);
        assert!(body.get("response_format").is_none());
        // The existing provider object is untouched.
        assert!(body["provider"].get("require_parameters").is_none());
    }

    #[test]
    fn schema_sets_response_format_and_require_parameters() {
        let schema = json!({
            "type": "object",
            "required": ["answer"],
            "properties": {"answer": {"type": "string"}}
        });
        let mut body = json!({ "model": "m", "provider": { "sort": "throughput" } });
        apply_output_schema(&mut body, Some(&schema));

        assert_eq!(body["response_format"]["type"], "json_schema");
        assert_eq!(body["response_format"]["json_schema"]["strict"], true);
        assert_eq!(body["response_format"]["json_schema"]["schema"], schema);
        // require_parameters is merged into the existing provider object, not
        // clobbering its routing prefs.
        assert_eq!(body["provider"]["require_parameters"], true);
        assert_eq!(body["provider"]["sort"], "throughput");
    }

    #[test]
    fn schema_creates_provider_object_when_absent() {
        let schema = json!({ "type": "object" });
        let mut body = json!({ "model": "m" });
        apply_output_schema(&mut body, Some(&schema));
        assert_eq!(body["provider"]["require_parameters"], true);
    }
}
