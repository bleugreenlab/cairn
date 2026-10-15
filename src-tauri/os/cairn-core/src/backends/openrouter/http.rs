//! OpenRouter's endpoint, headers, and provider-routing object, and the
//! structured-output constraint its one-shot completions carry.

use crate::models::{OpenRouterRouting, OpenRouterSort};
use serde_json::{json, Value};

pub(super) const CHAT_URL: &str = "https://openrouter.ai/api/v1/chat/completions";

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

/// Inject the native structured-output constraint into an OpenRouter request
/// body for a schema-constrained call (CAIRN-2505). The `response_format`
/// json_schema half is shared with every OpenAI-compatible provider; what is
/// OpenRouter's alone is `provider.require_parameters`, which routes ONLY to
/// providers that honor the schema, so a routed provider can't silently drop it.
/// Cairn's server-side validation of the stored artifact makes a schema
/// violation a loud failure; it cannot see an optional field a constrained
/// decoder foreclosed, which is what the shared wire's required-first property
/// order (`cairn_models::wire::schema_order`) exists to prevent. A model
/// with no schema-capable provider then fails the request loudly (an HTTP error
/// naming the model), which is the intended behavior.
pub(super) fn apply_output_schema(body: &mut Value, schema: Option<&Value>) {
    crate::backends::openai_compat::http::apply_output_schema(body, schema);
    if schema.is_none() {
        return;
    }
    match body.get_mut("provider").and_then(Value::as_object_mut) {
        Some(obj) => {
            obj.insert("require_parameters".to_string(), json!(true));
        }
        None => {
            body["provider"] = json!({ "require_parameters": true });
        }
    }
}

pub(super) fn openrouter_headers(api_key: &str) -> Result<Vec<(String, String)>, String> {
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
