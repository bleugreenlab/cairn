//! Condition evaluation module for recipe branching.
//!
//! Supports two evaluation modes:
//! - Programmatic: Uses evalexpr to evaluate expressions against artifact data
//! - AI: Uses Claude (Haiku) to answer questions and route to ports

use evalexpr::ContextWithMutableVariables;
use serde_json::Value;
use std::collections::HashMap;

/// Evaluate a programmatic condition expression against artifact data.
///
/// Supports simple expressions like:
/// - `plan.risk_score > 7`
/// - `implementation.files_changed > 10`
/// - `analysis.has_breaking_changes == true`
///
/// Returns the matching port name based on the expression result.
pub fn evaluate_programmatic_condition(
    expression: &str,
    artifact_data: &Value,
    ports: &[String],
) -> Result<String, String> {
    // Build a context with flattened artifact data
    let context = build_evalexpr_context(artifact_data)?;

    // Evaluate the expression
    let result = evalexpr::eval_with_context(expression, &context)
        .map_err(|e| format!("Expression evaluation error: {}", e))?;

    // Determine which port to activate based on result
    match result {
        evalexpr::Value::Boolean(true) => {
            // For boolean true, use first port (typically "yes" or "true")
            ports
                .first()
                .cloned()
                .ok_or_else(|| "No ports defined".to_string())
        }
        evalexpr::Value::Boolean(false) => {
            // For boolean false, use second port (typically "no" or "false")
            ports
                .get(1)
                .cloned()
                .ok_or_else(|| "Need at least 2 ports for boolean conditions".to_string())
        }
        evalexpr::Value::String(s) => {
            // For string results, match against port names (case-insensitive)
            let s_lower = s.to_lowercase();
            ports
                .iter()
                .find(|p| p.to_lowercase() == s_lower)
                .cloned()
                .ok_or_else(|| format!("Result '{}' doesn't match any port: {:?}", s, ports))
        }
        evalexpr::Value::Int(i) => {
            // For integer results, use as index into ports array
            let idx = i as usize;
            ports
                .get(idx)
                .cloned()
                .ok_or_else(|| format!("Index {} out of range for ports: {:?}", idx, ports))
        }
        evalexpr::Value::Float(f) => {
            // For float results, use floor as index
            let idx = f.floor() as usize;
            ports
                .get(idx)
                .cloned()
                .ok_or_else(|| format!("Index {} out of range for ports: {:?}", idx, ports))
        }
        _ => Err(format!("Unsupported expression result type: {:?}", result)),
    }
}

/// Build an evalexpr context from JSON artifact data.
///
/// Flattens nested JSON into dot-notation variables:
/// `{"plan": {"risk_score": 7}}` -> `plan.risk_score = 7`
fn build_evalexpr_context(data: &Value) -> Result<evalexpr::HashMapContext, String> {
    let mut context = evalexpr::HashMapContext::new();
    let flat = flatten_json(data, "");

    for (key, value) in flat {
        let eval_value = json_to_evalexpr_value(&value);
        context
            .set_value(key, eval_value)
            .map_err(|e| format!("Failed to set context value: {}", e))?;
    }

    Ok(context)
}

/// Flatten nested JSON into dot-notation keys.
fn flatten_json(value: &Value, prefix: &str) -> HashMap<String, Value> {
    let mut result = HashMap::new();

    match value {
        Value::Object(map) => {
            for (k, v) in map {
                let new_prefix = if prefix.is_empty() {
                    k.clone()
                } else {
                    format!("{}.{}", prefix, k)
                };
                result.extend(flatten_json(v, &new_prefix));
            }
        }
        Value::Array(arr) => {
            for (i, v) in arr.iter().enumerate() {
                let new_prefix = format!("{}[{}]", prefix, i);
                result.extend(flatten_json(v, &new_prefix));
            }
            // Also set the array length
            if !prefix.is_empty() {
                result.insert(
                    format!("{}.length", prefix),
                    Value::Number(arr.len().into()),
                );
            }
        }
        _ => {
            if !prefix.is_empty() {
                result.insert(prefix.to_string(), value.clone());
            }
        }
    }

    result
}

/// Convert a JSON value to an evalexpr value.
fn json_to_evalexpr_value(value: &Value) -> evalexpr::Value {
    match value {
        Value::Null => evalexpr::Value::Empty,
        Value::Bool(b) => evalexpr::Value::Boolean(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                evalexpr::Value::Int(i)
            } else if let Some(f) = n.as_f64() {
                evalexpr::Value::Float(f)
            } else {
                evalexpr::Value::Empty
            }
        }
        Value::String(s) => evalexpr::Value::String(s.clone()),
        Value::Array(_) | Value::Object(_) => {
            // Complex types become strings for comparison
            evalexpr::Value::String(value.to_string())
        }
    }
}

/// Evaluate an AI condition by asking a model a question.
///
/// Uses a one-shot completion to get the model to answer with exactly one of the port names.
/// Dispatches to the appropriate backend (Claude/Codex) based on the model.
pub(crate) fn evaluate_ai_condition(
    completion: &dyn crate::services::CompletionService,
    question: &str,
    ports: &[String],
    context: &str,
    model: Option<&str>,
) -> Result<String, String> {
    let port_list = ports.join(", ");
    let prompt = format!(
        "Answer with EXACTLY one word from this list: {}\n\n\
         Context:\n{}\n\n\
         Question: {}\n\n\
         Answer:",
        port_list, context, question
    );

    let response = completion.complete(crate::services::CompletionRequest {
        prompt,
        model: Some(model.unwrap_or("haiku").to_string()),
        backend: None,
        output_format: crate::services::OutputFormat::Text,
    })?;

    let response = response.text.trim().to_lowercase();

    // Match response to port (case-insensitive, partial match)
    for port in ports {
        if response.contains(&port.to_lowercase()) {
            return Ok(port.clone());
        }
    }

    // Try exact match as fallback
    for port in ports {
        if response == port.to_lowercase() {
            return Ok(port.clone());
        }
    }

    Err(format!(
        "Response '{}' didn't match any port: {:?}",
        response, ports
    ))
}

/// Gather context from upstream artifacts for condition evaluation.
pub(crate) fn serialize_context_for_ai(artifact_data: &Value) -> String {
    serde_json::to_string_pretty(artifact_data).unwrap_or_else(|_| "{}".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_flatten_json() {
        let data = json!({
            "plan": {
                "risk_score": 7,
                "has_breaking_changes": true
            },
            "files": ["a.rs", "b.rs"]
        });

        let flat = flatten_json(&data, "");

        assert_eq!(flat.get("plan.risk_score"), Some(&json!(7)));
        assert_eq!(flat.get("plan.has_breaking_changes"), Some(&json!(true)));
        assert_eq!(flat.get("files[0]"), Some(&json!("a.rs")));
        assert_eq!(flat.get("files.length"), Some(&json!(2)));
    }

    #[test]
    fn test_evaluate_boolean_condition() {
        let data = json!({
            "plan": {
                "risk_score": 8
            }
        });
        let ports = vec!["high_risk".to_string(), "low_risk".to_string()];

        let result = evaluate_programmatic_condition("plan.risk_score > 7", &data, &ports);
        assert_eq!(result, Ok("high_risk".to_string()));

        let result = evaluate_programmatic_condition("plan.risk_score > 10", &data, &ports);
        assert_eq!(result, Ok("low_risk".to_string()));
    }

    #[test]
    fn test_evaluate_string_condition() {
        let data = json!({
            "analysis": {
                "severity": "high"
            }
        });
        let ports = vec!["low".to_string(), "medium".to_string(), "high".to_string()];

        let result = evaluate_programmatic_condition("analysis.severity", &data, &ports);
        assert_eq!(result, Ok("high".to_string()));
    }

    #[test]
    fn test_context_serialization() {
        let data = json!({
            "plan": {"title": "Add feature"},
            "analysis": {"risk": "low"}
        });

        let context = serialize_context_for_ai(&data);
        assert!(context.contains("Add feature"));
        assert!(context.contains("risk"));
    }

    #[test]
    fn test_ai_condition_matches_port() {
        use crate::services::completion::{CompletionResponse, MockCompletionService};

        let mut mock = MockCompletionService::new();
        mock.expect_complete().returning(|_req| {
            Ok(CompletionResponse {
                text: "high_risk".to_string(),
            })
        });

        let ports = vec!["high_risk".to_string(), "low_risk".to_string()];
        let result = evaluate_ai_condition(&mock, "Is this risky?", &ports, "some context", None);
        assert_eq!(result, Ok("high_risk".to_string()));
    }

    #[test]
    fn test_ai_condition_case_insensitive_match() {
        use crate::services::completion::{CompletionResponse, MockCompletionService};

        let mut mock = MockCompletionService::new();
        mock.expect_complete().returning(|_req| {
            Ok(CompletionResponse {
                text: "LOW_RISK".to_string(),
            })
        });

        let ports = vec!["high_risk".to_string(), "low_risk".to_string()];
        let result = evaluate_ai_condition(&mock, "Is this risky?", &ports, "context", None);
        assert_eq!(result, Ok("low_risk".to_string()));
    }

    #[test]
    fn test_ai_condition_no_match() {
        use crate::services::completion::{CompletionResponse, MockCompletionService};

        let mut mock = MockCompletionService::new();
        mock.expect_complete().returning(|_req| {
            Ok(CompletionResponse {
                text: "maybe".to_string(),
            })
        });

        let ports = vec!["approve".to_string(), "reject".to_string()];
        let result = evaluate_ai_condition(&mock, "Should we?", &ports, "ctx", None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("didn't match any port"));
    }

    #[test]
    fn test_ai_condition_service_error() {
        use crate::services::completion::MockCompletionService;

        let mut mock = MockCompletionService::new();
        mock.expect_complete()
            .returning(|_req| Err("connection refused".to_string()));

        let ports = vec!["yes".to_string(), "no".to_string()];
        let result = evaluate_ai_condition(&mock, "Question?", &ports, "ctx", None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("connection refused"));
    }

    #[test]
    fn test_ai_condition_passes_model() {
        use crate::services::completion::{CompletionResponse, MockCompletionService};

        let mut mock = MockCompletionService::new();
        mock.expect_complete().returning(|req| {
            // Verify the model was passed through
            assert_eq!(req.model, Some("gpt-5.4-mini".to_string()));
            Ok(CompletionResponse {
                text: "yes".to_string(),
            })
        });

        let ports = vec!["yes".to_string(), "no".to_string()];
        let result = evaluate_ai_condition(&mock, "Question?", &ports, "ctx", Some("gpt-5.4-mini"));
        assert_eq!(result, Ok("yes".to_string()));
    }
}
