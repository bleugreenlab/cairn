//! Condition evaluation module for recipe branching.
//!
//! Supports two evaluation modes:
//! - Programmatic: Uses evalexpr to evaluate expressions against artifact data
//! - AI: The workflow host asks a model the node's question; this module
//!   matches the answer to a port

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

/// Match a model's answer to an AI condition onto exactly one of its ports.
///
/// Case-insensitive. An answer that equals a port wins outright. Otherwise the
/// answer must name exactly one port as whole words, so a chatty "Yes." routes
/// to `yes` while `no` does not fire inside "not". An answer naming two ports
/// ("no, not yes") is an error rather than a guess. A port mentioned only
/// inside a longer port's mention does not count on its own, so with ports
/// `approve` and `approve with changes`, "Approve with changes." names one.
pub fn match_ai_condition_answer(answer: &str, ports: &[String]) -> Result<String, String> {
    let trimmed = answer.trim();
    let normalized = trimmed.to_lowercase();

    if let Some(port) = ports.iter().find(|port| normalized == port.to_lowercase()) {
        return Ok(port.clone());
    }

    let answer_tokens = answer_tokens(&normalized);
    // (port index, start, end) for every contiguous occurrence of a port's
    // token sequence in the answer.
    let mut spans: Vec<(usize, usize, usize)> = Vec::new();
    for (index, port) in ports.iter().enumerate() {
        let port_tokens = answer_tokens_of(port);
        if port_tokens.is_empty() || port_tokens.len() > answer_tokens.len() {
            continue;
        }
        for start in 0..=answer_tokens.len() - port_tokens.len() {
            let end = start + port_tokens.len();
            if answer_tokens[start..end] == port_tokens[..] {
                spans.push((index, start, end));
            }
        }
    }

    let mut mentioned: Vec<usize> = Vec::new();
    for &(index, start, end) in &spans {
        let inside_longer = spans.iter().any(|&(other, other_start, other_end)| {
            other != index
                && other_start <= start
                && end <= other_end
                && other_end - other_start > end - start
        });
        if !inside_longer && !mentioned.contains(&index) {
            mentioned.push(index);
        }
    }
    mentioned.sort_unstable();

    match mentioned.as_slice() {
        [only] => Ok(ports[*only].clone()),
        [] => Err(format!(
            "Response '{trimmed}' didn't match any port: {ports:?}"
        )),
        several => {
            let found = several
                .iter()
                .map(|index| ports[*index].as_str())
                .collect::<Vec<_>>()
                .join(", ");
            Err(format!(
                "Response '{trimmed}' names more than one port ({found}); expected exactly one of {ports:?}"
            ))
        }
    }
}

/// Words of `text` for port matching: runs of alphanumerics, `_` and `-`.
fn answer_tokens(text: &str) -> Vec<&str> {
    text.split(|c: char| !(c.is_alphanumeric() || c == '_' || c == '-'))
        .filter(|token| !token.is_empty())
        .collect()
}

fn answer_tokens_of(port: &str) -> Vec<String> {
    answer_tokens(&port.to_lowercase())
        .into_iter()
        .map(str::to_string)
        .collect()
}

/// Gather context from upstream artifacts for condition evaluation.
pub fn serialize_context_for_ai(artifact_data: &Value) -> String {
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

    fn ports(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| name.to_string()).collect()
    }

    #[test]
    fn ai_answer_matches_port_exactly() {
        let ports = ports(&["high_risk", "low_risk"]);
        assert_eq!(
            match_ai_condition_answer("high_risk", &ports),
            Ok("high_risk".to_string())
        );
    }

    #[test]
    fn ai_answer_matches_case_insensitively_and_trimmed() {
        let ports = ports(&["high_risk", "low_risk"]);
        assert_eq!(
            match_ai_condition_answer("  LOW_RISK\n", &ports),
            Ok("low_risk".to_string())
        );
    }

    #[test]
    fn ai_answer_matches_a_port_named_as_a_whole_word() {
        let ports = ports(&["yes", "no"]);
        assert_eq!(
            match_ai_condition_answer("Yes.", &ports),
            Ok("yes".to_string())
        );
    }

    #[test]
    fn ai_answer_naming_two_ports_is_ambiguous() {
        let ports = ports(&["yes", "no"]);
        let error = match_ai_condition_answer("no, not yes", &ports).unwrap_err();
        assert!(error.contains("more than one port"), "{error}");
        assert!(error.contains("(yes, no)"), "{error}");
    }

    #[test]
    fn ai_answer_does_not_match_a_port_inside_a_word() {
        let ports = ports(&["yes", "no"]);
        let error = match_ai_condition_answer("not", &ports).unwrap_err();
        assert!(error.contains("didn't match any port"), "{error}");
        assert!(error.contains("'not'"), "{error}");
    }

    #[test]
    fn ai_answer_matches_a_multi_word_port() {
        let ports = ports(&["needs work", "ship"]);
        assert_eq!(
            match_ai_condition_answer("Needs work.", &ports),
            Ok("needs work".to_string())
        );
    }

    #[test]
    fn ai_answer_prefers_the_longer_port_it_names_over_its_prefix() {
        let ports = ports(&["approve", "approve with changes"]);
        assert_eq!(
            match_ai_condition_answer("Approve with changes.", &ports),
            Ok("approve with changes".to_string())
        );
        let error =
            match_ai_condition_answer("approve, not approve with changes", &ports).unwrap_err();
        assert!(error.contains("(approve, approve with changes)"), "{error}");
    }

    #[test]
    fn ai_answer_prefers_an_exact_port_over_a_contained_one() {
        let ports = ports(&["no", "not_sure"]);
        assert_eq!(
            match_ai_condition_answer("not_sure", &ports),
            Ok("not_sure".to_string())
        );
    }

    #[test]
    fn ai_answer_without_a_port_is_an_error() {
        let ports = ports(&["approve", "reject"]);
        let error = match_ai_condition_answer("maybe", &ports).unwrap_err();
        assert!(error.contains("didn't match any port"), "{error}");
    }
}
