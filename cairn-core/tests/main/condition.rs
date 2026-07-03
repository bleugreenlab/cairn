//! Integration tests for cairn_core::condition programmatic evaluation.
//!
//! Tests edge cases in expression evaluation, type handling, and error paths.

use cairn_core::condition::evaluate_programmatic_condition;
use serde_json::json;

#[test]
fn integer_result_selects_port_by_index() {
    let data = json!({"choice": 1});
    let ports = vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()];

    let result = evaluate_programmatic_condition("choice", &data, &ports).unwrap();
    assert_eq!(result, "beta");
}

#[test]
fn float_result_floors_to_index() {
    let data = json!({"score": 1.7});
    let ports = vec!["low".to_string(), "medium".to_string(), "high".to_string()];

    let result = evaluate_programmatic_condition("score", &data, &ports).unwrap();
    // floor(1.7) = 1 → "medium"
    assert_eq!(result, "medium");
}

#[test]
fn boolean_true_no_ports_errors() {
    let data = json!({"flag": true});
    let ports: Vec<String> = vec![];

    let result = evaluate_programmatic_condition("flag", &data, &ports);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("No ports defined"));
}

#[test]
fn boolean_false_single_port_errors() {
    let data = json!({"flag": false});
    let ports = vec!["only_one".to_string()];

    let result = evaluate_programmatic_condition("flag", &data, &ports);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("at least 2 ports"));
}

#[test]
fn out_of_range_index_errors() {
    let data = json!({"idx": 5});
    let ports = vec!["a".to_string(), "b".to_string()];

    let result = evaluate_programmatic_condition("idx", &data, &ports);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("out of range"));
}

#[test]
fn string_no_match_errors() {
    let data = json!({"severity": "critical"});
    let ports = vec!["low".to_string(), "medium".to_string(), "high".to_string()];

    let result = evaluate_programmatic_condition("severity", &data, &ports);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.contains("critical"));
    assert!(err.contains("doesn't match any port"));
}

#[test]
fn case_insensitive_string_matching() {
    let data = json!({"level": "HIGH"});
    let ports = vec!["low".to_string(), "medium".to_string(), "high".to_string()];

    let result = evaluate_programmatic_condition("level", &data, &ports).unwrap();
    assert_eq!(result, "high");
}

#[test]
fn empty_json_with_expression() {
    let data = json!({});
    let ports = vec!["yes".to_string(), "no".to_string()];

    // Referencing a non-existent variable should produce an evaluation error
    let result = evaluate_programmatic_condition("missing_field > 1", &data, &ports);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("evaluation error"));
}

#[test]
fn array_length_in_expression() {
    let data = json!({
        "files": ["a.rs", "b.rs", "c.rs"]
    });
    let ports = vec!["many_files".to_string(), "few_files".to_string()];

    let result = evaluate_programmatic_condition("files.length > 1", &data, &ports).unwrap();
    assert_eq!(result, "many_files");
}

#[test]
fn nested_boolean_field() {
    let data = json!({
        "plan": {
            "has_breaking_changes": true
        }
    });
    let ports = vec!["breaking".to_string(), "safe".to_string()];

    let result =
        evaluate_programmatic_condition("plan.has_breaking_changes == true", &data, &ports)
            .unwrap();
    assert_eq!(result, "breaking");
}
