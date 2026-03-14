//! Trigger evaluation logic for memory matching.
//!
//! Given a JSON value (hook stdin) and a list of memories with triggers,
//! returns which memories match.

use regex::Regex;
use std::collections::HashMap;

use crate::models::{Memory, MemoryTrigger};

/// Extract a value from a JSON object using simple dot-notation path.
///
/// Supports paths like:
/// - `$.tool_name` → `json["tool_name"]`
/// - `$.tool_input.file_path` → `json["tool_input"]["file_path"]`
/// - `$.error` → `json["error"]`
///
/// Returns the string representation of the value, or None if path doesn't exist.
fn extract_json_path(value: &serde_json::Value, path: &str) -> Option<String> {
    // Strip leading "$." if present
    let path = path.strip_prefix("$.").unwrap_or(path);

    let mut current = value;
    for segment in path.split('.') {
        current = current.get(segment)?;
    }

    // Convert to string representation
    match current {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Null => None,
        other => Some(other.to_string()),
    }
}

/// Check if a single trigger condition matches against the hook input.
fn condition_matches(hook_input: &serde_json::Value, json_path: &str, pattern: &Regex) -> bool {
    match extract_json_path(hook_input, json_path) {
        Some(value) => pattern.is_match(&value),
        None => false,
    }
}

/// Check if a memory matches the given hook input by compiling and evaluating its triggers.
///
/// Logic:
/// - Within a trigger group (same trigger_index): ALL conditions must match (AND)
/// - Across trigger groups (different indices): ANY group matching suffices (OR)
///
/// Returns false if the memory has no triggers or any trigger has an invalid regex.
fn memory_matches(memory: &Memory, hook_input: &serde_json::Value) -> bool {
    if memory.triggers.is_empty() {
        return false;
    }

    // Compile triggers, grouped by trigger_index
    let mut trigger_groups: HashMap<i32, Vec<(&MemoryTrigger, Regex)>> = HashMap::new();
    for trigger in &memory.triggers {
        let regex = match Regex::new(&trigger.pattern) {
            Ok(r) => r,
            Err(_) => return false,
        };
        trigger_groups
            .entry(trigger.trigger_index)
            .or_default()
            .push((trigger, regex));
    }

    trigger_groups.values().any(|conditions| {
        conditions
            .iter()
            .all(|(trigger, regex)| condition_matches(hook_input, &trigger.json_path, regex))
    })
}

/// Match memories against hook input, returning indices of matched memories.
///
/// Memories with invalid regex patterns in their triggers are silently skipped.
pub fn match_memories(hook_input: &serde_json::Value, memories: &[Memory]) -> Vec<usize> {
    memories
        .iter()
        .enumerate()
        .filter_map(|(i, memory)| {
            if memory_matches(memory, hook_input) {
                Some(i)
            } else {
                None
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{MemoryConfidence, MemoryTrigger};

    fn make_memory(content: &str, triggers: Vec<MemoryTrigger>) -> Memory {
        Memory {
            id: "test-memory".to_string(),
            project_id: None,
            content: content.to_string(),
            confidence: MemoryConfidence::Tentative,
            source_issue: None,
            created_at: 0,
            updated_at: 0,
            surfaced_count: 0,
            last_surfaced_at: None,
            active: true,
            triggers,
        }
    }

    fn make_trigger(index: i32, json_path: &str, pattern: &str) -> MemoryTrigger {
        MemoryTrigger {
            id: 0,
            memory_id: "test-memory".to_string(),
            trigger_index: index,
            json_path: json_path.to_string(),
            pattern: pattern.to_string(),
        }
    }

    #[test]
    fn test_extract_json_path_simple() {
        let json = serde_json::json!({"tool_name": "Write"});
        assert_eq!(
            extract_json_path(&json, "$.tool_name"),
            Some("Write".to_string())
        );
    }

    #[test]
    fn test_extract_json_path_nested() {
        let json = serde_json::json!({
            "tool_input": {"file_path": "/src/schema.rs"}
        });
        assert_eq!(
            extract_json_path(&json, "$.tool_input.file_path"),
            Some("/src/schema.rs".to_string())
        );
    }

    #[test]
    fn test_extract_json_path_missing() {
        let json = serde_json::json!({"tool_name": "Write"});
        assert_eq!(extract_json_path(&json, "$.nonexistent"), None);
    }

    #[test]
    fn test_extract_json_path_null_value() {
        let json = serde_json::json!({"error": null});
        assert_eq!(extract_json_path(&json, "$.error"), None);
    }

    #[test]
    fn test_extract_json_path_number() {
        let json = serde_json::json!({"count": 42});
        assert_eq!(extract_json_path(&json, "$.count"), Some("42".to_string()));
    }

    #[test]
    fn test_extract_json_path_without_dollar_prefix() {
        let json = serde_json::json!({"tool_name": "Write"});
        assert_eq!(
            extract_json_path(&json, "tool_name"),
            Some("Write".to_string())
        );
    }

    #[test]
    fn test_single_condition_match() {
        let memory = make_memory("Test memory", vec![make_trigger(0, "$.tool_name", "Write")]);
        let input = serde_json::json!({"tool_name": "Write"});
        let matches = match_memories(&input, &[memory]);
        assert_eq!(matches, vec![0]);
    }

    #[test]
    fn test_single_condition_no_match() {
        let memory = make_memory("Test memory", vec![make_trigger(0, "$.tool_name", "Write")]);
        let input = serde_json::json!({"tool_name": "Read"});
        let matches = match_memories(&input, &[memory]);
        assert!(matches.is_empty());
    }

    #[test]
    fn test_and_within_trigger_group() {
        let memory = make_memory(
            "Test memory",
            vec![
                make_trigger(0, "$.tool_name", "Edit|Write"),
                make_trigger(0, "$.tool_input.file_path", "schema\\.rs$"),
            ],
        );

        // Both match
        let input = serde_json::json!({
            "tool_name": "Write",
            "tool_input": {"file_path": "/src/schema.rs"}
        });
        assert_eq!(match_memories(&input, &[memory.clone()]), vec![0]);

        // Only tool_name matches
        let input = serde_json::json!({
            "tool_name": "Write",
            "tool_input": {"file_path": "/src/main.rs"}
        });
        assert!(match_memories(&input, &[memory]).is_empty());
    }

    #[test]
    fn test_or_across_trigger_groups() {
        let memory = make_memory(
            "Test memory",
            vec![
                // Trigger group 0: Write to schema.rs
                make_trigger(0, "$.tool_name", "Write"),
                make_trigger(0, "$.tool_input.file_path", "schema\\.rs$"),
                // Trigger group 1: Read server.rs
                make_trigger(1, "$.tool_name", "Read"),
                make_trigger(1, "$.tool_input.file_path", "server\\.rs$"),
            ],
        );

        // Match trigger group 1 (doesn't need group 0)
        let input = serde_json::json!({
            "tool_name": "Read",
            "tool_input": {"file_path": "/src/server.rs"}
        });
        assert_eq!(match_memories(&input, &[memory]), vec![0]);
    }

    #[test]
    fn test_missing_json_path_doesnt_crash() {
        let memory = make_memory(
            "Test memory",
            vec![make_trigger(0, "$.nonexistent.deep.path", "anything")],
        );
        let input = serde_json::json!({"tool_name": "Write"});
        let matches = match_memories(&input, &[memory]);
        assert!(matches.is_empty());
    }

    #[test]
    fn test_invalid_regex_skips_memory() {
        let memory = make_memory(
            "Test memory",
            vec![make_trigger(0, "$.tool_name", "[invalid regex")],
        );
        let input = serde_json::json!({"tool_name": "Write"});
        let matches = match_memories(&input, &[memory]);
        assert!(matches.is_empty());
    }

    #[test]
    fn test_no_triggers_no_match() {
        let memory = make_memory("Test memory", vec![]);
        let input = serde_json::json!({"tool_name": "Write"});
        let matches = match_memories(&input, &[memory]);
        assert!(matches.is_empty());
    }

    #[test]
    fn test_multiple_memories() {
        let memories = vec![
            make_memory("Memory A", vec![make_trigger(0, "$.tool_name", "Write")]),
            make_memory("Memory B", vec![make_trigger(0, "$.tool_name", "Read")]),
            make_memory(
                "Memory C",
                vec![make_trigger(0, "$.tool_name", "Write|Edit")],
            ),
        ];
        let input = serde_json::json!({"tool_name": "Write"});
        let matches = match_memories(&input, &memories);
        assert_eq!(matches, vec![0, 2]);
    }

    #[test]
    fn test_regex_pattern_matching() {
        let memory = make_memory(
            "Test memory",
            vec![make_trigger(0, "$.tool_input.command", "cargo test")],
        );
        let input = serde_json::json!({
            "tool_input": {"command": "cargo test --release"}
        });
        assert_eq!(match_memories(&input, &[memory]), vec![0]);
    }

    #[test]
    fn test_error_field_matching() {
        let memory = make_memory(
            "database locked memory",
            vec![
                make_trigger(0, "$.error", "database is locked"),
                make_trigger(0, "$.tool_input.command", "cargo test"),
            ],
        );
        let input = serde_json::json!({
            "error": "thread panicked: database is locked",
            "tool_input": {"command": "cargo test --test integration"}
        });
        assert_eq!(match_memories(&input, &[memory]), vec![0]);
    }
}
