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

/// Check if a memory's triggers match the given hook input.
///
/// Logic:
/// - Within a trigger group (same trigger_index): ALL conditions must match (AND)
/// - Across trigger groups (different indices): ANY group matching suffices (OR)
///
/// Returns false if the memory has no triggers or any trigger has an invalid regex.
fn triggers_match(memory: &Memory, hook_input: &serde_json::Value) -> bool {
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

/// Check if a memory's scope matches the agent's branch.
///
/// - `"project"` scope matches all agents
/// - `"branch:<name>"` scope only matches agents on that specific branch
/// - Unknown scopes are treated as permissive (match all)
fn scope_matches(memory_scope: &str, agent_branch: Option<&str>) -> bool {
    if memory_scope == "project" {
        return true;
    }
    if let Some(branch) = memory_scope.strip_prefix("branch:") {
        return agent_branch.is_some_and(|ab| ab == branch);
    }
    true // Unknown scope → permissive
}

/// Recursively collect all string values from a JSON value into a buffer.
fn flatten_json_strings(value: &serde_json::Value, buf: &mut String) {
    match value {
        serde_json::Value::String(s) => {
            buf.push(' ');
            buf.push_str(s);
        }
        serde_json::Value::Object(map) => {
            for (k, v) in map {
                buf.push(' ');
                buf.push_str(k);
                flatten_json_strings(v, buf);
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr {
                flatten_json_strings(v, buf);
            }
        }
        _ => {}
    }
}

/// Check if any of the memory's keywords appear in the hook input (case-insensitive).
fn keywords_match(hook_input: &serde_json::Value, keywords: &[String]) -> bool {
    if keywords.is_empty() {
        return false;
    }
    let mut text = String::new();
    flatten_json_strings(hook_input, &mut text);
    let text_lower = text.to_lowercase();
    keywords
        .iter()
        .any(|kw| text_lower.contains(&kw.to_lowercase()))
}

/// Match memories against hook input, returning indices of matched memories.
///
/// For each memory:
/// 1. `scope_matches` must pass (scope filter)
/// 2. At least one of `triggers_match` or `keywords_match` must pass
///
/// Memories with no triggers AND no keywords are never matched by this function.
/// They may be surfaced by other mechanisms (e.g., dynamic context injection).
///
/// Memories with invalid regex patterns in their triggers are silently skipped.
pub fn match_memories(
    hook_input: &serde_json::Value,
    memories: &[Memory],
    agent_branch: Option<&str>,
) -> Vec<usize> {
    memories
        .iter()
        .enumerate()
        .filter_map(|(i, memory)| {
            if !scope_matches(&memory.scope, agent_branch) {
                return None;
            }

            let trigger_hit = triggers_match(memory, hook_input);
            let keyword_hit = keywords_match(hook_input, &memory.keywords);

            if trigger_hit || keyword_hit {
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
            scope: "project".to_string(),
            keywords: vec![],
            source_run_id: None,
        }
    }

    fn make_memory_with_scope(
        content: &str,
        triggers: Vec<MemoryTrigger>,
        scope: &str,
        keywords: Vec<String>,
    ) -> Memory {
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
            scope: scope.to_string(),
            keywords,
            source_run_id: None,
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
        let matches = match_memories(&input, &[memory], None);
        assert_eq!(matches, vec![0]);
    }

    #[test]
    fn test_single_condition_no_match() {
        let memory = make_memory("Test memory", vec![make_trigger(0, "$.tool_name", "Write")]);
        let input = serde_json::json!({"tool_name": "Read"});
        let matches = match_memories(&input, &[memory], None);
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
        assert_eq!(match_memories(&input, &[memory.clone()], None), vec![0]);

        // Only tool_name matches
        let input = serde_json::json!({
            "tool_name": "Write",
            "tool_input": {"file_path": "/src/main.rs"}
        });
        assert!(match_memories(&input, &[memory], None).is_empty());
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
        assert_eq!(match_memories(&input, &[memory], None), vec![0]);
    }

    #[test]
    fn test_missing_json_path_doesnt_crash() {
        let memory = make_memory(
            "Test memory",
            vec![make_trigger(0, "$.nonexistent.deep.path", "anything")],
        );
        let input = serde_json::json!({"tool_name": "Write"});
        let matches = match_memories(&input, &[memory], None);
        assert!(matches.is_empty());
    }

    #[test]
    fn test_invalid_regex_skips_memory() {
        let memory = make_memory(
            "Test memory",
            vec![make_trigger(0, "$.tool_name", "[invalid regex")],
        );
        let input = serde_json::json!({"tool_name": "Write"});
        let matches = match_memories(&input, &[memory], None);
        assert!(matches.is_empty());
    }

    #[test]
    fn test_no_triggers_no_match() {
        let memory = make_memory("Test memory", vec![]);
        let input = serde_json::json!({"tool_name": "Write"});
        let matches = match_memories(&input, &[memory], None);
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
        let matches = match_memories(&input, &memories, None);
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
        assert_eq!(match_memories(&input, &[memory], None), vec![0]);
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
        assert_eq!(match_memories(&input, &[memory], None), vec![0]);
    }

    // --- Scope tests ---

    #[test]
    fn test_scope_project_matches_all() {
        let memory = make_memory_with_scope(
            "Project-scoped",
            vec![make_trigger(0, "$.tool_name", "Write")],
            "project",
            vec![],
        );
        let input = serde_json::json!({"tool_name": "Write"});
        // Matches with no branch
        assert_eq!(match_memories(&input, &[memory.clone()], None), vec![0]);
        // Matches with any branch
        assert_eq!(
            match_memories(&input, &[memory], Some("feature/x")),
            vec![0]
        );
    }

    #[test]
    fn test_scope_branch_matches_correct_branch() {
        let memory = make_memory_with_scope(
            "Branch-scoped",
            vec![make_trigger(0, "$.tool_name", "Write")],
            "branch:feature/x",
            vec![],
        );
        let input = serde_json::json!({"tool_name": "Write"});
        // Matches correct branch
        assert_eq!(
            match_memories(&input, &[memory.clone()], Some("feature/x")),
            vec![0]
        );
        // Doesn't match wrong branch
        assert!(match_memories(&input, &[memory.clone()], Some("main")).is_empty());
        // Doesn't match when no branch
        assert!(match_memories(&input, &[memory], None).is_empty());
    }

    // --- Keyword tests ---

    #[test]
    fn test_keywords_match_in_tool_input() {
        let memory = make_memory_with_scope(
            "DOM memory",
            vec![],
            "project",
            vec!["DOM".to_string(), "CSP".to_string()],
        );
        let input = serde_json::json!({
            "tool_name": "Write",
            "tool_input": {"file_path": "/src/dom-handler.ts"}
        });
        assert_eq!(match_memories(&input, &[memory], None), vec![0]);
    }

    #[test]
    fn test_keywords_case_insensitive() {
        let memory =
            make_memory_with_scope("DOM memory", vec![], "project", vec!["dom".to_string()]);
        let input = serde_json::json!({
            "tool_input": {"content": "Update the DOM handler"}
        });
        assert_eq!(match_memories(&input, &[memory], None), vec![0]);
    }

    #[test]
    fn test_keywords_no_match() {
        let memory =
            make_memory_with_scope("DOM memory", vec![], "project", vec!["DOM".to_string()]);
        let input = serde_json::json!({
            "tool_name": "Write",
            "tool_input": {"file_path": "/src/api.ts"}
        });
        assert!(match_memories(&input, &[memory], None).is_empty());
    }

    #[test]
    fn test_keywords_only_no_triggers() {
        // Memory with only keywords (no triggers) should match on keyword hit
        let memory = make_memory_with_scope(
            "Keyword-only memory",
            vec![],
            "project",
            vec!["migration".to_string()],
        );
        let input = serde_json::json!({
            "tool_name": "Bash",
            "tool_input": {"command": "diesel migration run"}
        });
        assert_eq!(match_memories(&input, &[memory], None), vec![0]);
    }

    #[test]
    fn test_no_triggers_no_keywords_no_match() {
        let memory = make_memory_with_scope("Empty memory", vec![], "project", vec![]);
        let input = serde_json::json!({"tool_name": "Write"});
        assert!(match_memories(&input, &[memory], None).is_empty());
    }

    // --- Combined tests ---

    #[test]
    fn test_trigger_or_keyword_either_suffices() {
        let memory = make_memory_with_scope(
            "Combined memory",
            vec![make_trigger(0, "$.tool_name", "Write")],
            "project",
            vec!["schema".to_string()],
        );
        // Trigger matches but keyword doesn't
        let input = serde_json::json!({"tool_name": "Write"});
        assert_eq!(match_memories(&input, &[memory.clone()], None), vec![0]);

        // Keyword matches but trigger doesn't
        let input = serde_json::json!({
            "tool_name": "Read",
            "tool_input": {"file_path": "schema.rs"}
        });
        assert_eq!(match_memories(&input, &[memory], None), vec![0]);
    }

    #[test]
    fn test_scope_filter_with_keywords() {
        let memory = make_memory_with_scope(
            "Branch-scoped with keywords",
            vec![],
            "branch:feature/x",
            vec!["migration".to_string()],
        );
        let input = serde_json::json!({
            "tool_input": {"command": "diesel migration run"}
        });
        // Matches on correct branch
        assert_eq!(
            match_memories(&input, &[memory.clone()], Some("feature/x")),
            vec![0]
        );
        // Blocked by scope on wrong branch
        assert!(match_memories(&input, &[memory], Some("main")).is_empty());
    }

    #[test]
    fn test_unknown_scope_is_permissive() {
        let memory = make_memory_with_scope(
            "Future scope type",
            vec![make_trigger(0, "$.tool_name", "Write")],
            "team:backend",
            vec![],
        );
        let input = serde_json::json!({"tool_name": "Write"});
        // Unknown scope should match (permissive fallback)
        assert_eq!(match_memories(&input, &[memory.clone()], None), vec![0]);
        assert_eq!(
            match_memories(&input, &[memory], Some("any-branch")),
            vec![0]
        );
    }

    #[test]
    fn test_keywords_match_object_keys() {
        // flatten_json_strings includes object keys in the text buffer
        let memory = make_memory_with_scope(
            "Matches key name",
            vec![],
            "project",
            vec!["file_path".to_string()],
        );
        let input = serde_json::json!({
            "tool_input": {"file_path": "/src/main.rs"}
        });
        assert_eq!(match_memories(&input, &[memory], None), vec![0]);
    }

    #[test]
    fn test_keywords_skip_non_string_primitives() {
        // Numbers, bools, null should not appear in flattened text
        let memory =
            make_memory_with_scope("Number keyword", vec![], "project", vec!["42".to_string()]);
        let input = serde_json::json!({
            "count": 42,
            "enabled": true,
            "error": null
        });
        assert!(match_memories(&input, &[memory], None).is_empty());
    }

    #[test]
    fn test_keywords_match_in_nested_arrays() {
        let memory = make_memory_with_scope(
            "Array keyword",
            vec![],
            "project",
            vec!["schema".to_string()],
        );
        let input = serde_json::json!({
            "tool_input": {
                "files": ["main.rs", "schema.rs", "lib.rs"]
            }
        });
        assert_eq!(match_memories(&input, &[memory], None), vec![0]);
    }

    #[test]
    fn test_scope_branch_with_triggers() {
        // Branch scope blocks even when triggers match
        let memory = make_memory_with_scope(
            "Branch + trigger",
            vec![make_trigger(0, "$.tool_name", "Write")],
            "branch:feature/x",
            vec![],
        );
        let input = serde_json::json!({"tool_name": "Write"});
        // Trigger matches but wrong branch → no match
        assert!(match_memories(&input, &[memory], Some("main")).is_empty());
    }
}
