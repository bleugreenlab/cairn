//! Agent toolkit configuration and resolution.
//!
//! This module provides tool permission resolution for Claude CLI sessions.
//! Agent configs define which tools are enabled. Overlapping tools (those with
//! both native Claude and Cairn MCP versions) always resolve to Cairn.

use crate::models::ALWAYS_DISALLOWED_TOOLS;
use std::collections::HashMap;

/// Overlapping tool pack mapping (tools with both native and Cairn versions).
struct OverlappingTool {
    native: &'static [&'static str],
    cairn: &'static [&'static str],
}

/// Get all overlapping tool packs (native ↔ Cairn mappings).
fn get_overlapping_tools() -> HashMap<&'static str, OverlappingTool> {
    let mut tools = HashMap::new();
    tools.insert(
        "read",
        OverlappingTool {
            native: &["Read"],
            cairn: &["mcp__cairn__read"],
        },
    );
    tools.insert(
        "write",
        OverlappingTool {
            native: &["Write"],
            cairn: &["mcp__cairn__write"],
        },
    );
    tools.insert(
        "edit",
        OverlappingTool {
            native: &["Edit"],
            cairn: &["mcp__cairn__edit"],
        },
    );
    tools.insert(
        "bash",
        OverlappingTool {
            native: &["Bash", "KillShell"],
            cairn: &["mcp__cairn__bash", "mcp__cairn__kill_shell"],
        },
    );
    tools.insert(
        "task",
        OverlappingTool {
            native: &["Task", "TaskOutput"],
            cairn: &["mcp__cairn__task", "mcp__cairn__batch_tasks"],
        },
    );
    tools.insert(
        "ask_user",
        OverlappingTool {
            native: &["AskUserQuestion"],
            cairn: &["mcp__cairn__ask_user"],
        },
    );
    tools
}

/// Get all known tools, including both versions of overlapping tools.
pub fn get_all_known_tools() -> Vec<String> {
    let mut all_tools = std::collections::HashSet::new();

    // Add overlapping tool packs (both native and Cairn versions)
    let overlapping = get_overlapping_tools();
    for mapping in overlapping.values() {
        for tool in mapping.native {
            all_tools.insert(tool.to_string());
        }
        for tool in mapping.cairn {
            all_tools.insert(tool.to_string());
        }
    }

    // Add always-disallowed tools
    for tool in ALWAYS_DISALLOWED_TOOLS {
        all_tools.insert(tool.to_string());
    }

    // Add Cairn-only tools (not part of overlapping packs)
    all_tools.insert("mcp__cairn__return".to_string());
    all_tools.insert("mcp__cairn__add_comment".to_string());
    all_tools.insert("mcp__cairn__search".to_string());
    all_tools.insert("mcp__cairn__list_issues".to_string());
    all_tools.insert("mcp__cairn__get_issue".to_string());
    all_tools.insert("mcp__cairn__create_issue".to_string());
    all_tools.insert("mcp__cairn__update_issue".to_string());
    all_tools.insert("mcp__cairn__get_plan".to_string());
    all_tools.insert("mcp__cairn__create_pr".to_string());
    all_tools.insert("mcp__cairn__retry_pr".to_string());
    all_tools.insert("mcp__cairn__skill".to_string());
    all_tools.insert("mcp__cairn__list_memories".to_string());
    all_tools.insert("mcp__cairn__create_memory".to_string());
    all_tools.insert("mcp__cairn__update_memory".to_string());
    all_tools.insert("mcp__cairn__deactivate_memory".to_string());
    all_tools.insert("mcp__cairn__message".to_string());

    // Add native-only tools
    all_tools.insert("Glob".to_string());
    all_tools.insert("Grep".to_string());
    all_tools.insert("WebFetch".to_string());
    all_tools.insert("WebSearch".to_string());
    all_tools.insert("LSP".to_string());
    all_tools.insert("TodoWrite".to_string());
    all_tools.insert("NotebookEdit".to_string());
    all_tools.insert("Skill".to_string());

    let mut tools: Vec<String> = all_tools.into_iter().collect();
    tools.sort();
    tools
}

/// Find an overlapping tool pack by any of its native or Cairn tool names.
fn find_overlapping_tool<'a>(
    tool: &str,
    overlapping: &'a HashMap<&'static str, OverlappingTool>,
) -> Option<(&'a str, &'a OverlappingTool)> {
    for (key, mapping) in overlapping {
        if mapping.native.contains(&tool) || mapping.cairn.contains(&tool) {
            return Some((key, mapping));
        }
    }
    None
}

/// Map agent tool names to their Cairn MCP equivalents.
///
/// For overlapping tools (Read/Write/Edit/Bash/Task/AskUser),
/// always use Cairn versions and disallow native versions.
/// Non-overlapping tools pass through unchanged.
///
/// Returns (allowed_tools, force_disallowed_tools).
pub fn resolve_tools(tools: &[String]) -> (Vec<String>, Vec<String>) {
    let overlapping = get_overlapping_tools();

    let mut allowed = Vec::new();
    let mut force_disallowed = Vec::new();
    let mut processed_packs = std::collections::HashSet::new();

    for tool in tools {
        if let Some((key, mapping)) = find_overlapping_tool(tool, &overlapping) {
            // Skip if we already processed this pack
            if processed_packs.contains(key) {
                continue;
            }
            processed_packs.insert(key);

            // Always use Cairn tools
            for t in mapping.cairn {
                allowed.push(t.to_string());
            }
            // Always disallow native equivalents
            for t in mapping.native {
                force_disallowed.push(t.to_string());
            }
        } else {
            allowed.push(tool.clone());
        }
    }

    (allowed, force_disallowed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_all_known_tools_includes_both_versions() {
        let tools = get_all_known_tools();
        // Native versions
        assert!(tools.contains(&"Read".to_string()));
        assert!(tools.contains(&"Write".to_string()));
        assert!(tools.contains(&"Edit".to_string()));
        assert!(tools.contains(&"Bash".to_string()));
        assert!(tools.contains(&"Task".to_string()));
        assert!(tools.contains(&"TaskOutput".to_string()));
        assert!(tools.contains(&"AskUserQuestion".to_string()));
        // Cairn versions
        assert!(tools.contains(&"mcp__cairn__read".to_string()));
        assert!(tools.contains(&"mcp__cairn__write".to_string()));
        assert!(tools.contains(&"mcp__cairn__edit".to_string()));
        assert!(tools.contains(&"mcp__cairn__bash".to_string()));
        assert!(tools.contains(&"mcp__cairn__task".to_string()));
        assert!(tools.contains(&"mcp__cairn__batch_tasks".to_string()));
        assert!(tools.contains(&"mcp__cairn__ask_user".to_string()));
        // Cairn-only tools
        assert!(tools.contains(&"mcp__cairn__return".to_string()));
        assert!(tools.contains(&"mcp__cairn__create_pr".to_string()));
        // Native-only tools
        assert!(tools.contains(&"Glob".to_string()));
        assert!(tools.contains(&"Grep".to_string()));
        assert!(tools.contains(&"NotebookEdit".to_string()));
        assert!(tools.contains(&"Skill".to_string()));
        // Always disallowed
        assert!(tools.contains(&"EnterPlanMode".to_string()));
        assert!(tools.contains(&"ExitPlanMode".to_string()));
    }

    #[test]
    fn test_resolve_tools_always_maps_to_cairn() {
        let tools = vec!["Read".to_string(), "Glob".to_string()];
        let (allowed, disallowed) = resolve_tools(&tools);

        // Read should map to mcp__cairn__read
        assert!(allowed.contains(&"mcp__cairn__read".to_string()));
        assert!(disallowed.contains(&"Read".to_string()));
        // Glob is native-only, passes through
        assert!(allowed.contains(&"Glob".to_string()));
    }

    #[test]
    fn test_resolve_tools_expands_tool_packs() {
        // Task should expand to full Cairn pack
        let tools = vec!["Task".to_string()];
        let (allowed, disallowed) = resolve_tools(&tools);

        assert!(allowed.contains(&"mcp__cairn__task".to_string()));
        assert!(allowed.contains(&"mcp__cairn__batch_tasks".to_string()));
        // Native versions should be disallowed
        assert!(disallowed.contains(&"Task".to_string()));
        assert!(disallowed.contains(&"TaskOutput".to_string()));
    }

    #[test]
    fn test_resolve_tools_bash_pack() {
        let tools = vec!["Bash".to_string()];
        let (allowed, disallowed) = resolve_tools(&tools);

        assert!(allowed.contains(&"mcp__cairn__bash".to_string()));
        assert!(allowed.contains(&"mcp__cairn__kill_shell".to_string()));
        assert!(disallowed.contains(&"Bash".to_string()));
    }

    #[test]
    fn test_resolve_tools_deduplicates_packs() {
        // Both Task and mcp__cairn__task refer to same pack
        let tools = vec!["Task".to_string(), "mcp__cairn__task".to_string()];
        let (allowed, _) = resolve_tools(&tools);

        let task_count = allowed.iter().filter(|t| *t == "mcp__cairn__task").count();
        assert_eq!(task_count, 1);
    }

    #[test]
    fn test_resolve_tools_cairn_input_maps_to_cairn() {
        // If agent config already specifies Cairn tool names, still works
        let tools = vec!["mcp__cairn__read".to_string()];
        let (allowed, disallowed) = resolve_tools(&tools);

        assert!(allowed.contains(&"mcp__cairn__read".to_string()));
        assert!(disallowed.contains(&"Read".to_string()));
    }
}
