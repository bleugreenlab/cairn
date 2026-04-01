//! Tool resolution utilities shared by backend adapters.
//!
//! Agent configs define which tools are enabled. Overlapping tools (those with
//! both native Claude and Cairn MCP versions) resolve to the preferred version:
//! Cairn by default, or native when `prefer_native` is set. Both `ClaudeBackend`
//! and `CodexBackend` call into these functions from their `resolve_tools()`
//! implementations.

use crate::models::ALWAYS_DISALLOWED_TOOLS;
use std::collections::HashMap;

/// Overlapping tool pack mapping (tools with both native and Cairn versions).
struct OverlappingTool {
    native: &'static [&'static str],
    cairn: &'static [&'static str],
    /// When true, resolve to native version instead of Cairn.
    prefer_native: bool,
}

/// Get all overlapping tool packs (native ↔ Cairn mappings).
fn get_overlapping_tools() -> HashMap<&'static str, OverlappingTool> {
    let mut tools = HashMap::new();
    tools.insert(
        "read",
        OverlappingTool {
            native: &["Read"],
            cairn: &["mcp__cairn__read"],
            prefer_native: false,
        },
    );
    tools.insert(
        "edit",
        OverlappingTool {
            native: &["Write", "Edit"],
            cairn: &["mcp__cairn__edit"],
            prefer_native: false,
        },
    );
    tools.insert(
        "bash",
        OverlappingTool {
            native: &["Bash", "KillShell"],
            cairn: &["mcp__cairn__bash", "mcp__cairn__kill_shell"],
            prefer_native: false,
        },
    );
    tools.insert(
        "task",
        OverlappingTool {
            native: &["Task", "TaskOutput"],
            cairn: &["mcp__cairn__task", "mcp__cairn__batch_tasks"],
            prefer_native: false,
        },
    );
    tools.insert(
        "ask_user",
        OverlappingTool {
            native: &["AskUserQuestion"],
            cairn: &["mcp__cairn__ask_user"],
            prefer_native: false,
        },
    );
    tools.insert(
        "todo",
        OverlappingTool {
            native: &["TodoWrite"],
            cairn: &["mcp__cairn__todo_write"],
            prefer_native: false,
        },
    );
    tools.insert(
        "glob",
        OverlappingTool {
            native: &["Glob"],
            cairn: &["mcp__cairn__glob"],
            prefer_native: false,
        },
    );
    tools.insert(
        "grep",
        OverlappingTool {
            native: &["Grep"],
            cairn: &["mcp__cairn__grep"],
            prefer_native: false,
        },
    );
    tools.insert(
        "web_fetch",
        OverlappingTool {
            native: &["WebFetch"],
            cairn: &["mcp__cairn__web_fetch"],
            prefer_native: true,
        },
    );
    tools.insert(
        "web_search",
        OverlappingTool {
            native: &["WebSearch"],
            cairn: &["mcp__cairn__web_search"],
            prefer_native: true,
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

    // Add Cairn-only tools (no native equivalent, not part of overlapping packs)
    all_tools.insert("mcp__cairn__execute".to_string());
    all_tools.insert("mcp__cairn__bug_report".to_string());
    all_tools.insert("mcp__cairn__db-schema".to_string());

    // Add native-only tools (no Cairn equivalent)
    all_tools.insert("LSP".to_string());
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

/// Map agent tool names to their preferred version (Cairn or native).
///
/// For overlapping tools, use the preferred version and disallow the other.
/// Most tools prefer Cairn; web_fetch/web_search prefer native.
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

            if mapping.prefer_native {
                // Use native tools, disallow Cairn equivalents
                for t in mapping.native {
                    allowed.push(t.to_string());
                }
                for t in mapping.cairn {
                    force_disallowed.push(t.to_string());
                }
            } else {
                // Use Cairn tools, disallow native equivalents
                for t in mapping.cairn {
                    allowed.push(t.to_string());
                }
                for t in mapping.native {
                    force_disallowed.push(t.to_string());
                }
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
        // Native versions (from overlapping tools)
        assert!(tools.contains(&"Read".to_string()));
        assert!(tools.contains(&"Write".to_string())); // Write is native side of merged edit pack
        assert!(tools.contains(&"Edit".to_string()));
        assert!(tools.contains(&"Bash".to_string()));
        assert!(tools.contains(&"Task".to_string()));
        assert!(tools.contains(&"TaskOutput".to_string()));
        assert!(tools.contains(&"AskUserQuestion".to_string()));
        assert!(tools.contains(&"Glob".to_string()));
        assert!(tools.contains(&"Grep".to_string()));
        assert!(tools.contains(&"WebFetch".to_string()));
        assert!(tools.contains(&"WebSearch".to_string()));
        // Cairn versions (from overlapping tools)
        assert!(tools.contains(&"mcp__cairn__read".to_string()));
        assert!(tools.contains(&"mcp__cairn__edit".to_string()));
        assert!(tools.contains(&"mcp__cairn__bash".to_string()));
        assert!(tools.contains(&"mcp__cairn__task".to_string()));
        assert!(tools.contains(&"mcp__cairn__batch_tasks".to_string()));
        assert!(tools.contains(&"mcp__cairn__ask_user".to_string()));
        assert!(tools.contains(&"TodoWrite".to_string()));
        assert!(tools.contains(&"mcp__cairn__todo_write".to_string()));
        assert!(tools.contains(&"mcp__cairn__glob".to_string()));
        assert!(tools.contains(&"mcp__cairn__grep".to_string()));
        assert!(tools.contains(&"mcp__cairn__web_fetch".to_string()));
        assert!(tools.contains(&"mcp__cairn__web_search".to_string()));
        // Cairn-only tools
        assert!(tools.contains(&"mcp__cairn__return".to_string()));
        assert!(tools.contains(&"mcp__cairn__create_pr".to_string()));
        assert!(tools.contains(&"mcp__cairn__execute".to_string()));
        assert!(tools.contains(&"mcp__cairn__bug_report".to_string()));
        assert!(tools.contains(&"mcp__cairn__db-schema".to_string()));
        // mcp__cairn__write should NOT exist (merged into edit)
        assert!(!tools.contains(&"mcp__cairn__write".to_string()));
        // Native-only tools
        assert!(tools.contains(&"NotebookEdit".to_string()));
        assert!(tools.contains(&"Skill".to_string()));
        // Always disallowed
        assert!(tools.contains(&"EnterPlanMode".to_string()));
        assert!(tools.contains(&"ExitPlanMode".to_string()));
    }

    #[test]
    fn test_resolve_tools_maps_to_preferred_version() {
        let tools = vec!["Read".to_string(), "Glob".to_string()];
        let (allowed, disallowed) = resolve_tools(&tools);

        // Read should map to Cairn (prefer_native: false)
        assert!(allowed.contains(&"mcp__cairn__read".to_string()));
        assert!(disallowed.contains(&"Read".to_string()));
        // Glob should map to Cairn (prefer_native: false)
        assert!(allowed.contains(&"mcp__cairn__glob".to_string()));
        assert!(disallowed.contains(&"Glob".to_string()));
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
    fn test_resolve_tools_todo_pack() {
        let tools = vec!["TodoWrite".to_string()];
        let (allowed, disallowed) = resolve_tools(&tools);

        assert!(allowed.contains(&"mcp__cairn__todo_write".to_string()));
        assert!(disallowed.contains(&"TodoWrite".to_string()));
    }

    #[test]
    fn test_resolve_tools_cairn_input_maps_to_cairn() {
        // If agent config already specifies Cairn tool names, still works
        let tools = vec!["mcp__cairn__read".to_string()];
        let (allowed, disallowed) = resolve_tools(&tools);

        assert!(allowed.contains(&"mcp__cairn__read".to_string()));
        assert!(disallowed.contains(&"Read".to_string()));
    }

    #[test]
    fn test_resolve_tools_glob_grep_to_cairn() {
        let tools = vec!["Glob".to_string(), "Grep".to_string()];
        let (allowed, disallowed) = resolve_tools(&tools);

        assert!(allowed.contains(&"mcp__cairn__glob".to_string()));
        assert!(allowed.contains(&"mcp__cairn__grep".to_string()));
        assert!(disallowed.contains(&"Glob".to_string()));
        assert!(disallowed.contains(&"Grep".to_string()));
    }

    #[test]
    fn test_resolve_tools_web_fetch_search_to_native() {
        let tools = vec!["WebFetch".to_string(), "WebSearch".to_string()];
        let (allowed, disallowed) = resolve_tools(&tools);

        // web_fetch/web_search prefer native
        assert!(allowed.contains(&"WebFetch".to_string()));
        assert!(allowed.contains(&"WebSearch".to_string()));
        assert!(disallowed.contains(&"mcp__cairn__web_fetch".to_string()));
        assert!(disallowed.contains(&"mcp__cairn__web_search".to_string()));
    }

    #[test]
    fn test_resolve_tools_web_fetch_cairn_input_still_native() {
        // Even if agent config specifies Cairn name, prefer_native wins
        let tools = vec!["mcp__cairn__web_fetch".to_string()];
        let (allowed, disallowed) = resolve_tools(&tools);

        assert!(allowed.contains(&"WebFetch".to_string()));
        assert!(disallowed.contains(&"mcp__cairn__web_fetch".to_string()));
    }

    #[test]
    fn test_resolve_tools_write_maps_through_edit_pack() {
        // Write should resolve to mcp__cairn__edit via the merged edit pack
        let tools = vec!["Write".to_string()];
        let (allowed, disallowed) = resolve_tools(&tools);

        assert!(allowed.contains(&"mcp__cairn__edit".to_string()));
        assert!(disallowed.contains(&"Write".to_string()));
        assert!(disallowed.contains(&"Edit".to_string()));
    }
}
