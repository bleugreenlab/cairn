//! Tool resolution utilities shared by backend adapters.
//!
//! Agent configs list tools using friendly, Claude-Code-style names (`Read`,
//! `Write`, `Edit`, `Bash`, …). Cairn exposes exactly three working verbs —
//! `read`, `write`, `run` — so the friendly verbs alias to them and every
//! other bare name is dropped. Every tool Cairn offers is an MCP tool, so a
//! bare name can only be a provider built-in, and built-ins never run (every
//! Claude launch passes `--tools ""`). The rule needs no roster of built-ins,
//! so one nobody has named yet is dropped all the same.
//!
//! Every backend calls [`resolve_tools`] from its `resolve_tools()`.

/// The log target of a Claude session whose declared tools broke the
/// `--tools ""` contract.
pub const TOOL_SURFACE_LOG_TARGET: &str = "cairn::claude_tool_surface";

/// The CLI-internal tool through which a `--json-schema` answer arrives. It is
/// declared only under `--json-schema`, and `--tools ""` rightly leaves it.
const STRUCTURED_OUTPUT: &str = "StructuredOutput";

/// Does this declared tool name sit outside Cairn's surface?
///
/// Every tool Cairn offers is an MCP tool: the three verbs (`mcp__cairn__*`),
/// the corpus tools, and any external server reached through the `cairn://mcp`
/// gateway. Every launch passes `--tools ""`, so the CLI declares no built-in,
/// and a bare name in a session's declared surface means that contract broke.
fn is_native_leak(tool: &str) -> bool {
    !tool.starts_with("mcp__") && tool != STRUCTURED_OUTPUT
}

/// Check the tool surface a Claude session declared in its `init` event,
/// returning every name outside Cairn's surface and logging an error when
/// there is any.
///
/// This is a tripwire, not a fence: `--tools ""` is what keeps built-ins off,
/// and nothing here feeds a later launch. It holds no memory, so every
/// violating session is reported. The `init` event itself is persisted as the
/// run's `system:init` event row, which keeps the evidence queryable.
pub fn report_declared_surface(run_id: &str, declared: &[String]) -> Vec<String> {
    let violations: Vec<String> = declared
        .iter()
        .filter(|tool| is_native_leak(tool))
        .cloned()
        .collect();
    if !violations.is_empty() {
        log::error!(
            target: TOOL_SURFACE_LOG_TARGET,
            "Run {run_id}: the Claude CLI declared {} tool(s) outside Cairn's MCP surface: {}. \
             Every launch passes `--tools \"\"`, so the CLI broke that contract.",
            violations.len(),
            violations.join(", "),
        );
    }
    violations
}

/// Map a friendly native tool name to its canonical Cairn verb.
///
/// Only the three working verbs have aliases. Canonical Cairn names (and any
/// other input) return `None` and are handled by [`resolve_tools`] directly.
fn alias(tool: &str) -> Option<&'static str> {
    match tool {
        "Read" => Some("mcp__cairn__read"),
        "Write" | "Edit" => Some("mcp__cairn__write"),
        "Bash" => Some("mcp__cairn__run"),
        _ => None,
    }
}

/// Dead Cairn tool names that no longer map to a live tool. Agent configs and
/// older toolkits may still list them; they are silently dropped.
const DEAD_CAIRN_TOOLS: &[&str] = &[
    "mcp__cairn__task",
    "mcp__cairn__batch_tasks",
    "mcp__cairn__ask_user",
    "mcp__cairn__web_fetch",
    "mcp__cairn__web_search",
];

/// Should this tool name be dropped from the allow-list entirely?
///
/// True for any bare (non-MCP) name, which can only be a provider built-in,
/// and for dead Cairn names. Aliased names (`Read`/`Write`/`Edit`/`Bash`) are
/// resolved before this is consulted, so they never reach here.
fn is_dropped(tool: &str) -> bool {
    !tool.starts_with("mcp__") || DEAD_CAIRN_TOOLS.contains(&tool)
}

/// Resolve an agent's configured tool names to the Cairn allow-list.
///
/// - Friendly verbs (`Read`/`Write`/`Edit`/`Bash`) map to their canonical
///   Cairn verb (`mcp__cairn__read`/`write`/`run`).
/// - Every other bare name (a provider built-in, named anywhere or not) and
///   dead Cairn names are dropped.
/// - All other MCP names (canonical Cairn verbs, Cairn corpus tools like
///   `create_pr` and the memory tools) pass through unchanged.
///
/// The result is deduplicated, preserving first-seen order.
pub fn resolve_tools(tools: &[String]) -> Vec<String> {
    let mut allowed: Vec<String> = Vec::new();

    let push = |name: &str, allowed: &mut Vec<String>| {
        if !allowed.iter().any(|t| t == name) {
            allowed.push(name.to_string());
        }
    };

    for tool in tools {
        if let Some(canon) = alias(tool) {
            push(canon, &mut allowed);
        } else if is_dropped(tool) {
            continue;
        } else {
            push(tool, &mut allowed);
        }
    }

    allowed
}

/// The three core verbs — the entire working Cairn surface.
pub const CORE_VERBS: [&str; 3] = ["mcp__cairn__read", "mcp__cairn__write", "mcp__cairn__run"];

/// Ensure the three core verbs are present in an allow-list, appending any that
/// are missing (order preserved, deduped).
///
/// Temporary permissions floor pending the capability-based rethink (CAIRN-1172):
/// per-tool allow-listing no longer fits a world where `read`/`write`/`run` are
/// the whole surface. Without this, a read-only agent (e.g. Explore) resolves to
/// just `read`, so its `write cairn:~/return` — how a sub-agent task returns —
/// trips the permission prompt and wedges the run. Both backends call this after
/// [`resolve_tools`].
pub fn ensure_core_verbs(allowed: &mut Vec<String>) {
    for verb in CORE_VERBS {
        if !allowed.iter().any(|t| t == verb) {
            allowed.push(verb.to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_bare_names_count_as_leaks() {
        // Everything Cairn offers is an MCP tool, whatever the server.
        assert!(!is_native_leak("mcp__cairn__read"));
        assert!(!is_native_leak("mcp__cairn__create_pr"));
        assert!(!is_native_leak("mcp__axon__look"));
        // The schema answer channel is the CLI's own, not a built-in.
        assert!(!is_native_leak("StructuredOutput"));
        // A bare name can only have come from the host harness.
        assert!(is_native_leak("ListAgents"));
        assert!(is_native_leak("SendMessage"));
    }

    /// The `tools` of an `init` event the real CLI emitted.
    fn recorded_init_tools(init: &str) -> Vec<String> {
        let init: serde_json::Value = serde_json::from_str(init).unwrap();
        init["tools"]
            .as_array()
            .expect("an init event lists its tools")
            .iter()
            .map(|tool| tool.as_str().unwrap().to_string())
            .collect()
    }

    /// Claude Code, launched with Cairn's own arguments and Cairn's MCP
    /// server, declared only Cairn's verbs, so the tripwire stays quiet.
    #[test]
    fn the_recorded_agent_launch_declares_only_cairn_tools() {
        let tools = recorded_init_tools(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../cairn-models/tests/fixtures/claude_init_tools_none-2.1.302.json"
        )));
        assert!(!tools.is_empty());
        assert!(
            tools.iter().all(|tool| tool.starts_with("mcp__cairn__")),
            "{tools:?}"
        );
        assert!(report_declared_surface("fixture", &tools).is_empty());
    }

    /// A schema-constrained call declares StructuredOutput, which is how its
    /// answer arrives, and that is not a violation.
    #[test]
    fn the_recorded_schema_call_declares_only_structured_output() {
        let tools = recorded_init_tools(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../cairn-models/tests/fixtures/claude_init_json_schema-2.1.302.json"
        )));
        assert_eq!(tools, vec!["StructuredOutput".to_string()]);
        assert!(report_declared_surface("fixture", &tools).is_empty());
    }

    #[test]
    fn read_aliases_to_cairn_read() {
        let allowed = resolve_tools(&["Read".to_string()]);
        assert_eq!(allowed, vec!["mcp__cairn__read".to_string()]);
    }

    #[test]
    fn write_and_edit_alias_to_cairn_write() {
        let allowed = resolve_tools(&["Write".to_string(), "Edit".to_string()]);
        // Both collapse to the single write verb, deduped.
        assert_eq!(allowed, vec!["mcp__cairn__write".to_string()]);
    }

    #[test]
    fn bash_aliases_to_cairn_run() {
        let allowed = resolve_tools(&["Bash".to_string()]);
        assert_eq!(allowed, vec!["mcp__cairn__run".to_string()]);
    }

    #[test]
    fn canonical_cairn_verbs_pass_through() {
        let allowed = resolve_tools(&[
            "mcp__cairn__read".to_string(),
            "mcp__cairn__write".to_string(),
            "mcp__cairn__run".to_string(),
        ]);
        assert_eq!(
            allowed,
            vec![
                "mcp__cairn__read".to_string(),
                "mcp__cairn__write".to_string(),
                "mcp__cairn__run".to_string(),
            ]
        );
    }

    #[test]
    fn native_tools_are_dropped() {
        let allowed = resolve_tools(&[
            "Task".to_string(),
            "TaskOutput".to_string(),
            "AskUserQuestion".to_string(),
            "WebFetch".to_string(),
            "WebSearch".to_string(),
            "Glob".to_string(),
            "Grep".to_string(),
            "LSP".to_string(),
            "Skill".to_string(),
            "NotebookEdit".to_string(),
            "apply_patch".to_string(),
            // Never enumerated anywhere: dropped because it is bare.
            "FutureBuiltin".to_string(),
        ]);
        assert!(
            allowed.is_empty(),
            "native tools must be dropped: {allowed:?}"
        );
    }

    #[test]
    fn dead_cairn_names_are_dropped() {
        let allowed = resolve_tools(&[
            "mcp__cairn__task".to_string(),
            "mcp__cairn__batch_tasks".to_string(),
            "mcp__cairn__ask_user".to_string(),
            "mcp__cairn__web_fetch".to_string(),
            "mcp__cairn__web_search".to_string(),
        ]);
        assert!(
            allowed.is_empty(),
            "dead cairn names must be dropped: {allowed:?}"
        );
    }

    #[test]
    fn cairn_corpus_tools_pass_through() {
        let allowed = resolve_tools(&[
            "mcp__cairn__create_pr".to_string(),
            "mcp__cairn__read".to_string(),
            "mcp__cairn__db-schema".to_string(),
        ]);
        assert!(allowed.contains(&"mcp__cairn__create_pr".to_string()));
        assert!(allowed.contains(&"mcp__cairn__read".to_string()));
        assert!(allowed.contains(&"mcp__cairn__db-schema".to_string()));
    }

    #[test]
    fn mixed_config_resolves_and_dedups() {
        let allowed = resolve_tools(&[
            "Read".to_string(),
            "Write".to_string(),
            "Edit".to_string(),
            "Bash".to_string(),
            "Glob".to_string(),
            "WebFetch".to_string(),
            "FutureBuiltin".to_string(),
            "mcp__cairn__read".to_string(), // duplicate of aliased Read
            "mcp__cairn__create_pr".to_string(),
        ]);
        assert_eq!(
            allowed,
            vec![
                "mcp__cairn__read".to_string(),
                "mcp__cairn__write".to_string(),
                "mcp__cairn__run".to_string(),
                "mcp__cairn__create_pr".to_string(),
            ]
        );
    }

    #[test]
    fn ensure_core_verbs_adds_missing_and_preserves_existing() {
        // Read-only agent: write + run get appended.
        let mut allowed = resolve_tools(&["Read".to_string()]);
        ensure_core_verbs(&mut allowed);
        assert_eq!(
            allowed,
            vec![
                "mcp__cairn__read".to_string(),
                "mcp__cairn__write".to_string(),
                "mcp__cairn__run".to_string(),
            ]
        );

        // Already-present verbs are not duplicated; extra tools are kept.
        let mut allowed = vec![
            "mcp__cairn__run".to_string(),
            "mcp__cairn__create_pr".to_string(),
        ];
        ensure_core_verbs(&mut allowed);
        assert_eq!(
            allowed,
            vec![
                "mcp__cairn__run".to_string(),
                "mcp__cairn__create_pr".to_string(),
                "mcp__cairn__read".to_string(),
                "mcp__cairn__write".to_string(),
            ]
        );
    }

    #[test]
    fn core_verbs_match_registered_mcp_tool_names() {
        // INVARIANT: the granted verb names must equal the MCP tool names the
        // cairn-cmd registers (`#[tool] async fn read/write/run` →
        // `mcp__cairn__read`/`write`/`run`). If the registered mutation tool is
        // renamed, this floor and the alias must move with it — otherwise every
        // call to the live verb is off the allow-list and trips the permission
        // prompt (or is denied), wedging the run. cairn-cmd is not a dependency
        // of cairn-core, so the names are pinned literally here; keep them in
        // lockstep with `#[tool] async fn write` in cairn-cmd/src/main.rs.
        assert_eq!(
            CORE_VERBS,
            ["mcp__cairn__read", "mcp__cairn__write", "mcp__cairn__run"]
        );
        assert_eq!(alias("Write"), Some("mcp__cairn__write"));
        assert_eq!(alias("Edit"), Some("mcp__cairn__write"));
    }
}
