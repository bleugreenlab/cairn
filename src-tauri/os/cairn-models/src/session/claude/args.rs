//! CLI argument building for Claude process
//!
//! This module handles the construction of command-line arguments for the Claude CLI,
//! including model selection, tool permissions, session resumption, and reasoning effort.

use crate::interface::session::SessionStart;
use cairn_domain::Model;
use std::path::PathBuf;

/// Configuration for building Claude CLI arguments.
#[derive(Debug, Clone)]
pub struct ClaudeArgsConfig {
    /// The `--mcp-config` value: a self-contained JSON string (Claude CLI accepts
    /// the config inline, not just a file path).
    pub mcp_config: String,
    pub skip_permissions: bool,
    pub model: Option<String>,
    pub session_start: SessionStart,
    pub effort: Option<String>, // Reasoning effort: low|medium|high|xhigh|max (None = CLI default)
    pub allowed_tools: Vec<String>,
    pub disallowed_tools: Vec<String>,
    pub system_prompt_file: Option<PathBuf>, // Path to file replacing Claude's default system prompt via --system-prompt-file
    pub settings_path: Option<PathBuf>,      // Path to additional settings JSON via --settings
    pub bidirectional: bool, // Enable stdin streaming with --input-format stream-json
    /// Resolved JSON Schema to constrain output via the CLI's built-in
    /// StructuredOutput tool (`--json-schema`). `Some` only for schema-
    /// constrained ephemeral calls; the structured result rides the terminal
    /// `result` event's `structured_output` (CAIRN-2505). `None` leaves output
    /// unconstrained (every non-call session). StructuredOutput is the CLI's
    /// own mechanism rather than a built-in, so `--tools ""` leaves it in
    /// place (`tests/claude_cli_contract.rs` pins this).
    pub json_schema: Option<serde_json::Value>,
    /// A tool-free one-shot: nothing saved to resume
    /// (`--no-session-persistence`, print mode only). Built-in tools are off
    /// for every launch, tool-free or not.
    pub tool_free: bool,
}

/// Build Claude CLI arguments from configuration.
/// Returns a vector of owned Strings for flexibility.
pub fn build_claude_args(config: &ClaudeArgsConfig) -> Vec<String> {
    let mut args = vec![
        "--output-format".to_string(),
        "stream-json".to_string(),
        "--mcp-config".to_string(),
        config.mcp_config.clone(),
        // Only the inline cairn server is configured. Without strict mode, Claude
        // Code also loads the user's global ~/.claude.json and any project
        // .mcp.json mcpServers, giving spawned agents native MCP servers. External
        // MCP must flow through the cairn gateway, so suppress those native paths.
        "--strict-mcp-config".to_string(),
        // Restrict Claude Code's settings + memory auto-discovery to the user
        // level: do NOT auto-load project/local settings.json or, crucially,
        // project/local CLAUDE.md memory. That native memory is injected as
        // context OUTSIDE the system prompt, so it would not appear in the
        // persisted `system:prompt` segments and would double-feed the project
        // instructions Cairn now assembles itself (the `project` segment). Cairn
        // owns that layer and supplies hooks via --settings and MCP via
        // --mcp-config, so project/local sources are pure redundancy. Verified
        // against the Claude CLI: `--setting-sources user` suppresses a
        // project-local CLAUDE.md while leaving --mcp-config attachment and
        // user-level auth/settings intact (unlike --bare/--safe-mode, which also
        // disable hooks, MCP, and keychain/OAuth that Cairn depends on).
        "--setting-sources".to_string(),
        "user".to_string(),
    ];

    // Enable bidirectional stdin streaming
    if config.bidirectional {
        args.push("--input-format".to_string());
        args.push("stream-json".to_string());
    }

    // Set reasoning effort if specified (replaces the removed --max-thinking-tokens flag)
    if let Some(ref effort) = config.effort {
        args.push("--effort".to_string());
        args.push(effort.clone());
    }

    // Add model flag if specified
    if let Some(ref model) = config.model {
        args.push("--model".to_string());
        // `opus[1m]` asks for Opus's 1M window on every CLI Cairn meets: 2.1.220
        // resolves bare `opus` to `claude-opus-5` at 200k, while 2.1.302+
        // resolves it to `claude-opus-5-5`, already 1M, where the suffix only
        // adds the long-context beta header. The other aliases stay bare: on the
        // older CLI `haiku` is Haiku 4.5, which has no 1M window to ask for.
        // Revisit once the CLI is pinned (cairn/5106) past 2.1.302.
        let model_str = if model.as_str() == Model::OPUS {
            "opus[1m]".to_string()
        } else {
            model.to_string()
        };
        args.push(model_str);
    }

    // Replace Claude's default system prompt entirely with the assembled stack.
    if let Some(ref file_path) = config.system_prompt_file {
        args.push("--system-prompt-file".to_string());
        args.push(file_path.to_string_lossy().to_string());
    }

    // Add settings file if specified (e.g., memory hooks)
    if let Some(ref settings_path) = config.settings_path {
        args.push("--settings".to_string());
        args.push(settings_path.to_string_lossy().to_string());
    }

    // Permission handling: allow mode auto-approves with
    // --dangerously-skip-permissions; ask/deny rely on the worktree fence inside
    // the verb handlers, so no CLI permission flag is emitted.
    if config.skip_permissions {
        args.push("--dangerously-skip-permissions".to_string());
    }

    // Which tools run without a permission prompt. This governs permission
    // only; which tools exist at all is `--tools` below.
    args.push("--allowedTools".to_string());
    args.push(config.allowed_tools.join(","));

    // Built-ins are already off, so a disallow list only ever names MCP tools
    // an agent is denied.
    if !config.disallowed_tools.is_empty() {
        args.push("--disallowedTools".to_string());
        args.push(config.disallowed_tools.join(","));
    }

    // Constrain output to a JSON Schema via the CLI's built-in StructuredOutput
    // tool. The schema-valid object comes back on the terminal `result` event as
    // `structured_output`, which Cairn captures server-side as the call's return
    // artifact (CAIRN-2505). Emitted only for schema-constrained ephemeral calls.
    if let Some(schema) = &config.json_schema {
        args.push("--json-schema".to_string());
        args.push(crate::wire::schema_order::ordered_schema_json(schema));
    }

    match &config.session_start {
        SessionStart::New { session_id } => {
            args.push("--session-id".to_string());
            args.push(session_id.clone());
        }
        SessionStart::Resume { backend_id, .. } => {
            args.push("--resume".to_string());
            args.push(backend_id.clone());
        }
        SessionStart::Fork {
            session_id,
            source_backend_id,
        } => {
            args.push("--resume".to_string());
            args.push(source_backend_id.clone());
            args.push("--session-id".to_string());
            args.push(session_id.clone());
            args.push("--fork-session".to_string());
        }
    }

    // Add print mode with verbose for stream-json compatibility
    args.push("--print".to_string());
    args.push("--verbose".to_string());
    if config.tool_free {
        args.push("--no-session-persistence".to_string());
    }
    // `--tools` is an allowlist of built-ins, and it is empty: no built-in is
    // declared to the model, including ones a later CLI adds, and their
    // definitions stay out of the prompt. Every tool Cairn offers is an MCP
    // tool, which this does not touch.
    args.push("--tools".to_string());
    args.push(String::new());

    // Enable partial message streaming for real-time token display
    args.push("--include-partial-messages".to_string());

    // The prompt never rides the argument vector: in bidirectional mode it is
    // a stream-json message, and in print mode the CLI reads it as plain text
    // from stdin. An argument would cap it at the platform's command-line
    // limit (about 32K characters on Windows), far below a model's context.
    args
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_config() -> ClaudeArgsConfig {
        ClaudeArgsConfig {
            mcp_config: "{\"mcpServers\":{}}".to_string(),
            skip_permissions: false,
            model: None,
            session_start: SessionStart::New {
                session_id: "cairn-uuid".to_string(),
            },
            effort: None,
            allowed_tools: vec!["Read".to_string()],
            disallowed_tools: vec![],
            system_prompt_file: None,
            settings_path: None,
            bidirectional: false,
            json_schema: None,
            tool_free: false,
        }
    }

    /// No launch relies on naming built-ins to switch them off: every one
    /// carries `--tools ""`, the CLI's allowlist of built-ins, so a built-in a
    /// CLI update adds is off before anyone has heard of it. A tool-free
    /// launch differs only in not being saved to resume.
    #[test]
    fn every_claude_launch_offers_an_explicit_builtin_allowlist() {
        let starts = [
            SessionStart::New {
                session_id: "cairn-uuid".to_string(),
            },
            SessionStart::Resume {
                session_id: "cairn-uuid".to_string(),
                backend_id: "claude-thread".to_string(),
            },
            SessionStart::Fork {
                session_id: "cairn-fork".to_string(),
                source_backend_id: "claude-thread".to_string(),
            },
        ];
        let schemas = [None, Some(serde_json::json!({"type": "object"}))];
        for start in &starts {
            for interactive in [true, false] {
                for with_mcp in [true, false] {
                    for schema in &schemas {
                        let config = ClaudeArgsConfig {
                            mcp_config: if with_mcp {
                                r#"{"mcpServers":{"cairn":{"type":"http","url":"http://o/mcp"}}}"#
                                    .to_string()
                            } else {
                                r#"{"mcpServers":{}}"#.to_string()
                            },
                            session_start: start.clone(),
                            bidirectional: interactive,
                            json_schema: schema.clone(),
                            tool_free: !with_mcp,
                            ..base_config()
                        };
                        let args = build_claude_args(&config);
                        let case = format!(
                            "{start:?}, interactive={interactive}, mcp={with_mcp}, schema={}",
                            schema.is_some()
                        );
                        assert_eq!(
                            args.iter().filter(|arg| *arg == "--tools").count(),
                            1,
                            "{case}: {args:?}"
                        );
                        assert_eq!(flag_value(&args, "--tools"), Some(""), "{case}");
                        assert_eq!(
                            args.contains(&"--no-session-persistence".to_string()),
                            !with_mcp,
                            "{case}: only a tool-free launch goes unsaved"
                        );
                    }
                }
            }
        }
    }

    /// With built-ins off by `--tools`, the disallow list only ever names an
    /// agent's own MCP denials, and is left off entirely when there are none.
    #[test]
    fn disallowed_tools_are_passed_only_when_some_are_named() {
        let none = build_claude_args(&base_config());
        assert!(!none.contains(&"--disallowedTools".to_string()), "{none:?}");
        let named = build_claude_args(&ClaudeArgsConfig {
            disallowed_tools: vec![
                "mcp__cairn__update_issue".to_string(),
                "mcp__deck__draw".to_string(),
            ],
            ..base_config()
        });
        assert_eq!(
            flag_value(&named, "--disallowedTools"),
            Some("mcp__cairn__update_issue,mcp__deck__draw")
        );
    }

    fn flag_value<'a>(args: &'a [String], flag: &str) -> Option<&'a str> {
        args.iter()
            .position(|argument| argument == flag)
            .and_then(|index| args.get(index + 1))
            .map(String::as_str)
    }

    #[test]
    fn new_resume_and_fork_use_identical_strict_mcp_policy() {
        let mut new_config = base_config();
        new_config.mcp_config =
            r#"{"mcpServers":{"cairn":{"command":"cairn-cmd","args":["mcp"]}}}"#.to_string();
        new_config.allowed_tools = vec![
            "mcp__cairn__read".to_string(),
            "mcp__cairn__write".to_string(),
            "mcp__cairn__run".to_string(),
        ];
        let mut resume_config = new_config.clone();
        resume_config.session_start = SessionStart::Resume {
            backend_id: "claude-thread".to_string(),
            session_id: "cairn-uuid".to_string(),
        };
        let mut fork_config = new_config.clone();
        fork_config.session_start = SessionStart::Fork {
            session_id: "cairn-fork".to_string(),
            source_backend_id: "claude-thread".to_string(),
        };

        let new_args = build_claude_args(&new_config);
        let resume_args = build_claude_args(&resume_config);
        let fork_args = build_claude_args(&fork_config);
        for candidate in [&resume_args, &fork_args] {
            assert_eq!(
                flag_value(&new_args, "--mcp-config"),
                flag_value(candidate, "--mcp-config")
            );
            assert!(candidate.contains(&"--strict-mcp-config".to_string()));
            assert_eq!(
                flag_value(&new_args, "--allowedTools"),
                flag_value(candidate, "--allowedTools")
            );
        }
    }

    #[test]
    fn test_json_schema_absent_by_default() {
        let args = build_claude_args(&base_config());
        assert!(!args.contains(&"--json-schema".to_string()));
    }

    #[test]
    fn test_json_schema_emitted_when_present() {
        let schema = serde_json::json!({
            "type": "object",
            "required": ["title"],
            "properties": {"answer": {"type": "string"}, "title": {"type": "string"}}
        });
        let config = ClaudeArgsConfig {
            json_schema: Some(schema.clone()),
            ..base_config()
        };
        let args = build_claude_args(&config);
        let idx = args
            .iter()
            .position(|x| x == "--json-schema")
            .expect("--json-schema flag missing");
        // The value is the compact JSON of the schema, in the same
        // required-first property order every other native constraint uses.
        assert_eq!(
            args[idx + 1],
            r#"{"properties":{"title":{"type":"string"},"answer":{"type":"string"}},"required":["title"],"type":"object"}"#
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&args[idx + 1]).unwrap(),
            schema
        );
        // Output format stays stream-json (structured_output rides the result event).
        assert!(args.contains(&"stream-json".to_string()));
    }

    #[test]
    fn test_basic_args() {
        let args = build_claude_args(&base_config());
        assert!(args.contains(&"--output-format".to_string()));
        assert!(args.contains(&"stream-json".to_string()));
        assert!(args.contains(&"--print".to_string()));
        assert!(args.contains(&"--verbose".to_string()));
    }

    #[test]
    fn test_setting_sources_restricted_to_user() {
        // Project/local CLAUDE.md memory must not be natively auto-loaded: Cairn
        // assembles the project segment itself, and native memory would be
        // injected outside the persisted system-prompt segments.
        let args = build_claude_args(&base_config());
        let idx = args
            .iter()
            .position(|x| x == "--setting-sources")
            .expect("--setting-sources flag missing");
        assert_eq!(args[idx + 1], "user");
    }

    #[test]
    fn test_strict_mcp_config_present() {
        // Spawned agents must have exactly one MCP server (cairn). Strict mode
        // prevents Claude Code from layering in native global/project servers.
        let args = build_claude_args(&base_config());
        assert!(args.contains(&"--strict-mcp-config".to_string()));
    }

    #[test]
    fn test_skip_permissions() {
        let config = ClaudeArgsConfig {
            skip_permissions: true,
            ..base_config()
        };
        let args = build_claude_args(&config);
        assert!(args.contains(&"--dangerously-skip-permissions".to_string()));
    }

    #[test]
    fn test_model_flag() {
        let config = ClaudeArgsConfig {
            model: Some(Model::OPUS.to_string()),
            ..base_config()
        };
        let args = build_claude_args(&config);
        assert!(args.contains(&"--model".to_string()));
        assert!(args.contains(&"opus[1m]".to_string()));
    }

    #[test]
    fn test_effort() {
        let config = ClaudeArgsConfig {
            effort: Some("high".to_string()),
            ..base_config()
        };
        let args = build_claude_args(&config);
        let idx = args
            .iter()
            .position(|x| x == "--effort")
            .expect("--effort flag missing");
        assert_eq!(args[idx + 1], "high");
    }

    #[test]
    fn test_effort_absent_by_default() {
        let args = build_claude_args(&base_config());
        assert!(!args.contains(&"--effort".to_string()));
        // The removed --max-thinking-tokens flag must never be emitted.
        assert!(!args.contains(&"--max-thinking-tokens".to_string()));
    }

    #[test]
    fn test_bidirectional_mode() {
        let config = ClaudeArgsConfig {
            bidirectional: true,
            ..base_config()
        };
        let args = build_claude_args(&config);
        assert!(args.contains(&"--input-format".to_string()));
        assert!(!args.contains(&"--".to_string()));
    }

    /// Print mode reads its prompt from stdin; nothing follows the flags.
    #[test]
    fn print_mode_puts_no_prompt_in_the_arguments() {
        let args = build_claude_args(&base_config());
        assert!(!args.contains(&"--input-format".to_string()));
        assert!(!args.contains(&"--".to_string()));
        assert_eq!(
            args.last().map(String::as_str),
            Some("--include-partial-messages")
        );
    }

    #[test]
    fn test_resume_session() {
        let config = ClaudeArgsConfig {
            session_start: SessionStart::Resume {
                session_id: "cairn-uuid".to_string(),
                backend_id: "session-abc".to_string(),
            },
            ..base_config()
        };
        let args = build_claude_args(&config);
        assert!(args.contains(&"--resume".to_string()));
        assert!(args.contains(&"session-abc".to_string()));
    }

    /// Why a prompt edit does not need a fresh session on Claude (CAIRN-4618).
    ///
    /// The system prompt is not fixed at session creation: the spawn reassembles
    /// the whole stack and hands it over as a file on every start, and `--resume`
    /// carries it exactly as a new session does. An edited agent prompt therefore
    /// takes effect on the existing conversation's next turn, so replacing that
    /// conversation to deliver it discards context for nothing.
    #[test]
    fn a_resumed_session_still_receives_the_assembled_system_prompt() {
        let mut config = base_config();
        config.session_start = SessionStart::Resume {
            session_id: "cairn-uuid".to_string(),
            backend_id: "session-abc".to_string(),
        };
        config.system_prompt_file = Some(std::path::PathBuf::from("/tmp/run/system-prompt.md"));
        let resumed = build_claude_args(&config);

        assert!(resumed.contains(&"--resume".to_string()));
        assert_eq!(
            flag_value(&resumed, "--system-prompt-file"),
            Some("/tmp/run/system-prompt.md"),
            "a resumed session was spawned without the current system prompt"
        );

        // And it is the same delivery a fresh session gets, so the two cannot
        // drift into one carrying the edit and the other not.
        let mut fresh = config.clone();
        fresh.session_start = SessionStart::New {
            session_id: "cairn-uuid".to_string(),
        };
        assert_eq!(
            flag_value(&build_claude_args(&fresh), "--system-prompt-file"),
            flag_value(&resumed, "--system-prompt-file")
        );
    }

    #[test]
    fn test_resume_excludes_session_id() {
        let config = ClaudeArgsConfig {
            session_start: SessionStart::Resume {
                session_id: "cairn-uuid".to_string(),
                backend_id: "backend-uuid".to_string(),
            },
            ..base_config()
        };
        let args = build_claude_args(&config);
        assert!(args.contains(&"--resume".to_string()));
        assert!(args.contains(&"backend-uuid".to_string()));
        assert!(!args.contains(&"--session-id".to_string()));
    }

    #[test]
    fn test_new_session_uses_session_id() {
        let config = ClaudeArgsConfig {
            session_start: SessionStart::New {
                session_id: "cairn-uuid".to_string(),
            },
            ..base_config()
        };
        let args = build_claude_args(&config);
        assert!(args.contains(&"--session-id".to_string()));
        assert!(args.contains(&"cairn-uuid".to_string()));
        assert!(!args.contains(&"--resume".to_string()));
    }

    #[test]
    fn test_system_prompt_file_emitted() {
        let config = ClaudeArgsConfig {
            system_prompt_file: Some(PathBuf::from("/tmp/claude-system-prompt.md")),
            ..base_config()
        };
        let args = build_claude_args(&config);
        let flag_idx = args
            .iter()
            .position(|x| x == "--system-prompt-file")
            .expect("--system-prompt-file flag missing");
        assert_eq!(args[flag_idx + 1], "/tmp/claude-system-prompt.md");
    }

    #[test]
    fn test_system_prompt_file_absent_by_default() {
        let args = build_claude_args(&base_config());
        assert!(!args.contains(&"--system-prompt-file".to_string()));
    }

    #[test]
    fn test_fork_session_includes_resume_and_new_session() {
        let config = ClaudeArgsConfig {
            session_start: SessionStart::Fork {
                session_id: "cairn-child".to_string(),
                source_backend_id: "claude-parent".to_string(),
            },
            ..base_config()
        };
        let args = build_claude_args(&config);
        assert!(args.contains(&"--resume".to_string()));
        assert!(args.contains(&"claude-parent".to_string()));
        assert!(args.contains(&"--session-id".to_string()));
        assert!(args.contains(&"cairn-child".to_string()));
        assert!(args.contains(&"--fork-session".to_string()));
    }
}
