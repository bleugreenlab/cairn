//! CLI argument building for Claude process
//!
//! This module handles the construction of command-line arguments for the Claude CLI,
//! including model selection, tool permissions, session resumption, and reasoning effort.

use crate::backends::SessionStart;
use crate::models::Model;
use std::path::PathBuf;

/// Configuration for building Claude CLI arguments.
#[derive(Debug, Clone)]
pub struct ClaudeArgsConfig {
    /// The `--mcp-config` value: a self-contained JSON string (Claude CLI accepts
    /// the config inline, not just a file path).
    pub mcp_config: String,
    pub skip_permissions: bool,
    pub model: Option<Model>,
    pub session_start: SessionStart,
    pub prompt: String,
    pub effort: Option<String>, // Reasoning effort: low|medium|high|xhigh|max (None = CLI default)
    pub allowed_tools: Vec<String>,
    pub disallowed_tools: Vec<String>,
    pub system_prompt_file: Option<PathBuf>, // Path to file replacing Claude's default system prompt via --system-prompt-file
    pub append_system_prompt_file: Option<PathBuf>, // Path to file with system prompt content via --append-system-prompt-file
    pub settings_path: Option<PathBuf>, // Path to additional settings JSON via --settings
    pub bidirectional: bool,            // Enable stdin streaming with --input-format stream-json
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
        // Opus has 1M context now, but the default alias hasn't caught up yet
        let model_str = if model.as_str() == Model::OPUS {
            "opus[1m]".to_string()
        } else {
            model.to_string()
        };
        args.push(model_str);
    }

    // Replace Claude's default system prompt entirely (must precede the
    // --append-system-prompt-file, though order doesn't actually matter to the CLI).
    if let Some(ref file_path) = config.system_prompt_file {
        args.push("--system-prompt-file".to_string());
        args.push(file_path.to_string_lossy().to_string());
    }

    // Add system prompt file if specified (agent instructions go here)
    if let Some(ref file_path) = config.append_system_prompt_file {
        args.push("--append-system-prompt-file".to_string());
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

    // Add allowed tools
    args.push("--allowedTools".to_string());
    args.push(config.allowed_tools.join(","));

    // Add disallowed tools
    args.push("--disallowedTools".to_string());
    args.push(config.disallowed_tools.join(","));

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

    // Enable partial message streaming for real-time token display
    args.push("--include-partial-messages".to_string());

    // In bidirectional mode, the prompt is sent via stdin after spawn.
    // Otherwise, add it as a positional argument.
    if !config.bidirectional {
        // End of flags marker - ensures prompt starting with '-' isn't interpreted as a flag
        args.push("--".to_string());

        // Add prompt as positional argument (must be last, after --)
        args.push(config.prompt.clone());
    }

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
            prompt: "Test".to_string(),
            effort: None,
            allowed_tools: vec!["Read".to_string()],
            disallowed_tools: vec![],
            system_prompt_file: None,
            append_system_prompt_file: None,
            settings_path: None,
            bidirectional: false,
        }
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
            model: Some(Model::new(Model::OPUS)),
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
        // Prompt should NOT be a positional arg in bidirectional mode
        assert!(!args.contains(&"--".to_string()));
    }

    #[test]
    fn test_prompt_after_separator() {
        let config = ClaudeArgsConfig {
            prompt: "-starts-with-dash".to_string(),
            ..base_config()
        };
        let args = build_claude_args(&config);
        let sep_idx = args.iter().position(|x| x == "--").unwrap();
        let prompt_idx = args.iter().position(|x| x == "-starts-with-dash").unwrap();
        assert_eq!(prompt_idx, sep_idx + 1);
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
