//! CLI argument building for Claude process
//!
//! This module handles the construction of command-line arguments for the Claude CLI,
//! including model selection, tool permissions, session resumption, and thinking tokens.

use crate::backends::SessionStart;
use crate::models::Model;
use std::path::PathBuf;

/// Configuration for building Claude CLI arguments.
#[derive(Debug, Clone)]
pub struct ClaudeArgsConfig {
    pub mcp_config_path: String,
    pub skip_permissions: bool,
    pub permission_prompt_tool: Option<String>, // MCP tool for permission prompts (replaces skip_permissions)
    pub model: Option<Model>,
    pub session_start: SessionStart,
    pub prompt: String,
    pub max_thinking_tokens: Option<i32>, // None = disabled, Some(n) = enable with n tokens
    pub allowed_tools: Vec<String>,
    pub disallowed_tools: Vec<String>,
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
        config.mcp_config_path.clone(),
    ];

    // Enable bidirectional stdin streaming
    if config.bidirectional {
        args.push("--input-format".to_string());
        args.push("stream-json".to_string());
    }

    // Add extended thinking if enabled
    if let Some(tokens) = config.max_thinking_tokens {
        args.push("--max-thinking-tokens".to_string());
        args.push(tokens.to_string());
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

    // Permission handling: prefer --permission-prompt-tool for user approval,
    // fall back to --dangerously-skip-permissions for auto-approval
    if let Some(ref tool) = config.permission_prompt_tool {
        args.push("--permission-prompt-tool".to_string());
        args.push(tool.clone());
    } else if config.skip_permissions {
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
            mcp_config_path: "/path/to/mcp.json".to_string(),
            skip_permissions: false,
            permission_prompt_tool: None,
            model: None,
            session_start: SessionStart::New {
                session_id: "cairn-uuid".to_string(),
            },
            prompt: "Test".to_string(),
            max_thinking_tokens: None,
            allowed_tools: vec!["Read".to_string()],
            disallowed_tools: vec![],
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
    fn test_skip_permissions() {
        let config = ClaudeArgsConfig {
            skip_permissions: true,
            ..base_config()
        };
        let args = build_claude_args(&config);
        assert!(args.contains(&"--dangerously-skip-permissions".to_string()));
    }

    #[test]
    fn test_permission_prompt_tool_takes_precedence() {
        let config = ClaudeArgsConfig {
            skip_permissions: true,
            permission_prompt_tool: Some("mcp__cairn__permission_prompt".to_string()),
            ..base_config()
        };
        let args = build_claude_args(&config);
        assert!(args.contains(&"--permission-prompt-tool".to_string()));
        assert!(!args.contains(&"--dangerously-skip-permissions".to_string()));
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
    fn test_thinking_tokens() {
        let config = ClaudeArgsConfig {
            max_thinking_tokens: Some(31999),
            ..base_config()
        };
        let args = build_claude_args(&config);
        assert!(args.contains(&"--max-thinking-tokens".to_string()));
        assert!(args.contains(&"31999".to_string()));
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
