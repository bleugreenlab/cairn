//! The Claude CLI as a session provider.
//!
//! [`ClaudeCli`] opens a conversation by starting `claude` in stream-json mode
//! on a [`SessionHost`]; [`ClaudeControl`] is the one writer to its stdin.

pub mod args;

use std::io::Write;

use cairn_platform::services::process::SpawnConfig;
use serde_json::json;

use crate::claude::tokens::OAUTH_TOKEN_ENV;
use crate::interface::session::{
    ApprovalMode, ClaudeMode, Cli, LaunchedSession, ProviderSpec, SecretEnv, SessionControl,
    SessionError, SessionEvents, SessionFacts, SessionHost, SessionInput, SessionProvider,
    SessionSpec, SessionStart,
};
use crate::wire::claude_stream;
use crate::wire::MessageContent;
use args::{build_claude_args, ClaudeArgsConfig};

/// The environment every Claude session runs with, whoever launches it.
///
/// Tool search and the CLI's own task list are off because Cairn owns tool
/// discovery and delegation. The MCP tool timeout is seven days: a blocking
/// `write` append (a sub-agent, a user question) has no host-side deadline, and
/// the CLI's default would abandon the call while the host keeps working.
pub const CLAUDE_SESSION_ENV: [(&str, &str); 3] = [
    ("ENABLE_TOOL_SEARCH", "false"),
    ("CLAUDE_CODE_ENABLE_TASKS", "false"),
    ("MCP_TOOL_TIMEOUT", "604800000"),
];

/// Every way a Claude CLI can be handed a credential through its environment.
/// A launch carries exactly the ones its own secrets set; any other value
/// inherited from Cairn's environment (an operator's API key, their own OAuth
/// token, a host-credentials file) is removed, so a process can only ever
/// authenticate as the account it was routed to.
pub const CLAUDE_AUTH_ENV: [&str; 7] = [
    "ANTHROPIC_API_KEY",
    "ANTHROPIC_AUTH_TOKEN",
    "CLAUDE_CODE_OAUTH_TOKEN",
    "CLAUDE_CODE_OAUTH_REFRESH_TOKEN",
    "CLAUDE_CODE_OAUTH_TOKEN_FILE_DESCRIPTOR",
    "CLAUDE_CODE_HOST_CREDS_FILE",
    "CLAUDE_CODE_PROVIDER_MANAGED_BY_HOST",
];

const CREDENTIAL_REQUEST_PREFIX: &str = "cairn-credential-";

/// The credential generation a pushed update named, read back from the
/// `request_id` of the `control_response` acknowledging it.
pub fn credential_push_generation(request_id: &str) -> Option<u64> {
    request_id
        .strip_prefix(CREDENTIAL_REQUEST_PREFIX)?
        .split('-')
        .next()?
        .parse()
        .ok()
}

/// Opens Claude CLI sessions.
#[derive(Debug, Clone, Copy, Default)]
pub struct ClaudeCli;

impl ClaudeCli {
    /// The argument vector and environment a launch would spawn with, and
    /// nothing else: no file is placed and no process started. `system_prompt`
    /// and `settings` are the paths the host placed.
    pub fn spawn_config(
        cli_path: &str,
        spec: &SessionSpec,
        system_prompt: Option<String>,
        settings: Option<String>,
    ) -> Result<SpawnConfig, SessionError> {
        let ProviderSpec::Claude(claude) = &spec.provider else {
            return Err(SessionError::Setup(
                "the Claude CLI opens only Claude sessions".to_string(),
            ));
        };
        if spec.mcp.is_none() && !matches!(spec.start, SessionStart::New { .. }) {
            return Err(SessionError::Setup(
                "a tool-free Claude session can only be a new one".to_string(),
            ));
        }
        let bidirectional = claude.mode == ClaudeMode::Interactive;
        let args = build_claude_args(&ClaudeArgsConfig {
            mcp_config: spec.mcp.as_ref().map_or_else(
                || crate::session::mcp::CLAUDE_NO_MCP_CONFIG.to_string(),
                |mcp| mcp.claude_mcp_config(),
            ),
            skip_permissions: spec.approval == ApprovalMode::Allow,
            model: spec.model.clone(),
            session_start: spec.start.clone(),
            effort: spec.effort.clone(),
            allowed_tools: claude.allowed_tools.clone(),
            disallowed_tools: claude.disallowed_tools.clone(),
            system_prompt_file: system_prompt.map(Into::into),
            settings_path: settings.map(Into::into),
            bidirectional,
            json_schema: spec.output_schema.clone(),
            tool_free: spec.mcp.is_none(),
        });
        let mut config = SpawnConfig::new(cli_path)
            .args(&args)
            .cwd(&spec.cwd)
            .stdin(true);
        for (name, value) in CLAUDE_SESSION_ENV {
            config = config.env(name, value);
        }
        for (name, value) in &spec.env {
            config = config.env(name, value);
        }
        // Removals apply after every overlay, so only what the secrets do not
        // set is removed.
        for name in CLAUDE_AUTH_ENV {
            if !spec.secrets.names().any(|secret| secret == name) {
                config = config.env_remove(name);
            }
        }
        Ok(spec.secrets.apply(config))
    }
}

impl SessionProvider for ClaudeCli {
    fn open(
        &self,
        host: &dyn SessionHost,
        spec: SessionSpec,
    ) -> Result<LaunchedSession, SessionError> {
        let ProviderSpec::Claude(claude) = &spec.provider else {
            return Err(SessionError::Setup(
                "the Claude CLI opens only Claude sessions".to_string(),
            ));
        };
        let prompt = &claude.system_prompt;
        let system_prompt = host
            .place_file(&prompt.dir, &prompt.name, prompt.contents.as_bytes())
            .map_err(SessionError::Setup)?;
        let settings = claude.hook_settings.as_ref().and_then(|file| {
            host.place_file(&file.dir, &file.name, file.contents.as_bytes())
                .map_err(|error| log::warn!("Claude hook settings not placed: {error}"))
                .ok()
        });
        let cli_path = host.resolve_cli(Cli::Claude)?;
        let config = Self::spawn_config(&cli_path, &spec, Some(system_prompt), settings)?;
        log::debug!(
            "ClaudeCli: spawning {} with {} arguments in {}",
            cli_path,
            config.args.len(),
            spec.cwd
        );

        let mut child = host.spawn(config).map_err(SessionError::Spawn)?;
        log::debug!("ClaudeCli: spawned, pid={}", child.id());
        let stdout = child
            .take_stdout()
            .ok_or_else(|| SessionError::Protocol("Failed to capture stdout".to_string()))?;
        let stderr = child.take_stderr();
        let stdin = child
            .take_stdin()
            .ok_or_else(|| SessionError::Protocol("Failed to capture stdin".to_string()))?;
        let control = match &claude.mode {
            ClaudeMode::Interactive => ClaudeControl::boxed(stdin),
            ClaudeMode::OneShot { prompt } => {
                feed_prompt(stdin, prompt.clone());
                ClaudeControl::boxed(Box::new(OneShotInput))
            }
        };

        let backend_session_id = match &spec.start {
            SessionStart::New { session_id } | SessionStart::Fork { session_id, .. } => {
                session_id.clone()
            }
            SessionStart::Resume { backend_id, .. } => backend_id.clone(),
        };
        Ok(LaunchedSession {
            control,
            events: SessionEvents::ClaudeStream { stdout, stderr },
            process: std::sync::Arc::new(std::sync::Mutex::new(Some(child))),
            facts: SessionFacts {
                provider: "claude".to_string(),
                backend_session_id: Some(backend_session_id),
                account_id: spec.account_id.clone(),
                host: host.name().to_string(),
                model: spec.model.clone(),
            },
        })
    }
}

/// Hand a print-mode process its prompt on stdin, then close it. On its own
/// thread, since a prompt larger than the pipe buffer is written only as fast
/// as the CLI reads it.
fn feed_prompt(mut stdin: Box<dyn Write + Send>, prompt: String) {
    std::thread::spawn(move || {
        if let Err(error) = stdin
            .write_all(prompt.as_bytes())
            .and_then(|()| stdin.flush())
        {
            log::warn!("ClaudeCli: the one-shot prompt was not delivered: {error}");
        }
    });
}

/// A one-shot session's stdin after its prompt: it takes nothing more.
struct OneShotInput;

impl Write for OneShotInput {
    fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
        Err(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "a one-shot Claude session takes no input after its prompt",
        ))
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Writes stream-json to a Claude process's stdin.
pub struct ClaudeControl {
    stdin: Box<dyn Write + Send>,
}

impl ClaudeControl {
    pub fn new(stdin: Box<dyn Write + Send>) -> Self {
        Self { stdin }
    }

    pub fn boxed(stdin: Box<dyn Write + Send>) -> Box<dyn SessionControl> {
        Box::new(Self::new(stdin))
    }
}

impl SessionControl for ClaudeControl {
    fn open_turn(&mut self, content: &MessageContent) -> Result<(), String> {
        let content = claude_stream::build_message_content(content)?;
        let initial_message = json!({
            "type": "user",
            "message": {
                "role": "user",
                "content": content
            }
        });
        writeln!(self.stdin, "{}", initial_message)
            .map_err(|e| format!("Failed to send initial prompt via stdin: {}", e))?;
        self.stdin
            .flush()
            .map_err(|e| format!("Failed to flush stdin: {}", e))
    }

    fn send(&mut self, input: &SessionInput<'_>) -> Result<(), String> {
        claude_stream::send_user_message(
            &mut self.stdin,
            input.session_id,
            input.content,
            input.parent_tool_use_id,
        )
    }

    fn interrupt(&mut self) -> Result<(), String> {
        let request_id = uuid::Uuid::new_v4().to_string();
        claude_stream::send_interrupt_request(&mut self.stdin, &request_id)
    }

    fn set_model(&mut self, model: &str) -> Result<(), String> {
        let request_id = uuid::Uuid::new_v4().to_string();
        claude_stream::send_set_model_request(&mut self.stdin, &request_id, model)
    }

    fn set_permission_mode(&mut self, mode: &str) -> Result<(), String> {
        let request_id = uuid::Uuid::new_v4().to_string();
        claude_stream::send_set_permission_mode_request(&mut self.stdin, &request_id, mode)
    }

    /// Sends only the OAuth token, the one credential the CLI accepts at
    /// runtime. A one-shot's stdin is closed after its prompt, so this fails
    /// there, which is why launches are never handed a credential close to
    /// expiry.
    fn update_credential(&mut self, env: &SecretEnv, generation: u64) -> Result<(), String> {
        let token = env
            .iter()
            .find(|(name, _)| *name == OAUTH_TOKEN_ENV)
            .map(|(_, value)| value)
            .ok_or_else(|| format!("the credential update carries no {OAUTH_TOKEN_ENV}"))?;
        let request_id = format!(
            "{CREDENTIAL_REQUEST_PREFIX}{generation}-{}",
            uuid::Uuid::new_v4()
        );
        claude_stream::send_update_environment_variables(
            &mut self.stdin,
            &request_id,
            &[(OAUTH_TOKEN_ENV, token)],
        )
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct Shared(Arc<Mutex<Vec<u8>>>);

    impl Write for Shared {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn lines(shared: &Shared) -> Vec<serde_json::Value> {
        String::from_utf8(shared.0.lock().unwrap().clone())
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    /// An agent launch, with Cairn's server attached, is offered no built-in
    /// tool (`--tools ""`), and with no MCP denials passes no disallow list.
    #[test]
    fn an_agent_launch_offers_no_built_in_tools() {
        use crate::interface::session::{
            ApprovalMode, ClaudeSpec, PlacedFile, SecretEnv, SessionSpec,
        };
        use crate::session::mcp::{McpServer, McpServers};
        let spec = SessionSpec {
            start: SessionStart::New {
                session_id: "agent".to_string(),
            },
            model: None,
            effort: None,
            approval: ApprovalMode::Allow,
            output_schema: None,
            cwd: "/tmp/worktree".to_string(),
            env: std::collections::BTreeMap::new(),
            mcp: Some(McpServers::new(vec![McpServer::cairn_http("http://o/mcp")]).unwrap()),
            secrets: SecretEnv::new(),
            account_id: None,
            provider: ProviderSpec::Claude(ClaudeSpec {
                system_prompt: PlacedFile {
                    dir: "/tmp/worktree".to_string(),
                    name: "system.md".to_string(),
                    contents: "work".to_string(),
                },
                hook_settings: None,
                allowed_tools: vec!["mcp__cairn__read".to_string()],
                disallowed_tools: vec![],
                mode: ClaudeMode::Interactive,
            }),
        };
        let config = ClaudeCli::spawn_config("/bin/claude", &spec, None, None).unwrap();
        let value = |flag: &str| {
            config
                .args
                .iter()
                .position(|arg| arg == flag)
                .map(|index| config.args[index + 1].clone())
        };
        assert_eq!(value("--tools").as_deref(), Some(""));
        assert_eq!(value("--disallowedTools"), None);
        assert!(!config
            .args
            .contains(&"--no-session-persistence".to_string()));
    }

    /// A tool-free one-shot: an empty strict MCP config, no built-in tools,
    /// no disallow list to pass, and nothing saved to resume.
    #[test]
    fn a_tool_free_one_shot_gets_no_servers_and_is_not_persisted() {
        use crate::interface::session::{
            ApprovalMode, ClaudeSpec, PlacedFile, SecretEnv, SessionSpec,
        };
        let spec = SessionSpec {
            start: SessionStart::New {
                session_id: "one-shot".to_string(),
            },
            model: Some("haiku".to_string()),
            effort: None,
            approval: ApprovalMode::Deny,
            output_schema: None,
            cwd: "/tmp/scratch".to_string(),
            env: std::collections::BTreeMap::new(),
            mcp: None,
            secrets: SecretEnv::new(),
            account_id: Some("acct".to_string()),
            provider: ProviderSpec::Claude(ClaudeSpec {
                system_prompt: PlacedFile {
                    dir: "/tmp/scratch".to_string(),
                    name: "system.md".to_string(),
                    contents: "answer".to_string(),
                },
                hook_settings: None,
                allowed_tools: vec![],
                disallowed_tools: vec![],
                mode: ClaudeMode::OneShot {
                    prompt: "hello".to_string(),
                },
            }),
        };
        let config = ClaudeCli::spawn_config("/bin/claude", &spec, None, None).unwrap();
        let value = |flag: &str| {
            config
                .args
                .iter()
                .position(|arg| arg == flag)
                .map(|index| config.args[index + 1].clone())
        };
        assert_eq!(
            value("--mcp-config").as_deref(),
            Some(r#"{"mcpServers":{}}"#)
        );
        assert!(config.args.contains(&"--strict-mcp-config".to_string()));
        assert_eq!(value("--allowedTools").as_deref(), Some(""));
        assert_eq!(value("--disallowedTools"), None);
        assert_eq!(value("--tools").as_deref(), Some(""));
        assert!(config
            .args
            .contains(&"--no-session-persistence".to_string()));
        assert!(!config
            .args
            .contains(&"--dangerously-skip-permissions".to_string()));
        assert!(
            !config.args.contains(&"hello".to_string()),
            "the prompt goes to stdin"
        );

        for start in [
            SessionStart::Resume {
                session_id: "s".to_string(),
                backend_id: "b".to_string(),
            },
            SessionStart::Fork {
                session_id: "s".to_string(),
                source_backend_id: "b".to_string(),
            },
        ] {
            let resumed = SessionSpec {
                start,
                ..spec_clone(&spec)
            };
            assert!(matches!(
                ClaudeCli::spawn_config("/bin/claude", &resumed, None, None),
                Err(SessionError::Setup(_))
            ));
        }
    }

    fn spec_clone(spec: &SessionSpec) -> SessionSpec {
        let ProviderSpec::Claude(claude) = &spec.provider else {
            unreachable!()
        };
        SessionSpec {
            start: spec.start.clone(),
            model: spec.model.clone(),
            effort: spec.effort.clone(),
            approval: spec.approval,
            output_schema: spec.output_schema.clone(),
            cwd: spec.cwd.clone(),
            env: spec.env.clone(),
            mcp: spec.mcp.clone(),
            secrets: spec.secrets.clone(),
            account_id: spec.account_id.clone(),
            provider: ProviderSpec::Claude(crate::interface::session::ClaudeSpec {
                system_prompt: claude.system_prompt.clone(),
                hook_settings: claude.hook_settings.clone(),
                allowed_tools: claude.allowed_tools.clone(),
                disallowed_tools: claude.disallowed_tools.clone(),
                mode: claude.mode.clone(),
            }),
        }
    }

    /// A prompt far past any command-line limit reaches a real process whole,
    /// on stdin, through the same open and reader a response uses.
    #[cfg(unix)]
    #[test]
    fn a_prompt_larger_than_a_command_line_arrives_on_stdin() {
        use crate::interface::session::{
            ApprovalMode, ClaudeSpec, CodexModels, PlacedFile, SecretEnv, SessionSpec,
        };
        use cairn_platform::services::process::{
            ChildProcess, ProcessSpawner, RealProcessSpawner, SpawnConfig,
        };

        /// Runs a stand-in for the CLI that counts its stdin and reports the
        /// count as the turn's result.
        struct CountingHost(std::path::PathBuf);
        impl SessionHost for CountingHost {
            fn name(&self) -> &str {
                "counting"
            }
            fn resolve_cli(&self, _: Cli) -> Result<String, SessionError> {
                Ok("claude".to_string())
            }
            fn spawn(&self, config: SpawnConfig) -> Result<Box<dyn ChildProcess>, String> {
                let argv: usize = config.args.iter().map(String::len).sum();
                assert!(argv < 16_384, "{argv} bytes of arguments");
                let script = r#"n=$(wc -c | tr -d ' '); printf '{"type":"result","subtype":"success","session_id":"s","is_error":false,"result":"%s"}\n' "$n""#;
                let mut stand_in = SpawnConfig::new("/bin/sh")
                    .arg("-c")
                    .arg(script)
                    .stdin(true);
                stand_in.cwd = config.cwd.clone();
                RealProcessSpawner.spawn(stand_in)
            }
            fn place_file(&self, _: &str, name: &str, contents: &[u8]) -> Result<String, String> {
                let path = self.0.join(name);
                std::fs::write(&path, contents).map_err(|e| e.to_string())?;
                Ok(path.to_string_lossy().into_owned())
            }
            fn prepare_codex_home(&self, _: CodexModels<'_>) -> Result<String, SessionError> {
                unreachable!()
            }
        }

        let dir = std::env::temp_dir().join(format!("one-shot-stdin-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let spec = SessionSpec {
            start: SessionStart::New {
                session_id: "s".to_string(),
            },
            model: None,
            effort: None,
            approval: ApprovalMode::Deny,
            output_schema: None,
            cwd: dir.to_string_lossy().into_owned(),
            env: std::collections::BTreeMap::new(),
            mcp: None,
            secrets: SecretEnv::new(),
            account_id: None,
            provider: ProviderSpec::Claude(ClaudeSpec {
                system_prompt: PlacedFile {
                    dir: dir.to_string_lossy().into_owned(),
                    name: "system.md".to_string(),
                    contents: "answer".to_string(),
                },
                hook_settings: None,
                allowed_tools: vec![],
                disallowed_tools: vec![],
                mode: ClaudeMode::OneShot {
                    prompt: "x".repeat(300_000),
                },
            }),
        };
        let launched = ClaudeCli.open(&CountingHost(dir.clone()), spec).unwrap();
        let reply =
            crate::session::one_shot::await_one_shot(launched, std::time::Duration::from_secs(20))
                .unwrap();
        assert_eq!(reply.text, "300000");
        let _ = std::fs::remove_dir_all(dir);
    }

    /// The opening message carries no session reference; a later one does.
    #[test]
    fn the_first_turn_and_a_later_message_keep_their_own_shapes() {
        let shared = Shared::default();
        let mut control = ClaudeControl::new(Box::new(shared.clone()));
        let content = MessageContent::text("hello");
        control.open_turn(&content).unwrap();
        control
            .send(&SessionInput {
                content: &MessageContent::text("again"),
                session_id: "session-1",
                parent_tool_use_id: None,
            })
            .unwrap();
        control.interrupt().unwrap();

        let sent = lines(&shared);
        assert_eq!(
            sent[0],
            json!({"type": "user", "message": {"role": "user", "content": "hello"}})
        );
        assert_eq!(sent[1]["session_id"], "session-1");
        assert!(sent[1].get("parent_tool_use_id").is_some());
        assert_eq!(sent[2]["request"]["subtype"], "interrupt");
    }

    #[test]
    fn a_credential_update_pushes_only_the_oauth_token_and_names_its_generation() {
        let shared = Shared::default();
        let mut control = ClaudeControl::new(Box::new(shared.clone()));
        let env = SecretEnv::new()
            .with(OAUTH_TOKEN_ENV, "at-7")
            .with("CLAUDE_CONFIG_DIR", "/home");
        control.update_credential(&env, 7).unwrap();
        let sent = lines(&shared);
        assert_eq!(sent[0]["type"], "update_environment_variables");
        assert_eq!(
            sent[0]["variables"],
            json!({ "CLAUDE_CODE_OAUTH_TOKEN": "at-7" })
        );
        let request_id = sent[0]["request_id"].as_str().unwrap();
        assert_eq!(credential_push_generation(request_id), Some(7));
        assert_eq!(credential_push_generation("some-other-request"), None);

        let mut one_shot = ClaudeControl::new(Box::new(OneShotInput));
        assert!(one_shot.update_credential(&env, 8).is_err());
    }

    #[test]
    fn a_launch_carries_only_the_credential_its_secrets_set() {
        use crate::interface::session::{ApprovalMode, ClaudeSpec, PlacedFile, SessionSpec};
        let spec = SessionSpec {
            start: SessionStart::New {
                session_id: "s".to_string(),
            },
            model: None,
            effort: None,
            approval: ApprovalMode::Deny,
            output_schema: None,
            cwd: "/tmp".to_string(),
            env: std::collections::BTreeMap::new(),
            mcp: None,
            secrets: SecretEnv::new().with(OAUTH_TOKEN_ENV, "at-SENTINEL"),
            account_id: Some("acct".to_string()),
            provider: ProviderSpec::Claude(ClaudeSpec {
                system_prompt: PlacedFile {
                    dir: "/tmp".to_string(),
                    name: "system.md".to_string(),
                    contents: "answer".to_string(),
                },
                hook_settings: None,
                allowed_tools: vec![],
                disallowed_tools: vec![],
                mode: ClaudeMode::Interactive,
            }),
        };
        let config = ClaudeCli::spawn_config("/bin/claude", &spec, None, None).unwrap();
        assert_eq!(
            config.env.get(OAUTH_TOKEN_ENV).map(String::as_str),
            Some("at-SENTINEL")
        );
        assert!(!config.env_remove.contains(&OAUTH_TOKEN_ENV.to_string()));
        for name in CLAUDE_AUTH_ENV
            .iter()
            .filter(|name| **name != OAUTH_TOKEN_ENV)
        {
            assert!(
                config.env_remove.contains(&name.to_string()),
                "{name} must not be inherited"
            );
        }
    }
}
