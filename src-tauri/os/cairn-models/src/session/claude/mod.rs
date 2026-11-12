//! The Claude CLI as a session provider.
//!
//! [`ClaudeCli`] opens a conversation by starting `claude` in stream-json mode
//! on a [`SessionHost`]; [`ClaudeControl`] is the one writer to its stdin.

pub mod args;

use std::io::Write;

use cairn_common::callback_contract::AGENT_MCP_TOOL_TIMEOUT;
#[cfg(unix)]
use cairn_platform::services::process::InheritedSecret;
use cairn_platform::services::process::SpawnConfig;
use serde_json::json;

pub use crate::claude::api_key::API_KEY_ENV;
use crate::claude::tokens::OAUTH_TOKEN_ENV;
use crate::interface::session::{
    ApprovalMode, ClaudeMode, Cli, LaunchedSession, ProviderSpec, SecretEnv, SessionControl,
    SessionError, SessionEvents, SessionFacts, SessionHost, SessionInput, SessionProvider,
    SessionSpec, SessionStart,
};
use crate::wire::claude_stream;
use crate::wire::MessageContent;
use args::{build_claude_args, ClaudeArgsConfig};

/// The switches every Claude CLI Cairn starts carries: sessions (interactive
/// and one-shot), sign-in, `auth status`, and the managed install's version
/// probe all read this one definition, and the claude-contract suite's
/// no-egress and no-update cases run under it, never a copy.
///
/// `DISABLE_AUTOUPDATER` and `DISABLE_UPDATES` keep the CLI on the release
/// Cairn installed: its self-updater manages the user's own
/// `~/.local/bin/claude`, never Cairn's. `CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC`
/// stops the rest of its own traffic; measured on 2.1.311 on macOS and
/// Linux, a session opened three connections to `api.anthropic.com` beside
/// its model endpoint without it, and none with it.
///
/// Operator decision (harness thread, 2026-11-05): every production Claude
/// launch sets `CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1`, beside
/// `DISABLE_AUTOUPDATER=1` and `DISABLE_UPDATES=1`. Its effect on
/// server-side feature flags is unverified. If a behavior Cairn relies on
/// turns out to be flag-gated and disappears, the remedy is to set that
/// behavior explicitly, through an env var, setting or flag the contract can
/// see. Re-enabling nonessential traffic is never the remedy.
pub const CLAUDE_HARDENING_ENV: &[(&str, &str)] = &[
    ("DISABLE_AUTOUPDATER", "1"),
    ("DISABLE_UPDATES", "1"),
    ("CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC", "1"),
];

/// The environment every Claude session runs with, whoever launches it:
/// [`CLAUDE_HARDENING_ENV`], then the session's own settings.
///
/// Tool search and the CLI's own task list are off because Cairn owns tool
/// discovery and delegation.
///
/// The CLI bounds an MCP tool call twice. `MCP_TOOL_TIMEOUT` caps its length;
/// `CLAUDE_CODE_MCP_TOOL_IDLE_TIMEOUT` abandons a call that has neither
/// answered nor reported progress for that long (by default 300 s over HTTP
/// and 1800 s over stdio). A Cairn call can be held open far longer: a blocking
/// `write` append awaits a sub-agent or a user with no host-side deadline. So
/// both are [`AGENT_MCP_TOOL_TIMEOUT`], and Cairn rather than the client
/// decides when a call ends. Cairn's MCP server sends progress heartbeats on
/// every held call, which is what keeps the idle bound from firing; the idle
/// setting is the backstop for a CLI build that stops honoring them. It is not
/// 0, which would disable the bound even for a wedged socket.
pub fn claude_session_env() -> Vec<(&'static str, String)> {
    let tool_timeout_ms = AGENT_MCP_TOOL_TIMEOUT.as_millis().to_string();
    CLAUDE_HARDENING_ENV
        .iter()
        .map(|(name, value)| (*name, value.to_string()))
        .chain([
            ("ENABLE_TOOL_SEARCH", "false".to_string()),
            ("CLAUDE_CODE_ENABLE_TASKS", "false".to_string()),
            ("MCP_TOOL_TIMEOUT", tool_timeout_ms.clone()),
            ("CLAUDE_CODE_MCP_TOOL_IDLE_TIMEOUT", tool_timeout_ms),
        ])
        .collect()
}

/// Every way a Claude CLI can be handed a credential through its environment.
/// A launch carries exactly the ones its own secrets set; any other value
/// inherited from Cairn's environment (an operator's API key, their own OAuth
/// token, a host-credentials file, a descriptor variable pointing at nothing
/// Cairn opened) is removed, so a process can only ever authenticate as the
/// account it was routed to.
pub const CLAUDE_AUTH_ENV: [&str; 8] = [
    API_KEY_ENV,
    "ANTHROPIC_AUTH_TOKEN",
    "CLAUDE_CODE_OAUTH_TOKEN",
    "CLAUDE_CODE_OAUTH_REFRESH_TOKEN",
    OAUTH_TOKEN_FD_ENV,
    API_KEY_FD_ENV,
    "CLAUDE_CODE_HOST_CREDS_FILE",
    "CLAUDE_CODE_PROVIDER_MANAGED_BY_HOST",
];

/// The variables that tell a Claude CLI which inherited descriptor holds its
/// OAuth token or its API key, and the descriptor a launch puts either on.
pub const OAUTH_TOKEN_FD_ENV: &str = "CLAUDE_CODE_OAUTH_TOKEN_FILE_DESCRIPTOR";
pub const API_KEY_FD_ENV: &str = "CLAUDE_CODE_API_KEY_FILE_DESCRIPTOR";
pub const CREDENTIAL_FD: i32 = 3;

/// Each credential the CLI reads from an inherited descriptor: the name a
/// launch carries it under, and the variable naming its descriptor. A spawn
/// hands over one descriptor, so a launch carries at most one of these.
const DESCRIPTOR_CREDENTIALS: [(&str, &str); 2] = [
    (OAUTH_TOKEN_ENV, OAUTH_TOKEN_FD_ENV),
    (API_KEY_ENV, API_KEY_FD_ENV),
];

/// Why a launch that names no config directory is refused. Without one the CLI
/// runs in `~/.claude`, where the operator's own hooks, `statusLine`, plugins,
/// `CLAUDE.md` and MCP servers load inside the agent's process.
const NO_CONFIG_DIR: &str = "a Claude launch must name its Cairn-owned config directory; it never runs in the user's ~/.claude";

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
        if !names_its_config_dir(spec) {
            return Err(SessionError::Setup(NO_CONFIG_DIR.to_string()));
        }
        let carried: Vec<&str> = DESCRIPTOR_CREDENTIALS
            .iter()
            .map(|(name, _)| *name)
            .filter(|name| spec.secrets.names().any(|secret| secret == *name))
            .collect();
        if carried.len() > 1 {
            return Err(SessionError::Setup(format!(
                "a Claude launch authenticates one way, but this one carries {}",
                carried.join(" and ")
            )));
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
        for (name, value) in claude_session_env() {
            config = config.env(name, &value);
        }
        for (name, value) in &spec.env {
            config = config.env(name, value);
        }
        let mut secrets = spec.secrets.clone();
        let (mut config, descriptor_env) = credential_by_descriptor(&mut secrets, config);
        // Removals apply after every overlay, so only what this launch does not
        // set is removed. An inherited `CLAUDE_CONFIG_DIR` needs no removal:
        // the launch's own, required above, overlays it.
        for name in CLAUDE_AUTH_ENV {
            let set = secrets.names().any(|secret| secret == name) || descriptor_env == Some(name);
            if !set {
                config = config.env_remove(name);
            }
        }
        Ok(secrets.apply(config))
    }
}

/// Whether the launch names a non-empty `CLAUDE_CONFIG_DIR` of its own.
fn names_its_config_dir(spec: &SessionSpec) -> bool {
    spec.secrets
        .iter()
        .chain(
            spec.env
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_str())),
        )
        .any(|(name, value)| name == "CLAUDE_CONFIG_DIR" && !value.is_empty())
}

/// Move the OAuth token or the API key, whichever `secrets` carries, onto an
/// inherited descriptor, and answer which descriptor variable now names it.
///
/// A launch environment is readable by every same-user process for as long as
/// the CLI lives, so the credential never goes there. The CLI reads the
/// descriptor once at startup and keeps the credential in memory; an OAuth
/// rotation is a stdin push (`ClaudeControl::update_credential`), which never
/// rewrites the launch environment either.
#[cfg(unix)]
fn credential_by_descriptor(
    secrets: &mut SecretEnv,
    config: SpawnConfig,
) -> (SpawnConfig, Option<&'static str>) {
    for (name, descriptor_env) in DESCRIPTOR_CREDENTIALS {
        if let Some(credential) = secrets.remove(name) {
            let config = config
                .inherited_secret(InheritedSecret::new(CREDENTIAL_FD, credential))
                .env(descriptor_env, &CREDENTIAL_FD.to_string());
            return (config, Some(descriptor_env));
        }
    }
    (config, None)
}

/// The Claude CLI reads no descriptor on Windows, so the credential stays in
/// the launch environment there: a named residual (`docs/identity.md`).
#[cfg(not(unix))]
fn credential_by_descriptor(
    _secrets: &mut SecretEnv,
    config: SpawnConfig,
) -> (SpawnConfig, Option<&'static str>) {
    (config, None)
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

    /// Claude's stream carries no turn identity to attribute an abort to, so
    /// the cause stays with the caller.
    fn interrupt(
        &mut self,
        _cause: crate::interface::session::InterruptCause,
    ) -> Result<(), String> {
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
            mcp: Some(
                McpServers::new(vec![McpServer {
                    name: "cairn".to_string(),
                    transport: crate::session::mcp::McpTransport::Http {
                        url: "http://o/mcp".to_string(),
                        bearer_env: Some("FIXTURE_MCP_TOKEN".to_string()),
                    },
                    required_tools: Vec::new(),
                }])
                .unwrap(),
            ),
            secrets: SecretEnv::new().with("CLAUDE_CONFIG_DIR", "/cairn/claude-home"),
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

        // Both of the CLI's tool-call bounds are Cairn's shared one.
        let tool_timeout_ms = AGENT_MCP_TOOL_TIMEOUT.as_millis().to_string();
        for name in ["MCP_TOOL_TIMEOUT", "CLAUDE_CODE_MCP_TOOL_IDLE_TIMEOUT"] {
            assert_eq!(config.env.get(name), Some(&tool_timeout_ms), "{name}");
        }
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
            secrets: SecretEnv::new().with("CLAUDE_CONFIG_DIR", "/cairn/claude-home"),
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
            secrets: SecretEnv::new().with("CLAUDE_CONFIG_DIR", "/cairn/claude-home"),
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
        control
            .interrupt(crate::interface::session::InterruptCause::OperatorStop)
            .unwrap();

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

    /// A launch on `credential`, in the config dir `/cairn/claude-home`.
    fn launch_spec(credential: SecretEnv) -> SessionSpec {
        let mut secrets = SecretEnv::new().with("CLAUDE_CONFIG_DIR", CONFIG_DIR);
        secrets.extend(credential);
        bare_launch_spec(secrets)
    }

    const CONFIG_DIR: &str = "/cairn/claude-home";

    fn bare_launch_spec(secrets: SecretEnv) -> SessionSpec {
        use crate::interface::session::{ApprovalMode, ClaudeSpec, PlacedFile};
        SessionSpec {
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
            secrets,
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
        }
    }

    /// Every Claude credential variable is removed from what the CLI would
    /// inherit, except the ones this launch sets itself, and the CLI runs in
    /// the config dir the launch named.
    fn assert_only_these_set(config: &SpawnConfig, set: &[&str]) {
        for name in CLAUDE_AUTH_ENV {
            assert_eq!(
                !config.env_remove.contains(&name.to_string()),
                set.contains(&name),
                "{name}: removed unless this launch sets it"
            );
        }
        assert_eq!(
            config.env.get("CLAUDE_CONFIG_DIR").map(String::as_str),
            Some(CONFIG_DIR)
        );
        assert!(!config.env_remove.contains(&"CLAUDE_CONFIG_DIR".to_string()));
    }

    /// A managed account's token reaches the CLI on its descriptor, announced
    /// by name, and never in its environment; an ambient token is still
    /// removed.
    #[cfg(unix)]
    #[test]
    fn a_managed_launch_hands_the_token_over_on_a_descriptor() {
        let spec = launch_spec(SecretEnv::new().with(OAUTH_TOKEN_ENV, "at-SENTINEL"));
        let config = ClaudeCli::spawn_config("/bin/claude", &spec, None, None).unwrap();
        assert!(!config.env.contains_key(OAUTH_TOKEN_ENV));
        assert!(!config.env.values().any(|value| value == "at-SENTINEL"));
        assert_eq!(
            config.env.get(OAUTH_TOKEN_FD_ENV).map(String::as_str),
            Some("3")
        );
        let secret = config
            .inherited_secret
            .as_ref()
            .expect("an inherited token");
        assert_eq!(secret.child_fd(), CREDENTIAL_FD);
        assert!(secret.is(b"at-SENTINEL"));
        assert_only_these_set(&config, &[OAUTH_TOKEN_FD_ENV]);
    }

    #[cfg(not(unix))]
    #[test]
    fn a_managed_launch_keeps_the_token_in_the_environment_where_no_descriptor_exists() {
        let spec = launch_spec(SecretEnv::new().with(OAUTH_TOKEN_ENV, "at-SENTINEL"));
        let config = ClaudeCli::spawn_config("claude", &spec, None, None).unwrap();
        assert_eq!(
            config.env.get(OAUTH_TOKEN_ENV).map(String::as_str),
            Some("at-SENTINEL")
        );
        assert!(config.inherited_secret.is_none());
        assert_only_these_set(&config, &[OAUTH_TOKEN_ENV]);
    }

    /// An API key reaches the CLI the way a managed token does: on its
    /// descriptor, announced by name, and never in its environment.
    #[cfg(unix)]
    #[test]
    fn an_api_key_launch_hands_the_key_over_on_a_descriptor() {
        let spec = launch_spec(SecretEnv::new().with(API_KEY_ENV, "sk-SENTINEL"));
        let config = ClaudeCli::spawn_config("/bin/claude", &spec, None, None).unwrap();
        assert!(!config.env.contains_key(API_KEY_ENV));
        assert!(!config.env.values().any(|value| value == "sk-SENTINEL"));
        assert_eq!(
            config.env.get(API_KEY_FD_ENV).map(String::as_str),
            Some("3")
        );
        let secret = config.inherited_secret.as_ref().expect("an inherited key");
        assert_eq!(secret.child_fd(), CREDENTIAL_FD);
        assert!(secret.is(b"sk-SENTINEL"));
        assert_only_these_set(&config, &[API_KEY_FD_ENV]);
    }

    #[cfg(not(unix))]
    #[test]
    fn an_api_key_launch_keeps_the_key_in_the_environment_where_no_descriptor_exists() {
        let spec = launch_spec(SecretEnv::new().with(API_KEY_ENV, "sk-SENTINEL"));
        let config = ClaudeCli::spawn_config("claude", &spec, None, None).unwrap();
        assert_eq!(
            config.env.get(API_KEY_ENV).map(String::as_str),
            Some("sk-SENTINEL")
        );
        assert!(config.inherited_secret.is_none());
        assert_only_these_set(&config, &[API_KEY_ENV]);
    }

    /// Nothing can fall through to `~/.claude`: a launch that names no config
    /// dir, or names an empty one, does not start.
    #[test]
    fn a_launch_without_a_config_dir_is_refused() {
        for secrets in [
            SecretEnv::new().with(API_KEY_ENV, "sk-SENTINEL"),
            SecretEnv::new()
                .with(API_KEY_ENV, "sk-SENTINEL")
                .with("CLAUDE_CONFIG_DIR", ""),
        ] {
            let spec = bare_launch_spec(secrets);
            match ClaudeCli::spawn_config("/bin/claude", &spec, None, None) {
                Err(SessionError::Setup(reason)) => assert_eq!(reason, NO_CONFIG_DIR),
                other => panic!("expected a refusal, got {:?}", other.map(|c| c.env)),
            }
        }
    }

    /// What Cairn's own environment holds never decides the CLI's home or its
    /// credential: an inherited `CLAUDE_CONFIG_DIR` is overlaid by the launch's
    /// own, and an inherited API-key descriptor variable is removed from a
    /// launch that set none.
    #[test]
    fn an_inherited_config_dir_or_api_key_descriptor_never_reaches_the_child() {
        let spec = launch_spec(SecretEnv::new().with(OAUTH_TOKEN_ENV, "at-SENTINEL"));
        let config = ClaudeCli::spawn_config("/bin/claude", &spec, None, None).unwrap();
        assert!(config.env_remove.contains(&API_KEY_FD_ENV.to_string()));
        assert!(config.env_remove.contains(&API_KEY_ENV.to_string()));
        assert_eq!(
            config.env.get("CLAUDE_CONFIG_DIR").map(String::as_str),
            Some(CONFIG_DIR)
        );

        // The same holds when the config dir arrives in the plain environment
        // rather than with the credential.
        let mut spec = bare_launch_spec(SecretEnv::new().with(API_KEY_ENV, "sk-SENTINEL"));
        spec.env
            .insert("CLAUDE_CONFIG_DIR".to_string(), CONFIG_DIR.to_string());
        let config = ClaudeCli::spawn_config("/bin/claude", &spec, None, None).unwrap();
        assert!(config.env_remove.contains(&OAUTH_TOKEN_FD_ENV.to_string()));
        assert_eq!(
            config.env.get("CLAUDE_CONFIG_DIR").map(String::as_str),
            Some(CONFIG_DIR)
        );
    }

    /// One descriptor, one credential: a launch carrying both a managed token
    /// and an API key is refused rather than one being silently chosen.
    #[test]
    fn a_launch_carrying_both_credentials_is_refused() {
        let spec = launch_spec(
            SecretEnv::new()
                .with(OAUTH_TOKEN_ENV, "at-SENTINEL")
                .with(API_KEY_ENV, "sk-SENTINEL"),
        );
        match ClaudeCli::spawn_config("/bin/claude", &spec, None, None) {
            Err(SessionError::Setup(reason)) => {
                assert!(reason.contains(API_KEY_ENV), "{reason}");
                assert!(reason.contains(OAUTH_TOKEN_ENV), "{reason}");
                assert!(!reason.contains("SENTINEL"), "{reason}");
            }
            other => panic!("expected a refusal, got {:?}", other.map(|c| c.env)),
        }
    }
}
