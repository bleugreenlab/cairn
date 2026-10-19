//! The launch side of the facet: how a model session is opened, driven, and
//! stopped.
//!
//! Three roles, kept apart so a session can move off this machine without the
//! trait changing:
//!
//! - A [`SessionHost`] is *where* the process runs. It spawns, places files,
//!   and resolves a CLI on its machine. Today that is the runner itself; an
//!   executor-hosted worker is another implementation.
//! - A [`SessionProvider`] is *how* a CLI conversation is opened: a new one,
//!   a resume of its own, or a fork of another's ([`SessionStart`]).
//! - A [`SessionControl`] is the only way to write to a live session: its
//!   first turn, later messages, an interrupt, a model or permission change,
//!   a renewed credential, and the answer to a request the session raised.
//!
//! A session never holds its own credentials. Everything secret arrives from
//! the caller in [`SecretEnv`] (or, for Codex, a caller-supplied login) and
//! reaches only the process environment. The facet never resolves an account;
//! [`SessionSpec::account_id`] is a label for the session's facts.

use std::any::Any;
use std::collections::BTreeMap;
use std::fmt;
use std::io::BufRead;
use std::sync::{Arc, Mutex};

use cairn_platform::services::process::{ChildProcess, SpawnConfig};
use serde_json::Value;

use crate::codex::app_server::AppServerClient;
use crate::codex::thread_router::ThreadRouter;
use crate::session::mcp::McpServers;
use crate::wire::MessageContent;

/// Whether a session starts a new provider conversation, continues its own,
/// or forks another's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionStart {
    New {
        session_id: String,
    },
    Resume {
        session_id: String,
        backend_id: String,
    },
    Fork {
        session_id: String,
        source_backend_id: String,
    },
}

impl SessionStart {
    pub fn session_id(&self) -> &str {
        match self {
            SessionStart::New { session_id }
            | SessionStart::Resume { session_id, .. }
            | SessionStart::Fork { session_id, .. } => session_id,
        }
    }

    pub fn backend_id(&self) -> Option<&str> {
        match self {
            SessionStart::Resume { backend_id, .. } => Some(backend_id),
            _ => None,
        }
    }

    pub fn source_backend_id(&self) -> Option<&str> {
        match self {
            SessionStart::Fork {
                source_backend_id, ..
            } => Some(source_backend_id),
            _ => None,
        }
    }

    /// The provider-side conversation this start replays, if any: a resume
    /// continues its own, a fork replays its source's. A new session replays
    /// nothing.
    pub fn replayed_backend_id(&self) -> Option<&str> {
        self.backend_id().or_else(|| self.source_backend_id())
    }
}

/// Environment values a session must receive but nothing may keep: an MCP
/// credential, an API key, a subscription's credential presentation.
///
/// It has no `Serialize`, and its `Debug` names the variables without their
/// values, so a spec can be logged or put in a panic message without leaking.
/// The values leave only through [`SecretEnv::apply`], into a process's
/// environment.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct SecretEnv(BTreeMap<String, String>);

impl SecretEnv {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.insert(name, value);
        self
    }

    pub fn insert(&mut self, name: impl Into<String>, value: impl Into<String>) {
        self.0.insert(name.into(), value.into());
    }

    pub fn extend(&mut self, other: SecretEnv) {
        self.0.extend(other.0);
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.0.keys().map(String::as_str)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Whether any value equals `value`. For tests that prove a secret did not
    /// travel somewhere it should not.
    pub fn contains_value(&self, value: &str) -> bool {
        self.0.values().any(|candidate| candidate == value)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
    }

    /// Put every value into the process environment `config` will spawn with.
    pub fn apply(&self, mut config: SpawnConfig) -> SpawnConfig {
        for (name, value) in &self.0 {
            config = config.env(name, value);
        }
        config
    }
}

impl fmt::Debug for SecretEnv {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map()
            .entries(self.0.keys().map(|name| (name, "<redacted>")))
            .finish()
    }
}

/// One secret value, with the same discipline as [`SecretEnv`]: no
/// `Serialize`, a redacted `Debug`, and a single way out.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret<T>(T);

impl<T> Secret<T> {
    pub fn new(value: T) -> Self {
        Self(value)
    }

    /// The value, for the one place that presents it.
    pub fn expose(&self) -> &T {
        &self.0
    }
}

impl<T> fmt::Debug for Secret<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(<redacted>)")
    }
}

/// How much a session may do without asking. Enforcement is Cairn's own (the
/// verb handlers' fence); each provider only translates this into its CLI's
/// flags or protocol fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalMode {
    Allow,
    Ask,
    Deny,
}

/// A file the host places before the process starts. Its contents travel
/// with the spec because the process need not share a filesystem with the
/// caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacedFile {
    pub dir: String,
    pub name: String,
    pub contents: String,
}

/// Everything a provider needs to open one session.
#[derive(Debug)]
pub struct SessionSpec {
    pub start: SessionStart,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub approval: ApprovalMode,
    /// Constrain the final output to this JSON Schema, natively.
    pub output_schema: Option<Value>,
    pub cwd: String,
    /// Non-secret environment: run identity, home URI, git identity.
    pub env: BTreeMap<String, String>,
    /// The MCP servers the session is given, built by the caller. Cairn gives
    /// its own `cairn` server; a host outside Cairn gives its own, and need not
    /// include `cairn` at all.
    ///
    /// `None` is a tool-free session: no server and nothing in their place,
    /// and nothing saved to resume. Claude gets an empty strict MCP config and
    /// `--no-session-persistence`, and, like every Claude launch, no built-in
    /// tools (`--tools ""`); a Codex thread is started ephemeral, read-only, with no environment, and
    /// with every server its app-server process was started with disabled for
    /// that thread. Only a new session can be tool-free.
    pub mcp: Option<McpServers>,
    /// The caller's secrets: the MCP credentials an [`McpServers`] entry names,
    /// an API key, a subscription's credential presentation. Never rendered
    /// into a server config.
    pub secrets: SecretEnv,
    /// The account the caller chose, recorded in the session's facts. Never
    /// used to fetch a credential.
    pub account_id: Option<String>,
    pub provider: ProviderSpec,
}

/// What only one provider needs.
#[derive(Debug)]
pub enum ProviderSpec {
    Claude(ClaudeSpec),
    Codex(CodexSpec),
}

pub struct CodexSpec {
    /// The app-server the thread opens on.
    pub server: CodexServer,
    /// `baseInstructions`: the Cairn, workspace, and project layers.
    pub base_instructions: String,
    /// `developerInstructions`: the agent's role and orientation, when any.
    pub developer_instructions: Option<String>,
    pub service_tier: Option<String>,
    pub native_goal: NativeGoal,
    /// A Codex-native tool the thread is given back. Only a tool-free thread
    /// (`SessionSpec::mcp` is `None`) takes one.
    pub native_tools: CodexNativeTools,
    /// The first turn's text when a resumed thread is gone and a fresh one is
    /// rebuilt from Cairn's transcript. Asked only on that fallback; `None`
    /// keeps the original message.
    pub resume_fallback: Option<Box<dyn FnMut() -> Option<String> + Send>>,
}

impl fmt::Debug for CodexSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CodexSpec")
            .field("server", &self.server)
            .field("service_tier", &self.service_tier)
            .field("native_goal", &self.native_goal)
            .field("native_tools", &self.native_tools)
            .finish_non_exhaustive()
    }
}

/// Which app-server a Codex thread opens on. Both came from
/// [`CodexSessionProvider::start_server`]; they differ in who owns the process.
pub enum CodexServer {
    /// This session's own app-server, read by an owned router
    /// ([`ThreadRouter::start_owned`]) from before the thread opens, so a
    /// server request without a `threadId` (the token refresh) is answered by
    /// the router's handler as it arrives, even while `thread/start` is still
    /// pending. `events` is the router's owner channel: every other message,
    /// in order, which the session reads as its notifications. Stopping the
    /// session stops the server.
    Dedicated {
        router: Arc<ThreadRouter>,
        events: crossbeam_channel::Receiver<Value>,
    },
    /// An app-server many threads share. The session holds no process, its
    /// notifications arrive through the server's router (the caller registers
    /// the opened thread on it before the first turn and puts the receiver in
    /// [`SessionEvents::CodexAppServer`]), and the shared thread is not given
    /// the per-thread MCP attachment check.
    ///
    /// A shared thread sees the MCP servers its server process was started
    /// with ([`ServerLaunch::mcp`]), not the ones in its [`SessionSpec`], so a
    /// host that pools servers keys its pool by server set. Cairn's pool is
    /// always the `cairn` server alone.
    Shared(Arc<ThreadRouter>),
}

impl fmt::Debug for CodexServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Dedicated { .. } => "Dedicated",
            Self::Shared(_) => "Shared",
        })
    }
}

/// The Codex-native tools a thread is given. Cairn's Codex home turns them all
/// off; a tool-free thread may take one back for itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CodexNativeTools {
    #[default]
    None,
    /// Codex's built-in image generation (`image_gen.imagegen`), for an image
    /// worker: see `cairn_models::image::codex`.
    ImageGeneration,
}

/// What the thread's native goal should be once it is open.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativeGoal {
    /// Nothing to reconcile.
    Leave,
    /// No native goal at all (a standing thread driven only by admitted turns).
    Clear,
    /// The job's assigned work.
    Objective(String),
}

/// How one Codex app-server process is started.
pub struct ServerLaunch<'a> {
    /// The Codex CLI, as the host resolved it.
    pub cli_path: &'a str,
    pub cwd: &'a str,
    pub env: &'a BTreeMap<String, String>,
    pub secrets: &'a SecretEnv,
    /// The MCP servers the process is started with, one `-c` override each.
    /// Every thread on it sees this set; a tool-free thread switches each off.
    pub mcp: &'a McpServers,
    /// The `account/login/start` params, when the caller authenticates the
    /// server.
    pub login: Option<&'a Secret<Value>>,
}

/// The Codex provider: an app-server is started first, then threads open on
/// it. Every Codex app-server Cairn runs a session on is started here.
pub trait CodexSessionProvider: SessionProvider {
    fn start_server(
        &self,
        host: &dyn SessionHost,
        launch: ServerLaunch<'_>,
    ) -> Result<Arc<AppServerClient>, SessionError>;
}

#[derive(Debug)]
pub struct ClaudeSpec {
    /// The whole assembled system prompt, delivered as `--system-prompt-file`.
    pub system_prompt: PlacedFile,
    /// Hook settings delivered as `--settings`. A failure to place them
    /// leaves the flag off rather than failing the launch.
    pub hook_settings: Option<PlacedFile>,
    pub allowed_tools: Vec<String>,
    pub disallowed_tools: Vec<String>,
    pub mode: ClaudeMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaudeMode {
    /// Stream-json on stdin: the first turn is [`SessionControl::open_turn`].
    Interactive,
    /// Print mode: the CLI takes the one prompt as its last argument.
    OneShot { prompt: String },
}

/// A CLI a host can run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cli {
    Claude,
    Codex,
}

/// Why a session did not open. Each variant's text is the underlying
/// message, unchanged; the variant says which step failed, so a caller can
/// report each the way it always has.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionError {
    /// The CLI is not installed on the host.
    CliNotFound(String),
    /// Cairn could not install the CLI it manages (see
    /// `cairn_models::session::codex::managed::CodexInstallError`); the text
    /// says what failed and how to retry.
    CliInstallFailed(String),
    /// Preparing the launch failed (placing a file, an invalid spec).
    Setup(String),
    /// The process did not start.
    Spawn(String),
    /// The process started but the conversation could not be opened.
    Protocol(String),
    /// The conversation opened, but the session's `cairn` MCP server did not
    /// attach to it.
    McpUnavailable(String),
}

impl fmt::Display for SessionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CliNotFound(text)
            | Self::CliInstallFailed(text)
            | Self::Setup(text)
            | Self::Spawn(text)
            | Self::Protocol(text)
            | Self::McpUnavailable(text) => f.write_str(text),
        }
    }
}

impl std::error::Error for SessionError {}

impl From<SessionError> for String {
    fn from(error: SessionError) -> Self {
        error.to_string()
    }
}

/// Where a session's process runs.
pub trait SessionHost: Send + Sync {
    /// The machine, as [`crate::interface::SessionView::host`] names it.
    fn name(&self) -> &str;
    /// The path of `cli` on this host, checked usable.
    fn resolve_cli(&self, cli: Cli) -> Result<String, SessionError>;
    fn spawn(&self, config: SpawnConfig) -> Result<Box<dyn ChildProcess>, String>;
    /// Write `contents` to `dir/name` on this host, creating `dir`, and
    /// answer the path the process should be told.
    fn place_file(&self, dir: &str, name: &str, contents: &[u8]) -> Result<String, String>;
    /// Prepare the Codex home every app-server on this host runs with (its
    /// hardened config and model catalog) and answer its path, for
    /// `CODEX_HOME`.
    fn prepare_codex_home(&self, models: CodexModels<'_>) -> Result<String, SessionError>;
}

/// Whether preparing the Codex home refreshes its model catalog first.
#[derive(Debug, Clone, Copy)]
pub enum CodexModels<'a> {
    /// Use the catalog the home already has.
    Keep,
    /// Ask the installed CLI for its current catalog first, signed in with
    /// `login` when given.
    Refresh { login: Option<&'a Secret<Value>> },
}

/// How a CLI conversation is opened.
pub trait SessionProvider: Send + Sync {
    fn open(
        &self,
        host: &dyn SessionHost,
        spec: SessionSpec,
    ) -> Result<LaunchedSession, SessionError>;
}

/// The OS process behind a session. Empty for a session with no process of
/// its own (a thread on a shared app-server, an in-process HTTP loop), which
/// is what keeps a stop from signalling anything shared.
pub type ProcessHandle = Arc<Mutex<Option<Box<dyn ChildProcess>>>>;

/// What the session emits, for the caller's reader.
pub enum SessionEvents {
    /// Claude's stream-json on stdout, and its stderr.
    ClaudeStream {
        stdout: Box<dyn BufRead + Send>,
        stderr: Option<Box<dyn BufRead + Send>>,
    },
    /// A Codex thread's app-server notifications. `notifications` is `None`
    /// on a shared server, whose pool routes each thread's own.
    CodexAppServer {
        client: Arc<AppServerClient>,
        notifications: Option<crossbeam_channel::Receiver<Value>>,
        /// The provider turn in flight, shared with the thread's control.
        current_turn: Arc<Mutex<Option<String>>>,
    },
}

impl fmt::Debug for SessionEvents {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ClaudeStream { .. } => f.write_str("ClaudeStream"),
            Self::CodexAppServer { .. } => f.write_str("CodexAppServer"),
        }
    }
}

/// What is known about a live session, carried on it rather than inferred.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionFacts {
    /// The provider key (`claude`, `codex`, `openrouter`, `custom:…`).
    pub provider: String,
    /// The provider's own handle for the conversation, once known.
    pub backend_session_id: Option<String>,
    pub account_id: Option<String>,
    /// The machine the session runs on.
    pub host: String,
    pub model: Option<String>,
}

/// An opened session.
pub struct LaunchedSession {
    pub control: Box<dyn SessionControl>,
    pub events: SessionEvents,
    pub process: ProcessHandle,
    pub facts: SessionFacts,
}

/// One user message to a live session.
#[derive(Debug, Clone, Copy)]
pub struct SessionInput<'a> {
    pub content: &'a MessageContent,
    /// Cairn's session id.
    pub session_id: &'a str,
    pub parent_tool_use_id: Option<&'a str>,
}

/// A live session's one writer, shared by everything that talks to it. Empty
/// once the session can no longer be written to.
pub type SharedControl = Arc<Mutex<Option<Box<dyn SessionControl>>>>;

/// What a [`SharedControl`] that has been emptied answers.
pub const NO_SESSION_CONTROL: &str = "Session control not available";

/// A host's answer to a [`crate::session::events::SessionEvent::ServerRequest`]:
/// the JSON-RPC `result`, or an `error` with its code and message.
///
/// Deliberately not `Debug`: a token request's answer carries an access token.
#[derive(Clone, PartialEq)]
pub enum ServerReply {
    Result(Value),
    Error { code: i32, message: String },
}

/// The only writer to a live session.
pub trait SessionControl: Send + Any {
    /// The conversation's first turn. It is its own operation because each
    /// CLI's opening message differs from a later one (Claude's carries no
    /// session reference; Codex's first `turn/start` carries the output
    /// schema).
    fn open_turn(&mut self, content: &MessageContent) -> Result<(), String>;
    /// A later message.
    fn send(&mut self, input: &SessionInput<'_>) -> Result<(), String>;
    /// Stop the current turn; the session stays open.
    fn interrupt(&mut self) -> Result<(), String>;
    fn set_model(&mut self, model: &str) -> Result<(), String>;
    fn set_permission_mode(&mut self, mode: &str) -> Result<(), String>;
    /// Hand the live process a renewed credential (`generation` of it), so it
    /// authenticates with that from its next request on without restarting.
    /// A provider or mode that cannot take one says so.
    fn update_credential(&mut self, _env: &SecretEnv, _generation: u64) -> Result<(), String> {
        Err("this session cannot take a credential update".to_string())
    }
    /// Answer the server request the session raised as `id`. A provider whose
    /// sessions raise none says so.
    fn reply(&mut self, _id: &Value, _reply: ServerReply) -> Result<(), String> {
        Err("this session takes no replies".to_string())
    }
    /// Provider-specific operations (Codex goals) downcast through here.
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secrets_debug_names_but_never_shows_values() {
        let env = SecretEnv::new()
            .with("ANTHROPIC_API_KEY", "sk-ant-very-secret")
            .with("CAIRN_MCP_TOKEN", "bearer-value");
        let debug = format!("{env:?}");
        assert!(debug.contains("ANTHROPIC_API_KEY"));
        assert!(!debug.contains("sk-ant-very-secret"));
        assert!(!debug.contains("bearer-value"));

        let secret = Secret::new(serde_json::json!({"accessToken": "tok-123"}));
        assert!(!format!("{secret:?}").contains("tok-123"));
        assert_eq!(secret.expose()["accessToken"], "tok-123");
    }

    #[test]
    fn secrets_reach_only_the_spawn_environment() {
        let config = SecretEnv::new()
            .with("CAIRN_MCP_TOKEN", "bearer-value")
            .apply(SpawnConfig::new("claude"));
        assert!(config
            .env
            .iter()
            .any(|(name, value)| name == "CAIRN_MCP_TOKEN" && value == "bearer-value"));
    }

    /// Neither secret type may be serializable: the compile-time half of the
    /// rule that a secret never reaches a stored row.
    #[test]
    fn secret_types_are_not_serializable() {
        use std::marker::PhantomData;
        struct Probe<T>(PhantomData<T>);
        trait Fallback {
            fn serializable(&self) -> bool {
                false
            }
        }
        impl<T> Fallback for Probe<T> {}
        impl<T: serde::Serialize> Probe<T> {
            fn serializable(&self) -> bool {
                true
            }
        }
        // The probe itself answers: a serializable type takes the inherent method.
        assert!(Probe::<String>(PhantomData).serializable());
        assert!(!Probe::<SecretEnv>(PhantomData).serializable());
        assert!(!Probe::<Secret<Value>>(PhantomData).serializable());
    }
}
