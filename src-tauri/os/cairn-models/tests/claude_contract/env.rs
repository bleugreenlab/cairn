//! One case's world: the real pinned Claude Code installed through
//! `ManagedTool`, launched by Cairn's own `LocalSessionHost` and
//! `ClaudeCli::open` (and so `ClaudeCli::spawn_config`, Cairn's argument
//! vector and environment, `CLAUDE_HARDENING_ENV` included), read through the
//! wire crate's parser, against the fake Messages API.
//!
//! The only departures from production, all set in the launch's own
//! `SessionSpec::env` (which `spawn_config` overlays after Cairn's session
//! environment, so the hardening env is Cairn's own, never a copy):
//!
//! - `ANTHROPIC_BASE_URL`, at the fake Messages API;
//! - `HTTPS_PROXY`/`HTTP_PROXY`/`ALL_PROXY` (and their lowercase forms) at
//!   the egress trap, with `NO_PROXY` exempting loopback;
//! - `CLAUDE_CODE_MAX_RETRIES`, for the error cases only;
//! - dummy credentials, handed through Cairn's real descriptor path
//!   (`ClaudeSessionCredential::for_launch`, `api_key_launch`).
//!
//! Around the launch is the sandbox a test process needs and a host already
//! is: the CLI inherits nothing from the test's own environment but a system
//! `PATH`, a fake `HOME` populated as an operator's (`~/.claude` hooks,
//! `CLAUDE.md`, skills and `~/.claude.json` MCP servers), and a `TMPDIR` of
//! the case's own. The working directory is under `/tmp`, outside any real
//! home, below an ancestor planted with a project `.claude/skills/planted`,
//! `CLAUDE.md` and `.mcp.json`. Each case has a fresh `CLAUDE_CONFIG_DIR`.

use std::collections::BTreeMap;
use std::fmt;
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cairn_models::claude::api_key::api_key_launch;
use cairn_models::claude::tokens::{ClaudeOAuth, ClaudeSessionCredential};
use cairn_models::interface::session::{
    ApprovalMode, ClaudeMode, ClaudeSpec, Cli, CodexModels, PlacedFile, ProcessHandle,
    ProviderSpec, SessionControl, SessionError, SessionEvents, SessionHost, SessionInput,
    SessionProvider, SessionSpec, SessionStart,
};
use cairn_models::managed::claude::CLAUDE;
use cairn_models::managed::codex::CODEX;
use cairn_models::managed::{ManagedTool, ManagedTools, RefusingSource};
use cairn_models::session::claude::ClaudeCli;
use cairn_models::session::local::LocalSessionHost;
use cairn_models::session::mcp::{
    McpServer, McpServers, McpTransport, CAIRN_SERVER_NAME, CAIRN_TOOLS,
};

/// The variable the contract's HTTP MCP server reads its bearer from. A
/// fixture name: Cairn attaches its own agents over stdio, never this way.
const FIXTURE_MCP_TOKEN_ENV: &str = "FIXTURE_MCP_TOKEN";
use cairn_models::wire::claude_stream::init::CORE_VERBS;
use cairn_models::wire::claude_stream::{parse_event, ClaudeEvent, Init};
use cairn_models::wire::MessageContent;
use cairn_platform::services::process::{
    ChildProcess, CommandOutput, ProcessSpawner, RealProcessSpawner, SpawnConfig,
};
use crossbeam_channel::{unbounded, Receiver};
use serde_json::{json, Value};

use crate::fake_messages::{FakeMessages, McpStub};

/// Where `bun run claude:contract` staged the pinned binary.
pub const ROOT_ENV: &str = "CAIRN_CLAUDE_CONTRACT_ROOT";

/// Record mode: with a directory here, the tool-surface cases write the raw
/// `system`/`init` event they saw to `<dir>/claude_init_<case>-<version>.json`
/// (the fixtures under `tests/fixtures/`).
pub const RECORD_ENV: &str = "CAIRN_CLAUDE_CONTRACT_RECORD";

/// How long one turn may take.
pub const TURN: Duration = Duration::from_secs(90);

/// The bearer Cairn's HTTP MCP attachment carries.
pub const MCP_BEARER: &str = "mcp-contract-bearer";
/// The dummy OAuth token a launch carries unless a case names another.
pub const OAUTH_TOKEN: &str = "sk-ant-oat01-contract-dummy";
/// A dummy key in the shape of a real Anthropic API key.
pub const API_KEY: &str = "sk-ant-api03-DUMMY-contract-key-000000000000000000000000-0000AA";

/// Canaries only the planted project ancestor holds.
pub const ANCESTOR_CANARY: &str = "CANARY-ancestor-claude-md-41c9";
pub const ANCESTOR_SKILL_CANARY: &str = "CANARY-ancestor-skill-8d2e";
/// Canaries only the fake operator's `~/.claude` holds.
pub const OPERATOR_CANARY: &str = "CANARY-operator-claude-md-7f3a";
pub const OPERATOR_SKILL_CANARY: &str = "CANARY-operator-skill-2b61";

/// A failed contract case, named so a gate negative can prove which case it
/// turned red.
#[derive(Debug)]
pub struct CaseFailure {
    pub case: &'static str,
    pub detail: String,
    /// The case never reached its assertions: installing or starting the CLI
    /// failed. A gate negative must not count this as its surface going red.
    pub setup: bool,
}

impl fmt::Display for CaseFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "claude-contract case `{}` failed: {}",
            self.case, self.detail
        )
    }
}

pub type CaseResult = Result<(), CaseFailure>;

/// The assertions of one named case.
#[derive(Clone, Copy)]
pub struct Check(pub &'static str);

impl Check {
    pub fn fail(self, detail: impl Into<String>) -> CaseFailure {
        CaseFailure {
            case: self.0,
            detail: detail.into(),
            setup: false,
        }
    }

    /// A step that only prepares the case; its failure is a setup failure.
    pub fn setup<T, E: fmt::Display>(
        self,
        result: Result<T, E>,
        what: &str,
    ) -> Result<T, CaseFailure> {
        result.map_err(|error| CaseFailure {
            setup: true,
            ..self.fail(format!("{what}: {error}"))
        })
    }

    pub fn that(self, holds: bool, detail: impl FnOnce() -> String) -> CaseResult {
        if holds {
            Ok(())
        } else {
            Err(self.fail(detail()))
        }
    }

    pub fn ok<T, E: fmt::Display>(
        self,
        result: Result<T, E>,
        what: &str,
    ) -> Result<T, CaseFailure> {
        result.map_err(|error| self.fail(format!("{what}: {error}")))
    }

    pub fn some<T>(self, value: Option<T>, what: &str) -> Result<T, CaseFailure> {
        value.ok_or_else(|| self.fail(format!("never observed {what}")))
    }
}

/// One way `break_relay.ts` rewrites the real binary's stdio, to prove the
/// case covering that surface can go red.
#[derive(Debug, Clone, Copy)]
pub enum Surface {
    ClaudeVersionRenamed,
    CoreVerbsDropped,
    BuiltinToolOffered,
    IsErrorDropped,
    UsageFieldRenamed,
    RateLimitResetRenamed,
    RateLimitStatusRenamed,
    ControlResponseReshaped,
    ToolResultRenamed,
    ResumeMissText,
    SessionInUseText,
    UnsolicitedControlRequest,
    Egress,
}

impl Surface {
    fn arg(self) -> &'static str {
        match self {
            Self::ClaudeVersionRenamed => "claude-version-renamed",
            Self::CoreVerbsDropped => "core-verbs-dropped",
            Self::BuiltinToolOffered => "builtin-tool-offered",
            Self::IsErrorDropped => "is-error-dropped",
            Self::UsageFieldRenamed => "usage-field-renamed",
            Self::RateLimitResetRenamed => "rate-limit-reset-renamed",
            Self::RateLimitStatusRenamed => "rate-limit-status-renamed",
            Self::ControlResponseReshaped => "control-response-reshaped",
            Self::ToolResultRenamed => "tool-result-renamed",
            Self::ResumeMissText => "resume-miss-text",
            Self::SessionInUseText => "session-in-use-text",
            Self::UnsolicitedControlRequest => "unsolicited-control-request",
            Self::Egress => "egress",
        }
    }
}

/// `bun`, which runs the break relay.
fn bun() -> String {
    std::env::var_os("PATH")
        .and_then(|path| {
            std::env::split_paths(&path)
                .map(|dir| dir.join("bun"))
                .find(|candidate| candidate.is_file())
        })
        .map(|path| path.to_string_lossy().into_owned())
        .unwrap_or_else(|| "bun".to_string())
}

pub fn suite_file(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("claude_contract")
        .join(name)
}

pub fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

/// The environment a launch inherits: a system `PATH`, the fake operator
/// home, and the case's own temp dir. Nothing else of the test's own.
#[derive(Clone)]
struct Sandbox {
    home: PathBuf,
    tmp: PathBuf,
}

impl Sandbox {
    fn seal(&self, mut config: SpawnConfig) -> SpawnConfig {
        for (name, value) in [
            ("PATH", "/usr/bin:/bin:/usr/sbin:/sbin".to_string()),
            ("HOME", self.home.to_string_lossy().into_owned()),
            ("TMPDIR", self.tmp.to_string_lossy().into_owned()),
        ] {
            if !config.env.contains_key(name) {
                config = config.env(name, &value);
            }
        }
        let inherited: Vec<String> = std::env::vars_os()
            .filter_map(|(name, _)| name.into_string().ok())
            .filter(|name| !config.env.contains_key(name))
            .collect();
        for name in inherited {
            config = config.env_remove(&name);
        }
        config
    }
}

/// Every line Cairn wrote to a CLI's stdin, and the argument vector of every
/// spawn.
#[derive(Clone, Default)]
pub struct Tap {
    sent: Arc<Mutex<Vec<String>>>,
    spawned: Arc<Mutex<Vec<Vec<String>>>>,
}

impl Tap {
    pub fn sent(&self) -> Vec<String> {
        self.sent.lock().unwrap().clone()
    }

    pub fn spawned(&self) -> Vec<Vec<String>> {
        self.spawned.lock().unwrap().clone()
    }

    /// The `request_id` of the last control request of `subtype` Cairn sent.
    pub fn last_control_request(&self, subtype: &str) -> Option<String> {
        self.sent().iter().rev().find_map(|line| {
            let message: Value = serde_json::from_str(line).ok()?;
            (message["type"] == "control_request" && message["request"]["subtype"] == subtype)
                .then(|| message["request_id"].as_str().map(str::to_string))
                .flatten()
        })
    }
}

struct TapChild {
    inner: Box<dyn ChildProcess>,
    sent: Arc<Mutex<Vec<String>>>,
}

impl ChildProcess for TapChild {
    fn id(&self) -> u32 {
        self.inner.id()
    }

    fn take_stdout(&mut self) -> Option<Box<dyn BufRead + Send>> {
        self.inner.take_stdout()
    }

    fn take_stderr(&mut self) -> Option<Box<dyn BufRead + Send>> {
        self.inner.take_stderr()
    }

    fn take_stdin(&mut self) -> Option<Box<dyn Write + Send>> {
        let inner = self.inner.take_stdin()?;
        Some(Box::new(TapWriter {
            inner,
            pending: Vec::new(),
            sent: self.sent.clone(),
        }))
    }

    fn try_wait(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
        self.inner.try_wait()
    }

    fn kill(&mut self) -> std::io::Result<()> {
        self.inner.kill()
    }
}

struct TapWriter {
    inner: Box<dyn Write + Send>,
    pending: Vec<u8>,
    sent: Arc<Mutex<Vec<String>>>,
}

impl Write for TapWriter {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(data)?;
        self.pending.extend_from_slice(&data[..written]);
        while let Some(end) = self.pending.iter().position(|byte| *byte == b'\n') {
            let line: Vec<u8> = self.pending.drain(..=end).collect();
            self.sent
                .lock()
                .unwrap()
                .push(String::from_utf8_lossy(&line).trim().to_string());
        }
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// The sandbox's spawner: what Cairn's host hands its process spawner, sealed
/// and tapped.
struct SandboxSpawner {
    sandbox: Sandbox,
    tap: Tap,
    /// Names removed from every launch: only the `UpdaterEnabled` negative
    /// sets any.
    strip: Vec<&'static str>,
}

impl SandboxSpawner {
    fn prepare(&self, config: SpawnConfig) -> SpawnConfig {
        let mut config = self.sandbox.seal(config);
        for name in &self.strip {
            config = config.env_remove(name);
        }
        let mut argv = vec![config.program.clone()];
        argv.extend(config.args.iter().cloned());
        self.tap.spawned.lock().unwrap().push(argv);
        config
    }
}

impl ProcessSpawner for SandboxSpawner {
    fn spawn(&self, config: SpawnConfig) -> Result<Box<dyn ChildProcess>, String> {
        let child = RealProcessSpawner.spawn(self.prepare(config))?;
        Ok(Box::new(TapChild {
            inner: child,
            sent: self.tap.sent.clone(),
        }))
    }

    fn run(&self, config: SpawnConfig) -> Result<CommandOutput, String> {
        RealProcessSpawner.run(self.prepare(config))
    }
}

/// Cairn's [`LocalSessionHost`], with, for a gate negative, the CLI wrapped
/// in the break relay.
pub struct ContractHost {
    inner: LocalSessionHost,
    broken: Option<(Surface, PathBuf)>,
}

impl SessionHost for ContractHost {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn resolve_cli(&self, cli: Cli) -> Result<String, SessionError> {
        let real = self.inner.resolve_cli(cli)?;
        let (Cli::Claude, Some((surface, dir))) = (cli, &self.broken) else {
            return Ok(real);
        };
        let wrapper = dir.join("claude-broken");
        let script = format!(
            "#!/bin/sh\nexec '{}' '{}' '{}' '{}' \"$@\"\n",
            bun(),
            suite_file("break_relay.ts").display(),
            surface.arg(),
            real
        );
        std::fs::write(&wrapper, script).map_err(|error| SessionError::Setup(error.to_string()))?;
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755))
            .map_err(|error| SessionError::Setup(error.to_string()))?;
        Ok(wrapper.to_string_lossy().into_owned())
    }

    fn spawn(&self, config: SpawnConfig) -> Result<Box<dyn ChildProcess>, String> {
        self.inner.spawn(config)
    }

    fn place_file(&self, dir: &str, name: &str, contents: &[u8]) -> Result<String, String> {
        self.inner.place_file(dir, name, contents)
    }

    fn prepare_codex_home(&self, _: CodexModels<'_>) -> Result<String, SessionError> {
        Err(SessionError::Setup(
            "the claude-contract host runs no Codex".to_string(),
        ))
    }
}

/// The credential a launch carries, by Cairn's own descriptor path.
#[derive(Clone, Copy)]
pub enum Credential {
    OAuth(&'static str),
    ApiKey(&'static str),
}

/// The MCP servers a launch attaches.
#[derive(Clone)]
pub enum Mcp {
    /// A server named `cairn` offering Cairn's three verbs, over streamable
    /// HTTP with a bearer read from the environment.
    Cairn,
    /// None: a tool-free launch.
    Without,
    Servers(McpServers),
}

/// What one launch opens with, beyond what every contract launch shares.
pub struct Launch {
    pub start: SessionStart,
    pub credential: Credential,
    pub mcp: Mcp,
    pub mode: ClaudeMode,
    pub schema: Option<Value>,
    pub model: Option<String>,
    pub retries: Option<u32>,
    pub hook_settings: Option<String>,
    /// A `CLAUDE_CONFIG_DIR` other than the case's own.
    pub config: Option<PathBuf>,
}

impl Launch {
    /// A new interactive agent session with Cairn's MCP attached, on a
    /// managed account's dummy OAuth token.
    pub fn new() -> Self {
        Self {
            start: SessionStart::New {
                session_id: uuid::Uuid::new_v4().to_string(),
            },
            credential: Credential::OAuth(OAUTH_TOKEN),
            mcp: Mcp::Cairn,
            mode: ClaudeMode::Interactive,
            schema: None,
            model: None,
            retries: None,
            hook_settings: None,
            config: None,
        }
    }

    pub fn session(mut self, session_id: &str) -> Self {
        self.start = SessionStart::New {
            session_id: session_id.to_string(),
        };
        self
    }

    pub fn resume(mut self, backend_id: &str) -> Self {
        self.start = SessionStart::Resume {
            backend_id: backend_id.to_string(),
            session_id: uuid::Uuid::new_v4().to_string(),
        };
        self
    }

    pub fn fork(mut self, source_backend_id: &str, session_id: &str) -> Self {
        self.start = SessionStart::Fork {
            session_id: session_id.to_string(),
            source_backend_id: source_backend_id.to_string(),
        };
        self
    }

    pub fn credential(mut self, credential: Credential) -> Self {
        self.credential = credential;
        self
    }

    pub fn mcp(mut self, mcp: Mcp) -> Self {
        self.mcp = mcp;
        self
    }

    /// A print-mode one-shot: tool-free, its prompt fed by `ClaudeCli::open`.
    pub fn one_shot(mut self, prompt: &str) -> Self {
        self.mode = ClaudeMode::OneShot {
            prompt: prompt.to_string(),
        };
        self.mcp = Mcp::Without;
        self
    }

    pub fn schema(mut self, schema: Value) -> Self {
        self.schema = Some(schema);
        self
    }

    pub fn model(mut self, model: &str) -> Self {
        self.model = Some(model.to_string());
        self
    }

    pub fn retries(mut self, retries: u32) -> Self {
        self.retries = Some(retries);
        self
    }

    pub fn hook_settings(mut self, settings: String) -> Self {
        self.hook_settings = Some(settings);
        self
    }

    pub fn config(mut self, config: PathBuf) -> Self {
        self.config = Some(config);
        self
    }
}

/// One stdout line: the raw JSON (or text) and, when Cairn's parser read it,
/// the typed event.
#[derive(Clone, Debug)]
pub struct Frame {
    pub raw: Value,
    pub event: Option<ClaudeEvent>,
}

impl Frame {
    pub fn kind(&self) -> &str {
        self.raw["type"].as_str().unwrap_or_default()
    }

    pub fn subtype(&self) -> &str {
        self.raw["subtype"].as_str().unwrap_or_default()
    }

    pub fn init(&self) -> Option<Init> {
        self.event.as_ref().and_then(Init::of)
    }
}

/// A launched session, read as Cairn reads one: every stdout line through
/// `claude_stream::parse_event`.
pub struct Session {
    pub control: Box<dyn SessionControl>,
    process: ProcessHandle,
    rx: Receiver<Frame>,
    pub frames: Vec<Frame>,
    stderr: Arc<Mutex<Vec<String>>>,
    /// The id Cairn launched the session under (`--session-id`, or the
    /// resumed one).
    pub session_id: String,
    opened: bool,
    tap: Tap,
}

impl Session {
    fn start(launched: cairn_models::interface::session::LaunchedSession, tap: Tap) -> Self {
        let SessionEvents::ClaudeStream { stdout, stderr } = launched.events else {
            unreachable!("a Claude session emits stream-json");
        };
        let (tx, rx) = unbounded();
        std::thread::spawn(move || {
            for line in stdout.lines().map_while(Result::ok) {
                if line.trim().is_empty() {
                    continue;
                }
                let frame = match parse_event(&line) {
                    Ok((event, raw)) => Frame {
                        raw,
                        event: Some(event),
                    },
                    Err(_) => Frame {
                        raw: serde_json::from_str(&line).unwrap_or(Value::String(line)),
                        event: None,
                    },
                };
                if tx.send(frame).is_err() {
                    return;
                }
            }
        });
        let lines = Arc::new(Mutex::new(Vec::new()));
        if let Some(stderr) = stderr {
            let keep = lines.clone();
            std::thread::spawn(move || {
                for line in stderr.lines().map_while(Result::ok) {
                    keep.lock().unwrap().push(line);
                }
            });
        }
        Self {
            control: launched.control,
            process: launched.process,
            rx,
            frames: Vec::new(),
            stderr: lines,
            session_id: launched.facts.backend_session_id.unwrap_or_default(),
            opened: false,
            tap,
        }
    }

    /// Send a user turn the way Cairn does: the first through `open_turn`,
    /// later ones through `send`.
    pub fn turn(&mut self, text: &str) -> Result<(), String> {
        let content = MessageContent::text(text);
        if self.opened {
            self.control.send(&SessionInput {
                content: &content,
                session_id: &self.session_id,
                parent_tool_use_id: None,
            })
        } else {
            self.opened = true;
            self.control.open_turn(&content)
        }
    }

    /// Read until `stop` holds for a frame, at most `timeout`.
    pub fn until(
        &mut self,
        timeout: Duration,
        mut stop: impl FnMut(&Frame) -> bool,
    ) -> Option<Frame> {
        let deadline = Instant::now() + timeout;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let frame = self.rx.recv_timeout(left).ok()?;
            self.frames.push(frame.clone());
            if stop(&frame) {
                return Some(frame);
            }
        }
    }

    /// Read everything that arrives within `period`.
    pub fn drain(&mut self, period: Duration) {
        self.until(period, |_| false);
    }

    /// The turn's `result`, or a failure carrying what the CLI did say.
    pub fn result(&mut self, c: Check) -> Result<Frame, CaseFailure> {
        match self.until(TURN, |frame| frame.kind() == "result") {
            Some(frame) => Ok(frame),
            None => Err(c.fail(format!(
                "the turn never produced a result\n{}",
                self.report()
            ))),
        }
    }

    /// Send a turn and read it to its `result`.
    pub fn run_turn(&mut self, c: Check, text: &str) -> Result<Frame, CaseFailure> {
        c.ok(self.turn(text), "sending a turn through ClaudeControl")?;
        self.result(c)
    }

    /// [`Self::run_turn`], requiring the turn to succeed.
    pub fn ok_turn(&mut self, c: Check, text: &str) -> Result<Frame, CaseFailure> {
        let result = self.run_turn(c, text)?;
        let failed = matches!(
            result.event,
            Some(ClaudeEvent::Result { is_error: true, .. })
        ) || result.event.is_none();
        if failed {
            return Err(c.fail(format!(
                "the turn failed: {}\n{}",
                result.raw,
                self.report()
            )));
        }
        Ok(result)
    }

    /// The session's `init`, read so far.
    pub fn init(&self) -> Option<(Frame, Init)> {
        self.frames
            .iter()
            .find_map(|frame| frame.init().map(|init| (frame.clone(), init)))
    }

    pub fn events(&self) -> impl Iterator<Item = &ClaudeEvent> {
        self.frames.iter().filter_map(|frame| frame.event.as_ref())
    }

    pub fn stderr(&self) -> Vec<String> {
        self.stderr.lock().unwrap().clone()
    }

    pub fn pid(&self) -> Option<u32> {
        self.process
            .lock()
            .unwrap()
            .as_ref()
            .map(|child| child.id())
    }

    /// Whether the process is still running.
    pub fn alive(&self) -> bool {
        self.process
            .lock()
            .unwrap()
            .as_mut()
            .is_some_and(|child| matches!(child.try_wait(), Ok(None)))
    }

    /// Wait up to `timeout` for the process to exit, answering its status.
    pub fn wait_exit(&self, timeout: Duration) -> Option<std::process::ExitStatus> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(child) = self.process.lock().unwrap().as_mut() {
                if let Ok(Some(status)) = child.try_wait() {
                    return Some(status);
                }
            }
            if Instant::now() > deadline {
                return None;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// Close stdin, as Cairn does when a session ends, and wait for the CLI
    /// to exit, so its transcript is complete for a resume.
    pub fn close(mut self) {
        self.control = Box::new(Closed);
        if self.wait_exit(Duration::from_secs(20)).is_none() {
            if let Some(child) = self.process.lock().unwrap().as_mut() {
                let _ = child.kill();
            }
        }
    }

    pub fn tap(&self) -> &Tap {
        &self.tap
    }

    /// What a failure shows: the last frames, and stderr.
    pub fn report(&self) -> String {
        let frames: Vec<String> = self
            .frames
            .iter()
            .rev()
            .take(12)
            .rev()
            .map(|frame| frame.raw.to_string().chars().take(400).collect())
            .collect();
        let stderr = self.stderr();
        format!(
            "frames:\n{}\nstderr:\n{}",
            frames.join("\n"),
            stderr[stderr.len().saturating_sub(15)..].join("\n")
        )
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        if let Some(child) = self.process.lock().unwrap().as_mut() {
            let _ = child.kill();
        }
    }
}

/// The control a closed session is left holding.
struct Closed;

impl SessionControl for Closed {
    fn open_turn(&mut self, _: &MessageContent) -> Result<(), String> {
        Err("closed".to_string())
    }
    fn send(&mut self, _: &SessionInput<'_>) -> Result<(), String> {
        Err("closed".to_string())
    }
    fn interrupt(
        &mut self,
        _cause: cairn_models::interface::session::InterruptCause,
    ) -> Result<(), String> {
        Err("closed".to_string())
    }
    fn set_model(&mut self, _: &str) -> Result<(), String> {
        Err("closed".to_string())
    }
    fn set_permission_mode(&mut self, _: &str) -> Result<(), String> {
        Err("closed".to_string())
    }
    fn update_credential(
        &mut self,
        _: &cairn_models::interface::session::SecretEnv,
        _: u64,
    ) -> Result<(), String> {
        Err("closed".to_string())
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub struct ContractEnv {
    pub fake: FakeMessages,
    pub mcp: McpStub,
    staged: PathBuf,
    dir: tempfile::TempDir,
    managed: Arc<ManagedTool>,
    /// The fake operator's `HOME`.
    pub home: PathBuf,
    /// This case's `CLAUDE_CONFIG_DIR`.
    pub config: PathBuf,
    /// Every launch's working directory.
    pub work: PathBuf,
    /// Where the operator's and the project's hooks leave their marks.
    pub markers: PathBuf,
    tmp: PathBuf,
    broken: Option<Surface>,
    strip: Vec<&'static str>,
    tap: Tap,
}

impl ContractEnv {
    /// A fresh world for one case. Fails, never skips, when the suite was not
    /// run through `bun run claude:contract`: a skip would read as a green
    /// gate with no evidence behind it.
    pub fn new() -> Result<Self, String> {
        let root = std::env::var_os(ROOT_ENV).ok_or_else(|| {
            format!("{ROOT_ENV} is unset: run `bun run claude:contract`, which stages the pinned Claude Code binary")
        })?;
        let pin = CLAUDE.embedded();
        let target = (CLAUDE.host_target)()
            .ok_or("Anthropic publishes no Claude Code build for this host")?;
        let staged = Path::new(&root)
            .join("downloads")
            .join((CLAUDE.download_file_name)(target, &pin.version));
        if !staged.is_file() {
            return Err(format!(
                "{} is missing: `bun run claude:contract` stages it",
                staged.display()
            ));
        }
        // One install for the whole run, made by whichever test gets there
        // first; the tests nextest runs in parallel share it.
        let shared = Path::new(&root).join("managed");
        let managed = Arc::new(install_root(&shared));
        with_install_lock(Path::new(&root), || {
            if managed.installed().is_none() {
                stage(&shared, &staged)?;
            }
            managed
                .ensure()
                .map(|_| ())
                .map_err(|error| error.to_string())
        })?;

        // Under /tmp, not the test's TMPDIR: an executor's TMPDIR can sit
        // under the real home, whose `.claude` the CLI would walk up into.
        let dir = tempfile::Builder::new()
            .prefix("claude-contract-")
            .tempdir_in("/tmp")
            .map_err(|error| error.to_string())?;
        let base = dir
            .path()
            .canonicalize()
            .map_err(|error| error.to_string())?;
        if let Some(real_home) = std::env::var_os("HOME") {
            if base.starts_with(&real_home) {
                return Err(format!(
                    "{} is under the real home {}; the CLI would read its .claude",
                    base.display(),
                    Path::new(&real_home).display()
                ));
            }
        }
        let home = base.join("home");
        let config = base.join("claude-config");
        let ancestor = base.join("project");
        let work = ancestor.join("work");
        let markers = base.join("markers");
        let tmp = base.join("tmp");
        for path in [&home, &config, &work, &markers, &tmp] {
            std::fs::create_dir_all(path).map_err(|error| error.to_string())?;
        }
        plant_operator_home(&home, &markers).map_err(|error| error.to_string())?;
        plant_project(&ancestor, &markers).map_err(|error| error.to_string())?;
        Ok(Self {
            fake: FakeMessages::start().map_err(|error| error.to_string())?,
            mcp: McpStub::start().map_err(|error| error.to_string())?,
            staged,
            dir,
            managed,
            home,
            config,
            work,
            markers,
            tmp,
            broken: None,
            strip: Vec::new(),
            tap: Tap::default(),
        })
    }

    /// Run the CLI through `break_relay.ts`, broken for `surface`.
    pub fn broken(mut self, surface: Surface) -> Self {
        self.broken = Some(surface);
        self
    }

    /// Launch without `DISABLE_AUTOUPDATER` and `DISABLE_UPDATES`: the
    /// `UpdaterEnabled` gate negative.
    pub fn updater_enabled(mut self) -> Self {
        self.strip = vec!["DISABLE_AUTOUPDATER", "DISABLE_UPDATES"];
        self
    }

    pub fn dir(&self) -> &Path {
        self.dir.path()
    }

    pub fn tap(&self) -> &Tap {
        &self.tap
    }

    /// A managed Claude with nothing installed yet and the staged binary in
    /// its downloads, as a host has after a manual download.
    pub fn fresh_managed(&self) -> Result<ManagedTool, String> {
        let root = self.dir.path().join("fresh");
        stage(&root, &self.staged)?;
        Ok(install_root(&root))
    }

    fn spawner(&self) -> SandboxSpawner {
        SandboxSpawner {
            sandbox: Sandbox {
                home: self.home.clone(),
                tmp: self.tmp.clone(),
            },
            tap: self.tap.clone(),
            strip: self.strip.clone(),
        }
    }

    pub fn host(&self) -> ContractHost {
        ContractHost {
            inner: LocalSessionHost::new(
                Arc::new(self.spawner()),
                self.dir.path().join("codex-home"),
                ManagedTools {
                    codex: Arc::new(ManagedTool::new(
                        &CODEX,
                        self.dir.path().join("no-codex"),
                        self.dir.path().join("bin"),
                        Arc::new(RefusingSource),
                        Arc::new(RefusingSource),
                    )),
                    claude: self.managed.clone(),
                },
            ),
            broken: self
                .broken
                .map(|surface| (surface, self.dir.path().to_path_buf())),
        }
    }

    /// The launch's departures from production: where the model is, and the
    /// trap every other request goes through.
    fn departures(&self, retries: Option<u32>) -> BTreeMap<String, String> {
        let proxy = self.fake.proxy_url();
        let mut env = BTreeMap::from([
            ("ANTHROPIC_BASE_URL".to_string(), self.fake.base_url()),
            ("NO_PROXY".to_string(), "127.0.0.1,localhost".to_string()),
            ("no_proxy".to_string(), "127.0.0.1,localhost".to_string()),
        ]);
        for name in [
            "HTTPS_PROXY",
            "https_proxy",
            "HTTP_PROXY",
            "http_proxy",
            "ALL_PROXY",
            "all_proxy",
        ] {
            env.insert(name.to_string(), proxy.clone());
        }
        if let Some(retries) = retries {
            env.insert("CLAUDE_CODE_MAX_RETRIES".to_string(), retries.to_string());
        }
        env
    }

    /// Open a session through Cairn's `ClaudeCli::open`.
    pub fn launch(&self, launch: Launch) -> Result<Session, String> {
        let config = launch.config.clone().unwrap_or_else(|| self.config.clone());
        let mut secrets = match launch.credential {
            Credential::OAuth(token) => {
                ClaudeSessionCredential::for_launch(&oauth(token)?, &config).env
            }
            Credential::ApiKey(key) => api_key_launch(key, &config),
        };
        let mcp = match launch.mcp {
            Mcp::Cairn => {
                secrets = secrets.with(FIXTURE_MCP_TOKEN_ENV, MCP_BEARER);
                Some(
                    McpServers::new(vec![McpServer {
                        name: CAIRN_SERVER_NAME.to_string(),
                        transport: McpTransport::Http {
                            url: self.mcp.url.clone(),
                            bearer_env: Some(FIXTURE_MCP_TOKEN_ENV.to_string()),
                        },
                        required_tools: CAIRN_TOOLS.iter().map(|tool| tool.to_string()).collect(),
                    }])
                    .map_err(|error| error.to_string())?,
                )
            }
            Mcp::Without => None,
            Mcp::Servers(servers) => Some(servers),
        };
        let one_shot = matches!(launch.mode, ClaudeMode::OneShot { .. });
        let spec = SessionSpec {
            start: launch.start,
            model: launch.model,
            effort: None,
            approval: if one_shot {
                ApprovalMode::Deny
            } else {
                ApprovalMode::Allow
            },
            output_schema: launch.schema,
            cwd: self.work.to_string_lossy().into_owned(),
            env: self.departures(launch.retries),
            mcp: mcp.clone(),
            secrets,
            account_id: None,
            provider: ProviderSpec::Claude(ClaudeSpec {
                system_prompt: PlacedFile {
                    dir: self
                        .dir
                        .path()
                        .join("prompt")
                        .to_string_lossy()
                        .into_owned(),
                    name: "system.md".to_string(),
                    contents: "You are the claude-contract test agent.".to_string(),
                },
                hook_settings: launch.hook_settings.map(|contents| PlacedFile {
                    dir: self
                        .dir
                        .path()
                        .join("prompt")
                        .to_string_lossy()
                        .into_owned(),
                    name: "settings.json".to_string(),
                    contents,
                }),
                allowed_tools: if mcp.is_some() {
                    CORE_VERBS.map(str::to_string).to_vec()
                } else {
                    Vec::new()
                },
                disallowed_tools: Vec::new(),
                mode: launch.mode,
            }),
        };
        let launched = ClaudeCli
            .open(&self.host(), spec)
            .map_err(|error| error.to_string())?;
        Ok(Session::start(launched, self.tap.clone()))
    }

    /// Run the installed CLI with `args` the way Cairn runs any Claude
    /// command outside a session: under `CLAUDE_HARDENING_ENV`, through the
    /// host's spawner, with this case's departures.
    pub fn run_cli(&self, cli: &Path, args: &[&str], timeout: Duration) -> Result<CliRun, String> {
        let mut config = SpawnConfig::new(&cli.to_string_lossy()).cwd(&self.work.to_string_lossy());
        for arg in args {
            config = config.arg(arg);
        }
        for (name, value) in cairn_models::session::claude::CLAUDE_HARDENING_ENV {
            config = config.env(name, value);
        }
        for (name, value) in self.departures(None) {
            config = config.env(&name, &value);
        }
        config = config.env("CLAUDE_CONFIG_DIR", &self.config.to_string_lossy());
        let mut child = self.spawner().spawn(config)?;
        let collect = |pipe: Option<Box<dyn BufRead + Send>>| {
            let lines = Arc::new(Mutex::new(Vec::new()));
            if let Some(pipe) = pipe {
                let keep = lines.clone();
                std::thread::spawn(move || {
                    for line in pipe.lines().map_while(Result::ok) {
                        keep.lock().unwrap().push(line);
                    }
                });
            }
            lines
        };
        let stdout = collect(child.take_stdout());
        let stderr = collect(child.take_stderr());
        let deadline = Instant::now() + timeout;
        let status = loop {
            if let Ok(Some(status)) = child.try_wait() {
                break Some(status);
            }
            if Instant::now() > deadline {
                let _ = child.kill();
                break None;
            }
            std::thread::sleep(Duration::from_millis(50));
        };
        std::thread::sleep(Duration::from_millis(200));
        let lines = |kept: &Arc<Mutex<Vec<String>>>| kept.lock().unwrap().join("\n");
        Ok(CliRun {
            status,
            stdout: lines(&stdout),
            stderr: lines(&stderr),
        })
    }

    /// Write `init` as a fixture when [`RECORD_ENV`] names a directory.
    pub fn record_init(&self, case: &str, raw: &Value) {
        let Ok(dir) = std::env::var(RECORD_ENV) else {
            return;
        };
        let version = raw["claude_code_version"].as_str().unwrap_or("unknown");
        let _ = std::fs::create_dir_all(&dir);
        let path = Path::new(&dir).join(format!("claude_init_{case}-{version}.json"));
        // Paths and ids are this run's; the fixture keeps only what tests read.
        let kept = json!({
            "type": raw["type"],
            "subtype": raw["subtype"],
            "claude_code_version": raw["claude_code_version"],
            "tools": raw["tools"],
            "mcp_servers": raw["mcp_servers"],
            "model": raw["model"],
            "permissionMode": raw["permissionMode"],
        });
        if std::fs::write(&path, serde_json::to_string_pretty(&kept).unwrap() + "\n").is_ok() {
            eprintln!("recorded the init event to {}", path.display());
        }
    }
}

/// What one non-session CLI run printed.
pub struct CliRun {
    pub status: Option<std::process::ExitStatus>,
    pub stdout: String,
    pub stderr: String,
}

/// A managed account's OAuth credential carrying `token`.
fn oauth(token: &str) -> Result<ClaudeOAuth, String> {
    ClaudeOAuth::from_cli_credentials(
        &json!({"claudeAiOauth": {"accessToken": token, "expiresAt": 4_102_444_800_000i64}})
            .to_string(),
    )
}

/// The operator's `~/.claude` under `home`: hooks and a `statusLine` that
/// touch markers, a `CLAUDE.md` and a skill holding canaries, and a
/// `~/.claude.json` declaring a user MCP server.
fn plant_operator_home(home: &Path, markers: &Path) -> std::io::Result<()> {
    let touch = |name: &str| format!("touch {}", markers.join(name).display());
    let hook = |name: &str| json!([{"hooks": [{"type": "command", "command": touch(name)}]}]);
    let dot_claude = home.join(".claude");
    std::fs::create_dir_all(dot_claude.join("skills").join("operator-skill"))?;
    std::fs::write(
        dot_claude.join("settings.json"),
        json!({
            "hooks": {
                "SessionStart": hook("operator-SessionStart"),
                "UserPromptSubmit": hook("operator-UserPromptSubmit"),
            },
            "statusLine": {"type": "command", "command": touch("operator-statusLine")},
        })
        .to_string(),
    )?;
    std::fs::write(
        dot_claude.join("CLAUDE.md"),
        format!("Always mention {OPERATOR_CANARY}.\n"),
    )?;
    std::fs::write(
        dot_claude
            .join("skills")
            .join("operator-skill")
            .join("SKILL.md"),
        format!("---\nname: operator-skill\ndescription: {OPERATOR_SKILL_CANARY}\n---\nOperator skill.\n"),
    )?;
    std::fs::write(
        home.join(".claude.json"),
        json!({"mcpServers": {"operator-probe": {
            "type": "stdio", "command": "/bin/sh", "args": ["-c", touch("operator-mcp")],
        }}})
        .to_string(),
    )
}

/// A project above the working directory: a skill, a `CLAUDE.md`, project
/// settings with a hook, and a `.mcp.json` server, each of which only a
/// project-scope load would pick up.
fn plant_project(ancestor: &Path, markers: &Path) -> std::io::Result<()> {
    let touch = |name: &str| format!("touch {}", markers.join(name).display());
    let dot_claude = ancestor.join(".claude");
    std::fs::create_dir_all(dot_claude.join("skills").join("planted"))?;
    std::fs::write(
        dot_claude.join("skills").join("planted").join("SKILL.md"),
        format!("---\nname: planted\ndescription: {ANCESTOR_SKILL_CANARY}\n---\nPlanted skill.\n"),
    )?;
    std::fs::write(
        dot_claude.join("settings.json"),
        json!({"hooks": {"UserPromptSubmit": [{"hooks": [{"type": "command", "command": touch("project-UserPromptSubmit")}]}]}})
            .to_string(),
    )?;
    std::fs::write(
        ancestor.join("CLAUDE.md"),
        format!("Always mention {ANCESTOR_CANARY}.\n"),
    )?;
    std::fs::write(
        ancestor.join(".mcp.json"),
        json!({"mcpServers": {"project-probe": {
            "type": "stdio", "command": "/bin/sh", "args": ["-c", touch("project-mcp")],
        }}})
        .to_string(),
    )
}

/// A managed Claude rooted at `root` that can never download.
fn install_root(root: &Path) -> ManagedTool {
    ManagedTool::new(
        &CLAUDE,
        root.join("tools").join("claude"),
        root.join("bin"),
        Arc::new(RefusingSource),
        Arc::new(RefusingSource),
    )
}

/// Put `staged` in the downloads of the managed Claude at `root`, where
/// `ensure()` installs it from without a download. The install moves the
/// download into place, so the staged file is linked (or copied), never
/// moved itself.
fn stage(root: &Path, staged: &Path) -> Result<(), String> {
    let downloads = root.join("tools").join("claude").join("downloads");
    std::fs::create_dir_all(&downloads).map_err(|error| error.to_string())?;
    let download = downloads.join(staged.file_name().ok_or("the staged binary has no name")?);
    if download.exists() || std::fs::hard_link(staged, &download).is_ok() {
        return Ok(());
    }
    let partial = downloads.join(format!(".staging-{}", std::process::id()));
    std::fs::copy(staged, &partial).map_err(|error| error.to_string())?;
    std::fs::rename(&partial, &download).map_err(|error| error.to_string())
}

/// Run `install` holding a lock file in `dir`, so the test processes nextest
/// runs in parallel install the shared binary once.
fn with_install_lock(
    dir: &Path,
    install: impl FnOnce() -> Result<(), String>,
) -> Result<(), String> {
    let lock = dir.join("install.lock");
    let deadline = Instant::now() + Duration::from_secs(600);
    loop {
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&lock)
        {
            Ok(_) => break,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                if Instant::now() > deadline {
                    return Err(format!("{} was held for ten minutes", lock.display()));
                }
                std::thread::sleep(Duration::from_millis(200));
            }
            Err(error) => return Err(format!("cannot take {}: {error}", lock.display())),
        }
    }
    let installed = install();
    let _ = std::fs::remove_file(&lock);
    installed
}

/// A live process's launch environment, the block `KERN_PROCARGS2` and
/// `/proc/<pid>/environ` show any process of the same user. Only names and
/// containment checks leave it, never a value: on a real machine the values
/// would be real credentials.
pub struct LaunchEnvironment(Vec<Vec<u8>>);

impl LaunchEnvironment {
    pub fn of(pid: u32) -> Self {
        Self(launch_environment_entries(pid))
    }

    pub fn names(&self) -> impl Iterator<Item = String> + '_ {
        self.0.iter().filter_map(|entry| {
            let name = entry.split(|byte| *byte == b'=').next()?;
            Some(String::from_utf8_lossy(name).into_owned())
        })
    }

    pub fn holds(&self, needle: &str) -> bool {
        let needle = needle.as_bytes();
        self.0
            .iter()
            .any(|entry| entry.windows(needle.len()).any(|window| window == needle))
    }

    /// Whether one entry is exactly `name=value`.
    pub fn has(&self, name: &str, value: &str) -> bool {
        let entry = format!("{name}={value}");
        self.0.iter().any(|candidate| candidate == entry.as_bytes())
    }
}

/// `KERN_PROCARGS2` is the argument count, the executable path, NUL padding,
/// the arguments, then the environment up to the first empty string.
#[cfg(target_os = "macos")]
fn launch_environment_entries(pid: u32) -> Vec<Vec<u8>> {
    let mut mib = [libc::CTL_KERN, libc::KERN_PROCARGS2, pid as libc::c_int];
    let mut buf = vec![0u8; 1 << 20];
    let mut size = buf.len();
    // SAFETY: `mib` names three levels and `buf` is writable for `size` bytes.
    let rc = unsafe {
        libc::sysctl(
            mib.as_mut_ptr(),
            3,
            buf.as_mut_ptr().cast(),
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    };
    if rc != 0 {
        return Vec::new();
    }
    buf.truncate(size);
    let argc = i32::from_ne_bytes(buf[..4].try_into().unwrap()) as usize;
    let mut fields = buf[4..].split(|byte| *byte == 0);
    fields.next();
    let mut fields = fields.skip_while(|field| field.is_empty());
    for _ in 0..argc {
        fields.next();
    }
    fields
        .take_while(|field| !field.is_empty())
        .map(<[u8]>::to_vec)
        .collect()
}

#[cfg(not(target_os = "macos"))]
fn launch_environment_entries(pid: u32) -> Vec<Vec<u8>> {
    std::fs::read(format!("/proc/{pid}/environ"))
        .unwrap_or_default()
        .split(|byte| *byte == 0)
        .filter(|entry| !entry.is_empty())
        .map(<[u8]>::to_vec)
        .collect()
}
