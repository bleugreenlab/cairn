//! One case's world: the real pinned Codex installed through `ManagedCodex`,
//! Cairn's real session host and rendered config, the fake provider, and the
//! readers a case asserts with.
//!
//! The only departure from production is [`ContractHost::prepare_codex_home`]:
//! after Cairn writes its hardened `config.toml`, the host adds a model
//! provider pointing at the fake (top-level `model_provider` plus one
//! `[model_providers.contract]` table). Everything else (`initialize`,
//! `thread/start` params, MCP `-c` overrides, goal and interrupt calls) is
//! Cairn's own code.

use std::collections::BTreeMap;
use std::fmt;
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use base64::Engine as _;
use cairn_models::codex::app_server::AppServerClient;
use cairn_models::codex::thread_router::{Scoped, ThreadRouter};
use cairn_models::interface::session::{
    ApprovalMode, Cli, CodexModels, CodexNativeTools, CodexServer, CodexSessionProvider, CodexSpec,
    LaunchedSession, NativeGoal, ProviderSpec, Secret, SecretEnv, ServerLaunch, SessionError,
    SessionEvents, SessionHost, SessionProvider, SessionSpec, SessionStart,
};
use cairn_models::session::codex::managed::{ManagedCodex, RefusingSource};
use cairn_models::session::codex::{pin, CodexAppServer, CodexControl};
use cairn_models::session::local::LocalSessionHost;
use cairn_models::session::mcp::{McpServer, McpServers, McpTransport};
use cairn_models::wire::codex_app_server::{
    Notification, AUTH_TOKENS_REFRESH_METHOD, SUPPORTED_SERVER_REQUESTS,
};
use cairn_models::wire::MessageContent;
use cairn_platform::services::process::{
    ChildProcess, CommandOutput, ProcessSpawner, RealProcessSpawner, SpawnConfig,
};
use crossbeam_channel::Receiver;
use serde_json::{json, Value};

use crate::fake_responses::FakeResponses;

/// Where `bun run codex:contract` staged the pinned package.
pub const ROOT_ENV: &str = "CAIRN_CODEX_CONTRACT_ROOT";

/// The model the fake serves, as the seeded catalog names it.
pub const FAKE_MODEL: &str = "fake-model";

/// The environment variable the contract provider reads its (dummy) key from.
const PROVIDER_KEY_ENV: &str = "CONTRACT_PROVIDER_KEY";

/// A `models_cache.json` for the home, so `write_codex_config` builds its
/// hardened catalog without a signed-in model refresh. One entry, carrying
/// every field Codex's `ModelInfo` requires.
const MODELS_CACHE: &str = r#"{"fetched_at":"2026-10-18T00:00:00Z","client_version":"0.0.0","models":[{"slug":"fake-model","display_name":"Contract fake","description":"The codex-contract suite's fake provider model","default_reasoning_level":"low","supported_reasoning_levels":[{"effort":"low","description":"low"}],"shell_type":"shell_command","visibility":"list","supported_in_api":true,"base_instructions":"You are the codex-contract fake model.","priority":1,"availability_nux":null,"upgrade":null,"support_verbosity":false,"default_verbosity":null,"apply_patch_tool_type":null,"truncation_policy":{"mode":"tokens","limit":10000},"context_window":258400,"experimental_supported_tools":[]}]}"#;

/// A failed contract case, named so a gate negative can prove which case it
/// turned red.
#[derive(Debug)]
pub struct CaseFailure {
    pub case: &'static str,
    pub detail: String,
    /// The case never reached its assertions: installing or starting Codex
    /// failed. A gate negative must not count this as its surface going red.
    pub setup: bool,
}

impl fmt::Display for CaseFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "codex-contract case `{}` failed: {}",
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

/// One way `break_proxy.ts` rewrites the real binary's traffic, to prove the
/// case covering that surface can go red.
#[derive(Debug, Clone, Copy)]
pub enum Surface {
    InterruptMismatchText,
    NoActiveTurnText,
    MissingRolloutText,
    GoalStatusRenamed,
    WillRetryDropped,
    CapacitySentence,
    TurnStatusRenamed,
    McpItemRenamed,
    UnhandledServerRequest,
    ThreadStartParamRejected,
    Egress,
    ConfigWarning,
    ThreadStartAwaitsServerRequest,
    RefreshNeverSent,
}

impl Surface {
    fn arg(self) -> &'static str {
        match self {
            Self::InterruptMismatchText => "interrupt-mismatch-text",
            Self::NoActiveTurnText => "no-active-turn-text",
            Self::MissingRolloutText => "missing-rollout-text",
            Self::GoalStatusRenamed => "goal-status-renamed",
            Self::WillRetryDropped => "will-retry-dropped",
            Self::CapacitySentence => "capacity-sentence",
            Self::TurnStatusRenamed => "turn-status-renamed",
            Self::McpItemRenamed => "mcp-item-renamed",
            Self::UnhandledServerRequest => "unhandled-server-request",
            Self::ThreadStartParamRejected => "thread-start-param-rejected",
            Self::Egress => "egress",
            Self::ConfigWarning => "config-warning",
            Self::ThreadStartAwaitsServerRequest => "thread-start-awaits-server-request",
            Self::RefreshNeverSent => "refresh-never-sent",
        }
    }
}

/// `bun`, which runs the MCP fixture and the break proxy.
pub fn bun() -> String {
    std::env::var_os("PATH")
        .and_then(|path| {
            std::env::split_paths(&path)
                .map(|dir| dir.join("bun"))
                .find(|candidate| candidate.is_file())
        })
        .map(|path| path.to_string_lossy().into_owned())
        .unwrap_or_else(|| "bun".to_string())
}

fn suite_file(name: &str) -> String {
    format!("{}/tests/codex_contract/{name}", env!("CARGO_MANIFEST_DIR"))
}

/// One stdio MCP server with one `echo` tool, configured exactly as Cairn
/// configures any stdio server it hands a Codex process.
pub fn fixture_mcp() -> McpServers {
    McpServers::new(vec![McpServer {
        name: "fixture".to_string(),
        transport: McpTransport::Stdio {
            command: bun(),
            args: vec![suite_file("mcp_fixture.ts")],
            env: BTreeMap::new(),
            inherit_env: Vec::new(),
        },
        required_tools: vec!["echo".to_string()],
    }])
    .expect("the fixture server name is valid")
}

/// A ChatGPT login whose tokens expired long ago (unsigned, `alg: none`),
/// shaped as Cairn feeds a subscription login, so Codex asks the host to
/// refresh it.
pub fn expired_chatgpt_login() -> Secret<Value> {
    let encode =
        |value: Value| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(value.to_string());
    let token = format!(
        "{}.{}.{}",
        encode(json!({"alg": "none", "typ": "JWT"})),
        encode(json!({
            "exp": 1_700_000_000u64, "iat": 1_690_000_000u64,
            "email": "contract@example.invalid",
            "https://api.openai.com/auth": {
                "chatgpt_account_id": "contract-account",
                "chatgpt_plan_type": "pro",
                "chatgpt_user_id": "contract-user"
            }
        })),
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode("unsigned"),
    );
    Secret::new(json!({
        "type": "chatgptAuthTokens",
        "idToken": token,
        "accessToken": token,
        "chatgptAccountId": "contract-account",
    }))
}

/// What a Codex process was sent and what it printed on stderr.
#[derive(Clone, Default)]
struct Tap {
    sent: Arc<Mutex<Vec<String>>>,
    stderr: Arc<Mutex<Vec<String>>>,
}

struct TapSpawner(Tap);

impl ProcessSpawner for TapSpawner {
    fn spawn(&self, config: SpawnConfig) -> Result<Box<dyn ChildProcess>, String> {
        let child = RealProcessSpawner.spawn(config)?;
        Ok(Box::new(TapChild {
            inner: child,
            tap: self.0.clone(),
        }))
    }

    fn run(&self, config: SpawnConfig) -> Result<CommandOutput, String> {
        RealProcessSpawner.run(config)
    }
}

struct TapChild {
    inner: Box<dyn ChildProcess>,
    tap: Tap,
}

impl ChildProcess for TapChild {
    fn id(&self) -> u32 {
        self.inner.id()
    }

    fn take_stdout(&mut self) -> Option<Box<dyn BufRead + Send>> {
        self.inner.take_stdout()
    }

    /// Kept for the failure report; the client then has no stderr to log.
    fn take_stderr(&mut self) -> Option<Box<dyn BufRead + Send>> {
        if let Some(stderr) = self.inner.take_stderr() {
            let lines = self.tap.stderr.clone();
            std::thread::spawn(move || {
                for line in stderr.lines().map_while(Result::ok) {
                    let mut lines = lines.lock().unwrap();
                    if lines.len() == 200 {
                        lines.remove(0);
                    }
                    lines.push(line);
                }
            });
        }
        None
    }

    fn take_stdin(&mut self) -> Option<Box<dyn Write + Send>> {
        let inner = self.inner.take_stdin()?;
        Some(Box::new(TapWriter {
            inner,
            pending: Vec::new(),
            sent: self.tap.sent.clone(),
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

/// Cairn's [`LocalSessionHost`], with the contract provider added to the
/// config it renders and, for a gate negative, the CLI wrapped in the break
/// proxy.
pub struct ContractHost {
    inner: LocalSessionHost,
    /// `(top-level keys, tables)` to add around Cairn's rendered config.
    provider: Option<(String, String)>,
    broken: Option<(Surface, PathBuf)>,
    rendered: Arc<Mutex<Option<String>>>,
}

impl SessionHost for ContractHost {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn resolve_cli(&self, cli: Cli) -> Result<String, SessionError> {
        let real = self.inner.resolve_cli(cli)?;
        let (Cli::Codex, Some((surface, dir))) = (cli, &self.broken) else {
            return Ok(real);
        };
        let wrapper = dir.join("codex-broken");
        let script = format!(
            "#!/bin/sh\nexec '{}' '{}' '{}' '{}' \"$@\"\n",
            bun(),
            suite_file("break_proxy.ts"),
            surface.arg(),
            real
        );
        std::fs::write(&wrapper, script).map_err(|error| SessionError::Setup(error.to_string()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755))
                .map_err(|error| SessionError::Setup(error.to_string()))?;
        }
        Ok(wrapper.to_string_lossy().into_owned())
    }

    fn spawn(&self, config: SpawnConfig) -> Result<Box<dyn ChildProcess>, String> {
        self.inner.spawn(config)
    }

    fn place_file(&self, dir: &str, name: &str, contents: &[u8]) -> Result<String, String> {
        self.inner.place_file(dir, name, contents)
    }

    fn prepare_codex_home(&self, models: CodexModels<'_>) -> Result<String, SessionError> {
        let home = self.inner.prepare_codex_home(models)?;
        let path = Path::new(&home).join("config.toml");
        let rendered = std::fs::read_to_string(&path)
            .map_err(|error| SessionError::Setup(error.to_string()))?;
        *self.rendered.lock().unwrap() = Some(rendered.clone());
        if let Some((top, tables)) = &self.provider {
            std::fs::write(&path, format!("{top}{rendered}{tables}"))
                .map_err(|error| SessionError::Setup(error.to_string()))?;
        }
        Ok(home)
    }
}

/// What a thread opens with, beyond what every contract thread shares.
pub struct Open {
    pub start: SessionStart,
    pub mcp: Option<McpServers>,
    pub goal: NativeGoal,
    pub effort: Option<String>,
    pub model: Option<String>,
}

impl Open {
    /// A new tool-free thread on the fake model.
    pub fn new() -> Self {
        Self {
            start: SessionStart::New {
                session_id: uuid::Uuid::new_v4().to_string(),
            },
            mcp: None,
            goal: NativeGoal::Leave,
            effort: None,
            model: Some(FAKE_MODEL.to_string()),
        }
    }

    pub fn mcp(mut self, servers: McpServers) -> Self {
        self.mcp = Some(servers);
        self
    }

    pub fn resume(mut self, backend_id: &str) -> Self {
        self.start = SessionStart::Resume {
            backend_id: backend_id.to_string(),
            session_id: uuid::Uuid::new_v4().to_string(),
        };
        self
    }

    pub fn effort(mut self, effort: &str) -> Self {
        self.effort = Some(effort.to_string());
        self
    }
}

/// An open thread and its control.
pub struct Session {
    pub launched: LaunchedSession,
    pub thread_id: String,
    pub current_turn: Arc<Mutex<Option<String>>>,
    opened: bool,
}

impl Session {
    pub fn control(&mut self) -> &mut CodexControl {
        self.launched
            .control
            .as_any_mut()
            .downcast_mut::<CodexControl>()
            .expect("a Codex session's control is CodexControl")
    }

    /// Start a turn the way Cairn does (the first through `open_turn`, later
    /// ones through `send_turn`) and answer its id.
    pub fn turn(&mut self, text: &str) -> Result<String, String> {
        let content = MessageContent::text(text);
        if self.opened {
            self.control().send_turn(&content)?;
        } else {
            self.launched.control.open_turn(&content)?;
            self.opened = true;
        }
        self.current_turn
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| "turn/start answered no turn id".to_string())
    }
}

/// The notifications and server requests of one app-server, read as a case
/// needs them from the owner channel of the server's router. Server requests
/// are answered as a Cairn reader with no handler for them would: a token
/// refresh with the refusal code, anything else declined.
pub struct Stream {
    rx: Receiver<Value>,
    client: Arc<AppServerClient>,
    pub seen: Vec<Value>,
    pub requests: Vec<String>,
}

impl Stream {
    fn new(client: &Arc<AppServerClient>, rx: Receiver<Value>) -> Self {
        Self {
            rx,
            client: client.clone(),
            seen: Vec::new(),
            requests: Vec::new(),
        }
    }

    /// Read until `stop` holds for a message, at most `timeout`.
    pub fn until(
        &mut self,
        timeout: Duration,
        mut stop: impl FnMut(&Value) -> bool,
    ) -> Option<Value> {
        let deadline = Instant::now() + timeout;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let message = self.rx.recv_timeout(left).ok()?;
            match Notification::parse(&message) {
                Notification::AuthTokensRefreshRequest { id } => {
                    self.requests.push(AUTH_TOKENS_REFRESH_METHOD.to_string());
                    let _ =
                        self.client
                            .respond_error(id, -32000, "codex-contract has no credential");
                }
                Notification::ServerRequest { id, method } => {
                    self.requests.push(method.to_string());
                    let _ = self
                        .client
                        .respond_error(id, -32601, "codex-contract declines");
                }
                _ => {}
            }
            self.seen.push(message.clone());
            if stop(&message) {
                return Some(message);
            }
        }
    }

    pub fn drain(&mut self, period: Duration) {
        self.until(period, |_| false);
    }

    /// Every server request seen that Cairn has no handler for.
    pub fn unsupported_requests(&self) -> Vec<String> {
        self.requests
            .iter()
            .filter(|method| !SUPPORTED_SERVER_REQUESTS.contains(&method.as_str()))
            .cloned()
            .collect()
    }
}

pub fn method_of(message: &Value) -> Option<&str> {
    message.get("method").and_then(Value::as_str)
}

pub fn thread_of(message: &Value) -> Option<&str> {
    message.pointer("/params/threadId").and_then(Value::as_str)
}

pub struct ContractEnv {
    pub fake: FakeResponses,
    staged: PathBuf,
    dir: tempfile::TempDir,
    managed: Arc<ManagedCodex>,
    codex_home: PathBuf,
    work: PathBuf,
    tap: Tap,
    rendered: Arc<Mutex<Option<String>>>,
    broken: Option<Surface>,
    retries: u32,
    chatgpt_provider: bool,
    servers: Mutex<Vec<Server>>,
}

/// One launched app-server and the owned router reading it, as Cairn's
/// dedicated sessions run it. The router's handler passes everything, so every
/// message (server requests included) reaches the owner channel a [`Stream`]
/// reads, and a request is answered only when a case reads it. That is what
/// lets the refresh cases hold a token refresh back until `thread/start`
/// returns.
struct Server {
    client: Arc<AppServerClient>,
    router: Arc<ThreadRouter>,
    events: Receiver<Value>,
}

impl ContractEnv {
    /// A fresh world for one case. Fails, never skips, when the suite was not
    /// run through `bun run codex:contract`: a skip would read as a green gate
    /// with no evidence behind it.
    pub fn new() -> Result<Self, String> {
        let root = std::env::var_os(ROOT_ENV).ok_or_else(|| {
            format!("{ROOT_ENV} is unset: run `bun run codex:contract`, which stages the pinned Codex package")
        })?;
        let pin = pin::embedded_pin();
        let target = pin::host_target().ok_or("OpenAI publishes no Codex package for this host")?;
        let archive_name = format!("codex-package-{target}-{}.tar.gz", pin.version);
        let staged = Path::new(&root).join("downloads").join(&archive_name);
        if !staged.is_file() {
            return Err(format!(
                "{} is missing: `bun run codex:pin fetch` stages it",
                staged.display()
            ));
        }
        // One install for the whole run, made by whichever test gets there
        // first: unpacking the package per test, in parallel, is slow enough
        // to time out Cairn's own version probe.
        let shared = Path::new(&root).join("managed");
        let managed = Arc::new(install_root(&shared));
        with_install_lock(Path::new(&root), || {
            stage(&shared, &staged)?;
            managed
                .ensure()
                .map(|_| ())
                .map_err(|error| error.to_string())
        })?;
        let dir = tempfile::tempdir().map_err(|error| error.to_string())?;
        let codex_home = dir.path().join("codex-home");
        let work = dir.path().join("work");
        for path in [&codex_home, &work] {
            std::fs::create_dir_all(path).map_err(|error| error.to_string())?;
        }
        std::fs::write(codex_home.join("models_cache.json"), MODELS_CACHE)
            .map_err(|error| error.to_string())?;
        Ok(Self {
            fake: FakeResponses::start().map_err(|error| error.to_string())?,
            staged,
            dir,
            managed,
            codex_home,
            work,
            tap: Tap::default(),
            rendered: Arc::new(Mutex::new(None)),
            broken: None,
            retries: 0,
            chatgpt_provider: false,
            servers: Mutex::new(Vec::new()),
        })
    }

    /// Run the CLI through `break_proxy.ts`, broken for `surface`.
    pub fn broken(mut self, surface: Surface) -> Self {
        self.broken = Some(surface);
        self
    }

    /// Let Codex retry a failed request or stream this many times.
    pub fn retries(mut self, retries: u32) -> Self {
        self.retries = retries;
        self
    }

    /// Sign the provider in with the ChatGPT login, as Cairn's subscription
    /// sessions are, and have the fake reject that login the way the ChatGPT
    /// backend rejects an expired token. Codex's startup prewarm then meets the
    /// rejection inside `thread/start` and asks the host for a refresh, on
    /// every platform and without leaving the machine.
    pub fn chatgpt_provider(mut self) -> Self {
        self.chatgpt_provider = true;
        self.fake.reject_auth();
        self
    }

    pub fn dir(&self) -> &Path {
        self.dir.path()
    }

    pub fn managed(&self) -> &Arc<ManagedCodex> {
        &self.managed
    }

    /// A `ManagedCodex` with nothing installed yet and the staged package in
    /// its downloads, as a host has after a manual download.
    pub fn fresh_managed(&self) -> Result<ManagedCodex, String> {
        let root = self.dir.path().join("fresh");
        stage(&root, &self.staged)?;
        Ok(install_root(&root))
    }

    /// The `config.toml` Cairn rendered, before the contract provider.
    pub fn rendered_config(&self) -> Option<String> {
        self.rendered.lock().unwrap().clone()
    }

    /// Every line Cairn wrote to a Codex process's stdin.
    pub fn sent(&self) -> Vec<String> {
        self.tap.sent.lock().unwrap().clone()
    }

    pub fn host(&self) -> ContractHost {
        let base = self.fake.base_url();
        let retries = self.retries;
        ContractHost {
            inner: LocalSessionHost::new(
                Arc::new(TapSpawner(self.tap.clone())),
                &self.codex_home,
                self.managed.clone(),
            ),
            provider: Some({
                let auth = if self.chatgpt_provider {
                    "requires_openai_auth = true\nsupports_websockets = true\n".to_string()
                } else {
                    format!("env_key = \"{PROVIDER_KEY_ENV}\"\n")
                };
                (
                    "model_provider = \"contract\"\n".to_string(),
                    format!(
                        "\n[model_providers.contract]\nname = \"contract\"\nbase_url = \"{base}\"\nwire_api = \"responses\"\n{auth}request_max_retries = {retries}\nstream_max_retries = {retries}\nstream_idle_timeout_ms = 60000\n"
                    ),
                )
            }),
            broken: self
                .broken
                .map(|surface| (surface, self.dir.path().to_path_buf())),
            rendered: self.rendered.clone(),
        }
    }

    /// Start an app-server through Cairn's own `start_server`, with the
    /// production environment plus the egress trap.
    pub fn launch(
        &self,
        mcp: Option<&McpServers>,
        login: Option<&Secret<Value>>,
    ) -> Result<Arc<AppServerClient>, String> {
        let host = self.host();
        let cli = host.resolve_cli(Cli::Codex)?;
        let home = host.prepare_codex_home(CodexModels::Keep)?;
        let proxy = self.fake.proxy_url();
        let mut env = BTreeMap::from([
            ("CODEX_HOME".to_string(), home),
            ("CAIRN_RUN_ID".to_string(), "codex-contract".to_string()),
            (
                "RUST_LOG".to_string(),
                std::env::var("CAIRN_CODEX_CONTRACT_RUST_LOG")
                    .unwrap_or_else(|_| "warn".to_string()),
            ),
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
        let secrets = SecretEnv::new().with(PROVIDER_KEY_ENV, "contract");
        let none = McpServers::new(Vec::new()).expect("an empty server set is valid");
        let started = Instant::now();
        let client = CodexAppServer
            .start_server(
                &host,
                ServerLaunch {
                    cli_path: &cli,
                    cwd: &self.work.to_string_lossy(),
                    env: &env,
                    secrets: &secrets,
                    mcp: mcp.unwrap_or(&none),
                    login,
                },
            )
            .map_err(|error| {
                format!(
                    "{error} (after {:?})\ncodex stderr:\n{}",
                    started.elapsed(),
                    self.stderr_tail()
                )
            })?;
        let (router, events) = ThreadRouter::start_owned(client.clone(), |_, _| Scoped::Passed);
        self.servers.lock().unwrap().push(Server {
            client: client.clone(),
            router,
            events,
        });
        Ok(client)
    }

    /// The router and owner channel of `client`, launched by [`Self::launch`].
    fn server(&self, client: &Arc<AppServerClient>) -> (Arc<ThreadRouter>, Receiver<Value>) {
        let servers = self.servers.lock().unwrap();
        let server = servers
            .iter()
            .find(|server| Arc::ptr_eq(&server.client, client))
            .expect("every client a case uses comes from ContractEnv::launch");
        (server.router.clone(), server.events.clone())
    }

    /// A reader over `client`'s messages.
    pub fn stream(&self, client: &Arc<AppServerClient>) -> Stream {
        Stream::new(client, self.server(client).1)
    }

    /// Open a thread on `client` through Cairn's `CodexAppServer::open`.
    pub fn open(&self, client: &Arc<AppServerClient>, open: Open) -> Result<Session, String> {
        let (router, events) = self.server(client);
        let spec = SessionSpec {
            start: open.start,
            model: open.model,
            effort: open.effort,
            approval: ApprovalMode::Allow,
            output_schema: None,
            cwd: self.work.to_string_lossy().into_owned(),
            env: BTreeMap::new(),
            mcp: open.mcp,
            secrets: SecretEnv::new(),
            account_id: None,
            provider: ProviderSpec::Codex(CodexSpec {
                server: CodexServer::Dedicated { router, events },
                base_instructions: "You are the codex-contract test agent.".to_string(),
                developer_instructions: None,
                service_tier: None,
                native_goal: open.goal,
                native_tools: CodexNativeTools::None,
                resume_fallback: Some(Box::new(|| Some("contract transcript".to_string()))),
            }),
        };
        let launched = CodexAppServer.open(&self.host(), spec)?;
        let thread_id = launched
            .facts
            .backend_session_id
            .clone()
            .ok_or("the opened session names no thread")?;
        let SessionEvents::CodexAppServer { current_turn, .. } = &launched.events else {
            return Err("a Codex session emits app-server events".to_string());
        };
        let current_turn = current_turn.clone();
        Ok(Session {
            launched,
            thread_id,
            current_turn,
            opened: false,
        })
    }

    /// The case's own egress record is empty.
    pub fn no_egress(&self, check: Check) -> CaseResult {
        let egress = self.fake.egress();
        check.that(egress.is_empty(), || {
            format!("Codex tried to reach the network under Cairn's config: {egress:?}")
        })
    }

    /// The case's egress record holds nothing but refused
    /// `CONNECT chatgpt.com:443`, the one connection a ChatGPT-signed-in
    /// Codex 0.163.0 makes that Cairn's config cannot switch off: the
    /// app-server's remote-control websocket loop
    /// (`codex_app_server_transport::transport::remote_control`), which dials
    /// `chatgpt.com/backend-api/` at start even while its status is
    /// `disabled`. Only managed requirements or an internal daemon variable
    /// stop it. The trap refused every attempt, so nothing left the machine;
    /// any other host, or a request that is not a CONNECT, is still red.
    pub fn only_remote_control_egress(&self, check: Check) -> CaseResult {
        let egress = self.fake.egress();
        let other: Vec<&String> = egress
            .iter()
            .filter(|attempt| attempt.as_str() != "CONNECT chatgpt.com:443")
            .collect();
        check.that(other.is_empty(), || {
            format!("Codex tried to reach more than the remote-control backend under Cairn's config: {other:?} (all refused attempts: {egress:?})")
        })
    }

    /// Codex's last stderr lines, for a failure report.
    pub fn stderr_tail(&self) -> String {
        let lines = self.tap.stderr.lock().unwrap();
        lines[lines.len().saturating_sub(15)..].join("\n")
    }
}

/// A `ManagedCodex` rooted at `root` that can never download.
fn install_root(root: &Path) -> ManagedCodex {
    ManagedCodex::new(
        root.join("tools").join("codex"),
        root.join("bin"),
        Arc::new(RefusingSource),
        Arc::new(RefusingSource),
    )
}

/// Put `staged` in the downloads of the `ManagedCodex` at `root`, where
/// `ensure()` installs it from without a download. Never writes over an
/// existing file: a hard link shares the staged package's inode, so copying
/// onto it would truncate the package itself.
fn stage(root: &Path, staged: &Path) -> Result<(), String> {
    let downloads = root.join("tools").join("codex").join("downloads");
    std::fs::create_dir_all(&downloads).map_err(|error| error.to_string())?;
    let archive = downloads.join(staged.file_name().ok_or("the staged package has no name")?);
    if archive.exists() || std::fs::hard_link(staged, &archive).is_ok() {
        return Ok(());
    }
    let partial = downloads.join(format!(".staging-{}", std::process::id()));
    std::fs::copy(staged, &partial).map_err(|error| error.to_string())?;
    std::fs::rename(&partial, &archive).map_err(|error| error.to_string())
}

/// Run `install` holding a lock file in `dir`, so the test processes nextest
/// runs in parallel install the shared package once.
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

impl Drop for ContractEnv {
    fn drop(&mut self) {
        for server in self.servers.lock().unwrap().drain(..) {
            server.client.shutdown();
        }
    }
}
