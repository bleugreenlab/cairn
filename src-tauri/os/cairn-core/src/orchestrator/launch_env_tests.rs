//! No callback credential reaches the launch environment of a Claude process,
//! a Codex app-server, or the `cairn-cmd` shim either starts, on macOS and
//! Linux (tenet #1381, `cairn_common::auth::FORBIDDEN_LAUNCH_ENV`).
//!
//! Each test launches through `launch::start_session`, the path every run
//! takes, with the real Claude and Codex providers on a host that spawns for
//! real. Only the CLI is a stand-in: a script that writes its argv and its
//! environment to files and exits. Every credential is planted where a launch
//! could pick it up: a canary for each in the runner's own environment (the
//! test runs again in a child holding them, `launch_canary::in_planted_child`),
//! and the credentials the launch itself composes, the run's bearer and the
//! account's key, which are searched for by value. The shim's environment is
//! what the CLI's own environment and its rendered `cairn` server entry give
//! it, so both are searched.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use cairn_common::auth::launch_canary;
use cairn_models::interface::session::{Cli, CodexModels, SessionError, SessionHost};
use cairn_models::session::local::LocalSessionHost;
use cairn_platform::services::process::{ChildProcess, RealProcessSpawner, SpawnConfig};

use super::launch_coverage_tests::config;
use super::models::tests::seeded_orchestrator;
use super::models::ModelRuntime;
use super::Orchestrator;
use crate::backends::{backend_for_name, launch};

const HOME_URI: &str = "cairn://p/COVERAGE/1/1/builder";
/// What the coverage identity signs Claude in with, delivered on a descriptor.
const CLAUDE_KEY: &str = "sk-ant-coverage-claude-key";
/// What it signs Codex in with, delivered in a JSON-RPC login.
const CODEX_KEY: &str = "sk-coverage-codex-key";

/// A stand-in CLI that records how it was launched into `dir` and exits.
fn stand_in(dir: &Path) -> String {
    format!(
        "printf '%s\\n' \"$@\" > '{dir}/args'\nenv > '{dir}/env.part'\nmv '{dir}/env.part' '{dir}/env'\nexit 0",
        dir = dir.display()
    )
}

/// The local host, spawning the planted CLIs for real, with a Codex home that
/// needs no signed-in model refresh.
struct StandInHost {
    local: LocalSessionHost,
    codex_home: PathBuf,
}

impl SessionHost for StandInHost {
    fn name(&self) -> &str {
        self.local.name()
    }
    fn resolve_cli(&self, cli: Cli) -> Result<String, SessionError> {
        self.local.resolve_cli(cli)
    }
    fn spawn(&self, config: SpawnConfig) -> Result<Box<dyn ChildProcess>, String> {
        self.local.spawn(config)
    }
    fn place_file(&self, dir: &str, name: &str, contents: &[u8]) -> Result<String, String> {
        self.local.place_file(dir, name, contents)
    }
    fn prepare_codex_home(&self, _models: CodexModels<'_>) -> Result<String, SessionError> {
        std::fs::create_dir_all(&self.codex_home)
            .map_err(|e| SessionError::Setup(e.to_string()))?;
        Ok(self.codex_home.display().to_string())
    }
}

struct Launched {
    orch: Orchestrator,
    /// Where the stand-in for `cli` records its launch.
    claude: PathBuf,
    codex: PathBuf,
    _temp: tempfile::TempDir,
}

async fn orchestrator() -> Launched {
    let temp = tempfile::tempdir().unwrap();
    let (claude, codex) = (
        temp.path().join("claude-launch"),
        temp.path().join("codex-launch"),
    );
    std::fs::create_dir_all(&claude).unwrap();
    std::fs::create_dir_all(&codex).unwrap();
    let managed = cairn_models::managed::testing::tools_planted(
        &temp.path().join("managed"),
        &stand_in(&claude),
        &stand_in(&codex),
    );
    let mut orch = seeded_orchestrator().await;
    // Every facet but the host and its CLIs stays the real one the builder
    // installs, Claude and Codex included.
    orch.models_runtime = ModelRuntime {
        host: Arc::new(StandInHost {
            local: LocalSessionHost::new(
                Arc::new(RealProcessSpawner),
                temp.path().join("codex-home"),
                managed.clone(),
            ),
            codex_home: temp.path().join("codex-home"),
        }),
        managed,
        ..orch.models_runtime.clone()
    };
    Launched {
        orch,
        claude,
        codex,
        _temp: temp,
    }
}

async fn seed_run(orch: &Orchestrator, run_id: &str, session_id: &str) {
    orch.db
        .local
        .execute(
            "INSERT INTO runs(id, status, session_id, created_at, updated_at)
             VALUES (?1, 'starting', ?2, 1, 1)",
            cairn_db::turso::params![run_id, session_id],
        )
        .await
        .unwrap();
}

/// Launch `backend` for a new run as the orchestrator composes it: the runner's
/// own stdio attachment and a bearer minted for it. Answers the bearer's value.
async fn launch(orch: &Orchestrator, backend: &'static str, ephemeral: bool) -> String {
    let (run_id, session_id) = (format!("run-{backend}"), format!("session-{backend}"));
    seed_run(orch, &run_id, &session_id).await;
    let mut launch_config = config(&run_id, &session_id, "sonnet", ephemeral);
    if backend == "codex" {
        launch_config.model = Some(crate::models::Model::new("gpt-coverage"));
    }
    launch_config.mcp = crate::config::mcp_setup::runner_stdio_attachment(orch, Some(HOME_URI));
    let bearer =
        crate::mcp::run_bearer::bearer_for_attachment(orch, &launch_config.mcp, &run_id, HOME_URI)
            .unwrap();
    let audience = crate::mcp::run_bearer::audience_for_url(
        &crate::fleet::residency::runner_callback_url(orch),
    )
    .unwrap();
    let bearer_value = crate::mcp::run_bearer::service_credential(&bearer, &audience).unwrap();
    launch_config.mcp_bearer = Some(bearer);
    let launch_orch = orch.clone();
    // The stand-in exits at once, so the launch may well report a failure;
    // what it spawned is the evidence. On a thread of its own, so the test's
    // runtime does not wait out whatever the launch then waits for.
    let runtime = tokio::runtime::Handle::current();
    std::thread::spawn(move || {
        let _entered = runtime.enter();
        let _ = launch::start_session(
            &launch_orch,
            backend,
            backend_for_name(Some(backend)).as_ref(),
            launch_config,
        );
    });
    bearer_value
}

/// The stand-in's argv and environment, once it has written them.
async fn recorded(dir: &Path) -> (String, String) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while !dir.join("env").is_file() {
        assert!(
            Instant::now() < deadline,
            "the stand-in in {} never ran",
            dir.display()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    (
        std::fs::read_to_string(dir.join("args")).unwrap(),
        std::fs::read_to_string(dir.join("env")).unwrap(),
    )
}

fn socket(orch: &Orchestrator) -> String {
    crate::mcp::run_bearer::callback_socket_path(&orch.config_dir)
        .display()
        .to_string()
}

/// The process and its shim are given nothing a canary or a composed
/// credential could be found in, and the shim is told its callback socket.
fn assert_launch_clean(class: &str, orch: &Orchestrator, args: &str, env: &str, composed: &[&str]) {
    launch_canary::assert_no_credential(class, env, composed);
    launch_canary::assert_no_credential(&format!("{class}'s argv"), args, composed);
    launch_canary::assert_names_socket(&format!("{class}'s cairn-cmd shim"), args, &socket(orch));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_claude_session_leader_and_its_shim_carry_no_callback_credential() {
    if !launch_canary::in_planted_child(
        module_path!(),
        "a_claude_session_leader_and_its_shim_carry_no_callback_credential",
    ) {
        return;
    }
    let launched = orchestrator().await;
    let bearer = launch(&launched.orch, "claude", false).await;
    let (args, env) = recorded(&launched.claude).await;
    assert!(args.contains("--mcp-config"), "{args}");
    assert_launch_clean(
        "a Claude process",
        &launched.orch,
        &args,
        &env,
        &[&bearer, CLAUDE_KEY],
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_codex_session_and_its_shim_carry_no_callback_credential() {
    if !launch_canary::in_planted_child(
        module_path!(),
        "a_codex_session_and_its_shim_carry_no_callback_credential",
    ) {
        return;
    }
    let launched = orchestrator().await;
    let bearer = launch(&launched.orch, "codex", false).await;
    let (args, env) = recorded(&launched.codex).await;
    assert!(args.contains("mcp_servers.cairn"), "{args}");
    assert_launch_clean(
        "a Codex app-server",
        &launched.orch,
        &args,
        &env,
        &[&bearer, CODEX_KEY],
    );
}

/// The pool's bearer is minted inside its start and never presented on Unix,
/// so it is searched for by name; the canaries and the account key by value.
#[tokio::test(flavor = "multi_thread")]
async fn the_pooled_codex_server_and_its_shim_carry_no_callback_credential() {
    if !launch_canary::in_planted_child(
        module_path!(),
        "the_pooled_codex_server_and_its_shim_carry_no_callback_credential",
    ) {
        return;
    }
    let launched = orchestrator().await;
    let _run_bearer = launch(&launched.orch, "codex", true).await;
    let (args, env) = recorded(&launched.codex).await;
    assert!(
        env.lines().any(|line| line
            == format!(
                "CAIRN_RUN_ID={}",
                crate::mcp::run_bearer::POOLED_CODEX_RUN_SENTINEL
            )),
        "the pooled server was the one launched:\n{env}"
    );
    assert_launch_clean(
        "the pooled Codex app-server",
        &launched.orch,
        &args,
        &env,
        &[CODEX_KEY],
    );
}
