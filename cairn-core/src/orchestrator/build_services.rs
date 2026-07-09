//! Managed Build Services supervisor.
//!
//! Lifecycle for the Cairn-owned build-service daemons declared in settings
//! (see `config::build_services` and `docs/worktree-fence.md`): launch each
//! enabled service under its **service sandbox**, health-check it, relaunch a
//! dead/unreachable one, and expose the merged client env injected into fenced
//! agent spawns. sccache is the first configured instance.
//!
//! The core logic lives in free functions that take a `&dyn ProcessSpawner` and
//! pure config, so it is unit-testable without a full `Orchestrator`; the
//! `Orchestrator` methods are thin wrappers that read settings and hold the
//! launcher handles.

use std::collections::HashMap;
use std::net::{TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::config::build_services::{BuildServiceConfig, ReadyProbe, Templates};
use crate::config::settings;
use crate::services::sandbox::{self, SandboxPolicy};
use crate::services::{ChildProcess, ProcessSpawner, SpawnConfig};

use super::Orchestrator;

/// Timeout for a TCP reachability probe. Short — this can gate fenced builds.
const TCP_PROBE_TIMEOUT: Duration = Duration::from_millis(300);

/// Hard deadline for a health round-trip. A healthy sccache server answers
/// `--show-stats` well within this even under load; a wedged one never does, so
/// exceeding it means wedged. Kept comfortably under the supervisor tick so a
/// wedge is caught and recovered within one cycle.
const HEALTH_ROUND_TRIP_DEADLINE: Duration = Duration::from_secs(5);

/// Poll cadence while waiting for a spawned probe to exit or a killed daemon to
/// be reaped.
const HEALTH_POLL_INTERVAL: Duration = Duration::from_millis(25);

/// How long to wait for a killed daemon to actually exit (freeing its listening
/// port) before relaunching over it.
const CHILD_REAP_TIMEOUT: Duration = Duration::from_secs(2);

/// The rustc-wrapper / CMake compiler launcher, compiled into the binary from
/// its single source of truth `scripts/cache-wrapper.sh`. Installed to a stable
/// host path at startup (see `install_cache_wrapper`) so the `RUSTC_WRAPPER` the
/// default sccache service injects always resolves to one wrapper identity.
const CACHE_WRAPPER: &str = include_str!("../../../../../scripts/cache-wrapper.sh");

/// Install the embedded cache wrapper to `{cairn_home}/bin/cache-wrapper.sh`,
/// executable, overwriting any prior copy so upgrades propagate on every startup.
///
/// This is the stable path the default sccache service injects as `RUSTC_WRAPPER`.
/// Keeping it in one host location (rather than the repo-relative
/// `scripts/cache-wrapper.sh`) means every worktree's cargo shares one wrapper
/// identity, so cargo fingerprints never flip between a bare `cargo` in an agent
/// shell and the `bun run` scripts. The wrapper degrades safely with no sccache
/// on PATH (`exec "$@"`), so installing it is harmless even where the injected
/// env is never used. Best-effort at the call site: a failure is logged, never
/// fatal.
pub(crate) fn install_cache_wrapper(cairn_home: &Path) -> std::io::Result<PathBuf> {
    let bin_dir = cairn_home.join("bin");
    std::fs::create_dir_all(&bin_dir)?;
    let dest = bin_dir.join("cache-wrapper.sh");
    std::fs::write(&dest, CACHE_WRAPPER)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&dest, std::fs::Permissions::from_mode(0o755))?;
    }
    Ok(dest)
}

/// Build the spawn config for launching a service daemon under its service
/// sandbox. Pure (no spawning), so it can be asserted directly in tests.
///
/// Returns `None` if the service's `start` argv is empty. The daemon is confined
/// to its `state_dir` (or `cairn_home` as a harmless fallback) + temp + the
/// configured write globs, and receives the service's own `env` so it knows
/// where to listen / cache.
pub(crate) fn build_service_spawn_config(
    cfg: &BuildServiceConfig,
    templates: &Templates,
    deny_read: Vec<PathBuf>,
) -> Option<SpawnConfig> {
    let start = cfg.expanded_start(templates);
    let (program, args) = start.split_first()?;
    let write_globs = cfg.expanded_write(templates);
    let state_dir = cfg
        .expanded_state_dir(templates)
        .unwrap_or_else(|| templates.cairn_home.clone());

    let sandbox = if sandbox::is_available() {
        Some(SandboxPolicy::for_service(
            &state_dir,
            &write_globs,
            deny_read,
        ))
    } else {
        None
    };

    let mut config = SpawnConfig::new(program)
        .args(args.iter().cloned())
        .sandbox(sandbox);
    // A daemon manages its own lifetime; don't hold its stdio pipes open.
    config.capture_stdout = false;
    config.capture_stderr = false;
    for (k, v) in cfg.expanded_env(templates) {
        config = config.env(&k, &v);
    }
    // Daemon-only launch env (e.g. sccache's foreground-server switches and its
    // error-log diagnostics) is applied to the daemon spawn but is deliberately
    // absent from `merge_client_env`, so it never leaks into fenced client tooling.
    for (k, v) in cfg.expanded_launch_env(templates) {
        config = config.env(&k, &v);
    }
    Some(config)
}

/// Launch one service daemon via the spawner under its service sandbox.
pub(crate) fn launch_service(
    spawner: &dyn ProcessSpawner,
    cfg: &BuildServiceConfig,
    templates: &Templates,
    deny_read: Vec<PathBuf>,
) -> Result<Box<dyn ChildProcess>, String> {
    let config = build_service_spawn_config(cfg, templates, deny_read)
        .ok_or_else(|| "build service has an empty start command".to_string())?;
    spawner.spawn(config)
}

/// Whether the service's exit-0 `command` liveness probe succeeds. A cheap
/// reachability check with no deadline (the original `command`-probe semantics);
/// a non-zero exit or a spawn error reads as unreachable.
fn command_probe_ok(cmd: &[String]) -> bool {
    let Some((prog, args)) = cmd.split_first() else {
        return false;
    };
    crate::env::command(prog)
        .args(args)
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Health verdict for a supervised build-service daemon.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ServiceHealth {
    /// Reachable and answering a request/response round-trip within the deadline.
    Healthy,
    /// Listening on its port but not answering the round-trip within the deadline
    /// — a wedged-but-listening daemon (e.g. sccache stuck on its LRU cache mutex).
    /// Recovery must KILL it before relaunch: the port stays occupied and
    /// `sccache --stop-server` hangs against a wedged server.
    Wedged,
    /// Not listening — dead or never started. Recovery just (re)launches.
    Down,
}

/// Health-check a service, escalating from a cheap liveness probe to a deadlined
/// request/response round-trip.
///
/// A bare TCP connect (or an exit-0 `command`) can't tell a *wedged* daemon from
/// a healthy one: sccache's client-server protocol has no per-request timeout, so
/// a wedged-but-listening server accepts the connect and then blocks the client's
/// request read forever. So we run liveness first, then gate a real round-trip
/// behind it:
///
/// - **Liveness** mirrors the historical probe precedence — a TCP connect if
///   configured, otherwise the exit-0 `command` probe. A liveness failure is
///   `Down` (dead/unreachable): startup and the supervisor (re)launch it, and a
///   `command`-probed service keeps its original meaning rather than being
///   silently treated as healthy.
/// - **Wedge detection** is a deadlined request/response round-trip, reached only
///   when live (so it can never accidentally auto-start a server). A round-trip
///   that fails within the deadline is `Wedged`. The deadline is enforced here in
///   the (unfenced) runner process, never via a shell `timeout` (absent on macOS,
///   and it would run outside the fence anyway).
pub(crate) fn probe_health(
    spawner: &dyn ProcessSpawner,
    probe: &ReadyProbe,
    env: &HashMap<String, String>,
    deadline: Duration,
) -> ServiceHealth {
    let live = match (&probe.tcp, &probe.command) {
        (Some(addr), _) => tcp_reachable(addr),
        (None, Some(cmd)) => command_probe_ok(cmd),
        (None, None) => true, // no liveness probe configured
    };
    if !live {
        return ServiceHealth::Down;
    }
    if let Some(cmd) = &probe.round_trip {
        return if round_trip_healthy(spawner, cmd, env, deadline) {
            ServiceHealth::Healthy
        } else {
            ServiceHealth::Wedged
        };
    }
    // Live, and no round-trip configured: liveness is all we can assert.
    ServiceHealth::Healthy
}

/// Run a health round-trip command with a HARD, Rust-enforced deadline: spawn it
/// unconfined (the probe runs in the runner, not a fenced agent), poll for exit,
/// and if it exceeds the deadline, kill it and report unhealthy. Returns true
/// only on a clean, in-time exit. The service's client env is passed so the probe
/// talks to the right daemon; the daemon-only launch env is excluded (e.g.
/// `SCCACHE_START_SERVER` would make `sccache --show-stats` refuse to run).
fn round_trip_healthy(
    spawner: &dyn ProcessSpawner,
    cmd: &[String],
    env: &HashMap<String, String>,
    deadline: Duration,
) -> bool {
    let Some((program, args)) = cmd.split_first() else {
        return true;
    };
    let mut config = SpawnConfig::new(program).args(args.iter().cloned());
    config.capture_stdout = false;
    config.capture_stderr = false;
    for (k, v) in env {
        config = config.env(k, v);
    }
    let mut child = match spawner.spawn(config) {
        Ok(child) => child,
        Err(e) => {
            log::debug!("health round-trip spawn failed: {e}");
            return false;
        }
    };
    let start = std::time::Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return status.success(),
            Ok(None) => {
                if start.elapsed() >= deadline {
                    // Wedged: the request read is blocked. Kill the probe (it would
                    // otherwise hang forever) and report unhealthy.
                    let _ = child.kill();
                    return false;
                }
                std::thread::sleep(HEALTH_POLL_INTERVAL);
            }
            Err(e) => {
                log::debug!("health round-trip wait failed: {e}");
                return false;
            }
        }
    }
}

/// Wait briefly for a killed child to exit so the OS releases its resources (its
/// listening port) before we relaunch over it or return. Bounded by
/// `CHILD_REAP_TIMEOUT` so a child that ignores the signal can't hang the caller.
fn reap_child_briefly(child: &mut dyn ChildProcess) {
    let start = std::time::Instant::now();
    while start.elapsed() < CHILD_REAP_TIMEOUT {
        match child.try_wait() {
            Ok(Some(_)) | Err(_) => break,
            Ok(None) => std::thread::sleep(HEALTH_POLL_INTERVAL),
        }
    }
}

fn tcp_reachable(addr: &str) -> bool {
    match addr.to_socket_addrs() {
        Ok(mut addrs) => addrs
            .next()
            .map(|a| TcpStream::connect_timeout(&a, TCP_PROBE_TIMEOUT).is_ok())
            .unwrap_or(false),
        Err(_) => false,
    }
}

/// Merge the expanded client env of every enabled service. Injected into fenced
/// agent spawns so their tooling connects to the Cairn-owned daemons.
pub(crate) fn merge_client_env(
    services: &HashMap<String, BuildServiceConfig>,
    templates: &Templates,
) -> HashMap<String, String> {
    let mut env = HashMap::new();
    for cfg in services.values().filter(|c| c.enabled) {
        for (k, v) in cfg.expanded_env(templates) {
            env.insert(k, v);
        }
    }
    env
}

/// Runtime status of one build service, for the settings UI.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BuildServiceStatus {
    pub name: String,
    /// Whether the service is enabled in settings.
    pub enabled: bool,
    /// Whether the launch program resolves on PATH (or is an absolute path).
    pub installed: bool,
    /// Whether the full health probe currently reports the daemon healthy (live
    /// and answering a round-trip) — what the supervisor's recovery path sees, so
    /// a wedged-but-listening daemon reads as not reachable.
    pub reachable: bool,
    /// The launch argv, templates expanded (for display).
    pub start: Vec<String>,
    /// The cross-worktree writable globs, templates expanded (the grant).
    pub write: Vec<String>,
    /// The daemon's state dir, templates expanded.
    pub state_dir: Option<String>,
    /// Sorted client-env keys this service injects (values omitted).
    pub env_keys: Vec<String>,
    /// The raw, template-unexpanded config — the editable source of truth the
    /// settings UI binds its form to (so edits round-trip `{worktrees}` etc.).
    pub config: BuildServiceConfig,
}

/// Whether the service's launch program resolves (on PATH or an absolute path).
/// The built-in default sccache entry uses this to stay inert unless `sccache`
/// is actually installed.
fn service_on_path(cfg: &BuildServiceConfig) -> bool {
    match cfg.start.first() {
        Some(prog) => Path::new(prog).is_absolute() || crate::env::find_binary(prog).is_ok(),
        None => false,
    }
}

impl Orchestrator {
    fn build_service_templates(&self) -> Templates {
        settings::build_service_templates(&self.config_dir, None)
    }

    /// Enabled services whose launch program is installed.
    fn launchable_services(&self) -> Vec<(String, BuildServiceConfig)> {
        settings::load_build_services(&self.config_dir)
            .into_iter()
            .filter(|(_, c)| c.enabled && service_on_path(c))
            .collect()
    }

    /// Startup entry point: install the embedded rustc wrapper, then bring every
    /// enabled, installed build service to a healthy state via
    /// [`Self::ensure_build_services_ready`]. Best-effort throughout — failures
    /// log and are never fatal, because the client wrapper falls back to a plain
    /// compiler when the daemon is unreachable.
    pub fn start_build_services(&self) {
        // Install the embedded rustc wrapper to `{cairnHome}/bin` first, before
        // any early return, so the `RUSTC_WRAPPER` the default sccache service
        // injects always resolves — even on a host without a service sandbox,
        // where clients run uncached but the wrapper must still exist to exec the
        // compiler. Overwrite each startup so upgrades propagate.
        if let Err(e) = install_cache_wrapper(&self.config_dir) {
            log::warn!("failed to install cache wrapper: {e}");
        }
        self.ensure_build_services_ready();
    }

    /// Bring every enabled, installed build service to a healthy state: launch a
    /// down one, and kill-then-relaunch a wedged one. Idempotent — a healthy
    /// daemon is left in place (the cache is intentionally shared/persistent).
    ///
    /// Health is a deadlined request/response round-trip ([`probe_health`]), not
    /// just a reachability check, so this recovers a wedged-but-listening daemon
    /// that a bare TCP probe would miss. Best-effort: every failure logs and is
    /// never fatal, because the client wrapper and `SCCACHE_IGNORE_SERVER_IO_ERROR`
    /// fall back to uncached compiles when the daemon is unreachable. Runs the
    /// health round-trip as a subprocess, so call it off the async runtime
    /// (`spawn_blocking` / a dedicated thread), never on a hot path.
    pub fn ensure_build_services_ready(&self) {
        if !sandbox::is_available() {
            // No service sandbox on this host; clients run uncached (the
            // cache-wrapper guard never auto-starts a confined server).
            return;
        }
        let templates = self.build_service_templates();
        let deny_read = self.sandbox_deny_read();
        for (name, cfg) in self.launchable_services() {
            let client_env = cfg.expanded_env(&templates);
            let health = match &cfg.ready {
                Some(probe) => probe_health(
                    self.services.process.as_ref(),
                    probe,
                    &client_env,
                    HEALTH_ROUND_TRIP_DEADLINE,
                ),
                // No probe to assess health: treat a service we already supervise
                // as fine, and one we don't as needing a launch.
                None => {
                    if self
                        .build_service_children
                        .lock()
                        .unwrap()
                        .contains_key(&name)
                    {
                        ServiceHealth::Healthy
                    } else {
                        ServiceHealth::Down
                    }
                }
            };
            match health {
                ServiceHealth::Healthy => {
                    log::debug!("build service '{name}' healthy; not relaunching");
                    continue;
                }
                ServiceHealth::Wedged => {
                    // Kill the wedged daemon before relaunch: its port stays
                    // occupied and `sccache --stop-server` hangs against it, so we
                    // kill the supervised child handle directly.
                    log::warn!("build service '{name}' wedged; killing and relaunching");
                    self.kill_build_service_child(&name);
                }
                ServiceHealth::Down => {
                    log::info!("build service '{name}' down; launching");
                }
            }
            // Ensure the daemon's state dir exists before launch: sccache creates
            // its SCCACHE_ERROR_LOG file (under stateDir) before starting the
            // server, and a missing parent dir would fail that and take the whole
            // server down on a fresh machine.
            if let Some(dir) = cfg.expanded_state_dir(&templates) {
                if let Err(e) = std::fs::create_dir_all(&dir) {
                    log::debug!("create build service state dir {dir:?}: {e}");
                }
            }
            match launch_service(
                self.services.process.as_ref(),
                &cfg,
                &templates,
                deny_read.clone(),
            ) {
                Ok(child) => {
                    log::info!("started build service '{name}'");
                    self.build_service_children
                        .lock()
                        .unwrap()
                        .insert(name, child);
                }
                Err(e) => log::warn!("failed to start build service '{name}': {e}"),
            }
        }
    }

    /// Kill a supervised build-service daemon by its held child handle, then wait
    /// briefly for it to exit so the OS releases the listening port before a
    /// relaunch binds it. The default sccache daemon runs foreground in Cairn's
    /// process group, so the handle's SIGKILL reaps the server itself. No-op if no
    /// handle is held (e.g. a daemon orphaned by a prior process crash); the
    /// relaunch then races the stale listener, and the client failover keeps
    /// builds correct meanwhile.
    fn kill_build_service_child(&self, name: &str) {
        let child = self.build_service_children.lock().unwrap().remove(name);
        let Some(mut child) = child else {
            return;
        };
        if let Err(e) = child.kill() {
            log::debug!("kill build service '{name}': {e}");
        }
        reap_child_briefly(&mut *child);
    }

    /// Spawn the build-service supervisor: on a periodic tick, health-check every
    /// enabled service and recover any that has died or wedged (kill-then-relaunch)
    /// without a runner restart. Backstops the startup launch so a daemon that
    /// dies or wedges mid-session is restored within one interval (~1 min). Each
    /// tick runs the health round-trip as a subprocess, so it runs on a blocking
    /// thread. Owned by the always-on hosts (runner, non-inert server); must run
    /// within a tokio runtime.
    pub fn spawn_build_service_supervisor(&self) {
        /// Cadence of the health/recovery tick. Short enough to meet the
        /// ~1-minute recovery bar, cheap enough to run continuously (a healthy
        /// daemon costs one TCP connect plus one `--show-stats` round-trip).
        const TICK_INTERVAL: Duration = Duration::from_secs(60);
        let orch = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(TICK_INTERVAL).await;
                let orch = orch.clone();
                if let Err(e) =
                    tokio::task::spawn_blocking(move || orch.ensure_build_services_ready()).await
                {
                    log::warn!("build service supervisor tick failed: {e}");
                }
            }
        });
    }

    /// Best-effort stop of supervised daemons: kills the launcher handles held.
    /// The default sccache daemon runs foreground in Cairn's process group, so
    /// this SIGKILLs the server itself; a service configured to detach may still
    /// outlive its launcher, which is acceptable for a shared cache.
    pub fn stop_build_services(&self) {
        // Drain the handles, then kill and briefly reap each so a killed daemon has
        // actually exited (and released its listening port) before we return — so
        // a `restart` (stop then start) re-probes a truly-down port rather than a
        // dying-but-still-listening one and misreading its health.
        let children: Vec<(String, Box<dyn ChildProcess>)> = self
            .build_service_children
            .lock()
            .unwrap()
            .drain()
            .collect();
        for (name, mut child) in children {
            if let Err(e) = child.kill() {
                log::debug!("stop build service '{name}': {e}");
            }
            reap_child_briefly(&mut *child);
        }
    }

    /// Runtime status of every configured (or default) build service, for the
    /// settings UI. `reachable` reflects the full health probe ([`probe_health`]),
    /// so the UI agrees with the supervisor's recovery path: a wedged-but-listening
    /// daemon reads as not reachable rather than falsely OK. The health probe runs
    /// a subprocess round-trip when the daemon is live, so call this on demand,
    /// never on a hot path.
    pub fn build_service_statuses(&self) -> Vec<BuildServiceStatus> {
        let templates = self.build_service_templates();
        let mut out: Vec<BuildServiceStatus> = settings::load_build_services(&self.config_dir)
            .into_iter()
            .map(|(name, cfg)| {
                let mut env_keys: Vec<String> = cfg.env.keys().cloned().collect();
                env_keys.sort();
                let reachable = cfg
                    .ready
                    .as_ref()
                    .map(|probe| {
                        probe_health(
                            self.services.process.as_ref(),
                            probe,
                            &cfg.expanded_env(&templates),
                            HEALTH_ROUND_TRIP_DEADLINE,
                        ) == ServiceHealth::Healthy
                    })
                    .unwrap_or(false);
                BuildServiceStatus {
                    name,
                    enabled: cfg.enabled,
                    installed: service_on_path(&cfg),
                    reachable,
                    start: cfg.expanded_start(&templates),
                    write: cfg.expanded_write(&templates),
                    state_dir: cfg
                        .expanded_state_dir(&templates)
                        .map(|p| p.to_string_lossy().to_string()),
                    env_keys,
                    config: cfg,
                }
            })
            .collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    /// The merged client env for enabled build services, expanded for the given
    /// per-spawn `worktree`. Injected into fenced agent spawns.
    pub fn build_service_client_env(&self, worktree: Option<&Path>) -> HashMap<String, String> {
        let templates =
            settings::build_service_templates(&self.config_dir, worktree.map(Path::to_path_buf));
        merge_client_env(&settings::load_build_services(&self.config_dir), &templates)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::build_services::default_sccache_service;
    use crate::services::testing::{MockChildProcess, MockProcessSpawner};

    fn templates() -> Templates {
        Templates {
            home: PathBuf::from("/home/u"),
            cairn_home: PathBuf::from("/home/u/.cairn"),
            worktrees: PathBuf::from("/home/u/.cairn/worktrees"),
            worktree: None,
        }
    }

    #[test]
    fn merge_client_env_includes_enabled_excludes_disabled() {
        let mut services = HashMap::new();
        services.insert("sccache".to_string(), default_sccache_service());
        let mut disabled = default_sccache_service();
        disabled.enabled = false;
        disabled
            .env
            .insert("DISABLED_ONLY".to_string(), "1".to_string());
        services.insert("other".to_string(), disabled);

        let env = merge_client_env(&services, &templates());
        assert_eq!(
            env.get("SCCACHE_SERVER_PORT").map(String::as_str),
            Some("4227")
        );
        assert_eq!(
            env.get("SCCACHE_DIR").map(String::as_str),
            Some("/home/u/.cache/sccache-cairn")
        );
        // A disabled service contributes nothing, even unique keys.
        assert!(!env.contains_key("DISABLED_ONLY"));
    }

    #[test]
    fn spawn_config_confines_to_state_dir_and_globs_and_carries_env() {
        let cfg = default_sccache_service();
        let config = build_service_spawn_config(&cfg, &templates(), vec![]).unwrap();
        assert_eq!(config.program, "sccache");
        // Bare `sccache`: the foreground server is selected via SCCACHE_START_SERVER
        // (launch env below), not a `--start-server` arg.
        assert!(config.args.is_empty());
        // The daemon's own env tells it where to listen/cache.
        assert_eq!(
            config.env.get("SCCACHE_DIR").map(String::as_str),
            Some("/home/u/.cache/sccache-cairn")
        );
        // Daemon-only launch env is applied to the daemon spawn so it runs the
        // in-process foreground server (killable via its supervised handle).
        assert_eq!(
            config.env.get("SCCACHE_START_SERVER").map(String::as_str),
            Some("1")
        );
        assert_eq!(
            config.env.get("SCCACHE_NO_DAEMON").map(String::as_str),
            Some("1")
        );
        assert_eq!(
            config.env.get("SCCACHE_ERROR_LOG").map(String::as_str),
            Some("/home/u/.cache/sccache-cairn/sccache-error.log")
        );
        // Daemon pipes are not held open.
        assert!(!config.capture_stdout);
        assert!(!config.capture_stderr);
        // On a sandbox-capable host the service sandbox is applied with the state
        // dir writable and one regex grant per configured `write` glob: the
        // worktrees target tree plus the two check-isolation COW-clone roots (so a
        // cache-miss compile the confined daemon runs can write into a clone's
        // target/ instead of EPERMing).
        if sandbox::is_available() {
            let policy = config.sandbox.expect("service sandbox should be applied");
            assert!(policy
                .writable_paths()
                .contains(&PathBuf::from("/home/u/.cache/sccache-cairn")));
            assert_eq!(
                policy.writable_regex,
                vec![
                    "^/home/u/\\.cairn/worktrees/.*/target/.*".to_string(),
                    "^/home/u/\\.cairn/check-clones/.*/target/.*".to_string(),
                    "^/home/u/\\.cairn/turn-check-clones/.*/target/.*".to_string(),
                ]
            );
        }
    }

    #[test]
    fn launch_service_spawns_expected_command() {
        let mut spawner = MockProcessSpawner::new();
        spawner
            .expect_spawn()
            .withf(|cfg| {
                cfg.program == "sccache"
                    && cfg.args.is_empty()
                    && cfg.env.get("SCCACHE_START_SERVER").map(String::as_str) == Some("1")
                    && cfg.env.get("SCCACHE_SERVER_PORT").map(String::as_str) == Some("4227")
            })
            .returning(|_| Ok(Box::new(MockChildProcess::with_stdout(7, vec![]))));

        let child = launch_service(&spawner, &default_sccache_service(), &templates(), vec![])
            .expect("launch should succeed");
        assert_eq!(child.id(), 7);
    }

    // Serialized under one key: these tests bind an ephemeral port and then
    // assert on a *just-dropped* one. Run concurrently, one test's OS-reused
    // ephemeral port can re-bind the port another just closed, so the "closed"
    // assertion flakes. Serializing them removes the intra-module race.
    #[test]
    #[serial_test::serial(build_service_port)]
    fn probe_health_tcp_liveness_healthy_when_listening_down_when_closed() {
        // A tcp-only probe (no round_trip): a listening port is Healthy, a closed
        // one is Down. The spawner is never called (no round-trip to run).
        let spawner = MockProcessSpawner::new();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        assert_eq!(
            probe_health(
                &spawner,
                &ReadyProbe::tcp(addr.clone()),
                &HashMap::new(),
                Duration::from_secs(1),
            ),
            ServiceHealth::Healthy,
            "a listening port must probe healthy"
        );

        drop(listener);
        assert_eq!(
            probe_health(
                &spawner,
                &ReadyProbe::tcp(addr),
                &HashMap::new(),
                Duration::from_secs(1),
            ),
            ServiceHealth::Down,
            "a closed port must probe down"
        );
    }

    #[test]
    fn probe_health_command_probe_down_on_failure_healthy_on_success() {
        // A command-only probe (no tcp/round_trip) keeps its exit-0 liveness
        // semantics: a failing command is Down (so startup/supervisor relaunch
        // the service), a succeeding one is Healthy — it is NOT silently treated
        // as healthy just because there is no tcp/round_trip.
        let spawner = MockProcessSpawner::new();
        let failing = ReadyProbe {
            tcp: None,
            command: Some(vec!["false".to_string()]),
            round_trip: None,
        };
        assert_eq!(
            probe_health(&spawner, &failing, &HashMap::new(), Duration::from_secs(1)),
            ServiceHealth::Down
        );
        let ok = ReadyProbe {
            tcp: None,
            command: Some(vec!["true".to_string()]),
            round_trip: None,
        };
        assert_eq!(
            probe_health(&spawner, &ok, &HashMap::new(), Duration::from_secs(1)),
            ServiceHealth::Healthy
        );
    }

    fn round_trip_cmd() -> Vec<String> {
        vec!["sccache".to_string(), "--show-stats".to_string()]
    }

    #[test]
    fn round_trip_healthy_true_on_clean_in_time_exit() {
        // A probe that exits 0 within the deadline is a healthy round-trip.
        let mut spawner = MockProcessSpawner::new();
        spawner
            .expect_spawn()
            .returning(|_| Ok(Box::new(MockChildProcess::failing(1, "", 0))));
        assert!(round_trip_healthy(
            &spawner,
            &round_trip_cmd(),
            &HashMap::new(),
            Duration::from_secs(1),
        ));
    }

    #[test]
    fn round_trip_healthy_false_on_nonzero_exit() {
        let mut spawner = MockProcessSpawner::new();
        spawner
            .expect_spawn()
            .returning(|_| Ok(Box::new(MockChildProcess::failing(1, "boom", 1))));
        assert!(!round_trip_healthy(
            &spawner,
            &round_trip_cmd(),
            &HashMap::new(),
            Duration::from_secs(1),
        ));
    }

    #[test]
    fn round_trip_healthy_false_when_deadline_exceeded() {
        // A probe process that never exits (a wedged server blocks the request
        // read) is killed at the Rust-enforced deadline and reported unhealthy —
        // no reliance on a shell `timeout`.
        let mut spawner = MockProcessSpawner::new();
        spawner
            .expect_spawn()
            .returning(|_| Ok(Box::new(MockChildProcess::with_stdout(1, vec![]))));
        assert!(!round_trip_healthy(
            &spawner,
            &round_trip_cmd(),
            &HashMap::new(),
            Duration::from_millis(40),
        ));
    }

    #[test]
    #[serial_test::serial(build_service_port)]
    fn probe_health_healthy_when_listening_and_round_trip_clean() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let probe = ReadyProbe {
            tcp: Some(addr),
            command: None,
            round_trip: Some(round_trip_cmd()),
        };
        let mut spawner = MockProcessSpawner::new();
        spawner
            .expect_spawn()
            .returning(|_| Ok(Box::new(MockChildProcess::failing(1, "", 0))));
        assert_eq!(
            probe_health(&spawner, &probe, &HashMap::new(), Duration::from_secs(1)),
            ServiceHealth::Healthy
        );
    }

    #[test]
    #[serial_test::serial(build_service_port)]
    fn probe_health_wedged_when_listening_but_round_trip_hangs() {
        // The daemon accepts the TCP connect (listening) but never answers the
        // round-trip — the wedged-but-listening case a bare TCP probe misses.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let probe = ReadyProbe {
            tcp: Some(addr),
            command: None,
            round_trip: Some(round_trip_cmd()),
        };
        let mut spawner = MockProcessSpawner::new();
        spawner
            .expect_spawn()
            .returning(|_| Ok(Box::new(MockChildProcess::with_stdout(1, vec![]))));
        assert_eq!(
            probe_health(&spawner, &probe, &HashMap::new(), Duration::from_millis(40)),
            ServiceHealth::Wedged
        );
    }

    #[test]
    #[serial_test::serial(build_service_port)]
    fn probe_health_down_when_port_closed_skips_round_trip() {
        // A closed port short-circuits to Down without spawning the round-trip —
        // gating the round-trip behind liveness is what prevents it from ever
        // auto-starting a server. The mock spawner has no expectations, so a spawn
        // would panic.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        drop(listener);
        let probe = ReadyProbe {
            tcp: Some(addr),
            command: None,
            round_trip: Some(round_trip_cmd()),
        };
        let spawner = MockProcessSpawner::new();
        assert_eq!(
            probe_health(&spawner, &probe, &HashMap::new(), Duration::from_secs(1)),
            ServiceHealth::Down
        );
    }

    #[cfg(unix)]
    #[test]
    fn install_cache_wrapper_writes_executable_wrapper() {
        use std::os::unix::fs::PermissionsExt;
        let temp = tempfile::tempdir().unwrap();
        let dest = install_cache_wrapper(temp.path()).unwrap();
        assert_eq!(dest, temp.path().join("bin").join("cache-wrapper.sh"));

        let meta = std::fs::metadata(&dest).unwrap();
        assert!(
            meta.permissions().mode() & 0o111 != 0,
            "installed wrapper must be executable"
        );
        // The embedded body is the real script (has its sccache guard), and a
        // second install overwrites cleanly so upgrades propagate.
        let body = std::fs::read_to_string(&dest).unwrap();
        assert!(body.contains("command -v sccache"));
        assert_eq!(install_cache_wrapper(temp.path()).unwrap(), dest);
    }
}
