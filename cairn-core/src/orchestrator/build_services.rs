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

/// Whether a service's health probe currently reports it reachable.
pub(crate) fn probe_ready(probe: &ReadyProbe) -> bool {
    if let Some(addr) = &probe.tcp {
        return tcp_reachable(addr);
    }
    if let Some(cmd) = &probe.command {
        if let Some((prog, args)) = cmd.split_first() {
            return crate::env::command(prog)
                .args(args)
                .output()
                .map(|o| o.status.success())
                .unwrap_or(false);
        }
    }
    false
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
    /// Whether the health probe currently reports the daemon reachable.
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

    /// Launch every enabled, installed build service that is not already
    /// reachable. Idempotent via the health probe: an already-running shared
    /// daemon is left in place (the cache is intentionally shared/persistent).
    /// Best-effort — a launch failure is logged, never fatal, because the client
    /// wrapper falls back to a plain compiler when the daemon is unreachable.
    pub fn start_build_services(&self) {
        if !sandbox::is_available() {
            // No service sandbox on this host; clients run uncached (the
            // cache-wrapper guard never auto-starts a confined server).
            return;
        }
        let templates = self.build_service_templates();
        let deny_read = self.sandbox_deny_read();
        for (name, cfg) in self.launchable_services() {
            if let Some(probe) = &cfg.ready {
                if probe_ready(probe) {
                    log::debug!("build service '{name}' already reachable; not relaunching");
                    continue;
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

    /// Relaunch any enabled service that has become unreachable. Same as
    /// `start_build_services` (the probe makes it a no-op for healthy services);
    /// named separately for call sites that run it on a timer or before builds.
    pub fn ensure_build_services_ready(&self) {
        self.start_build_services();
    }

    /// Best-effort stop of supervised daemons: kills the launcher handles held.
    /// A detached daemon (e.g. an sccache server) may outlive its launcher; that
    /// is acceptable for a shared cache and a deliberate non-goal to force-stop.
    pub fn stop_build_services(&self) {
        let mut children = self.build_service_children.lock().unwrap();
        for (name, child) in children.iter_mut() {
            if let Err(e) = child.kill() {
                log::debug!("stop build service '{name}': {e}");
            }
        }
        children.clear();
    }

    /// Runtime status of every configured (or default) build service, for the
    /// settings UI. Includes a live health probe, so call it on demand, not on a
    /// hot path.
    pub fn build_service_statuses(&self) -> Vec<BuildServiceStatus> {
        let templates = self.build_service_templates();
        let mut out: Vec<BuildServiceStatus> = settings::load_build_services(&self.config_dir)
            .into_iter()
            .map(|(name, cfg)| {
                let mut env_keys: Vec<String> = cfg.env.keys().cloned().collect();
                env_keys.sort();
                BuildServiceStatus {
                    name,
                    enabled: cfg.enabled,
                    installed: service_on_path(&cfg),
                    reachable: cfg.ready.as_ref().map(probe_ready).unwrap_or(false),
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
            Some("4226")
        );
        assert_eq!(
            env.get("SCCACHE_DIR").map(String::as_str),
            Some("/home/u/.cache/sccache")
        );
        // A disabled service contributes nothing, even unique keys.
        assert!(!env.contains_key("DISABLED_ONLY"));
    }

    #[test]
    fn spawn_config_confines_to_state_dir_and_globs_and_carries_env() {
        let cfg = default_sccache_service();
        let config = build_service_spawn_config(&cfg, &templates(), vec![]).unwrap();
        assert_eq!(config.program, "sccache");
        assert_eq!(config.args, vec!["--start-server"]);
        // The daemon's own env tells it where to listen/cache.
        assert_eq!(
            config.env.get("SCCACHE_DIR").map(String::as_str),
            Some("/home/u/.cache/sccache")
        );
        // Daemon pipes are not held open.
        assert!(!config.capture_stdout);
        assert!(!config.capture_stderr);
        // On a sandbox-capable host the service sandbox is applied with the
        // worktrees-target regex grant and the state dir writable.
        if sandbox::is_available() {
            let policy = config.sandbox.expect("service sandbox should be applied");
            assert!(policy
                .writable_paths()
                .contains(&PathBuf::from("/home/u/.cache/sccache")));
            assert_eq!(
                policy.writable_regex,
                vec!["^/home/u/\\.cairn/worktrees/.*/target/.*".to_string()]
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
                    && cfg.args == vec!["--start-server"]
                    && cfg.env.get("SCCACHE_SERVER_PORT").map(String::as_str) == Some("4226")
            })
            .returning(|_| Ok(Box::new(MockChildProcess::with_stdout(7, vec![]))));

        let child = launch_service(&spawner, &default_sccache_service(), &templates(), vec![])
            .expect("launch should succeed");
        assert_eq!(child.id(), 7);
    }

    #[test]
    fn tcp_probe_true_when_listening_false_when_closed() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let reachable = ReadyProbe::tcp(addr.to_string());
        assert!(probe_ready(&reachable), "a listening port must probe ready");

        // Drop the listener so the port closes, then probe it.
        drop(listener);
        // A port we just closed is almost certainly unbound now.
        let closed = ReadyProbe::tcp(addr.to_string());
        assert!(!probe_ready(&closed), "a closed port must not probe ready");
    }
}
