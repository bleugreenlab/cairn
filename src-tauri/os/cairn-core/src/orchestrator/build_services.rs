//! Managed Build Services supervisor.
//!
//! Lifecycle for the Cairn-owned build-service daemons declared in settings
//! (see `config::build_services` and `docs/worktree-fence.md`): launch each
//! enabled machine-wide service without a worktree fence, health-check it, relaunch a
//! dead/unreachable one, and expose the merged client env injected into spawns
//! that build inside a managed build root. sccache is the first configured
//! instance.
//!
//! The core logic lives in free functions that take a `&dyn ProcessSpawner` and
//! pure config, so it is unit-testable without a full `Orchestrator`; the
//! `Orchestrator` methods are thin wrappers that read settings and hold the
//! launcher handles.

use std::collections::{BTreeMap, HashMap};
use std::io::Read;
use std::net::{TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::time::Duration;

use cairn_common::executor_protocol::{
    CompileCacheHealth, CompileCacheState, CompileCacheStats, Measurement, MeasurementGap,
};
use sha2::{Digest, Sha256};

use crate::config::build_services::{
    cell_build_root, managed_build_roots, managed_root_containing, BuildServiceConfig,
    LaunchFingerprint, ReadyProbe, Templates,
};
use crate::config::settings;
use crate::services::sandbox::{self, SandboxPolicy};
use crate::services::{ChildProcess, ProcessSpawner, SpawnConfig};

use super::Orchestrator;

const BUILD_SERVICE_UNFIT_ENV: &str = "CAIRN_BUILD_SERVICE_UNFIT";

/// A compile-cache service's claim on its own port: clients attach to the
/// supervised daemon and never start one. Declared by the service's own env.
const BUILD_SERVICE_CLIENT_ENV: &str = "CAIRN_BUILD_SERVICE_CLIENT";

/// The cache wrapper this runner ships, delivered to `{cairnHome}/bin` so the
/// output-directory gate travels with the runner that relies on it.
const BUNDLED_CACHE_WRAPPER: &str = include_str!("../../../../../scripts/cache-wrapper.sh");

/// The line a wrapper carries when it enforces the output-directory gate
/// (`scripts/cache-wrapper.sh`). A wrapper without it ignores
/// [`BUILD_SERVICE_ROOTS_ENV`] and would send every compile to the daemon,
/// including ones whose output its grant does not cover.
const WRAPPER_OUT_DIR_GATE: &str = "cairn-cache-wrapper-capability: out-dir-gate";

const WRAPPER_ENV_KEYS: [&str; 2] = ["RUSTC_WRAPPER", "CARGO_BUILD_RUSTC_WRAPPER"];

fn installed_wrapper_path(templates: &Templates) -> PathBuf {
    templates.cairn_home.join("bin").join("cache-wrapper.sh")
}

fn wrapper_has_gate(path: &Path) -> bool {
    std::fs::read_to_string(path).is_ok_and(|content| content.contains(WRAPPER_OUT_DIR_GATE))
}

/// Deliver this runner's wrapper to `path` when the copy there is missing or
/// predates the gate. A gated copy is left alone: `scripts/rust-cache-env.ts`
/// keeps it in step with the checkout, and the runner must not undo a newer
/// one. Returns whether it wrote.
fn install_cache_wrapper(path: &Path) -> Result<bool, String> {
    if wrapper_has_gate(path) {
        return Ok(false);
    }
    let parent = path
        .parent()
        .ok_or_else(|| format!("{} has no parent", path.display()))?;
    std::fs::create_dir_all(parent).map_err(|e| format!("create {}: {e}", parent.display()))?;
    let staging = path.with_extension(format!("{}.staging", std::process::id()));
    std::fs::write(&staging, BUNDLED_CACHE_WRAPPER)
        .map_err(|e| format!("write {}: {e}", staging.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&staging, std::fs::Permissions::from_mode(0o755))
            .map_err(|e| format!("chmod {}: {e}", staging.display()))?;
    }
    std::fs::rename(&staging, path).map_err(|e| {
        let _ = std::fs::remove_file(&staging);
        format!("install {}: {e}", path.display())
    })?;
    Ok(true)
}

/// Whether every wrapper `env` names enforces the output-directory gate. Only
/// a compile-cache service that claims its port is asked; with no wrapper at
/// all, nothing would enforce the gate either.
fn wrapper_enforces_gate(env: &HashMap<String, String>) -> bool {
    if !env.contains_key(BUILD_SERVICE_CLIENT_ENV) {
        return true;
    }
    let wrappers: Vec<&String> = WRAPPER_ENV_KEYS
        .iter()
        .filter_map(|key| env.get(*key))
        .filter(|value| !value.is_empty())
        .collect();
    !wrappers.is_empty()
        && wrappers
            .iter()
            .all(|wrapper| wrapper_has_gate(Path::new(wrapper.as_str())))
}

/// Routing as the build's wrapper can honour it. A wrapper that predates the
/// gate cannot keep an ungranted output away from the daemon, so it is routed
/// nothing: `CAIRN_BUILD_SERVICE_UNFIT`, which every wrapper honours.
fn routing_the_wrapper_can_enforce(routing: Routing, env: &HashMap<String, String>) -> Routing {
    if wrapper_enforces_gate(env) {
        routing
    } else {
        Routing {
            daemon_unfit: true,
            roots: Vec::new(),
        }
    }
}

/// The managed roots the daemon may serve right now, canonical and `:`-joined:
/// granted, proven by this incarnation, and not since found unfit.
/// `scripts/cache-wrapper.sh` attaches only for a compile whose output
/// directory lies in a `target/` tree under one of them, and compiles directly
/// otherwise, so a build that writes anywhere else never reaches the daemon.
const BUILD_SERVICE_ROOTS_ENV: &str = "CAIRN_BUILD_SERVICE_ROOTS";

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

/// Bound startup reconciliation after a daemon launch. The macOS launchctl
/// request exits as soon as launchd accepts the job, before cache initialization
/// and socket binding necessarily finish, so readiness gets its own real window.
const STARTUP_RECONCILE_TIMEOUT: Duration = Duration::from_secs(15);

/// Floor and ceiling of the relaunch backoff.
///
/// A compile cache is worth retrying promptly, but never indefinitely without
/// producing health: each failed sccache launch appends to the daemon's own error
/// log, which is how one such log reached 5.4 MB of a single repeated line
/// (CAIRN-3332).
const RESTART_BACKOFF_MIN: Duration = Duration::from_secs(5);
const RESTART_BACKOFF_MAX: Duration = Duration::from_secs(300);

/// Consecutive failed launches past which the condition stops being an ordinary
/// retry and becomes a named, operator-visible `recoveryFailed`. Automatic launch
/// attempts stop at this boundary; passive health observation continues, so an
/// operator-started daemon can recover the service.
const RECOVERY_FAILED_AFTER: u32 = 5;

/// How many recovery attempts may DESTROY a daemon before the supervisor stops
/// trying to and just reports.
///
/// Killing a shared compile cache is irreversible for everything currently
/// compiling against it, so the authority to do it has to be earned. Two
/// separate facts earn it, and the reason is the defect this bounds: for three
/// weeks a healthy daemon was declared wedged on every tick, and the ONLY thing
/// standing between that false verdict and a daemon killed every 15 seconds was
/// an executable-identity comparison that happened to be broken (CAIRN-3332).
/// A recovery policy whose safety depends on another bug is not a policy.
///
/// So: a probe that has never once answered in this process is an untested
/// instrument and authorizes exactly ONE destructive attempt, not an endless
/// series. And once recovery has been attempted this many times without
/// producing health, the likelier explanation is that the probe is wrong rather
/// than that every daemon wedges on arrival — so the service goes to
/// `recoveryFailed`, keeps being observed and reported, and is left alone. One
/// healthy round trip resets all of it.
const DESTRUCTIVE_ATTEMPT_LIMIT: u32 = 3;

/// How much retained diagnostic text a probe or launch may contribute.
///
/// Bounded at the seam rather than at the display, so unbounded daemon output
/// can never become unbounded retained state or reach a UI surface.
const DIAGNOSTIC_BYTES: usize = 600;
const CACHE_QUARANTINE_FILE_LIMIT: usize = 100_000;

/// How much of a probe's stdout is read. The sccache report is roughly 1.5 KiB;
/// this leaves generous headroom while keeping the read bounded.
const PROBE_OUTPUT_BYTES: u64 = 16_384;

fn reset_recovery_for_changed_config(
    state: &mut BuildServiceRuntimeDiagnostic,
    cfg: &BuildServiceConfig,
    templates: &Templates,
    now_ms: u64,
) -> bool {
    let current = service_config_fingerprint(cfg);
    if !recovery_exhausted(state.consecutive_failures)
        || state
            .failure_config
            .as_deref()
            .is_none_or(|failed| failed == current)
    {
        return false;
    }
    state.consecutive_failures = 0;
    state.next_attempt_unix_ms = None;
    state.current_failure = None;
    state.failure_config = None;
    state.enter("degraded", now_ms);
    clear_recovery_failure(cfg, templates);
    true
}

const RECOVERY_FAILURE_FILE: &str = ".cairn-recovery-failed.json";

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct PersistedRecoveryFailure {
    config_fingerprint: String,
    consecutive_failures: u32,
    diagnosis: String,
}

fn recovery_failure_path(cfg: &BuildServiceConfig, templates: &Templates) -> Option<PathBuf> {
    cfg.expanded_state_dir(templates)
        .map(|dir| dir.join(RECOVERY_FAILURE_FILE))
}

fn read_recovery_failure(
    cfg: &BuildServiceConfig,
    templates: &Templates,
) -> Option<PersistedRecoveryFailure> {
    let path = recovery_failure_path(cfg, templates)?;
    let bytes = std::fs::read(path).ok()?;
    let persisted: PersistedRecoveryFailure = serde_json::from_slice(&bytes).ok()?;
    (persisted.config_fingerprint == service_config_fingerprint(cfg)).then_some(persisted)
}

fn write_recovery_failure(
    cfg: &BuildServiceConfig,
    templates: &Templates,
    consecutive_failures: u32,
    diagnosis: &str,
) -> Result<(), String> {
    let Some(path) = recovery_failure_path(cfg, templates) else {
        return Ok(());
    };
    let parent = path
        .parent()
        .ok_or_else(|| "recovery failure path has no parent".to_string())?;
    std::fs::create_dir_all(parent).map_err(|e| format!("create recovery state dir: {e}"))?;
    let record = PersistedRecoveryFailure {
        config_fingerprint: service_config_fingerprint(cfg),
        consecutive_failures,
        diagnosis: bounded_tail(diagnosis),
    };
    let bytes = serde_json::to_vec(&record).map_err(|e| format!("encode recovery state: {e}"))?;
    let temporary = path.with_extension("json.tmp");
    std::fs::write(&temporary, bytes).map_err(|e| format!("write recovery state: {e}"))?;
    std::fs::rename(&temporary, &path).map_err(|e| format!("commit recovery state: {e}"))
}

fn clear_recovery_failure(cfg: &BuildServiceConfig, templates: &Templates) {
    if let Some(path) = recovery_failure_path(cfg, templates) {
        if let Err(error) = std::fs::remove_file(path) {
            if error.kind() != std::io::ErrorKind::NotFound {
                log::warn!("clear build service recovery state: {error}");
            }
        }
    }
}

fn may_launch_service(
    cfg: &BuildServiceConfig,
    templates: &Templates,
    runner_is_confined: bool,
) -> bool {
    let _ = (cfg, templates);
    !runner_is_confined
}

/// Build the spawn config for launching a service daemon. Pure (no spawning), so
/// it can be asserted directly in tests.
///
/// Every configured service retains its declared service sandbox, whose write
/// grant is [`BuildServiceConfig::launch_grant`] for `roots`: each managed
/// root's `target/` trees by construction, then the configured extras.
fn build_service_spawn_config(
    cfg: &BuildServiceConfig,
    templates: &Templates,
    roots: &[PathBuf],
    deny_read: Vec<PathBuf>,
) -> Option<SpawnConfig> {
    let start = cfg.expanded_start(templates);
    let (program, args) = start.split_first()?;
    let write_globs = cfg.launch_grant(templates, roots);
    let state_dir = cfg
        .expanded_state_dir(templates)
        .unwrap_or_else(|| templates.cairn_home.clone());

    // A build service is Cairn's own daemon, but it runs compilers over agent
    // code, so its profile also closes with the operator credential boundary.
    let confinement = if sandbox::is_available() {
        sandbox::Confinement::Fenced(SandboxPolicy::for_service(
            &state_dir,
            &write_globs,
            deny_read,
        ))
    } else {
        sandbox::Confinement::Unconfined
    };
    let mut config = SpawnConfig::new(program)
        .args(args.iter().cloned())
        // A foreground service keeps its cwd for its whole lifetime. Never let
        // it inherit the runner's cwd: the installed app directory is replaced
        // during rebuilds and build-slot directories are reclaimed after a
        // batch, either of which leaves rustc unable to resolve the daemon's
        // working directory. The persistent state directory already exists
        // before launch.
        .cwd(&state_dir.to_string_lossy())
        .confinement(confinement);
    // A daemon manages its own lifetime; don't hold its stdio pipes open.
    config.capture_stdout = false;
    config.capture_stderr = false;
    for (k, v) in cfg.expanded_env(templates) {
        config = config.env(&k, &v);
    }
    // Daemon-only launch env (e.g. sccache's foreground-server switches and its
    // error-log diagnostics) is applied to the daemon spawn but is deliberately
    // absent from `merge_client_env`, so it never leaks into client tooling.
    for (k, v) in cfg.expanded_launch_env(templates) {
        config = config.env(&k, &v);
    }
    Some(config)
}

/// The env vars that name a process's temp directory. A daemon keeps the values
/// it was launched with for its entire life, which is why a configured build
/// service may pin them explicitly instead of inheriting them.
const DAEMON_TEMP_ENV: [&str; 3] = ["TMPDIR", "TMP", "TEMP"];

/// The distinct temp directories a service's launch env pins, in declaration
/// order of [`DAEMON_TEMP_ENV`].
fn daemon_temp_dirs(cfg: &BuildServiceConfig, templates: &Templates) -> Vec<PathBuf> {
    let launch_env = cfg.expanded_launch_env(templates);
    let mut dirs: Vec<PathBuf> = Vec::new();
    for key in DAEMON_TEMP_ENV {
        if let Some(dir) = launch_env.get(key).map(PathBuf::from) {
            if !dirs.contains(&dir) {
                dirs.push(dir);
            }
        }
    }
    dirs
}

/// Launch one machine-wide service daemon via the spawner.
///
/// Creates the temp directories the launch env pins first: the daemon inherits
/// those values for life, and sccache's server aborts a compile outright when the
/// path is missing, so the directory must exist before the process does.
fn launch_service(
    spawner: &dyn ProcessSpawner,
    cfg: &BuildServiceConfig,
    templates: &Templates,
    roots: &[PathBuf],
    deny_read: Vec<PathBuf>,
) -> Result<Box<dyn ChildProcess>, String> {
    let config = build_service_spawn_config(cfg, templates, roots, deny_read)
        .ok_or_else(|| "build service has an empty start command".to_string())?;
    for dir in daemon_temp_dirs(cfg, templates) {
        if let Err(e) = std::fs::create_dir_all(&dir) {
            log::warn!("create build service temp dir {dir:?}: {e}");
        }
    }
    spawner.spawn(config)
}

/// The record of what the running daemon was granted, written beside its state
/// by the runner that launched it.
///
/// A sandbox grant is fixed when a process starts and cannot be read back from
/// the kernel, so this record is the only account of it. It names the process
/// it describes by pid and start token, so a record left by an earlier daemon
/// never vouches for a later one on a reused pid.
const DAEMON_GRANT_FILE: &str = ".cairn-daemon-grant.json";

/// Where every runner sharing the daemon registers its managed roots, one file
/// per Cairn home, so a launch by any of them grants all of them.
const GRANT_ROOTS_DIR: &str = "grant-roots";

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DaemonGrantRecord {
    pub(crate) pid: u32,
    pub(crate) start_token: String,
    pub(crate) launch_fingerprint: LaunchFingerprint,
    /// Canonical managed roots whose `target/` trees the daemon may write.
    pub(crate) roots: Vec<String>,
    /// The whole write grant, as handed to the sandbox, for an operator reading the file.
    pub(crate) write_globs: Vec<String>,
    pub(crate) launcher_cairn_home: String,
    pub(crate) launched_at_unix_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct RootRegistration {
    cairn_home: String,
    roots: Vec<String>,
    runner_pid: u32,
    runner_start_token: Option<String>,
    updated_at_unix_ms: u64,
}

/// Where one service's grant record and root registry live: its state
/// directory, which every runner on the machine resolves identically, or a
/// per-service directory beside where [`machine_lock_path`] falls back to.
fn grant_state_dir(cfg: &BuildServiceConfig, templates: &Templates, name: &str) -> PathBuf {
    cfg.expanded_state_dir(templates).unwrap_or_else(|| {
        templates
            .home
            .join(".cache")
            .join(format!("cairn-build-service-{name}"))
    })
}

fn root_strings(roots: &[PathBuf]) -> Vec<String> {
    roots
        .iter()
        .map(|root| root.display().to_string())
        .collect()
}

fn write_json_atomically<T: serde::Serialize>(path: &Path, value: &T) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| format!("{} has no parent", path.display()))?;
    std::fs::create_dir_all(parent).map_err(|e| format!("create {}: {e}", parent.display()))?;
    let bytes = serde_json::to_vec_pretty(value).map_err(|e| format!("encode: {e}"))?;
    let temporary = path.with_extension(format!("{}.tmp", std::process::id()));
    std::fs::write(&temporary, bytes).map_err(|e| format!("write {}: {e}", temporary.display()))?;
    std::fs::rename(&temporary, path).map_err(|e| {
        let _ = std::fs::remove_file(&temporary);
        format!("commit {}: {e}", path.display())
    })
}

fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Option<T> {
    serde_json::from_slice(&std::fs::read(path).ok()?).ok()
}

fn read_grant_record(dir: &Path) -> Option<DaemonGrantRecord> {
    read_json(&dir.join(DAEMON_GRANT_FILE))
}

fn write_grant_record(dir: &Path, record: &DaemonGrantRecord) -> Result<(), String> {
    write_json_atomically(&dir.join(DAEMON_GRANT_FILE), record)
}

fn registration_path(dir: &Path, cairn_home: &Path) -> PathBuf {
    let digest = Sha256::digest(cairn_home.to_string_lossy().as_bytes());
    let key: String = digest[..8]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    dir.join(GRANT_ROOTS_DIR).join(format!("{key}.json"))
}

/// Record this runner's managed roots beside the daemon, rewriting the file
/// only when what it says has changed.
fn register_runner_roots(
    dir: &Path,
    templates: &Templates,
    roots: &[String],
) -> Result<(), String> {
    let path = registration_path(dir, &templates.cairn_home);
    let cairn_home = templates.cairn_home.display().to_string();
    let runner_pid = std::process::id();
    let runner_start_token = cairn_common::process_identity::process_start_token(runner_pid);
    let unchanged = read_json::<RootRegistration>(&path).is_some_and(|current| {
        current.cairn_home == cairn_home
            && current.roots == roots
            && current.runner_pid == runner_pid
            && current.runner_start_token == runner_start_token
    });
    if unchanged {
        return Ok(());
    }
    write_json_atomically(
        &path,
        &RootRegistration {
            cairn_home,
            roots: roots.to_vec(),
            runner_pid,
            runner_start_token,
            updated_at_unix_ms: unix_ms(),
        },
    )
}

fn registered_runner_gone(registration: &RootRegistration) -> bool {
    match &registration.runner_start_token {
        Some(token) => {
            cairn_common::process_identity::process_presence(registration.runner_pid, token)
                .is_gone()
        }
        None => !cairn_common::process_identity::process_is_alive(registration.runner_pid),
    }
}

/// The managed roots a launch is granted: this runner's own, then every root
/// registered by a runner that is still running, so runners that share the
/// daemon converge on one grant instead of relaunching it for each other.
/// Registrations whose runner is gone are pruned here.
fn live_registered_roots(dir: &Path, own: &[String]) -> Vec<String> {
    let mut others: Vec<String> = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir.join(GRANT_ROOTS_DIR)) {
        for path in entries.flatten().map(|entry| entry.path()) {
            if path.extension().is_none_or(|extension| extension != "json") {
                continue;
            }
            let Some(registration) = read_json::<RootRegistration>(&path) else {
                continue;
            };
            if registered_runner_gone(&registration) {
                log::debug!(
                    "pruning the build-service root registration of {} (runner pid {} is gone)",
                    registration.cairn_home,
                    registration.runner_pid
                );
                let _ = std::fs::remove_file(&path);
                continue;
            }
            others.extend(registration.roots);
        }
    }
    others.sort();
    let mut roots = own.to_vec();
    for root in others {
        if !roots.contains(&root) {
            roots.push(root);
        }
    }
    roots
}

/// Whether the listening daemon's recorded grant covers this runner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum GrantVerdict {
    Covered(DaemonGrantRecord),
    Stale(GrantStale),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GrantStale {
    /// What differs, phrased for the operator log.
    pub(crate) reason: String,
    /// The record, when it does describe the listening daemon: its roots still
    /// serve while the daemon cannot be replaced.
    pub(crate) record: Option<DaemonGrantRecord>,
}

/// Judge the listening daemon's grant against this runner.
///
/// `Covered` needs a record naming the listener by pid and start token, all of
/// this runner's managed roots in it, and — only when this runner launched it
/// — an unchanged [`LaunchFingerprint`]. A daemon another runner launched from
/// a different configuration is that runner's to replace, which is what keeps
/// runners sharing one daemon from relaunching it for each other.
fn running_grant_verdict(
    control: &dyn ListenerProcessControl,
    addr: &str,
    cfg: &BuildServiceConfig,
    templates: &Templates,
    dir: &Path,
) -> GrantVerdict {
    let stale = |reason: String, record: Option<DaemonGrantRecord>| {
        GrantVerdict::Stale(GrantStale { reason, record })
    };
    let listener = match control.listener(addr) {
        Ok(Some(listener)) => listener,
        Ok(None) => {
            return stale(
                format!("launch grant changed (no listener on {addr} could be identified)"),
                None,
            )
        }
        Err(e) => {
            return stale(
                format!(
                    "launch grant changed (the listener on {addr} could not be identified: {e})"
                ),
                None,
            )
        }
    };
    let Some(record) = read_grant_record(dir) else {
        return stale("launch grant changed (no grant record)".to_string(), None);
    };
    let token = control.start_token(listener.pid);
    if record.pid != listener.pid || token.as_deref() != Some(record.start_token.as_str()) {
        return stale(
            format!(
                "launch grant changed (the grant record describes pid {} started {}, not the listening pid {} started {})",
                record.pid,
                record.start_token,
                listener.pid,
                token.as_deref().unwrap_or("at an unreadable time")
            ),
            None,
        );
    }
    let own = root_strings(&managed_build_roots(templates));
    if let Some(missing) = own.iter().find(|root| !record.roots.contains(root)) {
        let reason = format!("launch grant changed (missing root {missing})");
        return stale(reason, Some(record));
    }
    if record.launcher_cairn_home == templates.cairn_home.display().to_string() {
        let changed = cfg
            .launch_fingerprint(templates)
            .changed_parts(&record.launch_fingerprint);
        if !changed.is_empty() {
            let reason = format!("launch fingerprint changed ({})", changed.join(", "));
            return stale(reason, Some(record));
        }
    }
    GrantVerdict::Covered(record)
}

/// What a launch is granted, where that is recorded, and what it may not adopt.
struct LaunchGrant<'a> {
    /// Where the grant record lives ([`grant_state_dir`]).
    dir: &'a Path,
    /// The managed roots the daemon is granted ([`live_registered_roots`]).
    roots: &'a [PathBuf],
    /// An incarnation already proven unable to serve. It answers its health
    /// round trip and its grant may be covered, so without this it would be
    /// adopted straight back.
    condemned: Option<&'a (u32, String)>,
}

/// Why a healthy listener must not be adopted, or `None` when it may be.
fn adoption_refusal(
    control: &dyn ListenerProcessControl,
    addr: &str,
    cfg: &BuildServiceConfig,
    templates: &Templates,
    grant: &LaunchGrant<'_>,
) -> Option<String> {
    match running_grant_verdict(control, addr, cfg, templates, grant.dir) {
        GrantVerdict::Covered(record) => grant
            .condemned
            .filter(|(pid, token)| *pid == record.pid && *token == record.start_token)
            .map(|_| {
                format!(
                    "listener pid {} is the incarnation already proven unable to serve",
                    record.pid
                )
            }),
        GrantVerdict::Stale(stale) => Some(stale.reason),
    }
}

/// Whether the service's exit-0 `command` liveness probe succeeds. A cheap
/// reachability check with no deadline (the original `command`-probe semantics);
/// a non-zero exit or a spawn error reads as unreachable.
fn command_probe_ok(cmd: &[String]) -> bool {
    let Some((prog, args)) = cmd.split_first() else {
        return false;
    };
    cairn_platform::process::observed_output(crate::env::command(prog).args(args))
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Health verdict for a supervised build-service daemon.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ServiceHealth {
    /// Reachable and answering a request/response round-trip within the deadline.
    Healthy,
    /// Listening, but unable to serve the builds it accepts — either not
    /// answering the round-trip within the deadline (a wedged-but-listening
    /// daemon, e.g. sccache stuck on its LRU cache mutex) or answering while
    /// unable to use its temp directory (see [`listener_temp_dir_live`]).
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
fn probe_service(
    spawner: &dyn ProcessSpawner,
    probe: &ReadyProbe,
    env: &HashMap<String, String>,
    deadline: Duration,
) -> ServiceObservation {
    let live = match (&probe.tcp, &probe.command) {
        (Some(addr), _) => tcp_reachable(addr),
        (None, Some(cmd)) => command_probe_ok(cmd),
        (None, None) => true, // no liveness probe configured
    };
    if !live {
        return ServiceObservation {
            health: ServiceHealth::Down,
            round_trip: None,
            unfit: None,
            grant: None,
        };
    }
    let Some(cmd) = &probe.round_trip else {
        // Live, and no round-trip configured: liveness is all we can assert.
        return ServiceObservation {
            health: ServiceHealth::Healthy,
            round_trip: None,
            unfit: None,
            grant: None,
        };
    };
    let round_trip = run_round_trip(spawner, cmd, env, deadline);
    ServiceObservation {
        health: if round_trip.healthy() {
            ServiceHealth::Healthy
        } else {
            ServiceHealth::Wedged
        },
        round_trip: Some(round_trip),
        unfit: None,
        grant: None,
    }
}

/// The verdict alone, for callers that act on it and do not report it.
fn probe_health(
    spawner: &dyn ProcessSpawner,
    probe: &ReadyProbe,
    env: &HashMap<String, String>,
    deadline: Duration,
) -> ServiceHealth {
    probe_service(spawner, probe, env, deadline).health
}

/// A health verdict together with the evidence behind it.
#[derive(Debug)]
pub(crate) struct ServiceObservation {
    pub(crate) health: ServiceHealth,
    /// Present whenever a round-trip was actually run. Absent means liveness
    /// alone decided the verdict, which is a different fact from a round-trip
    /// that ran and failed.
    pub(crate) round_trip: Option<RoundTrip>,
    /// Why the daemon was judged unable to do its work, when it was. Set only
    /// by a FUNCTIONAL verdict (see [`unfit_to_compile`]), never by a liveness
    /// or round-trip failure, which already speak for themselves. Carrying the
    /// sentence rather than a flag is what lets the panel and the agent
    /// advisory say the numbers that produced the verdict.
    pub(crate) unfit: Option<String>,
    /// Whether the answering daemon's recorded grant covers this runner.
    /// Judged only for a healthy, identifiable listener, and never a health
    /// verdict: a stale grant leaves a daemon that answers, and is only unfit
    /// for the roots it was not granted.
    pub(crate) grant: Option<GrantVerdict>,
}

/// What a health round-trip did, as opposed to merely whether it passed.
///
/// The supervisor used to reduce this to a bool. That is why a daemon misjudged
/// as wedged three times a minute for three weeks left behind no evidence of
/// *why* beyond the verdict itself, and why the only way to answer the question
/// was to reason backwards from log timestamps (CAIRN-3332). A verdict a person
/// cannot check is a verdict that can be wrong indefinitely.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RoundTripOutcome {
    /// The probe exited within the deadline, carrying its own status.
    Exited { success: bool, code: Option<i32> },
    /// Still running at the deadline, so it was killed. This is the wedged
    /// signature: sccache's protocol has no per-request timeout, so a
    /// listening-but-hung server blocks the client's request read forever.
    TimedOut,
    /// The probe could not be started, or could not be waited on.
    Unrunnable,
}

#[derive(Debug, Clone)]
pub(crate) struct RoundTrip {
    pub(crate) outcome: RoundTripOutcome,
    pub(crate) elapsed: Duration,
    /// The probe's stdout. For sccache this is the statistics report, which is
    /// simultaneously the health signal and the only source of cache counters —
    /// which is why one round trip serves both and the UI never runs its own.
    pub(crate) stdout: String,
    /// Bounded tail of whatever the probe said about failing.
    pub(crate) diagnostic: String,
}

impl RoundTrip {
    fn healthy(&self) -> bool {
        matches!(self.outcome, RoundTripOutcome::Exited { success: true, .. })
    }

    /// One line an operator can read, naming what happened and how long it took.
    fn summary(&self) -> String {
        let ms = self.elapsed.as_millis();
        let what = match &self.outcome {
            RoundTripOutcome::Exited { success: true, .. } => "answered".to_string(),
            RoundTripOutcome::Exited { code, .. } => match code {
                Some(code) => format!("failed with status {code}"),
                None => "failed without a status".to_string(),
            },
            RoundTripOutcome::TimedOut => "never answered (killed at the deadline)".to_string(),
            RoundTripOutcome::Unrunnable => "could not be run".to_string(),
        };
        if self.diagnostic.trim().is_empty() {
            format!("health round trip {what} in {ms} ms")
        } else {
            format!(
                "health round trip {what} in {ms} ms: {}",
                self.diagnostic.trim()
            )
        }
    }
}

/// Run a health round-trip command with a HARD, Rust-enforced deadline: spawn it
/// unconfined (the probe runs in the runner, not a fenced agent), poll for exit,
/// and if it exceeds the deadline, kill it. The service's client env is passed so
/// the probe talks to the right daemon; the daemon-only launch env is excluded
/// (e.g. `SCCACHE_START_SERVER` would make `sccache --show-stats` refuse to run).
///
/// Both streams are captured and drained **concurrently**, by reader threads
/// started before the wait loop. This ordering is the whole correctness of the
/// probe, not a detail.
///
/// `sccache --show-stats` can block writing its own report when nothing is
/// reading it, and a probe blocked on its own output looks exactly like a daemon
/// that never answered. Measured against the live daemon on the machine this was
/// diagnosed on: undrained, 11 of 20 probes hit a 6 s deadline; drained
/// concurrently, 0 of 20 did and the slowest took 50 ms. The old probe inherited
/// the runner's own stdout — a heavily contended pipe carrying tens of megabytes
/// of log traffic — which is why it hung on every tick for three weeks and
/// reported a daemon answering shell clients in 11 ms as wedged (CAIRN-3332).
///
/// A health check that its own success can hang is not a health check. Killing
/// the child on timeout closes the pipes, so the readers reach EOF and the joins
/// below cannot outlive the deadline.
fn run_round_trip(
    spawner: &dyn ProcessSpawner,
    cmd: &[String],
    env: &HashMap<String, String>,
    deadline: Duration,
) -> RoundTrip {
    let started = std::time::Instant::now();
    let Some((program, args)) = cmd.split_first() else {
        return RoundTrip {
            outcome: RoundTripOutcome::Exited {
                success: true,
                code: Some(0),
            },
            elapsed: Duration::ZERO,
            stdout: String::new(),
            diagnostic: String::new(),
        };
    };
    let mut config = SpawnConfig::new(program).args(args.iter().cloned());
    config.capture_stdout = true;
    config.capture_stderr = true;
    for (k, v) in env {
        config = config.env(k, v);
    }
    let mut child = match spawner.spawn(config) {
        Ok(child) => child,
        Err(e) => {
            log::debug!("health round-trip spawn failed: {e}");
            return RoundTrip {
                outcome: RoundTripOutcome::Unrunnable,
                elapsed: started.elapsed(),
                stdout: String::new(),
                diagnostic: bounded_tail(&e),
            };
        }
    };
    // Start draining BEFORE waiting. Anything after this point that blocks the
    // child on its output is a bug in this function.
    let stdout_drain = child.take_stdout().map(drain_in_background);
    let stderr_drain = child.take_stderr().map(drain_in_background);
    let mut wait_error = String::new();
    let outcome = loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                break RoundTripOutcome::Exited {
                    success: status.success(),
                    code: status.code(),
                }
            }
            Ok(None) => {
                if started.elapsed() >= deadline {
                    // Wedged: the request read is blocked. Kill the probe (it
                    // would otherwise hang forever) and report what we saw.
                    let _ = child.kill();
                    break RoundTripOutcome::TimedOut;
                }
                std::thread::sleep(HEALTH_POLL_INTERVAL);
            }
            Err(e) => {
                wait_error = e.to_string();
                break RoundTripOutcome::Unrunnable;
            }
        }
    };
    let elapsed = started.elapsed();
    let stdout = collect_drained(stdout_drain);
    let stderr = collect_drained(stderr_drain);
    let diagnostic = if wait_error.is_empty() {
        bounded_tail(&stderr)
    } else {
        bounded_tail(&format!("{wait_error}; {stderr}"))
    };
    RoundTrip {
        outcome,
        elapsed,
        stdout,
        diagnostic,
    }
}

/// Drain a captured stream on its own thread, bounded, so the child can never
/// block on output nobody is reading. Ends at EOF, which a killed child's closed
/// pipe provides.
fn drain_in_background(
    stream: Box<dyn std::io::BufRead + Send>,
) -> std::thread::JoinHandle<String> {
    std::thread::spawn(move || {
        let mut buffer = Vec::new();
        let _ = stream.take(PROBE_OUTPUT_BYTES).read_to_end(&mut buffer);
        String::from_utf8_lossy(&buffer).into_owned()
    })
}

fn collect_drained(drain: Option<std::thread::JoinHandle<String>>) -> String {
    drain
        .map(|handle| handle.join().unwrap_or_default())
        .unwrap_or_default()
}

/// The tail of a diagnostic, bounded on a character boundary.
///
/// A tail rather than a head: the last thing a failing process said is the part
/// that names the failure.
fn bounded_tail(text: &str) -> String {
    let trimmed = text.trim();
    if trimmed.len() <= DIAGNOSTIC_BYTES {
        return trimmed.to_string();
    }
    let mut start = trimmed.len() - DIAGNOSTIC_BYTES;
    while start < trimmed.len() && !trimmed.is_char_boundary(start) {
        start += 1;
    }
    format!("\u{2026}{}", &trimmed[start..])
}

/// How a supervised child ended, in words.
///
/// A signal and a non-zero status are different stories — an operating system
/// reclaiming memory versus a daemon refusing to start — and collapsing them
/// into "it stopped answering" is what left the death of a shared daemon
/// undiagnosable.
fn describe_exit(status: &std::process::ExitStatus) -> String {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(signal) = status.signal() {
            return format!("killed by signal {signal}");
        }
    }
    match status.code() {
        Some(code) => format!("exited with status {code}"),
        None => "exited without a status".to_string(),
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

fn any_tcp_endpoint_reachable(addrs: impl IntoIterator<Item = std::net::SocketAddr>) -> bool {
    addrs
        .into_iter()
        .any(|addr| TcpStream::connect_timeout(&addr, TCP_PROBE_TIMEOUT).is_ok())
}

fn tcp_reachable(addr: &str) -> bool {
    addr.to_socket_addrs()
        .map(any_tcp_endpoint_reachable)
        .unwrap_or(false)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ListenerProcess {
    pid: u32,
    executable: PathBuf,
}

trait ListenerProcessControl: Send + Sync {
    fn listener(&self, addr: &str) -> Result<Option<ListenerProcess>, String>;
    fn terminate(&self, pid: u32) -> Result<(), String>;
    /// The environment the listening process was launched with. A daemon's
    /// environment is fixed at exec, so this is the only way to learn what a
    /// daemon Cairn did not spawn — one adopted from an earlier runner, or
    /// auto-started by a client — is actually using.
    fn environ(&self, pid: u32) -> Result<HashMap<String, String>, String>;
    /// The kernel start token of `pid`. With the pid it names one daemon
    /// incarnation, which is what a grant record and a fitness verdict are about.
    fn start_token(&self, pid: u32) -> Option<String>;
}

struct OsListenerProcessControl;

impl ListenerProcessControl for OsListenerProcessControl {
    fn start_token(&self, pid: u32) -> Option<String> {
        cairn_common::process_identity::process_start_token(pid)
    }

    fn listener(&self, addr: &str) -> Result<Option<ListenerProcess>, String> {
        let resolved: Vec<_> = addr
            .to_socket_addrs()
            .map_err(|e| format!("resolve TCP address '{addr}': {e}"))?
            .collect();
        let port = resolved
            .first()
            .map(|addr| addr.port().to_string())
            .ok_or_else(|| format!("TCP address '{addr}' resolved to no endpoints"))?;
        let endpoints: Vec<String> = resolved.iter().map(ToString::to_string).collect();
        let output = cairn_platform::process::observed_output(crate::env::command("lsof").args([
            "-nP",
            &format!("-iTCP:{port}"),
            "-sTCP:LISTEN",
            "-Fpn",
        ]))
        .map_err(|e| format!("inspect listener on {addr}: {e}"))?;
        if !output.status.success() && output.stdout.is_empty() {
            return Ok(None);
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        let Some(pid) = listener_pid_from_lsof(&stdout, &endpoints)? else {
            return Ok(None);
        };
        let executable = process_executable(pid)?;
        Ok(Some(ListenerProcess { pid, executable }))
    }

    fn environ(&self, pid: u32) -> Result<HashMap<String, String>, String> {
        process_environ(pid)
    }

    fn terminate(&self, pid: u32) -> Result<(), String> {
        #[cfg(unix)]
        {
            nix::sys::signal::kill(
                nix::unistd::Pid::from_raw(pid as i32),
                nix::sys::signal::Signal::SIGKILL,
            )
            .map_err(|e| format!("terminate listener pid {pid}: {e}"))?;
            Ok(())
        }
        #[cfg(not(unix))]
        {
            let _ = pid;
            Err("listener termination is unsupported on this platform".to_string())
        }
    }
}

fn listener_pid_from_lsof(output: &str, endpoints: &[String]) -> Result<Option<u32>, String> {
    let mut current_pid = None;
    let mut matches = Vec::new();
    for line in output.lines() {
        if let Some(pid) = line
            .strip_prefix('p')
            .and_then(|pid| pid.parse::<u32>().ok())
        {
            current_pid = Some(pid);
            continue;
        }
        let Some(name) = line.strip_prefix('n') else {
            continue;
        };
        let endpoint = name
            .strip_prefix("TCP ")
            .unwrap_or(name)
            .strip_suffix(" (LISTEN)")
            .unwrap_or(name);
        if endpoints.iter().any(|expected| expected == endpoint) {
            if let Some(pid) = current_pid {
                matches.push(pid);
            }
        }
    }
    matches.sort_unstable();
    matches.dedup();
    match matches.as_slice() {
        [] => Ok(None),
        [pid] => Ok(Some(*pid)),
        _ => Err(format!(
            "multiple listener processes matched configured endpoints {}: {:?}",
            endpoints.join(", "),
            matches
        )),
    }
}

/// Parse one `KEY=VALUE` environment token. Keys are restricted to the shell's
/// portable character set so tokens from a process's *command line* (`--flag=x`,
/// a path fragment) are never mistaken for environment entries.
fn parse_env_assignment(token: &str) -> Option<(String, String)> {
    let (key, value) = token.split_once('=')?;
    let portable = !key.is_empty()
        && key
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_');
    portable.then(|| (key.to_string(), value.to_string()))
}

#[cfg(target_os = "linux")]
fn process_environ(pid: u32) -> Result<HashMap<String, String>, String> {
    let raw = std::fs::read(format!("/proc/{pid}/environ"))
        .map_err(|e| format!("read environment for listener pid {pid}: {e}"))?;
    Ok(String::from_utf8_lossy(&raw)
        .split('\0')
        .filter_map(parse_env_assignment)
        .collect())
}

/// BSD `ps` prints the command line and the environment as one whitespace-joined
/// blob, so a value containing a space is truncated at that space. Callers must
/// therefore treat this as evidence, never as ground truth: every consumer here
/// fails OPEN, acting only on what it can positively confirm.
#[cfg(not(target_os = "linux"))]
fn process_environ(pid: u32) -> Result<HashMap<String, String>, String> {
    let output = cairn_platform::process::observed_output(crate::env::command("ps").args([
        "eww",
        "-p",
        &pid.to_string(),
        "-o",
        "command=",
    ]))
    .map_err(|e| format!("read environment for listener pid {pid}: {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "read environment for listener pid {pid}: ps failed"
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout)
        .split_whitespace()
        .filter_map(parse_env_assignment)
        .collect())
}

/// Whether the daemon listening at `addr` can still USE its temp directory.
///
/// A daemon that answers a health round-trip is not necessarily able to do work.
/// sccache's server creates a temp directory per cache-miss compile to stage
/// rustc's output; when the directory it was launched with has been reclaimed —
/// classically, an ephemeral build-cell scratch path inherited by a machine-wide
/// daemon — it accepts every connection and then kills every compile with
/// `Failed to create temp dir`. Answering while unable to work is worse than
/// being down, because it suppresses the client wrapper's fallback.
///
/// Fails OPEN: only a temp directory we can positively confirm is gone counts as
/// broken. An unreadable environment, an unresolvable listener, or a relative
/// value proves nothing, and killing a working daemon on a guess is the more
/// expensive mistake.
fn listener_temp_dir_live(control: &dyn ListenerProcessControl, addr: &str) -> bool {
    let listener = match control.listener(addr) {
        Ok(Some(listener)) => listener,
        Ok(None) => return true,
        Err(e) => {
            log::debug!("inspect listener on {addr}: {e}");
            return true;
        }
    };
    let environ = match control.environ(listener.pid) {
        Ok(environ) => environ,
        Err(e) => {
            log::debug!("{e}");
            return true;
        }
    };
    DAEMON_TEMP_ENV.iter().all(|key| match environ.get(*key) {
        Some(dir) if dir.starts_with('/') => {
            let live = Path::new(dir).is_dir();
            if !live {
                log::warn!(
                    "listener pid {} on {addr} holds a {key} that no longer exists: {dir}",
                    listener.pid
                );
            }
            live
        }
        _ => true,
    })
}

const ADOPTION_ENV_KEYS: [&str; 5] = [
    "SCCACHE_DIR",
    "SCCACHE_SERVER_PORT",
    "TMPDIR",
    "TMP",
    "TEMP",
];

fn listener_environment_mismatch(
    control: &dyn ListenerProcessControl,
    addr: &str,
    cfg: &BuildServiceConfig,
    templates: &Templates,
) -> Option<String> {
    let listener = control.listener(addr).ok().flatten()?;
    let observed = control.environ(listener.pid).ok()?;
    let mut expected = cfg.expanded_env(templates);
    expected.extend(cfg.expanded_launch_env(templates));
    ADOPTION_ENV_KEYS.iter().find_map(|key| {
        let expected = expected.get(*key)?;
        let observed = observed.get(*key)?;
        (observed != expected).then(|| {
            format!(
                "listener pid {} at {} has {key}={observed:?}, expected {expected:?}",
                listener.pid,
                listener.executable.display()
            )
        })
    })
}

/// Where the capability probe compiles for one managed root.
///
/// Inside that root's granted `target/` shape (`.compile-cache-probe` is what
/// the grant's leading `**` matches), so a passing compile proves exactly the
/// write every build under that root needs, and a failing one shows the root is
/// missing from the daemon's grant. A dot-directory, so nothing that enumerates
/// cells or worktrees under the root mistakes it for one.
fn capability_probe_dir(root: &Path) -> PathBuf {
    root.join(".compile-cache-probe").join("target")
}

/// A trivial compile is fast, but this runs on a loaded developer machine and a
/// cold rustc is not instant. Generous, because the cost of exceeding it is a
/// false accusation against a shared daemon.
const CAPABILITY_PROBE_DEADLINE: Duration = Duration::from_secs(60);

/// How soon an inconclusive root is probed again, doubling per attempt.
const PROBE_RETRY_MIN: Duration = Duration::from_secs(60);
const PROBE_RETRY_MAX: Duration = Duration::from_secs(30 * 60);

fn probe_retry_after(attempts: u32) -> Duration {
    PROBE_RETRY_MIN
        .saturating_mul(1u32 << attempts.saturating_sub(1).min(5))
        .min(PROBE_RETRY_MAX)
}

/// What one managed root's probe established about the daemon.
#[derive(Debug, Clone, PartialEq, Eq)]
enum RootFitness {
    /// The daemon compiled into the root.
    Proven,
    /// The daemon could not, while rustc run directly could. `summary` is the
    /// few words the status line carries; `detail` is the whole account.
    Unfit { summary: String, detail: String },
    /// Nothing was learned (a busy daemon, a broken toolchain, an uncreatable
    /// probe directory), so the root is retried later rather than judged.
    Inconclusive(String),
}

/// The probe's findings for one daemon incarnation.
#[derive(Debug, Default)]
struct FitnessReport {
    roots: BTreeMap<String, RootFitness>,
    /// A failure that is about the daemon rather than any root: it wrote a
    /// cache entry and could not serve it back.
    daemon_unfit: Option<String>,
}

/// How the cache-hit leg went, when it ran.
enum HitLeg {
    Served,
    Failed(String),
}

/// A name unique to one probe compile, so concurrent probes from several
/// runners, or of several roots within one millisecond, never share a file.
fn probe_nonce() -> String {
    static SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let sequence = SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!("{}_{}_{sequence}", unix_ms(), std::process::id())
}

/// The few words a status line carries for a failed probe compile.
fn unfit_summary(attempt: &RoundTrip) -> String {
    if attempt.diagnostic.contains("Operation not permitted") {
        "write refused".to_string()
    } else {
        "compile failed".to_string()
    }
}

/// Ask the daemon to compile into each of `roots`, and once in total to serve
/// what it just compiled. See [`probe_root`] for the experiment and its guards.
///
/// The miss leg runs once per root because a write grant is per root. The hit
/// leg exercises the cache directory, which no root changes, so it runs once.
fn probe_daemon_fitness(
    spawner: &dyn ProcessSpawner,
    cfg: &BuildServiceConfig,
    templates: &Templates,
    env: &HashMap<String, String>,
    roots: &[String],
    deadline: Duration,
) -> FitnessReport {
    let mut report = FitnessReport::default();
    let Some(client) = cfg.expanded_start(templates).into_iter().next() else {
        return report;
    };
    let mut probe_env = env.clone();
    probe_env.insert("CARGO_INCREMENTAL".to_string(), "0".to_string());
    let mut hit_settled = false;
    for root in roots {
        let (fitness, hit) = probe_root(
            spawner,
            &client,
            Path::new(root),
            &probe_env,
            deadline,
            !hit_settled,
        );
        match hit {
            Some(HitLeg::Served) => hit_settled = true,
            Some(HitLeg::Failed(reason)) => {
                hit_settled = true;
                report.daemon_unfit = Some(reason);
            }
            None => {}
        }
        report.roots.insert(root.clone(), fitness);
    }
    report
}

/// Ask the daemon to do the one thing only it can do, under one managed root,
/// and watch whether it can.
///
/// Rather than infer capability from statistics, this performs the experiment:
/// hand the daemon a crate whose source is ours and trivially valid, ask it to
/// write the output under the root, where every build under that root writes,
/// and see what happens. A genuine compiler error is impossible for that source,
/// so a failure is about the daemon and nothing else.
///
/// The guard that keeps it from ever accusing a daemon for someone else's fault
/// is a **control compile**, which is the fail-open discipline this module
/// already uses — act only on what can be positively confirmed. If the probe
/// fails, the same source is compiled again by rustc DIRECTLY. Only if the
/// direct compile succeeds is the daemon the difference between them; if it
/// fails too, the toolchain is the problem (missing, mismatched, or broken) and
/// the daemon is left alone.
///
/// That control subsumes every "is the toolchain usable" question, which is why
/// there is no separate `rustc` lookup here. Resolving a binary on this path
/// would mean spawning `which` on a supervisor tick to learn something the
/// experiment already reports.
///
/// `CARGO_INCREMENTAL=0` is set explicitly: sccache refuses an incremental
/// invocation with exit 1 before running anything, and a probe defeated by an
/// ambient env var would read as a broken daemon.
///
/// A probe still running at `deadline` proves nothing either way. The daemon
/// answered its statistics round trip moments before (that is what reaching
/// this probe means), and it runs at most one compile per core, so on a machine
/// whose builds hold every slot the probe waits in line behind them. Calling
/// that unfit had the supervisor try to kill a busy daemon four times in one
/// release build on 11-11, and a daemon it had launched itself would have taken
/// every compile on the machine down with it.
///
/// `with_hit` asks for the cache-hit leg after a passing miss; its outcome is
/// returned separately because it is about the daemon, not the root.
fn probe_root(
    spawner: &dyn ProcessSpawner,
    client: &str,
    root: &Path,
    env: &HashMap<String, String>,
    deadline: Duration,
    with_hit: bool,
) -> (RootFitness, Option<HitLeg>) {
    let dir = capability_probe_dir(root);
    if let Err(e) = std::fs::create_dir_all(&dir) {
        return (
            RootFitness::Inconclusive(format!("could not create {}: {e}", dir.display())),
            None,
        );
    }
    // A nonce makes every probe a cache MISS, which is the whole point: a hit
    // would be served without the daemon compiling anything, and compiling is
    // what is under test.
    let nonce = probe_nonce();
    let source = dir.join(format!("probe_{nonce}.rs"));
    if let Err(e) = std::fs::write(&source, format!("pub const PROBE: &str = \"{nonce}\";\n")) {
        return (
            RootFitness::Inconclusive(format!("could not write {}: {e}", source.display())),
            None,
        );
    }

    let out = dir.join(format!("out_{nonce}"));
    let cleanup = || {
        let _ = std::fs::remove_file(&source);
        let _ = std::fs::remove_dir_all(&out);
    };
    if let Err(e) = std::fs::create_dir_all(&out) {
        cleanup();
        return (
            RootFitness::Inconclusive(format!("could not create {}: {e}", out.display())),
            None,
        );
    }

    let rustc_args: Vec<String> = [
        "--crate-name",
        "cairn_compile_cache_probe",
        "--edition=2021",
        "--crate-type",
        "rlib",
        "--emit=dep-info,metadata,link",
        "-C",
        "debuginfo=0",
        "--out-dir",
    ]
    .iter()
    .map(|s| s.to_string())
    .chain([out.to_string_lossy().to_string()])
    .chain([source.to_string_lossy().to_string()])
    .collect();

    let mut through_daemon = vec![client.to_string(), "rustc".to_string()];
    through_daemon.extend(rustc_args.iter().cloned());
    let direct_control = || {
        let mut directly = vec!["rustc".to_string()];
        directly.extend(rustc_args.iter().cloned());
        run_round_trip(spawner, &directly, &HashMap::new(), deadline)
    };
    let queued = |attempt: &RoundTrip| {
        RootFitness::Inconclusive(format!(
            "the daemon had not finished a trivial compile when the probe gave up, which on a \
             busy machine is a queue ({})",
            attempt.summary()
        ))
    };
    let toolchain = |control: &RoundTrip| {
        RootFitness::Inconclusive(format!(
            "rustc could not compile the probe source directly either ({})",
            control.summary()
        ))
    };

    let miss = run_round_trip(spawner, &through_daemon, env, deadline);
    if !miss.healthy() {
        if miss.outcome == RoundTripOutcome::TimedOut {
            cleanup();
            return (queued(&miss), None);
        }
        let control = direct_control();
        cleanup();
        if !control.healthy() {
            return (toolchain(&control), None);
        }
        return (
            RootFitness::Unfit {
                summary: unfit_summary(&miss),
                detail: format!(
                    "the daemon could not compile a trivially valid crate into {}, which rustc \
                     compiled directly without complaint: {}",
                    out.display(),
                    miss.summary()
                ),
            },
            None,
        );
    }
    if !with_hit {
        cleanup();
        return (RootFitness::Proven, None);
    }
    let hit = run_round_trip(spawner, &through_daemon, env, deadline);
    if hit.healthy() {
        cleanup();
        return (RootFitness::Proven, Some(HitLeg::Served));
    }
    if hit.outcome == RoundTripOutcome::TimedOut {
        cleanup();
        return (queued(&hit), None);
    }
    let control = direct_control();
    cleanup();
    if !control.healthy() {
        return (toolchain(&control), None);
    }
    (
        RootFitness::Proven,
        Some(HitLeg::Failed(format!(
            "the daemon could not serve the cache entry it had just written into {}, which rustc \
             compiled directly without complaint — so the compile cache is failing work it \
             accepts rather than serving it: {}",
            out.display(),
            hit.summary()
        ))),
    )
}

/// Where a daemon's grant is recorded, for an assessment that should judge
/// whether it covers this runner.
#[derive(Clone, Copy)]
struct GrantContext<'a> {
    cfg: &'a BuildServiceConfig,
    templates: &'a Templates,
    dir: &'a Path,
}

/// Health as the supervisor acts on it: the [`probe_health`] verdict, downgraded
/// to [`ServiceHealth::Wedged`] when the daemon that answered it can no longer
/// use its temp directory (see [`listener_temp_dir_live`]), plus — for a
/// healthy, identifiable listener and a `grant` context — whether its recorded
/// grant covers this runner ([`running_grant_verdict`]).
///
/// No compile runs here. The capability probe is bounded to once per daemon
/// incarnation and per root, which only the supervisor's runtime record can
/// track ([`probe_daemon_fitness`]); this is also what lets the settings UI ask
/// without ever running a compiler.
fn assess_service(
    spawner: &dyn ProcessSpawner,
    control: &dyn ListenerProcessControl,
    probe: &ReadyProbe,
    env: &HashMap<String, String>,
    deadline: Duration,
    grant: Option<GrantContext<'_>>,
) -> ServiceObservation {
    let mut observation = probe_service(spawner, probe, env, deadline);
    if observation.health != ServiceHealth::Healthy {
        return observation;
    }
    let Some(addr) = probe.tcp.as_deref() else {
        return observation;
    };
    if !listener_temp_dir_live(control, addr) {
        observation.health = ServiceHealth::Wedged;
        return observation;
    }
    if let Some(context) = grant {
        observation.grant = Some(running_grant_verdict(
            control,
            addr,
            context.cfg,
            context.templates,
            context.dir,
        ));
    }
    observation
}

/// The verdict alone, for callers that act on it and do not report it.
fn assess_health(
    spawner: &dyn ProcessSpawner,
    control: &dyn ListenerProcessControl,
    probe: &ReadyProbe,
    env: &HashMap<String, String>,
    deadline: Duration,
) -> ServiceHealth {
    assess_service(spawner, control, probe, env, deadline, None).health
}

#[cfg(target_os = "linux")]
fn process_executable(pid: u32) -> Result<PathBuf, String> {
    std::fs::read_link(format!("/proc/{pid}/exe"))
        .map_err(|e| format!("resolve executable for listener pid {pid}: {e}"))
}

#[cfg(not(target_os = "linux"))]
fn process_executable(pid: u32) -> Result<PathBuf, String> {
    let output = cairn_platform::process::observed_output(crate::env::command("ps").args([
        "-p",
        &pid.to_string(),
        "-o",
        "comm=",
    ]))
    .map_err(|e| format!("resolve executable for listener pid {pid}: {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "resolve executable for listener pid {pid}: ps failed"
        ));
    }
    let executable = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if executable.is_empty() {
        return Err(format!(
            "resolve executable for listener pid {pid}: empty path"
        ));
    }
    Ok(PathBuf::from(executable))
}

fn expected_service_executable(cfg: &BuildServiceConfig, templates: &Templates) -> Option<PathBuf> {
    let program = cfg.expanded_start(templates).into_iter().next()?;
    let path = PathBuf::from(&program);
    let resolved = if path.is_absolute() {
        path
    } else {
        PathBuf::from(crate::env::find_binary(&program).ok()?)
    };
    Some(std::fs::canonicalize(&resolved).unwrap_or(resolved))
}

/// The configuration's identity, stable across loads.
///
/// Object keys are sorted before serializing: `env` and `launchEnv` are hash
/// maps whose iteration order differs between two loads of the same file, so
/// serializing them as iterated made equal configurations disagree, and with
/// them every comparison against a recorded failure's fingerprint.
fn service_config_fingerprint(cfg: &BuildServiceConfig) -> String {
    fn sorted(value: serde_json::Value) -> serde_json::Value {
        match value {
            serde_json::Value::Object(map) => {
                let mut entries: Vec<(String, serde_json::Value)> = map.into_iter().collect();
                entries.sort_by(|(left, _), (right, _)| left.cmp(right));
                serde_json::Value::Object(
                    entries
                        .into_iter()
                        .map(|(key, value)| (key, sorted(value)))
                        .collect(),
                )
            }
            serde_json::Value::Array(items) => {
                serde_json::Value::Array(items.into_iter().map(sorted).collect())
            }
            other => other,
        }
    }
    serde_json::to_value(cfg)
        .map(|value| sorted(value).to_string())
        .unwrap_or_else(|_| format!("{cfg:?}"))
}

/// Whether an observed listener's executable names the program this service
/// launches.
///
/// The two sides are not symmetric, and treating them as if they were is what
/// made a wedged daemon permanently unrecoverable (CAIRN-3332). `expected` is a
/// path this process resolved itself. `actual` is whatever the operating system
/// reported: on macOS [`process_executable`] reads `ps -o comm=`, which gives
/// the name a process was INVOKED with, so a service started as bare `sccache`
/// reports exactly `sccache` -- a bare name carrying no directory.
/// Canonicalizing that against the runner's own working directory can never
/// produce the launch path, so the comparison failed every time, forever, and
/// the identity guard written to protect unrelated processes became the reason
/// Cairn could not act on its own daemon. The runner's log said so once a
/// minute for three weeks: `refusing to terminate pid 2640 at sccache
/// (expected /opt/homebrew/Cellar/sccache/0.15.0/bin/sccache)`.
///
/// So an absolute observation is still compared strictly, and a bare name is
/// compared on the whole of what the OS actually said -- its file name. That is
/// a weaker claim, deliberately: it is only ever reached for a process already
/// proven to hold this service's own configured port, where the alternative to
/// acting is not caution but paralysis. A relative path that carries a
/// directory proves neither thing and is refused.
fn same_executable(actual: &Path, expected: &Path) -> bool {
    if actual.is_absolute() {
        let actual = std::fs::canonicalize(actual).unwrap_or_else(|_| actual.to_path_buf());
        let expected = std::fs::canonicalize(expected).unwrap_or_else(|_| expected.to_path_buf());
        return actual == expected;
    }
    let bare = actual
        .parent()
        .is_none_or(|parent| parent.as_os_str().is_empty());
    bare && actual.file_name().is_some() && actual.file_name() == expected.file_name()
}

/// Parse an `sccache --show-stats` report into a statistics sample.
///
/// The report is a label, a run of two or more spaces, then the value. Labels
/// are matched exactly rather than by prefix: `Cache hits` and `Cache hits rate`
/// differ only by suffix, and reading the second as the first would publish a
/// percentage as a count.
///
/// `None` when the text carries no recognizable report, so a probe that answered
/// with something else becomes a named gap rather than a row of zeros. That
/// distinction is the contract -- a cache nobody has measured must never render
/// as a cache with no hits.
fn parse_cache_stats(report: &str) -> Option<CompileCacheStats> {
    let fields: HashMap<&str, &str> = report.lines().filter_map(split_stats_line).collect();
    let count = |label: &str| {
        fields
            .get(label)
            .and_then(|value| value.trim().parse::<u64>().ok())
    };
    Some(CompileCacheStats {
        compile_requests: count("Compile requests")?,
        cache_hits: count("Cache hits").unwrap_or(0),
        cache_misses: count("Cache misses").unwrap_or(0),
        // Both spellings are summed rather than chosen between: which one an
        // sccache release emits has changed, and a missing label reads as zero.
        non_cacheable: count("Non-cacheable calls").unwrap_or(0)
            + count("Non-cacheable compilations").unwrap_or(0),
        cache_errors: count("Cache read errors").unwrap_or(0)
            + count("Cache write errors").unwrap_or(0)
            + count("Cache errors").unwrap_or(0),
        // What the daemon did with the compiles it ran itself. `Compilations` is
        // sccache's label for the ones that SUCCEEDED, which reads as a total
        // until you notice `Compilation failures` sitting beside it; the field
        // names here say which is which so no consumer has to know that.
        compiles_executed: count("Compile requests executed").unwrap_or(0),
        compilations: count("Compilations").unwrap_or(0),
        compile_failures: count("Compilation failures").unwrap_or(0),
        cache_size_bytes: fields
            .get("Cache size")
            .and_then(|value| parse_bytes(value)),
        max_cache_size_bytes: fields
            .get("Max cache size")
            .and_then(|value| parse_bytes(value)),
    })
}

/// Split one report line at the run of whitespace between its label and value.
/// The FIRST such run: a label never contains a double space, while a value
/// (`50 GiB`) may contain a single one.
fn split_stats_line(line: &str) -> Option<(&str, &str)> {
    let split = line.find("  ")?;
    let label = line[..split].trim();
    let value = line[split..].trim();
    (!label.is_empty() && !value.is_empty()).then_some((label, value))
}

/// Parse a human byte size as sccache prints it (`50 GiB`, `349 MiB`).
fn parse_bytes(value: &str) -> Option<u64> {
    let mut parts = value.split_whitespace();
    let amount: f64 = parts.next()?.parse().ok()?;
    if !amount.is_finite() || amount < 0.0 {
        return None;
    }
    let scale: u64 = match parts.next().unwrap_or("B") {
        "B" | "bytes" | "byte" => 1,
        "KiB" => 1 << 10,
        "MiB" => 1 << 20,
        "GiB" => 1 << 30,
        "TiB" => 1 << 40,
        "kB" => 1_000,
        "MB" => 1_000_000,
        "GB" => 1_000_000_000,
        "TB" => 1_000_000_000_000,
        _ => return None,
    };
    Some((amount * scale as f64) as u64)
}

/// How long to wait before the next relaunch attempt.
///
/// Capped exponential with jitter. The jitter is not cosmetic: a developer
/// machine really does run several runners at once -- the installed app beside
/// dev instances -- each supervising the SAME shared daemon on the same port,
/// and without spread their retries converge into a synchronized launch storm
/// against one socket.
fn restart_backoff(consecutive_failures: u32, jitter: f64) -> Duration {
    let steps = consecutive_failures.saturating_sub(1).min(8);
    let base = RESTART_BACKOFF_MIN
        .saturating_mul(1u32 << steps)
        .min(RESTART_BACKOFF_MAX);
    // `Duration::mul_f64` PANICS on a non-finite factor, and `f64::clamp`
    // propagates NaN rather than clamping it. A spread that could abort the
    // supervisor is worse than no spread, so a value that is not a number is
    // treated as no jitter at all.
    let spread = if jitter.is_finite() {
        jitter.clamp(0.0, 1.0)
    } else {
        0.0
    };
    base.mul_f64(1.0 + spread * 0.5)
}

/// A jitter fraction with no dependency and no global state, derived from the
/// clock. Spread is all that is needed here; unpredictability is not.
fn clock_jitter() -> f64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.subsec_nanos())
        .unwrap_or(0);
    f64::from(nanos % 1_000) / 1_000.0
}

fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_millis() as u64)
        .unwrap_or(0)
}

fn recover_listener_conflict(
    control: &dyn ListenerProcessControl,
    addr: &str,
    expected_executable: &Path,
) -> Result<u32, String> {
    let listener = control
        .listener(addr)?
        .ok_or_else(|| format!("sccache port conflict: no listener found on {addr}"))?;
    if !same_executable(&listener.executable, expected_executable) {
        return Err(format!(
            "sccache port conflict: refusing to terminate pid {} at {} (expected {})",
            listener.pid,
            listener.executable.display(),
            expected_executable.display()
        ));
    }
    control.terminate(listener.pid)?;
    Ok(listener.pid)
}

fn launch_child_exit_ends_startup(submitted_to_user_service: bool, success: bool) -> bool {
    !submitted_to_user_service || !success
}

fn recovery_exhausted(consecutive_failures: u32) -> bool {
    consecutive_failures >= RECOVERY_FAILED_AFTER
}

fn reconcile_launched_service(
    spawner: &dyn ProcessSpawner,
    control: &dyn ListenerProcessControl,
    cfg: &BuildServiceConfig,
    templates: &Templates,
    grant: &LaunchGrant<'_>,
    deny_read: Vec<PathBuf>,
    may_destroy: bool,
) -> Result<Option<Box<dyn ChildProcess>>, String> {
    reconcile_launched_service_with_timeout(
        spawner,
        control,
        cfg,
        templates,
        grant,
        deny_read,
        may_destroy,
        STARTUP_RECONCILE_TIMEOUT,
    )
}

/// Launch the daemon and settle who ends up serving the port: our child, a
/// compatible daemon adopted as it stands, or our child again after a
/// replaced listener.
///
/// A listener is adopted only when its recorded grant covers this runner
/// ([`adoption_refusal`]), in both places adoption happens: the startup loop,
/// where our child lost the port to a healthy listener, and the replacement
/// path after it. Adopting on health alone is how a daemon granted for another
/// build root kept serving after the root moved.
#[allow(clippy::too_many_arguments)]
fn reconcile_launched_service_with_timeout(
    spawner: &dyn ProcessSpawner,
    control: &dyn ListenerProcessControl,
    cfg: &BuildServiceConfig,
    templates: &Templates,
    grant: &LaunchGrant<'_>,
    deny_read: Vec<PathBuf>,
    may_destroy: bool,
    startup_timeout: Duration,
) -> Result<Option<Box<dyn ChildProcess>>, String> {
    let mut child = launch_service(spawner, cfg, templates, grant.roots, deny_read.clone())?;
    let submitted_to_user_service = false;
    let Some(probe) = cfg.ready.as_ref() else {
        return Ok(Some(child));
    };
    let client_env = cfg.expanded_env(templates);
    let deadline = std::time::Instant::now() + startup_timeout;
    let mut submission_completed = false;
    loop {
        if submitted_to_user_service && !submission_completed {
            match child.try_wait() {
                Ok(Some(status)) if status.success() => submission_completed = true,
                Ok(Some(status)) => {
                    return Err(format!(
                        "launchd submission exited before the service became healthy: {}",
                        describe_exit(&status)
                    ));
                }
                Ok(None) | Err(_) if std::time::Instant::now() < deadline => {
                    std::thread::sleep(HEALTH_POLL_INTERVAL);
                    continue;
                }
                Ok(None) | Err(_) => {
                    return Err(format!(
                        "launchd did not finish accepting the service within {} seconds",
                        startup_timeout.as_secs()
                    ));
                }
            }
        }
        // `probe_health` first and the temp-dir check only on a healthy verdict:
        // the latter shells out to inspect the listening process, which must not
        // run on every poll of this loop.
        if probe_health(spawner, probe, &client_env, HEALTH_ROUND_TRIP_DEADLINE)
            == ServiceHealth::Healthy
        {
            if probe
                .tcp
                .as_deref()
                .is_none_or(|addr| listener_temp_dir_live(control, addr))
            {
                if submitted_to_user_service {
                    // `child` is launchctl, not the daemon. A successful submit
                    // hands lifetime ownership to launchd, so there is no child
                    // handle for this process to retain.
                    return Ok(None);
                }
                if !matches!(child.try_wait(), Ok(Some(_))) {
                    return Ok(Some(child));
                }
                // Our child is gone, so whoever answered is another process,
                // and answering says nothing about what it was granted.
                match probe
                    .tcp
                    .as_deref()
                    .and_then(|addr| adoption_refusal(control, addr, cfg, templates, grant))
                {
                    None => return Ok(None),
                    Some(refusal) => {
                        log::info!("not adopting the build service's listener: {refusal}");
                    }
                }
            }
            // Something is serving but cannot do work, or does not hold a
            // grant that covers this runner. Do not adopt it; fall through to
            // the replacement path below.
            break;
        }
        if let Ok(Some(status)) = child.try_wait() {
            if launch_child_exit_ends_startup(submitted_to_user_service, status.success()) {
                if submitted_to_user_service {
                    return Err(format!(
                        "launchd submission exited before the service became healthy: {}",
                        describe_exit(&status)
                    ));
                }
                break;
            }
            // Exit 0 is launchd accepting the job, not the daemon exiting.
            // Keep polling the service instead of immediately launching a
            // second job that boots out the first while it is still starting.
            submission_completed = true;
        }
        if std::time::Instant::now() >= deadline {
            if submitted_to_user_service && submission_completed {
                return Err(format!(
                    "launchd accepted the service, but it did not prove a healthy round trip within {} seconds",
                    STARTUP_RECONCILE_TIMEOUT.as_secs()
                ));
            }
            break;
        }
        std::thread::sleep(HEALTH_POLL_INTERVAL);
    }

    let Some(addr) = probe.tcp.as_deref() else {
        return Err("build service exited before becoming healthy".to_string());
    };
    let expected = expected_service_executable(cfg, templates).ok_or_else(|| {
        "sccache port conflict: launch executable could not be resolved".to_string()
    })?;

    // A compatible server that won the startup race is safe to adopt, but only if
    // it is healthy in the full sense: an adopted daemon's environment is not ours
    // to choose, so a stray one holding a reclaimed temp dir would otherwise be
    // taken on as-is and fail every compile routed to it, and its sandbox grant
    // is fixed, so one granted for other roots would fail every compile under
    // ours (CAIRN-3355, cairn/5556). Its grant record is the only account of that
    // grant. Only an unhealthy listener whose executable is exactly the
    // configured service binary is eligible for termination; unrelated listeners
    // are never killed.
    let environment_mismatch = listener_environment_mismatch(control, addr, cfg, templates);
    let grant_refusal = if environment_mismatch.is_none()
        && assess_health(
            spawner,
            control,
            probe,
            &client_env,
            HEALTH_ROUND_TRIP_DEADLINE,
        ) == ServiceHealth::Healthy
    {
        match adoption_refusal(control, addr, cfg, templates, grant) {
            None => return Ok(None),
            Some(refusal) => Some(refusal),
        }
    } else {
        None
    };
    // Health assessment can outlive the endpoint it assessed. Re-check the real
    // socket at the decision boundary before treating this as a bind conflict:
    // otherwise a child that exits during its round trip becomes a phantom
    // listener which no later retry can replace.
    if !tcp_reachable(addr) {
        return launch_service(spawner, cfg, templates, grant.roots, deny_read).map(Some);
    }

    // Terminating the listener is the irreversible step, so it is the one the
    // caller's confidence gates. Refusing leaves a possibly-working shared cache
    // in place and reports why, which is strictly better than destroying it on a
    // verdict this process cannot yet vouch for.
    if !may_destroy {
        if let Some(mismatch) = environment_mismatch {
            return Err(format!(
                "refusing to adopt the reachable build-service daemon because {mismatch}; leaving it in place"
            ));
        }
        if let Some(refusal) = grant_refusal {
            return Err(format!(
                "refusing to adopt the reachable build-service daemon because its grant does \
                 not cover this runner ({refusal}); leaving it in place"
            ));
        }
        return Err(format!(
            "a TCP connection to {addr} succeeded at the termination decision, but this \
             runner's health probe has not earned the confidence to terminate that listener; \
             leaving it in place"
        ));
    }
    recover_listener_conflict(control, addr, &expected)?;
    let reap_deadline = std::time::Instant::now() + CHILD_REAP_TIMEOUT;
    while tcp_reachable(addr) && std::time::Instant::now() < reap_deadline {
        std::thread::sleep(HEALTH_POLL_INTERVAL);
    }
    launch_service(spawner, cfg, templates, grant.roots, deny_read).map(Some)
}

/// The build a client env is being composed for.
///
/// Which builds may be pointed at the supervised daemon is decided by where they
/// write, because the daemon runs each cache-miss compile itself: a build whose
/// `target/` the service sandbox does not cover fails outright rather than
/// missing the cache. The daemon is granted every managed root's `target/`
/// trees by construction ([`BuildServiceConfig::launch_grant`]), and a build is
/// routed by the managed root it falls under.
#[derive(Debug, Clone, Copy)]
enum ClientBuild<'a> {
    /// A spawn with a known build directory, admitted only when that directory
    /// lies inside a managed build root.
    At(&'a Path),
    /// A command in an executor cell on this machine. A cell lives under
    /// `{buildRoot}/build-slots` (the colocated build root, cairn/5286), so it
    /// is inside that managed root without a path test — which matters because
    /// the runner composing a cell request knows the cell's repository but not
    /// the absolute slot the executor will hand it. Its caller states the rest:
    /// not the project's live checkout, and placed on the colocated executor.
    Cell,
}

/// Which managed roots the compile cache may serve right now, as the
/// supervisor's runtime record says. See [`BuildServiceRuntimeDiagnostic::routing`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Routing {
    /// The daemon as a whole was found failing the work it accepts.
    pub(crate) daemon_unfit: bool,
    /// Canonical roots that are granted, proven, and not found unfit.
    pub(crate) roots: Vec<String>,
}

/// The client env every enabled service contributes to `build`, or nothing when
/// `build` may not be pointed at these daemons.
///
/// This decides only *whether* a build is told about the services, never *what*
/// they say: a service that claims its port against client auto-start declares
/// [`BUILD_SERVICE_CLIENT_ENV`] in its own env, because that claim is true of one
/// daemon rather than of build services in general.
///
/// The service env is always injected for an admitted build, so `RUSTC_WRAPPER`
/// never moves and cargo's fingerprints stay valid however routing changes.
/// What changes is [`BUILD_SERVICE_ROOTS_ENV`], the roots the wrapper may attach
/// for, and [`BUILD_SERVICE_UNFIT_ENV`], set when this build's own root may not
/// be served at all.
fn client_env_for(
    services: &HashMap<String, BuildServiceConfig>,
    templates: &Templates,
    build: ClientBuild<'_>,
    routing: &Routing,
) -> HashMap<String, String> {
    let root = match build {
        ClientBuild::At(build_dir) => match managed_root_containing(templates, build_dir) {
            Some(root) => root,
            None => return HashMap::new(),
        },
        ClientBuild::Cell => cell_build_root(templates),
    };
    let mut env = merge_client_env(services, templates);
    // Only meaningful beside a daemon this build was actually pointed at; an
    // empty merge means no service claimed this build and there is nothing to
    // withdraw it from.
    if env.is_empty() {
        return env;
    }
    env.insert(BUILD_SERVICE_ROOTS_ENV.to_string(), routing.roots.join(":"));
    let root = root.display().to_string();
    if routing.daemon_unfit || !routing.roots.contains(&root) {
        env.insert(BUILD_SERVICE_UNFIT_ENV.to_string(), "1".to_string());
    }
    env
}

/// Merge the expanded client env of every enabled service. Injected into spawns
/// that build inside a managed root so their tooling connects to the Cairn-owned
/// daemons.
fn merge_client_env(
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

/// Last health and restart observations recorded by the supervisor.
///
/// This is a lifecycle record, not a single latest verdict. The previous
/// version held only the most recent health word and restart time, which is why
/// a daemon could be relaunched thirty-three thousand times without anything
/// being able to say how often, since when, or with what result (CAIRN-3332).
/// Everything here is bounded. Ordinary observations are runtime-only; terminal
/// recovery exhaustion is persisted separately so a runner restart cannot reset
/// the launch budget and resume a machine-wide restart loop.
#[derive(Debug, Clone, Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BuildServiceRuntimeDiagnostic {
    pub(crate) last_health: Option<String>,
    pub(crate) last_checked_at: Option<i64>,
    pub(crate) last_restart_at: Option<i64>,
    pub(crate) last_restart_reason: Option<String>,
    /// The current supervisor-owned startup/recovery failure. Cleared as soon as
    /// the service is healthy, started, or adopted; unlike the persistent error
    /// log this is causal state suitable for check-failure classification.
    pub(crate) current_failure: Option<String>,
    /// Fingerprint of the service configuration that produced `current_failure`.
    /// A settings change invalidates the failure even before the next supervisor tick.
    pub(crate) failure_config: Option<String>,
    /// Which incarnation of the daemon the retained statistics describe. Counters
    /// reset to zero on restart, so a consumer that cannot see this move cannot
    /// tell a reset from a collapse in cache effectiveness.
    pub(crate) generation: u64,
    /// Launches this process has made since it started.
    pub(crate) restart_count: u64,
    /// Failed launches since the last healthy observation.
    pub(crate) consecutive_failures: u32,
    /// When the next bounded relaunch is due, while one is scheduled.
    pub(crate) next_attempt_unix_ms: Option<u64>,
    /// When the lifecycle state last changed, so "restarting" can be reported
    /// with how long it has been restarting.
    pub(crate) state_changed_at_unix_ms: u64,
    pub(crate) lifecycle: Option<String>,
    pub(crate) supervised_pid: Option<u32>,
    pub(crate) launched_at_unix_ms: Option<u64>,
    /// Identity of the supervisor that launched the current child.
    pub(crate) launcher_cairn_home: Option<String>,
    pub(crate) launcher_pid: Option<u32>,
    pub(crate) launcher_confined: Option<bool>,
    /// What the last health round trip did, in one bounded line.
    pub(crate) last_probe: Option<String>,
    /// How a supervised child was last observed to end, and when. Learned from
    /// the child handle rather than from the port going quiet, because only the
    /// handle can distinguish a signal from a refused start.
    pub(crate) last_exit: Option<String>,
    pub(crate) last_exit_at_unix_ms: Option<u64>,
    /// The most recent statistics sample and the true instant it was taken.
    pub(crate) stats: Option<CompileCacheStats>,
    pub(crate) stats_at_unix_ms: Option<u64>,
    /// Why there is no usable sample, when there is none.
    pub(crate) stats_gap: Option<String>,
    /// Why the daemon is currently judged unable to compile, when it is.
    ///
    /// Distinct from `current_failure`, which also carries launch and recovery
    /// failures: this one says specifically that a LISTENING daemon is
    /// destroying the compiles it accepts, which is the single condition that
    /// makes routing builds to it worse than not caching at all. Set only on
    /// positive evidence and cleared by health, so the absence of an observation
    /// never withdraws a build from a working cache.
    pub(crate) unfit: Option<String>,
    /// A reported corrupt artifact that quarantine could not positively remove.
    ///
    /// A generic capability compile uses a fresh nonce and therefore cannot prove
    /// that the offending cache key became readable. This runtime-only latch is
    /// not released merely because the bounded scanner removed some other empty
    /// entry: the reviewed mmap evidence does not name a cache key, so no removed
    /// path can be proven to be the artifact that failed.
    #[serde(skip)]
    pub(crate) unresolved_corruption: bool,
    /// Whether this process has ever seen the health round trip answer.
    ///
    /// Until it has, the probe is an untested instrument and its unhealthy
    /// verdicts are not evidence about the daemon. See
    /// [`BuildServiceRuntimeDiagnostic::may_destroy`].
    pub(crate) round_trip_proven: bool,
    /// The daemon incarnation — listener pid and kernel start token — that the
    /// grant and fitness fields below describe. A new incarnation clears every
    /// per-root verdict, so a fresh daemon routes nothing until each root is
    /// proven, and a running one is probed once rather than on every tick: a
    /// process's grant cannot change while it runs.
    pub(crate) incarnation: Option<(u32, String)>,
    /// This runner's managed roots, canonical, as of the last reconcile. Held
    /// here so the hot reads that render them never touch the filesystem.
    pub(crate) own_roots: Vec<String>,
    /// The roots the running incarnation's grant record covers.
    pub(crate) granted_roots: Vec<String>,
    /// Roots this incarnation has compiled into.
    pub(crate) proven_roots: Vec<String>,
    /// Roots this incarnation was proven unable to write, with the few words
    /// the status line carries. Never a reason to refuse a lane: clients
    /// building there compile directly.
    pub(crate) unfit_roots: BTreeMap<String, String>,
    /// When an inconclusive root may be probed again.
    pub(crate) inconclusive_retry_at: BTreeMap<String, u64>,
    #[serde(skip)]
    pub(crate) inconclusive_attempts: BTreeMap<String, u32>,
    /// A verdict about the incarnation as a whole — it wrote a cache entry and
    /// could not serve it back — kept so it holds without being re-probed.
    pub(crate) incarnation_unfit: Option<String>,
    /// Why the running daemon's grant does not cover this runner, while it
    /// does not. Its own outcome, never `current_failure`: the daemon answers,
    /// and is only unfit for the roots it was not granted.
    pub(crate) grant_stale: Option<GrantStaleNote>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GrantStaleNote {
    pub(crate) reason: String,
    /// Whether this runner will replace the daemon, as opposed to leaving it
    /// running and compiling the uncovered roots directly.
    pub(crate) relaunching: bool,
}

impl BuildServiceRuntimeDiagnostic {
    /// Note which daemon incarnation is listening and what its record grants,
    /// forgetting every per-root verdict when the incarnation is a new one.
    fn observe_incarnation(&mut self, incarnation: Option<(u32, String)>, granted: &[String]) {
        if self.incarnation != incarnation {
            self.incarnation = incarnation;
            self.proven_roots.clear();
            self.unfit_roots.clear();
            self.inconclusive_retry_at.clear();
            self.inconclusive_attempts.clear();
            self.incarnation_unfit = None;
        }
        self.granted_roots = granted.to_vec();
    }

    /// This runner's granted roots with no verdict for the current
    /// incarnation, less those waiting out an inconclusive probe's backoff.
    fn roots_awaiting_probe(&self, now_ms: u64) -> Vec<String> {
        if self.incarnation.is_none() || self.incarnation_unfit.is_some() {
            return Vec::new();
        }
        self.own_roots
            .iter()
            .filter(|root| self.granted_roots.contains(root))
            .filter(|root| !self.proven_roots.contains(root))
            .filter(|root| !self.unfit_roots.contains_key(*root))
            .filter(|root| {
                self.inconclusive_retry_at
                    .get(*root)
                    .is_none_or(|at| now_ms >= *at)
            })
            .cloned()
            .collect()
    }

    fn apply_fitness(&mut self, report: FitnessReport, now_ms: u64) {
        for (root, fitness) in report.roots {
            match fitness {
                RootFitness::Proven => {
                    self.inconclusive_retry_at.remove(&root);
                    self.inconclusive_attempts.remove(&root);
                    if !self.proven_roots.contains(&root) {
                        self.proven_roots.push(root);
                    }
                }
                RootFitness::Unfit { summary, .. } => {
                    self.inconclusive_retry_at.remove(&root);
                    self.inconclusive_attempts.remove(&root);
                    self.unfit_roots.insert(root, summary);
                }
                RootFitness::Inconclusive(_) => {
                    let attempts = self.inconclusive_attempts.entry(root.clone()).or_default();
                    *attempts = attempts.saturating_add(1);
                    let wait = probe_retry_after(*attempts).as_millis() as u64;
                    self.inconclusive_retry_at.insert(root, now_ms + wait);
                }
            }
        }
        if report.daemon_unfit.is_some() {
            self.incarnation_unfit = report.daemon_unfit;
        }
    }

    /// The roots a build may be routed to the daemon from right now.
    fn routable_roots(&self) -> Vec<String> {
        if self.unfit.is_some() || self.incarnation_unfit.is_some() || self.incarnation.is_none() {
            return Vec::new();
        }
        self.proven_roots
            .iter()
            .filter(|root| self.granted_roots.contains(root))
            .filter(|root| !self.unfit_roots.contains_key(*root))
            .cloned()
            .collect()
    }

    /// What a build is told about the daemon. Read from the same record the
    /// panel reads, so what a build is told and what an operator sees can
    /// never disagree.
    pub(crate) fn routing(&self) -> Routing {
        Routing {
            daemon_unfit: self.unfit.is_some() || self.incarnation_unfit.is_some(),
            roots: self.routable_roots(),
        }
    }

    /// One sentence for each of this runner's roots the daemon may not serve,
    /// or for the daemon as a whole. Assembled from the record alone: nothing
    /// is probed to render it.
    pub(crate) fn fitness(&self) -> Vec<String> {
        if let Some(reason) = self.unfit.as_ref().or(self.incarnation_unfit.as_ref()) {
            return vec![format!(
                "unfit for every managed root ({}); clients compile directly",
                bounded_tail(reason)
            )];
        }
        if self.incarnation.is_none() && self.grant_stale.is_none() {
            return Vec::new();
        }
        let routable = self.routable_roots();
        self.own_roots
            .iter()
            .filter(|root| !routable.contains(root))
            .map(|root| {
                if let Some(reason) = self.unfit_roots.get(root) {
                    return format!("unfit for {root} ({reason}); clients compile directly");
                }
                if !self.granted_roots.contains(root) {
                    let next = match &self.grant_stale {
                        Some(note) if note.relaunching => "relaunch pending",
                        _ => "clients compile directly",
                    };
                    return format!("{root} is not in the running daemon's grant; {next}");
                }
                if self.inconclusive_retry_at.contains_key(root) {
                    return format!(
                        "fitness for {root} is unproven (the last probe was inconclusive); \
                         clients compile directly"
                    );
                }
                format!("fitness for {root} is not yet proven; clients compile directly")
            })
            .collect()
    }

    /// Whether an unhealthy verdict is trustworthy enough to destroy a daemon
    /// over. See [`DESTRUCTIVE_ATTEMPT_LIMIT`] for why this is bounded at all.
    ///
    /// Non-destructive recovery — launching into a port nothing holds — is
    /// always allowed and is not gated by this. What this gates is killing
    /// something that is currently listening.
    fn may_destroy(&self) -> bool {
        let limit = if self.round_trip_proven {
            DESTRUCTIVE_ATTEMPT_LIMIT
        } else {
            1
        };
        self.consecutive_failures < limit
    }

    /// Keep a stronger unresolved-corruption verdict across a generic healthy
    /// probe, which compiles a different nonce and cannot verify the failing key.
    fn clear_unfit_after_healthy_probe(&mut self, now_ms: u64) -> bool {
        if !self.unresolved_corruption {
            self.unfit = None;
            return true;
        }
        let reason = self
            .unfit
            .clone()
            .unwrap_or_else(|| "compile-cache corruption remains unresolved".to_string());
        self.last_health = Some("wedged".to_string());
        self.last_probe = Some(reason.clone());
        self.stats_gap = Some(reason.clone());
        self.current_failure = Some(reason);
        self.enter("degraded", now_ms);
        false
    }

    /// Move to a lifecycle state, dating the transition only when it is one.
    ///
    /// Re-dating an unchanged state on every tick would make a service that has
    /// been restarting for an hour report that it just started restarting.
    fn enter(&mut self, lifecycle: &str, now_ms: u64) {
        if self.lifecycle.as_deref() != Some(lifecycle) {
            self.lifecycle = Some(lifecycle.to_string());
            self.state_changed_at_unix_ms = now_ms;
        }
    }
}

/// Read-only build-service state captured at an infrastructure failure boundary.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BuildServiceDiagnosticSnapshot {
    pub(crate) name: String,
    pub(crate) configured: bool,
    pub(crate) enabled: bool,
    /// Whether this process retained a child handle after spawning the service.
    /// A healthy daemon adopted from an earlier process reports `false` here.
    pub(crate) supervised_child: bool,
    pub(crate) config_fingerprint: Option<String>,
    pub(crate) state_dir: Option<String>,
    pub(crate) error_log_tail: Option<String>,
    pub(crate) runtime: BuildServiceRuntimeDiagnostic,
}

impl BuildServiceDiagnosticSnapshot {
    pub(crate) fn current_failure(&self) -> Option<&str> {
        (self.enabled && self.runtime.failure_config == self.config_fingerprint)
            .then_some(self.runtime.current_failure.as_deref())
            .flatten()
    }

    /// The agent-facing half of this snapshot: one composed sentence, offered
    /// only when the service is itself unhealthy. Whether it is RELEVANT to a
    /// given failure is a separate question the caller answers — see
    /// `execution::checks::with_build_service_advisory`.
    ///
    /// An agent cannot restart a daemon and has nothing to do with its config
    /// fingerprint, state dir, or supervision shape. That detail is serialized
    /// whole into the operator's infrastructure-failure log record, which is
    /// where it belongs.
    pub(crate) fn agent_advisory(&self) -> Option<String> {
        if !self.enabled {
            return None;
        }
        let unhealthy = self.current_failure().is_some()
            || matches!(
                self.runtime.last_health.as_deref(),
                Some("wedged") | Some("down")
            );
        unhealthy.then(|| {
            format!(
                "Cairn's shared build-cache service ({}) is unhealthy right now, which commonly \
                 causes failures of this kind. This is Cairn's infrastructure, not your change; \
                 the details are in Cairn's operator log.",
                self.name
            )
        })
    }
}

/// Runtime status of one build service, for the settings UI.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BuildServiceStatus {
    pub(crate) name: String,
    /// Whether the service is enabled in settings.
    pub(crate) enabled: bool,
    /// Whether the launch program resolves on PATH (or is an absolute path).
    pub(crate) installed: bool,
    /// Whether the full health probe currently reports the daemon healthy (live
    /// and answering a round-trip) — what the supervisor's recovery path sees, so
    /// a wedged-but-listening daemon reads as not reachable.
    pub(crate) reachable: bool,
    /// The launch argv, templates expanded (for display).
    start: Vec<String>,
    /// The cross-worktree writable globs, templates expanded (the grant).
    write: Vec<String>,
    /// The daemon's state dir, templates expanded.
    state_dir: Option<String>,
    /// Sorted client-env keys this service injects (values omitted).
    env_keys: Vec<String>,
    /// The raw, template-unexpanded config — the editable source of truth the
    /// settings UI binds its form to (so edits round-trip `{worktrees}` etc.).
    config: BuildServiceConfig,
    /// One sentence per managed root the daemon may not serve, from the
    /// supervisor's record ([`BuildServiceRuntimeDiagnostic::fitness`]).
    pub(crate) fitness: Vec<String>,
}

/// An advisory, machine-wide reconciliation lock for one service.
///
/// Serializing inside a process is not enough, and the deployment that proves it
/// is the ordinary one: this development machine runs the installed app beside
/// three dev-instance runners, and every one supervises the SAME daemon on the
/// same port, because a service's port and cache directory derive from `{home}`
/// rather than from a runner's own Cairn home. Four supervisors that can each
/// independently decide to terminate and relaunch one listener are not four
/// chances to recover; they are a race whose outcome is competing daemons on one
/// port. In-process serialization cannot see any of it.
///
/// `flock` on a file beside the daemon's own state gives single-owner
/// reconciliation across processes, and the kernel releases it when the holder
/// exits, so a crashed runner cannot wedge the machine. Advisory and
/// best-effort: on a host where the lock cannot be taken at all, reconciliation
/// still proceeds, because a supervisor that refuses to run is worse than one
/// that races.
struct MachineReconcileLock {
    /// Held for its descriptor, and unlocked explicitly on drop.
    file: std::fs::File,
}

/// Release the lock explicitly rather than by closing the file.
///
/// Closing is *usually* enough, and that is the trap. An `flock` lock lives on
/// the open file description, and `fork` duplicates every descriptor into the
/// child — so from the moment any thread in this process forks until that child
/// reaches `exec`, a copy of this description exists that `close` does not
/// dispose of. A runner that drops the lock inside that window has not released
/// it: the next `flock` attempt reads `HeldElsewhere` from a lock whose owner is
/// already gone.
///
/// The window is microseconds, so it presents as a rare flake rather than a
/// clear failure — measured here as roughly two in three runs of this module's
/// tests under contention, against a supervisor that spawns processes constantly.
/// `LOCK_UN` releases the lock on the description itself, which every copy
/// shares, so release stops depending on how many children happen to be between
/// `fork` and `exec`.
impl Drop for MachineReconcileLock {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            // SAFETY: `self.file` owns the descriptor for the duration of the
            // call, and `flock` only reads it.
            unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) };
        }
    }
}

/// Where a service's machine-wide lock lives: beside its own state, which is a
/// machine-stable location every runner resolves identically. A service with no
/// state directory falls back to one keyed by name under the user's home, for
/// the same reason.
fn machine_lock_path(cfg: &BuildServiceConfig, templates: &Templates, name: &str) -> PathBuf {
    match cfg.expanded_state_dir(templates) {
        Some(dir) => dir.join(".cairn-reconcile.lock"),
        None => templates
            .home
            .join(".cache")
            .join(format!("cairn-build-service-{name}.lock")),
    }
}

/// Who, if anyone, owns reconciliation of a service right now.
///
/// Three outcomes, not two, because "another runner is doing it" and "this host
/// cannot use lock files" call for opposite responses. Collapsing them into one
/// absent value makes an unusable lock silently switch supervision off — which
/// is precisely the failure mode this whole change exists to end.
enum ReconcileOwnership {
    /// This process holds the machine-wide lock and should reconcile.
    Owned(MachineReconcileLock),
    /// Another process is reconciling this service. Skip; it needs no help.
    HeldElsewhere,
    /// The lock could not be used at all here. Reconcile anyway, unserialized:
    /// a supervisor that refuses to run is worse than one that races.
    Unavailable,
}

/// Try to take the machine-wide lock for a service.
fn lock_service_machine_wide(path: &Path) -> ReconcileOwnership {
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let file = match std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)
    {
        Ok(file) => file,
        Err(e) => {
            log::debug!("build service reconcile lock {path:?} is unusable: {e}");
            return ReconcileOwnership::Unavailable;
        }
    };
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        // SAFETY: `file` owns the descriptor for the duration of the call, and
        // `flock` only reads it. `LOCK_NB` makes this a try-lock that never
        // blocks the supervisor thread.
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return ReconcileOwnership::HeldElsewhere;
        }
    }
    ReconcileOwnership::Owned(MachineReconcileLock { file })
}

/// Whether a new cache observation is news worth invalidating the panel for.
///
/// Appearing and disappearing are both news; so is any material difference. A
/// fresher measurement time over identical counters is not, because the
/// supervisor re-samples on every tick and treating that as a change would turn
/// an idle machine into a permanent event source.
fn cache_change_is_news(
    previous: Option<&CompileCacheHealth>,
    current: Option<&CompileCacheHealth>,
) -> bool {
    match (previous, current) {
        (None, None) => false,
        (Some(previous), Some(current)) => previous.materially_differs(current),
        _ => true,
    }
}

/// Which configured service is THE compile cache for this machine.
///
/// One service, not a merge: the compile cache is a singular thing an operator
/// reads a single state for, and a panel that averaged two of them would report
/// neither. `sccache` by name when present (it is the built-in default), and
/// otherwise the first by name so a differently-named single service is still
/// reported rather than silently absent.
fn quarantine_zero_length_cache_entries(state_dir: &Path) -> Vec<PathBuf> {
    let mut removed = Vec::new();
    let mut pending = vec![(state_dir.to_path_buf(), 0usize)];
    let mut visited = 0usize;
    while let Some((dir, depth)) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(dir) else {
            continue;
        };
        for entry in entries.flatten() {
            if visited >= CACHE_QUARANTINE_FILE_LIMIT {
                return removed;
            }
            visited += 1;
            let path = entry.path();
            let Ok(kind) = entry.file_type() else {
                continue;
            };
            if kind.is_dir() && depth < 8 {
                pending.push((path, depth + 1));
            } else if kind.is_file()
                && depth >= 2
                && entry.metadata().is_ok_and(|metadata| metadata.len() == 0)
                && std::fs::remove_file(&path).is_ok()
            {
                log::warn!(
                    "quarantined zero-length compile-cache entry {}",
                    path.display()
                );
                removed.push(path);
            }
        }
    }
    removed
}

/// One service's grant site for a reconcile: where its record and registry
/// live, this runner's own managed roots, and the roots a launch is granted.
struct GrantSite {
    dir: PathBuf,
    own_roots: Vec<String>,
    launch_roots: Vec<PathBuf>,
}

/// Ensure the daemon's state dir exists before launch: sccache creates its
/// SCCACHE_ERROR_LOG file (under stateDir) before starting the server, and a
/// missing parent dir would fail that and take the whole server down on a
/// fresh machine.
fn ensure_state_dir(cfg: &BuildServiceConfig, templates: &Templates) {
    if let Some(dir) = cfg.expanded_state_dir(templates) {
        if let Err(e) = std::fs::create_dir_all(&dir) {
            log::debug!("create build service state dir {dir:?}: {e}");
        }
    }
}

/// Keep the statistics a healthy round trip carried. One round trip serves
/// both purposes — the health verdict and the counters — and the UI never runs
/// its own.
fn record_stats_sample(
    state: &mut BuildServiceRuntimeDiagnostic,
    observation: &ServiceObservation,
    now_ms: u64,
) {
    match observation.round_trip.as_ref() {
        Some(round_trip) => match parse_cache_stats(&round_trip.stdout) {
            Some(stats) => {
                state.stats = Some(stats);
                state.stats_at_unix_ms = Some(now_ms);
                state.stats_gap = None;
            }
            None => {
                state.stats_gap = Some(
                    "the health round trip answered with no readable statistics report".to_string(),
                );
            }
        },
        None => {
            state.stats_gap =
                Some("this service has no statistics round trip configured".to_string())
        }
    }
}

fn compile_cache_service(
    services: &HashMap<String, BuildServiceConfig>,
) -> Option<(String, BuildServiceConfig)> {
    let mut names: Vec<&String> = services.keys().collect();
    names.sort();
    let chosen = names
        .iter()
        .find(|name| name.as_str() == "sccache")
        .or(names.first())?;
    services
        .get(*chosen)
        .map(|cfg| ((*chosen).clone(), cfg.clone()))
}

/// Whether the service's launch program resolves (on PATH or an absolute path).
/// The built-in default sccache entry uses this to stay inert unless `sccache`
/// is actually installed.
///
/// Resolving a bare program name runs `which` as a subprocess, so this belongs
/// to reconciliation and to the settings screen — the two callers that are
/// deliberately on demand — and never to a read that renders a panel. What a
/// panel needs is the answer reconciliation already found; see
/// [`ObservedCompileCacheService`].
fn service_on_path(cfg: &BuildServiceConfig) -> bool {
    match cfg.start.first() {
        Some(prog) => Path::new(prog).is_absolute() || crate::env::find_binary(prog).is_ok(),
        None => false,
    }
}

/// What reconciliation last found out about this machine's compile-cache
/// service, as distinct from what the supervisor has since observed the daemon
/// *doing* (which lives in [`BuildServiceRuntimeDiagnostic`]).
///
/// These are configuration facts, and establishing them costs a settings-file
/// parse and a PATH-resolution subprocess. They change when a person edits
/// settings or installs a binary, never on the cadence a substrate-health read
/// happens at, so reconciliation establishes them on every pass and the read
/// path consumes what it published. Reconciliation runs at startup, on every
/// settings write, and on the supervisor's fifteen-second tick — the same
/// cadence the cache statistics themselves are sampled at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ObservedCompileCacheService {
    pub(crate) name: String,
    pub(crate) enabled: bool,
    /// Whether the launch program resolved, by [`service_on_path`].
    pub(crate) installed: bool,
}

/// Read the compile-cache service out of a loaded settings map.
///
/// Free-standing and taking the map rather than the config dir, so the caller
/// that has already loaded settings does not load them twice and so the
/// resolution rule can be tested without a filesystem.
fn observe_compile_cache_service(
    services: &HashMap<String, BuildServiceConfig>,
) -> Option<ObservedCompileCacheService> {
    compile_cache_service(services).map(|(name, cfg)| ObservedCompileCacheService {
        name,
        enabled: cfg.enabled,
        installed: service_on_path(&cfg),
    })
}

impl Orchestrator {
    fn build_service_templates(&self) -> Templates {
        settings::build_service_templates(&self.config_dir, None)
    }

    pub(crate) fn report_compile_cache_corruption(&self, evidence: &str) -> Vec<PathBuf> {
        let services = settings::load_build_services(&self.config_dir);
        let Some((name, cfg)) = compile_cache_service(&services) else {
            return Vec::new();
        };
        let templates = self.build_service_templates();
        let removed = cfg
            .expanded_state_dir(&templates)
            .map(|dir| quarantine_zero_length_cache_entries(&dir))
            .unwrap_or_default();
        let reason = bounded_tail(&format!("compile-cache corruption reported: {evidence}"));
        let mut diagnostics = self.build_service_runtime.lock().unwrap();
        let state = diagnostics.entry(name).or_default();
        state.last_health = Some("wedged".to_string());
        state.last_probe = Some(reason.clone());
        state.current_failure = Some(reason.clone());
        state.failure_config = Some(service_config_fingerprint(&cfg));
        state.stats_gap = Some(reason.clone());
        state.unfit = Some(reason);
        // The mmap signature identifies the failure mode, not the cache key.
        // Quarantining zero-length entries is useful cleanup, but removing any
        // particular entry cannot prove it was the one this compile attempted to
        // read. Keep routing uncached until stronger evidence (or a fresh runner
        // lifecycle) resets this runtime-only ruling.
        state.unresolved_corruption = true;
        state.enter("degraded", unix_ms());
        removed
    }

    /// Enabled services whose launch program is installed.
    fn launchable_services(
        services: HashMap<String, BuildServiceConfig>,
    ) -> Vec<(String, BuildServiceConfig)> {
        services
            .into_iter()
            .filter(|(_, c)| c.enabled && service_on_path(c))
            .collect()
    }

    /// Publish what settings and PATH currently say about the compile-cache
    /// service, for readers that must not go and find out for themselves.
    /// See [`ObservedCompileCacheService`].
    fn publish_compile_cache_service(&self, services: &HashMap<String, BuildServiceConfig>) {
        let observation = observe_compile_cache_service(services);
        if let Ok(mut published) = self.compile_cache_service.write() {
            *published = observation;
        }
    }

    /// The compile-cache service as reconciliation last observed it.
    ///
    /// A read never establishes this itself, and the absence of an observation
    /// is reported as an absent compile cache rather than repaired on the spot.
    /// That is the whole point: establishing it parses settings and resolves the
    /// launch program on PATH, which shells out, and the substrate-health
    /// snapshot that consumes it is rebuilt on every fleet change. A lazy repair
    /// here would make the invariant "a read does no I/O" true only after some
    /// unspecified earlier call, which is not an invariant at all.
    ///
    /// Nothing real reports an absent cache for long: every runner and non-inert
    /// server reconciles during startup, so this is the boot gap rather than a
    /// steady state. A host that genuinely never reconciles also never launches a
    /// build service, and reporting no compile cache is the honest answer there.
    pub(crate) fn observed_compile_cache_service(&self) -> Option<ObservedCompileCacheService> {
        self.compile_cache_service
            .read()
            .ok()
            .and_then(|published| published.clone())
    }

    /// Startup entry point: bring every enabled, installed user-configured build
    /// service to a healthy state via
    /// [`Self::ensure_build_services_ready`]. Best-effort throughout — failures
    /// log and are never fatal, because the client wrapper falls back to a plain
    /// compiler when the daemon is unreachable.
    pub fn start_build_services(&self) {
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
        // Republish first, ahead of every early return below. What is configured
        // and installed is true regardless of whether this host can confine a
        // service or whether another reconcile currently holds the lock, and the
        // panel that reports it must not be made stale by either.
        let services = settings::load_build_services(&self.config_dir);
        self.publish_compile_cache_service(&services);
        self.deliver_cache_wrapper(&services);
        if !sandbox::is_available() {
            // No service sandbox on this host; clients run uncached (the
            // cache-wrapper guard never auto-starts a confined server).
            return;
        }
        // Serialize reconciliation. A settings-triggered restart, the periodic
        // tick, and a startup launch can all arrive at once, and three of them
        // racing on one port produce competing daemons and a restart storm
        // rather than a recovery. `try_lock` rather than `lock`: a reconcile
        // already running has just done this work, so a second caller has
        // nothing to add by waiting for a turn.
        let Ok(_reconciling) = self.build_service_reconcile.try_lock() else {
            log::debug!("build service reconcile already in flight; skipping");
            return;
        };
        let templates = self.build_service_templates();
        let deny_read = self.sandbox_deny_read();
        let own_roots = root_strings(&managed_build_roots(&templates));
        for (name, cfg) in Self::launchable_services(services) {
            // Then serialize across processes. Without this the in-process lock
            // above only stops one runner from racing itself, while the four
            // runners this machine actually runs race each other for the same
            // port. Held for the whole reconcile, released on drop.
            let _owned = match lock_service_machine_wide(&machine_lock_path(
                &cfg, &templates, &name,
            )) {
                ReconcileOwnership::HeldElsewhere => {
                    log::debug!("build service '{name}' is being reconciled by another runner");
                    continue;
                }
                ReconcileOwnership::Owned(lock) => Some(lock),
                ReconcileOwnership::Unavailable => {
                    log::warn!(
                            "build service '{name}' reconcile lock is unavailable; refusing to launch without machine-wide ownership"
                        );
                    continue;
                }
            };
            self.reconcile_registered_service(
                &OsListenerProcessControl,
                &name,
                &cfg,
                &templates,
                &own_roots,
                deny_read.clone(),
                may_launch_service(&cfg, &templates, sandbox::current_process_is_confined()),
            );
        }
    }

    /// Install this runner's wrapper at `{cairnHome}/bin/cache-wrapper.sh` when
    /// an enabled service routes builds through that path. Only this runner's
    /// own home is ever written; a wrapper configured anywhere else is the
    /// operator's, and routing through it waits until it carries the gate.
    fn deliver_cache_wrapper(&self, services: &HashMap<String, BuildServiceConfig>) {
        let templates = self.build_service_templates();
        let path = installed_wrapper_path(&templates);
        let named = services.values().filter(|cfg| cfg.enabled).any(|cfg| {
            let env = cfg.expanded_env(&templates);
            WRAPPER_ENV_KEYS
                .iter()
                .any(|key| env.get(*key).is_some_and(|value| Path::new(value) == path))
        });
        if !named {
            return;
        }
        match install_cache_wrapper(&path) {
            Ok(true) => log::info!("installed the cache wrapper at {}", path.display()),
            Ok(false) => {}
            Err(e) => log::warn!("could not install the cache wrapper: {e}"),
        }
    }

    /// Register this runner's managed roots beside the daemon, then drive the
    /// service one step. Runs under the machine-wide reconcile lock, which is
    /// what keeps the registry and the grant record consistent across runners.
    #[allow(clippy::too_many_arguments)]
    fn reconcile_registered_service(
        &self,
        control: &dyn ListenerProcessControl,
        name: &str,
        cfg: &BuildServiceConfig,
        templates: &Templates,
        own_roots: &[String],
        deny_read: Vec<PathBuf>,
        may_launch: bool,
    ) {
        let dir = grant_state_dir(cfg, templates, name);
        if let Err(e) = register_runner_roots(&dir, templates, own_roots) {
            log::warn!(
                "build service '{name}' could not register this runner's managed roots: {e}"
            );
        }
        let launch_roots = live_registered_roots(&dir, own_roots)
            .into_iter()
            .map(PathBuf::from)
            .collect();
        let site = GrantSite {
            dir,
            own_roots: own_roots.to_vec(),
            launch_roots,
        };
        self.reconcile_build_service(control, name, cfg, templates, &site, deny_read, may_launch);
    }

    /// Drive one service one step toward health.
    ///
    /// The states are explicit because "unhealthy" is three different
    /// situations: a relaunch is scheduled and pending (`restarting`), the
    /// daemon is unreachable and this process cannot currently fix it
    /// (`degraded`), or launches have failed past the backoff ceiling
    /// (`recoveryFailed`). Only the last is a condition an operator must see.
    /// At that boundary active launch attempts stop, while health observation
    /// continues so an externally restored service is adopted immediately.
    ///
    /// A healthy daemon whose grant does not cover this runner is a fourth
    /// situation with its own branch ([`Self::reconcile_stale_grant`]), because
    /// it must never reach `current_failure`: that field refuses check dispatch,
    /// and a daemon that answers is only unfit for the roots it was not granted.
    #[allow(clippy::too_many_arguments)]
    fn reconcile_build_service(
        &self,
        control: &dyn ListenerProcessControl,
        name: &str,
        cfg: &BuildServiceConfig,
        templates: &Templates,
        site: &GrantSite,
        deny_read: Vec<PathBuf>,
        may_launch: bool,
    ) {
        let now_ms = unix_ms();
        let may_destroy;
        let condemned;
        {
            let mut diagnostics = self.build_service_runtime.lock().unwrap();
            let state = diagnostics.entry(name.to_string()).or_default();
            state.own_roots = site.own_roots.clone();
            if reset_recovery_for_changed_config(state, cfg, templates, now_ms) {
                log::info!(
                    "build service '{name}' configuration changed; resetting exhausted recovery budget"
                );
            }
            if state.consecutive_failures == 0 {
                if let Some(persisted) = read_recovery_failure(cfg, templates) {
                    state.consecutive_failures = persisted.consecutive_failures;
                    state.current_failure = Some(persisted.diagnosis);
                    state.failure_config = Some(persisted.config_fingerprint);
                    state.next_attempt_unix_ms = None;
                    state.enter("recoveryFailed", now_ms);
                }
            }
        }
        // Reap first: a child that exited on its own is the only place the HOW
        // of a death survives. A probe can only report that the port went quiet.
        if let Some(exit) = self.observe_child_exit(name) {
            log::warn!("build service '{name}' {exit}");
            let mut diagnostics = self.build_service_runtime.lock().unwrap();
            let state = diagnostics.entry(name.to_string()).or_default();
            state.last_exit = Some(exit);
            state.last_exit_at_unix_ms = Some(now_ms);
            state.supervised_pid = None;
        }

        let client_env = cfg.expanded_env(templates);
        let supervised = self
            .build_service_children
            .lock()
            .unwrap()
            .contains_key(name);
        let mut observation = match &cfg.ready {
            Some(probe) => assess_service(
                self.services.process.as_ref(),
                control,
                probe,
                &client_env,
                HEALTH_ROUND_TRIP_DEADLINE,
                Some(GrantContext {
                    cfg,
                    templates,
                    dir: &site.dir,
                }),
            ),
            // No probe to assess health: treat a service we already supervise as
            // fine, and one we don't as needing a launch.
            None => ServiceObservation {
                health: if supervised {
                    ServiceHealth::Healthy
                } else {
                    ServiceHealth::Down
                },
                round_trip: None,
                unfit: None,
                grant: None,
            },
        };
        match observation.grant.take() {
            Some(GrantVerdict::Stale(stale)) if observation.health == ServiceHealth::Healthy => {
                self.reconcile_stale_grant(
                    control,
                    name,
                    cfg,
                    templates,
                    site,
                    deny_read,
                    may_launch,
                    &observation,
                    stale,
                    now_ms,
                );
                return;
            }
            Some(GrantVerdict::Covered(record)) => {
                if let Some(reason) = self.prove_incarnation_fitness(
                    name,
                    cfg,
                    templates,
                    &client_env,
                    &record,
                    now_ms,
                ) {
                    // Liveness is not fitness: an incarnation proven unable to
                    // serve is not healthy, whatever its round trip says.
                    observation.health = ServiceHealth::Wedged;
                    observation.unfit = Some(format!("listener pid {}: {reason}", record.pid));
                }
            }
            _ => {}
        }
        let health_name = match observation.health {
            ServiceHealth::Healthy => "healthy",
            ServiceHealth::Wedged => "wedged",
            ServiceHealth::Down => "down",
        };

        {
            let mut diagnostics = self.build_service_runtime.lock().unwrap();
            let state = diagnostics.entry(name.to_string()).or_default();
            state.last_health = Some(health_name.to_string());
            state.last_checked_at = Some(chrono::Utc::now().timestamp());
            state.last_probe = observation
                .unfit
                .clone()
                .or_else(|| observation.round_trip.as_ref().map(RoundTrip::summary));
            if observation.health == ServiceHealth::Healthy {
                record_stats_sample(state, &observation, now_ms);
                state.consecutive_failures = 0;
                state.next_attempt_unix_ms = None;
                state.grant_stale = None;
                // Healthy says the daemon works in general, not that a
                // previously failing cache key was removed, so retain the
                // stronger corruption diagnosis.
                if !state.clear_unfit_after_healthy_probe(now_ms) {
                    log::warn!(
                        "build service '{name}' answered a generic health probe, but reported cache corruption remains unresolved"
                    );
                    return;
                }
                state.current_failure = None;
                state.failure_config = None;
                clear_recovery_failure(cfg, templates);
                if state.generation == 0 {
                    // A daemon this process did not launch is still an
                    // incarnation, and the counters it reports belong to it.
                    state.generation = 1;
                }
                // Health is the ONLY thing that clears the attempt streak, and
                // the only thing that proves the probe works. A launch that
                // succeeds and then dies before the next tick must not reset
                // either, or a service can churn forever without ever reaching
                // backoff or `recoveryFailed`.
                state.round_trip_proven |= observation
                    .round_trip
                    .as_ref()
                    .is_some_and(RoundTrip::healthy);
                state.enter("healthy", now_ms);
                log::debug!("build service '{name}' healthy; not relaunching");
                return;
            }
            // Unhealthy: whatever counters we hold describe a daemon that is no
            // longer answering, so they are explicitly stale rather than current.
            // A FUNCTIONAL verdict is the exception worth stating precisely: the
            // counters are not merely stale there, they are the evidence, and
            // "the compile cache is wedged" would replace the one sentence that
            // explains the verdict with the word it produced.
            state.stats_gap = Some(match observation.unfit.as_deref() {
                Some(reason) => reason.to_string(),
                None => format!("the compile cache is {health_name}"),
            });
            // Recorded before every early return below, so a daemon waiting out
            // its backoff still says why it is being restarted. This is also
            // what puts the condition in front of an AGENT whose build it broke
            // (see `BuildServiceDiagnosticSnapshot::agent_advisory`).
            if observation.health == ServiceHealth::Wedged {
                let reason = observation
                    .unfit
                    .as_deref()
                    .unwrap_or("the reachable daemon is wedged and cannot safely accept builds");
                log::warn!("build service '{name}' is unfit: {reason}");
                state.current_failure = Some(bounded_tail(reason));
                state.failure_config = Some(service_config_fingerprint(cfg));
                state.unfit = Some(bounded_tail(reason));
            }
            if recovery_exhausted(state.consecutive_failures)
                && state.next_attempt_unix_ms.is_none_or(|next| now_ms >= next)
            {
                if state.next_attempt_unix_ms.is_some() {
                    let reason = format!(
                        "the service remained {health_name} after {} consecutive launch attempts; automatic recovery is stopped while passive health observation continues",
                        state.consecutive_failures
                    );
                    log::error!("build service '{name}' recovery stopped: {reason}");
                    state.current_failure = Some(reason.clone());
                    state.failure_config = Some(service_config_fingerprint(cfg));
                    if let Err(error) =
                        write_recovery_failure(cfg, templates, state.consecutive_failures, &reason)
                    {
                        log::error!(
                            "build service '{name}' could not persist recovery exhaustion: {error}"
                        );
                        state.current_failure = Some(format!(
                            "{reason}; additionally failed to persist this diagnosis: {error}"
                        ));
                    }
                    state.next_attempt_unix_ms = None;
                }
                state.enter("recoveryFailed", now_ms);
                return;
            }
            if let Some(next) = state.next_attempt_unix_ms {
                if now_ms < next {
                    let lifecycle = if state.consecutive_failures >= RECOVERY_FAILED_AFTER {
                        "recoveryFailed"
                    } else {
                        "restarting"
                    };
                    state.enter(lifecycle, now_ms);
                    log::debug!(
                        "build service '{name}' {health_name}; next launch attempt in {} ms",
                        next.saturating_sub(now_ms)
                    );
                    return;
                }
            }
            if !may_launch {
                log::warn!(
                    "build service '{name}' is {health_name}, but runner pid {} under {} is confined; refusing machine-wide daemon recovery or launch",
                    std::process::id(),
                    templates.cairn_home.display()
                );
                state.current_failure = Some(
                    "this runner is confined and may not launch the machine-wide build service"
                        .to_string(),
                );
                state.failure_config = Some(service_config_fingerprint(cfg));
                state.enter("degraded", now_ms);
                return;
            }
            state.enter("degraded", now_ms);
            state.last_restart_at = Some(chrono::Utc::now().timestamp());
            state.last_restart_reason = Some(health_name.to_string());
            // Count the attempt BEFORE making it and schedule the next one now,
            // so every path out of here — launch error, launch that dies later,
            // or a panic — leaves a service that backs off rather than one that
            // retries at full rate.
            may_destroy = state.may_destroy();
            state.consecutive_failures = state.consecutive_failures.saturating_add(1);
            let wait = restart_backoff(state.consecutive_failures, clock_jitter());
            state.next_attempt_unix_ms = Some(now_ms + wait.as_millis() as u64);
            // An incarnation proven unable to serve keeps answering its round
            // trip, so the launch below must not adopt it straight back.
            condemned = state
                .incarnation_unfit
                .is_some()
                .then(|| state.incarnation.clone())
                .flatten();
        }

        if observation.health == ServiceHealth::Wedged {
            if !may_destroy {
                log::warn!(
                    "build service '{name}' reads wedged, but this runner's health probe has \
                     not earned the confidence to kill it; leaving it running"
                );
                let mut diagnostics = self.build_service_runtime.lock().unwrap();
                let state = diagnostics.entry(name.to_string()).or_default();
                state.current_failure = Some(
                    "the daemon reads wedged and recovery has already been attempted without \
                     producing health; leaving it in place rather than killing it repeatedly"
                        .to_string(),
                );
                state.failure_config = Some(service_config_fingerprint(cfg));
                state.enter("recoveryFailed", now_ms);
                return;
            }
            // Kill the wedged daemon before relaunch: its port stays occupied
            // and `sccache --stop-server` hangs against it, so the supervised
            // child handle is killed directly.
            log::warn!("build service '{name}' wedged; killing and relaunching");
            self.kill_build_service_child(name);
        } else {
            log::info!("build service '{name}' down; launching");
        }
        ensure_state_dir(cfg, templates);

        let grant = LaunchGrant {
            dir: &site.dir,
            roots: &site.launch_roots,
            condemned: condemned.as_ref(),
        };
        let launched = reconcile_launched_service(
            self.services.process.as_ref(),
            control,
            cfg,
            templates,
            &grant,
            deny_read,
            may_destroy,
        );
        match launched {
            Ok(child) => self.record_launch(control, name, cfg, templates, site, child),
            Err(e) => {
                log::warn!("failed to start build service '{name}': {e}");
                let mut diagnostics = self.build_service_runtime.lock().unwrap();
                let state = diagnostics.entry(name.to_string()).or_default();
                state.current_failure = Some(bounded_tail(&e));
                state.failure_config = Some(service_config_fingerprint(cfg));
                state.supervised_pid = None;
                if state.consecutive_failures >= RECOVERY_FAILED_AFTER {
                    state.enter("recoveryFailed", unix_ms());
                } else {
                    state.enter("restarting", unix_ms());
                }
            }
        }
        let mut diagnostics = self.build_service_runtime.lock().unwrap();
        let state = diagnostics.entry(name.to_string()).or_default();
        if recovery_exhausted(state.consecutive_failures) {
            let reason = state.current_failure.clone().unwrap_or_else(|| {
                format!(
                    "the service did not prove health after {} consecutive launch attempts; automatic recovery is stopped while passive health observation continues",
                    state.consecutive_failures
                )
            });
            if let Err(error) =
                write_recovery_failure(cfg, templates, state.consecutive_failures, &reason)
            {
                log::error!(
                    "build service '{name}' could not persist recovery exhaustion: {error}"
                );
                state.current_failure = Some(format!(
                    "{reason}; additionally failed to persist this diagnosis: {error}"
                ));
            } else {
                state.current_failure = Some(reason);
            }
            state.failure_config = Some(service_config_fingerprint(cfg));
            state.next_attempt_unix_ms = None;
            state.enter("recoveryFailed", unix_ms());
        }
    }

    /// A healthy daemon whose recorded grant does not cover this runner: no
    /// record (every daemon that predates grant records), a record for another
    /// process, a missing root, or this runner's own launch configuration
    /// changed since it launched the daemon.
    ///
    /// When this runner may replace the daemon it relaunches it through the
    /// ordinary launch path, counted against `consecutive_failures` and backoff
    /// like any relaunch, so a relaunch that does not fix the grant cannot loop.
    /// Otherwise the daemon is left serving the roots its record does cover, and
    /// builds under the rest compile directly. Either way this never writes
    /// `current_failure`, so no check lane is refused over it.
    #[allow(clippy::too_many_arguments)]
    fn reconcile_stale_grant(
        &self,
        control: &dyn ListenerProcessControl,
        name: &str,
        cfg: &BuildServiceConfig,
        templates: &Templates,
        site: &GrantSite,
        deny_read: Vec<PathBuf>,
        may_launch: bool,
        observation: &ServiceObservation,
        stale: GrantStale,
        now_ms: u64,
    ) {
        let relaunch = {
            let mut diagnostics = self.build_service_runtime.lock().unwrap();
            let state = diagnostics.entry(name.to_string()).or_default();
            state.last_health = Some("healthy".to_string());
            state.last_checked_at = Some(chrono::Utc::now().timestamp());
            state.last_probe = observation.round_trip.as_ref().map(RoundTrip::summary);
            record_stats_sample(state, observation, now_ms);
            state.round_trip_proven |= observation
                .round_trip
                .as_ref()
                .is_some_and(RoundTrip::healthy);
            if state.generation == 0 {
                state.generation = 1;
            }
            match &stale.record {
                Some(record) => state.observe_incarnation(
                    Some((record.pid, record.start_token.clone())),
                    &record.roots,
                ),
                None => state.observe_incarnation(None, &[]),
            }
            // The daemon answers, so a launch or recovery failure recorded
            // before it did no longer describes it, in memory or on disk; left
            // standing it would refuse every check lane while builds here
            // compile directly. Reported corruption is a verdict about the
            // cache, not the process, and is kept exactly as health keeps it.
            if state.clear_unfit_after_healthy_probe(now_ms) {
                state.current_failure = None;
                state.failure_config = None;
                clear_recovery_failure(cfg, templates);
            }
            let may_relaunch = may_launch
                && state.may_destroy()
                && !recovery_exhausted(state.consecutive_failures);
            let due = state.next_attempt_unix_ms.is_none_or(|next| now_ms >= next);
            let note = GrantStaleNote {
                reason: stale.reason.clone(),
                relaunching: may_relaunch,
            };
            if state.grant_stale.as_ref() != Some(&note) {
                if may_relaunch {
                    log::warn!("build service '{name}' {}; relaunching", stale.reason);
                } else {
                    log::warn!(
                        "build service '{name}' grant stale ({}); clients compile directly",
                        stale.reason
                    );
                }
            }
            state.grant_stale = Some(note);
            state.enter("degraded", now_ms);
            let relaunch = may_relaunch && due;
            if relaunch {
                state.last_restart_at = Some(chrono::Utc::now().timestamp());
                state.last_restart_reason = Some(stale.reason.clone());
                state.consecutive_failures = state.consecutive_failures.saturating_add(1);
                let wait = restart_backoff(state.consecutive_failures, clock_jitter());
                state.next_attempt_unix_ms = Some(now_ms + wait.as_millis() as u64);
            }
            relaunch
        };

        if !relaunch {
            // The roots the record does cover still serve, once proven.
            if let Some(record) = &stale.record {
                let client_env = cfg.expanded_env(templates);
                if let Some(reason) = self.prove_incarnation_fitness(
                    name,
                    cfg,
                    templates,
                    &client_env,
                    record,
                    now_ms,
                ) {
                    log::warn!(
                        "build service '{name}' listener pid {} cannot serve: {reason}",
                        record.pid
                    );
                }
            }
            return;
        }

        self.kill_build_service_child(name);
        ensure_state_dir(cfg, templates);
        let grant = LaunchGrant {
            dir: &site.dir,
            roots: &site.launch_roots,
            condemned: None,
        };
        match reconcile_launched_service(
            self.services.process.as_ref(),
            control,
            cfg,
            templates,
            &grant,
            deny_read,
            true,
        ) {
            Ok(child) => self.record_launch(control, name, cfg, templates, site, child),
            Err(e) => {
                log::warn!("build service '{name}' could not be relaunched for its grant: {e}");
                let mut diagnostics = self.build_service_runtime.lock().unwrap();
                let state = diagnostics.entry(name.to_string()).or_default();
                state.grant_stale = Some(GrantStaleNote {
                    reason: format!("{}; relaunch failed: {}", stale.reason, bounded_tail(&e)),
                    relaunching: false,
                });
                if !self
                    .build_service_children
                    .lock()
                    .unwrap()
                    .contains_key(name)
                {
                    state.supervised_pid = None;
                }
            }
        }
    }

    /// Prove, once per daemon incarnation, that it can compile into each of
    /// this runner's managed roots its record grants. Returns the reason when
    /// the incarnation as a whole was found unable to serve.
    ///
    /// A root with a verdict for this incarnation is never probed again, and an
    /// inconclusive one waits out a backoff, so a healthy machine pays a few
    /// trivial compiles per daemon launch rather than two every tick. What can
    /// break mid-life is covered by the checks that do run every tick: the
    /// temp-dir liveness check, the round trip, and reported corruption.
    fn prove_incarnation_fitness(
        &self,
        name: &str,
        cfg: &BuildServiceConfig,
        templates: &Templates,
        env: &HashMap<String, String>,
        record: &DaemonGrantRecord,
        now_ms: u64,
    ) -> Option<String> {
        let incarnation = Some((record.pid, record.start_token.clone()));
        let awaiting: Vec<String> = {
            let mut diagnostics = self.build_service_runtime.lock().unwrap();
            let state = diagnostics.entry(name.to_string()).or_default();
            state.observe_incarnation(incarnation.clone(), &record.roots);
            state.roots_awaiting_probe(now_ms)
        }
        .into_iter()
        // A root with nothing in it yet has no build to serve; it is probed
        // once it exists.
        .filter(|root| Path::new(root).is_dir())
        .collect();
        if !awaiting.is_empty() {
            let report = probe_daemon_fitness(
                self.services.process.as_ref(),
                cfg,
                templates,
                env,
                &awaiting,
                CAPABILITY_PROBE_DEADLINE,
            );
            for (root, fitness) in &report.roots {
                match fitness {
                    RootFitness::Proven => {
                        log::info!("build service '{name}' proved it can compile into {root}")
                    }
                    RootFitness::Unfit { detail, .. } => log::warn!(
                        "build service '{name}' is unfit for {root}; builds there compile directly: {detail}"
                    ),
                    RootFitness::Inconclusive(why) => log::debug!(
                        "build service '{name}' fitness probe for {root} was inconclusive: {why}"
                    ),
                }
            }
            let mut diagnostics = self.build_service_runtime.lock().unwrap();
            let state = diagnostics.entry(name.to_string()).or_default();
            if state.incarnation == incarnation {
                state.apply_fitness(report, now_ms);
            }
        }
        self.build_service_runtime
            .lock()
            .unwrap()
            .get(name)
            .and_then(|state| state.incarnation_unfit.clone())
    }

    /// Account for a launch that produced a daemon, or adopted one: a new
    /// incarnation, whose grant is recorded beside the daemon's state so every
    /// runner sharing it can judge it ([`running_grant_verdict`]).
    #[allow(clippy::too_many_arguments)]
    fn record_launch(
        &self,
        control: &dyn ListenerProcessControl,
        name: &str,
        cfg: &BuildServiceConfig,
        templates: &Templates,
        site: &GrantSite,
        child: Option<Box<dyn ChildProcess>>,
    ) {
        let pid = child.as_ref().map(|child| child.id());
        let record = pid.and_then(|pid| {
            let Some(start_token) = control.start_token(pid) else {
                log::warn!(
                    "build service '{name}' launched pid {pid}, but its start token could not be read, so its grant cannot be recorded"
                );
                return None;
            };
            Some(DaemonGrantRecord {
                pid,
                start_token,
                launch_fingerprint: cfg.launch_fingerprint(templates),
                roots: root_strings(&site.launch_roots),
                write_globs: cfg.launch_grant(templates, &site.launch_roots),
                launcher_cairn_home: templates.cairn_home.display().to_string(),
                launched_at_unix_ms: unix_ms(),
            })
        });
        if let Some(record) = &record {
            if let Err(e) = write_grant_record(&site.dir, record) {
                log::warn!("build service '{name}' could not record its launch grant: {e}");
            }
        }
        let mut diagnostics = self.build_service_runtime.lock().unwrap();
        let state = diagnostics.entry(name.to_string()).or_default();
        match &child {
            Some(_) => log::info!("started build service '{name}'"),
            None => log::info!("adopted existing healthy build service '{name}'"),
        }
        state.current_failure = None;
        state.failure_config = None;
        state.grant_stale = None;
        // A launched daemon is not routable merely because spawn succeeded: its
        // new incarnation has no verdicts, so clients compile directly until the
        // next tick's capability compile proves each root. An adopted daemon
        // keeps whatever its incarnation has already proven.
        state.unfit = None;
        if pid.is_some() {
            match &record {
                Some(record) => state.observe_incarnation(
                    Some((record.pid, record.start_token.clone())),
                    &record.roots,
                ),
                None => state.observe_incarnation(None, &[]),
            }
        }
        state.restart_count += 1;
        // A new incarnation means the daemon's counters restarted at
        // zero. Retiring the sample with the generation is what keeps a
        // reset from reading as a collapse in hit rate.
        state.generation += 1;
        state.stats = None;
        state.stats_at_unix_ms = None;
        state.stats_gap =
            Some("the compile cache has just restarted and has not been sampled".to_string());
        state.supervised_pid = pid;
        state.launched_at_unix_ms = Some(unix_ms());
        state.launcher_cairn_home = Some(templates.cairn_home.display().to_string());
        state.launcher_pid = Some(std::process::id());
        state.launcher_confined = Some(false);
        // Launched, NOT proven. A spawn that succeeds says nothing about
        // whether the daemon serves; the next tick's probe decides, and
        // only that clears the attempt streak. Declaring health here is
        // how a spawn-then-exit loop would reset its own backoff on
        // every attempt and never reach `recoveryFailed`.
        state.enter(
            if state.consecutive_failures >= RECOVERY_FAILED_AFTER {
                "recoveryFailed"
            } else {
                "restarting"
            },
            unix_ms(),
        );
        drop(diagnostics);
        if let Some(child) = child {
            self.build_service_children
                .lock()
                .unwrap()
                .insert(name.to_string(), child);
        }
    }

    /// Observe a supervised child that has exited, removing its handle.
    ///
    /// Returns how it ended, once, so a caller records each death exactly once.
    fn observe_child_exit(&self, name: &str) -> Option<String> {
        let mut children = self.build_service_children.lock().unwrap();
        let exit = match children.get_mut(name)?.try_wait() {
            Ok(Some(status)) => describe_exit(&status),
            // An un-waitable handle is a handle we can no longer supervise, and
            // holding it would make a dead daemon look alive.
            Err(e) => format!("could not be waited on: {e}"),
            Ok(None) => return None,
        };
        children.remove(name);
        Some(exit)
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
        /// Cadence of the health/recovery tick.
        ///
        /// Shorter than the old minute because the backoff schedule, not the
        /// tick, is now what bounds relaunch attempts: a tick that finds a
        /// service in backoff does nothing but re-observe it. A healthy daemon
        /// costs one TCP connect plus one `--show-stats` round trip, and that
        /// round trip is also the statistics sample, so a faster tick buys both
        /// quicker recovery and fresher numbers for the same work.
        const TICK_INTERVAL: Duration = Duration::from_secs(15);
        let orch = self.clone();
        tokio::spawn(async move {
            let mut reported: Option<CompileCacheHealth> = None;
            loop {
                tokio::time::sleep(TICK_INTERVAL).await;
                let ticking = orch.clone();
                if let Err(e) =
                    tokio::task::spawn_blocking(move || ticking.ensure_build_services_ready()).await
                {
                    log::warn!("build service supervisor tick failed: {e}");
                }
                // The cache's state changes on this loop's own cadence, not on
                // an executor heartbeat, so the invalidation is emitted from
                // here. Only on news: the tick re-samples every fifteen seconds
                // and a restated identical sample would otherwise wake the panel
                // forever on a perfectly idle machine.
                let current = orch.compile_cache_health();
                if cache_change_is_news(reported.as_ref(), current.as_ref()) {
                    let _ = orch
                        .services
                        .emitter
                        .emit("substrate-health-change", serde_json::json!({}));
                    reported = current;
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
                // No compile probe: `assess_health` never runs one, and
                // rendering a list must never run a compiler. Fitness comes
                // from what the supervisor already found.
                let reachable = cfg
                    .ready
                    .as_ref()
                    .map(|probe| {
                        assess_health(
                            self.services.process.as_ref(),
                            &OsListenerProcessControl,
                            probe,
                            &cfg.expanded_env(&templates),
                            HEALTH_ROUND_TRIP_DEADLINE,
                        ) == ServiceHealth::Healthy
                    })
                    .unwrap_or(false);
                let fitness = if cfg.enabled {
                    let mut fitness: Vec<String> = self
                        .build_service_runtime
                        .lock()
                        .unwrap()
                        .get(&name)
                        .map(BuildServiceRuntimeDiagnostic::fitness)
                        .unwrap_or_default();
                    if !wrapper_enforces_gate(&cfg.expanded_env(&templates)) {
                        fitness.push(
                            "the cache wrapper builds run predates the output-directory gate; \
                             clients compile directly"
                                .to_string(),
                        );
                    }
                    fitness
                } else {
                    Vec::new()
                };
                BuildServiceStatus {
                    fitness,
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

    /// Read-only failure-boundary snapshot. It never probes, restarts, or otherwise
    /// mutates the service; the periodic supervisor remains the sole recovery owner.
    pub(crate) fn build_service_diagnostic_snapshot(
        &self,
        service_name: &str,
    ) -> BuildServiceDiagnosticSnapshot {
        const ERROR_TAIL_CHARS: usize = 2_000;
        let templates = self.build_service_templates();
        let config = settings::load_build_services(&self.config_dir)
            .into_iter()
            .find(|(name, _)| name == service_name)
            .map(|(_, config)| config);
        let state_dir = config
            .as_ref()
            .and_then(|config| config.expanded_state_dir(&templates));
        let error_log_tail = state_dir
            .as_ref()
            .and_then(|dir| std::fs::read_to_string(dir.join("sccache-error.log")).ok())
            .map(|contents| {
                let chars: Vec<char> = contents.chars().collect();
                chars[chars.len().saturating_sub(ERROR_TAIL_CHARS)..]
                    .iter()
                    .collect()
            });
        BuildServiceDiagnosticSnapshot {
            name: service_name.to_string(),
            configured: config.is_some(),
            enabled: config.as_ref().is_some_and(|config| config.enabled),
            config_fingerprint: config.as_ref().map(service_config_fingerprint),
            supervised_child: self
                .build_service_children
                .lock()
                .unwrap()
                .contains_key(service_name),
            state_dir: state_dir.map(|dir| dir.to_string_lossy().to_string()),
            error_log_tail,
            runtime: self
                .build_service_runtime
                .lock()
                .unwrap()
                .get(service_name)
                .cloned()
                .unwrap_or_default(),
        }
    }

    /// This machine's compile cache as the Substrate snapshot reads it.
    ///
    /// Read-only and non-blocking by construction: every fact here was
    /// established by the supervisor, and this only assembles them. It is a
    /// hot read — the substrate-health snapshot is rebuilt on every fleet
    /// change — so it must touch neither the daemon nor the filesystem.
    /// Sampling belongs to the supervisor alone, so the panel cannot be made
    /// slow (or made to auto-start a server) by the condition it exists to
    /// report; the same reason its statistics come from the supervisor's
    /// `--show-stats` round trip is why its configuration comes from
    /// reconciliation's [`ObservedCompileCacheService`] rather than from a
    /// settings parse and a `which` subprocess run per read.
    pub fn compile_cache_health(&self) -> Option<CompileCacheHealth> {
        let observed = self.observed_compile_cache_service()?;
        let name = observed.name;
        let now_ms = unix_ms();
        let runtime = self
            .build_service_runtime
            .lock()
            .unwrap()
            .get(&name)
            .cloned()
            .unwrap_or_default();

        let (state, unsupervised) = if !observed.enabled {
            (CompileCacheState::Disabled, None)
        } else if !sandbox::is_available() {
            // No service sandbox means Cairn deliberately runs no confined
            // daemon here and every build compiles uncached by design. That is a
            // configuration, not a fault, and must not read as one.
            (
                CompileCacheState::Disabled,
                Some("this host has no service sandbox, so builds compile uncached by design"),
            )
        } else if !observed.installed {
            (CompileCacheState::NotInstalled, None)
        } else {
            match runtime.lifecycle.as_deref() {
                Some("healthy") => (CompileCacheState::Healthy, None),
                Some("restarting") => (CompileCacheState::Restarting, None),
                Some("recoveryFailed") => (CompileCacheState::RecoveryFailed, None),
                Some("degraded") => (CompileCacheState::Degraded, None),
                // The supervisor has not reached this service yet in this
                // process. Saying so is honest; calling it healthy would not be.
                _ => (
                    CompileCacheState::Degraded,
                    Some("the supervisor has not observed this service yet"),
                ),
            }
        };

        // Statistics and health are separate facts. A healthy daemon can have
        // no usable sample, and an unhealthy one must render as a named gap
        // rather than as a cache with no hits.
        let stats = match (runtime.stats, runtime.stats_at_unix_ms) {
            (Some(stats), Some(measured_at)) if state == CompileCacheState::Healthy => {
                Measurement::measured(measured_at, stats)
            }
            _ => Measurement::unavailable_with(
                runtime.stats_at_unix_ms.unwrap_or(now_ms),
                MeasurementGap::NotSampled,
                runtime
                    .stats_gap
                    .clone()
                    .unwrap_or_else(|| "no sample has been taken yet".to_string()),
            ),
        };

        // A daemon can be healthy and still unfit for one of this runner's
        // roots, and that is the sentence an operator needs: builds there
        // compile directly.
        let fitness = (!runtime.fitness().is_empty()).then(|| runtime.fitness().join("; "));
        let condition = match state {
            CompileCacheState::Healthy => fitness,
            CompileCacheState::Disabled | CompileCacheState::NotInstalled => {
                unsupervised.map(str::to_string)
            }
            _ => unsupervised
                .map(str::to_string)
                .or_else(|| runtime.current_failure.clone())
                .or(fitness)
                .or_else(|| runtime.last_exit.clone())
                .or_else(|| runtime.last_probe.clone()),
        };

        Some(CompileCacheHealth {
            service: name,
            state,
            generation: runtime.generation,
            restart_count: runtime.restart_count,
            consecutive_failures: runtime.consecutive_failures,
            next_attempt_unix_ms: runtime.next_attempt_unix_ms,
            state_changed_at_unix_ms: runtime.state_changed_at_unix_ms,
            stats,
            condition,
        })
    }

    /// The merged client env for enabled build services, for a spawn that builds
    /// in `build_dir` — or nothing, when that directory is outside the managed
    /// build roots.
    ///
    /// Injection follows **where a spawn builds**, not whether it is supervised.
    /// The fence is a supervision policy and the compile cache is a performance
    /// mechanism; what actually binds a build to this daemon is whether the
    /// daemon may write where that build writes, which is decided per managed
    /// root ([`crate::config::build_services::managed_build_roots`]). Keying this
    /// on the fence instead left every build on an `allow`-dial workspace
    /// running plain `rustc` against no cache at all.
    pub(crate) fn build_service_client_env(&self, build_dir: &Path) -> HashMap<String, String> {
        let templates =
            settings::build_service_templates(&self.config_dir, Some(build_dir.to_path_buf()));
        let services = settings::load_build_services(&self.config_dir);
        let routing = routing_the_wrapper_can_enforce(
            self.compile_cache_routing(&services),
            &merge_client_env(&services, &templates),
        );
        client_env_for(&services, &templates, ClientBuild::At(build_dir), &routing)
    }

    /// Which roots this machine's compile cache may serve, from the same
    /// runtime record the panel reads.
    fn compile_cache_routing(&self, services: &HashMap<String, BuildServiceConfig>) -> Routing {
        let Some((name, _)) = compile_cache_service(services) else {
            return Routing::default();
        };
        self.build_service_runtime
            .lock()
            .unwrap()
            .get(&name)
            .map(BuildServiceRuntimeDiagnostic::routing)
            .unwrap_or_default()
    }

    /// One sentence per managed root this machine's compile cache may not
    /// serve. A hot read: assembled from the supervisor's record, with no
    /// settings parse, probe, or filesystem access.
    pub(crate) fn compile_cache_fitness(&self) -> Vec<String> {
        let Some(observed) = self.observed_compile_cache_service() else {
            return Vec::new();
        };
        if !observed.enabled {
            return Vec::new();
        }
        self.build_service_runtime
            .lock()
            .unwrap()
            .get(&observed.name)
            .map(BuildServiceRuntimeDiagnostic::fitness)
            .unwrap_or_default()
    }

    /// The client env for a command running in an executor cell **on this
    /// machine**.
    ///
    /// A cell lives under `{buildRoot}/build-slots` (the colocated build root,
    /// cairn/5286), so it is inside that managed root without a path test —
    /// which matters because the runner composing a cell request knows the
    /// cell's repository but not the absolute slot the executor will hand it.
    /// The caller states the two things this cannot see: that the batch is not
    /// running in the project's live checkout, and that it was placed on the
    /// colocated executor (this daemon answers on loopback and is named by this
    /// machine's paths).
    pub(crate) fn cell_build_service_client_env(&self) -> HashMap<String, String> {
        let services = settings::load_build_services(&self.config_dir);
        let templates = settings::build_service_templates(&self.config_dir, None);
        let routing = routing_the_wrapper_can_enforce(
            self.compile_cache_routing(&services),
            &merge_client_env(&services, &templates),
        );
        client_env_for(&services, &templates, ClientBuild::Cell, &routing)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::config::build_services::{canonical_path, target_grant_glob};
    use crate::db::DbState;
    use crate::services::testing::{MockChildProcess, MockProcessSpawner, TestServicesBuilder};
    use crate::storage::{LocalDb, SearchIndex};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    fn default_sccache_service() -> BuildServiceConfig {
        BuildServiceConfig {
            enabled: true,
            start: vec!["sccache".into()],
            ready: Some(ReadyProbe {
                tcp: Some("127.0.0.1:4227".into()),
                command: None,
                round_trip: Some(vec!["sccache".into(), "--show-stats".into()]),
            }),
            state_dir: Some("{home}/.cache/sccache-cairn".into()),
            write: vec!["{worktrees}/**/target/**".into()],
            env: HashMap::from([
                ("SCCACHE_SERVER_PORT".into(), "4227".into()),
                ("SCCACHE_DIR".into(), "{home}/.cache/sccache-cairn".into()),
                (
                    "RUSTC_WRAPPER".into(),
                    "{cairnHome}/bin/cache-wrapper.sh".into(),
                ),
                (BUILD_SERVICE_CLIENT_ENV.into(), "1".into()),
            ]),
            launch_env: HashMap::from([
                ("SCCACHE_START_SERVER".into(), "1".into()),
                ("TMPDIR".into(), "{home}/.cache/sccache-cairn-tmp".into()),
                ("TMP".into(), "{home}/.cache/sccache-cairn-tmp".into()),
                ("TEMP".into(), "{home}/.cache/sccache-cairn-tmp".into()),
            ]),
        }
    }

    fn templates() -> Templates {
        Templates {
            home: PathBuf::from("/home/u"),
            cache_home: PathBuf::from("/home/u"),
            cairn_home: PathBuf::from("/home/u/.cairn"),
            build_root: PathBuf::from("/home/u/.cairn"),
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
        // The port claim travels with the service that makes it.
        assert_eq!(
            env.get(BUILD_SERVICE_CLIENT_ENV).map(String::as_str),
            Some("1")
        );
        assert_eq!(
            env.get("SCCACHE_DIR").map(String::as_str),
            Some("/home/u/.cache/sccache-cairn")
        );
        // A disabled service contributes nothing, even unique keys.
        assert!(!env.contains_key("DISABLED_ONLY"));
    }

    /// Two loads of one configuration are one configuration, whatever order
    /// their env maps iterate in.
    #[test]
    fn the_config_fingerprint_does_not_depend_on_map_order() {
        let cfg = default_sccache_service();
        // Each collect builds a map with a fresh random hasher, so these
        // iterate in different orders, as two loads of settings do.
        let fingerprints: std::collections::BTreeSet<String> = (0..16)
            .map(|_| {
                let mut copy = cfg.clone();
                copy.env = cfg.env.clone().into_iter().collect();
                copy.launch_env = cfg.launch_env.clone().into_iter().collect();
                service_config_fingerprint(&copy)
            })
            .chain([service_config_fingerprint(&cfg)])
            .collect();
        assert_eq!(fingerprints.len(), 1, "{fingerprints:?}");
    }

    #[test]
    fn recovery_exhaustion_survives_runtime_state_recreation() {
        let root = tempfile::tempdir().unwrap();
        let mut cfg = default_sccache_service();
        cfg.state_dir = Some(root.path().display().to_string());
        let templates = templates_in(root.path());
        let diagnosis = "the service remained down after 5 consecutive launch attempts";

        write_recovery_failure(&cfg, &templates, RECOVERY_FAILED_AFTER, diagnosis).unwrap();
        let restored = read_recovery_failure(&cfg, &templates).unwrap();
        assert_eq!(restored.consecutive_failures, RECOVERY_FAILED_AFTER);
        assert_eq!(restored.diagnosis, diagnosis);

        let failed_fingerprint = restored.config_fingerprint;
        cfg.env.insert("CONFIG_CHANGED".into(), "1".into());
        assert!(
            read_recovery_failure(&cfg, &templates).is_none(),
            "a changed service definition must ignore the stale persisted failure"
        );

        let mut runtime = BuildServiceRuntimeDiagnostic {
            consecutive_failures: RECOVERY_FAILED_AFTER,
            lifecycle: Some("recoveryFailed".into()),
            current_failure: Some(diagnosis.into()),
            failure_config: Some(failed_fingerprint),
            ..BuildServiceRuntimeDiagnostic::default()
        };
        assert!(reset_recovery_for_changed_config(
            &mut runtime,
            &cfg,
            &templates,
            99
        ));
        assert_eq!(runtime.consecutive_failures, 0);
        assert_eq!(runtime.lifecycle.as_deref(), Some("degraded"));
        assert!(runtime.current_failure.is_none());
    }

    pub(crate) struct FakeListenerControl {
        /// Held open only when a test needs the port to answer a real TCP probe.
        /// Binding an ephemeral port is not free of consequence here — see the
        /// serialization note below — so tests that only exercise this control
        /// use [`FakeListenerControl::detached`] and bind nothing.
        socket: std::sync::Arc<std::sync::Mutex<Option<std::net::TcpListener>>>,
        listening: std::sync::Mutex<bool>,
        process: std::sync::Mutex<ListenerProcess>,
        environ: Result<HashMap<String, String>, String>,
        terminated: std::sync::Mutex<Vec<u32>>,
    }

    /// The start token the fake reports for `pid`: one incarnation per pid.
    fn fake_token(pid: u32) -> String {
        format!("start-{pid}")
    }

    impl FakeListenerControl {
        fn new(socket: Option<std::net::TcpListener>, pid: u32, executable: PathBuf) -> Self {
            Self {
                listening: std::sync::Mutex::new(socket.is_some()),
                socket: std::sync::Arc::new(std::sync::Mutex::new(socket)),
                process: std::sync::Mutex::new(ListenerProcess { pid, executable }),
                environ: Ok(HashMap::new()),
                terminated: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn is_listening(&self) -> bool {
            *self.listening.lock().unwrap()
        }

        /// A new daemon takes the port: what a relaunch does once the old
        /// listener is gone.
        fn relisten(&self, addr: &str, pid: u32) {
            *self.socket.lock().unwrap() = Some(std::net::TcpListener::bind(addr).unwrap());
            self.process.lock().unwrap().pid = pid;
            *self.listening.lock().unwrap() = true;
        }

        /// The same port, answered by a different process.
        fn set_pid(&self, pid: u32) {
            self.process.lock().unwrap().pid = pid;
        }

        /// A listener this control reports without holding a real socket.
        fn detached(pid: u32, executable: PathBuf) -> Self {
            let control = Self::new(None, pid, executable);
            *control.listening.lock().unwrap() = true;
            control
        }

        fn with_env(mut self, key: &str, value: &str) -> Self {
            self.environ
                .as_mut()
                .unwrap()
                .insert(key.to_string(), value.to_string());
            self
        }

        fn with_unreadable_environ(mut self) -> Self {
            self.environ = Err("read environment for listener pid 1: denied".to_string());
            self
        }
    }

    impl ListenerProcessControl for FakeListenerControl {
        fn listener(&self, _addr: &str) -> Result<Option<ListenerProcess>, String> {
            Ok(self
                .listening
                .lock()
                .unwrap()
                .then(|| self.process.lock().unwrap().clone()))
        }

        fn start_token(&self, pid: u32) -> Option<String> {
            Some(fake_token(pid))
        }

        fn environ(&self, _pid: u32) -> Result<HashMap<String, String>, String> {
            self.environ.clone()
        }

        fn terminate(&self, pid: u32) -> Result<(), String> {
            self.terminated.lock().unwrap().push(pid);
            *self.listening.lock().unwrap() = false;
            self.socket.lock().unwrap().take();
            Ok(())
        }
    }

    #[test]
    fn lsof_listener_selection_matches_exact_configured_address() {
        let output = concat!(
            "p100\n",
            "nTCP 0.0.0.0:4227 (LISTEN)\n",
            "p200\n",
            "nTCP 127.0.0.1:4227 (LISTEN)\n",
            "p300\n",
            "nTCP [::1]:4227 (LISTEN)\n",
        );
        assert_eq!(
            listener_pid_from_lsof(output, &["127.0.0.1:4227".to_string()]).unwrap(),
            Some(200)
        );
        assert_eq!(
            listener_pid_from_lsof(output, &["[::1]:4227".to_string()]).unwrap(),
            Some(300)
        );
    }

    #[test]
    fn lsof_listener_selection_refuses_ambiguous_exact_matches() {
        let output = "p100\nn127.0.0.1:4227\np200\nn127.0.0.1:4227\n";
        let error = listener_pid_from_lsof(output, &["127.0.0.1:4227".to_string()]).unwrap_err();
        assert!(error.contains("multiple listener processes"));
    }

    fn sccache_services() -> HashMap<String, BuildServiceConfig> {
        HashMap::from([("sccache".to_string(), default_sccache_service())])
    }

    /// A daemon proven for every managed root of `templates`.
    fn fit_routing(templates: &Templates) -> Routing {
        Routing {
            daemon_unfit: false,
            roots: root_strings(&managed_build_roots(templates)),
        }
    }

    /// The regression this whole change is about: the compile cache is a
    /// performance mechanism and the fence is a supervision policy, so a build
    /// inside a managed root is pointed at the daemon with no fence in sight.
    /// Gating this on the fence left every build on an `allow`-dial workspace
    /// running plain `rustc` against no cache at all.
    #[test]
    fn a_build_inside_a_managed_root_is_pointed_at_the_daemon() {
        let routing = fit_routing(&templates());
        for build_dir in [
            "/home/u/.cairn/worktrees/cairn-1/src-tauri",
            "/home/u/.cairn/build-slots/cairn/slot-3",
        ] {
            let env = client_env_for(
                &sccache_services(),
                &templates(),
                ClientBuild::At(Path::new(build_dir)),
                &routing,
            );
            assert_eq!(
                env.get("SCCACHE_SERVER_PORT").map(String::as_str),
                Some("4227"),
                "{build_dir} builds where the daemon may write, so it should use it"
            );
            // Named to the wrapper as "attach, never start": nothing but the
            // supervised daemon may hold Cairn's port, and this spawn need not be
            // fenced to be told so.
            assert_eq!(
                env.get(BUILD_SERVICE_CLIENT_ENV).map(String::as_str),
                Some("1")
            );
            // And told which roots it may attach for, canonically spelled.
            assert_eq!(
                env.get(BUILD_SERVICE_ROOTS_ENV).cloned(),
                Some(routing.roots.join(":"))
            );
            assert!(!env.contains_key(BUILD_SERVICE_UNFIT_ENV), "{env:?}");
        }
    }

    /// Routing is per root: a daemon proven for the worktrees but not for the
    /// cells still serves the worktrees, and a cell compiles directly.
    #[test]
    fn a_build_is_routed_only_from_a_root_the_daemon_may_serve() {
        let t = templates();
        let worktrees = canonical_path(Path::new("/home/u/.cairn/worktrees"))
            .display()
            .to_string();
        let routing = Routing {
            daemon_unfit: false,
            roots: vec![worktrees.clone()],
        };
        let cell = client_env_for(&sccache_services(), &t, ClientBuild::Cell, &routing);
        assert_eq!(
            cell.get(BUILD_SERVICE_UNFIT_ENV).map(String::as_str),
            Some("1")
        );
        assert_eq!(cell.get(BUILD_SERVICE_ROOTS_ENV), Some(&worktrees));
        let worktree = client_env_for(
            &sccache_services(),
            &t,
            ClientBuild::At(Path::new("/home/u/.cairn/worktrees/cairn-1")),
            &routing,
        );
        assert!(
            !worktree.contains_key(BUILD_SERVICE_UNFIT_ENV),
            "{worktree:?}"
        );
        // A daemon with no proven root routes nothing, and says so.
        let unproven = client_env_for(
            &sccache_services(),
            &t,
            ClientBuild::Cell,
            &Routing::default(),
        );
        assert_eq!(
            unproven.get(BUILD_SERVICE_ROOTS_ENV).map(String::as_str),
            Some("")
        );
        assert_eq!(
            unproven.get(BUILD_SERVICE_UNFIT_ENV).map(String::as_str),
            Some("1")
        );
    }

    /// The other half, and the reason admission is by build directory at all: the
    /// daemon runs each cache-miss compile itself, so a build it may not write
    /// for does not lose a cache hit — rustc's output write is kernel-denied and
    /// the compile fails with `Operation not permitted`.
    #[test]
    fn a_build_outside_the_managed_roots_is_left_to_its_own_cache() {
        let env = client_env_for(
            &sccache_services(),
            &templates(),
            ClientBuild::At(Path::new("/home/u/projects/cairn")),
            &fit_routing(&templates()),
        );
        assert!(
            env.is_empty(),
            "the developer's own checkout must keep sccache's defaults and its own \
             unconfined server, not attach to the confined daemon: {env:?}"
        );
    }

    /// A cell is inside a managed root by construction, which is what lets the
    /// runner admit one without knowing the absolute slot the executor will hand
    /// it.
    #[test]
    fn an_executor_cell_is_admitted_without_a_path() {
        let env = client_env_for(
            &sccache_services(),
            &templates(),
            ClientBuild::Cell,
            &fit_routing(&templates()),
        );
        assert_eq!(
            env.get("SCCACHE_SERVER_PORT").map(String::as_str),
            Some("4227")
        );
        assert_eq!(
            env.get(BUILD_SERVICE_CLIENT_ENV).map(String::as_str),
            Some("1")
        );
        assert!(!env.contains_key(BUILD_SERVICE_UNFIT_ENV), "{env:?}");
    }

    /// With nothing to attach to, a build must be told nothing at all: a stray
    /// port claim would make the wrapper skip the auto-started server that is the
    /// correct behavior where Cairn supervises no compile cache.
    #[test]
    fn no_enabled_service_names_no_daemon() {
        let mut disabled = default_sccache_service();
        disabled.enabled = false;
        let services = HashMap::from([("sccache".to_string(), disabled)]);

        assert!(client_env_for(
            &services,
            &templates(),
            ClientBuild::Cell,
            &Routing::default()
        )
        .is_empty());
    }

    /// Build services are a generic facility, so "some service is enabled" is not
    /// "an sccache daemon holds Cairn's port". A workspace running an unrelated
    /// service with sccache switched off must keep sccache's own behavior —
    /// auto-starting its server — rather than being told to attach to whatever
    /// happens to be listening on the default port, or to compile uncached
    /// forever.
    #[test]
    fn an_unrelated_service_does_not_claim_the_compile_cache_port() {
        let mut sccache = default_sccache_service();
        sccache.enabled = false;
        let mut other = BuildServiceConfig {
            enabled: true,
            ..default_sccache_service()
        };
        other.env = HashMap::from([("FOO".to_string(), "bar".to_string())]);
        let services = HashMap::from([
            ("sccache".to_string(), sccache),
            ("mycache".to_string(), other),
        ]);

        let env = client_env_for(
            &services,
            &templates(),
            ClientBuild::Cell,
            &fit_routing(&templates()),
        );
        assert_eq!(env.get("FOO").map(String::as_str), Some("bar"));
        assert!(
            !env.contains_key(BUILD_SERVICE_CLIENT_ENV),
            "only a service that supervises a compile-cache daemon may claim its \
             port against client auto-start: {env:?}"
        );
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
                    && cfg.confinement.is_fenced()
            })
            .returning(|_| Ok(Box::new(MockChildProcess::with_stdout(7, vec![]))));

        let child = launch_service(
            &spawner,
            &default_sccache_service(),
            &templates(),
            &[],
            vec![],
        )
        .expect("launch should succeed");
        assert_eq!(child.id(), 7);
    }

    /// The grant comes from the managed roots by construction, so a moved
    /// build root is granted with no `write` entry naming it — and nothing
    /// broader than its `target/` trees is granted.
    #[test]
    fn a_daemon_is_granted_every_managed_roots_target_tree_by_construction() {
        let mut cfg = default_sccache_service();
        cfg.write.clear();
        let t = Templates {
            build_root: PathBuf::from("/Volumes/Builds/x"),
            ..templates()
        };
        let roots = managed_build_roots(&t);
        let expected: Vec<String> = roots
            .iter()
            .map(|root| sandbox::glob_to_regex(&target_grant_glob(root)))
            .collect();
        assert!(expected
            .iter()
            .any(|regex| regex.ends_with("/build-slots/.*/target/.*")));
        assert!(expected
            .iter()
            .any(|regex| regex.ends_with("/worktrees/.*/target/.*")));
        let config = build_service_spawn_config(&cfg, &t, &roots, vec![]).unwrap();
        if let sandbox::Confinement::Fenced(policy) = &config.confinement {
            assert_eq!(policy.writable_regex, expected);
        }
        // The probe's site is inside that shape, so a passing probe proves the
        // exact write a build under the root makes.
        for root in &roots {
            let grant =
                regex::Regex::new(&sandbox::glob_to_regex(&target_grant_glob(root))).unwrap();
            assert!(grant.is_match(
                &capability_probe_dir(root)
                    .join("out_1/probe.d")
                    .to_string_lossy()
            ));
            assert!(!grant.is_match(&root.join("cairn-1/src/lib.rs").to_string_lossy()));
        }
    }

    /// Templates rooted at a real directory, so paths the launch path creates
    /// can be asserted on disk.
    fn templates_in(root: &Path) -> Templates {
        Templates {
            home: root.to_path_buf(),
            cache_home: root.to_path_buf(),
            cairn_home: root.join(".cairn"),
            build_root: root.join(".cairn"),
            worktrees: root.join(".cairn").join("worktrees"),
            worktree: None,
        }
    }

    #[test]
    fn launch_service_creates_the_pinned_daemon_temp_dir() {
        // The daemon inherits the pinned TMPDIR for life and sccache aborts a
        // compile outright when that path is missing, so the directory must exist
        // before the process does: the confined daemon cannot create it itself.
        let root = tempfile::tempdir().unwrap();
        let templates = templates_in(root.path());
        let cfg = default_sccache_service();
        let dirs = daemon_temp_dirs(&cfg, &templates);
        assert!(
            !dirs.is_empty(),
            "the default sccache service must pin a daemon temp dir"
        );
        for dir in &dirs {
            assert!(!dir.exists(), "precondition: {dir:?} should not exist yet");
        }

        let mut spawner = MockProcessSpawner::new();
        spawner
            .expect_spawn()
            .returning(|_| Ok(Box::new(MockChildProcess::with_stdout(7, vec![]))));
        launch_service(&spawner, &cfg, &templates, &[], vec![]).expect("launch should succeed");

        for dir in &dirs {
            assert!(dir.is_dir(), "daemon temp dir {dir:?} was not created");
        }
    }

    #[test]
    #[serial_test::serial(build_service_port)]
    fn startup_bind_conflict_adopts_healthy_compatible_server() {
        use mockall::Sequence;

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let mut cfg = default_sccache_service();
        cfg.ready.as_mut().unwrap().tcp = Some(addr);

        let mut sequence = Sequence::new();
        let mut spawner = MockProcessSpawner::new();
        spawner
            .expect_spawn()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|_| Ok(Box::new(MockChildProcess::failing(10, "bind conflict", 1))));
        spawner
            .expect_spawn()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|_| Ok(Box::new(MockChildProcess::failing(11, "", 0))));

        let control =
            FakeListenerControl::new(Some(listener), 42, PathBuf::from("/unused/sccache"));
        // Adoptable because its record covers this runner: launched by another
        // runner, so only the roots are compared, never that runner's config.
        let grant_dir = tempfile::tempdir().unwrap();
        let t = templates();
        write_grant_record(
            grant_dir.path(),
            &grant_record(
                42,
                root_strings(&managed_build_roots(&t)),
                &cfg,
                &t,
                "/other",
            ),
        )
        .unwrap();
        let child = reconcile_launched_service(
            &spawner,
            &control,
            &cfg,
            &t,
            &unrecorded(&grant_dir),
            vec![],
            true,
        )
        .expect("healthy server should be adopted");
        assert!(child.is_none());
        assert!(control.terminated.lock().unwrap().is_empty());
    }

    /// A record for this daemon written by `launcher`.
    fn grant_record(
        pid: u32,
        roots: Vec<String>,
        cfg: &BuildServiceConfig,
        templates: &Templates,
        launcher: &str,
    ) -> DaemonGrantRecord {
        DaemonGrantRecord {
            pid,
            start_token: fake_token(pid),
            launch_fingerprint: cfg.launch_fingerprint(templates),
            roots,
            write_globs: Vec::new(),
            launcher_cairn_home: launcher.to_string(),
            launched_at_unix_ms: 1,
        }
    }

    /// A launch whose grant is recorded in `dir`, granting no roots.
    fn unrecorded(dir: &tempfile::TempDir) -> LaunchGrant<'_> {
        LaunchGrant {
            dir: dir.path(),
            roots: &[],
            condemned: None,
        }
    }

    /// Answering is not a grant. A healthy server that won the port with no
    /// record covering this runner — every daemon that predates grant records,
    /// or one granted for a build root that has since moved — is never adopted,
    /// and one this runner may not destroy is left running with the reason.
    #[test]
    #[serial_test::serial(build_service_port)]
    fn a_healthy_server_without_a_covering_grant_is_never_adopted() {
        let temp = tempfile::tempdir().unwrap();
        let executable = temp.path().join("sccache");
        std::fs::write(&executable, "fake").unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let mut cfg = default_sccache_service();
        cfg.start = vec![executable.to_string_lossy().to_string()];
        cfg.ready.as_mut().unwrap().tcp = Some(addr);
        let mut spawner = MockProcessSpawner::new();
        spawner.expect_spawn().returning(|config| {
            Ok(Box::new(
                if config.env.contains_key("SCCACHE_START_SERVER") {
                    MockChildProcess::failing(10, "Address already in use", 1)
                } else {
                    MockChildProcess::failing(11, "", 0)
                },
            ))
        });
        let control = FakeListenerControl::new(Some(listener), 42, executable);
        let grant_dir = tempfile::tempdir().unwrap();
        let refused = match reconcile_launched_service(
            &spawner,
            &control,
            &cfg,
            &templates(),
            &unrecorded(&grant_dir),
            vec![],
            false,
        ) {
            Err(refused) => refused,
            Ok(_) => panic!("a daemon with no grant record must not be adopted"),
        };
        assert!(
            refused.contains("grant does not cover this runner"),
            "{refused}"
        );
        assert!(refused.contains("no grant record"), "{refused}");
        assert!(control.terminated.lock().unwrap().is_empty());
    }

    #[test]
    #[serial_test::serial(build_service_port)]
    fn startup_bind_conflict_terminates_verified_orphan_then_relaunches() {
        use mockall::Sequence;

        let temp = tempfile::tempdir().unwrap();
        let executable = temp.path().join("sccache");
        std::fs::write(&executable, "fake").unwrap();
        let executable = std::fs::canonicalize(executable).unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let mut cfg = default_sccache_service();
        cfg.start = vec![executable.to_string_lossy().to_string()];
        cfg.ready.as_mut().unwrap().tcp = Some(addr);

        let mut sequence = Sequence::new();
        let mut spawner = MockProcessSpawner::new();
        spawner
            .expect_spawn()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|_| Ok(Box::new(MockChildProcess::failing(20, "bind conflict", 1))));
        for id in [21, 22] {
            spawner
                .expect_spawn()
                .times(1)
                .in_sequence(&mut sequence)
                .returning(move |_| Ok(Box::new(MockChildProcess::failing(id, "unhealthy", 1))));
        }
        spawner
            .expect_spawn()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|_| Ok(Box::new(MockChildProcess::with_stdout(23, vec![]))));

        let control = FakeListenerControl::new(Some(listener), 4242, executable);
        let grant_dir = tempfile::tempdir().unwrap();
        let child = reconcile_launched_service(
            &spawner,
            &control,
            &cfg,
            &templates(),
            &unrecorded(&grant_dir),
            vec![],
            true,
        )
        .expect("verified orphan should be replaced")
        .expect("replacement should be supervised");
        assert_eq!(child.id(), 23);
        assert_eq!(*control.terminated.lock().unwrap(), vec![4242]);
    }

    #[test]
    fn listener_temp_dir_liveness_acts_only_on_confirmed_evidence() {
        let live = tempfile::tempdir().unwrap();
        let live = live.path().to_string_lossy().to_string();
        let reclaimed = tempfile::tempdir().unwrap();
        let reclaimed = reclaimed.path().to_string_lossy().to_string();
        std::fs::remove_dir_all(&reclaimed).unwrap();
        // Nothing here talks to a real socket, so nothing binds one: an ephemeral
        // port bound in one test can be reused by the OS for a port another test
        // just released, which is the race the serialized group below exists for.
        let sccache = PathBuf::from("/unused/sccache");
        let bound = || FakeListenerControl::detached(1, sccache.clone());

        // The one case we act on: a daemon still naming a directory that is gone.
        let poisoned = bound().with_env("TMPDIR", &reclaimed);
        assert!(!listener_temp_dir_live(&poisoned, "127.0.0.1:1"));

        // Everything else fails open — killing a working daemon on a guess is the
        // more expensive mistake.
        for (case, control) in [
            ("a live temp dir", bound().with_env("TMPDIR", &live)),
            ("no temp dir in the environment", bound()),
            (
                "a relative value BSD ps may have truncated",
                bound().with_env("TMPDIR", "folders"),
            ),
            (
                "an unreadable environment",
                bound().with_unreadable_environ(),
            ),
            (
                "no listener at all",
                FakeListenerControl::new(None, 1, sccache.clone()),
            ),
        ] {
            assert!(
                listener_temp_dir_live(&control, "127.0.0.1:1"),
                "{case} proves nothing and must not condemn the daemon"
            );
        }
    }

    #[test]
    #[serial_test::serial(build_service_port)]
    fn startup_bind_conflict_replaces_a_daemon_that_cannot_use_its_temp_dir() {
        // The defect this guards: a daemon that answers every probe but holds a
        // reclaimed temp dir fails EVERY compile routed to it, machine-wide.
        // Adopting it trades a port conflict for a broken build fabric, so it is
        // replaced instead — subject to the same fail-closed executable check that
        // protects unrelated listeners.
        use mockall::Sequence;

        let temp = tempfile::tempdir().unwrap();
        let executable = temp.path().join("sccache");
        std::fs::write(&executable, "fake").unwrap();
        let executable = std::fs::canonicalize(executable).unwrap();
        let reclaimed = tempfile::tempdir().unwrap();
        let reclaimed = reclaimed.path().to_string_lossy().to_string();
        std::fs::remove_dir_all(&reclaimed).unwrap();

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let mut cfg = default_sccache_service();
        cfg.start = vec![executable.to_string_lossy().to_string()];
        cfg.ready.as_mut().unwrap().tcp = Some(addr);

        let mut sequence = Sequence::new();
        let mut spawner = MockProcessSpawner::new();
        // Our launch loses the bind race to the stray daemon...
        spawner
            .expect_spawn()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|_| Ok(Box::new(MockChildProcess::failing(30, "bind conflict", 1))));
        // ...whose health round-trips both answer cleanly. Answering is exactly
        // what makes this daemon dangerous, so the verdict must not come from the
        // round-trip alone.
        spawner
            .expect_spawn()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|_| Ok(Box::new(MockChildProcess::failing(31, "", 0))));
        spawner
            .expect_spawn()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|_| Ok(Box::new(MockChildProcess::with_stdout(33, vec![]))));

        let control = FakeListenerControl::new(Some(listener), 777, executable)
            .with_env("TMPDIR", &reclaimed);
        let grant_dir = tempfile::tempdir().unwrap();
        let child = reconcile_launched_service(
            &spawner,
            &control,
            &cfg,
            &templates(),
            &unrecorded(&grant_dir),
            vec![],
            true,
        )
        .expect("a daemon that cannot use its temp dir must be replaced")
        .expect("the replacement should be supervised");
        assert_eq!(child.id(), 33);
        assert_eq!(*control.terminated.lock().unwrap(), vec![777]);
    }

    /// The safety property the whole recovery path now rests on.
    ///
    /// For three weeks a healthy daemon was declared wedged on every tick, and
    /// the only thing standing between that false verdict and a shared cache
    /// killed every 15 seconds was an executable-identity comparison that
    /// happened to be broken. Fixing that comparison without bounding
    /// destruction would have converted a stuck supervisor into a destructive
    /// one. So an unhealthy verdict this runner cannot vouch for must leave the
    /// listener alone (CAIRN-3332).
    #[test]
    #[serial_test::serial(build_service_port)]
    fn an_unvouched_verdict_never_terminates_a_listening_daemon() {
        let temp = tempfile::tempdir().unwrap();
        let executable = temp.path().join("sccache");
        std::fs::write(&executable, "fake").unwrap();
        let executable = std::fs::canonicalize(executable).unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let mut cfg = default_sccache_service();
        cfg.start = vec![executable.to_string_lossy().to_string()];
        cfg.ready.as_mut().unwrap().tcp = Some(addr);

        // Our launch loses the bind race, and every round trip reads unhealthy —
        // exactly the production shape, where the listener is in fact fine.
        let mut spawner = MockProcessSpawner::new();
        spawner
            .expect_spawn()
            .returning(|_| Ok(Box::new(MockChildProcess::failing(40, "unhealthy", 1))));

        let control = FakeListenerControl::new(Some(listener), 4242, executable);
        let grant_dir = tempfile::tempdir().unwrap();
        let refused = match reconcile_launched_service(
            &spawner,
            &control,
            &cfg,
            &templates(),
            &unrecorded(&grant_dir),
            vec![],
            false,
        ) {
            Err(refused) => refused,
            Ok(_) => panic!("an unvouched verdict must not authorize termination"),
        };
        assert!(
            refused.contains("a TCP connection to")
                && refused.contains("succeeded at the termination decision"),
            "{refused}"
        );
        assert!(refused.contains("leaving it in place"), "{refused}");
        assert!(
            control.terminated.lock().unwrap().is_empty(),
            "a daemon this runner cannot vouch for was killed anyway"
        );
    }

    #[test]
    #[serial_test::serial(build_service_port)]
    fn a_listener_that_dies_during_health_assessment_is_not_a_bind_conflict() {
        use mockall::Sequence;

        let root = tempfile::tempdir().unwrap();
        let executable = root.path().join("sccache");
        std::fs::write(&executable, "fake").unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();

        let mut cfg = default_sccache_service();
        cfg.start = vec![executable.to_string_lossy().to_string()];
        cfg.ready.as_mut().unwrap().tcp = Some(addr);
        let control = FakeListenerControl::new(Some(listener), 50, executable);
        let socket = control.socket.clone();

        let mut sequence = Sequence::new();
        let mut spawner = MockProcessSpawner::new();
        spawner
            .expect_spawn()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|_| Ok(Box::new(MockChildProcess::failing(50, "startup failed", 1))));
        spawner
            .expect_spawn()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(move |_| {
                socket.lock().unwrap().take();
                Ok(Box::new(MockChildProcess::failing(51, "unhealthy", 1)))
            });
        spawner
            .expect_spawn()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|_| Ok(Box::new(MockChildProcess::with_stdout(52, vec![]))));

        let child = reconcile_launched_service(
            &spawner,
            &control,
            &cfg,
            &templates_in(root.path()),
            &unrecorded(&root),
            vec![],
            false,
        )
        .expect("a port which is free at decision time must permit another launch")
        .expect("the replacement launch must be supervised");
        assert_eq!(child.id(), 52);
        assert!(control.terminated.lock().unwrap().is_empty());
    }

    #[test]
    fn tcp_reachability_checks_every_resolved_address() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let live = listener.local_addr().unwrap();
        let closed: std::net::SocketAddr = CLOSED_ADDR.parse().unwrap();

        assert!(
            any_tcp_endpoint_reachable([closed, live]),
            "a closed first address must not hide a live address family"
        );
        assert!(
            !any_tcp_endpoint_reachable([closed]),
            "all closed addresses must report the endpoint free"
        );
    }

    /// Destruction authority is earned, and spent.
    #[test]
    fn destruction_authority_is_bounded_and_reset_only_by_health() {
        let mut state = BuildServiceRuntimeDiagnostic::default();
        // An instrument that has never once answered authorizes exactly one
        // destructive attempt, not an endless series.
        assert!(state.may_destroy(), "the first attempt is always allowed");
        state.consecutive_failures = 1;
        assert!(
            !state.may_destroy(),
            "an unproven probe must not keep killing a daemon it cannot vouch for"
        );

        // A probe that has answered before has earned more attempts, but still
        // a bounded number: if a freshly launched daemon also reads unhealthy,
        // the likelier explanation is the probe, not an epidemic of wedges.
        state.round_trip_proven = true;
        assert!(state.may_destroy());
        state.consecutive_failures = DESTRUCTIVE_ATTEMPT_LIMIT;
        assert!(!state.may_destroy());

        // One healthy observation restores everything, which is the only thing
        // that does.
        state.consecutive_failures = 0;
        assert!(state.may_destroy());
    }

    /// A launch that succeeds and then dies is a failed recovery.
    ///
    /// Counting only immediate spawn errors let a spawn-then-exit loop reset its
    /// own streak on every attempt, so a service could churn forever without
    /// ever reaching backoff or `recoveryFailed`. The streak is therefore
    /// advanced by the attempt and cleared only by health.
    #[test]
    fn repeated_spawn_then_exit_reaches_backoff_and_recovery_failed() {
        let mut state = BuildServiceRuntimeDiagnostic::default();
        let mut waits = Vec::new();
        for attempt in 1..=RECOVERY_FAILED_AFTER {
            // Each tick: the probe reads unhealthy (the daemon launched last
            // time is already gone), so another attempt is counted.
            state.consecutive_failures = state.consecutive_failures.saturating_add(1);
            waits.push(restart_backoff(state.consecutive_failures, 0.0));
            // The launch itself SUCCEEDS every time, which is the case that used
            // to reset the streak.
            state.restart_count += 1;
            state.generation += 1;
            state.enter(
                if state.consecutive_failures >= RECOVERY_FAILED_AFTER {
                    "recoveryFailed"
                } else {
                    "restarting"
                },
                u64::from(attempt),
            );
        }
        assert_eq!(state.consecutive_failures, RECOVERY_FAILED_AFTER);
        assert_eq!(state.lifecycle.as_deref(), Some("recoveryFailed"));
        // Backoff genuinely grew rather than retrying at full rate forever.
        assert!(waits.windows(2).all(|pair| pair[1] > pair[0]), "{waits:?}");
        assert!(waits.last().unwrap() >= &(RESTART_BACKOFF_MIN * 8));
        // And it stopped destroying well before it gave up reporting. Once the
        // final scheduled wait expires, it must also stop launching rather than
        // producing the incident's five-minute loop forever.
        assert!(!state.may_destroy());
        assert!(recovery_exhausted(state.consecutive_failures));

        // Health, and only health, clears it.
        state.consecutive_failures = 0;
        state.round_trip_proven = true;
        state.enter("healthy", 99);
        assert!(state.may_destroy());
    }

    /// One machine, one reconciler.
    ///
    /// The measured deployment runs four runners against one shared daemon,
    /// because a service's port derives from `{home}` and not from a runner's
    /// own Cairn home. An in-process lock cannot see the other three.
    #[test]
    fn the_machine_wide_lock_admits_one_reconciler_and_releases_on_drop() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("state").join(".cairn-reconcile.lock");

        let owned = match lock_service_machine_wide(&path) {
            ReconcileOwnership::Owned(lock) => lock,
            _ => panic!("the first runner must take the lock"),
        };
        assert!(
            matches!(
                lock_service_machine_wide(&path),
                ReconcileOwnership::HeldElsewhere
            ),
            "a second runner must not reconcile the same service concurrently"
        );
        drop(owned);
        assert!(
            matches!(
                lock_service_machine_wide(&path),
                ReconcileOwnership::Owned(_)
            ),
            "the lock must be reusable once its holder is gone, or this runner \
             would stop reconciling after its first tick"
        );

        // A lock this host cannot use must not switch supervision off. It is a
        // coordination aid, and losing it costs coordination, not supervision.
        let blocked = temp.path().join("not-a-directory");
        std::fs::write(&blocked, "").unwrap();
        assert!(
            matches!(
                lock_service_machine_wide(&blocked.join("nested.lock")),
                ReconcileOwnership::Unavailable
            ),
            "an unusable lock must report itself, not masquerade as another owner"
        );

        // The lock is keyed on the daemon's own state directory, which every
        // runner on the machine resolves identically however its own Cairn home
        // is named -- that identity is the entire point.
        let cfg = default_sccache_service();
        let dev = Templates {
            cairn_home: PathBuf::from("/home/u/.cairn-dev-agent-cairn-1-builder-0"),
            ..templates()
        };
        assert_eq!(
            machine_lock_path(&cfg, &templates(), "sccache"),
            machine_lock_path(&cfg, &dev, "sccache")
        );
    }

    #[test]
    fn foreign_listener_is_never_terminated() {
        let temp = tempfile::tempdir().unwrap();
        let expected = temp.path().join("sccache");
        let foreign = temp.path().join("postgres");
        std::fs::write(&expected, "fake").unwrap();
        std::fs::write(&foreign, "fake").unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let control = FakeListenerControl::new(Some(listener), 99, foreign);
        let error = recover_listener_conflict(&control, &addr, &expected).unwrap_err();
        assert!(error.contains("refusing to terminate pid 99"));
        assert!(control.terminated.lock().unwrap().is_empty());
    }

    /// An address nothing can be listening on, by construction.
    ///
    /// The obvious alternative — bind an ephemeral port, drop the listener, and
    /// assert the address is now closed — is a race rather than a fixture. A
    /// released ephemeral port goes straight back into the pool, so any of the
    /// several thousand tests in this binary (or anything else on the machine)
    /// can bind it between the drop and the probe; the "closed" assertion then
    /// sees a live socket. Serializing the tests that used the trick did not
    /// close that window, because the port is a machine-wide resource and not a
    /// module-wide one. Port 1 is never handed out as an ephemeral port and
    /// cannot be bound without root, so a connect to it is refused every time.
    const CLOSED_ADDR: &str = "127.0.0.1:1";

    #[test]
    fn probe_health_tcp_liveness_healthy_when_listening_down_when_closed() {
        // A tcp-only probe (no round_trip): a listening port is Healthy, a closed
        // one is Down. The spawner is never called (no round-trip to run).
        let spawner = MockProcessSpawner::new();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        assert_eq!(
            probe_health(
                &spawner,
                &ReadyProbe::tcp(addr),
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
                &ReadyProbe::tcp(CLOSED_ADDR.to_string()),
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

    /// A round trip has to report what it DID, not just whether it passed.
    ///
    /// The three outcomes are three different stories about a daemon — it
    /// answered, it refused, it never answered — and collapsing them into a
    /// bool is exactly what left a healthy daemon misjudged as wedged for three
    /// weeks with nothing on record to contradict the verdict (CAIRN-3332).
    #[test]
    fn round_trip_reports_the_outcome_behind_its_verdict() {
        let mut spawner = MockProcessSpawner::new();
        spawner
            .expect_spawn()
            .returning(|_| Ok(Box::new(MockChildProcess::failing(1, "", 0))));
        let answered = run_round_trip(
            &spawner,
            &round_trip_cmd(),
            &HashMap::new(),
            Duration::from_secs(1),
        );
        assert!(answered.healthy());
        assert_eq!(
            answered.outcome,
            RoundTripOutcome::Exited {
                success: true,
                code: Some(0)
            }
        );
        assert!(answered.summary().contains("answered"));

        let mut spawner = MockProcessSpawner::new();
        spawner
            .expect_spawn()
            .returning(|_| Ok(Box::new(MockChildProcess::failing(1, "boom", 1))));
        let refused = run_round_trip(
            &spawner,
            &round_trip_cmd(),
            &HashMap::new(),
            Duration::from_secs(1),
        );
        assert!(!refused.healthy());
        assert_eq!(
            refused.outcome,
            RoundTripOutcome::Exited {
                success: false,
                code: Some(1)
            }
        );
        // Whatever the probe said about failing is retained, because that line
        // is the whole diagnosis.
        assert!(refused.diagnostic.contains("boom"), "{refused:?}");
        assert!(refused.summary().contains("status 1"));

        // A probe that never exits is the wedged signature: sccache's protocol
        // has no per-request timeout, so a listening-but-hung server blocks the
        // client's request read forever. It is killed at the Rust-enforced
        // deadline, with no reliance on a shell `timeout`.
        let mut spawner = MockProcessSpawner::new();
        spawner
            .expect_spawn()
            .returning(|_| Ok(Box::new(MockChildProcess::with_stdout(1, vec![]))));
        let hung = run_round_trip(
            &spawner,
            &round_trip_cmd(),
            &HashMap::new(),
            Duration::from_millis(40),
        );
        assert!(!hung.healthy());
        assert_eq!(hung.outcome, RoundTripOutcome::TimedOut);
        assert!(hung.elapsed >= Duration::from_millis(40));
        assert!(hung.summary().contains("never answered"));
    }

    /// The round trip's own stdout is the statistics sample. One probe answers
    /// both questions, so nothing else ever has to talk to the daemon.
    #[test]
    fn round_trip_carries_the_statistics_report_it_asked_for() {
        let report: Vec<String> = SAMPLE_STATS.lines().map(str::to_string).collect();
        let mut spawner = MockProcessSpawner::new();
        spawner
            .expect_spawn()
            .returning(move |_| Ok(Box::new(MockChildProcess::with_stdout(1, report.clone()))));
        let round_trip = run_round_trip(
            &spawner,
            &round_trip_cmd(),
            &HashMap::new(),
            Duration::from_secs(1),
        );
        let stats = parse_cache_stats(&round_trip.stdout).expect("the report must parse");
        assert_eq!(stats.compile_requests, 4_812);
        assert_eq!(stats.cache_hits, 4_101);
    }

    /// The real shape of an `sccache --show-stats` report, captured from
    /// sccache 0.15.0 on the machine this was diagnosed on.
    const SAMPLE_STATS: &str = "\
Compile requests                   4812
Compile requests executed           502
Cache hits                         4101
Cache misses                        502
Cache hits rate                   89.09 %
Cache timeouts                        0
Cache read errors                     1
Forced recaches                       0
Cache write errors                    2
Cache errors                          0
Compilations                        502
Compilation failures                  0
Non-cacheable compilations            0
Non-cacheable calls                 209
Cache location                  Local disk: \"/Users/u/.cache/sccache-cairn\"
Cache size                       12 GiB
Max cache size                       50 GiB
";

    /// The same report shape as [`SAMPLE_STATS`], from the daemon that produced
    /// CAIRN-3355: it answers instantly, it has been asked for plenty, and every
    /// single compile it ran itself was destroyed. Counters are the real ones.
    const FAILING_STATS: &str = "\
Compile requests                   1056
Compile requests executed           230
Cache hits                            0
Cache misses                          0
Cache timeouts                        0
Cache read errors                     0
Forced recaches                       0
Cache write errors                    0
Cache errors                          2
Compilations                          0
Compilation failures                228
Non-cacheable compilations            0
Non-cacheable calls                 823
Cache location                  Local disk: \"/Users/u/.cache/sccache-cairn\"
Cache size                      438 MiB
Max cache size                       50 GiB
";

    /// A warm cache on a HEALTHY daemon, while the one crate under edit fails to
    /// compile. Hits do not increment `Compilations`, so the counters are the
    /// incident's shape exactly — and the daemon is fine.
    const WARM_CACHE_BROKEN_CRATE_STATS: &str = "\
Compile requests                    900
Compile requests executed             8
Cache hits                          850
Cache misses                          0
Cache timeouts                        0
Cache read errors                     0
Forced recaches                       0
Cache write errors                    0
Cache errors                          0
Compilations                          0
Compilation failures                  8
Non-cacheable compilations            0
Non-cacheable calls                  42
Cache location                  Local disk: \"/Users/u/.cache/sccache-cairn\"
Cache size                       12 GiB
Max cache size                       50 GiB
";

    /// A round trip that both ANSWERS (so liveness and the round trip pass) and
    /// carries a report. The stock helpers do one or the other.
    fn answering_with(report: &str) -> MockChildProcess {
        let lines: Vec<String> = report.lines().map(str::to_string).collect();
        let mut child = MockChildProcess::with_stdout(1, lines);
        child.set_exited();
        child
    }

    fn probe_for(addr: String) -> ReadyProbe {
        ReadyProbe {
            tcp: Some(addr),
            command: None,
            round_trip: Some(round_trip_cmd()),
        }
    }

    /// A capability site whose compile probe behaves as the caller chooses.
    ///
    /// The spawner sees three different invocations on the condemning path, and
    /// which one it is answering is decided by argv, because that is what
    /// distinguishes them in production too: the statistics round trip
    /// (`sccache --show-stats`), the probe compile (`sccache rustc …`), and the
    /// control compile (`rustc …`).
    fn spawner_where_the_probe(succeeds: bool, report: &'static str) -> MockProcessSpawner {
        let mut spawner = MockProcessSpawner::new();
        spawner.expect_spawn().returning(move |config| {
            if config.program == "rustc" {
                // The control: rustc compiles our own trivially valid source.
                return Ok(Box::new(MockChildProcess::failing(1, "", 0)));
            }
            if config.args.iter().any(|arg| arg == "--show-stats") {
                return Ok(Box::new(answering_with(report)));
            }
            Ok(Box::new(if succeeds {
                MockChildProcess::failing(1, "", 0)
            } else {
                MockChildProcess::failing(1, "error: Operation not permitted (os error 1)", 1)
            }))
        });
        spawner
    }

    /// A capability site rooted in a real directory, and its managed roots.
    ///
    /// The probe compiles into each managed build root, so the site has to be
    /// a place that can actually be created — with the fixture's `/home/u` it
    /// cannot, and the probe correctly declines to accuse anyone, which would
    /// make these tests pass for the wrong reason. The `TempDir` is returned so
    /// the caller keeps it alive for the duration of the assertion.
    fn capability_service() -> (
        BuildServiceConfig,
        Templates,
        Vec<String>,
        tempfile::TempDir,
    ) {
        let temp = tempfile::tempdir().unwrap();
        let templates = templates_in(temp.path());
        let roots = root_strings(&managed_build_roots(&templates));
        (default_sccache_service(), templates, roots, temp)
    }

    /// A daemon whose sandbox grant covers `granted` and nothing else — the
    /// partial grant that actually caused CAIRN-3355 and cairn/5556, rather
    /// than a daemon that fails everything.
    ///
    /// It answers the statistics round trip, refuses a compile whose `--out-dir`
    /// falls outside its grant, and compiles anything inside it. The control
    /// (`rustc` directly) always succeeds, which is what the real control does:
    /// the supervisor's probe spawns carry no sandbox, so the unconfined rustc
    /// can write where the confined daemon cannot.
    fn spawner_for_daemon_granted(granted: Vec<PathBuf>) -> MockProcessSpawner {
        let mut spawner = MockProcessSpawner::new();
        spawner.expect_spawn().returning(move |config| {
            if config.program == "rustc" {
                return Ok(Box::new(MockChildProcess::failing(1, "", 0)));
            }
            if config.args.iter().any(|arg| arg == "--show-stats") {
                return Ok(Box::new(answering_with(FAILING_STATS)));
            }
            let out = config
                .args
                .iter()
                .skip_while(|arg| *arg != "--out-dir")
                .nth(1)
                .cloned()
                .unwrap_or_default();
            Ok(Box::new(
                if granted.iter().any(|root| Path::new(&out).starts_with(root)) {
                    MockChildProcess::failing(1, "", 0)
                } else {
                    MockChildProcess::failing(1, "error: Operation not permitted (os error 1)", 1)
                },
            ))
        });
        spawner
    }

    /// The probe this replaces compiled into `~/.cache`, which every service
    /// sandbox can write through its toolchain grants, so it passed for a
    /// daemon granted nothing else — the daemon of cairn/5556, whose grant
    /// named the home while its builds had moved to a build volume. Each managed
    /// root is now its own experiment.
    #[test]
    fn a_daemon_granted_only_a_neutral_directory_is_unfit_for_every_managed_root() {
        let (cfg, templates, roots, _site) = capability_service();
        let spawner =
            spawner_for_daemon_granted(vec![canonical_path(&templates.home.join(".cache"))]);

        let report = probe_daemon_fitness(
            &spawner,
            &cfg,
            &templates,
            &HashMap::new(),
            &roots,
            Duration::from_secs(1),
        );

        assert_eq!(report.roots.len(), 2, "{report:?}");
        for root in &roots {
            match &report.roots[root] {
                RootFitness::Unfit { summary, detail } => {
                    assert_eq!(summary, "write refused");
                    assert!(detail.contains("Operation not permitted"), "{detail}");
                    assert!(detail.contains(root.as_str()), "{detail}");
                }
                other => panic!("{root} must be unfit, not {other:?}"),
            }
            assert!(capability_probe_dir(Path::new(root)).starts_with(root));
        }
        assert_eq!(
            report.daemon_unfit, None,
            "a missing root is not a broken daemon"
        );
    }

    /// Unfitness is per root: a daemon granted the worktrees but not the cells
    /// keeps serving the worktrees.
    #[test]
    fn a_daemon_granted_one_root_is_unfit_only_for_the_other() {
        let (cfg, templates, roots, _site) = capability_service();
        let worktrees = roots[0].clone();
        let cells = roots[1].clone();
        let spawner = spawner_for_daemon_granted(vec![PathBuf::from(&worktrees)]);

        let report = probe_daemon_fitness(
            &spawner,
            &cfg,
            &templates,
            &HashMap::new(),
            &roots,
            Duration::from_secs(1),
        );

        assert_eq!(report.roots[&worktrees], RootFitness::Proven);
        assert!(
            matches!(&report.roots[&cells], RootFitness::Unfit { summary, .. } if summary == "write refused"),
            "{report:?}"
        );
        let mut state = BuildServiceRuntimeDiagnostic {
            own_roots: roots.clone(),
            ..BuildServiceRuntimeDiagnostic::default()
        };
        state.observe_incarnation(Some((7, fake_token(7))), &roots);
        state.apply_fitness(report, 0);
        assert_eq!(state.routable_roots(), vec![worktrees]);
        assert_eq!(
            state.fitness(),
            vec![format!(
                "unfit for {cells} (write refused); clients compile directly"
            )]
        );
    }

    /// The verdict this whole change exists to make possible.
    ///
    /// The daemon is listening and answers `--show-stats` immediately, so every
    /// instrument that existed before this reads it as healthy. Its own report
    /// says it has run 228 compiles and finished none of them — and when asked to
    /// compile a trivially valid crate it cannot, while rustc compiles the same
    /// source directly without complaint. That is a process destroying the builds
    /// routed to it, which is exactly what it did for a day while the panel
    /// showed a green cache line (CAIRN-3355).
    #[test]
    fn a_daemon_proven_unable_to_compile_is_unfit_for_the_root() {
        let spawner = spawner_where_the_probe(false, FAILING_STATS);
        let (cfg, templates, roots, _site) = capability_service();

        let report = probe_daemon_fitness(
            &spawner,
            &cfg,
            &templates,
            &HashMap::new(),
            &roots[..1],
            Duration::from_secs(1),
        );

        match &report.roots[&roots[0]] {
            RootFitness::Unfit { detail, .. } => assert!(
                detail.contains("trivially valid"),
                "the reason must name the experiment that produced it: {detail}"
            ),
            other => panic!("expected an unfit root, not {other:?}"),
        }
    }

    /// The false positive the counters cannot see, and the probe settles.
    ///
    /// A warm cache serving every dependency from disk while the one crate under
    /// edit fails to compile produces the incident's exact counters. Statistics
    /// would condemn a working daemon here, kill a shared cache, and push every
    /// build on the machine onto uncached compiles — because someone's code did
    /// not build. The probe compiles fine, so nothing happens.
    #[test]
    fn a_working_daemon_survives_a_developer_iterating_on_a_broken_crate() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let probe = probe_for(listener.local_addr().unwrap().to_string());
        let spawner = spawner_where_the_probe(true, WARM_CACHE_BROKEN_CRATE_STATS);
        let control = FakeListenerControl::new(None, 1, PathBuf::from("/unused/sccache"));
        let (cfg, templates, roots, _site) = capability_service();

        let observation = assess_service(
            &spawner,
            &control,
            &probe,
            &HashMap::new(),
            Duration::from_secs(1),
            None,
        );
        assert_eq!(observation.health, ServiceHealth::Healthy);
        let report = probe_daemon_fitness(
            &spawner,
            &cfg,
            &templates,
            &HashMap::new(),
            &roots,
            Duration::from_secs(1),
        );
        assert!(
            report
                .roots
                .values()
                .all(|fitness| *fitness == RootFitness::Proven),
            "the daemon compiled the probe, so the failures are the code's and not its own: {report:?}"
        );
        assert_eq!(report.daemon_unfit, None);
    }

    /// A busy daemon is not a broken one. The probe compile waits in line behind
    /// the machine's own compiles, and one still waiting at the deadline says
    /// nothing about whether the daemon can compile, while rustc run directly,
    /// which waits for no one, succeeds. Reading that as unfit is what had the
    /// supervisor trying to kill a daemon in the middle of a release build.
    #[test]
    fn a_daemon_still_compiling_the_probe_at_the_deadline_is_not_condemned() {
        let mut spawner = MockProcessSpawner::new();
        spawner.expect_spawn().returning(|config| {
            Ok(Box::new(if config.program == "rustc" {
                MockChildProcess::failing(1, "", 0)
            } else {
                // Never exits: queued behind the compiles that hold every slot.
                MockChildProcess::with_stdout(1, vec![])
            }))
        });
        let (cfg, templates, roots, _site) = capability_service();
        let report = probe_daemon_fitness(
            &spawner,
            &cfg,
            &templates,
            &HashMap::new(),
            &roots[..1],
            Duration::from_millis(40),
        );
        assert!(
            matches!(report.roots[&roots[0]], RootFitness::Inconclusive(_)),
            "{report:?}"
        );
        assert_eq!(report.daemon_unfit, None);
    }

    /// Counters alone never condemn: with no capability site there is no
    /// experiment, and an unanswered question is not a verdict.
    #[test]
    fn counters_alone_never_condemn_a_daemon() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let probe = probe_for(listener.local_addr().unwrap().to_string());
        let mut spawner = MockProcessSpawner::new();
        spawner
            .expect_spawn()
            .returning(|_| Ok(Box::new(answering_with(FAILING_STATS))));
        // No listener reported, so the temp-dir check fails open and cannot be
        // what produces the verdict.
        let control = FakeListenerControl::new(None, 1, PathBuf::from("/unused/sccache"));

        let observation = assess_service(
            &spawner,
            &control,
            &probe,
            &HashMap::new(),
            Duration::from_secs(1),
            None,
        );

        assert_eq!(
            observation.health,
            ServiceHealth::Healthy,
            "counters alone may never condemn a daemon: they cannot tell a denied write \
             from a crate that does not compile"
        );
        assert_eq!(observation.unfit, None);
    }

    /// A build pointed at a daemon Cairn has watched fail is told so, and
    /// compiles directly rather than dying (see `scripts/cache-wrapper.sh`).
    ///
    /// The marker rides BESIDE the routing env rather than replacing it: cargo
    /// fingerprints `RUSTC_WRAPPER`, so degrading by withdrawing the wrapper
    /// would invalidate every crate in every managed build root and turn a cache
    /// outage into a machine-wide rebuild.
    #[test]
    fn an_unfit_daemon_costs_a_build_its_cache_hits_and_not_its_lane() {
        let unfit_daemon = Routing {
            daemon_unfit: true,
            ..fit_routing(&templates())
        };
        let unfit = client_env_for(
            &sccache_services(),
            &templates(),
            ClientBuild::Cell,
            &unfit_daemon,
        );
        assert_eq!(
            unfit.get(BUILD_SERVICE_UNFIT_ENV).map(String::as_str),
            Some("1")
        );
        assert_eq!(
            unfit.get("RUSTC_WRAPPER").map(String::as_str),
            Some("/home/u/.cairn/bin/cache-wrapper.sh"),
            "the wrapper must not move, or degrading would rebuild the world"
        );

        // A daemon proven for this build's root says nothing.
        let fit = client_env_for(
            &sccache_services(),
            &templates(),
            ClientBuild::Cell,
            &fit_routing(&templates()),
        );
        assert!(!fit.contains_key(BUILD_SERVICE_UNFIT_ENV));

        // A build no service claimed has nothing to be withdrawn from, and a
        // lone marker in an otherwise empty env would say something false.
        let unclaimed = client_env_for(
            &sccache_services(),
            &templates(),
            ClientBuild::At(Path::new("/home/u/projects/cairn")),
            &unfit_daemon,
        );
        assert!(unclaimed.is_empty(), "{unclaimed:?}");
    }

    /// A statistics report is parsed by exact label, and a response that is not
    /// one produces no sample at all.
    ///
    /// Both halves matter. `Cache hits` and `Cache hits rate` differ only by
    /// suffix, so prefix matching would publish a percentage as a count. And a
    /// probe that answered with something unrecognizable must yield `None` —
    /// a row of zeros would render a working cache as one serving nothing.
    #[test]
    fn cache_statistics_parse_by_exact_label_or_not_at_all() {
        let stats = parse_cache_stats(SAMPLE_STATS).expect("a real report must parse");
        assert_eq!(stats.compile_requests, 4_812);
        assert_eq!(
            stats.cache_hits, 4_101,
            "the rate line must not be read here"
        );
        assert_eq!(stats.cache_misses, 502);
        assert_eq!(stats.non_cacheable, 209);
        // Read, write, and general errors are summed: any of them is the cache
        // failing at its job.
        assert_eq!(stats.cache_errors, 3);
        assert_eq!(stats.cache_size_bytes, Some(12 * 1024 * 1024 * 1024));
        assert_eq!(stats.max_cache_size_bytes, Some(50 * 1024 * 1024 * 1024));
        assert_eq!(stats.hit_rate(), Some(4_101.0 / 4_603.0));
        // What the daemon ran ITSELF, and how that went. `Compilations` is
        // sccache's label for the SUCCEEDING ones, and reading it as a total is
        // how a daemon failing everything reads as one doing plenty.
        assert_eq!(stats.compiles_executed, 502);
        assert_eq!(stats.compilations, 502);
        assert_eq!(stats.compile_failures, 0);

        let failing = parse_cache_stats(FAILING_STATS).expect("a real report must parse");
        assert_eq!(failing.compiles_executed, 230);
        assert_eq!(failing.compilations, 0);
        assert_eq!(failing.compile_failures, 228);

        for unusable in [
            "",
            "sccache: error: Address already in use (os error 48)",
            "Cache hits                         4101",
        ] {
            assert_eq!(
                parse_cache_stats(unusable),
                None,
                "a response with no compile-request count is not a sample: {unusable:?}"
            );
        }
    }

    /// The comparison that made a wedged daemon permanently unrecoverable.
    ///
    /// macOS reports a bare-invoked process's executable as a bare name, and
    /// canonicalizing that against the runner's cwd can never equal the resolved
    /// launch path — so Cairn refused to act on its own daemon once a minute for
    /// three weeks while builds silently went uncached.
    #[test]
    fn executable_identity_matches_a_bare_name_without_matching_a_stranger() {
        let expected = Path::new("/opt/homebrew/Cellar/sccache/0.15.0/bin/sccache");
        assert!(
            same_executable(Path::new("sccache"), expected),
            "a bare name is the whole of what `ps -o comm=` reports for our own daemon"
        );
        assert!(!same_executable(Path::new("postgres"), expected));
        // A relative path carrying a directory proves neither identity nor
        // difference, so it is refused rather than guessed at.
        assert!(!same_executable(Path::new("bin/sccache"), expected));
        assert!(!same_executable(Path::new(""), expected));
        // An absolute observation is still compared strictly: a stranger that
        // merely shares a file name is never a match.
        assert!(!same_executable(
            Path::new("/usr/local/bin/sccache"),
            expected
        ));
    }

    /// Backoff is capped, monotonic, and spread.
    ///
    /// The spread is not cosmetic: a developer machine really does run several
    /// runners at once, each supervising the SAME shared daemon on one port, and
    /// without jitter their retries converge into a synchronized launch storm.
    #[test]
    fn restart_backoff_grows_to_a_ceiling_and_is_spread() {
        assert_eq!(restart_backoff(1, 0.0), RESTART_BACKOFF_MIN);
        assert_eq!(restart_backoff(2, 0.0), RESTART_BACKOFF_MIN * 2);
        assert_eq!(restart_backoff(3, 0.0), RESTART_BACKOFF_MIN * 4);
        // Capped, and it stays capped however long the outage runs.
        assert_eq!(restart_backoff(20, 0.0), RESTART_BACKOFF_MAX);
        assert_eq!(restart_backoff(u32::MAX, 0.0), RESTART_BACKOFF_MAX);
        // Jitter only ever adds, and never more than half again, so a retry
        // cannot be delayed unboundedly by it.
        assert!(restart_backoff(1, 1.0) > restart_backoff(1, 0.0));
        assert!(restart_backoff(20, 1.0) <= RESTART_BACKOFF_MAX.mul_f64(1.5));
        assert_eq!(restart_backoff(1, f64::NAN), RESTART_BACKOFF_MIN);
    }

    /// A human byte size as sccache prints it, and nothing it does not.
    #[test]
    fn byte_sizes_parse_only_when_the_unit_is_understood() {
        assert_eq!(parse_bytes("50 GiB"), Some(50 * 1024 * 1024 * 1024));
        assert_eq!(parse_bytes("349 MiB"), Some(349 * 1024 * 1024));
        assert_eq!(parse_bytes("12"), Some(12));
        assert_eq!(parse_bytes("1.5 KiB"), Some(1_536));
        assert_eq!(parse_bytes("unknown"), None);
        assert_eq!(parse_bytes("12 parsecs"), None);
    }

    /// A diagnostic is bounded at the seam, so unbounded daemon output can never
    /// become unbounded retained state. The tail, not the head: the last thing a
    /// failing process said is the part that names the failure.
    #[test]
    fn diagnostics_are_bounded_to_their_tail_on_a_character_boundary() {
        assert_eq!(bounded_tail("  boom  "), "boom");
        let long = format!("{}THE ACTUAL ERROR", "\u{e9}".repeat(DIAGNOSTIC_BYTES));
        let bounded = bounded_tail(&long);
        assert!(bounded.len() <= DIAGNOSTIC_BYTES + 4);
        assert!(bounded.ends_with("THE ACTUAL ERROR"));
        assert!(bounded.starts_with('\u{2026}'));
    }

    #[test]
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
    fn probe_health_down_when_port_closed_skips_round_trip() {
        // A closed port short-circuits to Down without spawning the round-trip —
        // gating the round-trip behind liveness is what prevents it from ever
        // auto-starting a server. The mock spawner has no expectations, so a spawn
        // would panic.
        let probe = ReadyProbe {
            tcp: Some(CLOSED_ADDR.to_string()),
            command: None,
            round_trip: Some(round_trip_cmd()),
        };
        let spawner = MockProcessSpawner::new();
        assert_eq!(
            probe_health(&spawner, &probe, &HashMap::new(), Duration::from_secs(1)),
            ServiceHealth::Down
        );
    }

    /// The advisory is the ONLY half of this snapshot an agent reads, so it
    /// answers to two rules: it appears only when the service can actually
    /// explain a failure, and it names none of the supervision detail the
    /// operator log carries in full.
    #[test]
    fn agent_advisory_speaks_only_for_a_currently_unhealthy_service() {
        let recovered = BuildServiceDiagnosticSnapshot {
            name: "sccache".into(),
            configured: true,
            enabled: true,
            supervised_child: false,
            config_fingerprint: Some("current".into()),
            state_dir: Some("/Users/someone/.cairn/sccache".into()),
            error_log_tail: Some("sccache: error: Address already in use (os error 48)".into()),
            runtime: BuildServiceRuntimeDiagnostic {
                last_health: Some("healthy".into()),
                failure_config: Some("current".into()),
                current_failure: None,
                ..BuildServiceRuntimeDiagnostic::default()
            },
        };
        // A healthy daemon explains nothing, however loud its historical log is.
        assert_eq!(recovered.agent_advisory(), None);
        assert!(recovered.error_log_tail.is_some());

        let mut wedged = recovered.clone();
        wedged.runtime.last_health = Some("wedged".into());
        let advisory = wedged.agent_advisory().expect("a wedged daemon advises");
        assert!(advisory.contains("sccache"));
        assert!(advisory.contains("not your change"));
        for operator_only in [
            "supervisedChild",
            "lastHealth",
            "configured=",
            "/Users/",
            "Address already in use",
        ] {
            assert!(
                !advisory.contains(operator_only),
                "advisory must not carry {operator_only}: {advisory}"
            );
        }

        let mut failing = recovered.clone();
        failing.runtime.current_failure = Some("sccache: error: Address already in use".into());
        assert!(failing.agent_advisory().is_some());

        // A disabled service is not part of the story at all.
        let mut disabled = failing.clone();
        disabled.enabled = false;
        assert_eq!(disabled.agent_advisory(), None);
    }

    #[test]
    fn cache_hit_read_failure_is_unfit() {
        let calls = std::sync::Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let mut spawner = MockProcessSpawner::new();
        spawner.expect_spawn().returning(move |config| {
            if config.program == "rustc" {
                return Ok(Box::new(MockChildProcess::failing(1, "", 0)));
            }
            if config.args.iter().any(|arg| arg == "--show-stats") {
                return Ok(Box::new(answering_with(SAMPLE_STATS)));
            }
            Ok(Box::new(if observed.fetch_add(1, Ordering::SeqCst) == 0 {
                MockChildProcess::failing(1, "", 0)
            } else {
                MockChildProcess::failing(1, "memory map must have a non-zero length", 1)
            }))
        });
        let (cfg, templates, roots, _site) = capability_service();
        let report = probe_daemon_fitness(
            &spawner,
            &cfg,
            &templates,
            &HashMap::new(),
            &roots[..1],
            Duration::from_secs(1),
        );
        // The write succeeded, so the root is proven; serving what it wrote
        // failed, which is about the daemon and not the root.
        assert_eq!(report.roots[&roots[0]], RootFitness::Proven);
        let reason = report
            .daemon_unfit
            .expect("a failed hit condemns the daemon");
        assert!(
            reason.contains("serve the cache entry it had just written"),
            "{reason}"
        );
        assert!(
            reason.contains("memory map must have a non-zero length"),
            "{reason}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn unresolved_corruption_survives_a_healthy_generic_probe() {
        let reason = "compile-cache corruption reported: corrupt archive".to_string();
        let mut state = BuildServiceRuntimeDiagnostic {
            last_health: Some("wedged".into()),
            last_probe: Some(reason.clone()),
            current_failure: Some(reason.clone()),
            stats_gap: Some(reason.clone()),
            unfit: Some(reason.clone()),
            unresolved_corruption: true,
            ..BuildServiceRuntimeDiagnostic::default()
        };

        // The healthy branch runs this exact transition after its fresh-nonce
        // capability probe. That success is not evidence about the corrupt key.
        assert!(!state.clear_unfit_after_healthy_probe(99));

        assert_eq!(state.unfit.as_deref(), Some(reason.as_str()));
        assert_eq!(state.current_failure.as_deref(), Some(reason.as_str()));
        assert_eq!(state.lifecycle.as_deref(), Some("degraded"));
    }

    #[test]
    fn unrelated_quarantined_entry_does_not_resolve_reported_corruption() {
        let mut state = BuildServiceRuntimeDiagnostic {
            unfit: Some("compile-cache corruption reported: corrupt archive".into()),
            // A zero-length entry was removed, but the evidence did not identify
            // it as the key the failed compile attempted to read.
            unresolved_corruption: true,
            ..BuildServiceRuntimeDiagnostic::default()
        };

        assert!(!state.clear_unfit_after_healthy_probe(99));
        assert!(state.unfit.is_some());
    }

    #[test]
    fn corruption_quarantine_removes_only_sharded_empty_entries() {
        let root = tempfile::tempdir().unwrap();
        let shard = root.path().join("cache").join("ab").join("cd");
        std::fs::create_dir_all(&shard).unwrap();
        let corrupt = shard.join("corrupt");
        let healthy = shard.join("healthy");
        let metadata = root.path().join(".cairn-reconcile.lock");
        std::fs::write(&corrupt, []).unwrap();
        std::fs::write(&healthy, b"archive").unwrap();
        std::fs::write(&metadata, []).unwrap();

        assert_eq!(
            quarantine_zero_length_cache_entries(root.path()),
            vec![corrupt.clone()]
        );
        assert!(!corrupt.exists());
        assert!(healthy.exists());
        assert!(metadata.exists());
    }

    async fn test_orchestrator(config_dir: &Path) -> Orchestrator {
        let local = LocalDb::open_migrated_for_test(config_dir.join("build-services.db"))
            .await
            .unwrap();
        let search = Arc::new(SearchIndex::open_or_create(config_dir.join("search")).unwrap());
        Orchestrator::builder(
            Arc::new(DbState::new(Arc::new(local), search)),
            Arc::new(TestServicesBuilder::new().build()),
            config_dir.to_path_buf(),
        )
        .build()
    }

    /// Settings naming one compile-cache service whose launch program is an
    /// absolute path, which [`service_on_path`] reads as installed without
    /// consulting PATH.
    ///
    /// The state directory is pinned inside `config_dir` because the
    /// machine-wide reconcile lock lives beside it. Without one,
    /// [`machine_lock_path`] falls back to the real user home, where the lock
    /// is shared with a running Cairn and with every other test process.
    fn write_sccache_settings(config_dir: &Path, enabled: bool) {
        let state_dir = config_dir.join("sccache-state").display().to_string();
        std::fs::write(
            crate::config::settings::get_settings_path(config_dir),
            format!(
                "buildServices:\n  sccache:\n    enabled: {enabled}\n    stateDir: {state_dir:?}\n    start:\n      - /opt/sccache\n"
            ),
        )
        .unwrap();
    }

    /// The substrate-health snapshot is rebuilt on every fleet change, so the
    /// compile-cache section of it must be assembled from what the supervisor
    /// published and nothing else. Establishing it independently costs a
    /// settings parse and a `which` subprocess, which is what made
    /// `get_substrate_health` a ~30ms blocking call (CAIRN-4397).
    ///
    /// Pinned by contradiction: the observation says the service is missing
    /// while the settings file on disk says it is installed. A read that went
    /// and found out for itself could not report `NotInstalled`.
    #[tokio::test]
    async fn compile_cache_health_answers_from_the_published_observation() {
        let config_dir = tempfile::tempdir().unwrap();
        write_sccache_settings(config_dir.path(), true);
        let orch = test_orchestrator(config_dir.path()).await;

        *orch.compile_cache_service.write().unwrap() = Some(ObservedCompileCacheService {
            name: "sccache".to_string(),
            enabled: true,
            installed: false,
        });

        let health = orch
            .compile_cache_health()
            .expect("a configured compile cache must be reported");
        assert_eq!(health.service, "sccache");
        assert_eq!(health.state, CompileCacheState::NotInstalled);

        // And the read survives the settings file it never consults.
        std::fs::remove_file(crate::config::settings::get_settings_path(
            config_dir.path(),
        ))
        .unwrap();
        assert_eq!(
            orch.compile_cache_health().map(|health| health.state),
            Some(CompileCacheState::NotInstalled)
        );
    }

    /// Reconciliation republishes before it can bail out. A host with no service
    /// sandbox, or one whose reconcile lock is already held, still changed
    /// nothing about what is configured and installed — so a panel must not be
    /// left reporting a settings state that has since been edited away.
    ///
    /// The held reconcile lock is the reachable early return here: it is the one
    /// that does not depend on the host.
    #[tokio::test]
    async fn reconcile_publishes_what_it_observed_before_it_can_bail_out() {
        let config_dir = tempfile::tempdir().unwrap();
        write_sccache_settings(config_dir.path(), true);
        let orch = test_orchestrator(config_dir.path()).await;

        let reconciling = orch.build_service_reconcile.lock().unwrap();
        orch.ensure_build_services_ready();
        assert_eq!(
            orch.observed_compile_cache_service(),
            Some(ObservedCompileCacheService {
                name: "sccache".to_string(),
                enabled: true,
                installed: true,
            })
        );

        write_sccache_settings(config_dir.path(), false);
        orch.ensure_build_services_ready();
        drop(reconciling);
        assert_eq!(
            orch.observed_compile_cache_service()
                .map(|service| service.enabled),
            Some(false),
            "a settings edit must reach the panel through the next reconcile"
        );
        assert_eq!(
            orch.compile_cache_health().map(|health| health.state),
            Some(CompileCacheState::Disabled)
        );
    }

    /// The non-blocking boundary, pinned from the other side: a read must not
    /// establish the observation it is missing.
    ///
    /// Settings on disk name a compile cache, and this orchestrator has never
    /// reconciled. Reading reports nothing — not because there is nothing to
    /// report, but because finding out would mean parsing that file and resolving
    /// its launch program on PATH, which shells out. A read that repaired its own
    /// gap would make "a read does no I/O" contingent on some earlier call having
    /// paid for it, so the property is written as an absolute here.
    ///
    /// Real hosts do not sit in this state: every runner and non-inert server
    /// reconciles during startup. What this fixes in place is the boot gap.
    #[tokio::test]
    async fn a_read_never_establishes_the_observation_it_is_missing() {
        let config_dir = tempfile::tempdir().unwrap();
        write_sccache_settings(config_dir.path(), true);
        let orch = test_orchestrator(config_dir.path()).await;

        assert_eq!(orch.compile_cache_health(), None);
        assert_eq!(
            orch.observed_compile_cache_service(),
            None,
            "reading must leave the observation unestablished, not fill it in"
        );

        // The same settings, once reconciliation has actually looked at them.
        // Publication precedes every early return, so holding the reconcile
        // lock stops this pass right after it publishes. Unheld, whether the
        // pass goes on to launch the service depends on the host's sandbox,
        // whether this process is itself confined, and who holds the
        // machine-wide lock, none of which this test is about.
        let reconciling = orch.build_service_reconcile.lock().unwrap();
        orch.ensure_build_services_ready();
        drop(reconciling);
        assert_eq!(
            orch.compile_cache_health().map(|health| health.service),
            Some("sccache".to_string())
        );
    }

    #[test]
    fn a_record_for_another_pid_or_start_token_is_not_this_daemons_grant() {
        let dir = tempfile::tempdir().unwrap();
        let t = templates();
        let cfg = default_sccache_service();
        let own = root_strings(&managed_build_roots(&t));
        let control = FakeListenerControl::detached(42, PathBuf::from("/unused/sccache"));

        write_grant_record(
            dir.path(),
            &grant_record(41, own.clone(), &cfg, &t, "/other"),
        )
        .unwrap();
        match running_grant_verdict(&control, "127.0.0.1:1", &cfg, &t, dir.path()) {
            GrantVerdict::Stale(stale) => {
                assert!(
                    stale.reason.contains("not the listening pid 42"),
                    "{stale:?}"
                );
                assert_eq!(
                    stale.record, None,
                    "a record about another process covers nothing"
                );
            }
            covered => panic!("{covered:?}"),
        }

        // Same pid, but a different lifetime: the pid was reused.
        let mut reused = grant_record(42, own.clone(), &cfg, &t, "/other");
        reused.start_token = "start-earlier".into();
        write_grant_record(dir.path(), &reused).unwrap();
        assert!(matches!(
            running_grant_verdict(&control, "127.0.0.1:1", &cfg, &t, dir.path()),
            GrantVerdict::Stale(GrantStale { record: None, .. })
        ));

        write_grant_record(dir.path(), &grant_record(42, own, &cfg, &t, "/other")).unwrap();
        assert!(matches!(
            running_grant_verdict(&control, "127.0.0.1:1", &cfg, &t, dir.path()),
            GrantVerdict::Covered(_)
        ));
    }

    #[test]
    fn no_record_means_stale() {
        let dir = tempfile::tempdir().unwrap();
        let control = FakeListenerControl::detached(42, PathBuf::from("/unused/sccache"));
        assert_eq!(
            running_grant_verdict(
                &control,
                "127.0.0.1:1",
                &default_sccache_service(),
                &templates(),
                dir.path()
            ),
            GrantVerdict::Stale(GrantStale {
                reason: "launch grant changed (no grant record)".into(),
                record: None,
            })
        );
    }

    /// Several runners share one daemon, each with its own home and build root.
    /// The grant is the union of every live runner's roots, and a daemon
    /// another runner launched from a different configuration is that runner's
    /// to replace — otherwise runners would relaunch each other's daemon in a
    /// loop.
    #[test]
    fn runners_sharing_a_daemon_converge_on_the_union_of_their_roots() {
        let dir = tempfile::tempdir().unwrap();
        let installed = templates();
        let dev = Templates {
            cairn_home: PathBuf::from("/home/u/.cairn-dev-x"),
            build_root: PathBuf::from("/Volumes/Builds/dev-x"),
            ..templates()
        };
        let installed_roots = root_strings(&managed_build_roots(&installed));
        let dev_roots = root_strings(&managed_build_roots(&dev));
        register_runner_roots(dir.path(), &dev, &dev_roots).unwrap();
        // A runner that has exited leaves its registration behind.
        let mut gone = std::process::Command::new("true").spawn().unwrap();
        let gone_pid = gone.id();
        gone.wait().unwrap();
        let stale_path = registration_path(dir.path(), Path::new("/home/u/.cairn-gone"));
        write_json_atomically(
            &stale_path,
            &RootRegistration {
                cairn_home: "/home/u/.cairn-gone".into(),
                roots: vec!["/gone/build-slots".into()],
                runner_pid: gone_pid,
                runner_start_token: Some("long-gone".into()),
                updated_at_unix_ms: 1,
            },
        )
        .unwrap();

        let union = live_registered_roots(dir.path(), &installed_roots);
        assert_eq!(&union[..installed_roots.len()], &installed_roots[..]);
        for root in &dev_roots {
            assert!(
                union.contains(root),
                "{union:?} lacks the live runner's {root}"
            );
        }
        assert!(!union.contains(&"/gone/build-slots".to_string()));
        assert!(
            !stale_path.exists(),
            "a gone runner's registration is pruned"
        );

        // The dev runner launched the daemon for the union, from a config the
        // installed runner does not share. The installed runner is covered.
        let mut dev_cfg = default_sccache_service();
        dev_cfg.write.push("/dev/only/**".into());
        let control = FakeListenerControl::detached(42, PathBuf::from("/unused/sccache"));
        write_grant_record(
            dir.path(),
            &grant_record(42, union, &dev_cfg, &dev, "/home/u/.cairn-dev-x"),
        )
        .unwrap();
        assert!(matches!(
            running_grant_verdict(
                &control,
                "127.0.0.1:1",
                &default_sccache_service(),
                &installed,
                dir.path()
            ),
            GrantVerdict::Covered(_)
        ));
        // The dev runner, whose own launch configuration this is, is covered too.
        assert!(matches!(
            running_grant_verdict(&control, "127.0.0.1:1", &dev_cfg, &dev, dir.path()),
            GrantVerdict::Covered(_)
        ));
    }

    /// A new incarnation has proven nothing, so it routes nothing until each
    /// root is proven; the incarnation it replaced keeps none of its verdicts.
    #[test]
    fn a_fresh_incarnation_routes_nothing_until_each_root_is_proven() {
        let roots = vec!["/r/worktrees".to_string(), "/r/build-slots".to_string()];
        let mut state = BuildServiceRuntimeDiagnostic {
            own_roots: roots.clone(),
            ..BuildServiceRuntimeDiagnostic::default()
        };
        state.observe_incarnation(Some((1, fake_token(1))), &roots);
        state.proven_roots = roots.clone();
        assert_eq!(state.routable_roots(), roots);

        state.observe_incarnation(Some((2, fake_token(2))), &roots);
        assert!(state.routable_roots().is_empty());
        assert_eq!(state.routing().roots, Vec::<String>::new());
        assert_eq!(state.roots_awaiting_probe(0), roots);
        assert_eq!(
            state.fitness(),
            vec![
                "fitness for /r/worktrees is not yet proven; clients compile directly".to_string(),
                "fitness for /r/build-slots is not yet proven; clients compile directly"
                    .to_string(),
            ]
        );

        state.apply_fitness(
            FitnessReport {
                roots: BTreeMap::from([(roots[0].clone(), RootFitness::Proven)]),
                daemon_unfit: None,
            },
            0,
        );
        assert_eq!(state.routable_roots(), vec![roots[0].clone()]);

        // An inconclusive root waits out its backoff, doubling each time.
        let inconclusive = || FitnessReport {
            roots: BTreeMap::from([(roots[1].clone(), RootFitness::Inconclusive("queued".into()))]),
            daemon_unfit: None,
        };
        state.apply_fitness(inconclusive(), 1_000);
        assert!(state.roots_awaiting_probe(1_000).is_empty());
        assert_eq!(state.roots_awaiting_probe(61_000), vec![roots[1].clone()]);
        state.apply_fitness(inconclusive(), 61_000);
        assert!(state.roots_awaiting_probe(61_000 + 119_999).is_empty());
        assert_eq!(
            state.roots_awaiting_probe(61_000 + 120_000),
            vec![roots[1].clone()]
        );
        assert_eq!(probe_retry_after(40), PROBE_RETRY_MAX);
    }

    /// A daemon on a real loopback port described by a fake listener control,
    /// a spawner that plays that daemon (launch, statistics round trip, probe
    /// compiles, and the direct-rustc control), and an orchestrator to drive
    /// the real reconcile with.
    pub(crate) struct DaemonHarness {
        _machine: tempfile::TempDir,
        config_dir: PathBuf,
        templates: Templates,
        cfg: BuildServiceConfig,
        control: Arc<FakeListenerControl>,
        launches: Arc<AtomicUsize>,
        probe_compiles: Arc<AtomicUsize>,
        orch: Orchestrator,
    }

    const LISTENING_PID: u32 = 4242;

    /// A harness whose runner keeps its cells under `build_root(machine)`.
    async fn daemon_harness(build_root: impl FnOnce(&Path) -> PathBuf) -> DaemonHarness {
        let machine = tempfile::tempdir().unwrap();
        let home = machine.path().join("home");
        std::fs::create_dir_all(&home).unwrap();
        let mut templates = templates_in(&home);
        templates.build_root = build_root(machine.path());
        let executable = home.join("sccache");
        std::fs::write(&executable, "fake").unwrap();
        let executable = std::fs::canonicalize(executable).unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let mut cfg = default_sccache_service();
        cfg.start = vec![executable.display().to_string()];
        cfg.ready.as_mut().unwrap().tcp = Some(addr.clone());
        cfg.state_dir = Some("{home}/state".into());
        let control = Arc::new(FakeListenerControl::new(
            Some(listener),
            LISTENING_PID,
            executable,
        ));
        let launches = Arc::new(AtomicUsize::new(0));
        let probe_compiles = Arc::new(AtomicUsize::new(0));
        let mut spawner = MockProcessSpawner::new();
        {
            let control = control.clone();
            let launches = launches.clone();
            let probe_compiles = probe_compiles.clone();
            spawner.expect_spawn().returning(move |config| {
                if config.program == "rustc" {
                    return Ok(Box::new(MockChildProcess::failing(1, "", 0)));
                }
                if config.args.iter().any(|arg| arg == "--show-stats") {
                    return Ok(Box::new(answering_with(SAMPLE_STATS)));
                }
                if config.args.first().is_some_and(|arg| arg == "rustc") {
                    probe_compiles.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::new(MockChildProcess::failing(1, "", 0)));
                }
                assert!(
                    config.env.contains_key("SCCACHE_START_SERVER"),
                    "unexpected spawn: {} {:?}",
                    config.program,
                    config.args
                );
                let launch = launches.fetch_add(1, Ordering::SeqCst) as u32 + 1;
                if control.is_listening() {
                    // The port is taken: the launch loses the bind race.
                    return Ok(Box::new(MockChildProcess::failing(
                        1_000 + launch,
                        "Address already in use",
                        1,
                    )));
                }
                control.relisten(&addr, 2_000 + launch);
                Ok(Box::new(MockChildProcess::with_stdout(
                    2_000 + launch,
                    vec![],
                )))
            });
        }
        let config_dir = machine.path().join("config");
        std::fs::create_dir_all(&config_dir).unwrap();
        crate::config::settings::upsert_build_service(&config_dir, "sccache", &cfg).unwrap();
        let orch = test_orchestrator_with(&config_dir, spawner).await;
        DaemonHarness {
            _machine: machine,
            config_dir,
            templates,
            cfg,
            control,
            launches,
            probe_compiles,
            orch,
        }
    }

    impl DaemonHarness {
        fn grant_dir(&self) -> PathBuf {
            grant_state_dir(&self.cfg, &self.templates, "sccache")
        }

        fn own_roots(&self) -> Vec<String> {
            root_strings(&managed_build_roots(&self.templates))
        }

        fn record(&self, pid: u32, roots: Vec<String>, launcher: &str) {
            write_grant_record(
                &self.grant_dir(),
                &grant_record(pid, roots, &self.cfg, &self.templates, launcher),
            )
            .unwrap();
        }

        fn own_launcher(&self) -> String {
            self.templates.cairn_home.display().to_string()
        }

        fn reconcile(&self, cfg: &BuildServiceConfig) {
            self.orch.reconcile_registered_service(
                &*self.control,
                "sccache",
                cfg,
                &self.templates,
                &self.own_roots(),
                vec![],
                true,
            );
        }

        fn runtime(&self) -> BuildServiceRuntimeDiagnostic {
            self.orch
                .build_service_runtime
                .lock()
                .unwrap()
                .get("sccache")
                .cloned()
                .unwrap_or_default()
        }

        fn cell_env(&self) -> HashMap<String, String> {
            client_env_for(
                &HashMap::from([("sccache".to_string(), self.cfg.clone())]),
                &self.templates,
                ClientBuild::Cell,
                &self.runtime().routing(),
            )
        }
    }

    async fn test_orchestrator_with(
        config_dir: &Path,
        spawner: MockProcessSpawner,
    ) -> Orchestrator {
        let local = LocalDb::open_migrated_for_test(config_dir.join("build-services.db"))
            .await
            .unwrap();
        let search = Arc::new(SearchIndex::open_or_create(config_dir.join("search")).unwrap());
        Orchestrator::builder(
            Arc::new(DbState::new(Arc::new(local), search)),
            Arc::new(TestServicesBuilder::new().with_process(spawner).build()),
            config_dir.to_path_buf(),
        )
        .build()
    }

    /// The daemon of cairn/5556: launched while cells lived in the home, still
    /// answering after the build root moved to a volume, with no grant there.
    async fn harness_with_a_moved_build_root() -> DaemonHarness {
        let harness = daemon_harness(|machine| machine.join("volume")).await;
        let old_roots = root_strings(&managed_build_roots(&Templates {
            build_root: harness.templates.cairn_home.clone(),
            ..harness.templates.clone()
        }));
        harness.record(LISTENING_PID, old_roots, "/home/u/.cairn-other");
        harness
    }

    /// The relaunch path, end to end through the real reconcile.
    #[tokio::test]
    #[serial_test::serial(build_service_port)]
    async fn a_daemon_recorded_with_an_old_build_root_is_relaunched_without_refusing_a_lane() {
        let harness = harness_with_a_moved_build_root().await;
        let cell_root = cell_build_root(&harness.templates).display().to_string();

        harness.reconcile(&harness.cfg);

        let runtime = harness.runtime();
        assert_eq!(runtime.current_failure, None);
        assert_eq!(runtime.failure_config, None);
        assert_eq!(
            runtime.last_restart_reason,
            Some(format!("launch grant changed (missing root {cell_root})"))
        );
        // Our launch lost the port, the stale listener was verified and
        // terminated, and the relaunch took the port.
        assert_eq!(harness.launches.load(Ordering::SeqCst), 2);
        assert_eq!(
            *harness.control.terminated.lock().unwrap(),
            vec![LISTENING_PID]
        );
        let record =
            read_grant_record(&harness.grant_dir()).expect("the relaunch records its grant");
        assert_eq!(record.pid, 2_002);
        assert_eq!(record.start_token, fake_token(2_002));
        assert!(record.roots.contains(&cell_root), "{record:?}");
        assert!(
            record
                .write_globs
                .contains(&target_grant_glob(Path::new(&cell_root))),
            "{record:?}"
        );
        assert_eq!(runtime.incarnation, Some((2_002, fake_token(2_002))));
        // A fresh incarnation routes nothing until it is proven.
        assert_eq!(
            harness
                .cell_env()
                .get(BUILD_SERVICE_UNFIT_ENV)
                .map(String::as_str),
            Some("1")
        );

        // The next tick finds the relaunched daemon covered and healthy.
        harness.reconcile(&harness.cfg);
        let runtime = harness.runtime();
        assert_eq!(runtime.lifecycle.as_deref(), Some("healthy"));
        assert_eq!(runtime.grant_stale, None);
        assert_eq!(runtime.consecutive_failures, 0);
    }

    /// The daemon after a no-destroy reconcile of a stale grant, for the
    /// check-dispatch test in `execution::checks`, which must see no failure.
    pub(crate) async fn stale_grant_reconciled_without_destroy(
    ) -> (BuildServiceDiagnosticSnapshot, DaemonHarness) {
        let harness = harness_with_a_moved_build_root().await;
        // What exhausted recovery leaves behind, in memory and on disk: the
        // failure refuses lanes until something says the daemon answers.
        let exhausted = "the service remained down after 5 consecutive launch attempts";
        write_recovery_failure(
            &harness.cfg,
            &harness.templates,
            RECOVERY_FAILED_AFTER,
            exhausted,
        )
        .unwrap();
        {
            let mut diagnostics = harness.orch.build_service_runtime.lock().unwrap();
            let state = diagnostics.entry("sccache".to_string()).or_default();
            state.round_trip_proven = true;
            state.consecutive_failures = RECOVERY_FAILED_AFTER;
            state.current_failure = Some(exhausted.to_string());
            state.failure_config = Some(service_config_fingerprint(&harness.cfg));
            state.enter("recoveryFailed", 1);
        }
        assert!(
            harness
                .orch
                .build_service_diagnostic_snapshot("sccache")
                .current_failure()
                .is_some(),
            "precondition: the exhausted recovery refuses lanes"
        );
        harness.reconcile(&harness.cfg);
        assert!(
            read_recovery_failure(&harness.cfg, &harness.templates).is_none(),
            "the persisted failure no longer describes an answering daemon"
        );
        let snapshot = harness.orch.build_service_diagnostic_snapshot("sccache");
        (snapshot, harness)
    }

    /// Where the daemon may not be replaced, it keeps serving the roots its
    /// record covers, builds under the rest compile directly, and nothing
    /// reaches `current_failure` — the field that refuses check dispatch.
    #[tokio::test]
    #[serial_test::serial(build_service_port)]
    async fn a_daemon_recorded_with_an_old_build_root_is_not_adopted_and_never_refuses_a_lane() {
        let (snapshot, harness) = stale_grant_reconciled_without_destroy().await;
        let cell_root = cell_build_root(&harness.templates).display().to_string();

        assert_eq!(harness.launches.load(Ordering::SeqCst), 0);
        assert!(harness.control.terminated.lock().unwrap().is_empty());
        let runtime = harness.runtime();
        assert_eq!(runtime.current_failure, None);
        assert_eq!(runtime.failure_config, None);
        assert_eq!(snapshot.current_failure(), None);
        assert_eq!(runtime.lifecycle.as_deref(), Some("degraded"));
        assert_eq!(runtime.last_health.as_deref(), Some("healthy"));
        let note = runtime
            .grant_stale
            .clone()
            .expect("the stale grant is named");
        assert_eq!(
            note.reason,
            format!("launch grant changed (missing root {cell_root})")
        );
        assert!(!note.relaunching);

        let env = harness.cell_env();
        assert_eq!(
            env.get(BUILD_SERVICE_UNFIT_ENV).map(String::as_str),
            Some("1")
        );
        let routed = env
            .get(BUILD_SERVICE_ROOTS_ENV)
            .cloned()
            .unwrap_or_default();
        assert!(!routed.split(':').any(|root| root == cell_root), "{routed}");
        assert!(runtime.fitness().contains(&format!(
            "{cell_root} is not in the running daemon's grant; clients compile directly"
        )));
    }

    /// The runner delivers the gate it relies on, and never downgrades a
    /// gated copy a newer checkout synced.
    #[test]
    fn the_runner_installs_a_gated_wrapper_and_keeps_a_newer_one() {
        assert!(BUNDLED_CACHE_WRAPPER.contains(WRAPPER_OUT_DIR_GATE));
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("bin").join("cache-wrapper.sh");
        assert!(install_cache_wrapper(&path).unwrap());
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            BUNDLED_CACHE_WRAPPER
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o755
            );
        }
        // An old wrapper is replaced...
        std::fs::write(&path, "#!/usr/bin/env bash\nexec \"$@\"\n").unwrap();
        assert!(install_cache_wrapper(&path).unwrap());
        assert!(wrapper_has_gate(&path));
        // ...a newer gated one is not.
        let newer = format!("#!/usr/bin/env bash\n# {WRAPPER_OUT_DIR_GATE}\n# newer\n");
        std::fs::write(&path, &newer).unwrap();
        assert!(!install_cache_wrapper(&path).unwrap());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), newer);
    }

    /// The upgrade path: a new runner beside a wrapper installed before the
    /// gate existed. That wrapper ignores the roots, so it is routed nothing
    /// and every build compiles directly, including a bare cargo build into a
    /// target dir outside the grant.
    #[test]
    fn routing_is_withheld_while_the_wrapper_lacks_the_gate() {
        let temp = tempfile::tempdir().unwrap();
        let wrapper = temp.path().join("cache-wrapper.sh");
        std::fs::write(&wrapper, "#!/usr/bin/env bash\nexec sccache \"$@\"\n").unwrap();
        let mut cfg = default_sccache_service();
        for key in WRAPPER_ENV_KEYS {
            cfg.env.insert(key.into(), wrapper.display().to_string());
        }
        let services = HashMap::from([("sccache".to_string(), cfg)]);
        let t = templates();
        let env_for = |routing: Routing| {
            let routing =
                routing_the_wrapper_can_enforce(routing, &merge_client_env(&services, &t));
            client_env_for(&services, &t, ClientBuild::Cell, &routing)
        };

        let old = env_for(fit_routing(&t));
        assert_eq!(
            old.get(BUILD_SERVICE_UNFIT_ENV).map(String::as_str),
            Some("1")
        );
        assert_eq!(
            old.get(BUILD_SERVICE_ROOTS_ENV).map(String::as_str),
            Some("")
        );

        std::fs::write(&wrapper, BUNDLED_CACHE_WRAPPER).unwrap();
        let gated = env_for(fit_routing(&t));
        assert!(!gated.contains_key(BUILD_SERVICE_UNFIT_ENV), "{gated:?}");

        // A missing wrapper enforces nothing either.
        std::fs::remove_file(&wrapper).unwrap();
        assert_eq!(
            env_for(fit_routing(&t))
                .get(BUILD_SERVICE_UNFIT_ENV)
                .map(String::as_str),
            Some("1")
        );
    }

    /// One probe per root per incarnation, not two compiles every tick.
    #[tokio::test]
    #[serial_test::serial(build_service_port)]
    async fn the_capability_probe_runs_once_per_incarnation() {
        let harness = daemon_harness(|machine| machine.join("volume")).await;
        for root in harness.own_roots() {
            std::fs::create_dir_all(root).unwrap();
        }
        harness.record(LISTENING_PID, harness.own_roots(), &harness.own_launcher());

        for _ in 0..3 {
            harness.reconcile(&harness.cfg);
        }
        // A miss per root, and the hit once.
        assert_eq!(harness.probe_compiles.load(Ordering::SeqCst), 3);
        let runtime = harness.runtime();
        assert_eq!(runtime.lifecycle.as_deref(), Some("healthy"));
        assert_eq!(runtime.routing().roots, harness.own_roots());
        assert!(runtime.fitness().is_empty(), "{:?}", runtime.fitness());
        assert!(!harness.cell_env().contains_key(BUILD_SERVICE_UNFIT_ENV));
        assert_eq!(harness.launches.load(Ordering::SeqCst), 0);

        // Another process on the same port is another incarnation.
        harness.control.set_pid(5_000);
        harness.record(5_000, harness.own_roots(), &harness.own_launcher());
        harness.reconcile(&harness.cfg);
        assert_eq!(harness.probe_compiles.load(Ordering::SeqCst), 6);
        harness.reconcile(&harness.cfg);
        assert_eq!(harness.probe_compiles.load(Ordering::SeqCst), 6);
    }

    /// Settings are reloaded on every reconcile, so editing what a running
    /// daemon was launched with relaunches it on the next one. Returns whether
    /// `change` relaunched the daemon, and the reason recorded for it.
    async fn reconcile_after_settings_change(
        change: impl FnOnce(&mut BuildServiceConfig),
    ) -> (bool, Option<String>) {
        let harness = daemon_harness(|machine| machine.join("volume")).await;
        harness.record(LISTENING_PID, harness.own_roots(), &harness.own_launcher());
        let mut changed = harness.cfg.clone();
        change(&mut changed);
        crate::config::settings::upsert_build_service(&harness.config_dir, "sccache", &changed)
            .unwrap();
        let reloaded = crate::config::settings::load_build_services(&harness.config_dir)
            .remove("sccache")
            .expect("the upserted service");
        assert_eq!(reloaded, changed);
        harness.reconcile(&reloaded);
        let relaunched = harness.launches.load(Ordering::SeqCst) > 0;
        if relaunched {
            let record = read_grant_record(&harness.grant_dir()).unwrap();
            assert_eq!(
                record.launch_fingerprint,
                reloaded.launch_fingerprint(&harness.templates)
            );
            assert_eq!(harness.runtime().current_failure, None);
        }
        (relaunched, harness.runtime().last_restart_reason)
    }

    #[tokio::test]
    #[serial_test::serial(build_service_port)]
    async fn a_settings_upsert_that_changes_write_relaunches_the_service() {
        let (relaunched, reason) =
            reconcile_after_settings_change(|cfg| cfg.write.push("/opt/extra/**".into())).await;
        assert!(relaunched);
        assert_eq!(
            reason.as_deref(),
            Some("launch fingerprint changed (write)")
        );

        let (relaunched, reason) =
            reconcile_after_settings_change(|cfg| cfg.start.push("--foreground".into())).await;
        assert!(relaunched);
        assert_eq!(
            reason.as_deref(),
            Some("launch fingerprint changed (start)")
        );

        let (relaunched, reason) = reconcile_after_settings_change(|cfg| {
            cfg.launch_env.insert("SCCACHE_LOG".into(), "debug".into());
        })
        .await;
        assert!(relaunched);
        assert_eq!(
            reason.as_deref(),
            Some("launch fingerprint changed (launchEnv)")
        );

        // Client env is not what the daemon was launched with.
        let (relaunched, reason) = reconcile_after_settings_change(|cfg| {
            cfg.env.insert("SCCACHE_CACHE_SIZE".into(), "60G".into());
        })
        .await;
        assert!(!relaunched);
        assert_eq!(reason, None);
    }
}
