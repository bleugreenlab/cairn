//! Managed Build Services supervisor.
//!
//! Lifecycle for the Cairn-owned build-service daemons declared in settings
//! (see `config::build_services` and `docs/worktree-fence.md`): launch each
//! enabled service under its **service sandbox**, health-check it, relaunch a
//! dead/unreachable one, and expose the merged client env injected into spawns
//! that build inside a managed build root. sccache is the first configured
//! instance.
//!
//! The core logic lives in free functions that take a `&dyn ProcessSpawner` and
//! pure config, so it is unit-testable without a full `Orchestrator`; the
//! `Orchestrator` methods are thin wrappers that read settings and hold the
//! launcher handles.

use std::collections::HashMap;
use std::io::Read;
use std::net::{TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::time::Duration;

use cairn_common::executor_protocol::{
    CompileCacheHealth, CompileCacheState, CompileCacheStats, Measurement, MeasurementGap,
};

use crate::config::build_services::{
    builds_in_managed_root, BuildServiceConfig, ReadyProbe, Templates, BUILD_SERVICE_UNFIT_ENV,
};
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

/// Bound startup reconciliation after a daemon launch. A foreground service
/// should either become healthy or exit with its bind error almost immediately.
const STARTUP_RECONCILE_TIMEOUT: Duration = Duration::from_secs(2);

/// Floor and ceiling of the relaunch backoff.
///
/// A compile cache is worth retrying often and forever, and never worth a
/// launch storm: each failed sccache launch appends to the daemon's own error
/// log, which is how one such log reached 5.4 MB of a single repeated line
/// (CAIRN-3332).
const RESTART_BACKOFF_MIN: Duration = Duration::from_secs(5);
const RESTART_BACKOFF_MAX: Duration = Duration::from_secs(300);

/// Consecutive failed launches past which the condition stops being an ordinary
/// retry and becomes a named, operator-visible `recoveryFailed`. Recovery keeps
/// being attempted at the backoff ceiling; what changes is that the failure is
/// stated rather than left to be inferred from builds getting slower.
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

/// How much of a probe's stdout is read. The sccache report is roughly 1.5 KiB;
/// this leaves generous headroom while keeping the read bounded.
const PROBE_OUTPUT_BYTES: u64 = 16_384;

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
fn install_cache_wrapper(cairn_home: &Path) -> std::io::Result<PathBuf> {
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
fn build_service_spawn_config(
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
    // absent from `merge_client_env`, so it never leaks into client tooling.
    for (k, v) in cfg.expanded_launch_env(templates) {
        config = config.env(&k, &v);
    }
    Some(config)
}

/// The env vars that name a process's temp directory. A daemon keeps the values
/// it was launched with for its entire life, which is why a build service pins
/// them explicitly instead of inheriting them (see `default_sccache_service`).
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

/// Launch one service daemon via the spawner under its service sandbox.
///
/// Creates the temp directories the launch env pins first: the daemon inherits
/// those values for life, and sccache's server aborts a compile outright when the
/// path is missing, so the directory must exist before the process does. Sandbox
/// confinement means the daemon generally cannot create it itself.
fn launch_service(
    spawner: &dyn ProcessSpawner,
    cfg: &BuildServiceConfig,
    templates: &Templates,
    deny_read: Vec<PathBuf>,
) -> Result<Box<dyn ChildProcess>, String> {
    let config = build_service_spawn_config(cfg, templates, deny_read)
        .ok_or_else(|| "build service has an empty start command".to_string())?;
    for dir in daemon_temp_dirs(cfg, templates) {
        if let Err(e) = std::fs::create_dir_all(&dir) {
            log::warn!("create build service temp dir {dir:?}: {e}");
        }
    }
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
        };
    }
    let Some(cmd) = &probe.round_trip else {
        // Live, and no round-trip configured: liveness is all we can assert.
        return ServiceObservation {
            health: ServiceHealth::Healthy,
            round_trip: None,
            unfit: None,
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

fn tcp_reachable(addr: &str) -> bool {
    match addr.to_socket_addrs() {
        Ok(mut addrs) => addrs
            .next()
            .map(|a| TcpStream::connect_timeout(&a, TCP_PROBE_TIMEOUT).is_ok())
            .unwrap_or(false),
        Err(_) => false,
    }
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
}

struct OsListenerProcessControl;

impl ListenerProcessControl for OsListenerProcessControl {
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
        let output = crate::env::command("lsof")
            .args(["-nP", &format!("-iTCP:{port}"), "-sTCP:LISTEN", "-Fpn"])
            .output()
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
    let output = crate::env::command("ps")
        .args(["eww", "-p", &pid.to_string(), "-o", "command="])
        .output()
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

/// How many compiles the daemon must have run ITSELF before "not one of them
/// succeeded" is evidence about the daemon rather than about the code.
///
/// The counters cannot say WHY a compile the daemon ran returned non-zero:
/// sccache scores a denied output write and a genuine type error identically
/// (measured — a server-side `Operation not permitted` reaches the client as
/// exit 1, byte-identical to a compiler error; see `scripts/cache-wrapper.sh`).
/// What separates them is the company they keep. A real cargo build compiles
/// dozens of registry dependencies before it ever reaches code anyone edited,
/// so a daemon that is working produces successes long before it accumulates
/// this many failures, whatever state the tree is in. A floor this low is
/// crossed within seconds of a real breakage; a floor at all is what keeps two
/// hand-run broken compiles from accusing a healthy daemon.
const UNSERVICEABLE_COMPILE_FLOOR: u64 = 8;

/// Whether the daemon's own counters say it cannot do the one thing only it can
/// do, with the sentence that says so.
///
/// A cache miss is not served from disk — the DAEMON runs that compile, in its
/// own process, under its own sandbox grant and its own temp directory. That is
/// the only work whose success depends on the daemon's environment rather than
/// on the build's, and it is therefore the only work whose failure a build can
/// do nothing about. When every one of those fails, the daemon is not a slow
/// cache; it is a process that takes compiles and destroys them, and it does it
/// while answering `--show-stats` in eleven milliseconds.
///
/// This is what "healthy" failed to mean for a whole day: a daemon launched by a
/// runner two days older than the grant fix held the port, failed 228 of the 230
/// compiles it executed, and reported HEALTHY throughout, because liveness was
/// the entire predicate (CAIRN-3355). Liveness is a claim about a socket.
/// Health has to be a claim about work.
///
/// Deliberately silent about partial failure. A daemon that serves one Cairn
/// home and denies another has successes, so it does not raise this and its
/// counters are left to an operator to read — which is why
/// [`CompileCacheStats::compile_failures`] is reported on the panel whether or
/// not this fires.
///
/// **This is a question, not a verdict.** The counters cannot tell a denied
/// output write from a genuine compiler error, and one realistic shape produces
/// exactly this reading on a perfectly healthy daemon: a warm cache where every
/// dependency HITS (hits do not increment `compilations`) while the one crate
/// being edited misses, executes, and fails to compile because its source is
/// broken. Iterate on that crate eight times and a working daemon looks like the
/// incident. Cache hits do not rescue the distinction either — measured, a hit's
/// outputs are written by the CLIENT, so a hit proves nothing about whether the
/// daemon can write anywhere at all. Only [`prove_daemon_cannot_compile`] can
/// answer what this asks.
fn compiles_look_unserviceable(stats: &CompileCacheStats) -> bool {
    stats.compile_failures >= UNSERVICEABLE_COMPILE_FLOOR && stats.compilations == 0
}

/// Where the capability probe compiles to.
///
/// Inside a managed build root, because the daemon's grant over those roots is
/// the thing under test; anywhere else would prove something nobody asked. The
/// `target/` segment is load-bearing — the grant covers `build-slots/**/target/**`
/// and not the slot root.
///
/// **THIS RUNNER'S own build root, deliberately, and that is the whole scope of
/// the claim.** One daemon serves every Cairn home on the machine and its grant
/// can cover some while missing others — that asymmetry *is* CAIRN-3355, where a
/// daemon launched from a dev instance's home served that home perfectly and
/// denied every cell under the installed app's. A supervisor cannot answer "can
/// this daemon write everywhere", and should not try: this machine carries some
/// three hundred `~/.cairn*` homes, most belonging to instances that no longer
/// exist.
///
/// It does not need to. A runner's cells live under its own `cairnHome` by
/// construction (see [`MANAGED_BUILD_ROOTS`]), so this probe answers exactly the
/// question the runner has standing to ask: *can this daemon serve the builds I
/// am about to route to it?* That is the question gating both decisions it feeds
/// — whether to keep routing builds here, and whether to replace the daemon —
/// and it means the runner whose cells are being destroyed is always the runner
/// that detects it. In the incident the failing cells were the installed app's,
/// and the installed app was supervising throughout.
///
/// The corollary, stated because it is the limit: a runner is silent about a
/// home it does not build in. A daemon that serves this home and denies another
/// keeps its verdict here, and the other home's runner is the one that must
/// notice — which it will, by this same probe, unless it is too old to carry it.
fn capability_probe_dir(templates: &Templates) -> PathBuf {
    templates
        .cairn_home
        .join("build-slots")
        .join(".compile-cache-probe")
        .join("target")
}

/// A trivial compile is fast, but this runs on a loaded developer machine and a
/// cold rustc is not instant. Generous, because the cost of exceeding it is a
/// false accusation against a shared daemon.
const CAPABILITY_PROBE_DEADLINE: Duration = Duration::from_secs(60);

/// Ask the daemon to do the one thing only it can do, and watch whether it can.
///
/// Returns `Some(reason)` only when the daemon is PROVEN unable to compile.
///
/// This exists because the counters raise a question they cannot answer (see
/// [`compiles_look_unserviceable`]). Rather than sharpen a statistic until it
/// looks decisive, this performs the experiment: hand the daemon a crate whose
/// source is ours and trivially valid, ask it to write the output into a managed
/// build root, and see what happens. A genuine compiler error is impossible for
/// that source, so a failure is about the daemon and nothing else.
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
fn prove_daemon_cannot_compile(
    spawner: &dyn ProcessSpawner,
    cfg: &BuildServiceConfig,
    templates: &Templates,
    env: &HashMap<String, String>,
) -> Option<String> {
    let client = cfg.expanded_start(templates).into_iter().next()?;
    let dir = capability_probe_dir(templates);
    if let Err(e) = std::fs::create_dir_all(&dir) {
        log::debug!("compile cache capability probe: create {dir:?}: {e}");
        return None;
    }
    // A nonce makes every probe a cache MISS, which is the whole point: a hit
    // would be served without the daemon compiling anything, and compiling is
    // what is under test.
    let nonce = unix_ms();
    let source = dir.join(format!("probe_{nonce}.rs"));
    if let Err(e) = std::fs::write(&source, format!("pub const PROBE: u64 = {nonce};\n")) {
        log::debug!("compile cache capability probe: write {source:?}: {e}");
        return None;
    }

    let out = dir.join(format!("out_{nonce}"));
    let cleanup = || {
        let _ = std::fs::remove_file(&source);
        let _ = std::fs::remove_dir_all(&out);
    };
    if let Err(e) = std::fs::create_dir_all(&out) {
        log::debug!("compile cache capability probe: create {out:?}: {e}");
        cleanup();
        return None;
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

    let mut probe_env = env.clone();
    probe_env.insert("CARGO_INCREMENTAL".to_string(), "0".to_string());

    let mut through_daemon = vec![client, "rustc".to_string()];
    through_daemon.extend(rustc_args.iter().cloned());
    let attempt = run_round_trip(
        spawner,
        &through_daemon,
        &probe_env,
        CAPABILITY_PROBE_DEADLINE,
    );
    if attempt.healthy() {
        cleanup();
        return None;
    }

    let mut directly = vec!["rustc".to_string()];
    directly.extend(rustc_args);
    let control = run_round_trip(
        spawner,
        &directly,
        &HashMap::new(),
        CAPABILITY_PROBE_DEADLINE,
    );
    cleanup();
    if !control.healthy() {
        log::debug!(
            "compile cache capability probe is inconclusive: rustc could not compile the probe \
             source directly either ({})",
            control.summary()
        );
        return None;
    }

    Some(format!(
        "the daemon could not compile a trivially valid crate into {}, which rustc compiled \
         directly without complaint — so builds that miss this cache are being failed by it \
         rather than served: {}",
        out.display(),
        attempt.summary()
    ))
}

/// Health as the supervisor acts on it: the [`probe_health`] verdict, downgraded
/// to [`ServiceHealth::Wedged`] when the daemon that answered it cannot do its
/// work — it can no longer use its temp directory (see [`listener_temp_dir_live`])
/// or it cannot compile at all (see [`prove_daemon_cannot_compile`]). Recovery
/// for all of them is the same — kill and relaunch — because all of them leave a
/// listening daemon that fails the work it accepts.
///
/// Function is checked LAST and only on an otherwise-healthy daemon, so a
/// verdict already explained by a hung round trip is never re-explained by
/// counters that round trip failed to fetch.
///
/// `capability` is the site the compile probe may use, and passing `None`
/// withholds the probe entirely — for callers that must not run a compiler as a
/// side effect of being asked a question (the settings UI), and for tests. With
/// no site, counters alone never condemn anything: they cannot.
fn assess_service(
    spawner: &dyn ProcessSpawner,
    control: &dyn ListenerProcessControl,
    probe: &ReadyProbe,
    env: &HashMap<String, String>,
    deadline: Duration,
    capability: Option<(&BuildServiceConfig, &Templates)>,
) -> ServiceObservation {
    let mut observation = probe_service(spawner, probe, env, deadline);
    if observation.health != ServiceHealth::Healthy {
        return observation;
    }
    if let Some(addr) = probe.tcp.as_deref() {
        if !listener_temp_dir_live(control, addr) {
            observation.health = ServiceHealth::Wedged;
            return observation;
        }
    }
    // The round trip that just proved liveness also carries the counters, so
    // reading them costs nothing. They only ever raise the question; an
    // unparseable report raises none, and leaves the daemon alone.
    let suspect = observation
        .round_trip
        .as_ref()
        .and_then(|round_trip| parse_cache_stats(&round_trip.stdout))
        .is_some_and(|stats| compiles_look_unserviceable(&stats));
    if !suspect {
        return observation;
    }
    let Some((cfg, templates)) = capability else {
        return observation;
    };
    // Only an experiment can separate a daemon that cannot write from a crate
    // that does not compile, so the counters buy exactly one thing: permission
    // to spend a compile finding out.
    if let Some(reason) = prove_daemon_cannot_compile(spawner, cfg, templates, env) {
        observation.health = ServiceHealth::Wedged;
        observation.unfit = Some(reason);
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
    capability: Option<(&BuildServiceConfig, &Templates)>,
) -> ServiceHealth {
    assess_service(spawner, control, probe, env, deadline, capability).health
}

#[cfg(target_os = "linux")]
fn process_executable(pid: u32) -> Result<PathBuf, String> {
    std::fs::read_link(format!("/proc/{pid}/exe"))
        .map_err(|e| format!("resolve executable for listener pid {pid}: {e}"))
}

#[cfg(not(target_os = "linux"))]
fn process_executable(pid: u32) -> Result<PathBuf, String> {
    let output = crate::env::command("ps")
        .args(["-p", &pid.to_string(), "-o", "comm="])
        .output()
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

fn service_config_fingerprint(cfg: &BuildServiceConfig) -> String {
    serde_json::to_string(cfg).unwrap_or_else(|_| format!("{cfg:?}"))
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

fn reconcile_launched_service(
    spawner: &dyn ProcessSpawner,
    control: &dyn ListenerProcessControl,
    cfg: &BuildServiceConfig,
    templates: &Templates,
    deny_read: Vec<PathBuf>,
    may_destroy: bool,
) -> Result<Option<Box<dyn ChildProcess>>, String> {
    let mut child = launch_service(spawner, cfg, templates, deny_read.clone())?;
    let Some(probe) = cfg.ready.as_ref() else {
        return Ok(Some(child));
    };
    let client_env = cfg.expanded_env(templates);
    let deadline = std::time::Instant::now() + STARTUP_RECONCILE_TIMEOUT;
    loop {
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
                return match child.try_wait() {
                    Ok(Some(_)) => Ok(None),
                    Ok(None) | Err(_) => Ok(Some(child)),
                };
            }
            // Something is serving but cannot do work. Do not adopt it; fall
            // through to the replacement path below.
            break;
        }
        if matches!(child.try_wait(), Ok(Some(_))) || std::time::Instant::now() >= deadline {
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
    // taken on as-is and fail every compile routed to it. Only an unhealthy
    // listener whose executable is exactly the configured service binary is
    // eligible for termination; unrelated listeners are never killed.
    if assess_health(
        spawner,
        control,
        probe,
        &client_env,
        HEALTH_ROUND_TRIP_DEADLINE,
        // Adoption is exactly where the compile probe earns its cost: the daemon
        // being adopted was launched by some other process, with a grant this one
        // did not choose and cannot read. Taking on a daemon that cannot compile
        // is how a stale generation came to own the machine (CAIRN-3355).
        Some((cfg, templates)),
    ) == ServiceHealth::Healthy
    {
        return Ok(None);
    }
    // Terminating the listener is the irreversible step, so it is the one the
    // caller's confidence gates. Refusing leaves a possibly-working shared cache
    // in place and reports why, which is strictly better than destroying it on a
    // verdict this process cannot yet vouch for.
    if !may_destroy {
        return Err(format!(
            "a listener holds {addr} and this runner's health probe has not earned the \
             confidence to terminate it; leaving it in place"
        ));
    }
    recover_listener_conflict(control, addr, &expected)?;
    let reap_deadline = std::time::Instant::now() + CHILD_REAP_TIMEOUT;
    while tcp_reachable(addr) && std::time::Instant::now() < reap_deadline {
        std::thread::sleep(HEALTH_POLL_INTERVAL);
    }
    launch_service(spawner, cfg, templates, deny_read).map(Some)
}

/// The build a client env is being composed for.
///
/// Which builds may be pointed at the supervised daemon is decided by where they
/// write, because the daemon runs each cache-miss compile itself: a build whose
/// `target/` the service sandbox does not cover fails outright rather than
/// missing the cache. See `config::build_services::MANAGED_BUILD_ROOTS`.
#[derive(Debug, Clone, Copy)]
enum ClientBuild<'a> {
    /// A spawn with a known build directory, admitted only when that directory
    /// lies inside a managed build root.
    At(&'a Path),
    /// A command in an executor cell on this machine. A cell is materialized
    /// under `{cairnHome}/build-slots` by construction, so it is inside a
    /// managed root without a path test — which matters because the runner
    /// composing a cell request knows the cell's repository but not the absolute
    /// slot the executor will hand it. Its caller states the rest: not the
    /// project's live checkout, and placed on the colocated executor.
    Cell,
}

/// The client env every enabled service contributes to `build`, or nothing when
/// `build` may not be pointed at these daemons.
///
/// This decides only *whether* a build is told about the services, never *what*
/// they say: a service that claims its port against client auto-start declares
/// [`BUILD_SERVICE_CLIENT_ENV`] in its own env, because that claim is true of one
/// daemon rather than of build services in general.
fn client_env_for(
    services: &HashMap<String, BuildServiceConfig>,
    templates: &Templates,
    build: ClientBuild<'_>,
    unfit: bool,
) -> HashMap<String, String> {
    if let ClientBuild::At(build_dir) = build {
        if !builds_in_managed_root(templates, build_dir) {
            return HashMap::new();
        }
    }
    let mut env = merge_client_env(services, templates);
    // Only meaningful beside a daemon this build was actually pointed at; an
    // empty merge means no service claimed this build and there is nothing to
    // withdraw it from.
    if unfit && !env.is_empty() {
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
/// Everything here is bounded and runtime-only; none of it is persisted.
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
    /// Whether this process has ever seen the health round trip answer.
    ///
    /// Until it has, the probe is an untested instrument and its unhealthy
    /// verdicts are not evidence about the daemon. See
    /// [`BuildServiceRuntimeDiagnostic::may_destroy`].
    pub(crate) round_trip_proven: bool,
}

impl BuildServiceRuntimeDiagnostic {
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
        for (name, cfg) in self.launchable_services() {
            // Then serialize across processes. Without this the in-process lock
            // above only stops one runner from racing itself, while the four
            // runners this machine actually runs race each other for the same
            // port. Held for the whole reconcile, released on drop.
            let _owned =
                match lock_service_machine_wide(&machine_lock_path(&cfg, &templates, &name)) {
                    ReconcileOwnership::HeldElsewhere => {
                        log::debug!("build service '{name}' is being reconciled by another runner");
                        continue;
                    }
                    ReconcileOwnership::Owned(lock) => Some(lock),
                    // Losing the lock costs coordination, never supervision.
                    ReconcileOwnership::Unavailable => None,
                };
            self.reconcile_build_service(&name, &cfg, &templates, deny_read.clone());
        }
    }

    /// Drive one service one step toward health.
    ///
    /// The states are explicit because "unhealthy" is three different
    /// situations: a relaunch is scheduled and pending (`restarting`), the
    /// daemon is unreachable and this process cannot currently fix it
    /// (`degraded`), or launches have failed past the backoff ceiling
    /// (`recoveryFailed`). Only the last is a condition an operator must see,
    /// and it stays retryable forever -- giving up permanently would trade a
    /// slow build fabric for a dead one.
    fn reconcile_build_service(
        &self,
        name: &str,
        cfg: &BuildServiceConfig,
        templates: &Templates,
        deny_read: Vec<PathBuf>,
    ) {
        let now_ms = unix_ms();
        let may_destroy;
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
        let observation = match &cfg.ready {
            Some(probe) => assess_service(
                self.services.process.as_ref(),
                &OsListenerProcessControl,
                probe,
                &client_env,
                HEALTH_ROUND_TRIP_DEADLINE,
                Some((cfg, templates)),
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
            },
        };
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
            state.last_probe = observation.round_trip.as_ref().map(RoundTrip::summary);
            if observation.health == ServiceHealth::Healthy {
                // One round trip serves both purposes: the health verdict above
                // and the counters below. The UI never runs its own.
                match observation.round_trip.as_ref() {
                    Some(round_trip) => match parse_cache_stats(&round_trip.stdout) {
                        Some(stats) => {
                            state.stats = Some(stats);
                            state.stats_at_unix_ms = Some(now_ms);
                            state.stats_gap = None;
                        }
                        None => {
                            state.stats_gap = Some(
                                "the health round trip answered with no readable statistics report"
                                    .to_string(),
                            );
                        }
                    },
                    None => {
                        state.stats_gap =
                            Some("this service has no statistics round trip configured".to_string())
                    }
                }
                state.current_failure = None;
                state.failure_config = None;
                state.unfit = None;
                state.consecutive_failures = 0;
                state.next_attempt_unix_ms = None;
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
            if let Some(reason) = observation.unfit.as_deref() {
                log::warn!("build service '{name}' is unfit to compile: {reason}");
                state.current_failure = Some(bounded_tail(reason));
                state.failure_config = Some(service_config_fingerprint(cfg));
                state.unfit = Some(bounded_tail(reason));
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
        // Ensure the daemon's state dir exists before launch: sccache creates
        // its SCCACHE_ERROR_LOG file (under stateDir) before starting the
        // server, and a missing parent dir would fail that and take the whole
        // server down on a fresh machine.
        if let Some(dir) = cfg.expanded_state_dir(templates) {
            if let Err(e) = std::fs::create_dir_all(&dir) {
                log::debug!("create build service state dir {dir:?}: {e}");
            }
        }

        let launched = reconcile_launched_service(
            self.services.process.as_ref(),
            &OsListenerProcessControl,
            cfg,
            templates,
            deny_read,
            may_destroy,
        );
        let mut diagnostics = self.build_service_runtime.lock().unwrap();
        let state = diagnostics.entry(name.to_string()).or_default();
        match launched {
            Ok(child) => {
                let pid = child.as_ref().map(|child| child.id());
                match &child {
                    Some(_) => log::info!("started build service '{name}'"),
                    None => log::info!("adopted existing healthy build service '{name}'"),
                }
                state.current_failure = None;
                state.failure_config = None;
                // A new incarnation is not the old one's verdict. Its counters
                // start at zero, so the unfitness finding they produced retires
                // with them rather than condemning a daemon nothing has judged.
                state.unfit = None;
                state.restart_count += 1;
                // A new incarnation means the daemon's counters restarted at
                // zero. Retiring the sample with the generation is what keeps a
                // reset from reading as a collapse in hit rate.
                state.generation += 1;
                state.stats = None;
                state.stats_at_unix_ms = None;
                state.stats_gap = Some(
                    "the compile cache has just restarted and has not been sampled".to_string(),
                );
                state.supervised_pid = pid;
                state.launched_at_unix_ms = Some(unix_ms());
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
                if let Some(child) = child {
                    self.build_service_children
                        .lock()
                        .unwrap()
                        .insert(name.to_string(), child);
                }
            }
            Err(e) => {
                log::warn!("failed to start build service '{name}': {e}");
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
                            // No compile probe: this answers a settings screen,
                            // and rendering a list must never run a compiler.
                            None,
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
    /// Read-only and non-blocking by construction: it consumes what the
    /// supervisor already sampled and never probes, launches, or runs
    /// `--show-stats` itself. Sampling belongs to the supervisor alone, so the
    /// panel cannot be made slow (or made to auto-start a server) by the
    /// condition it exists to report.
    pub fn compile_cache_health(&self) -> Option<CompileCacheHealth> {
        let services = settings::load_build_services(&self.config_dir);
        let (name, cfg) = compile_cache_service(&services)?;
        let now_ms = unix_ms();
        let runtime = self
            .build_service_runtime
            .lock()
            .unwrap()
            .get(&name)
            .cloned()
            .unwrap_or_default();

        let (state, unsupervised) = if !cfg.enabled {
            (CompileCacheState::Disabled, None)
        } else if !sandbox::is_available() {
            // No service sandbox means Cairn deliberately runs no confined
            // daemon here and every build compiles uncached by design. That is a
            // configuration, not a fault, and must not read as one.
            (
                CompileCacheState::Disabled,
                Some("this host has no service sandbox, so builds compile uncached by design"),
            )
        } else if !service_on_path(&cfg) {
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

        let condition = match state {
            CompileCacheState::Healthy => None,
            CompileCacheState::Disabled | CompileCacheState::NotInstalled => {
                unsupervised.map(str::to_string)
            }
            _ => unsupervised
                .map(str::to_string)
                .or_else(|| runtime.current_failure.clone())
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
    /// daemon may write where that build writes (see
    /// [`crate::config::build_services::MANAGED_BUILD_ROOTS`]). Keying this on
    /// the fence instead left every build on an `allow`-dial workspace running
    /// plain `rustc` against no cache at all.
    pub(crate) fn build_service_client_env(&self, build_dir: &Path) -> HashMap<String, String> {
        let templates =
            settings::build_service_templates(&self.config_dir, Some(build_dir.to_path_buf()));
        let services = settings::load_build_services(&self.config_dir);
        let unfit = self.compile_cache_is_unfit(&services);
        client_env_for(&services, &templates, ClientBuild::At(build_dir), unfit)
    }

    /// Whether the supervisor has positively observed this machine's compile
    /// cache failing the compiles it accepts.
    ///
    /// Read from the same runtime record the panel reads, so what a build is
    /// told and what an operator sees can never disagree.
    fn compile_cache_is_unfit(&self, services: &HashMap<String, BuildServiceConfig>) -> bool {
        let Some((name, _)) = compile_cache_service(services) else {
            return false;
        };
        self.build_service_runtime
            .lock()
            .unwrap()
            .get(&name)
            .is_some_and(|runtime| runtime.unfit.is_some())
    }

    /// The client env for a command running in an executor cell **on this
    /// machine**.
    ///
    /// A cell is materialized under `{cairnHome}/build-slots` by construction, so
    /// it is inside a managed build root without a path test — which matters
    /// because the runner composing a cell request knows the cell's repository
    /// but not the absolute slot the executor will hand it. The caller states the
    /// two things this cannot see: that the batch is not running in the project's
    /// live checkout, and that it was placed on the colocated executor (this
    /// daemon answers on loopback and is named by this machine's paths).
    pub(crate) fn cell_build_service_client_env(&self) -> HashMap<String, String> {
        let services = settings::load_build_services(&self.config_dir);
        let unfit = self.compile_cache_is_unfit(&services);
        client_env_for(
            &services,
            &settings::build_service_templates(&self.config_dir, None),
            ClientBuild::Cell,
            unfit,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::build_services::{default_sccache_service, BUILD_SERVICE_CLIENT_ENV};
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
                    // The daemon's own pinned temp dir: a sibling of the state
                    // dir, so it needs a grant of its own.
                    "^/home/u/\\.cache/sccache-cairn-tmp/.*".to_string(),
                    "^/home/u/\\.cairn/worktrees/.*/target/.*".to_string(),
                    // Every Cairn home on the machine, not just this one: they
                    // all supervise the same daemon, and only one of them
                    // launched it.
                    "^/home/u/\\.cairn[^/]*/build-slots/.*/target/.*".to_string(),
                ]
            );
        }
    }

    struct FakeListenerControl {
        /// Held open only when a test needs the port to answer a real TCP probe.
        /// Binding an ephemeral port is not free of consequence here — see the
        /// serialization note below — so tests that only exercise this control
        /// use [`FakeListenerControl::detached`] and bind nothing.
        socket: std::sync::Mutex<Option<std::net::TcpListener>>,
        listening: std::sync::Mutex<bool>,
        process: ListenerProcess,
        environ: Result<HashMap<String, String>, String>,
        terminated: std::sync::Mutex<Vec<u32>>,
    }

    impl FakeListenerControl {
        fn new(socket: Option<std::net::TcpListener>, pid: u32, executable: PathBuf) -> Self {
            Self {
                listening: std::sync::Mutex::new(socket.is_some()),
                socket: std::sync::Mutex::new(socket),
                process: ListenerProcess { pid, executable },
                environ: Ok(HashMap::new()),
                terminated: std::sync::Mutex::new(Vec::new()),
            }
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
            Ok(self.listening.lock().unwrap().then(|| self.process.clone()))
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

    /// The regression this whole change is about: the compile cache is a
    /// performance mechanism and the fence is a supervision policy, so a build
    /// inside a managed root is pointed at the daemon with no fence in sight.
    /// Gating this on the fence left every build on an `allow`-dial workspace
    /// running plain `rustc` against no cache at all.
    #[test]
    fn a_build_inside_a_managed_root_is_pointed_at_the_daemon() {
        for build_dir in [
            "/home/u/.cairn/worktrees/CAIRN-1/src-tauri",
            "/home/u/.cairn/build-slots/CAIRN/slot-3",
        ] {
            let env = client_env_for(
                &sccache_services(),
                &templates(),
                ClientBuild::At(Path::new(build_dir)),
                false,
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
        }
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
            false,
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
        let env = client_env_for(&sccache_services(), &templates(), ClientBuild::Cell, false);
        assert_eq!(
            env.get("SCCACHE_SERVER_PORT").map(String::as_str),
            Some("4227")
        );
        assert_eq!(
            env.get(BUILD_SERVICE_CLIENT_ENV).map(String::as_str),
            Some("1")
        );
    }

    /// With nothing to attach to, a build must be told nothing at all: a stray
    /// port claim would make the wrapper skip the auto-started server that is the
    /// correct behavior where Cairn supervises no compile cache.
    #[test]
    fn no_enabled_service_names_no_daemon() {
        let mut disabled = default_sccache_service();
        disabled.enabled = false;
        let services = HashMap::from([("sccache".to_string(), disabled)]);

        assert!(client_env_for(&services, &templates(), ClientBuild::Cell, false).is_empty());
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

        let env = client_env_for(&services, &templates(), ClientBuild::Cell, false);
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
            })
            .returning(|_| Ok(Box::new(MockChildProcess::with_stdout(7, vec![]))));

        let child = launch_service(&spawner, &default_sccache_service(), &templates(), vec![])
            .expect("launch should succeed");
        assert_eq!(child.id(), 7);
    }

    /// Templates rooted at a real directory, so paths the launch path creates
    /// can be asserted on disk.
    fn templates_in(root: &Path) -> Templates {
        Templates {
            home: root.to_path_buf(),
            cairn_home: root.join(".cairn"),
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
        launch_service(&spawner, &cfg, &templates, vec![]).expect("launch should succeed");

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
        let child =
            reconcile_launched_service(&spawner, &control, &cfg, &templates(), vec![], true)
                .expect("healthy server should be adopted");
        assert!(child.is_none());
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
        let child =
            reconcile_launched_service(&spawner, &control, &cfg, &templates(), vec![], true)
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
        for id in [31, 32] {
            spawner
                .expect_spawn()
                .times(1)
                .in_sequence(&mut sequence)
                .returning(move |_| Ok(Box::new(MockChildProcess::failing(id, "", 0))));
        }
        spawner
            .expect_spawn()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|_| Ok(Box::new(MockChildProcess::with_stdout(33, vec![]))));

        let control = FakeListenerControl::new(Some(listener), 777, executable)
            .with_env("TMPDIR", &reclaimed);
        let child =
            reconcile_launched_service(&spawner, &control, &cfg, &templates(), vec![], true)
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
        let refused =
            match reconcile_launched_service(&spawner, &control, &cfg, &templates(), vec![], false)
            {
                Err(refused) => refused,
                Ok(_) => panic!("an unvouched verdict must not authorize termination"),
            };
        assert!(refused.contains("leaving it in place"), "{refused}");
        assert!(
            control.terminated.lock().unwrap().is_empty(),
            "a daemon this runner cannot vouch for was killed anyway"
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
        // And it stopped destroying well before it gave up reporting.
        assert!(!state.may_destroy());

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

    /// A capability site rooted in a real directory.
    ///
    /// The probe compiles into a managed build root, so the site has to be a
    /// place that can actually be created — with the fixture's `/home/u` it
    /// cannot, and the probe correctly declines to accuse anyone, which would
    /// make these tests pass for the wrong reason. The `TempDir` is returned so
    /// the caller keeps it alive for the duration of the assertion.
    fn capability_service() -> (BuildServiceConfig, Templates, tempfile::TempDir) {
        let temp = tempfile::tempdir().unwrap();
        let templates = templates_in(temp.path());
        (default_sccache_service(), templates, temp)
    }

    /// A daemon whose sandbox grant covers `granted` and nothing else — the
    /// cross-home partial grant that actually caused CAIRN-3355, rather than a
    /// daemon that fails everything.
    ///
    /// It answers the statistics round trip, refuses a compile whose `--out-dir`
    /// falls outside its grant, and compiles anything inside it. The control
    /// (`rustc` directly) always succeeds, which is what the real control does:
    /// the supervisor's probe spawns carry no sandbox, so the unconfined rustc
    /// can write where the confined daemon cannot.
    fn spawner_for_daemon_granted(granted: PathBuf) -> MockProcessSpawner {
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
            Ok(Box::new(if Path::new(&out).starts_with(&granted) {
                MockChildProcess::failing(1, "", 0)
            } else {
                MockChildProcess::failing(1, "error: Operation not permitted (os error 1)", 1)
            }))
        });
        spawner
    }

    /// The recurrence class, modelled exactly.
    ///
    /// One daemon, one partial grant: it can write the dev instance's home (the
    /// stale runner that launched it) and not the installed app's. Both
    /// supervisors see the same suspicious counters, and they are SUPPOSED to
    /// reach opposite verdicts, because they are asking about different builds.
    ///
    /// The runner inside the grant is not being lied to — its own cells really do
    /// compile — so condemning the daemon there would kill a cache that works for
    /// every build that runner routes. The runner outside it is the one whose
    /// cells are being destroyed, and it is the one that acts. That is the whole
    /// design: the supervisor that suffers the breakage is the supervisor that
    /// detects it.
    #[test]
    fn a_daemon_with_a_partial_grant_is_condemned_by_the_home_it_denies() {
        let machine = tempfile::tempdir().unwrap();
        let dev_home = machine.path().join("dev-instance");
        let app_home = machine.path().join("installed-app");
        let cfg = default_sccache_service();

        // The grant the stale runner compiled in: its own home's build slots.
        let granted = templates_in(&dev_home).cairn_home.join("build-slots");

        let verdict = |home: &Path| {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let probe = probe_for(listener.local_addr().unwrap().to_string());
            let spawner = spawner_for_daemon_granted(granted.clone());
            let control = FakeListenerControl::new(None, 1, PathBuf::from("/unused/sccache"));
            assess_service(
                &spawner,
                &control,
                &probe,
                &HashMap::new(),
                Duration::from_secs(1),
                Some((&cfg, &templates_in(home))),
            )
        };

        let inside = verdict(&dev_home);
        assert_eq!(
            inside.health,
            ServiceHealth::Healthy,
            "the daemon compiles this runner's own cells, so this runner has no grounds \
             to kill a cache that works for every build it routes"
        );

        let outside = verdict(&app_home);
        assert_eq!(
            outside.health,
            ServiceHealth::Wedged,
            "this runner's cells are the ones being destroyed, so this is the runner \
             that must detect it — the exact shape of CAIRN-3355"
        );
        assert!(outside.unfit.is_some());
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
    fn a_daemon_proven_unable_to_compile_is_not_healthy() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let probe = probe_for(listener.local_addr().unwrap().to_string());
        let spawner = spawner_where_the_probe(false, FAILING_STATS);
        let control = FakeListenerControl::new(None, 1, PathBuf::from("/unused/sccache"));
        let (cfg, templates, _site) = capability_service();

        let observation = assess_service(
            &spawner,
            &control,
            &probe,
            &HashMap::new(),
            Duration::from_secs(1),
            Some((&cfg, &templates)),
        );

        assert_eq!(observation.health, ServiceHealth::Wedged);
        let reason = observation
            .unfit
            .expect("the verdict must carry its reason");
        assert!(
            reason.contains("trivially valid"),
            "the reason must name the experiment that produced it: {reason}"
        );
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
        let (cfg, templates, _site) = capability_service();

        let observation = assess_service(
            &spawner,
            &control,
            &probe,
            &HashMap::new(),
            Duration::from_secs(1),
            Some((&cfg, &templates)),
        );

        assert_eq!(
            observation.health,
            ServiceHealth::Healthy,
            "the daemon compiled the probe, so the failures are the code's and not its own"
        );
        assert_eq!(observation.unfit, None);
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

    /// The other half: a working daemon is left alone. Same probe, same path,
    /// same instruments — only the counters differ.
    #[test]
    fn a_daemon_whose_compiles_succeed_stays_healthy() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let probe = probe_for(listener.local_addr().unwrap().to_string());
        let mut spawner = MockProcessSpawner::new();
        spawner
            .expect_spawn()
            .returning(|_| Ok(Box::new(answering_with(SAMPLE_STATS))));
        let control = FakeListenerControl::new(None, 1, PathBuf::from("/unused/sccache"));
        let (cfg, templates, _site) = capability_service();

        let observation = assess_service(
            &spawner,
            &control,
            &probe,
            &HashMap::new(),
            Duration::from_secs(1),
            Some((&cfg, &templates)),
        );
        assert_eq!(observation.health, ServiceHealth::Healthy);
        assert_eq!(observation.unfit, None);
        // Counters that raise no question spend no compile: the mock spawner
        // above answers everything with a statistics report, so a probe would
        // have been read as a successful compile and gone unnoticed. The point
        // is that it never ran.
    }

    /// What the counters may and may not be asked.
    ///
    /// They raise a question; they never answer one. sccache scores a denied
    /// output write and a genuine type error identically, so the only shape worth
    /// spending a probe on is "many run, none finished".
    #[test]
    fn the_counters_raise_the_question_only_where_it_is_worth_asking() {
        let with = |compilations, compile_failures| CompileCacheStats {
            compile_requests: 500,
            compiles_executed: compilations + compile_failures,
            compilations,
            compile_failures,
            ..CompileCacheStats::default()
        };

        assert!(compiles_look_unserviceable(&with(0, 228)));

        // A tree full of genuine compile errors still compiles its dependencies,
        // so successes accompany the failures and there is nothing to ask.
        assert!(!compiles_look_unserviceable(&with(140, 228)));

        // Below the floor, "none succeeded" is a sample size, not a question.
        assert!(!compiles_look_unserviceable(&with(
            0,
            UNSERVICEABLE_COMPILE_FLOOR - 1
        )));

        // A daemon serving purely from cache executes nothing, which is the
        // healthiest state there is — and the one a naive "zero compilations"
        // test would condemn.
        assert!(!compiles_look_unserviceable(&CompileCacheStats {
            compile_requests: 4_000,
            cache_hits: 4_000,
            ..CompileCacheStats::default()
        }));

        // THE FALSE POSITIVE THIS DESIGN EXISTS TO SURVIVE. A warm cache where
        // every dependency hits, while the one crate being edited misses and
        // fails to compile because its source is broken, produces the incident's
        // exact counters on a perfectly healthy daemon. The counters still raise
        // the question here — they cannot tell — which is precisely why raising
        // it may not condemn anything by itself.
        assert!(compiles_look_unserviceable(&CompileCacheStats {
            compile_requests: 900,
            cache_hits: 850,
            compiles_executed: 8,
            compilations: 0,
            compile_failures: 8,
            ..CompileCacheStats::default()
        }));
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
        let unfit = client_env_for(&sccache_services(), &templates(), ClientBuild::Cell, true);
        assert_eq!(
            unfit.get(BUILD_SERVICE_UNFIT_ENV).map(String::as_str),
            Some("1")
        );
        assert_eq!(
            unfit.get("RUSTC_WRAPPER").map(String::as_str),
            Some("/home/u/.cairn/bin/cache-wrapper.sh"),
            "the wrapper must not move, or degrading would rebuild the world"
        );

        // A healthy daemon says nothing, so the marker's presence is always a
        // positive observation rather than the absence of one.
        let fit = client_env_for(&sccache_services(), &templates(), ClientBuild::Cell, false);
        assert!(!fit.contains_key(BUILD_SERVICE_UNFIT_ENV));

        // A build no service claimed has nothing to be withdrawn from, and a
        // lone marker in an otherwise empty env would say something false.
        let unclaimed = client_env_for(
            &sccache_services(),
            &templates(),
            ClientBuild::At(Path::new("/home/u/projects/cairn")),
            true,
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
