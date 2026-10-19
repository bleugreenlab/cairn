//! Per-user OS-service registration, shared by the Cairn daemons that install
//! themselves on a machine.
//!
//! Two daemons need this: `cairn-orch` (the always-on local runner) and
//! `cairn-exec` (the resident executor a bootstrapped machine runs). Both want
//! the same thing from an operating system — a per-user, zero-elevation
//! autostart entry that survives logout and reboot — and all three platforms
//! express it differently: a launchd LaunchAgent plist, a systemd user unit, an
//! HKCU Run-key value. What differs between the two daemons is not the
//! mechanism but the [`ServiceSpec`]: the identity it registers under, the
//! command it launches, and how the service manager should supervise it.
//!
//! Only the pure unit *generators* ([`plist_contents`], [`systemd_unit_contents`],
//! [`windows_run_command`]) are compiled on every platform so their output can be
//! asserted cross-platform; the side-effectful register/unregister/query code is
//! `cfg`-gated to the platform that owns the relevant service manager and reached
//! through the [`sys`] alias.
//!
//! What is deliberately NOT here: liveness. Whether the registered process is
//! doing its job is a question only the daemon can answer (the runner holds a
//! single-owner lock and serves a health endpoint; the executor asserts presence
//! by heartbeat over its own link), so status probes live with their daemons.
//! What IS here is the service manager's own accounting of the process it
//! supervises — [`runtime`] and [`recent_log`] — because that is the manager's
//! fact, and the only witness to a daemon that died before it could say why.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
mod other;
#[cfg(target_os = "windows")]
mod windows;

// The active platform's service-manager backend. Every backend exposes the same
// surface: `install`, `restart`, `uninstall`, `is_installed`, `is_loaded`,
// `location`. macOS and Windows additionally expose a stale-registration
// self-heal, reached through the `cfg`-gated wrappers at the bottom of this file.
#[cfg(target_os = "linux")]
use linux as sys;
#[cfg(target_os = "macos")]
use macos as sys;
#[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
use other as sys;
#[cfg(target_os = "windows")]
use windows as sys;

/// Everything a generated unit needs. One spec produces the launchd plist, the
/// systemd unit, and the Windows Run-key command for a single daemon identity.
#[derive(Debug, Clone)]
pub struct ServiceSpec {
    /// launchd LaunchAgent label / reverse-DNS service id (macOS).
    pub label: String,
    /// systemd user unit file name, including the `.service` suffix (Linux).
    pub unit_name: String,
    /// HKCU Run-key value name (Windows).
    pub run_value: String,
    /// systemd `Description=`.
    pub description: String,
    /// Absolute path to the executable the unit launches.
    pub exe: PathBuf,
    /// Arguments passed to that executable.
    pub args: Vec<String>,
    /// Ordered environment pairs baked into the unit. A launchd/systemd service
    /// has no login shell, so anything the daemon needs from the environment
    /// must ride in the unit itself.
    pub env: Vec<(String, String)>,
    /// Directory for the service's stdout/stderr logs (launchd needs explicit
    /// paths; a shell-less agent has nowhere else to send them).
    pub log_dir: PathBuf,
    /// Basename for those logs, e.g. `orch` produces `orch.out.log`.
    pub log_prefix: String,
    /// `NumberOfFiles` / `LimitNOFILE`. Children inherit this ceiling.
    pub file_descriptor_limit: u32,
    /// How the service manager should react when the process exits.
    pub supervision: Supervision,
    /// A previous identity to stop and remove before registering this one.
    pub legacy: Option<LegacyIdentity>,
}

/// A pre-rename identity that must be evicted before the current one starts, so
/// two generations never run against the same data directory.
#[derive(Debug, Clone, Default)]
pub struct LegacyIdentity {
    pub label: Option<String>,
    pub unit_name: Option<String>,
    pub run_value: Option<String>,
}

impl LegacyIdentity {
    fn of(label: &str, unit_name: &str, run_value: &str) -> Self {
        Self {
            label: Some(label.to_string()),
            unit_name: Some(unit_name.to_string()),
            run_value: Some(run_value.to_string()),
        }
    }

    /// The name this platform's service manager knows it by.
    pub fn platform_name(&self) -> Option<&str> {
        if cfg!(target_os = "macos") {
            self.label.as_deref()
        } else if cfg!(windows) {
            self.run_value.as_deref()
        } else {
            self.unit_name.as_deref()
        }
    }
}

/// The runner's service identity (cairn/4377 onward).
pub const RUNNER_LABEL: &str = "computer.cairn.orch";
pub const RUNNER_UNIT: &str = "cairn-orch.service";
pub const RUNNER_RUN_VALUE: &str = "CairnOrch";
/// The identity the runner registered under before cairn/4377. On a machine
/// enrolled before the transport inversion it is also what supervised the
/// executor that machine ran, which is why the executor's own retirement keys
/// on it.
pub const PRE_RENAME_RUNNER_LABEL: &str = "computer.cairn.runner";
pub const PRE_RENAME_RUNNER_UNIT: &str = "cairn-runner.service";
pub const PRE_RENAME_RUNNER_RUN_VALUE: &str = "CairnRunner";

pub fn runner_identity() -> LegacyIdentity {
    LegacyIdentity::of(RUNNER_LABEL, RUNNER_UNIT, RUNNER_RUN_VALUE)
}

pub fn pre_rename_runner_identity() -> LegacyIdentity {
    LegacyIdentity::of(
        PRE_RENAME_RUNNER_LABEL,
        PRE_RENAME_RUNNER_UNIT,
        PRE_RENAME_RUNNER_RUN_VALUE,
    )
}

/// A process ended through a held handle, with the creation time it was
/// verified by.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminatedProcess {
    pub pid: u32,
    pub start_token: String,
}

/// What a registration says it launches, read from the registration itself.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UnitDescription {
    pub program: Option<PathBuf>,
    pub args: Vec<String>,
    /// The Cairn home it runs against: `CAIRN_HOME` in its environment, or
    /// `--data-dir` in its arguments.
    pub cairn_home: Option<PathBuf>,
}

/// A service registered on this machine under some identity, with the service
/// manager's account of its process.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RegisteredUnit {
    /// The identity's name on this platform.
    pub name: String,
    /// The registration file, where the platform keeps one.
    pub location: Option<PathBuf>,
    /// Its program, arguments, and home. Default when the registration is
    /// loaded but its file is gone.
    pub description: UnitDescription,
    pub runtime: Option<ServiceRuntime>,
    /// The literal registration value, where it is one (the Windows Run
    /// value), which is how the process it launched is recognized.
    pub command_line: Option<String>,
}

/// Split a command line into words, honoring double quotes. Backslashes are
/// literal, which is what both a systemd `ExecStart=` path and a Windows Run
/// value need.
fn split_command_line(line: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    let mut started = false;
    for ch in line.chars() {
        match ch {
            '"' => {
                quoted = !quoted;
                started = true;
            }
            ch if ch.is_whitespace() && !quoted => {
                if started {
                    words.push(std::mem::take(&mut current));
                    started = false;
                }
            }
            ch => {
                current.push(ch);
                started = true;
            }
        }
    }
    if started {
        words.push(current);
    }
    words
}

/// Read a launchd plist as `plutil -convert json -o - <plist>` renders it.
pub fn parse_launchd_plist_json(json: &str) -> Result<UnitDescription, String> {
    let value: serde_json::Value =
        serde_json::from_str(json).map_err(|error| format!("parse plist json: {error}"))?;
    let mut words: Vec<String> = value
        .get("ProgramArguments")
        .and_then(serde_json::Value::as_array)
        .map(|args| {
            args.iter()
                .filter_map(|arg| arg.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    let program = value
        .get("Program")
        .and_then(serde_json::Value::as_str)
        .map(PathBuf::from)
        .or_else(|| (!words.is_empty()).then(|| PathBuf::from(words.remove(0))));
    let cairn_home = value
        .get("EnvironmentVariables")
        .and_then(|env| env.get("CAIRN_HOME"))
        .and_then(serde_json::Value::as_str)
        .filter(|home| !home.is_empty())
        .map(PathBuf::from)
        .or_else(|| data_dir_argument(&words));
    Ok(UnitDescription {
        program,
        args: words,
        cairn_home,
    })
}

/// Read a systemd unit's `ExecStart=` and `Environment=CAIRN_HOME=`.
pub fn parse_systemd_unit(unit: &str) -> UnitDescription {
    let mut description = UnitDescription::default();
    let mut env_home = None;
    for line in unit.lines() {
        let line = line.trim();
        if let Some(command) = line.strip_prefix("ExecStart=") {
            // systemd's executable prefixes (`-`, `@`, `+`, `!`, `:`) are not
            // part of the path.
            let command = command.trim_start_matches(['-', '@', '+', '!', ':']);
            let mut words = split_command_line(command);
            if !words.is_empty() {
                description.program = Some(PathBuf::from(words.remove(0)));
            }
            description.args = words;
        } else if let Some(assignments) = line.strip_prefix("Environment=") {
            for assignment in split_command_line(assignments) {
                if let Some(home) = assignment.strip_prefix("CAIRN_HOME=") {
                    env_home =
                        Some(PathBuf::from(home)).filter(|home| !home.as_os_str().is_empty());
                }
            }
        }
    }
    description.cairn_home = env_home.or_else(|| data_dir_argument(&description.args));
    description
}

/// Read a Windows Run-key value: a quoted program path and its arguments.
pub fn parse_run_value(value: &str) -> UnitDescription {
    let mut words = split_command_line(value);
    let program = (!words.is_empty()).then(|| PathBuf::from(words.remove(0)));
    UnitDescription {
        program,
        cairn_home: data_dir_argument(&words),
        args: words,
    }
}

fn data_dir_argument(args: &[String]) -> Option<PathBuf> {
    args.iter().enumerate().find_map(|(index, arg)| {
        if arg == "--data-dir" {
            args.get(index + 1).map(PathBuf::from)
        } else {
            arg.strip_prefix("--data-dir=").map(PathBuf::from)
        }
    })
}

/// What the service manager does when the supervised process exits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Supervision {
    /// Restart even after a clean (status 0) exit. launchd expresses this as
    /// `KeepAlive: true` versus `KeepAlive: {SuccessfulExit: false}`.
    pub restart_after_clean_exit: bool,
    /// Seconds the manager waits before restarting a failed process
    /// (systemd `RestartSec`).
    pub restart_delay_secs: u64,
    /// launchd `ThrottleInterval`. `None` leaves launchd's 10-second default in
    /// place and omits the key entirely.
    pub launchd_throttle_interval_secs: Option<u64>,
}

impl Supervision {
    /// Always-on: any exit, clean or not, is restarted promptly. The shape of a
    /// daemon whose exit is never a considered decision.
    pub fn always_on() -> Self {
        Self {
            restart_after_clean_exit: true,
            restart_delay_secs: 5,
            launchd_throttle_interval_secs: None,
        }
    }

    /// Restart only after a failure, at one shared cadence both managers
    /// express: systemd's `RestartSec` and launchd's `ThrottleInterval` are set
    /// from the same number, so a daemon cannot be patient on one platform and
    /// frantic on the other.
    ///
    /// A clean exit is left alone, so `service uninstall`-less shutdowns and
    /// deliberate stops stay stopped.
    pub fn retry_on_failure(delay_secs: u64) -> Self {
        Self {
            restart_after_clean_exit: false,
            restart_delay_secs: delay_secs,
            launchd_throttle_interval_secs: Some(delay_secs),
        }
    }
}

impl ServiceSpec {
    fn exe_display(&self) -> String {
        self.exe.display().to_string()
    }

    /// The command line as a POSIX service manager wants it: program then
    /// arguments, space separated.
    fn command_line(&self) -> String {
        let mut out = self.exe_display();
        for arg in &self.args {
            out.push(' ');
            out.push_str(arg);
        }
        out
    }

    fn log_path(&self, stream: &str) -> String {
        self.log_dir
            .join(format!("{}.{stream}.log", self.log_prefix))
            .display()
            .to_string()
    }
}

// ─── Pure unit generators (compiled on every platform, asserted by tests) ────

/// launchd LaunchAgent property list. `RunAtLoad` plus the spec's `KeepAlive`
/// policy make it a per-user agent; the env block carries whatever the daemon
/// would otherwise have inherited from a login shell.
///
/// `ProcessType` is `Interactive`, never `Background`: launchd's darwinbg band
/// (throttled CPU scheduling + low-priority disk I/O) is inherited by every
/// child the daemon spawns — agent sessions, cargo builds, git, the sccache
/// daemon. These are interactive-latency execution engines, and under
/// `Background` those children measured glacial in prod (15-60s worktree
/// creation, 12s exec registration).
pub fn plist_contents(spec: &ServiceSpec) -> String {
    let mut env_block = String::new();
    if !spec.env.is_empty() {
        env_block.push_str("\t<key>EnvironmentVariables</key>\n\t<dict>\n");
        for (k, v) in &spec.env {
            env_block.push_str(&format!(
                "\t\t<key>{}</key>\n\t\t<string>{}</string>\n",
                xml_escape(k),
                xml_escape(v)
            ));
        }
        env_block.push_str("\t</dict>\n");
    }

    let mut program_args = format!("\t\t<string>{}</string>\n", xml_escape(&spec.exe_display()));
    for arg in &spec.args {
        program_args.push_str(&format!("\t\t<string>{}</string>\n", xml_escape(arg)));
    }

    let keep_alive = if spec.supervision.restart_after_clean_exit {
        "\t<key>KeepAlive</key>\n\t<true/>\n".to_string()
    } else {
        // A clean exit is a decision; only failures are restarted.
        "\t<key>KeepAlive</key>\n\t<dict>\n\t\t<key>SuccessfulExit</key>\n\t\t<false/>\n\t</dict>\n"
            .to_string()
    };
    let throttle = match spec.supervision.launchd_throttle_interval_secs {
        Some(secs) => format!("\t<key>ThrottleInterval</key>\n\t<integer>{secs}</integer>\n"),
        None => String::new(),
    };

    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n\
<plist version=\"1.0\">\n\
<dict>\n\
\t<key>Label</key>\n\
\t<string>{label}</string>\n\
\t<key>ProgramArguments</key>\n\
\t<array>\n\
{program_args}\
\t</array>\n\
{env_block}\
\t<key>RunAtLoad</key>\n\
\t<true/>\n\
{keep_alive}\
{throttle}\
\t<key>ProcessType</key>\n\
\t<string>Interactive</string>\n\
\t<key>SoftResourceLimits</key>\n\
\t<dict>\n\
\t\t<key>NumberOfFiles</key>\n\
\t\t<integer>{file_descriptor_limit}</integer>\n\
\t</dict>\n\
\t<key>HardResourceLimits</key>\n\
\t<dict>\n\
\t\t<key>NumberOfFiles</key>\n\
\t\t<integer>{file_descriptor_limit}</integer>\n\
\t</dict>\n\
\t<key>StandardOutPath</key>\n\
\t<string>{out}</string>\n\
\t<key>StandardErrorPath</key>\n\
\t<string>{err}</string>\n\
</dict>\n\
</plist>\n",
        label = xml_escape(&spec.label),
        program_args = program_args,
        env_block = env_block,
        keep_alive = keep_alive,
        throttle = throttle,
        file_descriptor_limit = spec.file_descriptor_limit,
        out = xml_escape(&spec.log_path("out")),
        err = xml_escape(&spec.log_path("err")),
    )
}

/// systemd user unit. `WantedBy=default.target` makes it a per-user service
/// enabled at login; the restart policy comes from the spec.
///
/// Deliberately absent: `RestartPreventExitStatus`. A daemon that exits refusing
/// its configuration is fixed by editing that configuration on the machine, and
/// a stopped unit would make picking the fix up require someone to log in and
/// start it again. Both managers therefore retry — slowly, on the cadence
/// [`Supervision::retry_on_failure`] sets — rather than latching off.
pub fn systemd_unit_contents(spec: &ServiceSpec) -> String {
    let mut env_lines = String::new();
    for (k, v) in &spec.env {
        env_lines.push_str(&format!("Environment=\"{k}={v}\"\n"));
    }
    format!(
        "[Unit]\n\
Description={description}\n\
Documentation=https://cairn.computer\n\
After=network-online.target\n\
Wants=network-online.target\n\
\n\
[Service]\n\
Type=simple\n\
ExecStart={command}\n\
{env_lines}\
LimitNOFILE={file_descriptor_limit}\n\
Restart=on-failure\n\
RestartSec={restart_delay}\n\
\n\
[Install]\n\
WantedBy=default.target\n",
        description = spec.description,
        command = spec.command_line(),
        env_lines = env_lines,
        file_descriptor_limit = spec.file_descriptor_limit,
        restart_delay = spec.supervision.restart_delay_secs,
    )
}

/// Windows per-user Run-key command value. Explorer evaluates this REG_SZ at
/// logon, giving the daemon the same zero-elevation autostart shape as a launchd
/// LaunchAgent or systemd user unit, but without Task Scheduler's admin-owned
/// task store.
///
/// Two consequences of that mechanism, both real limitations rather than parity
/// gaps to paper over: the Run key expresses NO restart policy at all (a crashed
/// daemon stays down until the next logon), and it carries no environment. A
/// daemon that needs configuration on Windows must therefore take it in
/// arguments, which is why the command is generated from [`ServiceSpec::args`]
/// and not from the env block.
///
/// Arguments containing a space or a path separator are quoted; bare
/// subcommands and option flags are not, which is exactly the shape Windows
/// command-line parsing needs.
pub fn windows_run_command(spec: &ServiceSpec) -> String {
    let mut out = format!("\"{}\"", spec.exe_display());
    for arg in &spec.args {
        out.push(' ');
        if arg.contains(' ') || arg.contains('/') || arg.contains('\\') {
            out.push('"');
            out.push_str(arg);
            out.push('"');
        } else {
            out.push_str(arg);
        }
    }
    out
}

/// Ordered systemd operations for a full install. The final restart is
/// load-bearing: `enable --now` does not replace an already-active generation.
pub fn systemd_install_steps(unit_name: &str) -> Vec<Vec<String>> {
    vec![
        vec!["daemon-reload".to_string()],
        vec![
            "enable".to_string(),
            "--now".to_string(),
            unit_name.to_string(),
        ],
        vec!["restart".to_string(), unit_name.to_string()],
    ]
}

/// What a service manager reports about the process it supervises.
///
/// The manager's account rather than the daemon's: it is what still speaks for a
/// daemon that crashed, panicked, or failed before it could record anything of
/// its own.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ServiceRuntime {
    /// The manager has a live process for the service right now.
    pub running: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    /// The status the supervised process last exited with, when it has exited
    /// since the service was loaded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_exit_status: Option<i32>,
    /// How many times the manager has started it again since it was loaded,
    /// when the manager counts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restarts: Option<u32>,
}

/// Read `launchctl print gui/<uid>/<label>`.
///
/// Only the job's own top-level keys are read; nested blocks (endpoints, event
/// triggers, the environment) reuse names like `state` for other things.
/// `runs` counts every spawn since the job was bootstrapped, so a job spawned
/// once has restarted zero times.
pub fn parse_launchctl_print(output: &str) -> ServiceRuntime {
    let mut runtime = ServiceRuntime::default();
    let mut depth = 0usize;
    for line in output.lines() {
        let line = line.trim();
        if line == "}" {
            depth = depth.saturating_sub(1);
            continue;
        }
        let opens = line.ends_with('{');
        if depth == 1 {
            if let Some((key, value)) = line.split_once(" = ") {
                match key.trim() {
                    "state" => runtime.running = value.trim() == "running",
                    "pid" => runtime.pid = value.trim().parse().ok(),
                    "runs" => {
                        runtime.restarts = value
                            .trim()
                            .parse::<u32>()
                            .ok()
                            .map(|runs| runs.saturating_sub(1))
                    }
                    // `78: EX_CONFIG`, or `(never exited)`.
                    "last exit code" => {
                        runtime.last_exit_status = value
                            .split(':')
                            .next()
                            .and_then(|code| code.trim().parse().ok())
                    }
                    _ => {}
                }
            }
        }
        if opens {
            depth += 1;
        }
    }
    runtime
}

/// The properties [`parse_systemctl_show`] reads.
pub const SYSTEMCTL_RUNTIME_PROPERTIES: &str =
    "ActiveState,SubState,MainPID,ExecMainCode,ExecMainStatus,NRestarts";

/// Read `systemctl --user show <unit> --property=`[`SYSTEMCTL_RUNTIME_PROPERTIES`].
///
/// `ExecMainCode` is 0 while the main process has not exited, and otherwise
/// says how it ended; only then is `ExecMainStatus` an exit status.
pub fn parse_systemctl_show(output: &str) -> ServiceRuntime {
    let mut runtime = ServiceRuntime::default();
    let (mut active, mut sub, mut exited) = (false, false, false);
    let mut status = None;
    for line in output.lines() {
        let Some((key, value)) = line.trim().split_once('=') else {
            continue;
        };
        match key {
            "ActiveState" => active = value == "active",
            "SubState" => sub = value == "running",
            "MainPID" => runtime.pid = value.parse().ok().filter(|pid| *pid != 0),
            "ExecMainCode" => exited = value != "0",
            "ExecMainStatus" => status = value.parse().ok(),
            "NRestarts" => runtime.restarts = value.parse().ok(),
            _ => {}
        }
    }
    runtime.running = active && sub;
    runtime.last_exit_status = if exited { status } else { None };
    runtime
}

/// The last `lines` lines of `text`.
#[cfg_attr(not(any(target_os = "macos", target_os = "linux")), allow(dead_code))]
pub(crate) fn tail_lines(text: &str, lines: usize) -> String {
    let all: Vec<&str> = text.lines().collect();
    all[all.len().saturating_sub(lines)..].join("\n")
}

/// Escape the five XML predefined entities. `&` first so later replacements do
/// not double-escape it.
pub(crate) fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

/// Per-user directory a unit is written into, overridable via
/// `CAIRN_SERVICE_UNIT_DIR` (tests point it at a temp dir so they never touch the
/// real `~/Library/LaunchAgents` or `~/.config/systemd/user`).
#[cfg(any(target_os = "macos", target_os = "linux"))]
pub(crate) fn resolved_unit_dir(default_rel: &[&str]) -> Result<PathBuf, String> {
    if let Some(over) = std::env::var_os("CAIRN_SERVICE_UNIT_DIR") {
        return Ok(PathBuf::from(over));
    }
    let mut dir = std::env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| "cannot resolve HOME directory".to_string())?;
    for seg in default_rel {
        dir.push(seg);
    }
    Ok(dir)
}

// ─── systemd linger ──────────────────────────────────────────────────────────

/// logind's per-user linger, which decides whether a systemd user manager —
/// and every user unit it runs — outlives the user's last session. Without it
/// the manager stops at logout, taking the service and `/run/user/<uid>` with
/// it, so a user unit only meets this module's "survives logout" promise for a
/// lingering user. A seam so the ensure-then-verify sequence is provable
/// without logind; the real control is `SystemLinger` on Linux.
pub trait Linger {
    fn linger_enabled(&self, user: &str) -> Result<bool, String>;
    fn enable_linger(&self, user: &str) -> Result<(), String>;
}

/// Linger was off and could not be turned on from this session. Its display
/// names the one command that fixes it; callers add what to re-run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LingerRefusal {
    pub user: String,
    pub cause: String,
}

impl LingerRefusal {
    pub fn command(&self) -> String {
        format!("sudo loginctl enable-linger {}", self.user)
    }
}

impl std::fmt::Display for LingerRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "systemd stops {user}'s user services at logout unless lingering is enabled for \
             {user}, and enabling it from here failed ({cause}). Run:\n\n  {command}\n",
            user = self.user,
            cause = self.cause,
            command = self.command(),
        )
    }
}

/// Make `user` linger, or refuse. Already lingering is left alone; otherwise
/// linger is enabled and then read back, because `enable-linger` succeeding is
/// not the same fact as logind recording it.
pub fn ensure_linger<L: Linger + ?Sized>(host: &L, user: &str) -> Result<(), LingerRefusal> {
    let refusal = |cause: String| LingerRefusal {
        user: user.to_string(),
        cause,
    };
    if host.linger_enabled(user).map_err(refusal)? {
        return Ok(());
    }
    // polkit lets a user enable their own linger from an ordinary login or ssh
    // session; a `su` or container shell usually cannot, which is the refusal.
    host.enable_linger(user).map_err(refusal)?;
    if host.linger_enabled(user).map_err(refusal)? {
        Ok(())
    } else {
        Err(refusal("loginctl still reports Linger=no".to_string()))
    }
}

#[cfg(target_os = "linux")]
pub use linux::{current_user, SystemLinger};

// ─── Public lifecycle API (dispatches to the active `sys` backend) ───────────

/// Register the daemon as a per-user OS service and start it.
pub fn install(spec: &ServiceSpec) -> Result<(), String> {
    sys::install(spec)
}

/// Unregister and remove the per-user OS service.
pub fn uninstall(spec: &ServiceSpec) -> Result<(), String> {
    sys::uninstall(spec)
}

/// Bounce the installed service in place WITHOUT rewriting the unit file, so
/// local edits survive and the restart is atomic where the platform allows it.
/// Falls back to [`install`] when nothing is registered, so a repair path still
/// works.
pub fn restart(spec: &ServiceSpec) -> Result<(), String> {
    if !sys::is_installed(spec) {
        return sys::install(spec);
    }
    sys::restart(spec)
}

/// The unit file / registry value exists.
pub fn is_installed(spec: &ServiceSpec) -> bool {
    sys::is_installed(spec)
}

/// The service manager has the unit loaded (bootstrapped / enabled / present).
pub fn is_loaded(spec: &ServiceSpec) -> bool {
    sys::is_loaded(spec)
}

/// On-disk unit location, when the platform uses a file.
pub fn location(spec: &ServiceSpec) -> Option<PathBuf> {
    sys::location(spec)
}

/// The service manager's account of the supervised process. `None` where the
/// platform has no manager that tracks one (the Windows Run key starts a
/// process at logon and forgets it), or the manager could not be asked.
pub fn runtime(spec: &ServiceSpec) -> Option<ServiceRuntime> {
    sys::runtime(spec)
}

/// The last `lines` lines the service's process wrote to its error stream, as
/// the platform keeps them: launchd's error log, or the systemd user journal.
pub fn recent_log(spec: &ServiceSpec, lines: usize) -> Option<String> {
    sys::recent_log(spec, lines).filter(|log| !log.trim().is_empty())
}

/// The service registered on this machine under `identity`, if any, read from
/// its registration and its manager. Present when either the registration file
/// exists or the manager has it loaded.
pub fn describe_unit(identity: &LegacyIdentity) -> Result<Option<RegisteredUnit>, String> {
    sys::describe_unit(identity)
}

/// Stop the service registered under `identity` and remove its registration.
/// The same path the runner's install takes for its pre-rename identity, and
/// what the executor's legacy retirement takes for that identity's supervisor.
/// Nothing registered is not an error.
pub fn evict_legacy(identity: &LegacyIdentity) -> Result<(), String> {
    sys::evict_legacy(identity)
}

/// End the processes a Windows Run value started, matched on executable and
/// argument tail. Windows only, because removing a Run value stops nothing.
#[cfg(windows)]
pub fn terminate_run_value_processes(command_line: &str) -> Result<Vec<TerminatedProcess>, String> {
    sys::terminate_run_value_processes(command_line)
}

/// Self-heal an installed registration that has drifted from what this binary
/// generates — a launchd plist whose template changed, or a Windows Run value
/// pointing at a stale executable path. A no-op on platforms whose registration
/// carries nothing that can go stale (systemd re-reads its unit on
/// `daemon-reload`, and the install path always writes it fresh).
pub fn refresh_if_stale(spec: &ServiceSpec) {
    #[cfg(any(target_os = "macos", target_os = "windows"))]
    sys::refresh_if_stale(spec);
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    let _ = spec;
}

#[cfg(test)]
mod tests;
