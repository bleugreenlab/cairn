//! systemd user-unit lifecycle (Linux): write the unit under
//! `~/.config/systemd/user` and enable/disable it in the `--user` manager. No
//! elevation — a per-user unit so it reads that user's `~/.cairn`.

use std::ffi::CStr;
use std::path::{Path, PathBuf};
use std::process::Command;

use super::{
    parse_systemd_unit, resolved_unit_dir, systemd_install_steps, systemd_unit_contents,
    LegacyIdentity, Linger, RegisteredUnit, ServiceSpec,
};

/// The login name of the user this process runs as.
pub fn current_user() -> String {
    // SAFETY: getuid has no preconditions and cannot fail. getpwuid returns a
    // pointer into static storage or null; the name is copied out before
    // anything else can call it.
    let uid = unsafe { libc::getuid() };
    let name = unsafe {
        let entry = libc::getpwuid(uid);
        (!entry.is_null()).then(|| {
            CStr::from_ptr((*entry).pw_name)
                .to_string_lossy()
                .into_owned()
        })
    };
    name.or_else(|| std::env::var("USER").ok())
        .unwrap_or_else(|| uid.to_string())
}

/// logind's linger, through `loginctl`.
pub struct SystemLinger;

impl Linger for SystemLinger {
    fn linger_enabled(&self, user: &str) -> Result<bool, String> {
        // logind's own record of lingering users.
        if Path::new("/var/lib/systemd/linger").join(user).exists() {
            return Ok(true);
        }
        let result = Command::new("loginctl")
            .args(["show-user", user, "-p", "Linger", "--value"])
            .output()
            .map_err(|error| {
                format!(
                    "loginctl is not available ({error}); a systemd user service needs \
                     systemd-logind"
                )
            })?;
        // A user with no session and no linger is "not logged in or lingering",
        // which loginctl reports as a failure; both mean Linger=no.
        Ok(result.status.success() && String::from_utf8_lossy(&result.stdout).trim() == "yes")
    }

    fn enable_linger(&self, user: &str) -> Result<(), String> {
        let result = Command::new("loginctl")
            .args(["--no-ask-password", "enable-linger", user])
            .output()
            .map_err(|error| format!("loginctl: {error}"))?;
        if result.status.success() {
            Ok(())
        } else {
            Err(String::from_utf8_lossy(&result.stderr).trim().to_string())
        }
    }
}

fn unit_dir() -> Result<PathBuf, String> {
    resolved_unit_dir(&[".config", "systemd", "user"])
}

fn unit_path(unit_name: &str) -> Result<PathBuf, String> {
    Ok(unit_dir()?.join(unit_name))
}

pub fn install(spec: &ServiceSpec) -> Result<(), String> {
    let dir = unit_dir()?;
    std::fs::create_dir_all(&dir).map_err(|e| format!("create {}: {e}", dir.display()))?;

    // Stop the old generation before the new unit can open the same data dir.
    if let Some(legacy) = spec.legacy.as_ref() {
        let _ = evict_legacy(legacy);
    }

    let path = unit_path(&spec.unit_name)?;
    std::fs::write(&path, systemd_unit_contents(spec))
        .map_err(|e| format!("write {}: {e}", path.display()))?;

    for step in systemd_install_steps(&spec.unit_name) {
        let step: Vec<&str> = step.iter().map(String::as_str).collect();
        run_systemctl(&step)?;
    }
    Ok(())
}

/// Rewrite the installed unit when its template has drifted, and reload the
/// user manager so the running unit is supervised by the new policy. Acts only
/// in the process the unit started (its cgroup names the unit).
pub fn refresh_if_stale(spec: &ServiceSpec) {
    let Ok(path) = unit_path(&spec.unit_name) else {
        return;
    };
    let own_cgroup = std::fs::read_to_string("/proc/self/cgroup").ok();
    let needs_reload = || {
        Command::new("systemctl")
            .args([
                "--user",
                "show",
                &spec.unit_name,
                "--property=NeedDaemonReload",
                "--value",
            ])
            .output()
            .is_ok_and(|out| String::from_utf8_lossy(&out.stdout).trim() == "yes")
    };
    match super::refresh_systemd_unit(spec, own_cgroup.as_deref(), &path, needs_reload, || {
        run_systemctl(&["daemon-reload"])
    }) {
        Ok(super::SystemdRefresh::Rewritten) => tracing::info!(
            unit = %spec.unit_name,
            path = %path.display(),
            "systemd unit template drifted; rewrote it and reloaded the user manager"
        ),
        Ok(super::SystemdRefresh::Reloaded) => tracing::info!(
            unit = %spec.unit_name,
            "systemd unit was current on disk but not loaded; reloaded the user manager"
        ),
        Ok(_) => {}
        Err(error) => tracing::warn!(unit = %spec.unit_name, %error, "systemd unit refresh failed"),
    }
}

pub fn uninstall(spec: &ServiceSpec) -> Result<(), String> {
    // Best-effort disable+stop; ignore errors so a partially-installed unit still
    // cleans up.
    let _ = run_systemctl(&["disable", "--now", &spec.unit_name]);
    if let Ok(path) = unit_path(&spec.unit_name) {
        if path.exists() {
            std::fs::remove_file(&path).map_err(|e| format!("remove {}: {e}", path.display()))?;
        }
    }
    let _ = run_systemctl(&["daemon-reload"]);
    Ok(())
}

/// `systemctl --user stop` the unit and its pre-rename name. A stop job is not a
/// failure, so `Restart=` does not fire; the unit stays enabled and starts again
/// at the next login. A unit that is not loaded makes `stop` fail, which is
/// nothing to stop, so only a unit still active afterwards is an error.
pub fn stop(spec: &ServiceSpec) -> Result<(), String> {
    let legacy = spec
        .legacy
        .as_ref()
        .and_then(|legacy| legacy.unit_name.as_deref());
    super::stop_each(
        std::iter::once(spec.unit_name.as_str()).chain(legacy),
        |unit| {
            let stopped = run_systemctl(&["stop", unit]);
            let still_active = Command::new("systemctl")
                .args(["--user", "is-active", "--quiet", unit])
                .status()
                .is_ok_and(|status| status.success());
            match (still_active, stopped) {
                (false, _) => Ok(()),
                (true, Err(error)) => Err(format!("{unit} is still active: {error}")),
                (true, Ok(())) => Err(format!("{unit} is still active after systemctl stop")),
            }
        },
    )
}

/// Bounce the systemd user unit in place with `systemctl --user restart` — no
/// unit-file rewrite. Falls back to [`install`] when the unit is not known to
/// systemd (never installed, or written but not `daemon-reload`ed).
pub fn restart(spec: &ServiceSpec) -> Result<(), String> {
    match run_systemctl(&["restart", &spec.unit_name]) {
        Ok(()) => Ok(()),
        Err(e) => {
            tracing::info!(
                "systemctl --user restart {} failed ({e}); falling back to install",
                spec.unit_name
            );
            install(spec)
        }
    }
}

pub fn is_installed(spec: &ServiceSpec) -> bool {
    unit_path(&spec.unit_name)
        .map(|p| p.exists())
        .unwrap_or(false)
}

pub fn is_loaded(spec: &ServiceSpec) -> bool {
    Command::new("systemctl")
        .args(["--user", "is-active", &spec.unit_name])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

pub fn location(spec: &ServiceSpec) -> Option<PathBuf> {
    unit_path(&spec.unit_name).ok()
}

pub fn runtime(spec: &ServiceSpec) -> Option<super::ServiceRuntime> {
    let out = Command::new("systemctl")
        .args([
            "--user",
            "show",
            &spec.unit_name,
            &format!("--property={}", super::SYSTEMCTL_RUNTIME_PROPERTIES),
        ])
        .output()
        .ok()
        .filter(|out| out.status.success())?;
    Some(super::parse_systemctl_show(&String::from_utf8_lossy(
        &out.stdout,
    )))
}

pub fn recent_log(spec: &ServiceSpec, lines: usize) -> Option<String> {
    let out = Command::new("journalctl")
        .args([
            "--user",
            "--unit",
            &spec.unit_name,
            "--lines",
            &lines.to_string(),
            "--no-pager",
            "--output",
            "cat",
        ])
        .output()
        .ok()
        .filter(|out| out.status.success())?;
    Some(super::tail_lines(
        &String::from_utf8_lossy(&out.stdout),
        lines,
    ))
}

/// The user unit registered under `identity`'s unit name: its file, and the
/// user manager's account of its main process.
pub fn describe_unit(identity: &LegacyIdentity) -> Result<Option<RegisteredUnit>, String> {
    let Some(unit) = identity.unit_name.as_deref() else {
        return Ok(None);
    };
    let path = unit_path(unit)?;
    let shown = Command::new("systemctl")
        .args([
            "--user",
            "show",
            unit,
            &format!(
                "--property=LoadState,{}",
                super::SYSTEMCTL_RUNTIME_PROPERTIES
            ),
        ])
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map(|out| String::from_utf8_lossy(&out.stdout).into_owned());
    let loaded = shown
        .as_deref()
        .is_some_and(|shown| shown.lines().any(|line| line.trim() == "LoadState=loaded"));
    let present = path.exists();
    if !present && !loaded {
        return Ok(None);
    }
    let description = if present {
        let text = std::fs::read_to_string(&path)
            .map_err(|error| format!("read {}: {error}", path.display()))?;
        parse_systemd_unit(&text)
    } else {
        Default::default()
    };
    Ok(Some(RegisteredUnit {
        name: unit.to_string(),
        location: present.then_some(path),
        description,
        runtime: loaded
            .then(|| shown.as_deref().map(super::parse_systemctl_show))
            .flatten(),
        command_line: None,
    }))
}

/// `disable --now` stops the unit, and with it every process in its cgroup,
/// then the file is removed so the user manager forgets it.
pub fn evict_legacy(identity: &LegacyIdentity) -> Result<(), String> {
    let Some(unit) = identity.unit_name.as_deref() else {
        return Ok(());
    };
    // A unit that is not loaded makes `disable` fail; that is nothing to stop.
    // A unit still active afterwards is a real failure, whatever disable said.
    let disabled = run_systemctl(&["disable", "--now", unit]);
    let still_active = Command::new("systemctl")
        .args(["--user", "is-active", "--quiet", unit])
        .status()
        .is_ok_and(|status| status.success());
    if still_active {
        return Err(match disabled {
            Err(error) => format!("{unit} is still active: {error}"),
            Ok(()) => format!("{unit} is still active after systemctl disable --now"),
        });
    }
    let path = unit_path(unit)?;
    if path.exists() {
        std::fs::remove_file(&path).map_err(|e| format!("remove {}: {e}", path.display()))?;
    }
    let _ = run_systemctl(&["daemon-reload"]);
    let _ = run_systemctl(&["reset-failed", unit]);
    Ok(())
}

fn run_systemctl(args: &[&str]) -> Result<(), String> {
    let mut full = vec!["--user"];
    full.extend_from_slice(args);
    let out = Command::new("systemctl")
        .args(&full)
        .output()
        .map_err(|e| format!("systemctl {}: {e}", args.join(" ")))?;
    if !out.status.success() {
        return Err(format!(
            "systemctl {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    Ok(())
}
