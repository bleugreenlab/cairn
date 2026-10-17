//! launchd LaunchAgent lifecycle (macOS): write the plist under
//! `~/Library/LaunchAgents` and bootstrap/bootout it in the per-user GUI domain.
//! No elevation — a per-user agent so it reads that user's `~/.cairn`.

use std::path::PathBuf;
use std::process::Command;

use super::{
    parse_launchd_plist_json, plist_contents, resolved_unit_dir, LegacyIdentity, RegisteredUnit,
    ServiceSpec,
};

fn unit_dir() -> Result<PathBuf, String> {
    resolved_unit_dir(&["Library", "LaunchAgents"])
}

fn plist_path(label: &str) -> Result<PathBuf, String> {
    Ok(unit_dir()?.join(format!("{label}.plist")))
}

fn kickstart_command(target: &str, replace_running: bool) -> Command {
    let mut command = Command::new("launchctl");
    command.arg("kickstart");
    if replace_running {
        command.arg("-k");
    }
    command.arg(target);
    command
}

/// The per-user GUI domain target, e.g. `gui/501`.
fn gui_domain() -> String {
    // SAFETY: getuid() is infallible and has no preconditions.
    let uid = unsafe { libc::getuid() };
    format!("gui/{uid}")
}

pub fn install(spec: &ServiceSpec) -> Result<(), String> {
    let dir = unit_dir()?;
    std::fs::create_dir_all(&dir).map_err(|e| format!("create {}: {e}", dir.display()))?;

    let domain = gui_domain();
    // Adoption ordering is load-bearing: stop and remove any pre-rename job
    // before launchd can start the new identity against the same data dir.
    if let Some(legacy) = spec.legacy.as_ref() {
        let _ = evict_legacy(legacy);
    }

    let path = plist_path(&spec.label)?;
    std::fs::write(&path, plist_contents(spec))
        .map_err(|e| format!("write {}: {e}", path.display()))?;

    let target = format!("{domain}/{}", spec.label);
    // Idempotent: bootout any existing instance before bootstrapping (ignore
    // errors — there may be nothing loaded).
    let _ = Command::new("launchctl")
        .args(["bootout", &target])
        .output();
    let out = Command::new("launchctl")
        .args(["bootstrap", &domain, &path.to_string_lossy()])
        .output()
        .map_err(|e| format!("launchctl bootstrap: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "launchctl bootstrap failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    // RunAtLoad should start it, but a non-destructive kickstart makes "running
    // now" explicit if launchd has not scheduled it yet. Do not use `-k` here:
    // bootstrap may already have started the new process, and killing that fresh
    // process immediately puts its replacement behind launchd's crash throttle.
    let _ = kickstart_command(&target, false).output();
    Ok(())
}

/// File-only self-heal for template drift: when launchd spawned this process
/// from an installed plist whose contents no longer match what
/// [`plist_contents`] generates, rewrite the file in place. No
/// bootout/bootstrap — that would kill this very process — so the refreshed
/// plist (e.g. a `ProcessType` change) applies at the next launchd respawn
/// (reboot, crash, or explicit service restart).
///
/// Never runs for cargo/dev/manual invocations: those are not launchd children
/// (parent pid != 1), and must not clobber the installed service's plist with
/// their own exe path and environment.
pub fn refresh_if_stale(spec: &ServiceSpec) {
    // SAFETY: getppid() is infallible and has no preconditions.
    if unsafe { libc::getppid() } != 1 {
        return;
    }
    let Ok(path) = plist_path(&spec.label) else {
        return;
    };
    let Ok(existing) = std::fs::read_to_string(&path) else {
        return; // not installed (or unreadable) — nothing to refresh
    };
    let desired = plist_contents(spec);
    if existing == desired {
        return;
    }
    match std::fs::write(&path, &desired) {
        Ok(()) => tracing::info!(
            "launchd plist template drifted; rewrote {} (takes effect at the next launchd respawn)",
            path.display()
        ),
        Err(e) => tracing::warn!("launchd plist refresh: write {}: {e}", path.display()),
    }
}

pub fn uninstall(spec: &ServiceSpec) -> Result<(), String> {
    let target = format!("{}/{}", gui_domain(), spec.label);
    let _ = Command::new("launchctl")
        .args(["bootout", &target])
        .output();
    if let Ok(path) = plist_path(&spec.label) {
        if path.exists() {
            std::fs::remove_file(&path).map_err(|e| format!("remove {}: {e}", path.display()))?;
        }
    }
    Ok(())
}

/// Bounce the launchd job in place with `launchctl kickstart -k` — no plist
/// rewrite, no bootout/bootstrap flap. Falls back to a full [`install`] when the
/// job is on disk but not bootstrapped in this GUI domain (kickstart then fails
/// to resolve the service).
pub fn restart(spec: &ServiceSpec) -> Result<(), String> {
    let target = format!("{}/{}", gui_domain(), spec.label);
    let out = kickstart_command(&target, true)
        .output()
        .map_err(|e| format!("launchctl kickstart {target}: {e}"))?;
    if out.status.success() {
        return Ok(());
    }
    tracing::info!(
        "launchctl kickstart {target} failed ({}); falling back to bootstrap via install",
        String::from_utf8_lossy(&out.stderr).trim()
    );
    install(spec)
}

pub fn is_installed(spec: &ServiceSpec) -> bool {
    plist_path(&spec.label).map(|p| p.exists()).unwrap_or(false)
}

pub fn is_loaded(spec: &ServiceSpec) -> bool {
    let target = format!("{}/{}", gui_domain(), spec.label);
    Command::new("launchctl")
        .args(["print", &target])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

pub fn location(spec: &ServiceSpec) -> Option<PathBuf> {
    plist_path(&spec.label).ok()
}

pub fn runtime(spec: &ServiceSpec) -> Option<super::ServiceRuntime> {
    let target = format!("{}/{}", gui_domain(), spec.label);
    let out = Command::new("launchctl")
        .args(["print", &target])
        .output()
        .ok()
        .filter(|out| out.status.success())?;
    Some(super::parse_launchctl_print(&String::from_utf8_lossy(
        &out.stdout,
    )))
}

pub fn recent_log(spec: &ServiceSpec, lines: usize) -> Option<String> {
    let log = std::fs::read(spec.log_path("err")).ok()?;
    Some(super::tail_lines(&String::from_utf8_lossy(&log), lines))
}

/// The LaunchAgent registered under `identity`'s label: its plist, read through
/// `plutil` so every plist encoding parses the same way, and launchd's account
/// of its process.
pub fn describe_unit(identity: &LegacyIdentity) -> Result<Option<RegisteredUnit>, String> {
    let Some(label) = identity.label.as_deref() else {
        return Ok(None);
    };
    let path = plist_path(label)?;
    let target = format!("{}/{label}", gui_domain());
    let runtime = Command::new("launchctl")
        .args(["print", &target])
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map(|out| super::parse_launchctl_print(&String::from_utf8_lossy(&out.stdout)));
    let present = path.exists();
    if !present && runtime.is_none() {
        return Ok(None);
    }
    let description = if present {
        let out = Command::new("plutil")
            .args(["-convert", "json", "-o", "-"])
            .arg(&path)
            .output()
            .map_err(|error| format!("plutil {}: {error}", path.display()))?;
        if !out.status.success() {
            return Err(format!(
                "plutil could not read {}: {}",
                path.display(),
                String::from_utf8_lossy(&out.stderr).trim()
            ));
        }
        parse_launchd_plist_json(&String::from_utf8_lossy(&out.stdout))?
    } else {
        Default::default()
    };
    Ok(Some(RegisteredUnit {
        name: label.to_string(),
        location: present.then_some(path),
        description,
        runtime,
        command_line: None,
    }))
}

/// Boot the job out of the GUI domain, which stops its process, and remove its
/// plist so nothing bootstraps it again at the next login.
pub fn evict_legacy(identity: &LegacyIdentity) -> Result<(), String> {
    let Some(label) = identity.label.as_deref() else {
        return Ok(());
    };
    let target = format!("{}/{label}", gui_domain());
    let booted_out = Command::new("launchctl")
        .args(["bootout", &target])
        .output();
    // Nothing loaded is an error from launchctl and not one here; a job still
    // loaded afterwards is, whatever bootout said, because it keeps respawning.
    let still_loaded = Command::new("launchctl")
        .args(["print", &target])
        .output()
        .is_ok_and(|out| out.status.success());
    if still_loaded {
        return Err(match booted_out {
            Ok(out) => format!(
                "launchctl bootout {target} left it loaded: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            ),
            Err(error) => format!("launchctl bootout {target}: {error}"),
        });
    }
    let path = plist_path(label)?;
    if path.exists() {
        std::fs::remove_file(&path).map_err(|e| format!("remove {}: {e}", path.display()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(command: &Command) -> Vec<String> {
        command
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect()
    }

    #[test]
    fn install_kickstart_never_kills_the_freshly_bootstrapped_daemon() {
        assert_eq!(
            args(&kickstart_command("gui/501/computer.cairn.orch", false)),
            ["kickstart", "gui/501/computer.cairn.orch"]
        );
    }

    #[test]
    fn explicit_restart_still_replaces_the_running_process() {
        assert_eq!(
            args(&kickstart_command("gui/501/computer.cairn.orch", true)),
            ["kickstart", "-k", "gui/501/computer.cairn.orch"]
        );
    }
}
