//! systemd user-unit lifecycle (Linux): write the unit under
//! `~/.config/systemd/user` and enable/disable it in the `--user` manager. No
//! elevation — a per-user unit so it reads that user's `~/.cairn`.

use std::path::PathBuf;
use std::process::Command;

use super::{resolved_unit_dir, systemd_install_steps, systemd_unit_contents, ServiceSpec};

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
    if let Some(legacy) = spec.legacy.as_ref().and_then(|l| l.unit_name.as_deref()) {
        let _ = run_systemctl(&["disable", "--now", legacy]);
        let legacy_path = dir.join(legacy);
        if legacy_path.exists() {
            let _ = std::fs::remove_file(&legacy_path);
        }
        let _ = run_systemctl(&["daemon-reload"]);
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
