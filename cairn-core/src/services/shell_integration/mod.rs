//! Shell integration: inject OSC 133 semantic-prompt markers into interactive
//! shells so the backend can detect when a command is executing.
//!
//! Three small scripts (zsh / bash / fish) source the user's real shell config
//! and then install pre/post-command hooks that emit OSC 133 `C`/`D` markers.
//! [`apply_shell_integration`] materializes the scripts to a stable per-install
//! directory and configures a [`CommandBuilder`] to load them via the
//! shell-appropriate injection vector:
//!
//! - **zsh** — a temporary `ZDOTDIR` whose `.zshenv`/`.zshrc` source the user's
//!   real config (from the original `ZDOTDIR`) and then install the hooks,
//!   restoring `ZDOTDIR` afterward so nested shells behave.
//! - **bash** — `--rcfile <script>`; the script sources `~/.bashrc` then hooks.
//! - **fish** — `--init-command 'source <script>'`.
//!
//! Unknown shells are left unmodified (graceful degradation: the terminal simply
//! never reports busy). Markers are emitted by these scripts and stripped on the
//! backend by [`super::pty_osc::Osc133Parser`]; xterm never sees the 133 bytes.

use portable_pty::CommandBuilder;
use std::path::{Path, PathBuf};

const ZSH_ZSHENV: &str = include_str!("zshenv.zsh");
const ZSH_ZSHRC: &str = include_str!("zshrc.zsh");
const BASH_RC: &str = include_str!("bash.bash");
const FISH_INIT: &str = include_str!("fish.fish");

/// The stable per-install directory where integration scripts are materialized.
pub fn integration_dir() -> PathBuf {
    cairn_common::paths::cairn_home().join("shell_integration")
}

/// Materialize the integration scripts under `dir` and configure `cmd` to load
/// the one matching `shell_path`. Best-effort: any IO failure leaves `cmd`
/// unmodified so the terminal still spawns (just without busy reporting), and an
/// unrecognized shell is a silent no-op.
pub fn apply_shell_integration(cmd: &mut CommandBuilder, shell_path: &str, dir: &Path) {
    match shell_basename(shell_path).as_str() {
        "zsh" => {
            let zdotdir = dir.join("zsh");
            if write_script(&zdotdir.join(".zshenv"), ZSH_ZSHENV).is_err()
                || write_script(&zdotdir.join(".zshrc"), ZSH_ZSHRC).is_err()
            {
                return;
            }
            // Preserve the user's ZDOTDIR so our scripts can source their config.
            if let Some(orig) = std::env::var_os("ZDOTDIR") {
                cmd.env("CAIRN_ZDOTDIR_ORIG", orig);
            }
            cmd.env("ZDOTDIR", &zdotdir);
        }
        "bash" => {
            let rc = dir.join("bash.bash");
            if write_script(&rc, BASH_RC).is_err() {
                return;
            }
            cmd.arg("--rcfile");
            cmd.arg(&rc);
        }
        "fish" => {
            let init = dir.join("fish.fish");
            if write_script(&init, FISH_INIT).is_err() {
                return;
            }
            cmd.arg("--init-command");
            cmd.arg(format!("source {}", shell_quote(&init)));
        }
        _ => {}
    }
}

/// The lowercased shell name without directory or `.exe` suffix.
fn shell_basename(shell_path: &str) -> String {
    let name = Path::new(shell_path)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("")
        .to_lowercase();
    name.trim_end_matches(".exe").to_string()
}

/// Write `contents` to `path`, creating parent dirs. Always rewrites so an
/// upgraded binary refreshes the on-disk scripts.
fn write_script(path: &Path, contents: &str) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, contents)
}

/// Single-quote a path for a fish `source` init-command, escaping embedded quotes.
fn shell_quote(path: &Path) -> String {
    format!("'{}'", path.to_string_lossy().replace('\'', "'\\''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basename_strips_dir_and_exe_and_lowercases() {
        assert_eq!(shell_basename("/bin/zsh"), "zsh");
        assert_eq!(shell_basename("/usr/local/bin/fish"), "fish");
        assert_eq!(shell_basename("BASH.EXE"), "bash");
        assert_eq!(shell_basename(""), "");
    }

    #[test]
    fn unknown_shell_leaves_command_untouched() {
        let dir = std::env::temp_dir().join("cairn-shell-integ-test-unknown");
        let mut cmd = CommandBuilder::new("/bin/dash");
        apply_shell_integration(&mut cmd, "/bin/dash", &dir);
        // dash is unrecognized: no args were added.
        assert!(cmd.get_argv().len() == 1);
    }

    #[test]
    fn bash_gets_rcfile_and_materializes_script() {
        let dir = std::env::temp_dir().join(format!(
            "cairn-shell-integ-test-bash-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let mut cmd = CommandBuilder::new("/bin/bash");
        apply_shell_integration(&mut cmd, "/bin/bash", &dir);
        assert!(dir.join("bash.bash").exists());
        let argv = cmd.get_argv();
        assert!(argv.iter().any(|a| a.to_str() == Some("--rcfile")));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn zsh_materializes_zdotdir_scripts() {
        let dir =
            std::env::temp_dir().join(format!("cairn-shell-integ-test-zsh-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut cmd = CommandBuilder::new("/bin/zsh");
        apply_shell_integration(&mut cmd, "/bin/zsh", &dir);
        assert!(dir.join("zsh").join(".zshenv").exists());
        assert!(dir.join("zsh").join(".zshrc").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
