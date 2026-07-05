//! Environment utilities for running CLI commands in signed/sandboxed apps.
//!
//! Signed/notarized macOS apps run with a restricted PATH that doesn't include
//! user-installed tools like claude, gh, git, npx, etc. This module provides
//! utilities to resolve the user's actual PATH and run commands with it.
//!
//! On Windows, similar issues can occur with PATH resolution in GUI apps.

use std::process::Command;
use std::sync::OnceLock;

#[cfg(not(windows))]
use std::process::{Output, Stdio};
#[cfg(not(windows))]
use std::time::{Duration, Instant};

#[cfg(windows)]
use std::os::windows::process::CommandExt;

#[cfg(windows)]
const CREATE_NO_WINDOW: u32 = 0x08000000;

/// Cached user PATH - resolved once on first use
static USER_PATH: OnceLock<String> = OnceLock::new();

/// Get the PATH separator for the current platform
#[cfg(windows)]
const PATH_SEP: char = ';';

/// Get the user's home directory
fn get_home_dir() -> String {
    // Try platform-specific env vars first, then fall back to dirs crate
    #[cfg(windows)]
    {
        std::env::var("USERPROFILE")
            .or_else(|_| std::env::var("HOME"))
            .unwrap_or_else(|_| {
                dirs::home_dir()
                    .map(|p| p.to_string_lossy().to_string())
                    .unwrap_or_else(|| "C:\\Users".to_string())
            })
    }
    #[cfg(not(windows))]
    {
        std::env::var("HOME").unwrap_or_else(|_| {
            dirs::home_dir()
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_else(|| "/Users".to_string())
        })
    }
}

/// Get a reasonable PATH for finding CLI tools.
/// Includes common installation locations and the user's shell PATH.
/// Result is cached for subsequent calls.
pub fn get_user_path() -> &'static str {
    USER_PATH.get_or_init(|| {
        let home = get_home_dir();

        #[cfg(windows)]
        {
            // Windows common paths where CLI tools are installed
            let common_paths = [
                format!("{}\\.bun\\bin", home),
                format!("{}\\AppData\\Local\\Programs\\bun", home),
                format!("{}\\.cargo\\bin", home),
                format!("{}\\AppData\\Roaming\\npm", home),
                format!("{}\\AppData\\Local\\Yarn\\bin", home),
                format!("{}\\scoop\\shims", home),
                "C:\\Program Files\\nodejs".to_string(),
                "C:\\Program Files\\Git\\cmd".to_string(),
            ];

            // Get existing PATH and prepend common paths
            let existing_path = std::env::var("PATH").unwrap_or_default();
            let mut all_paths: Vec<&str> = common_paths.iter().map(|s| s.as_str()).collect();
            if !existing_path.is_empty() {
                all_paths.push(&existing_path);
            }

            all_paths.join(&PATH_SEP.to_string())
        }

        #[cfg(not(windows))]
        {
            // Unix common paths where CLI tools are installed
            let common_paths = format!(
                "{}/.claude/local/bin:{}/.bun/bin:{}/.local/bin:{}/.npm/bin:{}/.yarn/bin:{}/.cargo/bin:/usr/local/bin:/opt/homebrew/bin",
                home, home, home, home, home, home
            );

            // Start with the process's actual PATH (includes Docker ENV, etc.)
            let env_path = std::env::var("PATH").unwrap_or_default();

            if let Some(shell_path) = resolve_user_shell_path() {
                return format!("{}:{}:{}", common_paths, shell_path, env_path);
            }

            if env_path.is_empty() {
                format!("{}:/usr/bin:/bin:/usr/sbin:/sbin", common_paths)
            } else {
                format!("{}:{}", common_paths, env_path)
            }
        }
    })
}

#[cfg(not(windows))]
fn resolve_user_shell_path() -> Option<String> {
    let user_shell = crate::services::get_default_shell();
    let mut user_shell_command = Command::new(&user_shell);
    user_shell_command.args(["-ilc", "command env"]);
    shell_path_from_command(&mut user_shell_command).or_else(|| {
        let mut fallback_command = Command::new("sh");
        fallback_command.args(["-lc", "command env"]);
        shell_path_from_command(&mut fallback_command)
    })
}

#[cfg(not(windows))]
fn shell_path_from_command(command: &mut Command) -> Option<String> {
    let output = command_output_with_timeout(command, Duration::from_secs(3)).ok()?;
    if !output.status.success() {
        return None;
    }
    parse_path_from_env_output(&String::from_utf8_lossy(&output.stdout))
}

#[cfg(not(windows))]
fn command_output_with_timeout(
    command: &mut Command,
    timeout: Duration,
) -> std::io::Result<Output> {
    let mut child = command
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .stdout(Stdio::piped())
        .spawn()?;
    let deadline = Instant::now() + timeout;

    loop {
        if child.try_wait()?.is_some() {
            return child.wait_with_output();
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            return child.wait_with_output();
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

#[cfg(not(windows))]
pub(crate) fn parse_path_from_env_output(output: &str) -> Option<String> {
    output
        .lines()
        .rev()
        .find_map(|line| line.strip_prefix("PATH=").filter(|path| !path.is_empty()))
        .map(ToOwned::to_owned)
}

/// Find a binary by name using the user's PATH.
/// Returns the full path to the binary if found.
pub fn find_binary(name: &str) -> Result<String, String> {
    let user_path = get_user_path();

    #[cfg(windows)]
    {
        // On Windows, use 'where' command
        let output = Command::new("cmd")
            .args(["/c", &format!("where {}", name)])
            .env("PATH", user_path)
            .output()
            .map_err(|e| format!("Failed to find {}: {}", name, e))?;

        if !output.status.success() {
            return Err(format!("{} not found in PATH", name));
        }

        // 'where' can return multiple lines, take the first one
        let paths = String::from_utf8_lossy(&output.stdout);
        let path = paths.lines().next().unwrap_or("").trim().to_string();

        if path.is_empty() {
            return Err(format!("{} not found", name));
        }

        Ok(path)
    }

    #[cfg(not(windows))]
    {
        // On Unix, use 'which' command
        let output = Command::new("sh")
            .args(["-c", &format!("which {}", name)])
            .env("PATH", user_path)
            .output()
            .map_err(|e| format!("Failed to find {}: {}", name, e))?;

        if !output.status.success() {
            return Err(format!("{} not found in PATH", name));
        }

        let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if path.is_empty() {
            return Err(format!("{} not found", name));
        }

        Ok(path)
    }
}

/// Create a Command with the user's PATH set and console hidden on Windows.
pub fn command(program: &str) -> Command {
    let mut cmd = Command::new(program);
    cmd.env("PATH", get_user_path());

    #[cfg(windows)]
    cmd.creation_flags(CREATE_NO_WINDOW);

    cmd
}

/// Create a Command for git with the user's PATH set.
pub fn git() -> Command {
    command("git")
}

/// Create a Command for gh (GitHub CLI) with the user's PATH set.
pub fn gh() -> Command {
    command("gh")
}

#[cfg(test)]
#[cfg(not(windows))]
mod tests {
    use super::parse_path_from_env_output;

    #[test]
    fn parse_path_from_env_output_picks_path_line() {
        let output = "SHELL=/bin/zsh\nPATH=/usr/local/bin:/usr/bin\nHOME=/Users/example\n";
        assert_eq!(
            parse_path_from_env_output(output).as_deref(),
            Some("/usr/local/bin:/usr/bin")
        );
    }

    #[test]
    fn parse_path_from_env_output_ignores_non_path_lines() {
        let output = "SHELL=/bin/zsh\nCAIRN_PATH_HINT=/tmp/bin\nHOME=/Users/example\n";
        assert_eq!(parse_path_from_env_output(output), None);
    }

    #[test]
    fn parse_path_from_env_output_uses_last_path_line() {
        let output = "PATH=/minimal\nnoise from shell rc\nPATH=/shell/configured:/usr/bin\n";
        assert_eq!(
            parse_path_from_env_output(output).as_deref(),
            Some("/shell/configured:/usr/bin")
        );
    }

    #[test]
    fn parse_path_from_env_output_rejects_empty_path() {
        assert_eq!(parse_path_from_env_output("PATH=\n"), None);
    }
}
