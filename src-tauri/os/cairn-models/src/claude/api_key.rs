//! The Cairn-owned Claude CLI home every API-key launch runs in.
//!
//! A Claude CLI with no `CLAUDE_CONFIG_DIR` runs in the operator's own
//! `~/.claude`, where their hooks, `statusLine`, plugins, `CLAUDE.md` and MCP
//! servers load inside the agent's process. So an API-key launch names its own
//! directory, `<config root>/claude-api-key`, the way a managed account names
//! its profile.
//!
//! The directory is not an account home: nothing signs in to it, it holds no
//! credential (the key travels on an inherited descriptor and the CLI writes it
//! nowhere) and no `settings.json`, and it is shared by every API-key account,
//! so a conversation resumes on whichever key routing picks. What it does hold
//! is what the CLI writes for Cairn's sessions: its `.claude.json`, and the
//! `projects/` transcripts a resume replays. It lives in the public tree
//! because API keys work in builds without subscription support.

use std::path::{Path, PathBuf};

use crate::interface::session::SecretEnv;

/// The name a Claude CLI knows an Anthropic API key by. A launch carries the
/// key under this name and `ClaudeCli` decides how it travels.
pub const API_KEY_ENV: &str = "ANTHROPIC_API_KEY";

const API_KEY_HOME_DIR: &str = "claude-api-key";

/// Where API-key launches run, under the configuration root `config_dir`.
pub fn api_key_home(config_dir: &Path) -> PathBuf {
    config_dir.join(API_KEY_HOME_DIR)
}

/// The API-key home under `config_dir`, created if absent and readable by its
/// owner alone, since the transcripts it keeps are the agent's conversations.
pub fn prepare_api_key_home(config_dir: &Path) -> Result<PathBuf, String> {
    if config_dir.as_os_str().is_empty() {
        return Err(
            "API-key Claude launches have no home: the credential store has no configuration root"
                .to_string(),
        );
    }
    let home = api_key_home(config_dir);
    std::fs::create_dir_all(&home)
        .map_err(|e| format!("could not create {}: {e}", home.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&home, std::fs::Permissions::from_mode(0o700))
            .map_err(|e| format!("could not restrict {}: {e}", home.display()))?;
    }
    Ok(home)
}

/// What a Claude process launched on an API key receives: the key, under the
/// name the CLI knows it by, and the home it runs in. The API-key counterpart
/// of `ClaudeSessionCredential::for_launch`.
pub fn api_key_launch(key: &str, home: &Path) -> SecretEnv {
    SecretEnv::new()
        .with(API_KEY_ENV, key)
        .with("CLAUDE_CONFIG_DIR", home.to_string_lossy())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_home_is_under_the_configuration_root_and_owner_only() {
        let root = tempfile::tempdir().unwrap();
        let home = prepare_api_key_home(root.path()).unwrap();
        assert_eq!(home, api_key_home(root.path()));
        assert!(home.starts_with(root.path()));
        assert!(home.is_dir());
        if let Some(user_home) = std::env::var_os("HOME").map(PathBuf::from) {
            assert_ne!(home, user_home);
            assert_ne!(home, user_home.join(".claude"));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&home).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o700);
        }

        // Preparing it again repairs what it finds rather than failing.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&home, std::fs::Permissions::from_mode(0o755)).unwrap();
            prepare_api_key_home(root.path()).unwrap();
            let mode = std::fs::metadata(&home).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o700);
        }
    }

    #[test]
    fn no_configuration_root_is_no_home() {
        assert!(prepare_api_key_home(Path::new("")).is_err());
    }

    #[test]
    fn the_launch_carries_the_key_and_the_home_and_nothing_else() {
        let home = Path::new("/config/claude-api-key");
        let env = api_key_launch("sk-SENTINEL", home);
        assert_eq!(
            env.names().collect::<Vec<_>>(),
            [API_KEY_ENV, "CLAUDE_CONFIG_DIR"]
        );
        assert!(env.contains_value("sk-SENTINEL"));
        assert!(env.contains_value("/config/claude-api-key"));
    }
}
