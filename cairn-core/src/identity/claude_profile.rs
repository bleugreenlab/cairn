//! Cairn-managed Claude CLI profile lifecycle.

use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::process::Command;

const PROFILES_DIR: &str = "claude-profiles";
const TRANSCRIPTS_DIR: &str = "_transcripts";

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ClaudeAuthStatus {
    pub logged_in: bool,
    #[serde(default)]
    pub email: Option<String>,
    #[serde(default, rename = "subscriptionType")]
    pub subscription_type: Option<String>,
}

/// A managed profile lives under the configuration root that owns the identity
/// store it belongs to. There is deliberately no variant that infers the root:
/// sign-in, sign-out and session resolution all have to name the same
/// directory, and an inferred one silently disagreed with the real one
/// whenever Cairn ran with a non-default config dir.
pub fn profile_dir_in(config_dir: &Path, account_id: &str) -> PathBuf {
    config_dir.join(PROFILES_DIR).join(account_id)
}

/// Create a hermetic profile and ensure all profiles share Claude transcripts.
pub fn provision_profile(profile: &Path) -> Result<(), String> {
    std::fs::create_dir_all(profile).map_err(|err| {
        format!(
            "Failed to create Claude profile {}: {err}",
            profile.display()
        )
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::{symlink, PermissionsExt};
        std::fs::set_permissions(profile, std::fs::Permissions::from_mode(0o700))
            .map_err(|err| format!("Failed to secure Claude profile: {err}"))?;
        let profiles_root = profile
            .parent()
            .ok_or_else(|| "Claude profile has no parent".to_string())?;
        let transcripts = profiles_root.join(TRANSCRIPTS_DIR);
        std::fs::create_dir_all(&transcripts)
            .map_err(|err| format!("Failed to create shared Claude transcripts: {err}"))?;
        let projects = profile.join("projects");
        let correct = std::fs::read_link(&projects)
            .map(|target| target == transcripts)
            .unwrap_or(false);
        if !correct {
            if projects.is_dir() && !projects.is_symlink() {
                std::fs::remove_dir_all(&projects)
                    .map_err(|err| format!("Failed to replace Claude projects directory: {err}"))?;
            } else if projects.symlink_metadata().is_ok() {
                std::fs::remove_file(&projects)
                    .map_err(|err| format!("Failed to replace Claude projects link: {err}"))?;
            }
            symlink(&transcripts, &projects)
                .map_err(|err| format!("Failed to link shared Claude transcripts: {err}"))?;
        }
    }
    Ok(())
}

pub fn auth_status(claude: &Path, profile: &Path) -> Result<ClaudeAuthStatus, String> {
    provision_profile(profile)?;
    let output = Command::new(claude)
        .args(["auth", "status", "--json"])
        .env("CLAUDE_CONFIG_DIR", profile)
        .env_remove("ANTHROPIC_API_KEY")
        .env_remove("CLAUDE_CODE_OAUTH_TOKEN")
        .output()
        .map_err(|err| format!("Failed to query Claude auth status: {err}"))?;
    serde_json::from_slice(&output.stdout)
        .map_err(|err| format!("Invalid Claude auth status response: {err}"))
}

pub fn logout(claude: &Path, profile: &Path) -> Result<(), String> {
    if profile.exists() {
        let status = Command::new(claude)
            .args(["auth", "logout"])
            .env("CLAUDE_CONFIG_DIR", profile)
            .status()
            .map_err(|err| format!("Failed to log out Claude profile: {err}"))?;
        if !status.success() {
            return Err(format!("Claude logout exited with {status}"));
        }
        std::fs::remove_dir_all(profile)
            .map_err(|err| format!("Failed to remove Claude profile: {err}"))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn provisioning_creates_and_repairs_shared_projects_link() {
        let temp = TempDir::new().unwrap();
        let profile = profile_dir_in(temp.path(), "acc_one");
        provision_profile(&profile).unwrap();
        let expected = temp.path().join(PROFILES_DIR).join(TRANSCRIPTS_DIR);
        assert_eq!(
            std::fs::read_link(profile.join("projects")).unwrap(),
            expected
        );

        std::fs::remove_file(profile.join("projects")).unwrap();
        std::fs::create_dir(profile.join("projects")).unwrap();
        std::fs::write(profile.join("projects").join("stale"), "x").unwrap();
        provision_profile(&profile).unwrap();
        assert_eq!(
            std::fs::read_link(profile.join("projects")).unwrap(),
            expected
        );
    }

    #[test]
    fn parses_auth_status_metadata() {
        let status: ClaudeAuthStatus = serde_json::from_str(
            r#"{"loggedIn":true,"email":"work@example.com","subscriptionType":"max"}"#,
        )
        .unwrap();
        assert!(status.logged_in);
        assert_eq!(status.email.as_deref(), Some("work@example.com"));
        assert_eq!(status.subscription_type.as_deref(), Some("max"));
    }
}
