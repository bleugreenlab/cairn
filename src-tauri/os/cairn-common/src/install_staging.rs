//! Shared names for atomic executable installation.
//!
//! Staging, replacement, and stale-file cleanup all consume these definitions,
//! so an installer cannot create a class of sibling that its sweeper does not
//! recognise. The platform is explicit because a runner commonly names files
//! for a remote operating system rather than its own.

/// Marks an install attempt's private staging sibling.
pub const STAGING_INFIX: &str = "cairn-install";

/// Marks an install attempt's rollback copy of the binary it replaced.
pub const REPLACED_INFIX: &str = "cairn-replaced";

/// Age after which a later install may reclaim an abandoned sibling.
pub const STAGING_SWEEP_MINUTES: u64 = 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstallPlatform {
    Posix,
    Windows,
}

/// One install attempt's private sibling paths.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstallStaging {
    path: String,
    replaced_path: String,
    token: String,
}

impl InstallStaging {
    /// Creates same-directory paths, keeping the final renames on one volume.
    pub fn new(destination: &str, platform: InstallPlatform) -> Self {
        Self::with_token(
            destination,
            platform,
            &uuid::Uuid::new_v4().simple().to_string(),
        )
    }

    /// Builds paths from a caller-selected token.
    ///
    /// This is useful when an attempt must recover names from durable state and
    /// lets tests assert exact names rather than patterns.
    pub fn with_token(destination: &str, platform: InstallPlatform, token: &str) -> Self {
        let executable_suffix = match platform {
            InstallPlatform::Posix => "",
            // Windows executes the staged file for its identity check, and a
            // path outside PATHEXT is not reliably invokable by PowerShell.
            InstallPlatform::Windows => ".exe",
        };
        Self {
            path: format!("{destination}.{STAGING_INFIX}-{token}{executable_suffix}"),
            replaced_path: format!("{destination}.{REPLACED_INFIX}-{token}"),
            token: token.to_string(),
        }
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn replaced_path(&self) -> &str {
        &self.replaced_path
    }

    pub fn token(&self) -> &str {
        &self.token
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn staging_and_replacement_names_share_the_sweep_infixes() {
        let posix = InstallStaging::with_token("/opt/cairn-exec", InstallPlatform::Posix, "abc");
        assert_eq!(posix.path(), "/opt/cairn-exec.cairn-install-abc");
        assert_eq!(posix.replaced_path(), "/opt/cairn-exec.cairn-replaced-abc");
        assert!(posix.path().contains(STAGING_INFIX));
        assert!(posix.replaced_path().contains(REPLACED_INFIX));

        let windows = InstallStaging::with_token(
            r"C:\Users\dev\cairn-exec.exe",
            InstallPlatform::Windows,
            "abc",
        );
        assert_eq!(
            windows.path(),
            r"C:\Users\dev\cairn-exec.exe.cairn-install-abc.exe"
        );
        assert_eq!(
            windows.replaced_path(),
            r"C:\Users\dev\cairn-exec.exe.cairn-replaced-abc"
        );
    }
}
