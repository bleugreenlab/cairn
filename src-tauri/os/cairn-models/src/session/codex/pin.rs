//! The Codex CLI release Cairn runs: which version, with which checksums, for
//! which app-server dialect.
//!
//! `codex-pin.json` (beside this crate's `Cargo.toml`) is the one place a
//! Codex version is written. This build embeds it; CI republishes it on every
//! merge to `main` as the rolling `codex-integration-v{N}` release, which
//! [`super::managed::ManagedCodex`] follows at runtime, so a compatible Codex
//! bump reaches existing installs without an app release (see
//! `docs/backends.md`, "Managed Codex CLI").

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// The app-server dialect this build speaks.
///
/// Increment it when Cairn's own code has to change to drive a newer Codex
/// (a renamed method, a reshaped notification, a new required capability).
/// The bump PR that makes those changes increments `integration` in
/// `codex-pin.json` to match, so it publishes under a new
/// `codex-integration-v{N}` tag and builds that predate the changes keep the
/// last pin published for their own `N`. A bump that needs no Cairn change
/// leaves it alone.
pub const CODEX_INTEGRATION: u32 = 1;

/// The GitHub repository OpenAI publishes Codex releases from. A pin chooses
/// only a version and its digests; the host is fixed here.
const CODEX_RELEASES: &str = "https://github.com/openai/codex/releases/download";

/// Where Cairn's own rolling releases are published.
const CAIRN_RELEASES: &str = "https://github.com/bleugreenlab/cairn/releases/download";

/// Every target OpenAI publishes a Codex package for that Cairn runs on, in
/// [`host_target`]'s terms. Linux maps to the static musl packages.
pub const SUPPORTED_TARGETS: [&str; 6] = [
    "aarch64-apple-darwin",
    "x86_64-apple-darwin",
    "aarch64-unknown-linux-musl",
    "x86_64-unknown-linux-musl",
    "aarch64-pc-windows-msvc",
    "x86_64-pc-windows-msvc",
];

/// One Codex release, as `codex-pin.json` records it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodexPin {
    /// The [`CODEX_INTEGRATION`] this pin was verified against.
    pub integration: u32,
    /// What `codex --version` reports, without the `codex-cli ` prefix.
    pub version: String,
    /// OpenAI's release tag, always `rust-v{version}`.
    pub tag: String,
    /// Why this version was chosen.
    pub reason: String,
    /// The release's `codex-package-<target>.tar.gz`, by target.
    pub packages: BTreeMap<String, CodexPackage>,
}

/// One target's package archive.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodexPackage {
    /// Lowercase hex SHA-256 of the archive, from the release's
    /// `codex-package_SHA256SUMS`.
    pub sha256: String,
    /// The archive's size in bytes; a download is cut off past it.
    pub size: u64,
}

impl CodexPin {
    /// Parse and [`validate`](Self::validate) a pin document.
    pub fn parse(text: &str) -> Result<Self, String> {
        let pin: Self =
            serde_json::from_str(text).map_err(|error| format!("not a Codex pin: {error}"))?;
        pin.validate()?;
        Ok(pin)
    }

    /// Refuse a pin this build cannot install from: another dialect, a
    /// version or tag that could steer the package URL anywhere but OpenAI's
    /// release for that version, or a malformed digest.
    pub fn validate(&self) -> Result<(), String> {
        if self.integration != CODEX_INTEGRATION {
            return Err(format!(
                "the pin is for Codex integration {}, this build speaks {CODEX_INTEGRATION}",
                self.integration
            ));
        }
        let version_ok = !self.version.is_empty()
            && self.version.starts_with(|c: char| c.is_ascii_digit())
            && self
                .version
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-');
        if !version_ok {
            return Err(format!("the pin's version {:?} is malformed", self.version));
        }
        if self.tag != format!("rust-v{}", self.version) {
            return Err(format!(
                "the pin's tag {:?} is not rust-v{}",
                self.tag, self.version
            ));
        }
        if self.reason.trim().is_empty() {
            return Err("the pin records no reason".to_string());
        }
        if self.packages.is_empty() {
            return Err("the pin lists no packages".to_string());
        }
        for (target, package) in &self.packages {
            if !SUPPORTED_TARGETS.contains(&target.as_str()) {
                return Err(format!("the pin names an unknown target {target:?}"));
            }
            let digest_ok = package.sha256.len() == 64
                && package
                    .sha256
                    .chars()
                    .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c));
            if !digest_ok {
                return Err(format!("the pin's sha256 for {target} is malformed"));
            }
            if package.size == 0 {
                return Err(format!("the pin's size for {target} is zero"));
            }
        }
        Ok(())
    }

    pub fn package(&self, target: &str) -> Option<&CodexPackage> {
        self.packages.get(target)
    }
}

/// The pin this build was compiled with.
pub fn embedded_pin() -> CodexPin {
    CodexPin::parse(include_str!("../../../codex-pin.json"))
        .expect("codex-pin.json is valid (pin.rs tests assert it)")
}

/// This host's Codex package target, or `None` where OpenAI publishes none.
pub fn host_target() -> Option<&'static str> {
    let target = if cfg!(all(target_os = "macos", target_arch = "aarch64")) {
        SUPPORTED_TARGETS[0]
    } else if cfg!(all(target_os = "macos", target_arch = "x86_64")) {
        SUPPORTED_TARGETS[1]
    } else if cfg!(all(target_os = "linux", target_arch = "aarch64")) {
        SUPPORTED_TARGETS[2]
    } else if cfg!(all(target_os = "linux", target_arch = "x86_64")) {
        SUPPORTED_TARGETS[3]
    } else if cfg!(all(target_os = "windows", target_arch = "aarch64")) {
        SUPPORTED_TARGETS[4]
    } else if cfg!(all(target_os = "windows", target_arch = "x86_64")) {
        SUPPORTED_TARGETS[5]
    } else {
        return None;
    };
    Some(target)
}

/// The archive's file name in OpenAI's release.
pub fn package_file_name(target: &str) -> String {
    format!("codex-package-{target}.tar.gz")
}

/// Where OpenAI publishes `target`'s package for `pin`.
pub fn package_url(pin: &CodexPin, target: &str) -> String {
    format!("{CODEX_RELEASES}/{}/{}", pin.tag, package_file_name(target))
}

/// OpenAI's release page for Codex `version`.
pub fn release_page_url(version: &str) -> String {
    format!("https://github.com/openai/codex/releases/tag/rust-v{version}")
}

/// Where the pin for this build's [`CODEX_INTEGRATION`] is published.
/// `CAIRN_RELEASE_BASE_URL` replaces the GitHub release with a mirror laid out
/// the same way, as it does for the executor distribution.
pub fn published_pin_url() -> String {
    let base = std::env::var("CAIRN_RELEASE_BASE_URL")
        .ok()
        .map(|base| base.trim().trim_end_matches('/').to_string())
        .filter(|base| !base.is_empty())
        .unwrap_or_else(|| CAIRN_RELEASES.to_string());
    format!("{base}/codex-integration-v{CODEX_INTEGRATION}/codex-pin.json")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_embedded_pin_speaks_this_builds_integration_and_covers_every_target() {
        let pin = embedded_pin();
        assert_eq!(pin.integration, CODEX_INTEGRATION);
        for target in SUPPORTED_TARGETS {
            assert!(pin.package(target).is_some(), "no package for {target}");
        }
        if let Some(target) = host_target() {
            assert!(SUPPORTED_TARGETS.contains(&target));
        }
    }

    #[test]
    fn a_package_url_is_always_openais_release_for_the_pinned_version() {
        let pin = embedded_pin();
        assert_eq!(
            package_url(&pin, "aarch64-apple-darwin"),
            format!(
                "https://github.com/openai/codex/releases/download/rust-v{}/codex-package-aarch64-apple-darwin.tar.gz",
                pin.version
            )
        );
    }

    #[test]
    fn a_pin_that_could_steer_the_download_is_refused() {
        let mut pin = embedded_pin();
        pin.tag = "rust-v0.153.3/../../evil".to_string();
        assert!(pin.validate().unwrap_err().contains("tag"));

        let mut pin = embedded_pin();
        pin.version = "0.1/../x".to_string();
        pin.tag = format!("rust-v{}", pin.version);
        assert!(pin.validate().unwrap_err().contains("malformed"));

        let mut pin = embedded_pin();
        pin.integration += 1;
        assert!(pin.validate().unwrap_err().contains("integration"));

        let mut pin = embedded_pin();
        pin.packages.get_mut("x86_64-apple-darwin").unwrap().sha256 = "ABC".into();
        assert!(pin.validate().unwrap_err().contains("sha256"));
    }
}
