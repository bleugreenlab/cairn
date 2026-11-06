//! The Codex CLI entry: OpenAI's per-target package archives, published on
//! its GitHub releases, unpacked whole.

use super::pin::ToolPin;
use super::spec::{Artifact, ToolSpec};

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

/// Every target OpenAI publishes a Codex package for that Cairn runs on.
/// Linux maps to the static musl packages.
pub const CODEX_TARGETS: [&str; 6] = [
    "aarch64-apple-darwin",
    "x86_64-apple-darwin",
    "aarch64-unknown-linux-musl",
    "x86_64-unknown-linux-musl",
    "aarch64-pc-windows-msvc",
    "x86_64-pc-windows-msvc",
];

pub static CODEX: ToolSpec = ToolSpec {
    name: "codex",
    display_name: "Codex",
    publisher: "OpenAI",
    integration: CODEX_INTEGRATION,
    embedded_pin: include_str!("../../codex-pin.json"),
    supported_targets: &CODEX_TARGETS,
    host_target,
    package_url,
    asset_file_name: package_file_name,
    download_file_name,
    check_tag,
    artifact: Artifact::TarGzTree {
        manifest: "codex-package.json",
    },
    probe_env: Vec::new,
    shim: true,
};

/// This host's Codex package target, or `None` where OpenAI publishes none.
pub fn host_target() -> Option<&'static str> {
    let target = if cfg!(all(target_os = "macos", target_arch = "aarch64")) {
        CODEX_TARGETS[0]
    } else if cfg!(all(target_os = "macos", target_arch = "x86_64")) {
        CODEX_TARGETS[1]
    } else if cfg!(all(target_os = "linux", target_arch = "aarch64")) {
        CODEX_TARGETS[2]
    } else if cfg!(all(target_os = "linux", target_arch = "x86_64")) {
        CODEX_TARGETS[3]
    } else if cfg!(all(target_os = "windows", target_arch = "aarch64")) {
        CODEX_TARGETS[4]
    } else if cfg!(all(target_os = "windows", target_arch = "x86_64")) {
        CODEX_TARGETS[5]
    } else {
        return None;
    };
    Some(target)
}

/// The archive's file name in OpenAI's release.
pub fn package_file_name(target: &str) -> String {
    format!("codex-package-{target}.tar.gz")
}

fn download_file_name(target: &str, version: &str) -> String {
    format!("codex-package-{target}-{version}.tar.gz")
}

/// Where OpenAI publishes `target`'s package for `pin`: the release tagged
/// `rust-v{version}`, which [`check_tag`] holds the pin's tag to.
pub fn package_url(pin: &ToolPin, target: &str) -> String {
    format!(
        "{CODEX_RELEASES}/rust-v{}/{}",
        pin.version,
        package_file_name(target)
    )
}

/// OpenAI's release page for Codex `version`.
pub fn release_page_url(version: &str) -> String {
    format!("https://github.com/openai/codex/releases/tag/rust-v{version}")
}

/// Codex pins carry OpenAI's tag, always `rust-v{version}`. It is kept (and
/// required) because runners on builds that predate the managed-tool
/// mechanism parse the published Codex pin with a required `tag`.
fn check_tag(pin: &ToolPin) -> Result<(), String> {
    let expected = format!("rust-v{}", pin.version);
    match &pin.tag {
        Some(tag) if *tag == expected => Ok(()),
        other => Err(format!("the pin's tag {other:?} is not {expected}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_package_url_is_always_openais_release_for_the_pinned_version() {
        let pin = CODEX.embedded();
        assert_eq!(
            package_url(&pin, "aarch64-apple-darwin"),
            format!(
                "https://github.com/openai/codex/releases/download/rust-v{}/codex-package-aarch64-apple-darwin.tar.gz",
                pin.version
            )
        );
    }

    #[test]
    fn the_codex_pin_is_published_where_older_runners_follow_it() {
        std::env::remove_var("CAIRN_RELEASE_BASE_URL");
        assert_eq!(
            CODEX.published_pin_url(),
            "https://github.com/bleugreenlab/cairn/releases/download/codex-integration-v1/codex-pin.json"
        );
    }
}
