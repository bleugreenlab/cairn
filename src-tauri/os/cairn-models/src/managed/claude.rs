//! The Claude Code CLI entry: Anthropic's native per-platform builds, each a
//! single bare executable served by version from its release bucket (the
//! source `claude.ai/install.sh` installs from).

use std::path::Path;
use std::sync::OnceLock;

use super::pin::ToolPin;
use super::spec::{Artifact, ToolSpec};

/// The stream-json launch contract this build speaks: the argument vector and
/// environment Cairn starts `claude` with, the input frames it writes, and
/// the output it parses.
///
/// Increment it only when Cairn's own Claude client code changes to follow a
/// CLI change (a renamed flag, a reshaped frame, a new required field). The
/// bump PR that makes those changes increments `integration` in
/// `claude-pin.json` to match, so it publishes under a new
/// `claude-integration-v{N}` tag and builds that predate the changes keep the
/// last pin published for their own `N`. A version bump that needs no Cairn
/// change leaves it alone.
pub const CLAUDE_INTEGRATION: u32 = 1;

/// Where Anthropic publishes Claude Code builds, by version and platform. A
/// pin chooses only a version and its digests; the host is fixed here.
const CLAUDE_RELEASES: &str = "https://downloads.claude.ai/claude-code-releases";

/// Every platform Anthropic publishes a Claude Code build for, spelled as its
/// release manifest spells them.
pub const CLAUDE_TARGETS: [&str; 8] = [
    "darwin-arm64",
    "darwin-x64",
    "linux-x64",
    "linux-arm64",
    "linux-x64-musl",
    "linux-arm64-musl",
    "win32-x64",
    "win32-arm64",
];

pub static CLAUDE: ToolSpec = ToolSpec {
    name: "claude",
    display_name: "Claude Code",
    publisher: "Anthropic",
    integration: CLAUDE_INTEGRATION,
    embedded_pin: include_str!("../../claude-pin.json"),
    supported_targets: &CLAUDE_TARGETS,
    host_target,
    package_url,
    asset_file_name,
    download_file_name,
    check_tag,
    artifact: Artifact::Executable {
        file: executable_name,
    },
    probe_env: hardening_env,
    // No `<cairn_home>/bin/claude` forwarder. Codex's shim is safe because
    // Codex has no self-updater; a forwarder to the managed Claude on agent
    // PATH would run it without Cairn's launch environment, and so with its
    // self-updater live, which manages the user's own `~/.local/bin/claude`.
    shim: false,
};

fn executable_name(target: &str) -> &'static str {
    if target.starts_with("win32-") {
        "claude.exe"
    } else {
        "claude"
    }
}

/// Where Anthropic serves `target`'s build of `pin`'s version.
pub fn package_url(pin: &ToolPin, target: &str) -> String {
    format!(
        "{CLAUDE_RELEASES}/{}/{target}/{}",
        pin.version,
        executable_name(target)
    )
}

/// Anthropic's manifest for Claude Code `version`: the version and every
/// platform's checksum and size.
pub fn release_manifest_url(version: &str) -> String {
    format!("{CLAUDE_RELEASES}/{version}/manifest.json")
}

fn asset_file_name(target: &str) -> String {
    format!("{target}/{}", executable_name(target))
}

fn download_file_name(target: &str, version: &str) -> String {
    let extension = if target.starts_with("win32-") {
        ".exe"
    } else {
        ""
    };
    format!("claude-{target}-{version}{extension}")
}

/// Anthropic's builds are addressed by version alone; a Claude pin carries
/// no tag.
fn check_tag(pin: &ToolPin) -> Result<(), String> {
    match &pin.tag {
        None => Ok(()),
        Some(tag) => Err(format!(
            "the pin's tag {tag:?} is not used: Anthropic's builds are addressed by version alone"
        )),
    }
}

fn hardening_env() -> Vec<(&'static str, &'static str)> {
    crate::session::claude::CLAUDE_HARDENING_ENV.to_vec()
}

/// This host's Claude Code platform, or `None` where Anthropic publishes
/// none. On Linux the C library is detected at runtime: the musl build fails
/// to start on glibc, and the glibc build on musl.
pub fn host_target() -> Option<&'static str> {
    if cfg!(all(target_os = "macos", target_arch = "aarch64")) {
        Some("darwin-arm64")
    } else if cfg!(all(target_os = "macos", target_arch = "x86_64")) {
        Some("darwin-x64")
    } else if cfg!(all(target_os = "windows", target_arch = "x86_64")) {
        Some("win32-x64")
    } else if cfg!(all(target_os = "windows", target_arch = "aarch64")) {
        Some("win32-arm64")
    } else if cfg!(all(target_os = "linux", target_arch = "x86_64")) {
        Some(linux_target(false, host_is_musl()))
    } else if cfg!(all(target_os = "linux", target_arch = "aarch64")) {
        Some(linux_target(true, host_is_musl()))
    } else {
        None
    }
}

fn linux_target(arm64: bool, musl: bool) -> &'static str {
    match (arm64, musl) {
        (false, false) => "linux-x64",
        (true, false) => "linux-arm64",
        (false, true) => "linux-x64-musl",
        (true, true) => "linux-arm64-musl",
    }
}

fn host_is_musl() -> bool {
    static MUSL: OnceLock<bool> = OnceLock::new();
    *MUSL.get_or_init(|| {
        libc_is_musl(&|path| path.exists(), &|| {
            let output = std::process::Command::new("ldd")
                .arg("/bin/ls")
                .stdin(std::process::Stdio::null())
                .output()
                .ok()?;
            let mut text = String::from_utf8_lossy(&output.stdout).into_owned();
            text.push_str(&String::from_utf8_lossy(&output.stderr));
            Some(text)
        })
    })
}

/// `install.sh`'s rule: a musl loader under `/lib`, or `ldd /bin/ls`
/// mentioning musl on either stream.
fn libc_is_musl(exists: &dyn Fn(&Path) -> bool, ldd: &dyn Fn() -> Option<String>) -> bool {
    exists(Path::new("/lib/libc.musl-x86_64.so.1"))
        || exists(Path::new("/lib/libc.musl-aarch64.so.1"))
        || ldd().is_some_and(|output| output.contains("musl"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_build_url_is_always_anthropics_bucket_for_the_pinned_version() {
        let pin = CLAUDE.embedded();
        assert_eq!(
            package_url(&pin, "linux-x64"),
            format!(
                "https://downloads.claude.ai/claude-code-releases/{}/linux-x64/claude",
                pin.version
            )
        );
        assert_eq!(
            package_url(&pin, "win32-arm64"),
            format!(
                "https://downloads.claude.ai/claude-code-releases/{}/win32-arm64/claude.exe",
                pin.version
            )
        );
        assert_eq!(
            download_file_name("win32-x64", "2.1.311"),
            "claude-win32-x64-2.1.311.exe"
        );
        assert_eq!(
            download_file_name("darwin-arm64", "2.1.311"),
            "claude-darwin-arm64-2.1.311"
        );
    }

    #[test]
    fn the_claude_pin_is_published_beside_codexs() {
        std::env::remove_var("CAIRN_RELEASE_BASE_URL");
        assert_eq!(
            CLAUDE.published_pin_url(),
            "https://github.com/bleugreenlab/cairn/releases/download/claude-integration-v1/claude-pin.json"
        );
    }

    #[test]
    fn linux_picks_the_build_for_its_c_library() {
        let none = |_: &Path| false;
        let glibc = || Some("\tlinux-vdso.so.1\n\tlibc.so.6 => /lib64/libc.so.6\n".to_string());
        assert!(!libc_is_musl(&none, &glibc));
        assert!(!libc_is_musl(&none, &|| None));

        let alpine_loader = |path: &Path| path == Path::new("/lib/libc.musl-x86_64.so.1");
        assert!(libc_is_musl(&alpine_loader, &|| None));
        let arm_loader = |path: &Path| path == Path::new("/lib/libc.musl-aarch64.so.1");
        assert!(libc_is_musl(&arm_loader, &|| None));
        let musl_ldd = || Some("musl libc (x86_64)\nVersion 1.2.4\n".to_string());
        assert!(libc_is_musl(&none, &musl_ldd));

        assert_eq!(linux_target(false, false), "linux-x64");
        assert_eq!(linux_target(true, false), "linux-arm64");
        assert_eq!(linux_target(false, true), "linux-x64-musl");
        assert_eq!(linux_target(true, true), "linux-arm64-musl");
    }

    #[test]
    fn the_version_probe_runs_under_the_launch_hardening() {
        assert_eq!(
            (CLAUDE.probe_env)(),
            crate::session::claude::CLAUDE_HARDENING_ENV.to_vec()
        );
    }
}
