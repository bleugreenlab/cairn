//! What one managed CLI contributes to the managed-tool mechanism: the only
//! per-tool surface. Everything else (pin following, download, digest check,
//! staging, swap, prune, probe) is [`super::ManagedTool`]'s, shared.

use super::pin::ToolPin;

/// Where Cairn's own rolling releases are published.
const CAIRN_RELEASES: &str = "https://github.com/bleugreenlab/cairn/releases/download";

/// The file a release written by the engine itself (an [`Artifact::Executable`])
/// carries beside its entrypoint, naming what it holds.
pub const RELEASE_MARKER: &str = "cairn-release.json";

/// How an upstream package becomes a release directory. This is the one
/// structural difference between the managed CLIs.
#[derive(Debug, Clone, Copy)]
pub enum Artifact {
    /// A `.tar.gz` unpacked whole. `manifest` is the upstream file inside it
    /// that names the package's `version`, `target` and `entrypoint`
    /// (Codex's `codex-package.json`); a release is complete when it matches
    /// the pin.
    TarGzTree { manifest: &'static str },
    /// One bare executable. The digest-checked download is moved (never
    /// copied: it is hundreds of megabytes) into the release as
    /// `file(target)` and made executable, and the engine writes
    /// [`RELEASE_MARKER`] beside it.
    Executable { file: fn(&str) -> &'static str },
}

/// The names a refusal speaks in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ToolLabel {
    /// The executable's name, as a user would find it on a `PATH`.
    pub name: &'static str,
    /// The product name, as `{display_name} CLI` reads.
    pub display_name: &'static str,
    /// Who publishes the packages.
    pub publisher: &'static str,
}

/// A managed CLI.
pub struct ToolSpec {
    /// The directory under `<cairn_home>/tools`, the shim's name, and the
    /// prefix of the published pin's tag and file.
    pub name: &'static str,
    pub display_name: &'static str,
    pub publisher: &'static str,
    /// The launch contract this build speaks. A pin for another integration
    /// is refused, and a release installed for another is never launched.
    pub integration: u32,
    /// The pin this build was compiled with (`include_str!` of the tool's
    /// `<name>-pin.json`).
    pub embedded_pin: &'static str,
    /// Every package target the publisher ships that Cairn runs on, in the
    /// publisher's own spelling. A pin must cover all of them.
    pub supported_targets: &'static [&'static str],
    /// This host's target, or `None` where the publisher ships none.
    pub host_target: fn() -> Option<&'static str>,
    /// Where the publisher serves `target`'s package for the pin. A pin
    /// chooses a version and digests, never a host.
    pub package_url: fn(&ToolPin, &str) -> String,
    /// The package's file name in the publisher's release, for refusals.
    pub asset_file_name: fn(&str) -> String,
    /// The file under `downloads/` a package for `(target, version)` is
    /// downloaded to, and where an operator may drop one by hand.
    pub download_file_name: fn(&str, &str) -> String,
    /// The tool's rule for the pin's `tag`.
    pub check_tag: fn(&ToolPin) -> Result<(), String>,
    pub artifact: Artifact,
    /// The environment `<entrypoint> --version` runs with.
    pub probe_env: fn() -> Vec<(&'static str, &'static str)>,
    /// Whether `<cairn_home>/bin/<name>` forwards to the active release, so
    /// agent shells resolve the same CLI Cairn runs.
    pub shim: bool,
}

impl ToolSpec {
    pub fn label(&self) -> ToolLabel {
        ToolLabel {
            name: self.name,
            display_name: self.display_name,
            publisher: self.publisher,
        }
    }

    /// The pin this build was compiled with.
    pub fn embedded(&'static self) -> ToolPin {
        ToolPin::parse(self, self.embedded_pin).unwrap_or_else(|error| {
            panic!(
                "{}-pin.json is invalid ({error}); the managed:: tests assert it",
                self.name
            )
        })
    }

    /// The rolling release the pin for this build's integration is
    /// published under: `{name}-integration-v{N}`.
    pub fn release_tag(&self) -> String {
        format!("{}-integration-v{}", self.name, self.integration)
    }

    /// The pin's file name, in the repository and in the release.
    pub fn pin_file_name(&self) -> String {
        format!("{}-pin.json", self.name)
    }

    /// Where the pin for this build's integration is published.
    /// `CAIRN_RELEASE_BASE_URL` replaces the GitHub release with a mirror laid
    /// out the same way, as it does for the executor distribution.
    pub fn published_pin_url(&self) -> String {
        let base = std::env::var("CAIRN_RELEASE_BASE_URL")
            .ok()
            .map(|base| base.trim().trim_end_matches('/').to_string())
            .filter(|base| !base.is_empty())
            .unwrap_or_else(|| CAIRN_RELEASES.to_string());
        format!("{base}/{}/{}", self.release_tag(), self.pin_file_name())
    }
}
