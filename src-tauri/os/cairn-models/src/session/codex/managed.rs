//! The Codex CLI Cairn runs: installed by Cairn from the pinned release into
//! `<cairn_home>/tools/codex`, verified, and resolved by absolute path. A
//! `codex` on the user's `PATH` is never consulted.
//!
//! Layout under the root:
//!
//! - `<version>-<target>/`: one unpacked OpenAI package (`bin/codex`,
//!   `codex-package.json`, its resources), as OpenAI's `install.sh` lays it out.
//! - `current.json`: which release is active. It is replaced by a rename, so a
//!   reader sees the old release or the new one, never neither.
//! - `pin.json`: the last published pin that validated, and its ETag.
//! - `downloads/codex-package-<target>-<version>.tar.gz`: where an archive is
//!   downloaded to, and where an operator may drop one by hand; it is used if
//!   its digest matches the pin.
//!
//! `<cairn_home>/bin/codex` forwards to the active release, so agent shells
//! resolve the same Codex Cairn runs.

use std::fmt;
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::pin::{self, CodexPackage, CodexPin, CODEX_INTEGRATION};

/// How often a running host checks the published pin.
pub const UPDATE_INTERVAL: Duration = Duration::from_secs(6 * 60 * 60);

/// How long the unpacked CLI may take to report its version.
const PROBE_TIMEOUT: Duration = Duration::from_secs(30);

/// A staging dir this old belongs to an install that died.
const STALE_STAGING: Duration = Duration::from_secs(60 * 60);

/// Fetches the published pin document.
pub trait PinSource: Send + Sync {
    /// The document, or [`PinFetch::NotModified`] when it still carries
    /// `etag`.
    fn fetch(&self, etag: Option<&str>) -> Result<PinFetch, String>;
}

pub enum PinFetch {
    NotModified,
    Body { text: String, etag: Option<String> },
}

/// Downloads a package archive.
pub trait Fetch: Send + Sync {
    /// Write `url`'s body to `dest`, failing once it exceeds `max_bytes`.
    fn download(&self, url: &str, dest: &Path, max_bytes: u64) -> Result<(), String>;
}

/// Which pin an install followed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PinOrigin {
    /// The pin CI published for this build's integration.
    Published,
    /// The last published pin that validated, while the publication is
    /// unreachable.
    Cached,
    /// The pin this build was compiled with.
    Embedded,
}

impl fmt::Display for PinOrigin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Published => "published",
            Self::Cached => "cached",
            Self::Embedded => "embedded",
        })
    }
}

/// The active Codex CLI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolved {
    /// The release's entrypoint, absolute.
    pub path: PathBuf,
    pub version: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpdateOutcome {
    /// The effective pin is already active.
    Current(Resolved),
    /// A new release was installed and is now active.
    Installed {
        resolved: Resolved,
        previous: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManagedCodexStatus {
    Installed {
        version: String,
        pin_source: PinOrigin,
    },
    Installing {
        version: String,
    },
    /// Nothing is installed, and the last attempt failed with this text.
    Failed(String),
    NotInstalled,
}

/// What a refusal tells the operator to do by hand.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManualRoute {
    pub version: String,
    pub asset: String,
    pub url: String,
    pub sha256: String,
    pub archive: PathBuf,
}

/// Why Cairn has no Codex CLI to run. The route is boxed to keep the error
/// small enough to return by value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodexInstallError {
    UnsupportedPlatform {
        os: &'static str,
        arch: &'static str,
    },
    Download {
        route: Box<ManualRoute>,
        reason: String,
    },
    Integrity {
        route: Box<ManualRoute>,
        expected: String,
        actual: String,
    },
    Extract {
        route: Box<ManualRoute>,
        reason: String,
    },
    Probe {
        route: Box<ManualRoute>,
        found: Option<String>,
    },
}

const NO_PATH_FALLBACK: &str = "Cairn does not fall back to a `codex` found on your PATH.";
const RETRIES: &str = "It retries the next time a Codex session starts, at its next six-hourly update check, and at every restart.";

fn abbreviate(digest: &str) -> String {
    match digest.get(..12) {
        Some(head) if digest.len() > 12 => format!("{head}…"),
        _ => digest.to_string(),
    }
}

impl fmt::Display for CodexInstallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (route, failure) = match self {
            Self::UnsupportedPlatform { os, arch } => {
                return write!(
                    f,
                    "Cairn runs its own pinned Codex CLI, and OpenAI publishes no Codex package for this platform ({os}/{arch}), so Codex sessions cannot run here. {NO_PATH_FALLBACK}"
                );
            }
            Self::Download { route, reason } => (
                route,
                format!("the download of {} failed ({reason})", route.asset),
            ),
            Self::Integrity {
                route,
                expected,
                actual,
            } => (
                route,
                format!(
                    "the download of {} did not match its pinned checksum (expected sha256 {}, got {})",
                    route.asset,
                    abbreviate(expected),
                    abbreviate(actual)
                ),
            ),
            Self::Extract { route, reason } => (
                route,
                format!("unpacking {} failed ({reason})", route.asset),
            ),
            Self::Probe { route, found } => {
                let found = found.as_deref().unwrap_or("no version it could read");
                return write!(
                    f,
                    "Cairn could not install its pinned Codex CLI {}: the unpacked `codex --version` reported {found}, not {}. {NO_PATH_FALLBACK} {RETRIES}",
                    route.version, route.version
                );
            }
        };
        write!(
            f,
            "Cairn could not install its pinned Codex CLI {}: {failure}. {NO_PATH_FALLBACK} {RETRIES} To install by hand, download {}, confirm its SHA-256 is {}, and save it as {}.",
            route.version,
            route.url,
            route.sha256,
            route.archive.display()
        )
    }
}

impl std::error::Error for CodexInstallError {}

/// `current.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Current {
    version: String,
    target: String,
    /// The [`CODEX_INTEGRATION`] of the build that installed it. A release
    /// from another integration is never launched: after an upgrade or a
    /// rollback it is as good as not installed.
    integration: u32,
    pin_source: PinOrigin,
}

/// `pin.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CachedPin {
    etag: Option<String>,
    pin: CodexPin,
}

/// The fields of OpenAI's `codex-package.json` an install checks.
#[derive(Debug, Deserialize)]
struct PackageManifest {
    version: String,
    target: String,
    entrypoint: String,
}

#[derive(Default)]
struct State {
    installing: Option<String>,
    last_error: Option<CodexInstallError>,
}

/// Installs, follows, and resolves the Codex CLI Cairn runs. One per host;
/// share it.
pub struct ManagedCodex {
    root: PathBuf,
    bin_dir: PathBuf,
    target: Option<&'static str>,
    embedded: CodexPin,
    pins: Arc<dyn PinSource>,
    fetch: Arc<dyn Fetch>,
    prefer_embedded: bool,
    lock: Mutex<()>,
    state: Mutex<State>,
}

impl ManagedCodex {
    /// A Codex installed under `root`, with its forwarder in `bin_dir`,
    /// following `pins` and downloading through `fetch`.
    pub fn new(
        root: impl Into<PathBuf>,
        bin_dir: impl Into<PathBuf>,
        pins: Arc<dyn PinSource>,
        fetch: Arc<dyn Fetch>,
    ) -> Self {
        Self {
            root: root.into(),
            bin_dir: bin_dir.into(),
            target: pin::host_target(),
            embedded: pin::embedded_pin(),
            pins,
            fetch,
            prefer_embedded: false,
            lock: Mutex::new(()),
            state: Mutex::new(State::default()),
        }
    }

    /// This host's managed Codex: `<cairn_home>/tools/codex`, following the
    /// published pin, except outside production, where the embedded pin wins
    /// so a branch's bump is what its dev instance runs.
    ///
    /// Test builds get a per-process scratch root and sources that refuse, so
    /// no test can download a Codex or touch the real Cairn home.
    pub fn for_host() -> Self {
        #[cfg(any(test, feature = "test-utils"))]
        {
            let scratch =
                std::env::temp_dir().join(format!("cairn-test-codex-{}", std::process::id()));
            Self::new(
                scratch.join("tools").join("codex"),
                scratch.join("bin"),
                Arc::new(RefusingSource),
                Arc::new(RefusingSource),
            )
        }
        #[cfg(not(any(test, feature = "test-utils")))]
        {
            Self::new(
                cairn_common::paths::cairn_home()
                    .join("tools")
                    .join("codex"),
                cairn_common::toolchain_path::cairn_bin_dir(),
                Arc::new(PublishedPin::new(pin::published_pin_url())),
                Arc::new(HttpFetch),
            )
            .prefer_embedded(cairn_common::paths::is_dev())
        }
    }

    /// Follow the embedded pin and never the published one.
    pub fn prefer_embedded(mut self, prefer: bool) -> Self {
        self.prefer_embedded = prefer;
        self
    }

    /// Install for `target` instead of this host's (`None`: unsupported).
    pub fn with_target(mut self, target: Option<&'static str>) -> Self {
        self.target = target;
        self
    }

    /// Treat `pin` as the one this build was compiled with.
    pub fn with_embedded_pin(mut self, pin: CodexPin) -> Self {
        self.embedded = pin;
        self
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Whether OpenAI publishes a Codex package for this host.
    pub fn supported(&self) -> bool {
        self.target.is_some()
    }

    /// The active release, if one is installed and complete and was
    /// installed for this build's integration. Never touches the network.
    pub fn installed(&self) -> Option<Resolved> {
        let target = self.target?;
        let current = self.read_current()?;
        if current.target != target || current.integration != CODEX_INTEGRATION {
            return None;
        }
        let release = self.release_dir(&current.version, target)?;
        let path = complete_release(&release, &current.version, target)?;
        Some(Resolved {
            path,
            version: current.version,
        })
    }

    /// The Codex to launch: whatever is installed, even if it is behind the
    /// pin (a launch never waits on a version check), else the effective pin
    /// installed now.
    pub fn ensure(&self) -> Result<Resolved, CodexInstallError> {
        let target = self.require_target()?;
        if let Some(resolved) = self.installed() {
            return Ok(resolved);
        }
        let _install = self.install_lock();
        // An install that held the lock while this call waited may have
        // finished it.
        if let Some(resolved) = self.installed() {
            return Ok(resolved);
        }
        let (pin, origin) = self.effective_pin();
        self.install(&pin, origin, target)
    }

    /// Follow the effective pin: install it beside the active release and
    /// swap to it if it differs. A failure keeps the active release.
    pub fn update(&self) -> Result<UpdateOutcome, CodexInstallError> {
        let target = self.require_target()?;
        let _install = self.install_lock();
        self.update_locked(target)
    }

    fn update_locked(&self, target: &'static str) -> Result<UpdateOutcome, CodexInstallError> {
        let (pin, origin) = self.effective_pin();
        let current = self.installed();
        if let Some(resolved) = current.as_ref().filter(|r| r.version == pin.version) {
            return Ok(UpdateOutcome::Current(resolved.clone()));
        }
        let previous = current.map(|resolved| resolved.version);
        let resolved = self.install(&pin, origin, target)?;
        Ok(UpdateOutcome::Installed { resolved, previous })
    }

    /// [`Self::update`], unless an install or update is already running, in
    /// which case `None`.
    pub fn update_if_idle(&self) -> Option<Result<UpdateOutcome, CodexInstallError>> {
        let target = match self.require_target() {
            Ok(target) => target,
            Err(error) => return Some(Err(error)),
        };
        let _install = match self.lock.try_lock() {
            Ok(guard) => guard,
            Err(std::sync::TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => return None,
        };
        Some(self.update_locked(target))
    }

    pub fn status(&self) -> ManagedCodexStatus {
        let (installing, last_error) = {
            let state = self.state_guard();
            (state.installing.clone(), state.last_error.clone())
        };
        if let Some(resolved) = self.installed() {
            let pin_source = self
                .read_current()
                .map(|current| current.pin_source)
                .unwrap_or(PinOrigin::Embedded);
            return ManagedCodexStatus::Installed {
                version: resolved.version,
                pin_source,
            };
        }
        if let Some(version) = installing {
            return ManagedCodexStatus::Installing { version };
        }
        if self.target.is_none() {
            return ManagedCodexStatus::Failed(self.unsupported().to_string());
        }
        match last_error {
            Some(error) => ManagedCodexStatus::Failed(error.to_string()),
            None => ManagedCodexStatus::NotInstalled,
        }
    }

    fn require_target(&self) -> Result<&'static str, CodexInstallError> {
        self.target.ok_or_else(|| self.unsupported())
    }

    fn unsupported(&self) -> CodexInstallError {
        CodexInstallError::UnsupportedPlatform {
            os: std::env::consts::OS,
            arch: std::env::consts::ARCH,
        }
    }

    fn install_lock(&self) -> MutexGuard<'_, ()> {
        self.lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn state_guard(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// `<root>/<version>-<target>`, refusing a version that would name
    /// anything else.
    fn release_dir(&self, version: &str, target: &str) -> Option<PathBuf> {
        let name = format!("{version}-{target}");
        let mut components = Path::new(&name).components();
        match (components.next(), components.next()) {
            (Some(Component::Normal(_)), None) => Some(self.root.join(name)),
            _ => None,
        }
    }

    fn read_current(&self) -> Option<Current> {
        serde_json::from_slice(&std::fs::read(self.root.join("current.json")).ok()?).ok()
    }

    fn read_cached_pin(&self) -> Option<CachedPin> {
        let cached: CachedPin =
            serde_json::from_slice(&std::fs::read(self.root.join("pin.json")).ok()?).ok()?;
        cached.pin.validate().ok()?;
        Some(cached)
    }

    /// The pin to follow: the published one when it validates for this
    /// build, else the last published one that did, else the embedded one.
    fn effective_pin(&self) -> (CodexPin, PinOrigin) {
        if self.prefer_embedded {
            return (self.embedded.clone(), PinOrigin::Embedded);
        }
        let cached = self.read_cached_pin();
        let fetched = self
            .pins
            .fetch(cached.as_ref().and_then(|cached| cached.etag.as_deref()));
        let fallback = |cached: Option<CachedPin>| match cached {
            Some(cached) => (cached.pin, PinOrigin::Cached),
            None => (self.embedded.clone(), PinOrigin::Embedded),
        };
        match fetched {
            Ok(PinFetch::NotModified) => match cached {
                Some(cached) => (cached.pin, PinOrigin::Published),
                None => fallback(None),
            },
            Ok(PinFetch::Body { text, etag }) => match CodexPin::parse(&text) {
                Ok(pin) if self.target.is_some_and(|t| pin.package(t).is_some()) => {
                    let record = CachedPin {
                        etag,
                        pin: pin.clone(),
                    };
                    if let Err(error) = write_json_atomically(&self.root, "pin.json", &record) {
                        log::warn!("managed codex: could not cache the published pin: {error}");
                    }
                    (pin, PinOrigin::Published)
                }
                Ok(_) => {
                    log::warn!("managed codex: the published pin has no package for this host");
                    fallback(cached)
                }
                Err(error) => {
                    log::warn!("managed codex: ignoring the published pin: {error}");
                    fallback(cached)
                }
            },
            Err(error) => {
                log::info!("managed codex: the published pin is unreachable: {error}");
                fallback(cached)
            }
        }
    }

    fn route(&self, pin: &CodexPin, target: &str, package: &CodexPackage) -> Box<ManualRoute> {
        Box::new(ManualRoute {
            version: pin.version.clone(),
            asset: pin::package_file_name(target),
            url: pin::package_url(pin, target),
            sha256: package.sha256.clone(),
            archive: self
                .root
                .join("downloads")
                .join(format!("codex-package-{target}-{}.tar.gz", pin.version)),
        })
    }

    /// Install `pin` and make it active. Runs under the install lock.
    fn install(
        &self,
        pin: &CodexPin,
        origin: PinOrigin,
        target: &'static str,
    ) -> Result<Resolved, CodexInstallError> {
        self.state_guard().installing = Some(pin.version.clone());
        let result = self.install_inner(pin, origin, target);
        let mut state = self.state_guard();
        state.installing = None;
        match &result {
            Ok(_) => state.last_error = None,
            Err(error) => {
                log::warn!("managed codex: {error}");
                state.last_error = Some(error.clone());
            }
        }
        result
    }

    fn install_inner(
        &self,
        pin: &CodexPin,
        origin: PinOrigin,
        target: &'static str,
    ) -> Result<Resolved, CodexInstallError> {
        let Some(package) = pin.package(target) else {
            return Err(self.unsupported());
        };
        let route = self.route(pin, target, package);
        let extract = |reason: String| CodexInstallError::Extract {
            route: route.clone(),
            reason,
        };
        let release = self
            .release_dir(&pin.version, target)
            .ok_or_else(|| extract(format!("the version {:?} is malformed", pin.version)))?;
        let previous = self.read_current();

        if complete_release(&release, &pin.version, target).is_none() {
            self.remove_stale_staging();
            let archive = self.obtain_archive(&route, package)?;
            self.unpack(&archive, &release, &pin.version, target)
                .map_err(extract)?;
            // The archive is not needed once its release is in place.
            let _ = std::fs::remove_file(&archive);
        }
        let entrypoint = complete_release(&release, &pin.version, target)
            .ok_or_else(|| extract("the unpacked package is incomplete".to_string()))?;

        let found = probe_version(&entrypoint);
        if found.as_deref() != Some(pin.version.as_str()) {
            let _ = std::fs::remove_dir_all(&release);
            return Err(CodexInstallError::Probe { route, found });
        }

        let current = Current {
            version: pin.version.clone(),
            target: target.to_string(),
            integration: CODEX_INTEGRATION,
            pin_source: origin,
        };
        write_json_atomically(&self.root, "current.json", &current)
            .map_err(|error| extract(format!("could not record the active release: {error}")))?;
        cairn_platform::env::ensure_tool_shim(
            &self.bin_dir,
            "codex",
            &entrypoint.to_string_lossy(),
        );
        self.prune(
            target,
            &pin.version,
            previous.map(|current| current.version),
        );
        log::info!(
            "managed codex: {} ({origin} pin) is active at {}",
            pin.version,
            entrypoint.display()
        );
        Ok(Resolved {
            path: entrypoint,
            version: pin.version.clone(),
        })
    }

    /// The archive at the route's path, if its digest matches; else a fresh
    /// download, checked before it is moved there.
    fn obtain_archive(
        &self,
        route: &ManualRoute,
        package: &CodexPackage,
    ) -> Result<PathBuf, CodexInstallError> {
        let archive = &route.archive;
        if archive.is_file() {
            match sha256_file(archive) {
                Ok(digest) if digest == package.sha256 => return Ok(archive.clone()),
                Ok(digest) => log::warn!(
                    "managed codex: {} has sha256 {digest}, not the pinned {}; downloading",
                    archive.display(),
                    package.sha256
                ),
                Err(error) => log::warn!("managed codex: {error}; downloading"),
            }
        }
        let download = |reason: String| CodexInstallError::Download {
            route: Box::new(route.clone()),
            reason,
        };
        let downloads = archive.parent().unwrap_or(&self.root);
        std::fs::create_dir_all(downloads)
            .map_err(|error| download(format!("cannot create {}: {error}", downloads.display())))?;
        let partial = downloads.join(format!(
            ".{}.{}-{}.tmp",
            route.asset,
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ));
        let result = (|| {
            self.fetch
                .download(&route.url, &partial, package.size)
                .map_err(download)?;
            let actual = sha256_file(&partial).map_err(download)?;
            if actual != package.sha256 {
                return Err(CodexInstallError::Integrity {
                    route: Box::new(route.clone()),
                    expected: package.sha256.clone(),
                    actual,
                });
            }
            std::fs::rename(&partial, archive).map_err(|error| {
                download(format!("cannot move it to {}: {error}", archive.display()))
            })
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(&partial);
        }
        result.map(|()| archive.clone())
    }

    /// Unpack `archive` into a staging dir, then rename it to `release`.
    fn unpack(
        &self,
        archive: &Path,
        release: &Path,
        version: &str,
        target: &str,
    ) -> Result<(), String> {
        let staging = self.root.join(format!(
            ".staging-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ));
        let result = (|| {
            std::fs::create_dir_all(&staging)
                .map_err(|error| format!("cannot create {}: {error}", staging.display()))?;
            unpack_archive(archive, &staging)?;
            if complete_release(&staging, version, target).is_none() {
                return Err(format!(
                    "it is not a complete Codex {version} package for {target}"
                ));
            }
            if release.exists() {
                // Another process may have installed it meanwhile; a
                // complete one is accepted, anything else is replaced.
                if complete_release(release, version, target).is_some() {
                    return Ok(());
                }
                std::fs::remove_dir_all(release)
                    .map_err(|error| format!("cannot replace {}: {error}", release.display()))?;
            }
            match std::fs::rename(&staging, release) {
                Ok(()) => Ok(()),
                Err(_) if complete_release(release, version, target).is_some() => Ok(()),
                Err(error) => Err(format!("cannot move it to {}: {error}", release.display())),
            }
        })();
        let _ = std::fs::remove_dir_all(&staging);
        result
    }

    /// Remove staging dirs an install that died left behind.
    fn remove_stale_staging(&self) {
        let Ok(entries) = std::fs::read_dir(&self.root) else {
            return;
        };
        for entry in entries.flatten() {
            let name = entry.file_name();
            if !name.to_string_lossy().starts_with(".staging-") {
                continue;
            }
            let stale = entry
                .metadata()
                .and_then(|meta| meta.modified())
                .ok()
                .and_then(|modified| modified.elapsed().ok())
                .is_some_and(|age| age > STALE_STAGING);
            if stale {
                let _ = std::fs::remove_dir_all(entry.path());
            }
        }
    }

    /// Keep the active release and the one before it (sessions may still be
    /// running from it); remove every other release for `target`. Best
    /// effort: a release still in use on Windows is retried next time.
    fn prune(&self, target: &str, active: &str, previous: Option<String>) {
        let keep: Vec<String> = std::iter::once(active.to_string())
            .chain(previous)
            .map(|version| format!("{version}-{target}"))
            .collect();
        let Ok(entries) = std::fs::read_dir(&self.root) else {
            return;
        };
        let suffix = format!("-{target}");
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if !name.ends_with(&suffix) || keep.contains(&name) || !entry.path().is_dir() {
                continue;
            }
            if let Err(error) = std::fs::remove_dir_all(entry.path()) {
                log::debug!("managed codex: could not prune {name}: {error}");
            }
        }
    }
}

/// The entrypoint of the release at `dir`, if its `codex-package.json` names
/// `version` for `target` and the entrypoint exists inside it.
fn complete_release(dir: &Path, version: &str, target: &str) -> Option<PathBuf> {
    let manifest: PackageManifest =
        serde_json::from_slice(&std::fs::read(dir.join("codex-package.json")).ok()?).ok()?;
    if manifest.version != version || manifest.target != target {
        return None;
    }
    let entrypoint = Path::new(&manifest.entrypoint);
    if !stays_inside(entrypoint) {
        return None;
    }
    let path = dir.join(entrypoint);
    path.is_file().then_some(path)
}

/// Whether a relative path names something inside the dir it is joined to.
fn stays_inside(path: &Path) -> bool {
    let mut depth = 0usize;
    for component in path.components() {
        match component {
            Component::Normal(_) => depth += 1,
            Component::CurDir => {}
            Component::ParentDir if depth > 0 => depth -= 1,
            _ => return false,
        }
    }
    depth > 0
}

/// Unpack a `.tar.gz` into `dest`, refusing any entry that would land outside
/// it.
fn unpack_archive(archive: &Path, dest: &Path) -> Result<(), String> {
    let file = std::fs::File::open(archive)
        .map_err(|error| format!("cannot open {}: {error}", archive.display()))?;
    let mut tar = tar::Archive::new(flate2::read::GzDecoder::new(file));
    let entries = tar
        .entries()
        .map_err(|error| format!("cannot read the archive: {error}"))?;
    for entry in entries {
        let mut entry = entry.map_err(|error| format!("cannot read the archive: {error}"))?;
        let path = entry
            .path()
            .map_err(|error| format!("an entry has an unreadable path: {error}"))?
            .into_owned();
        if path.components().all(|c| c == Component::CurDir) {
            continue;
        }
        if !stays_inside(&path) {
            return Err(format!(
                "the entry {} would land outside the package",
                path.display()
            ));
        }
        if let Some(link) = entry
            .link_name()
            .map_err(|error| format!("an entry has an unreadable link: {error}"))?
        {
            // A hard link names a path from the archive's root; a symlink, one
            // from its own directory.
            let resolved = if entry.header().entry_type().is_hard_link() {
                link.to_path_buf()
            } else {
                path.parent().unwrap_or(Path::new("")).join(&link)
            };
            if !stays_inside(&resolved) {
                return Err(format!(
                    "the link {} -> {} would point outside the package",
                    path.display(),
                    link.display()
                ));
            }
        }
        let placed = entry
            .unpack_in(dest)
            .map_err(|error| format!("cannot unpack {}: {error}", path.display()))?;
        if !placed {
            return Err(format!(
                "the entry {} would land outside the package",
                path.display()
            ));
        }
    }
    Ok(())
}

fn sha256_file(path: &Path) -> Result<String, String> {
    let mut file = std::fs::File::open(path)
        .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 1 << 16];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

/// Write `value` as `<dir>/<name>` by renaming a temp file over it, which
/// replaces it atomically on every platform without symlink privileges.
fn write_json_atomically(dir: &Path, name: &str, value: &impl Serialize) -> Result<(), String> {
    std::fs::create_dir_all(dir)
        .map_err(|error| format!("cannot create {}: {error}", dir.display()))?;
    let text = serde_json::to_vec_pretty(value).map_err(|error| error.to_string())?;
    let temp = dir.join(format!(
        ".{name}.{}-{}.tmp",
        std::process::id(),
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::write(&temp, text)
        .map_err(|error| format!("cannot write {}: {error}", temp.display()))?;
    std::fs::rename(&temp, dir.join(name)).map_err(|error| {
        let _ = std::fs::remove_file(&temp);
        format!("cannot replace {name}: {error}")
    })
}

/// The version a Codex CLI reports: the first token of `codex --version`'s
/// output that starts with a digit (`codex-cli 0.153.3` → `0.153.3`).
pub fn parse_codex_version(output: &str) -> Option<&str> {
    output
        .split_whitespace()
        .find(|token| token.starts_with(|c: char| c.is_ascii_digit()))
}

/// Run `<entrypoint> --version` and return the version it reports.
fn probe_version(entrypoint: &Path) -> Option<String> {
    let mut child = std::process::Command::new(entrypoint)
        .arg("--version")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .ok()?;
    let deadline = Instant::now() + PROBE_TIMEOUT;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => break,
            Ok(None) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(20));
            }
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
        }
    }
    let mut output = String::new();
    child.stdout.take()?.read_to_string(&mut output).ok()?;
    parse_codex_version(&output).map(str::to_string)
}

const USER_AGENT: &str = concat!("cairn-managed-codex/", env!("CARGO_PKG_VERSION"));

/// Run `work` on its own thread: a `reqwest::blocking` client must never be
/// built or dropped on a tokio worker, and callers here may be on one.
fn off_runtime<T: Send + 'static>(
    work: impl FnOnce() -> Result<T, String> + Send + 'static,
) -> Result<T, String> {
    std::thread::spawn(work)
        .join()
        .map_err(|_| "the download thread panicked".to_string())?
}

/// Downloads over HTTPS.
pub struct HttpFetch;

impl Fetch for HttpFetch {
    fn download(&self, url: &str, dest: &Path, max_bytes: u64) -> Result<(), String> {
        let url = url.to_string();
        let dest = dest.to_path_buf();
        off_runtime(move || {
            let client = reqwest::blocking::Client::builder()
                .user_agent(USER_AGENT)
                .connect_timeout(Duration::from_secs(30))
                .timeout(Duration::from_secs(60 * 60))
                .build()
                .map_err(|error| error.to_string())?;
            let response = client
                .get(&url)
                .send()
                .and_then(|response| response.error_for_status())
                .map_err(|error| error.to_string())?;
            let mut file = std::fs::File::create(&dest)
                .map_err(|error| format!("cannot create {}: {error}", dest.display()))?;
            let written = std::io::copy(&mut response.take(max_bytes + 1), &mut file)
                .map_err(|error| error.to_string())?;
            if written > max_bytes {
                return Err(format!("it is larger than the pinned {max_bytes} bytes"));
            }
            file.sync_all().map_err(|error| error.to_string())
        })
    }
}

/// The pin CI publishes for this build's integration.
pub struct PublishedPin {
    url: String,
}

impl PublishedPin {
    pub fn new(url: String) -> Self {
        Self { url }
    }
}

impl PinSource for PublishedPin {
    fn fetch(&self, etag: Option<&str>) -> Result<PinFetch, String> {
        let url = self.url.clone();
        let etag = etag.map(str::to_string);
        off_runtime(move || {
            let client = reqwest::blocking::Client::builder()
                .user_agent(USER_AGENT)
                .connect_timeout(Duration::from_secs(15))
                .timeout(Duration::from_secs(60))
                .build()
                .map_err(|error| error.to_string())?;
            let mut request = client.get(&url);
            if let Some(etag) = &etag {
                request = request.header(reqwest::header::IF_NONE_MATCH, etag);
            }
            let response = request.send().map_err(|error| error.to_string())?;
            if response.status() == reqwest::StatusCode::NOT_MODIFIED {
                return Ok(PinFetch::NotModified);
            }
            let response = response
                .error_for_status()
                .map_err(|error| error.to_string())?;
            let etag = response
                .headers()
                .get(reqwest::header::ETAG)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            let text = response.text().map_err(|error| error.to_string())?;
            Ok(PinFetch::Body { text, etag })
        })
    }
}

/// Refuses every fetch: what test builds install with.
pub struct RefusingSource;

const REFUSED: &str = "downloads are disabled in this build";

impl PinSource for RefusingSource {
    fn fetch(&self, _etag: Option<&str>) -> Result<PinFetch, String> {
        Err(REFUSED.to_string())
    }
}

impl Fetch for RefusingSource {
    fn download(&self, _url: &str, _dest: &Path, _max_bytes: u64) -> Result<(), String> {
        Err(REFUSED.to_string())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::session::codex::pin::SUPPORTED_TARGETS;
    use std::collections::{BTreeMap, HashMap};

    fn target() -> &'static str {
        pin::host_target().unwrap_or(SUPPORTED_TARGETS[3])
    }

    fn tar_gz(entries: &[(&str, &[u8], u32)]) -> Vec<u8> {
        let mut builder = tar::Builder::new(flate2::write::GzEncoder::new(
            Vec::new(),
            flate2::Compression::fast(),
        ));
        for (path, data, mode) in entries {
            let mut header = tar::Header::new_gnu();
            header.set_size(data.len() as u64);
            header.set_mode(*mode);
            builder.append_data(&mut header, path, *data).unwrap();
        }
        builder.into_inner().unwrap().finish().unwrap()
    }

    /// An OpenAI-shaped package whose `bin/codex` reports `reports`.
    fn package(version: &str, reports: &str) -> Vec<u8> {
        let manifest = serde_json::json!({
            "layoutVersion": 1,
            "version": version,
            "target": target(),
            "entrypoint": "bin/codex",
            "resourcesDir": "codex-resources",
            "pathDir": "codex-path",
        })
        .to_string();
        let script = format!("#!/bin/sh\necho 'codex-cli {reports}'\n");
        tar_gz(&[
            ("bin/codex", script.as_bytes(), 0o755),
            ("codex-package.json", manifest.as_bytes(), 0o644),
        ])
    }

    /// A `ManagedCodex` under `dir` whose active release's `bin/codex` runs
    /// `script` (a `/bin/sh` body that must answer `--version` with
    /// `version`), installed the way an operator's hand-dropped archive is.
    #[cfg(unix)]
    pub(crate) fn installed_script(dir: &Path, version: &str, script: &str) -> ManagedCodex {
        let manifest = serde_json::json!({
            "version": version,
            "target": target(),
            "entrypoint": "bin/codex",
        })
        .to_string();
        let body = format!("#!/bin/sh\n{script}\n");
        let archive = tar_gz(&[
            ("bin/codex", body.as_bytes(), 0o755),
            ("codex-package.json", manifest.as_bytes(), 0o644),
        ]);
        let pin = pin_for(version, &archive);
        let root = dir.join("tools").join("codex");
        let dropped = root
            .join("downloads")
            .join(format!("codex-package-{}-{version}.tar.gz", target()));
        std::fs::create_dir_all(dropped.parent().unwrap()).unwrap();
        std::fs::write(&dropped, archive).unwrap();
        let managed = ManagedCodex::new(
            root,
            dir.join("bin"),
            Arc::new(RefusingSource),
            Arc::new(RefusingSource),
        )
        .with_target(Some(target()))
        .with_embedded_pin(pin);
        managed.ensure().expect("the scripted Codex installs");
        managed
    }

    fn sha256(bytes: &[u8]) -> String {
        hex::encode(Sha256::digest(bytes))
    }

    fn pin_for(version: &str, archive: &[u8]) -> CodexPin {
        pin_with_digest(version, &sha256(archive), archive.len() as u64)
    }

    fn pin_with_digest(version: &str, sha256: &str, size: u64) -> CodexPin {
        CodexPin {
            integration: CODEX_INTEGRATION,
            version: version.to_string(),
            tag: format!("rust-v{version}"),
            reason: "test".to_string(),
            packages: BTreeMap::from([(
                target().to_string(),
                CodexPackage {
                    sha256: sha256.to_string(),
                    size,
                },
            )]),
        }
    }

    #[derive(Default)]
    struct Served {
        archives: Mutex<HashMap<String, Vec<u8>>>,
        downloads: Mutex<Vec<String>>,
    }

    impl Served {
        fn serve(&self, pin: &CodexPin, bytes: Vec<u8>) {
            self.archives
                .lock()
                .unwrap()
                .insert(pin::package_url(pin, target()), bytes);
        }
        fn count(&self) -> usize {
            self.downloads.lock().unwrap().len()
        }
    }

    impl Fetch for Served {
        fn download(&self, url: &str, dest: &Path, max_bytes: u64) -> Result<(), String> {
            self.downloads.lock().unwrap().push(url.to_string());
            let bytes = self
                .archives
                .lock()
                .unwrap()
                .get(url)
                .cloned()
                .ok_or_else(|| "HTTP status client error (404 Not Found)".to_string())?;
            if bytes.len() as u64 > max_bytes {
                return Err(format!("it is larger than the pinned {max_bytes} bytes"));
            }
            std::fs::write(dest, bytes).map_err(|error| error.to_string())
        }
    }

    enum Reply {
        Body(String),
        Unreachable,
    }

    struct Published {
        reply: Mutex<Reply>,
        etags: Mutex<Vec<Option<String>>>,
    }

    impl Published {
        fn new(reply: Reply) -> Arc<Self> {
            Arc::new(Self {
                reply: Mutex::new(reply),
                etags: Mutex::new(Vec::new()),
            })
        }
        fn publish(&self, pin: &CodexPin) {
            *self.reply.lock().unwrap() = Reply::Body(serde_json::to_string(pin).unwrap());
        }
        fn go_offline(&self) {
            *self.reply.lock().unwrap() = Reply::Unreachable;
        }
    }

    impl PinSource for Published {
        fn fetch(&self, etag: Option<&str>) -> Result<PinFetch, String> {
            self.etags.lock().unwrap().push(etag.map(str::to_string));
            match &*self.reply.lock().unwrap() {
                Reply::Unreachable => Err("connection refused".to_string()),
                Reply::Body(text) => {
                    // Always a fresh body; `a_cached_pin_is_revalidated_with_its_etag`
                    // covers the not-modified answer.
                    Ok(PinFetch::Body {
                        text: text.clone(),
                        etag: Some(format!("\"{}\"", sha256(text.as_bytes()))),
                    })
                }
            }
        }
    }

    struct Fixture {
        _dir: tempfile::TempDir,
        root: PathBuf,
        bin: PathBuf,
        served: Arc<Served>,
        published: Arc<Published>,
    }

    impl Fixture {
        fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            Self {
                root: dir.path().join("tools").join("codex"),
                bin: dir.path().join("bin"),
                _dir: dir,
                served: Arc::new(Served::default()),
                published: Published::new(Reply::Unreachable),
            }
        }

        fn managed(&self, embedded: &CodexPin) -> ManagedCodex {
            ManagedCodex::new(
                &self.root,
                &self.bin,
                self.published.clone(),
                self.served.clone(),
            )
            .with_target(Some(target()))
            .with_embedded_pin(embedded.clone())
        }

        fn release(&self, version: &str) -> PathBuf {
            self.root.join(format!("{version}-{}", target()))
        }

        fn archive_path(&self, version: &str) -> PathBuf {
            self.root
                .join("downloads")
                .join(format!("codex-package-{}-{version}.tar.gz", target()))
        }

        /// Everything under the root other than releases, `current.json`,
        /// `pin.json`, and an empty `downloads`.
        fn leftovers(&self) -> Vec<String> {
            let mut found = Vec::new();
            for entry in std::fs::read_dir(&self.root)
                .into_iter()
                .flatten()
                .flatten()
            {
                let name = entry.file_name().to_string_lossy().into_owned();
                if name.starts_with('.') {
                    found.push(name);
                }
            }
            for entry in std::fs::read_dir(self.root.join("downloads"))
                .into_iter()
                .flatten()
                .flatten()
            {
                found.push(format!("downloads/{}", entry.file_name().to_string_lossy()));
            }
            found
        }
    }

    fn manual_route_text(fixture: &Fixture, pin: &CodexPin) -> String {
        let target = target();
        format!(
            "To install by hand, download https://github.com/openai/codex/releases/download/rust-v{v}/codex-package-{target}.tar.gz, confirm its SHA-256 is {sha}, and save it as {archive}.",
            v = pin.version,
            sha = pin.package(target).unwrap().sha256,
            archive = fixture.archive_path(&pin.version).display(),
        )
    }

    #[cfg(unix)]
    #[test]
    fn a_digest_mismatch_is_refused_and_installs_nothing() {
        let fixture = Fixture::new();
        let good = package("9.1.0", "9.1.0");
        let pin = pin_for("9.1.0", &good);
        let tampered = package("9.1.0", "6.6.6");
        fixture.served.serve(&pin, tampered.clone());
        let managed = fixture.managed(&pin);

        let error = managed.ensure().unwrap_err();
        let expected = sha256(&good);
        let actual = sha256(&tampered);
        assert_eq!(
            error.to_string(),
            format!(
                "Cairn could not install its pinned Codex CLI 9.1.0: the download of codex-package-{target}.tar.gz did not match its pinned checksum (expected sha256 {e}…, got {a}…). Cairn does not fall back to a `codex` found on your PATH. It retries the next time a Codex session starts, at its next six-hourly update check, and at every restart. {route}",
                target = target(),
                e = &expected[..12],
                a = &actual[..12],
                route = manual_route_text(&fixture, &pin),
            )
        );
        assert!(matches!(error, CodexInstallError::Integrity { .. }));
        assert!(!fixture.release("9.1.0").exists());
        assert!(fixture.leftovers().is_empty(), "{:?}", fixture.leftovers());
        assert!(std::fs::symlink_metadata(fixture.bin.join("codex")).is_err());
        assert!(managed.installed().is_none());
        assert_eq!(
            managed.status(),
            ManagedCodexStatus::Failed(error.to_string())
        );
    }

    #[cfg(unix)]
    #[test]
    fn a_failed_update_keeps_the_working_release() {
        let fixture = Fixture::new();
        let first = package("9.1.0", "9.1.0");
        let first_pin = pin_for("9.1.0", &first);
        fixture.served.serve(&first_pin, first);
        let managed = fixture.managed(&first_pin);
        let installed = managed.ensure().unwrap();

        let next = pin_with_digest("9.2.0", &"0".repeat(64), 1 << 20);
        fixture.served.serve(&next, package("9.2.0", "9.2.0"));
        fixture.published.publish(&next);
        let error = managed.update().unwrap_err();
        assert!(matches!(error, CodexInstallError::Integrity { .. }));
        assert_eq!(managed.installed(), Some(installed.clone()));
        assert_eq!(
            std::fs::read_link(fixture.bin.join("codex")).unwrap(),
            installed.path
        );
        assert!(!fixture.release("9.2.0").exists());
        assert!(fixture.leftovers().is_empty(), "{:?}", fixture.leftovers());
        assert!(matches!(
            managed.status(),
            ManagedCodexStatus::Installed { .. }
        ));
    }

    /// After an upgrade (or a rollback) that changes the integration, the
    /// release the other build installed is never launched, even when the
    /// install for this integration fails.
    #[cfg(unix)]
    #[test]
    fn a_release_installed_for_another_integration_is_refused_not_launched() {
        for other in [CODEX_INTEGRATION + 1, CODEX_INTEGRATION - 1] {
            let fixture = Fixture::new();
            let archive = package("9.1.0", "9.1.0");
            let old_pin = pin_for("9.1.0", &archive);
            fixture.served.serve(&old_pin, archive);
            let installed = fixture.managed(&old_pin).ensure().unwrap();
            assert!(installed.path.is_file());
            // Rewrite the record as the other build left it.
            let mut current = fixture.managed(&old_pin).read_current().unwrap();
            current.integration = other;
            write_json_atomically(&fixture.root, "current.json", &current).unwrap();

            // This build's pin cannot be fetched.
            let new_pin = pin_with_digest("9.2.0", &"c".repeat(64), 100);
            let managed = fixture.managed(&new_pin);
            assert_eq!(managed.installed(), None, "integration {other}");
            let error = managed.ensure().unwrap_err();
            assert!(
                matches!(error, CodexInstallError::Download { .. }),
                "integration {other}: {error}"
            );
            assert!(matches!(
                managed.update(),
                Err(CodexInstallError::Download { .. })
            ));
            assert_eq!(
                managed.status(),
                ManagedCodexStatus::Failed(error.to_string()),
                "onboarding must not report the other integration's release as installed"
            );
        }
    }

    #[test]
    fn a_failed_download_is_refused_with_the_manual_route() {
        let fixture = Fixture::new();
        let pin = pin_with_digest("9.1.0", &"a".repeat(64), 100);
        let managed = fixture.managed(&pin);
        let error = managed.ensure().unwrap_err();
        assert_eq!(
            error.to_string(),
            format!(
                "Cairn could not install its pinned Codex CLI 9.1.0: the download of codex-package-{target}.tar.gz failed (HTTP status client error (404 Not Found)). Cairn does not fall back to a `codex` found on your PATH. It retries the next time a Codex session starts, at its next six-hourly update check, and at every restart. {route}",
                target = target(),
                route = manual_route_text(&fixture, &pin),
            )
        );
        assert!(fixture.leftovers().is_empty(), "{:?}", fixture.leftovers());
    }

    #[test]
    fn an_unsupported_platform_is_refused() {
        let fixture = Fixture::new();
        let managed = fixture.managed(&pin::embedded_pin()).with_target(None);
        assert!(!managed.supported());
        let error = managed.ensure().unwrap_err();
        assert_eq!(
            error.to_string(),
            format!(
                "Cairn runs its own pinned Codex CLI, and OpenAI publishes no Codex package for this platform ({}/{}), so Codex sessions cannot run here. Cairn does not fall back to a `codex` found on your PATH.",
                std::env::consts::OS,
                std::env::consts::ARCH
            )
        );
        assert_eq!(fixture.served.count(), 0);
    }

    #[cfg(unix)]
    #[test]
    fn a_cli_reporting_another_version_is_refused_and_removed() {
        let fixture = Fixture::new();
        let archive = package("9.1.0", "0.0.1");
        let pin = pin_for("9.1.0", &archive);
        fixture.served.serve(&pin, archive);
        let managed = fixture.managed(&pin);
        let error = managed.ensure().unwrap_err();
        assert_eq!(
            error.to_string(),
            "Cairn could not install its pinned Codex CLI 9.1.0: the unpacked `codex --version` reported 0.0.1, not 9.1.0. Cairn does not fall back to a `codex` found on your PATH. It retries the next time a Codex session starts, at its next six-hourly update check, and at every restart."
        );
        assert!(!fixture.release("9.1.0").exists());
        assert!(managed.installed().is_none());
    }

    #[cfg(unix)]
    #[test]
    fn a_published_pin_beats_the_embedded_one_in_production() {
        let fixture = Fixture::new();
        let embedded = pin_for("9.1.0", &package("9.1.0", "9.1.0"));
        let archive = package("9.2.0", "9.2.0");
        let published = pin_for("9.2.0", &archive);
        fixture.served.serve(&published, archive);
        fixture.published.publish(&published);
        let managed = fixture.managed(&embedded);
        assert_eq!(managed.ensure().unwrap().version, "9.2.0");
        assert_eq!(
            managed.status(),
            ManagedCodexStatus::Installed {
                version: "9.2.0".to_string(),
                pin_source: PinOrigin::Published
            }
        );
    }

    #[test]
    fn an_unreachable_publication_falls_back_to_the_cached_then_the_embedded_pin() {
        let fixture = Fixture::new();
        let embedded = pin_with_digest("9.1.0", &"a".repeat(64), 1);
        let published = pin_with_digest("9.2.0", &"b".repeat(64), 1);
        let managed = fixture.managed(&embedded);

        assert_eq!(
            managed.effective_pin(),
            (embedded.clone(), PinOrigin::Embedded)
        );
        fixture.published.publish(&published);
        assert_eq!(
            managed.effective_pin(),
            (published.clone(), PinOrigin::Published)
        );
        fixture.published.go_offline();
        assert_eq!(managed.effective_pin(), (published, PinOrigin::Cached));
    }

    #[test]
    fn a_cached_pin_is_revalidated_with_its_etag() {
        struct NotModified;
        impl PinSource for NotModified {
            fn fetch(&self, etag: Option<&str>) -> Result<PinFetch, String> {
                assert_eq!(etag, Some("\"v2\""));
                Ok(PinFetch::NotModified)
            }
        }
        let fixture = Fixture::new();
        let published = pin_with_digest("9.2.0", &"b".repeat(64), 1);
        write_json_atomically(
            &fixture.root,
            "pin.json",
            &CachedPin {
                etag: Some("\"v2\"".to_string()),
                pin: published.clone(),
            },
        )
        .unwrap();
        let managed = ManagedCodex::new(
            &fixture.root,
            &fixture.bin,
            Arc::new(NotModified),
            fixture.served.clone(),
        )
        .with_target(Some(target()));
        assert_eq!(managed.effective_pin(), (published, PinOrigin::Published));
    }

    #[test]
    fn a_published_pin_for_another_integration_is_ignored() {
        let fixture = Fixture::new();
        let embedded = pin_with_digest("9.1.0", &"a".repeat(64), 1);
        let mut other = pin_with_digest("9.9.0", &"b".repeat(64), 1);
        other.integration = CODEX_INTEGRATION + 1;
        fixture.published.publish(&other);
        let managed = fixture.managed(&embedded);
        assert_eq!(managed.effective_pin(), (embedded, PinOrigin::Embedded));
        assert!(!fixture.root.join("pin.json").exists());
    }

    #[test]
    fn outside_production_the_embedded_pin_wins() {
        let fixture = Fixture::new();
        let embedded = pin_with_digest("9.1.0", &"a".repeat(64), 1);
        fixture
            .published
            .publish(&pin_with_digest("9.2.0", &"b".repeat(64), 1));
        let managed = fixture.managed(&embedded).prefer_embedded(true);
        assert_eq!(managed.effective_pin(), (embedded, PinOrigin::Embedded));
        assert!(fixture.published.etags.lock().unwrap().is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn update_swaps_to_a_new_pin_beside_the_old_and_prunes_the_one_before() {
        let fixture = Fixture::new();
        let pins: Vec<CodexPin> = ["9.1.0", "9.2.0", "9.3.0"]
            .iter()
            .map(|version| {
                let archive = package(version, version);
                let pin = pin_for(version, &archive);
                fixture.served.serve(&pin, archive);
                pin
            })
            .collect();
        let managed = fixture.managed(&pins[0]);
        let first = managed.ensure().unwrap();
        assert_eq!(first.version, "9.1.0");

        fixture.published.publish(&pins[1]);
        let UpdateOutcome::Installed { resolved, previous } = managed.update().unwrap() else {
            panic!("9.2.0 is installed");
        };
        assert_eq!(resolved.version, "9.2.0");
        assert_eq!(previous.as_deref(), Some("9.1.0"));
        assert_eq!(managed.installed(), Some(resolved.clone()));
        assert_eq!(
            std::fs::read_link(fixture.bin.join("codex")).unwrap(),
            resolved.path
        );
        assert!(first.path.is_file(), "the previous release is kept");

        let downloads = fixture.served.count();
        assert_eq!(
            managed.update().unwrap(),
            UpdateOutcome::Current(resolved.clone())
        );
        assert_eq!(
            fixture.served.count(),
            downloads,
            "no download for the same pin"
        );

        fixture.published.publish(&pins[2]);
        let UpdateOutcome::Installed { resolved, .. } = managed.update().unwrap() else {
            panic!("9.3.0 is installed");
        };
        assert_eq!(resolved.version, "9.3.0");
        assert!(
            fixture.release("9.2.0").is_dir(),
            "the previous release is kept"
        );
        assert!(
            !fixture.release("9.1.0").exists(),
            "older releases are pruned"
        );
        assert!(fixture.leftovers().is_empty(), "{:?}", fixture.leftovers());
    }

    #[cfg(unix)]
    #[test]
    fn a_matching_archive_dropped_by_hand_installs_without_a_download() {
        let fixture = Fixture::new();
        let archive = package("9.1.0", "9.1.0");
        let pin = pin_for("9.1.0", &archive);
        let dropped = fixture.archive_path("9.1.0");
        std::fs::create_dir_all(dropped.parent().unwrap()).unwrap();
        std::fs::write(&dropped, archive).unwrap();
        let managed = fixture.managed(&pin);
        assert_eq!(managed.ensure().unwrap().version, "9.1.0");
        assert_eq!(fixture.served.count(), 0);
    }

    #[test]
    fn entries_that_escape_the_package_are_refused() {
        fn raw_entry(name: &[u8], kind: tar::EntryType, link: Option<&[u8]>) -> Vec<u8> {
            let mut builder = tar::Builder::new(flate2::write::GzEncoder::new(
                Vec::new(),
                flate2::Compression::fast(),
            ));
            let mut header = tar::Header::new_gnu();
            let gnu = header.as_gnu_mut().unwrap();
            gnu.name[..name.len()].copy_from_slice(name);
            if let Some(link) = link {
                gnu.linkname[..link.len()].copy_from_slice(link);
            }
            header.set_entry_type(kind);
            header.set_size(if link.is_some() { 0 } else { 4 });
            header.set_mode(0o644);
            header.set_cksum();
            let data: &[u8] = if link.is_some() { b"" } else { b"evil" };
            builder.append(&header, data).unwrap();
            builder.into_inner().unwrap().finish().unwrap()
        }
        let cases = [
            raw_entry(b"../evil", tar::EntryType::Regular, None),
            raw_entry(b"/tmp/evil", tar::EntryType::Regular, None),
            raw_entry(b"bin/../../evil", tar::EntryType::Regular, None),
            raw_entry(b"bin/link", tar::EntryType::Symlink, Some(b"/etc/passwd")),
            raw_entry(b"bin/link", tar::EntryType::Symlink, Some(b"../../escape")),
        ];
        for (index, bytes) in cases.iter().enumerate() {
            let dir = tempfile::tempdir().unwrap();
            let archive = dir.path().join("a.tar.gz");
            std::fs::write(&archive, bytes).unwrap();
            let dest = dir.path().join("dest");
            std::fs::create_dir_all(&dest).unwrap();
            let error = unpack_archive(&archive, &dest).unwrap_err();
            assert!(
                error.contains("outside the package"),
                "case {index}: {error}"
            );
            assert!(!dir.path().join("evil").exists());
        }
    }

    #[test]
    fn the_version_is_read_from_codex_version_output() {
        assert_eq!(parse_codex_version("codex-cli 0.153.3\n"), Some("0.153.3"));
        assert_eq!(parse_codex_version("0.37.1"), Some("0.37.1"));
        assert_eq!(
            parse_codex_version("codex-cli 0.154.0-alpha.2"),
            Some("0.154.0-alpha.2")
        );
        assert_eq!(parse_codex_version("codex-cli"), None);
        assert_eq!(parse_codex_version(""), None);
    }
}
