//! Managed CLIs: the command-line tools Cairn installs from a pinned release
//! into `<cairn_home>/tools/<name>`, verifies, and resolves by absolute path.
//! A CLI on the user's `PATH` is never consulted. One engine,
//! [`ManagedTool`], serves every tool; a [`ToolSpec`] names only what
//! differs ([`codex::CODEX`], [`claude::CLAUDE`]).
//!
//! Layout under a tool's root:
//!
//! - `<version>-<target>/`: one release. A [`spec::Artifact::TarGzTree`]
//!   package is unpacked as its publisher lays it out; an
//!   [`spec::Artifact::Executable`] is the binary plus [`RELEASE_MARKER`].
//! - `current.json`: which release is active. It is replaced by a rename, so a
//!   reader sees the old release or the new one, never neither.
//! - `pin.json`: the last published pin that validated, and its ETag.
//! - `downloads/<file>`: where a package is downloaded to, and where an
//!   operator may drop one by hand; it is used if its digest matches the pin.
//!
//! A launch that finds nothing installed waits on the one install in
//! progress (or starts it), and is woken when it lands. A network fault
//! during the download is retried inside that install, so waiters stay
//! waiting rather than fail; a genuine failure (an unsupported platform, a
//! digest mismatch, a wrong version, a refused download) is refused once and
//! not fetched again until the pin changes, onboarding asks again, a file
//! appears at the manual-install path, or the process restarts.

pub mod claude;
pub mod codex;
pub mod pin;
pub mod spec;

use std::fmt;
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, TryLockError};
use std::time::{Duration, Instant, SystemTime};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::interface::session::Cli;
pub use pin::{ToolPackage, ToolPin};
pub use spec::{Artifact, ToolLabel, ToolSpec, RELEASE_MARKER};

/// How often a running host checks the published pins.
pub const UPDATE_INTERVAL: Duration = Duration::from_secs(6 * 60 * 60);

/// How long an installed CLI may take to report its version.
const PROBE_TIMEOUT: Duration = Duration::from_secs(30);

/// A staging dir this old belongs to an install that died.
const STALE_STAGING: Duration = Duration::from_secs(60 * 60);

/// The waits before each retry of a download a network fault interrupted;
/// the last repeats for as long as the fault lasts.
pub const TRANSIENT_BACKOFF: [Duration; 5] = [
    Duration::from_secs(10),
    Duration::from_secs(30),
    Duration::from_secs(60),
    Duration::from_secs(120),
    Duration::from_secs(300),
];

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

/// Why a download did not complete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchError {
    pub reason: String,
    /// A network fault worth retrying: connect, DNS, a timeout, a stall or a
    /// reset mid-body, HTTP 5xx or 429. Anything else (403, 404, 410, a body
    /// past its pinned size, a local file error) is genuine.
    pub transient: bool,
}

impl FetchError {
    pub fn transient(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
            transient: true,
        }
    }

    pub fn refused(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
            transient: false,
        }
    }
}

impl From<String> for FetchError {
    fn from(reason: String) -> Self {
        Self::refused(reason)
    }
}

impl fmt::Display for FetchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.reason)
    }
}

/// Downloads a package.
pub trait Fetch: Send + Sync {
    /// Write `url`'s body to `dest`, failing once it exceeds `max_bytes`, and
    /// giving up between chunks once `stop` is set (the host is shutting
    /// down).
    fn download(
        &self,
        url: &str,
        dest: &Path,
        max_bytes: u64,
        stop: &Arc<AtomicBool>,
    ) -> Result<(), FetchError>;
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

/// The active release of a managed CLI.
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
pub enum ManagedToolStatus {
    Installed {
        version: String,
        pin_source: PinOrigin,
    },
    Installing {
        version: String,
        /// Bytes of the package downloaded so far.
        downloaded: u64,
        /// The package's pinned size.
        total: u64,
        /// The network fault the install is retrying past, if any.
        last_error: Option<String>,
    },
    /// Nothing is installed, and the last attempt was refused with this
    /// text.
    Failed(String),
    NotInstalled,
}

/// Where an install stands, as a launch waiting on it is told.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstallProgress {
    pub tool: ToolLabel,
    pub version: String,
    pub downloaded: u64,
    pub total: u64,
    pub last_error: Option<String>,
}

impl InstallProgress {
    /// The one line a waiting launch writes to its transcript.
    pub fn waiting_message(&self) -> String {
        let megabytes = |bytes: u64| (bytes + 500_000) / 1_000_000;
        let phase = if self.total > 0 && self.downloaded >= self.total {
            "verifying the download".to_string()
        } else if self.downloaded == 0 {
            format!("starting a {} MB download", megabytes(self.total))
        } else {
            format!(
                "downloading, {} of {} MB",
                megabytes(self.downloaded),
                megabytes(self.total)
            )
        };
        let retrying = self
            .last_error
            .as_deref()
            .map(|error| format!(" The download was interrupted ({error}) and retries by itself."))
            .unwrap_or_default();
        format!(
            "Waiting for Cairn's pinned {} {} to finish installing ({phase}).{retrying} The session starts when it lands.",
            self.tool.display_name, self.version
        )
    }
}

/// What a refusal tells the operator to do by hand.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManualRoute {
    pub version: String,
    pub asset: String,
    pub url: String,
    pub sha256: String,
    pub size: u64,
    /// Where a hand-downloaded package is picked up.
    pub archive: PathBuf,
}

/// Why Cairn has no managed CLI to run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolInstallError {
    pub tool: ToolLabel,
    pub failure: InstallFailure,
}

/// The step that failed. Routes are boxed to keep the error small enough to
/// return by value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InstallFailure {
    UnsupportedPlatform {
        os: &'static str,
        arch: &'static str,
    },
    Download {
        route: Box<ManualRoute>,
        reason: String,
        /// See [`FetchError::transient`].
        transient: bool,
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
    /// The wait or the install was abandoned, not refused: the host is
    /// shutting down, or the launch waiting on it no longer wants it. Never
    /// stands as a refusal.
    Cancelled { reason: String },
}

/// Why an install or a wait on one was given up.
const SHUTTING_DOWN: &str = "Cairn is shutting down";

impl ToolInstallError {
    /// The install or the wait was abandoned rather than refused.
    pub fn is_cancelled(&self) -> bool {
        matches!(self.failure, InstallFailure::Cancelled { .. })
    }

    /// A network fault the install retries by itself. Every other failure is
    /// genuine: refused once, and not attempted again for the same pin
    /// until something changes.
    pub fn is_transient(&self) -> bool {
        matches!(
            self.failure,
            InstallFailure::Download {
                transient: true,
                ..
            }
        )
    }

    pub fn route(&self) -> Option<&ManualRoute> {
        match &self.failure {
            InstallFailure::UnsupportedPlatform { .. } | InstallFailure::Cancelled { .. } => None,
            InstallFailure::Download { route, .. }
            | InstallFailure::Integrity { route, .. }
            | InstallFailure::Extract { route, .. }
            | InstallFailure::Probe { route, .. } => Some(route),
        }
    }
}

impl ToolSpec {
    /// This host's package target, or the refusal for a host the publisher
    /// ships nothing for. Static: it needs no [`ManagedTool`].
    pub fn host_supported(&self) -> Result<&'static str, ToolInstallError> {
        (self.host_target)().ok_or_else(|| ToolInstallError {
            tool: self.label(),
            failure: InstallFailure::UnsupportedPlatform {
                os: std::env::consts::OS,
                arch: std::env::consts::ARCH,
            },
        })
    }
}

/// When a refused install is attempted again.
const RETRIES: &str = "It tries again when its pin changes, when you choose Check again in onboarding, and at every restart.";

fn abbreviate(digest: &str) -> String {
    match digest.get(..12) {
        Some(head) if digest.len() > 12 => format!("{head}…"),
        _ => digest.to_string(),
    }
}

impl fmt::Display for ToolInstallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ToolLabel {
            name,
            display_name,
            publisher,
        } = self.tool;
        let no_path = format!("Cairn does not fall back to a `{name}` found on your PATH.");
        let (route, failure) = match &self.failure {
            InstallFailure::UnsupportedPlatform { os, arch } => {
                return write!(
                    f,
                    "Cairn runs its own pinned {display_name} CLI, and {publisher} publishes no {display_name} package for this platform ({os}/{arch}), so {display_name} sessions cannot run here. {no_path}"
                );
            }
            InstallFailure::Cancelled { reason } => {
                return write!(
                    f,
                    "Stopped waiting for Cairn's pinned {display_name} CLI to install: {reason}."
                );
            }
            InstallFailure::Download { route, reason, .. } => (
                route,
                format!("the download of {} failed ({reason})", route.asset),
            ),
            InstallFailure::Integrity {
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
            InstallFailure::Extract { route, reason } => (
                route,
                format!("installing {} failed ({reason})", route.asset),
            ),
            InstallFailure::Probe { route, found } => {
                let found = found.as_deref().unwrap_or("no version it could read");
                return write!(
                    f,
                    "Cairn could not install its pinned {display_name} CLI {}: the installed `{name} --version` reported {found}, not {}. {no_path} {RETRIES}",
                    route.version, route.version
                );
            }
        };
        write!(
            f,
            "Cairn could not install its pinned {display_name} CLI {}: {failure}. {no_path} {RETRIES} To install by hand, download {}, confirm its SHA-256 is {}, and save it as {}.",
            route.version,
            route.url,
            route.sha256,
            route.archive.display()
        )
    }
}

impl std::error::Error for ToolInstallError {}

/// `current.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Current {
    version: String,
    target: String,
    /// The tool's integration in the build that installed it. A release from
    /// another integration is never launched: after an upgrade or a
    /// rollback it is as good as not installed.
    integration: u32,
    pin_source: PinOrigin,
}

/// `pin.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CachedPin {
    etag: Option<String>,
    pin: ToolPin,
}

/// The fields of a release's manifest (the publisher's, or
/// [`RELEASE_MARKER`]) a completeness check reads.
#[derive(Debug, Deserialize)]
struct PackageManifest {
    version: String,
    target: String,
    entrypoint: String,
}

/// [`RELEASE_MARKER`], as the engine writes it.
#[derive(Debug, Serialize)]
struct ReleaseMarker<'a> {
    version: &'a str,
    target: &'a str,
    entrypoint: &'a str,
    sha256: &'a str,
}

/// An install in progress.
struct Installing {
    version: String,
    total: u64,
    /// The download being written, while one is.
    partial: Option<PathBuf>,
    /// Bytes known downloaded once the download is done.
    downloaded: u64,
    last_error: Option<String>,
}

/// A genuine failure standing for the pin it was for.
struct Refusal {
    version: String,
    error: ToolInstallError,
    /// The modification time of the file at the manual-install path when it
    /// was last considered, so a dropped file is hashed once.
    drop_seen: Option<SystemTime>,
}

#[derive(Default)]
struct State {
    installing: Option<Installing>,
    refused: Option<Refusal>,
}

/// Installs, follows, and resolves one managed CLI. One per tool per host;
/// share it.
pub struct ManagedTool {
    spec: &'static ToolSpec,
    root: PathBuf,
    bin_dir: PathBuf,
    target: Option<&'static str>,
    embedded: ToolPin,
    pins: Arc<dyn PinSource>,
    fetch: Arc<dyn Fetch>,
    prefer_embedded: bool,
    backoff: Vec<Duration>,
    /// Held for the length of an install or update. Its holder publishes
    /// `State::installing` the moment it takes it ([`InstallGuard`]), so a
    /// launch that finds it held is always told where the install stands.
    lock: Mutex<()>,
    state: Mutex<State>,
    /// Signalled whenever `state` changes in a way a waiter cares about: an
    /// install began or ended, or the host began shutting down.
    changed: Condvar,
    /// Set once by [`Self::shutdown`]: no new attempt starts, a backoff wait
    /// ends at once, every waiter returns, and a transfer stops at its next
    /// chunk.
    shutting_down: Arc<AtomicBool>,
    /// A background install thread ([`Self::start_background_install`]) is
    /// running or about to take the install lock.
    background: AtomicBool,
}

/// The install lock, held, with the install it covers published in the
/// tool's state for as long as it is held. Dropping it clears the record and
/// wakes every waiter.
struct InstallGuard<'a> {
    tool: &'a ManagedTool,
    _lock: MutexGuard<'a, ()>,
}

impl<'a> InstallGuard<'a> {
    fn new(tool: &'a ManagedTool, lock: MutexGuard<'a, ()>) -> Self {
        // Until the effective pin is known (a published-pin lookup can take a
        // while), the install is reported as the embedded pin's.
        let total = tool
            .target
            .and_then(|target| tool.embedded.package(target))
            .map_or(0, |package| package.size);
        tool.state_guard().installing = Some(Installing {
            version: tool.embedded.version.clone(),
            total,
            partial: None,
            downloaded: 0,
            last_error: None,
        });
        tool.changed.notify_all();
        Self { tool, _lock: lock }
    }
}

impl Drop for InstallGuard<'_> {
    fn drop(&mut self) {
        self.tool.state_guard().installing = None;
        self.tool.changed.notify_all();
    }
}

impl ManagedTool {
    /// `spec`'s CLI installed under `root`, with its forwarder (if the tool
    /// has one) in `bin_dir`, following `pins` and downloading through
    /// `fetch`.
    pub fn new(
        spec: &'static ToolSpec,
        root: impl Into<PathBuf>,
        bin_dir: impl Into<PathBuf>,
        pins: Arc<dyn PinSource>,
        fetch: Arc<dyn Fetch>,
    ) -> Self {
        Self {
            spec,
            root: root.into(),
            bin_dir: bin_dir.into(),
            target: (spec.host_target)(),
            embedded: spec.embedded(),
            pins,
            fetch,
            prefer_embedded: false,
            backoff: TRANSIENT_BACKOFF.to_vec(),
            lock: Mutex::new(()),
            state: Mutex::new(State::default()),
            changed: Condvar::new(),
            shutting_down: Arc::new(AtomicBool::new(false)),
            background: AtomicBool::new(false),
        }
    }

    /// The host is shutting down: an install retrying past a network fault
    /// stops at its next wait, a transfer stops at its next chunk (a stalled
    /// one within [`HttpFetch`]'s one-minute read bound), no new attempt
    /// starts, and every launch waiting on an install returns.
    pub fn shutdown(&self) {
        self.shutting_down.store(true, Ordering::SeqCst);
        let _state = self.state_guard();
        self.changed.notify_all();
    }

    fn cancelled(&self, reason: &str) -> ToolInstallError {
        self.error(InstallFailure::Cancelled {
            reason: reason.to_string(),
        })
    }

    fn check_not_shutting_down(&self) -> Result<(), ToolInstallError> {
        if self.shutting_down.load(Ordering::SeqCst) {
            Err(self.cancelled(SHUTTING_DOWN))
        } else {
            Ok(())
        }
    }

    /// Take the install lock, blocking, and publish the install it covers.
    fn begin_install(&self) -> InstallGuard<'_> {
        InstallGuard::new(self, self.install_lock())
    }

    /// [`Self::begin_install`], unless an install already holds the lock.
    fn try_begin_install(&self) -> Option<InstallGuard<'_>> {
        let lock = match self.lock.try_lock() {
            Ok(guard) => guard,
            Err(TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
            Err(TryLockError::WouldBlock) => return None,
        };
        Some(InstallGuard::new(self, lock))
    }

    /// This host's managed `spec`: `<cairn_home>/tools/<name>`, following
    /// the published pin, except outside production, where the embedded pin
    /// wins so a branch's bump is what its dev instance runs.
    ///
    /// Test builds get a per-process scratch root and sources that refuse, so
    /// no test can download a CLI or touch the real Cairn home.
    pub fn for_host(spec: &'static ToolSpec) -> Self {
        #[cfg(any(test, feature = "test-utils"))]
        {
            let scratch = std::env::temp_dir().join(format!(
                "cairn-test-{}-{}",
                spec.name,
                std::process::id()
            ));
            Self::new(
                spec,
                scratch.join("tools").join(spec.name),
                scratch.join("bin"),
                Arc::new(RefusingSource),
                Arc::new(RefusingSource),
            )
        }
        #[cfg(not(any(test, feature = "test-utils")))]
        {
            Self::new(
                spec,
                cairn_common::paths::cairn_home()
                    .join("tools")
                    .join(spec.name),
                cairn_common::toolchain_path::cairn_bin_dir(),
                Arc::new(PublishedPin::new(spec.published_pin_url(), spec.name)),
                Arc::new(HttpFetch::new(spec.name)),
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
    pub fn with_embedded_pin(mut self, pin: ToolPin) -> Self {
        self.embedded = pin;
        self
    }

    /// Wait `backoff` between retries of an interrupted download instead of
    /// [`TRANSIENT_BACKOFF`].
    pub fn with_backoff(mut self, backoff: Vec<Duration>) -> Self {
        self.backoff = backoff;
        self
    }

    pub fn spec(&self) -> &'static ToolSpec {
        self.spec
    }

    pub fn label(&self) -> ToolLabel {
        self.spec.label()
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// This host's package target, if the publisher ships one.
    pub fn target(&self) -> Option<&'static str> {
        self.target
    }

    /// Whether the publisher ships a package for this host.
    pub fn supported(&self) -> bool {
        self.target.is_some()
    }

    /// The refusal for a host the publisher ships nothing for, or `Ok`.
    pub fn check_supported(&self) -> Result<(), ToolInstallError> {
        self.require_target().map(|_| ())
    }

    /// The active release, if one is installed and complete and was
    /// installed for this build's integration. Never touches the network.
    pub fn installed(&self) -> Option<Resolved> {
        let target = self.target?;
        let current = self.read_current()?;
        if current.target != target || current.integration != self.spec.integration {
            return None;
        }
        let release = self.release_dir(&current.version, target)?;
        let path = complete_release(self.spec, &release, &current.version, target)?;
        Some(Resolved {
            path,
            version: current.version,
        })
    }

    /// The CLI to launch, installing it on this thread if nothing is
    /// installed (behind any install already running). It cannot be
    /// abandoned; a launch that may be stopped uses [`Self::ensure_with`].
    pub fn ensure(&self) -> Result<Resolved, ToolInstallError> {
        let target = self.require_target()?;
        if let Some(resolved) = self.installed() {
            return Ok(resolved);
        }
        if let Some(refusal) = self.standing_refusal(None) {
            return Err(refusal);
        }
        wait_off_runtime(|| {
            self.check_not_shutting_down()?;
            let _install = self.begin_install();
            if let Some(resolved) = self.installed() {
                return Ok(resolved);
            }
            if let Some(refusal) = self.standing_refusal(None) {
                return Err(refusal);
            }
            let (pin, origin) = self.effective_pin();
            self.record_installing(&pin, target);
            self.install(&pin, origin, target)
        })
    }

    /// Start the install on a thread of its own, unless one is already
    /// running, so that no launch owns it: every launch only waits on it, and
    /// any of them can stop waiting without stopping it.
    fn start_background_install(self: &Arc<Self>) {
        if self.state_guard().installing.is_some() || self.background.swap(true, Ordering::SeqCst) {
            return;
        }
        let tool = Arc::clone(self);
        let spawned = std::thread::Builder::new()
            .name(format!("managed-{}-install", self.spec.name))
            .spawn(move || {
                let Some(install) = tool.try_begin_install() else {
                    // Another install holds the lock; its end wakes the
                    // waiters.
                    tool.background.store(false, Ordering::SeqCst);
                    let _state = tool.state_guard();
                    tool.changed.notify_all();
                    return;
                };
                if tool.installed().is_none() && tool.standing_refusal(None).is_none() {
                    if let Ok(target) = tool.require_target() {
                        let (pin, origin) = tool.effective_pin();
                        tool.record_installing(&pin, target);
                        // The result stands in the tool's state (installed,
                        // or the refusal) for every waiter to read.
                        let _ = tool.install(&pin, origin, target);
                    }
                }
                // Cleared before the guard is dropped, so a waiter woken by
                // the drop never sees an install that is neither running nor
                // about to.
                tool.background.store(false, Ordering::SeqCst);
                drop(install);
            });
        if spawned.is_err() {
            self.background.store(false, Ordering::SeqCst);
        }
    }

    /// The CLI to launch: whatever is installed, even if it is behind the pin
    /// (a launch never waits on a version check), else the effective pin,
    /// installed now.
    ///
    /// When nothing is installed the call blocks until it is. No caller owns
    /// the install: if none is running, one is started on a thread of its own
    /// ([`Self::start_background_install`]), and every launch, the one that
    /// started it included, only waits on it. `on_wait` is called once, before
    /// blocking, with where the install stands; the installer publishes that
    /// the moment it takes the install lock, so a launch arriving while it
    /// still looks up the published pin is told too. A network fault is
    /// retried inside the install, so it keeps the call waiting; a genuine
    /// failure returns, and returns again at once for the same pin without
    /// another download (see [`Self::retry_if_idle`]).
    ///
    /// A waiting call is woken when the install ends, and asks `still_wanted`
    /// at least once a second: once it answers false (the launch was
    /// stopped), the call detaches with a [`InstallFailure::Cancelled`] and
    /// the install goes on for everyone else. [`Self::shutdown`] ends every
    /// wait.
    ///
    /// A worker of a multi-thread tokio runtime hands its queue off before it
    /// waits, so a launch held by an install costs the runtime one thread,
    /// never a worker.
    pub fn ensure_with(
        self: &Arc<Self>,
        on_wait: impl FnOnce(InstallProgress),
        still_wanted: &dyn Fn() -> bool,
    ) -> Result<Resolved, ToolInstallError> {
        self.require_target()?;
        if let Some(resolved) = self.installed() {
            return Ok(resolved);
        }
        if let Some(refusal) = self.standing_refusal(None) {
            return Err(refusal);
        }
        let mut on_wait = Some(on_wait);
        wait_off_runtime(move || loop {
            self.check_not_shutting_down()?;
            if !still_wanted() {
                return Err(self.cancelled("the launch was stopped"));
            }
            self.start_background_install();
            if let Some(progress) = self.progress() {
                if let Some(tell) = on_wait.take() {
                    tell(progress);
                }
            }
            {
                let state = self.state_guard();
                let pending = state.installing.is_some() || self.background.load(Ordering::SeqCst);
                if pending && !self.shutting_down.load(Ordering::SeqCst) {
                    let _ = self
                        .changed
                        .wait_timeout(state, Duration::from_secs(1))
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                }
            }
            if let Some(resolved) = self.installed() {
                return Ok(resolved);
            }
            if let Some(refusal) = self.standing_refusal(None) {
                return Err(refusal);
            }
        })
    }

    /// Name the pin the install in progress is for, once it is known.
    fn record_installing(&self, pin: &ToolPin, target: &str) {
        if let Some(installing) = self.state_guard().installing.as_mut() {
            installing.version = pin.version.clone();
            installing.total = pin.package(target).map_or(0, |package| package.size);
        }
        self.changed.notify_all();
    }

    /// Follow the effective pin: install it beside the active release and
    /// swap to it if it differs. A failure keeps the active release; a pin
    /// already refused is not attempted again.
    pub fn update(&self) -> Result<UpdateOutcome, ToolInstallError> {
        let target = self.require_target()?;
        self.check_not_shutting_down()?;
        let _install = self.begin_install();
        self.update_locked(target)
    }

    fn update_locked(&self, target: &'static str) -> Result<UpdateOutcome, ToolInstallError> {
        let (pin, origin) = self.effective_pin();
        self.record_installing(&pin, target);
        let current = self.installed();
        if let Some(resolved) = current.as_ref().filter(|r| r.version == pin.version) {
            return Ok(UpdateOutcome::Current(resolved.clone()));
        }
        if let Some(refusal) = self.standing_refusal(Some(&pin.version)) {
            return Err(refusal);
        }
        let previous = current.map(|resolved| resolved.version);
        let resolved = self.install(&pin, origin, target)?;
        Ok(UpdateOutcome::Installed { resolved, previous })
    }

    /// [`Self::update`], unless an install or update is already running, in
    /// which case `None`.
    pub fn update_if_idle(&self) -> Option<Result<UpdateOutcome, ToolInstallError>> {
        let target = match self.require_target() {
            Ok(target) => target,
            Err(error) => return Some(Err(error)),
        };
        if let Err(error) = self.check_not_shutting_down() {
            return Some(Err(error));
        }
        let _install = self.try_begin_install()?;
        Some(self.update_locked(target))
    }

    /// Forget a standing refusal and try again, unless an install is already
    /// running. Onboarding's "Check again" lands here.
    pub fn retry_if_idle(&self) -> Option<Result<UpdateOutcome, ToolInstallError>> {
        self.state_guard().refused = None;
        self.update_if_idle()
    }

    pub fn status(&self) -> ManagedToolStatus {
        if let Some(resolved) = self.installed() {
            let pin_source = self
                .read_current()
                .map(|current| current.pin_source)
                .unwrap_or(PinOrigin::Embedded);
            return ManagedToolStatus::Installed {
                version: resolved.version,
                pin_source,
            };
        }
        if let Some(progress) = self.progress() {
            return ManagedToolStatus::Installing {
                version: progress.version,
                downloaded: progress.downloaded,
                total: progress.total,
                last_error: progress.last_error,
            };
        }
        if self.target.is_none() {
            return ManagedToolStatus::Failed(self.unsupported().to_string());
        }
        match &self.state_guard().refused {
            Some(refusal) => ManagedToolStatus::Failed(refusal.error.to_string()),
            None => ManagedToolStatus::NotInstalled,
        }
    }

    /// Where the install in progress stands, if one is.
    pub fn progress(&self) -> Option<InstallProgress> {
        let state = self.state_guard();
        let installing = state.installing.as_ref()?;
        let live = installing
            .partial
            .as_ref()
            .and_then(|partial| std::fs::metadata(partial).ok())
            .map_or(0, |meta| meta.len());
        Some(InstallProgress {
            tool: self.label(),
            version: installing.version.clone(),
            downloaded: installing.downloaded.max(live).min(installing.total),
            total: installing.total,
            last_error: installing.last_error.clone(),
        })
    }

    fn require_target(&self) -> Result<&'static str, ToolInstallError> {
        self.target.ok_or_else(|| self.unsupported())
    }

    fn unsupported(&self) -> ToolInstallError {
        ToolInstallError {
            tool: self.label(),
            failure: InstallFailure::UnsupportedPlatform {
                os: std::env::consts::OS,
                arch: std::env::consts::ARCH,
            },
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

    /// The refusal standing for `version` (any version when `None`), unless
    /// a file has appeared at its manual-install path since it was last
    /// considered and its digest matches the pin, in which case the refusal
    /// is lifted so the install picks the file up. A dropped file is hashed
    /// once.
    fn standing_refusal(&self, version: Option<&str>) -> Option<ToolInstallError> {
        let (error, route) = {
            let mut state = self.state_guard();
            let refusal = state.refused.as_mut()?;
            if version.is_some_and(|version| version != refusal.version) {
                return None;
            }
            let Some(route) = refusal.error.route().cloned() else {
                return Some(refusal.error.clone());
            };
            let stamp = drop_stamp(&route);
            if stamp.is_none() || stamp == refusal.drop_seen {
                return Some(refusal.error.clone());
            }
            refusal.drop_seen = stamp;
            (refusal.error.clone(), route)
        };
        if sha256_file(&route.archive).is_ok_and(|digest| digest == route.sha256) {
            log::info!(
                "managed {}: {} was saved by hand and matches the pin; installing it",
                self.spec.name,
                route.archive.display()
            );
            self.state_guard().refused = None;
            return None;
        }
        log::warn!(
            "managed {}: {} does not match the pinned sha256 {}",
            self.spec.name,
            route.archive.display(),
            route.sha256
        );
        Some(error)
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
        cached.pin.validate(self.spec).ok()?;
        Some(cached)
    }

    /// The pin to follow: the published one when it validates for this
    /// build, else the last published one that did, else the embedded one.
    fn effective_pin(&self) -> (ToolPin, PinOrigin) {
        if self.prefer_embedded {
            return (self.embedded.clone(), PinOrigin::Embedded);
        }
        let name = self.spec.name;
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
            Ok(PinFetch::Body { text, etag }) => match ToolPin::parse(self.spec, &text) {
                Ok(pin) if self.target.is_some_and(|t| pin.package(t).is_some()) => {
                    let record = CachedPin {
                        etag,
                        pin: pin.clone(),
                    };
                    if let Err(error) = write_json_atomically(&self.root, "pin.json", &record) {
                        log::warn!("managed {name}: could not cache the published pin: {error}");
                    }
                    (pin, PinOrigin::Published)
                }
                Ok(_) => {
                    log::warn!("managed {name}: the published pin has no package for this host");
                    fallback(cached)
                }
                Err(error) => {
                    log::warn!("managed {name}: ignoring the published pin: {error}");
                    fallback(cached)
                }
            },
            Err(error) => {
                log::info!("managed {name}: the published pin is unreachable: {error}");
                fallback(cached)
            }
        }
    }

    fn route(&self, pin: &ToolPin, target: &str, package: &ToolPackage) -> Box<ManualRoute> {
        Box::new(ManualRoute {
            version: pin.version.clone(),
            asset: (self.spec.asset_file_name)(target),
            url: (self.spec.package_url)(pin, target),
            sha256: package.sha256.clone(),
            size: package.size,
            archive: self
                .root
                .join("downloads")
                .join((self.spec.download_file_name)(target, &pin.version)),
        })
    }

    fn error(&self, failure: InstallFailure) -> ToolInstallError {
        ToolInstallError {
            tool: self.label(),
            failure,
        }
    }

    /// Install `pin` and make it active. Runs under an [`InstallGuard`],
    /// which publishes and clears the install record. A genuine failure
    /// stands as the refusal for `pin`; an abandoned install does not.
    fn install(
        &self,
        pin: &ToolPin,
        origin: PinOrigin,
        target: &'static str,
    ) -> Result<Resolved, ToolInstallError> {
        let result = self.install_inner(pin, origin, target);
        let mut state = self.state_guard();
        match &result {
            Ok(_) => state.refused = None,
            Err(error) if error.is_cancelled() => {
                log::info!("managed {}: {error}", self.spec.name);
            }
            Err(error) => {
                log::warn!("managed {}: {error}", self.spec.name);
                state.refused = Some(Refusal {
                    version: pin.version.clone(),
                    drop_seen: error.route().and_then(drop_stamp),
                    error: error.clone(),
                });
            }
        }
        result
    }

    fn install_inner(
        &self,
        pin: &ToolPin,
        origin: PinOrigin,
        target: &'static str,
    ) -> Result<Resolved, ToolInstallError> {
        let Some(package) = pin.package(target) else {
            return Err(self.unsupported());
        };
        let route = self.route(pin, target, package);
        let extract = |reason: String| {
            self.error(InstallFailure::Extract {
                route: route.clone(),
                reason,
            })
        };
        let release = self
            .release_dir(&pin.version, target)
            .ok_or_else(|| extract(format!("the version {:?} is malformed", pin.version)))?;
        let previous = self.read_current();

        if complete_release(self.spec, &release, &pin.version, target).is_none() {
            self.remove_stale_staging();
            let archive = self.obtain_archive_retrying(&route, package)?;
            self.place(&archive, &release, &pin.version, target, &package.sha256)
                .map_err(extract)?;
            // The package is not needed once its release is in place (an
            // executable was moved into it already).
            let _ = std::fs::remove_file(&archive);
        }
        let entrypoint = complete_release(self.spec, &release, &pin.version, target)
            .ok_or_else(|| extract("the installed release is incomplete".to_string()))?;

        let found = probe_version(&entrypoint, &(self.spec.probe_env)());
        if found.as_deref() != Some(pin.version.as_str()) {
            let _ = std::fs::remove_dir_all(&release);
            return Err(self.error(InstallFailure::Probe { route, found }));
        }

        let current = Current {
            version: pin.version.clone(),
            target: target.to_string(),
            integration: self.spec.integration,
            pin_source: origin,
        };
        write_json_atomically(&self.root, "current.json", &current)
            .map_err(|error| extract(format!("could not record the active release: {error}")))?;
        if self.spec.shim {
            cairn_platform::env::ensure_tool_shim(
                &self.bin_dir,
                self.spec.name,
                &entrypoint.to_string_lossy(),
            );
        }
        self.prune(
            target,
            &pin.version,
            previous.map(|current| current.version),
        );
        log::info!(
            "managed {}: {} ({origin} pin) is active at {}",
            self.spec.name,
            pin.version,
            entrypoint.display()
        );
        Ok(Resolved {
            path: entrypoint,
            version: pin.version.clone(),
        })
    }

    /// [`Self::obtain_archive`], retrying a network fault after each
    /// [`Self::with_backoff`] wait for as long as it lasts, so the launches
    /// waiting on this install wait rather than fail.
    fn obtain_archive_retrying(
        &self,
        route: &ManualRoute,
        package: &ToolPackage,
    ) -> Result<PathBuf, ToolInstallError> {
        let mut attempt = 0usize;
        loop {
            self.check_not_shutting_down()?;
            match self.obtain_archive(route, package) {
                Err(error) if error.is_transient() => {
                    let wait = self
                        .backoff
                        .get(attempt)
                        .or(self.backoff.last())
                        .copied()
                        .unwrap_or_default();
                    attempt += 1;
                    let reason = match &error.failure {
                        InstallFailure::Download { reason, .. } => reason.clone(),
                        _ => error.to_string(),
                    };
                    log::info!(
                        "managed {}: the download of {} was interrupted ({reason}); retrying in {}s",
                        self.spec.name,
                        route.asset,
                        wait.as_secs()
                    );
                    let mut state = self.state_guard();
                    if let Some(installing) = state.installing.as_mut() {
                        installing.last_error = Some(reason);
                    }
                    // Woken early only by a shutdown, which the loop's head
                    // then reports.
                    let deadline = Instant::now() + wait;
                    while !self.shutting_down.load(Ordering::SeqCst) {
                        let left = deadline.saturating_duration_since(Instant::now());
                        if left.is_zero() {
                            break;
                        }
                        state = self
                            .changed
                            .wait_timeout(state, left)
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .0;
                    }
                }
                other => return other,
            }
        }
    }

    /// The package at the route's path, if its digest matches; else a fresh
    /// download, checked before it is moved there.
    fn obtain_archive(
        &self,
        route: &ManualRoute,
        package: &ToolPackage,
    ) -> Result<PathBuf, ToolInstallError> {
        let name = self.spec.name;
        let archive = &route.archive;
        if archive.is_file() {
            match sha256_file(archive) {
                Ok(digest) if digest == package.sha256 => return Ok(archive.clone()),
                Ok(digest) => log::warn!(
                    "managed {name}: {} has sha256 {digest}, not the pinned {}; downloading",
                    archive.display(),
                    package.sha256
                ),
                Err(error) => log::warn!("managed {name}: {error}; downloading"),
            }
        }
        let download = |error: FetchError| {
            self.error(InstallFailure::Download {
                route: Box::new(route.clone()),
                reason: error.reason,
                transient: error.transient,
            })
        };
        let downloads = archive.parent().unwrap_or(&self.root);
        std::fs::create_dir_all(downloads).map_err(|error| {
            download(FetchError::refused(format!(
                "cannot create {}: {error}",
                downloads.display()
            )))
        })?;
        let partial = downloads.join(format!(
            ".{}.{}-{}.tmp",
            archive
                .file_name()
                .map(|name| name.to_string_lossy())
                .unwrap_or_default(),
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ));
        if let Some(installing) = self.state_guard().installing.as_mut() {
            installing.partial = Some(partial.clone());
        }
        let result = (|| {
            self.fetch
                .download(&route.url, &partial, package.size, &self.shutting_down)
                .map_err(download)?;
            let actual = sha256_file(&partial).map_err(|reason| download(reason.into()))?;
            if actual != package.sha256 {
                return Err(self.error(InstallFailure::Integrity {
                    route: Box::new(route.clone()),
                    expected: package.sha256.clone(),
                    actual,
                }));
            }
            std::fs::rename(&partial, archive).map_err(|error| {
                download(FetchError::refused(format!(
                    "cannot move it to {}: {error}",
                    archive.display()
                )))
            })
        })();
        if let Some(installing) = self.state_guard().installing.as_mut() {
            installing.partial = None;
            if result.is_ok() {
                installing.downloaded = installing.total;
            }
        }
        if result.is_err() {
            let _ = std::fs::remove_file(&partial);
        }
        result.map(|()| archive.clone())
    }

    /// Lay the package out in a staging dir as the tool's [`Artifact`] says,
    /// then rename it to `release`.
    fn place(
        &self,
        archive: &Path,
        release: &Path,
        version: &str,
        target: &str,
        sha256: &str,
    ) -> Result<(), String> {
        let staging = self.root.join(format!(
            ".staging-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ));
        let result = (|| {
            std::fs::create_dir_all(&staging)
                .map_err(|error| format!("cannot create {}: {error}", staging.display()))?;
            match self.spec.artifact {
                Artifact::TarGzTree { .. } => unpack_archive(archive, &staging)?,
                Artifact::Executable { file } => {
                    place_executable(archive, &staging, file(target), version, target, sha256)?
                }
            }
            if complete_release(self.spec, &staging, version, target).is_none() {
                return Err(format!(
                    "it is not a complete {} {version} package for {target}",
                    self.spec.display_name
                ));
            }
            if release.exists() {
                // Another process may have installed it meanwhile; a
                // complete one is accepted, anything else is replaced.
                if complete_release(self.spec, release, version, target).is_some() {
                    return Ok(());
                }
                std::fs::remove_dir_all(release)
                    .map_err(|error| format!("cannot replace {}: {error}", release.display()))?;
            }
            match std::fs::rename(&staging, release) {
                Ok(()) => Ok(()),
                Err(_) if complete_release(self.spec, release, version, target).is_some() => Ok(()),
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
                log::debug!(
                    "managed {}: could not prune {name}: {error}",
                    self.spec.name
                );
            }
        }
    }
}

/// Every managed CLI on one host.
#[derive(Clone)]
pub struct ManagedTools {
    pub codex: Arc<ManagedTool>,
    pub claude: Arc<ManagedTool>,
}

impl ManagedTools {
    /// This host's managed CLIs ([`ManagedTool::for_host`]).
    pub fn for_host() -> Self {
        Self {
            codex: Arc::new(ManagedTool::for_host(&codex::CODEX)),
            claude: Arc::new(ManagedTool::for_host(&claude::CLAUDE)),
        }
    }

    pub fn get(&self, cli: Cli) -> &Arc<ManagedTool> {
        match cli {
            Cli::Codex => &self.codex,
            Cli::Claude => &self.claude,
        }
    }

    pub fn all(&self) -> [(Cli, &Arc<ManagedTool>); 2] {
        [(Cli::Codex, &self.codex), (Cli::Claude, &self.claude)]
    }

    /// [`ManagedTool::shutdown`] for every tool.
    pub fn shutdown(&self) {
        for (_, tool) in self.all() {
            tool.shutdown();
        }
    }
}

/// The modification time of a file of the pinned size at the route's
/// manual-install path, if one is there.
fn drop_stamp(route: &ManualRoute) -> Option<SystemTime> {
    let meta = std::fs::metadata(&route.archive).ok()?;
    if !meta.is_file() || meta.len() != route.size {
        return None;
    }
    Some(meta.modified().unwrap_or(SystemTime::UNIX_EPOCH))
}

/// The entrypoint of the release at `dir`, if its manifest names `version`
/// for `target` and the entrypoint exists inside it.
fn complete_release(spec: &ToolSpec, dir: &Path, version: &str, target: &str) -> Option<PathBuf> {
    let manifest = match spec.artifact {
        Artifact::TarGzTree { manifest } => manifest,
        Artifact::Executable { .. } => RELEASE_MARKER,
    };
    let manifest: PackageManifest =
        serde_json::from_slice(&std::fs::read(dir.join(manifest)).ok()?).ok()?;
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

/// Move the digest-checked executable into `staging` as `file`, make it
/// executable, and write the [`RELEASE_MARKER`] naming it.
fn place_executable(
    archive: &Path,
    staging: &Path,
    file: &str,
    version: &str,
    target: &str,
    sha256: &str,
) -> Result<(), String> {
    let entrypoint = staging.join(file);
    std::fs::rename(archive, &entrypoint)
        .map_err(|error| format!("cannot move it to {}: {error}", entrypoint.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&entrypoint, std::fs::Permissions::from_mode(0o755))
            .map_err(|error| format!("cannot make {} executable: {error}", entrypoint.display()))?;
    }
    write_json_atomically(
        staging,
        RELEASE_MARKER,
        &ReleaseMarker {
            version,
            target,
            entrypoint: file,
            sha256,
        },
    )
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

/// The version a managed CLI reports: the first token of `--version`'s
/// output that starts with a digit (`codex-cli 0.153.3` → `0.153.3`,
/// `2.1.311 (Claude Code)` → `2.1.311`).
pub fn parse_cli_version(output: &str) -> Option<&str> {
    output
        .split_whitespace()
        .find(|token| token.starts_with(|c: char| c.is_ascii_digit()))
}

/// Run `<entrypoint> --version` under `env` and return the version it
/// reports.
fn probe_version(entrypoint: &Path, env: &[(&str, &str)]) -> Option<String> {
    let mut child = std::process::Command::new(entrypoint)
        .arg("--version")
        .envs(env.iter().copied())
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
    parse_cli_version(&output).map(str::to_string)
}

/// Run `wait` where blocking is allowed: a worker of a multi-thread runtime
/// hands its queue off with `block_in_place` first. `block_in_place` panics
/// on a current-thread runtime, so that flavor and plain threads run it
/// directly.
fn wait_off_runtime<T>(wait: impl FnOnce() -> T) -> T {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(wait)
        }
        _ => wait(),
    }
}

/// Run `work` on its own thread: a `reqwest::blocking` client must never be
/// built or dropped on a tokio worker, and callers here may be on one.
///
/// A worker of a multi-thread runtime hands its queue off with
/// `block_in_place` before it joins, as `cairn_db::storage::run_db_blocking`
/// does, so a download costs the runtime one thread rather than one worker for
/// its whole length.
fn off_runtime<T: Send + 'static, E: Send + 'static + From<String>>(
    work: impl FnOnce() -> Result<T, E> + Send + 'static,
) -> Result<T, E> {
    wait_off_runtime(move || {
        std::thread::spawn(work)
            .join()
            .map_err(|_| E::from("the download thread panicked".to_string()))?
    })
}

/// Whether a failed request is a network fault worth retrying.
fn classify(error: reqwest::Error) -> FetchError {
    match error.status() {
        Some(status) => FetchError {
            transient: status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS,
            reason: error.to_string(),
        },
        None if error.is_builder() => FetchError::refused(error.to_string()),
        // Connect, DNS, a timeout, or the body cut off.
        None => FetchError::transient(error.to_string()),
    }
}

fn user_agent(tool: &str) -> String {
    format!("cairn-managed-{tool}/{}", env!("CARGO_PKG_VERSION"))
}

/// Downloads over HTTPS.
pub struct HttpFetch {
    user_agent: String,
}

impl HttpFetch {
    /// Downloads for the managed `tool`, named in the user agent.
    pub fn new(tool: &str) -> Self {
        Self {
            user_agent: user_agent(tool),
        }
    }
}

/// How long one read may wait for bytes. `reqwest::blocking` applies its
/// `timeout` to each operation (the send, and every body read), not to the
/// transfer, so this cuts off a stalled transfer after a minute without
/// bounding a slow one that keeps making progress.
const READ_STALL: Duration = Duration::from_secs(60);

impl Fetch for HttpFetch {
    fn download(
        &self,
        url: &str,
        dest: &Path,
        max_bytes: u64,
        stop: &Arc<AtomicBool>,
    ) -> Result<(), FetchError> {
        use std::io::Write;
        let url = url.to_string();
        let dest = dest.to_path_buf();
        let user_agent = self.user_agent.clone();
        let stop = Arc::clone(stop);
        off_runtime(move || {
            let client = cairn_tls::blocking_client_builder()
                .user_agent(user_agent)
                .connect_timeout(Duration::from_secs(30))
                .timeout(READ_STALL)
                .build()
                .map_err(|error| FetchError::refused(error.to_string()))?;
            let mut response = client
                .get(&url)
                .send()
                .and_then(|response| response.error_for_status())
                .map_err(classify)?;
            let mut file = std::fs::File::create(&dest).map_err(|error| {
                FetchError::refused(format!("cannot create {}: {error}", dest.display()))
            })?;
            let mut buffer = vec![0u8; 1 << 16];
            let mut written = 0u64;
            loop {
                if stop.load(Ordering::SeqCst) {
                    return Err(FetchError::transient(SHUTTING_DOWN));
                }
                // A read error mid-body is the connection stalling or
                // resetting.
                let read = response
                    .read(&mut buffer)
                    .map_err(|error| FetchError::transient(error.to_string()))?;
                if read == 0 {
                    break;
                }
                written += read as u64;
                if written > max_bytes {
                    return Err(FetchError::refused(format!(
                        "it is larger than the pinned {max_bytes} bytes"
                    )));
                }
                file.write_all(&buffer[..read])
                    .map_err(|error| FetchError::refused(error.to_string()))?;
            }
            file.sync_all()
                .map_err(|error| FetchError::refused(error.to_string()))
        })
    }
}

/// The pin CI publishes for this build's integration.
pub struct PublishedPin {
    url: String,
    user_agent: String,
}

impl PublishedPin {
    pub fn new(url: String, tool: &str) -> Self {
        Self {
            url,
            user_agent: user_agent(tool),
        }
    }
}

impl PinSource for PublishedPin {
    fn fetch(&self, etag: Option<&str>) -> Result<PinFetch, String> {
        let url = self.url.clone();
        let etag = etag.map(str::to_string);
        let user_agent = self.user_agent.clone();
        off_runtime(move || {
            let client = cairn_tls::blocking_client_builder()
                .user_agent(user_agent)
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
    fn download(
        &self,
        _url: &str,
        _dest: &Path,
        _max_bytes: u64,
        _stop: &Arc<AtomicBool>,
    ) -> Result<(), FetchError> {
        Err(FetchError::refused(REFUSED))
    }
}

#[cfg(any(test, feature = "test-utils"))]
pub mod testing;

#[cfg(test)]
pub(crate) mod tests;
