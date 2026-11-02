//! Live containment probes: attempted crossings at the seams that actually run
//! work.
//!
//! Nothing here reads a profile and concludes that a boundary holds. A profile
//! read can prove a rule is absent; only an attempted crossing can show that
//! one is effective. Each probe therefore performs two crossings through the
//! same seam whose placement is being decided: the in-bounds one it is supposed
//! to be allowed, and the out-of-bounds one it is supposed to be denied. The
//! first is the positive control. Without it, a harness that cannot write
//! anywhere at all reports exactly what a working sandbox reports.

#[cfg(unix)]
use std::path::{Path, PathBuf};

use cairn_common::executor_protocol::{
    ContainmentObservations, EdgeDisposition, EdgeFingerprint, EdgeId, EffectClass, Fingerprint,
    FingerprintAlgorithm, InvalidationInput, ObservedEffect, PolicyMode, PositiveScope,
    ProbeVersion, RemotePlatform,
};

use crate::SandboxPolicy;

/// Locally-minted proof that a crossing was actually attempted.
///
/// Deliberately not serializable, and deliberately private in its fields: in
/// this process only this module can mint one, so a configuration read cannot
/// produce a positive disposition and the compiler is what enforces it. On the
/// wire an observation is an authenticated claim a machine makes about itself,
/// and serializing this witness would imply a guarantee that does not survive
/// the link.
pub struct ProbeWitness {
    edge: EdgeId,
    effect: EffectClass,
    positive_control_passed: bool,
    crossing_permitted: bool,
    detail: String,
}

impl ProbeWitness {
    /// Whether the in-bounds control crossing succeeded. Reading a witness is
    /// harmless; only minting one is restricted to this module.
    pub fn positive_control_passed(&self) -> bool {
        self.positive_control_passed
    }

    /// Whether the out-of-bounds crossing succeeded.
    pub fn crossing_permitted(&self) -> bool {
        self.crossing_permitted
    }

    /// What the probe saw, in words.
    pub fn detail(&self) -> &str {
        &self.detail
    }

    /// Turn an attempted crossing into the disposition it earns.
    ///
    /// A probe whose positive control failed measured a broken seam rather than
    /// a contained one, so it yields `Unmeasured` and names that: reporting
    /// `Refused` there is precisely the false confidence this module exists to
    /// prevent.
    pub fn into_disposition(self, scope: PositiveScope) -> EdgeDisposition {
        let observed = ObservedEffect {
            effect: self.effect,
            positive_control_passed: self.positive_control_passed,
            detail: self.detail,
        };
        if !self.positive_control_passed {
            return EdgeDisposition::Unmeasured {
                reason: format!(
                    "the in-bounds control crossing for {:?} did not succeed, so this probe measured a broken seam rather than a contained one: {}",
                    self.edge, observed.detail
                ),
            };
        }
        if self.crossing_permitted {
            EdgeDisposition::Ambient {
                observed,
                scope: Some(scope),
            }
        } else {
            EdgeDisposition::Refused { scope, observed }
        }
    }
}

/// The current value of one declared invalidation input, or `None` if this
/// machine cannot read it.
///
/// `None` is the whole point of the return type. Fingerprint completeness is an
/// earned claim: if even one input capable of widening an edge cannot be
/// enumerated or read, a positive disposition on that edge must be
/// unconstructible rather than merely discouraged. Because [`PositiveScope`]
/// takes a [`Fingerprint`] by value, a `None` here makes the positive variants
/// impossible to build, and the probe is forced to report `Unmeasured` naming
/// the input it could not obtain.
/// Where an invalidation input's current value comes from.
///
/// A trait rather than a bare function because the values it reads are exactly
/// the mutable host state this whole mechanism exists to notice moving. A test
/// that can only mutate an already-built matrix proves the comparator and
/// nothing about invalidation; moving an input underneath the production
/// assembly path is what proves the machine actually re-reads it.
pub trait InvalidationInputs {
    fn value(&self, input: InvalidationInput, build_id: &str, boot: &str) -> Option<String>;
}

/// The real host. Every read here is live — the profile is regenerated, the
/// mount table is re-run — so nothing in this path may be memoized.
pub struct LiveInputs;

impl InvalidationInputs for LiveInputs {
    fn value(&self, input: InvalidationInput, build_id: &str, boot: &str) -> Option<String> {
        input_value(input, build_id, boot)
    }
}

fn input_value(input: InvalidationInput, build_id: &str, boot: &str) -> Option<String> {
    match input {
        InvalidationInput::ExecutorBuild => Some(build_id.to_string()),
        InvalidationInput::BootGeneration => Some(boot.to_string()),
        InvalidationInput::SandboxProfile => crate::enforcement_description(),
        InvalidationInput::MountTopology => mount_topology(),
        InvalidationInput::PageCallPolicy => crate::page_call::policy_input(),
        // The remaining inputs have no closed, readable form on this machine
        // yet: a network policy that is not expressed anywhere cannot be read,
        // and the build-service set is user-configured and open. Their edges
        // therefore stay unmeasured, which is the correct outcome rather than a
        // defect -- see the owning children in
        // `docs/security/executor-containment.md`.
        InvalidationInput::NetworkPolicy
        | InvalidationInput::DnsConfiguration
        | InvalidationInput::ProxyConfiguration
        | InvalidationInput::DependencyCatalogConfiguration
        | InvalidationInput::PackageConfiguration
        | InvalidationInput::ToolchainConfiguration
        | InvalidationInput::CellLeasePolicy
        | InvalidationInput::RelayPolicy
        | InvalidationInput::ArtifactPolicy
        | InvalidationInput::ExecutorLinkPolicy
        | InvalidationInput::BuildServiceConfiguration
        // The runner assembles a resident's environment from its own, so the
        // input that could widen this edge is the runner's whole environment,
        // which the executor cannot read. cairn/3391 owns closing it.
        | InvalidationInput::EnvironmentAssemblyPolicy
        | InvalidationInput::DevInstanceIngressPolicy
        | InvalidationInput::DesktopAutomationConfiguration => None,
    }
}

/// The mount table this machine's paths resolve through.
fn mount_topology() -> Option<String> {
    #[cfg(target_os = "linux")]
    {
        std::fs::read_to_string("/proc/self/mountinfo").ok()
    }
    #[cfg(target_os = "macos")]
    {
        let output = std::process::Command::new("/sbin/mount").output().ok()?;
        output
            .status
            .success()
            .then(|| String::from_utf8_lossy(&output.stdout).into_owned())
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        None
    }
}

/// This edge's invalidation fingerprint as it stands now, or `None` if any
/// declared input could not be read.
pub fn fingerprint(edge: EdgeId, build_id: &str, boot: &str) -> Option<Fingerprint> {
    fingerprint_from(&LiveInputs, edge, build_id, boot)
}

pub fn fingerprint_from(
    provider: &dyn InvalidationInputs,
    edge: EdgeId,
    build_id: &str,
    boot: &str,
) -> Option<Fingerprint> {
    let inputs = edge.definition().invalidation_inputs;
    let mut hasher = <sha2::Sha256 as sha2::Digest>::new();
    for input in inputs {
        let value = provider.value(*input, build_id, boot)?;
        sha2::Digest::update(
            &mut hasher,
            format!("{input:?}\u{1f}{value}\u{1e}").as_bytes(),
        );
    }
    Some(Fingerprint {
        algorithm: FingerprintAlgorithm::Sha256,
        digest: format!("{:x}", sha2::Digest::finalize(hasher)),
        inputs: inputs.to_vec(),
    })
}

/// Every edge's current fingerprint, so a runner can tell a measurement that is
/// still current from one whose inputs moved underneath it.
pub fn current_fingerprints(build_id: &str, boot: &str) -> Vec<EdgeFingerprint> {
    current_fingerprints_from(&LiveInputs, build_id, boot)
}

pub fn current_fingerprints_from(
    provider: &dyn InvalidationInputs,
    build_id: &str,
    boot: &str,
) -> Vec<EdgeFingerprint> {
    cairn_common::executor_protocol::CONTAINMENT_EDGE_IDS
        .into_iter()
        .map(|edge| EdgeFingerprint {
            edge,
            fingerprint: fingerprint_from(provider, edge, build_id, boot),
        })
        .collect()
}

/// Attempt both crossings of the batch-spawn filesystem seam and report what
/// happened.
///
/// The spawn goes through exactly the wrapping a batch item gets -- `wrap_argv`
/// then `install_pre_exec` -- because a probe that applied confinement its own
/// way would measure its own harness. The out-of-bounds target sits directly in
/// `$HOME`, which is deliberately outside every writable carve-out: the temp
/// dirs and toolchain caches are granted on purpose, so writing to one would
/// prove nothing about the boundary.
#[cfg(unix)]
pub fn probe_batch_spawn_filesystem(mode: PolicyMode) -> Result<ProbeWitness, String> {
    let worktree = tempfile::tempdir().map_err(|e| format!("create probe worktree: {e}"))?;
    let worktree = worktree
        .path()
        .canonicalize()
        .unwrap_or_else(|_| worktree.path().into());
    let in_bounds = worktree.join("containment-probe-in-bounds");

    let home =
        crate::home_dir().ok_or_else(|| "no home directory to probe outside of".to_string())?;
    let out_of_bounds = home.join(format!(
        "cairn-containment-probe-{}",
        uuid::Uuid::new_v4().simple()
    ));

    // Each mode is exercised through the policy that mode actually builds.
    // `Unconfined` builds none, which is the honest thing to measure: the
    // production path applies no policy either, so the probe must not invent
    // one and then report what it found.
    let policy = match mode {
        PolicyMode::Confined => Some(SandboxPolicy::for_run(&worktree, &[], Vec::new())),
        PolicyMode::ReadOnlyCheckout => Some(SandboxPolicy::for_readonly_checkout(
            &worktree,
            &[],
            Vec::new(),
        )),
        PolicyMode::Unconfined => None,
        PolicyMode::NotApplicable => {
            return Err("the filesystem seam always runs under a policy mode".to_string())
        }
        PolicyMode::PageCall => {
            return Err("the page-call policy is measured by its own probe".to_string())
        }
    };
    let script = format!(
        "printf probe > {} 2>/dev/null; printf probe > {} 2>/dev/null; exit 0",
        shell_quote(&in_bounds),
        shell_quote(&out_of_bounds)
    );
    let args = vec!["-c".to_string(), script];
    let (program, args) = match &policy {
        Some(policy) => crate::wrap_argv("/bin/sh", &args, policy),
        None => ("/bin/sh".to_string(), args),
    };
    let mut command = std::process::Command::new(&program);
    command.args(&args);
    if let Some(policy) = &policy {
        crate::install_pre_exec(&mut command, policy);
    }
    command
        .output()
        .map_err(|e| format!("spawn the batch-seam probe: {e}"))?;

    let positive_control_passed = in_bounds.is_file();
    let crossing_permitted = out_of_bounds.exists();
    // Leave nothing behind on a machine that turned out to be unconfined.
    let _ = std::fs::remove_file(&out_of_bounds);

    let detail = match (positive_control_passed, crossing_permitted) {
        (true, false) => "in-bounds write succeeded and the out-of-bounds write into the home directory was refused".to_string(),
        (true, true) => "in-bounds write succeeded and the out-of-bounds write into the home directory also succeeded, so this seam applies no filesystem confinement".to_string(),
        (false, _) => "the in-bounds control write did not appear, so this probe could not exercise the seam".to_string(),
    };

    Ok(ProbeWitness {
        edge: batch_filesystem_edge(mode),
        effect: EffectClass::Write,
        positive_control_passed,
        crossing_permitted,
        detail,
    })
}

#[cfg(not(unix))]
pub fn probe_batch_spawn_filesystem(mode: PolicyMode) -> Result<ProbeWitness, String> {
    let _ = mode;
    Err("no probe implements the batch-spawn filesystem seam on this platform yet".to_string())
}

/// Which edge a batch-seam measurement is about.
pub const fn batch_filesystem_edge(mode: PolicyMode) -> EdgeId {
    match mode {
        PolicyMode::Confined => EdgeId::BatchSpawnFilesystemConfined,
        PolicyMode::ReadOnlyCheckout => EdgeId::BatchSpawnFilesystemReadOnlyCheckout,
        PolicyMode::PageCall => EdgeId::PageCallFilesystemRead,
        PolicyMode::Unconfined | PolicyMode::NotApplicable => {
            EdgeId::BatchSpawnFilesystemUnconfined
        }
    }
}

/// The probed class and mode of a host-socket edge, or `None` for an edge this
/// module has no host-socket probe for.
fn host_socket_class(
    edge: EdgeId,
) -> Option<(cairn_common::executor_protocol::ProcessClass, PolicyMode)> {
    let key = edge.definition().key;
    (host_socket_edge(key.process_class, key.policy_mode) == Some(edge))
        .then_some((key.process_class, key.policy_mode))
}

/// Which edge a host-socket measurement is about, for the process classes that
/// have a probe. The other classes stay `Unmeasured`.
pub const fn host_socket_edge(
    class: cairn_common::executor_protocol::ProcessClass,
    mode: PolicyMode,
) -> Option<EdgeId> {
    use cairn_common::executor_protocol::ProcessClass;
    Some(match (class, mode) {
        (ProcessClass::BatchSpawn, PolicyMode::Confined) => EdgeId::BatchSpawnHostSocketConfined,
        (ProcessClass::BatchSpawn, PolicyMode::ReadOnlyCheckout) => {
            EdgeId::BatchSpawnHostSocketReadOnlyCheckout
        }
        (ProcessClass::BatchSpawn, PolicyMode::Unconfined) => {
            EdgeId::BatchSpawnHostSocketUnconfined
        }
        (ProcessClass::PipedResident, PolicyMode::Confined) => {
            EdgeId::PipedResidentHostSocketConfined
        }
        (ProcessClass::PipedResident, PolicyMode::ReadOnlyCheckout) => {
            EdgeId::PipedResidentHostSocketReadOnlyCheckout
        }
        (ProcessClass::PipedResident, PolicyMode::Unconfined) => {
            EdgeId::PipedResidentHostSocketUnconfined
        }
        (ProcessClass::PtyResident, PolicyMode::Confined) => EdgeId::PtyResidentHostSocketConfined,
        (ProcessClass::PtyResident, PolicyMode::ReadOnlyCheckout) => {
            EdgeId::PtyResidentHostSocketReadOnlyCheckout
        }
        (ProcessClass::PtyResident, PolicyMode::Unconfined) => {
            EdgeId::PtyResidentHostSocketUnconfined
        }
        _ => return None,
    })
}

/// Attempt, through a process class's real spawn seam, to reach a host
/// service's unix socket from inside the fence, and report what happened.
///
/// The harness -- this unconfined process -- binds a pathname listener in a
/// fresh directory beneath the temp root and an abstract listener, which is
/// the shape of the session bus, a tmux server, or an ssh agent. The temp root
/// is deliberately in the writable set: write rights there must not carry
/// socket resolution, and this is where that is proven. The child spawns
/// through `wrap_argv`, then the class's own seam ([`crate::attach_pty_session`]
/// for a PTY resident), then `install_pre_exec`, and only then a final hook
/// that makes the attempts with raw syscalls, so no helper binary is needed
/// and nothing allocates. The positive control binds and connects to the
/// job's own socket in its scratch: a seam that cannot use a socket at all
/// measured nothing.
#[cfg(target_os = "linux")]
pub fn probe_host_socket(
    class: cairn_common::executor_protocol::ProcessClass,
    mode: PolicyMode,
) -> Result<ProbeWitness, String> {
    use cairn_common::executor_protocol::ProcessClass;
    use std::os::linux::net::SocketAddrExt;
    use std::os::unix::process::CommandExt;

    let edge = host_socket_edge(class, mode)
        .ok_or_else(|| format!("no host-socket probe for {class:?} under {mode:?}"))?;
    // Every socket lives under a short root: `$TMPDIR` can be a cell scratch or
    // check-lane path deep enough to overrun the 108-byte socket address,
    // which would leave the probe measuring nothing.
    let worktree =
        socket_probe::short_tempdir().map_err(|e| format!("create probe worktree: {e}"))?;
    let scratch =
        socket_probe::short_tempdir().map_err(|e| format!("create probe scratch: {e}"))?;
    let outside =
        socket_probe::short_tempdir().map_err(|e| format!("create probe host dir: {e}"))?;

    let host_path = outside.path().join("host.sock");
    let _host = std::os::unix::net::UnixListener::bind(&host_path)
        .map_err(|e| format!("bind the probe's host socket: {e}"))?;
    let abstract_name = format!("cairn-containment-{}", uuid::Uuid::new_v4().simple());
    let _host_abstract = std::os::unix::net::SocketAddr::from_abstract_name(&abstract_name)
        .and_then(|address| std::os::unix::net::UnixListener::bind_addr(&address))
        .map_err(|e| format!("bind the probe's abstract socket: {e}"))?;

    let attempt = socket_probe::Attempt::new(
        &scratch.path().join("own.sock"),
        &host_path,
        abstract_name.as_bytes(),
    )?;

    let scratch_grant = [scratch.path().to_string_lossy().into_owned()];
    let policy = match mode {
        PolicyMode::Confined => Some(SandboxPolicy::for_run(
            worktree.path(),
            &scratch_grant,
            Vec::new(),
        )),
        PolicyMode::ReadOnlyCheckout => Some(SandboxPolicy::for_readonly_checkout(
            worktree.path(),
            &scratch_grant,
            Vec::new(),
        )),
        PolicyMode::Unconfined => None,
        PolicyMode::NotApplicable => {
            return Err("the host-socket seam always runs under a policy mode".to_string())
        }
        PolicyMode::PageCall => {
            return Err("the page-call policy is measured by its own probe".to_string())
        }
    }
    .map(|mut policy| {
        policy.allow_job_sockets_in(scratch.path());
        policy
    });

    let (report_read, report_write) = socket_probe::report_pipe()?;
    let (program, args) = match &policy {
        Some(policy) => crate::wrap_argv("/bin/true", &[], policy),
        None => ("/bin/true".to_string(), Vec::new()),
    };
    let mut command = std::process::Command::new(&program);
    command.args(&args).current_dir(worktree.path());
    let _pty_master = match class {
        ProcessClass::PtyResident => {
            let (master, slave) = socket_probe::open_pty()?;
            crate::attach_pty_session(&mut command, slave)
                .map_err(|e| format!("attach the probe to a PTY: {e}"))?;
            Some(master)
        }
        _ => {
            command
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null());
            None
        }
    };
    if let Some(policy) = &policy {
        crate::install_pre_exec(&mut command, policy);
    }
    let report_fd = std::os::fd::AsRawFd::as_raw_fd(&report_write);
    // SAFETY: the attempt makes only socket, bind, listen, connect, close,
    // unlink and write calls on stack data captured by value.
    unsafe {
        command.pre_exec(move || {
            attempt.run(report_fd);
            Ok(())
        });
    }
    let spawned = command.spawn();
    drop(report_write);
    let mut child = spawned.map_err(|e| format!("spawn the host-socket probe: {e}"))?;
    let mut report = Vec::new();
    let _ = std::io::Read::read_to_end(&mut std::fs::File::from(report_read), &mut report);
    let _ = child.wait();

    let [control, pathname, abstract_reached] = report[..] else {
        return Err(format!(
            "the host-socket probe reported {} bytes rather than three",
            report.len()
        ));
    };
    let positive_control_passed = control == b'1';
    let pathname_reached = pathname == b'1';
    let abstract_reached = abstract_reached == b'1';
    let detail = socket_probe::describe(
        positive_control_passed,
        pathname_reached,
        abstract_reached,
        crate::linux::kernel_abi(),
    );
    Ok(ProbeWitness {
        edge,
        effect: EffectClass::Invoke,
        positive_control_passed,
        crossing_permitted: pathname_reached || abstract_reached,
        detail,
    })
}

/// Only Linux has a probe. macOS confines through `sandbox-exec`, which a
/// pre-exec attempt would run before, so it would measure its own harness;
/// that platform's socket boundary is cairn/5493.
#[cfg(not(target_os = "linux"))]
pub fn probe_host_socket(
    class: cairn_common::executor_protocol::ProcessClass,
    mode: PolicyMode,
) -> Result<ProbeWitness, String> {
    let _ = (class, mode);
    Err(format!(
        "no probe attempts the host-socket seam on {}; it applies no socket boundary yet",
        std::env::consts::OS
    ))
}

#[cfg(target_os = "linux")]
pub(crate) mod socket_probe {
    use std::os::fd::{FromRawFd, OwnedFd};
    use std::os::unix::ffi::OsStrExt;
    use std::path::Path;

    /// The three attempts, as addresses built before the fork so the child
    /// only makes syscalls.
    #[derive(Clone, Copy)]
    pub(super) struct Attempt {
        control: (libc::sockaddr_un, libc::socklen_t),
        pathname: (libc::sockaddr_un, libc::socklen_t),
        abstract_name: (libc::sockaddr_un, libc::socklen_t),
    }

    // SAFETY: plain address data.
    unsafe impl Send for Attempt {}
    unsafe impl Sync for Attempt {}

    pub(crate) fn address(
        bytes: &[u8],
        abstract_name: bool,
    ) -> Result<(libc::sockaddr_un, libc::socklen_t), String> {
        // SAFETY: an all-zero sockaddr_un is a valid empty address.
        let mut address: libc::sockaddr_un = unsafe { std::mem::zeroed() };
        address.sun_family = libc::AF_UNIX as libc::sa_family_t;
        let offset = usize::from(abstract_name);
        // A pathname needs its terminating NUL inside sun_path.
        if offset + bytes.len() >= address.sun_path.len() {
            return Err(format!(
                "probe socket name is {} bytes, longer than a unix socket address holds",
                bytes.len()
            ));
        }
        for (slot, byte) in address.sun_path[offset..].iter_mut().zip(bytes) {
            *slot = *byte as libc::c_char;
        }
        let family = std::mem::size_of::<libc::sa_family_t>();
        let length = if abstract_name {
            family + 1 + bytes.len()
        } else {
            family + bytes.len() + 1
        };
        Ok((address, length as libc::socklen_t))
    }

    impl Attempt {
        pub(super) fn new(
            control: &Path,
            host: &Path,
            abstract_name: &[u8],
        ) -> Result<Self, String> {
            Ok(Self {
                control: address(control.as_os_str().as_bytes(), false)?,
                pathname: address(host.as_os_str().as_bytes(), false)?,
                abstract_name: address(abstract_name, true)?,
            })
        }

        /// Runs in the confined child. Writes `1` or `0` for the control, the
        /// pathname crossing and the abstract crossing, in that order.
        pub(super) fn run(&self, report: libc::c_int) {
            // SAFETY: async-signal-safe syscalls on owned stack data.
            unsafe {
                let control = self.control_reachable();
                let result = [
                    b'0' + u8::from(control),
                    b'0' + u8::from(connects(&self.pathname)),
                    b'0' + u8::from(connects(&self.abstract_name)),
                ];
                libc::write(report, result.as_ptr().cast(), result.len());
            }
        }

        unsafe fn control_reachable(&self) -> bool {
            let (address, length) = &self.control;
            let listener = libc::socket(libc::AF_UNIX, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0);
            if listener < 0 {
                return false;
            }
            let bound = libc::bind(
                listener,
                (address as *const libc::sockaddr_un).cast(),
                *length,
            ) == 0
                && libc::listen(listener, 1) == 0;
            let reached = bound && connects(&self.control);
            libc::close(listener);
            libc::unlink(address.sun_path.as_ptr());
            reached
        }
    }

    unsafe fn connects((address, length): &(libc::sockaddr_un, libc::socklen_t)) -> bool {
        let socket = libc::socket(libc::AF_UNIX, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0);
        if socket < 0 {
            return false;
        }
        let connected = libc::connect(
            socket,
            (address as *const libc::sockaddr_un).cast(),
            *length,
        ) == 0;
        libc::close(socket);
        connected
    }

    /// A fresh directory directly under `/tmp`, short enough that a socket path
    /// beneath it always fits a unix socket address. `/tmp` itself is a shared
    /// root and never socket-resolvable; a directory beneath it can be.
    pub(crate) fn short_tempdir() -> std::io::Result<tempfile::TempDir> {
        tempfile::Builder::new().prefix("cairn-").tempdir_in("/tmp")
    }

    pub(crate) fn report_pipe() -> Result<(OwnedFd, OwnedFd), String> {
        let mut fds = [0; 2];
        // SAFETY: fds has room for both ends.
        if unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) } != 0 {
            return Err(format!(
                "open the probe's report pipe: {}",
                std::io::Error::last_os_error()
            ));
        }
        // SAFETY: pipe2 returned two fresh descriptors this function now owns.
        Ok(unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) })
    }

    pub(crate) fn open_pty() -> Result<(OwnedFd, std::fs::File), String> {
        let (mut master, mut slave) = (0, 0);
        // SAFETY: openpty writes two descriptors; the optional arguments are null.
        let rc = unsafe {
            libc::openpty(
                &mut master,
                &mut slave,
                std::ptr::null_mut(),
                std::ptr::null(),
                std::ptr::null(),
            )
        };
        if rc != 0 {
            return Err(format!(
                "open a PTY for the probe: {}",
                std::io::Error::last_os_error()
            ));
        }
        // SAFETY: openpty returned two fresh descriptors this function now owns.
        Ok(unsafe {
            (
                OwnedFd::from_raw_fd(master),
                std::fs::File::from_raw_fd(slave),
            )
        })
    }

    pub(super) fn describe(
        control: bool,
        pathname: bool,
        abstract_name: bool,
        kernel_abi: Option<i32>,
    ) -> String {
        let abi = kernel_abi.map_or_else(|| "none".to_string(), |abi| abi.to_string());
        if !control {
            return "the job could not bind and reach its own socket in its scratch, so this probe could not exercise the seam".to_string();
        }
        let below = |required: i32| kernel_abi.is_none_or(|abi| abi < required);
        let mut reached = Vec::new();
        if pathname {
            reached.push(if below(crate::linux::RESOLVE_UNIX_ABI) {
                format!("kernel Landlock ABI {abi} < {}: pathname sockets served from outside the fence are reachable", crate::linux::RESOLVE_UNIX_ABI)
            } else {
                format!("a pathname socket served from outside the fence was reachable on kernel Landlock ABI {abi}")
            });
        }
        if abstract_name {
            reached.push(if below(crate::linux::ABSTRACT_SCOPE_ABI) {
                format!("kernel Landlock ABI {abi} < {}: abstract sockets served from outside the fence are reachable", crate::linux::ABSTRACT_SCOPE_ABI)
            } else {
                format!("an abstract socket served from outside the fence was reachable on kernel Landlock ABI {abi}")
            });
        }
        if reached.is_empty() {
            format!("the job reached its own socket, and both a pathname and an abstract socket served from outside the fence were refused (kernel Landlock ABI {abi})")
        } else {
            reached.join("; ")
        }
    }
}

/// A host-socket edge as this machine can currently report it.
pub fn observe_host_socket(
    build_id: &str,
    boot: &str,
    platform: Option<RemotePlatform>,
    class: cairn_common::executor_protocol::ProcessClass,
    mode: PolicyMode,
) -> EdgeDisposition {
    let Some(edge) = host_socket_edge(class, mode) else {
        return EdgeDisposition::Unmeasured {
            reason: format!("no probe attempts the {class:?} host-socket seam yet"),
        };
    };
    let Some(platform) = platform else {
        return EdgeDisposition::Unmeasured {
            reason: "this machine is not one of the published executor platforms, so its measurements have no closed platform identity to bind to".into(),
        };
    };
    let Some(invalidation_fingerprint) = fingerprint(edge, build_id, boot) else {
        return EdgeDisposition::Unmeasured {
            reason: "this machine cannot currently read every input that could widen the host-socket seam, so no positive disposition is constructible".into(),
        };
    };
    let witness = match probe_host_socket(class, mode) {
        Ok(witness) => witness,
        Err(reason) => return EdgeDisposition::Unmeasured { reason },
    };
    let definition = edge.definition();
    witness.into_disposition(PositiveScope {
        executor_build_id: build_id.to_string(),
        boot_generation: boot.to_string(),
        platform,
        process_class: definition.key.process_class,
        carrier: definition.key.carrier,
        policy_mode: definition.key.policy_mode,
        probe_version: ProbeVersion::V1,
        claim_revision: definition.claim_revision,
        invalidation_fingerprint,
    })
}

/// Attempt to read a Cairn home's operator credential the way an unfenced agent
/// spawn would, and report whether it could.
///
/// The planted home sits directly in `$HOME` under a `.cairn` name, which is
/// where every real Cairn home on the machine lives, and the spawn goes through
/// exactly [`crate::Confinement::CredentialFloor`]'s wrapping, because a probe
/// that confined itself some other way would measure its own harness. The
/// positive control is an ordinary file beside the credential: a seam that
/// cannot read anything there measured nothing.
#[cfg(unix)]
pub fn probe_operator_credential_read() -> Result<ProbeWitness, String> {
    const CONTROL: &str = "cairn-containment-control";
    const CREDENTIAL: &str = "cairn-containment-credential";

    let home = crate::home_dir()
        .ok_or_else(|| "no home directory to plant a Cairn home in".to_string())?;
    let probe_home = home.join(format!(
        ".cairn-containment-probe-{}",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir(&probe_home).map_err(|e| format!("create the probe home: {e}"))?;
    let control = probe_home.join("control");
    let credential = probe_home.join(cairn_common::auth::OPERATOR_SECRET_FILE);
    let planted = std::fs::write(&control, CONTROL)
        .and_then(|()| std::fs::write(&credential, CREDENTIAL))
        .map_err(|e| format!("plant the probe credential: {e}"));
    let output = planted.and_then(|()| {
        let script = format!(
            "cat {} 2>/dev/null; cat {} 2>/dev/null; exit 0",
            shell_quote(&control),
            shell_quote(&credential)
        );
        let confinement = crate::Confinement::CredentialFloor;
        let (program, args) = confinement.wrap_argv("/bin/sh", &["-c".to_string(), script]);
        let mut command = std::process::Command::new(&program);
        command.args(&args);
        confinement.install_pre_exec(&mut command);
        command
            .output()
            .map_err(|e| format!("spawn the operator credential probe: {e}"))
    });
    // The probe home is removed by this unconfined process whatever happened.
    let _ = std::fs::remove_dir_all(&probe_home);
    let stdout = String::from_utf8_lossy(&output?.stdout).into_owned();

    let positive_control_passed = stdout.contains(CONTROL);
    let crossing_permitted = stdout.contains(CREDENTIAL);
    let detail = match (positive_control_passed, crossing_permitted) {
        (true, false) => "an agent spawn read an ordinary file in a Cairn home and was refused that home's operator_auth_secret".to_string(),
        (true, true) => format!(
            "an agent spawn read a Cairn home's operator_auth_secret: {} cannot enforce the operator credential boundary, so any agent process on this machine can approve its own escalation with a credential it finds here",
            std::env::consts::OS
        ),
        (false, _) => "the ordinary file beside the planted credential could not be read, so this probe could not exercise the seam".to_string(),
    };
    Ok(ProbeWitness {
        edge: EdgeId::OperatorCredentialRead,
        effect: EffectClass::Read,
        positive_control_passed,
        crossing_permitted,
        detail,
    })
}

#[cfg(not(unix))]
pub fn probe_operator_credential_read() -> Result<ProbeWitness, String> {
    Err(format!(
        "no probe implements the operator credential boundary on {}, and no sandbox primitive enforces it there",
        std::env::consts::OS
    ))
}

/// The operator credential edge as this machine can currently report it.
pub fn observe_operator_credential_read(
    build_id: &str,
    boot: &str,
    platform: Option<RemotePlatform>,
) -> EdgeDisposition {
    let edge = EdgeId::OperatorCredentialRead;
    let Some(platform) = platform else {
        return EdgeDisposition::Unmeasured {
            reason: "this machine is not one of the published executor platforms, so its measurements have no closed platform identity to bind to".into(),
        };
    };
    let Some(invalidation_fingerprint) = fingerprint(edge, build_id, boot) else {
        return EdgeDisposition::Unmeasured {
            reason: "this machine cannot currently read every input that could widen the operator credential boundary, so no positive disposition is constructible".into(),
        };
    };
    let witness = match probe_operator_credential_read() {
        Ok(witness) => witness,
        Err(reason) => return EdgeDisposition::Unmeasured { reason },
    };
    let definition = edge.definition();
    witness.into_disposition(PositiveScope {
        executor_build_id: build_id.to_string(),
        boot_generation: boot.to_string(),
        platform,
        process_class: definition.key.process_class,
        carrier: definition.key.carrier,
        policy_mode: definition.key.policy_mode,
        probe_version: ProbeVersion::V1,
        claim_revision: definition.claim_revision,
        invalidation_fingerprint,
    })
}

#[cfg(unix)]
fn shell_quote(path: &Path) -> String {
    format!("'{}'", path.to_string_lossy().replace('\'', "'\\''"))
}

/// What one probe crossing saw, kept so the page-call measurement can answer
/// for two edges from one run; only this module turns it into a witness.
#[derive(Debug, Clone)]
struct Crossing {
    positive_control_passed: bool,
    crossing_permitted: bool,
    detail: String,
}

/// The page-call policy as this machine measured it, once per process: the
/// read edge, the egress edge, and each program it ran.
#[derive(Debug, Clone)]
struct PageCallMeasurement {
    filesystem: Result<Crossing, String>,
    egress: Result<Crossing, String>,
    programs: Vec<cairn_common::executor_protocol::PageCallProgramEvidence>,
}

fn page_call_measurement() -> &'static PageCallMeasurement {
    static MEASURED: std::sync::OnceLock<PageCallMeasurement> = std::sync::OnceLock::new();
    MEASURED.get_or_init(measure_page_call)
}

/// Each program the page-call probe ran under the page-call policy, and
/// whether it ran. Measured with the page-call edges, so it is current
/// exactly while they are.
pub fn page_call_programs() -> Vec<cairn_common::executor_protocol::PageCallProgramEvidence> {
    page_call_measurement().programs.clone()
}

/// Spawn `program` under `policy` the way a page-call batch item is spawned:
/// through the policy's wrapping, with the environment cleared to `env`, in
/// `dir`, killed if it outlasts a bound a trivial program never approaches.
#[cfg(unix)]
fn run_page_call_spawn(
    policy: &SandboxPolicy,
    env: &[(String, String)],
    dir: &Path,
    program: &str,
    args: &[String],
) -> Result<std::process::Output, String> {
    use std::io::Read;
    let (wrapped, wrapped_args) = crate::wrap_argv(program, args, policy);
    let mut command = std::process::Command::new(&wrapped);
    command
        .args(&wrapped_args)
        .env_clear()
        .envs(env.iter().cloned())
        .current_dir(dir)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    crate::install_pre_exec(&mut command, policy);
    let mut child = command
        .spawn()
        .map_err(|error| format!("spawn {program} under the page-call policy: {error}"))?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if std::time::Instant::now() < deadline => {
                std::thread::sleep(std::time::Duration::from_millis(25));
            }
            Ok(None) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!(
                    "{program} did not finish within 20 s under the page-call policy"
                ));
            }
            Err(error) => return Err(format!("wait for {program}: {error}")),
        }
    };
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    if let Some(mut pipe) = child.stdout.take() {
        let _ = pipe.read_to_end(&mut stdout);
    }
    if let Some(mut pipe) = child.stderr.take() {
        let _ = pipe.read_to_end(&mut stderr);
    }
    Ok(std::process::Output {
        status,
        stdout,
        stderr,
    })
}

#[cfg(unix)]
fn first_words(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes);
    let text = text.trim();
    if text.is_empty() {
        return "no output".to_string();
    }
    text.chars().take(240).collect()
}

/// Attempt every crossing the page-call policy is supposed to refuse, through
/// exactly the wrapping and environment a page-call batch gets, beside the
/// controls it is supposed to allow.
///
/// Refused: a planted `mcp_auth_secret` and `cairn.db` in a Cairn home beside
/// the user's own, the same in every Cairn home an interpreter root sits in
/// (so admitting a pinned toolchain tree is shown not to expose its home), a
/// file directly in the user's home, and a file in a sibling of the batch
/// directory. Allowed: a file in the batch directory, and each program
/// running a trivial line under its own roots. Egress is attempted with the
/// first interpreter that ran: a connection to a stand-in relay port must
/// succeed and one to a second loopback port must not.
#[cfg(unix)]
fn measure_page_call() -> PageCallMeasurement {
    use cairn_common::executor_protocol::PageCallProgramEvidence;
    let unmeasured = |reason: String| PageCallMeasurement {
        filesystem: Err(reason.clone()),
        egress: Err(reason),
        programs: Vec::new(),
    };
    let mut programs = Vec::new();
    let mut resolved = Vec::new();
    for program in crate::page_call::PAGE_CALL_PROGRAMS {
        match crate::page_call::resolve_program(program) {
            Ok(found) => resolved.push(found),
            Err(detail) => programs.push(PageCallProgramEvidence {
                program: program.to_string(),
                passed: false,
                resolved: None,
                detail,
            }),
        }
    }
    let Some(os_home) = crate::home_dir() else {
        return unmeasured("no home directory to plant a Cairn home in".to_string());
    };
    let base = match tempfile::Builder::new().prefix("pc").tempdir_in("/tmp") {
        Ok(base) => base,
        Err(error) => return unmeasured(format!("create the page-call probe directory: {error}")),
    };
    let base_path = base
        .path()
        .canonicalize()
        .unwrap_or_else(|_| base.path().into());
    let dir = base_path.join("d");
    let sibling = base_path.join("s");
    let id = uuid::Uuid::new_v4().simple().to_string();
    let probe_home = os_home.join(format!(".cairn-page-call-probe-{id}"));
    let outside = os_home.join(format!("cairn-page-call-probe-{id}"));
    let homes = crate::OperatorCredentialFloor::for_this_machine()
        .known_homes()
        .unwrap_or_default();
    let interpreter_homes: Vec<PathBuf> = homes
        .iter()
        .filter(|home| {
            resolved
                .iter()
                .flat_map(|program| program.roots.iter())
                .any(|root| root.starts_with(home))
        })
        .map(|home| home.join(format!(".page-call-probe-{id}")))
        .collect();
    let mut targets: Vec<(String, PathBuf)> = vec![
        (
            "a planted mcp_auth_secret in a Cairn home".into(),
            probe_home.join("mcp_auth_secret"),
        ),
        (
            "a planted cairn.db in a Cairn home".into(),
            probe_home.join("cairn.db"),
        ),
        ("a file in the user's home".into(), outside.clone()),
        (
            "a file in a sibling of the batch directory".into(),
            sibling.join("f"),
        ),
    ];
    for planted in &interpreter_homes {
        let home = planted
            .parent()
            .map(Path::display)
            .map(|d| d.to_string())
            .unwrap_or_default();
        targets.push((
            format!("a planted mcp_auth_secret in {home}, beside an admitted interpreter"),
            planted.join("mcp_auth_secret"),
        ));
        targets.push((
            format!("a planted cairn.db in {home}, beside an admitted interpreter"),
            planted.join("cairn.db"),
        ));
    }
    let planted = (|| -> std::io::Result<()> {
        std::fs::create_dir_all(&dir)?;
        crate::page_call::prepare_dir(&dir)?;
        std::fs::write(dir.join("control"), "pc-control")?;
        std::fs::create_dir_all(&sibling)?;
        std::fs::create_dir_all(&probe_home)?;
        for planted in &interpreter_homes {
            std::fs::create_dir_all(planted)?;
        }
        std::fs::write(&outside, "pc-outside")?;
        for (_, target) in &targets {
            std::fs::write(target, "pc-planted")?;
        }
        Ok(())
    })();
    let relay = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0));
    let other = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0));
    let measured = match (planted, &relay, &other) {
        (Err(error), _, _) => unmeasured(format!("plant the page-call probe files: {error}")),
        (_, Err(error), _) | (_, _, Err(error)) => unmeasured(format!(
            "bind the page-call probe's loopback ports: {error}"
        )),
        (Ok(()), Ok(relay), Ok(other)) => {
            let relay_port = relay.local_addr().map(|addr| addr.port()).unwrap_or(0);
            let other_port = other.local_addr().map(|addr| addr.port()).unwrap_or(0);
            measure_page_call_crossings(&dir, &resolved, &targets, relay_port, other_port, programs)
        }
    };
    // Removed by this unconfined process whatever happened.
    let _ = std::fs::remove_dir_all(&probe_home);
    let _ = std::fs::remove_file(&outside);
    for planted in &interpreter_homes {
        let _ = std::fs::remove_dir_all(planted);
    }
    drop(base);
    measured
}

#[cfg(unix)]
fn measure_page_call_crossings(
    dir: &Path,
    resolved: &[crate::page_call::ResolvedProgram],
    targets: &[(String, PathBuf)],
    relay_port: u16,
    other_port: u16,
    mut programs: Vec<cairn_common::executor_protocol::PageCallProgramEvidence>,
) -> PageCallMeasurement {
    use cairn_common::executor_protocol::PageCallProgramEvidence;
    let all_roots: Vec<PathBuf> = resolved
        .iter()
        .flat_map(|program| program.roots.iter().cloned())
        .collect();
    let widest = SandboxPolicy::for_page_call(dir, &all_roots, relay_port);
    let env = crate::page_call::base_env(dir, resolved);

    // A pathname unix socket bound outside the batch directory, so the egress
    // crossing proves the hard-required `ResolveUnix` actually refuses a host
    // socket (`/run/user/<uid>/bus`, a tmux server, the ssh agent) on a
    // kernel that claims ABI 9, not only that a kernel below it fails to build
    // the ruleset. Linux only: on macOS a socket path outside the read
    // allow-list is unreadable, so the filesystem crossing already covers it.
    #[cfg(target_os = "linux")]
    let unix_guard = probe_unix_listener();
    #[cfg(target_os = "linux")]
    let unix_path = unix_guard.as_ref().map(|(_, _, path)| path.clone());
    #[cfg(not(target_os = "linux"))]
    let unix_path: Option<String> = None;

    let mut script = String::new();
    for (index, (_, target)) in targets.iter().enumerate() {
        script.push_str(&format!(
            "cat {} >/dev/null 2>&1 && echo leak:{index}\n",
            shell_quote(target)
        ));
    }
    script.push_str(&format!("cat {}\n", shell_quote(&dir.join("control"))));
    let filesystem = run_page_call_spawn(&widest, &env, dir, "/bin/sh", &["-c".to_string(), script])
        .map(|output| {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let positive_control_passed = stdout.contains("pc-control");
            let leaked: Vec<&str> = stdout
                .lines()
                .filter_map(|line| line.strip_prefix("leak:")?.parse::<usize>().ok())
                .filter_map(|index| targets.get(index).map(|(label, _)| label.as_str()))
                .collect();
            let detail = match (positive_control_passed, leaked.is_empty()) {
                (false, _) => format!(
                    "the batch directory's own file could not be read under the page-call policy, so this probe could not exercise the seam: {}",
                    first_words(&output.stderr)
                ),
                (true, true) => format!(
                    "read the batch directory's own file and was refused {}",
                    targets.iter().map(|(label, _)| label.as_str()).collect::<Vec<_>>().join("; ")
                ),
                (true, false) => format!("the page-call policy let a batch read {}", leaked.join("; ")),
            };
            Crossing {
                positive_control_passed,
                crossing_permitted: !leaked.is_empty(),
                detail,
            }
        });

    // On macOS the page-call profile refuses `process-info*` and `signal`
    // against every process but the batch's own. Whether anything gates
    // `KERN_PROCARGS2` is a fact about the OS, not an assumption the profile
    // text can make (macOS 26.6 gates it under no Seatbelt rule at all), so a
    // crossing proves it per machine: a sibling carries a marker only another
    // process's environment read could see, and the page-call child must fail
    // both to read it and to signal that sibling. A leak, or an inability to
    // run the crossing at all, folds into the filesystem witness so the edge
    // is Ambient or Unmeasured and the machine is never a candidate. Linux is not exposed on this axis: `/proc/<pid>/
    // environ` is outside the read allow-list and signals are scoped (ABI 6).
    #[cfg(target_os = "macos")]
    let filesystem = filesystem
        .map(|crossing| macos_fold_process_table(crossing, dir, resolved, relay_port, &env));

    let mut connect_with: Option<(&crate::page_call::ResolvedProgram, Vec<String>)> = None;
    for program in resolved {
        let args: Vec<String> = match program.program.as_str() {
            "bun" => vec!["-e".into(), "console.log('pc-' + 'ok')".into()],
            "python3" => vec!["-c".into(), "print('pc-' + 'ok')".into()],
            _ => vec!["-c".into(), "echo pc-ok".into()],
        };
        let policy = SandboxPolicy::for_page_call(dir, &program.roots, relay_port);
        let path = program.path.to_string_lossy().into_owned();
        let (passed, detail) = match run_page_call_spawn(&policy, &env, dir, &path, &args) {
            Ok(output)
                if output.status.success()
                    && String::from_utf8_lossy(&output.stdout).contains("pc-ok") =>
            {
                (true, "ran under the page-call policy".to_string())
            }
            Ok(output) => (
                false,
                format!(
                    "exited {} under the page-call policy: {}",
                    output
                        .status
                        .code()
                        .map_or_else(|| "by signal".to_string(), |code| code.to_string()),
                    first_words(&output.stderr)
                ),
            ),
            Err(error) => (false, error),
        };
        if passed && connect_with.is_none() {
            let connect = match program.program.as_str() {
                "python3" => {
                    let unix = unix_path.as_ref().map_or(String::new(), |path| format!(
                        "\ntry:\n    s = socket.socket(socket.AF_UNIX); s.settimeout(2); s.connect({path:?}); s.close(); print('unix-open')\nexcept Exception:\n    print('unix-shut')\n"
                    ));
                    Some(vec![
                        "-c".to_string(),
                        format!(
                            "import socket\nfor port, label in (({relay_port}, 'relay'), ({other_port}, 'other')):\n    try:\n        socket.create_connection(('127.0.0.1', port), 2).close()\n        print(label + '-open')\n    except Exception:\n        print(label + '-shut')\n{unix}"
                        ),
                    ])
                }
                "bun" => Some(vec![
                    "-e".to_string(),
                    format!(
                        "const net = require('net'); for (const [port, label] of [[{relay_port}, 'relay'], [{other_port}, 'other']]) {{ await new Promise((done) => {{ const socket = net.connect(port, '127.0.0.1'); const end = (state) => {{ console.log(label + '-' + state); socket.destroy(); done(); }}; socket.on('connect', () => end('open')); socket.on('error', () => end('shut')); setTimeout(() => end('shut'), 2000); }}); }}"
                    ),
                ]),
                _ => None,
            };
            if let Some(args) = connect {
                connect_with = Some((program, args));
            }
        }
        programs.push(PageCallProgramEvidence {
            program: program.program.clone(),
            passed,
            resolved: Some(program.path.to_string_lossy().into_owned()),
            detail,
        });
    }

    let egress = match connect_with {
        None => Err(
            "no interpreter ran under the page-call policy, so no connection could be attempted"
                .to_string(),
        ),
        Some((program, args)) => {
            let policy = SandboxPolicy::for_page_call(dir, &program.roots, relay_port);
            let path = program.path.to_string_lossy().into_owned();
            run_page_call_spawn(&policy, &env, dir, &path, &args).map(|output| {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let positive_control_passed = stdout.contains("relay-open");
                let tcp_leaked = stdout.contains("other-open");
                let unix_leaked = stdout.contains("unix-open");
                let crossing_permitted = tcp_leaked || unix_leaked;
                let detail = match (positive_control_passed, tcp_leaked, unix_leaked) {
                    (false, _, _) => format!(
                        "{} could not reach the stand-in relay port, so this probe could not exercise the seam: {}",
                        program.program,
                        first_words(&output.stderr)
                    ),
                    (true, false, false) => format!(
                        "{} reached the relay port and was refused a second loopback port and a host unix socket",
                        program.program
                    ),
                    (true, true, _) => format!(
                        "{} reached a loopback port other than the relay, so this platform cannot confine a page-call batch's TCP egress here; the runner's refusal of every request without a valid credential is the bound",
                        program.program
                    ),
                    (true, false, true) => format!(
                        "{} reached a host pathname unix socket outside its allow-list, so this platform cannot confine a page-call batch's socket egress here",
                        program.program
                    ),
                };
                Crossing {
                    positive_control_passed,
                    crossing_permitted,
                    detail,
                }
            })
        }
    };
    #[cfg(target_os = "linux")]
    drop(unix_guard);
    PageCallMeasurement {
        filesystem,
        egress,
        programs,
    }
}

/// Bind a listening pathname unix socket in a short directory beneath `/tmp`,
/// outside any page-call batch directory and so outside its socket-resolvable
/// set. The returned guard keeps it listening until the egress crossing has
/// run; the path is where a confined child must be refused a `connect`.
#[cfg(target_os = "linux")]
fn probe_unix_listener() -> Option<(tempfile::TempDir, std::os::unix::net::UnixListener, String)> {
    let dir = socket_probe::short_tempdir().ok()?;
    let path = dir.path().join("host.sock");
    let listener = std::os::unix::net::UnixListener::bind(&path).ok()?;
    let path = path.to_string_lossy().into_owned();
    Some((dir, listener, path))
}

/// A page-call child's attempt to read another same-uid process's environment
/// through `KERN_PROCARGS2` and to signal it. Prints `PROC <env|noenv>
/// <sig|nosig>`; stdlib only, so it reads nothing outside the allow-list.
#[cfg(target_os = "macos")]
const PROCESS_TABLE_SNIPPET: &str = r#"import ctypes, os, signal, sys
pid = int(sys.argv[1]); marker = sys.argv[2].encode()
libc = ctypes.CDLL(None, use_errno=True)
mib = (ctypes.c_int * 3)(1, 49, pid)  # CTL_KERN, KERN_PROCARGS2, pid
size = ctypes.c_size_t(0)
env_leaked = False
if libc.sysctl(mib, 3, None, ctypes.byref(size), None, 0) == 0 and size.value:
    buf = ctypes.create_string_buffer(size.value)
    if libc.sysctl(mib, 3, buf, ctypes.byref(size), None, 0) == 0:
        env_leaked = marker in buf.raw
try:
    os.kill(pid, signal.SIGCONT); sig = True
except OSError:
    sig = False
print('PROC', 'env' if env_leaked else 'noenv', 'sig' if sig else 'nosig')
"#;

/// Fold the process-table crossing into the filesystem witness. Only called
/// once the filesystem positive control has passed, so a false here is a real
/// boundary failure rather than a broken harness.
#[cfg(target_os = "macos")]
fn macos_fold_process_table(
    mut crossing: Crossing,
    dir: &Path,
    resolved: &[crate::page_call::ResolvedProgram],
    relay_port: u16,
    env: &[(String, String)],
) -> Crossing {
    if !crossing.positive_control_passed {
        return crossing;
    }
    let Some(python) = resolved.iter().find(|program| program.program == "python3") else {
        crossing.positive_control_passed = false;
        crossing.detail = "no python3 resolved to prove a page batch cannot read another process's environment, so the process-table boundary is unproven".into();
        return crossing;
    };
    let marker = format!("pc-env-{}", uuid::Uuid::new_v4().simple());
    let mut sibling = match std::process::Command::new("/bin/sleep")
        .arg("30")
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("PC_PROBE_MARKER", &marker)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
    {
        Ok(child) => child,
        Err(error) => {
            crossing.positive_control_passed = false;
            crossing.detail =
                format!("could not plant a sibling to probe the process-table boundary: {error}");
            return crossing;
        }
    };
    let policy = SandboxPolicy::for_page_call(dir, &python.roots, relay_port);
    let outcome = run_page_call_spawn(
        &policy,
        env,
        dir,
        &python.path.to_string_lossy(),
        &[
            "-c".to_string(),
            PROCESS_TABLE_SNIPPET.to_string(),
            sibling.id().to_string(),
            marker,
        ],
    );
    let _ = sibling.kill();
    let _ = sibling.wait();
    match outcome {
        Ok(output) => {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let tokens: Vec<&str> = stdout.split_whitespace().collect();
            if !tokens.contains(&"PROC") {
                crossing.positive_control_passed = false;
                crossing.detail = format!(
                    "the process-table probe did not run under the page-call policy, so the boundary is unproven: {}",
                    first_words(&output.stderr)
                );
            } else if tokens.contains(&"env") || tokens.contains(&"sig") {
                crossing.crossing_permitted = true;
                crossing.detail = format!(
                    "the page-call policy let a batch {} another same-uid process (exposing its relay capability or letting it be signalled)",
                    if tokens.contains(&"env") {
                        "read the environment of"
                    } else {
                        "signal"
                    }
                );
            }
            crossing
        }
        Err(error) => {
            crossing.positive_control_passed = false;
            crossing.detail = format!(
                "the process-table probe could not spawn under the page-call policy: {error}"
            );
            crossing
        }
    }
}

#[cfg(not(unix))]
fn measure_page_call() -> PageCallMeasurement {
    let reason = format!(
        "no page-call policy exists on {}: no sandbox primitive confines its reads",
        std::env::consts::OS
    );
    PageCallMeasurement {
        filesystem: Err(reason.clone()),
        egress: Err(reason),
        programs: Vec::new(),
    }
}

/// A page-call edge as this machine can currently report it.
pub fn observe_page_call(
    edge: EdgeId,
    build_id: &str,
    boot: &str,
    platform: Option<RemotePlatform>,
) -> EdgeDisposition {
    let Some(platform) = platform else {
        return EdgeDisposition::Unmeasured {
            reason: "this machine is not one of the published executor platforms, so its measurements have no closed platform identity to bind to".into(),
        };
    };
    let Some(invalidation_fingerprint) = fingerprint(edge, build_id, boot) else {
        return EdgeDisposition::Unmeasured {
            reason: "this machine cannot currently read every input that could widen the page-call policy, so no positive disposition is constructible".into(),
        };
    };
    let measurement = page_call_measurement();
    let (crossing, effect) = match edge {
        EdgeId::PageCallFilesystemRead => (&measurement.filesystem, EffectClass::Read),
        EdgeId::PageCallEgress => (&measurement.egress, EffectClass::Invoke),
        other => {
            return EdgeDisposition::Unmeasured {
                reason: format!("{other:?} is not a page-call edge"),
            }
        }
    };
    let crossing = match crossing {
        Ok(crossing) => crossing.clone(),
        Err(reason) => {
            return EdgeDisposition::Unmeasured {
                reason: reason.clone(),
            }
        }
    };
    let definition = edge.definition();
    ProbeWitness {
        edge,
        effect,
        positive_control_passed: crossing.positive_control_passed,
        crossing_permitted: crossing.crossing_permitted,
        detail: crossing.detail,
    }
    .into_disposition(PositiveScope {
        executor_build_id: build_id.to_string(),
        boot_generation: boot.to_string(),
        platform,
        process_class: definition.key.process_class,
        carrier: definition.key.carrier,
        policy_mode: definition.key.policy_mode,
        probe_version: ProbeVersion::V1,
        claim_revision: definition.claim_revision,
        invalidation_fingerprint,
    })
}

/// The batch-spawn filesystem edge as this machine can currently report it.
///
/// Every failure path yields `Unmeasured` naming its cause rather than a weaker
/// positive: a machine that could not probe has not established that it is
/// unconfined either.
pub fn observe_batch_spawn_filesystem(
    build_id: &str,
    boot: &str,
    platform: Option<RemotePlatform>,
    mode: PolicyMode,
) -> EdgeDisposition {
    let edge = batch_filesystem_edge(mode);
    let Some(platform) = platform else {
        return EdgeDisposition::Unmeasured {
            reason: "this machine is not one of the published executor platforms, so its measurements have no closed platform identity to bind to".into(),
        };
    };
    let Some(invalidation_fingerprint) = fingerprint(edge, build_id, boot) else {
        return EdgeDisposition::Unmeasured {
            reason: "this machine cannot currently read every input that could widen the batch-spawn filesystem seam, so no positive disposition is constructible".into(),
        };
    };
    let witness = match probe_batch_spawn_filesystem(mode) {
        Ok(witness) => witness,
        Err(reason) => return EdgeDisposition::Unmeasured { reason },
    };
    let definition = edge.definition();
    witness.into_disposition(PositiveScope {
        executor_build_id: build_id.to_string(),
        boot_generation: boot.to_string(),
        platform,
        process_class: definition.key.process_class,
        carrier: definition.key.carrier,
        policy_mode: definition.key.policy_mode,
        probe_version: ProbeVersion::V1,
        claim_revision: definition.claim_revision,
        invalidation_fingerprint,
    })
}

/// This machine's whole containment matrix, measured once per process.
///
/// One assembly point serves both the advertisement and the per-execution
/// record, because the two must never disagree about what this machine is: a
/// capability page saying one thing while an execution reports another is the
/// drift that makes containment evidence unreadable. It is cached because a
/// probe spawns a process, and because re-probing per execution would report a
/// different measurement under the same build and boot, which the currency
/// rules would then have to reject.
///
/// Every edge appears. An edge with no probe is `Unmeasured` naming that fact,
/// never absent — absence from the matrix would be indistinguishable from
/// safety, and `Unmeasured` is what refuses.
pub fn observations() -> ContainmentObservations {
    observations_from(&LiveInputs)
}

/// The immutable half: what the probes actually saw, measured once per process.
///
/// Cached because a probe spawns a process and because re-probing would produce
/// a second measurement under the same build and boot, which the currency rules
/// would then have to arbitrate between. A measurement is a fact about an
/// instant; caching it is what makes it a fact rather than a sample.
fn probe_evidence(
    build_id: &str,
    boot: &str,
    platform: Option<RemotePlatform>,
) -> &'static Vec<cairn_common::executor_protocol::EdgeObservation> {
    static EVIDENCE: std::sync::OnceLock<Vec<cairn_common::executor_protocol::EdgeObservation>> =
        std::sync::OnceLock::new();
    EVIDENCE.get_or_init(|| {
        let observed_at_unix_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_millis() as u64)
            .unwrap_or_default();
        cairn_common::executor_protocol::CONTAINMENT_EDGE_IDS
            .into_iter()
            .map(|edge| {
                let disposition = match edge {
                    EdgeId::BatchSpawnFilesystemConfined => observe_batch_spawn_filesystem(
                        build_id,
                        boot,
                        platform,
                        PolicyMode::Confined,
                    ),
                    EdgeId::BatchSpawnFilesystemReadOnlyCheckout => observe_batch_spawn_filesystem(
                        build_id,
                        boot,
                        platform,
                        PolicyMode::ReadOnlyCheckout,
                    ),
                    EdgeId::BatchSpawnFilesystemUnconfined => observe_batch_spawn_filesystem(
                        build_id,
                        boot,
                        platform,
                        PolicyMode::Unconfined,
                    ),
                    EdgeId::OperatorCredentialRead => {
                        observe_operator_credential_read(build_id, boot, platform)
                    }
                    EdgeId::PageCallFilesystemRead | EdgeId::PageCallEgress => {
                        observe_page_call(edge, build_id, boot, platform)
                    }
                    socket if host_socket_class(socket).is_some() => {
                        let (class, mode) = host_socket_class(socket).expect("guarded");
                        observe_host_socket(build_id, boot, platform, class, mode)
                    }
                    other => EdgeDisposition::Unmeasured {
                        reason: format!(
                            "no probe attempts the {other:?} seam yet; see docs/security/executor-containment.md for the boundary that owns it"
                        ),
                    },
                };
                cairn_common::executor_protocol::EdgeObservation {
                    edge,
                    disposition,
                    observed_at_unix_ms,
                }
            })
            .collect()
    })
}

/// The matrix as it stands right now.
///
/// Deliberately **not** cached, and this is the whole mechanism rather than a
/// performance note. The measured half is frozen at probe time; the current
/// fingerprints are re-read from the live host on every call. Caching both
/// together would compare a value against itself, so a sandbox profile, mount
/// table, helper, or policy could change after boot and the stale evidence
/// would stay eligible forever — the mechanism would look present and do
/// nothing. Every assembly point must therefore call this, not memoize it.
pub fn observations_from(provider: &dyn InvalidationInputs) -> ContainmentObservations {
    let build_id = cairn_common::build_identity::current_executable_build_id()
        .unwrap_or_else(|_| "unknown".to_string());
    let boot = cairn_common::build_identity::process_boot_generation();
    let platform = RemotePlatform::current();
    ContainmentObservations {
        current_fingerprints: current_fingerprints_from(provider, &build_id, &boot),
        observations: probe_evidence(&build_id, &boot, platform).clone(),
        executor_build_id: build_id,
        boot_generation: boot,
        platform,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope() -> PositiveScope {
        let definition = EdgeId::BatchSpawnFilesystemConfined.definition();
        PositiveScope {
            policy_mode: definition.key.policy_mode,
            executor_build_id: "build".into(),
            boot_generation: "boot".into(),
            platform: RemotePlatform::DarwinArm64,
            process_class: definition.key.process_class,
            carrier: definition.key.carrier,
            probe_version: ProbeVersion::V1,
            claim_revision: definition.claim_revision,
            invalidation_fingerprint: Fingerprint {
                algorithm: FingerprintAlgorithm::Sha256,
                digest: "digest".into(),
                inputs: definition.invalidation_inputs.to_vec(),
            },
        }
    }

    /// The property the whole evidence model rests on: a probe that could not
    /// perform the crossing it was supposed to be allowed has measured a broken
    /// seam, and a broken seam must never be reported as a strong one.
    #[test]
    fn a_failed_positive_control_can_never_yield_a_positive_disposition() {
        for crossing_permitted in [false, true] {
            let witness = ProbeWitness {
                edge: EdgeId::BatchSpawnFilesystemConfined,
                effect: EffectClass::Write,
                positive_control_passed: false,
                crossing_permitted,
                detail: "the in-bounds control write did not appear".into(),
            };
            let disposition = witness.into_disposition(scope());
            assert!(
                matches!(disposition, EdgeDisposition::Unmeasured { .. }),
                "a failed control produced {disposition:?}"
            );
        }
    }

    /// The operator credential edge says what this platform can actually do:
    /// refused where Seatbelt runs, measured as readable where nothing can
    /// refuse it, and never claimed refused on a seam the probe could not use.
    #[cfg(unix)]
    #[test]
    fn the_operator_credential_probe_reports_what_this_platform_enforces() {
        let witness = match probe_operator_credential_read() {
            Ok(witness) => witness,
            // A fenced test process cannot plant a home in `$HOME` at all. The
            // executor that measures this in production is never fenced.
            Err(reason) if reason.starts_with("create the probe home") => {
                eprintln!("skipping: this process cannot write its own home: {reason}");
                return;
            }
            Err(reason) => panic!("the probe runs on unix: {reason}"),
        };
        if !witness.positive_control_passed {
            // Only a macOS process already inside a sandbox, which cannot apply
            // another, fails the control; elsewhere the seam always works.
            if !cfg!(target_os = "macos") {
                panic!("the seam's control failed off macOS: {}", witness.detail);
            }
            assert!(
                crate::current_process_is_confined(),
                "an unconfined process must be able to exercise the seam: {}",
                witness.detail
            );
            return;
        }
        assert_eq!(
            witness.crossing_permitted,
            !cfg!(target_os = "macos"),
            "{}",
            witness.detail
        );
        let disposition = witness.into_disposition(scope());
        if cfg!(target_os = "macos") {
            assert!(matches!(disposition, EdgeDisposition::Refused { .. }));
        } else {
            assert!(
                matches!(disposition, EdgeDisposition::Ambient { .. }),
                "a platform that cannot enforce the boundary must say so: {disposition:?}"
            );
        }
    }

    /// Live: through the real page-call wrapping, a planted secret, database,
    /// home file and sibling stay unreadable while the batch's own file reads,
    /// `sh` runs, and wherever the probe could try a connection the second
    /// loopback port is refused on macOS and on Linux from Landlock ABI 4.
    /// A confined macOS process cannot apply the page-call profile, so a test
    /// lane inside a Seatbelt fence cannot exercise the probe; the executor
    /// that measures it in production never is. Keyed to one reason so the
    /// skip census resolves it (the test passes its own name as a literal).
    #[cfg(target_os = "macos")]
    fn skip_page_call_probe_when_confined(test: &str) -> bool {
        if crate::current_process_is_confined() {
            eprintln!("skipping {test}: a confined process cannot apply the page-call profile");
            cairn_common::skip_ledger::record_skip(test, "nested-sandbox-exec");
            return true;
        }
        false
    }

    /// macOS answers `KERN_PROCARGS2` for any same-uid process under every
    /// Seatbelt profile, `(deny default)` included (cairn/5787 comment 2), so
    /// the process-table crossing leaks on every Mac and no profile can close
    /// it. Skipped only when that read is the measured verdict, so any other
    /// crossing still fails the test; the production probe is unchanged and
    /// keeps refusing page calls on these machines.
    #[cfg(target_os = "macos")]
    fn skip_page_call_probe_where_procargs_is_ungated(
        test: &str,
        measured: &PageCallMeasurement,
    ) -> bool {
        if matches!(&measured.filesystem, Ok(crossing) if crossing.crossing_permitted
            && crossing.detail.contains("read the environment of another same-uid process"))
        {
            eprintln!("skipping {test}: Seatbelt does not gate KERN_PROCARGS2 on this macOS");
            cairn_common::skip_ledger::record_skip(test, "procargs-ungated");
            return true;
        }
        false
    }

    /// A fenced test process cannot plant a probe home in `$HOME`. The
    /// production probe runs unconfined, so this is a test-lane gap only.
    #[cfg(unix)]
    fn skip_page_call_probe_when_home_unplantable(
        test: &str,
        measured: &PageCallMeasurement,
    ) -> bool {
        if matches!(&measured.filesystem, Err(reason) if reason.starts_with("plant the page-call probe files"))
        {
            eprintln!("skipping {test}: a fenced process cannot plant a probe home");
            cairn_common::skip_ledger::record_skip(test, "worktree-fence");
            return true;
        }
        false
    }

    #[cfg(unix)]
    #[test]
    fn the_page_call_probe_refuses_every_planted_crossing_live() {
        #[cfg(target_os = "macos")]
        if skip_page_call_probe_when_confined(
            "the_page_call_probe_refuses_every_planted_crossing_live",
        ) {
            return;
        }
        let measured = measure_page_call();
        // A page-call policy hard-requires Landlock ABI 9 (for `ResolveUnix`),
        // so a kernel below it cannot build the ruleset and the machine is
        // excluded. That exclusion is the correct outcome, asserted here as a
        // real fact (the policy could not be applied) rather than skipped; the
        // policy's shape is covered by the unit tests. Every current Linux
        // host is ABI 8, so this is the branch they take today.
        #[cfg(target_os = "linux")]
        if crate::linux::kernel_abi().is_none_or(|abi| abi < crate::linux::RESOLVE_UNIX_ABI) {
            assert!(
                measured.filesystem.is_err(),
                "a kernel below Landlock ABI 9 must not be able to apply the page-call policy"
            );
            return;
        }
        if skip_page_call_probe_when_home_unplantable(
            "the_page_call_probe_refuses_every_planted_crossing_live",
            &measured,
        ) {
            return;
        }
        #[cfg(target_os = "macos")]
        if skip_page_call_probe_where_procargs_is_ungated(
            "the_page_call_probe_refuses_every_planted_crossing_live",
            &measured,
        ) {
            return;
        }
        let filesystem = match &measured.filesystem {
            Ok(crossing) => crossing.clone(),
            Err(reason) => panic!("the page-call probe runs on unix: {reason}"),
        };
        assert!(filesystem.positive_control_passed, "{}", filesystem.detail);
        assert!(!filesystem.crossing_permitted, "{}", filesystem.detail);
        let sh = measured
            .programs
            .iter()
            .find(|program| program.program == "sh")
            .expect("sh is probed");
        assert!(
            sh.passed,
            "sh must run under the page-call policy: {}",
            sh.detail
        );
        if let Ok(egress) = &measured.egress {
            if egress.positive_control_passed {
                #[cfg(target_os = "macos")]
                let confined = true;
                #[cfg(target_os = "linux")]
                let confined =
                    crate::linux::kernel_abi().is_some_and(|abi| abi >= crate::linux::NET_TCP_ABI);
                assert_eq!(!egress.crossing_permitted, confined, "{}", egress.detail);
            }
        }
    }

    /// A live crossing, not a configuration read. Whatever this machine turns
    /// out to be, the report has to be internally honest: a positive
    /// disposition only ever accompanies a control that passed.
    #[cfg(unix)]
    #[test]
    fn the_batch_seam_probe_attempts_a_real_crossing() {
        let witness = probe_batch_spawn_filesystem(PolicyMode::Confined)
            .expect("the batch seam probe runs on unix");
        let permitted = witness.crossing_permitted;
        match witness.into_disposition(scope()) {
            EdgeDisposition::Refused { observed, .. } => {
                assert!(observed.positive_control_passed);
                assert!(!permitted);
                assert!(observed.detail.contains("refused"), "{}", observed.detail);
            }
            EdgeDisposition::Ambient { observed, .. } => {
                assert!(observed.positive_control_passed);
                assert!(permitted);
                assert!(
                    observed.detail.contains("no filesystem confinement"),
                    "{}",
                    observed.detail
                );
            }
            EdgeDisposition::Unmeasured { reason } => {
                assert!(reason.contains("control crossing"), "{reason}");
            }
            EdgeDisposition::Constrained { .. } => {
                panic!("the filesystem seam declares no constrained envelope")
            }
        }
    }

    /// Fingerprint completeness is earned per edge. The filesystem seam can read
    /// every input it declares; the edges whose inputs have no closed readable
    /// form must fail to fingerprint, which is what keeps them unmeasured.
    #[test]
    fn a_fingerprint_exists_only_where_every_declared_input_is_readable() {
        assert!(fingerprint(EdgeId::BatchSpawnFilesystemConfined, "build", "boot").is_some());
        assert!(fingerprint(EdgeId::BatchSpawnEgress, "build", "boot").is_none());
        assert!(fingerprint(EdgeId::BuildServiceStateDir, "build", "boot").is_none());
        assert!(
            fingerprint(EdgeId::BatchSpawnEnvironmentInheritance, "build", "boot").is_none(),
            "the runner's environment assembly is not readable from the executor"
        );

        let observation =
            observe_batch_spawn_filesystem("build", "boot", None, PolicyMode::Confined);
        assert!(matches!(observation, EdgeDisposition::Unmeasured { .. }));
    }

    /// A host whose sandbox profile has moved since the probe ran.
    struct ShiftedProfile;

    impl InvalidationInputs for ShiftedProfile {
        fn value(&self, input: InvalidationInput, build_id: &str, boot: &str) -> Option<String> {
            match input {
                InvalidationInput::SandboxProfile => {
                    Some("a profile that is not the one the probe ran under".to_string())
                }
                other => LiveInputs.value(other, build_id, boot),
            }
        }
    }

    /// A host that can no longer read one of its own declared inputs.
    struct UnreadableProfile;

    impl InvalidationInputs for UnreadableProfile {
        fn value(&self, input: InvalidationInput, build_id: &str, boot: &str) -> Option<String> {
            match input {
                InvalidationInput::SandboxProfile => None,
                other => LiveInputs.value(other, build_id, boot),
            }
        }
    }

    /// Invalidation has to survive the production assembly path, not just the
    /// comparator.
    ///
    /// This is the regression for a real bug. The measured fingerprint and the
    /// current one were cached together in a single process-wide value, so
    /// `satisfies` compared a value against itself: no host change could ever
    /// invalidate anything, and the mechanism was fully present while doing
    /// nothing. A test that edits an already-built matrix cannot catch that,
    /// because it never exercises the assembly. This one moves a declared input
    /// underneath the same function production calls and requires the verdict
    /// to change.
    #[test]
    fn moving_a_declared_input_invalidates_evidence_through_the_production_assembly() {
        use cairn_common::executor_protocol::{
            EdgeRequirement, MaximumDisposition, WorkloadEnvelope,
        };

        let edge = EdgeId::BatchSpawnFilesystemConfined;
        let wanted = WorkloadEnvelope {
            process_class: edge.definition().key.process_class,
            edges: vec![EdgeRequirement {
                edge,
                maximum_disposition: MaximumDisposition::Ambient,
            }],
        };

        // Both matrices come from the production assembly. Only the host
        // underneath differs, and the measured half is the same cached probe.
        let live = observations_from(&LiveInputs);
        let Some(EdgeDisposition::Refused { .. } | EdgeDisposition::Ambient { .. }) = live
            .observations
            .iter()
            .find(|observation| observation.edge == edge)
            .map(|observation| observation.disposition.clone())
        else {
            // This machine could not probe the seam at all, so there is no
            // positive evidence for a host change to invalidate.
            return;
        };
        assert!(
            live.satisfies(&wanted).is_ok(),
            "current evidence should satisfy a request it is current for"
        );

        let moved = observations_from(&ShiftedProfile);
        assert_eq!(
            moved.satisfies(&wanted).unwrap_err().deficiency,
            cairn_common::executor_protocol::ContainmentDeficiency::FingerprintChanged,
            "a profile change after the probe must invalidate the probe"
        );

        let unreadable = observations_from(&UnreadableProfile);
        assert_eq!(
            unreadable.satisfies(&wanted).unwrap_err().deficiency,
            cairn_common::executor_protocol::ContainmentDeficiency::FingerprintUncomputable,
            "an input that can no longer be read must not leave stale evidence eligible"
        );
    }

    /// Confined evidence must not place an unconfined workload. The two are
    /// different edges precisely so that no comparison between them can pass.
    #[test]
    fn confined_evidence_cannot_place_an_unconfined_workload() {
        use cairn_common::executor_protocol::{
            EdgeRequirement, MaximumDisposition, WorkloadEnvelope,
        };

        // The three modes are genuinely different edges, each measured through
        // the policy that mode actually builds.
        for (edge, mode) in [
            (EdgeId::BatchSpawnFilesystemConfined, PolicyMode::Confined),
            (
                EdgeId::BatchSpawnFilesystemReadOnlyCheckout,
                PolicyMode::ReadOnlyCheckout,
            ),
            (
                EdgeId::BatchSpawnFilesystemUnconfined,
                PolicyMode::Unconfined,
            ),
        ] {
            assert_eq!(edge.definition().key.policy_mode, mode);
        }

        // Take the confined measurement and try to pass it off as evidence
        // about another mode. Before the split that was representable, because
        // the seam and carrier matched and nothing named the policy. Now the
        // scope carries the mode it measured and the edge carries the mode it
        // is about, so the mismatch is rejected rather than compared.
        let mut observations = observations_from(&LiveInputs);
        let confined = observations
            .observations
            .iter()
            .find(|observation| observation.edge == EdgeId::BatchSpawnFilesystemConfined)
            .expect("the confined seam has a row")
            .clone();
        if matches!(confined.disposition, EdgeDisposition::Unmeasured { .. }) {
            return;
        }
        let target = EdgeId::BatchSpawnFilesystemUnconfined;
        for observation in observations.observations.iter_mut() {
            if observation.edge == target {
                observation.disposition = confined.disposition.clone();
            }
        }

        let wanted = WorkloadEnvelope {
            process_class: cairn_common::executor_protocol::ProcessClass::BatchSpawn,
            edges: vec![EdgeRequirement {
                edge: target,
                maximum_disposition: MaximumDisposition::Ambient,
            }],
        };
        assert_eq!(
            observations.satisfies(&wanted).unwrap_err().deficiency,
            cairn_common::executor_protocol::ContainmentDeficiency::NotObserved,
            "a confined measurement must not answer for the unconfined seam"
        );
        assert!(
            observations.validate().is_err(),
            "and the wire must refuse to carry it in the first place"
        );
    }

    /// A fingerprint has to move when the enforcement moves, or it certifies
    /// yesterday's profile forever.
    #[test]
    fn a_fingerprint_moves_when_a_declared_input_moves() {
        let edge = EdgeId::BatchSpawnFilesystemConfined;
        let first = fingerprint(edge, "build-a", "boot").unwrap();
        let rebuilt = fingerprint(edge, "build-b", "boot").unwrap();
        let rebooted = fingerprint(edge, "build-a", "boot-2").unwrap();
        assert_ne!(first.digest, rebuilt.digest);
        assert_ne!(first.digest, rebooted.digest);
        assert_eq!(first.inputs, edge.definition().invalidation_inputs.to_vec());
    }
}
