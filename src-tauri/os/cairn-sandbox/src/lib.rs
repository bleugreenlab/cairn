//! OS-level filesystem sandbox applied at the process-spawn seam.
//!
//! The `run` verb's commands execute in a Cairn-owned host process: ordinary
//! workspace commands are spawned by the runner/server, while build-slot batches
//! are spawned by the supervised `cairn-exec`. Neither path inherits the
//! agent CLI's own sandbox. A worktree boundary therefore has to be enforced by
//! wrapping each command at its actual spawn seam. This module builds
//! a per-spawn [`SandboxPolicy`] and applies it to the spawned `Command`:
//!
//! - **macOS**: rewrite argv to `sandbox-exec -p <SBPL profile> <program> …`.
//! - **Linux**: install a `landlock` ruleset in a `pre_exec` hook (child-side,
//!   before `exec`).
//! - **Windows / other**: no kernel primitive — runs unconfined (documented).
//!
//! The policy confines **writes** to `{worktree, tmp, granted paths}`, allows
//! **reads broadly** (the toolchain reads `~/.cargo`, `~/.npm`, `~/.gitconfig`,
//! `/usr/lib`, …), and **hard-denies reads** of a narrow sensitive denylist of
//! external credential stores (see [`default_deny_read`], which deliberately
//! does *not* deny Cairn's own `~/.cairn[-dev]`). See `docs/worktree-fence.md`.
//!
//! Separately from the fence, every agent spawn carries the operator credential
//! boundary ([`operator_credential`]), including a spawn the fence leaves
//! unconfined: no agent process may read a Cairn home's `operator_auth_secret`.
//! Each seam states its [`Confinement`] so that choice is made explicitly.

mod approved_crossing;
pub mod containment;
#[cfg(all(test, target_os = "linux"))]
mod host_socket_tests;
mod isolated;
#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
pub mod macos;
mod nested;
pub mod operator_credential;
pub mod page_call;
pub mod scratch_refusal;
mod setuid_refusal;
mod write_refusal;

pub use approved_crossing::{
    bounded_rerun, cairn_state_reached_by, crossing_access, NO_PATH_RECOVERED, NO_POLICY_TO_WIDEN,
};
pub use isolated::{IsolatedChild, IsolatedLimits};
pub use operator_credential::{
    credential_denial, detect_credential_floor_denial, Confinement, MacosProfile,
    OperatorCredentialFloor, OPERATOR_CREDENTIAL_REFUSAL,
};
pub use setuid_refusal::setuid_refusal_note;
pub use write_refusal::out_of_bounds_write_note;

use std::path::{Path, PathBuf};
use std::time::SystemTime;

/// Whether this process is already running under an inherited kernel sandbox.
///
/// A child sandbox can narrow an inherited policy but cannot widen it. Long-lived
/// machine services must therefore refuse to launch from a confined supervisor.
#[cfg(target_os = "macos")]
pub fn current_process_is_confined() -> bool {
    macos::current_process_is_confined()
}

#[cfg(not(target_os = "macos"))]
pub fn current_process_is_confined() -> bool {
    false
}

/// Evidence that a process can launch a nested macOS Seatbelt profile and
/// perform a write which that profile explicitly permits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NestedSandboxWriteProbe {
    pub available: bool,
    /// Whether the refusal was `sandbox_apply` itself (exit 71): the kernel
    /// declining to apply a profile inside one, as opposed to a write the
    /// nested profile should have permitted failing.
    pub apply_refused: bool,
    pub detail: String,
}

/// Whether THIS process can apply a nested profile and write inside it: the
/// question a test asks of the environment it is already running in.
///
/// An executor deciding whether its check lanes can do so asks a different
/// question, because it spawns those lanes inside a profile of its own; see
/// [`sandbox_capability_evidence`].
///
/// This deliberately does not consult `CAIRN_SANDBOXED`: that marker records a
/// Cairn policy decision, while an inherited Seatbelt profile is kernel state.
#[cfg(target_os = "macos")]
pub fn probe_nested_sandbox_write() -> NestedSandboxWriteProbe {
    static PROBE: std::sync::OnceLock<NestedSandboxWriteProbe> = std::sync::OnceLock::new();
    PROBE
        .get_or_init(|| macos::probe_nested_sandbox_write_under(&Confinement::Unconfined))
        .clone()
}

#[cfg(not(target_os = "macos"))]
pub fn probe_nested_sandbox_write() -> NestedSandboxWriteProbe {
    nested_probe_not_applicable()
}

#[cfg(not(target_os = "macos"))]
fn nested_probe_not_applicable() -> NestedSandboxWriteProbe {
    NestedSandboxWriteProbe {
        available: false,
        apply_refused: false,
        detail: format!("not applicable on {}", std::env::consts::OS),
    }
}

/// A per-spawn filesystem confinement profile.
///
/// Built fresh for each spawn from the current worktree and the session's
/// granted crossings, so a user-approved crossing path widens the very next
/// spawn's writable set (and drops out of the read denylist).
#[derive(Debug, Clone)]
pub struct SandboxPolicy {
    /// The agent's worktree — writable (and always readable).
    pub worktree: PathBuf,
    /// Additional writable subpaths: temp dirs, toolchain caches, and
    /// session-granted crossings. Also re-allowed for reads.
    pub writable_extra: Vec<PathBuf>,
    /// Subpaths whose reads are denied — the narrow set of obviously-sensitive
    /// secrets (external credential stores). For an `Ask` agent a denied read
    /// still surfaces as an approvable crossing; only `Deny` makes it final.
    pub deny_read: Vec<PathBuf>,
    /// Subpaths re-allowed for reads and never for writes: approved read
    /// crossings ([`SandboxPolicy::grant_crossing`]). Linux does not enforce a
    /// broad policy's read denylist, so a read crossing never arises there.
    pub readable_extra: Vec<PathBuf>,
    /// Additional writable scopes expressed as anchored regexes rather than
    /// concrete subpaths. These let a long-lived **service** sandbox (see
    /// [`SandboxPolicy::for_service`]) grant writes across many sibling paths
    /// that do not yet exist — e.g. every worktree's `target/` tree — which a
    /// fixed `subpath` list cannot express. Empty for ordinary `for_run`
    /// policies. Enforced on macOS via SBPL `(regex ...)`; the Linux landlock
    /// path is allowlist-by-concrete-fd and does not yet translate these (the
    /// service sandbox is macOS-first, mirroring the read-denylist gap).
    pub writable_regex: Vec<String>,
    /// Whether the `worktree` root is writable. True for an ordinary worktree run
    /// and for a service. False for a **read-only-checkout** policy (see
    /// [`SandboxPolicy::for_readonly_checkout`]), where the project's live
    /// checkout must stay readable but every write into it is kernel-denied. The
    /// worktree is always in [`readable_paths`](Self::readable_paths) regardless;
    /// this flag only gates the write-allow.
    pub worktree_writable: bool,
    /// Directories beneath which a fenced spawn may reach a pathname unix
    /// socket served from outside its own sandbox: the cell's own scratch, so
    /// a later batch of a job can reconnect to a server an earlier batch
    /// started there. A writable worktree joins this set implicitly (see
    /// [`socket_resolvable_paths`](Self::socket_resolvable_paths)), and a
    /// shared root never does (see
    /// [`allow_job_sockets_in`](Self::allow_job_sockets_in)). Enforced by
    /// Linux landlock from ABI 9; macOS does not yet scope sockets.
    pub socket_resolvable: Vec<PathBuf>,
    /// The executor's lineage callback socket, which every fence may reach,
    /// whatever its network scope: a fenced command calls Cairn there with no
    /// credential, so the socket is its one door to Cairn. On macOS a
    /// relay-only profile allows exactly this path; on Linux its directory
    /// joins the socket-resolvable set (`LANDLOCK_ACCESS_FS_RESOLVE_UNIX`).
    pub callback_socket: Option<PathBuf>,
    /// The marker this spawn's denials carry into the macOS violation log.
    pub violation_tag: ViolationTag,
    /// How far reads and the network reach. Broad and ambient for every fence
    /// but the page-call policy ([`SandboxPolicy::for_page_call`]), which
    /// narrows both.
    pub reach: PolicyReach,
}

/// What a fenced spawn may read, and which sockets it may reach, beyond the
/// writes [`SandboxPolicy`] confines.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PolicyReach {
    pub reads: ReadScope,
    pub network: NetworkScope,
}

/// Which reads a fenced spawn keeps.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum ReadScope {
    /// Everything except [`SandboxPolicy::deny_read`]: the toolchain reads
    /// widely, so an ordinary fence confines writes and denies only a narrow
    /// set of secrets.
    #[default]
    Broad,
    /// Nothing but these roots, the writable set, and `/dev`. On macOS the
    /// metadata of every path stays visible (the loader resolves paths by
    /// stat), and the root directory node itself is readable (bun opens `/` at
    /// startup); nothing beneath an unlisted directory is.
    Allowlist { roots: Vec<PathBuf> },
}

/// Which sockets a fenced spawn may reach.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum NetworkScope {
    /// Whatever the host allows.
    #[default]
    Ambient,
    /// A TCP connection to this loopback port and nothing else: the executor's
    /// relay, the one door to Cairn a page-call batch holds a capability for.
    /// On macOS every other socket and every Mach service lookup is refused;
    /// on Linux, TCP connect and bind are confined from Landlock ABI 4 and the
    /// rest of the network is not.
    RelayOnly { port: u16 },
}

/// A marker the macOS profile attaches to each fence deny rule with SBPL's
/// `(with message ...)`, so every violation the kernel logs for this spawn's
/// process tree carries it.
///
/// The kernel logs a Seatbelt violation under the process that made the denied
/// call, and that is usually a descendant of the spawned process (the tool an
/// inline script or `sh -c` started), never the spawned pid itself. The profile
/// is inherited across fork and exec, so the tag reaches every descendant
/// without tracking a process tree that has mostly exited by the time anyone
/// looks. A fresh tag per policy keeps a concurrent spawn's denials apart.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViolationTag(String);

impl ViolationTag {
    /// What every tag begins with, which is what the violation stream filters
    /// the log by.
    pub(crate) const PREFIX: &'static str = "cairn-fence-";

    pub fn fresh() -> Self {
        Self(format!("{}{}", Self::PREFIX, uuid::Uuid::new_v4().simple()))
    }

    /// A tag that never changes, for a profile whose text is a fingerprint
    /// rather than one that confines a spawn.
    fn canonical() -> Self {
        Self(format!("{}canonical", Self::PREFIX))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The argv of the process-wide `log stream` that reads tagged Seatbelt
/// violations on macOS, program first.
///
/// The single source of that command. The macOS fence spawns exactly this,
/// and [`violation_stream_sudoers_command`] renders the one sudoers rule that
/// lets a non-admin executor account run it (`log stream` refuses anyone not
/// in `admin`). The render is committed as
/// `scripts/lib/fence-violation-stream.sudoers-command`, which the Mac setup
/// script installs and the runner-isolation gate allows; a test pins that file
/// to this function, so neither can drift from what is spawned.
pub fn violation_stream_argv() -> Vec<String> {
    vec![
        "/usr/bin/log".into(),
        "stream".into(),
        "--style".into(),
        "ndjson".into(),
        "--predicate".into(),
        format!("eventMessage CONTAINS \"{}\"", ViolationTag::PREFIX),
    ]
}

/// What an account outside `admin` runs [`violation_stream_argv`] under: sudo,
/// told to refuse rather than prompt when no rule permits the command.
pub const VIOLATION_STREAM_SUDO: [&str; 2] = ["/usr/bin/sudo", "-n"];

/// The argv the macOS fence spawns for its violation stream, given how this
/// account reaches `log stream`: [`violation_stream_argv`] itself, behind
/// [`VIOLATION_STREAM_SUDO`] for an account outside `admin`. Nothing else is
/// ever added, so what sudo is asked to run is what the sudoers rule names.
pub fn violation_stream_spawn_argv(
    access: cairn_common::executor_protocol::ViolationStreamAccess,
) -> Vec<String> {
    use cairn_common::executor_protocol::ViolationStreamAccess;
    let argv = violation_stream_argv();
    match access {
        ViolationStreamAccess::Admin => argv,
        ViolationStreamAccess::SudoRule => VIOLATION_STREAM_SUDO
            .iter()
            .map(|arg| arg.to_string())
            .chain(argv)
            .collect(),
    }
}

/// This machine's sandbox capability evidence: whether a check lane can apply
/// a nested Seatbelt profile, and whether a fenced denial is recovered by path.
///
/// One assembly point serves the advertisement, every liveness pass, and the
/// per-execution record, so none of them can disagree about the machine. Both
/// halves re-measure when what they depend on may have moved, so a pass after
/// a host change carries the new answer without a daemon restart.
pub fn sandbox_capability_evidence() -> cairn_common::executor_protocol::SandboxCapabilityEvidence {
    let mut evidence = nested::check_lane_nested_sandbox_evidence();
    evidence.violation_recovery = violation_recovery();
    evidence
}

/// Keeps this process's sandbox violation stream running while held.
///
/// A fenced spawn whose denials will be read back ([`detect_denial`],
/// [`IsolatedChild::denials_since`]) takes one before it spawns and keeps it
/// until it has read them, because the stream only sees denials made while it
/// runs. The stream is shared by every holder and stops a minute after the
/// last is dropped, so a process with nothing fenced in flight runs no `log`
/// process. Inert off macOS, where no denied path is recovered out of band.
#[must_use = "the violation stream stops once the last watch is dropped"]
pub struct ViolationWatch {
    #[cfg(target_os = "macos")]
    _lease: macos::ViolationLease,
}

/// Start or join this process's violation stream; see [`ViolationWatch`].
/// Blocks for up to two seconds when it starts one.
pub fn watch_violations() -> ViolationWatch {
    ViolationWatch {
        #[cfg(target_os = "macos")]
        _lease: macos::watch_violations(),
    }
}

#[cfg(target_os = "macos")]
fn violation_recovery() -> Option<cairn_common::executor_protocol::ViolationRecoveryEvidence> {
    Some(macos::violation_recovery())
}

/// Only macOS recovers a denied path out of band; elsewhere a denial stays
/// command-scoped, so there is nothing to report.
#[cfg(not(target_os = "macos"))]
fn violation_recovery() -> Option<cairn_common::executor_protocol::ViolationRecoveryEvidence> {
    None
}

/// The sudoers command spec, everything after `NOPASSWD: `, that permits
/// exactly [`violation_stream_argv`] and nothing else.
pub fn violation_stream_sudoers_command() -> String {
    sudoers_command(&violation_stream_argv())
}

/// Render argv as a sudoers command spec that matches it byte for byte.
///
/// sudo compares a rule's argument text, as an fnmatch pattern, against the
/// invoked arguments joined by single spaces, and in that text `"` is an
/// ordinary character (sudo 1.9.17 rejects `\"` there, measured on macOS). So
/// the render escapes only what sudoers requires inside arguments
/// (`\ , : =`), and refuses an argument whose text a single-space join would
/// not reproduce, or that would turn into a pattern or a comment.
fn sudoers_command(argv: &[String]) -> String {
    assert!(
        argv.first().is_some_and(|program| program.starts_with('/')),
        "a sudoers command must name its program by absolute path: {argv:?}"
    );
    argv.iter()
        .map(|arg| {
            assert!(
                !arg.is_empty()
                    && !arg.starts_with(' ')
                    && !arg.ends_with(' ')
                    && !arg.contains("  "),
                "sudo joins arguments with single spaces, so {arg:?} cannot be matched exactly"
            );
            assert!(
                !arg.chars().any(|c| matches!(c, '*' | '?' | '[' | ']' | '#')
                    || c.is_control()
                    || (c.is_whitespace() && c != ' ')),
                "{arg:?} would be a pattern, a comment or a line break in sudoers, not literal text"
            );
            let mut rendered = String::with_capacity(arg.len());
            for c in arg.chars() {
                if matches!(c, '\\' | ',' | ':' | '=') {
                    rendered.push('\\');
                }
                rendered.push(c);
            }
            rendered
        })
        .collect::<Vec<_>>()
        .join(" ")
}

pub fn paths_overlap(a: &Path, b: &Path) -> bool {
    // Compare canonical forms so macOS /var -> /private/var and /tmp ->
    // /private/tmp aliases cannot smuggle an ancestor write allowance back over
    // a read-only checkout.
    let a = a.canonicalize().unwrap_or_else(|_| a.to_path_buf());
    let b = b.canonicalize().unwrap_or_else(|_| b.to_path_buf());
    a.starts_with(&b) || b.starts_with(&a)
}

impl SandboxPolicy {
    /// Build a policy for a worktree run.
    ///
    /// `granted` is the set of session-allowed crossing descriptors (resolved
    /// paths for path crossings). Any granted path is added to the writable set,
    /// which is re-allowed for reads after the denylist — so an approved crossing
    /// wins over a denylist entry whose subtree contains it.
    pub fn for_run(worktree: &Path, granted: &[String], deny_read: Vec<PathBuf>) -> Self {
        let granted_paths: Vec<PathBuf> = granted
            .iter()
            .filter(|g| g.starts_with('/'))
            .map(PathBuf::from)
            .collect();

        let mut writable_extra = default_writable_extra();
        writable_extra.extend(granted_paths);

        Self {
            worktree: worktree.to_path_buf(),
            writable_extra,
            deny_read,
            writable_regex: Vec::new(),
            worktree_writable: true,
            socket_resolvable: Vec::new(),
            callback_socket: None,
            readable_extra: Vec::new(),
            violation_tag: ViolationTag::fresh(),
            reach: Default::default(),
        }
    }

    /// Build a policy for a non-worktree run on the project's **live checkout**.
    ///
    /// Like [`for_run`](Self::for_run) but the checkout itself is **read-only**:
    /// it stays in the readable set (the agent reads the project source) but is
    /// dropped from the writable set, so any write into the live checkout is
    /// kernel-denied. Only temp dirs, toolchain caches, and session grants stay
    /// writable. This makes "a non-worktree agent cannot leave a file behind in
    /// the live checkout" true at the kernel, complementing the read-only
    /// dirt-detection warning. See `docs/worktree-fence.md`.
    pub fn for_readonly_checkout(
        checkout: &Path,
        granted: &[String],
        deny_read: Vec<PathBuf>,
    ) -> Self {
        let granted_paths: Vec<PathBuf> = granted
            .iter()
            .filter(|g| g.starts_with('/'))
            .map(PathBuf::from)
            // A grant within the read-only checkout (or an ancestor whose subpath
            // grant would include it) must NOT re-open the checkout to writes: the
            // read-only guarantee outranks any session grant. Dropped here so the
            // policy is bulletproof regardless of caller.
            .filter(|p| !paths_overlap(p, checkout))
            .collect();

        let mut writable_extra = default_writable_extra();
        // The read-only checkout outranks every writable carve-out, including a
        // temp/toolchain root that happens to contain it. An ancestor allowance
        // would otherwise re-open the checkout on allowlist sandboxes.
        writable_extra.retain(|path| !paths_overlap(path, checkout));
        writable_extra.extend(granted_paths);

        Self {
            worktree: checkout.to_path_buf(),
            writable_extra,
            deny_read,
            writable_regex: Vec::new(),
            worktree_writable: false,
            socket_resolvable: Vec::new(),
            callback_socket: None,
            readable_extra: Vec::new(),
            violation_tag: ViolationTag::fresh(),
            reach: Default::default(),
        }
    }

    /// Build a policy for a long-lived **build service** daemon (see
    /// `docs/worktree-fence.md` — Managed Build Services).
    ///
    /// A service is shared across every worktree it serves, so it cannot be
    /// confined to a single worktree like `for_run`. Instead it is allowed to
    /// write only its own `state_dir` (its cache/state home), the standard
    /// temp/toolchain dirs, and the configured `writable_globs` (the explicit
    /// cross-worktree grant, e.g. `{worktrees}/**/target/**`). Reads stay broad
    /// minus `deny_read`, so the daemon still cannot read external secret
    /// stores, and it notably cannot write worktree source, `$HOME`, or secrets.
    ///
    /// `writable_globs` are already template-expanded absolute glob patterns;
    /// each is converted to an anchored regex for the OS layer.
    pub fn for_service(
        state_dir: &Path,
        writable_globs: &[String],
        deny_read: Vec<PathBuf>,
    ) -> Self {
        let mut writable_extra = default_writable_extra();
        writable_extra.push(state_dir.to_path_buf());

        Self {
            // The daemon's state dir is its primary writable+readable root; it
            // stands in for `worktree` (a service has no single worktree).
            worktree: state_dir.to_path_buf(),
            writable_extra,
            deny_read,
            writable_regex: writable_globs.iter().map(|g| glob_to_regex(g)).collect(),
            // The state dir stands in as the worktree, so it is resolvable:
            // a daemon's own control socket lives there.
            worktree_writable: true,
            socket_resolvable: Vec::new(),
            callback_socket: None,
            readable_extra: Vec::new(),
            violation_tag: ViolationTag::fresh(),
            reach: Default::default(),
        }
    }

    /// Build the **page-call** policy: what code a page hands its service runs
    /// under (see `docs/worktree-fence.md`).
    ///
    /// `dir` is the batch's own directory, its cwd, `HOME` and `TMPDIR`, and
    /// the only place it writes. Reads are confined to the system roots every
    /// process needs to start ([`system_read_roots`]), the shell roots a
    /// `command` item needs ([`shell_read_roots`]), the resolved interpreters'
    /// own trees (`interpreter_roots`, already checked by
    /// [`page_call::validate_root`]), and `dir`. Nothing is granted for temp
    /// directories, toolchain caches, or session crossings, so nothing here
    /// grows with the host. The network reaches only `relay_port`.
    ///
    /// One family with every other fence: the same profile generator and
    /// ruleset, narrower on two axes, closed by the same operator credential
    /// floor.
    pub fn for_page_call(dir: &Path, interpreter_roots: &[PathBuf], relay_port: u16) -> Self {
        let mut roots = system_read_roots();
        roots.extend(shell_read_roots());
        for root in interpreter_roots {
            if !roots.contains(root) {
                roots.push(root.clone());
            }
        }
        Self {
            worktree: dir.to_path_buf(),
            writable_extra: Vec::new(),
            deny_read: Vec::new(),
            writable_regex: Vec::new(),
            worktree_writable: true,
            socket_resolvable: Vec::new(),
            callback_socket: None,
            readable_extra: Vec::new(),
            violation_tag: ViolationTag::fresh(),
            reach: PolicyReach {
                reads: ReadScope::Allowlist { roots },
                network: NetworkScope::RelayOnly { port: relay_port },
            },
        }
    }

    /// Grant the writes an **interactive shell** makes on its own behalf.
    ///
    /// A PTY terminal deliberately boots the user's real shell, so that shell's
    /// startup config runs inside the fence: it rewrites its universal
    /// variables, appends history, and lets plugins update their state under the
    /// XDG data and state homes. None of that is the work a fence exists to
    /// contain — it is the cost of having chosen the user's shell over a bare
    /// one — and denying it makes every terminal open print kernel-denial
    /// stanzas before the prompt appears (`mktemp: … Operation not permitted`,
    /// fish's `Unable to open universal variable file`). Commands typed into the
    /// shell inherit the spawn's confinement, so this widens what they may write
    /// by exactly the named leaves below, and by nothing else.
    ///
    /// Opt-in per spawn, because only the terminal seam boots a shell: a `run`
    /// batch executes through `/bin/sh -c`, which sources no user config and
    /// needs nothing here.
    ///
    /// The grant names individual leaves — the shell's own directory under each
    /// XDG base, and the state directories of plugins that write as a side
    /// effect of the shell running. The bases themselves stay unwritable,
    /// because a write rule is recursive and granting `~/.local/share` would
    /// hand every command typed into the terminal write access to every
    /// application's state beneath it.
    ///
    /// Ensures those bases exist as a side effect: the kernel permits creating a
    /// granted leaf that does not exist yet, but only when its parent already
    /// does (see [`ensure_shell_state_bases`]).
    pub fn grant_interactive_shell_state(&mut self, shell_path: &str) {
        ensure_shell_state_bases();
        self.grant_state_dirs(interactive_shell_state_dirs(shell_path));
    }

    /// Add writable roots, skipping any the read denylist covers. A grant is
    /// re-allowed for reads (see [`Self::readable_paths`]), so admitting a
    /// denied subtree here would silently undo the denial.
    fn grant_state_dirs(&mut self, dirs: Vec<PathBuf>) {
        for dir in dirs {
            if self
                .deny_read
                .iter()
                .any(|denied| paths_overlap(&dir, denied))
            {
                continue;
            }
            if !self.writable_extra.contains(&dir) {
                self.writable_extra.push(dir);
            }
        }
    }

    /// Writable subpaths that drive the OS **write-allow**: the worktree (only
    /// when [`worktree_writable`](Self::worktree_writable)) + extras (temp +
    /// toolchain + grants). A read-only-checkout policy drops the worktree here so
    /// writes into the live checkout are kernel-denied.
    pub fn writable_paths(&self) -> Vec<PathBuf> {
        let mut paths = Vec::new();
        if self.worktree_writable {
            paths.push(self.worktree.clone());
        }
        paths.extend(self.writable_extra.iter().cloned());
        paths
    }

    /// Grant this spawn its `$TMPDIR`: `dir` becomes writable, and a socket
    /// server bound beneath it stays reachable from the job's later spawns.
    ///
    /// The one home for that rule, so every seam that exports a `TMPDIR` grants
    /// the same two things for it. A read-only checkout's guarantee outranks it,
    /// as it outranks a session grant: a `dir` overlapping that checkout is not
    /// made writable.
    pub fn grant_tmpdir(&mut self, dir: &Path) {
        let reopens_readonly_checkout =
            !self.worktree_writable && paths_overlap(dir, &self.worktree);
        if !reopens_readonly_checkout && !self.writable_extra.iter().any(|path| path == dir) {
            self.writable_extra.push(dir.to_path_buf());
        }
        self.allow_job_sockets_in(dir);
    }

    /// Let later spawns of the same job reach socket servers bound beneath
    /// `dir`, which is the cell's own scratch (its `$TMPDIR`).
    ///
    /// Each spawn installs its own landlock domain, so two batches of one job
    /// are sibling domains and the kernel treats a server one of them started
    /// as foreign to the other. Naming the job's scratch here is what keeps
    /// `tmux -S $TMPDIR/t.sock` started in one batch usable from the next.
    ///
    /// A shared root is refused rather than trusted to callers: granting
    /// `/tmp`, `$HOME`, `/run` or Cairn's scratch root would re-open every
    /// other job's sockets and the predictable host ones (`/tmp/tmux-<uid>`,
    /// `/tmp/ssh-*`, `/run/user/<uid>/bus`) the boundary exists to close.
    pub fn allow_job_sockets_in(&mut self, dir: &Path) {
        if is_shared_socket_root(dir) || self.socket_resolvable.iter().any(|path| path == dir) {
            return;
        }
        self.socket_resolvable.push(dir.to_path_buf());
    }

    /// Subpaths beneath which a fenced spawn may reach a socket served from
    /// outside its own sandbox: the worktree (only when
    /// [`worktree_writable`](Self::worktree_writable)) plus the job's scratch.
    /// Filtered again here, so a worktree that happens to be a shared root
    /// never widens the set either.
    pub fn socket_resolvable_paths(&self) -> Vec<PathBuf> {
        let mut paths = Vec::new();
        if self.worktree_writable {
            paths.push(self.worktree.clone());
        }
        paths.extend(self.socket_resolvable.iter().cloned());
        paths.retain(|path| !is_shared_socket_root(path));
        // Its directory holds nothing but Cairn's own sockets, so granting it
        // reaches the callback socket and no one else's server.
        if let Some(dir) = self.callback_socket.as_deref().and_then(Path::parent) {
            if !paths.iter().any(|path| path == dir) {
                paths.push(dir.to_path_buf());
            }
        }
        paths
    }

    /// Readable subpaths re-allowed after the read denylist: the worktree
    /// (**always**, even for a read-only checkout) + extras + approved reads.
    /// So a denylist entry covering a prefix of the checkout can never block
    /// the agent from reading its own source, and a read-only checkout stays
    /// fully readable while its writes are denied.
    #[cfg(any(test, target_os = "macos"))]
    fn readable_paths(&self) -> Vec<PathBuf> {
        let mut paths = vec![self.worktree.clone()];
        paths.extend(self.writable_extra.iter().cloned());
        paths.extend(self.readable_extra.iter().cloned());
        paths
    }
}

/// The system paths a dynamically linked program reads to start: the loader,
/// its shared cache, the system libraries, and the time-zone and locale data.
/// Nothing here holds user data. Measured by `IsolatedChild`'s live tests and
/// shared with the page-call policy, so there is one answer to what the
/// loader and runtime need.
pub(crate) fn system_read_roots() -> Vec<PathBuf> {
    #[cfg(target_os = "macos")]
    let paths: &[&str] = &[
        "/usr/lib",
        "/System/Library",
        "/System/Cryptexes",
        "/System/Volumes/Preboot/Cryptexes",
        "/Library/Apple/usr/lib",
        "/private/var/db/dyld",
        "/private/var/db/timezone",
        "/usr/share/zoneinfo",
        // The system ICU's locale data: without it `Intl` cannot format a number.
        "/usr/share/icu",
        "/private/etc/localtime",
        "/dev/null",
        "/dev/random",
        "/dev/urandom",
    ];
    #[cfg(not(target_os = "macos"))]
    let paths: &[&str] = &[
        "/lib",
        "/lib64",
        "/usr/lib",
        "/usr/lib64",
        "/etc/ld.so.cache",
        "/etc/localtime",
        "/usr/share/zoneinfo",
        "/dev/null",
        "/dev/random",
        "/dev/urandom",
        // The runtime sizes its heap and thread pool from these.
        "/proc/meminfo",
        "/proc/cpuinfo",
        "/sys/devices/system/cpu",
        "/sys/fs/cgroup",
    ];
    paths.iter().map(PathBuf::from).collect()
}

/// What a `command` item's shell and the ordinary utilities it runs read:
/// the system binary directories. Separate from [`system_read_roots`] because
/// an isolated child execs only its own program and needs none of them.
pub(crate) fn shell_read_roots() -> Vec<PathBuf> {
    #[cfg(target_os = "macos")]
    let paths: &[&str] = &["/bin", "/usr/bin", "/usr/libexec"];
    #[cfg(not(target_os = "macos"))]
    let paths: &[&str] = &["/bin", "/usr/bin", "/usr/libexec", "/proc/self"];
    paths.iter().map(PathBuf::from).collect()
}

/// The default read denylist, anchored at the user's home directory.
///
/// Deliberately **narrow** — hard-deny is a last resort for *obviously sensitive*
/// secrets the agent cannot otherwise reach: external credential stores and
/// private keys (`~/.aws`, `~/.config/gcloud`, `~/.netrc`, `~/.ssh`, `~/.gnupg`).
///
/// Cairn's own `~/.cairn[-dev]` is **not** denied: its DB contents are already
/// reachable through `cairn://` resources, the callback secret is injected into
/// every spawn's env regardless, and the workspace packages (`skills/`,
/// `agents/`, `recipes/`, `tools/`, `resources/`) plus the worktree itself live
/// there and must stay readable. The agent reading its own state dir is
/// expected, not a leak. `~/.config/gh` is likewise not denied — the toolchain
/// reads it to push, and it is already an app-scoped token.
///
/// (For an `Ask` agent these denials are not impassable: a denied read surfaces
/// as an approvable crossing and a grant lets it through. Only `Deny` agents
/// hard-fail. `~/.ssh` is conservative — an ssh git remote needs a one-time
/// grant or its removal via settings.)
pub fn default_deny_read() -> Vec<PathBuf> {
    match home_dir() {
        Some(h) => default_deny_read_in(&h),
        None => Vec::new(),
    }
}

fn default_deny_read_in(home: &Path) -> Vec<PathBuf> {
    [".aws", ".config/gcloud", ".netrc", ".ssh", ".gnupg"]
        .iter()
        .map(|rel| home.join(rel))
        .collect()
}

/// Whether OS-level sandboxing is available on this platform/host at runtime.
///
/// macOS: `sandbox-exec` is always present. Linux: requires a landlock-capable
/// kernel (ABI ≥ 1, kernel 5.13+). Other platforms: unavailable.
pub fn is_available() -> bool {
    #[cfg(target_os = "macos")]
    {
        true
    }
    #[cfg(target_os = "linux")]
    {
        linux::is_available()
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        false
    }
}

/// Rewrite a spawn's `(program, args)` to apply the sandbox.
///
/// On macOS this returns the `sandbox-exec`-wrapped invocation. On Linux and
/// other platforms the argv is unchanged (Linux confines via `pre_exec`; see
/// [`install_pre_exec`]).
pub fn wrap_argv(program: &str, args: &[String], policy: &SandboxPolicy) -> (String, Vec<String>) {
    #[cfg(target_os = "macos")]
    {
        macos::wrap_argv(program, args, policy)
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = policy;
        (program.to_string(), args.to_vec())
    }
}

/// Install the sandbox on a `std::process::Command` for platforms that confine
/// in-process (Linux landlock via `pre_exec`). No-op on macOS (argv-wrapped)
/// and unsupported platforms.
pub fn install_pre_exec(cmd: &mut std::process::Command, policy: &SandboxPolicy) {
    #[cfg(target_os = "linux")]
    {
        linux::install_pre_exec(cmd, policy);
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (cmd, policy);
    }
}

/// Attach `command` to a pseudoterminal as a session leader, the way a resident
/// PTY spawns: `slave` becomes its stdio and controlling terminal, inherited
/// signal dispositions reset to default, and every inherited descriptor above
/// stdio is marked close-on-exec.
///
/// It lives beside [`install_pre_exec`] because it is half of the PTY spawn
/// seam: the executor's terminal spawn and the containment probe for that seam
/// both call it, then install the confinement, so the probe measures the very
/// hook order a terminal gets. Call it before [`install_pre_exec`]; hooks run
/// in registration order, and the confinement must cover the program and
/// nothing the session setup needs.
///
/// Descriptors are marked rather than closed: std's own exec-error pipe is
/// among them, and closing it would turn a failed exec or a failed
/// confinement into a spawn that appears to succeed.
#[cfg(unix)]
pub fn attach_pty_session(
    command: &mut std::process::Command,
    slave: std::fs::File,
) -> std::io::Result<()> {
    use std::os::unix::process::CommandExt;

    command
        .stdin(std::process::Stdio::from(slave.try_clone()?))
        .stdout(std::process::Stdio::from(slave.try_clone()?))
        .stderr(std::process::Stdio::from(slave));
    // SAFETY: runs in the forked child before exec and makes only
    // async-signal-safe calls, apart from the descriptor listing, which reads a
    // directory exactly as portable-pty's own hook did.
    unsafe {
        command.pre_exec(|| {
            for signal in [
                libc::SIGCHLD,
                libc::SIGHUP,
                libc::SIGINT,
                libc::SIGQUIT,
                libc::SIGTERM,
                libc::SIGALRM,
            ] {
                libc::signal(signal, libc::SIG_DFL);
            }
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            // Without a controlling terminal the shell gets no SIGWINCH on
            // resize and no job control.
            #[allow(clippy::cast_lossless)]
            if libc::ioctl(0, libc::TIOCSCTTY as _, 0) == -1 {
                return Err(std::io::Error::last_os_error());
            }
            mark_inherited_descriptors_cloexec();
            Ok(())
        });
    }
    Ok(())
}

#[cfg(unix)]
fn mark_inherited_descriptors_cloexec() {
    let Ok(entries) = std::fs::read_dir("/dev/fd") else {
        return;
    };
    for fd in entries
        .flatten()
        .filter_map(|entry| entry.file_name().to_str()?.parse::<libc::c_int>().ok())
        .filter(|fd| *fd > 2)
    {
        // SAFETY: fcntl on a descriptor that has since closed only returns
        // EBADF.
        unsafe {
            let flags = libc::fcntl(fd, libc::F_GETFD);
            if flags != -1 {
                libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC);
            }
        }
    }
}

/// What this platform would actually apply to a confined batch spawn, as text
/// that changes whenever the enforcement does.
///
/// This is a fingerprint input, never evidence on its own: it exists so that a
/// measurement taken under one profile or ruleset stops counting the moment
/// that profile or ruleset changes. `None` on a platform that applies no
/// filesystem confinement, which makes the input unreadable and so keeps the
/// edge unmeasured rather than letting an empty description stand in for a
/// stable one.
pub fn enforcement_description() -> Option<String> {
    // A canonical policy, not a live one: the question this answers is "what
    // shape of rule does this build install", and pinning the paths keeps the
    // digest stable across cells while still moving when the generator does.
    let mut policy = SandboxPolicy::for_run(Path::new("/cairn/containment/probe"), &[], Vec::new());
    policy.allow_job_sockets_in(Path::new("/cairn/containment/scratch"));
    policy.violation_tag = ViolationTag::canonical();
    #[cfg(target_os = "macos")]
    {
        let floor = OperatorCredentialFloor::new(
            Some(std::path::PathBuf::from("/cairn/containment/home")),
            [],
        );
        Some(macos::sbpl_profile(&policy, &floor))
    }
    #[cfg(target_os = "linux")]
    {
        Some(linux::enforcement_description(&policy))
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = policy;
        None
    }
}

/// A detected sandbox denial, used to drive the worktree fence after a command
/// was blocked by the kernel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SandboxDenial {
    /// A precise denied path was recovered (macOS unified log) — the fence can
    /// raise a path-scoped crossing whose grant generalizes across commands.
    Path {
        path: PathBuf,
        /// Kernel operation reported by the sandbox log, such as
        /// `file-write-create`, when the platform exposes it.
        operation: Option<String>,
    },
    /// A denial occurred but no path was recovered — the fence raises a
    /// command-scoped crossing.
    Command,
}

/// Detect whether a sandboxed command was blocked by the kernel.
///
/// The unified trigger across platforms: command output carrying a
/// permission-denial signature. A clean final shell exit suppresses synthetic
/// command-scoped fallback for Deny agents, but not for Ask agents: shells can
/// mask an earlier sandbox denial with `;`, `||`, traps, or harness cleanup. On
/// macOS the precise denied path is recovered from the unified log (best-effort),
/// upgrading a command-scoped denial to a path-scoped one. `violations` is the
/// tag of the policy the spawn ran under (see [`ViolationTag`]): the lookup
/// finds a violation made anywhere in the spawn's process tree and none made
/// outside it, and without a tag there is nothing to look up.
/// `command_scoped_fallback` lets Ask agents raise a recoverable prompt when
/// macOS path recovery misses while Deny agents keep the raw command failure.
/// Only call this when the sandbox was actually applied (worktree agent with
/// `OnEscape` Ask/Deny).
pub fn detect_denial(
    exit_code: Option<i32>,
    combined_output: &str,
    violations: Option<&ViolationTag>,
    since: SystemTime,
    command_scoped_fallback: bool,
) -> Option<SandboxDenial> {
    // A clean final shell exit can mask an earlier sandbox denial (`cmd; echo
    // $?`, `cmd || true`, traps, harness cleanup). For Ask agents, keep looking
    // for a denial signature so the user has a recovery path. For Deny agents
    // (fallback disabled), preserve the historical raw-output behavior.
    if exit_code == Some(0) && !command_scoped_fallback {
        return None;
    }
    // Gate on a denial signature so ordinary exits (test failures, grep
    // no-match, successful commands) never raise the fence — and so the macOS log
    // query only runs for plausibly-denied commands.
    if !has_denial_signature(combined_output) {
        return None;
    }

    #[cfg(target_os = "macos")]
    {
        // Prefer the authoritative, path-scoped kernel log violation. For Ask
        // agents, fall back to a recoverable command-scoped crossing if the log
        // lookup misses; for Deny agents, preserve the historical fail-fast raw
        // command output rather than synthesizing a denial.
        match violations {
            Some(tag) => match macos::detect_violation(tag, since) {
                Some(violation) => Some(SandboxDenial::Path {
                    path: violation.path,
                    operation: Some(violation.operation),
                }),
                None if command_scoped_fallback => Some(SandboxDenial::Command),
                None => None,
            },
            None if command_scoped_fallback => Some(SandboxDenial::Command),
            None => None,
        }
    }
    #[cfg(not(target_os = "macos"))]
    {
        // No out-of-band violation signal; the signature is the only trigger
        // (command-scoped), so it recognises both C-style and Go-style errno
        // text. The cost is a false positive on a genuine non-sandbox
        // permission error, from a C tool or a Go one, which raises an
        // answerable command-scoped crossing for a fenced spawn
        // (docs/worktree-fence.md, "Permission decisions").
        let _ = (violations, since, command_scoped_fallback);
        Some(SandboxDenial::Command)
    }
}

/// The first line of `output` that reads as a permission denial, bounded, for a
/// prompt that could not recover the denied path to quote.
///
/// The command wrote this text, so it is a clue for whoever answers the prompt
/// and never evidence: it must not become a crossing's scope.
pub fn reported_denial_line(output: &str) -> Option<String> {
    const MAX_CHARS: usize = 240;
    let line = output
        .lines()
        .map(str::trim)
        .find(|line| line_has_denial_signature(line))?;
    let mut bounded: String = line.chars().take(MAX_CHARS).collect();
    if bounded.len() < line.len() {
        bounded.push('…');
    }
    Some(bounded)
}

/// Whether output carries a permission-denial signature.
///
/// C-style errno text is libc's capitalised `strerror` (`Permission denied`).
/// Go-style errno text is lowercase and arrives in the `*PathError` /
/// `*SyscallError` shape `op path: errno-text`, which is what podman, docker,
/// buildah and most Go tooling print, so the Go forms are matched with their
/// leading `: `. A bare lowercase phrase is deliberately not a signature: an
/// Ask spawn is scanned even on a clean exit, and `rg 'permission denied'` or
/// a test log's prose must not raise a crossing.
pub fn has_denial_signature(output: &str) -> bool {
    output.lines().any(line_has_denial_signature)
}

/// Whether one output line carries a permission-denial signature.
pub(crate) fn line_has_denial_signature(line: &str) -> bool {
    const SIGS: [&str; 6] = [
        "Operation not permitted",
        "Permission denied",
        "EACCES",
        "EPERM",
        ": operation not permitted",
        ": permission denied",
    ];
    !is_selinux_creation_context_warning(line) && SIGS.iter().any(|s| line.contains(s))
}

/// Whether a line is the warning SELinux-aware GNU tools print when they cannot
/// set the security context for the files they are about to create.
///
/// On an SELinux host, GNU `sed -i` (and coreutils' `install`, `cp`, `mkdir`)
/// first writes the desired context to `/proc/self/attr/fscreate`. The fence
/// does not grant procfs writes, so that write fails, the tool prints
/// `warning: failed to set default file creation context to …: Permission
/// denied`, and then carries on creating the file with the context it inherits
/// from its directory. The edit lands and the exit status is 0. Read as a
/// denial, it turned every in-worktree `sed -i` on such a host into an
/// unanswerable command-scoped prompt for an edit that had already happened.
fn is_selinux_creation_context_warning(line: &str) -> bool {
    line.contains("failed to set default file creation context")
}

/// The static writable carve-outs shared by the OS sandbox **and** the `write`
/// verb's fence: temp dirs + toolchain cache/state dirs (cargo registry +
/// package-cache lock, npm cache, …). `$HOME` is otherwise read-only, so without
/// these a cold-cache or dependency-adding `cargo build`/`npm ci` would be
/// kernel-denied. Excludes session grants (those flow through the fence's grant
/// check) and the worktree. The `write` verb treats a write here as in-sandbox
/// (no prompt), matching a shell write under `run`.
pub fn default_writable_extra() -> Vec<PathBuf> {
    let mut dirs = temp_dirs();
    dirs.extend(toolchain_writable_dirs());
    // Cairn-managed shared uv package cache (`<cairn_home>/uv-cache`), pointed at
    // by `UV_CACHE_DIR` on every agent spawn. Existence-filtered like the
    // toolchain dirs; the host creates it at startup (`env::ensure_uv_cache_dir`),
    // so a fenced agent can populate it without tripping an out-of-worktree
    // write, while uv's default `~/.cache/uv` stays out of the picture.
    if let Some(uv_cache) = std::env::var_os("UV_CACHE_DIR").map(PathBuf::from) {
        if uv_cache.exists() {
            dirs.push(uv_cache);
        }
    }
    // Cairn's scratch root (`<cairn_home>/scratch`) is deliberately absent: it
    // holds Cairn's per-process bookkeeping, and granting it whole let agents
    // make unowned sibling workspaces there. A spawn whose `$TMPDIR` is its own
    // directory under the root is granted exactly that one through
    // `SandboxPolicy::grant_tmpdir`; see `scratch_refusal`.
    // The executor's tool cache, where a project's setup caches the tools it
    // builds for itself (the pinned `tursodb`). Existence-filtered like the uv
    // cache: the executor creates it before every spawn.
    dirs.extend(tool_cache_grant());
    // Cairn's own log directory is deliberately absent; see
    // `the_cairn_log_dir_is_never_writable`.
    dirs
}

/// The executor's tool cache, when this process was handed one and it exists.
///
/// The executor exports `CAIRN_TOOL_CACHE` at bootstrap
/// (`cairn_common::executor_paths::ExecutorPaths::tool_cache`). Only a
/// directory carrying that layout's leaf name is accepted, because the grant is
/// recursive and this function also runs in processes the executor did not
/// start: a desktop app launched from a shell that happens to export
/// `CAIRN_TOOL_CACHE=$HOME` must not hand every fenced spawn the home directory.
pub(crate) fn tool_cache_grant() -> Option<PathBuf> {
    tool_cache_grant_from(std::env::var_os(
        cairn_common::executor_paths::TOOL_CACHE_ENV,
    ))
}

fn tool_cache_grant_from(value: Option<std::ffi::OsString>) -> Option<PathBuf> {
    let cache = PathBuf::from(value?);
    let named = cache.file_name()
        == Some(std::ffi::OsStr::new(
            cairn_common::executor_paths::TOOL_CACHE_DIR,
        ));
    (named && cache.is_absolute() && cache.is_dir()).then_some(cache)
}

/// Standard temp directories that must stay writable so the toolchain (cargo,
/// rustc, npm, git) can use scratch space.
/// Toolchain cache/state dirs that must stay writable so build tools work with
/// their shared caches in `$HOME`. Confining writes to the worktree alone breaks
/// cargo (`~/.cargo` registry cache + `.package-cache` lock), npm (`~/.npm`),
/// and friends — they write to these on a cold cache or when a dependency is
/// added. Only existing dirs are emitted (a missing one is inert in the rules).
fn toolchain_writable_dirs() -> Vec<PathBuf> {
    match home_dir() {
        Some(h) => toolchain_writable_dirs_in(&h),
        None => Vec::new(),
    }
}

fn toolchain_writable_dirs_in(home: &Path) -> Vec<PathBuf> {
    let mut dirs: Vec<PathBuf> = [
        ".cargo",
        ".rustup",
        ".npm",
        ".bun",
        ".yarn",
        ".cache",
        ".pnpm-store",
        ".local/share/pnpm",
        ".gradle",
        ".m2",
        ".deno",
        "go",
        // macOS XDG-cache equivalent (go-build, many tools).
        "Library/Caches",
    ]
    .iter()
    .map(|rel| home.join(rel))
    .collect();
    dirs.retain(|p| p.exists());
    dirs
}

/// Directory names of shell plugins that write state as a side effect of the
/// shell simply running: directory trackers that record every `cd`, and history
/// stores. Each is named individually because the XDG bases they sit under also
/// hold unrelated application state, and a write rule is recursive — granting a
/// base would hand every command typed into the terminal write access to all of
/// it. A plugin that is not listed surfaces as an ordinary, visible denial,
/// which is the signal to add it here deliberately rather than to widen the
/// boundary.
const SHELL_PLUGIN_STATE_DIRS: [&str; 5] = ["z", "zoxide", "autojump", "fasd", "atuin"];

/// The paths an interactive shell writes for its own bookkeeping: its own
/// directory beneath each XDG base (fish keeps `fish_variables` under the config
/// home and its history under the data home), the state directories of the
/// plugins in [`SHELL_PLUGIN_STATE_DIRS`], and the history files zsh and bash
/// keep directly in `$HOME`.
///
/// Deliberately **not** filtered to paths that already exist: a grant naming a
/// path that does not exist yet is exactly what lets a first-run shell create
/// it, and the OS rule is inert until something does. The XDG bases are never
/// granted, only entered.
pub fn interactive_shell_state_dirs(shell_path: &str) -> Vec<PathBuf> {
    let Some(home) = home_dir() else {
        return Vec::new();
    };
    interactive_shell_state_dirs_in(
        &home,
        &xdg_base(&home, "XDG_CONFIG_HOME", ".config"),
        &xdg_base(&home, "XDG_DATA_HOME", ".local/share"),
        &xdg_base(&home, "XDG_STATE_HOME", ".local/state"),
        shell_path,
    )
}

/// Best-effort creation of the XDG base directories a shell keeps its state
/// under.
///
/// The grants name leaves beneath these bases rather than the bases themselves,
/// which is what keeps unrelated application state out of the writable set. The
/// kernel will let a confined shell create a granted leaf that does not exist
/// yet — but only if the parent already does, since `mkdir -p
/// ~/.local/share/fish` on a fresh home must create `~/.local/share` first and
/// that path is not granted. Creating the standard bases before confining the
/// shell is what makes a first-run home behave like an established one. Mirrors
/// `env::ensure_uv_cache_dir`, which exists for the same reason.
fn ensure_shell_state_bases() {
    let Some(home) = home_dir() else {
        return;
    };
    for base in [
        xdg_base(&home, "XDG_CONFIG_HOME", ".config"),
        xdg_base(&home, "XDG_DATA_HOME", ".local/share"),
        xdg_base(&home, "XDG_STATE_HOME", ".local/state"),
    ] {
        let _ = std::fs::create_dir_all(base);
    }
}

/// Resolve an XDG base directory, falling back to its default under `home`. A
/// relative value is ignored, as the XDG spec requires.
fn xdg_base(home: &Path, var: &str, fallback: &str) -> PathBuf {
    std::env::var_os(var)
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
        .unwrap_or_else(|| home.join(fallback))
}

fn interactive_shell_state_dirs_in(
    home: &Path,
    config_home: &Path,
    data_home: &Path,
    state_home: &Path,
    shell_path: &str,
) -> Vec<PathBuf> {
    let shell = shell_basename(shell_path);
    let mut dirs = Vec::new();
    if !shell.is_empty() {
        // nushell is the one common shell whose dir is not its binary name.
        let name = if shell == "nu" { "nushell" } else { &shell };
        dirs.push(config_home.join(name));
        dirs.push(data_home.join(name));
        dirs.push(state_home.join(name));
    }
    for plugin in SHELL_PLUGIN_STATE_DIRS {
        dirs.push(data_home.join(plugin));
        dirs.push(state_home.join(plugin));
    }
    // Shells that keep history in `$HOME` rather than under an XDG base.
    let home_state: &[&str] = match shell.as_str() {
        "zsh" => &[".zsh_history", ".zsh_sessions"],
        "bash" => &[".bash_history"],
        _ => &[],
    };
    dirs.extend(home_state.iter().map(|relative| home.join(relative)));
    dirs
}

/// The shell's lowercased binary name, without directory or `.exe` suffix.
fn shell_basename(shell_path: &str) -> String {
    Path::new(shell_path)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default()
        .to_lowercase()
        .trim_end_matches(".exe")
        .to_string()
}

/// Whether granting socket resolution beneath `dir` would reach sockets that
/// are not the job's own: `dir` is, or contains, a temp root, the home
/// directory, Cairn's scratch root, or a runtime/device root.
fn is_shared_socket_root(dir: &Path) -> bool {
    let mut roots = temp_dirs();
    roots.extend(home_dir());
    roots.push(cairn_common::scratch::scratch_root());
    roots.extend(["/run", "/var/run", "/dev"].map(PathBuf::from));
    is_shared_socket_root_in(dir, &roots)
}

fn is_shared_socket_root_in(dir: &Path, roots: &[PathBuf]) -> bool {
    let canonical = |path: &Path| path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    // Both spellings of `dir`: on macOS `/home` canonicalizes to
    // `/System/Volumes/Data/home`, which a root that does not exist (and so
    // keeps its raw spelling) never starts with. Matching more only refuses more.
    let spellings = [dir.to_path_buf(), canonical(dir)];
    !dir.is_absolute()
        || roots.iter().any(|root| {
            let root_canonical = canonical(root);
            spellings
                .iter()
                .any(|dir| root_canonical.starts_with(dir) || root.starts_with(dir))
        })
}

fn temp_dirs() -> Vec<PathBuf> {
    let mut dirs = vec![std::env::temp_dir()];
    for p in [
        "/tmp",
        "/private/tmp",
        "/var/folders",
        "/private/var/folders",
    ] {
        let path = PathBuf::from(p);
        if path.exists() && !dirs.contains(&path) {
            dirs.push(path);
        }
    }
    dirs
}

/// Escape a string for an SBPL double-quoted literal.
#[cfg(any(test, target_os = "macos"))]
pub(crate) fn escape_sbpl(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

fn home_dir() -> Option<PathBuf> {
    std::env::var("HOME")
        .ok()
        .map(PathBuf::from)
        .or_else(dirs::home_dir)
}

/// Convert an absolute writable glob into a start-anchored regex string for the
/// OS sandbox layer (macOS SBPL `(regex ...)`).
///
/// Glob semantics: `**` matches any depth (including `/`), `*` matches within a
/// single path segment. The result is anchored at the start (`^`) but not the
/// end, so it matches the glob's prefix and everything beneath it (subpath
/// semantics). Regex metacharacters in the literal portions are escaped.
pub fn glob_to_regex(glob: &str) -> String {
    let chars: Vec<char> = glob.chars().collect();
    let mut re = String::from("^");
    let mut i = 0;
    while i < chars.len() {
        match chars[i] {
            '*' => {
                if i + 1 < chars.len() && chars[i + 1] == '*' {
                    // `**` — any depth, including path separators.
                    re.push_str(".*");
                    i += 2;
                } else {
                    // `*` — within a single segment.
                    re.push_str("[^/]*");
                    i += 1;
                }
            }
            // Escape regex metacharacters that can appear in literal path parts.
            c @ ('.' | '+' | '(' | ')' | '[' | ']' | '{' | '}' | '^' | '$' | '?' | '|' | '\\') => {
                re.push('\\');
                re.push(c);
                i += 1;
            }
            c => {
                re.push(c);
                i += 1;
            }
        }
    }
    re
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The sudoers rule the Mac setup script installs and the runner-isolation
    /// gate allows, as committed. Read at compile time, so the public mirror
    /// carries it (`MIRROR_ASSETS` in `scripts/mirror-os.ts`).
    const COMMITTED_SUDOERS_COMMAND: &str =
        include_str!("../../../../scripts/lib/fence-violation-stream.sudoers-command");

    #[test]
    fn committed_violation_stream_sudoers_command_is_the_render() {
        let rendered = format!("{}\n", violation_stream_sudoers_command());
        if std::env::var_os("CAIRN_WRITE_SUDOERS_COMMAND").is_some() {
            let path = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../../scripts/lib/fence-violation-stream.sudoers-command");
            std::fs::write(&path, &rendered).unwrap();
            return;
        }
        assert_eq!(
            COMMITTED_SUDOERS_COMMAND, rendered,
            "scripts/lib/fence-violation-stream.sudoers-command is not the render of \
             cairn_sandbox::violation_stream_argv. Regenerate it with \
             `CAIRN_WRITE_SUDOERS_COMMAND=1 cargo test -p cairn-sandbox --lib \
             committed_violation_stream_sudoers_command_is_the_render` (from src-tauri), \
             commit it, and reinstall the rule on every Mac executor host \
             (docs/releases.md)"
        );
    }

    #[test]
    fn violation_stream_sudoers_command_matches_the_spawned_argv() {
        // sudo compares the rule's arguments, unescaped, with argv joined by
        // single spaces; the program is compared separately, as a path.
        let argv = violation_stream_argv();
        assert_eq!(
            violation_stream_sudoers_command().replace('\\', ""),
            argv.join(" ")
        );
        assert_eq!(argv[0], "/usr/bin/log");
        assert!(argv[5].contains(ViolationTag::PREFIX));
    }

    /// An account outside `admin` reaches `log stream` only through the
    /// operator's rule, which sudo matches byte for byte. So what the fence
    /// spawns behind `sudo -n` must be exactly the command the committed rule
    /// names: program by path, arguments joined by single spaces.
    #[test]
    fn the_sudo_spawn_runs_exactly_the_command_the_installed_rule_names() {
        use cairn_common::executor_protocol::ViolationStreamAccess;
        let spawned = violation_stream_spawn_argv(ViolationStreamAccess::SudoRule);
        assert_eq!(spawned[..2], VIOLATION_STREAM_SUDO);
        let command = &spawned[2..];
        assert_eq!(command, violation_stream_argv());
        assert_eq!(
            COMMITTED_SUDOERS_COMMAND.trim_end().replace('\\', ""),
            command.join(" "),
            "the sudo spawn and scripts/lib/fence-violation-stream.sudoers-command disagree, so sudo -n would refuse it"
        );
        assert_eq!(
            violation_stream_spawn_argv(ViolationStreamAccess::Admin),
            violation_stream_argv(),
            "an admin account runs the pinned command itself"
        );
    }

    #[test]
    fn sudoers_command_escapes_what_sudoers_requires_and_nothing_else() {
        let argv: Vec<String> = ["/bin/x", "a,b:c=d\\e", "\"quoted\" text"]
            .map(String::from)
            .to_vec();
        assert_eq!(
            sudoers_command(&argv),
            "/bin/x a\\,b\\:c\\=d\\\\e \"quoted\" text"
        );
    }

    #[test]
    fn sudoers_command_refuses_what_it_cannot_render_literally() {
        for arg in [
            "glob*",
            "one?",
            "[set]",
            "#comment",
            "two  spaces",
            " lead",
            "",
            "tab\there",
        ] {
            let argv = vec!["/bin/x".to_string(), arg.to_string()];
            assert!(
                std::panic::catch_unwind(|| sudoers_command(&argv)).is_err(),
                "{arg:?} was rendered"
            );
        }
        assert!(std::panic::catch_unwind(|| sudoers_command(&["log".to_string()])).is_err());
    }

    #[test]
    fn default_denylist_is_external_secrets_only() {
        // Pure: no global-env mutation, so it can't race other tests.
        let deny = default_deny_read_in(Path::new("/home/tester"));
        // External credential stores + keys are denied (the narrow last resort).
        assert!(deny.contains(&PathBuf::from("/home/tester/.aws")));
        assert!(deny.contains(&PathBuf::from("/home/tester/.ssh")));
        assert!(deny.contains(&PathBuf::from("/home/tester/.gnupg")));
        assert!(deny.contains(&PathBuf::from("/home/tester/.config/gcloud")));
        // Cairn's own state dir is NOT denied: DB contents are URI-reachable,
        // the secret is in env, and skills/agents/recipes + the worktree live
        // there and must stay readable.
        assert!(!deny.contains(&PathBuf::from("/home/tester/.cairn")));
        assert!(!deny.contains(&PathBuf::from("/home/tester/.cairn/cairn.db")));
        assert!(!deny.contains(&PathBuf::from("/home/tester/.cairn/skills")));
    }

    #[test]
    fn interactive_shell_state_dirs_are_scoped_to_the_shell_not_the_xdg_bases() {
        let home = Path::new("/home/tester");
        let (config, data, state) = (
            home.join(".config"),
            home.join(".local/share"),
            home.join(".local/state"),
        );

        let dirs =
            interactive_shell_state_dirs_in(home, &config, &data, &state, "/opt/homebrew/bin/fish");

        // The shell's own directory beneath each base: `fish_variables` under
        // the config home, history under the data home.
        assert!(dirs.contains(&config.join("fish")));
        assert!(dirs.contains(&data.join("fish")));
        assert!(dirs.contains(&state.join("fish")));
        // Plugins that write as a side effect of the shell running.
        assert!(dirs.contains(&data.join("z")));
        // The bases themselves are never granted. A write rule is recursive, so
        // granting one would open every application's state beside the shell's
        // to anything typed into the terminal.
        assert!(!dirs.contains(&config));
        assert!(!dirs.contains(&data));
        assert!(!dirs.contains(&state));
        assert!(!dirs.contains(&home.to_path_buf()));
    }

    #[test]
    fn interactive_shell_state_dirs_are_creation_safe_and_shell_specific() {
        // A home where nothing exists yet: the paths are granted all the same,
        // because naming a path that does not exist is what lets a first-run
        // shell create it.
        let home = Path::new("/home/never-logged-in");
        let (config, data, state) = (
            home.join(".config"),
            home.join(".local/share"),
            home.join(".local/state"),
        );

        let dirs = interactive_shell_state_dirs_in(home, &config, &data, &state, "/bin/zsh");

        assert!(dirs.contains(&data.join("zsh")));
        assert!(dirs.contains(&home.join(".zsh_history")));
        assert!(
            !dirs.contains(&home.join(".bash_history")),
            "another shell's history is not this shell's state"
        );
        assert!(
            !dirs.contains(&config.join("fish")),
            "another shell's config is not this shell's state"
        );
    }

    #[test]
    fn interactive_shell_grant_never_reopens_a_denied_root() {
        let denied = PathBuf::from("/home/x/.local/share");
        let mut policy = SandboxPolicy::for_run(Path::new("/work/wt"), &[], vec![denied.clone()]);

        policy.grant_state_dirs(vec![
            denied.clone(),
            denied.join("z"),
            PathBuf::from("/home/x/.config/fish"),
        ]);

        // A grant is re-allowed for reads, so a denied root — or anything inside
        // one — must never enter the writable set through this door.
        assert!(!policy.writable_extra.contains(&denied));
        assert!(!policy.writable_extra.contains(&denied.join("z")));
        assert!(policy
            .writable_extra
            .contains(&PathBuf::from("/home/x/.config/fish")));

        policy.grant_state_dirs(vec![PathBuf::from("/home/x/.config/fish")]);
        assert_eq!(
            policy
                .writable_extra
                .iter()
                .filter(|path| *path == &PathBuf::from("/home/x/.config/fish"))
                .count(),
            1,
            "repeated grants must not accumulate duplicates"
        );
    }

    #[test]
    fn the_socket_resolvable_set_is_the_job_scratch_and_a_writable_worktree() {
        let wt = PathBuf::from("/work/wt");
        let scratch = PathBuf::from("/cairn/cells/a/scratch");
        let mut policy = SandboxPolicy::for_run(&wt, &[], vec![]);
        assert_eq!(policy.socket_resolvable_paths(), vec![wt.clone()]);

        policy.allow_job_sockets_in(&scratch);
        policy.allow_job_sockets_in(&scratch);
        assert_eq!(policy.socket_resolvable_paths(), vec![wt, scratch.clone()]);

        // None of the writable carve-outs leaks in: they are shared roots
        // whose sockets belong to the host and to other jobs.
        let resolvable = policy.socket_resolvable_paths();
        for extra in &policy.writable_extra {
            assert!(!resolvable.contains(extra), "{}", extra.display());
        }

        // A read-only checkout is not a place the job can bind a server.
        let mut readonly = SandboxPolicy::for_readonly_checkout(Path::new("/live"), &[], vec![]);
        readonly.allow_job_sockets_in(&scratch);
        assert_eq!(readonly.socket_resolvable_paths(), vec![scratch]);
    }

    #[test]
    fn a_shared_root_never_becomes_socket_resolvable() {
        let roots = [
            PathBuf::from("/tmp"),
            PathBuf::from("/home/tester"),
            PathBuf::from("/home/tester/.cairn/scratch"),
            PathBuf::from("/run"),
        ];
        for shared in [
            "/tmp",
            "/",
            "/home",
            "/home/tester",
            "/home/tester/.cairn",
            "/home/tester/.cairn/scratch",
            "/run",
            "relative/dir",
        ] {
            assert!(
                is_shared_socket_root_in(Path::new(shared), &roots),
                "{shared} must be refused"
            );
        }
        // A directory beneath a shared root is the job's own: a cell scratch
        // under the scratch root, or a test's tempdir under /tmp.
        for own in [
            "/home/tester/.cairn/scratch/job-1",
            "/tmp/cell-abc",
            "/home/tester/.cairn-exec/build-slots/x/.authority/slot-1/scratch",
        ] {
            assert!(
                !is_shared_socket_root_in(Path::new(own), &roots),
                "{own} is a job directory"
            );
        }

        // And the live entry point refuses the real temp root.
        let mut policy = SandboxPolicy::for_run(Path::new("/work/wt"), &[], vec![]);
        policy.allow_job_sockets_in(&std::env::temp_dir());
        policy.allow_job_sockets_in(&cairn_common::scratch::scratch_root());
        assert_eq!(
            policy.socket_resolvable_paths(),
            vec![PathBuf::from("/work/wt")]
        );
    }

    #[test]
    fn for_run_grant_is_writable_and_read_reallowed() {
        let wt = PathBuf::from("/work/wt");
        let deny = vec![PathBuf::from("/home/x/.aws"), PathBuf::from("/secret/data")];
        let granted = vec!["/secret/data".to_string()];
        let policy = SandboxPolicy::for_run(&wt, &granted, deny);

        // Granted path is writable (and re-allowed for reads via the writable
        // set), so it wins over the denylist entry without mutating the denylist.
        assert!(policy
            .writable_extra
            .contains(&PathBuf::from("/secret/data")));
        assert!(policy
            .writable_paths()
            .contains(&PathBuf::from("/secret/data")));
        assert!(policy.deny_read.contains(&PathBuf::from("/secret/data")));
        assert!(policy.deny_read.contains(&PathBuf::from("/home/x/.aws")));
        // Worktree is always writable (and thus readable via the re-allow).
        assert!(policy.writable_paths().contains(&wt));
    }

    #[test]
    fn for_readonly_checkout_drops_cwd_from_writable_but_keeps_it_readable() {
        let checkout = PathBuf::from("/project/live");
        let granted = vec!["/scratch/ok".to_string()];
        let policy = SandboxPolicy::for_readonly_checkout(&checkout, &granted, vec![]);

        // The live checkout is NOT writable: a write into it is kernel-denied,
        // even though it is the policy's `worktree` root.
        assert!(
            !policy.writable_paths().contains(&checkout),
            "the live checkout must be dropped from the writable set"
        );
        // But it stays readable so the agent can read project source.
        assert!(
            policy.readable_paths().contains(&checkout),
            "the live checkout must stay in the readable set"
        );
        // Session grants and the temp/toolchain carve-outs remain writable.
        assert!(policy
            .writable_paths()
            .contains(&PathBuf::from("/scratch/ok")));
        for p in default_writable_extra() {
            assert!(
                policy.writable_paths().contains(&p),
                "toolchain/temp carve-out must stay writable: {}",
                p.display()
            );
        }
    }

    /// Cairn's scratch root holds every job's bookkeeping directory. Granted
    /// whole, it let an agent make a sibling workspace beside its own that
    /// nothing owned; a spawn is granted only its own `$TMPDIR` there.
    #[test]
    fn the_scratch_root_is_never_in_a_default_writable_set() {
        // The root itself is what used to be pushed. (A test process's root may
        // sit under a temp dir the fence grants for unrelated reasons, so this
        // asks for the root by name rather than for any ancestor.)
        let scratch = cairn_common::scratch::scratch_root();
        let covers_root = |paths: &[PathBuf]| paths.contains(&scratch);
        assert!(
            !covers_root(&default_writable_extra()),
            "the scratch root must not be writable by default: {}",
            scratch.display()
        );
        let wt = Path::new("/project/wt");
        for (name, policy) in [
            ("for_run", SandboxPolicy::for_run(wt, &[], vec![])),
            (
                "for_readonly_checkout",
                SandboxPolicy::for_readonly_checkout(wt, &[], vec![]),
            ),
            (
                "for_service",
                SandboxPolicy::for_service(Path::new("/svc/state"), &[], vec![]),
            ),
        ] {
            assert!(
                !covers_root(&policy.writable_paths()),
                "{name} must not carry the scratch root into its writable set"
            );
            // Reads stay broad: nothing denies the root, so it is still read.
            assert!(matches!(policy.reach.reads, ReadScope::Broad), "{name}");
            assert!(
                !policy
                    .deny_read
                    .iter()
                    .any(|denied| scratch.starts_with(denied)),
                "{name}"
            );
        }
    }

    #[test]
    fn grant_tmpdir_opens_exactly_that_directory() {
        let scratch = cairn_common::scratch::scratch_root();
        let own = scratch.join("P.1.1.builder");
        let mut policy = SandboxPolicy::for_run(Path::new("/project/wt"), &[], vec![]);
        policy.grant_tmpdir(&own);
        policy.grant_tmpdir(&own);

        assert_eq!(
            policy
                .writable_paths()
                .iter()
                .filter(|p| **p == own)
                .count(),
            1,
            "granted once, however many seams name it"
        );
        assert!(policy.socket_resolvable_paths().contains(&own));
        assert!(!policy.writable_paths().contains(&scratch));
        assert!(!policy.socket_resolvable_paths().contains(&scratch));
    }

    #[test]
    fn grant_tmpdir_never_reopens_a_readonly_checkout() {
        let checkout = PathBuf::from("/project/live");
        let mut policy = SandboxPolicy::for_readonly_checkout(&checkout, &[], vec![]);
        policy.grant_tmpdir(&checkout.join("tmp"));
        assert!(!policy.writable_paths().contains(&checkout.join("tmp")));
    }

    /// Cairn's own log directory must never join the writable set, however
    /// tempting the nested `cairn` CLI makes it.
    ///
    /// A fenced spawn confines an agent's whole process tree, not the one Cairn
    /// binary inside it, so a grant meant to let the CLI keep its log would hand
    /// arbitrary branch code the ability to truncate, delete, or fabricate the
    /// durable logs of the desktop app, the runner, and every concurrent job.
    /// `default_writable_extra` is also what the `write` verb classifies
    /// absolute targets against, and that check is ancestor-based, so every file
    /// beneath the directory would become writable with no prompt on that
    /// surface too. Logs are the evidence trail bug reports are built from;
    /// shared durable state stays protected even from a well-motivated grant.
    ///
    /// Nothing is lost by denying it. A CLI whose log destination is unwritable
    /// still runs — `cairn_common::logging::init` degrades to a fileless logger
    /// — and the CLI is a thin client that forwards to the app, so the
    /// substantive record of what a call did is written by that unfenced process
    /// regardless.
    #[test]
    fn the_cairn_log_dir_is_never_writable() {
        // Derived from the home rather than through `cairn_log_dir`, whose
        // `CAIRN_LOG_DIR` override could name a temp dir the fence grants for
        // unrelated reasons. This is the canonical host location being pinned.
        let logs = cairn_common::paths::cairn_home().join("logs");
        assert!(
            !default_writable_extra().contains(&logs),
            "the Cairn log directory must not be writable: {}",
            logs.display()
        );
        let policy = SandboxPolicy::for_run(Path::new("/project/wt"), &[], vec![]);
        assert!(
            !policy.writable_paths().contains(&logs),
            "no run policy may carry the log directory into its writable set"
        );
    }

    #[test]
    fn for_readonly_checkout_drops_checkout_covering_grants() {
        // No session grant — not even one for the checkout itself, a path inside
        // it, or an ancestor whose subpath grant would include it — may re-open
        // the read-only checkout to writes.
        let checkout = PathBuf::from("/project/live");
        let granted = vec![
            "/project/live".to_string(),     // the checkout itself
            "/project/live/src".to_string(), // inside the checkout
            "/project".to_string(),          // ancestor (subpath would include it)
            "/scratch/ok".to_string(),       // safely outside
        ];
        let policy = SandboxPolicy::for_readonly_checkout(&checkout, &granted, vec![]);
        let writable = policy.writable_paths();
        assert!(!writable.contains(&checkout));
        assert!(!writable.contains(&PathBuf::from("/project/live/src")));
        assert!(!writable.contains(&PathBuf::from("/project")));
        // A safely-outside grant still applies.
        assert!(writable.contains(&PathBuf::from("/scratch/ok")));
        // And the checkout stays readable throughout.
        assert!(policy.readable_paths().contains(&checkout));
    }

    #[test]
    fn for_readonly_checkout_drops_overlapping_default_writable_roots() {
        let temp = tempfile::tempdir().unwrap();
        let checkout = temp.path().join("cairn-readonly-checkout");
        std::fs::create_dir(&checkout).unwrap();
        let policy = SandboxPolicy::for_readonly_checkout(&checkout, &[], vec![]);
        assert!(policy
            .writable_paths()
            .iter()
            .all(|path| !(path.starts_with(&checkout) || checkout.starts_with(path))));
        assert!(policy.readable_paths().contains(&checkout));
    }

    #[test]
    fn only_an_existing_absolute_tool_cache_is_granted() {
        let temp = tempfile::tempdir().unwrap();
        let cache = temp
            .path()
            .join(cairn_common::executor_paths::TOOL_CACHE_DIR);
        assert_eq!(
            tool_cache_grant_from(Some(cache.clone().into())),
            None,
            "a cache that does not exist yet grants nothing"
        );
        std::fs::create_dir(&cache).unwrap();
        assert_eq!(
            tool_cache_grant_from(Some(cache.clone().into())),
            Some(cache)
        );
        assert_eq!(tool_cache_grant_from(None), None);
        assert_eq!(
            tool_cache_grant_from(Some("tool-cache".into())),
            None,
            "a relative value would resolve against each spawn's cwd"
        );
    }

    #[test]
    fn a_tool_cache_variable_naming_another_directory_grants_nothing() {
        // The variable is read from this process's own environment, which a
        // desktop app inherits from whatever shell launched it. A home
        // directory is the case that matters: granting it would hand every
        // fenced spawn the whole home.
        let home = tempfile::tempdir().unwrap();
        assert_eq!(
            tool_cache_grant_from(Some(home.path().as_os_str().to_owned())),
            None
        );
    }

    #[test]
    fn for_run_keeps_cwd_writable_and_readable() {
        let wt = PathBuf::from("/work/wt");
        let policy = SandboxPolicy::for_run(&wt, &[], vec![]);
        assert!(policy.writable_paths().contains(&wt));
        assert!(policy.readable_paths().contains(&wt));
    }

    #[test]
    fn no_denial_on_success_or_unrelated_failure_without_signature() {
        let t = SystemTime::now();
        // Clean exit with no signature: never a denial.
        assert_eq!(detect_denial(Some(0), "all good", None, t, false), None);
        assert_eq!(detect_denial(Some(0), "all good", None, t, true), None);
        // Non-zero but no signature: a normal failure, not a fence.
        assert_eq!(
            detect_denial(Some(1), "test failed: 2 errors", None, t, false),
            None
        );
        assert_eq!(
            detect_denial(Some(1), "test failed: 2 errors", None, t, true),
            None
        );
    }

    #[test]
    fn streaming_signature_helper_detects_known_denials() {
        assert!(has_denial_signature(
            "bash: /tmp/x: Operation not permitted"
        ));
        assert!(has_denial_signature("Permission denied"));
        assert!(has_denial_signature("EACCES"));
        assert!(has_denial_signature("EPERM"));
        // Go's `op path: errno-text`; this is rootless podman under the Linux
        // fence, verbatim (fedora, exit 125).
        assert!(has_denial_signature(PODMAN_FENCED_RUNTIME_LOCK));
        assert!(has_denial_signature("mount /x: operation not permitted"));
        assert!(!has_denial_signature("ordinary test failure"));
        assert!(!has_denial_signature("denied by policy: 2 tests"));
        // Lowercase prose without Go's colon form is not a denial; an Ask
        // spawn is scanned even when it exits 0.
        assert!(!has_denial_signature(
            "returns permission denied when the token expires"
        ));
        assert!(!has_denial_signature("rg 'permission denied' src"));
        // The symbolic errno names are uppercase tokens, not prose.
        assert!(!has_denial_signature("eperm and eacces in a sentence"));
        assert!(!has_denial_signature(""));
    }

    /// GNU sed 4.9's in-place edit on an SELinux-enforcing host under the Linux
    /// fence, verbatim (fedora). The file was edited and sed exited 0.
    const SELINUX_SED_IN_PLACE_WARNING: &str = "sed: warning: failed to set default file creation context to unconfined_u:object_r:user_home_t:s0: Permission denied";

    #[test]
    fn an_selinux_creation_context_warning_is_not_a_denial() {
        let t = SystemTime::now();
        assert!(!has_denial_signature(SELINUX_SED_IN_PLACE_WARNING));
        assert_eq!(
            detect_denial(Some(0), SELINUX_SED_IN_PLACE_WARNING, None, t, true),
            None,
            "a successful in-place edit must not raise a crossing"
        );
        // The warning only excuses its own line: a real denial elsewhere in
        // the same output still counts.
        let alongside =
            format!("{SELINUX_SED_IN_PLACE_WARNING}\nbash: /etc/hosts: Permission denied\n");
        assert!(has_denial_signature(&alongside));
        // GNU sed misreading BSD's `-i ''`: the real failure is a missing
        // file, which is the agent's to fix, not the fence's.
        let bsd_syntax = format!(
            "sed: can't read s/a/b/: No such file or directory\n{SELINUX_SED_IN_PLACE_WARNING}"
        );
        assert_eq!(detect_denial(Some(2), &bsd_syntax, None, t, true), None);
    }

    const PODMAN_FENCED_RUNTIME_LOCK: &str = "Error: acquiring runtime init lock: open /run/user/1000/libpod/tmp/alive.lck: open /run/user/1000/libpod/tmp/alive.lck: permission denied";

    #[test]
    fn ask_fallback_detects_denial_even_when_shell_exit_is_masked() {
        let t = SystemTime::now();
        let output = "bash: /Users/u/probe: Operation not permitted\nexit=1";
        // Ask fallback still prompts: `cmd; echo exit=$?` can make the shell's
        // final status zero even though the sandbox blocked an earlier write.
        assert_eq!(
            detect_denial(Some(0), output, None, t, true),
            Some(SandboxDenial::Command)
        );
        // Deny/no-fallback preserves raw output for clean final exits.
        assert_eq!(detect_denial(Some(0), output, None, t, false), None);
    }

    #[cfg(not(target_os = "macos"))]
    #[test]
    fn denial_signature_without_path_is_command_scoped() {
        let t = SystemTime::now();
        // No out-of-band signal: the signature is the trigger (command-scoped).
        assert_eq!(
            detect_denial(
                Some(1),
                "bash: /etc/x: Operation not permitted",
                None,
                t,
                false,
            ),
            Some(SandboxDenial::Command)
        );
        assert_eq!(
            detect_denial(Some(13), "open: Permission denied", None, t, true),
            Some(SandboxDenial::Command)
        );
        // A container engine refused by the fence raises a crossing, which is
        // how the agent learns why it failed; it names no path to approve.
        assert_eq!(
            detect_denial(Some(125), PODMAN_FENCED_RUNTIME_LOCK, None, t, true),
            Some(SandboxDenial::Command)
        );
        assert_eq!(
            detect_denial(Some(125), PODMAN_FENCED_RUNTIME_LOCK, None, t, false),
            Some(SandboxDenial::Command)
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_signature_without_logged_violation_is_ask_fallback_only() {
        let t = SystemTime::now();
        // Ask agents get a recoverable command-scoped crossing when the unified
        // log path lookup misses (represented here by an untagged spawn).
        assert_eq!(
            detect_denial(
                Some(1),
                "bash: /etc/x: Operation not permitted",
                None,
                t,
                true,
            ),
            Some(SandboxDenial::Command)
        );
        // Deny agents preserve the old fail-fast raw-output behavior.
        assert_eq!(
            detect_denial(
                Some(1),
                "bash: /etc/x: Operation not permitted",
                None,
                t,
                false,
            ),
            None
        );
    }

    #[test]
    fn the_reported_denial_line_is_the_first_signature_line_bounded() {
        let output = "fetching\nerror: cannot open .git/FETCH_HEAD: Permission denied\nfatal: later: Operation not permitted";
        assert_eq!(
            reported_denial_line(output).as_deref(),
            Some("error: cannot open .git/FETCH_HEAD: Permission denied")
        );
        assert_eq!(reported_denial_line("all good\n"), None);
        let long = format!("{}: Permission denied", "x".repeat(400));
        let bounded = reported_denial_line(&long).unwrap();
        assert_eq!(bounded.chars().count(), 241);
        assert!(bounded.ends_with('…'));
    }

    #[test]
    fn every_policy_gets_its_own_violation_tag() {
        let wt = Path::new("/work/wt");
        let a = SandboxPolicy::for_run(wt, &[], vec![]);
        let b = SandboxPolicy::for_run(wt, &[], vec![]);
        assert_ne!(a.violation_tag, b.violation_tag);
        // SBPL embeds the tag in a string literal; it must need no escaping.
        assert!(a
            .violation_tag
            .as_str()
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-'));
    }

    #[test]
    fn the_enforcement_fingerprint_is_stable_across_reads() {
        assert_eq!(enforcement_description(), enforcement_description());
    }

    #[test]
    fn toolchain_caches_are_writable_when_present() {
        // Pure: cargo/npm caches present in a home → they are in the writable set,
        // so cold-cache / dependency-adding builds are not kernel-denied.
        let home = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(home.path().join(".cargo")).unwrap();
        std::fs::create_dir_all(home.path().join(".npm")).unwrap();

        let dirs = toolchain_writable_dirs_in(home.path());
        assert!(dirs.contains(&home.path().join(".cargo")));
        assert!(dirs.contains(&home.path().join(".npm")));
        // A cache dir that doesn't exist is not emitted (inert in the rules).
        assert!(!dirs.contains(&home.path().join(".gradle")));
    }

    #[test]
    fn glob_to_regex_converts_glob_semantics() {
        // `**` spans path separators; `*` stays within a segment; `.` is escaped.
        assert_eq!(
            glob_to_regex("/home/u/.cairn/worktrees/**/target/**"),
            "^/home/u/\\.cairn/worktrees/.*/target/.*"
        );
        assert_eq!(glob_to_regex("/a/*/b"), "^/a/[^/]*/b");
        // A concrete worktree target path matches the worktrees glob.
        let re =
            regex::Regex::new(&glob_to_regex("/home/u/.cairn/worktrees/**/target/**")).unwrap();
        assert!(re.is_match("/home/u/.cairn/worktrees/CAIRN-1/src-tauri/target/release/deps/x.d"));
        // A worktree source path does NOT match (writes there stay denied).
        assert!(!re.is_match("/home/u/.cairn/worktrees/CAIRN-1/src-tauri/src/lib.rs"));
    }

    #[test]
    fn for_service_grants_state_dir_and_globs_only() {
        let state = PathBuf::from("/home/u/.cairn/sccache");
        let policy = SandboxPolicy::for_service(
            &state,
            &["/home/u/.cairn/worktrees/**/target/**".to_string()],
            vec![PathBuf::from("/home/u/.aws")],
        );
        // State dir is writable+readable; the worktrees glob is a regex grant.
        assert!(policy.writable_paths().contains(&state));
        assert_eq!(
            policy.writable_regex,
            vec!["^/home/u/\\.cairn/worktrees/.*/target/.*".to_string()]
        );
        // Secret store stays denied.
        assert!(policy.deny_read.contains(&PathBuf::from("/home/u/.aws")));
    }

    #[test]
    fn for_run_ignores_non_path_grants() {
        // A normalized-command descriptor (no leading slash) is not a path grant.
        let policy = SandboxPolicy::for_run(
            Path::new("/work/wt"),
            &["sudo rm -rf /".to_string()],
            vec![PathBuf::from("/home/x/.aws")],
        );
        assert!(!policy
            .writable_extra
            .contains(&PathBuf::from("sudo rm -rf /")));
        assert!(policy.deny_read.contains(&PathBuf::from("/home/x/.aws")));
    }
}
