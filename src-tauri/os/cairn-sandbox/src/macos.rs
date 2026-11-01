//! macOS sandbox: Seatbelt SBPL profile generation + `sandbox-exec` wrapping.
//!
//! The profile uses last-matching-rule-wins semantics to express:
//!   - allow everything by default (so the toolchain's broad reads/execs work),
//!   - deny all filesystem writes,
//!   - re-allow writes under the worktree, temp dirs, granted crossings, /dev,
//!   - deny reads of the sensitive denylist,
//!   - and, last, the operator credential boundary, which nothing re-allows.

use super::{
    escape_sbpl, Confinement, NestedSandboxWriteProbe, OperatorCredentialFloor, SandboxPolicy,
};
use cairn_common::executor_protocol::{
    ViolationRecoveryEvidence, ViolationRecoveryOutcome, ViolationStreamAccess,
};
use std::collections::VecDeque;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::RecvTimeoutError;
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime};

pub(crate) fn current_process_is_confined() -> bool {
    process_confinement().unwrap_or(true)
}

/// Whether this process runs under an inherited profile, or `None` when the
/// question could not be asked because `sandbox-exec` did not launch.
///
/// A profile cannot be removed from a running process, so the answer holds for
/// the life of the process and is measured once.
pub(crate) fn process_confinement() -> Option<bool> {
    static CONFINED: OnceLock<Option<bool>> = OnceLock::new();
    *CONFINED.get_or_init(|| {
        // Seatbelt profiles are inherited and cannot be replaced. Applying an
        // allow-default profile in a disposable child therefore succeeds only
        // when the parent is unconfined; a child of a sandboxed process exits
        // because sandbox_apply is not permitted.
        //
        // Do not use the private sandbox_check API here. On current macOS it
        // reports the platform's baseline policy as well as an inherited
        // process profile: even an ordinary shell receives "denied" for a
        // file-create query it can perform successfully.
        Command::new("/usr/bin/sandbox-exec")
            .args(["-p", "(version 1)(allow default)", "/usr/bin/true"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .ok()
            .map(|status| !status.success())
    })
}

/// Spawn a command exactly as a spawn under `outer` is wrapped, and inside it
/// apply a nested profile exactly as a fenced agent command is wrapped, then
/// attempt a write that nested profile permits.
///
/// That is what a sandbox test running under `outer` does, so this measures
/// the workload's own path rather than this process's.
pub(crate) fn probe_nested_sandbox_write_under(outer: &Confinement) -> NestedSandboxWriteProbe {
    let outcome = tempfile::tempdir().map(|dir| {
        let root = dir
            .path()
            .canonicalize()
            .unwrap_or_else(|_| dir.path().into());
        let probe = root.join("probe");
        let inner = Confinement::Fenced(SandboxPolicy::for_run(&root, &[], Vec::new()));
        let (inner_program, inner_args) = inner.wrap_argv(
            "/bin/sh",
            &["-c".to_string(), "echo ok > probe".to_string()],
        );
        let (program, args) = outer.wrap_argv(&inner_program, &inner_args);
        let mut command = Command::new(&program);
        command.args(&args).current_dir(&root).stdin(Stdio::null());
        outer.install_pre_exec(&mut command);
        match command.output() {
            Ok(output) if output.status.success() && probe.exists() => NestedSandboxWriteProbe {
                available: true,
                apply_refused: false,
                detail: "an in-bounds temp write succeeded under a nested Seatbelt profile".into(),
            },
            Ok(output) => {
                let stderr = String::from_utf8_lossy(&output.stderr);
                NestedSandboxWriteProbe {
                    available: false,
                    apply_refused: output.status.code() == Some(71)
                        && stderr.contains("sandbox_apply"),
                    detail: format!(
                        "nested Seatbelt in-bounds temp write refused (exit {:?}): {}",
                        output.status.code(),
                        stderr.lines().next().unwrap_or("no stderr")
                    ),
                }
            }
            Err(error) => NestedSandboxWriteProbe {
                available: false,
                apply_refused: false,
                detail: format!("could not launch nested Seatbelt probe: {error}"),
            },
        }
    });
    outcome.unwrap_or_else(|error| NestedSandboxWriteProbe {
        available: false,
        apply_refused: false,
        detail: format!("could not create nested Seatbelt probe directory: {error}"),
    })
}

/// Generate the SBPL profile string for a policy, closed by the operator
/// credential boundary `floor` describes.
pub(crate) fn sbpl_profile(policy: &SandboxPolicy, floor: &OperatorCredentialFloor) -> String {
    let mut out = String::new();
    out.push_str("(version 1)\n");
    out.push_str("(allow default)\n");
    // Every fence deny carries the spawn's tag into the violation log, which is
    // how a denial made by any descendant is found again (see `ViolationTag`).
    let report = format!("(with message \"{}\")", policy.violation_tag.as_str());
    // Confine writes: deny everything, then re-allow the writable set.
    out.push_str(&format!("(deny file-write* (subpath \"/\") {report})\n"));

    let mut writable: Vec<PathBuf> = policy.writable_paths();
    // /dev is I/O plumbing (tty, null, fd) and must stay writable.
    writable.push(PathBuf::from("/dev"));

    // Combine concrete subpath grants with any regex grants (service sandboxes
    // express cross-worktree scopes like `{worktrees}/**/target/**` as regexes
    // because the matching dirs do not all exist at profile-build time).
    let writable_rules = join_rules(
        &subpath_rules(&writable),
        &regex_rules(&policy.writable_regex),
    );
    if !writable_rules.is_empty() {
        out.push_str(&format!("(allow file-write* {})\n", writable_rules));
    }

    match &policy.reach.reads {
        crate::ReadScope::Broad => {
            // Hard-deny reads of the sensitive denylist.
            let deny_rules = subpath_rules(&policy.deny_read);
            if !deny_rules.is_empty() {
                out.push_str(&format!("(deny file-read* {deny_rules} {report})\n"));
            }

            // Re-allow reads of the readable set LAST (last-match-wins) so a
            // denylist entry (e.g. a user-configured one covering a prefix of
            // the worktree) can never block the agent from reading its own
            // source. The readable set keeps the worktree even for a
            // read-only-checkout policy, whose writable set drops it — so a
            // non-worktree live checkout stays readable while its writes are
            // denied. Regex write scopes are re-allowed for reads too.
            let mut readable: Vec<PathBuf> = policy.readable_paths();
            readable.push(PathBuf::from("/dev"));
            let read_allow_rules = join_rules(
                &subpath_rules(&readable),
                &regex_rules(&policy.writable_regex),
            );
            if !read_allow_rules.is_empty() {
                out.push_str(&format!("(allow file-read* {})\n", read_allow_rules));
            }
        }
        crate::ReadScope::Allowlist { roots } => {
            // Closed by default; metadata stays visible so the loader can
            // resolve paths, and the root node itself is readable because
            // bun opens `/` at startup and aborts when refused (measured by
            // `bun_starts_under_the_isolated_profile_live`). It shows the
            // names of the top-level directories and nothing in them.
            out.push_str(&format!("(deny file-read* (subpath \"/\") {report})\n"));
            out.push_str("(allow file-read-metadata)\n");
            out.push_str("(allow file-read* (literal \"/\"))\n");
            let mut readable: Vec<PathBuf> = roots.clone();
            readable.extend(policy.writable_paths());
            readable.push(PathBuf::from("/dev"));
            out.push_str(&format!(
                "(allow file-read* {})\n",
                subpath_rules(&readable)
            ));
        }
    }

    if let crate::NetworkScope::RelayOnly { port } = policy.reach.network {
        // One socket: a TCP connection to the executor's relay. Every other
        // socket, unix ones included, and every Mach service lookup is refused,
        // which is what keeps the runner's loopback API out of reach.
        out.push_str(&format!("(deny network* {report})\n"));
        out.push_str(&format!(
            "(allow network-outbound (remote ip \"localhost:{port}\"))\n"
        ));
        out.push_str(&format!("(deny mach-lookup {report})\n"));
    }

    // A page-call batch shares its uid with every other batch and resident on
    // the machine, and each of those carries its own relay capability in its
    // environment. `(allow default)` leaves the process table open, so the
    // fence must close the three ways one process reads or disturbs another:
    // `process-info*` (enumeration and `proc_pidinfo`), the `kern.procargs2`
    // / `kern.procargs` sysctls (which return a process's whole argument and
    // environment block), and `signal`. Scoped to the allow-list read shape,
    // which is the page-call policy and nothing else; a runtime that needs
    // its own procargs fails the probe's positive control and its machine is
    // refused rather than admitted, so denying it even for self is fail-closed.
    if matches!(policy.reach.reads, crate::ReadScope::Allowlist { .. }) {
        out.push_str(&format!("(deny process-info* (target others) {report})\n"));
        out.push_str(&format!(
            "(deny sysctl-read (sysctl-name \"kern.procargs2\") (sysctl-name \"kern.procargs\") {report})\n"
        ));
        out.push_str(&format!("(deny signal (target others) {report})\n"));
    }

    // The operator credential boundary goes after every allow above, so no
    // worktree, grant, or read re-allow can reopen it.
    out.push_str(&floor.sbpl_rules());

    out
}

/// Join two space-separated SBPL clause runs, dropping empties.
fn join_rules(a: &str, b: &str) -> String {
    match (a.is_empty(), b.is_empty()) {
        (true, true) => String::new(),
        (false, true) => a.to_string(),
        (true, false) => b.to_string(),
        (false, false) => format!("{a} {b}"),
    }
}

/// Build a space-joined run of `(regex #"...")` clauses. The patterns are
/// already anchored regex strings (see `super::glob_to_regex`); only the SBPL
/// string delimiter `"` is escaped — backslashes are intentional regex escapes.
fn regex_rules(patterns: &[String]) -> String {
    let mut seen: Vec<String> = Vec::new();
    for p in patterns {
        let escaped = p.replace('"', "\\\"");
        let clause = format!("(regex #\"{}\")", escaped);
        if !seen.contains(&clause) {
            seen.push(clause);
        }
    }
    seen.join(" ")
}

/// Rewrite argv to run under `sandbox-exec` with the generated profile.
///
/// Also makes sure the violation stream is watching, since it can only report
/// a denial made while it was running (see [`detect_violation`]).
pub(crate) fn wrap_argv(
    program: &str,
    args: &[String],
    policy: &SandboxPolicy,
) -> (String, Vec<String>) {
    let _ = ViolationStream::watching();
    wrap_argv_with_profile(
        program,
        args,
        sbpl_profile(policy, &OperatorCredentialFloor::for_this_machine()),
    )
}

/// Rewrite argv to run under an already-generated SBPL profile.
pub(crate) fn wrap_argv_with_profile(
    program: &str,
    args: &[String],
    profile: String,
) -> (String, Vec<String>) {
    let mut wrapped = vec!["-p".to_string(), profile, program.to_string()];
    wrapped.extend(args.iter().cloned());
    ("/usr/bin/sandbox-exec".to_string(), wrapped)
}

/// Build a space-joined run of `(subpath "...")` clauses for canonicalized
/// paths, each literal escaped for SBPL. A path that does not exist yet still
/// gets a clause: the rule is inert until something appears there, and emitting
/// it is what lets a confined process create a path the policy has granted.
fn subpath_rules(paths: &[PathBuf]) -> String {
    let mut seen: Vec<String> = Vec::new();
    for p in paths {
        let canonical = canonicalize_best_effort(p);
        let literal = escape_sbpl(&canonical.to_string_lossy());
        let clause = format!("(subpath \"{}\")", literal);
        if !seen.contains(&clause) {
            seen.push(clause);
        }
    }
    seen.join(" ")
}

/// Canonicalize a path if it exists; otherwise return it unchanged. SBPL
/// `subpath` matches the literal prefix, so a stable absolute path is what
/// matters — canonicalization mainly resolves macOS `/tmp` → `/private/tmp`
/// and `/var` → `/private/var` symlinks so the rule matches the real path the
/// kernel reports.
fn canonicalize_best_effort(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

/// A sandbox violation recovered from the unified log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxViolation {
    pub(crate) operation: String,
    pub(crate) path: PathBuf,
}

/// Recover the denied path for a violation that carries `tag`, made since
/// `since`. Best-effort: `None` when no violation stream is running or none
/// arrives in time (the caller then falls back to a command-scoped denial).
///
/// The kernel logs a violation as process 0, naming the process that made the
/// denied call only inside the message, so the spawned pid cannot select it;
/// the tag the spawn's profile attached to its deny rules does. `since` keeps
/// apart spawns that share one policy, and so one tag.
pub(crate) fn detect_violation(
    tag: &crate::ViolationTag,
    since: SystemTime,
) -> Option<SandboxViolation> {
    // A stream started now could not have seen the denial, so only one that
    // was already watching is asked.
    let stream = ViolationStream::current()?;
    stream.await_tagged(tag.as_str(), since, VIOLATION_ARRIVAL_WAIT)
}

/// How long a denial waits for its violation to arrive on the stream. The
/// kernel's entry normally lands well inside this; waiting is paid only by a
/// failed command whose output already reads as a permission denial.
const VIOLATION_ARRIVAL_WAIT: Duration = Duration::from_millis(1500);

/// What the kernel's Seatbelt extension is logged as sending from.
const KERNEL_SANDBOX_SENDER: &str = "/Sandbox";

/// A command that runs `argv` and stops it when this process exits.
///
/// macOS has no parent-death signal, so a long-lived child such as `log stream`
/// would outlive this process until its next matching entry. The wrapper ties
/// it to this process's lifetime instead: a background reader blocks on the
/// wrapper's stdin, which the caller must keep open and never write, and kills
/// the command when that pipe closes. The command's stdout and stderr are the
/// wrapper's, so a command that exits on its own still closes the caller's end
/// and a refusal it prints reaches the caller; the killer's own output goes to
/// /dev/null.
///
/// When the command is `sudo`, the kill lands on sudo, because the root `log`
/// beneath it is not this account's to signal, and sudo relays it. sudo drops
/// a signal sent from a process group that sudo or its command leads, so a
/// command cannot kill itself through it (`exec_nopty.c`, sudo 1.9.17). The
/// killer is in neither: without job control the wrapper starts the command in
/// the caller's own group, which sudo does not lead. Starting the command with
/// `set -m` or in a group of its own would make the kill silently inert.
fn tied_to_this_process(argv: &[&str]) -> Command {
    const WRAPPER: &str = r#"exec 3<&0
"$@" </dev/null &
l=$!
{ read -r _ <&3; kill "$l"; } >/dev/null 2>&1 &
wait "$l""#;
    let mut command = Command::new("/bin/sh");
    command
        .args(["-c", WRAPPER, "sh"])
        .args(argv)
        .stdin(Stdio::piped());
    command
}

/// How this account reaches `log stream`.
fn stream_access() -> ViolationStreamAccess {
    if account_is_admin() {
        ViolationStreamAccess::Admin
    } else {
        ViolationStreamAccess::SudoRule
    }
}

/// Whether this process's effective user is in `admin`, as the directory
/// service answers now.
///
/// Not this process's own group list: the kernel fixes that when the process
/// tree's login began, so an account taken out of `admin` keeps it in every
/// process a running daemon starts, while `log` asks the directory service and
/// refuses. Membership that cannot be read counts as not admin, and the sudo
/// path then either works or says why it did not.
fn account_is_admin() -> bool {
    // <membership.h>, in libSystem.
    extern "C" {
        fn mbr_uid_to_uuid(uid: libc::uid_t, uu: *mut u8) -> libc::c_int;
        fn mbr_gid_to_uuid(gid: libc::gid_t, uu: *mut u8) -> libc::c_int;
        fn mbr_check_membership(
            user: *const u8,
            group: *const u8,
            ismember: *mut libc::c_int,
        ) -> libc::c_int;
    }
    let Some(admin) = group_id(c"admin") else {
        return false;
    };
    let (mut user, mut group, mut member) = ([0u8; 16], [0u8; 16], 0);
    // SAFETY: each uuid buffer is the 16 bytes a `uuid_t` is, and every
    // pointer outlives its call.
    unsafe {
        mbr_uid_to_uuid(libc::geteuid(), user.as_mut_ptr()) == 0
            && mbr_gid_to_uuid(admin, group.as_mut_ptr()) == 0
            && mbr_check_membership(user.as_ptr(), group.as_ptr(), &mut member) == 0
            && member != 0
    }
}

fn group_id(name: &std::ffi::CStr) -> Option<libc::gid_t> {
    // SAFETY: `group` and `buffer` outlive the call, and `found` is only
    // dereferenced when the call set it to point at `group`.
    unsafe {
        let mut group: libc::group = std::mem::zeroed();
        let mut buffer = vec![0 as libc::c_char; 16 * 1024];
        let mut found: *mut libc::group = std::ptr::null_mut();
        let status = libc::getgrnam_r(
            name.as_ptr(),
            &mut group,
            buffer.as_mut_ptr(),
            buffer.len(),
            &mut found,
        );
        (status == 0 && !found.is_null()).then(|| (*found).gr_gid)
    }
}

/// Why the violation stream did not start, in words an operator can act on.
///
/// A non-admin account's stream is refused in two places that need different
/// remedies. Inside a Seatbelt profile the kernel refuses sudo's setuid exec
/// before any rule is consulted (`sh` reports `Operation not permitted` and
/// exits 126), and no rule can fix that; outside one, sudo itself refuses a
/// command no installed rule names, which the operator's setup script fixes.
fn refusal_reason(
    access: ViolationStreamAccess,
    spawned: &str,
    exit: Option<i32>,
    printed: &str,
) -> String {
    let printed = printed.trim();
    let said = if printed.is_empty() {
        "nothing on stderr".to_string()
    } else {
        format!("{printed:?}")
    };
    let ended = exit.map_or_else(
        || "was stopped by a signal".to_string(),
        |code| format!("exited {code}"),
    );
    match access {
        ViolationStreamAccess::SudoRule
            if exit == Some(126) || printed.contains("Operation not permitted") =>
        {
            format!(
                "the fence refused sudo: this process runs under a Seatbelt profile, which refuses sudo before any sudoers rule is consulted, so it cannot start the violation stream (`{spawned}` {ended}: {said})"
            )
        }
        ViolationStreamAccess::SudoRule
            if printed.contains("password is required")
                || printed.contains("not allowed to execute") =>
        {
            format!(
                "sudo rule absent: this executor account is not in admin and no installed sudoers rule permits it exactly `{}` (`{spawned}` {ended}: {said}); the operator installs the rule with `sudo scripts/setup-self-hosted-runner-macos.sh --executor-user <account>`",
                crate::violation_stream_sudoers_command()
            )
        }
        ViolationStreamAccess::SudoRule => {
            format!("sudo did not start the violation stream: `{spawned}` {ended}: {said}")
        }
        ViolationStreamAccess::Admin => {
            format!("the violation stream did not start: `{spawned}` {ended}: {said}")
        }
    }
}

/// One process-wide `log stream` of tagged kernel Seatbelt violations.
///
/// A stream rather than a `log show` query afterwards: the persisted log does
/// not hold these entries. Asked for a spawn's denials moments later, `log
/// show` returns nothing, top-level process or descendant alike, while a live
/// stream receives them as the kernel emits them.
pub(crate) struct ViolationStream {
    seen: Mutex<Seen>,
    arrived: Condvar,
    alive: AtomicBool,
    access: ViolationStreamAccess,
    /// Whether a canary denial came back through this stream, once asked.
    recovery: Mutex<Option<RecoveryMeasurement>>,
    /// Held, never written: when this process exits the pipe closes, and the
    /// wrapper in [`ViolationStream::start`] stops `log stream` in response.
    _lifeline: Mutex<std::process::ChildStdin>,
}

#[derive(Default)]
struct Seen {
    violations: VecDeque<(SystemTime, String, SandboxViolation)>,
    /// Every tagged kernel denial as `operation target`, path or not (a
    /// `mach-lookup` names a service). For telling a person what a profile
    /// refused; never a crossing's scope, which only `violations` supplies.
    denials: VecDeque<(SystemTime, String, String)>,
    /// Tagged lines that were not kernel violations, kept for a failing live
    /// test to show.
    #[cfg(test)]
    rejected: VecDeque<String>,
}

struct RecoveryMeasurement {
    outcome: ViolationRecoveryOutcome,
    at: Instant,
    at_unix_ms: u64,
}

/// A start that failed: how it was attempted, when, and what refused it.
#[derive(Clone)]
struct StreamRefusal {
    access: ViolationStreamAccess,
    at: Instant,
    at_unix_ms: u64,
    reason: String,
}

impl StreamRefusal {
    fn now(access: ViolationStreamAccess, reason: String) -> Self {
        Self {
            access,
            at: Instant::now(),
            at_unix_ms: crate::nested::unix_now_ms(),
            reason,
        }
    }
}

enum StreamSlot {
    Running(Arc<ViolationStream>),
    Refused(StreamRefusal),
}

/// What the stream's reader saw first.
enum StreamStart {
    Announced,
    /// The stream ended before printing anything, with its exit code.
    Ended(Option<i32>),
}

/// The running stream, or why it last failed to start, so a process that
/// cannot stream (a sandboxed one: `log` refuses to run there) does not pay a
/// spawn attempt for every fenced command, and can say why it is blind.
static VIOLATION_STREAM: Mutex<Option<StreamSlot>> = Mutex::new(None);

const STREAM_RETRY_AFTER: Duration = Duration::from_secs(60);
const STREAM_READY_WAIT: Duration = Duration::from_secs(2);
/// How long a stream that has exited gets to finish handing over its stderr.
const COMPLAINT_WAIT: Duration = Duration::from_millis(500);
const COMPLAINT_LIMIT: usize = 4096;
const KEPT_VIOLATIONS: usize = 1024;
const VIOLATION_RETENTION: Duration = Duration::from_secs(600);

impl ViolationStream {
    /// The running stream, starting one when there is none.
    pub(crate) fn watching() -> Option<Arc<Self>> {
        Self::watching_or_refusal().ok()
    }

    fn watching_or_refusal() -> Result<Arc<Self>, StreamRefusal> {
        let mut slot = VIOLATION_STREAM.lock().unwrap_or_else(|p| p.into_inner());
        match slot.as_ref() {
            Some(StreamSlot::Running(stream)) if stream.alive.load(Ordering::Acquire) => {
                return Ok(stream.clone())
            }
            Some(StreamSlot::Refused(refusal)) if refusal.at.elapsed() < STREAM_RETRY_AFTER => {
                return Err(refusal.clone())
            }
            _ => {}
        }
        // Asked per start rather than per spawn: membership is a directory
        // service query, and an account moving in or out of `admin` reaches
        // the next start, which a refused stream makes within a minute.
        let started = Self::start(stream_access());
        *slot = Some(match &started {
            Ok(stream) => StreamSlot::Running(stream.clone()),
            Err(refusal) => StreamSlot::Refused(refusal.clone()),
        });
        started
    }

    fn current() -> Option<Arc<Self>> {
        match VIOLATION_STREAM.lock().ok()?.as_ref()? {
            StreamSlot::Running(stream) => Some(stream.clone()),
            StreamSlot::Refused(_) => None,
        }
    }

    fn start(access: ViolationStreamAccess) -> Result<Arc<Self>, StreamRefusal> {
        let refused = |reason: String| StreamRefusal::now(access, reason);
        let argv = crate::violation_stream_spawn_argv(access);
        let spawned = argv.join(" ");
        let argv: Vec<&str> = argv.iter().map(String::as_str).collect();
        let mut child = tied_to_this_process(&argv)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| refused(format!("could not spawn `{spawned}`: {error}")))?;
        let (Some(lifeline), Some(stdout), Some(stderr)) =
            (child.stdin.take(), child.stdout.take(), child.stderr.take())
        else {
            let _ = child.kill();
            let _ = child.wait();
            return Err(refused(format!("`{spawned}` started without its pipes")));
        };
        let stream = Arc::new(Self {
            seen: Mutex::new(Seen::default()),
            arrived: Condvar::new(),
            alive: AtomicBool::new(true),
            access,
            recovery: Mutex::new(None),
            _lifeline: Mutex::new(lifeline),
        });
        let complaint = complaint_of(stderr);
        let (ready, first_line) = std::sync::mpsc::channel();
        let reader = stream.clone();
        std::thread::Builder::new()
            .name("cairn-sandbox-violations".into())
            .spawn(move || {
                let mut ready = Some(ready);
                for line in BufReader::new(stdout).lines() {
                    let Ok(line) = line else { break };
                    // `log stream` announces its filter before any entry.
                    if let Some(ready) = ready.take() {
                        let _ = ready.send(StreamStart::Announced);
                    }
                    reader.record(&line);
                }
                reader.alive.store(false, Ordering::Release);
                reader.arrived.notify_all();
                let exit = child.wait().ok().and_then(|status| status.code());
                if let Some(ready) = ready {
                    let _ = ready.send(StreamStart::Ended(exit));
                }
            })
            .map_err(|error| refused(format!("could not start the stream's reader: {error}")))?;
        match first_line.recv_timeout(STREAM_READY_WAIT) {
            Ok(StreamStart::Announced) | Err(RecvTimeoutError::Timeout) => {}
            Ok(StreamStart::Ended(exit)) => {
                let printed = complaint.recv_timeout(COMPLAINT_WAIT).unwrap_or_default();
                return Err(refused(refusal_reason(access, &spawned, exit, &printed)));
            }
            Err(RecvTimeoutError::Disconnected) => {
                return Err(refused(format!(
                    "the reader of `{spawned}` stopped before the stream announced itself"
                )))
            }
        }
        if stream.alive.load(Ordering::Acquire) {
            Ok(stream)
        } else {
            Err(refused(format!(
                "`{spawned}` exited just after announcing itself"
            )))
        }
    }

    fn record(&self, line: &str) {
        let Ok(mut seen) = self.seen.lock() else {
            return;
        };
        if let Some((tag, summary)) = parse_denial_summary(line) {
            let now = SystemTime::now();
            while seen.denials.len() >= KEPT_VIOLATIONS
                || seen.denials.front().is_some_and(|(at, ..)| {
                    now.duration_since(*at).unwrap_or_default() > VIOLATION_RETENTION
                })
            {
                seen.denials.pop_front();
            }
            seen.denials.push_back((now, tag, summary));
            self.arrived.notify_all();
        }
        match parse_violation_entry(line) {
            Some((tag, violation)) => {
                let now = SystemTime::now();
                let violations = &mut seen.violations;
                while violations.len() >= KEPT_VIOLATIONS
                    || violations.front().is_some_and(|(at, ..)| {
                        now.duration_since(*at).unwrap_or_default() > VIOLATION_RETENTION
                    })
                {
                    violations.pop_front();
                }
                violations.push_back((now, tag, violation));
                self.arrived.notify_all();
            }
            #[cfg(test)]
            None => {
                if seen.rejected.len() >= 16 {
                    seen.rejected.pop_front();
                }
                seen.rejected.push_back(line.to_string());
            }
            #[cfg(not(test))]
            None => {}
        }
    }

    fn await_tagged(
        &self,
        tag: &str,
        since: SystemTime,
        wait: Duration,
    ) -> Option<SandboxViolation> {
        let deadline = Instant::now() + wait;
        let mut seen = self.seen.lock().ok()?;
        loop {
            if let Some((.., violation)) = seen
                .violations
                .iter()
                .find(|(at, seen_tag, _)| seen_tag == tag && *at >= since)
            {
                return Some(violation.clone());
            }
            let left = deadline.checked_duration_since(Instant::now())?;
            if left.is_zero() || !self.alive.load(Ordering::Acquire) {
                return None;
            }
            seen = self.arrived.wait_timeout(seen, left).ok()?.0;
        }
    }

    /// The distinct denials tagged `tag` since `since`, waiting up to `wait`
    /// for the first to arrive.
    fn tagged_denials(&self, tag: &str, since: SystemTime, wait: Duration) -> Vec<String> {
        let deadline = Instant::now() + wait;
        let Ok(mut seen) = self.seen.lock() else {
            return Vec::new();
        };
        loop {
            let mut found: Vec<String> = Vec::new();
            for (at, seen_tag, summary) in &seen.denials {
                if seen_tag == tag && *at >= since && !found.contains(summary) {
                    found.push(summary.clone());
                }
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if !found.is_empty() || left.is_zero() || !self.alive.load(Ordering::Acquire) {
                return found;
            }
            match self.arrived.wait_timeout(seen, left) {
                Ok((next, _)) => seen = next,
                Err(_) => return found,
            }
        }
    }

    #[cfg(test)]
    fn describe(&self, tag: &str) -> String {
        let Ok(seen) = self.seen.lock() else {
            return "violation stream state poisoned".into();
        };
        let ours: Vec<_> = seen
            .violations
            .iter()
            .filter(|(_, t, _)| t == tag)
            .collect();
        format!(
            "stream alive={}, {} violation(s) recorded, ours {ours:?}, rejected tagged lines {:?}",
            self.alive.load(Ordering::Acquire),
            seen.violations.len(),
            seen.rejected
        )
    }
}

/// Collect what the stream prints on stderr, bounded, and hand it over once the
/// stream closes it: a refusal's own words are the most specific reason there is.
fn complaint_of(stderr: std::process::ChildStderr) -> std::sync::mpsc::Receiver<String> {
    let (complaint, received) = std::sync::mpsc::channel();
    let _ = std::thread::Builder::new()
        .name("cairn-sandbox-violations-stderr".into())
        .spawn(move || {
            let mut kept = String::new();
            for line in BufReader::new(stderr).lines() {
                let Ok(line) = line else { break };
                if kept.len() < COMPLAINT_LIMIT {
                    if !kept.is_empty() {
                        kept.push('\n');
                    }
                    kept.push_str(&line);
                }
            }
            let _ = complaint.send(kept);
        });
    received
}

/// Whether a fenced spawn's denial is recovered by path on this machine,
/// measured by recovering one.
///
/// A stream that started is not yet evidence: one whose access or predicate
/// matched nothing would start just the same. So each stream's first ask denies
/// a canary write under a fresh tag and waits for it to come back with its
/// path. A canary that came back holds for the stream's life; one that did not
/// is retried after [`STREAM_RETRY_AFTER`]. A stream that could not start is
/// retried on the same cadence, which is what turns this Measured on the next
/// liveness pass after the operator installs the sudo rule, with no restart.
pub(crate) fn violation_recovery() -> ViolationRecoveryEvidence {
    let stream = match ViolationStream::watching_or_refusal() {
        Ok(stream) => stream,
        Err(refusal) => {
            return ViolationRecoveryEvidence {
                access: refusal.access,
                outcome: ViolationRecoveryOutcome::Unmeasured {
                    reason: refusal.reason,
                },
                measured_at_unix_ms: refusal.at_unix_ms,
            }
        }
    };
    let mut held = stream.recovery.lock().unwrap_or_else(|p| p.into_inner());
    let remeasure = held.as_ref().is_none_or(|measured| {
        matches!(
            measured.outcome,
            ViolationRecoveryOutcome::Unmeasured { .. }
        ) && measured.at.elapsed() >= STREAM_RETRY_AFTER
    });
    if remeasure {
        *held = Some(RecoveryMeasurement {
            outcome: recover_a_canary_denial(stream.access),
            at: Instant::now(),
            at_unix_ms: crate::nested::unix_now_ms(),
        });
    }
    let measured = held.as_ref().expect("measured above");
    ViolationRecoveryEvidence {
        access: stream.access,
        outcome: measured.outcome.clone(),
        measured_at_unix_ms: measured.at_unix_ms,
    }
}

/// Deny one write the way a fenced spawn's is denied, then recover it from the
/// stream the way a fenced spawn's caller does.
///
/// The target sits in a sibling temp directory with nothing writable granted
/// beside the worktree, so the profile denies it, and nothing is ever written.
fn recover_a_canary_denial(access: ViolationStreamAccess) -> ViolationRecoveryOutcome {
    let unmeasured = |reason: String| ViolationRecoveryOutcome::Unmeasured { reason };
    let (worktree, outside) = match (tempfile::tempdir(), tempfile::tempdir()) {
        (Ok(worktree), Ok(outside)) => (worktree, outside),
        (Err(error), _) | (_, Err(error)) => {
            return unmeasured(format!(
                "could not create the recovery canary's directories: {error}"
            ))
        }
    };
    let target = canonicalize_best_effort(outside.path()).join("cairn-violation-canary");
    let policy = SandboxPolicy {
        worktree: worktree.path().to_path_buf(),
        writable_extra: Vec::new(),
        deny_read: Vec::new(),
        writable_regex: Vec::new(),
        worktree_writable: true,
        socket_resolvable: Vec::new(),
        violation_tag: crate::ViolationTag::fresh(),
        reach: Default::default(),
    };
    let since = SystemTime::now();
    let (program, args) = wrap_argv(
        "/usr/bin/touch",
        &[target.to_string_lossy().into_owned()],
        &policy,
    );
    let output = match Command::new(program)
        .args(args)
        .stdin(Stdio::null())
        .output()
    {
        Ok(output) => output,
        Err(error) => return unmeasured(format!("could not spawn the recovery canary: {error}")),
    };
    if target.exists() {
        return unmeasured(format!(
            "the recovery canary's write to {} landed, so there was no denial to recover",
            target.display()
        ));
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    if output.status.code() == Some(71) && stderr.contains("sandbox_apply") {
        return unmeasured(format!(
            "the recovery canary's profile could not be applied, so it made no denial to recover: {}",
            stderr.trim()
        ));
    }
    match detect_violation(&policy.violation_tag, since) {
        Some(violation) if violation.path == target => ViolationRecoveryOutcome::Measured {
            detail: format!(
                "a canary {} of {} was denied and recovered by path through `{}`",
                violation.operation,
                target.display(),
                crate::violation_stream_spawn_argv(access).join(" ")
            ),
        },
        Some(violation) => unmeasured(format!(
            "the recovery canary's denial came back naming {} rather than {}",
            violation.path.display(),
            target.display()
        )),
        None => unmeasured(format!(
            "the recovery canary's write to {} was denied, but its violation did not arrive on the stream within {:?}",
            target.display(),
            VIOLATION_ARRIVAL_WAIT
        )),
    }
}

/// Parse one `log stream --style ndjson` entry into the spawn tag it carries
/// and the violation it reports, if it is a tagged kernel Seatbelt violation.
///
/// A tagged violation message is the kernel's line with the rule's message
/// after it, on the same line or the next:
///   `Sandbox: touch(1234) deny(1) file-write-create /Users/x/escape.txt`
///   `cairn-fence-…`
/// The path is what follows the `deny(N) <operation>` prefix on the first line.
///
/// Only the kernel's own entries count. Any process in the tree can log text
/// shaped like a violation, tag included, but not as process 0 from the
/// Seatbelt extension, and the path parsed here becomes a crossing's scope.
/// The tag is the message's last one, because the rule's message is appended
/// after the path and a path can itself be named to look like a tag.
fn parse_violation_entry(line: &str) -> Option<(String, SandboxViolation)> {
    static DENY: OnceLock<regex::Regex> = OnceLock::new();
    let deny = DENY.get_or_init(|| regex::Regex::new(r"deny\(\d+\)\s+(\S+)\s+(/.+)$").unwrap());
    let (tag, violation) = tagged_kernel_violation(line)?;
    let caps = deny.captures(&violation)?;
    Some((
        tag,
        SandboxViolation {
            operation: caps.get(1)?.as_str().to_string(),
            path: PathBuf::from(caps.get(2)?.as_str().trim()),
        },
    ))
}

/// A tagged kernel denial as `operation target`, whatever the target is.
fn parse_denial_summary(line: &str) -> Option<(String, String)> {
    static DENY: OnceLock<regex::Regex> = OnceLock::new();
    let deny = DENY.get_or_init(|| regex::Regex::new(r"deny\(\d+\)\s+(\S+)(?:\s+(.+))?$").unwrap());
    let (tag, violation) = tagged_kernel_violation(line)?;
    let caps = deny.captures(&violation)?;
    let operation = caps.get(1)?.as_str();
    let summary = match caps.get(2) {
        Some(target) => format!("{operation} {}", target.as_str().trim()),
        None => operation.to_string(),
    };
    Some((tag, summary))
}

/// The tag and the violation text of a kernel Seatbelt entry, with the tag
/// removed; `None` for anything not logged by the kernel's extension as
/// process 0. Both parsers above stand on this check.
fn tagged_kernel_violation(line: &str) -> Option<(String, String)> {
    static TAG: OnceLock<regex::Regex> = OnceLock::new();
    let tag_re = TAG.get_or_init(|| {
        regex::Regex::new(&format!(
            "{}[0-9a-z]+",
            regex::escape(crate::ViolationTag::PREFIX)
        ))
        .unwrap()
    });
    let value: serde_json::Value = serde_json::from_str(line.trim()).ok()?;
    // The field is numeric in ndjson output.
    if value.get("processID")?.as_u64()? != 0 {
        return None;
    }
    if !value
        .get("senderImagePath")?
        .as_str()?
        .contains(KERNEL_SANDBOX_SENDER)
    {
        return None;
    }
    let msg = value.get("eventMessage")?.as_str()?;
    let tag = tag_re.find_iter(msg).last()?;
    let first = msg.lines().next()?;
    let violation = if tag.end() <= first.len() {
        &first[..tag.start()]
    } else {
        first
    };
    Some((tag.as_str().to_string(), violation.trim().to_string()))
}

/// Start the process-wide violation stream if it is not running, so a spawn
/// made after this can have its denials read back. Best-effort.
pub(crate) fn watch_violations() {
    let _ = ViolationStream::watching();
}

/// The distinct denials a profile tagged `tag` made since `since`, waiting up
/// to `wait` for the first; empty when no stream is running.
pub(crate) fn denials_since(
    tag: &crate::ViolationTag,
    since: SystemTime,
    wait: Duration,
) -> Vec<String> {
    ViolationStream::current()
        .map(|stream| stream.tagged_denials(tag.as_str(), since, wait))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;
    use tempfile::tempdir;

    /// The page-call profile closes reads by default and opens only its roots,
    /// its own directory and `/dev`; it permits one socket, the relay; and the
    /// credential floor still closes it. An ordinary fence does none of that.
    #[test]
    fn a_page_call_profile_reads_only_its_allowlist_and_reaches_only_the_relay() {
        let policy = SandboxPolicy::for_page_call(
            Path::new("/b/scratch/pc-1"),
            &[PathBuf::from(
                "/Library/Frameworks/Python.framework/Versions/3.12",
            )],
            4321,
        );
        let profile = sbpl_profile(&policy, &test_floor());
        let report = format!("(with message \"{}\")", policy.violation_tag.as_str());
        let read_deny = format!("(deny file-read* (subpath \"/\") {report})");
        assert!(profile.contains(&read_deny), "{profile}");
        assert!(profile.contains("(allow file-read-metadata)"));
        assert!(profile.contains("(allow file-read* (literal \"/\"))"));
        let reads = profile
            .lines()
            .find(|line| line.starts_with("(allow file-read* (subpath"))
            .expect("an allow-list of reads");
        for root in [
            "/usr/lib",
            "/System/Library",
            "/bin",
            "/Library/Frameworks/Python.framework/Versions/3.12",
            "/b/scratch/pc-1",
            "/dev",
        ] {
            assert!(
                reads.contains(&format!("(subpath \"{root}\")")),
                "{root}: {reads}"
            );
        }
        assert!(
            !reads.contains("/Users") && !reads.contains(".cairn"),
            "{reads}"
        );
        let writes = profile
            .lines()
            .find(|line| line.starts_with("(allow file-write*"))
            .expect("a write allow");
        assert_eq!(
            writes,
            "(allow file-write* (subpath \"/b/scratch/pc-1\") (subpath \"/dev\"))"
        );
        assert!(profile.contains(&format!("(deny network* {report})")));
        assert!(profile.contains("(allow network-outbound (remote ip \"localhost:4321\"))"));
        assert!(profile.contains(&format!("(deny mach-lookup {report})")));
        // The process table is closed to other processes: no page batch reads
        // a sibling batch's environment (and its relay capability) through
        // `process-info*` or `kern.procargs2`, and none signals another.
        assert!(profile.contains(&format!("(deny process-info* (target others) {report})")));
        assert!(profile.contains("(sysctl-name \"kern.procargs2\")"));
        assert!(profile.contains(&format!("(deny signal (target others) {report})")));
        // The read deny precedes the allow-list, and the floor is last.
        let deny_at = profile.find(&read_deny).unwrap();
        assert!(deny_at < profile.find(reads).unwrap());
        assert!(profile
            .trim_end()
            .ends_with(test_floor().sbpl_rules().trim_end()));

        let ordinary = sbpl_profile(
            &SandboxPolicy::for_run(Path::new("/work/wt"), &[], Vec::new()),
            &test_floor(),
        );
        assert!(!ordinary.contains("(deny file-read* (subpath \"/\")"));
        assert!(!ordinary.contains("network"));
        assert!(!ordinary.contains("mach-lookup"));
    }

    #[test]
    fn profile_has_expected_structure() {
        let policy = SandboxPolicy {
            worktree: PathBuf::from("/work/wt"),
            writable_extra: vec![],
            deny_read: vec![PathBuf::from("/home/x/.aws")],
            writable_regex: vec![],
            worktree_writable: true,
            socket_resolvable: vec![],
            violation_tag: crate::ViolationTag::fresh(),
            reach: Default::default(),
        };
        let profile = sbpl_profile(&policy, &test_floor());
        assert!(profile.starts_with("(version 1)\n(allow default)\n"));
        let report = format!("(with message \"{}\")", policy.violation_tag.as_str());
        assert!(profile.contains(&format!("(deny file-write* (subpath \"/\") {report})")));
        assert!(
            profile.contains(&format!("(subpath \"/home/x/.aws\") {report})")),
            "the read denylist reports the spawn's tag too:\n{profile}"
        );
        assert!(profile.contains("(allow file-write*"));
        assert!(profile.contains("(subpath \"/dev\")"));
        // deny-read appears after the writable allow (last-match-wins ordering),
        // and the worktree read re-allow appears after deny-read so it wins.
        let write_idx = profile.find("(allow file-write*").unwrap();
        let deny_idx = profile.find("(deny file-read*").unwrap();
        let read_allow_idx = profile.rfind("(allow file-read*").unwrap();
        assert!(deny_idx > write_idx);
        assert!(
            read_allow_idx > deny_idx,
            "worktree read re-allow must come after deny-read"
        );
    }

    /// A floor pinned to a fixed home, so a profile's text does not depend on
    /// the machine running the test.
    fn test_floor() -> OperatorCredentialFloor {
        OperatorCredentialFloor::new(Some(PathBuf::from("/Users/tester")), [])
    }

    #[test]
    fn the_operator_credential_boundary_closes_every_profile() {
        // A worktree inside a Cairn home and a session grant of the home itself
        // are both re-allowed for reads; the boundary must still come after
        // them, because Seatbelt applies the last matching rule.
        let policy = SandboxPolicy::for_run(
            Path::new("/Users/tester/.cairn/scratch/job"),
            &["/Users/tester/.cairn-dev-x".to_string()],
            vec![],
        );
        let profile = sbpl_profile(&policy, &test_floor());
        let boundary = profile
            .find("(deny file-read-data file-write* (regex")
            .expect("the boundary is in every fenced profile");
        let last_allow = profile.rfind("(allow ").unwrap();
        assert!(
            boundary > last_allow,
            "the boundary must follow every allow:\n{profile}"
        );
        assert!(profile.contains(r#"^/Users/tester/\.cairn[^/]*/operator_auth_secret$"#));
    }

    #[test]
    fn wrap_argv_prefixes_sandbox_exec() {
        let policy = SandboxPolicy {
            worktree: PathBuf::from("/work/wt"),
            writable_extra: vec![],
            deny_read: vec![],
            writable_regex: vec![],
            worktree_writable: true,
            socket_resolvable: vec![],
            violation_tag: crate::ViolationTag::fresh(),
            reach: Default::default(),
        };
        let (program, args) = wrap_argv("bash", &["-c".into(), "echo hi".into()], &policy);
        assert_eq!(program, "/usr/bin/sandbox-exec");
        assert_eq!(args[0], "-p");
        assert_eq!(args[2], "bash");
        assert_eq!(args[3], "-c");
        assert_eq!(args[4], "echo hi");
    }

    const TAG: &str = "cairn-fence-0123abcd";
    const KERNEL: &str = r#""processID":0,"senderImagePath":"/System/Library/Extensions/Sandbox.kext/Contents/MacOS/Sandbox""#;

    /// The first violation tagged `tag` among `ndjson`'s stream entries, the
    /// way [`ViolationStream`] selects it.
    fn parse_violation(ndjson: &str, tag: &str) -> Option<SandboxViolation> {
        ndjson
            .lines()
            .filter_map(super::parse_violation_entry)
            .find(|(seen, _)| seen == tag)
            .map(|(_, violation)| violation)
    }

    /// A path named to look like another spawn's tag cannot claim that spawn:
    /// the kernel appends the rule's message after the path, so the last tag
    /// is the one the profile attached.
    #[test]
    fn parse_violation_takes_the_rule_tag_not_a_tag_shaped_path() {
        let ndjson = format!(
            r#"{{{KERNEL},"eventMessage":"Sandbox: touch(5) deny(1) file-write-create /x/cairn-fence-victim\n{TAG}"}}"#
        );
        assert_eq!(
            super::parse_violation_entry(ndjson.trim()).map(|(tag, v)| (tag, v.path)),
            Some((TAG.to_string(), PathBuf::from("/x/cairn-fence-victim")))
        );
        assert_eq!(parse_violation(&ndjson, "cairn-fence-victim"), None);
    }

    /// A denial without a path (a mach service, a network operation) is kept
    /// for display, and never becomes a violation a crossing could be scoped to.
    #[test]
    fn a_denial_without_a_path_is_shown_but_never_a_crossing() {
        let ndjson = format!(
            r#"{{{KERNEL},"eventMessage":"Sandbox: bun(77) deny(1) mach-lookup com.apple.system.logger\n{TAG}"}}"#
        );
        assert_eq!(
            super::parse_denial_summary(ndjson.trim()),
            Some((
                TAG.to_string(),
                "mach-lookup com.apple.system.logger".to_string()
            ))
        );
        assert_eq!(super::parse_violation_entry(ndjson.trim()), None);
        let forged = format!(
            r#"{{"processID":42,"senderImagePath":"/usr/bin/bun","eventMessage":"Sandbox: bun(77) deny(1) mach-lookup x\n{TAG}"}}"#
        );
        assert_eq!(
            super::parse_denial_summary(&forged),
            None,
            "only the kernel's entries count"
        );
    }

    #[test]
    fn parse_violation_path_extracts_denied_path() {
        // The rule's message follows the kernel's line; the process named in
        // the message is a child of the spawn, which no pid filter could find.
        let ndjson = format!(
            r#"{{{KERNEL},"eventMessage":"Sandbox: touch(1235) deny(1) file-write-create /Users/x/escape.txt\n{TAG}"}}
{{{KERNEL},"eventMessage":"some unrelated log line"}}"#
        );
        let violation = parse_violation(&ndjson, TAG);
        assert_eq!(
            violation,
            Some(SandboxViolation {
                operation: "file-write-create".into(),
                path: PathBuf::from("/Users/x/escape.txt"),
            })
        );
    }

    #[test]
    fn parse_violation_path_accepts_the_tag_on_the_violation_line() {
        let ndjson = format!(
            r#"{{{KERNEL},"eventMessage":"Sandbox: git(77) deny(1) file-write-create /repo/.git/FETCH_HEAD {TAG}"}}"#
        );
        assert_eq!(
            parse_violation(&ndjson, TAG).map(|v| v.path),
            Some(PathBuf::from("/repo/.git/FETCH_HEAD"))
        );
    }

    #[test]
    fn parse_violation_path_ignores_other_spawns_and_non_deny() {
        let ndjson = format!(
            r#"{{{KERNEL},"eventMessage":"Sandbox: cat(9) deny(1) file-read-data /Users/x/.aws/credentials\ncairn-fence-ffff"}}
{{{KERNEL},"eventMessage":"Sandbox: cat(9) deny(1) file-read-data /Users/x/.aws/config"}}
{{{KERNEL},"eventMessage":"allow file-read-data /Users/x/ok\n{TAG}"}}"#
        );
        assert_eq!(parse_violation(&ndjson, TAG), None);
    }

    /// The parsed path becomes a crossing's scope, and a process in the tree
    /// can log a line shaped like a violation, tag and all.
    #[test]
    fn parse_violation_path_ignores_entries_the_kernel_did_not_write() {
        let ndjson = format!(
            r#"{{"processID":1235,"senderImagePath":"/usr/bin/logger","eventMessage":"Sandbox: touch(1) deny(1) file-write-create /Users/x/.ssh\n{TAG}"}}
{{"processID":0,"senderImagePath":"/usr/lib/libSystem.B.dylib","eventMessage":"Sandbox: touch(1) deny(1) file-write-create /Users/x/.ssh\n{TAG}"}}
{{"processID":1235,"senderImagePath":"/System/Library/Extensions/Sandbox.kext/Contents/MacOS/Sandbox","eventMessage":"Sandbox: touch(1) deny(1) file-write-create /Users/x/.ssh\n{TAG}"}}"#
        );
        assert_eq!(parse_violation(&ndjson, TAG), None);
    }

    #[test]
    fn parse_violation_path_handles_read_denials() {
        let ndjson = format!(
            r#"{{{KERNEL},"eventMessage":"Sandbox: cat(42) deny(1) file-read-data /home/u/.ssh/id_rsa\n{TAG}"}}"#
        );
        assert_eq!(
            parse_violation(&ndjson, TAG),
            Some(SandboxViolation {
                operation: "file-read-data".into(),
                path: PathBuf::from("/home/u/.ssh/id_rsa"),
            })
        );
    }

    #[test]
    fn escape_handles_quotes_and_backslashes() {
        assert_eq!(escape_sbpl("a\"b"), "a\\\"b");
        assert_eq!(escape_sbpl("a\\b"), "a\\\\b");
    }

    // Live end-to-end: spawn real commands under the generated profile via
    // sandbox-exec and assert the kernel enforces the policy. Runs only on
    // macOS, where sandbox-exec is present.
    //
    // macOS sandboxes do not nest: inside an outer sandbox a nested
    // `sandbox-exec` cannot write even where its own profile permits it, so there
    // is no policy left to observe. That refusal IS the behavior here, which makes
    // these the one family of tests a skip legitimately belongs to — but the skip
    // has to state a measured fact rather than a guess about the environment.
    // Reading `CAIRN_SANDBOXED` was such a guess, and it was wrong in both
    // directions: inherited down a spawn chain where no sandbox existed, and
    // absent where one did (CAIRN-3164). So each live test PROBES the capability
    // and records a skip naming it, declared in `src-tauri/skip-manifest.toml`.
    //
    // The probe costs one process spawn and every live test asks, so it resolves
    // once per test binary.
    fn nested_sandbox_exec_available() -> bool {
        static AVAILABLE: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        *AVAILABLE.get_or_init(|| {
            let Ok(dir) = tempdir() else {
                return false;
            };
            // The narrowest form of what every live test below does: a write the
            // profile explicitly permits.
            let policy = SandboxPolicy {
                worktree: dir.path().to_path_buf(),
                writable_extra: vec![],
                deny_read: vec![],
                writable_regex: vec![],
                worktree_writable: true,
                socket_resolvable: vec![],
                violation_tag: crate::ViolationTag::fresh(),
                reach: Default::default(),
            };
            let probe = dir.path().join("probe");
            let (program, args) = wrap_argv(
                "/bin/bash",
                &["-c".into(), format!("echo ok > {}", probe.display())],
                &policy,
            );
            Command::new(program)
                .args(args)
                .output()
                .is_ok_and(|out| out.status.success())
                && probe.exists()
        })
    }

    fn skip_without_nested_sandbox_exec(test: &str) -> bool {
        if nested_sandbox_exec_available() {
            return false;
        }
        eprintln!("skipping {test}: a nested sandbox-exec cannot write inside its own profile");
        cairn_common::skip_ledger::record_skip(test, "nested-sandbox-exec");
        true
    }

    #[test]
    fn enforces_write_confinement_and_read_denylist_live() {
        if skip_without_nested_sandbox_exec("enforces_write_confinement_and_read_denylist_live") {
            return;
        }
        let wt = tempdir().unwrap();
        let outside = tempdir().unwrap();
        let secret = outside.path().join("secret");
        std::fs::create_dir_all(&secret).unwrap();
        std::fs::write(secret.join("token"), "s3cr3t").unwrap();

        // Empty writable_extra so the sibling temp dir is genuinely outside the
        // writable set. (In production temp dirs are writable, but a real
        // escape targets $HOME or a sibling worktree, not the temp tree.)
        let policy = SandboxPolicy {
            worktree: wt.path().to_path_buf(),
            writable_extra: vec![],
            deny_read: vec![secret.clone()],
            writable_regex: vec![],
            worktree_writable: true,
            socket_resolvable: vec![],
            violation_tag: crate::ViolationTag::fresh(),
            reach: Default::default(),
        };

        let run = |cmd: &str| -> std::process::Output {
            let (program, args) = wrap_argv("/bin/bash", &["-c".into(), cmd.into()], &policy);
            Command::new(program).args(args).output().unwrap()
        };

        // In-worktree write succeeds.
        let inside_file = wt.path().join("ok.txt");
        let out = run(&format!("echo hi > {}", inside_file.display()));
        assert!(out.status.success(), "in-worktree write should succeed");
        assert!(inside_file.exists());

        // Out-of-worktree write is blocked by the kernel.
        let escape = outside.path().join("escape.txt");
        let out = run(&format!("echo x > {}", escape.display()));
        assert!(
            !out.status.success(),
            "out-of-worktree write must be denied"
        );
        assert!(!escape.exists(), "escape file must not be created");

        // Denylisted read is blocked even though reads are otherwise broad.
        let out = run(&format!("cat {}", secret.join("token").display()));
        assert!(!out.status.success(), "denylisted read must be denied");

        // A non-denylisted read outside the worktree still works (read-broad).
        let public = outside.path().join("public.txt");
        std::fs::write(&public, "hello").unwrap();
        let out = run(&format!("cat {}", public.display()));
        assert!(out.status.success(), "non-denylisted reads stay allowed");
    }

    // Live: a writable_extra carve-out (the mechanism behind keeping
    // ~/.cargo, ~/.npm, etc. writable) actually permits writes there while the
    // surrounding tree stays read-only. This is the regression guard the review
    // flagged as missing for the toolchain-cache fix.
    // Live: even when a (user-configured) denylist covers a prefix of the
    // worktree, the worktree stays readable via the final writable re-allow,
    // while a sensitive sibling under the same denied base is blocked.
    #[test]
    fn worktree_under_denied_prefix_stays_readable_live() {
        if skip_without_nested_sandbox_exec("worktree_under_denied_prefix_stays_readable_live") {
            return;
        }
        let base = tempdir().unwrap();
        let wt = base.path().join("worktrees").join("wt");
        std::fs::create_dir_all(wt.join("src")).unwrap();
        std::fs::write(wt.join("src/lib.rs"), "fn main() {}").unwrap();
        std::fs::write(base.path().join("secret"), "s3cr3t").unwrap();

        let policy = SandboxPolicy {
            worktree: wt.clone(),
            writable_extra: vec![],
            deny_read: vec![base.path().to_path_buf()], // a broad denylist entry
            writable_regex: vec![],
            worktree_writable: true,
            socket_resolvable: vec![],
            violation_tag: crate::ViolationTag::fresh(),
            reach: Default::default(),
        };
        let run = |cmd: &str| -> std::process::Output {
            let (program, args) = wrap_argv("/bin/bash", &["-c".into(), cmd.into()], &policy);
            Command::new(program).args(args).output().unwrap()
        };

        // Worktree nested under the denied base is still readable.
        let out = run(&format!("cat {}", wt.join("src/lib.rs").display()));
        assert!(
            out.status.success(),
            "worktree under a denied prefix must stay readable"
        );
        // A sensitive sibling under the denied base is blocked.
        let out = run(&format!("cat {}", base.path().join("secret").display()));
        assert!(
            !out.status.success(),
            "sibling under the denied base must be denied"
        );
    }

    // The interactive-shell grant has two properties worth proving at the
    // kernel rather than in the derivation: a shell can create its own state on
    // a home that has never run it, and nothing else under the same XDG base
    // becomes writable in the process.
    #[test]
    fn interactive_shell_state_grant_creates_shell_paths_but_not_neighbours_live() {
        if skip_without_nested_sandbox_exec(
            "interactive_shell_state_grant_creates_shell_paths_but_not_neighbours_live",
        ) {
            return;
        }
        let wt = tempdir().unwrap();
        let home = tempdir().unwrap();
        let data = home.path().join(".local/share");
        // The host ensures the base exists; the shell's own dir does not yet.
        std::fs::create_dir_all(&data).unwrap();
        let neighbour = data.join("some-other-app");
        std::fs::create_dir_all(&neighbour).unwrap();

        let policy = SandboxPolicy {
            worktree: wt.path().to_path_buf(),
            writable_extra: vec![data.join("fish"), data.join("z")],
            deny_read: vec![],
            writable_regex: vec![],
            worktree_writable: true,
            socket_resolvable: vec![],
            violation_tag: crate::ViolationTag::fresh(),
            reach: Default::default(),
        };
        let run = |cmd: &str| -> std::process::Output {
            let (program, args) = wrap_argv("/bin/bash", &["-c".into(), cmd.into()], &policy);
            Command::new(program).args(args).output().unwrap()
        };

        let fish = data.join("fish");
        let out = run(&format!(
            "mkdir -p {0} && echo hi > {0}/fish_history",
            fish.display()
        ));
        assert!(
            out.status.success(),
            "a first-run shell must be able to create its own state: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(fish.join("fish_history").exists());

        let out = run(&format!("echo x > {}/db", neighbour.display()));
        assert!(
            !out.status.success(),
            "an unrelated application's state under the same base must stay unwritable"
        );

        let out = run(&format!("mkdir {}/newly-invented-app", data.display()));
        assert!(
            !out.status.success(),
            "the XDG base itself must stay unwritable"
        );
    }

    /// What a read-only-checkout profile actually costs, at the kernel. Dropping
    /// the checkout from the writable set also drops every carve-out that
    /// contains it, so the writable set narrows to the explicit grants: the
    /// checkout's own build directory and any Cairn state directory beside it
    /// (a jj store, a sibling cell) become unwritable while both stay readable.
    ///
    /// That is the mechanism behind every specimen denial in CAIRN-3227. Which
    /// spawns receive this profile is the fence dial's decision and lives in
    /// `cairn-core`; what the profile does once applied is this.
    #[test]
    fn readonly_checkout_profile_denies_the_checkout_and_neighbouring_state_live() {
        if skip_without_nested_sandbox_exec(
            "readonly_checkout_profile_denies_the_checkout_and_neighbouring_state_live",
        ) {
            return;
        }
        let base = tempdir().unwrap();
        let checkout = base.path().join("live-checkout");
        let build_dir = checkout.join("target");
        std::fs::create_dir_all(&build_dir).unwrap();
        std::fs::write(checkout.join("Cargo.toml"), "[package]").unwrap();
        // Stands in for `~/.cairn/jj-stores/<store>`: Cairn's own state, beside
        // the checkout and in nobody's writable set.
        let store = base.path().join("cairn-home/jj-stores/store");
        std::fs::create_dir_all(&store).unwrap();
        // Stands in for the per-job scratch the executor grants explicitly.
        let scratch = base.path().join("scratch");
        std::fs::create_dir_all(&scratch).unwrap();

        let policy = SandboxPolicy::for_readonly_checkout(
            &checkout,
            &[scratch.to_string_lossy().into_owned()],
            vec![],
        );
        let confined = |cmd: &str| -> std::process::Output {
            let (program, args) = wrap_argv("/bin/bash", &["-c".into(), cmd.into()], &policy);
            Command::new(program).args(args).output().unwrap()
        };

        let build_probe = build_dir.join("probe");
        let store_probe = store.join("working_copy.lock");
        let scratch_probe = scratch.join("probe");
        let write = |path: &Path| format!("echo x > {}", path.display());

        assert!(
            !confined(&write(&build_probe)).status.success(),
            "a read-only checkout denies its own build directory"
        );
        assert!(
            !confined(&write(&store_probe)).status.success(),
            "Cairn's own state beside the checkout is denied too"
        );
        assert!(
            confined(&format!("cat {}", checkout.join("Cargo.toml").display()))
                .status
                .success(),
            "the checkout stays readable"
        );
        assert!(
            confined(&write(&scratch_probe)).status.success(),
            "an explicit grant stays writable"
        );

        // The control is the whole point of the dial: with no policy applied —
        // what every spawn surface produces for a `fence: allow` agent — the same
        // three operations are ordinary filesystem writes.
        std::fs::remove_file(&scratch_probe).unwrap();
        for path in [&build_probe, &store_probe, &scratch_probe] {
            let out = Command::new("/bin/bash")
                .args(["-c", &write(path)])
                .output()
                .unwrap();
            assert!(
                out.status.success() && path.exists(),
                "unconfined spawn must perform the specimen write: {}",
                path.display()
            );
        }
    }

    /// Run `script` under `policy` and recover its denial the way a fenced
    /// spawn's caller does, with what the violation stream saw attached for a
    /// failure to show.
    fn denial_under(
        policy: &SandboxPolicy,
        script: String,
    ) -> (Option<crate::SandboxDenial>, String) {
        let since = SystemTime::now();
        let (program, args) = wrap_argv("/bin/sh", &["-c".into(), script], policy);
        let out = Command::new(program).args(args).output().unwrap();
        let output = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        let denial = crate::detect_denial(
            out.status.code(),
            &output,
            Some(&policy.violation_tag),
            since,
            true,
        );
        let stream = ViolationStream::current().map_or_else(
            || format!("no violation stream is running: {}", last_refusal()),
            |stream| stream.describe(policy.violation_tag.as_str()),
        );
        (
            denial,
            format!("exit {:?}, output {output:?}; {stream}", out.status.code()),
        )
    }

    fn outside_write_policy(worktree: &Path) -> SandboxPolicy {
        // Empty writable_extra so a sibling temp dir is genuinely outside the
        // writable set.
        SandboxPolicy {
            worktree: worktree.to_path_buf(),
            writable_extra: vec![],
            deny_read: vec![],
            writable_regex: vec![],
            worktree_writable: true,
            socket_resolvable: vec![],
            violation_tag: crate::ViolationTag::fresh(),
            reach: Default::default(),
        }
    }

    /// The kernel logs a violation under the process that made the denied
    /// call. Here that is `touch`, a child of the spawned `sh` (the trailing
    /// `; true` stops sh from exec-ing it in place), so a lookup keyed on the
    /// spawned pid finds nothing and the prompt would blame `sh -c`.
    #[test]
    fn a_denial_made_by_a_child_process_is_recovered_by_path_live() {
        if skip_without_nested_sandbox_exec(
            "a_denial_made_by_a_child_process_is_recovered_by_path_live",
        ) {
            return;
        }
        let wt = tempdir().unwrap();
        let outside = tempdir().unwrap();
        let outside = outside.path().canonicalize().unwrap();
        let policy = outside_write_policy(wt.path());
        let target = outside.join("x");
        let (denial, evidence) = denial_under(&policy, format!("touch {}; true", target.display()));
        assert!(!target.exists(), "the write must have been refused");
        assert!(
            matches!(&denial, Some(crate::SandboxDenial::Path { path, .. }) if *path == target),
            "expected a path-scoped denial of {}, got {denial:?}; {evidence}",
            target.display()
        );
    }

    /// Two spawns denied in the same window each recover their own path, never
    /// the other's: the tag, not the time window, is what attributes a denial.
    #[test]
    fn concurrent_spawns_recover_only_their_own_denials_live() {
        if skip_without_nested_sandbox_exec("concurrent_spawns_recover_only_their_own_denials_live")
        {
            return;
        }
        let wt = tempdir().unwrap();
        let outside = tempdir().unwrap();
        let outside = outside.path().canonicalize().unwrap();
        let (first, second) = (outside.join("first"), outside.join("second"));
        let first_policy = outside_write_policy(wt.path());
        let second_policy = outside_write_policy(wt.path());
        let first_script = format!("exec touch {}", first.display());
        let second_script = format!("touch {}; true", second.display());
        let (first_denial, first_evidence) = std::thread::scope(|scope| {
            let first = scope.spawn(|| denial_under(&first_policy, first_script));
            let (second_denial, second_evidence) = denial_under(&second_policy, second_script);
            assert!(
                matches!(&second_denial, Some(crate::SandboxDenial::Path { path, .. }) if *path == second),
                "got {second_denial:?}; {second_evidence}"
            );
            first.join().unwrap()
        });
        assert!(
            matches!(&first_denial, Some(crate::SandboxDenial::Path { path, .. }) if *path == first),
            "got {first_denial:?}; {first_evidence}"
        );
    }

    /// The stream must not outlive the process that started it. Closing the
    /// lifeline is what this process's exit does to it, so a stand-in command
    /// that would otherwise run for ten minutes has to stop within moments, and
    /// the wrapper only exits once its command has.
    #[test]
    fn a_tied_command_stops_when_its_lifeline_closes() {
        let mut child = tied_to_this_process(&["/bin/sleep", "600"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let lifeline = child.stdin.take().unwrap();
        std::thread::sleep(Duration::from_millis(200));
        assert!(
            child.try_wait().unwrap().is_none(),
            "the command must keep running while its lifeline is open"
        );
        drop(lifeline);
        let deadline = Instant::now() + Duration::from_secs(10);
        while child.try_wait().unwrap().is_none() {
            if Instant::now() > deadline {
                let _ = child.kill();
                panic!("the tied command outlived its lifeline");
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// Every fenced spawn asks for the stream, and only the first starts one:
    /// a stream per spawn would cost a `log` process each and would miss
    /// violations made before it was ready.
    #[test]
    fn a_second_spawn_reuses_the_running_violation_stream_live() {
        if skip_without_nested_sandbox_exec(
            "a_second_spawn_reuses_the_running_violation_stream_live",
        ) {
            return;
        }
        let first = ViolationStream::watching_or_refusal()
            .unwrap_or_else(|refusal| panic!("the stream did not start: {}", refusal.reason));
        let second = ViolationStream::watching().expect("the stream is still running");
        assert!(Arc::ptr_eq(&first, &second));
    }

    /// Why the last start failed, for a failing live test to show.
    fn last_refusal() -> String {
        match VIOLATION_STREAM
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
        {
            Some(StreamSlot::Refused(refusal)) => refusal.reason.clone(),
            Some(StreamSlot::Running(_)) => "a stream is recorded as running".into(),
            None => "no start was attempted".into(),
        }
    }

    /// A refused stream says which of the two refusals it met, because they
    /// have different remedies, and carries the refusing tool's own words.
    #[test]
    fn a_refusal_names_what_refused_the_stream() {
        let spawned = crate::violation_stream_spawn_argv(ViolationStreamAccess::SudoRule).join(" ");
        let absent = refusal_reason(
            ViolationStreamAccess::SudoRule,
            &spawned,
            Some(1),
            "sudo: a password is required\n",
        );
        assert!(absent.starts_with("sudo rule absent: "), "{absent}");
        assert!(absent.contains("sudo: a password is required"), "{absent}");
        assert!(
            absent.contains(&crate::violation_stream_sudoers_command()),
            "the operator is shown the exact rule to install: {absent}"
        );

        let fenced = refusal_reason(
            ViolationStreamAccess::SudoRule,
            &spawned,
            Some(126),
            "sh: /usr/bin/sudo: Operation not permitted",
        );
        assert!(fenced.starts_with("the fence refused sudo: "), "{fenced}");
        assert!(fenced.contains("Operation not permitted"), "{fenced}");

        let admin = refusal_reason(
            ViolationStreamAccess::Admin,
            &crate::violation_stream_argv().join(" "),
            Some(64),
            "log: Must be admin to run 'stream' command",
        );
        assert!(
            admin.contains("Must be admin to run 'stream' command"),
            "{admin}"
        );

        let silent = refusal_reason(ViolationStreamAccess::SudoRule, &spawned, None, "");
        assert!(
            silent.ends_with("was stopped by a signal: nothing on stderr"),
            "{silent}"
        );
    }

    /// The access decision rests on the directory service's membership, which
    /// is what `log` consults and what `dsmemberutil` reports, not on this
    /// process's own group list, which stays stale after an account leaves
    /// `admin`.
    #[test]
    fn admin_membership_is_the_directory_services_answer() {
        // SAFETY: geteuid has no preconditions.
        let uid = unsafe { libc::geteuid() }.to_string();
        let out = Command::new("/usr/bin/dsmemberutil")
            .args(["checkmembership", "-u", &uid, "-G", "admin"])
            .output()
            .unwrap();
        let said = String::from_utf8_lossy(&out.stdout);
        let member = said.contains("is a member");
        assert!(
            member || said.contains("is not a member"),
            "unexpected dsmemberutil answer: {said:?}"
        );
        assert_eq!(account_is_admin(), member, "dsmemberutil said {said:?}");
    }

    /// Through sudo, the lifeline's kill reaches sudo, and sudo relays it to
    /// the root `log` only when the sender is in no process group that sudo or
    /// its command leads. The killer shares the wrapper's group, so the tied
    /// command must not lead a group of its own, or the kill is silently
    /// dropped and a root `log stream` outlives its executor.
    #[test]
    fn a_tied_command_leads_no_process_group_so_sudo_relays_the_lifeline_kill() {
        let mut child = tied_to_this_process(&["/bin/sh", "-c", "echo $$; exec /bin/sleep 600"])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let lifeline = child.stdin.take().unwrap();
        let mut line = String::new();
        BufReader::new(child.stdout.take().unwrap())
            .read_line(&mut line)
            .unwrap();
        let command: libc::pid_t = line.trim().parse().unwrap();
        // SAFETY: plain process-group queries.
        let (command_group, own_group) = unsafe { (libc::getpgid(command), libc::getpgrp()) };
        assert_ne!(
            command_group, command,
            "the tied command leads its own process group, so sudo would drop the lifeline's kill"
        );
        assert_eq!(
            command_group, own_group,
            "the tied command, and the killer beside it, run in the caller's group"
        );
        drop(lifeline);
        let deadline = Instant::now() + Duration::from_secs(10);
        while child.try_wait().unwrap().is_none() {
            if Instant::now() > deadline {
                let _ = child.kill();
                panic!("the tied command outlived its lifeline");
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// Recovery is reported measured only when a real denial came back with
    /// its path, and otherwise names what stopped it: never a stream that
    /// merely started, never a blank.
    #[test]
    fn violation_recovery_is_measured_by_a_recovered_canary_or_names_its_refusal_live() {
        if skip_without_nested_sandbox_exec(
            "violation_recovery_is_measured_by_a_recovered_canary_or_names_its_refusal_live",
        ) {
            return;
        }
        let evidence = violation_recovery();
        let admin = account_is_admin();
        assert_eq!(
            evidence.access,
            if admin {
                ViolationStreamAccess::Admin
            } else {
                ViolationStreamAccess::SudoRule
            }
        );
        match &evidence.outcome {
            ViolationRecoveryOutcome::Measured { detail } => {
                assert!(detail.contains("recovered by path"), "{detail}")
            }
            ViolationRecoveryOutcome::Unmeasured { reason } if admin => {
                panic!("an admin account must recover its canary: {reason}")
            }
            ViolationRecoveryOutcome::Unmeasured { reason } => assert!(
                reason.starts_with("sudo rule absent: ")
                    || reason.starts_with("the fence refused sudo: "),
                "a non-admin account without recovery must say why: {reason}"
            ),
        }
    }

    /// The real thing on a host with the rule: a root `log stream` started
    /// through `sudo -n` stops when the lifeline closes. Run it there with
    /// `cargo test -p cairn-sandbox --lib -- --ignored the_lifeline_stops_a_root_log_stream_through_sudo_live`
    /// (from src-tauri).
    #[test]
    #[ignore = "needs the operator's sudo rule for the violation stream; run on a Mac executor host with --ignored"]
    fn the_lifeline_stops_a_root_log_stream_through_sudo_live() {
        use std::io::Read as _;
        let argv = crate::violation_stream_spawn_argv(ViolationStreamAccess::SudoRule);
        let argv: Vec<&str> = argv.iter().map(String::as_str).collect();
        let mut wrapper = tied_to_this_process(&argv)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let lifeline = wrapper.stdin.take().unwrap();
        let mut stdout = BufReader::new(wrapper.stdout.take().unwrap());
        let mut announced = String::new();
        // `log stream` announces its filter first; a refused sudo exits silently here.
        stdout.read_line(&mut announced).unwrap();
        if announced.is_empty() {
            let mut said = String::new();
            let _ = wrapper.stderr.take().unwrap().read_to_string(&mut said);
            panic!("sudo -n did not start the stream: {said}");
        }
        let log = descendant_named(wrapper.id() as libc::pid_t, "log")
            .expect("a `log` process runs beneath sudo");
        drop(lifeline);
        let deadline = Instant::now() + Duration::from_secs(10);
        while wrapper.try_wait().unwrap().is_none() {
            assert!(
                Instant::now() < deadline,
                "sudo did not exit after the lifeline's kill, so root `log` pid {log} is still running and must be stopped by hand"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        // A live root process answers EPERM to a null signal; a gone one, ESRCH.
        while unsafe { libc::kill(log, 0) } == 0
            || std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
        {
            assert!(
                Instant::now() < deadline,
                "root `log` pid {log} outlived the lifeline"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// The first process named `name` beneath `root`, whether sudo runs its
    /// command directly or under a monitor process (its pty mode).
    fn descendant_named(root: libc::pid_t, name: &str) -> Option<libc::pid_t> {
        let pgrep = |args: &[&str]| -> Vec<libc::pid_t> {
            Command::new("/usr/bin/pgrep")
                .args(args)
                .output()
                .map(|out| {
                    String::from_utf8_lossy(&out.stdout)
                        .lines()
                        .filter_map(|line| line.trim().parse().ok())
                        .collect()
                })
                .unwrap_or_default()
        };
        let named = pgrep(&["-x", name]);
        let mut frontier = vec![root];
        while let Some(parent) = frontier.pop() {
            for child in pgrep(&["-P", &parent.to_string()]) {
                if named.contains(&child) {
                    return Some(child);
                }
                frontier.push(child);
            }
        }
        None
    }

    #[test]
    fn nested_write_probe_is_authoritative_when_allow_default_false_negatives() {
        let allow_default_claims_unconfined = !current_process_is_confined();
        let measured = crate::probe_nested_sandbox_write();
        if allow_default_claims_unconfined && !measured.available {
            assert!(
                measured.detail.contains("refused") || measured.detail.contains("could not"),
                "the measured refusal must retain actionable provenance: {}",
                measured.detail
            );
        }
        // Capability classification is the behavioral result in every state;
        // the allow-default probe is intentionally not consulted.
        assert_eq!(
            measured.available,
            crate::probe_nested_sandbox_write().available
        );
    }

    #[test]
    fn writable_extra_carveout_permits_writes_live() {
        if skip_without_nested_sandbox_exec("writable_extra_carveout_permits_writes_live") {
            return;
        }
        let wt = tempdir().unwrap();
        let cache_parent = tempdir().unwrap();
        let cache = cache_parent.path().join("cache");
        std::fs::create_dir_all(&cache).unwrap();

        let policy = SandboxPolicy {
            worktree: wt.path().to_path_buf(),
            writable_extra: vec![cache.clone()],
            deny_read: vec![],
            writable_regex: vec![],
            worktree_writable: true,
            socket_resolvable: vec![],
            violation_tag: crate::ViolationTag::fresh(),
            reach: Default::default(),
        };
        let run = |cmd: &str| -> std::process::Output {
            let (program, args) = wrap_argv("/bin/bash", &["-c".into(), cmd.into()], &policy);
            Command::new(program).args(args).output().unwrap()
        };

        // Write inside the carve-out succeeds.
        let inside = cache.join("artifact");
        let out = run(&format!("echo x > {}", inside.display()));
        assert!(
            out.status.success(),
            "write into writable_extra must succeed"
        );
        assert!(inside.exists());

        // Write to the carve-out's parent (not granted) is still denied.
        let sibling = cache_parent.path().join("escape.txt");
        let out = run(&format!("echo x > {}", sibling.display()));
        assert!(
            !out.status.success(),
            "write outside the carve-out must be denied"
        );
        assert!(!sibling.exists());
    }

    #[test]
    fn readonly_checkout_profile_reads_but_does_not_write_cwd() {
        // A non-worktree live-checkout policy keeps the checkout readable but
        // drops it from the write-allow, so a kernel write into it is denied.
        let checkout = PathBuf::from("/project/live");
        let policy = SandboxPolicy::for_readonly_checkout(&checkout, &[], vec![]);
        let profile = sbpl_profile(&policy, &test_floor());

        // The checkout appears in the final read re-allow.
        let read_allow = profile.rfind("(allow file-read*").unwrap();
        assert!(
            profile[read_allow..].contains("/project/live"),
            "read-only checkout must stay readable:\n{profile}"
        );
        // ... but NOT in the write-allow line.
        let write_allow_line = profile
            .lines()
            .find(|l| l.starts_with("(allow file-write*"))
            .unwrap_or("");
        assert!(
            !write_allow_line.contains("/project/live"),
            "read-only checkout must NOT be writable: {write_allow_line}"
        );
    }

    #[test]
    fn for_run_profile_writes_cwd() {
        let wt = PathBuf::from("/work/wt");
        let policy = SandboxPolicy::for_run(&wt, &[], vec![]);
        let profile = sbpl_profile(&policy, &test_floor());
        let write_allow_line = profile
            .lines()
            .find(|l| l.starts_with("(allow file-write*"))
            .unwrap_or("");
        assert!(
            write_allow_line.contains("/work/wt"),
            "for_run must keep cwd writable: {write_allow_line}"
        );
    }

    #[test]
    fn service_profile_contains_regex_write_allow() {
        let policy = SandboxPolicy::for_service(
            Path::new("/home/u/.cairn/sccache"),
            &["/home/u/.cairn/worktrees/**/target/**".to_string()],
            vec![],
        );
        let profile = sbpl_profile(&policy, &test_floor());
        assert!(profile.contains("(allow file-write*"));
        // The worktrees-target glob is emitted as an anchored SBPL regex.
        assert!(
            profile.contains("(regex #\"^/home/u/\\.cairn/worktrees/.*/target/.*\")"),
            "profile missing worktrees-target regex allow:\n{profile}"
        );
    }

    /// A throwaway OS home holding a normal Cairn home and a dev-instance home,
    /// each with both secrets, plus a same-named fixture outside any home.
    struct CredentialFixture {
        _dir: tempfile::TempDir,
        root: PathBuf,
        os_home: PathBuf,
        floor: OperatorCredentialFloor,
    }

    const SECRET_MARKER: &str = "OPERATOR-SECRET-MARKER-32-BYTES!";

    impl CredentialFixture {
        fn new() -> Self {
            let dir = tempdir().unwrap();
            let root = dir.path().canonicalize().unwrap();
            let os_home = root.join("home");
            for home in [".cairn", ".cairn-dev-agent-cairn-1-builder-0"] {
                let home = os_home.join(home);
                std::fs::create_dir_all(&home).unwrap();
                std::fs::write(home.join("operator_auth_secret"), SECRET_MARKER).unwrap();
                std::fs::write(home.join("mcp_auth_secret"), "mcp-secret").unwrap();
            }
            std::fs::create_dir_all(root.join("fixture")).unwrap();
            std::fs::write(root.join("fixture/operator_auth_secret"), "fixture").unwrap();
            let floor = OperatorCredentialFloor::new(Some(os_home.clone()), []);
            Self {
                _dir: dir,
                root,
                os_home,
                floor,
            }
        }

        fn dev_secret(&self) -> PathBuf {
            self.os_home
                .join(".cairn-dev-agent-cairn-1-builder-0/operator_auth_secret")
        }

        fn run(&self, profile: &str, script: &str) -> std::process::Output {
            let (program, args) = wrap_argv_with_profile(
                "/bin/bash",
                &["-c".into(), script.into()],
                profile.to_string(),
            );
            Command::new(program)
                .args(args)
                .current_dir(&self.root)
                .output()
                .unwrap()
        }

        fn floor_only_profile(&self) -> String {
            format!("(version 1)\n(allow default)\n{}", self.floor.sbpl_rules())
        }
    }

    fn leaked(out: &std::process::Output) -> bool {
        String::from_utf8_lossy(&out.stdout).contains(SECRET_MARKER)
    }

    // Live: every way of naming a Cairn home's operator credential that the
    // boundary exists to refuse, under the floor an unfenced agent spawn gets.
    #[test]
    fn operator_credential_boundary_refuses_every_spelling_live() {
        if skip_without_nested_sandbox_exec(
            "operator_credential_boundary_refuses_every_spelling_live",
        ) {
            return;
        }
        use std::os::unix::fs::MetadataExt as _;

        let fixture = CredentialFixture::new();
        let profile = fixture.floor_only_profile();
        let secret = fixture.dev_secret();
        let home = secret.parent().unwrap().to_path_buf();
        let root = &fixture.root;
        let metadata = std::fs::metadata(&secret).unwrap();
        let mut attempts = vec![
            ("direct", format!("cat {}", secret.display())),
            (
                "normal home",
                format!("cat {}/.cairn/operator_auth_secret", fixture.os_home.display()),
            ),
            (
                "relative",
                format!("cd {} && cat operator_auth_secret", home.display()),
            ),
            (
                "dot segments",
                format!(
                    "cd {} && cat ./.cairn/../.cairn-dev-agent-cairn-1-builder-0/./operator_auth_secret",
                    fixture.os_home.display()
                ),
            ),
            (
                "symlinked file",
                format!("ln -s {} {r}/l1 && cat {r}/l1", secret.display(), r = root.display()),
            ),
            (
                "symlinked home",
                format!(
                    "ln -s {} {r}/l2 && cat {r}/l2/operator_auth_secret",
                    home.display(),
                    r = root.display()
                ),
            ),
            (
                "case variant",
                format!("cat {}/OPERATOR_AUTH_SECRET", home.display()),
            ),
            (
                "hard link",
                format!("ln {} {r}/h1 && cat {r}/h1", secret.display(), r = root.display()),
            ),
            (
                "APFS clone",
                format!("cp -c {} {r}/c1 && cat {r}/c1", secret.display(), r = root.display()),
            ),
            (
                "inode path",
                format!("cat /.vol/{}/{}", metadata.dev(), metadata.ino()),
            ),
            (
                "rename the file",
                format!("mv {} {r}/m1 && cat {r}/m1", secret.display(), r = root.display()),
            ),
            (
                "rename the home",
                format!(
                    "mv {} {r}/moved && cat {r}/moved/operator_auth_secret",
                    home.display(),
                    r = root.display()
                ),
            ),
        ];
        let firmlink = Path::new("/System/Volumes/Data");
        if firmlink.is_dir() {
            attempts.push((
                "data-volume firmlink",
                format!("cat {}{}", firmlink.display(), secret.display()),
            ));
        }

        for (spelling, script) in attempts {
            let out = fixture.run(&profile, &script);
            assert!(
                !leaked(&out),
                "{spelling} read the operator credential: {script}"
            );
        }
        assert!(
            secret.exists(),
            "no attempt may move the credential out from under its home"
        );
    }

    // Live: the boundary takes nothing else. The MCP secret beside it, the
    // home's other files, a new dev-instance home, and a same-named fixture in a
    // temp directory all behave exactly as they do with no profile at all.
    #[test]
    fn operator_credential_boundary_leaves_ordinary_work_alone_live() {
        if skip_without_nested_sandbox_exec(
            "operator_credential_boundary_leaves_ordinary_work_alone_live",
        ) {
            return;
        }
        let fixture = CredentialFixture::new();
        let profile = fixture.floor_only_profile();
        let home = fixture.dev_secret().parent().unwrap().to_path_buf();
        for (what, script) in [
            ("read the MCP secret", format!("cat {}/mcp_auth_secret", home.display())),
            ("stat the credential", format!("ls -l {}", fixture.dev_secret().display())),
            (
                "write inside a home",
                format!("echo x > {h}/new && mkdir {h}/sub", h = home.display()),
            ),
            (
                "create a new dev-instance home",
                format!(
                    "mkdir {h}/.cairn-dev-new && echo x > {h}/.cairn-dev-new/f",
                    h = fixture.os_home.display()
                ),
            ),
            (
                "use a same-named fixture outside any home",
                format!(
                    "cat {r}/fixture/operator_auth_secret && echo y > {r}/fixture/operator_auth_secret",
                    r = fixture.root.display()
                ),
            ),
        ] {
            let out = fixture.run(&profile, &script);
            assert!(
                out.status.success(),
                "{what} must still work: {}",
                String::from_utf8_lossy(&out.stderr)
            );
        }
    }

    // Live: a family home that is a symlink to a directory elsewhere is read
    // through its real location, which the family pattern never spells; the
    // discovered floor covers it there. And no agent may plant such a link.
    #[test]
    fn a_relocated_home_and_a_planted_link_are_both_refused_live() {
        if skip_without_nested_sandbox_exec(
            "a_relocated_home_and_a_planted_link_are_both_refused_live",
        ) {
            return;
        }
        let fixture = CredentialFixture::new();
        let peer = fixture.root.join("volumes/peer-home");
        std::fs::create_dir_all(&peer).unwrap();
        std::fs::write(peer.join("operator_auth_secret"), SECRET_MARKER).unwrap();
        std::os::unix::fs::symlink(&peer, fixture.os_home.join(".cairn-dev-peer")).unwrap();
        let floor = OperatorCredentialFloor::discovered(Some(fixture.os_home.clone()), []);
        let profile = format!("(version 1)\n(allow default)\n{}", floor.sbpl_rules());

        for spelling in [
            peer.join("operator_auth_secret"),
            fixture.os_home.join(".cairn-dev-peer/operator_auth_secret"),
        ] {
            let out = fixture.run(&profile, &format!("cat {}", spelling.display()));
            assert!(!leaked(&out), "{} was readable", spelling.display());
        }

        let planted = fixture.os_home.join(".cairn-dev-future");
        let out = fixture.run(
            &profile,
            &format!("ln -s {} {}", fixture.root.display(), planted.display()),
        );
        assert!(
            !out.status.success() && !planted.exists(),
            "a symlink planted at a family name would relocate a future home"
        );
        let out = fixture.run(&profile, &format!("mkdir {}", planted.display()));
        assert!(out.status.success(), "creating a real home stays allowed");
    }

    // Live: a fenced profile whose worktree and grants cover a Cairn home still
    // refuses its credential, because the boundary is the profile's last word.
    #[test]
    fn a_fenced_profile_cannot_reopen_the_operator_credential_live() {
        if skip_without_nested_sandbox_exec(
            "a_fenced_profile_cannot_reopen_the_operator_credential_live",
        ) {
            return;
        }
        let fixture = CredentialFixture::new();
        let home = fixture.dev_secret().parent().unwrap().to_path_buf();
        let policy = SandboxPolicy {
            worktree: home.clone(),
            writable_extra: vec![fixture.os_home.clone()],
            deny_read: vec![],
            writable_regex: vec![],
            worktree_writable: true,
            socket_resolvable: vec![],
            violation_tag: crate::ViolationTag::fresh(),
            reach: Default::default(),
        };
        let profile = sbpl_profile(&policy, &fixture.floor);
        let out = fixture.run(&profile, &format!("cat {}", fixture.dev_secret().display()));
        assert!(!leaked(&out), "a fence grant reopened the credential");
        let out = fixture.run(&profile, &format!("cat {}/mcp_auth_secret", home.display()));
        assert!(out.status.success(), "the worktree itself stays readable");
    }

    // Live: a service sandbox writes worktree `target/` trees (the cross-worktree
    // grant) and its own state dir, but NOT worktree source or arbitrary paths.
    // This is the regression guard for the Managed Build Services fix.
    #[test]
    fn service_sandbox_permits_worktree_target_writes_only_live() {
        if skip_without_nested_sandbox_exec(
            "service_sandbox_permits_worktree_target_writes_only_live",
        ) {
            return;
        }
        let base = tempdir().unwrap();
        // Canonicalize so the glob's literal prefix matches the path the kernel
        // reports (macOS /var/folders -> /private/var/folders).
        let root = base.path().canonicalize().unwrap();
        let worktrees = root.join("worktrees");
        let wt = worktrees.join("CAIRN-1").join("src-tauri");
        std::fs::create_dir_all(wt.join("target/release/deps")).unwrap();
        std::fs::create_dir_all(wt.join("src")).unwrap();
        let state = root.join("sccache");
        std::fs::create_dir_all(&state).unwrap();

        let glob = format!("{}/**/target/**", worktrees.display());
        // Build the policy with only the state dir writable (no blanket temp
        // carve-out) so the test isolates the regex grant: the worktree source
        // lives under the temp tree, which `for_service` would otherwise make
        // writable in production. The regex is the mechanism under test.
        let policy = SandboxPolicy {
            worktree: state.clone(),
            writable_extra: vec![state.clone()],
            deny_read: vec![],
            writable_regex: vec![super::super::glob_to_regex(&glob)],
            worktree_writable: true,
            socket_resolvable: vec![],
            violation_tag: crate::ViolationTag::fresh(),
            reach: Default::default(),
        };
        let run = |cmd: &str| -> std::process::Output {
            let (program, args) = wrap_argv("/bin/bash", &["-c".into(), cmd.into()], &policy);
            Command::new(program).args(args).output().unwrap()
        };

        // Write into a worktree target tree: allowed via the regex grant.
        let target_file = wt.join("target/release/deps/x.d");
        let out = run(&format!("echo x > {}", target_file.display()));
        assert!(
            out.status.success(),
            "service must write worktree target dirs"
        );
        assert!(target_file.exists());

        // Write into worktree SOURCE: denied (it is not a target dir).
        let src_file = wt.join("src/evil.rs");
        let out = run(&format!("echo x > {}", src_file.display()));
        assert!(
            !out.status.success(),
            "service must NOT write worktree source"
        );
        assert!(!src_file.exists());

        // Write into the service's own state dir: allowed.
        let state_file = state.join("cache.bin");
        let out = run(&format!("echo x > {}", state_file.display()));
        assert!(out.status.success(), "service must write its own state dir");

        // Write to an arbitrary sibling outside all grants: denied.
        let escape = root.join("escape.txt");
        let out = run(&format!("echo x > {}", escape.display()));
        assert!(
            !out.status.success(),
            "service must not write arbitrary paths"
        );
        assert!(!escape.exists());
    }
}
