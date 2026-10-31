//! Linux sandbox: a landlock ruleset installed in a `pre_exec` hook.
//!
//! landlock is allowlist-only (no deny rules), so each boundary is expressed by
//! *handling* an access right and granting it only beneath named paths;
//! everything else loses that right at the kernel. Two boundaries ride in the
//! one ruleset:
//!
//! - **Writes.** The write rights are granted beneath the writable set (the
//!   worktree, temp and toolchain dirs, session grants). Reads stay
//!   unrestricted so the toolchain (cargo/npm/git/compilers) works.
//! - **Host sockets.** `LANDLOCK_ACCESS_FS_RESOLVE_UNIX` (ABI 9) refuses
//!   `connect(2)` — and `sendmsg(2)` with an explicit address — to a pathname
//!   unix socket whose server was created outside the caller's Landlock domain,
//!   unless the right is granted beneath the socket's real location.
//!   `LANDLOCK_SCOPE_ABSTRACT_UNIX_SOCKET` (ABI 6) does the same for abstract
//!   sockets, with no exceptions. Together they close the session bus, the
//!   systemd and gpg/ssh agents, X11/Wayland, and every same-uid tmux server a
//!   batch did not start itself. A server the job started inside its own fence
//!   stays reachable: within one spawn by domain membership, and across the
//!   job's spawns (which are sibling domains) through the socket-resolvable
//!   set, which is only the cell's scratch and a writable worktree.
//!
//! The write floor is a hard requirement (ABI 2); everything above it is best
//! effort, so an older kernel still runs fenced work with what it can enforce
//! and [`enforcement_description`] says exactly what that is. Below ABI 9 the
//! socket boundary does not exist, and the containment probe measures it as
//! `Ambient` rather than letting the write fence imply it.
//!
//! Not enforced on Linux: the sensitive-read denylist (landlock would require
//! allowlisting every readable root minus the denylist), and signal scoping
//! (`LANDLOCK_SCOPE_SIGNAL`), which would stop a batch signalling its own
//! job's processes from earlier batches and terminals.

use super::SandboxPolicy;
use std::io;
use std::os::unix::process::CommandExt;
use std::path::PathBuf;

use landlock::{
    AccessFs, BitFlags, CompatLevel, Compatible, Ruleset, RulesetAttr, RulesetCreatedAttr, Scope,
    ABI,
};

/// The ABI every fenced spawn requires. ABI 1 cannot grant REFER, so it
/// rejects cross-directory rename/link with EXDEV and breaks package managers
/// that stage files in a temp directory before moving them into the checkout.
const FLOOR_ABI: ABI = ABI::V2;

/// The newest ABI whose rights this build asks for when the kernel offers them.
const TARGET_ABI: ABI = ABI::V9;

/// The first ABI that can refuse a pathname unix socket.
pub(crate) const RESOLVE_UNIX_ABI: i32 = 9;

/// The first ABI that can scope abstract unix sockets to the domain.
pub(crate) const ABSTRACT_SCOPE_ABI: i32 = 6;

/// Every right the ruleset handles. Handling a right is what takes it away
/// from everything not granted it.
fn handled_rights() -> BitFlags<AccessFs> {
    AccessFs::from_write(TARGET_ABI)
}

/// What the writable set is granted: every handled right except
/// `ResolveUnix`.
///
/// This exclusion is load-bearing. The writable set contains `/tmp`, `/dev`,
/// `~/.cache` and Cairn's scratch root, and granting the full write set beneath
/// them would re-open `/tmp/tmux-<uid>`, `/tmp/ssh-*`, `/dev/log` and every
/// other job's sockets -- exactly the crossings the socket boundary closes.
fn writable_grant() -> BitFlags<AccessFs> {
    handled_rights() & !AccessFs::ResolveUnix
}

/// What the socket-resolvable set is granted: `ResolveUnix` alone.
fn resolvable_grant() -> BitFlags<AccessFs> {
    AccessFs::ResolveUnix.into()
}

/// The running kernel's Landlock ABI, or `None` when Landlock is not
/// implemented or not enabled.
///
/// Read from the kernel rather than from this crate's constants: the same
/// ruleset is a different guarantee on a different kernel, and the socket
/// boundary in particular exists only from ABI 9.
pub(crate) fn kernel_abi() -> Option<i32> {
    const LANDLOCK_CREATE_RULESET_VERSION: libc::c_uint = 1;
    // SAFETY: with a null attribute pointer, zero size and the VERSION flag the
    // syscall only reports the ABI; it creates nothing and touches no memory.
    let version = unsafe {
        libc::syscall(
            libc::SYS_landlock_create_ruleset,
            std::ptr::null::<libc::c_void>(),
            0usize,
            LANDLOCK_CREATE_RULESET_VERSION,
        )
    };
    (version > 0).then_some(version as i32)
}

fn abi_from_kernel(version: i32) -> ABI {
    match version {
        i32::MIN..=0 => ABI::Unsupported,
        1 => ABI::V1,
        2 => ABI::V2,
        3 => ABI::V3,
        4 => ABI::V4,
        5 => ABI::V5,
        6 => ABI::V6,
        7 => ABI::V7,
        8 => ABI::V8,
        _ => ABI::V9,
    }
}

/// The rights and scopes a ruleset built here actually enforces on a kernel at
/// `version`: the handled rights the kernel knows, and the abstract-socket
/// scope from ABI 6.
fn effective_on(version: Option<i32>) -> (BitFlags<AccessFs>, bool) {
    let abi = abi_from_kernel(version.unwrap_or(0));
    (
        handled_rights() & AccessFs::from_write(abi),
        version.is_some_and(|v| v >= ABSTRACT_SCOPE_ABI),
    )
}

/// The landlock ruleset this build installs on this kernel, as text that moves
/// whenever the enforcement does.
///
/// The kernel's ABI is part of it because the same paths under a different
/// ABI are a different guarantee: ABI 2 grants REFER where ABI 1 does not, and
/// only ABI 9 refuses a pathname socket. A measurement taken on one kernel is
/// therefore not a measurement of the other. The ABI is read from the running
/// kernel, and the rights are derived from it rather than observed; the probe
/// is what observes them.
pub(crate) fn enforcement_description(policy: &SandboxPolicy) -> String {
    let version = kernel_abi();
    let (rights, abstract_scoped) = effective_on(version);
    let mut writable: Vec<String> = policy
        .writable_extra
        .iter()
        .chain(std::iter::once(&policy.worktree))
        .map(|path| path.to_string_lossy().into_owned())
        .collect();
    writable.sort();
    let mut resolvable: Vec<String> = policy
        .socket_resolvable_paths()
        .iter()
        .map(|path| path.to_string_lossy().into_owned())
        .collect();
    resolvable.sort();
    format!(
        "landlock kernel-abi={} floor={FLOOR_ABI:?} handled={rights:?} scopes=[{}] available={} write-allow=[{}] resolve-unix-allow=[{}]",
        version.map_or_else(|| "none".to_string(), |v| v.to_string()),
        if abstract_scoped { "AbstractUnixSocket" } else { "" },
        is_available(),
        writable.join(","),
        resolvable.join(","),
    )
}

/// Probe whether the running kernel supports the write floor (ABI ≥ 2).
///
/// Creates (but does not enforce) a ruleset; the `landlock_create_ruleset`
/// syscall fails on kernels without landlock or with it disabled.
pub fn is_available() -> bool {
    Ruleset::default()
        .set_compatibility(CompatLevel::HardRequirement)
        .handle_access(AccessFs::from_write(FLOOR_ABI))
        .and_then(|r| r.create())
        .is_ok()
}

/// Install a `pre_exec` hook that confines the child's writes to the policy's
/// writable set and its unix-socket connections to its own domain plus the
/// socket-resolvable set. Fail-closed: if the write floor cannot be applied,
/// the spawn fails rather than running unconfined.
///
/// For a read-only-checkout policy the live checkout is absent from
/// [`SandboxPolicy::writable_paths`] and from
/// [`SandboxPolicy::socket_resolvable_paths`], so both boundaries exclude it
/// without a Linux-specific branch.
pub fn install_pre_exec(cmd: &mut std::process::Command, policy: &SandboxPolicy) {
    let (writable, resolvable) = rule_paths(policy);

    // SAFETY: the closure runs in the forked child before exec. It only calls
    // landlock syscalls and allocates — no shared-state mutation that would be
    // unsafe across the fork.
    unsafe {
        cmd.pre_exec(move || restrict(&writable, &resolvable));
    }
}

/// The two rule sets a policy installs, never merged: the writable set (which
/// gets every handled right but `ResolveUnix`) and the socket-resolvable set
/// (which gets `ResolveUnix` alone).
fn rule_paths(policy: &SandboxPolicy) -> (Vec<PathBuf>, Vec<PathBuf>) {
    let mut writable = policy.writable_paths();
    // /dev is I/O plumbing (tty, null, fd) and must stay writable.
    writable.push(PathBuf::from("/dev"));
    (writable, policy.socket_resolvable_paths())
}

fn restrict(writable: &[PathBuf], resolvable: &[PathBuf]) -> io::Result<()> {
    // The floor is a hard requirement: a kernel below ABI 2 fails the spawn.
    // Everything above it (TRUNCATE, IOCTL_DEV, RESOLVE_UNIX, the abstract
    // socket scope) is best effort, so an older kernel drops what it cannot
    // enforce instead of refusing to run fenced work; the probe then measures
    // the socket boundary as absent on that kernel rather than assuming it.
    let ruleset = Ruleset::default()
        .set_compatibility(CompatLevel::HardRequirement)
        .handle_access(AccessFs::from_write(FLOOR_ABI))
        .map_err(to_io)?
        .set_compatibility(CompatLevel::BestEffort)
        .handle_access(handled_rights())
        .map_err(to_io)?
        .scope(Scope::AbstractUnixSocket)
        .map_err(to_io)?
        .create()
        .map_err(to_io)?;

    // A missing path yields no fd and is skipped: nothing can be written or
    // bound there anyway. On a kernel without RESOLVE_UNIX the resolvable
    // rules carry no right it knows, and best effort drops them.
    let ruleset = ruleset
        .add_rules(landlock::path_beneath_rules(writable, writable_grant()))
        .map_err(to_io)?
        .add_rules(landlock::path_beneath_rules(resolvable, resolvable_grant()))
        .map_err(to_io)?;

    ruleset.restrict_self().map_err(to_io)?;
    Ok(())
}

fn to_io<E: std::fmt::Display>(e: E) -> io::Error {
    io::Error::other(format!("landlock: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn the_writable_set_never_carries_resolve_unix() {
        // Granting ResolveUnix beneath /tmp, /dev or the scratch root would
        // re-open every predictable host socket. The writable grant must
        // therefore lack it on every ABI this build knows, and the resolvable
        // grant must be exactly it.
        assert!(!writable_grant().contains(AccessFs::ResolveUnix));
        assert!(handled_rights().contains(AccessFs::ResolveUnix));
        assert_eq!(resolvable_grant(), BitFlags::from(AccessFs::ResolveUnix));
        assert_eq!(writable_grant() | resolvable_grant(), handled_rights());
    }

    #[test]
    fn the_rule_sets_stay_apart_and_exclude_shared_roots() {
        let mut policy = SandboxPolicy::for_run(Path::new("/work/wt"), &[], Vec::new());
        policy.allow_job_sockets_in(Path::new("/cairn/cells/a/scratch"));
        let (writable, resolvable) = rule_paths(&policy);
        assert!(writable.contains(&PathBuf::from("/dev")));
        assert_eq!(
            resolvable,
            vec![
                PathBuf::from("/work/wt"),
                PathBuf::from("/cairn/cells/a/scratch")
            ]
        );
        for shared in ["/tmp", "/dev"] {
            assert!(!resolvable.contains(&PathBuf::from(shared)), "{shared}");
        }
    }

    #[test]
    fn effective_rights_follow_the_kernel_abi() {
        let (abi7, scoped7) = effective_on(Some(7));
        assert!(!abi7.contains(AccessFs::ResolveUnix));
        assert!(abi7.contains(AccessFs::Truncate | AccessFs::IoctlDev));
        assert!(scoped7);
        let (abi9, scoped9) = effective_on(Some(9));
        assert!(abi9.contains(AccessFs::ResolveUnix));
        assert!(scoped9);
        let (abi5, scoped5) = effective_on(Some(5));
        assert!(!scoped5);
        assert!(!abi5.contains(AccessFs::ResolveUnix));
        let (none, unscoped) = effective_on(None);
        assert!(none.is_empty());
        assert!(!unscoped);
    }
}
