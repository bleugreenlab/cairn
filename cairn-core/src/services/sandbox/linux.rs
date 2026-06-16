//! Linux sandbox: landlock write-confinement installed in a `pre_exec` hook.
//!
//! landlock is allowlist-only (no deny rules), so write-confinement is
//! expressed by handling the write access rights and granting them only to the
//! writable set; everything else becomes read-only at the kernel level. Reads
//! are left unrestricted so the toolchain (cargo/npm/git/compilers) works.
//!
//! v1 scope: **write confinement only.** The sensitive-read denylist is enforced
//! on macOS but not yet on Linux — landlock would require allowlisting every
//! readable root minus the denylist (or a `bwrap` mount-mask), tracked as a
//! follow-up. The headless target still gains a kernel write boundary.

use super::SandboxPolicy;
use std::io;
use std::os::unix::process::CommandExt;
use std::path::PathBuf;

use landlock::{Access, AccessFs, Ruleset, RulesetAttr, RulesetCreatedAttr, ABI};

const ABI_VERSION: ABI = ABI::V1;

/// Probe whether the running kernel supports landlock (ABI ≥ 1).
///
/// Creates (but does not enforce) a ruleset; the `landlock_create_ruleset`
/// syscall fails on kernels without landlock or with it disabled.
pub fn is_available() -> bool {
    Ruleset::default()
        .handle_access(AccessFs::from_write(ABI_VERSION))
        .and_then(|r| r.create())
        .is_ok()
}

/// Install a `pre_exec` hook that restricts the child to write only within the
/// policy's writable set. Fail-closed: if landlock cannot be applied, the spawn
/// fails rather than running unconfined.
pub fn install_pre_exec(cmd: &mut std::process::Command, policy: &SandboxPolicy) {
    let mut writable = policy.writable_paths();
    // /dev is I/O plumbing (tty, null, fd) and must stay writable.
    writable.push(PathBuf::from("/dev"));

    // SAFETY: the closure runs in the forked child before exec. It only calls
    // landlock syscalls and allocates — no shared-state mutation that would be
    // unsafe across the fork.
    unsafe {
        cmd.pre_exec(move || restrict_writes(&writable));
    }
}

fn restrict_writes(writable: &[PathBuf]) -> io::Result<()> {
    // NOTE (ABI gap): `from_write(ABI::V1)` does not include `TRUNCATE` (added
    // in landlock ABI 3), so an out-of-worktree `truncate()` of an existing
    // file is not blocked under V1. Inode metadata ops (chmod/chown/utimes) are
    // not landlock-gated at all. Since write-confinement is currently the only
    // Linux guarantee, bumping the ABI (with `CompatLevel::BestEffort`) to cover
    // TRUNCATE is a tracked follow-up; it is left at V1 here because the Linux
    // path is not yet runtime-verified.
    let write_access = AccessFs::from_write(ABI_VERSION);
    let ruleset = Ruleset::default()
        .handle_access(write_access)
        .map_err(to_io)?
        .create()
        .map_err(to_io)?;

    // Grant write to existing writable paths (a missing path yields no fd and
    // is simply skipped — it cannot be written to anyway).
    let rules = landlock::path_beneath_rules(writable, write_access);
    let ruleset = ruleset.add_rules(rules).map_err(to_io)?;

    ruleset.restrict_self().map_err(to_io)?;
    Ok(())
}

fn to_io<E: std::fmt::Display>(e: E) -> io::Error {
    io::Error::other(format!("landlock: {e}"))
}
