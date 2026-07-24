//! Linux sandbox: landlock write-confinement installed in a `pre_exec` hook.
//!
//! landlock is allowlist-only (no deny rules), so write-confinement is
//! expressed by handling the write access rights and granting them only to the
//! writable set; everything else becomes read-only at the kernel level. Reads
//! are left unrestricted so the toolchain (cargo/npm/git/compilers) works.
//!
//! v2 scope: **write confinement only.** The sensitive-read denylist is enforced
//! on macOS but not yet on Linux — landlock would require allowlisting every
//! readable root minus the denylist (or a `bwrap` mount-mask), tracked as a
//! follow-up. The headless target still gains a kernel write boundary.

use super::SandboxPolicy;
use std::io;
use std::os::unix::process::CommandExt;
use std::path::PathBuf;

use landlock::{AccessFs, CompatLevel, Compatible, Ruleset, RulesetAttr, RulesetCreatedAttr, ABI};

const ABI_VERSION: ABI = ABI::V2;

/// Probe whether the running kernel supports landlock ABI ≥ 2.
///
/// Creates (but does not enforce) a ruleset; the `landlock_create_ruleset`
/// syscall fails on kernels without landlock or with it disabled.
pub fn is_available() -> bool {
    Ruleset::default()
        .set_compatibility(CompatLevel::HardRequirement)
        .handle_access(AccessFs::from_write(ABI_VERSION))
        .and_then(|r| r.create())
        .is_ok()
}

/// Install a `pre_exec` hook that restricts the child to write only within the
/// policy's writable set. Fail-closed: if landlock cannot be applied, the spawn
/// fails rather than running unconfined.
///
/// For a read-only-checkout policy the live checkout is absent from
/// [`SandboxPolicy::writable_paths`], so write-confinement excludes it
/// automatically here — the kernel denies writes into the checkout while reads
/// stay unconfined (the v2 Linux scope). No Linux-specific branch is needed.
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
    // ABI 2 is the minimum viable write sandbox for build commands. ABI 1
    // implicitly rejects cross-directory rename/link with EXDEV because it
    // cannot grant REFER, breaking package managers that stage files in a temp
    // directory before installing them into the checkout. Require ABI 2 rather
    // than silently falling back to that behavior. TRUNCATE (ABI 3) and inode
    // metadata operations remain outside the current confinement guarantee.
    let write_access = AccessFs::from_write(ABI_VERSION);
    let ruleset = Ruleset::default()
        .set_compatibility(CompatLevel::HardRequirement)
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
