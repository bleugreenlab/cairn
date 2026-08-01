//! Shape of a Cairn scratch directory — the sanctioned writable temp dir a job
//! or cell is handed as `$TMPDIR`.
//!
//! Two layers provision one: `cairn_core::scratch` names and registers the
//! host-local job scratch dir, and `cairn_executor` derives a cell's own. Which
//! of them served a given execution is not something an agent can see or should
//! have to care about, so the parts of a scratch dir's shape that decide whether
//! ordinary tooling behaves normally live here, once, and both delegate.

use std::path::{Path, PathBuf};

/// Directory name of the host-owned scratch root inside the Cairn home.
const SCRATCH_ROOT_DIR: &str = "scratch";

/// Root of the host-owned per-job scratch directories: `<cairn_home>/scratch`.
///
/// Anchoring on the Cairn home rather than the platform temp root is what keeps
/// an agent-visible scratch path readable. `std::env::temp_dir()` resolves to
/// `/var/folders/<xx>/<opaque-hash>/T` on macOS, so every path Cairn advertised
/// out of a scratch dir — a terminal log line, an attachment path — carried a
/// per-user hash segment nobody can read back, remember, or retype. Nothing
/// about a scratch dir needs the platform temp root: Cairn provisions it, points
/// `TMPDIR`/`TMP`/`TEMP` at it, and reclaims it at teardown, so it belongs in
/// Cairn's own home beside the build cells whose scratch already lives there.
///
/// Keying on the Cairn home also separates instances that previously shared one
/// temp root: a dev instance (`~/.cairn-dev-<key>`) and the production app can
/// hold jobs with identical node coordinates without landing on one directory.
///
/// This root sits outside the platform temp dirs the fence grants wholesale, so
/// `cairn_sandbox::default_writable_extra` grants it by name.
pub fn scratch_root() -> PathBuf {
    crate::paths::cairn_home().join(SCRATCH_ROOT_DIR)
}

/// Directory name of an installed JavaScript dependency tree — the same in a
/// checkout and in the scratch link that stands in for it.
const NODE_MODULES: &str = "node_modules";

/// Give a scratch directory the dependency resolution a checkout would have, by
/// linking `<scratch>/node_modules` at `<checkout>/node_modules`.
///
/// Without this, a helper script written to `$TMPDIR` cannot import the project's
/// packages at all. Node and bun resolve a bare specifier such as `@cairn/sdk` by
/// walking up from the *importing file's* own directory looking for a
/// `node_modules`; the process cwd never enters into it, and bun ignores
/// `NODE_PATH` outright. A scratch dir sits outside the checkout, so that walk
/// passes no `node_modules` however the script is launched, and the failure reads
/// as a missing dependency rather than as a missing path. The link restores the
/// one entry the walk expects to find.
///
/// Inline code is unaffected either way, because it reaches its interpreter as
/// source rather than as a file: `bun -e` has no importing file to walk up from
/// and resolves from cwd instead, which is already the checkout root.
///
/// Python needs no counterpart. uv discovers a project from the process cwd, so a
/// scratch script already resolves the surrounding project's environment, and a
/// script carrying PEP 723 metadata gets its own environment from that metadata
/// wherever the file happens to live.
///
/// The link is created whether or not the checkout has a `node_modules` yet, which
/// is deliberate: an install can land at any point in a job's life, long after the
/// environment was provisioned. An unresolvable link is skipped by the upward walk
/// exactly as an absent one is, so pointing at the eventual location costs nothing
/// and begins working the moment the install arrives.
///
/// Best-effort and idempotent, like the rest of scratch provisioning: a failure is
/// logged and leaves the directory fully usable for everything else.
pub fn link_scratch_dependency_resolution(scratch: &Path, checkout: &Path) {
    let target = checkout.join(NODE_MODULES);
    let link = scratch.join(NODE_MODULES);
    // A scratch dir handed out as a process residence has no checkout beside it —
    // the agent CLI process and the REPL controller both run that way — and asking
    // it to resolve against itself would produce a self-referential link.
    if link == target {
        return;
    }
    match std::fs::symlink_metadata(&link) {
        // Whether an entry can be followed is the discriminator, rather than any
        // link-type flag: `read_link` succeeds for a symlink and for a Windows
        // junction, and fails for a real directory. That keeps this branch correct
        // without depending on how a given platform classifies its own link kind.
        Ok(_) => match std::fs::read_link(&link) {
            Ok(current) if points_at(&current, &target) => return,
            // A link pointing elsewhere means this scratch dir was last used with a
            // different checkout. Re-point it: left alone it would silently resolve
            // a helper script against another project's packages.
            Ok(_) => {
                if let Err(error) = remove_link(&link) {
                    tracing::warn!(
                        "could not replace stale dependency link {}: {error}",
                        link.display()
                    );
                    return;
                }
            }
            // Anything real here belongs to whoever put it there; never clobber it.
            Err(_) => return,
        },
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            tracing::warn!("could not inspect {}: {error}", link.display());
            return;
        }
    }
    if let Err(error) = create_link(&target, &link) {
        tracing::warn!(
            "could not link {} to {}: {error}",
            link.display(),
            target.display()
        );
    }
}

/// Whether an existing link entry already points at `target`, so provisioning can
/// leave it alone. The recorded path is compared first, then the resolved paths,
/// because Windows reports a junction's target in its own normalized form and a
/// spelling difference is not a reason to tear a working link down and rebuild it.
fn points_at(recorded: &Path, target: &Path) -> bool {
    if recorded == target {
        return true;
    }
    match (recorded.canonicalize(), target.canonicalize()) {
        (Ok(recorded), Ok(target)) => recorded == target,
        _ => false,
    }
}

#[cfg(unix)]
fn create_link(target: &Path, link: &Path) -> std::io::Result<()> {
    std::os::unix::fs::symlink(target, link)
}

/// Windows gets a **directory junction**, not a symlink. `symlink_dir` is the
/// obvious call and the wrong one: `CreateSymbolicLinkW` requires
/// `SeCreateSymbolicLinkPrivilege` — elevation, or Developer Mode — so on an
/// ordinary unprivileged install it fails, leaving the capability broken for
/// exactly the users who cannot fix it. A junction is a reparse point any user may
/// create in a directory they can already write, and it is what the JavaScript
/// ecosystem itself depends on: npm links workspace packages into `node_modules`
/// with junctions on Windows and pnpm does the same, so resolving a package
/// through one is the well-trodden path rather than a novel trick.
///
/// A junction may also be created before its target exists, which is what keeps
/// the link-ahead-of-install behavior identical on every platform. A symlink would
/// have allowed that too; elevation is the reason it is not used.
///
/// `mklink /J` is a `cmd` builtin, so it goes through the command processor, and
/// the whole tail is passed verbatim with both paths explicitly quoted: `cmd`
/// re-parses its own command line, and Rust's ordinary argument quoting is not
/// what it expects, so an unquoted path containing a space would be split. Two
/// bounded limits remain, each degrading to the same logged warning as any other
/// failure — a junction must target a local volume, and `cmd` expands `%NAME%`
/// even inside a quoted argument.
#[cfg(windows)]
fn create_link(target: &Path, link: &Path) -> std::io::Result<()> {
    use std::os::windows::process::CommandExt as _;
    let status = std::process::Command::new("cmd")
        .raw_arg(format!(
            "/c mklink /J \"{}\" \"{}\"",
            link.display(),
            target.display()
        ))
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()?;
    if status.success() {
        Ok(())
    } else {
        Err(std::io::Error::other(format!(
            "`mklink /J` failed with {status}"
        )))
    }
}

#[cfg(unix)]
fn remove_link(link: &Path) -> std::io::Result<()> {
    std::fs::remove_file(link)
}

/// A junction, like a directory symlink, is a directory entry on Windows, so it is
/// unlinked with `remove_dir`. That removes the link itself and never recurses
/// into the checkout behind it.
#[cfg(windows)]
fn remove_link(link: &Path) -> std::io::Result<()> {
    std::fs::remove_dir(link)
}

/// These assert behavior rather than the link primitive — that a package is
/// readable through the link, that a stale one stops resolving — so the same cases
/// exercise a Unix symlink and a Windows junction without being written twice or
/// depending on how either platform classifies its own link kind.
#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    /// A checkout and a scratch dir as the two provisioners lay them out: siblings
    /// with no containment either way.
    fn pair() -> (tempfile::TempDir, PathBuf, PathBuf) {
        let temp = tempfile::tempdir().unwrap();
        let checkout = temp.path().join("checkout");
        let scratch = temp.path().join("scratch");
        std::fs::create_dir_all(&checkout).unwrap();
        std::fs::create_dir_all(&scratch).unwrap();
        (temp, checkout, scratch)
    }

    /// Write an installed package the way an install leaves one.
    fn install_package(checkout: &Path, name: &str) {
        let dir = checkout.join(NODE_MODULES).join(name);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("index.js"), b"installed").unwrap();
    }

    #[test]
    fn a_scratch_script_reaches_the_checkouts_packages_through_the_link() {
        let (_temp, checkout, scratch) = pair();
        install_package(&checkout, "probe-pkg");

        link_scratch_dependency_resolution(&scratch, &checkout);

        // The upward walk from a file in `scratch` now finds a `node_modules` whose
        // contents are the checkout's own.
        assert_eq!(
            std::fs::read(scratch.join("node_modules/probe-pkg/index.js")).unwrap(),
            b"installed"
        );
    }

    /// The link is provisioned before any install runs, so pointing at a directory
    /// that does not exist yet is the ordinary case rather than the exception.
    #[test]
    fn linking_ahead_of_an_install_lets_the_later_install_take_effect() {
        let (_temp, checkout, scratch) = pair();
        assert!(!checkout.join(NODE_MODULES).exists());

        link_scratch_dependency_resolution(&scratch, &checkout);
        let link = scratch.join(NODE_MODULES);
        assert!(
            std::fs::symlink_metadata(&link).is_ok(),
            "the link must be created before the install it points at"
        );
        assert!(
            !link.exists(),
            "and must not resolve yet, which is what makes it inert for the walk"
        );

        install_package(&checkout, "probe-pkg");
        assert_eq!(
            std::fs::read(link.join("probe-pkg/index.js")).unwrap(),
            b"installed",
            "an install after provisioning must become reachable with no re-provision"
        );
    }

    /// Provisioning runs on every spawn and across resumes.
    #[test]
    fn repeated_calls_converge_on_one_link() {
        let (_temp, checkout, scratch) = pair();
        install_package(&checkout, "probe-pkg");

        for _ in 0..3 {
            link_scratch_dependency_resolution(&scratch, &checkout);
        }
        assert_eq!(
            std::fs::read(scratch.join("node_modules/probe-pkg/index.js")).unwrap(),
            b"installed",
            "provisioning repeats on every spawn, so it must converge rather than break"
        );
    }

    #[test]
    fn a_link_left_by_another_checkout_is_repointed() {
        let (temp, checkout, scratch) = pair();
        let stale = temp.path().join("other-checkout");
        install_package(&stale, "stale-pkg");
        install_package(&checkout, "current-pkg");
        link_scratch_dependency_resolution(&scratch, &stale);

        link_scratch_dependency_resolution(&scratch, &checkout);

        assert!(
            scratch.join("node_modules/current-pkg/index.js").exists(),
            "a reused scratch dir must resolve against the checkout it now serves"
        );
        assert!(
            !scratch.join("node_modules/stale-pkg").exists(),
            "and must not still resolve against the previous checkout"
        );
    }

    #[test]
    fn a_real_directory_placed_by_whoever_owns_the_scratch_dir_is_left_alone() {
        let (_temp, checkout, scratch) = pair();
        install_package(&checkout, "from-checkout");
        let theirs = scratch.join(NODE_MODULES).join("theirs");
        std::fs::create_dir_all(&theirs).unwrap();

        link_scratch_dependency_resolution(&scratch, &checkout);

        assert!(
            theirs.exists(),
            "a real node_modules must never be clobbered"
        );
        assert!(!scratch.join("node_modules/from-checkout").exists());
    }

    /// Some spawns are handed a scratch dir as their whole residence, with no
    /// checkout beside it, and pass that same path as their cwd.
    #[test]
    fn a_residence_that_is_its_own_checkout_gets_no_self_referential_link() {
        let (_temp, _checkout, scratch) = pair();

        link_scratch_dependency_resolution(&scratch, &scratch);

        assert!(
            std::fs::symlink_metadata(scratch.join(NODE_MODULES)).is_err(),
            "a scratch dir must not be linked to itself"
        );
    }

    /// The blast-radius test. Teardown and scratch reset both remove the whole
    /// scratch tree, and the tree now contains a link into a real checkout: if that
    /// removal followed the link it would delete the project's installed packages.
    #[test]
    fn reclaiming_a_scratch_dir_does_not_delete_the_checkouts_packages() {
        let (_temp, checkout, scratch) = pair();
        install_package(&checkout, "probe-pkg");
        link_scratch_dependency_resolution(&scratch, &checkout);

        std::fs::remove_dir_all(&scratch).unwrap();

        assert!(!scratch.exists());
        assert_eq!(
            std::fs::read(checkout.join("node_modules/probe-pkg/index.js")).unwrap(),
            b"installed",
            "reclaiming scratch must not reach through the link into the checkout"
        );
    }
}
