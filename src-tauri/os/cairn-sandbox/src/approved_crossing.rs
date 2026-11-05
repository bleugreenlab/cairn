//! What an approved fence crossing adds to a policy, and the reason when
//! nothing bounded can be added.
//!
//! An approval answers one question about one denial: may the item touch this
//! path in this way. So a rerun after an approval keeps the policy the denied
//! attempt ran under and adds exactly that access to that path. When the
//! denial names no path, or an operation the policy cannot grant on its own,
//! there is no bounded addition, and the answer is a refusal with its reason.
//! It is never a rerun with the fence lifted: a policy nobody wrote is not a
//! decision anyone made. See `docs/worktree-fence.md`, "Approved crossings".

use std::path::{Path, PathBuf};

use cairn_common::executor_protocol::{CrossingAccess, CrossingRerun};

use crate::operator_credential::OperatorCredentialFloor;
use crate::scratch_refusal::{is_cairn_home_name, is_user_home};
use crate::write_refusal::resolve_through_existing_ancestor;
use crate::{paths_overlap, NetworkScope, ReadScope, SandboxPolicy};

/// Why a denial that recovered no path cannot be approved.
pub const NO_PATH_RECOVERED: &str = "This item's output reads as a permission denial, and the sandbox could not say which path, if any, it refused. So there is nothing bounded to approve: an approval would have to rerun the item with no sandbox at all, and an approval never does that. If the denial is the fence's, keep the work inside the checkout or `$TMPDIR`. If the work genuinely needs to reach outside them, an operator can set this agent's fence to `allow`.";

/// Why a denial under a spawn the fence did not confine cannot be approved.
pub const NO_POLICY_TO_WIDEN: &str = "This item ran with no fence policy, so the refusal did not come from the fence and no approval changes it.";

/// The directory an executor keeps its cells in, under its home or a mounted
/// build root.
const CELLS_DIR: &str = "build-slots";

/// The access an approval can add back for a refused kernel operation, when
/// there is one.
///
/// An ordinary macOS fence profile tags exactly two kinds of deny rule: the
/// write confinement (`file-write*`) and the sensitive-read denylist
/// (`file-read*`). Anything else a violation names (a socket, a Mach service,
/// a signal) is outside what the fence grants and has no bounded addition.
pub fn crossing_access(operation: &str) -> Option<CrossingAccess> {
    if operation.starts_with("file-write") {
        Some(CrossingAccess::Write)
    } else if operation.starts_with("file-read") {
        Some(CrossingAccess::Read)
    } else {
        None
    }
}

/// What approving a denial would give the rerun of an item `policy` confined:
/// the bounded addition, or why there is none.
///
/// `policy` is the policy the denied attempt ran under, or `None` when the
/// fence applied none. `path` and `operation` are what the kernel's own
/// violation record said; `path` is `None` when nothing was recovered. Output
/// text the command printed must never be passed as `path`: the result scopes
/// a grant.
pub fn bounded_rerun(
    policy: Option<&SandboxPolicy>,
    path: Option<&Path>,
    operation: Option<&str>,
) -> CrossingRerun {
    let refused = |reason: String| CrossingRerun::Refused { reason };
    let Some(policy) = policy else {
        return refused(NO_POLICY_TO_WIDEN.into());
    };
    let Some(path) = path else {
        return refused(NO_PATH_RECOVERED.into());
    };
    let Some(operation) = operation else {
        return refused(format!(
            "The sandbox refused an access to {} but did not record which operation, so there is no bounded grant to approve and an approval cannot rerun the item.",
            path.display()
        ));
    };
    let Some(access) = crossing_access(operation) else {
        return refused(format!(
            "The sandbox refused a `{operation}` of {}, which is neither a read nor a write, so the fence has no bounded grant for it and an approval cannot rerun the item.",
            path.display()
        ));
    };
    match policy.clone().grant_crossing(path, access) {
        Ok(()) => CrossingRerun::Widened { access },
        Err(reason) => refused(reason),
    }
}

impl SandboxPolicy {
    /// Add exactly what an approved crossing grants: `access` to `path`, and
    /// to everything beneath it when it is a directory, since every grant in
    /// a policy is a subpath rule.
    ///
    /// The one place a fence approval becomes policy. An addition this policy
    /// cannot make without granting more than the approval said is refused
    /// with the reason, in words for the agent and the operator.
    pub fn grant_crossing(&mut self, path: &Path, access: CrossingAccess) -> Result<(), String> {
        let shown = path.display();
        if !path.is_absolute() {
            return Err(format!(
                "The sandbox reported {shown}, which is not an absolute path, so there is no bounded grant to approve."
            ));
        }
        if !matches!(
            (&self.reach.reads, self.reach.network),
            (ReadScope::Broad, NetworkScope::Ambient)
        ) {
            return Err(
                "This item runs under a narrowed policy that is complete as built, so no approval widens it."
                    .into(),
            );
        }
        if let Some(held) = self.held_beneath(path) {
            return Err(format!(
                "Approving access to {shown} would also open {}, which lies beneath it, so it is not one bounded grant. Name a path inside it instead.",
                held.display()
            ));
        }
        if let Some(state) = cairn_state_reached_by(path) {
            return Err(format!(
                "Approving access to {shown} would reach {}, which is Cairn's own state: its settings, databases and credentials, or the cells other work runs in. No approval opens that.",
                state.display()
            ));
        }
        match access {
            CrossingAccess::Write => {
                if !self.worktree_writable && paths_overlap(path, &self.worktree) {
                    return Err(format!(
                        "{shown} is in the read-only checkout this item runs in, and no approval opens it to writes."
                    ));
                }
                if let Some(denied) = self
                    .deny_read
                    .iter()
                    .find(|denied| paths_overlap(path, denied))
                {
                    return Err(format!(
                        "A writable path is readable too, and {shown} overlaps the sensitive-read denylist entry {}, so a write grant there would also reopen reads the approval did not cover.",
                        denied.display()
                    ));
                }
                // Landlock opens each writable path when the ruleset is
                // installed and skips one that is missing, so a grant naming a
                // file the rerun would create grants nothing, and the nearest
                // grant that would work, its parent, is wider than the approval.
                #[cfg(target_os = "linux")]
                if !path.exists() {
                    return Err(format!(
                        "{shown} does not exist yet, and Landlock can grant only a path that exists. Granting its parent directory instead would be wider than the approval, so the approval cannot rerun the item."
                    ));
                }
                push_once(&mut self.writable_extra, path);
            }
            CrossingAccess::Read => push_once(&mut self.readable_extra, path),
        }
        Ok(())
    }

    /// A root this policy or the host holds that a subpath grant of `path`
    /// would contain: the filesystem root, the user's home, the checkout.
    fn held_beneath(&self, path: &Path) -> Option<PathBuf> {
        let path = canonical(path);
        let mut held = vec![PathBuf::from("/"), self.worktree.clone()];
        held.extend(crate::home_dir());
        held.into_iter()
            .find(|root| canonical(root).starts_with(&path))
    }
}

/// The part of Cairn's own state a grant of `path` would reach, in either
/// direction: every grant is a subpath rule, so one that contains a protected
/// directory opens it, and the command chooses which path gets denied (`touch
/// ~/.cairn` is a denied `file-write-times` of the directory itself). A path
/// inside one is refused too, because a database or another job's checkout is no
/// safer one file at a time.
///
/// The homes this process can name are checked first, then the family by shape,
/// because the path may be on a machine or in an account this process cannot
/// list. A project's own in-repo `.cairn/` is not a home: it is not directly in
/// a user's home directory.
pub fn cairn_state_reached_by(path: &Path) -> Option<PathBuf> {
    let resolved = resolve_through_existing_ancestor(path);
    for candidate in [path, resolved.as_path()] {
        if let Some(home) = OperatorCredentialFloor::for_this_machine()
            .every_home()
            .into_iter()
            .find(|home| paths_overlap(candidate, home))
        {
            return Some(home);
        }
        // A user's home holds a Cairn home by convention, and so does what
        // holds the user homes, whether or not this process can see inside.
        if is_user_home(candidate) || matches!(candidate.to_str(), Some("/Users" | "/home")) {
            return Some(candidate.to_path_buf());
        }
        if let Some(found) = candidate.ancestors().find(|ancestor| {
            let name = ancestor.file_name().and_then(|name| name.to_str());
            name == Some(CELLS_DIR)
                || (name.is_some_and(is_cairn_home_name)
                    && ancestor.parent().is_some_and(is_user_home))
        }) {
            return Some(found.to_path_buf());
        }
        // A mounted build root: the directory that holds the cells.
        if candidate.join(CELLS_DIR).is_dir() {
            return Some(candidate.to_path_buf());
        }
    }
    None
}

fn canonical(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

fn push_once(paths: &mut Vec<PathBuf>, path: &Path) {
    if !paths.iter().any(|existing| existing == path) {
        paths.push(path.to_path_buf());
    }
}

/// A directory for a crossing test's outside paths. It must not be under a Cairn
/// home, and a check lane's `$TMPDIR` is under the executor's.
#[cfg(test)]
pub(crate) fn outside_cairn_state() -> tempfile::TempDir {
    let temp = std::env::temp_dir();
    let parent = if cairn_state_reached_by(&temp).is_some() {
        PathBuf::from("/tmp")
    } else {
        temp
    };
    tempfile::tempdir_in(parent).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ordinary(worktree: &Path) -> SandboxPolicy {
        SandboxPolicy::for_run(worktree, &[], vec![PathBuf::from("/home/u/.ssh")])
    }

    #[test]
    fn only_reads_and_writes_have_an_access_to_add_back() {
        for write in ["file-write-create", "file-write-data", "file-write-unlink"] {
            assert_eq!(
                crossing_access(write),
                Some(CrossingAccess::Write),
                "{write}"
            );
        }
        for read in ["file-read-data", "file-read-metadata"] {
            assert_eq!(crossing_access(read), Some(CrossingAccess::Read), "{read}");
        }
        for other in ["network-outbound", "mach-lookup", "signal", "process-exec"] {
            assert_eq!(crossing_access(other), None, "{other}");
        }
    }

    #[test]
    fn a_write_grant_adds_that_path_and_nothing_else() {
        let outside = outside_cairn_state();
        let target = outside.path().join("a");
        std::fs::write(&target, "").unwrap();
        let before = ordinary(Path::new("/work/wt"));
        let mut after = before.clone();
        after
            .grant_crossing(&target, CrossingAccess::Write)
            .unwrap_or_else(|reason| panic!("{reason}"));
        let mut expected = before.writable_paths();
        expected.push(target.clone());
        assert_eq!(after.writable_paths(), expected);
        assert_eq!(after.readable_extra, before.readable_extra);
        assert_eq!(after.deny_read, before.deny_read);
        assert_eq!(
            after.socket_resolvable_paths(),
            before.socket_resolvable_paths()
        );
        assert_eq!(after.reach, before.reach);

        // Granting it twice is still one rule.
        after
            .grant_crossing(&target, CrossingAccess::Write)
            .unwrap_or_else(|reason| panic!("{reason}"));
        assert_eq!(after.writable_paths(), expected);
    }

    /// Landlock skips a missing path, so a grant for a file the rerun would
    /// create could only be made by granting its parent, which is wider.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_write_grant_for_a_missing_path_is_refused_on_linux() {
        let outside = outside_cairn_state();
        let reason = ordinary(Path::new("/work/wt"))
            .grant_crossing(&outside.path().join("not-yet"), CrossingAccess::Write)
            .expect_err("Landlock cannot name a missing path");
        assert!(reason.contains("does not exist yet"), "{reason}");
    }

    #[test]
    fn a_read_grant_never_joins_the_write_set() {
        let before = ordinary(Path::new("/work/wt"));
        let mut after = before.clone();
        let key = Path::new("/home/u/.ssh/known_hosts");
        after
            .grant_crossing(key, CrossingAccess::Read)
            .unwrap_or_else(|reason| panic!("{reason}"));
        assert_eq!(after.writable_paths(), before.writable_paths());
        assert_eq!(after.readable_extra, vec![key.to_path_buf()]);
        assert!(after.readable_paths().contains(&key.to_path_buf()));
    }

    #[test]
    fn a_grant_that_would_hold_a_root_is_refused() {
        let worktree = Path::new("/work/wt");
        for broad in [Path::new("/"), Path::new("/work"), worktree] {
            for access in [CrossingAccess::Write, CrossingAccess::Read] {
                let reason = ordinary(worktree)
                    .grant_crossing(broad, access)
                    .expect_err("a grant containing a held root is not bounded");
                assert!(reason.contains("lies beneath it"), "{reason}");
            }
        }
        if let Some(home) = crate::home_dir() {
            let reason = ordinary(worktree)
                .grant_crossing(&home, CrossingAccess::Write)
                .expect_err("the home directory is not one bounded grant");
            assert!(reason.contains("lies beneath it"), "{reason}");
        }
    }

    /// `touch ~/.cairn` is a denied `file-write-times` of the directory itself,
    /// so without this an approval would open the whole home for writes, the
    /// settings document with it. A path inside one is refused the same way.
    #[test]
    fn a_denial_on_a_cairn_home_itself_has_no_bounded_rerun() {
        let policy = ordinary(Path::new("/work/wt"));
        let home = crate::home_dir().expect("a test runs with a home");
        let mounted = tempfile::tempdir().unwrap();
        std::fs::create_dir(mounted.path().join(CELLS_DIR)).unwrap();
        for reached in [
            home.join(".cairn"),
            home.join(".cairn-dev-peer"),
            home.join(".cairn/settings.yaml"),
            home.join(".cairn/cairn.db"),
            cairn_common::paths::cairn_home(),
            PathBuf::from("/home/dev/.cairn-exec"),
            PathBuf::from("/Users/other/.cairn"),
            PathBuf::from("/Users/other"),
            PathBuf::from("/home"),
            PathBuf::from("/home/dev/.cairn-exec/build-slots/cairn/slot-9/src/lib.rs"),
            PathBuf::from("/Volumes/builds/build-slots"),
            PathBuf::from("/Volumes/builds/build-slots/cairn/slot-9"),
            mounted.path().to_path_buf(),
        ] {
            for operation in ["file-write-times", "file-write-create", "file-read-data"] {
                match bounded_rerun(Some(&policy), Some(&reached), Some(operation)) {
                    CrossingRerun::Refused { reason } => assert!(
                        reason.contains("lies beneath it") || reason.contains("own state"),
                        "{}: {reason}",
                        reached.display()
                    ),
                    other => panic!("{} {operation}: {other:?}", reached.display()),
                }
            }
        }
    }

    /// A project's own `.cairn/` is project content, not a home, and neither a
    /// directory that merely shares the prefix away from a user's home.
    #[test]
    fn an_in_repo_cairn_folder_is_not_cairn_state() {
        for elsewhere in ["/home/dev/projects/app/.cairn/notes", "/opt/.cairn-tools/x"] {
            assert_eq!(
                cairn_state_reached_by(Path::new(elsewhere)),
                None,
                "{elsewhere}"
            );
        }
    }

    #[test]
    fn a_write_grant_never_reopens_a_readonly_checkout_or_a_denied_read() {
        let checkout = Path::new("/project/live");
        let mut readonly = SandboxPolicy::for_readonly_checkout(checkout, &[], vec![]);
        let reason = readonly
            .grant_crossing(&checkout.join("src/a.rs"), CrossingAccess::Write)
            .expect_err("the read-only checkout outranks an approval");
        assert!(reason.contains("read-only checkout"), "{reason}");

        let reason = ordinary(Path::new("/work/wt"))
            .grant_crossing(Path::new("/home/u/.ssh/config"), CrossingAccess::Write)
            .expect_err("a writable path is readable, so it would reopen the denylist");
        assert!(reason.contains("denylist entry /home/u/.ssh"), "{reason}");
    }

    #[test]
    fn a_narrowed_policy_takes_no_grant() {
        let mut page_call = SandboxPolicy::for_page_call(Path::new("/b/pc/1"), &[], 4321);
        let reason = page_call
            .grant_crossing(Path::new("/outside/a"), CrossingAccess::Write)
            .expect_err("a page-call policy is complete as built");
        assert!(reason.contains("complete as built"), "{reason}");
    }

    #[test]
    fn a_denial_without_a_bounded_addition_is_refused_with_its_reason() {
        let policy = ordinary(Path::new("/work/wt"));
        let refused = |rerun: CrossingRerun| match rerun {
            CrossingRerun::Refused { reason } => reason,
            other => panic!("expected a refusal, got {other:?}"),
        };
        assert_eq!(
            refused(bounded_rerun(Some(&policy), None, None)),
            NO_PATH_RECOVERED
        );
        assert_eq!(
            refused(bounded_rerun(
                None,
                Some(Path::new("/outside/a")),
                Some("file-write-create")
            )),
            NO_POLICY_TO_WIDEN
        );
        let reason = refused(bounded_rerun(
            Some(&policy),
            Some(Path::new("/outside/a")),
            Some("network-outbound"),
        ));
        assert!(reason.contains("`network-outbound`"), "{reason}");
        let reason = refused(bounded_rerun(
            Some(&policy),
            Some(Path::new("/outside/a")),
            None,
        ));
        assert!(
            reason.contains("did not record which operation"),
            "{reason}"
        );
    }

    #[test]
    fn a_boundable_denial_names_the_access_the_rerun_gets() {
        let policy = ordinary(Path::new("/work/wt"));
        let outside = outside_cairn_state();
        let target = outside.path().join("a");
        std::fs::write(&target, "").unwrap();
        assert_eq!(
            bounded_rerun(Some(&policy), Some(&target), Some("file-write-data")),
            CrossingRerun::Widened {
                access: CrossingAccess::Write
            }
        );
        assert_eq!(
            bounded_rerun(
                Some(&policy),
                Some(Path::new("/home/u/.ssh/known_hosts")),
                Some("file-read-data")
            ),
            CrossingRerun::Widened {
                access: CrossingAccess::Read
            }
        );
    }
}
