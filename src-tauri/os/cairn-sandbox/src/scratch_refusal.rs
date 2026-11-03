//! Why a fenced spawn may not write under Cairn's scratch root, said in one
//! sentence wherever the fence refuses it.
//!
//! The root (`<cairn_home>/scratch`, [`cairn_common::scratch::scratch_root`])
//! holds the directory Cairn keeps for each agent process: its system prompt
//! file, terminal logs and dependency link. That is Cairn's bookkeeping,
//! reclaimed when the node ends, not a workspace. The fence grants no part of
//! the root by default; a spawn whose `$TMPDIR` is its own directory there is
//! granted exactly that directory through [`crate::SandboxPolicy::grant_tmpdir`].
//! Before that, the root was granted whole, and agents made sibling workspaces
//! beside their own directory that nothing owned or reclaimed.
//!
//! The sentence is shared by the fence's route refusal (the `run` and `write`
//! surfaces) and by the out-of-bounds write note a failed command carries, so
//! an agent reads the same answer however the write reached the boundary.

use std::path::{Component, Path, PathBuf};

use crate::write_refusal::{absolute_paths_in, resolve_through_existing_ancestor};

/// Whether `path` is Cairn's scratch root or anything beneath it.
///
/// Compared lexically and again through the deepest existing ancestor
/// canonicalized, so a path spelled through `/var`, `/tmp` or a firmlink matches
/// a root that resolves to `/private/...`, and a `..` cannot walk a path that
/// leaves the root back into it, or one that enters it back out.
pub fn is_scratch_root_write(path: &Path) -> bool {
    is_under_root(path, &cairn_common::scratch::scratch_root())
}

/// The Cairn scratch root `path` is under: this process's own, or, matched by
/// structure, any `scratch` directory directly inside a Cairn home (`.cairn` or
/// `.cairn-*` sitting directly in a user's home directory), or `None`.
///
/// The home must sit in a user's home directory because a project keeps its
/// own `.cairn/` folder in its repository, and an executor's cell checkouts
/// carry it too (`~/.cairn-exec/build-slots/<project>/slot-N/.cairn/`); a
/// `scratch` directory there is project content, not Cairn's bookkeeping.
///
/// The fence adjudicates on the runner, but a denied write may have been made
/// on a remote executor whose Cairn home the runner does not know. An approved
/// crossing reruns the item with no fence at all, so a root refused by name
/// here and merely prompted for there would be one place with two rules. The
/// structural match closes that without a protocol field.
pub fn scratch_root_containing(path: &Path) -> Option<PathBuf> {
    let local = cairn_common::scratch::scratch_root();
    if is_under_root(path, &local) {
        return Some(local);
    }
    if !path.is_absolute() {
        return None;
    }
    lexically_normal(path)
        .ancestors()
        .find(|ancestor| {
            ancestor.file_name() == Some(std::ffi::OsStr::new(SCRATCH_DIR))
                && ancestor.parent().is_some_and(|cairn_home| {
                    cairn_home
                        .file_name()
                        .and_then(|name| name.to_str())
                        .is_some_and(is_cairn_home_name)
                        && cairn_home.parent().is_some_and(is_user_home)
                })
        })
        .map(Path::to_path_buf)
}

/// Whether `dir` is a user's home directory by shape (`/Users/<name>`,
/// `/home/<name>`, `/root`, `/var/root`), or this process's own home.
fn is_user_home(dir: &Path) -> bool {
    if crate::home_dir().is_some_and(|home| home == dir) {
        return true;
    }
    let parts: Vec<_> = dir.components().collect();
    match parts.as_slice() {
        [Component::RootDir, Component::Normal(base), Component::Normal(_)] => {
            *base == "Users" || *base == "home"
        }
        _ => dir == Path::new("/root") || dir == Path::new("/var/root"),
    }
}

const SCRATCH_DIR: &str = "scratch";

/// `.cairn`, or a `.cairn-` home: a dev instance's, an executor's.
fn is_cairn_home_name(name: &str) -> bool {
    name == ".cairn" || name.starts_with(".cairn-")
}

fn is_under_root(path: &Path, root: &Path) -> bool {
    if !path.is_absolute() {
        return false;
    }
    let path = lexically_normal(path);
    let root = lexically_normal(root);
    path.starts_with(&root)
        || resolve_through_existing_ancestor(&path)
            .starts_with(resolve_through_existing_ancestor(&root))
}

/// `path` with `.` dropped and each `..` folded into its parent, without
/// touching the file system.
fn lexically_normal(path: &Path) -> PathBuf {
    let mut normal = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normal.pop();
            }
            other => normal.push(other.as_os_str()),
        }
    }
    normal
}

/// The first absolute path named in `line` that is under the scratch root.
///
/// `line` is a denial line out of a command's own output, which the agent's
/// code wrote. A caller may use the answer only to choose this refusal over a
/// prompt, never as a scope: a refusal gives the agent less than the prompt it
/// replaces, so a forged line costs its author an answer and widens nothing.
pub fn reported_scratch_root_write(line: &str) -> Option<PathBuf> {
    absolute_paths_in(line).find(|path| scratch_root_containing(path).is_some())
}

/// The refusal for a write to `path` under a scratch root, naming the root it
/// is under ([`scratch_root_containing`]; this process's own otherwise).
pub fn scratch_root_refusal(path: &Path) -> String {
    format!(
        "Cairn refused this write: `{path}` is inside Cairn's scratch root (`{root}`), which \
         holds Cairn's own per-process bookkeeping (prompt files, terminal logs, dependency \
         links) and is removed when the node ends. No permission grant opens it. Put working \
         data under `$TMPDIR` instead: it is this node's scratch, follows the node to whichever \
         machine runs its work, and is kept until the issue resolves or the thread closes, up \
         to the project's `scratch.ceilingMiB`. Data larger than that is a build-root or \
         external-volume question for the operator.",
        path = path.display(),
        root = scratch_root_containing(path)
            .unwrap_or_else(cairn_common::scratch::scratch_root)
            .display(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_root_and_everything_beneath_it_match() {
        let root = Path::new("/home/x/.cairn/scratch");
        for path in [
            "/home/x/.cairn/scratch",
            "/home/x/.cairn/scratch/",
            "/home/x/.cairn/scratch/triome-45-measure",
            "/home/x/.cairn/scratch/cairn.1.1.builder/terminals/dev.log",
            "/home/x/.cairn/scratch/./sibling",
            "/home/x/.cairn/other/../scratch/sibling",
        ] {
            assert!(is_under_root(Path::new(path), root), "{path}");
        }
    }

    #[test]
    fn paths_outside_the_root_do_not_match() {
        let root = Path::new("/home/x/.cairn/scratch");
        for path in [
            "/home/x/.cairn",
            "/home/x/.cairn/scratch-other/x",
            "/home/x/.cairn/tmp/n1234/tree/x",
            "/home/x/.cairn/scratch/../tmp/x",
            "relative/scratch",
            "/tmp/x",
        ] {
            assert!(!is_under_root(Path::new(path), root), "{path}");
        }
    }

    /// On macOS `/var` is a link to `/private/var`, so a root and a path spelled
    /// either way must agree.
    #[test]
    fn a_root_reached_through_a_link_matches_either_spelling() {
        let base = tempfile::tempdir().unwrap();
        let real = base.path().join("real");
        let root = real.join("scratch");
        std::fs::create_dir_all(&root).unwrap();
        let alias = base.path().join("alias");
        #[cfg(unix)]
        std::os::unix::fs::symlink(&real, &alias).unwrap();
        #[cfg(not(unix))]
        return;

        assert!(is_under_root(&alias.join("scratch/sibling"), &root));
        assert!(is_under_root(&root.join("sibling"), &alias.join("scratch")));
        assert!(!is_under_root(&alias.join("elsewhere"), &root));
    }

    /// A remote executor's scratch root, whose home the runner does not know,
    /// is matched by structure, and the refusal names that root.
    #[test]
    fn another_cairn_homes_scratch_root_is_matched_by_structure() {
        for (path, root) in [
            ("/home/dev/.cairn/scratch/lab", "/home/dev/.cairn/scratch"),
            (
                "/home/dev/.cairn-executor/scratch/a/b",
                "/home/dev/.cairn-executor/scratch",
            ),
            (
                "/Users/u/.cairn-dev-k1/scratch",
                "/Users/u/.cairn-dev-k1/scratch",
            ),
            // This machine's own dev-instance homes are Cairn homes too.
            (
                "/Users/u/.cairn-dev/scratch/x",
                "/Users/u/.cairn-dev/scratch",
            ),
        ] {
            assert_eq!(
                scratch_root_containing(Path::new(path)),
                Some(PathBuf::from(root)),
                "{path}"
            );
            assert!(scratch_root_refusal(Path::new(path)).contains(&format!("(`{root}`)")));
        }
        for path in [
            "/home/dev/.cairnrc/scratch/x",
            "/home/dev/project/scratch/x",
            "/home/dev/.cairn/tmp/n1/tree/x",
            "/home/dev/.cairn/scratch-old/x",
            "relative/.cairn/scratch/x",
            // A project's own `.cairn/` folder, in a checkout or a cell.
            "/Users/u/projects/app/.cairn/scratch/x",
            "/home/dev/.cairn-exec/build-slots/app/slot-1/.cairn/scratch/x",
        ] {
            assert_eq!(scratch_root_containing(Path::new(path)), None, "{path}");
        }
        let line = "mkdir: cannot create directory '/home/dev/.cairn-executor/scratch/x': Permission denied";
        assert_eq!(
            reported_scratch_root_write(line),
            Some(PathBuf::from("/home/dev/.cairn-executor/scratch/x"))
        );
    }

    #[test]
    fn a_reported_linux_denial_line_names_its_scratch_path() {
        let root = cairn_common::scratch::scratch_root();
        let line = format!(
            "mkdir: cannot create directory '{}': Permission denied",
            root.join("x").display()
        );
        assert_eq!(reported_scratch_root_write(&line), Some(root.join("x")));
        assert_eq!(
            reported_scratch_root_write(
                "mkdir: cannot create directory '/tmp/x': Permission denied"
            ),
            None
        );
    }

    #[test]
    fn the_refusal_names_the_path_the_root_and_the_one_place_to_write() {
        let root = cairn_common::scratch::scratch_root();
        let refusal = scratch_root_refusal(&root.join("lab"));
        assert!(refusal.contains(&format!("`{}`", root.join("lab").display())));
        assert!(refusal.contains(&format!("(`{}`)", root.display())));
        assert!(refusal.contains("`$TMPDIR`"));
        assert!(refusal.contains("No permission grant opens it."));
        assert!(refusal.contains("`scratch.ceilingMiB`"));
    }
}
