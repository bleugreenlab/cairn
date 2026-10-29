//! Why a confined process could not write where it tried, said where it fails.
//!
//! A fenced spawn may write only its checkout, its scratch and temp
//! directories, the toolchain caches, the executor's tool cache, and the grants
//! it carries (see [`crate::default_writable_extra`]). A tool that keeps state
//! anywhere else fails with its own `Permission denied`, which reads as a
//! file-mode problem. The specimen was `cargo install --root
//! ~/.cairn/tools/tursodb` refused `.crates.toml` in a directory the same user
//! owned and could write from any unconfined shell (cairn/5443): nothing in
//! that output said the cell's boundary was the cause, or which scope the tool
//! had needed.
//!
//! The note is appended after the command's untouched output, and only when the
//! evidence supports it. The command failed; its output names a refused path,
//! or the kernel log recovered one; that path is outside every writable scope
//! of the policy the spawn ran under; and the path's own permissions would have
//! let this user write it. The last condition is what separates the fence's
//! refusal from an ordinary file-mode error outside the cell, which would have
//! failed unconfined too and is not the fence's to explain.

use std::path::{Path, PathBuf};

use crate::{Confinement, SandboxPolicy};

/// How many lines above a denial signature a refused path is looked for in.
/// Tools commonly print the path and the errno on separate lines: cargo prints
/// `error: failed to open: PATH`, a blank line, `Caused by:`, and only then
/// `Permission denied (os error 13)`.
const PRECEDING_LINES: usize = 3;

/// Kernel pseudo-filesystems. A refused write there is never a cache that
/// belongs somewhere else: the SELinux `fscreate` warning is the common case
/// (see `is_selinux_creation_context_warning`).
const PSEUDO_FILESYSTEMS: [&str; 3] = ["/proc", "/sys", "/dev"];

/// The diagnostic to append to a failed command's output when the fence it ran
/// under refused a write outside every scope it may write, or `None` for every
/// other outcome.
///
/// `recovered` is the path the platform's own violation record named, when it
/// named one (the macOS unified log); it is preferred over a path read out of
/// the command's output.
pub fn out_of_bounds_write_note(
    confinement: &Confinement,
    exit_code: Option<i32>,
    output: &str,
    recovered: Option<&Path>,
) -> Option<String> {
    exit_code.filter(|code| *code != 0)?;
    let Confinement::Fenced(policy) = confinement else {
        return None;
    };
    let refused = recovered
        .map(Path::to_path_buf)
        .into_iter()
        .chain(refused_paths_in_output(output))
        .find(|path| refused_by_the_fence(policy, path))?;
    Some(note(policy, &refused))
}

fn note(policy: &SandboxPolicy, refused: &Path) -> String {
    let scope = if refused.is_dir() {
        refused
    } else {
        refused.parent().unwrap_or(refused)
    };
    let writable = policy
        .writable_paths()
        .iter()
        .map(|path| format!("`{}`", path.display()))
        .collect::<Vec<_>>()
        .join(", ");
    let where_caches_go = match crate::tool_cache_grant()
        .filter(|cache| policy.writable_paths().contains(cache))
    {
        Some(cache) => format!(
            "A tool that keeps a machine-wide cache belongs under `$CAIRN_TOOL_CACHE` \
             (`{}`), which this executor grants every cell; state that need not outlive the \
             batch belongs under `$TMPDIR`.",
            cache.display()
        ),
        None => "This process grants no tool cache (`$CAIRN_TOOL_CACHE` names none here), so a \
                 machine-wide cache has no writable home in this cell; state that need not \
                 outlive the batch belongs under `$TMPDIR`."
            .to_string(),
    };
    format!(
        "Cairn: this command ran in a confined cell, and `{refused}` is outside every directory \
         the cell may write. The cell's sandbox refuses that write whatever the file's own \
         permissions say, so changing its permissions or ownership will not help: the confined \
         cell may not write `{scope}`. It may write {writable}. {where_caches_go}",
        refused = refused.display(),
        scope = scope.display(),
    )
}

/// Absolute paths named on each denial-signature line and the lines above it,
/// the denial line's own first.
fn refused_paths_in_output(output: &str) -> Vec<PathBuf> {
    let lines: Vec<&str> = output.lines().collect();
    let mut paths = Vec::new();
    for (index, line) in lines.iter().enumerate() {
        if !crate::line_has_denial_signature(line) {
            continue;
        }
        let window = &lines[index.saturating_sub(PRECEDING_LINES)..=index];
        for candidate in window.iter().rev() {
            for path in absolute_paths_in(candidate) {
                if !paths.contains(&path) {
                    paths.push(path);
                }
            }
        }
    }
    paths
}

/// Words in `line` that are absolute paths, with the quoting and trailing
/// punctuation tools wrap them in removed.
fn absolute_paths_in(line: &str) -> impl Iterator<Item = PathBuf> + '_ {
    line.split(|c: char| {
        c.is_whitespace()
            || matches!(
                c,
                '\'' | '"' | '`' | '(' | ')' | '[' | ']' | '<' | '>' | ',' | ';' | '='
            )
    })
    .map(|word| word.trim_end_matches([':', '.']))
    .filter(|word| word.len() > 1 && word.starts_with('/') && !word.starts_with("//"))
    .map(PathBuf::from)
}

fn refused_by_the_fence(policy: &SandboxPolicy, path: &Path) -> bool {
    if PSEUDO_FILESYSTEMS.iter().any(|root| path.starts_with(root)) {
        return false;
    }
    // A denied read of an external credential store is the fence's prompt to
    // raise, not a write that needed a scope.
    if policy
        .deny_read
        .iter()
        .any(|denied| path.starts_with(denied))
    {
        return false;
    }
    let resolved = resolve_through_existing_ancestor(path);
    let inside_a_writable_scope = policy.writable_paths().iter().any(|root| {
        path.starts_with(root)
            || root
                .canonicalize()
                .is_ok_and(|root| resolved.starts_with(root))
    });
    let matches_a_writable_pattern = policy.writable_regex.iter().any(|pattern| {
        regex::Regex::new(pattern).is_ok_and(|pattern| {
            pattern.is_match(&path.to_string_lossy())
                || pattern.is_match(&resolved.to_string_lossy())
        })
    });
    !inside_a_writable_scope && !matches_a_writable_pattern && permissions_allow_write(path)
}

/// The deepest ancestor of `path` (or `path` itself) that exists.
fn nearest_existing(path: &Path) -> Option<&Path> {
    path.ancestors().find(|ancestor| ancestor.exists())
}

/// `path` with its deepest existing ancestor canonicalized, so a refused path
/// spelled through `/tmp` or `/var` on macOS compares against writable roots
/// that resolve to `/private/...`.
fn resolve_through_existing_ancestor(path: &Path) -> PathBuf {
    let Some(existing) = nearest_existing(path) else {
        return path.to_path_buf();
    };
    let Ok(canonical) = existing.canonicalize() else {
        return path.to_path_buf();
    };
    match path.strip_prefix(existing) {
        Ok(rest) if !rest.as_os_str().is_empty() => canonical.join(rest),
        _ => canonical,
    }
}

/// Whether this process, outside any sandbox, may write `path`, or create it in
/// its deepest existing ancestor, by the file system's own permissions.
#[cfg(unix)]
fn permissions_allow_write(path: &Path) -> bool {
    use std::os::unix::ffi::OsStrExt;
    let Some(existing) = nearest_existing(path) else {
        return false;
    };
    let Ok(existing) = std::ffi::CString::new(existing.as_os_str().as_bytes()) else {
        return false;
    };
    // SAFETY: `existing` is a NUL-terminated string that outlives the call.
    unsafe { libc::access(existing.as_ptr(), libc::W_OK) == 0 }
}

#[cfg(not(unix))]
fn permissions_allow_write(_path: &Path) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fenced policy whose only writable root is `worktree`, so a temp
    /// directory beside it is genuinely outside the cell.
    fn fenced(worktree: &Path) -> Confinement {
        Confinement::Fenced(SandboxPolicy {
            worktree: worktree.to_path_buf(),
            writable_extra: Vec::new(),
            deny_read: Vec::new(),
            writable_regex: Vec::new(),
            worktree_writable: true,
            violation_tag: crate::ViolationTag::fresh(),
        })
    }

    fn cargo_refusal(path: &Path) -> String {
        format!(
            "error: failed to open: {}\n\nCaused by:\n  Permission denied (os error 13)\n",
            path.display()
        )
    }

    #[test]
    fn the_cargo_install_specimen_names_the_refused_path_and_the_scope() {
        let worktree = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let crates = outside.path().join("tursodb").join(".crates.toml");

        let note = out_of_bounds_write_note(
            &fenced(worktree.path()),
            Some(101),
            &cargo_refusal(&crates),
            None,
        )
        .expect("a refused write outside the cell is explained");

        assert!(note.contains(&format!("`{}`", crates.display())), "{note}");
        assert!(
            note.contains(&format!(
                "may not write `{}`",
                outside.path().join("tursodb").display()
            )),
            "the note names the directory the write needed: {note}"
        );
        assert!(
            note.contains(&format!("`{}`", worktree.path().display())),
            "the note lists what the cell may write: {note}"
        );
    }

    #[test]
    fn a_refusal_on_the_same_line_as_its_path_is_read() {
        let worktree = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let target = outside.path().join("stamp");
        let output = format!(
            "touch: cannot touch '{}': Permission denied",
            target.display()
        );
        let note = out_of_bounds_write_note(&fenced(worktree.path()), Some(1), &output, None);
        assert!(note.is_some_and(|note| note.contains(&target.display().to_string())));
    }

    #[test]
    fn a_refused_path_inside_the_cell_is_not_the_fences_to_explain() {
        let worktree = tempfile::tempdir().unwrap();
        let inside = worktree.path().join("target").join(".lock");
        assert_eq!(
            out_of_bounds_write_note(
                &fenced(worktree.path()),
                Some(101),
                &cargo_refusal(&inside),
                None
            ),
            None
        );
    }

    #[test]
    fn only_a_failed_fenced_command_is_annotated() {
        let worktree = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let output = cargo_refusal(&outside.path().join("x"));
        assert_eq!(
            out_of_bounds_write_note(&fenced(worktree.path()), Some(0), &output, None),
            None,
            "a command that succeeded needs no explanation"
        );
        assert_eq!(
            out_of_bounds_write_note(&Confinement::Unconfined, Some(1), &output, None),
            None
        );
        assert_eq!(
            out_of_bounds_write_note(&Confinement::CredentialFloor, Some(1), &output, None),
            None
        );
    }

    #[cfg(unix)]
    #[test]
    fn a_file_mode_refusal_outside_the_cell_is_left_alone() {
        use std::os::unix::fs::PermissionsExt;
        // Root may write a directory whatever its mode, so it cannot observe the
        // distinction this test pins.
        if unsafe { libc::geteuid() } == 0 {
            return;
        }
        let worktree = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let locked = outside.path().join("locked");
        std::fs::create_dir(&locked).unwrap();
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o555)).unwrap();

        let note = out_of_bounds_write_note(
            &fenced(worktree.path()),
            Some(1),
            &cargo_refusal(&locked.join("file")),
            None,
        );
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert_eq!(
            note, None,
            "an unconfined process would have been refused too, so the fence is not the cause"
        );
    }

    #[test]
    fn a_denied_credential_store_and_procfs_are_not_write_scopes() {
        let worktree = tempfile::tempdir().unwrap();
        let store = tempfile::tempdir().unwrap();
        let confinement = Confinement::Fenced(SandboxPolicy {
            deny_read: vec![store.path().to_path_buf()],
            ..match fenced(worktree.path()) {
                Confinement::Fenced(policy) => policy,
                _ => unreachable!(),
            }
        });
        assert_eq!(
            out_of_bounds_write_note(
                &confinement,
                Some(1),
                &cargo_refusal(&store.path().join("credentials")),
                None
            ),
            None
        );
        let selinux = "sed: warning: failed to set default file creation context to \
                       /proc/self/attr/fscreate: Permission denied";
        assert_eq!(
            out_of_bounds_write_note(&confinement, Some(1), selinux, None),
            None
        );
    }

    #[test]
    fn a_recovered_path_is_preferred_to_one_read_from_output() {
        let worktree = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let logged = outside.path().join("logged");
        let printed = outside.path().join("printed");
        let note = out_of_bounds_write_note(
            &fenced(worktree.path()),
            Some(1),
            &cargo_refusal(&printed),
            Some(&logged),
        )
        .unwrap();
        assert!(note.contains(&logged.display().to_string()), "{note}");
        assert!(!note.contains(&printed.display().to_string()), "{note}");
    }

    #[test]
    fn paths_are_unwrapped_from_the_punctuation_tools_print_around_them() {
        let words: Vec<PathBuf> =
            absolute_paths_in("open `/a/b.toml`: (\"/c/d\"), '/e/f'. //network x=/g/h, /")
                .collect();
        assert_eq!(
            words,
            ["/a/b.toml", "/c/d", "/e/f", "/g/h"]
                .iter()
                .map(PathBuf::from)
                .collect::<Vec<_>>()
        );
    }
}
