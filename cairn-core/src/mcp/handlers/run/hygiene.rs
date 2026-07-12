//! Working-directory hygiene: path normalization, `cd`-command advisory notes,
//! and branch-checkout tracked-change verification.

use crate::orchestrator::Orchestrator;

/// Extract the target path from a command that begins with `cd`.
///
/// Returns `None` when the command does not start with `cd ` or when no usable
/// path argument can be extracted (e.g., `cd` with no argument, or `cd -`).
/// Handles quoted paths (`cd "/path"`, `cd '/path'`) and paths followed by
/// `&&`, `;`, or other shell operators (`cd /path && git status`).
fn extract_cd_target(command: &str) -> Option<String> {
    let trimmed = command.trim();
    // Must start with `cd ` — not a longer command like `cdda` or `cdiff`.
    let rest = trimmed.strip_prefix("cd ")?;
    let rest = rest.trim_start();
    if rest.is_empty() || rest.starts_with('&') || rest.starts_with(';') {
        return None; // `cd` with no path argument
    }
    // Handle quoted paths: the cd target is inside the quotes.
    if let Some(inner) = rest.strip_prefix('"') {
        let end = inner.find('"')?;
        return Some(inner[..end].to_string());
    }
    if let Some(inner) = rest.strip_prefix('\'') {
        let end = inner.find('\'')?;
        return Some(inner[..end].to_string());
    }
    // Unquoted: the path is the first whitespace-delimited token. Strip a
    // trailing `;` that separates the cd from a subsequent command.
    let path = rest.split_whitespace().next()?;
    let path = path.trim_end_matches(';');
    if path == "-" || path.is_empty() {
        return None;
    }
    Some(path.to_string())
}

/// Expand a leading `~` to the home directory.
pub(super) fn expand_tilde(path: &str) -> String {
    if path == "~" {
        std::env::var("HOME").unwrap_or_else(|_| "~".to_string())
    } else if let Some(rest) = path.strip_prefix("~/") {
        let home = std::env::var("HOME").unwrap_or_default();
        format!("{home}/{rest}")
    } else {
        path.to_string()
    }
}

/// Lexically normalize a path: expand `~`, make absolute relative to `cwd`, and
/// resolve `.` and `..` components without touching the filesystem. This is a
/// pure string operation — no canonicalize, no symlink resolution — so it works
/// for paths that may not exist yet and stays consistent across comparisons.
fn normalize_path(path: &str, cwd: &str) -> String {
    let expanded = expand_tilde(path);
    let p = std::path::Path::new(&expanded);
    let absolute = if p.is_absolute() {
        p.to_path_buf()
    } else {
        std::path::Path::new(cwd).join(p)
    };
    let mut normalized = std::path::PathBuf::new();
    for component in absolute.components() {
        use std::path::Component;
        match component {
            Component::ParentDir => {
                if !normalized.as_os_str().is_empty() {
                    normalized.pop();
                }
            }
            Component::CurDir => {} // skip `.`
            other => normalized.push(other.as_os_str()),
        }
    }
    normalized.to_string_lossy().to_string()
}

/// Scan resolved command headers for `cd` commands and produce advisory notes.
///
/// A `cd` to the current working directory (the worktree) gets an informational
/// note: the worktree is already the cwd, so no navigation prefix is needed.
/// A `cd` to the project's primary checkout (the repo root) gets a warning:
/// commands should be run from the worktree, not the main checkout, because
/// changes made outside the worktree are not tracked or committed.
///
/// Notes are deduplicated: at most one informational and one warning note per
/// batch, regardless of how many commands repeat the same `cd`.
fn checkout_tracked_status(
    orch: &Orchestrator,
    checkout: &std::path::Path,
) -> Result<String, String> {
    if crate::jj::is_jj_dir(checkout) {
        let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
        return crate::jj::working_copy_dirty_paths(&jj, checkout)
            .map(|paths| paths.join("\n"))
            .map_err(|error| {
                format!(
                    "jj diff --summary failed for {}: {error}",
                    checkout.display()
                )
            });
    }
    let ctx = format!("git status for {}", checkout.display());
    let output = crate::jj::bounded_command_output(
        crate::env::git().arg("-C").arg(checkout).args([
            "status",
            "--porcelain",
            "--untracked-files=no",
        ]),
        crate::jj::JJ_DEFAULT_TIMEOUT,
        &ctx,
    )?;
    if !output.status.success() {
        return Err(format!(
            "git status failed for {}: {}",
            checkout.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

pub(super) fn checkout_has_tracked_changes(
    orch: &Orchestrator,
    checkout: &std::path::Path,
) -> Result<bool, String> {
    Ok(!checkout_tracked_status(orch, checkout)?.is_empty())
}

pub(super) fn check_cd_commands<'a, I>(headers: I, cwd: &str, repo_root: Option<&str>) -> String
where
    I: IntoIterator<Item = &'a str>,
{
    let normalized_cwd = normalize_path(cwd, cwd);
    let normalized_repo = repo_root.map(|r| normalize_path(r, cwd));

    let mut noted_cwd = false;
    let mut noted_repo = false;
    let mut notes: Vec<String> = Vec::new();
    for header in headers {
        let Some(target) = extract_cd_target(header) else {
            continue;
        };
        let normalized_target = normalize_path(&target, cwd);
        if normalized_target == normalized_cwd && !noted_cwd {
            noted_cwd = true;
            notes.push(format!(
                "\u{2139}\u{fe0f} Note: `cd {target}` \u{2014} your working directory is already \
                 `{cwd}`; you do not need to prefix commands with `cd`."
            ));
        } else if let Some(ref repo) = normalized_repo {
            if normalized_target == *repo && !noted_repo {
                noted_repo = true;
                notes.push(format!(
                    "\u{26a0}\u{fe0f} Warning: `cd {target}` navigates to the project's primary \
                     checkout, not your worktree. Commands should be run from the worktree \
                     (`{cwd}`), not the main repository checkout. Changes can only be made \
                     in a worktree."
                ));
            }
        }
    }
    notes.join("\n\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn extract_cd_target_simple_path() {
        assert_eq!(
            extract_cd_target("cd /work/myproject"),
            Some("/work/myproject".to_string())
        );
    }

    #[test]
    fn extract_cd_target_with_trailing_command() {
        assert_eq!(
            extract_cd_target("cd /work/myproject && git status"),
            Some("/work/myproject".to_string())
        );
    }

    #[test]
    fn extract_cd_target_double_quoted() {
        assert_eq!(
            extract_cd_target("cd \"/work/my project\" && git status"),
            Some("/work/my project".to_string())
        );
    }

    #[test]
    fn extract_cd_target_single_quoted() {
        assert_eq!(
            extract_cd_target("cd '/work/my project' && git status"),
            Some("/work/my project".to_string())
        );
    }

    #[test]
    fn extract_cd_target_tilde() {
        assert_eq!(
            extract_cd_target("cd ~/projects/cairn"),
            Some("~/projects/cairn".to_string())
        );
    }

    #[test]
    fn extract_cd_target_no_argument() {
        assert_eq!(extract_cd_target("cd"), None);
        assert_eq!(extract_cd_target("cd "), None);
    }

    #[test]
    fn extract_cd_target_dash() {
        assert_eq!(extract_cd_target("cd -"), None);
    }

    #[test]
    fn extract_cd_target_not_a_cd_command() {
        assert_eq!(extract_cd_target("git status"), None);
        assert_eq!(extract_cd_target("cdda /path"), None);
        assert_eq!(extract_cd_target("cdiff /path"), None);
    }

    #[test]
    fn extract_cd_target_semicolon_separator() {
        assert_eq!(
            extract_cd_target("cd /work/myproject; git status"),
            Some("/work/myproject".to_string())
        );
    }

    #[test]
    fn normalize_path_absolute() {
        assert_eq!(
            normalize_path("/work/myproject", "/some/cwd"),
            "/work/myproject"
        );
    }

    #[test]
    fn normalize_path_relative_joined_to_cwd() {
        assert_eq!(
            normalize_path("subdir", "/work/myproject"),
            "/work/myproject/subdir"
        );
    }

    #[test]
    fn normalize_path_dot_dot_resolved() {
        assert_eq!(normalize_path("..", "/work/myproject"), "/work");
    }

    #[test]
    fn normalize_path_dot_components_skipped() {
        assert_eq!(
            normalize_path("./subdir", "/work/myproject"),
            "/work/myproject/subdir"
        );
    }

    #[test]
    fn normalize_path_trailing_slash() {
        assert_eq!(
            normalize_path("/work/myproject/", "/some/cwd"),
            "/work/myproject"
        );
    }

    #[test]
    fn check_cd_commands_to_cwd_gets_informational_note() {
        let cwd = "/work/myproject";
        let notes = check_cd_commands(["cd /work/myproject && git status"], cwd, None);
        assert!(notes.contains('\u{2139}'));
        assert!(notes.contains("already"));
        assert!(notes.contains("do not need to prefix"));
    }

    #[test]
    fn check_cd_commands_to_repo_root_gets_warning() {
        let cwd = "/work/myproject-builder-0";
        let notes = check_cd_commands(["cd /repos/cairn && git status"], cwd, Some("/repos/cairn"));
        assert!(notes.contains('\u{26a0}'));
        assert!(notes.contains("primary checkout"));
        assert!(notes.contains("worktree"));
    }

    #[test]
    fn check_cd_commands_to_unrelated_path_no_note() {
        let notes = check_cd_commands(["cd /tmp/scratch"], "/work/myproject", Some("/repos/cairn"));
        assert!(notes.is_empty());
    }

    #[test]
    fn check_cd_commands_no_cd_command_no_note() {
        let notes = check_cd_commands(
            ["git status", "ls -la"],
            "/work/myproject",
            Some("/repos/cairn"),
        );
        assert!(notes.is_empty());
    }

    #[test]
    fn check_cd_commands_deduplicates_repeated_cd_to_cwd() {
        let cwd = "/work/myproject";
        let notes = check_cd_commands(
            ["cd /work/myproject", "cd /work/myproject && git status"],
            cwd,
            None,
        );
        // Only one informational note despite two cd commands to the same path.
        let info_count = notes.matches('\u{2139}').count();
        assert_eq!(info_count, 1);
    }

    #[test]
    fn check_cd_commands_both_cwd_and_repo_notes() {
        let cwd = "/work/myproject-builder-0";
        let notes = check_cd_commands(
            ["cd /work/myproject-builder-0", "cd /repos/cairn"],
            cwd,
            Some("/repos/cairn"),
        );
        assert!(notes.contains('\u{2139}'));
        assert!(notes.contains('\u{26a0}'));
    }

    #[test]
    fn check_cd_commands_cwd_equals_repo_root_only_informational() {
        // When the agent runs on the live checkout (cwd == repo_root), a cd to it
        // is redundant, not dangerous — only the informational note should appear.
        let cwd = "/repos/cairn";
        let notes = check_cd_commands(["cd /repos/cairn"], cwd, Some("/repos/cairn"));
        assert!(notes.contains('\u{2139}'));
        assert!(!notes.contains('\u{26a0}'));
    }

    #[test]
    fn check_cd_commands_dot_dot_to_repo_root_warns() {
        let notes = check_cd_commands(["cd .."], "/work/myproject", Some("/work"));
        // cd .. resolves to /work, which is the repo root — this should warn.
        assert!(notes.contains('\u{26a0}'));
    }
}
