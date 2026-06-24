//! Worktree-fence carveouts for user-accepted dev-launch commands.
//!
//! The worktree fence confines a `run` command's **writes** to the worktree (plus
//! temp/toolchain caches and session grants) and **hard-denies reads** of a
//! secret-store denylist (see `services::sandbox` and `docs/worktree-fence.md`).
//! That is the right default, but it blocks legitimate dev-launch workflows that
//! must cross the boundary every time — e.g. Cairn's own `bun run dev:instance`,
//! which writes a per-branch slot registry and a per-instance home outside the
//! worktree. Without a durable allowlist each launch parks on a fence prompt.
//!
//! A carveout rides on a project **terminal command** (the blessed command is
//! already declared there, so there is no second block to keep in sync), but it
//! is split into a *declaration* and an *acceptance* so a cloned repo cannot grant
//! itself a crossing:
//!
//! - **Declaration** — a `TerminalCommand` in repo-committed
//!   `[project]/.cairn/config.yaml` may carry an optional `write` scope list (the
//!   paths it needs). This is only a *request*; on its own it does nothing.
//! - **Acceptance** — the user approves a specific command as a fence-crosser,
//!   stored per project in user-owned `~/.cairn/settings.yaml`
//!   (`acceptedFenceCommands`, surfaced as a checkbox on the terminal shortcut).
//!   Only an accepted command's carveout takes effect.
//!
//! An accepted command with a `write` list is confined to those scopes (still
//! secret-store-gated, since the declaration is repo-committed); an accepted
//! command with **no** `write` list runs **unconfined** — the coarse "this command
//! may cross the fence" the user opted into, which also covers a launcher that
//! reads a secret store (e.g. fetching SSM env from `~/.aws`) without anyone
//! having to enumerate paths.
//!
//! A non-accepted command is untouched, and a project with no accepted commands
//! keeps the exact default fence. Ad-hoc unknown crossings still surface as
//! approvable permission requests; acceptance only pre-ordains the one command.

use std::path::{Path, PathBuf};

use crate::config::build_services::Templates;
use crate::models::TerminalCommand;

/// The carveouts that apply to a single command, resolved from a project's
/// terminal commands and the user's per-project acceptance list.
#[derive(Debug, Default, PartialEq)]
pub struct ResolvedCarveouts {
    /// Expanded writable glob strings to add to the spawn's writable set.
    pub write_globs: Vec<String>,
    /// An accepted command with no declared `write` scopes — run it unconfined.
    pub unconfined: bool,
    /// Declared scopes that could not be granted because they touch a secret
    /// store. Surfaced so the human sees the crossing fell back to the Ask flow.
    pub dropped_sensitive: Vec<String>,
}

impl ResolvedCarveouts {
    /// Whether any carveout took effect (so the caller can skip work when not).
    pub fn is_empty(&self) -> bool {
        self.write_globs.is_empty() && !self.unconfined
    }
}

/// Resolve the carveouts that apply to `command`.
///
/// A project `TerminalCommand` contributes only when (a) the running `command`
/// matches its declared `command`, and (b) that command is in `accepted` (the
/// user's per-project acceptance list). An accepted command with declared `write`
/// scopes is confined to them (never intersecting `deny_read`); an accepted
/// command with no scopes is `unconfined`. Pure and side-effect free so it is
/// unit-testable without an orchestrator.
pub fn resolve_carveouts(
    command: &str,
    project_terminals: &[TerminalCommand],
    accepted: &[String],
    deny_read: &[PathBuf],
    templates: &Templates,
) -> ResolvedCarveouts {
    let cmd = normalize(command);
    let accepted: Vec<String> = accepted.iter().map(|c| normalize(c)).collect();
    let mut out = ResolvedCarveouts::default();

    for tc in project_terminals {
        let declared = normalize(&tc.command);
        if declared.is_empty() || !cmd.contains(&declared) {
            continue;
        }
        if !accepted.contains(&declared) {
            continue; // declared a fence-crosser, but the user has not accepted it
        }
        if tc.write.is_empty() {
            // Coarse acceptance: the user trusts this exact command to cross the
            // fence, with no narrowed scopes — run it unconfined.
            out.unconfined = true;
            continue;
        }
        for g in &tc.write {
            let glob = expand_str(templates, g);
            if glob_intersects_denylist(&glob, deny_read) {
                out.dropped_sensitive.push(glob);
            } else {
                out.write_globs.push(glob);
            }
        }
    }

    out
}

/// Collapse runs of whitespace so command matching is layout-insensitive.
fn normalize(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Expand templates and a leading `~` in a config string, returning a string
/// (glob metacharacters are preserved).
fn expand_str(t: &Templates, s: &str) -> String {
    let expanded = t.expand(s);
    if let Some(rest) = expanded.strip_prefix("~/") {
        return t.home.join(rest).to_string_lossy().into_owned();
    }
    if expanded == "~" {
        return t.home.to_string_lossy().into_owned();
    }
    expanded
}

/// Whether a writable glob's scope intersects any denylisted secret store.
///
/// Compares the glob's literal prefix (up to the first `*`) against each deny
/// entry component-wise in both directions: drop the glob if its scope sits
/// inside a secret store (`~/.aws/cache/*`) **or** is broad enough to contain one
/// (`~`, `~/*`). This is what keeps a repo-committed `write` carveout from ever
/// widening access to a co-developer's credentials — even once accepted.
fn glob_intersects_denylist(glob: &str, deny: &[PathBuf]) -> bool {
    let cut = glob.find('*').unwrap_or(glob.len());
    let prefix = Path::new(&glob[..cut]);
    deny.iter()
        .any(|d| prefix.starts_with(d) || d.starts_with(prefix))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn templates() -> Templates {
        Templates {
            home: PathBuf::from("/home/u"),
            cairn_home: PathBuf::from("/home/u/.cairn"),
            worktrees: PathBuf::from("/home/u/.cairn/worktrees"),
            worktree: Some(PathBuf::from("/home/u/.cairn/worktrees/CAIRN-1")),
        }
    }

    fn deny() -> Vec<PathBuf> {
        vec![PathBuf::from("/home/u/.aws"), PathBuf::from("/home/u/.ssh")]
    }

    fn terminal(command: &str, write: &[&str]) -> TerminalCommand {
        TerminalCommand {
            name: "dev".to_string(),
            command: command.to_string(),
            write: write.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn declared_but_unaccepted_is_inert() {
        let project = [terminal(
            "bun run build:mcp && bun dev:instance",
            &["~/.cairn-dev-*"],
        )];
        // The repo declares a write scope, but with no acceptance it does nothing.
        let r = resolve_carveouts(
            "bun run build:mcp && bun dev:instance",
            &project,
            &[],
            &deny(),
            &templates(),
        );
        assert!(
            r.is_empty(),
            "unaccepted declaration must not widen the fence"
        );
    }

    #[test]
    fn accepted_with_scopes_grants_those_writes() {
        let cmd = "bun run build:mcp && bun dev:instance";
        let project = [terminal(cmd, &["~/.cairn-dev-*"])];
        let accepted = [cmd.to_string()];
        // Accepted: the declared scope applies; trailing args still match.
        for command in [cmd, "bun run build:mcp && bun dev:instance --seed empty"] {
            let r = resolve_carveouts(command, &project, &accepted, &deny(), &templates());
            assert_eq!(r.write_globs, vec!["/home/u/.cairn-dev-*".to_string()]);
            assert!(!r.unconfined);
        }
        // An unrelated command does not match even if accepted.
        let r = resolve_carveouts("cargo test", &project, &accepted, &deny(), &templates());
        assert!(r.is_empty());
    }

    #[test]
    fn accepted_without_scopes_is_unconfined() {
        let cmd = "./launch-dev";
        let project = [terminal(cmd, &[])];
        let r = resolve_carveouts(cmd, &project, &[cmd.to_string()], &deny(), &templates());
        assert!(r.unconfined, "a scopeless accepted command crosses fully");
        assert!(r.write_globs.is_empty());
    }

    #[test]
    fn accepted_scope_intersecting_denylist_is_dropped() {
        let cmd = "launch";
        // Even when accepted, a repo-declared scope touching a secret store is
        // dropped — acceptance does not let a repo exfiltrate credentials.
        let project = [terminal(cmd, &["~/.aws/cache/*", "~", "~/*"])];
        let r = resolve_carveouts(cmd, &project, &[cmd.to_string()], &deny(), &templates());
        assert!(r.write_globs.is_empty());
        assert_eq!(r.dropped_sensitive.len(), 3);
    }

    #[test]
    fn similar_named_sibling_is_not_treated_as_secret() {
        let cmd = "launch";
        let project = [terminal(cmd, &["~/.awsfoo/*"])];
        let r = resolve_carveouts(cmd, &project, &[cmd.to_string()], &deny(), &templates());
        assert_eq!(r.write_globs, vec!["/home/u/.awsfoo/*".to_string()]);
        assert!(r.dropped_sensitive.is_empty());
    }

    #[test]
    fn templates_expand_in_scopes() {
        let cmd = "go";
        let project = [terminal(cmd, &["{cairnHome}/state/*"])];
        let r = resolve_carveouts(cmd, &project, &[cmd.to_string()], &deny(), &templates());
        assert_eq!(r.write_globs, vec!["/home/u/.cairn/state/*".to_string()]);
    }
}
