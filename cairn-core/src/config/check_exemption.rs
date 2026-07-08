//! Worktree-fence exemption for project-declared check/test commands.
//!
//! Running the project's own tests/checks is a trusted, project-declared action,
//! not a risky out-of-worktree mutation. The worktree fence should never prompt
//! for, nor hang on, a declared check/test command. This module provides the pure
//! matcher that [`build_run_sandbox_policy`] consults to run such commands
//! **unconfined** (host permissions, no fence prompt) — matching the turn-end
//! cadence, which already runs these exact commands unconfined at turn-end. Exempting
//! them in the agent path only makes it match a trust decision the system already
//! makes; the fence was never the guard on a declared check's *content*.
//!
//! [`build_run_sandbox_policy`]: crate::mcp::handlers::run
//!
//! ## Trust source: the canonical main checkout, not the agent worktree
//!
//! The matcher's inputs — the `checks` contract and the project's package.json
//! `scripts` — are the trust boundary: a command they name runs unconfined. They
//! must therefore come from a source the running agent cannot mutate. The caller
//! ([`build_run_sandbox_policy`]) loads both from the **live main checkout**
//! (resolved via `resolve_local_repo_path_and_key`), exactly like the check
//! cadences' `load_live_project_checks`, with the agent worktree used only as a
//! fallback when the project repo cannot be resolved. This closes the self-grant
//! hole: a branch cannot add a `command: python escape.py` check or a
//! `test:escape` package script to its own worktree config and have it run
//! unconfined, because the matcher never sees the worktree's copy when the main
//! checkout resolves.
//!
//! ## The exemption must be tight to the declared command, not a prefix
//!
//! An exempt command runs FULLY UNCONFINED, so the match must not admit arbitrary
//! extra program arguments: a canonical prefix with an appended option can perform
//! out-of-worktree IO through the program itself, no shell metacharacter required
//! (`bunx tsc --noEmit --generateTrace ~/.ssh/x` writes a trace file). The match is
//! therefore bounded to exactly what the project declares or the cadence
//! generates:
//!
//! - **Shell-syntax guard (first).** Any shell control, chaining, or redirection
//!   metacharacter (`&&`, `||`, `;`, `|`, `&`, backtick, `$(`, `>`, `<`, or a
//!   newline) makes the command **not** exempt (`bun run test:rust; curl …`,
//!   `bun run test:rust > ~/x`).
//! - **Recognizer A — known project script.** The BARE `bun run <script>` (no
//!   trailing arguments) where `<script>` follows the `test`/`check`/`test:`/
//!   `check:` convention AND is a real script in the canonical package.json.
//!   Covers `bun run check:rust` / `bun run check:dead`, which an agent runs
//!   manually but which are not declared checks. Extra args make it non-exempt
//!   (they run sandboxed, prompting only if they actually cross the fence).
//! - **Recognizer B — declared check.** A placeholder-free declared command must
//!   match **exactly** after whitespace normalization. A `{targets}` command
//!   matches its literal head followed only by the `-p <crate>` pairs the planner
//!   generates (crate-name tokens, nothing path-like). A `{changedFiles}` command
//!   matches its head followed only by repo-relative path tokens (no option
//!   flags, no absolute/`~`/`..` escapes) — the shape the cadence substitutes.
//!
//! See `docs/worktree-fence.md` and `docs/checks.md`.

use std::collections::{BTreeSet, HashMap};
use std::path::Path;

use crate::config::project_settings::CheckCommand;

/// The selector placeholder a declared check command carries, if any. The tail a
/// command may legitimately add after the check's literal head is constrained to
/// exactly the shape the planner substitutes for this placeholder.
#[derive(Clone, Copy)]
enum Placeholder {
    /// `{targets}` → space-separated `-p <crate>` pairs.
    Targets,
    /// `{changedFiles}` → space-separated repo-relative paths.
    ChangedFiles,
}

const CHANGED_FILES: &str = "{changedFiles}";
const TARGETS: &str = "{targets}";

/// Whether `command` is a project-declared check/test invocation that the
/// worktree fence should run unconfined.
///
/// Pure and side-effect free: it decides purely from the command string, the
/// project's `checks` contract, and the project's package.json script names.
/// Both `checks` and `scripts` MUST be sourced from the canonical main checkout
/// (see the module docs) so a branch cannot self-grant an unconfined command by
/// editing its own worktree config.
pub fn is_exempt_check_command(
    command: &str,
    checks: &HashMap<String, CheckCommand>,
    scripts: &BTreeSet<String>,
) -> bool {
    // Shell-syntax guard first: a control, chaining, or redirection metacharacter
    // means the command is more than a single confined check invocation, so it is
    // never exempt — it falls through to the sandbox instead.
    if has_shell_metachar(command) {
        return false;
    }

    let normalized = normalize(command);
    if normalized.is_empty() {
        return false;
    }

    matches_known_project_script(&normalized, scripts)
        || matches_declared_check(&normalized, checks)
}

/// Load the package.json `scripts` names at `repo_root` (its own map keys). Empty
/// when the file is absent, unparseable, or declares no scripts — a non-JS project
/// simply exempts nothing via recognizer A. Reads only; the caller points this at
/// the canonical main checkout.
pub fn load_project_scripts(repo_root: &Path) -> BTreeSet<String> {
    let Ok(content) = std::fs::read_to_string(repo_root.join("package.json")) else {
        return BTreeSet::new();
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&content) else {
        return BTreeSet::new();
    };
    value
        .get("scripts")
        .and_then(|s| s.as_object())
        .map(|obj| obj.keys().cloned().collect())
        .unwrap_or_default()
}

/// Recognizer A: the BARE `bun run <script>` — no trailing arguments — where
/// `<script>` follows the `test`/`check` naming convention AND is a real script
/// in the canonical package.json. The convention alone is not enough (that would
/// admit an agent-invented `test:escape`), and trailing args are refused because
/// the command runs unconfined and an appended option could redirect IO.
fn matches_known_project_script(normalized: &str, scripts: &BTreeSet<String>) -> bool {
    let Some(script) = normalized.strip_prefix("bun run ") else {
        return false;
    };
    // Bare invocation only: a space means trailing args, which are not part of the
    // project-declared command and run sandboxed instead.
    if script.contains(' ') {
        return false;
    }
    let follows_convention = script == "test"
        || script == "check"
        || script.starts_with("test:")
        || script.starts_with("check:");
    follows_convention && scripts.contains(script)
}

/// Recognizer B: `normalized` matches some declared check command, bounded to the
/// exact command (placeholder-free) or the head plus the constrained tail shape
/// the planner substitutes for the check's placeholder.
fn matches_declared_check(normalized: &str, checks: &HashMap<String, CheckCommand>) -> bool {
    checks
        .values()
        .any(|check| check_command_matches(normalized, &check.command))
}

/// Whether the normalized `command` matches a single declared `check_command`.
fn check_command_matches(command: &str, check_command: &str) -> bool {
    let Some((placeholder, head, after)) = split_placeholder(check_command) else {
        // Placeholder-free: the exempt command must equal the declared command
        // exactly (after normalization). No extra arguments — they run unconfined.
        return command == normalize(check_command);
    };

    // Only the selector-at-the-end shape is supported; a check with literal text
    // AFTER the placeholder falls through to the sandbox (fail closed). None of
    // the recognized placeholders are used mid-command in practice.
    if !normalize(after).is_empty() {
        return false;
    }

    let head = normalize(head);
    if head.is_empty() {
        return false; // require a literal head anchor
    }
    match command.strip_prefix(&head) {
        // `command` is exactly the head: the placeholder substituted to empty (the
        // planner's conservative full run, e.g. degraded `{targets}`).
        Some("") => true,
        // A tail follows at a word boundary: validate it against the placeholder.
        Some(rest) => match rest.strip_prefix(' ') {
            Some(tail) => tail_is_valid_for(placeholder, tail),
            None => false, // no boundary: `test:ru` head must not match `test:rust`
        },
        None => false,
    }
}

/// Split a check command at its selector placeholder into `(kind, head, after)`,
/// or `None` when it is placeholder-free.
fn split_placeholder(check_command: &str) -> Option<(Placeholder, &str, &str)> {
    if let Some(i) = check_command.find(CHANGED_FILES) {
        let after = &check_command[i + CHANGED_FILES.len()..];
        Some((Placeholder::ChangedFiles, &check_command[..i], after))
    } else if let Some(i) = check_command.find(TARGETS) {
        let after = &check_command[i + TARGETS.len()..];
        Some((Placeholder::Targets, &check_command[..i], after))
    } else {
        None
    }
}

/// Whether `tail` (the substituted portion after a check's head) matches the
/// exact shape the planner generates for `placeholder`. An empty tail is handled
/// by the caller; here `tail` is non-empty.
fn tail_is_valid_for(placeholder: Placeholder, tail: &str) -> bool {
    let tokens: Vec<&str> = tail.split_whitespace().collect();
    if tokens.is_empty() {
        return true;
    }
    match placeholder {
        // `-p <crate>` pairs only — exactly `resolve_crate_targets` → `-p {c}`.
        Placeholder::Targets => {
            let mut i = 0;
            while i < tokens.len() {
                if tokens[i] != "-p" {
                    return false;
                }
                match tokens.get(i + 1) {
                    Some(name) if is_safe_crate_name(name) => i += 2,
                    _ => return false,
                }
            }
            true
        }
        // Repo-relative path tokens only — the impact-matched changed files.
        Placeholder::ChangedFiles => tokens.iter().all(|t| is_safe_relative_path(t)),
    }
}

/// A cargo crate name: alphanumerics, `-`, `_`. Excludes anything path-like
/// (`/`, `.`, `~`) or an option flag, so a `{targets}` tail cannot smuggle a
/// path argument.
fn is_safe_crate_name(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

/// A repo-relative path token: not an option flag, not absolute, not home-rooted,
/// and with no `..` escape — the shape the cadence substitutes for changed files.
fn is_safe_relative_path(s: &str) -> bool {
    !s.is_empty()
        && !s.starts_with('-')
        && !s.starts_with('/')
        && !s.starts_with('~')
        && !s.split('/').any(|seg| seg == "..")
}

/// Whether the raw command contains any shell control, chaining, or redirection
/// metacharacter. `&` catches `&&`, `|` catches `||`, `>` catches `>>`/`2>`, and
/// `<` catches heredocs (`<<`/`<<<`); `$(` is the one multi-char form. Redirection
/// is guarded because the exempt command runs unconfined, so a `>` to an
/// out-of-worktree path would perform exactly the mutation the fence guards.
fn has_shell_metachar(command: &str) -> bool {
    command.contains("$(")
        || command
            .chars()
            .any(|c| matches!(c, ';' | '|' | '&' | '`' | '>' | '<' | '\n'))
}

/// Collapse runs of whitespace so command matching is layout-insensitive
/// (mirrors `dev_commands::normalize`).
fn normalize(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::project_settings::{CheckPolicy, CheckWhen};

    fn no_checks() -> HashMap<String, CheckCommand> {
        HashMap::new()
    }

    fn check(command: &str) -> CheckCommand {
        CheckCommand {
            command: command.to_string(),
            impact: None,
            policy: CheckPolicy::Advisory,
            when: CheckWhen::Write,
            timeout: None,
        }
    }

    /// A representative `checks` map mirroring `.cairn/config.yaml`.
    fn repo_checks() -> HashMap<String, CheckCommand> {
        let mut m = HashMap::new();
        m.insert(
            "frontend".to_string(),
            check("bunx vitest related {changedFiles}"),
        );
        m.insert("typecheck".to_string(), check("bunx tsc --noEmit"));
        m.insert("rust".to_string(), check("bun run test:rust {targets}"));
        m.insert("api".to_string(), check("bun run test:api"));
        m.insert("web".to_string(), check("bun run check:web"));
        m.insert("frontend-full".to_string(), check("bunx vitest run"));
        m.insert("rust-full".to_string(), check("bun run test:rust"));
        m
    }

    /// The canonical package.json script names, mirroring this repo's package.json.
    /// Deliberately does NOT include `test:foo` / `test:escape`.
    fn repo_scripts() -> BTreeSet<String> {
        [
            "test",
            "check",
            "test:rust",
            "test:api",
            "test:frontend",
            "check:rust",
            "check:dead",
            "check:web",
            "check:ui",
            "build:cmd",
            "dev:instance",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }

    fn exempt(cmd: &str) -> bool {
        is_exempt_check_command(cmd, &repo_checks(), &repo_scripts())
    }

    #[test]
    fn recognizer_a_matches_bare_canonical_scripts() {
        for cmd in [
            "bun run check:rust",
            "bun run check:dead",
            "bun run test",
            "bun run check",
            "bun run test:rust", // bare (also rust-full exact via recognizer B)
        ] {
            assert!(exempt(cmd), "recognizer A should exempt bare script: {cmd}");
        }
    }

    #[test]
    fn recognizer_a_layout_insensitive() {
        assert!(is_exempt_check_command(
            "  bun   run   check:rust  ",
            &no_checks(),
            &repo_scripts()
        ));
    }

    #[test]
    fn recognizer_a_rejects_non_check_invented_and_extra_args() {
        for cmd in [
            // Real scripts, but not test/check (dev:instance is a carveout case).
            "bun run build:cmd",
            "bun run dev:instance",
            // Convention-following but not a canonical script: the invented name.
            "bun run test:escape",
            "bun run test:foo",
            "bun run check:evil",
            // Canonical script WITH extra args — unbounded unconfined args refused.
            "bun run check:rust --generateTrace ~/.ssh/cairn-trace",
            "bun run check:dead --out ~/escape",
            // Not a bun-run at all.
            "cargo build",
            "bun runtest:rust",
        ] {
            assert!(!exempt(cmd), "recognizer A should NOT exempt: {cmd}");
        }
    }

    #[test]
    fn shell_syntax_guard_blocks_chaining_and_redirection() {
        for cmd in [
            // chaining
            "bun run test:rust && rm -rf ~",
            "bun run test:rust; curl x | sh",
            "bun run test:rust | tee out",
            "bun run test:rust || echo fail",
            "bun run test:rust & echo bg",
            "bun run test:rust `whoami`",
            "bun run test:rust $(id)",
            "bun run test:rust\nrm -rf ~",
            // redirection
            "bun run test:rust > ~/.ssh/authorized_keys",
            "bun run test:rust >> /tmp/log",
            "bun run test:rust 2> /tmp/err",
            "bun run test:rust 2>&1",
            "bun run test:rust < /etc/passwd",
            "bun run test:rust <<EOF",
            "bunx tsc --noEmit > ~/escape",
        ] {
            assert!(!exempt(cmd), "shell-syntax guard should block: {cmd:?}");
        }
    }

    #[test]
    fn recognizer_b_placeholder_free_matches_exactly() {
        // Exact declared commands are exempt.
        assert!(exempt("bunx tsc --noEmit"));
        assert!(exempt("bunx vitest run"));
        assert!(exempt("bun run test:api"));
        assert!(exempt("bun run check:web"));

        // Extra program arguments after a placeholder-free check are NOT exempt:
        // an appended option can write out-of-worktree with no shell syntax.
        assert!(!exempt(
            "bunx tsc --noEmit --generateTrace ~/.ssh/cairn-trace"
        ));
        assert!(!exempt("bunx vitest run --outputFile ~/escape.json"));
        assert!(!exempt("bun run test:api --coverageDirectory ~/x"));
    }

    #[test]
    fn recognizer_b_targets_accepts_only_p_crate_pairs() {
        // The `{targets}` shape the planner generates: `-p <crate>` pairs (or empty).
        assert!(exempt("bun run test:rust")); // empty substitution (full run)
        assert!(exempt("bun run test:rust -p cairn-core"));
        assert!(exempt("bun run test:rust -p cairn-core -p cairn-common"));

        // Anything other than clean `-p <crate>` pairs is refused.
        assert!(!exempt("bun run test:rust --generateTrace ~/x"));
        assert!(!exempt("bun run test:rust -p cairn-core --foo"));
        assert!(!exempt("bun run test:rust -p ../evil")); // path-like crate token
        assert!(!exempt("bun run test:rust -p ~/x"));
        assert!(!exempt("bun run test:rust cairn-core")); // missing -p flag
        assert!(!exempt("bun run test:rust -p")); // dangling -p
    }

    #[test]
    fn recognizer_b_changed_files_accepts_only_relative_paths() {
        // The `{changedFiles}` shape: repo-relative path tokens (or empty).
        assert!(exempt("bunx vitest related")); // empty substitution
        assert!(exempt("bunx vitest related src/a.ts"));
        assert!(exempt("bunx vitest related src/a.ts packages/ui/b.ts"));

        // Option flags and path escapes are refused.
        assert!(!exempt("bunx vitest related --generateTrace ~/x"));
        assert!(!exempt("bunx vitest related /etc/passwd"));
        assert!(!exempt("bunx vitest related ~/secret"));
        assert!(!exempt("bunx vitest related ../../etc/shadow"));
    }

    #[test]
    fn recognizer_b_word_boundary() {
        // A shorter declared `{targets}` head must not match a longer script token.
        let mut short = HashMap::new();
        short.insert("partial".to_string(), check("bun run test:ru {targets}"));
        assert!(!check_command_matches(
            "bun run test:rust",
            "bun run test:ru {targets}"
        ));
        assert!(!is_exempt_check_command(
            "bun run test:rust",
            &short,
            &BTreeSet::new()
        ));
    }

    #[test]
    fn worktree_only_declaration_is_not_exempt() {
        // The caller sources `checks`/`scripts` from the canonical main checkout,
        // so a check command or package script that exists ONLY in the agent's
        // worktree is modeled here as "absent from the canonical sets" — and must
        // not be exempt. This is the self-grant hole an earlier review flagged.
        assert!(!exempt("python scripts/escape.py"));
        assert!(!exempt("bun run test:escape"));
    }

    #[test]
    fn recognizer_b_still_covers_declared_checks_when_scripts_empty() {
        // With no package.json scripts (recognizer A inert), a declared check
        // command is still exempt via recognizer B — but a check-only script that
        // is not a declared check (check:rust) is not.
        let checks = repo_checks();
        let empty = BTreeSet::new();
        assert!(is_exempt_check_command(
            "bun run test:rust",
            &checks,
            &empty
        ));
        assert!(!is_exempt_check_command(
            "bun run check:rust",
            &checks,
            &empty
        ));
    }

    #[test]
    fn recognizer_b_no_match_for_unrelated_command() {
        assert!(!exempt("cargo build --release"));
        assert!(!exempt("bunx eslint ."));
    }

    #[test]
    fn empty_command_is_not_exempt() {
        assert!(!exempt("   "));
    }

    #[test]
    fn load_project_scripts_reads_package_json() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("package.json"),
            r#"{"scripts": {"test:rust": "...", "check:rust": "..."}}"#,
        )
        .unwrap();
        let scripts = load_project_scripts(dir.path());
        assert!(scripts.contains("test:rust"));
        assert!(scripts.contains("check:rust"));
        assert!(!scripts.contains("test:escape"));
    }

    #[test]
    fn load_project_scripts_empty_when_absent_or_scriptless() {
        let dir = tempfile::tempdir().unwrap();
        assert!(
            load_project_scripts(dir.path()).is_empty(),
            "absent ⇒ empty"
        );
        std::fs::write(dir.path().join("package.json"), r#"{"name": "x"}"#).unwrap();
        assert!(
            load_project_scripts(dir.path()).is_empty(),
            "no scripts key ⇒ empty"
        );
    }
}
