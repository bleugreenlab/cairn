//! The landing binder: sequential identifiers numbered when a branch lands on
//! its project's default branch, and never before.
//!
//! A branch that adds a migration or changes the executor protocol takes a
//! value from a global sequence, and it cannot know which value is free until
//! it lands: any number it chooses earlier is a guess that another branch may
//! land first. So it authors the provisional spelling (`NEXT_name.sql`, `///
//! Version NEXT (cairn/N)`), and the landing, which is serialized by the store
//! lock, allocates against the tip it lands on and rewrites the branch in one
//! sealed commit: `Bind landing identifiers: migration 0284_x, executor-protocol
//! 158`. Two branches that both say NEXT land as consecutive values in landing
//! order. A branch that numbered by hand is bound the same way, so a number
//! another branch took first is rebound rather than refused.
//!
//! What gets bound is what the branch authored against the destination, not
//! only the literal word NEXT: every numbered file it added to a declared
//! directory, and the one version heading it wrote (see
//! [`ledger::plan_binding`]). The rules are code (`numbered-file`,
//! `version-heading`); `.cairn/config.yaml`'s `landing.identifiers`, read at
//! the destination, instantiates them.
//!
//! Nothing here renumbers at replay. A base advance carries `NEXT` through
//! unchanged, and a child merged into an integration branch carries it to the
//! integration branch's own landing.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use cairn_common::executor_protocol_ledger as ledger;

use crate::config::project_settings::IdentifierRule;

/// The subject every bind commit opens with.
pub(crate) const BIND_SUBJECT: &str = "Bind landing identifiers";

/// The provisional prefix of a numbered file the landing has not numbered.
const PROVISIONAL: &str = "NEXT";

/// Files larger than this are not searched for a stem to rewrite: a stem
/// reference lives in source and prose, not in a generated blob.
const REWRITE_SIZE_LIMIT: usize = 2 * 1024 * 1024;

/// One file mutation applied to a materialized tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FileEdit {
    /// Move a file, carrying its content untouched.
    Rename { from: String, to: String },
    /// Replace a file's whole content.
    Rewrite { path: String, content: String },
}

impl FileEdit {
    fn paths(&self) -> Vec<String> {
        match self {
            Self::Rename { from, to } => vec![from.clone(), to.clone()],
            Self::Rewrite { path, .. } => vec![path.clone()],
        }
    }
}

/// Every path `edits` write, deduplicated, for a path-scoped commit.
pub(crate) fn touched_paths(edits: &[FileEdit]) -> Vec<String> {
    edits
        .iter()
        .flat_map(FileEdit::paths)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// Apply `edits` on `tip` and seal them onto `bookmark` as one commit with
/// `message`. Renames are applied before rewrites, in order.
pub(crate) fn seal_edits(
    jj: &crate::jj::JjEnv,
    store: &Path,
    bookmark: &str,
    tip: &str,
    edits: &[FileEdit],
    message: &str,
) -> Result<(), String> {
    if edits.is_empty() {
        return Ok(());
    }
    crate::jj::with_transient_workspace(jj, store, tip, |workspace| {
        for edit in edits {
            match edit {
                FileEdit::Rename { from, to } => {
                    let target = workspace.join(to);
                    if let Some(parent) = target.parent() {
                        std::fs::create_dir_all(parent)
                            .map_err(|error| format!("create `{}`: {error}", parent.display()))?;
                    }
                    std::fs::rename(workspace.join(from), &target)
                        .map_err(|error| format!("rename `{from}` to `{to}`: {error}"))?;
                }
                FileEdit::Rewrite { path, content } => {
                    std::fs::write(workspace.join(path), content)
                        .map_err(|error| format!("rewrite `{path}`: {error}"))?;
                }
            }
        }
        let paths = touched_paths(edits);
        let path_refs = paths.iter().map(String::as_str).collect::<Vec<_>>();
        crate::jj::seal_paths(jj, workspace, message, None, &path_refs, Some(bookmark))?;
        Ok(())
    })
}

/// One value a landing allocated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Allocation {
    /// The `landing.identifiers` name that allocated it.
    pub kind: String,
    pub value: u64,
    /// What carries the value on the default branch: `0289_landing_allocations`,
    /// `Version 158`.
    pub name: String,
    /// What the branch authored: `NEXT_landing_allocations`, `NEXT`, or the
    /// number it chose by hand.
    pub provisional: String,
}

/// Everything one landing binds, and the commit that records it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct BindPlan {
    pub edits: Vec<FileEdit>,
    pub allocations: Vec<Allocation>,
    /// Lines for the commit body: each move, and each site that still spells a
    /// moved number as a bare literal.
    pub notes: Vec<String>,
}

impl BindPlan {
    /// `migration 0284_x, executor-protocol 158`; empty when nothing was
    /// allocated.
    pub(crate) fn summary(&self) -> String {
        self.allocations
            .iter()
            .map(
                |allocation| match allocation.name.strip_prefix("Version ") {
                    Some(version) => format!("{} {version}", allocation.kind),
                    None => format!("{} {}", allocation.kind, allocation.name),
                },
            )
            .collect::<Vec<_>>()
            .join(", ")
    }

    pub(crate) fn commit_message(&self) -> String {
        let mut message = format!("{BIND_SUBJECT}: {}", self.summary());
        if !self.notes.is_empty() {
            message.push_str("\n\n");
            message.push_str(&self.notes.join("\n"));
        }
        message
    }

    /// The planned content of `path`, if an earlier rule already rewrote it.
    fn planned_text(&self, path: &str) -> Option<&str> {
        self.edits.iter().rev().find_map(|edit| match edit {
            FileEdit::Rewrite {
                path: planned,
                content,
            } if planned == path => Some(content.as_str()),
            _ => None,
        })
    }

    /// Plan `content` for `path`, replacing an earlier rewrite of it.
    fn rewrite(&mut self, path: &str, content: String) {
        self.edits.retain(
            |edit| !matches!(edit, FileEdit::Rewrite { path: planned, .. } if planned == path),
        );
        self.edits.push(FileEdit::Rewrite {
            path: path.to_string(),
            content,
        });
    }
}

/// What a file name in a numbered directory says about its number.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Numbered {
    Bound(u64),
    Provisional,
}

/// A numbered file name split into its number, its stem, and the rest:
/// `0284_x.sql` is (Bound(284), `0284_x`, `x.sql`). `None` for a name that is
/// not `<width digits>_rest` or `NEXT[k]_rest`.
fn numbered(name: &str, width: usize) -> Option<(Numbered, &str, &str)> {
    let stem = name.rsplit_once('.').map_or(name, |(stem, _)| stem);
    if let Some(after) = name.strip_prefix(PROVISIONAL) {
        let rest = match after.as_bytes() {
            [b'_', ..] => &after[1..],
            [digit, b'_', ..] if digit.is_ascii_digit() => &after[2..],
            _ => return None,
        };
        return (!rest.is_empty()).then_some((Numbered::Provisional, stem, rest));
    }
    let digits = name.bytes().take_while(u8::is_ascii_digit).count();
    if digits != width || name.as_bytes().get(width) != Some(&b'_') {
        return None;
    }
    let rest = &name[width + 1..];
    if rest.is_empty() {
        return None;
    }
    let value = name[..width].parse().ok()?;
    Some((Numbered::Bound(value), stem, rest))
}

/// Replace each whole-token occurrence of a key of `substitutions` in `text`.
/// A token is bounded by anything but `[A-Za-z0-9_]`, so `NEXT_a` never
/// matches inside `NEXT_a_b`.
pub(crate) fn replace_tokens(text: &str, substitutions: &BTreeMap<String, String>) -> String {
    let word = |c: Option<char>| c.is_some_and(|c| c.is_ascii_alphanumeric() || c == '_');
    let mut sites: Vec<(usize, usize, &str)> = Vec::new();
    for (old, new) in substitutions {
        for (at, _) in text.match_indices(old.as_str()) {
            let before = text[..at].chars().next_back();
            let after = text[at + old.len()..].chars().next();
            if !word(before) && !word(after) {
                sites.push((at, at + old.len(), new.as_str()));
            }
        }
    }
    sites.sort_by_key(|(start, _, _)| *start);
    let mut out = String::with_capacity(text.len());
    let mut cursor = 0;
    for (start, end, new) in sites {
        if start < cursor {
            continue;
        }
        out.push_str(&text[cursor..start]);
        out.push_str(new);
        cursor = end;
    }
    out.push_str(&text[cursor..]);
    out
}

/// Bind every file the branch added to one numbered directory.
///
/// `tip_names` and `target_names` are the directory's file names on the branch
/// (already replayed onto the destination) and at the destination; the files
/// in the first and not the second are the branch's. They take the
/// destination's next free numbers in apply order: numbered files by number,
/// then provisional ones in lexical stem order, the same order
/// `cairn-db`'s derived tail applies them in. Each stem that changes is
/// renamed and rewritten wherever `changed` (every text file the branch
/// changed, at its tip) names it as a whole token. A moved number that a Rust
/// or TypeScript file still spells as a bare quoted literal is reported in the
/// commit body, never rewritten: only its author knows which migration it
/// meant.
pub(crate) fn plan_numbered_file(
    kind: &str,
    directory: &str,
    width: usize,
    tip_names: &BTreeSet<String>,
    target_names: &BTreeSet<String>,
    changed: &[(String, String)],
    plan: &mut BindPlan,
) -> Result<(), String> {
    let taken = target_names
        .iter()
        .filter_map(|name| match numbered(name, width)? {
            (Numbered::Bound(value), _, _) => Some(value),
            (Numbered::Provisional, _, _) => None,
        })
        .max()
        .unwrap_or(0);
    let mut added: Vec<(Numbered, &str, &str, &str)> = tip_names
        .difference(target_names)
        .filter_map(|name| {
            numbered(name, width).map(|(number, stem, rest)| (number, stem, rest, name.as_str()))
        })
        .collect();
    if added.is_empty() {
        return Ok(());
    }
    added.sort_by(|a, b| {
        let key = |(number, stem, _, _): &(Numbered, &str, &str, &str)| match number {
            Numbered::Bound(value) => (0u8, *value, String::new()),
            Numbered::Provisional => (1u8, 0, stem.to_string()),
        };
        key(a).cmp(&key(b))
    });

    let mut substitutions: BTreeMap<String, String> = BTreeMap::new();
    let mut renamed: BTreeMap<String, String> = BTreeMap::new();
    let mut rebound_numbers: Vec<(u64, String)> = Vec::new();
    let mut next = taken;
    for (number, stem, rest, name) in &added {
        next += 1;
        let bound_name = format!("{next:0width$}_{rest}");
        let bound_stem = bound_name
            .rsplit_once('.')
            .map_or(bound_name.as_str(), |(stem, _)| stem)
            .to_string();
        if next.to_string().len() > width {
            return Err(format!(
                "[{kind}] the next number, {next}, does not fit {width} digits in `{directory}`"
            ));
        }
        plan.allocations.push(Allocation {
            kind: kind.to_string(),
            value: next,
            name: bound_stem.clone(),
            provisional: stem.to_string(),
        });
        if *name == bound_name.as_str() {
            continue;
        }
        match number {
            Numbered::Provisional => {}
            Numbered::Bound(old) => {
                rebound_numbers.push((*old, bound_stem.clone()));
            }
        }
        plan.notes
            .push(format!("- [{kind}] `{stem}` -> `{bound_stem}`"));
        substitutions.insert(stem.to_string(), bound_stem);
        renamed.insert(
            format!("{directory}/{name}"),
            format!("{directory}/{bound_name}"),
        );
    }
    for (from, to) in &renamed {
        plan.edits.push(FileEdit::Rename {
            from: from.clone(),
            to: to.clone(),
        });
    }
    if substitutions.is_empty() {
        return Ok(());
    }

    for (path, text) in changed {
        let current = plan.planned_text(path).unwrap_or(text).to_string();
        let rewritten = replace_tokens(&current, &substitutions);
        if rewritten == current {
            continue;
        }
        let landed_path = renamed.get(path).cloned().unwrap_or_else(|| path.clone());
        plan.rewrite(&landed_path, rewritten);
    }

    for (old, bound_stem) in rebound_numbers {
        let digits = format!("{old:0width$}");
        let quoted = [format!("\"{digits}\""), format!("'{digits}'")];
        let sites: Vec<String> = changed
            .iter()
            .filter(|(path, _)| path.ends_with(".rs") || path.ends_with(".ts"))
            .flat_map(|(path, text)| {
                text.lines()
                    .enumerate()
                    .filter(|(_, line)| quoted.iter().any(|needle| line.contains(needle.as_str())))
                    .map(move |(index, _)| format!("{path}:{}", index + 1))
            })
            .collect();
        if !sites.is_empty() {
            plan.notes.push(format!(
                "  `{digits}` still appears as a bare literal at {}; a number reference does not \
                 follow a binding, so name the migration ({bound_stem}) instead",
                sites.join(", ")
            ));
        }
    }
    Ok(())
}

/// Bind the one version heading the branch authored above `anchor` in `file`.
/// `tip_text` is the file on the branch, already replayed onto the
/// destination; an earlier rule's rewrite of the file is planned on top of.
pub(crate) fn plan_version_heading(
    kind: &str,
    file: &str,
    anchor: &str,
    tip_text: &str,
    target_text: &str,
    plan: &mut BindPlan,
) -> Result<(), String> {
    let tip_text = plan.planned_text(file).unwrap_or(tip_text).to_string();
    let Some(binding) = ledger::plan_binding(&tip_text, target_text, anchor)
        .map_err(|error| format!("[{kind}] {error}"))?
    else {
        return Ok(());
    };
    let authored = match binding.authored {
        ledger::HeadingVersion::Next => ledger::PROVISIONAL.to_string(),
        ledger::HeadingVersion::Bound(value) => value.to_string(),
    };
    plan.allocations.push(Allocation {
        kind: kind.to_string(),
        value: binding.bound,
        name: format!("Version {}", binding.bound),
        provisional: format!("Version {authored}"),
    });
    if !binding.rewrites() {
        return Ok(());
    }
    let content = ledger::apply_binding(&tip_text, &binding, anchor)
        .map_err(|error| format!("[{kind}] {error}"))?;
    plan.notes.push(format!(
        "- [{kind}] `/// Version {authored}` -> `/// Version {}`",
        binding.bound
    ));
    plan.rewrite(file, content);
    Ok(())
}

/// The landed commit's invariant for one numbered directory, against the
/// destination it landed on: no provisional name, every number used once, and
/// the numbers new to the destination exactly its next ones, in a run.
pub(crate) fn numbered_invariant(
    kind: &str,
    directory: &str,
    width: usize,
    landed_names: &BTreeSet<String>,
    target_names: &BTreeSet<String>,
) -> Result<(), String> {
    let mut numbers: BTreeMap<u64, Vec<&str>> = BTreeMap::new();
    for name in landed_names {
        match numbered(name, width) {
            Some((Numbered::Provisional, _, _)) => {
                return Err(format!(
                    "[{kind}] `{directory}/{name}` would land unbound; only the landing numbers it"
                ))
            }
            Some((Numbered::Bound(value), _, _)) => numbers.entry(value).or_default().push(name),
            None => {}
        }
    }
    if let Some((value, names)) = numbers.iter().find(|(_, names)| names.len() > 1) {
        return Err(format!(
            "[{kind}] {value:0width$} would be taken by {} in `{directory}`",
            names.join(" and ")
        ));
    }
    let taken = target_names
        .iter()
        .filter_map(|name| match numbered(name, width)? {
            (Numbered::Bound(value), _, _) => Some(value),
            _ => None,
        })
        .max()
        .unwrap_or(0);
    let new: Vec<u64> = landed_names
        .difference(target_names)
        .filter_map(|name| match numbered(name, width)? {
            (Numbered::Bound(value), _, _) => Some(value),
            _ => None,
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let expected: Vec<u64> = (taken + 1..=taken + new.len() as u64).collect();
    if new != expected {
        return Err(format!(
            "[{kind}] the landing adds {new:?} to `{directory}`, which ends at {taken}; a landing \
             adds exactly the next numbers"
        ));
    }
    Ok(())
}

// ── The store side ───────────────────────────────────────────────────────────────

/// A branch bound against one destination tip.
#[derive(Debug, Clone)]
pub(crate) struct BoundLanding {
    /// The head that lands: the bind commit, or the (replayed) branch tip when
    /// nothing needed binding.
    pub head: String,
    pub plan: BindPlan,
}

/// The names directly inside `directory` at `rev`.
fn names_in(
    jj: &crate::jj::JjEnv,
    store: &Path,
    rev: &str,
    directory: &str,
) -> Result<BTreeSet<String>, String> {
    let prefix = format!("{directory}/");
    Ok(crate::jj::paths_under_at(jj, store, rev, directory)?
        .into_iter()
        .filter_map(|path| {
            path.strip_prefix(&prefix)
                .filter(|name| !name.contains('/'))
                .map(ToOwned::to_owned)
        })
        .collect())
}

/// Every path whose content differs between two revisions, by its name at `to`.
fn changed_paths(
    jj: &crate::jj::JjEnv,
    store: &Path,
    from: &str,
    to: &str,
) -> Result<Vec<String>, String> {
    jj.run(
        store,
        &[
            "diff",
            "--ignore-working-copy",
            "--name-only",
            "--from",
            from,
            "--to",
            to,
        ],
        "jj diff --name-only (landing bind)",
    )
    .map(|listed| {
        listed
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(ToOwned::to_owned)
            .collect()
    })
}

/// Whether the branch changed anything a declared identifier binds, since it
/// left `target`. A branch that did not lands exactly as it would without a
/// binder: it is not replayed or rewritten.
pub(crate) fn touches_identifiers(
    jj: &crate::jj::JjEnv,
    store: &Path,
    source: &str,
    target: &str,
    rules: &[(String, IdentifierRule)],
) -> Result<bool, String> {
    if rules.is_empty() {
        return Ok(false);
    }
    let changed = crate::jj::logical_changed_files(jj, store, target, source)?;
    Ok(changed
        .iter()
        .flat_map(|change| std::iter::once(&change.path).chain(change.previous_path.iter()))
        .any(|path| {
            rules.iter().any(|(_, rule)| match rule {
                IdentifierRule::NumberedFile { directory, .. } => {
                    path.starts_with(&format!("{directory}/"))
                }
                IdentifierRule::VersionHeading { file, .. } => path == file,
            })
        }))
}

/// Bind `branch`, which must already descend from `target`, against `target`,
/// and seal the bind commit onto it when there is anything to bind. The caller
/// holds the store lock.
pub(crate) fn bind_onto(
    jj: &crate::jj::JjEnv,
    store: &Path,
    branch: &str,
    target: &str,
    rules: &[(String, IdentifierRule)],
) -> Result<BoundLanding, String> {
    let tip = crate::jj::bookmark_commit(jj, store, branch)
        .ok_or_else(|| format!("bookmark `{branch}` did not resolve to one commit"))?;
    let mut plan = BindPlan::default();
    let mut changed: Option<Vec<(String, String)>> = None;
    for (kind, rule) in rules {
        match rule {
            IdentifierRule::NumberedFile { directory, width } => {
                let tip_names = names_in(jj, store, &tip, directory)?;
                let target_names = names_in(jj, store, target, directory)?;
                if tip_names.difference(&target_names).next().is_none() {
                    continue;
                }
                if changed.is_none() {
                    changed = Some(changed_texts(jj, store, target, &tip)?);
                }
                plan_numbered_file(
                    kind,
                    directory,
                    *width,
                    &tip_names,
                    &target_names,
                    changed.as_deref().unwrap_or_default(),
                    &mut plan,
                )?;
            }
            IdentifierRule::VersionHeading { file, anchor } => {
                let (Some(target_text), Some(tip_text)) = (
                    crate::jj::text_at_revision(jj, store, target, file)?,
                    crate::jj::text_at_revision(jj, store, &tip, file)?,
                ) else {
                    continue;
                };
                plan_version_heading(kind, file, anchor, &tip_text, &target_text, &mut plan)?;
            }
        }
    }
    if plan.edits.is_empty() {
        return Ok(BoundLanding { head: tip, plan });
    }
    seal_edits(jj, store, branch, &tip, &plan.edits, &plan.commit_message())?;
    let head = crate::jj::bookmark_commit(jj, store, branch)
        .ok_or_else(|| format!("the bind commit left `{branch}` unresolved"))?;
    if head == tip {
        return Err(format!("the bind commit did not move `{branch}`"));
    }
    Ok(BoundLanding { head, plan })
}

/// Every text file `head` changed against `target`, with its content at `head`.
fn changed_texts(
    jj: &crate::jj::JjEnv,
    store: &Path,
    target: &str,
    head: &str,
) -> Result<Vec<(String, String)>, String> {
    let paths = changed_paths(jj, store, target, head)?;
    let present = crate::jj::paths_present_at(jj, store, head, &paths)?;
    let mut texts = Vec::new();
    for path in paths {
        if let crate::jj::FileContent::Text(text) =
            crate::jj::file_at_revision(jj, store, head, &path, &present)?
        {
            if text.len() <= REWRITE_SIZE_LIMIT {
                texts.push((path, text));
            }
        }
    }
    Ok(texts)
}

/// Judge a bound head before it lands, off the store lock: every file the
/// landing changes still parses where it parsed on the destination, and every
/// declared identifier holds its invariant. Read-only over immutable commits,
/// so the lock is held only for the recheck that the tips have not moved.
pub(crate) fn validate_landing(
    jj: &crate::jj::JjEnv,
    store: &Path,
    head: &str,
    target: &str,
    target_branch: &str,
    source_branch: &str,
    rules: &[(String, IdentifierRule)],
) -> Result<(), String> {
    let changed = changed_paths(jj, store, target, head)?;
    let failures = crate::jj::introduced_parse_failures(jj, store, head, &[target], &changed);
    if !failures.is_empty() {
        return Err(crate::jj::unparseable_landing_refusal(
            target_branch,
            source_branch,
            &failures,
        ));
    }
    for (kind, rule) in rules {
        let violation = match rule {
            IdentifierRule::NumberedFile { directory, width } => numbered_invariant(
                kind,
                directory,
                *width,
                &names_in(jj, store, head, directory)?,
                &names_in(jj, store, target, directory)?,
            ),
            IdentifierRule::VersionHeading { file, anchor } => match (
                crate::jj::text_at_revision(jj, store, head, file)?,
                crate::jj::text_at_revision(jj, store, target, file)?,
            ) {
                (Some(landed), Some(destination)) => {
                    ledger::landed_invariant(&landed, &destination, anchor)
                        .map_err(|error| format!("[{kind}] {error}"))
                }
                _ => Ok(()),
            },
        };
        violation.map_err(|error| {
            format!(
                "Refusing to merge `{source_branch}` into `{target_branch}`: {error}. \
                 `{target_branch}` was not advanced."
            )
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(list: &[&str]) -> BTreeSet<String> {
        list.iter().map(|name| (*name).to_string()).collect()
    }

    const DIR: &str = "src-tauri/turso_migrations";

    fn bind(tip: &[&str], target: &[&str], changed: &[(&str, &str)]) -> Result<BindPlan, String> {
        let mut plan = BindPlan::default();
        let changed: Vec<(String, String)> = changed
            .iter()
            .map(|(path, text)| ((*path).to_string(), (*text).to_string()))
            .collect();
        plan_numbered_file(
            "migration",
            DIR,
            4,
            &names(tip),
            &names(target),
            &changed,
            &mut plan,
        )?;
        Ok(plan)
    }

    #[test]
    fn two_next_files_against_one_tip_bind_consecutively_in_lexical_order() {
        let plan = bind(
            &["0283_old.sql", "NEXT_b.sql", "NEXT1_a.sql"],
            &["0283_old.sql"],
            &[],
        )
        .unwrap();
        assert_eq!(
            plan.edits,
            vec![
                FileEdit::Rename {
                    from: format!("{DIR}/NEXT1_a.sql"),
                    to: format!("{DIR}/0284_a.sql"),
                },
                FileEdit::Rename {
                    from: format!("{DIR}/NEXT_b.sql"),
                    to: format!("{DIR}/0285_b.sql"),
                },
            ]
        );
        assert_eq!(plan.summary(), "migration 0284_a, migration 0285_b");
        assert_eq!(
            plan.commit_message().lines().next(),
            Some("Bind landing identifiers: migration 0284_a, migration 0285_b")
        );
    }

    #[test]
    fn back_to_back_landings_take_n_plus_one_then_n_plus_two() {
        // The first branch lands 0284_a; the second, replayed onto that tip,
        // binds its NEXT against it.
        let first = bind(&["0283_old.sql", "NEXT_a.sql"], &["0283_old.sql"], &[]).unwrap();
        assert_eq!(first.allocations[0].value, 284);
        let second = bind(
            &["0283_old.sql", "0284_a.sql", "NEXT_b.sql"],
            &["0283_old.sql", "0284_a.sql"],
            &[],
        )
        .unwrap();
        assert_eq!(second.allocations[0].value, 285);
        assert_eq!(second.allocations[0].name, "0285_b");
        assert_eq!(second.allocations[0].provisional, "NEXT_b");
    }

    #[test]
    fn a_hand_numbered_collision_is_rebound_and_its_literals_reported() {
        let plan = bind(
            &["0283_old.sql", "0284_theirs.sql", "0284_mine.sql"],
            &["0283_old.sql", "0284_theirs.sql"],
            &[(
                "src-tauri/os/cairn-db/src/x.rs",
                "let m = \"0284\"; // the version\n",
            )],
        )
        .unwrap();
        assert_eq!(
            plan.edits[0],
            FileEdit::Rename {
                from: format!("{DIR}/0284_mine.sql"),
                to: format!("{DIR}/0285_mine.sql"),
            }
        );
        assert!(
            plan.notes
                .iter()
                .any(|note| note.contains("src-tauri/os/cairn-db/src/x.rs:1")),
            "{:?}",
            plan.notes
        );
    }

    #[test]
    fn a_hand_number_that_is_already_next_is_kept_and_still_recorded() {
        let plan = bind(&["0283_old.sql", "0284_mine.sql"], &["0283_old.sql"], &[]).unwrap();
        assert!(plan.edits.is_empty());
        assert_eq!(plan.allocations.len(), 1);
        assert_eq!(plan.allocations[0].name, "0284_mine");
    }

    #[test]
    fn the_stem_token_is_rewritten_in_every_branch_changed_file() {
        let plan = bind(
            &["0283_old.sql", "NEXT_mine.sql"],
            &["0283_old.sql"],
            &[
                ("docs/notes.md", "see NEXT_mine.sql, not NEXT_mine_two\n"),
                (
                    "src-tauri/turso_migrations/NEXT_mine.sql",
                    "-- lineage: shared\n-- follows NEXT_mine\nSELECT 1;\n",
                ),
                ("src/untouched.rs", "fn nothing() {}\n"),
            ],
        )
        .unwrap();
        assert_eq!(
            plan.edits,
            vec![
                FileEdit::Rename {
                    from: format!("{DIR}/NEXT_mine.sql"),
                    to: format!("{DIR}/0284_mine.sql"),
                },
                FileEdit::Rewrite {
                    path: "docs/notes.md".into(),
                    content: "see 0284_mine.sql, not NEXT_mine_two\n".into(),
                },
                FileEdit::Rewrite {
                    path: format!("{DIR}/0284_mine.sql"),
                    content: "-- lineage: shared\n-- follows 0284_mine\nSELECT 1;\n".into(),
                },
            ]
        );
    }

    #[test]
    fn a_rust_reference_to_the_stem_is_rewritten_and_not_called_equivalent() {
        let plan = bind(
            &["0283_old.sql", "NEXT_mine.sql"],
            &["0283_old.sql"],
            &[("src/lib.rs", "include_str!(\"../NEXT_mine.sql\");\n")],
        )
        .unwrap();
        assert!(plan.edits.contains(&FileEdit::Rewrite {
            path: "src/lib.rs".into(),
            content: "include_str!(\"../0284_mine.sql\");\n".into(),
        }));
    }

    #[test]
    fn the_numbered_invariant_refuses_next_a_duplicate_and_a_gap() {
        let target = names(&["0283_old.sql"]);
        assert!(numbered_invariant(
            "m",
            DIR,
            4,
            &names(&["0283_old.sql", "0284_a.sql"]),
            &target
        )
        .is_ok());
        assert!(numbered_invariant(
            "m",
            DIR,
            4,
            &names(&["0283_old.sql", "NEXT_a.sql"]),
            &target
        )
        .unwrap_err()
        .contains("unbound"));
        assert!(numbered_invariant(
            "m",
            DIR,
            4,
            &names(&["0283_old.sql", "0283_a.sql"]),
            &target
        )
        .unwrap_err()
        .contains("would be taken by"));
        assert!(numbered_invariant(
            "m",
            DIR,
            4,
            &names(&["0283_old.sql", "0285_a.sql"]),
            &target
        )
        .unwrap_err()
        .contains("exactly the next numbers"));
    }

    const PROTOCOL: &str = "src-tauri/os/cairn-common/src/executor_protocol.rs";
    const ANCHOR: &str = "EXECUTOR_PROTOCOL_VERSION";
    const CONSTANT: &str =
        "pub const EXECUTOR_PROTOCOL_VERSION: u32 = derived::EXECUTOR_PROTOCOL_VERSION;\n";

    #[test]
    fn a_next_heading_binds_one_above_the_tip() {
        let target = format!("/// Version 157 adds main's.\n{CONSTANT}");
        let tip = format!(
            "/// Version 157 adds main's.\n///\n/// Version NEXT (cairn/5222) adds x.\n{CONSTANT}"
        );
        let mut plan = BindPlan::default();
        plan_version_heading(
            "executor-protocol",
            PROTOCOL,
            ANCHOR,
            &tip,
            &target,
            &mut plan,
        )
        .unwrap();
        assert_eq!(plan.summary(), "executor-protocol 158");
        let FileEdit::Rewrite { content, .. } = &plan.edits[0] else {
            panic!("a rewrite");
        };
        assert!(content.contains("/// Version 158 (cairn/5222) adds x.\n"));
        assert_eq!(ledger::landed_invariant(content, &target, ANCHOR), Ok(()));
    }

    #[test]
    fn two_next_headings_landing_back_to_back_take_consecutive_versions() {
        let main = format!("/// Version 157 adds main's.\n{CONSTANT}");
        let first =
            format!("/// Version 157 adds main's.\n///\n/// Version NEXT (cairn/1) a.\n{CONSTANT}");
        let mut plan = BindPlan::default();
        plan_version_heading("p", PROTOCOL, ANCHOR, &first, &main, &mut plan).unwrap();
        let FileEdit::Rewrite {
            content: landed, ..
        } = plan.edits[0].clone()
        else {
            panic!("a rewrite");
        };
        // The second branch, replayed onto the first's landing, carries both
        // paragraphs (the version-paragraphs merge keeps the landed one first).
        let second = landed.replace(
            CONSTANT,
            &format!("///\n/// Version NEXT (cairn/2) b.\n{CONSTANT}"),
        );
        let mut plan = BindPlan::default();
        plan_version_heading("p", PROTOCOL, ANCHOR, &second, &landed, &mut plan).unwrap();
        assert_eq!(plan.allocations[0].value, 159);
    }

    #[test]
    fn a_collided_hand_heading_is_rebound_and_moves_what_the_code_sees() {
        let target = format!("/// Version 157 adds main's.\n{CONSTANT}");
        let tip =
            format!("/// Version 157 adds main's.\n///\n/// Version 157 adds mine.\n{CONSTANT}");
        let mut plan = BindPlan::default();
        plan_version_heading("p", PROTOCOL, ANCHOR, &tip, &target, &mut plan).unwrap();
        assert_eq!(plan.allocations[0].value, 158);
        assert_eq!(plan.allocations[0].provisional, "Version 157");
    }

    #[test]
    fn whole_tokens_only() {
        let substitutions = BTreeMap::from([("NEXT_a".to_string(), "0284_a".to_string())]);
        assert_eq!(
            replace_tokens("NEXT_a NEXT_a_b xNEXT_a \"NEXT_a.sql\"", &substitutions),
            "0284_a NEXT_a_b xNEXT_a \"0284_a.sql\""
        );
    }
}
