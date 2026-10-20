//! The parse gate, applied to commits in the store.
//!
//! [`crate::parse_gate`] says whether one text parses. This module says whether
//! a MERGE broke a file, which is a different and narrower question: a merged
//! path is refused only when it does not parse while every side that went into
//! it does. A file that was already broken on the branch is the branch's own
//! business and its checks report it; a file that was broken on the destination
//! is not this merge's doing. Only a merge that turned parseable inputs into an
//! unparseable result is positive evidence, and refusal is reserved for positive
//! evidence.
//!
//! Two places apply it. A replay applies it to the paths both sides changed,
//! inside the rebase and before the export, so an unparseable tree is rolled
//! back exactly as a recorded conflict is (see [`super::rebase_branch_onto`]).
//! A landing applies it to every path the fold changes, before the target
//! bookmark moves (see [`super::merge_into_bookmark`] and
//! [`landing_parse_refusal`]), because the default branch must never hold an
//! unparseable tree whatever produced it.

use super::*;
use std::collections::BTreeSet;
use std::path::Path;

pub use crate::parse_gate::ParseFailure;
use crate::parse_gate::{is_gated, parse_failure};

/// A replay refused because the tree it produced does not parse, though jj
/// recorded no conflict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationRefusal {
    /// The rebased commit that was refused before export. Never exported, and
    /// may be garbage-collected.
    pub commit: String,
    /// One entry per path, each at the parser's first error.
    pub failures: Vec<ParseFailure>,
}

/// The gated paths among `paths` whose content at `merged` does not parse while
/// every one of `sides` that holds the path does, and at least one holds it. A
/// path no side holds is new on the merged side: if it does not parse, the
/// merge did not do that.
///
/// Advisory on its own failures: a revision that cannot be read yields no
/// refusal for the paths it would have decided, and says so in the log. The
/// gate stops a tree only on positive evidence.
pub fn introduced_parse_failures(
    jj: &JjEnv,
    store: &Path,
    merged: &str,
    sides: &[&str],
    paths: &[String],
) -> Vec<ParseFailure> {
    let gated: Vec<String> = paths
        .iter()
        .filter(|path| is_gated(path))
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    if gated.is_empty() {
        return Vec::new();
    }
    // Each candidate failure, with whether some side has held it parseable.
    let mut failures: Vec<(ParseFailure, bool)> = texts_at(jj, store, merged, &gated)
        .into_iter()
        .filter_map(|(path, text)| parse_failure(&path, &text))
        .map(|failure| (failure, false))
        .collect();
    for side in sides {
        if failures.is_empty() {
            break;
        }
        let candidates: Vec<String> = failures
            .iter()
            .map(|(failure, _)| failure.path.clone())
            .collect();
        let Some(present) = presence(jj, store, side, &candidates) else {
            failures.clear();
            break;
        };
        failures.retain_mut(|(failure, held)| {
            match file_at_revision(jj, store, side, &failure.path, &present) {
                Ok(FileContent::Text(text)) => {
                    *held = true;
                    parse_failure(&failure.path, &text).is_none()
                }
                Ok(FileContent::Absent | FileContent::Binary) => true,
                Err(error) => {
                    log::warn!(
                        "parse gate: could not read `{}` at `{side}`, so the merge is not blamed \
                         for it: {error}",
                        failure.path
                    );
                    false
                }
            }
        });
    }
    failures
        .into_iter()
        .filter_map(|(failure, held)| held.then_some(failure))
        .collect()
}

/// The refusal for a fold of bookmark `source` into bookmark `target` whose
/// result would not parse, or `None` when every path `source` changes against
/// `target` parses (or was already unparseable on `target`).
pub fn landing_parse_refusal(
    jj: &JjEnv,
    store: &Path,
    target: &str,
    source: &str,
) -> Option<String> {
    let (target_rev, source_rev) = (bookmark_rev(target), bookmark_rev(source));
    let changed = listed_changes(jj, store, &target_rev, &source_rev)?;
    let failures = introduced_parse_failures(jj, store, &source_rev, &[&target_rev], &changed);
    (!failures.is_empty()).then(|| unparseable_landing_refusal(target, source, &failures))
}

/// The refusal for a pull request from bookmark `source` that broke a file it
/// changed: a path changed since the fork from `target` that parsed at the fork
/// and parses on `target`, and does not parse at the branch head.
///
/// Only what the branch changed is judged, so a branch that inherited a file
/// broken on an earlier `target`, since repaired there, is not refused for it.
/// This judges the branch head, not the tree a host-side merge will produce;
/// the two agree whenever the branch is current with `target`.
pub fn branch_parse_refusal(
    jj: &JjEnv,
    store: &Path,
    target: &str,
    source: &str,
) -> Option<String> {
    let (target_rev, source_rev) = (bookmark_rev(target), bookmark_rev(source));
    let fork = revset_commit(
        jj,
        store,
        &format!("heads(::({target_rev}) & ::({source_rev}))"),
    )?;
    let changed = listed_changes(jj, store, &fork, &source_rev)?;
    let failures =
        introduced_parse_failures(jj, store, &source_rev, &[&fork, &target_rev], &changed);
    (!failures.is_empty()).then(|| unparseable_landing_refusal(target, source, &failures))
}

fn bookmark_rev(name: &str) -> String {
    format!("bookmarks(exact:{name:?})")
}

fn listed_changes(jj: &JjEnv, store: &Path, from: &str, to: &str) -> Option<Vec<String>> {
    changed_paths(jj, store, from, to)
        .map_err(|error| {
            log::warn!("parse gate: could not list `{from}..{to}`, landing unchecked: {error}");
        })
        .ok()
}

/// What an agent is told when its landing was refused for an unparseable tree.
pub fn unparseable_landing_refusal(
    target: &str,
    source: &str,
    failures: &[ParseFailure],
) -> String {
    let (subject, verb) = if failures.len() == 1 {
        ("a file", "parses")
    } else {
        ("files", "parse")
    };
    format!(
        "Refusing to merge: `{source}` would land a tree on `{target}` in which {subject} that \
         {verb} on `{target}` no longer {verb}. `{target}` was not advanced.\n{}\nFix the \
         file(s) on `{source}` with ordinary edits (an unparseable file after a base advance is \
         usually a merged list closed twice or an entry outside its brackets), commit, then merge \
         again.",
        render_failures(failures)
    )
}

/// One line per failure, `path:line:column: message`.
pub fn render_failures(failures: &[ParseFailure]) -> String {
    failures
        .iter()
        .map(|failure| format!("- {failure}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn presence(jj: &JjEnv, store: &Path, rev: &str, paths: &[String]) -> Option<BTreeSet<String>> {
    paths_present_at(jj, store, rev, paths)
        .map_err(|error| {
            log::warn!("parse gate: could not list paths at `{rev}`, nothing decided: {error}");
        })
        .ok()
}

fn texts_at(jj: &JjEnv, store: &Path, rev: &str, paths: &[String]) -> Vec<(String, String)> {
    let Some(present) = presence(jj, store, rev, paths) else {
        return Vec::new();
    };
    paths
        .iter()
        .filter_map(
            |path| match file_at_revision(jj, store, rev, path, &present) {
                Ok(FileContent::Text(text)) => Some((path.clone(), text)),
                Ok(FileContent::Absent | FileContent::Binary) => None,
                Err(error) => {
                    log::warn!("parse gate: could not read `{path}` at `{rev}`: {error}");
                    None
                }
            },
        )
        .collect()
}

/// Every path whose content differs between two revisions, by its name at
/// `to`. Deleted paths are listed too and then simply read as absent.
fn changed_paths(jj: &JjEnv, store: &Path, from: &str, to: &str) -> Result<Vec<String>, String> {
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
        "jj diff --name-only (parse gate)",
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
