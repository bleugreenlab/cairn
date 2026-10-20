//! The contract this module exists to hold: one copy of the ceremony, and every
//! site that writes a ref into the shared repository routing through it.
//!
//! These are static guards over the call sites' own source, in the idiom of
//! `publish.rs`'s `ladder_contract`. The sites cannot see each other — a jj
//! export in the vcs layer and a `git update-ref` in the check dispatcher know
//! nothing of one another — yet they share one lock file, so the only place the
//! agreement between them can live is a test that reads all of them at once.
/// Every function that writes a ref into a project's shared Git repository,
/// named with the source file it lives in.
const REF_WRITERS: &[(&str, &str, &str)] = &[
    (
        "jj git import",
        include_str!("../../../cairn-vcs/src/jj/env.rs"),
        "fn import_git(",
    ),
    (
        "logical-head export",
        include_str!("../../../cairn-vcs/src/jj/export.rs"),
        "fn run_export(",
    ),
    (
        "sealed check refs",
        include_str!("../execution/checks.rs"),
        "fn git_check_ref_write(",
    ),
    (
        "stranded delta ref",
        include_str!("../mcp/handlers/run/mod.rs"),
        "fn strand_conflicted_delta(",
    ),
    (
        "logical-head commit write",
        include_str!("../../../cairn-vcs/src/jj/publish.rs"),
        "pub async fn publish_logical_head_exported(",
    ),
    (
        "ref-store maintenance retirement",
        include_str!("../../../cairn-vcs/src/jj/maintenance.rs"),
        "pub fn retire_refs(",
    ),
    (
        "ref-store maintenance keep refs",
        include_str!("../../../cairn-vcs/src/jj/maintenance.rs"),
        "fn recreate_keep_refs(",
    ),
    // Every jj rewrite writes a `refs/jj/keep/<commit>` ref per commit it
    // mints, so the store replay and flatten contend for the lock too.
    (
        "store replay rebase",
        include_str!("../../../cairn-vcs/src/jj/merge.rs"),
        "pub(crate) fn rebase_branch_onto_with_resolution(",
    ),
    (
        "resolution carry",
        include_str!("../../../cairn-vcs/src/jj/merge.rs"),
        "fn apply_resolution_carry(",
    ),
    (
        "flatten squash",
        include_str!("../../../cairn-vcs/src/jj/merge.rs"),
        "pub fn squash_branch_onto(",
    ),
    (
        "superseded-history hide",
        include_str!("../../../cairn-vcs/src/jj/merge.rs"),
        "pub(crate) fn hide_superseded_history(",
    ),
    (
        "store view sweep",
        include_str!("../../../cairn-vcs/src/jj/maintenance.rs"),
        "pub fn sweep_store_view(",
    ),
    (
        "replayed-twin bookmark settle",
        include_str!("../../../cairn-vcs/src/jj/maintenance.rs"),
        "fn resolve_twin_bookmark_conflicts(",
    ),
    (
        "flatten twin cleanup",
        include_str!("../../../cairn-vcs/src/jj/reconcile.rs"),
        "pub fn flatten_branch_recovery(",
    ),
    (
        "operation restore",
        include_str!("../../../cairn-vcs/src/jj/reconcile.rs"),
        "pub fn restore_operation(",
    ),
    (
        "held delta ref",
        include_str!("../mcp/handlers/run/held.rs"),
        "fn held_ref_transaction(",
    ),
];

/// The one sanctioned shortcut: a repeatable store rewrite routed through
/// `run_store_rewrite`, which is itself patience plus an operation-head guard.
const PATIENT_ROUTES: &[&str] = &["with_packed_refs_patience", "run_store_rewrite("];

/// The body of a free function, from its signature to the closing brace
/// rustfmt puts in column zero.
fn body_of<'a>(source: &'a str, signature: &str) -> &'a str {
    let start = source
        .find(signature)
        .unwrap_or_else(|| panic!("`{signature}` no longer exists"));
    let rest = &source[start..];
    let end = rest
        .find("\n}\n")
        .unwrap_or_else(|| panic!("`{signature}` is not a free function"));
    &rest[..end]
}

#[test]
fn every_ref_writing_site_routes_through_the_shared_wrapper() {
    assert!(
        body_of(
            include_str!("../../../cairn-vcs/src/jj/packed_refs.rs"),
            "pub(crate) fn run_store_rewrite("
        )
        .contains("with_packed_refs_patience("),
        "run_store_rewrite is only a sanctioned route while it runs through the shared wrapper"
    );
    for (name, source, signature) in REF_WRITERS {
        let body = body_of(source, signature);
        assert!(
            PATIENT_ROUTES.iter().any(|route| body.contains(route)),
            "{name} writes a ref without packed-refs patience. Every ref transaction in the \
             project repository contends for one packed-refs.lock, and an impatient one \
             rolls back work that already succeeded. Wrap it in \
             `with_packed_refs_patience`."
        );
    }
}

#[test]
fn no_site_keeps_a_private_copy_of_the_healing_ceremony() {
    for (name, source, _) in REF_WRITERS {
        for copied in [
            "lsof",
            "heal_stale",
            "stale_unheld_lock",
            "packed-ref file could not be obtained",
        ] {
            assert!(
                !source.contains(copied),
                "{name} contains `{copied}`, which belongs to the stale-lock ceremony: \
                 cairn_common::lock_proof proves a lock abandoned, and jj/packed_refs.rs \
                 heals through it. The dev/ino revalidation in that proof decides \
                 whether removing a lock is safe; a second copy will drift on it."
            );
        }
    }
}
