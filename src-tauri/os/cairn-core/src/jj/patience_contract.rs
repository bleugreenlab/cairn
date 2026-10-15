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
];

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
    for (name, source, signature) in REF_WRITERS {
        assert!(
            body_of(source, signature).contains("with_packed_refs_patience"),
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
                "{name} contains `{copied}`, which belongs to the stale-lock ceremony in \
                 jj/packed_refs.rs. The dev/ino revalidation in that ceremony decides \
                 whether removing a lock is safe; a second copy will drift on it."
            );
        }
    }
}
