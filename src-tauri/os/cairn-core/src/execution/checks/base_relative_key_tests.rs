use super::*;

const MIGRATION: &str = "src-tauri/turso_migrations/NEXT_x.sql";

fn check(yaml: &str) -> CheckCommand {
    serde_yaml::from_str(yaml).unwrap()
}

fn change(path: &str) -> GraphFileChange {
    GraphFileChange {
        path: path.to_string(),
        previous_path: None,
        status: "added".to_string(),
        additions: 1,
        deletions: 0,
    }
}

fn key(name: &str, check: &CheckCommand, basis: ChangeBasis<'_>) -> String {
    let selector = InputSelector::from_globs(check.impact.as_deref().unwrap_or(&[]));
    let entries = vec![(MIGRATION.to_string(), "blob".to_string())];
    check_result_key(
        check,
        &selector,
        Some(&entries),
        "tree",
        "macos-aarch64",
        "rustc=1;bun=1",
        CheckChange { name, basis },
    )
}

fn against(changed: &[GraphFileChange]) -> ChangeBasis<'_> {
    ChangeBasis::Against {
        changed,
        base_entries: None,
        base_findings: None,
    }
}

/// The cairn/5942 reuse: a red computed against a change that omitted the
/// `NEXT_` file was replayed for the same tree measured against the change
/// that does carry it. Same tree, different change: no shared key, so the
/// second evaluation cannot reuse the first one's verdict.
#[test]
fn the_same_tree_against_a_different_change_does_not_share_a_key() {
    let migrations = check(
        "command: bun run check:migrations\nbaseRelative: true\nimpact: [src-tauri/turso_migrations/**]\n",
    );
    let with_next = [change(MIGRATION)];
    let without = [change("src-tauri/turso_migrations/0337_x.sql")];
    assert_ne!(
        key("migrations", &migrations, against(&with_next)),
        key("migrations", &migrations, against(&without)),
    );
    assert_ne!(
        key("migrations", &migrations, against(&with_next)),
        key("migrations", &migrations, against(&[])),
        "an empty change is a change too"
    );
    assert_eq!(
        key("migrations", &migrations, against(&with_next)),
        key("migrations", &migrations, against(&with_next)),
        "the same change reuses"
    );
    // What the change touched outside the check's inputs is not something
    // the check depends on, so it neither moves the key nor costs a re-run.
    let elsewhere = [change(MIGRATION), change("src/lib.rs")];
    assert_eq!(
        key("migrations", &migrations, against(&with_next)),
        key("migrations", &migrations, against(&elsewhere)),
    );
}

/// Both sides of a rename are part of the change a base-relative key names.
#[test]
fn a_renames_source_is_part_of_the_change() {
    let migrations = check(
        "command: bun run check:migrations\nbaseRelative: true\nimpact: [src-tauri/turso_migrations/**]\n",
    );
    let added = [change(MIGRATION)];
    let mut moved = change(MIGRATION);
    moved.status = "renamed".to_string();
    moved.previous_path = Some("src-tauri/turso_migrations/0337_x.sql".to_string());
    assert_ne!(
        key("migrations", &migrations, against(&added)),
        key(
            "migrations",
            &migrations,
            against(std::slice::from_ref(&moved))
        ),
    );
    assert_eq!(
        check_transport_env(&[moved], None)[0].1,
        format!("src-tauri/turso_migrations/0337_x.sql\n{MIGRATION}"),
        "the transport names the destination and the source"
    );
}

/// A check that does not declare `baseRelative` keeps the key it had, byte
/// for byte, whatever change it is handed.
#[test]
fn a_check_read_off_its_tree_ignores_the_change() {
    let fmt = check("command: cargo fmt --check\nimpact: [src-tauri/turso_migrations/**]\n");
    let with_next = [change(MIGRATION)];
    assert_eq!(
        key("fmt", &fmt, against(&with_next)),
        key("fmt", &fmt, against(&[])),
    );
    assert_eq!(
        key("fmt", &fmt, against(&with_next)),
        key("fmt", &fmt, ChangeBasis::Unknown),
    );
}

/// A base-relative check with no change to name gets a key nothing else
/// produces: it neither reuses a verdict nor leaves one behind.
#[test]
fn an_unknown_change_shares_no_key() {
    let migrations = check(
        "command: bun run check:migrations\nbaseRelative: true\nimpact: [src-tauri/turso_migrations/**]\n",
    );
    assert_ne!(
        key("migrations", &migrations, ChangeBasis::Unknown),
        key("migrations", &migrations, ChangeBasis::Unknown),
    );
}

/// The executor-protocol guard is shipped its base's content, so its key
/// names the base blob each bundled path came from: the same tree and
/// change compared against a different base protocol source is a
/// different question. Without the base's listing it has no key to share.
#[test]
fn a_base_content_check_names_its_base_blobs() {
    let protocol = check("command: bun run check:executor-protocol\nimpact: [src-tauri/**]\n");
    let changed = [change(EXECUTOR_PROTOCOL_SOURCE)];
    let base = |blob: &str| vec![(EXECUTOR_PROTOCOL_SOURCE.to_string(), Some(blob.to_string()))];
    let (v40, v41) = (base("v40"), base("v41"));
    let key_against = |entries: Option<&[(String, Option<String>)]>| {
        key(
            EXECUTOR_PROTOCOL_CHECK,
            &protocol,
            ChangeBasis::Against {
                changed: &changed,
                base_entries: entries,
                base_findings: None,
            },
        )
    };
    assert!(is_base_relative(EXECUTOR_PROTOCOL_CHECK, &protocol));
    assert_eq!(key_against(Some(&v40)), key_against(Some(&v40)));
    assert_ne!(key_against(Some(&v40)), key_against(Some(&v41)));
    assert_ne!(key_against(None), key_against(None));
}

/// cairn/6374: lint is not base-relative, but its command subtracts the
/// base finding set it is shipped, so its verdict at one tree differs by
/// base. Its key names that set: two bases with different recorded sets,
/// and a base whose set is unrecorded against one that recorded none, are
/// different questions. A run shipped no set keys as the base's own
/// whole-tree row does, which is the row the baseline lookup names.
#[test]
fn a_base_subtracting_key_names_the_finding_set_it_is_shipped() {
    use crate::execution::lint_baseline::LintBaselineTransport;
    let lint = check("command: bun run check:lint\nimpact: [src/**]\n");
    let changed = [change("src/Queue.tsx")];
    let shipped = |fingerprints: Option<&[&str]>| LintBaselineTransport {
        base: "main".to_string(),
        commit: "base-a".to_string(),
        tree: "tree-a".to_string(),
        input_hash: "input-a".to_string(),
        fingerprints: fingerprints.map(|set| {
            set.iter()
                .map(|fingerprint| fingerprint.to_string())
                .collect()
        }),
        missing: fingerprints
            .is_none()
            .then(|| "no recorded lint result".to_string()),
    };
    let key_shipping = |changed: &[GraphFileChange], set: Option<&LintBaselineTransport>| {
        key(
            "lint",
            &lint,
            ChangeBasis::Against {
                changed,
                base_entries: None,
                base_findings: set,
            },
        )
    };
    let base_a = shipped(Some(&["F"]));
    let base_b = shipped(Some(&["G"]));
    let clean = shipped(Some(&[]));
    let unrecorded = shipped(None);

    assert!(!is_base_relative("lint", &lint));
    assert_ne!(
        key_shipping(&changed, Some(&base_a)),
        key_shipping(&changed, Some(&base_b)),
        "F is the base's own error against A and the branch's against B"
    );
    assert_ne!(
        key_shipping(&changed, Some(&unrecorded)),
        key_shipping(&changed, Some(&clean)),
        "an unrecorded base is no verdict; a clean one judges every finding"
    );
    for set in [&base_a, &clean, &unrecorded] {
        assert_ne!(
            key_shipping(&changed, Some(set)),
            key_shipping(&changed, None),
            "a shipped set is never the whole-tree question"
        );
    }
    assert_eq!(
        key_shipping(&changed, None),
        key("lint", &lint, ChangeBasis::Unknown),
        "shipping no set keys as the base's own row, which the lookup names"
    );

    // The set decides the verdict; where it was read from only labels the
    // report, and lint does not read the change.
    let relabeled = LintBaselineTransport {
        base: "agent/parent".to_string(),
        commit: "base-c".to_string(),
        tree: "tree-c".to_string(),
        input_hash: "input-c".to_string(),
        ..shipped(Some(&["F"]))
    };
    assert_eq!(
        key_shipping(&changed, Some(&base_a)),
        key_shipping(&[change("src/Other.tsx")], Some(&relabeled)),
    );
    assert_eq!(
        key_shipping(&changed, Some(&shipped(Some(&["F", "G"])))),
        key_shipping(&changed, Some(&shipped(Some(&["G", "F"])))),
        "a set, in any order"
    );
    assert_ne!(
        key_shipping(&changed, Some(&shipped(Some(&["F"])))),
        key_shipping(&changed, Some(&shipped(Some(&["F", "F"])))),
        "identical findings in one file are counted, so the multiset is the set"
    );

    // A check that subtracts nothing never reads the set.
    let fmt = check("command: cargo fmt --check\nimpact: [src/**]\n");
    assert_eq!(
        key(
            "fmt",
            &fmt,
            ChangeBasis::Against {
                changed: &changed,
                base_entries: None,
                base_findings: Some(&base_a),
            },
        ),
        key("fmt", &fmt, against(&changed)),
    );
}

/// Every project check that reads the changed-file set as a statement about
/// the change is base-relative: check:migrations judges what the change
/// added, and the scoped findings wrappers (check-rust.ts, check-dead.ts)
/// gate only on findings in changed files, and check-fmt.ts formats only the
/// changed Rust files. executor-protocol is by the base content it is shipped.
#[test]
fn the_projects_change_reading_checks_are_base_relative() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../.cairn/config.yaml");
    let config: serde_yaml::Value =
        serde_yaml::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    let declared = |name: &str| -> CheckCommand {
        serde_yaml::from_value(config["checks"][name].clone())
            .unwrap_or_else(|error| panic!("{name}: {error}"))
    };
    for name in ["migrations", "rust-lint", "dead-code", "rust-fmt"] {
        assert!(declared(name).base_relative, "{name} declares baseRelative");
    }
    assert!(is_base_relative(
        EXECUTOR_PROTOCOL_CHECK,
        &declared(EXECUTOR_PROTOCOL_CHECK)
    ));
    assert!(!is_base_relative("lockfile", &declared("lockfile")));
}

/// The narrowing baseline's delta is diffed by path, so a rename since the
/// baseline reads as its source deleted and its destination added.
#[test]
fn the_baseline_delta_reads_a_rename_as_both_paths() {
    let selector = InputSelector::from_globs(&["src-tauri/turso_migrations/**".to_string()]);
    let baseline = vec![(
        "src-tauri/turso_migrations/0337_x.sql".to_string(),
        "blob".to_string(),
    )];
    let current = vec![(MIGRATION.to_string(), "blob".to_string())];
    let delta: Vec<(String, String)> =
        diff_tree_entries_for_selector(&baseline, &current, &selector)
            .into_iter()
            .map(|change| (change.path, change.status))
            .collect();
    assert_eq!(
        delta,
        vec![
            (
                "src-tauri/turso_migrations/0337_x.sql".to_string(),
                "deleted".to_string()
            ),
            (MIGRATION.to_string(), "added".to_string()),
        ]
    );
}
