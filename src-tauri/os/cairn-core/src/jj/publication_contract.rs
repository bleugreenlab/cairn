/// Every site that advances a managed branch, with its source.
const PUBLICATION_SITES: &[(&str, &str)] = &[
    ("run", include_str!("../mcp/handlers/run/mod.rs")),
    (
        "write",
        include_str!("../mcp/handlers/write/file_mutations.rs"),
    ),
    ("memory triage", include_str!("../execution/actions.rs")),
];

/// The verbs that commit on behalf of an agent, and therefore owe an origin
/// push while a remote PR is open on the branch. Each carries every source
/// file its ladder is spread across. The memory-triage ledger is excluded
/// deliberately: it publishes onto a branch whose PR its own action opens
/// afterwards.
const AGENT_COMMIT_VERBS: &[(&str, &[&str])] = &[
    ("run", &[include_str!("../mcp/handlers/run/mod.rs")]),
    (
        "write",
        &[
            include_str!("../mcp/handlers/write/mod.rs"),
            include_str!("../mcp/handlers/write/file_mutations.rs"),
        ],
    ),
];

/// Paths that push a managed branch on their own initiative rather than at an
/// explicit publication barrier (opening or re-saving a pull request). Each must
/// ask the same publication rule the commit verbs ask, or it becomes a second
/// definition of when a branch belongs on origin.
///
/// There are none today. Base advance is lazy: the preparer and explicit
/// replays rewrite the store and publish nothing, leaving origin to the branch's
/// next commit verb or PR barrier, and `base_advance_publishes_nothing` holds
/// them to that. A path that starts publishing on its own belongs here.
const AUTOMATIC_PUBLISHERS: &[(&str, &str)] = &[];

/// The base-advance modules whose whole source is production code (their tests
/// live in sibling files), none of which may push.
const NON_PUBLISHING_ADVANCERS: &[(&str, &str)] = &[
    (
        "base-advance preparer",
        include_str!("../orchestrator/base_advance/preparation.rs"),
    ),
    (
        "explicit base replay",
        include_str!("../orchestrator/base_advance/replay_requests.rs"),
    ),
    (
        "base-advance retirement",
        include_str!("../orchestrator/base_advance/retirement.rs"),
    ),
];

const MANAGED_PUSH_TRIGGERS: &[(&str, &str)] = &[
    ("run", include_str!("../mcp/handlers/run/mod.rs")),
    ("write", include_str!("../mcp/handlers/write/mod.rs")),
    (
        "create-pr resave",
        include_str!("../pr_data/actions/create_pr.rs"),
    ),
    ("create-pr open", include_str!("../execution/actions.rs")),
    (
        "managed publication wrapper",
        include_str!("../orchestrator/base_advance.rs"),
    ),
];

#[test]
fn no_site_publishes_a_logical_head_without_exporting_it() {
    for (name, source) in PUBLICATION_SITES {
        assert!(
            source.contains("publish_logical_head_exported"),
            "{name} must publish through the sanctioned barrier"
        );
        for bare in [
            "cairn_vcs::publish_logical_head(",
            "cairn_vcs::publish_logical_mutations(",
        ] {
            assert!(
                !source.contains(bare),
                "{name} calls {bare} directly, which moves the jj bookmark and leaves the \
                 branch ref frozen. Publish through publish_logical_head_exported."
            );
        }
    }
}

#[test]
fn both_commit_verbs_climb_the_same_ladder() {
    for (name, sources) in AGENT_COMMIT_VERBS {
        for rung in [
            "publication_requirement_for_managed_branch",
            "publish_managed_branch",
        ] {
            assert!(
                sources.iter().any(|source| source.contains(rung)),
                "{name} never reaches `{rung}`. Both commit verbs advance the same branches, \
                 so a verb missing a rung silently withholds publication for every commit \
                 made through it."
            );
        }
    }
}

#[test]
fn automatic_publishers_consult_the_commit_verbs_rule() {
    for (name, source) in AUTOMATIC_PUBLISHERS {
        assert!(
            source.contains("publication_requirement_for_managed_branch"),
            "{name} publishes a managed branch without asking \
             `publication_requirement_for_managed_branch`, so it would push branches the \
             commit verbs deliberately keep local."
        );
    }
}

/// Lazy base advance never reaches origin itself. `base_advance.rs` holds the
/// managed publication wrapper, so its production code names the wrapper once,
/// at its definition; the modules beside it name no push at all. A base-advance
/// path that publishes must move into `AUTOMATIC_PUBLISHERS` and ask the rule.
#[test]
fn base_advance_publishes_nothing() {
    let wrapper_home = include_str!("../orchestrator/base_advance.rs")
        .split("\n#[cfg(test)]\nmod tests {")
        .next()
        .unwrap();
    assert_eq!(
        wrapper_home.matches("publish_managed_branch(").count(),
        1,
        "base_advance.rs calls `publish_managed_branch` on its own initiative; register it \
         in AUTOMATIC_PUBLISHERS and ask `publication_requirement_for_managed_branch`"
    );
    for (name, source) in NON_PUBLISHING_ADVANCERS {
        for push in [
            "publish_managed_branch(",
            "push_managed_branch",
            "publish_branch_to_origin(",
            "push_to_origin(",
        ] {
            assert!(
                !source.contains(push),
                "{name} reaches `{push}`; base advance publishes nothing on its own. Register \
                 it in AUTOMATIC_PUBLISHERS and ask `publication_requirement_for_managed_branch`"
            );
        }
    }
}

#[test]
fn every_managed_push_trigger_uses_the_recovery_wrapper() {
    for (name, source) in MANAGED_PUSH_TRIGGERS {
        assert!(
            source.contains("publish_managed_branch"),
            "{name} bypasses the canonical stale-publication recovery wrapper"
        );
        assert!(
            !source.contains("push_to_origin("),
            "{name} pushes a managed branch without stale or untracked-remote recovery"
        );
    }
}
