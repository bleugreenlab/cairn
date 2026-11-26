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
/// One today: a carry. Base advance is lazy about WHEN a branch moves, but a
/// branch it moves that an open remote pull request needs (its head, or the
/// base a child PR targets) is published in the same operation by
/// `base_advance::publish_carried_branch`, which `prepare_branch` calls once
/// its guards and reservation are released. Left local, that carry is the
/// commit a GitHub merge into the branch forks from, and the store and origin
/// diverge. The preparer modules themselves still name no push
/// (`base_advance_publishes_nothing`). A path that starts publishing on its
/// own belongs here.
const AUTOMATIC_PUBLISHERS: &[(&str, &str)] = &[(
    "base-advance carry publication",
    include_str!("../orchestrator/base_advance.rs"),
)];

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

/// Base advance reaches origin through one door only. `base_advance.rs` holds
/// the managed publication wrapper, so its production code names the wrapper
/// twice: at its definition, and in `publish_carried_branch`, which asks the
/// commit verbs' rule first. The modules beside it name no push at all. Any
/// other base-advance path that publishes must go through that function.
#[test]
fn base_advance_publishes_nothing() {
    let wrapper_home = include_str!("../orchestrator/base_advance.rs")
        .split("\n#[cfg(test)]\nmod tests {")
        .next()
        .unwrap();
    assert_eq!(
        wrapper_home.matches("publish_managed_branch(").count(),
        2,
        "base_advance.rs calls `publish_managed_branch` somewhere other than its definition \
         and `publish_carried_branch`; publish a carry through `publish_carried_branch`, which \
         asks `publication_requirement_for_managed_branch`"
    );
    let carry = wrapper_home
        .split("pub(crate) async fn publish_carried_branch(")
        .nth(1)
        .and_then(|rest| rest.split("\n}\n").next())
        .expect("base_advance.rs defines publish_carried_branch");
    let asks = carry
        .find("publication_requirement_for_managed_branch(")
        .expect("publish_carried_branch asks the publication rule");
    let publishes = carry
        .find("publish_managed_branch(")
        .expect("publish_carried_branch publishes through the managed wrapper");
    assert!(
        asks < publishes,
        "publish_carried_branch must ask the publication rule before it publishes"
    );
    for (name, source) in NON_PUBLISHING_ADVANCERS {
        for push in [
            "publish_managed_branch(",
            "push_managed_branch",
            "push_commit_expecting_unrecorded(",
            "publish_landing_atomic_unrecorded(",
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
