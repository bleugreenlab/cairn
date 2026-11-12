//! The publish action and publication ledger (cairn/5962).
//!
//! One resource is both: appending `{action:"start"}` asks for a publication,
//! and reading it lists publications newest first. Each publication is a grid
//! of targets by legs, a row per (target, leg), and each leg has a log.
use super::specs::*;
use super::types::*;
use cairn_uri::ResourceKind;

const PUBLISH_ACTION: KeySpec = KeySpec::new("action", KeyType::Str, "start");
const PUBLISH_KIND: KeySpec = KeySpec::new("kind", KeyType::Str, "executor (default) or toolPin");
const PUBLISH_COMMIT: KeySpec = KeySpec::new(
    "commit",
    KeyType::Str,
    "the full sha to publish; default the base branch's tip",
);
const PUBLISH_FORCE: KeySpec = KeySpec::new(
    "force",
    KeyType::Array,
    "target triples to rebuild even if the live release would carry them",
);
const PUBLISH_TOOL: KeySpec =
    KeySpec::new("tool", KeyType::Str, "the pinned tool, for kind toolPin");

const PUBLICATION_RELATED: &[RelatedSpec] = &[RelatedSpec {
    label: "the publish action and every publication, newest first",
    kind: ResourceKind::ProjectPublish,
    actions: true,
}];

const LEG_LOG_RELATED: &[RelatedSpec] = &[RelatedSpec {
    label: "the publication: its targets by legs",
    kind: ResourceKind::ProjectPublication,
    actions: false,
}];

pub(crate) const PROJECT_PUBLISH_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ProjectPublish,
    uri_template: "cairn://p/{project}/publish",
    name: "Publish",
    description: "The project's publish action and publication ledger, newest first. Append {action:\"start\"} to publish: the plan runs, then each target's legs are placed on eligible builders, and a leg no builder can take is deferred with the reason, never queued. One pending publication per project: a start replaces one that has not begun and queues behind one that is running. Private to this install",
    read_projections: &[
        ProjectionSpec { key: "offset", values: "publications skipped in newest-first order (default 0)" },
        ProjectionSpec { key: "limit", values: "publications per page, 1-199 (default 25)" },
    ],
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[MutationSpec {
        mode: ChangeMode::Append,
        required: &[PUBLISH_ACTION],
        optional: &[PUBLISH_KIND, PUBLISH_COMMIT, PUBLISH_FORCE, PUBLISH_TOOL],
        label: "start a publication",
        example: "write({changes:[{target:\"cairn:@/publish\",mode:\"append\",payload:{action:\"start\"}}]})",
    }],
};

pub(crate) const PROJECT_PUBLICATION_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ProjectPublication,
    uri_template: "cairn://p/{project}/publish/{id}",
    name: "Publication",
    description: "One publication: its commit, protocol, trigger, mode and state, and a row per target and leg with builder, builder kind, state and reason, times, asset hashes, artifact location, signer, verifier and log",
    read_projections: NO_PROJECTIONS,
    related: PUBLICATION_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const PROJECT_PUBLICATION_LEG_LOG_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ProjectPublicationLegLog,
    uri_template: "cairn://p/{project}/publish/{id}/{target}/{leg}/log",
    name: "Publication leg log",
    description: "One leg's output, oldest first, as its builder reported it",
    read_projections: &[
        ProjectionSpec {
            key: "since",
            values: "only lines after this line number",
        },
        ProjectionSpec {
            key: "limit",
            values: "N lines",
        },
    ],
    related: LEG_LOG_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

#[cfg(test)]
mod tests {
    use super::super::{contract_for, mutation_spec};
    use super::*;

    #[test]
    fn only_the_ledger_is_writable_and_only_by_a_start() {
        for kind in [
            ResourceKind::ProjectPublication,
            ResourceKind::ProjectPublicationLegLog,
        ] {
            assert!(contract_for(kind).expect("registered").mutations.is_empty());
        }
        for mode in ChangeMode::ALL {
            assert_eq!(
                mutation_spec(ResourceKind::ProjectPublish, *mode).is_some(),
                matches!(mode, ChangeMode::Append),
                "{mode:?}"
            );
        }
        let start = mutation_spec(ResourceKind::ProjectPublish, ChangeMode::Append).unwrap();
        assert_eq!(
            start.required.iter().map(|k| k.key).collect::<Vec<_>>(),
            ["action"]
        );
        assert_eq!(
            start.optional.iter().map(|k| k.key).collect::<Vec<_>>(),
            ["kind", "commit", "force", "tool"]
        );
    }
}
