//! Tenets resource contracts.
use super::specs::*;
use super::types::*;

const TENET_STATEMENT: KeySpec = KeySpec::new(
    "statement",
    KeyType::Str,
    "one sentence of how the project does things, as the project's handbook would put it, for the operator to answer yes or no at a glance; no URIs and no reasoning; at most 200 characters",
);
const TENET_RATIONALE: KeySpec = KeySpec::new(
    "rationale",
    KeyType::Str,
    "what the statement rests on, shown collapsed beneath it: why, in the terms it was argued; the alternatives ruled out, so a yes records the rejection; and the canonical cairn://p/... URIs where it was argued; at most 1500 characters",
);
const TENET_SUPERSEDES: KeySpec = KeySpec::new(
    "supersedes",
    KeyType::Int,
    "number of an approved tenet this one replaces once approved",
);
const TENET_WITHDRAW: KeySpec = KeySpec::new(
    "withdraw",
    KeyType::Bool,
    "true; the proposing thread only, while the tenet awaits a verdict",
);
const TENET_VERDICT: KeySpec = KeySpec::new(
    "verdict",
    KeyType::Str,
    "\"yes\", \"no\", or \"your_call\" (approves, recording that the thread was trusted); operator only, given in the desktop app, refused to agents",
);
const TENET_COMMENT: KeySpec = KeySpec::new(
    "comment",
    KeyType::Str,
    "the operator's note beside the verdict",
);

pub(crate) const PROJECT_TENETS_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ProjectTenets,
    uri_template: "cairn://p/{project}/tenets",
    name: "Project tenets",
    description: "The statements of how a project does things that its threads put to the operator, and the yes/no verdicts given, newest first; a thread proposes, only the operator answers, and a tenet never changes once proposed",
    read_projections: &[
        ProjectionSpec {
            key: "status",
            values: "proposed|approved|declined|withdrawn|superseded, pending (awaiting a verdict, oldest first), or log (approved, declined, and superseded)",
        },
        ProjectionSpec { key: "thread", values: "a thread name: only the tenets that thread proposed" },
        ProjectionSpec { key: "slug", values: "a migrated ruling's slug" },
        ProjectionSpec { key: "search", values: "case-insensitive statement substring" },
        ProjectionSpec { key: "limit", values: "N (default 50, max 200)" },
        ProjectionSpec { key: "format", values: "json for the full projection" },
    ],
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[MutationSpec {
        mode: ChangeMode::Append,
        required: &[TENET_STATEMENT],
        optional: &[TENET_RATIONALE, TENET_SUPERSEDES],
        label: "propose a tenet (threads only)",
        example: "write({changes:[{target:\"cairn:@/tenets\",mode:\"append\",payload:{statement:\"Releases ship from main only.\",rationale:\"Why: ... Rejected: release branches, because ... Argued at cairn://p/PROJECT/N.\"}}]})",
    }],
};

pub(crate) const PROJECT_TENET_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ProjectTenet,
    uri_template: "cairn://p/{project}/tenets/{number}",
    name: "Tenet",
    description: "One tenet by number (a migrated ruling's slug is an alias): its statement and rationale, the verdict and who gave it, what it supersedes or was superseded by, and what it gates. Immutable: withdraw and propose again, or propose a successor with supersedes",
    read_projections: &[ProjectionSpec { key: "format", values: "json for the full projection" }],
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[
        MutationSpec {
            mode: ChangeMode::Patch,
            required: &[TENET_WITHDRAW],
            optional: &[],
            label: "withdraw an unanswered proposal (proposing thread only)",
            example: "write({changes:[{target:\"cairn:@/tenets/3\",mode:\"patch\",payload:{withdraw:true}}]})",
        },
        MutationSpec {
            mode: ChangeMode::Patch,
            required: &[TENET_VERDICT],
            optional: &[TENET_COMMENT],
            label: "answer a proposal (operator only, in the desktop app)",
            example: "write({changes:[{target:\"cairn://p/PROJECT/tenets/3\",mode:\"patch\",payload:{verdict:\"yes\",comment:\"...\"}}]})",
        },
    ],
};
