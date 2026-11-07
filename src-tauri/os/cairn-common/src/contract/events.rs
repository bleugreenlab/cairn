//! The durable fact log: every minted route fact as one row, newest first.
//! Read-only; rows are minted by producers and settled by the fact log worker.
//! Beside it, the project's ingress endpoints: the minted publishers whose
//! events become `ingress` rows, and the deliveries those rows point at.

use super::specs::{NO_CROSS_ACTIONS, NO_MUTATIONS, NO_PROJECTIONS, NO_RELATED};
use super::types::*;
use cairn_uri::ResourceKind;

const LIST_KEYS: &[ProjectionSpec] = &[
    ProjectionSpec {
        key: "source",
        values: "a fact source (attention, file_changed, github_comment, ingress, post_created, post_commented, thread_stream)",
    },
    ProjectionSpec {
        key: "match",
        values: "field:value[,field:value...] (needs source; the same test a standing trigger applies, e.g. match=endpoint:bugs,type:bug.reported)",
    },
    ProjectionSpec {
        key: "since",
        values: "EPOCH",
    },
    ProjectionSpec {
        key: "from",
        values: "ID (lowest row id, inclusive; a coalesced wake cites from/to)",
    },
    ProjectionSpec {
        key: "to",
        values: "ID (highest row id, inclusive)",
    },
    ProjectionSpec {
        key: "limit",
        values: "N (default 50, max 200)",
    },
    ProjectionSpec {
        key: "offset",
        values: "N",
    },
];

const ROW_KEYS: &[ProjectionSpec] = &[ProjectionSpec {
    key: "explain",
    values: "a thread URI: whether this row would wake that thread, per standing trigger, and the scope or field that kept it out",
}];

const LIST_DESCRIPTION: &str = "Every minted route fact, newest first: what it pointed at, which threads it woke, and what its routes did";
const ROW_DESCRIPTION: &str =
    "One fact log row: its stored fields, the record it points at, its wakes, and its route firings";

const fn list(kind: ResourceKind, uri: &'static str, name: &'static str) -> ResourceContract {
    ResourceContract {
        kind,
        uri_template: uri,
        name,
        description: LIST_DESCRIPTION,
        read_projections: LIST_KEYS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: NO_MUTATIONS,
    }
}

const fn row(kind: ResourceKind, uri: &'static str, name: &'static str) -> ResourceContract {
    ResourceContract {
        kind,
        uri_template: uri,
        name,
        description: ROW_DESCRIPTION,
        read_projections: ROW_KEYS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: NO_MUTATIONS,
    }
}

pub(crate) const PROJECT_EVENTS_CONTRACT: ResourceContract = list(
    ResourceKind::ProjectEvents,
    "cairn://p/{project}/events",
    "Project events",
);
pub(crate) const PROJECT_EVENT_CONTRACT: ResourceContract = row(
    ResourceKind::ProjectEvent,
    "cairn://p/{project}/events/{id}",
    "Project event",
);
const ENDPOINT_NAME: KeySpec = KeySpec::new(
    "name",
    KeyType::Str,
    "1-63 lowercase letters, digits, or hyphens; unique among the project's live endpoints",
);
const ENDPOINT_PROVIDER: KeySpec = KeySpec::new(
    "provider",
    KeyType::Str,
    "first-party, cairn-hmac, bearer, or github. The api generates an external endpoint's secret and this machine's keychain keeps it; linear is minted by its integration",
);
const ENDPOINT_TOPIC: KeySpec = KeySpec::new(
    "topic",
    KeyType::Str,
    "first-party only, and required there: cairn.bugs (bug.reported from the public bug sink)",
);
const ENDPOINT_TYPE_FROM: KeySpec = KeySpec::new(
    "type_from",
    KeyType::Any,
    "external only: where each event's type comes from, as the api's type_from takes it (default: the provider's own)",
);
const ENDPOINT_MAX_BODY_BYTES: KeySpec = KeySpec::new(
    "max_body_bytes",
    KeyType::Int,
    "external only: the largest body the api accepts",
);
const ENDPOINT_RATE_PER_MIN: KeySpec = KeySpec::new(
    "rate_per_min",
    KeyType::Int,
    "external only: deliveries per minute before the api refuses",
);
const ENDPOINT_ROTATE: KeySpec = KeySpec::new(
    "rotate",
    KeyType::Bool,
    "true: replace the secret; the new one goes to this machine's keychain",
);

pub(crate) const PROJECT_EVENT_ENDPOINTS_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ProjectEventEndpoints,
    uri_template: "cairn://p/{project}/events/endpoints",
    name: "Project event endpoints",
    description: "The ingress endpoints this install minted for the project: their provider, URL, grant, health, and undelivered and rejected counts. Their events become `ingress` rows in the project's events. A secret never passes through here",
    read_projections: NO_PROJECTIONS,
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[MutationSpec {
        mode: ChangeMode::Append,
        required: &[ENDPOINT_NAME, ENDPOINT_PROVIDER],
        optional: &[
            ENDPOINT_TOPIC,
            ENDPOINT_TYPE_FROM,
            ENDPOINT_MAX_BODY_BYTES,
            ENDPOINT_RATE_PER_MIN,
        ],
        label: "mint endpoint",
        example: "write({changes:[{target:\"cairn://p/PROJECT/events/endpoints\",mode:\"append\",payload:{name:\"ci\",provider:\"cairn-hmac\"}}]})",
    }],
};
pub(crate) const PROJECT_EVENT_ENDPOINT_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ProjectEventEndpoint,
    uri_template: "cairn://p/{project}/events/endpoints/{name}",
    name: "Project event endpoint",
    description: "One ingress endpoint: its provider, URL, grant, where its secret is kept, and recent deliveries",
    read_projections: NO_PROJECTIONS,
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[
        MutationSpec {
            mode: ChangeMode::Patch,
            required: &[ENDPOINT_ROTATE],
            optional: &[],
            label: "rotate endpoint secret",
            example: "write({changes:[{target:\"cairn://p/PROJECT/events/endpoints/NAME\",mode:\"patch\",payload:{rotate:true}}]})",
        },
        MutationSpec {
            mode: ChangeMode::Delete,
            required: &[],
            optional: &[],
            label: "revoke endpoint",
            example: "write({changes:[{target:\"cairn://p/PROJECT/events/endpoints/NAME\",mode:\"delete\"}]})",
        },
    ],
};
pub(crate) const PROJECT_EVENT_DELIVERY_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ProjectEventDelivery,
    uri_template: "cairn://p/{project}/events/endpoints/{name}/deliveries/{id}",
    name: "Project event delivery",
    description: "One event an endpoint delivered, with its body fenced as untrusted content: the record an `ingress` row points at",
    read_projections: NO_PROJECTIONS,
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const WORKSPACE_EVENTS_CONTRACT: ResourceContract = list(
    ResourceKind::WorkspaceEvents,
    "cairn://routes/events",
    "Workspace events",
);
pub(crate) const WORKSPACE_EVENT_CONTRACT: ResourceContract = row(
    ResourceKind::WorkspaceEvent,
    "cairn://routes/events/{id}",
    "Workspace event",
);
