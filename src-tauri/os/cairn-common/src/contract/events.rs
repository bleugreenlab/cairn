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
        values: "a fact source (attention, channel_message, file_changed, github_comment, ingress, post_created, post_commented, …); a retired source still lists its historical rows but takes no match",
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
    "One fact log row: its stored fields, the record it points at, its wakes, its route firings, and its decision-route feedback";

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

const FEEDBACK_EVENT: KeySpec = KeySpec::new(
    "event",
    KeyType::Any,
    "the event URI the delivered message cites (cairn://p/{project}/events/{id}), or its id",
);
const FEEDBACK_VERDICT: KeySpec = KeySpec::new("verdict", KeyType::Str, "not_mine | reroute");
const FEEDBACK_TO: KeySpec = KeySpec::new(
    "to",
    KeyType::Str,
    "the chartered thread URI that owns it; required for reroute, refused for not_mine",
);
const FEEDBACK_NOTE: KeySpec = KeySpec::new("note", KeyType::Str, "");
pub(crate) const PROJECT_EVENT_FEEDBACK_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ProjectEventFeedback,
    uri_template: "cairn://p/{project}/events/feedback",
    name: "Decision-route feedback",
    description: "The project's decision-route label set: one row per routing outcome, swept from the event log every five minutes. Each address-bearing decision question has one row per thread it was delivered to. A recipient-NULL row means no recipient, not necessarily refusal: it also records an admitted question whose selected deliveries were all non-fired, or a non-recipient's label on an admitted event. Read outcome to distinguish refusal from undelivered admission. Historical rows without a decision snapshot read `unrecorded`. When firing history has been evicted, the sweep estimates unrecorded events matching current eligible decision routes without inventing a route or decision-node row. Each row snapshots the pick, probability, method, charter revision and admitted picks, beside the label and the passive engagement the sweep records 48 hours after the event (engaged when the recipient cited the event or met an engagement signal declared by its source; on a refused-side row, engaged_by names the first thread that cited it). Append {event, verdict} to label a delivery: the labeller is the writing thread, never the payload, and only a chartered thread or the operator may write. `not_mine` disowns a delivery on the writer's own row. `reroute` with `to` names the thread that owns it: on the recipient's own row it also delivers the original message once to that thread's messages; from anyone else it records where the event ended up on the refused-side row and delivers nothing. Only project-owned events dispatched by a decision route accept labels; project-less events are refused. Rerouting sends nothing off-box.",
    read_projections: &[
        ProjectionSpec { key: "route", values: "route id" },
        ProjectionSpec { key: "source", values: "fact source" },
        ProjectionSpec { key: "since", values: "epoch seconds; events at or after" },
        ProjectionSpec { key: "limit", values: "N rows (default 50, max 500)" },
        ProjectionSpec { key: "verdict", values: "not_mine | reroute" },
        ProjectionSpec { key: "outcome", values: "admitted | below_threshold | refused | unanswerable | failed | unrecorded" },
        ProjectionSpec { key: "thread", values: "thread URI; rows delivered or re-routed to it" },
        ProjectionSpec { key: "engaged", values: "true | false | unchecked" },
    ],
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[MutationSpec {
        mode: ChangeMode::Append,
        required: &[FEEDBACK_EVENT, FEEDBACK_VERDICT],
        optional: &[FEEDBACK_TO, FEEDBACK_NOTE],
        label: "label a decision routing",
        example: "write({changes:[{target:\"cairn://p/PROJECT/events/feedback\",mode:\"append\",payload:{event:\"cairn://p/PROJECT/events/ID\",verdict:\"reroute\",to:\"cairn://p/PROJECT/THREAD\"}}]})",
    }],
};
