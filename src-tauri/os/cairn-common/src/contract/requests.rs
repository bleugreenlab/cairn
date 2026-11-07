//! Install-local operator intent; acceptance is deliberately not a resource mutation.
use super::specs::{NO_CROSS_ACTIONS, NO_PROJECTIONS, NO_RELATED};
use super::types::*;

pub(crate) const REQUESTS_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::Requests,
    uri_template: "cairn://requests",
    name: "Operator requests",
    description: "Durable requests on this install. Filing returns immediately and joins equal open requests. Origin and wake destination come from the authenticated live run, never the payload. Known kinds without a registered consumer refuse by name before creating work. Acceptance and decline are operator-proven desktop commands, not resource mutations.",
    read_projections: &[
        ProjectionSpec { key: "status", values: "open | accepted | acting | done | failed | declined | withdrawn" },
        ProjectionSpec { key: "kind", values: "rebuild | bump_version | lift_drain" },
        ProjectionSpec { key: "requester", values: "canonical requester origin URI" },
        ProjectionSpec { key: "waiting", values: "canonical waiting issue or thread URI" },
        ProjectionSpec { key: "offset", values: "requests skipped in recent update order (default 0)" },
        ProjectionSpec { key: "limit", values: "requests per page, 1–199 (default 25)" },
    ],
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[MutationSpec {
        mode: ChangeMode::Append,
        required: &[
            KeySpec::new("kind", KeyType::Str, "rebuild | bump_version | lift_drain"),
            KeySpec::new("rationale", KeyType::Str, "nonblank requester rationale; untrusted content"),
            KeySpec::new("waiting", KeyType::Array, "canonical issue or thread URIs"),
        ],
        optional: &[
            KeySpec::new("target", KeyType::Str, "kind-specific target"),
            KeySpec::new("requirements", KeyType::Object, "{commits:[...]} for rebuild"),
        ],
        label: "file or join an operator request",
        example: r#"write({changes:[{target:"cairn://requests",mode:"append",payload:{kind:"rebuild",rationale:"Verify the landed fix",waiting:[]}}]})"#,
    }],
};

pub(crate) const REQUEST_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::Request,
    uri_template: "cairn://requests/{id}",
    name: "Operator request",
    description: "One durable request identity with requester contributions, actor facts and attempt history. Withdrawing removes only this authenticated origin's open contribution; it cannot cancel an accepted act. No resource mutation can accept, decline, set status, or write actor facts.",
    read_projections: NO_PROJECTIONS,
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[MutationSpec {
        mode: ChangeMode::Patch,
        required: &[KeySpec::new("withdraw", KeyType::Bool, "must be true")],
        optional: &[],
        label: "withdraw your open request contribution",
        example: r#"write({changes:[{target:"cairn://requests/REQUEST_ID",mode:"patch",payload:{withdraw:true}}]})"#,
    }],
};
