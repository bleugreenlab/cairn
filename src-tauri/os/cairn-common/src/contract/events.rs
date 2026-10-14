//! The durable fact log: every minted route fact as one row, newest first.
//! Read-only; rows are minted by producers and settled by the fact log worker.

use super::specs::{NO_CROSS_ACTIONS, NO_MUTATIONS, NO_RELATED};
use super::types::*;

const LIST_KEYS: &[ProjectionSpec] = &[
    ProjectionSpec {
        key: "source",
        values: "a fact source (attention, file_changed, github_comment, thread_stream)",
    },
    ProjectionSpec {
        key: "since",
        values: "EPOCH",
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
