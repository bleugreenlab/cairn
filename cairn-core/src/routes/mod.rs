//! Declarative fact routing.

mod definition;
mod dispatcher;
mod graph;
mod lifecycle;
mod predicate;
mod references;

pub use definition::{
    parse_definition, ArgumentBinding, DedupeWindow, NodePosition, RouteDefinition, RouteEdge,
    RouteNode, RouteNodeConfig, RouteSink,
};
pub use dispatcher::{dispatch, record_channel_outcome, ChannelSubmission, RouteContext};
pub use graph::RouteGraph;
pub use lifecycle::{dispatch_attention, spawn_attention_routes};
pub use predicate::{
    matches, Fact, FactFieldShape, FactRegistry, FactSourceShape, FieldKind, FieldVocabulary,
    Presence, TriggerClause,
};
pub use references::validate_references;

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

tokio::task_local! {
    static ROUTE_PROVENANCE: String;
}

pub(crate) async fn with_provenance<T>(
    route_id: String,
    future: impl std::future::Future<Output = T>,
) -> T {
    ROUTE_PROVENANCE.scope(route_id, future).await
}

pub(crate) fn current_provenance() -> Option<String> {
    ROUTE_PROVENANCE.try_with(Clone::clone).ok()
}

/// Runtime fact envelope. Provenance is structural: route-produced facts are
/// visible to other consumers but never admitted to another route firing.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RouteFact {
    pub source: String,
    pub identity: String,
    pub fields: BTreeMap<String, serde_json::Value>,
    /// What this fact says, in the producer's own words. Identity is a key and
    /// `fields` can hold a serialized envelope, so neither is readable on its
    /// own; the producer knows what the fact meant and renders it here for the
    /// firing journal. This is envelope metadata, deliberately outside `fields`
    /// so it never widens the matchable fact registry.
    #[serde(default)]
    pub summary: Option<String>,
    #[serde(default)]
    pub route_provenance: Option<String>,
}

impl RouteFact {
    pub fn is_route_generated(&self) -> bool {
        self.route_provenance.is_some()
    }
}
