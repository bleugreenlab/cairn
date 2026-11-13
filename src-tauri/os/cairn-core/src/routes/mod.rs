//! Declarative fact routing.

pub mod decision;
mod definition;
mod dispatcher;
mod graph;
mod index;
mod lifecycle;
pub mod log;
mod mentions;
mod predicate;
mod references;
mod worker;

pub use definition::{
    parse_definition, ArgumentBinding, Bindable, ChannelDestination, DedupeWindow, NodePosition,
    RouteDefinition, RouteEdge, RouteNode, RouteNodeConfig, RouteSink,
};
#[cfg(test)]
pub(crate) use dispatcher::HANGING_RESPONSE;
pub use dispatcher::{
    record_channel_outcome, test_definition, ChannelSubmission, LoggedEvent, RouteContext,
    RouteTestResult, EVENT_PLACEHOLDER,
};
pub use graph::{GraphEdge, RouteGraph};
pub use index::{invalidate_trigger_index, FactLogHandle};
pub use lifecycle::{mint_attention, spawn_attention_minter};
pub use log::{mint, spawn_mint, spawn_mint_deferred, Mint, MintOwned};
pub(crate) use mentions::structural_mentions;
pub(crate) use predicate::value_matches;
pub use predicate::{
    clause_value_problem, explain_clause, matches, AddressKind, Fact, FactFieldShape, FactRegistry,
    FactSourceShape, FactSurface, FieldKind, FieldSpec, FieldVocabulary, LogDedupe, Presence,
    SourceSpec, TriggerClause, ANSWERS_PREFIX, COMPARISON_OPERATORS, EVENT_FIELD,
};
pub use references::validate_references;
pub(crate) use worker::{
    addressed_via, explain_thread_triggers, fact_row_uri, project_key, rearm_held_wakes,
    HELD_HORIZON_MS,
};
#[cfg(test)]
pub(crate) use worker::{
    drain_fact_log, drain_fact_log_within, fixture, flush_due_wakes, index_pass, EventLogPolicy,
};
pub use worker::{event_uri, spawn_fact_log_worker};

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

tokio::task_local! {
    static ROUTE_PROVENANCE: String;
}

/// Snapshot the local installation identity at an autonomous fact boundary.
///
/// Producers call this when the fact is decided; sinks must never reconstruct
/// provenance later from mutable fact fields or ambient identity.
pub(crate) fn installation_machine_origin(
    orch: &crate::orchestrator::Orchestrator,
) -> Result<crate::issues::crud::IssueAuthorship, String> {
    crate::issues::crud::installation_machine_authorship(
        orch.anon_device_manager.device_id(),
        orch.services.clock.now(),
    )
    .map_err(|error| error.to_string())
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
    /// Immutable decision-time identity captured by the producer. This remains
    /// outside matchable fields so response transforms cannot rewrite who acted.
    /// It is skipped from route journals and outbound channel records because
    /// the full appearance snapshot is storage-internal provenance.
    #[serde(skip)]
    pub origin: Option<crate::issues::crud::IssueAuthorship>,
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
