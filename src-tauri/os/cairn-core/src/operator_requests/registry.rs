//! Closed request-kind policy and runner-owned consumer registration.
//!
//! A definition existing is not evidence that its physical consumer exists.
//! Empty adapter slots refuse work before persistence or authorization.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use serde_json::Value;

use super::AcceptedRequestContext;
use crate::orchestrator::Orchestrator;
use cairn_common::requests::{
    FencedCommandAcceptance, RequestAcceptance, RequestAttemptOutcome, RequestKind,
};

pub const REGISTERED_KINDS: &[&str] = &["rebuild", "lift_drain", "fenced_command"];

pub use cairn_common::requests::AcceptancePolicy;

#[derive(Debug, Clone, Copy)]
pub struct KindDefinition {
    pub name: &'static str,
    pub actor: &'static str,
    pub dependency: &'static str,
    pub acceptance_policy: AcceptancePolicy,
}

pub const DEFINITIONS: &[KindDefinition] = &[
    KindDefinition {
        name: "rebuild",
        actor: "devops.rebuild",
        dependency: "the macOS rebuild consumer, which only a macOS runner registers",
        acceptance_policy: RequestKind::Rebuild.acceptance_policy(),
    },
    KindDefinition {
        name: "lift_drain",
        actor: "execution.fleet",
        dependency: "cairn/6383 execution-owned generation-attributed FleetConfig drain adapter",
        acceptance_policy: RequestKind::LiftDrain.acceptance_policy(),
    },
    KindDefinition {
        name: "fenced_command",
        actor: super::fenced_command::ACTOR,
        dependency:
            "the fenced_command consumer, which dispatches through the fleet's operator-act path",
        acceptance_policy: RequestKind::FencedCommand.acceptance_policy(),
    },
];

pub fn definition(kind: &str) -> Result<&'static KindDefinition, String> {
    DEFINITIONS
        .iter()
        .find(|entry| entry.name == kind)
        .ok_or_else(|| {
            format!(
                "Unknown operator request kind '{kind}'; registered kinds: {}",
                REGISTERED_KINDS.join(", ")
            )
        })
}

/// Trusted source state comes from the registered runner component, never the
/// filing payload. A source stamp is read by the consumer's canonical reader.
pub(crate) use cairn_common::requests::ActorFacts;

#[derive(Debug, Clone)]
pub(crate) struct ExecutorTarget {
    pub uri: String,
    pub executor_id: String,
    pub drain_key: String,
}

/// A report is checked by the consumer against decisive physical evidence
/// before the primitive persists an outcome. It is not an agent JSON patch.
pub(crate) struct ConsumerReport {
    pub outcome: RequestAttemptOutcome,
    pub document: Value,
    pub settled: bool,
    /// Decisive artifact/boot evidence, verified by the registered consumer.
    pub built_commit: Option<String>,
}

/// Consumers provide facts and validate their physical snapshot, but cannot
/// create an acceptance. Dispatch receives only a checked acting reference,
/// never a bearer or caller-authored facts.
#[async_trait]
pub(crate) trait RequestConsumer: Send + Sync {
    fn kind(&self) -> &'static str;
    fn actor(&self) -> &'static str;
    /// Held by the primitive from before the DB claim until dispatch (and any
    /// report it makes) returns, and by the consumer's own reconciliation, so
    /// recovery can never observe a claim whose effect is still in flight.
    async fn exclusion(&self) -> Option<tokio::sync::OwnedMutexGuard<()>> {
        None
    }
    async fn availability(&self, orch: &Orchestrator) -> Result<(), String>;
    /// `target` is the request target as filed (or stored), so a consumer whose
    /// facts are per-target (lift_drain's executor) never needs mutable state
    /// to learn what a filing is about. Consumers with one global fact set
    /// ignore it.
    async fn facts(
        &self,
        orch: &Orchestrator,
        request_id: Option<&str>,
        target: Option<&str>,
    ) -> Result<ActorFacts, String>;
    async fn validate_snapshot(
        &self,
        orch: &Orchestrator,
        acceptance: &RequestAcceptance,
    ) -> Result<(), String>;
    async fn contains_commits(
        &self,
        orch: &Orchestrator,
        candidate: &str,
        required: &[String],
    ) -> Result<(), String>;
    async fn executor_target(
        &self,
        orch: &Orchestrator,
        uri: &str,
    ) -> Result<ExecutorTarget, String>;
    async fn verify_report(
        &self,
        orch: &Orchestrator,
        context: &AcceptedRequestContext,
        report: ConsumerReport,
    ) -> Result<ConsumerReport, String>;
    async fn dispatch(
        &self,
        orch: &Orchestrator,
        context: &AcceptedRequestContext,
    ) -> Result<(), String>;
    /// A bounded, operator-proven request to stop an acting attempt. Consumers
    /// without a safe stop point refuse; a cancel never settles the attempt by
    /// itself, the actor's own outcome does.
    async fn cancel(
        &self,
        _orch: &Orchestrator,
        _context: &AcceptedRequestContext,
        _operator: &cairn_common::identity::PrincipalRef,
    ) -> Result<(), String> {
        Err(format!("{} has no bounded cancel", self.kind()))
    }
}

/// Registration is runner code, never a resource mutation, plugin payload or
/// agent-selected actor. No production adapter is installed by the primitive.
#[derive(Default)]
pub struct RequestRegistry {
    adapters: RwLock<BTreeMap<&'static str, Arc<dyn RequestConsumer>>>,
}

impl RequestRegistry {
    pub(crate) fn register(&self, adapter: Arc<dyn RequestConsumer>) -> Result<(), String> {
        let entry = definition(adapter.kind())?;
        if entry.actor != adapter.actor() {
            return Err(format!(
                "{} is registered to actor {}, not {}",
                entry.name,
                entry.actor,
                adapter.actor()
            ));
        }
        let mut adapters = self
            .adapters
            .write()
            .map_err(|_| "Operator request registry is unavailable")?;
        if adapters.contains_key(entry.name) {
            return Err(format!("{} already has a registered consumer", entry.name));
        }
        adapters.insert(entry.name, adapter);
        Ok(())
    }

    pub(crate) fn consumer(&self, kind: &str) -> Result<Arc<dyn RequestConsumer>, String> {
        let entry = definition(kind)?;
        self.adapters
            .read()
            .map_err(|_| "Operator request registry is unavailable")?
            .get(entry.name)
            .cloned()
            .ok_or_else(|| {
                format!(
                    "{} is UNAVAILABLE: requires {}",
                    entry.name, entry.dependency
                )
            })
    }

    pub fn unavailable_reason(&self, kind: &str) -> Option<String> {
        self.consumer(kind).err()
    }
}

/// The digest an acceptance's binding names. A `fenced_command` act binds each
/// field of its frozen act and executor witness through the canonical encoder,
/// so the binding names the machine, the measurement and the exact bytes.
pub(crate) fn act_digest(
    kind: RequestKind,
    target: Option<&str>,
    fenced: Option<&FencedCommandAcceptance>,
) -> cairn_common::authorization::ActDigest {
    cairn_common::authorization::ActDigest::of(kind.as_str(), |encoder| {
        encoder.field("target", target.unwrap_or(""));
        if let Some(fenced) = fenced {
            let (act, witness) = (&fenced.act, &fenced.witness);
            encoder.field("executor_id", &act.executor_id);
            encoder.field("device_id", &act.device_id);
            encoder.field("project_id", &act.project_id);
            encoder.field("branch", &act.branch);
            encoder.field("commit", &act.commit);
            encoder.field("cwd", &act.cwd);
            encoder.field("command", &act.command);
            encoder.field("timeout_ms", &act.timeout_ms.to_string());
            encoder.field("node", &act.node);
            encoder.field(
                "connection_generation",
                &witness.connection_generation.to_string(),
            );
            encoder.field("executor_build_id", &witness.executor_build_id);
            encoder.field("boot_generation", &witness.boot_generation);
            encoder.field("floor_fingerprint", &witness.floor_fingerprint);
            encoder.field(
                "floor_measured_at_unix_ms",
                &witness.floor_measured_at_unix_ms.to_string(),
            );
            encoder.field("scratch_head", fenced.scratch_head.as_deref().unwrap_or(""));
            // Bound beside the target that already encodes it, so the
            // binding names the declaration's commit and digest by field.
            // Absent for a raw act, whose digest is unchanged.
            if let Some(declared) = &act.declaration {
                encoder.field(
                    "declaration",
                    &serde_json::to_string(declared).unwrap_or_default(),
                );
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_kind_names_the_closed_registry() {
        let error = definition("shell").unwrap_err();
        for name in REGISTERED_KINDS {
            assert!(error.contains(name));
        }
    }

    #[test]
    fn production_consumers_are_unavailable_by_name() {
        let registry = RequestRegistry::default();
        for entry in DEFINITIONS {
            let error = registry.unavailable_reason(entry.name).unwrap();
            assert!(error.contains(entry.name));
            assert!(error.contains(entry.dependency));
            assert!(error.contains("UNAVAILABLE"));
        }
    }
}
