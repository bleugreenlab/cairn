//! Closed request-kind policy and runner-owned consumer registration.
//!
//! A definition existing is not evidence that its physical consumer exists.
//! Empty adapter slots refuse work before persistence or authorization.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::AcceptedRequestContext;
use crate::orchestrator::Orchestrator;
use cairn_common::requests::{
    DrainAcceptance, RequestAcceptance, RequestAttemptOutcome, RequestKind,
};

pub const REGISTERED_KINDS: &[&str] = &["rebuild", "bump_version", "lift_drain"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcceptancePolicy {
    OperatorCredential,
}

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
        dependency: "cairn/6386 request-driven rebuild adapter, cairn/6384 protected rebuild ledger/floor, cairn/6230 source stamp and cairn/6385 fleet phases",
        acceptance_policy: AcceptancePolicy::OperatorCredential,
    },
    KindDefinition {
        name: "bump_version",
        actor: "devops.rebuild",
        dependency: "cairn/6387 request-driven bump/landing adapter, cairn/6386 rebuild launch and cairn/6385 fleet phases",
        acceptance_policy: AcceptancePolicy::OperatorCredential,
    },
    KindDefinition {
        name: "lift_drain",
        actor: "execution.fleet",
        dependency: "cairn/6383 execution-owned generation-attributed FleetConfig drain adapter",
        acceptance_policy: AcceptancePolicy::OperatorCredential,
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

/// Plain release versions only: request targets cannot smuggle prerelease or
/// build metadata into the bump actor's restricted transformation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) struct ReleaseVersion(pub u64, pub u64, pub u64);

impl ReleaseVersion {
    pub(crate) fn parse(value: &str) -> Result<Self, String> {
        let invalid = || {
            format!("Invalid bump_version target '{value}': use sm, md, lg or plain X.Y.Z (no prerelease/build metadata)")
        };
        let parts: Vec<_> = value.split('.').collect();
        if parts.len() != 3 {
            return Err(invalid());
        }
        let mut numbers = [0; 3];
        for (index, part) in parts.iter().enumerate() {
            if part.is_empty()
                || !part.bytes().all(|byte| byte.is_ascii_digit())
                || (part.len() > 1 && part.starts_with('0'))
            {
                return Err(invalid());
            }
            numbers[index] = part.parse().map_err(|_| invalid())?;
        }
        Ok(Self(numbers[0], numbers[1], numbers[2]))
    }

    pub(crate) fn resolve(self, requested: &str) -> Result<Self, String> {
        let overflow = || "bump_version target overflows a release version component".to_string();
        match requested {
            "sm" => Ok(Self(
                self.0,
                self.1,
                self.2.checked_add(1).ok_or_else(overflow)?,
            )),
            "md" => Ok(Self(self.0, self.1.checked_add(1).ok_or_else(overflow)?, 0)),
            "lg" => Ok(Self(self.0.checked_add(1).ok_or_else(overflow)?, 0, 0)),
            explicit => {
                let target = Self::parse(explicit)?;
                if target <= self {
                    return Err(format!(
                        "bump_version target {target} must be greater than running version {self}"
                    ));
                }
                Ok(target)
            }
        }
    }
}

impl std::fmt::Display for ReleaseVersion {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}.{}.{}", self.0, self.1, self.2)
    }
}

/// Trusted source state comes from the registered runner component, never the
/// filing payload. A source stamp is read by the consumer's canonical reader.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ActorFacts {
    pub document: Value,
    pub running_version: Option<String>,
    pub main_commit: Option<String>,
    pub candidate_commit: Option<String>,
    pub base_commit: Option<String>,
    pub policy_project_id: Option<String>,
    pub drain: Option<DrainAcceptance>,
}

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
    async fn availability(&self, orch: &Orchestrator) -> Result<(), String>;
    async fn facts(
        &self,
        orch: &Orchestrator,
        request_id: Option<&str>,
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
}

/// Registration is runner code, never a resource mutation, plugin payload or
/// agent-selected actor. No production adapter is installed by the primitive.
#[derive(Default)]
pub struct RequestRegistry {
    adapters: RwLock<BTreeMap<&'static str, Arc<dyn RequestConsumer>>>,
}

impl RequestRegistry {
    #[allow(dead_code)] // Production registration lands with the enforcing consumer.
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

pub(crate) fn act_digest(
    kind: RequestKind,
    target: Option<&str>,
) -> cairn_common::authorization::ActDigest {
    cairn_common::authorization::ActDigest::of(kind.as_str(), |encoder| match target {
        Some(target) => {
            encoder.field("target", target);
        }
        None => {
            encoder.field("target", "");
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

    #[test]
    fn release_targets_are_strict_and_tiers_fix_minimum_at_filing() {
        let running = ReleaseVersion::parse("1.2.3").unwrap();
        assert_eq!(running.resolve("sm").unwrap().to_string(), "1.2.4");
        assert_eq!(running.resolve("md").unwrap().to_string(), "1.3.0");
        assert_eq!(running.resolve("lg").unwrap().to_string(), "2.0.0");
        assert_eq!(running.resolve("2.1.0").unwrap().to_string(), "2.1.0");
        for bad in [
            "1.2",
            "01.2.3",
            "1.2.3-alpha",
            "1.2.4+build",
            "+1.2.4",
            " 1.2.4",
            "1.2.3",
            "0.9.0",
        ] {
            assert!(running.resolve(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn smaller_explicit_target_never_reduces_another_contribution_minimum() {
        let running = ReleaseVersion(1, 2, 3);
        let minimums = [
            running.resolve("lg").unwrap(),
            running.resolve("1.2.9").unwrap(),
        ];
        assert_eq!(minimums.into_iter().max().unwrap(), ReleaseVersion(2, 0, 0));
    }
}
