//! The release publisher as the fleet surfaces present it: a role its host
//! machine has, not a machine.
//!
//! A `publishOnly` executor is a second `cairn-exec` under its own account on
//! a host that already runs an ordinary executor (docs/executor.md, "Publishes
//! only"). Placement, enrolment and the isolation probe treat it as the
//! executor it is. Everything that lists or counts machines asks here instead,
//! so it is folded into its host's row as one readiness line and never counted
//! as a machine of its own.

use std::collections::HashMap;

use cairn_common::executor_protocol::{
    BuildPrerequisiteStatus, EnrolledRemote, ExecutorHealthSnapshot, FleetPublisher, MachineRole,
};

impl super::Fleet {
    /// Adopt [`super::FleetConfig::publisher_hosts`] of a config just loaded or
    /// just persisted, replacing the whole cache.
    pub fn hydrate_publisher_hosts(&self, hosts: impl IntoIterator<Item = (String, String)>) {
        *self.publisher_hosts.lock().unwrap() = hosts.into_iter().collect();
    }

    /// Publisher executor ID to the executor ID of the ordinary executor on
    /// its machine.
    pub fn publisher_hosts(&self) -> HashMap<String, String> {
        self.publisher_hosts.lock().unwrap().clone()
    }
}

/// Every `publishOnly` executor among `attached` and `enrolled`, with the host
/// row that carries it and whether it can publish now. Sorted by name.
pub fn fleet_publishers<'a>(
    attached: impl IntoIterator<Item = &'a ExecutorHealthSnapshot>,
    enrolled: &[EnrolledRemote],
    hosts: &HashMap<String, String>,
    now_unix_ms: u64,
) -> Vec<FleetPublisher> {
    let attached: Vec<&ExecutorHealthSnapshot> = attached.into_iter().collect();
    let name_of = |executor_id: &str| {
        attached
            .iter()
            .find(|health| health.identity.executor_id == executor_id)
            .map(|health| health.public_name.clone())
            .or_else(|| {
                enrolled
                    .iter()
                    .find(|remote| remote.executor_id == executor_id)
                    .map(|remote| remote.name.clone())
            })
            .unwrap_or_else(|| executor_id.to_string())
    };
    let mut publishers: Vec<FleetPublisher> = attached
        .iter()
        .filter(|health| health.role == MachineRole::PublishOnly)
        .map(|health| {
            (
                health.identity.executor_id.clone(),
                health.public_name.clone(),
                not_ready(health, now_unix_ms),
            )
        })
        .chain(
            enrolled
                .iter()
                .filter(|remote| remote.role == MachineRole::PublishOnly)
                .map(|remote| {
                    (
                        remote.executor_id.clone(),
                        remote.name.clone(),
                        Some("not attached".to_string()),
                    )
                }),
        )
        .map(|(executor_id, name, not_ready)| {
            let host_executor_id = hosts.get(&executor_id).cloned();
            FleetPublisher {
                host_name: host_executor_id.as_deref().map(name_of),
                host_executor_id,
                executor_id,
                name,
                not_ready,
            }
        })
        .collect();
    publishers.sort_by(|a, b| a.name.cmp(&b.name));
    publishers
}

/// The one reason an attached publisher cannot publish now, in placement's own
/// terms: an isolation verdict that is not a fresh PASS, then build
/// prerequisites the machine itself reported MISSING (placement enforces
/// nothing for a verdict that is absent or unchecked, so neither is a reason).
fn not_ready(health: &ExecutorHealthSnapshot, now_unix_ms: u64) -> Option<String> {
    let capabilities = &health.advertisement.capabilities;
    if let Some(why) = super::publisher_ineligibility(
        health.role,
        capabilities.publish_isolation.as_ref(),
        now_unix_ms,
    ) {
        return Some(why);
    }
    capabilities
        .build_prerequisites
        .as_ref()
        .filter(|check| check.effective_status() == BuildPrerequisiteStatus::Missing)
        .map(|_| "build prerequisites MISSING".to_string())
}
