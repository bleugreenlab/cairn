//! The fleet's view of node scratch replicas (cairn/5253).
//!
//! Each machine advertises the replicas it holds
//! ([`ExecutorAdvertisement::node_scratches`]), and the fleet keeps the latest
//! advertisement per connection, so placement and the live-process pin read
//! replicas from memory. What needs the database (authorizing a reconnected
//! machine, deleting a closed scratch's replica, acknowledging a deletion) is
//! handed to subscribers as [`NodeScratchEvent`]s; `node_scratch::lifecycle`
//! is the one subscriber.

use std::sync::Arc;

use cairn_common::executor_protocol::{ExecutorMessage, NodeScratchReplica, ResidencyHolder};

use super::Fleet;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum NodeScratchEvent {
    /// A connection's advertised replicas changed, or it attached.
    Advertised {
        executor_id: String,
        generation: u64,
        replicas: Vec<NodeScratchReplica>,
    },
    /// A machine removed its replica of `scratch_id`.
    Deleted {
        executor_id: String,
        scratch_id: String,
    },
}

/// Handed the fleet that emitted the event rather than holding one, so a
/// subscriber never keeps the fleet it is registered on alive.
pub(crate) type NodeScratchSubscriber = Arc<dyn Fn(&Fleet, NodeScratchEvent) + Send + Sync>;

/// One machine's replica of a scratch, as placement reads it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AdvertisedReplica {
    pub executor_id: String,
    pub replica: NodeScratchReplica,
}

impl Fleet {
    pub(crate) fn subscribe_node_scratch_events(
        &self,
        subscriber: impl Fn(&Fleet, NodeScratchEvent) + Send + Sync + 'static,
    ) {
        self.node_scratch_subscribers
            .lock()
            .unwrap()
            .push(Arc::new(subscriber));
    }

    pub(super) fn emit_node_scratch_event(&self, event: NodeScratchEvent) {
        let subscribers = self.node_scratch_subscribers.lock().unwrap().clone();
        for subscriber in subscribers {
            subscriber(self, event.clone());
        }
    }

    /// Send to whatever connection `executor_id` holds now. `false` when the
    /// machine is not connected, which is not an error for anything scratch
    /// sends: a machine that is offline is cleaned when its advertisement next
    /// names what it holds.
    pub(crate) fn send_to_connected(&self, executor_id: &str, message: ExecutorMessage) -> bool {
        let sender = self
            .lock_connections()
            .get(executor_id)
            .map(|entry| entry.sender.clone());
        sender.is_some_and(|sender| sender.send(message).is_ok())
    }

    /// Every connected machine's replica of `scratch_id`.
    pub(crate) fn node_scratch_replicas(&self, scratch_id: &str) -> Vec<AdvertisedReplica> {
        self.lock_connections()
            .iter()
            .filter(|(_, entry)| !entry.sender.is_closed())
            .filter_map(|(executor_id, entry)| {
                entry
                    .advertisement
                    .node_scratches
                    .iter()
                    .find(|replica| replica.scratch_id == scratch_id)
                    .map(|replica| AdvertisedReplica {
                        executor_id: executor_id.clone(),
                        replica: replica.clone(),
                    })
            })
            .collect()
    }

    /// Every connected machine, and whether it is the colocated one.
    pub(crate) fn connected_executors(&self) -> Vec<(String, bool)> {
        self.lock_connections()
            .iter()
            .filter(|(_, entry)| !entry.sender.is_closed())
            .map(|(executor_id, entry)| (executor_id.clone(), entry.colocated))
            .collect()
    }

    /// The colocated machine's pressure and critical free-space lines, as it
    /// last reported them. The runner's scratch root shares that machine's
    /// volume, so it answers to the same lines. `None` while no colocated
    /// machine is attached, or one that does not report its thresholds.
    pub(crate) fn colocated_storage_lines(&self) -> Option<(u64, u64)> {
        self.lock_connections()
            .values()
            .find(|entry| entry.colocated && !entry.sender.is_closed())
            .map(|entry| {
                (
                    entry.health.disk.pressure_free_bytes,
                    entry.health.disk.critical_free_bytes,
                )
            })
            .filter(|(pressure, critical)| *pressure > 0 && *critical > 0)
    }

    /// The public name of a connected machine, for run-result text.
    pub(crate) fn executor_display_name(&self, executor_id: &str) -> Option<String> {
        self.lock_connections()
            .get(executor_id)
            .map(super::executor_public_name)
    }

    /// Whether a machine holding a node's scratch can take a batch of `job`: it
    /// is connected and not drained, and it has not refused that job's home. A
    /// guess that sent the job back to the machine that just refused it would
    /// undo the withdrawal, so the guess follows the withdrawal's own lapse rule
    /// and never refuses a batch: with no holder left it goes without affinity.
    pub(crate) fn scratch_holder_can_take(&self, executor_id: &str, job: &ResidencyHolder) -> bool {
        self.connected_executors()
            .iter()
            .any(|(id, _)| id == executor_id)
            && !self.is_drained(executor_id)
            && self.executor_display_name(executor_id).is_some()
            && !self
                .withdrawn_home_executors(job)
                .iter()
                .any(|id| id == executor_id)
    }
}
