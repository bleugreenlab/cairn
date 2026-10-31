//! The fleet's half of inference deployments: the holds that keep a
//! deployment's machines away from batch work, the commands sent to its head,
//! and the reports that come back.
//!
//! A hold is a drain with a named reason (`InferenceReserved`), held by this
//! runner alone and never told to the machine, beside the operator's drain and
//! the self-update hold. The cairn-inference facet decides which machines are
//! held; this module only applies the set it is given and routes the head's
//! reports to whoever subscribed. Holds are keyed by executor public name,
//! which is how a deployment names its machines.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use cairn_common::executor_protocol::{
    CellPriority, DeploymentOperation, DeploymentPhaseReport, ExecutorMessage, InferenceHold,
    MACHINE_READING_STALE_AFTER_MS,
};
use cairn_inference::control::{gib, RunningBatch};
use tokio::sync::mpsc;

use super::{executor_names_match, executor_public_name, CancelCause, Fleet, RequestIdentity};

/// Why a batch was stopped to make room for an inference deployment: what the
/// deployment needed on the machine and what the measurements said, so the
/// work's own record says what took its machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Preemption {
    pub deployment: String,
    /// The model the deployment serves.
    pub model: String,
    /// The machine, by public name.
    pub executor: String,
    pub need_bytes: u64,
    pub free_bytes: u64,
    /// What the batch's processes held when it was chosen.
    pub resident_bytes: u64,
}

impl Preemption {
    pub fn sentence(&self) -> String {
        format!(
            "preempted to load {} (inference deployment {}) on {}, which needed {} there with {} free while this work held {}, measured",
            self.model,
            self.deployment,
            self.executor,
            gib(self.need_bytes),
            gib(self.free_bytes),
            gib(self.resident_bytes)
        )
    }
}

/// One report from a deployment's head, with the machine it came from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeploymentReportReceived {
    pub executor: String,
    pub command_id: String,
    pub deployment: String,
    pub phase: DeploymentPhaseReport,
}

impl Fleet {
    /// Replace every inference hold, by executor public name, and restamp each
    /// live connection. The caller hands over the whole set each time, so a
    /// released hold is simply absent.
    ///
    /// The connection table is taken first, as attach takes it before reading
    /// the holds, so an attach either sees this set or is restamped by it.
    ///
    /// Work waiting on a hold (placement waits on one rather than refusing)
    /// is woken when any hold is released, so it is offered the machine at
    /// once.
    pub fn set_inference_holds(&self, holds: HashMap<String, InferenceHold>) {
        let mut released = false;
        {
            let mut connections = self.lock_connections();
            for entry in connections.values_mut() {
                let hold = holds.get(&executor_public_name(entry)).cloned();
                released |= entry.inference_hold.is_some() && hold.is_none();
                entry.inference_hold = hold;
            }
            *self.inference_holds.lock().unwrap() = holds;
        }
        if released {
            self.connection_ready.notify_waiters();
        }
    }

    /// The hold for the machine with this public name, read under the
    /// connection table's lock by attach.
    pub(super) fn inference_hold_named(&self, public_name: &str) -> Option<InferenceHold> {
        self.inference_holds
            .lock()
            .unwrap()
            .get(public_name)
            .cloned()
    }

    /// Every hold this runner applies now, by executor public name.
    pub fn inference_holds(&self) -> HashMap<String, InferenceHold> {
        self.inference_holds.lock().unwrap().clone()
    }

    /// The batches running on the attached machines publicly named `names`,
    /// by name, as a reserving deployment's drain weighs them.
    ///
    /// A batch is restartable when it was submitted as a check, so an agent
    /// turn (and work stating no priority) never is. Its memory is the latest
    /// live sample of its whole process tree, and only while that sample is
    /// fresh: an old reading is history, not a measurement of a conflict.
    pub fn running_batches(&self, names: &BTreeSet<String>) -> BTreeMap<String, Vec<RunningBatch>> {
        let now = super::unix_time_ms();
        let connections = self.lock_connections();
        let causes = self.cancel_causes.lock().unwrap();
        let mut running = BTreeMap::new();
        for entry in connections.values() {
            let name = executor_public_name(entry);
            if !names.contains(&name) {
                continue;
            }
            let batches = entry
                .occupancy()
                .executing_requests
                .into_iter()
                .map(|request| RunningBatch {
                    restartable: matches!(
                        request.priority,
                        Some(CellPriority::ReviewCheck | CellPriority::WriteCheck)
                    ),
                    resident_bytes: request
                        .live_usage
                        .filter(|sample| {
                            now.saturating_sub(sample.measured_at_unix_ms)
                                <= MACHINE_READING_STALE_AFTER_MS
                        })
                        .and_then(|sample| sample.resident_bytes),
                    preempted: causes
                        .preempted(&(request.request_id.clone(), request.attempt_id.clone())),
                    request_id: request.request_id,
                    attempt_id: request.attempt_id,
                })
                .collect();
            running.insert(name, batches);
        }
        running
    }

    /// Stop one running batch to make room for an inference deployment. The
    /// cause travels to whoever waits on it, so a check ends as preempted, not
    /// as a failure. False when the attempt is no longer bound to a machine.
    pub(crate) fn preempt(&self, request: RequestIdentity, preemption: Preemption) -> bool {
        if self.queue.binding_of(&request).is_none() {
            return false;
        }
        log::info!(
            "inference: preempting request {} attempt {}: {}",
            request.0,
            request.1,
            preemption.sentence()
        );
        self.retire_item(&request, CancelCause::Preempted(Arc::new(preemption)));
        true
    }

    /// Receive every deployment report from now on. One subscriber: a second
    /// call replaces the first, whose receiver then ends.
    pub fn subscribe_deployment_reports(
        &self,
    ) -> mpsc::UnboundedReceiver<DeploymentReportReceived> {
        let (tx, rx) = mpsc::unbounded_channel();
        *self.deployment_reports.lock().unwrap() = Some(tx);
        rx
    }

    pub(super) fn deliver_deployment_report(
        &self,
        executor_id: &str,
        command_id: String,
        deployment: String,
        phase: DeploymentPhaseReport,
    ) {
        let executor = self
            .executor_public_name(executor_id)
            .unwrap_or_else(|| executor_id.to_string());
        let delivered = self
            .deployment_reports
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|tx| {
                tx.send(DeploymentReportReceived {
                    executor: executor.clone(),
                    command_id: command_id.clone(),
                    deployment: deployment.clone(),
                    phase: phase.clone(),
                })
                .is_ok()
            });
        if !delivered {
            log::warn!(
                "inference: {executor} reported {phase:?} for deployment {deployment} (command {command_id}) and nothing is listening"
            );
        }
    }

    /// Send a deployment command to the attached machine with this public
    /// name. Fails, naming why, when no such machine is attached or its link
    /// has closed: the caller fails the command rather than waiting out its
    /// bound.
    pub fn send_deployment_command(
        &self,
        executor_name: &str,
        command_id: &str,
        deployment: &str,
        operation: DeploymentOperation,
    ) -> Result<(), String> {
        let sender = {
            let connections = self.lock_connections();
            connections
                .values()
                .find(|entry| executor_names_match(executor_name, &executor_public_name(entry)))
                .map(|entry| entry.sender.clone())
        };
        let Some(sender) = sender else {
            return Err(format!("executor {executor_name} is not attached"));
        };
        sender
            .send(ExecutorMessage::DeploymentCommand {
                command_id: command_id.to_string(),
                deployment: deployment.to_string(),
                operation,
            })
            .map_err(|_| {
                format!("executor {executor_name}'s link closed before the command was sent")
            })
    }
}
