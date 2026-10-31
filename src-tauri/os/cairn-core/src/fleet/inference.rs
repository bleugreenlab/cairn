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

use std::collections::HashMap;

use cairn_common::executor_protocol::{
    DeploymentOperation, DeploymentPhaseReport, ExecutorMessage, InferenceHold,
};
use tokio::sync::mpsc;

use super::{executor_names_match, executor_public_name, Fleet};

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
    pub fn set_inference_holds(&self, holds: HashMap<String, InferenceHold>) {
        let mut connections = self.lock_connections();
        for entry in connections.values_mut() {
            entry.inference_hold = holds.get(&executor_public_name(entry)).cloned();
        }
        *self.inference_holds.lock().unwrap() = holds;
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
