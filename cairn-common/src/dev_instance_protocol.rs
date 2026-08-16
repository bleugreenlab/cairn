//! Public wire contract for an attached, runner-owned development instance launch.
//!
//! Lease authority stays inside the runner. In particular, none of these types
//! contain a `ResidencyFence` or permit clients to issue lease operations.

use serde::{Deserialize, Serialize};

use crate::executor_protocol::{
    CellOutcome, CellUnavailableReason, ResidencyFailureKind, ResidentProcessStream,
    StorageFailureKind, StorageFailureStage,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DevInstanceLaunchRequest {
    /// Stable identity of a project registered with the runner.
    pub project_id: String,
    /// Runner-resolvable logical branch, node URI, or immutable commit selector.
    /// When absent, the runner must prove a managed-workspace coordinate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selector: Option<String>,
    /// Absolute path of the checkout the caller launched from. An implicit
    /// launch names "the commit this checkout is on", and only the caller knows
    /// which checkout that is — the runner cannot infer it from a connection.
    /// The runner proves the path belongs to the project before reading its
    /// refs, and falls back to the project's own checkout when it is absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkout: Option<String>,
    /// Terminal process key that initiated this launch, when the client runs
    /// inside a tracked terminal. This is relationship metadata, not authority.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_terminal_session_id: Option<String>,
    pub seed: String,
    #[serde(default)]
    pub force_copy: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "control", rename_all = "camelCase")]
pub enum DevInstanceLaunchControl {
    Terminate,
    ConnectionClosing,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DevInstanceReadiness {
    pub app_url: String,
    pub runner_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(
    tag = "event",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum DevInstanceLaunchEvent {
    Resolving,
    Resolved {
        selector: String,
        commit_id: String,
    },
    Acquiring,
    Acquired,
    Starting,
    Running,
    Output {
        sequence: u64,
        stream: ResidentProcessStream,
        data: Vec<u8>,
    },
    Ready {
        readiness: DevInstanceReadiness,
    },
    Exited {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        exit_code: Option<i32>,
        restartable: bool,
    },
    ExecutorLost {
        restartable: bool,
    },
    Releasing,
    Released,
    Failed {
        failure: DevInstanceLaunchFailure,
    },
}

/// Machine-readable launch failures at the public runner boundary.
///
/// Diagnostics remain human-readable context. Structured executor storage and
/// admission evidence is retained in the variants where the lower protocol
/// supplies it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum DevInstanceLaunchFailure {
    InvalidRequest {
        diagnostic: String,
    },
    InvalidProject {
        diagnostic: String,
    },
    ProjectNotFound {
        diagnostic: String,
    },
    InvalidSelector {
        diagnostic: String,
    },
    SelectorNotFound {
        diagnostic: String,
    },
    AmbiguousSelector {
        diagnostic: String,
    },
    WorkingCopyCoordinateUnproven {
        diagnostic: String,
    },
    /// The coordinate resolved, but the commit it names cannot host a dev
    /// instance: it is absent from the project's object database, or its tree
    /// does not carry the development runtime entrypoint.
    UnbuildableCoordinate {
        diagnostic: String,
    },
    ColocatedExecutorUnavailable {
        diagnostic: String,
    },
    AdmissionRefused {
        diagnostic: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<CellUnavailableReason>,
    },
    MaterializationPublication {
        diagnostic: String,
    },
    MaterializationStorage {
        diagnostic: String,
        stage: StorageFailureStage,
        storage_kind: StorageFailureKind,
        slot_retired: bool,
    },
    MaterializationCleanup {
        diagnostic: String,
    },
    MaterializationPersistence {
        diagnostic: String,
    },
    ConflictingLaunch {
        diagnostic: String,
    },
    ProcessStart {
        diagnostic: String,
    },
    ProcessExit {
        diagnostic: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        exit_code: Option<i32>,
    },
    LeaseUnavailable {
        diagnostic: String,
    },
    LeaseLost {
        diagnostic: String,
    },
    Cancelled {
        diagnostic: String,
    },
    ReleaseFailure {
        diagnostic: String,
    },
}

impl DevInstanceLaunchFailure {
    /// Translate a lower-level lifetime failure without leaking lease authority.
    /// Detailed cell outcomes take precedence over the coarse lifetime kind.
    pub fn from_residency_failure(
        kind: ResidencyFailureKind,
        diagnostic: String,
        cell_outcome: Option<CellOutcome>,
    ) -> Self {
        match cell_outcome {
            Some(CellOutcome::StorageFailure {
                stage,
                kind: storage_kind,
                diagnostic,
                slot_retired,
                ..
            }) => Self::MaterializationStorage {
                diagnostic,
                stage,
                storage_kind,
                slot_retired,
            },
            Some(CellOutcome::Unavailable { reason, diagnostic }) => Self::AdmissionRefused {
                diagnostic,
                reason: Some(reason),
            },
            Some(CellOutcome::FailedAfterExecution { diagnostic, .. }) => {
                Self::MaterializationPublication { diagnostic }
            }
            Some(CellOutcome::Cancelled { .. }) => Self::Cancelled { diagnostic },
            Some(CellOutcome::Completed { exit_code, .. }) => Self::ProcessExit {
                diagnostic,
                exit_code,
            },
            None => match kind {
                ResidencyFailureKind::InvalidDeclaration => Self::InvalidRequest { diagnostic },
                ResidencyFailureKind::ConflictingDeclaration => {
                    Self::ConflictingLaunch { diagnostic }
                }
                ResidencyFailureKind::NotFound
                | ResidencyFailureKind::StaleEpoch
                | ResidencyFailureKind::InvalidState => Self::LeaseLost { diagnostic },
                ResidencyFailureKind::Unavailable => Self::LeaseUnavailable { diagnostic },
                ResidencyFailureKind::Admission => Self::AdmissionRefused {
                    diagnostic,
                    reason: None,
                },
                ResidencyFailureKind::Process => Self::ProcessStart { diagnostic },
                ResidencyFailureKind::Cleanup => Self::MaterializationCleanup { diagnostic },
                ResidencyFailureKind::Persistence => {
                    Self::MaterializationPersistence { diagnostic }
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor_protocol::{AdmissionRejectionReason, CellExecutionMeta};

    fn round_trip<T>(value: T)
    where
        T: std::fmt::Debug + Clone + PartialEq + Serialize + for<'de> Deserialize<'de>,
    {
        let json = serde_json::to_value(value.clone()).unwrap();
        assert_eq!(serde_json::from_value::<T>(json).unwrap(), value);
    }

    fn diagnostic_failure_variants() -> Vec<DevInstanceLaunchFailure> {
        let d = || "detail".to_owned();
        vec![
            DevInstanceLaunchFailure::InvalidRequest { diagnostic: d() },
            DevInstanceLaunchFailure::InvalidProject { diagnostic: d() },
            DevInstanceLaunchFailure::ProjectNotFound { diagnostic: d() },
            DevInstanceLaunchFailure::InvalidSelector { diagnostic: d() },
            DevInstanceLaunchFailure::SelectorNotFound { diagnostic: d() },
            DevInstanceLaunchFailure::AmbiguousSelector { diagnostic: d() },
            DevInstanceLaunchFailure::WorkingCopyCoordinateUnproven { diagnostic: d() },
            DevInstanceLaunchFailure::UnbuildableCoordinate { diagnostic: d() },
            DevInstanceLaunchFailure::ColocatedExecutorUnavailable { diagnostic: d() },
            DevInstanceLaunchFailure::MaterializationPublication { diagnostic: d() },
            DevInstanceLaunchFailure::MaterializationCleanup { diagnostic: d() },
            DevInstanceLaunchFailure::MaterializationPersistence { diagnostic: d() },
            DevInstanceLaunchFailure::ConflictingLaunch { diagnostic: d() },
            DevInstanceLaunchFailure::ProcessStart { diagnostic: d() },
            DevInstanceLaunchFailure::LeaseUnavailable { diagnostic: d() },
            DevInstanceLaunchFailure::LeaseLost { diagnostic: d() },
            DevInstanceLaunchFailure::Cancelled { diagnostic: d() },
            DevInstanceLaunchFailure::ReleaseFailure { diagnostic: d() },
        ]
    }

    #[test]
    fn request_and_every_control_round_trip() {
        round_trip(DevInstanceLaunchRequest {
            project_id: "project-1".into(),
            selector: Some("feature/dev".into()),
            checkout: Some("/repos/app".into()),
            source_terminal_session_id: Some("terminal-session".into()),
            seed: "empty".into(),
            force_copy: true,
        });
        round_trip(DevInstanceLaunchControl::Terminate);
        round_trip(DevInstanceLaunchControl::ConnectionClosing);
    }

    #[test]
    fn every_event_variant_round_trips() {
        let failure = DevInstanceLaunchFailure::LeaseLost {
            diagnostic: "executor disconnected".into(),
        };
        let events = vec![
            DevInstanceLaunchEvent::Resolving,
            DevInstanceLaunchEvent::Resolved {
                selector: "feature/dev".into(),
                commit_id: "abc123".into(),
            },
            DevInstanceLaunchEvent::Acquiring,
            DevInstanceLaunchEvent::Acquired,
            DevInstanceLaunchEvent::Starting,
            DevInstanceLaunchEvent::Running,
            DevInstanceLaunchEvent::Output {
                sequence: 4,
                stream: ResidentProcessStream::Stdout,
                data: b"ready".to_vec(),
            },
            DevInstanceLaunchEvent::Ready {
                readiness: DevInstanceReadiness {
                    app_url: "http://localhost:1420".into(),
                    runner_url: "http://localhost:3849".into(),
                },
            },
            DevInstanceLaunchEvent::Exited {
                exit_code: Some(0),
                restartable: false,
            },
            DevInstanceLaunchEvent::ExecutorLost { restartable: true },
            DevInstanceLaunchEvent::Releasing,
            DevInstanceLaunchEvent::Released,
            DevInstanceLaunchEvent::Failed { failure },
        ];
        for event in events {
            round_trip(event);
        }
    }

    #[test]
    fn every_failure_variant_round_trips() {
        let mut failures = diagnostic_failure_variants();
        failures.extend([
            DevInstanceLaunchFailure::AdmissionRefused {
                diagnostic: "full".into(),
                reason: Some(CellUnavailableReason::AdmissionRejected {
                    reason: AdmissionRejectionReason::QueueFull,
                }),
            },
            DevInstanceLaunchFailure::MaterializationStorage {
                diagnostic: "disk full".into(),
                stage: StorageFailureStage::ProvisioningCheckout,
                storage_kind: StorageFailureKind::NoSpace,
                slot_retired: true,
            },
            DevInstanceLaunchFailure::ProcessExit {
                diagnostic: "early exit".into(),
                exit_code: Some(101),
            },
        ]);
        for failure in failures {
            round_trip(failure);
        }
    }

    #[test]
    fn wire_shape_is_camel_case_and_contains_no_lease_fence() {
        let request = serde_json::to_value(DevInstanceLaunchRequest {
            project_id: "p".into(),
            selector: None,
            checkout: None,
            source_terminal_session_id: None,
            seed: "empty".into(),
            force_copy: true,
        })
        .unwrap();
        assert_eq!(
            request,
            serde_json::json!({"projectId":"p","seed":"empty","forceCopy":true})
        );

        let resolved = serde_json::to_value(DevInstanceLaunchEvent::Resolved {
            selector: "main".into(),
            commit_id: "abc".into(),
        })
        .unwrap();
        assert_eq!(
            resolved,
            serde_json::json!({"event":"resolved","selector":"main","commitId":"abc"})
        );
        assert!(!serde_json::to_string(&resolved).unwrap().contains("lease"));
    }

    #[test]
    fn lifetime_failure_kinds_map_exhaustively() {
        let cases = [
            (ResidencyFailureKind::InvalidDeclaration, "invalidRequest"),
            (
                ResidencyFailureKind::ConflictingDeclaration,
                "conflictingLaunch",
            ),
            (ResidencyFailureKind::NotFound, "leaseLost"),
            (ResidencyFailureKind::Unavailable, "leaseUnavailable"),
            (ResidencyFailureKind::StaleEpoch, "leaseLost"),
            (ResidencyFailureKind::InvalidState, "leaseLost"),
            (ResidencyFailureKind::Admission, "admissionRefused"),
            (ResidencyFailureKind::Process, "processStart"),
            (ResidencyFailureKind::Cleanup, "materializationCleanup"),
            (
                ResidencyFailureKind::Persistence,
                "materializationPersistence",
            ),
        ];
        for (kind, expected) in cases {
            let mapped =
                DevInstanceLaunchFailure::from_residency_failure(kind, "detail".into(), None);
            assert_eq!(serde_json::to_value(mapped).unwrap()["kind"], expected);
        }
    }

    #[test]
    fn lifetime_cell_outcomes_preserve_typed_detail() {
        let storage = CellOutcome::StorageFailure {
            request_id: "r".into(),
            attempt_id: "a".into(),
            stage: StorageFailureStage::DeltaUpload,
            kind: StorageFailureKind::QuotaExceeded,
            diagnostic: "quota".into(),
            slot_retired: true,
        };
        assert_eq!(
            DevInstanceLaunchFailure::from_residency_failure(
                ResidencyFailureKind::Persistence,
                "outer".into(),
                Some(storage)
            ),
            DevInstanceLaunchFailure::MaterializationStorage {
                diagnostic: "quota".into(),
                stage: StorageFailureStage::DeltaUpload,
                storage_kind: StorageFailureKind::QuotaExceeded,
                slot_retired: true,
            }
        );

        let unavailable = CellOutcome::Unavailable {
            reason: CellUnavailableReason::AdmissionRejected {
                reason: AdmissionRejectionReason::Draining,
            },
            diagnostic: "draining".into(),
        };
        assert!(matches!(
            DevInstanceLaunchFailure::from_residency_failure(
                ResidencyFailureKind::Admission,
                "outer".into(),
                Some(unavailable)
            ),
            DevInstanceLaunchFailure::AdmissionRefused {
                reason: Some(CellUnavailableReason::AdmissionRejected {
                    reason: AdmissionRejectionReason::Draining
                }),
                ..
            }
        ));
    }

    #[test]
    fn completed_lifetime_outcome_maps_to_typed_process_exit() {
        let completed = CellOutcome::Completed {
            request_id: "r".into(),
            attempt_id: "a".into(),
            exit_code: Some(7),
            output: String::new(),
            timed_out: false,
            metadata: CellExecutionMeta {
                warmth: None,
                load_context: None,
                executor_id: "e".into(),
                executor_device_id: "d".into(),
                executor_connection_generation: 1,
                cell_id: "c".into(),
                cell_epoch: 2,
                started_at_unix_ms: 3,
                finished_at_unix_ms: 4,
                duration_ms: None,
                peak_rss_bytes: None,
                peak_physical_footprint_bytes: None,
                disk_delta_bytes: None,
                measurement_quality: None,
                environment_fingerprint: String::new(),
                verdict_platform: None,
                verdict_arch: None,
                toolchain_fingerprint: None,
                verdict_environment_hash: None,
                sandbox: None,
            },
            mutation_delta: None,
            sandbox_denials: vec![],
            tracked_modifications: None,
        };
        assert_eq!(
            DevInstanceLaunchFailure::from_residency_failure(
                ResidencyFailureKind::Process,
                "exit".into(),
                Some(completed)
            ),
            DevInstanceLaunchFailure::ProcessExit {
                diagnostic: "exit".into(),
                exit_code: Some(7)
            }
        );
    }
}
