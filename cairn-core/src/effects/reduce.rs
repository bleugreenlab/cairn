//! Effect result reducer.
//!
//! Processes `EffectResult` values returned by the host `EffectExecutor`
//! and produces follow-on `WorkflowEffect`s that feed back into the
//! effect loop.

use super::types::{EffectResult, WorkflowEffect};

/// Process an effect result from the host, produce follow-on effects.
///
/// This is the feedback path: host executor completes an effect and returns
/// an `EffectResult`, which this function maps to zero or more new effects
/// that re-enter the effect loop.
pub fn reduce_effect_result(result: EffectResult) -> Vec<WorkflowEffect> {
    match result {
        EffectResult::CheckpointComplete {
            job_id,
            passed,
            error,
        } => {
            if passed {
                vec![WorkflowEffect::ApplyCheckpointApproval { job_id }]
            } else {
                vec![WorkflowEffect::ApplyCheckpointRejection {
                    job_id,
                    reason: error,
                }]
            }
        }
        EffectResult::ConditionEvaluated {
            execution_id,
            node_id,
            port,
            error_msg,
        } => {
            vec![WorkflowEffect::StoreConditionEvaluation {
                execution_id,
                node_id,
                port,
                error_msg,
            }]
        }
        EffectResult::WorktreeCreated { execution_id, .. } => {
            vec![WorkflowEffect::AdvanceDag {
                execution_id,
                outbox_entry_id: None,
            }]
        }
        EffectResult::WorktreeFailed { job_id, error } => {
            vec![WorkflowEffect::MarkJobFailed { job_id, error }]
        }
        EffectResult::ActionComplete { execution_id } => {
            vec![WorkflowEffect::AdvanceDag {
                execution_id,
                outbox_entry_id: None,
            }]
        }
        EffectResult::ActionFailed {
            action_run_id,
            execution_id,
            error,
        } => {
            vec![
                WorkflowEffect::MarkActionRunFailed {
                    action_run_id,
                    error,
                },
                WorkflowEffect::AdvanceDag {
                    execution_id,
                    outbox_entry_id: None,
                },
            ]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoint_pass_produces_approval() {
        let effects = reduce_effect_result(EffectResult::CheckpointComplete {
            job_id: "j1".into(),
            passed: true,
            error: None,
        });
        assert_eq!(effects.len(), 1);
        assert!(matches!(
            &effects[0],
            WorkflowEffect::ApplyCheckpointApproval { job_id } if job_id == "j1"
        ));
    }

    #[test]
    fn checkpoint_fail_produces_rejection() {
        let effects = reduce_effect_result(EffectResult::CheckpointComplete {
            job_id: "j1".into(),
            passed: false,
            error: Some("test failed".into()),
        });
        assert_eq!(effects.len(), 1);
        assert!(matches!(
            &effects[0],
            WorkflowEffect::ApplyCheckpointRejection { job_id, reason }
                if job_id == "j1" && reason.as_deref() == Some("test failed")
        ));
    }

    #[test]
    fn condition_evaluated_produces_store() {
        let effects = reduce_effect_result(EffectResult::ConditionEvaluated {
            execution_id: "e1".into(),
            node_id: "n1".into(),
            port: "yes".into(),
            error_msg: None,
        });
        assert_eq!(effects.len(), 1);
        assert!(matches!(
            &effects[0],
            WorkflowEffect::StoreConditionEvaluation { execution_id, node_id, port, .. }
                if execution_id == "e1" && node_id == "n1" && port == "yes"
        ));
    }

    #[test]
    fn worktree_created_advances_dag() {
        let effects = reduce_effect_result(EffectResult::WorktreeCreated {
            job_id: "j1".into(),
            execution_id: "e1".into(),
        });
        assert_eq!(effects.len(), 1);
        assert!(matches!(
            &effects[0],
            WorkflowEffect::AdvanceDag { execution_id, .. } if execution_id == "e1"
        ));
    }

    #[test]
    fn worktree_failed_marks_job_failed() {
        let effects = reduce_effect_result(EffectResult::WorktreeFailed {
            job_id: "j1".into(),
            error: "disk full".into(),
        });
        assert_eq!(effects.len(), 1);
        assert!(matches!(
            &effects[0],
            WorkflowEffect::MarkJobFailed { job_id, error }
                if job_id == "j1" && error == "disk full"
        ));
    }

    #[test]
    fn action_complete_advances_dag() {
        let effects = reduce_effect_result(EffectResult::ActionComplete {
            execution_id: "e1".into(),
        });
        assert_eq!(effects.len(), 1);
        assert!(matches!(
            &effects[0],
            WorkflowEffect::AdvanceDag { execution_id, .. } if execution_id == "e1"
        ));
    }

    #[test]
    fn action_failed_marks_and_advances() {
        let effects = reduce_effect_result(EffectResult::ActionFailed {
            action_run_id: "ar1".into(),
            execution_id: "e1".into(),
            error: "timeout".into(),
        });
        assert_eq!(effects.len(), 2);
        assert!(matches!(
            &effects[0],
            WorkflowEffect::MarkActionRunFailed { action_run_id, error }
                if action_run_id == "ar1" && error == "timeout"
        ));
        assert!(matches!(
            &effects[1],
            WorkflowEffect::AdvanceDag { execution_id, .. } if execution_id == "e1"
        ));
    }
}
