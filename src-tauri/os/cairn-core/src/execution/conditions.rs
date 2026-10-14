pub use cairn_workflow::conditions::*;

/// Execute a checkpoint as a pure verdict in a disposable executor cell. The
/// job's durable branch coordinate is resolved from the runner store; no process
/// residence or user checkout participates in identity or execution.
pub(crate) async fn execute_programmatic_checkpoint(
    orch: &crate::orchestrator::Orchestrator,
    job_id: &str,
    node_name: &str,
    command: &str,
) -> Result<ExecutedCheckpoint, String> {
    crate::execution::checks::execute_job_verdict(orch, job_id, node_name, command)
        .await
        .map(|result| ExecutedCheckpoint {
            coordinate: result.coordinate,
            output: CheckpointRunOutput {
                passed: result.exit_code == Some(0) && !result.timed_out,
                exit_code: result.exit_code.unwrap_or(-1),
                stdout: result.output,
                stderr: String::new(),
            },
        })
}
