//! Durable history of programmatic command-checkpoint runs and the pure
//! helpers that drive the checkpoint <-> agent auto-fix loop.
//!
//! A standalone checkpoint node runs a command in the upstream agent's
//! worktree. On failure the job blocks; instead of being a dead end (resolvable
//! only by the override-to-pass Confirm), the failure wakes the upstream agent
//! with the captured output. When the agent commits a fix and goes idle, the
//! re-arm pass in `reduce_dag` re-runs the checkpoint on the new worktree HEAD.
//!
//! The `checkpoint_runs` table is the source of truth for that loop: the row
//! count is the attempt number (a hard cap bounds flapping), the latest row's
//! commit SHA gates re-arming (only re-run when the agent actually committed
//! something new), and the latest row's output is what the wake message shows.
//! It lives here rather than on the seeded checkpoint artifact because re-arming
//! deletes that artifact (so the projection falls back through to Pending).

use cairn_common::ids;

use crate::execution::advancement::run_advancement_db;
use crate::execution::conditions::CheckpointRunOutput;
use crate::orchestrator::Orchestrator;
use crate::storage::RowExt;
use turso::params;

/// Hard cap on automatic checkpoint re-run iterations per job. A flapping
/// backstop: once this many command runs have been recorded for a checkpoint
/// job, the auto-loop stops waking the upstream agent and the gate stays Blocked
/// for manual resolution (Confirm override or the manual Re-run button).
pub const CHECKPOINT_MAX_ATTEMPTS: i64 = 5;

/// Max bytes of stdout/stderr retained per run (tail). Keeps the wake message
/// and stored history bounded while preserving the actionable end of the output.
const OUTPUT_TAIL_BYTES: usize = 8 * 1024;

/// A recorded checkpoint command run.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckpointRunRow {
    pub attempt: i64,
    pub command: Option<String>,
    pub commit_sha: Option<String>,
    pub exit_code: i32,
    pub passed: bool,
    pub stdout_tail: Option<String>,
    pub stderr_tail: Option<String>,
    pub ran_at: i64,
}

/// Keep the last `max_bytes` of `s`, on a char boundary, prefixing an ellipsis
/// marker when truncation occurred.
fn tail(s: &str, max_bytes: usize) -> String {
    if s.len() <= max_bytes {
        return s.to_string();
    }
    let mut start = s.len() - max_bytes;
    while start < s.len() && !s.is_char_boundary(start) {
        start += 1;
    }
    format!("…(truncated)\n{}", &s[start..])
}

/// Record one checkpoint command run. Returns the 1-based attempt number for
/// this job (existing row count + 1). Both passes and failures are recorded so
/// the attempt count reflects every command execution.
pub fn record_checkpoint_run(
    orch: &Orchestrator,
    job_id: &str,
    command: &str,
    commit_sha: Option<&str>,
    output: &CheckpointRunOutput,
) -> Result<i64, String> {
    let db = run_advancement_db({
        let dbs = orch.db.clone();
        let job_id = job_id.to_string();
        async move {
            crate::execution::routing::owning_db_for_job(&dbs, &job_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;
    let job_id = job_id.to_string();
    let command = command.to_string();
    let commit_sha = commit_sha.map(str::to_string);
    let stdout_tail = tail(&output.stdout, OUTPUT_TAIL_BYTES);
    let stderr_tail = tail(&output.stderr, OUTPUT_TAIL_BYTES);
    let exit_code = output.exit_code;
    let passed = output.passed;
    run_advancement_db(async move {
        db.write(|conn| {
            let job_id = job_id.clone();
            let command = command.clone();
            let commit_sha = commit_sha.clone();
            let stdout_tail = stdout_tail.clone();
            let stderr_tail = stderr_tail.clone();
            Box::pin(async move {
                let attempt = {
                    let mut rows = conn
                        .query(
                            "SELECT COUNT(*) FROM checkpoint_runs WHERE job_id = ?1",
                            (job_id.as_str(),),
                        )
                        .await?;
                    rows.next()
                        .await?
                        .map(|row| row.i64(0))
                        .transpose()?
                        .unwrap_or(0)
                        + 1
                };
                let id = ids::mint_child(job_id.as_str());
                let now = chrono::Utc::now().timestamp();
                conn.execute(
                    "INSERT INTO checkpoint_runs (
                        id, job_id, attempt, command, commit_sha, exit_code, passed,
                        stdout_tail, stderr_tail, ran_at
                     )
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                    params![
                        id.as_str(),
                        job_id.as_str(),
                        attempt,
                        command.as_str(),
                        commit_sha.as_deref(),
                        exit_code as i64,
                        if passed { 1_i64 } else { 0_i64 },
                        stdout_tail.as_str(),
                        stderr_tail.as_str(),
                        now,
                    ],
                )
                .await?;
                Ok(attempt)
            })
        })
        .await
        .map_err(|e| format!("Failed to record checkpoint run: {e}"))
    })
}

/// Number of recorded runs (= attempts) for a checkpoint job.
pub fn checkpoint_attempt_count(orch: &Orchestrator, job_id: &str) -> Result<i64, String> {
    let db = run_advancement_db({
        let dbs = orch.db.clone();
        let job_id = job_id.to_string();
        async move {
            crate::execution::routing::owning_db_for_job(&dbs, &job_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;
    let job_id = job_id.to_string();
    run_advancement_db(async move {
        db.read(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT COUNT(*) FROM checkpoint_runs WHERE job_id = ?1",
                        (job_id.as_str(),),
                    )
                    .await?;
                Ok(rows
                    .next()
                    .await?
                    .map(|row| row.i64(0))
                    .transpose()?
                    .unwrap_or(0))
            })
        })
        .await
        .map_err(|e| format!("Failed to count checkpoint runs: {e}"))
    })
}

/// The most recent recorded run for a checkpoint job, if any.
pub fn latest_checkpoint_run(
    orch: &Orchestrator,
    job_id: &str,
) -> Result<Option<CheckpointRunRow>, String> {
    let db = run_advancement_db({
        let dbs = orch.db.clone();
        let job_id = job_id.to_string();
        async move {
            crate::execution::routing::owning_db_for_job(&dbs, &job_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;
    let job_id = job_id.to_string();
    run_advancement_db(async move {
        db.read(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT attempt, command, commit_sha, exit_code, passed,
                                stdout_tail, stderr_tail, ran_at
                         FROM checkpoint_runs
                         WHERE job_id = ?1
                         ORDER BY ran_at DESC, attempt DESC
                         LIMIT 1",
                        (job_id.as_str(),),
                    )
                    .await?;
                rows.next()
                    .await?
                    .map(|row| {
                        Ok::<_, crate::storage::DbError>(CheckpointRunRow {
                            attempt: row.i64(0)?,
                            command: row.opt_text(1)?,
                            commit_sha: row.opt_text(2)?,
                            exit_code: row.i64(3)? as i32,
                            passed: row.i64(4)? != 0,
                            stdout_tail: row.opt_text(5)?,
                            stderr_tail: row.opt_text(6)?,
                            ran_at: row.i64(7)?,
                        })
                    })
                    .transpose()
            })
        })
        .await
        .map_err(|e| format!("Failed to load latest checkpoint run: {e}"))
    })
}

/// Delete all recorded runs for a checkpoint job. Used by the manual Re-run
/// button to start a fresh attempt cycle (resets the cap).
pub fn reset_checkpoint_runs(orch: &Orchestrator, job_id: &str) -> Result<(), String> {
    let db = run_advancement_db({
        let dbs = orch.db.clone();
        let job_id = job_id.to_string();
        async move {
            crate::execution::routing::owning_db_for_job(&dbs, &job_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;
    let job_id = job_id.to_string();
    run_advancement_db(async move {
        db.write(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                conn.execute(
                    "DELETE FROM checkpoint_runs WHERE job_id = ?1",
                    (job_id.as_str(),),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .map_err(|e| format!("Failed to reset checkpoint runs: {e}"))
    })
}

/// Whether a Blocked checkpoint job is eligible for an automatic re-run.
///
/// Re-run only when the upstream agent committed new work since the last run
/// (`head_sha != last_run_sha`); if the agent idled without committing, stay
/// Blocked. The upstream must not currently be running (let its turn finish),
/// and the attempt cap must not be exhausted. Missing SHAs (can't determine
/// progress) are treated as not eligible.
pub fn is_rearm_eligible(
    head_sha: Option<&str>,
    last_run_sha: Option<&str>,
    upstream_running: bool,
    attempts: i64,
    cap: i64,
) -> bool {
    if upstream_running || attempts >= cap {
        return false;
    }
    match (head_sha, last_run_sha) {
        (Some(head), Some(last)) => head != last,
        _ => false,
    }
}

/// Build the system message that wakes the upstream agent after a failed command
/// checkpoint. States the checkpoint name and command, the exit code, the output
/// tail, and that the checkpoint re-runs automatically once the agent commits a
/// fix and goes idle.
pub fn build_checkpoint_failure_message(
    node_name: &str,
    command: &str,
    exit_code: i32,
    stdout_tail: &str,
    stderr_tail: &str,
) -> String {
    let mut msg = String::new();
    msg.push_str(&format!(
        "The `{node_name}` checkpoint failed. It ran `{command}` in your worktree and the command exited with code {exit_code}.\n\n"
    ));
    let stdout_trimmed = stdout_tail.trim();
    if !stdout_trimmed.is_empty() {
        msg.push_str("## stdout\n\n```\n");
        msg.push_str(stdout_trimmed);
        msg.push_str("\n```\n\n");
    }
    let stderr_trimmed = stderr_tail.trim();
    if !stderr_trimmed.is_empty() {
        msg.push_str("## stderr\n\n```\n");
        msg.push_str(stderr_trimmed);
        msg.push_str("\n```\n\n");
    }
    msg.push_str(
        "Fix the underlying problem and commit your changes. When you finish and go idle, the checkpoint re-runs automatically against your updated worktree — no need to re-run the command yourself. If it passes, the workflow continues; if it fails again, you'll be woken with the new output.",
    );
    msg
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eligible_when_sha_changed_upstream_idle_under_cap() {
        assert!(is_rearm_eligible(
            Some("newsha"),
            Some("oldsha"),
            false,
            1,
            5
        ));
    }

    #[test]
    fn not_eligible_when_sha_unchanged() {
        // Agent idled without committing -> no progress -> stay Blocked.
        assert!(!is_rearm_eligible(
            Some("samesha"),
            Some("samesha"),
            false,
            1,
            5
        ));
    }

    #[test]
    fn not_eligible_when_upstream_running() {
        assert!(!is_rearm_eligible(
            Some("newsha"),
            Some("oldsha"),
            true,
            1,
            5
        ));
    }

    #[test]
    fn not_eligible_at_cap() {
        assert!(!is_rearm_eligible(
            Some("newsha"),
            Some("oldsha"),
            false,
            5,
            5
        ));
    }

    #[test]
    fn not_eligible_with_missing_sha() {
        assert!(!is_rearm_eligible(None, Some("oldsha"), false, 1, 5));
        assert!(!is_rearm_eligible(Some("newsha"), None, false, 1, 5));
    }

    #[test]
    fn failure_message_includes_command_exit_and_output() {
        let msg = build_checkpoint_failure_message(
            "CI",
            "bun run ci",
            1,
            "build output here",
            "error: test failed",
        );
        assert!(msg.contains("`CI`"));
        assert!(msg.contains("bun run ci"));
        assert!(msg.contains("code 1"));
        assert!(msg.contains("build output here"));
        assert!(msg.contains("error: test failed"));
        assert!(msg.contains("re-runs automatically"));
    }

    #[test]
    fn failure_message_omits_empty_output_sections() {
        let msg = build_checkpoint_failure_message("CI", "exit 1", 1, "", "");
        assert!(!msg.contains("## stdout"));
        assert!(!msg.contains("## stderr"));
    }

    #[test]
    fn tail_keeps_suffix_and_marks_truncation() {
        let s = "a".repeat(100);
        let t = tail(&s, 10);
        assert!(t.contains("truncated"));
        assert!(t.ends_with(&"a".repeat(10)));
    }

    #[test]
    fn tail_passthrough_when_short() {
        assert_eq!(tail("short", 100), "short");
    }
}
