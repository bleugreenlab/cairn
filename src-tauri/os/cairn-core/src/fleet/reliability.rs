//! How each real completion of a command ended on each machine: completed,
//! stalled, timed out or killed.
//!
//! Duration profiles learn only from work that succeeded (a lane that died 30
//! minutes in is not a sample of how long the lane takes), so this is the one
//! record of a machine whose lanes keep dying the same way. Placement does not
//! read it: the runner assigns work to the first machine with room, and ranks
//! nothing.

use crate::storage::LocalDb;
use cairn_common::executor_protocol::{CommandResourceIdentity, CommandStop};
use cairn_db::turso::params;
use std::sync::Arc;

/// How long a row is kept at all. Longer than the horizon only so a reader of
/// the table can see what aged out.
const RETENTION_MS: u64 = 7 * 24 * 60 * 60 * 1_000;

/// How one real completion ended, as far as the machine's reliability goes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum CommandOutcome {
    /// The command ended itself with an exit code, red or green. A red is a
    /// verdict about the code, not about the machine.
    Completed,
    /// The executor's silence stop ended it.
    Stalled,
    /// It ran past its budget.
    TimedOut,
    /// A signal ended it: a kernel OOM kill, an external SIGKILL. Not a
    /// verdict about the code, and not evidence the machine works either.
    Killed { signal: Option<i32> },
}

/// The signal the kernel's out-of-memory killer sends, on every unix.
const SIGKILL: i32 = 9;

impl CommandOutcome {
    /// Classify a completion, or `None` when it says nothing about the machine:
    /// a cancelled command was withdrawn, and a lane that selected nothing did
    /// no work.
    ///
    /// A signal death arrives two ways. The executor reports no exit code when
    /// the command's own process died by signal. A `bun run`/cargo wrapper
    /// whose child was killed exits 128 + the signal instead, and 137 (SIGKILL)
    /// and 143 (SIGTERM) are the two a reaper sends.
    pub(super) fn classify(
        exit_code: Option<i32>,
        timed_out: bool,
        stop: Option<&CommandStop>,
        selected_nothing: bool,
    ) -> Option<Self> {
        match stop {
            Some(CommandStop::Cancelled) => return None,
            Some(CommandStop::Silent { .. }) => return Some(Self::Stalled),
            // The kernel SIGKILLed part of the tree for want of memory on this
            // machine: the same signal death a wrapper reports as 137, so the
            // same signature, whatever the leader then exited with.
            Some(CommandStop::OomKilled { .. }) => {
                return Some(Self::Killed {
                    signal: Some(SIGKILL),
                })
            }
            None => {}
        }
        if timed_out {
            return Some(Self::TimedOut);
        }
        match exit_code {
            None => Some(Self::Killed { signal: None }),
            Some(code @ (137 | 143)) => Some(Self::Killed {
                signal: Some(code - 128),
            }),
            Some(0) if selected_nothing => None,
            Some(_) => Some(Self::Completed),
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Stalled => "stalled",
            Self::TimedOut => "timed_out",
            Self::Killed { .. } => "killed",
        }
    }

    /// The name two failures must share to count as the same failure.
    fn signature(&self) -> Option<String> {
        match self {
            Self::Completed => None,
            Self::Stalled => Some("silent_stop".into()),
            Self::TimedOut => Some("timed_out".into()),
            Self::Killed {
                signal: Some(signal),
            } => Some(format!("signal:{signal}")),
            Self::Killed { signal: None } => Some("signal".into()),
        }
    }
}

/// Record one completion of `identity` on `executor_id`, pruning rows past
/// retention in the same write.
pub(super) async fn record(
    db: Arc<LocalDb>,
    identity: &CommandResourceIdentity,
    os: &str,
    executor_id: &str,
    outcome: &CommandOutcome,
    duration_ms: Option<u64>,
    finished_at_unix_ms: u64,
) -> Result<(), String> {
    let _written = super::profile_reads::Writing::apart_from_needs(&db);
    let identity = identity.clone();
    let os = os.to_string();
    let executor_id = executor_id.to_string();
    let outcome_name = outcome.as_str();
    let signature = outcome.signature();
    db.write(move |conn| {
        let identity = identity.clone();
        let os = os.clone();
        let executor_id = executor_id.clone();
        let signature = signature.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO command_outcome_history
                   (identity_version, command_identity, os, executor_id, outcome, signature,
                    duration_ms, finished_at_unix_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    identity.version as i64,
                    identity.key,
                    os,
                    executor_id,
                    outcome_name,
                    signature,
                    duration_ms.map(|ms| ms as i64),
                    finished_at_unix_ms as i64
                ],
            )
            .await?;
            conn.execute(
                "DELETE FROM command_outcome_history WHERE finished_at_unix_ms < ?1",
                params![finished_at_unix_ms.saturating_sub(RETENTION_MS) as i64],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINUTE: u64 = 60_000;

    fn silent() -> CommandStop {
        CommandStop::Silent {
            quiet_ms: 30 * MINUTE,
            last_output_unix_ms: None,
            cpu_ms_in_window: None,
            storage_io_bytes_in_window: None,
            tree: Vec::new(),
        }
    }

    #[test]
    fn a_completion_is_classified_by_what_ended_it() {
        assert_eq!(
            CommandOutcome::classify(Some(1), false, Some(&silent()), false),
            Some(CommandOutcome::Stalled)
        );
        assert_eq!(
            CommandOutcome::classify(Some(1), false, None, false),
            Some(CommandOutcome::Completed),
            "a red is a verdict about the code, not the machine"
        );
        assert_eq!(
            CommandOutcome::classify(Some(0), false, None, false),
            Some(CommandOutcome::Completed)
        );
        assert_eq!(
            CommandOutcome::classify(Some(0), false, None, true),
            None,
            "a lane that selected nothing did no work"
        );
        assert_eq!(
            CommandOutcome::classify(None, true, None, false),
            Some(CommandOutcome::TimedOut)
        );
        assert_eq!(
            CommandOutcome::classify(None, false, Some(&CommandStop::Cancelled), false),
            None
        );
    }

    /// A kernel OOM kill is not a success vouching for the machine.
    #[test]
    fn a_signal_death_is_killed_never_completed() {
        assert_eq!(
            CommandOutcome::classify(None, false, None, false),
            Some(CommandOutcome::Killed { signal: None })
        );
        let killed = CommandOutcome::classify(Some(137), false, None, false).unwrap();
        assert_eq!(killed, CommandOutcome::Killed { signal: Some(9) });
        assert_eq!(killed.signature().as_deref(), Some("signal:9"));
        assert_eq!(
            CommandOutcome::classify(Some(143), false, None, false),
            Some(CommandOutcome::Killed { signal: Some(15) })
        );
    }
}
