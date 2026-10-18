//! What each machine's recent completions of a command say about whether it
//! will answer, and the price that puts on placing the command there.
//!
//! Duration profiles learn only from work that succeeded (a lane that died 30
//! minutes in is not a sample of how long the lane takes), so without this a
//! machine whose lanes keep stalling the same way looks exactly as good as one
//! that answers, and keeps being chosen. This module remembers those deaths
//! per machine and turns a repeated one into milliseconds the ranking adds to
//! that machine's time to a verdict.
//!
//! It prices and never excludes, per the scheduling canon that a measurement
//! prices placement and does not gate work: a penalized machine still wins
//! when it is the only candidate, or when everything else is further behind
//! than its penalty. The penalty retires on its own as successes enter the
//! window or the failures age past the horizon.

use crate::storage::LocalDb;
use cairn_common::executor_protocol::{CommandResourceIdentity, CommandStop, ReliabilityEstimate};
use cairn_db::turso::params;
use std::sync::Arc;

/// How many of a machine's newest outcomes of a command are read.
pub(super) const RELIABILITY_WINDOW: usize = 8;
/// How old an outcome may be and still count.
pub(super) const RELIABILITY_HORIZON_MS: u64 = 72 * 60 * 60 * 1_000;
/// How long a row is kept at all. Longer than the horizon only so a reader of
/// the table can see what aged out.
const RETENTION_MS: u64 = 7 * 24 * 60 * 60 * 1_000;
/// Fewer outcomes than this in the window is no evidence about the machine.
const MIN_OUTCOMES: usize = 2;
/// One failure is a stumble; the same failure twice is a pattern.
const MIN_FAILURES: u32 = 2;
/// The failure probability is capped so the expected number of failed
/// attempts, `p/(1-p)`, stays finite and bounded at three.
const MAX_FAILURE_PROBABILITY_MILLIS: u64 = 750;

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

/// One outcome in a machine's window: the signature it failed with, if it
/// failed, and how long it ran.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Recent {
    failed_signature: Option<String>,
    duration_ms: Option<u64>,
}

/// The penalty one window earns: the expected cost of the failed attempts
/// before a success, `p/(1-p)` times how long a failure runs, where `p` is the
/// share of the window failing with its most frequent signature.
#[derive(Debug, Clone, PartialEq, Eq)]
struct WindowPrice {
    signature: String,
    failures: u32,
    of: u32,
    mean_failure_ms: u64,
    penalty_ms: u64,
}

fn price_window(window: &[Recent]) -> Option<WindowPrice> {
    if window.len() < MIN_OUTCOMES {
        return None;
    }
    let mut counts: std::collections::BTreeMap<&str, u32> = std::collections::BTreeMap::new();
    for signature in window
        .iter()
        .filter_map(|recent| recent.failed_signature.as_deref())
    {
        *counts.entry(signature).or_default() += 1;
    }
    // Most frequent, and the alphabetically first among equals so the same
    // window always names the same failure.
    let (signature, failures) =
        counts
            .into_iter()
            .max_by(|(left_sig, left), (right_sig, right)| {
                left.cmp(right).then_with(|| right_sig.cmp(left_sig))
            })?;
    if failures < MIN_FAILURES {
        return None;
    }
    let durations: Vec<u64> = window
        .iter()
        .filter(|recent| recent.failed_signature.as_deref() == Some(signature))
        .filter_map(|recent| recent.duration_ms)
        .collect();
    if durations.is_empty() {
        return None;
    }
    let mean_failure_ms = durations.iter().sum::<u64>() / durations.len() as u64;
    let of = window.len() as u32;
    let probability_millis =
        (u64::from(failures) * 1_000 / u64::from(of)).min(MAX_FAILURE_PROBABILITY_MILLIS);
    let penalty_ms =
        mean_failure_ms.saturating_mul(probability_millis) / (1_000 - probability_millis);
    (penalty_ms > 0).then(|| WindowPrice {
        signature: signature.to_string(),
        failures,
        of,
        mean_failure_ms,
        penalty_ms,
    })
}

async fn load_window(
    db: Arc<LocalDb>,
    identity: &CommandResourceIdentity,
    executor_id: &str,
    now_unix_ms: u64,
) -> Result<Vec<Recent>, String> {
    let identity = identity.clone();
    let executor_id = executor_id.to_string();
    db.read(move |conn| {
        let identity = identity.clone();
        let executor_id = executor_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT outcome, COALESCE(signature, ''), COALESCE(duration_ms, -1)
                       FROM command_outcome_history
                      WHERE identity_version = ?1 AND command_identity = ?2
                        AND executor_id = ?3 AND finished_at_unix_ms >= ?4
                      ORDER BY finished_at_unix_ms DESC, rowid DESC
                      LIMIT ?5",
                    params![
                        identity.version as i64,
                        identity.key,
                        executor_id,
                        now_unix_ms.saturating_sub(RELIABILITY_HORIZON_MS) as i64,
                        RELIABILITY_WINDOW as i64
                    ],
                )
                .await?;
            let mut window = Vec::new();
            while let Some(row) = rows.next().await? {
                let outcome = row.get::<String>(0)?;
                let signature = row.get::<String>(1)?;
                let duration = row.get::<i64>(2)?;
                window.push(Recent {
                    failed_signature: (outcome != "completed" && !signature.is_empty())
                        .then_some(signature),
                    duration_ms: (duration >= 0).then_some(duration as u64),
                });
            }
            Ok(window)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// What `executor_id`'s own recent history of these commands adds to its time
/// to a verdict, or `None` when it adds nothing.
///
/// A batch is priced as the sum of its items, the way its duration is, and is
/// named by the item whose failures cost the most. A machine with too few
/// outcomes carries no penalty, and never inherits a sibling's: an estimate
/// borrowed by platform would charge an unmeasured machine for another's
/// stalls.
pub(super) async fn resolve(
    db: Arc<LocalDb>,
    identities: &[&CommandResourceIdentity],
    executor_id: &str,
    executor_name: &str,
    now_unix_ms: u64,
) -> Option<ReliabilityEstimate> {
    let mut total_penalty_ms = 0u64;
    let mut worst: Option<WindowPrice> = None;
    for identity in identities {
        let Ok(window) = load_window(db.clone(), identity, executor_id, now_unix_ms).await else {
            continue;
        };
        let Some(price) = price_window(&window) else {
            continue;
        };
        total_penalty_ms = total_penalty_ms.saturating_add(price.penalty_ms);
        if worst
            .as_ref()
            .is_none_or(|worst| price.penalty_ms > worst.penalty_ms)
        {
            worst = Some(price);
        }
    }
    let worst = worst?;
    Some(ReliabilityEstimate {
        executor_name: executor_name.to_string(),
        signature: worst.signature,
        failures: worst.failures,
        of: worst.of,
        mean_failure_ms: worst.mean_failure_ms,
        penalty_ms: total_penalty_ms,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINUTE: u64 = 60_000;

    fn identity() -> CommandResourceIdentity {
        CommandResourceIdentity {
            version: 1,
            key: "rust-tests".into(),
        }
    }

    fn silent() -> CommandStop {
        CommandStop::Silent {
            quiet_ms: 30 * MINUTE,
            last_output_unix_ms: None,
            cpu_ms_in_window: None,
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

    fn stall(minutes: u64) -> Recent {
        Recent {
            failed_signature: Some("silent_stop".into()),
            duration_ms: Some(minutes * MINUTE),
        }
    }

    fn pass() -> Recent {
        Recent {
            failed_signature: None,
            duration_ms: Some(10 * MINUTE),
        }
    }

    #[test]
    fn a_single_failure_prices_nothing() {
        assert_eq!(price_window(&[stall(30), pass(), pass()]), None);
        assert_eq!(
            price_window(&[stall(30)]),
            None,
            "one outcome is no evidence"
        );
    }

    #[test]
    fn two_of_four_same_signature_failures_price_one_failed_attempt() {
        let price = price_window(&[stall(30), pass(), stall(30), pass()]).unwrap();
        assert_eq!(price.signature, "silent_stop");
        assert_eq!((price.failures, price.of), (2, 4));
        assert_eq!(price.mean_failure_ms, 30 * MINUTE);
        // p = 1/2, so p/(1-p) = 1 expected failed attempt of 30 minutes.
        assert_eq!(price.penalty_ms, 30 * MINUTE);
    }

    #[test]
    fn the_failure_probability_is_capped() {
        let price = price_window(&[stall(30), stall(30), stall(30), stall(30)]).unwrap();
        // p is capped at 3/4, so at most three failed attempts are charged.
        assert_eq!(price.penalty_ms, 90 * MINUTE);
    }

    #[test]
    fn different_failures_do_not_add_up_to_a_pattern() {
        let timed_out = Recent {
            failed_signature: Some("timed_out".into()),
            duration_ms: Some(30 * MINUTE),
        };
        assert_eq!(price_window(&[stall(30), timed_out, pass()]), None);
    }

    async fn record_at(
        db: &Arc<LocalDb>,
        executor_id: &str,
        outcome: CommandOutcome,
        finished: u64,
    ) {
        record(
            db.clone(),
            &identity(),
            "macos",
            executor_id,
            &outcome,
            Some(30 * MINUTE),
            finished,
        )
        .await
        .unwrap();
    }

    /// The estimate is the machine's own: a second macOS machine with no
    /// outcomes in the window carries no penalty for its sibling's stalls.
    #[tokio::test]
    async fn a_machine_is_priced_by_its_own_history_only() {
        let db = Arc::new(crate::storage::migrated_test_db("reliability-own.db").await);
        let now = 10 * RELIABILITY_HORIZON_MS;
        for (offset, outcome) in [
            CommandOutcome::Stalled,
            CommandOutcome::Completed,
            CommandOutcome::Stalled,
            CommandOutcome::Completed,
        ]
        .into_iter()
        .enumerate()
        {
            record_at(&db, "mac-a", outcome, now - 1_000 + offset as u64).await;
        }
        let priced = resolve(db.clone(), &[&identity()], "mac-a", "local", now)
            .await
            .unwrap();
        assert_eq!(priced.penalty_ms, 30 * MINUTE);
        assert_eq!(
            priced.describe(),
            "local +30m — 2 of 4 recent lanes of this command on local ended silent_stop (mean 30m)"
        );
        assert_eq!(
            resolve(db, &[&identity()], "mac-b", "mac-b", now).await,
            None,
            "an unmeasured machine never inherits a sibling's stalls"
        );
    }

    /// Successes entering the window, and failures ageing past the horizon,
    /// each retire the penalty.
    #[tokio::test]
    async fn the_window_and_the_horizon_retire_the_penalty() {
        let db = Arc::new(crate::storage::migrated_test_db("reliability-retire.db").await);
        let now = 10 * RELIABILITY_HORIZON_MS;
        record_at(&db, "mac", CommandOutcome::Stalled, now - 3_000).await;
        record_at(&db, "mac", CommandOutcome::Stalled, now - 2_000).await;
        assert!(resolve(db.clone(), &[&identity()], "mac", "mac", now)
            .await
            .is_some());
        assert_eq!(
            resolve(
                db.clone(),
                &[&identity()],
                "mac",
                "mac",
                now + RELIABILITY_HORIZON_MS
            )
            .await,
            None,
            "stalls older than the horizon no longer count"
        );
        for offset in 0..RELIABILITY_WINDOW as u64 {
            record_at(&db, "mac", CommandOutcome::Completed, now - 1_000 + offset).await;
        }
        assert_eq!(
            resolve(db, &[&identity()], "mac", "mac", now + 1_000).await,
            None,
            "a full window of successes pushes the stalls out"
        );
    }

    #[tokio::test]
    async fn a_batch_is_priced_as_the_sum_of_its_items() {
        let db = Arc::new(crate::storage::migrated_test_db("reliability-batch.db").await);
        let now = 10 * RELIABILITY_HORIZON_MS;
        let other = CommandResourceIdentity {
            version: 1,
            key: "frontend-tests".into(),
        };
        for identity in [identity(), other.clone()] {
            for offset in 0..2 {
                record(
                    db.clone(),
                    &identity,
                    "macos",
                    "mac",
                    &CommandOutcome::Stalled,
                    Some(30 * MINUTE),
                    now - 1_000 + offset,
                )
                .await
                .unwrap();
            }
        }
        let priced = resolve(db, &[&identity(), &other], "mac", "mac", now)
            .await
            .unwrap();
        // Each item alone is capped at three failed attempts of 30 minutes.
        assert_eq!(priced.penalty_ms, 2 * 90 * MINUTE);
    }
}
