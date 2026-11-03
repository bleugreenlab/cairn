//! A machine that did not return from a self-update.
//!
//! The rolling self-update ([`super::rollout`]) directs an emptied machine to
//! install the build this runner serves and awaits its return. Past
//! [`super::rollout::RETURN_BUDGET_MS`] it concludes the update stranded the
//! machine and halts that build. That conclusion is an incident (an artifact
//! the machine cannot run or fetch leaves it dark), and this module is what
//! makes it one rather than a journal row nobody reads:
//!
//! - **The page names it.** The machine's enrolment record carries an
//!   [`UnreturnedUpdate`] from the conclusion until the machine attaches, and
//!   [`describe`] is the one sentence every surface says it in, distinct from
//!   the executor's own parting `executor updating to build …` (true for the
//!   seconds of a restart) and from an ordinary absence (a sleeping laptop).
//! - **The runner tries its bootstrap once.** The conclusion is queued as an
//!   [`UpdateRecovery`] for whatever holds the machine's ssh bootstrap, which
//!   restarts the daemon once, bounded, and records what it found. One attempt
//!   per directed build: the rollout halts the build at the conclusion, so the
//!   machine is never directed there or concluded again, and after the attempt
//!   it is the operator's to bring back.
//! - **The fleet's owner hears.** The outcome is minted as one
//!   [`EXECUTOR_UPDATE_UNRETURNED`] fact per machine and build, so a thread
//!   holding a standing trigger on that source is woken once per stranding,
//!   with the build, when it was directed, when the machine was last seen, and
//!   what the restart found.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use cairn_common::executor_protocol::{UnreturnedUpdate, UpdateRestart};
use serde_json::Value;

use crate::orchestrator::Orchestrator;

/// The fact source a stranded self-update is minted on.
pub const EXECUTOR_UPDATE_UNRETURNED: &str = "executor_update_unreturned";

/// One stranding, as the holder of the machine's bootstrap needs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateRecovery {
    pub executor_id: String,
    /// The machine's public name.
    pub name: String,
    pub last_seen_unix_ms: Option<u64>,
    /// The conclusion; its `restart` is filled in by whoever attempts one.
    pub update: UnreturnedUpdate,
}

/// Strandings waiting for the bootstrap holder, and the signal that one
/// arrived. A runner with no bootstrap holder leaves them queued, which costs
/// one entry per machine and build.
#[derive(Debug, Default)]
pub(crate) struct UpdateRecoveries {
    pending: Mutex<Vec<UpdateRecovery>>,
    arrived: Arc<tokio::sync::Notify>,
}

impl UpdateRecoveries {
    pub(crate) fn push(&self, recovery: UpdateRecovery) {
        self.pending.lock().unwrap().push(recovery);
        // `notify_one` keeps a permit when nobody waits yet, so a stranding
        // concluded between two of the holder's drains is not missed.
        self.arrived.notify_one();
    }

    pub(crate) fn take(&self) -> Vec<UpdateRecovery> {
        std::mem::take(&mut *self.pending.lock().unwrap())
    }

    pub(crate) fn arrived(&self) -> Arc<tokio::sync::Notify> {
        self.arrived.clone()
    }
}

/// A span as `cairn://executors` words an age: milliseconds, seconds, minutes,
/// then hours, each followed by `ago`.
fn ago(ms: u64) -> String {
    let seconds = ms / 1_000;
    let minutes = seconds / 60;
    if ms < 1_000 {
        format!("{ms}ms ago")
    } else if seconds < 60 {
        format!("{seconds}s ago")
    } else if minutes < 60 {
        format!("{minutes}m ago")
    } else {
        format!("{}h ago", minutes / 60)
    }
}

/// A unix-millisecond instant as a UTC wall-clock time, to the minute.
fn clock(unix_ms: u64) -> String {
    i64::try_from(unix_ms)
        .ok()
        .and_then(chrono::DateTime::from_timestamp_millis)
        .map(|at| at.format("%Y-%m-%d %H:%M UTC").to_string())
        .unwrap_or_else(|| format!("{unix_ms} ms"))
}

/// The condition in one line, the same on every surface that names it:
/// `did not return from the self-update to <build> directed at <time>; last
/// seen <age> ago`, then the restart's outcome once one was attempted.
pub fn describe(
    update: &UnreturnedUpdate,
    last_seen_unix_ms: Option<u64>,
    now_unix_ms: u64,
) -> String {
    let seen = match last_seen_unix_ms {
        Some(seen) => format!("last seen {}", ago(now_unix_ms.saturating_sub(seen))),
        None => "not seen since this runner started".to_string(),
    };
    let mut line = format!(
        "did not return from the self-update to {} directed at {}; {seen}",
        update.build_id,
        clock(update.directed_at_unix_ms)
    );
    if let Some(restart) = &update.restart {
        line.push_str("; ");
        line.push_str(&restart_text(restart));
    }
    line
}

/// `restart attempted at <time>: <outcome>`, or that it is still running.
pub fn restart_text(restart: &UpdateRestart) -> String {
    format!(
        "restart attempted at {}: {}",
        clock(restart.attempted_at_unix_ms),
        restart.outcome.as_deref().unwrap_or("in progress")
    )
}

/// Mint the stranding's fact once its restart has an outcome (or the runner
/// has none to attempt). Unique per machine and build, so a redelivery is
/// absorbed and the owning thread is woken once per stranding.
pub async fn mint_unreturned(
    orch: &Orchestrator,
    recovery: &UpdateRecovery,
) -> Result<Option<i64>, String> {
    let update = &recovery.update;
    let now = crate::fleet::unix_time_ms();
    let mut fields = BTreeMap::from([
        ("executor".to_string(), Value::String(recovery.name.clone())),
        (
            "executorId".to_string(),
            Value::String(recovery.executor_id.clone()),
        ),
        ("build".to_string(), Value::String(update.build_id.clone())),
        (
            "directedAt".to_string(),
            Value::String(clock(update.directed_at_unix_ms)),
        ),
        (
            "detailUri".to_string(),
            Value::String(format!("cairn://executors/{}", recovery.name)),
        ),
    ]);
    if let Some(seen) = recovery.last_seen_unix_ms {
        fields.insert("lastSeen".into(), Value::String(clock(seen)));
    }
    if let Some(restart) = &update.restart {
        fields.insert("restart".into(), Value::String(restart_text(restart)));
    }
    crate::routes::mint(
        orch,
        crate::routes::Mint {
            fact: crate::routes::RouteFact {
                source: EXECUTOR_UPDATE_UNRETURNED.to_string(),
                identity: format!("{}:{}", recovery.executor_id, update.build_id),
                fields,
                origin: crate::routes::installation_machine_origin(orch).ok(),
                summary: Some(format!(
                    "{} {}",
                    recovery.name,
                    describe(update, recovery.last_seen_unix_ms, now)
                )),
                route_provenance: None,
            },
            record_ref: format!("cairn://executors/{}", recovery.name),
            record_id: Some(recovery.executor_id.clone()),
            project_id: None,
            presence: crate::routes::log::operator_presence().await,
            actor_job_id: None,
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 2026-11-03 02:51:55 UTC, the bgsrv-win directive.
    const DIRECTED: u64 = 1_793_674_315_000;

    fn unreturned(restart: Option<UpdateRestart>) -> UnreturnedUpdate {
        UnreturnedUpdate {
            build_id: "content-v2:3ee6fabd".into(),
            directed_at_unix_ms: DIRECTED,
            concluded_at_unix_ms: DIRECTED + 25 * 60_000,
            restart,
        }
    }

    #[test]
    fn the_sentence_names_the_build_the_directive_and_the_last_sighting() {
        let now = DIRECTED + 3 * 60 * 60_000 + 30 * 60_000;
        assert_eq!(
            describe(&unreturned(None), Some(DIRECTED), now),
            "did not return from the self-update to content-v2:3ee6fabd directed at 2026-11-03 02:51 UTC; last seen 3h ago"
        );
        assert!(!describe(&unreturned(None), Some(DIRECTED), now).contains("updating"));
    }

    #[test]
    fn the_restart_rides_on_the_sentence_running_and_finished() {
        let now = DIRECTED + 30 * 60_000;
        let attempted = DIRECTED + 25 * 60_000 + 30_000;
        let running = describe(
            &unreturned(Some(UpdateRestart {
                attempted_at_unix_ms: attempted,
                outcome: None,
            })),
            Some(DIRECTED),
            now,
        );
        assert!(
            running.ends_with("; restart attempted at 2026-11-03 03:17 UTC: in progress"),
            "{running}"
        );
        let done = describe(
            &unreturned(Some(UpdateRestart {
                attempted_at_unix_ms: attempted,
                outcome: Some("ssh could not reach the host".into()),
            })),
            Some(DIRECTED),
            now,
        );
        assert!(
            done.ends_with(
                "restart attempted at 2026-11-03 03:17 UTC: ssh could not reach the host"
            ),
            "{done}"
        );
    }

    #[test]
    fn a_queued_stranding_survives_until_taken_and_signals_a_late_waiter() {
        let recoveries = UpdateRecoveries::default();
        let recovery = UpdateRecovery {
            executor_id: "id".into(),
            name: "bgsrv-win".into(),
            last_seen_unix_ms: Some(DIRECTED),
            update: unreturned(None),
        };
        recoveries.push(recovery.clone());
        let arrived = recoveries.arrived();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        runtime.block_on(async {
            tokio::time::timeout(std::time::Duration::from_secs(1), arrived.notified())
                .await
                .expect("the permit was kept for a waiter that came later");
        });
        assert_eq!(recoveries.take(), [recovery]);
        assert!(recoveries.take().is_empty());
    }
}
