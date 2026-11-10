//! The host's half of Codex home retention
//! (`cairn_models::session::codex::retention`): Cairn's record of when the
//! session bound to each Codex thread was last active, and the schedule passes
//! run on. Only Cairn's own home, `~/.cairn/codex`, is pruned; the user's
//! `~/.codex` is never opened.

use super::cairn_codex_home;
use crate::orchestrator::Orchestrator;
use crate::storage::{DbResult, LocalDb, RowExt};
use cairn_db::turso::params;
use cairn_models::session::codex::retention::{
    prune_codex_home, unix_now, RetentionReport, RetentionStop,
};
use std::collections::HashMap;
use std::time::Duration;

/// How long one pass may spend deleting. Codex's `thread/delete` rescans every
/// rollout's header, about two seconds a thread on a home of 9,500 rollouts,
/// so a large backlog is worked off over several passes, each cheaper than the
/// last.
const PASS_BUDGET: Duration = Duration::from_secs(10 * 60);

/// The wait after a pass its budget cut short.
const BACKLOG_INTERVAL: Duration = Duration::from_secs(30 * 60);

/// The wait after a pass that finished: a day of sessions ages past the
/// window a few dozen threads at a time.
const STEADY_INTERVAL: Duration = Duration::from_secs(6 * 60 * 60);

/// Every Codex thread a Cairn session is bound to, with the unix seconds the
/// session was last active: its own last update or close, or its runs' latest
/// update, a live run counting as `now`. Not filtered by backend: a thread id
/// is a UUID no other backend's session id shares, and a session that is
/// counted needlessly only keeps a thread longer.
pub(crate) async fn thread_activity(db: &LocalDb, now: i64) -> DbResult<Vec<(String, i64)>> {
    db.query_all(
        "SELECT s.backend_id,
                MAX(s.updated_at, COALESCE(s.closed_at, 0),
                    COALESCE((SELECT MAX(CASE WHEN r.status = 'live' THEN ?1 ELSE r.updated_at END)
                              FROM runs r WHERE r.session_id = s.id), 0))
         FROM sessions s
         WHERE s.backend_id IS NOT NULL",
        params![now],
        |row| Ok((row.text(0)?, row.i64(1)?)),
    )
    .await
}

impl Orchestrator {
    /// Prune Cairn's Codex home now and then on a schedule: again after
    /// [`BACKLOG_INTERVAL`] while a pass ends on its budget, otherwise after
    /// [`STEADY_INTERVAL`].
    pub fn spawn_codex_home_retention(&self) {
        let orch = self.clone();
        tokio::spawn(async move {
            loop {
                let backlog = orch
                    .run_codex_home_retention()
                    .await
                    .is_some_and(|report| report.stopped == Some(RetentionStop::Budget));
                tokio::time::sleep(if backlog {
                    BACKLOG_INTERVAL
                } else {
                    STEADY_INTERVAL
                })
                .await;
            }
        });
    }

    /// One pass, or `None` when none could run: Codex is not installed, or
    /// Cairn's session record could not be read, without which nothing is
    /// deleted.
    async fn run_codex_home_retention(&self) -> Option<RetentionReport> {
        let Some(installed) = self.models_runtime().managed.codex.installed() else {
            log::debug!("Codex home retention skipped: Codex is not installed");
            return None;
        };
        let now = unix_now();
        let mut activity: HashMap<String, i64> = HashMap::new();
        for db in self.db.all_dbs().await {
            match thread_activity(&db, now).await {
                Ok(rows) => {
                    for (thread_id, active) in rows {
                        let latest = activity.entry(thread_id).or_insert(active);
                        *latest = (*latest).max(active);
                    }
                }
                Err(error) => {
                    log::warn!(
                        "Codex home retention skipped: Cairn's session record could not be read: {error}"
                    );
                    return None;
                }
            }
        }
        let codex = installed.path.to_string_lossy().into_owned();
        let home = cairn_codex_home();
        let report = tokio::task::spawn_blocking(move || {
            prune_codex_home(&codex, &home, &activity, now, PASS_BUDGET)
        })
        .await
        .map_err(|error| log::error!("Codex home retention pass ended abnormally: {error}"))
        .ok()?;
        log_report(&report);
        Some(report)
    }
}

fn mib(bytes: u64) -> String {
    format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0))
}

fn log_report(report: &RetentionReport) {
    let summary = format!(
        "Codex home retention: {} threads ({}) scanned; kept {} with an active session and {} written recently; \
         {} idle ({}): deleted {}, already gone {}, refused {}, not reached {}; reclaimed {} in {:.1}s",
        report.scanned_threads,
        mib(report.scanned_bytes),
        report.kept_active,
        report.kept_recent,
        report.candidates,
        mib(report.candidate_bytes),
        report.deleted,
        report.already_gone,
        report.refused,
        report.remaining,
        mib(report.reclaimed_bytes),
        report.elapsed.as_secs_f64(),
    );
    match &report.stopped {
        None => log::info!("{summary}"),
        Some(RetentionStop::Budget) => {
            log::info!("{summary}; budget spent, the rest follows in the next pass")
        }
        Some(RetentionStop::Refusals(count)) => log::warn!(
            "{summary}; stopped after {count} refusals in a row: {:?}",
            report.refusal_samples
        ),
        Some(RetentionStop::Server(reason)) => log::warn!("{summary}; stopped: {reason}"),
    }
    if report.refused > 0 && !matches!(report.stopped, Some(RetentionStop::Refusals(_))) {
        log::info!(
            "Codex home retention refusals: {:?}",
            report.refusal_samples
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn session(db: &LocalDb, id: &str, thread: &str, updated: i64, closed: Option<i64>) {
        db.execute(
            "INSERT INTO sessions (id, chat_id, backend, status, created_at, updated_at, closed_at, backend_id)
             VALUES (?1, 'chat-1', 'codex', ?2, ?3, ?3, ?4, ?5)",
            params![
                id,
                if closed.is_some() { "closed" } else { "open" },
                updated,
                closed,
                thread
            ],
        )
        .await
        .unwrap();
    }

    async fn run(db: &LocalDb, id: &str, session: &str, status: &str, updated: i64) {
        db.execute(
            "INSERT INTO runs (id, chat_id, status, session_id, created_at, updated_at)
             VALUES (?1, 'chat-1', ?2, ?3, ?4, ?4)",
            params![id, status, session, updated],
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn a_thread_is_as_active_as_its_session_and_runs_and_a_live_run_is_now() {
        let db = crate::storage::migrated_test_db("codex-retention-activity").await;
        let now = 1_800_000_000;
        session(&db, "s-closed", "thread-closed", 100, Some(200)).await;
        session(&db, "s-ran", "thread-ran", 100, None).await;
        run(&db, "r-ran", "s-ran", "exited", 500).await;
        session(&db, "s-live", "thread-live", 100, None).await;
        run(&db, "r-live-old", "s-live", "exited", 300).await;
        run(&db, "r-live", "s-live", "live", 400).await;
        db.execute(
            "INSERT INTO sessions (id, chat_id, backend, status, created_at, updated_at)
             VALUES ('s-unbound', 'chat-1', 'codex', 'open', 1, 1)",
            (),
        )
        .await
        .unwrap();

        let mut activity = thread_activity(&db, now).await.unwrap();
        activity.sort();
        assert_eq!(
            activity,
            [
                ("thread-closed".to_string(), 200),
                ("thread-live".to_string(), now),
                ("thread-ran".to_string(), 500),
            ]
        );
    }
}
