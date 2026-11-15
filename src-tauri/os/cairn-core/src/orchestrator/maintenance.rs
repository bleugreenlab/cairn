//! The orchestrator's long-running maintenance loops. Each pass's work lives
//! with its subject (`backends::codex::retention`, `vcs_maintenance`); this
//! file only schedules them on the orchestrator.

use std::collections::HashMap;

use cairn_models::session::codex::retention::{
    prune_codex_home, unix_now, RetentionReport, RetentionStop,
};

use super::Orchestrator;
use crate::backends::codex::cairn_codex_home;
use crate::backends::codex::retention::{
    log_report, thread_activity, BACKLOG_INTERVAL, PASS_BUDGET, STEADY_INTERVAL,
};
use crate::vcs_maintenance::{run_pass, MEASURE_INTERVAL};

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

    /// Measure every project's ref store hourly and prune the enabled ones
    /// daily. Each pass is its own task, so a panic in one is logged and the
    /// next hour's pass still runs.
    pub fn spawn_ref_store_maintenance(&self) {
        let orch = self.clone();
        tokio::spawn(async move {
            loop {
                let pass = orch.clone();
                let joined = crate::orchestrator::base_advance::supervised_sweep_pass(async move {
                    run_pass(&pass).await;
                })
                .await;
                if let Err(error) = joined {
                    log::error!("ref-store maintenance pass ended abnormally: {error}");
                }
                tokio::time::sleep(MEASURE_INTERVAL).await;
            }
        });
    }
}
