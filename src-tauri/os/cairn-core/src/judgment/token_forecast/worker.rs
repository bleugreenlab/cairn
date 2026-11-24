//! The background worker: each pass backfills a bounded batch of labels,
//! then refits when one is due. A refit is due weekly once a calibration is
//! active (at startup too, when the active one is older than a week), and,
//! while none is, whenever labels have landed since the last attempt. Every
//! refit is recorded, accepted or rejected.
//!
//! Nothing here decides capacity, admission or placement.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use super::answerer::DecideAnswerer;
use super::backfill::{self, Budget};
use super::fit::{self, Refit, RefitVerdict, FLOOR};
use super::{population, question, store, ANSWERER_VERSION};
use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;

/// How often a pass runs. The first runs at startup.
pub const PASS_INTERVAL: Duration = Duration::from_secs(3_600);
/// How old the active calibration may grow before a refit.
pub const REFIT_EVERY_SECS: i64 = 7 * 86_400;

pub fn spawn(orch: Orchestrator) {
    let worker = orch.clone();
    orch.spawn_detached(async move {
        let orch = worker;
        let mut ticks = tokio::time::interval(PASS_INTERVAL);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticks.tick().await;
            run_pass(&orch).await;
        }
    });
}

async fn run_pass(orch: &Orchestrator) {
    let dbs = orch.db.all_dbs().await;
    let labels_db = orch.db.local.clone();
    let answerer = DecideAnswerer::new(orch.clone());
    let report = backfill::pass(
        &dbs,
        &labels_db,
        &answerer,
        orch.services.clock.now(),
        Budget::default(),
    )
    .await;
    if report.examined > 0 || report.stopped.is_some() {
        log::info!("token forecast backfill: {report:?}");
    }
    match refit_if_due(
        &dbs,
        &labels_db,
        ANSWERER_VERSION,
        orch.services.clock.now(),
    )
    .await
    {
        Ok(Some(refit)) => match &refit.verdict {
            RefitVerdict::Accepted(calibration) => log::info!(
                "token forecast: calibration {} is active (fit on {}, shifted on {})",
                calibration.version,
                calibration.fit_n,
                calibration.shift_n
            ),
            RefitVerdict::Rejected(rejected) => log::info!(
                "token forecast: refit {} rejected: {}",
                rejected.version,
                rejected.reason
            ),
        },
        Ok(None) => {}
        Err(error) => log::warn!("token forecast: the refit failed: {error}"),
    }
}

/// Refit and record it when one is due.
pub async fn refit_if_due(
    dbs: &[Arc<LocalDb>],
    labels_db: &LocalDb,
    answerer_version: &str,
    now: i64,
) -> Result<Option<Refit>, String> {
    let question_version = question::question_version();
    let read = |error: crate::storage::DbError| error.to_string();
    let due = match store::active_calibration(labels_db, answerer_version, question_version)
        .await
        .map_err(read)?
    {
        Some(active) => now - active.as_of >= REFIT_EVERY_SECS,
        None => {
            let newest = store::newest_label_at(labels_db, answerer_version, question_version)
                .await
                .map_err(read)?;
            let attempted = store::latest_refit_at(labels_db, answerer_version, question_version)
                .await
                .map_err(read)?;
            match (newest, attempted) {
                (None, _) => false,
                (Some(_), None) => true,
                (Some(newest), Some(attempted)) => newest > attempted,
            }
        }
    };
    if !due {
        return Ok(None);
    }
    let refit = refit_now(dbs, labels_db, answerer_version, now, now).await?;
    store::record_refit(labels_db, &refit, now)
        .await
        .map_err(read)?;
    Ok(Some(refit))
}

/// The population of every database and the stored labels, refit at `as_of`.
pub async fn refit_now(
    dbs: &[Arc<LocalDb>],
    labels_db: &LocalDb,
    answerer_version: &str,
    as_of: i64,
    snapshot: i64,
) -> Result<Refit, String> {
    let question_version = question::question_version();
    let mut issues = Vec::new();
    let mut seen = HashSet::new();
    for db in dbs {
        for issue in population::load(db, FLOOR)
            .await
            .map_err(|error| format!("could not read the population: {error}"))?
        {
            if seen.insert(issue.issue_id.clone()) {
                issues.push(issue);
            }
        }
    }
    let labels = store::fit_labels(labels_db, answerer_version, question_version)
        .await
        .map_err(|error| format!("could not read the labels: {error}"))?;
    Ok(fit::refit(
        &issues,
        &labels,
        as_of,
        snapshot,
        answerer_version,
        question_version,
    ))
}
