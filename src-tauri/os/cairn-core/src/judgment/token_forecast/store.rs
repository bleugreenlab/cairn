//! Where the forecast's labels and calibrations live: the private app
//! database (`token_forecast_labels`, `token_forecast_calibrations`),
//! readable through `cairn://db`. Nothing lives in a scratch directory.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use cairn_db::turso::params;

use super::calibration::{Calibration, CalibrationBin};
use super::fit::{FitLabel, Refit, RefitVerdict, FIT_CODE_VERSION};
use super::{ForecastStore, ScoreAnswer};
use crate::services::Clock;
use crate::storage::{DbError, DbResult, LocalDb, RowExt};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LabelStatus {
    Labelled,
    /// Looked at and left out (maker, scrub); not reconsidered.
    Skipped,
    /// The model declined a question; not re-asked for the same versions.
    Refused,
}

impl LabelStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Labelled => "labelled",
            Self::Skipped => "skipped",
            Self::Refused => "refused",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LabelSource {
    Filing,
    Backfill,
}

impl LabelSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Filing => "filing",
            Self::Backfill => "backfill",
        }
    }
}

/// Where a label's text came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TextSource {
    /// Handed over by the consumer at filing.
    Filing,
    /// The creation call's own payload.
    Event,
    /// The issue's text when it was labelled.
    Current,
}

impl TextSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Filing => "filing",
            Self::Event => "event",
            Self::Current => "current",
        }
    }
}

/// One label row.
#[derive(Debug, Clone, PartialEq)]
pub struct LabelRecord {
    pub issue_id: String,
    pub answerer_version: String,
    pub question_version: String,
    pub status: LabelStatus,
    pub skip_reason: Option<String>,
    pub answer: Option<ScoreAnswer>,
    pub text_hash: Option<String>,
    pub text_source: Option<TextSource>,
    pub filing_maker: Option<String>,
    pub filing_maker_method: Option<String>,
    pub source: LabelSource,
}

pub async fn upsert_label(db: &LocalDb, label: &LabelRecord, now: i64) -> DbResult<()> {
    let answer = label.answer.as_ref();
    let provenance = answer.map(|a| &a.provenance);
    db.execute(
        "INSERT INTO token_forecast_labels
             (issue_id, answerer_version, question_version, status, skip_reason,
              effort, scope, score, distributions_json, text_hash, text_source,
              filing_maker, filing_maker_method, source, model, provider, zdr_basis,
              cost_usd, labeled_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19)
         ON CONFLICT(issue_id, answerer_version, question_version) DO UPDATE SET
             status = excluded.status, skip_reason = excluded.skip_reason,
             effort = excluded.effort, scope = excluded.scope, score = excluded.score,
             distributions_json = excluded.distributions_json,
             text_hash = excluded.text_hash, text_source = excluded.text_source,
             filing_maker = excluded.filing_maker,
             filing_maker_method = excluded.filing_maker_method,
             source = excluded.source, model = excluded.model,
             provider = excluded.provider, zdr_basis = excluded.zdr_basis,
             cost_usd = excluded.cost_usd, labeled_at = excluded.labeled_at",
        params![
            label.issue_id.as_str(),
            label.answerer_version.as_str(),
            label.question_version.as_str(),
            label.status.as_str(),
            label.skip_reason.as_deref(),
            answer.map(|a| a.effort),
            answer.map(|a| a.scope),
            answer.map(|a| a.score),
            answer.map(|a| a.distributions.to_string()),
            label.text_hash.as_deref(),
            label.text_source.map(TextSource::as_str),
            label.filing_maker.as_deref(),
            label.filing_maker_method.as_deref(),
            label.source.as_str(),
            provenance.map(|p| p.model.clone()),
            provenance.map(|p| p.provider.clone()),
            provenance.map(|p| p.zdr_satisfied_by.clone()),
            provenance.and_then(|p| p.cost_usd),
            now
        ],
    )
    .await?;
    Ok(())
}

/// Every issue with a label row of any status for these versions.
pub async fn labelled_issue_ids(
    db: &LocalDb,
    answerer_version: &str,
    question_version: &str,
) -> DbResult<std::collections::HashSet<String>> {
    let rows = db
        .query_all(
            "SELECT issue_id FROM token_forecast_labels
             WHERE answerer_version = ?1 AND question_version = ?2",
            params![answerer_version, question_version],
            |row| row.text(0),
        )
        .await?;
    Ok(rows.into_iter().collect())
}

/// The answered labels for these versions, by issue id.
pub async fn fit_labels(
    db: &LocalDb,
    answerer_version: &str,
    question_version: &str,
) -> DbResult<HashMap<String, FitLabel>> {
    let rows = db
        .query_all(
            "SELECT issue_id, score, filing_maker, text_source FROM token_forecast_labels
             WHERE answerer_version = ?1 AND question_version = ?2
               AND status = 'labelled' AND score IS NOT NULL",
            params![answerer_version, question_version],
            |row| {
                Ok((
                    row.text(0)?,
                    FitLabel {
                        score: row.f64(1)?,
                        filing_maker: row.opt_text(2)?,
                        text_source: row.opt_text(3)?,
                    },
                ))
            },
        )
        .await?;
    Ok(rows.into_iter().collect())
}

/// When the newest answered label for these versions landed.
pub async fn newest_label_at(
    db: &LocalDb,
    answerer_version: &str,
    question_version: &str,
) -> DbResult<Option<i64>> {
    db.query_one(
        "SELECT max(labeled_at) FROM token_forecast_labels
         WHERE answerer_version = ?1 AND question_version = ?2 AND status = 'labelled'",
        params![answerer_version, question_version],
        |row| row.opt_i64(0),
    )
    .await
}

/// What the backfill has spent since `since`: each call's reported cost, or
/// its reservation when none was reported.
pub async fn backfill_spend_since(db: &LocalDb, since: i64) -> DbResult<f64> {
    db.query_one(
        "SELECT coalesce(sum(coalesce(settled_usd, reserved_usd)), 0.0)
         FROM token_forecast_spend WHERE reserved_at >= ?1",
        params![since],
        |row| row.f64(0),
    )
    .await
}

/// Reserve `usd` for one call before it is sent.
pub async fn reserve_spend(
    db: &LocalDb,
    id: &str,
    issue_id: &str,
    usd: f64,
    now: i64,
) -> DbResult<()> {
    db.execute(
        "INSERT INTO token_forecast_spend (id, issue_id, reserved_at, reserved_usd)
         VALUES (?1, ?2, ?3, ?4)",
        params![id, issue_id, now, usd],
    )
    .await?;
    Ok(())
}

/// Settle a reservation to the cost the reply reported.
pub async fn settle_spend(db: &LocalDb, id: &str, usd: f64, now: i64) -> DbResult<()> {
    db.execute(
        "UPDATE token_forecast_spend SET settled_usd = ?2, settled_at = ?3 WHERE id = ?1",
        params![id, usd, now],
    )
    .await?;
    Ok(())
}

fn decode<T: serde::de::DeserializeOwned>(column: &str, text: Option<String>) -> DbResult<T> {
    let text = text.ok_or_else(|| DbError::Row(format!("{column} is empty")))?;
    serde_json::from_str(&text).map_err(|error| DbError::Row(format!("{column}: {error}")))
}

/// The active calibration for these versions.
pub async fn active_calibration(
    db: &LocalDb,
    answerer_version: &str,
    question_version: &str,
) -> DbResult<Option<Calibration>> {
    let row = db
        .query_opt(
            "SELECT version, as_of, table_from, table_to, shift_from, shift_to, fit_n, shift_n,
                    grp, edges_json, bins_json, shifts_json, large_actuals_coverage
             FROM token_forecast_calibrations
             WHERE answerer_version = ?1 AND question_version = ?2 AND status = 'active'",
            params![answerer_version, question_version],
            |row| {
                Ok((
                    row.text(0)?,
                    [
                        row.i64(1)?,
                        row.i64(2)?,
                        row.i64(3)?,
                        row.i64(4)?,
                        row.i64(5)?,
                    ],
                    (row.i64(6)?, row.i64(7)?),
                    row.text(8)?,
                    (row.opt_text(9)?, row.opt_text(10)?, row.opt_text(11)?),
                    row.opt_f64(12)?,
                ))
            },
        )
        .await?;
    let Some((
        version,
        times,
        (fit_n, shift_n),
        group,
        (edges, bins, shifts),
        large_actuals_coverage,
    )) = row
    else {
        return Ok(None);
    };
    let bins: Vec<CalibrationBin> = decode("bins_json", bins)?;
    Ok(Some(Calibration {
        version,
        answerer_version: answerer_version.to_string(),
        question_version: question_version.to_string(),
        as_of: times[0],
        table_window: [times[1], times[2]],
        shift_window: [times[3], times[4]],
        fit_n: fit_n.max(0) as u64,
        shift_n: shift_n.max(0) as u64,
        edges: decode("edges_json", edges)?,
        bins,
        shifts: decode("shifts_json", shifts)?,
        group,
        large_actuals_coverage,
    }))
}

/// When the newest refit for these versions, accepted or not, was recorded.
pub async fn latest_refit_at(
    db: &LocalDb,
    answerer_version: &str,
    question_version: &str,
) -> DbResult<Option<i64>> {
    db.query_one(
        "SELECT max(created_at) FROM token_forecast_calibrations
         WHERE answerer_version = ?1 AND question_version = ?2",
        params![answerer_version, question_version],
        |row| row.opt_i64(0),
    )
    .await
}

fn json(value: &impl serde::Serialize) -> String {
    serde_json::to_string(value).expect("calibration fields serialize")
}

/// Record a refit. An accepted one becomes the active calibration for its
/// versions in the same write, superseding the previous one; recording the
/// same version twice changes nothing but its time.
pub async fn record_refit(db: &LocalDb, refit: &Refit, now: i64) -> DbResult<()> {
    let exclusions = json(&refit.exclusions);
    let row = match &refit.verdict {
        RefitVerdict::Accepted(c) => CalibrationRow {
            version: c.version.clone(),
            answerer_version: c.answerer_version.clone(),
            question_version: c.question_version.clone(),
            status: "active",
            reason: None,
            as_of: c.as_of,
            table_window: c.table_window,
            shift_window: c.shift_window,
            fit_n: c.fit_n as i64,
            shift_n: c.shift_n as i64,
            group: c.group.clone(),
            edges: Some(json(&c.edges)),
            bins: Some(json(&c.bins)),
            shifts: Some(json(&c.shifts)),
            large_actuals_coverage: c.large_actuals_coverage,
        },
        RefitVerdict::Rejected(r) => CalibrationRow {
            version: r.version.clone(),
            answerer_version: r.answerer_version.clone(),
            question_version: r.question_version.clone(),
            status: "rejected",
            reason: Some(r.reason.clone()),
            as_of: r.as_of,
            table_window: r.table_window,
            shift_window: r.shift_window,
            fit_n: r.fit_n as i64,
            shift_n: r.shift_n as i64,
            group: super::applicability::ANTHROPIC.to_string(),
            edges: None,
            bins: None,
            shifts: None,
            large_actuals_coverage: None,
        },
    };
    db.write(move |conn| {
        let row = row.clone();
        let exclusions = exclusions.clone();
        Box::pin(async move {
            if row.status == "active" {
                conn.execute(
                    "UPDATE token_forecast_calibrations SET status = 'superseded'
                     WHERE answerer_version = ?1 AND question_version = ?2
                       AND status = 'active' AND version <> ?3",
                    params![
                        row.answerer_version.as_str(),
                        row.question_version.as_str(),
                        row.version.as_str()
                    ],
                )
                .await?;
            }
            conn.execute(
                "INSERT INTO token_forecast_calibrations
                     (version, answerer_version, question_version, status, reason, as_of,
                      table_from, table_to, shift_from, shift_to, fit_n, shift_n, grp,
                      edges_json, bins_json, shifts_json, exclusions_json, code_version,
                      created_at, large_actuals_coverage)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15,
                         ?16, ?17, ?18, ?19, ?20)
                 ON CONFLICT(version) DO UPDATE SET
                     status = excluded.status, created_at = excluded.created_at",
                params![
                    row.version.as_str(),
                    row.answerer_version.as_str(),
                    row.question_version.as_str(),
                    row.status,
                    row.reason.as_deref(),
                    row.as_of,
                    row.table_window[0],
                    row.table_window[1],
                    row.shift_window[0],
                    row.shift_window[1],
                    row.fit_n,
                    row.shift_n,
                    row.group.as_str(),
                    row.edges.as_deref(),
                    row.bins.as_deref(),
                    row.shifts.as_deref(),
                    exclusions.as_str(),
                    FIT_CODE_VERSION,
                    now,
                    row.large_actuals_coverage
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
}

#[derive(Debug, Clone)]
struct CalibrationRow {
    version: String,
    answerer_version: String,
    question_version: String,
    status: &'static str,
    reason: Option<String>,
    as_of: i64,
    table_window: [i64; 2],
    shift_window: [i64; 2],
    fit_n: i64,
    shift_n: i64,
    group: String,
    edges: Option<String>,
    bins: Option<String>,
    shifts: Option<String>,
    large_actuals_coverage: Option<f64>,
}

/// Every refit for these versions, newest first: (version, status, reason,
/// exclusions). For reports and tests.
pub async fn refit_history(
    db: &LocalDb,
    answerer_version: &str,
    question_version: &str,
) -> DbResult<Vec<(String, String, Option<String>, BTreeMap<String, u64>)>> {
    let rows = db
        .query_all(
            "SELECT version, status, reason, exclusions_json FROM token_forecast_calibrations
             WHERE answerer_version = ?1 AND question_version = ?2
             ORDER BY created_at DESC, version",
            params![answerer_version, question_version],
            |row| Ok((row.text(0)?, row.text(1)?, row.opt_text(2)?, row.text(3)?)),
        )
        .await?;
    rows.into_iter()
        .map(|(version, status, reason, exclusions)| {
            Ok((
                version,
                status,
                reason,
                decode("exclusions_json", Some(exclusions))?,
            ))
        })
        .collect()
}

/// The production [`ForecastStore`]: the private database, stamped by the
/// orchestrator's clock.
pub struct DbForecastStore {
    db: Arc<LocalDb>,
    clock: Arc<dyn Clock>,
}

impl DbForecastStore {
    pub fn new(db: Arc<LocalDb>, clock: Arc<dyn Clock>) -> Self {
        Self { db, clock }
    }
}

#[async_trait]
impl ForecastStore for DbForecastStore {
    async fn active_calibration(
        &self,
        answerer_version: &str,
        question_version: &str,
    ) -> Result<Option<Calibration>, String> {
        active_calibration(&self.db, answerer_version, question_version)
            .await
            .map_err(|error| format!("could not read the active calibration: {error}"))
    }

    async fn record_label(&self, label: LabelRecord) -> Result<(), String> {
        upsert_label(&self.db, &label, self.clock.now())
            .await
            .map_err(|error| format!("could not record the label: {error}"))
    }
}
