//! The usage-window time series (`usage_window_readings`): what each provider
//! account's usage windows read, each time a refresh changed them.
//!
//! Kept as plain strings and numbers so this crate need not know the model
//! facet's usage types; the orchestrator maps a snapshot's windows in.

use super::{DbResult, LocalDb, RowExt};
use crate::turso::params;

/// How long a reading is kept regardless of resets, in seconds.
pub const USAGE_READING_MIN_AGE_SECS: i64 = 60 * 24 * 60 * 60;

/// How many of a window's most recent resets keep their readings past
/// [`USAGE_READING_MIN_AGE_SECS`].
pub const USAGE_READING_KEPT_RESETS: i64 = 8;

/// One window as a refresh measured it.
#[derive(Debug, Clone, PartialEq)]
pub struct UsageReadingInput {
    pub window_id: String,
    pub window_name: String,
    pub scope: String,
    pub scope_target: Option<String>,
    pub remaining_percent: f64,
    pub window_duration_mins: Option<i64>,
    pub resets_at: Option<i64>,
}

/// One stored reading.
#[derive(Debug, Clone, PartialEq)]
pub struct UsageWindowReadingRow {
    pub id: i64,
    pub provider: String,
    pub account_id: String,
    pub window_id: String,
    pub window_name: String,
    pub scope: String,
    pub scope_target: Option<String>,
    pub remaining_percent: f64,
    pub window_duration_mins: Option<i64>,
    pub resets_at: Option<i64>,
    pub captured_at: i64,
    pub source: String,
}

const READING_COLUMNS: &str = "id,provider,account_id,window_id,window_name,scope,scope_target,remaining_percent,window_duration_mins,resets_at,captured_at,source";

fn reading_from_row(row: &crate::turso::Row) -> DbResult<UsageWindowReadingRow> {
    Ok(UsageWindowReadingRow {
        id: row.i64(0)?,
        provider: row.text(1)?,
        account_id: row.text(2)?,
        window_id: row.text(3)?,
        window_name: row.text(4)?,
        scope: row.text(5)?,
        scope_target: row.opt_text(6)?,
        remaining_percent: row.f64(7)?,
        window_duration_mins: row.opt_i64(8)?,
        resets_at: row.opt_i64(9)?,
        captured_at: row.i64(10)?,
        source: row.text(11)?,
    })
}

/// Record one refresh's windows for an account, in one transaction, and return
/// how many readings were inserted.
///
/// A window whose newest stored reading has the same remaining percent, reset
/// time and duration is skipped: the comparison reads the table, so it holds
/// across restarts. Each window that gains a row is then pruned of readings
/// older than [`USAGE_READING_MIN_AGE_SECS`] that also fall outside its last
/// [`USAGE_READING_KEPT_RESETS`] resets, whichever keeps more. `captured_at`
/// is the clock both rules measure from.
pub async fn record_usage_readings(
    db: &LocalDb,
    provider: &str,
    account_id: &str,
    source: &str,
    captured_at: i64,
    windows: Vec<UsageReadingInput>,
) -> DbResult<usize> {
    let provider = provider.to_string();
    let account_id = account_id.to_string();
    let source = source.to_string();
    db.write(move |conn| {
        let provider = provider.clone();
        let account_id = account_id.clone();
        let source = source.clone();
        let windows = windows.clone();
        Box::pin(async move {
            let mut inserted = 0;
            for window in windows {
                let group = (
                    account_id.as_str(),
                    provider.as_str(),
                    window.window_id.as_str(),
                );
                let previous = {
                    let mut rows = conn
                        .query(
                            "SELECT remaining_percent, resets_at, window_duration_mins
                             FROM usage_window_readings
                             WHERE account_id = ?1 AND provider = ?2 AND window_id = ?3
                             ORDER BY captured_at DESC, id DESC
                             LIMIT 1",
                            params![group.0, group.1, group.2],
                        )
                        .await?;
                    match rows.next().await? {
                        Some(row) => Some((row.f64(0)?, row.opt_i64(1)?, row.opt_i64(2)?)),
                        None => None,
                    }
                };
                if previous
                    == Some((
                        window.remaining_percent,
                        window.resets_at,
                        window.window_duration_mins,
                    ))
                {
                    continue;
                }
                conn.execute(
                    "INSERT INTO usage_window_readings (
                        provider, account_id, window_id, window_name, scope, scope_target,
                        remaining_percent, window_duration_mins, resets_at, captured_at, source
                     ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
                    params![
                        group.1,
                        group.0,
                        group.2,
                        window.window_name.as_str(),
                        window.scope.as_str(),
                        window.scope_target.as_deref(),
                        window.remaining_percent,
                        window.window_duration_mins,
                        window.resets_at,
                        captured_at,
                        source.as_str()
                    ],
                )
                .await?;
                inserted += 1;

                // The oldest of the last KEPT_RESETS distinct resets, floored to
                // the hour so jitter in a provider's reset time is not counted as
                // another reset. None while the window has seen fewer resets, in
                // which case only age bounds it.
                let kept_from: Option<i64> = {
                    let mut rows = conn
                        .query(
                            "SELECT DISTINCT resets_at / 3600 AS reset_hour
                             FROM usage_window_readings
                             WHERE account_id = ?1 AND provider = ?2 AND window_id = ?3
                               AND resets_at IS NOT NULL
                             ORDER BY reset_hour DESC
                             LIMIT 1 OFFSET ?4",
                            params![group.0, group.1, group.2, USAGE_READING_KEPT_RESETS - 1],
                        )
                        .await?;
                    match rows.next().await? {
                        Some(row) => Some(row.i64(0)?),
                        None => None,
                    }
                };
                let aged_before = captured_at - USAGE_READING_MIN_AGE_SECS;
                match kept_from {
                    Some(kept_from) => {
                        conn.execute(
                            "DELETE FROM usage_window_readings
                             WHERE account_id = ?1 AND provider = ?2 AND window_id = ?3
                               AND captured_at < ?4
                               AND (resets_at IS NULL OR resets_at / 3600 < ?5)",
                            params![group.0, group.1, group.2, aged_before, kept_from],
                        )
                        .await?;
                    }
                    None => {
                        conn.execute(
                            "DELETE FROM usage_window_readings
                             WHERE account_id = ?1 AND provider = ?2 AND window_id = ?3
                               AND captured_at < ?4 AND resets_at IS NULL",
                            params![group.0, group.1, group.2, aged_before],
                        )
                        .await?;
                    }
                }
            }
            Ok(inserted)
        })
    })
    .await
}

/// An account's readings, oldest first. `window` matches a window's name or
/// its id; `since` bounds `captured_at` from below.
pub async fn list_usage_readings(
    db: &LocalDb,
    account_id: &str,
    window: Option<&str>,
    since: Option<i64>,
) -> DbResult<Vec<UsageWindowReadingRow>> {
    let since = since.unwrap_or(i64::MIN);
    match window {
        Some(window) => {
            db.query_all(
                format!(
                    "SELECT {READING_COLUMNS} FROM usage_window_readings
                     WHERE account_id = ?1 AND captured_at >= ?2
                       AND (window_name = ?3 OR window_id = ?3)
                     ORDER BY captured_at, id"
                ),
                (account_id.to_string(), since, window.to_string()),
                reading_from_row,
            )
            .await
        }
        None => {
            db.query_all(
                format!(
                    "SELECT {READING_COLUMNS} FROM usage_window_readings
                     WHERE account_id = ?1 AND captured_at >= ?2
                     ORDER BY captured_at, id"
                ),
                (account_id.to_string(), since),
                reading_from_row,
            )
            .await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DAY: i64 = 24 * 60 * 60;

    fn window(id: &str, remaining: f64, resets_at: Option<i64>) -> UsageReadingInput {
        UsageReadingInput {
            window_id: id.into(),
            window_name: "Week".into(),
            scope: "weekly".into(),
            scope_target: None,
            remaining_percent: remaining,
            window_duration_mins: None,
            resets_at,
        }
    }

    async fn record(db: &LocalDb, at: i64, windows: Vec<UsageReadingInput>) -> usize {
        record_usage_readings(db, "claude", "acct-a", "claude_usage_oauth", at, windows)
            .await
            .unwrap()
    }

    async fn captured(db: &LocalDb) -> Vec<i64> {
        list_usage_readings(db, "acct-a", None, None)
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.captured_at)
            .collect()
    }

    #[tokio::test]
    async fn an_unchanged_reading_is_skipped_across_calls() {
        let db = super::super::migrated_test_db("usage-readings-dedupe.db").await;
        let reset = 10 * DAY;
        assert_eq!(
            record(&db, 100, vec![window("weekly_all", 80.0, Some(reset))]).await,
            1
        );
        // A separate call reads the stored row to decide.
        assert_eq!(
            record(&db, 200, vec![window("weekly_all", 80.0, Some(reset))]).await,
            0
        );
        assert_eq!(
            record(&db, 300, vec![window("weekly_all", 79.5, Some(reset))]).await,
            1
        );
        assert_eq!(
            record(
                &db,
                400,
                vec![window("weekly_all", 79.5, Some(reset + 7 * DAY))]
            )
            .await,
            1
        );
        // Another account's identical reading is its own series.
        let other = record_usage_readings(
            &db,
            "claude",
            "acct-b",
            "claude_usage_oauth",
            500,
            vec![window("weekly_all", 79.5, Some(reset + 7 * DAY))],
        )
        .await
        .unwrap();
        assert_eq!(other, 1);
        assert_eq!(captured(&db).await, [100, 300, 400]);
    }

    #[tokio::test]
    async fn readings_past_sixty_days_go_only_outside_the_last_eight_resets() {
        let db = super::super::migrated_test_db("usage-readings-resets.db").await;
        // Ten distinct resets, each read once an hour apart, long before `now`.
        let now = 400 * DAY;
        for week in 0..10 {
            let at = 100 * DAY + week * 3600;
            record(
                &db,
                at,
                vec![window("weekly_all", 50.0, Some(at + 3 * DAY))],
            )
            .await;
        }
        assert_eq!(captured(&db).await.len(), 10);
        record(
            &db,
            now,
            vec![window("weekly_all", 49.0, Some(now + 3 * DAY))],
        )
        .await;
        // The new reading is the newest reset; the 7 newest old ones stay with it.
        let kept = captured(&db).await;
        assert_eq!(kept.len(), 8);
        assert_eq!(kept[0], 100 * DAY + 3 * 3600);
        assert_eq!(*kept.last().unwrap(), now);
    }

    #[tokio::test]
    async fn with_few_resets_only_age_prunes() {
        let db = super::super::migrated_test_db("usage-readings-age.db").await;
        let reset = 500 * DAY;
        // Three resets spread over 90 days; readings inside the last 60 days stay.
        let now = 200 * DAY;
        for (offset, remaining, resets_at) in [
            (90, 90.0, Some(reset)),
            (61, 80.0, Some(reset + 7 * DAY)),
            (59, 70.0, Some(reset + 14 * DAY)),
            (1, 60.0, None),
        ] {
            record(
                &db,
                now - offset * DAY,
                vec![window("weekly_all", remaining, resets_at)],
            )
            .await;
        }
        // Old readings with a reset survive while fewer than 8 resets exist.
        assert_eq!(captured(&db).await.len(), 4);

        let db = super::super::migrated_test_db("usage-readings-age-null.db").await;
        record(&db, now - 90 * DAY, vec![window("rolling", 90.0, None)]).await;
        record(&db, now - 30 * DAY, vec![window("rolling", 80.0, None)]).await;
        record(&db, now, vec![window("rolling", 70.0, None)]).await;
        // A window with no reset times is bounded by age alone.
        assert_eq!(captured(&db).await, [now - 30 * DAY, now]);
    }

    #[tokio::test]
    async fn a_model_scoped_window_is_found_by_name_or_id() {
        let db = super::super::migrated_test_db("usage-readings-model.db").await;
        let opus = UsageReadingInput {
            window_id: "weekly_opus-opus".into(),
            window_name: "Opus".into(),
            scope: "weekly".into(),
            scope_target: Some("Opus".into()),
            remaining_percent: 42.0,
            window_duration_mins: Some(7 * 24 * 60),
            resets_at: Some(10 * DAY),
        };
        record(
            &db,
            100,
            vec![window("weekly_all", 80.0, Some(10 * DAY)), opus],
        )
        .await;
        let by_name = list_usage_readings(&db, "acct-a", Some("Opus"), None)
            .await
            .unwrap();
        assert_eq!(by_name.len(), 1);
        let row = &by_name[0];
        assert_eq!(row.window_id, "weekly_opus-opus");
        assert_eq!(row.window_name, "Opus");
        assert_eq!(row.scope_target.as_deref(), Some("Opus"));
        assert_eq!(row.window_duration_mins, Some(7 * 24 * 60));
        assert_eq!(row.source, "claude_usage_oauth");
        let by_id = list_usage_readings(&db, "acct-a", Some("weekly_opus-opus"), None)
            .await
            .unwrap();
        assert_eq!(by_id, by_name);
        assert!(list_usage_readings(&db, "acct-a", None, Some(101))
            .await
            .unwrap()
            .is_empty());
    }
}
