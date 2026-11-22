//! Durable observations of actual replay comparisons, independent of review-wave sharing.
use std::path::Path;

use cairn_db::turso::params;

use super::ReplayComparison;
use crate::storage::{LocalDb, RowExt};

/// Call once after measuring, never once per consumer of the measured result.
/// The owning project's database must be supplied, not the machine-local overlay.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn record(
    db: &LocalDb,
    project_id: &str,
    store: &Path,
    branch: &str,
    before: Option<&str>,
    after: Option<&str>,
    comparison: &ReplayComparison,
) -> Result<(), String> {
    let (outcome, reason) = match comparison {
        ReplayComparison::Unchanged => ("unchanged", None),
        ReplayComparison::Changed => ("changed", None),
        ReplayComparison::Unreadable { reason } => ("unreadable", Some(reason.as_str())),
    };
    db.execute(
        "INSERT INTO replay_comparisons
         (project_id, store_path, bookmark, before_commit, after_commit, outcome, reason, observed_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![project_id, store.to_string_lossy().as_ref(), branch, before, after,
                outcome, reason, chrono::Utc::now().timestamp_millis()],
    )
    .await
    .map(|_| ())
    .map_err(|error| format!("record replay comparison: {error}"))
}

#[derive(Debug, Default)]
pub(crate) struct Health {
    comparisons: i64,
    unreadable: i64,
    unreadable_day: i64,
    recent: Vec<Unreadable>,
}

#[derive(Debug)]
struct Unreadable {
    branch: String,
    before: Option<String>,
    after: Option<String>,
    reason: String,
    observed_ms: i64,
}

/// A branch scope follows its durable bookmark across stores/executors. The
/// project scope includes every store, even when this reader has no local repo.
/// One statement pins counts and recent reasons to the same database snapshot.
pub(crate) async fn health(
    db: &LocalDb,
    project_id: &str,
    branch: Option<&str>,
    now_ms: i64,
) -> Result<Health, String> {
    let rows = db
        .query_all(
            "WITH scoped AS (
            SELECT * FROM replay_comparisons
            WHERE project_id = ?1 AND (?2 IS NULL OR bookmark = ?2)
         ), totals AS (
            SELECT COUNT(*) AS comparisons,
                   COALESCE(SUM(outcome = 'unreadable'), 0) AS unreadable,
                   COALESCE(SUM(outcome = 'unreadable' AND observed_ms >= ?3), 0) AS unreadable_day
            FROM scoped
         ), recent AS (
            SELECT * FROM scoped WHERE outcome = 'unreadable'
            ORDER BY observed_ms DESC, id DESC LIMIT 5
         )
         SELECT totals.comparisons, totals.unreadable, totals.unreadable_day,
                recent.bookmark, recent.before_commit, recent.after_commit,
                recent.reason, recent.observed_ms
         FROM totals LEFT JOIN recent ON 1 = 1
         ORDER BY recent.observed_ms DESC, recent.id DESC",
            params![project_id, branch, now_ms.saturating_sub(86_400_000)],
            |row| {
                Ok((
                    row.get::<i64>(0)?,
                    row.get::<i64>(1)?,
                    row.get::<i64>(2)?,
                    match row.opt_text(3)? {
                        Some(branch) => Some(Unreadable {
                            branch,
                            before: row.opt_text(4)?,
                            after: row.opt_text(5)?,
                            reason: row.text(6)?,
                            observed_ms: row.get(7)?,
                        }),
                        None => None,
                    },
                ))
            },
        )
        .await
        .map_err(|error| format!("read replay comparison health: {error}"))?;
    let mut health = Health::default();
    for (comparisons, unreadable, unreadable_day, recent) in rows {
        health.comparisons = comparisons;
        health.unreadable = unreadable;
        health.unreadable_day = unreadable_day;
        health.recent.extend(recent);
    }
    Ok(health)
}

fn one_line(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

pub(crate) fn render(result: &Result<Health, String>) -> String {
    let health = match result {
        Ok(health) => health,
        Err(error) => {
            return format!(
                "Replay comparison health: unknown ({}) — this is not a zero unreadable count.\n",
                one_line(error)
            )
        }
    };
    let mut out = format!(
        "Replay comparison health: {} unreadable in the last 24 hours; {} unreadable of {} actual comparisons since recording began.\n",
        health.unreadable_day, health.unreadable, health.comparisons
    );
    if health.comparisons == 0 {
        out.push_str(
            "No comparisons recorded; patch equality has not been established by this history.\n",
        );
    }
    if !health.recent.is_empty() {
        out.push_str("\nRecent unreadable comparisons (newest first, at most 5):\n");
        for row in &health.recent {
            let time = chrono::DateTime::from_timestamp_millis(row.observed_ms)
                .map(|time| time.to_rfc3339())
                .unwrap_or_else(|| row.observed_ms.to_string());
            out.push_str(&format!(
                "- {time}: {} ({} → {}): {}\n",
                one_line(&row.branch),
                row.before.as_deref().unwrap_or("missing tip"),
                row.after.as_deref().unwrap_or("missing tip"),
                one_line(&row.reason),
            ));
        }
        out.push_str("\nUnreadable keeps the running wave at its original tree; it never proves equality or carries verdicts to the new head.\n");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn durable_counts_are_per_measurement_and_scope_to_the_owning_project_and_branch() {
        let db = crate::storage::migrated_test_db("replay-comparison-health").await;
        db.execute_script(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
             VALUES ('p', 'default', 'P', 'p', '/repo', 'main', 1, 1),
                    ('other', 'default', 'Other', 'other', '/other', 'main', 1, 1);".to_string()
        ).await.unwrap();
        for comparison in [
            ReplayComparison::Unchanged,
            ReplayComparison::Changed,
            ReplayComparison::Unreadable {
                reason: "operation budget exhausted".into(),
            },
            ReplayComparison::Unreadable {
                reason: "missing tip".into(),
            },
        ] {
            record(
                &db,
                "p",
                Path::new("/store-a"),
                "branch-a",
                Some("old"),
                Some("new"),
                &comparison,
            )
            .await
            .unwrap();
        }
        record(
            &db,
            "p",
            Path::new("/store-b"),
            "branch-b",
            None,
            None,
            &ReplayComparison::Unreadable {
                reason: "other store".into(),
            },
        )
        .await
        .unwrap();
        record(
            &db,
            "other",
            Path::new("/store-a"),
            "branch-a",
            None,
            None,
            &ReplayComparison::Unreadable {
                reason: "other project".into(),
            },
        )
        .await
        .unwrap();
        db.execute(
            "UPDATE replay_comparisons SET observed_ms = 1 WHERE reason = 'missing tip'",
            params![],
        )
        .await
        .unwrap();

        let now = chrono::Utc::now().timestamp_millis();
        let branch = health(&db, "p", Some("branch-a"), now).await.unwrap();
        assert_eq!(
            (branch.comparisons, branch.unreadable, branch.unreadable_day),
            (4, 2, 1)
        );
        let text = render(&Ok(branch));
        assert!(text
            .contains("1 unreadable in the last 24 hours; 2 unreadable of 4 actual comparisons"));
        assert!(text.contains("operation budget exhausted"));
        assert!(text.contains("missing tip"));
        assert!(!text.contains("other project"));
        assert!(text.contains("never proves equality"));
        let project = health(&db, "p", None, now).await.unwrap();
        assert_eq!((project.comparisons, project.unreadable), (5, 3));
        assert!(render(&Ok(project)).contains("other store"));
        assert_eq!(
            health(&db, "p", Some("empty"), now)
                .await
                .unwrap()
                .comparisons,
            0
        );
    }

    #[test]
    fn missing_health_is_not_reported_as_zero_or_as_patch_change() {
        assert!(render(&Err("database unavailable".into())).contains("unknown"));
        assert!(render(&Err("database unavailable".into())).contains("not a zero"));
        let empty = render(&Ok(Health::default()));
        assert!(empty.contains("No comparisons recorded"));
        assert!(empty.contains("equality has not been established"));
    }
}
