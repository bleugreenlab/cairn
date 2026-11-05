//! Scheduled methods: the `schedule` a service's manifest declares, called by
//! the supervisor on a fixed cadence as the service's owner, with no turn.
//!
//! A thread runs code without a turn only through a service (tenet 1313).
//! On-demand code is a method call; scheduled code is a `schedule` entry
//! naming a method; wakes stay attention. So this module never starts a turn
//! and wakes the owner only once per failure streak ([`super::notify_owner`],
//! whose one other caller is a service whose always-on machine was lost).
//!
//! Each declared entry is a `service_schedules` row holding its anchored
//! recurrence and what its last occurrence did. [`sync`] keeps the rows in step
//! with the declarations whenever the supervisor applies an owner's source, so
//! a runner restart resumes each cadence instead of re-anchoring it. An
//! occurrence is claimed in the database before the call is made, so a restart
//! mid-call or a second runner cannot run one occurrence twice; an occurrence
//! missed while no runner was up is run once, not once per missed interval.
//! A call the placement substrate refused (`placement_unavailable`) gives its
//! claim back ([`unclaim`]) and is retried, a bounded number of times, rather
//! than settled: the refusal is the substrate's, not the service's failure.
//!
//! A scheduled call is an ordinary call ([`super::supervisor::call`]): it boots
//! an idle `on_demand` service, passes the concurrency gate, executes under a
//! delegated run whose caller acts for the owner (the project for a project's
//! service, the owning thread for a thread's, so the method writes the owner's
//! thread database under the store's one ownership rule), and writes the same
//! `service_log` call row as any call.

use cairn_db::turso::params;
use serde_json::Value;

use super::declaration::ScheduleEntry;
use super::ServiceOwner;
use crate::storage::{DbResult, LocalDb, RowExt};

/// Bound on how much of a failure the streak notice quotes; the service log
/// keeps the call row and the status keeps the full last error.
const NOTICE_ERROR_CHARS: usize = 1_500;

/// One declared entry's cadence and last outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ScheduleRow {
    pub entry_key: String,
    pub method: String,
    pub args: Value,
    pub every_ms: i64,
    /// Epoch seconds the recurrence is anchored at.
    pub anchor_at: i64,
    pub last_fired_occurrence_at: Option<i64>,
    pub last_run_at: Option<i64>,
    /// `ok`, `failed`, or `skipped` (the service was stopped).
    pub last_status: Option<String>,
    pub last_error: Option<String>,
    pub consecutive_failures: i64,
}

impl ScheduleRow {
    /// The newest occurrence at or before `now` that has not been run.
    pub(crate) fn due_at(&self, now: i64) -> Option<i64> {
        crate::orchestrator::wakes::due_occurrence(self.anchor_at, self.every_ms, now)
            .filter(|due| self.last_fired_occurrence_at.is_none_or(|last| *due > last))
    }

    /// The first occurrence after `now`.
    pub(crate) fn next_due_at(&self, now: i64) -> Option<i64> {
        crate::orchestrator::wakes::next_occurrence(self.anchor_at, self.every_ms, now)
    }
}

/// An interval as a person writes it: `15m`, `1h`, `2d`.
pub(crate) fn every_text(every_ms: i64) -> String {
    const UNITS: [(i64, &str); 4] = [
        (86_400_000, "d"),
        (3_600_000, "h"),
        (60_000, "m"),
        (1_000, "s"),
    ];
    UNITS
        .iter()
        .find(|(size, _)| every_ms % size == 0)
        .map(|(size, unit)| format!("{}{unit}", every_ms / size))
        .unwrap_or_else(|| format!("{every_ms}ms"))
}

const COLUMNS: &str =
    "entry_key, method, args_json, every_ms, anchor_at, last_fired_occurrence_at, \
     last_run_at, last_status, last_error, consecutive_failures";

fn row_of(row: &cairn_db::turso::Row) -> DbResult<ScheduleRow> {
    Ok(ScheduleRow {
        entry_key: row.text(0)?,
        method: row.text(1)?,
        args: serde_json::from_str(&row.text(2)?).unwrap_or(Value::Null),
        every_ms: row.i64(3)?,
        anchor_at: row.i64(4)?,
        last_fired_occurrence_at: row.opt_i64(5)?,
        last_run_at: row.opt_i64(6)?,
        last_status: row.opt_text(7)?,
        last_error: row.opt_text(8)?,
        consecutive_failures: row.i64(9)?,
    })
}

/// A service's schedule rows, in method order.
pub(crate) async fn list(
    db: &LocalDb,
    project_id: &str,
    owner: &ServiceOwner,
    service: &str,
) -> Result<Vec<ScheduleRow>, String> {
    let (project_id, owner, service) = (
        project_id.to_string(),
        owner.key().to_string(),
        service.to_string(),
    );
    db.read(move |conn| {
        let (project_id, owner, service) = (project_id.clone(), owner.clone(), service.clone());
        Box::pin(async move {
            let mut rows = conn
                .query(
                    &format!(
                        "SELECT {COLUMNS} FROM service_schedules
                         WHERE project_id = ?1 AND owner_thread = ?2 AND service = ?3
                         ORDER BY method, every_ms, entry_key"
                    ),
                    params![project_id.as_str(), owner.as_str(), service.as_str()],
                )
                .await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                out.push(row_of(&row)?);
            }
            Ok(out)
        })
    })
    .await
    .map_err(|error| format!("Failed to read the service's schedule: {error}"))
}

/// Every service of `project_id` with schedule rows, and its owner. A row
/// whose owning thread no longer exists is left out: nothing can call it.
pub(crate) async fn scheduled_services(
    db: &LocalDb,
    project_id: &str,
) -> Result<Vec<(ServiceOwner, String)>, String> {
    let project_id = project_id.to_string();
    db.read(move |conn| {
        let project_id = project_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT DISTINCT s.owner_thread, s.service, t.name FROM service_schedules s
                     LEFT JOIN threads t ON t.id = s.owner_thread
                     WHERE s.project_id = ?1
                     ORDER BY s.owner_thread, s.service",
                    params![project_id.as_str()],
                )
                .await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                let (owner, service, name) = (row.text(0)?, row.text(1)?, row.opt_text(2)?);
                let owner = match (owner.is_empty(), name) {
                    (true, _) => ServiceOwner::Project,
                    (false, Some(name)) => ServiceOwner::Thread { id: owner, name },
                    (false, None) => continue,
                };
                out.push((owner, service));
            }
            Ok(out)
        })
    })
    .await
    .map_err(|error| format!("Failed to list scheduled services: {error}"))
}

/// Bring a service's rows in step with its declared `entries` (empty when the
/// service is no longer declared). A new entry's first occurrence is one
/// interval after it was first seen; an unchanged entry keeps its anchor and
/// its history. Answers whether the service has any entry left.
pub(crate) async fn sync(
    db: &LocalDb,
    project_id: &str,
    owner: &ServiceOwner,
    service: &str,
    entries: &[ScheduleEntry],
    now: i64,
) -> Result<bool, String> {
    let declared: Vec<(String, String, String, i64)> = entries
        .iter()
        .map(|entry| {
            (
                entry.key(),
                entry.method.clone(),
                entry.args.to_string(),
                entry.every_ms,
            )
        })
        .collect();
    let (project_id, owner, service) = (
        project_id.to_string(),
        owner.key().to_string(),
        service.to_string(),
    );
    db.write(move |conn| {
        let (project_id, owner, service, declared) = (
            project_id.clone(),
            owner.clone(),
            service.clone(),
            declared.clone(),
        );
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT entry_key FROM service_schedules
                     WHERE project_id = ?1 AND owner_thread = ?2 AND service = ?3",
                    params![project_id.as_str(), owner.as_str(), service.as_str()],
                )
                .await?;
            let mut stored = Vec::new();
            while let Some(row) = rows.next().await? {
                stored.push(row.text(0)?);
            }
            drop(rows);
            for key in stored
                .iter()
                .filter(|key| !declared.iter().any(|(declared, ..)| declared == *key))
            {
                conn.execute(
                    "DELETE FROM service_schedules
                     WHERE project_id = ?1 AND owner_thread = ?2 AND service = ?3 AND entry_key = ?4",
                    params![project_id.as_str(), owner.as_str(), service.as_str(), key.as_str()],
                )
                .await?;
            }
            for (key, method, args, every_ms) in &declared {
                if stored.contains(key) {
                    continue;
                }
                let anchor_at = now + (every_ms + 999) / 1_000;
                conn.execute(
                    "INSERT INTO service_schedules
                     (project_id, owner_thread, service, entry_key, method, args_json, every_ms,
                      anchor_at, created_at, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?9)",
                    params![
                        project_id.as_str(),
                        owner.as_str(),
                        service.as_str(),
                        key.as_str(),
                        method.as_str(),
                        args.as_str(),
                        *every_ms,
                        anchor_at,
                        now
                    ],
                )
                .await?;
            }
            Ok(!declared.is_empty())
        })
    })
    .await
    .map_err(|error| format!("Failed to record the service's schedule: {error}"))
}

/// Drop the rows of every service `owner` no longer declares, including one
/// removed while no runner was up to see it go.
pub(crate) async fn retain_services(
    db: &LocalDb,
    project_id: &str,
    owner: &ServiceOwner,
    declared: &[String],
) -> Result<(), String> {
    let (project_id, owner, declared) = (
        project_id.to_string(),
        owner.key().to_string(),
        declared.to_vec(),
    );
    db.write(move |conn| {
        let (project_id, owner, declared) = (project_id.clone(), owner.clone(), declared.clone());
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT DISTINCT service FROM service_schedules
                     WHERE project_id = ?1 AND owner_thread = ?2",
                    params![project_id.as_str(), owner.as_str()],
                )
                .await?;
            let mut gone = Vec::new();
            while let Some(row) = rows.next().await? {
                let service = row.text(0)?;
                if !declared.contains(&service) {
                    gone.push(service);
                }
            }
            drop(rows);
            for service in gone {
                conn.execute(
                    "DELETE FROM service_schedules
                     WHERE project_id = ?1 AND owner_thread = ?2 AND service = ?3",
                    params![project_id.as_str(), owner.as_str(), service.as_str()],
                )
                .await?;
            }
            Ok(())
        })
    })
    .await
    .map_err(|error| format!("Failed to prune service schedules: {error}"))
}

/// Claim occurrence `due` of one entry. True when this caller now owns it:
/// the claim is durable before the call is made.
pub(crate) async fn claim(
    db: &LocalDb,
    project_id: &str,
    owner: &ServiceOwner,
    service: &str,
    entry_key: &str,
    due: i64,
) -> Result<bool, String> {
    let key = (
        project_id.to_string(),
        owner.key().to_string(),
        service.to_string(),
        entry_key.to_string(),
    );
    db.write(move |conn| {
        let (project_id, owner, service, entry_key) = key.clone();
        Box::pin(async move {
            let changed = conn
                .execute(
                    "UPDATE service_schedules SET last_fired_occurrence_at = ?5, updated_at = ?5
                     WHERE project_id = ?1 AND owner_thread = ?2 AND service = ?3 AND entry_key = ?4
                       AND (last_fired_occurrence_at IS NULL OR last_fired_occurrence_at < ?5)",
                    params![
                        project_id.as_str(),
                        owner.as_str(),
                        service.as_str(),
                        entry_key.as_str(),
                        due
                    ],
                )
                .await?;
            Ok(changed == 1)
        })
    })
    .await
    .map_err(|error| format!("Failed to claim a scheduled occurrence: {error}"))
}

/// Give back the claim on occurrence `due`, so it is due again, restoring
/// `prior` as the last occurrence fired. Only this claim is undone: a later
/// occurrence claimed since stands.
pub(crate) async fn unclaim(
    db: &LocalDb,
    project_id: &str,
    owner: &ServiceOwner,
    service: &str,
    entry_key: &str,
    due: i64,
    prior: Option<i64>,
) -> Result<(), String> {
    let key = (
        project_id.to_string(),
        owner.key().to_string(),
        service.to_string(),
        entry_key.to_string(),
    );
    db.write(move |conn| {
        let (project_id, owner, service, entry_key) = key.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE service_schedules SET last_fired_occurrence_at = ?6
                 WHERE project_id = ?1 AND owner_thread = ?2 AND service = ?3 AND entry_key = ?4
                   AND last_fired_occurrence_at = ?5",
                params![
                    project_id.as_str(),
                    owner.as_str(),
                    service.as_str(),
                    entry_key.as_str(),
                    due,
                    prior
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|error| format!("Failed to release a scheduled occurrence: {error}"))
}

/// How an occurrence ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Outcome {
    Ok,
    Failed(String),
    /// Not called: the service was stopped explicitly. Neither a success nor a
    /// failure, so a streak neither ends nor grows.
    Skipped(String),
}

/// Record an occurrence's outcome. Answers whether it began a failure streak:
/// a failure after a success, or the first one ever.
pub(crate) async fn settle(
    db: &LocalDb,
    project_id: &str,
    owner: &ServiceOwner,
    service: &str,
    entry_key: &str,
    outcome: &Outcome,
    now: i64,
) -> Result<bool, String> {
    let key = (
        project_id.to_string(),
        owner.key().to_string(),
        service.to_string(),
        entry_key.to_string(),
    );
    let (status, error) = match outcome {
        Outcome::Ok => ("ok", None),
        Outcome::Failed(error) => ("failed", Some(error.clone())),
        Outcome::Skipped(reason) => ("skipped", Some(reason.clone())),
    };
    db.write(move |conn| {
        let (project_id, owner, service, entry_key) = key.clone();
        let error = error.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT consecutive_failures FROM service_schedules
                     WHERE project_id = ?1 AND owner_thread = ?2 AND service = ?3 AND entry_key = ?4",
                    params![
                        project_id.as_str(),
                        owner.as_str(),
                        service.as_str(),
                        entry_key.as_str()
                    ],
                )
                .await?;
            let Some(prior) = rows.next().await?.map(|row| row.i64(0)).transpose()? else {
                // The entry was removed while its call ran.
                return Ok(false);
            };
            drop(rows);
            let failures = match status {
                "ok" => 0,
                "failed" => prior + 1,
                _ => prior,
            };
            conn.execute(
                "UPDATE service_schedules
                 SET last_run_at = ?5, last_status = ?6, last_error = ?7,
                     consecutive_failures = ?8, updated_at = ?5
                 WHERE project_id = ?1 AND owner_thread = ?2 AND service = ?3 AND entry_key = ?4",
                params![
                    project_id.as_str(),
                    owner.as_str(),
                    service.as_str(),
                    entry_key.as_str(),
                    now,
                    status,
                    error.as_deref(),
                    failures
                ],
            )
            .await?;
            Ok(status == "failed" && prior == 0)
        })
    })
    .await
    .map_err(|error| format!("Failed to record a scheduled outcome: {error}"))
}

/// The text of the one notice a failure streak sends its owning thread.
pub(crate) fn streak_notice(
    home: &str,
    service: &str,
    row: &ScheduleRow,
    due: i64,
    error: &str,
) -> String {
    let at = chrono::DateTime::from_timestamp(due, 0)
        .map(|at| at.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| due.to_string());
    let mut quoted: String = error.chars().take(NOTICE_ERROR_CHARS).collect();
    if error.chars().nth(NOTICE_ERROR_CHARS).is_some() {
        quoted.push('…');
    }
    format!(
        "Service `{service}`'s scheduled `{method}` (every {every}) started failing at the \
         {at} occurrence: {quoted}\n\nThis is the only notice for this failure streak: later \
         failures are recorded in {home}/log and on {home}/status without waking you, and the \
         next success ends the streak.",
        method = row.method,
        every = every_text(row.every_ms),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(method: &str, every_ms: i64) -> ScheduleEntry {
        ScheduleEntry {
            every: every_text(every_ms),
            every_ms,
            method: method.into(),
            args: Value::Null,
        }
    }

    #[test]
    fn intervals_read_as_written() {
        assert_eq!(every_text(15 * 60_000), "15m");
        assert_eq!(every_text(3_600_000), "1h");
        assert_eq!(every_text(2 * 86_400_000), "2d");
        assert_eq!(every_text(90 * 60_000), "90m");
    }

    /// Syncing keeps an unchanged entry's anchor and history, anchors a new
    /// one an interval out, and drops an entry no longer declared.
    #[tokio::test]
    async fn sync_keeps_cadence_across_declarations() {
        let db = crate::storage::migrated_test_db("service-schedules.db").await;
        crate::project_services::testing::seed_project(&db).await;
        let owner = ServiceOwner::Project;
        let refresh = entry("refresh", 15 * 60_000);
        assert!(sync(
            &db,
            "p",
            &owner,
            "board",
            std::slice::from_ref(&refresh),
            1_000
        )
        .await
        .unwrap());
        let rows = list(&db, "p", &owner, "board").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].anchor_at, 1_000 + 900);
        assert_eq!(rows[0].due_at(1_899), None);
        assert_eq!(rows[0].due_at(1_900), Some(1_900));
        assert_eq!(rows[0].next_due_at(1_900), Some(2_800));

        assert!(claim(&db, "p", &owner, "board", &rows[0].entry_key, 1_900)
            .await
            .unwrap());
        assert!(
            !claim(&db, "p", &owner, "board", &rows[0].entry_key, 1_900)
                .await
                .unwrap(),
            "an occurrence is claimed once"
        );

        // A second entry joins; the first keeps its anchor and claim.
        let prune = entry("prune", 86_400_000);
        sync(
            &db,
            "p",
            &owner,
            "board",
            &[prune.clone(), refresh.clone()],
            5_000,
        )
        .await
        .unwrap();
        let rows = list(&db, "p", &owner, "board").await.unwrap();
        let kept = rows.iter().find(|row| row.method == "refresh").unwrap();
        assert_eq!(kept.anchor_at, 1_900);
        assert_eq!(kept.last_fired_occurrence_at, Some(1_900));
        let added = rows.iter().find(|row| row.method == "prune").unwrap();
        assert_eq!(added.anchor_at, 5_000 + 86_400);

        // An entry gone from the declaration goes; a service gone goes whole.
        sync(&db, "p", &owner, "board", &[prune], 6_000)
            .await
            .unwrap();
        assert_eq!(list(&db, "p", &owner, "board").await.unwrap().len(), 1);
        retain_services(&db, "p", &owner, &[]).await.unwrap();
        assert!(list(&db, "p", &owner, "board").await.unwrap().is_empty());
    }

    /// The first failure begins a streak; the next does not; a success ends
    /// it; a skip neither grows nor ends it.
    #[tokio::test]
    async fn a_streak_begins_once() {
        let db = crate::storage::migrated_test_db("service-schedule-streak.db").await;
        crate::project_services::testing::seed_project(&db).await;
        let owner = ServiceOwner::Project;
        sync(&db, "p", &owner, "board", &[entry("tick", 5 * 60_000)], 0)
            .await
            .unwrap();
        let key = list(&db, "p", &owner, "board").await.unwrap()[0]
            .entry_key
            .clone();
        let settle_as = |outcome: Outcome, now: i64| {
            let (db, owner, key) = (&db, owner.clone(), key.clone());
            async move {
                settle(db, "p", &owner, "board", &key, &outcome, now)
                    .await
                    .unwrap()
            }
        };
        assert!(settle_as(Outcome::Failed("boom".into()), 300).await);
        assert!(!settle_as(Outcome::Failed("boom".into()), 600).await);
        assert!(!settle_as(Outcome::Skipped("stopped".into()), 900).await);
        let row = &list(&db, "p", &owner, "board").await.unwrap()[0];
        assert_eq!(row.consecutive_failures, 2);
        assert_eq!(row.last_status.as_deref(), Some("skipped"));
        assert!(!settle_as(Outcome::Ok, 1_200).await);
        assert!(settle_as(Outcome::Failed("again".into()), 1_500).await);
    }
}
