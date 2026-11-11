//! Scheduled methods: the `schedule` a service's manifest declares, called by
//! the supervisor on a fixed cadence as the service's owner, with no turn.
//!
//! A thread runs code without a turn only through a service (tenet 1313).
//! On-demand code is a method call; scheduled code is a `schedule` entry
//! naming a method; wakes stay attention. So this module never starts a turn
//! and wakes the owner at most once per failure streak ([`super::notify_owner`],
//! whose one other caller is a service whose always-on machine was lost).
//!
//! Streak notices coalesce. A notice opens a quiet window ([`NoticeWindow`],
//! [`NOTICE_WINDOW_SECS`]): a later streak inside it whose failure kind (the
//! call's error code) was already reported sends nothing, one of a new kind
//! sends one notice naming every kind seen, and the owner acting on the
//! service (a source or manifest write, or an explicit start, stop, or
//! restart) ends the window, so the failure after a fix attempt is news. A
//! substrate incident that alternates slow successes and failures then wakes
//! the owner once per kind rather than once per streak. Every occurrence is
//! still recorded on the row and in the log.
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
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::declaration::ScheduleEntry;
use super::ServiceOwner;
use crate::storage::{DbResult, LocalDb, RowExt};

/// Bound on how much of a failure the streak notice quotes; the service log
/// keeps the call row and the status keeps the full last error.
const NOTICE_ERROR_CHARS: usize = 1_500;

/// How long a streak notice keeps later streaks of the kinds it reported quiet.
pub(crate) const NOTICE_WINDOW_SECS: i64 = 24 * 60 * 60;

/// What one entry's failure notices have reported since the notice that
/// opened the window, and every streak and failed occurrence since, the quiet
/// ones included. Kept as the row's `notice_window` JSON, so a runner restart
/// neither reopens nor forgets it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct NoticeWindow {
    /// Epoch seconds the opening notice was sent.
    pub opened_at: i64,
    /// Failure kinds the window's notices named, in the order they were seen.
    pub kinds: Vec<String>,
    pub streaks: i64,
    pub failures: i64,
    /// The service's source when the window opened: a different one is the
    /// owner having rewritten it, its manifest included.
    pub source_hash: Option<String>,
    /// The owner explicitly started, stopped, or restarted the service.
    pub closed: bool,
}

impl NoticeWindow {
    pub(crate) fn closes_at(&self) -> i64 {
        self.opened_at + NOTICE_WINDOW_SECS
    }

    /// Whether a streak beginning now, under `source_hash`, stays quiet when
    /// its kind was already reported. An unknown source on either side is no
    /// evidence of a rewrite.
    pub(crate) fn quiet(&self, now: i64, source_hash: Option<&str>) -> bool {
        !self.closed
            && now < self.closes_at()
            && match (self.source_hash.as_deref(), source_hash) {
                (Some(then), Some(current)) => then == current,
                _ => true,
            }
    }
}

/// The notice an occurrence's outcome calls for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Notice {
    /// A streak began with no quiet window open, and opened `window`.
    /// `previous` is the window before it when that one held more than one
    /// streak, so the counts its quiet streaks gathered reach the owner.
    Streak {
        window: NoticeWindow,
        previous: Option<NoticeWindow>,
    },
    /// A streak began inside a quiet window with a kind it had not reported.
    NewKind { window: NoticeWindow },
}

/// The window once an occurrence has settled, and the notice it calls for.
/// `prior_failures` is the length of the streak before this occurrence.
fn next_window(
    prior_failures: i64,
    window: Option<NoticeWindow>,
    outcome: &Outcome,
    now: i64,
    source_hash: Option<&str>,
) -> (Option<NoticeWindow>, Option<Notice>) {
    let Outcome::Failed { kind, .. } = outcome else {
        return (window, None);
    };
    if prior_failures > 0 {
        return (
            window.map(|mut window| {
                window.failures += 1;
                window
            }),
            None,
        );
    }
    match window {
        Some(mut window) if window.quiet(now, source_hash) => {
            window.streaks += 1;
            window.failures += 1;
            if window.kinds.contains(kind) {
                return (Some(window), None);
            }
            window.kinds.push(kind.clone());
            (Some(window.clone()), Some(Notice::NewKind { window }))
        }
        previous => {
            let window = NoticeWindow {
                opened_at: now,
                kinds: vec![kind.clone()],
                streaks: 1,
                failures: 1,
                source_hash: source_hash.map(str::to_string),
                closed: false,
            };
            (
                Some(window.clone()),
                Some(Notice::Streak {
                    window,
                    previous: previous.filter(|previous| previous.streaks > 1),
                }),
            )
        }
    }
}

fn window_of(json: Option<String>) -> Option<NoticeWindow> {
    json.and_then(|json| serde_json::from_str(&json).ok())
}

fn window_json(window: &Option<NoticeWindow>) -> Option<String> {
    window
        .as_ref()
        .map(|window| serde_json::to_string(window).expect("a notice window serializes"))
}

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
    pub notice_window: Option<NoticeWindow>,
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
     last_run_at, last_status, last_error, consecutive_failures, notice_window";

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
        notice_window: window_of(row.opt_text(10)?),
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
    /// `kind` is the call's error code, which tells one cause of failure
    /// from another; `error` is the full text recorded.
    Failed {
        kind: String,
        error: String,
    },
    /// Not called: the service was stopped explicitly. Neither a success nor a
    /// failure, so a streak neither ends nor grows.
    Skipped(String),
}

/// Record an occurrence's outcome, under the service's current `source_hash`.
/// Answers the notice it calls for: one only when it began a failure streak
/// (a failure after a success, or the first one ever) that no quiet window
/// covers, or whose kind the window has not reported.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn settle(
    db: &LocalDb,
    project_id: &str,
    owner: &ServiceOwner,
    service: &str,
    entry_key: &str,
    outcome: &Outcome,
    source_hash: Option<&str>,
    now: i64,
) -> Result<Option<Notice>, String> {
    let key = (
        project_id.to_string(),
        owner.key().to_string(),
        service.to_string(),
        entry_key.to_string(),
    );
    let (status, error) = match outcome {
        Outcome::Ok => ("ok", None),
        Outcome::Failed { error, .. } => ("failed", Some(error.clone())),
        Outcome::Skipped(reason) => ("skipped", Some(reason.clone())),
    };
    let (outcome, source_hash) = (outcome.clone(), source_hash.map(str::to_string));
    db.write(move |conn| {
        let (project_id, owner, service, entry_key) = key.clone();
        let (error, outcome, source_hash) = (error.clone(), outcome.clone(), source_hash.clone());
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT consecutive_failures, notice_window FROM service_schedules
                     WHERE project_id = ?1 AND owner_thread = ?2 AND service = ?3 AND entry_key = ?4",
                    params![
                        project_id.as_str(),
                        owner.as_str(),
                        service.as_str(),
                        entry_key.as_str()
                    ],
                )
                .await?;
            let Some(row) = rows.next().await? else {
                // The entry was removed while its call ran.
                return Ok(None);
            };
            let (prior, window) = (row.i64(0)?, window_of(row.opt_text(1)?));
            drop(rows);
            let (window, notice) =
                next_window(prior, window, &outcome, now, source_hash.as_deref());
            let failures = match status {
                "ok" => 0,
                "failed" => prior + 1,
                _ => prior,
            };
            conn.execute(
                "UPDATE service_schedules
                 SET last_run_at = ?5, last_status = ?6, last_error = ?7,
                     consecutive_failures = ?8, notice_window = ?9, updated_at = ?5
                 WHERE project_id = ?1 AND owner_thread = ?2 AND service = ?3 AND entry_key = ?4",
                params![
                    project_id.as_str(),
                    owner.as_str(),
                    service.as_str(),
                    entry_key.as_str(),
                    now,
                    status,
                    error.as_deref(),
                    failures,
                    window_json(&window).as_deref()
                ],
            )
            .await?;
            Ok(notice)
        })
    })
    .await
    .map_err(|error| format!("Failed to record a scheduled outcome: {error}"))
}

/// The owner explicitly started, stopped, or restarted `service`: end every
/// quiet window its entries hold, so the next streak is news whatever its
/// kind. The windows keep their counts for the next notice to report.
pub(crate) async fn end_notice_windows(
    db: &LocalDb,
    project_id: &str,
    owner: &ServiceOwner,
    service: &str,
) -> Result<(), String> {
    let (project_id, owner, service) = (
        project_id.to_string(),
        owner.key().to_string(),
        service.to_string(),
    );
    db.write(move |conn| {
        let (project_id, owner, service) = (project_id.clone(), owner.clone(), service.clone());
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT entry_key, notice_window FROM service_schedules
                     WHERE project_id = ?1 AND owner_thread = ?2 AND service = ?3
                       AND notice_window IS NOT NULL",
                    params![project_id.as_str(), owner.as_str(), service.as_str()],
                )
                .await?;
            let mut open = Vec::new();
            while let Some(row) = rows.next().await? {
                if let Some(window) = window_of(row.opt_text(1)?).filter(|window| !window.closed) {
                    open.push((row.text(0)?, window));
                }
            }
            drop(rows);
            for (entry_key, mut window) in open {
                window.closed = true;
                conn.execute(
                    "UPDATE service_schedules SET notice_window = ?5
                     WHERE project_id = ?1 AND owner_thread = ?2 AND service = ?3 AND entry_key = ?4",
                    params![
                        project_id.as_str(),
                        owner.as_str(),
                        service.as_str(),
                        entry_key.as_str(),
                        window_json(&Some(window)).as_deref()
                    ],
                )
                .await?;
            }
            Ok(())
        })
    })
    .await
    .map_err(|error| format!("Failed to end a schedule's quiet window: {error}"))
}

fn utc(secs: i64) -> String {
    chrono::DateTime::from_timestamp(secs, 0)
        .map(|at| at.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| secs.to_string())
}

fn kinds_text(kinds: &[String]) -> String {
    kinds
        .iter()
        .map(|kind| format!("`{kind}`"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// A window's counts as a notice or the status page reports them.
pub(crate) fn window_summary(window: &NoticeWindow) -> String {
    format!(
        "{} streaks, {} failed occurrences since {} (kinds: {})",
        window.streaks,
        window.failures,
        utc(window.opened_at),
        kinds_text(&window.kinds)
    )
}

/// The text of the notice a failure streak sends its owning thread, for the
/// streak that began at occurrence `due` failing with `error`.
pub(crate) fn streak_notice(
    home: &str,
    service: &str,
    row: &ScheduleRow,
    due: i64,
    error: &str,
    notice: &Notice,
) -> String {
    let at = utc(due);
    let mut quoted: String = error.chars().take(NOTICE_ERROR_CHARS).collect();
    if error.chars().nth(NOTICE_ERROR_CHARS).is_some() {
        quoted.push('…');
    }
    let (method, every) = (&row.method, every_text(row.every_ms));
    let ends = "writing, starting, stopping, or restarting the service ends the window";
    match notice {
        Notice::Streak { window, previous } => {
            let mut text = format!(
                "Service `{service}`'s scheduled `{method}` (every {every}) started failing at \
                 the {at} occurrence: {quoted}\n\nThis is the only notice for this failure \
                 streak: later failures are recorded in {home}/log and on {home}/status without \
                 waking you, and the next success ends the streak. Until {closes}, a new streak \
                 that fails with {kinds} again is recorded the same way with no notice; a \
                 failure of another kind sends one, and {ends}.",
                closes = utc(window.closes_at()),
                kinds = kinds_text(&window.kinds),
            );
            if let Some(previous) = previous {
                text.push_str(&format!(
                    "\n\nThe window before this one held {}.",
                    window_summary(previous)
                ));
            }
            text
        }
        Notice::NewKind { window } => {
            let kind = window.kinds.last().map(String::as_str).unwrap_or_default();
            format!(
                "Service `{service}`'s scheduled `{method}` (every {every}) started failing \
                 again at the {at} occurrence, with a kind of failure not reported before, \
                 `{kind}`: {quoted}\n\nThis window holds {summary}. Until {closes}, a new \
                 streak of one of these kinds is recorded in {home}/log and on {home}/status \
                 with no notice; a failure of another kind sends one, and {ends}.",
                summary = window_summary(window),
                closes = utc(window.closes_at()),
            )
        }
    }
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

    fn failed(kind: &str) -> Outcome {
        Outcome::Failed {
            kind: kind.into(),
            error: format!("it failed ({kind})"),
        }
    }

    /// A `board` service with one `tick` entry every five minutes, and that
    /// entry's key.
    async fn board(name: &str) -> (LocalDb, String) {
        let db = crate::storage::migrated_test_db(name).await;
        crate::project_services::testing::seed_project(&db).await;
        let owner = ServiceOwner::Project;
        sync(&db, "p", &owner, "board", &[entry("tick", 5 * 60_000)], 0)
            .await
            .unwrap();
        let key = list(&db, "p", &owner, "board").await.unwrap()[0]
            .entry_key
            .clone();
        (db, key)
    }

    async fn settle_as(
        db: &LocalDb,
        key: &str,
        outcome: Outcome,
        source: &str,
        now: i64,
    ) -> Option<Notice> {
        settle(
            db,
            "p",
            &ServiceOwner::Project,
            "board",
            key,
            &outcome,
            Some(source),
            now,
        )
        .await
        .unwrap()
    }

    async fn board_row(db: &LocalDb) -> ScheduleRow {
        list(db, "p", &ServiceOwner::Project, "board")
            .await
            .unwrap()
            .remove(0)
    }

    /// The first failure begins a streak and notifies; the next does not; a
    /// skip neither grows nor ends it; a success ends it.
    #[tokio::test]
    async fn a_streak_notifies_once() {
        let (db, key) = board("service-schedule-streak.db").await;
        let first = settle_as(&db, &key, failed("deadline"), "v1", 300).await;
        assert!(
            matches!(first, Some(Notice::Streak { previous: None, .. })),
            "{first:?}"
        );
        assert_eq!(
            settle_as(&db, &key, failed("deadline"), "v1", 600).await,
            None
        );
        assert_eq!(
            settle_as(&db, &key, Outcome::Skipped("stopped".into()), "v1", 900).await,
            None
        );
        let row = board_row(&db).await;
        assert_eq!(row.consecutive_failures, 2);
        assert_eq!(row.last_status.as_deref(), Some("skipped"));
        assert_eq!(settle_as(&db, &key, Outcome::Ok, "v1", 1_200).await, None);
        assert_eq!(board_row(&db).await.consecutive_failures, 0);
    }

    /// Inside the window a new streak of a reported kind is quiet, a new kind
    /// notifies naming every kind seen, and the window counts every streak
    /// and every failed occurrence, the quiet ones included.
    #[tokio::test]
    async fn a_window_quiets_known_kinds_and_names_a_new_one() {
        let (db, key) = board("service-schedule-window.db").await;
        assert!(settle_as(&db, &key, failed("deadline"), "v1", 300)
            .await
            .is_some());
        settle_as(&db, &key, failed("deadline"), "v1", 600).await;
        settle_as(&db, &key, Outcome::Ok, "v1", 900).await;
        assert_eq!(
            settle_as(&db, &key, failed("deadline"), "v1", 1_200).await,
            None,
            "the same kind again is not news"
        );
        settle_as(&db, &key, Outcome::Ok, "v1", 1_500).await;
        let Some(Notice::NewKind { window }) =
            settle_as(&db, &key, failed("store"), "v1", 1_800).await
        else {
            panic!("a new kind is news");
        };
        assert_eq!(window.kinds, ["deadline", "store"]);
        assert_eq!((window.streaks, window.failures), (3, 4));
        assert_eq!(window.opened_at, 300);
        settle_as(&db, &key, Outcome::Ok, "v1", 2_100).await;
        assert_eq!(
            settle_as(&db, &key, failed("store"), "v1", 2_400).await,
            None
        );
        let row = board_row(&db).await;
        let window = row.notice_window.unwrap();
        assert_eq!((window.streaks, window.failures), (4, 5));
        assert_eq!(row.consecutive_failures, 1);
    }

    /// Past the window, after a rewrite, or after an explicit start, stop, or
    /// restart, a streak notifies afresh, carrying the counts of the window
    /// before it when that held more than one streak.
    #[tokio::test]
    async fn a_window_ends_with_time_a_rewrite_or_an_owner_action() {
        let (db, key) = board("service-schedule-window-end.db").await;
        settle_as(&db, &key, failed("deadline"), "v1", 300).await;
        settle_as(&db, &key, Outcome::Ok, "v1", 600).await;
        settle_as(&db, &key, failed("deadline"), "v1", 900).await;
        settle_as(&db, &key, Outcome::Ok, "v1", 1_200).await;

        let late = 300 + NOTICE_WINDOW_SECS;
        let Some(Notice::Streak { window, previous }) =
            settle_as(&db, &key, failed("deadline"), "v1", late).await
        else {
            panic!("a streak past the window is news");
        };
        assert_eq!((window.opened_at, window.streaks), (late, 1));
        let previous = previous.expect("the last window held two streaks");
        assert_eq!((previous.streaks, previous.failures), (2, 2));
        settle_as(&db, &key, Outcome::Ok, "v1", late + 300).await;

        // A manifest or source write changes the service's source hash.
        let rewritten = settle_as(&db, &key, failed("deadline"), "v2", late + 600).await;
        assert!(
            matches!(rewritten, Some(Notice::Streak { previous: None, .. })),
            "{rewritten:?}"
        );
        settle_as(&db, &key, Outcome::Ok, "v2", late + 900).await;
        assert_eq!(
            settle_as(&db, &key, failed("deadline"), "v2", late + 1_200).await,
            None
        );
        settle_as(&db, &key, Outcome::Ok, "v2", late + 1_500).await;

        end_notice_windows(&db, "p", &ServiceOwner::Project, "board")
            .await
            .unwrap();
        assert!(board_row(&db).await.notice_window.unwrap().closed);
        let Some(Notice::Streak { previous, .. }) =
            settle_as(&db, &key, failed("deadline"), "v2", late + 1_800).await
        else {
            panic!("a streak after the owner acted is news");
        };
        assert_eq!(previous.map(|previous| previous.streaks), Some(2));
    }

    #[test]
    fn a_new_kind_notice_names_the_kind_and_the_counts() {
        let row = ScheduleRow {
            entry_key: "k".into(),
            method: "refresh".into(),
            args: Value::Null,
            every_ms: 15 * 60_000,
            anchor_at: 0,
            last_fired_occurrence_at: None,
            last_run_at: None,
            last_status: None,
            last_error: None,
            consecutive_failures: 0,
            notice_window: None,
        };
        let window = NoticeWindow {
            opened_at: 0,
            kinds: vec!["deadline".into(), "store".into()],
            streaks: 3,
            failures: 5,
            source_hash: None,
            closed: false,
        };
        let text = streak_notice(
            "cairn://p/cairn/ux/services/releases",
            "releases",
            &row,
            3_600,
            "bearer expired (store)",
            &Notice::NewKind { window },
        );
        assert!(text.contains("not reported before, `store`"), "{text}");
        assert!(
            text.contains("3 streaks, 5 failed occurrences since 1970-01-01 00:00:00 UTC"),
            "{text}"
        );
        assert!(text.contains("(kinds: `deadline`, `store`)"), "{text}");
        assert!(text.contains("Until 1970-01-02 00:00:00 UTC"), "{text}");
    }
}
