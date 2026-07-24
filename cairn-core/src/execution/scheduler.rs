//! Live recipe scheduler.
//!
//! Owned by the always-on hosts (`cairn-runner` and `cairn-server`), never the
//! thin desktop app. Each scheduled fire creates a fresh issue and starts the
//! recipe as that issue's execution, stamped `triggered_by = schedule`, so every
//! scheduled run gets the normal visibility surface (issue list, execution
//! timeline, attention).
//!
//! The engine:
//! - Loads schedule-triggered recipes from YAML files (workspace + per-project).
//! - Computes each recipe's next fire time from its [`ScheduleConfig`], resolved
//!   against the OS timezone at fire time so wall-clock schedules track DST.
//! - Uses a bounded sleep (`min(soonest_fire - now, 60s)`): waking at the cap
//!   just recomputes, so recipe create/edit/delete is picked up within a minute
//!   with no cross-crate plumbing; waking at the fire instant fires.
//! - Dedupes restart-safely off the executions table itself
//!   (`MAX(started_at) WHERE recipe_id = ? AND triggered_by = 'schedule'`) —
//!   no separate marker table or migration.

use std::path::PathBuf;

use chrono::{DateTime, Datelike, Days, Duration, LocalResult, NaiveDate, TimeZone, Timelike, Utc};
use chrono_tz::Tz;

use crate::config::recipes as config_recipes;
use crate::config::ConfigResult;
use crate::models::{
    RecipeNode, RecipeTrigger, ScheduleAt, ScheduleConfig, ScheduleEvery, SchedulePeriod,
    TriggerType,
};
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};

/// Upper bound on a single loop sleep. Waking at the cap recomputes all next-fire
/// times, so recipe create/edit/delete lands within a minute without a poke
/// channel back into core.
const MAX_SLEEP: std::time::Duration = std::time::Duration::from_secs(60);

/// A schedule-triggered recipe resolved to everything the loop needs to fire it.
struct ScheduledRecipe {
    id: String,
    name: String,
    /// Owning project id — resolves the execution's owning database.
    project_id: String,
    /// Owning project key — resolves the issue-creation owning database.
    project_key: String,
    /// Repo path of the owning project, used to reload the recipe at fire time.
    project_path: PathBuf,
    schedule_config: ScheduleConfig,
}

// ===========================================================================
// Next-fire math (ported verbatim from the pre-runner-split engine)
// ===========================================================================

/// Calculate the next fire time for a schedule configuration.
fn calculate_next_fire(
    config: &ScheduleConfig,
    timezone: &Tz,
    last_execution: Option<i64>,
) -> Option<DateTime<Utc>> {
    let now = Utc::now();
    let now_tz = now.with_timezone(timezone);

    match &config.every {
        ScheduleEvery::Period(period) => {
            let at = config.at.as_ref()?;
            calculate_period_next_fire(period, at, &now_tz, timezone)
        }
        ScheduleEvery::Interval(interval) => {
            calculate_interval_next_fire(interval, &now, last_execution)
        }
    }
}

/// Calculate next fire time for a period-based schedule.
fn calculate_period_next_fire(
    period: &SchedulePeriod,
    at: &ScheduleAt,
    now_tz: &DateTime<Tz>,
    timezone: &Tz,
) -> Option<DateTime<Utc>> {
    match period {
        SchedulePeriod::Hour => {
            // Run at :MM past every hour
            let mut date = now_tz.date_naive();
            let mut hour = now_tz.hour();
            let mut next = resolve_local_datetime(timezone, date, hour, at.minute)?;

            // If we're past this minute in the current hour, move to next hour
            if next <= *now_tz {
                if hour == 23 {
                    date = date.checked_add_days(Days::new(1))?;
                    hour = 0;
                } else {
                    hour += 1;
                }
                next = resolve_local_datetime(timezone, date, hour, at.minute)?;
            }

            Some(next.with_timezone(&Utc))
        }
        SchedulePeriod::Day => {
            // Run at HH:MM every day
            let mut date = now_tz.date_naive();
            let mut next = resolve_local_datetime(timezone, date, at.hour, at.minute)?;

            // If we're past this time today, move to tomorrow in local calendar
            // space. Adding an absolute 24-hour duration would drift wall-clock
            // schedules across DST transitions.
            if next <= *now_tz {
                date = date.checked_add_days(Days::new(1))?;
                next = resolve_local_datetime(timezone, date, at.hour, at.minute)?;
            }

            Some(next.with_timezone(&Utc))
        }
        SchedulePeriod::Weekday => {
            // Run at HH:MM Monday through Friday.
            let mut date = now_tz.date_naive();
            loop {
                let next = resolve_local_datetime(timezone, date, at.hour, at.minute)?;
                let weekday = next.weekday().num_days_from_monday();
                if weekday < 5 && next > *now_tz {
                    return Some(next.with_timezone(&Utc));
                }
                date = date.checked_add_days(Days::new(1))?;
            }
        }
        SchedulePeriod::Week => {
            // Run at HH:MM on specific day of week
            let target_weekday = at.day.unwrap_or(1); // 0=Sun, 1=Mon, etc.

            // Find next occurrence of target weekday
            let current_weekday = now_tz.weekday().num_days_from_sunday();
            let mut days_until = if target_weekday > current_weekday {
                target_weekday - current_weekday
            } else if target_weekday == current_weekday {
                0
            } else {
                7 - current_weekday + target_weekday
            };

            let mut date = now_tz
                .date_naive()
                .checked_add_days(Days::new(days_until as u64))?;
            let mut next = resolve_local_datetime(timezone, date, at.hour, at.minute)?;

            if next <= *now_tz {
                days_until += 7;
                date = now_tz
                    .date_naive()
                    .checked_add_days(Days::new(days_until as u64))?;
                next = resolve_local_datetime(timezone, date, at.hour, at.minute)?;
            }

            Some(next.with_timezone(&Utc))
        }
        SchedulePeriod::Month => {
            // Run at HH:MM on specific day of month
            let target_day = at.day.unwrap_or(1);
            let mut date = date_in_month(now_tz.year(), now_tz.month(), target_day)?;
            let mut next = resolve_local_datetime(timezone, date, at.hour, at.minute)?;

            // If we're past this time this month, move to next month in local
            // calendar space to preserve the requested wall-clock time across
            // DST transitions.
            if next <= *now_tz {
                let (year, month) = next_month(now_tz.year(), now_tz.month());
                date = date_in_month(year, month, target_day)?;
                next = resolve_local_datetime(timezone, date, at.hour, at.minute)?;
            }

            Some(next.with_timezone(&Utc))
        }
    }
}

fn resolve_local_datetime(
    timezone: &Tz,
    date: NaiveDate,
    hour: u32,
    minute: u32,
) -> Option<DateTime<Tz>> {
    match timezone.with_ymd_and_hms(date.year(), date.month(), date.day(), hour, minute, 0) {
        LocalResult::Single(dt) => Some(dt),
        // During fall-back, pick the first occurrence of the requested wall time
        // so a schedule fires once for that local clock label.
        LocalResult::Ambiguous(earliest, _) => Some(earliest),
        // During spring-forward, the requested wall time may not exist. Move
        // forward to the first valid local minute after the gap.
        LocalResult::None => {
            let local = date.and_hms_opt(hour, minute, 0)?;
            (1..=180).find_map(|minutes| {
                let candidate = local + Duration::minutes(minutes);
                timezone.from_local_datetime(&candidate).earliest()
            })
        }
    }
}

fn date_in_month(year: i32, month: u32, target_day: u32) -> Option<NaiveDate> {
    let last_day = last_day_of_month(year, month)?;
    NaiveDate::from_ymd_opt(year, month, target_day.min(last_day))
}

fn last_day_of_month(year: i32, month: u32) -> Option<u32> {
    let (next_year, next_month) = next_month(year, month);
    let first_of_next = NaiveDate::from_ymd_opt(next_year, next_month, 1)?;
    Some(first_of_next.pred_opt()?.day())
}

fn next_month(year: i32, month: u32) -> (i32, u32) {
    if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    }
}

/// Calculate next fire time for an interval-based schedule.
fn calculate_interval_next_fire(
    interval: &crate::models::ScheduleInterval,
    now: &DateTime<Utc>,
    last_execution: Option<i64>,
) -> Option<DateTime<Utc>> {
    let total_seconds = (interval.days as i64 * 86400)
        + (interval.hours as i64 * 3600)
        + (interval.minutes as i64 * 60);

    if total_seconds == 0 {
        return None;
    }

    let base_time = if let Some(last) = last_execution {
        DateTime::<Utc>::from_timestamp(last, 0)?
    } else {
        // First execution - schedule for now + interval
        *now
    };

    let next = base_time + Duration::seconds(total_seconds);

    // If next is in the past, calculate from now
    if next <= *now {
        Some(*now + Duration::seconds(total_seconds))
    } else {
        Some(next)
    }
}

/// Get the OS timezone as a named IANA timezone.
fn get_timezone() -> Tz {
    iana_time_zone::get_timezone()
        .ok()
        .and_then(|timezone| timezone.parse().ok())
        .unwrap_or(chrono_tz::UTC)
}

// ===========================================================================
// Loading, dedupe, and the fire path
// ===========================================================================

/// Load all schedule-triggered recipes that have a project home.
///
/// Workspace-scoped scheduled recipes are skipped with a warning: a scheduled
/// fire creates an issue, and an issue needs a project. Per-project recipes are
/// discovered by walking each project's repo path (from the private `projects`
/// table — team projects whose repo path is only known to a replica are a known
/// limitation, see the module's issue).
async fn load_scheduled_recipes(orch: &Orchestrator) -> Vec<ScheduledRecipe> {
    let mut result = Vec::new();
    let config_dir = &orch.config_dir;

    // Workspace-scoped scheduled recipes have no project home — warn and skip.
    if let Ok(ws_recipes) = config_recipes::list_recipes(config_dir, None) {
        for recipe_result in ws_recipes {
            if let ConfigResult::Ok(fr) = recipe_result {
                if fr.recipe.trigger == RecipeTrigger::Schedule
                    && fr.recipe.child_recipe_id.is_none()
                {
                    log::warn!(
                        "scheduler: skipping workspace-scoped scheduled recipe '{}' \
                         — a scheduled fire needs a project home",
                        fr.recipe.id
                    );
                }
            }
        }
    }

    // Per-project scheduled recipes: one issue + execution per fire.
    for (project_id, project_key, repo_path) in load_projects(&orch.db.local).await {
        let project_path = PathBuf::from(&repo_path);
        let Ok(proj_recipes) = config_recipes::list_recipes(config_dir, Some(&project_path)) else {
            continue;
        };
        for recipe_result in proj_recipes {
            let ConfigResult::Ok(fr) = recipe_result else {
                continue;
            };
            if fr.recipe.trigger != RecipeTrigger::Schedule
                || fr.recipe.child_recipe_id.is_some()
                || !fr.is_project_scoped
            {
                continue;
            }
            let Some(schedule_config) = schedule_config_of(&fr.recipe.nodes) else {
                continue;
            };
            result.push(ScheduledRecipe {
                id: fr.recipe.id,
                name: fr.recipe.name,
                project_id: project_id.clone(),
                project_key: project_key.clone(),
                project_path: project_path.clone(),
                schedule_config,
            });
        }
    }

    result
}

/// Extract the schedule config from a recipe's schedule-trigger node.
fn schedule_config_of(nodes: &[RecipeNode]) -> Option<ScheduleConfig> {
    nodes.iter().find_map(|node| {
        node.trigger_config.as_ref().and_then(|tc| {
            (tc.trigger_type == RecipeTrigger::Schedule)
                .then(|| tc.schedule_config.clone())
                .flatten()
        })
    })
}

/// Read `(id, key, repo_path)` for every project in the private database.
async fn load_projects(db: &LocalDb) -> Vec<(String, String, String)> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query("SELECT id, key, repo_path FROM projects", ())
                .await?;
            let mut projects = Vec::new();
            while let Some(row) = rows.next().await? {
                projects.push((row.text(0)?, row.text(1)?, row.text(2)?));
            }
            Ok(projects)
        })
    })
    .await
    .unwrap_or_default()
}

/// Timestamp of this recipe's most recent scheduled fire, from the executions
/// table in the project's owning database. This both seeds interval schedules
/// and makes firing restart-safe.
async fn last_scheduled_fire(orch: &Orchestrator, recipe: &ScheduledRecipe) -> Option<i64> {
    let db = orch.db.for_project(&recipe.project_key).await;
    // Scope to the owning project via the issue join: every scheduled execution
    // is issue-attached with `project_id` NULL, and all local projects share the
    // private DB, so an unscoped `recipe_id` match would let same-named recipes
    // in different projects (recipe ids are filenames — `nightly`, `daily`, …)
    // read each other's last fire and suppress or delay one another.
    db.query_opt_i64(
        "SELECT MAX(e.started_at) FROM executions e \
         JOIN issues i ON i.id = e.issue_id \
         WHERE e.recipe_id = ?1 AND e.triggered_by = 'schedule' AND i.project_id = ?2",
        (recipe.id.clone(), recipe.project_id.clone()),
    )
    .await
    .ok()
    .flatten()
}

/// The recipes in a computed batch whose fire instant is at or before `wake`.
/// Firing the whole due set — not just the single soonest recipe — is what keeps
/// recipes tied at the same instant (e.g. two dailies both at 09:00) from
/// starving each other: firing only the first would leave the rest to recompute
/// from a now-later clock and slip to their next period, every cycle.
fn due_at(
    next_fires: &[(ScheduledRecipe, DateTime<Utc>)],
    wake: DateTime<Utc>,
) -> Vec<&ScheduledRecipe> {
    next_fires
        .iter()
        .filter(|(_, fire)| *fire <= wake)
        .map(|(recipe, _)| recipe)
        .collect()
}

/// The scheduler loop. Each iteration recomputes every recipe's next fire time,
/// then sleeps `min(soonest_fire - now, 60s)`. A capped wake just recomputes
/// (picking up recipe edits); a wake at the fire instant fires the due recipe.
///
/// Restart-safe and double-fire-safe via the executions-table dedupe in
/// [`last_scheduled_fire`]: a fired schedule writes an execution row whose
/// `started_at` moves the next computed fire into the future.
async fn run_scheduler_loop(orch: Orchestrator) {
    log::info!("Recipe scheduler started");

    loop {
        // Resolve the OS timezone each iteration so wall-clock schedules track
        // timezone changes and DST transitions.
        let timezone = get_timezone();
        let recipes = load_scheduled_recipes(&orch).await;
        let now = Utc::now();

        // Compute every recipe's next fire time up front and keep the WHOLE
        // batch, not just the soonest — recipes tied at the same instant must all
        // fire on the same wake (see `due_at`).
        let mut next_fires: Vec<(ScheduledRecipe, DateTime<Utc>)> = Vec::new();
        for recipe in recipes {
            let last = last_scheduled_fire(&orch, &recipe).await;
            if let Some(next) = calculate_next_fire(&recipe.schedule_config, &timezone, last) {
                next_fires.push((recipe, next));
            }
        }

        let soonest = next_fires.iter().map(|(_, fire)| *fire).min();
        let sleep_dur = match soonest {
            Some(fire) => (fire - now)
                .to_std()
                .unwrap_or(std::time::Duration::ZERO)
                .min(MAX_SLEEP),
            None => MAX_SLEEP,
        };
        tokio::time::sleep(sleep_dur).await;

        // Fire every recipe whose computed instant has now arrived. A capped wake
        // (soonest still in the future) yields an empty due set and just
        // recomputes on the next iteration.
        let wake = Utc::now();
        let mut fire_error = false;
        for recipe in due_at(&next_fires, wake) {
            log::info!(
                "Firing scheduled recipe '{}' for project {}",
                recipe.id,
                recipe.project_key
            );
            if let Err(error) = fire_scheduled_recipe(&orch, recipe).await {
                log::error!("Failed to fire scheduled recipe '{}': {}", recipe.id, error);
                fire_error = true;
            }
        }
        if fire_error {
            // A failed fire writes no execution row, so its dedupe lookup still
            // returns the same due time; cool down a full interval so a
            // persistently failing recipe cannot hot-loop.
            tokio::time::sleep(MAX_SLEEP).await;
        }
    }
}

/// Create a fresh issue for one scheduled fire and start the recipe as that
/// issue's execution, stamped `triggered_by = schedule`.
async fn fire_scheduled_recipe(
    orch: &Orchestrator,
    recipe: &ScheduledRecipe,
) -> Result<(), String> {
    // Re-verify the recipe still schedules — it may have been edited during the
    // bounded sleep. A recipe whose trigger changed (or which vanished) is
    // silently dropped this cycle; the next load reflects the edit.
    match config_recipes::get_recipe(&orch.config_dir, &recipe.id, Some(&recipe.project_path)) {
        Ok(Some(fr)) if fr.recipe.trigger == RecipeTrigger::Schedule => {}
        Ok(_) => return Ok(()),
        Err(e) => return Err(format!("failed to reload recipe '{}': {e}", recipe.id)),
    }

    // Human-readable fire-time label in the OS-local timezone.
    let stamp = Utc::now()
        .with_timezone(&get_timezone())
        .format("%Y-%m-%d %H:%M")
        .to_string();
    let title = format!("{} — {}", recipe.name, stamp);
    let description = format!(
        "Scheduled run of recipe '{}' ({}).",
        recipe.name,
        describe_schedule(&recipe.schedule_config)
    );

    // Canonical issue-creation path (durable issue + embed/sync/db-change side
    // effects). `execution: None` — the execution is started below through the
    // trigger-typed path so it is stamped `schedule`, not `manual`.
    let outcome = crate::mcp::handlers::issues::create_issue_in_project(
        orch,
        &recipe.project_key,
        title,
        Some(description),
        None,
        None,
        None,
        None,
    )
    .await?;

    crate::execution::recipe::start_recipe_execution_and_advance(
        orch,
        &outcome.issue_id,
        Some(&recipe.id),
        &recipe.project_id,
        None,
        None,
        TriggerType::Schedule,
    )?;

    // Wake any in-flight `watch` on the issue, mirroring the executions-collection
    // start path.
    orch.wake_for_issue(&outcome.issue_id).await;
    Ok(())
}

/// Render a short human description of a schedule for the issue body.
fn describe_schedule(config: &ScheduleConfig) -> String {
    match &config.every {
        ScheduleEvery::Period(period) => {
            let period_label = match period {
                SchedulePeriod::Hour => "every hour",
                SchedulePeriod::Day => "every day",
                SchedulePeriod::Weekday => "every weekday",
                SchedulePeriod::Week => "every week",
                SchedulePeriod::Month => "every month",
            };
            match &config.at {
                Some(at) => format!("{period_label} at {:02}:{:02}", at.hour, at.minute),
                None => period_label.to_string(),
            }
        }
        ScheduleEvery::Interval(interval) => {
            let mut parts = Vec::new();
            if interval.days > 0 {
                parts.push(format!("{}d", interval.days));
            }
            if interval.hours > 0 {
                parts.push(format!("{}h", interval.hours));
            }
            if interval.minutes > 0 {
                parts.push(format!("{}m", interval.minutes));
            }
            if parts.is_empty() {
                "on an interval".to_string()
            } else {
                format!("every {}", parts.join(" "))
            }
        }
    }
}

/// Spawn the scheduler as a detached background loop. Called by the always-on
/// hosts (runner and non-inert server), never the thin desktop app.
pub(crate) fn spawn_recipe_scheduler(orch: Orchestrator) {
    tokio::spawn(run_scheduler_loop(orch));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{ScheduleAt, ScheduleInterval, SchedulePeriod};

    #[test]
    fn test_calculate_interval_next_fire() {
        let now = Utc::now();
        let interval = ScheduleInterval {
            days: 0,
            hours: 0,
            minutes: 30,
        };

        let next = calculate_interval_next_fire(&interval, &now, None);
        assert!(next.is_some());
        let next = next.unwrap();
        assert!(next > now);
        assert!(next - now < Duration::minutes(31));
    }

    #[test]
    fn test_calculate_period_next_fire_hour() {
        let timezone = chrono_tz::UTC;
        let now = Utc::now().with_timezone(&timezone);
        let at = ScheduleAt {
            day: None,
            hour: 0,
            minute: 30,
        };

        let next = calculate_period_next_fire(&SchedulePeriod::Hour, &at, &now, &timezone);
        assert!(next.is_some());
        let next = next.unwrap();
        assert!(next > Utc::now());
    }

    #[test]
    fn test_calculate_period_next_fire_day() {
        let timezone = chrono_tz::UTC;
        let now = Utc::now().with_timezone(&timezone);
        let at = ScheduleAt {
            day: None,
            hour: 9,
            minute: 0,
        };

        let next = calculate_period_next_fire(&SchedulePeriod::Day, &at, &now, &timezone);
        assert!(next.is_some());
        let next = next.unwrap();
        assert!(next > Utc::now());
    }

    #[test]
    fn daily_period_preserves_wall_clock_time_across_spring_dst() {
        let timezone = chrono_tz::America::Los_Angeles;
        let now = timezone
            .with_ymd_and_hms(2026, 3, 7, 10, 0, 0)
            .single()
            .unwrap();
        let at = ScheduleAt {
            day: None,
            hour: 9,
            minute: 0,
        };

        let next = calculate_period_next_fire(&SchedulePeriod::Day, &at, &now, &timezone)
            .unwrap()
            .with_timezone(&timezone);

        assert_eq!(next.year(), 2026);
        assert_eq!(next.month(), 3);
        assert_eq!(next.day(), 8);
        assert_eq!(next.hour(), 9);
        assert_eq!(next.minute(), 0);
    }

    #[test]
    fn weekly_period_preserves_wall_clock_time_across_fall_dst() {
        let timezone = chrono_tz::America::New_York;
        let now = timezone
            .with_ymd_and_hms(2026, 10, 31, 10, 0, 0)
            .single()
            .unwrap();
        let at = ScheduleAt {
            day: Some(0),
            hour: 9,
            minute: 0,
        };

        let next = calculate_period_next_fire(&SchedulePeriod::Week, &at, &now, &timezone)
            .unwrap()
            .with_timezone(&timezone);

        assert_eq!(next.year(), 2026);
        assert_eq!(next.month(), 11);
        assert_eq!(next.day(), 1);
        assert_eq!(next.hour(), 9);
        assert_eq!(next.minute(), 0);
    }

    #[test]
    fn weekly_period_uses_today_when_target_time_is_future() {
        let timezone = chrono_tz::America::Los_Angeles;
        let now = timezone
            .with_ymd_and_hms(2026, 6, 7, 8, 0, 0)
            .single()
            .unwrap();
        let at = ScheduleAt {
            day: Some(0),
            hour: 9,
            minute: 0,
        };

        let next = calculate_period_next_fire(&SchedulePeriod::Week, &at, &now, &timezone)
            .unwrap()
            .with_timezone(&timezone);

        assert_eq!(next.year(), 2026);
        assert_eq!(next.month(), 6);
        assert_eq!(next.day(), 7);
        assert_eq!(next.hour(), 9);
        assert_eq!(next.minute(), 0);
    }

    #[test]
    fn weekday_period_skips_weekend() {
        let timezone = chrono_tz::America::Los_Angeles;
        let now = timezone
            .with_ymd_and_hms(2026, 7, 3, 10, 0, 0)
            .single()
            .unwrap();
        let at = ScheduleAt {
            day: None,
            hour: 9,
            minute: 30,
        };

        let next = calculate_period_next_fire(&SchedulePeriod::Weekday, &at, &now, &timezone)
            .unwrap()
            .with_timezone(&timezone);

        assert_eq!(next.year(), 2026);
        assert_eq!(next.month(), 7);
        assert_eq!(next.day(), 6);
        assert_eq!(next.hour(), 9);
        assert_eq!(next.minute(), 30);
    }

    // ---- Fire path -------------------------------------------------------

    const SCHEDULE_RECIPE_YAML: &str = r#"cairnVersion: 1
name: Nightly
trigger: schedule
nodes:
- id: trigger-1
  type: trigger
  name: Trigger
  position: 0@0
  config:
    triggerType: schedule
    scope: issue
- id: builder-1
  type: agent
  name: Builder
  position: 0@100
  config:
    agent: build
edges:
- from: trigger-1@control-out
  to: builder-1@control-in
  type: control
- from: trigger-1@context-out
  to: builder-1@context-in
  type: context
"#;

    const BUILD_AGENT_MD: &str =
        "---\nname: Build\ndescription: builds\ntools: mcp__cairn__read\ntier: md\n---\nDo work.\n";

    fn orchestrator_with_config(db: LocalDb, config_dir: PathBuf) -> Orchestrator {
        use crate::db::DbState;
        use crate::orchestrator::OrchestratorBuilder;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::SearchIndex;
        use std::sync::Arc;

        let search =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        let db_state = Arc::new(DbState::new(Arc::new(db), search));
        let services = Arc::new(TestServicesBuilder::new().build());
        OrchestratorBuilder::new(db_state, services, config_dir).build()
    }

    fn daily_config() -> ScheduleConfig {
        ScheduleConfig {
            every: ScheduleEvery::Period(SchedulePeriod::Day),
            at: Some(ScheduleAt {
                day: None,
                hour: 9,
                minute: 0,
            }),
            allow_catchup: false,
            catchup_window_hours: 0,
        }
    }

    fn scheduled_recipe(id: &str, key: &str, project_id: &str) -> ScheduledRecipe {
        ScheduledRecipe {
            id: id.to_string(),
            name: id.to_string(),
            project_id: project_id.to_string(),
            project_key: key.to_string(),
            project_path: PathBuf::new(),
            schedule_config: daily_config(),
        }
    }

    /// Two recipes tied at the same fire instant are BOTH due at a wake at or
    /// after that instant — the fix for the single-`soonest` starvation bug.
    /// Neither is due before the instant.
    #[test]
    fn due_at_fires_every_tied_recipe() {
        let fire = Utc::now();
        let batch = vec![
            (scheduled_recipe("a", "PROJ", "p"), fire),
            (scheduled_recipe("b", "PROJ", "p"), fire),
        ];

        let due = due_at(&batch, fire + Duration::seconds(1));
        assert_eq!(due.len(), 2, "both tied recipes fire on the same wake");

        assert!(
            due_at(&batch, fire - Duration::seconds(1)).is_empty(),
            "nothing fires before its instant"
        );
    }

    /// Same-id scheduled recipes in different local projects (both routing to the
    /// private DB) must read their OWN last fire, not each other's — the fix for
    /// the cross-project interval-dedupe collision.
    #[tokio::test]
    async fn last_scheduled_fire_is_scoped_per_project() {
        let db = crate::storage::migrated_test_db("scheduler-dedupe.db").await;
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w', 'W', 1, 1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES('pa', 'w', 'A', 'PROJA', '/tmp/a', 1, 1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES('pb', 'w', 'B', 'PROJB', '/tmp/b', 1, 1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
             VALUES('ia', 'pa', 1, 'A', 'active', 'active', 'none', 1, 1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
             VALUES('ib', 'pb', 1, 'B', 'active', 'active', 'none', 1, 1);
            INSERT INTO executions(id, recipe_id, issue_id, status, started_at, triggered_by)
             VALUES('ea', 'nightly', 'ia', 'running', 1000, 'schedule');
            INSERT INTO executions(id, recipe_id, issue_id, status, started_at, triggered_by)
             VALUES('eb', 'nightly', 'ib', 'running', 2000, 'schedule');
            ",
        )
        .await
        .unwrap();

        let orch = orchestrator_with_config(db, tempfile::tempdir().unwrap().keep());

        let ra = scheduled_recipe("nightly", "PROJA", "pa");
        let rb = scheduled_recipe("nightly", "PROJB", "pb");
        assert_eq!(last_scheduled_fire(&orch, &ra).await, Some(1000));
        assert_eq!(last_scheduled_fire(&orch, &rb).await, Some(2000));
    }

    /// A single fire creates a fresh issue, starts its execution stamped
    /// `triggered_by = 'schedule'` with the issue attached, creates jobs, and —
    /// via the executions-table dedupe — pushes the next computed fire into the
    /// future so the same schedule never double-fires.
    #[tokio::test]
    async fn fire_creates_issue_and_schedule_execution() {
        let db = crate::storage::migrated_test_db("scheduler-fire.db").await;

        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let config_dir = root.join("config");
        std::fs::create_dir_all(config_dir.join("agents")).unwrap();
        std::fs::create_dir_all(config_dir.join("recipes")).unwrap();
        std::fs::write(config_dir.join("agents/build.md"), BUILD_AGENT_MD).unwrap();
        std::fs::write(
            config_dir.join("recipes/nightly.yaml"),
            SCHEDULE_RECIPE_YAML,
        )
        .unwrap();
        let project_dir = root.join("project");
        std::fs::create_dir_all(&project_dir).unwrap();

        db.execute_script(&format!(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w', 'W', 1, 1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES('p', 'w', 'Project', 'PROJ', '{}', 1, 1);
            ",
            project_dir.display()
        ))
        .await
        .unwrap();

        let orch = orchestrator_with_config(db, config_dir);
        let recipe = ScheduledRecipe {
            id: "nightly".to_string(),
            name: "Nightly".to_string(),
            project_id: "p".to_string(),
            project_key: "PROJ".to_string(),
            project_path: project_dir.clone(),
            schedule_config: daily_config(),
        };

        fire_scheduled_recipe(&orch, &recipe).await.unwrap();

        // A new issue exists.
        let issue_count = orch
            .db
            .local
            .query_opt_i64("SELECT COUNT(*) FROM issues", ())
            .await
            .unwrap();
        assert_eq!(issue_count, Some(1));

        // An execution exists with the issue attached and the schedule stamp.
        let (exec_issue, triggered_by) = orch
            .db
            .local
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query("SELECT issue_id, triggered_by FROM executions", ())
                        .await?;
                    let row = rows.next().await?.expect("one execution row");
                    Ok((row.opt_text(0)?, row.text(1)?))
                })
            })
            .await
            .unwrap();
        assert!(
            exec_issue.is_some(),
            "scheduled execution carries an issue_id"
        );
        assert_eq!(triggered_by, "schedule");

        // Jobs were created from the recipe's agent node.
        let job_count = orch
            .db
            .local
            .query_opt_i64("SELECT COUNT(*) FROM jobs", ())
            .await
            .unwrap();
        assert!(job_count.unwrap_or(0) >= 1, "the agent node produced a job");

        // No double fire: the executions-table dedupe now returns a last-fired
        // time, and the next computed fire is strictly in the future.
        let last = last_scheduled_fire(&orch, &recipe).await;
        assert!(last.is_some(), "the fire is recorded for dedupe");
        let next = calculate_next_fire(&recipe.schedule_config, &get_timezone(), last)
            .expect("a next fire time");
        assert!(
            next > Utc::now(),
            "the schedule does not immediately re-fire"
        );
    }
}
