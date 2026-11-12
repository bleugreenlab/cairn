//! One escalating incident per main-cadence suite and platform whose passes
//! the runner's own restarts keep cancelling (cairn/6969).
//!
//! A pass a restart cancels is cited once, by the boot that re-arms it
//! (`main_cadence_interruptions`, see [`super::arm_at_boot`]). That citation
//! says nothing about the pass before it dying the same way, and on a day the
//! runner restarts about as often as a colocated pass takes, a lane goes
//! unobserved for hours while each notice reads like the first. The streak is
//! that count: one row per lane, grown by each boot that finds the lane
//! cancelled, naming the restart (build and source) that cancelled each pass
//! and the machine it ran on, and cleared only by the lane's next complete row.
//! A death or a non-verdict does not clear it: the lane is still unobserved.
//!
//! The runner mirrors the rows in [`super::CadenceSlots`], so the project's
//! Main cadence section and the executor page render the incident in place
//! from one renderer without a read. The `notify` thread gets the steps
//! instead: the count rising past one, and the streak clearing.

use serde::{Deserialize, Serialize};

use super::*;

/// The restart that produced a runner boot: the build it booted and the
/// commit that build was made from, when it was stamped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RestartStamp {
    pub(crate) build_id: String,
    pub(crate) source_commit: Option<String>,
}

impl RestartStamp {
    /// This process's boot, once the host has recorded it.
    pub(crate) fn current() -> Option<Self> {
        crate::orchestrator::wakes::current_boot().map(|boot| Self {
            build_id: boot.build_id.clone(),
            source_commit: boot.source_commit.clone(),
        })
    }
}

/// One pass a restart cancelled.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RestartCancel {
    pub(crate) stopped_at_ms: i64,
    /// The tip the cancelled pass was attesting.
    pub(crate) cancelled_commit: String,
    /// The build the cancelling restart booted; absent when that boot was not
    /// recorded.
    pub(crate) build_id: Option<String>,
    pub(crate) source_commit: Option<String>,
    /// The machine the pass ran on, when it was placed.
    pub(crate) executor_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RestartStreak {
    pub(crate) project_id: String,
    pub(crate) check: String,
    pub(crate) platform: String,
    /// Oldest first.
    pub(crate) cancels: Vec<RestartCancel>,
    /// The lane's newest complete cadence row when the streak began: its
    /// commit and when it ran. What the lane has gone unobserved since.
    pub(crate) last_complete: Option<(String, i64)>,
    /// How many cancels the notify thread has been told of: the count's rise
    /// past it is what [`announce_rise`] posts.
    pub(crate) announced: usize,
}

impl RestartStreak {
    pub(super) fn key(&self) -> (String, LaneKey) {
        (
            self.project_id.clone(),
            (self.check.clone(), self.platform.clone()),
        )
    }

    /// Whether any cancelled pass of the streak ran on `executor_id`.
    pub(crate) fn ran_on(&self, executor_id: &str) -> bool {
        self.cancels
            .iter()
            .any(|cancel| cancel.executor_id.as_deref() == Some(executor_id))
    }
}

fn platform_label(platform: &str) -> &str {
    match platform {
        "macos" => "macOS",
        "linux" => "Linux",
        "windows" => "Windows",
        other => other,
    }
}

/// The incident as one line, the same on every surface that shows it.
pub(crate) fn render(streak: &RestartStreak, now_ms: i64, names: ExecutorNames<'_>) -> String {
    let unobserved = match &streak.last_complete {
        Some((commit, ran_at_ms)) => format!(
            "unobserved {} since `{}` (last complete row {})",
            age(now_ms, *ran_at_ms),
            short_commit(commit),
            stamp(*ran_at_ms)
        ),
        None => "no complete row on record".to_string(),
    };
    let cancels: Vec<String> = streak
        .cancels
        .iter()
        .map(|cancel| describe_cancel(cancel, names))
        .collect();
    format!(
        "{} {} cancelled by restart {}x running; {unobserved}. Cancelled by: {}.",
        platform_label(&streak.platform),
        streak.check,
        streak.cancels.len(),
        cancels.join("; ")
    )
}

fn describe_cancel(cancel: &RestartCancel, names: ExecutorNames<'_>) -> String {
    let restart = match &cancel.build_id {
        Some(build_id) => format!(
            "rebuild {} from {}",
            short_commit(build_id),
            cancel.source_commit.as_deref().map_or_else(
                || "source unknown".to_string(),
                |source| format!("`{}`", short_commit(source))
            )
        ),
        None => "a restart into an unrecorded build".to_string(),
    };
    let machine = cancel
        .executor_id
        .as_deref()
        .map(|executor_id| format!(" on {}", names(executor_id)))
        .unwrap_or_default();
    format!(
        "{} {restart} (pass on `{}`{machine})",
        stamp(cancel.stopped_at_ms),
        short_commit(&cancel.cancelled_commit)
    )
}

/// The message a streak's rise past one cancel posts: the incident, and that
/// the next complete row clears it.
pub(crate) fn escalation(streak: &RestartStreak, now_ms: i64, names: ExecutorNames<'_>) -> String {
    let mut text = format!(
        "**MAIN CADENCE RESTART STREAK** — {}",
        render(streak, now_ms, names)
    );
    match &streak.last_complete {
        Some((_, ran_at_ms)) => text.push_str(&format!(
            " Unobserved for {}; the next complete row clears this.",
            age(now_ms, *ran_at_ms)
        )),
        None => text.push_str(" The next complete row clears this."),
    }
    text
}

/// The line a streak's clearing posts.
pub(crate) fn render_cleared(streak: &RestartStreak, row: &CadenceRow) -> String {
    let count = streak.cancels.len();
    format!(
        "{} {} streak cleared: complete row {} on `{}` after {count} restart cancel{}.",
        platform_label(&streak.platform),
        streak.check,
        row.reference,
        short_commit(&row.commit),
        plural(count)
    )
}

const COLUMNS: &str = "project_id, check_name, platform, cancels_json, last_complete_commit, \
                       last_complete_ran_at_ms, announced_count";

fn streak_row(row: &cairn_db::turso::Row) -> crate::storage::DbResult<RestartStreak> {
    let last_complete = match (row.opt_text(4)?, row.opt_i64(5)?) {
        (Some(commit), Some(ran_at_ms)) => Some((commit, ran_at_ms)),
        _ => None,
    };
    Ok(RestartStreak {
        project_id: row.text(0)?,
        check: row.text(1)?,
        platform: row.text(2)?,
        // A row this build cannot read still counts its lane as streaking;
        // an empty list renders as such rather than hiding the lane.
        cancels: serde_json::from_str(&row.text(3)?).unwrap_or_default(),
        last_complete,
        announced: usize::try_from(row.i64(6)?).unwrap_or(0),
    })
}

async fn load(
    db: &LocalDb,
    project_id: &str,
    check: &str,
    platform: &str,
) -> Result<Option<RestartStreak>, String> {
    db.query_opt(
        format!(
            "SELECT {COLUMNS} FROM main_cadence_restart_streaks
              WHERE project_id = ?1 AND check_name = ?2 AND platform = ?3"
        ),
        (
            project_id.to_string(),
            check.to_string(),
            platform.to_string(),
        ),
        streak_row,
    )
    .await
    .map_err(|error| error.to_string())
}

/// Every streak this runner holds.
pub(crate) async fn load_all(db: &LocalDb) -> Result<Vec<RestartStreak>, String> {
    db.query_all(
        format!(
            "SELECT {COLUMNS} FROM main_cadence_restart_streaks
              ORDER BY project_id, check_name, platform"
        ),
        (),
        streak_row,
    )
    .await
    .map_err(|error| error.to_string())
}

async fn store(db: &LocalDb, streak: &RestartStreak, now_ms: i64) -> Result<(), String> {
    let cancels = serde_json::to_string(&streak.cancels).map_err(|error| error.to_string())?;
    let (commit, ran_at_ms) = match &streak.last_complete {
        Some((commit, ran_at_ms)) => (Some(commit.as_str()), Some(*ran_at_ms)),
        None => (None, None),
    };
    db.execute(
        "INSERT OR REPLACE INTO main_cadence_restart_streaks
           (project_id, check_name, platform, cancels_json, last_complete_commit,
            last_complete_ran_at_ms, announced_count, updated_at_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        (
            streak.project_id.clone(),
            streak.check.clone(),
            streak.platform.clone(),
            cancels,
            commit.map(str::to_string),
            ran_at_ms,
            i64::try_from(streak.announced).unwrap_or(i64::MAX),
            now_ms,
        ),
    )
    .await
    .map(|_| ())
    .map_err(|error| error.to_string())
}

/// End `project_id`'s streak on this lane, returning it when there was one.
pub(crate) async fn clear(
    db: &LocalDb,
    project_id: &str,
    check: &str,
    platform: &str,
) -> Result<Option<RestartStreak>, String> {
    let Some(streak) = load(db, project_id, check, platform).await? else {
        return Ok(None);
    };
    db.execute(
        "DELETE FROM main_cadence_restart_streaks
          WHERE project_id = ?1 AND check_name = ?2 AND platform = ?3",
        (
            project_id.to_string(),
            check.to_string(),
            platform.to_string(),
        ),
    )
    .await
    .map_err(|error| error.to_string())?;
    Ok(Some(streak))
}

/// End the lane's streak in its row and in the mirror, returning it when
/// there was one. A row that could not be deleted stays shown: the incident
/// is still on record, and the next complete row tries again.
pub(super) async fn clear_lane(
    orch: &Orchestrator,
    project_id: &str,
    lane: &CadenceLane,
) -> Option<RestartStreak> {
    match clear(&orch.db.local, project_id, &lane.check, &lane.platform).await {
        Ok(cleared) => {
            orch.main_cadence
                .lock()
                .unwrap()
                .restart_streaks
                .remove(&(project_id.to_string(), lane.key()));
            cleared
        }
        Err(error) => {
            log::warn!(
                "main cadence {} on {}: a complete row did not clear the restart streak: {error}",
                lane.check,
                lane.platform
            );
            None
        }
    }
}

/// Put back a streak whose clearing could not be announced, so the report's
/// retry clears and announces it.
pub(super) async fn restore(orch: &Orchestrator, streak: RestartStreak) {
    if let Err(error) = store(&orch.db.local, &streak, now_ms()).await {
        log::warn!(
            "main cadence {} on {}: the restart streak whose clearing was not delivered could not be restored: {error}",
            streak.check,
            streak.platform
        );
        return;
    }
    orch.main_cadence.lock().unwrap().set_restart_streak(streak);
}

/// The lane's newest complete cadence row: its commit and when it ran.
async fn last_complete(
    project_db: &LocalDb,
    project_id: &str,
    check: &str,
    platform: &str,
) -> Result<Option<(String, i64)>, String> {
    project_db
        .query_opt(
            "SELECT commit_sha, ran_at FROM check_result_observations
              WHERE project_id = ?1 AND cadence = 'main' AND complete = 1
                AND check_name = ?2 AND verdict_platform = ?3
              ORDER BY ran_at DESC LIMIT 1",
            (
                project_id.to_string(),
                check.to_string(),
                platform.to_string(),
            ),
            |row| Ok((row.text(0)?, row.i64(1)?)),
        )
        .await
        .map_err(|error| error.to_string())
}

/// Add one cancelled pass to its lane's streak, starting the streak when the
/// lane has none. The pass is stamped with the restart that cancelled it: the
/// one the interruption names when a boot recorded it, else `boot`, the boot
/// reading it back, since a stop cannot know the build it stops for.
/// Recording the same pass twice counts it once.
pub(crate) async fn record(
    db: &LocalDb,
    project_db: Option<&LocalDb>,
    project_id: &str,
    interruption: &Interruption,
    boot: Option<&RestartStamp>,
    now_ms: i64,
) -> Result<RestartStreak, String> {
    let mut streak = match load(db, project_id, &interruption.check, &interruption.platform).await?
    {
        Some(streak) => streak,
        None => RestartStreak {
            project_id: project_id.to_string(),
            check: interruption.check.clone(),
            platform: interruption.platform.clone(),
            cancels: Vec::new(),
            last_complete: None,
            announced: 0,
        },
    };
    // A streak started where the project's rows could not be read (a loss the
    // fleet settled) learns its last complete row from the next caller that can.
    if let (None, Some(project_db)) = (&streak.last_complete, project_db) {
        streak.last_complete = last_complete(
            project_db,
            project_id,
            &interruption.check,
            &interruption.platform,
        )
        .await?;
    }
    let restart = interruption.cancelled_by.as_ref().or(boot);
    let cancel = RestartCancel {
        stopped_at_ms: interruption.stopped_at_unix_ms,
        cancelled_commit: interruption.commit.clone(),
        build_id: restart.map(|restart| restart.build_id.clone()),
        source_commit: restart.and_then(|restart| restart.source_commit.clone()),
        executor_id: interruption.executor_id.clone(),
    };
    if !streak.cancels.iter().any(|held| {
        held.stopped_at_ms == cancel.stopped_at_ms
            && held.cancelled_commit == cancel.cancelled_commit
    }) {
        streak.cancels.push(cancel);
    }
    store(db, &streak, now_ms).await?;
    Ok(streak)
}

/// Move one interruption the boot read back into its lane's streak. The
/// interruption is stamped with `boot` first, if it names no restart yet, so
/// a transfer that fails here and is retried by a later boot still names the
/// restart that cancelled it; it is acknowledged (deleted from the queue)
/// only once its count is durable. Both writes are idempotent, so a crash
/// between them counts the pass once.
pub(super) async fn transfer(
    db: &LocalDb,
    project_db: Option<&LocalDb>,
    project_id: &str,
    interruption: &Interruption,
    boot: Option<&RestartStamp>,
    now_ms: i64,
) -> Result<(), String> {
    let mut interruption = interruption.clone();
    if interruption.cancelled_by.is_none() {
        if let Some(boot) = boot {
            db.execute(
                "UPDATE main_cadence_interruptions SET build_id = ?6, source_commit = ?7
                  WHERE project_id = ?1 AND check_name = ?2 AND platform = ?3
                    AND commit_sha = ?4 AND stopped_at = ?5 AND build_id IS NULL",
                (
                    project_id.to_string(),
                    interruption.check.clone(),
                    interruption.platform.clone(),
                    interruption.commit.clone(),
                    interruption.stopped_at_unix_ms,
                    boot.build_id.clone(),
                    boot.source_commit.clone(),
                ),
            )
            .await
            .map_err(|error| error.to_string())?;
            interruption.cancelled_by = Some(boot.clone());
        }
    }
    record(db, project_db, project_id, &interruption, None, now_ms).await?;
    db.execute(
        "DELETE FROM main_cadence_interruptions
          WHERE project_id = ?1 AND check_name = ?2 AND platform = ?3
            AND commit_sha = ?4 AND stopped_at = ?5",
        (
            project_id.to_string(),
            interruption.check.clone(),
            interruption.platform.clone(),
            interruption.commit.clone(),
            interruption.stopped_at_unix_ms,
        ),
    )
    .await
    .map(|_| ())
    .map_err(|error| error.to_string())
}

/// The boot's half: move every pass the last restart cancelled into its
/// lane's streak, then return every streak this runner holds, for the mirror.
/// An interruption whose transfer fails stays queued for the next boot.
pub(super) async fn at_boot(
    orch: &Orchestrator,
    interrupted: &[(String, Interruption)],
) -> Vec<RestartStreak> {
    let boot = RestartStamp::current();
    let now = now_ms();
    for (project_id, interruption) in interrupted {
        let project_db = match crate::execution::routing::owning_db_for_project(
            &orch.db, project_id,
        )
        .await
        {
            Ok(db) => Some(db),
            Err(error) => {
                log::warn!(
                        "main cadence {} on {}: the project's database is unreachable, so the restart streak starts without its last complete row: {error}",
                        interruption.check,
                        interruption.platform
                    );
                None
            }
        };
        if let Err(error) = transfer(
            &orch.db.local,
            project_db.as_deref(),
            project_id,
            interruption,
            boot.as_ref(),
            now,
        )
        .await
        {
            log::warn!(
                "main cadence: could not count {} into its restart streak; it stays queued for the next boot: {error}",
                interruption.describe()
            );
        }
    }
    match load_all(&orch.db.local).await {
        Ok(streaks) => streaks,
        Err(error) => {
            log::warn!("main cadence: restart streaks could not be read, so none renders until the next boot: {error}");
            Vec::new()
        }
    }
}

/// Re-read one lane's streak into the mirror, after a write the mirror did not
/// see (an adoption loss counted at its settlement edge), filling in its last
/// complete row from the project's database when the writer could not.
pub(super) async fn refresh(orch: &Orchestrator, project_id: &str, check: &str, platform: &str) {
    let streak = match load(&orch.db.local, project_id, check, platform).await {
        Ok(Some(streak)) => streak,
        Ok(None) => return,
        Err(error) => {
            log::warn!("main cadence {check} on {platform}: restart streak unreadable: {error}");
            return;
        }
    };
    let mut streak = streak;
    if streak.last_complete.is_none() {
        if let Ok(project_db) =
            crate::execution::routing::owning_db_for_project(&orch.db, project_id).await
        {
            if let Ok(Some(found)) = last_complete(&project_db, project_id, check, platform).await {
                streak.last_complete = Some(found);
                if let Err(error) = store(&orch.db.local, &streak, now_ms()).await {
                    log::warn!(
                        "main cadence {check} on {platform}: restart streak not updated: {error}"
                    );
                }
            }
        }
    }
    orch.main_cadence.lock().unwrap().set_restart_streak(streak);
}

/// Tell the lane's notify thread its streak has risen past one cancel, once
/// per rise. Called as the lane's run starts, so the escalation lands within
/// the boot grace of the restart that raised the count, whatever the run then
/// does and however long it takes. An undelivered escalation is retried by
/// the lane's next run.
pub(super) async fn announce_rise(
    orch: &Orchestrator,
    db: &LocalDb,
    project_id: &str,
    lane: &CadenceLane,
) {
    let Some(mut streak) = orch
        .main_cadence
        .lock()
        .unwrap()
        .restart_streak(project_id, lane)
        .filter(|streak| streak.cancels.len() >= 2 && streak.announced < streak.cancels.len())
        .cloned()
    else {
        return;
    };
    let names = |executor: &str| orch.fleet.executor_name_for_text(executor);
    let text = escalation(&streak, now_ms(), &names);
    if let Err(error) = try_deliver(orch, db, project_id, &lane.notify, &text).await {
        log::warn!("main cadence: {text} Not posted, retried on the lane's next run: {error}");
        return;
    }
    streak.announced = streak.cancels.len();
    if let Err(error) = store(&orch.db.local, &streak, now_ms()).await {
        log::warn!(
            "main cadence {} on {}: the escalation was posted but not marked, so it may post again: {error}",
            lane.check,
            lane.platform
        );
    }
    orch.main_cadence.lock().unwrap().set_restart_streak(streak);
}

/// Drop the streaks of lanes the tip no longer declares: nothing will clear
/// them.
pub(super) async fn forget_undeclared(
    orch: &Orchestrator,
    project_id: &str,
    declared: &[CadenceLane],
) {
    let undeclared: Vec<RestartStreak> = orch
        .main_cadence
        .lock()
        .unwrap()
        .restart_streaks(project_id)
        .into_iter()
        .filter(|streak| {
            !declared
                .iter()
                .any(|lane| lane.check == streak.check && lane.platform == streak.platform)
        })
        .collect();
    for streak in undeclared {
        match clear(&orch.db.local, project_id, &streak.check, &streak.platform).await {
            Ok(_) => {
                orch.main_cadence
                    .lock()
                    .unwrap()
                    .restart_streaks
                    .remove(&streak.key());
            }
            Err(error) => log::warn!(
                "main cadence: the restart streak of undeclared lane {} on {} could not be dropped: {error}",
                streak.check,
                streak.platform
            ),
        }
    }
}
