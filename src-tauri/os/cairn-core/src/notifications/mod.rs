//! The operator's notification center: one list, across every project, of the
//! work that waits on the operator and will not move until they act.
//!
//! Informational news has no entry here. A thread's unread replies are the
//! sidebar's unread badge, and a red check on a PR is the owning agent's to
//! act on (the PR's own entry names its check condition). Admitting either
//! buries what is actually waiting.
//!
//! Nothing here is a message log. Every entry is derived, on each rebuild, from
//! the live row that is its cause: a pending question, a proposed tenet, an open
//! PR, a failed agent nothing recovered. "Open" means the source's open
//! predicate holds right now, and "resolved" is read from the source's own
//! resolution timestamp. So an item handled anywhere in the app can never show
//! as needing action here (tenet 1290). The one thing the center stores is the
//! operator's own dismissals, in `operator_notification_dismissals`.
//!
//! Each project's list is a [`crate::orchestrator::generation_cache`] projection
//! beside the sidebar rollups, dirtied by writes to the tables it reads and
//! announced as `operator_notifications` when its answer changes.

mod derive;
pub mod holds;
#[cfg(test)]
mod tests;

pub use holds::{ExpectedRelease, Hold, HoldRegistry, HoldSource};

use crate::db::DbState;
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};
use cairn_db::turso::params;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap, HashSet};

/// How far back the "all" view reaches for resolved and dismissed entries.
pub const RECENT_WINDOW_SECS: i64 = 7 * 24 * 60 * 60;

/// The most resolved or dismissed entries one project contributes to "all".
pub const ALL_VIEW_CAP_PER_PROJECT: usize = 200;

/// How long a failed or crashed turn must stand with no successor before it
/// is an "agent failed" entry. Automatic recovery starts its successor well
/// inside this; a failure still standing after it is one the operator owns.
pub const AGENT_FAILURE_SETTLE_SECS: i64 = 30;

/// The projection's announcement name, which the frontend maps to its query.
pub const PROJECTION: &str = "operator_notifications";

pub(crate) const DISMISSALS_TABLE: &str = "operator_notification_dismissals";

/// The frame a [`HoldSource`] emits when its answer changes.
pub const HOLDS_TABLE: &str = "operator_holds";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NotificationKind {
    Question,
    Permission,
    Tenet,
    PrReady,
    AgentFailed,
    AwaitingReview,
    Hold,
    /// A thread's message to the operator that states a default, open until
    /// he answers it.
    OperatorMessage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NotificationState {
    Open,
    Resolved,
    Dismissed,
}

/// A pending permission, carrying what the node pane's permission prompt
/// renders and answers.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PermissionActionRequest {
    pub id: String,
    pub run_id: String,
    pub tool_use_id: String,
    pub tool_name: String,
    pub tool_input: serde_json::Value,
    pub created_at: i64,
}

/// The inline action an entry offers, carrying the ids the command the
/// existing surface uses for it needs. Link-only kinds carry none.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NotificationAction {
    /// `respond_to_prompt`. `questions` is the stored JSON, exactly as the
    /// node pane's pending-prompt read returns it.
    #[serde(rename_all = "camelCase")]
    Question {
        prompt_id: String,
        run_id: String,
        questions: String,
    },
    /// `respond_to_permission`.
    #[serde(rename_all = "camelCase")]
    Permission { request: PermissionActionRequest },
    /// `answer_tenet`.
    #[serde(rename_all = "camelCase")]
    Tenet { number: i64, tenet_uri: String },
    /// `merge_pr`, addressed by the PR's owning job or action run.
    #[serde(rename_all = "camelCase")]
    Merge {
        action_run_id: String,
        merge_method: String,
    },
    /// `override_operator_hold`.
    #[serde(rename_all = "camelCase")]
    Hold {
        source: String,
        hold_id: String,
        reason: String,
        expected_release: Option<ExpectedRelease>,
        override_label: String,
    },
    /// `write_channel_resource` append `{content}` on `message`, the
    /// message's `cairn://operator/messages/{id}`. `default_text` is what its
    /// sender does if he does not reply.
    #[serde(rename_all = "camelCase")]
    Reply {
        message: String,
        default_text: String,
    },
}

/// One entry in the center.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OperatorNotification {
    /// Stable identity. A new occurrence of the same kind of cause gets a new
    /// key, which is what lets a dismissal close exactly one occurrence.
    pub key: String,
    pub project_id: String,
    pub project_key: String,
    pub kind: NotificationKind,
    /// Unix seconds when the cause arose: the sort key, newest first.
    pub at: i64,
    pub summary: String,
    /// Canonical `cairn://` address of the cause.
    pub source_uri: String,
    pub state: NotificationState,
    /// Unix seconds of the resolution or dismissal; `None` while open.
    pub state_at: Option<i64>,
    pub action: Option<NotificationAction>,
    /// An awaiting-review entry the operator deferred: what it waits on, as
    /// the held-execution census renders a gate. Shown, never hidden.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deferred_until: Option<String>,
    /// An awaiting-review entry pending past a week with no deferral. A mark,
    /// never a wake.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub stale: bool,
    /// The newest live headline a thread wrote about this entry's cause (its
    /// `ref` is the entry's source or that source's issue), so the operator
    /// reads why before he merges. Absent when no thread wrote one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub headline: Option<crate::threads::headlines::HeadlineAbout>,
}

impl OperatorNotification {
    pub fn is_open(&self) -> bool {
        self.state == NotificationState::Open
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotificationView {
    /// Only what needs the operator now.
    Open,
    /// Also what resolved or was dismissed within [`RECENT_WINDOW_SECS`].
    All,
}

impl NotificationView {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "open" => Ok(Self::Open),
            "all" => Ok(Self::All),
            other => Err(format!("unknown notification view {other:?}")),
        }
    }
}

/// One project's derived list, newest first, with dismissals applied.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct OperatorNotificationsSnapshot {
    pub entries: Vec<OperatorNotification>,
    /// Unix seconds when this answer next changes with no write to announce
    /// it (a held-back failure settling). The cache schedules a rebuild then.
    pub next_change_at: Option<i64>,
}

/// The project an entry belongs to.
#[derive(Debug, Clone)]
pub(crate) struct ProjectRef {
    pub(crate) id: String,
    pub(crate) key: String,
}

/// Rebuild one project's list: the notification cache's rebuild.
///
/// Sources are read from the project's owning database (a team project's
/// replica); dismissals from the private database, because a dismissal is one
/// operator's and never replicates.
pub(crate) async fn rebuild_operator_notifications(
    dbs: &DbState,
    holds: &HoldRegistry,
    project_id: &str,
) -> Result<OperatorNotificationsSnapshot, String> {
    let db = crate::execution::routing::owning_db_for_project(dbs, project_id)
        .await
        .map_err(|error| error.to_string())?;
    let dismissals = load_dismissals(&dbs.local, project_id).await?;
    let now = chrono::Utc::now().timestamp();
    derive_snapshot(&db, &dbs.local, holds, project_id, now, &dismissals).await
}

/// Every entry one project's sources produce at `now`, with `dismissals`
/// (entry key to dismissal time) applied. `db` is the project's owning
/// database; `local` the private one, which holds the operator's own messages.
pub(crate) async fn derive_snapshot(
    db: &LocalDb,
    local: &LocalDb,
    holds: &HoldRegistry,
    project_id: &str,
    now: i64,
    dismissals: &HashMap<String, i64>,
) -> Result<OperatorNotificationsSnapshot, String> {
    let Some(key) = db
        .query_opt_text(
            "SELECT key FROM projects WHERE id = ?1",
            params![project_id.to_string()],
        )
        .await
        .map_err(|error| error.to_string())?
    else {
        return Ok(OperatorNotificationsSnapshot::default());
    };
    let project = ProjectRef {
        id: project_id.to_string(),
        key,
    };
    let since = now - RECENT_WINDOW_SECS;
    let (mut entries, next_change_at) =
        derive::from_sources(db, local, &project, since, now).await?;
    entries.extend(derive::holds(holds, &project));
    derive::attach_headlines(db, &project, &mut entries).await;
    for entry in &mut entries {
        // A resolved entry stays resolved: that it was handled says more than
        // that the operator also closed it.
        if entry.is_open() {
            if let Some(&at) = dismissals.get(&entry.key) {
                entry.state = NotificationState::Dismissed;
                entry.state_at = Some(at);
            }
        }
    }
    entries.retain(|entry| entry.is_open() || entry.state_at.is_some_and(|at| at >= since));
    let mut seen = HashSet::new();
    entries.retain(|entry| seen.insert(entry.key.clone()));
    sort_newest_first(&mut entries);
    Ok(OperatorNotificationsSnapshot {
        entries,
        next_change_at,
    })
}

fn sort_newest_first(entries: &mut [OperatorNotification]) {
    entries.sort_by(|a, b| b.at.cmp(&a.at).then_with(|| a.key.cmp(&b.key)));
}

async fn load_dismissals(
    local: &LocalDb,
    project_id: &str,
) -> Result<HashMap<String, i64>, String> {
    local
        .query_all(
            "SELECT entry_key, dismissed_at FROM operator_notification_dismissals
              WHERE project_id = ?1",
            params![project_id.to_string()],
            |row| Ok((row.text(0)?, row.i64(1)?)),
        )
        .await
        .map(|rows| rows.into_iter().collect())
        .map_err(|error| error.to_string())
}

/// The entries the center shows: every project on this runner, or one.
pub async fn list_operator_notifications(
    orch: &Orchestrator,
    view: NotificationView,
    project_id: Option<&str>,
) -> Result<Vec<OperatorNotification>, String> {
    let project_ids = match project_id {
        Some(project_id) => vec![project_id.to_string()],
        None => listed_project_ids(&orch.db).await,
    };
    let since = chrono::Utc::now().timestamp() - RECENT_WINDOW_SECS;
    let mut entries = Vec::new();
    for project_id in project_ids {
        let snapshot = match orch.operator_notifications_cache.get(&project_id).await {
            Ok(snapshot) => snapshot,
            Err(error) => {
                log::warn!("operator notifications for project {project_id} unavailable: {error}");
                continue;
            }
        };
        entries.extend(select_view(&snapshot.entries, view, since));
    }
    // A workspace-wide hold is carried by every project's list; show it once.
    let mut seen = HashSet::new();
    entries.retain(|entry| seen.insert(entry.key.clone()));
    sort_newest_first(&mut entries);
    Ok(entries)
}

/// One project's share of a view: every open entry, and for "all" the newest
/// [`ALL_VIEW_CAP_PER_PROJECT`] closed ones still inside the window.
pub(crate) fn select_view(
    entries: &[OperatorNotification],
    view: NotificationView,
    since: i64,
) -> Vec<OperatorNotification> {
    let open = entries.iter().filter(|entry| entry.is_open()).cloned();
    match view {
        NotificationView::Open => open.collect(),
        NotificationView::All => open
            .chain(
                entries
                    .iter()
                    .filter(|entry| {
                        !entry.is_open() && entry.state_at.is_some_and(|at| at >= since)
                    })
                    .take(ALL_VIEW_CAP_PER_PROJECT)
                    .cloned(),
            )
            .collect(),
    }
}

/// Every visible project across the private database and open team replicas.
async fn listed_project_ids(dbs: &DbState) -> Vec<String> {
    let mut ids = Vec::new();
    let mut seen = HashSet::new();
    for db in dbs.all_dbs().await {
        match db
            .query_all(
                "SELECT id FROM projects WHERE hidden = 0 ORDER BY id",
                (),
                |row| row.text(0),
            )
            .await
        {
            Ok(rows) => ids.extend(rows.into_iter().filter(|id| seen.insert(id.clone()))),
            Err(error) => log::warn!("operator notifications could not list projects: {error}"),
        }
    }
    ids
}

/// One entry the operator closed.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Dismissal {
    pub project_id: String,
    pub entry_key: String,
}

/// Record the operator's close of entries, possibly across projects. Closing
/// one entry and clearing a whole view are the same operation, so each closed
/// entry behaves exactly as if dismissed alone. Returns how many were new.
pub async fn dismiss_operator_notifications(
    orch: &Orchestrator,
    dismissals: &[Dismissal],
) -> Result<usize, String> {
    let now = orch.services.clock.now();
    let mut inserted = 0;
    let mut touched = BTreeSet::new();
    let mut failure = None;
    for dismissal in dismissals {
        match record_dismissal(
            &orch.db.local,
            &dismissal.project_id,
            &dismissal.entry_key,
            now,
        )
        .await
        {
            Ok(new) => {
                inserted += usize::from(new);
                touched.insert(dismissal.project_id.as_str());
            }
            Err(error) => {
                failure = Some(error);
                break;
            }
        }
    }
    // Announce what did land even when a later row failed, so the list the
    // operator is looking at never hides a recorded dismissal.
    for project_id in touched {
        // The operator is looking at the rows they just closed, so this
        // rebuild skips the floor, as marking a thread viewed does.
        orch.operator_notifications_cache
            .invalidate_now(project_id, "dismissal");
        let _ = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({
                "table": DISMISSALS_TABLE,
                "action": "insert",
                "projectId": project_id,
            }),
        );
    }
    match failure {
        Some(error) => Err(error),
        None => Ok(inserted),
    }
}

pub(crate) async fn record_dismissal(
    local: &LocalDb,
    project_id: &str,
    entry_key: &str,
    now: i64,
) -> Result<bool, String> {
    local
        .execute(
            "INSERT INTO operator_notification_dismissals (entry_key, project_id, dismissed_at)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(entry_key) DO NOTHING",
            params![entry_key, project_id, now],
        )
        .await
        .map(|changed| changed > 0)
        .map_err(|error| error.to_string())
}

/// Hand an override to the source that reported the hold.
pub fn override_operator_hold(
    orch: &Orchestrator,
    source: &str,
    hold_id: &str,
) -> Result<(), String> {
    orch.hold_sources.override_hold(source, hold_id)?;
    // The source announces its own change; this covers one that does not.
    orch.operator_notifications_cache
        .invalidate_matching("hold-override", |_| true);
    Ok(())
}
