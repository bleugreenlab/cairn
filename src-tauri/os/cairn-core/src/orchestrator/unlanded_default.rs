//! The store's default branch ahead of origin by commits no landing produced.
//!
//! A project's default branch is supposed to move in exactly two ways: a Cairn
//! landing, which records the commit it produced as its merge request's
//! `merged_commit` and publishes it to origin, or an import of origin's own
//! advance. A store copy that holds commits beyond origin's tip and ends at a
//! commit no merge request landed came some other way. An agent committing
//! straight onto the default branch is the case this exists for (cairn/7323).
//! It does damage while it waits: every landing onto the default branch refuses
//! until the store and origin agree, and work prepared onto the default branch
//! carries the stray commits forward.
//!
//! The base-branch poll already learns origin's tip every minute. After each
//! poll, [`check`] compares the store's bookmark with it. A head seen ahead on
//! [`CONFIRMING_POLLS`] consecutive polls is raised once, as a message to
//! [`NOTICE_THREADS`]. A landing is not mistaken for one: its head is a
//! recorded `merged_commit`, and it reaches origin within the same publication,
//! well inside one poll interval.
//!
//! It reaches the operator's notification center only while it has a
//! consequence: a landing into the branch was refused because the store's copy
//! was not origin's ([`landing_refused`]), and that merge request is still
//! open, waiting on it. A dormant project whose branch sits ahead with nothing
//! trying to land waits on no one, so it lists nothing. The entry resolves when
//! the incident clears or the last refused landing stops waiting (merged by
//! other means, or closed), and carries no override: the remedy is a
//! publication or a reset, done elsewhere.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex, Weak};

use cairn_db::turso::params;

use crate::notifications::holds::{ExpectedRelease, Hold, HoldSource};
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};

const SOURCE: &str = "unlanded-default";

/// Consecutive polls a head must stay ahead before it is an incident.
const CONFIRMING_POLLS: u32 = 2;

/// Threads told when an incident is raised: the store's default branch and
/// every landing onto it are the execution thread's.
const NOTICE_THREADS: &[&str] = &["execution"];

/// One poll's comparison of the store's default branch with origin's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Comparison {
    /// The store holds origin's tip and more, ending at a commit no landing
    /// produced.
    Ahead { local: String, origin: String },
    /// Equal to origin, behind it, or ahead only by a landing's own commit.
    NotAhead,
    /// Nothing could be concluded (origin unknown, a read failed, or the two
    /// have diverged, which the sync reports itself).
    Unknown,
}

/// A store default branch carrying commits no landing produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Incident {
    pub(crate) project_id: String,
    pub(crate) branch: String,
    pub(crate) local: String,
    pub(crate) origin: String,
    /// Unix seconds of the first poll that saw this head ahead.
    pub(crate) first_seen: i64,
    pub(crate) raised_at: Option<i64>,
    pub(crate) cleared_at: Option<i64>,
    /// Unix seconds since a landing into the branch, refused while this
    /// incident stood, has been waiting on it: what makes it the operator's.
    pub(crate) waiting_since: Option<i64>,
    /// Unix seconds when the last refused landing stopped waiting, with the
    /// incident still standing.
    pub(crate) waiting_ended: Option<i64>,
    sightings: u32,
}

impl Incident {
    fn sighted(project_id: &str, branch: &str, local: String, origin: String, now: i64) -> Self {
        Self {
            project_id: project_id.to_string(),
            branch: branch.to_string(),
            local,
            origin,
            first_seen: now,
            raised_at: None,
            cleared_at: None,
            waiting_since: None,
            waiting_ended: None,
            sightings: 0,
        }
    }

    fn is_open(&self) -> bool {
        self.raised_at.is_some() && self.cleared_at.is_none()
    }

    fn is_waited_on(&self) -> bool {
        self.waiting_since.is_some() && self.waiting_ended.is_none()
    }

    /// When the operator's entry for it resolved: the incident clearing or
    /// nothing waiting on it any more, whichever came first.
    fn released_at(&self) -> Option<i64> {
        match (self.cleared_at, self.waiting_ended) {
            (Some(cleared), Some(ended)) => Some(cleared.min(ended)),
            (cleared, ended) => cleared.or(ended),
        }
    }

    pub(crate) fn describe(&self) -> String {
        let branch = &self.branch;
        format!(
            "The store's `{branch}` is at `{local}`, ahead of origin's `{origin}` by commits no \
             landing produced: no merged pull request into `{branch}` ends at `{local}`, so \
             something committed straight onto the default branch. Until the store and origin \
             agree, every landing into `{branch}` is refused, and work prepared onto `{branch}` \
             carries those commits. Read them with `cairn:@/vcs/log?branch={branch}` and decide: \
             publish them after review, or put the store's `{branch}` back on origin's tip.",
            local = short(&self.local),
            origin = short(&self.origin),
        )
    }
}

/// What one noted comparison changed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Change {
    None,
    Raised(Incident),
    Cleared(Incident),
}

/// Per-process sightings, keyed by project and branch. Like the rest of the
/// base-branch registry it describes this process's knowledge, which a restart
/// resets; a stray head still there after a restart is raised again two polls
/// later.
#[derive(Default)]
pub(crate) struct Sightings {
    state: Mutex<SightingState>,
}

type BranchKey = (String, String);

#[derive(Default)]
struct SightingState {
    incidents: HashMap<BranchKey, Incident>,
    /// Merge requests into each branch whose landing was refused because the
    /// store's copy was not origin's, kept while they stay open.
    refused: HashMap<BranchKey, BTreeSet<String>>,
}

impl Sightings {
    fn lock(&self) -> std::sync::MutexGuard<'_, SightingState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Record one poll's comparison at `now` (Unix seconds).
    pub(crate) fn note(
        &self,
        project_id: &str,
        branch: &str,
        comparison: Comparison,
        now: i64,
    ) -> Change {
        let mut state = self.lock();
        let SightingState {
            incidents: entries,
            refused,
        } = &mut *state;
        let key = (project_id.to_string(), branch.to_string());
        match comparison {
            Comparison::Unknown => Change::None,
            Comparison::NotAhead => {
                // Landings into the branch can go through again, so nothing
                // they were refused for still waits on it.
                refused.remove(&key);
                match entries.get_mut(&key) {
                    Some(entry) if entry.is_open() => {
                        entry.cleared_at = Some(now);
                        Change::Cleared(entry.clone())
                    }
                    Some(entry) if entry.raised_at.is_none() => {
                        entries.remove(&key);
                        Change::None
                    }
                    _ => Change::None,
                }
            }
            Comparison::Ahead { local, origin } => {
                let entry = entries.entry(key.clone()).or_insert_with(|| {
                    Incident::sighted(project_id, branch, local.clone(), origin.clone(), now)
                });
                if entry.is_open() {
                    // More commits on top of a raised incident extend it; the
                    // owners were told once and the hold shows the new head.
                    entry.local = local;
                    entry.origin = origin;
                    return Change::None;
                }
                if entry.local != local || entry.cleared_at.is_some() {
                    *entry = Incident::sighted(project_id, branch, local, origin, now);
                } else {
                    entry.origin = origin;
                }
                entry.sightings += 1;
                if entry.sightings < CONFIRMING_POLLS {
                    return Change::None;
                }
                entry.raised_at = Some(now);
                // A landing refused before the second sighting confirmed it
                // is already waiting on it.
                if refused.get(&key).is_some_and(|ids| !ids.is_empty()) {
                    entry.waiting_since = Some(now);
                }
                Change::Raised(entry.clone())
            }
        }
    }

    /// Record that landing `merge_request` into `branch` was refused because
    /// the store's copy was not origin's. Returns whether that made a standing
    /// incident the operator's.
    pub(crate) fn note_refused_landing(
        &self,
        project_id: &str,
        branch: &str,
        merge_request: &str,
        now: i64,
    ) -> bool {
        let mut state = self.lock();
        let key = (project_id.to_string(), branch.to_string());
        state
            .refused
            .entry(key.clone())
            .or_default()
            .insert(merge_request.to_string());
        match state.incidents.get_mut(&key) {
            Some(incident) if incident.is_open() && !incident.is_waited_on() => {
                incident.waiting_since = Some(now);
                incident.waiting_ended = None;
                true
            }
            _ => false,
        }
    }

    /// The refused merge requests into `branch` not yet seen closed.
    fn refused_landings(&self, project_id: &str, branch: &str) -> Vec<String> {
        self.lock()
            .refused
            .get(&(project_id.to_string(), branch.to_string()))
            .map(|ids| ids.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Keep only the refused landings into `branch` that are still open.
    /// Returns whether the last one leaving ended the operator's entry.
    fn settle_refused(
        &self,
        project_id: &str,
        branch: &str,
        still_open: &HashSet<String>,
        now: i64,
    ) -> bool {
        let mut state = self.lock();
        let key = (project_id.to_string(), branch.to_string());
        let Some(ids) = state.refused.get_mut(&key) else {
            return false;
        };
        ids.retain(|id| still_open.contains(id));
        if !ids.is_empty() {
            return false;
        }
        state.refused.remove(&key);
        match state.incidents.get_mut(&key) {
            Some(incident) if incident.is_open() && incident.is_waited_on() => {
                incident.waiting_ended = Some(now);
                true
            }
            _ => false,
        }
    }

    /// Raised incidents for `project_id` that a refused landing waited on,
    /// still waiting or resolved within the notification center's recent
    /// window.
    fn reported(&self, project_id: &str, now: i64) -> Vec<Incident> {
        self.lock()
            .incidents
            .values()
            .filter(|incident| incident.project_id == project_id && incident.raised_at.is_some())
            .filter(|incident| incident.waiting_since.is_some())
            .filter(|incident| {
                incident
                    .released_at()
                    .is_none_or(|at| at >= now - crate::notifications::RECENT_WINDOW_SECS)
            })
            .cloned()
            .collect()
    }
}

/// Compare the store's `branch` with origin's last observed tip, note it, and
/// raise or clear the incident. Called after each base-branch poll.
pub(crate) async fn check(
    orch: &Orchestrator,
    db: &LocalDb,
    project_id: &str,
    repo_path: &Path,
    branch: &str,
) {
    let comparison = compare(orch, db, project_id, repo_path, branch).await;
    let now = orch.services.clock.now();
    match orch
        .base_branch_sync
        .unlanded
        .note(project_id, branch, comparison, now)
    {
        Change::None => {}
        Change::Cleared(incident) => {
            log::info!(
                "project {project_id}: the store's `{branch}` agrees with origin again (was `{}`)",
                short(&incident.local)
            );
            emit_holds(orch, project_id);
        }
        Change::Raised(incident) => {
            log::error!("project {project_id}: {}", incident.describe());
            emit_holds(orch, project_id);
            notify_threads(orch, db, &incident).await;
        }
    }
    settle_refused_landings(orch, db, project_id, branch, now).await;
}

/// Drop refused landings that are no longer open, so an incident nothing
/// waits on leaves the operator's list.
async fn settle_refused_landings(
    orch: &Orchestrator,
    db: &LocalDb,
    project_id: &str,
    branch: &str,
    now: i64,
) {
    let sightings = &orch.base_branch_sync.unlanded;
    if sightings.refused_landings(project_id, branch).is_empty() {
        return;
    }
    let open = db
        .query_all(
            "SELECT id FROM merge_requests
              WHERE project_id = ?1 AND target_branch = ?2 AND status = 'open'",
            params![project_id, branch],
            |row| row.text(0),
        )
        .await;
    match open {
        Ok(open) => {
            if sightings.settle_refused(project_id, branch, &open.into_iter().collect(), now) {
                emit_holds(orch, project_id);
            }
        }
        Err(error) => log::warn!(
            "project {project_id}: could not tell which refused landings into `{branch}` still \
             wait: {error}"
        ),
    }
}

/// A landing of `merge_request` into the default branch `branch` was refused
/// because the store's copy holds commits origin's lacks. If the incident is
/// raised, it now has a consequence, and the operator sees it.
pub(crate) fn landing_refused(
    orch: &Orchestrator,
    project_id: &str,
    branch: &str,
    merge_request: &str,
) {
    let now = orch.services.clock.now();
    if orch
        .base_branch_sync
        .unlanded
        .note_refused_landing(project_id, branch, merge_request, now)
    {
        emit_holds(orch, project_id);
    }
}

async fn compare(
    orch: &Orchestrator,
    db: &LocalDb,
    project_id: &str,
    repo_path: &Path,
    branch: &str,
) -> Comparison {
    let Some(origin) = orch
        .base_branch_sync
        .snapshot(project_id, branch)
        .and_then(|state| state.remote_tip)
    else {
        return Comparison::Unknown;
    };
    let store = crate::jj::project_store_dir(&orch.config_dir, repo_path);
    let Ok(local) = cairn_vcs::resolve_coordinate(&store, branch).await else {
        return Comparison::Unknown;
    };
    if local == origin {
        return Comparison::NotAhead;
    }
    let contains = |commit: String, ancestor: String| {
        let store = store.clone();
        async move {
            cairn_vcs::commit_stamp(&store, &commit, Some(&ancestor))
                .await
                .ok()
                .and_then(|stamp| stamp.contains)
        }
    };
    if contains(local.clone(), origin.clone()).await != Some(true) {
        // Behind origin is the sync's ordinary work; diverged is reported by
        // the sync and by every landing. Neither concludes anything here.
        return if contains(origin.clone(), local.clone()).await == Some(true) {
            Comparison::NotAhead
        } else {
            Comparison::Unknown
        };
    }
    match landed_by_a_merge(db, project_id, branch, &local).await {
        Ok(true) => Comparison::NotAhead,
        Ok(false) => Comparison::Ahead { local, origin },
        Err(error) => {
            log::warn!(
                "project {project_id}: could not tell whether `{branch}` was landed: {error}"
            );
            Comparison::Unknown
        }
    }
}

/// Whether a merged pull request into `branch` produced `commit`.
pub(crate) async fn landed_by_a_merge(
    db: &LocalDb,
    project_id: &str,
    branch: &str,
    commit: &str,
) -> Result<bool, String> {
    db.query_one(
        "SELECT EXISTS(SELECT 1 FROM merge_requests
                       WHERE project_id = ?1 AND status = 'merged'
                         AND target_branch = ?2 AND merged_commit = ?3)",
        params![project_id, branch, commit],
        |row| Ok(row.i64(0)? != 0),
    )
    .await
    .map_err(|error| error.to_string())
}

async fn notify_threads(orch: &Orchestrator, db: &LocalDb, incident: &Incident) {
    let notice = format!("⛔ [Incident] {}", incident.describe());
    for name in NOTICE_THREADS {
        let (project_id, lookup) = (incident.project_id.clone(), name.to_string());
        let thread = db
            .read(move |conn| {
                let (project_id, lookup) = (project_id.clone(), lookup.clone());
                Box::pin(async move {
                    crate::threads::thread_id_by_name_conn(conn, &project_id, &lookup).await
                })
            })
            .await;
        let thread_id = match thread {
            Ok(Some(thread_id)) => thread_id,
            Ok(None) => continue,
            Err(error) => {
                log::warn!("unlanded default: could not resolve thread {name}: {error}");
                continue;
            }
        };
        if let Err(error) = crate::messages::delivery::append_thread_message(
            orch,
            db,
            &thread_id,
            crate::messages::delivery::ThreadSender::System {
                name: crate::messages::reporters::DEFAULT_BRANCH_WATCH.name,
                uri: crate::messages::reporters::DEFAULT_BRANCH_WATCH.uri,
            },
            &notice,
            crate::messages::reporters::DEFAULT_BRANCH_WATCH.urgency,
        )
        .await
        {
            log::warn!("unlanded default: could not tell thread {name}: {error}");
        }
    }
}

fn emit_holds(orch: &Orchestrator, project_id: &str) {
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({
            "table": crate::notifications::HOLDS_TABLE,
            "projectId": project_id,
        }),
    );
}

fn short(commit: &str) -> &str {
    &commit[..commit.len().min(12)]
}

/// Surfaces raised incidents a refused landing waits on in the operator's
/// notification center.
struct Notices {
    sightings: Weak<Sightings>,
}

impl HoldSource for Notices {
    fn name(&self) -> &'static str {
        SOURCE
    }

    fn holds(&self, project_id: &str) -> Vec<Hold> {
        let Some(sightings) = self.sightings.upgrade() else {
            return Vec::new();
        };
        sightings
            .reported(project_id, chrono::Utc::now().timestamp())
            .into_iter()
            .map(|incident| {
                let waiting_since = incident.waiting_since.unwrap_or(incident.first_seen);
                Hold {
                    // Per waiting episode: a landing refused again after the
                    // last one stopped waiting is a new occurrence.
                    id: format!(
                        "{SOURCE}:{}:{}:{waiting_since}",
                        incident.branch, incident.local
                    ),
                    project_id: Some(incident.project_id.clone()),
                    reason: incident.describe(),
                    expected_release: Some(ExpectedRelease::Text(format!(
                        "When the store's `{}` and origin's agree again, or no refused landing \
                         still waits on it",
                        incident.branch
                    ))),
                    override_label: None,
                    source_uri: None,
                    opened_at: waiting_since,
                    released_at: incident.released_at(),
                }
            })
            .collect()
    }
}

/// Register the incident source with the notification center.
pub(crate) fn register(orch: &Orchestrator) {
    let sightings: &Arc<Sightings> = &orch.base_branch_sync.unlanded;
    orch.register_hold_source(Arc::new(Notices {
        sightings: Arc::downgrade(sightings),
    }));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn reporter_notice_uses_peer_identity_and_respects_its_mute() {
        use crate::db::DbState;
        use crate::orchestrator::wakes::{mute, subscribe};
        use crate::services::testing::{RecordingProcessSpawner, TestServicesBuilder};
        use crate::storage::SearchIndex;
        use std::sync::Arc;

        let db = Arc::new(crate::storage::migrated_test_db("default-branch-watch-sender.db").await);
        let config_dir = tempfile::tempdir().unwrap();
        let state = Arc::new(DbState::new(
            db.clone(),
            Arc::new(
                SearchIndex::open_or_create(config_dir.path().join("search-index.db")).unwrap(),
            ),
        ));
        let services = Arc::new(
            TestServicesBuilder::new()
                .with_process(RecordingProcessSpawner::new())
                .build(),
        );
        let orch = Orchestrator::builder(state, services, config_dir.path().to_path_buf()).build();
        db.execute_script(
            "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('p','default','P','prj','/tmp/p',1,1);
             INSERT INTO threads(id, project_id, name, status, attention, created_at, updated_at)
               VALUES ('t','p','execution','active','none',1,1);
             INSERT INTO jobs(id, thread_id, project_id, node_name, uri_segment, status, created_at, updated_at)
               VALUES ('j','t','p','thread','thread','idle',1,1);
             INSERT INTO runs(id, project_id, job_id, status, created_at, updated_at)
               VALUES ('r','p','j','live',1,1);",
        ).await.unwrap();
        const REPORTER: &str = "cairn://reporters/default-branch-watch";
        // Only the exact reporter may cross this broad peer mute. A missing or
        // different reference cannot produce the push asserted below.
        mute(&db, "j", "peer", None, None, None, None, "agent")
            .await
            .unwrap();
        subscribe(&db, "j", "peer", Some(REPORTER), None, "agent")
            .await
            .unwrap();
        let incident = Incident::sighted("p", "main", "stray".into(), "origin".into(), 1);
        notify_threads(&orch, &db, &incident).await;
        let pushes = db
            .query_all(
                "SELECT key, wake, boundary FROM attention_pushes WHERE recipient='j'",
                (),
                |row| Ok((row.text(0)?, row.text(1)?, row.text(2)?)),
            )
            .await
            .unwrap();
        assert_eq!(
            pushes.len(),
            1,
            "the exact reporter reference enables delivery"
        );
        assert!(
            !pushes[0].0.contains(":operator_message:"),
            "a machine notice is not the user's word"
        );
        let urgency = db
            .query_one(
                "SELECT urgency FROM messages WHERE channel_type='direct'",
                (),
                |row| row.opt_text(0),
            )
            .await
            .unwrap();
        assert_eq!(urgency.as_deref(), Some("steer"));
        assert_eq!(pushes[0].1, "wake");
        assert_eq!(pushes[0].2, "event", "the incident retains steer urgency");

        mute(&db, "j", "peer", Some(REPORTER), None, None, None, "agent")
            .await
            .unwrap();
        notify_threads(&orch, &db, &incident).await;
        let messages = db
            .query_one(
                "SELECT COUNT(*) FROM messages WHERE channel_type='thread' AND channel_id='t'",
                (),
                |row| row.i64(0),
            )
            .await
            .unwrap();
        assert_eq!(messages, 2, "muting retains the thread conversation record");
        let pushes_after = db
            .query_one(
                "SELECT COUNT(*) FROM attention_pushes WHERE recipient='j'",
                (),
                |row| row.i64(0),
            )
            .await
            .unwrap();
        assert_eq!(pushes_after, 1, "the reporter mute prevents another wake");
        assert_a_landings_own_commit_is_not_unlanded(&db).await;
    }

    fn ahead(local: &str) -> Comparison {
        Comparison::Ahead {
            local: local.into(),
            origin: "origin-tip".into(),
        }
    }

    /// A head ahead of origin that nothing tries to land on (a dormant
    /// project's operator commit) is an incident the owning thread hears
    /// about, but no operator entry: it waits on no one.
    #[test]
    fn an_incident_no_landing_waits_on_is_no_operator_entry() {
        let sightings = Arc::new(Sightings::default());
        let notices = Notices {
            sightings: Arc::downgrade(&sightings),
        };
        sightings.note("p", "main", ahead("579f20af"), 10);
        assert!(matches!(
            sightings.note("p", "main", ahead("579f20af"), 70),
            Change::Raised(_)
        ));
        assert!(sightings.reported("p", 80).is_empty());
        assert!(notices.holds("p").is_empty());
    }

    /// A landing refused because of the incident makes it the operator's,
    /// with no override button, until that landing stops waiting; a later
    /// refusal is a new occurrence, and the incident clearing resolves it.
    #[test]
    fn a_refused_landing_lists_the_incident_until_it_stops_waiting() {
        let sightings = Arc::new(Sightings::default());
        let notices = Notices {
            sightings: Arc::downgrade(&sightings),
        };
        let now = chrono::Utc::now().timestamp();
        sightings.note("p", "main", ahead("stray"), now - 100);
        sightings.note("p", "main", ahead("stray"), now - 90);

        assert!(sightings.note_refused_landing("p", "main", "mr-1", now - 80));
        assert!(
            !sightings.note_refused_landing("p", "main", "mr-2", now - 75),
            "already waited on"
        );
        let holds = notices.holds("p");
        assert_eq!(holds.len(), 1);
        let first = holds[0].clone();
        assert_eq!(first.override_label, None, "no do-nothing button");
        assert_eq!(first.opened_at, now - 80);
        assert_eq!(first.released_at, None);
        assert!(first.reason.contains("cairn:@/vcs/log?branch=main"));
        assert!(notices.holds("other").is_empty());

        // One refused landing still open keeps it listed.
        let open = |ids: &[&str]| ids.iter().map(|id| id.to_string()).collect();
        assert!(!sightings.settle_refused("p", "main", &open(&["mr-2"]), now - 60));
        assert_eq!(notices.holds("p")[0].released_at, None);
        assert!(sightings.settle_refused("p", "main", &open(&[]), now - 50));
        assert_eq!(notices.holds("p")[0].released_at, Some(now - 50));

        // Refused again: a new occurrence an earlier dismissal does not hide.
        assert!(sightings.note_refused_landing("p", "main", "mr-3", now - 40));
        let again = notices.holds("p");
        assert_eq!(again.len(), 1);
        assert_ne!(again[0].id, first.id);
        assert_eq!(again[0].released_at, None);

        assert!(matches!(
            sightings.note("p", "main", Comparison::NotAhead, now - 30),
            Change::Cleared(_)
        ));
        assert_eq!(notices.holds("p")[0].released_at, Some(now - 30));
        assert!(
            sightings.refused_landings("p", "main").is_empty(),
            "landings go through once the store agrees with origin"
        );
    }

    /// A landing refused before the second sighting confirms the incident is
    /// already waiting on it when it is raised.
    #[test]
    fn a_landing_refused_before_the_raise_is_waiting_from_the_raise() {
        let sightings = Sightings::default();
        sightings.note("p", "main", ahead("stray"), 10);
        assert!(!sightings.note_refused_landing("p", "main", "mr-1", 20));
        let Change::Raised(incident) = sightings.note("p", "main", ahead("stray"), 70) else {
            panic!("the second sighting raises");
        };
        assert_eq!(incident.waiting_since, Some(70));
        assert_eq!(sightings.reported("p", 80).len(), 1);
    }

    /// cairn/7323: a stray head is raised on its second consecutive sighting,
    /// once, shown to the operator while a landing waits on it until it
    /// clears, and a landing in between (a head that is not ahead) never raises
    /// anything.
    #[test]
    fn a_head_ahead_of_origin_is_raised_once_when_it_persists() {
        let sightings = Sightings::default();
        assert_eq!(
            sightings.note("p", "main", ahead("stray"), 10),
            Change::None
        );
        let Change::Raised(incident) = sightings.note("p", "main", ahead("stray"), 70) else {
            panic!("the second sighting raises");
        };
        assert_eq!((incident.first_seen, incident.raised_at), (10, Some(70)));
        assert!(incident.describe().contains("no landing produced"));

        // More stray commits on top extend it without a second raise.
        assert_eq!(
            sightings.note("p", "main", ahead("stray-2"), 130),
            Change::None
        );
        assert_eq!(
            sightings.note("p", "main", Comparison::Unknown, 190),
            Change::None
        );
        assert!(sightings.note_refused_landing("p", "main", "mr", 190));
        let reported = sightings.reported("p", 190);
        assert_eq!(reported.len(), 1);
        assert_eq!(reported[0].local, "stray-2");
        assert!(sightings.reported("other", 190).is_empty());

        let Change::Cleared(cleared) = sightings.note("p", "main", Comparison::NotAhead, 250)
        else {
            panic!("agreeing with origin clears it");
        };
        assert_eq!(cleared.cleared_at, Some(250));
        assert_eq!(
            sightings.reported("p", 260)[0].cleared_at,
            Some(250),
            "a cleared incident stays visible as resolved"
        );

        // A fresh stray head after clearing is a new incident.
        assert_eq!(
            sightings.note("p", "main", ahead("again"), 300),
            Change::None
        );
        assert!(matches!(
            sightings.note("p", "main", ahead("again"), 360),
            Change::Raised(_)
        ));
    }

    /// A head that changes between polls (a landing moved it, then it was
    /// published) is never confirmed.
    #[test]
    fn a_head_seen_ahead_once_is_not_an_incident() {
        let sightings = Sightings::default();
        assert_eq!(sightings.note("p", "main", ahead("a"), 10), Change::None);
        assert_eq!(sightings.note("p", "main", ahead("b"), 70), Change::None);
        assert_eq!(
            sightings.note("p", "main", Comparison::NotAhead, 130),
            Change::None
        );
        assert_eq!(sightings.note("p", "main", ahead("c"), 190), Change::None);
        assert!(sightings.reported("p", 190).is_empty());
    }

    /// A local landing leaves the store ahead of origin by design; its head is
    /// its merge request's `merged_commit`, which is what tells the two apart.
    async fn assert_a_landings_own_commit_is_not_unlanded(db: &LocalDb) {
        db.execute_script(
            "INSERT INTO merge_requests(id, job_id, project_id, title, source_branch, target_branch,
                                        status, merged_commit, opened_at, updated_at, is_local)
               VALUES ('mr','j','p','T','agent/prj-1-builder-0','main','merged','landed',1,1,1),
                      ('mr-open','j2','p','T','agent/prj-2-builder-0','main','open','pending',1,1,1);",
        )
        .await
        .unwrap();
        assert!(landed_by_a_merge(db, "p", "main", "landed").await.unwrap());
        assert!(!landed_by_a_merge(db, "p", "main", "stray").await.unwrap());
        assert!(
            !landed_by_a_merge(db, "p", "main", "pending").await.unwrap(),
            "only a merged request's commit counts"
        );
        assert!(
            !landed_by_a_merge(db, "p", "release", "landed")
                .await
                .unwrap(),
            "a landing counts only for the branch it landed on"
        );
    }
}
