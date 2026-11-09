//! Retirement of the legacy base-scoped reconcile queue, and the upkeep loop
//! that replaced its sweep.
//!
//! Before base advance became lazy, a base movement minted a
//! `jj_reconcile_intents` row for its `(base, commit)` coordinate and a durable
//! sweep replayed every live branch cut from that base onto it, one
//! `jj_reconcile_items` row per branch. Nothing mints such work any more: a
//! branch is carried onto its base only at a demand boundary
//! ([`super::preparation`]), whose outcome rows are created `completed`. What
//! the old runtime left queued or in flight is settled here, without replaying
//! it.
//!
//! # Which rows are legacy
//!
//! The set of intents still `pending` or `running` is captured by id the first
//! time the upkeep loop reads a database: at startup for the local database,
//! and when a team database is first seen. Only captured ids are retired. The
//! capture is by id rather than by timestamp, so a row the old process wrote in
//! the same second as this one started, or stamped from a skewed clock, is
//! still legacy. It is exact under a single-orchestrator stop, upgrade, start,
//! which is the supported upgrade: no old worker holds a lease once its
//! process has stopped. Running an old and a new orchestrator against one
//! database is not supported; the old one would keep sweeping.
//!
//! # What a retired intent leaves behind
//!
//! The intent becomes `superseded`. It is never expanded into the branches it
//! would have replayed, and no note or wake is sent for it. Its own item rows
//! are inspected, never the live job set:
//!
//! - An item the old worker installed (`graph_moved`, outcome `rebased_clean`
//!   or `silent`) but had not finished delivering is an accepted publication
//!   whose Git export may not have completed. When the branch's literal head
//!   is still that install, the item is recorded `export_pending` at it, so
//!   the next preparation of exactly that branch re-verifies the export. When
//!   the head has moved on, the later operation owns its own export and the
//!   item is recorded `export_superseded`. The accepted install is kept,
//!   neither replayed nor rolled back. A closed session or a replay request
//!   on the row is history (the install itself served the request) and does
//!   not stop the recovery.
//! - An unfinished item (`pending`/`running`) whose literal head differs from
//!   what the old worker observed may have been installed without being
//!   recorded, and is recorded `export_pending` at the literal head for the
//!   same exact-branch recovery. One whose head is unchanged installed
//!   nothing and stays as history.
//! - An item with an open conflict session, or an unfinished item carrying an
//!   owner's replay request, is left verbatim. The session keeps resolving
//!   through `cairn:~/rebase`, and the request is recovered by its exact branch
//!   through the replay-request path, never from here.
//! - A held-descendant intent (`trigger_sources_json` naming
//!   `held_descendant`: the eager design's record of a conflicted parent's
//!   descendant) is superseded like any other, and each branch it held is
//!   named with its parent and session in the log and the `retired:`
//!   diagnostic. The hold itself is not lost: the preparer derives it from the
//!   parent's own open session at the branch's next demand
//!   ([`super::preparation::held_by_parent_conflict`]).
//!
//! A rewritten item changes only its outcome, status and (for an unfinished
//! item) observed tip; what it held before is prepended to `last_diagnostic`,
//! and session and request columns are never written. Literal heads are read
//! in-process under the store writer lock, after reserving exactly the
//! inspected bookmarks. The read never settles concurrent operation heads or
//! repairs a name: a store it cannot read literally defers the intent to the
//! next pass.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use cairn_db::turso::params;

use super::preparation::{OUTCOME_EXPORT_PENDING, OUTCOME_EXPORT_SUPERSEDED};
use crate::orchestrator::{Orchestrator, StoreLockClass};
use crate::storage::{LocalDb, RowExt};

/// How often the upkeep loop runs.
pub(crate) const UPKEEP_INTERVAL: Duration = Duration::from_secs(30);

/// How long a retirement waits for the inspected bookmarks' reservations, and
/// then for the store writer lock. A busy store defers one intent to the next
/// pass rather than holding up the intents behind it.
const RESERVATION_WAIT: Duration = Duration::from_secs(10);
const LOCK_WAIT: Duration = Duration::from_secs(10);

/// The prefix of a retired intent's `last_diagnostic`.
pub(crate) const RETIRED: &str = "retired:";

/// The legacy intent ids captured per database, keyed by the database handle.
#[derive(Default)]
pub(crate) struct LegacyQueue {
    captured: tokio::sync::Mutex<HashMap<usize, HashSet<String>>>,
}

impl LegacyQueue {
    /// The legacy ids of `db` still to retire, capturing them on first sight.
    async fn remaining(&self, db: &Arc<LocalDb>) -> Result<HashSet<String>, String> {
        let key = Arc::as_ptr(db) as usize;
        let mut captured = self.captured.lock().await;
        if let Some(ids) = captured.get(&key) {
            return Ok(ids.clone());
        }
        let ids = capture_legacy_intents(db).await?;
        if !ids.is_empty() {
            log::info!(
                "legacy reconcile retirement captured {} queued base-scoped intent(s)",
                ids.len()
            );
        }
        captured.insert(key, ids.clone());
        Ok(ids)
    }

    async fn settled(&self, db: &Arc<LocalDb>, ids: &[String]) {
        if let Some(remaining) = self
            .captured
            .lock()
            .await
            .get_mut(&(Arc::as_ptr(db) as usize))
        {
            for id in ids {
                remaining.remove(id);
            }
        }
    }
}

/// One upkeep pass over every database: attempt the recovery fences whose
/// repair backoff has elapsed, retire what the previous process queued, then
/// retry marker materializations still pending. Nothing here replays a branch
/// or wakes anyone; the one thing it sends is a fence's outcome notice to its
/// owning issue (and, for a store fence, the fleet threads). Returns whether
/// every open database's captured queue is now fully retired, which is when
/// the explicit replay requests the previous process accepted may be served.
pub(crate) async fn upkeep_pass(orch: &Orchestrator, queue: &LegacyQueue) -> bool {
    let mut complete = true;
    for db in orch.db.all_dbs().await {
        // First, so a due repair runs on its persisted backoff from startup and
        // every pass, with no operator in the loop. It reads and changes only
        // fenced records, and delivers their pending notices.
        crate::orchestrator::branch_recovery::run_due_recoveries(orch, &db).await;
        match queue.remaining(&db).await {
            Ok(ids) if !ids.is_empty() => {
                let (_, settled) = retire_legacy_reconcile_intents(orch, &db, &ids).await;
                queue.settled(&db, &settled).await;
                if !queue.remaining(&db).await.is_ok_and(|left| left.is_empty()) {
                    complete = false;
                }
            }
            Ok(_) => {}
            Err(error) => {
                complete = false;
                log::warn!("legacy reconcile retirement could not capture queued intents: {error}")
            }
        }
        super::retry_pending_materializations(orch, &db).await;
    }
    complete
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RetirementReport {
    pub(crate) retired: usize,
    pub(crate) export_recovery: usize,
    /// Intents left for the next pass because their store was busy or a head
    /// could not be read literally.
    pub(crate) deferred: usize,
}

/// The ids of every intent `pending` or `running` now.
pub(crate) async fn capture_legacy_intents(db: &LocalDb) -> Result<HashSet<String>, String> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id FROM jj_reconcile_intents WHERE status IN ('pending', 'running')",
                    (),
                )
                .await?;
            let mut ids = HashSet::new();
            while let Some(row) = rows.next().await? {
                ids.insert(row.text(0)?);
            }
            Ok(ids)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// Retire every captured intent still `pending` or `running`. Returns the
/// report and the ids that need no further pass (retired, or settled by
/// someone else).
pub(crate) async fn retire_legacy_reconcile_intents(
    orch: &Orchestrator,
    db: &LocalDb,
    captured: &HashSet<String>,
) -> (RetirementReport, Vec<String>) {
    let mut report = RetirementReport::default();
    let mut settled = Vec::new();
    let intents = match load_open(db).await {
        Ok(intents) => intents,
        Err(error) => {
            log::warn!("legacy reconcile retirement could not list queued intents: {error}");
            return (report, settled);
        }
    };
    let open: HashSet<&str> = intents.iter().map(|intent| intent.id.as_str()).collect();
    settled.extend(
        captured
            .iter()
            .filter(|id| !open.contains(id.as_str()))
            .cloned(),
    );
    for intent in intents
        .iter()
        .filter(|intent| captured.contains(&intent.id))
    {
        match retire_intent(orch, db, intent).await {
            Ok(retired) => {
                if let Some(exports) = retired {
                    report.retired += 1;
                    report.export_recovery += exports;
                }
                settled.push(intent.id.clone());
            }
            Err(error) => {
                report.deferred += 1;
                log::warn!(
                    "legacy reconcile intent {} ({} at {}) stays queued for the next upkeep \
                     pass: {error}",
                    intent.id,
                    intent.target_branch,
                    intent.destination_commit
                );
            }
        }
    }
    if report != RetirementReport::default() {
        log::info!(
            "legacy reconcile retirement: {} intent(s) superseded without replay, {} branch \
             export(s) left for exact-branch recovery, {} deferred",
            report.retired,
            report.export_recovery,
            report.deferred
        );
    }
    (report, settled)
}

#[derive(Debug)]
struct LegacyIntent {
    id: String,
    store_path: String,
    target_branch: String,
    destination_commit: String,
    status: String,
    held_descendant: bool,
}

impl LegacyIntent {
    /// One line per branch a held-descendant intent held, naming the branch,
    /// its parent and its session, so none is superseded silently.
    fn held_descendants(&self, items: &[LegacyItem]) -> Vec<String> {
        if !self.held_descendant {
            return Vec::new();
        }
        if items.is_empty() {
            return vec![format!(
                "held-descendant intent {} for parent `{}` at {} named no branch",
                self.id, self.target_branch, self.destination_commit
            )];
        }
        items
            .iter()
            .map(|item| {
                format!(
                    "held descendant `{}` of parent `{}` at {} (session {}, {})",
                    item.bookmark,
                    self.target_branch,
                    self.destination_commit,
                    self.id,
                    if item.open_session {
                        "open"
                    } else {
                        "no open session"
                    }
                )
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
struct LegacyItem {
    bookmark: String,
    status: String,
    outcome_kind: Option<String>,
    observed_tip: Option<String>,
    open_session: bool,
    replay_requested: bool,
}

impl LegacyItem {
    fn accepted_install(&self) -> bool {
        self.status == "graph_moved"
            && matches!(
                self.outcome_kind.as_deref(),
                Some("rebased_clean" | "silent")
            )
    }

    fn unfinished(&self) -> bool {
        matches!(self.status.as_str(), "pending" | "running")
    }

    fn inspected(&self) -> bool {
        !self.open_session
            && (self.accepted_install() || (self.unfinished() && !self.replay_requested))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Settlement {
    outcome: &'static str,
    observed_tip: String,
    /// `None` keeps the item's status.
    status: Option<&'static str>,
    note: String,
}

/// What one inspected item records, given its branch's literal head.
fn settle(item: &LegacyItem, head: Option<&str>) -> Option<Settlement> {
    let head = head?;
    let observed = item.observed_tip.as_deref();
    let was = |what: &str| {
        format!(
            "{RETIRED} legacy {what} (item {}, outcome {}, observed {}); literal head {head}",
            item.status,
            item.outcome_kind.as_deref().unwrap_or("none"),
            observed.unwrap_or("none"),
        )
    };
    if item.accepted_install() {
        return Some(match observed {
            Some(installed) if installed != head => Settlement {
                outcome: OUTCOME_EXPORT_SUPERSEDED,
                observed_tip: installed.to_string(),
                status: Some("completed"),
                note: was("install was superseded by a later operation, which owns its export"),
            },
            _ => Settlement {
                outcome: OUTCOME_EXPORT_PENDING,
                observed_tip: head.to_string(),
                status: Some("completed"),
                note: was(
                    "install is the literal head; its export is re-verified at the next demand",
                ),
            },
        });
    }
    if item.unfinished() && observed != Some(head) {
        return Some(Settlement {
            outcome: OUTCOME_EXPORT_PENDING,
            observed_tip: head.to_string(),
            status: None,
            note: was(
                "work was unfinished and the head moved; its export is re-verified at the next \
                 demand",
            ),
        });
    }
    None
}

async fn load_open(db: &LocalDb) -> Result<Vec<LegacyIntent>, String> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, store_path, target_branch, destination_commit, status,
                            COALESCE(trigger_sources_json, '') LIKE '%\"held_descendant\"%'
                       FROM jj_reconcile_intents
                      WHERE status IN ('pending', 'running')
                      ORDER BY created_at",
                    (),
                )
                .await?;
            let mut intents = Vec::new();
            while let Some(row) = rows.next().await? {
                intents.push(LegacyIntent {
                    id: row.text(0)?,
                    store_path: row.text(1)?,
                    target_branch: row.text(2)?,
                    destination_commit: row.text(3)?,
                    status: row.text(4)?,
                    held_descendant: row.get::<i64>(5)? != 0,
                });
            }
            Ok(intents)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

async fn load_items(db: &LocalDb, intent_id: &str) -> Result<Vec<LegacyItem>, String> {
    let intent_id = intent_id.to_string();
    db.read(move |conn| {
        let intent_id = intent_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT bookmark, status, outcome_kind, observed_tip,
                            COALESCE(resolution_state, '') = 'open',
                            replay_requested_at IS NOT NULL
                       FROM jj_reconcile_items WHERE intent_id = ?1",
                    (intent_id.as_str(),),
                )
                .await?;
            let mut items = Vec::new();
            while let Some(row) = rows.next().await? {
                items.push(LegacyItem {
                    bookmark: row.text(0)?,
                    status: row.text(1)?,
                    outcome_kind: row.opt_text(2)?,
                    observed_tip: row.opt_text(3)?,
                    open_session: row.get::<i64>(4)? != 0,
                    replay_requested: row.get::<i64>(5)? != 0,
                });
            }
            Ok(items)
        })
    })
    .await
    .map_err(|error| format!("read items: {error}"))
}

/// The literal head of `branch`, read in-process without settling operation
/// heads or repairing the name. `None` when the bookmark is absent.
async fn literal_head(store: &std::path::Path, branch: &str) -> Result<Option<String>, String> {
    match cairn_vcs::resolve_coordinate(store, branch).await {
        Ok(head) => Ok(Some(head)),
        Err(cairn_vcs::CoordinateResolutionError::Absent { .. }) => Ok(None),
        Err(error) => Err(format!("`{branch}` cannot be read literally: {error}")),
    }
}

/// Retire one intent. `Ok(Some(n))` is retired with `n` exports left for
/// recovery; `Ok(None)` means another writer settled it first.
async fn retire_intent(
    orch: &Orchestrator,
    db: &LocalDb,
    intent: &LegacyIntent,
) -> Result<Option<usize>, String> {
    let items = load_items(db, &intent.id).await?;
    let held = intent.held_descendants(&items);
    let inspected: Vec<LegacyItem> = items.into_iter().filter(LegacyItem::inspected).collect();
    let store = PathBuf::from(&intent.store_path);
    let purpose = format!("retire legacy reconcile intent {}", intent.id);
    let names: Vec<&str> = inspected
        .iter()
        .map(|item| item.bookmark.as_str())
        .collect();
    let _reservation = if names.is_empty() {
        None
    } else {
        Some(
            orch.reserve_branches(&store, &names, &purpose, RESERVATION_WAIT)
                .await?,
        )
    };
    let guard = orch
        .acquire_jj_store_lock_with_timeout(
            &store,
            StoreLockClass::Background,
            purpose.clone(),
            Some(LOCK_WAIT),
        )
        .await
        .map_err(|()| format!("the store writer lock stayed busy for {LOCK_WAIT:?}"))?;

    // A store that no longer exists has no head to export.
    let mut heads = HashMap::new();
    if store.exists() {
        let _phase = guard.phase("read literal heads");
        for name in &names {
            heads.insert(name.to_string(), literal_head(&store, name).await?);
        }
    }

    let settlements: Vec<(String, Settlement)> = inspected
        .iter()
        .filter_map(|item| {
            let head = heads.get(&item.bookmark).and_then(Option::as_deref);
            settle(item, head).map(|settled| (item.bookmark.clone(), settled))
        })
        .collect();
    let exports = settlements
        .iter()
        .filter(|(_, settled)| settled.outcome == OUTCOME_EXPORT_PENDING)
        .count();
    let mut diagnostic = format!(
        "{RETIRED} base advance is lazy; this {} base-scoped replay of `{}` at {} was \
         superseded at upgrade without expanding it into branches. Each branch is carried \
         onto its base at its own next commit, requested check or run, or merge.",
        intent.status, intent.target_branch, intent.destination_commit
    );
    if !held.is_empty() {
        diagnostic.push_str(&format!(
            " It superseded with it: {}. Each is held again by its parent's own open conflict \
             session at its next demand.",
            held.join("; ")
        ));
    }
    let intent_id = intent.id.clone();
    let now = chrono::Utc::now().timestamp();
    // Recorded under the guard so no writer moves an inspected head between
    // the read and the record.
    let retired = db
        .write(move |conn| {
            // Each retry owns its inputs: the future outlives this call.
            let (intent_id, diagnostic, settlements) =
                (intent_id.clone(), diagnostic.clone(), settlements.clone());
            Box::pin(async move {
                let changed = conn
                    .execute(
                        "UPDATE jj_reconcile_intents
                            SET status = 'superseded', lease_owner = NULL,
                                lease_expires_at = NULL, updated_at = ?3,
                                last_diagnostic = CASE
                                  WHEN COALESCE(last_diagnostic, '') = '' THEN ?2
                                  ELSE ?2 || char(10) || last_diagnostic END
                          WHERE id = ?1 AND status IN ('pending', 'running')",
                        params![intent_id.as_str(), diagnostic.as_str(), now],
                    )
                    .await?;
                if changed == 0 {
                    return Ok(false);
                }
                for (bookmark, settled) in &settlements {
                    conn.execute(
                        "UPDATE jj_reconcile_items
                            SET outcome_kind = ?3, observed_tip = ?4,
                                status = COALESCE(?5, status), updated_at = ?7,
                                last_diagnostic = CASE
                                  WHEN COALESCE(last_diagnostic, '') = '' THEN ?6
                                  ELSE ?6 || char(10) || last_diagnostic END
                          WHERE intent_id = ?1 AND bookmark = ?2",
                        params![
                            intent_id.as_str(),
                            bookmark.as_str(),
                            settled.outcome,
                            settled.observed_tip.as_str(),
                            settled.status,
                            settled.note.as_str(),
                            now
                        ],
                    )
                    .await?;
                }
                Ok(true)
            })
        })
        .await
        .map_err(|error| format!("record retirement: {error}"))?;
    drop(guard);
    if retired {
        for line in &held {
            log::warn!(
                "legacy reconcile retirement superseded intent {}: {line}",
                intent.id
            );
        }
    }
    Ok(retired.then_some(exports))
}

#[cfg(test)]
#[path = "retirement_tests.rs"]
mod tests;
