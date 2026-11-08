//! Durable recovery fences for budgeted reconcile rollbacks that could not be
//! verified, and the deferred targeted repair that clears them.
//!
//! A budgeted reconcile transaction restores its saved operation immediately,
//! under the original store lock (`cairn_vcs::jj::restore_operation`). When that
//! restore, its export, or their verification falls short, the transaction's
//! ledger carries typed [`RecoveryCoordinates`]. The holder of the store lock
//! persists them here BEFORE releasing it, so no queued writer can act on the
//! uncertain state first, and a restart finds the fence where it was left.
//!
//! A fence is cleared only by a verified repair: tip movement, a new base,
//! cooldown expiry, an explicit replay and ordinary reads leave it in place.
//! The repair runs from the reconcile sweep on a persisted backoff, five
//! minutes doubling to an hour, under the store lock with its own bounded
//! allowance. It never restores a historical operation; it moves only the
//! recorded bookmarks and their Git heads, by compare-and-swap on the current
//! view (`cairn_vcs::jj::repair_branch_recovery`). A store-scope fence is first
//! validated (`validate_store_recovery`) and downgraded to a branch fence only
//! on affirmative evidence.
//!
//! Not covered here, and owned by cairn/6201: a crash between the forward
//! mutation and this record, publishers already running when the fence is
//! written, and writers that bypass Cairn's managed boundaries.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use cairn_db::turso::params;
use cairn_vcs::jj::{RecoveryCoordinates, RecoveryScope, RepairVerdict, StoreValidation};

use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};

/// The budget class of the attempt that left a fence, as it is reported.
pub(crate) const BUDGET_AUTOMATIC: &str = "automatic";
pub(crate) const BUDGET_EXPLICIT: &str = "explicit";

/// One repair attempt's allowance under the store lock.
const REPAIR_ALLOWANCE: Duration = Duration::from_secs(20);

#[cfg(test)]
static TEST_REPAIR_ALLOWANCE_MS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

fn repair_allowance() -> Duration {
    #[cfg(test)]
    {
        let override_ms = TEST_REPAIR_ALLOWANCE_MS.load(std::sync::atomic::Ordering::SeqCst);
        if override_ms > 0 {
            return Duration::from_millis(override_ms);
        }
    }
    REPAIR_ALLOWANCE
}

/// Pauses a repair worker after it is admitted to write, for the rendezvous
/// test of a repair that outlives its allowance.
#[cfg(test)]
type RepairHook = Box<dyn Fn() + Send + Sync>;
#[cfg(test)]
static TEST_REPAIR_HOOK: Mutex<Option<RepairHook>> = Mutex::new(None);

/// Repair workers this process started, by recovery id, with whether each has
/// finished. A repair is in process and cannot be cancelled, so one that
/// outlives its allowance keeps running after the store lock is released; this
/// is what stops a later attempt from validating or clearing its fence while
/// it may still commit. A restart empties it, correctly: the worker died with
/// the process that ran it.
static ACTIVE_REPAIRS: Mutex<Vec<(String, std::sync::Arc<std::sync::atomic::AtomicBool>)>> =
    Mutex::new(Vec::new());

fn repair_worker_active(id: &str) -> bool {
    let mut active = ACTIVE_REPAIRS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    active.retain(|(_, finished)| !finished.load(std::sync::atomic::Ordering::SeqCst));
    active.iter().any(|(active_id, _)| active_id == id)
}

/// Where a recovery that could not be written to the database is kept, inside
/// the store's own `.jj` directory (which jj ignores), so it survives a restart
/// and every refusal sees it.
fn unrecorded_marker(store: &Path) -> PathBuf {
    store.join(".jj").join("cairn-unrecorded-recoveries.json")
}

/// One recovery the database refused, kept in the marker until migrated.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct UnrecordedRecovery {
    project_id: String,
    principal: Option<String>,
    budget_class: String,
    operation: String,
    coordinates: RecoveryCoordinates,
    reason: String,
    recorded_at: i64,
}

fn read_marker(store: &Path) -> Result<Vec<UnrecordedRecovery>, String> {
    match std::fs::read_to_string(unrecorded_marker(store)) {
        Ok(text) => serde_json::from_str(&text)
            .map_err(|error| format!("the unrecorded-recovery marker is unreadable: {error}")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(error) => Err(format!(
            "the unrecorded-recovery marker could not be read: {error}"
        )),
    }
}

fn write_marker(store: &Path, entries: &[UnrecordedRecovery]) -> Result<(), String> {
    let path = unrecorded_marker(store);
    if entries.is_empty() {
        return match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(format!("remove the unrecorded-recovery marker: {error}")),
        };
    }
    let text = serde_json::to_string_pretty(entries).map_err(|error| error.to_string())?;
    // Written whole and renamed into place, so a crash mid-write cannot leave
    // a marker that parses as fewer recoveries than it held.
    let staging = path.with_extension("json.tmp");
    std::fs::write(&staging, text)
        .and_then(|()| std::fs::rename(&staging, &path))
        .map_err(|error| format!("write the unrecorded-recovery marker: {error}"))
}

/// Forget everything this process holds about recoveries, as a restart does.
#[cfg(test)]
pub(crate) fn forget_process_state_for_test() {
    UNRECORDED
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clear();
}

/// Threads told about a store-scope fence: a whole-store fence is a project
/// outage, and these own where work runs and how models reach it.
const STORE_FENCE_THREADS: &[&str] = &["execution", "models"];

/// Stores whose recovery could not be recorded durably. Admission to them stays
/// refused for this process's lifetime: an unrecorded failure must not reopen
/// the store merely because the database write failed.
static UNRECORDED: Mutex<Vec<(PathBuf, String)>> = Mutex::new(Vec::new());

fn unrecorded_refusal(store: &Path) -> Option<String> {
    UNRECORDED
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .iter()
        .find(|(path, _)| path == store)
        .map(|(_, reason)| reason.clone())
}

/// Five minutes, doubling to an hour: the same curve as the reconcile timeout
/// cooldown, so a repair that keeps failing does not hammer a loaded store.
fn repair_backoff_seconds(attempts: i64) -> i64 {
    (300_i64 * (1_i64 << attempts.saturating_sub(1).clamp(0, 4))).min(3600)
}

#[derive(Debug, Clone)]
pub(crate) struct RecoveryRecord {
    pub(crate) id: String,
    pub(crate) project_id: String,
    pub(crate) store_path: String,
    pub(crate) scope: RecoveryScope,
    pub(crate) phase: String,
    pub(crate) budget_class: String,
    pub(crate) principal: Option<String>,
    pub(crate) operation: String,
    pub(crate) affected: Vec<String>,
    pub(crate) coordinates: RecoveryCoordinates,
    pub(crate) diagnostic: String,
    pub(crate) generation: i64,
    pub(crate) repair_attempts: i64,
    pub(crate) next_repair_at: i64,
    pub(crate) last_repair_outcome: Option<String>,
    pub(crate) created_at: i64,
}

impl RecoveryRecord {
    /// Whether this fence covers a mutation touching any of `bookmarks`.
    pub(crate) fn fences(&self, bookmarks: &[&str]) -> bool {
        self.scope == RecoveryScope::Store
            || bookmarks
                .iter()
                .any(|bookmark| self.affected.iter().any(|fenced| fenced == bookmark))
    }

    fn retry_time(&self) -> String {
        use chrono::TimeZone as _;
        match chrono::Local.timestamp_opt(self.next_repair_at, 0).single() {
            Some(at) => at.format("%H:%M %Z (UTC%:z)").to_string(),
            None => format!("Unix time {}", self.next_repair_at),
        }
    }

    /// The answer a refused write, replay or publication returns: which
    /// recovery, which bookmark, which budget class failed, and the one
    /// supported route.
    pub(crate) fn refusal(&self, bookmark: Option<&str>, action: &str) -> String {
        let subject = match (self.scope, bookmark) {
            (RecoveryScope::Store, _) => "this project's branch store".to_string(),
            (RecoveryScope::Branch, Some(bookmark)) => format!("`{bookmark}`"),
            (RecoveryScope::Branch, None) => format!("`{}`", self.affected.join("`, `")),
        };
        let route = match self.scope {
            RecoveryScope::Branch => format!(
                "Its targeted repair puts only the recorded bookmark(s) and Git head(s) back and \
                 retries automatically at {}; to retry it now, write cairn:~/rebase \
                 {{action:\"repair\"}}. Do not reset, force-push or replay around it.",
                self.retry_time()
            ),
            RecoveryScope::Store => format!(
                "Nothing may write or publish the store until its view is re-established; \
                 validation retries automatically at {}, and execution has been told. Reads \
                 and verdict-only branch runs are unaffected. Wait; do not reset or \
                 force-push.",
                self.retry_time()
            ),
        };
        format!(
            "{action} refused: {subject} is fenced by recovery {} ({} scope, phase {}, left by \
             an {} reconcile whose rollback could not be verified). {route}",
            self.id,
            self.scope.as_str(),
            self.phase,
            self.budget_class,
        )
    }

    fn notice(&self, outcome: &str) -> String {
        let survivors = if self.coordinates.survivors.is_empty() {
            String::new()
        } else {
            format!(
                "\nWriter(s) not proven gone: {}.",
                self.coordinates
                    .survivors
                    .iter()
                    .map(|survivor| format!(
                        "pid {} (group {}, start {})",
                        survivor.pid,
                        survivor.process_group,
                        survivor.start_token.as_deref().unwrap_or("unknown")
                    ))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        let owed = match self.scope {
            RecoveryScope::Store => {
                "Checks still owed: every recorded writer proven gone, then \
                                    the local view shown wholly before or wholly after the \
                                    failed restore, then each affected bookmark's Git head."
            }
            RecoveryScope::Branch => {
                "Checks still owed: each affected bookmark compare-and-swapped \
                                     back to its pre-attempt target, its Git head exported to \
                                     match, and every other bookmark and head unchanged."
            }
        };
        format!(
            "jj branch recovery {id}: {outcome}\nStore: {store}\nScope: {scope}; phase: {phase}; \
             budget class: {class}; operation: {operation}\nAffected bookmark(s): {affected}\n\
             Saved pre-attempt operation (diagnostic only, never restored later): {pre_op}{survivors}\n\
             {owed}\nNext automatic attempt: {retry}.\nDetail: {diagnostic}",
            id = self.id,
            store = self.store_path,
            scope = self.scope.as_str(),
            phase = self.phase,
            class = self.budget_class,
            operation = self.operation,
            affected = if self.affected.is_empty() {
                "<not established>".to_string()
            } else {
                self.affected.join(", ")
            },
            pre_op = self.coordinates.pre_op.as_deref().unwrap_or("<unknown>"),
            retry = self.retry_time(),
            diagnostic = self.diagnostic,
        )
    }
}

fn parse_scope(scope: &str) -> RecoveryScope {
    if scope == "branch" {
        RecoveryScope::Branch
    } else {
        RecoveryScope::Store
    }
}

const RECORD_COLUMNS: &str = "id, project_id, store_path, scope, phase, budget_class, principal, \
     operation, affected, coordinates, diagnostic, generation, repair_attempts, next_repair_at, \
     last_repair_outcome, notice_pending, created_at";

fn record_from_row(
    row: &cairn_db::turso::Row,
) -> Result<RecoveryRecord, cairn_db::storage::DbError> {
    let coordinates: String = row.text(9)?;
    let affected: String = row.text(8)?;
    let scope: String = row.text(3)?;
    let parse = |what: &str, error: serde_json::Error| {
        cairn_db::storage::DbError::Row(format!("jj_branch_recoveries.{what}: {error}"))
    };
    Ok(RecoveryRecord {
        id: row.text(0)?,
        project_id: row.text(1)?,
        store_path: row.text(2)?,
        scope: parse_scope(&scope),
        phase: row.text(4)?,
        budget_class: row.text(5)?,
        principal: row.opt_text(6)?,
        operation: row.text(7)?,
        affected: serde_json::from_str(&affected).map_err(|error| parse("affected", error))?,
        coordinates: serde_json::from_str(&coordinates)
            .map_err(|error| parse("coordinates", error))?,
        diagnostic: row.text(10)?,
        generation: row.i64(11)?,
        repair_attempts: row.i64(12)?,
        next_repair_at: row.i64(13)?,
        last_repair_outcome: row.opt_text(14)?,
        created_at: row.i64(16)?,
    })
}

/// Persist every recovery a transaction left. The caller still holds the
/// store lock and releases it only after this returns.
///
/// A failed database write falls back to a marker file in the store's `.jj`
/// directory, which every refusal honours and which survives a restart; the
/// sweep migrates it into the table, removing it only after the insert
/// succeeds. The store is also refused for this process's lifetime, so even a
/// marker that cannot be written leaves admission closed until a restart, and
/// the failure is returned for the caller to report. A crash before the
/// failure was observed at all is cairn/6201's pre-mutation journal.
pub(crate) async fn record_recoveries(
    db: &LocalDb,
    project_id: &str,
    store: &Path,
    principal: Option<&str>,
    budget_class: &str,
    operation: &str,
    recoveries: Vec<RecoveryCoordinates>,
) -> Result<Vec<RecoveryRecord>, String> {
    let mut recorded = Vec::new();
    for coordinates in recoveries {
        let now = chrono::Utc::now().timestamp();
        let record = RecoveryRecord {
            id: format!("rcv-{}", uuid::Uuid::new_v4().simple()),
            project_id: project_id.to_string(),
            store_path: store.to_string_lossy().into_owned(),
            scope: coordinates.scope,
            phase: coordinates.phase.as_str().to_string(),
            budget_class: budget_class.to_string(),
            principal: principal.map(str::to_string),
            operation: operation.to_string(),
            affected: coordinates.affected().map(str::to_string).collect(),
            diagnostic: coordinates.evidence.join("; "),
            coordinates,
            generation: 1,
            repair_attempts: 0,
            next_repair_at: now.saturating_add(repair_backoff_seconds(1)),
            last_repair_outcome: None,
            created_at: now,
        };
        let persisted = tokio::time::timeout(Duration::from_secs(5), insert(db, &record))
            .await
            .map_err(|_| "timed out persisting the recovery fence".to_string())
            .and_then(|result| result);
        if let Err(error) = persisted {
            let entry = UnrecordedRecovery {
                project_id: record.project_id.clone(),
                principal: record.principal.clone(),
                budget_class: record.budget_class.clone(),
                operation: record.operation.clone(),
                coordinates: record.coordinates.clone(),
                reason: error.clone(),
                recorded_at: now,
            };
            let marker = read_marker(store)
                .map(|mut entries| {
                    entries.push(entry);
                    entries
                })
                .and_then(|entries| write_marker(store, &entries));
            let reason = format!(
                "a rollback in this store could not be verified, and recording its recovery \
                 fence in the database failed ({error}); {}. Nothing may write or publish the \
                 store until the recovery is recorded and validated: {}",
                match &marker {
                    Ok(()) => format!(
                        "it is kept in {} until the sweep records it",
                        unrecorded_marker(store).display()
                    ),
                    Err(marker_error) => format!(
                        "the fallback marker could not be written either ({marker_error}), so \
                         this runner refuses the store until it restarts"
                    ),
                },
                record.coordinates.describe()
            );
            log::error!("jj branch recovery: {reason}");
            UNRECORDED
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push((store.to_path_buf(), reason.clone()));
            return Err(reason);
        }
        log::error!(
            "jj branch recovery {} recorded: store={} scope={} phase={} budget_class={} \
             principal={} affected=[{}] pre_op={} survivors={} evidence=[{}]",
            record.id,
            record.store_path,
            record.scope.as_str(),
            record.phase,
            record.budget_class,
            record.principal.as_deref().unwrap_or("-"),
            record.affected.join(","),
            record.coordinates.pre_op.as_deref().unwrap_or("-"),
            record.coordinates.survivors.len(),
            record.diagnostic,
        );
        recorded.push(record);
    }
    Ok(recorded)
}

async fn insert(db: &LocalDb, record: &RecoveryRecord) -> Result<(), String> {
    let affected = serde_json::to_string(&record.affected).map_err(|error| error.to_string())?;
    let coordinates =
        serde_json::to_string(&record.coordinates).map_err(|error| error.to_string())?;
    db.execute(
        "INSERT INTO jj_branch_recoveries
         (id, project_id, store_path, scope, status, phase, budget_class, principal, operation,
          affected, coordinates, diagnostic, generation, repair_attempts, next_repair_at,
          notice_pending, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, 'fenced', ?5, ?6, ?7, ?8, ?9, ?10, ?11, 1, 0, ?12, 1, ?13, ?13)",
        params![
            record.id.as_str(),
            record.project_id.as_str(),
            record.store_path.as_str(),
            record.scope.as_str(),
            record.phase.as_str(),
            record.budget_class.as_str(),
            record.principal.as_deref(),
            record.operation.as_str(),
            affected.as_str(),
            coordinates.as_str(),
            record.diagnostic.as_str(),
            record.next_repair_at,
            record.created_at
        ],
    )
    .await
    .map(|_| ())
    .map_err(|error| format!("persist branch recovery: {error}"))
}

async fn load(
    db: &LocalDb,
    filter: &'static str,
    value: String,
) -> Result<Vec<RecoveryRecord>, String> {
    db.read(move |conn| {
        let value = value.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    &format!("SELECT {RECORD_COLUMNS} FROM jj_branch_recoveries WHERE {filter}"),
                    params![value.as_str()],
                )
                .await?;
            let mut records = Vec::new();
            while let Some(row) = rows.next().await? {
                records.push(record_from_row(&row)?);
            }
            Ok(records)
        })
    })
    .await
    .map_err(|error| format!("load branch recoveries: {error}"))
}

/// Every fence standing on `store`.
pub(crate) async fn fences(db: &LocalDb, store: &Path) -> Result<Vec<RecoveryRecord>, String> {
    load(
        db,
        "store_path = ?1 AND status = 'fenced' ORDER BY created_at",
        store.to_string_lossy().into_owned(),
    )
    .await
}

/// Refuse `action` when a fence covers any of `bookmarks` (or the whole
/// store). Fails closed: a fence table that cannot be read refuses too.
///
/// Call it at the locked mutation boundary, after the store lock is held, so
/// a fence recorded by the transaction that held the lock just before is seen.
pub(crate) async fn refuse_if_fenced(
    db: &LocalDb,
    store: &Path,
    bookmarks: &[&str],
    action: &str,
) -> Result<(), String> {
    let fences = standing_fences(db, store, action).await?;
    match fences.iter().find(|fence| fence.fences(bookmarks)) {
        Some(fence) => {
            let bookmark = bookmarks
                .iter()
                .find(|bookmark| fence.affected.iter().any(|fenced| fenced == *bookmark))
                .copied();
            Err(fence.refusal(bookmark, action))
        }
        None => Ok(()),
    }
}

/// Every fence on `store`, after the refusals that stand outside the table:
/// this process's unrecorded failures and the restart-durable marker. Fails
/// closed on anything unreadable.
async fn standing_fences(
    db: &LocalDb,
    store: &Path,
    action: &str,
) -> Result<Vec<RecoveryRecord>, String> {
    if let Some(reason) = unrecorded_refusal(store) {
        return Err(format!("{action} refused: {reason}"));
    }
    match read_marker(store) {
        Ok(entries) if entries.is_empty() => {}
        Ok(entries) => {
            return Err(format!(
                "{action} refused: {} recovery record(s) for this store could not be written to \
                 the database and are held in {} until the sweep records them ({}). Nothing may \
                 write or publish the store until they are recorded and validated.",
                entries.len(),
                unrecorded_marker(store).display(),
                entries
                    .iter()
                    .map(|entry| entry.coordinates.describe())
                    .collect::<Vec<_>>()
                    .join("; ")
            ))
        }
        Err(error) => return Err(format!("{action} refused: {error}")),
    }
    fences(db, store).await.map_err(|error| {
        format!("{action} refused: the branch recovery fences could not be read ({error})")
    })
}

/// Store admission is the union of every open database's live fences. A
/// historical row in one database must not hide another database's obligation.
pub(crate) async fn preserved_import_bookmarks(
    orch: &Orchestrator,
    store: &Path,
    moving: &[&str],
    action: &str,
) -> Result<Vec<String>, String> {
    let mut preserved = std::collections::BTreeSet::new();
    for db in orch.db.all_dbs().await {
        preserved.extend(refuse_import_if_fenced(&db, store, moving, action).await?);
    }
    Ok(preserved.into_iter().collect())
}

pub(crate) async fn refuse_coordinate_if_fenced(
    orch: &Orchestrator,
    store: &Path,
    bookmarks: &[&str],
    action: &str,
) -> Result<(), String> {
    for db in orch.db.all_dbs().await {
        refuse_if_fenced(&db, store, bookmarks, action).await?;
    }
    Ok(())
}

/// Refuse an import of Git refs into `store` (an origin fast-forward, a
/// default-branch import, job preparation's import) that a fence forbids.
///
/// A store fence refuses every import. Explicitly moving a fenced branch also
/// refuses. Other branch fences become an import scope: their local and remote
/// Git inputs must remain unobserved until recovery settles them.
pub(crate) async fn refuse_import_if_fenced(
    db: &LocalDb,
    store: &Path,
    moving: &[&str],
    action: &str,
) -> Result<Vec<String>, String> {
    let fences = standing_fences(db, store, action).await?;
    if let Some(fence) = fences
        .iter()
        .find(|fence| fence.scope == RecoveryScope::Store || fence.fences(moving))
    {
        let bookmark = moving
            .iter()
            .find(|bookmark| fence.affected.iter().any(|fenced| fenced == *bookmark))
            .copied();
        return Err(fence.refusal(bookmark, action));
    }
    Ok(fences
        .iter()
        .flat_map(|fence| fence.affected.iter().cloned())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect())
}

/// Record every recovery a marker holds, removing each from the marker only
/// once its row is written. Entries for projects another database owns are
/// left for that database's sweep.
async fn migrate_unrecorded_markers(orch: &Orchestrator, db: &LocalDb) {
    let stores = match std::fs::read_dir(orch.config_dir.join("jj-stores")) {
        Ok(entries) => entries
            .flatten()
            .map(|entry| entry.path())
            .filter(|store| unrecorded_marker(store).exists())
            .collect::<Vec<_>>(),
        Err(_) => return,
    };
    for store in stores {
        let entries = match read_marker(&store) {
            Ok(entries) => entries,
            Err(error) => {
                log::error!(
                    "jj branch recovery: {} stays refused: {error}",
                    store.display()
                );
                continue;
            }
        };
        let mut remaining = Vec::new();
        for entry in entries {
            let owned = db
                .query_opt_i64(
                    "SELECT 1 FROM projects WHERE id = ?1",
                    params![entry.project_id.as_str()],
                )
                .await
                .ok()
                .flatten()
                .is_some();
            if !owned {
                remaining.push(entry);
                continue;
            }
            match record_recoveries_once(db, &store, &entry).await {
                Ok(id) => log::warn!(
                    "jj branch recovery {id}: recorded from the fallback marker of {} (the \
                     database write failed at {}: {})",
                    store.display(),
                    entry.recorded_at,
                    entry.reason
                ),
                Err(error) => {
                    log::warn!(
                        "jj branch recovery: marker entry for {} still not recorded: {error}",
                        store.display()
                    );
                    remaining.push(entry);
                }
            }
        }
        if let Err(error) = write_marker(&store, &remaining) {
            log::error!(
                "jj branch recovery: {} keeps its marker: {error}",
                store.display()
            );
        }
    }
}

async fn record_recoveries_once(
    db: &LocalDb,
    store: &Path,
    entry: &UnrecordedRecovery,
) -> Result<String, String> {
    let now = chrono::Utc::now().timestamp();
    let record = RecoveryRecord {
        id: format!("rcv-{}", uuid::Uuid::new_v4().simple()),
        project_id: entry.project_id.clone(),
        store_path: store.to_string_lossy().into_owned(),
        scope: entry.coordinates.scope,
        phase: entry.coordinates.phase.as_str().to_string(),
        budget_class: entry.budget_class.clone(),
        principal: entry.principal.clone(),
        operation: entry.operation.clone(),
        affected: entry.coordinates.affected().map(str::to_string).collect(),
        diagnostic: entry.coordinates.evidence.join("; "),
        coordinates: entry.coordinates.clone(),
        generation: 1,
        repair_attempts: 0,
        // Validated on its own backoff from now, like any new fence.
        next_repair_at: now,
        last_repair_outcome: None,
        created_at: now,
    };
    insert(db, &record).await.map(|()| record.id)
}

/// Bring the named branch's recoveries forward to now, for the managed
/// `{action:"repair"}` route. The attempt itself runs bounded, under the
/// store lock, through [`run_due_recoveries`].
pub(crate) async fn repair_now(
    orch: &Orchestrator,
    db: &LocalDb,
    store: &Path,
    branch: &str,
) -> Result<String, String> {
    let fenced: Vec<RecoveryRecord> = fences(db, store)
        .await?
        .into_iter()
        .filter(|fence| fence.fences(&[branch]))
        .collect();
    if fenced.is_empty() {
        return Ok(format!(
            "`{branch}` has no recovery fence; nothing to repair."
        ));
    }
    for fence in &fenced {
        db.execute(
            "UPDATE jj_branch_recoveries SET next_repair_at = ?2, updated_at = ?2
             WHERE id = ?1 AND status = 'fenced'",
            params![fence.id.as_str(), chrono::Utc::now().timestamp()],
        )
        .await
        .map_err(|error| format!("schedule recovery repair: {error}"))?;
    }
    run_due_recoveries(orch, db).await;
    let after = fences(db, store).await?;
    let lines: Vec<String> = fenced
        .iter()
        .map(|fence| match after.iter().find(|row| row.id == fence.id) {
            None => format!(
                "recovery {} was repaired and verified; the fence is cleared",
                fence.id
            ),
            Some(row) => format!(
                "recovery {} is still fenced ({} scope): {}; it retries automatically at {}",
                row.id,
                row.scope.as_str(),
                row.last_repair_outcome
                    .as_deref()
                    .unwrap_or("not attempted"),
                row.retry_time()
            ),
        })
        .collect();
    Ok(lines.join("\n"))
}

/// Affirmative lifecycle evidence for recreating a recorded, merged head.
/// Absence alone is not permission to export a branch again. No landing graph
/// proof is needed: squash merges need not contain the recorded commit, and
/// repair preserves that commit rather than deleting the bookmark.
async fn merged_missing_heads(
    orch: &Orchestrator,
    store: &Path,
    record: &RecoveryRecord,
) -> Result<BTreeSet<String>, String> {
    let mut merged_branches = BTreeSet::new();
    for bookmark in &record.coordinates.bookmarks {
        let Some(pre) = bookmark.pre.commit() else {
            continue;
        };
        if bookmark.pre_git.as_deref() != Some(pre) {
            continue;
        }
        let branch = &bookmark.bookmark;
        let mut lifecycle = crate::vcs_maintenance::BranchLifecycle::default();
        let mut merged = false;
        for db in orch.db.all_dbs().await {
            let jobs = db.query_all(
                "SELECT p.repo_path, j.status, i.status,
                        COALESCE(i.merged_at, i.closed_at, i.updated_at),
                        j.branch = p.default_branch,
                        EXISTS(SELECT 1 FROM runs r WHERE r.job_id = j.id AND r.status IN ('starting', 'live'))
                 FROM jobs j JOIN projects p ON p.id = j.project_id
                 LEFT JOIN issues i ON i.id = j.issue_id WHERE j.branch = ?1",
                params![branch.as_str()],
                |row| Ok((row.text(0)?, row.text(1)?, row.opt_text(2)?,
                    row.opt_i64(3)?, row.i64(4)? != 0, row.i64(5)? != 0)),
            ).await.map_err(|error| error.to_string())?;
            for (repo, status, issue_status, at, base, live_run) in jobs {
                if crate::jj::project_store_dir(&orch.config_dir, Path::new(&repo)) != store {
                    continue;
                }
                lifecycle.job(
                    &status,
                    issue_status.as_deref().map(|s| (s, at.unwrap_or(0))),
                );
                lifecycle.held |= base || live_run;
                merged |= issue_status.as_deref() == Some("merged");
            }
            let prs = db
                .query_all(
                    "SELECT p.repo_path, m.status, m.github_state,
                        i.status, COALESCE(i.merged_at, i.closed_at, i.updated_at),
                        COALESCE(m.merged_at, m.closed_at, m.updated_at),
                        m.source_branch = p.default_branch
                 FROM merge_requests m JOIN projects p ON p.id = m.project_id
                 LEFT JOIN issues i ON i.id = m.issue_id WHERE m.source_branch = ?1",
                    params![branch.as_str()],
                    |row| {
                        Ok((
                            row.text(0)?,
                            row.text(1)?,
                            row.opt_text(2)?,
                            row.opt_text(3)?,
                            row.opt_i64(4)?,
                            row.i64(5)?,
                            row.i64(6)? != 0,
                        ))
                    },
                )
                .await
                .map_err(|error| error.to_string())?;
            for (repo, status, github, issue, at, settled, base) in prs {
                if crate::jj::project_store_dir(&orch.config_dir, Path::new(&repo)) != store {
                    continue;
                }
                // Even an unpublished open PR holds the branch here.
                lifecycle.pull_request(
                    &status,
                    github.as_deref(),
                    true,
                    settled,
                    issue.as_deref().map(|s| (s, at.unwrap_or(0))),
                );
                lifecycle.held |= base;
                merged |= status == "merged"
                    || github
                        .as_deref()
                        .is_some_and(|s| s.eq_ignore_ascii_case("merged"));
            }
        }
        if merged && !lifecycle.held {
            merged_branches.insert(branch.clone());
        }
    }
    if merged_branches.is_empty() {
        return Ok(merged_branches);
    }
    let store = store.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let heads = cairn_vcs::jj::backing_branch_heads(&store)?;
        merged_branches.retain(|branch| !heads.contains_key(branch));
        Ok(merged_branches)
    })
    .await
    .map_err(|error| format!("read merged missing heads: {error}"))?
}

enum Attempt {
    Cleared(String),
    Downgraded(RecoveryCoordinates, String),
    /// The worker outlived its allowance and may still commit: the record
    /// becomes a store fence with fresh baselines, and the lock is released
    /// without waiting for it.
    Escalated(RecoveryCoordinates, String),
    Kept(String),
}

/// One bounded attempt, under the caller's store guard.
///
/// The worker is in process and cannot be cancelled, so the bound is kept by
/// not waiting for it: past the allowance the store is fenced whole, the
/// worker is left registered as active, and the guard can be released with
/// exclusion intact. No attempt runs while an earlier worker for the same
/// record is still alive, so nothing validates or clears a fence it might
/// still be changing.
async fn run_one_attempt(
    store: &Path,
    record: &RecoveryRecord,
    merged_missing: BTreeSet<String>,
) -> Attempt {
    if repair_worker_active(&record.id) {
        return Attempt::Kept(
            "an earlier repair worker for this recovery is still running; nothing is validated \
             or cleared until it finishes"
                .into(),
        );
    }
    let allowance = repair_allowance();
    let deadline = std::time::Instant::now() + allowance;
    let finished = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    ACTIVE_REPAIRS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .push((record.id.clone(), std::sync::Arc::clone(&finished)));
    let task_store = store.to_path_buf();
    let task_record = record.clone();
    let worker_finished = std::sync::Arc::clone(&finished);
    let task_merged_missing = merged_missing.clone();
    let admitted = std::sync::Arc::new(Mutex::new(None));
    let task_admitted = std::sync::Arc::clone(&admitted);
    let worker = tokio::task::spawn_blocking(move || {
        let outcome = attempt(
            &task_store,
            &task_record,
            deadline,
            &task_merged_missing,
            &task_admitted,
        );
        worker_finished.store(true, std::sync::atomic::Ordering::SeqCst);
        outcome
    });
    match tokio::time::timeout_at(deadline.into(), worker).await {
        Ok(Ok(outcome)) => outcome,
        Ok(Err(error)) => {
            finished.store(true, std::sync::atomic::Ordering::SeqCst);
            Attempt::Kept(format!("repair worker failed: {error}"))
        }
        Err(_) => {
            let detail = format!(
                "the repair outlived its {}s allowance and may still commit; the store is fenced \
                 whole until it has finished and the view is validated",
                allowance.as_secs_f64()
            );
            // Validation may have narrowed a store fence before admitting the
            // repair. Escalate its actual admitted coordinates, not a broader
            // snapshot with a different set of targets.
            let mut repairing = record.clone();
            if let Some(coordinates) = admitted
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
            {
                repairing.scope = RecoveryScope::Branch;
                repairing.coordinates = coordinates;
            }
            let coordinates = escalation_coordinates(store, &repairing).await;
            Attempt::Escalated(coordinates, detail)
        }
    }
}

/// Store-scope coordinates for an attempt that outlived its allowance, read
/// from the current view with a bound of its own. A read that does not answer
/// leaves no baseline, and the fence then stays until one can be taken.
async fn escalation_coordinates(store: &Path, record: &RecoveryRecord) -> RecoveryCoordinates {
    let mut fallback = record.coordinates.clone();
    fallback.scope = RecoveryScope::Store;
    fallback.phase = cairn_vcs::jj::RollbackFailurePhase::RepairInterrupted;
    if record.scope != RecoveryScope::Branch {
        // A store record keeps the baselines it was being validated against.
        fallback.evidence.push(
            "a validation or narrowed repair outlived its allowance; the store stays fenced".into(),
        );
        return fallback;
    }
    let read_store = store.to_path_buf();
    let branch = record.coordinates.clone();
    match tokio::time::timeout(
        Duration::from_secs(5),
        tokio::task::spawn_blocking(move || {
            cairn_vcs::jj::interrupted_repair_coordinates(&read_store, &branch)
        }),
    )
    .await
    {
        Ok(Ok(coordinates)) => coordinates,
        _ => {
            fallback.pre_view = None;
            fallback.failed_view = None;
            fallback.evidence.push(
                "a targeted repair outlived its allowance and the view could not be read in \
                 time to take baselines"
                    .into(),
            );
            fallback
        }
    }
}

fn attempt(
    store: &Path,
    record: &RecoveryRecord,
    deadline: std::time::Instant,
    merged_missing: &BTreeSet<String>,
    admitted: &Mutex<Option<RecoveryCoordinates>>,
) -> Attempt {
    let mut coordinates = record.coordinates.clone();
    let mut downgraded = false;
    if coordinates.scope == RecoveryScope::Store {
        match cairn_vcs::jj::validate_store_recovery(store, &coordinates) {
            StoreValidation::Resolved => {
                return Attempt::Cleared(
                    "validated: the store view and every Git head the snapshot held consistent \
                     are back at their pre-attempt positions"
                        .into(),
                )
            }
            StoreValidation::Downgrade(branch) => {
                coordinates = branch;
                downgraded = true;
            }
            StoreValidation::Uncertain(reason) => {
                return Attempt::Kept(format!("store view not established: {reason}"))
            }
        }
    }
    // Admission to write: an allowance already spent by validation is not
    // spent again on a write that may then outlive the lock.
    {
        let mut admitted = admitted
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if std::time::Instant::now() >= deadline {
            return Attempt::Kept(
                "the repair allowance was spent before the repair could start writing".into(),
            );
        }
        *admitted = Some(coordinates.clone());
    }
    #[cfg(test)]
    if let Some(hook) = TEST_REPAIR_HOOK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .as_ref()
    {
        hook();
    }
    match cairn_vcs::jj::repair_branch_recovery(store, &coordinates, &record.id, merged_missing) {
        RepairVerdict::Verified { operation_id } => Attempt::Cleared(format!(
            "targeted repair verified at operation {}",
            operation_id.as_deref().unwrap_or("<unknown>")
        )),
        RepairVerdict::Refused(reason) if downgraded => Attempt::Downgraded(
            coordinates,
            format!("validated and narrowed to a branch fence; repair not yet verified: {reason}"),
        ),
        RepairVerdict::Refused(reason) => Attempt::Kept(format!("repair not verified: {reason}")),
    }
}

/// Attempt every fence whose backoff has elapsed, and deliver pending notices.
/// Run from the reconcile sweep, so startup and the periodic sweep both reach
/// it without anyone poking the worker.
pub(crate) async fn run_due_recoveries(orch: &Orchestrator, db: &LocalDb) {
    migrate_unrecorded_markers(orch, db).await;
    let now = chrono::Utc::now().timestamp();
    let due = match load(
        db,
        "status = 'fenced' AND next_repair_at <= ?1",
        now.to_string(),
    )
    .await
    {
        Ok(due) => due,
        Err(error) => {
            log::warn!("jj branch recovery: could not enumerate due repairs: {error}");
            return;
        }
    };
    for due_record in due {
        let store = PathBuf::from(&due_record.store_path);
        let guard = orch
            .acquire_jj_store_lock(
                &store,
                crate::orchestrator::StoreLockClass::Background,
                format!("targeted branch recovery {}", due_record.id),
            )
            .await;
        // Re-read under the lock: another attempt may have repaired, escalated
        // or narrowed this record while this one waited for the guard.
        let record = match load(db, "id = ?1 AND status = 'fenced'", due_record.id.clone()).await {
            Ok(mut rows) if !rows.is_empty() => rows.remove(0),
            Ok(_) => continue,
            Err(error) => {
                log::warn!(
                    "jj branch recovery {}: could not re-read the record: {error}",
                    due_record.id
                );
                continue;
            }
        };
        let started = std::time::Instant::now();
        let outcome = {
            let _phase = guard.phase(format!(
                "recovery {} scope={} attempt={}",
                record.id,
                record.scope.as_str(),
                record.repair_attempts + 1
            ));
            match tokio::time::timeout(
                Duration::from_secs(5),
                merged_missing_heads(orch, &store, &record),
            )
            .await
            {
                Ok(Ok(merged_missing)) => run_one_attempt(&store, &record, merged_missing).await,
                Ok(Err(error)) => Attempt::Kept(format!(
                    "merged-head recreation evidence unavailable: {error}"
                )),
                Err(_) => Attempt::Kept(
                    "merged-head recreation evidence exceeded its 5s allowance".into(),
                ),
            }
        };
        let persisted = persist_attempt(db, &record, &outcome).await;
        drop(guard);
        log::warn!(
            "jj branch recovery {}: attempt {} took {:.2}s: {}",
            record.id,
            record.repair_attempts + 1,
            started.elapsed().as_secs_f64(),
            match &outcome {
                Attempt::Cleared(detail) => format!("cleared: {detail}"),
                Attempt::Downgraded(_, detail)
                | Attempt::Escalated(_, detail)
                | Attempt::Kept(detail) => detail.clone(),
            }
        );
        if let Err(error) = persisted {
            log::error!(
                "jj branch recovery {}: recording the attempt failed: {error}",
                record.id
            );
        }
    }
    deliver_pending_notices(orch, db).await;
}

async fn persist_attempt(
    db: &LocalDb,
    record: &RecoveryRecord,
    outcome: &Attempt,
) -> Result<(), String> {
    let now = chrono::Utc::now().timestamp();
    let attempts = record.repair_attempts + 1;
    let result = match outcome {
        Attempt::Cleared(detail) => {
            db.execute(
                "UPDATE jj_branch_recoveries SET status = 'repaired', repair_attempts = ?2,
                 last_repair_outcome = ?3, notice_pending = 1, repaired_at = ?4, updated_at = ?4
                 WHERE id = ?1 AND generation = ?5",
                params![
                    record.id.as_str(),
                    attempts,
                    detail.as_str(),
                    now,
                    record.generation
                ],
            )
            .await
        }
        Attempt::Downgraded(coordinates, detail) => {
            let affected: Vec<&str> = coordinates.affected().collect();
            let affected = serde_json::to_string(&affected).map_err(|error| error.to_string())?;
            let coordinates =
                serde_json::to_string(coordinates).map_err(|error| error.to_string())?;
            db.execute(
                "UPDATE jj_branch_recoveries SET scope = 'branch', affected = ?2, coordinates = ?3,
                 generation = generation + 1, repair_attempts = ?4, last_repair_outcome = ?5,
                 next_repair_at = ?6, notice_pending = 1, updated_at = ?7
                 WHERE id = ?1 AND generation = ?8",
                params![
                    record.id.as_str(),
                    affected.as_str(),
                    coordinates.as_str(),
                    attempts,
                    detail.as_str(),
                    now.saturating_add(repair_backoff_seconds(attempts)),
                    now,
                    record.generation
                ],
            )
            .await
        }
        Attempt::Escalated(coordinates, detail) => {
            let affected: Vec<&str> = coordinates.affected().collect();
            let affected = serde_json::to_string(&affected).map_err(|error| error.to_string())?;
            let phase = coordinates.phase.as_str();
            let coordinates =
                serde_json::to_string(coordinates).map_err(|error| error.to_string())?;
            db.execute(
                "UPDATE jj_branch_recoveries SET scope = 'store', phase = ?2, affected = ?3,
                 coordinates = ?4, generation = generation + 1, repair_attempts = ?5,
                 last_repair_outcome = ?6, next_repair_at = ?7, notice_pending = 1, updated_at = ?8
                 WHERE id = ?1 AND generation = ?9",
                params![
                    record.id.as_str(),
                    phase,
                    affected.as_str(),
                    coordinates.as_str(),
                    attempts,
                    detail.as_str(),
                    now.saturating_add(repair_backoff_seconds(attempts)),
                    now,
                    record.generation
                ],
            )
            .await
        }
        Attempt::Kept(detail) => {
            db.execute(
                "UPDATE jj_branch_recoveries SET repair_attempts = ?2, last_repair_outcome = ?3,
                 next_repair_at = ?4, notice_pending = 1, updated_at = ?5
                 WHERE id = ?1 AND generation = ?6",
                params![
                    record.id.as_str(),
                    attempts,
                    detail.as_str(),
                    now.saturating_add(repair_backoff_seconds(attempts)),
                    now,
                    record.generation
                ],
            )
            .await
        }
    };
    result
        .map(|_| ())
        .map_err(|error| format!("record recovery attempt: {error}"))
}

/// Deliver every pending notice, independent of the fence: a notice that
/// cannot be delivered is retried on the next sweep and never reopens a store.
pub(crate) async fn deliver_pending_notices(orch: &Orchestrator, db: &LocalDb) {
    let pending = match load(db, "notice_pending = ?1", "1".to_string()).await {
        Ok(pending) => pending,
        Err(error) => {
            log::warn!("jj branch recovery: could not enumerate pending notices: {error}");
            return;
        }
    };
    for record in pending {
        let status = db
            .query_opt_text(
                "SELECT status FROM jj_branch_recoveries WHERE id = ?1",
                params![record.id.as_str()],
            )
            .await
            .ok()
            .flatten()
            .unwrap_or_default();
        let outcome = match (status.as_str(), record.last_repair_outcome.as_deref()) {
            ("repaired", Some(detail)) => {
                format!("repaired and verified; the fence is cleared ({detail}).")
            }
            (_, Some(detail)) => format!(
                "still fenced after repair attempt {}: {detail}.",
                record.repair_attempts
            ),
            (_, None) => {
                "fence recorded. Admission to the affected bookmark(s) is refused until a \
                          verified repair clears it."
                    .to_string()
            }
        };
        let notice = record.notice(&outcome);
        match announce(orch, db, &record, &notice).await {
            Ok(()) => {
                if let Err(error) = db
                    .execute(
                        "UPDATE jj_branch_recoveries SET notice_pending = 0 WHERE id = ?1",
                        params![record.id.as_str()],
                    )
                    .await
                {
                    log::warn!(
                        "jj branch recovery {}: clearing notice flag failed: {error}",
                        record.id
                    );
                }
            }
            Err(error) => log::warn!(
                "jj branch recovery {}: notice not delivered, retried next sweep: {error}",
                record.id
            ),
        }
    }
}

async fn announce(
    orch: &Orchestrator,
    db: &LocalDb,
    record: &RecoveryRecord,
    notice: &str,
) -> Result<(), String> {
    // Owning issues of every affected branch (and the principal) hear about
    // their own branches.
    let mut branches: Vec<String> = record.affected.clone();
    if let Some(principal) = record.principal.as_ref() {
        if !branches.contains(principal) {
            branches.push(principal.clone());
        }
    }
    let project_id = record.project_id.clone();
    let content = notice.to_string();
    let owners_told = db
        .write(move |conn| {
            let project_id = project_id.clone();
            let branches = branches.clone();
            let content = content.clone();
            Box::pin(async move {
                let now = chrono::Utc::now().timestamp();
                let mut owners = std::collections::BTreeSet::new();
                for branch in &branches {
                    let mut rows = conn
                        .query(
                            "SELECT DISTINCT issue_id FROM jobs
                             WHERE project_id = ?1 AND branch = ?2 AND issue_id IS NOT NULL",
                            params![project_id.as_str(), branch.as_str()],
                        )
                        .await?;
                    while let Some(row) = rows.next().await? {
                        owners.insert(row.text(0)?);
                    }
                }
                for issue_id in &owners {
                    crate::issues::comments::insert_conn(
                        conn,
                        &cairn_common::ids::mint_child(issue_id),
                        issue_id,
                        &content,
                        "agent",
                        now,
                    )
                    .await?;
                }
                Ok(owners.len())
            })
        })
        .await
        .map_err(|error| format!("owner notice: {error}"))?;
    if owners_told > 0 {
        let _ = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table":"comments", "action":"insert"}),
        );
    }
    // A whole-store fence is an outage of the project's branch store; so is a
    // fence nobody owns. Either reaches the threads that run the fleet.
    if record.scope == RecoveryScope::Store || owners_told == 0 {
        let mut delivered = 0;
        for name in STORE_FENCE_THREADS {
            let project_id = record.project_id.clone();
            let name = name.to_string();
            let thread = db
                .read(move |conn| {
                    let project_id = project_id.clone();
                    let name = name.clone();
                    Box::pin(async move {
                        crate::threads::thread_id_by_name_conn(conn, &project_id, &name).await
                    })
                })
                .await
                .map_err(|error| format!("resolve notice thread: {error}"))?;
            if let Some(thread_id) = thread {
                crate::messages::delivery::append_thread_message(
                    orch,
                    db,
                    &thread_id,
                    None,
                    "jj branch recovery",
                    notice,
                )
                .await?;
                delivered += 1;
            }
        }
        if delivered == 0 && owners_told == 0 {
            // No owner and no fleet thread: the log line is the notice, and the
            // rebase page and refusals still carry every coordinate.
            log::error!(
                "jj branch recovery {} (no owner or fleet thread): {notice}",
                record.id
            );
        }
    }
    Ok(())
}

/// The fences a rebase page shows for `branch`.
pub(crate) async fn fences_for_branch(
    db: &LocalDb,
    store: &Path,
    branch: &str,
) -> Result<Vec<RecoveryRecord>, String> {
    Ok(fences(db, store)
        .await?
        .into_iter()
        .filter(|fence| fence.fences(&[branch]))
        .collect())
}

/// One fence rendered for the rebase page.
pub(crate) fn render_for_page(record: &RecoveryRecord) -> String {
    format!(
        "- Recovery `{}`: {} scope, phase `{}`, {} budget, affected {}; saved operation `{}`; \
         repair attempts {}{}. Next automatic attempt at {}. Retry now with \
         `{{action:\"repair\"}}`.",
        record.id,
        record.scope.as_str(),
        record.phase,
        record.budget_class,
        if record.affected.is_empty() {
            "<not established>".to_string()
        } else {
            record
                .affected
                .iter()
                .map(|name| format!("`{name}`"))
                .collect::<Vec<_>>()
                .join(", ")
        },
        record.coordinates.pre_op.as_deref().unwrap_or("unknown"),
        record.repair_attempts,
        record
            .last_repair_outcome
            .as_deref()
            .map(|outcome| format!(" (last: {outcome})"))
            .unwrap_or_default(),
        record.retry_time(),
    )
}

/// Test controls for the repair rendezvous.
#[cfg(test)]
pub(crate) mod test_support {
    use super::*;

    /// Restores the default allowance and removes the hook when dropped.
    pub(crate) struct RepairPause;

    impl Drop for RepairPause {
        fn drop(&mut self) {
            TEST_REPAIR_ALLOWANCE_MS.store(0, std::sync::atomic::Ordering::SeqCst);
            *TEST_REPAIR_HOOK
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
        }
    }

    /// Shorten the repair allowance and run `hook` in each repair worker once
    /// it is admitted to write, before it writes anything.
    pub(crate) fn pause_repairs_after_write_admission(
        allowance: Duration,
        hook: impl Fn() + Send + Sync + 'static,
    ) -> RepairPause {
        TEST_REPAIR_ALLOWANCE_MS.store(
            allowance.as_millis() as u64,
            std::sync::atomic::Ordering::SeqCst,
        );
        *TEST_REPAIR_HOOK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Box::new(hook));
        RepairPause
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repair_backoff_starts_at_five_minutes_and_caps_at_an_hour() {
        assert_eq!(repair_backoff_seconds(1), 300);
        assert_eq!(repair_backoff_seconds(2), 600);
        assert_eq!(repair_backoff_seconds(5), 3600);
        assert_eq!(repair_backoff_seconds(50), 3600);
    }
}
