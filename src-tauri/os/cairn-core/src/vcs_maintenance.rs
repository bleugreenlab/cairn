//! Ref-store maintenance: keeping each project's shared Git ref store and jj
//! operation log bounded, and showing an operator how big they are.
//!
//! A project's jj store uses the project checkout's own `.git` as its backend,
//! so every publication, check batch, executor rescue, and the operator's own
//! git share one `packed-refs`. jj protects every commit it writes with a
//! `refs/jj/keep` ref and snapshots every git ref into each operation's view,
//! and nothing ever pruned either. This pass does.
//!
//! Every hour it takes a lock-free census of each store's refs and of its Git
//! object database (packs, kept packs, loose objects, the multi-pack index)
//! and records it. Once a day it prunes each store:
//!
//! 1. Cairn's own refs whose lifecycle ended: `refs/cairn/checks/*` that no
//!    check batch holds, `refs/cairn/stranded/*` whose issue was resolved more
//!    than the retention window ago, and `refs/cairn/rescued/*` older than it.
//! 2. Under the store lock, the store's view. First the origin tracking ref of
//!    every agent branch Cairn's records say is finished
//!    ([`BranchLifecycle`]) is retired with its `<branch>@origin` remote
//!    bookmark (`cairn_vcs::jj::retire_remote_tracking_refs`): the store never
//!    prunes its fetches, so without this a tracking ref outlives the branch
//!    GitHub deleted after the merge and anchors its history for good. The
//!    remote's advertisement is never the key, because it can lag the store's
//!    own push. Then every visible commit that no bookmark, remote bookmark,
//!    tag, working copy, or base reaches is hidden, after any bookmark
//!    conflicted between two replays of one branch is settled on the newer one
//!    (`cairn_vcs::jj::sweep_store_view`). Superseded history that some step
//!    made visible again would otherwise stay visible for good, as divergent
//!    twins and unreferenced heads that keep their objects alive. Hiding
//!    changes only the view; objects go only through step 4's reclaim.
//! 3. At most weekly, under the store lock, the operation log older than the
//!    window.
//! 4. Outside the store lock, unreachable operations and keep refs for anything
//!    that is not an index head; then, at most once per window and outside
//!    every Cairn lock, a cruft repack that reclaims unreferenced Git objects
//!    older than the window. `cairn_vcs::jj::reclaim_unreachable_objects`
//!    documents how it honours the operator's reflogs, stash, and branches.
//!
//! It never prunes while a reconcile intent for the store is pending or
//! running, or while the operator's git holds `index.lock` or
//! `packed-refs.lock`; a skipped prune is recorded and retried next hour.
//!
//! Separately from the prune, every hour, the object database is compacted
//! whenever git's own auto-gc thresholds trip ([`compaction_due`]): small packs
//! are rolled up geometrically under a multi-pack index. Compaction removes
//! nothing, so it takes no lock and waits for no confirmation; only an
//! opted-out store is left alone. It does wait out the operator's own git
//! ([`object_store_busy`]). Staying under those thresholds is also what keeps
//! the operator's own `git gc --auto` from deciding to repack the whole store.
//!
//! Pruning is the default. The one gate is the first prune of a store that is
//! already above a size threshold: that rewrites a large, long-lived ref store
//! and object database in one go, so it waits for a single confirmation
//! (`refStorePrune: true` on the project resource) and keeps measuring until it
//! gets one. `refStorePrune: false` opts a store out: it is measured and never
//! pruned. See [`prune_gate`].

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cairn_db::turso::params;
use serde_json::json;

use crate::jj::{
    ObjectStoreCensus, ObjectStoreCompaction, OperationLogTruncation, PackedRefs, RefStoreCensus,
    RefTarget, StoreGarbageCollection,
};
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};

const DAY: u64 = 24 * 60 * 60;

/// Hidden history, operations, and unreferenced objects younger than this are
/// kept. A commit older than it that nothing references any more (a rewritten
/// or abandoned commit cited in an old issue) stops resolving.
pub(crate) const RETENTION: Duration = Duration::from_secs(30 * DAY);

/// How long a loose keep ref is spared without being an index head: long enough
/// for a writer to commit the operation that names its commit.
const KEEP_REF_GRACE: Duration = Duration::from_secs(60 * 60);

/// A check ref no batch in this process holds is spared this long, for a batch
/// another process on this machine (a dev instance) may still be running.
const CHECK_REF_GRACE: Duration = Duration::from_secs(60 * 60);

const MEASURE_INTERVAL: Duration = Duration::from_secs(60 * 60);
pub(crate) const PRUNE_INTERVAL: Duration = Duration::from_secs(DAY);
/// Truncation rewrites every retained operation and reindexes the store under
/// the store lock (about a minute on the Cairn project itself), so it runs at
/// most weekly.
const TRUNCATE_INTERVAL: Duration = Duration::from_secs(7 * DAY);
/// An enabled store whose operation log has gone this long without a
/// truncation is not being bounded, whatever its ref sweeps report.
pub(crate) const TRUNCATION_OVERDUE: Duration = Duration::from_secs(14 * DAY);
/// An enabled store whose last successful prune is older than this is overdue.
pub(crate) const PRUNE_OVERDUE: Duration = Duration::from_secs(2 * DAY);

/// Above either threshold a prune is due at the next hourly pass, whatever the
/// daily schedule says.
pub(crate) const KEEP_REF_THRESHOLD: u64 = 50_000;
pub(crate) const PACKED_BYTES_THRESHOLD: u64 = 4 * 1024 * 1024;

/// Packs (kept packs aside) above which compaction runs: `gc.autoPackLimit`'s
/// default. Past it the operator's own `git gc --auto` repacks everything into
/// one pack, rewriting the base pack (28 minutes on the Cairn project), and
/// every object lookup pays to open each pack; git does not count kept packs,
/// and neither does this.
pub(crate) const PACK_LIMIT: u64 = 50;
/// Loose objects, estimated the way git estimates them, above which compaction
/// runs: `gc.auto`'s default, past which `git gc --auto` runs.
pub(crate) const LOOSE_OBJECT_LIMIT: u64 = 6_700;
/// The cruft reclaim rewrites the base pack, so it runs at most once per
/// retention window. An object is dropped by the first reclaim after it is a
/// window older than its last repack (see `cairn_vcs::jj` maintenance for why
/// compaction restamps it). Garbage written just after one reclaim is moved
/// into a cruft pack by the next and dropped by the one after, so it lives
/// between one and about two windows.
pub(crate) const RECLAIM_INTERVAL: Duration = RETENTION;
/// An `index.lock` or `packed-refs.lock` older than this is a leftover of an
/// interrupted writer, not the operator's git at work. Such leftovers do
/// accumulate in a busy store, and treating each as busy would skip object
/// maintenance for good.
const STALE_LOCK: Duration = Duration::from_secs(600);
/// git's own rule: a `gc.pid` older than twelve hours belongs to a gc that
/// died. A live gc repacking a large store legitimately runs for half an hour.
const STALE_GC_PID: Duration = Duration::from_secs(12 * 60 * 60);

const STORE_LOCK_WAIT: Duration = Duration::from_secs(600);

/// A commit younger than this is never hidden by the view sweep: a writer
/// outside the store lock may not yet have named it.
const VIEW_SWEEP_GRACE: Duration = Duration::from_secs(60 * 60);

/// A branch finished more recently than this keeps its tracking ref: the merge
/// or close that finished it may still be pushing or deleting it.
const FINISHED_BRANCH_GRACE: Duration = Duration::from_secs(60 * 60);

/// The remote whose tracking refs of finished branches the prune retires.
const TRACKING_REMOTE: &str = "origin";

const CHECK_PREFIX: &str = "refs/cairn/checks/";
const STRANDED_PREFIX: &str = "refs/cairn/stranded/";
const RESCUED_PREFIX: &str = "refs/cairn/rescued/";

/// One project store on this machine.
#[derive(Debug, Clone)]
pub(crate) struct ProjectStore {
    pub(crate) project_id: String,
    pub(crate) store: PathBuf,
    /// The base branches of every project the store serves.
    pub(crate) base_branches: Vec<String>,
}

impl Orchestrator {
    /// Measure every project's ref store hourly and prune the enabled ones
    /// daily. Each pass is its own task, so a panic in one is logged and the
    /// next hour's pass still runs.
    pub fn spawn_ref_store_maintenance(&self) {
        let orch = self.clone();
        tokio::spawn(async move {
            loop {
                let pass = orch.clone();
                let joined = crate::orchestrator::base_advance::supervised_sweep_pass(async move {
                    run_pass(&pass).await;
                })
                .await;
                if let Err(error) = joined {
                    log::error!("ref-store maintenance pass ended abnormally: {error}");
                }
                tokio::time::sleep(MEASURE_INTERVAL).await;
            }
        });
    }
}

async fn run_pass(orch: &Orchestrator) {
    for project in project_stores(orch).await {
        let started = unix_now();
        let (census, objects) = match census_store(&project.store).await {
            Ok(Some((_, census, objects))) => (census, objects),
            Ok(None) => continue,
            Err(error) => {
                log::warn!(
                    "ref-store census of {} failed: {error}",
                    project.store.display()
                );
                continue;
            }
        };
        let db = &orch.db.local;
        record_measurement(db, &project, &census, &objects, started).await;
        let last_ok = last_successful_prune(db, &project.store, false).await;
        let gate = prune_gate(
            prune_policy(db, &project.store).await,
            last_ok.is_some(),
            &census,
        );
        match gate {
            PruneGate::OptedOut => {}
            PruneGate::AwaitingConfirmation => record_awaiting_confirmation(db, &project).await,
            PruneGate::Open => {
                if prune_is_due(&census, last_ok, unix_now()) {
                    prune(orch, &project).await;
                }
            }
        }
        if compaction_allowed(gate) {
            compact_if_due(db, &project).await;
        }
    }
}

/// Compaction removes nothing, so it needs no confirmation; only a store the
/// operator opted out is left untouched.
pub(crate) fn compaction_allowed(gate: PruneGate) -> bool {
    gate != PruneGate::OptedOut
}

/// Whether git's own auto-gc thresholds have tripped.
pub(crate) fn compaction_due(census: &ObjectStoreCensus) -> bool {
    census.packs.saturating_sub(census.kept_packs) > PACK_LIMIT
        || census.loose_objects > LOOSE_OBJECT_LIMIT
}

/// Whether a prune should also reclaim unreferenced objects.
pub(crate) fn reclaim_is_due(last_reclaim: Option<i64>, now: i64) -> bool {
    last_reclaim.is_none_or(|last| now - last >= RECLAIM_INTERVAL.as_secs() as i64)
}

/// Compact the store's object database if a fresh census says it is due. The
/// census is taken again here because a prune that just reclaimed has left the
/// store compacted.
async fn compact_if_due(db: &LocalDb, project: &ProjectStore) {
    let store = project.store.clone();
    let census = blocking(move || {
        let git_dir = crate::jj::store_git_dir(&store)
            .ok_or_else(|| "the store has no git repository behind it".to_string())?;
        let census = crate::jj::census_object_store(&git_dir)?;
        Ok((git_dir, census))
    })
    .await;
    match census {
        Ok((git_dir, census)) if compaction_due(&census) => {
            compact(db, project, &git_dir, census).await
        }
        Ok(_) => {}
        Err(error) => log::warn!(
            "object-store census of {} failed: {error}",
            project.store.display()
        ),
    }
}

/// Run one geometric compaction of the store's object database and record it.
/// It holds no Cairn lock: it never drops an object, so every reader and
/// writer may run beside it.
pub(crate) async fn compact(
    db: &LocalDb,
    project: &ProjectStore,
    git_dir: &Path,
    before: ObjectStoreCensus,
) {
    let started = unix_now();
    let result = match object_store_busy(git_dir, SystemTime::now()) {
        Some(lock) => Err(format!("skipped:operator-git-busy:{lock}")),
        None => blocking({
            let git_dir = git_dir.to_path_buf();
            move || crate::jj::compact_object_store(&git_dir)
        })
        .await
        .map_err(failed),
    };
    log_object_step(&project.store, "compaction", &result);
    record_compaction(db, project, started, &result, &before).await;
}

/// What the operator's own git holds that an object repack must not race,
/// named with its age so a leftover lock is visible as one: a `gc.pid` younger
/// than [`STALE_GC_PID`] (the operator's own gc is repacking), or an
/// `index.lock` or `packed-refs.lock` younger than [`STALE_LOCK`]. An older
/// lock is logged as a leftover and not waited for.
pub(crate) fn object_store_busy(git_dir: &Path, now: SystemTime) -> Option<String> {
    let age = |lock: &str| {
        let meta = git_dir.join(lock).metadata().ok()?;
        // A lock whose age cannot be read counts as just taken.
        Some(
            meta.modified()
                .ok()
                .and_then(|modified| now.duration_since(modified).ok())
                .unwrap_or_default(),
        )
    };
    let held = |lock: &str, age: Duration| {
        format!(
            "{lock} ({} old)",
            crate::clock::format_elapsed(age.as_secs() as i64)
        )
    };
    for (lock, stale) in [
        ("gc.pid", STALE_GC_PID),
        ("index.lock", STALE_LOCK),
        ("packed-refs.lock", STALE_LOCK),
    ] {
        match age(lock) {
            Some(age) if age < stale => return Some(held(lock, age)),
            Some(age) => log::warn!(
                "{} holds a leftover {}; object maintenance does not wait for it",
                git_dir.display(),
                held(lock, age)
            ),
            None => {}
        }
    }
    None
}

/// One line per compaction or reclaim: what it changed and how long it took, or
/// why it did not run.
fn log_object_step(store: &Path, mode: &str, result: &Result<ObjectStoreCompaction, String>) {
    match result {
        Ok(step) => log::info!(
            "object-store {mode} of {}: {} packs to {}, {} to {} pack bytes, about {} to {} loose \
             objects, in {:.1}s",
            store.display(),
            step.before.packs,
            step.after.packs,
            step.before.pack_bytes,
            step.after.pack_bytes,
            step.before.loose_objects,
            step.after.loose_objects,
            step.duration.as_secs_f64(),
        ),
        Err(outcome) => log::warn!("object-store {mode} of {}: {outcome}", store.display()),
    }
}

/// How this machine treats a store's prune, as set through `refStorePrune`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum PrunePolicy {
    /// No choice recorded: prune, except a first prune of a store over a
    /// threshold, which waits for confirmation.
    #[default]
    Default,
    /// Pruning confirmed, which also clears the first-prune gate.
    Confirmed,
    /// Measured, never pruned.
    OptedOut,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PruneGate {
    Open,
    AwaitingConfirmation,
    OptedOut,
}

pub(crate) const AWAITING_CONFIRMATION: &str = "skipped:awaiting-first-prune-confirmation";

/// Whether a store may be pruned.
///
/// Pruning is the default, because the writes that grow a store are what must
/// bound it, with nobody having to remember a switch. The one exception is the
/// risk this pass was built around: the first prune of a store that is already
/// above a threshold rewrites a large, long-lived ref store and object
/// database in one go, so it waits for a single confirmation. A store that has
/// been pruned once has been pruned safely, and later growth needs nothing.
pub(crate) fn prune_gate(
    policy: PrunePolicy,
    ever_pruned: bool,
    census: &RefStoreCensus,
) -> PruneGate {
    match policy {
        PrunePolicy::OptedOut => PruneGate::OptedOut,
        PrunePolicy::Confirmed => PruneGate::Open,
        PrunePolicy::Default if ever_pruned || !over_threshold(census) => PruneGate::Open,
        PrunePolicy::Default => PruneGate::AwaitingConfirmation,
    }
}

pub(crate) fn over_threshold(census: &RefStoreCensus) -> bool {
    keep_refs(census) > KEEP_REF_THRESHOLD || census.packed_bytes > PACKED_BYTES_THRESHOLD
}

/// Record that the store is waiting for its first-prune confirmation, at most
/// once a day, so the wait is visible without a row every hour.
async fn record_awaiting_confirmation(db: &LocalDb, project: &ProjectStore) {
    let now = unix_now();
    let recent = db
        .query_opt(
            "SELECT 1 FROM ref_store_samples
             WHERE store_path = ?1 AND kind = 'prune' AND outcome = ?2 AND started_at > ?3
             LIMIT 1",
            params![
                store_key(&project.store),
                AWAITING_CONFIRMATION,
                now - PRUNE_INTERVAL.as_secs() as i64
            ],
            |row| row.i64(0),
        )
        .await;
    if matches!(recent, Ok(None)) {
        record_prune(
            db,
            project,
            now,
            AWAITING_CONFIRMATION,
            &PruneReport::default(),
        )
        .await;
    }
}

/// Every project with a repository on this machine and a jj store beside it,
/// once per store however many databases name it.
async fn project_stores(orch: &Orchestrator) -> Vec<ProjectStore> {
    let mut stores: Vec<ProjectStore> = Vec::new();
    for db in orch.db.all_dbs().await {
        let Ok(rows) = db
            .query_all(
                "SELECT id, repo_path, COALESCE(default_branch, '') FROM projects
                 WHERE repo_path != ''",
                (),
                |row| Ok((row.text(0)?, row.text(1)?, row.text(2)?)),
            )
            .await
        else {
            continue;
        };
        for (project_id, repo_path, base) in rows {
            let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(&repo_path));
            if !store.join(".jj").is_dir() {
                continue;
            }
            let index = match stores.iter().position(|known| known.store == store) {
                Some(index) => index,
                None => {
                    stores.push(ProjectStore {
                        project_id,
                        store,
                        base_branches: Vec::new(),
                    });
                    stores.len() - 1
                }
            };
            let bases = &mut stores[index].base_branches;
            if !base.is_empty() && !bases.contains(&base) {
                bases.push(base);
            }
        }
    }
    stores
}

/// The store's git directory and a census of its refs and objects, or `None`
/// when the store has no git backend this pass can resolve.
async fn census_store(
    store: &Path,
) -> Result<Option<(PathBuf, RefStoreCensus, ObjectStoreCensus)>, String> {
    let store = store.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let Some(git_dir) = crate::jj::store_git_dir(&store) else {
            return Ok(None);
        };
        let census = crate::jj::census_ref_store(&git_dir)?;
        let objects = crate::jj::census_object_store(&git_dir)?;
        Ok(Some((git_dir, census, objects)))
    })
    .await
    .map_err(|error| format!("census task failed: {error}"))?
}

/// The key a store is recorded under: its path as the store lock names it.
fn store_key(store: &Path) -> String {
    store.to_string_lossy().into_owned()
}

fn namespaces_json(census: &RefStoreCensus) -> String {
    let map: serde_json::Map<String, serde_json::Value> = census
        .namespaces
        .iter()
        .map(|(namespace, refs)| {
            (
                namespace.clone(),
                json!({ "packed": refs.packed, "loose": refs.loose }),
            )
        })
        .collect();
    serde_json::Value::Object(map).to_string()
}

fn keep_refs(census: &RefStoreCensus) -> u64 {
    census
        .namespaces
        .get("jj/keep")
        .map(|refs| refs.packed + refs.loose)
        .unwrap_or(0)
}

fn objects_json(census: &ObjectStoreCensus) -> serde_json::Value {
    json!({
        "packs": census.packs,
        "kept_packs": census.kept_packs,
        "cruft_packs": census.cruft_packs,
        "pack_bytes": census.pack_bytes,
        "loose_objects": census.loose_objects,
        "multi_pack_index": census.multi_pack_index,
    })
}

fn objects_from_json(value: &serde_json::Value) -> Option<ObjectStoreCensus> {
    let count = |key: &str| value.get(key).and_then(serde_json::Value::as_u64);
    Some(ObjectStoreCensus {
        packs: count("packs")?,
        kept_packs: count("kept_packs").unwrap_or(0),
        cruft_packs: count("cruft_packs").unwrap_or(0),
        pack_bytes: count("pack_bytes").unwrap_or(0),
        loose_objects: count("loose_objects").unwrap_or(0),
        multi_pack_index: value
            .get("multi_pack_index")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
    })
}

async fn record_measurement(
    db: &LocalDb,
    project: &ProjectStore,
    census: &RefStoreCensus,
    objects: &ObjectStoreCensus,
    started: i64,
) {
    let finished = unix_now();
    let result = db
        .execute(
            "INSERT INTO ref_store_samples
               (id, project_id, store_path, kind, started_at, finished_at, outcome,
                packed_bytes, packed_entries, loose_refs, ref_locks, namespaces_json, duration_ms,
                packs, kept_packs, pack_bytes, loose_objects, multi_pack_index)
             VALUES (?1, ?2, ?3, 'measure', ?4, ?5, 'ok', ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13,
                     ?14, ?15, ?16)",
            params![
                uuid::Uuid::new_v4().to_string(),
                project.project_id.as_str(),
                store_key(&project.store),
                started,
                finished,
                census.packed_bytes as i64,
                census.packed_entries as i64,
                census.loose_refs as i64,
                census.ref_locks as i64,
                namespaces_json(census),
                (finished - started) * 1000,
                objects.packs as i64,
                objects.kept_packs as i64,
                objects.pack_bytes as i64,
                objects.loose_objects as i64,
                i64::from(objects.multi_pack_index)
            ],
        )
        .await;
    if let Err(error) = result {
        log::warn!("failed to record a ref-store measurement: {error}");
    }
    let cutoff = finished - RETENTION.as_secs() as i64;
    if let Err(error) = db
        .execute(
            "DELETE FROM ref_store_samples WHERE kind = 'measure' AND started_at < ?1",
            params![cutoff],
        )
        .await
    {
        log::warn!("failed to trim old ref-store measurements: {error}");
    }
}

pub(crate) async fn prune_policy(db: &LocalDb, store: &Path) -> PrunePolicy {
    let policy = db
        .query_opt(
            "SELECT prune_policy FROM ref_store_maintenance WHERE store_path = ?1",
            params![store_key(store)],
            |row| row.text(0),
        )
        .await
        .ok()
        .flatten();
    match policy.as_deref() {
        Some("confirmed") => PrunePolicy::Confirmed,
        Some("opted_out") => PrunePolicy::OptedOut,
        _ => PrunePolicy::Default,
    }
}

/// Confirm this machine's prune of `store` (`true`) or opt it out (`false`).
pub(crate) async fn set_prune_confirmed(
    db: &LocalDb,
    project_id: &str,
    store: &Path,
    confirmed: bool,
) -> Result<(), String> {
    let policy = if confirmed { "confirmed" } else { "opted_out" };
    db.execute(
        "INSERT INTO ref_store_maintenance (store_path, project_id, prune_policy, updated_at)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT (store_path) DO UPDATE SET
           project_id = excluded.project_id,
           prune_policy = excluded.prune_policy,
           updated_at = excluded.updated_at",
        params![store_key(store), project_id, policy, unix_now()],
    )
    .await
    .map(|_| ())
    .map_err(|error| format!("record the ref-store prune setting: {error}"))
}

/// When the store's refs were last swept and collected (`truncations_only`
/// false), which a partial prune did too; or when its operation log was last
/// truncated (`true`). A partial prune whose truncation was refused records no
/// `ops_reparented`, so it never counts as a truncation; one that truncated and
/// then failed only its reclaim does.
async fn last_successful_prune(db: &LocalDb, store: &Path, truncations_only: bool) -> Option<i64> {
    let sql = if truncations_only {
        "SELECT MAX(started_at) FROM ref_store_samples
         WHERE store_path = ?1 AND kind = 'prune' AND ops_reparented IS NOT NULL
           AND (outcome = 'ok' OR outcome LIKE 'partial:%')"
    } else {
        "SELECT MAX(started_at) FROM ref_store_samples
         WHERE store_path = ?1 AND kind = 'prune'
           AND (outcome = 'ok' OR outcome LIKE 'partial:%')"
    };
    db.query_opt(sql, params![store_key(store)], |row| row.opt_i64(0))
        .await
        .ok()
        .flatten()
        .flatten()
}

/// When a prune last reclaimed unreferenced objects: the latest prune that ran
/// the reclaim (`reclaimed_bytes` set) and did not fail.
pub(crate) async fn last_successful_reclaim(db: &LocalDb, store: &Path) -> Option<i64> {
    db.query_opt(
        "SELECT MAX(started_at) FROM ref_store_samples
         WHERE store_path = ?1 AND kind = 'prune' AND reclaimed_bytes IS NOT NULL
           AND (outcome = 'ok' OR outcome LIKE 'partial:%')",
        params![store_key(store)],
        |row| row.opt_i64(0),
    )
    .await
    .ok()
    .flatten()
    .flatten()
}

/// Whether the census or the schedule calls for a prune now.
pub(crate) fn prune_is_due(census: &RefStoreCensus, last_ok_prune: Option<i64>, now: i64) -> bool {
    over_threshold(census)
        || last_ok_prune.is_none_or(|last| now - last >= PRUNE_INTERVAL.as_secs() as i64)
}

/// What one prune did, accumulated as it goes so a pass that stops halfway
/// still records what it changed.
#[derive(Debug, Default)]
struct PruneReport {
    before: Option<RefStoreCensus>,
    after: Option<RefStoreCensus>,
    store_bytes_before: Option<u64>,
    store_bytes_after: Option<u64>,
    cairn: CairnRefSweep,
    /// Finished branches whose origin tracking ref the view sweep retired, or
    /// why it could not.
    tracking_refs: Option<Result<crate::jj::RetiredTrackingRefs, String>>,
    view: Option<crate::jj::StoreViewSweep>,
    truncation: Option<OperationLogTruncation>,
    truncation_refusal: Option<String>,
    gc: Option<StoreGarbageCollection>,
    reclaim: Option<ObjectStoreCompaction>,
    /// Why a reclaim that was due did not run; the next prune retries it.
    reclaim_skipped: Option<String>,
    /// git's error from a reclaim that ran and failed; the next prune retries it.
    reclaim_failed: Option<String>,
}

/// Cairn-owned refs the sweep retired, and the ones it deliberately kept.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CairnRefSweep {
    pub(crate) checks: u64,
    pub(crate) stranded: u64,
    pub(crate) rescued: u64,
    /// Stranded refs whose job and issue the database cannot name, kept.
    pub(crate) stranded_unattributed: u64,
    /// Refs left because a leftover `<ref>.lock` blocks them.
    pub(crate) locked: u64,
}

impl CairnRefSweep {
    fn removed(&self) -> u64 {
        self.checks + self.stranded + self.rescued
    }
}

/// Run one prune of `project`'s store and record it.
pub(crate) async fn prune(orch: &Orchestrator, project: &ProjectStore) {
    let started = unix_now();
    let mut report = PruneReport::default();
    // A truncation refused by a live foreign workspace leaves the refs swept
    // but the history unbounded, and a failed reclaim leaves the objects
    // unreclaimed, so either is recorded as partial, never as ok. Partial
    // still counts as swept, so a reclaim that keeps failing (a full repack
    // needs about the store's size in free disk) is retried at the next daily
    // prune rather than re-running the whole prune every hour.
    let outcome = match run_prune(orch, project, &mut report).await {
        Ok(()) => match (&report.truncation_refusal, &report.reclaim_failed) {
            (Some(refusal), _) => format!("partial:{refusal}"),
            (None, Some(error)) => format!("partial:reclaim-failed:{error}"),
            (None, None) => "ok".to_string(),
        },
        Err(outcome) => outcome,
    };
    if outcome != "ok" {
        log::warn!("ref-store prune of {}: {outcome}", project.store.display());
    }
    record_prune(&orch.db.local, project, started, &outcome, &report).await;
}

/// The prune itself. `Err` carries the recorded outcome: `skipped:<reason>`
/// for a precondition that will pass on a later hour, `failed:<error>`
/// otherwise.
async fn run_prune(
    orch: &Orchestrator,
    project: &ProjectStore,
    report: &mut PruneReport,
) -> Result<(), String> {
    let store = project.store.clone();
    let git_dir = crate::jj::store_git_dir(&store)
        .ok_or_else(|| "failed:the store has no git repository behind it".to_string())?;
    if reconcile_in_flight(orch, &store).await {
        return Err("skipped:reconcile-in-flight".into());
    }
    if operator_git_busy(&git_dir) {
        return Err("skipped:operator-git-busy".into());
    }

    let (census, bytes) = blocking({
        let (git_dir, store) = (git_dir.clone(), store.clone());
        move || {
            Ok((
                crate::jj::census_ref_store(&git_dir)?,
                directory_bytes(&store.join(".jj")),
            ))
        }
    })
    .await
    .map_err(failed)?;
    report.before = Some(census);
    report.store_bytes_before = Some(bytes);

    report.cairn = sweep_cairn_refs(orch, &store, &git_dir)
        .await
        .map_err(failed)?;

    // The view sweep only hides; a skipped or failed one leaves residue the
    // next prune collects, so it never holds back the rest of this one.
    match sweep_view(orch, project).await {
        Ok((tracking, sweep)) => {
            match &tracking {
                Ok(retired) if !retired.refs.is_empty() => log::info!(
                    "store view sweep of {}: retired the origin tracking refs of {} finished \
                     branch(es) ({}); undo point: operation {}, then re-create each ref at \
                     the object named",
                    store.display(),
                    retired.refs.len(),
                    retired
                        .refs
                        .iter()
                        .map(|target| format!("{} {}", target.name, target.oid))
                        .collect::<Vec<_>>()
                        .join(", "),
                    retired.operation_before
                ),
                Ok(_) => {}
                Err(error) => log::warn!(
                    "store view sweep of {}: retiring finished branches' tracking refs: {error}",
                    store.display()
                ),
            }
            report.tracking_refs = Some(tracking);
            report.view = Some(sweep.clone());
            if sweep.commits_hidden > 0 || !sweep.bookmarks_resolved.is_empty() {
                log::info!(
                    "store view sweep of {}: hid {} head(s) and {} commit(s), settled {} \
                     replayed-twin bookmark(s) ({}); undo point: operation {}",
                    store.display(),
                    sweep.heads_hidden,
                    sweep.commits_hidden,
                    sweep.bookmarks_resolved.len(),
                    sweep
                        .bookmarks_resolved
                        .iter()
                        .map(|settled| format!("{} -> {}", settled.bookmark, settled.kept))
                        .collect::<Vec<_>>()
                        .join(", "),
                    sweep.operation_before
                );
            }
        }
        Err(outcome) => log::warn!("store view sweep of {}: {outcome}", store.display()),
    }

    let now = unix_now();
    let truncation_due = last_successful_prune(&orch.db.local, &store, true)
        .await
        .is_none_or(|last| now - last >= TRUNCATE_INTERVAL.as_secs() as i64);
    if truncation_due {
        let Ok(_guard) = orch
            .acquire_jj_store_lock_with_timeout(
                &store,
                crate::orchestrator::StoreLockClass::Background,
                "ref-store maintenance: truncate the operation log",
                Some(STORE_LOCK_WAIT),
            )
            .await
        else {
            return Err("skipped:store-busy".into());
        };
        // The lock is what every reconcile takes, so this answer cannot change
        // until the truncation is done.
        if reconcile_in_flight(orch, &store).await {
            return Err("skipped:reconcile-in-flight".into());
        }
        let cutoff = SystemTime::now() - RETENTION;
        let result = blocking({
            let store = store.clone();
            move || crate::jj::truncate_operation_log(&store, cutoff)
        })
        .await;
        match result {
            Ok(truncation) => report.truncation = Some(truncation),
            // A live foreign workspace refuses with nothing changed; the rest
            // of the pass still bounds the refs.
            Err(error) if error.contains("refused") => report.truncation_refusal = Some(error),
            Err(error) => return Err(failed(error)),
        }
    }

    let gc = blocking({
        let store = store.clone();
        move || {
            let now = SystemTime::now();
            crate::jj::collect_store_garbage(&store, now - RETENTION, now - KEEP_REF_GRACE)
        }
    })
    .await
    .map_err(failed)?;
    report.gc = Some(gc);

    // After the keep refs were recreated, so every hidden commit inside the
    // window is reachable when the repack walks; outside every Cairn lock,
    // because it rewrites the base pack and takes as long as a full gc. A
    // reclaim the operator's git is racing, or one that fails, waits for the
    // next prune rather than failing a prune whose ref work already landed.
    if reclaim_is_due(
        last_successful_reclaim(&orch.db.local, &store).await,
        unix_now(),
    ) {
        match object_store_busy(&git_dir, SystemTime::now()) {
            Some(lock) => {
                let reason = format!("operator-git-busy:{lock}");
                log_object_step(&store, "reclaim", &Err(format!("skipped:{reason}")));
                report.reclaim_skipped = Some(reason);
            }
            None => {
                let reclaim = blocking({
                    let git_dir = git_dir.clone();
                    move || {
                        crate::jj::reclaim_unreachable_objects(
                            &git_dir,
                            SystemTime::now() - RETENTION,
                        )
                    }
                })
                .await;
                match reclaim {
                    Ok(step) => {
                        log_object_step(&store, "reclaim", &Ok(step));
                        report.reclaim = Some(step);
                    }
                    Err(error) => {
                        log_object_step(&store, "reclaim", &Err(failed(error.clone())));
                        report.reclaim_failed = Some(error);
                    }
                }
            }
        }
    }

    let (census, bytes) = blocking(move || {
        Ok((
            crate::jj::census_ref_store(&git_dir)?,
            directory_bytes(&store.join(".jj")),
        ))
    })
    .await
    .map_err(failed)?;
    report.after = Some(census);
    report.store_bytes_after = Some(bytes);
    Ok(())
}

/// Retire finished branches' tracking refs, then sweep the store's view, under
/// the store lock, which every reconcile and replay takes, so no rewrite is
/// between naming a commit and anchoring it. A retirement that fails is
/// reported beside the sweep and does not stop it.
async fn sweep_view(
    orch: &Orchestrator,
    project: &ProjectStore,
) -> Result<
    (
        Result<crate::jj::RetiredTrackingRefs, String>,
        crate::jj::StoreViewSweep,
    ),
    String,
> {
    let Ok(_guard) = orch
        .acquire_jj_store_lock_with_timeout(
            &project.store,
            crate::orchestrator::StoreLockClass::Background,
            "ref-store maintenance: sweep the store view",
            Some(STORE_LOCK_WAIT),
        )
        .await
    else {
        return Err("skipped:store-busy".into());
    };
    if reconcile_in_flight(orch, &project.store).await {
        return Err("skipped:reconcile-in-flight".into());
    }
    let cutoff = unix_now() - FINISHED_BRANCH_GRACE.as_secs() as i64;
    let finished = finished_branches(orch, project, cutoff).await;
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let store = project.store.clone();
    let bases = project.base_branches.clone();
    blocking(move || {
        let tracking = finished.and_then(|finished| {
            crate::jj::retire_remote_tracking_refs(&jj, &store, TRACKING_REMOTE, &finished)
        });
        let sweep = crate::jj::sweep_store_view(&jj, &store, &bases, VIEW_SWEEP_GRACE)?;
        Ok((tracking, sweep))
    })
    .await
    .map_err(failed)
}

/// What Cairn's own records say about one agent branch: whether anything has
/// finished it, and whether anything still holds it.
///
/// A branch is finished once an issue it belongs to was resolved (merged or
/// closed) or a pull request from it was merged or closed, provided nothing
/// holds it: no node on it that is not done, no published pull request from it
/// that is still open, and no issue it belongs to that is unresolved. A branch
/// no record names is never finished, whatever the remote says about it.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BranchLifecycle {
    /// The latest moment a record finished the branch.
    pub(crate) finished_at: Option<i64>,
    pub(crate) held: bool,
}

impl BranchLifecycle {
    /// A node on the branch, with the issue it works for.
    pub(crate) fn job(&mut self, status: &str, issue: Option<(&str, i64)>) {
        if !matches!(status, "complete" | "failed" | "cancelled") {
            self.held = true;
        }
        self.issue(issue);
    }

    /// A pull request from the branch, with the issue it belongs to.
    ///
    /// Either Cairn's own status or GitHub's last observed state saying merged
    /// or closed makes it terminal: each lags the other in recorded history. An
    /// open one never published to GitHub is a draft whose lifecycle is its
    /// issue's.
    pub(crate) fn pull_request(
        &mut self,
        status: &str,
        github_state: Option<&str>,
        published: bool,
        settled_at: i64,
        issue: Option<(&str, i64)>,
    ) {
        let terminal = is_resolution(status)
            || github_state.is_some_and(|state| is_resolution(&state.to_ascii_lowercase()));
        if terminal {
            self.finish(settled_at);
        } else if published {
            self.held = true;
        }
        self.issue(issue);
    }

    fn issue(&mut self, issue: Option<(&str, i64)>) {
        match issue {
            Some((status, resolved_at)) if is_resolution(status) => self.finish(resolved_at),
            Some(_) => self.held = true,
            None => {}
        }
    }

    fn finish(&mut self, at: i64) {
        self.finished_at = Some(self.finished_at.map_or(at, |known| known.max(at)));
    }

    /// Finished, and by no record later than `cutoff`.
    pub(crate) fn finished_by(&self, cutoff: i64) -> bool {
        !self.held && self.finished_at.is_some_and(|at| at <= cutoff)
    }
}

fn is_resolution(status: &str) -> bool {
    matches!(status, "merged" | "closed")
}

/// The agent branches of `project`'s store that every database's records say
/// were finished by `cutoff`. An unreadable record is not evidence that nothing
/// holds a branch, so any read failure retires nothing.
async fn finished_branches(
    orch: &Orchestrator,
    project: &ProjectStore,
    cutoff: i64,
) -> Result<Vec<String>, String> {
    let mut serves_store = std::collections::HashMap::<String, bool>::new();
    let mut on_store = |repo_path: &str| {
        *serves_store
            .entry(repo_path.to_string())
            .or_insert_with(|| {
                crate::jj::project_store_dir(&orch.config_dir, Path::new(repo_path))
                    == project.store
            })
    };
    let mut branches = std::collections::BTreeMap::<String, BranchLifecycle>::new();
    for db in orch.db.all_dbs().await {
        let jobs = db
            .query_all(
                "SELECT p.repo_path, j.branch, j.status, i.status,
                        COALESCE(i.merged_at, i.closed_at, i.updated_at)
                 FROM jobs j
                 JOIN projects p ON p.id = j.project_id
                 LEFT JOIN issues i ON i.id = j.issue_id
                 WHERE j.branch LIKE 'agent/%'",
                (),
                |row| {
                    Ok((
                        row.text(0)?,
                        row.text(1)?,
                        row.text(2)?,
                        row.opt_text(3)?,
                        row.opt_i64(4)?,
                    ))
                },
            )
            .await
            .map_err(|error| format!("read the nodes on agent branches: {error}"))?;
        for (repo_path, branch, status, issue_status, resolved_at) in jobs {
            if on_store(&repo_path) {
                let issue = issue_status
                    .as_deref()
                    .map(|s| (s, resolved_at.unwrap_or(0)));
                branches.entry(branch).or_default().job(&status, issue);
            }
        }

        let pull_requests = db
            .query_all(
                "SELECT p.repo_path, m.source_branch, m.status, m.github_state,
                        m.github_pr_number IS NOT NULL,
                        COALESCE(m.merged_at, m.closed_at, m.updated_at),
                        i.status, COALESCE(i.merged_at, i.closed_at, i.updated_at)
                 FROM merge_requests m
                 JOIN projects p ON p.id = m.project_id
                 LEFT JOIN issues i ON i.id = m.issue_id
                 WHERE m.source_branch LIKE 'agent/%'",
                (),
                |row| {
                    Ok((
                        row.text(0)?,
                        row.text(1)?,
                        row.text(2)?,
                        row.opt_text(3)?,
                        row.i64(4)? != 0,
                        row.i64(5)?,
                        row.opt_text(6)?,
                        row.opt_i64(7)?,
                    ))
                },
            )
            .await
            .map_err(|error| format!("read the pull requests from agent branches: {error}"))?;
        for (
            repo_path,
            branch,
            status,
            github_state,
            published,
            settled_at,
            issue_status,
            resolved_at,
        ) in pull_requests
        {
            if on_store(&repo_path) {
                let issue = issue_status
                    .as_deref()
                    .map(|s| (s, resolved_at.unwrap_or(0)));
                branches.entry(branch).or_default().pull_request(
                    &status,
                    github_state.as_deref(),
                    published,
                    settled_at,
                    issue,
                );
            }
        }
    }
    Ok(branches
        .into_iter()
        .filter(|(branch, lifecycle)| {
            lifecycle.finished_by(cutoff) && !project.base_branches.contains(branch)
        })
        .map(|(branch, _)| branch)
        .collect())
}

fn failed(error: String) -> String {
    format!("failed:{error}")
}

async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> Result<T, String> + Send + 'static,
) -> Result<T, String> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|error| format!("maintenance task failed: {error}"))?
}

async fn reconcile_in_flight(orch: &Orchestrator, store: &Path) -> bool {
    let key = store_key(store);
    for db in orch.db.all_dbs().await {
        let pending = db
            .query_opt(
                "SELECT 1 FROM jj_reconcile_intents
                 WHERE store_path = ?1 AND status IN ('pending', 'running') LIMIT 1",
                params![key.as_str()],
                |row| row.i64(0),
            )
            .await;
        if !matches!(pending, Ok(None)) {
            // An unreadable intent table is not evidence that nothing is in
            // flight.
            return true;
        }
    }
    false
}

/// The operator's own git is mid-operation on the checkout. The ref sweep
/// itself contends for `packed-refs.lock`, so here any lock counts.
fn operator_git_busy(git_dir: &Path) -> bool {
    git_dir.join("index.lock").exists() || git_dir.join("packed-refs.lock").exists()
}

/// Bytes under `dir`, counted from file metadata.
fn directory_bytes(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .filter_map(Result::ok)
        .map(|entry| match entry.file_type() {
            Ok(kind) if kind.is_dir() => directory_bytes(&entry.path()),
            Ok(kind) if kind.is_file() => entry.metadata().map(|meta| meta.len()).unwrap_or(0),
            _ => 0,
        })
        .sum()
}

/// Retire the Cairn-owned refs whose lifecycle has ended.
async fn sweep_cairn_refs(
    orch: &Orchestrator,
    store: &Path,
    git_dir: &Path,
) -> Result<CairnRefSweep, String> {
    let (checks, stranded, rescued) = blocking({
        let git_dir = git_dir.to_path_buf();
        move || {
            Ok((
                crate::jj::list_refs(&git_dir, CHECK_PREFIX)?,
                crate::jj::list_refs(&git_dir, STRANDED_PREFIX)?,
                crate::jj::list_refs(&git_dir, RESCUED_PREFIX)?,
            ))
        }
    })
    .await?;
    let now = SystemTime::now();
    let mut sweep = CairnRefSweep::default();

    let expired_checks: Vec<RefTarget> = checks
        .into_iter()
        .filter(|target| {
            !crate::execution::checks::check_ref_is_held(&target.name)
                && ref_older_than(git_dir, &target.name, now - CHECK_REF_GRACE)
        })
        .collect();

    let mut expired_stranded = Vec::new();
    for target in stranded {
        match stranded_ref_resolution(orch, &target.name).await {
            StrandedOwner::Unattributed => sweep.stranded_unattributed += 1,
            StrandedOwner::Open => {}
            StrandedOwner::ResolvedAt(resolved) => {
                if resolved < unix_of(now - RETENTION) {
                    expired_stranded.push(target);
                }
            }
        }
    }

    let rescued_cutoff_ms = unix_of(now - RETENTION) * 1000;
    let expired_rescued: Vec<RefTarget> = rescued
        .into_iter()
        .filter(|target| rescued_ref_millis(&target.name).is_some_and(|ms| ms < rescued_cutoff_ms))
        .collect();

    for (expired, count) in [
        (expired_checks, &mut sweep.checks),
        (expired_stranded, &mut sweep.stranded),
        (expired_rescued, &mut sweep.rescued),
    ] {
        if expired.is_empty() {
            continue;
        }
        let retired = blocking({
            let (store, git_dir) = (store.to_path_buf(), git_dir.to_path_buf());
            move || crate::jj::retire_refs(PackedRefs::JjStore(&store), &git_dir, &expired)
        })
        .await?;
        *count = retired.removed;
        sweep.locked += retired.locked;
    }
    Ok(sweep)
}

/// A packed ref has no age of its own; it was packed by a maintenance pass or
/// an operator long after it was written, so it counts as old.
fn ref_older_than(git_dir: &Path, name: &str, cutoff: SystemTime) -> bool {
    git_dir
        .join(name)
        .metadata()
        .and_then(|meta| meta.modified())
        .map_or(true, |modified| modified < cutoff)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StrandedOwner {
    Unattributed,
    Open,
    ResolvedAt(i64),
}

/// The job a stranded ref is named for, from `refs/cairn/stranded/<job>/<attempt>`.
/// A flat `refs/cairn/stranded/<attempt>` predates job attribution.
pub(crate) fn stranded_ref_job(name: &str) -> Option<&str> {
    let rest = name.strip_prefix(STRANDED_PREFIX)?;
    let (job, attempt) = rest.split_once('/')?;
    (!job.is_empty() && !attempt.is_empty()).then_some(job)
}

async fn stranded_ref_resolution(orch: &Orchestrator, name: &str) -> StrandedOwner {
    let Some(job) = stranded_ref_job(name) else {
        return StrandedOwner::Unattributed;
    };
    for db in orch.db.all_dbs().await {
        let row = db
            .query_opt(
                "SELECT i.status, COALESCE(i.merged_at, i.closed_at)
                 FROM jobs j JOIN issues i ON i.id = j.issue_id
                 WHERE j.id = ?1",
                params![job],
                |row| Ok((row.text(0)?, row.opt_i64(1)?)),
            )
            .await;
        match row {
            Ok(Some((status, Some(resolved)))) if status == "merged" || status == "closed" => {
                return StrandedOwner::ResolvedAt(resolved)
            }
            Ok(Some(_)) => return StrandedOwner::Open,
            _ => {}
        }
    }
    StrandedOwner::Unattributed
}

/// The capture time a rescued ref carries as its last path segment.
pub(crate) fn rescued_ref_millis(name: &str) -> Option<i64> {
    name.strip_prefix(RESCUED_PREFIX)?
        .rsplit('/')
        .next()?
        .parse()
        .ok()
}

async fn record_prune(
    db: &LocalDb,
    project: &ProjectStore,
    started: i64,
    outcome: &str,
    report: &PruneReport,
) {
    let finished = unix_now();
    let census = report.after.as_ref().or(report.before.as_ref());
    let detail = json!({
        "before": report.before.as_ref().map(|census| json!({
            "packed_bytes": census.packed_bytes,
            "packed_entries": census.packed_entries,
            "loose_refs": census.loose_refs,
            "keep_refs": keep_refs(census),
        })),
        "jj_store_bytes_before": report.store_bytes_before,
        "cairn_refs": {
            "checks": report.cairn.checks,
            "stranded": report.cairn.stranded,
            "rescued": report.cairn.rescued,
            "stranded_unattributed": report.cairn.stranded_unattributed,
            "locked": report.cairn.locked,
        },
        "origin_tracking_refs": match &report.tracking_refs {
            // Deleting a ref deletes its reflog, so the refs and objects
            // recorded here are the only way to re-create them.
            Some(Ok(retired)) => json!({
                "retired": retired.refs.len(),
                "locked": retired.locked,
                "operation_before": retired.operation_before,
                "refs": retired.refs.iter()
                    .map(|target| json!({ "ref": target.name, "oid": target.oid }))
                    .collect::<Vec<_>>(),
            }),
            Some(Err(error)) => json!({ "failed": error }),
            None => serde_json::Value::Null,
        },
        "view_sweep": report.view.as_ref().map(|sweep| json!({
            "operation_before": sweep.operation_before,
            "heads_hidden": sweep.heads_hidden,
            "commits_hidden": sweep.commits_hidden,
            "bookmarks_settled": sweep.bookmarks_resolved.len(),
        })),
        "truncation": report.truncation.as_ref().map(|truncation| json!({
            "reparented": truncation.reparented,
            "truncate_ms": truncation.truncate.as_millis() as u64,
            "reindex_ms": truncation.reindex.as_millis() as u64,
        })),
        "truncation_refusal": report.truncation_refusal,
        "gc": report.gc.as_ref().map(|gc| json!({
            "index_heads": gc.index_heads,
            "keep_refs_before": gc.keep_refs_before,
            "keep_refs_created": gc.keep_refs_created,
            "keep_refs_locked": gc.keep_refs_locked,
            "operations_before": gc.operations_before,
            "operations_after": gc.operations_after,
            "op_store_gc_ms": gc.op_store_gc.as_millis() as u64,
            "ref_phase_ms": gc.ref_phase.as_millis() as u64,
        })),
        "objects": match (&report.reclaim, &report.reclaim_skipped, &report.reclaim_failed) {
            (Some(reclaim), _, _) => json!({
                "before": objects_json(&reclaim.before),
                "after": objects_json(&reclaim.after),
                "duration_ms": reclaim.duration.as_millis() as u64,
            }),
            (None, Some(reason), _) => json!({ "skipped": reason }),
            (None, None, Some(error)) => json!({ "failed": error }),
            (None, None, None) => serde_json::Value::Null,
        },
    });
    let objects = report.reclaim.as_ref().map(|reclaim| reclaim.after);
    let result = db
        .execute(
            "INSERT INTO ref_store_samples
               (id, project_id, store_path, kind, started_at, finished_at, outcome,
                packed_bytes, packed_entries, loose_refs, ref_locks, namespaces_json,
                jj_store_bytes, removed_keep_refs, removed_cairn_refs, ops_abandoned,
                ops_reparented, detail_json, duration_ms, packs, kept_packs, pack_bytes,
                loose_objects, multi_pack_index, reclaimed_bytes)
             VALUES (?1, ?2, ?3, 'prune', ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14,
                     ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24)",
            params![
                uuid::Uuid::new_v4().to_string(),
                project.project_id.as_str(),
                store_key(&project.store),
                started,
                finished,
                outcome,
                census.map(|census| census.packed_bytes as i64),
                census.map(|census| census.packed_entries as i64),
                census.map(|census| census.loose_refs as i64),
                census.map(|census| census.ref_locks as i64),
                census.map(namespaces_json),
                report
                    .store_bytes_after
                    .or(report.store_bytes_before)
                    .map(|bytes| bytes as i64),
                report.gc.as_ref().map(|gc| gc.keep_refs_removed as i64),
                Some(report.cairn.removed() as i64),
                report
                    .gc
                    .as_ref()
                    .map(|gc| gc.operations_before.saturating_sub(gc.operations_after) as i64),
                report
                    .truncation
                    .as_ref()
                    .map(|truncation| truncation.reparented as i64),
                detail.to_string(),
                (finished - started) * 1000,
                objects.map(|objects| objects.packs as i64),
                objects.map(|objects| objects.kept_packs as i64),
                objects.map(|objects| objects.pack_bytes as i64),
                objects.map(|objects| objects.loose_objects as i64),
                objects.map(|objects| i64::from(objects.multi_pack_index)),
                report.reclaim.as_ref().map(|reclaim| reclaim
                    .before
                    .pack_bytes
                    .saturating_sub(reclaim.after.pack_bytes)
                    as i64)
            ],
        )
        .await;
    if let Err(error) = result {
        log::warn!("failed to record a ref-store prune: {error}");
    }
}

/// Record one compaction: the census after it (before it, when it did not
/// run) in the object columns, and both in `detail_json`.
async fn record_compaction(
    db: &LocalDb,
    project: &ProjectStore,
    started: i64,
    result: &Result<ObjectStoreCompaction, String>,
    before: &ObjectStoreCensus,
) {
    let finished = unix_now();
    let (outcome, census, detail, duration_ms) = match result {
        Ok(step) => (
            "ok".to_string(),
            step.after,
            json!({
                "mode": "geometric",
                "before": objects_json(&step.before),
                "after": objects_json(&step.after),
            }),
            step.duration.as_millis() as i64,
        ),
        Err(outcome) => (
            outcome.clone(),
            *before,
            json!({ "mode": "geometric", "before": objects_json(before) }),
            (finished - started) * 1000,
        ),
    };
    let key = store_key(&project.store);
    let result = db
        .execute(
            "INSERT INTO ref_store_samples
               (id, project_id, store_path, kind, started_at, finished_at, outcome,
                detail_json, duration_ms, packs, kept_packs, pack_bytes, loose_objects,
                multi_pack_index)
             VALUES (?1, ?2, ?3, 'compact', ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            params![
                uuid::Uuid::new_v4().to_string(),
                project.project_id.as_str(),
                key.as_str(),
                started,
                finished,
                outcome,
                detail.to_string(),
                duration_ms,
                census.packs as i64,
                census.kept_packs as i64,
                census.pack_bytes as i64,
                census.loose_objects as i64,
                i64::from(census.multi_pack_index)
            ],
        )
        .await;
    if let Err(error) = result {
        log::warn!("failed to record an object-store compaction: {error}");
    }
    // A busy store compacts several times a day. Older rows go after the
    // retention window, as measurements do, but the latest always stays so the
    // page can say when a quiet store was last compacted.
    if let Err(error) = db
        .execute(
            "DELETE FROM ref_store_samples
             WHERE kind = 'compact' AND store_path = ?1 AND started_at < ?2
               AND started_at < (SELECT MAX(started_at) FROM ref_store_samples
                                 WHERE kind = 'compact' AND store_path = ?1)",
            params![key.as_str(), finished - RETENTION.as_secs() as i64],
        )
        .await
    {
        log::warn!("failed to trim old object-store compactions: {error}");
    }
}

/// What the project page shows about a store.
#[derive(Debug, Clone, Default)]
pub(crate) struct RefStoreOverview {
    pub(crate) policy: PrunePolicy,
    pub(crate) latest: Option<RefStoreSample>,
    pub(crate) day_ago: Option<RefStoreSample>,
    pub(crate) week_ago: Option<RefStoreSample>,
    pub(crate) last_prune: Option<PruneSample>,
    /// The last prune that swept refs, fully or partially.
    pub(crate) last_ok_prune_at: Option<i64>,
    /// The last prune that also truncated the operation log.
    pub(crate) last_truncation_at: Option<i64>,
    /// The latest census of the object database, from whichever row last took
    /// one. Its row records no cruft packs, so that count reads zero.
    pub(crate) objects: Option<ObjectStoreCensus>,
    pub(crate) last_compaction: Option<ObjectStepSample>,
    /// The last prune that reclaimed unreferenced objects.
    pub(crate) last_reclaim: Option<ObjectStepSample>,
}

/// One compaction or reclaim as recorded.
#[derive(Debug, Clone, Default)]
pub(crate) struct ObjectStepSample {
    pub(crate) started_at: i64,
    pub(crate) outcome: String,
    pub(crate) duration_ms: i64,
    pub(crate) before: Option<ObjectStoreCensus>,
    pub(crate) after: Option<ObjectStoreCensus>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct RefStoreSample {
    pub(crate) at: i64,
    pub(crate) packed_bytes: u64,
    pub(crate) packed_entries: u64,
    pub(crate) loose_refs: u64,
    pub(crate) ref_locks: u64,
    /// `(namespace, packed, loose)`, in the census's namespace order.
    pub(crate) namespaces: Vec<(String, u64, u64)>,
}

impl RefStoreSample {
    pub(crate) fn total_refs(&self) -> u64 {
        self.packed_entries + self.loose_refs
    }

    pub(crate) fn namespace(&self, name: &str) -> (u64, u64) {
        self.namespaces
            .iter()
            .find(|(namespace, _, _)| namespace == name)
            .map(|(_, packed, loose)| (*packed, *loose))
            .unwrap_or((0, 0))
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct PruneSample {
    pub(crate) started_at: i64,
    pub(crate) outcome: String,
    pub(crate) duration_ms: i64,
    pub(crate) removed_keep_refs: Option<i64>,
    pub(crate) removed_cairn_refs: Option<i64>,
    pub(crate) ops_abandoned: Option<i64>,
    pub(crate) truncated: bool,
    /// Why the reclaim this prune was due to run did not.
    pub(crate) reclaim_skipped: Option<String>,
    /// git's error from this prune's reclaim.
    pub(crate) reclaim_failed: Option<String>,
}

const SAMPLE_COLUMNS: &str =
    "finished_at, packed_bytes, packed_entries, loose_refs, ref_locks, namespaces_json";

fn sample_from_row(row: &cairn_db::turso::Row) -> crate::storage::DbResult<RefStoreSample> {
    let namespaces = row
        .opt_text(5)?
        .and_then(|text| serde_json::from_str::<serde_json::Value>(&text).ok())
        .and_then(|value| value.as_object().cloned())
        .map(|map| {
            map.into_iter()
                .map(|(namespace, refs)| {
                    let count = |key| {
                        refs.get(key)
                            .and_then(serde_json::Value::as_u64)
                            .unwrap_or(0)
                    };
                    (namespace, count("packed"), count("loose"))
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(RefStoreSample {
        at: row.i64(0)?,
        packed_bytes: row.i64(1)? as u64,
        packed_entries: row.i64(2)? as u64,
        loose_refs: row.i64(3)? as u64,
        ref_locks: row.opt_i64(4)?.unwrap_or(0) as u64,
        namespaces,
    })
}

async fn sample_at_or_before(db: &LocalDb, project_id: &str, at: i64) -> Option<RefStoreSample> {
    db.query_opt(
        format!(
            "SELECT {SAMPLE_COLUMNS} FROM ref_store_samples
             WHERE project_id = ?1 AND packed_entries IS NOT NULL AND finished_at <= ?2
             ORDER BY finished_at DESC, rowid DESC LIMIT 1"
        ),
        params![project_id, at],
        sample_from_row,
    )
    .await
    .ok()
    .flatten()
}

/// Everything the project page renders about a project's ref store on this
/// machine.
pub(crate) async fn ref_store_overview(
    orch: &Orchestrator,
    project_id: &str,
    repo_path: &Path,
) -> RefStoreOverview {
    let db = &orch.db.local;
    let store = crate::jj::project_store_dir(&orch.config_dir, repo_path);
    let now = unix_now();
    let last_prune = db
        .query_opt(
            "SELECT started_at, outcome, duration_ms, removed_keep_refs, removed_cairn_refs,
                    ops_abandoned, ops_reparented, detail_json
             FROM ref_store_samples
             WHERE project_id = ?1 AND kind = 'prune'
             ORDER BY started_at DESC, rowid DESC LIMIT 1",
            params![project_id],
            |row| {
                let detail = detail(row.opt_text(7)?);
                let objects = |key: &str| {
                    detail
                        .as_ref()?
                        .pointer(&format!("/objects/{key}"))
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string)
                };
                let reclaim_skipped = objects("skipped");
                let reclaim_failed = objects("failed");
                Ok(PruneSample {
                    started_at: row.i64(0)?,
                    outcome: row.text(1)?,
                    duration_ms: row.i64(2)?,
                    removed_keep_refs: row.opt_i64(3)?,
                    removed_cairn_refs: row.opt_i64(4)?,
                    ops_abandoned: row.opt_i64(5)?,
                    truncated: row.opt_i64(6)?.is_some(),
                    reclaim_skipped,
                    reclaim_failed,
                })
            },
        )
        .await
        .ok()
        .flatten();
    let objects = db
        .query_opt(
            "SELECT packs, kept_packs, pack_bytes, loose_objects, multi_pack_index
             FROM ref_store_samples
             WHERE project_id = ?1 AND packs IS NOT NULL
             ORDER BY finished_at DESC, rowid DESC LIMIT 1",
            params![project_id],
            |row| {
                Ok(ObjectStoreCensus {
                    packs: row.i64(0)? as u64,
                    kept_packs: row.opt_i64(1)?.unwrap_or(0) as u64,
                    cruft_packs: 0,
                    pack_bytes: row.opt_i64(2)?.unwrap_or(0) as u64,
                    loose_objects: row.opt_i64(3)?.unwrap_or(0) as u64,
                    multi_pack_index: row.opt_i64(4)?.unwrap_or(0) != 0,
                })
            },
        )
        .await
        .ok()
        .flatten();
    let last_compaction = object_step(
        db,
        "SELECT started_at, outcome, duration_ms, detail_json FROM ref_store_samples
         WHERE project_id = ?1 AND kind = 'compact'
         ORDER BY started_at DESC, rowid DESC LIMIT 1",
        project_id,
        "",
    )
    .await;
    let last_reclaim = object_step(
        db,
        "SELECT started_at, outcome, duration_ms, detail_json FROM ref_store_samples
         WHERE project_id = ?1 AND kind = 'prune' AND reclaimed_bytes IS NOT NULL
           AND (outcome = 'ok' OR outcome LIKE 'partial:%')
         ORDER BY started_at DESC, rowid DESC LIMIT 1",
        project_id,
        "/objects",
    )
    .await;
    RefStoreOverview {
        policy: prune_policy(db, &store).await,
        latest: sample_at_or_before(db, project_id, now).await,
        day_ago: sample_at_or_before(db, project_id, now - DAY as i64).await,
        week_ago: sample_at_or_before(db, project_id, now - 7 * DAY as i64).await,
        last_prune,
        last_ok_prune_at: last_successful_prune(db, &store, false).await,
        last_truncation_at: last_successful_prune(db, &store, true).await,
        objects,
        last_compaction,
        last_reclaim,
    }
}

fn detail(text: Option<String>) -> Option<serde_json::Value> {
    serde_json::from_str(&text?).ok()
}

/// One compaction or reclaim row, its before and after census read from
/// `detail_json` at `pointer`.
async fn object_step(
    db: &LocalDb,
    sql: &str,
    project_id: &str,
    pointer: &'static str,
) -> Option<ObjectStepSample> {
    db.query_opt(sql, params![project_id], move |row| {
        let detail = detail(row.opt_text(3)?);
        let census = |key: &str| {
            detail
                .as_ref()?
                .pointer(&format!("{pointer}/{key}"))
                .and_then(objects_from_json)
        };
        Ok(ObjectStepSample {
            started_at: row.i64(0)?,
            outcome: row.text(1)?,
            duration_ms: row.i64(2)?,
            before: census("before"),
            after: census("after"),
        })
    })
    .await
    .ok()
    .flatten()
}

fn unix_of(time: SystemTime) -> i64 {
    time.duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs() as i64)
        .unwrap_or(0)
}

fn unix_now() -> i64 {
    unix_of(SystemTime::now())
}

#[cfg(test)]
mod tests;
