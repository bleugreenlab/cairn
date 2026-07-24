//! Database types for Cairn Core.
//!
//! Contains the runtime database state wrapper and migration status types.
//! Database initialization and path resolution remain in host crates since
//! they depend on platform-specific app data directories.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;

use cairn_common::ids::{self, RoutableId, RouteScope};

use crate::account::team_token_minter::TeamTokenMinter;
use crate::services::EventEmitter;
use crate::storage::{
    run_pull_task, run_push_task, DbError, DbResult, LocalDb, MigrationRunner, RouteReconcile,
    RowExt, SearchIndex, SyncCadence, TeamSyncScope, TEAM_MIGRATIONS,
};
use crate::storage::{ContentStoreFactory, TeamReplicaContext};

// `TeamId` is defined in cairn-db's storage layer and re-exported through the
// core storage facade; re-export it here so `crate::db::TeamId` is unchanged.
pub use crate::storage::TeamId;

/// Normalized project key -> routing target. `None` = the private database;
/// `Some(team)` routes to that team's replica. Shared (`Arc`) so the per-team
/// pull task's route reconciler can update it without holding all of `DbState`.
type RouteMap = HashMap<String, Option<TeamId>>;

/// Sync configuration for one team-owned database, as held in the private
/// `teams` registry (migration 0082). Used to open or re-open the team's synced
/// replica.
#[derive(Debug, Clone)]
pub struct TeamConfig {
    pub team_id: TeamId,
    /// The team's human-readable name. Used to seed the synced `teams` root row
    /// (the NOT NULL FK parent every team `projects.team_id` references). Sourced
    /// from the account's org membership (`org_name`) when connecting, and read
    /// back from the private `teams` registry's `name` column at startup.
    pub team_name: String,
    pub sync_url: String,
    /// `None` for an unauthenticated local sync server (`tursodb --sync-server`).
    pub auth_token: Option<String>,
    pub replica_path: PathBuf,
}

/// Runtime dependencies for the per-team background sync loop, injected by the
/// host once `Services` (hence the event emitter) exists — later than
/// `load_routing_catalog`, so `enable_team_sync` retro-spawns tasks for teams
/// already opened at startup.
#[derive(Clone)]
pub struct SyncRuntime {
    pub emitter: Arc<dyn EventEmitter>,
    pub cadence: SyncCadence,
}

/// The push and pull `JoinHandle`s for one team's sync loop. Dropping it aborts
/// both tasks, so removing a team from `sync_tasks` (`close_team`) or dropping
/// `DbState` stops the loop cleanly with no leaked tasks. `push()`/`pull()` are
/// cancel-safe, so an aborted in-flight op simply doesn't complete.
struct TeamSyncHandle {
    push: JoinHandle<()>,
    pull: JoinHandle<()>,
}

impl Drop for TeamSyncHandle {
    fn drop(&mut self) {
        self.push.abort();
        self.pull.abort();
    }
}

/// Runtime database state and the project -> database router.
///
/// Cairn stores each project WHOLLY in exactly one database: the private
/// `local` database (every local project, plus workspace/global state) or a
/// team-owned synced replica. `for_project` resolves which one a project's reads
/// and writes target, defaulting to `local` on any cache miss so existing
/// local-only installs behave exactly as before.
pub struct DbState {
    /// The private database. Always present.
    pub local: Arc<LocalDb>,
    /// Opened team replicas, keyed by team id.
    teams: RwLock<HashMap<TeamId, Arc<LocalDb>>>,
    /// Normalized project key -> routing target, cached from `project_routes`.
    /// `Arc` so the per-team pull task's route reconciler shares this cache.
    routes: Arc<RwLock<RouteMap>>,
    pub search_index: Arc<SearchIndex>,
    /// The per-team sync runtime, set once by the host via `enable_team_sync`.
    /// `None` until enabled (and forever, for a host that never enables it).
    sync_runtime: RwLock<Option<SyncRuntime>>,
    /// Live push+pull task handles, keyed by team id. Dropping a handle aborts
    /// its tasks, so this map IS the loop's lifecycle.
    sync_tasks: RwLock<HashMap<TeamId, TeamSyncHandle>>,
    /// Whether device authentication is currently available. Team registrations
    /// and replicas outlive an account session, but their network loops must not:
    /// a missing device JWT is a stable signed-out state, not a retryable outage.
    team_sync_authorized: AtomicBool,
    /// Serializes the flag change with the corresponding task-map mutation so a
    /// reconnect cannot be lost behind a delayed sign-out clear.
    team_sync_lifecycle: Mutex<()>,
    /// Single-flight gate serializing team opens. `open_team` keeps a lock-free
    /// `teams.read` fast path and only acquires this on a miss, re-checking the
    /// map under it, so two concurrent opens of the same team converge on one
    /// replica handle (and one migration run, one sync-task set) instead of
    /// racing two `open_synced` calls onto the same file.
    open_gate: Mutex<()>,
    /// The host-installed rotating sync-token minter, set once via
    /// `set_team_token_minter` BEFORE any production team opens. When present,
    /// `open_team` builds the replica with a per-request token callback; when
    /// absent (focused tests and local-only hosts that intentionally skip
    /// installation), it uses the static/unauthenticated `TeamConfig::auth_token`
    /// path unchanged.
    team_token_minter: RwLock<Option<Arc<dyn TeamTokenMinter>>>,
    /// The host-installed content-store factory, set once via
    /// `set_content_store_factory` alongside the token minter. When present,
    /// `open_team` attaches a per-team [`ContentStore`] to the replica so its
    /// archival offloads/fetches reach the shared store; when absent (focused
    /// tests and local-only hosts that intentionally skip installation) a team
    /// replica carries no store and behaves exactly as before.
    ///
    /// [`ContentStore`]: crate::storage::ContentStore
    content_store_factory: RwLock<Option<Arc<dyn ContentStoreFactory>>>,
}

impl DbState {
    pub fn new(local: Arc<LocalDb>, search_index: Arc<SearchIndex>) -> Self {
        Self {
            local,
            teams: RwLock::new(HashMap::new()),
            routes: Arc::new(RwLock::new(HashMap::new())),
            search_index,
            sync_runtime: RwLock::new(None),
            sync_tasks: RwLock::new(HashMap::new()),
            team_sync_authorized: AtomicBool::new(true),
            team_sync_lifecycle: Mutex::new(()),
            open_gate: Mutex::new(()),
            team_token_minter: RwLock::new(None),
            content_store_factory: RwLock::new(None),
        }
    }

    /// Install the rotating sync-token minter. The host calls this once at
    /// startup, BEFORE `load_routing_catalog`, so even the startup reopen of
    /// already-registered teams gets token rotation. Leaving it unset preserves
    /// the static/unauthenticated open path byte-for-byte.
    pub async fn set_team_token_minter(&self, minter: Arc<dyn TeamTokenMinter>) {
        *self.team_token_minter.write().await = Some(minter);
    }

    /// Install the per-team content-store factory. The host calls this once at
    /// startup, alongside `set_team_token_minter` and BEFORE any team opens, so
    /// every replica (startup or runtime-created) gets a content store. Leaving
    /// it unset preserves the storeless team-replica path.
    pub async fn set_content_store_factory(&self, factory: Arc<dyn ContentStoreFactory>) {
        *self.content_store_factory.write().await = Some(factory);
    }

    /// Returns whether the host installed the rotating sync-token minter.
    pub async fn has_team_token_minter(&self) -> bool {
        self.team_token_minter.read().await.is_some()
    }

    /// Returns whether the host installed the per-team content-store factory.
    pub async fn has_content_store_factory(&self) -> bool {
        self.content_store_factory.read().await.is_some()
    }

    /// Normalizes a project key the same way `lookup_project_by_key` does (upper
    /// case), so `cairn` and `CAIRN` route identically.
    fn normalize_key(key: &str) -> String {
        key.to_uppercase()
    }

    /// Resolve the owning open team for a project id. Object-plane grants fail
    /// closed when the project is local or its team replica is not connected.
    pub async fn team_id_for_project(&self, project_id: &str) -> Result<Option<TeamId>, String> {
        let teams = self.teams.read().await;
        for (team_id, db) in teams.iter() {
            let exists = db
                .query_opt(
                    "SELECT id FROM projects WHERE id = ?1 LIMIT 1",
                    (project_id.to_owned(),),
                    |row| row.text(0),
                )
                .await
                .map_err(|error| error.to_string())?
                .is_some();
            if exists {
                return Ok(Some(team_id.clone()));
            }
        }
        Ok(None)
    }

    /// Resolves the database a project's reads and writes should target.
    ///
    /// Defaults to the private database on any cache miss or not-yet-opened team,
    /// so a project with no route (every local project) is a strict no-op versus
    /// the previous single-database behavior.
    pub async fn for_project(&self, project_key: &str) -> Arc<LocalDb> {
        let key = Self::normalize_key(project_key);
        let team_id = {
            let routes = self.routes.read().await;
            match routes.get(&key) {
                Some(Some(team_id)) => team_id.clone(),
                _ => return self.local.clone(),
            }
        };
        self.teams
            .read()
            .await
            .get(&team_id)
            .cloned()
            .unwrap_or_else(|| self.local.clone())
    }

    /// Records a project's routing target. `team` is `None` for a local project.
    /// Called by project create and by startup route loading.
    pub async fn set_route(&self, project_key: &str, team: Option<TeamId>) {
        let key = Self::normalize_key(project_key);
        self.routes.write().await.insert(key, team);
    }

    /// The routing scope a project's NEW entities should be minted into
    /// (CAIRN-2210). A project with no route (every local project) is `Local`,
    /// so a local install mints bare ids exactly as before; a team project mints
    /// `{team_id}~{uuid}` ids that self-route to the team replica.
    pub async fn route_scope_for_project(&self, project_key: &str) -> RouteScope {
        let key = Self::normalize_key(project_key);
        match self.routes.read().await.get(&key) {
            Some(Some(team_id)) => RouteScope::Team(team_id.clone()),
            _ => RouteScope::Local,
        }
    }

    /// Mint a self-routing id for a NEW root entity owned by `project_key` (an
    /// issue or a top-level execution). Child entities should instead inherit
    /// their parent's scope via [`cairn_common::ids::mint_inheriting`] so the
    /// prefix propagates down the object graph with no route lookup.
    pub async fn mint_for_project(&self, project_key: &str) -> RoutableId {
        ids::mint(self.route_scope_for_project(project_key).await)
    }

    /// The already-open synced replica for a team, if registered and opened.
    /// Returns `None` when no team with this id is open (not in the private
    /// `teams` registry, or its replica failed to open at startup), so callers
    /// that require a team DB surface a clear error rather than silently falling
    /// back to the private database.
    pub async fn team_db(&self, team_id: &str) -> Option<Arc<LocalDb>> {
        self.teams.read().await.get(team_id).cloned()
    }

    #[cfg(test)]
    pub(crate) async fn register_team_db_for_test(&self, team_id: TeamId, db: Arc<LocalDb>) {
        self.teams.write().await.insert(team_id, db);
    }

    /// Opens (or returns the already-open) synced replica for a team, running
    /// `TEAM_MIGRATIONS` once when the replica is newly created.
    ///
    /// A bootstrapping replica receives schema + data from the sync server on
    /// open; only the first creator (whose replica is still empty after the
    /// bootstrap pull) migrates locally and pushes. Re-running on a replica that
    /// already carries the schema is a no-op.
    pub async fn open_team(&self, cfg: TeamConfig) -> DbResult<Arc<LocalDb>> {
        // Lock-free fast path: an already-open team returns immediately.
        if let Some(existing) = self.teams.read().await.get(&cfg.team_id) {
            return Ok(existing.clone());
        }
        // Miss: serialize opens behind the gate, then RE-CHECK under it — a
        // concurrent open may have finished while we waited, in which case we
        // return its handle rather than opening a second one on the same file.
        let _gate = self.open_gate.lock().await;
        if let Some(existing) = self.teams.read().await.get(&cfg.team_id) {
            return Ok(existing.clone());
        }
        // Select the auth path: a rotating per-request token when a minter is
        // installed, else the static/unauthenticated `auth_token`.
        let minter = self.team_token_minter.read().await.clone();
        let mut db = match minter {
            Some(minter) => {
                let team_id = cfg.team_id.clone();
                LocalDb::open_synced_with_token_fn(
                    &cfg.replica_path,
                    cfg.sync_url.clone(),
                    move || {
                        let minter = minter.clone();
                        let team_id = team_id.clone();
                        // Turso invokes this before every sync HTTP request; the
                        // minter's cache caps how often it actually hits the api.
                        async move {
                            minter
                                .mint(&team_id)
                                .await
                                .map_err(cairn_db::turso::Error::Error)
                        }
                    },
                )
                .await?
            }
            None => {
                LocalDb::open_synced(
                    &cfg.replica_path,
                    cfg.sync_url.clone(),
                    cfg.auth_token.clone(),
                )
                .await?
            }
        };
        // bootstrap_if_empty pulls the server's schema/data (including the synced
        // cairn_schema_migrations ledger) on open; converge once more first.
        let _ = db.pull().await;
        // Always run the idempotent runner. A freshly bootstrapped replica carries
        // every applied version in its synced ledger, so nothing applies; a newly
        // created replica establishes the schema; an existing replica missing a
        // newer SHARED_TAIL migration applies just that one. Push only when
        // something actually applied. (Gating on "schema present" instead would
        // strand every future shared migration on replicas this method created.)
        let applied = MigrationRunner::new(TEAM_MIGRATIONS.to_vec())
            .run(&db)
            .await?;
        // Repair legacy replicas whose `projects` table predates the
        // `is_workspace` column (an older binary applied a pre-column team head).
        // Fresh replicas already have it, so this is idempotent by swallowing the
        // benign duplicate-column error — turso has no ADD COLUMN IF NOT EXISTS and
        // the migration runner has no per-statement tolerance, so it cannot be a
        // tracked ALTER. Without it, the team-workspace row seed can't write.
        ensure_projects_is_workspace_column(&db).await;
        // Repair legacy replicas whose `action_configs` table predates the
        // `workspace_id` column (an older team head dropped the whole workspace
        // arm). The shared action-config query layer names `workspace_id`
        // unconditionally, so without the column every action CRUD against the
        // replica fails `no column named workspace_id`. Same runtime-repair shape
        // and rationale as `is_workspace` above.
        ensure_action_configs_workspace_id_column(&db).await;
        // Repair legacy replicas whose team schema predates the CAIRN-2629
        // `device_presence` table (device-runner presence for team execution
        // ownership). Naturally idempotent via CREATE TABLE IF NOT EXISTS, and a
        // backstop for the migration-vs-sync race even though the tracked team
        // migration also uses IF NOT EXISTS.
        ensure_device_presence_table(&db).await;
        ensure_executor_registry_table(&db).await;
        // Seed the team's OWN root row into the synced `teams` table — the NOT
        // NULL FK parent that every team `projects.team_id` references after the
        // CAIRN-2129 re-rooting. Nothing else ever seeds it, so without this the
        // FIRST project created into a freshly provisioned team fails with
        // `FOREIGN KEY constraint failed` (CAIRN-2180). A TRACKED write, so the
        // push below propagates it to the shared DB and teammates pull it. Runs
        // after migrations so the `teams` table exists, and is idempotent
        // (ON CONFLICT(id) DO NOTHING) across repeated opens and across members.
        let seeded = seed_team_root(&db, &cfg.team_id, &cfg.team_name).await?;
        if !applied.is_empty() || seeded {
            db.push().await?;
        }
        // Attach this team's content store so archival offload (and the matching
        // reconstruct fetch) target the shared per-team store. The handle now
        // carries its own team scope; detection downstream is on the handle, not
        // a separate lookup. Storeless when no factory is installed (tests,
        // headless), which keeps the inline path.
        if let Some(factory) = self.content_store_factory.read().await.clone() {
            db.set_team_context(TeamReplicaContext {
                team_id: cfg.team_id.clone(),
                store: factory.store_for(&cfg.team_id),
                private_db: Some(self.local.clone()),
            });
            if let Err(error) =
                crate::storage::pack_catalog::backfill_execution_pack_catalog(&db, 32).await
            {
                log::warn!("incremental pack catalog backfill failed: {error}");
            }
        }
        let db = Arc::new(db);
        self.teams
            .write()
            .await
            .insert(cfg.team_id.clone(), db.clone());
        // Once the host has enabled the loop, a newly opened replica (startup or
        // runtime-created) gets its push+pull tasks immediately; a team opened
        // before enablement is picked up by `enable_team_sync`.
        if self.team_sync_authorized.load(Ordering::Acquire) {
            if let Some(runtime) = self.sync_runtime.read().await.clone() {
                self.spawn_team_sync(&cfg.team_id, db.clone(), &runtime)
                    .await;
            }
        }
        // Reconcile routes from the just-opened replica so a teammate's projects
        // (which carry no `project_routes` row on THIS host — only their creator
        // writes one) become routable. Best-effort: a failure leaves those
        // projects without a route, which 404s (fail-closed) rather than
        // mis-routing, and the pull-task reconciler retries on the next pull.
        if let Err(error) =
            reconcile_team_routes(&self.local, &db, &self.routes, &cfg.team_id).await
        {
            log::warn!(
                "initial route reconcile for team `{}` failed: {error}",
                cfg.team_id
            );
        }
        Ok(db)
    }

    /// Spawn this team's push and pull tasks under `runtime`, unless they are
    /// already running. Holds only the `sync_tasks` lock.
    async fn spawn_team_sync(&self, team_id: &TeamId, db: Arc<LocalDb>, runtime: &SyncRuntime) {
        let mut tasks = self.sync_tasks.write().await;
        // Recheck under the task-map lock so a concurrent sign-out cannot clear
        // the map and then lose a racing spawn.
        if !self.team_sync_authorized.load(Ordering::Acquire) || tasks.contains_key(team_id) {
            return;
        }
        // A reconciler holding only the private DB, the team replica, the shared
        // route cache, and the team id — NOT `DbState` itself, so the spawned
        // pull task never forms an Arc cycle back through `sync_tasks`.
        let reconciler: Arc<dyn RouteReconcile> = Arc::new(TeamRouteReconciler {
            local: self.local.clone(),
            team_db: db.clone(),
            routes: self.routes.clone(),
            team_id: team_id.clone(),
        });
        let push = tokio::spawn(run_push_task(db.clone(), runtime.cadence.clone()));
        let pull = tokio::spawn(run_pull_task(
            db,
            runtime.emitter.clone(),
            runtime.cadence.clone(),
            Some(reconciler),
        ));
        tasks.insert(team_id.clone(), TeamSyncHandle { push, pull });
    }

    /// Enable the per-team background sync loop. Stores `runtime` so teams opened
    /// later spawn their tasks via `open_team`, and spawns tasks for every
    /// already-open team that lacks them (teams opened at startup, before the
    /// host could build the runtime). With no team open this is fully inert — the
    /// dormancy guarantee for local-only installs.
    pub async fn enable_team_sync(&self, runtime: SyncRuntime) {
        *self.sync_runtime.write().await = Some(runtime.clone());
        if !self.team_sync_authorized.load(Ordering::Acquire) {
            return;
        }
        self.spawn_all_team_sync(&runtime).await;
    }

    async fn spawn_all_team_sync(&self, runtime: &SyncRuntime) {
        let open: Vec<(TeamId, Arc<LocalDb>)> = self
            .teams
            .read()
            .await
            .iter()
            .map(|(id, db)| (id.clone(), db.clone()))
            .collect();
        for (team_id, db) in open {
            self.spawn_team_sync(&team_id, db, runtime).await;
        }
    }

    /// Match the team sync task lifecycle to device-auth availability. Signing
    /// out aborts every network loop but deliberately keeps replicas and routes
    /// open for local access. Signing back in reuses the retained runtime and
    /// starts exactly one push/pull pair per open team.
    pub(crate) async fn set_team_sync_authorized(&self, authorized: bool) {
        let _lifecycle = self.team_sync_lifecycle.lock().await;
        let was_authorized = self.team_sync_authorized.swap(authorized, Ordering::AcqRel);
        if authorized {
            if !was_authorized {
                log::info!("team sync resumed after device authentication became available");
            }
            if let Some(runtime) = self.sync_runtime.read().await.clone() {
                self.spawn_all_team_sync(&runtime).await;
            }
        } else {
            self.sync_tasks.write().await.clear();
            if was_authorized {
                log::info!("team sync paused because no device authentication is available");
            }
        }
    }

    /// Stop a team's sync loop and forget its replica. Dropping the handle aborts
    /// the push+pull tasks; integrity holds because an aborted op marks nothing
    /// done and unpushed frames simply retry on a future open.
    async fn close_team(&self, team_id: &str) {
        self.sync_tasks.write().await.remove(team_id);
        self.teams.write().await.remove(team_id);
    }

    /// Insert or update a team's row in the private `teams` registry — the
    /// durable source of truth for “which teams should be open” that
    /// `load_routing_catalog` reopens at startup. The stored `auth_token` is
    /// always NULL: production rotation flows through the token minter callback,
    /// not a stored static token. `name` is the team's human-readable name
    /// (resolved from the account's org membership by `connect_team`); it is read
    /// back into `TeamConfig::team_name` at startup to seed the replica's root row.
    pub async fn upsert_team_registry(
        &self,
        team_id: &str,
        name: &str,
        sync_url: &str,
        replica_path: &str,
    ) -> DbResult<()> {
        // The team id is the routing prefix every team-owned entity carries, so a
        // malformed id would mint un-parseable ids. Reject loudly at the one
        // registration chokepoint rather than discovering it at parse time
        // (CAIRN-2210). The opaque better-auth org PK satisfies the guard; the
        // renameable slug must never be registered here.
        if !ids::is_valid_team_id(team_id) {
            return Err(DbError::internal(format!(
                "refusing to register team id {team_id:?}: a routing prefix must be \
                 non-empty, at most 64 chars, and [A-Za-z0-9] only"
            )));
        }
        let now = chrono::Utc::now().timestamp();
        self.local
            .execute(
                "INSERT INTO teams(id, name, sync_url, auth_token, replica_path, created_at)
                 VALUES (?1, ?2, ?3, NULL, ?4, ?5)
                 ON CONFLICT(id) DO UPDATE SET
                     name = excluded.name,
                     sync_url = excluded.sync_url,
                     replica_path = excluded.replica_path",
                (
                    team_id.to_string(),
                    name.to_string(),
                    sync_url.to_string(),
                    replica_path.to_string(),
                    now,
                ),
            )
            .await?;
        Ok(())
    }

    /// Team ids currently in the private `teams` registry — the durable
    /// "which teams to reopen at startup" set. The account-teams reconcile
    /// diffs this against the account's current org memberships to find teams
    /// the user no longer belongs to and forget them.
    pub(crate) async fn registered_team_ids(&self) -> DbResult<Vec<String>> {
        self.local
            .query_all("SELECT id FROM teams", (), |row| row.text(0))
            .await
    }

    /// Resolve the human-readable name for a registered team. The private
    /// registry is the local canonical join between opaque routing ids and team
    /// display names; presentation layers should not expose the routing id when
    /// this value is available.
    pub async fn registered_team_name(&self, team_id: &str) -> DbResult<Option<String>> {
        self.local
            .query_opt(
                "SELECT name FROM teams WHERE id = ?1",
                (team_id.to_string(),),
                |row| row.text(0),
            )
            .await
    }

    /// Fully forget a team the account no longer belongs to. Stops its sync loop
    /// and drops its open replica (`close_team`), removes its in-memory route
    /// cache entries, and deletes its durable `project_routes` and `teams`
    /// registry rows — so it neither serves reads now (project listing iterates
    /// only open replicas) nor reopens at the next `load_routing_catalog`. The
    /// on-disk replica file is left in place (inert once closed and
    /// deregistered); a rejoin re-bootstraps it. Idempotent: a team never
    /// registered or opened is a no-op. Returns whether a registry row existed.
    pub(crate) async fn forget_team(&self, team_id: &str) -> DbResult<bool> {
        // In-memory teardown: abort the sync tasks and drop the replica handle.
        self.close_team(team_id).await;
        // Drop route-cache entries that pointed at this team so a stale read
        // can't resolve to the now-closed replica before the durable delete.
        self.routes
            .write()
            .await
            .retain(|_key, target| target.as_deref() != Some(team_id));
        // Durable removal: routes first, then the registry row.
        self.local
            .execute(
                "DELETE FROM project_routes WHERE team_id = ?1",
                (team_id.to_string(),),
            )
            .await?;
        let affected = self
            .local
            .execute("DELETE FROM teams WHERE id = ?1", (team_id.to_string(),))
            .await?;
        Ok(affected > 0)
    }

    /// Test accessor: number of currently-open team replicas.
    #[cfg(feature = "test-utils")]
    pub async fn open_team_count(&self) -> usize {
        self.teams.read().await.len()
    }

    /// Test accessor: number of teams with a live push+pull sync-task pair.
    #[cfg(feature = "test-utils")]
    pub async fn sync_task_count(&self) -> usize {
        self.sync_tasks.read().await.len()
    }

    /// Test-only: register an already-open database as a team replica.
    ///
    /// `open_team` is the production path, but it requires a live sync server,
    /// which command- and handler-layer tests cannot stand up. Injecting a
    /// second database here lets those tests exercise the owning-DB resolution
    /// the desktop create path depends on (CAIRN-2184): registered here under its
    /// team id, the injected DB is exactly what `owning_db` / `owning_db_for_issue`
    /// resolve a `{team}~…` id to through the prefix-parse router. CAIRN-2181's own
    /// routing test never exercised a real team replica, which is exactly how the
    /// desktop gap shipped.
    #[cfg(feature = "test-utils")]
    pub async fn insert_team_db_for_test(&self, team_id: &str, db: Arc<LocalDb>) {
        self.teams.write().await.insert(team_id.to_string(), db);
    }

    /// The ids of every currently-open team replica. Drives per-team startup
    /// passes (e.g. team-workspace materialization) that must touch each open
    /// team exactly once.
    pub async fn open_team_ids(&self) -> Vec<String> {
        self.teams.read().await.keys().cloned().collect()
    }

    /// Every open database: the private one plus each opened team replica. The
    /// search-index drain iterates this so locally-originated writes in any
    /// database get indexed.
    pub async fn all_dbs(&self) -> Vec<Arc<LocalDb>> {
        let mut dbs = vec![self.local.clone()];
        dbs.extend(self.teams.read().await.values().cloned());
        dbs
    }

    /// Drains the pending search outbox of EVERY open database into the single
    /// URI-keyed index. Documents are project-encoded in their URI, so one index
    /// serves all databases without key collision. The bar this slice meets is
    /// that team outboxes DRAIN (locally-originated writes index) rather than
    /// accumulate; indexing pull-arrived rows on a receiving replica, whose
    /// triggers never fired, is a deferred follow-up.
    pub(crate) async fn apply_pending_search(&self) -> DbResult<usize> {
        let mut total = 0;
        for db in self.all_dbs().await {
            total += self.search_index.apply_pending(&db).await?;
        }
        Ok(total)
    }

    /// Brings the search index current at startup: a full rebuild from the
    /// private database when the index is new or incompatible, then a drain of
    /// every open team replica's outbox.
    pub async fn refresh_search_index(&self) -> DbResult<usize> {
        let dbs = self.all_dbs().await;
        if self.search_index.needs_rebuild() {
            // Rebuild clears the index, so it must reload source rows — including
            // already-applied ones — from EVERY open database, or a rebuild drops
            // team content until it next changes.
            self.search_index.rebuild_many(&dbs).await
        } else {
            self.apply_pending_search().await
        }
    }

    /// Opens every team replica registered in the private `teams` catalog and
    /// loads `project_routes` into the in-memory route cache. A failure to open
    /// one team is logged and skipped so an unreachable team does not block
    /// startup. With no teams configured this is a strict no-op, preserving
    /// existing local-only behavior.
    pub async fn load_routing_catalog(&self) -> DbResult<()> {
        let teams: Vec<TeamConfig> = self
            .local
            .query_all(
                "SELECT id, name, sync_url, auth_token, replica_path FROM teams",
                (),
                |row| {
                    Ok(TeamConfig {
                        team_id: row.text(0)?,
                        team_name: row.text(1)?,
                        sync_url: row.text(2)?,
                        auth_token: row.opt_text(3)?,
                        replica_path: PathBuf::from(row.text(4)?),
                    })
                },
            )
            .await?;
        for cfg in teams {
            let team_id = cfg.team_id.clone();
            if let Err(error) = self.open_team(cfg).await {
                log::warn!("failed to open team `{team_id}` replica at startup: {error}");
            }
        }

        let routes: Vec<(String, Option<TeamId>)> = self
            .local
            .query_all(
                "SELECT project_key, team_id FROM project_routes",
                (),
                |row| Ok((row.text(0)?, row.opt_text(1)?)),
            )
            .await?;
        for (key, team) in routes {
            self.set_route(&key, team).await;
        }
        Ok(())
    }
}

/// Idempotently insert a team replica's own root row into its synced `teams`
/// table — the NOT NULL FK parent that every team `projects.team_id` references
/// (CAIRN-2129 re-rooting). Returns whether a row was actually inserted, so
/// `open_team` pushes only when the seed is new. `ON CONFLICT(id) DO NOTHING`
/// makes it a no-op on a replica that already carries the row — a teammate's,
/// arrived via bootstrap, or one this host seeded on a prior open — so first
/// writer wins and every member converges on the same id+name from the same org.
async fn seed_team_root(db: &LocalDb, team_id: &str, name: &str) -> DbResult<bool> {
    let now = chrono::Utc::now().timestamp();
    let affected = db
        .execute(
            "INSERT INTO teams(id, name, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?3)
             ON CONFLICT(id) DO NOTHING",
            (team_id.to_string(), name.to_string(), now),
        )
        .await?;
    Ok(affected > 0)
}

/// Scan an opened team replica's projects and persist a `project_routes` row
/// (and update the in-memory cache) for each, mapping its key to `team_id`.
///
/// Idempotent (`INSERT OR IGNORE`): a project this host created already has its
/// authoritative route from `create_routed`, so reconcile only fills the gaps
/// left by teammate-created projects that arrived via sync. Used both by the
/// initial `open_team` and by the per-team pull-task reconciler, so a project
/// that appears AFTER open becomes routable on the next pull without a restart.
/// Idempotently ensure a team replica's `projects` table carries the
/// `is_workspace` column. See the call site in [`DbState::open_team`] for why
/// this is a runtime repair rather than a tracked migration. Best-effort: a
/// failure other than the benign duplicate-column signature is logged, and the
/// workspace-row seed downstream then simply logs and skips.
async fn ensure_projects_is_workspace_column(team_db: &LocalDb) {
    if let Err(error) = team_db
        .execute_batch("ALTER TABLE projects ADD COLUMN is_workspace INTEGER NOT NULL DEFAULT 0")
        .await
    {
        if !error
            .to_string()
            .to_lowercase()
            .contains("duplicate column")
        {
            log::warn!("ensuring projects.is_workspace on team replica failed: {error}");
        }
    }
}

/// Idempotently ensure a team replica's `action_configs` table carries the
/// nullable `workspace_id` column. See the call site in [`DbState::open_team`]
/// for why this is a runtime repair rather than a tracked migration. Team rows
/// are always project-anchored (workspace_id NULL), but the one shared
/// action-config query layer names the column in every statement, so a legacy
/// replica missing it cannot serve any action CRUD. Best-effort: a failure
/// other than the benign duplicate-column signature is logged.
async fn ensure_action_configs_workspace_id_column(team_db: &LocalDb) {
    if let Err(error) = team_db
        .execute_batch("ALTER TABLE action_configs ADD COLUMN workspace_id TEXT")
        .await
    {
        if !error
            .to_string()
            .to_lowercase()
            .contains("duplicate column")
        {
            log::warn!("ensuring action_configs.workspace_id on team replica failed: {error}");
        }
    }
}

/// Idempotently ensure a team replica carries the `device_presence` table
/// (CAIRN-2629). See the call site in [`DbState::open_team`]. Best-effort:
/// `CREATE TABLE IF NOT EXISTS` is naturally idempotent, so any error here is a
/// genuine failure worth logging rather than a benign already-exists.
async fn ensure_device_presence_table(team_db: &LocalDb) {
    if let Err(error) = team_db
        .execute_batch(
            "CREATE TABLE IF NOT EXISTS device_presence (\
                device_id TEXT PRIMARY KEY NOT NULL, \
                device_name TEXT NOT NULL, \
                last_seen INTEGER NOT NULL, \
                project_keys TEXT NOT NULL DEFAULT '[]', \
                updated_at INTEGER NOT NULL)",
        )
        .await
    {
        log::warn!("ensuring device_presence on team replica failed: {error}");
    }
}

/// Backstop the tracked team migration against migration/sync ordering races.
async fn ensure_executor_registry_table(team_db: &LocalDb) {
    if let Err(error) = team_db
        .execute_batch(
            "CREATE TABLE IF NOT EXISTS executor_registry (\
            device_id TEXT NOT NULL, executor_id TEXT NOT NULL, display_name TEXT NOT NULL, \
            os TEXT NOT NULL, arch TEXT NOT NULL, logical_cores INTEGER NOT NULL, \
            toolchains TEXT NOT NULL DEFAULT '[]', projects_served TEXT NOT NULL DEFAULT '[]', \
            current_load INTEGER NOT NULL, \
            warm_commits TEXT NOT NULL DEFAULT '[]', connection_generation INTEGER NOT NULL, \
            status TEXT NOT NULL, last_seen INTEGER NOT NULL, expires_at INTEGER NOT NULL, \
            updated_at INTEGER NOT NULL, PRIMARY KEY (device_id, executor_id))",
        )
        .await
    {
        log::warn!("ensuring executor_registry on team replica failed: {error}");
    }
}

async fn reconcile_team_routes(
    local: &LocalDb,
    team_db: &LocalDb,
    routes: &RwLock<RouteMap>,
    team_id: &TeamId,
) -> DbResult<Vec<String>> {
    // Auto-provision the team's workspace project row, first-writer-wins. The
    // team workspace COMES WITH the team (a twin of the personal ~/.cairn), so
    // an existing team gains it on next open/pull and a new team has it from the
    // start; every member's reconcile converges on the same is_workspace row.
    // Path-less here: the machine-local repo and its `project_routes` clone path
    // are materialized by the services-aware provisioning step, so an
    // un-materialized member simply contributes no config layer (graceful).
    let ws_id =
        cairn_common::ids::mint(cairn_common::ids::RouteScope::Team(team_id.clone())).into_string();
    if let Err(error) = crate::projects::crud::seed_team_workspace_project_db(
        team_db,
        chrono::Utc::now().timestamp(),
        team_id,
        &ws_id,
        &crate::projects::crud::team_workspace_key(team_id),
        "",
    )
    .await
    {
        log::warn!("seeding team workspace project for `{team_id}` failed: {error}");
    }

    let projects = team_db
        .read(|conn| {
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
        .await?;
    // Serialize reconciliation across team replicas with the same lock that owns
    // the live route cache. A project key can move between replicas; holding this
    // guard through the durable updates keeps the cache and catalog in one order.
    let mut routes = routes.write().await;
    let project_ids = projects
        .iter()
        .map(|(project_id, _, _)| project_id.clone())
        .collect::<Vec<_>>();
    let project_keys = projects
        .iter()
        .map(|(_, project_key, _)| project_key.to_uppercase())
        .collect::<std::collections::HashSet<_>>();
    let stale_keys = local
        .read(|conn| {
            let team_id = team_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT project_key FROM project_routes WHERE team_id = ?1",
                        [team_id],
                    )
                    .await?;
                let mut keys = Vec::new();
                while let Some(row) = rows.next().await? {
                    keys.push(row.text(0)?);
                }
                Ok(keys)
            })
        })
        .await?
        .into_iter()
        .filter(|key| !project_keys.contains(key))
        .collect::<Vec<_>>();
    for key in &stale_keys {
        // Keep the machine-local clone path while clearing ownership. If another
        // replica now owns this key, its upsert below reassigns the same row; the
        // result is order-independent whether the old or new team reconciles first.
        let cleared = local
            .execute(
                "UPDATE project_routes SET team_id = NULL
                 WHERE project_key = ?1 AND team_id = ?2",
                (key.clone(), team_id.clone()),
            )
            .await?;
        if cleared > 0 {
            routes.insert(key.clone(), None);
        }
    }

    let now = chrono::Utc::now().timestamp();
    for (_, project_key, repo_path) in projects {
        let key = project_key.to_uppercase();
        local
            .execute(
                "INSERT INTO project_routes(project_key, team_id, created_at)
                 VALUES (?1, ?2, ?3)
                 ON CONFLICT(project_key) DO UPDATE SET team_id = excluded.team_id",
                (key.clone(), team_id.clone(), now),
            )
            .await?;
        if !repo_path.is_empty() && path_is_git_repo(Path::new(&repo_path)) {
            local
                .execute(
                    "UPDATE project_routes
                     SET local_repo_path = ?1
                     WHERE project_key = ?2 AND local_repo_path IS NULL",
                    (repo_path.clone(), key.clone()),
                )
                .await?;
        }
        routes.insert(key, Some(team_id.clone()));
    }
    Ok(project_ids)
}

fn path_is_git_repo(path: &Path) -> bool {
    path.join(".git").exists() || path.join("objects").is_dir()
}

/// The [`RouteReconcile`] the per-team pull task invokes after applying remote
/// frames. Holds only the pieces reconcile needs — never `DbState` — so the
/// spawned task forms no Arc cycle back through `sync_tasks`.
struct TeamRouteReconciler {
    local: Arc<LocalDb>,
    team_db: Arc<LocalDb>,
    routes: Arc<RwLock<RouteMap>>,
    team_id: TeamId,
}

#[async_trait::async_trait]
impl RouteReconcile for TeamRouteReconciler {
    async fn reconcile(&self) -> Result<TeamSyncScope, String> {
        let project_ids =
            reconcile_team_routes(&self.local, &self.team_db, &self.routes, &self.team_id)
                .await
                .map_err(|error| {
                    format!(
                        "pull-triggered route reconcile for team `{}` failed: {error}",
                        self.team_id
                    )
                })?;
        Ok(TeamSyncScope {
            team_id: self.team_id.clone(),
            project_ids,
        })
    }
}

// ============================================================================
// Migration Status Types (for frontend communication)
// ============================================================================

/// Status check result for migration UI
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MigrationStatus {
    needed: bool,
    pending_migrations: Vec<String>,
    current_db_path: String,
    error_message: Option<String>,
}

/// Schema change detected during migration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaChange {
    table: String,
    change_type: String,
    old_name: Option<String>,
    new_name: Option<String>,
    auto_mapped: bool,
}

/// Per-table result for frontend display
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableMigrationResult {
    name: String,
    old_count: usize,
    new_count: usize,
    status: String,
    error: Option<String>,
}

/// Final migration result for frontend display
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MigrationResult {
    success: bool,
    tables: Vec<TableMigrationResult>,
    schema_changes: Vec<SchemaChange>,
    total_rows_restored: usize,
    total_rows_attempted: usize,
    warnings: Vec<String>,
}

const STANDALONE_IMPORT_MESSAGE: &str =
    "Legacy SQLite import is a standalone migration utility, not an app runtime path";

/// Check whether the interactive runtime data-migration flow has work to do.
///
/// Schema migrations are owned by the database initializer and run before the
/// host exposes an invoke surface. The old first-launch data-migration UI is now
/// a compatibility surface: runtime databases are already initialized directly,
/// and legacy SQLite import is no longer an in-app operation.
pub fn check_data_migration_status(db_path: &Path) -> MigrationStatus {
    MigrationStatus {
        needed: false,
        pending_migrations: Vec::new(),
        current_db_path: db_path.to_string_lossy().to_string(),
        error_message: None,
    }
}

/// Start the interactive data-migration flow.
///
/// The former in-app legacy SQLite import path has been retired to standalone
/// tooling, so the user-facing confirmation semantics remain explicit: callers
/// that ask to start a migration receive the same rejection the desktop command
/// returned before the runner cutover.
pub fn start_data_migration(_db_path: &Path) -> Result<(MigrationResult, PathBuf), String> {
    Err(STANDALONE_IMPORT_MESSAGE.to_string())
}

/// Confirm the interactive data-migration flow.
///
/// With no staged in-app data migration, confirm is a no-op retained for desktop
/// compatibility.
pub fn confirm_data_migration(_db_path: &Path) -> Result<(), String> {
    Ok(())
}

/// Cancel the interactive data-migration flow by removing the legacy staged
/// database path if one exists.
pub fn cancel_data_migration(db_path: &Path) -> Result<(), String> {
    let temp_path = db_path.with_extension("db.new");
    if temp_path.exists() {
        std::fs::remove_file(&temp_path)
            .map_err(|e| format!("Failed to remove {}: {e}", temp_path.display()))?;
    }
    Ok(())
}

/// Results from startup recovery.
///
/// Contains outbox entries to replay.
pub struct StartupRecovery {
    /// Pending outbox entries that need to be replayed after Orchestrator is built.
    pub outbox_entries: Vec<crate::effects::outbox::OutboxEntry>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::SearchIndex;

    async fn db_state(name: &str) -> DbState {
        let local = Arc::new(crate::storage::migrated_test_db(name).await);
        let index =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        DbState::new(local, index)
    }

    async fn migrated_team_db(name: &str) -> LocalDb {
        let temp = tempfile::tempdir().unwrap();
        let db = LocalDb::open(temp.keep().join(name)).await.unwrap();
        MigrationRunner::new(TEAM_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db
    }

    /// An unrouted (local) project mints bare ids, so local installs are
    /// byte-for-byte unchanged by the self-routing id format.
    #[tokio::test(flavor = "current_thread")]
    async fn unrouted_project_mints_local_bare_ids() {
        let dbs = db_state("db-bridge-local.db").await;
        assert_eq!(
            dbs.route_scope_for_project("CAIRN").await,
            RouteScope::Local
        );
        let id = dbs.mint_for_project("CAIRN").await;
        assert!(
            !id.as_str().contains('~'),
            "a local project must mint a bare id"
        );
        assert_eq!(id.route_scope(), Ok(RouteScope::Local));
    }

    /// A team-routed project mints `{team_id}~{uuid}` ids that self-route back to
    /// the team. Key normalization (`cairn` == `CAIRN`) is preserved.
    #[tokio::test(flavor = "current_thread")]
    async fn team_routed_project_mints_prefixed_ids() {
        let dbs = db_state("db-bridge-team.db").await;
        dbs.set_route("CAIRN", Some("teamABC123".to_string())).await;
        assert_eq!(
            dbs.route_scope_for_project("cairn").await,
            RouteScope::Team("teamABC123".to_string())
        );
        let id = dbs.mint_for_project("CAIRN").await;
        assert_eq!(
            id.route_scope(),
            Ok(RouteScope::Team("teamABC123".to_string()))
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn reconcile_team_routes_backfills_local_repo_path_when_creator_repo_exists() {
        let dbs = db_state("db-bridge-route-backfill.db").await;
        let team_db = migrated_team_db("db-bridge-route-backfill-team.db").await;
        let repo = tempfile::tempdir().unwrap();
        std::process::Command::new("git")
            .args(["init", "-q"])
            .current_dir(repo.path())
            .status()
            .unwrap();
        let repo_path = repo.path().to_string_lossy().to_string();

        team_db
            .execute_script(&format!(
                "
                INSERT INTO teams(id, name, created_at, updated_at) VALUES ('teamABC123', 'Team', 1, 1);
                INSERT INTO projects(id, team_id, name, key, repo_path, created_at, updated_at)
                 VALUES ('teamABC123~00000000-0000-4000-8000-000000000001', 'teamABC123', 'Project', 'Legacy', '{}', 1, 1);
                ",
                repo_path.replace('\'', "''")
            ))
            .await
            .unwrap();

        dbs.local
            .execute(
                "INSERT INTO teams(id, name, sync_url, replica_path, created_at) VALUES ('teamABC123', 'Team', 'http://sync', '/tmp/team.db', 1)",
                (),
            )
            .await
            .unwrap();

        let project_ids =
            reconcile_team_routes(&dbs.local, &team_db, &dbs.routes, &"teamABC123".to_string())
                .await
                .unwrap();
        assert!(
            project_ids.contains(&"teamABC123~00000000-0000-4000-8000-000000000001".to_string())
        );

        let stored = dbs
            .local
            .query_opt_text(
                "SELECT local_repo_path FROM project_routes WHERE project_key = 'LEGACY'",
                (),
            )
            .await
            .unwrap();
        assert_eq!(stored.as_deref(), Some(repo_path.as_str()));
        assert_eq!(
            dbs.route_scope_for_project("legacy").await,
            RouteScope::Team("teamABC123".to_string())
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn reconcile_team_routes_returns_current_projects_and_removes_stale_routes() {
        let dbs = db_state("db-bridge-route-scope.db").await;
        let team_db = migrated_team_db("db-bridge-route-scope-team.db").await;
        let team_id = "teamABC123".to_string();

        team_db
            .execute_script(
                "
                INSERT INTO teams(id, name, created_at, updated_at) VALUES ('teamABC123', 'Team', 1, 1);
                INSERT INTO projects(id, team_id, name, key, repo_path, created_at, updated_at)
                 VALUES ('teamABC123~00000000-0000-4000-8000-000000000011', 'teamABC123', 'One', 'ONE', '', 1, 1);
                INSERT INTO projects(id, team_id, name, key, repo_path, created_at, updated_at)
                 VALUES ('teamABC123~00000000-0000-4000-8000-000000000012', 'teamABC123', 'Two', 'TWO', '', 1, 1);
                ",
            )
            .await
            .unwrap();
        dbs.local
            .execute_script(
                "
                INSERT INTO teams(id, name, sync_url, replica_path, created_at)
                 VALUES ('teamABC123', 'Team', 'http://sync', '/tmp/team.db', 1);
                INSERT INTO project_routes(project_key, team_id, created_at)
                 VALUES ('STALE', 'teamABC123', 1);
                ",
            )
            .await
            .unwrap();
        dbs.routes
            .write()
            .await
            .insert("STALE".to_string(), Some(team_id.clone()));

        let project_ids = reconcile_team_routes(&dbs.local, &team_db, &dbs.routes, &team_id)
            .await
            .unwrap();

        assert!(
            project_ids.contains(&"teamABC123~00000000-0000-4000-8000-000000000011".to_string())
        );
        assert!(
            project_ids.contains(&"teamABC123~00000000-0000-4000-8000-000000000012".to_string())
        );
        assert_eq!(
            dbs.route_scope_for_project("ONE").await,
            RouteScope::Team(team_id.clone())
        );
        assert_eq!(
            dbs.route_scope_for_project("TWO").await,
            RouteScope::Team(team_id)
        );
        assert_eq!(
            dbs.route_scope_for_project("STALE").await,
            RouteScope::Local
        );
        assert_eq!(
            dbs.local
                .query_opt_text(
                    "SELECT team_id FROM project_routes WHERE project_key = 'STALE'",
                    (),
                )
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn reconcile_team_routes_reassigns_between_teams_in_either_order() {
        for old_team_first in [true, false] {
            let dbs = db_state(if old_team_first {
                "db-bridge-route-move-old-first.db"
            } else {
                "db-bridge-route-move-new-first.db"
            })
            .await;
            let old_db = migrated_team_db("db-bridge-route-move-old.db").await;
            let new_db = migrated_team_db("db-bridge-route-move-new.db").await;
            let old_team = "teamOLD123".to_string();
            let new_team = "teamNEW123".to_string();
            let local_path = "/tmp/local-foo";

            new_db
                .execute_script(
                    "INSERT INTO teams(id, name, created_at, updated_at) VALUES ('teamNEW123', 'New', 1, 1);
                     INSERT INTO projects(id, team_id, name, key, repo_path, created_at, updated_at)
                     VALUES ('teamNEW123~00000000-0000-4000-8000-000000000021', 'teamNEW123', 'Foo', 'FOO', '', 1, 1);",
                )
                .await
                .unwrap();
            dbs.local
                .execute_script(
                    "INSERT INTO teams(id, name, sync_url, replica_path, created_at)
                     VALUES ('teamOLD123', 'Old', 'http://old', '/tmp/old.db', 1);
                     INSERT INTO teams(id, name, sync_url, replica_path, created_at)
                     VALUES ('teamNEW123', 'New', 'http://new', '/tmp/new.db', 1);",
                )
                .await
                .unwrap();
            dbs.local
                .execute(
                    "INSERT INTO project_routes(project_key, team_id, local_repo_path, created_at)
                     VALUES ('FOO', 'teamOLD123', ?1, 1)",
                    [local_path],
                )
                .await
                .unwrap();
            dbs.routes
                .write()
                .await
                .insert("FOO".to_string(), Some(old_team.clone()));

            let reconcile_old =
                || reconcile_team_routes(&dbs.local, &old_db, &dbs.routes, &old_team);
            let reconcile_new =
                || reconcile_team_routes(&dbs.local, &new_db, &dbs.routes, &new_team);
            if old_team_first {
                reconcile_old().await.unwrap();
                reconcile_new().await.unwrap();
            } else {
                reconcile_new().await.unwrap();
                reconcile_old().await.unwrap();
            }

            assert_eq!(
                dbs.route_scope_for_project("FOO").await,
                RouteScope::Team(new_team.clone())
            );
            let stored = dbs
                .local
                .read(|conn| {
                    Box::pin(async move {
                        let mut rows = conn
                            .query(
                                "SELECT team_id, local_repo_path FROM project_routes WHERE project_key = 'FOO'",
                                (),
                            )
                            .await?;
                        let row = rows.next().await?.expect("FOO route");
                        Ok((row.text(0)?, row.text(1)?))
                    })
                })
                .await
                .unwrap();
            assert_eq!(stored, (new_team.clone(), local_path.to_string()));
        }
    }

    /// A team replica whose `action_configs` table predates `workspace_id` (an
    /// older team head that dropped the whole workspace arm) gets the column
    /// added so the shared action-config query layer works, and repeating the
    /// repair is a benign no-op.
    #[tokio::test(flavor = "current_thread")]
    async fn ensure_action_configs_workspace_id_column_repairs_and_is_idempotent() {
        let temp = tempfile::tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("legacy-actions-team.db"))
            .await
            .unwrap();
        // A legacy `action_configs` table WITHOUT workspace_id (older team head).
        db.execute_batch(
            "CREATE TABLE action_configs (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, project_id TEXT NOT NULL)",
        )
        .await
        .unwrap();

        async fn has_workspace_id(db: &LocalDb) -> i64 {
            db.read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT COUNT(*) FROM pragma_table_info('action_configs') WHERE name = 'workspace_id'",
                            (),
                        )
                        .await?;
                    rows.next().await?.unwrap().i64(0)
                })
            })
            .await
            .unwrap()
        }

        assert_eq!(has_workspace_id(&db).await, 0);
        ensure_action_configs_workspace_id_column(&db).await;
        assert_eq!(has_workspace_id(&db).await, 1, "column added");
        // Idempotent: repeating swallows the duplicate-column error.
        ensure_action_configs_workspace_id_column(&db).await;
        assert_eq!(has_workspace_id(&db).await, 1);
    }

    /// A team replica whose `projects` table predates `is_workspace` gets the
    /// column added, and repeating the repair is a benign no-op (the
    /// duplicate-column error is swallowed).
    #[tokio::test(flavor = "current_thread")]
    async fn ensure_projects_is_workspace_column_repairs_and_is_idempotent() {
        let temp = tempfile::tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("legacy-team.db"))
            .await
            .unwrap();
        // A legacy `projects` table WITHOUT the column (older team head).
        db.execute_batch(
            "CREATE TABLE projects (id TEXT PRIMARY KEY, team_id TEXT NOT NULL, key TEXT)",
        )
        .await
        .unwrap();

        async fn has_is_workspace(db: &LocalDb) -> i64 {
            db.read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT COUNT(*) FROM pragma_table_info('projects') WHERE name = 'is_workspace'",
                            (),
                        )
                        .await?;
                    rows.next().await?.unwrap().i64(0)
                })
            })
            .await
            .unwrap()
        }

        assert_eq!(has_is_workspace(&db).await, 0);
        ensure_projects_is_workspace_column(&db).await;
        assert_eq!(has_is_workspace(&db).await, 1, "column added");
        // Idempotent: repeating swallows the duplicate-column error.
        ensure_projects_is_workspace_column(&db).await;
        assert_eq!(has_is_workspace(&db).await, 1);
    }

    /// A team replica missing the CAIRN-2629 `device_presence` table gets it
    /// created, and repeating the repair converges on exactly one table (the
    /// `CREATE TABLE IF NOT EXISTS` is naturally idempotent).
    #[tokio::test(flavor = "current_thread")]
    async fn ensure_device_presence_table_repairs_and_is_idempotent() {
        let temp = tempfile::tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("legacy-presence-team.db"))
            .await
            .unwrap();

        async fn presence_table_count(db: &LocalDb) -> i64 {
            db.read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='device_presence'",
                            (),
                        )
                        .await?;
                    rows.next().await?.unwrap().i64(0)
                })
            })
            .await
            .unwrap()
        }

        assert_eq!(presence_table_count(&db).await, 0);
        ensure_device_presence_table(&db).await;
        assert_eq!(presence_table_count(&db).await, 1, "table created");
        // Idempotent: repeating is a no-op via IF NOT EXISTS.
        ensure_device_presence_table(&db).await;
        assert_eq!(presence_table_count(&db).await, 1);
    }

    /// Forgetting a team removes its registry row, its project routes, and its
    /// in-memory route-cache entries, so it neither serves reads nor reopens.
    #[tokio::test(flavor = "current_thread")]
    async fn forget_team_removes_registry_routes_and_cache() {
        let dbs = db_state("db-bridge-forget.db").await;
        dbs.upsert_team_registry("teamABC123", "Team", "http://sync", "/tmp/t.db")
            .await
            .unwrap();
        dbs.set_route("PROJ", Some("teamABC123".to_string())).await;
        dbs.local
            .execute(
                "INSERT INTO project_routes(project_key, team_id, created_at) VALUES ('PROJ', 'teamABC123', 0)",
                (),
            )
            .await
            .unwrap();

        assert!(dbs.forget_team("teamABC123").await.unwrap());

        // Registry row gone — won't reopen at startup.
        assert!(dbs.registered_team_ids().await.unwrap().is_empty());
        // Durable route row gone.
        let routes = dbs
            .local
            .query_all(
                "SELECT project_key FROM project_routes WHERE team_id = 'teamABC123'",
                (),
                |row| row.text(0),
            )
            .await
            .unwrap();
        assert!(routes.is_empty());
        // Route cache no longer resolves the project to the team.
        assert_eq!(dbs.route_scope_for_project("PROJ").await, RouteScope::Local);
    }

    /// Forgetting a team that was never registered is a no-op returning `false`.
    #[tokio::test(flavor = "current_thread")]
    async fn forget_team_unregistered_is_noop() {
        let dbs = db_state("db-bridge-forget-noop.db").await;
        assert!(!dbs.forget_team("teamABC123").await.unwrap());
    }

    /// The team-id guard rejects a malformed routing prefix at the registration
    /// chokepoint rather than letting it mint un-parseable ids.
    #[tokio::test(flavor = "current_thread")]
    async fn team_registry_rejects_malformed_team_id() {
        let dbs = db_state("db-bridge-guard.db").await;
        assert!(dbs
            .upsert_team_registry("teamABC123", "Team", "http://x", "/p")
            .await
            .is_ok());
        assert!(dbs
            .upsert_team_registry("bad/slug", "Team", "http://x", "/p")
            .await
            .is_err());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn team_registry_resolves_display_name() {
        let dbs = db_state("db-bridge-team-name.db").await;
        dbs.upsert_team_registry("teamABC123", "Acme", "http://sync", "/tmp/t.db")
            .await
            .unwrap();

        assert_eq!(
            dbs.registered_team_name("teamABC123").await.unwrap(),
            Some("Acme".to_string())
        );
        assert_eq!(dbs.registered_team_name("missing").await.unwrap(), None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn device_auth_transition_pauses_and_rearms_open_team_sync_tasks() {
        let dbs = db_state("db-bridge-sync-auth-lifecycle.db").await;
        let team_id = "teamABC123".to_string();
        dbs.teams.write().await.insert(
            team_id.clone(),
            Arc::new(migrated_team_db("db-bridge-sync-auth-team.db").await),
        );
        dbs.enable_team_sync(SyncRuntime {
            emitter: Arc::new(crate::services::testing::CapturingEmitter::new()),
            cadence: SyncCadence::default(),
        })
        .await;

        assert!(dbs.sync_tasks.read().await.contains_key(&team_id));
        dbs.set_team_sync_authorized(false).await;
        assert!(dbs.sync_tasks.read().await.is_empty());

        // Repeating the stable signed-out state stays inert. A genuine auth
        // transition is what re-arms the retained runtime.
        dbs.set_team_sync_authorized(false).await;
        assert!(dbs.sync_tasks.read().await.is_empty());
        dbs.set_team_sync_authorized(true).await;
        assert!(dbs.sync_tasks.read().await.contains_key(&team_id));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn reconnect_waiting_behind_sign_out_cannot_lose_its_rearm() {
        let dbs = Arc::new(db_state("db-bridge-sync-auth-race.db").await);
        let team_id = "teamABC123".to_string();
        dbs.teams.write().await.insert(
            team_id.clone(),
            Arc::new(migrated_team_db("db-bridge-sync-auth-race-team.db").await),
        );
        dbs.enable_team_sync(SyncRuntime {
            emitter: Arc::new(crate::services::testing::CapturingEmitter::new()),
            cadence: SyncCadence::default(),
        })
        .await;

        // Hold the lifecycle gate while both transitions queue. Tokio's mutex is
        // FIFO, so sign-out runs first and reconnect must re-arm after its clear.
        let gate = dbs.team_sync_lifecycle.lock().await;
        let sign_out = {
            let dbs = dbs.clone();
            tokio::spawn(async move { dbs.set_team_sync_authorized(false).await })
        };
        tokio::task::yield_now().await;
        let reconnect = {
            let dbs = dbs.clone();
            tokio::spawn(async move { dbs.set_team_sync_authorized(true).await })
        };
        drop(gate);
        sign_out.await.unwrap();
        reconnect.await.unwrap();

        assert!(dbs.team_sync_authorized.load(Ordering::Acquire));
        assert!(dbs.sync_tasks.read().await.contains_key(&team_id));
    }
}
