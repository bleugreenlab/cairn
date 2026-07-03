//! Database types for Cairn Core.
//!
//! Contains the runtime database state wrapper and migration status types.
//! Database initialization and path resolution remain in host crates since
//! they depend on platform-specific app data directories.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;

use cairn_common::ids::{self, RoutableId, RouteScope};

use crate::account::team_token_minter::TeamTokenMinter;
use crate::archival::store::{ContentStoreFactory, TeamReplicaContext};
use crate::services::EventEmitter;
use crate::storage::{
    run_pull_task, run_push_task, DbError, DbResult, LocalDb, MigrationRunner, RouteReconcile,
    RowExt, SearchIndex, SyncCadence, TEAM_MIGRATIONS,
};

pub type TeamId = String;

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
    /// [`ContentStore`]: crate::archival::store::ContentStore
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
    pub async fn register_team_db_for_test(&self, team_id: TeamId, db: Arc<LocalDb>) {
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
                        async move { minter.mint(&team_id).await.map_err(turso::Error::Error) }
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
        }
        let db = Arc::new(db);
        self.teams
            .write()
            .await
            .insert(cfg.team_id.clone(), db.clone());
        // Once the host has enabled the loop, a newly opened replica (startup or
        // runtime-created) gets its push+pull tasks immediately; a team opened
        // before enablement is picked up by `enable_team_sync`.
        if let Some(runtime) = self.sync_runtime.read().await.clone() {
            self.spawn_team_sync(&cfg.team_id, db.clone(), &runtime)
                .await;
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
        if tasks.contains_key(team_id) {
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
        let open: Vec<(TeamId, Arc<LocalDb>)> = self
            .teams
            .read()
            .await
            .iter()
            .map(|(id, db)| (id.clone(), db.clone()))
            .collect();
        for (team_id, db) in open {
            self.spawn_team_sync(&team_id, db, &runtime).await;
        }
    }

    /// Stop a team's sync loop and forget its replica. Dropping the handle aborts
    /// the push+pull tasks; integrity holds because an aborted op marks nothing
    /// done and unpushed frames simply retry on a future open.
    pub async fn close_team(&self, team_id: &str) {
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
    pub async fn registered_team_ids(&self) -> DbResult<Vec<String>> {
        self.local
            .query_all("SELECT id FROM teams", (), |row| row.text(0))
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
    pub async fn forget_team(&self, team_id: &str) -> DbResult<bool> {
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
    pub async fn apply_pending_search(&self) -> DbResult<usize> {
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
async fn reconcile_team_routes(
    local: &LocalDb,
    team_db: &LocalDb,
    routes: &RwLock<RouteMap>,
    team_id: &TeamId,
) -> DbResult<usize> {
    let projects = team_db
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query("SELECT key, repo_path FROM projects", ())
                    .await?;
                let mut projects = Vec::new();
                while let Some(row) = rows.next().await? {
                    projects.push((row.text(0)?, row.text(1)?));
                }
                Ok(projects)
            })
        })
        .await?;
    let now = chrono::Utc::now().timestamp();
    let mut added = 0;
    for (project_key, repo_path) in projects {
        let key = project_key.to_uppercase();
        local
            .execute(
                "INSERT OR IGNORE INTO project_routes(project_key, team_id, created_at)
                 VALUES (?1, ?2, ?3)",
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
        routes.write().await.insert(key, Some(team_id.clone()));
        added += 1;
    }
    Ok(added)
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
    async fn reconcile(&self) {
        if let Err(error) =
            reconcile_team_routes(&self.local, &self.team_db, &self.routes, &self.team_id).await
        {
            log::warn!(
                "pull-triggered route reconcile for team `{}` failed: {error}",
                self.team_id
            );
        }
    }
}

// ============================================================================
// Migration Status Types (for frontend communication)
// ============================================================================

/// Status check result for migration UI
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MigrationStatus {
    pub needed: bool,
    pub pending_migrations: Vec<String>,
    pub current_db_path: String,
    pub error_message: Option<String>,
}

/// Schema change detected during migration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaChange {
    pub table: String,
    pub change_type: String,
    pub old_name: Option<String>,
    pub new_name: Option<String>,
    pub auto_mapped: bool,
}

/// Per-table result for frontend display
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableMigrationResult {
    pub name: String,
    pub old_count: usize,
    pub new_count: usize,
    pub status: String,
    pub error: Option<String>,
}

/// Final migration result for frontend display
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MigrationResult {
    pub success: bool,
    pub tables: Vec<TableMigrationResult>,
    pub schema_changes: Vec<SchemaChange>,
    pub total_rows_restored: usize,
    pub total_rows_attempted: usize,
    pub warnings: Vec<String>,
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

        reconcile_team_routes(&dbs.local, &team_db, &dbs.routes, &"teamABC123".to_string())
            .await
            .unwrap();

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
}
