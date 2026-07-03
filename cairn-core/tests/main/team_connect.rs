//! Desktop team data integration (team-collab slice 2.3): `open_team`
//! concurrency-safety, route reconcile from a synced-in replica, and the
//! rotating sync-token callback.
//!
//! ## A skip is NOT a pass — the server-backed tests MUST be run unfenced
//!
//! Every server-backed test here self-skips inside the worktree fence
//! (`common::skip_if_fenced`) and when no sync server is reachable. So a fenced
//! `bun run test:rust` SKIPS them and still reads green — that green says NOTHING
//! about whether they pass. To exercise them for real, run UNFENCED (the
//! `CAIRN_CALLBACK_URL`/`CAIRN_RUN_ID`/`CAIRN_WORKTREE` trio unset) with
//! `tursodb` on PATH, e.g. from `src-tauri/`:
//!
//! ```text
//! cargo test -p cairn-core --features test-utils --test main --no-run
//! env -u CAIRN_SANDBOXED -u CAIRN_CALLBACK_URL -u CAIRN_RUN_ID -u CAIRN_WORKTREE \
//!     ./target/debug/deps/main-* team_connect --test-threads=1
//! ```

use crate::common;

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::common::sync_server::SyncServer;
use cairn_core::account::TeamTokenMinter;
use cairn_core::internal::db::{DbState, SyncRuntime, TeamConfig};
use cairn_core::internal::services::testing::CapturingEmitter;
use cairn_core::internal::storage::{
    LocalDb, MigrationRunner, SearchIndex, SyncCadence, TURSO_MIGRATIONS,
};
use tempfile::tempdir;

fn test_cadence() -> SyncCadence {
    SyncCadence {
        push_debounce: Duration::from_millis(100),
        push_backstop: Duration::from_secs(1),
        pull_interval: Duration::from_millis(200),
        backoff_base: Duration::from_millis(100),
        backoff_cap: Duration::from_secs(2),
    }
}

async fn private_db_state(dir: &Path, name: &str) -> Arc<DbState> {
    let db = LocalDb::open(dir.join(name)).await.unwrap();
    MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
        .run(&db)
        .await
        .unwrap();
    let search = SearchIndex::open_or_create(dir.join(format!("{name}.search"))).unwrap();
    Arc::new(DbState::new(Arc::new(db), Arc::new(search)))
}

async fn enable_loop(dbs: &DbState) -> Arc<CapturingEmitter> {
    let emitter = Arc::new(CapturingEmitter::new());
    dbs.enable_team_sync(SyncRuntime {
        emitter: emitter.clone(),
        cadence: test_cadence(),
    })
    .await;
    emitter
}

fn team_config(team_id: &str, url: &str, replica: &Path) -> TeamConfig {
    TeamConfig {
        team_id: team_id.to_string(),
        team_name: "Team".to_string(),
        sync_url: url.to_string(),
        auth_token: None,
        replica_path: replica.to_path_buf(),
    }
}

/// Register a team in the private `teams` catalog so a `project_routes` row
/// (which FKs to `teams(id)`) can be written during reconcile.
async fn seed_team_registry(dbs: &DbState, team_id: &str, url: &str, replica: &Path) {
    dbs.local
        .execute(
            "INSERT INTO teams(id, name, sync_url, auth_token, replica_path, created_at) \
             VALUES (?1, 'Team', ?2, NULL, ?3, 1)",
            (team_id, url, replica.to_string_lossy().to_string()),
        )
        .await
        .unwrap();
}

/// A stub minter with an atomic invocation counter and a flip-able failure mode,
/// mirroring the turso reference's per-request callback test. Shared via `Arc`,
/// so flipping `fail` is visible to the callback captured at open time.
struct CountingMinter {
    count: Arc<AtomicUsize>,
    fail: AtomicBool,
    token: String,
}

#[async_trait::async_trait]
impl TeamTokenMinter for CountingMinter {
    async fn mint(&self, _team_id: &str) -> Result<String, String> {
        self.count.fetch_add(1, Ordering::SeqCst);
        if self.fail.load(Ordering::SeqCst) {
            Err("stub mint failure".to_string())
        } else {
            Ok(self.token.clone())
        }
    }
}

async fn wait_for_team_name(db: &LocalDb, id: &str, expected: &str, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let seen = db
            .query_text("SELECT name FROM teams WHERE id = ?1", (id.to_string(),))
            .await
            .unwrap();
        if seen.as_deref() == Some(expected) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    false
}

/// Concurrency regression (REQUIRED): two concurrent `open_team` of the SAME
/// team converge on ONE replica handle, one `teams` entry, and one sync-task
/// pair — the double-checked single-flight under the open-gate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_open_team_is_single_flight() {
    if common::skip_if_fenced("concurrent_open_team_is_single_flight") {
        return;
    }
    let Some(server) = SyncServer::locate_or_spawn() else {
        eprintln!("skipping: no CAIRN_TEST_SYNC_URL and no tursodb on PATH");
        return;
    };
    let url = server.url().to_string();
    let dir = tempdir().unwrap();

    let dbs = private_db_state(dir.path(), "private.db").await;
    enable_loop(&dbs).await;
    let replica = dir.path().join("team.db");

    let (a, b) = tokio::join!(
        dbs.open_team(team_config("team-1", &url, &replica)),
        dbs.open_team(team_config("team-1", &url, &replica)),
    );
    let a = a.expect("first open");
    let b = b.expect("second open");

    assert!(
        Arc::ptr_eq(&a, &b),
        "both concurrent opens must return the SAME replica handle"
    );
    assert_eq!(
        dbs.open_team_count().await,
        1,
        "exactly one team replica is registered"
    );
    assert_eq!(
        dbs.sync_task_count().await,
        1,
        "exactly one sync-task pair is spawned"
    );
}

/// Route reconcile: a host opening a team whose replica already carries a project
/// it never created (a teammate's, arriving via bootstrap) persists a
/// `project_routes` row and routes that key to the replica — NOT `local`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn open_team_reconciles_synced_in_project_routes() {
    if common::skip_if_fenced("open_team_reconciles_synced_in_project_routes") {
        return;
    }
    let Some(server) = SyncServer::locate_or_spawn() else {
        eprintln!("skipping: no CAIRN_TEST_SYNC_URL and no tursodb on PATH");
        return;
    };
    let url = server.url().to_string();
    let dir = tempdir().unwrap();

    // Host A establishes the team and inserts a project a teammate “created”,
    // then pushes — WITHOUT writing any route on this host's behalf.
    let dbs_a = private_db_state(dir.path(), "private-a.db").await;
    let replica_a = dir.path().join("team-a.db");
    seed_team_registry(&dbs_a, "team-1", &url, &replica_a).await;
    let team_a = dbs_a
        .open_team(team_config("team-1", &url, &replica_a))
        .await
        .unwrap();
    // open_team seeds the `teams` root row, so the project insert's FK resolves.
    team_a
        .execute(
            "INSERT INTO projects(id, team_id, name, \"key\", repo_path, created_at, updated_at) \
             VALUES ('p', 'team-1', 'Proj', 'TEAMP', '/tmp/p', 1, 1)",
            (),
        )
        .await
        .unwrap();
    team_a.push().await.unwrap();

    // Host B (which never created TEAMP) opens the team; reconcile must route it.
    let dbs_b = private_db_state(dir.path(), "private-b.db").await;
    let replica_b = dir.path().join("team-b.db");
    seed_team_registry(&dbs_b, "team-1", &url, &replica_b).await;
    let team_b = dbs_b
        .open_team(team_config("team-1", &url, &replica_b))
        .await
        .unwrap();

    let route = dbs_b
        .local
        .query_text(
            "SELECT team_id FROM project_routes WHERE project_key = 'TEAMP'",
            (),
        )
        .await
        .unwrap();
    assert_eq!(
        route.as_deref(),
        Some("team-1"),
        "open_team must persist a project_routes row for the synced-in project"
    );
    assert!(
        Arc::ptr_eq(&dbs_b.for_project("teamp").await, &team_b),
        "the synced-in project routes to the team replica, not local"
    );
    assert!(
        !Arc::ptr_eq(&dbs_b.for_project("teamp").await, &dbs_b.local),
        "the synced-in project must NOT fall back to the private database"
    );
}

/// Rotation: the rotating-token callback is invoked when a minter is installed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn open_team_invokes_the_rotating_token_callback() {
    if common::skip_if_fenced("open_team_invokes_the_rotating_token_callback") {
        return;
    }
    let Some(server) = SyncServer::locate_or_spawn() else {
        eprintln!("skipping: no CAIRN_TEST_SYNC_URL and no tursodb on PATH");
        return;
    };
    let url = server.url().to_string();
    let dir = tempdir().unwrap();

    let dbs = private_db_state(dir.path(), "private.db").await;
    let counter = Arc::new(AtomicUsize::new(0));
    dbs.set_team_token_minter(Arc::new(CountingMinter {
        count: counter.clone(),
        fail: AtomicBool::new(false),
        token: "rotating-token".to_string(),
    }))
    .await;

    let replica = dir.path().join("team.db");
    seed_team_registry(&dbs, "team-1", &url, &replica).await;
    dbs.open_team(team_config("team-1", &url, &replica))
        .await
        .expect("open with a rotating-token minter");

    assert!(
        counter.load(Ordering::SeqCst) > 0,
        "the auth callback must be invoked before sync HTTP requests during open"
    );
}

/// Rotation resilience: when the minter starts failing, the push loop backs off
/// (never crashes); when it recovers, the backed-off commit lands on a peer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn push_recovers_after_a_minter_outage() {
    if common::skip_if_fenced("push_recovers_after_a_minter_outage") {
        return;
    }
    let Some(server) = SyncServer::locate_or_spawn() else {
        eprintln!("skipping: no CAIRN_TEST_SYNC_URL and no tursodb on PATH");
        return;
    };
    let url = server.url().to_string();
    let dir = tempdir().unwrap();

    let dbs_a = private_db_state(dir.path(), "private-a.db").await;
    let minter = Arc::new(CountingMinter {
        count: Arc::new(AtomicUsize::new(0)),
        fail: AtomicBool::new(false),
        token: "tok".to_string(),
    });
    dbs_a.set_team_token_minter(minter.clone()).await;
    enable_loop(&dbs_a).await;
    let team_a = dbs_a
        .open_team(team_config("team-1", &url, &dir.path().join("team-a.db")))
        .await
        .expect("host A opens the team with a working minter");

    // Minter outage: the push task's mint now errors, so push backs off.
    minter.fail.store(true, Ordering::SeqCst);
    team_a
        .execute(
            "INSERT INTO teams(id, name, created_at, updated_at) VALUES ('r1', 'One', 1, 1)",
            (),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Recovery: the minter works again; the backed-off push should land.
    minter.fail.store(false, Ordering::SeqCst);

    // Host B (unauthenticated; same local server) converges on the row.
    let dbs_b = private_db_state(dir.path(), "private-b.db").await;
    enable_loop(&dbs_b).await;
    let team_b = dbs_b
        .open_team(team_config("team-1", &url, &dir.path().join("team-b.db")))
        .await
        .expect("host B opens the team");

    assert!(
        wait_for_team_name(&team_b, "r1", "One", Duration::from_secs(15)).await,
        "the commit written during the minter outage must land on B after recovery"
    );
}
