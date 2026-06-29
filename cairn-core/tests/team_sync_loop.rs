//! Per-team background sync loop proof (CAIRN-2140): a routed write propagates
//! between members through the LOOP — the push task on the writer and the pull
//! task on the reader — with NO manual `push()`/`pull()`.
//!
//! ## A skip is NOT a pass — these tests MUST be run unfenced
//!
//! Every server-backed test here self-skips inside the worktree fence: when the
//! `CAIRN_CALLBACK_URL` / `CAIRN_RUN_ID` / `CAIRN_WORKTREE` trio is present
//! (`common::skip_if_fenced`), each returns early. So a fenced `bun run
//! test:rust` SKIPS them and still reads green — a skip carries no signal about
//! correctness. To exercise them for real, run UNFENCED (the trio unset) with
//! `tursodb` on PATH, e.g. from `src-tauri/`:
//!
//! ```text
//! cargo test -p cairn-core --features test-utils --test team_sync_loop --no-run
//! env -u CAIRN_SANDBOXED -u CAIRN_CALLBACK_URL -u CAIRN_RUN_ID -u CAIRN_WORKTREE \
//!     ./target/debug/deps/team_sync_loop-* --test-threads=1
//! ```
//!
//! Self-skips inside the worktree fence (`common::skip_if_fenced`) and when no
//! sync server is reachable. Unfenced, each server-backed test enables the loop
//! via `DbState::enable_team_sync` and asserts propagation end to end. The
//! dormancy test needs no server and runs everywhere, proving the loop is inert
//! with no team configured.

mod common;

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use cairn_core::internal::db::{DbState, SyncRuntime, TeamConfig};
use cairn_core::internal::services::testing::CapturingEmitter;
use cairn_core::internal::storage::{
    LocalDb, MigrationRunner, SearchIndex, SyncCadence, TURSO_MIGRATIONS,
};
use common::sync_server::SyncServer;
use tempfile::tempdir;

/// A fast cadence so the loop converges in seconds rather than the production
/// 30s/3s defaults, while keeping a debounce and a backoff to exercise.
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

/// Enable the per-team sync loop with the test cadence and a capturing emitter,
/// returning the emitter so a test can assert on the pull-driven `db-change`.
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

/// Poll `db` for a team-row name, returning true once it equals `expected` or
/// false at the deadline.
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

async fn insert_team_row(db: &LocalDb, id: &str, name: &str) {
    db.execute(
        "INSERT INTO teams(id, name, created_at, updated_at) VALUES (?1, ?2, 1, 1)",
        (id.to_string(), name.to_string()),
    )
    .await
    .unwrap();
}

/// (1) End-to-end through the loop: a routed write on host A becomes visible on
/// host B with no manual push — A's push task and B's pull task carry it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_on_host_a_propagates_to_host_b_through_the_loop() {
    if common::skip_if_fenced("write_on_host_a_propagates_to_host_b_through_the_loop") {
        return;
    }
    let Some(server) = SyncServer::locate_or_spawn() else {
        eprintln!("skipping: no CAIRN_TEST_SYNC_URL and no tursodb on PATH");
        return;
    };
    let url = server.url().to_string();
    let dir = tempdir().unwrap();

    // Host A: enable the loop, then open the team (establishes + pushes schema).
    let dbs_a = private_db_state(dir.path(), "private-a.db").await;
    enable_loop(&dbs_a).await;
    let team_a = dbs_a
        .open_team(team_config("team-1", &url, &dir.path().join("team-a.db")))
        .await
        .expect("host A opens the team");

    // Host B: enable the loop, then open the same team (bootstraps the schema).
    let dbs_b = private_db_state(dir.path(), "private-b.db").await;
    let emitter_b = enable_loop(&dbs_b).await;
    let team_b = dbs_b
        .open_team(team_config("team-1", &url, &dir.path().join("team-b.db")))
        .await
        .expect("host B opens the team");

    // Routed write on A — NO manual push.
    insert_team_row(&team_a, "t1", "Propagated").await;

    assert!(
        wait_for_team_name(&team_b, "t1", "Propagated", Duration::from_secs(10)).await,
        "A's write should reach B through the push+pull loop within 10s"
    );
    assert!(
        emitter_b.has_event("db-change"),
        "B's pull task should emit a db-change when it applies pulled frames"
    );
}

/// (2) Transient-outage resilience: the sync server dies while A writes, A's
/// push backs off, and after the server restarts every commit lands on B with
/// none lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn push_recovers_after_a_transient_outage_with_no_lost_commits() {
    if common::skip_if_fenced("push_recovers_after_a_transient_outage_with_no_lost_commits") {
        return;
    }
    let Some(mut server) = SyncServer::locate_or_spawn() else {
        eprintln!("skipping: no CAIRN_TEST_SYNC_URL and no tursodb on PATH");
        return;
    };
    if !server.is_owned() {
        eprintln!("skipping: needs an owned tursodb (cannot restart an external server)");
        return;
    }
    let url = server.url().to_string();
    let dir = tempdir().unwrap();

    let dbs_a = private_db_state(dir.path(), "private-a.db").await;
    enable_loop(&dbs_a).await;
    let team_a = dbs_a
        .open_team(team_config("team-1", &url, &dir.path().join("team-a.db")))
        .await
        .expect("host A opens the team");

    // Outage: kill the server, then write five rows. The writes commit locally;
    // A's push task fails and backs off, retaining the unpushed frames.
    server.stop();
    for i in 0..5 {
        insert_team_row(&team_a, &format!("o{i}"), &format!("Outage {i}")).await;
    }
    // Let the push task attempt, fail, and back off at least once.
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Recovery: restart on the same address and backing file.
    assert!(
        server.restart(),
        "the sync server should restart on the same address"
    );

    // Host B opens after recovery and should converge on ALL five rows once A's
    // backed-off push lands them.
    let dbs_b = private_db_state(dir.path(), "private-b.db").await;
    enable_loop(&dbs_b).await;
    let team_b = dbs_b
        .open_team(team_config("team-1", &url, &dir.path().join("team-b.db")))
        .await
        .expect("host B opens the team after recovery");

    let deadline = Instant::now() + Duration::from_secs(15);
    let mut count = 0_i64;
    while Instant::now() < deadline {
        count = team_b
            .query_text(
                "SELECT CAST(COUNT(*) AS TEXT) FROM teams WHERE id LIKE 'o%'",
                (),
            )
            .await
            .unwrap()
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0);
        if count == 5 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(
        count, 5,
        "all five commits written during the outage must land on B after recovery"
    );
}

/// (3) Per-team isolation: a second team whose server is dead loops in backoff
/// without stalling a healthy team's propagation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unreachable_team_does_not_stall_a_healthy_team() {
    if common::skip_if_fenced("an_unreachable_team_does_not_stall_a_healthy_team") {
        return;
    }
    let Some(good) = SyncServer::locate_or_spawn() else {
        eprintln!("skipping: no CAIRN_TEST_SYNC_URL and no tursodb on PATH");
        return;
    };
    let Some(mut bad) = SyncServer::locate_or_spawn() else {
        eprintln!("skipping: needs a second tursodb instance");
        return;
    };
    if !good.is_owned() || !bad.is_owned() {
        eprintln!("skipping: needs two owned tursodb instances (cannot kill an external server)");
        return;
    }
    let good_url = good.url().to_string();
    let bad_url = bad.url().to_string();
    let dir = tempdir().unwrap();

    // Host A enables the loop and opens BOTH teams against live servers, so both
    // teams' loops spawn.
    let dbs_a = private_db_state(dir.path(), "private-a.db").await;
    enable_loop(&dbs_a).await;
    let team_good = dbs_a
        .open_team(team_config(
            "good",
            &good_url,
            &dir.path().join("good-a.db"),
        ))
        .await
        .expect("open good team");
    let team_bad = dbs_a
        .open_team(team_config("bad", &bad_url, &dir.path().join("bad-a.db")))
        .await
        .expect("open bad team");

    // Kill team-bad's server: its push+pull loop is now wedged in backoff.
    bad.stop();
    insert_team_row(&team_bad, "b", "Bad").await;
    insert_team_row(&team_good, "g", "Good").await;

    // A second host on the GOOD team still receives team-good's write promptly,
    // proving team-bad's stuck loop never stalled team-good's.
    let dbs_b = private_db_state(dir.path(), "private-b.db").await;
    enable_loop(&dbs_b).await;
    let team_good_b = dbs_b
        .open_team(team_config(
            "good",
            &good_url,
            &dir.path().join("good-b.db"),
        ))
        .await
        .expect("host B opens the good team");

    assert!(
        wait_for_team_name(&team_good_b, "g", "Good", Duration::from_secs(10)).await,
        "team-good propagates within bound despite team-bad's loop being stuck in backoff"
    );
}

/// (4) Dormancy: with no team configured, enabling the loop spawns zero tasks,
/// emits nothing, and leaves local-only behavior unchanged. Needs no server, so
/// it runs everywhere (including inside the fence).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dormant_with_no_team_configured() {
    let dir = tempdir().unwrap();
    let dbs = private_db_state(dir.path(), "private.db").await;
    let emitter = enable_loop(&dbs).await;

    // A local-only write still works and is byte-unchanged (no synced DB). The
    // PRIVATE `teams` table is the routing registry (distinct columns from the
    // team DB's `teams` root), so this writes its schema.
    dbs.local
        .execute(
            "INSERT INTO teams(id, name, sync_url, replica_path, created_at) \
             VALUES ('local', 'Local', 'http://unused', '/tmp/unused', 1)",
            (),
        )
        .await
        .unwrap();

    // Give any (nonexistent) loop a chance to misbehave.
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert_eq!(
        dbs.all_dbs().await.len(),
        1,
        "with no team configured the only open database is the private one"
    );
    assert!(
        !emitter.has_event("db-change"),
        "with no synced DB the pull loop never runs, so it emits no db-change"
    );
}

/// (5) No push<->pull feedback loop: applying a `pull()` must NOT fire the
/// `commit_signal` that drives the outbound push task. `commit_signal` is fired
/// in exactly one place — the Ok arm of `LocalDb::transaction_with_begin` —
/// whereas `pull()` applies remote frames via physical WAL replay OUTSIDE the
/// transaction API, so a pull can never re-arm a push. Without this property a
/// pulled write would wake the writer to push, which would wake the reader to
/// pull, ad infinitum. Driven manually (no loop) for determinism: arm a fresh
/// `commit_signal` waiter on the reader, pull real remote frames, and assert the
/// waiter does NOT complete (a short timeout that must ELAPSE); then confirm a
/// genuine local transaction DOES fire it, proving the signal itself works.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn applying_a_pull_does_not_fire_the_commit_signal() {
    if common::skip_if_fenced("applying_a_pull_does_not_fire_the_commit_signal") {
        return;
    }
    let Some(server) = SyncServer::locate_or_spawn() else {
        eprintln!("skipping: no CAIRN_TEST_SYNC_URL and no tursodb on PATH");
        return;
    };
    let url = server.url().to_string();
    let dir = tempdir().unwrap();

    // Host A establishes the team and writes + pushes a first row.
    let dbs_a = private_db_state(dir.path(), "private-a.db").await;
    let team_a = dbs_a
        .open_team(team_config("team-1", &url, &dir.path().join("team-a.db")))
        .await
        .expect("host A opens the team");
    insert_team_row(&team_a, "t1", "One").await;
    team_a.push().await.unwrap();

    // Host B bootstraps the established team (via pull, NOT a transaction — so B
    // has fired no commit signal and holds no stale permit).
    let dbs_b = private_db_state(dir.path(), "private-b.db").await;
    let team_b = dbs_b
        .open_team(team_config("team-1", &url, &dir.path().join("team-b.db")))
        .await
        .expect("host B opens the team");

    // A writes a SECOND row and pushes, giving B real remote frames to pull.
    insert_team_row(&team_a, "t2", "Two").await;
    team_a.push().await.unwrap();

    // Negative case, scoped so the armed waiter deregisters on drop before the
    // sanity check arms a fresh one (a lingering waiter would FIFO-steal the next
    // notify_one).
    {
        // open_team runs seed_team_root (CAIRN-2180), an idempotent tracked write
        // whose committed transaction fires commit_signal once even on B's no-op
        // INSERT OR IGNORE (B bootstrapped A's already-seeded root). Consume that
        // one bootstrap permit FIRST by awaiting the ready Notified; a bare
        // enable() would instead leave the future READY and falsely satisfy the
        // pull assertion below.
        let signal = team_b.commit_signal();
        {
            let drain = signal.notified();
            tokio::pin!(drain);
            if drain.as_mut().enable() {
                drain.await;
            }
        }
        // Arm a FRESH waiter for the pull, now clean (no permit), so the assertion
        // reflects ONLY whether the pull fires commit_signal.
        let notified = signal.notified();
        tokio::pin!(notified);
        assert!(
            !notified.as_mut().enable(),
            "after draining the bootstrap seed permit, B holds no commit permit before the pull"
        );

        // The pull lands remote frames via physical WAL replay (outside the
        // transaction API). It must report changes — otherwise the negative
        // assertion below would be vacuous.
        let changed = team_b.pull().await.unwrap();
        assert!(
            changed,
            "the pull must actually land remote frames for this test to mean anything"
        );

        // The armed waiter must NOT complete: a pull fires no commit signal, so
        // the timeout must ELAPSE.
        let fired = tokio::time::timeout(Duration::from_millis(500), notified.as_mut()).await;
        assert!(
            fired.is_err(),
            "applying a pull must NOT fire commit_signal (no push<->pull feedback loop)"
        );
    }
    assert_eq!(
        team_b
            .query_text("SELECT name FROM teams WHERE id = ?1", ("t2".to_string(),))
            .await
            .unwrap()
            .as_deref(),
        Some("Two"),
        "the pulled row is present even though it fired no commit signal"
    );

    // Sanity: a genuine LOCAL transaction on the same synced replica DOES fire the
    // signal, proving the mechanism works and the negative result above is real.
    let signal2 = team_b.commit_signal();
    let notified2 = signal2.notified();
    tokio::pin!(notified2);
    notified2.as_mut().enable();
    insert_team_row(&team_b, "t3", "Three").await;
    let fired2 = tokio::time::timeout(Duration::from_secs(2), notified2.as_mut()).await;
    assert!(
        fired2.is_ok(),
        "a local transaction on the synced replica DOES fire commit_signal"
    );
}
