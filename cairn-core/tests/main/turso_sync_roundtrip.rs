//! Turso Sync round-trip proof: a write on one replica, pushed to a local sync
//! server, becomes visible on a second replica after it bootstraps and pulls.
//!
//! This is the proof obligation for the multi-database storage foundation
//! (CAIRN-2126): it exercises `LocalDb::open_synced` + `push`/`pull` end to end
//! against a real sync server.
//!
//! ## A skip is NOT a pass — these tests MUST be run unfenced
//!
//! Every test here self-skips inside the worktree fence: when the
//! `CAIRN_CALLBACK_URL` / `CAIRN_RUN_ID` / `CAIRN_WORKTREE` trio is present
//! (`common::skip_if_fenced`), each returns early. So a fenced `bun run
//! test:rust` SKIPS them and still reads green — that green says NOTHING about
//! whether they pass. (This masking is exactly what hid a whole class of FK
//! failures in the routed-create tests until they were finally run unfenced.) To
//! exercise them for real, run UNFENCED (the trio unset) with `tursodb` on PATH,
//! e.g. from `src-tauri/`:
//!
//! ```text
//! cargo test -p cairn-core --features test-utils --test main --no-run
//! env -u CAIRN_SANDBOXED -u CAIRN_CALLBACK_URL -u CAIRN_RUN_ID -u CAIRN_WORKTREE \
//!     ./target/debug/deps/main-* turso_sync_roundtrip --test-threads=1
//! ```
//!
//! Self-skips inside the worktree fence (`common::skip_if_fenced`) and when no
//! sync server is reachable -- `CAIRN_TEST_SYNC_URL` is honored first, otherwise
//! a `tursodb --sync-server` subprocess is spawned when the binary is on PATH.
//! So `bun run test:rust` stays green in the fence while unfenced CI or a
//! provisioned box runs it for real.

use crate::common;
use std::path::Path;
use std::sync::Arc;

use crate::common::sync_server::SyncServer;

use cairn_core::internal::db::{DbState, TeamConfig};
use cairn_core::internal::mcp::handlers::{comments_artifacts, issues, messages};
use cairn_core::internal::mcp::types::McpCallbackRequest;
use cairn_core::internal::orchestrator::Orchestrator;
use cairn_core::internal::services::testing::TestServicesBuilder;
use cairn_core::internal::services::RealClock;
use cairn_core::internal::storage::{
    LocalDb, MigrationRunner, SearchIndex, TEAM_MIGRATIONS, TURSO_MIGRATIONS,
};
use cairn_core::issues::crud as issue_crud;
use cairn_core::models::CreateProject;
use cairn_core::projects::crud;
use tempfile::tempdir;

const PROBE_SCHEMA: &str = "
    CREATE TABLE sync_probe (
        id TEXT PRIMARY KEY NOT NULL,
        val TEXT NOT NULL
    );
";

async fn open_replica(path: &Path, url: &str) -> LocalDb {
    LocalDb::open_synced(path, url, None)
        .await
        .expect("open synced replica")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn turso_sync_write_push_pull_read_roundtrip() {
    if common::skip_if_fenced("turso_sync_write_push_pull_read_roundtrip") {
        return;
    }
    let Some(server) = SyncServer::locate_or_spawn() else {
        eprintln!(
            "skipping turso_sync_write_push_pull_read_roundtrip: no CAIRN_TEST_SYNC_URL and no tursodb on PATH"
        );
        return;
    };
    let url = server.url();

    let dir = tempdir().unwrap();
    let path_a = dir.path().join("replica-a.db");
    let path_b = dir.path().join("replica-b.db");

    // Replica A establishes the schema, writes a row, and pushes it.
    let a = open_replica(&path_a, url).await;
    a.execute_batch(PROBE_SCHEMA).await.unwrap();
    a.execute(
        "INSERT INTO sync_probe(id, val) VALUES (?1, ?2)",
        ("r1", "one"),
    )
    .await
    .unwrap();
    a.push().await.unwrap();

    // Replica B bootstraps from the server (schema + data) and sees the row.
    let b = open_replica(&path_b, url).await;
    let seen = b
        .query_text("SELECT val FROM sync_probe WHERE id = ?1", ("r1",))
        .await
        .unwrap();
    assert_eq!(
        seen.as_deref(),
        Some("one"),
        "replica B should bootstrap the row replica A pushed"
    );

    // A writes a second row and pushes; B pulls and converges on it.
    a.execute(
        "INSERT INTO sync_probe(id, val) VALUES (?1, ?2)",
        ("r2", "two"),
    )
    .await
    .unwrap();
    a.push().await.unwrap();

    let changed = b.pull().await.unwrap();
    assert!(changed, "replica B.pull() should report applied changes");
    let seen2 = b
        .query_text("SELECT val FROM sync_probe WHERE id = ?1", ("r2",))
        .await
        .unwrap();
    assert_eq!(
        seen2.as_deref(),
        Some("two"),
        "replica B should see A's second pushed row after pull"
    );
}

/// §1 CDC/trigger gate for the shared-space schema (CAIRN-2129): empirically
/// confirms, against the REAL team schema, the behavior the team-DB search
/// design depends on.
///
/// (A) A trigger-generated row PUSHES: an `issues` insert on replica A fires the
///     `search_issues_insert` AFTER INSERT trigger locally, enqueuing one
///     `search_outbox` row; CDC is logical/statement-level so that row is a
///     distinct logical change and rides the push.
/// (B) `pull`/bootstrap does NOT re-fire triggers on the receiver: replica B
///     receives the issue AND the outbox row as ordinary synced data via
///     physical WAL-frame replay, so exactly ONE outbox row exists (not two),
///     and its mutable `status` column converged unchanged.
///
/// This is why this slice keeps the schema identical across lineages and drains
/// only locally-originated writes: correctly indexing pull-arrived rows (whose
/// triggers never fire on the receiver) is a deferred follow-up.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn team_schema_trigger_row_syncs_without_refiring_on_receiver() {
    if common::skip_if_fenced("team_schema_trigger_row_syncs_without_refiring_on_receiver") {
        return;
    }
    let Some(server) = SyncServer::locate_or_spawn() else {
        eprintln!(
            "skipping team_schema_trigger_row_syncs_without_refiring_on_receiver: no CAIRN_TEST_SYNC_URL and no tursodb on PATH"
        );
        return;
    };
    let url = server.url();

    let dir = tempdir().unwrap();
    let path_a = dir.path().join("team-a.db");
    let path_b = dir.path().join("team-b.db");

    // Replica A establishes the real team schema and inserts a team -> project ->
    // issue chain. The issues AFTER INSERT search trigger fires LOCALLY on A.
    let a = open_replica(&path_a, url).await;
    MigrationRunner::new(TEAM_MIGRATIONS.to_vec())
        .run(&a)
        .await
        .unwrap();
    a.execute(
        "INSERT INTO teams(id, name, created_at, updated_at) VALUES ('t', 'Team', 1, 1)",
        (),
    )
    .await
    .unwrap();
    a.execute(
        "INSERT INTO projects(id, team_id, name, \"key\", repo_path, created_at, updated_at) \
         VALUES ('p', 't', 'Proj', 'TEAM', '/tmp/p', 1, 1)",
        (),
    )
    .await
    .unwrap();
    a.execute(
        "INSERT INTO issues(id, project_id, number, title, created_at, updated_at) \
         VALUES ('i', 'p', 1, 'Index me', 1, 1)",
        (),
    )
    .await
    .unwrap();
    let a_status = a
        .query_text("SELECT status FROM search_outbox WHERE source_id = 'i'", ())
        .await
        .unwrap();
    assert_eq!(
        a_status.as_deref(),
        Some("pending"),
        "(A) the issues trigger must enqueue a pending search_outbox row on the writer"
    );
    a.push().await.unwrap();

    // Replica B bootstraps from the server. It should see the issue AND exactly
    // one outbox row (the one A's trigger generated, arriving as synced data),
    // proving B's trigger did not re-fire on the physically-replayed insert.
    let b = open_replica(&path_b, url).await;
    let issue = b
        .query_text("SELECT title FROM issues WHERE id = 'i'", ())
        .await
        .unwrap();
    assert_eq!(
        issue.as_deref(),
        Some("Index me"),
        "replica B should bootstrap the issue A pushed"
    );
    let outbox_count = b
        .query_text(
            "SELECT CAST(COUNT(*) AS TEXT) FROM search_outbox WHERE source_id = 'i'",
            (),
        )
        .await
        .unwrap();
    assert_eq!(
        outbox_count.as_deref(),
        Some("1"),
        "(B) the trigger-generated outbox row syncs as data and B's trigger does NOT re-fire (2 would mean it did)"
    );
    let b_status = b
        .query_text("SELECT status FROM search_outbox WHERE source_id = 'i'", ())
        .await
        .unwrap();
    assert_eq!(
        b_status.as_deref(),
        Some("pending"),
        "the mutable status column converges via sync unchanged"
    );
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

/// §7 headline proof for the project -> team-DB router: a write resolved through
/// `for_project(team_key)` lands in the team replica (and syncs to a second
/// replica), while `for_project(local_key)` resolves to the private database
/// only — a strict no-op for local projects.
///
/// This is also the CAIRN-2170 non-masking regression anchor. Replica B is a
/// SEPARATE replica file that bootstraps from a raw, freshly-spawned
/// `tursodb --sync-server` (the exact supervisor invocation), and the closing
/// assertion is DATA-LEVEL: it reads the actual `issues` row back out of B after
/// B pulled it, not merely that A's local `push()` returned `Ok`. A push the
/// server silently rejected would leave B empty and FAIL here, rather than
/// self-healing the way a same-window CREATE would. That guarantee only holds
/// when this runs UNFENCED against the pinned tursodb — see the file header and
/// `.github/workflows/sync-tests.yml`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn for_project_routes_writes_to_the_team_db_and_syncs() {
    if common::skip_if_fenced("for_project_routes_writes_to_the_team_db_and_syncs") {
        return;
    }
    let Some(server) = SyncServer::locate_or_spawn() else {
        eprintln!(
            "skipping for_project_routes_writes_to_the_team_db_and_syncs: no CAIRN_TEST_SYNC_URL and no tursodb on PATH"
        );
        return;
    };
    let url = server.url();
    let dir = tempdir().unwrap();

    // A DbState whose private DB holds a local project, and whose `project_routes`
    // map a shared project to a team replica.
    let dbs_a = private_db_state(dir.path(), "private-a.db").await;
    let team = dbs_a
        .open_team(TeamConfig {
            team_id: "team-1".to_string(),
            team_name: "Team".to_string(),
            sync_url: url.to_string(),
            auth_token: None,
            replica_path: dir.path().join("team-a.db"),
        })
        .await
        .expect("open_team establishes and pushes the schema");
    dbs_a.set_route("TEAM", Some("team-1".to_string())).await;
    dbs_a.set_route("LOCAL", None).await;

    // Routing: the shared key resolves to the team replica; the local key (and a
    // lowercase spelling, proving key normalization) resolves to the private DB.
    let routed_team = dbs_a.for_project("team").await;
    let routed_local = dbs_a.for_project("local").await;
    assert!(
        Arc::ptr_eq(&routed_team, &team),
        "for_project(team) must resolve to the opened team replica"
    );
    assert!(
        Arc::ptr_eq(&routed_local, &dbs_a.local),
        "for_project(local) must resolve to the private database"
    );
    assert!(
        !Arc::ptr_eq(&routed_team, &dbs_a.local),
        "the team replica must be a distinct database from private"
    );

    // A write resolved through the routed team DB lands there...
    routed_team
        .execute(
            "INSERT INTO teams(id, name, created_at, updated_at) VALUES ('t', 'Team', 1, 1)",
            (),
        )
        .await
        .unwrap();
    routed_team
        .execute(
            "INSERT INTO projects(id, team_id, name, \"key\", repo_path, created_at, updated_at) \
             VALUES ('p', 't', 'Proj', 'TEAM', '/tmp/p', 1, 1)",
            (),
        )
        .await
        .unwrap();
    routed_team
        .execute(
            "INSERT INTO issues(id, project_id, number, title, created_at, updated_at) \
             VALUES ('shared-1', 'p', 1, 'Shared issue', 1, 1)",
            (),
        )
        .await
        .unwrap();
    let in_team = routed_team
        .query_text("SELECT title FROM issues WHERE id = 'shared-1'", ())
        .await
        .unwrap();
    assert_eq!(in_team.as_deref(), Some("Shared issue"));
    // ...and NOT in the private database.
    let in_private = dbs_a
        .local
        .query_text("SELECT title FROM issues WHERE id = 'shared-1'", ())
        .await
        .unwrap();
    assert_eq!(
        in_private, None,
        "a routed team write must not touch the private database"
    );

    // Sync proof: push the team DB, then a second host's DbState opens the same
    // team and bootstraps the issue (open_team must NOT re-migrate — the schema
    // already exists on the server).
    routed_team.push().await.unwrap();
    let dbs_b = private_db_state(dir.path(), "private-b.db").await;
    let team_b = dbs_b
        .open_team(TeamConfig {
            team_id: "team-1".to_string(),
            team_name: "Team".to_string(),
            sync_url: url.to_string(),
            auth_token: None,
            replica_path: dir.path().join("team-b.db"),
        })
        .await
        .expect("second host opens the established team replica");
    let seen = team_b
        .query_text("SELECT title FROM issues WHERE id = 'shared-1'", ())
        .await
        .unwrap();
    assert_eq!(
        seen.as_deref(),
        Some("Shared issue"),
        "the second replica should bootstrap the routed team write"
    );
}

/// Register a team in the PRIVATE `teams` catalog so a `project_routes` stub
/// (which FKs to `teams(id)`) can be written. Mirrors what the api/ control
/// plane will seed in production.
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

fn create_project_input(id: &str, key: &str, repo: &Path, team_id: Option<&str>) -> CreateProject {
    CreateProject {
        id: Some(id.to_string()),
        name: format!("{key} Project"),
        key: key.to_string(),
        repo_path: repo.to_string_lossy().to_string(),
        team_id: team_id.map(str::to_string),
    }
}

/// A [`ContentStoreFactory`](cairn_core::internal::storage::ContentStoreFactory) that hands
/// every team the SAME shared store, so what host A offloads, host B fetches.
struct SharedStoreFactory(std::sync::Arc<dyn cairn_core::internal::storage::ContentStore>);

impl cairn_core::internal::storage::ContentStoreFactory for SharedStoreFactory {
    fn store_for(
        &self,
        _team_id: &cairn_core::internal::db::TeamId,
    ) -> std::sync::Arc<dyn cairn_core::internal::storage::ContentStore> {
        self.0.clone()
    }
}

/// §7e (CAIRN-2188) reconstruct-coherence via the shared content store. A
/// torn-down team run offloads its archival blobs to the per-team content store
/// and keeps only rows + hash pointers on the synced replica; a SECOND host
/// bootstraps the run from sync and reconstructs it byte-identically by fetching
/// the blob bytes from the shared store BY HASH — never from an `archival_blobs`
/// table (absent on a team replica) and never from an originating worktree.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn team_run_archival_offloads_to_store_and_reconstructs_on_a_second_replica() {
    if common::skip_if_fenced(
        "team_run_archival_offloads_to_store_and_reconstructs_on_a_second_replica",
    ) {
        return;
    }
    let Some(server) = SyncServer::locate_or_spawn() else {
        eprintln!("skipping team_run_archival_offloads_to_store_...: no sync server");
        return;
    };
    let url = server.url();
    let dir = tempdir().unwrap();

    // One shared content store stands in for the brokered S3-backed store, wired
    // into BOTH hosts' factories. `store` (typed) is kept for assertions; `shared`
    // is the same instance behind the trait object the factories hand out.
    let store = cairn_core::internal::storage::InMemoryContentStore::new();
    let shared: std::sync::Arc<dyn cairn_core::internal::storage::ContentStore> =
        std::sync::Arc::new(store.clone());

    // ---- Host A: routed team project + a system:prompt event, then archive. ----
    let dbs_a = private_db_state(dir.path(), "private-a.db").await;
    dbs_a
        .set_content_store_factory(std::sync::Arc::new(SharedStoreFactory(shared.clone())))
        .await;
    let replica_a = dir.path().join("team-a.db");
    seed_team_registry(&dbs_a, "team-1", url, &replica_a).await;
    let team_a = dbs_a
        .open_team(TeamConfig {
            team_id: "team-1".to_string(),
            team_name: "Team".to_string(),
            sync_url: url.to_string(),
            auth_token: None,
            replica_path: replica_a,
        })
        .await
        .unwrap();
    assert!(
        team_a.content_store().is_some(),
        "the opened team replica carries the content store"
    );

    let repo = dir.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    crud::create_routed(
        &dbs_a,
        &RealClock,
        create_project_input("p", "TEAMP", &repo, Some("team-1")),
        None,
    )
    .await
    .expect("create routed team project");

    // Seed the execution skeleton (shared tables) directly into the team replica.
    let worktree = repo.to_string_lossy().to_string();
    team_a
        .execute(
            "INSERT INTO executions(id, recipe_id, status, started_at) VALUES ('exec','r','running',1)",
            (),
        )
        .await
        .unwrap();
    team_a
        .execute(
            "INSERT INTO jobs(id, execution_id, project_id, status, worktree_path, created_at, updated_at)
             VALUES ('job','exec','p','complete',?1,1,1)",
            (worktree.clone(),),
        )
        .await
        .unwrap();
    team_a
        .execute(
            "INSERT INTO runs(id, job_id, project_id, status, created_at, updated_at)
             VALUES ('run','job','p','exited',1,1)",
            (),
        )
        .await
        .unwrap();

    // A system:prompt event: its static segments archive to content-addressed
    // blobs (no git eligibility needed), which the team offload path PUTs to the
    // shared store. `CLAUDE-BASE` is a static segment, so it leaves the inline row.
    let backend_base = "CLAUDE-BASE ".repeat(800);
    let cairn = format!("\n\n{}", "CAIRN-PROMPT ".repeat(700));
    let workspace = "\n\n## Workspace Instructions\n\nworkspace doctrine".to_string();
    let agent = "\n\n<agent_role>\nbuilder role body".to_string();
    let (data, _content) = cairn_core::internal::storage::event_fixture::system_prompt(&[
        ("backend_base", &backend_base),
        ("cairn", &cairn),
        ("workspace", &workspace),
        ("agent", &agent),
        (
            "dynamic",
            "\n\n## Orientation\n\ncwd=/work/run\n</agent_role>",
        ),
    ]);
    team_a
        .execute(
            "INSERT INTO events(id, run_id, sequence, timestamp, event_type, data, created_at)
             VALUES ('sp1','run',1,1,'system:prompt',?1,1)",
            (data,),
        )
        .await
        .unwrap();

    let summary = cairn_core::archival::archive_target(
        &team_a,
        &worktree,
        &worktree,
        &["job".to_string()],
        None,
    )
    .await
    .unwrap();
    assert_eq!(summary.system_prompt, 1, "the system prompt is archived");
    assert!(
        store.len().await >= 1,
        "the static segments were offloaded to the shared store"
    );

    team_a.push().await.unwrap();

    // ---- Host B: bootstrap from sync, reconstruct from the shared store. ----
    let dbs_b = private_db_state(dir.path(), "private-b.db").await;
    dbs_b
        .set_content_store_factory(std::sync::Arc::new(SharedStoreFactory(shared.clone())))
        .await;
    let team_b = dbs_b
        .open_team(TeamConfig {
            team_id: "team-1".to_string(),
            team_name: "Team".to_string(),
            sync_url: url.to_string(),
            auth_token: None,
            replica_path: dir.path().join("team-b.db"),
        })
        .await
        .expect("second host opens the team");

    // The synced replica row is a STUB: the heavy static bytes are NOT on the
    // replica (they live only in the shared store).
    let raw = team_b
        .query_text("SELECT data FROM events WHERE id = 'sp1'", ())
        .await
        .unwrap()
        .expect("the system:prompt event row synced to host B");
    assert!(
        !raw.contains("CLAUDE-BASE"),
        "the synced replica carries only a stub, never the offloaded segment bytes"
    );

    // Reconstruction on host B fetches the blob bytes from the shared store by
    // hash and restores the prompt — no archival_blobs table on this replica, no
    // originating worktree.
    let events = cairn_core::runs::queries::list_events(team_b.clone(), "run").unwrap();
    let sp = events
        .iter()
        .find(|e| e.id == "sp1")
        .expect("the system:prompt event is present after reconstruction");
    assert!(
        sp.data.contains("CLAUDE-BASE"),
        "host B reconstructs the static segment bytes fetched from the shared store"
    );
}

/// CAIRN-2180 regression: the REAL provisioning sequence — `open_team` on a raw,
/// freshly provisioned replica, then `create_routed` the FIRST project into the
/// team — must NOT fail the `projects.team_id` FOREIGN KEY. The team's `projects`
/// table re-roots at `team_id` (NOT NULL FK to `teams`), and nothing but
/// `open_team` seeds the team's own root row; before the fix the first create
/// failed with `FOREIGN KEY constraint failed`.
///
/// This test relies ENTIRELY on `open_team`'s seed — NO manual root insert. A
/// manual seed here would re-mask the exact gap this regression guards, so the
/// assertions below prove the root row exists because `open_team` put it there,
/// and that it (with the project) syncs through to a SECOND replica.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn open_team_seeds_team_root_so_first_create_routed_succeeds() {
    if common::skip_if_fenced("open_team_seeds_team_root_so_first_create_routed_succeeds") {
        return;
    }
    let Some(server) = SyncServer::locate_or_spawn() else {
        eprintln!("skipping open_team_seeds_team_root_...: no sync server");
        return;
    };
    let url = server.url();
    let dir = tempdir().unwrap();

    let dbs = private_db_state(dir.path(), "private.db").await;
    let replica = dir.path().join("team.db");
    seed_team_registry(&dbs, "team-1", url, &replica).await;
    // A raw replica + TEAM_MIGRATIONS only — open_team must seed the root itself.
    let team = dbs
        .open_team(TeamConfig {
            team_id: "team-1".to_string(),
            team_name: "Acme Team".to_string(),
            sync_url: url.to_string(),
            auth_token: None,
            replica_path: replica,
        })
        .await
        .expect("open raw team replica");

    // open_team seeded the team's own root row with the configured name.
    assert_eq!(
        team.query_text("SELECT name FROM teams WHERE id = 'team-1'", ())
            .await
            .unwrap()
            .as_deref(),
        Some("Acme Team"),
        "open_team must seed the team root row so projects.team_id resolves"
    );

    // The FIRST project into the team must NOT fail the team_id FK.
    crud::create_routed(
        &dbs,
        &RealClock,
        create_project_input("p-team", "TEAMP", &dir.path().join("t"), Some("team-1")),
        None,
    )
    .await
    .expect("first create_routed into a team must not fail the FK");
    assert_eq!(
        team.query_text("SELECT name FROM projects WHERE id = 'p-team'", ())
            .await
            .unwrap()
            .as_deref(),
        Some("TEAMP Project"),
        "the project row lands in the team replica"
    );

    // Both the seeded root and the project propagate to a SECOND replica.
    team.push().await.unwrap();
    let dbs_b = private_db_state(dir.path(), "private-b.db").await;
    let team_b = dbs_b
        .open_team(TeamConfig {
            team_id: "team-1".to_string(),
            team_name: "Acme Team".to_string(),
            sync_url: url.to_string(),
            auth_token: None,
            replica_path: dir.path().join("team-b.db"),
        })
        .await
        .expect("second host opens the team");
    assert_eq!(
        team_b
            .query_text("SELECT name FROM teams WHERE id = 'team-1'", ())
            .await
            .unwrap()
            .as_deref(),
        Some("Acme Team"),
        "the seeded team root row syncs to a second replica"
    );
    assert_eq!(
        team_b
            .query_text("SELECT name FROM projects WHERE id = 'p-team'", ())
            .await
            .unwrap()
            .as_deref(),
        Some("TEAMP Project"),
        "the project row syncs to a second replica"
    );
}

/// §7.1 project-create routing: a team-routed create lands the `projects` row in
/// the team replica with a `project_routes` stub (carrying the team id) in the
/// PRIVATE database; a local create keeps both the row and a NULL route private.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_routed_writes_project_row_to_team_db_and_route_stub_to_private() {
    if common::skip_if_fenced(
        "create_routed_writes_project_row_to_team_db_and_route_stub_to_private",
    ) {
        return;
    }
    let Some(server) = SyncServer::locate_or_spawn() else {
        eprintln!(
            "skipping create_routed_writes_...: no CAIRN_TEST_SYNC_URL and no tursodb on PATH"
        );
        return;
    };
    let url = server.url();
    let dir = tempdir().unwrap();

    let dbs = private_db_state(dir.path(), "private.db").await;
    let replica = dir.path().join("team.db");
    seed_team_registry(&dbs, "team-1", url, &replica).await;
    let team = dbs
        .open_team(TeamConfig {
            team_id: "team-1".to_string(),
            team_name: "Team".to_string(),
            sync_url: url.to_string(),
            auth_token: None,
            replica_path: replica,
        })
        .await
        .expect("open team replica");

    // Team-routed create.
    let (_p, target) = crud::create_routed(
        &dbs,
        &RealClock,
        create_project_input("p-team", "TEAMP", &dir.path().join("t"), Some("team-1")),
        None,
    )
    .await
    .expect("create team-routed project");
    assert!(Arc::ptr_eq(&target, &team));
    assert_eq!(
        team.query_text("SELECT name FROM projects WHERE id = 'p-team'", ())
            .await
            .unwrap()
            .as_deref(),
        Some("TEAMP Project")
    );
    assert_eq!(
        dbs.local
            .query_text("SELECT name FROM projects WHERE id = 'p-team'", ())
            .await
            .unwrap(),
        None,
        "a team project's row must not touch the private database"
    );
    assert_eq!(
        dbs.local
            .query_text(
                "SELECT team_id FROM project_routes WHERE project_key = 'TEAMP'",
                (),
            )
            .await
            .unwrap()
            .as_deref(),
        Some("team-1")
    );
    assert!(
        Arc::ptr_eq(&dbs.for_project("teamp").await, &team),
        "for_project resolves the normalized key to the team replica"
    );

    // Local create: row + NULL route stay private.
    let (_lp, local_target) = crud::create_routed(
        &dbs,
        &RealClock,
        create_project_input("p-local", "LOCALP", &dir.path().join("l"), None),
        None,
    )
    .await
    .expect("create local project");
    assert!(Arc::ptr_eq(&local_target, &dbs.local));
    assert_eq!(
        dbs.local
            .query_text("SELECT name FROM projects WHERE id = 'p-local'", ())
            .await
            .unwrap()
            .as_deref(),
        Some("LOCALP Project")
    );
    assert_eq!(
        team.query_text("SELECT name FROM projects WHERE id = 'p-local'", ())
            .await
            .unwrap(),
        None,
        "a local project's row must not reach the team replica"
    );
    assert_eq!(
        dbs.local
            .query_text(
                "SELECT CAST(COUNT(*) AS TEXT) FROM project_routes \
                 WHERE project_key = 'LOCALP' AND team_id IS NULL",
                (),
            )
            .await
            .unwrap()
            .as_deref(),
        Some("1"),
        "a local project's route stub stores a NULL team"
    );
    assert!(Arc::ptr_eq(&dbs.for_project("localp").await, &dbs.local));
}

/// §7.2 routed mutation + sync: a project created via `create_routed` plus a
/// routed domain write both sync to a second host's replica.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_routed_project_and_routed_write_sync_to_a_second_replica() {
    if common::skip_if_fenced("create_routed_project_and_routed_write_sync_to_a_second_replica") {
        return;
    }
    let Some(server) = SyncServer::locate_or_spawn() else {
        eprintln!("skipping create_routed_project_and_routed_write_sync_...: no sync server");
        return;
    };
    let url = server.url();
    let dir = tempdir().unwrap();

    let dbs_a = private_db_state(dir.path(), "private-a.db").await;
    let replica_a = dir.path().join("team-a.db");
    seed_team_registry(&dbs_a, "team-1", url, &replica_a).await;
    let team_a = dbs_a
        .open_team(TeamConfig {
            team_id: "team-1".to_string(),
            team_name: "Team".to_string(),
            sync_url: url.to_string(),
            auth_token: None,
            replica_path: replica_a,
        })
        .await
        .unwrap();

    let (_p, target) = crud::create_routed(
        &dbs_a,
        &RealClock,
        create_project_input("p", "TEAMP", &dir.path().join("repo"), Some("team-1")),
        None,
    )
    .await
    .expect("create routed team project");
    assert!(Arc::ptr_eq(&target, &team_a));

    // A routed domain write through the handle `for_project` resolves.
    let routed = dbs_a.for_project("TEAMP").await;
    assert!(Arc::ptr_eq(&routed, &team_a));
    routed
        .execute(
            "INSERT INTO issues(id, project_id, number, title, created_at, updated_at) \
             VALUES ('iss', 'p', 1, 'Routed issue', 1, 1)",
            (),
        )
        .await
        .unwrap();
    routed.push().await.unwrap();

    // A second host opens the same team and bootstraps the project AND the issue.
    let dbs_b = private_db_state(dir.path(), "private-b.db").await;
    let team_b = dbs_b
        .open_team(TeamConfig {
            team_id: "team-1".to_string(),
            team_name: "Team".to_string(),
            sync_url: url.to_string(),
            auth_token: None,
            replica_path: dir.path().join("team-b.db"),
        })
        .await
        .expect("second host opens the team");
    assert_eq!(
        team_b
            .query_text("SELECT name FROM projects WHERE id = 'p'", ())
            .await
            .unwrap()
            .as_deref(),
        Some("TEAMP Project"),
        "create_routed's project row syncs to a second replica"
    );
    assert_eq!(
        team_b
            .query_text("SELECT title FROM issues WHERE id = 'iss'", ())
            .await
            .unwrap()
            .as_deref(),
        Some("Routed issue"),
        "a routed write syncs to a second replica"
    );
}

/// §7.4 list aggregation: the union across `all_dbs()` (the primitive the
/// project-list command builds on) surfaces both a local and a team project,
/// proving schema-aware `list_db` reads the team replica's re-rooted `projects`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn all_dbs_list_unions_local_and_team_projects() {
    if common::skip_if_fenced("all_dbs_list_unions_local_and_team_projects") {
        return;
    }
    let Some(server) = SyncServer::locate_or_spawn() else {
        eprintln!("skipping all_dbs_list_unions_...: no sync server");
        return;
    };
    let url = server.url();
    let dir = tempdir().unwrap();
    let dbs = private_db_state(dir.path(), "private.db").await;
    let replica = dir.path().join("team.db");
    seed_team_registry(&dbs, "team-1", url, &replica).await;
    // open_team registers the replica (and seeds its team root); the handle is
    // resolved via `all_dbs`/`for_project` below, so no local binding is needed.
    dbs.open_team(TeamConfig {
        team_id: "team-1".to_string(),
        team_name: "Team".to_string(),
        sync_url: url.to_string(),
        auth_token: None,
        replica_path: replica,
    })
    .await
    .unwrap();

    crud::create_routed(
        &dbs,
        &RealClock,
        create_project_input("p-local", "LOCALP", &dir.path().join("l"), None),
        None,
    )
    .await
    .unwrap();
    crud::create_routed(
        &dbs,
        &RealClock,
        create_project_input("p-team", "TEAMP", &dir.path().join("t"), Some("team-1")),
        None,
    )
    .await
    .unwrap();

    let mut ids = std::collections::HashSet::new();
    for db in dbs.all_dbs().await {
        for project in crud::list_db(&db).await.unwrap() {
            ids.insert(project.id);
        }
    }
    assert!(
        ids.contains("p-local"),
        "the local project appears in the union"
    );
    assert!(
        ids.contains("p-team"),
        "the team project appears in the union (schema-aware list_db reads the re-rooted table)"
    );
}

/// §1.2c routed lifecycle mutation: a team-only project mutated through the
/// KEY-resolved handle lands in the TEAM replica (not the private database) and
/// reads back via the schema-aware `get_db`, then syncs to a second host. This
/// covers the `for_project(key)` + `set_*_db`/`get_db` mutator path; the id-keyed
/// `owning_db` resolver is now a prefix-parse delegate covered by the fail-closed
/// unit tests in `cairn_core::execution::routing`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn routed_lifecycle_mutations_hit_the_team_db_and_sync() {
    if common::skip_if_fenced("routed_lifecycle_mutations_hit_the_team_db_and_sync") {
        return;
    }
    let Some(server) = SyncServer::locate_or_spawn() else {
        eprintln!("skipping routed_lifecycle_mutations_...: no sync server");
        return;
    };
    let url = server.url();
    let dir = tempdir().unwrap();
    let dbs = private_db_state(dir.path(), "private.db").await;
    let replica = dir.path().join("team.db");
    seed_team_registry(&dbs, "team-lifecycle", url, &replica).await;
    let team = dbs
        .open_team(TeamConfig {
            team_id: "team-lifecycle".to_string(),
            team_name: "Team".to_string(),
            sync_url: url.to_string(),
            auth_token: None,
            replica_path: replica,
        })
        .await
        .unwrap();

    crud::create_routed(
        &dbs,
        &RealClock,
        create_project_input(
            "p-team",
            "TEAMP",
            &dir.path().join("t"),
            Some("team-lifecycle"),
        ),
        None,
    )
    .await
    .unwrap();

    // The project's home replica is resolved by KEY (`for_project`), where the
    // rename, hide, and default-branch mutations all land. The id-keyed `owning_db`
    // resolver is now an O(1) prefix parse covered by the fail-closed unit tests in
    // `cairn_core::execution::routing`; this synthetic bare project id ("p-team")
    // routes Local by design, so it is not exercised through that seam here.
    let by_key = dbs.for_project("teamp").await;
    assert!(Arc::ptr_eq(&by_key, &team));
    crud::set_name_db(&by_key, "p-team", "Renamed Team")
        .await
        .unwrap();
    crud::set_hidden_db(&by_key, "p-team", true).await.unwrap();
    crud::set_default_branch_db(&by_key, "p-team", "release")
        .await
        .unwrap();

    // The schema-aware get_db reads the mutations back from the team replica.
    let row = crud::get_db(&team, "p-team").await.unwrap().unwrap();
    assert_eq!(row.name, "Renamed Team");
    assert_eq!(row.hidden, 1);
    assert_eq!(row.default_branch.as_deref(), Some("release"));

    // None of it touched the private database.
    assert_eq!(
        dbs.local
            .query_text("SELECT name FROM projects WHERE id = 'p-team'", ())
            .await
            .unwrap(),
        None,
        "a routed team mutation must not write the private database"
    );

    // The mutations sync: a second host bootstraps the renamed/hidden row.
    team.push().await.unwrap();
    let dbs_b = private_db_state(dir.path(), "private-b.db").await;
    let team_b = dbs_b
        .open_team(TeamConfig {
            team_id: "team-lifecycle".to_string(),
            team_name: "Team".to_string(),
            sync_url: url.to_string(),
            auth_token: None,
            replica_path: dir.path().join("team-b.db"),
        })
        .await
        .unwrap();
    let synced = crud::get_db(&team_b, "p-team").await.unwrap().unwrap();
    assert_eq!(synced.name, "Renamed Team");
    assert_eq!(synced.hidden, 1);
    assert_eq!(synced.default_branch.as_deref(), Some("release"));
}

/// Build a test `Orchestrator` over an existing multi-DB `DbState` (one that has
/// a team replica open and a project routed to it), so the issue-content WRITE
/// handlers run against the real router rather than a single private DB.
fn orchestrator_over(dbs: Arc<DbState>, config_dir: &Path) -> Orchestrator {
    let services = Arc::new(TestServicesBuilder::new().build());
    Orchestrator::builder(dbs, services, config_dir.to_path_buf()).build()
}

/// A user-driven (no run) MCP request: `run_id = None` makes the handlers treat
/// the caller as an external user, exercising the in-scope content path without
/// the job/run-keyed execution side effects that stay private (CAIRN-2181).
fn external_request(cwd: &Path) -> McpCallbackRequest {
    McpCallbackRequest {
        thread_id: None,
        cwd: cwd.to_string_lossy().to_string(),
        run_id: None,
        tool: "write".to_string(),
        payload: serde_json::Value::Null,
        tool_use_id: None,
    }
}

/// CAIRN-2181 headline: the ISSUE-content WRITE handlers route to the owning team
/// replica, not the private DB. Driving the ACTUAL handlers
/// (`create_issue_in_project` / `append_issue_comment` /
/// `append_project_or_issue_message`) rather than a raw INSERT proves the routing
/// fix the live `project not found` bug would otherwise reproduce: each row lands
/// in the team replica, is ABSENT from the private DB, and syncs to a second
/// replica.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn issue_content_handlers_route_writes_to_team_db_and_sync() {
    if common::skip_if_fenced("issue_content_handlers_route_writes_to_team_db_and_sync") {
        return;
    }
    let Some(server) = SyncServer::locate_or_spawn() else {
        eprintln!("skipping issue_content_handlers_route_...: no sync server");
        return;
    };
    let url = server.url();
    let dir = tempdir().unwrap();

    // Host A: a private DbState with a team replica open and a project routed to it.
    let dbs_a = private_db_state(dir.path(), "private-a.db").await;
    let replica_a = dir.path().join("team-a.db");
    seed_team_registry(&dbs_a, "team-1", url, &replica_a).await;
    let team_a = dbs_a
        .open_team(TeamConfig {
            team_id: "team-1".to_string(),
            team_name: "Team".to_string(),
            sync_url: url.to_string(),
            auth_token: None,
            replica_path: replica_a,
        })
        .await
        .expect("open team replica");

    let (_p, target) = crud::create_routed(
        &dbs_a,
        &RealClock,
        create_project_input("p-team", "TEAMP", &dir.path().join("repo"), Some("team-1")),
        None,
    )
    .await
    .expect("create routed team project");
    assert!(Arc::ptr_eq(&target, &team_a));

    let orch = orchestrator_over(dbs_a.clone(), &dir.path().join("config"));
    let request = external_request(&dir.path().join("repo"));

    // 1. CREATE through the routed handler — the regression the live bug hit.
    let outcome = issues::create_issue_in_project(
        &orch,
        "TEAMP",
        "Routed issue".to_string(),
        Some("body".to_string()),
        None,
        None,
        None,
        None,
    )
    .await
    .expect("create issue must succeed (no `project not found`)");
    let number = outcome.number;
    let issue_id = outcome.issue_id.clone();

    assert_eq!(
        team_a
            .query_text(
                "SELECT title FROM issues WHERE id = ?1",
                (issue_id.clone(),)
            )
            .await
            .unwrap()
            .as_deref(),
        Some("Routed issue"),
        "the issue row must land in the team replica"
    );
    assert_eq!(
        dbs_a
            .local
            .query_text(
                "SELECT title FROM issues WHERE id = ?1",
                (issue_id.clone(),)
            )
            .await
            .unwrap(),
        None,
        "the issue row must NOT touch the private database (the live regression)"
    );

    // 1b. Routed READS resolve too. `get` and `list` both hydrate labels via the
    // `issue_labels JOIN labels` that failed `no such table: labels` against the
    // team replica before it gained the (empty) `labels` table (CAIRN-2186).
    // They must SUCCEED and resolve the issue with NO labels (team-scoped label
    // management is deferred), proving the routed read surface is schema-complete.
    let got = issue_crud::get(&team_a, &issue_id)
        .await
        .expect("routed get must resolve without `no such table: labels`")
        .expect("the created team issue must be found in its replica");
    assert_eq!(got.title, "Routed issue");
    assert!(
        got.labels.is_empty(),
        "a team issue resolves with empty labels (team label management is deferred)"
    );
    let listed = issue_crud::list(&team_a, "p-team")
        .await
        .expect("routed list must resolve without `no such table: labels`");
    assert!(
        listed
            .iter()
            .any(|issue| issue.id == issue_id && issue.labels.is_empty()),
        "the team issue appears in the routed list with empty labels"
    );

    // 2. COMMENT append through the routed handler.
    comments_artifacts::append_issue_comment(&orch, &request, "TEAMP", number, "a routed comment")
        .await
        .expect("append comment");
    assert_eq!(
        team_a
            .query_text(
                "SELECT CAST(COUNT(*) AS TEXT) FROM comments WHERE content = 'a routed comment'",
                (),
            )
            .await
            .unwrap()
            .as_deref(),
        Some("1"),
        "the comment row must land in the team replica"
    );
    assert_eq!(
        dbs_a
            .local
            .query_text(
                "SELECT id FROM comments WHERE content = 'a routed comment'",
                ()
            )
            .await
            .unwrap(),
        None,
        "the comment row must NOT touch the private database"
    );

    // 3. MESSAGE append through the routed handler.
    messages::append_project_or_issue_message(
        &orch,
        &request,
        "TEAMP",
        Some(number),
        "a routed message",
    )
    .await
    .expect("append message");
    assert_eq!(
        team_a
            .query_text(
                "SELECT CAST(COUNT(*) AS TEXT) FROM messages WHERE content = 'a routed message'",
                (),
            )
            .await
            .unwrap()
            .as_deref(),
        Some("1"),
        "the message row must land in the team replica"
    );
    assert_eq!(
        dbs_a
            .local
            .query_text(
                "SELECT id FROM messages WHERE content = 'a routed message'",
                ()
            )
            .await
            .unwrap(),
        None,
        "the message row must NOT touch the private database"
    );

    // Sync proof: push, then a second host bootstraps all three routed rows.
    team_a.push().await.unwrap();
    let dbs_b = private_db_state(dir.path(), "private-b.db").await;
    let team_b = dbs_b
        .open_team(TeamConfig {
            team_id: "team-1".to_string(),
            team_name: "Team".to_string(),
            sync_url: url.to_string(),
            auth_token: None,
            replica_path: dir.path().join("team-b.db"),
        })
        .await
        .expect("second host opens the team");
    assert_eq!(
        team_b
            .query_text(
                "SELECT title FROM issues WHERE id = ?1",
                (issue_id.clone(),)
            )
            .await
            .unwrap()
            .as_deref(),
        Some("Routed issue"),
        "the routed issue syncs to a second replica"
    );
    assert_eq!(
        team_b
            .query_text(
                "SELECT CAST(COUNT(*) AS TEXT) FROM comments WHERE content = 'a routed comment'",
                (),
            )
            .await
            .unwrap()
            .as_deref(),
        Some("1"),
        "the routed comment syncs to a second replica"
    );
    assert_eq!(
        team_b
            .query_text(
                "SELECT CAST(COUNT(*) AS TEXT) FROM messages WHERE content = 'a routed message'",
                (),
            )
            .await
            .unwrap()
            .as_deref(),
        Some("1"),
        "the routed message syncs to a second replica"
    );
}

/// Seed a minimal execution skeleton (issue + execution + job + run) for project
/// `p-team` into `db`. Mirrors the rows a real routed start creates, so the
/// CAIRN-2182 resolvers have an `executions`/`jobs`/`runs` row to resolve.
async fn seed_execution_skeleton(db: &LocalDb, proj: &str, exec: &str, job: &str, run: &str) {
    let proj = proj.to_string();
    let exec = exec.to_string();
    let job = job.to_string();
    let run = run.to_string();
    db.write(move |conn| {
        let proj = proj.clone();
        let exec = exec.clone();
        let job = job.clone();
        let run = run.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
                 VALUES ('issue-1', ?1, 1, 'Team run', 'active', 1, 1)",
                (proj.as_str(),),
            )
            .await?;
            conn.execute(
                "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq, triggered_by)
                 VALUES (?1, 'recipe-1', 'issue-1', ?2, 'running', 1, 1, 'manual')",
                (exec.as_str(), proj.as_str()),
            )
            .await?;
            conn.execute(
                "INSERT INTO jobs(id, execution_id, agent_config_id, issue_id, project_id, node_name, status, uri_segment, created_at, updated_at)
                 VALUES (?1, ?2, 'agent-1', 'issue-1', ?3, 'builder', 'running', 'builder', 1, 1)",
                (job.as_str(), exec.as_str(), proj.as_str()),
            )
            .await?;
            conn.execute(
                "INSERT INTO runs(id, project_id, issue_id, job_id, status, created_at, updated_at, start_mode)
                 VALUES (?1, ?2, 'issue-1', ?3, 'live', 1, 1, 'resume')",
                (run.as_str(), proj.as_str(), job.as_str()),
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

/// Insert a live `full` transcript event into `db`. `data` MUST be valid JSON:
/// the team `events` trigger's WHEN clause runs `json_extract(NEW.data, '$.content')`
/// on every insert, which raises `malformed JSON` on non-JSON text and fails the
/// INSERT itself.
async fn insert_full_event(db: &LocalDb, run: &str, content: &str) {
    let run = run.to_string();
    let content = content.to_string();
    db.write(move |conn| {
        let run = run.clone();
        let content = content.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO events (
                    id, run_id, session_id, sequence, timestamp, event_type, data,
                    parent_tool_use_id, created_at, input_tokens, cache_read_tokens,
                    cache_create_tokens, output_tokens, turn_id
                 ) VALUES ('ev-1', ?1, NULL, 0, 1, 'assistant', ?2, NULL, 1, NULL, NULL, NULL, NULL, NULL)",
                (run.as_str(), content.as_str()),
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

/// CAIRN-2182 headline: a routed EXECUTION's skeleton + transcript rows land in
/// the team replica, NOT the private DB, and sync to a second host. The
/// execution analogue of `issue_content_handlers_route_writes_to_team_db_and_sync`:
/// it proves the fail-closed `owning_db_for_*` resolvers route every
/// execution-entangled id to the owning replica, and that a LIVE run's `full`
/// events (content inline) reconstruct from the replica alone on a teammate's
/// host (a full event is self-contained, so reconstruction is identity).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn routed_execution_writes_hit_the_team_db_and_sync() {
    if common::skip_if_fenced("routed_execution_writes_hit_the_team_db_and_sync") {
        return;
    }
    let Some(server) = SyncServer::locate_or_spawn() else {
        eprintln!("skipping routed_execution_writes_...: no sync server");
        return;
    };
    let url = server.url();
    let dir = tempdir().unwrap();

    // Guard-valid (alphanumeric) team id, used both as the open/registry key and
    // as the routable id prefix so routing_db_for_id parses back to this team.
    let team = "teamexec";
    let proj = "teamexec~00000000-0000-4000-8000-000000000001";
    let exec = "teamexec~00000000-0000-4000-8000-000000000002";
    let job = "teamexec~00000000-0000-4000-8000-000000000003";
    let run = "teamexec~00000000-0000-4000-8000-000000000004";
    let dbs_a = private_db_state(dir.path(), "private-a.db").await;
    let replica_a = dir.path().join("team-a.db");
    seed_team_registry(&dbs_a, team, url, &replica_a).await;
    let team_a = dbs_a
        .open_team(TeamConfig {
            team_id: team.to_string(),
            team_name: "Team".to_string(),
            sync_url: url.to_string(),
            auth_token: None,
            replica_path: replica_a,
        })
        .await
        .expect("open team replica");
    let (_p, target) = crud::create_routed(
        &dbs_a,
        &RealClock,
        create_project_input(proj, "TEAMP", &dir.path().join("repo"), Some(team)),
        None,
    )
    .await
    .expect("create routed team project");
    assert!(Arc::ptr_eq(&target, &team_a));

    seed_execution_skeleton(&team_a, proj, exec, job, run).await;

    // The fail-closed resolvers route every execution id to the OWNING replica.
    use cairn_core::internal::execution::routing;
    assert!(Arc::ptr_eq(
        &routing::owning_db_for_project(&dbs_a, proj).await.unwrap(),
        &team_a
    ));
    assert!(Arc::ptr_eq(
        &routing::owning_db_for_execution(&dbs_a, exec)
            .await
            .unwrap(),
        &team_a
    ));
    assert!(Arc::ptr_eq(
        &routing::owning_db_for_job(&dbs_a, job).await.unwrap(),
        &team_a
    ));
    let run_owner = routing::owning_db_for_run(&dbs_a, run).await.unwrap();
    assert!(Arc::ptr_eq(&run_owner, &team_a));

    // A live transcript `full` event written through the RESOLVED handle lands in
    // the replica with its content inline (self-contained for reconstruction).
    // `data` is the serialized event — valid JSON, as the team `events` trigger's
    // `json_extract(NEW.data, '$.content')` requires on every insert.
    let event_data = r#"{"content":"hello from the team run"}"#;
    insert_full_event(&run_owner, run, event_data).await;

    // None of the execution rows touched the private DB (the split-brain the
    // fail-closed routing exists to prevent).
    for (table, id) in [
        ("executions", exec),
        ("jobs", job),
        ("runs", run),
        ("events", "ev-1"),
    ] {
        assert_eq!(
            dbs_a
                .local
                .query_text(
                    format!("SELECT id FROM {table} WHERE id = ?1"),
                    (id.to_string(),)
                )
                .await
                .unwrap(),
            None,
            "the {table} row must NOT touch the private database"
        );
    }

    // Push, then a second host bootstraps the full skeleton AND the live event
    // with its inline content — a teammate reconstructs the live run from rows
    // alone (no worktree, no content store).
    team_a.push().await.unwrap();
    let dbs_b = private_db_state(dir.path(), "private-b.db").await;
    let team_b = dbs_b
        .open_team(TeamConfig {
            team_id: team.to_string(),
            team_name: "Team".to_string(),
            sync_url: url.to_string(),
            auth_token: None,
            replica_path: dir.path().join("team-b.db"),
        })
        .await
        .expect("second host opens the team");
    for (table, id) in [
        ("executions", exec),
        ("jobs", job),
        ("runs", run),
        ("events", "ev-1"),
    ] {
        assert_eq!(
            team_b
                .query_text(
                    format!("SELECT id FROM {table} WHERE id = ?1"),
                    (id.to_string(),)
                )
                .await
                .unwrap()
                .as_deref(),
            Some(id),
            "the routed {table} row must sync to a second replica"
        );
    }
    assert_eq!(
        team_b
            .query_text("SELECT data FROM events WHERE id = 'ev-1'", ())
            .await
            .unwrap()
            .as_deref(),
        Some(event_data),
        "the live full event's inline content reconstructs from the replica alone"
    );
}

/// CAIRN-2182 fail-closed regression: when a project is team-routed but the
/// replica is NOT open, the execution resolvers ERROR rather than silently
/// falling back to the private database (the CAIRN-2170 silent-non-propagation
/// class). A team run's writes must never land in private.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fail_closed_execution_routing_when_team_replica_not_open() {
    if common::skip_if_fenced("fail_closed_execution_routing_when_team_replica_not_open") {
        return;
    }
    let Some(server) = SyncServer::locate_or_spawn() else {
        eprintln!("skipping fail_closed_execution_routing_...: no sync server");
        return;
    };
    let url = server.url();
    let dir = tempdir().unwrap();

    // Host A opens the team and creates the routed project + execution skeleton.
    let team = "teamfailclosed";
    let proj = "teamfailclosed~00000000-0000-4000-8000-000000000001";
    let exec = "teamfailclosed~00000000-0000-4000-8000-000000000002";
    let job = "teamfailclosed~00000000-0000-4000-8000-000000000003";
    let run = "teamfailclosed~00000000-0000-4000-8000-000000000004";
    let dbs_a = private_db_state(dir.path(), "private-a.db").await;
    let replica_a = dir.path().join("team-a.db");
    seed_team_registry(&dbs_a, team, url, &replica_a).await;
    let team_a = dbs_a
        .open_team(TeamConfig {
            team_id: team.to_string(),
            team_name: "Team".to_string(),
            sync_url: url.to_string(),
            auth_token: None,
            replica_path: replica_a,
        })
        .await
        .expect("open team replica");
    crud::create_routed(
        &dbs_a,
        &RealClock,
        create_project_input(proj, "TEAMP", &dir.path().join("repo"), Some(team)),
        None,
    )
    .await
    .expect("create routed team project");
    seed_execution_skeleton(&team_a, proj, exec, job, run).await;

    // Host C knows the project is team-routed but has NOT opened the replica, so
    // its only open database is private — which carries none of these rows.
    let dbs_c = private_db_state(dir.path(), "private-c.db").await;
    dbs_c.set_route("TEAMP", Some(team.to_string())).await;

    use cairn_core::internal::execution::routing;
    assert!(
        routing::owning_db_for_project(&dbs_c, proj).await.is_err(),
        "a team project with a closed replica must NOT resolve to private"
    );
    assert!(routing::owning_db_for_execution(&dbs_c, exec)
        .await
        .is_err());
    assert!(routing::owning_db_for_job(&dbs_c, job).await.is_err());
    assert!(routing::owning_db_for_run(&dbs_c, run).await.is_err());

    for (table, id) in [("executions", exec), ("jobs", job), ("runs", run)] {
        assert_eq!(
            dbs_c
                .local
                .query_text(
                    format!("SELECT id FROM {table} WHERE id = ?1"),
                    (id.to_string(),)
                )
                .await
                .unwrap(),
            None,
            "fail-closed routing must leave no {table} row in the private database"
        );
    }
}

/// Give the seeded `run-1` a session and two transcript events keyed to it, plus
/// a pending permission request — the exact rows a live team run accumulates that
/// the desktop chat reads back. `data` MUST be valid JSON (the team `events`
/// trigger runs `json_extract(NEW.data, '$.content')` on every insert).
async fn seed_session_events_and_permission(
    db: &LocalDb,
    run: &str,
    ev1: &str,
    ev2: &str,
    perm: &str,
) {
    let run = run.to_string();
    let ev1 = ev1.to_string();
    let ev2 = ev2.to_string();
    let perm = perm.to_string();
    db.write(move |conn| {
        let run = run.clone();
        let ev1 = ev1.clone();
        let ev2 = ev2.clone();
        let perm = perm.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE runs SET session_id = 'sess-1' WHERE id = ?1",
                (run.as_str(),),
            )
            .await?;
            for (id, seq) in [(ev1.as_str(), 0), (ev2.as_str(), 1)] {
                conn.execute(
                    "INSERT INTO events (
                        id, run_id, session_id, sequence, timestamp, event_type, data,
                        parent_tool_use_id, created_at, input_tokens, cache_read_tokens,
                        cache_create_tokens, output_tokens, turn_id
                     ) VALUES (?1, ?3, 'sess-1', ?2, 1, 'assistant', '{\"content\":\"hi\"}', NULL, ?2, NULL, NULL, NULL, NULL, NULL)",
                    (id, seq, run.as_str()),
                )
                .await?;
            }
            conn.execute(
                "INSERT INTO permission_requests (id, run_id, tool_use_id, tool_name, tool_input, status, created_at)
                 VALUES (?1, ?2, 'tu-1', 'bash', '{}', 'pending', 1)",
                (perm.as_str(), run.as_str()),
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

/// Seed a run + one event living ONLY in the private database, to prove the
/// resolvers short-circuit on the always-open private DB for a local execution
/// (a strict no-op — byte-for-byte the prior `&db.local` behavior).
async fn seed_local_run_with_events(db: &LocalDb) {
    db.write(move |conn| {
        Box::pin(async move {
            conn.execute(
                "INSERT INTO runs (id, status, session_id, created_at, updated_at)
                 VALUES ('run-local', 'live', 'sess-local', 1, 1)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO events (
                    id, run_id, session_id, sequence, timestamp, event_type, data,
                    parent_tool_use_id, created_at, input_tokens, cache_read_tokens,
                    cache_create_tokens, output_tokens, turn_id
                 ) VALUES ('ev-local', 'run-local', 'sess-local', 0, 1, 'assistant', '{}', NULL, 1, NULL, NULL, NULL, NULL, NULL)",
                (),
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

/// CAIRN-2225 headline: the desktop run/event/transcript READ surface resolves a
/// team execution's rows to the OWNING replica, driving the REAL cairn-core read
/// queries the Tauri commands delegate to (`list_events` /
/// `list_events_for_session_delta`). This is the symmetric counterpart to
/// `routed_execution_writes_hit_the_team_db_and_sync` (the WRITE side,
/// CAIRN-2182): a team run's `runs`/`events`/`permission_requests` live wholly in
/// the replica, so reading them from the private DB returns empty — the live
/// loading-dots / approval-gate-never-surfaces failure. It also pins the
/// session-keyed resolution (`runs.session_id`) the chat-delta hook depends on,
/// and the local-only no-op proving a private-DB run resolves to `self.local`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn routed_run_reads_resolve_to_team_replica_and_local_to_private() {
    if common::skip_if_fenced("routed_run_reads_resolve_to_team_replica_and_local_to_private") {
        return;
    }
    let Some(server) = SyncServer::locate_or_spawn() else {
        eprintln!("skipping routed_run_reads_resolve_...: no sync server");
        return;
    };
    let url = server.url();
    let dir = tempdir().unwrap();

    // Guard-valid team id used as the registry/open key AND the routable id
    // prefix so routing_db_for_id parses every id back to this team.
    let team = "teamreads";
    let proj = "teamreads~00000000-0000-4000-8000-000000000001";
    let exec = "teamreads~00000000-0000-4000-8000-000000000002";
    let job = "teamreads~00000000-0000-4000-8000-000000000003";
    let run = "teamreads~00000000-0000-4000-8000-000000000004";
    let ev1 = "teamreads~00000000-0000-4000-8000-000000000005";
    let ev2 = "teamreads~00000000-0000-4000-8000-000000000006";
    let perm = "teamreads~00000000-0000-4000-8000-000000000007";

    let dbs_a = private_db_state(dir.path(), "private-a.db").await;
    let replica_a = dir.path().join("team-a.db");
    seed_team_registry(&dbs_a, team, url, &replica_a).await;
    let team_a = dbs_a
        .open_team(TeamConfig {
            team_id: team.to_string(),
            team_name: "Team".to_string(),
            sync_url: url.to_string(),
            auth_token: None,
            replica_path: replica_a,
        })
        .await
        .expect("open team replica");
    crud::create_routed(
        &dbs_a,
        &RealClock,
        create_project_input(proj, "TEAMP", &dir.path().join("repo"), Some(team)),
        None,
    )
    .await
    .expect("create routed team project");

    seed_execution_skeleton(&team_a, proj, exec, job, run).await;
    seed_session_events_and_permission(&team_a, run, ev1, ev2, perm).await;

    use cairn_core::internal::execution::routing;
    use cairn_core::runs::queries;

    // Run-keyed read: `list_events` resolves to the replica and returns its events.
    let run_owner = routing::owning_db_for_run(&dbs_a, run).await.unwrap();
    assert!(Arc::ptr_eq(&run_owner, &team_a));
    let events = queries::list_events(run_owner.clone(), run).unwrap();
    assert_eq!(
        events.len(),
        2,
        "list_events returns the team run's events from the replica"
    );

    // Session-keyed read: `owning_db_for_session` resolves via `runs.session_id`,
    // and `list_events_for_session_delta` (the chat-delta hook's query) returns
    // the session's events from the replica.
    let session_owner = routing::owning_db_for_session(&dbs_a, "sess-1")
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&session_owner, &team_a));
    let delta =
        queries::list_events_for_session_delta(session_owner.clone(), "sess-1", None).unwrap();
    assert_eq!(
        delta.events.len(),
        2,
        "the session delta returns the team session's events from the replica"
    );

    // Event-keyed read resolves to the replica too.
    let event_owner = routing::owning_db_for_event(&dbs_a, ev1).await.unwrap();
    assert!(Arc::ptr_eq(&event_owner, &team_a));

    // Pending-permission read (the approval gate): the row lives in the replica,
    // resolved by the run, and is ABSENT from the private DB — the private read
    // that left the gate unsurfaced and the run unable to advance.
    assert_eq!(
        run_owner
            .query_text(
                "SELECT id FROM permission_requests WHERE run_id = ?1 AND status = 'pending'",
                (run,),
            )
            .await
            .unwrap()
            .as_deref(),
        Some(perm),
        "the pending permission request reads from the run's owning replica"
    );
    for (table, id) in [
        ("runs", run),
        ("events", ev1),
        ("permission_requests", perm),
    ] {
        assert_eq!(
            dbs_a
                .local
                .query_text(
                    format!("SELECT id FROM {table} WHERE id = ?1"),
                    (id.to_string(),)
                )
                .await
                .unwrap(),
            None,
            "the team {table} row must NOT exist in the private DB (the failing private read)"
        );
    }

    // Local no-op: a run/session that lives ONLY in the private DB resolves to
    // `self.local` and reads back from private — byte-for-byte the prior behavior.
    seed_local_run_with_events(&dbs_a.local).await;
    let local_run_owner = routing::owning_db_for_run(&dbs_a, "run-local")
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&local_run_owner, &dbs_a.local));
    let local_session_owner = routing::owning_db_for_session(&dbs_a, "sess-local")
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&local_session_owner, &dbs_a.local));
    let local_events = queries::list_events(local_run_owner, "run-local").unwrap();
    assert_eq!(
        local_events.len(),
        1,
        "a local run's events read back from the private DB unchanged"
    );
}

/// Give the seeded team `run-1` a session, a terminal predecessor turn, and a
/// pending permission request keyed to it — the exact shape a live team
/// execution parks at an approval gate, all FK'd into the replica.
async fn seed_team_run_turn_and_permission(db: &LocalDb, run: &str, job: &str, perm: &str) {
    let run = run.to_string();
    let job = job.to_string();
    let perm = perm.to_string();
    db.write(move |conn| {
        let run = run.clone();
        let job = job.clone();
        let perm = perm.clone();
        Box::pin(async move {
            conn.execute("UPDATE runs SET session_id = 'sess-1' WHERE id = ?1", (run.as_str(),))
                .await?;
            conn.execute(
                "INSERT INTO turns (id, session_id, run_id, job_id, sequence, predecessor_id, state, start_reason, created_at, updated_at)
                 VALUES ('turn-pred', 'sess-1', ?1, ?2, 1, NULL, 'yielded', 'initial', 1, 1)",
                (run.as_str(), job.as_str()),
            )
            .await?;
            conn.execute(
                "INSERT INTO permission_requests (id, run_id, job_id, tool_use_id, tool_name, tool_input, status, turn_id, created_at)
                 VALUES (?1, ?2, ?3, 'tu-1', 'bash', '{}', 'pending', 'turn-pred', 1)",
                (perm.as_str(), run.as_str(), job.as_str()),
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

/// Seed a run + a pending permission request living ONLY in the private DB, to
/// prove the response resolver short-circuits on the always-open private DB for
/// a local execution (a strict no-op — byte-for-byte the prior `&db.local`
/// behavior).
async fn seed_local_run_and_permission(db: &LocalDb) {
    db.write(move |conn| {
        Box::pin(async move {
            conn.execute(
                "INSERT INTO runs (id, status, session_id, created_at, updated_at)
                 VALUES ('run-local', 'live', 'sess-local', 1, 1)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO permission_requests (id, run_id, tool_use_id, tool_name, tool_input, status, created_at)
                 VALUES ('perm-local', 'run-local', 'tu-l', 'bash', '{}', 'pending', 1)",
                (),
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

/// CAIRN-2227 headline: the permission RESPONSE write path resolves a team
/// execution's request to the OWNING replica, driving the REAL
/// `resolve_permission_request` the desktop `respond_to_permission` command
/// delegates to. This is the WRITE counterpart to
/// `routed_run_reads_resolve_to_team_replica_and_local_to_private` (CAIRN-2225's
/// read side): a team run's `permission_requests`/`turns`/`runs` rows live wholly
/// in the replica, so answering against the private DB errors `Permission request
/// not found` and the run never resumes. The fix records the response, marks the
/// run for resume (a successor turn), and recomputes issue status all in the
/// replica, with NOTHING leaking to private. The local-only no-op proves a
/// private-DB request resolves to `self.local` and is answered there unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn routed_permission_response_resolves_to_team_replica_and_local_to_private() {
    if common::skip_if_fenced(
        "routed_permission_response_resolves_to_team_replica_and_local_to_private",
    ) {
        return;
    }
    let Some(server) = SyncServer::locate_or_spawn() else {
        eprintln!("skipping routed_permission_response_resolve_...: no sync server");
        return;
    };
    let url = server.url();
    let dir = tempdir().unwrap();

    // Guard-valid team id used as the registry/open key AND the routable id
    // prefix so routing_db_for_id parses every id back to this team.
    let team = "teampermresp";
    let proj = "teampermresp~00000000-0000-4000-8000-000000000001";
    let exec = "teampermresp~00000000-0000-4000-8000-000000000002";
    let job = "teampermresp~00000000-0000-4000-8000-000000000003";
    let run = "teampermresp~00000000-0000-4000-8000-000000000004";
    let perm = "teampermresp~00000000-0000-4000-8000-000000000005";

    let dbs_a = private_db_state(dir.path(), "private-a.db").await;
    let replica_a = dir.path().join("team-a.db");
    seed_team_registry(&dbs_a, team, url, &replica_a).await;
    let team_a = dbs_a
        .open_team(TeamConfig {
            team_id: team.to_string(),
            team_name: "Team".to_string(),
            sync_url: url.to_string(),
            auth_token: None,
            replica_path: replica_a,
        })
        .await
        .expect("open team replica");
    crud::create_routed(
        &dbs_a,
        &RealClock,
        create_project_input(proj, "TEAMP", &dir.path().join("repo"), Some(team)),
        None,
    )
    .await
    .expect("create routed team project");

    seed_execution_skeleton(&team_a, proj, exec, job, run).await;
    seed_team_run_turn_and_permission(&team_a, run, job, perm).await;

    use cairn_core::internal::execution::routing;
    use cairn_core::internal::mcp::handlers::permission::{
        resolve_permission_request, PermissionDecision, PermissionScope,
    };

    let orch = orchestrator_over(dbs_a.clone(), &dir.path().join("config"));

    // The request resolves to the replica, not the private DB.
    let perm_owner = routing::owning_db_for_permission_request(&dbs_a, perm)
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&perm_owner, &team_a));

    // Drive the REAL allow. continue_job_impl fails fast (the seeded job has no
    // live session to resume) and is logged, but every DB write the response path
    // makes lands BEFORE that and must hit the replica.
    resolve_permission_request(
        &orch,
        perm,
        PermissionDecision::Allow,
        PermissionScope::Once,
    )
    .await
    .expect("resolve the team permission request");

    // The response is recorded in the REPLICA.
    assert_eq!(
        team_a
            .query_text(
                "SELECT status FROM permission_requests WHERE id = ?1",
                (perm,),
            )
            .await
            .unwrap()
            .as_deref(),
        Some("allowed"),
        "the permission response is recorded in the team replica"
    );
    assert!(
        team_a
            .query_text(
                "SELECT response FROM permission_requests WHERE id = ?1",
                (perm,),
            )
            .await
            .unwrap()
            .is_some(),
        "the response JSON is stored in the replica"
    );

    // The run is marked for resume: a successor turn was created in the replica.
    assert!(
        team_a
            .query_text(
                "SELECT id FROM turns WHERE predecessor_id = 'turn-pred' AND start_reason = 'permission_response'",
                (),
            )
            .await
            .unwrap()
            .is_some(),
        "the successor turn that resumes the run lands in the replica"
    );

    // NOTHING leaks to the private DB.
    for (table, id) in [
        ("permission_requests", perm),
        ("runs", run),
        ("turns", "turn-pred"),
    ] {
        assert_eq!(
            dbs_a
                .local
                .query_text(
                    format!("SELECT id FROM {table} WHERE id = ?1"),
                    (id.to_string(),)
                )
                .await
                .unwrap(),
            None,
            "no team {table} row may leak to the private DB"
        );
    }
    assert_eq!(
        dbs_a
            .local
            .query_text(
                "SELECT id FROM turns WHERE start_reason = 'permission_response'",
                ()
            )
            .await
            .unwrap(),
        None,
        "no successor turn may leak to the private DB"
    );

    // Local no-op: a request that lives ONLY in the private DB resolves to
    // `self.local` and is answered there — byte-for-byte the prior behavior.
    seed_local_run_and_permission(&dbs_a.local).await;
    let local_owner = routing::owning_db_for_permission_request(&dbs_a, "perm-local")
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&local_owner, &dbs_a.local));
    resolve_permission_request(
        &orch,
        "perm-local",
        PermissionDecision::Allow,
        PermissionScope::Once,
    )
    .await
    .expect("resolve the local permission request");
    assert_eq!(
        dbs_a
            .local
            .query_text(
                "SELECT status FROM permission_requests WHERE id = 'perm-local'",
                (),
            )
            .await
            .unwrap()
            .as_deref(),
        Some("allowed"),
        "a local permission response is recorded in the private DB unchanged"
    );
}

/// CAIRN-2229 headline: the ask_user QUESTION flow routes to a team run's owning
/// replica end to end. This is the sibling of
/// `routed_permission_response_resolves_to_team_replica_and_local_to_private`
/// (CAIRN-2227) for prompts: `ask_questions` hardcoded `orch.db.local` for the
/// run lookup and the prompt insert, so a team run — whose `runs`/`prompts`/`issues`
/// rows live wholly in the replica — failed at the very first lookup with the live
/// `No run found with id '…~…'` error, before a prompt could ever be stored. The
/// fix resolves the owning database once (`lookup_run_routed`) and threads it
/// through the insert and every issue/job read, and routes the node-URI answer
/// path's prompt lookup by project (`for_project`).
///
/// This drives the REAL handlers the desktop/resource layer delegate to:
/// `ask_questions` (background, so it stores without yielding/blocking) then
/// `answer_node_question` (the `cairn://…/questions/q-1` resource patch path). It
/// asserts the prompt AND its answer land in the replica with NOTHING in the
/// empty private DB, plus a local-only no-op proving a bare run is answered in
/// the private DB byte-for-byte unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn routed_question_flow_resolves_to_team_replica_and_local_to_private() {
    if common::skip_if_fenced("routed_question_flow_resolves_to_team_replica_and_local_to_private")
    {
        return;
    }
    let Some(server) = SyncServer::locate_or_spawn() else {
        eprintln!("skipping routed_question_flow_resolves_...: no sync server");
        return;
    };
    let url = server.url();
    let dir = tempdir().unwrap();

    use cairn_core::internal::execution::routing;
    use cairn_core::internal::mcp::handlers::planning::{answer_node_question, ask_questions};
    use cairn_core::internal::mcp::types::{AskUserPayload, Question};
    use serde_json::json;

    // Guard-valid team id used as the registry/open key AND the routable id prefix
    // so routing_db_for_id parses every id back to this team.
    let team = "teamquestion";
    let proj = "teamquestion~00000000-0000-4000-8000-000000000001";
    let exec = "teamquestion~00000000-0000-4000-8000-000000000002";
    let job = "teamquestion~00000000-0000-4000-8000-000000000003";
    let run = "teamquestion~00000000-0000-4000-8000-000000000004";

    let dbs_a = private_db_state(dir.path(), "private-a.db").await;
    let replica_a = dir.path().join("team-a.db");
    seed_team_registry(&dbs_a, team, url, &replica_a).await;
    let team_a = dbs_a
        .open_team(TeamConfig {
            team_id: team.to_string(),
            team_name: "Team".to_string(),
            sync_url: url.to_string(),
            auth_token: None,
            replica_path: replica_a,
        })
        .await
        .expect("open team replica");
    crud::create_routed(
        &dbs_a,
        &RealClock,
        create_project_input(proj, "TEAMP", &dir.path().join("repo"), Some(team)),
        None,
    )
    .await
    .expect("create routed team project");
    seed_execution_skeleton(&team_a, proj, exec, job, run).await;

    let orch = orchestrator_over(dbs_a.clone(), &dir.path().join("config"));

    // Sanity: the run resolves to the replica, not private.
    assert!(Arc::ptr_eq(
        &routing::owning_db_for_run(&dbs_a, run).await.unwrap(),
        &team_a
    ));

    // 1. ASK: drive the REAL ask_user handler (background = store without
    //    yielding). Before the fix this returned the live `No run found` error
    //    because lookup_run hit the empty private DB.
    let request = McpCallbackRequest {
        thread_id: None,
        cwd: dir.path().join("repo").to_string_lossy().to_string(),
        run_id: Some(run.to_string()),
        tool: "write".to_string(),
        payload: serde_json::Value::Null,
        tool_use_id: None,
    };
    let payload = AskUserPayload {
        questions: vec![Question {
            question: "Proceed?".to_string(),
            header: None,
            options: vec![],
            multi_select: false,
        }],
    };
    let ask_result = ask_questions(&orch, &request, payload, true, None).await;
    assert!(
        !ask_result.contains("No run found"),
        "ask_questions must route to the replica, not fail with the live `No run found` bug: {ask_result}"
    );
    assert!(
        ask_result.contains("recorded"),
        "background ask_questions records the prompt: {ask_result}"
    );

    // The prompt row landed in the REPLICA, addressable at q-1...
    assert!(
        team_a
            .query_text(
                "SELECT id FROM prompts WHERE run_id = ?1 AND uri_segment = 'q-1'",
                (run,),
            )
            .await
            .unwrap()
            .is_some(),
        "the team prompt is stored in the replica"
    );
    // ...and NOT in the private database (the live regression).
    assert_eq!(
        dbs_a
            .local
            .query_text("SELECT id FROM prompts WHERE run_id = ?1", (run,))
            .await
            .unwrap(),
        None,
        "the team prompt must NOT touch the private DB (the live `No run found` regression)"
    );

    // 2. ANSWER: drive the REAL node-URI answer handler. lookup_prompt_for_node_question
    //    routes by project (for_project) to find the prompt in the replica, then
    //    answer_prompt_id records the response there (fail-closed by prompt id).
    let outcome = answer_node_question(
        &orch,
        "TEAMP",
        1,
        1,
        "builder",
        "q-1",
        &json!({"answer": "Proceed"}),
    )
    .await
    .expect("answer the team question via its node URI");
    assert_eq!(outcome.response, "Proceed");
    assert!(!outcome.duplicate);

    // The answer is recorded in the REPLICA.
    assert_eq!(
        team_a
            .query_text(
                "SELECT response FROM prompts WHERE run_id = ?1 AND uri_segment = 'q-1'",
                (run,),
            )
            .await
            .unwrap()
            .as_deref(),
        Some("Proceed"),
        "the answer is recorded in the team replica"
    );
    // No prompt — answered or otherwise — leaked to the private database.
    assert_eq!(
        dbs_a
            .local
            .query_text("SELECT id FROM prompts", ())
            .await
            .unwrap(),
        None,
        "no prompt may leak to the private DB"
    );

    // Local no-op: a bare-id run + project living ONLY in the private DB asks and
    // answers a question entirely in private — byte-for-byte the prior behavior.
    crud::create_routed(
        &dbs_a,
        &RealClock,
        create_project_input("p-local", "LOCALP", &dir.path().join("l"), None),
        None,
    )
    .await
    .expect("create local project");
    seed_execution_skeleton(
        &dbs_a.local,
        "p-local",
        "exec-local",
        "job-local",
        "run-local-q",
    )
    .await;
    let local_request = McpCallbackRequest {
        thread_id: None,
        cwd: dir.path().join("l").to_string_lossy().to_string(),
        run_id: Some("run-local-q".to_string()),
        tool: "write".to_string(),
        payload: serde_json::Value::Null,
        tool_use_id: None,
    };
    let local_payload = AskUserPayload {
        questions: vec![Question {
            question: "Local?".to_string(),
            header: None,
            options: vec![],
            multi_select: false,
        }],
    };
    assert!(Arc::ptr_eq(
        &routing::owning_db_for_run(&dbs_a, "run-local-q")
            .await
            .unwrap(),
        &dbs_a.local
    ));
    let local_ask = ask_questions(&orch, &local_request, local_payload, true, None).await;
    assert!(
        local_ask.contains("recorded"),
        "a local background ask records the prompt in the private DB: {local_ask}"
    );
    answer_node_question(
        &orch,
        "LOCALP",
        1,
        1,
        "builder",
        "q-1",
        &json!({"answer": "Local"}),
    )
    .await
    .expect("answer the local question");
    assert_eq!(
        dbs_a
            .local
            .query_text(
                "SELECT response FROM prompts WHERE run_id = 'run-local-q' AND uri_segment = 'q-1'",
                (),
            )
            .await
            .unwrap()
            .as_deref(),
        Some("Local"),
        "a local question is asked and answered in the private DB unchanged"
    );
}
