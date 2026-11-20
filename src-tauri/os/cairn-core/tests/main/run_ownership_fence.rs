//! CAIRN-3287: the tool-serving path is fenced on run ownership.
//!
//! An agent process outlives the runner that spawned it, and the runner's
//! transcript reader does not: it is an in-process thread over the agent's
//! stdout. In the specimen incident a successor runner served a predecessor's
//! orphaned agent for 38 minutes — shell batches ran, files changed, and not one
//! event was persisted. [`dispatch_tool`] therefore refuses a tool call whose run
//! this runner process does not own.
//!
//! Every refusal test asserts the ABSENCE OF THE EFFECT, not just the presence of
//! refusal text: a fence that refuses politely and then executes anyway would pass
//! a text-only assertion while leaving the defect exactly where it was. The effect
//! used here is an issue comment — a real mutation the `write` handler performs
//! and a test can count — reached with no worktree, executor, or jj store, so the
//! fence is exercised hermetically at the one choke point above every handler.

use crate::common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use cairn_core::internal::agent_process::process::RunHandle;
use cairn_core::internal::dispatch::dispatch_tool;
use cairn_core::internal::mcp::types::McpCallbackRequest;
use cairn_core::internal::orchestrator::Orchestrator;
use cairn_core::internal::storage::LocalDb;
use cairn_db::turso::params;

/// Wall-clock stand-in for "this runner booted". Fixture runs are placed on
/// either side of it in whole seconds, which is the granularity `runs` stores.
const BOOT_AT: i64 = 1_000_000;

const RUN_ID: &str = "run-fenced";
const COMMENT: &str = "a comment only a served write could have made";

/// One run whose ownership coordinates the caller chooses, plus the issue its
/// tool call would comment on.
async fn fixture(
    started_at: Option<i64>,
    created_at: i64,
    status: &str,
) -> (tempfile::TempDir, Arc<LocalDb>, Orchestrator) {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed_run(&db, started_at, created_at, status).await;
    let orch = common::orchestrator_booted_at(&temp, db.clone(), BOOT_AT);
    (temp, db, orch)
}

async fn seed_run(db: &LocalDb, started_at: Option<i64>, created_at: i64, status: &str) {
    seed_run_as(db, RUN_ID, started_at, created_at, status).await;
}

async fn seed_run_as(
    db: &LocalDb,
    run_id: &str,
    started_at: Option<i64>,
    created_at: i64,
    status: &str,
) {
    let status = status.to_string();
    let run_id = run_id.to_string();
    db.write(move |conn| {
        let status = status.clone();
        let run_id = run_id.clone();
        Box::pin(async move {
            for sql in [
                "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
                 VALUES ('proj-1', 'default', 'Test Project', 'fence', '/tmp/test-repo', 1, 1)",
                "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
                 VALUES ('issue-1', 'proj-1', 1, 'fenced issue', 'active', 1, 1)",
                "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
                 VALUES ('exec-1', 'recipe-default', 'issue-1', 'proj-1', 'running', 1, 1)",
                "INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, node_name,
                                   uri_segment, status, current_session_id, created_at, updated_at)
                 VALUES ('job-1', 'exec-1', 'builder', 'issue-1', 'proj-1', 'Builder',
                         'builder', 'running', 'session-1', 1, 1)",
                "INSERT INTO sessions (id, job_id, status, created_at, updated_at)
                 VALUES ('session-1', 'job-1', 'active', 1, 1)",
            ] {
                conn.execute(sql, ()).await?;
            }
            conn.execute(
                "INSERT INTO runs (id, project_id, issue_id, job_id, status, session_id,
                                   started_at, created_at, updated_at, start_mode)
                 VALUES (?1, 'proj-1', 'issue-1', 'job-1', ?2, 'session-1', ?3, ?4, ?4, 'fresh')",
                params![run_id.as_str(), status.as_str(), started_at, created_at],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

/// The tool call under test: a `write` that appends a comment to the fixture's
/// issue. Dispatched, it leaves a row; fenced, it must leave nothing.
fn commenting_write(run_id: &str) -> McpCallbackRequest {
    McpCallbackRequest {
        answer_bound_ms: None,
        thread_id: None,
        cwd: "/tmp".to_string(),
        run_id: Some(run_id.to_string()),
        tool: "write".to_string(),
        payload: serde_json::json!({
            "changes": [{
                "target": "cairn://p/fence/1",
                "mode": "append",
                "payload": { "content": COMMENT },
            }]
        }),
        tool_use_id: None,
        origin: None,
    }
}

async fn dispatch(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let cursors = Mutex::new(HashMap::new());
    dispatch_tool(orch, request, &cursors)
        .await
        .into_inner()
        .content
}

async fn comment_count(db: &LocalDb) -> i64 {
    common::query_i64(db, "SELECT COUNT(*) FROM comments")
        .await
        .unwrap()
}

fn assert_refused(result: &str) {
    assert!(
        result.contains("no longer served") && result.contains("Stop here"),
        "expected the ownership refusal, got: {result}"
    );
}

fn assert_erased(result: &str) {
    assert_refused(result);
    assert!(
        result.contains("record no longer exists"),
        "expected the erased-run refusal, got: {result}"
    );
}

/// A `read` of an absolute file, as the agent's own `read` tool sends it.
fn file_read(run_id: &str, path: &std::path::Path) -> McpCallbackRequest {
    McpCallbackRequest {
        answer_bound_ms: None,
        thread_id: None,
        cwd: "/tmp".to_string(),
        run_id: Some(run_id.to_string()),
        tool: "read".to_string(),
        payload: serde_json::json!({ "path": format!("file:{}", path.display()) }),
        tool_use_id: None,
        origin: None,
    }
}

fn register_process(orch: &Orchestrator, run_id: &str) {
    let handle = RunHandle::new(
        Arc::new(Mutex::new(None)),
        Arc::new(Mutex::new(None)),
        Some("session-1".to_string()),
        Some("job-1".to_string()),
    );
    orch.process_state
        .processes
        .lock()
        .unwrap()
        .register(run_id.to_string(), handle);
}

/// The incident, reduced: a run spawned before this runner booted asks for a
/// mutation. It is refused AND the mutation does not happen.
#[tokio::test]
async fn a_run_spawned_before_boot_is_refused_and_changes_nothing() {
    let (_temp, db, orch) = fixture(Some(BOOT_AT - 45), BOOT_AT - 60, "live").await;

    let result = dispatch(&orch, &commenting_write(RUN_ID)).await;

    assert_refused(&result);
    assert_eq!(
        comment_count(&db).await,
        0,
        "a fenced write must not reach the handler: the whole point is the absence of the effect"
    );
}

/// The guard that gives the test above teeth: the same fixture one second on the
/// other side of boot is served, and the effect lands. Without this, a fence that
/// refused everything would look correct.
#[tokio::test]
async fn a_run_spawned_after_boot_is_served_and_its_change_lands() {
    let (_temp, db, orch) = fixture(Some(BOOT_AT + 1), BOOT_AT + 1, "live").await;

    let result = dispatch(&orch, &commenting_write(RUN_ID)).await;

    assert!(
        !result.contains("no longer served"),
        "a run this runner spawned must be served: {result}"
    );
    assert_eq!(
        comment_count(&db).await,
        1,
        "the write should land: {result}"
    );
}

/// Seconds are the stored granularity, so a run spawned during the boot second
/// carries `started_at == boot_at`. It is ours.
#[tokio::test]
async fn a_run_spawned_during_the_boot_second_is_served() {
    let (_temp, db, orch) = fixture(Some(BOOT_AT), BOOT_AT, "live").await;

    let result = dispatch(&orch, &commenting_write(RUN_ID)).await;

    assert_eq!(
        comment_count(&db).await,
        1,
        "the boundary must be strict `<`, never `<=`: {result}"
    );
}

/// A run still `starting` has no `started_at` yet; `created_at` decides, and it
/// decides both ways.
#[tokio::test]
async fn a_null_started_at_falls_back_to_created_at() {
    let (_temp, db, orch) = fixture(None, BOOT_AT - 30, "starting").await;
    assert_refused(&dispatch(&orch, &commenting_write(RUN_ID)).await);
    assert_eq!(comment_count(&db).await, 0);

    let (_temp, db, orch) = fixture(None, BOOT_AT + 30, "starting").await;
    let result = dispatch(&orch, &commenting_write(RUN_ID)).await;
    assert_eq!(
        comment_count(&db).await,
        1,
        "a run created after boot is still starting, not disowned: {result}"
    );
}

/// A run this runner spawned but has already finalized has nothing recording it
/// either, so a process still calling tools for it is refused too. This is the
/// defense-in-depth clause covering the window before startup reconciliation
/// finishes — 15 seconds in the incident, with the callback already serving.
#[tokio::test]
async fn a_finalized_run_is_refused_even_though_it_postdates_boot() {
    for status in ["crashed", "exited"] {
        let (_temp, db, orch) = fixture(Some(BOOT_AT + 10), BOOT_AT + 10, status).await;
        assert_refused(&dispatch(&orch, &commenting_write(RUN_ID)).await);
        assert_eq!(comment_count(&db).await, 0, "status={status}");
    }
}

/// A live process handle settles ownership on its own: the registry is in-memory
/// and starts empty at boot, so a run it holds was spawned here. This is the
/// clause that makes a false positive need two independent signals to be wrong at
/// once — even a row that looks pre-boot is served while its process is ours.
#[tokio::test]
async fn a_registered_process_is_served_whatever_its_row_says() {
    let (_temp, db, orch) = fixture(Some(BOOT_AT - 45), BOOT_AT - 60, "live").await;
    register_process(&orch, RUN_ID);

    let result = dispatch(&orch, &commenting_write(RUN_ID)).await;

    assert_eq!(
        comment_count(&db).await,
        1,
        "a run with a live handle in this runner's registry is owned: {result}"
    );
}

/// Run rows are deleted only when their issue or thread is erased, so a routed
/// database that answers with no row is a definite fact: the run was erased, or
/// its id is stale or forged. Nothing records what such a caller does.
#[tokio::test]
async fn an_unknown_run_is_refused_as_erased() {
    let (_temp, db, orch) = fixture(Some(BOOT_AT + 1), BOOT_AT + 1, "live").await;

    let result = dispatch(&orch, &commenting_write("run-that-never-existed")).await;

    assert_erased(&result);
    assert_eq!(comment_count(&db).await, 0);
}

/// The live process of an erased job. The registry entry proves this runner
/// spawned it, but not that anything still records it: the process-map hit must
/// not short-circuit past the missing row, for reads or for writes.
#[tokio::test]
async fn a_registered_run_whose_row_was_erased_is_refused_on_reads_and_writes() {
    let (temp, db, orch) = fixture(Some(BOOT_AT + 1), BOOT_AT + 1, "live").await;
    register_process(&orch, RUN_ID);
    let file = temp.path().join("erased-read.txt");
    std::fs::write(&file, "content an erased run must not see").unwrap();

    // Served while its record exists, so the refusals below are the erasure's.
    let before = dispatch(&orch, &file_read(RUN_ID, &file)).await;
    assert!(
        before.contains("content an erased run must not see"),
        "{before}"
    );

    common::execute(&db, "DELETE FROM runs WHERE id = 'run-fenced'")
        .await
        .unwrap();

    let write = dispatch(&orch, &commenting_write(RUN_ID)).await;
    assert_erased(&write);
    assert_eq!(
        comment_count(&db).await,
        0,
        "an erased run's write must not reach the handler: {write}"
    );

    let read = dispatch(&orch, &file_read(RUN_ID, &file)).await;
    assert_erased(&read);
    assert!(
        !read.contains("content an erased run must not see"),
        "an erased run's read must not reach the handler: {read}"
    );
}

/// A record the host cannot read right now is not a record that is gone. A
/// team-routed run whose replica is not open fails routing: that call is refused
/// as retryable, nothing is latched, and once the replica opens the next call is
/// served normally.
#[tokio::test]
async fn an_unreadable_run_record_refuses_only_the_call_it_hit() {
    const TEAM: &str = "teamFence1";
    let run_id = format!("{TEAM}~{}", uuid::Uuid::new_v4());
    let (temp, db) = common::migrated_db().await;
    let orch = common::orchestrator_booted_at(&temp, Arc::new(db), BOOT_AT);
    let (_team_temp, team_db) = common::migrated_db().await;
    let team_db = Arc::new(team_db);
    seed_run_as(&team_db, &run_id, Some(BOOT_AT + 1), BOOT_AT + 1, "live").await;
    register_process(&orch, &run_id);
    let file = temp.path().join("transient-read.txt");
    std::fs::write(&file, "served once the record is readable").unwrap();

    let refused = dispatch(&orch, &file_read(&run_id, &file)).await;
    assert!(
        refused.contains("could not read this run's record") && refused.contains("Retry"),
        "expected the retryable refusal, got: {refused}"
    );
    assert!(
        !refused.contains("no longer served") && !refused.contains("record no longer exists"),
        "a read failure must not be reported as a disowned or erased run: {refused}"
    );
    assert!(
        !refused.contains("served once the record is readable"),
        "{refused}"
    );

    orch.db.insert_team_db_for_test(TEAM, team_db).await;

    let served = dispatch(&orch, &file_read(&run_id, &file)).await;
    assert!(
        served.contains("served once the record is readable"),
        "the next call, with the record readable, must be served: {served}"
    );
}

/// A request with no run identity at all (a user-invoked `cairn read`, the
/// desktop's own calls) is untouched by the fence.
#[tokio::test]
async fn a_request_without_a_run_id_is_not_fenced() {
    let (_temp, db, orch) = fixture(Some(BOOT_AT - 45), BOOT_AT - 60, "live").await;
    let mut request = commenting_write(RUN_ID);
    request.run_id = None;

    let result = dispatch(&orch, &request).await;

    assert!(!result.contains("no longer served"), "{result}");
    assert_eq!(
        comment_count(&db).await,
        1,
        "a runless write is served as before: {result}"
    );
}

/// Reads are fenced on the same terms as mutations. A zombie's reads are
/// harmless in themselves, but serving them keeps it working — believing it is
/// making progress that nothing will record.
#[tokio::test]
async fn reads_are_fenced_too() {
    let (_temp, _db, orch) = fixture(Some(BOOT_AT - 45), BOOT_AT - 60, "live").await;
    let request = McpCallbackRequest {
        answer_bound_ms: None,
        thread_id: None,
        cwd: "/tmp".to_string(),
        run_id: Some(RUN_ID.to_string()),
        tool: "read".to_string(),
        payload: serde_json::json!({ "path": "file:/etc/hostname" }),
        tool_use_id: None,
        origin: None,
    };

    assert_refused(&dispatch(&orch, &request).await);
}

/// The fence in its PRODUCTION configuration, where a wrong answer refuses every
/// tool call of every live agent.
///
/// Everything above chooses `boot_at` to place a fixture run on one side of the
/// line. These two build the orchestrator the way the runner's runtime does — no
/// override, so `boot_at` is the real construction time — and stamp the run row
/// the way `insert_run` and `transition_run_to_live` stamp it, with
/// `chrono::Utc::now().timestamp()`. That pins both real inputs to the comparison
/// (its units and its direction) rather than a fixture's chosen constants.
mod production_defaults {
    use super::*;
    use cairn_core::internal::db::DbState;
    use cairn_core::internal::services::testing::TestServicesBuilder;
    use cairn_core::internal::storage::SearchIndex;

    /// A host built the way the runner's runtime builds it: no `boot_at`
    /// override, so ownership is judged against this call's wall clock.
    ///
    /// The order matters and mirrors production, where the orchestrator exists
    /// before any run it will own: build the host FIRST, then let the caller
    /// stamp its run. Seeding first made the fixture's own setup cost (running
    /// every migration) look like elapsed time before boot, and the run was
    /// correctly refused for predating a host that did not yet exist.
    async fn host_booted_now() -> (tempfile::TempDir, Arc<LocalDb>, Orchestrator) {
        let (temp, db) = common::migrated_db().await;
        let db = Arc::new(db);
        let search = Arc::new(SearchIndex::open_or_create(temp.path().join("search")).unwrap());
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(db.clone(), search)),
            Arc::new(TestServicesBuilder::new().build()),
            temp.path().join("config"),
        )
        .build();
        (temp, db, orch)
    }

    /// The happy path: a live host serving a run it just spawned. If this ever
    /// fails, every agent on the machine is fenced out.
    #[tokio::test]
    async fn a_run_stamped_now_is_served_by_a_just_built_orchestrator() {
        let (_temp, db, orch) = host_booted_now().await;
        let now = chrono::Utc::now().timestamp();
        seed_run(&db, Some(now), now, "live").await;

        let result = dispatch(&orch, &commenting_write(RUN_ID)).await;

        assert_eq!(
            comment_count(&db).await,
            1,
            "a run spawned by this host must be served under the production default: {result}"
        );
    }

    /// And the same production default still catches the incident's shape: a run
    /// left behind by a host that existed long before this one.
    #[tokio::test]
    async fn a_run_stamped_long_ago_is_refused_by_a_just_built_orchestrator() {
        let (_temp, db, orch) = host_booted_now().await;
        seed_run(&db, Some(1), 1, "live").await;

        assert_refused(&dispatch(&orch, &commenting_write(RUN_ID)).await);
        assert_eq!(comment_count(&db).await, 0);
    }
}
