//! One job, one execution home.
//!
//! Every surface of an agent job — synchronous `run` batches, terminals, and
//! promoted runs — executes in a single environment. These tests assert that
//! behaviorally, through the agent-facing tools, rather than by inspecting the
//! executor's internals: a file one surface writes is a file the other reads,
//! at the same absolute path, with the same `$TMPDIR`.
//!
//! The concurrency case is the load-bearing one. Sharing used to happen by
//! accident when nothing else held a cell, and invert exactly when a terminal
//! was alive — so a test that only exercises the quiet case proves nothing.
//!
//! Every test here provisions a real cell, spawns real shells, and drives a real
//! executor, so like the other executor-backed suites it cannot run nested
//! inside a Cairn worktree fence and gates on `common::skip_if_fenced`. The skip
//! is recorded rather than silent: the runner reports skipped separately from
//! passed, so an all-skipped suite can never read as an all-passed one.

use crate::common;

use std::collections::HashMap;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use cairn_common::executor_protocol::{ResidencyOperation, ResidencyResult};
use cairn_core::internal::db::DbState;
use cairn_core::internal::mcp::handlers::read::handle_read_file;
use cairn_core::internal::mcp::handlers::run::handle_run;
use cairn_core::internal::mcp::handlers::write::handle_write;
use cairn_core::internal::mcp::types::McpCallbackRequest;
use cairn_core::internal::orchestrator::Orchestrator;
use cairn_core::internal::services::testing::TestServicesBuilder;
use cairn_core::internal::services::{RealProcessSpawner, RealPtyFactory};
use cairn_core::internal::storage::{LocalDb, RowExt, SearchIndex};
use cairn_core::models::{
    AgentSnapshot, ExecutionSnapshot, RecipeSnapshot, RecipeTrigger, TriggerContext, TriggerType,
};
use cairn_db::turso::params;
use serde_json::{json, Value};
use tempfile::TempDir;

/// Wait for a path to appear, then print it. Both surfaces synchronize on the
/// filesystem they are supposed to share, so no test depends on wall-clock
/// ordering between a terminal's shell and a run batch.
///
/// The budget is deliberately generous. These tests provision a cell, spawn a
/// real shell, and wait on cross-process filesystem effects; when the whole
/// workspace suite runs in parallel, all three take far longer than they do in
/// isolation. A tight bound here does not detect a broken handoff faster — it
/// just converts contention into a spurious failure.
const SYNC_ATTEMPTS: u32 = 600;
const SYNC_INTERVAL: &str = "0.1";

fn await_and_print(path: &str) -> String {
    format!(
        "for _ in $(seq 1 {SYNC_ATTEMPTS}); do [ -f {path} ] && break; sleep {SYNC_INTERVAL}; done; cat {path}"
    )
}

fn git(repo: &Path, args: &[&str]) {
    let output = Command::new("git")
        .args(args)
        .current_dir(repo)
        .output()
        .unwrap_or_else(|error| panic!("git {args:?}: {error}"));
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// A repository with a gitignored directory, which is where state that exists
/// only in an environment lives: virtualenvs, `node_modules`, build caches.
fn init_repo(repo: &Path) {
    std::fs::create_dir_all(repo).unwrap();
    git(repo, &["init", "-q"]);
    git(repo, &["config", "user.email", "test@example.com"]);
    git(repo, &["config", "user.name", "Test User"]);
    std::fs::write(repo.join(".gitignore"), "env/\nnode_modules/\n").unwrap();
    std::fs::write(repo.join("README.md"), "initial\n").unwrap();
    std::fs::write(repo.join("tracked.txt"), "tracked\n").unwrap();
    git(repo, &["add", ".gitignore", "README.md", "tracked.txt"]);
    git(repo, &["commit", "-qm", "initial"]);
}

fn agent_snapshot() -> AgentSnapshot {
    AgentSnapshot {
        edited_at: None,
        id: "agent-1".to_string(),
        name: "Builder".to_string(),
        description: String::new(),
        prompt: String::new(),
        tools: Vec::new(),
        tier: None,
        backend_preference: None,
        selection: None,
        model: None,
        disallowed_tools: None,
        skills: None,
        // No fence at all: both `run` and terminals then execute unconfined, so
        // these tests observe the shared environment rather than OS sandbox
        // policy, and stay platform-independent.
        fence: None,
        sandbox: None,
        on_escape: None,
        resolved_backend: None,
        extras: None,
    }
}

async fn seed(db: &LocalDb, project_id: &str, branch: &str, base_commit: &str) {
    let mut agents = HashMap::new();
    agents.insert("agent-1".to_string(), agent_snapshot());
    let snapshot = ExecutionSnapshot::new(
        RecipeSnapshot {
            id: "recipe-home".to_string(),
            name: "Execution home".to_string(),
            description: None,
            trigger: RecipeTrigger::Manual,
            nodes: Vec::new(),
            edges: Vec::new(),
        },
        agents,
        HashMap::new(),
        TriggerContext {
            issue_id: Some("issue-1".to_string()),
            project_id: project_id.to_string(),
            trigger_type: TriggerType::Manual,
            event_payload: None,
            initiated_via: None,
        },
    )
    .to_json()
    .unwrap();

    let project_id = project_id.to_string();
    let branch = branch.to_string();
    let base_commit = base_commit.to_string();
    db.write(move |conn| {
        let project_id = project_id.clone();
        let branch = branch.clone();
        let base_commit = base_commit.clone();
        let snapshot = snapshot.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO issues(id, project_id, number, title, status, attention, created_at, updated_at)
                 VALUES ('issue-1', ?1, 1, 'Execution home', 'active', 'none', 1, 1)",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot, triggered_by)
                 VALUES ('exec-1', 'recipe-home', 'issue-1', ?1, 'running', 1, 1, ?2, 'manual')",
                params![project_id.as_str(), snapshot.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO jobs(id, execution_id, agent_config_id, issue_id, project_id, node_name, status, current_session_id, uri_segment, branch, base_commit, created_at, updated_at)
                 VALUES ('job-1', 'exec-1', 'agent-1', 'issue-1', ?1, 'builder', 'running', 'session-1', 'builder', ?2, ?3, 1, 1)",
                params![project_id.as_str(), branch.as_str(), base_commit.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO sessions(id, job_id, status, created_at, updated_at)
                 VALUES ('session-1', 'job-1', 'active', 1, 1)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO runs(id, project_id, issue_id, job_id, status, session_id, created_at, updated_at, start_mode)
                 VALUES ('r-1', ?1, 'issue-1', 'job-1', 'live', 'session-1', 1, 1, 'resume')",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO turns(id, session_id, run_id, job_id, sequence, state, created_at, updated_at)
                 VALUES ('turn-1', 'session-1', 'r-1', 'job-1', 1, 'running', 1, 1)",
                (),
            )
            .await?;
            conn.execute(
                "UPDATE jobs SET current_turn_id = 'turn-1' WHERE id = 'job-1'",
                (),
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

struct Fixture {
    _temp: TempDir,
    _db: Arc<LocalDb>,
    orch: Orchestrator,
    cwd: String,
}

async fn fixture() -> Fixture {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let repo = temp.path().join("project");
    init_repo(&repo);
    let project_id = common::insert_project_with_repo(&db, "exh", &repo).await;
    let branch = "agent/EXH-1-builder-0";
    let base_commit = common::head_sha(&repo);
    seed(&db, &project_id, branch, &base_commit).await;

    let search_index = Arc::new(SearchIndex::open_or_create(temp.path().join("search")).unwrap());
    let db_state = Arc::new(DbState::new(db.clone(), search_index));
    let services = Arc::new(
        TestServicesBuilder::new()
            .with_process(RealProcessSpawner)
            .with_pty_factory(RealPtyFactory)
            .build(),
    );
    let orch = Orchestrator::builder(db_state, services, temp.path().join("config")).build();
    common::attach_test_executor(&orch);

    let residence = temp.path().join("residence");
    common::provision_jj_workspace(&temp.path().join("config"), &repo, &residence, branch);
    let cwd = residence.display().to_string();
    Fixture {
        _temp: temp,
        _db: db,
        orch,
        cwd,
    }
}

impl Fixture {
    async fn run(&self, payload: Value) -> String {
        let result = handle_run(
            &self.orch,
            &McpCallbackRequest {
                thread_id: None,
                cwd: self.cwd.clone(),
                run_id: Some("r-1".to_string()),
                tool: "run".to_string(),
                payload,
                tool_use_id: Some("toolu-home".to_string()),
            },
        )
        .await;
        serde_json::from_str::<Value>(&result).unwrap()["text"]
            .as_str()
            .unwrap()
            .to_string()
    }

    async fn shell(&self, command: &str) -> String {
        self.run(json!({ "commands": [{ "command": command }] }))
            .await
    }

    /// A terminal legitimately runs the operator's login shell, which is not
    /// necessarily POSIX. These tests are about the environment two surfaces
    /// share, not about shell dialect, so the terminal side is pinned to `sh`.
    async fn start_terminal(&self, slug: &str, script: &str) -> String {
        assert!(!script.contains('\''), "terminal scripts are single-quoted");
        self.start_terminal_command(slug, &format!("sh -c '{script}'"))
            .await
    }

    async fn start_terminal_command(&self, slug: &str, command: &str) -> String {
        handle_write(
            &self.orch,
            &McpCallbackRequest {
                thread_id: None,
                cwd: String::new(),
                run_id: Some("r-1".to_string()),
                tool: "write".to_string(),
                // `wake: exit` makes the terminal a one-shot `$SHELL -c`, which
                // runs the command directly instead of typing it into an
                // interactive shell. That keeps these tests about the shared
                // environment rather than about shell startup behavior.
                payload: json!({ "changes": [{
                    "target": format!("cairn://p/exh/1/1/builder/terminal/{slug}"),
                    "mode": "create",
                    "payload": { "command": command, "wake": "exit" }
                }]}),
                tool_use_id: None,
            },
        )
        .await
    }

    /// Deliver a command line to an already-running terminal, the way an agent's
    /// `write` append does.
    async fn send_terminal(&self, slug: &str, line: &str) -> String {
        handle_write(
            &self.orch,
            &McpCallbackRequest {
                thread_id: None,
                cwd: self.cwd.clone(),
                run_id: Some("r-1".to_string()),
                tool: "write".to_string(),
                payload: json!({ "changes": [{
                    "target": format!("cairn://p/exh/1/1/builder/terminal/{slug}"),
                    "mode": "append",
                    "payload": { "content": line }
                }]}),
                tool_use_id: None,
            },
        )
        .await
    }

    /// The PTY session id behind a terminal slug, which is the only handle the
    /// operator's keystroke path has.
    async fn terminal_session_id(&self, slug: &str) -> String {
        let slug = slug.to_string();
        self._db
            .read(move |conn| {
                let slug = slug.clone();
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT session_id FROM job_terminals WHERE slug = ?1 LIMIT 1",
                            params![slug.as_str()],
                        )
                        .await?;
                    rows.next().await?.expect("terminal row present").text(0)
                })
            })
            .await
            .unwrap()
    }

    /// Deliver a line the way the terminal pane does: raw bytes ending in the `\r`
    /// an Enter key emits, through the operator's own entry point rather than the
    /// agent's.
    async fn type_into_terminal(&self, slug: &str, line: &str) -> Result<(), String> {
        let session_id = self.terminal_session_id(slug).await;
        cairn_core::terminal_host::write_pty(&self.orch, session_id, format!("{line}\r")).await
    }

    /// Publish a whole-file mutation through the logical head. This advances what
    /// `read` serves and deliberately does not materialize any checkout.
    async fn write_file(&self, path: &str, content: &str) -> String {
        handle_write(
            &self.orch,
            &McpCallbackRequest {
                thread_id: None,
                cwd: self.cwd.clone(),
                run_id: Some("r-1".to_string()),
                tool: "write".to_string(),
                payload: json!({
                    "changes": [{
                        "target": format!("file:{path}"),
                        "mode": "replace",
                        "payload": { "content": content }
                    }],
                    "commit_msg": format!("rewrite {path}")
                }),
                tool_use_id: None,
            },
        )
        .await
    }

    /// Read through the logical head, which is what publication advances — not
    /// the shared home's working tree, which every surface is free to dirty.
    async fn read(&self, path: &str) -> String {
        handle_read_file(
            &self.orch,
            &McpCallbackRequest {
                thread_id: None,
                cwd: self.cwd.clone(),
                run_id: Some("r-1".to_string()),
                tool: "read".to_string(),
                payload: json!({ "path": path }),
                tool_use_id: None,
            },
        )
        .await
    }

    /// Revoke the job's execution lease, standing in for an executor reclaim.
    /// Reclaim legality is the contract that separates lease-window convergence
    /// from a durable job-to-materialization binding, so a job has to survive
    /// this at any moment without noticing.
    async fn revoke_execution_lease(&self) {
        let fence = self
            .orch
            .fleet
            .residency_fence(&cairn_common::executor_protocol::ResidencyHolder::Job {
                job_id: "job-1".into(),
            })
            .expect("job had no execution lease to revoke");
        let result = self
            .orch
            .fleet
            .operate_residency(&self.orch, ResidencyOperation::Release { fence })
            .await;
        assert!(
            !matches!(result, ResidencyResult::Failed { .. }),
            "revoking the execution lease failed: {result:?}"
        );
    }

    /// Which execution environment a terminal recorded itself in, as the holder
    /// key the fence persists.
    async fn terminal_residency(&self, slug: &str) -> Option<String> {
        let slug = slug.to_string();
        self._db
            .read(move |conn| {
                let slug = slug.clone();
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT residency_holder FROM job_terminals WHERE slug = ?1 LIMIT 1",
                            params![slug.as_str()],
                        )
                        .await?;
                    match rows.next().await? {
                        Some(row) => row.opt_text(0),
                        None => Ok(None),
                    }
                })
            })
            .await
            .unwrap()
    }

    async fn terminal_row(&self, slug: &str) -> String {
        let slug = slug.to_string();
        self._db
            .read(move |conn| {
                let slug = slug.clone();
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT status, command, COALESCE(exit_code, -1), COALESCE(output_tail, '') FROM job_terminals WHERE slug = ?1 LIMIT 1",
                            params![slug.as_str()],
                        )
                        .await?;
                    match rows.next().await? {
                        Some(row) => Ok(format!(
                            "status={} command={:?} exit={} tail={:?}",
                            row.text(0)?,
                            row.text(1)?,
                            row.i64(2)?,
                            row.text(3)?
                        )),
                        None => Ok("no terminal row".to_string()),
                    }
                })
            })
            .await
            .unwrap()
    }

    fn kill_terminals(&self) {
        if let Ok(sessions) = self.orch.pty_state.sessions.lock() {
            for session in sessions.values() {
                if let Ok(mut session) = session.lock() {
                    let _ = session.child.kill();
                }
            }
        }
    }
}

/// Criterion 1 and 5: state that exists only in the environment — the class a
/// package install belongs to — crosses between `run` and a terminal in both
/// directions, while the terminal is running.
#[tokio::test]
async fn ignored_state_hands_off_between_run_and_a_live_terminal() {
    if common::skip_if_fenced("ignored_state_hands_off_between_run_and_a_live_terminal") {
        return;
    }
    let fixture = fixture().await;

    let created = fixture
        .start_terminal(
            "handoff",
            &format!(
                "mkdir -p env; echo WROTE-BY-TERMINAL > env/from-terminal; {}; cp env/from-run env/seen-by-terminal; sleep 60",
                await_and_print("env/from-run")
            ),
        )
        .await;
    assert_eq!(
        fixture.terminal_residency("handoff").await.as_deref(),
        Some("job:job-1"),
        "terminal did not join the job's execution environment: {created}"
    );

    // Terminal → run.
    let seen = fixture.shell(&await_and_print("env/from-terminal")).await;
    assert!(
        seen.contains("WROTE-BY-TERMINAL"),
        "a terminal's environment state was invisible to `run`: {seen}"
    );

    // Run → terminal, proved by the terminal's own copy of the run's file.
    fixture
        .shell("mkdir -p env && echo WROTE-BY-RUN > env/from-run")
        .await;
    let echoed = fixture
        .shell(&await_and_print("env/seen-by-terminal"))
        .await;
    assert!(
        echoed.contains("WROTE-BY-RUN"),
        "a `run` batch's environment state was invisible to a terminal: {echoed}"
    );

    fixture.kill_terminals();
}

/// The headline divergence, stated at the layer an agent stands in: a `write`
/// advances the logical head, a `read` shows the new content, and a command
/// DELIVERED to a shell that was already running must see that same content.
///
/// A terminal used to be aligned only when its shell spawned, so a write landing
/// afterwards left every later command in that shell compiling pre-write source —
/// and nothing on any surface reported the disagreement. From inside a session
/// that is indistinguishable from the substrate corrupting the agent's work.
///
/// The terminal reports what it saw through the gitignored directory both surfaces
/// share, because an alignment is entitled to reset tracked paths and is not
/// entitled to touch that one.
#[tokio::test]
async fn a_command_delivered_to_a_live_terminal_sees_what_a_read_serves() {
    if common::skip_if_fenced("a_command_delivered_to_a_live_terminal_sees_what_a_read_serves") {
        return;
    }
    let fixture = fixture().await;

    // A shell that stays up reading lines from its PTY, spawned BEFORE the write.
    // That ordering is the whole point: its spawn-time alignment is about to go
    // stale.
    fixture
        .start_terminal_command(
            "probe",
            "sh -c 'while IFS= read -r line; do eval \"$line\"; done'",
        )
        .await;

    let published = fixture
        .write_file("tracked.txt", "rewritten through the logical head\n")
        .await;
    let served = fixture.read("file:tracked.txt").await;
    assert!(
        served.contains("rewritten through the logical head"),
        "the write did not advance what a read serves: {published} / {served}"
    );

    fixture
        .send_terminal(
            "probe",
            "mkdir -p env && cp tracked.txt env/seen-by-terminal",
        )
        .await;

    let seen = fixture
        .shell(&await_and_print("env/seen-by-terminal"))
        .await;
    assert!(
        seen.contains("rewritten through the logical head"),
        "a command delivered to a live terminal ran against content no `read` serves: {seen}"
    );

    fixture.kill_terminals();
}

/// The same invariant through the OTHER delivery surface. An operator typing into
/// the terminal pane reaches the PTY by a different entry point than an agent's
/// append — `terminal_host::write_pty` — and a newly delivered command line there
/// is a command into the same job residence. A second surface that wrote to the
/// PTY directly is exactly how the alignment gate came to be bypassable, so this
/// pins that both surfaces go through it.
#[tokio::test]
async fn a_line_typed_into_the_terminal_pane_sees_what_a_read_serves() {
    if common::skip_if_fenced("a_line_typed_into_the_terminal_pane_sees_what_a_read_serves") {
        return;
    }
    let fixture = fixture().await;

    fixture
        .start_terminal_command(
            "typed",
            "sh -c 'while IFS= read -r line; do eval \"$line\"; done'",
        )
        .await;

    fixture
        .write_file("tracked.txt", "rewritten before the operator typed\n")
        .await;
    let served = fixture.read("file:tracked.txt").await;
    assert!(
        served.contains("rewritten before the operator typed"),
        "the write did not advance what a read serves: {served}"
    );

    fixture
        .type_into_terminal("typed", "mkdir -p env && cp tracked.txt env/seen-by-pane")
        .await
        .expect("the operator's Enter must be delivered");

    let seen = fixture.shell(&await_and_print("env/seen-by-pane")).await;
    assert!(
        seen.contains("rewritten before the operator typed"),
        "a line typed into the terminal pane ran against content no `read` serves: {seen}"
    );

    fixture.kill_terminals();
}

/// Criterion 2: `$TMPDIR` is one directory for the whole job — across two
/// consecutive `run` batches, and across `run` and a live terminal. The
/// batch-to-batch half is the regression that used to fail silently, because
/// scratch was wiped before every batch.
#[tokio::test]
async fn tmpdir_is_shared_across_batches_and_with_a_live_terminal() {
    if common::skip_if_fenced("tmpdir_is_shared_across_batches_and_with_a_live_terminal") {
        return;
    }
    let fixture = fixture().await;

    fixture
        .shell("echo first-batch > \"$TMPDIR/handoff\"")
        .await;
    let second = fixture.shell("cat \"$TMPDIR/handoff\"").await;
    assert!(
        second.contains("first-batch"),
        "a second `run` batch could not read what the first wrote to $TMPDIR: {second}"
    );

    fixture
        .start_terminal(
            "tmp",
            "cp \"$TMPDIR/handoff\" \"$TMPDIR/seen-by-terminal\"; echo WROTE-BY-TERMINAL > \"$TMPDIR/from-terminal\"; sleep 60",
        )
        .await;

    let from_terminal = fixture
        .shell(&await_and_print("\"$TMPDIR/from-terminal\""))
        .await;
    assert!(
        from_terminal.contains("WROTE-BY-TERMINAL"),
        "a terminal's $TMPDIR was not the run's $TMPDIR: {from_terminal}"
    );
    let echoed = fixture
        .shell(&await_and_print("\"$TMPDIR/seen-by-terminal\""))
        .await;
    assert!(
        echoed.contains("first-batch"),
        "a terminal could not read what `run` wrote to $TMPDIR: {echoed}"
    );

    fixture.kill_terminals();
}

/// Writing a helper script to `$TMPDIR` and running it is an ordinary thing to do,
/// and on an ordinary checkout that script can import the project's installed
/// packages. A scratch dir is not inside the checkout, though, and bun resolves a
/// bare specifier by walking up from the *importing file's* own directory — never
/// from cwd, and never through `NODE_PATH` — so nothing about how the script is
/// launched can recover the project's packages on its own. Both surfaces must
/// therefore give scratch the resolution a checkout would have; without it the one
/// sanctioned way to run a project-aware helper script fails with `Cannot find
/// module`.
///
/// The install here lands after the environment was provisioned, which is the
/// normal order rather than a corner: an agent installs packages partway through a
/// job, long after its scratch dir was created.
#[tokio::test]
async fn a_scratch_helper_script_imports_the_checkouts_packages() {
    if common::skip_if_fenced("a_scratch_helper_script_imports_the_checkouts_packages") {
        return;
    }
    let fixture = fixture().await;

    // An installed package as an install leaves one: ignored content in the
    // checkout, reachable only as a bare specifier through `node_modules`.
    fixture
        .shell(
            "mkdir -p node_modules/probe-pkg \
             && printf '{\"name\":\"probe-pkg\",\"type\":\"module\",\"main\":\"index.js\"}' > node_modules/probe-pkg/package.json \
             && printf 'export const marker = \"RESOLVED-FROM-CHECKOUT\";\\n' > node_modules/probe-pkg/index.js \
             && printf 'import { marker } from \"probe-pkg\"; console.log(marker);\\n' > \"$TMPDIR/helper.ts\"",
        )
        .await;

    // `cd /` first: resolution that survives that came from the script's own
    // location rather than from the process cwd, which is the property under test.
    let from_run = fixture
        .shell("cd / && bun \"$TMPDIR/helper.ts\" 2>&1")
        .await;
    assert!(
        from_run.contains("RESOLVED-FROM-CHECKOUT"),
        "a `run` batch could not import the checkout's packages from a $TMPDIR script: {from_run}"
    );

    fixture
        .start_terminal(
            "helper",
            "mkdir -p env; bun \"$TMPDIR/helper.ts\" > env/out 2>&1; mv env/out env/terminal-helper; sleep 60",
        )
        .await;
    let from_terminal = fixture.shell(&await_and_print("env/terminal-helper")).await;
    assert!(
        from_terminal.contains("RESOLVED-FROM-CHECKOUT"),
        "a terminal could not import the checkout's packages from a $TMPDIR script: {from_terminal}"
    );

    fixture.kill_terminals();
}

/// Criteria 3 and 4: the two surfaces agree on their environment fingerprint
/// while both are live. Selection used to push a run batch out of a cell a
/// terminal was holding, so this must be asserted under concurrency.
///
/// Agreement alone is not the whole guarantee. Both surfaces also start at the
/// project checkout ROOT, which is what lets every sanctioned operation be
/// written in project-relative terms; a regression that moved both into a
/// subdirectory or into scratch would keep their parity green while forcing
/// agents back onto absolute materialization paths. So each surface's `pwd` is
/// pinned against its own repository top level.
#[tokio::test]
async fn run_and_terminal_report_the_same_environment_while_both_are_live() {
    if common::skip_if_fenced("run_and_terminal_report_the_same_environment_while_both_are_live") {
        return;
    }
    let fixture = fixture().await;

    fixture
        .start_terminal(
            "fingerprint",
            "mkdir -p env; pwd > env/fp; echo \"$TMPDIR\" >> env/fp; git rev-parse --show-toplevel >> env/fp; mv env/fp env/terminal-fingerprint; sleep 60",
        )
        .await;

    let terminal = fixture
        .shell(&await_and_print("env/terminal-fingerprint"))
        .await;
    let from_run = fixture
        .shell("pwd; echo \"$TMPDIR\"; git rev-parse --show-toplevel")
        .await;

    let reported: Vec<&str> = terminal
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with('/'))
        .collect();
    assert_eq!(
        reported.len(),
        3,
        "terminal did not report a full fingerprint: {terminal}"
    );
    for line in &reported {
        assert!(
            from_run.contains(line),
            "terminal reported {line:?}, which `run` did not: {from_run}"
        );
    }

    // The fingerprint is (pwd, $TMPDIR, repository top level) in that order, so
    // comparing its first and last entries asks whether the surface started at
    // the checkout root rather than merely somewhere inside it.
    assert_eq!(
        reported[0], reported[2],
        "a terminal did not start at the project checkout root: {terminal}"
    );

    let run_reported: Vec<&str> = from_run
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with('/'))
        .collect();
    assert_eq!(
        run_reported.len(),
        3,
        "`run` did not report a full fingerprint: {from_run}"
    );
    assert_eq!(
        run_reported[0], run_reported[2],
        "a run batch did not start at the project checkout root: {from_run}"
    );

    fixture.kill_terminals();
}

/// The publication bracket. A terminal's in-progress edit to a tracked file
/// predates the batch, so a batch that declines to commit undoes only its own
/// paths and leaves the terminal's work exactly where it was. Restoring the
/// whole tree — what an exclusive cell could afford — would destroy it.
///
/// The committing half also covers base advance under convergence: publishing
/// moves the branch tip, so the batch that follows resolves a newer commit and
/// the cell must advance onto it while the terminal still holds its edit.
#[tokio::test]
async fn declining_to_commit_undoes_only_the_batch_and_spares_concurrent_work() {
    if common::skip_if_fenced(
        "declining_to_commit_undoes_only_the_batch_and_spares_concurrent_work",
    ) {
        return;
    }
    let fixture = fixture().await;

    // The terminal's edit has to be finished before the batch under test opens
    // its bracket, which is the whole point: a change that predates the batch is
    // not the batch's to undo. So the readiness signal is read from the host
    // rather than from a `run` batch, which would itself be a bracket in flight.
    let home = fixture.shell("pwd").await;
    let home = Path::new(
        home.lines()
            .map(str::trim)
            .find(|line| line.starts_with('/'))
            .expect("run batch reported no working directory"),
    );

    let created = fixture
        .start_terminal(
            "dirt",
            "echo terminal-edit > tracked.txt; echo READY > dirtied; sleep 60",
        )
        .await;
    assert_eq!(
        fixture.terminal_residency("dirt").await.as_deref(),
        Some("job:job-1"),
        "terminal did not join the job's execution environment: {created}"
    );
    let dirtied = home.join("dirtied");
    for _ in 0..SYNC_ATTEMPTS {
        if dirtied.exists() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(
        dirtied.exists(),
        "terminal did not reach the shared home ({created}) [{}]",
        fixture.terminal_row("dirt").await
    );

    // With a commit_msg the batch publishes, and the published head must carry
    // the batch's own file and nothing the terminal was holding.
    fixture
        .run(json!({
            "commands": [{ "command": "echo committed > batch-only.txt" }],
            "commit_msg": "publish only the batch"
        }))
        .await;
    assert!(
        fixture
            .read("file:batch-only.txt")
            .await
            .contains("committed"),
        "a committing batch did not publish its own change"
    );
    let published_tracked = fixture.read("file:tracked.txt").await;
    assert!(
        !published_tracked.contains("terminal-edit"),
        "a committing batch published work another process had in flight: {published_tracked}"
    );

    // Without a commit_msg the batch's own new file must be gone afterwards,
    // and the terminal's edit must survive untouched.
    fixture.shell("echo batch > uncommitted.txt").await;
    let after = fixture
        .shell("test -f uncommitted.txt && echo PRESENT || echo ABSENT; cat tracked.txt")
        .await;
    assert!(
        after.contains("ABSENT"),
        "an uncommitted batch's own change survived: {after}"
    );
    assert!(
        after.contains("terminal-edit"),
        "an uncommitted batch destroyed work another process had in flight: {after}"
    );

    fixture.kill_terminals();
}

/// Reclaim legality is the contract. Convergence lasts exactly as long as a live
/// lease; the executor may take it back at any moment, and a job must survive
/// that without noticing. If losing the lease could fail a job, the shape would
/// no longer be a lease window — it would be the durable job-to-materialization
/// binding that retiring dedicated worktrees deliberately removed.
#[tokio::test]
async fn losing_the_execution_lease_between_batches_is_survivable_and_invisible() {
    if common::skip_if_fenced(
        "losing_the_execution_lease_between_batches_is_survivable_and_invisible",
    ) {
        return;
    }
    let fixture = fixture().await;

    let first = fixture
        .shell("mkdir -p env; echo installed > env/package; echo warm > \"$TMPDIR/handoff\"")
        .await;
    assert!(
        !first.contains("Exit code"),
        "the first batch did not run: {first}"
    );

    fixture.revoke_execution_lease().await;

    // The next batch acquires again at the job's current coordinate and runs
    // normally. Nothing about the boundary is the agent's problem.
    let after = fixture
        .shell("cat tracked.txt; test -f \"$TMPDIR/handoff\" && echo SCRATCH-KEPT || echo SCRATCH-GONE")
        .await;
    assert!(
        after.contains("tracked"),
        "tracked content was not current after reclaim: {after}"
    );
    assert!(
        after.contains("SCRATCH-GONE"),
        "scratch outlived the lease that owned it: {after}"
    );

    // Whether `env/package` survives is deliberately not asserted: re-acquire
    // prefers the previous cell when it still exists, and getting a warm
    // environment back is the point of that preference. What must hold either
    // way is that absence reads as an ordinary machine's absence — a missing
    // file, never the vocabulary of the substrate that reclaimed it.
    let missing = fixture.shell("cat env/never-written").await;
    assert!(
        missing.contains("No such file or directory"),
        "a missing path did not read as an ordinary missing file: {missing}"
    );
    for text in [&first, &after, &missing] {
        for word in [
            "lease",
            "epoch",
            "incarnation",
            "cell",
            "coordinate",
            "materialization",
        ] {
            assert!(
                !text.to_lowercase().contains(word),
                "agent-visible output named the substrate ({word:?}): {text}"
            );
        }
    }
}

/// The scope boundary. Convergence is for a job's own execution; a branch-scoped
/// run addresses a different coordinate entirely and must never join the job's
/// environment. Checks and builds take the same path, which is what keeps their
/// per-batch cells and per-batch scratch hygiene intact.
#[tokio::test]
async fn a_branch_scoped_run_never_joins_the_job_environment() {
    if common::skip_if_fenced("a_branch_scoped_run_never_joins_the_job_environment") {
        return;
    }
    let fixture = fixture().await;

    let seeded = fixture
        .shell("mkdir -p env; echo job-only > env/marker; cat env/marker")
        .await;
    assert!(seeded.contains("job-only"), "seeding failed: {seeded}");

    let elsewhere = fixture
        .run(json!({
            "commands": [{ "command": "cat env/marker" }],
            "branch": "main"
        }))
        .await;
    assert!(
        !elsewhere.contains("job-only"),
        "a branch-scoped run reached into the job's environment: {elsewhere}"
    );
}
