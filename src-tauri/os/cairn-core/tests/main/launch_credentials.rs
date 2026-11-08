//! No callback credential reaches the launch environment of a `run` shell
//! item, a terminal or a REPL on macOS and Linux, on the runner's own
//! colocated executor or on a machine it placed the process on (tenet #1381,
//! `cairn_common::auth::FORBIDDEN_LAUNCH_ENV`).
//!
//! The machines are the node scratch suite's: real `ExecutorRuntime`s, whose
//! children inherit this process's environment as an executor's children
//! inherit the executor's. Each test runs again in a child holding a canary for
//! every credential (`launch_canary::in_planted_child`), so a canary stands for
//! both the runner's and the executor's own environment. What each class is
//! composed with, a batch's runner context or a resident's capability, is
//! searched for by the name it would travel under. The spawned process writes
//! its environment to a file, which no output scrubbing touches.
//!
//! Like that suite these cannot run nested inside a Cairn worktree fence, and
//! each records a declared skip there (skip-manifest.toml).

use crate::common;
use crate::node_scratch_acceptance::{eventually, node, Node, MACHINE_A, MACHINE_B, OWNER};

use cairn_common::auth::launch_canary;
use serde_json::json;
use std::path::{Path, PathBuf};

/// Where the lineage callback socket the test executors serve listens.
fn socket() -> String {
    cairn_exec::callback_roots::socket()
        .expect("the test executors serve a callback socket")
        .display()
        .to_string()
}

/// A fresh file for one process's environment, under the node's own temp.
fn dump(node: &Node, name: &str) -> PathBuf {
    let dir = node.machines.temp.path().join("launch-env");
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(name)
}

/// `env`, written so the file appears whole, and then held open until the
/// test has read it ([`observed`]), so what the process was composed with is
/// still registered while it is searched for.
fn write_env(path: &Path) -> String {
    format!(
        "env > {0}.part && mv {0}.part {0} && while [ ! -e {0}.release ]; do sleep 0.1; done",
        path.display()
    )
}

/// The environment the process at `path` wrote, and every credential found in
/// it while the process still lives, after which it is let go.
async fn observed(path: &Path) -> (String, Vec<String>) {
    eventually("the process's environment", || path.is_file()).await;
    let env = std::fs::read_to_string(path).unwrap();
    let leaks = launch_canary::leaks(&env, &[]);
    std::fs::write(format!("{}.release", path.display()), "").unwrap();
    (env, leaks)
}

fn assert_clean(class: &str, (env, leaks): &(String, Vec<String>)) {
    assert!(
        leaks.is_empty(),
        "{class}'s launch environment carries a credential: {leaks:?}\n{env}"
    );
    launch_canary::assert_names_socket(class, env, &socket());
}

/// Every machine but the colocated one drained, so it is where work lands.
async fn only_colocated(node: &Node) {
    node.attach_colocated().await;
    node.drain(MACHINE_A, true).await;
    node.drain(MACHINE_B, true).await;
}

/// The item is read while its batch runs, so the batch's context is live.
async fn shell_env(node: &Node, machine: Option<&str>, name: &str) -> (String, Vec<String>) {
    let path = dump(node, name);
    let command = write_env(&path);
    let (reply, observed) = tokio::join!(node.shell("r-1", machine, &command), observed(&path));
    assert!(
        path.is_file(),
        "the item did not write its environment: {reply}"
    );
    observed
}

#[tokio::test(flavor = "multi_thread")]
async fn a_placed_run_shell_item_carries_no_callback_credential() {
    if common::skip_if_fenced("a_placed_run_shell_item_carries_no_callback_credential")
        || !launch_canary::in_planted_child(
            module_path!(),
            "a_placed_run_shell_item_carries_no_callback_credential",
        )
    {
        return;
    }
    let node = node().await;
    let env = shell_env(&node, Some(MACHINE_A), "placed-shell").await;
    assert_clean("a placed run shell item", &env);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_colocated_run_shell_item_carries_no_callback_credential() {
    if common::skip_if_fenced("a_colocated_run_shell_item_carries_no_callback_credential")
        || !launch_canary::in_planted_child(
            module_path!(),
            "a_colocated_run_shell_item_carries_no_callback_credential",
        )
    {
        return;
    }
    let node = node().await;
    only_colocated(&node).await;
    let env = shell_env(&node, None, "colocated-shell").await;
    assert_clean("a colocated run shell item", &env);
}

async fn terminal_env(node: &Node, machine: Option<&str>, name: &str) -> (String, Vec<String>) {
    let path = dump(node, name);
    node.start_terminal_of("r-1", OWNER, name, machine, &write_env(&path))
        .await;
    observed(&path).await
}

#[tokio::test(flavor = "multi_thread")]
async fn a_placed_terminal_carries_no_callback_credential() {
    if common::skip_if_fenced("a_placed_terminal_carries_no_callback_credential")
        || !launch_canary::in_planted_child(
            module_path!(),
            "a_placed_terminal_carries_no_callback_credential",
        )
    {
        return;
    }
    let node = node().await;
    let env = terminal_env(&node, Some(MACHINE_A), "placed-terminal").await;
    assert_clean("a placed terminal", &env);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_colocated_terminal_carries_no_callback_credential() {
    if common::skip_if_fenced("a_colocated_terminal_carries_no_callback_credential")
        || !launch_canary::in_planted_child(
            module_path!(),
            "a_colocated_terminal_carries_no_callback_credential",
        )
    {
        return;
    }
    let node = node().await;
    only_colocated(&node).await;
    let env = terminal_env(&node, None, "colocated-terminal").await;
    assert_clean("a colocated terminal", &env);
}

/// A REPL's eval-server is Python here: its environment is written from inside
/// it, as JSON.
async fn repl_env(node: &Node, machine: Option<&str>, slug: &str) -> Option<(String, Vec<String>)> {
    use cairn_common::executor_protocol::ExecutorSelector;
    use cairn_core::internal::mcp::handlers::repl::{self, ReplLang};
    use cairn_core::internal::mcp::handlers::RunContext;

    let ctx = RunContext {
        run_id: "r-1".to_string(),
        job_id: "job-1".to_string(),
        exec_seq: Some(1),
        issue_id: Some("issue-1".to_string()),
        issue_number: Some(1),
        thread_id: None,
        project_id: node.machines.project_id.clone(),
        project_key: "nsc".to_string(),
        job_name: Some("builder".to_string()),
        agent_config_id: None,
    };
    let selector = machine.map(|name| ExecutorSelector {
        name: Some(name.to_string()),
        ..ExecutorSelector::default()
    });
    let session = match repl::spawn_session(
        node.orch(),
        &ctx.job_id,
        &ctx.project_id,
        &node.cwd,
        Some(&ctx),
        ReplLang::Python,
        slug,
        &[],
        selector.as_ref(),
    )
    .await
    {
        Ok(session) => session,
        Err(error) if error.contains("python") || error.contains("uv") => {
            eprintln!("skipping: no Python to spawn a REPL with: {error}");
            return None;
        }
        Err(error) => panic!("the REPL did not start: {error}"),
    };
    node.orch()
        .repl_state
        .insert(ctx.job_id.clone(), slug.to_string(), session);
    let path = dump(node, slug);
    let code = format!(
        "import json, os\nopen({part:?}, 'w').write(json.dumps(dict(os.environ)))\nos.replace({part:?}, {path:?})",
        part = format!("{}.part", path.display()),
        path = path.display().to_string(),
    );
    let reply = node
        .run(
            "r-1",
            json!({ "commands": [{ "code": code, "interpreter": "python", "repl": slug }] }),
        )
        .await;
    assert!(
        path.is_file(),
        "the REPL did not write its environment: {reply}"
    );
    // Read before the REPL stops, while its capability is live.
    let observed = observed(&path).await;
    if let Some(session) = node.orch().repl_state.remove(&ctx.job_id, slug) {
        session.stop_and_release(node.orch()).await;
    }
    Some(observed)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_placed_repl_carries_no_callback_credential() {
    if common::skip_if_fenced("a_placed_repl_carries_no_callback_credential")
        || !launch_canary::in_planted_child(
            module_path!(),
            "a_placed_repl_carries_no_callback_credential",
        )
    {
        return;
    }
    let node = node().await;
    if let Some(env) = repl_env(&node, Some(MACHINE_A), "placed-repl").await {
        assert_clean("a placed REPL", &env);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_colocated_repl_carries_no_callback_credential() {
    if common::skip_if_fenced("a_colocated_repl_carries_no_callback_credential")
        || !launch_canary::in_planted_child(
            module_path!(),
            "a_colocated_repl_carries_no_callback_credential",
        )
    {
        return;
    }
    let node = node().await;
    only_colocated(&node).await;
    if let Some(env) = repl_env(&node, None, "colocated-repl").await {
        assert_clean("a colocated REPL", &env);
    }
}
