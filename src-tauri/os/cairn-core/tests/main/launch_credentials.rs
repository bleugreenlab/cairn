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
use crate::node_scratch_acceptance::{eventually, node, Node, MACHINE_A, MACHINE_B};

use cairn_common::auth::launch_canary;
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
    let shell = node.shell("r-1", machine, &command);
    tokio::pin!(shell);
    // The item holds until its environment has been read, so a reply that
    // arrives first means the item ended without writing it, and the reply is
    // the only account of why.
    tokio::select! {
        reply = &mut shell => panic!("the item ended without writing its environment: {reply}"),
        () = eventually("the process's environment", || path.is_file()) => {}
    }
    let (_reply, observed) = tokio::join!(shell, observed(&path));
    observed
}

#[tokio::test(flavor = "multi_thread")]
async fn a_colocated_run_shell_item_carries_no_callback_credential() {
    // The colocated executor wraps the item in a Seatbelt profile, which a
    // process that cannot nest one sees refused at `sandbox_apply`: the item
    // never runs, so there is no environment to search.
    if common::skip_if_fenced("a_colocated_run_shell_item_carries_no_callback_credential")
        || cairn_sandbox::skip_without_nested_sandbox(
            "a_colocated_run_shell_item_carries_no_callback_credential",
        )
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
