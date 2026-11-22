//! A running batch keeps its CLI relay across a link bounce of the machine
//! running it (cairn/5631).
//!
//! The batch runs on a real, non-colocated `ExecutorRuntime` and reaches the
//! runner as the `cairn` CLI does: one HTTP call per `cairn read cairn:~`, to
//! its relay over the lineage socket, authorized by the runner's own
//! `authorize_mcp_relay` for the batch's context. The bounce is the runner's
//! side of a real one: the link closes (`relay_link_closed`, then the
//! disconnect), the machine attaches again at a new generation, and its first
//! frame names what it still serves (`reattach_relays`).
//!
//! The batch prints nothing while a bounce is in progress, so these tests
//! depend on neither reading of output during a gap; only on a call waiting
//! for the reattach.
//!
//! Like the other executor-cell suites these cannot run nested inside a Cairn
//! worktree fence, and each records a declared skip there (skip-manifest.toml).
//! The batch also spawns wrapped in a Seatbelt profile on macOS, so a process
//! that cannot nest one records a declared skip as well.

use crate::common;
use crate::node_scratch_acceptance::{node, MACHINE_A};

use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;

/// A batch that makes `reads` relayed `cairn read cairn:~` calls, the `i`th
/// once the test writes `go-<i>`, leaving each call's status and body behind.
fn reading_batch(dir: &Path, reads: usize) -> String {
    let dir = dir.display();
    format!(
        r#"for i in $(seq 1 {reads}); do
  while [ ! -f "{dir}/go-$i" ]; do sleep 0.05; done
  curl -sS -o "{dir}/body-$i" -w '%{{http_code}}' --unix-socket "$CAIRN_CALLBACK_SOCKET" \
    -H 'content-type: application/json' \
    --data '{{"cwd":"/tmp","run_id":"r-1","tool":"read","payload":{{"paths":["cairn:~"]}}}}' \
    http://cairn/api/mcp > "{dir}/status-$i.part"
  mv "{dir}/status-$i.part" "{dir}/status-$i"
done"#
    )
}

fn handshake_dir(node: &crate::node_scratch_acceptance::Node) -> PathBuf {
    let dir = node.machines.temp.path().join("handshake");
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn go(dir: &Path, read: usize) {
    std::fs::write(dir.join(format!("go-{read}")), "").unwrap();
}

/// The `read`th call's status and body, once it has answered. A batch that
/// ends first, having failed to start or died before the call, is reported by
/// its own reply rather than as a call that went unanswered.
async fn answered(dir: &Path, read: usize, batch: &mut JoinHandle<String>) -> (String, String) {
    let status = dir.join(format!("status-{read}"));
    let settled = || {
        std::fs::read_to_string(&status).ok().map(|code| {
            let body =
                std::fs::read_to_string(dir.join(format!("body-{read}"))).unwrap_or_default();
            (code.trim().to_owned(), body)
        })
    };
    for _ in 0..1_200 {
        if let Some(answer) = settled() {
            return answer;
        }
        if batch.is_finished() {
            // The batch may have written this status just before it ended.
            if let Some(answer) = settled() {
                return answer;
            }
            panic!(
                "read {read} never ran: the batch ended first: {:?}",
                batch.await
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("read {read} never answered");
}

/// Two bounces under one running batch. A call made in each gap waits for the
/// reattach and then succeeds; nothing is refused, and the batch's result is
/// accepted at the end on the link it finished under.
#[tokio::test(flavor = "multi_thread")]
async fn a_running_batch_keeps_its_relay_across_link_bounces() {
    if common::skip_if_fenced("a_running_batch_keeps_its_relay_across_link_bounces")
        || cairn_sandbox::skip_without_nested_sandbox(
            "a_running_batch_keeps_its_relay_across_link_bounces",
        )
    {
        return;
    }
    let node = Arc::new(node().await);
    let dir = handshake_dir(&node);
    let mut batch = cairn_common::task::spawn({
        let node = node.clone();
        let command = reading_batch(&dir, 3);
        async move { node.shell("r-1", Some(MACHINE_A), &command).await }
    });

    go(&dir, 1);
    let (status, body) = answered(&dir, 1, &mut batch).await;
    assert_eq!(status, "200", "before any bounce: {body}");

    for read in 2..=3 {
        let generation = common::drop_link(node.orch(), MACHINE_A);
        go(&dir, read);
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            !dir.join(format!("status-{read}")).exists(),
            "a call in the gap waits for the reattach"
        );
        common::reannounce_link(node.orch(), MACHINE_A);
        let (status, body) = answered(&dir, read, &mut batch).await;
        assert_eq!(status, "200", "read {read}, on link {generation}: {body}");
    }

    let reply = tokio::time::timeout(Duration::from_secs(120), batch)
        .await
        .expect("the batch finished")
        .unwrap();
    assert!(
        reply.starts_with(&format!("Ran on **{MACHINE_A}**")),
        "the batch's result was accepted: {reply}"
    );
}

/// A machine that comes back without naming the batch leaves it detached: its
/// next call waits the bound and is then told why, as a retryable 503.
#[tokio::test(flavor = "multi_thread")]
async fn a_batch_whose_executor_never_reattaches_is_told_its_relay_is_detached() {
    if common::skip_if_fenced(
        "a_batch_whose_executor_never_reattaches_is_told_its_relay_is_detached",
    ) || cairn_sandbox::skip_without_nested_sandbox(
        "a_batch_whose_executor_never_reattaches_is_told_its_relay_is_detached",
    ) {
        return;
    }
    // Shortened for this test's sake; a bounce in another test reattaches far
    // inside it.
    common::RELAY_GAP_WAIT_MS.store(2_000, Ordering::Relaxed);
    let node = Arc::new(node().await);
    let dir = handshake_dir(&node);
    let mut batch = cairn_common::task::spawn({
        let node = node.clone();
        let command = reading_batch(&dir, 2);
        async move { node.shell("r-1", Some(MACHINE_A), &command).await }
    });

    go(&dir, 1);
    let (status, body) = answered(&dir, 1, &mut batch).await;
    assert_eq!(status, "200", "before the bounce: {body}");

    common::drop_link(node.orch(), MACHINE_A);
    go(&dir, 2);
    let (status, body) = answered(&dir, 2, &mut batch).await;
    assert_eq!(status, "503", "{body}");
    assert!(
        body.starts_with("relay detached by a link loss at ")
            && body.contains("; the executor has not reattached within "),
        "{body}"
    );

    let reply = tokio::time::timeout(Duration::from_secs(120), batch)
        .await
        .expect("the batch finished")
        .unwrap();
    assert!(
        reply.starts_with(&format!("Ran on **{MACHINE_A}**")),
        "{reply}"
    );
}
