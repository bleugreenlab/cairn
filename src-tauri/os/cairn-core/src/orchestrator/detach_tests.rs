use super::spawn_onto;
use std::sync::mpsc;
use std::time::Duration;

fn must_not_fail() -> impl FnOnce() + Send + 'static {
    || panic!("on_spawn_failure must not fire when a runtime is available")
}

/// Off any runtime (the common turn-end case: the backends' stdout threads),
/// the task still runs to completion, on a runtime of its own.
#[test]
fn runs_without_any_runtime() {
    assert!(
        tokio::runtime::Handle::try_current().is_err(),
        "a plain #[test] must have no ambient runtime to exercise the detached-thread path"
    );
    let (tx, rx) = mpsc::channel();
    spawn_onto(
        None,
        async move {
            tx.send(()).unwrap();
        },
        must_not_fail(),
    );
    rx.recv_timeout(Duration::from_secs(5))
        .expect("detached task should complete without any runtime");
}

/// Given a runtime, the task runs there.
#[tokio::test]
async fn runs_on_the_runtime_it_is_given() {
    let (tx, rx) = tokio::sync::oneshot::channel();
    spawn_onto(
        Some(&tokio::runtime::Handle::current()),
        async move {
            let _ = tx.send(());
        },
        must_not_fail(),
    );
    tokio::time::timeout(Duration::from_secs(5), rx)
        .await
        .expect("detached task should complete on the given runtime")
        .expect("sender should not be dropped before signalling");
}

/// A detached task that spawns work for others and returns at once: the shape of
/// a turn-end wave that opens a shared execution, or releases a check ref, and
/// is then cancelled.
async fn spawns_a_child_that_reports_later(report: mpsc::Sender<()>) {
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = report.send(());
    });
}

/// Started from a thread with no runtime, onto a runtime that outlives it, the
/// task's children outlive the task.
#[test]
fn what_a_detached_task_spawns_outlives_it_on_a_lasting_runtime() {
    let host = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let (tx, rx) = mpsc::channel();
    std::thread::spawn({
        let host = host.handle().clone();
        move || {
            spawn_onto(
                Some(&host),
                spawns_a_child_that_reports_later(tx),
                must_not_fail(),
            )
        }
    })
    .join()
    .unwrap();
    rx.recv_timeout(Duration::from_secs(5))
        .expect("the child must finish after its parent returned");
}

/// The hazard the lasting runtime exists to avoid, pinned so the doc on
/// [`spawn_onto`] stays true: on a runtime built for the task, the task's
/// children are dropped unfinished as soon as the task returns.
#[test]
fn what_a_detached_task_spawns_dies_with_a_runtime_built_for_it() {
    let (tx, rx) = mpsc::channel();
    spawn_onto(None, spawns_a_child_that_reports_later(tx), must_not_fail());
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(5)),
        Err(mpsc::RecvTimeoutError::Disconnected),
        "the child is dropped, and its sender with it, when the parent's runtime drops"
    );
}
