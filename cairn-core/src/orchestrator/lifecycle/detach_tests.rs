use super::detach_onto_runtime;
use std::sync::mpsc;
use std::time::Duration;

/// The regression test for the silently-dropped turn-end checks: drive
/// `detach_onto_runtime` from a plain OS thread with NO ambient Tokio runtime
/// (the common turn-end case — the backends' stdout threads) and assert the
/// future actually runs to completion. Before the fix this path did nothing.
#[test]
fn runs_future_without_ambient_runtime() {
    assert!(
        tokio::runtime::Handle::try_current().is_err(),
        "a plain #[test] must have no ambient runtime to exercise the detached-thread path"
    );
    let (tx, rx) = mpsc::channel();
    detach_onto_runtime(
        async move {
            tx.send(()).unwrap();
        },
        || panic!("on_spawn_failure must not fire when the runtime builds"),
    );
    rx.recv_timeout(Duration::from_secs(5))
        .expect("detached future should complete without an ambient runtime");
}

/// With an ambient runtime `detach_onto_runtime` takes the `tokio::spawn`
/// path; the future must still run to completion.
#[tokio::test]
async fn runs_future_with_ambient_runtime() {
    assert!(tokio::runtime::Handle::try_current().is_ok());
    let (tx, rx) = tokio::sync::oneshot::channel();
    detach_onto_runtime(
        async move {
            let _ = tx.send(());
        },
        || panic!("on_spawn_failure must not fire when the runtime builds"),
    );
    tokio::time::timeout(Duration::from_secs(5), rx)
        .await
        .expect("detached future should complete with an ambient runtime")
        .expect("sender should not be dropped before signalling");
}
