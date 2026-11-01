use std::any::Any;
use std::future::Future;

/// The text a panic carried, recovered from the payload a caught unwind hands
/// back.
///
/// A panic's message is the whole of what it tells you, and it lives only in
/// this box: the standard hook prints it to stderr, which a packaged app does
/// not keep. Every `join()`/`catch_unwind()` site that discards the payload
/// reports that some thread "panicked" and nothing about why — an assertion
/// deep in the database engine and a `None` unwrapped in our own code arrive at
/// the reader identically. Recovering the string costs two downcasts and is the
/// difference between a diagnosable failure and a mystery.
pub fn panic_message(payload: &(dyn Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "panic payload was not a string".to_string()
    }
}

/// Run an async DB operation to completion from synchronous code.
///
/// The future runs on a fresh scoped thread with its own current-thread
/// runtime; the caller blocks until it finishes. When the caller is itself a
/// worker of a multi-threaded tokio runtime (the runner's axum handlers reach
/// the sync facades directly), that park is poison: each call idles one worker
/// for the full DB round-trip, and under agent load the whole runtime — HTTP
/// surface, health endpoint, everything — starves. `block_in_place` tells the
/// runtime to migrate its other tasks off this worker first, so the park costs
/// one thread instead of one runtime.
///
/// `block_in_place` panics on a current-thread runtime, so it is gated to the
/// multi-thread flavor; plain sync callers (desktop Tauri commands, tests) and
/// a bridge nested inside another bridge's current-thread runtime take the
/// direct path unchanged.
///
/// This is the one sync-to-async database bridge. Domain wrappers (such as
/// `run_advancement_db`) delegate here rather than spawning and joining a
/// thread of their own: a plain join on a worker holds the worker's core as
/// well as its thread, so the runtime runs one worker short for the whole call.
/// A DAG reduction waiting on jj behind the store lock, repeated across enough
/// workers, starved the runner until open write transactions stopped reaching
/// their commits.
pub fn run_db_blocking<T, F, Fut>(make_future: F) -> Result<T, String>
where
    T: Send,
    F: FnOnce() -> Fut + Send,
    Fut: Future<Output = Result<T, String>>,
{
    let run = move || {
        std::thread::scope(|scope| {
            scope
                .spawn(move || {
                    tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|e| format!("Failed to start database runtime: {e}"))?
                        .block_on(make_future())
                })
                .join()
                .map_err(|payload| {
                    format!("Database task panicked: {}", panic_message(&*payload))
                })?
        })
    };

    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(run)
        }
        _ => run(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn works_outside_any_runtime() {
        let out = run_db_blocking(|| async { Ok::<_, String>(7) });
        assert_eq!(out, Ok(7));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn works_on_multi_thread_runtime_worker() {
        // The block_in_place path: must not panic and must not deadlock.
        let out = run_db_blocking(|| async { Ok::<_, String>(11) });
        assert_eq!(out, Ok(11));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn works_inside_spawn_blocking() {
        // Trigger dispatch reaches the sync facades from spawn_blocking
        // threads; block_in_place must tolerate a non-worker runtime thread.
        let out = tokio::task::spawn_blocking(|| run_db_blocking(|| async { Ok::<_, String>(17) }))
            .await
            .unwrap();
        assert_eq!(out, Ok(17));
    }

    #[test]
    fn panic_message_recovers_both_payload_shapes() {
        let literal = std::panic::catch_unwind(|| panic!("a &'static str payload")).unwrap_err();
        assert_eq!(panic_message(&*literal), "a &'static str payload");

        let formatted =
            std::panic::catch_unwind(|| panic!("a String payload: {}", 1 + 1)).unwrap_err();
        assert_eq!(panic_message(&*formatted), "a String payload: 2");
    }

    #[test]
    fn a_panicking_future_reports_what_it_panicked_with() {
        let out = run_db_blocking(|| async {
            panic!("dirty pages should be empty for read txn");
            #[allow(unreachable_code)]
            Ok::<_, String>(0)
        });
        let error = out.unwrap_err();
        assert!(
            error.contains("dirty pages should be empty for read txn"),
            "panic message must survive the join: {error}"
        );
    }

    /// With one worker, a plain spawn-and-join from a runtime task would park
    /// the only worker thread with its core, and no timer or other task would
    /// run until the call returned. The handoff keeps the runtime running.
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn a_call_from_a_worker_leaves_the_runtime_running() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        use std::time::Duration;

        let ticks = Arc::new(AtomicUsize::new(0));
        let ticker = tokio::spawn({
            let ticks = ticks.clone();
            async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    ticks.fetch_add(1, Ordering::SeqCst);
                }
            }
        });
        // Spawned, not awaited in the test body: the body runs inside
        // `block_on`, not on the worker this needs to occupy.
        let ticked_during_call = tokio::spawn(async move {
            run_db_blocking(move || async move {
                let before = ticks.load(Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(300)).await;
                Ok::<_, String>(ticks.load(Ordering::SeqCst) - before)
            })
        })
        .await
        .unwrap()
        .unwrap();
        ticker.abort();
        assert!(
            ticked_during_call > 0,
            "the runtime's other tasks must keep running while a worker waits on the bridge"
        );
    }

    #[tokio::test]
    async fn works_on_current_thread_runtime() {
        // block_in_place would panic here; the flavor guard must route the
        // direct path (safe: the future runs on its own thread + runtime).
        let out = run_db_blocking(|| async { Ok::<_, String>(13) });
        assert_eq!(out, Ok(13));
    }
}
