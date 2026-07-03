use std::future::Future;

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
/// multi-thread flavor; plain sync callers (desktop Tauri commands, tests) take
/// the direct path unchanged.
pub(crate) fn run_db_blocking<T, F, Fut>(make_future: F) -> Result<T, String>
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
                .map_err(|_| "Database task panicked".to_string())?
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

    #[tokio::test]
    async fn works_on_current_thread_runtime() {
        // block_in_place would panic here; the flavor guard must route the
        // direct path (safe: the future runs on its own thread + runtime).
        let out = run_db_blocking(|| async { Ok::<_, String>(13) });
        assert_eq!(out, Ok(13));
    }
}
