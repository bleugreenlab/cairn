//! Blocking work that finishes before the caller's poll returns.

/// Run blocking `work` to completion before returning, without ever leaving it
/// running detached from the caller.
///
/// Unlike `spawn_blocking`, this leaves nothing running once the caller's
/// future is dropped. A future that reaches this call with no `.await` around
/// it completes in the poll that started it, so whatever the caller borrows or
/// holds, a store writer guard above all, outlives the work. A detached
/// blocking task would keep writing after a cancelled caller had released its
/// guard.
///
/// Where the work runs depends on the calling thread:
///
/// - **A multi-thread runtime's thread**: on this thread, inside
///   `block_in_place`, which first hands a worker's core to a replacement.
/// - **A current-thread runtime's thread**: on a scoped thread this one joins.
///   `block_in_place` panics there, and work that starts a runtime of its own
///   (as the store transactions do) cannot run on a thread already driving one.
/// - **Outside any runtime**: on this thread.
///
/// Either way the work occupies one thread for its length, the same cost
/// `spawn_blocking` pays. A panic in the work resumes on the caller.
pub fn in_place_blocking<T: Send>(work: impl FnOnce() -> T + Send) -> T {
    match tokio::runtime::Handle::try_current().map(|handle| handle.runtime_flavor()) {
        Ok(tokio::runtime::RuntimeFlavor::MultiThread) => tokio::task::block_in_place(work),
        Ok(_) => std::thread::scope(|scope| scope.spawn(work).join())
            .unwrap_or_else(|panic| std::panic::resume_unwind(panic)),
        Err(_) => work(),
    }
}

#[cfg(test)]
mod tests {
    use super::in_place_blocking;

    /// Work that starts its own runtime, as the store transactions do, and
    /// reports whether it ran on the calling thread.
    fn starts_a_runtime() -> bool {
        let caller = std::thread::current().id();
        in_place_blocking(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async { std::thread::current().id() == caller })
        })
    }

    #[test]
    fn outside_a_runtime_the_work_runs_on_this_thread() {
        assert!(starts_a_runtime());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn on_a_current_thread_runtime_the_work_runs_on_a_joined_thread() {
        assert!(!starts_a_runtime());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn on_a_multi_thread_worker_the_work_runs_in_place() {
        assert!(tokio::spawn(async { starts_a_runtime() }).await.unwrap());
    }

    #[tokio::test(flavor = "current_thread")]
    #[should_panic(expected = "the work's own panic")]
    async fn a_panic_in_the_work_reaches_the_caller() {
        in_place_blocking(|| panic!("the work's own panic"));
    }
}
