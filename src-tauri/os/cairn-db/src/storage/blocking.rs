use std::any::Any;
use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::OnceLock;

use cairn_platform::runtime_regions::hand_off_and_block;
use cairn_platform::stack::off_frame;
use tokio::runtime::{Handle, RuntimeFlavor};

static BRIDGE_RUNTIME: OnceLock<Handle> = OnceLock::new();

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

/// Name the runtime that plain threads bridge through for the life of the
/// process, so a call from one needs no thread and no runtime of its own.
///
/// The first installation wins. `Handle::block_on` cannot drive a current-thread
/// runtime's timers or IO, so a handle to one is refused and the process keeps
/// the scoped path, which is slower but correct. The runtime must have its time
/// and IO drivers enabled and outlive the callers that matter: once it has shut
/// down, a bridged timer panics, which [`run_db_blocking`] reports as an error.
pub fn install_bridge_runtime(handle: Handle) {
    if handle.runtime_flavor() != RuntimeFlavor::MultiThread {
        log::error!("Database bridge refused a runtime that is not multi-thread");
        return;
    }
    let _ = BRIDGE_RUNTIME.set(handle);
}

/// Run an async DB operation to completion from synchronous code.
///
/// This is the one sync-to-async database bridge. Domain wrappers (such as
/// `run_advancement_db`) delegate here rather than spawning and joining a
/// thread of their own. Where the future runs depends on the calling thread:
///
/// - **A thread of a multi-thread runtime** (a worker, a `spawn_blocking`
///   thread, a bridged future that reached a sync facade): on this thread,
///   inside `block_in_place` (through `runtime_regions::hand_off_and_block`,
///   so the stall watchdog stops counting the thread as a worker). Parking a
///   worker with its core idles the runtime
///   one worker short for the whole call, and enough of them at once starve the
///   runner until open write transactions stop reaching their commits.
///   `block_in_place` hands the core to a replacement first, so the park costs
///   one thread instead of one runtime.
/// - **A plain thread, once [`install_bridge_runtime`] has run** (a backend's
///   stdout reader): on this thread, under the installed runtime.
/// - **Inside a current-thread runtime, or a plain thread with nothing
///   installed** (a nested `#[tokio::test]`, desktop Tauri commands): on a fresh
///   scoped thread with its own current-thread runtime. `block_on` panics inside
///   a current-thread runtime, and `block_in_place` cannot hand off its core.
///
/// The first two spawn nothing, which matters because the reader threads call
/// this for every line a session prints. They do share the caller's stack,
/// thread-locals and task-locals with the future, which the scoped path does
/// not, and a timer in the future fires only when one of the runtime's workers
/// turns the driver, so a starved runtime delays it. The scoped path keeps the
/// future's timers on its private runtime.
///
/// A panic in the future comes back as `Err` on every path, carrying its
/// message, so it never unwinds through the caller.
pub fn run_db_blocking<T, F, Fut>(make_future: F) -> Result<T, String>
where
    T: Send,
    F: FnOnce() -> Fut + Send,
    Fut: Future<Output = Result<T, String>>,
{
    bridge(BRIDGE_RUNTIME.get(), make_future)
}

fn bridge<T, F, Fut>(installed: Option<&Handle>, make_future: F) -> Result<T, String>
where
    T: Send,
    F: FnOnce() -> Fut + Send,
    Fut: Future<Output = Result<T, String>>,
{
    match Handle::try_current() {
        Ok(current) if current.runtime_flavor() == RuntimeFlavor::MultiThread => {
            hand_off_and_block(|| on_this_thread(&current, make_future))
        }
        Ok(_) => on_a_scoped_thread(make_future),
        Err(_) => match installed {
            Some(runtime) => on_this_thread(runtime, make_future),
            None => on_a_scoped_thread(make_future),
        },
    }
}

fn on_this_thread<T, F, Fut>(runtime: &Handle, make_future: F) -> Result<T, String>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T, String>>,
{
    // Boxed because the future's state would otherwise land on the caller's
    // stack, which on a worker is already part-way down a task's poll.
    catch_unwind(AssertUnwindSafe(|| {
        runtime.block_on(off_frame(make_future))
    }))
    .map_err(|payload| database_task_panicked(&*payload))?
}

fn on_a_scoped_thread<T, F, Fut>(make_future: F) -> Result<T, String>
where
    T: Send,
    F: FnOnce() -> Fut + Send,
    Fut: Future<Output = Result<T, String>>,
{
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
            .map_err(|payload| database_task_panicked(&*payload))?
    })
}

fn database_task_panicked(payload: &(dyn Any + Send)) -> String {
    format!("Database task panicked: {}", panic_message(payload))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    const WORKER: &str = "bridge-test-worker";

    fn runner_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name(WORKER)
            .enable_all()
            .build()
            .unwrap()
    }

    #[test]
    fn works_outside_any_runtime() {
        let out = run_db_blocking(|| async { Ok::<_, String>(7) });
        assert_eq!(out, Ok(7));
    }

    #[test]
    fn a_plain_thread_with_nothing_installed_takes_a_scoped_thread() {
        let ran_on = bridge(None, || async { Ok(thread::current().id()) }).unwrap();
        assert_ne!(ran_on, thread::current().id());
    }

    /// The reader threads' hot path. A future that ran on the caller cannot
    /// have needed a thread, and a task it spawned running on a worker of the
    /// installed runtime shows that it found that runtime rather than building
    /// one. Its timer has to fire too, which only those workers can do.
    #[test]
    fn a_plain_thread_runs_the_future_on_itself_under_the_installed_runtime() {
        let runtime = runner_runtime();
        let caller = thread::current().id();
        for _ in 0..50 {
            let (ran_on, spawned_on) = bridge(Some(runtime.handle()), || async {
                tokio::time::sleep(Duration::from_millis(1)).await;
                let spawned_on =
                    tokio::spawn(async { thread::current().name().map(str::to_owned) })
                        .await
                        .unwrap();
                Ok((thread::current().id(), spawned_on))
            })
            .unwrap();
            assert_eq!(ran_on, caller);
            assert_eq!(spawned_on.as_deref(), Some(WORKER));
        }
    }

    /// A sync facade reached from inside a bridged future bridges again, now
    /// with the installed runtime entered on this thread.
    #[test]
    fn a_nested_bridge_stays_on_the_plain_thread() {
        let runtime = runner_runtime();
        let handle = runtime.handle().clone();
        let inner_ran_on = bridge(Some(runtime.handle()), move || async move {
            bridge(Some(&handle), || async {
                tokio::time::sleep(Duration::from_millis(1)).await;
                Ok(thread::current().id())
            })
        })
        .unwrap();
        assert_eq!(inner_ran_on, thread::current().id());
    }

    /// Nothing joins a thread on this path, so the panic has to be stopped
    /// here or it would unwind a reader thread and end its session's stream.
    #[test]
    fn a_panic_under_the_installed_runtime_comes_back_as_an_error() {
        let runtime = runner_runtime();
        let out: Result<(), String> = bridge(Some(runtime.handle()), || async {
            panic!("dirty pages should be empty for read txn");
            #[allow(unreachable_code)]
            Ok(())
        });
        let error = out.unwrap_err();
        assert!(
            error.contains("dirty pages should be empty for read txn"),
            "panic message must survive: {error}"
        );
        let next = bridge(Some(runtime.handle()), || async { Ok::<_, String>(23) });
        assert_eq!(next, Ok(23));
    }

    #[test]
    fn a_current_thread_runtime_is_refused_as_the_bridge_runtime() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        install_bridge_runtime(runtime.handle().clone());
        assert!(BRIDGE_RUNTIME.get().is_none());
    }

    /// The same call from a runtime worker spawns nothing either, and what the
    /// future spawns belongs to the worker's runtime, not one that dies with
    /// the call.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_worker_runs_the_future_on_itself_and_its_spawns_outlive_the_call() {
        let (caller, ran_on, spawned) = tokio::spawn(async {
            let caller = thread::current().id();
            let (ran_on, spawned) = run_db_blocking(|| async {
                let spawned = tokio::spawn(async {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    29
                });
                Ok::<_, String>((thread::current().id(), spawned))
            })
            .unwrap();
            (caller, ran_on, spawned)
        })
        .await
        .unwrap();
        assert_eq!(ran_on, caller);
        assert_eq!(spawned.await.unwrap(), 29);
    }

    /// Stopped short of the task, whose join would otherwise report the panic
    /// and lose the message, and short of the worker.
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn a_panic_on_a_worker_comes_back_as_an_error() {
        let out = tokio::spawn(async {
            run_db_blocking(|| async {
                panic!("a panic on the worker path");
                #[allow(unreachable_code)]
                Ok::<_, String>(0)
            })
        })
        .await
        .expect("the panic must not reach the task");
        assert!(out.unwrap_err().contains("a panic on the worker path"));
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
        // scoped path (safe: the future runs on its own thread + runtime).
        let out = run_db_blocking(|| async { Ok::<_, String>(13) });
        assert_eq!(out, Ok(13));

        let ran_on = run_db_blocking(|| async { Ok(thread::current().id()) }).unwrap();
        assert_ne!(ran_on, thread::current().id());
    }

    /// A current-thread runtime stays on the scoped path even with a runtime
    /// installed: `block_on` from inside it would panic.
    #[tokio::test]
    async fn a_current_thread_runtime_ignores_the_installed_runtime() {
        let runtime = runner_runtime();
        let ran_on = bridge(Some(runtime.handle()), || async {
            tokio::time::sleep(Duration::from_millis(1)).await;
            Ok(thread::current().id())
        })
        .unwrap();
        assert_ne!(ran_on, thread::current().id());
        // Dropping a runtime blocks, which a runtime worker may not do.
        runtime.shutdown_background();
    }
}
