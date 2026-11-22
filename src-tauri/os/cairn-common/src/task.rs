//! Count live async tasks by runtime and spawning source location, without Tokio hooks.
//! Counts include queued, unpolled futures; blocking tasks and raw Tokio spawns
//! are not attributed. Local tasks use the currently entered runtime.
use std::collections::HashMap;
use std::future::Future;
use std::panic::Location;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use serde::Serialize;
use tokio::runtime::{Handle, Id};
use tokio::task::JoinHandle;

type Key = (Id, &'static Location<'static>);
type Registry = HashMap<Key, Arc<AtomicI64>>;

fn registry() -> &'static Mutex<Registry> {
    static REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

struct Guard(Arc<AtomicI64>);

impl Guard {
    fn new(handle: &Handle, location: &'static Location<'static>) -> Self {
        let mut registry = registry().lock().unwrap_or_else(|e| e.into_inner());
        let count = registry.entry((handle.id(), location)).or_default().clone();
        count.fetch_add(1, Ordering::Relaxed);
        Self(count)
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Spawn on the current runtime, retaining the external caller's location.
#[track_caller]
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    spawn_boxed_on(&Handle::current(), Box::pin(future))
}

/// Spawn on an explicit runtime, even when a different runtime is entered.
#[track_caller]
pub fn spawn_on<F>(handle: &Handle, future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    spawn_boxed_on(handle, Box::pin(future))
}

// Box at the public entry points, before forwarding or capturing the future:
// carrying it inline through those frames multiplies large callers' stack use.
#[track_caller]
fn spawn_boxed_on<F>(handle: &Handle, future: Pin<Box<F>>) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    // Construct outside the async body: cancellation before first poll must
    // retire the count too. Capturing the guard also covers spawn panics.
    let guard = Guard::new(handle, Location::caller());
    handle.spawn(async move {
        let _guard = guard;
        future.await
    })
}

/// Spawn a non-Send task on the current LocalSet, attributed to the entered
/// runtime. Like Tokio's spawn_local, panics outside a local execution context.
#[track_caller]
pub fn spawn_local<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + 'static,
    F::Output: 'static,
{
    let future = Box::pin(future);
    let guard = Guard::new(&Handle::current(), Location::caller());
    tokio::task::spawn_local(async move {
        let _guard = guard;
        future.await
    })
}

/// A source location with live counted futures.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SiteSnapshot {
    pub file: &'static str,
    pub line: u32,
    pub column: u32,
    pub live: i64,
}

/// Runtime-scoped census. Fields are sampled, not an atomic view of Tokio's
/// scheduler; completion can race reads and local tasks need not be included
/// in Tokio's alive-task metric.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Snapshot {
    pub total_live: i64,
    pub top_sites: Vec<SiteSnapshot>,
    pub remainder_live: i64,
    pub runtime_alive_tasks: usize,
}

/// Read all counted sites on this runtime and retain at most `limit` leaders.
/// Zero counters (including retired runtimes) are pruned without retaining
/// runtime handles or futures. Totals include sites omitted by the limit.
pub fn snapshot(handle: &Handle, limit: usize) -> Snapshot {
    let mut registry = registry().lock().unwrap_or_else(|e| e.into_inner());
    let mut sites = Vec::new();
    registry.retain(|(runtime, location), count| {
        let live = count.load(Ordering::Relaxed);
        if *runtime == handle.id() && live > 0 {
            sites.push(SiteSnapshot {
                file: location.file(),
                line: location.line(),
                column: location.column(),
                live,
            });
        }
        live > 0
    });
    let total_live = sites.iter().map(|site| site.live).sum();
    sites.sort_by(|a, b| {
        b.live
            .cmp(&a.live)
            .then_with(|| (a.file, a.line, a.column).cmp(&(b.file, b.line, b.column)))
    });
    sites.truncate(limit);
    let top_live: i64 = sites.iter().map(|site| site.live).sum();
    Snapshot {
        total_live,
        top_sites: sites,
        remainder_live: total_live - top_live,
        runtime_alive_tasks: handle.metrics().num_alive_tasks(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::pending;

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
    }

    #[test]
    fn large_inline_future_fits_in_a_small_spawn_stack() {
        // Build off the measured stack so this isolates submission and polling,
        // not the caller's construction of the inline state.
        let state = [7_u8; 256 * 1024];
        let future = Box::new(async move {
            tokio::task::yield_now().await;
            std::hint::black_box(&state);
            state[0]
        });
        std::thread::Builder::new()
            .name("large counted spawn within a 1 MiB stack".into())
            .stack_size(1024 * 1024)
            .spawn(move || {
                let rt = runtime();
                let _entered = rt.enter();
                let task = spawn(*future);
                assert_eq!(snapshot(rt.handle(), 8).total_live, 1);
                assert_eq!(rt.block_on(task).unwrap(), 7);
                assert_eq!(snapshot(rt.handle(), 8).total_live, 0);
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn lifecycle_completion_panic_cancel_unpolled_and_shutdown() {
        let rt = runtime();
        let handle = rt.handle().clone();
        let unpolled = spawn_on(&handle, pending::<()>());
        assert_eq!(snapshot(&handle, 8).total_live, 1);
        unpolled.abort();
        rt.block_on(async {
            assert!(unpolled.await.unwrap_err().is_cancelled());
        });
        assert_eq!(snapshot(&handle, 8).total_live, 0);
        rt.block_on(async {
            assert_eq!(spawn(async { 7 }).await.unwrap(), 7);
            assert!(spawn(async { panic!("task panic") })
                .await
                .unwrap_err()
                .is_panic());
            let (started, ready) = tokio::sync::oneshot::channel();
            let running = spawn(async {
                started.send(()).unwrap();
                pending::<()>().await
            });
            ready.await.unwrap();
            assert_eq!(snapshot(&handle, 8).total_live, 1);
            running.abort();
            assert!(running.await.unwrap_err().is_cancelled());
        });
        assert_eq!(snapshot(&handle, 8).total_live, 0);
        let _task = spawn_on(&handle, pending::<()>());
        drop(rt);
        assert_eq!(snapshot(&handle, 8).total_live, 0);
    }

    #[test]
    fn separates_runtimes_and_reports_all_sites_beyond_limit() {
        let a = runtime();
        let b = runtime();
        let _entered = a.enter();
        let _one = spawn_on(b.handle(), pending::<()>());
        let location_line = line!() + 1;
        let tasks: Vec<_> = (0..3).map(|_| spawn(pending::<()>())).collect();
        let _other = spawn(pending::<()>());
        let snap = snapshot(a.handle(), 1);
        assert_eq!(snap.total_live, 4);
        assert_eq!(snap.top_sites.len(), 1);
        assert_eq!(snap.top_sites[0].live, 3);
        assert_eq!(snap.top_sites[0].line, location_line);
        assert_eq!(snap.remainder_live, 1);
        assert_eq!(snap.runtime_alive_tasks, a.metrics().num_alive_tasks());
        assert_eq!(snapshot(b.handle(), 8).total_live, 1);
        assert_eq!(snapshot(a.handle(), 0).remainder_live, 4);
        drop(tasks);
    }

    #[test]
    fn local_tasks_and_spawn_panic_retire_guards() {
        let rt = runtime();
        let _entered = rt.enter();
        let panic = std::panic::catch_unwind(|| spawn_local(pending::<()>()));
        assert!(panic.is_err());
        assert_eq!(snapshot(rt.handle(), 8).total_live, 0);
        let local = tokio::task::LocalSet::new();
        local.block_on(&rt, async {
            let value = std::rc::Rc::new(7);
            let task = spawn_local(async move { *value });
            assert_eq!(snapshot(rt.handle(), 8).total_live, 1);
            assert_eq!(task.await.unwrap(), 7);
        });
        assert_eq!(snapshot(rt.handle(), 8).total_live, 0);
        local.block_on(&rt, async {
            let _task = spawn_local(pending::<()>());
        });
        assert_eq!(snapshot(rt.handle(), 8).total_live, 1);
        drop(local);
        assert_eq!(snapshot(rt.handle(), 8).total_live, 0);
    }
}
