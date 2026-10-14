//! What each thread is doing right now, readable from another thread.
//!
//! A stall watchdog observes a runtime from outside it, so it cannot read a
//! thread-local to learn what a blocked worker is in the middle of. Each thread
//! instead owns a slot in a process-wide registry: Tokio's park hooks keep the
//! slot's running clock, and [`enter`] records named regions of synchronous
//! work on it. The observer reads every slot of the runtime it watches and
//! names the work that kept a worker from parking.
//!
//! Writes are a thread-local lookup plus an uncontended lock on the thread's
//! own slot. The observer only ever `try_lock`s a slot, so it can neither block
//! a worker nor be blocked by one.

use std::cell::RefCell;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

struct ThreadSlot {
    thread_name: String,
    runtime: OnceLock<&'static str>,
    /// Nanoseconds since [`origin`] at which this worker last unparked; zero
    /// while parked, and always zero on a thread that is not a runtime worker.
    running_since: AtomicU64,
    regions: Mutex<Vec<ActiveRegion>>,
}

struct ActiveRegion {
    label: &'static str,
    detail: Option<Arc<str>>,
    entered_at: u64,
}

/// One thread as the observer sees it.
#[derive(Debug, Clone, PartialEq)]
pub struct ThreadReport {
    pub thread_name: String,
    /// How long this worker has gone without parking; `None` while parked.
    pub running_for: Option<Duration>,
    /// The innermost region the thread is in, if any.
    pub region: Option<RegionReport>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RegionReport {
    pub label: &'static str,
    pub detail: Option<Arc<str>>,
    pub elapsed: Duration,
}

static REGISTRY: Mutex<Vec<Arc<ThreadSlot>>> = Mutex::new(Vec::new());

fn origin() -> Instant {
    static ORIGIN: OnceLock<Instant> = OnceLock::new();
    *ORIGIN.get_or_init(Instant::now)
}

/// Never zero, so zero can mean "not running".
fn now_nanos() -> u64 {
    u64::try_from(origin().elapsed().as_nanos())
        .unwrap_or(u64::MAX)
        .max(1)
}

fn since(nanos: u64) -> Duration {
    Duration::from_nanos(now_nanos().saturating_sub(nanos))
}

/// Removes the thread's slot from the registry when the thread exits, so the
/// registry tracks live threads rather than every thread that ever ran.
struct Registration(Arc<ThreadSlot>);

impl Drop for Registration {
    fn drop(&mut self) {
        if let Ok(mut registry) = REGISTRY.lock() {
            registry.retain(|slot| !Arc::ptr_eq(slot, &self.0));
        }
    }
}

thread_local! {
    static SLOT: RefCell<Option<Registration>> = const { RefCell::new(None) };
}

fn with_slot<R>(f: impl FnOnce(&ThreadSlot) -> R) -> Option<R> {
    SLOT.try_with(|cell| {
        let mut cell = cell.borrow_mut();
        let registration = cell.get_or_insert_with(|| {
            let slot = Arc::new(ThreadSlot {
                thread_name: std::thread::current()
                    .name()
                    .unwrap_or("<unnamed>")
                    .to_string(),
                runtime: OnceLock::new(),
                running_since: AtomicU64::new(0),
                regions: Mutex::new(Vec::new()),
            });
            if let Ok(mut registry) = REGISTRY.lock() {
                registry.push(slot.clone());
            }
            Registration(slot)
        });
        f(&registration.0)
    })
    .ok()
}

/// Claim the current thread for `runtime`. Install as a Tokio builder's
/// `on_thread_start`, which runs on workers and blocking-pool threads alike.
pub fn register_runtime_thread(runtime: &'static str) {
    with_slot(|slot| {
        let _ = slot.runtime.set(runtime);
    });
}

/// The current worker resumed. Install as `on_thread_unpark`.
pub fn mark_running() {
    with_slot(|slot| slot.running_since.store(now_nanos(), Ordering::Release));
}

/// The current worker is about to park. Install as `on_thread_park`.
pub fn mark_parked() {
    with_slot(|slot| slot.running_since.store(0, Ordering::Release));
}

/// Marks the current thread as inside a named region until dropped. Regions
/// nest; dropping a guard restores whatever region enclosed it.
///
/// The guard is deliberately `!Send`: a region describes synchronous work on
/// one thread, so holding one across an `.await` is a compile error rather than
/// a report naming the wrong thread.
#[must_use = "the region ends when the guard is dropped"]
pub struct RegionGuard {
    depth: Option<usize>,
    _not_send: PhantomData<*const ()>,
}

impl Drop for RegionGuard {
    fn drop(&mut self) {
        if let Some(depth) = self.depth {
            with_slot(|slot| {
                if let Ok(mut regions) = slot.regions.lock() {
                    regions.truncate(depth);
                }
            });
        }
    }
}

pub fn enter(label: &'static str) -> RegionGuard {
    push_region(label, None)
}

/// [`enter`] with a detail naming the subject, such as the executor a frame
/// came from. An `Arc<str>` so a hot path tags without allocating.
pub fn enter_with(label: &'static str, detail: Arc<str>) -> RegionGuard {
    push_region(label, Some(detail))
}

fn push_region(label: &'static str, detail: Option<Arc<str>>) -> RegionGuard {
    let depth = with_slot(|slot| {
        let mut regions = slot.regions.lock().ok()?;
        let depth = regions.len();
        regions.push(ActiveRegion {
            label,
            detail,
            entered_at: now_nanos(),
        });
        Some(depth)
    })
    .flatten();
    RegionGuard {
        depth,
        _not_send: PhantomData,
    }
}

/// Every thread of `runtime` that is running or inside a region, for a stall
/// report. A slot whose lock is momentarily held by its own thread reports no
/// region rather than waiting for it.
pub fn runtime_threads(runtime: &str) -> Vec<ThreadReport> {
    let slots = match REGISTRY.lock() {
        Ok(registry) => registry
            .iter()
            .filter(|slot| slot.runtime.get().is_some_and(|name| *name == runtime))
            .cloned()
            .collect::<Vec<_>>(),
        Err(_) => return Vec::new(),
    };
    slots
        .iter()
        .filter_map(|slot| {
            let running_since = slot.running_since.load(Ordering::Acquire);
            let running_for = (running_since != 0).then(|| since(running_since));
            let region = slot.regions.try_lock().ok().and_then(|regions| {
                regions.last().map(|region| RegionReport {
                    label: region.label,
                    detail: region.detail.clone(),
                    elapsed: since(region.entered_at),
                })
            });
            (running_for.is_some() || region.is_some()).then(|| ThreadReport {
                thread_name: slot.thread_name.clone(),
                running_for,
                region,
            })
        })
        .collect()
}

/// `name(running_ms=N region=label[detail] region_ms=N)` per thread, joined by
/// `; `, or `none`. One token-friendly field for a log line.
pub fn format_thread_reports(reports: &[ThreadReport]) -> String {
    if reports.is_empty() {
        return "none".to_string();
    }
    reports
        .iter()
        .map(|report| {
            let running = report
                .running_for
                .map(|running| running.as_millis().to_string())
                .unwrap_or_else(|| "parked".to_string());
            match &report.region {
                Some(region) => format!(
                    "{}(running_ms={running} region={}{} region_ms={})",
                    report.thread_name,
                    region.label,
                    region
                        .detail
                        .as_ref()
                        .map(|detail| format!("[{detail}]"))
                        .unwrap_or_default(),
                    region.elapsed.as_millis(),
                ),
                None => format!("{}(running_ms={running} region=none)", report.thread_name),
            }
        })
        .collect::<Vec<_>>()
        .join("; ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    fn spawn_registered<F>(runtime: &'static str, body: F) -> std::thread::JoinHandle<()>
    where
        F: FnOnce() + Send + 'static,
    {
        std::thread::Builder::new()
            .name(format!("{runtime}-thread"))
            .spawn(move || {
                register_runtime_thread(runtime);
                body();
            })
            .unwrap()
    }

    #[test]
    fn nested_regions_restore_the_enclosing_one() {
        let (step_tx, step_rx) = mpsc::channel();
        let (go_tx, go_rx) = mpsc::channel::<()>();
        let thread = spawn_registered("regions-nesting", move || {
            let detail: Arc<str> = Arc::from("exec-1");
            let _outer = enter_with("outer", detail);
            {
                let _inner = enter("inner");
                step_tx.send(()).unwrap();
                go_rx.recv().unwrap();
            }
            step_tx.send(()).unwrap();
            go_rx.recv().unwrap();
        });

        step_rx.recv().unwrap();
        let reports = runtime_threads("regions-nesting");
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].region.as_ref().unwrap().label, "inner");

        go_tx.send(()).unwrap();
        step_rx.recv().unwrap();
        let reports = runtime_threads("regions-nesting");
        let region = reports[0].region.as_ref().unwrap();
        assert_eq!(region.label, "outer");
        assert_eq!(region.detail.as_deref(), Some("exec-1"));
        assert!(format_thread_reports(&reports).contains("region=outer[exec-1]"));

        go_tx.send(()).unwrap();
        thread.join().unwrap();
        assert!(
            runtime_threads("regions-nesting").is_empty(),
            "an exited thread leaves the registry"
        );
    }

    #[test]
    fn a_parked_thread_outside_any_region_is_not_reported() {
        let (step_tx, step_rx) = mpsc::channel();
        let (go_tx, go_rx) = mpsc::channel::<()>();
        let thread = spawn_registered("regions-parking", move || {
            mark_running();
            step_tx.send(()).unwrap();
            go_rx.recv().unwrap();
            mark_parked();
            step_tx.send(()).unwrap();
            go_rx.recv().unwrap();
        });

        step_rx.recv().unwrap();
        std::thread::sleep(Duration::from_millis(20));
        let reports = runtime_threads("regions-parking");
        assert_eq!(reports.len(), 1);
        assert!(reports[0].running_for.unwrap() >= Duration::from_millis(20));
        assert!(reports[0].region.is_none());
        assert!(format_thread_reports(&reports).contains("region=none"));

        go_tx.send(()).unwrap();
        step_rx.recv().unwrap();
        assert!(runtime_threads("regions-parking").is_empty());

        go_tx.send(()).unwrap();
        thread.join().unwrap();
    }

    #[test]
    fn threads_of_other_runtimes_are_not_reported() {
        let (step_tx, step_rx) = mpsc::channel();
        let (go_tx, go_rx) = mpsc::channel::<()>();
        let thread = spawn_registered("regions-elsewhere", move || {
            let _region = enter("busy");
            step_tx.send(()).unwrap();
            go_rx.recv().unwrap();
        });
        step_rx.recv().unwrap();
        assert!(runtime_threads("regions-watched").is_empty());
        assert_eq!(format_thread_reports(&[]), "none");
        go_tx.send(()).unwrap();
        thread.join().unwrap();
    }
}
