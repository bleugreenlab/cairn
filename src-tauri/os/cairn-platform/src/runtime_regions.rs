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
//!
//! A worker that has not parked for a minute is either executing for a minute
//! or was left off the CPU for most of one, and the two call for opposite
//! responses: a task dump for the first, less load on the host for the second.
//! Each slot therefore carries a handle to its thread's CPU clock, captured on
//! the thread when it registers, and the observer samples it on its own tick
//! ([`sample_cpu`]), keeping a baseline per unpark. A report then says how much
//! of a worker's unparked time it actually spent on a CPU, at no cost to the
//! worker's park and unpark path.

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
    cpu_clock: ThreadCpuClock,
    /// The interval [`sample_cpu`] last baselined: the `running_since` of a
    /// worker, or the innermost region's entry for a thread that never parks.
    cpu_epoch: AtomicU64,
    /// The thread's CPU time, in nanoseconds, when that interval was first
    /// sampled. Accurate to one observer tick.
    cpu_baseline: AtomicU64,
}

impl ThreadSlot {
    /// The start of whatever interval a report about this thread covers.
    fn epoch(&self) -> u64 {
        let running_since = self.running_since.load(Ordering::Acquire);
        if running_since != 0 {
            return running_since;
        }
        self.regions
            .try_lock()
            .ok()
            .and_then(|regions| regions.last().map(|region| region.entered_at))
            .unwrap_or(0)
    }
}

/// A handle another thread can read this thread's CPU time through.
struct ThreadCpuClock {
    #[cfg(target_os = "linux")]
    clock: Option<libc::clockid_t>,
    /// The thread's mach port, or zero once the thread has released it.
    #[cfg(target_os = "macos")]
    port: std::sync::atomic::AtomicU32,
}

#[cfg(target_os = "macos")]
mod mach {
    #[repr(C)]
    #[derive(Default)]
    pub(super) struct TimeValue {
        pub seconds: i32,
        pub microseconds: i32,
    }

    #[repr(C)]
    #[derive(Default)]
    pub(super) struct ThreadBasicInfo {
        pub user_time: TimeValue,
        pub system_time: TimeValue,
        pub cpu_usage: i32,
        pub policy: i32,
        pub run_state: i32,
        pub flags: i32,
        pub suspend_count: i32,
        pub sleep_time: i32,
    }

    pub(super) const THREAD_BASIC_INFO: u32 = 3;

    extern "C" {
        pub(super) static mach_task_self_: u32;
        pub(super) fn mach_thread_self() -> u32;
        pub(super) fn mach_port_deallocate(task: u32, name: u32) -> i32;
        pub(super) fn thread_info(thread: u32, flavor: u32, info: *mut i32, count: *mut u32)
            -> i32;
    }
}

impl ThreadCpuClock {
    /// The calling thread's clock. Must run on the thread it describes.
    fn current() -> Self {
        #[cfg(target_os = "linux")]
        {
            let mut clock: libc::clockid_t = 0;
            // SAFETY: writes one clockid for the calling thread.
            let ok = unsafe { libc::pthread_getcpuclockid(libc::pthread_self(), &mut clock) } == 0;
            Self {
                clock: ok.then_some(clock),
            }
        }
        #[cfg(target_os = "macos")]
        {
            // SAFETY: returns a send right to the calling thread, released in
            // `release`.
            let port = unsafe { mach::mach_thread_self() };
            Self {
                port: std::sync::atomic::AtomicU32::new(port),
            }
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        Self {}
    }

    /// CPU time the thread has used, in nanoseconds, or `None` when this
    /// platform has no reading or the thread is gone.
    fn read_ns(&self) -> Option<u64> {
        #[cfg(target_os = "linux")]
        {
            let clock = self.clock?;
            let mut now = libc::timespec {
                tv_sec: 0,
                tv_nsec: 0,
            };
            // SAFETY: reads one clock into a local.
            (unsafe { libc::clock_gettime(clock, &mut now) } == 0).then(|| {
                (now.tv_sec as u64)
                    .saturating_mul(1_000_000_000)
                    .saturating_add(now.tv_nsec as u64)
            })
        }
        #[cfg(target_os = "macos")]
        {
            let port = self.port.load(Ordering::Acquire);
            if port == 0 {
                return None;
            }
            let mut info = mach::ThreadBasicInfo::default();
            let mut count = (std::mem::size_of::<mach::ThreadBasicInfo>() / 4) as u32;
            // SAFETY: `info` is a correctly sized thread_basic_info and
            // `count` its length in natural_t words.
            let status = unsafe {
                mach::thread_info(
                    port,
                    mach::THREAD_BASIC_INFO,
                    (&mut info as *mut mach::ThreadBasicInfo).cast(),
                    &mut count,
                )
            };
            let micros = |time: &mach::TimeValue| {
                (time.seconds.max(0) as u64) * 1_000_000 + time.microseconds.max(0) as u64
            };
            (status == 0).then(|| (micros(&info.user_time) + micros(&info.system_time)) * 1_000)
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        None
    }

    /// Give the handle back. Runs on the thread itself as it exits. A reader
    /// racing this sees zero and takes no reading.
    fn release(&self) {
        #[cfg(target_os = "macos")]
        {
            let port = self.port.swap(0, Ordering::AcqRel);
            if port != 0 {
                // SAFETY: releases the send right `current` took.
                unsafe {
                    mach::mach_port_deallocate(mach::mach_task_self_, port);
                }
            }
        }
    }
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
    /// Not the duration of any one task: a worker that keeps finding ready
    /// work never parks.
    pub running_for: Option<Duration>,
    /// CPU time the thread used over the interval reported (since it
    /// unparked, or since its region began), when the platform can say.
    pub cpu: Option<Duration>,
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
        self.0.cpu_clock.release();
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
                cpu_clock: ThreadCpuClock::current(),
                cpu_epoch: AtomicU64::new(0),
                cpu_baseline: AtomicU64::new(0),
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

fn slots_of(runtime: &str) -> Vec<Arc<ThreadSlot>> {
    match REGISTRY.lock() {
        Ok(registry) => registry
            .iter()
            .filter(|slot| slot.runtime.get().is_some_and(|name| *name == runtime))
            .cloned()
            .collect(),
        Err(_) => Vec::new(),
    }
}

/// CPU this whole process has used, user and system, from `getrusage`, or
/// `None` where it cannot be read.
pub fn process_cpu_time() -> Option<Duration> {
    #[cfg(unix)]
    {
        // SAFETY: fills one rusage for the calling process.
        let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
        if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) } != 0 {
            return None;
        }
        let total = |time: libc::timeval| {
            Duration::from_secs(time.tv_sec.max(0) as u64)
                + Duration::from_micros(time.tv_usec.max(0) as u64)
        };
        Some(total(usage.ru_utime) + total(usage.ru_stime))
    }
    #[cfg(not(unix))]
    None
}

/// Baseline the CPU clock of every thread of `runtime` whose reporting
/// interval has started since the last call. The watchdog calls this on each
/// of its ticks, which bounds a report's CPU error by one tick.
pub fn sample_cpu(runtime: &str) {
    for slot in slots_of(runtime) {
        let epoch = slot.epoch();
        if epoch != 0 && slot.cpu_epoch.load(Ordering::Acquire) != epoch {
            if let Some(cpu) = slot.cpu_clock.read_ns() {
                slot.cpu_baseline.store(cpu, Ordering::Release);
                slot.cpu_epoch.store(epoch, Ordering::Release);
            }
        }
    }
}

/// Every thread of `runtime` that is running or inside a region, for a stall
/// report. A slot whose lock is momentarily held by its own thread reports no
/// region rather than waiting for it.
pub fn runtime_threads(runtime: &str) -> Vec<ThreadReport> {
    slots_of(runtime)
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
            let epoch = slot.epoch();
            let cpu = (epoch != 0 && slot.cpu_epoch.load(Ordering::Acquire) == epoch)
                .then(|| slot.cpu_clock.read_ns())
                .flatten()
                .map(|now| {
                    Duration::from_nanos(
                        now.saturating_sub(slot.cpu_baseline.load(Ordering::Acquire)),
                    )
                });
            (running_for.is_some() || region.is_some()).then(|| ThreadReport {
                thread_name: slot.thread_name.clone(),
                running_for,
                cpu,
                region,
            })
        })
        .collect()
}

/// `name(unparked_ms=N cpu_ms=N off_cpu_ms=N region=label[detail]
/// region_ms=N)` per thread, joined by `; `, or `none`. One token-friendly field
/// for a log line.
///
/// `off_cpu_ms` is the part of the interval the thread was not on a CPU: near
/// zero for a worker genuinely executing, near the whole interval for one the
/// host left waiting. The CPU fields are absent where the platform cannot say.
pub fn format_thread_reports(reports: &[ThreadReport]) -> String {
    if reports.is_empty() {
        return "none".to_string();
    }
    reports
        .iter()
        .map(|report| {
            let unparked = report
                .running_for
                .map(|running| running.as_millis().to_string())
                .unwrap_or_else(|| "parked".to_string());
            let interval = report
                .running_for
                .or(report.region.as_ref().map(|region| region.elapsed));
            let cpu = match (report.cpu, interval) {
                (Some(cpu), Some(interval)) => format!(
                    " cpu_ms={} off_cpu_ms={}",
                    cpu.as_millis(),
                    interval.saturating_sub(cpu).as_millis()
                ),
                _ => String::new(),
            };
            let region = match &report.region {
                Some(region) => format!(
                    "region={}{} region_ms={}",
                    region.label,
                    region
                        .detail
                        .as_ref()
                        .map(|detail| format!("[{detail}]"))
                        .unwrap_or_default(),
                    region.elapsed.as_millis(),
                ),
                None => "region=none".to_string(),
            };
            format!(
                "{}(unparked_ms={unparked}{cpu} {region})",
                report.thread_name
            )
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

    /// The discriminator the watchdog exists for. A worker spinning reads as
    /// on-CPU for its whole interval; a worker asleep reads as off-CPU for it.
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn a_spinning_worker_reads_on_cpu_and_a_sleeping_one_reads_off_cpu() {
        fn measure(runtime: &'static str, spin: bool) -> (Duration, Duration) {
            let (step_tx, step_rx) = mpsc::channel();
            let (go_tx, go_rx) = mpsc::channel::<()>();
            let thread = spawn_registered(runtime, move || {
                mark_running();
                step_tx.send(()).unwrap();
                let until = Instant::now() + Duration::from_millis(300);
                while Instant::now() < until {
                    if spin {
                        std::hint::spin_loop();
                    } else {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                }
                step_tx.send(()).unwrap();
                go_rx.recv().unwrap();
            });
            step_rx.recv().unwrap();
            sample_cpu(runtime);
            step_rx.recv().unwrap();
            let report = runtime_threads(runtime).remove(0);
            go_tx.send(()).unwrap();
            thread.join().unwrap();
            (
                report.running_for.unwrap(),
                report.cpu.expect("a CPU reading"),
            )
        }
        let (unparked, cpu) = measure("regions-spinning", true);
        assert!(
            cpu >= unparked.mul_f32(0.5),
            "spinning: cpu {cpu:?} of {unparked:?}"
        );
        let (unparked, cpu) = measure("regions-sleeping", false);
        assert!(
            cpu <= unparked.mul_f32(0.25),
            "sleeping: cpu {cpu:?} of {unparked:?}"
        );
        let report = ThreadReport {
            thread_name: "w".into(),
            running_for: Some(Duration::from_millis(1_000)),
            cpu: Some(Duration::from_millis(30)),
            region: None,
        };
        assert_eq!(
            format_thread_reports(&[report]),
            "w(unparked_ms=1000 cpu_ms=30 off_cpu_ms=970 region=none)"
        );
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
