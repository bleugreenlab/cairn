//! Fresh-pipe capacity: whether a new anonymous pipe on this host can hold what
//! a producer writes into it before its reader starts.
//!
//! On macOS a fresh pipe's buffer starts small and grows only while the
//! kernel's pipe-memory budget has room (`sys_pipe.c` gates growth on
//! `amountpipekva < maxpipekva`). When that budget is spent, every new pipe
//! holds about 512 bytes, and any program that writes more than that before its
//! reader runs (a Homebrew bash 5.3 here-document, `clang -v` under a build
//! service that drains late) blocks forever at near-zero CPU (cairn/6023).
//! Host memory says nothing about this: the host that read 512 B had 46 GiB
//! free. So it is its own admission floor, beside memory and disk.
//!
//! The module has three layers, each usable alone:
//!
//! - [`fill_until_full`] is the probe's arithmetic over an injectable writer,
//!   and [`sample_fresh_pipe`] runs it against a real pipe on this host;
//! - [`PipeHealth`] is the evidence one host has gathered and the pure policy
//!   over it ([`PipeHealth::assess`]), serializable so an executor can ship
//!   it and the runner can evaluate it against its own clock;
//! - [`PipeHealthMonitor`] owns one host's evidence and a dedicated sampler
//!   thread.
//!
//! A reading is not a reservation. Another process can spend the budget between
//! a sample and a launch, so admission rechecks at the last practical boundary
//! and nothing here claims to prevent external exhaustion.

use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::executor_protocol::{Measurement, MeasurementGap, MeasurementReading};

/// Every probe write is this size, from the first one onward. A large first
/// write can take the kernel's unconditional initial-allocation path and read
/// healthy on an exhausted host; small writes see the growth gate.
pub const PROBE_CHUNK_BYTES: usize = 512;
/// The probe stops once the pipe has accepted this much and reports the
/// figure as a lower bound. Enough to tell every threshold apart without
/// spending more kernel memory than one healthy pipe.
pub const PROBE_CAP_BYTES: u64 = 64 * 1024;
/// Write syscalls one probe may issue, `EINTR` retries included: twice the
/// chunks the cap needs.
pub const PROBE_MAX_ATTEMPTS: u32 = 2 * (PROBE_CAP_BYTES as u32 / PROBE_CHUNK_BYTES as u32);

/// How often a monitor samples.
pub const SAMPLE_INTERVAL_MS: u64 = 5_000;
/// A fresh reading below this enters the degraded state at once. Operational,
/// not a guarantee: 4 KiB is well above the observed 512 B failure and below
/// what a healthy macOS pipe takes (16-64 KiB).
pub const DEGRADED_BELOW_BYTES: u64 = 4_096;
/// A degraded host recovers only on readings at least this large.
pub const RECOVERED_AT_BYTES: u64 = 16_384;
/// Two spaced readings at or above [`RECOVERED_AT_BYTES`] clear the latch, so
/// one lucky sample during churn does not reopen admission.
pub const RECOVERY_SAMPLES: u8 = 2;
/// The least separation between the two recovery readings: one sampling
/// interval, less a second of scheduling jitter on the sampler's own sleep.
pub const RECOVERY_SPACING_MS: u64 = SAMPLE_INTERVAL_MS - 1_000;
/// A successful reading is evidence for this long.
pub const FRESH_FOR_MS: u64 = 30_000;
/// Once measured evidence expires, new work is held this long (two sampling
/// intervals) and then admitted with a named gap. Fixed from the last
/// successful reading, so repeated failures cannot extend it: expired evidence
/// never holds a host shut indefinitely.
pub const STALE_HOLD_MS: u64 = 2 * SAMPLE_INTERVAL_MS;

/// What one probe found: the bytes a fresh pipe accepted before it refused a
/// nonblocking write.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub struct PipeCapacity {
    pub accepted_bytes: u64,
    /// The probe stopped at [`PROBE_CAP_BYTES`] before the pipe filled, so the
    /// pipe holds *at least* `accepted_bytes`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub at_least: bool,
}

impl PipeCapacity {
    pub fn describe(&self) -> String {
        if self.at_least {
            format!("at least {} B", self.accepted_bytes)
        } else {
            format!("{} B", self.accepted_bytes)
        }
    }
}

// ── Probe ───────────────────────────────────────────────────────────────────

/// The one operation the probe needs from a pipe's write end, so a test can
/// script every syscall outcome.
pub trait ProbeWriter {
    fn write_chunk(&mut self, chunk: &[u8]) -> std::io::Result<usize>;
}

/// Write [`PROBE_CHUNK_BYTES`] chunks into a nonblocking pipe until it refuses
/// one, and count what it accepted.
///
/// Partial writes count for the bytes they took. `EAGAIN` ends the probe with
/// a measurement, including one of 0 B. A zero-byte write, any other error, or
/// running out of attempts is a sampling failure, never a reading.
pub fn fill_until_full(writer: &mut impl ProbeWriter) -> Result<PipeCapacity, String> {
    let chunk = [0u8; PROBE_CHUNK_BYTES];
    let mut accepted = 0u64;
    for _ in 0..PROBE_MAX_ATTEMPTS {
        if accepted >= PROBE_CAP_BYTES {
            break;
        }
        match writer.write_chunk(&chunk) {
            Ok(0) => return Err("a pipe write accepted zero bytes".into()),
            Ok(written) => accepted = accepted.saturating_add(written as u64),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                return Ok(PipeCapacity {
                    accepted_bytes: accepted,
                    at_least: false,
                });
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(format!("pipe write failed: {error}")),
        }
    }
    if accepted >= PROBE_CAP_BYTES {
        return Ok(PipeCapacity {
            accepted_bytes: accepted,
            at_least: true,
        });
    }
    Err(format!(
        "the pipe neither filled nor reached {PROBE_CAP_BYTES} B within {PROBE_MAX_ATTEMPTS} \
         write attempts"
    ))
}

/// Probe one fresh pipe on this host now.
///
/// No subprocess, shell, reader thread or here-document: the probe must not
/// depend on the resource it measures. Both descriptors are owned and close on
/// every path; the reader stays open and undrained for the whole probe, so a
/// write can only fill the pipe, never raise `SIGPIPE`.
#[cfg(target_os = "macos")]
pub fn sample_fresh_pipe(now_unix_ms: u64) -> Measurement<PipeCapacity> {
    match probe_macos() {
        Ok(capacity) => Measurement::measured(now_unix_ms, capacity),
        Err(detail) => {
            Measurement::unavailable_with(now_unix_ms, MeasurementGap::SamplingFailed, detail)
        }
    }
}

/// The policy is Darwin-specific: off macOS there is no reading, and the
/// monitor reports the floor as not applicable rather than as healthy.
#[cfg(not(target_os = "macos"))]
pub fn sample_fresh_pipe(now_unix_ms: u64) -> Measurement<PipeCapacity> {
    Measurement::unavailable(now_unix_ms, MeasurementGap::UnsupportedPlatform)
}

#[cfg(target_os = "macos")]
fn probe_macos() -> Result<PipeCapacity, String> {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

    struct FdWriter<'a>(&'a OwnedFd);
    impl ProbeWriter for FdWriter<'_> {
        fn write_chunk(&mut self, chunk: &[u8]) -> std::io::Result<usize> {
            // SAFETY: the descriptor is open for the borrow and the buffer is
            // valid for `chunk.len()` bytes.
            let written =
                unsafe { libc::write(self.0.as_raw_fd(), chunk.as_ptr().cast(), chunk.len()) };
            if written < 0 {
                Err(std::io::Error::last_os_error())
            } else {
                Ok(written as usize)
            }
        }
    }

    fn set_flag(
        fd: &OwnedFd,
        get: libc::c_int,
        set: libc::c_int,
        flag: libc::c_int,
    ) -> Result<(), String> {
        // SAFETY: fcntl on an owned, open descriptor with integer arguments.
        let flags = unsafe { libc::fcntl(fd.as_raw_fd(), get) };
        if flags < 0 || unsafe { libc::fcntl(fd.as_raw_fd(), set, flags | flag) } < 0 {
            return Err(format!("fcntl: {}", std::io::Error::last_os_error()));
        }
        Ok(())
    }

    let mut fds = [0 as libc::c_int; 2];
    // SAFETY: `pipe` writes two descriptors into the two-element array.
    if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
        return Err(format!("pipe: {}", std::io::Error::last_os_error()));
    }
    // SAFETY: both descriptors were just returned by `pipe` and nothing else
    // owns them. Owning them before any fallible step closes them on every path.
    let (reader, writer) = unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) };
    // macOS has no `pipe2`, so close-on-exec is set straight after creation,
    // as the standard library's own pipes are.
    set_flag(&reader, libc::F_GETFD, libc::F_SETFD, libc::FD_CLOEXEC)?;
    set_flag(&writer, libc::F_GETFD, libc::F_SETFD, libc::FD_CLOEXEC)?;
    set_flag(&writer, libc::F_GETFL, libc::F_SETFL, libc::O_NONBLOCK)?;
    let result = fill_until_full(&mut FdWriter(&writer));
    drop(writer);
    drop(reader);
    result
}

// ── Evidence and policy ─────────────────────────────────────────────────────

/// A successful probe and when it was taken.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub struct PipeCapacityObservation {
    pub measured_at_unix_ms: u64,
    pub capacity: PipeCapacity,
}

/// Everything one host has measured about its fresh pipes, and the pure
/// admission policy over it.
///
/// It is the evidence, not a verdict: [`assess`](Self::assess) takes the
/// caller's clock, so an executor can ship this and the runner judges its age
/// at the moment it decides, not at the moment it was sent.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub struct PipeHealth {
    /// Whether the floor applies on this host's platform at all.
    pub applies: bool,
    /// The latest successful probe. A failed refresh never replaces or
    /// re-stamps it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_capacity: Option<PipeCapacityObservation>,
    /// The latest attempt, success or failure, for diagnosis.
    #[serde(default)]
    pub last_attempt: Measurement<PipeCapacity>,
    /// Latched below [`DEGRADED_BELOW_BYTES`] and cleared only by
    /// [`RECOVERY_SAMPLES`] spaced fresh readings at [`RECOVERED_AT_BYTES`].
    #[serde(default)]
    pub degraded: bool,
    /// When the first recovery reading of the current streak was taken.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_since_unix_ms: Option<u64>,
}

impl Default for PipeHealth {
    fn default() -> Self {
        Self::for_this_platform()
    }
}

impl PipeHealth {
    /// Evidence for a host that has not been sampled yet.
    pub fn new(applies: bool) -> Self {
        Self {
            applies,
            last_capacity: None,
            last_attempt: Measurement::default(),
            degraded: false,
            recovery_since_unix_ms: None,
        }
    }

    pub fn for_this_platform() -> Self {
        Self::new(cfg!(target_os = "macos"))
    }

    /// Fold one probe attempt into the evidence.
    ///
    /// Attempts older than the latest, and a repeat of the latest, change
    /// nothing, so a duplicated or reordered delivery cannot advance recovery.
    pub fn observe(&mut self, sample: Measurement<PipeCapacity>) {
        let at = sample.measured_at_unix_ms;
        let latest = self.last_attempt.measured_at_unix_ms;
        if at < latest || (at == latest && sample == self.last_attempt) {
            return;
        }
        match sample.reading {
            MeasurementReading::Measured { value: capacity } => {
                let continuous = self.last_capacity.is_some_and(|last| {
                    at.saturating_sub(last.measured_at_unix_ms) <= FRESH_FOR_MS
                });
                if !continuous {
                    self.recovery_since_unix_ms = None;
                }
                if capacity.accepted_bytes < DEGRADED_BELOW_BYTES {
                    self.degraded = true;
                    self.recovery_since_unix_ms = None;
                } else if self.degraded {
                    if capacity.accepted_bytes >= RECOVERED_AT_BYTES {
                        match self.recovery_since_unix_ms {
                            Some(first) if at.saturating_sub(first) >= RECOVERY_SPACING_MS => {
                                self.degraded = false;
                                self.recovery_since_unix_ms = None;
                            }
                            Some(_) => {}
                            None => self.recovery_since_unix_ms = Some(at),
                        }
                    } else {
                        self.recovery_since_unix_ms = None;
                    }
                }
                self.last_capacity = Some(PipeCapacityObservation {
                    measured_at_unix_ms: at,
                    capacity,
                });
                self.last_attempt = Measurement::measured(at, capacity);
            }
            MeasurementReading::Unavailable { reason, detail } => {
                self.recovery_since_unix_ms = None;
                self.last_attempt = Measurement {
                    measured_at_unix_ms: at,
                    reading: MeasurementReading::Unavailable { reason, detail },
                };
            }
        }
    }

    /// Whether new work may start on this host at `now_unix_ms`, and why.
    pub fn assess(&self, now_unix_ms: u64) -> PipeHealthAssessment {
        let mut assessment = PipeHealthAssessment {
            admission: PipeAdmission::Unmeasured,
            capacity: None,
            measured_at_unix_ms: None,
            age_ms: None,
            degraded_latch: self.degraded,
            recovery_samples: u8::from(self.degraded && self.recovery_since_unix_ms.is_some()),
            stale_hold_until_unix_ms: None,
            gap: None,
            last_failure: self.last_failure(),
        };
        if !self.applies {
            assessment.gap = Some(
                "pipe health: not applicable; the fresh-pipe floor is measured on macOS only"
                    .into(),
            );
            return assessment;
        }
        let Some(last) = self.last_capacity else {
            let reason = assessment
                .last_failure
                .clone()
                .unwrap_or_else(|| "no sample has completed yet".into());
            assessment.gap = Some(format!("pipe health: not measured; {reason}"));
            return assessment;
        };
        let age = now_unix_ms.saturating_sub(last.measured_at_unix_ms);
        assessment.capacity = Some(last.capacity);
        assessment.measured_at_unix_ms = Some(last.measured_at_unix_ms);
        assessment.age_ms = Some(age);
        if age <= FRESH_FOR_MS {
            assessment.admission = if self.degraded {
                PipeAdmission::Degraded
            } else {
                PipeAdmission::Healthy
            };
            return assessment;
        }
        assessment.recovery_samples = 0;
        let hold_until = last
            .measured_at_unix_ms
            .saturating_add(FRESH_FOR_MS + STALE_HOLD_MS);
        if now_unix_ms < hold_until {
            assessment.admission = PipeAdmission::StaleHold;
            assessment.stale_hold_until_unix_ms = Some(hold_until);
            return assessment;
        }
        let why = assessment
            .last_failure
            .clone()
            .unwrap_or_else(|| "no sample has completed since".into());
        assessment.gap = Some(format!(
            "pipe health: stale; last measured {} {}s ago, admitting without current evidence ({why})",
            last.capacity.describe(),
            age / 1_000,
        ));
        assessment
    }

    /// The next instant at which [`assess`](Self::assess) changes its answer
    /// with no new sample: the end of freshness, then the end of the stale hold.
    pub fn next_transition_at(&self, now_unix_ms: u64) -> Option<u64> {
        let last = self.last_capacity?.measured_at_unix_ms;
        let expires = last.saturating_add(FRESH_FOR_MS + 1);
        let released = last.saturating_add(FRESH_FOR_MS + STALE_HOLD_MS);
        [expires, released]
            .into_iter()
            .find(|&instant| instant > now_unix_ms)
    }

    fn last_failure(&self) -> Option<String> {
        match &self.last_attempt.reading {
            MeasurementReading::Measured { .. } => None,
            MeasurementReading::Unavailable { reason, detail } => {
                if *reason == MeasurementGap::NotSampled
                    && self.last_attempt.measured_at_unix_ms == 0
                {
                    return None;
                }
                Some(match detail {
                    Some(detail) => format!("{}: {detail}", reason.describe()),
                    None => reason.describe().to_string(),
                })
            }
        }
    }
}

/// The four answers the floor gives.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum PipeAdmission {
    /// A fresh reading, and the host is not latched degraded.
    Healthy,
    /// A fresh reading, and the host is latched degraded: new work waits.
    Degraded,
    /// Measured evidence has expired; new work waits until the bounded hold
    /// ends.
    StaleHold,
    /// No current evidence (never sampled, not applicable, or stale past the
    /// hold): new work starts, under a named gap.
    Unmeasured,
}

impl PipeAdmission {
    pub fn admits(self) -> bool {
        matches!(self, Self::Healthy | Self::Unmeasured)
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Healthy => "measured healthy",
            Self::Degraded => "measured degraded",
            Self::StaleHold => "bounded stale hold",
            Self::Unmeasured => "admit with gap",
        }
    }
}

/// The floor's answer at one instant, with the evidence behind it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub struct PipeHealthAssessment {
    pub admission: PipeAdmission,
    /// The latest successful reading behind the answer, if there is one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capacity: Option<PipeCapacity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub measured_at_unix_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub age_ms: Option<u64>,
    /// The degraded latch, which survives into an unmeasured answer so the
    /// next fresh readings must still recover.
    #[serde(default)]
    pub degraded_latch: bool,
    /// Recovery readings so far, out of [`RECOVERY_SAMPLES`].
    #[serde(default)]
    pub recovery_samples: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stale_hold_until_unix_ms: Option<u64>,
    /// Why an admitting answer has no current measurement behind it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gap: Option<String>,
    /// The latest attempt's failure, when the latest attempt failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_failure: Option<String>,
}

impl PipeHealthAssessment {
    /// The answer for a host that sent no pipe evidence at all: an executor
    /// built before the probe. It admits under a named gap; a missing field is
    /// neither zero nor healthy.
    pub fn not_reported(reason: impl Into<String>) -> Self {
        Self {
            admission: PipeAdmission::Unmeasured,
            capacity: None,
            measured_at_unix_ms: None,
            age_ms: None,
            degraded_latch: false,
            recovery_samples: 0,
            stale_hold_until_unix_ms: None,
            gap: Some(format!("pipe health: not measured; {}", reason.into())),
            last_failure: None,
        }
    }

    pub fn admits(&self) -> bool {
        self.admission.admits()
    }

    /// One line naming the answer and its evidence, for refusals, notices and
    /// the executor page alike.
    pub fn describe(&self) -> String {
        let reading = match (self.capacity, self.age_ms) {
            (Some(capacity), Some(age)) => {
                format!(
                    "fresh pipe held {} {:.1}s ago",
                    capacity.describe(),
                    age as f64 / 1_000.0
                )
            }
            _ => "no reading".into(),
        };
        match self.admission {
            PipeAdmission::Healthy => format!(
                "pipe health: measured healthy; {reading} (degraded below {DEGRADED_BELOW_BYTES} B)"
            ),
            PipeAdmission::Degraded => format!(
                "pipe health: measured degraded; {reading}, below the {DEGRADED_BELOW_BYTES} B floor \
                 or not yet recovered; new processes wait until {RECOVERY_SAMPLES} readings \
                 {RECOVERY_SPACING_MS} ms apart reach {RECOVERED_AT_BYTES} B ({}/{RECOVERY_SAMPLES} so far)",
                self.recovery_samples
            ),
            PipeAdmission::StaleHold => format!(
                "pipe health: bounded stale hold; {reading}, older than {FRESH_FOR_MS} ms; new \
                 processes wait {:.1}s more, then admit with a gap{}",
                (FRESH_FOR_MS + STALE_HOLD_MS).saturating_sub(self.age_ms.unwrap_or_default()) as f64
                    / 1_000.0,
                self.last_failure
                    .as_ref()
                    .map(|failure| format!(" (latest attempt: {failure})"))
                    .unwrap_or_default()
            ),
            PipeAdmission::Unmeasured => self
                .gap
                .clone()
                .unwrap_or_else(|| "pipe health: not measured".into()),
        }
    }
}

/// A change in a host's admission answer, as a monitor reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipeHealthTransition {
    pub from: PipeAdmission,
    pub to: PipeHealthAssessment,
}

impl PipeHealthTransition {
    /// Whether new work is held now and was not before.
    pub fn engaged(&self) -> bool {
        self.from.admits() && !self.to.admits()
    }

    /// Whether new work admits now and was held before.
    pub fn cleared(&self) -> bool {
        !self.from.admits() && self.to.admits()
    }

    /// How a clearance came about: a measured recovery, or evidence expiring
    /// into an admission with a gap. The operator should never mistake the
    /// second for the first.
    pub fn clearance(&self) -> Option<&'static str> {
        self.cleared().then_some(match self.to.admission {
            PipeAdmission::Healthy => "measured recovery",
            _ => "admitting with a gap; evidence expired, not recovered",
        })
    }
}

// ── Monitor ─────────────────────────────────────────────────────────────────

fn unix_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

/// One host's pipe evidence, shared between the sampler that gathers it and
/// every admission boundary that reads it.
///
/// Cloning shares the evidence. Reading never probes: only the sampler thread
/// does, so there is at most one probe in flight per monitor however many
/// boundaries ask.
#[derive(Clone, Default)]
pub struct PipeHealthMonitor {
    evidence: Arc<Mutex<PipeHealth>>,
}

impl PipeHealthMonitor {
    pub fn new() -> Self {
        Self::default()
    }

    /// A monitor over given evidence, with no sampler: for tests and for
    /// hosts whose evidence arrives from elsewhere.
    pub fn with_evidence(evidence: PipeHealth) -> Self {
        Self {
            evidence: Arc::new(Mutex::new(evidence)),
        }
    }

    pub fn evidence(&self) -> PipeHealth {
        self.evidence.lock().unwrap().clone()
    }

    pub fn assess(&self, now_unix_ms: u64) -> PipeHealthAssessment {
        self.evidence.lock().unwrap().assess(now_unix_ms)
    }

    /// The answer now, by the wall clock. Every admission boundary calls this
    /// at decision time, so evidence ages between sampler ticks too.
    pub fn assess_now(&self) -> PipeHealthAssessment {
        self.assess(unix_time_ms())
    }

    pub fn observe(&self, sample: Measurement<PipeCapacity>) {
        self.evidence.lock().unwrap().observe(sample);
    }

    /// Sample this host's fresh pipes every [`SAMPLE_INTERVAL_MS`] on a
    /// dedicated thread, calling `on_transition` whenever the admission answer
    /// changes, including when evidence ages from fresh into the stale hold and
    /// out of it with no new sample.
    ///
    /// A thread rather than a task: the probe is a handful of syscalls, but a
    /// host in trouble is exactly when syscalls stall, and a stalled probe
    /// should only age the evidence, never occupy a runtime worker. The probe
    /// runs outside the evidence lock. The thread holds the evidence weakly
    /// and ends once every monitor clone is gone.
    pub fn spawn_sampler(
        &self,
        on_transition: impl FnMut(PipeHealthTransition) + Send + 'static,
    ) -> std::io::Result<std::thread::JoinHandle<()>> {
        self.spawn_sampler_with(
            sample_fresh_pipe,
            Duration::from_millis(SAMPLE_INTERVAL_MS),
            on_transition,
        )
    }

    pub fn spawn_sampler_with(
        &self,
        mut probe: impl FnMut(u64) -> Measurement<PipeCapacity> + Send + 'static,
        interval: Duration,
        mut on_transition: impl FnMut(PipeHealthTransition) + Send + 'static,
    ) -> std::io::Result<std::thread::JoinHandle<()>> {
        let evidence: Weak<Mutex<PipeHealth>> = Arc::downgrade(&self.evidence);
        std::thread::Builder::new()
            .name("cairn-pipe-health".into())
            .spawn(move || {
                let interval_ms = interval.as_millis().max(1) as u64;
                let mut previous = {
                    let Some(evidence) = evidence.upgrade() else {
                        return;
                    };
                    let answer = evidence.lock().unwrap().assess(unix_time_ms()).admission;
                    answer
                };
                let mut next_sample = 0u64;
                loop {
                    let now = unix_time_ms();
                    if now >= next_sample {
                        let sample = probe(now);
                        let Some(shared) = evidence.upgrade() else {
                            return;
                        };
                        shared.lock().unwrap().observe(sample);
                        next_sample = now.saturating_add(interval_ms);
                    }
                    let Some(shared) = evidence.upgrade() else {
                        return;
                    };
                    let now = unix_time_ms();
                    let (current, wake_at) = {
                        let health = shared.lock().unwrap();
                        (health.assess(now), health.next_transition_at(now))
                    };
                    drop(shared);
                    if current.admission != previous {
                        let from = std::mem::replace(&mut previous, current.admission);
                        on_transition(PipeHealthTransition { from, to: current });
                    }
                    let wake_at = wake_at.map_or(next_sample, |at| at.min(next_sample));
                    let now = unix_time_ms();
                    std::thread::sleep(Duration::from_millis(wake_at.saturating_sub(now).max(1)));
                }
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::io::{Error, ErrorKind};

    struct Scripted {
        outcomes: VecDeque<std::io::Result<usize>>,
        calls: u32,
        dropped: Arc<std::sync::atomic::AtomicBool>,
    }

    impl Scripted {
        fn new(outcomes: Vec<std::io::Result<usize>>) -> Self {
            Self {
                outcomes: outcomes.into(),
                calls: 0,
                dropped: Arc::default(),
            }
        }
    }

    impl ProbeWriter for Scripted {
        fn write_chunk(&mut self, chunk: &[u8]) -> std::io::Result<usize> {
            assert_eq!(
                chunk.len(),
                PROBE_CHUNK_BYTES,
                "every write is a small chunk"
            );
            self.calls += 1;
            self.outcomes
                .pop_front()
                .unwrap_or_else(|| Err(Error::from(ErrorKind::WouldBlock)))
        }
    }

    impl Drop for Scripted {
        fn drop(&mut self) {
            self.dropped
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    fn full_chunks(count: usize) -> Vec<std::io::Result<usize>> {
        (0..count).map(|_| Ok(PROBE_CHUNK_BYTES)).collect()
    }

    #[test]
    fn a_pipe_that_takes_one_chunk_reads_512_bytes() {
        let mut writer = Scripted::new(full_chunks(1));
        assert_eq!(
            fill_until_full(&mut writer),
            Ok(PipeCapacity {
                accepted_bytes: 512,
                at_least: false
            })
        );
    }

    #[test]
    fn exactly_4096_bytes_is_not_degraded() {
        let mut writer = Scripted::new(full_chunks(8));
        let capacity = fill_until_full(&mut writer).unwrap();
        assert_eq!(capacity.accepted_bytes, 4_096);
        let mut health = PipeHealth::new(true);
        health.observe(Measurement::measured(1_000, capacity));
        assert_eq!(health.assess(1_000).admission, PipeAdmission::Healthy);
    }

    #[test]
    fn the_cap_reports_a_lower_bound_without_another_write() {
        let mut writer = Scripted::new(full_chunks(500));
        let capacity = fill_until_full(&mut writer).unwrap();
        assert_eq!(
            capacity,
            PipeCapacity {
                accepted_bytes: 65_536,
                at_least: true
            }
        );
        assert_eq!(writer.calls, 128);
        assert_eq!(capacity.describe(), "at least 65536 B");
    }

    #[test]
    fn partial_writes_count_the_bytes_they_took() {
        let mut writer = Scripted::new(vec![
            Ok(512),
            Ok(100),
            Err(Error::from(ErrorKind::WouldBlock)),
        ]);
        assert_eq!(fill_until_full(&mut writer).unwrap().accepted_bytes, 612);
    }

    #[test]
    fn an_immediate_eagain_is_a_zero_byte_measurement() {
        let mut writer = Scripted::new(vec![Err(Error::from(ErrorKind::WouldBlock))]);
        assert_eq!(fill_until_full(&mut writer).unwrap().accepted_bytes, 0);
    }

    #[test]
    fn interrupts_retry_within_a_bound_and_then_fail() {
        let mut writer = Scripted::new(vec![
            Ok(512),
            Err(Error::from(ErrorKind::Interrupted)),
            Ok(512),
        ]);
        assert_eq!(fill_until_full(&mut writer).unwrap().accepted_bytes, 1_024);

        let endless = (0..10_000)
            .map(|_| Err(Error::from(ErrorKind::Interrupted)))
            .collect();
        let mut writer = Scripted::new(endless);
        let error = fill_until_full(&mut writer).unwrap_err();
        assert!(error.contains("write attempts"), "{error}");
        assert_eq!(writer.calls, PROBE_MAX_ATTEMPTS);
    }

    #[test]
    fn zero_writes_and_other_errors_are_sampling_failures() {
        let mut writer = Scripted::new(vec![Ok(0)]);
        assert!(fill_until_full(&mut writer).is_err());
        let mut writer = Scripted::new(vec![Ok(512), Err(Error::from(ErrorKind::BrokenPipe))]);
        assert!(fill_until_full(&mut writer)
            .unwrap_err()
            .contains("pipe write failed"));
    }

    #[test]
    fn the_writer_is_released_on_every_outcome() {
        for outcomes in [
            full_chunks(1),
            vec![Ok(0)],
            vec![Err(Error::from(ErrorKind::Other))],
            full_chunks(200),
        ] {
            let writer = Scripted::new(outcomes);
            let dropped = writer.dropped.clone();
            {
                let mut writer = writer;
                let _ = fill_until_full(&mut writer);
            }
            assert!(dropped.load(std::sync::atomic::Ordering::SeqCst));
        }
    }

    fn reading(at: u64, bytes: u64) -> Measurement<PipeCapacity> {
        Measurement::measured(
            at,
            PipeCapacity {
                accepted_bytes: bytes,
                at_least: false,
            },
        )
    }

    fn failure(at: u64) -> Measurement<PipeCapacity> {
        Measurement::unavailable_with(at, MeasurementGap::SamplingFailed, "pipe: EMFILE")
    }

    #[test]
    fn an_unsampled_host_admits_with_a_named_gap() {
        let health = PipeHealth::new(true);
        let answer = health.assess(5_000);
        assert_eq!(answer.admission, PipeAdmission::Unmeasured);
        assert!(answer.admits());
        assert_eq!(answer.capacity, None, "never a fabricated reading");
        assert_eq!(
            answer.describe(),
            "pipe health: not measured; no sample has completed yet"
        );

        let mut health = PipeHealth::new(true);
        health.observe(failure(1_000));
        assert!(health
            .assess(1_000)
            .describe()
            .contains("not measured; sampling failed: pipe: EMFILE"));
    }

    #[test]
    fn an_unsupported_platform_is_not_applicable_rather_than_green() {
        let mut health = PipeHealth::new(false);
        health.observe(Measurement::unavailable(
            1_000,
            MeasurementGap::UnsupportedPlatform,
        ));
        let answer = health.assess(1_000);
        assert_eq!(answer.admission, PipeAdmission::Unmeasured);
        assert!(answer.describe().contains("not applicable"));
    }

    #[test]
    fn a_low_reading_degrades_at_once_and_an_initial_healthy_one_admits() {
        let mut health = PipeHealth::new(true);
        health.observe(reading(1_000, 4_096));
        assert_eq!(health.assess(1_000).admission, PipeAdmission::Healthy);
        health.observe(reading(6_000, 4_095));
        let answer = health.assess(6_000);
        assert_eq!(answer.admission, PipeAdmission::Degraded);
        assert!(!answer.admits());
        assert!(
            answer.describe().contains("4095 B"),
            "{}",
            answer.describe()
        );
    }

    #[test]
    fn recovery_needs_two_spaced_readings_and_resets_on_a_dip_or_failure() {
        let mut health = PipeHealth::new(true);
        health.observe(reading(0, 512));
        // The intermediate range holds.
        health.observe(reading(5_000, 8_192));
        assert_eq!(health.assess(5_000).admission, PipeAdmission::Degraded);
        // One recovery reading is not enough, nor is a second too soon.
        health.observe(reading(10_000, 65_536));
        assert_eq!(health.assess(10_000).recovery_samples, 1);
        health.observe(reading(11_000, 65_536));
        assert_eq!(health.assess(11_000).admission, PipeAdmission::Degraded);
        // A dip resets progress.
        health.observe(reading(15_000, 8_192));
        assert_eq!(health.assess(15_000).recovery_samples, 0);
        health.observe(reading(20_000, 65_536));
        // A failure resets it too, and never renews the reading's timestamp.
        health.observe(failure(25_000));
        assert_eq!(health.assess(25_000).recovery_samples, 0);
        assert_eq!(health.assess(25_000).measured_at_unix_ms, Some(20_000));
        health.observe(reading(30_000, 65_536));
        health.observe(reading(35_000, 65_536));
        assert_eq!(health.assess(35_000).admission, PipeAdmission::Healthy);
    }

    #[test]
    fn duplicate_and_reordered_samples_change_nothing() {
        let mut health = PipeHealth::new(true);
        health.observe(reading(0, 512));
        health.observe(reading(10_000, 65_536));
        let before = health.clone();
        health.observe(reading(10_000, 65_536));
        health.observe(reading(5_000, 65_536));
        assert_eq!(health, before);
    }

    #[test]
    fn expired_evidence_holds_for_a_bounded_time_then_admits_with_a_gap() {
        let mut health = PipeHealth::new(true);
        health.observe(reading(0, 512));
        assert_eq!(
            health.assess(FRESH_FOR_MS).admission,
            PipeAdmission::Degraded
        );
        // Repeated failures neither freshen nor extend anything.
        for at in (FRESH_FOR_MS..FRESH_FOR_MS + STALE_HOLD_MS + 5_000).step_by(1_000) {
            health.observe(failure(at));
        }
        let held = health.assess(FRESH_FOR_MS + 1);
        assert_eq!(held.admission, PipeAdmission::StaleHold);
        assert_eq!(
            held.stale_hold_until_unix_ms,
            Some(FRESH_FOR_MS + STALE_HOLD_MS)
        );
        assert!(held.describe().contains("EMFILE"), "{}", held.describe());
        // Even a degraded last reading cannot refuse forever.
        let released = health.assess(FRESH_FOR_MS + STALE_HOLD_MS);
        assert_eq!(released.admission, PipeAdmission::Unmeasured);
        assert!(released.admits());
        assert!(
            released.describe().contains("stale; last measured 512 B"),
            "{}",
            released.describe()
        );
        assert!(released.degraded_latch);
        // A fresh reading resumes enforcement, and the latch still needs recovery.
        let at = FRESH_FOR_MS + STALE_HOLD_MS + 6_000;
        health.observe(reading(at, 65_536));
        assert_eq!(health.assess(at).admission, PipeAdmission::Degraded);
        health.observe(reading(at + 5_000, 65_536));
        assert_eq!(health.assess(at + 5_000).admission, PipeAdmission::Healthy);
    }

    #[test]
    fn a_gap_in_readings_restarts_recovery() {
        let mut health = PipeHealth::new(true);
        health.observe(reading(0, 512));
        health.observe(reading(5_000, 65_536));
        // The next reading comes after freshness lapsed: the streak restarts.
        health.observe(reading(5_000 + FRESH_FOR_MS + 1, 65_536));
        assert_eq!(
            health.assess(5_000 + FRESH_FOR_MS + 1).admission,
            PipeAdmission::Degraded
        );
    }

    #[test]
    fn the_next_transition_is_expiry_then_release() {
        let mut health = PipeHealth::new(true);
        assert_eq!(health.next_transition_at(0), None);
        health.observe(reading(1_000, 65_536));
        assert_eq!(
            health.next_transition_at(1_000),
            Some(1_000 + FRESH_FOR_MS + 1)
        );
        assert_eq!(
            health.next_transition_at(1_000 + FRESH_FOR_MS + 1),
            Some(1_000 + FRESH_FOR_MS + STALE_HOLD_MS)
        );
        assert_eq!(
            health.next_transition_at(1_000 + FRESH_FOR_MS + STALE_HOLD_MS),
            None
        );
    }

    #[test]
    fn evidence_round_trips_and_a_missing_report_is_a_gap() {
        let mut health = PipeHealth::new(true);
        health.observe(reading(0, 512));
        health.observe(failure(5_000));
        let json = serde_json::to_string(&health).unwrap();
        assert_eq!(serde_json::from_str::<PipeHealth>(&json).unwrap(), health);
        let minimal: PipeHealth = serde_json::from_str(r#"{"applies":true}"#).unwrap();
        assert_eq!(minimal.assess(1).admission, PipeAdmission::Unmeasured);
        let missing = PipeHealthAssessment::not_reported("the executor predates the probe");
        assert!(missing.admits());
        assert_eq!(missing.capacity, None);
    }

    #[test]
    fn transitions_name_measured_recovery_apart_from_expiry() {
        let mut health = PipeHealth::new(true);
        health.observe(reading(0, 512));
        let recovered = PipeHealthTransition {
            from: PipeAdmission::Degraded,
            to: {
                let mut h = health.clone();
                h.observe(reading(5_000, 65_536));
                h.observe(reading(10_000, 65_536));
                h.assess(10_000)
            },
        };
        assert_eq!(recovered.clearance(), Some("measured recovery"));
        let expired = PipeHealthTransition {
            from: PipeAdmission::StaleHold,
            to: health.assess(FRESH_FOR_MS + STALE_HOLD_MS),
        };
        assert!(expired.cleared());
        assert!(expired.clearance().unwrap().contains("not recovered"));
        let engaged = PipeHealthTransition {
            from: PipeAdmission::Healthy,
            to: health.assess(0),
        };
        assert!(engaged.engaged());
    }

    #[test]
    fn the_sampler_publishes_transitions_and_ends_with_its_monitor() {
        let monitor = PipeHealthMonitor::with_evidence(PipeHealth::new(true));
        let (sender, receiver) = std::sync::mpsc::channel();
        let mut bytes = vec![65_536u64, 512, 512].into_iter();
        let handle = monitor
            .spawn_sampler_with(
                move |now| reading(now, bytes.next().unwrap_or(512)),
                Duration::from_millis(20),
                move |transition| {
                    let _ = sender.send(transition);
                },
            )
            .unwrap();
        let first = receiver.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(first.from, PipeAdmission::Unmeasured);
        assert_eq!(first.to.admission, PipeAdmission::Healthy);
        let second = receiver.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(second.engaged());
        assert_eq!(monitor.assess_now().admission, PipeAdmission::Degraded);
        drop(monitor);
        handle.join().unwrap();
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn the_real_probe_completes_quickly_and_leaks_no_descriptors() {
        let open_fds = || std::fs::read_dir("/dev/fd").unwrap().count();
        let before = open_fds();
        let started = std::time::Instant::now();
        for _ in 0..50 {
            let sample = sample_fresh_pipe(1);
            // Valid either way; the host's health is not this test's claim.
            if let Some(capacity) = sample.value() {
                assert!(capacity.accepted_bytes <= PROBE_CAP_BYTES);
                assert_eq!(
                    capacity.at_least,
                    capacity.accepted_bytes >= PROBE_CAP_BYTES
                );
            }
        }
        assert!(started.elapsed() < Duration::from_secs(10));
        assert_eq!(open_fds(), before);
    }

    #[cfg(not(target_os = "macos"))]
    #[test]
    fn the_probe_is_not_applicable_off_macos() {
        assert_eq!(
            sample_fresh_pipe(7).gap(),
            Some(MeasurementGap::UnsupportedPlatform)
        );
        assert!(!PipeHealth::for_this_platform().applies);
    }
}
