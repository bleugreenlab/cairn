//! One executor link's clocks, and the one judgement of whether it is silent.
//!
//! A runner that declares a peer silent is making a claim about the peer from
//! its own observations, and the claim is only as good as the observer. On
//! 10-16 the runner charged bgsrv-mac with 116-167 s of silence while
//! bgsrv-mac's own writer sat inside one blocked send for 328 s: the runner's
//! socket pump had stopped reading because it was waiting on a snapshot apply,
//! and nothing it read from its stamps could tell that apart from a silent
//! peer. A suspended host misreads the same way (cairn/4819), with the judge
//! asleep rather than busy.
//!
//! So a link carries three kinds of evidence, all on a monotonic clock that
//! does not advance across host suspend:
//!
//! - `last_received`: the reader dequeued a frame. Evidence from the peer.
//! - `reader_tick`: the reader's own timer ran. Evidence that the judge was
//!   listening, kept apart from frame arrival on purpose, because a stamp that
//!   mixes the two cannot say which one stopped.
//! - the ingest phase: what the task applying this link's messages is doing
//!   and since when, so a slow apply names itself.
//!
//! [`LinkClock::judge`] is the rule built on them: silence counts only while
//! the judge was listening. A reader that was away for more than a few ticks
//! hands the peer a fresh window when it resumes (the passive re-probe: an
//! executor beats every [`EXECUTOR_HEARTBEAT_INTERVAL_MS`], well inside any
//! staleness bound), and a reader that is away right now cannot call anyone
//! silent at all.

use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::OnceLock;
use std::time::Instant;

#[cfg(doc)]
use cairn_common::executor_protocol::EXECUTOR_HEARTBEAT_INTERVAL_MS;

/// How long the reader's timer may go without running before the reader
/// counts as absent. Its tick is one second, so this is a few missed ticks:
/// enough to absorb scheduling jitter on a busy host, far short of any
/// staleness bound.
pub(crate) const READER_ABSENT_AFTER_MS: u64 = 3_000;

const NANOS_PER_MS: u64 = 1_000_000;

/// Where the monotonic clock starts. Any fixed value works; a day in means a
/// stamp "some minutes before now" is representable from the first instant
/// of the process, and zero stays free to mean "never stamped".
const MONOTONIC_BASE_NS: u64 = 24 * 3_600 * 1_000 * NANOS_PER_MS;

/// Nanoseconds since this process first asked, on a clock that only moves
/// forward and, on macOS and Linux, does not move while the host is asleep.
/// Never zero, so zero can mean "never stamped".
pub fn monotonic_now_ns() -> u64 {
    static ORIGIN: OnceLock<Instant> = OnceLock::new();
    let origin = *ORIGIN.get_or_init(Instant::now);
    u64::try_from(origin.elapsed().as_nanos())
        .unwrap_or(u64::MAX)
        .saturating_add(MONOTONIC_BASE_NS)
}

/// What the task applying a link's messages is doing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum IngestPhase {
    Idle = 0,
    Heartbeat = 1,
    Advertisement = 2,
    SnapshotApply = 3,
    Announce = 4,
    Message = 5,
}

impl IngestPhase {
    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Heartbeat,
            2 => Self::Advertisement,
            3 => Self::SnapshotApply,
            4 => Self::Announce,
            5 => Self::Message,
            _ => Self::Idle,
        }
    }

    /// The name the log and the rejection use.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Heartbeat => "heartbeat",
            Self::Advertisement => "advertisement",
            Self::SnapshotApply => "snapshot.apply",
            Self::Announce => "announce",
            Self::Message => "message",
        }
    }
}

/// The verdict on one link at one instant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkJudgement {
    /// Heard from within the bound, counting only time the judge was listening.
    Fresh,
    /// Silent past the bound while the judge was listening the whole time.
    PeerSilent { silence_ms: u64 },
    /// The reader that would have heard the peer is not running now, so no
    /// silence can be charged to the peer. `ingest_phase` and `ingest_age_ms`
    /// say what the link's ingest is doing, which is usually why.
    JudgeAbsent {
        reader_silence_ms: u64,
        ingest_phase: IngestPhase,
        ingest_age_ms: u64,
    },
}

/// One link's clocks. Shared between the transport's reader and ingest tasks
/// (which stamp it) and the fleet (which judges it), lock-free.
#[derive(Debug, Default)]
pub struct LinkClock {
    last_received_ns: AtomicU64,
    reader_tick_ns: AtomicU64,
    judge_resumed_ns: AtomicU64,
    ingest_phase: AtomicU8,
    ingest_since_ns: AtomicU64,
    heartbeats_received: AtomicU64,
    heartbeats_applied: AtomicU64,
}

impl LinkClock {
    /// The reader received a heartbeat. Returns its sequence number, which
    /// ingest hands back to [`Self::heartbeat_applied`] once it is in.
    pub fn heartbeat_received(&self) -> u64 {
        self.heartbeats_received.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// Ingest applied heartbeat `sequence`. A beat superseded in the queue by
    /// a newer one is covered when the newer one is applied.
    pub fn heartbeat_applied(&self, sequence: u64) {
        self.heartbeats_applied
            .fetch_max(sequence, Ordering::AcqRel);
    }

    /// Whether a heartbeat the reader received is still waiting to be applied:
    /// the facts on record are older than what the executor last said.
    pub fn heartbeat_pending(&self) -> bool {
        self.heartbeats_received.load(Ordering::Acquire)
            > self.heartbeats_applied.load(Ordering::Acquire)
    }

    /// The reader dequeued a frame.
    pub fn frame_received(&self) {
        self.frame_received_at(monotonic_now_ns());
    }

    /// The reader's timer arm ran.
    pub fn reader_ticked(&self) {
        self.reader_ticked_at(monotonic_now_ns());
    }

    /// The ingest task entered `phase`. The returned guard returns it to idle.
    pub fn ingest(&self, phase: IngestPhase) -> IngestGuard<'_> {
        self.ingest_since_ns
            .store(monotonic_now_ns(), Ordering::Relaxed);
        self.ingest_phase.store(phase as u8, Ordering::Relaxed);
        IngestGuard { clock: self }
    }

    /// What ingest is doing, and for how long, at `now_ns`.
    pub fn ingest_state(&self, now_ns: u64) -> (IngestPhase, u64) {
        let phase = IngestPhase::from_u8(self.ingest_phase.load(Ordering::Relaxed));
        let since = self.ingest_since_ns.load(Ordering::Relaxed);
        let age_ms = if phase == IngestPhase::Idle || since == 0 {
            0
        } else {
            now_ns.saturating_sub(since) / NANOS_PER_MS
        };
        (phase, age_ms)
    }

    /// How long since the reader last dequeued a frame, or `None` before it
    /// ever has.
    pub fn received_silence_ms(&self, now_ns: u64) -> Option<u64> {
        stamped(self.last_received_ns.load(Ordering::Relaxed))
            .map(|at| now_ns.saturating_sub(at) / NANOS_PER_MS)
    }

    /// How long since the reader's timer last ran, or `None` before it ever
    /// has.
    pub fn reader_silence_ms(&self, now_ns: u64) -> Option<u64> {
        stamped(self.reader_tick_ns.load(Ordering::Relaxed))
            .map(|at| now_ns.saturating_sub(at) / NANOS_PER_MS)
    }

    /// Stage a link whose reader last dequeued a frame `received_ago_ms` ago
    /// and last ticked `reader_ago_ms` ago, having been listening without a
    /// gap since the earlier of the two, for tests of the judgement's
    /// consumers.
    #[cfg(test)]
    pub(crate) fn stage(&self, received_ago_ms: u64, reader_ago_ms: u64) {
        let now = monotonic_now_ns();
        let ago = |ms: u64| now - ms * NANOS_PER_MS;
        self.last_received_ns
            .store(ago(received_ago_ms), Ordering::Relaxed);
        self.reader_tick_ns
            .store(ago(reader_ago_ms), Ordering::Relaxed);
        self.judge_resumed_ns
            .store(ago(received_ago_ms.max(reader_ago_ms)), Ordering::Relaxed);
    }

    fn frame_received_at(&self, now_ns: u64) {
        self.last_received_ns.store(now_ns, Ordering::Relaxed);
    }

    fn reader_ticked_at(&self, now_ns: u64) {
        let previous = self.reader_tick_ns.swap(now_ns, Ordering::Relaxed);
        let absent_ns = READER_ABSENT_AFTER_MS * NANOS_PER_MS;
        if previous == 0 || now_ns.saturating_sub(previous) > absent_ns {
            self.judge_resumed_ns.store(now_ns, Ordering::Relaxed);
        }
    }

    /// Judge this link at `now_ns` against `bound_ms`.
    ///
    /// `wall_silence_ms` is the runner's older evidence, from the wall-clock
    /// stamp ingest writes when it applies a beat or a changed snapshot. It is
    /// the whole answer only while no reader has stamped this clock (a link the
    /// transport never served, which is how tests stage one); once one has, the
    /// reader's monotonic stamps decide, because they are written when a frame
    /// arrives rather than when ingest gets round to it.
    pub fn judge(&self, now_ns: u64, wall_silence_ms: u64, bound_ms: u64) -> LinkJudgement {
        let Some(received_at) = stamped(self.last_received_ns.load(Ordering::Relaxed)) else {
            return if wall_silence_ms > bound_ms {
                LinkJudgement::PeerSilent {
                    silence_ms: wall_silence_ms,
                }
            } else {
                LinkJudgement::Fresh
            };
        };
        if let Some(reader_silence_ms) = self.reader_silence_ms(now_ns) {
            if reader_silence_ms > READER_ABSENT_AFTER_MS {
                let (ingest_phase, ingest_age_ms) = self.ingest_state(now_ns);
                return LinkJudgement::JudgeAbsent {
                    reader_silence_ms,
                    ingest_phase,
                    ingest_age_ms,
                };
            }
        }
        let listening_since = received_at.max(self.judge_resumed_ns.load(Ordering::Relaxed));
        let silence_ms = now_ns.saturating_sub(listening_since) / NANOS_PER_MS;
        if silence_ms > bound_ms {
            LinkJudgement::PeerSilent { silence_ms }
        } else {
            LinkJudgement::Fresh
        }
    }
}

fn stamped(at_ns: u64) -> Option<u64> {
    (at_ns != 0).then_some(at_ns)
}

/// Returns a link's ingest to idle when the phase it entered ends.
pub struct IngestGuard<'a> {
    clock: &'a LinkClock,
}

impl Drop for IngestGuard<'_> {
    fn drop(&mut self) {
        self.clock
            .ingest_phase
            .store(IngestPhase::Idle as u8, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: u64 = NANOS_PER_MS;
    const BOUND_MS: u64 = 75_000;

    /// A link whose reader ticks every second from `from_ms` to `to_ms`, and
    /// whose peer beats every 30 s over the same span when `beating`.
    fn run(clock: &LinkClock, from_ms: u64, to_ms: u64, beating: bool) {
        let mut at = from_ms;
        while at <= to_ms {
            clock.reader_ticked_at(at * MS);
            if beating && at.is_multiple_of(30_000) {
                clock.frame_received_at(at * MS);
            }
            at += 1_000;
        }
    }

    #[test]
    fn a_peer_beating_inside_the_bound_is_fresh() {
        let clock = LinkClock::default();
        run(&clock, 1_000, 300_000, true);
        assert_eq!(clock.judge(300_000 * MS, 0, BOUND_MS), LinkJudgement::Fresh);
    }

    #[test]
    fn a_silent_peer_with_the_reader_listening_is_peer_silent() {
        let clock = LinkClock::default();
        run(&clock, 1_000, 30_000, true);
        run(&clock, 31_000, 200_000, false);
        assert_eq!(
            clock.judge(200_000 * MS, 0, BOUND_MS),
            LinkJudgement::PeerSilent {
                silence_ms: 170_000
            }
        );
    }

    /// The 10-16 bgsrv-mac specimen: the reader stopped for five minutes while
    /// the peer kept trying to send. Nothing in that gap is the peer's silence,
    /// and while it lasts the judgement says the judge is away.
    #[test]
    fn a_reader_gap_with_the_peer_beating_never_reads_as_peer_silence() {
        let clock = LinkClock::default();
        run(&clock, 1_000, 60_000, true);
        let _ingest = clock.ingest(IngestPhase::SnapshotApply);
        for at in (61_000..=360_000).step_by(1_000) {
            let judgement = clock.judge(at * MS, 0, BOUND_MS);
            assert!(
                !matches!(judgement, LinkJudgement::PeerSilent { .. }),
                "at {at}ms: {judgement:?}"
            );
        }
        assert!(matches!(
            clock.judge(360_000 * MS, 0, BOUND_MS),
            LinkJudgement::JudgeAbsent {
                reader_silence_ms: 300_000,
                ingest_phase: IngestPhase::SnapshotApply,
                ..
            }
        ));
        // The reader resumes, the backlog arrives, and the peer is fresh.
        drop(_ingest);
        run(&clock, 361_000, 400_000, true);
        assert_eq!(clock.judge(400_000 * MS, 0, BOUND_MS), LinkJudgement::Fresh);
    }

    /// The same gap with a peer that is genuinely gone: it is charged with
    /// silence exactly one fresh window after the reader resumes, not at once.
    #[test]
    fn a_dead_peer_behind_a_reader_gap_is_silent_one_window_after_the_reader_resumes() {
        let clock = LinkClock::default();
        run(&clock, 1_000, 60_000, true);
        // Reader away from 60 s to 360 s; the peer died at 60 s.
        run(&clock, 361_000, 361_000 + BOUND_MS, false);
        assert_eq!(
            clock.judge((361_000 + BOUND_MS) * MS, 0, BOUND_MS),
            LinkJudgement::Fresh
        );
        run(&clock, 362_000 + BOUND_MS, 362_000 + BOUND_MS, false);
        assert!(matches!(
            clock.judge((362_000 + BOUND_MS) * MS, 0, BOUND_MS),
            LinkJudgement::PeerSilent { .. }
        ));
    }

    /// A clock no reader has stamped makes no claim of its own; the wall-clock
    /// evidence decides, as it did before the reader stamped anything.
    #[test]
    fn an_unstamped_clock_defers_to_the_wall_clock_evidence() {
        let clock = LinkClock::default();
        assert_eq!(clock.judge(MS, 10_000, BOUND_MS), LinkJudgement::Fresh);
        assert_eq!(
            clock.judge(MS, 90_000, BOUND_MS),
            LinkJudgement::PeerSilent { silence_ms: 90_000 }
        );
    }

    #[test]
    fn an_ingest_phase_names_itself_until_it_ends() {
        let clock = LinkClock::default();
        {
            let _guard = clock.ingest(IngestPhase::Heartbeat);
            assert_eq!(
                clock.ingest_state(monotonic_now_ns()).0,
                IngestPhase::Heartbeat
            );
        }
        assert_eq!(
            clock.ingest_state(monotonic_now_ns()),
            (IngestPhase::Idle, 0)
        );
    }
}
