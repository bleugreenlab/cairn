//! A transaction's COMMIT, polled again on a fixed cadence while it is
//! pending, and timed.
//!
//! A commit waits inside the engine: behind another transaction's commit it
//! read from (a commit dependency), or parked behind the group-commit leader.
//! Either way it returns `Pending` and relies on the engine to wake it. If that
//! wake never comes, nothing polls the commit again: the caller holds its
//! connection and every transaction that read the commit's rows waits behind
//! it. Polling again on a tick turns such a lost wakeup into a delay of at most
//! one tick. It is always safe, because polling a pending commit only asks the
//! engine whether it can make progress.
//!
//! The watch also records which poll the commit returned on, which is what
//! tells a lost wakeup from a genuine wait when a slow one is reported.

use std::fmt;
use std::future::{poll_fn, Future};
use std::pin::pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use tokio::time::{interval_at, Instant, Interval, MissedTickBehavior};

#[derive(Debug, Clone, Copy)]
pub(super) struct CommitWatchBounds {
    /// How long a pending commit waits for the engine before it is polled
    /// again anyway.
    pub repoll_every: Duration,
    /// How long a commit may be pending before it is reported.
    pub slow_after: Duration,
}

impl CommitWatchBounds {
    pub(super) const PRODUCTION: Self = Self {
        repoll_every: Duration::from_millis(250),
        slow_after: Duration::from_secs(1),
    };
}

/// The poll a commit returned on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ReturnedOn {
    /// The first poll: the commit never waited.
    FirstPoll,
    /// A poll the engine asked for by waking the commit.
    EngineWake,
    /// A poll only the re-poll tick asked for. The engine had not woken the
    /// commit, so without the tick it would still be pending.
    Repoll,
    /// A poll neither asked for, such as one another future in the same task
    /// caused.
    OtherWake,
}

impl fmt::Display for ReturnedOn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::FirstPoll => "the first poll",
            Self::EngineWake => "an engine wake (a genuine wait; the re-poll only observed it)",
            Self::Repoll => {
                "a re-poll the engine never asked for (a lost wakeup; the re-poll contained it)"
            }
            Self::OtherWake => "a poll neither the engine nor the re-poll asked for",
        })
    }
}

/// What one watched commit did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct CommitWatch {
    pub elapsed: Duration,
    /// Polls the tick caused.
    pub repolls: u32,
    pub returned_on: ReturnedOn,
}

/// The waker the commit sees: it notes that the engine asked for a poll, then
/// wakes the task.
struct EngineWaker {
    woke: AtomicBool,
    task: Mutex<Waker>,
}

impl EngineWaker {
    fn task(&self) -> std::sync::MutexGuard<'_, Waker> {
        self.task
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Wake for EngineWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.woke.store(true, Ordering::SeqCst);
        self.task().wake_by_ref();
    }
}

/// Drive `commit` to completion, polling it again every
/// `bounds.repoll_every` while it is pending. `on_slow` runs once, on the first
/// poll at or past `bounds.slow_after`, with the time pending and the re-polls
/// so far.
///
/// A commit that is ready on its first poll costs one allocation and no timer.
pub(super) async fn watch_commit<T>(
    commit: impl Future<Output = T>,
    bounds: CommitWatchBounds,
    on_slow: impl FnOnce(Duration, u32),
) -> (T, CommitWatch) {
    let started = Instant::now();
    let mut commit = pin!(commit);
    let mut on_slow = Some(on_slow);
    let mut engine: Option<(Arc<EngineWaker>, Waker)> = None;
    let mut tick: Option<Interval> = None;
    let mut repolls = 0_u32;
    let mut polls = 0_u32;
    poll_fn(|cx| {
        let (flag, waker) = engine.get_or_insert_with(|| {
            let flag = Arc::new(EngineWaker {
                woke: AtomicBool::new(false),
                task: Mutex::new(cx.waker().clone()),
            });
            let waker = Waker::from(flag.clone());
            (flag, waker)
        });
        {
            let mut task = flag.task();
            if !task.will_wake(cx.waker()) {
                *task = cx.waker().clone();
            }
        }
        let mut ticked = false;
        if let Some(tick) = tick.as_mut() {
            while tick.poll_tick(cx).is_ready() {
                ticked = true;
            }
        }
        if ticked {
            repolls += 1;
        }
        let engine_woke = flag.woke.swap(false, Ordering::SeqCst);
        let first = polls == 0;
        polls += 1;
        if let Poll::Ready(value) = commit.as_mut().poll(&mut Context::from_waker(waker)) {
            let returned_on = if first {
                ReturnedOn::FirstPoll
            } else if engine_woke {
                ReturnedOn::EngineWake
            } else if ticked {
                ReturnedOn::Repoll
            } else {
                ReturnedOn::OtherWake
            };
            return Poll::Ready((
                value,
                CommitWatch {
                    elapsed: started.elapsed(),
                    repolls,
                    returned_on,
                },
            ));
        }
        if tick.is_none() {
            let mut fresh = interval_at(started + bounds.repoll_every, bounds.repoll_every);
            fresh.set_missed_tick_behavior(MissedTickBehavior::Delay);
            // Registers the timer; the first tick is in the future.
            let _ = fresh.poll_tick(cx);
            tick = Some(fresh);
        }
        let pending = started.elapsed();
        if pending >= bounds.slow_after {
            if let Some(on_slow) = on_slow.take() {
                on_slow(pending, repolls);
            }
        }
        Poll::Pending
    })
    .await
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::rc::Rc;

    use super::*;

    const BOUNDS: CommitWatchBounds = CommitWatchBounds::PRODUCTION;

    /// Pending on its first poll without keeping the waker it was handed,
    /// then ready: a commit whose wakeup was lost.
    fn never_woken() -> impl Future<Output = u8> {
        let mut polled = false;
        poll_fn(move |_cx| {
            if polled {
                Poll::Ready(7)
            } else {
                polled = true;
                Poll::Pending
            }
        })
    }

    #[tokio::test(start_paused = true)]
    async fn a_commit_the_engine_never_wakes_returns_on_a_repoll() {
        // Unwatched, it never finishes: nothing polls it a second time. (Not
        // `tokio::time::timeout`, which polls its future again at the deadline
        // and so is itself a re-poll.)
        let unwatched = tokio::spawn(never_woken());
        tokio::time::sleep(Duration::from_secs(60)).await;
        assert!(!unwatched.is_finished());
        unwatched.abort();

        let (value, watch) = watch_commit(never_woken(), BOUNDS, |_, _| {
            panic!("one tick is under the slow bound")
        })
        .await;
        assert_eq!(value, 7);
        assert_eq!(watch.returned_on, ReturnedOn::Repoll);
        assert_eq!(watch.repolls, 1);
        assert_eq!(watch.elapsed, BOUNDS.repoll_every);
    }

    #[tokio::test(start_paused = true)]
    async fn a_commit_the_engine_wakes_returns_on_the_engine_wake() {
        let mut polled = false;
        let woken_by_engine = poll_fn(move |cx: &mut Context<'_>| {
            if polled {
                return Poll::Ready(());
            }
            polled = true;
            let waker = cx.waker().clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(10)).await;
                waker.wake();
            });
            Poll::Pending
        });
        let ((), watch) = watch_commit(woken_by_engine, BOUNDS, |_, _| {
            panic!("a 10 ms wait is not slow")
        })
        .await;
        assert_eq!(watch.returned_on, ReturnedOn::EngineWake);
        assert_eq!(watch.repolls, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_slow_commit_is_reported_once_past_the_bound() {
        let started = Instant::now();
        let pending_for_1600_ms = poll_fn(move |_cx| {
            if started.elapsed() >= Duration::from_millis(1600) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        });
        let reports = Rc::new(Cell::new(0));
        let seen = reports.clone();
        let ((), watch) = watch_commit(pending_for_1600_ms, BOUNDS, move |pending, repolls| {
            seen.set(seen.get() + 1);
            assert_eq!(pending, BOUNDS.slow_after);
            assert_eq!(repolls, 4);
        })
        .await;
        assert_eq!(reports.get(), 1);
        assert_eq!(watch.returned_on, ReturnedOn::Repoll);
        assert_eq!(watch.repolls, 7);
        assert_eq!(watch.elapsed, Duration::from_millis(1750));
    }

    #[tokio::test(start_paused = true)]
    async fn a_ready_commit_takes_no_timer_and_reports_nothing() {
        let (value, watch) = watch_commit(std::future::ready(3), BOUNDS, |_, _| {
            panic!("never pending")
        })
        .await;
        assert_eq!(value, 3);
        assert_eq!(
            watch,
            CommitWatch {
                elapsed: Duration::ZERO,
                repolls: 0,
                returned_on: ReturnedOn::FirstPoll,
            }
        );
    }
}
