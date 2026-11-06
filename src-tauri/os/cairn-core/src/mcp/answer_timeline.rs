//! What one callback's answer cost, phase by phase, for the transport's
//! answer-time log line.
//!
//! The transport logs one line per callback when it answers: the tool, how the
//! call ended, its total time against the bound it had to answer within, and
//! where the time went. A handler that knows its own phases (`run` does)
//! records them here; the transport reads them back after the handler returns.
//! The record is task-local, so it belongs to the one callback being answered
//! and a handler that records nothing costs nothing.

use std::cell::RefCell;
use std::future::Future;
use std::time::Duration;

/// How a callback ended, as the answer-time log reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnswerOutcome {
    /// Answered with the call's real result.
    Answered,
    /// Answered with its real result after being held open past grace.
    Held,
    /// Answered with a suspend marker; the turn resumes durably.
    Parked,
    /// Refused before any work started.
    Refused,
    /// Could be neither answered nor parked in time, so it answered with a
    /// handle and its batch kept running.
    Detached,
    /// Could be neither answered, parked nor detached in time, so the work
    /// was cancelled.
    Cancelled,
    /// The handler failed.
    Error,
}

impl AnswerOutcome {
    pub fn word(self) -> &'static str {
        match self {
            Self::Answered => "answered",
            Self::Held => "held",
            Self::Parked => "parked",
            Self::Refused => "refused",
            Self::Detached => "detached",
            Self::Cancelled => "cancelled",
            Self::Error => "error",
        }
    }
}

/// The phases one callback recorded, in the order they ran.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AnswerTimeline {
    pub outcome: Option<AnswerOutcome>,
    pub phases: Vec<(&'static str, Duration)>,
}

impl AnswerTimeline {
    /// `[prepare=12ms, place+run=130004ms, ...]`.
    pub fn render_phases(&self) -> String {
        let phases = self
            .phases
            .iter()
            .map(|(name, took)| format!("{name}={}ms", took.as_millis()))
            .collect::<Vec<_>>()
            .join(", ");
        format!("[{phases}]")
    }
}

tokio::task_local! {
    static TIMELINE: RefCell<AnswerTimeline>;
}

/// Run one callback's handling with a fresh timeline, and return what it
/// recorded alongside its output.
pub async fn scope<F: Future>(future: F) -> (F::Output, AnswerTimeline) {
    TIMELINE
        .scope(RefCell::new(AnswerTimeline::default()), async move {
            let output = future.await;
            let timeline = TIMELINE.with(|timeline| timeline.take());
            (output, timeline)
        })
        .await
}

/// Record one phase of the current callback. Outside a [`scope`] (a handler
/// called directly, as tests do) it is a no-op.
pub fn record_phase(name: &'static str, took: Duration) {
    let _ = TIMELINE.try_with(|timeline| timeline.borrow_mut().phases.push((name, took)));
}

/// Record how the current callback ended. The last word wins.
pub fn set_outcome(outcome: AnswerOutcome) {
    let _ = TIMELINE.try_with(|timeline| timeline.borrow_mut().outcome = Some(outcome));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_scope_returns_what_its_handler_recorded() {
        let (value, timeline) = scope(async {
            record_phase("prepare", Duration::from_millis(3));
            record_phase("place+run", Duration::from_millis(40));
            set_outcome(AnswerOutcome::Held);
            7
        })
        .await;
        assert_eq!(value, 7);
        assert_eq!(timeline.outcome, Some(AnswerOutcome::Held));
        assert_eq!(timeline.render_phases(), "[prepare=3ms, place+run=40ms]");
        // Outside a scope, recording is harmless.
        record_phase("prepare", Duration::ZERO);
        set_outcome(AnswerOutcome::Error);
    }
}
