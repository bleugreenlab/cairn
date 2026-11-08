//! The runner host's fresh-pipe floor (cairn/6023).
//!
//! Executors gate their own hosts; this gates the one an executor cannot see:
//! the runner's, where every Claude CLI session, dedicated Codex app-server and
//! process-backed call is spawned. The evidence and policy are the shared ones
//! in [`cairn_common::pipe_health`], sampled here on the runner's own clock.
//!
//! While the floor refuses, a launch that would add a process waits queued
//! with a named shortfall (`LaneShortfall::PipeHealth`), whatever memory says.
//! A warm continuation, a stream on a resident server, and a stateless HTTP
//! call add no process and are untouched. When the floor clears, by measured
//! recovery or by evidence expiring into a named gap, every process lane is
//! promoted again, so nothing waits on an unrelated process exit.
//!
//! The operator sees one hold per engagement, workspace-wide rather than under
//! any project, because the condition is the host's and no project caused it.

use std::sync::{Arc, Mutex};

use cairn_common::pipe_health::{
    PipeHealthAssessment, PipeHealthTransition, RECOVERED_AT_BYTES, RECOVERY_SAMPLES,
};

use crate::notifications::{ExpectedRelease, Hold, HoldSource};
use crate::orchestrator::Orchestrator;

const SOURCE: &str = "pipe-health";

/// The lanes the floor holds, in the operator's words.
const AFFECTED: &str = "new Claude and Codex sessions, process-backed calls, and new Codex \
                        app-servers on this runner's host; running sessions, warm \
                        continuations and stateless calls are not held";

#[derive(Debug, Clone)]
struct Engagement {
    id: String,
    reason: String,
    opened_at: i64,
    released_at: Option<i64>,
}

/// The floor's operator-visible record: at most one engagement at a time,
/// opened when the floor engages and released, with how it cleared, when it
/// clears. Repeated samples inside one engagement change nothing.
#[derive(Default)]
pub(crate) struct PipeFloorNotices {
    engagement: Mutex<Option<Engagement>>,
}

impl PipeFloorNotices {
    /// Fold one transition in. Returns whether the record changed, which is
    /// when the notification center needs to hear about it.
    pub(crate) fn observe(&self, transition: &PipeHealthTransition, now: i64) -> bool {
        let mut engagement = self.engagement.lock().unwrap_or_else(|e| e.into_inner());
        if transition.engaged() {
            *engagement = Some(Engagement {
                id: uuid::Uuid::new_v4().to_string(),
                reason: format!(
                    "This runner's host is refusing new agent processes: {}. Held: {AFFECTED}. \
                     It clears after {RECOVERY_SAMPLES} spaced readings of at least \
                     {RECOVERED_AT_BYTES} B.",
                    transition.to.describe()
                ),
                opened_at: now,
                released_at: None,
            });
            return true;
        }
        if let (Some(clearance), Some(open)) = (transition.clearance(), engagement.as_mut()) {
            if open.released_at.is_none() {
                open.released_at = Some(now);
                open.reason = format!(
                    "{} Cleared by {clearance}: {}.",
                    open.reason,
                    transition.to.describe()
                );
                return true;
            }
        }
        false
    }
}

impl HoldSource for PipeFloorNotices {
    fn name(&self) -> &'static str {
        SOURCE
    }

    fn holds(&self, _project_id: &str) -> Vec<Hold> {
        let engagement = self.engagement.lock().unwrap_or_else(|e| e.into_inner());
        let Some(engagement) = engagement.as_ref() else {
            return Vec::new();
        };
        if engagement.released_at.is_some_and(|at| {
            at < chrono::Utc::now().timestamp() - crate::notifications::RECENT_WINDOW_SECS
        }) {
            return Vec::new();
        }
        vec![Hold {
            id: format!("{SOURCE}:{}", engagement.id),
            project_id: None,
            reason: engagement.reason.clone(),
            expected_release: Some(ExpectedRelease::Text(
                "When fresh pipes on this host measure healthy again".into(),
            )),
            override_label: "Clears on measurement".into(),
            source_uri: Some("cairn://executors".into()),
            opened_at: engagement.opened_at,
            released_at: engagement.released_at,
        }]
    }

    fn override_hold(&self, _hold_id: &str) -> Result<(), String> {
        Err(
            "the fresh-pipe floor clears on measurement, not on request: a process started \
             now could block forever on its first pipe write"
                .into(),
        )
    }
}

impl Orchestrator {
    /// The runner host's fresh-pipe floor when it refuses new processes now,
    /// assessed at the moment of asking. `None` admits, including under a
    /// named gap (never sampled, not applicable, or stale past the hold).
    pub(crate) fn process_floor_refusal(&self) -> Option<PipeHealthAssessment> {
        let assessment = self.pipe_health.assess_now();
        (!assessment.admits()).then_some(assessment)
    }

    /// The floor's answer now, admitting or not, for the surfaces that show it.
    pub(crate) fn process_floor(&self) -> PipeHealthAssessment {
        self.pipe_health.assess_now()
    }

    /// Register the floor's notices and start sampling this host. Each
    /// transition updates the operator's hold, and a clearance promotes every
    /// process lane at once.
    pub(crate) fn spawn_pipe_floor(&self) {
        let notices = Arc::new(PipeFloorNotices::default());
        self.register_hold_source(notices.clone());
        let orch = self.clone();
        let started = self.pipe_health.spawn_sampler(move |transition| {
            if transition.engaged() {
                log::warn!(
                    "runner host fresh-pipe floor engaged: {}",
                    transition.to.describe()
                );
            } else {
                log::info!(
                    "runner host fresh-pipe floor changed from {} ({}): {}",
                    transition.from.label(),
                    transition.clearance().unwrap_or("still held"),
                    transition.to.describe()
                );
            }
            if notices.observe(&transition, chrono::Utc::now().timestamp()) {
                let _ = orch.services.emitter.emit(
                    "db-change",
                    serde_json::json!({ "table": crate::notifications::HOLDS_TABLE }),
                );
            }
            if transition.cleared() {
                let promoter = orch.clone();
                orch.spawn_blocking_detached(move || {
                    crate::execution::jobs::promote_private_lanes_blocking(&promoter);
                    crate::execution::jobs::promote_call_lanes(&promoter);
                });
            }
        });
        if let Err(error) = started {
            log::warn!("could not start the runner's pipe-health sampler; the floor stays unmeasured: {error}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_common::executor_protocol::Measurement;
    use cairn_common::pipe_health::{PipeAdmission, PipeCapacity, PipeHealth};

    fn assessment(bytes: u64, recovered: bool) -> PipeHealthAssessment {
        let mut health = PipeHealth::new(true);
        let reading = |at| {
            Measurement::measured(
                at,
                PipeCapacity {
                    accepted_bytes: bytes,
                    at_least: false,
                },
            )
        };
        if recovered {
            health.observe(Measurement::measured(
                0,
                PipeCapacity {
                    accepted_bytes: 512,
                    at_least: false,
                },
            ));
            health.observe(reading(5_000));
            health.observe(reading(10_000));
            return health.assess(10_000);
        }
        health.observe(reading(0));
        health.assess(0)
    }

    #[test]
    fn one_notice_per_engagement_and_its_clearance_is_named() {
        let notices = PipeFloorNotices::default();
        let engaged = PipeHealthTransition {
            from: PipeAdmission::Healthy,
            to: assessment(512, false),
        };
        assert!(notices.observe(&engaged, 100));
        let holds = notices.holds("any");
        assert_eq!(holds.len(), 1);
        assert_eq!(
            holds[0].project_id, None,
            "the host's condition, not a project's"
        );
        assert!(holds[0].reason.contains("512 B"), "{}", holds[0].reason);
        assert!(holds[0].reason.contains("process-backed calls"));
        let first_id = holds[0].id.clone();

        // A degraded-to-stale-hold move is not a new engagement.
        let still_held = PipeHealthTransition {
            from: PipeAdmission::Degraded,
            to: PipeHealthAssessment {
                admission: PipeAdmission::StaleHold,
                ..assessment(512, false)
            },
        };
        assert!(!notices.observe(&still_held, 110));
        assert_eq!(notices.holds("any")[0].id, first_id);

        let cleared = PipeHealthTransition {
            from: PipeAdmission::Degraded,
            to: assessment(65_536, true),
        };
        let now = chrono::Utc::now().timestamp();
        assert!(notices.observe(&cleared, now));
        let holds = notices.holds("any");
        assert_eq!(holds[0].released_at, Some(now));
        assert!(
            holds[0].reason.contains("measured recovery"),
            "{}",
            holds[0].reason
        );
        assert!(
            !notices.observe(&cleared, now + 10),
            "a clearance is recorded once"
        );
        assert!(notices.override_hold(&first_id).is_err());
    }

    #[test]
    fn expiry_into_a_gap_is_not_reported_as_recovery() {
        let notices = PipeFloorNotices::default();
        notices.observe(
            &PipeHealthTransition {
                from: PipeAdmission::Healthy,
                to: assessment(512, false),
            },
            1,
        );
        notices.observe(
            &PipeHealthTransition {
                from: PipeAdmission::StaleHold,
                to: PipeHealthAssessment::not_reported("stale"),
            },
            chrono::Utc::now().timestamp(),
        );
        let reason = &notices.holds("any")[0].reason;
        assert!(reason.contains("not recovered"), "{reason}");
    }
}
