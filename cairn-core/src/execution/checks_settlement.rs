//! Whether a node's check lanes have stopped moving, and what they stopped on.
//!
//! One predicate, shared by the two surfaces that ask it: the `waitFor`
//! condition an agent parks its turn on ([`crate::mcp::handlers::owned_wait`]),
//! and the elective `checks` wake it subscribes to instead when its turn is
//! otherwise complete ([`crate::orchestrator::wakes`]). Both need the same
//! answer, and an orchestrator that merges on one and is woken by the other must
//! not be told two different things about the same node.
//!
//! Settling is not the same as passing. A lane can stop moving without ever
//! producing a verdict -- a wave withdrawn for a resolved issue, a host restart
//! mid-suite, a check suppressed after repeated infrastructure failures, a tree
//! identical to its base that runs nothing at all. Those are terminal states
//! too, and a waiter told to keep waiting for them waits forever. So settlement
//! includes them and NAMES them, rather than implying a verdict is still coming.

use crate::execution::checks_status::{
    format_status_annotation, node_check_statuses, NodeCheckState, NodeCheckStatus,
};
use crate::messages::delivery::HeadTurn;
use crate::orchestrator::Orchestrator;

/// How long a verdictless reading must hold before it is believed.
///
/// A turn's completion and the arming of its check wave are two steps of one
/// synchronous hook, not one atomic act: the turn row reads `completed` for as
/// long as it takes `spawn_turn_end_checks` to run its topology and
/// launchability queries and claim the single-flight slot. A poll landing in
/// that window sees an idle node with no wave in flight -- indistinguishable, on
/// state alone, from a node whose wave died. Believing it at once would report
/// every lane as verdictless at exactly the moment they were all about to run.
///
/// The dwell is paid only by the ambiguous reading. A node whose lanes all hold
/// real verdicts settles the instant it is observed, because nothing about that
/// reading is racy.
pub(crate) const VERDICTLESS_DWELL_MS: i64 = 10_000;

/// What a node's watched check lanes are doing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Settlement {
    /// Nothing is going to change these lanes further. `verdictless` names the
    /// lanes that stopped without ever producing a verdict.
    Settled { verdictless: Vec<String> },
    /// Something is still expected to move them.
    Moving(Moving),
}

/// Why a node has not settled, in the terms an agent reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Moving {
    /// Lanes executing right now.
    pub(crate) running: Vec<String>,
    /// Lanes holding no verdict that a wave is still expected to produce.
    pub(crate) awaiting: Vec<String>,
    /// One sentence naming what is still to happen.
    pub(crate) reason: String,
}

impl Settlement {
    pub(crate) fn is_settled(&self) -> bool {
        matches!(self, Settlement::Settled { .. })
    }
}

/// Classify one snapshot of a node's watched lanes.
///
/// Pure and total, so every state it can report is reachable in a test without a
/// repository, a wave, or a live agent. `statuses` is already narrowed to the
/// watched lanes, which is the whole of the per-suite form: waiting on one suite
/// is waiting on a node whose lane set happens to have one member.
///
/// The order of the arms is the order of certainty. A running lane is the most
/// concrete thing that can be said. A live turn is next: a node still working is
/// still changing the tree its lanes describe, so even an all-green reading of
/// it is a verdict about a tree that is about to be replaced -- merging on that
/// would race the agent's next commit. Only when neither holds does the absence
/// of a wave mean the lanes have stopped.
pub(crate) fn classify(
    statuses: &[NodeCheckStatus],
    turn: HeadTurn,
    wave_in_flight: bool,
) -> Settlement {
    let named = |state: NodeCheckState| -> Vec<String> {
        statuses
            .iter()
            .filter(|status| status.state == state)
            .map(|status| status.name.clone())
            .collect()
    };
    let running = named(NodeCheckState::Running);
    let pending = named(NodeCheckState::Pending);

    if !running.is_empty() {
        let reason = format!(
            "{} still executing",
            plural_lanes(running.len(), "lane is", "lanes are")
        );
        return Settlement::Moving(Moving {
            running,
            awaiting: pending,
            reason,
        });
    }
    if let Some(mid_work) = turn.mid_work_reason() {
        return Settlement::Moving(Moving {
            running,
            awaiting: pending,
            reason: format!("the node has not finished its turn: {mid_work}"),
        });
    }
    // A wave in flight holds open only the lanes it might still produce, which
    // is what makes the per-suite form work: a wait narrowed to one suite that
    // has already returned its verdict is answered NOW, even while unrelated
    // lanes of the same wave keep running. Treating the node-wide flag as
    // moving unconditionally would park such a wait until the whole wave ended,
    // which is the whole-node answer wearing the per-suite form's name.
    if wave_in_flight && !pending.is_empty() {
        return Settlement::Moving(Moving {
            running,
            awaiting: pending,
            reason: "a check wave is in flight and has not selected these lanes yet".to_string(),
        });
    }
    Settlement::Settled {
        verdictless: pending,
    }
}

fn plural_lanes(count: usize, one: &str, many: &str) -> String {
    format!("{count} {}", if count == 1 { one } else { many })
}

/// A node's watched lanes and what they are doing, taken together.
#[derive(Debug, Clone)]
pub(crate) struct ChecksSnapshot {
    /// The watched lanes, in the order the `/checks` resource renders them.
    pub(crate) statuses: Vec<NodeCheckStatus>,
    pub(crate) settlement: Settlement,
}

impl ChecksSnapshot {
    /// The one word an orchestrator branches on.
    ///
    /// Neither `incomplete` nor `not_applicable` is folded into a verdict: a
    /// lane that never ran is not a pass, and calling it a failure would send an
    /// agent hunting for a red suite that does not exist. The distinction earns
    /// its keep hardest at single-suite granularity, where "did rust-tests
    /// pass?" answered `passed` by a lane the impact gate excluded is simply a
    /// false statement about a suite that never ran.
    pub(crate) fn verdict(&self) -> &'static str {
        let verdictless = match &self.settlement {
            Settlement::Settled { verdictless } => !verdictless.is_empty(),
            Settlement::Moving(_) => return "moving",
        };
        let any = |state: NodeCheckState| self.statuses.iter().any(|s| s.state == state);
        if any(NodeCheckState::Failed) {
            "failed"
        } else if verdictless {
            "incomplete"
        } else if !any(NodeCheckState::Passed) && any(NodeCheckState::NotApplicable) {
            "not_applicable"
        } else {
            "passed"
        }
    }

    /// A one-line tally of the watched lanes, for the resume's headline.
    pub(crate) fn tally(&self) -> String {
        let count = |state: NodeCheckState| {
            self.statuses
                .iter()
                .filter(|status| status.state == state)
                .count()
        };
        let mut parts = Vec::new();
        for (count, label) in [
            (count(NodeCheckState::Passed), "passed"),
            (count(NodeCheckState::Failed), "failed"),
            (count(NodeCheckState::NotApplicable), "not applicable"),
            (count(NodeCheckState::Running), "running"),
            (count(NodeCheckState::Pending), "no verdict"),
        ] {
            if count > 0 {
                parts.push(format!("{count} {label}"));
            }
        }
        if parts.is_empty() {
            return "no check lanes".to_string();
        }
        parts.join(", ")
    }

    /// Every watched lane on one line each, the way the resume inlines them.
    ///
    /// Deliberately terse next to the `/checks` resource's own render: a resume
    /// carries the shape of the answer and the URI to read for the rest, not a
    /// suite's output.
    pub(crate) fn lane_lines(&self) -> Vec<String> {
        self.statuses
            .iter()
            .map(|status| {
                let state = match status.state {
                    NodeCheckState::Passed => "passed",
                    NodeCheckState::Failed => "failed",
                    NodeCheckState::Running => "running",
                    NodeCheckState::NotApplicable => "not applicable",
                    // A pending lane in a SETTLED snapshot is one that stopped
                    // without a verdict, so it says that rather than "pending",
                    // which would read as "still queued".
                    NodeCheckState::Pending if self.settlement.is_settled() => "no verdict",
                    NodeCheckState::Pending => "pending",
                };
                match format_status_annotation(status) {
                    Some(annotation) => format!("- {} [{state}] {annotation}", status.name),
                    None => format!("- {} [{state}]", status.name),
                }
            })
            .collect()
    }
}

/// Take a node's settlement snapshot: its lane statuses, narrowed to `suite`
/// when one is named, plus the classification of what they are doing.
///
/// `Err` means the question itself does not resolve -- an unknown job, a node
/// with no resolvable check contract, a suite the node does not configure. The
/// arming path surfaces that as a refusal; a poll loop that has already armed
/// treats it as a transient and keeps waiting, since a momentarily unreadable
/// repository says nothing about whether the lanes settled.
pub(crate) async fn node_checks_settlement(
    orch: &Orchestrator,
    job_id: &str,
    suite: Option<&str>,
) -> Result<ChecksSnapshot, String> {
    let statuses = node_check_statuses(orch, job_id).await.ok_or_else(|| {
        "this node has no resolvable check contract (no worktree, recipe node, or \
         .cairn/config.yaml at its head)"
            .to_string()
    })?;
    let statuses = match suite {
        None => statuses,
        Some(suite) => {
            let matched: Vec<NodeCheckStatus> = statuses
                .iter()
                .filter(|status| status.name == suite)
                .cloned()
                .collect();
            if matched.is_empty() {
                let configured = statuses
                    .iter()
                    .map(|status| status.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                let configured = if configured.is_empty() {
                    "this node configures no checks".to_string()
                } else {
                    format!("configured suites: {configured}")
                };
                return Err(format!("unknown check suite '{suite}'; {configured}"));
            }
            matched
        }
    };
    let turn = crate::messages::delivery::head_turn_for_job_async(orch, job_id).await;
    let wave_in_flight =
        orch.turn_end_checks_in_flight(job_id) || orch.write_checks_in_flight(job_id);
    let settlement = classify(&statuses, turn, wave_in_flight);
    Ok(ChecksSnapshot {
        statuses,
        settlement,
    })
}

/// The node a checks wait or wake is aimed at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ChecksTarget {
    /// The canonical `.../checks` URI, which is also the key both surfaces match
    /// on: the wait polls it and the wake routes to it.
    pub(crate) uri: String,
    pub(crate) job_id: String,
    /// The node (or `node/task`) segment, for a message that has to name what it
    /// is talking about without re-parsing the URI.
    pub(crate) label: String,
}

/// A checks ref parsed down to the node it names, before any database is asked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ChecksCoords {
    pub(crate) canonical: String,
    pub(crate) project: String,
    pub(crate) number: i32,
    pub(crate) exec_seq: i32,
    pub(crate) node_id: String,
    pub(crate) task_name: Option<String>,
}

/// Parse an agent-supplied checks `ref` into the node coordinates it names.
///
/// Accepts the canonical `.../checks` URI and the bare node/task URI it hangs
/// off, because an agent that has a node URI in hand should not have to know
/// which suffix this surface wants. `home` is the caller's own node URI, used to
/// expand a `cairn:~/` reference; pass it already resolved.
pub(crate) fn checks_coords(reference: &str, home: Option<&str>) -> Result<ChecksCoords, String> {
    use cairn_common::uri::{parse_uri, CairnResource};

    let reference = reference.trim();
    let expanded = if let Some(rest) = reference.strip_prefix("cairn:~/") {
        let home = home.ok_or("a cairn:~/ checks ref needs a resolvable home node")?;
        format!("{}/{}", home.trim_end_matches('/'), rest)
    } else {
        reference.to_string()
    };
    let canonical = expanded.trim_end_matches('/');
    // A bare node URI is the same target as its checks collection, so it is
    // accepted and normalized rather than refused on a missing suffix.
    let canonical = match parse_uri(canonical) {
        Some(CairnResource::Node { .. }) | Some(CairnResource::Task { .. }) => {
            format!("{canonical}/checks")
        }
        _ => canonical.to_string(),
    };
    let (project, number, exec_seq, node_id, task_name) = match parse_uri(&canonical) {
        Some(CairnResource::NodeChecks {
            project,
            number,
            exec_seq,
            node_id,
        }) => (project, number, exec_seq, node_id, None),
        Some(CairnResource::TaskChecks {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
        }) => (project, number, exec_seq, node_id, Some(task_name)),
        _ => {
            return Err(format!(
                "checks ref must be a node or task checks URI \
                 (e.g. cairn://p/CAIRN/3427/1/builder/checks or cairn:~/checks); got '{reference}'"
            ))
        }
    };
    Ok(ChecksCoords {
        canonical,
        project,
        number,
        exec_seq,
        node_id,
        task_name,
    })
}

/// Resolve an agent-supplied checks `ref` to the node and job it addresses.
pub(crate) async fn resolve_checks_target(
    orch: &Orchestrator,
    reference: &str,
    home: Option<&str>,
) -> Result<ChecksTarget, String> {
    let coords = checks_coords(reference, home)?;
    let ChecksCoords {
        canonical,
        project,
        number,
        exec_seq,
        node_id,
        task_name,
    } = coords;
    let db = orch.db.for_project(&project).await;
    let job_id = crate::resources::resolve_node_or_task_job_id(
        &db,
        &project,
        number,
        exec_seq,
        &node_id,
        task_name.as_deref(),
    )
    .await?;
    let label = match &task_name {
        None => node_id,
        Some(task_name) => format!("{node_id}/{task_name}"),
    };
    Ok(ChecksTarget {
        uri: canonical,
        job_id,
        label,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lane(name: &str, state: NodeCheckState) -> NodeCheckStatus {
        NodeCheckStatus {
            job_id: "job".to_string(),
            request_id: None,
            name: name.to_string(),
            state,
            policy: "advisory".to_string(),
            when: "review".to_string(),
            cached: None,
            duration_ms: None,
            ran_at: None,
            passed: None,
            failed: None,
            skipped: None,
            suite_failures: None,
            failure_names: Vec::new(),
            output_tail: None,
            failure_kind: None,
            suppressed_after: None,
        }
    }

    fn moving(settlement: &Settlement) -> &Moving {
        match settlement {
            Settlement::Moving(moving) => moving,
            Settlement::Settled { verdictless } => {
                panic!("expected a moving node, got settled with verdictless {verdictless:?}")
            }
        }
    }

    /// The whole point of the condition, and the specimen it was built from: two
    /// lanes green, two still executing. An orchestrator told this had settled
    /// would merge a PR whose tests had not finished.
    #[test]
    fn a_running_lane_is_never_settled() {
        let statuses = [
            lane("migrations", NodeCheckState::Passed),
            lane("rust-fmt", NodeCheckState::Passed),
            lane("rust-lint", NodeCheckState::Running),
            lane("rust-tests", NodeCheckState::Running),
        ];
        let settlement = classify(&statuses, HeadTurn::Idle, true);
        assert_eq!(
            moving(&settlement).running,
            vec!["rust-lint".to_string(), "rust-tests".to_string()]
        );
        assert!(moving(&settlement).reason.contains("executing"));
    }

    /// A node still working is still changing the tree its lanes describe, so an
    /// all-green reading of it is a verdict about a tree that is about to be
    /// replaced. Settling there would hand a merging orchestrator a green it
    /// would race the agent's next commit to act on.
    #[test]
    fn a_node_mid_turn_is_not_settled_even_when_every_lane_is_green() {
        let statuses = [
            lane("rust-tests", NodeCheckState::Passed),
            lane("typecheck", NodeCheckState::Passed),
        ];
        for turn in [HeadTurn::Live, HeadTurn::SelfSuspended] {
            let settlement = classify(&statuses, turn, false);
            assert!(
                moving(&settlement)
                    .reason
                    .contains("has not finished its turn"),
                "{turn:?} must read as unfinished work, got {settlement:?}"
            );
        }
        assert!(classify(&statuses, HeadTurn::Idle, false).is_settled());
    }

    /// A wave that has claimed its slot but not yet started a lane holds every
    /// lane pending. That is the ordinary launch window, not a dead wave.
    #[test]
    fn a_wave_in_flight_holds_pending_lanes_open() {
        let statuses = [lane("rust-tests", NodeCheckState::Pending)];
        let settlement = classify(&statuses, HeadTurn::Idle, true);
        assert_eq!(moving(&settlement).awaiting, vec!["rust-tests".to_string()]);
        assert!(moving(&settlement).reason.contains("in flight"));
    }

    /// The per-suite form IS the whole-node predicate over a one-member lane
    /// set, and this is what has to be true for that to hold: a wave in flight
    /// holds open the lanes it might still produce, not every lane there is.
    ///
    /// The two calls below are the two lane sets `node_checks_settlement` hands
    /// to `classify` for the same instant of the same node -- the whole node,
    /// and the node narrowed to `rust-tests`. `rust-tests` has returned;
    /// `rust-lint` is still going. A suite-scoped wait must be answered now.
    #[test]
    fn a_suite_that_has_returned_settles_while_the_rest_of_its_wave_runs() {
        let whole_node = [
            lane("rust-lint", NodeCheckState::Running),
            lane("rust-tests", NodeCheckState::Passed),
        ];
        assert!(
            !classify(&whole_node, HeadTurn::Idle, true).is_settled(),
            "the node as a whole is still moving"
        );

        let narrowed = [lane("rust-tests", NodeCheckState::Passed)];
        assert_eq!(
            classify(&narrowed, HeadTurn::Idle, true),
            Settlement::Settled {
                verdictless: Vec::new()
            },
            "a suite with a verdict must not be parked on unrelated lanes of its wave"
        );

        // The wave still holds open a selected lane that has NOT returned, which
        // is the signal's real job.
        let not_yet = [lane("rust-tests", NodeCheckState::Pending)];
        assert!(!classify(&not_yet, HeadTurn::Idle, true).is_settled());
    }

    /// The honest bound. A lane with no verdict, no wave to produce one, and an
    /// idle node has stopped -- an interrupted wave, a suppressed check, a tree
    /// identical to its base. Waiting for it is waiting forever, so it settles
    /// and says which lanes came back empty.
    #[test]
    fn a_lane_nothing_will_run_settles_as_verdictless() {
        let statuses = [
            lane("rust-tests", NodeCheckState::Passed),
            lane("rust-lint", NodeCheckState::Pending),
        ];
        assert_eq!(
            classify(&statuses, HeadTurn::Idle, false),
            Settlement::Settled {
                verdictless: vec!["rust-lint".to_string()]
            }
        );
    }

    /// A check the impact gate excluded will never run, and that is a decided
    /// outcome rather than a gap. It settles without being reported as a lane
    /// that came back empty.
    #[test]
    fn a_not_applicable_lane_settles_without_counting_as_verdictless() {
        let statuses = [
            lane("rust-tests", NodeCheckState::Passed),
            lane("frontend-tests", NodeCheckState::NotApplicable),
        ];
        assert_eq!(
            classify(&statuses, HeadTurn::Idle, false),
            Settlement::Settled {
                verdictless: Vec::new()
            }
        );
    }

    /// A project with no configured checks has nothing to wait for, and says so
    /// immediately rather than parking a turn on an empty set forever.
    #[test]
    fn a_node_with_no_lanes_is_settled() {
        assert!(classify(&[], HeadTurn::Idle, false).is_settled());
    }

    /// The one word an orchestrator branches on. `incomplete` is its own answer:
    /// a lane that never ran is not a pass, and calling it a failure would send
    /// an agent hunting for a red suite that does not exist.
    #[test]
    fn verdict_separates_incomplete_from_passed_and_failed() {
        let snapshot = |statuses: Vec<NodeCheckStatus>| {
            let settlement = classify(&statuses, HeadTurn::Idle, false);
            ChecksSnapshot {
                statuses,
                settlement,
            }
        };
        assert_eq!(
            snapshot(vec![lane("a", NodeCheckState::Passed)]).verdict(),
            "passed"
        );
        assert_eq!(
            snapshot(vec![
                lane("a", NodeCheckState::Passed),
                lane("b", NodeCheckState::Failed)
            ])
            .verdict(),
            "failed"
        );
        assert_eq!(
            snapshot(vec![
                lane("a", NodeCheckState::Passed),
                lane("b", NodeCheckState::Pending)
            ])
            .verdict(),
            "incomplete"
        );
        // A red lane beside an empty one is still red: the failure is the
        // actionable fact, and burying it under "incomplete" would hide it.
        assert_eq!(
            snapshot(vec![
                lane("a", NodeCheckState::Failed),
                lane("b", NodeCheckState::Pending)
            ])
            .verdict(),
            "failed"
        );
        // Nothing ran, because nothing applied. Saying "passed" here would be a
        // false statement about a suite that never executed -- which is exactly
        // what a single-suite wait asked about.
        assert_eq!(
            snapshot(vec![lane("a", NodeCheckState::NotApplicable)]).verdict(),
            "not_applicable"
        );
        // But a green node with an excluded lane beside it did pass.
        assert_eq!(
            snapshot(vec![
                lane("a", NodeCheckState::Passed),
                lane("b", NodeCheckState::NotApplicable)
            ])
            .verdict(),
            "passed"
        );
    }

    /// A pending lane means two different things either side of settlement, and
    /// the rendering has to say which: still queued, or never going to run.
    #[test]
    fn a_pending_lane_renders_as_no_verdict_only_once_settled() {
        let statuses = vec![lane("rust-lint", NodeCheckState::Pending)];
        let settled = ChecksSnapshot {
            settlement: classify(&statuses, HeadTurn::Idle, false),
            statuses: statuses.clone(),
        };
        assert_eq!(settled.lane_lines(), vec!["- rust-lint [no verdict]"]);
        let moving = ChecksSnapshot {
            settlement: classify(&statuses, HeadTurn::Idle, true),
            statuses,
        };
        assert_eq!(moving.lane_lines(), vec!["- rust-lint [pending]"]);
    }

    #[test]
    fn a_checks_ref_accepts_the_shapes_an_agent_actually_holds() {
        let home = "cairn://p/CAIRN/3437/1/builder";
        for reference in [
            "cairn://p/CAIRN/3427/1/builder/checks",
            // The bare node URI names the same node; requiring the suffix would
            // be a spelling test, not a scoping one.
            "cairn://p/CAIRN/3427/1/builder",
            "cairn://p/CAIRN/3427/1/builder/checks/",
        ] {
            let coords = checks_coords(reference, Some(home)).expect(reference);
            assert_eq!(coords.canonical, "cairn://p/CAIRN/3427/1/builder/checks");
            assert_eq!(coords.number, 3427);
            assert_eq!(coords.node_id, "builder");
            assert_eq!(coords.task_name, None);
        }

        let own = checks_coords("cairn:~/checks", Some(home)).unwrap();
        assert_eq!(own.canonical, format!("{home}/checks"));

        let task =
            checks_coords("cairn://p/CAIRN/3427/1/builder/task/review/checks", None).unwrap();
        assert_eq!(task.task_name.as_deref(), Some("review"));
    }

    /// A ref that names something other than a node's lanes is refused with the
    /// shape it should have had, not a bare parse failure.
    #[test]
    fn a_ref_that_is_not_a_node_is_refused_with_the_shape_it_needed() {
        for reference in [
            "cairn://p/CAIRN/3427",
            "cairn:~/terminal/dev",
            "rust-tests",
            "",
        ] {
            let error =
                checks_coords(reference, Some("cairn://p/CAIRN/1/1/builder")).expect_err(reference);
            assert!(
                error.contains("node or task checks URI"),
                "{reference}: {error}"
            );
        }
        assert!(checks_coords("cairn:~/checks", None)
            .unwrap_err()
            .contains("home node"));
    }
}
