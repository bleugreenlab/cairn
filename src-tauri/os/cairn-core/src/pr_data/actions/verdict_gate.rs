//! Verdict existence at landing: a pull request lands on the project's default
//! branch only if its head carries a complete verdict for every check its change
//! makes applicable.
//!
//! The question is whether the head was judged, never how it came out. A red
//! does not stop a merge here, whatever the check's policy; an applicable check
//! with no verdict at all does, because the absence of a verdict is not a
//! verdict of any colour, and a merge that carried none rendered exactly like
//! one whose checks had passed. What counts as complete is
//! [`crate::execution::checks_status::LaneExistence`], read from the node's own
//! lane projection (the same rows `/checks` shows, with impact, input-hash reuse
//! and same-patch carry-forward already applied), and then, for any lane that
//! projection leaves open, from every verdict recorded at the head commit by any
//! cadence, so that a manual whole-suite `cairn check run` answers it.
//!
//! Only the operator may land a head this gate refuses, with a reason. The
//! waiver names the suites it covers and is journaled with the merge and
//! written on the issue, so a merge without verdicts is never a silent one.

use std::path::Path;
use std::sync::Arc;

use crate::execution::checks_status::{
    format_status_annotation, lane_existence, row_existence, LaneExistence, NodeCheckState,
    NodeCheckStatus, WaveCadence,
};
use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;

use super::context::MergeMrContext;
use super::resolution::PrResolutionAttribution;

/// The phrase every refusal carries, and the one the PR view looks for to offer
/// the operator's waiver.
pub const NO_VERDICT: &str = "no complete verdict";

/// The operator's explicit decision to land a head without a complete verdict
/// for every applicable check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerdictWaiver {
    pub reason: String,
}

/// One applicable check, and what its head holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SuiteVerdict {
    pub suite: String,
    pub policy: String,
    pub existence: LaneExistence,
    /// The lane's row at this head, as `/checks` annotates it, when it has one.
    pub last_row: Option<String>,
    /// Set to whether it passed, when the verdict rests on an exit code alone
    /// (no structured result): counted as complete in the interim before
    /// cairn/4941 gives its runner a parser.
    pub exit_code_verdict: Option<bool>,
}

/// What the gate read about a head.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum HeadVerdicts {
    /// Every applicable check and its state. Empty when the head declares no
    /// checks, or none applies to its change.
    Read(Vec<SuiteVerdict>),
    /// Whether the head carries verdicts could not be established.
    Unreadable(String),
}

/// What the gate established, for the landing that follows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct VerdictGate {
    /// The head the verdicts were read for; the landing is bound to it.
    pub head: String,
    /// The operator's waiver as the merge journal and the issue record it.
    pub waived: Option<String>,
}

fn short(commit: &str) -> &str {
    commit.get(..12).unwrap_or(commit)
}

/// Gate a landing on the project's default branch. `Ok` carries the head the
/// landing must honour and any waiver to record; `Err` is the refusal.
pub(super) async fn gate_verdict_existence(
    orch: &Orchestrator,
    db: &Arc<LocalDb>,
    merge_context: &MergeMrContext,
    waiver: Option<&VerdictWaiver>,
    attribution: Option<&PrResolutionAttribution>,
) -> Result<VerdictGate, String> {
    let source = merge_context.source_branch.as_str();
    if waiver.is_some() && !super::landing_gate::is_operator(attribution) {
        return Err(format!(
            "Refusing to merge `{source}`: landing without a verdict is the operator's act, \
             and this merge was requested by {}. Ask the operator to land it.",
            attribution.map_or("an unattributed caller", |value| value.actor_kind)
        ));
    }
    let head = {
        let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
        let store =
            crate::jj::project_store_dir(&orch.config_dir, Path::new(&merge_context.mr.repo_path));
        crate::jj::bookmark_commit_checked(&jj, &store, source)?.ok_or_else(|| {
            format!("Refusing to merge `{source}`: the branch does not exist in the store.")
        })?
    };
    let verdicts = head_verdicts(orch, db, merge_context, &head).await;
    let actor = attribution
        .and_then(|value| value.actor_identity.as_deref())
        .or(attribution.map(|value| value.actor_kind))
        .unwrap_or("operator");
    let waived = decide(
        source,
        &merge_context.target_branch,
        &head,
        &verdicts,
        waiver.map(|waiver| (waiver, actor)),
    )?;
    Ok(VerdictGate { head, waived })
}

/// Read what `head` carries for every check that applies to the change.
async fn head_verdicts(
    orch: &Orchestrator,
    db: &Arc<LocalDb>,
    merge_context: &MergeMrContext,
    head: &str,
) -> HeadVerdicts {
    let repository = Path::new(&merge_context.mr.repo_path);
    match crate::execution::checks::checks_declared_at_commit(repository, head).await {
        Ok(false) => return HeadVerdicts::Read(Vec::new()),
        Ok(true) => {}
        Err(error) => return HeadVerdicts::Unreadable(error),
    }
    let job_id = merge_context.mr.job_id.as_str();
    // The lanes answer the node's head. A node whose branch is not the one
    // landing would be answering for something else.
    match crate::execution::cache::resolve_job_logical_head(orch, job_id).await {
        Ok(node_head) if node_head == head => {}
        Ok(node_head) => {
            return HeadVerdicts::Unreadable(format!(
                "the node's lanes answer its head {}, not the branch head {}",
                short(&node_head),
                short(head)
            ))
        }
        Err(error) => {
            return HeadVerdicts::Unreadable(format!("the node's head is unresolvable: {error}"))
        }
    }
    let Some(statuses) = crate::execution::checks_status::node_check_statuses(orch, job_id).await
    else {
        return HeadVerdicts::Unreadable("the node's check lanes could not be read".to_string());
    };
    let mut suites = applicable_suites(&statuses, WaveCadence::of(orch, job_id));
    let open: Vec<String> = suites
        .iter()
        .filter(|suite| suite.existence.blocks_landing())
        .map(|suite| suite.suite.clone())
        .collect();
    if !open.is_empty() {
        match super::pin_bump::commit_verdicts(db, &merge_context.project_id, head, &open).await {
            Ok(recorded) => answer_from_commit(&mut suites, &recorded),
            // The projection's answer stands; a lane it left open stays open.
            Err(error) => log::warn!("verdict gate: commit verdicts at {head}: {error}"),
        }
    }
    HeadVerdicts::Read(suites)
}

/// Every lane of the projection that applies to the head, with its state.
pub(crate) fn applicable_suites(
    statuses: &[NodeCheckStatus],
    cadence: WaveCadence,
) -> Vec<SuiteVerdict> {
    statuses
        .iter()
        .filter_map(|status| {
            lane_existence(status, cadence).map(|existence| SuiteVerdict {
                suite: status.name.clone(),
                policy: status.policy.clone(),
                exit_code_verdict: (existence.is_complete() && status.parser.is_none())
                    .then_some(status.state != NodeCheckState::Failed),
                existence,
                last_row: format_status_annotation(status),
            })
        })
        .collect()
}

/// A lane the projection left open is complete when any verdict recorded at
/// the head commit is: a manual whole-suite run keys differently from a
/// selector suite's lane and never shows on it, but it judged this head.
pub(crate) fn answer_from_commit(
    suites: &mut [SuiteVerdict],
    recorded: &[super::pin_bump::CollectedVerdict],
) {
    for suite in suites.iter_mut().filter(|s| s.existence.blocks_landing()) {
        let answer = recorded.iter().find(|verdict| {
            verdict.check == suite.suite
                && row_existence(
                    verdict.passed,
                    verdict.failure_kind.as_deref(),
                    None,
                    verdict
                        .parsed
                        .as_ref()
                        .map(|parsed| parsed.verdict_facts())
                        .as_ref(),
                )
                .is_complete()
        });
        if let Some(verdict) = answer {
            suite.existence = LaneExistence::Complete;
            suite.exit_code_verdict = verdict.parsed.is_none().then_some(verdict.passed);
        }
    }
}

/// Decide the landing from what the head carries. `Ok(None)` lands; `Ok(Some)`
/// lands under the operator's waiver, worded for the record; `Err` refuses.
pub(crate) fn decide(
    source: &str,
    target: &str,
    head: &str,
    verdicts: &HeadVerdicts,
    waiver: Option<(&VerdictWaiver, &str)>,
) -> Result<Option<String>, String> {
    let missing: Vec<&SuiteVerdict> = match verdicts {
        HeadVerdicts::Read(suites) => suites
            .iter()
            .filter(|suite| suite.existence.blocks_landing())
            .collect(),
        HeadVerdicts::Unreadable(_) => Vec::new(),
    };
    let nothing_missing = matches!(verdicts, HeadVerdicts::Read(_)) && missing.is_empty();
    match waiver {
        None if nothing_missing => Ok(None),
        None => Err(refusal(source, target, head, verdicts, &missing)),
        Some(_) if nothing_missing => Err(format!(
            "Refusing to merge `{source}`: a verdict waiver was given, but its head {} carries \
             a complete verdict for every applicable check, so there is nothing to waive. Merge \
             without it.",
            short(head)
        )),
        Some((waiver, actor)) => {
            let covers = match verdicts {
                HeadVerdicts::Unreadable(error) => {
                    format!("whether it carried verdicts could not be read ({error})")
                }
                HeadVerdicts::Read(_) => format!(
                    "without a complete verdict for {}",
                    missing
                        .iter()
                        .map(|suite| format!("{} ({})", suite.suite, suite.existence.label()))
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            };
            Ok(Some(format!(
                "verdicts waived by {actor}: {reason}. Landed head {head} {covers}",
                reason = waiver.reason,
                head = short(head),
            )))
        }
    }
}

/// What would give one check a verdict, worded for the refusal.
fn remedy(suite: &SuiteVerdict, source: &str) -> String {
    let run = format!("`cairn check run {} --branch {source}`", suite.suite);
    let row = suite.last_row.as_deref();
    match &suite.existence {
        LaneExistence::Complete | LaneExistence::NoSelectableWork => String::new(),
        LaneExistence::Absent => format!("no lane has run it on this head. {run} records one."),
        LaneExistence::Pending { running: false } => {
            "a wave has queued it and has not reported. Merge again once it does.".to_string()
        }
        LaneExistence::Pending { running: true } => {
            "a wave is running it now. Merge again once it reports.".to_string()
        }
        LaneExistence::Died { kind } => format!(
            "its run on this head ended without a verdict ({}). {run} runs it again.",
            row.unwrap_or(kind)
        ),
        LaneExistence::Breaker { after } => format!(
            "Cairn stopped running it after {after} consecutive infrastructure failures. Repair \
             the cause (for a machine, `write cairn://executors/<name> {{repaired: true}}` once it \
             is fixed) or let the paced release retry it, then {run}."
        ),
        LaneExistence::Empty => format!(
            "its run selected no test for a change its impact matched{}: the impact names files \
             its selection maps to nothing, which is the cairn/5325 defect. {run} executes the \
             suite's whole-suite command, which judges the head.",
            row.map(|row| format!(" ({row})")).unwrap_or_default()
        ),
        LaneExistence::Incomplete(reason) => {
            format!("its run did not finish ({reason}). {run} runs it again.")
        }
    }
}

fn refusal(
    source: &str,
    target: &str,
    head: &str,
    verdicts: &HeadVerdicts,
    missing: &[&SuiteVerdict],
) -> String {
    let head = short(head);
    let detail = match verdicts {
        HeadVerdicts::Unreadable(error) => format!(
            "whether its head {head} carries a verdict for the checks its change touches could \
             not be established: {error}. Merge again."
        ),
        HeadVerdicts::Read(suites) => {
            let lines = missing
                .iter()
                .map(|suite| {
                    format!(
                        "- {} ({}): {}: {}",
                        suite.suite,
                        suite.policy,
                        suite.existence.label(),
                        remedy(suite, source)
                    )
                })
                .collect::<Vec<_>>()
                .join("\n");
            let exit_codes = suites
                .iter()
                .filter_map(|suite| {
                    suite.exit_code_verdict.map(|passed| {
                        format!("{} ({})", suite.suite, if passed { "green" } else { "red" })
                    })
                })
                .collect::<Vec<_>>();
            let exit_codes = if exit_codes.is_empty() {
                String::new()
            } else {
                format!(
                    "\nCounted as verdicts on their exit code alone, with no structured result \
                     (an interim until cairn/4941 gives their runners a parser): {}.",
                    exit_codes.join(", ")
                )
            };
            format!(
                "its head {head} has {NO_VERDICT} for {count} check(s) its change makes \
                 applicable. A red does not stop a merge here; an applicable check that was \
                 never judged does.\n{lines}{exit_codes}",
                count = missing.len(),
            )
        }
    };
    format!(
        "Refusing to merge `{source}` into `{target}`: {detail}\n`{target}` was not advanced. \
         Only the operator may land a head with {NO_VERDICT}, from the desktop app's PR view \
         (\"Merge without verdicts\") or as a signed-in owner or admin through \
         `merge_pr_as_operator` with `waiveVerdicts`; the waiver and its reason are recorded on \
         the merge and on the issue."
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::check_parsers::ParsedCheckResult;
    use crate::execution::checks_status::UnverifiedReason;
    use crate::pr_data::actions::pin_bump::CollectedVerdict;

    const HEAD: &str = "0123456789abcdef0123456789abcdef01234567";

    fn status(name: &str, policy: &str, state: NodeCheckState) -> NodeCheckStatus {
        crate::execution::checks_status::test_status(name, policy, "review", state)
    }

    fn parsed(selection: &str, passed: usize, complete: bool) -> ParsedCheckResult {
        ParsedCheckResult {
            schema_version: 1,
            complete,
            selection: selection.to_string(),
            tests: vec![],
            undeclared_skips: 0,
            parser: "nextest".to_string(),
            passed,
            failed: 0,
            skipped: 0,
            suite_failures: 0,
            deferred: 0,
            report: None,
            failures: vec![],
        }
    }

    fn with_tests(mut lane: NodeCheckStatus, selection: &str, passed: usize) -> NodeCheckStatus {
        lane.parser = Some("nextest".into());
        lane.selection = Some(selection.into());
        lane.complete = Some(true);
        lane.passed = Some(passed);
        lane.failed = Some(0);
        lane.skipped = Some(0);
        lane
    }

    fn existence_of(lane: NodeCheckStatus, cadence: WaveCadence) -> Option<LaneExistence> {
        applicable_suites(&[lane], cadence)
            .pop()
            .map(|suite| suite.existence)
    }

    #[test]
    fn every_lane_state_maps_to_its_existence() {
        let idle = WaveCadence::default();
        let reviewing = WaveCadence::reviewing_for_test();
        let cases: Vec<(NodeCheckStatus, WaveCadence, Option<LaneExistence>)> = vec![
            (
                with_tests(
                    status("rust-tests", "advisory", NodeCheckState::Passed),
                    "full",
                    3,
                ),
                idle,
                Some(LaneExistence::Complete),
            ),
            (
                status("rust-tests", "advisory", NodeCheckState::Failed),
                idle,
                Some(LaneExistence::Complete),
            ),
            (
                // A script check's green carries no structured result: its
                // exit code is the verdict.
                status("migrations", "advisory", NodeCheckState::Unverified),
                idle,
                Some(LaneExistence::Complete),
            ),
            (
                status("rust-tests", "advisory", NodeCheckState::Pending),
                idle,
                Some(LaneExistence::Absent),
            ),
            (
                status("rust-tests", "advisory", NodeCheckState::Pending),
                reviewing,
                Some(LaneExistence::Pending { running: false }),
            ),
            (
                status("rust-tests", "advisory", NodeCheckState::Running),
                reviewing,
                Some(LaneExistence::Pending { running: true }),
            ),
            (
                {
                    let mut lane = status("rust-tests", "advisory", NodeCheckState::Failed);
                    lane.failure_kind = Some("infrastructure".into());
                    lane
                },
                idle,
                Some(LaneExistence::Died {
                    kind: "infrastructure".into(),
                }),
            ),
            (
                {
                    let mut lane = status("rust-tests", "advisory", NodeCheckState::Failed);
                    lane.failure_kind = Some("infrastructure".into());
                    lane.suppressed_after = Some(3);
                    lane
                },
                idle,
                Some(LaneExistence::Breaker { after: 3 }),
            ),
            (
                with_tests(
                    status("rust-tests", "advisory", NodeCheckState::Unverified),
                    "empty",
                    0,
                ),
                idle,
                Some(LaneExistence::Empty),
            ),
            (
                {
                    let mut lane = with_tests(
                        status("rust-tests", "advisory", NodeCheckState::Unverified),
                        "full",
                        4,
                    );
                    lane.complete = Some(false);
                    lane
                },
                idle,
                Some(LaneExistence::Incomplete(
                    UnverifiedReason::IncompleteEnumeration,
                )),
            ),
            (
                status("rust-tests", "advisory", NodeCheckState::NotApplicable),
                idle,
                None,
            ),
            (
                status("next-check", "advisory", NodeCheckState::Unsupported),
                idle,
                None,
            ),
        ];
        for (lane, cadence, expected) in cases {
            let shown = format!("{lane:?}");
            assert_eq!(existence_of(lane, cadence), expected, "{shown}");
        }
    }

    /// A write-cadence wave in flight does not make a review lane pending.
    #[test]
    fn only_a_wave_of_the_lanes_own_cadence_makes_it_pending() {
        let writing = WaveCadence::writing_for_test();
        assert_eq!(
            existence_of(
                status("rust-tests", "advisory", NodeCheckState::Pending),
                writing
            ),
            Some(LaneExistence::Absent)
        );
    }

    fn read(lanes: Vec<NodeCheckStatus>) -> HeadVerdicts {
        HeadVerdicts::Read(applicable_suites(&lanes, WaveCadence::default()))
    }

    #[test]
    fn an_advisory_red_does_not_block() {
        let verdicts = read(vec![
            status("rust-lint", "advisory", NodeCheckState::Failed),
            with_tests(
                status("rust-tests", "gate", NodeCheckState::Passed),
                "full",
                9,
            ),
        ]);
        assert_eq!(decide("agent/x", "main", HEAD, &verdicts, None), Ok(None));
    }

    #[test]
    fn an_advisory_absent_blocks_and_names_the_run_that_clears_it() {
        let verdicts = read(vec![
            with_tests(status("api", "gate", NodeCheckState::Passed), "full", 9),
            status("rust-tests", "advisory", NodeCheckState::Pending),
        ]);
        let refusal = decide("agent/x", "main", HEAD, &verdicts, None).unwrap_err();
        assert!(refusal.contains(NO_VERDICT), "{refusal}");
        assert!(
            refusal.contains("- rust-tests (advisory): absent: no lane has run it on this head."),
            "{refusal}"
        );
        assert!(
            refusal.contains("`cairn check run rust-tests --branch agent/x`"),
            "{refusal}"
        );
        assert!(!refusal.contains("- api"), "{refusal}");
        assert!(refusal.contains("`main` was not advanced"), "{refusal}");
        assert!(refusal.contains("0123456789ab"), "{refusal}");
    }

    #[test]
    fn every_state_that_is_not_a_verdict_blocks_with_its_own_remedy() {
        let died = {
            let mut lane = status("external-host", "advisory", NodeCheckState::Failed);
            lane.failure_kind = Some("killed".into());
            lane
        };
        let breaker = {
            let mut lane = status("rust-lint", "advisory", NodeCheckState::Failed);
            lane.failure_kind = Some("infrastructure".into());
            lane.suppressed_after = Some(3);
            lane
        };
        let empty = with_tests(
            status("rust-tests", "advisory", NodeCheckState::Unverified),
            "empty",
            0,
        );
        let verdicts = read(vec![died, breaker, empty]);
        let refusal = decide("agent/x", "main", HEAD, &verdicts, None).unwrap_err();
        assert!(
            refusal.contains("- external-host (advisory): died:"),
            "{refusal}"
        );
        assert!(
            refusal.contains("- rust-lint (advisory): breaker: Cairn stopped running it after 3"),
            "{refusal}"
        );
        assert!(refusal.contains("{repaired: true}"), "{refusal}");
        assert!(
            refusal.contains("- rust-tests (advisory): empty: its run selected no test"),
            "{refusal}"
        );
        assert!(refusal.contains("3 check(s)"), "{refusal}");
    }

    fn rust_lint(selection: &str, complete: bool) -> NodeCheckStatus {
        let mut lane = status("rust-lint", "advisory", NodeCheckState::Unverified);
        lane.parser = Some("rust-lint".into());
        lane.selection = Some(selection.into());
        lane.complete = Some(complete);
        lane.passed = Some(0);
        lane.failed = Some(0);
        lane.skipped = Some(0);
        lane
    }

    /// A scripts-only change applies rust-lint (its impact names `scripts/**`)
    /// and puts no crate in its cone. Nothing in the lint's domain changed, so
    /// the lane is not applicable by content: it lands without a waiver, and
    /// it is not a green either.
    #[test]
    fn a_scripts_only_diff_under_rust_lints_impact_lands_without_a_waiver() {
        assert_eq!(
            existence_of(rust_lint("empty", false), WaveCadence::default()),
            Some(LaneExistence::NoSelectableWork)
        );
        let verdicts = read(vec![
            rust_lint("empty", false),
            with_tests(
                status("rust-tests", "advisory", NodeCheckState::Passed),
                "fast",
                8000,
            ),
        ]);
        assert_eq!(decide("agent/x", "main", HEAD, &verdicts, None), Ok(None));
    }

    /// A lint that stopped before measuring decided nothing about the change;
    /// and a test runner's empty selection over a change its impact matched is
    /// cairn/5325's defect. Both still refuse.
    #[test]
    fn a_lint_that_never_measured_and_a_test_runners_empty_selection_still_refuse() {
        assert_eq!(
            existence_of(rust_lint("unknown", false), WaveCadence::default()),
            Some(LaneExistence::Incomplete(UnverifiedReason::IncompleteLint))
        );
        let verdicts = read(vec![with_tests(
            status("rust-tests", "advisory", NodeCheckState::Unverified),
            "empty",
            0,
        )]);
        let refusal = decide("agent/x", "main", HEAD, &verdicts, None).unwrap_err();
        assert!(refusal.contains("cairn/5325"), "{refusal}");
    }

    /// A verdict with no structured result counts, and the refusal says which
    /// ones rest on an exit code alone and what retires that.
    #[test]
    fn a_refusal_names_the_verdicts_that_rest_on_an_exit_code_alone() {
        let verdicts = read(vec![
            status("api", "gate", NodeCheckState::Failed),
            status("rust-tests", "advisory", NodeCheckState::Pending),
        ]);
        let refusal = decide("agent/x", "main", HEAD, &verdicts, None).unwrap_err();
        assert!(
            refusal.contains("exit code alone") && refusal.contains("cairn/4941"),
            "{refusal}"
        );
        assert!(refusal.contains("api (red)"), "{refusal}");
    }

    #[test]
    fn a_head_that_declares_no_checks_lands() {
        assert_eq!(
            decide(
                "agent/x",
                "main",
                HEAD,
                &HeadVerdicts::Read(Vec::new()),
                None
            ),
            Ok(None)
        );
    }

    #[test]
    fn an_unreadable_head_is_refused_never_passed() {
        let refusal = decide(
            "agent/x",
            "main",
            HEAD,
            &HeadVerdicts::Unreadable("config does not parse".into()),
            None,
        )
        .unwrap_err();
        assert!(refusal.contains("could not be established: config does not parse"));
        assert!(
            refusal.contains(NO_VERDICT),
            "the PR view keys on it: {refusal}"
        );
    }

    #[test]
    fn the_operators_waiver_lands_and_names_what_it_covers() {
        let waiver = VerdictWaiver {
            reason: "fleet starved; ran cargo test by hand".into(),
        };
        let verdicts = read(vec![
            status("rust-tests", "advisory", NodeCheckState::Pending),
            status("rust-lint", "advisory", NodeCheckState::Failed),
        ]);
        let detail = decide(
            "agent/x",
            "main",
            HEAD,
            &verdicts,
            Some((&waiver, "owner@example.com")),
        )
        .unwrap()
        .expect("a waiver is recorded");
        assert_eq!(
            detail,
            "verdicts waived by owner@example.com: fleet starved; ran cargo test by hand. Landed \
             head 0123456789ab without a complete verdict for rust-tests (absent)"
        );
    }

    #[test]
    fn a_waiver_with_nothing_to_waive_is_refused() {
        let waiver = VerdictWaiver {
            reason: "just in case".into(),
        };
        let verdicts = read(vec![status(
            "rust-lint",
            "advisory",
            NodeCheckState::Failed,
        )]);
        let refusal =
            decide("agent/x", "main", HEAD, &verdicts, Some((&waiver, "op"))).unwrap_err();
        assert!(refusal.contains("nothing to waive"), "{refusal}");
    }

    fn recorded(
        check: &str,
        passed: bool,
        kind: Option<&str>,
        result: ParsedCheckResult,
    ) -> CollectedVerdict {
        CollectedVerdict {
            check: check.into(),
            platform: "linux".into(),
            commit_sha: HEAD.into(),
            observation_id: format!("obs-{check}"),
            disposition: "fresh".into(),
            passed,
            failure_kind: kind.map(str::to_string),
            parsed: Some(result),
        }
    }

    /// A manual whole-suite run keys differently from a selector suite's lane,
    /// so the lane stays empty; the run judged the head, and answers it.
    #[test]
    fn a_whole_suite_run_at_the_head_answers_an_open_lane() {
        let mut suites = applicable_suites(
            &[
                status("rust-tests", "advisory", NodeCheckState::Pending),
                status("rust-lint", "advisory", NodeCheckState::Pending),
                status("external-host", "advisory", NodeCheckState::Pending),
            ],
            WaveCadence::default(),
        );
        answer_from_commit(
            &mut suites,
            &[
                recorded("rust-tests", true, None, parsed("full", 8000, true)),
                // An empty run and a dead one at the head answer nothing.
                recorded("rust-lint", true, None, parsed("empty", 0, true)),
                recorded(
                    "external-host",
                    false,
                    Some("capacity"),
                    parsed("full", 0, true),
                ),
            ],
        );
        let states: Vec<_> = suites
            .iter()
            .map(|suite| (suite.suite.as_str(), suite.existence.label()))
            .collect();
        assert_eq!(
            states,
            vec![
                ("rust-tests", "complete"),
                ("rust-lint", "absent"),
                ("external-host", "absent"),
            ]
        );
    }
}
