//! The landing compile gate: a pull request lands on the project's default
//! branch only if the MERGE RESULT compiles.
//!
//! Each branch's own checks judge the branch's head. Two heads that are green
//! on their own can still stitch into a tree that does not build: one reshapes
//! a type, the other adds a use of the old shape in a crate neither touched.
//! Only the tree that would actually land shows that. So at the moment of
//! landing this builds that tree in the store (the target's live tip, fetched
//! from origin, plus the branch's change), runs the project's `when: landing`
//! checks against it, and lands only on a green.
//!
//! Three answers, and only one of them lands:
//!
//! - builds: the merge proceeds, onto the very tips that were judged. If
//!   either tip moved while the gate ran, the new merge result is judged
//!   instead; a tree whose inputs did not change reuses its verdict.
//! - does not build: refused, with every rustc error, and the remedy named.
//! - not run: no cell was admitted within the check's `budgetSeconds`, or
//!   placement refused it. Refused as "compile gate unavailable", quoting
//!   placement. A gate that cannot run never passes a merge. The operator, and
//!   only the operator, may then land it with an explicit skip, which names
//!   the refusal it overrides and is journaled with the merge.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use crate::config::project_settings::CheckWhen;
use crate::execution::checks::{CheckOutcome, ProspectiveGate};
use crate::execution::compile_facts::{
    builds_fact_for_tree, record_compile_fact, CompileFactWrite, CompileOutcome, CompileSource,
};
use crate::execution::compile_signature::parse_compile_failure_from_tail;
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};
use cairn_db::turso::params;

use super::context::MergeMrContext;
use super::resolution::PrResolutionAttribution;
use super::store_merge::{
    live_default_landing_tips, prospective_default_landing, ProspectiveMerge,
};

/// The phrase every unavailable refusal carries, and the one a skip looks for
/// in the journaled refusal it overrides.
const UNAVAILABLE: &str = "compile gate unavailable";

/// How many times the gate re-judges a merge result because a tip moved
/// under it, before it gives the merge back to its caller.
const MAX_JUDGEMENTS: usize = 4;

/// The operator's explicit decision to land without the compile gate's
/// verdict, after the gate refused as unavailable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LandingGateSkip {
    pub reason: String,
}

/// What the gate established, for the landing that follows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct GatedLanding {
    /// The source commit the verdict is about. A GitHub landing binds the
    /// merge to it, so a PR that moves after the gate is refused by GitHub.
    pub source_tip: Option<String>,
    /// What the merge journal records about the gate, when there is
    /// something beyond an ordinary green: an operator skip.
    pub journal_detail: Option<String>,
}

impl GatedLanding {
    /// A landing no compile gate applies to: not onto the default branch, or
    /// no `when: landing` check declared.
    pub(super) fn ungated() -> Self {
        Self {
            source_tip: None,
            journal_detail: None,
        }
    }
}

/// The gate's reading of one merge result's landing outcomes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LandingJudgement {
    Builds,
    DoesNotBuild { check: String, diagnostics: String },
    NotRun { check: String, refusal: String },
}

/// A red outranks a check that did not run: it is a verdict, and the remedy
/// it names is the one the branch needs whatever the other check would say.
pub(crate) fn judge(outcomes: &[CheckOutcome]) -> LandingJudgement {
    let mut not_run = None;
    for outcome in outcomes {
        if outcome.passed {
            continue;
        }
        let a_verdict = outcome.suppressed_after.is_none()
            && outcome
                .failure_kind
                .is_none_or(|kind| kind.is_verdict_about_the_code());
        let failure = outcome
            .compile_failure
            .clone()
            .or_else(|| parse_compile_failure_from_tail(&outcome.output_tail));
        // A killed or misconfigured machine is not evidence the tree does not
        // build; only a command that ran and failed, or a cargo build failure
        // however the run was classified, is.
        let says_does_not_build = failure.is_some()
            || (a_verdict
                && outcome.failure_kind.is_none_or(|kind| {
                    kind == crate::execution::checks::CheckFailureKind::Unclassified
                }));
        if says_does_not_build {
            let diagnostics = match failure {
                Some(failure) if !failure.errors.is_empty() => failure.render(),
                _ => outcome.output_tail.trim().to_string(),
            };
            return LandingJudgement::DoesNotBuild {
                check: outcome.name.clone(),
                diagnostics,
            };
        }
        not_run.get_or_insert_with(|| LandingJudgement::NotRun {
            check: outcome.name.clone(),
            refusal: outcome
                .not_recorded
                .as_deref()
                .filter(|_| outcome.output_tail.trim().is_empty())
                .unwrap_or(outcome.output_tail.trim())
                .to_string(),
        });
    }
    not_run.unwrap_or(LandingJudgement::Builds)
}

fn short(commit: &str) -> &str {
    commit.get(..12).unwrap_or(commit)
}

pub(crate) fn does_not_build_refusal(
    source: &str,
    target: &str,
    base: &str,
    check: &str,
    diagnostics: &str,
) -> String {
    format!(
        "Refusing to merge `{source}` into `{target}`: the merge result does not compile. \
         `{target}` at {base} plus this branch's change fails the landing check `{check}`, so \
         landing it would leave `{target}` unbuildable. `{target}` was not advanced.\n\
         {diagnostics}\n\
         Remedy: rebase `{source}` onto current `{target}` and fix these errors on the branch. \
         Cairn replays a branch onto `{target}` when `{target}` advances (`cairn:~/rebase` shows \
         where it stands), and once it sits on {base} its own checks report the same errors. \
         Fix them with ordinary edits, commit, then merge again.",
        base = short(base),
    )
}

pub(crate) fn unavailable_refusal(
    source: &str,
    target: &str,
    check: &str,
    refusal: &str,
) -> String {
    let refusal = if refusal.trim().is_empty() {
        "(placement gave no reason)"
    } else {
        refusal.trim()
    };
    format!(
        "Refusing to merge `{source}` into `{target}`: {UNAVAILABLE}. The landing check \
         `{check}` did not run against the merge result, and a merge result nobody compiled \
         does not land. `{target}` was not advanced.\n\
         Placement said: {refusal}\n\
         Merge again once a machine can take it. The operator may instead land it without the \
         gate, from the desktop app's PR view (\"Land without the compile gate\") or as a \
         signed-in owner or admin through `merge_pr_as_operator` with `skipLandingGate`; the \
         skip is journaled against this refusal."
    )
}

/// Gate a landing on the project's default branch. `Ok` carries what the
/// landing must honour; `Err` is the refusal, already worded for the caller.
#[allow(clippy::too_many_arguments)]
pub(super) async fn gate_default_landing(
    orch: &Orchestrator,
    db: &Arc<LocalDb>,
    job_id: &str,
    merge_context: &MergeMrContext,
    method: &str,
    skip: Option<&LandingGateSkip>,
    attribution: Option<&PrResolutionAttribution>,
) -> Result<GatedLanding, String> {
    let source = merge_context.source_branch.as_str();
    let target = merge_context.target_branch.as_str();
    let repository = merge_context.mr.repo_path.as_str();
    let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(repository));

    if !landing_checks_declared(orch, merge_context).await {
        if skip.is_some() {
            return Err(format!(
                "Refusing to merge `{source}`: a landing-gate skip was given, but neither \
                 `{source}` nor `{target}` declares a `when: landing` check, so there is no \
                 gate to skip. Merge without it."
            ));
        }
        return Ok(GatedLanding::ungated());
    }

    if let Some(skip) = skip {
        return operator_skip(orch, db, merge_context, &store, skip, attribution).await;
    }

    let started_unix_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    for _ in 0..MAX_JUDGEMENTS {
        let prospective = {
            let _store_guard = orch
                .acquire_jj_store_lock(
                    &store,
                    crate::orchestrator::StoreLockClass::Foreground,
                    format!("landing gate merge result for {source}"),
                )
                .await;
            prospective_default_landing(orch, merge_context, method).await?
        };
        let landing = landing_commands_at(Path::new(repository), &prospective.commit_id).await;
        let judgement = if reused_green(
            db,
            &merge_context.project_id,
            &prospective.tree_hash,
            &landing,
        )
        .await
        {
            log::info!(
                "landing gate[{job_id}]: the merge result of `{source}` ({}) already has a \
                 `builds` fact for every landing check; landing without a run",
                short(&prospective.tree_hash)
            );
            LandingJudgement::Builds
        } else {
            let outcomes = crate::execution::checks::run_prospective_tree_checks(
                orch,
                db.clone(),
                &merge_context.project_id,
                repository,
                Path::new(repository),
                &prospective.commit_id,
                &prospective.target_tip,
                &prospective.tree_hash,
                &prospective.tree_entries,
                &prospective.changed_files,
                job_id,
                crate::fleet::CellPriority::ReviewCheck,
                ProspectiveGate::Landing { started_unix_ms },
            )
            .await
            .map_err(|error| unavailable_refusal(source, target, "(planning)", &error))?;
            record_landing_facts(db, merge_context, &prospective, &landing, &outcomes).await;
            judge(&outcomes)
        };
        match judgement {
            LandingJudgement::Builds => {}
            LandingJudgement::DoesNotBuild { check, diagnostics } => {
                return Err(does_not_build_refusal(
                    source,
                    target,
                    &prospective.target_tip,
                    &check,
                    &diagnostics,
                ));
            }
            LandingJudgement::NotRun { check, refusal } => {
                return Err(unavailable_refusal(source, target, &check, &refusal));
            }
        }
        // The verdict is about the tips it was built from. Land only if they
        // still stand; otherwise judge the merge result that would land now.
        let live = {
            let _store_guard = orch
                .acquire_jj_store_lock(
                    &store,
                    crate::orchestrator::StoreLockClass::Foreground,
                    format!("landing gate tip recheck for {source}"),
                )
                .await;
            live_default_landing_tips(orch, merge_context).await?
        };
        if live
            == (
                prospective.source_tip.clone(),
                prospective.target_tip.clone(),
            )
        {
            return Ok(GatedLanding {
                source_tip: Some(prospective.source_tip),
                journal_detail: None,
            });
        }
        log::info!(
            "landing gate[{job_id}]: tips moved while judging `{source}` ({} on {} -> {} on {}); \
             judging the new merge result",
            short(&prospective.source_tip),
            short(&prospective.target_tip),
            short(&live.0),
            short(&live.1),
        );
    }
    Err(format!(
        "Refusing to merge `{source}` into `{target}`: `{target}` or the branch kept moving while \
         the landing compile gate judged the merge result, {MAX_JUDGEMENTS} times. `{target}` was \
         not advanced; merge again."
    ))
}

/// The landing checks a commit's contract declares, by name, with the command
/// each runs: the `scope` its compile fact records verbatim.
async fn landing_commands_at(repository: &Path, commit: &str) -> BTreeMap<String, String> {
    crate::execution::checks::load_checks_contract_at_commit(repository, commit)
        .await
        .map(|contract| {
            contract
                .contract
                .checks
                .into_iter()
                .filter(|(_, check)| check.when == CheckWhen::Landing)
                .map(|(name, check)| (name, check.command))
                .collect()
        })
        .unwrap_or_default()
}

/// Whether every landing check already has a `builds` fact for this exact
/// tree and command, from any source. A tree compiled once is not compiled
/// again to land.
async fn reused_green(
    db: &LocalDb,
    project_id: &str,
    tree_hash: &str,
    landing: &BTreeMap<String, String>,
) -> bool {
    if landing.is_empty() {
        return false;
    }
    for command in landing.values() {
        match builds_fact_for_tree(db, project_id, tree_hash, command).await {
            Ok(Some(_)) => {}
            Ok(None) => return false,
            Err(error) => {
                log::warn!("landing gate: compile facts unreadable, running the gate: {error}");
                return false;
            }
        }
    }
    true
}

/// One compile fact per landing check the gate ran or tried to run, about the
/// merge result, its base, and the head it merged. A write failure is logged
/// and never changes the merge's outcome: the refusal or landing stands on the
/// verdict, not on its record.
async fn record_landing_facts(
    db: &Arc<LocalDb>,
    merge_context: &MergeMrContext,
    prospective: &ProspectiveMerge,
    landing: &BTreeMap<String, String>,
    outcomes: &[CheckOutcome],
) {
    for outcome in outcomes {
        let (fact_outcome, diagnostic_tail) = match judge(std::slice::from_ref(outcome)) {
            LandingJudgement::Builds => (CompileOutcome::Builds, None),
            LandingJudgement::DoesNotBuild { diagnostics, .. } => {
                (CompileOutcome::DoesNotBuild, Some(diagnostics))
            }
            LandingJudgement::NotRun { refusal, .. } => (CompileOutcome::NotRun, Some(refusal)),
        };
        let failure = (fact_outcome == CompileOutcome::DoesNotBuild)
            .then(|| {
                outcome
                    .compile_failure
                    .clone()
                    .or_else(|| parse_compile_failure_from_tail(&outcome.output_tail))
            })
            .flatten();
        let machine = outcome.recorded.as_ref().and_then(|recorded| {
            crate::execution::cache::get_check_observation_machine(
                db.clone(),
                &merge_context.project_id,
                &recorded.id,
            )
            .ok()
            .flatten()
        });
        let fact = CompileFactWrite {
            project_id: merge_context.project_id.clone(),
            tree_commit: prospective.commit_id.clone(),
            tree_hash: prospective.tree_hash.clone(),
            base_commit: Some(prospective.target_tip.clone()),
            head_commit: Some(prospective.source_tip.clone()),
            merge_request_id: Some(merge_context.mr.mr_id.clone()),
            platform: machine
                .as_ref()
                .and_then(|machine| machine.verdict_platform.clone()),
            executor_id: machine.and_then(|machine| machine.executor_id).or_else(|| {
                outcome
                    .recorded
                    .as_ref()
                    .and_then(|r| r.executor_id.clone())
            }),
            scope: landing
                .get(&outcome.name)
                .cloned()
                .unwrap_or_else(|| outcome.name.clone()),
            check_name: Some(outcome.name.clone()),
            outcome: fact_outcome,
            failure,
            diagnostic_tail,
            source: CompileSource::LandingGate,
            observation_id: outcome
                .recorded
                .as_ref()
                .map(|recorded| recorded.id.clone()),
        };
        if let Err(error) = record_compile_fact(db, fact).await {
            log::warn!("landing gate: {error}");
        }
    }
}

/// Whether the branch or the target declares any landing check. Read from
/// both, because the merge result carries the branch's config edits and the
/// target's: a check either side declares is in the tree that would land.
async fn landing_checks_declared(orch: &Orchestrator, merge_context: &MergeMrContext) -> bool {
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let repository = Path::new(&merge_context.mr.repo_path);
    let store = crate::jj::project_store_dir(&orch.config_dir, repository);
    for branch in [&merge_context.source_branch, &merge_context.target_branch] {
        let Some(commit) = crate::jj::bookmark_commit(&jj, &store, branch) else {
            continue;
        };
        let declared =
            crate::execution::checks::load_checks_contract_at_commit(repository, &commit)
                .await
                .is_some_and(|contract| {
                    contract
                        .contract
                        .checks
                        .values()
                        .any(|check| check.when == CheckWhen::Landing)
                });
        if declared {
            return true;
        }
    }
    false
}

/// Defence in depth only, never the gate. A person's authority to skip is
/// established at the transport (`has_operator_authority`), and every
/// resource-write path refuses a skip before it reaches the merge; this keeps a
/// future caller that forgot both from landing a skip attributed to an agent.
pub(super) fn is_operator(attribution: Option<&PrResolutionAttribution>) -> bool {
    attribution.is_some_and(|value| matches!(value.actor_kind, "operator-ui" | "operator-cli"))
}

async fn operator_skip(
    orch: &Orchestrator,
    db: &Arc<LocalDb>,
    merge_context: &MergeMrContext,
    store: &Path,
    skip: &LandingGateSkip,
    attribution: Option<&PrResolutionAttribution>,
) -> Result<GatedLanding, String> {
    let source = merge_context.source_branch.as_str();
    if !is_operator(attribution) {
        return Err(format!(
            "Refusing to merge `{source}`: skipping the landing compile gate is the operator's \
             act, and this merge was requested by {}. Ask the operator to land it.",
            attribution.map_or("an unattributed caller", |value| value.actor_kind)
        ));
    }
    let Some((attempt_id, refusal)) =
        latest_unavailable_refusal(db, &merge_context.mr.mr_id).await?
    else {
        return Err(format!(
            "Refusing to merge `{source}`: a landing-gate skip overrides a '{UNAVAILABLE}' \
             refusal, and this PR's most recent merge attempt was not one. Merge without the \
             skip first; skip only if the gate cannot run."
        ));
    };
    let source_tip = {
        let _store_guard = orch
            .acquire_jj_store_lock(
                store,
                crate::orchestrator::StoreLockClass::Foreground,
                format!("landing gate skip for {source}"),
            )
            .await;
        live_default_landing_tips(orch, merge_context).await?.0
    };
    let first_line = refusal
        .lines()
        .find(|line| line.starts_with("Placement said:"))
        .unwrap_or_else(|| refusal.lines().next().unwrap_or_default());
    Ok(GatedLanding {
        source_tip: Some(source_tip),
        journal_detail: Some(format!(
            "landing compile gate skipped by {actor}: {reason}. Overrides the '{UNAVAILABLE}' \
             refusal of merge attempt {attempt_id} ({first_line})",
            actor = attribution
                .and_then(|value| value.actor_identity.as_deref())
                .or(attribution.map(|value| value.actor_kind))
                .unwrap_or("operator"),
            reason = skip.reason,
        )),
    })
}

/// The most recent terminal merge attempt on this PR, when it was a refusal
/// because the compile gate could not run: its attempt id and detail.
async fn latest_unavailable_refusal(
    db: &LocalDb,
    merge_request_id: &str,
) -> Result<Option<(String, String)>, String> {
    let merge_request_id = merge_request_id.to_string();
    db.read(move |conn| {
        let merge_request_id = merge_request_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT attempt_id, state, detail
                       FROM merge_attempt_events
                      WHERE merge_request_id = ?1 AND sequence = 1
                      ORDER BY created_at DESC, rowid DESC
                      LIMIT 1",
                    params![merge_request_id.as_str()],
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Ok(None);
            };
            let (attempt_id, state, detail) = (row.text(0)?, row.text(1)?, row.opt_text(2)?);
            Ok(detail
                .filter(|detail| state == "failed" && detail.contains(UNAVAILABLE))
                .map(|detail| (attempt_id, detail)))
        })
    })
    .await
    .map_err(|error| format!("Failed to read the merge journal for a landing-gate skip: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::checks::CheckFailureKind;
    use crate::execution::compile_signature::parse_compile_failure;

    fn outcome(name: &str, passed: bool) -> CheckOutcome {
        CheckOutcome {
            name: name.into(),
            passed,
            exit_code: Some(if passed { 0 } else { 101 }),
            failure_kind: None,
            parsed: None,
            output_tail: String::new(),
            cached: false,
            duration_ms: 1,
            suppressed_after: None,
            recorded: None,
            not_recorded: None,
            compile_failure: None,
        }
    }

    const STITCH: &str = "error[E0063]: missing field `decision` in initializer of `Verdict`
   --> cairn-exec/src/placement.rs:412:9
error[E0308]: mismatched types
   --> os/cairn-core/src/execution/impact_slice.rs:437:21
error: could not compile `cairn-exec` (lib test) due to 1 previous error
error: could not compile `cairn-core` (lib) due to 1 previous error
";

    #[test]
    fn a_green_merge_result_builds_and_nothing_applicable_builds_vacuously() {
        assert_eq!(judge(&[]), LandingJudgement::Builds);
        assert_eq!(
            judge(&[outcome("landing-compile", true)]),
            LandingJudgement::Builds
        );
    }

    /// The cairn/5290 stitch: every error is in the refusal, not just the
    /// first, and the remedy names rebase onto the current target.
    #[test]
    fn a_merge_result_that_does_not_compile_is_refused_with_every_rustc_error() {
        let mut red = outcome("landing-compile", false);
        red.compile_failure = parse_compile_failure(STITCH);
        red.output_tail =
            "error: could not compile `cairn-core` (lib) due to 1 previous error".into();
        let LandingJudgement::DoesNotBuild { check, diagnostics } = judge(&[red]) else {
            panic!("a compile red is a does-not-build");
        };
        let refusal =
            does_not_build_refusal("agent/x", "main", "0123456789abcdef", &check, &diagnostics);
        assert!(
            refusal.contains("cairn-exec/src/placement.rs:412:9: error[E0063]"),
            "{refusal}"
        );
        assert!(
            refusal.contains("os/cairn-core/src/execution/impact_slice.rs:437:21: error[E0308]"),
            "{refusal}"
        );
        assert!(refusal.contains("`main` at 0123456789ab"), "{refusal}");
        assert!(
            refusal.contains("Remedy: rebase `agent/x` onto current `main`"),
            "{refusal}"
        );
        assert!(refusal.contains("`main` was not advanced"), "{refusal}");
    }

    /// A reused red has no full output; what errors its tail still holds are
    /// read from it rather than rendered as a raw dump.
    #[test]
    fn a_reused_red_reads_its_errors_from_the_tail() {
        let mut red = outcome("landing-compile", false);
        red.cached = true;
        // A stored tail begins wherever the 4k cut fell, here inside a line.
        red.output_tail = format!("ing cairn-exec v0.12.34\n{STITCH}");
        let LandingJudgement::DoesNotBuild { diagnostics, .. } = judge(&[red]) else {
            panic!("a compile red is a does-not-build");
        };
        assert!(
            diagnostics.starts_with("- cairn-exec/src/placement.rs:412:9"),
            "{diagnostics}"
        );
    }

    /// No cell within budget is a refusal, never a pass, and it quotes what
    /// placement said.
    #[test]
    fn a_gate_that_could_not_run_refuses_and_quotes_placement() {
        let mut idle = outcome("landing-compile", false);
        idle.exit_code = None;
        idle.failure_kind = Some(CheckFailureKind::Infrastructure);
        idle.output_tail = "No machine available to Cairn can run this check's platform or toolchain. \
            NoMatchingExecutor: no executor on linux became usable before this request's wait horizon."
            .into();
        let LandingJudgement::NotRun { check, refusal } = judge(&[idle]) else {
            panic!("an infrastructure failure is not a verdict");
        };
        let text = unavailable_refusal("agent/x", "main", &check, &refusal);
        assert!(text.contains("compile gate unavailable"), "{text}");
        assert!(
            text.contains("Placement said: No machine available"),
            "{text}"
        );
        assert!(
            text.contains("before this request's wait horizon"),
            "{text}"
        );
        assert!(text.contains("Land without the compile gate"), "{text}");
    }

    #[test]
    fn a_red_outranks_a_check_that_did_not_run() {
        let mut idle = outcome("landing-compile-macos", false);
        idle.failure_kind = Some(CheckFailureKind::Capacity);
        idle.output_tail = "capacity".into();
        let mut red = outcome("landing-compile", false);
        red.output_tail = STITCH.into();
        assert!(matches!(
            judge(&[idle, red]),
            LandingJudgement::DoesNotBuild { check, .. } if check == "landing-compile"
        ));
    }

    /// A killed machine or a missing toolchain did not compile the tree, so it
    /// cannot say the tree does not compile.
    #[test]
    fn a_killed_or_misplaced_run_is_not_a_does_not_build() {
        for kind in [
            CheckFailureKind::Killed,
            CheckFailureKind::Environment,
            CheckFailureKind::Cancelled,
        ] {
            let mut run = outcome("landing-compile", false);
            run.failure_kind = Some(kind);
            run.output_tail = "signal 9".into();
            assert!(
                matches!(judge(&[run]), LandingJudgement::NotRun { .. }),
                "{kind:?}"
            );
        }
    }

    #[test]
    fn only_the_operator_may_skip() {
        assert!(is_operator(Some(&PrResolutionAttribution::operator_ui())));
        assert!(is_operator(Some(&PrResolutionAttribution::operator_cli())));
        assert!(!is_operator(Some(&PrResolutionAttribution::agent(
            "cairn://p/cairn/1/1/builder".into()
        ))));
        assert!(!is_operator(None));
    }
}
