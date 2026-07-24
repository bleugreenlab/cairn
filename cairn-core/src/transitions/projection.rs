//! Job status as a derived projection.
//!
//! Job progress is not a fact written by whoever holds the job — it is a
//! *projection* of facts that already live elsewhere: DAG readiness, the latest
//! turn outcome, the upstream-failure relation, and the artifact resolution
//! (`confirmed`). This mirrors how execution and issue status are already
//! recomputed (see `recompute_execution_status_conn` / `recompute_issue_status_conn`).
//!
//! [`derive_job_status`] is a pure function of [`JobFacts`]; it has no DB access
//! and is exhaustively unit-tested below. The conn-based fact gathering and the
//! execution-wide sweep live in
//! [`crate::execution::advancement::recompute`] (`recompute_job_status_conn`,
//! `recompute_execution_jobs_conn`).

use crate::models::JobStatus;

/// Whether a job's recipe node carries an approval/auto checkpoint, and of which
/// shape. This is the single unified checkpoint classification that replaces the
/// three historical detectors (`get_checkpoint_info`, `has_approval_checkpoint_slot`,
/// `get_job_checkpoint_type`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointGate {
    /// No checkpoint gate on this node.
    None,
    /// Producing node whose output schema declares `confirm_policy: user`: blocks
    /// once its turn completes, until the artifact is confirmed. This is the sole
    /// human-approval gate.
    ConfirmGate,
    /// Standalone checkpoint node running a pass-through command. While the
    /// command is in flight the job is claimed Running; a passing command confirms
    /// the artifact (Complete), a failing one blocks the job (resumable halt). The
    /// fail signal is an unconfirmed checkpoint artifact seeded by `block_job`.
    ///
    /// A checkpoint node has no output contract (no output schema, and no
    /// context-out edge to feed a downstream consumer's input schema), so
    /// `requires_output` is always false for it and the seeded checkpoint
    /// artifact (whose `output_name` is NULL) is read via the unnamed-contract
    /// path. A future context-out on a checkpoint would need the named-contract
    /// artifact lookup revisited to avoid an arm→re-run loop.
    Command,
}

/// The resolution of a job's blocking gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Resolution {
    /// Not yet resolved (artifact not confirmed).
    Pending,
    /// Confirmed — approve (or a passing command checkpoint) flipped
    /// `artifact.confirmed`.
    Confirmed,
}

/// The facts the projection reads. Gathered fresh from the DB by
/// `crate::execution::advancement::recompute`; kept as a plain struct so the
/// derivation itself is a pure, exhaustively testable function.
#[derive(Debug, Clone)]
pub struct JobFacts {
    /// Upstream control dependencies are satisfied (`is_job_ready_conn`).
    pub(crate) dag_ready: bool,
    /// Some upstream control-dependency job is `Failed`.
    pub(crate) upstream_failed: bool,
    /// The latest turn for the job is live (Running/Pending/Yielded). Host waits
    /// (Yielded) keep the job Running.
    pub(crate) live_turn: bool,
    /// The latest turn ended in genuine failure (`TurnState::Failed`).
    /// Interrupt/Cancel are user-initiated pauses, NOT failures — they leave the
    /// job resumable (no terminal turn) so it never cascades to downstream.
    pub(crate) turn_failed: bool,
    /// The latest turn completed successfully.
    pub(crate) turn_complete: bool,
    /// The checkpoint classification of the job's node.
    pub(crate) checkpoint: CheckpointGate,
    /// The resolution of the blocking gate.
    pub(crate) resolution: Resolution,
    /// The node declares an output contract — either its own output schema, or
    /// it feeds a downstream consumer's input schema (e.g. a builder feeding a
    /// `pr` node's `create-pr`). It was told to produce a `cairn:~/<name>`
    /// artifact, so completing without one is a soft-lock, not a real finish.
    pub(crate) requires_output: bool,
    /// An artifact row exists for this job (the declared output was produced).
    pub(crate) artifact_present: bool,
    /// The node derives as long-running from its recipe's topology (a
    /// contract-less control-terminal in a recipe with no terminal action node):
    /// at clean turn-end with no unmet output contract it settles Idle
    /// (non-terminal, resumable) instead of Complete, so it keeps taking wakes
    /// until the issue is closed. Sourced from `is_long_running_node`, not a flag.
    pub(crate) long_running: bool,
}

impl JobFacts {
    /// A bare DAG job with no turn, no checkpoint, dependencies unmet — the
    /// neutral baseline tests tweak one fact at a time from.
    #[cfg(test)]
    fn bare() -> Self {
        JobFacts {
            dag_ready: false,
            upstream_failed: false,
            live_turn: false,
            turn_failed: false,
            turn_complete: false,
            checkpoint: CheckpointGate::None,
            resolution: Resolution::Pending,
            requires_output: false,
            artifact_present: false,
            long_running: false,
        }
    }
}

/// Derive a job's status from its facts. Pure, total, idempotent — the same
/// facts always yield the same status, and illegal statuses are underivable
/// (e.g. `Complete` requires a completion fact).
///
/// There is no `Ready`. A job is **Pending** until its control-deps are met and
/// it is claimed; advancement claims it straight to **Running** (the claim is
/// the `pending→Running` write in `advance_execution_impl`'s pending-scan). The
/// projection therefore never produces a resting "deps met, awaiting start"
/// value — it derives **Pending** for any not-yet-resolved, not-currently-live
/// job, and the sweep is guarded against demoting a claimed `Running` job back
/// to `Pending` (the start-gap before a turn goes live).
///
/// Precedence (for a normal DAG job):
/// 1. Live turn attached -> **Running**. Active work is never preempted by a
///    cascade or readiness re-derivation; the job runs until its turn ends.
/// 2. Latest turn failed, or resolution rejected -> **Failed**.
/// 3. Latest turn completed -> **Complete**, unless a declared output is
///    missing (-> **Blocked**, idle/resumable), the node derives as
///    long-running with no unmet output contract (-> **Idle**, non-terminal),
///    or a User-policy confirm gate is unconfirmed (-> **Blocked**).
/// 4. Upstream control-dep `Failed` -> **Failed** (cascade by derivation).
/// 5. Dependencies unmet -> **Pending**.
/// 6. DAG-ready, no turn yet: standalone-checkpoint nodes resolve from their
///    own facts; everything else is **Pending** (advancement claims it to Running).
///
/// A job's own terminal *turn* (rungs 2-3) outranks the cascade: a job that
/// already ran to completion is not retroactively failed by an upstream that
/// fails later — its work stands. Cascade (rung 4) therefore only reaches jobs
/// that have not produced a terminal turn (unstarted, or a checkpoint node).
pub(crate) fn derive_job_status(facts: &JobFacts) -> JobStatus {
    // 1. A live turn is active work — never preempted by cascade or readiness.
    if facts.live_turn {
        return JobStatus::Running;
    }

    // 2. Ran and failed — terminal.
    if facts.turn_failed {
        return JobStatus::Failed;
    }

    let confirmed = facts.resolution == Resolution::Confirmed;

    // 3. Ran to completion: gate on declared output, then on a User-policy
    //    confirm gate. A completed job's work stands even if an upstream later
    //    fails (cascade is rung 4).
    if facts.turn_complete {
        // A node that declares an output contract must actually produce it before
        // it can complete and fire control-out. Missing output holds Blocked
        // (idle, resumable) rather than advancing downstream onto empty work.
        if facts.requires_output && !facts.artifact_present {
            return JobStatus::Blocked;
        }
        // A long-running node with no unmet output contract settles Idle
        // (non-terminal, resumable) instead of Complete, so it keeps taking
        // wakes until the issue is closed. A node that still owes a required
        // output stays on the contract path above; one whose contract is
        // satisfied still completes normally.
        if facts.long_running && !facts.requires_output {
            return JobStatus::Idle;
        }
        return match facts.checkpoint {
            CheckpointGate::ConfirmGate if !confirmed => JobStatus::Blocked,
            _ => JobStatus::Complete,
        };
    }

    // 4. Cascade by derivation: an upstream control-dependency failure fails a
    //    job that has not run (and is not actively running, handled at rung 1).
    if facts.upstream_failed {
        return JobStatus::Failed;
    }

    // 5. Dependencies unmet: not started.
    if !facts.dag_ready {
        return JobStatus::Pending;
    }

    // 6. DAG-ready, no turn. A command checkpoint resolves from its command —
    //    it never runs an agent turn.
    if facts.checkpoint == CheckpointGate::Command {
        if confirmed {
            return JobStatus::Complete;
        }
        // A failed command seeds an unconfirmed checkpoint artifact (via
        // `block_job`); that artifact is the fail signal -> Blocked (resumable).
        // No artifact yet means the command is still in flight: fall through to
        // Pending and advancement claims it Running to run the command.
        if facts.artifact_present {
            return JobStatus::Blocked;
        }
    }
    // 7. DAG-ready agent / programmatic job, not yet live. The projection
    //    derives Pending; advancement claims it to Running.
    JobStatus::Pending
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! facts {
        ($($field:ident: $value:expr),* $(,)?) => {
            JobFacts {
                $($field: $value,)*
                ..JobFacts::bare()
            }
        };
    }

    #[test]
    fn pending_when_deps_unmet() {
        let f = JobFacts::bare();
        assert_eq!(derive_job_status(&f), JobStatus::Pending);
    }

    #[test]
    fn pending_when_deps_met_no_turn() {
        // No `Ready`: a deps-met, unstarted job derives Pending; advancement
        // claims it straight to Running.
        let f = facts! {
            dag_ready: true,
        };
        assert_eq!(derive_job_status(&f), JobStatus::Pending);
    }

    #[test]
    fn running_when_live_turn() {
        let f = facts! {
            dag_ready: true,
            live_turn: true,
        };
        assert_eq!(derive_job_status(&f), JobStatus::Running);
    }

    #[test]
    fn yielded_turn_keeps_running() {
        // Yielded is folded into `live_turn` by the gatherer; host waits keep it Running.
        let f = facts! {
            dag_ready: true,
            live_turn: true,
        };
        assert_eq!(derive_job_status(&f), JobStatus::Running);
    }

    #[test]
    fn complete_when_turn_complete_no_checkpoint() {
        let f = facts! {
            dag_ready: true,
            turn_complete: true,
        };
        assert_eq!(derive_job_status(&f), JobStatus::Complete);
    }

    #[test]
    fn failed_when_turn_failed() {
        let f = facts! {
            dag_ready: true,
            turn_failed: true,
        };
        assert_eq!(derive_job_status(&f), JobStatus::Failed);
    }

    #[test]
    fn failed_when_upstream_failed_even_if_pending() {
        // Cascade by derivation: upstream failure wins over not-ready.
        let f = facts! {
            dag_ready: false,
            upstream_failed: true,
        };
        assert_eq!(derive_job_status(&f), JobStatus::Failed);
    }

    #[test]
    fn live_turn_beats_upstream_failure() {
        // A running downstream job is not cascade-failed mid-turn; it stays
        // Running until its turn ends (then a later recompute fails it).
        let f = facts! {
            dag_ready: false,
            upstream_failed: true,
            live_turn: true,
        };
        assert_eq!(derive_job_status(&f), JobStatus::Running);
    }

    #[test]
    fn completed_job_not_retroactively_cascaded() {
        // A job that already completed is not failed by a late upstream failure;
        // its work stands (matches the pre-projection cascade, which skipped
        // terminal jobs).
        let f = facts! {
            dag_ready: true,
            turn_complete: true,
            upstream_failed: true,
        };
        assert_eq!(derive_job_status(&f), JobStatus::Complete);
    }

    #[test]
    fn failed_turn_beats_unmet_readiness() {
        // A job that ran and failed is terminal even if its node looks not-ready
        // (it clearly started, so it was ready).
        let f = facts! {
            dag_ready: false,
            turn_failed: true,
        };
        assert_eq!(derive_job_status(&f), JobStatus::Failed);
    }

    #[test]
    fn blocked_when_required_output_missing() {
        // Declares an output contract but ended its turn without producing it:
        // hold Blocked (idle/resumable), do not advance downstream.
        let f = facts! {
            dag_ready: true,
            turn_complete: true,
            requires_output: true,
            artifact_present: false,
        };
        assert_eq!(derive_job_status(&f), JobStatus::Blocked);
    }

    #[test]
    fn complete_when_required_output_present() {
        // Output produced (e.g. a builder wrote create-pr): completes and fires
        // control-out. No embedded checkpoint here (PR-feeding nodes auto-confirm).
        let f = facts! {
            dag_ready: true,
            turn_complete: true,
            requires_output: true,
            artifact_present: true,
        };
        assert_eq!(derive_job_status(&f), JobStatus::Complete);
    }

    #[test]
    fn complete_when_no_output_required() {
        // A node with no declared output is unchanged by the gate.
        let f = facts! {
            dag_ready: true,
            turn_complete: true,
            requires_output: false,
        };
        assert_eq!(derive_job_status(&f), JobStatus::Complete);
    }

    #[test]
    fn confirm_gate_with_output_present_but_unconfirmed_still_blocks() {
        // Output written but a User-policy confirm gate remains: Blocked for
        // confirmation, same as before the output gate existed.
        let f = facts! {
            dag_ready: true,
            turn_complete: true,
            requires_output: true,
            artifact_present: true,
            checkpoint: CheckpointGate::ConfirmGate,
            resolution: Resolution::Pending,
        };
        assert_eq!(derive_job_status(&f), JobStatus::Blocked);
    }

    #[test]
    fn blocked_when_confirm_gate_unconfirmed() {
        let f = facts! {
            dag_ready: true,
            turn_complete: true,
            checkpoint: CheckpointGate::ConfirmGate,
            resolution: Resolution::Pending,
        };
        assert_eq!(derive_job_status(&f), JobStatus::Blocked);
    }

    #[test]
    fn complete_when_confirm_gate_confirmed() {
        let f = facts! {
            dag_ready: true,
            turn_complete: true,
            checkpoint: CheckpointGate::ConfirmGate,
            resolution: Resolution::Confirmed,
        };
        assert_eq!(derive_job_status(&f), JobStatus::Complete);
    }

    #[test]
    fn confirm_gate_pending_before_turn_completes() {
        // An agent node carrying a confirm gate is just an unstarted job until
        // its turn completes: Pending (advancement claims to Running).
        let f = facts! {
            dag_ready: true,
            checkpoint: CheckpointGate::ConfirmGate,
        };
        assert_eq!(derive_job_status(&f), JobStatus::Pending);
    }

    #[test]
    fn command_checkpoint_pending_while_in_flight() {
        // No artifact yet: the command is still running. Claimed Running by
        // advancement; the projection derives Pending until it resolves.
        let f = facts! {
            dag_ready: true,
            checkpoint: CheckpointGate::Command,
        };
        assert_eq!(derive_job_status(&f), JobStatus::Pending);
    }

    #[test]
    fn command_checkpoint_blocks_on_fail() {
        // A failed command seeds an unconfirmed checkpoint artifact (block_job);
        // an unconfirmed artifact present is the fail signal -> Blocked.
        let f = facts! {
            dag_ready: true,
            checkpoint: CheckpointGate::Command,
            artifact_present: true,
            resolution: Resolution::Pending,
        };
        assert_eq!(derive_job_status(&f), JobStatus::Blocked);
    }

    #[test]
    fn idle_when_long_running_no_output() {
        // The core coordinator-on-main case: a flagged node ends its turn with
        // no output contract and settles Idle (non-terminal, resumable).
        let f = facts! {
            dag_ready: true,
            turn_complete: true,
            long_running: true,
            requires_output: false,
        };
        assert_eq!(derive_job_status(&f), JobStatus::Idle);
    }

    #[test]
    fn long_running_ignored_when_output_present() {
        // A flagged node that still carries an output contract honors it: with
        // the artifact produced it completes normally rather than idling.
        let f = facts! {
            dag_ready: true,
            turn_complete: true,
            long_running: true,
            requires_output: true,
            artifact_present: true,
        };
        assert_eq!(derive_job_status(&f), JobStatus::Complete);
    }

    #[test]
    fn long_running_still_blocks_on_missing_output() {
        // The missing-output wedge outranks the idle path: a flagged node that
        // owes an output but has not produced it blocks, not idles.
        let f = facts! {
            dag_ready: true,
            turn_complete: true,
            long_running: true,
            requires_output: true,
            artifact_present: false,
        };
        assert_eq!(derive_job_status(&f), JobStatus::Blocked);
    }

    #[test]
    fn long_running_running_while_turn_live() {
        // A live turn always outranks idle: the flagged node runs until its
        // turn ends, then re-derives Idle.
        let f = facts! {
            dag_ready: true,
            live_turn: true,
            long_running: true,
        };
        assert_eq!(derive_job_status(&f), JobStatus::Running);
    }

    #[test]
    fn command_checkpoint_completes_on_pass() {
        let f = facts! {
            dag_ready: true,
            checkpoint: CheckpointGate::Command,
            artifact_present: true,
            resolution: Resolution::Confirmed,
        };
        assert_eq!(derive_job_status(&f), JobStatus::Complete);
    }
}
