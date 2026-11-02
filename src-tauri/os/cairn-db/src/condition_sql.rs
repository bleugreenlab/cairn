//! The SQL predicates that decide whether a condition evaluation still matters.
//!
//! Several projections must agree on when a blocked condition is waiting on
//! someone: the execution status, the issue's `needs_approval` attention, the
//! attention item's blocked owner, and the guard on recording a new evaluation.
//! They live in different crates, so the predicates are macros that expand to
//! string literals, usable inside `concat!` to build `&'static str` queries.
//!
//! The same holds for gated executions: the gate sweep and the gate census must
//! agree on which held executions are still waiting to start, so that
//! predicate lives here too.

/// True when execution `e` can still act: not superseded by a newer attempt,
/// and its issue (if any) not resolved. Expects an `executions` row aliased
/// `e`.
#[macro_export]
macro_rules! live_execution_sql {
    () => {
        "e.lifecycle != 'superseded' AND NOT EXISTS (SELECT 1 FROM issues resolved_issue WHERE resolved_issue.id = e.issue_id AND (resolved_issue.merged_at IS NOT NULL OR resolved_issue.closed_at IS NOT NULL))"
    };
}

/// True when execution `e` was filed with `after:` gates and has not been
/// released. On its own this does NOT mean the execution will start: see
/// [`awaiting_release_sql!`]. Expects an `executions` row aliased `e`.
#[macro_export]
macro_rules! gated_unreleased_sql {
    () => {
        "e.released_at IS NULL AND EXISTS (SELECT 1 FROM execution_gates held_gate WHERE held_gate.execution_id = e.id)"
    };
}

/// True when execution `e` is waiting to start: gated, unreleased, and still
/// live. This is the ONE definition of "held" — the gate sweep releases exactly
/// these, and the gate census lists exactly these as held. A gated, unreleased
/// execution that fails it (superseded, or its issue resolved) can never start,
/// and every surface says so instead of calling it waiting. Expects an
/// `executions` row aliased `e`.
#[macro_export]
macro_rules! awaiting_release_sql {
    () => {
        concat!(
            $crate::gated_unreleased_sql!(),
            " AND ",
            $crate::live_execution_sql!()
        )
    };
}

/// True when gate row `g` is a CAPACITY hold the system filed (the execution
/// queue's fleet slot or thread focus), not an authored `after:` gate. Expects
/// an `execution_gates` row aliased `g`. The fact names are
/// `execution::gates::capacity`'s, which a test holds to this string.
#[macro_export]
macro_rules! capacity_gate_sql {
    () => {
        "json_extract(g.predicate_json, '$.fact') IN ('fleet_slot', 'thread_focus')"
    };
}

/// True when execution `e` still waits on an AUTHORED `after:` gate. Such an
/// execution takes no place in the execution queue: a capacity row it holds
/// from before an `after:` patch is neither ahead of anyone nor numbered, and
/// its place is decided again when its own gates clear. Expects an
/// `executions` row aliased `e`; spells the capacity facts as
/// [`capacity_gate_sql!`] does (a test holds the two together).
#[macro_export]
macro_rules! awaits_authored_gate_sql {
    () => {
        "EXISTS (SELECT 1 FROM execution_gates authored_gate WHERE authored_gate.execution_id = e.id AND authored_gate.satisfied_at IS NULL AND json_extract(authored_gate.predicate_json, '$.fact') NOT IN ('fleet_slot', 'thread_focus'))"
    };
}

/// True when prompt `p` is an open question on its job `j`'s current turn:
/// unanswered, unwithdrawn, on a live job, on a turn that was not cancelled.
/// Expects `prompts p`, `jobs j` (resolved as `COALESCE(p.job_id, r.job_id)`)
/// and `turns pt` (the prompt's turn, LEFT JOINed). The notification center
/// lists exactly these as questions, and the fleet cap frees exactly these
/// jobs' slots, so the two can never disagree about who waits on the operator.
#[macro_export]
macro_rules! open_prompt_sql {
    () => {
        "p.response IS NULL AND p.withdrawn_at IS NULL AND p.turn_id = j.current_turn_id AND j.status NOT IN ('complete', 'failed', 'cancelled') AND COALESCE(pt.state, '') <> 'cancelled'"
    };
}

/// True when permission request `pr` still waits on the operator. Expects a
/// `permission_requests` row aliased `pr`.
#[macro_export]
macro_rules! pending_permission_sql {
    () => {
        "pr.status = 'pending'"
    };
}

/// True when job `j` waits on the operator: an open question on its current
/// turn ([`open_prompt_sql!`]) or a pending permission request
/// ([`pending_permission_sql!`]). Expects a `jobs` row aliased `j`.
#[macro_export]
macro_rules! job_awaits_operator_sql {
    () => {
        concat!(
            "(EXISTS (SELECT 1 FROM prompts p JOIN runs r ON r.id = p.run_id LEFT JOIN turns pt ON pt.id = p.turn_id WHERE COALESCE(p.job_id, r.job_id) = j.id AND ",
            $crate::open_prompt_sql!(),
            ") OR EXISTS (SELECT 1 FROM permission_requests pr JOIN runs r ON r.id = pr.run_id WHERE COALESCE(pr.job_id, r.job_id) = j.id AND ",
            $crate::pending_permission_sql!(),
            "))"
        )
    };
}

/// True when execution `e` is one the FLEET cap may count: live, started (not
/// held), and not a thread's host execution (a thread's task jobs live in an
/// issue-less host execution, and threads never count). Expects an
/// `executions` row aliased `e`.
///
/// Whether it counts also needs a working job ([`fleet_working_job_sql!`],
/// then a machine-use classification of that job's yielded turn in
/// `execution::gates::capacity`) and `e.runner_device_id` to be THIS device:
/// each runner owns its own fleet, so a team member's executions consume
/// theirs. That device scope is deliberate and is the opposite of
/// [`open_thread_child_sql!`], which has none. Do not align the two.
#[macro_export]
macro_rules! fleet_counted_execution_sql {
    () => {
        concat!(
            $crate::live_execution_sql!(),
            " AND NOT (",
            $crate::gated_unreleased_sql!(),
            ") AND NOT EXISTS (SELECT 1 FROM jobs thread_host_job WHERE thread_host_job.execution_id = e.id AND thread_host_job.thread_id IS NOT NULL)"
        )
    };
}

/// True when job `j` may hold its execution's fleet slot: `running`, or
/// `pending` and placeable, and not waiting on the operator. Expects a `jobs`
/// row aliased `j`.
///
/// A placeable `pending` job counts, so a release burst cannot overrun the
/// fleet before it is placed (a freshly released root job has no
/// predecessors). A `pending` job is NOT placeable while a job in its
/// execution is waiting on the operator (`blocked` on an approval or review,
/// or [`job_awaits_operator_sql!`], the one definition the notification
/// center also reads): that is a downstream node whose
/// predecessor is parked on the operator, and counting it would keep an
/// execution in plan review holding a slot with nothing running. That test
/// is an approximation of "its DAG predecessors are complete": a placeable
/// parallel branch beside a parked sibling is under-counted, which
/// over-releases by one (the safe direction; per-machine admission absorbs
/// it). `blocked`, `idle` and terminal jobs never count.
#[macro_export]
macro_rules! fleet_working_job_sql {
    () => {
        concat!(
            // The subquery rebinds `j` to each job that waits on the
            // operator, so the shared predicate applies to it unchanged; the
            // left-hand `j.execution_id` is still the outer job's. The job
            // itself needs no exclusion: it is `pending`, so not `blocked`,
            // and if it awaits the operator the outer test drops it anyway.
            "(j.status = 'running' OR (j.status = 'pending' AND j.execution_id NOT IN (SELECT j.execution_id FROM jobs j WHERE j.execution_id IS NOT NULL AND j.status IN ('blocked', 'pending', 'running') AND (j.status = 'blocked' OR ",
            $crate::job_awaits_operator_sql!(),
            ")))) AND NOT ",
            $crate::job_awaits_operator_sql!()
        )
    };
}

/// True when issue `i` is an OPEN STARTED child for the thread focus cap: not
/// merged or closed, with an execution that has started (any execution that is
/// not gated-and-unreleased). Open while its PR waits for review or merge, on
/// purpose: that is the stale work the cap exists to hold new work behind. The
/// caller adds `i.parent_thread_id = ?`, so grandchildren (whose parent is an
/// issue) and sub-agent tasks (jobs, not issues) never count, and a reparented
/// child moves between counts at once. Expects an `issues` row aliased `i`.
///
/// No device scope, deliberately the opposite of
/// [`fleet_counted_execution_sql!`]: a thread's open work is open wherever it
/// runs.
#[macro_export]
macro_rules! open_thread_child_sql {
    () => {
        concat!(
            "i.merged_at IS NULL AND i.closed_at IS NULL AND EXISTS (SELECT 1 FROM executions e WHERE e.issue_id = i.id AND NOT (",
            $crate::gated_unreleased_sql!(),
            "))"
        )
    };
}

/// True when condition row `ce` is blocked and its node has no routed row, so
/// the condition is still routed nowhere. Expects a `condition_evaluations` row
/// aliased `ce`.
#[macro_export]
macro_rules! still_blocked_condition_sql {
    () => {
        "ce.outcome = 'blocked' AND NOT EXISTS (SELECT 1 FROM condition_evaluations routed WHERE routed.execution_id = ce.execution_id AND routed.recipe_node_id = ce.recipe_node_id AND routed.outcome = 'routed')"
    };
}
