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

/// True when condition row `ce` is blocked and its node has no routed row, so
/// the condition is still routed nowhere. Expects a `condition_evaluations` row
/// aliased `ce`.
#[macro_export]
macro_rules! still_blocked_condition_sql {
    () => {
        "ce.outcome = 'blocked' AND NOT EXISTS (SELECT 1 FROM condition_evaluations routed WHERE routed.execution_id = ce.execution_id AND routed.recipe_node_id = ce.recipe_node_id AND routed.outcome = 'routed')"
    };
}
