//! The SQL predicates that decide whether a condition evaluation still matters.
//!
//! Several projections must agree on when a blocked condition is waiting on
//! someone: the execution status, the issue's `needs_approval` attention, the
//! attention item's blocked owner, and the guard on recording a new evaluation.
//! They live in different crates, so the predicates are macros that expand to
//! string literals, usable inside `concat!` to build `&'static str` queries.

/// True when execution `e` can still act: not superseded by a newer attempt,
/// and its issue (if any) not resolved. Expects an `executions` row aliased
/// `e`.
#[macro_export]
macro_rules! live_execution_sql {
    () => {
        "e.lifecycle != 'superseded' AND NOT EXISTS (SELECT 1 FROM issues resolved_issue WHERE resolved_issue.id = e.issue_id AND (resolved_issue.merged_at IS NOT NULL OR resolved_issue.closed_at IS NOT NULL))"
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
