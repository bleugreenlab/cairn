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

/// True when issue `resolved_issue` is resolved: merged or closed. This is the
/// ONE definition of the issue half of execution liveness. [`live_execution_sql!`]
/// (and so the gate sweep's [`awaiting_release_sql!`]) excludes an execution on
/// such an issue, and filing an execution refuses such an issue, so release and
/// filing cannot disagree about which issues take no new work. It reads the
/// resolution timestamps, not `status`. Expects an `issues` row aliased
/// `resolved_issue`.
#[macro_export]
macro_rules! resolved_issue_sql {
    () => {
        "(resolved_issue.merged_at IS NOT NULL OR resolved_issue.closed_at IS NOT NULL)"
    };
}

/// True when execution `e` can still act: neither superseded nor withdrawn,
/// and its issue (if any) not resolved ([`resolved_issue_sql!`]). Expects an
/// `executions` row aliased `e`.
#[macro_export]
macro_rules! live_execution_sql {
    () => {
        concat!(
            "e.lifecycle NOT IN ('superseded', 'withdrawn') AND NOT EXISTS (SELECT 1 FROM issues resolved_issue WHERE resolved_issue.id = e.issue_id AND ",
            $crate::resolved_issue_sql!(),
            ")"
        )
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

/// The executions that have gate rows, as the row source of a statement that
/// LISTS gated or held executions. [`gated_unreleased_sql!`] implies it (and so
/// [`awaiting_release_sql!`] does), so ANDing it changes no result; it lets
/// the planner walk `execution_gates` and seek each execution through
/// `idx_executions_hold_facts` instead of scanning `executions`, whose every
/// row read costs that row's snapshot (cairn/5894). The list is deduplicated,
/// so an execution with several gates comes back once.
///
/// Only as a top-level WHERE conjunct: under OR, CASE or NOT, Turso rebuilds or
/// probes the list per row (cairn/5332), which is why the predicates keep their
/// EXISTS. Name no `INDEXED BY` on `e` beside it: the pinned Turso serves a
/// forced index only through an equality, so the hint turns each seek into a
/// scan of the whole index. Expects an `executions` row aliased `e`.
#[macro_export]
macro_rules! gated_execution_source_sql {
    () => {
        "e.id IN (SELECT gated_source.execution_id FROM execution_gates gated_source)"
    };
}

/// The executions with an UNSATISFIED gate row: [`gated_execution_source_sql!`]
/// narrowed to the rows `idx_execution_gates_unsatisfied` holds, which grow
/// with live work rather than with history. A source only for a statement
/// whose predicate requires an unsatisfied authored gate; an execution whose gates are all satisfied
/// but which is still unreleased is still held, and this would miss it. The
/// same placement rules apply. Expects an `executions` row aliased `e`.
#[macro_export]
macro_rules! unsatisfied_gate_source_sql {
    () => {
        "e.id IN (SELECT unsatisfied_source.execution_id FROM execution_gates unsatisfied_source WHERE unsatisfied_source.satisfied_at IS NULL)"
    };
}

/// True when prompt `p` is an open question on its job `j`'s current turn:
/// unanswered, unwithdrawn, on a live job, on a turn that was not cancelled.
/// Expects `prompts p`, `jobs j` (resolved as `COALESCE(p.job_id, r.job_id)`)
/// and `turns pt` (the prompt's turn, LEFT JOINed). The notification center
/// lists exactly these as questions.
#[macro_export]
macro_rules! open_prompt_sql {
    () => {
        "p.response IS NULL AND p.withdrawn_at IS NULL AND p.turn_id = j.current_turn_id AND j.status NOT IN ('complete', 'failed', 'cancelled') AND COALESCE(pt.state, '') <> 'cancelled'"
    };
}

/// True when question `p` waits on the operator: unanswered and unwithdrawn,
/// and either asked non-blocking (`p.background = 1`) by a job that is still
/// live, or a blocking question holding its job ([`open_prompt_sql!`]).
///
/// The two predicates answer different questions and must not be merged.
/// [`open_prompt_sql!`] is "this question holds its job":
/// [`job_awaits_operator_sql!`] count on it, and a non-blocking asker is not
/// held, since its turn moves on by design. This one is "the operator owes
/// this question an answer", for the surfaces that list questions to the
/// operator: the notification center, a questions listing's open state, the
/// thread seed and the children census. An MCP tool's input request (owned by
/// its durable wait, `mcp_wait_id`) is the wait's to surface, not this one's.
/// Expects `prompts p`, `jobs j` and `turns pt` as [`open_prompt_sql!`] does.
#[macro_export]
macro_rules! pending_question_sql {
    () => {
        concat!(
            "(p.response IS NULL AND p.withdrawn_at IS NULL AND ((p.background = 1 AND p.mcp_wait_id IS NULL AND j.status NOT IN ('complete', 'failed', 'cancelled')) OR (",
            $crate::open_prompt_sql!(),
            ")))"
        )
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

/// True when job `j` has an active launch or resume hold. Held work is not placeable,
/// but remains nonterminal, so its execution stays live. Expects a `jobs` row
/// aliased `j`.
#[macro_export]
macro_rules! job_held_sql {
    () => {
        concat!(
            "EXISTS (SELECT 1 FROM provider_limit_holds supply_hold WHERE supply_hold.job_id = j.id AND ",
            $crate::supply_hold_owns_job_sql!(),
            ")"
        )
    };
}

/// A live execution held entirely at pending job launch, even after release.
/// The hold table supplies candidate execution ids; runnable siblings exclude it.
/// Expects execution alias `e`.
#[macro_export]
macro_rules! launch_held_execution_sql {
    () => {
        concat!(
            "e.id IN (SELECT j.execution_id FROM provider_limit_holds supply_hold
                       JOIN jobs j ON j.id = supply_hold.job_id
                       WHERE supply_hold.kind = 'launch' AND ",
            $crate::supply_hold_owns_job_sql!(),
            ") AND ", $crate::live_execution_sql!(),
            " AND EXISTS (SELECT 1 FROM issues held_issue WHERE held_issue.id = e.issue_id
                           AND held_issue.merged_at IS NULL AND held_issue.closed_at IS NULL)
              AND NOT EXISTS (SELECT 1 FROM jobs live_job WHERE live_job.execution_id = e.id
                   AND (live_job.status = 'running' OR (live_job.status = 'pending'
                        AND NOT EXISTS (SELECT 1 FROM provider_limit_holds h
                            WHERE h.job_id = live_job.id AND h.state = 'active' AND h.kind = 'launch'))))"
        )
    };
}

/// True when hold `supply_hold` is an active launch or resume hold that still
/// owns job `j`: a launch hold's job is still `pending`, a resume hold's job
/// still rests on the refused head. A hold whose node moved on (an operator's
/// start or continue) holds nothing, even before the scheduler retires it.
/// Expects a `provider_limit_holds` row aliased `supply_hold` and a `jobs`
/// row aliased `j`.
#[macro_export]
macro_rules! supply_hold_owns_job_sql {
    () => {
        "supply_hold.state = 'active' AND ((supply_hold.kind = 'launch' AND j.status = 'pending') OR (supply_hold.kind = 'resume' AND j.current_turn_id = supply_hold.turn_id))"
    };
}

/// True when job `j` has not ended: `pending`, `running`, `blocked` or `idle`.
/// A job that waits on the operator (a question, a permission, a PR or plan
/// awaiting review) is always one of these, so it is covered; `idle` is
/// resumable, which is how a long-running coordinator child waits. Expects a
/// `jobs` row aliased `j`.
#[macro_export]
macro_rules! nonterminal_job_sql {
    () => {
        "j.status IN ('pending', 'running', 'blocked', 'idle')"
    };
}

/// True when issue `i` is an OPEN child for listings: unresolved, with a
/// released live execution and nonterminal job, or an open PR. Includes both
/// running and waiting work. This is a listing predicate, not queue admission.
/// The caller adds `i.parent_thread_id = ?`. Expects an `issues` row aliased `i`;
/// intentionally independent of device scope.
#[macro_export]
macro_rules! open_thread_child_sql {
    () => {
        concat!(
            "i.merged_at IS NULL AND i.closed_at IS NULL AND (EXISTS (SELECT 1 FROM executions e JOIN jobs j ON j.execution_id = e.id WHERE e.issue_id = i.id AND ",
            $crate::live_execution_sql!(),
            " AND NOT (",
            $crate::gated_unreleased_sql!(),
            ") AND ",
            $crate::nonterminal_job_sql!(),
            ") OR EXISTS (SELECT 1 FROM merge_requests open_pr WHERE open_pr.issue_id = i.id AND open_pr.status = 'open'))"
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

#[cfg(test)]
mod tests {
    use crate::storage::{LocalDb, RowExt};

    /// A project, a thread `th`, and a parent issue 9 for grandchildren.
    async fn world() -> LocalDb {
        let db = crate::storage::migrated_test_db("open-thread-child.db").await;
        db.execute_script(
            "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES('p', 'default', 'P', 'prj', '/tmp/p', 1, 1);
             INSERT INTO threads(id, project_id, name, created_at, updated_at)
               VALUES('th', 'p', 'harness', 1, 1);
             INSERT INTO issues(id, project_id, number, title, status, progress, attention,
                                created_at, updated_at)
               VALUES('i9', 'p', 9, 'Parent', 'active', 'active', 'none', 1, 1);",
        )
        .await
        .unwrap();
        db
    }

    /// A direct child of `th` with no execution: a backlog child.
    async fn child(db: &LocalDb, number: i32) {
        db.execute_script(format!(
            "INSERT INTO issues(id, project_id, number, title, status, progress, attention,
                                parent_thread_id, created_at, updated_at)
               VALUES('i{number}', 'p', {number}, 'Child {number}', 'active', 'active', 'none',
                      'th', 1, 1);"
        ))
        .await
        .unwrap();
    }

    /// A started execution on issue `number` whose one job has `status`.
    async fn started(db: &LocalDb, number: i32, lifecycle: &str, status: &str) {
        db.execute_script(format!(
            "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq,
                                    lifecycle)
               VALUES('e{number}', 'r', 'i{number}', 'p', 'running', 1, 1, '{lifecycle}');
             INSERT INTO jobs(id, execution_id, project_id, issue_id, node_name, status,
                              created_at, updated_at)
               VALUES('j{number}', 'e{number}', 'p', 'i{number}', 'builder', '{status}', 1, 1);"
        ))
        .await
        .unwrap();
    }

    async fn pr(db: &LocalDb, number: i32, status: &str) {
        db.execute_script(format!(
            "INSERT INTO merge_requests(id, job_id, project_id, issue_id, title, source_branch,
                                        target_branch, status, opened_at, updated_at)
               VALUES('mr{number}', 'j{number}', 'p', 'i{number}', 'PR', 'b', 'main', '{status}',
                      1, 1);"
        ))
        .await
        .unwrap();
    }

    async fn counted(db: &LocalDb) -> Vec<i64> {
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        concat!(
                            "SELECT i.number FROM issues i WHERE i.parent_thread_id = 'th' AND ",
                            open_thread_child_sql!(),
                            " ORDER BY i.number"
                        ),
                        (),
                    )
                    .await?;
                let mut out = Vec::new();
                while let Some(row) = rows.next().await? {
                    out.push(row.i64(0)?);
                }
                Ok(out)
            })
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn a_child_counts_while_it_has_live_work_or_an_open_pr() {
        let db = world().await;

        // Failed at preparation with no PR (cairn/4741's shape): the job
        // failed and its run ended `job_preparation_failed`.
        child(&db, 1).await;
        started(&db, 1, "canonical", "failed").await;
        db.execute_script(
            "INSERT INTO runs(id, project_id, job_id, issue_id, status, exit_reason,
                              created_at, updated_at)
               VALUES('r1', 'p', 'j1', 'i1', 'failed', 'job_preparation_failed', 1, 1);",
        )
        .await
        .unwrap();

        // Failed, but its PR is open.
        child(&db, 2).await;
        started(&db, 2, "canonical", "failed").await;
        pr(&db, 2, "open").await;

        // A PR awaiting review leaves its job blocked.
        child(&db, 3).await;
        started(&db, 3, "canonical", "blocked").await;
        pr(&db, 3, "open").await;

        // A coordinator child waiting idle.
        child(&db, 4).await;
        started(&db, 4, "canonical", "idle").await;

        // Only a superseded execution, its job never ended.
        child(&db, 5).await;
        started(&db, 5, "superseded", "running").await;

        // A withdrawn execution cannot regain liveness through stale job state.
        child(&db, 13).await;
        started(&db, 13, "withdrawn", "running").await;

        // A backlog child.
        child(&db, 6).await;

        // A grandchild: its parent is an issue, not the thread.
        db.execute_script(
            "INSERT INTO issues(id, project_id, number, title, status, progress, attention,
                                parent_issue_id, created_at, updated_at)
               VALUES('i7', 'p', 7, 'Grandchild', 'active', 'active', 'none', 'i9', 1, 1);",
        )
        .await
        .unwrap();
        started(&db, 7, "canonical", "running").await;

        // Merged, and closed, with work still running.
        child(&db, 8).await;
        started(&db, 8, "canonical", "running").await;
        child(&db, 10).await;
        started(&db, 10, "canonical", "running").await;
        pr(&db, 10, "open").await;
        db.execute_script(
            "UPDATE issues SET merged_at = 5 WHERE id = 'i8';
             UPDATE issues SET closed_at = 5 WHERE id = 'i10';",
        )
        .await
        .unwrap();

        // A closed PR with failed jobs.
        child(&db, 11).await;
        started(&db, 11, "canonical", "failed").await;
        pr(&db, 11, "closed").await;

        // A held execution (gated, unreleased) is not work under way.
        child(&db, 12).await;
        db.execute_script(
            "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
               VALUES('e12', 'r', 'i12', 'p', 'held', 1, 1);
             INSERT INTO execution_gates(id, execution_id, seq, predicate_json, created_at)
               VALUES('g12', 'e12', 0, '{\"fact\":\"clock\"}', 1);",
        )
        .await
        .unwrap();

        assert_eq!(counted(&db).await, vec![2, 3, 4]);

        // A restart makes the failed child count again. It takes the attempt,
        // superseding the failed execution, as a launch does.
        db.execute_script(
            "UPDATE executions SET lifecycle = 'superseded' WHERE id = 'e1';
             INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
               VALUES('e1b', 'r', 'i1', 'p', 'running', 2, 2);
             INSERT INTO jobs(id, execution_id, project_id, issue_id, node_name, status,
                              created_at, updated_at)
               VALUES('j1b', 'e1b', 'p', 'i1', 'builder', 'pending', 2, 2);",
        )
        .await
        .unwrap();
        assert_eq!(counted(&db).await, vec![1, 2, 3, 4]);
    }
}
