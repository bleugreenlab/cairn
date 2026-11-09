use crate::storage::{DbError, LocalDb, RowExt};
use crate::turso::params;
/// The thread that owns a job created beneath `parent_job_id`, or `None` when
/// the parent belongs to no thread.
///
/// Thread ownership is inherited at creation, never re-derived by descent: a
/// thread's session job carries `jobs.thread_id`, and every child it spawns — a
/// sub-agent task, an ephemeral call — belongs to the same thread. That column
/// is the whole of what the thread pane can see, because `list_jobs_for_thread`
/// selects on it alone; a child that fails to inherit it cannot be listed in the
/// thread's task rollup, opened as a tab, rolled into the thread's status, or
/// joined to through its artifacts. A job whose parent belongs to an issue
/// execution has no thread to inherit and stays NULL.
///
/// Inheriting from the immediate parent is enough for arbitrary nesting: each
/// generation is stamped when it is created, so a grandchild reads an already
/// stamped parent.
pub async fn inherited_thread_id_conn(
    conn: &crate::storage::TrackedConnection,
    parent_job_id: &str,
) -> crate::storage::DbResult<Option<String>> {
    let mut rows = conn
        .query(
            "SELECT thread_id FROM jobs WHERE id = ?1",
            crate::turso::params![parent_job_id],
        )
        .await?;
    match rows.next().await? {
        Some(row) => row.get::<Option<String>>(0).map_err(Into::into),
        None => Ok(None),
    }
}

/// A thread's current lifecycle state, or `None` when no such thread exists.
///
/// The eligibility question every dormancy boundary asks, resolved from the one
/// column that answers it.
pub async fn thread_status_conn(
    conn: &crate::storage::TrackedConnection,
    thread_id: &str,
) -> crate::storage::DbResult<Option<crate::models::ThreadStatus>> {
    let mut rows = conn
        .query(
            "SELECT status FROM threads WHERE id = ?1",
            crate::turso::params![thread_id],
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };
    crate::models::ThreadStatus::parse(&row.get::<String>(0)?)
        .map(Some)
        .map_err(crate::storage::DbError::Row)
}

/// Whether `job_id` is the session job of a thread that is currently closed.
///
/// The one dormancy predicate, asked at every boundary that would give a thread
/// work: wake delivery, the direct parent push, the queued-push resume gate, and
/// turn admission itself. Naming the dormant thing rather than a per-boundary
/// notion of "eligible" is deliberate — the boundaries differ, the question does
/// not.
///
/// It keys on [`SESSION_JOB_SHAPE`] rather than `jobs.thread_id`, because a
/// thread's sub-agent tasks carry its id too and they are ordinary jobs whose own
/// work closure does not touch.
///
/// A job Cairn cannot resolve is NOT dormant. That is the safe direction: an
/// unresolvable job keeps the behaviour every non-thread job has, and the only
/// thing this gate exists to stop is giving work to a thread that was closed.
pub async fn is_dormant_thread_session(db: &crate::storage::LocalDb, job_id: &str) -> bool {
    let job_id = job_id.to_string();
    let closed = db
        .read(move |conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        &format!(
                            "SELECT 1 FROM jobs j
                             JOIN threads t ON t.id = j.thread_id
                             WHERE j.id = ?1 AND {SESSION_JOB_SHAPE} AND t.status = 'closed'
                             LIMIT 1"
                        ),
                        crate::turso::params![job_id],
                    )
                    .await?;
                Ok(rows.next().await?.is_some())
            })
        })
        .await;
    closed.unwrap_or(false)
}

pub const SESSION_JOB_SHAPE: &str = "j.parent_job_id IS NULL AND j.uri_segment = 'thread'";

/// One of an issue's two parent edges, as filing proof names them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FilingParent {
    /// `issues.parent_issue_id`.
    Issue,
    /// `issues.parent_thread_id`.
    Thread,
}

impl FilingParent {
    /// The filing-proof rule for this parent edge: the one definition of "this
    /// edge was created by filing and has not changed since", which every
    /// authority derived from issue parentage applies (permission-answer
    /// ancestry, gated-artifact confirmation).
    ///
    /// A SQL predicate over `i` (an `issues` row) and `j` (a `jobs` row), meant
    /// as a join condition (`issues i JOIN jobs j ON {proof}`). It holds exactly
    /// when `j` is the issue's recorded filing job (`i.parent_job_id`), that job
    /// sits on the parent this edge names (it ran on the parent issue, or it is
    /// the parent thread's session job), and `i.filing_parent_revoked_at` is
    /// unset. Migration `0406_issue_filing_parent_revocation` stamps that column
    /// on every pre-existing edge and on the first change to any edge, so a
    /// legacy, adopted, or reparented edge never satisfies it. A missing filing
    /// job or parent compares as NULL and fails the predicate: an edge whose
    /// proof cannot be determined is unproven.
    ///
    /// It proves a single edge. A walk across several hops joins it at every
    /// hop it crosses.
    pub fn proof_sql(self) -> String {
        let parent = match self {
            FilingParent::Issue => "j.issue_id = i.parent_issue_id".to_string(),
            FilingParent::Thread => {
                format!("j.thread_id = i.parent_thread_id AND {SESSION_JOB_SHAPE}")
            }
        };
        format!("j.id = i.parent_job_id AND i.filing_parent_revoked_at IS NULL AND {parent}")
    }
}

/// The job's canonical home URI. Issue jobs resolve to their node URI
/// (`cairn://p/{KEY}/{number}/{seq}/{segment}`, nesting sub-agent tasks under
/// their parent); a thread's session resolves to `cairn://p/{KEY}/{thread-name}`
/// and the tasks it spawns nest beneath it the same way. This is the one
/// canonical job-id → home-URI resolution.
/// `Ok(None)` when the job can't be resolved (unknown id, no owning issue or
/// thread, or no `uri_segment` assigned yet).
pub async fn home_uri_for_job(db: &LocalDb, job_id: &str) -> Result<Option<String>, DbError> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move { home_uri_for_job_conn(conn, &job_id).await })
    })
    .await
}

/// Connection-level variant of [`home_uri_for_job`] for callers already inside
/// a caller-owned transaction.
pub async fn home_uri_for_job_conn(
    conn: &crate::storage::TrackedConnection,
    job_id: &str,
) -> Result<Option<String>, DbError> {
    let mut rows = conn
        .query(
            &format!(
                "SELECT p.key, i.number, COALESCE(e.seq, 1), j.uri_segment,
                        parent.uri_segment, j.agent_config_id, t.name,
                        (j.thread_id IS NOT NULL AND {session}) AS is_thread_session,
                        j.service_thread_id, st.name AS service_thread_name
                 FROM jobs j
                 LEFT JOIN issues i ON i.id = j.issue_id
                 LEFT JOIN jobs parent ON j.parent_job_id = parent.id
                 LEFT JOIN threads t ON t.id = COALESCE(j.thread_id, parent.thread_id)
                 LEFT JOIN threads st ON st.id = j.service_thread_id
                 JOIN projects p ON p.id = COALESCE(i.project_id, j.project_id)
                 LEFT JOIN executions e ON e.id = j.execution_id
                 WHERE j.id = ?1 LIMIT 1",
                session = SESSION_JOB_SHAPE
            ),
            params![job_id],
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };
    let key = row.text(0)?;
    let segment = row.opt_text(3)?;
    // A service's job: no issue, thread, or parent; its segment is the service
    // id. A thread's service names its owner in `service_thread_id`, and a
    // thread that is gone leaves it with no home rather than a project one.
    if row.opt_text(5)?.as_deref() == Some("service") {
        return Ok(match row.opt_text(8)? {
            None => segment.map(|service| cairn_uri::build_project_service_uri(&key, &service)),
            Some(_) => match (row.opt_text(9)?, segment) {
                (Some(thread), Some(service)) => {
                    Some(cairn_uri::build_thread_service_uri(&key, &thread, &service))
                }
                _ => None,
            },
        });
    }
    // Only a thread's session job is the thread address itself; everything else
    // under it hangs beneath. A task reaches its thread through its parent, and
    // the pre-cutover jobs migration 0157 re-pointed carry the thread's id
    // directly — so "has a thread id" cannot be the test, or a task would
    // resolve to the very same home URI as the session it belongs to. This is
    // the same rule that decides session identity, spelled once.
    //
    // Resolving a task here is what gives it a home URI at all: without one the
    // run cannot start, and every task spawned from a thread died at
    // `session_start_failed` before its agent spoke.
    if let Some(name) = row.opt_text(6)? {
        let is_session = row.opt_i64(7)?.unwrap_or_default() != 0;
        return Ok(match (is_session, segment) {
            (true, _) => Some(cairn_uri::build_thread_uri(&key, &name)),
            (false, Some(segment)) => Some(cairn_uri::build_thread_task_uri(&key, &name, &segment)),
            (false, None) => None,
        });
    }
    let Some(number) = row.opt_i64(1)? else {
        return Ok(None);
    };
    let number = number as i32;
    let seq = row.i64(2)? as i32;
    let parent_segment = row.opt_text(4)?;
    let is_workflow = row.opt_text(5)?.as_deref() == Some("workflow");
    let parent_segment = (!is_workflow).then_some(parent_segment).flatten();
    Ok(segment.map(|seg| {
        cairn_uri::build_job_base_uri(&key, number, seq, &seg, parent_segment.as_deref())
    }))
}
