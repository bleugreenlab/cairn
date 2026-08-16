//! Threads: durable, non-terminal sessions (CAIRN-3377).
//!
//! A thread orchestrates children and converses indefinitely instead of
//! completing an objective. Two consequences of that live here, and both are
//! keyed on the job's structural owner in [`job_owner`]:
//!
//! - Its session must stay cheap to run for weeks. [`compaction`] is the
//!   mechanism that makes that true: it replaces concluded history with a table
//!   of contents whose lines are re-readable at their URIs, and keeps the
//!   thread's authored arc and a recency window verbatim.
//! - It owns no branch, so it writes no tracked files.
//!   [`commit_refusal_for_job`] is the refusal both commit verbs take before
//!   they act on a `commit_msg`.

pub mod compaction;
pub mod crud;
pub mod definition;
pub use definition::{
    default_thread_definition, resolve_thread_definition, ThreadDefinition, ARC_ARTIFACT_NAME,
};

use crate::storage::LocalDb;

/// Resolve either canonical thread-name syntax or a migrated numeric alias.
///
/// Numeric issue addresses remain issue addresses while an issue row exists;
/// only a number vacated by the thread cutover can resolve through
/// `threads.migrated_from_number`.
pub(crate) async fn resolve_parent_thread_uri_conn(
    conn: &cairn_db::turso::Connection,
    uri: &str,
) -> crate::storage::DbResult<Option<(String, String, String)>> {
    use cairn_common::uri::{parse_uri, CairnResource};
    let (project, name, migrated_number) = match parse_uri(uri) {
        Some(CairnResource::Thread {
            project,
            name,
            path,
        }) if path.is_empty() => (project, Some(name), None),
        Some(CairnResource::Issue { project, number }) => (project, None, Some(number)),
        _ => return Ok(None),
    };
    let mut rows = conn
        .query(
            "SELECT t.id, t.project_id, t.name
             FROM threads t
             JOIN projects p ON p.id = t.project_id
             WHERE LOWER(p.key) = ?1
               AND ((?2 IS NOT NULL AND t.name = ?2)
                    OR (?3 IS NOT NULL
                        AND t.migrated_from_number = ?3
                        AND NOT EXISTS (
                            SELECT 1 FROM issues i
                            WHERE i.project_id = t.project_id AND i.number = ?3
                        )))
             LIMIT 1",
            cairn_db::turso::params![project, name.as_deref(), migrated_number],
        )
        .await?;
    match rows.next().await? {
        Some(row) => Ok(Some((
            row.get::<String>(0)?,
            row.get::<String>(1)?,
            row.get::<String>(2)?,
        ))),
        None => Ok(None),
    }
}

/// Return the live thread-owned job, creating its branchless job and session
/// directly when the thread has none.
///
/// Refuses a CLOSED thread: dormancy means no session is reused, recreated, or
/// re-subscribed until the thread is active again.
pub async fn ensure_thread_session(db: &LocalDb, thread_id: &str) -> Result<String, String> {
    let thread_id = thread_id.to_string();
    db.write(move |conn| {
        let thread_id = thread_id.clone();
        Box::pin(async move { ensure_thread_session_conn(conn, &thread_id, None).await })
    })
    .await
    .map_err(|error| error.to_string())
}

/// Why a closed thread refuses to establish a session, phrased once so callers
/// and tests key on one sentence.
pub(crate) fn closed_thread_refusal(thread_id: &str) -> String {
    format!("thread is closed: {thread_id}")
}

/// A thread's current lifecycle state, or `None` when no such thread exists.
///
/// The eligibility question every dormancy boundary asks, resolved from the one
/// column that answers it.
pub(crate) async fn thread_status_conn(
    conn: &cairn_db::turso::Connection,
    thread_id: &str,
) -> crate::storage::DbResult<Option<crate::models::ThreadStatus>> {
    let mut rows = conn
        .query(
            "SELECT status FROM threads WHERE id = ?1",
            cairn_db::turso::params![thread_id],
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };
    crate::models::ThreadStatus::parse(&row.get::<String>(0)?)
        .map(Some)
        .map_err(crate::storage::DbError::Row)
}

/// Whether a thread is accepting prompts and wakes. A thread that no longer
/// exists is not.
pub(crate) async fn thread_is_active(db: &LocalDb, thread_id: &str) -> Result<bool, String> {
    let thread_id = thread_id.to_string();
    db.read(move |conn| {
        let thread_id = thread_id.clone();
        Box::pin(async move { thread_status_conn(conn, &thread_id).await })
    })
    .await
    .map(|status| status.is_some_and(|status| status.is_active()))
    .map_err(|error| error.to_string())
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
pub(crate) async fn is_dormant_thread_session(db: &LocalDb, job_id: &str) -> bool {
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
                        cairn_db::turso::params![job_id],
                    )
                    .await?;
                Ok(rows.next().await?.is_some())
            })
        })
        .await;
    closed.unwrap_or(false)
}

/// The blocking form, for the synchronous turn-admission funnel.
pub(crate) fn is_dormant_thread_session_sync(db: &LocalDb, job_id: &str) -> bool {
    let job_id = job_id.to_string();
    crate::storage::run_db_blocking(move || async move {
        Ok(is_dormant_thread_session(db, &job_id).await)
    })
    .unwrap_or(false)
}

/// Why a closed thread will not start a turn. User-facing: this is what the
/// desktop composer surfaces when a send races a close.
pub(crate) fn dormant_thread_refusal() -> String {
    "This thread is closed, so it takes no new work. Reopen it to continue.".to_string()
}

/// The shape of a thread's session job, as a `jobs j` predicate.
///
/// A thread's session is the branchless job [`ensure_thread_session_conn`]
/// mints: it hangs off no parent and takes the reserved `thread` segment. Two
/// other kinds of job also carry a thread's id and neither is its session —
/// the sub-agent tasks the session spawns, which are ordinary child jobs, and
/// the pre-cutover thread-issue's jobs, which migration 0157 re-pointed at the
/// thread wholesale. Matching on `thread_id` alone therefore resolves to
/// whichever of them is newest, which is how a thread came to report a finished
/// sub-agent task as its live session.
pub(crate) const SESSION_JOB_SHAPE: &str = "j.parent_job_id IS NULL AND j.uri_segment = 'thread'";

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
pub(crate) async fn inherited_thread_id_conn(
    conn: &cairn_db::turso::Connection,
    parent_job_id: &str,
) -> crate::storage::DbResult<Option<String>> {
    let mut rows = conn
        .query(
            "SELECT thread_id FROM jobs WHERE id = ?1",
            cairn_db::turso::params![parent_job_id],
        )
        .await?;
    match rows.next().await? {
        Some(row) => row.get::<Option<String>>(0).map_err(Into::into),
        None => Ok(None),
    }
}

/// The thread ROW a project-scoped thread name addresses, or `None` when the
/// project holds no thread of that name.
///
/// The thread's id is its durable identity: sessions are replaced and compacted
/// beneath it and its name can change, while this row persists. Anything that
/// must outlive a session — a reading position, most of all — keys on this
/// rather than on the session job or on the name.
pub(crate) async fn thread_id_by_name_conn(
    conn: &cairn_db::turso::Connection,
    project_id: &str,
    name: &str,
) -> crate::storage::DbResult<Option<String>> {
    let mut rows = conn
        .query(
            "SELECT id FROM threads WHERE project_id = ?1 AND name = ?2 LIMIT 1",
            cairn_db::turso::params![project_id, name],
        )
        .await?;
    rows.next()
        .await?
        .map(|row| row.get::<String>(0))
        .transpose()
        .map_err(Into::into)
}

/// [`thread_id_by_name_conn`] for a caller holding a routed handle rather than a
/// connection, addressed by project KEY the way a URI spells it.
pub(crate) async fn thread_id_by_name(
    db: &crate::storage::LocalDb,
    project_key: &str,
    name: &str,
) -> crate::storage::DbResult<Option<String>> {
    db.query_text(
        "SELECT t.id FROM threads t
         JOIN projects p ON t.project_id = p.id
         WHERE p.key = ?1 AND t.name = ?2 LIMIT 1",
        (
            cairn_common::uri::canonical_project(project_key),
            name.to_string(),
        ),
    )
    .await
}

/// The one answer to "which job runs this thread's session", addressed the way
/// a URI addresses a thread.
pub(crate) async fn session_job_id_by_name_conn(
    conn: &cairn_db::turso::Connection,
    project_key: &str,
    thread_name: &str,
) -> crate::storage::DbResult<Option<String>> {
    let mut rows = conn
        .query(
            &format!(
                "SELECT j.id FROM jobs j
                 JOIN threads t ON j.thread_id = t.id
                 JOIN projects p ON t.project_id = p.id
                 WHERE p.key = ?1 AND t.name = ?2 AND {SESSION_JOB_SHAPE}
                 ORDER BY j.created_at DESC, j.rowid DESC LIMIT 1"
            ),
            cairn_db::turso::params![
                cairn_common::uri::canonical_project(project_key),
                thread_name
            ],
        )
        .await?;
    rows.next()
        .await?
        .map(|row| row.get::<String>(0))
        .transpose()
        .map_err(Into::into)
}

/// The output contract every thread session job carries: the `arc` preset, by
/// name. The name resolves through the shared preset registry, so the schema
/// the thread's arc validates against is the same one the artifact resource
/// advertises on read.
fn arc_output_contract() -> String {
    serde_json::to_string(&crate::models::DelegatedOutputContract {
        schema_type: crate::models::OutputSchema::Preset(ARC_ARTIFACT_NAME.to_string()),
        tool_name: None,
        description: None,
    })
    .expect("a preset output contract serializes")
}

/// The one answer to "which job is this thread's task", addressed the way a URI
/// addresses one: `cairn://p/{project}/{thread}/task/{segment}`.
///
/// The inverse of the thread-task arm of `home_uri_for_job_conn`, which names a
/// task by walking parent → thread; walking the same edge back is what keeps
/// address and resolution from drifting apart.
///
/// Deliberately narrower than the namer in one respect: the namer reaches its
/// thread through `COALESCE(j.thread_id, parent.thread_id)`, so a non-session job
/// carrying a thread id DIRECTLY — which every pre-cutover job migration 0157
/// re-pointed does — is also named `.../task/{segment}`, while this requires the
/// parent to be a session. Those jobs resolve as not-found here. That is no
/// regression (the coordinate refused them outright before) and it keeps one job
/// from being reachable by two routes, but it does mean "inverse" holds for jobs
/// the live delegation path creates, not for every job the namer can name.
///
/// Resolving through the parent rather than the thread's newest session lets a
/// task outlive the session that spawned it. That is only safe because
/// `allocate_child_task_segment` reserves a thread task's segment across every
/// session of the thread, so the `(thread, segment)` pair this keys on is unique
/// and the `LIMIT 1` below is belt-and-braces rather than a tiebreak that picks.
pub(crate) async fn task_job_id_by_name_conn(
    conn: &cairn_db::turso::Connection,
    project_key: &str,
    thread_name: &str,
    task_segment: &str,
) -> crate::storage::DbResult<Option<String>> {
    // `j` is bound to the SESSION here, not to the task, so `SESSION_JOB_SHAPE`
    // applies to it verbatim; the task is `child`.
    let mut rows = conn
        .query(
            &format!(
                "SELECT child.id FROM jobs child
                 JOIN jobs j ON j.id = child.parent_job_id
                 JOIN threads t ON j.thread_id = t.id
                 JOIN projects p ON t.project_id = p.id
                 WHERE p.key = ?1 AND t.name = ?2 AND child.uri_segment = ?3
                   AND {SESSION_JOB_SHAPE}
                 ORDER BY child.created_at DESC, child.rowid DESC LIMIT 1"
            ),
            cairn_db::turso::params![
                cairn_common::uri::canonical_project(project_key),
                thread_name,
                task_segment
            ],
        )
        .await?;
    rows.next()
        .await?
        .map(|row| row.get::<String>(0))
        .transpose()
        .map_err(Into::into)
}

/// Conn-level session establishment, composable into a caller's transaction so
/// a thread row and its session commit or roll back together. Thread creation
/// composes this after its INSERT: a durable named thread with no job would be
/// deaf and, because the name is already taken, unretriable. A metadata or
/// definition edit composes it after its UPDATE, so the session and the derived
/// trigger index it rebuilds are established from what was just written — no
/// successful definition write can leave a trigger-carrying thread deaf.
///
/// A CLOSED thread is refused an ESTABLISHMENT here, and only that. Resolving a
/// session that already exists and is live still succeeds, because closing a
/// thread does not cancel a turn already running — and that turn writes its own
/// todos, wakes, questions, and artifacts through this function. Refusing those
/// would present dormancy to the running agent as breakage. What dormancy
/// actually withholds is a NEW turn, which is refused at the one funnel that
/// admits resumes (`continue_job_impl_with_intent`), and a new prompt, which is
/// refused at `messages::delivery::append_thread_message`; both check the thread
/// directly rather than relying on this one.
pub(crate) async fn ensure_thread_session_conn(
    conn: &cairn_db::turso::Connection,
    thread_id: &str,
    model: Option<&crate::models::ModelSelection>,
) -> crate::storage::DbResult<String> {
    let mut rows = conn
        .query(
            "SELECT project_id, definition, status FROM threads WHERE id = ?1",
            cairn_db::turso::params![thread_id],
        )
        .await?;
    let row = rows.next().await?.ok_or_else(|| {
        crate::storage::DbError::Internal(format!("thread not found: {thread_id}"))
    })?;
    let project_id = row.get::<String>(0)?;
    let stored_definition = row.get::<Option<String>>(1)?;
    let status = crate::models::ThreadStatus::parse(&row.get::<String>(2)?)
        .map_err(crate::storage::DbError::Row)?;
    drop(rows);
    let definition = resolve_thread_definition(stored_definition.as_deref())
        .map_err(crate::storage::DbError::Row)?;

    let mut existing = conn
        .query(
            &format!(
                "SELECT j.id FROM jobs j
                 WHERE j.thread_id = ?1 AND {SESSION_JOB_SHAPE}
                   AND j.status NOT IN ('complete','failed','cancelled')
                 ORDER BY j.created_at DESC LIMIT 1"
            ),
            cairn_db::turso::params![thread_id],
        )
        .await?;
    let existing_job_id = existing
        .next()
        .await?
        .map(|row| row.get::<String>(0))
        .transpose()?;
    drop(existing);
    let job_id = if let Some(job_id) = existing_job_id {
        // A session job minted before the arc had a registered contract carries
        // none, so its arc writes are stored unvalidated and its `cairn:~/arc`
        // read has no fields to advertise. Backfilling here means every thread
        // — migrated or born after the cutover — answers to the same contract,
        // rather than the answer depending on when the thread was created.
        conn.execute(
            "UPDATE jobs SET output_contract = ?1 WHERE id = ?2 AND output_contract IS NULL",
            cairn_db::turso::params![arc_output_contract().as_str(), job_id.as_str()],
        )
        .await?;
        job_id
    } else if !status.is_active() {
        return Err(crate::storage::DbError::Internal(closed_thread_refusal(
            thread_id,
        )));
    } else {
        let output_contract = arc_output_contract();
        let job_id = cairn_common::ids::mint_child(thread_id);
        let session_id = cairn_common::ids::mint_session_id().into_string();
        let now = chrono::Utc::now().timestamp() as i32;
        conn.execute(
            "INSERT INTO jobs (
                id, thread_id, project_id, status, agent_config_id,
                current_session_id, output_contract, node_name, uri_segment,
                branch, execution_id, issue_id, model, created_at, updated_at
             ) VALUES (?1, ?2, ?3, 'idle', ?4, ?5, ?6, 'thread', 'thread',
                       NULL, NULL, NULL, ?7, ?8, ?8)",
            cairn_db::turso::params![
                job_id.as_str(),
                thread_id,
                project_id.as_str(),
                definition.agent.as_str(),
                session_id.as_str(),
                output_contract.as_str(),
                model.map(|selection| selection.model.as_str()),
                now,
            ],
        )
        .await?;
        conn.execute(
            "INSERT INTO sessions (
                id, job_id, backend, status, sequence, created_at, updated_at
             ) VALUES (?1, ?2, ?3, 'open', 1, ?4, ?4)",
            cairn_db::turso::params![
                session_id.as_str(),
                job_id.as_str(),
                model
                    .map(|selection| selection.backend.as_str())
                    .unwrap_or("claude"),
                now
            ],
        )
        .await?;
        job_id
    };
    crate::orchestrator::wakes::seed_default_job_subscriptions_conn(conn, &job_id).await?;
    crate::orchestrator::wakes::rebuild_derived_thread_subscriptions_conn(
        conn,
        &job_id,
        &definition.triggers,
    )
    .await?;
    Ok(job_id)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JobOwner {
    Thread,
    Issue,
    Unknown,
}

/// Resolve ownership from the job row itself. A durable job belongs to exactly
/// one execution or thread; `issue_id` is supporting issue context for the
/// execution-owned shape, not a second owner.
pub(crate) async fn job_owner(db: &LocalDb, job_id: &str) -> JobOwner {
    let job_id = job_id.to_string();
    let query_job_id = job_id.clone();
    let row = db
        .read(move |conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT execution_id, issue_id, thread_id FROM jobs WHERE id = ?1",
                        cairn_db::turso::params![query_job_id],
                    )
                    .await?;
                let Some(row) = rows.next().await? else {
                    return Ok(None);
                };
                Ok(Some((
                    row.get::<Option<String>>(0)?,
                    row.get::<Option<String>>(1)?,
                    row.get::<Option<String>>(2)?,
                )))
            })
        })
        .await;
    let Ok(Some((execution_id, issue_id, thread_id))) = row else {
        return JobOwner::Unknown;
    };
    classify_owner(
        execution_id.as_deref(),
        issue_id.as_deref(),
        thread_id.as_deref(),
    )
}

/// [`job_owner`] for a caller that already holds the row, so asking the same
/// question costs no second read. Both answer through [`classify_owner`]; there
/// is one notion of what a thread is.
pub(crate) fn owner_of_job(job: &crate::db_records::DbJob) -> JobOwner {
    classify_owner(
        job.execution_id.as_deref(),
        job.issue_id.as_deref(),
        job.thread_id.as_deref(),
    )
}

/// The ownership predicate itself.
///
/// `thread_id` is the owner; `execution_id` is machinery, and the two are not
/// mutually exclusive. A thread's session job acquires an execution the moment
/// it takes a turn or delegates — a passive host carrying its agent snapshot and
/// its delegated packets. Reading that as issue ownership silently demoted a
/// thread, taking its rolling compaction and its commit refusal with it.
fn classify_owner(
    execution_id: Option<&str>,
    issue_id: Option<&str>,
    thread_id: Option<&str>,
) -> JobOwner {
    let issue_owned = execution_id.is_some() || issue_id.is_some();
    match (thread_id.is_some(), issue_owned) {
        (true, _) => JobOwner::Thread,
        (false, true) => JobOwner::Issue,
        (false, false) => JobOwner::Unknown,
    }
}

/// Whether the rolling compaction path applies to a job's session.
///
/// Every compaction API takes this from its caller rather than deciding for
/// itself, so there is exactly one place in the codebase that answers "is this a
/// thread?" and no second ownership source can drift from `jobs.thread_id`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadCompaction {
    Enabled,
    Disabled,
}

impl ThreadCompaction {
    pub fn is_enabled(self) -> bool {
        self == ThreadCompaction::Enabled
    }
}

/// Resolve whether `job_id` runs a thread session from canonical job ownership.
///
/// Every compaction API takes the answer as a parameter instead of re-deriving
/// it, over the one kind lookup in [`identity_for_job`], so no second source can
/// drift from the column. Do not add a flag, an env override, or a per-agent
/// opt-in beside it: a capability with two sources is a capability that
/// disagrees with itself.
///
/// A job Cairn cannot resolve reads as `Disabled`. That is the safe direction:
/// an unresolvable job keeps the ordinary full-digest reseed, which is correct
/// for every issue and merely expensive for a thread.
pub(crate) async fn compaction_capability_for_job(db: &LocalDb, job_id: &str) -> ThreadCompaction {
    match job_owner(db, job_id).await {
        JobOwner::Thread => ThreadCompaction::Enabled,
        _ => ThreadCompaction::Disabled,
    }
}

/// Why a tool batch from `job_id` may not carry a `commit_msg`, or `None` when
/// it may.
///
/// A thread owns no branch. Its execution resolves to the `base` branch target,
/// which rewrites every agent node to [`crate::models::BranchMode::None`], and a
/// job without a branch of its own reads and writes its *base branch*. So a
/// `commit_msg` from a thread does not land on a reviewable side branch to be
/// squashed or abandoned later — it lands on the project's default branch, with
/// no pull request and no review surface. External state depends on this
/// refusal, which is why it is taken up front, before the batch runs, rather
/// than discovered after a tree has been dirtied.
///
/// `commit_msg` is the entire surface to close, on both commit verbs. A `write`
/// cannot touch a tracked file without one — `change_validation` rejects a
/// file-target change that lacks it, and `mode:"apply"` carries its own — and a
/// `run` that dirties the tree without one is already restored to HEAD by the
/// commit barrier. Refusing the field therefore refuses every tracked write, and
/// leaves reads, operational commands, and scratch dirt to the machinery that
/// already handles them.
///
/// A job that cannot be resolved to an issue is treated as ordinary. That is the
/// same safe direction [`compaction_capability_for_job`] takes, for the same
/// reason: an unresolvable job must keep the behaviour every issue has.
pub(crate) async fn commit_refusal_for_job(db: &LocalDb, job_id: &str) -> Option<String> {
    if job_owner(db, job_id).await != JobOwner::Thread {
        return None;
    }
    Some(
        "Refusing to commit from a thread-owned job. A thread owns no branch, so this batch is running ON the project's base branch — a commit_msg here lands on the default branch directly, with no pull request and no review surface. Threads orchestrate and converse; work that changes the repository belongs in a child issue, which owns a branch, ships its own PR, and merges to the base branch. Re-send this batch without commit_msg: reading, running commands, and scratch files are all yours, and anything the batch dirties is restored to HEAD for you."
            .to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn seed(name: &str) -> LocalDb {
        let db = crate::storage::migrated_test_db(name).await;
        db.execute_script(
            "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('p','default','P','prj','/tmp/p',1,1);
             INSERT INTO issues(id, project_id, number, title, status, attention, created_at, updated_at)
               VALUES ('i-issue','p',1,'An ordinary issue','active','none',1,1);
             INSERT INTO threads(id, project_id, name, status, attention, created_at, updated_at)
               VALUES ('t-thread','p','topic','active','none',1,1);
             INSERT INTO executions(id, issue_id, project_id, recipe_id, status, started_at, seq)
               VALUES ('e-issue','i-issue','p','build','running',1,1);
             INSERT INTO jobs(id, execution_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at)
               VALUES ('j-issue','e-issue','i-issue','p','running','builder','builder',1,1);
             INSERT INTO jobs(id, thread_id, project_id, status, uri_segment, node_name, created_at, updated_at)
               VALUES ('j-thread','t-thread','p','running','thread','thread',1,1);",
        )
        .await
        .unwrap();
        db
    }

    #[tokio::test]
    async fn a_thread_job_compacts_and_an_ordinary_issue_job_does_not() {
        let db = seed("thread-capability-by-owner.db").await;

        assert_eq!(
            compaction_capability_for_job(&db, "j-thread").await,
            ThreadCompaction::Enabled
        );
        assert_eq!(
            compaction_capability_for_job(&db, "j-issue").await,
            ThreadCompaction::Disabled
        );
    }

    /// A thread that has delegated once is still thread-owned.
    ///
    /// Spawning a task stamps the synthetic execution delegation books its
    /// packets in onto the thread's OWN session job. Reading that as issue
    /// ownership silently took the thread's rolling compaction and its
    /// commit-to-base refusal away at the moment it first delegated.
    #[tokio::test]
    async fn a_thread_that_has_delegated_is_still_thread_owned() {
        let db = seed("thread-owner-after-delegation.db").await;
        db.execute_script(
            "INSERT INTO executions(id, project_id, recipe_id, status, started_at, seq)
               VALUES ('e-delegated','p','delegation','running',1,1);
             UPDATE jobs SET execution_id = 'e-delegated' WHERE id = 'j-thread';",
        )
        .await
        .unwrap();

        assert_eq!(job_owner(&db, "j-thread").await, JobOwner::Thread);
        assert_eq!(
            compaction_capability_for_job(&db, "j-thread").await,
            ThreadCompaction::Enabled
        );
        assert!(
            commit_refusal_for_job(&db, "j-thread").await.is_some(),
            "a thread still owns no branch after delegating"
        );
    }

    /// A thread's session is its own job, never a task it spawned.
    ///
    /// Migration 0157 re-pointed the pre-cutover thread-issue's jobs at the
    /// thread, so delegated jobs carry a thread's id too — and they are newer
    /// than the session. Selecting the newest job by `thread_id` therefore
    /// reported a finished sub-agent task as the thread's live session, and
    /// would have handed a task the conversation meant for the thread.
    #[tokio::test]
    async fn a_task_carrying_the_thread_id_is_not_mistaken_for_its_session() {
        let db = seed("thread-session-identity.db").await;
        db.execute_script(
            "INSERT INTO jobs(id, thread_id, parent_job_id, project_id, status, uri_segment,
                              node_name, created_at, updated_at)
               VALUES ('j-task','t-thread','j-thread','p','running','survey-agent','Survey',9,9);",
        )
        .await
        .unwrap();

        let session = db
            .read(|conn| {
                Box::pin(async move { session_job_id_by_name_conn(conn, "prj", "topic").await })
            })
            .await
            .unwrap();
        assert_eq!(session.as_deref(), Some("j-thread"));
    }

    /// Closing withholds a NEW session; it does not revoke the one already
    /// running.
    ///
    /// Closing deliberately does not cancel a turn in flight, and that turn
    /// writes its own todos, wakes, questions, and artifacts by resolving its
    /// session through this function. Refusing a live session would present
    /// dormancy to the running agent as breakage — so the refusal is scoped to
    /// establishment, which is what dormancy is actually withholding.
    #[tokio::test]
    async fn a_closed_thread_keeps_a_live_session_but_establishes_no_new_one() {
        let db = seed("thread-closed-session.db").await;
        assert_eq!(
            ensure_thread_session(&db, "t-thread").await.unwrap(),
            "j-thread"
        );

        db.execute("UPDATE threads SET status='closed' WHERE id='t-thread'", ())
            .await
            .unwrap();
        assert_eq!(
            ensure_thread_session(&db, "t-thread").await.unwrap(),
            "j-thread",
            "a turn already running keeps resolving its own session"
        );

        // Once that session is gone, dormancy has something to withhold.
        db.execute("UPDATE jobs SET status='complete' WHERE id='j-thread'", ())
            .await
            .unwrap();
        let refused = ensure_thread_session(&db, "t-thread")
            .await
            .expect_err("a closed thread establishes nothing");
        assert!(
            refused.contains("thread is closed"),
            "the refusal names dormancy rather than reading as breakage: {refused}"
        );
        assert_eq!(
            db.query_opt_i64("SELECT COUNT(*) FROM jobs WHERE thread_id='t-thread'", ())
                .await
                .unwrap(),
            Some(1),
            "a refused establishment creates no session job"
        );

        // Reopening returns the thread to the ordinary path: its old session is
        // terminal, so a fresh one is minted exactly as it would be for any
        // thread with none.
        db.execute("UPDATE threads SET status='active' WHERE id='t-thread'", ())
            .await
            .unwrap();
        let recreated = ensure_thread_session(&db, "t-thread").await.unwrap();
        assert_ne!(recreated, "j-thread");
        assert_eq!(
            ensure_thread_session(&db, "t-thread").await.unwrap(),
            recreated,
            "and the reopened session is reused like any other"
        );
    }

    /// The dormancy gate keys on the SESSION job's shape, not on `thread_id`.
    /// A thread's sub-agent tasks carry its id too, and closing the thread is not
    /// a reason to stop delivering to work that is still running under it.
    #[tokio::test]
    async fn only_a_closed_threads_session_job_is_dormant() {
        let db = seed("thread-recipient-eligibility.db").await;
        db.execute_script(
            "INSERT INTO jobs(id, thread_id, parent_job_id, project_id, status, uri_segment,
                              node_name, created_at, updated_at)
               VALUES ('j-task','t-thread','j-thread','p','running','survey','Survey',9,9);",
        )
        .await
        .unwrap();

        assert!(!is_dormant_thread_session(&db, "j-thread").await);
        assert!(!is_dormant_thread_session(&db, "j-task").await);

        db.execute("UPDATE threads SET status='closed' WHERE id='t-thread'", ())
            .await
            .unwrap();
        assert!(is_dormant_thread_session(&db, "j-thread").await);
        assert!(
            !is_dormant_thread_session(&db, "j-task").await,
            "a task the thread spawned is an ordinary job and keeps its delivery"
        );
        assert!(
            !is_dormant_thread_session(&db, "j-issue").await,
            "an ordinary issue job is untouched"
        );
        assert!(
            !is_dormant_thread_session(&db, "no-such-job").await,
            "an unresolvable job keeps the behaviour every non-thread job has"
        );

        db.execute("UPDATE threads SET status='active' WHERE id='t-thread'", ())
            .await
            .unwrap();
        assert!(!is_dormant_thread_session(&db, "j-thread").await);
    }

    #[tokio::test]
    async fn an_unresolvable_job_keeps_the_ordinary_path() {
        let db = seed("thread-capability-unknown.db").await;

        assert_eq!(
            compaction_capability_for_job(&db, "no-such-job").await,
            ThreadCompaction::Disabled
        );
    }

    /// The tracked-writes half of the thread posture. A thread runs on the base
    /// branch, so its commit has nowhere to land but the project's default
    /// branch; an ordinary issue's job owns a branch and is untouched.
    #[tokio::test]
    async fn a_thread_job_is_refused_a_commit_msg_and_an_ordinary_issue_job_is_not() {
        let db = seed("thread-commit-refusal-by-owner.db").await;

        let refusal = commit_refusal_for_job(&db, "j-thread")
            .await
            .expect("a thread job must be refused a commit_msg");
        assert!(
            refusal.contains("thread-owned") && refusal.contains("owns no branch"),
            "the refusal states the posture, not just the rejection: {refusal}"
        );
        assert!(
            refusal.contains("child issue"),
            "the refusal points at where durable changes do belong: {refusal}"
        );

        assert_eq!(
            commit_refusal_for_job(&db, "j-issue").await,
            None,
            "every ordinary issue commits exactly as it did before"
        );
    }

    #[tokio::test]
    async fn a_job_that_does_not_resolve_to_a_thread_commits_as_an_ordinary_issue() {
        let db = seed("thread-commit-refusal-unresolvable.db").await;
        assert_eq!(commit_refusal_for_job(&db, "j-issue").await, None);
        assert_eq!(commit_refusal_for_job(&db, "no-such-job").await, None);
    }
}
