use super::*;

pub(crate) fn db_internal(message: impl Into<String>) -> DbError {
    DbError::internal(message.into())
}

fn session_from_row(row: &cairn_db::turso::Row) -> DbResult<Session> {
    Ok(Session {
        id: row.text(0)?,
        job_id: row.opt_text(1)?,
        chat_id: row.opt_text(2)?,
        backend: row.text(3)?,
        account_id: row.opt_text(4)?,
        status: row.text(5)?.parse().unwrap_or(SessionStatus::Open),
        parent_session_id: row.opt_text(6)?,
        replaced_by_id: row.opt_text(7)?,
        terminal_reason: row.opt_text(8)?,
        sequence: row.i64(9)? as i32,
        created_at: row.i64(10)?,
        closed_at: row.opt_i64(11)?,
        updated_at: row.i64(12)?,
        backend_id: row.opt_text(13)?,
    })
}

pub(crate) async fn load_job_conn(
    conn: &crate::storage::TrackedConnection,
    job_id: &str,
) -> DbResult<Option<DbJob>> {
    let sql = format!("SELECT {JOB_COLUMNS} FROM jobs WHERE id = ?1");
    let mut rows = conn.query(&sql, (job_id,)).await?;
    rows.next()
        .await?
        .map(|row| db_job_from_row(&row))
        .transpose()
}

pub(super) async fn load_job(
    db: Arc<LocalDb>,
    job_id: String,
    context: &'static str,
) -> Result<DbJob, String> {
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            load_job_conn(conn, &job_id)
                .await?
                .ok_or_else(|| DbError::Row(format!("job not found: {job_id}")))
        })
    })
    .await
    .map_err(|e| db_error(context, e))
}

pub(super) async fn load_job_context(
    db: Arc<LocalDb>,
    dbs: std::sync::Arc<crate::db::DbState>,
    job_id: String,
) -> Result<(DbJob, String, Option<String>, Option<PathBuf>), String> {
    let (job, project_id, issue_id) = db
        .read(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let job = load_job_conn(conn, &job_id)
                    .await?
                    .ok_or_else(|| DbError::Row(format!("job not found: {job_id}")))?;
                Ok((job.clone(), job.project_id.clone(), job.issue_id.clone()))
            })
        })
        .await
        .map_err(|e| db_error("Job not found", e))?;
    let project_path = load_project_path(dbs, project_id.clone()).await?;
    Ok((job, project_id, issue_id, project_path))
}

pub(super) async fn load_project_repo_and_key(
    dbs: std::sync::Arc<crate::db::DbState>,
    project_id: String,
) -> Result<(String, String), String> {
    let (repo_path, key) =
        crate::projects::crud::resolve_local_repo_path_and_key(&dbs, &project_id)
            .await
            .map_err(|e| db_error("Project not found", DbError::internal(e.to_string())))?;
    let repo_path = repo_path.ok_or_else(|| {
        "Clone this team project's repository before running executions".to_string()
    })?;
    Ok((repo_path, key))
}

pub(super) async fn load_project_path(
    dbs: std::sync::Arc<crate::db::DbState>,
    project_id: String,
) -> Result<Option<PathBuf>, String> {
    crate::projects::crud::resolve_local_repo_path_and_key(&dbs, &project_id)
        .await
        .map(|(path, _)| path.map(PathBuf::from))
        .map_err(|e| {
            db_error(
                "Failed to load project path",
                DbError::internal(e.to_string()),
            )
        })
}

pub(super) async fn load_execution_seq(
    db: Arc<LocalDb>,
    execution_id: String,
) -> Result<Option<i32>, String> {
    db.read(|conn| {
        let execution_id = execution_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT seq FROM executions WHERE id = ?1",
                    (execution_id.as_str(),),
                )
                .await?;
            rows.next()
                .await?
                .map(|row| row.opt_i64(0).map(|seq| seq.map(|value| value as i32)))
                .transpose()
                .map(|value| value.flatten())
        })
    })
    .await
    .map_err(|e| db_error("Failed to load execution sequence", e))
}

pub(super) async fn load_display_id(
    db: Arc<LocalDb>,
    project_id: String,
    issue_id: Option<String>,
) -> Result<String, String> {
    db.read(|conn| {
        let project_id = project_id.clone();
        let issue_id = issue_id.clone();
        Box::pin(async move {
            if let Some(issue_id) = issue_id {
                let mut rows = conn
                    .query(
                        "SELECT number FROM issues WHERE id = ?1",
                        (issue_id.as_str(),),
                    )
                    .await?;
                let number = rows
                    .next()
                    .await?
                    .map(|row| row.i64(0))
                    .transpose()?
                    .unwrap_or(0);
                return Ok(number.to_string());
            }

            let mut rows = conn
                .query(
                    "SELECT COUNT(*) FROM executions WHERE project_id = ?1 AND issue_id IS NULL",
                    (project_id.as_str(),),
                )
                .await?;
            let count = rows
                .next()
                .await?
                .map(|row| row.i64(0))
                .transpose()?
                .unwrap_or(0);
            Ok(format!("run{count}"))
        })
    })
    .await
    .map_err(|e| db_error("Failed to load display id", e))
}

pub(super) async fn count_existing_branched_jobs(
    db: Arc<LocalDb>,
    issue_id: Option<String>,
    execution_id: Option<String>,
) -> Result<i64, String> {
    db.read(|conn| {
        let issue_id = issue_id.clone();
        let execution_id = execution_id.clone();
        Box::pin(async move {
            let (sql, value) = if let Some(issue_id) = issue_id {
                (
                    "SELECT COUNT(*) FROM jobs WHERE issue_id = ?1 AND branch IS NOT NULL",
                    Some(issue_id),
                )
            } else if let Some(execution_id) = execution_id {
                (
                    "SELECT COUNT(*) FROM jobs WHERE execution_id = ?1 AND branch IS NOT NULL",
                    Some(execution_id),
                )
            } else {
                return Ok(0);
            };
            let value = value.expect("count value");
            let mut rows = conn.query(sql, (value.as_str(),)).await?;
            rows.next()
                .await?
                .map(|row| row.i64(0))
                .transpose()
                .map(|value| value.unwrap_or(0))
        })
    })
    .await
    .map_err(|e| db_error("Failed to count existing branched jobs", e))
}

/// Resolve the durable `pack_anchor` for a job when its branch coordinate is prepared.
///
/// A job based off the project default branch anchors directly on its own
/// `base_commit`: that commit is reachable from the default branch, hence
/// durable. A job whose base is an ephemeral integration branch inherits the
/// parent job's anchor instead — either an explicitly delegated child
/// `parent_job_id`) or a child-issue execution (its base branch is a parent
/// issue's live branch). Inheritance reads the immediate parent's already-stored
/// `pack_anchor`, falling back to the parent's `base_commit`; because every
/// ancestor resolved and stored its own anchor at its own creation, one hop
/// chases the chain to the durable root, so the rule is depth-proof for nested
/// coordinators. Unresolvable bases stay NULL (never guessed).
async fn resolve_pack_anchor_conn(
    conn: &crate::storage::TrackedConnection,
    job: &DbJob,
    base_commit: Option<&str>,
) -> DbResult<Option<String>> {
    if let Some(parent_job_id) = job.parent_job_id.as_deref() {
        return parent_pack_anchor_conn(conn, parent_job_id).await;
    }

    if let Some(issue_id) = job.issue_id.as_deref() {
        if let Some(parent_branch) =
            crate::issues::relations::resolve_parent_branch(conn, issue_id).await?
        {
            return parent_pack_anchor_by_branch_conn(conn, &job.project_id, &parent_branch).await;
        }
    }

    Ok(base_commit.map(str::to_string))
}

/// The inherited anchor for an explicit parent job: its `pack_anchor`, else its
/// `base_commit`.
async fn parent_pack_anchor_conn(
    conn: &crate::storage::TrackedConnection,
    parent_job_id: &str,
) -> DbResult<Option<String>> {
    let mut rows = conn
        .query(
            "SELECT pack_anchor, base_commit FROM jobs WHERE id = ?1 LIMIT 1",
            (parent_job_id,),
        )
        .await?;
    rows.next()
        .await?
        .map(|row| Ok::<_, DbError>(row.opt_text(0)?.or(row.opt_text(1)?)))
        .transpose()
        .map(Option::flatten)
}

/// The inherited anchor for a child-issue execution, found by the parent's
/// integration branch.
async fn parent_pack_anchor_by_branch_conn(
    conn: &crate::storage::TrackedConnection,
    project_id: &str,
    branch: &str,
) -> DbResult<Option<String>> {
    let mut rows = conn
        .query(
            "SELECT pack_anchor, base_commit FROM jobs
             WHERE project_id = ?1 AND branch = ?2
             ORDER BY created_at DESC
             LIMIT 1",
            params![project_id, branch],
        )
        .await?;
    rows.next()
        .await?
        .map(|row| Ok::<_, DbError>(row.opt_text(0)?.or(row.opt_text(1)?)))
        .transpose()
        .map(Option::flatten)
}

pub(super) async fn update_job_coordinate(
    db: Arc<LocalDb>,
    job_id: String,
    branch: Option<String>,
    base_commit: Option<String>,
    now: i32,
) -> Result<(), String> {
    db.write(|conn| {
        let job_id = job_id.clone();
        let branch = branch.clone();
        let base_commit = base_commit.clone();
        Box::pin(async move {
            let job = load_job_conn(conn, &job_id)
                .await?
                .ok_or_else(|| DbError::Row(format!("job not found: {job_id}")))?;
            let pack_anchor = resolve_pack_anchor_conn(conn, &job, base_commit.as_deref()).await?;
            conn.execute(
                "UPDATE jobs SET branch = ?1, base_commit = ?2,
                     pack_anchor = ?3, updated_at = ?4 WHERE id = ?5",
                params![
                    branch.as_deref(),
                    base_commit.as_deref(),
                    pack_anchor.as_deref(),
                    now,
                    job_id.as_str()
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|e| db_error("Failed to update job coordinate", e))
}

pub(super) async fn load_session_conn(
    conn: &crate::storage::TrackedConnection,
    session_id: &str,
) -> DbResult<Option<Session>> {
    let sql = format!("SELECT {SESSION_COLUMNS} FROM sessions WHERE id = ?1");
    let mut rows = conn.query(&sql, (session_id,)).await?;
    rows.next()
        .await?
        .map(|row| session_from_row(&row))
        .transpose()
}

pub(super) async fn load_session_optional(
    db: Arc<LocalDb>,
    session_id: String,
) -> Result<Option<Session>, String> {
    db.read(|conn| {
        let session_id = session_id.clone();
        Box::pin(async move { load_session_conn(conn, &session_id).await })
    })
    .await
    .map_err(|e| db_error("Failed to load session", e))
}

/// Latest event `created_at` for a session, or `None` when the session has no
/// events yet. Backs the cold-resume staleness gate (CAIRN-2534) — the single
/// notion of a session's last activity, reused rather than duplicated.
pub(super) async fn load_last_event_time_for_session(
    db: Arc<LocalDb>,
    session_id: String,
) -> Result<Option<i64>, String> {
    db.read(|conn| {
        let session_id = session_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT MAX(created_at) FROM events WHERE session_id = ?1",
                    params![session_id.as_str()],
                )
                .await?;
            match rows.next().await? {
                Some(row) => Ok(row.opt_i64(0)?),
                None => Ok(None),
            }
        })
    })
    .await
    .map_err(|e| db_error("Failed to load last event time", e))
}

/// When this session last made a provider sampling request, or `None` when it
/// never has. The only clock the cache-economics decision consults (CAIRN-4757).
///
/// Deliberately narrower than [`load_last_event_time_for_session`], which
/// answers a different question and should keep counting everything. A prompt
/// cache is refreshed by a request to the provider, not by anything Cairn does
/// locally, so the horizon must be measured from provider traffic alone.
///
/// The token columns are what make that measurable. They are written from the
/// provider's own usage report, so their presence *is* the evidence that a
/// request happened and touched the prefix. A tool result, a system event, an
/// attention notification, or a check verdict carries none of them and so cannot
/// extend the horizon. `parent_tool_use_id IS NULL` excludes sub-agent traffic,
/// which runs against its own prefix and refreshes nothing here — the same
/// filter the context-token reader applies for the same reason.
///
/// Unlike that reader this one keeps `result:*` events: a result row reports the
/// usage of a request that genuinely happened, and freshness asks when, not how
/// much.
///
/// The account is the launch account of the run that made the request. The
/// session pin is deliberately not consulted: a failover is exactly what moves
/// it, so it would name the account the request did not run on.
pub(super) async fn load_last_provider_request_for_session(
    db: Arc<LocalDb>,
    session_id: String,
) -> Result<Option<LastProviderRequest>, String> {
    db.read(|conn| {
        let session_id = session_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT e.created_at, r.account_id FROM events e
                       LEFT JOIN runs r ON r.id = e.run_id
                      WHERE e.session_id = ?1
                        AND e.parent_tool_use_id IS NULL
                        AND (e.input_tokens IS NOT NULL
                          OR e.output_tokens IS NOT NULL
                          OR e.cache_read_tokens IS NOT NULL
                          OR e.cache_create_tokens IS NOT NULL)
                      ORDER BY e.created_at DESC LIMIT 1",
                    params![session_id.as_str()],
                )
                .await?;
            match rows.next().await? {
                Some(row) => Ok(Some(LastProviderRequest {
                    at: row.i64(0)?,
                    account_id: row.opt_text(1)?,
                })),
                None => Ok(None),
            }
        })
    })
    .await
    .map_err(|e| db_error("Failed to load last provider request time", e))
}

/// The most recent request a session's own conversation made to its provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct LastProviderRequest {
    pub(super) at: i64,
    /// The account the request ran on, when its run recorded one.
    pub(super) account_id: Option<String>,
}

pub(super) async fn insert_session_conn(
    conn: &crate::storage::TrackedConnection,
    session_id: &str,
    job_id: Option<&str>,
    chat_id: Option<&str>,
    backend: &str,
    now: i32,
) -> DbResult<()> {
    conn.execute(
        "INSERT INTO sessions(
            id, job_id, chat_id, backend, status, parent_session_id, replaced_by_id,
            terminal_reason, sequence, created_at, closed_at, updated_at, backend_id
        )
        VALUES (?1, ?2, ?3, ?4, 'open', NULL, NULL, NULL, 1, ?5, NULL, ?5, NULL)",
        params![session_id, job_id, chat_id, backend, now],
    )
    .await?;
    Ok(())
}

async fn resolve_prepare_session_start_conn(
    conn: &crate::storage::TrackedConnection,
    job: &DbJob,
    session: &Session,
    job_backend: &str,
) -> crate::backends::SessionStart {
    if let Some(parent_session_id) = session.parent_session_id.as_deref() {
        match load_session_conn(conn, parent_session_id).await {
            Ok(Some(parent_session)) => {
                // A rotation successor starts on a seed of its own, except one
                // a reopen minted, which forks the conversation the resolution
                // closed so the node resumes with its context.
                let is_rotation_successor =
                    parent_session.replaced_by_id.as_deref() == Some(session.id.as_str());
                let forks_parent = !is_rotation_successor
                    || crate::sessions::queries::continues_closed_work(&parent_session);
                if forks_parent && parent_session.backend == job_backend {
                    if let Some(source_backend_id) = parent_session.backend_id {
                        return crate::backends::SessionStart::Fork {
                            session_id: session.id.clone(),
                            source_backend_id,
                        };
                    }

                    log::warn!(
                        "Session {} was prepared to fork from {} but parent has no confirmed backend_id; starting fresh",
                        &session.id[..session.id.len().min(8)],
                        &parent_session_id[..parent_session_id.len().min(8)]
                    );
                }
            }
            Ok(None) => {
                log::warn!(
                    "Session {} references missing parent session {}; starting fresh",
                    &session.id[..session.id.len().min(8)],
                    &parent_session_id[..parent_session_id.len().min(8)]
                );
            }
            Err(error) => {
                log::warn!(
                    "Failed to load parent session {} for job {}: {}; starting fresh",
                    parent_session_id,
                    job.id,
                    error
                );
            }
        }
    }

    crate::backends::SessionStart::New {
        session_id: session.id.clone(),
    }
}

pub(super) async fn prepare_session(
    db: Arc<LocalDb>,
    job_id: String,
    job: DbJob,
    now: i32,
) -> Result<(String, crate::backends::SessionStart, String), String> {
    let seed_db = db.clone();
    let seed_job_id = job_id.clone();
    let prepared = db
        .write(|conn| {
            let job_id = job_id.clone();
            let job = job.clone();
            Box::pin(async move {
                let backend = job
                    .model
                    .as_deref()
                    .map(crate::backends::resolved_backend_for_model)
                    .unwrap_or(crate::backends::CLAUDE_FAMILY_BACKEND)
                    .to_string();
                let session_id = if let Some(sid) = job.current_session_id.as_deref() {
                    sid.to_string()
                } else {
                    let session_id = ids::mint_session_id().into_string();
                    insert_session_conn(
                        conn,
                        &session_id,
                        Some(job_id.as_str()),
                        None,
                        &backend,
                        now,
                    )
                    .await?;
                    conn.execute(
                        "UPDATE jobs SET current_session_id = ?1, updated_at = ?2 WHERE id = ?3",
                        params![session_id.as_str(), now, job_id.as_str()],
                    )
                    .await?;
                    session_id
                };

                let session = load_session_conn(conn, &session_id)
                    .await?
                    .ok_or_else(|| DbError::Row(format!("session not found: {session_id}")))?;
                let session_start =
                    resolve_prepare_session_start_conn(conn, &job, &session, &backend).await;
                let run_start_mode = run_start_mode(&session_start).to_string();
                Ok((session_id, session_start, run_start_mode))
            })
        })
        .await
        .map_err(|e| db_error("Failed to prepare session", e))?;
    crate::orchestrator::wakes::seed_default_job_subscriptions(&seed_db, &seed_job_id)
        .await
        .map_err(|e| db_error("Failed to seed default wake subscriptions", DbError::Row(e)))?;
    Ok(prepared)
}

#[cfg(test)]
pub(crate) async fn prepare_session_for_test(
    db: Arc<LocalDb>,
    job_id: &str,
    now: i32,
) -> Result<(String, crate::backends::SessionStart, String), String> {
    let job = load_job(
        db.clone(),
        job_id.to_string(),
        "load job for session preparation",
    )
    .await?;
    prepare_session(db, job_id.to_string(), job, now).await
}

/// The job's latest run while it is non-terminal. A warm process counts: its run
/// stays `live` between turns.
pub async fn live_run_for_job(db: &LocalDb, job_id: &str) -> Result<Option<String>, String> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, status FROM runs WHERE job_id = ?1 ORDER BY created_at DESC LIMIT 1",
                    (job_id.as_str(),),
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Ok(None);
            };
            let status = row
                .text(1)?
                .parse::<RunStatus>()
                .unwrap_or(RunStatus::Crashed);
            if status.is_terminal() {
                return Ok(None);
            }
            Ok(Some(row.text(0)?))
        })
    })
    .await
    .map_err(|e| e.to_string())
}

pub(super) struct RunInsert {
    pub(super) run_id: String,
    pub(super) issue_id: Option<String>,
    pub(super) project_id: Option<String>,
    pub(super) job_id: Option<String>,
    pub(super) status: String,
    pub(super) session_id: Option<String>,
    pub(super) started_at: Option<i32>,
    pub(super) created_at: i32,
    pub(super) updated_at: i32,
    pub(super) start_mode: Option<String>,
    pub(super) warn_existing_active: bool,
}

pub(super) async fn insert_run(db: Arc<LocalDb>, input: RunInsert) -> Result<i64, String> {
    db.write(|conn| {
        let input = RunInsert {
            run_id: input.run_id.clone(),
            issue_id: input.issue_id.clone(),
            project_id: input.project_id.clone(),
            job_id: input.job_id.clone(),
            status: input.status.clone(),
            session_id: input.session_id.clone(),
            started_at: input.started_at,
            created_at: input.created_at,
            updated_at: input.updated_at,
            start_mode: input.start_mode.clone(),
            warn_existing_active: input.warn_existing_active,
        };
        Box::pin(async move {
            let existing_active = if input.warn_existing_active {
                if let Some(job_id) = input.job_id.as_deref() {
                    let mut rows = conn
                        .query(
                            "SELECT COUNT(*) FROM runs
                             WHERE job_id = ?1 AND status IN ('starting', 'live')",
                            (job_id,),
                        )
                        .await?;
                    rows.next()
                        .await?
                        .map(|row| row.i64(0))
                        .transpose()?
                        .unwrap_or(0)
                } else {
                    0
                }
            } else {
                0
            };

            conn.execute(
                "INSERT INTO runs(
                    id, issue_id, project_id, job_id, chat_id, status, session_id,
                    error_message, started_at, exited_at, created_at, updated_at,
                    exit_reason, start_mode
                )
                VALUES (?1, ?2, ?3, ?4, NULL, ?5, ?6, NULL, ?7, NULL, ?8, ?9, NULL, ?10)",
                params![
                    input.run_id.as_str(),
                    input.issue_id.as_deref(),
                    input.project_id.as_deref(),
                    input.job_id.as_deref(),
                    input.status.as_str(),
                    input.session_id.as_deref(),
                    input.started_at,
                    input.created_at,
                    input.updated_at,
                    input.start_mode.as_deref(),
                ],
            )
            .await?;
            Ok(existing_active)
        })
    })
    .await
    .map_err(|e| db_error("Failed to create run", e))
}

pub(super) async fn load_run(
    db: Arc<LocalDb>,
    run_id: String,
    context: &'static str,
) -> Result<Run, String> {
    db.read(|conn| {
        let run_id = run_id.clone();
        Box::pin(async move {
            let sql = format!("SELECT {RUN_COLUMNS} FROM runs WHERE id = ?1");
            let mut rows = conn.query(&sql, (run_id.as_str(),)).await?;
            rows.next()
                .await?
                .map(|row| run_from_row(&row))
                .transpose()?
                .ok_or_else(|| DbError::Row(format!("run not found: {run_id}")))
        })
    })
    .await
    .map_err(|e| db_error(context, e))
}

pub(crate) struct ChildInsert {
    pub(crate) job_id: String,
    pub(crate) run_id: String,
    pub(crate) session_id: String,
    /// The delegating parent job, or `None` for a parent-less **top-level**
    /// node-less job — a standalone workflow launched from the UI (CAIRN-2651),
    /// which has no caller to resume. A top-level insert allocates its segment in
    /// the execution's own namespace and anchors packs on its own `base_commit`.
    pub(crate) parent_job_id: Option<String>,
    /// The child's durable branch coordinate. `None` means the child is
    /// repository-independent.
    pub(crate) branch: Option<String>,
    pub(crate) agent_config_id: String,
    pub(crate) project_id: String,
    pub(crate) issue_id: Option<String>,
    pub(crate) execution_id: Option<String>,
    pub(crate) description: String,
    pub(crate) model: Option<String>,
    /// Runner-owned logical head inherited or resolved at creation.
    pub(crate) base_commit: Option<String>,
    /// JSON-serialized `DelegatedOutputContract` (CAIRN-2481): the per-run output
    /// schema the artifact-write handler resolves for this node-less job. `None`
    /// preserves the `task_name -> return` fallback (child tasks).
    pub(crate) output_contract: Option<String>,
    /// Durable workflow tags persisted on the run (CAIRN-2481). No UI yet.
    pub(crate) label: Option<String>,
    pub(crate) phase: Option<String>,
    /// Links this child job to the originating `write` tool-use for transcript
    /// correlation; `None` for child tasks.
    pub(crate) parent_tool_use_id: Option<String>,
    pub(crate) task_index: Option<i32>,
    pub(crate) now: i32,
}

pub(crate) async fn insert_child_job_session_run(
    db: Arc<LocalDb>,
    input: ChildInsert,
) -> Result<(), String> {
    let seed_db = db.clone();
    let seed_job_id = input.job_id.clone();
    db.write(|conn| {
        let input = ChildInsert {
            job_id: input.job_id.clone(),
            run_id: input.run_id.clone(),
            session_id: input.session_id.clone(),
            parent_job_id: input.parent_job_id.clone(),
            branch: input.branch.clone(),
            agent_config_id: input.agent_config_id.clone(),
            project_id: input.project_id.clone(),
            issue_id: input.issue_id.clone(),
            execution_id: input.execution_id.clone(),
            description: input.description.clone(),
            model: input.model.clone(),
            base_commit: input.base_commit.clone(),
            output_contract: input.output_contract.clone(),
            label: input.label.clone(),
            phase: input.phase.clone(),
            parent_tool_use_id: input.parent_tool_use_id.clone(),
            task_index: input.task_index,
            now: input.now,
        };
        Box::pin(async move {
            let uri_segment_base = crate::node_segments::task_segment_base(
                None,
                Some(input.description.as_str()),
                Some(input.agent_config_id.as_str()),
            );
            // A delegated child dedupes its segment under its parent and follows
            // the parent's durable pack lineage. A parent-less top-level node-less
            // job (a standalone workflow launched
            // from the UI, CAIRN-2651) allocates its segment in the execution's own
            // top-level namespace and anchors packs on its own base commit.
            let (uri_segment, pack_anchor) = match input.parent_job_id.as_deref() {
                Some(parent_job_id) => (
                    crate::node_segments::allocate_child_task_segment(
                        conn,
                        parent_job_id,
                        &uri_segment_base,
                    )
                    .await?,
                    parent_pack_anchor_conn(conn, parent_job_id).await?,
                ),
                None => {
                    let issue_id = input.issue_id.as_deref().ok_or_else(|| {
                        DbError::internal("top-level child job requires an issue_id")
                    })?;
                    let execution_id = input.execution_id.as_deref().ok_or_else(|| {
                        DbError::internal("top-level child job requires an execution_id")
                    })?;
                    (
                        crate::node_segments::allocate_top_level_segment(
                            conn,
                            issue_id,
                            execution_id,
                            &uri_segment_base,
                        )
                        .await?,
                        input.base_commit.clone(),
                    )
                }
            };

            // A call spawned by a thread's session belongs to that thread, the
            // same way a delegated task does: `list_jobs_for_thread` is keyed on
            // this column alone, so without it the call is unlistable and
            // unopenable in the thread's pane.
            let thread_id = match input.parent_job_id.as_deref() {
                Some(parent) => crate::threads::inherited_thread_id_conn(conn, parent).await?,
                None => None,
            };

            conn.execute(
                "INSERT INTO jobs(
                    id, execution_id, recipe_node_id, parent_job_id,
                    branch, base_commit, current_session_id, resume_session_id,
                    status, agent_config_id, issue_id, project_id, task_description,
                    created_at, updated_at, completed_at, parent_tool_use_id, task_index,
                    started_at, model, node_name, base_branch, current_turn_id, uri_segment,
                    pack_anchor, output_contract, thread_id
                )
                VALUES (
                    ?1, ?2, NULL, ?3,
                    ?4, ?5, ?6, NULL,
                    'running', ?7, ?8, ?9, ?10,
                    ?11, ?11, NULL, ?12, ?13,
                    ?11, ?14, NULL, NULL, NULL, ?15,
                    ?16, ?17, ?18
                )",
                params![
                    input.job_id.as_str(),
                    input.execution_id.as_deref(),
                    input.parent_job_id.as_deref(),
                    input.branch.as_deref(),
                    input.base_commit.as_deref(),
                    input.session_id.as_str(),
                    input.agent_config_id.as_str(),
                    input.issue_id.as_deref(),
                    input.project_id.as_str(),
                    input.description.as_str(),
                    input.now,
                    input.parent_tool_use_id.as_deref(),
                    input.task_index,
                    input.model.as_deref(),
                    uri_segment.as_str(),
                    pack_anchor.as_deref(),
                    input.output_contract.as_deref(),
                    thread_id.as_deref(),
                ],
            )
            .await
            .map_err(|e| DbError::internal(format!("Failed to create child job: {e}")))?;

            // The child session records the backend its model resolves to, the
            // same derivation prepare_job uses for DAG jobs. Hardcoding "claude"
            // here made every Codex child-task reply see desired "codex" !=
            // stored "claude" and rotate to a fresh session, wiping history at
            // the system prompt (CAIRN-2598).
            let session_backend = input
                .model
                .as_deref()
                .map(crate::backends::resolved_backend_for_model)
                .unwrap_or(crate::backends::CLAUDE_FAMILY_BACKEND);
            insert_session_conn(
                conn,
                &input.session_id,
                Some(input.job_id.as_str()),
                None,
                session_backend,
                input.now,
            )
            .await?;

            conn.execute(
                "INSERT INTO runs(
                    id, issue_id, project_id, job_id, chat_id, status, session_id,
                    error_message, started_at, exited_at, created_at, updated_at,
                    exit_reason, start_mode, label, phase
                )
                VALUES (?1, ?2, ?3, ?4, NULL, 'starting', ?5, NULL, ?6, NULL, ?6, ?6, NULL, 'fresh', ?7, ?8)",
                params![
                    input.run_id.as_str(),
                    input.issue_id.as_deref(),
                    Some(input.project_id.as_str()),
                    Some(input.job_id.as_str()),
                    Some(input.session_id.as_str()),
                    input.now,
                    input.label.as_deref(),
                    input.phase.as_deref(),
                ],
            )
            .await
            .map_err(|e| DbError::internal(format!("Failed to create run: {e}")))?;
            Ok(())
        })
    })
    .await
    .map_err(|e| e.to_string())?;
    crate::orchestrator::wakes::seed_default_job_subscriptions(&seed_db, &seed_job_id).await?;
    Ok(())
}

/// Establish a node-less child's OWN agent in its execution's snapshot, then
/// insert the child's job, session, and run. Returns the execution the child
/// settled on.
///
/// A run's fence resolves through job → execution → snapshot → the job's agent
/// (`resolve_fence_policy`). A delegated task's agent joins the snapshot during
/// DAG expansion and every later turn passes the resume funnel's ensure, but a
/// node-less child (a cross-node task, an ephemeral call) is never expanded and
/// starts its first turn straight from here. Skipping this left that whole first
/// turn refusing every `run` batch with an unresolvable fence.
///
/// The snapshot is only ever ADDED to, never re-resolved: the agent entry is
/// shared with every sibling of the same agent in this execution, and a thread
/// child would otherwise be classed a tracking host. It is established BEFORE
/// any row exists, so a child whose agent cannot be snapshotted is refused with
/// nothing left behind. That ordering matters beyond tidiness: a packet-less
/// child's status has no reducer, so a child created and then failed here would
/// sit `running` forever.
pub(crate) async fn insert_child_with_agent_snapshot(
    orch: &Orchestrator,
    db: Arc<LocalDb>,
    input: ChildInsert,
    project_path: Option<&Path>,
) -> Result<Option<String>, String> {
    let Some(execution_id) = input.execution_id.clone() else {
        // No execution to add to: the host this creates is seeded with the
        // child's agent, and it needs the child's row to point at it.
        let job_id = input.job_id.clone();
        insert_child_job_session_run(db.clone(), input).await?;
        let job = load_job(db, job_id, "Child job not found").await?;
        return ensure_host_agent_snapshot(orch, &job, project_path, SnapshotFreshness::Frozen)
            .await;
    };
    ensure_execution_agent_snapshot(
        orch,
        &execution_id,
        &input.agent_config_id,
        input.model.as_deref(),
        project_path,
        SnapshotFreshness::Frozen,
    )
    .await
    .map_err(|error| {
        format!(
            "agent '{}' could not be added to the execution snapshot its fence resolves through: {error}",
            input.agent_config_id
        )
    })?;
    insert_child_job_session_run(db, input).await?;
    Ok(Some(execution_id))
}

#[cfg(test)]
mod project_path_tests {
    use super::*;
    use crate::storage::{migrated_test_db, SearchIndex};

    const TEAM_ID: &str = "teamABC123";
    const PROJECT_ID: &str = "teamABC123~00000000-0000-4000-8000-000000000001";

    async fn routed_team_project_state() -> Arc<crate::db::DbState> {
        let private = Arc::new(migrated_test_db("execution-team-path-private.db").await);
        private
            .execute(
                "INSERT INTO teams(id, name, sync_url, replica_path, created_at) VALUES (?1, 'Team', 'http://sync', '/tmp/team.db', 1)",
                (TEAM_ID,),
            )
            .await
            .unwrap();
        private
            .execute(
                "INSERT INTO project_routes(project_key, team_id, created_at) VALUES ('prj', ?1, 1)",
                (TEAM_ID,),
            )
            .await
            .unwrap();

        let team_db = Arc::new(migrated_test_db("execution-team-path-team.db").await);
        team_db
            .execute_script(format!(
                "
                INSERT INTO workspaces(id, name, created_at, updated_at) VALUES ('ws','w',1,1);
                INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
                 VALUES ('{}','ws','Project','prj','/creator/repo',1,1);
                ",
                PROJECT_ID
            ))
            .await
            .unwrap();

        let index =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        let dbs = Arc::new(crate::db::DbState::new(private, index));
        dbs.register_team_db_for_test(TEAM_ID.to_string(), team_db)
            .await;
        dbs.set_route("prj", Some(TEAM_ID.to_string())).await;
        dbs
    }

    #[tokio::test(flavor = "current_thread")]
    async fn load_project_repo_and_key_blocks_uncloned_team_projects() {
        let dbs = routed_team_project_state().await;

        let error = load_project_repo_and_key(dbs, PROJECT_ID.to_string())
            .await
            .unwrap_err();

        assert_eq!(
            error,
            "Clone this team project's repository before running executions"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn load_project_repo_and_key_uses_team_local_repo_path_when_cloned() {
        let dbs = routed_team_project_state().await;
        crate::projects::crud::set_local_repo_path(&dbs.local, "prj", Path::new("/member/repo"))
            .await
            .unwrap();

        let (repo_path, key) = load_project_repo_and_key(dbs, PROJECT_ID.to_string())
            .await
            .unwrap();

        assert_eq!(repo_path, "/member/repo");
        assert_eq!(key, "prj");
    }
}

#[cfg(test)]
mod prepare_session_tests {
    use super::*;
    use crate::storage::migrated_test_db;

    #[tokio::test(flavor = "current_thread")]
    async fn prepare_session_backfills_current_session_id_for_sessionless_job() {
        let db = Arc::new(migrated_test_db("prepare-session-backfill.db").await);
        db.execute_script(
            "
            INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('proj', 'default', 'Project', 'prj', '/repo', 1, 1);
            INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
             VALUES ('iss', 'proj', 1, 'Issue', 'active', 1, 1);
            INSERT INTO jobs (id, project_id, issue_id, status, created_at, updated_at)
             VALUES ('job', 'proj', 'iss', 'running', 1, 1);
            ",
        )
        .await
        .unwrap();

        let job = load_job(db.clone(), "job".to_string(), "load job")
            .await
            .unwrap();
        assert!(job.current_session_id.is_none());

        let (session_id, session_start, run_start_mode) =
            prepare_session(db.clone(), "job".to_string(), job, 2)
                .await
                .unwrap();

        assert_eq!(run_start_mode, "fresh");
        assert!(matches!(
            session_start,
            crate::backends::SessionStart::New { session_id: ref id } if id == &session_id
        ));

        let reloaded = load_job(db.clone(), "job".to_string(), "reload job")
            .await
            .unwrap();
        assert_eq!(
            reloaded.current_session_id.as_deref(),
            Some(session_id.as_str())
        );

        let stored_session = load_session_optional(db, session_id.clone())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored_session.job_id.as_deref(), Some("job"));
        assert_eq!(stored_session.id, session_id);
    }
}

#[cfg(test)]
mod pack_anchor_tests {
    use super::*;
    use crate::storage::migrated_test_db;

    // Realistic 40-hex shas; the resolution logic only compares strings, but
    // matching the real shape keeps the fixtures honest.
    const ROOT_SHA: &str = "1111111111111111111111111111111111111111";
    const P2_SHA: &str = "2222222222222222222222222222222222222222";
    const CHILD_SHA: &str = "3333333333333333333333333333333333333333";
    const DEFAULT_SHA: &str = "4444444444444444444444444444444444444444";

    async fn job_anchors(db: &LocalDb, job_id: &str) -> (Option<String>, Option<String>) {
        let job_id = job_id.to_string();
        db.read(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT base_commit, pack_anchor FROM jobs WHERE id = ?1",
                        (job_id.as_str(),),
                    )
                    .await?;
                let row = rows
                    .next()
                    .await?
                    .ok_or_else(|| DbError::Row(format!("job not found: {job_id}")))?;
                Ok((row.opt_text(0)?, row.opt_text(1)?))
            })
        })
        .await
        .unwrap()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn default_branch_job_anchors_on_its_own_base_commit() {
        let db = Arc::new(migrated_test_db("pack-anchor-default.db").await);
        db.execute_script(
            "
            INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
             VALUES ('proj', 'default', 'Project', 'prj', '/repo', 'main', 1, 1);
            INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
             VALUES ('iss', 'proj', 1, 'Top-level', 'active', 1, 1);
            INSERT INTO jobs (id, project_id, issue_id, status, base_branch, created_at, updated_at)
             VALUES ('job', 'proj', 'iss', 'pending', 'main', 1, 1);
            ",
        )
        .await
        .unwrap();

        update_job_coordinate(
            db.clone(),
            "job".to_string(),
            Some("agent/job".to_string()),
            Some(DEFAULT_SHA.to_string()),
            2,
        )
        .await
        .unwrap();

        let (base_commit, pack_anchor) = job_anchors(&db, "job").await;
        assert_eq!(base_commit.as_deref(), Some(DEFAULT_SHA));
        // A default-branch base is durable, so the anchor is the base_commit itself.
        assert_eq!(pack_anchor.as_deref(), Some(DEFAULT_SHA));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn child_issue_job_inherits_parent_pack_anchor() {
        let db = Arc::new(migrated_test_db("pack-anchor-child.db").await);
        db.execute_script(
            "
            INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
             VALUES ('proj', 'default', 'Project', 'prj', '/repo', 'main', 1, 1);
            INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
             VALUES ('parent', 'proj', 1, 'Parent', 'active', 1, 1);
            INSERT INTO issues (id, project_id, number, title, status, parent_issue_id, created_at, updated_at)
             VALUES ('child', 'proj', 2, 'Child', 'active', 'parent', 1, 1);
            INSERT INTO jobs (id, project_id, issue_id, status, branch, created_at, updated_at)
             VALUES ('job-parent', 'proj', 'parent', 'running', 'agent/parent-int', 1, 1);
            INSERT INTO jobs (id, project_id, issue_id, status, base_branch, created_at, updated_at)
             VALUES ('job-child', 'proj', 'child', 'pending', 'agent/parent-int', 1, 1);
            ",
        )
        .await
        .unwrap();
        db.execute(
            "UPDATE jobs SET base_commit = ?1, pack_anchor = ?1 WHERE id = 'job-parent'",
            params![ROOT_SHA],
        )
        .await
        .unwrap();

        update_job_coordinate(
            db.clone(),
            "job-child".to_string(),
            Some("agent/child-int".to_string()),
            Some(CHILD_SHA.to_string()),
            2,
        )
        .await
        .unwrap();

        let (base_commit, pack_anchor) = job_anchors(&db, "job-child").await;
        assert_eq!(base_commit.as_deref(), Some(CHILD_SHA));
        // The child's base is the parent's ephemeral integration branch, so the
        // anchor is the parent's durable anchor, not the child's own base_commit.
        assert_eq!(pack_anchor.as_deref(), Some(ROOT_SHA));
    }

    /// A thread's child anchors on its OWN base commit.
    ///
    /// The pack anchor asks the same question branch derivation does — is my base
    /// ephemeral, so must I inherit a durable anchor from my parent? — and reads
    /// the same resolver to answer it. A thread confers no branch, so its child is
    /// based on the project default, that base commit is reachable from the
    /// default branch and therefore already durable, and the child anchors on
    /// itself rather than borrowing the thread's anchor.
    #[tokio::test(flavor = "current_thread")]
    async fn thread_child_job_anchors_on_its_own_base_commit() {
        let db = Arc::new(migrated_test_db("pack-anchor-thread-child.db").await);
        db.execute_script(
            "
            INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
             VALUES ('proj', 'default', 'Project', 'prj', '/repo', 'main', 1, 1);
            INSERT INTO threads (id, project_id, name, status, attention, created_at, updated_at)
             VALUES ('thread', 'proj', 'thread', 'active', 'none', 1, 1);
            INSERT INTO issues (id, project_id, number, title, status, parent_thread_id, created_at, updated_at)
             VALUES ('child', 'proj', 2, 'Child', 'active', 'thread', 1, 1);
            INSERT INTO jobs (id, project_id, thread_id, status, branch, created_at, updated_at)
             VALUES ('job-thread', 'proj', 'thread', 'running', 'agent/thread-int', 1, 1);
            INSERT INTO jobs (id, project_id, issue_id, status, base_branch, created_at, updated_at)
             VALUES ('job-child', 'proj', 'child', 'pending', 'main', 1, 1);
            ",
        )
        .await
        .unwrap();
        db.execute(
            "UPDATE jobs SET base_commit = ?1, pack_anchor = ?1 WHERE id = 'job-thread'",
            params![ROOT_SHA],
        )
        .await
        .unwrap();

        update_job_coordinate(
            db.clone(),
            "job-child".to_string(),
            Some("agent/child-int".to_string()),
            Some(CHILD_SHA.to_string()),
            2,
        )
        .await
        .unwrap();

        let (base_commit, pack_anchor) = job_anchors(&db, "job-child").await;
        assert_eq!(base_commit.as_deref(), Some(CHILD_SHA));
        assert_eq!(
            pack_anchor.as_deref(),
            Some(CHILD_SHA),
            "a thread's child must not borrow the thread's anchor"
        );
    }

    /// A sub-agent task whose parent recorded no base still spawns.
    ///
    /// `create_child_task` and the ephemeral-call path both used to resolve the
    /// parent's `jobs.base_commit` and abort when it was NULL, which failed an
    /// agent's `write cairn:~/tasks` over a bookkeeping column that has no
    /// bearing on whether the task can run — a child places its own runs live,
    /// and a call owns no checkout at all. Both now carry the value as the
    /// `Option` it is, and this is the seam where that lands: the row is
    /// written with a NULL base, and the archival lineage still resolves from
    /// the parent's own anchor rather than from the missing base.
    #[tokio::test(flavor = "current_thread")]
    async fn a_child_task_spawns_when_its_parent_recorded_no_base_commit() {
        let db = Arc::new(migrated_test_db("child-insert-null-base.db").await);
        db.execute_script(
            "
            INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
             VALUES ('proj', 'default', 'Project', 'prj', '/repo', 'main', 1, 1);
            INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
             VALUES ('iss', 'proj', 1, 'Issue', 'active', 1, 1);
            INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
             VALUES ('exec', 'recipe', 'iss', 'proj', 'running', 1, 1);
            INSERT INTO jobs (id, project_id, issue_id, execution_id, status, branch, created_at, updated_at)
             VALUES ('job-parent', 'proj', 'iss', 'exec', 'running', 'agent/parent', 1, 1);
            ",
        )
        .await
        .unwrap();
        // The parent carries a durable anchor but never had its base recorded —
        // exactly the state of any job that does not run through `prepare_job`.
        db.execute(
            "UPDATE jobs SET base_commit = NULL, pack_anchor = ?1 WHERE id = 'job-parent'",
            params![ROOT_SHA],
        )
        .await
        .unwrap();

        insert_child_job_session_run(
            db.clone(),
            ChildInsert {
                job_id: "job-child".to_string(),
                run_id: "run-child".to_string(),
                session_id: "session-child".to_string(),
                parent_job_id: Some("job-parent".to_string()),
                branch: Some("agent/parent".to_string()),
                agent_config_id: "explore".to_string(),
                project_id: "proj".to_string(),
                issue_id: Some("iss".to_string()),
                execution_id: Some("exec".to_string()),
                description: "map the parser".to_string(),
                model: None,
                // The change under test: the parent had nothing to hand down.
                base_commit: None,
                output_contract: None,
                label: None,
                phase: None,
                parent_tool_use_id: None,
                task_index: None,
                now: 2,
            },
        )
        .await
        .expect("an unrecorded parent base is not a reason to refuse a task spawn");

        let (base_commit, pack_anchor) = job_anchors(&db, "job-child").await;
        assert_eq!(base_commit, None, "the child records what the parent had");
        assert_eq!(
            pack_anchor.as_deref(),
            Some(ROOT_SHA),
            "the archival lineage comes from the parent's anchor, not its base"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn child_of_a_child_inherits_durable_root_anchor() {
        let db = Arc::new(migrated_test_db("pack-anchor-nested.db").await);
        db.execute_script(
            "
            INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
             VALUES ('proj', 'default', 'Project', 'prj', '/repo', 'main', 1, 1);
            INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
             VALUES ('gp', 'proj', 1, 'Grandparent', 'active', 1, 1);
            INSERT INTO issues (id, project_id, number, title, status, parent_issue_id, created_at, updated_at)
             VALUES ('p2', 'proj', 2, 'Parent', 'active', 'gp', 1, 1);
            INSERT INTO issues (id, project_id, number, title, status, parent_issue_id, created_at, updated_at)
             VALUES ('c', 'proj', 3, 'Child', 'active', 'p2', 1, 1);
            INSERT INTO jobs (id, project_id, issue_id, status, branch, created_at, updated_at)
             VALUES ('job-gp', 'proj', 'gp', 'running', 'agent/gp-int', 1, 1);
            INSERT INTO jobs (id, project_id, issue_id, status, branch, base_branch, created_at, updated_at)
             VALUES ('job-p2', 'proj', 'p2', 'running', 'agent/p2-int', 'agent/gp-int', 1, 1);
            INSERT INTO jobs (id, project_id, issue_id, status, base_branch, created_at, updated_at)
             VALUES ('job-c', 'proj', 'c', 'pending', 'agent/p2-int', 1, 1);
            ",
        )
        .await
        .unwrap();
        // The grandparent is a default-branch coordinator: its anchor is its base.
        db.execute(
            "UPDATE jobs SET base_commit = ?1, pack_anchor = ?1 WHERE id = 'job-gp'",
            params![ROOT_SHA],
        )
        .await
        .unwrap();

        // Resolve the middle coordinator first: it inherits the grandparent's anchor.
        update_job_coordinate(
            db.clone(),
            "job-p2".to_string(),
            Some("agent/p2-int".to_string()),
            Some(P2_SHA.to_string()),
            2,
        )
        .await
        .unwrap();
        let (_, p2_anchor) = job_anchors(&db, "job-p2").await;
        assert_eq!(p2_anchor.as_deref(), Some(ROOT_SHA));

        // The grandchild inherits through the middle coordinator's stored anchor.
        update_job_coordinate(
            db.clone(),
            "job-c".to_string(),
            Some("agent/c-int".to_string()),
            Some(CHILD_SHA.to_string()),
            3,
        )
        .await
        .unwrap();
        let (base_commit, c_anchor) = job_anchors(&db, "job-c").await;
        assert_eq!(base_commit.as_deref(), Some(CHILD_SHA));
        assert_eq!(c_anchor.as_deref(), Some(ROOT_SHA));
    }
}

#[cfg(test)]
mod load_run_tests {
    use super::*;
    use crate::models::RunStartMode;
    use crate::storage::migrated_test_db;

    /// Regression for CAIRN-2230: `load_run` builds `SELECT {RUN_COLUMNS} FROM
    /// runs` from the canonical run projection. Migration 0081 dropped
    /// `runs.backend`, but a duplicate column list still named it, so the SELECT
    /// failed to parse against the real post-0081 schema ("no such column:
    /// backend") and surfaced on the resume/creation path as "Run not found
    /// after creation". This drives the real `load_run` against a fully-migrated
    /// db so the projection MUST parse, and pins the ordinals of the columns the
    /// drop shifted (`chat_id`, `exit_reason`, `start_mode`).
    #[tokio::test(flavor = "current_thread")]
    async fn load_run_parses_against_post_0081_schema() {
        let db = Arc::new(migrated_test_db("load-run-post-0081.db").await);
        db.execute_script(
            "
            INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('proj-1', 'default', 'Project', 'prj', '/repo', 1, 1);
            INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
             VALUES ('iss-1', 'proj-1', 1, 'Issue', 'active', 1, 1);
            INSERT INTO jobs (id, project_id, issue_id, status, created_at, updated_at)
             VALUES ('job-1', 'proj-1', 'iss-1', 'running', 1, 1);
            INSERT INTO runs (
                id, issue_id, project_id, job_id, status, session_id,
                error_message, started_at, exited_at, created_at, updated_at,
                chat_id, exit_reason, start_mode
            )
            VALUES (
                'run-1', 'iss-1', 'proj-1', 'job-1', 'live', 'sess-1',
                NULL, 10, NULL, 1, 2,
                'chat-1', 'completed', 'resume'
            );
            ",
        )
        .await
        .unwrap();

        let run = load_run(db, "run-1".to_string(), "load run")
            .await
            .expect("load_run must parse the post-0081 runs projection");

        assert_eq!(run.id, "run-1");
        assert_eq!(run.issue_id.as_deref(), Some("iss-1"));
        assert_eq!(run.project_id.as_deref(), Some("proj-1"));
        assert_eq!(run.job_id.as_deref(), Some("job-1"));
        assert_eq!(run.status, RunStatus::Live);
        assert_eq!(run.session_id.as_deref(), Some("sess-1"));
        assert_eq!(run.started_at, Some(10));
        assert_eq!(run.created_at, 1);
        assert_eq!(run.updated_at, 2);
        // Columns whose ordinals the dropped `backend` column would have shifted.
        assert_eq!(run.chat_id.as_deref(), Some("chat-1"));
        assert_eq!(run.exit_reason.as_deref(), Some("completed"));
        assert_eq!(run.start_mode, Some(RunStartMode::Resume));
    }
}

/// The fence a node-less child runs under, through both kinds of parent that can
/// spawn one. The builder case is the bug-sink specimens' shape (an Editor task
/// under a builder, an Investigate rescue under a builder); the thread case is a
/// call hung off a thread's synthetic host.
#[cfg(test)]
mod child_agent_snapshot_tests {
    use super::*;
    use crate::db::DbState;
    use crate::mcp::handlers::permission::fence_policy_for_run;
    use crate::models::{Fence, RecipeSnapshot, RecipeTrigger, TriggerContext, TriggerType};
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{migrated_test_db, SearchIndex};

    const BRANCH: &str = "agent/p-1-builder-0";

    /// `models: [lg]` resolves against the built-in pool defaults with no
    /// settings file; the fence declaration is the part under test.
    fn agent(name: &str, fence: &str) -> String {
        format!(
            "---\nname: {name}\ndescription: test agent\ntools: mcp__cairn__read, mcp__cairn__run\nmodels: [lg]\nfence: {fence}\n---\n\nDo the work.\n"
        )
    }

    struct Fixture {
        _temp: tempfile::TempDir,
        db: Arc<LocalDb>,
        orch: Orchestrator,
    }

    async fn fixture(name: &str) -> Fixture {
        let temp = tempfile::tempdir().unwrap();
        let config_dir = temp.path().join("config");
        let agents_dir = config_dir.join("agents");
        std::fs::create_dir_all(&agents_dir).unwrap();
        for (id, name, fence) in [
            ("build", "Build", "ask"),
            ("thread", "Thread", "allow"),
            ("rescuer", "Rescuer", "allow"),
            ("auditor", "Auditor", "deny"),
        ] {
            std::fs::write(agents_dir.join(format!("{id}.md")), agent(name, fence)).unwrap();
        }

        let db = Arc::new(migrated_test_db(name).await);
        let search = Arc::new(SearchIndex::open_or_create(temp.path().join("search")).unwrap());
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(db.clone(), search)),
            Arc::new(TestServicesBuilder::new().build()),
            config_dir,
        )
        .build();
        for sql in [
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('p', 'default', 'P', 'p', '/tmp/p', 1, 1)",
            "INSERT INTO threads (id, project_id, name, status, created_at, updated_at)
             VALUES ('t', 'p', 'general', 'active', 1, 1)",
        ] {
            db.execute(sql, ()).await.unwrap();
        }
        Fixture {
            _temp: temp,
            db,
            orch,
        }
    }

    /// A builder on its recipe execution `e`, whose snapshot carries exactly the
    /// builder's own agent and nothing a child would need.
    async fn seed_builder(fx: &Fixture) {
        let snapshot = ExecutionSnapshot::new(
            RecipeSnapshot {
                id: "r".into(),
                name: "R".into(),
                description: None,
                trigger: RecipeTrigger::Manual,
                nodes: vec![],
                edges: vec![],
            },
            HashMap::new(),
            HashMap::new(),
            TriggerContext {
                issue_id: Some("i".into()),
                project_id: "p".into(),
                trigger_type: TriggerType::Manual,
                event_payload: None,
                initiated_via: None,
            },
        )
        .to_json()
        .unwrap();
        fx.db
            .execute(
                "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
                 VALUES ('i', 'p', 1, 'T', 'active', 1, 1)",
                (),
            )
            .await
            .unwrap();
        fx.db
            .execute(
                "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot)
                 VALUES ('e', 'r', 'i', 'p', 'running', 1, 1, ?1)",
                params![snapshot.as_str()],
            )
            .await
            .unwrap();
        fx.db
            .execute(
                "INSERT INTO jobs (id, execution_id, agent_config_id, issue_id, project_id, node_name,
                    status, uri_segment, branch, base_commit, created_at, updated_at)
                 VALUES ('b', 'e', 'build', 'i', 'p', 'builder', 'running', 'builder', ?1, 'base', 1, 1)",
                params![BRANCH],
            )
            .await
            .unwrap();
        fx.db
            .execute(
                "INSERT INTO runs (id, project_id, issue_id, job_id, status, created_at, updated_at, start_mode)
                 VALUES ('b-run', 'p', 'i', 'b', 'live', 1, 1, 'resume')",
                (),
            )
            .await
            .unwrap();
        let builder = load_job(fx.db.clone(), "b".into(), "builder")
            .await
            .unwrap();
        ensure_host_agent_snapshot(&fx.orch, &builder, None, SnapshotFreshness::Frozen)
            .await
            .unwrap();
    }

    /// A thread session job with its synthetic host established, as delegation
    /// establishes it before hanging a child off it. Returns the host's id.
    async fn seed_thread(fx: &Fixture) -> String {
        for sql in [
            "INSERT INTO jobs (id, thread_id, project_id, status, agent_config_id, current_session_id,
                node_name, uri_segment, created_at, updated_at)
             VALUES ('ts', 't', 'p', 'idle', 'thread', 'ts-session', 'thread', 'thread', 1, 1)",
            "INSERT INTO sessions (id, job_id, backend, status, sequence, created_at, updated_at)
             VALUES ('ts-session', 'ts', 'claude', 'open', 1, 1, 1)",
            "INSERT INTO runs (id, project_id, job_id, status, created_at, updated_at, start_mode)
             VALUES ('ts-run', 'p', 'ts', 'live', 1, 1, 'resume')",
        ] {
            fx.db.execute(sql, ()).await.unwrap();
        }
        let thread = load_job(fx.db.clone(), "ts".into(), "thread")
            .await
            .unwrap();
        ensure_host_agent_snapshot(&fx.orch, &thread, None, SnapshotFreshness::for_job(&thread))
            .await
            .unwrap()
            .expect("a thread host")
    }

    fn child(
        job_id: &str,
        parent: &str,
        execution_id: Option<&str>,
        issue_id: Option<&str>,
        branch: Option<&str>,
        agent: &str,
    ) -> ChildInsert {
        ChildInsert {
            job_id: job_id.into(),
            run_id: format!("{job_id}-run"),
            session_id: format!("{job_id}-session"),
            parent_job_id: Some(parent.into()),
            branch: branch.map(Into::into),
            agent_config_id: agent.into(),
            project_id: "p".into(),
            issue_id: issue_id.map(Into::into),
            execution_id: execution_id.map(Into::into),
            description: format!("{agent} task"),
            model: None,
            base_commit: Some("base".into()),
            output_contract: None,
            label: None,
            phase: None,
            parent_tool_use_id: None,
            task_index: None,
            now: 1,
        }
    }

    #[tokio::test]
    async fn a_child_under_a_builder_resolves_its_own_agents_fence() {
        let fx = fixture("child-snapshot-builder.db").await;
        seed_builder(&fx).await;

        // What both spawn paths did before: the bare rows, with nothing putting
        // the child's agent where its fence resolves. The refusal names the link.
        insert_child_job_session_run(
            fx.db.clone(),
            child("bare", "b", Some("e"), Some("i"), Some(BRANCH), "rescuer"),
        )
        .await
        .unwrap();
        let reason = fence_policy_for_run(&fx.orch, "bare-run")
            .await
            .expect_err("no snapshot names the bare child's agent");
        assert!(
            reason.contains("'rescuer' is not in its execution snapshot"),
            "{reason}"
        );

        let settled = insert_child_with_agent_snapshot(
            &fx.orch,
            fx.db.clone(),
            child("c", "b", Some("e"), Some("i"), Some(BRANCH), "rescuer"),
            None,
        )
        .await
        .unwrap();

        assert_eq!(
            settled.as_deref(),
            Some("e"),
            "the child shares its builder's execution"
        );
        assert_eq!(
            fence_policy_for_run(&fx.orch, "c-run").await,
            Ok(Fence::Allow)
        );
        assert_eq!(
            fence_policy_for_run(&fx.orch, "b-run").await,
            Ok(Fence::Ask),
            "adding the child's agent must not disturb the builder's own entry"
        );
    }

    #[tokio::test]
    async fn a_child_under_a_thread_keeps_a_fence_narrower_than_the_threads() {
        let fx = fixture("child-snapshot-thread.db").await;
        let host = seed_thread(&fx).await;

        let settled = insert_child_with_agent_snapshot(
            &fx.orch,
            fx.db.clone(),
            child("c", "ts", Some(&host), None, None, "auditor"),
            None,
        )
        .await
        .unwrap();

        assert_eq!(
            settled.as_deref(),
            Some(host.as_str()),
            "the child shares its thread's host"
        );
        assert_eq!(
            fence_policy_for_run(&fx.orch, "c-run").await,
            Ok(Fence::Deny),
            "a child whose own agent is narrower than its thread's stays narrowed"
        );
        assert_eq!(
            fence_policy_for_run(&fx.orch, "ts-run").await,
            Ok(Fence::Allow)
        );
    }

    /// A child that would start into a refusal on every `run` batch is refused
    /// instead, before any row of it exists.
    #[tokio::test]
    async fn a_child_whose_agent_cannot_be_snapshotted_is_never_created() {
        let fx = fixture("child-snapshot-unresolvable.db").await;
        seed_builder(&fx).await;

        let error = insert_child_with_agent_snapshot(
            &fx.orch,
            fx.db.clone(),
            child("c", "b", Some("e"), Some("i"), Some(BRANCH), "ghost"),
            None,
        )
        .await
        .expect_err("an agent with no definition has nothing to snapshot");

        assert!(error.contains("'ghost'"), "{error}");
        for (sql, id) in [
            ("SELECT status FROM jobs WHERE id = ?1", "c"),
            ("SELECT status FROM runs WHERE id = ?1", "c-run"),
        ] {
            assert_eq!(
                fx.db.query_opt_text(sql, params![id]).await.unwrap(),
                None,
                "a refused child leaves no row behind"
            );
        }
    }
}
