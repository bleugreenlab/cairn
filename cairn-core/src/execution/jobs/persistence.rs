use super::*;

pub(super) fn db_internal(message: impl Into<String>) -> DbError {
    DbError::internal(message.into())
}

pub(super) fn session_from_row(row: &cairn_db::turso::Row) -> DbResult<Session> {
    Ok(Session {
        id: row.text(0)?,
        job_id: row.opt_text(1)?,
        chat_id: row.opt_text(2)?,
        backend: row.text(3)?,
        status: row.text(4)?.parse().unwrap_or(SessionStatus::Open),
        parent_session_id: row.opt_text(5)?,
        replaced_by_id: row.opt_text(6)?,
        terminal_reason: row.opt_text(7)?,
        sequence: row.i64(8)? as i32,
        created_at: row.i64(9)?,
        closed_at: row.opt_i64(10)?,
        updated_at: row.i64(11)?,
        backend_id: row.opt_text(12)?,
    })
}

pub(super) async fn load_job_conn(
    conn: &cairn_db::turso::Connection,
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

/// Resolve a worktree's current HEAD as a full commit sha, used as a job's
/// `base_commit`. Failures (missing git, unborn/detached HEAD) leave it NULL
/// rather than guessing a value.
pub(super) fn worktree_head_commit(orch: &Orchestrator, worktree_path: &Path) -> Option<String> {
    // A jj workspace is `.jj`-only — no git HEAD to read — so resolve the base
    // jj-side from `@-` (the latest sealed commit, which is the base at job
    // creation). Same `None`-on-failure contract as the git path.
    if crate::jj::is_jj_dir(worktree_path) {
        let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
        return match crate::jj::head_commit(&jj, worktree_path) {
            Ok(sha) => {
                let sha = sha.trim().to_string();
                if sha.is_empty() {
                    log::warn!(
                        "Empty jj head commit for workspace {}; leaving base_commit NULL",
                        worktree_path.display()
                    );
                    None
                } else {
                    Some(sha)
                }
            }
            Err(error) => {
                log::warn!(
                    "Failed to resolve jj head commit for workspace {}: {}; leaving base_commit NULL",
                    worktree_path.display(),
                    error
                );
                None
            }
        };
    }
    match orch
        .services
        .git
        .rev_parse(worktree_path, vec!["HEAD".to_string()])
    {
        Ok(sha) => {
            let sha = sha.trim().to_string();
            if sha.is_empty() {
                log::warn!(
                    "Empty HEAD sha for worktree {}; leaving base_commit NULL",
                    worktree_path.display()
                );
                None
            } else {
                Some(sha)
            }
        }
        Err(error) => {
            log::warn!(
                "Failed to resolve HEAD sha for worktree {}: {}; leaving base_commit NULL",
                worktree_path.display(),
                error
            );
            None
        }
    }
}

/// Resolve the durable `pack_anchor` for a job at worktree-assignment time.
///
/// A job based off the project default branch anchors directly on its own
/// `base_commit`: that commit is reachable from the default branch, hence
/// durable. A job whose base is an ephemeral integration branch inherits the
/// parent job's anchor instead — either an inherited-worktree child (explicit
/// `parent_job_id`) or a child-issue execution (its base branch is a parent
/// issue's live branch). Inheritance reads the immediate parent's already-stored
/// `pack_anchor`, falling back to the parent's `base_commit`; because every
/// ancestor resolved and stored its own anchor at its own creation, one hop
/// chases the chain to the durable root, so the rule is depth-proof for nested
/// coordinators. Unresolvable bases stay NULL (never guessed).
pub(super) async fn resolve_pack_anchor_conn(
    conn: &cairn_db::turso::Connection,
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
    conn: &cairn_db::turso::Connection,
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
    conn: &cairn_db::turso::Connection,
    project_id: &str,
    branch: &str,
) -> DbResult<Option<String>> {
    let mut rows = conn
        .query(
            "SELECT pack_anchor, base_commit FROM jobs
             WHERE project_id = ?1 AND branch = ?2 AND worktree_path IS NOT NULL
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

pub(super) async fn update_job_worktree(
    db: Arc<LocalDb>,
    job_id: String,
    worktree_path: Option<String>,
    branch: Option<String>,
    base_commit: Option<String>,
    now: i32,
) -> Result<(), String> {
    db.write(|conn| {
        let job_id = job_id.clone();
        let worktree_path = worktree_path.clone();
        let branch = branch.clone();
        let base_commit = base_commit.clone();
        Box::pin(async move {
            let job = load_job_conn(conn, &job_id)
                .await?
                .ok_or_else(|| DbError::Row(format!("job not found: {job_id}")))?;
            let pack_anchor = resolve_pack_anchor_conn(conn, &job, base_commit.as_deref()).await?;
            conn.execute(
                "UPDATE jobs SET worktree_path = ?1, branch = ?2, base_commit = ?3,
                     pack_anchor = ?4, updated_at = ?5 WHERE id = ?6",
                params![
                    worktree_path.as_deref(),
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
    .map_err(|e| db_error("Failed to update job", e))
}

pub(super) async fn load_session_conn(
    conn: &cairn_db::turso::Connection,
    session_id: &str,
) -> DbResult<Option<Session>> {
    let sql = format!("SELECT {SESSION_COLUMNS} FROM sessions WHERE id = ?1");
    let mut rows = conn.query(&sql, (session_id,)).await?;
    rows.next()
        .await?
        .map(|row| session_from_row(&row))
        .transpose()
}

/// Read and clear the job's `needs_fresh_session` flag, returning whether it was
/// set. A prompt edit sets it; the continue path consumes it once, between
/// turns, to rotate the session so the edited system prompt takes effect.
pub(super) async fn take_needs_fresh_session(
    db: Arc<LocalDb>,
    job_id: String,
) -> Result<bool, String> {
    db.write(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT needs_fresh_session FROM jobs WHERE id = ?1",
                    (job_id.as_str(),),
                )
                .await?;
            let needs = rows
                .next()
                .await?
                .map(|row| row.i64(0))
                .transpose()?
                .unwrap_or(0)
                != 0;
            if needs {
                conn.execute(
                    "UPDATE jobs SET needs_fresh_session = 0 WHERE id = ?1",
                    (job_id.as_str(),),
                )
                .await?;
            }
            Ok(needs)
        })
    })
    .await
    .map_err(|e| db_error("Failed to take needs_fresh_session", e))
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

pub(super) async fn insert_session_conn(
    conn: &cairn_db::turso::Connection,
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

pub(super) async fn resolve_prepare_session_start_conn(
    conn: &cairn_db::turso::Connection,
    job: &DbJob,
    session: &Session,
    job_backend: &str,
) -> crate::backends::SessionStart {
    if let Some(parent_session_id) = session.parent_session_id.as_deref() {
        match load_session_conn(conn, parent_session_id).await {
            Ok(Some(parent_session)) => {
                let is_rotation_successor =
                    parent_session.replaced_by_id.as_deref() == Some(session.id.as_str());
                if !is_rotation_successor && parent_session.backend == job_backend {
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
                    .and_then(crate::backends::backend_for_model)
                    .unwrap_or("claude")
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
    pub(crate) parent_job_id: String,
    /// The inherited worktree path, or `None` for a worktree-less child (an
    /// ephemeral `worktree: none` call runs in a scratch dir with no project
    /// tree binding).
    pub(crate) worktree_path: Option<String>,
    /// The child's own branch. `None` for an inherited-worktree child (it shares
    /// the parent's branch); `Some(ephemeral)` for an ambient parent's ephemeral
    /// worktree, so teardown can forget the jj workspace and delete the branch.
    pub(crate) branch: Option<String>,
    pub(crate) agent_config_id: String,
    pub(crate) project_id: String,
    pub(crate) issue_id: Option<String>,
    pub(crate) execution_id: Option<String>,
    pub(crate) description: String,
    pub(crate) model: Option<String>,
    /// HEAD sha of the inherited parent worktree at creation.
    pub(crate) base_commit: Option<String>,
    /// 1 when this child owns a throwaway ephemeral worktree (ambient parent);
    /// reclaimed when the child job terminalizes.
    pub(crate) owns_ephemeral_worktree: bool,
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
            worktree_path: input.worktree_path.clone(),
            branch: input.branch.clone(),
            agent_config_id: input.agent_config_id.clone(),
            project_id: input.project_id.clone(),
            issue_id: input.issue_id.clone(),
            execution_id: input.execution_id.clone(),
            description: input.description.clone(),
            model: input.model.clone(),
            base_commit: input.base_commit.clone(),
            owns_ephemeral_worktree: input.owns_ephemeral_worktree,
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
            let uri_segment = crate::node_segments::allocate_child_task_segment(
                conn,
                &input.parent_job_id,
                &uri_segment_base,
            )
            .await?;

            // Inherited-worktree child: it shares the parent's worktree, so its
            // pack_anchor follows the parent's lineage rather than its own base.
            let pack_anchor = parent_pack_anchor_conn(conn, &input.parent_job_id).await?;

            conn.execute(
                "INSERT INTO jobs(
                    id, execution_id, recipe_node_id, parent_job_id,
                    worktree_path, branch, base_commit, current_session_id, resume_session_id,
                    status, agent_config_id, issue_id, project_id, task_description,
                    created_at, updated_at, completed_at, parent_tool_use_id, task_index,
                    started_at, model, node_name, base_branch, current_turn_id, uri_segment,
                    pack_anchor, owns_ephemeral_worktree, output_contract
                )
                VALUES (
                    ?1, ?2, NULL, ?3,
                    ?4, ?19, ?13, ?5, NULL,
                    'running', ?6, ?7, ?8, ?9,
                    ?10, ?10, NULL, ?15, ?16,
                    ?10, ?11, NULL, NULL, NULL, ?12,
                    ?14, ?18, ?17
                )",
                params![
                    input.job_id.as_str(),
                    input.execution_id.as_deref(),
                    input.parent_job_id.as_str(),
                    input.worktree_path.as_deref(),
                    input.session_id.as_str(),
                    input.agent_config_id.as_str(),
                    input.issue_id.as_deref(),
                    input.project_id.as_str(),
                    input.description.as_str(),
                    input.now,
                    input.model.as_deref(),
                    uri_segment.as_str(),
                    input.base_commit.as_deref(),
                    pack_anchor.as_deref(),
                    input.parent_tool_use_id.as_deref(),
                    input.task_index,
                    input.output_contract.as_deref(),
                    input.owns_ephemeral_worktree as i64,
                    input.branch.as_deref(),
                ],
            )
            .await
            .map_err(|e| DbError::internal(format!("Failed to create child job: {e}")))?;

            insert_session_conn(
                conn,
                &input.session_id,
                Some(input.job_id.as_str()),
                None,
                "claude",
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
                "INSERT INTO project_routes(project_key, team_id, created_at) VALUES ('PRJ', ?1, 1)",
                (TEAM_ID,),
            )
            .await
            .unwrap();

        let team_db = Arc::new(migrated_test_db("execution-team-path-team.db").await);
        team_db
            .execute_script(&format!(
                "
                INSERT INTO workspaces(id, name, created_at, updated_at) VALUES ('ws','w',1,1);
                INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
                 VALUES ('{}','ws','Project','PRJ','/creator/repo',1,1);
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
        dbs.set_route("PRJ", Some(TEAM_ID.to_string())).await;
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
        crate::projects::crud::set_local_repo_path(&dbs.local, "PRJ", Path::new("/member/repo"))
            .await
            .unwrap();

        let (repo_path, key) = load_project_repo_and_key(dbs, PROJECT_ID.to_string())
            .await
            .unwrap();

        assert_eq!(repo_path, "/member/repo");
        assert_eq!(key, "PRJ");
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
             VALUES ('proj', 'default', 'Project', 'PRJ', '/repo', 1, 1);
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
             VALUES ('proj', 'default', 'Project', 'PRJ', '/repo', 'main', 1, 1);
            INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
             VALUES ('iss', 'proj', 1, 'Top-level', 'active', 1, 1);
            INSERT INTO jobs (id, project_id, issue_id, status, base_branch, created_at, updated_at)
             VALUES ('job', 'proj', 'iss', 'pending', 'main', 1, 1);
            ",
        )
        .await
        .unwrap();

        update_job_worktree(
            db.clone(),
            "job".to_string(),
            Some("/wt/job".to_string()),
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
             VALUES ('proj', 'default', 'Project', 'PRJ', '/repo', 'main', 1, 1);
            INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
             VALUES ('parent', 'proj', 1, 'Parent', 'active', 1, 1);
            INSERT INTO issues (id, project_id, number, title, status, parent_issue_id, created_at, updated_at)
             VALUES ('child', 'proj', 2, 'Child', 'active', 'parent', 1, 1);
            INSERT INTO jobs (id, project_id, issue_id, status, branch, worktree_path, created_at, updated_at)
             VALUES ('job-parent', 'proj', 'parent', 'running', 'agent/parent-int', '/wt/parent', 1, 1);
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

        update_job_worktree(
            db.clone(),
            "job-child".to_string(),
            Some("/wt/child".to_string()),
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

    #[tokio::test(flavor = "current_thread")]
    async fn child_of_a_child_inherits_durable_root_anchor() {
        let db = Arc::new(migrated_test_db("pack-anchor-nested.db").await);
        db.execute_script(
            "
            INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
             VALUES ('proj', 'default', 'Project', 'PRJ', '/repo', 'main', 1, 1);
            INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
             VALUES ('gp', 'proj', 1, 'Grandparent', 'active', 1, 1);
            INSERT INTO issues (id, project_id, number, title, status, parent_issue_id, created_at, updated_at)
             VALUES ('p2', 'proj', 2, 'Parent', 'active', 'gp', 1, 1);
            INSERT INTO issues (id, project_id, number, title, status, parent_issue_id, created_at, updated_at)
             VALUES ('c', 'proj', 3, 'Child', 'active', 'p2', 1, 1);
            INSERT INTO jobs (id, project_id, issue_id, status, branch, worktree_path, created_at, updated_at)
             VALUES ('job-gp', 'proj', 'gp', 'running', 'agent/gp-int', '/wt/gp', 1, 1);
            INSERT INTO jobs (id, project_id, issue_id, status, branch, worktree_path, base_branch, created_at, updated_at)
             VALUES ('job-p2', 'proj', 'p2', 'running', 'agent/p2-int', '/wt/p2', 'agent/gp-int', 1, 1);
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
        update_job_worktree(
            db.clone(),
            "job-p2".to_string(),
            Some("/wt/p2".to_string()),
            Some("agent/p2-int".to_string()),
            Some(P2_SHA.to_string()),
            2,
        )
        .await
        .unwrap();
        let (_, p2_anchor) = job_anchors(&db, "job-p2").await;
        assert_eq!(p2_anchor.as_deref(), Some(ROOT_SHA));

        // The grandchild inherits through the middle coordinator's stored anchor.
        update_job_worktree(
            db.clone(),
            "job-c".to_string(),
            Some("/wt/c".to_string()),
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
             VALUES ('proj-1', 'default', 'Project', 'PRJ', '/repo', 1, 1);
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
