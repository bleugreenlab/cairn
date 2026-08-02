use super::*;

use crate::threads::compaction::CompactionTrigger;
use crate::threads::ThreadCompaction;

// ============================================================================
// on_job_complete_impl
// ============================================================================

#[derive(Debug)]
enum PushJobCoordinate {
    Node {
        project: String,
        number: i32,
        exec_seq: i32,
        node: String,
    },
    Task {
        project: String,
        number: i32,
        exec_seq: i32,
        node: String,
        task: String,
    },
}

fn push_job_coordinate(content_ref: &str) -> Option<PushJobCoordinate> {
    use cairn_common::uri::CairnResource;
    match cairn_common::uri::parse_uri(content_ref)? {
        CairnResource::Node {
            project,
            number,
            exec_seq,
            node_id,
        }
        | CairnResource::NodeChat {
            project,
            number,
            exec_seq,
            node_id,
        }
        | CairnResource::NodeChatRaw {
            project,
            number,
            exec_seq,
            node_id,
        }
        | CairnResource::NodeArtifact {
            project,
            number,
            exec_seq,
            node_id,
            ..
        }
        | CairnResource::NodeDiff {
            project,
            number,
            exec_seq,
            node_id,
        }
        | CairnResource::NodeTasks {
            project,
            number,
            exec_seq,
            node_id,
        }
        | CairnResource::NodeCalls {
            project,
            number,
            exec_seq,
            node_id,
        }
        | CairnResource::NodeWakes {
            project,
            number,
            exec_seq,
            node_id,
        }
        | CairnResource::NodeChecks {
            project,
            number,
            exec_seq,
            node_id,
        }
        | CairnResource::NodeQuestions {
            project,
            number,
            exec_seq,
            node_id,
        }
        | CairnResource::NodeQuestion {
            project,
            number,
            exec_seq,
            node_id,
            ..
        }
        | CairnResource::NodePermissions {
            project,
            number,
            exec_seq,
            node_id,
        }
        | CairnResource::NodePermission {
            project,
            number,
            exec_seq,
            node_id,
            ..
        }
        | CairnResource::NodeMessages {
            project,
            number,
            exec_seq,
            node_id,
        }
        | CairnResource::NodeProgress {
            project,
            number,
            exec_seq,
            node_id,
        }
        | CairnResource::NodeMemories {
            project,
            number,
            exec_seq,
            node_id,
        }
        | CairnResource::NodeSymbols {
            project,
            number,
            exec_seq,
            node_id,
            ..
        } => Some(PushJobCoordinate::Node {
            project,
            number,
            exec_seq,
            node: node_id,
        }),
        CairnResource::Task {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
        }
        | CairnResource::TaskChat {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
        }
        | CairnResource::TaskChatRaw {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
        }
        | CairnResource::TaskArtifact {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
            ..
        }
        | CairnResource::TaskChecks {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
        }
        | CairnResource::TaskPermissions {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
        }
        | CairnResource::TaskPermission {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
            ..
        }
        | CairnResource::TaskMessages {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
        } => Some(PushJobCoordinate::Task {
            project,
            number,
            exec_seq,
            node: node_id,
            task: task_name,
        }),
        CairnResource::JobTodos {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
        } => Some(match task_name {
            Some(task) => PushJobCoordinate::Task {
                project,
                number,
                exec_seq,
                node: node_id,
                task,
            },
            None => PushJobCoordinate::Node {
                project,
                number,
                exec_seq,
                node: node_id,
            },
        }),
        _ => None,
    }
}

async fn push_job_coordinate_exists(
    db: &LocalDb,
    coordinate: PushJobCoordinate,
) -> Result<bool, String> {
    db.read(|conn| {
        Box::pin(async move {
            let exists = match coordinate {
                PushJobCoordinate::Node {
                    project,
                    number,
                    exec_seq,
                    node,
                } => {
                    let mut rows = conn
                        .query(
                            "SELECT EXISTS(
                           SELECT 1 FROM jobs j
                           JOIN projects p ON p.id = j.project_id
                           JOIN issues i ON i.id = j.issue_id
                           JOIN executions e ON e.id = j.execution_id
                           WHERE p.key = ?1 AND i.number = ?2 AND e.seq = ?3
                             AND j.uri_segment = ?4
                             AND (j.parent_job_id IS NULL OR j.agent_config_id = 'workflow')
                         )",
                            params![project, number as i64, exec_seq as i64, node],
                        )
                        .await?;
                    rows.next()
                        .await?
                        .is_some_and(|row| row.i64(0).unwrap_or(0) != 0)
                }
                PushJobCoordinate::Task {
                    project,
                    number,
                    exec_seq,
                    node,
                    task,
                } => {
                    let mut rows = conn
                        .query(
                            "SELECT EXISTS(
                           SELECT 1 FROM jobs j
                           JOIN jobs parent ON parent.id = j.parent_job_id
                           JOIN projects p ON p.id = j.project_id
                           JOIN issues i ON i.id = j.issue_id
                           JOIN executions e ON e.id = j.execution_id
                           WHERE p.key = ?1 AND i.number = ?2 AND e.seq = ?3
                             AND parent.uri_segment = ?4 AND j.uri_segment = ?5
                             AND j.agent_config_id != 'workflow'
                         )",
                            params![project, number as i64, exec_seq as i64, node, task],
                        )
                        .await?;
                    rows.next()
                        .await?
                        .is_some_and(|row| row.i64(0).unwrap_or(0) != 0)
                }
            };
            Ok(exists)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

async fn retain_resolvable_pushes(
    db: &LocalDb,
    recipient: &str,
    pushes: Vec<crate::orchestrator::attention_push::Push>,
) -> Vec<crate::orchestrator::attention_push::Push> {
    let mut retained = Vec::with_capacity(pushes.len());
    for push in pushes {
        let Some(coordinate) = push_job_coordinate(&push.content_ref) else {
            retained.push(push);
            continue;
        };
        match push_job_coordinate_exists(db, coordinate).await {
            Ok(true) => retained.push(push),
            Ok(false) => {
                match crate::orchestrator::attention_push::delete_pending_by_id(db, &push.id).await {
                    Ok(()) => log::warn!(
                        "dropping unresolved attention push: recipient={} id={} key={} content_ref={}",
                        recipient,
                        push.id,
                        push.key,
                        push.content_ref
                    ),
                    Err(error) => {
                        log::warn!(
                            "failed to delete unresolved attention push {}; delivering fail-open: {}",
                            push.id,
                            error
                        );
                        retained.push(push);
                    }
                }
            }
            Err(error) => {
                log::warn!(
                    "attention push coordinate probe failed for {} ({}); delivering fail-open: {}",
                    push.id,
                    push.content_ref,
                    error
                );
                retained.push(push);
            }
        }
    }
    retained
}

/// Called when a job finishes. Advances the execution DAG if applicable.
///
/// Only advances for jobs that are part of a recipe DAG (have both `execution_id`
/// and `recipe_node_id`). Rows missing either field are not runnable DAG jobs and
/// are ignored here.
pub async fn on_job_complete_impl(orch: &Orchestrator, job_id: &str) -> Result<Vec<Job>, String> {
    // Tear down any external MCP gateway connections this job opened
    // (cairn://mcp/... family). Connections are pooled per job id, so closing
    // here releases the per-session server processes (e.g. Playwright browsers).
    if let Some(gateway) = orch.mcp_gateway() {
        gateway.close_session(job_id).await;
    }

    let job_id = job_id.to_string();
    let db = crate::execution::routing::owning_db_for_job(&orch.db, &job_id)
        .await
        .map_err(|e| e.to_string())?;
    let (execution_id, recipe_node_id): (Option<String>, Option<String>) = run_db(async move {
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT execution_id, recipe_node_id FROM jobs WHERE id = ?1",
                        (job_id.as_str(),),
                    )
                    .await?;
                let row = rows
                    .next()
                    .await?
                    .ok_or_else(|| DbError::Row("job not found".to_string()))?;
                Ok((row.opt_text(0)?, row.opt_text(1)?))
            })
        })
        .await
        .map_err(|e| db_error("Job not found", e))
    })?;

    match execution_id {
        Some(exec_id) if recipe_node_id.is_some() => {
            crate::execution::advancement::advance_execution_with_actions(orch, &exec_id).await
        }
        _ => Ok(vec![]), // Not a runnable DAG job, so there is no DAG to advance.
    }
}

// ============================================================================
// prepare_job
// ============================================================================

/// Where a job that inherits its parent's branch starts, resolved live-first and
/// never failing.
///
/// The store is the authority: the parent's bookmark is what the branch
/// currently means, and it is the only rung of this ladder that is a coordinate
/// rather than a memory of one.
///
/// Below it sits the parent's recorded `jobs.base_commit`. That row is
/// bookkeeping, not a coordinate (CAIRN-3224), and the standing operator ruling
/// forbids failing a spawn over substrate state — so it stays as a fallback, but
/// only when the store can still produce the commit it names. An unverified
/// recorded commit is the worse failure of the two: minting a job at a commit
/// the store cannot resolve does not fail here, it fails later inside
/// materialization, far from the row that caused it.
///
/// The floor is the job's own base branch, resolved live. `resolve_base_rev`
/// always yields something — down to jj's root commit on an unborn repository —
/// so a parent whose branch has vanished entirely degrades to a real coordinate
/// instead of refusing to start.
fn inherited_head<F>(
    jj: &crate::jj::JjEnv,
    store: &Path,
    branch: &str,
    recorded_base: Option<&str>,
    base_ref: &str,
    git_rev_parse: F,
) -> String
where
    F: Fn(&str) -> Option<String>,
{
    if let Some(live) = crate::jj::bookmark_commit(jj, store, branch) {
        return live;
    }
    if let Some(recorded) = recorded_base {
        if crate::jj::revset_resolves(jj, store, recorded) {
            log::warn!(
                "[prepare_job] inherited branch {branch} does not resolve in the store; starting \
                 from the parent's recorded base {recorded}, which the store can still produce"
            );
            return recorded.to_string();
        }
        log::warn!(
            "[prepare_job] inherited branch {branch} does not resolve in the store, and the \
             parent's recorded base {recorded} does not resolve either; falling back to \
             {base_ref}"
        );
    } else {
        log::warn!(
            "[prepare_job] inherited branch {branch} does not resolve in the store and the parent \
             has no recorded base; falling back to {base_ref}"
        );
    }
    crate::jj::resolve_base_rev(jj, store, base_ref, git_rev_parse)
}

/// The parent's durable coordinate, as an inheriting child needs to see it.
pub(crate) struct ParentCoordinate {
    /// The branch the child continues. Its absence is fatal to inheritance.
    pub branch: Option<String>,
    /// The parent's archival `jobs.base_commit`, the verified fallback rung of
    /// [`inherited_head`] — never the primary coordinate.
    pub recorded_base: Option<String>,
}

/// The job facts coordinate selection reads, without the rest of the row.
pub(crate) struct CoordinateRequest<'a> {
    pub job_id: &'a str,
    pub parent_job_id: Option<&'a str>,
    /// A branch already recorded on the row — pre-assigned lineage, honored
    /// instead of minting a fresh name.
    pub existing_branch: Option<&'a str>,
    /// The job's own base branch, the floor every non-inheriting job starts from.
    pub base_ref: &'a str,
}

/// Select the durable coordinate a job starts from, one outcome per branch mode.
///
/// `inherit` is the only mode that reads another job: the child continues its
/// parent's branch and starts at that branch's **live head**, not at the base
/// branch either of them was cut from.
///
/// Two grades of inheritance, and the difference is what happens when the store
/// cannot produce that head. Plain `inherit` degrades through
/// [`inherited_head`]'s ladder, which always yields a coordinate. With
/// `requires_parent_head` it refuses instead: nothing below the parent's live
/// bookmark is the parent's work, so a child seeded from a recorded base or from
/// the base branch is doing confidently wrong work rather than degraded work.
/// Delegated tasks take the strict grade (CAIRN-3309). Both refusals — no
/// lineage, and a branch the store cannot resolve — name the child, the parent,
/// and what is missing, so a broken edge is legible without opening this file.
///
/// `isolate` mints a child branch at the resolved base; `none` stays on the base
/// coordinate with no branch of its own. Both resolve base live, and neither
/// consults a parent.
///
/// The DB-shaped work is passed in: `load_parent` fetches the parent's
/// coordinate, and `mint_name` derives a fresh branch name (it is only called
/// when a mint actually needs one). That keeps the whole decision testable
/// against a real jj store without an orchestrator.
pub(crate) fn select_job_coordinate<P, M, F>(
    behavior: &crate::execution::step_behavior::StepBehavior,
    request: CoordinateRequest<'_>,
    jj: &crate::jj::JjEnv,
    store: &Path,
    load_parent: P,
    mint_name: M,
    git_rev_parse: F,
) -> Result<(Option<String>, Option<String>), String>
where
    P: FnOnce(&str) -> Result<ParentCoordinate, String>,
    M: FnOnce() -> Result<String, String>,
    F: Fn(&str) -> Option<String>,
{
    if behavior.inherits_branch {
        let job_id = request.job_id;
        let parent_job_id = request.parent_job_id.ok_or_else(|| {
            format!(
                "Job {job_id} inherits its parent's branch but has no parent_job_id: \
                 the delegation edge that created it never recorded lineage"
            )
        })?;
        let parent = load_parent(parent_job_id)?;
        let branch = parent.branch.ok_or_else(|| {
            format!(
                "Job {job_id} cannot start: its parent job {parent_job_id} has no branch, \
                 so there is no logical head to inherit"
            )
        })?;
        if behavior.requires_parent_head {
            let head = crate::jj::bookmark_commit(jj, store, &branch).ok_or_else(|| {
                format!(
                    "Job {job_id} cannot start: the branch {branch} it inherits from parent job \
                     {parent_job_id} does not resolve in the store, so its logical head cannot \
                     be seeded. Starting from the base branch would run this job against code \
                     its parent has already moved past."
                )
            })?;
            return Ok((Some(branch), Some(head)));
        }
        let head = inherited_head(
            jj,
            store,
            &branch,
            parent.recorded_base.as_deref(),
            request.base_ref,
            git_rev_parse,
        );
        return Ok((Some(branch), Some(head)));
    }

    let base = crate::jj::resolve_base_rev(jj, store, request.base_ref, git_rev_parse);
    if !behavior.mints_branch {
        return Ok((None, Some(base)));
    }

    let branch = match request.existing_branch {
        Some(branch) => branch.to_string(),
        None => mint_name()?,
    };
    match crate::jj::bookmark_commit(jj, store, &branch) {
        Some(existing) if existing != base => {
            return Err(format!(
                "Branch {branch} already exists at a different commit"
            ));
        }
        Some(_) => {}
        None => crate::jj::create_bookmark_at(jj, store, &branch, &base)?,
    }
    Ok((Some(branch), Some(base)))
}

/// Prepare a job for execution: resolve its durable branch coordinate, create the
/// run record and scratch residence, and store the initial user event. Returns a
/// [`PreparedJob`] with everything the host layer needs to
/// call `start_agent_session`.
///
/// The job status must already be set to `"running"` by the caller before this is
/// invoked (Tauri does this synchronously so the UI sees the change immediately).
pub fn prepare_job(orch: &Orchestrator, job_id: &str) -> Result<PreparedJob, String> {
    // The cold-start funnel inserts a run exactly the way the resume funnel does,
    // so it takes the same per-job launch lock (CAIRN-3283). Admission control
    // for a cold start is the durable compare-and-set claim on `jobs.status` in
    // the transport's `start_job_background`; this lock is what keeps that claim
    // from interleaving with a concurrent resume of the same job.
    let launch_lock = orch.job_launch_lock(job_id);
    let _launch_guard = launch_lock
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    log::info!("[prepare_job] resolving owner for job {job_id}");
    // Resolve the job's owning database ONCE (fail-closed) and thread it through
    // every run/session/turn/event write below: a team job's rows live wholly in
    // its synced replica, so prepare must read and write there, not the private DB.
    let owning_db = run_db({
        let dbs = orch.db.clone();
        let job_id = job_id.to_string();
        async move {
            crate::execution::routing::owning_db_for_job(&dbs, &job_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;

    // ---- Load job -------------------------------------------------------
    let job = run_db(load_job(
        owning_db.clone(),
        job_id.to_string(),
        "Job not found",
    ))?;

    // ---- Load project info ----------------------------------------------
    let (repo_path, project_key) = run_db(load_project_repo_and_key(
        orch.db.clone(),
        job.project_id.clone(),
    ))?;

    // ---- Execution seq (for job-activated event) ------------------------
    let exec_seq = match &job.execution_id {
        Some(exec_id) => run_db(load_execution_seq(owning_db.clone(), exec_id.clone()))?,
        None => None,
    };
    // ---- Display ID (issue number or sequential run counter) ------------
    let display_id = run_db(load_display_id(
        owning_db.clone(),
        job.project_id.clone(),
        job.issue_id.clone(),
    ))?;

    // ---- Determine node behavior ----------------------------------------
    let node_id = job
        .recipe_node_id
        .as_ref()
        .ok_or("Job has no recipe_node_id; standalone jobs are no longer runnable")?;
    let execution_id = job
        .execution_id
        .as_ref()
        .ok_or("Job has recipe node but no execution_id")?;

    let all_nodes = run_db(load_nodes_from_execution(
        owning_db.clone(),
        execution_id.clone(),
    ))?;
    let node_map: HashMap<&str, &DbRecipeNode> =
        all_nodes.iter().map(|n| (n.id.as_str(), n)).collect();

    let node = node_map
        .get(node_id.as_str())
        .ok_or_else(|| format!("Recipe node not found: {}", node_id))?;

    if node.node_type == "action" {
        return Err("Action nodes execute inline during DAG advancement".to_string());
    }
    if node.node_type == "checkpoint" {
        return Err("Checkpoint nodes wait for approval, not session start".to_string());
    }

    let behavior = resolve_node_behavior(node);
    let (mints_branch, inherits_branch, step_name): (bool, bool, String) = (
        behavior.mints_branch,
        behavior.inherits_branch,
        node.name.clone(),
    );

    log::info!(
        "[prepare_job] job {job_id}: behavior mints_branch={mints_branch} inherits_branch={inherits_branch} step={step_name}"
    );

    // ---- Virtual branch preparation -------------------------------------
    // Agent jobs own only durable store coordinates. No directory is allocated
    // here: the backend process resides in the job scratch directory and command
    // execution obtains disposable executor projections separately.
    let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(&repo_path));
    crate::jj::ensure_project_store(&jj, &store, Path::new(&repo_path))?;

    let base_ref = job.base_branch.as_deref().unwrap_or("HEAD");
    let git_rev_parse = |revision: &str| {
        orch.services
            .git
            .rev_parse(Path::new(&repo_path), vec![revision.to_string()])
            .ok()
            .filter(|sha| !sha.is_empty())
    };

    let (branch, base_commit) = select_job_coordinate(
        &behavior,
        CoordinateRequest {
            job_id,
            parent_job_id: job.parent_job_id.as_deref(),
            existing_branch: job.branch.as_deref(),
            base_ref,
        },
        &jj,
        &store,
        |parent_job_id| {
            let parent = run_db(load_job(
                owning_db.clone(),
                parent_job_id.to_string(),
                "Parent job not found",
            ))?;
            Ok(ParentCoordinate {
                branch: parent.branch,
                recorded_base: parent.base_commit,
            })
        },
        || {
            let seq = run_db(count_existing_branched_jobs(
                owning_db.clone(),
                job.issue_id.clone(),
                job.execution_id.clone(),
            ))?;
            let safe_step_name: String = step_name
                .to_lowercase()
                .chars()
                .map(|c| if c.is_alphanumeric() { c } else { '-' })
                .collect::<String>()
                .trim_matches('-')
                .to_string();
            Ok(format!(
                "agent/{project_key}-{display_id}-{safe_step_name}-{seq}"
            ))
        },
        git_rev_parse,
    )?;

    run_db(update_job_coordinate(
        owning_db.clone(),
        job_id.to_string(),
        branch,
        base_commit,
        chrono::Utc::now().timestamp() as i32,
    ))?;

    let _ = orch.services.emitter.emit(
        "job-activated",
        serde_json::json!({
            "jobId": job_id,
            "issueId": job.issue_id,
            "nodeName": job.node_name,
            "execSeq": exec_seq,
        }),
    );

    // ---- Reload job with its durable branch coordinate ------------------
    let job = run_db(load_job(
        owning_db.clone(),
        job_id.to_string(),
        "Job not found after branch preparation",
    ))?;

    // ---- Agent config ---------------------------------------------------
    let project_path = Some(PathBuf::from(repo_path.clone()));

    let agent_config = load_agent_config(orch, &job, project_path.as_deref())?;
    log::info!("[prepare_job] job {job_id}: loaded agent config");

    // ---- Create session + run record --------------------------------------
    let run_id = ids::mint_child(job_id);
    let now = chrono::Utc::now().timestamp() as i32;
    let status_str = RunStatus::Starting.to_string();

    // Ensure a Session record exists for this job and derive the first-start mode.
    let had_current_session = job.current_session_id.is_some();
    log::info!("[prepare_job] job {job_id}: preparing session");
    let (session_id, session_start, run_start_mode) = run_db(prepare_session(
        owning_db.clone(),
        job_id.to_string(),
        job.clone(),
        now,
    ))?;
    log::info!("[prepare_job] job {job_id}: session {session_id} prepared");
    if !had_current_session {
        // `prepare_session` backfills jobs.current_session_id for older/sessionless
        // jobs. The returned `start_job` value and earlier worktree invalidations
        // can both predate that write, so emit an explicit jobs change to refresh
        // chat chrome such as ContextUsage as soon as the session is known.
        let _ = orch.services.emitter.emit(
            "db-change",
            crate::notify::job_db_change_ids(
                "update",
                &job.id,
                job.issue_id.as_deref(),
                job.execution_id.as_deref(),
                job.parent_job_id.as_deref(),
                job.parent_tool_use_id.as_deref(),
                &job.project_id,
            ),
        );
    }

    log::info!("[prepare_job] job {job_id}: inserting run {run_id}");
    let existing_active_count = run_db(insert_run(
        owning_db.clone(),
        RunInsert {
            run_id: run_id.clone(),
            issue_id: job.issue_id.clone(),
            project_id: Some(job.project_id.clone()),
            job_id: Some(job_id.to_string()),
            status: status_str.clone(),
            session_id: Some(session_id.clone()),
            started_at: None,
            created_at: now,
            updated_at: now,
            start_mode: Some(run_start_mode.clone()),
            warn_existing_active: true,
        },
    ))?;

    // Admission passes from the launch lock to the claim here, while the lock is
    // still held: from now until the transport registers the spawned process,
    // this job has a run and a turn but no serving process — the exact shape a
    // resume reads as idle (CAIRN-3283).
    let launch_claim = orch.claim_job_launch(job_id, &run_id);

    log::info!("[prepare_job] job {job_id}: run {run_id} inserted");
    if existing_active_count > 0 {
        log::warn!(
            "[prepare_job] Job {} already has {} active runs",
            job_id,
            existing_active_count
        );
    }

    let _ = orch.services.emitter.emit(
        "db-change",
        crate::notify::run_db_change_ids(
            "insert",
            &run_id,
            Some(job_id),
            job.issue_id.as_deref(),
            Some(&job.project_id),
        ),
    );

    // ---- Create initial turn ------------------------------------------------
    let turn_id = ids::mint_child(job_id);
    create_initial_turn(orch, &turn_id, &session_id, job_id)?;

    // ---- Resolve inputs + build prompt ----------------------------------
    let (resolved_inputs, artifact_schema_info) =
        run_db(resolve_inputs_and_schema(owning_db.clone(), job.clone()))?;

    let prompt = format_resolved_inputs(&resolved_inputs);

    // Provision the process residence independently of repository coordinates.
    // Session startup resolves the canonical home URI and registers the readable
    // scratch name; this early ensure guarantees a writable residence exists as
    // soon as preparation completes.
    crate::scratch::ensure_job_scratch_dir(job_id, None);

    let job_model = job.model.as_ref().map(Model::new);

    // ---- Store the launch prompt ----------------------------------------
    // Under the namespaced `user:launch` type, not `user`: this prompt is
    // composed from the issue's resolved inputs, so a watching parent must never
    // be shown its own issue description as something a person said (CAIRN-3408).
    store_launch_event_with_turn(orch, &run_id, &session_id, &prompt, now, Some(&turn_id))?;

    // ---- Emit system message for job start ------------------------------
    crate::messages::system::emit_job_event(
        orch,
        job_id,
        Some(&run_id),
        crate::messages::system::JobEvent::Started,
    );

    Ok(PreparedJob {
        run_id,
        session_id,
        session_start,
        prompt,
        job_model,
        agent_config,
        artifact_schema_info,
        execution_id: job.execution_id,
        turn_id,
        launch_claim,
    })
}

// ============================================================================
// continue_job_or_enqueue
// ============================================================================

/// User-facing continue: enqueue instead of resuming when a turn is already
/// active, so a stale composer send never 500s and never drops the message.
///
/// The chat composer decides send-vs-queue from a cached head-turn read; when a
/// turn goes active just after that read was taken, a plain Enter still routes to
/// the `continue_job` command (this path) rather than the queue. Left unguarded,
/// that reaches [`continue_job_impl`] with a `running`/`pending` head turn, trips
/// the active-turn guard, and returns an error the composer surfaces as a 500 —
/// dropping the typed text (CAIRN-2657).
///
/// Mirroring the direct-message guard ([`crate::messages::delivery::head_turn_active`]),
/// a message-bearing send against an active turn lands in the queue
/// ([`crate::messages::queued::Delivery::Queue`], delivered at turn end) instead.
/// The existing turn-end / flush-on-idle machinery already claims and delivers
/// `queue` rows, so no new delivery code is needed. Guarding here — the
/// authoritative layer — protects every caller of the command, not just the
/// composer. Internal Rust callers (prompt/permission answers, wakes, delegation
/// resume, advancement) keep calling [`continue_job_impl`] directly: they run at
/// genuine turn boundaries where no turn is active and must not be silently
/// converted into queued messages.
const FORCED_DIGEST_RESUME_TRIGGER: &str = "Continue from the conversation digest above.";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ContinuationIntent {
    Normal,
    ForceDigestReseed,
}

pub fn resume_job_from_digest(
    orch: &Orchestrator,
    job_id: &str,
    identity_override: Option<crate::identity::UserIdentity>,
) -> Result<Run, String> {
    let owning_db = run_db({
        let dbs = orch.db.clone();
        let job_id = job_id.to_string();
        async move {
            crate::execution::routing::owning_db_for_job(&dbs, &job_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;
    if crate::messages::delivery::head_turn_active_sync(&owning_db, job_id) {
        return Err(
            "Resume from digest is available only after the current turn finishes.".to_string(),
        );
    }

    continue_job_impl_with_intent(
        orch,
        job_id,
        Some(FORCED_DIGEST_RESUME_TRIGGER),
        identity_override,
        Some(ResumeContext {
            suppress_user_event: true,
            suppress_self_suspend_note: true,
            supersede_pending_retry: true,
            ..Default::default()
        }),
        ContinuationIntent::ForceDigestReseed,
    )
}

pub fn continue_job_or_enqueue(
    orch: &Orchestrator,
    job_id: &str,
    message: Option<&str>,
    identity_override: Option<crate::identity::UserIdentity>,
) -> Result<Run, String> {
    // Only a message-bearing send can be queued; a bare continue has nothing to
    // enqueue, so it falls straight through to the resume (and its guard).
    if let Some(text) = message.filter(|m| !m.trim().is_empty()) {
        // Resolve the owning DB (team job -> its synced replica), mirroring
        // continue_job_impl's own routing so team turns/queued rows stay correct.
        let owning_db = run_db({
            let dbs = orch.db.clone();
            let job_id = job_id.to_string();
            async move {
                crate::execution::routing::owning_db_for_job(&dbs, &job_id)
                    .await
                    .map_err(|e| e.to_string())
            }
        })?;
        if crate::messages::delivery::head_turn_active_sync(&owning_db, job_id) {
            crate::messages::queued::enqueue(
                &owning_db,
                job_id,
                text,
                crate::messages::queued::Delivery::Queue,
            )?;
            // Make the pending-queue chip appear immediately: QueryProvider
            // invalidates `queuedMessages` on this db-change, the same mechanism
            // the composer's own enqueue path relies on.
            let _ = orch.services.emitter.emit(
                "db-change",
                serde_json::json!({"table": "queued_messages", "action": "update"}),
            );
            // Return the job's latest run to honor the Run return contract; the
            // frontend ignores the value. `list_runs_for_job` orders newest-first,
            // matching the frontend's `runs[0]` "latest" convention. Fall through to
            // the resume only in the pathological case where an active turn has no
            // run row at all — the message is already recorded, so the fall-through
            // must not record it a second time.
            if let Some(run) = crate::runs::queries::list_runs_for_job(owning_db.clone(), job_id)
                .map_err(|e| e.to_string())?
                .into_iter()
                .next()
            {
                return Ok(run);
            }
        } else if let Err(error) =
            crate::messages::queued::record_direct_delivery(&owning_db, job_id, text)
        {
            // An idle child takes the operator's text straight into the resume
            // below, so the queue never sees it. Recording it delivered-on-arrival
            // is what keeps the operator-message record complete: the watchers'
            // catch-up digest reads that record, and the `user` transcript event
            // this resume is about to store is indistinguishable from a launch
            // prompt or a machinery-delivered payload (CAIRN-3390). It also carries
            // the watchers' catch-up push, so an operator message reaches them
            // whichever branch it took (CAIRN-3342). Best-effort: bookkeeping that
            // fails must not cost the operator the resume itself.
            log::warn!("recording an operator message for job {job_id} failed: {error}");
        }
    }
    continue_job_impl(
        orch,
        job_id,
        message,
        identity_override,
        Some(ResumeContext {
            supersede_pending_retry: true,
            ..Default::default()
        }),
    )
}

// ============================================================================
// continue_job_impl
// ============================================================================

/// Context for resuming a job after a durable suspend on the slow (>45s) path.
///
/// Its presence tells [`continue_job_impl`] not to store the resume message as a
/// visible user event: the result is already rendered in place as the
/// originating `write` call's synthetic tool_result. The message is still
/// forwarded to the agent so the model sees it in context.
#[derive(Debug, Clone, Default)]
pub struct ResumeContext {
    /// When true, skip storing the resume message as a `user` transcript event.
    pub(crate) suppress_user_event: bool,
    /// Suppress the self-suspend framing note when the hidden trigger belongs to
    /// another typed lifecycle action, such as manual digest resume.
    pub(crate) suppress_self_suspend_note: bool,
    /// Consume this already-claimed pending retry turn instead of creating a
    /// follow-up. Used only by best-effort automatic backend retries.
    pub(crate) preclaimed_retry_turn_id: Option<String>,
    /// User-facing continuation may take over an unstarted automatic retry.
    /// Reclassifying that pending head as a follow-up resets the retry budget
    /// and makes the sleeping timer's retry-head check fail.
    pub(crate) supersede_pending_retry: bool,
    /// Reuse this pre-created, still-pending successor turn (an owned-wait
    /// resolution's `WaitResolved` turn) instead of allocating a follow-up. It is
    /// started on the resumed run here — exactly once. Distinct from
    /// `preclaimed_retry_turn_id`, which carries automatic-retry claim semantics.
    pub(crate) preclaimed_successor_turn_id: Option<String>,
}

/// Validate a pre-created owned-wait successor turn before `continue_job_impl`
/// reuses it: it must belong to this job and the resolved session and still be
/// pending (its start happens here). Rejecting a mismatch keeps a stale or
/// foreign turn from being driven as this run's successor (CAIRN-2970).
fn validate_preclaimed_successor(
    db: Arc<LocalDb>,
    job_id: &str,
    session_id: &str,
    turn_id: &str,
) -> Result<(), String> {
    let info = run_db({
        let db = db.clone();
        let turn_id = turn_id.to_string();
        async move {
            db.read(|conn| {
                let turn_id = turn_id.clone();
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT job_id, session_id, state FROM turns WHERE id = ?1 LIMIT 1",
                            (turn_id.as_str(),),
                        )
                        .await?;
                    match rows.next().await? {
                        Some(row) => Ok(Some((row.opt_text(0)?, row.text(1)?, row.text(2)?))),
                        None => Ok(None),
                    }
                })
            })
            .await
            .map_err(|e| e.to_string())
        }
    })?;
    let Some((turn_job, turn_session, state)) = info else {
        return Err(format!("preclaimed successor turn {turn_id} not found"));
    };
    if turn_job.as_deref() != Some(job_id) {
        return Err(format!(
            "preclaimed successor turn {turn_id} belongs to a different job"
        ));
    }
    if turn_session != session_id {
        return Err(format!(
            "preclaimed successor turn {turn_id} belongs to session {turn_session}, not {session_id}"
        ));
    }
    let state: TurnState = state
        .parse()
        .map_err(|e| format!("preclaimed successor turn {turn_id} has invalid state: {e}"))?;
    if state != TurnState::Pending {
        return Err(format!(
            "preclaimed successor turn {turn_id} is {state}, not pending"
        ));
    }
    Ok(())
}

pub(crate) fn continue_automatic_retry(
    orch: &Orchestrator,
    job_id: &str,
    retry_turn_id: &str,
) -> Result<Run, String> {
    continue_job_impl(
        orch,
        job_id,
        None,
        None,
        Some(ResumeContext {
            suppress_user_event: true,
            suppress_self_suspend_note: false,
            preclaimed_retry_turn_id: Some(retry_turn_id.to_string()),
            supersede_pending_retry: false,
            preclaimed_successor_turn_id: None,
        }),
    )
}

/// Continue an existing job with an optional follow-up message.
///
/// Reuses a warm process if one exists for the job's session, otherwise starts
/// a new Claude process with `--resume`.
pub fn continue_job_impl(
    orch: &Orchestrator,
    job_id: &str,
    message: Option<&str>,
    identity_override: Option<crate::identity::UserIdentity>,
    prompt_resume: Option<ResumeContext>,
) -> Result<Run, String> {
    continue_job_impl_with_intent(
        orch,
        job_id,
        message,
        identity_override,
        prompt_resume,
        ContinuationIntent::Normal,
    )
}

/// The launch funnel for an existing job, and the one place a resume is
/// admitted (CAIRN-3283).
///
/// Deciding a job is idle, inserting its run, allocating its turn, spawning the
/// backend process and registering that process are five separate steps with no
/// mutual exclusion between them. Two wake facts routed concurrently for one job
/// each evaluated the idle check before either had created a turn, so both fell
/// through, both inserted a run, both drove the same turn row, and both spawned a
/// process against the same session — two independent agent contexts racing every
/// side effect of one turn. The per-job launch lock makes that whole section
/// atomic; the recheck under it is what makes the serialization correct, since a
/// caller that blocked and then acquired must attach to the launch it waited on
/// rather than mint a second one.
///
/// Nothing is lost by attaching: [`continue_job_impl`] claims ALL of a job's
/// pending side-channel notices, queued messages and attention pushes, so the
/// launch already in flight carries the waiting caller's content too — and
/// anything persisted after that claim is swept by the turn-end flush.
fn continue_job_impl_with_intent(
    orch: &Orchestrator,
    job_id: &str,
    message: Option<&str>,
    identity_override: Option<crate::identity::UserIdentity>,
    prompt_resume: Option<ResumeContext>,
    continuation_intent: ContinuationIntent,
) -> Result<Run, String> {
    let launch_lock = orch.job_launch_lock(job_id);
    // A poisoned lock means some earlier launch panicked. What it guards is
    // durable rows that the recheck below re-reads from scratch, so recover the
    // guard rather than refusing this job every resume from here on.
    let _launch_guard = launch_lock
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    // A pre-created retry or owned-wait successor turn is the caller's own
    // claimed turn (pending, validated below), not somebody else's launch.
    let owns_preclaimed_turn = prompt_resume.as_ref().is_some_and(|context| {
        context.preclaimed_retry_turn_id.is_some() || context.preclaimed_successor_turn_id.is_some()
    });
    if !owns_preclaimed_turn {
        if let Some(in_flight) = in_flight_launch(orch, job_id)? {
            // A resume carrying in-memory content has nowhere durable to leave
            // it, so refuse loudly instead of silently dropping the text. Every
            // such caller runs at a genuine turn boundary, where this branch
            // cannot fire unless something raced it.
            if message.is_some() {
                return Err(format!(
                    "Job {} is already running a turn (run {}); this resume was refused rather than started as a second concurrent session.",
                    &job_id[..job_id.len().min(8)],
                    &in_flight.id[..in_flight.id.len().min(8)]
                ));
            }
            log::info!(
                "Job {} already has a launch in flight (run {}); attaching to it instead of starting a second",
                &job_id[..job_id.len().min(8)],
                &in_flight.id[..in_flight.id.len().min(8)]
            );
            return Ok(in_flight);
        }
    }

    continue_job_launch_locked(
        orch,
        job_id,
        message,
        identity_override,
        prompt_resume,
        continuation_intent,
    )
}

/// The launch already in flight for this job, if there is one. Called only under
/// the per-job launch lock, before this resume has mutated anything.
///
/// A launch is in flight in either of two ways.
///
/// An admitted cold start holds a [`JobLaunchClaim`](crate::orchestrator::JobLaunchClaim)
/// from `prepare_job` until its process registers. Nothing observable is serving
/// a turn during that interval — that is the whole reason the claim exists — so
/// it is checked first, before any turn state.
///
/// Otherwise both halves must agree: the job's head turn is `running` in the
/// database AND the run driving it has a live in-process handle serving exactly
/// that turn. Requiring both is deliberate.
///
/// - A `pending` head turn is not in flight: it is a pre-created retry or
///   owned-wait successor whose owner is the caller.
/// - An active turn whose run has no live handle is not in flight either — that
///   is the stale turn [`reconcile_stale_active_turn_for_continue`] recovers, and
///   the incident's own job read as idle to both nudges precisely because a run
///   had sat in `starting` for two hours with no process behind it. Treating that
///   as in flight would convert a rare transient race into a permanently wedged
///   node.
fn in_flight_launch(orch: &Orchestrator, job_id: &str) -> Result<Option<Run>, String> {
    let owning_db = run_db({
        let dbs = orch.db.clone();
        let job_id = job_id.to_string();
        async move {
            crate::execution::routing::owning_db_for_job(&dbs, &job_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;

    // A cold start that has been admitted but whose process has not registered
    // yet owns this job's launch, even though nothing observable is serving a
    // turn for it. Checked before the turn state because that interval is
    // precisely where the turn state cannot tell the two apart.
    if let Some(run_id) = orch.claimed_launch_run(job_id) {
        return run_db(load_run(
            owning_db,
            run_id,
            "Run not found for claimed launch",
        ))
        .map(Some);
    }

    let Some(active) = load_active_turn_for_continue(owning_db.clone(), job_id.to_string())? else {
        return Ok(None);
    };
    if active.state != TurnState::Running {
        return Ok(None);
    }
    let Some(run_id) = active.run_id else {
        return Ok(None);
    };
    if orch.process_state.get_current_turn_id(&run_id).as_deref() != Some(active.turn_id.as_str()) {
        return Ok(None);
    }
    run_db(load_run(
        owning_db,
        run_id,
        "Run not found for in-flight turn",
    ))
    .map(Some)
}

/// Test seam for the launch recheck: the predicate is what makes serialization
/// correct, and its two failure modes (attaching to nothing, or refusing a
/// resume forever) are opposite and both severe.
#[cfg(any(test, feature = "test-utils"))]
pub fn in_flight_launch_for_test(
    orch: &Orchestrator,
    job_id: &str,
) -> Result<Option<String>, String> {
    in_flight_launch(orch, job_id).map(|run| run.map(|run| run.id))
}

fn continue_job_launch_locked(
    orch: &Orchestrator,
    job_id: &str,
    message: Option<&str>,
    identity_override: Option<crate::identity::UserIdentity>,
    prompt_resume: Option<ResumeContext>,
    continuation_intent: ContinuationIntent,
) -> Result<Run, String> {
    // Resolve the job's owning database ONCE (fail-closed): a team job resumes
    // against its synced replica, never the private DB.
    let owning_db = run_db({
        let dbs = orch.db.clone();
        let job_id = job_id.to_string();
        async move {
            crate::execution::routing::owning_db_for_job(&dbs, &job_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;

    // ---- Load job -------------------------------------------------------
    let (job, project_id, issue_id, project_path) = run_db(load_job_context(
        owning_db.clone(),
        orch.db.clone(),
        job_id.to_string(),
    ))?;
    let preclaimed_retry_turn_id = prompt_resume
        .as_ref()
        .and_then(|context| context.preclaimed_retry_turn_id.clone());
    let preclaimed_successor_turn_id = prompt_resume
        .as_ref()
        .and_then(|context| context.preclaimed_successor_turn_id.clone());
    // An owned-wait resolution pre-creates its `WaitResolved` successor turn and
    // hands it here to reuse; that changes reconcile, reseed, and turn-selection
    // below (CAIRN-2970).
    let resuming_owned_wait = preclaimed_successor_turn_id.is_some();
    if let Some(retry_turn_id) = preclaimed_retry_turn_id.as_deref() {
        if job.parent_job_id.is_some()
            || job.recipe_node_id.is_some()
            || job.agent_config_id.as_deref() == Some("workflow")
            || !pending_retry_head_matches(owning_db.clone(), job_id, retry_turn_id)?
        {
            return Err("automatic retry was superseded or is ineligible".to_string());
        }
    }

    // CAIRN-2629: same device-ownership guard as the start/claim path — refuse to
    // resume an execution owned by another machine (its runner owns the lifecycle).
    // Fail-open on a read hiccup (deferred_owner returns None), and treat a NULL /
    // this-machine owner as "may proceed".
    if let Some(exec_id) = job.execution_id.clone() {
        let this_device = orch.anon_device_manager.device_id();
        let deferred = run_db({
            let owning_db = owning_db.clone();
            async move {
                Ok::<_, String>(
                    crate::execution::ownership::deferred_owner(&owning_db, &exec_id, &this_device)
                        .await,
                )
            }
        })?;
        if let Some(owner) = deferred {
            return Err(format!(
                "This execution is owned by device {owner}, not this machine; resume it there."
            ));
        }
    }

    let current_session_id = job.current_session_id.as_ref().ok_or_else(|| {
        if job.status == "blocked" {
            "This job has no agent session to resume. A command checkpoint is resolved by confirming it (an override that continues the workflow).".to_string()
        } else {
            "Job has no current session to resume".to_string()
        }
    })?;

    // ---- Transition job to Running if in terminal state -------------------
    // Any resume (including the post-completion memory review) makes the agent
    // active, so the job is Running. The confirm affordance is decoupled from
    // run state (it keys off the unconfirmed artifact), so a Running review does
    // not hide it (CAIRN-1576).
    if job.status != "running" {
        if let Err(e) = transition_job_to_running(orch, job_id) {
            log::warn!("Failed to transition job {} to running: {}", job_id, e);
        }
    }

    let agent_config = load_agent_config(orch, &job, project_path.as_deref())?;
    let process_residence = crate::scratch::ensure_job_scratch_dir(job_id, None);
    let process_residence = process_residence.to_string_lossy().to_string();

    let now = chrono::Utc::now().timestamp() as i32;

    // Desired backend for this turn, mirroring cold-start backend selection
    // (agent `backend_preference`, else inferred from the model). Used to detect
    // a model change that crosses providers so the session can rotate.
    // Resolve-early: prefer the snapshot's stored selection backend so a resumed
    // session uses what it was launched with, never a re-resolution against
    // changed settings. backend_preference / model-derivation are fallbacks.
    let desired_backend = agent_config
        .as_ref()
        .and_then(|ac| ac.selection.as_ref().map(|s| s.backend.clone()))
        .or_else(|| {
            agent_config
                .as_ref()
                .and_then(|ac| ac.backend_preference.clone())
        })
        .or_else(|| {
            job.model
                .as_deref()
                .and_then(crate::backends::backend_for_model)
                .map(str::to_string)
        });

    // ---- Session identity check -----------------------------------------
    // Load Session and derive explicit continue semantics. When the requested
    // model now implies a different backend than the session was started on, the
    // prior backend's resume handle is invalid on the new backend, so rotate to a
    // fresh session on the new backend rather than resuming with a wrong handle.
    // This runs before the warm/cold split, so it covers cold continues too.
    // Cold-resume reseed decision (CAIRN-2534) is produced in the plain-resume
    // arm below and threaded into prompt assembly + seed-event storage.
    let mut reseed_outcome: Option<ReseedOutcome> = None;
    let (session_id, session_start, run_start_mode) = match run_db(load_session_optional(
        owning_db.clone(),
        current_session_id.clone(),
    ))? {
        Some(session) => match session.status {
            SessionStatus::Open => {
                if continuation_intent == ContinuationIntent::ForceDigestReseed {
                    // Digest construction and rotation must succeed before the
                    // prompt-edit flag is consumed. On failure the old session and
                    // its pending fresh-prompt requirement remain usable.
                    let outcome = finish_forced_session_reseed(
                        owning_db.clone(),
                        job_id,
                        force_session_reseed(orch, &owning_db, &job, &session),
                    )?;
                    let session_start = crate::backends::SessionStart::New {
                        session_id: outcome.new_session_id.clone(),
                    };
                    let run_start_mode = run_start_mode(&session_start).to_string();
                    let new_id = outcome.new_session_id.clone();
                    reseed_outcome = Some(outcome);
                    (new_id, session_start, run_start_mode)
                } else {
                    // Ordinary continuation consumes the flag while choosing its
                    // action, exactly as before manual digest resume existed.
                    let needs_fresh_session = run_db(take_needs_fresh_session(
                        owning_db.clone(),
                        job_id.to_string(),
                    ))?;
                    match decide_continue_action(
                        &session.backend,
                        session.backend_id.as_deref(),
                        desired_backend.as_deref(),
                        needs_fresh_session,
                    ) {
                        ContinueSessionAction::RotateToBackend(want) => {
                            // Evict any live process bound to the old session first.
                            if let Some(old_run) =
                                orch.process_state.find_process_by_session(&session.id)
                            {
                                orch.process_state.stop_and_remove(&old_run);
                            }
                            log::info!(
                                "Backend change {} -> {} for job {}; rotating to a fresh session",
                                session.backend,
                                want,
                                job_id
                            );
                            let new_session = run_db({
                                let db = owning_db.clone();
                                let session = session.clone();
                                let job_id = job_id.to_string();
                                let want = want.to_string();
                                let emitter = orch.services.emitter.clone();
                                async move {
                                    crate::sessions::queries::rotate_job_session_to_backend(
                                        db.as_ref(),
                                        &session,
                                        &job_id,
                                        &want,
                                        emitter.as_ref(),
                                    )
                                    .await
                                }
                            })?;
                            let session_start = crate::backends::SessionStart::New {
                                session_id: new_session.id.clone(),
                            };
                            let run_start_mode = run_start_mode(&session_start).to_string();
                            (new_session.id, session_start, run_start_mode)
                        }
                        ContinueSessionAction::RotateFresh => {
                            // Evict any live process bound to the old session, then
                            // rotate to a fresh same-backend session so this turn
                            // rebuilds the edited system prompt.
                            if let Some(old_run) =
                                orch.process_state.find_process_by_session(&session.id)
                            {
                                orch.process_state.stop_and_remove(&old_run);
                            }
                            log::info!(
                                "Prompt change for job {}; rotating to a fresh session",
                                job_id
                            );
                            let new_session = run_db({
                                let db = owning_db.clone();
                                let session = session.clone();
                                let job_id = job_id.to_string();
                                let emitter = orch.services.emitter.clone();
                                async move {
                                    crate::sessions::queries::rotate_job_session(
                                        db.as_ref(),
                                        &session,
                                        &job_id,
                                        emitter.as_ref(),
                                    )
                                    .await
                                }
                            })?;
                            let session_start = crate::backends::SessionStart::New {
                                session_id: new_session.id.clone(),
                            };
                            let run_start_mode = run_start_mode(&session_start).to_string();
                            (new_session.id, session_start, run_start_mode)
                        }
                        ContinueSessionAction::RetryStart => {
                            // A handle-less open session is a provisional startup that
                            // never reached the backend handshake. Retry the fresh start
                            // under the same Cairn session identity so its transcript and
                            // pending turn remain attached rather than rotating again.
                            let session_start = crate::backends::SessionStart::New {
                                session_id: session.id.clone(),
                            };
                            let run_start_mode = run_start_mode(&session_start).to_string();
                            (session.id.clone(), session_start, run_start_mode)
                        }
                        ContinueSessionAction::Resume => {
                            // Native backend resume of the open session (e.g. a Codex
                            // session carrying its stored thread id). Reseed is
                            // bypassed so a reply never restarts at the system prompt
                            // (CAIRN-2598).
                            let session_start = resolve_continue_session_start(&session)?;
                            let run_start_mode = run_start_mode(&session_start).to_string();
                            (session.id.clone(), session_start, run_start_mode)
                        }
                        ContinueSessionAction::MaybeReseed => {
                            // Reseed-eligible open session. If it has gone stale (last
                            // event older than the staleness threshold), a native
                            // backend resume would reload a prompt cache the provider
                            // has likely evicted; reseed instead by rotating to a
                            // fresh session primed with the node's `/chat` digest
                            // (CAIRN-2534). Any failure in the attempt falls open to
                            // native resume with session state untouched.
                            //
                            // An owned-wait resume is the exception: it reuses a
                            // successor turn already bound to this session, so a
                            // reseed rotation would strand that turn. Take native
                            // resume instead (CAIRN-2970).
                            let now_secs = orch.services.clock.now();
                            let reseed = if resuming_owned_wait {
                                None
                            } else {
                                attempt_session_reseed(orch, &owning_db, &job, &session, now_secs)
                            };
                            match reseed {
                                Some(outcome) => {
                                    let session_start = crate::backends::SessionStart::New {
                                        session_id: outcome.new_session_id.clone(),
                                    };
                                    let run_start_mode = run_start_mode(&session_start).to_string();
                                    let new_id = outcome.new_session_id.clone();
                                    reseed_outcome = Some(outcome);
                                    (new_id, session_start, run_start_mode)
                                }
                                None => {
                                    let session_start = resolve_continue_session_start(&session)?;
                                    let run_start_mode = run_start_mode(&session_start).to_string();
                                    (session.id.clone(), session_start, run_start_mode)
                                }
                            }
                        }
                    }
                }
            }
            // A closed or failed session cannot take another turn, and that is
            // not something the caller can retry past. Name what WOULD work
            // instead of reporting an internal session state: an operator who
            // reached this while trying to intervene on a merged issue got only
            // "Session cf7a2f00 is Closed and cannot be continued", which told
            // them nothing they could act on (CAIRN-3253).
            SessionStatus::Closed => {
                return Err(match session.terminal_reason.as_deref() {
                    Some("issue_merged") => "This node's issue has been merged, which stopped its work and closed its session, so it cannot take another turn. To pick the work back up, reopen the issue (set its status to backlog) and start a new execution on it.".to_string(),
                    Some("issue_closed") => "This node's issue has been closed, which stopped its work and closed its session, so it cannot take another turn. To pick the work back up, reopen the issue (set its status to backlog) and start a new execution on it.".to_string(),
                    Some("node_removed") => "This node was removed from its execution, which closed its session, so it cannot take another turn. Add the node back to the execution to run it again.".to_string(),
                    _ => "This node's session has been closed, so it cannot take another turn. Start a new execution on the issue to carry the work forward.".to_string(),
                });
            }
            SessionStatus::Failed => {
                return Err("This node's session ended in a failure, so it cannot take another turn. Start a new execution on the issue to carry the work forward.".to_string());
            }
        },
        None => {
            // No Session record found — legacy data (e.g. old Codex thread_id).
            // Use session_id directly; it may itself be a resume handle.
            log::info!(
                "No Session record for {}, using as-is (legacy)",
                &current_session_id[..current_session_id.len().min(8)]
            );
            (
                current_session_id.clone(),
                crate::backends::SessionStart::Resume {
                    session_id: current_session_id.clone(),
                    backend_id: current_session_id.clone(),
                },
                "resume".to_string(),
            )
        }
    };

    // ---- Find or create run ---------------------------------------------
    // A delayed retry is best-effort. Recheck its durable claim immediately
    // before allocating or waking a run so a superseding action wins cleanly.
    if let Some(retry_turn_id) = preclaimed_retry_turn_id.as_deref() {
        if !pending_retry_head_matches(owning_db.clone(), job_id, retry_turn_id)? {
            return Err("automatic retry was superseded before run launch".to_string());
        }
    }
    // A run row this host holds no process for is not a launch in flight; it is
    // a predecessor that never reached a terminal status, and left alone it
    // keeps answering `latest_run_for_job` (and therefore the whole delivery
    // ladder) on behalf of a process that does not exist (CAIRN-3291). Reap it
    // here, where the reaper's precondition is already met: the launch lock is
    // held, and this precedes the warm-reuse decision below, so every row is
    // still carrying whatever process it had when this resume arrived.
    crate::runs::reap::reap_stale_runs_for_job(orch, &owning_db, job_id);
    // Reconcile a reusable process against the job's requested model *before*
    // deciding to reuse it. `jobs.model` is the source of truth: a model change
    // restarts the process (cold resume with the new model) so the persisted
    // model wins. Cross-backend changes were already handled by rotation above,
    // so any process found here is on the correct backend.
    let existing_run_id = match orch.process_state.find_process_by_session(&session_id) {
        Some(found_run_id) => {
            match ensure_reused_process_model(
                &orch.process_state,
                &found_run_id,
                job.model.as_deref(),
            )? {
                ReuseDecision::Reuse => Some(found_run_id),
                ReuseDecision::Restart => {
                    log::info!(
                        "Model changed for session {}; evicting warm process {} and restarting",
                        &session_id[..session_id.len().min(8)],
                        &found_run_id[..found_run_id.len().min(8)]
                    );
                    orch.process_state.stop_and_remove(&found_run_id);
                    None
                }
            }
        }
        None => None,
    };
    // The owned-wait successor IS the intended (pending) active turn; reconcile
    // would cancel it, and the yielded predecessor leaves nothing else stale.
    if existing_run_id.is_none() && !resuming_owned_wait {
        let _ = reconcile_stale_active_turn_for_continue(orch, job_id, &session_id)?;
    }
    let (run_id, is_process_reuse) = if let Some(existing_id) = existing_run_id {
        log::info!(
            "Found existing process for session {}, reusing run {}",
            &session_id[..session_id.len().min(8)],
            &existing_id[..existing_id.len().min(8)]
        );
        (existing_id, true)
    } else {
        let new_run_id = ids::mint_child(job_id);
        let status_str = RunStatus::Starting.to_string();
        run_db(insert_run(
            owning_db.clone(),
            RunInsert {
                run_id: new_run_id.clone(),
                issue_id: issue_id.clone(),
                project_id: Some(project_id.clone()),
                job_id: Some(job_id.to_string()),
                status: status_str.clone(),
                session_id: Some(session_id.clone()),
                started_at: None,
                created_at: now,
                updated_at: now,
                start_mode: Some(run_start_mode.clone()),
                // Advisory only, and logged on the prepare path already: under
                // the per-job launch lock a pre-existing active run for this job
                // should now be impossible, so say so if one ever appears.
                warn_existing_active: true,
            },
        ))?;
        let _ = orch.services.emitter.emit(
            "db-change",
            crate::notify::run_db_change_ids(
                "insert",
                &new_run_id,
                Some(job_id),
                issue_id.as_deref(),
                Some(&project_id),
            ),
        );
        (new_run_id, false)
    };

    // ---- Create successor turn for follow-up ----------------------------
    // A resume carrying user-authored content (an explicit message or a
    // prompt/permission answer) is a work turn, never the post-completion
    // memory-review reflection. The pending-queued-message case (a user steer
    // that arrives without an explicit message) is detected inside
    // `create_followup_turn` against the rows the claim below sweeps up.
    let user_initiated = message.is_some()
        || prompt_resume
            .as_ref()
            .is_some_and(|context| context.preclaimed_retry_turn_id.is_none());
    let supersede_pending_retry = prompt_resume
        .as_ref()
        .is_some_and(|context| context.supersede_pending_retry);
    let turn_id = if let Some(successor_id) = preclaimed_successor_turn_id.as_deref() {
        // Reuse the pre-created owned-wait successor instead of allocating a
        // follow-up; it is started (pending -> running on this run) below like any
        // other pending successor.
        validate_preclaimed_successor(owning_db.clone(), job_id, &session_id, successor_id)?;
        successor_id.to_string()
    } else if let Some(retry_turn_id) = preclaimed_retry_turn_id {
        if !pending_retry_head_matches(owning_db.clone(), job_id, &retry_turn_id)? {
            return Err("automatic retry was superseded before launch".to_string());
        }
        retry_turn_id
    } else {
        create_followup_turn(
            orch,
            &session_id,
            job_id,
            user_initiated,
            supersede_pending_retry,
        )?
    };
    let retry_turn_prestarted = prompt_resume
        .as_ref()
        .is_some_and(|context| context.preclaimed_retry_turn_id.is_some());
    if retry_turn_prestarted && !claim_retry_turn_start(orch, &turn_id, &run_id)? {
        return Err("automatic retry was superseded before turn reservation".to_string());
    }

    // ---- Artifact schema ------------------------------------------------
    let artifact_schema_info = run_db(find_job_downstream_artifact_schema(
        owning_db.clone(),
        job.clone(),
    ))?;

    // CAIRN-1309: claim every queued user follow-up for this job (both `queue`
    // and any `steer` row that never reached a tool boundary). They are
    // delivered on this resume — covering the turn-end flush and the resume that
    // follows answering a question/permission prompt.
    let queued_messages = match crate::messages::queued::claim_all_for_job(&owning_db, job_id) {
        Ok(msgs) => msgs,
        Err(error) => {
            log::warn!(
                "failed to claim queued messages for resume prompt on job {}: {}",
                &job_id[..job_id.len().min(8)],
                error
            );
            Vec::new()
        }
    };
    let has_queued = !queued_messages.is_empty();

    // When there is no explicit resume message but the user queued follow-ups,
    // the queued content *is* the user's prompt — don't lead with the generic
    // "Continue where you left off." placeholder (and don't store it as a "You"
    // event; each queued message is stored as its own event below).
    // CAIRN-1881: drain this job's pending attention pushes. Both rousing
    // (`wake`/`interrupt`) and `passive` ride-along pushes deliver on a resume
    // that is already happening; each is lazy-resolved so a push whose referent
    // already resolved is skipped. They are stamped delivered atomically with
    // their carrying event, persisted below once the prompt is assembled.
    let drained_pushes = {
        let db = owning_db.clone();
        let recipient = job_id.to_string();
        run_db(async move {
            let pushes = crate::orchestrator::attention_push::list_pending_live(&db, &recipient)
                .await
                .map_err(|e| e.to_string())?;
            Ok::<_, String>(retain_resolvable_pushes(&db, &recipient, pushes).await)
        })
        .unwrap_or_default()
    };
    let has_pushes = !drained_pushes.is_empty();
    // CAIRN-1891: resolve each push's content_ref to its rendered resource content
    // so the resumed agent acts without a round-trip read. Uses the same in-process
    // backs `cairn read`; run on a scoped DB runtime so it can borrow orch.
    let push_prompt = if has_pushes {
        let pushes = drained_pushes.clone();
        crate::storage::run_db_blocking(move || async move {
            Ok::<_, String>(
                crate::orchestrator::attention_delivery::render_pushes_resolved(orch, &pushes)
                    .await,
            )
        })
        .ok()
        .flatten()
    } else {
        None
    };
    // CAIRN-1891: the persisted carrying event is the wake-card payload
    // (`{active, catchup}`) PLUS the `resolved` content the agent received, so the
    // transcript renders a card and its detail modal shows the full content (not
    // just the resource ref). The agent gets the same resolved content inline in
    // the prompt below.
    let push_summary = if has_pushes {
        Some(
            crate::orchestrator::attention_push::push_event_content_json(
                &drained_pushes,
                push_prompt.as_deref().unwrap_or_default(),
            ),
        )
    } else {
        None
    };
    let trigger = resolve_resume_trigger(message, has_queued, has_pushes);
    let base_prompt = resolve_skill_slash_command(orch, &trigger.message, project_path.as_deref());
    let side_channel_notices =
        match crate::messages::side_channel::claim_pending_side_channel_for_job(&owning_db, job_id)
        {
            Ok(notices) => notices,
            Err(error) => {
                log::warn!(
                    "failed to claim side-channel notices for resume prompt on job {}: {}",
                    &job_id[..job_id.len().min(8)],
                    error
                );
                Vec::new()
            }
        };
    if !side_channel_notices.is_empty() {
        crate::messages::transcript::insert_side_channel_events_sync(
            orch,
            &run_id,
            Some(&session_id),
            Some(&turn_id),
            &side_channel_notices,
        )?;
    }
    let queued_block = if has_queued {
        Some(
            queued_messages
                .iter()
                .map(|m| m.content.clone())
                .collect::<Vec<_>>()
                .join("\n\n"),
        )
    } else {
        None
    };
    let side_channel_block = if side_channel_notices.is_empty() {
        None
    } else {
        Some(crate::messages::transcript::render_side_channel_prompt_block(&side_channel_notices))
    };
    // A durable self-suspend resume injects the awaited result as the suspended
    // tool call's synthetic `tool_result` (this is exactly when
    // `suppress_user_event` is set). From the agent's side that call looks
    // interrupted mid-execution and then returns fine, which is disconcerting;
    // lead the forwarded prompt with a short note that names the pause as
    // deliberate. The note rides only in the live prompt, never a stored event,
    // so it frames this turn without cluttering the transcript.
    let suppress_user_event = prompt_resume
        .as_ref()
        .map(|ctx| ctx.suppress_user_event)
        .unwrap_or(false);
    let suppress_self_suspend_note = prompt_resume
        .as_ref()
        .is_some_and(|ctx| ctx.suppress_self_suspend_note);
    let artifact_handoff_note = artifact_handoff_resume_note(owning_db.clone(), job_id)?;
    let resume_note = match (
        (suppress_user_event && !suppress_self_suspend_note)
            .then_some(RESUME_AFTER_SELF_SUSPEND_NOTE),
        artifact_handoff_note.as_deref(),
    ) {
        (Some(self_suspend), Some(handoff)) => Some(format!("{self_suspend}\n\n{handoff}")),
        (Some(self_suspend), None) => Some(self_suspend.to_string()),
        (None, Some(handoff)) => Some(handoff.to_string()),
        (None, None) => None,
    };
    let prompt = assemble_resume_prompt(
        resume_note.as_deref(),
        queued_block,
        &base_prompt,
        side_channel_block,
        push_prompt.as_deref(),
    );
    // Reseed: the seed (header + prior-session digest) leads the reconstructed
    // context; the normal trigger tail follows. The seed is also stored as a
    // collapsible `user:seed` event below, while the trigger keeps its own
    // verbatim user event (CAIRN-2534).
    let prompt = apply_reseed_seed(prompt, reseed_outcome.as_ref());
    // This is the single outer turn-boundary stamp. Apply it after every producer,
    // including cold-resume reseeding, so all resumed input opens with the clock.
    let previous_turn_end = previous_turn_end_for_resume(owning_db.clone(), &turn_id)?;
    let prompt = prepend_resume_stamp(
        prompt,
        crate::clock::host(),
        chrono::Utc::now(),
        previous_turn_end,
    );

    let job_model = job.model.as_ref().map(Model::new);

    // ---- Store user event -----------------------------------------------
    // Store the user's message so the UI displays what was actually sent.
    //
    // Slow-path prompt resumes skip this: the answer is already rendered as the
    // originating Question call's synthetic tool_result, so a separate "You"
    // block would duplicate it. The message is still forwarded to the agent
    // below for the model's context.
    //
    // `suppress_user_event` was resolved above (it also gates the resume note);
    // reuse it here.
    // Skip the default "You" event when the user supplied no explicit message and
    // the content is carried entirely by queued follow-ups (stored individually
    // below) — storing the empty placeholder would render a blank You block. A
    // resume with no operator content at all still stores its event, but as the
    // `user:continuation` marker rather than a "You" block.
    // CAIRN-1881: persist the carrying event for the drained pushes and stamp each
    // delivered by it, atomically (same transaction as the event INSERT). The
    // pushes already ride in the resume prompt above, so they are delivered
    // regardless of `suppress_user_event`; recovery redelivers only pushes whose
    // carrying event never durably landed.
    // Reseed: store the seed (header + digest) as a collapsible `user:seed`
    // event ahead of every other event in this turn, so the transcript renders a
    // divider followed by the verbatim trigger message rather than a giant seed
    // bubble, and future digests collapse the seed to one line (CAIRN-2534).
    if let Some(outcome) = &reseed_outcome {
        store_seed_event_with_turn(
            orch,
            &run_id,
            &session_id,
            &outcome.seed_content,
            now,
            Some(&turn_id),
        )?;
    }
    if let Some(text) = &push_summary {
        let push_ids: Vec<String> = drained_pushes.iter().map(|p| p.id.clone()).collect();
        crate::execution::jobs::snapshots::store_attention_push_event(
            orch,
            &run_id,
            &session_id,
            text,
            &push_ids,
            now,
            Some(&turn_id),
        )?;
    }
    // Queued follow-ups — including passive "quiet" notes that rode along without
    // waking the agent — were authored before the immediate resume message, so
    // they render first, matching the prompt order assembled above. Each shows as
    // its own "You" block and drops out of the pending strip.
    if has_queued {
        for queued in &queued_messages {
            let display_message = queued.content.clone();
            store_user_event_with_turn(
                orch,
                &run_id,
                &session_id,
                &display_message,
                now,
                Some(&turn_id),
            )?;
        }
        let _ = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "queued_messages", "action": "update"}),
        );
    }
    let store_default_user_event = !suppress_user_event && !trigger.message.is_empty();
    if store_default_user_event {
        let display_message = trigger.message.clone();
        // A synthesized continuation is stored under its own namespaced type so
        // no surface can render it as something the operator said (CAIRN-3175).
        let store = if trigger.synthetic {
            store_continuation_event_with_turn
        } else {
            store_user_event_with_turn
        };
        store(
            orch,
            &run_id,
            &session_id,
            &display_message,
            now,
            Some(&turn_id),
        )?;
    }

    // ---- Warm process or new session ------------------------------------
    if is_process_reuse {
        // Establish the turn FULLY before waking the agent (CAIRN-2123). A warm
        // agent commonly fires a tool call the instant it resumes; if the wake
        // (`send_user_message`) preceded turn establishment, that call could
        // land in the `Busy`-without-turn window and capture a NULL turn for any
        // worktree-fence crossing it raised — durably parking the run with no
        // way to resume it. Ordering occupancy (`transition_to_active` then
        // `ServingTurn` via `begin_turn`) and the persisted DB turn
        // (`start_turn` writes `jobs.current_turn_id`) ahead of the wake closes
        // that window: a tool call produced on resume always observes
        // `ServingTurn(turn)`, never `Busy`.
        orch.process_state.transition_to_active(&run_id);
        if !retry_turn_prestarted {
            start_turn(orch, &turn_id, &run_id)?;
        }
        orch.process_state
            .set_current_turn_id(&run_id, Some(&turn_id));

        // Run stays Live — no durable status change for warm reuse.
        let message_text = prompt.clone();
        let image_project_id = project_id.clone();
        let image_project_key = run_db({
            let db = owning_db.clone();
            let project_id = project_id.clone();
            async move {
                db.query_opt_text("SELECT key FROM projects WHERE id = ?1", (project_id,))
                    .await
                    .map_err(|error| error.to_string())?
                    .ok_or_else(|| "resume project authority is unavailable".to_string())
            }
        })?;
        let message_content = crate::storage::run_db_blocking(move || async move {
            crate::agent_process::stdin::resolve_stable_images(
                &orch.db,
                &image_project_id,
                &image_project_key,
                message_text,
            )
            .await
        })?;
        crate::backends::stdin::send_user_message(
            &orch.process_state,
            &run_id,
            &message_content,
            &session_id,
            None,
            Some(&process_residence),
        )?;
    } else {
        crate::orchestrator::session::start_agent_session(
            orch,
            &run_id,
            &prompt,
            session_start,
            job_model,
            None,
            agent_config.as_ref(),
            artifact_schema_info.as_ref(),
            // Recipe node jobs write their artifact via the write verb through
            // the normal confirm/review flow; they are never natively
            // constrained (CAIRN-2505).
            false,
            false,
            job.execution_id.as_deref(),
            identity_override,
        )?;

        // The new session's process handle now exists, so establish the turn on
        // it. `start_agent_session` spawns the CLI and returns before the MCP
        // handshake completes, so the first tool call cannot arrive until after
        // this point — the turn is always established before the session emits a
        // crossing. (`set_current_turn_id` needs the handle to exist, which is
        // why this follows the spawn rather than preceding it.)
        if !retry_turn_prestarted {
            start_turn(orch, &turn_id, &run_id)?;
        }
        orch.process_state
            .set_current_turn_id(&run_id, Some(&turn_id));
    }

    // ---- Return run -----------------------------------------------------
    let run = run_db(load_run(
        owning_db.clone(),
        run_id.clone(),
        "Run not found after creation",
    ))?;
    Ok(run)
}

fn artifact_handoff_resume_note(
    owning_db: Arc<LocalDb>,
    job_id: &str,
) -> Result<Option<String>, String> {
    let job_id = job_id.to_string();
    run_db(async move {
        owning_db
            .query_opt(
                "SELECT a.output_name, a.artifact_type, a.version, a.confirmed
                   FROM jobs j
                   JOIN turns t ON t.id = j.current_turn_id
                   JOIN artifacts a ON a.job_id = j.id
                  WHERE j.id = ?1 AND t.end_reason = 'artifact_handoff'
                  ORDER BY a.created_at DESC, a.rowid DESC
                  LIMIT 1",
                (job_id,),
                |row| {
                    Ok((
                        row.opt_text(0)?,
                        row.text(1)?,
                        row.i64(2)? as i32,
                        row.i64(3)? != 0,
                    ))
                },
            )
            .await
            .map_err(|error| format!("Failed to resolve artifact handoff resume note: {error}"))
            .map(|artifact| {
                artifact.map(|(output_name, artifact_type, version, confirmed)| {
                    let name = output_name.unwrap_or(artifact_type);
                    let state = if confirmed {
                        "applied successfully"
                    } else {
                        "applied successfully and is awaiting user confirmation"
                    };
                    format!(
                        "Resuming after an artifact handoff. Your previous turn ended because you wrote your terminal output artifact (cairn:~/{name}, version {version}) — the write was {state} and the session paused for review. Any interruption notice in the prior transcript was Cairn ending the turn at that boundary, not a user abort. The message that resumes you follows."
                    )
                })
            })
    })
}

/// The immediate message a resume leads with, and whether Cairn wrote it.
struct ResumeTrigger {
    /// Text that becomes the resume prompt's immediate message. Empty when the
    /// content is carried entirely by queued follow-ups or attention pushes,
    /// which are assembled (and stored) as their own blocks.
    message: String,
    /// True when no operator content reached this resume at all, so `message` is
    /// [`SYNTHETIC_CONTINUATION_PROMPT`] — Cairn's own text. Every attribution
    /// decision downstream keys off this flag.
    synthetic: bool,
}

/// Decide what a resume leads with, given the caller's message and whether the
/// claims for this wake swept up queued follow-ups or attention pushes.
///
/// A blank message is no message: it reaches this seam only from a caller with
/// nothing to say, and treating it as operator content would store an empty
/// "You" block.
fn resolve_resume_trigger(
    message: Option<&str>,
    has_queued: bool,
    has_pushes: bool,
) -> ResumeTrigger {
    match message.filter(|text| !text.trim().is_empty()) {
        Some(operator) => ResumeTrigger {
            message: operator.to_string(),
            synthetic: false,
        },
        None if has_queued || has_pushes => ResumeTrigger {
            message: String::new(),
            synthetic: false,
        },
        None => ResumeTrigger {
            message: SYNTHETIC_CONTINUATION_PROMPT.to_string(),
            synthetic: true,
        },
    }
}

/// Prompt for a resume that carries no operator content at all.
///
/// Cairn resumes a job on its own for reasons the operator never authored — an
/// automatic retry, a flush that found nothing pending, a suspension whose
/// result arrives without a message. The wake still needs prompt text, so Cairn
/// writes it. The previous text ("Continue where you left off.") read as a terse
/// operator instruction, and agents obeyed it as one, planning around a
/// directive nobody gave (CAIRN-3175). This text names itself as substrate
/// instead: it says what happened, denies operator intent explicitly, and gives
/// the safe default for a turn that was already finished.
///
/// Sibling to [`RESUME_AFTER_SELF_SUSPEND_NOTE`], which corrects the adjacent
/// misattribution (CAIRN-3162): that note rebuts a *suspension* the CLI rendered
/// as a user rejection, while this text is the *prompt* for a resume nobody
/// asked for. A self-suspend resume with no message carries both.
const SYNTHETIC_CONTINUATION_PROMPT: &str = "Automatic resume: Cairn restarted this turn after the previous one ended without completing. No message accompanies it — nobody asked you to continue, and there is no new instruction here. Pick your own work back up from where it stopped; if it was already finished, close the turn with a short status rather than starting something new.";

/// Short note that leads the forwarded prompt on a durable self-suspend resume.
///
/// On the slow (>45s) path the awaited result is delivered as the suspended tool
/// call's synthetic `tool_result`, so to the resumed agent that call looks
/// interrupted mid-execution and then returns — a deliberate pause that reads
/// like a glitch. Naming it as an intentional suspend up front removes that
/// tension (CAIRN-2173).
const RESUME_AFTER_SELF_SUSPEND_NOTE: &str = "Resuming after an intentional self-suspend. Your run paused itself to wait on work it had already started — delegated work (a sub-agent task or a user question), an explicit wait, or one or more long-running command batches — and resumed now that the work finished. Cairn, not the user, parked those tool calls on purpose. If the CLI described any of them as rejected or interrupted by the user, that wording was a transport artifact; the real results follow, labeled by call when there is more than one.";

fn prepend_resume_stamp(
    prompt: String,
    clock: &crate::clock::HostClock,
    now: chrono::DateTime<chrono::Utc>,
    previous_turn_end: Option<i64>,
) -> String {
    format!(
        "{}\n\n{}",
        clock.resume_prefix(now, previous_turn_end),
        prompt
    )
}

fn previous_turn_end_for_resume(db: Arc<LocalDb>, turn_id: &str) -> Result<Option<i64>, String> {
    let turn_id = turn_id.to_string();
    run_db(async move {
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT predecessor.ended_at
                           FROM turns current
                           LEFT JOIN turns predecessor ON predecessor.id = current.predecessor_id
                          WHERE current.id = ?1
                          LIMIT 1",
                        (turn_id.as_str(),),
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => row.opt_i64(0),
                    None => Ok(None),
                }
            })
        })
        .await
        .map_err(|error| error.to_string())
    })
}

/// Assemble a resume prompt so notes that accumulated before this wake lead and
/// the immediate resume message follows them.
///
/// A `resume_note` (the self-suspend frame) leads everything when present.
/// Queued user follow-ups — including `passive` "quiet" notes that rode along
/// without waking the agent — were authored before the message that triggers
/// this resume, so they precede `base_prompt`. A quiet note "A" sent before a
/// waking message "B" is therefore delivered as "A\n\nB", matching the order the
/// user sent them rather than reversing it. Side-channel notices and resolved
/// attention pushes keep their established position after the immediate
/// message. Falls back to the generic continue placeholder when every part is
/// empty.
fn assemble_resume_prompt(
    resume_note: Option<&str>,
    queued_block: Option<String>,
    base_prompt: &str,
    side_channel_block: Option<String>,
    push_prompt: Option<&str>,
) -> String {
    let mut parts: Vec<String> = Vec::new();
    if let Some(note) = resume_note {
        if !note.is_empty() {
            parts.push(note.to_string());
        }
    }
    if let Some(q) = queued_block {
        if !q.is_empty() {
            parts.push(q);
        }
    }
    if !base_prompt.is_empty() {
        parts.push(base_prompt.to_string());
    }
    if let Some(s) = side_channel_block {
        if !s.is_empty() {
            parts.push(s);
        }
    }
    if let Some(p) = push_prompt {
        if !p.is_empty() {
            parts.push(p.to_string());
        }
    }
    if parts.is_empty() {
        SYNTHETIC_CONTINUATION_PROMPT.to_string()
    } else {
        parts.join("\n\n")
    }
}

// ============================================================================
// Cold-resume reseed (CAIRN-2534)
// ============================================================================

/// Cold-resume reseed staleness threshold: when an open session's last event is
/// older than this at resume, the native backend resume is replaced by a fresh
/// session seeded with the node's `/chat` digest. One hour comfortably outlives
/// a provider's prompt-cache TTL, so a resume inside the window still hits a warm
/// cache and reseeding would buy nothing.
const SESSION_STALENESS_THRESHOLD_SECS: i64 = 60 * 60;

/// Header that leads a compacted THREAD seed, framing the three sections that
/// follow. A thread is never finished, so the framing points at what keeps it
/// cheap rather than apologizing for a reconstruction: the arc is the only part
/// of this context nothing can regenerate.
const THREAD_SEED_HEADER: &str = "This thread's session was rebuilt so it stays cheap to run indefinitely. Below: the arc you authored, a table of contents for history that has been compacted away, and the most recent turns verbatim. Every compacted chapter names what it was and where to read it in full — read one only when it bears on what you are doing now, because re-reading costs more than it saves unless the range is genuinely load-bearing. Re-read any file whose content still matters, and keep `cairn:~/arc` current: decisions, the paths you rejected and why, and the open questions are the only things here that cannot be rebuilt from the transcript.";

/// Header that leads the reseed seed prompt, framing the digest that follows.
const RESEED_SEED_HEADER: &str = "The prior events in this session are summarized below because the underlying agent session was reconstructed after a period of inactivity (tool-result bodies were elided). Re-read any files whose content is still relevant, and re-read your working documents (todos/plan/board) for the current plan state before acting.";

/// Resolve the staleness threshold, honoring the `CAIRN_SESSION_STALENESS_SECS`
/// dev/test override (parsed, non-empty) and falling back to the constant —
/// mirroring the `CAIRN_JJ_BIN` env-override convention.
fn staleness_threshold_secs() -> i64 {
    std::env::var("CAIRN_SESSION_STALENESS_SECS")
        .ok()
        .and_then(|raw| {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                None
            } else {
                trimmed.parse::<i64>().ok()
            }
        })
        .unwrap_or(SESSION_STALENESS_THRESHOLD_SECS)
}

/// Pure staleness predicate: the session is stale once its last event is more
/// than `threshold` seconds behind `now`. A resume exactly at the boundary is
/// not stale (native resume), so the comparison is strict.
fn is_session_stale(now: i64, last_event_at: i64, threshold: i64) -> bool {
    now - last_event_at > threshold
}

/// The `sessions.backend` value persisted for a Codex session. A Codex session
/// with a stored `backend_id` (its native thread id) resumes through
/// `thread/resume`, which has its own stale-thread fallback (a fresh thread with
/// transcript preload) that only fires after Codex reports the thread missing.
const CODEX_SESSION_BACKEND: &str = "codex";

/// Whether an open session resumes natively and so must NOT be preempted by the
/// cold-resume reseed (CAIRN-2534). Codex sessions carrying a native thread id
/// (`backend_id`) resume via `thread/resume`; rotating them to a fresh
/// `SessionStart::New` would wipe the Codex thread and restart the reply at the
/// system prompt (CAIRN-2598). Claude and handle-less sessions stay
/// reseed-eligible.
fn session_prefers_native_resume(backend: &str, backend_id: Option<&str>) -> bool {
    backend.eq_ignore_ascii_case(CODEX_SESSION_BACKEND) && backend_id.is_some()
}

/// The continuation action for an open session on a user reply. This is the pure
/// decision; the caller performs the DB rotation / reseed / native-resume work.
#[derive(Debug, PartialEq, Eq)]
enum ContinueSessionAction {
    /// The requested model implies a different backend than the session was
    /// started on — rotate to a fresh session on that backend (the old backend's
    /// resume handle is invalid on the new one).
    RotateToBackend(String),
    /// A prompt edit since spawn requires a fresh same-backend session so this
    /// turn rebuilds the edited system prompt.
    RotateFresh,
    /// Retry a provisional session whose backend startup never confirmed a
    /// native resume handle. The Cairn session identity and transcript survive.
    RetryStart,
    /// Attempt the stale cold-resume reseed, falling back to native resume.
    MaybeReseed,
    /// Native backend resume of the open session.
    Resume,
}

/// Decide how an open session continues on a user reply. Ordering matters: a
/// cross-backend model change rotates first, then a prompt edit forces a fresh
/// same-backend session. A session without a confirmed backend handle retries
/// its original startup; a session that resumes natively (Codex with a stored
/// thread id) resumes directly, and only the remaining sessions are eligible for
/// the cold-resume reseed.
fn decide_continue_action(
    session_backend: &str,
    session_backend_id: Option<&str>,
    desired_backend: Option<&str>,
    needs_fresh_session: bool,
) -> ContinueSessionAction {
    if let Some(want) = desired_backend.filter(|want| *want != session_backend) {
        return ContinueSessionAction::RotateToBackend(want.to_string());
    }
    if needs_fresh_session {
        return ContinueSessionAction::RotateFresh;
    }
    if session_backend_id.is_none() {
        return ContinueSessionAction::RetryStart;
    }
    if session_prefers_native_resume(session_backend, session_backend_id) {
        return ContinueSessionAction::Resume;
    }
    ContinueSessionAction::MaybeReseed
}

/// The seed content delivered and stored on a reseed: the header framing the
/// digest, then the digest itself (`header + digest`). The trigger is appended
/// separately at delivery and stored as its own verbatim `user` event.
fn build_reseed_seed_content(digest: &str) -> String {
    format!("{RESEED_SEED_HEADER}\n\n{digest}")
}

/// The seed content delivered and stored when a thread compacts. Same shape as
/// [`build_reseed_seed_content`] — header then body, trigger appended later —
/// so a compacted thread seed remains an ordinary `user:seed` everywhere
/// downstream.
fn build_thread_seed_content(composed: &str, idle_secs: Option<i64>) -> String {
    match idle_secs.filter(|seconds| *seconds >= 60) {
        // A rebuilt session has no felt continuity with the one before it: every
        // relative reference in the turns below ("just now", "still running")
        // was written against a clock the resumed agent cannot see. Naming the
        // gap once, at the top, is what lets it read them correctly.
        Some(seconds) => format!(
            "{THREAD_SEED_HEADER}\n\nYour last turn ended {} ago; everything below was written before that.\n\n{composed}",
            crate::clock::format_elapsed(seconds)
        ),
        None => format!("{THREAD_SEED_HEADER}\n\n{composed}"),
    }
}

// ── Thread compaction triggers (CAIRN-3388) ───────────────────────────────────

/// What a thread compaction decision is made from.
#[derive(Debug, Clone, Copy, PartialEq)]
struct ThreadCompactionInputs {
    /// The prompt cache has expired, so the next turn pays a write regardless.
    stale: bool,
    /// Whether this composition would actually drop anything. A rotation that
    /// replaces nothing is pure loss: it costs the thread its reasoning and its
    /// continuity and reclaims no window at all.
    drops_anything: bool,
    /// Live context occupancy as `(used tokens, the model's window)`, from the
    /// provider's own accounting of the last request. `None` when there is no
    /// reading — a session that has not inferred yet, an unrecognized model, a
    /// backend whose catalog carries no window.
    occupancy: Option<(i64, i64)>,
    /// The fraction of the window at which a warm session gives something up.
    threshold: f64,
}

/// Whether a warm session has filled enough of its window to have to give
/// something up.
///
/// Deliberately not an economic question. Bytes can say what a rebuild costs in
/// money; they cannot say what it costs the thread, which is its own reasoning
/// and its sense of continuity — the thinking that produced a conclusion does not
/// survive into the seed, only the conclusion does. That cost is large, fixed,
/// and paid in full every time, so no amount of saved prefix earns it back. A
/// warm session therefore rebuilds only when the window is running out, which is
/// the one situation where not rebuilding is not an option either.
///
/// Occupancy comes from the provider's own accounting of the request just made.
/// That is the opposite of using a token counter to price a seed never sent: here
/// the question *is* what the last request weighed.
fn window_pressure_reached(occupancy: Option<(i64, i64)>, threshold: f64) -> bool {
    // No reading, no rebuild. The alternative — estimating occupancy from bytes
    // against an assumed window — is a second, cruder measure standing in for the
    // real one, which is exactly the substitution that made the previous trigger
    // fire on everything. When Cairn cannot see the window, the provider's own
    // limit is the backstop, and it fails loudly rather than silently degrading
    // the thread.
    let Some((used, window)) = occupancy else {
        return false;
    };
    if window <= 0 {
        return false;
    }
    used as f64 >= window as f64 * threshold
}

/// Decide whether a thread session compacts now.
///
/// Two triggers, both grounded in prompt-cache economics. A 1h cache write costs
/// 2× base input and a read 0.1×, so rewriting history mid-conversation is
/// expensive rather than free:
///
/// - **expiry**: the prompt cache is gone, so the session is being rebuilt
///   whatever happens and the choice is only what to rebuild it from. Thread
///   wakes are bursty, so expiry also tends to land at a natural boundary.
/// - **capacity**: the window is running out while the cache is still warm, by
///   [`window_pressure_reached`].
///
/// The two are asked in that order and for different reasons, which is the whole
/// shape of the policy. Expiry is not a judgement about whether rebuilding is
/// worth it — by then it has already happened. Capacity is the only thing that
/// makes a *live* session give up its continuity, and it does so because the
/// alternative is a request the provider will refuse.
///
/// What is deliberately absent is any comparison of what compaction would save
/// against what it would cost. A rebuild costs the thread its own reasoning: the
/// thinking that produced a conclusion does not survive into the seed, only the
/// conclusion does. Nothing measured in bytes buys that back, so "this would be
/// cheaper" is not a reason to do it, at any size.
///
/// A child reaching terminal status still never triggers anything by itself: it
/// marks what *became* compactable, and one of these two decides when to apply
/// the accumulated marks.
fn decide_thread_compaction(inputs: ThreadCompactionInputs) -> Option<CompactionTrigger> {
    if inputs.stale {
        return Some(CompactionTrigger::Expiry);
    }
    // A warm rotation that replaces nothing spends the thread's continuity and
    // reclaims no window, so it is worse than doing nothing at any occupancy.
    // Expiry is exempt: there the rebuild is already happening.
    if !inputs.drops_anything {
        return None;
    }
    window_pressure_reached(inputs.occupancy, inputs.threshold)
        .then_some(CompactionTrigger::Capacity)
}

/// Prepend the reseed seed to the trigger prompt when this resume reseeded;
/// identity otherwise. Keeps the seed-before-trigger ordering in one place.
fn apply_reseed_seed(prompt: String, reseed: Option<&ReseedOutcome>) -> String {
    match reseed {
        Some(outcome) => format!("{}\n\n{}", outcome.seed_content, prompt),
        None => prompt,
    }
}

/// A successful cold-resume reseed: the fresh session that was rotated in, and
/// the seed (header + prior-session digest) to deliver and store.
struct ReseedOutcome {
    new_session_id: String,
    seed_content: String,
}

/// Attempt a cold-resume reseed for an open, stale session (CAIRN-2534).
///
/// Returns `Some` only when the session is stale AND a fresh session was
/// successfully rotated in; the caller then spawns that fresh session and stores
/// the seed. Returns `None` — fail open — when the session is still fresh or any
/// step fails. Rotation is the last, only-persisted mutation, so a `None` from a
/// mid-attempt failure leaves session state exactly as a native resume would.
///
/// The digest is rendered from the prior history *before* any rotation or new
/// event, so it can never contain itself.
fn attempt_session_reseed(
    orch: &Orchestrator,
    owning_db: &Arc<LocalDb>,
    job: &DbJob,
    session: &Session,
    now: i64,
) -> Option<ReseedOutcome> {
    let last_event_at = run_db(load_last_event_time_for_session(
        owning_db.clone(),
        session.id.clone(),
    ))
    .ok()
    .flatten()?;
    let stale = is_session_stale(now, last_event_at, staleness_threshold_secs());

    // A thread is never finished, so it does not reseed from its whole
    // transcript: it compacts. That path also runs while the session is warm,
    // because the size ratio can fire between wakes.
    if thread_compaction_capability(owning_db, &job.id).is_enabled() {
        return attempt_thread_compaction(
            orch,
            owning_db,
            job,
            session,
            stale,
            Some(now.saturating_sub(last_event_at)),
        );
    }

    if !stale {
        return None;
    }

    let outcome = force_session_reseed(orch, owning_db, job, session).ok()?;

    log::info!(
        "Cold-resume reseed for job {}: rotated session {} -> {} ({}s idle > {}s)",
        &job.id[..job.id.len().min(8)],
        &session.id[..session.id.len().min(8)],
        &outcome.new_session_id[..outcome.new_session_id.len().min(8)],
        now - last_event_at,
        staleness_threshold_secs(),
    );
    Some(outcome)
}

fn finish_forced_session_reseed(
    owning_db: Arc<LocalDb>,
    job_id: &str,
    outcome: Result<ReseedOutcome, String>,
) -> Result<ReseedOutcome, String> {
    let outcome = outcome?;
    // The successful fresh session was built with the current agent snapshot, so
    // its pending prompt-edit rotation has now been satisfied. Consume only after
    // success; an error must preserve the flag for the next ordinary continuation.
    run_db(take_needs_fresh_session(owning_db, job_id.to_string()))?;
    Ok(outcome)
}

fn force_session_reseed(
    orch: &Orchestrator,
    owning_db: &Arc<LocalDb>,
    job: &DbJob,
    session: &Session,
) -> Result<ReseedOutcome, String> {
    // An operator forcing a digest resume on a thread gets the thread's own seed
    // shape; there is only one way to reconstruct a thread's context.
    if thread_compaction_capability(owning_db, &job.id).is_enabled() {
        let seed = compose_thread_seed_blocking(orch, owning_db, job)?;
        let idle_secs = run_db(load_last_event_time_for_session(
            owning_db.clone(),
            session.id.clone(),
        ))
        .ok()
        .flatten()
        .map(|last_event_at| orch.services.clock.now().saturating_sub(last_event_at));
        let framed = build_thread_seed_content(&seed.content, idle_secs);
        return apply_thread_compaction(
            orch,
            owning_db,
            job,
            session,
            DecidedCompaction {
                seed,
                framed,
                trigger: CompactionTrigger::Manual,
                occupancy: None,
            },
        );
    }

    // Construct the complete seed before any process or session mutation.
    let digest = {
        let db = owning_db.clone();
        let job = job.clone();
        run_db(
            async move { Ok::<_, String>(crate::resources::render_reseed_digest(&db, &job).await) },
        )?
    };
    if digest.trim().is_empty() || digest == "No runs found for this node." {
        return Err(
            "Cannot resume from digest because this job has no resumable transcript.".to_string(),
        );
    }

    rotate_with_seed(
        orch,
        owning_db,
        job,
        session,
        build_reseed_seed_content(&digest),
    )
}

/// Stop the live process and rotate to a fresh session carrying `seed_content`.
///
/// The one mutating half of every reseed, thread or not. Rotation is the last
/// and only persisted step, so any failure before it leaves session state
/// exactly as a native resume would have.
fn rotate_with_seed(
    orch: &Orchestrator,
    owning_db: &Arc<LocalDb>,
    job: &DbJob,
    session: &Session,
    seed_content: String,
) -> Result<ReseedOutcome, String> {
    if let Some(old_run) = orch.process_state.find_process_by_session(&session.id) {
        orch.process_state.stop_and_remove(&old_run);
    }

    let new_session = run_db({
        let db = owning_db.clone();
        let session = session.clone();
        let job_id = job.id.clone();
        let emitter = orch.services.emitter.clone();
        async move {
            crate::sessions::queries::rotate_job_session(
                db.as_ref(),
                &session,
                &job_id,
                emitter.as_ref(),
            )
            .await
            .map_err(|error| format!("Failed to rotate session for digest resume: {error}"))
        }
    })?;

    Ok(ReseedOutcome {
        new_session_id: new_session.id,
        seed_content,
    })
}

/// Whether this job's session compacts as a thread. One resolution point, in
/// `crate::threads`; a failure to resolve reads as "not a thread", which keeps
/// the ordinary path.
fn thread_compaction_capability(owning_db: &Arc<LocalDb>, job_id: &str) -> ThreadCompaction {
    let db = owning_db.clone();
    let job_id = job_id.to_string();
    run_db(async move {
        Ok::<_, String>(crate::threads::compaction_capability_for_job(&db, &job_id).await)
    })
    .unwrap_or(ThreadCompaction::Disabled)
}

/// The session's live context occupancy, as the provider last reported it.
///
/// `None` — no snapshot yet, an unrecognized model, a backend catalog without a
/// window — leaves a warm session alone rather than guessing at a number this
/// lossy a decision turns on.
fn session_occupancy(orch: &Orchestrator, session_id: &str) -> Option<(i64, i64)> {
    let orch = orch.clone();
    let session_id = session_id.to_string();
    let state =
        run_db(async move { Ok::<_, String>(orch.get_context_token_state(&session_id).await) })
            .ok()
            .flatten()?;
    Some((state.used_tokens, state.context_window?))
}

fn compose_thread_seed_blocking(
    orch: &Orchestrator,
    owning_db: &Arc<LocalDb>,
    job: &DbJob,
) -> Result<crate::resources::ThreadSeed, String> {
    let db = owning_db.clone();
    let job_id = job.id.clone();
    let now = orch.services.clock.now();
    run_db(async move { crate::resources::compose_thread_seed(&db, &job_id, now).await })
}

/// Compose a thread's seed, decide whether this continuation compacts, and apply
/// it if so.
///
/// Composition runs on every thread continuation because its result is also what
/// prices the decision. It is read-only: nothing is persisted and no session
/// moves unless a trigger fires, so the common case is a composed seed that is
/// thrown away and a native resume.
fn attempt_thread_compaction(
    orch: &Orchestrator,
    owning_db: &Arc<LocalDb>,
    job: &DbJob,
    session: &Session,
    stale: bool,
    idle_secs: Option<i64>,
) -> Option<ReseedOutcome> {
    let seed = match compose_thread_seed_blocking(orch, owning_db, job) {
        Ok(seed) => seed,
        Err(error) => {
            // Fail open, exactly as the ordinary reseed attempt does: the old
            // session and its current pointer stay usable.
            log::warn!(
                "Thread seed composition failed for job {}; resuming natively: {error}",
                &job.id[..job.id.len().min(8)]
            );
            return None;
        }
    };

    // Only consulted while warm, so the reading is skipped entirely on the
    // expiry path rather than blocking a rebuild that is already decided.
    let occupancy = if stale {
        None
    } else {
        session_occupancy(orch, &session.id)
    };
    let trigger = decide_thread_compaction(ThreadCompactionInputs {
        stale,
        drops_anything: seed.source_bytes > 0,
        occupancy,
        threshold: orch.get_settings().thread_compact_threshold,
    })?;

    let framed = build_thread_seed_content(&seed.content, idle_secs);

    let decided = DecidedCompaction {
        seed,
        framed,
        trigger,
        occupancy,
    };
    match apply_thread_compaction(orch, owning_db, job, session, decided) {
        Ok(outcome) => Some(outcome),
        Err(error) => {
            log::warn!(
                "Thread compaction failed for job {}; resuming natively: {error}",
                &job.id[..job.id.len().min(8)]
            );
            None
        }
    }
}

/// A compaction that has been decided on: what to send, why, and the reading
/// that caused it. Travelling together is what keeps them consistent — the bytes
/// the generation records are the bytes rotation delivers, and the occupancy the
/// log reports is the occupancy the decision turned on.
struct DecidedCompaction {
    seed: crate::resources::ThreadSeed,
    /// The seed exactly as it will be delivered, header and bridge line applied.
    framed: String,
    trigger: CompactionTrigger,
    occupancy: Option<(i64, i64)>,
}

/// Persist the generation, then rotate.
///
/// Order matters: the marks a seed folded into chapters are consumed in the same
/// transaction that records the chapters, before any session moves, because the
/// seed must be on record by the time it can reach an agent. The generation
/// records the session it was composed from, and rotation is what makes it
/// applied — so a rotation that fails leaves it pending, its marks still
/// eligible and its chapters still unclaimed, and the next continuation decides
/// exactly as it would have if this attempt had never happened.
fn apply_thread_compaction(
    orch: &Orchestrator,
    owning_db: &Arc<LocalDb>,
    job: &DbJob,
    session: &Session,
    decided: DecidedCompaction,
) -> Result<ReseedOutcome, String> {
    let DecidedCompaction {
        seed,
        framed,
        trigger,
        occupancy,
    } = decided;
    let seed_bytes = framed.len() as i64;
    let applied = crate::threads::compaction::AppliedCompaction {
        trigger,
        source_session_id: seed.source_session_id,
        entries: seed.entries,
        seed_bytes,
        source_bytes: seed.source_bytes,
        candidate_bytes: seed.candidate_bytes,
        compacted_through_block: seed.compacted_through_block,
        recency_start_block: seed.recency_start_block,
        consumed_child_issue_ids: seed.consumed_child_issue_ids,
    };
    let now = orch.services.clock.now();
    run_db({
        let db = owning_db.clone();
        let job_id = job.id.clone();
        async move {
            crate::threads::compaction::persist_generation(&db, &job_id, &applied, now)
                .await
                .map_err(|error| format!("Failed to persist a thread compaction: {error}"))
        }
    })?;

    let outcome = rotate_with_seed(orch, owning_db, job, session, framed)?;

    // Occupancy rides in the log line because it is what the warm decision turns
    // on; the byte counts describe what the rebuild did, never why it happened.
    let occupancy = match occupancy {
        Some((used, window)) if window > 0 => {
            format!("{used}/{window} tokens ({}%)", used * 100 / window)
        }
        _ => "occupancy unread".to_string(),
    };
    log::info!(
        "Thread compaction ({}) for job {}: {} chapters, {} bytes -> {} bytes, \
         framed seed {} bytes, {}, session {} -> {}",
        trigger.as_db(),
        &job.id[..job.id.len().min(8)],
        seed.new_entries,
        seed.source_bytes,
        seed.candidate_bytes,
        seed_bytes,
        occupancy,
        &session.id[..session.id.len().min(8)],
        &outcome.new_session_id[..outcome.new_session_id.len().min(8)],
    );

    Ok(outcome)
}

/// Outcome of reconciling a reusable process against the job's requested model.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ReuseDecision {
    /// The live process already matches (or can serve) the requested model — reuse it.
    Reuse,
    /// The live process cannot serve the requested model and must be restarted.
    Restart,
}

/// Reconcile a reusable warm/active process against the job's requested model.
///
/// `jobs.model` and the model recorded on the live process (`RunHandle.model`,
/// set at startup) are the source of truth. By the time this runs, any
/// cross-backend change has already been handled by session rotation in
/// `continue_job_impl`, so a process found for the current session is guaranteed
/// to be on the right backend — only the model can differ here.
///
/// A model change resolves to `Restart` rather than an in-place `set_model`. A
/// live switch is not trusted: for Claude the accept/reject arrives later as an
/// async control response, so reusing the process immediately would risk running
/// the next turn on a stale model and recording a model the process never
/// confirmed. Restart (cold resume of the same session with the new model) is
/// deterministic. If a restart would be required but the process is not idle,
/// returns an error rather than killing an active or host-blocked turn.
fn ensure_reused_process_model(
    process_state: &crate::agent_process::process::AgentProcessState,
    run_id: &str,
    desired_model: Option<&str>,
) -> Result<ReuseDecision, String> {
    // No requested model → nothing to reconcile; reuse as-is.
    let Some(desired) = desired_model else {
        return Ok(ReuseDecision::Reuse);
    };

    // Already serving the requested model → reuse.
    if process_state.get_model(run_id).as_deref() == Some(desired) {
        return Ok(ReuseDecision::Reuse);
    }

    // Model diverged → restart deterministically, but refuse to tear down a
    // process that is mid-turn or host-blocked.
    match process_state.get_occupancy(run_id) {
        Some(crate::agent_process::process::RunOccupancy::Idle) | None => {
            log::info!(
                "Model changed to {} for run {}; restarting to apply it",
                desired,
                &run_id[..run_id.len().min(8)]
            );
            Ok(ReuseDecision::Restart)
        }
        Some(other) => Err(format!(
            "Cannot change model to {} for run {}: process is busy ({:?})",
            desired,
            &run_id[..run_id.len().min(8)],
            other
        )),
    }
}

struct ActiveTurnForContinue {
    turn_id: String,
    state: TurnState,
    run_id: Option<String>,
}

fn load_active_turn_for_continue(
    db: Arc<LocalDb>,
    job_id: String,
) -> Result<Option<ActiveTurnForContinue>, String> {
    run_db(async move {
        db.read(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT id, state, run_id
                         FROM turns
                         WHERE job_id = ?1
                           AND state IN ('pending', 'running')
                         ORDER BY sequence DESC
                         LIMIT 1",
                        (job_id.as_str(),),
                    )
                    .await?;
                rows.next()
                    .await?
                    .map(|row| {
                        let state = row.text(1)?.parse().map_err(db_internal)?;
                        Ok(ActiveTurnForContinue {
                            turn_id: row.text(0)?,
                            state,
                            run_id: row.opt_text(2)?,
                        })
                    })
                    .transpose()
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
}

fn mark_stale_active_turn_for_continue(
    db: Arc<LocalDb>,
    active: ActiveTurnForContinue,
) -> Result<(), String> {
    run_db(async move {
        db.write(|conn| {
            let turn_id = active.turn_id.clone();
            let state = active.state.clone();
            let run_id = active.run_id.clone();
            Box::pin(async move {
                let now = chrono::Utc::now().timestamp();
                let target_state = match &state {
                    TurnState::Running => TurnState::Interrupted,
                    TurnState::Pending => TurnState::Cancelled,
                    _ => return Ok(()),
                };

                conn.execute(
                    "UPDATE turns
                     SET state = ?1,
                         ended_at = ?2,
                         updated_at = ?2
                     WHERE id = ?3
                       AND state = ?4",
                    params![
                        target_state.to_string(),
                        now,
                        turn_id.as_str(),
                        state.to_string()
                    ],
                )
                .await?;

                if let Some(run_id) = run_id.as_deref() {
                    conn.execute(
                        "UPDATE runs
                         SET status = 'crashed',
                             exit_reason = 'stale_continue_recovery',
                             exited_at = ?1,
                             updated_at = ?1
                         WHERE id = ?2
                           AND status IN ('starting', 'live', 'running', 'idle')",
                        params![now, run_id],
                    )
                    .await?;
                }

                Ok(())
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
}

fn reconcile_stale_active_turn_for_continue(
    orch: &Orchestrator,
    job_id: &str,
    session_id: &str,
) -> Result<bool, String> {
    if orch
        .process_state
        .find_process_by_session(session_id)
        .is_some()
    {
        return Ok(false);
    }

    let owning_db = run_db({
        let dbs = orch.db.clone();
        let job_id = job_id.to_string();
        async move {
            crate::execution::routing::owning_db_for_job(&dbs, &job_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;
    let Some(active) = load_active_turn_for_continue(owning_db.clone(), job_id.to_string())? else {
        return Ok(false);
    };

    if active
        .run_id
        .as_deref()
        .and_then(|run_id| orch.process_state.get_current_turn_id(run_id))
        .is_some()
    {
        return Ok(false);
    }

    let turn_id = active.turn_id.clone();
    let run_id = active.run_id.clone();
    let state = active.state.clone();
    mark_stale_active_turn_for_continue(owning_db.clone(), active)?;
    log::warn!(
        "Recovered stale {} turn {} for job {} before continue",
        state,
        turn_id,
        job_id
    );
    let turn_change = run_db({
        let db = owning_db.clone();
        let turn_id = turn_id.clone();
        async move { Ok(crate::notify::turn_db_change_for_id(&db, &turn_id, "update").await) }
    })?;
    let _ = orch.services.emitter.emit("db-change", turn_change);
    if let Some(run_id) = run_id.as_deref() {
        let run_change = run_db({
            let db = owning_db.clone();
            let run_id = run_id.to_string();
            async move { Ok(crate::notify::run_db_change_for_id(&db, &run_id, "update").await) }
        })?;
        let _ = orch.services.emitter.emit("db-change", run_change);
    }
    Ok(true)
}

#[cfg(any(test, feature = "test-utils"))]
pub fn reconcile_stale_active_turn_for_continue_for_test(
    orch: &Orchestrator,
    job_id: &str,
    session_id: &str,
) -> Result<bool, String> {
    reconcile_stale_active_turn_for_continue(orch, job_id, session_id)
}

#[cfg(test)]
mod tests {
    use super::{
        apply_reseed_seed, assemble_resume_prompt, build_reseed_seed_content,
        decide_continue_action, ensure_reused_process_model, finish_forced_session_reseed,
        is_session_stale, prepend_resume_stamp, previous_turn_end_for_resume, push_job_coordinate,
        resolve_resume_trigger, session_prefers_native_resume, staleness_threshold_secs,
        take_needs_fresh_session, ContinueSessionAction, PushJobCoordinate, ReseedOutcome,
        ReuseDecision, RESEED_SEED_HEADER, SESSION_STALENESS_THRESHOLD_SECS,
        SYNTHETIC_CONTINUATION_PROMPT,
    };
    use crate::agent_process::process::{wrap_plain_stdin, AgentProcessState, RunHandle};
    use crate::storage::migrated_test_db;
    use std::sync::{Arc, Mutex};

    #[test]
    fn wake_coordinate_parser_distinguishes_nested_tasks() {
        match push_job_coordinate("cairn://p/CAIRN/42/2/builder/task/review-rust/checks") {
            Some(PushJobCoordinate::Task {
                project,
                number,
                exec_seq,
                node,
                task,
            }) => {
                assert_eq!(
                    (
                        project.as_str(),
                        number,
                        exec_seq,
                        node.as_str(),
                        task.as_str()
                    ),
                    ("CAIRN", 42, 2, "builder", "review-rust")
                );
            }
            other => panic!("expected task coordinate, got {other:?}"),
        }
        assert!(push_job_coordinate("cairn://p/CAIRN/42").is_none());
        assert!(push_job_coordinate("not-a-cairn-uri").is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn successful_forced_reseed_consumes_prompt_edit_flag() {
        let db = Arc::new(migrated_test_db("forced-reseed-consumes-prompt-edit").await);
        db.execute_script(
            "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES('p','w','P','P','/tmp/p',1,1);
             INSERT INTO jobs(id, project_id, status, needs_fresh_session, created_at, updated_at)
               VALUES('j','p','blocked',1,1,1);",
        )
        .await
        .unwrap();

        let outcome = finish_forced_session_reseed(
            db.clone(),
            "j",
            Ok(ReseedOutcome {
                new_session_id: "new-session".to_string(),
                seed_content: "seed".to_string(),
            }),
        )
        .unwrap();
        assert_eq!(outcome.new_session_id, "new-session");
        assert!(!take_needs_fresh_session(db, "j".to_string()).await.unwrap());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn failed_forced_reseed_preserves_prompt_edit_flag() {
        let db = Arc::new(migrated_test_db("failed-reseed-preserves-prompt-edit").await);
        db.execute_script(
            "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES('p','w','P','P','/tmp/p',1,1);
             INSERT INTO jobs(id, project_id, status, needs_fresh_session, created_at, updated_at)
               VALUES('j','p','blocked',1,1,1);",
        )
        .await
        .unwrap();

        let error = match finish_forced_session_reseed(
            db.clone(),
            "j",
            Err("digest rendering failed".to_string()),
        ) {
            Ok(_) => panic!("failed forced reseed unexpectedly succeeded"),
            Err(error) => error,
        };
        assert_eq!(error, "digest rendering failed");
        assert!(take_needs_fresh_session(db, "j".to_string()).await.unwrap());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn resume_elapsed_uses_stored_predecessor_turn_end() {
        let db = Arc::new(migrated_test_db("resume-clock-predecessor").await);
        db.execute_script(
            "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES('p','w','P','P','/tmp/p',1,1);
             INSERT INTO jobs(id, project_id, status, current_session_id, created_at, updated_at)
               VALUES('j','p','running','s',1,1);
             INSERT INTO sessions(id, job_id, backend, status, created_at, updated_at)
               VALUES('s','j','codex','open',1,1);
             INSERT INTO turns(id, session_id, job_id, sequence, state, start_reason, created_at, ended_at, updated_at)
               VALUES('previous','s','j',1,'completed','initial',1,1000,1000);
             INSERT INTO turns(id, session_id, job_id, sequence, predecessor_id, state, start_reason, created_at, updated_at)
               VALUES('current','s','j',2,'previous','running','follow_up',1100,1100);",
        )
        .await
        .unwrap();

        assert_eq!(
            previous_turn_end_for_resume(db, "current").unwrap(),
            Some(1000)
        );
    }

    #[test]
    fn resumed_input_stamp_is_outermost_even_for_reseeded_context() {
        let clock = crate::clock::HostClock::fixed("America/Los_Angeles");
        let now = chrono::DateTime::from_timestamp(1_752_381_600, 0).unwrap();
        let reconstructed = apply_reseed_seed(
            "TRIGGER".to_string(),
            Some(&ReseedOutcome {
                new_session_id: "session-new".to_string(),
                seed_content: "SEED".to_string(),
            }),
        );
        let prompt =
            prepend_resume_stamp(reconstructed, &clock, now, Some(now.timestamp() - 192 * 60));
        assert_eq!(
            prompt,
            "[Sat 21:40 PDT — resumed after 3h 12m]\n\nSEED\n\nTRIGGER"
        );
    }

    #[test]
    fn passive_note_precedes_immediate_resume_message() {
        // A quiet note "A" queued before a waking message "B" must deliver as
        // "A then B", matching send order — not reversed.
        let prompt = assemble_resume_prompt(None, Some("A".to_string()), "B", None, None);
        assert_eq!(prompt, "A\n\nB");
    }

    #[test]
    fn multiple_queued_notes_keep_order_before_message() {
        let prompt = assemble_resume_prompt(None, Some("A1\n\nA2".to_string()), "B", None, None);
        assert_eq!(prompt, "A1\n\nA2\n\nB");
    }

    #[test]
    fn resume_prompt_without_queued_is_just_the_message() {
        let prompt = assemble_resume_prompt(None, None, "B", None, None);
        assert_eq!(prompt, "B");
    }

    #[test]
    fn queued_only_resume_has_no_placeholder() {
        let prompt = assemble_resume_prompt(None, Some("A".to_string()), "", None, None);
        assert_eq!(prompt, "A");
    }

    #[test]
    fn empty_resume_falls_back_to_the_synthetic_continuation() {
        // Second construction site: every part came back empty, so the assembled
        // prompt is Cairn's own continuation text rather than a bare directive.
        let prompt = assemble_resume_prompt(None, None, "", None, None);
        assert_eq!(prompt, SYNTHETIC_CONTINUATION_PROMPT);
    }

    #[test]
    fn synthetic_continuation_never_speaks_as_the_operator() {
        // The text an agent reads must name itself as Cairn's own resume and deny
        // operator intent outright. The retired placeholder ("Continue where you
        // left off.") read as a terse instruction and was obeyed as one
        // (CAIRN-3175), so its exact shape is pinned out.
        let text = SYNTHETIC_CONTINUATION_PROMPT;
        assert!(
            text.starts_with("Automatic resume:"),
            "the continuation must announce itself before anything else: {text}"
        );
        assert!(
            text.contains("nobody asked you to continue"),
            "the continuation must deny operator intent explicitly: {text}"
        );
        assert_ne!(
            text, "Continue where you left off.",
            "the bare directive form is exactly what agents mistook for an instruction"
        );
    }

    #[test]
    fn resume_without_operator_content_is_synthetic() {
        // First construction site: no message, no queued follow-up, no attention
        // push — Cairn is waking the job on its own.
        let trigger = resolve_resume_trigger(None, false, false);
        assert!(trigger.synthetic);
        assert_eq!(trigger.message, SYNTHETIC_CONTINUATION_PROMPT);
    }

    #[test]
    fn blank_operator_message_is_no_message() {
        // A whitespace-only message carries no operator intent, so it resolves to
        // the synthetic continuation instead of an empty "You" block.
        let trigger = resolve_resume_trigger(Some("   \n"), false, false);
        assert!(trigger.synthetic);
        assert_eq!(trigger.message, SYNTHETIC_CONTINUATION_PROMPT);
    }

    #[test]
    fn operator_message_resume_keeps_its_attribution() {
        let trigger = resolve_resume_trigger(Some("ship the fix"), false, false);
        assert!(!trigger.synthetic);
        assert_eq!(trigger.message, "ship the fix");
    }

    #[test]
    fn queued_or_pushed_content_leaves_the_trigger_empty() {
        // Queued follow-ups and attention pushes are assembled and stored as their
        // own blocks, so the trigger contributes nothing — and is not synthetic
        // either, because real content did reach this resume.
        for (has_queued, has_pushes) in [(true, false), (false, true), (true, true)] {
            let trigger = resolve_resume_trigger(None, has_queued, has_pushes);
            assert!(trigger.message.is_empty());
            assert!(!trigger.synthetic);
        }
    }

    #[test]
    fn side_channel_and_push_follow_the_immediate_message() {
        // Queued notes lead; the immediate message, then side-channel notices and
        // resolved attention pushes, follow — the established position for those
        // blocks.
        let prompt = assemble_resume_prompt(
            None,
            Some("A".to_string()),
            "B",
            Some("side".to_string()),
            Some("push"),
        );
        assert_eq!(prompt, "A\n\nB\n\nside\n\npush");
    }

    #[test]
    fn self_suspend_note_leads_the_resume_prompt() {
        // On a durable self-suspend resume the note frames the whole turn, so it
        // leads even queued notes — the agent reads "this pause was deliberate"
        // before the awaited result that follows.
        let prompt = assemble_resume_prompt(
            Some("NOTE"),
            Some("A".to_string()),
            "B",
            Some("side".to_string()),
            Some("push"),
        );
        assert_eq!(prompt, "NOTE\n\nA\n\nB\n\nside\n\npush");
    }

    #[test]
    fn absent_self_suspend_note_is_omitted() {
        // A normal (non-suspended) resume passes no note and is unchanged.
        let prompt = assemble_resume_prompt(None, None, "B", None, None);
        assert_eq!(prompt, "B");
    }

    #[test]
    fn fresh_session_is_not_stale() {
        // Under the threshold → native resume (not stale).
        assert!(!is_session_stale(1_000, 1_000, 3_600));
        assert!(!is_session_stale(4_599, 1_000, 3_600));
    }

    #[test]
    fn stale_session_is_detected() {
        // Over the threshold → reseed (stale).
        assert!(is_session_stale(4_601, 1_000, 3_600));
    }

    #[test]
    fn staleness_boundary_is_not_stale() {
        // Exactly at the threshold is NOT stale — the comparison is strict, so a
        // resume right at the edge takes the native path.
        assert!(!is_session_stale(4_600, 1_000, 3_600));
    }

    #[test]
    #[serial_test::serial(reseed_staleness_env)]
    fn staleness_threshold_honors_env_override_then_falls_back() {
        let original = std::env::var("CAIRN_SESSION_STALENESS_SECS").ok();

        std::env::set_var("CAIRN_SESSION_STALENESS_SECS", "30");
        assert_eq!(staleness_threshold_secs(), 30);

        // Blank / unparseable values fall back to the constant.
        std::env::set_var("CAIRN_SESSION_STALENESS_SECS", "  ");
        assert_eq!(staleness_threshold_secs(), SESSION_STALENESS_THRESHOLD_SECS);
        std::env::set_var("CAIRN_SESSION_STALENESS_SECS", "not-a-number");
        assert_eq!(staleness_threshold_secs(), SESSION_STALENESS_THRESHOLD_SECS);

        std::env::remove_var("CAIRN_SESSION_STALENESS_SECS");
        assert_eq!(staleness_threshold_secs(), SESSION_STALENESS_THRESHOLD_SECS);

        match original {
            Some(value) => std::env::set_var("CAIRN_SESSION_STALENESS_SECS", value),
            None => std::env::remove_var("CAIRN_SESSION_STALENESS_SECS"),
        }
    }

    #[test]
    fn reseed_seed_orders_header_digest_then_trigger() {
        // The seed content is HEADER + digest; applying it to the trigger yields
        // HEADER + digest + trigger, in that order (CAIRN-2534).
        let seed_content = build_reseed_seed_content("DIGEST_BODY");
        assert!(seed_content.starts_with(RESEED_SEED_HEADER));
        assert!(seed_content.contains("DIGEST_BODY"));

        let outcome = ReseedOutcome {
            new_session_id: "sess-new".to_string(),
            seed_content: seed_content.clone(),
        };
        let delivered = apply_reseed_seed("TRIGGER".to_string(), Some(&outcome));
        assert_eq!(delivered, format!("{seed_content}\n\nTRIGGER"));
        let header_at = delivered.find(RESEED_SEED_HEADER).unwrap();
        let digest_at = delivered.find("DIGEST_BODY").unwrap();
        let trigger_at = delivered.find("TRIGGER").unwrap();
        assert!(header_at < digest_at && digest_at < trigger_at);
    }

    /// Inputs for a warm session with a readable window, which is the only case
    /// where the decision is interesting.
    fn warm(used: i64, window: i64) -> super::ThreadCompactionInputs {
        super::ThreadCompactionInputs {
            stale: false,
            drops_anything: true,
            occupancy: Some((used, window)),
            threshold: 0.8,
        }
    }

    #[test]
    fn a_warm_session_rebuilds_only_when_its_window_is_running_out() {
        use super::decide_thread_compaction;
        use crate::threads::compaction::CompactionTrigger;

        assert_eq!(decide_thread_compaction(warm(159_000, 200_000)), None);
        assert_eq!(
            decide_thread_compaction(warm(160_000, 200_000)),
            Some(CompactionTrigger::Capacity),
            "the threshold is inclusive: at four fifths of the window, give something up"
        );

        // The threshold is configurable, and it is the only thing that moves the
        // boundary — nothing about the size of what would be dropped does.
        let mut eager = warm(120_000, 200_000);
        assert_eq!(decide_thread_compaction(eager), None);
        eager.threshold = 0.6;
        assert_eq!(
            decide_thread_compaction(eager),
            Some(CompactionTrigger::Capacity)
        );
    }

    #[test]
    fn a_warm_session_never_rebuilds_because_it_would_be_cheaper() {
        use super::decide_thread_compaction;

        // CAIRN-3404's six rotations, all warm, all fired by the old byte rule.
        // Their common feature is that none of them was near the window: the
        // thread was using a fraction of its context and rebuilding anyway,
        // losing its own reasoning six times in one afternoon to reclaim between
        // 1.1 KB and 18 KB. Occupancy is what decides now, so every one holds —
        // and would hold no matter how lopsided the byte counts were.
        for used in [8_000, 20_000, 40_000, 80_000, 120_000, 155_000] {
            assert_eq!(
                decide_thread_compaction(warm(used, 200_000)),
                None,
                "a warm session at {used} tokens of a 200k window gave up its \
                 continuity for a size saving"
            );
        }
    }

    #[test]
    fn a_warm_session_with_no_reading_is_left_alone() {
        use super::decide_thread_compaction;

        // Estimating occupancy from bytes against an assumed window would be a
        // cruder measure standing in for the real one, which is the substitution
        // that made the previous trigger fire on everything. Without a reading
        // the provider's own limit is the backstop, and it fails loudly.
        let mut blind = warm(0, 0);
        blind.occupancy = None;
        assert_eq!(decide_thread_compaction(blind), None);

        // A window the backend reports as zero is not a window at 0% full.
        assert_eq!(decide_thread_compaction(warm(500_000, 0)), None);
    }

    #[test]
    fn expiry_rebuilds_whatever_the_window_says_and_capacity_needs_something_to_drop() {
        use super::decide_thread_compaction;
        use crate::threads::compaction::CompactionTrigger;

        // The cache is gone, so the session is being rebuilt either way and the
        // only question is what from. An unreadable window must not block that,
        // and neither must an empty composition.
        let expired = super::ThreadCompactionInputs {
            stale: true,
            drops_anything: false,
            occupancy: None,
            threshold: 0.8,
        };
        assert_eq!(
            decide_thread_compaction(expired),
            Some(CompactionTrigger::Expiry)
        );

        // While warm, a rotation that replaces nothing spends the thread's
        // continuity and reclaims no window at all — worse than doing nothing,
        // however full the context is.
        let mut nothing_to_drop = warm(199_000, 200_000);
        nothing_to_drop.drops_anything = false;
        assert_eq!(decide_thread_compaction(nothing_to_drop), None);
    }

    #[test]
    fn a_thread_seed_carries_its_own_header_and_stays_a_seed() {
        // The thread seed is framed for a session that never ends, but it is the
        // same header-then-body shape, so it remains an ordinary `user:seed`
        // everywhere downstream.
        let content = super::build_thread_seed_content("## Arc\n\nCOMPOSED_BODY", None);
        assert!(content.starts_with(super::THREAD_SEED_HEADER));
        assert!(content.contains("COMPOSED_BODY"));

        // The bridge line names the gap the rebuilt session cannot feel, so the
        // relative references in the turns below stay readable. A sub-minute gap
        // is omitted, as it is at an ordinary turn boundary.
        let bridged = super::build_thread_seed_content("BODY", Some(3 * 3600 + 12 * 60));
        assert!(
            bridged.contains("Your last turn ended 3h 12m ago"),
            "the rebuild header lost its bridge line: {bridged}"
        );
        assert!(!super::build_thread_seed_content("BODY", Some(59)).contains("last turn ended"));
        assert!(
            !content.starts_with(RESEED_SEED_HEADER),
            "a thread must not be told its session was reconstructed after inactivity"
        );

        let outcome = ReseedOutcome {
            new_session_id: "sess-new".to_string(),
            seed_content: content.clone(),
        };
        assert_eq!(
            apply_reseed_seed("TRIGGER".to_string(), Some(&outcome)),
            format!("{content}\n\nTRIGGER")
        );
    }

    #[test]
    fn apply_reseed_seed_is_identity_without_reseed() {
        // A non-reseed resume delivers the trigger unchanged.
        assert_eq!(apply_reseed_seed("TRIGGER".to_string(), None), "TRIGGER");
    }

    /// Register a warm process with a recorded model and backend. The stdin is an
    /// in-memory writer so a live `send_set_model` (Claude) succeeds in tests.
    fn register_run(
        state: &AgentProcessState,
        run_id: &str,
        model: Option<&str>,
        backend: Option<&str>,
    ) {
        let mut processes = state.processes.lock().unwrap();
        let child = Arc::new(Mutex::new(None));
        let stdin = Arc::new(Mutex::new(Some(wrap_plain_stdin(Box::new(
            Vec::<u8>::new(),
        )))));
        let mut handle = RunHandle::new(child, stdin, Some(format!("sess-{run_id}")), None);
        handle.transition_to_warm();
        handle.model = model.map(|m| m.to_string());
        handle.backend = backend.map(|b| b.to_string());
        processes.register(run_id.to_string(), handle);
    }

    #[test]
    fn codex_reply_with_thread_id_resumes_not_reseeds() {
        // The CAIRN-2598 regression: an open Codex child-task session whose
        // backend matches the requested one and whose native thread id is stored
        // must resume, never rotate to a fresh session (which restarts at the
        // system prompt and wipes history).
        assert_eq!(
            decide_continue_action("codex", Some("thread-existing"), Some("codex"), false),
            ContinueSessionAction::Resume
        );
        // With no desired backend supplied (nothing to compare), a Codex session
        // with a thread id still resumes rather than reseeding.
        assert_eq!(
            decide_continue_action("codex", Some("thread-existing"), None, false),
            ContinueSessionAction::Resume
        );
    }

    #[test]
    fn matching_backend_without_native_handle_retries_startup() {
        // A confirmed Claude session (Claude does not have Codex's native-resume
        // preference) still flows through the cold-resume reseed path.
        assert_eq!(
            decide_continue_action("claude", Some("sess-abc"), Some("claude"), false),
            ContinueSessionAction::MaybeReseed
        );
        // A handle-less open session never completed its backend startup. Retry
        // that startup under the same Cairn session instead of trying to resume
        // an identifier that does not exist.
        assert_eq!(
            decide_continue_action("claude", None, Some("claude"), false),
            ContinueSessionAction::RetryStart
        );
        assert_eq!(
            decide_continue_action("codex", None, Some("codex"), false),
            ContinueSessionAction::RetryStart
        );
    }

    #[test]
    fn cross_backend_model_change_rotates() {
        // This is what the hardcoded-"claude" child session bug produced every
        // reply: desired "codex" vs stored "claude" rotated to a fresh session.
        assert_eq!(
            decide_continue_action("claude", Some("sess"), Some("codex"), false),
            ContinueSessionAction::RotateToBackend("codex".to_string())
        );
    }

    #[test]
    fn prompt_edit_forces_fresh_same_backend_session() {
        // needs_fresh_session wins over native resume: the edited system prompt
        // requires a fresh session even for a Codex thread.
        assert_eq!(
            decide_continue_action("codex", Some("thread-x"), Some("codex"), true),
            ContinueSessionAction::RotateFresh
        );
    }

    #[test]
    fn native_resume_predicate_is_codex_with_handle_only() {
        assert!(session_prefers_native_resume("codex", Some("thread-x")));
        assert!(session_prefers_native_resume("Codex", Some("thread-x")));
        assert!(!session_prefers_native_resume("codex", None));
        assert!(!session_prefers_native_resume("claude", Some("sess")));
    }

    #[test]
    fn matching_model_reuses_without_set_model() {
        let state = AgentProcessState::default();
        register_run(&state, "run-1", Some("opus"), None);
        let decision = ensure_reused_process_model(&state, "run-1", Some("opus")).unwrap();
        assert_eq!(decision, ReuseDecision::Reuse);
        assert_eq!(state.get_model("run-1"), Some("opus".to_string()));
    }

    #[test]
    fn no_desired_model_reuses() {
        let state = AgentProcessState::default();
        register_run(&state, "run-1", Some("opus"), None);
        let decision = ensure_reused_process_model(&state, "run-1", None).unwrap();
        assert_eq!(decision, ReuseDecision::Reuse);
    }

    #[test]
    fn changed_model_requests_restart_when_idle() {
        let state = AgentProcessState::default();
        register_run(&state, "run-1", Some("sonnet"), None);
        let decision = ensure_reused_process_model(&state, "run-1", Some("opus")).unwrap();
        assert_eq!(decision, ReuseDecision::Restart);
        // No unconfirmed in-place switch: tracked model is left unchanged; the
        // fresh process records the new model at startup.
        assert_eq!(state.get_model("run-1"), Some("sonnet".to_string()));
    }

    #[test]
    fn changed_model_requests_restart_for_any_backend() {
        // The helper no longer cares about backend (rotation handles cross-backend
        // upstream); a model change on a same-backend process always restarts.
        let state = AgentProcessState::default();
        register_run(&state, "run-1", Some("gpt-5.4-mini"), Some("codex"));
        let decision = ensure_reused_process_model(&state, "run-1", Some("gpt-5.4")).unwrap();
        assert_eq!(decision, ReuseDecision::Restart);
        assert_eq!(state.get_model("run-1"), Some("gpt-5.4-mini".to_string()));
    }

    #[test]
    fn changed_model_errors_when_busy() {
        let state = AgentProcessState::default();
        register_run(&state, "run-1", Some("opus"), None);
        // Mid-turn: a restart would kill active work → error instead.
        state.begin_turn("run-1", "turn-1");
        let err = ensure_reused_process_model(&state, "run-1", Some("sonnet")).unwrap_err();
        assert!(err.contains("busy"), "unexpected error: {err}");
    }

    // ---- The inherit ladder ---------------------------------------------
    //
    // Where a job that inherits its parent's branch starts is the one place a
    // recorded `jobs.base_commit` can still influence where work begins, so
    // every rung is pinned against a real jj store rather than reasoned about.
    // Two properties matter: a commit the store cannot produce is never chosen,
    // and no rung fails. A job minted at an unresolvable coordinate does not
    // fail here — it fails inside materialization, far from the row that
    // caused it.

    use super::inherited_head;
    use crate::jj::tests::{git, git_stdout, init_project, jj_bin};
    use crate::jj::{create_bookmark_at, ensure_project_store, JjEnv};
    use std::path::PathBuf;
    use tempfile::TempDir;

    /// A store holding two real commits on `main`, with the tempdirs that keep
    /// it alive.
    struct InheritFixture {
        _home: TempDir,
        _project: TempDir,
        jj: JjEnv,
        store: PathBuf,
        first: String,
        second: String,
    }

    /// `None` when jj is not resolvable on this machine, matching the skip
    /// convention the rest of the real-store suites use.
    fn inherit_fixture() -> Option<InheritFixture> {
        let bin = jj_bin()?;
        let home = TempDir::new().unwrap();
        let project = TempDir::new().unwrap();
        init_project(project.path());
        let first = git_stdout(project.path(), &["rev-parse", "HEAD"]);
        std::fs::write(project.path().join("second.rs"), "second\n").unwrap();
        git(project.path(), &["add", "-A"]);
        git(project.path(), &["commit", "-q", "-m", "second"]);
        let second = git_stdout(project.path(), &["rev-parse", "HEAD"]);

        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, project.path()).unwrap();
        Some(InheritFixture {
            _home: home,
            _project: project,
            jj,
            store,
            first,
            second,
        })
    }

    /// A commit id that is well-formed and absent from the store — the shape a
    /// recorded row takes after its commit is abandoned or was never fetched.
    const ABSENT_COMMIT: &str = "0123456789abcdef0123456789abcdef01234567";

    /// Rung one: the store is the authority. A resolvable recorded base does not
    /// get a say while the branch itself still resolves.
    #[test]
    #[serial_test::serial(jj)]
    fn the_live_bookmark_outranks_a_resolvable_recorded_base() {
        let Some(fx) = inherit_fixture() else {
            eprintln!("skipping the_live_bookmark_outranks_a_resolvable_recorded_base: no jj");
            return;
        };
        create_bookmark_at(&fx.jj, &fx.store, "agent/parent", &fx.first).unwrap();

        let head = inherited_head(
            &fx.jj,
            &fx.store,
            "agent/parent",
            Some(&fx.second),
            "main",
            |_| None,
        );

        assert_eq!(
            head, fx.first,
            "the branch's own bookmark is where the branch is; the recorded row is not"
        );
    }

    /// Rung two: the recorded base is used, but only once the branch is gone AND
    /// the store can still produce the commit it names.
    #[test]
    #[serial_test::serial(jj)]
    fn a_resolvable_recorded_base_is_used_once_the_bookmark_is_gone() {
        let Some(fx) = inherit_fixture() else {
            eprintln!(
                "skipping a_resolvable_recorded_base_is_used_once_the_bookmark_is_gone: no jj"
            );
            return;
        };

        let head = inherited_head(
            &fx.jj,
            &fx.store,
            "agent/never-created",
            Some(&fx.first),
            "main",
            |_| None,
        );

        assert_eq!(
            head, fx.first,
            "a recorded commit the store can still produce is a legitimate degraded start"
        );
    }

    /// Rung three, and the whole point of the rung: a recorded commit the store
    /// cannot resolve is refused, not carried forward. This is the case that
    /// used to mint a job at a coordinate materialization would later fail on.
    #[test]
    #[serial_test::serial(jj)]
    fn an_unresolvable_recorded_base_is_refused_for_the_live_base_branch() {
        let Some(fx) = inherit_fixture() else {
            eprintln!("skipping an_unresolvable_recorded_base_is_refused: no jj");
            return;
        };
        let base_tip = fx.second.clone();

        let head = inherited_head(
            &fx.jj,
            &fx.store,
            "agent/never-created",
            Some(ABSENT_COMMIT),
            "main",
            |revision| (revision == "main").then(|| base_tip.clone()),
        );

        assert_ne!(
            head, ABSENT_COMMIT,
            "a commit the store cannot produce must never be handed to a job"
        );
        assert_eq!(
            head, fx.second,
            "the floor is the job's own base branch, resolved live"
        );
    }

    /// The same floor carries a job whose parent recorded nothing at all — the
    /// state every sub-agent task, call, and workflow child is in when its
    /// parent's row was never written.
    #[test]
    #[serial_test::serial(jj)]
    fn an_absent_recorded_base_falls_through_to_the_live_base_branch() {
        let Some(fx) = inherit_fixture() else {
            eprintln!("skipping an_absent_recorded_base_falls_through: no jj");
            return;
        };
        let base_tip = fx.second.clone();

        let head = inherited_head(
            &fx.jj,
            &fx.store,
            "agent/never-created",
            None,
            "main",
            |revision| (revision == "main").then(|| base_tip.clone()),
        );

        assert_eq!(head, fx.second);
    }

    // ---- Mode dispatch ---------------------------------------------------
    //
    // The ladder above answers "where does an inheriting job start?". These
    // answer the question one level up: which jobs ask it at all, and what
    // happens to a job that asks with no lineage to answer from.

    use super::{select_job_coordinate, CoordinateRequest, ParentCoordinate};
    use crate::execution::step_behavior::StepBehavior;

    /// Plain inheritance: degrades through the ladder when the parent's branch
    /// cannot be resolved. The strict grade the delegation edge uses is pinned
    /// beside the node that emits it.
    fn inherits() -> StepBehavior {
        StepBehavior {
            mints_branch: false,
            inherits_branch: true,
            requires_parent_head: false,
        }
    }

    fn child_request(parent_job_id: Option<&str>) -> CoordinateRequest<'_> {
        CoordinateRequest {
            job_id: "child-job",
            parent_job_id,
            existing_branch: None,
            base_ref: "main",
        }
    }

    /// A branchless mode is not "no opinion": it puts the job on its base
    /// branch. That is right for a node authored that way and wrong for a
    /// delegated task, which is the substance of CAIRN-3309.
    #[test]
    #[serial_test::serial(jj)]
    fn a_branchless_mode_stays_on_the_base_branch() {
        let Some(fx) = inherit_fixture() else {
            eprintln!("skipping a_branchless_mode_stays_on_the_base_branch: no jj");
            return;
        };
        create_bookmark_at(&fx.jj, &fx.store, "agent/parent", &fx.first).unwrap();
        let base_tip = fx.second.clone();

        let (branch, base_commit) = select_job_coordinate(
            &StepBehavior {
                mints_branch: false,
                inherits_branch: false,
                requires_parent_head: false,
            },
            child_request(Some("parent-job")),
            &fx.jj,
            &fx.store,
            |_| unreachable!("a branchless job never reads its parent's coordinate"),
            || unreachable!("a branchless job mints nothing"),
            move |revision| (revision == "main").then(|| base_tip.clone()),
        )
        .unwrap();

        assert_eq!(branch, None, "the job owns no branch");
        assert_eq!(
            base_commit.as_deref(),
            Some(fx.second.as_str()),
            "it starts at its base branch, regardless of where any parent's branch sits"
        );
    }

    /// Inheritance is a hard requirement, not a preference. A parent whose
    /// branch is missing leaves nothing to seed from, and the refusal names the
    /// parent and what it lacks so the broken edge is legible from the message.
    #[test]
    #[serial_test::serial(jj)]
    fn a_parent_without_a_branch_refuses_the_spawn() {
        let Some(fx) = inherit_fixture() else {
            eprintln!("skipping a_parent_without_a_branch_refuses_the_spawn: no jj");
            return;
        };

        let error = select_job_coordinate(
            &inherits(),
            child_request(Some("parent-job")),
            &fx.jj,
            &fx.store,
            |_| {
                Ok(ParentCoordinate {
                    branch: None,
                    recorded_base: Some(fx.first.clone()),
                })
            },
            || unreachable!("an inheriting job mints nothing"),
            |_| None,
        )
        .expect_err("a child with no parent branch must not fall through to base");

        assert!(error.contains("parent job parent-job"), "{error}");
        assert!(error.contains("no branch"), "{error}");
        assert!(error.contains("child-job"), "{error}");
    }

    /// The same refusal one step earlier: a job that inherits but was never
    /// re-parented has no edge to follow at all.
    #[test]
    #[serial_test::serial(jj)]
    fn missing_lineage_refuses_the_spawn() {
        let Some(fx) = inherit_fixture() else {
            eprintln!("skipping missing_lineage_refuses_the_spawn: no jj");
            return;
        };

        let error = select_job_coordinate(
            &inherits(),
            child_request(None),
            &fx.jj,
            &fx.store,
            |_| unreachable!("there is no parent to load"),
            || unreachable!("an inheriting job mints nothing"),
            |_| None,
        )
        .expect_err("an inheriting job with no parent_job_id must not fall through to base");

        assert!(error.contains("parent_job_id"), "{error}");
    }

    /// The ladder has no failing rung. With the branch gone, nothing recorded,
    /// an unresolvable base ref, and a git that answers nothing, it still yields
    /// a coordinate — jj's always-present root commit. Refusing to start over
    /// substrate state is what the operator ruling forbids.
    #[test]
    #[serial_test::serial(jj)]
    fn the_ladder_yields_a_coordinate_even_when_every_rung_is_unresolvable() {
        let Some(fx) = inherit_fixture() else {
            eprintln!("skipping the_ladder_yields_a_coordinate_even_when_every_rung_fails: no jj");
            return;
        };

        let head = inherited_head(
            &fx.jj,
            &fx.store,
            "agent/never-created",
            Some(ABSENT_COMMIT),
            "no-such-base-branch",
            |_| None,
        );

        assert_eq!(
            head, "root()",
            "the ladder degrades to jj's root commit rather than failing the spawn"
        );
    }
}
