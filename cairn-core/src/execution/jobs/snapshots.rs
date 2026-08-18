use super::*;

pub(super) struct AgentSnapshotData {
    pub(super) agent: Option<AgentConfig>,
}

/// Store a queued user message while preserving its durable queue identity.
pub(crate) fn store_queued_user_event_with_turn(
    orch: &Orchestrator,
    run_id: &str,
    session_id: &str,
    queued_message_id: &str,
    content: &str,
    now: i32,
    turn_id: Option<&str>,
) -> Result<(), String> {
    store_transcript_event_with_turn(
        orch,
        run_id,
        session_id,
        now,
        turn_id,
        queued_user_transcript_event(session_id, queued_message_id, content),
        &[],
    )
    .map(|_| ())
}

pub(crate) fn queued_user_transcript_event(
    session_id: &str,
    queued_message_id: &str,
    content: &str,
) -> TranscriptEvent {
    TranscriptEvent {
        event_type: "user".to_string(),
        session_id: Some(session_id.to_string()),
        parent_tool_use_id: None,
        content: Some(content.to_string()),
        thinking: None,
        tool_name: None,
        tool_input: None,
        tool_uses: None,
        tool_use_id: None,
        tool_result: None,
        is_error: false,
        thinking_ms: None,
        queued_message_id: Some(queued_message_id.to_string()),
        raw: Some(serde_json::json!({ "queued_message_id": queued_message_id })),
    }
}

pub(super) async fn load_agent_snapshot_data(
    db: Arc<LocalDb>,
    execution_id: String,
    agent_config_id: String,
) -> Result<AgentSnapshotData, String> {
    db.read(|conn| {
        let execution_id = execution_id.clone();
        let agent_config_id = agent_config_id.clone();
        Box::pin(async move {
            let snapshot = load_execution_snapshot_conn(conn, &execution_id).await?;
            let Some(snapshot) = snapshot else {
                return Ok(AgentSnapshotData { agent: None });
            };
            // Carry the snapshot's concrete atomic selection + extras straight
            // through to the runtime AgentConfig — no re-resolution.
            let agent = snapshot
                .agents
                .get(&agent_config_id)
                .map(|agent: &AgentSnapshot| AgentConfig {
                    id: agent.id.clone(),
                    name: agent.name.clone(),
                    description: agent.description.clone(),
                    prompt: agent.prompt.clone(),
                    tools: agent.tools.clone(),
                    tier: agent
                        .tier
                        .clone()
                        .or_else(|| agent.selection.as_ref().map(|s| s.model.clone())),
                    workspace_id: None,
                    project_id: None,
                    created_at: snapshot.created_at as i32,
                    updated_at: snapshot.created_at as i32,
                    disallowed_tools: agent.disallowed_tools.clone(),
                    skills: agent.skills.clone(),
                    fence: agent.fence,
                    backend_preference: agent.backend_preference.clone(),
                    icon: None,
                    selection: agent.selection.clone(),
                    extras: agent.extras.clone(),
                });
            Ok(AgentSnapshotData { agent })
        })
    })
    .await
    .map_err(|e| db_error("Failed to load execution snapshot", e))
}

/// Whether a host execution's agent snapshot still tracks the agent definition
/// it was resolved from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SnapshotFreshness {
    /// Re-resolve an unedited snapshot from the agent files. A thread session is
    /// a single job that lives for months, so freezing its agent at creation
    /// would mean an edit to `thread.md` never reaches any thread that already
    /// exists — which is exactly the reach a Thread prompt change ships for. An
    /// explicit edit (`AgentSnapshot::edited_at`) ends the tracking.
    Tracking,
    /// Never re-resolve. An execution's snapshot IS its reproducibility
    /// guarantee: what ran is what was captured when it started.
    Frozen,
}

impl SnapshotFreshness {
    /// Derive freshness from job ownership. A thread's session job is the only
    /// long-lived host; every other job belongs to an execution and stays
    /// frozen.
    pub(crate) fn for_job(job: &DbJob) -> Self {
        match crate::threads::owner_of_job(job) {
            crate::threads::JobOwner::Thread => SnapshotFreshness::Tracking,
            crate::threads::JobOwner::Issue | crate::threads::JobOwner::Unknown => {
                SnapshotFreshness::Frozen
            }
        }
    }
}

/// Guarantee that this job's host execution exists and that its snapshot carries
/// the job's OWN agent, then return the execution id.
///
/// The walk that resolves a run's fence (`resolve_fence_policy`) and the OS
/// sandbox policy beside it both go run → job → execution → snapshot →
/// `agents[agent_config_id]`. That walk can only answer for an agent some
/// snapshot names. A thread session job is created with `execution_id` NULL, and
/// its first delegation stamps on a synthetic execution whose `agents` map holds
/// only the DELEGATED agents — so a thread's own agent appeared in no snapshot
/// anywhere and the walk resolved to nothing. Rather than teach the walk about
/// threads, this puts the snapshot where the walk already looks, which is why
/// `permission.rs`, `run/sandbox_policy.rs`, and `config_loading.rs` need no
/// thread-specific branch.
///
/// Four cases, in order: no execution at all (create one); an execution whose
/// snapshot lacks this agent (repair it IN PLACE, so a thread that has already
/// delegated keeps its `delegated_packets`); an unedited snapshot on a tracking
/// host (re-resolve); an edited snapshot (leave alone). Idempotent, and a no-op
/// for a job whose execution already snapshots it.
pub(crate) async fn ensure_host_agent_snapshot(
    orch: &Orchestrator,
    job: &DbJob,
    project_path: Option<&Path>,
    freshness: SnapshotFreshness,
) -> Result<Option<String>, String> {
    let Some(execution_id) = job.execution_id.clone() else {
        return create_host_execution(orch, job, job.agent_config_id.as_deref(), project_path)
            .await
            .map(Some);
    };
    // A job with no agent of its own (a workflow run, say) still wants a host to
    // book its packets in; there is simply nothing to snapshot for it.
    let Some(agent_config_id) = job.agent_config_id.clone() else {
        return Ok(Some(execution_id));
    };

    let db = crate::execution::routing::owning_db_for_execution(&orch.db, &execution_id)
        .await
        .map_err(|e| e.to_string())?;
    // Serialize the read-modify-write against `persist_task_packet`, which books
    // delegated packets into this same snapshot.
    let lock = orch.execution_lock(&execution_id);
    let _guard = lock.lock().await;

    let loaded = db
        .read({
            let execution_id = execution_id.clone();
            move |conn| {
                let execution_id = execution_id.clone();
                Box::pin(async move { load_execution_snapshot_conn(conn, &execution_id).await })
            }
        })
        .await
        .map_err(|e| db_error("Failed to load execution snapshot", e))?;
    let Some(mut snapshot) = loaded else {
        return Ok(Some(execution_id));
    };

    let stored = snapshot.agents.get(&agent_config_id).cloned();
    let refresh = match &stored {
        None => true,
        Some(agent) => freshness == SnapshotFreshness::Tracking && agent.edited_at.is_none(),
    };
    if !refresh {
        return Ok(Some(execution_id));
    }

    // Establishing the invariant is loud; keeping it CURRENT is not. A stored
    // snapshot is already a complete answer for the fence walk, so an agent
    // definition that momentarily will not resolve — renamed, mid-edit, a config
    // root not mounted — must not brick the turn. It just stops tracking until
    // the definition resolves again. A snapshot that does not exist yet has no
    // such fallback, and that case propagates below.
    let resolved = match resolve_host_agent(
        &orch.config_dir,
        &agent_config_id,
        job.model.as_deref(),
        project_path,
    ) {
        Ok(resolved) => resolved,
        Err(error) if stored.is_some() => {
            log::warn!(
                "Keeping the stored snapshot for agent '{agent_config_id}' on execution {execution_id}: {error}"
            );
            return Ok(Some(execution_id));
        }
        Err(error) => return Err(error),
    };
    if stored
        .as_ref()
        .is_some_and(|stored| agent_snapshots_match(stored, &resolved))
    {
        return Ok(Some(execution_id));
    }

    for (skill_id, skill) in host_skill_snapshots(&orch.config_dir, &resolved, project_path) {
        snapshot.skills.entry(skill_id).or_insert(skill);
    }
    snapshot.agents.insert(agent_config_id, resolved);
    let snapshot_json = snapshot.to_json()?;
    write_execution_snapshot(&db, &execution_id, snapshot_json).await?;
    Ok(Some(execution_id))
}

/// [`ensure_host_agent_snapshot`] addressed by job id, for a caller that holds
/// no loaded job — the agent-snapshot editor, which needs a snapshot to edit
/// before the job's first turn has created one.
///
/// A thread's session job is created with `execution_id` NULL and acquires its
/// host on its first launch. Without this, configuring a thread would only be
/// possible after it had already taken a turn on the defaults, which is the one
/// moment configuring it is least useful. The operation is the launch funnel's
/// own, and it is idempotent, so opening the editor early simply establishes
/// what the first turn would have.
pub async fn ensure_job_agent_snapshot(
    orch: &Orchestrator,
    job_id: &str,
) -> Result<Option<String>, String> {
    let owning = crate::execution::routing::owning_db_for_job(&orch.db, job_id)
        .await
        .map_err(|e| e.to_string())?;
    let job = load_job(owning, job_id.to_string(), "Job not found").await?;
    let project_path = load_project_path(orch.db.clone(), job.project_id.clone()).await?;
    let freshness = SnapshotFreshness::for_job(&job);
    ensure_host_agent_snapshot(orch, &job, project_path.as_deref(), freshness).await
}

/// Create the passive host execution for a job that owns no recipe, seeded with
/// the job's own agent, and point the job at it.
///
/// The shape is the synthetic delegation host's: `seq` and `issue_id` only for
/// an issue-owned job, `project_id` only otherwise, and a deliberately EMPTY
/// recipe — DAG advancement then sees nothing to run, so a host execution never
/// schedules anything of its own.
async fn create_host_execution(
    orch: &Orchestrator,
    job: &DbJob,
    agent_config_id: Option<&str>,
    project_path: Option<&Path>,
) -> Result<String, String> {
    let db = crate::execution::routing::owning_db_for_job(&orch.db, &job.id)
        .await
        .map_err(|e| e.to_string())?;
    // The caller's job row may be a moment stale; a host created since is the
    // one to use, never a second one.
    if let Some(existing) = db
        .query_opt_text(
            "SELECT execution_id FROM jobs WHERE id = ?1",
            params![job.id.as_str()],
        )
        .await
        .map_err(|e| db_error("Failed to load job execution context", e))?
    {
        return Ok(existing);
    }

    let mut agents = HashMap::new();
    let mut skills = HashMap::new();
    if let Some(agent_config_id) = agent_config_id {
        let agent = resolve_host_agent(
            &orch.config_dir,
            agent_config_id,
            job.model.as_deref(),
            project_path,
        )?;
        skills = host_skill_snapshots(&orch.config_dir, &agent, project_path);
        agents.insert(agent_config_id.to_string(), agent);
    }
    let now = chrono::Utc::now().timestamp();
    let snapshot = ExecutionSnapshot {
        recipe: crate::models::RecipeSnapshot {
            id: format!("host-{}", job.id),
            name: "Session Host".to_string(),
            description: Some("Passive host execution for a job that owns no recipe".to_string()),
            trigger: crate::models::RecipeTrigger::Manual,
            nodes: vec![],
            edges: vec![],
        },
        agents,
        skills,
        trigger_context: crate::models::TriggerContext {
            issue_id: job.issue_id.clone(),
            project_id: job.project_id.clone(),
            trigger_type: crate::models::TriggerType::Manual,
            event_payload: None,
            initiated_via: None,
        },
        presets: None,
        delegated_packets: vec![],
        branch_target: Default::default(),
        // A host execution runs no agent node of its own, so there is nothing for
        // a routing table to have decided.
        model_routing: None,
        created_at: now,
    };
    let snapshot_json = snapshot.to_json()?;

    let seq = match job.issue_id.as_deref() {
        Some(issue_id) => Some(next_execution_seq(&db, issue_id).await?),
        None => None,
    };
    let execution_id = ids::mint_child(&job.id);
    insert_host_execution(
        &db,
        &execution_id,
        &snapshot.recipe.id,
        job,
        now as i32,
        snapshot_json,
        seq,
    )
    .await?;
    Ok(execution_id)
}

/// Resolve a host job's agent from configuration files, through the same chain
/// `config_loading` uses: config dir → effective presets → file agent →
/// [`resolve_agent_snapshot`], the central function all `AgentSnapshot`
/// construction goes through.
///
/// Loud on failure: a snapshot with an empty prompt reads as a placeholder
/// downstream, so an unresolvable agent must propagate rather than be written.
///
/// `jobs.model` stays the authoritative launch fact — `effective_backend_name`
/// deliberately prefers it over the config's selection (CAIRN-3798) — so when it
/// is set the snapshot's `selection` is DERIVED from it. That is what makes the
/// two agree instead of compete, and it is what a per-thread agent editor
/// displays. Because an unedited host snapshot re-resolves every turn, a model
/// change on the job flows into the snapshot on the next turn with no second
/// write path.
fn resolve_host_agent(
    config_dir: &Path,
    agent_config_id: &str,
    job_model: Option<&str>,
    project_path: Option<&Path>,
) -> Result<AgentSnapshot, String> {
    let presets = load_effective_presets(config_dir, project_path);
    let file_agent = config_agents::get_agent(config_dir, agent_config_id, project_path)
        .map_err(|e| format!("Failed to load agent config '{agent_config_id}': {e}"))?
        .ok_or_else(|| format!("Agent config '{agent_config_id}' not found"))?;
    let override_selection = job_model.map(|model| {
        LaunchSelectionOverride::Concrete(crate::models::ModelSelection::new(
            crate::backends::resolved_backend_for_model(model),
            Model::new(model),
        ))
    });
    resolve_agent_snapshot(&file_agent, override_selection.as_ref(), &presets)
}

/// Skill snapshots for a host execution: only the skills the agent explicitly
/// names.
///
/// The runtime reads `AgentSnapshot.skills`; this map exists for display parity
/// with recipe executions. A tracking host rewrites its snapshot whenever the
/// agent definition moves, and copying every skill's full prompt in each time
/// would be a large write for a field nothing at runtime reads. An agent that
/// names none inherits all of them, which is not an enumerable reference.
fn host_skill_snapshots(
    config_dir: &Path,
    agent: &AgentSnapshot,
    project_path: Option<&Path>,
) -> HashMap<String, crate::models::SkillSnapshot> {
    let mut out = HashMap::new();
    let Some(skill_ids) = agent.skills.as_ref().filter(|ids| !ids.is_empty()) else {
        return out;
    };
    let Ok(file_skills) = crate::config::skills::list_skills(config_dir, project_path) else {
        return out;
    };
    for result in file_skills {
        let ConfigResult::Ok(skill) = result else {
            continue;
        };
        if !skill_ids.iter().any(|id| id == &skill.id) {
            continue;
        }
        // config_root_subdirs yields project first, so the first occurrence of an
        // id wins and project skills shadow workspace ones.
        out.entry(skill.id.clone())
            .or_insert(crate::models::SkillSnapshot {
                id: skill.id,
                name: skill.name,
                description: skill.description,
                prompt: skill.prompt,
                allowed_tools: skill.allowed_tools,
            });
    }
    out
}

/// Whether a stored agent snapshot already says what a fresh resolution says.
/// Compared through their serialized form because `AgentSnapshot` carries an
/// inline-schema-bearing shape that cannot derive `Eq`.
fn agent_snapshots_match(stored: &AgentSnapshot, resolved: &AgentSnapshot) -> bool {
    match (serde_json::to_value(stored), serde_json::to_value(resolved)) {
        (Ok(stored), Ok(resolved)) => stored == resolved,
        _ => false,
    }
}

async fn next_execution_seq(db: &LocalDb, issue_id: &str) -> Result<i32, String> {
    let issue_id = issue_id.to_string();
    let max = db
        .read(move |conn| {
            let issue_id = issue_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT MAX(seq) FROM executions WHERE issue_id = ?1",
                        params![issue_id.as_str()],
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => row.opt_i64(0),
                    None => Ok(None),
                }
            })
        })
        .await
        .map_err(|e| db_error("Failed to resolve next execution seq", e))?;
    Ok(max.map(|value| value as i32 + 1).unwrap_or(1))
}

/// Insert a host execution and point its job at it, in one transaction.
async fn insert_host_execution(
    db: &LocalDb,
    execution_id: &str,
    recipe_id: &str,
    job: &DbJob,
    now: i32,
    snapshot_json: String,
    seq: Option<i32>,
) -> Result<(), String> {
    let execution_id = execution_id.to_string();
    let recipe_id = recipe_id.to_string();
    let issue_id = job.issue_id.clone();
    // An issue-owned host hangs off its issue; a thread-owned one has no issue to
    // hang off, so it is anchored to the project instead.
    let project_id = job.issue_id.is_none().then(|| job.project_id.clone());
    let job_id = job.id.clone();
    db.write(move |conn| {
        let execution_id = execution_id.clone();
        let recipe_id = recipe_id.clone();
        let issue_id = issue_id.clone();
        let project_id = project_id.clone();
        let job_id = job_id.clone();
        let snapshot_json = snapshot_json.clone();
        Box::pin(async move {
            conn.execute(
                "
                INSERT INTO executions (
                    id, recipe_id, issue_id, project_id, status, started_at,
                    completed_at, snapshot, seq, initiator_sub,
                    initiator_org_id, triggered_by
                )
                VALUES (?1, ?2, ?3, ?4, 'running', ?5, NULL, ?6, ?7, NULL, NULL, 'manual')
                ",
                (
                    execution_id.as_str(),
                    recipe_id.as_str(),
                    issue_id.as_deref(),
                    project_id.as_deref(),
                    now,
                    snapshot_json.as_str(),
                    seq,
                ),
            )
            .await?;
            conn.execute(
                "UPDATE jobs SET execution_id = ?1 WHERE id = ?2",
                (execution_id.as_str(), job_id.as_str()),
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|e| db_error("Failed to create host execution", e))
}

async fn write_execution_snapshot(
    db: &LocalDb,
    execution_id: &str,
    snapshot_json: String,
) -> Result<(), String> {
    let execution_id = execution_id.to_string();
    db.write(move |conn| {
        let execution_id = execution_id.clone();
        let snapshot_json = snapshot_json.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE executions SET snapshot = ?1 WHERE id = ?2",
                (snapshot_json.as_str(), execution_id.as_str()),
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|e| db_error("Failed to persist host execution snapshot", e))
}

/// Returns the id of the event it inserted, which for a push-carrying event is
/// also the delivery's identity: it is what the stamped rows point at, and
/// therefore the one handle that can undo the delivery whole.
fn store_transcript_event_with_turn(
    orch: &Orchestrator,
    run_id: &str,
    session_id: &str,
    now: i32,
    turn_id: Option<&str>,
    transcript_event: TranscriptEvent,
    push_ids: &[String],
) -> Result<String, String> {
    let event_id = ids::mint_child(run_id);
    let event_type = transcript_event.event_type.clone();
    let event_data = transcript_event.observed().to_event_json();
    let turn_id = turn_id.map(str::to_string);

    let event = EventInsert {
        id: event_id.clone(),
        run_id: run_id.to_string(),
        session_id: Some(session_id.to_string()),
        timestamp: now,
        event_type: event_type.clone(),
        data: event_data.clone(),
        parent_tool_use_id: None,
        created_at: now,
        input_tokens: None,
        cache_read_tokens: None,
        cache_create_tokens: None,
        output_tokens: None,
        thinking_tokens: None,
        turn_id: turn_id.clone(),
        cost_usd: None,
    };
    // Route the event INSERT to the run's owning replica (fail-closed): a team
    // run's transcript lives wholly in its synced DB, never private.
    let owning = crate::storage::run_db_blocking({
        let dbs = orch.db.clone();
        let run_id = run_id.to_string();
        move || async move {
            crate::execution::routing::owning_db_for_run(&dbs, &run_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;
    // CAIRN-1881: when this event carries attention pushes, stamp them delivered
    // in the same transaction as the event INSERT (atomic delivery seam).
    if push_ids.is_empty() {
        insert_event(owning, event)?;
    } else {
        insert_event_stamping_pushes(owning, event, push_ids.to_vec())?;
    }

    let _ = orch.services.emitter.emit(
        "db-change",
        crate::notify::event_db_change_for_run(
            orch.db.local.clone(),
            run_id,
            Some(session_id),
            "insert",
        ),
    );

    Ok(event_id)
}

/// Store a user event in the transcript with an explicit turn_id.
pub(crate) fn store_user_event_with_turn(
    orch: &Orchestrator,
    run_id: &str,
    session_id: &str,
    content: &str,
    now: i32,
    turn_id: Option<&str>,
) -> Result<(), String> {
    store_user_like_event_with_turn(orch, run_id, session_id, content, now, turn_id, "user")
}

/// Store the cold-resume seed as a `user:seed` event (CAIRN-2534).
///
/// Same shape as a user event but with the namespaced `user:seed` type so the
/// digest renderer collapses it to one line and the frontend draws a divider
/// instead of a giant bubble. Stored ahead of the trigger's own `user` event so
/// the trigger stays a verbatim, visible user message.
pub(crate) fn store_seed_event_with_turn(
    orch: &Orchestrator,
    run_id: &str,
    session_id: &str,
    content: &str,
    now: i32,
    turn_id: Option<&str>,
) -> Result<(), String> {
    store_user_like_event_with_turn(orch, run_id, session_id, content, now, turn_id, "user:seed")
}

/// Store a job's opening prompt as a `user:launch` event (CAIRN-3408).
///
/// Every path that seeds a fresh run's transcript with the prompt that job was
/// started on routes here: the node launch, an ephemeral agent call and its
/// restart, a sub-task, and a workflow invocation. None of that text was typed
/// by the operator — it is composed from the issue's resolved inputs, or written
/// by the agent that spawned the child — so it is stored under the namespaced
/// type rather than plain `user`, and no projection that renders `user` as a
/// person can reach it.
pub fn store_launch_event_with_turn(
    orch: &Orchestrator,
    run_id: &str,
    session_id: &str,
    content: &str,
    now: i32,
    turn_id: Option<&str>,
) -> Result<(), String> {
    store_user_like_event_with_turn(
        orch,
        run_id,
        session_id,
        content,
        now,
        turn_id,
        crate::transcripts::LAUNCH_EVENT_TYPE,
    )
}

/// [`store_launch_event_with_turn`] against the run's current turn, for the
/// launch paths that create their turn after seeding the transcript.
pub(crate) fn store_launch_event(
    orch: &Orchestrator,
    run_id: &str,
    session_id: &str,
    content: &str,
    now: i32,
) -> Result<(), String> {
    let current_turn = orch.process_state.get_current_turn_id(run_id);
    store_launch_event_with_turn(
        orch,
        run_id,
        session_id,
        content,
        now,
        current_turn.as_deref(),
    )
}

/// Store a Cairn-synthesized resume nudge as a `user:continuation` event.
///
/// A resume that carries no operator content at all — no message, no queued
/// follow-up, no attention push — still needs a prompt to wake the agent, and
/// Cairn writes that prompt itself. Storing it under the namespaced
/// `user:continuation` type (the `user:seed` pattern) is what keeps every
/// downstream surface from attributing Cairn's own nudge to the operator: the
/// transcript projection collapses it to a marker line and the frontend draws
/// system framing instead of a "You" bubble.
pub fn store_continuation_event_with_turn(
    orch: &Orchestrator,
    run_id: &str,
    session_id: &str,
    content: &str,
    now: i32,
    turn_id: Option<&str>,
) -> Result<(), String> {
    store_user_like_event_with_turn(
        orch,
        run_id,
        session_id,
        content,
        now,
        turn_id,
        crate::transcripts::CONTINUATION_EVENT_TYPE,
    )
}

/// Shared storage path for a user-slot transcript event (`user` and its
/// `user:seed` / `user:continuation` / `user:launch` siblings): build the
/// `TranscriptEvent` with the given `event_type` and persist it.
#[allow(clippy::too_many_arguments)]
fn store_user_like_event_with_turn(
    orch: &Orchestrator,
    run_id: &str,
    session_id: &str,
    content: &str,
    now: i32,
    turn_id: Option<&str>,
    event_type: &str,
) -> Result<(), String> {
    let transcript_event = TranscriptEvent {
        event_type: event_type.to_string(),
        session_id: Some(session_id.to_string()),
        parent_tool_use_id: None,
        content: Some(content.to_string()),
        thinking: None,
        tool_name: None,
        tool_input: None,
        tool_uses: None,
        tool_use_id: None,
        tool_result: None,
        is_error: false,
        thinking_ms: None,
        queued_message_id: None,
        raw: None,
    };
    store_transcript_event_with_turn(
        orch,
        run_id,
        session_id,
        now,
        turn_id,
        transcript_event,
        &[],
    )
    .map(|_| ())
}

/// Store a synthetic `tool_result` event in the transcript, attached to an
/// existing tool call by `tool_use_id` and `turn_id`.
///
/// Used by the slow-path (>45s) prompt resume to render the answer in place
/// under the originating Question (`write`) call, mirroring what the fast path
/// gets from the CLI's own tool_result. Written directly to the DB so it is not
/// affected by the host-interrupt suppression gate in the reader thread.
#[allow(clippy::too_many_arguments)]
pub fn store_tool_result_event_with_turn(
    orch: &Orchestrator,
    run_id: &str,
    session_id: &str,
    tool_use_id: &str,
    content: &str,
    is_error: bool,
    now: i32,
    turn_id: Option<&str>,
) -> Result<(), String> {
    store_tool_result_event_with_resolution(
        orch,
        run_id,
        session_id,
        tool_use_id,
        content,
        is_error,
        now,
        turn_id,
        None,
    )
}

/// Store a synthetic tool result with the canonical receipt that completed its ask.
#[allow(clippy::too_many_arguments)]
pub fn store_tool_result_event_with_resolution(
    orch: &Orchestrator,
    run_id: &str,
    session_id: &str,
    tool_use_id: &str,
    content: &str,
    is_error: bool,
    now: i32,
    turn_id: Option<&str>,
    resolution: Option<&cairn_db::models::ResolutionReceipt>,
) -> Result<(), String> {
    let transcript_event = TranscriptEvent {
        event_type: "tool_result".to_string(),
        session_id: Some(session_id.to_string()),
        parent_tool_use_id: None,
        content: None,
        thinking: None,
        tool_name: None,
        tool_input: None,
        tool_uses: None,
        tool_use_id: Some(tool_use_id.to_string()),
        tool_result: Some(content.to_string()),
        is_error,
        thinking_ms: None,
        queued_message_id: None,
        raw: resolution.map(|receipt| serde_json::json!({ "resolution": receipt })),
    };
    store_transcript_event_with_turn(
        orch,
        run_id,
        session_id,
        now,
        turn_id,
        transcript_event,
        &[],
    )
    .map(|_| ())
}

/// Persist a single carrying event for drained attention pushes and stamp each
/// push delivered by it, atomically in the event-insert transaction
/// (CAIRN-1881). The rendered push text rides in `content`; recovery redelivers
/// only pushes whose carrying event never durably landed.
///
/// Returns the carrying event's id. The caller holds it until the launch reaches
/// a process, because until then this delivery may still have to be undone — see
/// [`crate::orchestrator::attention_push::revert_delivery`].
pub(crate) fn store_attention_push_event(
    orch: &Orchestrator,
    run_id: &str,
    session_id: &str,
    content: &str,
    push_ids: &[String],
    now: i32,
    turn_id: Option<&str>,
) -> Result<String, String> {
    // CAIRN-1891: the carrying event for a resumed-into wake renders through the
    // wake-card formatter; `content` is the structured `{active, catchup}` JSON.
    let transcript_event = TranscriptEvent {
        event_type: "attention:briefing".to_string(),
        session_id: Some(session_id.to_string()),
        parent_tool_use_id: None,
        content: Some(content.to_string()),
        thinking: None,
        tool_name: None,
        tool_input: None,
        tool_uses: None,
        tool_use_id: None,
        tool_result: None,
        is_error: false,
        thinking_ms: None,
        queued_message_id: None,
        raw: None,
    };
    store_transcript_event_with_turn(
        orch,
        run_id,
        session_id,
        now,
        turn_id,
        transcript_event,
        push_ids,
    )
}

#[cfg(test)]
mod host_snapshot_tests {
    use super::*;
    use crate::db::DbState;
    use crate::models::Fence;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{migrated_test_db, SearchIndex};
    use std::sync::Arc;

    /// The shipped `thread.md` in miniature: the fence declaration is the part
    /// under test, and `tier: lg` resolves against the built-in preset defaults
    /// with no settings file.
    fn thread_agent(prompt: &str) -> String {
        format!(
            "---\nname: Thread\ndescription: Holds an ongoing topic\ntools: mcp__cairn__read, mcp__cairn__run\ntier: lg\nfence: allow\n---\n\n{prompt}\n"
        )
    }

    struct Fixture {
        _temp: tempfile::TempDir,
        db: Arc<LocalDb>,
        orch: Orchestrator,
        agents_dir: PathBuf,
    }

    impl Fixture {
        fn write_agent(&self, id: &str, markdown: &str) {
            std::fs::write(self.agents_dir.join(format!("{id}.md")), markdown).unwrap();
        }

        async fn job(&self, job_id: &str) -> DbJob {
            let job_id = job_id.to_string();
            self.db
                .read(move |conn| {
                    let job_id = job_id.clone();
                    Box::pin(async move { load_job_conn(conn, &job_id).await })
                })
                .await
                .unwrap()
                .unwrap()
        }

        async fn job_execution_id(&self, job_id: &str) -> Option<String> {
            self.db
                .query_opt_text(
                    "SELECT execution_id FROM jobs WHERE id = ?1",
                    params![job_id],
                )
                .await
                .unwrap()
        }

        async fn snapshot(&self, execution_id: &str) -> ExecutionSnapshot {
            let json = self
                .db
                .query_opt_text(
                    "SELECT snapshot FROM executions WHERE id = ?1",
                    params![execution_id],
                )
                .await
                .unwrap()
                .expect("execution has a snapshot");
            crate::config::snapshot_migrate::load(&json).unwrap()
        }

        /// Run the ensure and return the execution id it settled on.
        async fn ensure(&self, job_id: &str, freshness: SnapshotFreshness) -> String {
            let job = self.job(job_id).await;
            ensure_host_agent_snapshot(&self.orch, &job, None, freshness)
                .await
                .unwrap()
                .expect("host execution")
        }
    }

    async fn fixture(name: &str) -> Fixture {
        let temp = tempfile::tempdir().unwrap();
        let config_dir = temp.path().join("config");
        let agents_dir = config_dir.join("agents");
        std::fs::create_dir_all(&agents_dir).unwrap();
        std::fs::write(
            agents_dir.join("thread.md"),
            thread_agent("Hold the topic."),
        )
        .unwrap();

        let db = Arc::new(migrated_test_db(name).await);
        let search = Arc::new(SearchIndex::open_or_create(temp.path().join("search")).unwrap());
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(db.clone(), search)),
            Arc::new(TestServicesBuilder::new().build()),
            config_dir,
        )
        .build();

        db.execute(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('p', 'default', 'P', 'p', '/tmp/p', 1, 1)",
            (),
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO threads (id, project_id, name, status, created_at, updated_at)
             VALUES ('t', 'p', 'general', 'active', 1, 1)",
            (),
        )
        .await
        .unwrap();

        Fixture {
            _temp: temp,
            db,
            orch,
            agents_dir,
        }
    }

    /// A thread session job as `ensure_thread_session` creates it: `execution_id`
    /// NULL, agent `thread`, and the model override living on `jobs.model`.
    async fn seed_thread_session(fx: &Fixture, model: Option<&str>) {
        fx.db
            .execute(
                "INSERT INTO jobs (
                    id, thread_id, project_id, status, agent_config_id, current_session_id,
                    node_name, uri_segment, model, created_at, updated_at
                 ) VALUES ('j', 't', 'p', 'idle', 'thread', 's', 'thread', 'thread', ?1, 1, 1)",
                params![model],
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn fresh_thread_session_gets_an_execution_carrying_its_own_agent() {
        let fx = fixture("host-snapshot-fresh-thread.db").await;
        seed_thread_session(&fx, None).await;

        let execution_id = fx.ensure("j", SnapshotFreshness::Tracking).await;

        assert_eq!(
            fx.job_execution_id("j").await.as_deref(),
            Some(execution_id.as_str()),
            "the job must point at the host that was created for it"
        );
        let snapshot = fx.snapshot(&execution_id).await;
        let agent = snapshot
            .agents
            .get("thread")
            .expect("the host snapshot carries the job's own agent");
        assert_eq!(agent.fence, Some(Fence::Allow));
        assert!(!agent.prompt.is_empty());
        assert!(
            snapshot.recipe.nodes.is_empty(),
            "a host execution schedules nothing of its own"
        );
    }

    /// The acceptance criterion: the fence walk resolves for a thread run through
    /// exactly the path it uses for every other run, with no thread branch.
    #[tokio::test]
    async fn thread_run_resolves_its_fence_through_the_ordinary_walk() {
        let fx = fixture("host-snapshot-fence-walk.db").await;
        seed_thread_session(&fx, None).await;
        fx.db
            .execute(
                "INSERT INTO runs (id, project_id, job_id, status, created_at, updated_at, start_mode)
                 VALUES ('r', 'p', 'j', 'live', 1, 1, 'resume')",
                (),
            )
            .await
            .unwrap();

        assert_eq!(
            crate::mcp::handlers::permission::resolve_fence_policy(&fx.orch, Some("r")).await,
            None,
            "before the ensure there is no snapshot to resolve through"
        );

        fx.ensure("j", SnapshotFreshness::Tracking).await;

        assert_eq!(
            crate::mcp::handlers::permission::resolve_fence_policy(&fx.orch, Some("r")).await,
            Some(Fence::Allow)
        );
    }

    /// The state the live threads are actually in: a synthetic host holding only
    /// the DELEGATED agent. The repair must happen in place, because the packets
    /// booked in that same snapshot have to survive it.
    #[tokio::test]
    async fn backfill_repairs_a_delegation_only_snapshot_and_keeps_its_packets() {
        let fx = fixture("host-snapshot-backfill-delegation.db").await;
        seed_thread_session(&fx, None).await;

        let delegated = serde_json::json!({
            "id": "pkt-1",
            "parentJobId": "j",
            "origin": "task_tool",
            "title": "Review",
            "problemStatement": "Review the change",
            "agentConfigId": "pr-review",
            "ownership": { "cwd": "/tmp/p" },
            "outputContract": { "schemaType": "return" },
            "status": "pending",
            "createdAt": 1
        });
        let snapshot = serde_json::json!({
            "recipe": {
                "id": "host-j", "name": "Session Host", "description": null,
                "trigger": "manual", "nodes": [], "edges": []
            },
            "agents": {
                "pr-review": {
                    "id": "pr-review", "name": "PR Review", "description": "",
                    "prompt": "review", "tools": [], "disallowedTools": null, "skills": null
                }
            },
            "skills": {},
            "triggerContext": { "issueId": null, "projectId": "p", "triggerType": "manual" },
            "delegatedPackets": [delegated],
            "createdAt": 1
        })
        .to_string();
        fx.db
            .execute(
                "INSERT INTO executions (id, recipe_id, project_id, status, started_at, snapshot, triggered_by)
                 VALUES ('e', 'host-j', 'p', 'running', 1, ?1, 'manual')",
                params![snapshot.as_str()],
            )
            .await
            .unwrap();
        fx.db
            .execute("UPDATE jobs SET execution_id = 'e' WHERE id = 'j'", ())
            .await
            .unwrap();

        let execution_id = fx.ensure("j", SnapshotFreshness::Tracking).await;

        assert_eq!(
            execution_id, "e",
            "the repair is in place, not a second host"
        );
        let repaired = fx.snapshot("e").await;
        assert!(repaired.agents.contains_key("thread"));
        assert!(
            repaired.agents.contains_key("pr-review"),
            "the delegated agent must survive the repair"
        );
        assert_eq!(
            repaired.delegated_packets.len(),
            1,
            "the packets booked in this snapshot must survive the repair"
        );
    }

    #[tokio::test]
    async fn an_unedited_tracking_snapshot_follows_its_agent_definition() {
        let fx = fixture("host-snapshot-tracking-refresh.db").await;
        seed_thread_session(&fx, None).await;
        let execution_id = fx.ensure("j", SnapshotFreshness::Tracking).await;

        fx.write_agent("thread", &thread_agent("Hold the topic, revised."));
        fx.ensure("j", SnapshotFreshness::Tracking).await;

        assert!(fx.snapshot(&execution_id).await.agents["thread"]
            .prompt
            .contains("revised"));
    }

    #[tokio::test]
    async fn an_edited_snapshot_stops_following_its_agent_definition() {
        let fx = fixture("host-snapshot-edit-freezes.db").await;
        seed_thread_session(&fx, None).await;
        let execution_id = fx.ensure("j", SnapshotFreshness::Tracking).await;

        let mut edited = fx.snapshot(&execution_id).await.agents["thread"].clone();
        edited.prompt = "Customized for this thread.".to_string();
        crate::execution::snapshot_edit::update_execution_agent(
            &fx.orch,
            &execution_id,
            "thread",
            edited,
        )
        .await
        .unwrap();
        assert!(fx.snapshot(&execution_id).await.agents["thread"]
            .edited_at
            .is_some());

        fx.write_agent("thread", &thread_agent("Hold the topic, revised."));
        fx.ensure("j", SnapshotFreshness::Tracking).await;

        assert_eq!(
            fx.snapshot(&execution_id).await.agents["thread"].prompt,
            "Customized for this thread.",
            "an explicit edit must not be reverted by a file change"
        );
    }

    /// An execution's snapshot is its reproducibility guarantee, so the ensure
    /// must never re-resolve one — even though the very same call re-resolves a
    /// thread's.
    #[tokio::test]
    async fn an_execution_owned_snapshot_stays_frozen() {
        let fx = fixture("host-snapshot-frozen-execution.db").await;
        seed_thread_session(&fx, None).await;
        let execution_id = fx.ensure("j", SnapshotFreshness::Tracking).await;

        fx.write_agent("thread", &thread_agent("Hold the topic, revised."));
        fx.ensure("j", SnapshotFreshness::Frozen).await;

        assert!(
            !fx.snapshot(&execution_id).await.agents["thread"]
                .prompt
                .contains("revised"),
            "a frozen host must not pick up a file change"
        );
    }

    #[tokio::test]
    async fn the_job_model_composes_into_the_resolved_selection() {
        let fx = fixture("host-snapshot-model-composition.db").await;
        seed_thread_session(&fx, Some("gpt-5.2-codex")).await;

        let execution_id = fx.ensure("j", SnapshotFreshness::Tracking).await;

        let selection = fx.snapshot(&execution_id).await.agents["thread"]
            .selection
            .clone()
            .expect("a resolved selection");
        assert_eq!(selection.model.as_str(), "gpt-5.2-codex");
        assert_eq!(selection.backend, "codex");
    }

    /// Delegation's old behavior, preserved: a job with no agent of its own still
    /// gets a host to book packets in.
    #[tokio::test]
    async fn an_agentless_job_still_gets_a_host_execution() {
        let fx = fixture("host-snapshot-agentless.db").await;
        fx.db
            .execute(
                "INSERT INTO jobs (id, project_id, status, node_name, uri_segment, created_at, updated_at)
                 VALUES ('j', 'p', 'running', 'flow', 'flow', 1, 1)",
                (),
            )
            .await
            .unwrap();

        let execution_id = fx.ensure("j", SnapshotFreshness::Frozen).await;

        assert_eq!(
            fx.job_execution_id("j").await.as_deref(),
            Some(execution_id.as_str())
        );
        assert!(fx.snapshot(&execution_id).await.agents.is_empty());
    }

    /// Establishing the invariant is loud. Launching a turn after failing to
    /// establish it would move the failure out of the launch and into every `run`
    /// batch the turn makes, as an unresolvable fence.
    #[tokio::test]
    async fn an_unresolvable_agent_refuses_to_establish_the_invariant() {
        let fx = fixture("host-snapshot-unresolvable-agent.db").await;
        seed_thread_session(&fx, None).await;
        std::fs::remove_file(fx.agents_dir.join("thread.md")).unwrap();

        let job = fx.job("j").await;
        let error = ensure_host_agent_snapshot(&fx.orch, &job, None, SnapshotFreshness::Tracking)
            .await
            .expect_err("an agent that cannot be resolved has no snapshot to stand in for it");

        assert!(
            error.contains("thread"),
            "the error names the agent: {error}"
        );
        assert_eq!(fx.job_execution_id("j").await, None);
    }

    /// Keeping the invariant CURRENT is not loud. A stored snapshot is already a
    /// complete answer for the fence walk, so an agent definition that has moved
    /// out from under a live thread stops the tracking rather than the thread.
    #[tokio::test]
    async fn an_unresolvable_agent_keeps_a_snapshot_that_already_exists() {
        let fx = fixture("host-snapshot-unresolvable-refresh.db").await;
        seed_thread_session(&fx, None).await;
        let execution_id = fx.ensure("j", SnapshotFreshness::Tracking).await;
        std::fs::remove_file(fx.agents_dir.join("thread.md")).unwrap();

        assert_eq!(
            fx.ensure("j", SnapshotFreshness::Tracking).await,
            execution_id
        );
        assert!(!fx.snapshot(&execution_id).await.agents["thread"]
            .prompt
            .is_empty());
    }

    /// The editor's entry point, addressed by job id: a thread configured before
    /// it has ever taken a turn has a snapshot to configure.
    #[tokio::test]
    async fn a_thread_is_configurable_before_its_first_turn() {
        let fx = fixture("host-snapshot-before-first-turn.db").await;
        seed_thread_session(&fx, None).await;
        assert_eq!(
            fx.job_execution_id("j").await,
            None,
            "a session job takes no execution until it launches or is edited"
        );

        let execution_id = ensure_job_agent_snapshot(&fx.orch, "j")
            .await
            .unwrap()
            .expect("host execution");

        assert_eq!(
            fx.job_execution_id("j").await.as_deref(),
            Some(execution_id.as_str())
        );
        assert!(fx
            .snapshot(&execution_id)
            .await
            .agents
            .contains_key("thread"));
    }

    #[tokio::test]
    async fn a_second_ensure_reuses_the_host_it_created() {
        let fx = fixture("host-snapshot-idempotent.db").await;
        seed_thread_session(&fx, None).await;

        let first = fx.ensure("j", SnapshotFreshness::Tracking).await;
        let second = fx.ensure("j", SnapshotFreshness::Tracking).await;

        assert_eq!(first, second);
    }
}

#[cfg(test)]
mod resolution_tests {
    #[test]
    fn completed_ask_event_metadata_keeps_canonical_receipt_provenance() {
        let receipt = cairn_db::models::ResolutionReceipt {
            id: Some("receipt-1".into()),
            surface: "channel_reply".into(),
            provider: Some("discord".into()),
            conversation: Some("discord:guild/channel".into()),
            actor: Some("discord:guild/channel:user-7".into()),
            resolved_at: 1_786_590_123,
        };
        let metadata = serde_json::json!({ "resolution": receipt });
        assert_eq!(metadata["resolution"]["provider"], "discord");
        assert_eq!(
            metadata["resolution"]["conversation"],
            "discord:guild/channel"
        );
        assert_eq!(
            metadata["resolution"]["actor"],
            "discord:guild/channel:user-7"
        );
        assert_eq!(metadata["resolution"]["resolvedAt"], 1_786_590_123_i64);
    }
}
