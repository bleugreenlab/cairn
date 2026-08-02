use super::*;

pub(super) struct AgentSnapshotData {
    pub(super) agent: Option<AgentConfig>,
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

fn store_transcript_event_with_turn(
    orch: &Orchestrator,
    run_id: &str,
    session_id: &str,
    now: i32,
    turn_id: Option<&str>,
    transcript_event: TranscriptEvent,
    push_ids: &[String],
) -> Result<(), String> {
    let event_id = ids::mint_child(run_id);
    let event_type = transcript_event.event_type.clone();
    let event_data = serde_json::to_string(&transcript_event).unwrap_or_default();
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

    Ok(())
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
}

/// Persist a single carrying event for drained attention pushes and stamp each
/// push delivered by it, atomically in the event-insert transaction
/// (CAIRN-1881). The rendered push text rides in `content`; recovery redelivers
/// only pushes whose carrying event never durably landed.
pub(crate) fn store_attention_push_event(
    orch: &Orchestrator,
    run_id: &str,
    session_id: &str,
    content: &str,
    push_ids: &[String],
    now: i32,
    turn_id: Option<&str>,
) -> Result<(), String> {
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
