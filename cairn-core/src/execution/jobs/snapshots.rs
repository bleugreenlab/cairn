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
                    selection: agent.selection.clone(),
                    extras: agent.extras.clone(),
                });
            Ok(AgentSnapshotData { agent })
        })
    })
    .await
    .map_err(|e| db_error("Failed to load execution snapshot", e))
}

/// Store a user event in the transcript.
pub(crate) fn store_user_event(
    orch: &Orchestrator,
    run_id: &str,
    session_id: &str,
    content: &str,
    now: i32,
    sequence: i32,
) -> Result<(), String> {
    let current_turn = orch.process_state.get_current_turn_id(run_id);
    store_user_event_with_turn(
        orch,
        run_id,
        session_id,
        content,
        now,
        sequence,
        current_turn.as_deref(),
    )
}

fn store_transcript_event_with_turn(
    orch: &Orchestrator,
    run_id: &str,
    session_id: &str,
    sequence: i32,
    now: i32,
    turn_id: Option<&str>,
    transcript_event: TranscriptEvent,
) -> Result<(), String> {
    let event_id = Uuid::new_v4().to_string();
    let event_type = transcript_event.event_type.clone();
    let event_data = serde_json::to_string(&transcript_event).unwrap_or_default();
    let turn_id = turn_id.map(str::to_string);

    insert_event(
        orch.db.local.clone(),
        EventInsert {
            id: event_id.clone(),
            run_id: run_id.to_string(),
            session_id: Some(session_id.to_string()),
            sequence,
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
        },
    )?;

    orch.sync(crate::sync::SyncMessage::Event(crate::sync::SyncEvent {
        id: event_id,
        run_id: run_id.to_string(),
        session_id: Some(session_id.to_string()),
        sequence: Some(sequence),
        event_type,
        data: Some(event_data),
        input_tokens: None,
        output_tokens: None,
        cache_read_tokens: None,
        cache_create_tokens: None,
        thinking_tokens: None,
        created_at: Some(now as i64),
        turn_id,
    }));

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "events", "action": "insert"}),
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
    sequence: i32,
    turn_id: Option<&str>,
) -> Result<(), String> {
    let sequence = if sequence >= 0 {
        sequence
    } else {
        get_next_sequence(orch.db.local.clone(), run_id)?
    };
    let transcript_event = TranscriptEvent {
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
        raw: None,
    };
    store_transcript_event_with_turn(
        orch,
        run_id,
        session_id,
        sequence,
        now,
        turn_id,
        transcript_event,
    )
}

/// Store an attention briefing as its own `attention:briefing` event. The
/// `items_json` payload (a `{active, catchup}` list) rides in `content`; the
/// frontend renders it as a compact wake card rather than a markdown "You"
/// block. The agent receives the resolved markdown via the prompt, not this
/// event — this is display-only (CAIRN-1647).
pub(crate) fn store_attention_briefing_event_with_turn(
    orch: &Orchestrator,
    run_id: &str,
    session_id: &str,
    items_json: &str,
    now: i32,
    sequence: i32,
    turn_id: Option<&str>,
) -> Result<(), String> {
    let sequence = if sequence >= 0 {
        sequence
    } else {
        get_next_sequence(orch.db.local.clone(), run_id)?
    };
    let transcript_event = TranscriptEvent {
        event_type: "attention:briefing".to_string(),
        session_id: Some(session_id.to_string()),
        parent_tool_use_id: None,
        content: Some(items_json.to_string()),
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
        sequence,
        now,
        turn_id,
        transcript_event,
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
    let sequence = get_next_sequence(orch.db.local.clone(), run_id)?;
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
        sequence,
        now,
        turn_id,
        transcript_event,
    )
}
