//! Rebuild the OpenAI-style message array an OpenAI-compatible turn sends: assemble the
//! system + prior transcript + new user message, normalize assistant/tool groups,
//! and map stored transcript events to chat messages.

use super::wire::{default_function_type, ChatMessage, ToolCall, ToolFunction};
use crate::agent_process::stream::TranscriptEvent;
use crate::backends::{SessionConfig, SessionStart};
use crate::orchestrator::Orchestrator;
use crate::storage::{run_db_blocking, RowExt};
use serde_json::Value;
use std::collections::HashMap;

const INTERRUPTED_TOOL_RESULT: &str = "Interrupted before the tool result was recorded.";

/// Stands in for a tool result that was stored empty. A blank content block is
/// rejectable, and a dropped tool message would orphan its call, so the empty
/// result is stated rather than sent or removed.
const EMPTY_TOOL_RESULT: &str = "The tool returned no output.";

/// Why a wholly empty turn is refused instead of assembled.
const EMPTY_USER_MESSAGE: &str = "Refusing to start a turn with an empty user message: an empty text block is rejected by the provider and would poison every later replay of this conversation.";

/// Concatenate assembled prompt segments into the full system prompt. This is
/// byte-identical to what `persist_system_prompt_event` records, so the wire
/// system message equals the persisted/displayed prompt with no drift.
pub(crate) fn build_conversation_messages(
    orch: &Orchestrator,
    config: &SessionConfig,
    session_id: &str,
    system_prompt: &str,
) -> Result<Vec<ChatMessage>, String> {
    if config.message_content.is_blank() {
        return Err(EMPTY_USER_MESSAGE.to_string());
    }
    let mut messages = vec![ChatMessage::system(system_prompt.to_string())];
    if !matches!(config.session_start, SessionStart::New { .. }) {
        messages.extend(load_prior_chat_messages(
            orch,
            session_id,
            &config.run_id,
            &config.project_id,
            &config.project_key,
        )?);
    }
    messages.push(ChatMessage::user_content(&config.message_content));
    Ok(messages)
}

fn load_prior_chat_messages(
    orch: &Orchestrator,
    session_id: &str,
    current_run_id: &str,
    project_id: &str,
    project_key: &str,
) -> Result<Vec<ChatMessage>, String> {
    let session_id = session_id.to_string();
    let current_run_id = current_run_id.to_string();
    let project_id = project_id.to_string();
    let project_key = project_key.to_string();
    let messages = run_db_blocking(|| async move {
        let session_db = crate::projects::crud::owning_db(&orch.db, &project_id).await?;
        let rows = session_db
            .query_all(
                // `user:launch` carries the task the job was started on. It is
                // bound as a parameter rather than written into the IN list so
                // the replay set cannot drift from the constant (CAIRN-3408):
                // dropping it here would rebuild a conversation whose opening
                // instruction is missing, and every turn after the first would
                // continue without ever having been told what to do.
                "SELECT event_type, data FROM events
                 WHERE session_id = ?1
                   AND run_id != ?2
                   AND (event_type IN ('user', 'assistant', 'tool_result')
                        OR event_type = ?3)
                 ORDER BY created_at ASC, rowid ASC",
                (
                    session_id.clone(),
                    current_run_id.clone(),
                    crate::transcripts::LAUNCH_EVENT_TYPE,
                ),
                |row| Ok((row.text(0)?, row.text(1)?)),
            )
            .await
            .map_err(|error| error.to_string())?;
        let mut out = Vec::new();
        for (event_type, data) in rows {
            let Ok(event) = serde_json::from_str::<TranscriptEvent>(&data) else {
                continue;
            };
            // Blank stored content is dropped rather than replayed: an empty
            // text block is rejected by the provider, and replaying one turns a
            // single bad turn into a conversation that can never resume
            // (CAIRN-3263).
            // A launch prompt reaches the provider in the same user role it
            // originally occupied. Namespacing it changed who Cairn says wrote
            // it, not what the model was told (CAIRN-3408).
            let message =
                if event_type == "user" || event_type == crate::transcripts::LAUNCH_EVENT_TYPE {
                    match event.content.filter(|text| !text.trim().is_empty()) {
                        Some(content) => {
                            let content = crate::agent_process::stdin::resolve_stable_images(
                                &orch.db,
                                &project_id,
                                &project_key,
                                content,
                            )
                            .await?;
                            Some(ChatMessage::user_content(&content))
                        }
                        None => None,
                    }
                } else {
                    transcript_event_to_chat_message(&event_type, event)
                };
            if let Some(message) = message {
                out.push(message);
            }
        }
        Ok::<_, String>(out)
    })?;
    Ok(normalize_tool_call_groups(messages))
}

/// Reconstruct protocol-valid assistant/tool groups from persisted history.
/// Results may be stored after unrelated events when a foreground prompt
/// suspends a turn, so association is by call id rather than adjacency.
pub(crate) fn normalize_tool_call_groups(messages: Vec<ChatMessage>) -> Vec<ChatMessage> {
    let mut stored_results = HashMap::new();
    let mut duplicate_results = Vec::new();
    for message in messages.iter().filter(|message| message.role == "tool") {
        let Some(tool_call_id) = message.tool_call_id.as_ref() else {
            continue;
        };
        if stored_results
            .insert(tool_call_id.clone(), message.clone())
            .is_some()
        {
            duplicate_results.push(tool_call_id.clone());
        }
    }

    let mut result = Vec::with_capacity(messages.len());
    let mut synthesized = Vec::new();
    for message in messages
        .into_iter()
        .filter(|message| message.role != "tool")
    {
        let call_ids = message
            .tool_calls
            .as_ref()
            .filter(|_| message.role == "assistant")
            .map(|calls| calls.iter().map(|call| call.id.clone()).collect::<Vec<_>>());
        result.push(message);
        for call_id in call_ids.into_iter().flatten() {
            if let Some(tool_result) = stored_results.remove(&call_id) {
                result.push(tool_result);
            } else {
                synthesized.push(call_id.clone());
                result.push(ChatMessage::tool(
                    call_id,
                    INTERRUPTED_TOOL_RESULT.to_string(),
                ));
            }
        }
    }

    if !synthesized.is_empty() || !duplicate_results.is_empty() || !stored_results.is_empty() {
        let orphan_ids = stored_results.keys().cloned().collect::<Vec<_>>();
        log::warn!(
            "Repaired OpenRouter tool history: synthesized={:?}, duplicates={:?}, orphans={:?}",
            synthesized,
            duplicate_results,
            orphan_ids
        );
    }
    result
}

pub(crate) fn transcript_event_to_chat_message(
    event_type: &str,
    event: TranscriptEvent,
) -> Option<ChatMessage> {
    match event_type {
        "user" => event
            .content
            .filter(|text| !text.trim().is_empty())
            .map(ChatMessage::user),
        "assistant" => {
            let tool_calls = event.tool_uses.as_ref().map(|uses| {
                uses.iter()
                    .map(|tool| ToolCall {
                        id: tool.id.clone(),
                        r#type: default_function_type(),
                        function: ToolFunction {
                            name: tool.name.clone(),
                            arguments: serde_json::to_string(&tool.input)
                                .unwrap_or_else(|_| "{}".to_string()),
                        },
                    })
                    .collect::<Vec<_>>()
            });
            // Replay structured reasoning verbatim and in original order; stored
            // under either casing depending on which writer persisted the event.
            let reasoning_details = event
                .raw
                .as_ref()
                .and_then(|raw| {
                    raw.get("reasoning_details")
                        .or_else(|| raw.get("reasoningDetails"))
                })
                // Writers store `null` (no reasoning) or `[]`; treat both as absent
                // so a non-reasoning tool-call turn does not replay `reasoning_details: null`.
                .filter(|value| {
                    !value.is_null() && !matches!(value, Value::Array(items) if items.is_empty())
                })
                .cloned();
            // A turn whose text came back blank replays as its tool calls
            // alone, and as nothing at all when it has none.
            let content = event.content.filter(|text| !text.trim().is_empty());
            let tool_calls = tool_calls.filter(|calls| !calls.is_empty());
            if content.is_none() && tool_calls.is_none() {
                None
            } else {
                Some(ChatMessage {
                    role: "assistant".to_string(),
                    content: content.map(super::wire::ChatContent::Text),
                    tool_call_id: None,
                    tool_calls,
                    reasoning_details,
                })
            }
        }
        "tool_result" => event
            .tool_use_id
            .zip(event.tool_result)
            .map(|(tool_call_id, content)| {
                let content = if content.trim().is_empty() {
                    EMPTY_TOOL_RESULT.to_string()
                } else {
                    content
                };
                ChatMessage::tool(tool_call_id, content)
            }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{LocalDb, SearchIndex};
    use std::sync::Arc;

    fn test_orchestrator(db: LocalDb) -> Orchestrator {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.keep();
        let config_dir = root.join("config");
        std::fs::create_dir_all(config_dir.join("agents")).unwrap();
        std::fs::create_dir_all(config_dir.join("recipes")).unwrap();
        let search_index = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
        let db_state = Arc::new(DbState::new(Arc::new(db), search_index));
        let services = Arc::new(TestServicesBuilder::new().build());
        OrchestratorBuilder::new(db_state, services, config_dir).build()
    }

    /// One session, one prior run: the launch prompt that opened the job, then
    /// the assistant's reply.
    async fn seed_prior_run(db: &LocalDb, launch_type: &str) {
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
              VALUES('p','w','Project','PROJ','/tmp/repo',1,1);
            INSERT INTO jobs(id, project_id, status, current_session_id, created_at, updated_at)
              VALUES('job-1','p','running','sess-1',1,1);
            INSERT INTO sessions(id, job_id, status, created_at, updated_at)
              VALUES('sess-1','job-1','active',1,1);
            INSERT INTO runs(id, project_id, job_id, session_id, status, created_at, updated_at)
              VALUES('run-1','p','job-1','sess-1','exited',1,1);
            ",
        )
        .await
        .unwrap();
        // Serialized from the same struct the storage path builds, so the
        // fixture cannot drift from what actually lands in `data`.
        let event_data = |event_type: &str, content: &str| {
            serde_json::to_string(&TranscriptEvent {
                event_type: event_type.to_string(),
                session_id: Some("sess-1".to_string()),
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
            })
            .unwrap()
        };
        let launch = event_data(launch_type, THE_TASK);
        let reply = event_data("assistant", "on it");
        let launch_type = launch_type.to_string();
        db.write(move |conn| {
            let launch = launch.clone();
            let reply = reply.clone();
            let launch_type = launch_type.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data, created_at)
                     VALUES('e1','run-1','sess-1',0,1,?1,?2,1)",
                    cairn_db::turso::params![launch_type.as_str(), launch.as_str()],
                )
                .await?;
                conn.execute(
                    "INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data, created_at)
                     VALUES('e2','run-1','sess-1',1,2,'assistant',?1,2)",
                    cairn_db::turso::params![reply.as_str()],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    const THE_TASK: &str = "Fix the panic in the CLI logger before touching anything else.";

    /// A second run on the same session must rebuild a conversation that still
    /// states the task (CAIRN-3408).
    ///
    /// The first request never exercises this path — it carries its prompt
    /// directly — so a launch prompt missing from replay stays invisible until
    /// some later turn continues with no idea what it was asked to do. Asserting
    /// against the old `user` type in the same test proves the reconstruction is
    /// equivalent, not merely non-empty.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_later_run_replays_the_launch_prompt_it_was_started_on() {
        for launch_type in ["user", crate::transcripts::LAUNCH_EVENT_TYPE] {
            let db = crate::storage::migrated_test_db("openai-compat-replay.db").await;
            seed_prior_run(&db, launch_type).await;
            let orch = test_orchestrator(db);

            let replayed = load_prior_chat_messages(&orch, "sess-1", "run-2", "p", "PROJ").unwrap();

            assert_eq!(
                replayed.len(),
                2,
                "expected the launch prompt and the reply ({launch_type}): {replayed:?}"
            );
            assert_eq!(replayed[0].role, "user", "({launch_type})");
            assert!(
                format!("{:?}", replayed[0].content).contains(THE_TASK),
                "a rebuilt conversation must still state the task ({launch_type}): {replayed:?}"
            );
            assert_eq!(replayed[1].role, "assistant", "({launch_type})");
        }
    }
}
