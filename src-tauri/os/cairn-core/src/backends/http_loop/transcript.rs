//! Cairn's persisted transcript, read back as replayable rows.
//!
//! Every HTTP protocol family rebuilds its outgoing conversation from the same
//! stored events, so WHICH rows replay — and the fact that a user turn's stable
//! image references must be resolved to bytes before anything can send them — is
//! one fact, kept here. What each row BECOMES on the wire is protocol-specific
//! and deliberately not decided here: a chat message, an Anthropic content
//! block, and a Responses input item are different shapes with different legal
//! orderings, and flattening them into a common enum would push each family's
//! exceptions into the others.

use crate::agent_process::stdin::MessageContent;
use crate::agent_process::stream::TranscriptEvent;
use crate::orchestrator::Orchestrator;
use crate::runs::queries::{event_from_row, EVENT_COLUMNS};
use crate::storage::run_db_blocking;

/// One prior transcript row, decoded.
///
/// A user turn arrives already resolved: turning `cairn://` image references
/// into bytes needs the database, so it happens inside the loader's runtime
/// block rather than being left for each protocol to redo. Every other row is
/// handed back as the persisted event, for the protocol to map.
pub(in crate::backends) enum ReplayRow {
    User(MessageContent),
    /// Boxed because a transcript event is several times the size of a user
    /// turn, and every row would otherwise pay for the largest variant.
    Event {
        event_type: String,
        event: Box<cairn_models::wire::ReplayEvent>,
    },
}

/// Load the rows a resumed session replays, oldest first.
///
/// `user:launch` carries the task the job was started on. It is bound as a
/// parameter rather than written into the `IN` list so the replay set cannot
/// drift from the constant (CAIRN-3408): dropping it here would rebuild a
/// conversation whose opening instruction is missing, and every turn after the
/// first would continue without ever having been told what to do.
///
/// Blank stored user content is dropped rather than replayed: an empty text
/// block is rejected by these providers, and replaying one turns a single bad
/// turn into a conversation that can never resume (CAIRN-3263).
pub(in crate::backends) fn load_prior_rows(
    orch: &Orchestrator,
    session_id: &str,
    current_run_id: &str,
    project_id: &str,
    project_key: &str,
) -> Result<Vec<ReplayRow>, String> {
    let session_id = session_id.to_string();
    let current_run_id = current_run_id.to_string();
    let project_id = project_id.to_string();
    let project_key = project_key.to_string();
    run_db_blocking(|| async move {
        let session_db = crate::projects::crud::owning_db(&orch.db, &project_id).await?;
        let rows = session_db
            .query_all(
                format!(
                    "SELECT {EVENT_COLUMNS} FROM events
                     WHERE session_id = ?1
                       AND run_id != ?2
                       AND (event_type IN ('user', 'assistant', 'tool_result')
                            OR event_type = ?3)
                     ORDER BY created_at ASC, rowid ASC"
                ),
                (
                    session_id.clone(),
                    current_run_id.clone(),
                    crate::transcripts::LAUNCH_EVENT_TYPE,
                ),
                event_from_row,
            )
            .await
            .map_err(|error| error.to_string())?;
        // A session resumed after its issue was resolved and reopened has
        // archived rows whose `data` is only a stub; restore them first.
        let rows = crate::storage::reconstruct_events(&session_db, rows).await;
        let mut out = Vec::new();
        for stored in rows {
            let event_type = stored.event_type;
            let Ok(event) = serde_json::from_str::<TranscriptEvent>(&stored.data) else {
                continue;
            };
            // A launch prompt reaches the provider in the same user role it
            // originally occupied. Namespacing it changed who Cairn says wrote
            // it, not what the model was told (CAIRN-3408).
            if event_type == "user" || event_type == crate::transcripts::LAUNCH_EVENT_TYPE {
                let Some(text) = event.content.filter(|text| !text.trim().is_empty()) else {
                    continue;
                };
                let content = crate::agent_process::stdin::resolve_stable_images(
                    &orch.db,
                    &project_id,
                    &project_key,
                    text,
                )
                .await?;
                out.push(ReplayRow::User(content));
            } else {
                out.push(ReplayRow::Event {
                    event_type,
                    event: Box::new(event.into()),
                });
            }
        }
        Ok::<_, String>(out)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{LocalDb, SearchIndex};
    use cairn_session::agent_process::stream::TranscriptJson;
    use std::sync::Arc;

    fn test_orchestrator(db: LocalDb) -> Orchestrator {
        let root = tempfile::tempdir().unwrap().keep();
        let config_dir = root.join("config");
        std::fs::create_dir_all(config_dir.join("agents")).unwrap();
        std::fs::create_dir_all(config_dir.join("recipes")).unwrap();
        let search_index = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
        let db_state = Arc::new(DbState::new(Arc::new(db), search_index));
        OrchestratorBuilder::new(
            db_state,
            Arc::new(TestServicesBuilder::new().build()),
            config_dir,
        )
        .build()
    }

    fn event_json(event_type: &str, content: Option<&str>, tool_result: Option<&str>) -> String {
        TranscriptEvent {
            event_type: event_type.to_string(),
            session_id: Some("sess-1".to_string()),
            parent_tool_use_id: None,
            content: content.map(str::to_string),
            thinking: None,
            tool_name: None,
            tool_input: None,
            tool_uses: None,
            tool_use_id: tool_result.map(|_| "t1".to_string()),
            tool_result: tool_result.map(str::to_string),
            is_error: false,
            thinking_ms: None,
            queued_message_id: None,
            raw: None,
        }
        .observed()
        .to_event_json()
    }

    /// A session reopened after its issue was resolved replays its archived
    /// rows in full, not as the hollow stubs stored in `data`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn archived_prior_rows_replay_in_full() {
        let db = crate::storage::migrated_test_db("http-replay-archived.db").await;
        db.execute_script(
            "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES('p','w','Project','proj','/tmp/repo',1,1);
             INSERT INTO executions(id, recipe_id, status, started_at) VALUES ('exec','r','complete',1);
             INSERT INTO jobs(id, execution_id, project_id, status, current_session_id, created_at, updated_at)
               VALUES('job-1','exec','p','running','sess-1',1,1);
             INSERT INTO sessions(id, job_id, status, created_at, updated_at)
               VALUES('sess-1','job-1','active',1,1);
             INSERT INTO runs(id, project_id, job_id, session_id, status, created_at, updated_at)
               VALUES('run-1','p','job-1','sess-1','exited',1,1);",
        )
        .await
        .unwrap();
        let rows = [
            ("user", event_json("user", Some("the task"), None)),
            ("assistant", event_json("assistant", Some("on it"), None)),
            (
                "tool_result",
                event_json("tool_result", None, Some("tool output")),
            ),
        ];
        for (seq, (event_type, data)) in rows.iter().enumerate() {
            db.execute(
                "INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data, created_at)
                 VALUES(?1,'run-1','sess-1',?2,?2,?3,?4,?2)",
                cairn_db::turso::params![format!("e{seq}"), seq as i64, *event_type, data.as_str()],
            )
            .await
            .unwrap();
        }
        let summary = crate::archival::archive_execution(
            &db,
            crate::archival::ArchiveTarget {
                execution_id: "exec".to_string(),
                ranges: Vec::new(),
            },
        )
        .await
        .unwrap();
        assert_eq!(summary.zstd, 3, "every prior row is archived");
        let orch = test_orchestrator(db);

        let replayed = load_prior_rows(&orch, "sess-1", "run-2", "p", "proj").unwrap();
        assert_eq!(replayed.len(), 3);
        assert!(matches!(&replayed[0], ReplayRow::User(_)));
        let ReplayRow::Event { event_type, event } = &replayed[1] else {
            panic!("expected the assistant row");
        };
        assert_eq!(event_type, "assistant");
        assert_eq!(event.content.as_deref(), Some("on it"));
        let ReplayRow::Event { event_type, event } = &replayed[2] else {
            panic!("expected the tool result row");
        };
        assert_eq!(event_type, "tool_result");
        assert_eq!(event.tool_result.as_deref(), Some("tool output"));
    }
}
