//! Latest persisted context-token observation.
use super::{DbResult, LocalDb, RowExt};
use cairn_domain::ContextTokenEventSnapshot;
use serde_json::Value;
use std::sync::Arc;
use turso::params;

pub async fn get_latest_context_token_event(
    db: Arc<LocalDb>,
    session_id: &str,
) -> DbResult<Option<ContextTokenEventSnapshot>> {
    let session_id = session_id.to_string();
    db.query_opt(
        "SELECT e.run_id,
                e.session_id,
                COALESCE(s.backend, 'claude') AS backend,
                j.model,
                e.input_tokens,
                e.cache_read_tokens,
                e.cache_create_tokens,
                e.output_tokens,
                e.thinking_tokens,
                e.created_at,
                e.data
         FROM events e
         LEFT JOIN runs r ON r.id = e.run_id
         LEFT JOIN sessions s ON s.id = e.session_id
         LEFT JOIN jobs j ON j.id = COALESCE(r.job_id, s.job_id)
         WHERE e.session_id = ?1
           AND e.parent_tool_use_id IS NULL
           AND e.event_type NOT LIKE 'result%'
           AND (
                e.input_tokens IS NOT NULL
             OR e.cache_read_tokens IS NOT NULL
             OR e.cache_create_tokens IS NOT NULL
             OR e.output_tokens IS NOT NULL
             OR e.thinking_tokens IS NOT NULL
           )
         ORDER BY e.created_at DESC, e.sequence DESC
         LIMIT 1",
        params![session_id],
        |row| {
            let db_model = row.opt_text(3)?;
            let data = row.text(10)?;
            Ok(ContextTokenEventSnapshot {
                run_id: row.text(0)?,
                session_id: row.text(1)?,
                backend: row.text(2)?.to_ascii_lowercase(),
                model: db_model.or_else(|| extract_model_from_event_data(&data)),
                input_tokens: row.opt_i64(4)?,
                cache_read_tokens: row.opt_i64(5)?,
                cache_create_tokens: row.opt_i64(6)?,
                output_tokens: row.opt_i64(7)?,
                thinking_tokens: row.opt_i64(8)?,
                captured_at: row.i64(9)?,
            })
        },
    )
    .await
}

fn extract_model_from_event_data(data: &str) -> Option<String> {
    let value: Value = serde_json::from_str(data).ok()?;
    for pointer in [
        "/model",
        "/raw/model",
        "/raw/message/model",
        "/raw/response/model",
        "/raw/turn/model",
    ] {
        if let Some(model) = value.pointer(pointer).and_then(Value::as_str) {
            if !model.is_empty() {
                return Some(model.to_string());
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn latest_observation_excludes_results_children_and_empty_updates() {
        let db = Arc::new(crate::storage::migrated_test_db("context-tokens.db").await);
        db.execute_script(r#"
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('ctx-ws', 'Context', 1, 1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
              VALUES('ctx-project', 'ctx-ws', 'Context', 'CTX', '/tmp/ctx', 1, 1);
            INSERT INTO jobs(id, project_id, status, model, created_at, updated_at)
              VALUES('ctx-job', 'ctx-project', 'running', NULL, 1, 1);
            INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
              VALUES('ctx-session', 'ctx-job', 'CLAUDE', 'open', 1, 1, 1);
            INSERT INTO runs(id, project_id, job_id, status, session_id, created_at, updated_at)
              VALUES('ctx-run', 'ctx-project', 'ctx-job', 'exited', 'ctx-session', 1, 1);
            INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                parent_tool_use_id, created_at, input_tokens, cache_read_tokens, cache_create_tokens, output_tokens)
              VALUES
                ('old', 'ctx-run', 'ctx-session', 1, 10, 'assistant', '{}', NULL, 10, 999, NULL, NULL, NULL),
                ('latest', 'ctx-run', 'ctx-session', 2, 10, 'assistant', '{"raw":{"message":{"model":"persisted-model"}}}', NULL, 10, 7, 100, 20, 3),
                ('result', 'ctx-run', 'ctx-session', 3, 11, 'result:success', '{}', NULL, 11, 9999, NULL, NULL, NULL),
                ('child', 'ctx-run', 'ctx-session', 4, 12, 'assistant', '{}', 'tool', 12, 9999, NULL, NULL, NULL),
                ('empty', 'ctx-run', 'ctx-session', 5, 13, 'assistant', '{}', NULL, 13, NULL, NULL, NULL, NULL);
        "#).await.unwrap();

        // Exercise the existing public query path and the new model identity.
        let event: cairn_domain::ContextTokenEventSnapshot =
            crate::models::get_latest_context_token_event(db.clone(), "ctx-session")
                .await
                .unwrap()
                .unwrap();
        assert_eq!(event.backend, "claude");
        assert_eq!(event.model.as_deref(), Some("persisted-model"));
        assert_eq!(event.captured_at, 10);
        assert_eq!(event.into_state(None).used_tokens, 130);
        assert!(
            get_latest_context_token_event(db.clone(), "another-session")
                .await
                .unwrap()
                .is_none()
        );

        db.execute_script("UPDATE jobs SET model = 'job-pinned-model' WHERE id = 'ctx-job'")
            .await
            .unwrap();
        let event = get_latest_context_token_event(db, "ctx-session")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(event.model.as_deref(), Some("job-pinned-model"));
    }
}
