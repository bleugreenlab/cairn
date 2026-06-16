use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use turso::params;

use crate::storage::{DbResult, LocalDb, RowExt};

/// Normalized per-turn context occupancy for live token-awareness surfaces.
///
/// `used_tokens` is the latest turn's full input prompt plus that turn's output.
/// It is not cumulative across turns.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ContextTokenState {
    pub run_id: String,
    pub session_id: Option<String>,
    pub backend: String,
    pub model: Option<String>,
    pub used_tokens: i64,
    pub context_window: Option<i64>,
    pub auto_compact_limit: Option<i64>,
    pub reasoning_tokens: Option<i64>,
    pub last_output_tokens: Option<i64>,
    pub captured_at: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ContextTokenEventSnapshot {
    pub run_id: String,
    pub session_id: String,
    pub backend: String,
    pub model: Option<String>,
    pub input_tokens: Option<i64>,
    pub cache_read_tokens: Option<i64>,
    pub cache_create_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub thinking_tokens: Option<i64>,
    pub captured_at: i64,
}

impl ContextTokenEventSnapshot {
    pub(crate) fn into_state(self, context_window: Option<i64>) -> ContextTokenState {
        let input_tokens = self.input_tokens.unwrap_or(0);
        let output_tokens = self.output_tokens.unwrap_or(0);
        let used_tokens = if self.backend.eq_ignore_ascii_case("codex") {
            input_tokens + output_tokens
        } else {
            input_tokens
                + self.cache_create_tokens.unwrap_or(0)
                + self.cache_read_tokens.unwrap_or(0)
                + output_tokens
        };

        ContextTokenState {
            run_id: self.run_id,
            session_id: Some(self.session_id),
            backend: self.backend,
            model: self.model,
            used_tokens,
            context_window,
            auto_compact_limit: None,
            reasoning_tokens: self.thinking_tokens,
            last_output_tokens: self.output_tokens,
            captured_at: self.captured_at,
        }
    }
}

pub(crate) async fn get_latest_context_token_event(
    db: Arc<LocalDb>,
    session_id: &str,
) -> DbResult<Option<ContextTokenEventSnapshot>> {
    let session_id = session_id.to_string();
    db.query_opt(
        "SELECT e.run_id,
                e.session_id,
                COALESCE(r.backend, s.backend, 'claude') AS backend,
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
           AND NOT (
                LOWER(COALESCE(r.backend, s.backend, 'claude')) = 'claude'
            AND e.event_type LIKE 'result%'
           )
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
