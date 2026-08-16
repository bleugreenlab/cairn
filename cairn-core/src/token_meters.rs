//! Model-aware token meters used by core transcript and context accounting.

use std::sync::OnceLock;

use cairn_common::token_meter::{Cut, TokenMeter};
use cairn_tokenize::Family;
use tiktoken_rs::{o200k_base, CoreBPE};

use crate::agent_process::process::AgentProcessState;
use crate::storage::{DbResult, RowExt};
use cairn_db::turso::{params, Connection};

pub struct ClaudeTokenMeter;
pub struct O200kTokenMeter;

pub static CLAUDE_TOKEN_METER: ClaudeTokenMeter = ClaudeTokenMeter;
pub static O200K_TOKEN_METER: O200kTokenMeter = O200kTokenMeter;

impl TokenMeter for ClaudeTokenMeter {
    fn count(&self, text: &str) -> u32 {
        cairn_tokenize::count(text, Family::V5)
    }

    fn cut(&self, text: &str, budget: u32) -> Cut {
        let cut = cairn_tokenize::cut(text, budget, Family::V5);
        Cut {
            byte_offset: cut.byte_offset,
            tokens: cut.tokens,
        }
    }
}

fn o200k() -> &'static CoreBPE {
    static BPE: OnceLock<CoreBPE> = OnceLock::new();
    BPE.get_or_init(|| o200k_base().expect("o200k_base BPE initializes"))
}

impl TokenMeter for O200kTokenMeter {
    fn count(&self, text: &str) -> u32 {
        o200k()
            .encode_ordinary(text)
            .len()
            .try_into()
            .unwrap_or(u32::MAX)
    }

    fn cut(&self, text: &str, budget: u32) -> Cut {
        let tokens = o200k().encode_ordinary(text);
        if tokens.len() <= budget as usize {
            return Cut {
                byte_offset: text.len(),
                tokens: tokens.len() as u32,
            };
        }
        let token_bytes: Vec<usize> = o200k()
            ._decode_native_and_split(tokens)
            .map(|bytes| bytes.len())
            .collect();
        let start = token_bytes
            .iter()
            .take(budget.saturating_sub(1) as usize)
            .sum::<usize>();
        let end = token_bytes
            .iter()
            .take(budget as usize + 1)
            .sum::<usize>()
            .min(text.len());
        let mut best = Cut {
            byte_offset: 0,
            tokens: 0,
        };
        for byte_offset in text
            .char_indices()
            .map(|(at, _)| at)
            .chain(std::iter::once(text.len()))
            .filter(|at| *at >= start && *at <= end)
        {
            let count = self.count(&text[..byte_offset]);
            if count <= budget && byte_offset >= best.byte_offset {
                best = Cut {
                    byte_offset,
                    tokens: count,
                };
            }
        }
        best
    }
}

/// Select a meter from durable or explicitly supplied backend/model metadata.
/// Missing and unrecognized backends retain Cairn's Claude-default semantics.
pub fn meter_for_backend_model(
    backend: Option<&str>,
    model: Option<&str>,
) -> &'static dyn TokenMeter {
    if let Some(model) = model.map(str::to_ascii_lowercase) {
        if model.contains("claude") {
            return &CLAUDE_TOKEN_METER;
        }
        let model_name = model.rsplit('/').next().unwrap_or(&model);
        let reasoning_family = ["o1", "o3", "o4"]
            .iter()
            .any(|family| model_name == *family || model_name.starts_with(&format!("{family}-")));
        if model.contains("gpt-") || model.contains("codex") || reasoning_family {
            return &O200K_TOKEN_METER;
        }
    }

    match backend
        .map(str::trim)
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("codex" | "openai") => &O200K_TOKEN_METER,
        _ => &CLAUDE_TOKEN_METER,
    }
}

/// Select the meter for a live process. The run id is the registry boundary:
/// an absent process deliberately falls through to the Claude default.
pub(crate) fn meter_for_process(
    process_state: &AgentProcessState,
    run_id: &str,
) -> &'static dyn TokenMeter {
    let backend = process_state.get_backend(run_id);
    let model = process_state.get_model(run_id);
    meter_for_backend_model(backend.as_deref(), model.as_deref())
}

/// Resolve the persisted identity for ingestion and historical backfills, where
/// the live process registry is intentionally not in scope. Backend identity is
/// canonical on `sessions`; the requested model is retained on the owning job.
pub(crate) async fn meter_for_run_conn(
    conn: &Connection,
    run_id: &str,
) -> DbResult<&'static dyn TokenMeter> {
    let mut rows = conn
        .query(
            "SELECT s.backend, j.model
             FROM runs r
             LEFT JOIN sessions s ON s.id = r.session_id
             LEFT JOIN jobs j ON j.id = r.job_id
             WHERE r.id = ?1
             LIMIT 1",
            params![run_id],
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(meter_for_backend_model(None, None));
    };
    let backend = row.opt_text(0)?;
    let model = row.opt_text(1)?;
    Ok(meter_for_backend_model(
        backend.as_deref(),
        model.as_deref(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backend_selection_defaults_unknown_to_claude() {
        assert!(std::ptr::eq(
            meter_for_backend_model(None, None),
            &CLAUDE_TOKEN_METER as &dyn TokenMeter
        ));
        for backend in ["openrouter", "ollama"] {
            assert!(std::ptr::eq(
                meter_for_backend_model(Some(backend), Some("auto")),
                &CLAUDE_TOKEN_METER as &dyn TokenMeter
            ));
        }
        assert!(std::ptr::eq(
            meter_for_backend_model(Some("future-backend"), None),
            &CLAUDE_TOKEN_METER as &dyn TokenMeter
        ));
    }

    #[test]
    fn compatible_backends_use_o200k_unless_model_is_claude() {
        for backend in ["codex", "openai"] {
            assert!(std::ptr::eq(
                meter_for_backend_model(Some(backend), None),
                &O200K_TOKEN_METER as &dyn TokenMeter
            ));
        }
        assert!(std::ptr::eq(
            meter_for_backend_model(Some("openrouter"), Some("anthropic/claude-sonnet-4-5")),
            &CLAUDE_TOKEN_METER as &dyn TokenMeter
        ));
        for model in [
            "openai/o3-mini",
            "openai/o4-mini",
            "openai/o1-preview",
            "o3",
        ] {
            assert!(std::ptr::eq(
                meter_for_backend_model(Some("openrouter"), Some(model)),
                &O200K_TOKEN_METER as &dyn TokenMeter
            ));
        }
    }

    #[test]
    fn adapters_delegate_count_and_cut_to_their_tokenizers() {
        let text = "hello, tokenizer";
        assert_eq!(
            CLAUDE_TOKEN_METER.count(text),
            cairn_tokenize::count(text, Family::V5)
        );
        assert_eq!(O200K_TOKEN_METER.cut("aaaaaaaa", 1).byte_offset, 8);
        let cut = O200K_TOKEN_METER.cut(text, 2);
        assert!(cut.tokens <= 2);
        assert!(text.is_char_boundary(cut.byte_offset));
        assert_eq!(
            cut.tokens,
            O200K_TOKEN_METER.count(&text[..cut.byte_offset])
        );
    }
}
