use serde::{Deserialize, Serialize};

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
pub struct ContextTokenEventSnapshot {
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
    pub fn into_state(self, context_window: Option<i64>) -> ContextTokenState {
        let input_tokens = self.input_tokens.unwrap_or(0);
        let output_tokens = self.output_tokens.unwrap_or(0);
        // Persisted token components are disjoint for every backend.
        let used_tokens = input_tokens
            + self.cache_create_tokens.unwrap_or(0)
            + self.cache_read_tokens.unwrap_or(0)
            + output_tokens;

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

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot(backend: &str) -> ContextTokenEventSnapshot {
        ContextTokenEventSnapshot {
            run_id: "run".to_string(),
            session_id: "session".to_string(),
            backend: backend.to_string(),
            model: Some("model".to_string()),
            input_tokens: Some(1000),
            cache_read_tokens: Some(400),
            cache_create_tokens: Some(50),
            output_tokens: Some(200),
            thinking_tokens: Some(20),
            captured_at: 1,
        }
    }

    #[test]
    fn openrouter_occupancy_sums_disjoint_cache() {
        let state = snapshot("openrouter").into_state(Some(200_000));
        assert_eq!(state.used_tokens, 1_000 + 50 + 400 + 200);
        assert_eq!(state.context_window, Some(200_000));
    }

    #[test]
    fn codex_occupancy_sums_disjoint_cache() {
        let state = snapshot("codex").into_state(Some(258_400));
        assert_eq!(state.used_tokens, 1_000 + 50 + 400 + 200);
    }

    #[test]
    fn claude_occupancy_sums_separately_reported_cache() {
        // Anthropic reports cache tokens separately from input, so they add in.
        let state = snapshot("claude").into_state(Some(200_000));
        assert_eq!(state.used_tokens, 1_000 + 50 + 400 + 200);
    }
}
