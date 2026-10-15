//! What the CLI providers share when they serve a response.
//!
//! Claude and Codex answer [`super::AgentBackend::complete`] with one tool-free
//! round trip through the facet: a session opened with no MCP attachment,
//! awaited with [`cairn_models::session::one_shot::await_one_shot`]. This module
//! holds the pieces around that call both backends need the same way: the
//! working directory, the default instructions, the token rule, and the
//! error mapping.

use std::path::PathBuf;

use cairn_models::interface::session::SessionError;
use cairn_models::session::one_shot::OneShotError;
use cairn_models::wire::TurnUsage;

use super::{CompletionError, CompletionRequest, CompletionTokens};

/// The least time a CLI one-shot is given. A CLI pays a start before it asks
/// anything (measured: a Claude Haiku one-shot about 4 to 5 seconds end to
/// end, a Codex one about 7), so the responses default of 15 seconds, sized
/// for an HTTP round trip, would fail real prompts for no reason.
const CLI_ONE_SHOT_MIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// How long a CLI one-shot may run: the request's own timeout, raised to the
/// CLI floor.
pub(crate) fn one_shot_timeout(request: &CompletionRequest) -> std::time::Duration {
    request.timeout.max(CLI_ONE_SHOT_MIN_TIMEOUT)
}

/// Instructions for a response that brings none of its own.
const TOOL_FREE_INSTRUCTIONS: &str =
    "Answer the user's message directly and completely. You have no tools.";

/// A response's own working directory, removed when the response is done.
///
/// A one-shot needs somewhere to run and to place its system prompt, and it
/// must not be a repository. This is the one place responses assume the CLI
/// runs on this machine; a host that runs sessions elsewhere replaces it.
pub(crate) struct ScratchGuard(PathBuf);

impl ScratchGuard {
    pub(crate) fn path(&self) -> String {
        self.0.to_string_lossy().into_owned()
    }
}

impl Drop for ScratchGuard {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_dir_all(&self.0) {
            log::debug!("response scratch {} not removed: {error}", self.0.display());
        }
    }
}

/// A fresh directory under Cairn's scratch root. Hidden (`.responses`) so it
/// is never mistaken for a job's scratch directory.
pub(crate) fn response_scratch_dir() -> Result<ScratchGuard, CompletionError> {
    let dir = cairn_common::scratch::scratch_root()
        .join(".responses")
        .join(uuid::Uuid::new_v4().to_string());
    std::fs::create_dir_all(&dir).map_err(|error| {
        CompletionError::Upstream(format!(
            "could not create a response working directory: {error}"
        ))
    })?;
    Ok(ScratchGuard(dir))
}

/// The system instructions a one-shot runs with.
pub(crate) fn instructions(request: &CompletionRequest) -> String {
    request
        .system
        .clone()
        .filter(|system| !system.trim().is_empty())
        .unwrap_or_else(|| TOOL_FREE_INSTRUCTIONS.to_string())
}

/// The preset options a response carries, as a session would read them.
pub(crate) fn runtime_extras(request: &CompletionRequest) -> crate::models::RuntimeExtras {
    serde_json::from_value(request.extras.clone()).unwrap_or_default()
}

/// Tokens as the responses journal counts them: the whole prompt in, the way
/// an HTTP backend's `prompt_tokens` does. The facet's one-shot usage is
/// already the neutral shape, whose `prompt_tokens` counts the cached parts.
pub(crate) fn whole_prompt_tokens(usage: Option<&TurnUsage>) -> CompletionTokens {
    let Some(usage) = usage else {
        return CompletionTokens::default();
    };
    let count = |tokens: Option<i32>| u64::try_from(tokens.unwrap_or(0)).unwrap_or(0);
    CompletionTokens {
        input: Some(count(usage.prompt_tokens)),
        output: Some(count(usage.completion_tokens)),
    }
}

pub(crate) fn map_open_error(error: SessionError) -> CompletionError {
    CompletionError::Upstream(error.to_string())
}

pub(crate) fn map_one_shot_error(error: OneShotError) -> CompletionError {
    match error {
        OneShotError::Timeout => CompletionError::Timeout,
        OneShotError::Failed(text) => CompletionError::Upstream(text),
        OneShotError::Protocol(text) => CompletionError::InvalidResponse(text),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_journal_counts_the_whole_prompt() {
        // 12 uncached prompt tokens, 900 read from cache, 100 written to it.
        let usage = TurnUsage::from_anthropic(Some(12), Some(5), Some(900), Some(100), None);
        assert_eq!(
            whole_prompt_tokens(Some(&usage)),
            CompletionTokens {
                input: Some(1012),
                output: Some(5)
            }
        );
        assert_eq!(whole_prompt_tokens(None), CompletionTokens::default());
    }

    #[test]
    fn a_cli_one_shot_gets_at_least_the_floor() {
        use std::time::Duration;
        let request = |secs| CompletionRequest {
            system: None,
            messages: Vec::new(),
            model: "m".into(),
            project_id: None,
            extras: serde_json::Value::Null,
            output_schema: None,
            timeout: Duration::from_secs(secs),
        };
        assert_eq!(one_shot_timeout(&request(15)), Duration::from_secs(30));
        assert_eq!(one_shot_timeout(&request(90)), Duration::from_secs(90));
    }

    #[test]
    fn one_shot_failures_map_onto_completion_errors() {
        assert_eq!(
            map_one_shot_error(OneShotError::Timeout),
            CompletionError::Timeout
        );
        assert!(matches!(
            map_one_shot_error(OneShotError::Failed("x".into())),
            CompletionError::Upstream(_)
        ));
        assert!(matches!(
            map_one_shot_error(OneShotError::Protocol("x".into())),
            CompletionError::InvalidResponse(_)
        ));
    }

    #[test]
    fn a_scratch_directory_is_removed_when_dropped() {
        let guard = response_scratch_dir().unwrap();
        let path = PathBuf::from(guard.path());
        assert!(path.is_dir());
        drop(guard);
        assert!(!path.exists());
    }
}
