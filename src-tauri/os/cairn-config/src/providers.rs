//! Preset resolution of the backend a session runs on. The shipped provider
//! catalog itself lives in `cairn_llm_types::catalog`.

use cairn_llm_types::catalog::resolved_backend_for_model;

/// The backend a session will actually run on, given the agent's resolved
/// selection and the model persisted on the job.
///
/// A model and the provider that serves it are ONE fact. Every site that needs
/// the provider asks here — the spawn in `start_agent_session`, the rotation
/// comparison in `continue_job_impl`, call admission, the session row a new job
/// is created with — so none of them can answer differently from the process
/// that actually starts.
///
/// `jobs.model` is a concrete model: written from the atomic selection the job
/// launched with, rewritten only by an explicit model edit, and handed verbatim
/// to the CLI at spawn. So when a model is present it decides the provider, with
/// one exception — a selection naming that SAME model speaks for it, because an
/// atomic `{ backend, model }` pair is the only thing able to name a provider a
/// model name cannot carry (an Ollama tag, a custom backend; see
/// [`CLAUDE_FAMILY_BACKEND`]).
///
/// The exception is deliberately narrow. An agent config resolved against
/// configuration that has since moved — a retuned tier preset, a changed
/// `active_backend` — carries a selection for a DIFFERENT model than the job
/// persisted, and letting it name the provider splits the pair: that is
/// CAIRN-3798, where live Claude `fable` threads were rotated onto Codex, which
/// then rejected the model outright. An agent's default may choose a model for a
/// job that has none; it may not reinterpret one the job already has.
///
/// With no job model the ladder is the agent's own: its atomic selection, then
/// its authored `backend_preference`. `None` collapses to Claude in
/// `backend_for_name`.
pub fn effective_backend_name(
    selection: Option<&crate::models::ModelSelection>,
    backend_preference: Option<&str>,
    model: Option<&str>,
) -> Option<String> {
    let Some(model) = model else {
        return selection
            .map(|selection| selection.backend.clone())
            .or_else(|| backend_preference.map(str::to_string));
    };
    match selection.filter(|selection| selection.model.as_str() == model) {
        Some(selection) => Some(selection.backend.clone()),
        None => Some(resolved_backend_for_model(model).to_string()),
    }
}

#[cfg(test)]
mod effective_selection_tests {
    use super::*;
    use crate::models::Model;
    fn selection(backend: &str, model: &str) -> crate::models::ModelSelection {
        crate::models::ModelSelection::new(backend, Model::new(model))
    }

    /// The CAIRN-3798 regression, stated as the resolution that produces it: a
    /// thread persisted on `fable` whose agent default has since resolved to a
    /// Codex model. The selection describes a different model, so it does not
    /// get to name the provider for this one — `fable` runs on Claude, which is
    /// what both the rotation check and the spawn must see.
    #[test]
    fn an_agent_default_never_reinterprets_the_job_model() {
        assert_eq!(
            effective_backend_name(
                Some(&selection("codex", "gpt-5.6-sol")),
                Some("codex"),
                Some("fable")
            )
            .as_deref(),
            Some("claude")
        );
    }

    /// The deliberate switch, which is the same question answered the other way:
    /// the job's model IS the Codex one, so Codex is what serves it.
    #[test]
    fn an_explicit_codex_model_resolves_to_codex() {
        assert_eq!(
            effective_backend_name(
                Some(&selection("claude", "fable")),
                None,
                Some("gpt-5.6-sol")
            )
            .as_deref(),
            Some("codex")
        );
    }

    /// A selection naming the job's own model is the atomic pair, and it is the
    /// only authority that can spell a provider the model name cannot carry.
    #[test]
    fn a_matching_selection_speaks_for_its_model() {
        assert_eq!(
            effective_backend_name(
                Some(&selection("ollama", "gemma4:e4b")),
                None,
                Some("gemma4:e4b")
            )
            .as_deref(),
            Some("ollama")
        );
        // An OpenRouter reroute persists the OpenRouter model id, so the pair
        // survives a restart rebuilt from `jobs.model` alone.
        assert_eq!(
            effective_backend_name(
                Some(&selection("openrouter", "~anthropic/claude-sonnet-latest")),
                None,
                Some("~anthropic/claude-sonnet-latest")
            )
            .as_deref(),
            Some("openrouter")
        );
    }

    /// With no model persisted there is nothing to reinterpret, so the agent's
    /// own ladder answers: its atomic selection, then its authored preference.
    #[test]
    fn without_a_job_model_the_agent_ladder_answers() {
        assert_eq!(
            effective_backend_name(Some(&selection("codex", "gpt-5.6-sol")), None, None).as_deref(),
            Some("codex")
        );
        assert_eq!(
            effective_backend_name(None, Some("codex"), None).as_deref(),
            Some("codex")
        );
        assert_eq!(effective_backend_name(None, None, None), None);
    }
}
