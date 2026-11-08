//! Explicit provider changes performed by model edits.
//!
//! A model edit may remain inside the provider serving an open session, in
//! which case the provider handles it natively. Crossing providers invalidates
//! that session's native handle, so every edit surface lands here to close the
//! old session and eagerly create the correctly-labelled successor.
//!
//! The edit's durable state moves as ONE transaction, assembled in
//! [`crate::sessions::queries`]: the job's model, the session compare-and-swap,
//! the predecessor's close, and the boundary event marking the seam. Process
//! teardown happens AFTER that commit and only logs on failure. The asymmetry is
//! deliberate: a surviving process is reclaimed by the ordinary reaping path,
//! whereas a half-applied model change is recoverable by nothing, and the
//! teardown used to sit between the session read and the swap it invalidates.

use cairn_models::interface::{ModelScope, Models, OfferQuery};
use cairn_models::types::catalog::VerbKind;

use crate::models::{Model, ModelSelection, SessionStatus};
use crate::orchestrator::Orchestrator;
use crate::sessions::queries::RotationCommit;
use crate::storage::LocalDb;

/// The selection a model edit stores for `requested`: the session offer the
/// catalog resolves it to, with the native id and provider as the catalog
/// spells them ([`cairn_models::interface::Catalog::pinned_offer`]). A launch
/// hands the stored model to its provider verbatim, and a CLI that tolerates
/// `Opus` for `opus` would otherwise let whatever was typed become the job's
/// launch fact.
///
/// `provider` is the provider the edit was made under and narrows a bare
/// name; a `provider/model` name selects its own. An edit pins one concrete
/// model, so a pool is refused: choosing within pools is the agent's
/// allotment, edited through its snapshot's `models`.
pub fn resolve_model_edit(
    orch: &Orchestrator,
    project_id: Option<&str>,
    provider: Option<&str>,
    requested: &str,
) -> Result<ModelSelection, String> {
    let scope = ModelScope {
        project_id: project_id.filter(|id| !id.is_empty()).map(str::to_string),
    };
    let catalog = orch
        .catalog(&scope)
        .map_err(|error| format!("the model catalog is unavailable: {error}"))?;
    let refusal = match catalog.pinned_offer(OfferQuery {
        requested: Some(requested),
        provider,
        ..OfferQuery::new(VerbKind::Session)
    }) {
        Ok(offer) => return Ok(ModelSelection::new(offer.provider, Model::new(offer.model))),
        Err(refusal) => refusal,
    };
    let name = requested.trim();
    let pool = orch
        .effective_models(scope.project_id.as_deref())
        .or_else(|_| orch.effective_models(None))
        .ok()
        .and_then(|models| {
            [Some(name), name.split_once('/').map(|(_, rest)| rest)]
                .into_iter()
                .flatten()
                .find(|id| models.pool(id).is_some())
                .map(str::to_string)
        });
    Err(match pool {
        Some(pool) => format!(
            "'{name}' names the pool '{pool}', and a model edit pins one model: name it as \
             provider/model (for example claude/opus), or edit the agent's allotment \
             (snapshot: {{models: [...]}}) to have it select within pools"
        ),
        None => refusal.to_string(),
    })
}

/// Apply the provider-change lifecycle for `job_id` when its open session is on
/// another backend. Returns whether a rotation was performed.
///
/// Either way the job ends up carrying `model`, and this is the only path that
/// writes it. `jobs.model` is the authoritative launch fact — a host agent
/// snapshot's `selection` is DERIVED from it and re-resolves every turn — so
/// keeping the write here, beside the session change it must agree with, is what
/// stops the two from being separately committable. A newly edited model comes
/// here as [`resolve_model_edit`] spelled it, never as it was typed.
pub async fn apply_for_job(
    orch: &Orchestrator,
    db: &LocalDb,
    job_id: &str,
    to_backend: &str,
    model: Option<&str>,
) -> Result<bool, String> {
    // `current_session_id` is NULL for a job that has not opened a session yet,
    // which is an ordinary state for a model edit to meet: `query_opt_text`
    // reads the NULL as no session, where `query_text` would refuse the row.
    let open_session = match db
        .query_opt_text(
            "SELECT current_session_id FROM jobs WHERE id = ?1",
            (job_id.to_string(),),
        )
        .await
        .map_err(|error| format!("Failed to read the job's current session: {error}"))?
    {
        Some(session_id) => Some(crate::sessions::queries::get(db, &session_id).await?),
        None => None,
    };

    // Only an open session holds a native handle worth invalidating, and only a
    // provider crossing invalidates it: a same-provider model change is served
    // in place by the running conversation, which just needs the column written.
    let Some(session) = open_session.filter(|session| {
        session.status == SessionStatus::Open && !session.backend.eq_ignore_ascii_case(to_backend)
    }) else {
        set_job_model(db, job_id, model).await?;
        return Ok(false);
    };

    // The boundary belongs to the run the operator is reading: the live one if
    // the job is working, otherwise the newest one it has.
    let active_run_id = db
        .query_text(
            "SELECT id FROM runs WHERE job_id = ?1 AND status IN ('starting', 'live')
             ORDER BY created_at DESC, rowid DESC LIMIT 1",
            (job_id.to_string(),),
        )
        .await
        .map_err(|error| format!("Failed to resolve the job's live run: {error}"))?;
    let latest_run_id = db
        .query_text(
            "SELECT id FROM runs WHERE job_id = ?1 ORDER BY created_at DESC, rowid DESC LIMIT 1",
            (job_id.to_string(),),
        )
        .await
        .map_err(|error| format!("Failed to resolve the job's latest run: {error}"))?;

    let _successor = crate::sessions::queries::rotate_job_session_with_backend(
        db,
        &session,
        job_id,
        to_backend,
        crate::sessions::queries::RotationReason::ProviderChange,
        Some(RotationCommit::provider_change(
            &session.backend,
            to_backend,
            model,
            active_run_id.clone().or_else(|| latest_run_id.clone()),
        )),
        orch.services.emitter.as_ref(),
    )
    .await?;

    // Past this point the transition is durable and self-consistent. A teardown
    // failure is reported but not returned: the edit did happen, and saying it
    // failed would send the operator to repair state that is already correct.
    if let Some(run_id) = active_run_id.as_deref() {
        crate::orchestrator::lifecycle::stop_active_turn_for_run(orch, run_id, true);
        if let Err(error) = crate::orchestrator::lifecycle::kill_session_with_reason(
            orch,
            run_id,
            "provider_change",
        ) {
            log::warn!(
                "Provider change for job {job_id} committed, but tearing down run {run_id} failed: {error}"
            );
        }
    }

    Ok(true)
}

/// Write the job's model for an edit that leaves the running session in place.
async fn set_job_model(db: &LocalDb, job_id: &str, model: Option<&str>) -> Result<(), String> {
    let Some(model) = model else {
        return Ok(());
    };
    let job_id = job_id.to_string();
    let model = model.to_string();
    db.write(|conn| {
        let job_id = job_id.clone();
        let model = model.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE jobs SET model = ?1, updated_at = ?2 WHERE id = ?3",
                cairn_db::turso::params![
                    model.as_str(),
                    chrono::Utc::now().timestamp(),
                    job_id.as_str()
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|error| format!("Failed to update the job's model: {error}"))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::SearchIndex;
    use cairn_models::types::catalog::{
        claude_models, DiscoveredModel, ProviderModelCatalog, ServedVerb, Wire,
    };
    use std::sync::Arc;

    pub(crate) const QWEN_SLUG: &str = "qwen/qwen3.6-35b-a3b";
    pub(crate) const QWEN_LOCAL: &str = "Qwen/Qwen3.6-35B-A3B";

    fn chat(id: &str, slug: &str) -> DiscoveredModel {
        let mut model = claude_models().remove(0);
        model.id = id.to_string();
        model.model = id.to_string();
        model.display_name = id.to_string();
        model.canonical_slug = Some(slug.to_string());
        model.serves = Some(ServedVerb::conversation(Wire::OpenAiChatCompletions));
        model
    }

    /// Claude's aliases, and one chat model OpenRouter offers by its slug and
    /// Cairn inference by its real name, the two merged on that slug.
    pub(crate) async fn seed_edit_catalog(orch: &Orchestrator) {
        crate::fleet::models::tests::seed_catalog_placement(orch, QWEN_LOCAL).await;
        for (backend, models) in [
            ("claude", claude_models()),
            ("openrouter", vec![chat(QWEN_SLUG, QWEN_SLUG)]),
            ("inference", vec![chat(QWEN_LOCAL, QWEN_SLUG)]),
        ] {
            orch.seed_model_catalog_for_test(ProviderModelCatalog {
                backend: backend.to_string(),
                models,
                options: Vec::new(),
                refreshed_at: Some(1),
                error: None,
            });
        }
    }

    async fn orchestrator() -> Orchestrator {
        let local =
            LocalDb::open_migrated_for_test(tempfile::tempdir().unwrap().keep().join("t.db"))
                .await
                .unwrap();
        let search =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        let orch = OrchestratorBuilder::new(
            Arc::new(DbState::new(Arc::new(local), search)),
            Arc::new(TestServicesBuilder::new().build()),
            tempfile::tempdir().unwrap().keep(),
        )
        .build();
        seed_edit_catalog(&orch).await;
        orch
    }

    fn pinned(backend: &str, model: &str) -> Result<ModelSelection, String> {
        Ok(ModelSelection::new(backend, Model::new(model)))
    }

    #[tokio::test]
    async fn a_model_edit_stores_the_offer_as_the_catalog_spells_it() {
        let orch = orchestrator().await;
        let resolve = |provider: Option<&str>, requested: &str| {
            resolve_model_edit(&orch, None, provider, requested)
        };

        assert_eq!(resolve(Some("claude"), "Opus"), pinned("claude", "opus"));
        assert_eq!(resolve(None, "Opus"), pinned("claude", "opus"));
        assert_eq!(
            resolve(Some("claude"), "sonnet"),
            pinned("claude", "sonnet")
        );
        // A qualified id stores the native id and its provider, whatever
        // provider the edit was made under.
        assert_eq!(
            resolve(Some("claude"), "openrouter/qwen/qwen3.6-35b-a3b"),
            pinned("openrouter", QWEN_SLUG)
        );
        assert_eq!(
            resolve(None, "inference/Qwen/Qwen3.6-35B-A3B"),
            pinned("inference", QWEN_LOCAL)
        );
        // A bare id is narrowed by the provider the edit was made under.
        assert_eq!(
            resolve(Some("inference"), QWEN_LOCAL),
            pinned("inference", QWEN_LOCAL)
        );
    }

    #[tokio::test]
    async fn a_model_edit_refuses_what_it_cannot_pin() {
        let orch = orchestrator().await;
        let resolve = |provider: Option<&str>, requested: &str| {
            resolve_model_edit(&orch, None, provider, requested).unwrap_err()
        };

        let unoffered = resolve(Some("claude"), "opus-9");
        assert!(
            unoffered.contains(
                "no installed provider offers 'opus-9'; claude offers haiku, sonnet, opus, fable"
            ),
            "{unoffered}"
        );
        let ambiguous = resolve(None, QWEN_LOCAL);
        assert_eq!(
            ambiguous,
            "'Qwen/Qwen3.6-35B-A3B' is offered by inference (inference/Qwen/Qwen3.6-35B-A3B) and openrouter (openrouter/qwen/qwen3.6-35b-a3b): name one."
        );
        // A pool is the allotment's vocabulary, never a pin.
        let pool = orch.effective_models(None).unwrap().pools[0].id.clone();
        for requested in [pool.clone(), format!("claude/{pool}")] {
            let refusal = resolve(Some("claude"), &requested);
            assert!(
                refusal.contains(&format!("names the pool '{pool}'"))
                    && refusal.contains("snapshot: {models: [...]}"),
                "{refusal}"
            );
        }
    }
}
