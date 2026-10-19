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

use crate::models::SessionStatus;
use crate::orchestrator::Orchestrator;
use crate::sessions::queries::RotationCommit;
use crate::storage::LocalDb;

/// Apply the provider-change lifecycle for `job_id` when its open session is on
/// another backend. Returns whether a rotation was performed.
///
/// Either way the job ends up carrying `model`, and this is the only path that
/// writes it. `jobs.model` is the authoritative launch fact — a host agent
/// snapshot's `selection` is DERIVED from it and re-resolves every turn — so
/// keeping the write here, beside the session change it must agree with, is what
/// stops the two from being separately committable.
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
