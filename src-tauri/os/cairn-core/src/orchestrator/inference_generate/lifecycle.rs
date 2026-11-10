//! How a generation ends, and what an ending leaves behind.
//!
//! [`finalize_generation`] is the one terminal transition. Every way a
//! generation can end comes through it: the agent's own outcome
//! ([`record_outcome`]), its job actually ending ([`job_ended`]), the
//! operator stopping its agent or cancelling it ([`operator_stopped`],
//! [`cancel`]), its issue being resolved ([`issue_resolved`]), its draft's
//! verification settling ([`verification_settled`]), an exhausted candidate
//! budget, and startup ([`reconcile_on_startup`]).
//!
//! Verification decides what is kept. A generation whose current draft
//! revision was promoted by its own approval-bound verification pass ends
//! `verified` with that profile kept, whatever ended it, so a pass is not lost
//! to a crash that follows it. Every other ending revokes the generation's
//! authority in one transaction (its approval is discarded and its ownership
//! rows stop being current drafts) and then removes the draft under the
//! inventory lock. A draft a running verification still holds cannot be
//! removed yet: the row says so (`cleanup_pending`) rather than claiming it is
//! gone, and the removal is retried whenever a verification settles and at
//! startup. Ownership rows are receipts and are never deleted.
//!
//! Waiting is not ending. A question the agent asked, a queued launch, a turn
//! that ended normally and a session that resumes cold leave the generation
//! open; nothing here runs on a timer. An ended generation's agent stays a
//! conversation the operator can message, and resuming it neither restores
//! the draft nor refunds attempts. Its issue stays open.

use cairn_db::turso::params;
use cairn_inference::{DeploymentId, InferenceProfileId};
use serde_json::{json, Value};

use super::launch::{reconcile_starting, settle_starting, AGENT_MODE, LAUNCH_FAILED, STARTING};
use super::promotion::{self, load_owned, surface_of};
use super::{get_generation, list_generations, select_generations, GenerationRow, RUNNING};
use crate::orchestrator::inference::{
    load_inventory, now_ms, write_inventory_held, Authoring, Removals, INVENTORY,
};
use crate::orchestrator::inference_verify::{digest, load_verifications};
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};

/// Ended with its draft verified and kept.
pub const VERIFIED: &str = "verified";
/// Ended without a pass and without saying why: its agent's job ended, or the
/// upgrade retired its workflow.
pub const FAILED: &str = "failed";
/// Ended by the operator: a stop, a cancel, or its issue resolved.
pub const CANCELLED: &str = "cancelled";
/// Ended because no candidate it may still write could pass.
pub const EXHAUSTED: &str = "exhausted";

/// The outcomes a generation's agent reports for itself.
const OUTCOMES: &[&str] = &[VERIFIED, "cannotRun", "gaveUp"];

/// Every state a generation ends in.
const TERMINAL: &[&str] = &[
    VERIFIED,
    "cannotRun",
    "gaveUp",
    FAILED,
    CANCELLED,
    EXHAUSTED,
    LAUNCH_FAILED,
];

/// The workflow that ran generations before they became ordinary issues. It
/// is never respawned, whatever startup managed to retire.
pub(crate) const RETIRED_WORKFLOW: &str = "inference-generate";

/// Why the upgrade ended a generation its retired workflow was running.
pub(crate) const UPGRADE_REASON: &str =
    "Cairn was upgraded, and profile generation no longer runs \
     as a workflow, so the workflow running this generation was stopped and not restarted";

fn is_open(state: &str) -> bool {
    state == RUNNING || state == STARTING
}

/// The verification whose pass promoted `generation`'s draft as it stands
/// now, read from the database rather than from anything a caller supplies:
/// its ownership row promoted by exactly that verification, at the
/// generation's current revision and full digest, a stored pass of that
/// digest on the generation's pool, and the inventory's profile still exactly
/// what passed. A generation that claims its verification must have been
/// promoted by that claim; one from before claims were recorded only by a
/// pass of the profile as it stands.
async fn proven(db: &LocalDb, generation: &GenerationRow) -> Result<Option<String>, String> {
    let Some(draft) = generation.draft_profile.as_deref() else {
        return Ok(None);
    };
    let Some(owned) = load_owned(db).await?.remove(draft) else {
        return Ok(None);
    };
    let Some(verification) = owned.promoted.clone() else {
        return Ok(None);
    };
    if owned.generation != generation.id {
        return Ok(None);
    }
    match generation.verification_id.as_deref() {
        Some(claimed) => {
            if claimed != verification
                || owned.revision != generation.draft_revision
                || generation.draft_digest.as_deref() != Some(owned.profile_digest.as_str())
            {
                return Ok(None);
            }
        }
        None if generation.mode == AGENT_MODE => return Ok(None),
        None => {}
    }
    let Some(row) = load_verifications(db)
        .await?
        .into_iter()
        .find(|row| row.id == verification)
    else {
        return Ok(None);
    };
    if row.outcome != "passed"
        || row.profile != draft
        || row.pool != generation.pool
        || row.profile_digest != owned.profile_digest
    {
        return Ok(None);
    }
    let inventory = load_inventory(db).await?;
    let Some(profile) = inventory.profiles.get(&InferenceProfileId::from(draft)) else {
        return Ok(None);
    };
    Ok(
        (digest(profile) == owned.profile_digest && surface_of(profile) == owned.surface_digest)
            .then_some(verification),
    )
}

/// End generation `id` as `requested`, with `outcome` as its record. The one
/// terminal transition, and idempotent: on a generation that already ended it
/// changes nothing but retrying a pending draft removal, and answers the row
/// as it stands.
///
/// A current verification pass of the draft (see [`proven`]) ends it
/// `verified` and keeps the profile, whatever was requested. Without one,
/// `verified` is refused and the generation stays open; anything else ends it
/// as requested, revokes its approval and draft ownership, and removes the
/// draft, or leaves `cleanup_pending` while a verification holds it.
///
/// It takes the inventory lock, as [`promotion::settle`] does, so it is never
/// called with that lock held.
pub(crate) async fn finalize_generation(
    db: &LocalDb,
    id: &str,
    requested: &str,
    outcome: Option<&Value>,
) -> Result<GenerationRow, String> {
    finalize(db, id, requested, outcome)
        .await
        .map(|(row, _)| row)
}

/// [`finalize_generation`], also answering whether this call ended it.
async fn finalize(
    db: &LocalDb,
    id: &str,
    requested: &str,
    outcome: Option<&Value>,
) -> Result<(GenerationRow, bool), String> {
    if !TERMINAL.contains(&requested) {
        return Err(format!(
            "a generation ends {}, not {requested}",
            TERMINAL.join(", ")
        ));
    }
    let missing = || format!("there is no generation {id}");
    let before = get_generation(db, id).await?.ok_or_else(missing)?;
    if is_open(&before.state) {
        // Promote a pass its claim earned before deciding what to keep: a
        // settlement interrupted by a crash is completed here.
        if let Some(verification) = before.verification_id.as_deref() {
            promotion::settle(db, verification).await?;
        }
    }
    let held = INVENTORY.lock().await;
    let generation = get_generation(db, id).await?.ok_or_else(missing)?;
    if !is_open(&generation.state) {
        let row = remove_left_draft(db, generation, &held).await?;
        return Ok((row, false));
    }
    let kept = proven(db, &generation).await?;
    if requested == VERIFIED && kept.is_none() {
        return Err(format!(
            "generation {id} has no verification pass of its current draft{} on pool {}; verification decides",
            generation
                .draft_profile
                .as_deref()
                .map(|draft| format!(" {draft}"))
                .unwrap_or_default(),
            generation.pool
        ));
    }
    let now = now_ms();
    let id_owned = id.to_string();
    let ended = match &kept {
        Some(verification) => {
            let mut recorded = match outcome.cloned() {
                Some(Value::Object(map)) => Value::Object(map),
                Some(other) => json!({ "detail": other }),
                None => json!({}),
            };
            recorded["outcome"] = json!(VERIFIED);
            recorded["verification"] = json!(verification);
            if requested != VERIFIED {
                recorded["endedAs"] = json!(requested);
            }
            let recorded = recorded.to_string();
            db.write(move |conn| {
                let (id, recorded) = (id_owned.clone(), recorded.clone());
                Box::pin(async move {
                    let changed = conn
                        .execute(
                            "UPDATE inference_generations SET state = ?2, outcome_json = ?3, finished_at = ?4
                              WHERE id = ?1 AND state IN (?5, ?6)",
                            params![id.as_str(), VERIFIED, recorded.as_str(), now, RUNNING, STARTING],
                        )
                        .await?;
                    Ok(changed > 0)
                })
            })
            .await
        }
        None => {
            let recorded = outcome
                .cloned()
                .unwrap_or_else(|| json!({ "outcome": requested }))
                .to_string();
            let state = requested.to_string();
            db.write(move |conn| {
                let (id, recorded, state) = (id_owned.clone(), recorded.clone(), state.clone());
                Box::pin(async move {
                    let changed = conn
                        .execute(
                            "UPDATE inference_generations SET state = ?2, outcome_json = ?3, finished_at = ?4,
                                    approved_surface = NULL, approval_prompt_id = NULL,
                                    approval_run_id = NULL, approval_revision = NULL,
                                    cleanup_pending = (draft_profile IS NOT NULL)
                              WHERE id = ?1 AND state IN (?5, ?6)",
                            params![id.as_str(), state.as_str(), recorded.as_str(), now, RUNNING, STARTING],
                        )
                        .await?;
                    if changed > 0 {
                        conn.execute(
                            "UPDATE inference_generated_profiles SET active = 0 WHERE generation_id = ?1",
                            params![id.as_str()],
                        )
                        .await?;
                    }
                    Ok(changed > 0)
                })
            })
            .await
        }
    }
    .map_err(|error| error.to_string())?;
    let row = get_generation(db, id).await?.ok_or_else(missing)?;
    let row = remove_left_draft(db, row, &held).await?;
    Ok((row, ended))
}

/// Remove the draft an ended, unverified generation left in the inventory,
/// with the deployments that name it, and forget it. A draft verification
/// proved is kept. A removal the inventory refuses (a verification still
/// holds the draft) leaves `cleanup_pending` set for the next retry.
///
/// Only the generation its ownership row names may remove a profile. Once an
/// ended generation's draft is gone, the draft door lets a later generation
/// write the same profile id, so a stale pointer on the old row names the new
/// owner's draft: that pointer is forgotten and the profile and its
/// deployments are left alone. A draft present in the inventory with no
/// ownership row is not removed either; it stays pending until the startup
/// backfill records whose it is.
async fn remove_left_draft(
    db: &LocalDb,
    generation: GenerationRow,
    held: &tokio::sync::MutexGuard<'_, ()>,
) -> Result<GenerationRow, String> {
    let Some(draft) = generation.draft_profile.clone() else {
        return Ok(generation);
    };
    if is_open(&generation.state) || generation.state == VERIFIED {
        return Ok(generation);
    }
    if proven(db, &generation).await?.is_some() {
        return Ok(generation);
    }
    let inventory = load_inventory(db).await?;
    let profile = InferenceProfileId::from(draft.as_str());
    let owner = load_owned(db)
        .await?
        .remove(&draft)
        .map(|owned| owned.generation);
    match owner.as_deref() {
        Some(owner) if owner != generation.id => {
            log::info!(
                "inference: generation {}'s draft {draft} now belongs to generation {owner}; \
                 forgetting the old pointer without removing it",
                generation.id
            );
            return forget_draft(db, &generation.id, &draft).await;
        }
        None if inventory.profiles.contains_key(&profile) => {
            log::warn!(
                "inference: generation {}'s draft {draft} has no ownership record, so it is \
                 not removed until one names this generation",
                generation.id
            );
            return mark_pending(db, &generation.id, &draft).await;
        }
        _ => {}
    }
    if inventory.profiles.contains_key(&profile) {
        let deployments: Vec<DeploymentId> = inventory
            .deployments
            .values()
            .filter(|deployment| deployment.profile == profile)
            .map(|deployment| deployment.id.clone())
            .collect();
        let removal = Authoring::Patch {
            upsert: None,
            remove: Removals {
                profiles: vec![profile],
                deployments,
                ..Removals::default()
            },
        };
        if let Err(why) = write_inventory_held(db, &removal, held).await {
            log::info!(
                "inference: generation {}'s draft {draft} stays until it can be removed: {why}",
                generation.id
            );
            return mark_pending(db, &generation.id, &draft).await;
        }
    }
    forget_draft(db, &generation.id, &draft).await
}

/// Leave generation `id`'s removal of `draft` pending for the next retry.
async fn mark_pending(db: &LocalDb, id: &str, draft: &str) -> Result<GenerationRow, String> {
    db.execute(
        "UPDATE inference_generations SET cleanup_pending = 1 WHERE id = ?1 AND draft_profile = ?2",
        params![id, draft],
    )
    .await
    .map_err(|error| error.to_string())?;
    get_generation(db, id)
        .await?
        .ok_or_else(|| format!("there is no generation {id}"))
}

/// Clear generation `id`'s pointer to `draft`, and any removal pending on it.
async fn forget_draft(db: &LocalDb, id: &str, draft: &str) -> Result<GenerationRow, String> {
    db.execute(
        "UPDATE inference_generations SET draft_profile = NULL, cleanup_pending = 0
          WHERE id = ?1 AND draft_profile = ?2",
        params![id, draft],
    )
    .await
    .map_err(|error| error.to_string())?;
    get_generation(db, id)
        .await?
        .ok_or_else(|| format!("there is no generation {id}"))
}

/// Retry removing every draft an ended, unverified generation still leaves in
/// the inventory: a removal a verification held, or a draft an earlier Cairn
/// left behind. Answers how many generations still wait on one.
pub(crate) async fn retry_cleanup(db: &LocalDb) -> Result<usize, String> {
    let left = select_generations(
        db,
        "WHERE draft_profile IS NOT NULL AND state NOT IN ('running', 'starting', 'verified')",
        None,
    )
    .await?;
    let mut pending = 0;
    for generation in left {
        let held = INVENTORY.lock().await;
        let id = generation.id.clone();
        match remove_left_draft(db, generation, &held).await {
            Ok(row) if !row.cleanup_pending => {}
            Ok(_) => pending += 1,
            // One removal that fails stays pending; the others still run.
            Err(error) => {
                log::warn!("inference: generation {id}'s draft could not be removed: {error}");
                pending += 1;
            }
        }
    }
    Ok(pending)
}

/// What follows once verification `verification`'s outcome is stored, for the
/// generation that claimed it: its pass promotes the draft and ends the
/// generation `verified`; a failure of its third candidate leaves it nothing
/// it may still verify, so it ends `exhausted`. Then every pending draft
/// removal is retried, since the verification no longer holds what it loaded.
/// Logs rather than raises: the verification's outcome stands either way, and
/// startup settles again.
pub(crate) async fn verification_settled(db: &LocalDb, verification: &str) {
    if let Err(error) = settle_verification(db, verification).await {
        log::warn!("inference: verification {verification} could not be settled: {error}");
    }
}

async fn settle_verification(db: &LocalDb, verification: &str) -> Result<(), String> {
    promotion::settle(db, verification).await?;
    let claimant = select_generations(
        db,
        "WHERE verification_id = ?1",
        Some(verification.to_string()),
    )
    .await?
    .pop()
    .filter(GenerationRow::is_open);
    if let Some(generation) = claimant {
        let row = load_verifications(db)
            .await?
            .into_iter()
            .find(|row| row.id == verification);
        match row {
            Some(row) if row.outcome == "passed" => {
                if let Err(why) = finalize(db, &generation.id, VERIFIED, None).await {
                    log::info!("inference: verification {verification} passed, but {why}");
                }
            }
            Some(row) if row.outcome != "running" && generation.attempts >= 3 => {
                let outcome = json!({
                    "outcome": EXHAUSTED,
                    "why": format!(
                        "its third and last candidate's verification {verification} {}{}",
                        row.outcome,
                        row.reason.map(|reason| format!(": {reason}")).unwrap_or_default()
                    ),
                });
                finalize(db, &generation.id, EXHAUSTED, Some(&outcome)).await?;
            }
            _ => {}
        }
    }
    retry_cleanup(db).await?;
    Ok(())
}

/// End generation `id` as `requested`, then withdraw any question its agent's
/// job still waits on and tell Settings and the offers.
pub(crate) async fn end(
    orch: &Orchestrator,
    id: &str,
    requested: &str,
    outcome: Option<&Value>,
) -> Result<GenerationRow, String> {
    let (row, ended) = finalize(&orch.db.local, id, requested, outcome).await?;
    if ended {
        if !row.job_id.is_empty() {
            withdraw_questions(orch, &row.job_id).await;
        }
        announce(orch);
    }
    Ok(row)
}

fn announce(orch: &Orchestrator) {
    orch.notifier.emit_change("inference_generations");
    orch.spawn_provider_model_catalog_refresh(vec![
        crate::backends::inference::INFERENCE_BACKEND_KEY.to_string(),
    ]);
}

/// End a generation with the outcome its own agent (`caller_job`) reports.
/// `verified` is accepted only when the database holds a current pass of the
/// draft (whatever verification the payload names); any other outcome removes
/// the draft. Answers the summary and the ended row.
pub async fn record_outcome(
    db: &LocalDb,
    caller_job: &str,
    id: &str,
    outcome: &Value,
) -> Result<(String, GenerationRow), String> {
    let generation = get_generation(db, id)
        .await?
        .ok_or_else(|| format!("there is no generation {id}"))?;
    if generation.job_id.is_empty() || generation.job_id != caller_job {
        return Err("only the generation's own agent reports its outcome".into());
    }
    if !generation.is_open() {
        return Err(format!(
            "generation {id} has already ended {}",
            generation.state
        ));
    }
    let state = outcome
        .get("outcome")
        .and_then(Value::as_str)
        .filter(|state| OUTCOMES.contains(state))
        .ok_or_else(|| format!("outcome.outcome is one of {}", OUTCOMES.join(", ")))?;
    let (ended, now) = finalize(db, id, state, Some(outcome)).await?;
    if !now {
        return Err(format!(
            "generation {id} ended {} while this was recorded",
            ended.state
        ));
    }
    Ok((summary(&generation, &ended), ended))
}

fn summary(before: &GenerationRow, ended: &GenerationRow) -> String {
    let id = &ended.id;
    if ended.state == VERIFIED {
        return format!(
            "Generation {id} verified profile {}, which is kept.",
            ended.draft_profile.as_deref().unwrap_or_default()
        );
    }
    let mut said = format!("Generation {id} ended {}.", ended.state);
    match (before.draft_profile.as_deref(), &ended.draft_profile) {
        (Some(draft), None) => said.push_str(&format!(" Removed its draft profile {draft}.")),
        (_, Some(draft)) => said.push_str(&format!(
            " Its draft profile {draft} is removed once the verification holding it ends."
        )),
        (None, None) => {}
    }
    said
}

/// Cancel a generation from Settings: its agent is stopped, as the operator's
/// Stop would (its conversation stays open to messages), and it ends
/// `cancelled`.
pub async fn cancel(orch: &Orchestrator, id: &str) -> Result<String, String> {
    let generation = get_generation(&orch.db.local, id)
        .await?
        .ok_or_else(|| format!("there is no generation {id}"))?;
    if !is_open(&generation.state) {
        return Err(format!(
            "generation {id} has already ended {}",
            generation.state
        ));
    }
    if !generation.job_id.is_empty() {
        if generation.mode == AGENT_MODE {
            let (stopper, job_id) = (orch.clone(), generation.job_id.clone());
            let stopped = tokio::task::spawn_blocking(move || {
                crate::orchestrator::lifecycle::operator_stop_job(&stopper, &job_id)
            })
            .await
            .map_err(|error| error.to_string())?;
            if let Err(error) = stopped {
                log::warn!("inference: stopping generation {id}'s agent failed: {error}");
            }
        } else if let Err(error) =
            crate::orchestrator::lifecycle::stop_workflow(orch, &generation.job_id).await
        {
            log::warn!("inference: stopping generation {id}'s workflow failed: {error}");
        }
    }
    let outcome = json!({ "outcome": CANCELLED, "why": "the operator cancelled it" });
    let ended = end(orch, id, CANCELLED, Some(&outcome)).await?;
    Ok(format!(
        "Cancelled generation {id}. {}",
        summary(&generation, &ended)
    ))
}

/// A job's status, read from the database its rows live in: `Ok(None)` when
/// it has no row, an error when that database cannot be read.
async fn job_status(orch: &Orchestrator, job_id: &str) -> Result<Option<String>, String> {
    let db = crate::execution::routing::owning_db_for_job(&orch.db, job_id)
        .await
        .map_err(|error| error.to_string())?;
    db.query_opt(
        "SELECT status FROM jobs WHERE id = ?1",
        params![job_id],
        |row| row.text(0),
    )
    .await
    .map_err(|error| error.to_string())
}

fn is_terminal_job(status: &str) -> bool {
    matches!(status, "complete" | "failed" | "cancelled")
}

/// The generation whose agent runs `job_id`, while it is open.
async fn open_for_job(orch: &Orchestrator, job_id: &str) -> Result<Option<GenerationRow>, String> {
    if job_id.is_empty() {
        return Ok(None);
    }
    super::open_generation_for_job(&orch.db.local, job_id).await
}

/// The generation still `starting` in job `job_id`'s execution. An agent
/// that has not bound yet is not the job of any generation, so it is found
/// through the execution it was queued in.
async fn starting_for_job(
    orch: &Orchestrator,
    job_id: &str,
) -> Result<Option<GenerationRow>, String> {
    let db = crate::execution::routing::owning_db_for_job(&orch.db, job_id)
        .await
        .map_err(|error| error.to_string())?;
    let execution = db
        .query_opt(
            "SELECT execution_id FROM jobs WHERE id = ?1",
            params![job_id],
            |row| row.opt_text(0),
        )
        .await
        .map_err(|error| error.to_string())?
        .flatten();
    let Some(execution) = execution else {
        return Ok(None);
    };
    Ok(select_generations(
        &orch.db.local,
        "WHERE execution_id = ?1 AND state = 'starting'",
        Some(execution),
    )
    .await?
    .pop())
}

/// A job of a generation's execution ended or was stopped before the
/// generation's agent bound: settle the generation by where its execution
/// actually stands. One that can no longer launch its agent ends
/// `launchFailed`; a held or queued one keeps waiting.
async fn settle_unbound(orch: &Orchestrator, job_id: &str) -> Result<(), String> {
    if job_id.is_empty() {
        return Ok(());
    }
    let Some(generation) = starting_for_job(orch, job_id).await? else {
        return Ok(());
    };
    if settle_starting(orch, &generation).await? {
        announce(orch);
    }
    Ok(())
}

/// A job ended: a generation its agent ran that has not reported an outcome
/// ends `failed`, keeping its draft only if verification already passed it.
/// A job that has resumed since is left alone.
pub(crate) async fn job_ended(orch: &Orchestrator, job_id: &str) -> Result<(), String> {
    let Some(generation) = open_for_job(orch, job_id).await? else {
        return settle_unbound(orch, job_id).await;
    };
    let Some(status) = job_status(orch, job_id)
        .await?
        .filter(|status| is_terminal_job(status))
    else {
        return Ok(());
    };
    let outcome = json!({
        "outcome": FAILED,
        "why": format!("its agent's job ended {status} without reporting an outcome"),
    });
    end(orch, &generation.id, FAILED, Some(&outcome)).await?;
    Ok(())
}

/// The operator stopped job `job_id`'s agent: a generation it runs is
/// cancelled, and its draft authority with it. The conversation stays open.
pub(crate) async fn operator_stopped(orch: &Orchestrator, job_id: &str) -> Result<(), String> {
    let Some(generation) = open_for_job(orch, job_id).await? else {
        return settle_unbound(orch, job_id).await;
    };
    let outcome = json!({
        "outcome": CANCELLED,
        "why": "the operator stopped its agent. The conversation stays open to messages; start a new generation from Settings > Inference to generate again",
    });
    end(orch, &generation.id, CANCELLED, Some(&outcome)).await?;
    Ok(())
}

/// Issue `issue_id` was merged or closed: every generation still open on it
/// is cancelled.
pub(crate) async fn issue_resolved(orch: &Orchestrator, issue_id: &str) -> Result<(), String> {
    let open = select_generations(
        &orch.db.local,
        "WHERE issue_id = ?1 AND state IN ('running', 'starting')",
        Some(issue_id.to_string()),
    )
    .await?;
    for generation in open {
        let outcome = json!({ "outcome": CANCELLED, "why": "its issue was resolved" });
        end(orch, &generation.id, CANCELLED, Some(&outcome)).await?;
    }
    Ok(())
}

/// Run `hook` for `job_id` on the current runtime, from the synchronous stop
/// and trigger paths, logging what fails.
fn spawn_hook<F, Fut>(orch: &Orchestrator, job_id: &str, what: &'static str, hook: F)
where
    F: FnOnce(Orchestrator, String) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<(), String>> + Send + 'static,
{
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        log::warn!("inference: no runtime to settle job {job_id}'s generation after {what}");
        return;
    };
    let (orch, job_id) = (orch.clone(), job_id.to_string());
    handle.spawn(async move {
        let id = job_id.clone();
        if let Err(error) = hook(orch, job_id).await {
            log::warn!("inference: settling job {id}'s generation after {what} failed: {error}");
        }
    });
}

/// [`job_ended`], from the job-ended trigger.
pub(crate) fn after_job_ended(orch: &Orchestrator, job_id: &str) {
    spawn_hook(orch, job_id, "its job ended", |orch, job_id| async move {
        job_ended(&orch, &job_id).await
    });
}

/// [`operator_stopped`], from the operator's Stop.
pub(crate) fn after_operator_stop(orch: &Orchestrator, job_id: &str) {
    spawn_hook(
        orch,
        job_id,
        "an operator stop",
        |orch, job_id| async move { operator_stopped(&orch, &job_id).await },
    );
}

/// Withdraw every question job `job_id` still waits on. A generation's
/// prompts can outlive its turn, so ending it withdraws them explicitly.
pub(crate) async fn withdraw_questions(orch: &Orchestrator, job_id: &str) {
    let Ok(db) = crate::execution::routing::owning_db_for_job(&orch.db, job_id).await else {
        return;
    };
    let job_id = job_id.to_string();
    let now = chrono::Utc::now().timestamp();
    let result = db
        .write(move |conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT p.id FROM prompts p JOIN runs r ON p.run_id = r.id \
                         WHERE r.job_id = ?1 AND p.response IS NULL AND p.withdrawn_at IS NULL",
                        (job_id.as_str(),),
                    )
                    .await?;
                let mut ids = Vec::new();
                while let Some(row) = rows.next().await? {
                    ids.push(row.text(0)?);
                }
                drop(rows);
                for id in &ids {
                    crate::mcp::handlers::planning::withdraw_prompt_conn(
                        conn,
                        id,
                        crate::mcp::handlers::permission::WithdrawalCause::OperatorStop,
                        now,
                    )
                    .await?;
                }
                Ok(ids)
            })
        })
        .await;
    match result {
        Ok(ids) => {
            for id in ids {
                let change = crate::notify::prompt_db_change_for_id(&db, &id, "update").await;
                let _ = orch.services.emitter.emit("db-change", change);
            }
        }
        Err(error) => {
            log::warn!("inference: withdrawing an ended generation's questions failed: {error}")
        }
    }
}

/// Startup's backstop, run before crashed workflows are redispatched:
///
/// 1. complete every pass a stopped Cairn did not get to promote;
/// 2. retire each generation a retired workflow still runs (see
///    [`retire_legacy`]);
/// 3. end every `starting` generation whose execution can no longer bind;
/// 4. end every bound generation whose agent's job ended while Cairn was down;
/// 5. retry every pending draft removal.
///
/// Every step runs, and every generation within one, whatever an earlier one
/// hit: one generation's failure is that generation's to retry next startup,
/// never a reason to leave the others unsettled. The failures are answered
/// together. A step skipped by an earlier failure loses nothing it guards:
/// [`finalize_generation`] completes its own generation's pending promotion,
/// and a draft without an ownership record is never removed.
///
/// Repeating it changes nothing.
pub async fn reconcile_on_startup(orch: &Orchestrator) -> Result<(), String> {
    let db = &orch.db.local;
    let mut failures = Vec::new();
    if let Err(error) = promotion::reconcile(db).await {
        failures.push(format!("settling unpromoted passes: {error}"));
    }
    match list_generations(db).await {
        Ok(generations) => {
            for generation in generations {
                if generation.mode == AGENT_MODE {
                    continue;
                }
                let retired = if is_open(&generation.state) {
                    retire_legacy(orch, &generation).await
                } else {
                    annotate_retired(orch, &generation).await
                };
                if let Err(error) = retired {
                    failures.push(format!("retiring generation {}: {error}", generation.id));
                }
            }
        }
        Err(error) => failures.push(format!("listing generations: {error}")),
    }
    if let Err(error) = reconcile_starting(orch).await {
        failures.push(format!("settling starting generations: {error}"));
    }
    match select_generations(
        db,
        "WHERE mode = ?1 AND state = 'running' AND job_id != ''",
        Some(AGENT_MODE.to_string()),
    )
    .await
    {
        Ok(bound) => {
            for generation in bound {
                if let Err(error) = end_if_job_gone(orch, &generation).await {
                    failures.push(format!("ending generation {}: {error}", generation.id));
                }
            }
        }
        Err(error) => failures.push(format!("listing bound generations: {error}")),
    }
    if let Err(error) = retry_cleanup(db).await {
        failures.push(format!("retrying draft removals: {error}"));
    }
    announce(orch);
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; "))
    }
}

/// End bound generation `generation` `failed` if its agent's job ended, or
/// vanished, while Cairn was down.
async fn end_if_job_gone(orch: &Orchestrator, generation: &GenerationRow) -> Result<(), String> {
    let why = match job_status(orch, &generation.job_id).await?.as_deref() {
        None => "its agent's job no longer exists".to_string(),
        Some(status) if is_terminal_job(status) => {
            format!("its agent's job ended {status} without reporting an outcome")
        }
        Some(_) => return Ok(()),
    };
    let outcome = json!({ "outcome": FAILED, "why": why });
    end(orch, &generation.id, FAILED, Some(&outcome)).await?;
    Ok(())
}

/// [`reconcile_on_startup`] as a startup step, logging what fails.
pub async fn reconcile_generations_on_startup(orch: &Orchestrator) {
    if let Err(error) = reconcile_on_startup(orch).await {
        log::warn!("inference: reconciling generations at startup failed: {error}");
    }
}

/// Retire a generation the retired `inference-generate` workflow was running:
/// fence its workflow so the crash sweep does not respawn it, withdraw its
/// questions, and end it `failed` with the upgrade's reason, keeping its
/// draft only when verification proved it as it stands. Its issue stays open
/// and is told why and how to generate anew. The workflow is never converted
/// into an ordinary agent.
async fn retire_legacy(orch: &Orchestrator, generation: &GenerationRow) -> Result<(), String> {
    if !generation.job_id.is_empty() {
        fence_workflow(orch, &generation.job_id).await?;
        withdraw_questions(orch, &generation.job_id).await;
    }
    let outcome = json!({ "outcome": FAILED, "why": UPGRADE_REASON, "upgrade": true });
    let ended = finalize_generation(&orch.db.local, &generation.id, FAILED, Some(&outcome)).await?;
    annotate_retired(orch, &ended).await
}

/// Make a retired workflow job's runs terminal and drop its respawn record,
/// so the startup crash sweep finds nothing to redispatch.
async fn fence_workflow(orch: &Orchestrator, job_id: &str) -> Result<(), String> {
    orch.db
        .local
        .execute(
            "DELETE FROM workflow_run WHERE job_id = ?1",
            params![job_id],
        )
        .await
        .map_err(|error| error.to_string())?;
    let owning = crate::execution::routing::owning_db_for_job(&orch.db, job_id)
        .await
        .map_err(|error| error.to_string())?;
    let job_id = job_id.to_string();
    let now = chrono::Utc::now().timestamp();
    owning
        .write(move |conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                conn.execute(
                    "UPDATE turns SET state = 'interrupted', ended_at = ?1, updated_at = ?1
                      WHERE job_id = ?2 AND state = 'running'",
                    params![now, job_id.as_str()],
                )
                .await?;
                conn.execute(
                    "UPDATE turns SET state = 'failed', ended_at = ?1, updated_at = ?1
                      WHERE job_id = ?2 AND state = 'pending'",
                    params![now, job_id.as_str()],
                )
                .await?;
                conn.execute(
                    "UPDATE runs SET status = 'exited', exit_reason = 'inference_generation_retired',
                            exited_at = ?1, updated_at = ?1
                      WHERE job_id = ?2 AND status NOT IN ('exited', 'failed', 'complete', 'completed')",
                    params![now, job_id.as_str()],
                )
                .await?;
                conn.execute(
                    "UPDATE jobs SET status = 'failed', updated_at = ?1
                      WHERE id = ?2 AND status NOT IN ('complete', 'failed', 'cancelled')",
                    params![now, job_id.as_str()],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .map_err(|error| error.to_string())
}

/// Tell a retired generation's issue why it stopped and how to generate
/// anew, once: the generation's outcome records that the note was written,
/// or that its issue's database answered that the issue is gone
/// (`issueMissing`). A database that cannot be reached, such as a team
/// replica that is not attached, proves nothing, so the note stays owed and
/// is retried at the next startup.
async fn annotate_retired(orch: &Orchestrator, generation: &GenerationRow) -> Result<(), String> {
    let Some(outcome) = generation.outcome.as_ref() else {
        return Ok(());
    };
    if outcome.get("upgrade") != Some(&json!(true)) || outcome.get("annotated").is_some() {
        return Ok(());
    }
    let mut annotated = outcome.clone();
    annotated["annotated"] = json!(true);
    if let Some(issue_id) = generation.issue_id.as_deref() {
        let owning = crate::issues::crud::owning_db_for_issue(&orch.db, issue_id)
            .await
            .map_err(|error| error.to_string())?;
        let exists = owning
            .query_opt(
                "SELECT 1 FROM issues WHERE id = ?1",
                params![issue_id],
                |row| row.i64(0),
            )
            .await
            .map_err(|error| error.to_string())?
            .is_some();
        if exists {
            post_retirement_note(orch, &owning, issue_id, generation).await?;
        } else {
            log::info!(
                "inference: retired generation {}'s issue {issue_id} no longer exists; no note is owed",
                generation.id
            );
            annotated["issueMissing"] = json!(true);
        }
    }
    orch.db
        .local
        .execute(
            "UPDATE inference_generations SET outcome_json = ?2 WHERE id = ?1",
            params![generation.id.as_str(), annotated.to_string()],
        )
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

/// The note on retired generation `generation`'s issue `issue_id`.
async fn post_retirement_note(
    orch: &Orchestrator,
    owning: &LocalDb,
    issue_id: &str,
    generation: &GenerationRow,
) -> Result<(), String> {
    let kept = match (
        generation.state.as_str(),
        generation.draft_profile.as_deref(),
    ) {
        (VERIFIED, Some(draft)) => {
            format!(" Its draft profile `{draft}` had already passed verification, so it is kept.")
        }
        _ => " Its unverified draft, if it wrote one, was removed.".to_string(),
    };
    let content = format!(
        "{UPGRADE_REASON} (generation `{id}`).{kept} To generate a profile for `{model}` on \
         pool `{pool}`, start a new generation from Settings > Inference: it runs as an \
         ordinary issue whose agent you can watch and message.",
        id = generation.id,
        model = generation.model,
        pool = generation.pool,
    );
    crate::issues::comments::create(
        owning,
        &*orch.services.clock,
        crate::models::CreateComment {
            issue_id: issue_id.to_string(),
            content,
            source: crate::models::CommentSource::Agent,
        },
    )
    .await
    .map_err(|error| error.to_string())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use cairn_inference::Inventory;

    use super::super::{insert_generation, replacement, write_generated};
    use super::*;
    use crate::db::DbState;
    use crate::orchestrator::inference::write_inventory;
    use crate::orchestrator::inference_models::tests::FakeHub;
    use crate::orchestrator::inference_models::{register, HUGGING_FACE};
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{migrated_test_db, MigrationRunner, SearchIndex, TURSO_MIGRATIONS};

    const HOME: &str = "cairn://p/prj/7/1/generator";
    const SHA: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const ARGS: &str = r#"-m, "{weights}""#;
    const ARGS_2: &str = r#"-m, "{weights}", -c, "4096""#;

    fn orchestrator(db: LocalDb) -> Orchestrator {
        let root = tempfile::tempdir().unwrap().keep();
        let state = Arc::new(DbState::new(
            Arc::new(db),
            Arc::new(SearchIndex::open_or_create(root.join("search.db")).unwrap()),
        ));
        Orchestrator::builder(state, Arc::new(TestServicesBuilder::new().build()), root).build()
    }

    async fn registered(db: &LocalDb) {
        let mut hub = FakeHub::fixtures();
        hub.bodies.insert(
            format!("{HUGGING_FACE}/api/models?filter=base_model:quantized:Qwen/Qwen3-Embedding-4B&limit=100"),
            "[]".into(),
        );
        register(db, &hub, "Qwen/Qwen3-Embedding-4B", None)
            .await
            .unwrap();
    }

    fn profile_yaml(id: &str, args: &str) -> String {
        format!(
            r#"
id: {id}
model:
  name: Qwen/Qwen3-Embedding-4B
  weights:
    - {{repo: Qwen/Qwen3-Embedding-4B-GGUF, revision: abc, path: Qwen3-Embedding-4B-Q8_0.gguf, sha256: {SHA}}}
servedName: Qwen/Qwen3-Embedding-4B
engine: {{kind: container, image: "ghcr.io/ggml-org/llama.cpp@sha256:{SHA}", server: llamaCpp, args: [{args}]}}
requirements:
  machines: 1
  memory: {{kind: discrete, vram: 8000000000, host: 0}}
"#
        )
    }

    fn draft(id: &str, args: &str) -> Value {
        json!({ "profile": profile_yaml(id, args) })
    }

    /// A generation bound to job `job` on issue `issue`, as its agent's
    /// launch leaves it.
    fn generation(id: &str, job: &str, issue: &str) -> GenerationRow {
        GenerationRow {
            id: id.into(),
            project_id: "p".into(),
            issue_id: Some(issue.into()),
            job_id: job.into(),
            mode: AGENT_MODE.into(),
            execution_id: Some(format!("e-{id}")),
            attempts: 0,
            draft_revision: 0,
            draft_digest: None,
            approval_prompt_id: None,
            approval_run_id: None,
            approval_revision: None,
            verification_id: None,
            cleanup_pending: false,
            node_uri: HOME.into(),
            model: "Qwen/Qwen3-Embedding-4B".into(),
            pool: "bbs".into(),
            state: RUNNING.into(),
            draft_profile: None,
            draft_written_at: None,
            approved_surface: None,
            hardware: BTreeMap::new(),
            outcome: None,
            started_at: 1,
            finished_at: None,
        }
    }

    async fn row(db: &LocalDb, id: &str) -> GenerationRow {
        get_generation(db, id).await.unwrap().unwrap()
    }

    async fn in_inventory(db: &LocalDb, profile: &str) -> bool {
        load_inventory(db)
            .await
            .unwrap()
            .profiles
            .contains_key(&InferenceProfileId::from(profile))
    }

    /// The ownership receipt of `profile`: whether it is a current draft, and
    /// what promoted it.
    async fn receipt(db: &LocalDb, profile: &str) -> (bool, Option<String>) {
        let owned = load_owned(db).await.unwrap().remove(profile).unwrap();
        (owned.active, owned.promoted)
    }

    /// The operator approves generation `id`'s current revision and its
    /// verification `verification` claims it, running.
    async fn claim(db: &LocalDb, id: &str, verification: &str) {
        let generation = row(db, id).await;
        let draft = generation.draft_profile.clone().unwrap();
        let owned = load_owned(db).await.unwrap().remove(&draft).unwrap();
        db.execute(
            "UPDATE inference_generations SET approval_revision = ?2, approved_surface = ?3, verification_id = ?4
              WHERE id = ?1",
            params![id, owned.revision, owned.surface_digest.as_str(), verification],
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO inference_verifications
               (id, profile_id, profile_digest, pool_id, deployment_id, machines_json,
                requested_by, started_at, outcome, stage)
             VALUES (?1, ?2, ?3, 'bbs', ?4, '[]', 'agent', 1, 'running', 'render')",
            params![
                verification,
                draft.as_str(),
                owned.profile_digest.as_str(),
                format!("verify-{verification}")
            ],
        )
        .await
        .unwrap();
    }

    /// Verification `verification` ends `outcome`, its settlement not yet run.
    async fn conclude(db: &LocalDb, verification: &str, outcome: &str) {
        db.execute(
            "UPDATE inference_verifications SET outcome = ?2, finished_at = 2 WHERE id = ?1",
            params![verification, outcome],
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn verified_needs_the_databases_pass_of_the_draft_as_it_stands() {
        let db = migrated_test_db("generation-finalize-verified").await;
        registered(&db).await;
        insert_generation(&db, &generation("g1", "j1", "i"))
            .await
            .unwrap();
        write_generated(&db, HOME, "j1", &draft("a", ARGS))
            .await
            .unwrap();
        let verified = json!({ "outcome": "verified", "verification": "forged" });

        let refusal = record_outcome(&db, "j-other", "g1", &verified)
            .await
            .unwrap_err();
        assert!(refusal.contains("own agent"), "{refusal}");
        let refusal = record_outcome(&db, "j1", "g1", &verified)
            .await
            .unwrap_err();
        assert!(refusal.contains("verification decides"), "{refusal}");
        assert!(
            row(&db, "g1").await.is_open(),
            "a refused outcome ends nothing"
        );

        // A pass of revision 1, promoted, does not verify the revision written
        // after it.
        claim(&db, "g1", "v1").await;
        conclude(&db, "v1", "passed").await;
        promotion::settle(&db, "v1").await.unwrap();
        assert_eq!(receipt(&db, "a").await.1.as_deref(), Some("v1"));
        write_generated(&db, HOME, "j1", &draft("a", ARGS_2))
            .await
            .unwrap();
        let refusal = record_outcome(&db, "j1", "g1", &verified)
            .await
            .unwrap_err();
        assert!(refusal.contains("verification decides"), "{refusal}");

        claim(&db, "g1", "v2").await;
        conclude(&db, "v2", "passed").await;
        let (said, ended) = record_outcome(&db, "j1", "g1", &verified).await.unwrap();
        assert_eq!(ended.state, VERIFIED);
        assert!(said.contains("verified profile a"), "{said}");
        let outcome = ended.outcome.clone().unwrap();
        assert_eq!(
            outcome["verification"], "v2",
            "the database's pass, not the payload's"
        );
        assert!(in_inventory(&db, "a").await);
        assert_eq!(receipt(&db, "a").await, (true, Some("v2".into())));

        // Idempotent: a later ending changes nothing.
        let again = finalize_generation(&db, "g1", FAILED, None).await.unwrap();
        assert_eq!(again, ended);
        assert!(in_inventory(&db, "a").await);
        let refusal = record_outcome(&db, "j1", "g1", &verified)
            .await
            .unwrap_err();
        assert!(refusal.contains("already ended verified"), "{refusal}");
    }

    #[tokio::test]
    async fn any_other_ending_revokes_the_generations_authority_and_removes_its_draft() {
        let db = migrated_test_db("generation-finalize-other").await;
        registered(&db).await;
        insert_generation(&db, &generation("g1", "j1", "i"))
            .await
            .unwrap();
        write_generated(&db, HOME, "j1", &draft("a", ARGS))
            .await
            .unwrap();
        claim(&db, "g1", "v1").await;
        conclude(&db, "v1", "failed").await;

        let gave_up = json!({ "outcome": "gaveUp", "why": "the model does not fit" });
        let (said, ended) = record_outcome(&db, "j1", "g1", &gave_up).await.unwrap();
        assert_eq!(ended.state, "gaveUp");
        assert_eq!(ended.outcome, Some(gave_up));
        assert!(said.contains("Removed its draft profile a"), "{said}");
        assert_eq!(ended.draft_profile, None);
        assert!(!ended.cleanup_pending);
        assert_eq!(
            (ended.approved_surface, ended.approval_revision),
            (None, None),
            "its approval is discarded"
        );
        assert!(!in_inventory(&db, "a").await);
        assert_eq!(
            receipt(&db, "a").await,
            (false, None),
            "the receipt stays, no longer a current draft"
        );
        assert!(
            write_generated(&db, HOME, "j1", &draft("a", ARGS))
                .await
                .is_err(),
            "an ended generation writes no draft"
        );
    }

    #[tokio::test]
    async fn a_held_draft_is_removed_once_its_verification_settles_and_is_never_promoted() {
        let db = migrated_test_db("generation-finalize-held").await;
        registered(&db).await;
        insert_generation(&db, &generation("g1", "j1", "i"))
            .await
            .unwrap();
        write_generated(&db, HOME, "j1", &draft("a", ARGS))
            .await
            .unwrap();
        claim(&db, "g1", "v1").await;

        let ended = finalize_generation(&db, "g1", CANCELLED, None)
            .await
            .unwrap();
        assert_eq!(ended.state, CANCELLED);
        assert_eq!(ended.draft_profile.as_deref(), Some("a"));
        assert!(
            ended.cleanup_pending,
            "the held removal is pending, not claimed"
        );
        assert!(in_inventory(&db, "a").await);
        assert_eq!(receipt(&db, "a").await, (false, None));
        assert_eq!(retry_cleanup(&db).await.unwrap(), 1, "still held");

        // The verification passes after its generation ended: the pass
        // promotes nothing, and the draft goes.
        conclude(&db, "v1", "passed").await;
        verification_settled(&db, "v1").await;
        let settled = row(&db, "g1").await;
        assert_eq!(settled.state, CANCELLED);
        assert_eq!(
            (settled.draft_profile, settled.cleanup_pending),
            (None, false)
        );
        assert!(!in_inventory(&db, "a").await);
        assert_eq!(receipt(&db, "a").await, (false, None));
    }

    #[tokio::test]
    async fn a_pass_no_one_settled_before_a_crash_keeps_its_profile() {
        let db = migrated_test_db("generation-finalize-crash").await;
        registered(&db).await;
        insert_generation(&db, &generation("g1", "j1", "i"))
            .await
            .unwrap();
        write_generated(&db, HOME, "j1", &draft("a", ARGS))
            .await
            .unwrap();
        claim(&db, "g1", "v1").await;
        conclude(&db, "v1", "passed").await;

        let why = json!({ "outcome": FAILED, "why": "its agent's job ended failed" });
        let ended = finalize_generation(&db, "g1", FAILED, Some(&why))
            .await
            .unwrap();
        assert_eq!(ended.state, VERIFIED);
        let outcome = ended.outcome.unwrap();
        assert_eq!(
            (
                outcome["endedAs"].as_str(),
                outcome["verification"].as_str()
            ),
            (Some(FAILED), Some("v1"))
        );
        assert_eq!(outcome["why"], why["why"]);
        assert!(in_inventory(&db, "a").await);
        assert_eq!(receipt(&db, "a").await.1.as_deref(), Some("v1"));
    }

    #[tokio::test]
    async fn settlement_ends_a_pass_verified_and_a_failed_last_candidate_exhausted() {
        let db = migrated_test_db("generation-finalize-settled").await;
        registered(&db).await;
        for (id, job) in [("g1", "j1"), ("g2", "j2"), ("g3", "j3")] {
            insert_generation(&db, &generation(id, job, "i"))
                .await
                .unwrap();
        }
        write_generated(&db, HOME, "j1", &draft("a", ARGS))
            .await
            .unwrap();
        for args in [ARGS, ARGS_2, ARGS] {
            write_generated(&db, HOME, "j2", &draft("b", args))
                .await
                .unwrap();
        }
        write_generated(&db, HOME, "j3", &draft("c", ARGS))
            .await
            .unwrap();
        for (id, verification, outcome) in [
            ("g1", "v1", "passed"),
            ("g2", "v2", "failed"),
            ("g3", "v3", "failed"),
        ] {
            claim(&db, id, verification).await;
            conclude(&db, verification, outcome).await;
            verification_settled(&db, verification).await;
        }

        assert_eq!(row(&db, "g1").await.state, VERIFIED);
        assert!(in_inventory(&db, "a").await);
        let exhausted = row(&db, "g2").await;
        assert_eq!(exhausted.state, EXHAUSTED);
        assert_eq!(exhausted.draft_profile, None);
        assert!(!in_inventory(&db, "b").await);
        let open = row(&db, "g3").await;
        assert!(
            open.is_open(),
            "a failed candidate with attempts left is not an ending"
        );
        assert!(in_inventory(&db, "c").await);
    }

    #[tokio::test]
    async fn an_old_generations_cleanup_never_removes_a_draft_its_id_now_names_for_another() {
        let db = migrated_test_db("generation-cleanup-owner").await;
        registered(&db).await;
        insert_generation(&db, &generation("old", "j-old", "i"))
            .await
            .unwrap();
        write_generated(&db, HOME, "j-old", &draft("a", ARGS))
            .await
            .unwrap();
        claim(&db, "old", "v1").await;
        let ended = finalize_generation(&db, "old", CANCELLED, None)
            .await
            .unwrap();
        assert!(
            ended.cleanup_pending,
            "its running verification holds the draft"
        );

        // The verification ends unsettled and the operator deletes the draft
        // by hand, so its id is free while the old row still points at it.
        conclude(&db, "v1", "failed").await;
        crate::orchestrator::inference::delete_profile(&db, "a")
            .await
            .unwrap();
        insert_generation(&db, &generation("new", "j-new", "i2"))
            .await
            .unwrap();
        write_generated(&db, HOME, "j-new", &draft("a", ARGS))
            .await
            .unwrap();
        assert_eq!(row(&db, "old").await.draft_profile.as_deref(), Some("a"));

        assert_eq!(retry_cleanup(&db).await.unwrap(), 0);
        verification_settled(&db, "v1").await;
        let finalized = finalize_generation(&db, "old", CANCELLED, None)
            .await
            .unwrap();
        assert_eq!(
            (finalized.draft_profile, finalized.cleanup_pending),
            (None, false),
            "only the old pointer is forgotten"
        );
        assert!(in_inventory(&db, "a").await, "the new owner's draft stays");
        assert!(row(&db, "new").await.is_open());
        assert_eq!(receipt(&db, "a").await, (true, None));
        let owner = load_owned(&db).await.unwrap().remove("a").unwrap();
        assert_eq!(owner.generation, "new");
    }

    #[tokio::test]
    async fn a_draft_without_an_ownership_record_is_not_removed() {
        let db = migrated_test_db("generation-cleanup-unowned").await;
        registered(&db).await;
        put_profile(&db, "z").await;
        let mut ended = generation("g1", "j1", "i");
        ended.state = FAILED.into();
        ended.draft_profile = Some("z".into());
        insert_generation(&db, &ended).await.unwrap();

        assert_eq!(retry_cleanup(&db).await.unwrap(), 1, "pending, not removed");
        let left = row(&db, "g1").await;
        assert_eq!(
            (left.draft_profile.as_deref(), left.cleanup_pending),
            (Some("z"), true)
        );
        assert!(in_inventory(&db, "z").await);

        // Once startup's backfill records it as this generation's, it goes.
        promotion::reconcile(&db).await.unwrap();
        assert_eq!(retry_cleanup(&db).await.unwrap(), 0);
        assert!(!in_inventory(&db, "z").await);
        assert_eq!(row(&db, "g1").await.draft_profile, None);
    }

    /// Profile `a`, verified for generation `old` and then edited by the
    /// operator, captured by running generation `new` regenerating it.
    async fn regenerating(name: &str) -> LocalDb {
        let db = migrated_test_db(name).await;
        registered(&db).await;
        insert_generation(&db, &generation("old", "j-old", "i"))
            .await
            .unwrap();
        write_generated(&db, HOME, "j-old", &draft("a", ARGS))
            .await
            .unwrap();
        claim(&db, "old", "v1").await;
        conclude(&db, "v1", "passed").await;
        verification_settled(&db, "v1").await;
        assert_eq!(row(&db, "old").await.state, VERIFIED);
        // The operator edits the stored profile, which stays generated.
        let mut edited =
            load_inventory(&db).await.unwrap().profiles[&InferenceProfileId::from("a")].clone();
        edited.notes = Some("edited by the operator".into());
        let mut edit = Inventory::default();
        edit.insert_profile(edited);
        write_inventory(
            &db,
            &Authoring::Patch {
                upsert: Some(edit.to_yaml()),
                remove: Removals::default(),
            },
        )
        .await
        .unwrap();
        assert_eq!(receipt(&db, "a").await.1, None, "the edit cleared the pass");

        let target = replacement::capture(&db, "Qwen/Qwen3-Embedding-4B", "a")
            .await
            .unwrap();
        insert_generation(&db, &generation("new", "j-new", "i2"))
            .await
            .unwrap();
        db.write(move |conn| {
            let target = target.clone();
            Box::pin(async move { replacement::record_in(conn, "new", &target).await })
        })
        .await
        .unwrap();
        db
    }

    /// The literal saved body of profile `id`.
    async fn saved_body(db: &LocalDb, id: &str) -> String {
        db.query_one(
            "SELECT body_json FROM inference_profiles WHERE id = ?1",
            params![id],
            |row| row.text(0),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn a_regeneration_ending_before_its_first_accepted_draft_leaves_its_target_untouched() {
        let db = regenerating("generation-replacement-preaccept").await;
        let body = saved_body(&db, "a").await;
        let owner = load_owned(&db).await.unwrap().remove("a").unwrap();

        let ended = finalize_generation(&db, "new", FAILED, None).await.unwrap();
        assert_eq!(
            (
                ended.state.as_str(),
                ended.draft_profile,
                ended.cleanup_pending
            ),
            (FAILED, None, false)
        );
        assert_eq!(retry_cleanup(&db).await.unwrap(), 0);
        assert_eq!(
            saved_body(&db, "a").await,
            body,
            "the target is as captured"
        );
        assert_eq!(load_owned(&db).await.unwrap().remove("a").unwrap(), owner);
        assert_eq!(row(&db, "old").await.state, VERIFIED);
        assert!(replacement::load(&db, "new").await.unwrap().is_some());

        // Nothing holds the profile any more, so it can be regenerated anew.
        replacement::capture(&db, "Qwen/Qwen3-Embedding-4B", "a")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn a_regeneration_failing_after_its_draft_was_accepted_removes_that_draft() {
        let db = regenerating("generation-replacement-postaccept").await;
        write_generated(&db, HOME, "j-new", &draft("a", ARGS))
            .await
            .unwrap();
        let owner = load_owned(&db).await.unwrap().remove("a").unwrap();
        assert_eq!((owner.generation.as_str(), owner.active), ("new", true));

        let ended = finalize_generation(&db, "new", FAILED, None).await.unwrap();
        assert_eq!(
            (
                ended.state.as_str(),
                ended.draft_profile,
                ended.cleanup_pending
            ),
            (FAILED, None, false)
        );
        assert!(
            !in_inventory(&db, "a").await,
            "the accepted draft is removed"
        );
        let receipt = load_owned(&db).await.unwrap().remove("a").unwrap();
        assert_eq!(
            (receipt.generation.as_str(), receipt.active),
            ("new", false),
            "the receipt stays with the generation that took it"
        );
        let captured = replacement::load(&db, "new").await.unwrap().unwrap();
        assert!(
            captured.body.contains("edited by the operator"),
            "the capture still holds the profile as it was saved"
        );
        assert_eq!(
            row(&db, "old").await.state,
            VERIFIED,
            "the old owner's record is unchanged"
        );
    }

    /// An orchestrator whose issue `i` (7) holds jobs `j1` and `j4` and issue
    /// `i2` (8) holds `j3`, each with a live run, and generations `g1`, `g4`
    /// and `g3` bound to them with drafts `a`, `d` and `c`. `j1`'s run waits
    /// on a question.
    async fn seeded(name: &str) -> Orchestrator {
        let orch = orchestrator(migrated_test_db(name).await);
        let db = &orch.db.local;
        db.execute_script(
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w','W',1,1);
             INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('p','w','P','prj','/tmp/p',1,1);
             INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
               VALUES ('i','p',7,'Generate profile','active',1,1), ('i2','p',8,'Generate profile','active',1,1);
             INSERT INTO jobs (id, issue_id, project_id, node_name, status, created_at, updated_at, uri_segment)
               VALUES ('j1','i','p','Generator','running',1,1,'generator'),
                      ('j3','i2','p','Generator','running',1,1,'generator'),
                      ('j4','i','p','Generator','running',1,1,'generator-2');
             INSERT INTO runs (id, issue_id, project_id, job_id, status, created_at, updated_at)
               VALUES ('r1','i','p','j1','live',1,1), ('r3','i2','p','j3','live',1,1), ('r4','i','p','j4','live',1,1);
             INSERT INTO prompts (id, run_id, job_id, questions, uri_segment, created_at)
               VALUES ('q1','r1','j1','[{\"question\":\"Quantization?\",\"options\":[]}]','q-1',1);",
        )
        .await
        .unwrap();
        registered(db).await;
        for (id, job, issue, profile) in [
            ("g1", "j1", "i", "a"),
            ("g3", "j3", "i2", "c"),
            ("g4", "j4", "i", "d"),
        ] {
            insert_generation(db, &generation(id, job, issue))
                .await
                .unwrap();
            write_generated(db, HOME, job, &draft(profile, ARGS))
                .await
                .unwrap();
        }
        orch
    }

    async fn withdrawn(db: &LocalDb, prompt: &str) -> bool {
        db.query_one(
            "SELECT withdrawn_at IS NOT NULL FROM prompts WHERE id = ?1",
            params![prompt],
            |row| row.i64(0),
        )
        .await
        .unwrap()
            != 0
    }

    #[tokio::test]
    async fn waiting_ends_nothing_and_a_stop_a_resolution_or_an_ended_job_does() {
        let orch = seeded("generation-lifecycle-hooks").await;
        let db = orch.db.local.clone();

        // A question waiting on the operator, and startup, end nothing.
        job_ended(&orch, "j1").await.unwrap();
        reconcile_on_startup(&orch).await.unwrap();
        for id in ["g1", "g3", "g4"] {
            assert!(row(&db, id).await.is_open(), "{id} is open while it waits");
        }
        assert!(!withdrawn(&db, "q1").await);
        assert!(in_inventory(&db, "a").await);

        // The operator's stop cancels the generation and withdraws its
        // question; its issue stays open.
        operator_stopped(&orch, "j1").await.unwrap();
        let stopped = row(&db, "g1").await;
        assert_eq!(stopped.state, CANCELLED);
        assert!(!in_inventory(&db, "a").await);
        assert!(withdrawn(&db, "q1").await);
        assert!(
            write_generated(&db, HOME, "j1", &draft("a", ARGS))
                .await
                .is_err(),
            "a resumed conversation does not resurrect the draft"
        );

        // Resolving issue i2 cancels only its generation.
        issue_resolved(&orch, "i2").await.unwrap();
        assert_eq!(row(&db, "g3").await.state, CANCELLED);
        assert!(!in_inventory(&db, "c").await);
        assert!(row(&db, "g4").await.is_open());

        // A job that actually ended without an outcome fails its generation.
        db.execute("UPDATE jobs SET status = 'complete' WHERE id = 'j4'", ())
            .await
            .unwrap();
        job_ended(&orch, "j4").await.unwrap();
        let failed = row(&db, "g4").await;
        assert_eq!(failed.state, FAILED);
        assert!(failed.outcome.unwrap()["why"]
            .as_str()
            .unwrap()
            .contains("ended complete"),);
        assert!(!in_inventory(&db, "d").await);

        let status = db
            .query_one("SELECT status FROM issues WHERE id = 'i'", (), |row| {
                row.text(0)
            })
            .await
            .unwrap();
        assert_eq!(
            status, "active",
            "a generation's issue is never closed for it"
        );
    }

    /// `seeded`, plus generation `gq` still `starting` in execution `exec-q`
    /// on issue `i`, whose agent job `jq` is queued and has not bound.
    async fn queued(name: &str) -> Orchestrator {
        let orch = seeded(name).await;
        orch.db
            .local
            .execute_script(
                "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
                   VALUES ('exec-q', 'inference-generate', 'i', 'p', 'running', 1, 2);
                 INSERT INTO jobs (id, execution_id, issue_id, project_id, node_name, status,
                                   created_at, updated_at, uri_segment)
                   VALUES ('jq', 'exec-q', 'i', 'p', 'Generator', 'queued', 1, 1, 'generator-q');",
            )
            .await
            .unwrap();
        let mut unbound = generation("gq", "", "i");
        unbound.state = STARTING.into();
        unbound.execution_id = Some("exec-q".into());
        insert_generation(&orch.db.local, &unbound).await.unwrap();
        orch
    }

    async fn launch_failures(db: &LocalDb) -> usize {
        crate::issues::comments::list(db, "i")
            .await
            .unwrap()
            .into_iter()
            .filter(|comment| comment.content.starts_with("**Launch failed.**"))
            .count()
    }

    #[tokio::test]
    async fn a_queued_agent_cancelled_before_it_binds_fails_the_launch_without_a_restart() {
        let orch = queued("generation-unbound-cancelled").await;
        let db = orch.db.local.clone();

        // Still queued: its execution can launch the agent, so it waits.
        job_ended(&orch, "jq").await.unwrap();
        assert_eq!(row(&db, "gq").await.state, STARTING);
        assert_eq!(launch_failures(&db).await, 0);

        db.execute("UPDATE jobs SET status = 'cancelled' WHERE id = 'jq'", ())
            .await
            .unwrap();
        job_ended(&orch, "jq").await.unwrap();
        let failed = row(&db, "gq").await;
        assert_eq!(failed.state, LAUNCH_FAILED);
        assert_eq!(failed.job_id, "", "it never bound");
        assert_eq!(launch_failures(&db).await, 1);
        for id in ["g1", "g3", "g4"] {
            assert!(row(&db, id).await.is_open(), "{id} is not this execution's");
        }

        // Settling again changes nothing and says nothing twice.
        job_ended(&orch, "jq").await.unwrap();
        assert_eq!(row(&db, "gq").await, failed);
        assert_eq!(launch_failures(&db).await, 1);
    }

    #[tokio::test]
    async fn an_operator_stop_of_an_agent_that_never_bound_fails_its_launch() {
        let orch = queued("generation-unbound-stopped").await;
        let db = orch.db.local.clone();
        db.execute("UPDATE jobs SET status = 'cancelled' WHERE id = 'jq'", ())
            .await
            .unwrap();

        operator_stopped(&orch, "jq").await.unwrap();
        assert_eq!(row(&db, "gq").await.state, LAUNCH_FAILED);
        assert_eq!(launch_failures(&db).await, 1);
        job_ended(&orch, "jq").await.unwrap();
        assert_eq!(launch_failures(&db).await, 1);
    }

    /// Write `profile` as the operator's own record, as an earlier Cairn's
    /// workflow draft stands once its origin is no longer trusted.
    async fn put_profile(db: &LocalDb, id: &str) {
        let profile = serde_yaml::from_str(&profile_yaml(id, ARGS)).unwrap();
        let mut records = Inventory::default();
        records.insert_profile(profile);
        write_inventory(
            db,
            &Authoring::Patch {
                upsert: Some(records.to_yaml()),
                remove: Removals::default(),
            },
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn the_upgrade_retires_running_workflow_generations_once_keeping_only_proven_drafts() {
        let dir = tempfile::tempdir().unwrap();
        let db = LocalDb::open(dir.path().join("upgrade.db")).await.unwrap();
        let before: Vec<_> = TURSO_MIGRATIONS
            .iter()
            .take_while(|migration| migration.name() != "inference_generated_profiles")
            .copied()
            .collect();
        assert!(
            before.len() < TURSO_MIGRATIONS.len(),
            "the ownership migration is in the lineage"
        );
        MigrationRunner::new(before).run(&db).await.unwrap();
        // Two generations the retired workflow was running when Cairn stopped,
        // as that schema held them.
        db.execute_script(
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w','W',1,1);
             INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('p','w','P','prj','/tmp/p',1,1);
             INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
               VALUES ('i','p',7,'Generate profile','active',1,1), ('i2','p',8,'Generate profile','active',1,1);
             INSERT INTO jobs (id, issue_id, project_id, node_name, status, created_at, updated_at, uri_segment)
               VALUES ('j-wf','i','p','inference-generate','running',1,1,'inference-generate'),
                      ('j-wf2','i2','p','inference-generate','running',1,1,'inference-generate');
             INSERT INTO runs (id, issue_id, project_id, job_id, status, created_at, updated_at)
               VALUES ('r-wf','i','p','j-wf','live',1,1), ('r-wf2','i2','p','j-wf2','crashed',1,1);
             INSERT INTO workflow_run (run_id, job_id, workflow_id, project_id, anchor_branch, branch_mode,
                                       package_path, script_path, args_json, output_name, created_at)
               VALUES ('r-wf','j-wf','inference-generate','p','main','inherit','/wf','/wf/main.ts','{}','result',1),
                      ('r-wf2','j-wf2','inference-generate','p','main','inherit','/wf','/wf/main.ts','{}','result',1);
             INSERT INTO prompts (id, run_id, job_id, questions, uri_segment, created_at)
               VALUES ('q-wf','r-wf','j-wf','[{\"question\":\"Approve?\",\"options\":[]}]','q-1',1);
             INSERT INTO inference_generations
               (id, project_id, issue_id, job_id, node_uri, model, pool, state, draft_profile,
                draft_written_at, approved_surface, started_at)
               VALUES ('legacy','p','i','j-wf','cairn://p/prj/7/1/inference-generate',
                       'Qwen/Qwen3-Embedding-4B','bbs','running','a',1,'stale-approval',1),
                      ('proven','p','i2','j-wf2','cairn://p/prj/8/1/inference-generate',
                       'Qwen/Qwen3-Embedding-4B','bbs','running','b',1,'stale-approval',1);",
        )
        .await
        .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        let orch = orchestrator(db);
        let db = orch.db.local.clone();
        registered(&db).await;
        put_profile(&db, "a").await;
        put_profile(&db, "b").await;
        // `b` passed verification as it stands before the upgrade.
        let b =
            digest(&load_inventory(&db).await.unwrap().profiles[&InferenceProfileId::from("b")]);
        db.execute(
            "INSERT INTO inference_verifications
               (id, profile_id, profile_digest, pool_id, deployment_id, machines_json,
                requested_by, started_at, finished_at, outcome, stage)
             VALUES ('v-b', 'b', ?1, 'bbs', 'verify-b', '[]', 'operator', 1, 2, 'passed', 'done')",
            params![b.as_str()],
        )
        .await
        .unwrap();
        let legacy = row(&db, "legacy").await;
        assert_eq!((legacy.mode.as_str(), legacy.attempts), ("workflow", 0));

        reconcile_on_startup(&orch).await.unwrap();

        let retired = row(&db, "legacy").await;
        assert_eq!(retired.state, FAILED);
        let outcome = retired.outcome.clone().unwrap();
        assert_eq!(outcome["why"], UPGRADE_REASON);
        assert_eq!(outcome["annotated"], true);
        assert_eq!(retired.draft_profile, None);
        assert_eq!(
            retired.approved_surface, None,
            "stale approval is discarded"
        );
        assert!(!in_inventory(&db, "a").await);
        let kept = row(&db, "proven").await;
        assert_eq!(kept.state, VERIFIED);
        assert!(
            in_inventory(&db, "b").await,
            "a proven draft survives the upgrade"
        );

        let count = |sql: &'static str| {
            let db = db.clone();
            async move { db.query_one(sql, (), |row| row.i64(0)).await.unwrap() }
        };
        assert_eq!(
            count("SELECT COUNT(*) FROM workflow_run").await,
            0,
            "nothing to redispatch"
        );
        assert_eq!(
            count("SELECT COUNT(*) FROM runs WHERE status = 'exited' AND exit_reason = 'inference_generation_retired'").await,
            2
        );
        assert_eq!(
            count("SELECT COUNT(*) FROM jobs WHERE status = 'failed'").await,
            2
        );
        assert!(withdrawn(&db, "q-wf").await);
        assert_eq!(
            count("SELECT COUNT(*) FROM issues WHERE status = 'active'").await,
            2,
            "the issues stay open"
        );
        let notes = |issue: &'static str| {
            let db = db.clone();
            async move {
                crate::issues::comments::list(&db, issue)
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|comment| comment.content)
                    .collect::<Vec<_>>()
            }
        };
        let on_i = notes("i").await;
        assert_eq!(on_i.len(), 1);
        assert!(on_i[0].contains("Settings > Inference"), "{}", on_i[0]);
        assert!(notes("i2").await[0].contains("already passed verification"));

        // Repeating startup, and the migrations, changes nothing.
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        reconcile_on_startup(&orch).await.unwrap();
        assert_eq!(row(&db, "legacy").await, retired);
        assert_eq!(row(&db, "proven").await, kept);
        assert_eq!((notes("i").await.len(), notes("i2").await.len()), (1, 1));
    }

    /// A team-routed issue id whose team replica is not attached.
    const UNATTACHED: &str = "acme~550e8400-e29b-41d4-a716-446655440000";

    /// Retired workflow generations, already ended by the upgrade but whose
    /// issue notes are still owed, one per `(id, issue, started_at)` in
    /// `retired`; beside them generation `g1`, cancelled while verification
    /// `v1` held its draft `a`, whose removal is pending until startup.
    async fn retired_beside_held(name: &str, retired: &[(&str, &str, i64)]) -> Orchestrator {
        let orch = orchestrator(migrated_test_db(name).await);
        let db = orch.db.local.clone();
        registered(&db).await;
        insert_generation(&db, &generation("g1", "j1", "i"))
            .await
            .unwrap();
        write_generated(&db, HOME, "j1", &draft("a", ARGS))
            .await
            .unwrap();
        claim(&db, "g1", "v1").await;
        let held = finalize_generation(&db, "g1", CANCELLED, None)
            .await
            .unwrap();
        assert!(held.cleanup_pending);
        // The verification ends while Cairn is down: only startup retries.
        conclude(&db, "v1", "failed").await;
        for (id, issue, started_at) in retired {
            let mut row = generation(id, &format!("j-{id}"), issue);
            row.mode = "workflow".into();
            row.execution_id = None;
            row.state = FAILED.into();
            row.started_at = *started_at;
            row.finished_at = Some(*started_at);
            row.outcome =
                Some(json!({ "outcome": FAILED, "why": UPGRADE_REASON, "upgrade": true }));
            insert_generation(&db, &row).await.unwrap();
        }
        orch
    }

    async fn comment_count(db: &LocalDb) -> i64 {
        db.query_one("SELECT COUNT(*) FROM comments", (), |row| row.i64(0))
            .await
            .unwrap()
    }

    async fn assert_draft_removed(db: &LocalDb) {
        let g1 = row(db, "g1").await;
        assert_eq!((g1.draft_profile, g1.cleanup_pending), (None, false));
        assert!(!in_inventory(db, "a").await);
    }

    #[tokio::test]
    async fn a_retired_generation_whose_issue_is_gone_owes_no_note_and_blocks_no_cleanup() {
        let orch =
            retired_beside_held("generation-upgrade-gone-issue", &[("legacy", "gone", 2)]).await;
        let db = orch.db.local.clone();

        reconcile_on_startup(&orch).await.unwrap();

        assert_draft_removed(&db).await;
        let settled = row(&db, "legacy").await;
        let outcome = settled.outcome.clone().unwrap();
        assert_eq!(
            (&outcome["annotated"], &outcome["issueMissing"]),
            (&json!(true), &json!(true))
        );
        assert_eq!(comment_count(&db).await, 0);

        // Repeating startup changes nothing and fails nothing.
        reconcile_on_startup(&orch).await.unwrap();
        assert_eq!(row(&db, "legacy").await, settled);
        assert_eq!(comment_count(&db).await, 0);
    }

    #[tokio::test]
    async fn an_unreachable_issue_database_leaves_the_note_owed_and_blocks_nothing_else() {
        // `unreachable` sorts first, so the rest run after its failure.
        let orch = retired_beside_held(
            "generation-upgrade-unreachable-issue",
            &[("unreachable", UNATTACHED, 3), ("gone", "gone", 2)],
        )
        .await;
        let db = orch.db.local.clone();

        for _ in 0..2 {
            let failure = reconcile_on_startup(&orch).await.unwrap_err();
            assert!(failure.contains("unreachable"), "{failure}");
            assert!(!failure.contains("generation gone"), "{failure}");

            let owed = row(&db, "unreachable").await.outcome.unwrap();
            assert!(
                owed.get("annotated").is_none(),
                "an unreachable database proves nothing; the note stays owed: {owed}"
            );
            assert_eq!(
                row(&db, "gone").await.outcome.unwrap()["issueMissing"],
                true
            );
            assert_draft_removed(&db).await;
        }
    }
}
