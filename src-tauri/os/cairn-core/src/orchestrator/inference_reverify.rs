//! Re-verifying a profile onto its template's current engine pin.
//!
//! A published engine bump never changes an existing profile
//! (`cairn_inference::engine_pins`). Moving one is a re-verify, which the
//! operator starts from Settings > Inference on a pool:
//!
//! - It writes a candidate: the profile with only its engine (the image, or
//!   the artifact and the program in it) replaced by the template's current
//!   pin, under an id of its own, recorded in `inference_pin_moves`. The
//!   promotion gate reads that table, so nothing but the re-verify's own
//!   verification ever places a candidate, and deployments of the profile
//!   keep rendering it as it is.
//! - The candidate is verified on the chosen pool like any profile
//!   (`inference_verify::begin`, then `run`).
//! - A pass moves the profile in one transaction: its engine becomes the
//!   candidate's, the candidate and its verification deployment go, and the
//!   passing verification row is re-pointed at the moved profile, so the
//!   profile's measured fit, speed and engine version are the new engine's on
//!   that pool. Anything else removes the candidate and leaves the profile
//!   exactly as it was, its verifications still current.
//!
//! A generated (governed) profile starts only while promoted as it stands,
//! and its engine is part of the runnable surface the operator approved. Its
//! re-verify is therefore the operator's approval of exactly the candidate's
//! surface, recorded on the re-verify's own row and shown as the engine
//! change (`0.8.3 → 0.8.4`). The candidate is never revised by its
//! verification, which has one claim, and the pass re-promotes the moved
//! profile in the move's own transaction, so it is never unpromoted on the
//! way. The generation that first wrote the profile is not reopened and
//! speaks for nothing here.

use cairn_db::turso::params;
use cairn_inference::engine_pins::EnginePins;
use cairn_inference::{
    DeploymentId, Engine, InferenceProfile, InferenceProfileId, Inventory, ProfileOrigin,
};

use super::inference::{
    load_inventory, prepare_inventory_write, save_rows_in, Authoring, Removals, INVENTORY,
};
use super::inference_generate::promotion::{surface_of, Gate};
use super::inference_verify::{self, digest, load_verifications, VerificationRow};
use crate::storage::{DbResult, LocalDb, RowExt};

/// A re-verify whose candidate is being verified.
pub const VERIFYING: &str = "verifying";
/// A re-verify that passed and moved its profile.
pub const MOVED: &str = "moved";
/// A re-verify that ended without moving its profile.
pub const FAILED: &str = "failed";

/// One re-verify of a profile onto its template's current engine pin.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PinMove {
    pub id: String,
    pub profile: String,
    pub candidate: String,
    pub pool: String,
    pub template: String,
    /// The release the profile ran when the re-verify began.
    pub from: String,
    /// The pin it is being moved onto.
    pub to: String,
    /// [`digest`] of the profile as it was when the re-verify began.
    pub profile_digest: String,
    /// [`digest`] of the candidate as written.
    pub candidate_digest: String,
    /// [`surface_of`] the candidate as written: what an approval binds to.
    pub surface_digest: String,
    /// The profile is a generation's, so the move needs the operator's
    /// approval and re-promotes it.
    pub governed: bool,
    pub approved_by: Option<String>,
    pub approved_at: Option<i64>,
    /// The verification of the candidate it admitted last.
    pub verification: Option<String>,
    /// [`VERIFYING`], [`MOVED`] or [`FAILED`].
    pub state: String,
    pub reason: Option<String>,
    pub started_at: i64,
    pub finished_at: Option<i64>,
}

const COLUMNS: &str = "id, profile_id, candidate_id, pool_id, template_id, from_engine, to_engine,
    profile_digest, candidate_digest, surface_digest, governed, approved_by, approved_at,
    verification_id, state, reason, started_at, finished_at";

fn move_of(row: &cairn_db::turso::Row) -> DbResult<PinMove> {
    Ok(PinMove {
        id: row.text(0)?,
        profile: row.text(1)?,
        candidate: row.text(2)?,
        pool: row.text(3)?,
        template: row.text(4)?,
        from: row.text(5)?,
        to: row.text(6)?,
        profile_digest: row.text(7)?,
        candidate_digest: row.text(8)?,
        surface_digest: row.text(9)?,
        governed: row.i64(10)? != 0,
        approved_by: row.opt_text(11)?,
        approved_at: row.opt_i64(12)?,
        verification: row.opt_text(13)?,
        state: row.text(14)?,
        reason: row.opt_text(15)?,
        started_at: row.i64(16)?,
        finished_at: row.opt_i64(17)?,
    })
}

/// Every re-verify, newest first.
pub async fn load_moves(db: &LocalDb) -> Result<Vec<PinMove>, String> {
    db.query_all(
        format!("SELECT {COLUMNS} FROM inference_pin_moves ORDER BY started_at DESC, id DESC"),
        (),
        move_of,
    )
    .await
    .map_err(|error| format!("read inference_pin_moves: {error}"))
}

/// Whether `profile` is the candidate of a governed profile's re-verify: what
/// the operator approved, which its verification must not revise.
pub async fn approved_candidate(db: &LocalDb, profile: &str) -> Result<bool, String> {
    Ok(load_moves(db)
        .await?
        .iter()
        .any(|m| m.candidate == profile && m.governed))
}

/// `profile` with only its engine replaced by its template's current pin,
/// and the change as a person reads it (`from`, `to`); `None` when it runs
/// the pin already, or has no pinned engine to move.
pub fn candidate(
    profile: &InferenceProfile,
    pins: &EnginePins,
) -> Option<(InferenceProfile, String, String, String)> {
    let lag = pins.lag(profile)?;
    let pin = pins.pin(&lag.template)?;
    let mut moved = profile.clone();
    match &mut moved.engine {
        Engine::Native(native) => {
            native.artifact = pin.artifact.clone()?;
            native.program = pin.program.clone()?;
        }
        Engine::Container(container) => container.image = pin.image.clone()?,
        Engine::Command { .. } => return None,
    }
    Some((moved, lag.template, lag.runs, lag.pinned))
}

/// What Settings offers a profile behind its pin.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReverifyOffer {
    /// The release it runs.
    pub from: String,
    /// The pin it would be re-verified on.
    pub to: String,
    /// The candidate's runnable-surface digest. A start sends it back, so it
    /// verifies (and, for a generated profile, approves) exactly what the
    /// page showed, never a pin that moved since.
    pub surface: String,
    /// A generated profile: starting it is the operator's approval of the
    /// engine change.
    pub needs_approval: bool,
    /// Why it cannot be re-verified now; the page hides the action while set.
    pub refusal: Option<String>,
    /// Its newest re-verify, under way or ended.
    pub last: Option<ReverifyView>,
}

/// A re-verify as Settings shows it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReverifyView {
    pub pool: String,
    pub to: String,
    pub state: String,
    pub reason: Option<String>,
}

/// The offer for `profile`, when it is behind its template's pin.
pub fn offer(gate: &Gate, pins: &EnginePins, profile: &InferenceProfile) -> Option<ReverifyOffer> {
    let (candidate, _, from, to) = candidate(profile, pins)?;
    Some(ReverifyOffer {
        from,
        to,
        surface: surface_of(&candidate),
        needs_approval: gate.governs(profile),
        refusal: gate.reverify_refusal(profile),
        last: gate
            .moves_of(profile.id.as_str())
            .next()
            .map(|m| ReverifyView {
                pool: m.pool.clone(),
                to: m.to.clone(),
                state: m.state.clone(),
                reason: m.reason.clone(),
            }),
    })
}

/// Start re-verifying `profile_id` on `pool` onto its template's current pin:
/// write the candidate and the re-verify's row together, claim the pool for
/// the candidate's verification, and run it in the background. `surface` is
/// the candidate surface the operator saw ([`ReverifyOffer::surface`]); for
/// a generated profile this call, the operator's own, is the approval of it.
pub async fn start(
    orch: &super::Orchestrator,
    profile_id: &str,
    pool: &str,
    surface: &str,
    requested_by: &str,
) -> Result<String, String> {
    let pool = super::inference_generate::detected_pool(orch, pool)?;
    let db = &orch.db.local;
    let pins = super::inference_pins::current();
    let now = crate::fleet::unix_time_ms() as i64;
    let row = {
        let held = INVENTORY.lock().await;
        let inventory = load_inventory(db).await?;
        let profile = inventory
            .profiles
            .get(&InferenceProfileId::from(profile_id))
            .ok_or_else(|| {
                format!("there is no profile {profile_id}; cairn://inference/profiles lists them")
            })?
            .clone();
        let Some((mut candidate, template, from, to)) = candidate(&profile, &pins) else {
            return Err(format!(
                "profile {profile_id} runs its template's current engine pin, or no pinned engine; there is nothing to re-verify it onto"
            ));
        };
        let gate = Gate::load(db).await?;
        if let Some(why) = gate.reverify_refusal(&profile) {
            return Err(format!(
                "profile {profile_id} cannot be re-verified on {to}: {why}"
            ));
        }
        let candidate_surface = surface_of(&candidate);
        if candidate_surface != surface {
            return Err(format!(
                "the engine pin for template {template} is no longer what the page showed (it is {to} now); nothing was written, so read the page again and re-verify on what it shows"
            ));
        }
        let id = uuid::Uuid::new_v4().to_string();
        candidate.id =
            InferenceProfileId::from(format!("{profile_id}-reverify-{}", &id[..8]).as_str());
        // The candidate is nobody's draft. Whether it is governed is this
        // row's to say, and the moved profile keeps the original's origin.
        candidate.origin = ProfileOrigin::Authored;
        let governed = gate.governs(&profile);
        let row = PinMove {
            id,
            profile: profile_id.to_string(),
            candidate: candidate.id.to_string(),
            pool: pool.key.to_string(),
            template,
            from,
            to,
            profile_digest: digest(&profile),
            candidate_digest: digest(&candidate),
            surface_digest: candidate_surface,
            governed,
            approved_by: governed.then(|| requested_by.to_string()),
            approved_at: governed.then_some(now),
            verification: None,
            state: VERIFYING.into(),
            reason: None,
            started_at: now,
            finished_at: None,
        };
        let mut records = Inventory::default();
        records.insert_profile(candidate);
        let (changes, _) = prepare_inventory_write(
            db,
            &inventory,
            &Authoring::Patch {
                upsert: Some(records.to_yaml()),
                remove: Removals::default(),
            },
            &held,
        )
        .await?;
        let inserted = row.clone();
        db.write(move |conn| {
            let (changes, row) = (changes.clone(), inserted.clone());
            Box::pin(async move {
                save_rows_in(conn, changes, row.started_at, None).await?;
                conn.execute(
                    &format!(
                        "INSERT INTO inference_pin_moves ({COLUMNS})
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13,
                                 NULL, ?14, NULL, ?15, NULL)"
                    ),
                    params![
                        row.id,
                        row.profile,
                        row.candidate,
                        row.pool,
                        row.template,
                        row.from,
                        row.to,
                        row.profile_digest,
                        row.candidate_digest,
                        row.surface_digest,
                        i64::from(row.governed),
                        row.approved_by,
                        row.approved_at,
                        row.state,
                        row.started_at
                    ],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .map_err(|error| format!("write inference_pin_moves: {error}"))?;
        row
    };
    let plan = match inference_verify::begin(
        db,
        &row.candidate,
        &pool,
        &orch.fleet.network(),
        requested_by,
        now,
    )
    .await
    {
        Ok(plan) => plan,
        Err(why) => {
            let said = settle(db, &row.id).await;
            return Err(match said {
                Ok(_) => format!("{why}; profile {profile_id} is unchanged"),
                Err(also) => format!("{why}; and then {also}"),
            });
        }
    };
    let live = inference_verify::Live::new(orch.clone())?;
    let (db, requested_by) = (db.clone(), requested_by.to_string());
    let (move_id, candidate, said_pool) = (
        row.id.clone(),
        InferenceProfileId::from(row.candidate.as_str()),
        pool.key.to_string(),
    );
    let first = plan.clone();
    cairn_common::task::spawn(async move {
        if let Err(error) = inference_verify::verify_in_order(
            &db,
            &live,
            vec![candidate],
            &pool,
            &requested_by,
            false,
            None,
            Some(first),
        )
        .await
        {
            log::warn!("inference: re-verify {move_id} stopped: {error}");
        }
        match settle(&db, &move_id).await {
            Ok(said) => log::info!("inference: re-verify {move_id}: {said}"),
            Err(error) => log::warn!("inference: re-verify {move_id} could not settle: {error}"),
        }
    });
    Ok(format!(
        "Re-verifying {profile_id} on {} ({} → {}) on {said_pool} (verification {}), reaching its head at {}. It moves onto the new engine only if this passes.",
        row.to, row.from, row.to, plan.id, plan.head
    ))
}

/// The profile `m` moves its original to, from the stored records, or why it
/// may not move.
fn moved_profile(
    m: &PinMove,
    gate: &Gate,
    inventory: &Inventory,
    verifications: &[VerificationRow],
) -> Result<(InferenceProfile, String), String> {
    let row = m
        .verification
        .as_deref()
        .and_then(|id| verifications.iter().find(|row| row.id == id))
        .ok_or("its verification never started")?;
    if row.outcome != "passed" {
        return Err(format!(
            "verification {} {}{}",
            row.id,
            row.outcome,
            row.reason
                .as_deref()
                .map(|why| format!(": {why}"))
                .unwrap_or_default()
        ));
    }
    let original = inventory
        .profiles
        .get(&InferenceProfileId::from(m.profile.as_str()))
        .ok_or_else(|| format!("profile {} is gone", m.profile))?;
    if digest(original) != m.profile_digest {
        return Err(format!(
            "profile {} was changed while its candidate was verified; a move lands only on the profile as it was when it began",
            m.profile
        ));
    }
    let candidate = inventory
        .profiles
        .get(&InferenceProfileId::from(m.candidate.as_str()))
        .ok_or_else(|| format!("its candidate {} is gone", m.candidate))?;
    if row.profile != m.candidate || row.profile_digest != digest(candidate) {
        return Err(format!(
            "its candidate {} is not what verification {} passed",
            m.candidate, row.id
        ));
    }
    if m.governed {
        if digest(candidate) != m.candidate_digest || surface_of(candidate) != m.surface_digest {
            return Err(format!(
                "its candidate {} is not what the operator approved",
                m.candidate
            ));
        }
        let promoted = gate.owned(&m.profile).is_some_and(|owned| {
            owned.promoted.is_some() && owned.profile_digest == m.profile_digest
        });
        if !promoted {
            return Err(format!(
                "profile {} is no longer promoted as it stood when the re-verify began",
                m.profile
            ));
        }
    }
    let mut moved = candidate.clone();
    moved.id = original.id.clone();
    moved.origin = original.origin.clone();
    Ok((moved, row.id.clone()))
}

/// Settle re-verify `id` once its verification has ended: move its profile on
/// a pass, else leave the profile as it was; either way remove the candidate.
/// Answers what it did. Repeating it changes nothing.
pub async fn settle(db: &LocalDb, id: &str) -> Result<String, String> {
    let held = INVENTORY.lock().await;
    let m = load_moves(db)
        .await?
        .into_iter()
        .find(|m| m.id == id)
        .ok_or_else(|| format!("there is no re-verify {id}"))?;
    if m.state != VERIFYING {
        return Ok(format!("it already {}", m.state));
    }
    let inventory = load_inventory(db).await?;
    let verifications = load_verifications(db).await?;
    if let Some(running) = m
        .verification
        .as_deref()
        .and_then(|v| verifications.iter().find(|row| row.id == v))
        .filter(|row| row.outcome == "running")
    {
        return Ok(format!("verification {} is still running", running.id));
    }
    let gate = Gate::load(db).await?;
    let candidate = InferenceProfileId::from(m.candidate.as_str());
    let remove = Removals {
        profiles: inventory
            .profiles
            .contains_key(&candidate)
            .then(|| candidate.clone())
            .into_iter()
            .collect(),
        deployments: inventory
            .deployments
            .values()
            .filter(|deployment| deployment.profile == candidate)
            .map(|deployment| deployment.id.clone())
            .collect::<Vec<DeploymentId>>(),
        ..Removals::default()
    };
    let now = crate::fleet::unix_time_ms() as i64;
    let why = match moved_profile(&m, &gate, &inventory, &verifications) {
        Ok((moved, verification)) => {
            let mut records = Inventory::default();
            records.insert_profile(moved.clone());
            match prepare_inventory_write(
                db,
                &inventory,
                &Authoring::Patch {
                    upsert: Some(records.to_yaml()),
                    remove: remove.clone(),
                },
                &held,
            )
            .await
            {
                Ok((changes, _)) => {
                    let (moved_digest, moved_surface) = (digest(&moved), surface_of(&moved));
                    let written = m.clone();
                    let verification_id = verification.clone();
                    db.write(move |conn| {
                        let (changes, m) = (changes.clone(), written.clone());
                        let (verification, moved_digest, moved_surface) = (
                            verification_id.clone(),
                            moved_digest.clone(),
                            moved_surface.clone(),
                        );
                        Box::pin(async move {
                            // Writing the profile clears its promotion; the
                            // re-promotion below lands in this same
                            // transaction, so no reader sees it unpromoted.
                            save_rows_in(conn, changes, now, None).await?;
                            conn.execute(
                                "UPDATE inference_verifications SET profile_id = ?2, profile_digest = ?3
                                  WHERE id = ?1",
                                params![verification.clone(), m.profile.clone(), moved_digest.clone()],
                            )
                            .await?;
                            if m.governed {
                                conn.execute(
                                    "UPDATE inference_generated_profiles
                                        SET profile_digest = ?2, surface_digest = ?3,
                                            promoted_verification_id = ?4
                                      WHERE profile_id = ?1",
                                    params![
                                        m.profile.clone(),
                                        moved_digest,
                                        moved_surface,
                                        verification
                                    ],
                                )
                                .await?;
                            }
                            conn.execute(
                                "UPDATE inference_pin_moves SET state = ?2, finished_at = ?3
                                  WHERE id = ?1 AND state = ?4",
                                params![m.id, MOVED, now, VERIFYING],
                            )
                            .await?;
                            Ok(())
                        })
                    })
                    .await
                    .map_err(|error| format!("write the move: {error}"))?;
                    log::info!(
                        "inference: re-verify {} moved profile {} from {} onto {}",
                        m.id,
                        m.profile,
                        m.from,
                        m.to
                    );
                    return Ok(format!(
                        "verification {verification} passed, and profile {} now runs {}",
                        m.profile, m.to
                    ));
                }
                Err(refused) => format!(
                    "it passed, but the move could not be written: {}",
                    String::from(refused)
                ),
            }
        }
        Err(why) => why,
    };
    fail(db, &inventory, &m, remove, why, &held, now).await
}

/// End `m` without moving its profile, removing its candidate. A candidate
/// that cannot be removed yet (its verification deployment still holds its
/// machines) is left to [`recover`]; the gate never places it meanwhile.
async fn fail(
    db: &LocalDb,
    inventory: &Inventory,
    m: &PinMove,
    remove: Removals,
    why: String,
    held: &tokio::sync::MutexGuard<'_, ()>,
    now: i64,
) -> Result<String, String> {
    let (changes, reason) = match prepare_inventory_write(
        db,
        inventory,
        &Authoring::Patch {
            upsert: None,
            remove,
        },
        held,
    )
    .await
    {
        Ok((changes, _)) => (changes, why.clone()),
        Err(refused) => (
            Vec::new(),
            format!(
                "{why}; its candidate {} could not be removed yet ({}), and is removed at the next start",
                m.candidate,
                String::from(refused)
            ),
        ),
    };
    let (id, stored) = (m.id.clone(), reason.clone());
    db.write(move |conn| {
        let (changes, id, reason) = (changes.clone(), id.clone(), stored.clone());
        Box::pin(async move {
            save_rows_in(conn, changes, now, None).await?;
            conn.execute(
                "UPDATE inference_pin_moves SET state = ?2, reason = ?3, finished_at = ?4
                  WHERE id = ?1 AND state = ?5",
                params![id, FAILED, reason, now, VERIFYING],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|error| format!("write inference_pin_moves: {error}"))?;
    log::info!(
        "inference: re-verify {} left profile {} on {}: {reason}",
        m.id,
        m.profile,
        m.from
    );
    Ok(format!("profile {} is unchanged: {reason}", m.profile))
}

/// At boot, after running verifications are failed: settle every re-verify a
/// stopped orch left under way (a pass it had not moved yet moves now), and
/// remove any ended re-verify's candidate still in the inventory.
pub async fn recover(db: &LocalDb) -> Result<(), String> {
    let booted = crate::fleet::unix_time_ms() as i64;
    for m in load_moves(db).await? {
        if m.started_at >= booted {
            continue;
        }
        if m.state == VERIFYING {
            settle(db, &m.id).await?;
            continue;
        }
        let held = INVENTORY.lock().await;
        let inventory = load_inventory(db).await?;
        let candidate = InferenceProfileId::from(m.candidate.as_str());
        if !inventory.profiles.contains_key(&candidate) {
            continue;
        }
        let remove = Removals {
            profiles: vec![candidate.clone()],
            deployments: inventory
                .deployments
                .values()
                .filter(|deployment| deployment.profile == candidate)
                .map(|deployment| deployment.id.clone())
                .collect(),
            ..Removals::default()
        };
        let (changes, _) = prepare_inventory_write(
            db,
            &inventory,
            &Authoring::Patch {
                upsert: None,
                remove,
            },
            &held,
        )
        .await?;
        super::inference::save_rows(db, changes, booted).await?;
        log::info!(
            "inference: removed candidate {} left by re-verify {}",
            m.candidate,
            m.id
        );
    }
    Ok(())
}
