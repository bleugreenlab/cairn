//! Positive promotion of generation-origin profiles.
//!
//! A profile a generation wrote never starts on the fleet on the strength of
//! who wrote it or of what it says about itself. Its row in
//! `inference_generated_profiles`, written by the generated-draft door in the
//! same transaction as the draft and keyed by profile id so that neither a
//! rewrite nor an edited `origin` erases it, carries the draft's revision, its
//! full-profile digest and its runnable-surface digest. Only a verification
//! pass of exactly that revision, claimed under the operator's approval of
//! exactly that surface on the generation's pool, promotes it ([`settle`]).
//!
//! A container start of a governed profile is refused unless the profile is
//! promoted as it stands now, or the deployment is the one verification claim
//! that approval admits. [`Gate`] is the single reading of that rule: the
//! final start (`inference_control::launch`), verification's start
//! (`inference_verify::begin`), placement's catalog and the Settings offers
//! all consult it, so an early filter and the final refusal never disagree.
//! Only the final start is the guarantee; the others keep drafts out of
//! choices early.
//!
//! Profiles generated before ownership was recorded have no row. They stay
//! startable only while a stored pass of exactly what they are now vouches
//! for them, and [`reconcile`] records that once at boot. Profiles the retired
//! template matcher wrote (generated, with no run) were never written by an
//! agent and are not governed.

use std::collections::{BTreeMap, BTreeSet};

use cairn_db::turso::params;
use cairn_inference::{
    Deployment, DeploymentId, InferenceProfile, InferenceProfileId, Inventory, ProfileOrigin,
};
use serde_json::Value;

use super::{list_generations, runnable_surface, surface_digest, GenerationRow};
use crate::orchestrator::inference::{load_inventory, INVENTORY};
use crate::orchestrator::inference_verify::{digest, load_verifications, VerificationRow};
use crate::storage::{DbResult, LocalDb, RowExt};

/// The state a generation that ended with its draft verified is left in.
const VERIFIED: &str = "verified";

/// What a generation wrote under one profile id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Owned {
    pub profile: String,
    pub generation: String,
    pub revision: i64,
    /// [`digest`] of the whole profile as the generation wrote it.
    pub profile_digest: String,
    /// [`surface_digest`] of its runnable surface as written.
    pub surface_digest: String,
    /// The verification whose pass promoted exactly this revision.
    pub promoted: Option<String>,
    /// Whether it is still its generation's current draft.
    pub active: bool,
}

const COLUMNS: &str = "profile_id, generation_id, revision, profile_digest, surface_digest, \
     promoted_verification_id, active";

fn owned_of(row: &cairn_db::turso::Row) -> DbResult<Owned> {
    Ok(Owned {
        profile: row.text(0)?,
        generation: row.text(1)?,
        revision: row.i64(2)?,
        profile_digest: row.text(3)?,
        surface_digest: row.text(4)?,
        promoted: row.opt_text(5)?,
        active: row.i64(6)? != 0,
    })
}

/// Every ownership row, by profile id.
pub async fn load_owned(db: &LocalDb) -> Result<BTreeMap<String, Owned>, String> {
    db.query_all(
        format!("SELECT {COLUMNS} FROM inference_generated_profiles"),
        (),
        owned_of,
    )
    .await
    .map(|rows| {
        rows.into_iter()
            .map(|owned| (owned.profile.clone(), owned))
            .collect()
    })
    .map_err(|error| format!("read inference_generated_profiles: {error}"))
}

/// The digest of what `profile` runs, as the draft door and the approval
/// prompt compute it. The draft door records an ownership row with this and
/// [`digest`] of the profile it stores, in the same transaction; a later
/// change to the stored profile clears the row's promotion as it is written.
pub fn surface_of(profile: &InferenceProfile) -> String {
    surface_digest(&runnable_surface(profile).unwrap_or(Value::Null))
}

/// How a verification of a profile may start.
#[derive(Debug, Clone, PartialEq)]
pub enum Admission {
    /// The profile is not a generation's, or is promoted as it stands.
    Ordinary,
    /// The approval-bound claim of a generation's current draft revision:
    /// claiming it must still find the generation bound exactly so.
    Draft(Binding),
    /// That revision's one claim already runs; a repeated start returns it.
    Replay(Box<VerificationRow>),
}

/// The generation state a draft's verification claim is bound to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Binding {
    pub generation: String,
    pub profile: String,
    pub revision: i64,
    pub profile_digest: String,
    pub surface_digest: String,
}

/// Everything the promotion rule reads, loaded together.
#[derive(Debug, Clone, Default)]
pub struct Gate {
    owned: BTreeMap<String, Owned>,
    generations: BTreeMap<String, GenerationRow>,
    /// Profiles any generation row names as its draft.
    drafted: BTreeSet<String>,
    verifications: Vec<VerificationRow>,
}

/// Whether `generation` is bound to `owned` exactly: the same draft, revision
/// and full digest, and the operator's approval of that revision's surface.
fn bound(generation: &GenerationRow, owned: &Owned) -> bool {
    generation.id == owned.generation
        && generation.draft_profile.as_deref() == Some(owned.profile.as_str())
        && generation.draft_revision == owned.revision
        && generation.draft_digest.as_deref() == Some(owned.profile_digest.as_str())
        && generation.approval_revision == Some(owned.revision)
        && generation.approved_surface.as_deref() == Some(owned.surface_digest.as_str())
}

impl Gate {
    pub async fn load(db: &LocalDb) -> Result<Self, String> {
        Ok(Self::of(
            load_owned(db).await?,
            list_generations(db).await?,
            load_verifications(db).await?,
        ))
    }

    pub fn of(
        owned: BTreeMap<String, Owned>,
        generations: Vec<GenerationRow>,
        verifications: Vec<VerificationRow>,
    ) -> Self {
        let drafted = generations
            .iter()
            .filter_map(|generation| generation.draft_profile.clone())
            .collect();
        Self {
            owned,
            generations: generations
                .into_iter()
                .map(|generation| (generation.id.clone(), generation))
                .collect(),
            drafted,
            verifications,
        }
    }

    fn verification(&self, id: &str) -> Option<&VerificationRow> {
        self.verifications.iter().find(|row| row.id == id)
    }

    /// Whether the promotion rule applies to `profile`: a generation owns or
    /// owned its id, a generation row names it, or it says an agent session
    /// generated it.
    pub fn governs(&self, profile: &InferenceProfile) -> bool {
        let id = profile.id.as_str();
        self.owned.contains_key(id)
            || self.drafted.contains(id)
            || matches!(
                profile.origin,
                ProfileOrigin::Generated { run: Some(_), .. }
            )
    }

    /// Why `profile`, as it stands, is not promoted; `None` when it is.
    fn unpromoted(&self, profile: &InferenceProfile) -> Option<String> {
        let id = profile.id.as_str();
        let current = digest(profile);
        let Some(owned) = self.owned.get(id) else {
            let vouched = self.verifications.iter().any(|row| {
                row.profile == id && row.outcome == "passed" && row.profile_digest == current
            });
            return (!vouched).then(|| {
                format!(
                    "profile {id} was generated, and no verification has passed it as it stands"
                )
            });
        };
        let Some(verification) = owned.promoted.as_deref() else {
            return Some(format!(
                "profile {id} is revision {} of generation {}'s draft, and no verification of it has passed",
                owned.revision, owned.generation
            ));
        };
        (owned.profile_digest != current || owned.surface_digest != surface_of(profile)).then(|| {
            format!(
                "profile {id} has changed since verification {verification} passed revision {} of generation {}'s draft, and what it is now has not passed",
                owned.revision, owned.generation
            )
        })
    }

    /// The running verification `deployment` belongs to, when it is the one
    /// claim the operator's approval of `profile`'s current revision admits,
    /// on its generation's pool.
    pub fn admitted_claim(
        &self,
        profile: &InferenceProfile,
        deployment: &Deployment,
    ) -> Option<&VerificationRow> {
        let id = profile.id.as_str();
        let owned = self.owned.get(id).filter(|owned| owned.active)?;
        let current = digest(profile);
        if owned.profile_digest != current || owned.surface_digest != surface_of(profile) {
            return None;
        }
        let generation = self.generations.get(&owned.generation)?;
        if !generation.is_open() || !bound(generation, owned) {
            return None;
        }
        let claim = self.verification(generation.verification_id.as_deref()?)?;
        (claim.outcome == "running"
            && claim.profile == id
            && claim.profile_digest == current
            && claim.pool == generation.pool
            && claim.deployment == deployment.id.as_str())
        .then_some(claim)
    }

    /// Why `deployment` may not start a container; `None` when it may. A
    /// deployment whose profile is missing is refused by its plan, not here.
    pub fn start_refusal(&self, inventory: &Inventory, deployment: &Deployment) -> Option<String> {
        let profile = inventory.profiles.get(&deployment.profile)?;
        if !self.governs(profile) {
            return None;
        }
        let why = self.unpromoted(profile)?;
        if self.admitted_claim(profile, deployment).is_some() {
            return None;
        }
        Some(format!(
            "{why}. A generated profile starts only once a verification pass promoted it as it stands, or as the verification the operator approved"
        ))
    }

    /// The governed profiles of `inventory` that are not promoted as they
    /// stand: what loads, offers and placement leave out.
    pub fn unpromoted_profiles(&self, inventory: &Inventory) -> BTreeSet<String> {
        inventory
            .profiles
            .values()
            .filter(|profile| self.governs(profile) && self.unpromoted(profile).is_some())
            .map(|profile| profile.id.to_string())
            .collect()
    }

    /// Why a fresh generation may not regenerate `profile` in place; `None`
    /// when it may. Regeneration recovers a profile a generation owns that is
    /// not promoted as it stands (an operator edit cleared its promotion, or
    /// no pass ever came) and that nothing works on now. Ownership is the
    /// recorded row, and the owner is a recorded generation that has ended:
    /// a missing owner refuses. The profile must still say it was generated
    /// by exactly that owner's agent, so an authored or imported profile
    /// written over a generated id is never a target even though the
    /// ownership row outlives it. The Settings offer and the replacement
    /// capture both read this, so the action is shown exactly where the
    /// capture would admit it; the capture adds what only its transaction can
    /// see.
    pub fn replacement_refusal(&self, profile: &InferenceProfile) -> Option<String> {
        let id = profile.id.as_str();
        let Some(owned) = self.owned.get(id) else {
            return Some(format!("no generation owns profile {id}"));
        };
        let Some(owner) = self.generations.get(&owned.generation) else {
            return Some(format!(
                "generation {} that owns profile {id} is not recorded",
                owned.generation
            ));
        };
        if let Some(why) = owner_refusal(owner, &profile.origin, profile.model.name.as_str(), id) {
            return Some(why);
        }
        if self.unpromoted(profile).is_none() {
            return Some(format!(
                "profile {id} is promoted as it stands and needs no regeneration"
            ));
        }
        let naming = |generation: &&GenerationRow| {
            generation.id == owned.generation || generation.draft_profile.as_deref() == Some(id)
        };
        if let Some(open) = self
            .generations
            .values()
            .filter(naming)
            .find(|generation| generation.is_open())
        {
            return Some(format!(
                "generation {} is still working on profile {id}",
                open.id
            ));
        }
        if let Some(pending) = self
            .generations
            .values()
            .filter(naming)
            .find(|generation| generation.cleanup_pending)
        {
            return Some(format!(
                "generation {}'s cleanup of profile {id} is still pending",
                pending.id
            ));
        }
        if let Some(running) = self
            .verifications
            .iter()
            .find(|row| row.profile == id && row.outcome == "running")
        {
            return Some(format!(
                "verification {} of profile {id} is still running",
                running.id
            ));
        }
        None
    }

    /// The profiles of `inventory` a fresh generation may regenerate in place.
    pub fn regenerable_profiles(&self, inventory: &Inventory) -> BTreeSet<String> {
        inventory
            .profiles
            .values()
            .filter(|profile| self.replacement_refusal(profile).is_none())
            .map(|profile| profile.id.to_string())
            .collect()
    }

    /// How a verification of `profile` (as the snapshot being claimed holds
    /// it) may start on `pool`, or why it may not.
    pub fn verification_admission(
        &self,
        profile: &InferenceProfile,
        pool: &str,
    ) -> Result<Admission, String> {
        if !self.governs(profile) || self.unpromoted(profile).is_none() {
            return Ok(Admission::Ordinary);
        }
        let id = profile.id.as_str();
        let refuse = |why: String| Err(format!("profile {id} cannot be verified: {why}"));
        let Some(owned) = self.owned.get(id) else {
            return refuse(
                "it was generated, and no generation owns it as it stands, so nothing the operator approved covers what it runs; generate a profile for its model anew".into(),
            );
        };
        let Some(generation) = self.generations.get(&owned.generation) else {
            return refuse(format!(
                "generation {} that wrote it is no longer recorded",
                owned.generation
            ));
        };
        if !generation.is_open() {
            return refuse(format!(
                "generation {} ended {}, and its draft is not verified after its generation ends",
                generation.id, generation.state
            ));
        }
        if !owned.active
            || generation.draft_profile.as_deref() != Some(id)
            || generation.draft_revision != owned.revision
        {
            return refuse(format!(
                "it is no longer generation {}'s current draft",
                generation.id
            ));
        }
        if owned.profile_digest != digest(profile) || owned.surface_digest != surface_of(profile) {
            return refuse(format!(
                "it has changed since generation {} wrote revision {}",
                generation.id, owned.revision
            ));
        }
        if generation.pool != pool {
            return refuse(format!(
                "generation {} verifies its draft on pool {}, not {pool}",
                generation.id, generation.pool
            ));
        }
        if !bound(generation, owned) {
            return refuse(format!(
                "the operator has not approved what revision {} runs (surface {}); generation {} asks before verifying it",
                owned.revision, owned.surface_digest, generation.id
            ));
        }
        if let Some(claimed) = generation.verification_id.as_deref() {
            return match self.verification(claimed) {
                Some(row) if row.outcome == "running" => {
                    Ok(Admission::Replay(Box::new(row.clone())))
                }
                Some(row) => refuse(format!(
                    "revision {} already had its one verification, {claimed}, which {}",
                    owned.revision, row.outcome
                )),
                None => refuse(format!(
                    "revision {} already had its one verification, {claimed}",
                    owned.revision
                )),
            };
        }
        Ok(Admission::Draft(Binding {
            generation: generation.id.clone(),
            profile: id.to_string(),
            revision: owned.revision,
            profile_digest: owned.profile_digest.clone(),
            surface_digest: owned.surface_digest.clone(),
        }))
    }

    /// The ownership row `verification`'s pass promotes: a pass of the
    /// current draft revision as it stands, on its generation's pool, that is
    /// the revision's one approval-bound claim.
    fn promotion(&self, inventory: &Inventory, verification: &str) -> Option<&Owned> {
        let row = self
            .verification(verification)
            .filter(|row| row.outcome == "passed")?;
        let owned = self
            .owned
            .get(&row.profile)
            .filter(|owned| owned.active && owned.promoted.is_none())?;
        let profile = inventory
            .profiles
            .get(&InferenceProfileId::from(row.profile.as_str()))?;
        let current = digest(profile);
        let generation = self.generations.get(&owned.generation)?;
        let settled = generation.is_open() || generation.state == VERIFIED;
        (settled
            && bound(generation, owned)
            && generation.verification_id.as_deref() == Some(verification)
            && row.pool == generation.pool
            && row.profile_digest == current
            && owned.profile_digest == current
            && owned.surface_digest == surface_of(profile))
        .then_some(owned)
    }
}

/// Why `owner`, the generation the ownership ledger names for profile `id`,
/// does not vouch for regenerating it: it has not ended, or the profile as it
/// stands (`origin`, serving `model`) is not that owner's agent's own draft.
/// The replacement capture's transaction applies the same rule to the row it
/// reads.
pub(crate) fn owner_refusal(
    owner: &GenerationRow,
    origin: &ProfileOrigin,
    model: &str,
    id: &str,
) -> Option<String> {
    if owner.state == super::RUNNING || owner.state == super::launch::STARTING {
        return Some(format!(
            "generation {} is still working on profile {id}",
            owner.id
        ));
    }
    let ProfileOrigin::Generated { run: Some(run), .. } = origin else {
        return Some(format!(
            "profile {id} is no longer a generation's draft as it stands; it is the operator's own"
        ));
    };
    if run != &owner.node_uri {
        return Some(format!(
            "profile {id} says {run} generated it, not generation {}'s agent",
            owner.id
        ));
    }
    if owner.model != model {
        return Some(format!(
            "profile {id} serves {model}, and generation {} generated for {}",
            owner.id, owner.model
        ));
    }
    None
}

/// Why writing `next` over `current` is refused for what generations own,
/// given the ownership rows and the verifications running now:
///
/// - a generated draft, or the deployment its verification claimed, is not
///   changed or removed while that verification runs;
/// - no write but the generated-draft door's (`generation`) introduces a
///   profile that says it was generated, or changes what a profile says about
///   being generated.
pub(crate) fn refusals(
    owned: &BTreeMap<String, Owned>,
    running: &[VerificationRow],
    current: &Inventory,
    next: &Inventory,
    generation: Option<&str>,
) -> Vec<String> {
    let mut refusals = Vec::new();
    for claim in running
        .iter()
        .filter(|row| owned.contains_key(&row.profile))
    {
        let profile = InferenceProfileId::from(claim.profile.as_str());
        if current.profiles.get(&profile) != next.profiles.get(&profile) {
            refusals.push(format!(
                "profile {} is a generation's draft that verification {} is running now; it is not changed or removed until that verification ends",
                claim.profile, claim.id
            ));
        }
        let deployment = DeploymentId::from(claim.deployment.as_str());
        if current.deployments.get(&deployment) != next.deployments.get(&deployment) {
            refusals.push(format!(
                "deployment {} belongs to verification {} of a generation's draft; it is not changed or removed until that verification ends",
                claim.deployment, claim.id
            ));
        }
    }
    if generation.is_none() {
        for (id, profile) in &next.profiles {
            if !matches!(profile.origin, ProfileOrigin::Generated { .. }) {
                continue;
            }
            if current.profiles.get(id).map(|before| &before.origin) == Some(&profile.origin) {
                continue;
            }
            refusals.push(format!(
                "profile {id} says it was generated, and only a generation's own draft writes a generated profile; a copy or an edit of one is the operator's own, written with no origin"
            ));
        }
    }
    refusals
}

/// [`refusals`] for a write of `next` over `current`, reading the ownership
/// rows and running verifications. The inventory writer calls it under the
/// inventory lock, so what it reads cannot move before the write lands.
pub(crate) async fn inventory_refusals(
    db: &LocalDb,
    current: &Inventory,
    next: &Inventory,
    generation: Option<&str>,
) -> Result<Vec<String>, String> {
    if current.profiles == next.profiles && current.deployments == next.deployments {
        return Ok(Vec::new());
    }
    let owned = load_owned(db).await?;
    let running: Vec<VerificationRow> = load_verifications(db)
        .await?
        .into_iter()
        .filter(|row| row.outcome == "running")
        .collect();
    Ok(refusals(&owned, &running, current, next, generation))
}

/// Promote what `verification` passed, when it is the approval-bound claim of
/// its generation's current draft revision as it stands. Answers the profile
/// it promoted. Verification settlement calls this once the verification's
/// outcome is stored; it is idempotent, so the boot backstop may repeat it.
pub async fn settle(db: &LocalDb, verification: &str) -> Result<Option<String>, String> {
    let _held = INVENTORY.lock().await;
    let inventory = load_inventory(db).await?;
    let gate = Gate::load(db).await?;
    let Some(owned) = gate.promotion(&inventory, verification).cloned() else {
        return Ok(None);
    };
    let verification = verification.to_string();
    let promoted = db
        .write(move |conn| {
            let (owned, verification) = (owned.clone(), verification.clone());
            Box::pin(async move {
                let changed = conn
                    .execute(
                        "UPDATE inference_generated_profiles SET promoted_verification_id = ?1
                          WHERE profile_id = ?2 AND generation_id = ?3 AND revision = ?4
                            AND profile_digest = ?5 AND surface_digest = ?6 AND active = 1
                            AND promoted_verification_id IS NULL",
                        params![
                            verification,
                            owned.profile.as_str(),
                            owned.generation.as_str(),
                            owned.revision,
                            owned.profile_digest.as_str(),
                            owned.surface_digest.as_str()
                        ],
                    )
                    .await?;
                Ok((changed > 0).then_some(owned.profile))
            })
        })
        .await
        .map_err(|error| format!("write inference_generated_profiles: {error}"))?;
    if let Some(profile) = &promoted {
        log::info!("inference: verification promoted generated profile {profile}");
    }
    Ok(promoted)
}

/// Verification settlement: what follows once a verification's outcome is
/// stored, whatever it was. A failure to settle is logged, never raised into
/// the verification, whose outcome stands; the boot backstop settles again.
pub async fn settled(db: &LocalDb, verification: &str) {
    super::verification_settled(db, verification).await;
}

/// The boot backstop: record ownership of generated profiles written before
/// it was recorded (promoted only by a stored pass of exactly what they are
/// now), then settle every pass a generation claimed that a stopped orch did
/// not get to promote. Repeating it changes nothing.
pub async fn reconcile(db: &LocalDb) -> Result<(), String> {
    {
        let _held = INVENTORY.lock().await;
        let inventory = load_inventory(db).await?;
        let gate = Gate::load(db).await?;
        let rows = backfill_rows(&gate, &inventory);
        if !rows.is_empty() {
            let count = rows.len();
            db.write(move |conn| {
                let rows = rows.clone();
                Box::pin(async move {
                    for row in rows {
                        conn.execute(
                            "INSERT INTO inference_generated_profiles
                               (profile_id, generation_id, revision, profile_digest, surface_digest,
                                promoted_verification_id, active)
                             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                             ON CONFLICT(profile_id) DO NOTHING",
                            params![
                                row.profile,
                                row.generation,
                                row.revision,
                                row.profile_digest,
                                row.surface_digest,
                                row.promoted,
                                i64::from(row.active)
                            ],
                        )
                        .await?;
                    }
                    Ok(())
                })
            })
            .await
            .map_err(|error| format!("write inference_generated_profiles: {error}"))?;
            log::info!("inference: recorded ownership of {count} generated profile(s)");
        }
    }
    let claimed: Vec<String> = list_generations(db)
        .await?
        .into_iter()
        .filter_map(|generation| generation.verification_id)
        .collect();
    for verification in claimed {
        settle(db, &verification).await?;
    }
    Ok(())
}

/// The ownership rows the governed profiles of `inventory` with none are
/// recorded with: their newest generation, if a row names them, and a stored
/// pass of exactly what they are now, if one exists.
fn backfill_rows(gate: &Gate, inventory: &Inventory) -> Vec<Owned> {
    inventory
        .profiles
        .values()
        .filter(|profile| !gate.owned.contains_key(profile.id.as_str()) && gate.governs(profile))
        .map(|profile| {
            let id = profile.id.as_str();
            let current = digest(profile);
            let generation = gate
                .generations
                .values()
                .filter(|generation| generation.draft_profile.as_deref() == Some(id))
                .max_by_key(|generation| generation.started_at);
            let run = match &profile.origin {
                ProfileOrigin::Generated { run: Some(run), .. } => run.clone(),
                _ => String::new(),
            };
            Owned {
                profile: id.to_string(),
                generation: generation.map_or(run, |generation| generation.id.clone()),
                revision: generation.map_or(0, |generation| generation.draft_revision),
                profile_digest: current.clone(),
                surface_digest: surface_of(profile),
                promoted: gate
                    .verifications
                    .iter()
                    .find(|row| {
                        row.profile == id
                            && row.outcome == "passed"
                            && row.profile_digest == current
                    })
                    .map(|row| row.id.clone()),
                active: generation.is_some_and(GenerationRow::is_open),
            }
        })
        .collect()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    const SHA: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    fn profile(id: &str, image_tag: &str, run: Option<&str>) -> InferenceProfile {
        let yaml = format!(
            r#"
id: {id}
model:
  name: chat/Model
  weights: [{{repo: chat/Model, revision: abc, path: m.gguf, sha256: {SHA}}}]
servedName: chat/Model
engine:
  kind: container
  image: "ghcr.io/ggml-org/llama.cpp@sha256:{SHA}"
  server: llamaCpp
  args: [-m, "{{weights}}", --alias, "{image_tag}"]
requirements: {{machines: 1}}
"#
        );
        let mut profile: InferenceProfile = serde_yaml::from_str(&yaml).unwrap();
        if let Some(run) = run {
            profile.origin = ProfileOrigin::Generated {
                run: Some(run.to_string()),
                model_revision: "abc".into(),
                starting_template: None,
                sources: Vec::new(),
                answers: Vec::new(),
            };
        }
        profile
    }

    fn deployment(id: &str, profile: &str) -> Deployment {
        serde_yaml::from_str(&format!(
            "{{id: {id}, profile: {profile}, machines: [box], head: box, apiPort: 8090, apiPath: /v1, healthPath: /v1/models}}"
        ))
        .unwrap()
    }

    fn inventory(profiles: &[&InferenceProfile], deployments: &[Deployment]) -> Inventory {
        let mut inventory = Inventory::default();
        for profile in profiles {
            inventory.insert_profile((*profile).clone());
        }
        for deployment in deployments {
            inventory.insert_deployment(deployment.clone());
        }
        inventory
    }

    pub(crate) fn owned(profile: &InferenceProfile, revision: i64) -> Owned {
        Owned {
            profile: profile.id.to_string(),
            generation: "g".into(),
            revision,
            profile_digest: digest(profile),
            surface_digest: surface_of(profile),
            promoted: None,
            active: true,
        }
    }

    /// Generation `g`, open, bound to `owned` and approved, with `claim` as
    /// its revision's verification.
    pub(crate) fn generation(owned: &Owned, claim: Option<&str>) -> GenerationRow {
        GenerationRow {
            id: "g".into(),
            project_id: "p".into(),
            issue_id: None,
            job_id: "job".into(),
            mode: "agent".into(),
            execution_id: Some("exec".into()),
            attempts: 1,
            draft_revision: owned.revision,
            draft_digest: Some(owned.profile_digest.clone()),
            approval_prompt_id: Some("prompt".into()),
            approval_run_id: Some("run".into()),
            approval_revision: Some(owned.revision),
            verification_id: claim.map(str::to_string),
            cleanup_pending: false,
            node_uri: "cairn://p/x/1/1/agent".into(),
            model: "chat/Model".into(),
            pool: "box".into(),
            state: super::super::RUNNING.into(),
            draft_profile: Some(owned.profile.clone()),
            draft_written_at: Some(1),
            approved_surface: Some(owned.surface_digest.clone()),
            hardware: BTreeMap::new(),
            outcome: None,
            started_at: 0,
            finished_at: None,
        }
    }

    fn verification(id: &str, profile: &InferenceProfile, outcome: &str) -> VerificationRow {
        VerificationRow {
            id: id.into(),
            profile: profile.id.to_string(),
            profile_digest: digest(profile),
            pool: "box".into(),
            deployment: format!("verify-{}-box", profile.id),
            machines: vec!["box".into()],
            requested_by: "operator".into(),
            started_at: 2,
            finished_at: None,
            outcome: outcome.into(),
            stage: "start".into(),
            reason: None,
            load_ms: None,
            footprint: None,
            context_tokens: None,
            kv_tokens: None,
            ttft_ms: None,
            decode_tps: None,
            prefill_tps: None,
            embed_tps: None,
            engine_version: None,
        }
    }

    fn gate(
        owned: &[Owned],
        generations: Vec<GenerationRow>,
        verifications: Vec<VerificationRow>,
    ) -> Gate {
        Gate::of(
            owned
                .iter()
                .map(|owned| (owned.profile.clone(), owned.clone()))
                .collect(),
            generations,
            verifications,
        )
    }

    #[test]
    fn only_an_owned_unpromoted_profile_nothing_works_on_is_regenerable() {
        const AGENT: &str = "cairn://p/x/1/1/agent";
        let verified = profile("d", "one", Some(AGENT));
        let mut row = owned(&verified, 1);
        row.promoted = Some("v".into());
        row.active = false;
        let mut ended = generation(&row, Some("v"));
        ended.state = VERIFIED.into();
        let edited = profile("d", "two", Some(AGENT));
        let claims_origin = profile("x", "one", Some(AGENT));
        let authored = profile("a", "one", None);

        let settled = gate(
            std::slice::from_ref(&row),
            vec![ended.clone()],
            vec![verification("v", &verified, "passed")],
        );
        assert_eq!(settled.replacement_refusal(&edited), None);
        assert!(settled.replacement_refusal(&verified).is_some());
        assert!(settled.replacement_refusal(&claims_origin).is_some());
        assert!(settled.replacement_refusal(&authored).is_some());
        assert_eq!(
            settled.regenerable_profiles(&inventory(&[&edited, &claims_origin, &authored], &[])),
            BTreeSet::from(["d".to_string()])
        );

        let mut open = generation(&row, None);
        open.id = "g2".into();
        let regenerating = gate(
            std::slice::from_ref(&row),
            vec![ended.clone(), open],
            Vec::new(),
        );
        assert!(regenerating.replacement_refusal(&edited).is_some());

        let mut cleaning = ended.clone();
        cleaning.cleanup_pending = true;
        let held = gate(std::slice::from_ref(&row), vec![cleaning], Vec::new());
        assert!(held.replacement_refusal(&edited).is_some());

        let verifying = gate(
            std::slice::from_ref(&row),
            vec![ended],
            vec![verification("v2", &edited, "running")],
        );
        assert!(verifying.replacement_refusal(&edited).is_some());
    }

    #[test]
    fn a_regeneration_target_is_its_recorded_ended_owners_own_generated_draft() {
        const AGENT: &str = "cairn://p/x/1/1/agent";
        let generated = profile("d", "one", Some(AGENT));
        let row = owned(&generated, 1);
        let mut ended = generation(&row, None);
        ended.state = "failed".into();
        let edited = profile("d", "two", Some(AGENT));
        let settled = gate(std::slice::from_ref(&row), vec![ended.clone()], Vec::new());
        assert_eq!(settled.replacement_refusal(&edited), None);

        // An owner the ledger names but no generation row records fails closed.
        let orphaned = gate(std::slice::from_ref(&row), Vec::new(), Vec::new());
        let why = orphaned.replacement_refusal(&edited).unwrap();
        assert!(why.contains("not recorded"), "{why}");

        // The operator's own profile, or an imported one, written over the
        // generated id keeps the ownership row but is not a target.
        let authored = profile("d", "two", None);
        let why = settled.replacement_refusal(&authored).unwrap();
        assert!(why.contains("operator's own"), "{why}");
        let mut imported = edited.clone();
        imported.origin = ProfileOrigin::Imported {
            url: "https://github.com/o/r".into(),
            commit: "c".into(),
            compose: "compose.yaml".into(),
            service: "llm".into(),
        };
        assert!(settled.replacement_refusal(&imported).is_some());
        let mut matched = edited.clone();
        matched.origin = ProfileOrigin::Generated {
            run: None,
            model_revision: "abc".into(),
            starting_template: None,
            sources: Vec::new(),
            answers: Vec::new(),
        };
        assert!(settled.replacement_refusal(&matched).is_some());

        // A run stamp naming any other agent, or another model, is refused.
        let elsewhere = profile("d", "two", Some("cairn://p/x/2/1/agent"));
        let why = settled.replacement_refusal(&elsewhere).unwrap();
        assert!(why.contains("not generation g's agent"), "{why}");
        let mut remodelled = edited.clone();
        remodelled.model.name = "chat/Other".into();
        let why = settled.replacement_refusal(&remodelled).unwrap();
        assert!(why.contains("generated for chat/Model"), "{why}");

        // An owner still starting is not ended.
        let mut starting = ended;
        starting.state = super::super::launch::STARTING.into();
        let launching = gate(std::slice::from_ref(&row), vec![starting], Vec::new());
        assert!(launching.replacement_refusal(&edited).is_some());
    }

    #[test]
    fn an_unpromoted_draft_never_starts_and_only_its_approved_claim_does() {
        let draft = profile("d", "one", Some("run"));
        let row = owned(&draft, 1);
        let claim = deployment("verify-d-box", "d");
        let stale = deployment("d-box", "d");
        let records = inventory(&[&draft], &[claim.clone(), stale.clone()]);

        let unclaimed = gate(
            std::slice::from_ref(&row),
            vec![generation(&row, None)],
            Vec::new(),
        );
        assert!(unclaimed.start_refusal(&records, &claim).is_some());
        assert!(unclaimed.start_refusal(&records, &stale).is_some());

        let claimed = gate(
            std::slice::from_ref(&row),
            vec![generation(&row, Some("v"))],
            vec![verification("v", &draft, "running")],
        );
        assert_eq!(claimed.start_refusal(&records, &claim), None);
        // The claim admits its own deployment, not another of the draft.
        assert!(claimed.start_refusal(&records, &stale).is_some());

        // A claim that is not bound to the operator's approval of this
        // revision admits nothing.
        let mut unapproved = generation(&row, Some("v"));
        unapproved.approval_revision = Some(0);
        let refused = gate(
            std::slice::from_ref(&row),
            vec![unapproved],
            vec![verification("v", &draft, "running")],
        );
        assert!(refused.start_refusal(&records, &claim).is_some());

        // Nor does an ended generation's, or a finished claim.
        let mut ended = generation(&row, Some("v"));
        ended.state = "cancelled".into();
        assert!(gate(
            std::slice::from_ref(&row),
            vec![ended],
            vec![verification("v", &draft, "running")]
        )
        .start_refusal(&records, &claim)
        .is_some());
        assert!(gate(
            std::slice::from_ref(&row),
            vec![generation(&row, Some("v"))],
            vec![verification("v", &draft, "failed")]
        )
        .start_refusal(&records, &claim)
        .is_some());
    }

    #[test]
    fn a_promoted_draft_starts_until_it_is_rewritten_or_its_origin_is_erased() {
        let draft = profile("d", "one", Some("run"));
        let mut row = owned(&draft, 1);
        row.promoted = Some("v".into());
        let stale = deployment("d-box", "d");
        let promoted = gate(
            std::slice::from_ref(&row),
            vec![generation(&row, Some("v"))],
            Vec::new(),
        );
        // The promoting verification may since have been pruned: the row is
        // what vouches.
        assert_eq!(
            promoted.start_refusal(&inventory(&[&draft], std::slice::from_ref(&stale)), &stale),
            None
        );

        let rewritten = profile("d", "two", Some("run"));
        let why = promoted
            .start_refusal(
                &inventory(&[&rewritten], std::slice::from_ref(&stale)),
                &stale,
            )
            .unwrap();
        assert!(
            why.contains("has changed since verification v passed"),
            "{why}"
        );

        // Saying it was authored does not take it out of the generation's
        // ownership.
        let mut erased = rewritten.clone();
        erased.origin = ProfileOrigin::Authored;
        assert!(promoted
            .start_refusal(&inventory(&[&erased], std::slice::from_ref(&stale)), &stale)
            .is_some());
        // Nor does a generation row that names it as a draft.
        let named = gate(&[], vec![generation(&owned(&erased, 1), None)], Vec::new());
        assert!(named
            .start_refusal(&inventory(&[&erased], std::slice::from_ref(&stale)), &stale)
            .is_some());
    }

    #[test]
    fn a_profile_generated_before_ownership_was_recorded_starts_only_on_a_pass_of_it_as_it_stands()
    {
        let legacy = profile("l", "one", Some("run"));
        let run = deployment("l-box", "l");
        let records = inventory(&[&legacy], std::slice::from_ref(&run));
        assert!(gate(&[], Vec::new(), Vec::new())
            .start_refusal(&records, &run)
            .is_some());
        let passed = gate(&[], Vec::new(), vec![verification("v", &legacy, "passed")]);
        assert_eq!(passed.start_refusal(&records, &run), None);
        let edited = profile("l", "two", Some("run"));
        assert!(passed
            .start_refusal(&inventory(&[&edited], std::slice::from_ref(&run)), &run)
            .is_some());
        // The retired template matcher's output was never an agent's and is
        // not governed.
        let mut template = profile("t", "one", None);
        template.origin = ProfileOrigin::Generated {
            run: None,
            model_revision: "abc".into(),
            starting_template: Some("llama".into()),
            sources: Vec::new(),
            answers: Vec::new(),
        };
        let t = deployment("t-box", "t");
        assert_eq!(
            gate(&[], Vec::new(), Vec::new())
                .start_refusal(&inventory(&[&template], std::slice::from_ref(&t)), &t),
            None
        );

        let rows = backfill_rows(&passed, &records);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].promoted.as_deref(), Some("v"));
        assert!(!rows[0].active);
        assert!(
            backfill_rows(&gate(&[], Vec::new(), Vec::new()), &records)[0]
                .promoted
                .is_none()
        );
    }

    #[test]
    fn a_draft_is_verified_once_per_revision_on_its_pool_under_the_operators_approval() {
        let draft = profile("d", "one", Some("run"));
        let row = owned(&draft, 2);
        let admitted = gate(
            std::slice::from_ref(&row),
            vec![generation(&row, None)],
            Vec::new(),
        );
        assert_eq!(
            admitted.verification_admission(&draft, "box"),
            Ok(Admission::Draft(Binding {
                generation: "g".into(),
                profile: "d".into(),
                revision: 2,
                profile_digest: digest(&draft),
                surface_digest: surface_of(&draft),
            }))
        );
        let why = admitted
            .verification_admission(&draft, "other")
            .unwrap_err();
        assert!(why.contains("on pool box, not other"), "{why}");

        // The snapshot claimed must be the revision approved.
        let changed = profile("d", "two", Some("run"));
        let why = admitted
            .verification_admission(&changed, "box")
            .unwrap_err();
        assert!(
            why.contains("changed since generation g wrote revision 2"),
            "{why}"
        );

        let mut unapproved = generation(&row, None);
        unapproved.approved_surface = Some("something-else".into());
        let why = gate(std::slice::from_ref(&row), vec![unapproved], Vec::new())
            .verification_admission(&draft, "box")
            .unwrap_err();
        assert!(why.contains("has not approved"), "{why}");

        let running = gate(
            std::slice::from_ref(&row),
            vec![generation(&row, Some("v"))],
            vec![verification("v", &draft, "running")],
        );
        assert!(matches!(
            running.verification_admission(&draft, "box"),
            Ok(Admission::Replay(claim)) if claim.id == "v"
        ));
        let spent = gate(
            std::slice::from_ref(&row),
            vec![generation(&row, Some("v"))],
            vec![verification("v", &draft, "failed")],
        );
        let why = spent.verification_admission(&draft, "box").unwrap_err();
        assert!(why.contains("already had its one verification, v"), "{why}");

        let mut ended = generation(&row, None);
        ended.state = "gaveUp".into();
        assert!(gate(std::slice::from_ref(&row), vec![ended], Vec::new())
            .verification_admission(&draft, "box")
            .is_err());
        // A forged generated profile no generation owns is never verified.
        let forged = profile("f", "one", Some("run"));
        assert!(admitted.verification_admission(&forged, "box").is_err());
        // An ordinary profile is not this rule's.
        let ordinary = profile("o", "one", None);
        assert_eq!(
            admitted.verification_admission(&ordinary, "box"),
            Ok(Admission::Ordinary)
        );
    }

    #[test]
    fn only_the_bound_claims_pass_of_the_draft_as_it_stands_promotes_it() {
        let draft = profile("d", "one", Some("run"));
        let row = owned(&draft, 1);
        let records = inventory(&[&draft], &[]);
        let bound_pass = gate(
            std::slice::from_ref(&row),
            vec![generation(&row, Some("v"))],
            vec![verification("v", &draft, "passed")],
        );
        assert_eq!(bound_pass.promotion(&records, "v"), Some(&row));
        // A pass the generation did not claim for this revision does not.
        let other = gate(
            std::slice::from_ref(&row),
            vec![generation(&row, Some("w"))],
            vec![verification("v", &draft, "passed")],
        );
        assert_eq!(other.promotion(&records, "v"), None);
        // Nor a pass of content the draft no longer has.
        let rewritten = profile("d", "two", Some("run"));
        assert_eq!(
            bound_pass.promotion(&inventory(&[&rewritten], &[]), "v"),
            None
        );
        // Nor once the generation was stopped.
        let mut cancelled = generation(&row, Some("v"));
        cancelled.state = "cancelled".into();
        assert_eq!(
            gate(
                std::slice::from_ref(&row),
                vec![cancelled],
                vec![verification("v", &draft, "passed")]
            )
            .promotion(&records, "v"),
            None
        );
        assert_eq!(
            gate(
                std::slice::from_ref(&row),
                vec![generation(&row, Some("v"))],
                vec![verification("v", &draft, "failed")]
            )
            .promotion(&records, "v"),
            None
        );
    }

    #[test]
    fn a_draft_under_verification_is_not_rewritten_removed_or_forged() {
        let draft = profile("d", "one", Some("run"));
        let row = owned(&draft, 1);
        let owned: BTreeMap<String, Owned> = [("d".to_string(), row)].into();
        let claim = deployment("verify-d-box", "d");
        let running = vec![verification("v", &draft, "running")];
        let current = inventory(&[&draft], std::slice::from_ref(&claim));

        let rewritten = inventory(
            &[&profile("d", "two", Some("run"))],
            std::slice::from_ref(&claim),
        );
        let why = refusals(&owned, &running, &current, &rewritten, Some("g"));
        assert_eq!(why.len(), 1, "{why:?}");
        assert!(why[0].contains("verification v is running"), "{why:?}");
        let removed = inventory(&[], &[]);
        assert_eq!(
            refusals(&owned, &running, &current, &removed, None).len(),
            2
        );
        // Once it ends, the generation may replace it.
        assert!(refusals(&owned, &[], &current, &rewritten, Some("g")).is_empty());

        // Writing a generated profile anywhere but the draft door is refused,
        // whether a new id or a profile made to say it was generated; an edit
        // that keeps what it says is not.
        let plain = profile("p", "one", None);
        let base = inventory(&[&plain, &draft], &[]);
        let forged = inventory(&[&profile("p", "one", Some("run")), &draft], &[]);
        assert_eq!(refusals(&owned, &[], &base, &forged, None).len(), 1);
        let copied = inventory(&[&plain, &draft, &profile("c", "one", Some("run"))], &[]);
        assert_eq!(refusals(&owned, &[], &base, &copied, None).len(), 1);
        let edited = inventory(&[&plain, &profile("d", "two", Some("run"))], &[]);
        assert!(refusals(&owned, &[], &base, &edited, None).is_empty());
    }
}
