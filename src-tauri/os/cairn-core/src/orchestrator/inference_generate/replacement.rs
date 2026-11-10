//! Regenerating a generated profile in place (cairn/6093).
//!
//! An operator edit clears a generated profile's promotion, and the ended
//! generation that wrote it never regains authority over it. The operator
//! recovers it by starting one fresh generation aimed at that exact profile.
//! Launch captures the profile as saved ([`capture`]) and records the capture
//! against the new generation before its agent runs ([`record_in`]); nothing
//! in the inventory or the ownership ledger moves then. The generation's first
//! accepted draft is the one write that may take the profile from its old
//! owner, and only while the profile is still exactly what was captured
//! ([`validate_in`]). A generation's capture is never taken again: a profile
//! that changed after launch needs a new regeneration.
//!
//! [`Gate::replacement_refusal`] is the policy of which profiles may be
//! regenerated, read by the Settings offer and by [`capture`]. The checks
//! here repeat it in SQL inside the transaction that relies on it, beside
//! what only that transaction can see: the literal saved body, the inventory
//! revision, and other generations' captures.
//!
//! The capture keeps the literal stored body. The ordinary issue carries the
//! same snapshot for the operator; this row is what the door checks against.

use cairn_db::turso::params;
use cairn_inference::{InferenceProfile, InferenceProfileId};

use super::launch::{AGENT_MODE, STARTING};
use super::promotion::{owner_refusal, surface_of, Gate};
use super::RUNNING;
use crate::orchestrator::inference::{load_inventory, now_ms, INVENTORY};
use crate::orchestrator::inference_verify::digest;
use crate::storage::{DbError, DbResult, LocalDb, RowExt, TrackedConnection};

/// A generated profile exactly as a regenerating generation captured it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    pub profile_id: String,
    /// The model the generation was started for, which the profile serves.
    pub model: String,
    /// The generation the ownership ledger names as the profile's owner.
    pub prior_generation: String,
    /// That owner's draft revision of it.
    pub prior_revision: i64,
    /// How many times the profile's inventory row had been written.
    pub inventory_revision: i64,
    /// [`digest`] of the profile as saved.
    pub profile_digest: String,
    /// [`surface_of`] the profile as saved.
    pub surface_digest: String,
    /// The literal stored `body_json`, with no secret reference expanded.
    pub body: String,
    pub updated_at: i64,
    pub captured_at: i64,
}

const COLUMNS: &str =
    "profile_id, model, prior_generation_id, prior_revision, inventory_revision, \
     profile_digest, surface_digest, body_json, updated_at, captured_at";

fn target_of(row: &cairn_db::turso::Row) -> DbResult<Target> {
    Ok(Target {
        profile_id: row.text(0)?,
        model: row.text(1)?,
        prior_generation: row.text(2)?,
        prior_revision: row.i64(3)?,
        inventory_revision: row.i64(4)?,
        profile_digest: row.text(5)?,
        surface_digest: row.text(6)?,
        body: row.text(7)?,
        updated_at: row.i64(8)?,
        captured_at: row.i64(9)?,
    })
}

fn refused(why: String) -> DbError {
    DbError::Row(why)
}

/// Capture `profile_id` for a generation regenerating it for `model`, or say
/// why it may not be regenerated. Takes the inventory lock itself.
pub async fn capture(db: &LocalDb, model: &str, profile_id: &str) -> Result<Target, String> {
    let _held = INVENTORY.lock().await;
    let inventory = load_inventory(db).await?;
    let profile = inventory
        .profiles
        .get(&InferenceProfileId::from(profile_id))
        .ok_or_else(|| format!("profile {profile_id} is not in the inventory"))?;
    if profile.model.name != model {
        return Err(format!(
            "profile {profile_id} serves {}, not {model}",
            profile.model.name
        ));
    }
    if let Some(why) = Gate::load(db).await?.replacement_refusal(profile) {
        return Err(format!("profile {profile_id} cannot be regenerated: {why}"));
    }
    let (model, id) = (model.to_string(), profile_id.to_string());
    db.read(move |conn| {
        let (model, id) = (model.clone(), id.clone());
        Box::pin(async move {
            let target = saved_in(conn, &model, &id).await?;
            match holds_in(conn, &target, None).await? {
                Some(why) => Err(refused(why)),
                None => Ok(target),
            }
        })
    })
    .await
    .map_err(|error| format!("profile {profile_id} cannot be regenerated: {error}"))
}

/// The profile as its inventory and ownership rows hold it now.
async fn saved_in(conn: &TrackedConnection, model: &str, id: &str) -> DbResult<Target> {
    let mut rows = conn
        .query(
            "SELECT body_json, updated_at FROM inference_profiles WHERE id = ?1",
            (id,),
        )
        .await?;
    let (body, updated_at) = rows
        .next()
        .await?
        .map(|row| Ok::<_, DbError>((row.text(0)?, row.i64(1)?)))
        .transpose()?
        .ok_or_else(|| refused(format!("profile {id} is not in the inventory")))?;
    drop(rows);
    let profile: InferenceProfile = serde_json::from_str(&body)
        .map_err(|error| refused(format!("profile {id} does not parse: {error}")))?;
    let mut rows = conn
        .query(
            "SELECT generation_id, revision, inventory_revision
               FROM inference_generated_profiles WHERE profile_id = ?1",
            (id,),
        )
        .await?;
    let (prior_generation, prior_revision, inventory_revision) = rows
        .next()
        .await?
        .map(|row| Ok::<_, DbError>((row.text(0)?, row.i64(1)?, row.i64(2)?)))
        .transpose()?
        .ok_or_else(|| refused(format!("no generation owns profile {id}")))?;
    Ok(Target {
        profile_id: id.to_string(),
        model: model.to_string(),
        prior_generation,
        prior_revision,
        inventory_revision,
        profile_digest: digest(&profile),
        surface_digest: surface_of(&profile),
        body,
        updated_at,
        captured_at: now_ms(),
    })
}

/// Why `target` no longer holds as captured, for `holder` (the generation
/// relying on the capture, if it exists yet); `None` while it holds.
async fn holds_in(
    conn: &TrackedConnection,
    target: &Target,
    holder: Option<&str>,
) -> DbResult<Option<String>> {
    let id = target.profile_id.as_str();
    let holder = holder.unwrap_or("");
    let mut rows = conn
        .query(
            "SELECT body_json, updated_at FROM inference_profiles WHERE id = ?1",
            (id,),
        )
        .await?;
    let saved = rows
        .next()
        .await?
        .map(|row| Ok::<_, DbError>((row.text(0)?, row.i64(1)?)))
        .transpose()?;
    drop(rows);
    match saved {
        None => return Ok(Some(format!("profile {id} is no longer in the inventory"))),
        Some((body, updated_at)) if body != target.body || updated_at != target.updated_at => {
            return Ok(Some(format!(
                "profile {id} was saved again after it was captured for regeneration"
            )));
        }
        Some(_) => {}
    }
    let mut rows = conn
        .query(
            "SELECT generation_id, revision, inventory_revision, promoted_verification_id
               FROM inference_generated_profiles WHERE profile_id = ?1",
            (id,),
        )
        .await?;
    let owned = rows
        .next()
        .await?
        .map(|row| Ok::<_, DbError>((row.text(0)?, row.i64(1)?, row.i64(2)?, row.opt_text(3)?)))
        .transpose()?;
    drop(rows);
    let Some((generation, revision, inventory_revision, promoted)) = owned else {
        return Ok(Some(format!("no generation owns profile {id}")));
    };
    if (generation.as_str(), revision, inventory_revision)
        != (
            target.prior_generation.as_str(),
            target.prior_revision,
            target.inventory_revision,
        )
    {
        return Ok(Some(format!(
            "profile {id} was written again after it was captured for regeneration"
        )));
    }
    if let Some(verification) = promoted {
        return Ok(Some(format!(
            "profile {id} was promoted by verification {verification} and needs no regeneration"
        )));
    }
    // The owner must be a recorded generation that has ended and whose agent
    // the saved profile (the captured body, which matched above) names.
    let mut rows = conn
        .query(
            &format!(
                "SELECT {} FROM inference_generations WHERE id = ?1",
                super::COLUMNS
            ),
            (target.prior_generation.as_str(),),
        )
        .await?;
    let owner = rows
        .next()
        .await?
        .map(|row| super::generation_of(&row))
        .transpose()?;
    drop(rows);
    let Some(owner) = owner else {
        return Ok(Some(format!(
            "generation {} that owns profile {id} is not recorded",
            target.prior_generation
        )));
    };
    let saved: InferenceProfile = serde_json::from_str(&target.body)
        .map_err(|error| refused(format!("profile {id} does not parse: {error}")))?;
    if let Some(why) = owner_refusal(&owner, &saved.origin, &target.model, id) {
        return Ok(Some(why));
    }
    if saved.model.name != target.model {
        return Ok(Some(format!(
            "profile {id} serves {}, not {}",
            saved.model.name, target.model
        )));
    }
    let mut rows = conn
        .query(
            "SELECT id, state, cleanup_pending FROM inference_generations
              WHERE (id = ?1 OR draft_profile = ?2) AND id != ?3
                AND (state IN (?4, ?5) OR cleanup_pending = 1)
              LIMIT 1",
            params![
                target.prior_generation.as_str(),
                id,
                holder,
                RUNNING,
                STARTING
            ],
        )
        .await?;
    let working = rows
        .next()
        .await?
        .map(|row| Ok::<_, DbError>((row.text(0)?, row.text(1)?, row.i64(2)? != 0)))
        .transpose()?;
    drop(rows);
    if let Some((generation, state, pending)) = working {
        return Ok(Some(if state == RUNNING || state == STARTING {
            format!("generation {generation} is still working on profile {id}")
        } else {
            debug_assert!(pending);
            format!("generation {generation}'s cleanup of profile {id} is still pending")
        }));
    }
    let mut rows = conn
        .query(
            "SELECT id FROM inference_verifications
              WHERE profile_id = ?1 AND outcome = 'running' LIMIT 1",
            (id,),
        )
        .await?;
    let running = rows.next().await?.map(|row| row.text(0)).transpose()?;
    drop(rows);
    if let Some(verification) = running {
        return Ok(Some(format!(
            "verification {verification} of profile {id} is still running"
        )));
    }
    let mut rows = conn
        .query(
            "SELECT r.generation_id FROM inference_generation_replacements r
               JOIN inference_generations g ON g.id = r.generation_id
              WHERE r.profile_id = ?1 AND r.generation_id != ?2 AND g.state IN (?3, ?4)
              LIMIT 1",
            params![id, holder, RUNNING, STARTING],
        )
        .await?;
    let rival = rows.next().await?.map(|row| row.text(0)).transpose()?;
    Ok(rival
        .map(|generation| format!("generation {generation} is already regenerating profile {id}")))
}

async fn load_in(conn: &TrackedConnection, generation_id: &str) -> DbResult<Option<Target>> {
    let mut rows = conn
        .query(
            &format!(
                "SELECT {COLUMNS} FROM inference_generation_replacements WHERE generation_id = ?1"
            ),
            (generation_id,),
        )
        .await?;
    rows.next().await?.map(|row| target_of(&row)).transpose()
}

/// Record `target` as what `generation_id` regenerates, in the caller's
/// transaction, after the generation's row is written and before its agent
/// runs. Refuses when the profile no longer holds as captured, or the
/// generation already has a capture: it is never taken again.
pub async fn record_in(
    conn: &TrackedConnection,
    generation_id: &str,
    target: &Target,
) -> DbResult<()> {
    let mut rows = conn
        .query(
            "SELECT state, mode, model FROM inference_generations WHERE id = ?1",
            (generation_id,),
        )
        .await?;
    let generation = rows
        .next()
        .await?
        .map(|row| Ok::<_, DbError>((row.text(0)?, row.text(1)?, row.text(2)?)))
        .transpose()?;
    drop(rows);
    let Some((state, mode, model)) = generation else {
        return Err(refused(format!(
            "generation {generation_id} is not recorded"
        )));
    };
    if (state != RUNNING && state != STARTING) || mode != AGENT_MODE {
        return Err(refused(format!(
            "generation {generation_id} is {state}, not an agent generation starting"
        )));
    }
    if model != target.model {
        return Err(refusal_of_model(generation_id, &model, target));
    }
    if load_in(conn, generation_id).await?.is_some() {
        return Err(refused(format!(
            "generation {generation_id} already captured the profile it regenerates; a capture is never taken again"
        )));
    }
    if let Some(why) = holds_in(conn, target, Some(generation_id)).await? {
        return Err(refused(why));
    }
    conn.execute(
        &format!(
            "INSERT INTO inference_generation_replacements (generation_id, {COLUMNS})
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)"
        ),
        params![
            generation_id,
            target.profile_id.as_str(),
            target.model.as_str(),
            target.prior_generation.as_str(),
            target.prior_revision,
            target.inventory_revision,
            target.profile_digest.as_str(),
            target.surface_digest.as_str(),
            target.body.as_str(),
            target.updated_at,
            target.captured_at
        ],
    )
    .await?;
    Ok(())
}

fn refusal_of_model(generation_id: &str, model: &str, target: &Target) -> DbError {
    refused(format!(
        "generation {generation_id} is for {model}; profile {} serves {}",
        target.profile_id, target.model
    ))
}

/// What `generation_id` regenerates, as it was captured at launch.
pub async fn load(db: &LocalDb, generation_id: &str) -> Result<Option<Target>, String> {
    let generation_id = generation_id.to_string();
    db.read(move |conn| {
        let generation_id = generation_id.clone();
        Box::pin(async move { load_in(conn, &generation_id).await })
    })
    .await
    .map_err(|error| format!("read inference_generation_replacements: {error}"))
}

/// Whether `generation_id`'s accepted draft of `profile_id` may take that
/// profile from its old owner, checked in the transaction that accepts the
/// draft, before anything is written. `Ok(true)` is the generation's first
/// accepted draft of the profile it captured, while that profile is still
/// exactly as captured. `Ok(false)` is an ordinary write: the generation
/// regenerates nothing, or already owns its target. Anything else refuses.
pub async fn validate_in(
    conn: &TrackedConnection,
    generation_id: &str,
    profile_id: &str,
) -> DbResult<bool> {
    let Some(target) = load_in(conn, generation_id).await? else {
        return Ok(false);
    };
    let id = target.profile_id.as_str();
    if id != profile_id {
        return Err(refused(format!(
            "generation {generation_id} regenerates profile {id}; its draft keeps that id, not {profile_id}"
        )));
    }
    let mut rows = conn
        .query(
            "SELECT state, draft_profile FROM inference_generations WHERE id = ?1",
            (generation_id,),
        )
        .await?;
    let generation = rows
        .next()
        .await?
        .map(|row| Ok::<_, DbError>((row.text(0)?, row.opt_text(1)?)))
        .transpose()?;
    drop(rows);
    let Some((state, draft)) = generation.filter(|(state, _)| state == RUNNING) else {
        return Err(refused(format!(
            "generation {generation_id} is not running"
        )));
    };
    debug_assert_eq!(state, RUNNING);
    match draft {
        None => match holds_in(conn, &target, Some(generation_id)).await? {
            None => Ok(true),
            Some(why) => Err(refused(format!(
                "{why}, so this generation no longer replaces it; the operator regenerates it again from its saved state"
            ))),
        },
        Some(draft) if draft != id => Err(refused(format!(
            "generation {generation_id} regenerates profile {id}, and its draft is {draft}"
        ))),
        Some(_) => {
            let mut rows = conn
                .query(
                    "SELECT generation_id FROM inference_generated_profiles WHERE profile_id = ?1",
                    (id,),
                )
                .await?;
            let owner = rows.next().await?.map(|row| row.text(0)).transpose()?;
            if owner.as_deref() == Some(generation_id) {
                Ok(false)
            } else {
                Err(refused(format!(
                    "profile {id} no longer belongs to generation {generation_id}"
                )))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::orchestrator::inference_generate::{insert_generation, GenerationRow};

    const MODEL: &str = "Qwen/Qwen3-Embedding-4B";
    const SHA: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    /// A generated container profile whose environment names a secret by
    /// reference, which a capture keeps literally.
    fn profile(id: &str, ctx: &str) -> InferenceProfile {
        serde_yaml::from_str(&format!(
            r#"
id: {id}
model:
  name: {MODEL}
  weights:
    - {{repo: Qwen/Qwen3-Embedding-4B-GGUF, revision: abc, path: Qwen3-Embedding-4B-Q8_0.gguf, sha256: {SHA}}}
servedName: {MODEL}
origin: {{kind: generated, run: cairn://p/prj/9/1/old, modelRevision: abc}}
engine: {{kind: container, image: "ghcr.io/ggml-org/llama.cpp@sha256:{SHA}", server: llamaCpp, args: [-m, "{{weights}}", --ctx-size, "{ctx}"], env: {{HF_TOKEN: "${{secret:hf}}"}}}}
requirements:
  machines: 1
  memory: {{kind: discrete, vram: 8000000000, host: 0}}
"#
        ))
        .unwrap()
    }

    fn generation(id: &str, state: &str) -> GenerationRow {
        GenerationRow {
            id: id.into(),
            project_id: "p".into(),
            issue_id: None,
            job_id: format!("job-{id}"),
            mode: AGENT_MODE.into(),
            execution_id: Some(format!("exec-{id}")),
            attempts: 0,
            draft_revision: 0,
            draft_digest: None,
            approval_prompt_id: None,
            approval_run_id: None,
            approval_revision: None,
            verification_id: None,
            cleanup_pending: false,
            node_uri: format!("cairn://p/prj/9/1/{id}"),
            model: MODEL.into(),
            pool: "bbs".into(),
            state: state.into(),
            draft_profile: None,
            draft_written_at: None,
            approved_surface: None,
            hardware: BTreeMap::new(),
            outcome: None,
            started_at: 1,
            finished_at: None,
        }
    }

    /// Profile `a`, written by generation `old` (ended) and then edited by the
    /// operator, so nothing promotes it and nothing works on it.
    async fn edited(name: &str) -> LocalDb {
        let db = crate::storage::migrated_test_db(name).await;
        let mut old = generation("old", "verified");
        old.draft_profile = Some("a".into());
        old.draft_revision = 1;
        insert_generation(&db, &old).await.unwrap();
        let written = profile("a", "2048");
        db.execute(
            "INSERT INTO inference_generated_profiles
               (profile_id, generation_id, revision, profile_digest, surface_digest,
                promoted_verification_id, active, inventory_revision)
             VALUES ('a', 'old', 1, ?1, ?2, NULL, 1, 2)",
            params![digest(&written), surface_of(&written)],
        )
        .await
        .unwrap();
        save(&db, &profile("a", "4096"), 20).await;
        db
    }

    /// Store `profile`'s row as an inventory write does.
    async fn save(db: &LocalDb, profile: &InferenceProfile, at: i64) {
        db.execute(
            "INSERT INTO inference_profiles (id, source, body_json, created_at, updated_at)
             VALUES (?1, 'cairn', ?2, ?3, ?3)
             ON CONFLICT(id) DO UPDATE SET body_json = excluded.body_json,
               updated_at = excluded.updated_at",
            params![
                profile.id.as_str(),
                serde_json::to_string(profile).unwrap(),
                at
            ],
        )
        .await
        .unwrap();
    }

    async fn record(db: &LocalDb, generation_id: &str, target: &Target) -> Result<(), String> {
        let (generation_id, target) = (generation_id.to_string(), target.clone());
        db.write(move |conn| {
            let (generation_id, target) = (generation_id.clone(), target.clone());
            Box::pin(async move { record_in(conn, &generation_id, &target).await })
        })
        .await
        .map_err(|error| error.to_string())
    }

    async fn validate(db: &LocalDb, generation_id: &str, profile_id: &str) -> Result<bool, String> {
        let (generation_id, profile_id) = (generation_id.to_string(), profile_id.to_string());
        db.write(move |conn| {
            let (generation_id, profile_id) = (generation_id.clone(), profile_id.clone());
            Box::pin(async move { validate_in(conn, &generation_id, &profile_id).await })
        })
        .await
        .map_err(|error| error.to_string())
    }

    /// `db` with generation `id` started and its capture of `a` recorded.
    async fn recorded(db: &LocalDb, id: &str, state: &str) -> Target {
        let target = capture(db, MODEL, "a").await.unwrap();
        insert_generation(db, &generation(id, state)).await.unwrap();
        record(db, id, &target).await.unwrap();
        target
    }

    #[tokio::test]
    async fn a_capture_keeps_the_saved_profile_literally_and_is_never_taken_again() {
        let db = edited("replacement-capture-literal").await;
        let stored: String = db
            .query_opt(
                "SELECT body_json FROM inference_profiles WHERE id = 'a'",
                (),
                |row| row.text(0),
            )
            .await
            .unwrap()
            .unwrap();
        let target = recorded(&db, "new", STARTING).await;
        let saved = profile("a", "4096");
        assert_eq!(target.body, stored);
        assert!(target.body.contains("${secret:hf}"), "{}", target.body);
        assert_eq!(
            (
                target.prior_generation.as_str(),
                target.prior_revision,
                target.inventory_revision,
                target.updated_at
            ),
            ("old", 1, 2, 20)
        );
        assert_eq!(target.profile_digest, digest(&saved));
        assert_eq!(target.surface_digest, surface_of(&saved));
        assert_eq!(load(&db, "new").await.unwrap(), Some(target.clone()));
        assert_eq!(load(&db, "other").await.unwrap(), None);

        let again = record(&db, "new", &target).await.unwrap_err();
        assert!(again.contains("never taken again"), "{again}");
    }

    #[tokio::test]
    async fn only_an_owned_unpromoted_profile_of_the_model_that_nothing_works_on_is_captured() {
        let db = edited("replacement-capture-refusals").await;
        let wrong = capture(&db, "Qwen/Other", "a").await.unwrap_err();
        assert!(wrong.contains("not Qwen/Other"), "{wrong}");
        let missing = capture(&db, MODEL, "b").await.unwrap_err();
        assert!(missing.contains("not in the inventory"), "{missing}");

        // A profile that only says it was generated, with no ownership row.
        save(&db, &profile("b", "4096"), 20).await;
        let unowned = capture(&db, MODEL, "b").await.unwrap_err();
        assert!(unowned.contains("no generation owns"), "{unowned}");

        db.execute(
            "UPDATE inference_generations SET state = 'running' WHERE id = 'old'",
            (),
        )
        .await
        .unwrap();
        let live = capture(&db, MODEL, "a").await.unwrap_err();
        assert!(live.contains("still working"), "{live}");
        db.execute(
            "UPDATE inference_generations SET state = 'failed', cleanup_pending = 1 WHERE id = 'old'",
            (),
        )
        .await
        .unwrap();
        let cleaning = capture(&db, MODEL, "a").await.unwrap_err();
        assert!(cleaning.contains("cleanup"), "{cleaning}");
        db.execute(
            "UPDATE inference_generations SET cleanup_pending = 0 WHERE id = 'old'",
            (),
        )
        .await
        .unwrap();

        let saved = profile("a", "4096");
        db.execute(
            "INSERT INTO inference_verifications
               (id, profile_id, profile_digest, pool_id, deployment_id, machines_json,
                requested_by, started_at, outcome, stage)
             VALUES ('v', 'a', ?1, 'bbs', 'd', '[]', 'operator', 1, 'running', 'load')",
            (digest(&saved),),
        )
        .await
        .unwrap();
        let verifying = capture(&db, MODEL, "a").await.unwrap_err();
        assert!(verifying.contains("still running"), "{verifying}");

        // A pass of the profile as it stands promotes it: nothing to recover.
        db.execute(
            "UPDATE inference_verifications SET outcome = 'passed' WHERE id = 'v'",
            (),
        )
        .await
        .unwrap();
        db.execute(
            "UPDATE inference_generated_profiles SET promoted_verification_id = 'v',
               profile_digest = ?1, surface_digest = ?2 WHERE profile_id = 'a'",
            params![digest(&saved), surface_of(&saved)],
        )
        .await
        .unwrap();
        let promoted = capture(&db, MODEL, "a").await.unwrap_err();
        assert!(promoted.contains("promoted"), "{promoted}");
    }

    #[tokio::test]
    async fn a_profile_changed_after_capture_is_not_recorded_even_when_its_body_is_restored() {
        let db = edited("replacement-capture-stale").await;
        let target = capture(&db, MODEL, "a").await.unwrap();
        insert_generation(&db, &generation("new", STARTING))
            .await
            .unwrap();

        save(&db, &profile("a", "8192"), 30).await;
        let edited = record(&db, "new", &target).await.unwrap_err();
        assert!(edited.contains("saved again"), "{edited}");

        // Edit and undo inside one millisecond: only the revision moved.
        save(&db, &profile("a", "4096"), 20).await;
        db.execute(
            "UPDATE inference_generated_profiles SET inventory_revision = inventory_revision + 2
              WHERE profile_id = 'a'",
            (),
        )
        .await
        .unwrap();
        let undone = record(&db, "new", &target).await.unwrap_err();
        assert!(undone.contains("written again"), "{undone}");
        assert_eq!(load(&db, "new").await.unwrap(), None);
    }

    #[tokio::test]
    async fn concurrent_regenerations_of_one_profile_cannot_both_capture_it() {
        let db = edited("replacement-capture-concurrent").await;
        let first = capture(&db, MODEL, "a").await.unwrap();
        let second = capture(&db, MODEL, "a").await.unwrap();
        for id in ["one", "two"] {
            insert_generation(&db, &generation(id, STARTING))
                .await
                .unwrap();
        }
        record(&db, "one", &first).await.unwrap();
        let raced = record(&db, "two", &second).await.unwrap_err();
        assert!(raced.contains("already regenerating"), "{raced}");
        let later = capture(&db, MODEL, "a").await.unwrap_err();
        assert!(later.contains("already regenerating"), "{later}");
        let accept = validate(&db, "two", "a").await.unwrap();
        assert!(!accept, "a generation with no capture regenerates nothing");

        // Once the first ends without accepting a draft, the profile is free.
        db.execute(
            "UPDATE inference_generations SET state = 'failed' WHERE id = 'one'",
            (),
        )
        .await
        .unwrap();
        capture(&db, MODEL, "a").await.unwrap();
    }

    #[tokio::test]
    async fn a_missing_owner_or_an_operator_or_imported_write_over_the_id_is_not_a_target() {
        let db = edited("replacement-owner-and-origin").await;
        let target = capture(&db, MODEL, "a").await.unwrap();
        insert_generation(&db, &generation("new", STARTING))
            .await
            .unwrap();

        // The transaction fails closed when the owner's row is gone, even
        // though nothing else about the profile moved.
        db.execute("DELETE FROM inference_generations WHERE id = 'old'", ())
            .await
            .unwrap();
        let orphaned = record(&db, "new", &target).await.unwrap_err();
        assert!(orphaned.contains("not recorded"), "{orphaned}");
        let refused = capture(&db, MODEL, "a").await.unwrap_err();
        assert!(refused.contains("not recorded"), "{refused}");
        let mut old = generation("old", "verified");
        old.draft_profile = Some("a".into());
        old.draft_revision = 1;
        insert_generation(&db, &old).await.unwrap();

        // The ownership row outlives a profile the operator writes under the
        // same id with its own origin, or imports there; neither is a target.
        let mut authored = profile("a", "4096");
        authored.origin = cairn_inference::ProfileOrigin::Authored;
        save(&db, &authored, 30).await;
        let own = capture(&db, MODEL, "a").await.unwrap_err();
        assert!(own.contains("operator's own"), "{own}");
        let mut imported = profile("a", "4096");
        imported.origin = cairn_inference::ProfileOrigin::Imported {
            url: "https://github.com/o/r".into(),
            commit: "c".into(),
            compose: "compose.yaml".into(),
            service: "llm".into(),
        };
        save(&db, &imported, 31).await;
        assert!(capture(&db, MODEL, "a").await.is_err());

        // A generated stamp naming another agent is not the owner's.
        let mut stamped = profile("a", "4096");
        if let cairn_inference::ProfileOrigin::Generated { run, .. } = &mut stamped.origin {
            *run = Some("cairn://p/prj/9/1/elsewhere".into());
        }
        save(&db, &stamped, 32).await;
        let forged = capture(&db, MODEL, "a").await.unwrap_err();
        assert!(forged.contains("not generation old's agent"), "{forged}");

        save(&db, &profile("a", "4096"), 33).await;
        let restored = capture(&db, MODEL, "a").await.unwrap();
        record(&db, "new", &restored).await.unwrap();
    }

    #[tokio::test]
    async fn a_capture_is_recorded_only_for_a_starting_agent_generation_of_its_model() {
        let db = edited("replacement-record-generation").await;
        let target = capture(&db, MODEL, "a").await.unwrap();
        let absent = record(&db, "ghost", &target).await.unwrap_err();
        assert!(absent.contains("not recorded"), "{absent}");
        insert_generation(&db, &generation("ended", "cancelled"))
            .await
            .unwrap();
        assert!(record(&db, "ended", &target).await.is_err());
        let mut legacy = generation("legacy", STARTING);
        legacy.mode = "workflow".into();
        insert_generation(&db, &legacy).await.unwrap();
        assert!(record(&db, "legacy", &target).await.is_err());
        let mut other = generation("other", STARTING);
        other.model = "Qwen/Other".into();
        insert_generation(&db, &other).await.unwrap();
        let model = record(&db, "other", &target).await.unwrap_err();
        assert!(model.contains("is for Qwen/Other"), "{model}");
        for id in ["ghost", "ended", "legacy", "other"] {
            assert_eq!(load(&db, id).await.unwrap(), None);
        }
    }

    #[tokio::test]
    async fn only_the_first_accepted_draft_of_the_exact_unchanged_target_takes_it() {
        let db = edited("replacement-validate").await;
        insert_generation(&db, &generation("plain", RUNNING))
            .await
            .unwrap();
        assert!(!validate(&db, "plain", "a").await.unwrap());

        recorded(&db, "new", STARTING).await;
        let starting = validate(&db, "new", "a").await.unwrap_err();
        assert!(starting.contains("not running"), "{starting}");
        db.execute(
            "UPDATE inference_generations SET state = 'running' WHERE id = 'new'",
            (),
        )
        .await
        .unwrap();
        let other = validate(&db, "new", "b").await.unwrap_err();
        assert!(other.contains("keeps that id"), "{other}");
        assert!(validate(&db, "new", "a").await.unwrap());

        // Another open generation drafting the target stops the transfer.
        db.execute(
            "UPDATE inference_generations SET draft_profile = 'a' WHERE id = 'plain'",
            (),
        )
        .await
        .unwrap();
        let drafted = validate(&db, "new", "a").await.unwrap_err();
        assert!(drafted.contains("still working"), "{drafted}");
        db.execute(
            "UPDATE inference_generations SET draft_profile = NULL WHERE id = 'plain'",
            (),
        )
        .await
        .unwrap();

        // An operator edit after launch is never silently recaptured.
        save(&db, &profile("a", "8192"), 30).await;
        let stale = validate(&db, "new", "a").await.unwrap_err();
        assert!(stale.contains("saved again"), "{stale}");
        save(&db, &profile("a", "4096"), 20).await;
        assert!(validate(&db, "new", "a").await.unwrap());

        // After the parent's accepted write transfers ownership, later
        // revisions are the generation's own writes.
        db.execute(
            "UPDATE inference_generations SET draft_profile = 'a', draft_revision = 1 WHERE id = 'new'",
            (),
        )
        .await
        .unwrap();
        let unowned = validate(&db, "new", "a").await.unwrap_err();
        assert!(unowned.contains("no longer belongs"), "{unowned}");
        db.execute(
            "UPDATE inference_generated_profiles SET generation_id = 'new', revision = 1
              WHERE profile_id = 'a'",
            (),
        )
        .await
        .unwrap();
        assert!(!validate(&db, "new", "a").await.unwrap());
        let renamed = validate(&db, "new", "b").await.unwrap_err();
        assert!(renamed.contains("keeps that id"), "{renamed}");

        db.execute(
            "UPDATE inference_generations SET state = 'failed' WHERE id = 'new'",
            (),
        )
        .await
        .unwrap();
        let ended = validate(&db, "new", "a").await.unwrap_err();
        assert!(ended.contains("not running"), "{ended}");
    }
}
