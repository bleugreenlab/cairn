//! The operator-only tenet policy on a project: what a tenet gate releases on,
//! and which recipes a thread may only file behind a tenet.
//!
//! Both live on the `projects` row and deliberately NOT in
//! `.cairn/config.yaml`: a branch can edit its own config, and a gate a branch
//! could edit is a gate the work it holds could lift. No agent resource
//! mutation reaches them; the operator's settings path is the only writer.

use serde::Serialize;

use crate::services::EventEmitter;
use crate::storage::{LocalDb, RowExt};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TenetRelease {
    /// Any recorded verdict releases, including a desktop answer no
    /// authenticator verified.
    Answered,
    /// Only a verdict confirmed through a verified authenticator releases.
    Ratified,
}

impl TenetRelease {
    pub fn as_str(self) -> &'static str {
        match self {
            TenetRelease::Answered => "answered",
            TenetRelease::Ratified => "ratified",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "answered" => Some(TenetRelease::Answered),
            "ratified" => Some(TenetRelease::Ratified),
            _ => None,
        }
    }
}

/// Why `ratified` cannot be chosen yet.
pub const RATIFIED_REFUSAL: &str = "no verified authenticator path exists yet";

/// The refusal for an agent resource mutation naming either policy column.
pub const OPERATOR_ONLY_POLICY_REFUSAL: &str = "the tenet release policy and the tenet-gated recipe list are operator-only project settings: they cannot be changed through an agent resource mutation, and they are kept out of .cairn/config.yaml so no branch can ungate itself";

const OPERATOR_ONLY_KEYS: &[&str] = &[
    "tenetRelease",
    "tenet_release",
    "tenetGatedRecipes",
    "tenet_gated_recipes",
];

/// [`OPERATOR_ONLY_POLICY_REFUSAL`] when `payload` names a policy column at its
/// top level, in either spelling.
pub fn operator_only_policy_refusal(payload: Option<&serde_json::Value>) -> Option<&'static str> {
    let map = payload?.as_object()?;
    OPERATOR_ONLY_KEYS
        .iter()
        .any(|key| map.contains_key(*key))
        .then_some(OPERATOR_ONLY_POLICY_REFUSAL)
}

/// The one sentence every surface prints for a release policy.
pub fn release_label(release: TenetRelease) -> String {
    match release {
        TenetRelease::Answered => "Releases on: answered (unverified desktop answer)".to_string(),
        TenetRelease::Ratified => "Releases on: ratified (verified authenticator)".to_string(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TenetPolicy {
    pub release: TenetRelease,
    pub gated_recipes: Vec<String>,
}

impl TenetPolicy {
    pub fn label(&self) -> String {
        release_label(self.release)
    }

    /// Whether a thread filing `recipe` must name or propose a tenet. Recipe
    /// ids are compared case-insensitively.
    pub fn gates_recipe(&self, recipe: &str) -> bool {
        self.gated_recipes
            .iter()
            .any(|gated| gated.eq_ignore_ascii_case(recipe))
    }
}

/// A project's tenet policy.
pub async fn load_policy(db: &LocalDb, project_id: &str) -> Result<TenetPolicy, String> {
    let (release, recipes) = db
        .query_opt(
            "SELECT tenet_release, tenet_gated_recipes FROM projects WHERE id = ?1",
            (project_id.to_string(),),
            |row| Ok((row.text(0)?, row.text(1)?)),
        )
        .await
        .map_err(|error| format!("Failed to load tenet policy: {error}"))?
        .ok_or_else(|| format!("project not found: {project_id}"))?;
    Ok(TenetPolicy {
        release: TenetRelease::parse(&release)
            .ok_or_else(|| format!("unknown tenet release policy: {release}"))?,
        gated_recipes: serde_json::from_str(&recipes)
            .map_err(|error| format!("invalid tenet_gated_recipes: {error}"))?,
    })
}

fn emit_project_changed(emitter: &dyn EventEmitter) {
    if let Err(error) = emitter.emit(
        "db-change",
        serde_json::json!({"table": "projects", "action": "update"}),
    ) {
        tracing::warn!(%error, "failed to emit projects db-change notification");
    }
}

/// Set what a project's tenet gates release on. Operator settings only.
pub async fn set_release(
    db: &LocalDb,
    emitter: &dyn EventEmitter,
    project_id: &str,
    release: &str,
) -> Result<TenetRelease, String> {
    let release = match TenetRelease::parse(release.trim()) {
        Some(TenetRelease::Answered) => TenetRelease::Answered,
        Some(TenetRelease::Ratified) => {
            return Err(format!(
                "tenet release `ratified` is refused: {RATIFIED_REFUSAL}"
            ))
        }
        None => {
            return Err(format!(
                "unknown tenet release policy `{release}`: expected `answered`"
            ))
        }
    };
    db.execute(
        "UPDATE projects SET tenet_release = ?1 WHERE id = ?2",
        (release.as_str().to_string(), project_id.to_string()),
    )
    .await
    .map_err(|error| format!("Failed to set tenet release policy: {error}"))?;
    emit_project_changed(emitter);
    Ok(release)
}

/// Set which recipes a thread may only file behind a tenet. Operator settings
/// only.
pub async fn set_gated_recipes(
    db: &LocalDb,
    emitter: &dyn EventEmitter,
    project_id: &str,
    recipes: &[String],
) -> Result<Vec<String>, String> {
    let mut cleaned: Vec<String> = Vec::new();
    for recipe in recipes {
        let recipe = recipe.trim();
        if recipe.is_empty() {
            return Err("a gated recipe id must not be empty".to_string());
        }
        if !cleaned.iter().any(|seen| seen.eq_ignore_ascii_case(recipe)) {
            cleaned.push(recipe.to_string());
        }
    }
    let encoded = serde_json::to_string(&cleaned).map_err(|error| error.to_string())?;
    db.execute(
        "UPDATE projects SET tenet_gated_recipes = ?1 WHERE id = ?2",
        (encoded, project_id.to_string()),
    )
    .await
    .map_err(|error| format!("Failed to set tenet-gated recipes: {error}"))?;
    emit_project_changed(emitter);
    Ok(cleaned)
}
