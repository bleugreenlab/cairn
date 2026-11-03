//! The orchestrator's half of model selection: what [`select`] reads, gathered
//! from where it lives (settings, the project overlay, the catalog, the
//! credential store, the usage book), and where a choice is stored.
//!
//! Nothing here probes a provider. Usage comes from what the book already
//! holds and the credential store's recorded health, so a preview costs a
//! settings read and a catalog build, not a network round trip.

use std::collections::BTreeMap;

use cairn_config::allotment::{
    model_ref, parse_allotment, AllotmentEntry, Band, EffortPlacement, LevelSource, ModelsConfig,
    PoolKind,
};
use cairn_config::project_settings::{convert_project_tiers, load_project_settings_read_only};
use cairn_config::settings::{convert_settings_tiers, load_settings_file};
use cairn_db::storage::run_db_blocking;
use cairn_models::interface::{Catalog, ModelScope, Models};
use cairn_models::supply::SupplyInputs;
use cairn_models::types::catalog::VerbKind;
use cairn_models::types::usage::ProviderUsageWindow;
use serde::Serialize;

use super::Orchestrator;
use crate::credentials::RoutedProvider;
use crate::execution::model_selection::{
    select, ResolvedConstraints, SelectionContext, SelectionRecord, SelectionRefusal,
    SelectionSubject, TopOfAllotment,
};
use crate::models::{ModelSelection, PresetOptionValue};

/// The pools, bands and ladder as Settings → Models and
/// `cairn://models/ladder` show them.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LadderView {
    pub pools: Vec<LadderPool>,
    /// The band pools, lowest first.
    pub bands: Vec<Band>,
    /// Every ladder entry, highest level first; entries off the ladder
    /// (launch options without a level) last.
    pub rungs: Vec<LadderRung>,
    /// Served session models on installed providers with no level, as
    /// `provider/model`.
    pub unranked: Vec<String>,
    pub default_models: Vec<AllotmentEntry>,
    pub conversion_notes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LadderPool {
    pub id: String,
    pub description: String,
    /// The slice of the ladder a band pool holds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub band: Option<Band>,
    /// A hand-picked pool's members, as `provider/model`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub members: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LadderRung {
    /// `provider/model`.
    pub model: String,
    pub level: Option<f64>,
    pub level_source: Option<LevelSource>,
    /// What the level was derived from, in words.
    pub level_from: Option<String>,
    pub band: Option<String>,
    pub maker: Option<String>,
    /// Position on the model's effort scale, 0..1.
    pub effort: Option<f64>,
    /// The level the entry launches at: its position's, else its kept word's
    /// when that is one of the model's levels.
    pub effort_level: Option<String>,
    /// An effort word kept from the tier grid, placed on the model's scale
    /// at launch (the entry's `effortLevel`). Written back unchanged by an
    /// edit that does not set an effort.
    pub effort_word: Option<String>,
    /// The model's effort levels, lowest first, each at the position the
    /// facet gives it. Empty for a model that takes no effort.
    pub effort_scale: Vec<EffortStep>,
    pub options: BTreeMap<String, PresetOptionValue>,
    /// The hand-picked pools it is a member of.
    pub pools: Vec<String>,
}

/// One level of a model's effort scale and the 0..1 position that launches at
/// it, as the facet's `position()` answers: what an editor writes to choose it.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EffortStep {
    pub level: String,
    pub position: f64,
}

/// The ladder view of `models` against `catalog`.
pub fn ladder_view(models: &ModelsConfig, catalog: &Catalog) -> LadderView {
    let bands = models.bands();
    let provider = |key: &str| {
        catalog
            .providers
            .iter()
            .find(|provider| provider.descriptor.key.as_ref() == key)
    };
    let pools = models
        .pools
        .iter()
        .map(|pool| LadderPool {
            id: pool.id.clone(),
            description: pool.description.clone(),
            band: match pool.kind {
                PoolKind::Band { .. } => bands.iter().find(|band| band.id == pool.id).cloned(),
                PoolKind::Set { .. } => None,
            },
            members: match &pool.kind {
                PoolKind::Set { members } => Some(members.iter().map(model_ref).collect()),
                PoolKind::Band { .. } => None,
            },
        })
        .collect();
    let mut rungs: Vec<LadderRung> = models
        .ladder
        .iter()
        .map(|rung| {
            let model = rung.model.model.as_str();
            let on = provider(&rung.model.backend);
            let scale = on.and_then(|provider| provider.effort_scale(model));
            LadderRung {
                model: model_ref(&rung.model),
                level: rung.level.as_ref().map(|level| level.value),
                level_source: rung.level.as_ref().map(|level| level.source),
                level_from: rung.level.as_ref().and_then(|level| level.from.clone()),
                band: rung
                    .level
                    .as_ref()
                    .and_then(|level| models.band_of(level.value))
                    .map(|band| band.id),
                maker: on.and_then(|provider| provider.maker(model)),
                effort: rung.effort,
                effort_level: match (rung.effort, scale.as_ref()) {
                    (Some(x), Some(scale)) => Some(scale.at(x).to_string()),
                    (None, Some(scale)) => rung
                        .effort_level
                        .clone()
                        .filter(|word| scale.position(word).is_some()),
                    (_, None) => None,
                },
                effort_word: rung
                    .effort_level
                    .clone()
                    .filter(|_| rung.effort.is_none()),
                effort_scale: on
                    .zip(scale.as_ref())
                    .map(|(provider, scale)| {
                        scale
                            .levels
                            .iter()
                            .filter_map(|level| {
                                Some(EffortStep {
                                    level: level.clone(),
                                    position: provider.position(model, level)?,
                                })
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
                options: rung.options.clone(),
                pools: models
                    .pools
                    .iter()
                    .filter(|pool| {
                        matches!(&pool.kind, PoolKind::Set { members } if members.contains(&rung.model))
                    })
                    .map(|pool| pool.id.clone())
                    .collect(),
            }
        })
        .collect();
    rungs.sort_by(|left, right| match (left.level, right.level) {
        (Some(a), Some(b)) => b.total_cmp(&a),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    });
    let laddered = |key: &str, model: &str| {
        models.ladder.iter().any(|rung| {
            rung.level.is_some() && rung.model.backend == key && rung.model.model.as_str() == model
        })
    };
    let mut unranked: Vec<String> = Vec::new();
    for provider in catalog
        .providers
        .iter()
        .filter(|provider| provider.installed)
    {
        let key = provider.descriptor.key.as_ref();
        let Some(listed) = &provider.models else {
            continue;
        };
        for model in listed.models.iter().filter(|model| !model.hidden) {
            let served = provider.admit(&model.model, VerbKind::Session).is_ok();
            let entry = format!("{key}/{}", model.model);
            if served && !laddered(key, &model.model) && !unranked.contains(&entry) {
                unranked.push(entry);
            }
        }
    }
    LadderView {
        pools,
        bands,
        rungs,
        unranked,
        default_models: models.default_models.clone(),
        conversion_notes: models.conversion_notes.clone(),
    }
}

/// The kinds of definition that name an allotment of their own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum DefinitionKind {
    Agent,
    /// An agent node's override or a decision node's allotment.
    RecipeNode,
    Response,
}

/// A definition that names an allotment of its own, and what it names.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AllotmentReference {
    pub kind: DefinitionKind,
    /// How the definition is named where it is edited: an agent's or a
    /// response's name, a recipe node as `recipe › node`.
    pub name: String,
    /// The project whose config holds it; `None` for the workspace's.
    pub project_id: Option<String>,
    pub models: Vec<AllotmentEntry>,
}

/// The verb a selection preview names: `session` unless asked otherwise.
pub fn preview_verb(raw: Option<&str>) -> Result<VerbKind, String> {
    match raw.unwrap_or("session") {
        "session" => Ok(VerbKind::Session),
        "complete" => Ok(VerbKind::Complete),
        "decide" => Ok(VerbKind::Decide),
        other => Err(format!(
            "verb '{other}' does not select from an allotment; use session, complete, or decide"
        )),
    }
}

/// Where an effort word sits on its model's own scale, as `catalog` answers.
/// A provider the catalog does not list has no scale.
pub(crate) fn position_in(
    catalog: &Catalog,
    selection: &ModelSelection,
    level: &str,
) -> EffortPlacement {
    let scale = catalog
        .providers
        .iter()
        .find(|provider| provider.descriptor.key.as_ref() == selection.backend)
        .and_then(|provider| provider.effort_scale(selection.model.as_str()));
    match scale {
        None => EffortPlacement::ScaleUnknown,
        Some(scale) => scale
            .position(level)
            .map_or(EffortPlacement::NotALevel, EffortPlacement::At),
    }
}

impl Orchestrator {
    /// The pools and ladder a launch in `project_id` selects within: the
    /// workspace's `models`, with the project's overlay laid over it.
    ///
    /// A workspace or project whose tier grid has not been converted yet is
    /// converted here in memory, the same conversion the one-time migration
    /// writes, so a preview never reads an empty ladder.
    pub fn effective_models(&self, project_id: Option<&str>) -> Result<ModelsConfig, String> {
        let catalog = self.catalog(&ModelScope {
            project_id: project_id.map(str::to_string),
        })?;
        self.effective_models_in(project_id, &catalog)
    }

    fn effective_models_in(
        &self,
        project_id: Option<&str>,
        catalog: &Catalog,
    ) -> Result<ModelsConfig, String> {
        let settings = self.get_settings();
        let position =
            |selection: &ModelSelection, level: &str| position_in(catalog, selection, level);
        // Until the one-time conversion has written `models` (it runs at
        // startup), the file's tier grid is converted here the same way.
        let workspace_file = || load_settings_file(&self.config_dir).unwrap_or_default();
        let mut models = match &settings.models {
            Some(models) => models.clone(),
            None => {
                convert_settings_tiers(&workspace_file(), &settings.enabled_providers, &position)
                    .models
            }
        };
        if let Some(project_id) = project_id {
            let file = load_project_settings_read_only(&self.project_path(project_id)?);
            let overlay = match &file.models {
                Some(overlay) => Some(overlay.clone()),
                None => convert_project_tiers(
                    &file,
                    &cairn_config::settings::workspace_legacy_grid(&self.config_dir)?,
                    &settings.enabled_providers,
                    &position,
                )
                .map(|(overlay, _)| overlay),
            };
            if let Some(overlay) = overlay {
                models.overlay(&overlay);
            }
        }
        Ok(models)
    }

    /// The ladder view of the pools and ladder `project_id` selects within.
    pub fn models_ladder(&self, project_id: Option<&str>) -> Result<LadderView, String> {
        let catalog = self.catalog(&ModelScope {
            project_id: project_id.map(str::to_string),
        })?;
        let models = self.effective_models_in(project_id, &catalog)?;
        Ok(ladder_view(&models, &catalog))
    }

    /// Every agent, recipe node and response, in the workspace and in every
    /// project, that names an allotment of its own. Renaming or deleting a
    /// pool, or removing a provider, reads it to say which definitions will
    /// refuse until they are edited; nothing here rewrites them. A file that
    /// does not parse names nothing it can be held to.
    pub fn allotment_references(&self) -> Result<Vec<AllotmentReference>, String> {
        use crate::config::{responses, ConfigResult};
        let mut references = Vec::new();
        for agent in self.list_agent_configs(None, None)? {
            if !agent.models.is_empty() {
                references.push(AllotmentReference {
                    kind: DefinitionKind::Agent,
                    name: agent.name,
                    project_id: agent.project_id,
                    models: agent.models,
                });
            }
        }
        for recipe in self.list_recipes(None, None)? {
            for node in &recipe.nodes {
                let models = node
                    .agent_config
                    .as_ref()
                    .and_then(|config| config.models.as_ref())
                    .or_else(|| {
                        node.decision_config
                            .as_ref()
                            .and_then(|config| config.models.as_ref())
                    });
                if let Some(models) = models.filter(|models| !models.is_empty()) {
                    references.push(AllotmentReference {
                        kind: DefinitionKind::RecipeNode,
                        name: format!("{} › {}", recipe.name, node.name),
                        project_id: recipe.project_id.clone(),
                        models: models.clone(),
                    });
                }
            }
        }
        // A project's listing also returns the workspace's responses, so
        // each scope keeps only its own files.
        let mut scopes = vec![(None, None)];
        for (project_id, path) in self.all_project_paths()? {
            scopes.push((Some(project_id), Some(path)));
        }
        for (project_id, path) in scopes {
            for listed in responses::list_responses(&self.config_dir, path.as_deref())? {
                let ConfigResult::Ok(file) = listed else {
                    continue;
                };
                if file.is_project_scoped != project_id.is_some() {
                    continue;
                }
                if let Some(models) = file.definition.models.filter(|models| !models.is_empty()) {
                    references.push(AllotmentReference {
                        kind: DefinitionKind::Response,
                        name: file.definition.name,
                        project_id: project_id.clone(),
                        models,
                    });
                }
            }
        }
        Ok(references)
    }

    /// What `allotment` (the workspace default when `None`) would select for
    /// `verb` now. Nothing is stored and no constraint applies: a preview
    /// asks about one node in isolation.
    pub fn preview_selection(
        &self,
        project_id: Option<&str>,
        allotment: Option<&[String]>,
        verb: VerbKind,
    ) -> Result<SelectionRecord, String> {
        let allotment = match allotment {
            Some(raw) => parse_allotment(raw)?,
            None => self.effective_models(project_id)?.default_models,
        };
        self.select_models(
            project_id,
            &allotment,
            verb,
            &ResolvedConstraints::default(),
            SelectionSubject::default(),
        )
        .map_err(String::from)
    }

    /// Choose the model `allotment` runs `verb` on in `project_id`, now.
    ///
    /// Reads the effective pools and ladder, the catalog for the project's
    /// scope, the credential store's accounts with the same recent-assignment
    /// evidence session routing spreads by, and usage windows the book holds
    /// for providers the credential store does not route (OpenCode Go). The
    /// record carries `subject`; storing it is the caller's.
    pub fn select_models(
        &self,
        project_id: Option<&str>,
        allotment: &[AllotmentEntry],
        verb: VerbKind,
        constraints: &ResolvedConstraints,
        subject: SelectionSubject,
    ) -> Result<SelectionRecord, SelectionRefusal> {
        let refuse = |message: String| SelectionRefusal { message };
        let catalog = self
            .catalog(&ModelScope {
                project_id: project_id.map(str::to_string),
            })
            .map_err(refuse)?;
        let models = self
            .effective_models_in(project_id, &catalog)
            .map_err(refuse)?;
        let store = self.get_credential_store();
        let assignments = run_db_blocking({
            let db = self.db.local.clone();
            move || async move { crate::sessions::queries::account_assignments(&db).await }
        })
        .unwrap_or_default();
        let book = self.provider_usage_snapshots.all();
        let provider_windows = |key: &str| -> Option<(Vec<ProviderUsageWindow>, i64)> {
            if RoutedProvider::for_backend(key).is_some() {
                return None;
            }
            book.values()
                .filter(|snapshot| {
                    snapshot.backend == key
                        && snapshot.error.is_none()
                        && !snapshot.windows.is_empty()
                })
                .max_by_key(|snapshot| snapshot.captured_at)
                .map(|snapshot| (snapshot.windows.clone(), snapshot.captured_at))
        };
        let ctx = SelectionContext {
            models: &models,
            catalog: &catalog,
            supply: SupplyInputs {
                store: store.as_ref(),
                project_id,
                assignments: &assignments,
                provider_windows: &provider_windows,
                now: chrono::Utc::now().timestamp(),
            },
        };
        let mut record = select(&ctx, allotment, verb, constraints, &TopOfAllotment)?;
        record.subject = subject;
        Ok(record)
    }

    /// The session model a job outside any recipe node runs on: a call, a
    /// child task, or a spawn checking its task can launch. It selects within
    /// its own allotment, else its agent's, else the workspace default.
    pub(crate) fn select_for_agent(
        &self,
        project_id: &str,
        own: Option<&[AllotmentEntry]>,
        agent: &[AllotmentEntry],
        subject: SelectionSubject,
    ) -> Result<SelectionRecord, String> {
        let defaults = self.effective_models(Some(project_id))?.default_models;
        let allotment =
            crate::execution::model_selection::effective_allotment(own, agent, &defaults);
        self.select_models(
            Some(project_id),
            &allotment,
            VerbKind::Session,
            &ResolvedConstraints::default(),
            subject,
        )
        .map_err(String::from)
    }

    /// The record a job pinned to `pin` stores in place of a selection.
    pub(crate) fn pinned_selection(
        &self,
        project_id: Option<&str>,
        pin: &crate::models::ModelSelection,
        subject: SelectionSubject,
    ) -> Result<SelectionRecord, String> {
        let catalog = self.catalog(&ModelScope {
            project_id: project_id.map(str::to_string),
        })?;
        let models = self.effective_models_in(project_id, &catalog)?;
        let mut record = crate::execution::model_selection::pinned(&models, &catalog, pin);
        record.subject = subject;
        Ok(record)
    }

    /// The selection a recipe node's live attempt ran on, for a constraint
    /// that names it. `None` while that node has no stored selection.
    pub(crate) fn node_selection(
        &self,
        execution_id: &str,
        recipe_node_id: &str,
    ) -> Result<Option<SelectionRecord>, String> {
        let dbs = self.db.clone();
        let execution_id = execution_id.to_string();
        let recipe_node_id = recipe_node_id.to_string();
        let stored = run_db_blocking(move || async move {
            let db = crate::execution::routing::owning_db_for_execution(&dbs, &execution_id)
                .await
                .map_err(|error| error.to_string())?;
            db.read(move |conn| {
                Box::pin(async move {
                    cairn_db::db_records::load_node_selection_conn(
                        conn,
                        &execution_id,
                        &recipe_node_id,
                    )
                    .await
                })
            })
            .await
            .map_err(|error| error.to_string())
        })?;
        stored
            .map(|json| {
                serde_json::from_str(&json)
                    .map_err(|error| format!("a stored selection does not parse: {error}"))
            })
            .transpose()
    }

    /// The selection stored on `job_id` when it launched, if it has one.
    pub(crate) fn job_selection(&self, job_id: &str) -> Result<Option<SelectionRecord>, String> {
        let dbs = self.db.clone();
        let job_id = job_id.to_string();
        let stored = run_db_blocking(move || async move {
            let db = crate::execution::routing::owning_db_for_job(&dbs, &job_id)
                .await
                .map_err(|error| error.to_string())?;
            db.read(move |conn| {
                let job_id = job_id.clone();
                Box::pin(async move {
                    cairn_db::db_records::load_job_selection_conn(conn, &job_id).await
                })
            })
            .await
            .map_err(|error| error.to_string())
        })?;
        stored
            .map(|json| {
                serde_json::from_str(&json)
                    .map_err(|error| format!("a stored selection does not parse: {error}"))
            })
            .transpose()
    }

    /// Store `record` as why `job_id` runs on the model it does.
    pub(crate) fn store_job_selection(
        &self,
        job_id: &str,
        record: &SelectionRecord,
    ) -> Result<(), String> {
        let json = serde_json::to_string(record)
            .map_err(|error| format!("a selection does not serialize: {error}"))?;
        let dbs = self.db.clone();
        let job_id = job_id.to_string();
        run_db_blocking(move || async move {
            let db = crate::execution::routing::owning_db_for_job(&dbs, &job_id)
                .await
                .map_err(|error| error.to_string())?;
            db.write(move |conn| {
                let (job_id, json) = (job_id.clone(), json.clone());
                Box::pin(async move {
                    cairn_db::db_records::set_job_selection_conn(conn, &job_id, Some(&json)).await
                })
            })
            .await
            .map_err(|error| error.to_string())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::model_selection::tests::{catalog, models, run, windows};
    use crate::resources::models::{render_ladder, render_selection};

    /// The ladder view shows a kept effort word, and names it as the level
    /// once the model's scale has it.
    #[test]
    fn the_ladder_view_shows_a_kept_effort_word() {
        let catalog = catalog();
        let terra = |word: &str| {
            ladder_view(
                &crate::execution::model_selection::tests::models_keeping(word),
                &catalog,
            )
            .rungs
            .into_iter()
            .find(|rung| rung.model == "codex/gpt-5.6-terra")
            .unwrap()
        };
        let on = terra("medium");
        assert_eq!(on.effort, None);
        assert_eq!(on.effort_word.as_deref(), Some("medium"));
        assert_eq!(on.effort_level.as_deref(), Some("medium"));
        let off = terra("ultra");
        assert_eq!(off.effort_word.as_deref(), Some("ultra"));
        assert_eq!(off.effort_level, None);
        let rendered = render_ladder(&[], || {
            Ok(ladder_view(
                &crate::execution::model_selection::tests::models_keeping("ultra"),
                &catalog,
            ))
        });
        assert!(
            rendered.contains("effort 'ultra' kept from the tier grid"),
            "{rendered}"
        );
    }

    #[test]
    fn the_ladder_view_places_rungs_in_bands_with_maker_and_effort() {
        let view = ladder_view(&models(), &catalog());
        let bands: Vec<&str> = view.bands.iter().map(|band| band.id.as_str()).collect();
        assert_eq!(bands, ["sm", "md", "lg"]);
        let sol = &view.rungs[0];
        assert_eq!(sol.model, "codex/gpt-5.6-sol");
        assert_eq!(sol.band.as_deref(), Some("lg"));
        assert_eq!(sol.maker.as_deref(), Some("openai"));
        assert_eq!(sol.level_source, Some(LevelSource::Set));
        // 0.5 on a five-level scale lands on the third level.
        assert_eq!(sol.effort_level.as_deref(), Some("high"));
        assert_eq!(sol.effort_scale.len(), 5);
        let glm = view
            .rungs
            .iter()
            .find(|rung| rung.model == "opencode-go/glm-5.3")
            .unwrap();
        assert_eq!(glm.maker.as_deref(), Some("z-ai"));
        assert_eq!(glm.band.as_deref(), Some("md"));
        let myth = view.pools.iter().find(|pool| pool.id == "myth").unwrap();
        assert_eq!(
            myth.members.as_deref(),
            Some(&["claude/fable".to_string()][..])
        );
        assert!(view.unranked.contains(&"codex/gpt-5.6-terra".to_string()));
        assert!(!view.unranked.contains(&"codex/gpt-5.6-sol".to_string()));
    }

    /// An editor writes the position a level's step carries, so each step must
    /// be exactly where the facet puts that level, and must launch at it.
    #[test]
    fn each_effort_step_is_the_facets_position_and_an_effortless_model_has_none() {
        let mut catalog = catalog();
        let opencode = catalog
            .providers
            .iter_mut()
            .find(|provider| provider.descriptor.key.as_ref() == "opencode-go")
            .unwrap();
        let listed = opencode.models.as_mut().unwrap();
        listed.models[0].supported_reasoning_efforts.clear();
        let view = ladder_view(&models(), &catalog);

        let sol = view
            .rungs
            .iter()
            .find(|rung| rung.model == "codex/gpt-5.6-sol")
            .unwrap();
        let codex = catalog
            .providers
            .iter()
            .find(|provider| provider.descriptor.key.as_ref() == "codex")
            .unwrap();
        let levels: Vec<&str> = sol
            .effort_scale
            .iter()
            .map(|step| step.level.as_str())
            .collect();
        assert_eq!(levels, ["low", "medium", "high", "xhigh", "ultra"]);
        for step in &sol.effort_scale {
            assert_eq!(
                Some(step.position),
                codex.position("gpt-5.6-sol", &step.level),
                "{}",
                step.level
            );
            let launched = codex.effort("gpt-5.6-sol", step.position).unwrap();
            assert_eq!(
                launched.reasoning_effort.as_deref(),
                Some(step.level.as_str())
            );
        }

        let glm = view
            .rungs
            .iter()
            .find(|rung| rung.model == "opencode-go/glm-5.3")
            .unwrap();
        assert!(glm.effort_scale.is_empty(), "{:?}", glm.effort_scale);
        assert_eq!(glm.effort_level, None);
    }

    #[test]
    fn the_ladder_renders_bands_highest_first_with_each_rungs_facts() {
        let text = render_ladder(&[], || Ok(ladder_view(&models(), &catalog())));
        let lg = text.find("### lg (0.67–1)").expect(&text);
        let sm = text.find("### sm (0–0.34)").expect(&text);
        assert!(lg < sm, "{text}");
        assert!(
            text.contains("- `codex/gpt-5.6-sol` 0.85 (set) — by openai, effort 0.5 → high of low|medium|high|xhigh|ultra"),
            "{text}"
        );
        assert!(
            text.contains("- `myth` set [claude/fable]: hand-picked"),
            "{text}"
        );
        assert!(text.contains("- `codex/gpt-5.6-terra`"), "{text}");
        assert!(
            text.contains("Default allotment (a definition naming none): [md]"),
            "{text}"
        );
        let refused = render_ladder(
            &cairn_common::query::parse_query_params("view=x").unwrap(),
            || unreachable!("a refused query reads nothing"),
        );
        assert!(refused.contains("takes no query parameters"), "{refused}");
    }

    #[test]
    fn the_selection_preview_renders_the_provenance_and_the_ranked_band() {
        let text = render_selection(
            &cairn_common::query::parse_query_params("models=sm,md,lg").unwrap(),
            |allotment, verb| {
                assert_eq!(
                    allotment.as_deref(),
                    Some(&["sm".to_string(), "md".to_string(), "lg".to_string()][..])
                );
                assert_eq!(verb, VerbKind::Session);
                let raw: Vec<String> = allotment.unwrap();
                let raw: Vec<&str> = raw.iter().map(String::as_str).collect();
                run(&raw, &ResolvedConstraints::default(), &windows).map_err(String::from)
            },
        );
        assert!(text.contains("[sm, md, lg] for session"), "{text}");
        assert!(
            text.contains("Chosen: `codex/gpt-5.6-sol` @ high"),
            "{text}"
        );
        assert!(text.contains("band lg"), "{text}");
        assert!(
            text.contains("set aside: outside the chosen band"),
            "{text}"
        );

        let refused = render_selection(&[], |allotment, _| {
            assert_eq!(
                allotment, None,
                "no `models` asks for the workspace default"
            );
            Err("nothing to run".to_string())
        });
        assert!(refused.contains("Refused: nothing to run"), "{refused}");
        let verb = render_selection(
            &cairn_common::query::parse_query_params("verb=image").unwrap(),
            |_, _| unreachable!("an unselectable verb is refused first"),
        );
        assert!(verb.contains("does not select from an allotment"), "{verb}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn every_definition_naming_an_allotment_is_reported_with_what_it_names() {
        let orch = crate::orchestrator::models::tests::bare_orchestrator().await;
        let write = |path: &str, text: &str| {
            let path = orch.config_dir.join(path);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, text).unwrap();
        };
        write(
            "agents/reviewer-x.md",
            "---\nname: Reviewer X\ndescription: reviews\ntools: mcp__cairn__read\nmodels: [md, claude/opus]\n---\nReview.\n",
        );
        write(
            "agents/plain-x.md",
            "---\nname: Plain X\ndescription: plain\ntools: mcp__cairn__read\n---\nWork.\n",
        );
        write(
            "recipes/review-x.yaml",
            "cairnVersion: 1\nname: Review X\ntrigger: manual\nnodes:\n- id: trigger-1\n  type: trigger\n  name: Trigger\n  position: 0@0\n  config:\n    triggerType: manual\n- id: reviewer-1\n  type: agent\n  name: Reviewer\n  position: 0@100\n  config:\n    agent: reviewer-x\n    models: [lg]\n- id: builder-1\n  type: agent\n  name: Builder\n  position: 0@200\n  config:\n    agent: plain-x\nedges:\n- from: trigger-1@control-out\n  to: reviewer-1@control-in\n  type: control\n",
        );
        write(
            "responses/summarize-x.md",
            "---\nname: Summarize X\ndescription: summarizes\nmodels: [sm]\n---\nSummarize.",
        );

        let references = orch.allotment_references().unwrap();
        let named = |name: &str| {
            references
                .iter()
                .find(|reference| reference.name == name)
                .map(|reference| {
                    let models: Vec<String> =
                        reference.models.iter().map(ToString::to_string).collect();
                    (reference.kind, reference.project_id.clone(), models)
                })
        };
        assert_eq!(
            named("Reviewer X"),
            Some((
                DefinitionKind::Agent,
                None,
                vec!["md".to_string(), "claude/opus".to_string()]
            ))
        );
        assert_eq!(
            named("Review X › Reviewer"),
            Some((DefinitionKind::RecipeNode, None, vec!["lg".to_string()]))
        );
        assert_eq!(
            named("Summarize X"),
            Some((DefinitionKind::Response, None, vec!["sm".to_string()]))
        );
        // Naming no allotment of its own is not a reference.
        assert_eq!(named("Plain X"), None);
        assert_eq!(named("Review X › Builder"), None);
        let json = serde_json::to_value(&references).unwrap();
        assert!(
            json.as_array().unwrap().iter().any(|reference| reference
                == &serde_json::json!({"kind": "recipeNode", "name": "Review X › Reviewer", "projectId": null, "models": ["lg"]})),
            "{json}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_tier_overlay_converts_against_the_archived_grid_and_is_never_written() {
        use cairn_config::allotment::{model_ref, AllotmentEntry};
        let orch = crate::orchestrator::models::tests::bare_orchestrator().await;
        // A workspace grid of its own: a fourth tier the shipped grid lacks.
        std::fs::write(
            orch.config_dir.join("settings.yaml"),
            "enabledProviders: [claude, codex]\ntiers: [sm, md, lg, xl]\nbackends:\n  claude:\n    xl: {model: fable}\n",
        )
        .unwrap();
        orch.migrate_models()
            .unwrap()
            .expect("the workspace converts");
        let settings = std::fs::read_to_string(orch.config_dir.join("settings.yaml")).unwrap();
        assert!(
            !settings.contains("tiers:"),
            "the grid is retired: {settings}"
        );
        assert!(orch
            .config_dir
            .join(cairn_config::settings::RETIRED_GRID_FILE)
            .exists());

        // A project checked out afterwards still says what its xl cell holds.
        let repo = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(repo.path().join(".cairn")).unwrap();
        let config = repo.path().join(".cairn/config.yaml");
        let written = "backends:\n  claude:\n    xl: {model: opus}\n";
        std::fs::write(&config, written).unwrap();
        orch.db
            .local
            .execute(
                "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
                 VALUES ('p', 'default', 'P', 'p', ?1, 1, 1)",
                cairn_db::turso::params![repo.path().to_string_lossy().to_string()],
            )
            .await
            .unwrap();

        let xl = |models: &ModelsConfig| -> Vec<String> {
            models
                .expand(&[AllotmentEntry::Pool("xl".into())])
                .unwrap()
                .candidates
                .iter()
                .map(|candidate| model_ref(&candidate.model))
                .collect()
        };
        assert_eq!(xl(&orch.effective_models(None).unwrap()), ["claude/fable"]);
        assert_eq!(
            xl(&orch.effective_models(Some("p")).unwrap()),
            ["claude/opus"]
        );

        // Startup reads the project and leaves its file as written.
        orch.migrate_models().unwrap();
        assert_eq!(std::fs::read_to_string(&config).unwrap(), written);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_workspace_converts_once_and_previews_read_its_pools() {
        let orch = crate::orchestrator::models::tests::bare_orchestrator().await;
        // Before the conversion runs, the ladder is the in-memory conversion.
        let before = orch.effective_models(None).unwrap();
        assert!(before.pool("md").is_some());
        let notes = orch.migrate_models().unwrap();
        assert!(notes.is_some());
        assert_eq!(orch.migrate_models().unwrap(), None);
        let after = orch.effective_models(None).unwrap();
        assert_eq!(after, orch.get_settings().models.unwrap());
        assert_eq!(after.pools, before.pools);
        let ladder = orch.models_ladder(None).unwrap();
        assert_eq!(ladder.pools.len(), after.pools.len());

        let refusal = orch
            .preview_selection(None, Some(&["xl".to_string()]), VerbKind::Session)
            .unwrap_err();
        assert!(refusal.contains("'xl' is not a pool"), "{refusal}");
    }
}
