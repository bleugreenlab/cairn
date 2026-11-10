//! The orchestrator's half of model selection: what [`select`] reads, gathered
//! from where it lives (settings, the project overlay, the catalog, the
//! credential store, the usage book), and where a choice is stored.
//!
//! Nothing here probes a provider. Usage comes from what the book already
//! holds and the credential store's recorded health, so a preview costs a
//! settings read and a catalog build, not a network round trip.

use cairn_config::library::LibraryRoots;
use std::collections::BTreeMap;

use cairn_config::allotment::{
    model_ref, parse_allotment, AllotmentEntry, Band, EffortPlacement, LevelSource, ModelsConfig,
    PoolKind,
};
use cairn_config::project_settings::{convert_project_tiers, load_project_settings_read_only};
use cairn_config::settings::{convert_settings_tiers, load_settings_file};
use cairn_db::storage::run_db_blocking;
use cairn_models::interface::{Catalog, CatalogProvider, ModelScope, Models};
use cairn_models::supply::SupplyInputs;
use cairn_models::types::benchmarks::Basis;
use cairn_models::types::catalog::VerbKind;
use cairn_models::types::usage::ProviderUsageWindow;
use serde::Serialize;

use super::Orchestrator;
use crate::credentials::RoutedProvider;
use crate::execution::model_selection::{
    select, BenchmarkPlacer, ResolvedConstraints, SelectionContext, SelectionRecord,
    SelectionRefusal, SelectionSubject, TopOfAllotment, CALIBRATE_BENCHMARKS,
};
use crate::models::{ModelSelection, PresetOptionValue};

/// What a selection preview was asked about.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreviewedAllotment {
    /// The allotment the caller wrote.
    Asked(Vec<String>),
    /// The workspace's default allotment.
    WorkspaceDefault,
    /// The workspace's `decisionModel`, which decide selects within.
    DecisionModel(String),
    /// No `decisionModel` is set: decide takes the catalog's best offer.
    BestDecisionOffer,
}

/// A selection preview: what was asked about, and what it chose or why it
/// refused.
#[derive(Debug, Clone)]
pub struct SelectionPreview {
    pub asked: PreviewedAllotment,
    pub record: Result<SelectionRecord, String>,
}

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
    /// Served session models on installed providers with no level, each with
    /// where the benchmarks would place it.
    pub unranked: Vec<UnrankedModel>,
    pub default_models: Vec<AllotmentEntry>,
    pub conversion_notes: Vec<String>,
    /// The credit lines the benchmark data's license requires.
    pub benchmark_attribution: Vec<String>,
    /// The anchor the benchmark levels are on (`v1`).
    pub benchmark_anchor: String,
    /// The points (solve rate, ladder level) the map from benchmark levels
    /// onto this ladder runs through; empty when it is not calibrated.
    pub benchmark_calibration: Vec<[f64; 2]>,
    /// What to do when it is not calibrated, said once for every surface.
    pub benchmark_note: Option<String>,
}

/// Where the shipped benchmark fit places a model: an input beside the
/// ladder, never a stored level.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BenchmarkHint {
    /// The expected solve rate on the anchor's reference panel, and its interval.
    pub level: f64,
    pub low: f64,
    pub high: f64,
    pub sources: Vec<String>,
    pub anchor: String,
    /// The solve rate mapped onto this ladder, with its interval mapped too;
    /// `None` while the ladder has fewer than two calibration rungs.
    pub position: Option<f64>,
    pub position_low: Option<f64>,
    pub position_high: Option<f64>,
    /// What "Place at" records as the level's `from`:
    /// `benchmarks 0.655 → 0.82 via your ladder, anchor v1`.
    pub from: Option<String>,
    /// `benchmarks: 0.655 solve rate → 0.82 on your ladder [0.78, 0.86], 3 sources, anchor v1`,
    /// or the raw solve rate alone when the ladder is not calibrated.
    pub summary: String,
    /// The fit's own solve rate when release order moved it (`level` is then
    /// the pooled one), and the releases of its line it was pooled with.
    pub measured: Option<f64>,
    pub pooled_with: Vec<String>,
    /// For a release with too few results of its own, the earlier release of
    /// its line whose level it takes as a floor.
    pub at_least: Option<String>,
}

fn benchmark_hint(
    placer: &BenchmarkPlacer<'_>,
    provider: &str,
    model: &str,
) -> Option<BenchmarkHint> {
    let placed = placer.placement(provider, model)?;
    let position = placer.position(placed.level);
    let (position_low, position_high) = (placer.position(placed.low), placer.position(placed.high));
    let note = placed
        .basis_note()
        .map(|note| format!("; {note}"))
        .unwrap_or_default();
    let summary = match (position, position_low, position_high) {
        (Some(at), _, _) if placed.is_floor() => format!(
            "benchmarks: at least {:.3} solve rate → at least {at:.2} on your ladder, anchor {}{note}",
            placed.level, placed.anchor
        ),
        (Some(at), Some(low), Some(high)) => format!(
            "benchmarks: {:.3} solve rate → {at:.2} on your ladder [{low:.2}, {high:.2}], {} source{}, anchor {}{note}",
            placed.level,
            placed.sources.len(),
            if placed.sources.len() == 1 { "" } else { "s" },
            placed.anchor
        ),
        _ => placed.summary(),
    };
    let identity_note = placer.identity_note(provider, model);
    let summary = match &identity_note {
        Some(note) => format!("{summary}; {note}"),
        None => summary,
    };
    let (measured, pooled_with, at_least) = match &placed.basis {
        Basis::Measured => (None, Vec::new(), None),
        Basis::Ordered {
            measured,
            pooled_with,
        } => (Some(*measured), pooled_with.clone(), None),
        Basis::AtLeast { succeeds } => (None, Vec::new(), Some(succeeds.clone())),
    };
    Some(BenchmarkHint {
        measured,
        pooled_with,
        at_least,
        from: placer.from_text(&placed).map(|from| match &identity_note {
            Some(note) => format!("{from}; {note}"),
            None => from,
        }),
        summary,
        position,
        position_low,
        position_high,
        level: placed.level,
        low: placed.low,
        high: placed.high,
        sources: placed.sources,
        anchor: placed.anchor,
    })
}

/// The newest release of `model`'s product line that `provider` lists after
/// it, as `provider/model`: what to replace an older rung with. Release order
/// alone decides; whether the newer one is worth its price is selection's.
fn superseded_by(provider: &CatalogProvider, model: &str) -> Option<String> {
    let identity = cairn_models::types::identity::identity();
    let (line, at) = identity.release(&provider.canonical(model)?)?;
    let key: &str = provider.descriptor.key.as_ref();
    provider
        .models
        .as_ref()?
        .models
        .iter()
        .filter(|listed| !listed.hidden)
        .filter_map(|listed| {
            let (other, later) = identity.release(&provider.canonical(&listed.model)?)?;
            (other == line && later > at).then_some((later, listed.model.as_str()))
        })
        .max_by_key(|(later, _)| *later)
        .map(|(_, newer)| format!("{key}/{newer}"))
}

/// A served model with no level on the ladder.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UnrankedModel {
    /// `provider/model`.
    pub model: String,
    /// Where the benchmarks place it, when they cover it.
    pub benchmark: Option<BenchmarkHint>,
    /// Alias observation provenance, including when benchmarks cannot place it.
    pub identity_note: Option<String>,
    /// The band the benchmark level falls in, which is where an allotment
    /// naming it selects it.
    pub band: Option<String>,
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
    /// The stored level only; a benchmark placement is in `benchmark`.
    pub level: Option<f64>,
    pub level_source: Option<LevelSource>,
    /// What the level was derived from, in words.
    pub level_from: Option<String>,
    /// The band of the level selection uses: the stored one, else the
    /// benchmark placement.
    pub band: Option<String>,
    /// Where the benchmarks place this model, shown beside a stored level
    /// and never replacing it.
    pub benchmark: Option<BenchmarkHint>,
    /// Alias observation provenance, including when benchmarks cannot place it.
    pub identity_note: Option<String>,
    /// A newer release of this model's product line the same provider offers
    /// (`provider/model`, the newest), when there is one.
    pub superseded_by: Option<String>,
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
    let placer = BenchmarkPlacer::new(models, catalog);
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
            let benchmark = benchmark_hint(&placer, &rung.model.backend, model);
            LadderRung {
                model: model_ref(&rung.model),
                level: rung.level.as_ref().map(|level| level.value),
                level_source: rung.level.as_ref().map(|level| level.source),
                level_from: rung.level.as_ref().and_then(|level| level.from.clone()),
                band: rung
                    .level
                    .as_ref()
                    .map(|level| level.value)
                    .or(benchmark.as_ref().and_then(|hint| hint.position))
                    .and_then(|level| models.band_of(level))
                    .map(|band| band.id),
                identity_note: on.and_then(|provider| provider.identity_note(model)),
                benchmark,
                superseded_by: on.and_then(|provider| superseded_by(provider, model)),
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
    let mut unranked: Vec<UnrankedModel> = Vec::new();
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
            if served
                && !laddered(key, &model.model)
                && !unranked.iter().any(|known| known.model == entry)
            {
                let benchmark = benchmark_hint(&placer, key, &model.model);
                unranked.push(UnrankedModel {
                    band: benchmark
                        .as_ref()
                        .and_then(|hint| hint.position)
                        .and_then(|position| models.band_of(position))
                        .map(|band| band.id),
                    benchmark,
                    identity_note: provider.identity_note(&model.model),
                    model: entry,
                });
            }
        }
    }
    let shipped = cairn_models::types::benchmarks::shipped();
    let calibration: Vec<[f64; 2]> = placer
        .map()
        .map(|map| map.points().iter().map(|&(x, y)| [x, y]).collect())
        .unwrap_or_default();
    LadderView {
        benchmark_note: calibration
            .is_empty()
            .then(|| format!("Benchmark levels are not placed yet: {CALIBRATE_BENCHMARKS}.")),
        benchmark_calibration: calibration,
        pools,
        bands,
        rungs,
        unranked,
        default_models: models.default_models.clone(),
        conversion_notes: models.conversion_notes.clone(),
        benchmark_attribution: shipped.attribution(),
        benchmark_anchor: shipped.anchor.clone(),
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
            for listed in
                responses::list_responses(&LibraryRoots::at(&self.config_dir, path.as_deref()))?
            {
                let ConfigResult::Ok(file) = listed else {
                    continue;
                };
                if file.is_project_scoped() != project_id.is_some() {
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

    /// What `allotment` would select for `verb` now. Nothing is stored and no
    /// constraint applies: a preview asks about one node in isolation.
    ///
    /// With no allotment it previews what a node naming none runs on: for
    /// decide, the workspace's `decisionModel`, or the catalog's best decision
    /// offer when that is unset (what [`Orchestrator::decision_target`]
    /// leaves to the catalog); for any other verb, the workspace's default
    /// allotment.
    pub fn preview_selection(
        &self,
        project_id: Option<&str>,
        allotment: Option<&[String]>,
        verb: VerbKind,
    ) -> SelectionPreview {
        let (asked, allotment) = match allotment {
            Some(raw) => (
                PreviewedAllotment::Asked(raw.to_vec()),
                parse_allotment(raw),
            ),
            None if verb == VerbKind::Decide => match self
                .get_settings()
                .decision_model
                .map(|model| model.trim().to_string())
                .filter(|model| !model.is_empty())
            {
                Some(model) => {
                    let entry = AllotmentEntry::parse(&model).map(|entry| vec![entry]);
                    (PreviewedAllotment::DecisionModel(model), entry)
                }
                None => {
                    return SelectionPreview {
                        asked: PreviewedAllotment::BestDecisionOffer,
                        record: self.best_decision_offer_record(project_id),
                    }
                }
            },
            None => (
                PreviewedAllotment::WorkspaceDefault,
                self.effective_models(project_id)
                    .map(|models| models.default_models),
            ),
        };
        let record = allotment.and_then(|allotment| {
            self.select_models(
                project_id,
                &allotment,
                verb,
                &ResolvedConstraints::default(),
                SelectionSubject::default(),
            )
            .map_err(String::from)
        });
        SelectionPreview { asked, record }
    }

    /// The catalog's best decision offer, as the record a pin to it stores.
    fn best_decision_offer_record(
        &self,
        project_id: Option<&str>,
    ) -> Result<SelectionRecord, String> {
        use cairn_models::interface::OfferQuery;
        let catalog = self.catalog(&ModelScope {
            project_id: project_id.map(str::to_string),
        })?;
        let offer = catalog
            .offer_for(OfferQuery::new(VerbKind::Decide))
            .map_err(|refusal| refusal.to_string())?;
        let models = self.effective_models_in(project_id, &catalog)?;
        let chosen = cairn_config::allotment::parse_model_ref(&format!(
            "{}/{}",
            offer.provider, offer.model
        ))?;
        let mut record = crate::execution::model_selection::pinned(&models, &catalog, &chosen);
        record.provenance = format!(
            "no `decisionModel` is set, so decide takes the catalog's best decision offer, {}",
            model_ref(&chosen)
        );
        Ok(record)
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
        let catalog = self.catalog(&ModelScope {
            project_id: project_id.map(str::to_string),
        })?;
        let models = self.effective_models_in(project_id, &catalog)?;
        let mut record = self.with_supply(project_id, |supply| {
            let ctx = SelectionContext {
                models: &models,
                catalog: &catalog,
                supply,
            };
            select(&ctx, allotment, verb, constraints, &TopOfAllotment)
        })?;
        record.subject = subject;
        Ok(record)
    }

    /// Run `f` over what supply reads in `project_id` now: the credential
    /// store's accounts with the same recent-assignment evidence session
    /// routing spreads by, and usage windows the book holds for providers the
    /// credential store does not route (OpenCode Go).
    fn with_supply<T>(&self, project_id: Option<&str>, f: impl FnOnce(SupplyInputs<'_>) -> T) -> T {
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
        f(SupplyInputs {
            store: store.as_ref(),
            project_id,
            assignments: &assignments,
            provider_windows: &provider_windows,
            now: chrono::Utc::now().timestamp(),
        })
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
    ///
    /// A pin is never replaced, but it is asked the same supply question a
    /// selection asks of its candidates: a pin whose every account is out of
    /// usage, blocked or out of credits is refused with
    /// [`SelectionRefusal::NoSupply`], so its launch holds instead of dying at
    /// session routing. Anything else about the pin (a provider not installed,
    /// no credential at all) is the launch's own error to report.
    pub(crate) fn pinned_selection(
        &self,
        project_id: Option<&str>,
        pin: &crate::models::ModelSelection,
        subject: SelectionSubject,
    ) -> Result<SelectionRecord, SelectionRefusal> {
        let catalog = self.catalog(&ModelScope {
            project_id: project_id.map(str::to_string),
        })?;
        let models = self.effective_models_in(project_id, &catalog)?;
        let reference = cairn_config::allotment::model_ref(pin);
        if let Some(provider) = catalog
            .providers
            .iter()
            .find(|provider| provider.installed && provider.descriptor.key == pin.backend.as_str())
        {
            let shortfall = self.with_supply(project_id, |supply| {
                cairn_models::supply::supply_for(provider, pin.model.as_str(), &supply).err()
            });
            if let Some(why) = shortfall.filter(|why| why.supply) {
                return Err(SelectionRefusal::NoSupply(
                    cairn_models::supply::CapacityRefusal::of(
                        &reference,
                        format!("pinned {reference} cannot run now: {why}"),
                        &why,
                    ),
                ));
            }
        }
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
    fn alias_provenance_survives_without_a_benchmark_and_never_changes_stored_levels() {
        use cairn_models::interface::{AliasResolution, ALIAS_RESOLUTION_TTL_SECS};
        let now = chrono::Utc::now().timestamp();
        let mut models = models();
        let mut calibration = models.ladder[0].clone();
        calibration.model =
            cairn_config::allotment::parse_model_ref("opencode-go/kimi-k2.5").unwrap();
        models.ladder.push(calibration);
        let original = models.clone();
        for (report, age, version, expected) in [
            (Some("claude-opus-4-6"), 0, None, "observed"),
            (
                Some("claude-opus-4-6"),
                ALIAS_RESOLUTION_TTL_SECS + 1,
                None,
                "stale",
            ),
            (Some("claude-unrecognized"), 0, None, "unrecognized"),
            (
                Some("claude-opus-4-6"),
                0,
                Some("new-version"),
                "CLI version changed",
            ),
            (None, 0, None, "no"),
        ] {
            let mut catalog = catalog();
            let claude = catalog
                .providers
                .iter_mut()
                .find(|on| on.descriptor.key == "claude")
                .unwrap();
            if let Some(report) = report {
                claude.alias_resolutions.push(AliasResolution {
                    alias: "opus".into(),
                    reported: report.into(),
                    canonical: cairn_models::types::identity::for_offer("claude", report, None)
                        .map(str::to_string),
                    observed_at: now - age,
                    session_id: "alias-observing-session".into(),
                    cli_version: Some("old-version".into()),
                    current_cli_version: version.map(str::to_string),
                });
            }
            let note = claude.identity_note("opus").unwrap();
            assert!(note.contains(expected), "{note}");
            if let Some(report) = report {
                assert!(note.contains("alias-observing-session"), "{note}");
                assert!(note.contains(report), "{note}");
            }
            let view = ladder_view(&models, &catalog);
            let opus = view
                .rungs
                .iter()
                .find(|r| r.model == "claude/opus")
                .unwrap();
            assert_eq!(opus.level, Some(0.83));
            assert_eq!(opus.level_source, Some(LevelSource::Set));
            assert_eq!(opus.identity_note.as_deref(), Some(note.as_str()));
            let text = render_ladder(&[], || Ok(view.clone()));
            assert!(text.contains(&note), "{text}");
            if report == Some("claude-opus-4-6") && age == 0 && version.is_none() {
                let hint = opus.benchmark.as_ref().expect("fresh alias benchmark");
                assert!(hint.summary.contains(&note));
                assert!(hint.from.as_ref().expect("calibrated").contains(&note));
                let mut unplaced = models.clone();
                unplaced
                    .ladder
                    .iter_mut()
                    .find(|r| r.model.backend == "claude" && r.model.model.as_str() == "opus")
                    .unwrap()
                    .model =
                    cairn_config::allotment::parse_model_ref("claude/claude-opus-4-6").unwrap();
                let ctx = SelectionContext {
                    models: &unplaced,
                    catalog: &catalog,
                    supply: SupplyInputs {
                        store: None,
                        project_id: None,
                        assignments: &[],
                        provider_windows: &|_| None,
                        now,
                    },
                };
                let record = select(
                    &ctx,
                    &parse_allotment(&["claude/opus"]).unwrap(),
                    VerbKind::Session,
                    &ResolvedConstraints::default(),
                    &TopOfAllotment,
                )
                .unwrap();
                assert_eq!(
                    record.candidates[0].level_source,
                    Some(LevelSource::Benchmarks)
                );
                assert_eq!(
                    record.candidates[0].identity_note.as_deref(),
                    Some(note.as_str())
                );
                assert_eq!(record.provenance.matches(&note).count(), 1);
            } else {
                assert!(opus.benchmark.is_none());
            }
            let unranked_models = ModelsConfig {
                ladder: Vec::new(),
                ..models.clone()
            };
            let unranked = ladder_view(&unranked_models, &catalog);
            let alias = unranked
                .unranked
                .iter()
                .find(|m| m.model == "claude/opus")
                .unwrap();
            assert_eq!(alias.identity_note.as_deref(), Some(note.as_str()));
            assert!(render_ladder(&[], || Ok(unranked.clone())).contains(&note));

            let ctx = SelectionContext {
                models: &models,
                catalog: &catalog,
                supply: SupplyInputs {
                    store: None,
                    project_id: None,
                    assignments: &[],
                    provider_windows: &|_| None,
                    now,
                },
            };
            let record = select(
                &ctx,
                &parse_allotment(&["claude/opus", "claude/haiku"]).unwrap(),
                VerbKind::Session,
                &ResolvedConstraints::default(),
                &TopOfAllotment,
            )
            .unwrap();
            let candidate = record
                .candidates
                .iter()
                .find(|c| c.model == "claude/opus")
                .unwrap();
            assert_eq!(candidate.level, Some(0.83));
            assert_eq!(candidate.identity_note.as_deref(), Some(note.as_str()));
            assert!(record
                .candidates
                .iter()
                .find(|c| c.model == "claude/haiku")
                .unwrap()
                .identity_note
                .is_some());
            assert!(record.provenance.contains(&note));
            assert!(render_selection(&[], |_, _| SelectionPreview {
                asked: PreviewedAllotment::WorkspaceDefault,
                record: Ok(record.clone()),
            })
            .contains(&note));
            let pinned =
                crate::execution::model_selection::pinned(&models, &catalog, &record.chosen);
            assert_eq!(
                pinned.candidates[0].identity_note.as_deref(),
                Some(note.as_str())
            );
            assert!(pinned.provenance.contains(&note));
            assert_eq!(models, original);
        }
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
        let unranked: Vec<&str> = view.unranked.iter().map(|u| u.model.as_str()).collect();
        assert!(unranked.contains(&"codex/gpt-5.6-terra"));
        assert!(!unranked.contains(&"codex/gpt-5.6-sol"));
        // Models the benchmarks do not cover keep today's view.
        assert!(view.unranked.iter().all(|u| u.benchmark.is_none()));
        assert!(sol.benchmark.is_none());
        assert!(!view.benchmark_attribution.is_empty());
    }

    #[test]
    fn approved_openrouter_variants_show_benchmark_hints_in_a_ladder_read() {
        let offers = [
            ("cohere/command-r7b-12-2024", "command-r7b"),
            ("meta-llama/llama-3.1-70b-instruct", "llama-3.1-70b"),
            ("meta-llama/llama-3.1-8b-instruct", "llama-3.1-8b"),
            ("meta-llama/llama-3.2-1b-instruct", "llama-3.2-1b"),
            ("meta-llama/llama-3.2-3b-instruct", "llama-3.2-3b"),
            ("meta-llama/llama-3.3-70b-instruct", "llama-3.3-70b"),
            ("nvidia/nemotron-3-super-120b-a12b", "nemotron-3-super"),
            ("amazon/nova-pro-v1", "nova-pro"),
            ("qwen/qwen-2.5-72b-instruct", "qwen2.5-72b"),
            ("openai/o4-mini:batch", "o4-mini"),
        ];
        let mut catalog = catalog();
        let mut provider = catalog.providers[0].clone();
        provider.descriptor = cairn_models::types::catalog::descriptor("openrouter")
            .cloned()
            .unwrap();
        provider.alias_resolutions.clear();
        let discovered = provider.models.as_mut().unwrap();
        discovered.backend = "openrouter".into();
        discovered.models = offers
            .iter()
            .map(|(native, _)| {
                let mut model = cairn_models::types::catalog::claude_models().remove(0);
                model.id = (*native).into();
                model.model = (*native).into();
                model.maker = cairn_models::types::catalog::openrouter_maker(native);
                model.canonical_slug =
                    (*native == "openai/o4-mini:batch").then(|| "openai/o4-mini-2025-04-16".into());
                model
            })
            .collect();
        catalog.providers.push(provider);

        let mut models = models();
        // Well-separated benchmarked releases calibrate solve rates onto this ladder.
        for (native, level) in [
            ("openrouter/meta-llama/llama-3.2-1b-instruct", 0.1),
            ("openrouter/meta-llama/llama-3.3-70b-instruct", 0.7),
        ] {
            models.ladder.push(cairn_config::allotment::Rung {
                model: cairn_config::allotment::parse_model_ref(native).unwrap(),
                level: Some(cairn_config::allotment::Level::set(level)),
                effort: None,
                effort_level: None,
                options: BTreeMap::new(),
            });
        }
        let original = models.clone();
        let view = ladder_view(&models, &catalog);
        let text = render_ladder(&[], || Ok(view.clone()));
        for (native, canonical) in offers {
            let reference = format!("openrouter/{native}");
            assert_eq!(
                catalog.benchmark("openrouter", native).unwrap().canonical,
                canonical
            );
            let hint = view
                .rungs
                .iter()
                .find(|rung| rung.model == reference)
                .and_then(|rung| rung.benchmark.as_ref())
                .or_else(|| {
                    view.unranked
                        .iter()
                        .find(|model| model.model == reference)
                        .and_then(|model| model.benchmark.as_ref())
                })
                .expect(native);
            let placement = cairn_models::types::benchmarks::shipped()
                .model(canonical)
                .unwrap();
            assert_eq!(
                (hint.level, hint.low, hint.high),
                (placement.level, placement.low, placement.high),
                "{native}"
            );
            assert!(hint.position.is_some(), "{native}");
            let line = text
                .lines()
                .find(|line| line.contains(&format!("`{reference}`")))
                .unwrap();
            assert!(line.contains(&hint.summary), "{line}");
            assert!(line.contains("on your ladder"), "{line}");
        }
        assert_eq!(models, original);
        println!("{text}");
    }

    /// A stored level stays the level, with the benchmark placement beside it
    /// as a hint; an unranked model the benchmarks cover shows its placement
    /// and band.
    #[test]
    fn the_ladder_view_hints_benchmarks_beside_a_stored_level_and_places_the_unranked() {
        let mut catalog = catalog();
        let openrouter = |models: &[&str]| cairn_models::interface::CatalogProvider {
            descriptor: cairn_models::types::catalog::descriptor("openrouter")
                .cloned()
                .unwrap(),
            installed: true,
            models: Some(cairn_models::types::catalog::ProviderModelCatalog {
                backend: "openrouter".into(),
                models: models
                    .iter()
                    .map(|id| {
                        let mut model = cairn_models::types::catalog::claude_models().remove(0);
                        model.id = id.to_string();
                        model.model = id.to_string();
                        model
                    })
                    .collect(),
                options: Vec::new(),
                refreshed_at: Some(1),
                error: None,
            }),
            serves: cairn_models::types::catalog::ServedVerb::conversation(
                cairn_models::types::catalog::Wire::OpenAiChatCompletions,
            ),
            unable: Vec::new(),
            alias_resolutions: Vec::new(),
        };
        catalog
            .providers
            .push(openrouter(&["anthropic/claude-opus-4.6", "openai/gpt-5.5"]));
        let stored = |model: &str, level: f64| cairn_config::allotment::Rung {
            model: cairn_config::allotment::parse_model_ref(model).unwrap(),
            level: Some(cairn_config::allotment::Level::set(level)),
            effort: None,
            effort_level: None,
            options: BTreeMap::new(),
        };
        let shipped = cairn_models::types::benchmarks::shipped();
        let rate = |id: &str| shipped.model(id).expect("shipped").level;
        let gpt_of = |view: &LadderView| {
            view.unranked
                .iter()
                .find(|u| u.model == "openrouter/openai/gpt-5.5")
                .cloned()
                .unwrap()
        };

        // One calibration rung fixes no scale: hints, but nothing placed.
        let mut models = models();
        models
            .ladder
            .push(stored("openrouter/anthropic/claude-opus-4.6", 0.8));
        let alone = ladder_view(&models, &catalog);
        let gpt = gpt_of(&alone);
        let raw = gpt.benchmark.as_ref().expect("a hint");
        assert_eq!((raw.position, gpt.band.as_deref()), (None, None));
        assert!(raw.summary.contains("solve rate") && !raw.summary.contains("on your ladder"));
        assert!(alone.benchmark_calibration.is_empty());
        let rendered = render_ladder(&[], || Ok(alone.clone()));
        assert!(rendered.contains(CALIBRATE_BENCHMARKS), "{rendered}");
        assert!(rendered.contains("(not placed)"), "{rendered}");

        let point = |id: &str, level: f64| {
            let placed = shipped.model(id).expect("shipped");
            cairn_config::allotment::CalibrationPoint {
                at: placed.level,
                low: placed.low,
                high: placed.high,
                level,
            }
        };

        // Two the benchmarks cannot tell apart are one point: a converted
        // Codex grid (gpt-5.3-codex at md's centre, gpt-5.5 at lg's) does not
        // calibrate.
        let mut codex = crate::execution::model_selection::tests::models();
        codex.ladder.push(stored("codex/gpt-5.3-codex", 0.505));
        codex.ladder.push(stored("codex/gpt-5.5", 0.835));
        let near = ladder_view(&codex, &catalog);
        assert!(near.benchmark_calibration.is_empty());
        assert!(near
            .benchmark_note
            .as_deref()
            .unwrap()
            .contains(CALIBRATE_BENCHMARKS));
        assert_eq!(gpt_of(&near).benchmark.unwrap().position, None);

        // Two it can: the user's own levels set the scale.
        models.ladder.push(stored("opencode-go/kimi-k2.5", 0.5));
        let view = ladder_view(&models, &catalog);
        let map = cairn_config::allotment::LevelMap::fit([
            point("claude-opus-4.6", 0.8),
            point("kimi-k2.5", 0.5),
        ])
        .expect("claude-opus-4.6 and kimi-k2.5 are distinguishable");
        let opus = view
            .rungs
            .iter()
            .find(|rung| rung.model == "openrouter/anthropic/claude-opus-4.6")
            .unwrap();
        assert_eq!(opus.level, Some(0.8));
        assert_eq!(opus.level_source, Some(LevelSource::Set));
        assert_eq!(opus.band.as_deref(), Some("lg"));
        let hint = opus
            .benchmark
            .as_ref()
            .expect("a hint beside the stored level");
        assert!(hint.summary.contains("solve rate →"), "{}", hint.summary);

        let gpt = gpt_of(&view);
        let placed = gpt.benchmark.as_ref().expect("placed");
        let expected = map.position(rate("gpt-5.5"));
        assert_eq!(placed.position, Some(expected));
        assert_eq!(gpt.band, models.band_of(expected).map(|band| band.id));
        assert!(placed.from.as_deref().unwrap().contains("via your ladder"));
        assert_eq!(view.benchmark_note, None);

        // A band edit moves where a placement falls; the position follows
        // only the calibration rungs.
        let mut moved = models.clone();
        for pool in &mut moved.pools {
            if pool.id == "lg" {
                pool.kind = cairn_config::allotment::PoolKind::Band {
                    min: expected + 0.01,
                };
            }
        }
        let after = gpt_of(&ladder_view(&moved, &catalog));
        assert_eq!(after.benchmark.as_ref().unwrap().position, Some(expected));
        assert_eq!(after.band.as_deref(), Some("md"));

        // Release order: a pooled release shows what the fit measured, a newer
        // release with too few results takes its predecessor's level as a
        // floor, and an older rung names the newest release that replaces it.
        catalog
            .providers
            .retain(|p| p.descriptor.key != "openrouter");
        catalog.providers.push(openrouter(&[
            "anthropic/claude-opus-4.6",
            "anthropic/claude-opus-4.8",
            "anthropic/claude-opus-5.5",
        ]));
        let ordered = ladder_view(&models, &catalog);
        let opus = ordered
            .rungs
            .iter()
            .find(|rung| rung.model == "openrouter/anthropic/claude-opus-4.6")
            .unwrap();
        assert_eq!(
            opus.superseded_by.as_deref(),
            Some("openrouter/anthropic/claude-opus-5.5")
        );
        let hint = opus.benchmark.as_ref().unwrap();
        assert_eq!(hint.pooled_with, vec!["claude-opus-4.7".to_string()]);
        assert!(hint.measured.unwrap() > hint.level, "{hint:?}");
        assert!(
            hint.summary.contains("to keep release order, measured"),
            "{}",
            hint.summary
        );
        let unranked = |id: &str| {
            ordered
                .unranked
                .iter()
                .find(|u| u.model == id)
                .cloned()
                .unwrap()
        };
        let newest = unranked("openrouter/anthropic/claude-opus-5.5");
        let floor = newest.benchmark.as_ref().unwrap();
        assert_eq!(floor.at_least.as_deref(), Some("claude-opus-4.8"));
        let predecessor = unranked("openrouter/anthropic/claude-opus-4.8");
        assert_eq!(
            floor.position,
            predecessor.benchmark.as_ref().unwrap().position
        );
        assert!(floor.summary.contains("at least"), "{}", floor.summary);
        assert!(floor
            .from
            .as_deref()
            .unwrap()
            .starts_with("benchmarks at least "));
        let rendered = render_ladder(&[], || Ok(ordered.clone()));
        assert!(
            rendered.contains("superseded by openrouter/anthropic/claude-opus-5.5"),
            "{rendered}"
        );
        // A floor places but never calibrates: with it and one measured rung,
        // the ladder still has a single calibration point.
        let mut floored = crate::execution::model_selection::tests::models();
        floored
            .ladder
            .push(stored("openrouter/anthropic/claude-opus-5.5", 0.9));
        floored.ladder.push(stored("opencode-go/kimi-k2.5", 0.5));
        assert!(ladder_view(&floored, &catalog)
            .benchmark_calibration
            .is_empty());

        let rendered = render_ladder(&[], || Ok(view.clone()));
        assert!(rendered.contains(&hint.summary), "{rendered}");
        assert!(
            rendered.contains("`openrouter/openai/gpt-5.5` placed by benchmarks at"),
            "{rendered}"
        );
        assert!(rendered.contains("## Benchmark data"), "{rendered}");
        assert!(rendered.contains("CC-BY-SA-4.0"), "{rendered}");
        assert!(rendered.contains("map onto this ladder"), "{rendered}");
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
                let asked: Vec<String> = allotment.unwrap();
                let raw: Vec<&str> = asked.iter().map(String::as_str).collect();
                SelectionPreview {
                    record: run(&raw, &ResolvedConstraints::default(), &windows)
                        .map_err(String::from),
                    asked: PreviewedAllotment::Asked(asked),
                }
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
            SelectionPreview {
                asked: PreviewedAllotment::WorkspaceDefault,
                record: Err("nothing to run".to_string()),
            }
        });
        assert!(
            refused.contains("The workspace's default allotment for session"),
            "{refused}"
        );
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
                .expand(
                    &[AllotmentEntry::Pool("xl".into())],
                    &cairn_config::allotment::unplaced,
                )
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
            .record
            .unwrap_err();
        assert!(refusal.contains("'xl' is not a pool"), "{refusal}");
    }
}
