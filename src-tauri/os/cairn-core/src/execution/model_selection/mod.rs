//! Model selection: which model in an allotment a launch runs on, at what
//! effort, on which account, and why.
//!
//! [`select`] is the one entry point. It expands the allotment against the
//! workspace's pools and ladder, keeps the candidates the catalog serves for
//! the verb (a filter on `serves`; the launch doors still `admit`), asks the
//! [`RequirementResolver`] for a capability, places that on a band, applies
//! node constraints, ranks the band by supply, and sets effort. The result is
//! a [`SelectionRecord`] stored on the job, so a constrained reviewer reads
//! its builder's choice and a resumed job never re-selects.
//!
//! A model named directly is never replaced: when it is the allotment's only
//! candidate and cannot be served, the launch is refused naming why. A refusal
//! whose only cause is supply (every account out of usage, blocked, or out of
//! credits) is [`SelectionRefusal::NoSupply`], which holds the node until
//! supply returns rather than failing it.

pub mod constraints;
pub mod requirement;

use std::cmp::Ordering;
use std::fmt;

use cairn_config::allotment::{
    model_ref, AllotmentEntry, CalibrationPoint, Candidate, ExpandedAllotment, Level, LevelMap,
    LevelSource, ModelsConfig,
};
use cairn_models::interface::{BenchmarkPlacement, Catalog, CatalogProvider};
use cairn_models::supply::{
    self, CapacityRefusal, Shortfall, Supply, SupplyAccount, SupplyInputs, SupplyKey,
};
use cairn_models::types::catalog::VerbKind;
use cairn_models::types::supply::CostClass;
use serde::{Deserialize, Serialize};

use crate::models::{ModelSelection, RuntimeExtras};

pub use constraints::ResolvedConstraints;
pub use requirement::{
    Capability, Requirement, RequirementResolver, RequirementSource, TopOfAllotment,
};

/// One candidate as the selection saw it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RankedCandidate {
    /// `provider/model`.
    pub model: String,
    pub level: Option<f64>,
    /// Where `level` came from; `benchmarks` for a model the shipped
    /// benchmark fit placed because the ladder holds no level for it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub level_source: Option<LevelSource>,
    /// The observed identity of a floating alias, independent of its level.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity_note: Option<String>,
    pub band: Option<String>,
    pub maker: Option<String>,
    /// The supply class, absent when the candidate was set aside before
    /// ranking (outside the band, or excluded by a constraint).
    pub class: Option<CostClass>,
    /// What it costs now, or why it was set aside.
    pub note: String,
}

/// What a selection chose, and everything that explains it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SelectionRecord {
    pub allotment: Vec<AllotmentEntry>,
    pub requirement: Requirement,
    /// The band the choice came from. `None` when nothing in the allotment
    /// is on the ladder.
    pub band: Option<String>,
    /// The band's members in supply order, then every candidate set aside.
    pub candidates: Vec<RankedCandidate>,
    pub chosen: ModelSelection,
    /// The chosen model's maker, which a `differentMakerFrom` reads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maker: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account: Option<SupplyAccount>,
    pub extras: RuntimeExtras,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub relaxations: Vec<String>,
    /// The one line that answers "why this model".
    pub provenance: String,
    /// The work this selection was made for, so a later judgment (a rubric,
    /// an escalation) can replay the same question against it.
    #[serde(default)]
    pub subject: SelectionSubject,
}

impl SelectionRecord {
    /// What was asked for and what was chosen at what effort, without the
    /// supply clause: the account a launch will run on is decided at the
    /// launch, and an agent filing work has no use for it.
    pub fn pick_line(&self) -> String {
        supply::pick_line(
            &self.requirement.note,
            &model_ref(&self.chosen),
            self.extras.reasoning_effort.as_deref(),
        )
    }
}

/// Where a selection was made. Set by the launch that stores the record.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SelectionSubject {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub issue_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_id: Option<String>,
}

/// A launch the selection cannot place.
#[derive(Debug, Clone, PartialEq)]
pub enum SelectionRefusal {
    /// Nothing waiting cures: said so it can be fixed.
    Misconfigured { message: String },
    /// Something could serve this launch but nothing can now.
    NoSupply(CapacityRefusal),
}

impl SelectionRefusal {
    pub fn message(&self) -> &str {
        match self {
            Self::Misconfigured { message } => message,
            Self::NoSupply(refusal) => &refusal.reason,
        }
    }

    /// The same refusal, its text led by `context`.
    pub fn context(self, context: impl fmt::Display) -> Self {
        match self {
            Self::Misconfigured { message } => Self::Misconfigured {
                message: format!("{context}: {message}"),
            },
            Self::NoSupply(mut refusal) => {
                refusal.reason = format!("{context}: {}", refusal.reason);
                Self::NoSupply(refusal)
            }
        }
    }
}

impl fmt::Display for SelectionRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message())
    }
}

impl From<SelectionRefusal> for String {
    fn from(refusal: SelectionRefusal) -> Self {
        refusal.message().to_string()
    }
}

/// Every other launch error is one waiting does not cure.
impl From<String> for SelectionRefusal {
    fn from(message: String) -> Self {
        Self::Misconfigured { message }
    }
}

fn refuse(message: impl Into<String>) -> SelectionRefusal {
    SelectionRefusal::Misconfigured {
        message: message.into(),
    }
}

/// The allotment a node selects within: its own (a recipe node's override, a
/// spawn's `models`), else its agent's, else the workspace default.
pub fn effective_allotment(
    node: Option<&[AllotmentEntry]>,
    agent: &[AllotmentEntry],
    default_models: &[AllotmentEntry],
) -> Vec<AllotmentEntry> {
    match node {
        Some(node) if !node.is_empty() => node.to_vec(),
        _ if !agent.is_empty() => agent.to_vec(),
        _ => default_models.to_vec(),
    }
}

/// What the Models page and `cairn://models/ladder` say when the ladder cannot
/// yet map benchmark levels onto itself.
pub const CALIBRATE_BENCHMARKS: &str = "place two covered models the benchmarks can tell apart (a frontier model and a smaller one) to calibrate benchmark placement";

/// Where the shipped benchmark fit places models on this workspace's ladder.
///
/// A benchmark level is an expected solve rate, not a ladder position, so it
/// is mapped through the ladder's own calibration rungs (entries with both a
/// stored level and a benchmark level) by [`LevelMap`]. Rungs whose benchmark
/// intervals overlap count as one point; with fewer than two distinguishable
/// points nothing is placed, and the raw solve rate is still shown as a hint.
/// A floor (a release placed at its predecessor's level for want of results
/// of its own) places but never calibrates: it was not measured.
pub struct BenchmarkPlacer<'a> {
    catalog: &'a Catalog,
    map: Option<LevelMap>,
}

impl<'a> BenchmarkPlacer<'a> {
    pub fn new(models: &ModelsConfig, catalog: &'a Catalog) -> Self {
        let points = models.ladder.iter().filter_map(|rung| {
            let stored = rung.level.as_ref()?.value;
            let placed = catalog
                .benchmark(&rung.model.backend, rung.model.model.as_str())
                .filter(|placed| !placed.is_floor())?;
            Some(CalibrationPoint {
                at: placed.level,
                low: placed.low,
                high: placed.high,
                level: stored,
            })
        });
        Self {
            catalog,
            map: LevelMap::fit(points),
        }
    }

    /// The map from solve rate to this ladder, when it is calibrated.
    pub fn map(&self) -> Option<&LevelMap> {
        self.map.as_ref()
    }

    /// The raw shipped placement of `model` on `provider`.
    pub fn placement(&self, provider: &str, model: &str) -> Option<BenchmarkPlacement> {
        self.catalog.benchmark(provider, model)
    }

    pub fn identity_note(&self, provider: &str, model: &str) -> Option<String> {
        self.catalog
            .providers
            .iter()
            .find(|on| on.descriptor.key == provider)?
            .identity_note(model)
    }

    /// A solve rate as a position on this ladder, when it is calibrated.
    pub fn position(&self, solve_rate: f64) -> Option<f64> {
        self.map.as_ref().map(|map| map.position(solve_rate))
    }

    /// How a mapped level records where it came from:
    /// `benchmarks 0.655 → 0.82 via your ladder, anchor v1`, then what release
    /// order did to the level when it did anything.
    pub fn from_text(&self, placed: &BenchmarkPlacement) -> Option<String> {
        let position = self.position(placed.level)?;
        let text = format!(
            "benchmarks {}{:.3} → {position:.2} via your ladder, anchor {}",
            if placed.is_floor() { "at least " } else { "" },
            placed.level,
            placed.anchor
        );
        Some(match placed.basis_note() {
            Some(note) => format!("{text}; {note}"),
            None => text,
        })
    }

    /// The placement [`ModelsConfig::expand`] uses for a model with no stored
    /// level: its benchmark solve rate mapped onto this ladder.
    pub fn level(&self, model: &ModelSelection) -> Option<Level> {
        let placed = self.placement(&model.backend, model.model.as_str())?;
        Some(Level {
            value: self.position(placed.level)?,
            source: LevelSource::Benchmarks,
            from: self.from_text(&placed).map(|from| {
                match self.identity_note(&model.backend, model.model.as_str()) {
                    Some(note) => format!("{from}; {note}"),
                    None => from,
                }
            }),
        })
    }
}

/// What selection reads.
pub struct SelectionContext<'a> {
    pub models: &'a ModelsConfig,
    /// Installed providers first, in the workspace's order.
    pub catalog: &'a Catalog,
    pub supply: SupplyInputs<'a>,
}

impl SelectionContext<'_> {
    fn provider(&self, key: &str) -> Option<(usize, &CatalogProvider)> {
        self.catalog
            .providers
            .iter()
            .enumerate()
            .find(|(_, provider)| provider.installed && provider.descriptor.key == key)
    }
}

struct Live<'a> {
    candidate: &'a Candidate,
    provider: &'a CatalogProvider,
    order: usize,
    maker: Option<String>,
}

/// Choose the model `allotment` runs `verb` on.
pub fn select(
    ctx: &SelectionContext<'_>,
    allotment: &[AllotmentEntry],
    verb: VerbKind,
    constraints: &ResolvedConstraints,
    resolver: &dyn RequirementResolver,
) -> Result<SelectionRecord, SelectionRefusal> {
    let names = allotment
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    let placer = BenchmarkPlacer::new(ctx.models, ctx.catalog);
    let expanded = ctx
        .models
        .expand(allotment, &|model| placer.level(model))
        .map_err(refuse)?;
    if expanded.candidates.is_empty() {
        return Err(refuse(format!(
            "[{names}] holds no model: its pools are empty. Add models to them on the Models page, or name a model as provider/model"
        )));
    }

    // 1. Keep what the catalog serves for the verb.
    let mut set_aside: Vec<RankedCandidate> = Vec::new();
    let mut live: Vec<Live<'_>> = Vec::new();
    for candidate in &expanded.candidates {
        let key = candidate.model.backend.as_str();
        let model = candidate.model.model.as_str();
        let why = match (ctx.provider(key), &constraints.fork_provider) {
            (_, Some(fork)) if fork != key => Some(format!(
                "not on {fork}, where the session this launch forks lives"
            )),
            (None, _) => Some(format!("{key} is not installed")),
            (Some((_, provider)), _) => provider.admit(model, verb).err().map(|refusal| {
                refusal
                    .detail
                    .unwrap_or_else(|| format!("does not serve {}", verb.as_str()))
            }),
        };
        match (why, ctx.provider(key)) {
            (None, Some((order, provider))) => live.push(Live {
                candidate,
                provider,
                order,
                maker: provider.maker(model),
            }),
            (why, _) => set_aside.push(aside(candidate, None, why.unwrap_or_default())),
        }
    }
    if live.is_empty() {
        let reasons: Vec<String> = set_aside
            .iter()
            .map(|c| format!("{}: {}", c.model, c.note))
            .collect();
        if let Some(fork) = &constraints.fork_provider {
            return Err(refuse(format!(
                "this launch forks a {fork} session, and [{names}] has no {fork} model that can run {} ({}); add a {fork} model to its models, or start it with a fresh session",
                verb.as_str(),
                reasons.join("; ")
            )));
        }
        return Err(refuse(format!(
            "nothing in [{names}] can run {}: {}",
            verb.as_str(),
            reasons.join("; ")
        )));
    }
    let served = ExpandedAllotment {
        entries: expanded.entries.clone(),
        candidates: live.iter().map(|l| l.candidate.clone()).collect(),
        bands: expanded
            .bands
            .iter()
            .filter(|band| {
                live.iter()
                    .any(|l| l.candidate.band.as_deref() == Some(&band.id))
            })
            .cloned()
            .collect(),
    };

    // 2. The requirement, placed on a band.
    let requirement = resolver.requirement(&served);
    let mut relaxations: Vec<String> = Vec::new();
    let mut band: Option<String> = match requirement.capability {
        Capability::Top => served.top().map(|band| band.id.clone()),
        Capability::At(x) => served.band_at(x).map(|band| band.id.clone()),
    };
    if let Some((node, record)) = &constraints.same_band_as {
        band = same_band(ctx.models, &served, node, record, &mut relaxations);
    }

    // 3. A different maker, relaxing the band to find one. When no band of
    // the allotment holds one, the maker rule gives way and the node selects
    // as if it were absent: a review by the same lab beats no review, and a
    // single-maker workspace must still be able to run a constrained recipe.
    let mut in_band = members(&served, &live, band.as_deref());
    let mut maker = MakerOutcome::Unconstrained;
    if let Some((node, record)) = &constraints.different_maker_from {
        match record.maker.clone() {
            None => {
                relaxations.push(format!(
                    "the maker of {node}'s {} is unknown, so no model can be shown to differ from it",
                    model_ref(&record.chosen)
                ));
            }
            Some(theirs) => {
                let differs = |l: &Live<'_>| l.maker.as_deref().is_some_and(|ours| ours != theirs);
                let here: Vec<&Live<'_>> = in_band.iter().copied().filter(|l| differs(l)).collect();
                let found = if here.is_empty() {
                    let tries: Vec<String> = match &band {
                        Some(from) => served
                            .relaxations_from(from)
                            .iter()
                            .map(|b| b.id.clone())
                            .collect(),
                        None => Vec::new(),
                    };
                    tries.into_iter().find_map(|next| {
                        let mut members = members(&served, &live, Some(&next));
                        members.retain(|l| differs(l));
                        (!members.is_empty()).then_some((Some(next), members))
                    })
                } else {
                    Some((band.clone(), here))
                };
                match found {
                    Some((next, members)) => {
                        if next != band {
                            let from = band.as_deref().unwrap_or("top");
                            relaxations.push(format!(
                                "band relaxed {from}→{} (no model by a maker other than {theirs} in {from} of [{names}])",
                                next.as_deref().unwrap_or("top"),
                            ));
                            band = next;
                        }
                        in_band = members;
                        for l in &live {
                            if !differs(l) {
                                let why = match &l.maker {
                                    Some(_) => format!("same maker as {node} ({theirs})"),
                                    None => {
                                        "maker unknown, so it cannot be shown to differ".to_string()
                                    }
                                };
                                set_aside.push(aside(l.candidate, l.maker.clone(), why));
                            }
                        }
                        maker = MakerOutcome::Differs { node, theirs };
                    }
                    None => {
                        relaxations.push(format!(
                            "no model from a maker other than {theirs} in [{names}]"
                        ));
                        maker = MakerOutcome::Waived { node, theirs };
                    }
                }
            }
        }
    }
    for l in &live {
        let kept = in_band
            .iter()
            .any(|m| m.candidate.model == l.candidate.model);
        let already = set_aside
            .iter()
            .any(|c| c.model == model_ref(&l.candidate.model));
        if !kept && !already {
            set_aside.push(aside(
                l.candidate,
                l.maker.clone(),
                "outside the chosen band".to_string(),
            ));
        }
    }

    // 4. Supply, inside the band.
    let mut ranked: Vec<(&Live<'_>, Result<Supply, Shortfall>)> = in_band
        .iter()
        .map(|l| {
            (
                *l,
                supply::supply_for(l.provider, l.candidate.model.model.as_str(), &ctx.supply),
            )
        })
        .collect();
    ranked.sort_by(|(left, ls), (right, rs)| match (ls, rs) {
        (Ok(ls), Ok(rs)) => supply::compare(&key(left, ls), &key(right, rs)),
        (Ok(_), Err(_)) => Ordering::Less,
        (Err(_), Ok(_)) => Ordering::Greater,
        (Err(_), Err(_)) => left.order.cmp(&right.order),
    });
    let sole_named = expanded.candidates.len() == 1 && expanded.candidates[0].named;
    let (first, first_supply) = &ranked[0];
    if let (true, Err(why)) = (sole_named, first_supply) {
        let model = model_ref(&first.candidate.model);
        if why.supply {
            return Err(SelectionRefusal::NoSupply(CapacityRefusal::of(
                &model,
                format!("{model} cannot run now: {why}"),
                why,
            )));
        }
        return Err(refuse(format!(
            "{model} cannot run now: {why}. A model named directly is never replaced by another; name a pool or another model beside it"
        )));
    }
    // Ranking puts every candidate that can run first, so an unrunnable
    // first means none in the band can. Supply never moves work across
    // bands, so the launch waits for one of these.
    let Ok(first_supply) = first_supply else {
        let shortfalls: Vec<(String, &Shortfall)> = ranked
            .iter()
            .filter_map(|(l, supply)| {
                supply
                    .as_ref()
                    .err()
                    .map(|why| (model_ref(&l.candidate.model), why))
            })
            .collect();
        let reasons = shortfalls
            .iter()
            .map(|(model, why)| format!("{model}: {why}"))
            .collect::<Vec<_>>()
            .join("; ");
        let soonest = shortfalls
            .iter()
            .filter(|(_, why)| why.supply)
            .filter_map(|(_, why)| why.resets_at.map(|at| (at, *why)))
            .min_by_key(|(at, _)| *at)
            .map(|(_, why)| why);
        if shortfalls.iter().any(|(_, why)| why.supply) {
            let reason = format!("nothing in [{names}] can run now: {reasons}");
            return Err(SelectionRefusal::NoSupply(match soonest {
                Some(why) => CapacityRefusal::of(format!("[{names}]"), reason, why),
                None => CapacityRefusal {
                    subject: format!("[{names}]"),
                    reason,
                    retry_at: None,
                    retry_account: None,
                },
            }));
        }
        return Err(refuse(format!(
            "nothing in [{names}] can run {}: {reasons}",
            verb.as_str()
        )));
    };
    let mut candidates: Vec<RankedCandidate> = ranked
        .iter()
        .map(|(l, supply)| RankedCandidate {
            model: model_ref(&l.candidate.model),
            level: l.candidate.level,
            level_source: l.candidate.level_source,
            identity_note: l.provider.identity_note(l.candidate.model.model.as_str()),
            band: l.candidate.band.clone(),
            maker: l.maker.clone(),
            class: supply.as_ref().ok().map(|s| s.cost.class()),
            note: match supply {
                Ok(s) => supply::describe(&l.provider.descriptor.display_name, s),
                Err(why) => why.reason.clone(),
            },
        })
        .collect();
    candidates.extend(set_aside);
    for candidate in &mut candidates {
        if let Some((provider, model)) = candidate.model.split_once('/') {
            candidate.identity_note = placer.identity_note(provider, model);
        }
    }

    // 5. Effort and options. Effort belongs to the work, so it comes from the
    // requirement alone; a ladder entry contributes only its other options.
    let chosen = first.candidate.model.clone();
    let mut extras = first
        .candidate
        .rung
        .as_ref()
        .map(|rung| rung.option_extras())
        .unwrap_or_default();
    let effort_note = match requirement.effort {
        Some(x) => {
            if let Some(effort) = first.provider.effort(chosen.model.as_str(), x) {
                extras.reasoning_effort = effort.reasoning_effort;
            }
            None
        }
        None => Some(PROVIDER_DEFAULT_EFFORT.to_string()),
    };

    let clause = supply::describe(&first.provider.descriptor.display_name, first_supply);
    let mut notes = relaxations.clone();
    let ours = first.maker.as_deref();
    match maker {
        MakerOutcome::Unconstrained => {}
        MakerOutcome::Differs { node, theirs } => notes.push(format!(
            "maker {} differs from {node}'s {theirs}",
            ours.unwrap_or("unknown")
        )),
        MakerOutcome::Waived { node, theirs } => notes.push(match ours {
            Some(ours) if ours == theirs => format!("same maker as {node}"),
            _ => format!("maker unknown, so it may be {node}'s {theirs}"),
        }),
    }
    notes.extend(effort_note);
    if first.candidate.level_source == Some(LevelSource::Benchmarks) {
        if let Some(from) = placer
            .level(&first.candidate.model)
            .and_then(|level| level.from)
        {
            notes.push(format!("level from {from}"));
        }
    }
    if first.candidate.level_source != Some(LevelSource::Benchmarks) {
        if let Some(note) = first.provider.identity_note(chosen.model.as_str()) {
            notes.push(note);
        }
    }
    let provenance = supply::provenance_line(
        &requirement.note,
        &model_ref(&chosen),
        extras.reasoning_effort.as_deref(),
        &clause,
        &notes,
    );
    Ok(SelectionRecord {
        allotment: allotment.to_vec(),
        requirement,
        band,
        candidates,
        maker: first.maker.clone(),
        account: first_supply.account.clone(),
        chosen,
        extras,
        relaxations,
        provenance,
        subject: SelectionSubject::default(),
    })
}

/// The provenance note of a launch whose requirement sets no effort.
pub const PROVIDER_DEFAULT_EFFORT: &str = "effort: provider default";

/// The record a pinned job stores: the pin itself, in the band of its ladder
/// entry (none when it is off the ladder) and with the catalog's maker, so a
/// constraint naming the job reads what it runs on.
pub fn pinned(models: &ModelsConfig, catalog: &Catalog, pin: &ModelSelection) -> SelectionRecord {
    let level = models
        .rung(pin)
        .and_then(|rung| rung.level.clone())
        .or_else(|| BenchmarkPlacer::new(models, catalog).level(pin));
    let level_source = level.as_ref().map(|level| level.source);
    let level = level.map(|level| level.value);
    let band = level
        .and_then(|level| models.band_of(level))
        .map(|band| band.id);
    let maker = catalog
        .providers
        .iter()
        .find(|provider| provider.descriptor.key == pin.backend.as_str())
        .and_then(|provider| provider.maker(pin.model.as_str()));
    let reference = model_ref(pin);
    SelectionRecord {
        allotment: vec![AllotmentEntry::Model(pin.clone())],
        requirement: Requirement {
            capability: Capability::Top,
            effort: None,
            source: RequirementSource::Pinned,
            note: "pinned".to_string(),
        },
        band: band.clone(),
        candidates: vec![RankedCandidate {
            model: reference.clone(),
            level,
            level_source,
            identity_note: BenchmarkPlacer::new(models, catalog)
                .identity_note(&pin.backend, pin.model.as_str()),
            band,
            maker: maker.clone(),
            class: None,
            note: "pinned".to_string(),
        }],
        chosen: pin.clone(),
        maker,
        account: None,
        extras: RuntimeExtras::default(),
        relaxations: Vec::new(),
        provenance: match BenchmarkPlacer::new(models, catalog)
            .identity_note(&pin.backend, pin.model.as_str())
        {
            Some(note) => format!("pinned to {reference}; {note}"),
            None => format!("pinned to {reference}"),
        },
        subject: SelectionSubject::default(),
    }
}

/// What a `differentMakerFrom` did to the choice.
enum MakerOutcome<'a> {
    Unconstrained,
    /// The choice is by another maker than the referenced node's.
    Differs {
        node: &'a str,
        theirs: String,
    },
    /// No band of the allotment held another maker, so the rule gave way.
    Waived {
        node: &'a str,
        theirs: String,
    },
}

fn key<'a>(live: &'a Live<'_>, supply: &'a Supply) -> SupplyKey<'a> {
    SupplyKey {
        supply,
        level: live.candidate.level,
        provider_order: live.order,
        model: live.candidate.model.model.as_str(),
    }
}

fn aside(candidate: &Candidate, maker: Option<String>, note: String) -> RankedCandidate {
    RankedCandidate {
        model: model_ref(&candidate.model),
        level: candidate.level,
        level_source: candidate.level_source,
        identity_note: None,
        band: candidate.band.clone(),
        maker,
        class: None,
        note,
    }
}

fn members<'a, 'b>(
    served: &ExpandedAllotment,
    live: &'b [Live<'a>],
    band: Option<&str>,
) -> Vec<&'b Live<'a>> {
    let in_band = served.in_band(band);
    live.iter()
        .filter(|l| in_band.iter().any(|c| c.model == l.candidate.model))
        .collect()
}

/// The band `record` ran in, or the nearest band of this allotment to it.
fn same_band(
    models: &ModelsConfig,
    served: &ExpandedAllotment,
    node: &str,
    record: &SelectionRecord,
    relaxations: &mut Vec<String>,
) -> Option<String> {
    let theirs = record.band.clone().or_else(|| {
        relaxations.push(format!(
            "{node}'s {} is off the ladder, so sameBandAs reads it as the top",
            model_ref(&record.chosen)
        ));
        None
    });
    let Some(theirs) = theirs else {
        return served.top().map(|band| band.id.clone());
    };
    if served.bands.iter().any(|band| band.id == theirs) {
        return Some(theirs);
    }
    let all = models.bands();
    let index = all.iter().position(|band| band.id == theirs)?;
    let above = all[index + 1..].iter();
    let below = all[..index].iter().rev();
    let nearest = above
        .chain(below)
        .find(|band| served.bands.iter().any(|ours| ours.id == band.id))
        .map(|band| band.id.clone());
    if let Some(nearest) = &nearest {
        relaxations.push(format!(
            "band relaxed {theirs}→{nearest} (this allotment has no {theirs} model to match {node})"
        ));
    }
    nearest.or_else(|| served.top().map(|band| band.id.clone()))
}

#[cfg(test)]
pub(crate) mod tests;
