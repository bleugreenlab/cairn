//! Model pools, the capability ladder, and allotments.
//!
//! A **pool** is a named, described set of models the user owns. A *band*
//! pool is a slice of the **ladder**: every model on the ladder has a level in
//! 0..1, and a band pool holds the entries whose level falls between its own
//! `min` and the next band's. A *set* pool is hand-picked. `sm`, `md` and `lg`
//! are seeded band pools, not built-in meanings: the user may rename,
//! redescribe or delete them. Each pool carries a description because the
//! judgment that later picks within an allotment reads it.
//!
//! An **allotment** is what a definition or recipe node names: any union of
//! pools and specific `provider/model` pairs (`models: [sm, md, lg]`,
//! `models: [lg, claude/fable]`). Selection chooses inside it; a model named
//! directly is a statement of intent and is never replaced by another.
//!
//! This module is configuration only: the shapes, their grammar and
//! validation, how an allotment expands into candidates on bands, and the
//! one-time conversion from the tier x provider preset grid. Choosing among
//! candidates (requirement, constraints, supply) belongs to cairn-core.

use std::collections::{BTreeMap, HashMap};
use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{Model, ModelSelection, Preset, PresetOptionValue, RuntimeExtras};

/// The `models` settings key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelsConfig {
    #[serde(default)]
    pub pools: Vec<Pool>,
    #[serde(default)]
    pub ladder: Vec<Rung>,
    /// The allotment of a definition that names none.
    #[serde(default = "default_default_models")]
    pub default_models: Vec<AllotmentEntry>,
    /// What the one-time conversion from tiers could not carry over exactly,
    /// shown once on the Models page until the user dismisses it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conversion_notes: Vec<String>,
}

fn default_default_models() -> Vec<AllotmentEntry> {
    vec![AllotmentEntry::Pool("md".to_string())]
}

impl Default for ModelsConfig {
    fn default() -> Self {
        Self {
            pools: Vec::new(),
            ladder: Vec::new(),
            default_models: default_default_models(),
            conversion_notes: Vec::new(),
        }
    }
}

/// A named, described set of models.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "PoolFile", into = "PoolFile")]
pub struct Pool {
    pub id: String,
    pub description: String,
    pub kind: PoolKind,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PoolKind {
    /// Every ladder entry whose level is at least `min` and below the next
    /// band's `min` (1.0 inclusive for the highest band).
    Band { min: f64 },
    /// Hand-picked models.
    Set { members: Vec<ModelSelection> },
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PoolFile {
    id: String,
    #[serde(default)]
    description: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    band: Option<BandFile>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    members: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize)]
struct BandFile {
    min: f64,
}

impl TryFrom<PoolFile> for Pool {
    type Error = String;

    fn try_from(file: PoolFile) -> Result<Self, String> {
        let kind = match (file.band, file.members) {
            (Some(band), None) => PoolKind::Band { min: band.min },
            (None, Some(members)) => PoolKind::Set {
                members: members
                    .iter()
                    .map(|member| parse_model_ref(member))
                    .collect::<Result<_, _>>()?,
            },
            (Some(_), Some(_)) => {
                return Err(format!(
                    "pool '{}' has both `band` and `members`; a pool is a band of the ladder or a hand-picked set, not both",
                    file.id
                ))
            }
            (None, None) => {
                return Err(format!(
                    "pool '{}' needs `band: {{ min: .. }}` or `members: [provider/model, ..]`",
                    file.id
                ))
            }
        };
        Ok(Pool {
            id: file.id,
            description: file.description,
            kind,
        })
    }
}

impl From<Pool> for PoolFile {
    fn from(pool: Pool) -> Self {
        let (band, members) = match pool.kind {
            PoolKind::Band { min } => (Some(BandFile { min }), None),
            PoolKind::Set { members } => (None, Some(members.iter().map(model_ref).collect())),
        };
        PoolFile {
            id: pool.id,
            description: pool.description,
            band,
            members,
        }
    }
}

/// One model's place on the ladder and how it launches.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Rung {
    #[serde(
        serialize_with = "serialize_model_ref",
        deserialize_with = "deserialize_model_ref"
    )]
    pub model: ModelSelection,
    /// Capability on 0..1, with where that number came from. `None` keeps the
    /// entry's launch options without placing it on the ladder.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub level: Option<Level>,
    /// The position on this model's own effort scale it launches at, 0..1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort: Option<f64>,
    /// An effort word kept from the tier grid because this model's scale was
    /// not known when it converted. Selection places it on the scale at
    /// launch; when the scale is known and the word is not one of its levels,
    /// the model launches at its provider's default and the provenance says
    /// so. `effort`, when set, wins over it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort_level: Option<String>,
    /// Launch options other than effort (`serviceTier`, `fastMode`).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub options: BTreeMap<String, PresetOptionValue>,
}

impl Rung {
    /// The non-effort launch options as the runtime bag a launch reads.
    pub fn option_extras(&self) -> RuntimeExtras {
        Preset {
            model: self.model.model.clone(),
            options: self
                .options
                .iter()
                .filter(|(key, _)| key.as_str() != EFFORT_OPTION)
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        }
        .to_extras()
    }
}

const EFFORT_OPTION: &str = "reasoningEffort";

/// A model's position on the ladder and its provenance.
///
/// A record rather than a bare number so a position can say whether a person
/// set it, Cairn seeded it, or a measurement put it there, and so later
/// refinements (a per-kind adjustment, a calibration from escalations) extend
/// the record instead of reshaping the ladder. In YAML a bare number is a
/// level the operator set.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(from = "LevelFile", into = "LevelFile")]
pub struct Level {
    pub value: f64,
    pub source: LevelSource,
    /// What the number was derived from, in words ("converted from the md
    /// tier"). Required in spirit for anything but [`LevelSource::Set`].
    pub from: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LevelSource {
    /// A person chose it.
    #[default]
    Set,
    /// Cairn proposed it as a starting point.
    Seeded,
    /// Measured from outcomes.
    Measured,
}

impl Level {
    pub fn set(value: f64) -> Self {
        Self {
            value,
            source: LevelSource::Set,
            from: None,
        }
    }

    pub fn seeded(value: f64, from: impl Into<String>) -> Self {
        Self {
            value,
            source: LevelSource::Seeded,
            from: Some(from.into()),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum LevelFile {
    Bare(f64),
    Sourced {
        value: f64,
        #[serde(default)]
        source: LevelSource,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        from: Option<String>,
    },
}

impl From<LevelFile> for Level {
    fn from(file: LevelFile) -> Self {
        match file {
            LevelFile::Bare(value) => Level::set(value),
            LevelFile::Sourced {
                value,
                source,
                from,
            } => Level {
                value,
                source,
                from,
            },
        }
    }
}

impl From<Level> for LevelFile {
    fn from(level: Level) -> Self {
        match (level.source, level.from) {
            (LevelSource::Set, None) => LevelFile::Bare(level.value),
            (source, from) => LevelFile::Sourced {
                value: level.value,
                source,
                from,
            },
        }
    }
}

/// One entry of an allotment: a pool, or one model on one provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AllotmentEntry {
    Pool(String),
    Model(ModelSelection),
}

impl AllotmentEntry {
    /// Parse one entry. An entry with a `/` is `provider/model`, split at the
    /// first `/` (so `openrouter/anthropic/claude-sonnet-4.5` names an
    /// OpenRouter model); one without is a pool id. Whether the pool exists is
    /// [`ModelsConfig::check_allotment`]'s question.
    pub fn parse(raw: &str) -> Result<Self, String> {
        let raw = raw.trim();
        if raw.is_empty() {
            return Err("an allotment entry is empty".to_string());
        }
        if raw.contains('/') {
            parse_model_ref(raw).map(Self::Model)
        } else {
            Ok(Self::Pool(raw.to_string()))
        }
    }
}

impl fmt::Display for AllotmentEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pool(id) => f.write_str(id),
            Self::Model(selection) => f.write_str(&model_ref(selection)),
        }
    }
}

impl Serialize for AllotmentEntry {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for AllotmentEntry {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        Self::parse(&raw).map_err(serde::de::Error::custom)
    }
}

/// The definition keys `models` replaced. Each is refused by name rather than
/// ignored, because an ignored `tier: lg` would quietly run on the workspace
/// default.
pub const RETIRED_MODEL_KEYS: &[&str] = &["tier", "model", "backend"];

/// The instruction for a definition that still names a retired model key, or
/// `None` when `key` is not one.
pub fn retired_model_key(key: &str) -> Option<String> {
    let instruction = match key {
        "tier" => "`tier:` was replaced by `models: [...]`, a list of pools (sm, md, lg) and provider/model pairs",
        "model" => "`model:` was replaced by `models: [...]`; name a model as provider/model (for example claude/opus), or name pools",
        "backend" => "`backend:` was retired; to run on one provider, name its model in `models: [provider/model]`",
        _ => return None,
    };
    Some(format!(
        "{instruction}; see docs/backends.md#model-selection"
    ))
}

/// A recipe node's constraints on its own selection, each naming an upstream
/// agent node whose stored selection it reads.
///
/// `differentMakerFrom` is absolute: a model by the same maker, or by a maker
/// that is unknown (a difference that cannot be proven), is never chosen.
/// `sameBandAs` may be relaxed within the node's allotment, to the nearest
/// band above and then below, and every relaxation is written into provenance.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeConstraints {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub same_band_as: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub different_maker_from: Option<String>,
}

impl NodeConstraints {
    pub fn is_empty(&self) -> bool {
        self.same_band_as.is_none() && self.different_maker_from.is_none()
    }

    /// Every node this one depends on, each once.
    pub fn references(&self) -> Vec<&str> {
        let mut nodes: Vec<&str> = Vec::new();
        for node in [&self.same_band_as, &self.different_maker_from]
            .into_iter()
            .flatten()
        {
            if !nodes.contains(&node.as_str()) {
                nodes.push(node);
            }
        }
        nodes
    }

    /// Refuse a constraint naming anything but an upstream agent node.
    pub fn check_references(
        &self,
        node: &str,
        is_upstream_agent: impl Fn(&str) -> bool,
    ) -> Result<(), String> {
        for (name, target) in [
            ("sameBandAs", &self.same_band_as),
            ("differentMakerFrom", &self.different_maker_from),
        ] {
            if let Some(target) = target {
                if target == node || !is_upstream_agent(target) {
                    return Err(format!(
                        "node '{node}': {name} names '{target}', which is not an agent node upstream of it; a constraint can only read a model already chosen"
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Parse an authored allotment. An empty list is refused: it would select
/// nothing, and "use the workspace default" is said by omitting the key.
pub fn parse_allotment<S: AsRef<str>>(raw: &[S]) -> Result<Vec<AllotmentEntry>, String> {
    if raw.is_empty() {
        return Err(
            "`models` is empty; name at least one pool or provider/model, or omit the key to use the workspace default"
                .to_string(),
        );
    }
    raw.iter()
        .map(|entry| AllotmentEntry::parse(entry.as_ref()))
        .collect()
}

/// `provider/model`, split at the first `/`.
pub fn parse_model_ref(raw: &str) -> Result<ModelSelection, String> {
    match raw.trim().split_once('/') {
        Some((provider, model)) if !provider.is_empty() && !model.is_empty() => Ok(
            ModelSelection::new(provider.to_string(), Model::new(model.to_string())),
        ),
        _ => Err(format!(
            "'{raw}' is not provider/model (for example claude/opus or openrouter/anthropic/claude-sonnet-4.5)"
        )),
    }
}

pub fn model_ref(selection: &ModelSelection) -> String {
    format!("{}/{}", selection.backend, selection.model.as_str())
}

fn serialize_model_ref<S: Serializer>(
    selection: &ModelSelection,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&model_ref(selection))
}

fn deserialize_model_ref<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<ModelSelection, D::Error> {
    let raw = String::deserialize(deserializer)?;
    parse_model_ref(&raw).map_err(serde::de::Error::custom)
}

/// One band of the ladder.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Band {
    pub id: String,
    pub min: f64,
    /// The next band's `min`, or 1.0 for the highest band.
    pub max: f64,
}

impl Band {
    pub fn centre(&self) -> f64 {
        round2(self.min + (self.max - self.min) / 2.0)
    }
}

fn round2(x: f64) -> f64 {
    (x * 100.0).round() / 100.0
}

/// One model an allotment can run, where it sits, and how it launches.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Candidate {
    #[serde(serialize_with = "serialize_model_ref")]
    pub model: ModelSelection,
    /// Its ladder level, when it has one.
    pub level: Option<f64>,
    /// The band its level falls in. `None` for a model off the ladder, which
    /// counts as the top of the allotment that names it.
    pub band: Option<String>,
    /// Named directly in the allotment rather than reached through a pool.
    pub named: bool,
    #[serde(skip)]
    pub rung: Option<Rung>,
}

/// An allotment expanded against the workspace's pools and ladder.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExpandedAllotment {
    pub entries: Vec<AllotmentEntry>,
    /// Every model the allotment can run, each once, highest level first and
    /// models off the ladder ahead of all of them.
    pub candidates: Vec<Candidate>,
    /// The bands its laddered candidates occupy, lowest first.
    pub bands: Vec<Band>,
}

impl ExpandedAllotment {
    /// The allotment's highest band, `None` when no candidate is on the
    /// ladder (every candidate is then the top).
    pub fn top(&self) -> Option<&Band> {
        self.bands.last()
    }

    /// The lowest band whose upper bound reaches `x`, else the top band.
    pub fn band_at(&self, x: f64) -> Option<&Band> {
        self.bands
            .iter()
            .find(|band| band.max >= x)
            .or_else(|| self.top())
    }

    /// The candidates in `band` (an allotment band id, or `None` for the
    /// top when nothing is laddered). Models off the ladder belong to the top.
    pub fn in_band(&self, band: Option<&str>) -> Vec<&Candidate> {
        let top = self.top().map(|top| top.id.as_str());
        self.candidates
            .iter()
            .filter(|candidate| match (&candidate.band, band) {
                (Some(own), Some(band)) => own == band,
                (None, band) => band == top,
                (Some(_), None) => false,
            })
            .collect()
    }

    /// The band ids to try after `band` when nothing in it satisfies a
    /// constraint: each nearer band above, then each nearer band below.
    pub fn relaxations_from(&self, band: &str) -> Vec<&Band> {
        let Some(index) = self.bands.iter().position(|known| known.id == band) else {
            return Vec::new();
        };
        let above = self.bands[index + 1..].iter();
        let below = self.bands[..index].iter().rev();
        above.chain(below).collect()
    }
}

impl ModelsConfig {
    pub fn pool(&self, id: &str) -> Option<&Pool> {
        self.pools.iter().find(|pool| pool.id == id)
    }

    /// The band pools, lowest first, each bounded above by the next.
    pub fn bands(&self) -> Vec<Band> {
        let mut mins: Vec<(&str, f64)> = self
            .pools
            .iter()
            .filter_map(|pool| match pool.kind {
                PoolKind::Band { min } => Some((pool.id.as_str(), min)),
                PoolKind::Set { .. } => None,
            })
            .collect();
        mins.sort_by(|left, right| left.1.total_cmp(&right.1));
        mins.iter()
            .enumerate()
            .map(|(index, (id, min))| Band {
                id: id.to_string(),
                min: *min,
                max: mins.get(index + 1).map_or(1.0, |next| next.1),
            })
            .collect()
    }

    /// The band `level` falls in. A level below the lowest band's `min` is
    /// in the lowest band.
    pub fn band_of(&self, level: f64) -> Option<Band> {
        let bands = self.bands();
        bands
            .iter()
            .rev()
            .find(|band| level >= band.min)
            .or_else(|| bands.first())
            .cloned()
    }

    pub fn rung(&self, model: &ModelSelection) -> Option<&Rung> {
        self.ladder.iter().find(|rung| rung.model == *model)
    }

    /// Refuse an allotment naming a pool this workspace does not have.
    pub fn check_allotment(&self, allotment: &[AllotmentEntry]) -> Result<(), String> {
        if allotment.is_empty() {
            return parse_allotment::<&str>(&[]).map(|_| ());
        }
        for entry in allotment {
            if let AllotmentEntry::Pool(id) = entry {
                if self.pool(id).is_none() {
                    let known: Vec<&str> = self.pools.iter().map(|pool| pool.id.as_str()).collect();
                    return Err(format!(
                        "'{id}' is not a pool in this workspace (pools: {}); name a model as provider/model, for example claude/{id}",
                        if known.is_empty() { "none".to_string() } else { known.join(", ") }
                    ));
                }
            }
        }
        Ok(())
    }

    /// Expand an allotment into its candidates and bands.
    pub fn expand(&self, allotment: &[AllotmentEntry]) -> Result<ExpandedAllotment, String> {
        self.check_allotment(allotment)?;
        let mut candidates: Vec<Candidate> = Vec::new();
        let mut add = |model: &ModelSelection, named: bool| {
            if let Some(existing) = candidates.iter_mut().find(|known| known.model == *model) {
                existing.named |= named;
                return;
            }
            let rung = self.rung(model).cloned();
            let level = rung
                .as_ref()
                .and_then(|rung| rung.level.as_ref())
                .map(|level| level.value);
            candidates.push(Candidate {
                model: model.clone(),
                level,
                band: level
                    .and_then(|level| self.band_of(level))
                    .map(|band| band.id),
                named,
                rung,
            });
        };
        let bands = self.bands();
        for entry in allotment {
            match entry {
                AllotmentEntry::Model(model) => add(model, true),
                AllotmentEntry::Pool(id) => match &self.pool(id).expect("checked").kind {
                    PoolKind::Set { members } => members.iter().for_each(|model| add(model, false)),
                    PoolKind::Band { .. } => {
                        for rung in &self.ladder {
                            let in_band = rung
                                .level
                                .as_ref()
                                .and_then(|level| self.band_of(level.value))
                                .is_some_and(|band| band.id == *id);
                            if in_band {
                                add(&rung.model, false);
                            }
                        }
                    }
                },
            }
        }
        candidates.sort_by(|left, right| match (left.level, right.level) {
            (None, None) => std::cmp::Ordering::Equal,
            (None, Some(_)) => std::cmp::Ordering::Less,
            (Some(_), None) => std::cmp::Ordering::Greater,
            (Some(left), Some(right)) => right.total_cmp(&left),
        });
        let occupied: Vec<Band> = bands
            .into_iter()
            .filter(|band| {
                candidates
                    .iter()
                    .any(|c| c.band.as_deref() == Some(&band.id))
            })
            .collect();
        Ok(ExpandedAllotment {
            entries: allotment.to_vec(),
            candidates,
            bands: occupied,
        })
    }

    /// Refuse a config whose pools or ladder cannot be read one way.
    pub fn validate(&self) -> Result<(), String> {
        let mut ids: Vec<&str> = Vec::new();
        for pool in &self.pools {
            let id = pool.id.trim();
            if id.is_empty() || id.contains('/') || id.contains(char::is_whitespace) {
                return Err(format!(
                    "pool id '{}' must be a non-empty word with no '/' or spaces",
                    pool.id
                ));
            }
            if ids.contains(&id) {
                return Err(format!("two pools are named '{id}'"));
            }
            ids.push(id);
            if let PoolKind::Band { min } = pool.kind {
                if !(0.0..1.0).contains(&min) {
                    return Err(format!(
                        "band '{id}' starts at {min}; a band's min is in 0..1"
                    ));
                }
            }
        }
        let bands = self.bands();
        for pair in bands.windows(2) {
            if pair[0].min == pair[1].min {
                return Err(format!(
                    "bands '{}' and '{}' both start at {}",
                    pair[0].id, pair[1].id, pair[0].min
                ));
            }
        }
        let mut models: Vec<&ModelSelection> = Vec::new();
        for rung in &self.ladder {
            if models.contains(&&rung.model) {
                return Err(format!("{} is on the ladder twice", model_ref(&rung.model)));
            }
            models.push(&rung.model);
            for (what, value) in [
                ("level", rung.level.as_ref().map(|level| level.value)),
                ("effort", rung.effort),
            ] {
                if let Some(value) = value {
                    if !(0.0..=1.0).contains(&value) {
                        return Err(format!(
                            "{}'s {what} is {value}; it is a position in 0..1",
                            model_ref(&rung.model)
                        ));
                    }
                }
            }
        }
        self.check_allotment(&self.default_models)
            .map_err(|error| format!("defaultModels: {error}"))
    }

    /// Lay a project overlay over this workspace config: pools merge by id
    /// and ladder entries by model, the overlay winning; a `defaultModels`
    /// the overlay sets replaces the workspace's.
    pub fn overlay(&mut self, overlay: &ModelsOverlay) {
        for pool in &overlay.pools {
            match self.pools.iter_mut().find(|known| known.id == pool.id) {
                Some(known) => *known = pool.clone(),
                None => self.pools.push(pool.clone()),
            }
        }
        for rung in &overlay.ladder {
            match self
                .ladder
                .iter_mut()
                .find(|known| known.model == rung.model)
            {
                Some(known) => *known = rung.clone(),
                None => self.ladder.push(rung.clone()),
            }
        }
        if let Some(default_models) = &overlay.default_models {
            self.default_models = default_models.clone();
        }
    }

    /// Drop every ladder entry and set-pool member on `provider`, returning
    /// what was dropped as `provider/model` strings.
    pub fn drop_provider(&mut self, provider: &str) -> Vec<String> {
        let mut dropped: Vec<String> = Vec::new();
        self.ladder.retain(|rung| {
            let keep = rung.model.backend != provider;
            if !keep {
                dropped.push(model_ref(&rung.model));
            }
            keep
        });
        for pool in &mut self.pools {
            if let PoolKind::Set { members } = &mut pool.kind {
                members.retain(|model| {
                    let keep = model.backend != provider;
                    if !keep && !dropped.contains(&model_ref(model)) {
                        dropped.push(model_ref(model));
                    }
                    keep
                });
            }
        }
        dropped
    }
}

/// A project's `models` key: additions and replacements over the workspace's.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelsOverlay {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pools: Vec<Pool>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ladder: Vec<Rung>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_models: Option<Vec<AllotmentEntry>>,
}

impl ModelsOverlay {
    pub fn is_empty(&self) -> bool {
        self.pools.is_empty() && self.ladder.is_empty() && self.default_models.is_none()
    }

    /// The overlay that turns `base` into `target` under
    /// [`ModelsConfig::overlay`]: every pool and ladder entry that differs or
    /// is new. An overlay cannot delete, so an entry `base` has and `target`
    /// lacks comes back with its level cleared, which takes it off the ladder
    /// (out of every band) while keeping how it launches.
    pub fn between(base: &ModelsConfig, target: &ModelsConfig) -> Self {
        let pools = target
            .pools
            .iter()
            .filter(|pool| base.pool(&pool.id) != Some(*pool))
            .cloned()
            .collect();
        let mut ladder: Vec<Rung> = target
            .ladder
            .iter()
            .filter(|rung| base.rung(&rung.model) != Some(*rung))
            .cloned()
            .collect();
        for rung in &base.ladder {
            if rung.level.is_some() && target.rung(&rung.model).is_none() {
                ladder.push(Rung {
                    level: None,
                    ..rung.clone()
                });
            }
        }
        let default_models =
            (base.default_models != target.default_models).then(|| target.default_models.clone());
        Self {
            pools,
            ladder,
            default_models,
        }
    }
}

// === Seeded pools ===

/// The pools a workspace starts with, and that the tiers of the same names
/// convert to. Seeds only: the user owns them from the first save.
pub const SEED_BANDS: &[(&str, &str)] = &[
    (
        "sm",
        "Mechanical or local work: lookups, exploration, summaries, single-file edits.",
    ),
    (
        "md",
        "Routine engineering: a feature or fix in a defined scope, across a few files.",
    ),
    (
        "lg",
        "Open-ended or high-stakes work: planning, architecture, review, subtle debugging, migrations.",
    ),
];

// === One-time conversion from the tier x provider grid ===

/// The retired tier configuration, as the effective (shipped defaults merged
/// with the user's file) grid a workspace resolved against.
pub struct LegacyTiers<'a> {
    pub tiers: &'a [String],
    pub tier_defaults: &'a HashMap<String, String>,
    pub backends: &'a HashMap<String, HashMap<String, Preset>>,
}

/// The outcome of converting a tier grid.
#[derive(Debug, Clone, PartialEq)]
pub struct Conversion {
    pub models: ModelsConfig,
    /// The converted `decisionModel`, when there was one.
    pub decision_model: Option<String>,
    /// Everything that did not carry over exactly, in plain sentences.
    pub notes: Vec<String>,
}

/// Where an effort word from the tier grid falls on its model's own scale.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum EffortPlacement {
    /// A level of the model's scale, at this 0..1 position.
    At(f64),
    /// The model's scale is known and the word is not one of its levels.
    NotALevel,
    /// The model's scale is not known: a provider whose levels come per model
    /// from discovery, before discovery has listed this model, or a model that
    /// takes no effort at all.
    ScaleUnknown,
}

/// Places an effort word on its model's scale. Supplied by the caller (the
/// catalog's `CatalogProvider::effort_scale`), because this crate cannot see
/// discovery.
pub type PositionFn<'a> = &'a dyn Fn(&ModelSelection, &str) -> EffortPlacement;

/// Convert a tier grid into pools and ladder entries.
///
/// - Each seeded tier present (`sm`, `md`, `lg`) becomes a band pool starting
///   at its index over the number present, carrying the seeded description.
/// - Each other tier becomes a set pool of its cells on installed providers.
/// - Every cell of a seeded tier on an installed provider becomes a ladder
///   entry at its band's centre; a model in several tiers keeps the highest
///   tier's level and options.
/// - A cell's `reasoningEffort` becomes a position on that model's scale; a
///   level off the scale (a mode, or a word the model does not use) becomes
///   the top, 1.0. Other options move across unchanged.
/// - A `decisionModel` naming a tier becomes that pool; `provider/tier`
///   becomes the model of that cell.
pub fn convert_tiers(
    legacy: &LegacyTiers<'_>,
    enabled: &[String],
    decision_model: Option<&str>,
    position: PositionFn<'_>,
) -> Conversion {
    let mut notes: Vec<String> = Vec::new();
    let seeded: Vec<(&str, &str)> = SEED_BANDS
        .iter()
        .copied()
        .filter(|(id, _)| legacy.tiers.iter().any(|tier| tier == id))
        .collect();
    let mut pools: Vec<Pool> = seeded
        .iter()
        .enumerate()
        .map(|(index, (id, description))| Pool {
            id: id.to_string(),
            description: description.to_string(),
            kind: PoolKind::Band {
                min: round2(index as f64 / seeded.len() as f64),
            },
        })
        .collect();
    let shell = ModelsConfig {
        pools: pools.clone(),
        ..ModelsConfig::default()
    };
    let bands = shell.bands();

    let cell = |backend: &str, tier: &str| -> Option<&Preset> {
        legacy
            .backends
            .get(backend)
            .and_then(|tiers| tiers.get(tier))
    };
    // A cell's effort as a position on its model's scale, or as the word
    // itself when that scale is not known yet.
    let effort_of = |selection: &ModelSelection,
                     preset: &Preset,
                     notes: &mut Vec<String>|
     -> (Option<f64>, Option<String>) {
        let Some(level) = preset
            .options
            .get(EFFORT_OPTION)
            .and_then(|value| value.as_str())
        else {
            return (None, None);
        };
        match position(selection, level) {
            // The word, not its position: a scale known before discovery may
            // grow once discovery lists the model (Codex's static four become
            // six), and a stored position would then name a higher level.
            // The word is placed on the model's own scale at each launch.
            EffortPlacement::At(_) => (None, Some(level.to_string())),
            EffortPlacement::NotALevel => {
                notes.push(format!(
                    "{}: effort '{level}' is not one of this model's levels, so it now launches at the top of its scale",
                    model_ref(selection)
                ));
                (Some(1.0), None)
            }
            // An unknown scale is not evidence the word is off it, so the
            // word is neither pushed to the top (which would silently raise
            // cost once discovery lists the model's levels) nor dropped: it
            // is kept and placed when the model launches.
            EffortPlacement::ScaleUnknown => {
                notes.push(format!(
                    "{}: effort '{level}' is kept as a word because this model's levels were not known when the conversion ran; it is placed on them when the model launches, or the model launches at its provider's default effort if it is not one of them",
                    model_ref(selection)
                ));
                (None, Some(level.to_string()))
            }
        }
    };
    let options_of = |preset: &Preset| -> BTreeMap<String, PresetOptionValue> {
        preset
            .options
            .iter()
            .filter(|(key, _)| key.as_str() != EFFORT_OPTION)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    };

    // Lowest tier first, so a higher tier's cell replaces a lower one's.
    let mut ladder: Vec<(usize, Rung, Vec<String>)> = Vec::new();
    for (rank, band) in bands.iter().enumerate() {
        for backend in enabled {
            let Some(preset) = cell(backend, &band.id) else {
                continue;
            };
            let selection = ModelSelection::new(backend.clone(), preset.model.clone());
            let mut effort_notes = Vec::new();
            let (effort, effort_level) = effort_of(&selection, preset, &mut effort_notes);
            let rung = Rung {
                model: selection.clone(),
                level: Some(Level::seeded(
                    band.centre(),
                    format!("converted from the {} tier", band.id),
                )),
                effort,
                effort_level,
                options: options_of(preset),
            };
            let here = format!("{backend}/{}", band.id);
            match ladder
                .iter_mut()
                .find(|(_, known, _)| known.model == selection)
            {
                Some(existing) if rank > existing.0 => {
                    let previous = format!("{backend}/{}", bands[existing.0].id);
                    existing.2.push(previous);
                    existing.0 = rank;
                    existing.1 = rung;
                    notes.extend(effort_notes);
                }
                Some(existing) => existing.2.push(here),
                None => {
                    ladder.push((rank, rung, Vec::new()));
                    notes.extend(effort_notes);
                }
            }
        }
    }
    for (rank, rung, others) in &ladder {
        if !others.is_empty() {
            notes.push(format!(
                "{} was in several tiers; it sits in {} and keeps that cell's options (also in {})",
                model_ref(&rung.model),
                bands[*rank].id,
                others.join(", ")
            ));
        }
    }
    for band in &bands {
        let providers: Vec<&String> = enabled
            .iter()
            .filter(|backend| cell(backend, &band.id).is_some())
            .collect();
        if let Some(default) = legacy.tier_defaults.get(&band.id) {
            if providers.len() > 1 && providers.contains(&default) {
                notes.push(format!(
                    "{} defaulted to {default}; its models on every installed provider now share the {} band, and the one with the cheapest supply right now runs",
                    band.id, band.id
                ));
            }
        }
    }

    for tier in legacy.tiers {
        if seeded.iter().any(|(id, _)| id == tier) {
            continue;
        }
        let members: Vec<ModelSelection> = enabled
            .iter()
            .filter_map(|backend| {
                cell(backend, tier)
                    .map(|preset| ModelSelection::new(backend.clone(), preset.model.clone()))
            })
            .collect();
        if members.is_empty() {
            notes.push(format!(
                "the '{tier}' tier had no model on an installed provider, so its pool is empty"
            ));
        }
        for backend in enabled {
            if let Some(preset) = cell(backend, tier) {
                let selection = ModelSelection::new(backend.clone(), preset.model.clone());
                let has_options =
                    preset.options.contains_key(EFFORT_OPTION) || !options_of(preset).is_empty();
                if has_options && !ladder.iter().any(|(_, rung, _)| rung.model == selection) {
                    let mut effort_notes = Vec::new();
                    let (effort, effort_level) = effort_of(&selection, preset, &mut effort_notes);
                    notes.extend(effort_notes);
                    ladder.push((
                        usize::MAX,
                        Rung {
                            model: selection,
                            level: None,
                            effort,
                            effort_level,
                            options: options_of(preset),
                        },
                        Vec::new(),
                    ));
                }
            }
        }
        pools.push(Pool {
            id: tier.clone(),
            description: format!("Converted from the custom '{tier}' tier."),
            kind: PoolKind::Set { members },
        });
    }

    let decision_model = decision_model.map(|raw| {
        let converted = convert_decision_model(raw, legacy, &pools);
        if converted != raw {
            notes.push(format!("decisionModel '{raw}' is now '{converted}'"));
        }
        converted
    });

    let default_models = if pools.iter().any(|pool| pool.id == "md") {
        default_default_models()
    } else {
        bands
            .last()
            .map(|band| band.id.clone())
            .or_else(|| pools.first().map(|pool| pool.id.clone()))
            .map(|id| vec![AllotmentEntry::Pool(id)])
            .unwrap_or_else(default_default_models)
    };

    Conversion {
        models: ModelsConfig {
            pools,
            ladder: ladder.into_iter().map(|(_, rung, _)| rung).collect(),
            default_models,
            conversion_notes: notes.clone(),
        },
        decision_model,
        notes,
    }
}

/// A `decisionModel` in the vocabulary of `pools`: a pool id stays, a
/// `provider/tier` becomes that cell's model, anything else is unchanged.
pub fn convert_decision_model(raw: &str, legacy: &LegacyTiers<'_>, pools: &[Pool]) -> String {
    if pools.iter().any(|pool| pool.id == raw) {
        return raw.to_string();
    }
    if let Some((backend, tier)) = raw.split_once('/') {
        if legacy.tiers.iter().any(|known| known == tier) {
            if let Some(preset) = legacy
                .backends
                .get(backend)
                .and_then(|tiers| tiers.get(tier))
            {
                return format!("{backend}/{}", preset.model.as_str());
            }
        }
    }
    raw.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pool(id: &str, min: f64) -> Pool {
        Pool {
            id: id.into(),
            description: format!("{id} work"),
            kind: PoolKind::Band { min },
        }
    }

    fn rung(model: &str, level: f64) -> Rung {
        Rung {
            model: parse_model_ref(model).unwrap(),
            level: Some(Level::set(level)),
            effort: None,
            effort_level: None,
            options: BTreeMap::new(),
        }
    }

    fn config() -> ModelsConfig {
        ModelsConfig {
            pools: vec![
                pool("lg", 0.67),
                pool("sm", 0.0),
                pool("md", 0.34),
                Pool {
                    id: "myth".into(),
                    description: "the strongest".into(),
                    kind: PoolKind::Set {
                        members: vec![parse_model_ref("claude/fable").unwrap()],
                    },
                },
            ],
            ladder: vec![
                rung("claude/haiku", 0.17),
                rung("codex/gpt-5.6-luna", 0.2),
                rung("claude/sonnet", 0.5),
                rung("opencode-go/glm-5.3", 0.45),
                rung("claude/opus", 0.83),
                rung("codex/gpt-5.6-sol", 0.9),
            ],
            ..ModelsConfig::default()
        }
    }

    fn entries(raw: &[&str]) -> Vec<AllotmentEntry> {
        parse_allotment(raw).unwrap()
    }

    #[test]
    fn grammar_splits_at_the_first_slash_and_refuses_empties() {
        assert_eq!(
            AllotmentEntry::parse("openrouter/anthropic/claude-sonnet-4.5").unwrap(),
            AllotmentEntry::Model(ModelSelection::new(
                "openrouter",
                Model::new("anthropic/claude-sonnet-4.5")
            ))
        );
        assert_eq!(
            AllotmentEntry::parse("lg").unwrap(),
            AllotmentEntry::Pool("lg".into())
        );
        assert!(AllotmentEntry::parse("claude/").is_err());
        assert!(AllotmentEntry::parse("/opus").is_err());
        assert!(parse_allotment::<&str>(&[])
            .unwrap_err()
            .contains("omit the key"));
    }

    #[test]
    fn an_unknown_pool_or_bare_model_is_refused_with_the_pools_named() {
        let error = config().check_allotment(&entries(&["sonnet"])).unwrap_err();
        assert!(error.contains("'sonnet' is not a pool"), "{error}");
        assert!(error.contains("sm, md"), "{error}");
        assert!(error.contains("claude/sonnet"), "{error}");
        config()
            .check_allotment(&entries(&["sm", "claude/fable"]))
            .unwrap();
    }

    #[test]
    fn bands_are_ordered_by_min_and_bounded_by_the_next() {
        let bands = config().bands();
        let shape: Vec<(&str, f64, f64)> = bands
            .iter()
            .map(|b| (b.id.as_str(), b.min, b.max))
            .collect();
        assert_eq!(
            shape,
            [("sm", 0.0, 0.34), ("md", 0.34, 0.67), ("lg", 0.67, 1.0)]
        );
        assert_eq!(config().band_of(0.34).unwrap().id, "md");
        assert_eq!(config().band_of(0.3399).unwrap().id, "sm");
        assert_eq!(config().band_of(1.0).unwrap().id, "lg");
    }

    #[test]
    fn a_band_pool_expands_to_its_ladder_slice() {
        let expanded = config().expand(&entries(&["md"])).unwrap();
        let models: Vec<String> = expanded
            .candidates
            .iter()
            .map(|c| model_ref(&c.model))
            .collect();
        assert_eq!(models, ["claude/sonnet", "opencode-go/glm-5.3"]);
        assert_eq!(expanded.bands.len(), 1);
    }

    #[test]
    fn a_model_off_the_ladder_is_the_top_of_its_allotment() {
        let expanded = config().expand(&entries(&["sm", "md", "myth"])).unwrap();
        assert_eq!(expanded.top().unwrap().id, "md");
        let top: Vec<String> = expanded
            .in_band(Some("md"))
            .iter()
            .map(|c| model_ref(&c.model))
            .collect();
        assert_eq!(
            top,
            ["claude/fable", "claude/sonnet", "opencode-go/glm-5.3"]
        );
        let alone = config().expand(&entries(&["claude/fable"])).unwrap();
        assert!(alone.top().is_none());
        assert_eq!(alone.in_band(None).len(), 1);
        assert!(alone.candidates[0].named);
    }

    #[test]
    fn placement_at_x_takes_the_lowest_band_reaching_it() {
        let expanded = config().expand(&entries(&["sm", "md", "lg"])).unwrap();
        assert_eq!(expanded.band_at(0.0).unwrap().id, "sm");
        assert_eq!(expanded.band_at(0.34).unwrap().id, "sm");
        assert_eq!(expanded.band_at(0.35).unwrap().id, "md");
        assert_eq!(expanded.band_at(1.0).unwrap().id, "lg");
        // An allotment without the band x needs places it at its own top.
        let small = config().expand(&entries(&["sm", "md"])).unwrap();
        assert_eq!(small.band_at(0.9).unwrap().id, "md");
        // And one without a low band starts at its lowest.
        let big = config().expand(&entries(&["md", "lg"])).unwrap();
        assert_eq!(big.band_at(0.1).unwrap().id, "md");
    }

    #[test]
    fn relaxation_goes_up_then_down() {
        let expanded = config().expand(&entries(&["sm", "md", "lg"])).unwrap();
        let order: Vec<&str> = expanded
            .relaxations_from("md")
            .iter()
            .map(|b| b.id.as_str())
            .collect();
        assert_eq!(order, ["lg", "sm"]);
    }

    #[test]
    fn validation_refuses_duplicates_and_out_of_range() {
        let mut twice = config();
        twice.ladder.push(rung("claude/haiku", 0.1));
        assert!(twice.validate().unwrap_err().contains("twice"));
        let mut level = config();
        level.ladder[0].level = Some(Level::set(1.5));
        assert!(level.validate().unwrap_err().contains("0..1"));
        let mut same = config();
        same.pools.push(pool("xl", 0.67));
        assert!(same.validate().unwrap_err().contains("both start"));
        let mut slash = config();
        slash.pools.push(pool("a/b", 0.9));
        assert!(slash.validate().is_err());
        config().validate().unwrap();
    }

    #[test]
    fn a_level_keeps_its_source_and_a_bare_number_is_set() {
        let yaml = "model: claude/opus\nlevel: {value: 0.83, source: seeded, from: converted from the lg tier}\n";
        let rung: Rung = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(
            rung.level,
            Some(Level::seeded(0.83, "converted from the lg tier"))
        );
        let bare: Rung = serde_yaml::from_str("model: claude/opus\nlevel: 0.9\n").unwrap();
        assert_eq!(bare.level, Some(Level::set(0.9)));
        assert_eq!(
            serde_yaml::to_string(&bare).unwrap(),
            "model: claude/opus\nlevel: 0.9\n"
        );
    }

    #[test]
    fn the_yaml_shape_round_trips() {
        let yaml = r#"
pools:
  - id: sm
    description: small
    band: { min: 0.0 }
  - id: myth
    description: hand-picked
    members: [claude/fable]
ladder:
  - model: openrouter/anthropic/claude-sonnet-4.5
    level: 0.5
    effort: 0.5
    options: { serviceTier: priority }
defaultModels: [sm]
"#;
        let parsed: ModelsConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(parsed.ladder[0].model.backend, "openrouter");
        assert_eq!(
            parsed.ladder[0].option_extras().service_tier.as_deref(),
            Some("priority")
        );
        let again: ModelsConfig =
            serde_yaml::from_str(&serde_yaml::to_string(&parsed).unwrap()).unwrap();
        assert_eq!(again, parsed);
        let both = "pools: [{id: x, band: {min: 0.1}, members: [claude/opus]}]";
        assert!(serde_yaml::from_str::<ModelsConfig>(both).is_err());
    }

    #[test]
    fn an_overlay_merges_pools_by_id_and_rungs_by_model() {
        let mut base = config();
        base.overlay(&ModelsOverlay {
            pools: vec![Pool {
                id: "md".into(),
                description: "project md".into(),
                kind: PoolKind::Band { min: 0.4 },
            }],
            ladder: vec![rung("claude/sonnet", 0.7), rung("claude/fable", 0.95)],
            default_models: Some(entries(&["lg"])),
        });
        assert_eq!(base.pool("md").unwrap().description, "project md");
        assert_eq!(base.pools.len(), 4);
        assert_eq!(
            base.rung(&parse_model_ref("claude/sonnet").unwrap())
                .unwrap()
                .level,
            Some(Level::set(0.7))
        );
        assert_eq!(base.ladder.len(), 7);
        assert_eq!(base.default_models, entries(&["lg"]));
    }

    // === Conversion fixtures ===

    fn preset(model: &str, effort: Option<&str>) -> Preset {
        let mut options = HashMap::new();
        if let Some(effort) = effort {
            options.insert(
                EFFORT_OPTION.to_string(),
                PresetOptionValue::Str(effort.into()),
            );
        }
        Preset {
            model: Model::new(model),
            options,
        }
    }

    fn grid(
        cells: &[(&str, &str, &str, Option<&str>)],
    ) -> HashMap<String, HashMap<String, Preset>> {
        let mut backends: HashMap<String, HashMap<String, Preset>> = HashMap::new();
        for (backend, tier, model, effort) in cells {
            backends
                .entry(backend.to_string())
                .or_default()
                .insert(tier.to_string(), preset(model, *effort));
        }
        backends
    }

    /// Each provider's scale as its catalog would answer: Claude's static four,
    /// Codex's discovered list (only sol lists `ultra`), Go's per model, and
    /// nothing yet for OpenRouter, which lists levels per model on discovery.
    fn position(selection: &ModelSelection, level: &str) -> EffortPlacement {
        let levels: &[&str] = match (selection.backend.as_str(), selection.model.as_str()) {
            ("claude", _) => &["low", "medium", "high", "xhigh"],
            ("codex", "gpt-5.6-sol") => &["low", "medium", "high", "xhigh", "ultra"],
            ("codex", _) => &["low", "medium", "high", "xhigh"],
            ("opencode-go", "kimi-k3") => &["none", "low", "high", "max"],
            ("opencode-go", _) => &["low", "high"],
            _ => return EffortPlacement::ScaleUnknown,
        };
        match levels.iter().position(|known| *known == level) {
            Some(index) => EffortPlacement::At((index as f64 + 0.5) / levels.len() as f64),
            None => EffortPlacement::NotALevel,
        }
    }

    fn strings(raw: &[&str]) -> Vec<String> {
        raw.iter().map(|s| s.to_string()).collect()
    }

    fn convert(
        tiers: &[&str],
        defaults: &[(&str, &str)],
        cells: &[(&str, &str, &str, Option<&str>)],
        enabled: &[&str],
        decision: Option<&str>,
    ) -> Conversion {
        let tiers = strings(tiers);
        let tier_defaults: HashMap<String, String> = defaults
            .iter()
            .map(|(t, b)| (t.to_string(), b.to_string()))
            .collect();
        let backends = grid(cells);
        convert_tiers(
            &LegacyTiers {
                tiers: &tiers,
                tier_defaults: &tier_defaults,
                backends: &backends,
            },
            &strings(enabled),
            decision,
            &position,
        )
    }

    fn band_models(models: &ModelsConfig, band: &str) -> Vec<String> {
        models
            .expand(&[AllotmentEntry::Pool(band.into())])
            .unwrap()
            .candidates
            .iter()
            .map(|c| model_ref(&c.model))
            .collect()
    }

    /// The operator's own grid: MD defaults to OpenCode Go's glm while
    /// MD/Anthropic still says sonnet, plus a custom MYTH tier.
    #[test]
    fn the_operators_grid_puts_every_md_cell_in_the_md_band() {
        let converted = convert(
            &["sm", "md", "lg", "myth"],
            &[
                ("sm", "claude"),
                ("md", "opencode-go"),
                ("lg", "codex"),
                ("myth", "claude"),
            ],
            &[
                ("claude", "sm", "haiku", None),
                ("claude", "md", "sonnet", Some("high")),
                ("claude", "lg", "opus", Some("high")),
                ("claude", "myth", "fable", Some("max")),
                ("codex", "sm", "gpt-5.6-luna", Some("low")),
                ("codex", "md", "gpt-5.6-terra", Some("medium")),
                ("codex", "lg", "gpt-5.6-sol", Some("ultra")),
                ("opencode-go", "md", "glm-5.3", Some("high")),
                (
                    "openrouter",
                    "md",
                    "~anthropic/claude-sonnet-latest",
                    Some("medium"),
                ),
            ],
            &["claude", "codex", "opencode-go"],
            Some("codex/lg"),
        );
        let models = &converted.models;
        models.validate().unwrap();
        assert_eq!(
            band_models(models, "md"),
            [
                "claude/sonnet",
                "codex/gpt-5.6-terra",
                "opencode-go/glm-5.3"
            ]
        );
        assert_eq!(
            band_models(models, "lg"),
            ["claude/opus", "codex/gpt-5.6-sol"]
        );
        // OpenRouter is not installed, so its cell is not carried over.
        assert!(models
            .ladder
            .iter()
            .all(|rung| rung.model.backend != "openrouter"));
        let sol = models
            .rung(&parse_model_ref("codex/gpt-5.6-sol").unwrap())
            .unwrap();
        assert_eq!(
            (sol.effort, sol.effort_level.as_deref()),
            (None, Some("ultra"))
        );
        let sonnet = models
            .rung(&parse_model_ref("claude/sonnet").unwrap())
            .unwrap();
        assert_eq!(
            (sonnet.effort, sonnet.effort_level.as_deref()),
            (None, Some("high"))
        );
        // MYTH is a hand-picked pool, and fable keeps its options off the ladder.
        let myth = models.pool("myth").unwrap();
        assert!(
            matches!(&myth.kind, PoolKind::Set { members } if model_ref(&members[0]) == "claude/fable")
        );
        let fable = models
            .rung(&parse_model_ref("claude/fable").unwrap())
            .unwrap();
        assert_eq!((fable.level.clone(), fable.effort), (None, Some(1.0)));
        assert!(
            converted
                .notes
                .iter()
                .any(|n| n.contains("claude/fable: effort 'max'")),
            "{:?}",
            converted.notes
        );
        assert!(converted
            .notes
            .iter()
            .any(|n| n.starts_with("md defaulted to opencode-go")));
        assert_eq!(
            converted.decision_model.as_deref(),
            Some("codex/gpt-5.6-sol")
        );
        assert_eq!(models.default_models, entries(&["md"]));
        assert_eq!(models.conversion_notes, converted.notes);
    }

    #[test]
    fn seeded_bands_take_their_descriptions_and_cover_the_ladder() {
        let converted = convert(
            &["sm", "md", "lg"],
            &[],
            &[("claude", "sm", "haiku", None)],
            &["claude"],
            None,
        );
        let bands = converted.models.bands();
        assert_eq!(
            bands.iter().map(|b| b.min).collect::<Vec<_>>(),
            [0.0, 0.33, 0.67]
        );
        for band in &bands {
            assert!(band.centre() >= band.min && band.centre() < band.max);
            let pool = converted.models.pool(&band.id).unwrap();
            assert!(!pool.description.is_empty());
        }
        assert!(converted.notes.is_empty(), "{:?}", converted.notes);
    }

    /// The bench pins one model under test into every tier of one custom
    /// provider.
    #[test]
    fn the_bench_grid_collapses_one_model_into_its_highest_tier() {
        let converted = convert(
            &["sm", "md", "lg", "myth"],
            &[
                ("sm", "custom:bench"),
                ("md", "custom:bench"),
                ("lg", "custom:bench"),
                ("myth", "custom:bench"),
            ],
            &[
                ("custom:bench", "sm", "qwen3-coder", None),
                ("custom:bench", "md", "qwen3-coder", None),
                ("custom:bench", "lg", "qwen3-coder", None),
                ("custom:bench", "myth", "qwen3-coder", None),
            ],
            &["claude", "codex", "custom:bench"],
            None,
        );
        let models = &converted.models;
        assert_eq!(models.ladder.len(), 1);
        assert_eq!(band_models(models, "lg"), ["custom:bench/qwen3-coder"]);
        assert!(band_models(models, "sm").is_empty());
        assert!(
            converted
                .notes
                .iter()
                .any(|n| n.contains("several tiers")
                    && n.contains("custom:bench/sm, custom:bench/md")),
            "{:?}",
            converted.notes
        );
        let myth = models.pool("myth").unwrap();
        assert!(matches!(&myth.kind, PoolKind::Set { members } if members.len() == 1));
    }

    #[test]
    fn a_collision_keeps_the_highest_cells_options() {
        let converted = convert(
            &["sm", "md", "lg"],
            &[],
            &[
                ("claude", "md", "sonnet", Some("low")),
                ("claude", "lg", "sonnet", Some("xhigh")),
            ],
            &["claude"],
            None,
        );
        let sonnet = &converted.models.ladder[0];
        assert_eq!(converted.models.ladder.len(), 1);
        assert_eq!(
            (sonnet.effort, sonnet.effort_level.as_deref()),
            (None, Some("xhigh"))
        );
        assert_eq!(
            converted
                .models
                .band_of(sonnet.level.as_ref().unwrap().value)
                .unwrap()
                .id,
            "lg"
        );
    }

    #[test]
    fn an_effort_off_the_models_scale_becomes_its_top_with_a_note() {
        let converted = convert(
            &["sm", "md", "lg"],
            &[],
            &[
                ("claude", "lg", "opus", Some("max")),
                ("codex", "md", "gpt-5.6-terra", Some("ultra")),
                ("opencode-go", "lg", "kimi-k3", Some("max")),
            ],
            &["claude", "codex", "opencode-go"],
            None,
        );
        let effort = |model: &str| {
            converted
                .models
                .rung(&parse_model_ref(model).unwrap())
                .unwrap()
                .effort
        };
        assert_eq!(effort("claude/opus"), Some(1.0));
        assert_eq!(effort("codex/gpt-5.6-terra"), Some(1.0));
        // Kimi's max is a level, so it is kept as that word and gets no note.
        assert_eq!(effort("opencode-go/kimi-k3"), None);
        assert_eq!(
            converted
                .models
                .rung(&parse_model_ref("opencode-go/kimi-k3").unwrap())
                .unwrap()
                .effort_level
                .as_deref(),
            Some("max")
        );
        assert_eq!(converted.notes.len(), 2, "{:?}", converted.notes);
    }

    /// Before discovery OpenRouter knows no model's levels, which says nothing
    /// about whether the stored word is one of them: the word is kept for
    /// selection to place.
    #[test]
    fn an_effort_on_a_scale_not_yet_known_is_kept_as_a_word_with_a_note() {
        let converted = convert(
            &["sm", "md", "lg"],
            &[],
            &[(
                "openrouter",
                "md",
                "anthropic/claude-haiku-5.5",
                Some("medium"),
            )],
            &["openrouter"],
            None,
        );
        let haiku = converted
            .models
            .rung(&parse_model_ref("openrouter/anthropic/claude-haiku-5.5").unwrap())
            .unwrap();
        assert_eq!(haiku.effort, None);
        assert_eq!(haiku.effort_level.as_deref(), Some("medium"));
        assert_eq!(converted.notes.len(), 1, "{:?}", converted.notes);
        assert!(
            converted.notes[0].starts_with(
                "openrouter/anthropic/claude-haiku-5.5: effort 'medium' is kept as a word"
            ),
            "{:?}",
            converted.notes
        );
    }

    #[test]
    fn a_decision_model_tier_becomes_its_pool() {
        let cells = [("claude", "sm", "haiku", None)];
        let pool = convert(&["sm", "md", "lg"], &[], &cells, &["claude"], Some("sm"));
        assert_eq!(pool.decision_model.as_deref(), Some("sm"));
        let concrete = convert(
            &["sm", "md", "lg"],
            &[],
            &cells,
            &["claude"],
            Some("typesafe/jev-1"),
        );
        assert_eq!(concrete.decision_model.as_deref(), Some("typesafe/jev-1"));
        assert!(concrete.notes.is_empty());
    }

    #[test]
    fn dropping_a_provider_removes_its_rungs_and_members() {
        let mut base = config();
        let dropped = base.drop_provider("claude");
        assert_eq!(
            dropped,
            [
                "claude/haiku",
                "claude/sonnet",
                "claude/opus",
                "claude/fable"
            ]
        );
        assert!(base
            .ladder
            .iter()
            .all(|rung| rung.model.backend != "claude"));
        assert!(
            matches!(&base.pool("myth").unwrap().kind, PoolKind::Set { members } if members.is_empty())
        );
    }
}
