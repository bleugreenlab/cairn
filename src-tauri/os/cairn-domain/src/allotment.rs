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
///
/// A rung carries no effort: effort is a property of the work, so a launch
/// takes its requirement's effort, else the provider's default.
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
    /// Placed at run time by the shipped benchmark fit, for a model with no
    /// stored level. Never stored: a ladder rung carrying it is refused, so
    /// a benchmark refresh can never move a level the ladder holds.
    Benchmarks,
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
    /// Its ladder level, when it has one: the stored level, or for a model
    /// with none, where the benchmarks place it.
    pub level: Option<f64>,
    /// Where `level` came from. [`LevelSource::Benchmarks`] marks a
    /// placement rather than a stored level.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub level_source: Option<LevelSource>,
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

/// A monotone map from another 0..1 scale onto this ladder's levels, fitted
/// through pairs (the other scale's value, the level the ladder holds) for the
/// same models.
///
/// The benchmark fit's levels are expected solve rates, not positions on a
/// user's ladder: the shipped ones span roughly 0.3 to 0.8 while a seeded
/// ladder runs 0.17 to 0.835. Placing a model the ladder holds no level for
/// goes through this map, fitted on the models the ladder does hold, so it
/// lands where the user's own levels say models like it belong.
///
/// Points the other scale cannot tell apart fix no scale between them: two
/// calibration models inside each other's intervals would turn noise into a
/// band boundary. So points are first pooled while a point's interval
/// overlaps the interval of its group's first point. The pooled points are
/// then fitted isotonically (pool-adjacent-violators, sorted by the other
/// scale), interpolated linearly between them, and extended beyond the ends
/// at the map's own overall slope, so the outside sits on the same scale as
/// the inside. The result is clamped to 0..1.
#[derive(Debug, Clone, PartialEq)]
pub struct LevelMap {
    points: Vec<(f64, f64)>,
}

/// One model the ladder holds a level for and the other scale also places:
/// where the other scale puts it (`at`, with its interval) and its level here.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CalibrationPoint {
    pub at: f64,
    pub low: f64,
    pub high: f64,
    pub level: f64,
}

impl LevelMap {
    /// The map through `points`, or `None` when fewer than two
    /// distinguishable points survive pooling (one point fixes no scale).
    pub fn fit(points: impl IntoIterator<Item = CalibrationPoint>) -> Option<Self> {
        let mut points: Vec<CalibrationPoint> = points
            .into_iter()
            .filter(|p| p.at.is_finite() && p.level.is_finite())
            .collect();
        points.sort_by(|a, b| a.at.total_cmp(&b.at).then(a.level.total_cmp(&b.level)));
        // Groups of points the other scale cannot tell apart, as (sum at,
        // sum level, count, the group's first point's upper bound).
        let mut groups: Vec<(f64, f64, f64, f64)> = Vec::new();
        for p in points {
            match groups.last_mut() {
                Some(group) if p.low <= group.3 => {
                    group.0 += p.at;
                    group.1 += p.level;
                    group.2 += 1.0;
                }
                _ => groups.push((p.at, p.level, 1.0, p.high)),
            }
        }
        // Blocks of (sum at, sum level, count); a block whose mean level is
        // above the next one's, or that shares its position, is merged with it.
        let mut blocks: Vec<(f64, f64, f64)> = Vec::new();
        for (x, y, n, _) in groups {
            blocks.push((x, y, n));
            while blocks.len() >= 2 {
                let (a, b) = (blocks[blocks.len() - 2], blocks[blocks.len() - 1]);
                let violates = a.1 / a.2 > b.1 / b.2 || a.0 / a.2 >= b.0 / b.2;
                if !violates {
                    break;
                }
                blocks.pop();
                let last = blocks.len() - 1;
                blocks[last] = (a.0 + b.0, a.1 + b.1, a.2 + b.2);
            }
        }
        let points: Vec<(f64, f64)> = blocks.into_iter().map(|(x, y, n)| (x / n, y / n)).collect();
        (points.len() >= 2).then_some(Self { points })
    }

    /// Where `x` on the other scale sits on this ladder.
    pub fn position(&self, x: f64) -> f64 {
        let points = &self.points;
        let (first, last) = (points[0], points[points.len() - 1]);
        let slope = (last.1 - first.1) / (last.0 - first.0);
        let y = if x <= first.0 {
            first.1 + (x - first.0) * slope
        } else if x >= last.0 {
            last.1 + (x - last.0) * slope
        } else {
            let i = points
                .windows(2)
                .position(|pair| x <= pair[1].0)
                .expect("x lies inside the fitted range");
            let ((x0, y0), (x1, y1)) = (points[i], points[i + 1]);
            y0 + (y1 - y0) * (x - x0) / (x1 - x0)
        };
        y.clamp(0.0, 1.0)
    }

    /// The pooled points the map runs through, lowest first.
    pub fn points(&self) -> &[(f64, f64)] {
        &self.points
    }
}

/// The placement lookup for [`ModelsConfig::expand`] when nothing beyond the
/// ladder places models.
pub fn unplaced(_: &ModelSelection) -> Option<Level> {
    None
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
    ///
    /// `placed` says where something other than the ladder places a model
    /// (the shipped benchmark fit). It is asked only for a model the
    /// allotment reaches by name or through a set pool that has no stored
    /// level; a stored level always wins, and band pools grow only from the
    /// ladder itself. Pass [`unplaced`] when nothing else places models.
    pub fn expand(
        &self,
        allotment: &[AllotmentEntry],
        placed: &dyn Fn(&ModelSelection) -> Option<Level>,
    ) -> Result<ExpandedAllotment, String> {
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
                .and_then(|rung| rung.level.clone())
                .or_else(|| placed(model));
            let level_source = level.as_ref().map(|level| level.source);
            let level = level.map(|level| level.value);
            candidates.push(Candidate {
                model: model.clone(),
                level,
                level_source,
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
            if rung
                .level
                .as_ref()
                .is_some_and(|level| level.source == LevelSource::Benchmarks)
            {
                return Err(format!(
                    "{}'s level has source 'benchmarks', which is a placement the benchmarks make at run time and is never stored; set the level (or record it as seeded from the benchmarks) instead",
                    model_ref(&rung.model)
                ));
            }
            if let Some(value) = rung.level.as_ref().map(|level| level.value) {
                if !(0.0..=1.0).contains(&value) {
                    return Err(format!(
                        "{}'s level is {value}; it is a position in 0..1",
                        model_ref(&rung.model)
                    ));
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

/// Convert a tier grid into pools and ladder entries.
///
/// - A grid's `tiers` list was always an ordered capability list, so every
///   tier becomes a band pool, in list order, starting at its index over the
///   number of tiers. A seeded id (`sm`, `md`, `lg`) carries the seeded
///   description; any other tier says what it was converted from and which
///   tier it sat above. Nothing in a grid expresses a reserved model, so no
///   set pool is produced.
/// - Every cell on an installed provider becomes a ladder entry at its
///   band's centre; a model in several tiers keeps the highest tier's level
///   and options.
/// - A cell's `reasoningEffort` is not carried over: effort comes from the
///   work a launch is for, else the provider's default. Other options move
///   across unchanged.
/// - A `decisionModel` naming a tier becomes that pool; `provider/tier`
///   becomes the model of that cell.
pub fn convert_tiers(
    legacy: &LegacyTiers<'_>,
    enabled: &[String],
    decision_model: Option<&str>,
) -> Conversion {
    let mut notes: Vec<String> = Vec::new();
    let mut tiers: Vec<&str> = Vec::new();
    for tier in legacy.tiers {
        if !tiers.contains(&tier.as_str()) {
            tiers.push(tier);
        }
    }
    let pools: Vec<Pool> = tiers
        .iter()
        .enumerate()
        .map(|(index, id)| Pool {
            id: id.to_string(),
            description: band_description(id, index.checked_sub(1).map(|below| tiers[below])),
            kind: PoolKind::Band {
                min: round2(index as f64 / tiers.len() as f64),
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
            let rung = Rung {
                model: selection.clone(),
                level: Some(Level::seeded(
                    band.centre(),
                    format!("converted from the {} tier", band.id),
                )),
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
                }
                Some(existing) => existing.2.push(here),
                None => ladder.push((rank, rung, Vec::new())),
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
            .map(|band| vec![AllotmentEntry::Pool(band.id.clone())])
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

/// The description of the band a tier converts to: the seeded one for a
/// seeded id, else where the tier came from and what it sat above, for the
/// operator to rewrite.
fn band_description(tier: &str, below: Option<&str>) -> String {
    if let Some((_, seeded)) = SEED_BANDS.iter().find(|(id, _)| *id == tier) {
        return seeded.to_string();
    }
    match below {
        Some(below) => format!("Converted from the {tier} tier, above {below}."),
        None => format!("Converted from the {tier} tier, the lowest."),
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
                    id: "pinned".into(),
                    description: "hand-picked for the work that names it".into(),
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
        let expanded = config().expand(&entries(&["md"]), &unplaced).unwrap();
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
        let expanded = config()
            .expand(&entries(&["sm", "md", "pinned"]), &unplaced)
            .unwrap();
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
        let alone = config()
            .expand(&entries(&["claude/fable"]), &unplaced)
            .unwrap();
        assert!(alone.top().is_none());
        assert_eq!(alone.in_band(None).len(), 1);
        assert!(alone.candidates[0].named);
    }

    #[test]
    fn placement_at_x_takes_the_lowest_band_reaching_it() {
        let expanded = config()
            .expand(&entries(&["sm", "md", "lg"]), &unplaced)
            .unwrap();
        assert_eq!(expanded.band_at(0.0).unwrap().id, "sm");
        assert_eq!(expanded.band_at(0.34).unwrap().id, "sm");
        assert_eq!(expanded.band_at(0.35).unwrap().id, "md");
        assert_eq!(expanded.band_at(1.0).unwrap().id, "lg");
        // An allotment without the band x needs places it at its own top.
        let small = config().expand(&entries(&["sm", "md"]), &unplaced).unwrap();
        assert_eq!(small.band_at(0.9).unwrap().id, "md");
        // And one without a low band starts at its lowest.
        let big = config().expand(&entries(&["md", "lg"]), &unplaced).unwrap();
        assert_eq!(big.band_at(0.1).unwrap().id, "md");
    }

    fn benchmarks(value: f64) -> Level {
        Level {
            value,
            source: LevelSource::Benchmarks,
            from: Some("benchmarks, anchor v1".into()),
        }
    }

    #[test]
    fn a_benchmark_level_places_only_a_model_with_no_stored_level() {
        let mut models = config();
        models.ladder.push(Rung {
            level: Some(Level::seeded(0.3, "converted from the sm tier")),
            ..rung("claude/haiku-seeded", 0.0)
        });
        // Every model would be placed at 0.9: only the unranked one is.
        let everywhere = |_: &ModelSelection| Some(benchmarks(0.9));
        let expanded = models
            .expand(
                &entries(&["pinned", "claude/sonnet", "claude/haiku-seeded"]),
                &everywhere,
            )
            .unwrap();
        let by = |name: &str| {
            expanded
                .candidates
                .iter()
                .find(|c| model_ref(&c.model) == name)
                .unwrap()
        };
        let fable = by("claude/fable");
        assert_eq!(fable.level, Some(0.9));
        assert_eq!(fable.level_source, Some(LevelSource::Benchmarks));
        assert_eq!(fable.band.as_deref(), Some("lg"));
        let sonnet = by("claude/sonnet");
        assert_eq!(sonnet.level, Some(0.5));
        assert_eq!(sonnet.level_source, Some(LevelSource::Set));
        let seeded = by("claude/haiku-seeded");
        assert_eq!(seeded.level, Some(0.3));
        assert_eq!(seeded.level_source, Some(LevelSource::Seeded));
        // A band pool grows only from the ladder: placement adds nobody to lg.
        let lg = models.expand(&entries(&["lg"]), &everywhere).unwrap();
        let members: Vec<String> = lg.candidates.iter().map(|c| model_ref(&c.model)).collect();
        assert_eq!(members, ["codex/gpt-5.6-sol", "claude/opus"]);
    }

    #[test]
    fn a_level_map_is_monotone_interpolates_and_extends_at_its_own_slope() {
        let p = |at: f64, half: f64, level: f64| CalibrationPoint {
            at,
            low: at - half,
            high: at + half,
            level,
        };
        let map =
            LevelMap::fit([p(0.5, 0.02, 0.505), p(0.7, 0.02, 0.835), p(0.3, 0.02, 0.17)]).unwrap();
        assert_eq!(map.points(), &[(0.3, 0.17), (0.5, 0.505), (0.7, 0.835)]);
        assert!((map.position(0.6) - 0.67).abs() < 1e-9);
        // Beyond the ends, the overall slope (0.665 / 0.4), not 1.
        assert!((map.position(0.75) - (0.835 + 0.05 * 1.6625)).abs() < 1e-9);
        assert!((map.position(0.2) - (0.17 - 0.1 * 1.6625)).abs() < 1e-9);
        assert_eq!(map.position(0.99), 1.0);
        assert_eq!(map.position(0.0), 0.0);
        // A pair out of order is pooled with its neighbour, never followed.
        let pooled =
            LevelMap::fit([p(0.4, 0.02, 0.6), p(0.5, 0.02, 0.4), p(0.7, 0.02, 0.9)]).unwrap();
        assert_eq!(pooled.points().len(), 2);
        assert!((pooled.points()[0].1 - 0.5).abs() < 1e-9);
        let mut last = 0.0;
        for step in 0..=100 {
            let y = pooled.position(step as f64 / 100.0);
            assert!(y >= last - 1e-12);
            last = y;
        }
        // A 0.655 solve rate the user put at 0.80 and a 0.45 one at 0.40.
        let user = LevelMap::fit([p(0.655, 0.04, 0.80), p(0.45, 0.04, 0.40)]).unwrap();
        let between = user.position(0.55);
        assert!(0.40 < between && between < 0.80, "{between}");
        assert!(user.position(0.5) < between && between < user.position(0.6));
        assert!(user.position(0.77) > 0.80);
        // One point, or two that pool into one, fix no scale.
        assert_eq!(LevelMap::fit([p(0.5, 0.0, 0.5)]), None);
        assert_eq!(LevelMap::fit([p(0.5, 0.0, 0.6), p(0.6, 0.0, 0.4)]), None);
        assert_eq!(LevelMap::fit([p(0.5, 0.0, 0.4), p(0.5, 0.0, 0.6)]), None);
    }

    #[test]
    fn calibration_points_the_benchmarks_cannot_tell_apart_fix_no_scale() {
        let p = |at: f64, low: f64, high: f64, level: f64| CalibrationPoint {
            at,
            low,
            high,
            level,
        };
        // A converted Codex grid: gpt-5.3-codex at md's centre and gpt-5.5 at
        // lg's, whose shipped intervals overlap. Two near-peers are one point.
        let codex = [p(0.638, 0.588, 0.694, 0.505), p(0.700, 0.656, 0.753, 0.835)];
        assert_eq!(LevelMap::fit(codex), None);
        // A covered smaller model calibrates it, with the near-peers pooled, so
        // two models 0.021 apart in solve rate stay close on the ladder.
        let map = LevelMap::fit([codex[0], codex[1], p(0.45, 0.42, 0.48, 0.17)]).unwrap();
        assert_eq!(map.points().len(), 2);
        let gap = map.position(0.676) - map.position(0.655);
        // Unpooled, the pair alone gave these two a gap of 0.11 and a band boundary.
        assert!((0.0..0.06).contains(&gap), "{gap}");
    }

    #[test]
    fn relaxation_goes_up_then_down() {
        let expanded = config()
            .expand(&entries(&["sm", "md", "lg"]), &unplaced)
            .unwrap();
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
        let mut stored = config();
        stored.ladder[0].level = Some(benchmarks(0.4));
        assert!(stored.validate().unwrap_err().contains("never stored"));
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
  - id: pinned
    description: hand-picked
    members: [claude/fable]
ladder:
  - model: openrouter/anthropic/claude-sonnet-4.5
    level: 0.5
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
        )
    }

    fn band_models(models: &ModelsConfig, band: &str) -> Vec<String> {
        models
            .expand(&[AllotmentEntry::Pool(band.into())], &unplaced)
            .unwrap()
            .candidates
            .iter()
            .map(|c| model_ref(&c.model))
            .collect()
    }

    /// MD defaults to OpenCode Go's glm while MD/Anthropic still says sonnet,
    /// plus a MYTH tier above LG.
    #[test]
    fn every_md_cell_lands_in_the_md_band() {
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
        // MYTH is the band above LG, and fable sits in it.
        assert_eq!(band_models(models, "myth"), ["claude/fable"]);
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
        assert_eq!(band_models(models, "myth"), ["custom:bench/qwen3-coder"]);
        for band in ["sm", "md", "lg"] {
            assert!(band_models(models, band).is_empty(), "{band}");
        }
        assert!(
            converted.notes.iter().any(|n| n.contains("several tiers")
                && n.contains("custom:bench/sm, custom:bench/md, custom:bench/lg")),
            "{:?}",
            converted.notes
        );
    }

    /// The operator's settings as they stood before conversion, every cell of
    /// them, with the providers the conversion is expected to see installed.
    #[test]
    fn the_operators_grid_converts_every_tier_to_a_band_in_order() {
        let tiers = strings(&["sm", "md", "lg", "myth"]);
        let tier_defaults: HashMap<String, String> = ["sm", "md", "lg", "myth"]
            .iter()
            .map(|tier| (tier.to_string(), "claude".to_string()))
            .collect();
        let mut backends = grid(&[
            ("codex", "sm", "gpt-6-luna", Some("low")),
            ("codex", "md", "gpt-6.1-terra", Some("medium")),
            ("codex", "lg", "gpt-6-astra", Some("high")),
            ("actual", "sm", "Qwen3.8-27B-Q4_0", None),
            ("ollama", "sm", "qwen3.8:27b-mlx", None),
            ("ollama", "md", "qwen3.8:latest", None),
            (
                "custom:acc_e0880682-8c8a-4a93-8437-d710ca583eda",
                "sm",
                "dsv4-flash",
                None,
            ),
            ("opencode-go", "sm", "minimax-m3", None),
            ("opencode-go", "md", "glm-5.3", Some("high")),
            ("opencode-go", "lg", "kimi-k3", Some("max")),
            ("openrouter", "sm", "~deepseek/deepseek-flash-latest", None),
            (
                "openrouter",
                "md",
                "anthropic/claude-haiku-5.5",
                Some("medium"),
            ),
            ("openrouter", "lg", "meta/muse-spark-open", Some("high")),
            ("openrouter", "myth", "moonshotai/kimi-k3", Some("xhigh")),
            ("claude", "sm", "haiku", None),
            ("claude", "md", "sonnet", Some("medium")),
            ("claude", "lg", "opus", Some("high")),
            ("claude", "myth", "fable", Some("high")),
        ]);
        for cell in backends.get_mut("codex").unwrap().values_mut() {
            cell.options
                .insert("fastMode".into(), PresetOptionValue::Bool(false));
        }
        let converted = convert_tiers(
            &LegacyTiers {
                tiers: &tiers,
                tier_defaults: &tier_defaults,
                backends: &backends,
            },
            &strings(&[
                "claude",
                "codex",
                "openrouter",
                "opencode-go",
                "ollama",
                "actual",
            ]),
            Some("typesafe/jev-1.14"),
        );
        let models = &converted.models;
        models.validate().unwrap();

        // Four bands, in the grid's order, each starting at index / count.
        let shape: Vec<(String, f64)> = models
            .bands()
            .into_iter()
            .map(|band| (band.id, band.min))
            .collect();
        assert_eq!(
            shape,
            [
                ("sm".to_string(), 0.0),
                ("md".to_string(), 0.25),
                ("lg".to_string(), 0.5),
                ("myth".to_string(), 0.75)
            ]
        );
        assert!(models
            .pools
            .iter()
            .all(|pool| matches!(pool.kind, PoolKind::Band { .. })));
        assert_eq!(
            models.pool("myth").unwrap().description,
            "Converted from the myth tier, above lg."
        );
        assert_eq!(models.pool("lg").unwrap().description, SEED_BANDS[2].1);

        // Fable and Kimi K3 are on the ladder, in MYTH, launching as before.
        assert_eq!(
            band_models(models, "myth"),
            ["claude/fable", "openrouter/moonshotai/kimi-k3"]
        );
        let fable = models
            .rung(&parse_model_ref("claude/fable").unwrap())
            .unwrap();
        assert_eq!(
            fable.level,
            Some(Level::seeded(0.88, "converted from the myth tier"))
        );

        // LG holds exactly the LG cells it held before, so an `lg` allotment
        // selects among the same models and never reaches MYTH.
        assert_eq!(
            band_models(models, "lg"),
            [
                "claude/opus",
                "codex/gpt-6-astra",
                "openrouter/meta/muse-spark-open",
                "opencode-go/kimi-k3"
            ]
        );
        let astra = models
            .rung(&parse_model_ref("codex/gpt-6-astra").unwrap())
            .unwrap();
        assert_eq!(
            astra.options.get("fastMode"),
            Some(&PresetOptionValue::Bool(false))
        );
        assert_eq!(
            band_models(models, "md"),
            [
                "claude/sonnet",
                "codex/gpt-6.1-terra",
                "openrouter/anthropic/claude-haiku-5.5",
                "opencode-go/glm-5.3",
                "ollama/qwen3.8:latest"
            ]
        );
        assert_eq!(
            band_models(models, "sm"),
            [
                "claude/haiku",
                "codex/gpt-6-luna",
                "openrouter/~deepseek/deepseek-flash-latest",
                "opencode-go/minimax-m3",
                "ollama/qwen3.8:27b-mlx",
                "actual/Qwen3.8-27B-Q4_0"
            ]
        );
        // The custom account is not installed, so its cell is not carried.
        assert_eq!(models.ladder.len(), 17);
        assert!(models.ladder.iter().all(|rung| rung.level.is_some()));

        assert_eq!(
            converted.decision_model.as_deref(),
            Some("typesafe/jev-1.14")
        );
        assert_eq!(models.default_models, entries(&["md"]));
    }

    #[test]
    fn a_tier_below_every_other_says_it_was_the_lowest() {
        let converted = convert(
            &["tiny", "sm", "md"],
            &[],
            &[("claude", "tiny", "haiku", None)],
            &["claude"],
            Some("claude/tiny"),
        );
        let models = &converted.models;
        assert_eq!(
            models.pool("tiny").unwrap().description,
            "Converted from the tiny tier, the lowest."
        );
        assert_eq!(models.band_of(0.0).unwrap().id, "tiny");
        assert_eq!(band_models(models, "tiny"), ["claude/haiku"]);
        assert_eq!(converted.decision_model.as_deref(), Some("claude/haiku"));
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
            converted
                .models
                .band_of(sonnet.level.as_ref().unwrap().value)
                .unwrap()
                .id,
            "lg"
        );
    }

    /// Effort belongs to the work, not the model: a cell's effort, on or off
    /// its model's scale, leaves nothing on the ladder entry and needs no
    /// note, while the cell's other options carry over.
    #[test]
    fn a_cells_effort_is_not_carried_onto_the_ladder() {
        let mut backends = grid(&[
            ("claude", "lg", "opus", Some("max")),
            ("codex", "md", "gpt-5.6-terra", Some("high")),
        ]);
        backends
            .get_mut("codex")
            .unwrap()
            .get_mut("md")
            .unwrap()
            .options
            .insert("fastMode".into(), PresetOptionValue::Bool(true));
        let tiers = strings(&["sm", "md", "lg"]);
        let converted = convert_tiers(
            &LegacyTiers {
                tiers: &tiers,
                tier_defaults: &HashMap::new(),
                backends: &backends,
            },
            &strings(&["claude", "codex"]),
            None,
        );
        assert!(converted.notes.is_empty(), "{:?}", converted.notes);
        let terra = converted
            .models
            .rung(&parse_model_ref("codex/gpt-5.6-terra").unwrap())
            .unwrap();
        assert_eq!(
            terra.options.keys().collect::<Vec<_>>(),
            ["fastMode"],
            "only the non-effort option carries over"
        );
        let written = serde_yaml::to_string(&converted.models).unwrap();
        assert!(!written.contains("effort"), "{written}");
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
            matches!(&base.pool("pinned").unwrap().kind, PoolKind::Set { members } if members.is_empty())
        );
    }
}
