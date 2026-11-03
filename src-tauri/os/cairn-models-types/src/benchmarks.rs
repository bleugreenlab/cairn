//! Benchmark levels: where public, licensed benchmark results place a model
//! on the capability ladder.
//!
//! `data/benchmarks/ladder.json` is written by `bench/neighbours/ladder.py`
//! (one additive fit over the sources whose results may ship, by tenet 1360).
//! It is mapped onto 0..1 through a frozen anchor, so a model's level is its
//! expected solve rate on the anchor's reference items. A level here is an
//! input to the ladder and never a stored setting: it places a model the user
//! has not, and it is shown as a hint beside a level the user has set.
//! The data is CC-BY-SA-4.0 as a whole; `data/benchmarks/NOTICE.md` credits
//! each source and [`Shipped::attribution`] carries the same lines.
//!
//! The fit's levels are kept as measured, then placed in release order within
//! each product line ([`crate::identity::Identity::lines`]). Neighbouring
//! releases sit inside each other's intervals, so the benchmarks alone can
//! rank Opus 4.6 above 4.7; a newer release is instead taken to be at least as
//! capable as an older one. That is an assumption imposed on the data, so each
//! placement says when it holds ([`Basis`]): releases out of order are pooled
//! to their precision-weighted mean, keeping their measured levels for the
//! hint, and a newer release with too few results of its own is placed at
//! the level of the newest release before it the fit placed. Such a floor is
//! a placement but never a calibration point.

use crate::identity::{identity, Identity};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::sync::OnceLock;

const LADDER: &str = include_str!("../data/benchmarks/ladder.json");

/// Where the shipped benchmark fit places one canonical model.
#[derive(Debug, Clone, PartialEq)]
pub struct BenchmarkPlacement {
    /// The canonical model id ([`crate::identity`]).
    pub canonical: String,
    /// The expected solve rate on the anchor's reference panel, 0..1.
    pub level: f64,
    /// The 95% interval's ends; it includes disagreement between scaffolds.
    pub low: f64,
    pub high: f64,
    /// The benchmark sources the level rests on.
    pub sources: Vec<String>,
    /// The anchor version the level is on (`v1`).
    pub anchor: String,
    /// Whether the level is the fit's own or set by release order.
    pub basis: Basis,
}

/// Where a placement's level comes from.
#[derive(Debug, Clone, PartialEq)]
pub enum Basis {
    /// The fit's own level.
    Measured,
    /// The fit's level moved to keep its line in release order: averaged,
    /// weighted by precision, with the neighbouring releases `pooled_with`.
    Ordered {
        measured: f64,
        pooled_with: Vec<String>,
    },
    /// Too few results of its own: placed at the level of `succeeds`, the
    /// newest earlier release of its line the fit placed. A floor (its
    /// interval reaches 1), so never a calibration point.
    AtLeast { succeeds: String },
}

impl BenchmarkPlacement {
    /// The placement in one line:
    /// `benchmarks: 0.714 solve rate [0.641, 0.780], 6 sources, anchor v1`,
    /// then what release order did to it, when it did anything.
    /// A level is a solve rate, not a ladder position; the ladder maps it
    /// through its own calibration rungs before placing anything.
    pub fn summary(&self) -> String {
        let base = match self.basis {
            Basis::AtLeast { .. } => format!(
                "benchmarks: at least {:.3} solve rate, anchor {}",
                self.level, self.anchor
            ),
            _ => format!(
                "benchmarks: {:.3} solve rate [{:.3}, {:.3}], {} source{}, anchor {}",
                self.level,
                self.low,
                self.high,
                self.sources.len(),
                if self.sources.len() == 1 { "" } else { "s" },
                self.anchor
            ),
        };
        match self.basis_note() {
            Some(note) => format!("{base}; {note}"),
            None => base,
        }
    }

    /// What release order did to the level, in words, when it did anything:
    /// `pooled with claude-opus-4.7 to keep release order, measured 0.676`.
    pub fn basis_note(&self) -> Option<String> {
        match &self.basis {
            Basis::Measured => None,
            Basis::Ordered {
                measured,
                pooled_with,
            } => Some(format!(
                "pooled with {} to keep release order, measured {measured:.3}",
                pooled_with.join(", ")
            )),
            Basis::AtLeast { succeeds } => Some(format!(
                "placed at {succeeds}, which it succeeds, for want of results of its own"
            )),
        }
    }

    /// Whether the placement is a floor rather than a level the benchmarks
    /// measured for this model: it places, but calibrates nothing.
    pub fn is_floor(&self) -> bool {
        matches!(self.basis, Basis::AtLeast { .. })
    }
}

/// The standard deviation a placement's 95% interval implies, floored so a
/// degenerate interval does not take all the weight.
fn spread(placement: &BenchmarkPlacement) -> f64 {
    ((placement.high - placement.low) / 3.92).max(1e-3)
}

/// Places each line's releases in release order. Within a line, releases the
/// fit placed out of order are pooled (pool-adjacent-violators, weighted by
/// the precision of each level), so a line's levels never fall with version.
/// A release the fit did not place, after one it did, is given a floor at
/// the newest earlier placed release's level.
fn in_release_order<'a>(
    placements: &mut BTreeMap<String, BenchmarkPlacement>,
    lines: impl Iterator<Item = &'a [String]>,
    anchor: &str,
) {
    for members in lines {
        // Blocks of (precision-weighted sum, total precision, releases).
        let mut blocks: Vec<(f64, f64, Vec<&String>)> = Vec::new();
        for id in members {
            let Some(placed) = placements.get(id) else {
                continue;
            };
            let weight = spread(placed).powi(-2);
            blocks.push((placed.level * weight, weight, vec![id]));
            while blocks.len() > 1 {
                let n = blocks.len();
                let mean = |b: &(f64, f64, Vec<&String>)| b.0 / b.1;
                if mean(&blocks[n - 2]) <= mean(&blocks[n - 1]) {
                    break;
                }
                let last = blocks.pop().expect("two blocks");
                let into = blocks.last_mut().expect("two blocks");
                into.0 += last.0;
                into.1 += last.1;
                into.2.extend(last.2);
            }
        }
        for (sum, weight, ids) in blocks.into_iter().filter(|block| block.2.len() > 1) {
            let level = sum / weight;
            for id in &ids {
                let placed = placements.get_mut(*id).expect("placed");
                let measured = placed.level;
                placed.level = level;
                placed.low = placed.low.min(level);
                placed.high = placed.high.max(level);
                placed.basis = Basis::Ordered {
                    measured,
                    pooled_with: ids
                        .iter()
                        .filter(|other| *other != id)
                        .map(|other| other.to_string())
                        .collect(),
                };
            }
        }
        let mut newest: Option<(&String, f64)> = None;
        let mut floors = Vec::new();
        for id in members {
            match placements.get(id) {
                Some(placed) => newest = Some((id, placed.level)),
                None => {
                    if let Some((succeeds, level)) = newest {
                        floors.push(BenchmarkPlacement {
                            canonical: id.clone(),
                            level,
                            low: level,
                            high: 1.0,
                            sources: Vec::new(),
                            anchor: anchor.to_string(),
                            basis: Basis::AtLeast {
                                succeeds: succeeds.clone(),
                            },
                        });
                    }
                }
            }
        }
        for floor in floors {
            placements.insert(floor.canonical.clone(), floor);
        }
    }
}

/// One source's credit, as the shipped notice prints it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceCredit {
    pub id: String,
    pub license: String,
    pub attribution: String,
    pub url: String,
    pub share_alike: bool,
}

#[derive(Deserialize)]
struct LadderFile {
    format: u32,
    license: String,
    anchor: AnchorSummary,
    sources: Vec<SourceCredit>,
    models: Vec<ModelLevel>,
}

#[derive(Deserialize)]
struct AnchorSummary {
    version: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ModelLevel {
    model: String,
    level: Option<f64>,
    level_interval: Option<[f64; 2]>,
    sources: Vec<String>,
}

/// The shipped benchmark data, parsed.
#[derive(Debug)]
pub struct Shipped {
    /// The data's own license as a whole.
    pub license: String,
    pub anchor: String,
    pub sources: Vec<SourceCredit>,
    placements: BTreeMap<String, BenchmarkPlacement>,
}

impl Shipped {
    fn parse(text: &str, identity: &Identity) -> Result<Shipped, String> {
        let file: LadderFile =
            serde_json::from_str(text).map_err(|e| format!("benchmarks/ladder.json: {e}"))?;
        if file.format != 1 {
            return Err(format!(
                "benchmarks/ladder.json is format {}, not 1",
                file.format
            ));
        }
        let anchor = file.anchor.version;
        let mut placements: BTreeMap<String, BenchmarkPlacement> = file
            .models
            .into_iter()
            .filter_map(|m| {
                let (level, [low, high]) = (m.level?, m.level_interval?);
                Some((
                    m.model.clone(),
                    BenchmarkPlacement {
                        canonical: m.model,
                        level,
                        low,
                        high,
                        sources: m.sources,
                        anchor: anchor.clone(),
                        basis: Basis::Measured,
                    },
                ))
            })
            .collect();
        in_release_order(
            &mut placements,
            identity.lines().map(|(_, members)| members),
            &anchor,
        );
        Ok(Shipped {
            license: file.license,
            anchor,
            sources: file.sources,
            placements,
        })
    }

    /// Where the benchmarks place the canonical model `id`, when they place
    /// it at all (a model under the evidence floor has no level unless a
    /// release before it in its line has one).
    pub fn model(&self, id: &str) -> Option<&BenchmarkPlacement> {
        self.placements.get(id)
    }

    /// Every placed model, in id order.
    pub fn placements(&self) -> impl Iterator<Item = &BenchmarkPlacement> {
        self.placements.values()
    }

    /// The credit lines the data's license requires, one per source:
    /// `<attribution>. <license>, <url>` with share-alike said where it
    /// applies, after a line naming the data's own license.
    pub fn attribution(&self) -> Vec<String> {
        let mut lines = vec![format!(
            "Benchmark levels: an adaptation of the sources below, licensed {} as a whole \
             (anchor {}).",
            self.license, self.anchor
        )];
        lines.extend(self.sources.iter().map(|s| {
            format!(
                "{}. {}{}, {}",
                s.attribution,
                s.license,
                if s.share_alike { " (share-alike)" } else { "" },
                s.url
            )
        }));
        lines
    }
}

/// The shipped benchmark data, parsed once.
pub fn shipped() -> &'static Shipped {
    static SHIPPED: OnceLock<Shipped> = OnceLock::new();
    // Compiled in and checked by this module's tests.
    SHIPPED.get_or_init(|| {
        Shipped::parse(LADDER, identity()).expect("the shipped benchmark data parses")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const LICENSES: &[&str] = &["CC-BY-4.0", "CC-BY-SA-4.0", "Apache-2.0", "MIT"];

    #[derive(Deserialize)]
    struct Cells {
        cells: Vec<Cell>,
    }

    #[derive(Deserialize)]
    struct Cell {
        model: String,
        source: String,
    }

    fn cells() -> Vec<(String, String)> {
        let parsed: Cells = serde_json::from_str(LADDER).expect("parses");
        parsed
            .cells
            .into_iter()
            .map(|c| (c.model, c.source))
            .collect()
    }

    #[test]
    fn the_shipped_file_parses_with_an_anchor_and_placements() {
        let data = Shipped::parse(LADDER, identity()).expect("parses");
        assert!(data.anchor.starts_with('v'), "{}", data.anchor);
        assert!(data.placements().count() > 10);
    }

    #[test]
    fn every_model_named_is_a_canonical_id() {
        let data = shipped();
        for placement in data.placements() {
            assert!(
                identity().canonical(&placement.canonical).is_some(),
                "{}",
                placement.canonical
            );
        }
        for (model, _) in &cells() {
            assert!(identity().canonical(model).is_some(), "{model}");
        }
    }

    #[test]
    fn every_source_is_licensed_credited_and_listed() {
        let data = shipped();
        assert!(!data.sources.is_empty());
        for source in &data.sources {
            assert!(
                LICENSES.contains(&source.license.as_str()),
                "{}: {}",
                source.id,
                source.license
            );
            assert!(!source.attribution.trim().is_empty(), "{}", source.id);
        }
        let listed: Vec<&str> = data.sources.iter().map(|s| s.id.as_str()).collect();
        for (model, source) in &cells() {
            assert!(listed.contains(&source.as_str()), "{model}: {source}");
        }
        // Share-alike reaches the whole adaptation.
        if data.sources.iter().any(|s| s.share_alike) {
            assert_eq!(data.license, "CC-BY-SA-4.0");
        }
        assert_eq!(data.attribution().len(), data.sources.len() + 1);
    }

    #[test]
    fn every_level_lies_in_zero_one_and_inside_its_interval() {
        for p in shipped().placements() {
            assert!((0.0..=1.0).contains(&p.level), "{}", p.canonical);
            assert!(p.low <= p.level && p.level <= p.high, "{}", p.canonical);
            if !p.is_floor() {
                assert!(p.sources.len() >= 2, "{} rests on one source", p.canonical);
            }
        }
    }

    fn placed(id: &str, level: f64, half: f64) -> (String, BenchmarkPlacement) {
        (
            id.to_string(),
            BenchmarkPlacement {
                canonical: id.to_string(),
                level,
                low: level - half,
                high: level + half,
                sources: vec!["a".into(), "b".into()],
                anchor: "v1".into(),
                basis: Basis::Measured,
            },
        )
    }

    #[test]
    fn a_line_is_placed_in_release_order_with_floors_for_unplaced_successors() {
        let line: Vec<String> = ["m-1", "m-2", "m-3", "m-4", "m-5"]
            .map(String::from)
            .to_vec();
        let mut placements: BTreeMap<_, _> = [
            placed("m-1", 0.50, 0.02),
            placed("m-2", 0.68, 0.04),
            placed("m-3", 0.64, 0.02),
            placed("other", 0.10, 0.02),
        ]
        .into_iter()
        .collect();
        in_release_order(&mut placements, std::iter::once(line.as_slice()), "v1");

        // In order already: untouched.
        assert_eq!(placements["m-1"].basis, Basis::Measured);
        assert_eq!(placements["m-1"].level, 0.50);
        // Out of order: pooled to the precision-weighted mean, nearer the
        // tighter m-3, with the measured levels kept.
        let (two, three) = (&placements["m-2"], &placements["m-3"]);
        assert_eq!(two.level, three.level);
        assert!((two.level - 0.648).abs() < 1e-9, "{}", two.level);
        assert_eq!(
            two.basis,
            Basis::Ordered {
                measured: 0.68,
                pooled_with: vec!["m-3".into()]
            }
        );
        assert!(two.low <= two.level && two.level <= two.high);
        assert!(three.low <= three.level && three.level <= three.high);
        assert!(two
            .summary()
            .ends_with("pooled with m-3 to keep release order, measured 0.680"));
        // Unplaced after a placed release: a floor at the newest placed level.
        for id in ["m-4", "m-5"] {
            let floor = &placements[id];
            assert_eq!(floor.level, three.level);
            assert_eq!((floor.low, floor.high), (three.level, 1.0));
            assert!(floor.is_floor() && floor.sources.is_empty());
            assert_eq!(
                floor.basis,
                Basis::AtLeast {
                    succeeds: "m-3".into()
                }
            );
            assert_eq!(
                floor.summary(),
                "benchmarks: at least 0.648 solve rate, anchor v1; placed at m-3, which it succeeds, for want of results of its own"
            );
        }
        // Not in a line: untouched.
        assert_eq!(placements["other"].level, 0.10);
    }

    #[test]
    fn the_shipped_lines_rise_with_release_and_place_the_newest_releases() {
        let data = shipped();
        for (line, members) in identity().lines() {
            let levels: Vec<f64> = members
                .iter()
                .filter_map(|id| data.model(id).map(|p| p.level))
                .collect();
            assert!(
                levels.windows(2).all(|pair| pair[0] <= pair[1]),
                "{line}: {levels:?}"
            );
        }
        // The fit measured Opus 4.6 above 4.7, inside each other's intervals.
        let (six, seven) = (
            data.model("claude-opus-4.6").unwrap(),
            data.model("claude-opus-4.7").unwrap(),
        );
        assert_eq!(six.level, seven.level);
        match (&six.basis, &seven.basis) {
            (Basis::Ordered { measured: a, .. }, Basis::Ordered { measured: b, .. }) => {
                assert!(a > b && *b < six.level && six.level < *a, "{a} {b}")
            }
            other => panic!("{other:?}"),
        }
        // Opus 5.5 rests on one source, so it takes 4.8's level as a floor.
        let newest = data.model("claude-opus-5.5").expect("placed by its line");
        assert_eq!(
            newest.basis,
            Basis::AtLeast {
                succeeds: "claude-opus-4.8".into()
            }
        );
        assert_eq!(newest.level, data.model("claude-opus-4.8").unwrap().level);
    }

    #[test]
    fn the_summary_is_one_line_with_interval_sources_and_anchor() {
        let placement = BenchmarkPlacement {
            canonical: "m".into(),
            level: 0.714,
            low: 0.641,
            high: 0.78,
            sources: (0..6).map(|n| n.to_string()).collect(),
            anchor: "v1".into(),
            basis: Basis::Measured,
        };
        assert_eq!(
            placement.summary(),
            "benchmarks: 0.714 solve rate [0.641, 0.780], 6 sources, anchor v1"
        );
    }
}
