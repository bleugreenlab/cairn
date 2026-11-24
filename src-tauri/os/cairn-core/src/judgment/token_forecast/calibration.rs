//! Score-to-quantile calibration: a line-for-line port of
//! `bench/token-forecast/score.py`'s `quantile`, `monotone`, `decide_table`,
//! `conformal_shift` and `shift`, so production maps a score exactly as
//! cairn/6271 measured. All quantities are natural-log billable tokens.

use serde::{Deserialize, Serialize};

use super::contract::{CalibrationProvenance, SoftBasis};

/// Equal-count score bins.
pub const BINS: usize = 5;
/// A bin with fewer fit issues than this takes the pooled quantiles.
pub const BIN_MIN: usize = 5;
/// The quantiles every table carries: p50, p80, p90.
pub const QUANTILES: [f64; 3] = [0.5, 0.8, 0.9];

/// Linear-interpolation quantile (type 7). `None` for no values.
pub fn quantile(values: &[f64], q: f64) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let pos = (sorted.len() - 1) as f64 * q;
    let lo = pos.floor() as usize;
    let hi = (lo + 1).min(sorted.len() - 1);
    Some(sorted[lo] + (sorted[hi] - sorted[lo]) * (pos - lo as f64))
}

/// p50, p80 and p90 of `values`.
pub fn quantiles(values: &[f64]) -> Option<[f64; 3]> {
    Some([
        quantile(values, QUANTILES[0])?,
        quantile(values, QUANTILES[1])?,
        quantile(values, QUANTILES[2])?,
    ])
}

/// Quantiles that never cross: p80 >= p50 and p90 >= p80.
pub fn monotone(p: [f64; 3]) -> [f64; 3] {
    let p80 = p[1].max(p[0]);
    [p[0], p80, p[2].max(p80)]
}

/// p80 and p90 raised by the conformal shifts; p50 kept.
pub fn shift(p: [f64; 3], shifts: [f64; 2]) -> [f64; 3] {
    monotone([p[0], p[1] + shifts[0], p[2] + shifts[1]])
}

/// Split-conformal upper shift: the ceil((n + 1) q)-th smallest residual.
pub fn conformal_shift(residuals: &[f64], q: f64) -> Option<f64> {
    if residuals.is_empty() {
        return None;
    }
    let mut sorted = residuals.to_vec();
    sorted.sort_by(f64::total_cmp);
    let k = ((sorted.len() + 1) as f64 * q).ceil() as usize;
    Some(sorted[k.min(sorted.len()).max(1) - 1])
}

/// Equal-count score bins and each bin's quantiles of the outcome.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Table {
    /// The [`BINS`] - 1 interior edges.
    pub edges: Vec<f64>,
    /// p50/p80/p90 per bin, lowest score first.
    pub bins: Vec<[f64; 3]>,
}

impl Table {
    /// The bin a score falls in: how many edges it exceeds.
    pub fn bin_of(&self, score: f64) -> usize {
        self.edges.iter().filter(|edge| score > **edge).count()
    }
}

/// `score.py::decide_table` over (score, outcome) pairs. `None` for no pairs.
pub fn decide_table(pairs: &[(f64, f64)]) -> Option<Table> {
    let scores: Vec<f64> = pairs.iter().map(|(s, _)| *s).collect();
    let edges = (1..BINS)
        .map(|i| quantile(&scores, i as f64 / BINS as f64))
        .collect::<Option<Vec<f64>>>()?;
    let pooled = quantiles(&pairs.iter().map(|(_, y)| *y).collect::<Vec<_>>())?;
    let table = Table {
        edges,
        bins: Vec::new(),
    };
    let mut by_bin: Vec<Vec<f64>> = vec![Vec::new(); BINS];
    for (s, y) in pairs {
        by_bin[table.bin_of(*s)].push(*y);
    }
    let bins = by_bin
        .iter()
        .map(|ys| {
            if ys.len() >= BIN_MIN {
                quantiles(ys).unwrap_or(pooled)
            } else {
                pooled
            }
        })
        .collect();
    Some(Table { bins, ..table })
}

/// One bin of a stored calibration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CalibrationBin {
    /// Unshifted p50/p80/p90 (ln tokens) from the full fit.
    pub quantiles: [f64; 3],
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub soft_basis: Option<SoftBasis>,
    /// Shifted p90 coverage of the shift-window issues whose score falls in
    /// this bin's interval (the final table's edges), each predicted by the
    /// shift-stage table, which never saw them. `None` when none fell in it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub heldout_coverage: Option<f64>,
    pub heldout_n: u64,
}

/// A fitted calibration: score bins, their quantiles and soft flags, and
/// the conformal shifts, for one answerer and question version.
#[derive(Debug, Clone, PartialEq)]
pub struct Calibration {
    pub version: String,
    pub answerer_version: String,
    pub question_version: String,
    pub as_of: i64,
    pub table_window: [i64; 2],
    pub shift_window: [i64; 2],
    pub fit_n: u64,
    pub shift_n: u64,
    pub edges: Vec<f64>,
    pub bins: Vec<CalibrationBin>,
    /// The p80 and p90 conformal shifts.
    pub shifts: [f64; 2],
    pub group: String,
    /// Held-out shifted p90 coverage of the largest fifth of shift-window
    /// issues by actual tokens.
    pub large_actuals_coverage: Option<f64>,
}

/// Where a score lands.
#[derive(Debug, Clone, PartialEq)]
pub struct Lookup {
    pub bin: usize,
    /// Shifted p50/p80/p90 (ln tokens).
    pub quantiles: [f64; 3],
    pub soft_basis: Option<SoftBasis>,
}

impl Calibration {
    pub fn lookup(&self, score: f64) -> Option<Lookup> {
        let bin = self.edges.iter().filter(|edge| score > **edge).count();
        let stored = self.bins.get(bin)?;
        Some(Lookup {
            bin,
            quantiles: shift(stored.quantiles, self.shifts),
            soft_basis: stored.soft_basis,
        })
    }

    pub fn provenance(&self) -> CalibrationProvenance {
        CalibrationProvenance {
            version: self.version.clone(),
            as_of: self.as_of,
            table_window: self.table_window,
            shift_window: self.shift_window,
            fit_n: self.fit_n,
            shift_n: self.shift_n,
            group: self.group.clone(),
        }
    }
}
