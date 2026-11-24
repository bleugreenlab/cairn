//! Fitting window drain: segmenting each account's readings into window
//! instances, sampling them at hour boundaries against the hourly spend,
//! non-negative least squares, the instance bootstrap, the support rule,
//! account scales, the holdout and the stability refits. Pure: [`fit`] reads
//! [`DrainInputs`] and nothing else.
//!
//! Every fit of one window shares one set of bootstrap draws, so a family's
//! interval, the pooled terms and each account's scale are percentiles of
//! the same resamples.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use super::{
    window_admits, AccountMix, AccountPlan, AccountScale, ComponentCheck, Components,
    CurrentWindow, DrainFit, DrainInputs, DrainSample, Estimate, FamilyRate, HoldoutRow,
    IntervalKind, Mix, PlanSpan, Reading, ScopeCheck, Spend, Support, Validation, WeekFit,
    WindowFit, ROUNDING_VARIANCE, Z90,
};

/// Bootstrap resamples per fit.
pub const BOOTSTRAP_DRAWS: usize = 300;
/// The bootstrap's seed, so a refit of the same data serves the same
/// intervals.
pub const BOOTSTRAP_SEED: u64 = 0x6270;
/// Instances a family must spend in, and a window must hold, before its
/// coefficient can be measured or an account scale served.
pub const MIN_MEASURED_INSTANCES: usize = 3;
/// The widest relative half-width a measured coefficient may have.
pub const MAX_RELATIVE_HALF_WIDTH: f64 = 0.5;
/// A sample is kept when a reading landed this close before its hour.
pub const FRESH_READING_SECS: i64 = 20 * 60;
/// The span an account's recent mix is taken over.
pub const RECENT_MIX_SECS: i64 = 7 * 24 * HOUR;
/// A drop in used percent larger than this starts a new instance.
pub const RESET_DROP_PCT: f64 = 1.0;

const HOUR: i64 = 3600;
const WEEK: i64 = 7 * 24 * HOUR;
/// 1970-01-01 was a Thursday; calendar weeks start on Monday.
const MONDAY_OFFSET: i64 = 4 * 24 * HOUR;

// ---------------------------------------------------------------------------
// Arithmetic
// ---------------------------------------------------------------------------

/// SplitMix64: a small, seedable generator for the bootstrap.
pub(crate) struct SplitMix(pub u64);

impl SplitMix {
    pub(crate) fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform on `[0, 1)`.
    #[cfg(test)]
    pub(crate) fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }
}

/// Solve the `m`×`m` system `a x = b` by Gaussian elimination with partial
/// pivoting, or `None` when it is singular.
fn solve(a: &[f64], b: &[f64], m: usize) -> Option<Vec<f64>> {
    let mut a = a.to_vec();
    let mut b = b.to_vec();
    let scale = a
        .iter()
        .fold(0.0f64, |acc, value| acc.max(value.abs()))
        .max(f64::MIN_POSITIVE);
    for col in 0..m {
        let pivot =
            (col..m).max_by(|&i, &j| a[i * m + col].abs().total_cmp(&a[j * m + col].abs()))?;
        if a[pivot * m + col].abs() <= scale * 1e-12 {
            return None;
        }
        if pivot != col {
            for k in 0..m {
                a.swap(col * m + k, pivot * m + k);
            }
            b.swap(col, pivot);
        }
        for row in col + 1..m {
            let factor = a[row * m + col] / a[col * m + col];
            if factor != 0.0 {
                for k in col..m {
                    a[row * m + k] -= factor * a[col * m + k];
                }
                b[row] -= factor * b[col];
            }
        }
    }
    let mut x = vec![0.0; m];
    for row in (0..m).rev() {
        let mut sum = b[row];
        for k in row + 1..m {
            sum -= a[row * m + k] * x[k];
        }
        x[row] = sum / a[row * m + row];
    }
    Some(x)
}

fn sub_system(g: &[f64], b: &[f64], m: usize, set: &[usize]) -> (Vec<f64>, Vec<f64>) {
    let n = set.len();
    let mut gs = vec![0.0; n * n];
    let mut bs = vec![0.0; n];
    for (i, &row) in set.iter().enumerate() {
        bs[i] = b[row];
        for (j, &col) in set.iter().enumerate() {
            gs[i * n + j] = g[row * m + col];
        }
    }
    (gs, bs)
}

/// Lawson-Hanson non-negative least squares on the normal equations:
/// minimize `k'Gk - 2b'k` subject to `k ≥ 0`. A column collinear with the
/// ones already in the solution is left at zero.
pub(crate) fn nnls(g: &[f64], b: &[f64], m: usize) -> Vec<f64> {
    let mut x = vec![0.0; m];
    let mut passive = vec![false; m];
    let mut banned = vec![false; m];
    let tolerance = 1e-10
        * b.iter()
            .fold(0.0f64, |acc, value| acc.max(value.abs()))
            .max(f64::MIN_POSITIVE);
    for _ in 0..3 * m + 10 {
        let gradient: Vec<f64> = (0..m)
            .map(|j| b[j] - (0..m).map(|k| g[j * m + k] * x[k]).sum::<f64>())
            .collect();
        let Some(enter) = (0..m)
            .filter(|&j| !passive[j] && !banned[j] && gradient[j] > tolerance)
            .max_by(|&i, &j| gradient[i].total_cmp(&gradient[j]))
        else {
            break;
        };
        passive[enter] = true;
        for _ in 0..m + 5 {
            let set: Vec<usize> = (0..m).filter(|&i| passive[i]).collect();
            if set.is_empty() {
                break;
            }
            let (gs, bs) = sub_system(g, b, m, &set);
            let Some(solution) = solve(&gs, &bs, set.len()) else {
                passive[enter] = false;
                banned[enter] = true;
                break;
            };
            let mut z = vec![0.0; m];
            for (i, &col) in set.iter().enumerate() {
                z[col] = solution[i];
            }
            if set.iter().all(|&i| z[i] > 0.0) {
                x = z;
                break;
            }
            let alpha = set
                .iter()
                .filter(|&&i| z[i] <= 0.0)
                .map(|&i| x[i] / (x[i] - z[i]))
                .fold(f64::INFINITY, f64::min);
            for i in 0..m {
                x[i] += alpha * (z[i] - x[i]);
            }
            let largest = x.iter().fold(0.0f64, |acc, value| acc.max(value.abs()));
            for &i in &set {
                if x[i] <= 1e-14 * (1.0 + largest) {
                    x[i] = 0.0;
                    passive[i] = false;
                }
            }
        }
    }
    x
}

/// The `p`th percentile of `values` (sorted in place), interpolated.
fn percentile(values: &mut [f64], p: f64) -> f64 {
    values.sort_by(f64::total_cmp);
    if values.is_empty() {
        return 0.0;
    }
    let rank = p * (values.len() - 1) as f64;
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;
    values[lower] + (values[upper] - values[lower]) * (rank - lower as f64)
}

fn interval(value: f64, draws: impl Iterator<Item = f64>) -> Estimate {
    let mut draws: Vec<f64> = draws.collect();
    Estimate {
        value,
        low: percentile(&mut draws, 0.05),
        high: percentile(&mut draws, 0.95),
    }
}

// ---------------------------------------------------------------------------
// Window instances
// ---------------------------------------------------------------------------

/// One window instance of one account: the readings between two resets.
#[derive(Debug, Clone, PartialEq)]
pub struct Instance {
    pub provider: String,
    pub account: String,
    pub window_id: String,
    pub window_name: String,
    pub scope_target: Option<String>,
    pub plan: String,
    /// `(captured_at, used_pct)`, oldest first.
    pub readings: Vec<(i64, f64)>,
    pub resets_at: Option<i64>,
    pub duration_mins: Option<i64>,
    /// The window has reset since: a later instance follows, or its reset
    /// time has passed.
    pub complete: bool,
}

impl Instance {
    pub fn started_at(&self) -> i64 {
        self.readings.first().map_or(0, |reading| reading.0)
    }

    pub fn ended_at(&self) -> i64 {
        self.readings.last().map_or(0, |reading| reading.0)
    }
}

/// The reset time to the nearest hour: providers jitter it by a second.
fn reset_hour(resets_at: Option<i64>) -> Option<i64> {
    resets_at.map(|at| (at + HOUR / 2).div_euclid(HOUR))
}

/// Each `(account, window id)` series, oldest reading first.
fn series(readings: &[Reading]) -> BTreeMap<(&str, &str), Vec<&Reading>> {
    let mut series: BTreeMap<(&str, &str), Vec<&Reading>> = BTreeMap::new();
    for reading in readings {
        series
            .entry((reading.account.as_str(), reading.window_id.as_str()))
            .or_default()
            .push(reading);
    }
    for readings in series.values_mut() {
        readings.sort_by_key(|reading| reading.captured_at);
    }
    series
}

/// Split one series where the reset time (to the hour) moves or the used
/// share drops by more than [`RESET_DROP_PCT`]. A series without reset times
/// splits on drops alone.
fn split<'a>(readings: &[&'a Reading]) -> Vec<Vec<&'a Reading>> {
    let mut segments: Vec<Vec<&Reading>> = Vec::new();
    let mut current: Vec<&Reading> = Vec::new();
    for reading in readings {
        if let Some(previous) = current.last() {
            let reset_moved = matches!(
                (reset_hour(previous.resets_at), reset_hour(reading.resets_at)),
                (Some(before), Some(after)) if before != after
            );
            let dropped = reading.used_pct < previous.used_pct - RESET_DROP_PCT;
            if reset_moved || dropped {
                segments.push(std::mem::take(&mut current));
            }
        }
        current.push(reading);
    }
    if !current.is_empty() {
        segments.push(current);
    }
    segments
}

/// The index of the plan span `account` held at `at`.
fn span_at(plans: &[PlanSpan], account: &str, at: i64) -> Option<usize> {
    plans
        .iter()
        .enumerate()
        .filter(|(_, span)| {
            span.account == account
                && span.started_at <= at
                && span.ended_at.is_none_or(|ended| at < ended)
        })
        .map(|(index, _)| index)
        .next_back()
}

/// Every window instance that can be fitted, and how many were left out by
/// reason: one that straddles a plan change, or ran on no recorded or no
/// priced plan.
pub fn segment(inputs: &DrainInputs) -> (Vec<Instance>, BTreeMap<String, usize>) {
    let mut instances = Vec::new();
    let mut skipped: BTreeMap<String, usize> = BTreeMap::new();
    for readings in series(&inputs.readings).values() {
        let segments = split(readings);
        let count = segments.len();
        for (index, segment) in segments.into_iter().enumerate() {
            let first = segment[0];
            let last = segment[segment.len() - 1];
            let start_span = span_at(&inputs.plans, &first.account, first.captured_at);
            let end_span = span_at(&inputs.plans, &first.account, last.captured_at);
            let reason = match (start_span, end_span) {
                (None, _) => Some("no plan recorded"),
                (Some(start), Some(end)) if start != end => Some("spans a plan change"),
                (Some(_), None) => Some("spans a plan change"),
                (Some(start), _) if inputs.plans[start].plan.is_none() => Some("unpriced plan"),
                _ => None,
            };
            if let Some(reason) = reason {
                *skipped.entry(reason.to_string()).or_default() += 1;
                continue;
            }
            let plan = start_span
                .and_then(|span| inputs.plans[span].plan.clone())
                .unwrap_or_default();
            let resets_at = last.resets_at;
            instances.push(Instance {
                provider: first.provider.clone(),
                account: first.account.clone(),
                window_id: first.window_id.clone(),
                window_name: first.window_name.clone(),
                scope_target: first.scope_target.clone(),
                plan,
                readings: segment
                    .iter()
                    .map(|reading| (reading.captured_at, reading.used_pct))
                    .collect(),
                resets_at,
                duration_mins: segment
                    .iter()
                    .filter_map(|reading| reading.duration_mins)
                    .max(),
                complete: index + 1 < count || resets_at.is_some_and(|at| at <= inputs.now),
            });
        }
    }
    (instances, skipped)
}

type SpendIndex<'a> = HashMap<&'a str, BTreeMap<i64, Vec<&'a Spend>>>;

fn index_spend(spend: &[Spend]) -> SpendIndex<'_> {
    let mut index: SpendIndex = HashMap::new();
    for row in spend {
        index
            .entry(row.account.as_str())
            .or_default()
            .entry(row.hour)
            .or_default()
            .push(row);
    }
    index
}

/// An instance sampled at each hour boundary from its first reading to its
/// last: the newest reading at or before the hour, beside the spend per
/// family from the start of the first reading's hour to that hour. A sample
/// is kept when a reading landed within [`FRESH_READING_SECS`] before it, or
/// nothing was spent since the reading it carries forward (readings are
/// written only on change, so an unchanged window is one with no new
/// reading). Nothing is extrapolated past the last reading.
pub fn samples(
    instance: &Instance,
    spend: &HashMap<&str, BTreeMap<i64, Vec<&Spend>>>,
) -> Vec<DrainSample> {
    let empty = BTreeMap::new();
    let hours = spend.get(instance.account.as_str()).unwrap_or(&empty);
    let first = instance.started_at();
    let last = instance.ended_at();
    let base = first.div_euclid(HOUR) * HOUR;
    let mut hour = if first == base { base } else { base + HOUR };
    let mut cumulative: BTreeMap<String, Mix> = BTreeMap::new();
    let mut buckets = hours.range(base..).peekable();
    let mut reading = 0;
    let mut out = Vec::new();
    while hour <= last {
        while let Some((&bucket, rows)) = buckets.peek() {
            if bucket >= hour {
                break;
            }
            for row in rows.iter() {
                cumulative
                    .entry(row.family.clone())
                    .or_default()
                    .add(&row.mix);
            }
            buckets.next();
        }
        while reading + 1 < instance.readings.len() && instance.readings[reading + 1].0 <= hour {
            reading += 1;
        }
        let (at, used_pct) = instance.readings[reading];
        let fresh = hour - at <= FRESH_READING_SECS;
        let quiet = || {
            hours
                .range(at.div_euclid(HOUR) * HOUR..hour)
                .all(|(_, rows)| rows.iter().all(|row| row.mix.tokens.total() <= 0.0))
        };
        if fresh || quiet() {
            out.push(DrainSample {
                at: hour,
                used_pct,
                spend: cumulative.clone(),
            });
        }
        hour += HOUR;
    }
    out
}

// ---------------------------------------------------------------------------
// The design: per-instance normal equations
// ---------------------------------------------------------------------------

struct Row {
    y: f64,
    x: Vec<f64>,
}

/// One instance's samples, with its within-instance demeaned normal
/// equations. Demeaning inside each instance is what the free intercept
/// `a_i` does, so a fit never estimates it.
struct InstanceDesign {
    account: String,
    started_at: i64,
    ended_at: i64,
    complete: bool,
    rows: Vec<Row>,
    gram: Vec<f64>,
    b: Vec<f64>,
    yy: f64,
    /// Each family's spend over the instance, in families order.
    mix: Vec<Mix>,
    out_of_scope_usd: f64,
}

/// The columns of every row: each admitted family's list dollars, then the
/// out-of-scope dollars, then the dominant family's four components in
/// millions of tokens.
struct Design {
    width: usize,
    families: Vec<String>,
    instances: Vec<InstanceDesign>,
}

impl Design {
    fn out_of_scope(&self) -> usize {
        self.families.len()
    }

    fn component(&self, component: usize) -> usize {
        self.families.len() + 1 + component
    }

    fn family_columns(&self) -> Vec<usize> {
        (0..self.families.len()).collect()
    }

    fn build(group: &[(&Instance, Vec<DrainSample>)], scope_target: Option<&str>) -> Self {
        let mut spend: BTreeMap<String, f64> = BTreeMap::new();
        for (_, samples) in group {
            if let Some(last) = samples.last() {
                for (family, mix) in &last.spend {
                    if window_admits(scope_target, family) {
                        *spend.entry(family.clone()).or_default() += mix.usd.total();
                    }
                }
            }
        }
        let mut families: Vec<(String, f64)> =
            spend.into_iter().filter(|(_, usd)| *usd > 0.0).collect();
        families.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        let families: Vec<String> = families.into_iter().map(|(family, _)| family).collect();
        let width = families.len() + 5;
        let dominant = families.first().cloned();
        let instances = group
            .iter()
            .map(|(instance, samples)| {
                let rows: Vec<Row> = samples
                    .iter()
                    .map(|sample| {
                        let mut x = vec![0.0; width];
                        for (family, mix) in &sample.spend {
                            match families.iter().position(|name| name == family) {
                                Some(column) => x[column] = mix.usd.total(),
                                None if !window_admits(scope_target, family) => {
                                    x[families.len()] += mix.usd.total();
                                }
                                None => {}
                            }
                        }
                        if let Some(mix) = dominant
                            .as_ref()
                            .and_then(|family| sample.spend.get(family))
                        {
                            for (component, tokens) in mix.tokens.as_array().into_iter().enumerate()
                            {
                                x[families.len() + 1 + component] = tokens / 1_000_000.0;
                            }
                        }
                        Row {
                            y: sample.used_pct,
                            x,
                        }
                    })
                    .collect();
                let n = rows.len() as f64;
                let mean_y = rows.iter().map(|row| row.y).sum::<f64>() / n;
                let mean_x: Vec<f64> = (0..width)
                    .map(|column| rows.iter().map(|row| row.x[column]).sum::<f64>() / n)
                    .collect();
                let mut gram = vec![0.0; width * width];
                let mut b = vec![0.0; width];
                let mut yy = 0.0;
                for row in &rows {
                    let dy = row.y - mean_y;
                    yy += dy * dy;
                    for i in 0..width {
                        let di = row.x[i] - mean_x[i];
                        b[i] += di * dy;
                        for j in 0..width {
                            gram[i * width + j] += di * (row.x[j] - mean_x[j]);
                        }
                    }
                }
                let last = samples.last();
                let mix = families
                    .iter()
                    .map(|family| {
                        last.and_then(|sample| sample.spend.get(family))
                            .copied()
                            .unwrap_or_default()
                    })
                    .collect();
                let out_of_scope_usd = rows.last().map_or(0.0, |row| row.x[families.len()]);
                InstanceDesign {
                    account: instance.account.clone(),
                    started_at: instance.started_at(),
                    ended_at: instance.ended_at(),
                    complete: instance.complete,
                    rows,
                    gram,
                    b,
                    yy,
                    mix,
                    out_of_scope_usd,
                }
            })
            .collect();
        Self {
            width,
            families,
            instances,
        }
    }

    /// The normal equations of `terms` (each a set of columns summed into one
    /// regressor) over the instances weighted by `weights`, with the summed
    /// `y'y`, row count and instance count.
    fn system(&self, weights: &[f64], terms: &[Vec<usize>]) -> (Vec<f64>, Vec<f64>, f64, f64, f64) {
        let p = terms.len();
        let w = self.width;
        let mut g = vec![0.0; p * p];
        let mut b = vec![0.0; p];
        let mut yy = 0.0;
        let mut rows = 0.0;
        let mut count = 0.0;
        for (instance, &weight) in self.instances.iter().zip(weights) {
            if weight == 0.0 {
                continue;
            }
            yy += weight * instance.yy;
            rows += weight * instance.rows.len() as f64;
            count += weight;
            for (i, term_i) in terms.iter().enumerate() {
                b[i] += weight * term_i.iter().map(|&c| instance.b[c]).sum::<f64>();
                for (j, term_j) in terms.iter().enumerate() {
                    let mut sum = 0.0;
                    for &ci in term_i {
                        for &cj in term_j {
                            sum += instance.gram[ci * w + cj];
                        }
                    }
                    g[i * p + j] += weight * sum;
                }
            }
        }
        (g, b, yy, rows, count)
    }

    /// Instances among `weights` in which `family` spent anything.
    fn instances_with(&self, weights: &[f64], family: usize) -> usize {
        self.instances
            .iter()
            .zip(weights)
            .filter(|(instance, weight)| **weight > 0.0 && instance.mix[family].usd.total() > 0.0)
            .count()
    }

    /// Bootstrap draws over the instances `weights` selects: each draw picks
    /// that many instances with replacement. None with too few instances.
    fn draws(&self, weights: &[f64], seed: u64) -> Vec<Vec<f64>> {
        let members: Vec<usize> = (0..self.instances.len())
            .filter(|&i| weights[i] > 0.0)
            .collect();
        if members.len() < MIN_MEASURED_INSTANCES {
            return Vec::new();
        }
        let mut rng = SplitMix(seed);
        (0..BOOTSTRAP_DRAWS)
            .map(|_| {
                let mut draw = vec![0.0; self.instances.len()];
                for _ in 0..members.len() {
                    draw[members[rng.below(members.len())]] += 1.0;
                }
                draw
            })
            .collect()
    }
}

/// One fit of a set of terms, with its draws and intervals.
struct TermFit {
    k: Vec<f64>,
    draws: Vec<Vec<f64>>,
    intervals: Vec<Estimate>,
    kind: IntervalKind,
}

fn residual_sd(g: &[f64], b: &[f64], yy: f64, rows: f64, count: f64, k: &[f64]) -> f64 {
    let p = k.len();
    let fitted: f64 = (0..p)
        .map(|i| k[i] * (0..p).map(|j| g[i * p + j] * k[j]).sum::<f64>())
        .sum();
    let rss = (yy - 2.0 * k.iter().zip(b).map(|(k, b)| k * b).sum::<f64>() + fitted).max(0.0);
    let parameters = k.iter().filter(|k| **k > 0.0).count() as f64;
    let dof = (rows - count - parameters).max(1.0);
    (rss / dof).max(ROUNDING_VARIANCE).sqrt()
}

fn fit_terms(
    design: &Design,
    weights: &[f64],
    draws: &[Vec<f64>],
    terms: &[Vec<usize>],
) -> TermFit {
    let p = terms.len();
    let (g, b, yy, rows, count) = design.system(weights, terms);
    let k = nnls(&g, &b, p);
    if !draws.is_empty() {
        let draws: Vec<Vec<f64>> = draws
            .iter()
            .map(|draw| {
                let (g, b, ..) = design.system(draw, terms);
                nnls(&g, &b, p)
            })
            .collect();
        let intervals = (0..p)
            .map(|i| interval(k[i], draws.iter().map(|draw| draw[i])))
            .collect();
        return TermFit {
            k,
            draws,
            intervals,
            kind: IntervalKind::Bootstrap,
        };
    }
    // Too few instances to resample: a per-sample standard error from the
    // active terms' normal equations.
    let sigma = residual_sd(&g, &b, yy, rows, count, &k);
    let active: Vec<usize> = (0..p).filter(|&i| k[i] > 0.0).collect();
    let (ga, _) = sub_system(&g, &b, p, &active);
    let mut variance = vec![f64::INFINITY; p];
    for (index, &term) in active.iter().enumerate() {
        let mut unit = vec![0.0; active.len()];
        unit[index] = 1.0;
        if let Some(column) = solve(&ga, &unit, active.len()) {
            variance[term] = column[index].max(0.0) * sigma * sigma;
        }
    }
    for term in 0..p {
        if k[term] == 0.0 && g[term * p + term] > 0.0 {
            variance[term] = sigma * sigma / g[term * p + term];
        }
    }
    let intervals = (0..p)
        .map(|i| {
            let half = Z90 * variance[i].sqrt();
            Estimate {
                value: k[i],
                low: (k[i] - half).max(0.0),
                high: k[i] + half,
            }
        })
        .collect();
    TermFit {
        k,
        draws: Vec::new(),
        intervals,
        kind: IntervalKind::SampleLevel,
    }
}

fn supported(estimate: &Estimate, instances: usize) -> bool {
    instances >= MIN_MEASURED_INSTANCES
        && estimate.low > 0.0
        && estimate.relative_half_width() <= MAX_RELATIVE_HALF_WIDTH
}

/// The served per-family rates of one set of instances, after the support
/// rule.
struct Served {
    rates: Vec<(Estimate, Support)>,
    /// Each family's coefficient per draw.
    draws: Vec<Vec<f64>>,
    point: Vec<f64>,
    all: Estimate,
    kind: IntervalKind,
    sd: f64,
}

fn serve(design: &Design, weights: &[f64], draws: &[Vec<f64>]) -> Served {
    let families = design.families.len();
    let singles: Vec<Vec<usize>> = (0..families).map(|f| vec![f]).collect();
    let instances: Vec<usize> = (0..families)
        .map(|f| design.instances_with(weights, f))
        .collect();
    let each = fit_terms(design, weights, draws, &singles);
    let all = fit_terms(design, weights, draws, &[design.family_columns()]);
    let measured: Vec<usize> = (0..families)
        .filter(|&f| supported(&each.intervals[f], instances[f]))
        .collect();
    let unsupported: Vec<usize> = (0..families).filter(|f| !measured.contains(f)).collect();
    let draw_count = each.draws.len();
    let mut rates = vec![(Estimate::exact(0.0), Support::Measured); families];
    let mut point = vec![0.0; families];
    let mut family_draws = vec![vec![0.0; families]; draw_count];
    let take = |rates: &mut Vec<(Estimate, Support)>,
                point: &mut Vec<f64>,
                family_draws: &mut Vec<Vec<f64>>,
                family: usize,
                fit: &TermFit,
                term: usize,
                support: Support| {
        rates[family] = (fit.intervals[term], support);
        point[family] = fit.k[term];
        for (draw, values) in family_draws.iter_mut().zip(&fit.draws) {
            draw[family] = values[term];
        }
    };
    if unsupported.is_empty() {
        for f in 0..families {
            take(
                &mut rates,
                &mut point,
                &mut family_draws,
                f,
                &each,
                f,
                Support::Measured,
            );
        }
    } else if unsupported.len() >= 2 && !measured.is_empty() {
        let mut terms: Vec<Vec<usize>> = measured.iter().map(|&f| vec![f]).collect();
        terms.push(unsupported.clone());
        let pooled = fit_terms(design, weights, draws, &terms);
        let pooled_term = terms.len() - 1;
        let pooled_instances = design
            .instances
            .iter()
            .zip(weights)
            .filter(|(instance, weight)| {
                **weight > 0.0
                    && unsupported
                        .iter()
                        .any(|&f| instance.mix[f].usd.total() > 0.0)
            })
            .count();
        for (term, &f) in measured.iter().enumerate() {
            take(
                &mut rates,
                &mut point,
                &mut family_draws,
                f,
                &pooled,
                term,
                Support::Measured,
            );
        }
        let names: Vec<String> = unsupported
            .iter()
            .map(|&f| design.families[f].clone())
            .collect();
        for &f in &unsupported {
            if supported(&pooled.intervals[pooled_term], pooled_instances) {
                take(
                    &mut rates,
                    &mut point,
                    &mut family_draws,
                    f,
                    &pooled,
                    pooled_term,
                    Support::Pooled {
                        families: names.clone(),
                    },
                );
            } else {
                take(
                    &mut rates,
                    &mut point,
                    &mut family_draws,
                    f,
                    &all,
                    0,
                    Support::PooledAll,
                );
            }
        }
    } else {
        for &f in &measured {
            take(
                &mut rates,
                &mut point,
                &mut family_draws,
                f,
                &each,
                f,
                Support::Measured,
            );
        }
        for &f in &unsupported {
            take(
                &mut rates,
                &mut point,
                &mut family_draws,
                f,
                &all,
                0,
                Support::PooledAll,
            );
        }
    }
    let (g, b, yy, rows, count) = design.system(weights, &singles);
    let distinct: BTreeSet<u64> = point
        .iter()
        .filter(|k| **k > 0.0)
        .map(|k| k.to_bits())
        .collect();
    let fitted: f64 = (0..families)
        .map(|i| {
            point[i]
                * (0..families)
                    .map(|j| g[i * families + j] * point[j])
                    .sum::<f64>()
        })
        .sum();
    let rss = (yy - 2.0 * point.iter().zip(&b).map(|(k, b)| k * b).sum::<f64>() + fitted).max(0.0);
    let dof = (rows - count - distinct.len() as f64).max(1.0);
    Served {
        rates,
        draws: family_draws,
        point,
        all: all.intervals[0],
        kind: each.kind,
        sd: (rss / dof).max(ROUNDING_VARIANCE).sqrt(),
    }
}

impl Design {
    /// `k'b` and `k'Gk` of one instance over the family columns.
    fn scale_terms(&self, instance: &InstanceDesign, k: &[f64]) -> (f64, f64) {
        let w = self.width;
        let families = self.families.len();
        let zy: f64 = (0..families).map(|i| k[i] * instance.b[i]).sum();
        let zz: f64 = (0..families)
            .map(|i| {
                k[i] * (0..families)
                    .map(|j| instance.gram[i * w + j] * k[j])
                    .sum::<f64>()
            })
            .sum();
        (zy, zz)
    }

    /// One account's scale against the rates `k`, over `weights`.
    fn account_scale(&self, account: &str, weights: &[f64], k: &[f64]) -> Option<f64> {
        let (mut zy, mut zz) = (0.0, 0.0);
        for (instance, weight) in self.instances.iter().zip(weights) {
            if *weight > 0.0 && instance.account == account {
                let (y, z) = self.scale_terms(instance, k);
                zy += weight * y;
                zz += weight * z;
            }
        }
        (zz > 0.0).then(|| zy / zz)
    }

    /// Predict an instance's used percent at each of its samples from its
    /// first: the first reading plus the drain of what was spent since.
    fn predict(&self, instance: &InstanceDesign, k: &[f64], scale: f64) -> Vec<(f64, f64)> {
        let first = &instance.rows[0];
        instance
            .rows
            .iter()
            .map(|row| {
                let drain: f64 = (0..self.families.len())
                    .map(|f| k[f] * (row.x[f] - first.x[f]))
                    .sum();
                (row.y, first.y + scale * drain)
            })
            .collect()
    }
}

/// The calendar week (Monday 00:00 UTC) `at` falls in.
fn week_start(at: i64) -> i64 {
    (at - MONDAY_OFFSET).div_euclid(WEEK) * WEEK + MONDAY_OFFSET
}

fn default_duration(window_name: &str) -> i64 {
    if window_name.eq_ignore_ascii_case("session") {
        5 * 60
    } else {
        7 * 24 * 60
    }
}

fn fit_window(group: &[(&Instance, Vec<DrainSample>)], seed: u64) -> WindowFit {
    let head = group[0].0;
    let scope_target = head.scope_target.as_deref();
    let design = Design::build(group, scope_target);
    let n = design.instances.len();
    let families = design.families.len();
    let everyone = vec![1.0; n];
    let draws = design.draws(&everyone, seed);
    let served = serve(&design, &everyone, &draws);

    let accounts: BTreeSet<&str> = design
        .instances
        .iter()
        .map(|i| i.account.as_str())
        .collect();
    let account_scales = accounts
        .iter()
        .filter_map(|account| {
            let scale = design.account_scale(account, &everyone, &served.point)?;
            let estimate = interval(
                scale,
                draws
                    .iter()
                    .zip(&served.draws)
                    .filter_map(|(draw, k)| design.account_scale(account, draw, k)),
            );
            let instances = design
                .instances
                .iter()
                .filter(|i| i.account == *account)
                .count();
            let distinct = instances >= MIN_MEASURED_INSTANCES && !draws.is_empty();
            let served = if distinct && !estimate.contains(1.0) {
                scale
            } else {
                1.0
            };
            Some(AccountScale {
                account: account.to_string(),
                scale: estimate,
                instances,
                served,
            })
        })
        .collect();

    let rates = design
        .families
        .iter()
        .enumerate()
        .map(|(f, family)| {
            let mut mix = Mix::default();
            for instance in &design.instances {
                mix.add(&instance.mix[f]);
            }
            FamilyRate {
                family: family.clone(),
                percent_per_usd: served.rates[f].0,
                support: served.rates[f].1.clone(),
                instances: design.instances_with(&everyone, f),
                spend_usd: mix.usd.total(),
                mix,
            }
        })
        .collect();

    WindowFit {
        provider: head.provider.clone(),
        plan: head.plan.clone(),
        window_name: head.window_name.clone(),
        scope_target: head.scope_target.clone(),
        duration_mins: group
            .iter()
            .filter_map(|(instance, _)| instance.duration_mins)
            .max()
            .unwrap_or_else(|| default_duration(&head.window_name)),
        instances: n,
        accounts: accounts.len(),
        samples: design.instances.iter().map(|i| i.rows.len()).sum(),
        interval: served.kind,
        families: rates,
        all: served.all,
        residual_sd: served.sd,
        account_scales,
        validation: validate(&design, seed),
        component_check: component_check(&design, &everyone, &draws, &served),
        stability: stability(&design, &served),
        scope_check: scope_target
            .filter(|_| design.instances.iter().any(|i| i.out_of_scope_usd > 0.0) && families > 0)
            .map(|_| {
                let mut terms: Vec<Vec<usize>> = (0..families).map(|f| vec![f]).collect();
                terms.push(vec![design.out_of_scope()]);
                let fit = fit_terms(&design, &everyone, &draws, &terms);
                ScopeCheck {
                    out_of_scope_usd: design.instances.iter().map(|i| i.out_of_scope_usd).sum(),
                    percent_per_usd: fit.intervals[families],
                }
            }),
    }
}

/// Hold out each account's latest complete instance, fit the rest with the
/// same rules, and predict the held-out instances from their tokens.
fn validate(design: &Design, seed: u64) -> Option<Validation> {
    let mut holdout: BTreeMap<&str, usize> = BTreeMap::new();
    for (index, instance) in design.instances.iter().enumerate() {
        if !instance.complete {
            continue;
        }
        let entry = holdout.entry(instance.account.as_str()).or_insert(index);
        if instance.started_at > design.instances[*entry].started_at {
            *entry = index;
        }
    }
    let held: BTreeSet<usize> = holdout.values().copied().collect();
    let training: Vec<f64> = (0..design.instances.len())
        .map(|i| if held.contains(&i) { 0.0 } else { 1.0 })
        .collect();
    let training_instances = training.iter().filter(|w| **w > 0.0).count();
    if held.is_empty() || training_instances == 0 {
        return None;
    }
    let draws = design.draws(&training, seed ^ 0x5eed);
    let served = serve(design, &training, &draws);
    let mut rows = Vec::new();
    let mut squared = 0.0;
    let mut count = 0usize;
    for &index in &held {
        let instance = &design.instances[index];
        let scale = design
            .account_scale(&instance.account, &training, &served.point)
            .unwrap_or(1.0);
        let pooled = design.predict(instance, &served.point, 1.0);
        let own = design.predict(instance, &served.point, scale);
        for (observed, predicted) in pooled.iter().skip(1) {
            squared += (observed - predicted).powi(2);
            count += 1;
        }
        let (observed, predicted) = *pooled.last()?;
        rows.push(HoldoutRow {
            account: instance.account.clone(),
            started_at: instance.started_at,
            ended_at: instance.ended_at,
            observed,
            predicted,
            predicted_per_account: own.last()?.1,
        });
    }
    let mean = |error: &dyn Fn(&HoldoutRow) -> f64| {
        rows.iter().map(error).sum::<f64>() / rows.len() as f64
    };
    Some(Validation {
        training_instances,
        end_mae: mean(&|row| (row.observed - row.predicted).abs()),
        end_mae_per_account: mean(&|row| (row.observed - row.predicted_per_account).abs()),
        sample_rmse: if count > 0 {
            (squared / count as f64).sqrt()
        } else {
            0.0
        },
        rows,
    })
}

/// A free fit of the dominant family's four components beside the other
/// families' dollars, against what its list-dollar rate implies per million
/// tokens of each component.
fn component_check(
    design: &Design,
    weights: &[f64],
    draws: &[Vec<f64>],
    served: &Served,
) -> Option<ComponentCheck> {
    let families = design.families.len();
    if families == 0 || design.instances_with(weights, 0) < MIN_MEASURED_INSTANCES {
        return None;
    }
    let mut terms: Vec<Vec<usize>> = (0..4).map(|c| vec![design.component(c)]).collect();
    terms.extend((1..families).map(|f| vec![f]));
    let fit = fit_terms(design, weights, draws, &terms);
    let mut mix = Mix::default();
    for instance in &design.instances {
        mix.add(&instance.mix[0]);
    }
    let tokens = mix.tokens.as_array();
    let usd = mix.usd.as_array();
    let k = served.point[0];
    Some(ComponentCheck {
        family: design.families[0].clone(),
        fitted: [
            fit.intervals[0],
            fit.intervals[1],
            fit.intervals[2],
            fit.intervals[3],
        ],
        list_implied: std::array::from_fn(|c| {
            if tokens[c] > 0.0 {
                k * usd[c] / tokens[c] * 1_000_000.0
            } else {
                0.0
            }
        }),
        usd_share: std::array::from_fn(|c| {
            if mix.usd.total() > 0.0 {
                usd[c] / mix.usd.total()
            } else {
                0.0
            }
        }),
    })
}

/// Each measured family's coefficient refitted on one calendar week's
/// instances at a time, the other families pooled as one term.
fn stability(design: &Design, served: &Served) -> Vec<WeekFit> {
    let measured: Vec<usize> = (0..design.families.len())
        .filter(|&f| served.rates[f].1 == Support::Measured)
        .collect();
    if measured.is_empty() {
        return Vec::new();
    }
    let rest: Vec<usize> = (0..design.families.len())
        .filter(|f| !measured.contains(f))
        .collect();
    let mut terms: Vec<Vec<usize>> = measured.iter().map(|&f| vec![f]).collect();
    if !rest.is_empty() {
        terms.push(rest);
    }
    let weeks: BTreeSet<i64> = design
        .instances
        .iter()
        .map(|i| week_start(i.started_at))
        .collect();
    weeks
        .into_iter()
        .map(|week| {
            let weights: Vec<f64> = design
                .instances
                .iter()
                .map(|i| {
                    if week_start(i.started_at) == week {
                        1.0
                    } else {
                        0.0
                    }
                })
                .collect();
            let fit = fit_terms(design, &weights, &[], &terms);
            WeekFit {
                week_start: week,
                instances: weights.iter().filter(|w| **w > 0.0).count(),
                percent_per_usd: measured
                    .iter()
                    .enumerate()
                    .filter(|(_, &f)| design.instances_with(&weights, f) > 0)
                    .map(|(term, &f)| (design.families[f].clone(), fit.k[term]))
                    .collect(),
            }
        })
        .collect()
}

/// Each series' latest reading, with its burn over the last 24 hours of the
/// instance it is in. `readings` must reach back over each series' current
/// instance and the day before its latest reading.
pub fn current_windows(readings: &[Reading]) -> Vec<CurrentWindow> {
    let mut out = Vec::new();
    for readings in series(readings).values() {
        let Some(segment) = split(readings).pop() else {
            continue;
        };
        let latest = segment[segment.len() - 1];
        let since = latest.captured_at - 24 * HOUR;
        let reference = segment
            .iter()
            .rev()
            .find(|reading| reading.captured_at <= since)
            .unwrap_or(&segment[0]);
        let hours = (latest.captured_at - reference.captured_at) as f64 / HOUR as f64;
        out.push(CurrentWindow {
            provider: latest.provider.clone(),
            account: latest.account.clone(),
            window_id: latest.window_id.clone(),
            window_name: latest.window_name.clone(),
            scope_target: latest.scope_target.clone(),
            used_pct: latest.used_pct,
            resets_at: latest.resets_at,
            captured_at: latest.captured_at,
            burn_pct_per_hour: (hours >= 1.0)
                .then(|| (latest.used_pct - reference.used_pct) / hours),
        });
    }
    out
}

/// The plan each account holds at `now`, or its latest when none is open.
fn plans_now(plans: &[PlanSpan], now: i64) -> Vec<AccountPlan> {
    let accounts: BTreeSet<&str> = plans.iter().map(|span| span.account.as_str()).collect();
    accounts
        .into_iter()
        .filter_map(|account| {
            let span = span_at(plans, account, now)
                .map(|index| &plans[index])
                .or_else(|| {
                    plans
                        .iter()
                        .filter(|span| span.account == account)
                        .max_by_key(|span| span.started_at)
                })?;
            Some(AccountPlan {
                account: account.to_string(),
                provider: span.provider.clone(),
                plan: span.plan.clone(),
            })
        })
        .collect()
}

fn mixes(inputs: &DrainInputs) -> Vec<AccountMix> {
    let mut by_key: BTreeMap<(&str, &str), (Option<Mix>, Mix)> = BTreeMap::new();
    for row in &inputs.spend {
        let entry = by_key
            .entry((row.account.as_str(), row.family.as_str()))
            .or_default();
        entry.1.add(&row.mix);
        if row.hour >= inputs.now - RECENT_MIX_SECS && row.mix.tokens.total() > 0.0 {
            entry.0.get_or_insert_with(Mix::default).add(&row.mix);
        }
    }
    by_key
        .into_iter()
        .map(|((account, family), (recent, all_time))| AccountMix {
            account: account.to_string(),
            family: family.to_string(),
            recent,
            all_time,
        })
        .collect()
}

/// One (provider, plan, window kind).
type GroupKey<'a> = (&'a str, &'a str, &'a str);

/// Fit every (provider, plan, window kind) the inputs hold an instance of.
pub fn fit(inputs: &DrainInputs) -> DrainFit {
    let (instances, skipped) = segment(inputs);
    let spend = index_spend(&inputs.spend);
    let mut groups: BTreeMap<GroupKey<'_>, Vec<(&Instance, Vec<DrainSample>)>> = BTreeMap::new();
    for instance in &instances {
        let samples = samples(instance, &spend);
        if samples.len() < 2 {
            continue;
        }
        groups
            .entry((
                instance.provider.as_str(),
                instance.plan.as_str(),
                instance.window_name.as_str(),
            ))
            .or_default()
            .push((instance, samples));
    }
    let windows = groups
        .values()
        .enumerate()
        .map(|(index, group)| fit_window(group, BOOTSTRAP_SEED.wrapping_add(index as u64)))
        .collect();
    let mut provider_of: HashMap<&str, &str> = HashMap::new();
    for reading in &inputs.readings {
        provider_of
            .entry(&reading.account)
            .or_insert(&reading.provider);
    }
    for span in &inputs.plans {
        provider_of.entry(&span.account).or_insert(&span.provider);
    }
    let families = inputs
        .spend
        .iter()
        .filter_map(|row| {
            let provider = provider_of.get(row.account.as_str())?;
            Some((row.family.clone(), provider.to_string()))
        })
        .collect();
    DrainFit {
        fitted_at: inputs.now,
        windows,
        plans: plans_now(&inputs.plans, inputs.now),
        mixes: mixes(inputs),
        rates: inputs.rates.clone(),
        families,
        dropped: inputs.dropped.clone(),
        skipped_instances: skipped,
        readings: inputs.readings.len(),
    }
}

/// The components of [`Components`] as list dollars at `rates`.
pub fn priced(tokens: Components, rates: &super::Rates) -> Mix {
    Mix {
        tokens,
        usd: Components {
            input: tokens.input * rates.input / 1_000_000.0,
            cache_read: tokens.cache_read * rates.cache_read / 1_000_000.0,
            cache_write: tokens.cache_write * rates.cache_write / 1_000_000.0,
            output: tokens.output * rates.output / 1_000_000.0,
        },
    }
}

#[cfg(test)]
mod tests;
