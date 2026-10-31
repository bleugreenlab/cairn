//! Learning the vibe axes from history: the one implementation both the
//! in-app recompute ([`super::vibe_recompute`]) and the offline regenerator
//! (`examples/compute_vibe_axes.rs`) call.
//!
//! An axis is a behavioural contrast between two classes of assistant events,
//! told apart by where each sits in its run, never by a human label:
//!
//! | Axis | Positive | Negative |
//! |---|---|---|
//! | PHASE (explore to ship) | `run_progress > 0.85` | `run_progress < 0.15` |
//! | FRICTION (smooth to recovering) | an errored tool result at most 2 events behind | the nearest one at least 15 behind, or none |
//!
//! The axis is the unit mean-difference vector `norm(mean(pos) - mean(neg))`
//! of the classes' unit-normalised embeddings. Its percentile table is the
//! 101-point empirical distribution of a separate, unbiased draw of events
//! projected onto it: the classes are the tails of the run by construction,
//! so they cannot stand in for the population a new event is ranked against.
//!
//! Because the labels come from the transcript alone, the axes mean the same
//! thing in every model's space, which is why a colour from one model and a
//! colour from the next can sit side by side in one skyline.

use serde::{Deserialize, Serialize};

use super::vibes::{AxisSet, VibeAxis};

/// The PHASE axis's name.
pub const PHASE: &str = "phase";
/// The FRICTION axis's name.
pub const FRICTION: &str = "friction";

/// Where an event sits in its run, measured in event steps over every event
/// the run recorded.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunPosition {
    /// `0.0` at the run's first event, `1.0` at its last.
    pub run_progress: f64,
    /// Steps back to the nearest errored tool result; `None` when there is none.
    pub dist_prev_error: Option<i64>,
    /// Steps forward to the nearest errored tool result; `None` when there is none.
    pub dist_next_error: Option<i64>,
}

/// Which class of a contrast an event falls in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Positive,
    Negative,
}

impl RunPosition {
    /// PHASE: ship (late in the run) against explore (early in it).
    pub fn phase(&self) -> Option<Side> {
        if self.run_progress > 0.85 {
            Some(Side::Positive)
        } else if self.run_progress < 0.15 {
            Some(Side::Negative)
        } else {
            None
        }
    }

    /// FRICTION: just after a tool error against far from any. An event with
    /// no error behind it is as smooth as one can be.
    pub fn friction(&self) -> Option<Side> {
        match self.dist_prev_error {
            Some(distance) if distance <= 2 => Some(Side::Positive),
            Some(distance) if distance >= 15 => Some(Side::Negative),
            Some(_) => None,
            None => Some(Side::Negative),
        }
    }
}

/// The position of every event of one run, given in order whether each is an
/// errored tool result.
pub fn run_positions(errors: &[bool]) -> Vec<RunPosition> {
    let n = errors.len();
    let progress = |i: usize| {
        if n > 1 {
            i as f64 / (n - 1) as f64
        } else {
            0.0
        }
    };
    let mut prev = vec![None; n];
    let mut last: Option<usize> = None;
    for (i, is_error) in errors.iter().enumerate() {
        prev[i] = last.map(|at| (i - at) as i64);
        if *is_error {
            last = Some(i);
        }
    }
    let mut next = vec![None; n];
    let mut following: Option<usize> = None;
    for i in (0..n).rev() {
        next[i] = following.map(|at| (at - i) as i64);
        if errors[i] {
            following = Some(i);
        }
    }
    (0..n)
        .map(|i| RunPosition {
            run_progress: progress(i),
            dist_prev_error: prev[i],
            dist_next_error: next[i],
        })
        .collect()
}

/// One embedded event the axes are learned from.
#[derive(Debug, Clone, PartialEq)]
pub struct Sample {
    pub vector: Vec<f32>,
    pub position: RunPosition,
    /// Drawn without regard to its class, so it may stand in the percentile
    /// table's population.
    pub in_distribution: bool,
}

/// How many events each class and the percentile population hold.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClassCounts {
    pub phase_positive: u64,
    pub phase_negative: u64,
    pub friction_positive: u64,
    pub friction_negative: u64,
    pub distribution: u64,
}

impl ClassCounts {
    /// Count `samples` into their classes.
    pub fn of<'a>(samples: impl IntoIterator<Item = (&'a RunPosition, bool)>) -> Self {
        let mut counts = Self::default();
        for (position, in_distribution) in samples {
            counts.add(position, in_distribution);
        }
        counts
    }

    pub fn add(&mut self, position: &RunPosition, in_distribution: bool) {
        match position.phase() {
            Some(Side::Positive) => self.phase_positive += 1,
            Some(Side::Negative) => self.phase_negative += 1,
            None => {}
        }
        match position.friction() {
            Some(Side::Positive) => self.friction_positive += 1,
            Some(Side::Negative) => self.friction_negative += 1,
            None => {}
        }
        if in_distribution {
            self.distribution += 1;
        }
    }

    /// Whether every class, and the percentile population, holds `min`.
    pub fn enough(&self, min: u64) -> bool {
        [
            self.phase_positive,
            self.phase_negative,
            self.friction_positive,
            self.friction_negative,
            self.distribution,
        ]
        .iter()
        .all(|count| *count >= min)
    }
}

/// Why no axes were learned.
#[derive(Debug, Clone, PartialEq)]
pub enum AxesError {
    /// A class, or the percentile population, holds fewer than `needed`
    /// events: a contrast that thin is mostly noise.
    TooFew { counts: ClassCounts, needed: u64 },
    /// The samples are not all one width, so they are not one space.
    MixedWidths,
}

impl std::fmt::Display for AxesError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooFew { counts, needed } => write!(
                f,
                "not enough labelled history: phase {}/{}, friction {}/{}, distribution {} (each needs {needed})",
                counts.phase_positive,
                counts.phase_negative,
                counts.friction_positive,
                counts.friction_negative,
                counts.distribution
            ),
            Self::MixedWidths => f.write_str("the samples are not all one width"),
        }
    }
}

/// Learn the PHASE and FRICTION axes of `model`'s space from `samples`,
/// refusing when any class or the percentile population holds fewer than
/// `min_per_class`.
pub fn compute_axes(
    model: &str,
    samples: &[Sample],
    min_per_class: u64,
) -> Result<AxisSet, AxesError> {
    let counts = ClassCounts::of(
        samples
            .iter()
            .map(|sample| (&sample.position, sample.in_distribution)),
    );
    if !counts.enough(min_per_class) {
        return Err(AxesError::TooFew {
            counts,
            needed: min_per_class,
        });
    }
    let dims = samples[0].vector.len();
    if samples.iter().any(|sample| sample.vector.len() != dims) {
        return Err(AxesError::MixedWidths);
    }

    let units: Vec<Vec<f32>> = samples.iter().map(|sample| unit(&sample.vector)).collect();
    let classes = |side: fn(&RunPosition) -> Option<Side>| {
        let mut positive = Vec::new();
        let mut negative = Vec::new();
        for (sample, vector) in samples.iter().zip(&units) {
            match side(&sample.position) {
                Some(Side::Positive) => positive.push(vector.clone()),
                Some(Side::Negative) => negative.push(vector.clone()),
                None => {}
            }
        }
        contrast_axis(&positive, &negative)
    };
    let population: Vec<&Vec<f32>> = samples
        .iter()
        .zip(&units)
        .filter(|(sample, _)| sample.in_distribution)
        .map(|(_, vector)| vector)
        .collect();

    let axes = [
        (PHASE, classes(RunPosition::phase)),
        (FRICTION, classes(RunPosition::friction)),
    ]
    .into_iter()
    .map(|(name, vector)| {
        let mut projections: Vec<f32> = population.iter().map(|v| dot(v, &vector)).collect();
        projections.sort_by(f32::total_cmp);
        VibeAxis {
            name: name.to_string(),
            percentiles: percentile_table(&projections),
            vector,
        }
    })
    .collect();

    Ok(AxisSet {
        model: model.to_string(),
        dims: dims as u32,
        axes,
    })
}

/// The cosine between two axis sets' same-named axes, in `[PHASE, FRICTION]`
/// order; `None` for an axis either lacks or at mismatched widths. How the
/// offline regenerator checks a recompute against the shipped axes.
pub fn axis_agreement(a: &AxisSet, b: &AxisSet) -> [Option<f32>; 2] {
    [PHASE, FRICTION].map(|name| {
        let (x, y) = (a.axis(name)?, b.axis(name)?);
        (x.vector.len() == y.vector.len()).then(|| dot(&unit(&x.vector), &unit(&y.vector)))
    })
}

fn unit(v: &[f32]) -> Vec<f32> {
    let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm == 0.0 {
        v.to_vec()
    } else {
        v.iter().map(|x| x / norm).collect()
    }
}

fn dot(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}

/// The element-wise mean of `vectors`; empty for none.
fn mean(vectors: &[Vec<f32>]) -> Vec<f32> {
    let Some(first) = vectors.first() else {
        return Vec::new();
    };
    let mut sum = vec![0.0_f32; first.len()];
    for vector in vectors {
        for (total, value) in sum.iter_mut().zip(vector) {
            *total += value;
        }
    }
    let n = vectors.len() as f32;
    sum.iter().map(|total| total / n).collect()
}

/// The unit mean-difference vector `norm(mean(positive) - mean(negative))`.
fn contrast_axis(positive: &[Vec<f32>], negative: &[Vec<f32>]) -> Vec<f32> {
    let difference: Vec<f32> = mean(positive)
        .iter()
        .zip(mean(negative))
        .map(|(p, n)| p - n)
        .collect();
    unit(&difference)
}

/// The 101 breakpoints `p0..=p100` of ascending `sorted`, interpolated
/// linearly between ranks as numpy's default percentile is.
fn percentile_table(sorted: &[f32]) -> Vec<f32> {
    (0..=100)
        .map(|p| percentile(sorted, p as f32 / 100.0))
        .collect()
}

fn percentile(sorted: &[f32], q: f32) -> f32 {
    match sorted.len() {
        0 => 0.0,
        1 => sorted[0],
        n => {
            let rank = q * (n - 1) as f32;
            let lo = rank.floor() as usize;
            let hi = (lo + 1).min(n - 1);
            sorted[lo] + (rank - lo as f32) * (sorted[hi] - sorted[lo])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::embeddings::VectorKey;
    use crate::embeddings::VibeState;

    fn at(run_progress: f64, dist_prev_error: Option<i64>) -> RunPosition {
        RunPosition {
            run_progress,
            dist_prev_error,
            dist_next_error: None,
        }
    }

    #[test]
    fn positions_measure_progress_and_the_nearest_error_each_way() {
        //            0      1     2      3      4     5
        let errors = [false, true, false, false, true, false];
        let positions = run_positions(&errors);
        let prev: Vec<_> = positions.iter().map(|p| p.dist_prev_error).collect();
        let next: Vec<_> = positions.iter().map(|p| p.dist_next_error).collect();
        assert_eq!(prev, [None, None, Some(1), Some(2), Some(3), Some(1)]);
        assert_eq!(next, [Some(1), Some(3), Some(2), Some(1), None, None]);
        assert_eq!(positions[0].run_progress, 0.0);
        assert_eq!(positions[5].run_progress, 1.0);
        assert!((positions[2].run_progress - 0.4).abs() < 1e-9);
        assert_eq!(run_positions(&[false])[0].run_progress, 0.0);
        assert!(run_positions(&[]).is_empty());
    }

    #[test]
    fn the_labels_partition_at_the_research_thresholds() {
        assert_eq!(at(0.86, None).phase(), Some(Side::Positive));
        assert_eq!(at(0.85, None).phase(), None, "the threshold is exclusive");
        assert_eq!(at(0.5, None).phase(), None);
        assert_eq!(at(0.15, None).phase(), None);
        assert_eq!(at(0.14, None).phase(), Some(Side::Negative));

        assert_eq!(at(0.5, Some(1)).friction(), Some(Side::Positive));
        assert_eq!(at(0.5, Some(2)).friction(), Some(Side::Positive));
        assert_eq!(at(0.5, Some(3)).friction(), None);
        assert_eq!(at(0.5, Some(14)).friction(), None);
        assert_eq!(at(0.5, Some(15)).friction(), Some(Side::Negative));
        assert_eq!(
            at(0.5, None).friction(),
            Some(Side::Negative),
            "no error behind is smooth"
        );

        let counts = ClassCounts::of([
            (&at(0.9, Some(1)), true),
            (&at(0.1, None), false),
            (&at(0.5, Some(5)), true),
        ]);
        assert_eq!(
            counts,
            ClassCounts {
                phase_positive: 1,
                phase_negative: 1,
                friction_positive: 1,
                friction_negative: 1,
                distribution: 2,
            }
        );
    }

    #[test]
    fn the_axis_is_the_unit_mean_difference() {
        let positive = vec![vec![1.0, 0.0, 0.0], vec![1.0, 0.0, 0.0]];
        let negative = vec![vec![0.0, 1.0, 0.0]];
        let axis = contrast_axis(&positive, &negative);
        let half = std::f32::consts::FRAC_1_SQRT_2;
        for (got, want) in axis.iter().zip([half, -half, 0.0]) {
            assert!((got - want).abs() < 1e-6, "{axis:?}");
        }
    }

    #[test]
    fn the_percentile_table_interpolates_like_numpy() {
        let sorted: Vec<f32> = (0..=100).map(|i| i as f32).collect();
        for (got, want) in percentile_table(&sorted).iter().zip(&sorted) {
            assert!((got - want).abs() < 1e-4, "{got} != {want}");
        }
        // numpy.percentile([0, 10], [0, 25, 50, 100]) == [0, 2.5, 5, 10]
        let table = percentile_table(&[0.0, 10.0]);
        assert_eq!(table.len(), 101);
        assert_eq!(
            (table[0], table[25], table[50], table[100]),
            (0.0, 2.5, 5.0, 10.0)
        );
        assert_eq!(percentile_table(&[]), vec![0.0; 101]);
    }

    /// Three dimensions: x is PHASE (ship +x, explore -x), y is FRICTION
    /// (after an error +y, smooth -y), z a shared offset every vector carries.
    fn fixed_samples(per_class: usize) -> Vec<Sample> {
        let mut samples = Vec::new();
        for i in 0..per_class {
            let jitter = i as f32 * 0.01;
            for (vector, position) in [
                (vec![1.0, jitter, 1.0], at(0.95, Some(40))),
                (vec![-1.0, -jitter, 1.0], at(0.05, Some(40))),
                (vec![jitter, 1.0, 1.0], at(0.5, Some(1))),
                (vec![-jitter, -1.0, 1.0], at(0.5, None)),
            ] {
                samples.push(Sample {
                    vector,
                    position,
                    in_distribution: true,
                });
            }
        }
        samples
    }

    #[test]
    fn axes_are_learned_from_fixed_vectors() {
        let set = compute_axes("m", &fixed_samples(10), 5).unwrap();
        assert_eq!((set.model.as_str(), set.dims), ("m", 3));
        let phase = set.axis(PHASE).unwrap();
        let friction = set.axis(FRICTION).unwrap();
        assert!(phase.vector[0] > 0.95, "{:?}", phase.vector);
        assert!(friction.vector[1] > 0.6, "{:?}", friction.vector);
        assert!(phase.vector[2].abs() < 1e-6, "the shared offset cancels");
        for axis in [phase, friction] {
            assert_eq!(axis.percentiles.len(), 101);
            assert!(axis.percentiles.windows(2).all(|w| w[0] <= w[1]));
        }

        // The learned set colours a ship event greener than an explore one.
        let state = VibeState::empty();
        state.insert(set);
        let key = VectorKey {
            model: "m".into(),
            dims: Some(3),
        };
        let ship = state.assign_one("a", &key, &[1.0, 0.0, 1.0]).unwrap();
        let explore = state.assign_one("b", &key, &[-1.0, 0.0, 1.0]).unwrap();
        assert!(
            ship.phase > 0.6 && explore.phase < 0.4,
            "{ship:?} {explore:?}"
        );
    }

    #[test]
    fn too_few_in_any_class_is_refused_with_the_counts() {
        let mut samples = fixed_samples(10);
        // Drop every friction-positive event but two.
        let mut kept = 0;
        samples.retain(|sample| {
            if sample.position.friction() == Some(Side::Positive) {
                kept += 1;
                kept <= 2
            } else {
                true
            }
        });
        match compute_axes("m", &samples, 5) {
            Err(AxesError::TooFew { counts, needed }) => {
                assert_eq!(needed, 5);
                assert_eq!(counts.friction_positive, 2);
                assert_eq!(counts.phase_positive, 10);
            }
            other => panic!("expected a refusal, got {other:?}"),
        }

        let mut few_drawn = fixed_samples(10);
        for sample in few_drawn.iter_mut().skip(3) {
            sample.in_distribution = false;
        }
        assert!(matches!(
            compute_axes("m", &few_drawn, 5),
            Err(AxesError::TooFew { counts, .. }) if counts.distribution == 3
        ));
        assert!(matches!(
            compute_axes("m", &[], 1),
            Err(AxesError::TooFew { .. })
        ));
    }

    #[test]
    fn mixed_widths_are_refused() {
        let mut samples = fixed_samples(3);
        samples[0].vector.push(0.0);
        assert_eq!(compute_axes("m", &samples, 1), Err(AxesError::MixedWidths));
    }

    #[test]
    fn agreement_is_the_cosine_per_axis() {
        let set = compute_axes("m", &fixed_samples(10), 5).unwrap();
        let [phase, friction] = axis_agreement(&set, &set);
        assert!((phase.unwrap() - 1.0).abs() < 1e-5);
        assert!((friction.unwrap() - 1.0).abs() < 1e-5);
        let bundled = AxisSet::bundled().unwrap();
        assert_eq!(
            axis_agreement(&set, &bundled),
            [None, None],
            "widths differ"
        );
    }
}
