//! Vibe coloring: behavioral 2-axis color assignment from Cohere-space embeddings.
//!
//! Each assistant message is projected onto two orthogonal, behaviorally-grounded
//! contrast axes learned from the real event corpus (96k+ assistant events,
//! embedded with the production `cohere.embed-v4:0`):
//!
//! - **PHASE** (run-phase explore → ship): low = exploration/investigation,
//!   high = completion/shipping. Linear-probe CV-AUC 0.905.
//! - **FRICTION** (post-error tool-use friction): low = smooth, high = recovering
//!   from tool errors. CV-AUC 0.875, orthogonal to PHASE (cos −0.006).
//!
//! Each projection is **percentile-rank-normalized** against the empirical corpus
//! distribution — the embedding cone makes absolute cosine useless for saturation
//! — into `[0,1]`, then mapped to an OKLCH color: PHASE drives the base hue
//! (indigo → green) and FRICTION bends the hue toward red while raising chroma and
//! darkening lightness, so tool-use friction *pops* in the skyline. The axes and
//! their percentile tables are computed offline and bundled as `vibe_axes.json`
//! (regenerate with `examples/compute_vibe_axes.rs`).

use serde::Deserialize;

use super::vector::cosine_similarity;

// ===== color ramp constants (provisional / tunable) =====
//
// PHASE sets the base hue along the indigo→green arc; FRICTION pulls it toward
// red and raises chroma + darkens lightness so friction stands out.
const H_EXPLORE: f32 = 265.0; // indigo/blue at phase01 = 0 (explore)
const H_SHIP: f32 = 150.0; // green at phase01 = 1 (ship)
const H_FRICTION: f32 = 28.0; // red target as friction rises
const W_HUE: f32 = 0.85; // max fraction of the way to red at friction01 = 1
const C_CALM: f32 = 0.06; // chroma at friction01 = 0 (muted phase hue)
const C_HOT: f32 = 0.21; // chroma at friction01 = 1 (friction pops)
const L_CALM: f32 = 0.72; // lightness at friction01 = 0
const L_HOT: f32 = 0.62; // lightness at friction01 = 1 (slightly darker/alarming)

/// A behaviorally-grounded contrast axis: a unit direction in embedding space
/// plus the empirical percentile breakpoints of corpus projections onto it.
#[derive(Debug, Clone)]
pub struct VibeAxis {
    /// Axis name (`"phase"` | `"friction"`).
    pub name: String,
    /// 1536-d unit contrast vector (`norm(mean(pos) - mean(neg))`).
    pub vector: Vec<f32>,
    /// 101 sorted breakpoints `p0..=p100` of the empirical projection
    /// distribution, used to percentile-rank a new projection into `[0,1]`.
    pub percentiles: Vec<f32>,
}

/// The result of assigning a vibe to an event: a CSS color plus the two
/// percentile-ranked axis coordinates (persisted alongside the color, so richer
/// frontend rendering can use them later).
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VibeAssignment {
    pub event_id: String,
    pub css_color: String,
    /// PHASE coordinate in `[0,1]` (0 = explore, 1 = ship).
    pub phase: f32,
    /// FRICTION coordinate in `[0,1]` (0 = smooth, 1 = post-error friction).
    pub friction: f32,
}

/// Holds the loaded contrast axes, ready for assignment. Expects a `phase` and a
/// `friction` axis; an empty set assigns no color (callers fall back to neutral).
pub struct VibeState {
    pub axes: Vec<VibeAxis>,
}

#[derive(Deserialize)]
struct BundledAxes {
    #[allow(dead_code)]
    model: String,
    #[allow(dead_code)]
    dims: u32,
    axes: Vec<BundledAxis>,
}

#[derive(Deserialize)]
struct BundledAxis {
    name: String,
    vector: Vec<f32>,
    percentiles: Vec<f32>,
}

/// Cohere-space contrast axes + percentile tables generated offline.
/// Regenerate with `cargo run --example compute_vibe_axes --features internal-api`.
const BUNDLED_AXES: &str = include_str!("vibe_axes.json");

impl VibeState {
    /// Load the bundled contrast axes. Returns an empty `VibeState` (which
    /// assigns no colors — callers fall back to neutral) if the bundle can't be
    /// parsed, so a bad bundle degrades gracefully rather than panicking.
    pub fn from_bundled() -> Self {
        match serde_json::from_str::<BundledAxes>(BUNDLED_AXES) {
            Ok(bundle) => {
                let axes = bundle
                    .axes
                    .into_iter()
                    .map(|a| VibeAxis {
                        name: a.name,
                        vector: a.vector,
                        percentiles: a.percentiles,
                    })
                    .collect::<Vec<_>>();
                log::info!("VibeState loaded {} contrast axes", axes.len());
                Self { axes }
            }
            Err(e) => {
                log::error!("Failed to parse bundled vibe axes: {e}");
                Self { axes: Vec::new() }
            }
        }
    }

    /// Find a loaded axis by name.
    fn axis(&self, name: &str) -> Option<&VibeAxis> {
        self.axes.iter().find(|a| a.name == name)
    }

    /// Assign a vibe color to a single embedding. Returns `None` when the `phase`
    /// or `friction` axis is missing (color falls back to neutral upstream).
    pub fn assign_one(&self, event_id: &str, embedding: &[f32]) -> Option<VibeAssignment> {
        let phase_axis = self.axis("phase")?;
        let friction_axis = self.axis("friction")?;

        // The axis is a unit vector and `cosine_similarity` normalizes the
        // embedding, so this is the normalized projection the percentile tables
        // were built from.
        let phase = percentile_rank(
            cosine_similarity(embedding, &phase_axis.vector),
            &phase_axis.percentiles,
        );
        let friction = percentile_rank(
            cosine_similarity(embedding, &friction_axis.vector),
            &friction_axis.percentiles,
        );

        Some(VibeAssignment {
            event_id: event_id.to_string(),
            css_color: color_for(phase, friction),
            phase,
            friction,
        })
    }
}

/// Map a projection value to its rank in `[0,1]` within the empirical percentile
/// breakpoints (`breaks[i]` is the i-th percentile, `i` in `0..=100`). Clamps to
/// the observed range, then linearly interpolates between the two bracketing
/// percentiles. Returns 0.0 for a degenerate (< 2 point) table.
fn percentile_rank(x: f32, breaks: &[f32]) -> f32 {
    if breaks.len() < 2 {
        return 0.0;
    }
    let last = breaks.len() - 1;
    if x <= breaks[0] {
        return 0.0;
    }
    if x >= breaks[last] {
        return 1.0;
    }
    // `breaks` is sorted ascending. `partition_point` returns the count of
    // elements strictly less than `x` = the index of the first element >= `x`.
    // Since `breaks[0] < x < breaks[last]`, `i` is in `1..=last`.
    let i = breaks.partition_point(|&b| b < x);
    let b0 = breaks[i - 1];
    let b1 = breaks[i];
    let frac = if (b1 - b0).abs() < f32::EPSILON {
        0.0
    } else {
        (x - b0) / (b1 - b0)
    };
    ((i - 1) as f32 + frac) / last as f32
}

/// Map percentile-ranked PHASE and FRICTION coordinates (both in `[0,1]`) to
/// OKLCH `(lightness, chroma, hue)`. PHASE sets the base hue (explore indigo →
/// ship green); FRICTION bends the hue toward red and raises chroma while
/// darkening lightness so tool-use friction stands out.
fn color_components(phase01: f32, friction01: f32) -> (f32, f32, f32) {
    let phase01 = phase01.clamp(0.0, 1.0);
    let friction01 = friction01.clamp(0.0, 1.0);
    let hue_phase = H_EXPLORE + (H_SHIP - H_EXPLORE) * phase01;
    let hue = shortarc_lerp(hue_phase, H_FRICTION, W_HUE * friction01);
    let chroma = C_CALM + (C_HOT - C_CALM) * friction01;
    let light = L_CALM + (L_HOT - L_CALM) * friction01;
    (light, chroma, hue)
}

/// Generate a CSS `oklch()` string for the given axis coordinates.
fn color_for(phase01: f32, friction01: f32) -> String {
    let (light, chroma, hue) = color_components(phase01, friction01);
    format!("oklch({:.2} {:.3} {:.0})", light, chroma, hue)
}

/// Interpolate between two hue angles along the shorter arc, returning a value in
/// `[0,360)`.
fn shortarc_lerp(a: f32, b: f32, t: f32) -> f32 {
    let delta = ((b - a + 540.0) % 360.0) - 180.0;
    (a + delta * t).rem_euclid(360.0)
}

/// Compute the element-wise average of a set of vectors.
/// Used by the offline axis generator.
pub fn average_vectors(vectors: &[Vec<f32>]) -> Vec<f32> {
    if vectors.is_empty() {
        return vec![];
    }
    let dims = vectors[0].len();
    let n = vectors.len() as f32;
    let mut avg = vec![0.0_f32; dims];
    for v in vectors {
        for (i, val) in v.iter().enumerate() {
            avg[i] += val;
        }
    }
    for val in &mut avg {
        *val /= n;
    }
    avg
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A 101-point linear ramp from `lo` to `hi`, like a percentile table whose
    /// rank is `(x - lo) / (hi - lo)`.
    fn ramp(lo: f32, hi: f32) -> Vec<f32> {
        (0..=100)
            .map(|i| lo + (hi - lo) * (i as f32 / 100.0))
            .collect()
    }

    #[test]
    fn percentile_rank_clamps_below_and_above() {
        let b = ramp(-1.0, 1.0);
        assert_eq!(percentile_rank(-5.0, &b), 0.0);
        assert_eq!(percentile_rank(-1.0, &b), 0.0);
        assert_eq!(percentile_rank(5.0, &b), 1.0);
        assert_eq!(percentile_rank(1.0, &b), 1.0);
    }

    #[test]
    fn percentile_rank_interpolates() {
        let b = ramp(-1.0, 1.0);
        // Midpoint of the distribution -> rank 0.5.
        assert!(
            (percentile_rank(0.0, &b) - 0.5).abs() < 1e-3,
            "{}",
            percentile_rank(0.0, &b)
        );
        // A quarter of the way -> rank ~0.25.
        assert!((percentile_rank(-0.5, &b) - 0.25).abs() < 1e-3);
        assert!((percentile_rank(0.5, &b) - 0.75).abs() < 1e-3);
    }

    #[test]
    fn percentile_rank_monotonic() {
        let b = ramp(-0.2, 0.3);
        let mut prev = -1.0_f32;
        for k in 0..=20 {
            let x = -0.3 + 0.05 * k as f32;
            let r = percentile_rank(x, &b);
            assert!(r >= prev - 1e-6, "rank decreased at x={x}: {r} < {prev}");
            assert!((0.0..=1.0).contains(&r));
            prev = r;
        }
    }

    #[test]
    fn percentile_rank_degenerate_table() {
        assert_eq!(percentile_rank(0.5, &[]), 0.0);
        assert_eq!(percentile_rank(0.5, &[0.1]), 0.0);
    }

    #[test]
    fn color_components_friction_raises_chroma_and_darkens() {
        let (l_calm, c_calm, _) = color_components(0.5, 0.0);
        let (l_hot, c_hot, _) = color_components(0.5, 1.0);
        assert!((c_calm - C_CALM).abs() < 1e-6);
        assert!((c_hot - C_HOT).abs() < 1e-6);
        assert!(c_hot > c_calm, "friction should raise chroma");
        assert!(l_hot < l_calm, "friction should darken lightness");
    }

    #[test]
    fn color_components_phase_runs_indigo_to_green() {
        // At zero friction the hue is the phase hue, indigo (explore) -> green (ship).
        let (_, _, hue_explore) = color_components(0.0, 0.0);
        let (_, _, hue_ship) = color_components(1.0, 0.0);
        assert!((hue_explore - H_EXPLORE).abs() < 1e-3);
        assert!((hue_ship - H_SHIP).abs() < 1e-3);
        // Ship is greener (lower hue) than explore.
        assert!(hue_ship < hue_explore);
    }

    #[test]
    fn color_components_friction_bends_ship_hue_toward_red() {
        // A ship-phase event (green ~150) under friction shifts warmward (toward
        // red 28) along the short arc, i.e. to a smaller hue.
        let (_, _, calm) = color_components(1.0, 0.0);
        let (_, _, hot) = color_components(1.0, 1.0);
        assert!(
            hot < calm,
            "friction should warm the ship hue: {hot} !< {calm}"
        );
    }

    #[test]
    fn shortarc_lerp_takes_short_way_across_zero() {
        // 10 -> 350 short arc goes down through 0; midpoint is 0.
        assert!((shortarc_lerp(10.0, 350.0, 0.5)).abs() < 1e-3);
        // 350 -> 10 short arc goes up through 0; midpoint is 0 (== 360).
        let m = shortarc_lerp(350.0, 10.0, 0.5);
        assert!(m < 1e-3 || (m - 360.0).abs() < 1e-3, "got {m}");
        // Endpoints.
        assert!((shortarc_lerp(40.0, 90.0, 0.0) - 40.0).abs() < 1e-3);
        assert!((shortarc_lerp(40.0, 90.0, 1.0) - 90.0).abs() < 1e-3);
    }

    #[test]
    fn color_for_formats_oklch() {
        let s = color_for(0.0, 0.0);
        assert_eq!(
            s,
            format!("oklch({:.2} {:.3} {:.0})", L_CALM, C_CALM, H_EXPLORE)
        );
    }

    #[test]
    fn average_vectors_basic() {
        let vecs = vec![vec![1.0, 2.0, 3.0], vec![3.0, 4.0, 5.0]];
        assert_eq!(average_vectors(&vecs), vec![2.0, 3.0, 4.0]);
    }

    #[test]
    fn average_vectors_empty() {
        let vecs: Vec<Vec<f32>> = vec![];
        assert!(average_vectors(&vecs).is_empty());
    }

    #[test]
    fn bundled_axes_load() {
        let state = VibeState::from_bundled();
        assert_eq!(state.axes.len(), 2, "expected phase + friction axes");
        let names: Vec<&str> = state.axes.iter().map(|a| a.name.as_str()).collect();
        assert!(names.contains(&"phase"));
        assert!(names.contains(&"friction"));
        for axis in &state.axes {
            assert_eq!(axis.vector.len(), 1536, "axis {} wrong dims", axis.name);
            assert_eq!(
                axis.percentiles.len(),
                101,
                "axis {} wrong percentile count",
                axis.name
            );
        }
    }

    #[test]
    fn bundled_phase_percentiles_match_findings() {
        // The corpus-derived PHASE projection distribution should reproduce the
        // validated research endpoints (FINDINGS: p05~-0.17, p50~+0.03, p95~+0.21).
        let state = VibeState::from_bundled();
        let phase = state.axis("phase").expect("phase axis");
        assert!(
            (phase.percentiles[5] - (-0.173)).abs() < 0.01,
            "p05={}",
            phase.percentiles[5]
        );
        assert!(
            (phase.percentiles[50] - 0.027).abs() < 0.01,
            "p50={}",
            phase.percentiles[50]
        );
        assert!(
            (phase.percentiles[95] - 0.214).abs() < 0.01,
            "p95={}",
            phase.percentiles[95]
        );
    }

    #[test]
    fn assign_one_none_without_axes() {
        let state = VibeState { axes: Vec::new() };
        assert!(state.assign_one("evt", &[1.0, 0.0]).is_none());
    }

    #[test]
    fn assign_one_none_with_only_one_axis() {
        let state = VibeState {
            axes: vec![VibeAxis {
                name: "phase".into(),
                vector: vec![1.0, 0.0],
                percentiles: ramp(-1.0, 1.0),
            }],
        };
        assert!(state.assign_one("evt", &[1.0, 0.0]).is_none());
    }

    #[test]
    fn assign_one_orders_phase_and_friction() {
        // Synthetic orthogonal axes: phase along x, friction along y, each with a
        // symmetric [-1,1] percentile ramp so rank ~ (proj + 1) / 2.
        let state = VibeState {
            axes: vec![
                VibeAxis {
                    name: "phase".into(),
                    vector: vec![1.0, 0.0],
                    percentiles: ramp(-1.0, 1.0),
                },
                VibeAxis {
                    name: "friction".into(),
                    vector: vec![0.0, 1.0],
                    percentiles: ramp(-1.0, 1.0),
                },
            ],
        };

        // High phase (points along +x), neutral friction.
        let ship = state.assign_one("ship", &[1.0, 0.0]).unwrap();
        // Low phase (points along -x).
        let explore = state.assign_one("explore", &[-1.0, 0.0]).unwrap();
        assert!(ship.phase > explore.phase);
        assert!((ship.phase - 1.0).abs() < 1e-3);
        assert!(explore.phase < 1e-3);
        // Neutral friction (orthogonal to y) ranks mid.
        assert!((ship.friction - 0.5).abs() < 1e-2);

        // High friction (points along +y) vs smooth (-y).
        let stuck = state.assign_one("stuck", &[0.0, 1.0]).unwrap();
        let smooth = state.assign_one("smooth", &[0.0, -1.0]).unwrap();
        assert!(stuck.friction > smooth.friction);
        assert!((stuck.friction - 1.0).abs() < 1e-3);
        assert!(smooth.friction < 1e-3);

        // Friction raises chroma: the stuck event's color is more saturated.
        let (_, c_stuck, _) = color_components(stuck.phase, stuck.friction);
        let (_, c_smooth, _) = color_components(smooth.phase, smooth.friction);
        assert!(c_stuck > c_smooth);
    }
}
