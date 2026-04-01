//! Vibe coloring: semantic color assignment from embeddings.
//!
//! Each assistant message gets a color reflecting its cognitive state —
//! progress, discovery, success, uncertainty, struggle — based on
//! proximity to predefined loci in embedding space.

use super::engine::EmbeddingEngine;

/// OKLCH color specification for a vibe locus.
#[derive(Debug, Clone)]
pub struct VibeColor {
    /// Lightness (0.0–1.0)
    pub l: f32,
    /// Maximum chroma (0.0–0.4) — modulated by similarity
    pub c: f32,
    /// Hue (0–360)
    pub h: f32,
}

impl VibeColor {
    /// Generate a CSS `oklch()` string with chroma scaled by similarity.
    pub fn to_css(&self, similarity: f32) -> String {
        let chroma = self.c * chroma_scale(similarity);
        format!("oklch({:.2} {:.3} {:.0})", self.l, chroma, self.h)
    }
}

/// Configuration for a single vibe locus before centroid computation.
#[derive(Debug, Clone)]
pub struct LocusConfig {
    pub name: String,
    pub color: VibeColor,
    pub examples: Vec<String>,
}

/// A computed vibe locus with its centroid embedding.
#[derive(Debug, Clone)]
pub struct VibeLocus {
    pub name: String,
    pub color: VibeColor,
    pub centroid: Vec<f32>,
}

/// The result of assigning a vibe to an event.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VibeAssignment {
    pub event_id: String,
    pub locus: String,
    pub css_color: String,
    pub similarity: f32,
}

/// Holds computed loci with centroids, ready for assignment.
pub struct VibeState {
    pub loci: Vec<VibeLocus>,
}

impl VibeState {
    /// Build VibeState by embedding example texts and computing centroids.
    pub fn new(engine: &mut EmbeddingEngine, configs: Vec<LocusConfig>) -> Result<Self, String> {
        let mut loci = Vec::with_capacity(configs.len());

        for config in configs {
            if config.examples.is_empty() {
                return Err(format!("Locus '{}' has no examples", config.name));
            }

            let embeddings = engine
                .embed(config.examples.clone())
                .map_err(|e| format!("Failed to embed examples for '{}': {}", config.name, e))?;

            let centroid = average_vectors(&embeddings);

            loci.push(VibeLocus {
                name: config.name,
                color: config.color,
                centroid,
            });
        }

        Ok(Self { loci })
    }

    /// Assign vibe colors to a batch of embeddings.
    ///
    /// Each input is (event_id, embedding_vector). Returns one VibeAssignment per input.
    pub fn assign(&self, embeddings: &[(String, Vec<f32>)]) -> Vec<VibeAssignment> {
        embeddings
            .iter()
            .map(|(event_id, embedding)| {
                let (best_locus, best_sim) = self.closest_locus(embedding);
                VibeAssignment {
                    event_id: event_id.clone(),
                    locus: best_locus.name.clone(),
                    css_color: best_locus.color.to_css(best_sim),
                    similarity: best_sim,
                }
            })
            .collect()
    }

    /// Find the closest locus to an embedding vector.
    fn closest_locus(&self, embedding: &[f32]) -> (&VibeLocus, f32) {
        self.loci
            .iter()
            .map(|locus| {
                let sim = EmbeddingEngine::cosine_similarity(embedding, &locus.centroid);
                (locus, sim)
            })
            .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
            .expect("VibeState must have at least one locus")
    }
}

/// Chroma modulation based on similarity.
///
/// Thresholds calibrated against actual event similarity distribution
/// (55K events, all-MiniLM-L6-v2): p15=0.185, p50=0.280, p75=0.363.
///
/// - similarity ≥ 0.35 → full chroma (1.0)  — top ~27% of events
/// - similarity 0.18–0.35 → linear ramp      — middle ~58%
/// - similarity < 0.18 → low chroma (0.15)   — bottom ~15%
fn chroma_scale(similarity: f32) -> f32 {
    if similarity >= 0.35 {
        1.0
    } else if similarity >= 0.18 {
        let t = (similarity - 0.18) / 0.17; // 0.0 at 0.18, 1.0 at 0.35
        0.15 + 0.85 * t
    } else {
        0.15
    }
}

/// Compute the element-wise average of a set of vectors.
fn average_vectors(vectors: &[Vec<f32>]) -> Vec<f32> {
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

/// Default loci with examples drawn from real assistant event data.
///
/// Each locus has ~15 examples: a mix of hand-crafted anchors and real
/// high-confidence texts (similarity > 0.6, margin > 0.1) from 55K
/// embedded events. More diverse examples → better centroids.
pub fn default_loci() -> Vec<LocusConfig> {
    vec![
        // Progress: actively investigating, reading code, moving through tasks.
        // The default state — forward motion without strong emotion.
        LocusConfig {
            name: "progress".into(),
            color: VibeColor {
                l: 0.65,
                c: 0.25,
                h: 250.0,
            },
            examples: vec![
                "Let me read the issue first and understand the context.".into(),
                "I'll explore the codebase to understand the structure.".into(),
                "Now let me check the existing implementation.".into(),
                "Moving to the second task now.".into(),
                "Let me look at how this is currently handled.".into(),
                "I need to understand the existing code before making changes.".into(),
                // Real data — high confidence, high margin
                "Let me explore the existing code to understand what I'm working with.".into(),
                "Now let me read the files to understand the current implementation before making changes.".into(),
                "Let me examine the current schema and code to understand the full picture.".into(),
                "Let me start by understanding the current implementation and the issue.".into(),
                "Let me read the relevant files to understand the current implementation.".into(),
                "Let me start by reading the relevant files to understand the current implementation.".into(),
                "Let me look at the existing code to understand the current structure.".into(),
                "Now let me read the existing files to understand the current implementation.".into(),
            ],
        },
        // Discovery: insight, synthesis, seeing the bigger picture, design moments.
        // Rare but meaningful — "aha" moments and architectural understanding.
        LocusConfig {
            name: "discovery".into(),
            color: VibeColor {
                l: 0.72,
                c: 0.22,
                h: 190.0,
            },
            examples: vec![
                "Interesting! The architecture here is quite elegant.".into(),
                "I see — the system already handles this case through the event pipeline.".into(),
                // Real data — synthesis, insight, design
                "That's an interesting architectural shift! Let me explore the current implementation to understand the trade-offs.".into(),
                "Excellent exploration results. Now I have a complete picture. Let me synthesize this into a unified design.".into(),
                "This is an interesting architectural question. Let me explore the current state and think through the tradeoffs.".into(),
                "Now I have a complete picture. Let me formulate the design approach.".into(),
                "Excellent question! This is a critical architectural consideration.".into(),
                "This is a great area to explore. Let me first ground myself in the current architecture.".into(),
                "Now I have enough context. Let me write a comprehensive plan for this feature.".into(),
                "Good exploration results. I now understand the architecture.".into(),
                "Now I have a complete picture. Let me design the implementation plan.".into(),
                "Let me think through the design carefully.".into(),
            ],
        },
        // Success: completion, things working, tests passing, wrapping up.
        // The payoff moments — green means "this worked".
        LocusConfig {
            name: "success".into(),
            color: VibeColor {
                l: 0.75,
                c: 0.25,
                h: 145.0,
            },
            examples: vec![
                "Done! All changes have been committed.".into(),
                "Perfect — that fixed it!".into(),
                "All done! The feature is implemented and working.".into(),
                "Successfully completed the implementation.".into(),
                "Everything is working now, PR is ready.".into(),
                "All tests passing, everything looks great.".into(),
                // Real data — completion, verification, PR creation
                "Done. All the requested fixes have been applied and the build passes.".into(),
                "Perfect! The fix is complete. Now I'll create a PR.".into(),
                "Excellent! The build succeeded. The fix is complete and ready.".into(),
                "All fixes are done. Let me summarize the changes.".into(),
                "Perfect! Both changes are complete. Now let me mark this task as completed.".into(),
                "Perfect! All tests passed. Now let me create a PR for this fix.".into(),
                "Perfect! Now let me mark the last task as completed and verify the changes look good.".into(),
                "Great! The build succeeded. Now I should create a PR for the changes.".into(),
            ],
        },
        // Uncertainty: doubt, not sure, debugging, trying variations.
        // Distinct from progress (which is confident forward motion) —
        // uncertainty means the path isn't clear.
        LocusConfig {
            name: "uncertainty".into(),
            color: VibeColor {
                l: 0.78,
                c: 0.22,
                h: 80.0,
            },
            examples: vec![
                "Hmm, let me try some variations to see what works.".into(),
                "Still not seeing the expected behavior here.".into(),
                "Still testing — the output looks the same as before.".into(),
                "I'm not entirely sure this is the right approach.".into(),
                "Let me reconsider — there might be a better way.".into(),
                "Let me add some debug output to see exactly what's happening.".into(),
                // Real data — genuine doubt, retracing steps
                "I see the issue. Let me trace through the logic more carefully.".into(),
                "Let me try a different approach — just look at the output directly.".into(),
                "You're right, sorry. Let me restore the tests and actually debug this.".into(),
                "Let me check the test output one more time to see if it's done yet.".into(),
                "Something is off here. Let me re-examine the assumptions.".into(),
                "That didn't work as expected. Let me rethink this.".into(),
                "Wait, I think I misread the code. Let me look again.".into(),
            ],
        },
        // Struggle: errors, failures, broken builds, things going wrong.
        // Red means "something is broken" — the strongest vibe signal.
        LocusConfig {
            name: "struggle".into(),
            color: VibeColor {
                l: 0.60,
                c: 0.25,
                h: 25.0,
            },
            examples: vec![
                "Both tools are failing with the same error.".into(),
                "Same error again — this approach isn't working.".into(),
                "The build is broken and I can't figure out why.".into(),
                "The tests are failing because of a missing dependency.".into(),
                // Real data — errors, CI failures, repeated problems
                "Still failing. Let me see what the actual error is now.".into(),
                "The CI is still failing. Let me check what the error is now.".into(),
                "Several issues to fix. Let me see the full error list first.".into(),
                "Build failure — the merge resolution may have introduced issues.".into(),
                "Let me run the migration to see the exact error.".into(),
                "Another conflict in the documentation. Let me check and resolve it.".into(),
                "The test is failing. Let me add better error reporting to see what the actual error is.".into(),
                "I need to fix the failing tests. Let me check the issues.".into(),
                "Let me run the build again to check for any remaining errors.".into(),
            ],
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chroma_scale_high_similarity() {
        assert_eq!(chroma_scale(0.8), 1.0);
        assert_eq!(chroma_scale(0.35), 1.0);
    }

    #[test]
    fn test_chroma_scale_mid_similarity() {
        let scale = chroma_scale(0.265);
        assert!(
            scale > 0.15 && scale < 1.0,
            "Expected mid-range, got {}",
            scale
        );
        // At 0.265: t = (0.265 - 0.18) / 0.17 = 0.5, scale = 0.15 + 0.85 * 0.5 = 0.575
        assert!((scale - 0.575).abs() < 1e-6);
    }

    #[test]
    fn test_chroma_scale_low_similarity() {
        assert_eq!(chroma_scale(0.1), 0.15);
        assert_eq!(chroma_scale(0.0), 0.15);
    }

    #[test]
    fn test_vibe_color_to_css() {
        let color = VibeColor {
            l: 0.70,
            c: 0.25,
            h: 250.0,
        };
        // High similarity → full chroma
        assert_eq!(color.to_css(0.8), "oklch(0.70 0.250 250)");
        // Low similarity → reduced chroma (0.25 * 0.15 = 0.0375)
        assert_eq!(color.to_css(0.1), "oklch(0.70 0.038 250)");
    }

    #[test]
    fn test_average_vectors() {
        let vecs = vec![vec![1.0, 2.0, 3.0], vec![3.0, 4.0, 5.0]];
        let avg = average_vectors(&vecs);
        assert_eq!(avg, vec![2.0, 3.0, 4.0]);
    }

    #[test]
    fn test_average_vectors_single() {
        let vecs = vec![vec![1.0, 2.0, 3.0]];
        let avg = average_vectors(&vecs);
        assert_eq!(avg, vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn test_average_vectors_empty() {
        let vecs: Vec<Vec<f32>> = vec![];
        let avg = average_vectors(&vecs);
        assert!(avg.is_empty());
    }

    #[test]
    fn test_default_loci_count() {
        let loci = default_loci();
        assert_eq!(loci.len(), 5);
        let names: Vec<&str> = loci.iter().map(|l| l.name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "progress",
                "discovery",
                "success",
                "uncertainty",
                "struggle"
            ]
        );
    }

    #[test]
    fn test_vibe_state_assign_winner_take_all() {
        // Create a simple VibeState with 2 loci at known positions
        let state = VibeState {
            loci: vec![
                VibeLocus {
                    name: "alpha".into(),
                    color: VibeColor {
                        l: 0.7,
                        c: 0.25,
                        h: 250.0,
                    },
                    centroid: vec![1.0, 0.0, 0.0],
                },
                VibeLocus {
                    name: "beta".into(),
                    color: VibeColor {
                        l: 0.7,
                        c: 0.25,
                        h: 145.0,
                    },
                    centroid: vec![0.0, 1.0, 0.0],
                },
            ],
        };

        // Vector close to alpha
        let assignments = state.assign(&[
            ("evt-1".into(), vec![0.9, 0.1, 0.0]),
            ("evt-2".into(), vec![0.1, 0.9, 0.0]),
        ]);

        assert_eq!(assignments.len(), 2);
        assert_eq!(assignments[0].locus, "alpha");
        assert_eq!(assignments[1].locus, "beta");
        assert!(assignments[0].similarity > 0.9);
        assert!(assignments[1].similarity > 0.9);
    }

    #[test]
    fn test_chroma_scale_at_boundary_018() {
        // At exactly 0.18: t = 0, scale = 0.15
        let scale = chroma_scale(0.18);
        assert!(
            (scale - 0.15).abs() < 1e-6,
            "At 0.18 boundary, got {}",
            scale
        );
    }

    #[test]
    fn test_chroma_scale_just_below_boundary_035() {
        // At 0.34: should still be in the ramp region
        let scale = chroma_scale(0.34);
        assert!(scale < 1.0 && scale > 0.15, "Expected ramp, got {}", scale);
    }

    #[test]
    fn test_closest_locus_picks_first_on_equal_similarity() {
        // When two loci are equidistant, max_by picks the last one
        // (or first depending on iterator behavior). The important thing
        // is that it doesn't panic and returns a valid assignment.
        let state = VibeState {
            loci: vec![
                VibeLocus {
                    name: "a".into(),
                    color: VibeColor {
                        l: 0.7,
                        c: 0.25,
                        h: 250.0,
                    },
                    centroid: vec![1.0, 0.0],
                },
                VibeLocus {
                    name: "b".into(),
                    color: VibeColor {
                        l: 0.7,
                        c: 0.25,
                        h: 145.0,
                    },
                    centroid: vec![0.0, 1.0],
                },
            ],
        };

        // Vector equidistant from both (45 degrees)
        let assignments = state.assign(&[("eq".into(), vec![1.0, 1.0])]);
        assert_eq!(assignments.len(), 1);
        // Should pick one of the two without panicking
        assert!(assignments[0].locus == "a" || assignments[0].locus == "b");
        // Similarity should be the same to both (~0.707)
        assert!(assignments[0].similarity > 0.5);
    }

    #[test]
    fn test_vibe_state_assign_css_color_varies_by_similarity() {
        let state = VibeState {
            loci: vec![VibeLocus {
                name: "only".into(),
                color: VibeColor {
                    l: 0.7,
                    c: 0.25,
                    h: 250.0,
                },
                centroid: vec![1.0, 0.0, 0.0],
            }],
        };

        let assignments = state.assign(&[
            ("close".into(), vec![1.0, 0.0, 0.0]), // identical → high sim
            ("far".into(), vec![0.0, 0.0, 1.0]),   // orthogonal → low sim
        ]);

        // Close should have higher chroma than far
        assert!(assignments[0].similarity > assignments[1].similarity);
        // Both assigned to "only" (winner-take-all with 1 locus)
        assert_eq!(assignments[0].locus, "only");
        assert_eq!(assignments[1].locus, "only");
    }
}
