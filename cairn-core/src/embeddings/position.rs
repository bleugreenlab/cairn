//! Session semantic position — pure math (CAIRN-1138).
//!
//! Two granularities, both maintained in-memory by the embed worker from the
//! event vectors it would otherwise discard:
//!
//! - **Live position** (`SessionLive`): per-session, dual-timescale EMA over
//!   event embeddings with pivot detection. Recency-biased — it answers "where
//!   is this session right now?" Persisted to `sessions.current_pos`.
//! - **Summary centroid** (`OwnerAccum`): per-node/chat, a substance-weighted
//!   running mean (`sum` + total `weight`). A durable post-hoc signpost —
//!   "what was this node about, overall?" Persisted to `resource_embeddings`
//!   at the node/chat URI.
//!
//! This module is pure: no DB, no network, no clock beyond the `Instant`s the
//! worker passes in. Everything here is unit-testable with fixed vectors.
//!
//! The constants in [`PositionConfig::default`] are **provisional calibration**
//! — reasonable starting points, expected to be tuned against real sessions.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use super::vector::cosine_similarity;

/// Which feed an event came from. Drives the substance weight (at the enqueue
/// site) and, in the worker, whether the event also receives a vibe color
/// (`Agent` and `User` events are colored).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PositionKind {
    /// User turn — high weight, colored. Strong topical signal.
    User,
    /// Agent content/thinking — the conceptual feed. Colored.
    Agent,
    /// `write` tool-use — structural signal (commit message + touched
    /// targets, never file contents). No vibe color.
    Change,
}

/// Position metadata attached to an `EmbedJob::Event` at the enqueue site,
/// where the session id, feed kind, and token counts are known. `weight` is the
/// precomputed substance weight (see [`PositionConfig::weight_for`]).
#[derive(Debug, Clone, PartialEq)]
pub struct PositionMeta {
    pub session_id: String,
    pub kind: PositionKind,
    pub weight: f32,
}

impl PositionMeta {
    pub fn new(session_id: impl Into<String>, kind: PositionKind, weight: f32) -> Self {
        Self {
            session_id: session_id.into(),
            kind,
            weight,
        }
    }
}

/// Calibration knobs for the position engine.
///
/// PROVISIONAL — the defaults are reasonable starting points, not tuned values.
#[derive(Debug, Clone)]
pub struct PositionConfig {
    /// EMA rate for the fast (recent) timescale.
    pub alpha_fast: f32,
    /// EMA rate for the slow (anchor) timescale.
    pub alpha_slow: f32,
    /// Below this cosine(slow, event) the slow anchor is treated as stale and
    /// re-anchors faster (a topic pivot).
    pub pivot_threshold: f32,
    /// Multiplier applied to `alpha_slow` when a pivot is detected.
    pub pivot_boost: f32,
    /// Role multiplier for user-turn substance weight.
    pub role_user: f32,
    /// Role multiplier for agent-content substance weight.
    pub role_agent: f32,
    /// Role multiplier for change-signal substance weight.
    pub role_change: f32,
}

impl Default for PositionConfig {
    fn default() -> Self {
        // PROVISIONAL calibration constants. Cohere Embed v4 vectors sit in a
        // tight cone (cosines cluster high), so the pivot threshold is set well
        // below the typical similarity rather than at a geometric midpoint.
        Self {
            alpha_fast: 0.45,
            alpha_slow: 0.12,
            pivot_threshold: 0.55,
            pivot_boost: 3.0,
            role_user: 2.0,
            role_agent: 1.0,
            role_change: 1.2,
        }
    }
}

impl PositionConfig {
    pub fn role_multiplier(&self, kind: PositionKind) -> f32 {
        match kind {
            PositionKind::User => self.role_user,
            PositionKind::Agent => self.role_agent,
            PositionKind::Change => self.role_change,
        }
    }

    /// Substance weight = substance units (token count, or a length estimate
    /// when token counts are absent) times the role multiplier.
    pub fn weight_for(&self, kind: PositionKind, tokens: Option<i32>, text: &str) -> f32 {
        substance_units(tokens, text) * self.role_multiplier(kind)
    }
}

/// Estimate an event's substance in token-ish units: prefer a real token count,
/// else ~4 characters per token. Floored at 1.0 so every event contributes.
pub fn substance_units(tokens: Option<i32>, text: &str) -> f32 {
    let raw = match tokens {
        Some(t) if t > 0 => t as f32,
        _ => text.chars().count() as f32 / 4.0,
    };
    raw.max(1.0)
}

/// L2-normalize, returning the input unchanged when it is all-zeros or empty.
pub fn normalize(v: &[f32]) -> Vec<f32> {
    let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm == 0.0 {
        v.to_vec()
    } else {
        v.iter().map(|x| x / norm).collect()
    }
}

fn lerp(a: &[f32], b: &[f32], t: f32) -> Vec<f32> {
    a.iter().zip(b).map(|(x, y)| x + (y - x) * t).collect()
}

/// One session's live position: dual-timescale EMA over event vectors.
#[derive(Debug, Clone)]
pub struct SessionLive {
    pub fast: Vec<f32>,
    pub slow: Vec<f32>,
    /// Resolved node/chat URI this session rolls up into, if known. `None`
    /// means the summary centroid can't be routed (live position still works).
    pub owner_uri: Option<String>,
    pub last_seen: Instant,
    pub dirty: bool,
}

impl SessionLive {
    fn new(owner_uri: Option<String>, now: Instant) -> Self {
        Self {
            fast: Vec::new(),
            slow: Vec::new(),
            owner_uri,
            last_seen: now,
            dirty: false,
        }
    }

    /// Fold an event vector into the live position. Returns the new current_pos.
    fn fold(&mut self, v: &[f32], cfg: &PositionConfig, now: Instant) -> Vec<f32> {
        let v = normalize(v);
        if self.fast.is_empty() || self.slow.is_empty() {
            // First event seeds both timescales directly.
            self.fast = v.clone();
            self.slow = v;
        } else {
            // Pivot: if the slow anchor disagrees with this event, let it
            // re-anchor faster so the position tracks a topic switch.
            let slow_sim = cosine_similarity(&self.slow, &v);
            let alpha_slow = if slow_sim < cfg.pivot_threshold {
                (cfg.alpha_slow * cfg.pivot_boost).min(1.0)
            } else {
                cfg.alpha_slow
            };
            self.fast = normalize(&lerp(&self.fast, &v, cfg.alpha_fast));
            self.slow = normalize(&lerp(&self.slow, &v, alpha_slow));
        }
        self.last_seen = now;
        self.dirty = true;
        self.current_pos()
    }

    /// `current_pos = norm(fast + slow)`. Falls back to whichever timescale is
    /// populated when only one is (shouldn't happen after the first fold).
    pub fn current_pos(&self) -> Vec<f32> {
        match (self.fast.is_empty(), self.slow.is_empty()) {
            (true, true) => Vec::new(),
            (true, false) => self.slow.clone(),
            (false, true) => self.fast.clone(),
            (false, false) => {
                let sum: Vec<f32> = self
                    .fast
                    .iter()
                    .zip(&self.slow)
                    .map(|(a, b)| a + b)
                    .collect();
                normalize(&sum)
            }
        }
    }

    /// Seed both timescales from a persisted position (resume continuity).
    fn seed(&mut self, pos: &[f32]) {
        let p = normalize(pos);
        self.fast = p.clone();
        self.slow = p;
    }
}

/// One node/chat's summary: substance-weighted vector sum (`centroid = norm(sum)`).
#[derive(Debug, Clone)]
pub struct OwnerAccum {
    pub sum: Vec<f32>,
    pub weight: f32,
    pub last_seen: Instant,
    pub dirty: bool,
}

impl OwnerAccum {
    fn new(now: Instant) -> Self {
        Self {
            sum: Vec::new(),
            weight: 0.0,
            last_seen: now,
            dirty: false,
        }
    }

    fn accumulate(&mut self, v: &[f32], w: f32, now: Instant) {
        let v = normalize(v);
        if self.sum.len() != v.len() {
            self.sum = vec![0.0; v.len()];
        }
        for (s, x) in self.sum.iter_mut().zip(&v) {
            *s += w * x;
        }
        self.weight += w;
        self.last_seen = now;
        self.dirty = true;
    }

    /// The raw weighted sum, persisted as-is so reload is lossless. Its
    /// direction is the centroid (`norm(sum)`); `vector_distance_cos` is
    /// scale-invariant, so storing the unnormalized sum is equivalent to the
    /// centroid for recall while preserving accumulated weight across eviction
    /// and restart.
    pub fn summary_vector(&self) -> Vec<f32> {
        self.sum.clone()
    }
}

/// Vectors flushed out of the engine on eviction or shutdown.
#[derive(Debug, Default, PartialEq)]
pub struct EvictionFlush {
    /// `(session_id, current_pos)` to persist to `sessions.current_pos`.
    pub sessions: Vec<(String, Vec<f32>)>,
    /// `(owner_uri, centroid)` to upsert into `resource_embeddings`.
    pub owners: Vec<(String, Vec<f32>)>,
}

/// In-memory position state across all live sessions and their owning nodes/chats.
pub struct PositionEngine {
    pub sessions: HashMap<String, SessionLive>,
    pub owners: HashMap<String, OwnerAccum>,
    pub cfg: PositionConfig,
}

impl PositionEngine {
    pub fn new(cfg: PositionConfig) -> Self {
        Self {
            sessions: HashMap::new(),
            owners: HashMap::new(),
            cfg,
        }
    }

    pub fn has_session(&self, session_id: &str) -> bool {
        self.sessions.contains_key(session_id)
    }

    /// Register a session ahead of its first fold, attaching its resolved owner
    /// URI and optionally seeding from a persisted position. A non-empty seed
    /// (resume) sets `fast = slow = norm(seed)`. The owner summary is NOT seeded
    /// here — it reloads from its own persisted vector via [`PositionEngine::seed_owner`],
    /// so idle gaps and restarts resume the running mean rather than resetting it.
    pub fn register_session(
        &mut self,
        session_id: &str,
        owner_uri: Option<String>,
        seed: Option<Vec<f32>>,
        now: Instant,
    ) {
        let mut live = SessionLive::new(owner_uri, now);
        if let Some(seed) = seed.filter(|s| !s.is_empty()) {
            live.seed(&seed);
        }
        self.sessions.insert(session_id.to_string(), live);
    }

    pub fn has_owner(&self, uri: &str) -> bool {
        self.owners.contains_key(uri)
    }

    /// Reload a node/chat summary accumulator from its persisted raw sum. Lets
    /// idle eviction and app restarts resume the substance-weighted running
    /// mean instead of resetting it to post-gap activity. The reloaded state is
    /// not marked dirty (it already equals what's persisted).
    pub fn seed_owner(&mut self, uri: &str, sum: Vec<f32>, now: Instant) {
        if sum.is_empty() {
            return;
        }
        let weight = sum.iter().map(|x| x * x).sum::<f32>().sqrt();
        self.owners.insert(
            uri.to_string(),
            OwnerAccum {
                sum,
                weight,
                last_seen: now,
                dirty: false,
            },
        );
    }

    /// Fold an event vector into a registered session's live position and its
    /// owner's summary centroid. Returns the new current_pos for persisting, or
    /// `None` if the session was never registered.
    pub fn fold(
        &mut self,
        session_id: &str,
        v: &[f32],
        weight: f32,
        now: Instant,
    ) -> Option<Vec<f32>> {
        let live = self.sessions.get_mut(session_id)?;
        let pos = live.fold(v, &self.cfg, now);
        if let Some(uri) = live.owner_uri.clone() {
            self.owners
                .entry(uri)
                .or_insert_with(|| OwnerAccum::new(now))
                .accumulate(v, weight, now);
        }
        Some(pos)
    }

    /// Drain dirty live positions for per-batch persistence, clearing the flag.
    pub fn take_dirty_sessions(&mut self) -> Vec<(String, Vec<f32>)> {
        let mut out = Vec::new();
        for (id, live) in self.sessions.iter_mut() {
            if live.dirty {
                out.push((id.clone(), live.current_pos()));
                live.dirty = false;
            }
        }
        out
    }

    /// Drain dirty summary centroids for the coarser upsert timer, clearing the flag.
    pub fn take_dirty_owners(&mut self) -> Vec<(String, Vec<f32>)> {
        let mut out = Vec::new();
        for (uri, accum) in self.owners.iter_mut() {
            if accum.dirty {
                out.push((uri.clone(), accum.summary_vector()));
                accum.dirty = false;
            }
        }
        out
    }

    /// Evict sessions and owners idle for at least `ttl`, returning their final
    /// vectors for one last persist.
    pub fn evict_idle(&mut self, now: Instant, ttl: Duration) -> EvictionFlush {
        let stale_sessions: Vec<String> = self
            .sessions
            .iter()
            .filter(|(_, l)| now.duration_since(l.last_seen) >= ttl)
            .map(|(id, _)| id.clone())
            .collect();
        let mut sessions = Vec::new();
        for id in stale_sessions {
            if let Some(live) = self.sessions.remove(&id) {
                sessions.push((id, live.current_pos()));
            }
        }

        let stale_owners: Vec<String> = self
            .owners
            .iter()
            .filter(|(_, a)| now.duration_since(a.last_seen) >= ttl)
            .map(|(uri, _)| uri.clone())
            .collect();
        let mut owners = Vec::new();
        for uri in stale_owners {
            if let Some(accum) = self.owners.remove(&uri) {
                owners.push((uri, accum.summary_vector()));
            }
        }

        EvictionFlush { sessions, owners }
    }

    /// Drain everything for a final flush (channel close / shutdown).
    pub fn drain_all(&mut self) -> EvictionFlush {
        let sessions = self
            .sessions
            .drain()
            .map(|(id, l)| (id, l.current_pos()))
            .collect();
        let owners = self
            .owners
            .drain()
            .map(|(uri, a)| (uri, a.summary_vector()))
            .collect();
        EvictionFlush { sessions, owners }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn approx(a: &[f32], b: &[f32]) -> bool {
        a.len() == b.len() && a.iter().zip(b).all(|(x, y)| (x - y).abs() < 1e-5)
    }

    #[test]
    fn substance_units_prefers_tokens_then_length_floored() {
        assert_eq!(substance_units(Some(120), ""), 120.0);
        // 8 chars / 4 = 2.0
        assert_eq!(substance_units(None, "abcdefgh"), 2.0);
        // Floor at 1.0 for tiny/empty events.
        assert_eq!(substance_units(None, ""), 1.0);
        assert_eq!(substance_units(Some(0), "abcd"), 1.0);
    }

    #[test]
    fn weight_for_scales_by_role_multiplier() {
        let cfg = PositionConfig::default();
        let user = cfg.weight_for(PositionKind::User, Some(100), "");
        let agent = cfg.weight_for(PositionKind::Agent, Some(100), "");
        // role_user (2.0) is exactly 2x role_agent (1.0).
        assert!((user - 2.0 * agent).abs() < 1e-5);
    }

    #[test]
    fn first_fold_seeds_both_timescales() {
        let mut engine = PositionEngine::new(PositionConfig::default());
        let now = Instant::now();
        engine.register_session("s1", None, None, now);
        let pos = engine.fold("s1", &[3.0, 4.0], 1.0, now).unwrap();
        // current_pos = norm(fast+slow) where fast=slow=norm([3,4])=[0.6,0.8].
        assert!(approx(&pos, &[0.6, 0.8]));
    }

    #[test]
    fn fold_on_unregistered_session_is_none() {
        let mut engine = PositionEngine::new(PositionConfig::default());
        assert!(engine
            .fold("ghost", &[1.0, 0.0], 1.0, Instant::now())
            .is_none());
    }

    #[test]
    fn pivot_reanchors_slow_on_low_similarity() {
        let cfg = PositionConfig::default();
        let now = Instant::now();

        // With a near-orthogonal second event, the slow anchor should be boosted.
        let mut boosted = PositionEngine::new(cfg.clone());
        boosted.register_session("s", None, None, now);
        boosted.fold("s", &[1.0, 0.0], 1.0, now);
        boosted.fold("s", &[0.0, 1.0], 1.0, now);
        let slow_boosted = boosted.sessions["s"].slow.clone();
        let sim_boosted = cosine_similarity(&slow_boosted, &[0.0, 1.0]);

        // A near-identical second event stays below the pivot threshold and
        // moves the slow anchor only at the base rate.
        let mut calm = PositionEngine::new(cfg);
        calm.register_session("s", None, None, now);
        calm.fold("s", &[1.0, 0.0], 1.0, now);
        calm.fold("s", &[0.99, 0.01], 1.0, now);
        let slow_calm = calm.sessions["s"].slow.clone();
        let sim_calm = cosine_similarity(&slow_calm, &[0.0, 1.0]);

        // The pivot event pulls the slow anchor far more toward the new
        // direction than the calm event does.
        assert!(sim_boosted > 0.4, "boosted slow sim {sim_boosted}");
        assert!(sim_calm < 0.2, "calm slow sim {sim_calm}");
    }

    #[test]
    fn owner_centroid_matches_weighted_mean() {
        let mut engine = PositionEngine::new(PositionConfig::default());
        let now = Instant::now();
        engine.register_session(
            "s",
            Some("cairn://p/PROJ/1/1/builder".to_string()),
            None,
            now,
        );
        // sum = 1*[1,0] + 3*[0,1] = [1,3]; centroid = norm([1,3]).
        engine.fold("s", &[1.0, 0.0], 1.0, now);
        engine.fold("s", &[0.0, 1.0], 3.0, now);
        let centroid = normalize(&engine.owners["cairn://p/PROJ/1/1/builder"].sum);
        let expected = normalize(&[1.0, 3.0]);
        assert!(approx(&centroid, &expected), "got {centroid:?}");
    }

    #[test]
    fn no_owner_uri_skips_summary_but_tracks_live() {
        let mut engine = PositionEngine::new(PositionConfig::default());
        let now = Instant::now();
        engine.register_session("s", None, None, now);
        assert!(engine.fold("s", &[1.0, 0.0], 1.0, now).is_some());
        assert!(engine.owners.is_empty());
    }

    #[test]
    fn register_seed_sets_live_position_only() {
        let now = Instant::now();
        let mut engine = PositionEngine::new(PositionConfig::default());
        engine.register_session(
            "s",
            Some("cairn://p/PROJ/1/1/builder".to_string()),
            Some(vec![0.0, 5.0]),
            now,
        );
        // Live position seeded to norm([0,5]) = [0,1].
        assert!(approx(&engine.sessions["s"].current_pos(), &[0.0, 1.0]));
        // The owner summary is NOT seeded from current_pos — it reloads from its
        // own persisted vector via seed_owner.
        assert!(engine.owners.is_empty());
    }

    #[test]
    fn has_owner_reflects_seed() {
        let mut engine = PositionEngine::new(PositionConfig::default());
        assert!(!engine.has_owner("o"));
        engine.seed_owner("o", vec![1.0, 0.0], Instant::now());
        assert!(engine.has_owner("o"));
    }

    #[test]
    fn summary_survives_evict_reload_losslessly() {
        // Accumulated history is preserved across evict → reload because the raw
        // weighted sum (not the unit centroid) is what's persisted. This is the
        // fix for idle gaps resetting a node's summary to post-gap activity.
        let now = Instant::now();
        let mut engine = PositionEngine::new(PositionConfig::default());
        engine.register_session("s", Some("o".to_string()), None, now);
        engine.fold("s", &[1.0, 0.0], 10.0, now); // heavy history toward [1,0]
        let flush = engine.evict_idle(now, Duration::from_secs(0));
        let (_, persisted) = &flush.owners[0];
        assert!(
            approx(persisted, &[10.0, 0.0]),
            "persisted raw sum {persisted:?}"
        );

        // A fresh engine reloads the raw sum; a light event nudges it only slightly.
        let mut reloaded = PositionEngine::new(PositionConfig::default());
        reloaded.register_session("s2", Some("o".to_string()), None, now);
        reloaded.seed_owner("o", persisted.clone(), now);
        reloaded.fold("s2", &[0.0, 1.0], 1.0, now);
        // norm([10,1]) — still dominated by reloaded history, not reset.
        let c = normalize(&reloaded.owners["o"].sum);
        assert!(c[0] > 0.95, "reloaded history should dominate, got {c:?}");
    }

    #[test]
    fn seed_owner_is_not_dirty() {
        let mut engine = PositionEngine::new(PositionConfig::default());
        engine.seed_owner("o", vec![1.0, 0.0], Instant::now());
        // Reloaded state equals what's persisted — no redundant rewrite.
        assert!(engine.take_dirty_owners().is_empty());
    }

    #[test]
    fn dirty_draining_clears_flags() {
        let mut engine = PositionEngine::new(PositionConfig::default());
        let now = Instant::now();
        engine.register_session("s", Some("owner".to_string()), None, now);
        engine.fold("s", &[1.0, 0.0], 1.0, now);
        assert_eq!(engine.take_dirty_sessions().len(), 1);
        assert_eq!(engine.take_dirty_owners().len(), 1);
        // Second drain with no new folds is empty.
        assert!(engine.take_dirty_sessions().is_empty());
        assert!(engine.take_dirty_owners().is_empty());
    }

    #[test]
    fn evict_idle_flushes_and_removes() {
        let mut engine = PositionEngine::new(PositionConfig::default());
        let now = Instant::now();
        engine.register_session("s", Some("owner".to_string()), None, now);
        engine.fold("s", &[1.0, 0.0], 1.0, now);
        // ttl 0 → everything is at least 0 old → evicted.
        let flush = engine.evict_idle(now, Duration::from_secs(0));
        assert_eq!(flush.sessions.len(), 1);
        assert_eq!(flush.owners.len(), 1);
        assert!(engine.sessions.is_empty());
        assert!(engine.owners.is_empty());
    }

    #[test]
    fn evict_idle_keeps_fresh_entries() {
        let mut engine = PositionEngine::new(PositionConfig::default());
        let now = Instant::now();
        engine.register_session("s", Some("owner".to_string()), None, now);
        engine.fold("s", &[1.0, 0.0], 1.0, now);
        let flush = engine.evict_idle(now, Duration::from_secs(3600));
        assert!(flush.sessions.is_empty());
        assert!(flush.owners.is_empty());
        assert!(engine.has_session("s"));
    }

    #[test]
    fn drain_all_empties_engine() {
        let mut engine = PositionEngine::new(PositionConfig::default());
        let now = Instant::now();
        engine.register_session("a", Some("o1".to_string()), None, now);
        engine.register_session("b", Some("o2".to_string()), None, now);
        engine.fold("a", &[1.0, 0.0], 1.0, now);
        engine.fold("b", &[0.0, 1.0], 1.0, now);
        let flush = engine.drain_all();
        assert_eq!(flush.sessions.len(), 2);
        assert_eq!(flush.owners.len(), 2);
        assert!(engine.sessions.is_empty());
    }
}
