//! Learning vibe axes for whichever embedding model is active, from this
//! workspace's own history.
//!
//! Axes are directions in one model's space ([`super::vibes`]). Cohere v4's
//! ship bundled; any other model's are learned here the first time it is
//! active, and stored in `vibe_axes` keyed by `(model, dims)`, so switching
//! back to a model finds its axes and learns nothing again. Until a set
//! exists, new events in that space get no colour; colours already stored
//! (`event_vibes`) are never touched, because the axes are defined by
//! behaviour, so a colour from one model means about what it does in another.
//!
//! ## One recompute
//!
//! Per-event vectors are discarded once coloured, so learning axes means
//! embedding a sample of history again:
//!
//! 1. **Sample.** Visit runs in random order, label each assistant event that
//!    carries prose by where it sits in its run ([`super::vibe_axes`]), and
//!    keep a uniform reservoir per class plus one regardless of class for the
//!    percentile population. Uniform over the visited runs' events, the
//!    reservoirs do not over-weight short runs; visiting at least
//!    [`Budget::min_runs`] runs keeps one long run from being a whole class.
//!    The drawn sample is written to `vibe_axis_samples` in one transaction.
//! 2. **Embed.** Chunk by chunk through the shared [`super::Embed`] door,
//!    behind its health gate, as documents in the resource corpus (the space
//!    the embed worker colours events in), each chunk's vectors persisted
//!    before the next is sent. A restart resumes at the first row without a
//!    vector; a failure costs one chunk. Chunks are paced ([`Budget::pause`])
//!    so the re-embed sweeps running beside a switch keep most of the
//!    provider.
//! 3. **Compute** ([`super::vibe_axes::compute_axes`]), store the set, delete
//!    the sample, and hand the set to the live [`VibeState`].
//!
//! ## How big a sample
//!
//! The shipped axes separate their classes with cross-validated AUC 0.905
//! (PHASE) and 0.875 (FRICTION): `d' = sqrt(2) * probit(AUC)` of about 1.85
//! and 1.63 standard deviations along the axis, where corpus projections
//! spread about 0.10, so the class means sit about `|delta| = 0.17` apart. A
//! mean-difference axis estimated from `n` unit vectors per class is off by
//! noise of squared norm about `2 * tr(S) / n`, where `tr(S)`, the
//! within-class variance, is about `1 - |mean|^2`, near 0.65 for an embedding
//! cone. Its cosine with the true axis is then about
//! `|delta| / sqrt(|delta|^2 + 1.3 / n)`, and the AUC it keeps about
//! `probit^-1(d' * cos / sqrt(2))`:
//!
//! | n per class | cosine | PHASE AUC | FRICTION AUC |
//! |---|---|---|---|
//! | 50 | 0.73 | 0.83 | 0.80 |
//! | 100 | 0.83 | 0.86 | 0.83 |
//! | 400 | 0.95 | 0.89 | 0.86 |
//! | 800 | 0.97 | 0.90 | 0.87 |
//!
//! So [`Budget::DEFAULT`] aims for 400 per class, within about 0.01 AUC of
//! the shipped axes, and refuses below 100, where an axis has lost about a
//! third of its separation and falls away quickly.
//!
//! Measured on 2026-10-28, that estimate is optimistic. A recompute on Cohere v4
//! over a copy of a mature workspace drew 199/172 events for PHASE and 100/672
//! for FRICTION. Its axes had cosine 0.66 and 0.58 with the shipped ones,
//! where the table predicts about 0.85. Either the noise is several times what
//! the estimate assumes, or part of the gap is drift between the research
//! corpus the bundle came from and today's history; a cosine with the shipped
//! axes cannot tell the two apart. Read as noise alone, 400 per class gives
//! about 0.75. The projection spreads did match the shipped tables closely.
//! `compute_vibe_axes --split-half` tells the two causes apart.
//!
//! The percentile population is 1,000 events, which puts a rank within about 1.6 points at the median
//! (`sqrt(0.25 / 1000)`). Classes overlap, so one recompute embeds at most
//! about 2,600 texts; free on a local server, cents on a remote one.
//!
//! ## Not enough history
//!
//! A workspace whose history cannot fill every class to the minimum stores
//! nothing; its colours stay neutral, the progress says so with the counts
//! found, and the sample is drawn again after [`NOT_ENOUGH_RETRY_SECS`] as
//! history grows.
//!
//! ## Consent
//!
//! History is sent to the provider, so the sample is scoped like the turn
//! sweep's: every run with consent, and without it only runs with a turn at
//! or after this install's boundary (see [`super::turns::Scope`]).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cairn_db::turso::{params, Value};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};

use crate::storage::events::history::{EventFilter, HistoryReader, HotRow};

use super::embedder::{Corpus, Embed, EmbedFailure, Purpose};
use super::space::{Reconciled, SpaceKeeper, Trigger};
use super::turns::{gate_wait, install_boundary, REJECTED_PAUSE_SECS};
use super::vibe_axes::{
    compute_axes, run_positions, AxesError, ClassCounts, RunPosition, Sample, Side,
};
use super::vibes::{AxisSet, VibeState};
use super::{extract_embeddable_text, vector};
use crate::storage::{DbResult, LocalDb, RowExt};

/// The log target a recompute reports on.
pub const VIBES_TARGET: &str = "embeddings::vibes";
/// The wait between checks once the active space has axes, or has no offer.
const IDLE_SECS: u64 = 60;
/// The wait after a storage failure.
const RETRY_SECS: u64 = 300;
/// How long a "not enough history" verdict stands before sampling again.
const NOT_ENOUGH_RETRY_SECS: u64 = 3600;

/// How much one recompute samples, and the least it trusts. See the module
/// docs for why [`Budget::DEFAULT`] is what it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Budget {
    /// Events kept per class.
    pub per_class: usize,
    /// Events kept regardless of class, for the percentile tables.
    pub distribution: usize,
    /// Fewer embedded events than this in any class, or in the percentile
    /// population, and no axes are stored.
    pub min_per_class: u64,
    /// Runs visited before the reservoirs may be called full.
    pub min_runs: usize,
    /// Runs visited at most.
    pub max_runs: usize,
    /// Texts per embedding call.
    pub chunk: usize,
    /// The wait after each embedded chunk.
    pub pause: Duration,
}

impl Budget {
    pub const DEFAULT: Self = Self {
        per_class: 400,
        distribution: 1000,
        min_per_class: 100,
        min_runs: 150,
        max_runs: 1500,
        chunk: 32,
        pause: Duration::from_secs(2),
    };
}

/// Where learning the active space's axes stands.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum VibeAxesProgress {
    /// The active space has axes; new events are coloured.
    Ready,
    /// Drawing the sample from history.
    Sampling,
    /// Embedding the sample: `embedded` of `total` events hold a vector.
    Embedding { embedded: u64, total: u64 },
    /// History is too thin to trust: `counts` is what was found, each class
    /// needs `needed`. Sampled again at `retry_at` (epoch milliseconds).
    #[serde(rename_all = "camelCase")]
    NotEnoughHistory {
        counts: ClassCounts,
        needed: u64,
        retry_at: i64,
    },
}

/// Run the recompute forever on the current tokio runtime, beside the
/// re-embed sweeps. `consented` is `Orchestrator::history_consented`.
pub fn spawn_vibe_axes_recompute(
    keeper: Arc<SpaceKeeper>,
    vibes: Arc<VibeState>,
    consented: Arc<dyn Fn() -> bool + Send + Sync>,
) {
    tokio::spawn(async move {
        let mut rng = StdRng::from_entropy();
        loop {
            let wait = step(&keeper, &vibes, consented(), Budget::DEFAULT, &mut rng).await;
            tokio::time::sleep(Duration::from_secs(wait)).await;
        }
    });
}

/// Step until the active space's axes are learned or refused: the offline
/// regenerator (`examples/compute_vibe_axes.rs`) runs exactly the app's
/// recompute this way, over a copy of a workspace database. It stops at the
/// first provider failure rather than waiting out the gate, because an
/// offline run must not keep spending a quota the operator's own install
/// needs. A stopped run's sample, with whatever was embedded, stays in the
/// copy and a rerun resumes from it; once a set is stored, `store_axes`
/// deletes the sample in the same transaction, by design, so a finished run
/// leaves no per-event vectors behind.
pub async fn learn_until_settled(
    keeper: &SpaceKeeper,
    vibes: &VibeState,
    consented: bool,
    budget: Budget,
    seed: u64,
) -> Option<VibeAxesProgress> {
    let mut rng = StdRng::seed_from_u64(seed);
    loop {
        let wait = step(keeper, vibes, consented, budget, &mut rng).await;
        match keeper.vibe_progress() {
            settled @ Some(VibeAxesProgress::Ready | VibeAxesProgress::NotEnoughHistory { .. }) => {
                return settled;
            }
            progress
                if keeper.embedder().space().is_none()
                    || keeper.embedder().health().retry_at.is_some() =>
            {
                return progress;
            }
            _ => tokio::time::sleep(Duration::from_secs(wait.min(IDLE_SECS))).await,
        }
    }
}

/// Draw and embed a sample exactly as the recompute does, then return it
/// rather than computing and storing a set: the regenerator's `--split-half`
/// measurement, which must store nothing. The sample stays in
/// `vibe_axis_samples`, so a later recompute over the same copy reuses its
/// vectors. Stops at the first provider failure, like
/// [`learn_until_settled`].
pub async fn embed_sample(
    keeper: &SpaceKeeper,
    consented: bool,
    budget: Budget,
    seed: u64,
) -> Result<Vec<Sample>, String> {
    let mut rng = StdRng::seed_from_u64(seed);
    keeper
        .reconcile(Trigger::Sweep)
        .await
        .map_err(|error| error.to_string())?;
    let embedder = keeper.embedder().clone();
    let space = embedder.space().ok_or("no embedding offer resolves")?;
    let since = if consented {
        None
    } else {
        Some(
            install_boundary(keeper.db())
                .await
                .ok_or("the install boundary is not established")?,
        )
    };
    let target = Target {
        embedder: embedder.as_ref(),
        generation: keeper.generation(),
        model: &space.model,
    };
    let outcome = recompute(keeper, target, since, budget, &mut rng, Finish::Return)
        .await
        .map_err(|error| error.to_string())?;
    match outcome {
        Outcome::Embedded(samples) => Ok(samples),
        Outcome::NotEnough(counts) => Err(AxesError::TooFew {
            counts,
            needed: budget.min_per_class,
        }
        .to_string()),
        Outcome::Wait(_) => Err(format!(
            "stopped at the first provider failure, without retrying: {}",
            embedder.health().last_error.unwrap_or_default()
        )),
        Outcome::Superseded | Outcome::Stored(_) => {
            Err("the embedding space changed under the sample".to_string())
        }
    }
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_millis() as i64)
}

/// One look at the active space: learn its axes if it has none. Returns how
/// long to wait before the next look, in seconds.
pub(crate) async fn step(
    keeper: &SpaceKeeper,
    vibes: &VibeState,
    consented: bool,
    budget: Budget,
    rng: &mut StdRng,
) -> u64 {
    match keeper.reconcile(Trigger::Sweep).await {
        Ok(Reconciled::Unresolved) => return IDLE_SECS,
        Ok(_) => {}
        Err(error) => {
            log::warn!(target: VIBES_TARGET, "vibe axes: reconciling the embedding space failed: {error}");
            return RETRY_SECS;
        }
    }
    let generation = keeper.generation();
    let embedder = keeper.embedder().clone();
    let Some(space) = embedder.space() else {
        return IDLE_SECS;
    };
    if vibes.covers(&space.model, space.resource_dims) {
        keeper.set_vibe_progress(generation, VibeAxesProgress::Ready);
        return IDLE_SECS;
    }
    if let Some(VibeAxesProgress::NotEnoughHistory { retry_at, .. }) = keeper.vibe_progress() {
        let left = retry_at - now_millis();
        if left > 0 {
            return (left as u64 / 1000).max(1);
        }
    }
    let health = embedder.health();
    if !health.configured {
        return IDLE_SECS;
    }
    if !embedder.ready() {
        return gate_wait(embedder.as_ref());
    }
    let since = if consented {
        None
    } else {
        match install_boundary(keeper.db()).await {
            Some(boundary) => Some(boundary),
            None => return RETRY_SECS,
        }
    };

    let target = Target {
        embedder: embedder.as_ref(),
        generation,
        model: &space.model,
    };
    match recompute(keeper, target, since, budget, rng, Finish::Store).await {
        Ok(Outcome::Stored(set)) => {
            log::info!(
                target: VIBES_TARGET,
                "vibe axes: learned for {} at {} from this workspace's history",
                set.model,
                set.dims
            );
            vibes.insert(set);
            keeper.set_vibe_progress(generation, VibeAxesProgress::Ready);
            IDLE_SECS
        }
        Ok(Outcome::NotEnough(counts)) => {
            log::info!(
                target: VIBES_TARGET,
                "vibe axes: {} for {}; colours stay neutral and history is sampled again in {}s",
                AxesError::TooFew { counts, needed: budget.min_per_class },
                space.model,
                NOT_ENOUGH_RETRY_SECS
            );
            keeper.set_vibe_progress(
                generation,
                VibeAxesProgress::NotEnoughHistory {
                    counts,
                    needed: budget.min_per_class,
                    retry_at: now_millis() + (NOT_ENOUGH_RETRY_SECS * 1000) as i64,
                },
            );
            NOT_ENOUGH_RETRY_SECS
        }
        Ok(Outcome::Wait(seconds)) => seconds,
        // `Finish::Store` never hands the sample back.
        Ok(Outcome::Superseded | Outcome::Embedded(_)) => 1,
        Err(error) => {
            log::warn!(target: VIBES_TARGET, "vibe axes: storage failed: {error}");
            RETRY_SECS
        }
    }
}

/// What a recompute does once its sample is embedded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Finish {
    /// Compute the set, store it, and drop the sample: the app.
    Store,
    /// Hand the embedded sample back and store nothing: a measurement.
    Return,
}

enum Outcome {
    Stored(AxisSet),
    /// The embedded sample, under [`Finish::Return`].
    Embedded(Vec<Sample>),
    NotEnough(ClassCounts),
    /// The provider is paused, or refused; come back after this many seconds.
    Wait(u64),
    /// The space changed under the recompute; look again.
    Superseded,
}

/// One sampled event as `vibe_axis_samples` holds it.
#[derive(Debug, Clone, PartialEq)]
struct SampleRow {
    event_id: String,
    position: RunPosition,
    in_distribution: bool,
    vector: Option<Vec<f32>>,
    dims: Option<u32>,
}

/// The space a recompute learns for, as the keeper held it when the recompute
/// began; once the keeper's generation moves on, the recompute is superseded.
struct Target<'a> {
    embedder: &'a dyn Embed,
    generation: u64,
    model: &'a str,
}

async fn recompute(
    keeper: &SpaceKeeper,
    target: Target<'_>,
    since: Option<i64>,
    budget: Budget,
    rng: &mut StdRng,
    finish: Finish,
) -> DbResult<Outcome> {
    let Target {
        embedder,
        generation,
        model,
    } = target;
    let db = keeper.db().as_ref();
    let mut rows = load_samples(db, model).await?;
    if rows.is_empty() {
        keeper.set_vibe_progress(generation, VibeAxesProgress::Sampling);
        log::info!(target: VIBES_TARGET, "vibe axes: sampling history to learn axes for {model}");
        rows = draw_sample(db, since, budget, rng).await?;
        let counts = ClassCounts::of(rows.iter().map(|row| (&row.position, row.in_distribution)));
        if !counts.enough(budget.min_per_class) {
            return Ok(Outcome::NotEnough(counts));
        }
        if keeper.generation() != generation {
            return Ok(Outcome::Superseded);
        }
        insert_samples(db, model, &rows).await?;
    }

    loop {
        let total = rows.len() as u64;
        let embedded = rows.iter().filter(|row| row.vector.is_some()).count() as u64;
        keeper.set_vibe_progress(generation, VibeAxesProgress::Embedding { embedded, total });
        let pending: Vec<String> = rows
            .iter()
            .filter(|row| row.vector.is_none())
            .take(budget.chunk)
            .map(|row| row.event_id.clone())
            .collect();
        if pending.is_empty() {
            break;
        }

        let texts = load_texts(db, &pending).await?;
        let (ids, texts): (Vec<String>, Vec<String>) = pending
            .iter()
            .filter_map(|id| texts.get(id).map(|text| (id.clone(), text.clone())))
            .unzip();
        // An event gone, or archived and carrying no prose once restored,
        // leaves the sample.
        let unreadable: Vec<String> = pending.into_iter().filter(|id| !ids.contains(id)).collect();
        if !unreadable.is_empty() {
            delete_samples(db, model, &unreadable).await?;
            rows.retain(|row| !unreadable.contains(&row.event_id));
        }
        if ids.is_empty() {
            continue;
        }

        match embedder
            .embed(Corpus::Resources, Purpose::Document, texts)
            .await
        {
            Ok(reply) => {
                if keeper.generation() != generation || reply.model != model {
                    return Ok(Outcome::Superseded);
                }
                // A width that moved mid-sample is a new space: start over.
                if rows
                    .iter()
                    .any(|row| row.dims.is_some_and(|dims| dims != reply.dims))
                {
                    clear_samples(db, model).await?;
                    return Ok(Outcome::Superseded);
                }
                store_vectors(db, model, &ids, &reply.vectors, reply.dims).await?;
                let vectors: HashMap<&String, &Vec<f32>> = ids.iter().zip(&reply.vectors).collect();
                for row in rows.iter_mut() {
                    if let Some(vector) = vectors.get(&row.event_id) {
                        row.vector = Some((*vector).clone());
                        row.dims = Some(reply.dims);
                    }
                }
                tokio::time::sleep(budget.pause).await;
            }
            Err(EmbedFailure::Unconfigured) => return Ok(Outcome::Wait(IDLE_SECS)),
            Err(EmbedFailure::Paused { .. } | EmbedFailure::Failed(_)) => {
                return Ok(Outcome::Wait(gate_wait(embedder)));
            }
            // Refused as written: this offer will not embed these events, and
            // asking again would be refused again, so they leave the sample.
            // Should the offer refuse everything, the sample drains to below
            // the minimum and reads as not enough history.
            Err(EmbedFailure::Rejected(reason)) => {
                log::warn!(
                    target: VIBES_TARGET,
                    "vibe axes: {} sampled events were refused and leave the sample: {reason}",
                    ids.len()
                );
                delete_samples(db, model, &ids).await?;
                rows.retain(|row| !ids.contains(&row.event_id));
                return Ok(Outcome::Wait(REJECTED_PAUSE_SECS));
            }
        }
    }

    let samples: Vec<Sample> = rows
        .into_iter()
        .filter_map(|row| {
            Some(Sample {
                vector: row.vector?,
                position: row.position,
                in_distribution: row.in_distribution,
            })
        })
        .collect();
    if finish == Finish::Return {
        return Ok(Outcome::Embedded(samples));
    }
    match compute_axes(model, &samples, budget.min_per_class) {
        Ok(set) => {
            let counts = ClassCounts::of(samples.iter().map(|s| (&s.position, s.in_distribution)));
            store_axes(db, &set, &counts).await?;
            Ok(Outcome::Stored(set))
        }
        Err(AxesError::TooFew { counts, .. }) => {
            clear_samples(db, model).await?;
            Ok(Outcome::NotEnough(counts))
        }
        Err(AxesError::MixedWidths) => {
            clear_samples(db, model).await?;
            Ok(Outcome::Superseded)
        }
    }
}

// ===== sampling =====

/// A uniform sample of at most `cap` of everything offered (Algorithm R).
struct Reservoir<T> {
    cap: usize,
    seen: usize,
    kept: Vec<T>,
}

impl<T> Reservoir<T> {
    fn new(cap: usize) -> Self {
        Self {
            cap,
            seen: 0,
            kept: Vec::new(),
        }
    }

    fn offer(&mut self, item: T, rng: &mut StdRng) {
        self.seen += 1;
        if self.kept.len() < self.cap {
            self.kept.push(item);
        } else {
            let slot = rng.gen_range(0..self.seen);
            if slot < self.cap {
                self.kept[slot] = item;
            }
        }
    }

    fn full(&self) -> bool {
        self.seen >= self.cap
    }
}

/// One event of a run, as labelling needs it.
struct Step {
    id: String,
    is_error: bool,
    /// An assistant event that carries (or, archived, may carry) prose.
    candidate: bool,
}

/// Draw a labelled sample from the runs in scope.
async fn draw_sample(
    db: &LocalDb,
    since: Option<i64>,
    budget: Budget,
    rng: &mut StdRng,
) -> DbResult<Vec<SampleRow>> {
    let mut runs: Vec<String> = match since {
        None => {
            db.query_all("SELECT id FROM runs", (), |row| row.text(0))
                .await?
        }
        Some(since) => {
            db.query_all(
                "SELECT DISTINCT run_id FROM turns WHERE created_at >= ?1 AND run_id IS NOT NULL",
                params![since],
                |row| row.text(0),
            )
            .await?
        }
    };
    runs.shuffle(rng);

    // Reservoirs per class: phase +/-, friction +/-.
    let mut classes: [Reservoir<(String, RunPosition)>; 4] =
        std::array::from_fn(|_| Reservoir::new(budget.per_class));
    let mut distribution = Reservoir::new(budget.distribution);
    for (visited, run_id) in runs.iter().take(budget.max_runs).enumerate() {
        let steps = run_steps(db, run_id).await?;
        let errors: Vec<bool> = steps.iter().map(|step| step.is_error).collect();
        for (step, position) in steps.into_iter().zip(run_positions(&errors)) {
            if !step.candidate {
                continue;
            }
            let class = |side: Option<Side>, positive: usize| match side {
                Some(Side::Positive) => Some(positive),
                Some(Side::Negative) => Some(positive + 1),
                None => None,
            };
            for slot in [class(position.phase(), 0), class(position.friction(), 2)]
                .into_iter()
                .flatten()
            {
                classes[slot].offer((step.id.clone(), position), rng);
            }
            distribution.offer((step.id, position), rng);
        }
        if visited + 1 >= budget.min_runs
            && distribution.full()
            && classes.iter().all(Reservoir::full)
        {
            break;
        }
        tokio::task::yield_now().await;
    }

    // An event drawn by several reservoirs is embedded once.
    let mut chosen: HashMap<String, (RunPosition, bool)> = HashMap::new();
    for (event_id, position) in classes.into_iter().flat_map(|reservoir| reservoir.kept) {
        chosen.entry(event_id).or_insert((position, false));
    }
    for (event_id, position) in distribution.kept {
        chosen.insert(event_id, (position, true));
    }
    let mut rows: Vec<SampleRow> = chosen
        .into_iter()
        .map(|(event_id, (position, in_distribution))| SampleRow {
            event_id,
            position,
            in_distribution,
            vector: None,
            dims: None,
        })
        .collect();
    rows.sort_by(|a, b| a.event_id.cmp(&b.event_id));
    Ok(rows)
}

/// Every event of `run_id` in order, reading only what labelling needs: a
/// stored event's error flag and whether it has prose come from SQL, so a
/// large tool result is never loaded; an archived tool result or assistant
/// event is restored to read them. Most of a mature workspace's history is
/// archived, so treating an archived assistant event as prose unread would
/// draw a sample that is mostly tool calls with nothing to embed.
async fn run_steps(db: &LocalDb, run_id: &str) -> DbResult<Vec<Step>> {
    let rows = db
        .query_all(
            "SELECT id, event_type,
                    COALESCE(storage_mode, 'full') = 'full',
                    CASE WHEN event_type = 'tool_result' AND COALESCE(storage_mode, 'full') = 'full'
                              AND json_valid(data)
                         THEN COALESCE(json_extract(data, '$.isError') = 1, 0) ELSE 0 END,
                    CASE WHEN event_type = 'assistant' AND COALESCE(storage_mode, 'full') = 'full'
                              AND json_valid(data)
                         THEN COALESCE(length(json_extract(data, '$.content')), 0)
                            + COALESCE(length(json_extract(data, '$.thinking')), 0)
                         ELSE 0 END
               FROM events WHERE run_id = ?1 ORDER BY sequence",
            params![run_id],
            |row| {
                Ok((
                    row.text(0)?,
                    row.text(1)?,
                    row.i64(2)? != 0,
                    row.i64(3)? != 0,
                    row.i64(4)? > 0,
                ))
            },
        )
        .await?;
    let archived: Vec<String> = rows
        .iter()
        .filter(|(_, kind, stored, _, _)| !stored && (kind == "tool_result" || kind == "assistant"))
        .map(|(id, ..)| id.clone())
        .collect();
    let mut restored_errors = std::collections::HashSet::new();
    let mut restored_prose = std::collections::HashSet::new();
    for event in load_events(db, &mut HistoryReader::new(db), &archived).await? {
        let is_error = serde_json::from_str::<serde_json::Value>(&event.data)
            .ok()
            .and_then(|data| data.get("isError").and_then(|flag| flag.as_bool()))
            .unwrap_or(false);
        let has_prose =
            extract_embeddable_text(&event.data).is_some_and(|text| !text.trim().is_empty());
        if event.event_type == "tool_result" && is_error {
            restored_errors.insert(event.id);
        } else if event.event_type == "assistant" && has_prose {
            restored_prose.insert(event.id);
        }
    }
    Ok(rows
        .into_iter()
        .map(|(id, kind, _, is_error, has_prose)| Step {
            is_error: is_error || restored_errors.contains(&id),
            candidate: kind == "assistant" && (has_prose || restored_prose.contains(&id)),
            id,
        })
        .collect())
}

/// The sample's id locator and hot bodies are one indexed statement per page.
/// Source selection is primed once across all selected runs, after release.
fn vibe_events_sql(count: usize) -> String {
    let placeholders = (1..=count)
        .map(|index| format!("?{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "SELECT {} FROM events WHERE id IN ({placeholders})",
        HotRow::columns(None)
    )
}

async fn load_events(
    db: &LocalDb,
    reader: &mut HistoryReader<'_>,
    ids: &[String],
) -> DbResult<Vec<crate::models::Event>> {
    let mut selected = Vec::new();
    for page in ids.chunks(256) {
        let values: Vec<Value> = page.iter().map(|id| Value::Text(id.clone())).collect();
        selected.extend(
            db.query_all(vibe_events_sql(page.len()), values, |row| {
                HotRow::from_row_at(row, 0)
            })
            .await?,
        );
    }
    let rows = reader
        .hydrate_selected_hot_rows(selected, EventFilter::default())
        .await?;
    Ok(rows
        .into_iter()
        .filter(|row| row.reconstruction != crate::storage::Reconstruction::Undecodable)
        .map(|row| row.event)
        .collect())
}

/// The embeddable text of each of `ids` that has any.
///
/// A sample names its events by id alone, so each event's run is looked up from
/// its hot row. An event with no hot row leaves the sample, whether it was
/// deleted or evicted, which is what the sample already does with an event it
/// cannot read.
async fn load_texts(db: &LocalDb, ids: &[String]) -> DbResult<HashMap<String, String>> {
    let mut texts = HashMap::new();
    let mut history = HistoryReader::new(db);
    for event in load_events(db, &mut history, ids).await? {
        if let Some(text) = extract_embeddable_text(&event.data) {
            let text = text.trim().to_string();
            if !text.is_empty() {
                texts.insert(event.id, text);
            }
        }
    }
    Ok(texts)
}

// ===== storage =====

async fn load_samples(db: &LocalDb, model: &str) -> DbResult<Vec<SampleRow>> {
    db.query_all(
        "SELECT event_id, run_progress, dist_prev_error, in_distribution, embedding, dims
           FROM vibe_axis_samples WHERE model = ?1 ORDER BY event_id",
        params![model],
        |row| {
            Ok(SampleRow {
                event_id: row.text(0)?,
                position: RunPosition {
                    run_progress: row.f64(1)?,
                    dist_prev_error: row.opt_i64(2)?,
                    dist_next_error: None,
                },
                in_distribution: row.i64(3)? != 0,
                vector: row.opt_blob(4)?.map(|bytes| vector::from_bytes(&bytes)),
                dims: row.opt_i64(5)?.map(|dims| dims as u32),
            })
        },
    )
    .await
}

async fn insert_samples(db: &LocalDb, model: &str, rows: &[SampleRow]) -> DbResult<()> {
    let model = model.to_string();
    let rows = rows.to_vec();
    db.write(move |conn| {
        let (model, rows) = (model.clone(), rows.clone());
        Box::pin(async move {
            for row in rows {
                conn.execute(
                    "INSERT OR REPLACE INTO vibe_axis_samples(model, event_id, run_progress,
                                                              dist_prev_error, in_distribution)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        model.as_str(),
                        row.event_id.as_str(),
                        row.position.run_progress,
                        row.position.dist_prev_error,
                        row.in_distribution as i64
                    ],
                )
                .await?;
            }
            Ok(())
        })
    })
    .await
}

async fn store_vectors(
    db: &LocalDb,
    model: &str,
    ids: &[String],
    vectors: &[Vec<f32>],
    dims: u32,
) -> DbResult<()> {
    let model = model.to_string();
    let rows: Vec<(String, Vec<u8>)> = ids
        .iter()
        .zip(vectors)
        .map(|(id, v)| (id.clone(), vector::to_bytes(v)))
        .collect();
    db.write(move |conn| {
        let (model, rows) = (model.clone(), rows.clone());
        Box::pin(async move {
            for (id, bytes) in rows {
                conn.execute(
                    "UPDATE vibe_axis_samples SET embedding = ?1, dims = ?2
                      WHERE model = ?3 AND event_id = ?4",
                    params![bytes, dims as i64, model.as_str(), id.as_str()],
                )
                .await?;
            }
            Ok(())
        })
    })
    .await
}

async fn delete_samples(db: &LocalDb, model: &str, ids: &[String]) -> DbResult<()> {
    for id in ids {
        db.execute(
            "DELETE FROM vibe_axis_samples WHERE model = ?1 AND event_id = ?2",
            params![model, id.as_str()],
        )
        .await?;
    }
    Ok(())
}

async fn clear_samples(db: &LocalDb, model: &str) -> DbResult<()> {
    db.execute(
        "DELETE FROM vibe_axis_samples WHERE model = ?1",
        params![model],
    )
    .await?;
    Ok(())
}

/// Store `set` under its `(model, dims)`, replacing any set there, and drop
/// the sample it was learned from, in one transaction.
async fn store_axes(db: &LocalDb, set: &AxisSet, counts: &ClassCounts) -> DbResult<()> {
    let axes = serde_json::to_string(set).unwrap_or_default();
    let counts = serde_json::to_string(counts).unwrap_or_default();
    let model = set.model.clone();
    let dims = set.dims as i64;
    let now = now_millis() / 1000;
    db.write(move |conn| {
        let (axes, counts, model) = (axes.clone(), counts.clone(), model.clone());
        Box::pin(async move {
            conn.execute(
                "INSERT OR REPLACE INTO vibe_axes(model, dims, axes, counts, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![model.as_str(), dims, axes.as_str(), counts.as_str(), now],
            )
            .await?;
            conn.execute(
                "DELETE FROM vibe_axis_samples WHERE model = ?1",
                params![model.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
}

/// Every set stored in `vibe_axes`, for [`VibeState`] at startup. A row that
/// does not parse is skipped (and logged): its space recomputes.
pub async fn load_stored_axes(db: &LocalDb) -> DbResult<Vec<AxisSet>> {
    let rows = db
        .query_all("SELECT model, dims, axes FROM vibe_axes", (), |row| {
            Ok((row.text(0)?, row.i64(1)?, row.text(2)?))
        })
        .await?;
    Ok(rows
        .into_iter()
        .filter_map(|(model, dims, axes)| match serde_json::from_str::<AxisSet>(&axes) {
            Ok(set) => Some(set),
            Err(error) => {
                log::warn!(target: VIBES_TARGET, "vibe axes: the stored set for {model} at {dims} does not parse: {error}");
                None
            }
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::embeddings::embedder::stub::StubEmbed;
    use crate::embeddings::space::tests::{count, keeper};
    use crate::embeddings::VectorKey;

    const BUDGET: Budget = Budget {
        per_class: 8,
        distribution: 10,
        min_per_class: 4,
        min_runs: 3,
        max_runs: 100,
        chunk: 5,
        pause: Duration::ZERO,
    };

    fn rng() -> StdRng {
        StdRng::seed_from_u64(7)
    }

    async fn db(name: &str) -> Arc<LocalDb> {
        let db = Arc::new(crate::storage::migrated_test_db(name).await);
        db.execute_script(
            "INSERT INTO workspaces(id, name, created_at, updated_at)
              VALUES ('workspace-1', 'Workspace', 1, 1);
             INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
              VALUES ('project-1', 'workspace-1', 'Project', 'proj', '/tmp/project', 1, 1);",
        )
        .await
        .unwrap();
        db
    }

    /// Seed run `run_id` with one event per `(event_type, data)`, in order.
    async fn seed_run(db: &LocalDb, run_id: &str, events: &[(&str, serde_json::Value)]) {
        db.execute(
            "INSERT INTO runs(id, project_id, status, session_id, created_at, updated_at)
             VALUES (?1, 'project-1', 'live', ?1, 10, 10)",
            params![run_id],
        )
        .await
        .unwrap();
        for (index, (event_type, data)) in events.iter().enumerate() {
            db.execute(
                "INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                                    created_at)
                 VALUES (?1, ?2, ?2, ?3, 10, ?4, ?5, 10)",
                params![
                    format!("{run_id}-{index}"),
                    run_id,
                    index as i64,
                    *event_type,
                    data.to_string()
                ],
            )
            .await
            .unwrap();
        }
    }

    /// A 40-event run alternating assistant prose and tool results, with
    /// errored results at 11 and 31: every class has members in every run.
    async fn seed_history(db: &LocalDb, runs: usize) {
        for run in 0..runs {
            let events: Vec<(&str, serde_json::Value)> = (0..40)
                .map(|i| {
                    if i % 2 == 0 {
                        let content = format!("run {run} step {i} {}", "x".repeat(i));
                        ("assistant", serde_json::json!({ "content": content }))
                    } else {
                        let error = i == 11 || i == 31;
                        (
                            "tool_result",
                            serde_json::json!({ "isError": error, "toolResult": "r" }),
                        )
                    }
                })
                .collect();
            seed_run(db, &format!("run-{run}"), &events).await;
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_model_without_axes_learns_them_from_history() {
        let db = db("vibes-learn.db").await;
        seed_history(&db, 10).await;
        let stub = Arc::new(StubEmbed::new("local-m", 4));
        let (keeper, _worker) = keeper(db.clone(), stub.clone());
        let vibes = VibeState::from_bundled();

        assert_eq!(
            step(&keeper, &vibes, true, BUDGET, &mut rng()).await,
            IDLE_SECS
        );

        assert!(vibes.covers("local-m", Some(4)));
        assert!(
            vibes.covers("cohere-embed-v4", Some(1536)),
            "the seed stays"
        );
        assert_eq!(keeper.vibe_progress(), Some(VibeAxesProgress::Ready));
        assert_eq!(
            count(
                &db,
                "SELECT COUNT(*) FROM vibe_axes WHERE model = 'local-m' AND dims = 4"
            )
            .await,
            1
        );
        assert_eq!(
            count(&db, "SELECT COUNT(*) FROM vibe_axis_samples").await,
            0
        );
        let calls = stub.calls();
        assert!(!calls.is_empty());
        for call in &calls {
            assert_eq!(
                (call.corpus, call.purpose),
                (Corpus::Resources, Purpose::Document)
            );
            assert!(call.texts.len() <= BUDGET.chunk);
            assert!(
                call.texts.iter().all(|text| text.starts_with("run ")),
                "only prose"
            );
        }
        let sent: usize = calls.iter().map(|call| call.texts.len()).sum();
        assert!(sent <= 4 * BUDGET.per_class + BUDGET.distribution, "{sent}");

        // A new event of that space is coloured, and a restart finds the set.
        let key = VectorKey {
            model: "local-m".into(),
            dims: Some(4),
        };
        assert!(vibes
            .assign_one("e", &key, &[30.0, 1.0, 1.0, 1.0])
            .is_some());
        let reloaded = VibeState::empty();
        for set in load_stored_axes(&db).await.unwrap() {
            reloaded.insert(set);
        }
        assert!(reloaded.covers("local-m", Some(4)));

        // Nothing more is sent once the space has axes.
        assert_eq!(
            step(&keeper, &vibes, true, BUDGET, &mut rng()).await,
            IDLE_SECS
        );
        assert_eq!(stub.calls().len(), calls.len());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn the_bundled_cohere_set_needs_no_recompute() {
        let db = db("vibes-cohere.db").await;
        seed_history(&db, 10).await;
        let stub = Arc::new(StubEmbed::cohere());
        let (keeper, _worker) = keeper(db.clone(), stub.clone());
        let vibes = VibeState::from_bundled();
        assert_eq!(
            step(&keeper, &vibes, true, BUDGET, &mut rng()).await,
            IDLE_SECS
        );
        assert_eq!(keeper.vibe_progress(), Some(VibeAxesProgress::Ready));
        assert!(stub.calls().is_empty());
        assert_eq!(
            count(&db, "SELECT COUNT(*) FROM vibe_axis_samples").await,
            0
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn thin_history_stores_nothing_and_says_so() {
        let db = db("vibes-thin.db").await;
        seed_history(&db, 1).await;
        let stub = Arc::new(StubEmbed::new("local-m", 4));
        let (keeper, _worker) = keeper(db.clone(), stub.clone());
        let vibes = VibeState::from_bundled();

        assert_eq!(
            step(&keeper, &vibes, true, BUDGET, &mut rng()).await,
            NOT_ENOUGH_RETRY_SECS
        );
        let Some(VibeAxesProgress::NotEnoughHistory { counts, needed, .. }) =
            keeper.vibe_progress()
        else {
            panic!("{:?}", keeper.vibe_progress());
        };
        assert_eq!(needed, BUDGET.min_per_class);
        // One run holds two events after an error, below the minimum of 4.
        assert_eq!(counts.friction_positive, 2);
        assert_eq!(counts.phase_positive, 3);
        assert!(!vibes.covers("local-m", Some(4)));
        assert!(stub.calls().is_empty(), "nothing is embedded for a refusal");
        assert_eq!(count(&db, "SELECT COUNT(*) FROM vibe_axes").await, 0);
        assert_eq!(
            count(&db, "SELECT COUNT(*) FROM vibe_axis_samples").await,
            0
        );

        // The verdict stands until its retry time rather than resampling.
        let wait = step(&keeper, &vibes, true, BUDGET, &mut rng()).await;
        assert!(wait > 60 && wait <= NOT_ENOUGH_RETRY_SECS, "{wait}");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_failed_chunk_resumes_where_it_stopped_after_a_restart() {
        let db = db("vibes-resume.db").await;
        seed_history(&db, 10).await;
        let stub = Arc::new(StubEmbed::new("local-m", 4));
        stub.script(vec![None, Some(EmbedFailure::Failed("HTTP 500".into()))]);
        let vibes = VibeState::from_bundled();

        let (first, _worker) = keeper(db.clone(), stub.clone());
        step(&first, &vibes, true, BUDGET, &mut rng()).await;
        let total = count(&db, "SELECT COUNT(*) FROM vibe_axis_samples").await as u64;
        assert_eq!(
            count(
                &db,
                "SELECT COUNT(*) FROM vibe_axis_samples WHERE embedding IS NOT NULL"
            )
            .await,
            BUDGET.chunk as i64,
            "the chunk before the failure is kept"
        );
        assert_eq!(
            first.vibe_progress(),
            Some(VibeAxesProgress::Embedding {
                embedded: BUDGET.chunk as u64,
                total
            })
        );
        assert!(!vibes.covers("local-m", Some(4)));

        // A restart: a keeper with fresh state over the same store.
        let (second, _worker) = keeper(db.clone(), stub.clone());
        assert_eq!(
            step(&second, &vibes, true, BUDGET, &mut rng()).await,
            IDLE_SECS
        );
        assert!(vibes.covers("local-m", Some(4)));
        let sent: u64 = stub
            .calls()
            .iter()
            .map(|call| call.texts.len() as u64)
            .sum();
        assert_eq!(
            sent,
            total + BUDGET.chunk as u64,
            "every sample sent once, plus the chunk that failed"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn refused_events_leave_the_sample() {
        let db = db("vibes-refused.db").await;
        seed_history(&db, 10).await;
        let stub = Arc::new(StubEmbed::new("local-m", 4));
        stub.script(vec![Some(EmbedFailure::Rejected("too long".into()))]);
        let (keeper, _worker) = keeper(db.clone(), stub.clone());
        let vibes = VibeState::from_bundled();
        // Classes deep enough that losing one chunk leaves each above the minimum.
        let budget = Budget {
            per_class: 12,
            ..BUDGET
        };
        let wait = step(&keeper, &vibes, true, budget, &mut rng()).await;
        assert_eq!(wait, REJECTED_PAUSE_SECS);
        let refused = stub.calls()[0].texts.clone();
        let left = count(&db, "SELECT COUNT(*) FROM vibe_axis_samples").await;
        assert_eq!(
            step(&keeper, &vibes, true, budget, &mut rng()).await,
            IDLE_SECS
        );
        let later: Vec<String> = stub.calls()[1..]
            .iter()
            .flat_map(|call| call.texts.clone())
            .collect();
        assert_eq!(later.len() as i64, left);
        assert!(refused.iter().all(|text| !later.contains(text)));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn without_consent_only_this_installs_runs_are_sampled() {
        let db = db("vibes-consent.db").await;
        seed_history(&db, 10).await;
        let stub = Arc::new(StubEmbed::new("local-m", 4));
        let (keeper, _worker) = keeper(db.clone(), stub.clone());
        let vibes = VibeState::from_bundled();
        // No run has a turn at or after the boundary, so none is in scope.
        assert_eq!(
            step(&keeper, &vibes, false, BUDGET, &mut rng()).await,
            NOT_ENOUGH_RETRY_SECS
        );
        assert!(stub.calls().is_empty());
        assert!(matches!(
            keeper.vibe_progress(),
            Some(VibeAxesProgress::NotEnoughHistory { counts, .. }) if counts == ClassCounts::default()
        ));
    }

    #[tokio::test]
    async fn vibe_id_lookup_cost_is_one_statement_per_256_ids() {
        use crate::storage::history_pack::testing::{publish_for_test, seed_fixture_execution};
        let db = crate::storage::migrated_test_db("vibe-hot-id-cost").await;
        seed_fixture_execution(&db, "vibe-hot-id-cost").await;
        let ids: Vec<String> = (0..513).map(|n| format!("vibe-hot-{n}")).collect();
        for (n, id) in ids.iter().enumerate() {
            db.execute(
                "INSERT INTO events(id,run_id,sequence,timestamp,event_type,data,created_at)
                VALUES(?1,?2,?3,1,'assistant','{\"content\":\"text\"}',1)",
                params![
                    id.as_str(),
                    if n % 2 == 0 { "run-1" } else { "run-2" },
                    n as i64 + 100
                ],
            )
            .await
            .unwrap();
        }
        for cataloged in [false, true] {
            if cataloged {
                publish_for_test(&db, "exec-1").await;
            }
            let before = (db.read_statement_count(), db.read_checkout_count());
            assert_eq!(load_texts(&db, &ids).await.unwrap().len(), 513);
            assert_eq!(
                (
                    db.read_statement_count() - before.0,
                    db.read_checkout_count() - before.1
                ),
                (3, 3)
            );
            let before = (db.read_statement_count(), db.read_checkout_count());
            assert!(load_texts(&db, &[]).await.unwrap().is_empty());
            assert_eq!(
                (
                    db.read_statement_count() - before.0,
                    db.read_checkout_count() - before.1
                ),
                (0, 0)
            );
        }
    }

    #[tokio::test]
    async fn vibe_selected_ids_keep_pack_and_quarantine_parity() {
        use crate::storage::history_pack::testing::{
            publish_for_test, read_both, seed_fixture_execution,
        };
        use crate::storage::quarantine::{quarantine, QuarantineSet, TRANSCRIPT_EVENT_SINK};
        let db = crate::storage::migrated_test_db("vibe-selected-parity").await;
        let events = seed_fixture_execution(&db, "vibe-selected-parity").await;
        let ids = vec![events.restored.clone(), events.second_run];
        publish_for_test(&db, "exec-1").await;
        let (hot, pack) = read_both(&db, || load_texts(&db, &ids)).await;
        let hot = hot.unwrap();
        assert_eq!(hot, pack.unwrap());
        assert_eq!(hot[&events.restored], "the restored body");
        let source = tempfile::tempdir().unwrap();
        quarantine().install_for(
            source.path(),
            QuarantineSet::from_entries([(
                TRANSCRIPT_EVENT_SINK.to_string(),
                events.restored.clone(),
                "vibe-test".to_string(),
            )]),
        );
        let (hot, pack) = read_both(&db, || load_texts(&db, &ids)).await;
        quarantine().install_for(source.path(), QuarantineSet::default());
        let hot = hot.unwrap();
        assert_eq!(hot, pack.unwrap());
        assert!(!hot.values().any(|text| text.contains("the restored body")));
    }

    #[tokio::test]
    async fn vibe_metadata_and_body_id_plan_uses_primary_key_at_production_size() {
        let db = crate::storage::migrated_test_db("vibe-id-plan").await;
        let binds = (0..256)
            .map(|n| Value::Text(format!("event-{n}")))
            .collect();
        let plan = crate::storage::query_plan(&db, &vibe_events_sql(256), binds).await;
        assert!(
            plan.contains("SEARCH events") && plan.contains("sqlite_autoindex_events"),
            "{plan}"
        );
        assert!(
            !plan.contains("SCAN events") && !plan.contains("TEMP B-TREE"),
            "{plan}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_run_is_labelled_from_sql_without_loading_its_results() {
        let db = db("vibes-steps.db").await;
        seed_run(
            &db,
            "r",
            &[
                ("assistant", serde_json::json!({ "content": "hello" })),
                ("assistant", serde_json::json!({ "thinking": "hmm" })),
                ("assistant", serde_json::json!({ "toolUses": [] })),
                ("tool_result", serde_json::json!({ "isError": true })),
                ("tool_result", serde_json::json!({ "isError": false })),
                ("tool_result", serde_json::json!({})),
                ("user", serde_json::json!({ "content": "hi" })),
            ],
        )
        .await;
        let steps = run_steps(&db, "r").await.unwrap();
        let errors: Vec<bool> = steps.iter().map(|step| step.is_error).collect();
        let candidates: Vec<bool> = steps.iter().map(|step| step.candidate).collect();
        assert_eq!(errors, [false, false, false, true, false, false, false]);
        assert_eq!(candidates, [true, true, false, false, false, false, false]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn an_archived_event_that_does_not_decode_is_not_drawn() {
        let db = db("vibes-archived.db").await;
        seed_run(
            &db,
            "r",
            &[
                ("assistant", serde_json::json!({ "content": "stub" })),
                ("tool_result", serde_json::json!({ "isError": true })),
            ],
        )
        .await;
        // Marked archived with an inline form that is no zstd payload.
        db.execute("UPDATE events SET storage_mode = 'zstd'", ())
            .await
            .unwrap();
        let steps = run_steps(&db, "r").await.unwrap();
        assert!(steps.iter().all(|step| !step.candidate && !step.is_error));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stored_axes_are_keyed_by_model_and_dims() {
        let db = db("vibes-store.db").await;
        let set = |model: &str, dims: u32, first: f32| AxisSet {
            model: model.into(),
            dims,
            axes: vec![crate::embeddings::vibes::VibeAxis {
                name: "phase".into(),
                vector: vec![first; dims as usize],
                percentiles: vec![0.0; 101],
            }],
        };
        let counts = ClassCounts::default();
        for stored in [
            set("m", 4, 1.0),
            set("m", 8, 1.0),
            set("n", 4, 1.0),
            set("m", 4, 2.0),
        ] {
            store_axes(&db, &stored, &counts).await.unwrap();
        }
        let mut loaded = load_stored_axes(&db).await.unwrap();
        loaded.sort_by(|a, b| (&a.model, a.dims).cmp(&(&b.model, b.dims)));
        let keys: Vec<(&str, u32)> = loaded.iter().map(|s| (s.model.as_str(), s.dims)).collect();
        assert_eq!(keys, [("m", 4), ("m", 8), ("n", 4)]);
        assert_eq!(loaded[0], set("m", 4, 2.0), "the same space is replaced");
    }
}
