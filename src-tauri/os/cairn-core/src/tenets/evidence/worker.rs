//! The evaluation worker: one consumer per process, woken by a hint after a
//! conclusion commits and by a periodic sweep that also serves as the startup
//! pass and the retry timer.
//!
//! Pending rows are the source of truth; the hint only shortens the wait.
//!
//! When nothing configured can answer a decision, an event is settled
//! `skipped` with the refusal as its `last_error`, and the periodic sweep
//! leaves it there: retrying cannot help until the catalog changes. Only the
//! startup pass re-queues those events ([`requeue_unanswerable`]), so
//! configuring a decision-capable offer and restarting re-evaluates them.
//!
//! An embedding outage is not the event's failure. While the shared embedding
//! gate is paused or the provider is failing, an event is left exactly as it
//! was and the next sweep tries again; only a refused request or a failed
//! judgement spends one of its [`super::MAX_ATTEMPTS`].

use std::sync::Arc;

use async_trait::async_trait;

use super::events::{
    due_events, mark_attempt_failed, mark_skipped, requeue_unanswerable_events, PendingEvent,
};
use super::judge::{self, Sign};
use super::vectors::{project_tenet_vectors, tenet_embed_text, vector_db};
use super::{stable_id, CONFIDENCE_FLOOR, EMBED_CHUNK, KNN_FLOOR, KNN_K, SWEEP_INTERVAL};
use crate::embeddings::{Corpus, Embed, EmbedFailure, Embedded, Purpose, VectorKey};
use crate::models::decision::{Decision, DecisionNode, MeasuredBy};
use crate::orchestrator::{DecisionFailure, Orchestrator};
use crate::services::EventEmitter;
use crate::storage::LocalDb;
use crate::tenets::{TenetFilter, TenetStatus, TenetView};

/// How many due events one database yields per pass.
const DRAIN_BATCH: usize = 20;

/// Everything the worker reaches outside its own tables. Production wraps the
/// orchestrator; tests record calls.
#[async_trait]
pub trait EvidenceDeps: Send + Sync {
    /// This machine's stable id, the owner key of the events it writes.
    fn this_device(&self) -> String;
    fn now(&self) -> i64;
    /// Every open database: the private one and each team replica.
    async fn dbs(&self) -> Vec<Arc<LocalDb>>;
    /// Embed `texts` into the resource corpus through the shared embedder.
    async fn embed(&self, purpose: Purpose, texts: Vec<String>) -> Result<Embedded, EmbedFailure>;
    /// The space resource vectors are stored in now; `None` when no
    /// embedding offer is selected.
    fn vector_key(&self) -> Option<VectorKey>;
    /// Answer `decision` about `input` through the models facet's `decide`,
    /// in `project_id`'s model scope.
    async fn judge(
        &self,
        project_id: &str,
        decision: &DecisionNode,
        input: &str,
    ) -> Result<Decision, DecisionFailure>;
    /// Wake a job that a message was just queued for.
    fn nudge(&self, job_id: &str);
    fn emitter(&self) -> &dyn EventEmitter;
}

struct OrchestratorDeps {
    orch: Orchestrator,
    embedder: Arc<dyn Embed>,
    device: String,
}

#[async_trait]
impl EvidenceDeps for OrchestratorDeps {
    fn this_device(&self) -> String {
        self.device.clone()
    }

    fn now(&self) -> i64 {
        self.orch.services.clock.now()
    }

    async fn dbs(&self) -> Vec<Arc<LocalDb>> {
        self.orch.db.all_dbs().await
    }

    async fn embed(&self, purpose: Purpose, texts: Vec<String>) -> Result<Embedded, EmbedFailure> {
        self.embedder.embed(Corpus::Resources, purpose, texts).await
    }

    fn vector_key(&self) -> Option<VectorKey> {
        self.embedder.active(Corpus::Resources)
    }

    async fn judge(
        &self,
        project_id: &str,
        decision: &DecisionNode,
        input: &str,
    ) -> Result<Decision, DecisionFailure> {
        self.orch
            .decide(Some(project_id), decision, input.to_string())
            .await
    }

    fn nudge(&self, job_id: &str) {
        if let Err(error) = crate::messages::delivery::nudge_job_for_urgency(
            &self.orch,
            job_id,
            crate::messages::queued::DeliveryUrgency::Steer,
        ) {
            log::warn!("tenet contradiction for {job_id} is queued but the nudge failed: {error}");
        }
    }

    fn emitter(&self) -> &dyn EventEmitter {
        self.orch.services.emitter.as_ref()
    }
}

/// Start the worker. The first interval tick fires at once, which is the
/// startup sweep.
pub fn spawn(orch: Orchestrator) {
    let hint = orch.tenet_evidence_hint();
    tokio::spawn(async move {
        let anon = orch.anon_device_manager.clone();
        let device = match tokio::task::spawn_blocking(move || anon.device_id()).await {
            Ok(device) => device,
            Err(error) => {
                log::warn!("tenet evidence worker could not resolve this machine's id: {error}");
                return;
            }
        };
        let embedder = orch.embedder();
        let deps = OrchestratorDeps {
            orch,
            embedder,
            device,
        };
        requeue_unanswerable(&deps).await;
        let mut interval = tokio::time::interval(SWEEP_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = hint.notified() => {}
                _ = interval.tick() => {}
            }
            drain_due(&deps).await;
            super::wake::recheck_contradictions(&deps).await;
        }
    });
}

/// What one evaluation came to.
#[derive(Debug, Clone, PartialEq)]
pub enum Processed {
    /// Settled without a judgement.
    Skipped(&'static str),
    /// Settled without a judgement because nothing configured can answer a
    /// decision; carries the refusal, which names the fixes.
    Unanswerable(String),
    /// Judged; this many evidence rows were stored.
    Done(usize),
    /// Another writer settled the event first.
    Lost,
    /// One failed attempt was recorded.
    Failed(String),
    /// Embedding is paused, failing, or not configured; the event was left
    /// untouched for the next sweep.
    Deferred(String),
}

/// Return every event this machine skipped for want of a decision-capable
/// offer to the queue, in every database. Called once, at startup: a catalog
/// change takes effect on restart, and the periodic sweep never re-asks a
/// refusal it would only get again.
pub async fn requeue_unanswerable(deps: &dyn EvidenceDeps) -> usize {
    let device = deps.this_device();
    let mut requeued = 0;
    for db in deps.dbs().await {
        match requeue_unanswerable_events(&db, &device).await {
            Ok(count) => requeued += count,
            Err(error) => {
                log::warn!("tenet evidence could not re-queue unanswerable events: {error}");
            }
        }
    }
    if requeued > 0 {
        log::info!("tenet evidence re-queued {requeued} event(s) skipped for want of a decision-capable model");
    }
    requeued
}

/// Evaluate every due event this machine owns, in every database.
pub async fn drain_due(deps: &dyn EvidenceDeps) -> Vec<Processed> {
    let device = deps.this_device();
    let mut results = Vec::new();
    for db in deps.dbs().await {
        let events = match due_events(&db, &device, deps.now(), DRAIN_BATCH).await {
            Ok(events) => events,
            Err(error) => {
                log::warn!("tenet evidence sweep could not read due events: {error}");
                continue;
            }
        };
        for event in events {
            results.push(process(deps, &db, &event).await);
        }
    }
    results
}

/// Evaluate one event and settle it.
pub async fn process(deps: &dyn EvidenceDeps, db: &LocalDb, event: &PendingEvent) -> Processed {
    let evaluation = evaluate(deps, db, event).await;
    // Read the clock only once the embedding and the judge have returned.
    // Evidence is dated when it lands, so a contradiction that finishes after
    // a wake fired is newer than that wake's edge and counts toward the next.
    let now = deps.now();
    let settled = match evaluation {
        Ok(Evaluation::Skip(reason)) => match mark_skipped(db, event, now, None).await {
            Ok(true) => Ok(Processed::Skipped(reason)),
            Ok(false) => Ok(Processed::Lost),
            Err(error) => Err(error.to_string()),
        },
        Ok(Evaluation::Unanswerable(refusal)) => {
            log::info!("tenet evidence for {} skipped: {refusal}", event.source_uri);
            match mark_skipped(db, event, now, Some(&refusal)).await {
                Ok(true) => Ok(Processed::Unanswerable(refusal)),
                Ok(false) => Ok(Processed::Lost),
                Err(error) => Err(error.to_string()),
            }
        }
        Ok(Evaluation::Judged { verdicts, model }) => store(db, event, &verdicts, &model, now)
            .await
            .map(|stored| match stored {
                Some(count) => Processed::Done(count),
                None => Processed::Lost,
            }),
        Err(Failure::Embedding(failure)) if failure.is_provider_outcome() => {
            log::debug!(
                "tenet evidence for {} waits for embedding: {failure}",
                event.source_uri
            );
            return Processed::Deferred(failure.to_string());
        }
        Err(failure) => Err(failure.to_string()),
    };
    match settled {
        Ok(processed) => {
            if matches!(processed, Processed::Done(count) if count > 0) {
                super::emit_evidence_changed(deps.emitter());
            }
            processed
        }
        Err(error) => {
            log::warn!(
                "tenet evidence for {} failed (attempt {}): {error}",
                event.source_uri,
                event.attempts + 1
            );
            if let Err(mark) = mark_attempt_failed(db, event, now, &error).await {
                log::warn!("could not record the failed tenet evidence attempt: {mark}");
            }
            Processed::Failed(error)
        }
    }
}

enum Evaluation {
    Skip(&'static str),
    /// No configured offer can answer the decision.
    Unanswerable(String),
    Judged {
        /// Each verdict worth storing.
        verdicts: Vec<StoredVerdict>,
        model: String,
    },
}

#[derive(Clone)]
struct StoredVerdict {
    tenet_id: String,
    sign: Sign,
    /// The measured probability of the chosen option.
    probability: f64,
    measured_by: MeasuredBy,
}

/// Why an evaluation produced nothing.
enum Failure {
    /// Embedding did not answer; only a refused request is the event's fault.
    Embedding(EmbedFailure),
    /// Anything else: storage, or a failed judgement.
    Other(String),
}

impl From<String> for Failure {
    fn from(error: String) -> Self {
        Self::Other(error)
    }
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Embedding(failure) => failure.fmt(f),
            Self::Other(error) => f.write_str(error),
        }
    }
}

async fn evaluate(
    deps: &dyn EvidenceDeps,
    db: &LocalDb,
    event: &PendingEvent,
) -> Result<Evaluation, Failure> {
    let approved = crate::tenets::list(
        db,
        &event.project_id,
        &TenetFilter {
            statuses: vec![TenetStatus::Approved],
            ..Default::default()
        },
    )
    .await?;
    let Some(project_key) = approved.first().map(|view| view.project_key.clone()) else {
        return Ok(Evaluation::Skip("no approved tenets"));
    };

    // Ensure every approved tenet has a vector in the space queries are
    // embedded in now. Each chunk is saved as soon as it returns, so a later
    // failure keeps what earlier chunks paid for.
    let key = deps
        .vector_key()
        .ok_or(Failure::Embedding(EmbedFailure::Unconfigured))?;
    let vectors_db = vector_db(db);
    let mut vectors = project_tenet_vectors(vectors_db, &project_key, &key)
        .await
        .map_err(|error| format!("could not load tenet vectors: {error}"))?;
    let missing: Vec<&TenetView> = approved
        .iter()
        .filter(|view| !vectors.contains_key(&view.uri))
        .collect();
    for chunk in missing.chunks(EMBED_CHUNK) {
        let texts = chunk.iter().map(|view| tenet_embed_text(view)).collect();
        let embedded = deps
            .embed(Purpose::Document, texts)
            .await
            .map_err(Failure::Embedding)?;
        for (view, vector) in chunk.iter().zip(embedded.vectors) {
            crate::embeddings::queries::upsert_resource_embedding_async(
                vectors_db,
                &view.uri,
                &crate::embeddings::vector::to_bytes(&vector),
                &embedded.model,
                embedded.dims as i32,
            )
            .await
            .map_err(|error| format!("could not save a tenet vector: {error}"))?;
            vectors.insert(view.uri.clone(), vector);
        }
    }

    let query = deps
        .embed(Purpose::Query, vec![event.text.clone()])
        .await
        .map_err(Failure::Embedding)?
        .vectors
        .pop()
        .unwrap_or_default();
    let mut scored: Vec<(f32, &TenetView)> = approved
        .iter()
        .filter_map(|view| {
            let vector = vectors.get(&view.uri)?;
            (vector.len() == query.len()).then(|| {
                (
                    crate::embeddings::vector::cosine_similarity(vector, &query),
                    view,
                )
            })
        })
        .collect();
    scored.sort_by(|a, b| {
        b.0.partial_cmp(&a.0)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.1.row.number.cmp(&b.1.row.number))
    });
    log::debug!(
        "tenet evidence for {}: best similarity {:?}",
        event.source_uri,
        scored.first().map(|(score, _)| *score)
    );
    let candidates: Vec<&TenetView> = scored
        .into_iter()
        .filter(|(score, _)| *score >= KNN_FLOOR)
        .take(KNN_K)
        .map(|(_, view)| view)
        .collect();
    if candidates.is_empty() {
        return Ok(Evaluation::Skip("no tenet is near enough"));
    }

    let decision = match deps
        .judge(
            &event.project_id,
            &judge::decision(&candidates),
            &event.text,
        )
        .await
    {
        Ok(decision) => decision,
        Err(DecisionFailure::Unanswerable(refusal)) => {
            return Ok(Evaluation::Unanswerable(refusal))
        }
        Err(DecisionFailure::Failed(error)) => return Err(Failure::Other(error)),
    };
    let verdicts = judge::parse(&decision, &candidates)
        .into_iter()
        .filter(|verdict| verdict.sign != Sign::Neither)
        .filter_map(|verdict| {
            let probability = verdict
                .probability
                .filter(|probability| *probability >= CONFIDENCE_FLOOR)?;
            Some(StoredVerdict {
                tenet_id: candidates[verdict.candidate].row.id.clone(),
                sign: verdict.sign,
                probability,
                measured_by: verdict.measured_by,
            })
        })
        .collect();
    Ok(Evaluation::Judged {
        verdicts,
        model: decision.model,
    })
}

/// Store the verdicts and settle the event in one write. `None` when another
/// writer settled the event first, in which case nothing is stored.
///
/// The first qualifying judgement of a tenet from a source stands: a reopened
/// and re-closed issue only adds evidence for tenets not yet judged from it.
async fn store(
    db: &LocalDb,
    event: &PendingEvent,
    verdicts: &[StoredVerdict],
    model: &str,
    now: i64,
) -> Result<Option<usize>, String> {
    let event = event.clone();
    let verdicts = verdicts.to_vec();
    let model = model.to_string();
    db.write(move |conn| {
        let event = event.clone();
        let verdicts = verdicts.clone();
        let model = model.clone();
        Box::pin(async move {
            let settled = conn
                .execute(
                    "UPDATE tenet_evidence_events SET state = 'done', evaluated_at = ?1
                     WHERE id = ?2 AND state = 'pending' AND owner_device_id = ?3",
                    cairn_db::turso::params![
                        now,
                        event.id.as_str(),
                        event.owner_device_id.as_str()
                    ],
                )
                .await?;
            if settled == 0 {
                return Ok(None);
            }
            let mut stored = 0;
            for verdict in &verdicts {
                let tenet_id = &verdict.tenet_id;
                // A tenet superseded while the judge ran gets no evidence.
                let still_approved = crate::storage::query_opt_text_conn(
                    conn,
                    "SELECT id FROM tenets WHERE id = ?1 AND status = 'approved'",
                    cairn_db::turso::params![tenet_id.as_str()],
                )
                .await?
                .is_some();
                if !still_approved {
                    continue;
                }
                let id = stable_id(
                    "tenet-evidence",
                    &format!("{tenet_id}:{}", event.source_uri),
                );
                stored += conn
                    .execute(
                        "INSERT OR IGNORE INTO tenet_evidence
                             (id, project_id, tenet_id, event_id, source_uri, sign, confidence,
                              measured_by, model, owner_device_id, evaluated_at)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                        cairn_db::turso::params![
                            id,
                            event.project_id.as_str(),
                            tenet_id.as_str(),
                            event.id.as_str(),
                            event.source_uri.as_str(),
                            verdict.sign.as_str(),
                            verdict.probability,
                            verdict.measured_by.as_str(),
                            model.as_str(),
                            event.owner_device_id.as_str(),
                            now
                        ],
                    )
                    .await? as usize;
            }
            Ok(Some(stored))
        })
    })
    .await
    .map_err(|error| format!("could not store tenet evidence: {error}"))
}
