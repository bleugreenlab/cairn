//! The resource-recall corpus, and the sweep that keeps it embedded.
//!
//! `resource_embeddings` holds one vector per resource, keyed by its canonical
//! URI. Each family has one producer that embeds it as it changes, and one
//! text builder here that producer and sweep share, so a re-embed writes
//! exactly the vector the producer would have:
//!
//! | Family | Producer | Rebuilt by |
//! |---|---|---|
//! | issue `cairn://p/K/N` | issue create and update (description) | this sweep |
//! | node or task artifact (latest version of each) | artifact write | this sweep |
//! | memory `…/memories/N` | memory create and update | this sweep |
//! | tenet `cairn://p/K/tenets/N` (approved) | the tenet evidence worker, lazily | this sweep |
//! | skill `cairn://skills/ID` | skill create and update (description) | this sweep, with consent |
//! | node summary (owner centroid) | the embed worker, from live events | live events only |
//!
//! A node summary is folded from event vectors that are discarded once
//! folded, so it cannot be rebuilt; after a model switch it regrows from the
//! node's next events and is not counted as progress.
//!
//! ## The sweep
//!
//! After a switch every vector is gone, and an idle install would otherwise
//! never get its corpus back: producers only fire on change. The sweep walks
//! the corpus kind by kind, newest first, and embeds whatever has no vector
//! in the active space. It is resumable: a walk holds its position in memory,
//! each embedding call is persisted before the next is sent, and a failed
//! call leaves the walk where it was, so a failure costs one call. Once a
//! walk finishes it counts the corpus for status, and the next one starts
//! after `REWALK_SECS`, which catches anything a producer missed.
//!
//! It answers the same two questions the turn sweep does (see
//! [`super::turns::Scope`]): without consent only resources created at or
//! after this install's boundary are embedded, and skills, which carry no
//! timestamp, only with consent.
//!
//! A resource whose text is empty gets a `dims = 0` marker, model-independent
//! like a turn tombstone, so it is not reconsidered while it stays empty.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use cairn_db::turso::{params, Value};

use super::embedder::{Corpus, EmbedFailure, Purpose, VectorKey};
use super::space::{Reconciled, ResourceProgress, SpaceKeeper, Trigger};
use super::turns::{gate_wait, install_boundary, REJECTED_PAUSE_SECS};
use super::{queries, vector};
use crate::models::Memory;
use crate::storage::{DbResult, LocalDb, RowExt};

/// Resources considered per step.
const PAGE: usize = 64;
/// Ceiling on resources per embedding call.
const CHUNK: usize = 64;
/// Ceiling on characters per embedding call: the unit a failure costs.
const CHUNK_CHARS: usize = 48_000;
/// The wait once a walk has nothing left to do.
const IDLE_SECS: u64 = 60;
/// The wait between steps that embedded something, so a re-embed does not
/// monopolise the provider live embedding shares.
const BACKLOG_PAUSE_SECS: u64 = 2;
/// How long a finished walk stands before the corpus is walked again.
const REWALK_SECS: u64 = 1800;
/// The wait after a storage failure or an unreadable boundary.
const RETRY_SECS: u64 = 300;

/// One resource as it is embedded: where its vector is stored, and the text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceTarget {
    pub uri: String,
    pub text: String,
}

// ===== the one text and URI builder per kind =====

/// An issue embeds its description.
pub fn issue_embed_text(description: Option<&str>) -> String {
    description.unwrap_or_default().to_string()
}

/// A memory embeds its content.
pub fn memory_embed_text(memory: &Memory) -> String {
    memory.content.clone()
}

/// A memory's vector lives at its node's `…/memories/{seq}`.
pub async fn memory_embed_target(db: &LocalDb, memory: &Memory) -> DbResult<ResourceTarget> {
    Ok(ResourceTarget {
        uri: crate::memories::db::build_node_memory_uri_for_memory(db, memory).await?,
        text: memory_embed_text(memory),
    })
}

/// An artifact's vector lives at its producing job's home plus the name it is
/// stored under (its type when it was written through the generic alias).
pub fn artifact_embed_uri_at(home: &str, output_name: Option<&str>, artifact_type: &str) -> String {
    format!("{home}/{}", output_name.unwrap_or(artifact_type))
}

/// [`artifact_embed_uri_at`] from the job's home, when the job is addressable.
pub async fn artifact_embed_uri(
    db: &LocalDb,
    job_id: &str,
    output_name: Option<&str>,
    artifact_type: &str,
) -> DbResult<Option<String>> {
    Ok(crate::jobs::queries::home_uri_for_job(db, job_id)
        .await?
        .map(|home| artifact_embed_uri_at(&home, output_name, artifact_type)))
}

/// An artifact embeds the prose fields its type carries; empty when none.
pub fn artifact_text(artifact_type: &str, data: &serde_json::Value) -> String {
    super::artifact_embed_text(artifact_type, data).unwrap_or_default()
}

/// A skill embeds its description.
pub fn skill_embed_text(description: &str) -> String {
    description.to_string()
}

/// A workspace skill's vector lives at `cairn://skills/{id}`.
pub fn skill_embed_target(skill_id: &str, description: &str) -> ResourceTarget {
    ResourceTarget {
        uri: cairn_common::uri::build_skill_uri(skill_id, &[]),
        text: skill_embed_text(description),
    }
}

// ===== where the corpus lives =====

/// What the sweep walks: the databases holding issues, memories, artifacts and
/// tenets, and the skills on disk. The orchestrator in production.
#[async_trait]
pub trait ResourceSource: Send + Sync {
    /// The private database and every open team replica.
    async fn corpus_dbs(&self) -> Vec<Arc<LocalDb>>;
    /// Every workspace skill, as it is embedded.
    fn skills(&self) -> Vec<ResourceTarget>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Issues,
    Memories,
    Artifacts,
    Tenets,
    Skills,
}

const KINDS: [Kind; 5] = [
    Kind::Issues,
    Kind::Memories,
    Kind::Artifacts,
    Kind::Tenets,
    Kind::Skills,
];

/// What remains of the current kind in the current database, newest first:
/// row ids still to be read, or targets already built.
enum Queue {
    Ids(VecDeque<String>),
    Targets(VecDeque<ResourceTarget>),
}

impl Queue {
    fn is_empty(&self) -> bool {
        match self {
            Self::Ids(ids) => ids.is_empty(),
            Self::Targets(targets) => targets.is_empty(),
        }
    }

    fn pop(&mut self, count: usize) {
        match self {
            Self::Ids(ids) => drop(ids.drain(..count.min(ids.len()))),
            Self::Targets(targets) => drop(targets.drain(..count.min(targets.len()))),
        }
    }
}

/// One walk over the corpus, in one space and one consent scope.
struct Walk {
    generation: u64,
    since: Option<i64>,
    dbs: Vec<Arc<LocalDb>>,
    db: usize,
    kind: usize,
    queue: Option<Queue>,
    /// Job homes already resolved this walk; many artifacts share a job.
    homes: HashMap<String, Option<String>>,
    tally: ResourceProgress,
}

fn in_list(values: &[String]) -> (String, Vec<Value>) {
    let placeholders = (1..=values.len())
        .map(|index| format!("?{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    let values = values.iter().map(|id| Value::Text(id.clone())).collect();
    (placeholders, values)
}

impl Walk {
    fn new(generation: u64, since: Option<i64>, dbs: Vec<Arc<LocalDb>>) -> Self {
        Self {
            generation,
            since,
            dbs,
            db: 0,
            kind: 0,
            queue: None,
            homes: HashMap::new(),
            tally: ResourceProgress::default(),
        }
    }

    /// The next page of targets and how many queue entries it consumed, or
    /// `None` once every kind of every database has been walked.
    async fn page(
        &mut self,
        source: &dyn ResourceSource,
    ) -> DbResult<Option<(Vec<ResourceTarget>, usize)>> {
        loop {
            if self.db >= self.dbs.len() {
                return Ok(None);
            }
            let db = self.dbs[self.db].clone();
            let kind = KINDS[self.kind];
            if self.queue.is_none() {
                self.queue = Some(self.load(&db, kind, source).await?);
            }
            if self.queue.as_ref().is_some_and(Queue::is_empty) {
                self.queue = None;
                self.kind += 1;
                if self.kind == KINDS.len() {
                    self.kind = 0;
                    self.db += 1;
                }
                continue;
            }
            let page = match self.queue.as_ref() {
                Some(Queue::Targets(targets)) => {
                    let page: Vec<ResourceTarget> = targets.iter().take(PAGE).cloned().collect();
                    let taken = page.len();
                    (page, taken)
                }
                Some(Queue::Ids(ids)) => {
                    let ids: Vec<String> = ids.iter().take(PAGE).cloned().collect();
                    let taken = ids.len();
                    (self.targets(&db, kind, &ids).await?, taken)
                }
                None => unreachable!("loaded above"),
            };
            return Ok(Some(page));
        }
    }

    /// Consume what the last page took.
    fn advance(&mut self, taken: usize) {
        if let Some(queue) = self.queue.as_mut() {
            queue.pop(taken);
        }
    }

    /// Everything of `kind` in `db` within the walk's scope, newest first.
    async fn load(
        &mut self,
        db: &LocalDb,
        kind: Kind,
        source: &dyn ResourceSource,
    ) -> DbResult<Queue> {
        let since = self.since.unwrap_or(i64::MIN);
        let ids = |row: &cairn_db::turso::Row| row.text(0);
        Ok(match kind {
            Kind::Issues => Queue::Ids(
                db.query_all(
                    "SELECT id FROM issues
                      WHERE description IS NOT NULL AND created_at >= ?1
                      ORDER BY created_at DESC, id DESC",
                    params![since],
                    ids,
                )
                .await?
                .into(),
            ),
            Kind::Memories => Queue::Ids(
                db.query_all(
                    // A draft is embedded when it is confirmed, not before.
                    "SELECT id FROM memories
                      WHERE job_id IS NOT NULL AND node_seq IS NOT NULL AND status != 'draft'
                        AND created_at >= ?1
                      ORDER BY created_at DESC, id DESC",
                    params![since],
                    ids,
                )
                .await?
                .into(),
            ),
            Kind::Artifacts => Queue::Ids(latest_artifacts(db, since).await?.into()),
            Kind::Tenets => Queue::Targets(approved_tenets(db, self.since).await?.into()),
            // Skills are files, not rows: walked once, with the private
            // database, and only with consent, having no timestamp.
            Kind::Skills if self.db == 0 && self.since.is_none() => {
                Queue::Targets(source.skills().into())
            }
            Kind::Skills => Queue::Targets(VecDeque::new()),
        })
    }

    /// The targets for `ids` of `kind`, in `ids`' order. A row whose URI
    /// cannot be built (its job no longer addressable) is left out.
    async fn targets(
        &mut self,
        db: &LocalDb,
        kind: Kind,
        ids: &[String],
    ) -> DbResult<Vec<ResourceTarget>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let (placeholders, values) = in_list(ids);
        let mut by_id: HashMap<String, ResourceTarget> = HashMap::new();
        match kind {
            Kind::Issues => {
                let rows = db
                    .query_all(
                        format!(
                            "SELECT i.id, p.key, i.number, i.description
                               FROM issues i JOIN projects p ON p.id = i.project_id
                              WHERE i.id IN ({placeholders})"
                        ),
                        values,
                        |row| Ok((row.text(0)?, row.text(1)?, row.i64(2)?, row.opt_text(3)?)),
                    )
                    .await?;
                for (id, key, number, description) in rows {
                    by_id.insert(
                        id,
                        ResourceTarget {
                            uri: cairn_common::uri::build_issue_uri(&key, number as i32),
                            text: issue_embed_text(description.as_deref()),
                        },
                    );
                }
            }
            Kind::Memories => {
                let memories = db
                    .query_all(
                        format!(
                            "SELECT {} FROM memories WHERE id IN ({placeholders})",
                            crate::memories::db::MEMORY_COLUMNS_FOR_COMMANDS
                        ),
                        values,
                        crate::memories::db::memory_from_row_for_commands,
                    )
                    .await?;
                for memory in memories {
                    if let Ok(target) = memory_embed_target(db, &memory).await {
                        by_id.insert(memory.id.clone(), target);
                    }
                }
            }
            Kind::Artifacts => {
                let rows = db
                    .query_all(
                        format!(
                            "SELECT id, job_id, output_name, artifact_type, data
                               FROM artifacts WHERE id IN ({placeholders})"
                        ),
                        values,
                        |row| {
                            Ok((
                                row.text(0)?,
                                row.text(1)?,
                                row.opt_text(2)?,
                                row.text(3)?,
                                row.text(4)?,
                            ))
                        },
                    )
                    .await?;
                for (id, job_id, output_name, artifact_type, data) in rows {
                    let home = match self.homes.get(&job_id) {
                        Some(home) => home.clone(),
                        None => {
                            let home = crate::jobs::queries::home_uri_for_job(db, &job_id).await?;
                            self.homes.insert(job_id.clone(), home.clone());
                            home
                        }
                    };
                    let Some(home) = home else { continue };
                    let data = serde_json::from_str(&data).unwrap_or(serde_json::Value::Null);
                    by_id.insert(
                        id,
                        ResourceTarget {
                            uri: artifact_embed_uri_at(
                                &home,
                                output_name.as_deref(),
                                &artifact_type,
                            ),
                            text: artifact_text(&artifact_type, &data),
                        },
                    );
                }
            }
            Kind::Tenets | Kind::Skills => {}
        }
        Ok(ids.iter().filter_map(|id| by_id.remove(id)).collect())
    }
}

/// The latest version of every artifact created at or after `since`, newest
/// first. Selected here rather than in SQL: `artifacts` has no index that
/// would let a correlated "latest" test run per row cheaply, and the columns
/// read are small.
async fn latest_artifacts(db: &LocalDb, since: i64) -> DbResult<Vec<String>> {
    let rows = db
        .query_all(
            "SELECT id, job_id, COALESCE(output_name, artifact_type), version, created_at
               FROM artifacts WHERE job_id IS NOT NULL",
            (),
            |row| {
                Ok((
                    row.text(0)?,
                    row.text(1)?,
                    row.text(2)?,
                    row.i64(3)?,
                    row.i64(4)?,
                ))
            },
        )
        .await?;
    let mut latest: HashMap<(String, String), (i64, i64, String)> = HashMap::new();
    for (id, job_id, name, version, created_at) in rows {
        let entry = latest
            .entry((job_id, name))
            .or_insert((version, created_at, id.clone()));
        if version > entry.0 {
            *entry = (version, created_at, id);
        }
    }
    let mut kept: Vec<(i64, String)> = latest
        .into_values()
        .filter(|(_, created_at, _)| *created_at >= since)
        .map(|(_, created_at, id)| (created_at, id))
        .collect();
    kept.sort_by(|a, b| b.cmp(a));
    Ok(kept.into_iter().map(|(_, id)| id).collect())
}

/// Every approved tenet in `db`, newest first, as the evidence worker embeds
/// it.
async fn approved_tenets(db: &LocalDb, since: Option<i64>) -> DbResult<Vec<ResourceTarget>> {
    let projects = db
        .query_all("SELECT id FROM projects", (), |row| row.text(0))
        .await?;
    let mut tenets = Vec::new();
    for project_id in projects {
        let views = crate::tenets::list(
            db,
            &project_id,
            &crate::tenets::TenetFilter {
                statuses: vec![crate::tenets::TenetStatus::Approved],
                ..Default::default()
            },
        )
        .await
        .map_err(crate::storage::DbError::Row)?;
        tenets.extend(
            views
                .into_iter()
                .filter(|view| since.is_none_or(|since| view.row.created_at >= since))
                .map(|view| {
                    (
                        view.row.created_at,
                        ResourceTarget {
                            text: crate::tenets::evidence::vectors::tenet_embed_text(&view),
                            uri: view.uri,
                        },
                    )
                }),
        );
    }
    tenets.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| b.1.uri.cmp(&a.1.uri)));
    Ok(tenets.into_iter().map(|(_, target)| target).collect())
}

/// What one page came to.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Settled {
    /// Resources with text: the ones progress counts.
    eligible: u64,
    /// Of those, holding a vector of the space now.
    embedded: u64,
    /// Embedded by this page.
    sent: u64,
}

/// Why a page stopped short.
#[derive(Debug, Clone, PartialEq)]
enum PageFailure {
    /// Not configured, paused, or the provider failed: the gate decides.
    Gate,
    /// The provider refused the call holding `uri` as written.
    Refused {
        uri: String,
        reason: String,
    },
    Storage(String),
}

/// Bring one page into `key`'s space: mark the empty, embed the rest that
/// have no vector there, persisting each call as it returns.
async fn settle_page(
    keeper: &SpaceKeeper,
    generation: u64,
    key: &VectorKey,
    targets: Vec<ResourceTarget>,
) -> Result<Settled, PageFailure> {
    let db = keeper.db();
    let storage = |error: crate::storage::DbError| PageFailure::Storage(error.to_string());
    let uris: Vec<String> = targets.iter().map(|target| target.uri.clone()).collect();
    let existing: HashMap<String, (String, i64)> = if uris.is_empty() {
        HashMap::new()
    } else {
        let (placeholders, values) = in_list(&uris);
        db.query_all(
            format!(
                "SELECT uri, model, dims FROM resource_embeddings WHERE uri IN ({placeholders})"
            ),
            values,
            |row| Ok((row.text(0)?, (row.text(1)?, row.i64(2)?))),
        )
        .await
        .map_err(storage)?
        .into_iter()
        .collect()
    };

    let mut settled = Settled::default();
    let mut missing: Vec<ResourceTarget> = Vec::new();
    for target in targets {
        let row = existing.get(&target.uri);
        if target.text.trim().is_empty() {
            if row.is_none_or(|(_, dims)| *dims != 0) {
                queries::upsert_resource_embedding_async(db, &target.uri, &[], &key.model, 0)
                    .await
                    .map_err(storage)?;
            }
            continue;
        }
        settled.eligible += 1;
        let in_space = row.is_some_and(|(model, dims)| {
            *model == key.model
                && *dims > 0
                && key.dims.is_none_or(|width| i64::from(width) == *dims)
        });
        if in_space {
            settled.embedded += 1;
        } else {
            missing.push(target);
        }
    }

    for chunk in chunks(&missing) {
        let texts: Vec<String> = chunk.iter().map(|target| target.text.clone()).collect();
        match keeper
            .embedder()
            .embed(Corpus::Resources, Purpose::Document, texts)
            .await
        {
            Ok(embedded) => {
                for (target, embedding) in chunk.iter().zip(embedded.vectors.iter()) {
                    queries::upsert_resource_embedding_async(
                        db,
                        &target.uri,
                        &vector::to_bytes(embedding),
                        &embedded.model,
                        embedded.dims as i32,
                    )
                    .await
                    .map_err(storage)?;
                }
                let count = chunk.len() as u64;
                settled.embedded += count;
                settled.sent += count;
                keeper.note_resources_embedded(generation, count);
            }
            Err(EmbedFailure::Rejected(reason)) => {
                return Err(PageFailure::Refused {
                    uri: chunk[0].uri.clone(),
                    reason,
                })
            }
            Err(_) => return Err(PageFailure::Gate),
        }
    }
    Ok(settled)
}

/// Split targets into calls bounded by count and by characters. A single
/// target always forms a call: the door caps each text.
fn chunks(targets: &[ResourceTarget]) -> Vec<&[ResourceTarget]> {
    let mut out = Vec::new();
    let (mut start, mut chars) = (0, 0);
    for (index, target) in targets.iter().enumerate() {
        let length = target.text.len().min(super::EMBED_TEXT_BUDGET);
        if index - start >= CHUNK || (index > start && chars + length > CHUNK_CHARS) {
            out.push(&targets[start..index]);
            start = index;
            chars = 0;
        }
        chars += length;
    }
    if start < targets.len() {
        out.push(&targets[start..]);
    }
    out
}

/// What one step of the sweep did, and so how long to wait.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Step {
    /// A page was settled; `sent` resources were embedded by it.
    Worked { sent: u64 },
    /// A walk finished and counted the corpus.
    Finished,
    /// Nothing to do now.
    Idle,
    /// Wait this long, in seconds.
    Wait(u64),
}

/// The sweep's state between steps. Held by its task; a test drives it
/// step by step.
pub(crate) struct ResourceSweep {
    keeper: Arc<SpaceKeeper>,
    source: Arc<dyn ResourceSource>,
    walk: Option<Walk>,
    /// When the last walk finished, and in which space and scope.
    finished: Option<(u64, Option<i64>, Instant)>,
    /// The resource the last refusal was logged for.
    refused: Option<String>,
}

impl ResourceSweep {
    pub(crate) fn new(keeper: Arc<SpaceKeeper>, source: Arc<dyn ResourceSource>) -> Self {
        Self {
            keeper,
            source,
            walk: None,
            finished: None,
            refused: None,
        }
    }

    /// Settle one page of the walk, starting a walk when one is due.
    pub(crate) async fn step(&mut self, consented: bool) -> Step {
        let embedder = self.keeper.embedder().clone();
        if !embedder.ready() {
            return if embedder.health().configured {
                Step::Wait(gate_wait(embedder.as_ref()))
            } else {
                Step::Idle
            };
        }
        let Some(key) = embedder.active(Corpus::Resources) else {
            return Step::Idle;
        };
        let Some(boundary) = install_boundary(self.keeper.db()).await else {
            return Step::Wait(RETRY_SECS);
        };
        let since = (!consented).then_some(boundary);
        let generation = self.keeper.generation();

        if self
            .walk
            .as_ref()
            .is_some_and(|walk| walk.generation != generation || walk.since != since)
        {
            self.walk = None;
            self.refused = None;
        }
        if self.walk.is_none() {
            let rested = self.finished.is_some_and(|(at_generation, at_since, at)| {
                at_generation == generation
                    && at_since == since
                    && at.elapsed() < Duration::from_secs(REWALK_SECS)
            });
            if rested {
                return Step::Idle;
            }
            self.walk = Some(Walk::new(generation, since, self.source.corpus_dbs().await));
        }
        let walk = self.walk.as_mut().expect("started above");

        let (targets, taken) = match walk.page(self.source.as_ref()).await {
            Ok(Some(page)) => page,
            Ok(None) => {
                self.keeper.set_resource_progress(generation, walk.tally);
                self.finished = Some((generation, since, Instant::now()));
                self.walk = None;
                return Step::Finished;
            }
            Err(error) => {
                log::warn!("resource embeddings: reading the corpus failed: {error}");
                return Step::Wait(RETRY_SECS);
            }
        };

        match settle_page(&self.keeper, generation, &key, targets).await {
            Ok(settled) => {
                walk.advance(taken);
                walk.tally.eligible += settled.eligible;
                walk.tally.embedded += settled.embedded;
                Step::Worked { sent: settled.sent }
            }
            Err(PageFailure::Gate) => Step::Wait(gate_wait(embedder.as_ref())),
            Err(PageFailure::Refused { uri, reason }) => {
                if self.refused.as_deref() != Some(uri.as_str()) {
                    log::warn!(
                        "resource embeddings: the call holding {uri} was refused: {reason}; retrying in {REJECTED_PAUSE_SECS}s"
                    );
                    self.refused = Some(uri);
                }
                Step::Wait(REJECTED_PAUSE_SECS)
            }
            Err(PageFailure::Storage(error)) => {
                log::warn!("resource embeddings: writing a page failed: {error}");
                Step::Wait(BACKLOG_PAUSE_SECS)
            }
        }
    }
}

/// Run the resource sweep forever on the current tokio runtime. Each loop
/// reconciles the space first, like the turn sweep, so a switch starts the
/// walk over within one step.
pub fn spawn_resource_sweep(
    keeper: Arc<SpaceKeeper>,
    consented: Arc<dyn Fn() -> bool + Send + Sync>,
    source: Arc<dyn ResourceSource>,
) {
    tokio::spawn(async move {
        let mut sweep = ResourceSweep::new(keeper.clone(), source);
        loop {
            match keeper.reconcile(Trigger::Sweep).await {
                Ok(Reconciled::Unresolved) => {
                    tokio::time::sleep(Duration::from_secs(IDLE_SECS)).await;
                    continue;
                }
                Ok(_) => {}
                Err(error) => {
                    log::warn!(
                        "resource embeddings: reconciling the embedding space failed: {error}"
                    );
                    tokio::time::sleep(Duration::from_secs(RETRY_SECS)).await;
                    continue;
                }
            }
            let wait = match sweep.step(consented()).await {
                Step::Worked { sent: 0 } => 0,
                Step::Worked { .. } => BACKLOG_PAUSE_SECS,
                Step::Finished | Step::Idle => IDLE_SECS,
                Step::Wait(seconds) => seconds,
            };
            if wait == 0 {
                tokio::task::yield_now().await;
            } else {
                tokio::time::sleep(Duration::from_secs(wait)).await;
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::embeddings::embedder::stub::StubEmbed;
    use crate::embeddings::space::tests::{count, keeper};

    struct Fixed {
        dbs: Vec<Arc<LocalDb>>,
        skills: Vec<ResourceTarget>,
    }

    #[async_trait]
    impl ResourceSource for Fixed {
        async fn corpus_dbs(&self) -> Vec<Arc<LocalDb>> {
            self.dbs.clone()
        }
        fn skills(&self) -> Vec<ResourceTarget> {
            self.skills.clone()
        }
    }

    const HOME: &str = "cairn://p/prj/42/1/builder";

    /// A project with an addressable node, three described issues, a
    /// confirmed memory, a plan in two versions and an empty return.
    async fn corpus(name: &str) -> Arc<LocalDb> {
        let db = Arc::new(crate::storage::migrated_test_db(name).await);
        db.execute_script(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('project-1', 'default', 'Project', 'prj', '/tmp/prj', 1, 1);
             INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
               VALUES ('issue-main', 'project-1', 42, 'Main', 'active', 1, 1);
             INSERT INTO issues (id, project_id, number, title, status, description, created_at, updated_at)
               VALUES ('i1', 'project-1', 1, 'One', 'active', 'one', 100, 100),
                      ('i2', 'project-1', 2, 'Two', 'active', 'two', 200, 200),
                      ('i3', 'project-1', 3, 'Three', 'active', 'three', 300, 300);
             INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
               VALUES ('exec-main', 'recipe', 'issue-main', 'project-1', 'running', 1, 1);
             INSERT INTO jobs (id, execution_id, issue_id, project_id, status, node_name, uri_segment, created_at, updated_at)
               VALUES ('job-main', 'exec-main', 'issue-main', 'project-1', 'running', 'builder', 'builder', 1, 1);
             INSERT INTO artifacts (id, job_id, artifact_type, data, version, parent_version_id, output_name, created_at, updated_at)
               VALUES ('a1', 'job-main', 'plan', '{\"title\":\"old\"}', 1, NULL, 'plan', 150, 150),
                      ('a2', 'job-main', 'plan', '{\"title\":\"new\"}', 2, 'a1', 'plan', 250, 250),
                      ('a3', 'job-main', 'return', '{\"content\":\"\"}', 1, NULL, 'return', 260, 260);",
        )
        .await
        .unwrap();
        crate::memories::db::create_memory(
            &db,
            "m1",
            None,
            "remember",
            Some("project-1"),
            "project",
            "project-1",
            Some("job-main"),
            Some(1),
            None,
        )
        .await
        .unwrap();
        db.execute(
            "UPDATE memories SET status = 'pending', created_at = 220 WHERE id = 'm1'",
            (),
        )
        .await
        .unwrap();
        db
    }

    fn sweep_over(db: Arc<LocalDb>, stub: Arc<StubEmbed>) -> (ResourceSweep, Arc<SpaceKeeper>) {
        let (keeper, _worker) = keeper(db.clone(), stub);
        let keeper = Arc::new(keeper);
        let source = Arc::new(Fixed {
            dbs: vec![db],
            skills: vec![skill_embed_target("s1", "skill desc")],
        });
        (ResourceSweep::new(keeper.clone(), source), keeper)
    }

    /// Step until the walk finishes, returning every step.
    async fn walk(sweep: &mut ResourceSweep, consented: bool) -> Vec<Step> {
        let mut steps = Vec::new();
        for _ in 0..50 {
            let step = sweep.step(consented).await;
            steps.push(step);
            if matches!(step, Step::Finished | Step::Idle | Step::Wait(_)) {
                return steps;
            }
        }
        panic!("the walk never finished: {steps:?}");
    }

    async fn stored(db: &LocalDb) -> Vec<(String, String, i64)> {
        db.query_all(
            "SELECT uri, model, dims FROM resource_embeddings ORDER BY uri",
            (),
            |row| Ok((row.text(0)?, row.text(1)?, row.i64(2)?)),
        )
        .await
        .unwrap()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_walk_embeds_each_kind_newest_first_as_its_producer_would() {
        let db = corpus("resources-walk.db").await;
        let stub = Arc::new(StubEmbed::new("m", 4));
        let (mut sweep, keeper) = sweep_over(db.clone(), stub.clone());
        keeper.reconcile(Trigger::Startup).await.unwrap();

        let steps = walk(&mut sweep, true).await;
        assert_eq!(steps.last(), Some(&Step::Finished), "{steps:?}");
        let sent: Vec<Vec<String>> = stub.calls().into_iter().map(|call| call.texts).collect();
        assert_eq!(
            sent,
            [
                vec!["three", "two", "one"],
                vec!["remember"],
                vec!["new"],
                vec!["skill desc"],
            ]
            .map(|texts| texts.into_iter().map(str::to_string).collect::<Vec<_>>()),
            "newest first within a kind; only the plan's latest version"
        );
        assert!(stub
            .calls()
            .iter()
            .all(|call| (call.corpus, call.purpose) == (Corpus::Resources, Purpose::Document)));

        // Each URI is the one its producer's builder derives.
        let memory = crate::memories::db::load_memory(&db, "m1").await.unwrap();
        let memory_target = memory_embed_target(&db, &memory).await.unwrap();
        assert_eq!(memory_target.uri, format!("{HOME}/memories/1"));
        assert_eq!(
            artifact_embed_uri(&db, "job-main", Some("plan"), "plan")
                .await
                .unwrap()
                .as_deref(),
            Some(format!("{HOME}/plan").as_str())
        );
        assert_eq!(
            artifact_text("plan", &serde_json::json!({"title": "new"})),
            "new"
        );
        // In URI order, as `stored` reads them.
        let expected = vec![
            ("cairn://p/prj/1".to_string(), "m".to_string(), 4),
            ("cairn://p/prj/2".to_string(), "m".to_string(), 4),
            ("cairn://p/prj/3".to_string(), "m".to_string(), 4),
            (format!("{HOME}/memories/1"), "m".to_string(), 4),
            (format!("{HOME}/plan"), "m".to_string(), 4),
            (format!("{HOME}/return"), "m".to_string(), 0),
            ("cairn://skills/s1".to_string(), "m".to_string(), 4),
        ];
        assert_eq!(stored(&db).await, expected);
        assert_eq!(
            keeper.resource_progress(),
            Some(ResourceProgress {
                embedded: 6,
                eligible: 6
            }),
            "the empty return is not eligible"
        );

        // A finished walk rests; a fresh one finds nothing to send, and the
        // empty artifact's marker is not retried.
        assert_eq!(sweep.step(true).await, Step::Idle);
        let (mut again, again_keeper) = sweep_over(db.clone(), stub.clone());
        assert_eq!(walk(&mut again, true).await.last(), Some(&Step::Finished));
        assert_eq!(stub.calls().len(), 4);
        assert_eq!(stored(&db).await, expected);
        assert_eq!(
            again_keeper
                .resource_progress()
                .map(|p| (p.embedded, p.eligible)),
            Some((6, 6))
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_failed_call_keeps_the_calls_before_it_and_resumes_where_it_stopped() {
        let db = corpus("resources-fail.db").await;
        for (id, number) in [("big-1", 11), ("big-2", 12), ("big-3", 13), ("big-4", 14)] {
            db.execute(
                "INSERT INTO issues (id, project_id, number, title, status, description, created_at, updated_at)
                 VALUES (?1, 'project-1', ?2, 'Big', 'active', ?3, ?4, ?4)",
                params![id, number, "x".repeat(20_000), 1000 + number],
            )
            .await
            .unwrap();
        }
        let stub = Arc::new(StubEmbed::new("m", 4));
        // The first page's calls: three bounded texts, then the fourth.
        stub.script(vec![None, Some(EmbedFailure::Failed("HTTP 500".into()))]);
        let (mut sweep, keeper) = sweep_over(db.clone(), stub.clone());
        keeper.reconcile(Trigger::Startup).await.unwrap();

        assert!(matches!(sweep.step(true).await, Step::Wait(_)));
        assert_eq!(
            count(
                &db,
                "SELECT COUNT(*) FROM resource_embeddings WHERE dims > 0"
            )
            .await,
            3,
            "the first call's vectors are kept"
        );
        let sent_before = stub.calls().len();
        assert_eq!(sweep.step(true).await, Step::Worked { sent: 4 });
        let resumed = &stub.calls()[sent_before..];
        assert_eq!(
            resumed
                .iter()
                .map(|call| call.texts.len())
                .collect::<Vec<_>>(),
            [4],
            "only what had no vector is sent again: the last big issue and the three small ones"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn without_consent_only_this_installs_own_region_is_walked() {
        let db = corpus("resources-consent.db").await;
        db.execute(
            "UPDATE embedding_state SET install_boundary = 200 WHERE id = 1",
            (),
        )
        .await
        .unwrap();
        let stub = Arc::new(StubEmbed::new("m", 4));
        let (mut sweep, keeper) = sweep_over(db.clone(), stub.clone());
        keeper.reconcile(Trigger::Startup).await.unwrap();
        assert_eq!(walk(&mut sweep, false).await.last(), Some(&Step::Finished));
        let sent: Vec<String> = stub
            .calls()
            .into_iter()
            .flat_map(|call| call.texts)
            .collect();
        assert_eq!(
            sent,
            ["three", "two", "remember", "new"],
            "no pre-boundary issue, no skill"
        );
        assert_eq!(
            keeper.resource_progress().map(|p| (p.embedded, p.eligible)),
            Some((4, 4))
        );

        // Consent widens the scope at once, without waiting out the rest.
        assert_eq!(walk(&mut sweep, true).await.last(), Some(&Step::Finished));
        // The narrow walk made one call per kind: issues, memories, artifacts.
        let widened: Vec<String> = stub.calls()[3..]
            .iter()
            .flat_map(|call| call.texts.clone())
            .collect();
        assert_eq!(widened, ["one", "skill desc"]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_switch_starts_the_walk_over_in_the_new_space() {
        let db = corpus("resources-switch.db").await;
        let stub = Arc::new(StubEmbed::new("m1", 4));
        let (mut sweep, keeper) = sweep_over(db.clone(), stub.clone());
        keeper.reconcile(Trigger::Startup).await.unwrap();
        assert_eq!(walk(&mut sweep, true).await.last(), Some(&Step::Finished));

        stub.set_space("cairn", "m2", 4);
        assert_eq!(
            keeper.reconcile(Trigger::SettingsChange).await.unwrap(),
            Reconciled::Switched
        );
        assert_eq!(keeper.resource_progress(), None);
        assert_eq!(walk(&mut sweep, true).await.last(), Some(&Step::Finished));
        assert_eq!(
            count(
                &db,
                "SELECT COUNT(*) FROM resource_embeddings WHERE dims > 0 AND model = 'm2'"
            )
            .await,
            6
        );
        assert_eq!(
            count(
                &db,
                "SELECT COUNT(*) FROM resource_embeddings WHERE model = 'm1' AND dims > 0"
            )
            .await,
            0
        );
        assert_eq!(
            keeper.resource_progress().map(|p| (p.embedded, p.eligible)),
            Some((6, 6))
        );
    }

    #[test]
    fn calls_are_bounded_by_count_and_characters() {
        let target = |chars: usize| ResourceTarget {
            uri: String::new(),
            text: "x".repeat(chars),
        };
        let big: Vec<ResourceTarget> = (0..4).map(|_| target(20_000)).collect();
        assert_eq!(
            chunks(&big).iter().map(|c| c.len()).collect::<Vec<_>>(),
            [3, 1],
            "each text counts at most the budget the door cuts it to"
        );
        let many: Vec<ResourceTarget> = (0..CHUNK + 1).map(|_| target(1)).collect();
        assert_eq!(
            chunks(&many).iter().map(|c| c.len()).collect::<Vec<_>>(),
            [CHUNK, 1]
        );
    }
}
