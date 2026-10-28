//! The one vector space every stored vector shares, and keeping it that way
//! when the embedding model changes.
//!
//! Vectors from two models are not comparable, and neither are vectors of one
//! model at two widths, so the stores (`turn_embeddings`, `resource_embeddings`,
//! `session_positions`) hold one space at a time. `embedding_state` records
//! which: the provider and model of the selected offer, and the width each
//! corpus is stored at.
//!
//! [`SpaceKeeper::reconcile`] compares that record with the space the selected
//! offer embeds into now ([`Embed::space`]). When they differ it deletes every
//! vector of another model or width in one transaction, records the new space,
//! resets the health gate (the old provider's outage says nothing about the
//! new one), and tells the embed worker to drop its in-memory positions. The
//! sweeps then re-embed history into the new space; until they do, search and
//! recall simply have fewer vectors to answer from.
//!
//! Three things are deliberately left alone:
//!
//! - Rows with `dims <= 0` are turn tombstones, deferral markers and empty
//!   resource markers. They record what the TEXT held, which no model changes.
//! - Vibe colours (`event_vibes`) stay: the vectors behind them are long gone,
//!   and the colours are still right for what they described.
//! - A selection that does not resolve (the provider is offline, a token is
//!   missing) purges nothing and falls back to nothing. That is an outage or
//!   an unconfigured install, not a switch.
//!
//! Every purge logs one warning on [`SPACE_TARGET`], naming both spaces, what
//! triggered it and how many vectors went, because a mistaken purge costs a
//! full re-embed and must never be silent.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use cairn_db::turso::params;
use tokio::sync::mpsc::UnboundedSender;

use super::embedder::{Embed, EmbedSpace, VectorKey};
use super::turns::TurnProgress;
use super::EmbedJob;
use crate::storage::{DbResult, LocalDb, RowExt};

/// The log target a purge is reported on.
pub const SPACE_TARGET: &str = "embeddings::space";

/// What asked for a reconcile, for the purge's log line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Trigger {
    Startup,
    SettingsChange,
    Sweep,
}

impl fmt::Display for Trigger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Startup => "startup",
            Self::SettingsChange => "settings change",
            Self::Sweep => "sweep",
        })
    }
}

/// What a reconcile found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reconciled {
    /// The recorded space is the active one.
    Unchanged,
    /// Nothing was recorded (a fresh install); the active space now is.
    Recorded,
    /// The active space differs from the recorded one: the old one's vectors
    /// are gone and history re-embeds.
    Switched,
    /// The selection resolves to no offer. Nothing was touched.
    Unresolved,
}

/// How far the resource corpus is through the active space: of the
/// resources in scope that carry text, how many hold a vector.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceProgress {
    pub embedded: u64,
    pub eligible: u64,
}

impl fmt::Display for EmbedSpace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let width = |dims: Option<u32>| dims.map_or("?".to_string(), |dims| dims.to_string());
        write!(
            f,
            "{}/{} (turns {}, resources {})",
            self.provider,
            self.model,
            width(self.turn_dims),
            width(self.resource_dims)
        )
    }
}

/// Whether `recorded` is the space `active` embeds into. A width is compared
/// only when both sides know it: an offer that does not state its native
/// width reports `None`, and that says nothing about a change.
fn same_space(recorded: &EmbedSpace, active: &EmbedSpace) -> bool {
    let width = |a: Option<u32>, b: Option<u32>| a.zip(b).is_none_or(|(a, b)| a == b);
    recorded.provider == active.provider
        && recorded.model == active.model
        && width(recorded.turn_dims, active.turn_dims)
        && width(recorded.resource_dims, active.resource_dims)
}

/// The space `embedding_state` records, when one is.
pub async fn recorded_space(db: &LocalDb) -> DbResult<Option<EmbedSpace>> {
    let row = db
        .query_opt(
            "SELECT provider, model, turn_dims, resource_dims FROM embedding_state WHERE id = 1",
            (),
            |row| {
                Ok((
                    row.opt_text(0)?,
                    row.opt_text(1)?,
                    row.opt_i64(2)?,
                    row.opt_i64(3)?,
                ))
            },
        )
        .await?;
    Ok(match row {
        Some((Some(provider), Some(model), turn_dims, resource_dims)) => Some(EmbedSpace {
            provider,
            model,
            turn_dims: turn_dims.map(|dims| dims as u32),
            resource_dims: resource_dims.map(|dims| dims as u32),
        }),
        _ => None,
    })
}

/// Vectors a purge removed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Purged {
    turns: u64,
    resources: u64,
    positions: u64,
}

impl Purged {
    fn any(&self) -> bool {
        self.turns + self.resources + self.positions > 0
    }
}

/// Delete every vector outside `space` and record it, in one transaction.
async fn purge_and_record(db: &LocalDb, space: &EmbedSpace) -> DbResult<Purged> {
    let model = space.model.clone();
    let provider = space.provider.clone();
    let turn_dims = space.turn_dims.map(i64::from);
    let resource_dims = space.resource_dims.map(i64::from);
    db.write(move |conn| {
        let (model, provider) = (model.clone(), provider.clone());
        Box::pin(async move {
            let turns = conn
                .execute(
                    "DELETE FROM turn_embeddings
                      WHERE dims > 0 AND (model != ?1 OR (?2 IS NOT NULL AND dims != ?2))",
                    params![model.as_str(), turn_dims],
                )
                .await?;
            let resources = conn
                .execute(
                    "DELETE FROM resource_embeddings
                      WHERE dims > 0 AND (model != ?1 OR (?2 IS NOT NULL AND dims != ?2))",
                    params![model.as_str(), resource_dims],
                )
                .await?;
            // Positions are folded from resource-corpus event vectors.
            let positions = conn
                .execute(
                    "DELETE FROM session_positions
                      WHERE model != ?1 OR (?2 IS NOT NULL AND dims != ?2)",
                    params![model.as_str(), resource_dims],
                )
                .await?;
            conn.execute(
                "UPDATE embedding_state
                    SET provider = ?1, model = ?2, turn_dims = ?3, resource_dims = ?4
                  WHERE id = 1",
                params![provider.as_str(), model.as_str(), turn_dims, resource_dims],
            )
            .await?;
            Ok(Purged {
                turns,
                resources,
                positions,
            })
        })
    })
    .await
}

/// What every [`SpaceKeeper`] over one store shares. The orchestrator owns
/// one; keepers are built around it on demand, because a keeper holds an
/// embedder that holds the orchestrator.
#[derive(Debug, Default)]
pub struct SpaceState {
    /// One reconcile at a time, so two triggers cannot both purge.
    lock: tokio::sync::Mutex<()>,
    /// Bumped by every switch. A sweep holding an older one is walking a
    /// space that no longer exists and starts over.
    generation: AtomicU64,
    /// The resource sweep's last count, `None` until a walk in this space
    /// has finished. Kept here because artifact and memory URIs are derived,
    /// so the count cannot be taken in SQL.
    resources: Mutex<Option<ResourceProgress>>,
    /// The last turn count, reused for [`TURN_COUNT_TTL`]: the Settings page
    /// polls every few seconds and the count reads every turn ever recorded.
    turns: Mutex<Option<CachedTurns>>,
}

/// How long a turn count is reused.
const TURN_COUNT_TTL: std::time::Duration = std::time::Duration::from_secs(5);

#[derive(Debug, Clone)]
struct CachedTurns {
    generation: u64,
    key: VectorKey,
    since: Option<i64>,
    at: std::time::Instant,
    progress: TurnProgress,
}

/// Keeps the stores in the selected offer's space, for everything that
/// reconciles and for the status read.
pub struct SpaceKeeper {
    db: Arc<LocalDb>,
    embedder: Arc<dyn Embed>,
    /// The embed worker's sender, told to drop its positions on a switch.
    worker: Arc<Mutex<Option<UnboundedSender<EmbedJob>>>>,
    state: Arc<SpaceState>,
}

impl SpaceKeeper {
    /// A keeper with state of its own.
    pub fn new(
        db: Arc<LocalDb>,
        embedder: Arc<dyn Embed>,
        worker: Arc<Mutex<Option<UnboundedSender<EmbedJob>>>>,
    ) -> Self {
        Self::with_state(db, embedder, worker, Arc::default())
    }

    pub fn with_state(
        db: Arc<LocalDb>,
        embedder: Arc<dyn Embed>,
        worker: Arc<Mutex<Option<UnboundedSender<EmbedJob>>>>,
        state: Arc<SpaceState>,
    ) -> Self {
        Self {
            db,
            embedder,
            worker,
            state,
        }
    }

    pub fn embedder(&self) -> &Arc<dyn Embed> {
        &self.embedder
    }

    pub fn db(&self) -> &Arc<LocalDb> {
        &self.db
    }

    /// Which space the stores are in, as a counter: it moves on every switch.
    pub fn generation(&self) -> u64 {
        self.state.generation.load(Ordering::SeqCst)
    }

    /// Bring the stores into the selected offer's space. See the module docs.
    pub async fn reconcile(&self, trigger: Trigger) -> DbResult<Reconciled> {
        let _one_at_a_time = self.state.lock.lock().await;
        let Some(active) = self.embedder.space() else {
            return Ok(Reconciled::Unresolved);
        };
        let recorded = recorded_space(&self.db).await?;
        if let Some(recorded) = &recorded {
            if same_space(recorded, &active) {
                // A width learned since it was recorded is filled in; that
                // is not a change of space.
                let learned = (recorded.turn_dims.is_none() && active.turn_dims.is_some())
                    || (recorded.resource_dims.is_none() && active.resource_dims.is_some());
                if learned {
                    self.db
                        .execute(
                            "UPDATE embedding_state
                                SET turn_dims = COALESCE(turn_dims, ?1),
                                    resource_dims = COALESCE(resource_dims, ?2)
                              WHERE id = 1",
                            params![
                                active.turn_dims.map(i64::from),
                                active.resource_dims.map(i64::from)
                            ],
                        )
                        .await?;
                }
                return Ok(Reconciled::Unchanged);
            }
        }

        let purged = purge_and_record(&self.db, &active).await?;
        self.state.generation.fetch_add(1, Ordering::SeqCst);
        *self
            .state
            .resources
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = None;
        self.embedder.reset_health();
        if let Ok(worker) = self.worker.lock() {
            if let Some(worker) = worker.as_ref() {
                let _ = worker.send(EmbedJob::ResetSpace);
            }
        }

        match &recorded {
            Some(old) => {
                log::warn!(
                    target: SPACE_TARGET,
                    "embeddings: switched from {old} to {active} on {trigger}: deleted {} turn, {} resource and {} position vectors; history re-embeds",
                    purged.turns,
                    purged.resources,
                    purged.positions
                );
                Ok(Reconciled::Switched)
            }
            None if purged.any() => {
                log::warn!(
                    target: SPACE_TARGET,
                    "embeddings: recorded {active} on {trigger}: deleted {} turn, {} resource and {} position vectors from another space",
                    purged.turns,
                    purged.resources,
                    purged.positions
                );
                Ok(Reconciled::Recorded)
            }
            None => {
                log::info!(target: SPACE_TARGET, "embeddings: recorded {active} on {trigger}");
                Ok(Reconciled::Recorded)
            }
        }
    }

    /// The turn corpus's progress through `key`'s space since `since`,
    /// counted at most every [`TURN_COUNT_TTL`] within one space.
    pub async fn turn_progress(
        &self,
        key: &VectorKey,
        since: Option<i64>,
    ) -> DbResult<TurnProgress> {
        let generation = self.generation();
        let cached = self
            .state
            .turns
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if let Some(cached) = cached.filter(|cached| {
            cached.generation == generation
                && cached.key == *key
                && cached.since == since
                && cached.at.elapsed() < TURN_COUNT_TTL
        }) {
            return Ok(cached.progress);
        }
        let progress = super::turns::turn_progress(&self.db, key, since).await?;
        *self.state.turns.lock().unwrap_or_else(|e| e.into_inner()) = Some(CachedTurns {
            generation,
            key: key.clone(),
            since,
            at: std::time::Instant::now(),
            progress,
        });
        Ok(progress)
    }

    /// The resource corpus's progress through the active space, once a
    /// walk has counted it.
    pub fn resource_progress(&self) -> Option<ResourceProgress> {
        *self
            .state
            .resources
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }

    /// Record a finished walk's count, unless the space changed under it.
    pub(crate) fn set_resource_progress(&self, generation: u64, progress: ResourceProgress) {
        if self.generation() == generation {
            *self
                .state
                .resources
                .lock()
                .unwrap_or_else(|e| e.into_inner()) = Some(progress);
        }
    }

    /// Count `embedded` more resources as done, unless the space changed.
    pub(crate) fn note_resources_embedded(&self, generation: u64, embedded: u64) {
        if self.generation() != generation {
            return;
        }
        if let Some(progress) = self
            .state
            .resources
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_mut()
        {
            progress.embedded = (progress.embedded + embedded).min(progress.eligible);
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::embeddings::embedder::stub::StubEmbed;
    use crate::embeddings::vector;
    use crate::test_support::{capture_log, captured_log};

    pub(crate) async fn count(db: &LocalDb, sql: &str) -> i64 {
        db.query_one(sql, (), |row| row.i64(0)).await.unwrap()
    }

    /// Seed one row of every kind in `model`'s space at `dims`, plus the
    /// model-independent markers.
    pub(crate) async fn seed_space(db: &LocalDb, tag: &str, model: &str, dims: u32) {
        let blob = vector::to_bytes(&vec![1.0; dims as usize]);
        for (suffix, dims, embedding) in [
            ("vec", dims as i64, blob.clone()),
            ("tomb", 0, Vec::new()),
            ("defer", -1, Vec::new()),
        ] {
            db.execute(
                "INSERT INTO turn_embeddings(turn_id, session_id, sequence, embedding, model,
                                             dims, created_at, updated_at)
                 VALUES (?1, 's', 1, ?2, ?3, ?4, 1, 1)",
                params![format!("{tag}-{suffix}"), embedding, model, dims],
            )
            .await
            .unwrap();
        }
        for (uri, dims, embedding) in [
            (format!("cairn://p/k/{tag}"), dims as i64, blob.clone()),
            (format!("cairn://p/k/{tag}-empty"), 0, Vec::new()),
        ] {
            db.execute(
                "INSERT INTO resource_embeddings(uri, embedding, model, dims, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, 1, 1)",
                params![uri, embedding, model, dims],
            )
            .await
            .unwrap();
        }
        db.execute(
            "INSERT INTO session_positions(session_id, embedding, model, dims, updated_at)
             VALUES (?1, ?2, ?3, ?4, 1)",
            params![format!("session-{tag}"), blob, model, dims as i64],
        )
        .await
        .unwrap();
    }

    pub(crate) fn keeper(
        db: Arc<LocalDb>,
        embedder: Arc<StubEmbed>,
    ) -> (SpaceKeeper, tokio::sync::mpsc::UnboundedReceiver<EmbedJob>) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        (
            SpaceKeeper::new(db, embedder, Arc::new(Mutex::new(Some(tx)))),
            rx,
        )
    }

    #[tokio::test(flavor = "current_thread")]
    async fn the_first_reconcile_on_a_fresh_install_records_the_space() {
        let db = Arc::new(crate::storage::migrated_test_db("space-fresh.db").await);
        let stub = Arc::new(StubEmbed::new("m1", 8));
        let (keeper, mut worker) = keeper(db.clone(), stub);
        assert_eq!(
            keeper.reconcile(Trigger::Startup).await.unwrap(),
            Reconciled::Recorded
        );
        assert_eq!(
            recorded_space(&db).await.unwrap(),
            Some(EmbedSpace {
                provider: "cairn".into(),
                model: "m1".into(),
                turn_dims: Some(8),
                resource_dims: Some(8),
            })
        );
        assert_eq!(worker.try_recv(), Ok(EmbedJob::ResetSpace));
        assert_eq!(
            keeper.reconcile(Trigger::Sweep).await.unwrap(),
            Reconciled::Unchanged
        );
        assert!(
            worker.try_recv().is_err(),
            "an unchanged space resets nothing"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_switch_at_the_same_width_purges_the_old_model_and_keeps_markers() {
        capture_log(SPACE_TARGET);
        let db = Arc::new(crate::storage::migrated_test_db("space-switch.db").await);
        let stub = Arc::new(StubEmbed::new("m1", 1536));
        stub.set_space("a", "m1", 1536);
        let (keeper, mut worker) = keeper(db.clone(), stub.clone());
        keeper.reconcile(Trigger::Startup).await.unwrap();
        let _ = worker.try_recv();
        seed_space(&db, "old", "m1", 1536).await;
        keeper.set_resource_progress(
            keeper.generation(),
            ResourceProgress {
                embedded: 1,
                eligible: 1,
            },
        );
        stub.set_paused(true);
        let generation = keeper.generation();

        stub.set_space("b", "m2", 1536);
        assert_eq!(
            keeper.reconcile(Trigger::SettingsChange).await.unwrap(),
            Reconciled::Switched
        );

        assert_eq!(
            count(&db, "SELECT COUNT(*) FROM turn_embeddings WHERE dims > 0").await,
            0
        );
        assert_eq!(
            count(&db, "SELECT COUNT(*) FROM turn_embeddings WHERE dims <= 0").await,
            2,
            "tombstones and deferral markers are model-independent"
        );
        assert_eq!(
            count(&db, "SELECT COUNT(*) FROM resource_embeddings").await,
            1,
            "only the empty-text marker survives"
        );
        assert_eq!(
            count(&db, "SELECT COUNT(*) FROM session_positions").await,
            0
        );
        assert_eq!(
            recorded_space(&db).await.unwrap().unwrap().model,
            "m2".to_string()
        );
        assert!(stub.health().retry_at.is_none(), "the gate is reset");
        assert_eq!(worker.try_recv(), Ok(EmbedJob::ResetSpace));
        assert_eq!(keeper.resource_progress(), None);
        assert!(keeper.generation() > generation);

        let logged: Vec<String> = captured_log(SPACE_TARGET)
            .into_iter()
            .filter(|line| line.contains("b/m2"))
            .collect();
        assert_eq!(logged.len(), 1, "{logged:?}");
        assert!(logged[0].contains("switched from a/m1 (turns 1536, resources 1536)"));
        assert!(logged[0].contains("on settings change"));
        assert!(logged[0].contains("deleted 1 turn, 1 resource and 1 position vectors"));

        // A count from the walk that was under way is discarded.
        keeper.set_resource_progress(generation, ResourceProgress::default());
        assert_eq!(keeper.resource_progress(), None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn an_unresolved_selection_touches_nothing() {
        let db = Arc::new(crate::storage::migrated_test_db("space-unresolved.db").await);
        let stub = Arc::new(StubEmbed::new("m1", 1536));
        let (keeper, mut worker) = keeper(db.clone(), stub.clone());
        keeper.reconcile(Trigger::Startup).await.unwrap();
        let _ = worker.try_recv();
        seed_space(&db, "kept", "m1", 1536).await;

        stub.set_unresolved();
        assert_eq!(
            keeper.reconcile(Trigger::Sweep).await.unwrap(),
            Reconciled::Unresolved
        );
        assert_eq!(count(&db, "SELECT COUNT(*) FROM turn_embeddings").await, 3);
        assert_eq!(
            count(&db, "SELECT COUNT(*) FROM resource_embeddings").await,
            2
        );
        assert_eq!(
            count(&db, "SELECT COUNT(*) FROM session_positions").await,
            1
        );
        assert_eq!(recorded_space(&db).await.unwrap().unwrap().model, "m1");
        assert!(worker.try_recv().is_err());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_width_change_alone_is_a_switch() {
        let db = Arc::new(crate::storage::migrated_test_db("space-width.db").await);
        let stub = Arc::new(StubEmbed::new("m1", 1536));
        let (keeper, _worker) = keeper(db.clone(), stub.clone());
        keeper.reconcile(Trigger::Startup).await.unwrap();
        seed_space(&db, "wide", "m1", 1536).await;
        stub.set_space("cairn", "m1", 256);
        assert_eq!(
            keeper.reconcile(Trigger::Sweep).await.unwrap(),
            Reconciled::Switched
        );
        assert_eq!(
            count(&db, "SELECT COUNT(*) FROM turn_embeddings WHERE dims > 0").await,
            0
        );
    }

    #[test]
    fn an_unknown_width_is_never_a_difference() {
        let known = EmbedSpace {
            provider: "p".into(),
            model: "m".into(),
            turn_dims: Some(1536),
            resource_dims: Some(1536),
        };
        let unknown = EmbedSpace {
            turn_dims: None,
            resource_dims: None,
            ..known.clone()
        };
        assert!(same_space(&known, &unknown));
        assert!(same_space(&unknown, &known));
        assert!(!same_space(
            &known,
            &EmbedSpace {
                provider: "q".into(),
                ..known.clone()
            }
        ));
    }
}
