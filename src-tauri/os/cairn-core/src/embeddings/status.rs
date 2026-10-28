//! The one answer to "where is embedding at?", for the Settings page, the
//! `cairn://models/embeddings` resource and agents alike.
//!
//! It reads three things that already exist rather than keeping a status of
//! its own: the selection and consent (the setting and the account), the
//! health gate (`super::embedder`), and how much of each corpus holds a
//! vector of the active space, counted within the consent scope. The turn
//! count is SQL over an index; the resource count is the resource sweep's
//! last walk (`super::resources`), because artifact and memory URIs are
//! derived and cannot be counted in SQL.

use std::time::{SystemTime, UNIX_EPOCH};

use super::embedder::{Corpus, EmbedHealth, EmbedSpace};
use super::space::{ResourceProgress, SpaceKeeper};
use super::turns::TurnProgress;

/// Where embedding is.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum EmbeddingState {
    /// History is still being embedded into the active space.
    Running,
    /// The provider failed; nothing is sent until `retry_at`.
    #[serde(rename_all = "camelCase")]
    Paused {
        since: Option<i64>,
        last_error: Option<String>,
        retry_at: i64,
    },
    /// Every eligible turn and resource holds a vector of the active space.
    Complete,
    /// The selection cannot be embedded with on this device.
    Unavailable { reason: String },
}

/// The health gate as a status read shows it. Times are epoch milliseconds.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthStatus {
    pub configured: bool,
    pub unavailable_since: Option<i64>,
    pub last_error: Option<String>,
    pub retry_at: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EmbeddingStatus {
    /// The model, as `provider/model`.
    pub selection: String,
    /// Chosen by the operator, rather than the default an unset setting means.
    pub explicit: bool,
    /// Whether history that predates this install is in scope.
    pub consented: bool,
    /// The space the selection embeds into, when it resolves.
    pub space: Option<EmbedSpace>,
    pub health: HealthStatus,
    pub state: EmbeddingState,
    /// `None` when the selection does not resolve.
    pub turns: Option<TurnProgress>,
    /// `None` until the resource sweep has walked the corpus in this space.
    pub resources: Option<ResourceProgress>,
}

fn millis(at: SystemTime) -> i64 {
    at.duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_millis() as i64)
}

/// What the counts and the gate add up to.
fn state_of(
    space: Option<&EmbedSpace>,
    health: &EmbedHealth,
    turns: Option<&TurnProgress>,
    resources: Option<&ResourceProgress>,
    now: SystemTime,
    unavailable: impl FnOnce() -> String,
) -> EmbeddingState {
    if space.is_none() || !health.configured {
        return EmbeddingState::Unavailable {
            reason: unavailable(),
        };
    }
    if let Some(retry_at) = health.retry_at.filter(|at| *at > now) {
        return EmbeddingState::Paused {
            since: health.unavailable_since.map(millis),
            last_error: health.last_error.clone(),
            retry_at: millis(retry_at),
        };
    }
    let turns_short = turns.is_none_or(|turns| turns.embedded < turns.eligible);
    let resources_short = resources.is_none_or(|resources| resources.embedded < resources.eligible);
    if turns_short || resources_short {
        EmbeddingState::Running
    } else {
        EmbeddingState::Complete
    }
}

/// The status of `keeper`'s stores for the selection `selection`.
/// `unavailable` says why, when the selection cannot be embedded with.
pub async fn embedding_status(
    keeper: &SpaceKeeper,
    selection: &str,
    explicit: bool,
    consented: bool,
    unavailable: impl FnOnce(&EmbedHealth) -> String,
) -> EmbeddingStatus {
    let embedder = keeper.embedder();
    let space = embedder.space();
    let health = embedder.health();
    let turns = match space.as_ref() {
        Some(space) => {
            // Without consent only this install's own region counts; before
            // the sweep has established where that begins, nothing does.
            let since = if consented {
                None
            } else {
                Some(
                    super::turns::recorded_boundary(keeper.db())
                        .await
                        .unwrap_or(i64::MAX),
                )
            };
            match keeper.turn_progress(&space.key(Corpus::Turns), since).await {
                Ok(progress) => Some(progress),
                Err(error) => {
                    log::debug!("embedding status: counting turns failed: {error}");
                    None
                }
            }
        }
        None => None,
    };
    let resources = space.as_ref().and(keeper.resource_progress());
    let state = state_of(
        space.as_ref(),
        &health,
        turns.as_ref(),
        resources.as_ref(),
        SystemTime::now(),
        || unavailable(&health),
    );
    EmbeddingStatus {
        selection: selection.to_string(),
        explicit,
        consented,
        space,
        health: HealthStatus {
            configured: health.configured,
            unavailable_since: health.unavailable_since.map(millis),
            last_error: health.last_error.clone(),
            retry_at: health.retry_at.map(millis),
        },
        state,
        turns,
        resources,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use crate::embeddings::embedder::stub::StubEmbed;
    use crate::embeddings::space::tests::keeper;
    use crate::embeddings::Trigger;

    fn space() -> EmbedSpace {
        EmbedSpace {
            provider: "p".into(),
            model: "m".into(),
            turn_dims: Some(4),
            resource_dims: Some(4),
        }
    }

    fn configured() -> EmbedHealth {
        EmbedHealth {
            configured: true,
            ..EmbedHealth::default()
        }
    }

    fn progress(embedded: u64, eligible: u64) -> (TurnProgress, ResourceProgress) {
        (
            TurnProgress {
                embedded,
                eligible,
                deferred: 0,
            },
            ResourceProgress { embedded, eligible },
        )
    }

    #[test]
    fn the_state_follows_the_gate_then_the_counts() {
        let now = SystemTime::now();
        let reason = || "no token".to_string();
        assert_eq!(
            state_of(None, &configured(), None, None, now, reason),
            EmbeddingState::Unavailable {
                reason: "no token".into()
            }
        );
        assert!(matches!(
            state_of(
                Some(&space()),
                &EmbedHealth::default(),
                None,
                None,
                now,
                reason
            ),
            EmbeddingState::Unavailable { .. }
        ));

        let paused = EmbedHealth {
            unavailable_since: Some(now - Duration::from_secs(60)),
            last_error: Some("HTTP 503".into()),
            retry_at: Some(now + Duration::from_secs(30)),
            ..configured()
        };
        let (turns, resources) = progress(1, 2);
        assert_eq!(
            state_of(
                Some(&space()),
                &paused,
                Some(&turns),
                Some(&resources),
                now,
                reason
            ),
            EmbeddingState::Paused {
                since: paused.unavailable_since.map(millis),
                last_error: Some("HTTP 503".into()),
                retry_at: millis(now + Duration::from_secs(30)),
            }
        );

        let running = |turns: TurnProgress, resources: Option<ResourceProgress>| {
            state_of(
                Some(&space()),
                &configured(),
                Some(&turns),
                resources.as_ref(),
                now,
                reason,
            )
        };
        let (short, done) = (progress(1, 2).0, progress(2, 2));
        assert_eq!(running(short, Some(done.1)), EmbeddingState::Running);
        assert_eq!(
            running(done.0, None),
            EmbeddingState::Running,
            "resources not yet counted"
        );
        assert_eq!(
            running(done.0, Some(progress(0, 3).1)),
            EmbeddingState::Running
        );
        assert_eq!(running(done.0, Some(done.1)), EmbeddingState::Complete);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn an_unresolved_selection_reads_unavailable_with_its_reason() {
        let db = Arc::new(crate::storage::migrated_test_db("status-unresolved.db").await);
        let stub = Arc::new(StubEmbed::new("m", 4));
        stub.set_unresolved();
        let (keeper, _worker) = keeper(db, stub);
        let status = embedding_status(&keeper, "x/y", true, true, |_| "x is offline".into()).await;
        assert_eq!(
            status.state,
            EmbeddingState::Unavailable {
                reason: "x is offline".into()
            }
        );
        assert_eq!(
            (status.space, status.turns, status.resources),
            (None, None, None)
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_fresh_switch_reads_running_with_nothing_embedded() {
        let db = Arc::new(crate::storage::migrated_test_db("status-switch.db").await);
        let stub = Arc::new(StubEmbed::new("m", 4));
        let (keeper, _worker) = keeper(db, stub);
        keeper.reconcile(Trigger::Startup).await.unwrap();
        let status = embedding_status(&keeper, "p/m", true, true, |_| String::new()).await;
        assert_eq!(status.state, EmbeddingState::Running);
        assert_eq!(status.turns.map(|turns| turns.embedded), Some(0));
        assert_eq!(status.resources, None);
        let json = serde_json::to_value(&status).unwrap();
        assert_eq!(json["state"]["kind"], "running");
        assert_eq!(json["space"]["turnDims"], 4);
    }
}
