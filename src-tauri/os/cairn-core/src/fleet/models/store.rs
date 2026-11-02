//! Placement's model records in the orch's database: the requests
//! (`inference_requests`), the pins (`model_pins`) and each pool's
//! preferences (`model_pool_preferences`). The residents themselves are not
//! stored: they are rebuilt from the act side's phases, which are.

use std::collections::BTreeMap;

use cairn_db::turso::params;
use cairn_inference::DeploymentId;

use super::requests::{Request, RequestId, RequestState};
use super::rules::{Pin, Preference};
use super::settings::Ms;
use crate::fleet::pools::PoolKey;
use crate::storage::{LocalDb, RowExt};

/// How long an answered request is kept as demand history and on the page.
pub const ANSWERED_KEPT_MS: Ms = 7 * 24 * 3_600_000;

fn stored_state(state: &str, at: Option<Ms>, reason: Option<String>) -> RequestState {
    let at = at.unwrap_or(0);
    match state {
        "ready" => RequestState::Ready { at },
        "failed" => RequestState::Failed {
            at,
            reason: reason.unwrap_or_default(),
        },
        "cancelled" => RequestState::Cancelled { at },
        _ => RequestState::Waiting,
    }
}

fn state_columns(state: &RequestState) -> (&'static str, Option<Ms>, Option<String>) {
    match state {
        RequestState::Waiting => ("waiting", None, None),
        RequestState::Ready { at } => ("ready", Some(*at), None),
        RequestState::Failed { at, reason } => ("failed", Some(*at), Some(reason.clone())),
        RequestState::Cancelled { at } => ("cancelled", Some(*at), None),
    }
}

/// Every stored request: the waiting ones and the last week's answered ones.
pub async fn load_requests(db: &LocalDb) -> Result<Vec<Request>, String> {
    db.query_all(
        "SELECT id, pool, deployment, model, requester, created_at, state, state_at, reason
           FROM inference_requests ORDER BY id",
        (),
        |row| {
            Ok(Request {
                id: row.i64(0)?,
                pool: PoolKey(row.text(1)?),
                deployment: DeploymentId::from(row.text(2)?),
                model: row.text(3)?,
                requester: row.text(4)?,
                at: row.i64(5)?,
                state: stored_state(&row.text(6)?, row.opt_i64(7)?, row.opt_text(8)?),
            })
        },
    )
    .await
    .map_err(|error| format!("read inference_requests: {error}"))
}

pub async fn insert_request(
    db: &LocalDb,
    pool: &PoolKey,
    deployment: &DeploymentId,
    model: &str,
    requester: &str,
    at: Ms,
) -> Result<RequestId, String> {
    let row = (
        pool.to_string(),
        deployment.to_string(),
        model.to_string(),
        requester.to_string(),
    );
    db.write(move |conn| {
        let (pool, deployment, model, requester) = row.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "INSERT INTO inference_requests (pool, deployment, model, requester, created_at, state)
                     VALUES (?1, ?2, ?3, ?4, ?5, 'waiting') RETURNING id",
                    params![pool, deployment, model, requester, at],
                )
                .await?;
            let row = rows.next().await?.ok_or_else(|| {
                crate::storage::DbError::Row("INSERT ... RETURNING id returned no row".into())
            })?;
            row.i64(0)
        })
    })
    .await
    .map_err(|error| format!("write inference_requests: {error}"))
}

/// Record that `settled` requests stopped waiting. A request already answered
/// (cancelled meanwhile) keeps its answer.
pub async fn settle(db: &LocalDb, settled: &[(RequestId, RequestState)]) -> Result<(), String> {
    for (id, state) in settled {
        let (state, at, reason) = state_columns(state);
        db.execute(
            "UPDATE inference_requests SET state = ?2, state_at = ?3, reason = ?4
              WHERE id = ?1 AND state = 'waiting'",
            params![*id, state, at, reason],
        )
        .await
        .map_err(|error| format!("write inference_requests: {error}"))?;
    }
    Ok(())
}

/// Cancel a waiting request.
pub async fn cancel_request(
    db: &LocalDb,
    id: RequestId,
    by: &str,
    now: Ms,
) -> Result<String, String> {
    let changed = db
        .execute(
            "UPDATE inference_requests SET state = 'cancelled', state_at = ?2, reason = ?3
              WHERE id = ?1 AND state = 'waiting'",
            params![id, now, format!("cancelled by {by}")],
        )
        .await
        .map_err(|error| format!("write inference_requests: {error}"))?;
    if changed == 0 {
        let state = db
            .query_opt_text(
                "SELECT state FROM inference_requests WHERE id = ?1",
                params![id],
            )
            .await
            .map_err(|error| format!("read inference_requests: {error}"))?;
        return Err(match state {
            Some(state) => format!("request #{id} is {state}, not waiting"),
            None => format!("there is no request #{id}"),
        });
    }
    Ok(format!("Cancelled request #{id}"))
}

pub async fn prune_answered(db: &LocalDb, now: Ms) -> Result<(), String> {
    db.execute(
        "DELETE FROM inference_requests WHERE state != 'waiting' AND state_at < ?1",
        params![now - ANSWERED_KEPT_MS],
    )
    .await
    .map(|_| ())
    .map_err(|error| format!("prune inference_requests: {error}"))
}

/// A pin as stored: the model's real name beside the lease.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredPin {
    pub model: String,
    pub pin: Pin,
}

pub async fn load_pins(db: &LocalDb) -> Result<BTreeMap<DeploymentId, StoredPin>, String> {
    let rows = db
        .query_all(
            "SELECT deployment_id, model, holder, at FROM model_pins ORDER BY deployment_id",
            (),
            |row| Ok((row.text(0)?, row.text(1)?, row.text(2)?, row.i64(3)?)),
        )
        .await
        .map_err(|error| format!("read model_pins: {error}"))?;
    Ok(rows
        .into_iter()
        .map(|(id, model, holder, at)| {
            (
                DeploymentId::from(id),
                StoredPin {
                    model,
                    pin: Pin { holder, at },
                },
            )
        })
        .collect())
}

pub async fn save_pin(
    db: &LocalDb,
    deployment: &DeploymentId,
    pinned: &StoredPin,
) -> Result<(), String> {
    db.execute(
        "INSERT INTO model_pins (deployment_id, model, holder, at) VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(deployment_id) DO UPDATE SET model = excluded.model,
           holder = excluded.holder, at = excluded.at",
        params![
            deployment.as_str(),
            pinned.model.as_str(),
            pinned.pin.holder.as_str(),
            pinned.pin.at
        ],
    )
    .await
    .map(|_| ())
    .map_err(|error| format!("write model_pins: {error}"))
}

pub async fn delete_pin(db: &LocalDb, deployment: &DeploymentId) -> Result<(), String> {
    db.execute(
        "DELETE FROM model_pins WHERE deployment_id = ?1",
        params![deployment.as_str()],
    )
    .await
    .map(|_| ())
    .map_err(|error| format!("write model_pins: {error}"))
}

pub async fn load_preferences(db: &LocalDb) -> Result<BTreeMap<PoolKey, Preference>, String> {
    let rows = db
        .query_all(
            "SELECT pool_key, warm_model, idle_override_secs FROM model_pool_preferences
              ORDER BY pool_key",
            (),
            |row| Ok((row.text(0)?, row.opt_text(1)?, row.opt_i64(2)?)),
        )
        .await
        .map_err(|error| format!("read model_pool_preferences: {error}"))?;
    Ok(rows
        .into_iter()
        .map(|(pool, warm_model, idle)| {
            (
                PoolKey(pool),
                Preference {
                    warm_model,
                    idle_override_secs: idle.and_then(|secs| u64::try_from(secs).ok()),
                },
            )
        })
        .collect())
}

pub async fn save_preference(
    db: &LocalDb,
    pool: &PoolKey,
    preference: &Preference,
    at: Ms,
) -> Result<(), String> {
    if preference == &Preference::default() {
        return db
            .execute(
                "DELETE FROM model_pool_preferences WHERE pool_key = ?1",
                params![pool.as_str()],
            )
            .await
            .map(|_| ())
            .map_err(|error| format!("write model_pool_preferences: {error}"));
    }
    let idle = preference
        .idle_override_secs
        .map(|secs| i64::try_from(secs).unwrap_or(i64::MAX));
    db.execute(
        "INSERT INTO model_pool_preferences (pool_key, warm_model, idle_override_secs, updated_at)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(pool_key) DO UPDATE SET warm_model = excluded.warm_model,
           idle_override_secs = excluded.idle_override_secs, updated_at = excluded.updated_at",
        params![pool.as_str(), preference.warm_model.clone(), idle, at],
    )
    .await
    .map(|_| ())
    .map_err(|error| format!("write model_pool_preferences: {error}"))
}
