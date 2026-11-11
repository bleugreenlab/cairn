//! Runner-local claims on remote check attempts. Open claims are debts, not
//! leases: pruning never expires them, even when their executor stays offline.

use super::{DbError, DbResult, LocalDb, RowExt};
use cairn_db::turso::{params, Row};

const COLUMNS: &str = "request_id,attempt_id,project_id,check_names,platform,commit_sha,input_hashes,consumer,consumer_ref,executor_id,machine,granted_at_ms,state,stopped_at_ms,evidence,observation_id,updated_ms,recording_plans";
const RETENTION_MS: i64 = 7 * 24 * 60 * 60 * 1000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Claim {
    pub request_id: String,
    pub attempt_id: String,
    pub project_id: String,
    pub check_names: Vec<String>,
    pub platform: String,
    pub commit_sha: String,
    pub input_hashes: Vec<String>,
    pub recording_plans: Vec<serde_json::Value>,
    pub consumer: String,
    pub consumer_ref: String,
    pub executor_id: String,
    pub machine: String,
    pub granted_at_ms: i64,
    pub state: String,
    pub stopped_at_ms: Option<i64>,
    pub evidence: Option<String>,
    pub observation_id: Option<String>,
    pub updated_ms: i64,
}

fn from_row(row: &Row) -> DbResult<Claim> {
    Ok(Claim {
        request_id: row.text(0)?,
        attempt_id: row.text(1)?,
        project_id: row.text(2)?,
        check_names: decode_list(row.text(3)?, "check_names")?,
        platform: row.text(4)?,
        commit_sha: row.text(5)?,
        input_hashes: decode_list(row.text(6)?, "input_hashes")?,
        consumer: row.text(7)?,
        consumer_ref: row.text(8)?,
        executor_id: row.text(9)?,
        machine: row.text(10)?,
        granted_at_ms: row.i64(11)?,
        state: row.text(12)?,
        stopped_at_ms: row.opt_i64(13)?,
        evidence: row.opt_text(14)?,
        observation_id: row.opt_text(15)?,
        updated_ms: row.i64(16)?,
        recording_plans: serde_json::from_str(&row.text(17)?).map_err(|error| {
            DbError::internal(format!("invalid adoption recording plans: {error}"))
        })?,
    })
}

fn decode_list(json: String, column: &str) -> DbResult<Vec<String>> {
    serde_json::from_str(&json).map_err(|error| {
        DbError::internal(format!("invalid check lane adoption {column}: {error}"))
    })
}

#[cfg(test)]
pub(crate) async fn insert(db: &LocalDb, claim: &Claim) -> DbResult<()> {
    insert_grant(db, claim, &[]).await
}

/// The grant and all initial consumers become durable in one commit.
pub(crate) async fn insert_grant(db: &LocalDb, claim: &Claim, waiters: &[Waiter]) -> DbResult<()> {
    let claim = claim.clone();
    let waiters = waiters.to_vec();
    db.write(move |conn| {
        let claim = claim.clone();
        let waiters = waiters.clone();
        Box::pin(async move {
            if claim.check_names.len() != claim.input_hashes.len()
                || (!claim.recording_plans.is_empty() && claim.recording_plans.len() != claim.check_names.len())
            {
                return Err(DbError::internal("check lane adoption item arrays must have equal lengths"));
            }
            let check_names = encode(&claim.check_names)?;
            let input_hashes = encode(&claim.input_hashes)?;
            let recording_plans = encode(&claim.recording_plans)?;
            conn.execute(
                format!("INSERT INTO check_lane_adoptions ({COLUMNS}) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18)"),
                params![claim.request_id.clone(),claim.attempt_id.clone(),claim.project_id,
                    check_names,claim.platform,claim.commit_sha,input_hashes,claim.consumer,
                    claim.consumer_ref,claim.executor_id,claim.machine,claim.granted_at_ms,
                    claim.state,claim.stopped_at_ms,claim.evidence,claim.observation_id,
                    claim.updated_ms,recording_plans],
            ).await?;
            for waiter in waiters {
                if waiter.request_id != claim.request_id || waiter.attempt_id != claim.attempt_id {
                    return Err(DbError::internal("waiter does not belong to this grant"));
                }
                insert_waiter_on(conn, &waiter).await?;
            }
            Ok(())
        })
    }).await
}

fn encode(value: &impl serde::Serialize) -> DbResult<String> {
    serde_json::to_string(value)
        .map_err(|error| DbError::internal(format!("encode check adoption metadata: {error}")))
}

/// All unresolved attempts, including grants whose runner died abruptly.
pub(crate) async fn open(db: &LocalDb) -> DbResult<Vec<Claim>> {
    db.query_all(
        format!("SELECT {COLUMNS} FROM check_lane_adoptions WHERE state IN ('running','awaiting_adoption','joined') ORDER BY granted_at_ms,request_id,attempt_id"),
        (),
        from_row,
    ).await
}

/// Terminal claims remain discoverable so a resent result is not misclassified
/// as an unowned request.
pub(crate) async fn lookup(
    db: &LocalDb,
    request_id: &str,
    attempt_id: &str,
) -> DbResult<Option<Claim>> {
    db.query_opt(
        format!("SELECT {COLUMNS} FROM check_lane_adoptions WHERE request_id=?1 AND attempt_id=?2"),
        (request_id.to_owned(), attempt_id.to_owned()),
        from_row,
    )
    .await
}

/// Settled observations are immutable. Cancellation can only be refined to
/// lost when a stray retained result arrives; that cannot attach an observation.
/// Open states may recur across multiple runner restarts. Missing optional
/// metadata preserves what an earlier transition already knew.
pub(crate) async fn transition(
    db: &LocalDb,
    request_id: &str,
    attempt_id: &str,
    state: &str,
    evidence: Option<&str>,
    observation_id: Option<&str>,
    stopped_at_ms: Option<i64>,
) -> DbResult<()> {
    if !matches!(
        state,
        "running" | "awaiting_adoption" | "joined" | "recorded" | "lost" | "cancelled"
    ) {
        return Err(DbError::internal(format!(
            "unknown check lane adoption state: {state}"
        )));
    }
    db.execute(
        "UPDATE check_lane_adoptions SET state=?3,evidence=COALESCE(?4,evidence),observation_id=CASE WHEN state='cancelled' THEN observation_id ELSE COALESCE(?5,observation_id) END,stopped_at_ms=CASE WHEN state='cancelled' THEN stopped_at_ms ELSE COALESCE(?6,stopped_at_ms) END,updated_ms=MAX(updated_ms,?7) WHERE request_id=?1 AND attempt_id=?2 AND (state IN ('running','awaiting_adoption','joined') OR (state='cancelled' AND ?3='lost'))",
        params![request_id.to_owned(), attempt_id.to_owned(), state.to_owned(),
            evidence.map(str::to_owned), observation_id.map(str::to_owned),
            stopped_at_ms, chrono::Utc::now().timestamp_millis()],
    ).await?;
    Ok(())
}

/// Housekeeping only: age never settles an outstanding adoption debt.
pub(crate) async fn prune(db: &LocalDb) -> DbResult<()> {
    prune_before(db, chrono::Utc::now().timestamp_millis() - RETENTION_MS).await
}

async fn prune_before(db: &LocalDb, cutoff_ms: i64) -> DbResult<()> {
    // Boot loads claims asynchronously. Empty housekeeping must not open a
    // write transaction beside unrelated ingress or other read-only startup.
    let eligible = db.query_opt(
        "SELECT 1 FROM check_lane_adoptions WHERE state IN ('recorded','lost','cancelled') AND updated_ms < ?1 AND NOT EXISTS (SELECT 1 FROM check_lane_adoption_waiters w WHERE w.request_id=check_lane_adoptions.request_id AND w.attempt_id=check_lane_adoptions.attempt_id AND (w.state IN ('running','awaiting_adoption','joined','recorded') OR w.updated_ms >= ?1)) LIMIT 1",
        (cutoff_ms,),
        |row| row.i64(0),
    ).await?;
    if eligible.is_none() {
        return Ok(());
    }
    db.execute(
        "DELETE FROM check_lane_adoptions WHERE state IN ('recorded','lost','cancelled') AND updated_ms < ?1 AND NOT EXISTS (SELECT 1 FROM check_lane_adoption_waiters w WHERE w.request_id=check_lane_adoptions.request_id AND w.attempt_id=check_lane_adoptions.attempt_id AND (w.state IN ('running','awaiting_adoption','joined','recorded') OR w.updated_ms >= ?1))",
        (cutoff_ms,),
    ).await?;
    Ok(())
}

const WAITER_COLUMNS: &str = "request_id,attempt_id,check_name,item_index,consumer,consumer_ref,state,observation_id,evidence,updated_ms";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Waiter {
    pub request_id: String,
    pub attempt_id: String,
    pub check_name: String,
    pub item_index: usize,
    pub consumer: String,
    pub consumer_ref: String,
    pub state: String,
    pub observation_id: Option<String>,
    pub evidence: Option<String>,
    pub updated_ms: i64,
}

impl Waiter {
    /// A withdrawn consumer keeps its observation, but no longer owns adoption.
    pub(crate) fn is_cadence_adopter(&self) -> bool {
        self.consumer == "main_cadence"
            && matches!(
                self.state.as_str(),
                "running" | "awaiting_adoption" | "joined" | "recorded" | "delivered"
            )
    }
}

fn waiter_from_row(row: &Row) -> DbResult<Waiter> {
    Ok(Waiter {
        request_id: row.text(0)?,
        attempt_id: row.text(1)?,
        check_name: row.text(2)?,
        item_index: usize::try_from(row.i64(3)?)
            .map_err(|error| DbError::internal(format!("invalid adoption item index: {error}")))?,
        consumer: row.text(4)?,
        consumer_ref: row.text(5)?,
        state: row.text(6)?,
        observation_id: row.opt_text(7)?,
        evidence: row.opt_text(8)?,
        updated_ms: row.i64(9)?,
    })
}

/// Idempotent registration never reopens an existing consumer.
pub(crate) async fn insert_waiter(db: &LocalDb, waiter: &Waiter) -> DbResult<()> {
    let waiter = waiter.clone();
    db.write(move |conn| {
        let waiter = waiter.clone();
        Box::pin(async move { insert_waiter_on(conn, &waiter).await })
    })
    .await
}

async fn insert_waiter_on(conn: &super::TrackedConnection, waiter: &Waiter) -> DbResult<()> {
    let mut rows = conn
        .query(
            "SELECT check_names FROM check_lane_adoptions WHERE request_id=?1 AND attempt_id=?2",
            params![waiter.request_id.clone(), waiter.attempt_id.clone()],
        )
        .await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| DbError::internal("adoption waiter has no physical grant"))?;
    let names = decode_list(row.text(0)?, "check_names")?;
    if names.get(waiter.item_index) != Some(&waiter.check_name) {
        return Err(DbError::internal(
            "adoption waiter suite does not match its physical item index",
        ));
    }
    drop(rows);
    let index = i64::try_from(waiter.item_index)
        .map_err(|error| DbError::internal(format!("invalid adoption item index: {error}")))?;
    conn.execute(
        format!("INSERT INTO check_lane_adoption_waiters ({WAITER_COLUMNS}) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10) ON CONFLICT(request_id,attempt_id,check_name,consumer,consumer_ref) DO NOTHING"),
        params![waiter.request_id.clone(),waiter.attempt_id.clone(),waiter.check_name.clone(),
            index,waiter.consumer.clone(),waiter.consumer_ref.clone(),waiter.state.clone(),
            waiter.observation_id.clone(),waiter.evidence.clone(),waiter.updated_ms],
    ).await?;
    Ok(())
}

pub(crate) async fn waiters(
    db: &LocalDb,
    request_id: &str,
    attempt_id: &str,
) -> DbResult<Vec<Waiter>> {
    db.query_all(
        format!("SELECT {WAITER_COLUMNS} FROM check_lane_adoption_waiters WHERE request_id=?1 AND attempt_id=?2 ORDER BY item_index,consumer,consumer_ref"),
        (request_id.to_owned(),attempt_id.to_owned()),waiter_from_row,
    ).await
}

/// Recorded cadence consumers are still open until their report is delivered.
#[cfg(test)]
pub(crate) async fn open_waiters(db: &LocalDb) -> DbResult<Vec<Waiter>> {
    db.query_all(
        format!("SELECT {WAITER_COLUMNS} FROM check_lane_adoption_waiters WHERE state IN ('running','awaiting_adoption','joined','recorded') ORDER BY request_id,attempt_id,item_index,consumer,consumer_ref"),
        (),waiter_from_row,
    ).await
}

pub(crate) async fn transition_waiter(
    db: &LocalDb,
    waiter: &Waiter,
    state: &str,
    evidence: Option<&str>,
    observation_id: Option<&str>,
) -> DbResult<()> {
    if !matches!(
        state,
        "running"
            | "awaiting_adoption"
            | "joined"
            | "recorded"
            | "recorded_undelivered"
            | "delivered"
            | "lost"
            | "cancelled"
    ) {
        return Err(DbError::internal(format!(
            "unknown check adoption waiter state: {state}"
        )));
    }
    db.execute(
        "UPDATE check_lane_adoption_waiters SET state=?6,evidence=CASE WHEN state='recorded' THEN COALESCE(evidence,?7) ELSE COALESCE(?7,evidence) END,observation_id=CASE WHEN state IN ('cancelled','recorded') THEN observation_id ELSE COALESCE(?8,observation_id) END,updated_ms=MAX(updated_ms,?9) WHERE request_id=?1 AND attempt_id=?2 AND check_name=?3 AND consumer=?4 AND consumer_ref=?5 AND (state IN ('running','awaiting_adoption','joined') OR (state='cancelled' AND ?6='lost') OR (state='recorded' AND ?6 IN ('delivered','recorded_undelivered')))",
        params![waiter.request_id.clone(),waiter.attempt_id.clone(),waiter.check_name.clone(),
            waiter.consumer.clone(),waiter.consumer_ref.clone(),state.to_owned(),
            evidence.map(str::to_owned),observation_id.map(str::to_owned),
            chrono::Utc::now().timestamp_millis()],
    ).await?;
    Ok(())
}

/// Preserve the physical item's recording evidence even when its consumer
/// withdrew first. This does not reopen or deliver the cancelled consumer.
pub(crate) async fn record_cancelled_waiter_observation(
    db: &LocalDb,
    waiter: &Waiter,
    observation_id: &str,
) -> DbResult<()> {
    db.execute(
        "UPDATE check_lane_adoption_waiters SET observation_id=?6,updated_ms=MAX(updated_ms,?7)
         WHERE request_id=?1 AND attempt_id=?2 AND check_name=?3 AND consumer=?4 AND consumer_ref=?5
           AND state='cancelled' AND observation_id IS NULL",
        params![
            waiter.request_id.clone(),
            waiter.attempt_id.clone(),
            waiter.check_name.clone(),
            waiter.consumer.clone(),
            waiter.consumer_ref.clone(),
            observation_id.to_owned(),
            chrono::Utc::now().timestamp_millis()
        ],
    )
    .await?;
    let accepted = db
        .query_opt(
            "SELECT observation_id FROM check_lane_adoption_waiters
         WHERE request_id=?1 AND attempt_id=?2 AND check_name=?3 AND consumer=?4 AND consumer_ref=?5
           AND state='cancelled'",
            params![
                waiter.request_id.clone(),
                waiter.attempt_id.clone(),
                waiter.check_name.clone(),
                waiter.consumer.clone(),
                waiter.consumer_ref.clone()
            ],
            |row| row.opt_text(0),
        )
        .await?;
    if accepted.flatten().as_deref() != Some(observation_id) {
        return Err(DbError::internal(
            "cancelled check consumer has no matching immutable observation",
        ));
    }
    Ok(())
}

/// Commit the failure decision for the physical grant and every consumer before
/// the caller cancels remote work or acknowledges its retained result.
pub(crate) async fn settle_failure(
    db: &LocalDb,
    request_id: &str,
    attempt_id: &str,
    state: &str,
    evidence: &str,
) -> DbResult<()> {
    if !matches!(state, "lost" | "cancelled") {
        return Err(DbError::internal(
            "check adoption failure must be lost or cancelled",
        ));
    }
    let request_id = request_id.to_owned();
    let attempt_id = attempt_id.to_owned();
    let state = state.to_owned();
    let evidence = evidence.to_owned();
    let now = chrono::Utc::now().timestamp_millis();
    db.write(move |conn| {
        let request_id = request_id.clone();
        let attempt_id = attempt_id.clone();
        let state = state.clone();
        let evidence = evidence.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE check_lane_adoption_waiters SET state=CASE WHEN state='recorded' THEN 'recorded_undelivered' ELSE ?3 END,evidence=CASE WHEN state='recorded' THEN COALESCE(evidence,?4) ELSE ?4 END,updated_ms=MAX(updated_ms,?5) WHERE request_id=?1 AND attempt_id=?2 AND (state IN ('running','awaiting_adoption','joined','recorded') OR (state='cancelled' AND ?3='lost'))",
                params![request_id.clone(),attempt_id.clone(),state.clone(),evidence.clone(),now],
            ).await?;
            conn.execute(
                "UPDATE check_lane_adoptions SET state=?3,evidence=?4,updated_ms=MAX(updated_ms,?5) WHERE request_id=?1 AND attempt_id=?2 AND (state IN ('running','awaiting_adoption','joined') OR (state='cancelled' AND ?3='lost'))",
                params![request_id,attempt_id,state,evidence,now],
            ).await?;
            Ok(())
        })
    }).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn waiter(claim: &Claim, index: usize, consumer: &str, consumer_ref: &str) -> Waiter {
        Waiter {
            request_id: claim.request_id.clone(),
            attempt_id: claim.attempt_id.clone(),
            check_name: claim.check_names[index].clone(),
            item_index: index,
            consumer: consumer.into(),
            consumer_ref: consumer_ref.into(),
            state: "running".into(),
            observation_id: None,
            evidence: None,
            updated_ms: claim.updated_ms,
        }
    }

    #[tokio::test]
    async fn a_cancelled_consumer_retains_physical_recording_evidence_without_reopening() {
        let db = crate::storage::migrated_test_db("cancelled-physical-receipt").await;
        let physical = claim("cancelled-receipt", "running", 30);
        let consumer = waiter(&physical, 0, "manual", "handle");
        insert_grant(&db, &physical, std::slice::from_ref(&consumer))
            .await
            .unwrap();
        transition_waiter(
            &db,
            &consumer,
            "cancelled",
            Some("requester withdrew"),
            None,
        )
        .await
        .unwrap();
        record_cancelled_waiter_observation(&db, &consumer, "physical-source")
            .await
            .unwrap();
        record_cancelled_waiter_observation(&db, &consumer, "physical-source")
            .await
            .unwrap();
        assert!(
            record_cancelled_waiter_observation(&db, &consumer, "different-source")
                .await
                .is_err()
        );
        for state in ["running", "recorded", "delivered"] {
            transition_waiter(
                &db,
                &consumer,
                state,
                Some("must not replace"),
                Some("different-source"),
            )
            .await
            .unwrap();
        }
        let saved = waiters(&db, &physical.request_id, &physical.attempt_id)
            .await
            .unwrap();
        assert_eq!(saved[0].state, "cancelled");
        assert_eq!(saved[0].evidence.as_deref(), Some("requester withdrew"));
        assert_eq!(saved[0].observation_id.as_deref(), Some("physical-source"));
        assert!(open_waiters(&db).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn normalized_waiters_and_recording_plans_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("shared-grant.db");
        let mut physical = claim("shared", "running", 30);
        physical.consumer = "manual".into();
        physical.check_names = vec!["first".into(), "second".into(), "third".into()];
        physical.input_hashes = vec!["hash1".into(), "hash2".into(), "hash3".into()];
        physical.recording_plans = (0..3)
            .map(|index| serde_json::json!({"item":index,"selection":"partial"}))
            .collect();
        let manual: Vec<_> = (0..3)
            .map(|index| waiter(&physical, index, "manual", "handle"))
            .collect();
        let cadence = waiter(&physical, 1, "main_cadence", "purpose-second");
        {
            let db = LocalDb::open_migrated_for_test(&path).await.unwrap();
            insert_grant(&db, &physical, &manual).await.unwrap();
            insert_waiter(&db, &cadence).await.unwrap();
            insert_waiter(&db, &cadence).await.unwrap();
        }
        let db = LocalDb::open(&path).await.unwrap();
        assert_eq!(
            lookup(&db, "request", "shared").await.unwrap(),
            Some(physical)
        );
        assert_eq!(waiters(&db, "request", "shared").await.unwrap().len(), 4);
        assert_eq!(open_waiters(&db).await.unwrap().len(), 4);
        for (index, consumer) in manual.iter().enumerate() {
            transition_waiter(
                &db,
                consumer,
                "recorded_undelivered",
                Some("no manual adopter"),
                Some(&format!("observation-{index}")),
            )
            .await
            .unwrap();
        }
        transition_waiter(&db, &cadence, "recorded", None, Some("observation-1"))
            .await
            .unwrap();
        assert_eq!(
            open_waiters(&db).await.unwrap().len(),
            1,
            "recorded cadence still owes delivery"
        );
        transition_waiter(&db, &cadence, "delivered", None, Some("must-not-replace"))
            .await
            .unwrap();
        assert!(open_waiters(&db).await.unwrap().is_empty());
        let settled = waiters(&db, "request", "shared").await.unwrap();
        assert_eq!(
            settled
                .iter()
                .filter(|w| w.state == "recorded_undelivered")
                .count(),
            3
        );
        let delivered = settled
            .iter()
            .find(|w| w.consumer == "main_cadence")
            .unwrap();
        assert_eq!(delivered.state, "delivered");
        assert_eq!(delivered.observation_id.as_deref(), Some("observation-1"));
        for consumer in &settled {
            transition_waiter(&db, consumer, "running", Some("late"), Some("other"))
                .await
                .unwrap();
        }
        assert_eq!(waiters(&db, "request", "shared").await.unwrap(), settled);
    }

    #[tokio::test]
    async fn failure_settlement_is_atomic_and_preserves_accepted_observations() {
        let db = crate::storage::migrated_test_db("check-adoption-settle-failure").await;
        let physical = claim("failure", "running", 30);
        let unresolved = waiter(&physical, 0, "manual", "handle");
        let mut recorded = waiter(&physical, 0, "main_cadence", "purpose");
        recorded.state = "recorded".into();
        recorded.observation_id = Some("observation".into());
        recorded.evidence = Some("committed".into());
        insert_grant(&db, &physical, &[unresolved.clone(), recorded.clone()])
            .await
            .unwrap();
        assert!(
            settle_failure(&db, "request", "failure", "recorded", "invalid")
                .await
                .is_err()
        );
        let before = waiters(&db, "request", "failure").await.unwrap();
        db.execute_script("CREATE TRIGGER reject_adoption_failure BEFORE UPDATE ON check_lane_adoptions BEGIN SELECT RAISE(ABORT, 'injected physical settlement failure'); END;").await.unwrap();
        assert!(
            settle_failure(&db, "request", "failure", "cancelled", "runner stopped")
                .await
                .is_err()
        );
        assert_eq!(waiters(&db, "request", "failure").await.unwrap(), before);
        assert_eq!(
            lookup(&db, "request", "failure").await.unwrap(),
            Some(physical)
        );
        db.execute_script("DROP TRIGGER reject_adoption_failure;")
            .await
            .unwrap();
        settle_failure(&db, "request", "failure", "cancelled", "runner stopped")
            .await
            .unwrap();
        let cancelled = waiters(&db, "request", "failure").await.unwrap();
        let recorded = cancelled
            .iter()
            .find(|w| w.consumer == "main_cadence")
            .unwrap();
        assert_eq!(recorded.state, "recorded_undelivered");
        assert_eq!(recorded.observation_id.as_deref(), Some("observation"));
        assert_eq!(recorded.evidence.as_deref(), Some("committed"));
        let unresolved = cancelled.iter().find(|w| w.consumer == "manual").unwrap();
        assert_eq!(unresolved.state, "cancelled");
        assert_eq!(unresolved.evidence.as_deref(), Some("runner stopped"));
        assert!(open_waiters(&db).await.unwrap().is_empty());
        assert_eq!(
            lookup(&db, "request", "failure")
                .await
                .unwrap()
                .unwrap()
                .state,
            "cancelled"
        );
        settle_failure(&db, "request", "failure", "lost", "stray retained result")
            .await
            .unwrap();
        let lost = waiters(&db, "request", "failure").await.unwrap();
        assert_eq!(
            lost.iter().find(|w| w.consumer == "main_cadence").unwrap(),
            recorded
        );
        let unresolved = lost.iter().find(|w| w.consumer == "manual").unwrap();
        assert_eq!(unresolved.state, "lost");
        assert!(unresolved.observation_id.is_none());
        assert_eq!(
            lookup(&db, "request", "failure")
                .await
                .unwrap()
                .unwrap()
                .state,
            "lost"
        );
        settle_failure(&db, "request", "failure", "cancelled", "late")
            .await
            .unwrap();
        assert_eq!(waiters(&db, "request", "failure").await.unwrap(), lost);
    }

    #[tokio::test]
    async fn recorded_waiter_refinement_fills_missing_evidence_only() {
        let db = crate::storage::migrated_test_db("check-adoption-missing-evidence").await;
        for attempt in ["direct", "atomic"] {
            let physical = claim(attempt, "running", 30);
            let mut consumer = waiter(&physical, 0, "main_cadence", "purpose");
            consumer.state = "recorded".into();
            consumer.observation_id = Some("observation".into());
            insert_grant(&db, &physical, std::slice::from_ref(&consumer))
                .await
                .unwrap();
            if attempt == "direct" {
                transition_waiter(
                    &db,
                    &consumer,
                    "recorded_undelivered",
                    Some("sibling failed"),
                    Some("other"),
                )
                .await
                .unwrap();
            } else {
                settle_failure(&db, "request", attempt, "lost", "sibling failed")
                    .await
                    .unwrap();
            }
            let settled = waiters(&db, "request", attempt).await.unwrap();
            assert_eq!(settled[0].state, "recorded_undelivered");
            assert_eq!(settled[0].observation_id.as_deref(), Some("observation"));
            assert_eq!(settled[0].evidence.as_deref(), Some("sibling failed"));
        }
    }

    #[tokio::test]
    async fn recorded_waiter_can_settle_undelivered_without_overwriting_observation_or_evidence() {
        let db = crate::storage::migrated_test_db("check-adoption-undelivered").await;
        let physical = claim("undelivered", "running", 30);
        let consumer = waiter(&physical, 0, "main_cadence", "purpose");
        insert_grant(&db, &physical, std::slice::from_ref(&consumer))
            .await
            .unwrap();
        transition_waiter(
            &db,
            &consumer,
            "recorded",
            Some("committed"),
            Some("observation"),
        )
        .await
        .unwrap();
        assert_eq!(open_waiters(&db).await.unwrap().len(), 1);
        transition_waiter(
            &db,
            &consumer,
            "recorded_undelivered",
            Some("must not replace"),
            Some("other"),
        )
        .await
        .unwrap();
        let settled = waiters(&db, "request", "undelivered").await.unwrap();
        assert_eq!(settled[0].state, "recorded_undelivered");
        assert_eq!(settled[0].observation_id.as_deref(), Some("observation"));
        assert_eq!(settled[0].evidence.as_deref(), Some("committed"));
        assert!(open_waiters(&db).await.unwrap().is_empty());
        for state in ["running", "recorded", "delivered", "lost"] {
            transition_waiter(&db, &consumer, state, Some("late"), Some("late"))
                .await
                .unwrap();
        }
        assert_eq!(
            waiters(&db, "request", "undelivered").await.unwrap(),
            settled
        );
    }

    #[tokio::test]
    async fn invalid_waiter_rolls_back_entire_grant_and_partial_plans_are_rejected() {
        let db = crate::storage::migrated_test_db("check-adoption-atomic").await;
        let physical = claim("atomic", "running", 30);
        let good = waiter(&physical, 0, "manual", "handle");
        let mut bad = good.clone();
        bad.item_index = 1;
        assert!(insert_grant(&db, &physical, &[good.clone(), bad])
            .await
            .is_err());
        assert!(lookup(&db, "request", "atomic").await.unwrap().is_none());
        assert!(waiters(&db, "request", "atomic").await.unwrap().is_empty());
        assert!(insert_waiter(&db, &good).await.is_err());
        let mut partial = physical.clone();
        partial.recording_plans = vec![serde_json::json!({}), serde_json::json!({})];
        assert!(insert(&db, &partial).await.is_err());
    }

    #[tokio::test]
    async fn cancelled_refines_to_lost_without_attaching_observation() {
        let db = crate::storage::migrated_test_db("check-adoption-refinement").await;
        let physical = claim("cancelled", "cancelled", 30);
        let mut consumer = waiter(&physical, 0, "manual", "handle");
        consumer.state = "cancelled".into();
        insert_grant(&db, &physical, std::slice::from_ref(&consumer))
            .await
            .unwrap();
        transition(
            &db,
            "request",
            "cancelled",
            "lost",
            Some("stray retained result"),
            Some("forbidden"),
            Some(99),
        )
        .await
        .unwrap();
        transition_waiter(
            &db,
            &consumer,
            "lost",
            Some("stray retained result"),
            Some("forbidden"),
        )
        .await
        .unwrap();
        let lost = lookup(&db, "request", "cancelled").await.unwrap().unwrap();
        assert_eq!(lost.state, "lost");
        assert_eq!(lost.evidence.as_deref(), Some("stray retained result"));
        assert_eq!(lost.stopped_at_ms, physical.stopped_at_ms);
        assert!(lost.observation_id.is_none());
        let lost_waiters = waiters(&db, "request", "cancelled").await.unwrap();
        assert_eq!(lost_waiters[0].state, "lost");
        assert!(lost_waiters[0].observation_id.is_none());
        transition(
            &db,
            "request",
            "cancelled",
            "recorded",
            None,
            Some("late"),
            None,
        )
        .await
        .unwrap();
        transition_waiter(&db, &consumer, "recorded", None, Some("late"))
            .await
            .unwrap();
        assert_eq!(
            lookup(&db, "request", "cancelled").await.unwrap(),
            Some(lost)
        );
        assert_eq!(
            waiters(&db, "request", "cancelled").await.unwrap(),
            lost_waiters
        );
    }

    #[tokio::test]
    async fn pruning_never_discards_a_pending_consumer_delivery() {
        let db = crate::storage::migrated_test_db("check-adoption-waiter-prune").await;
        let physical = claim("pending", "recorded", 1);
        let mut consumer = waiter(&physical, 0, "main_cadence", "purpose");
        consumer.state = "recorded".into();
        consumer.observation_id = Some("observation".into());
        insert_grant(&db, &physical, std::slice::from_ref(&consumer))
            .await
            .unwrap();
        prune_before(&db, 100).await.unwrap();
        assert!(lookup(&db, "request", "pending").await.unwrap().is_some());
        assert_eq!(open_waiters(&db).await.unwrap(), vec![consumer.clone()]);
        transition_waiter(&db, &consumer, "delivered", None, None)
            .await
            .unwrap();
        prune_before(&db, i64::MAX).await.unwrap();
        assert!(lookup(&db, "request", "pending").await.unwrap().is_none());
        assert!(waiters(&db, "request", "pending").await.unwrap().is_empty());
    }

    fn claim(attempt: &str, state: &str, updated_ms: i64) -> Claim {
        Claim {
            request_id: "request".into(),
            attempt_id: attempt.into(),
            project_id: "project".into(),
            check_names: vec!["rust-tests".into()],
            platform: "linux".into(),
            commit_sha: "commit".into(),
            input_hashes: vec!["input".into()],
            recording_plans: vec![],
            consumer: "main_cadence".into(),
            consumer_ref: "purpose".into(),
            executor_id: "executor".into(),
            machine: "remote".into(),
            granted_at_ms: 10,
            state: state.into(),
            stopped_at_ms: Some(20),
            evidence: Some("runner stopped".into()),
            observation_id: None,
            updated_ms,
        }
    }

    #[tokio::test]
    async fn claims_survive_reopen_and_lookup_includes_terminal_attempts() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("adoptions.db");
        let mut running = claim("running", "running", 30);
        running.consumer = "turn_end".into();
        running.check_names = vec!["rust-tests".into(), "lint \"quoted\" λ".into()];
        running.input_hashes = vec!["input-rust".into(), "input-lint".into()];
        let mut recorded = claim("recorded", "recorded", 40);
        recorded.observation_id = Some("observation".into());
        {
            let db = LocalDb::open_migrated_for_test(&path).await.unwrap();
            insert(&db, &running).await.unwrap();
            insert(&db, &recorded).await.unwrap();
            assert!(
                insert(&db, &running).await.is_err(),
                "composite identity is unique"
            );
        }
        let db = LocalDb::open(&path).await.unwrap();
        assert_eq!(open(&db).await.unwrap(), vec![running.clone()]);
        for expected in [running, recorded] {
            assert_eq!(
                lookup(&db, &expected.request_id, &expected.attempt_id)
                    .await
                    .unwrap(),
                Some(expected)
            );
        }
        assert!(lookup(&db, "request", "missing").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn mismatched_or_malformed_check_arrays_are_rejected() {
        let db = crate::storage::migrated_test_db("check-adoption-arrays").await;
        let mut bad = claim("bad", "running", 30);
        bad.input_hashes.clear();
        assert!(insert(&db, &bad).await.is_err());
        assert!(lookup(&db, "request", "bad").await.unwrap().is_none());
        assert!(decode_list("not json".into(), "check_names").is_err());
        assert!(decode_list("[1]".into(), "input_hashes").is_err());
        assert!(decode_list("null".into(), "check_names").is_err());
    }

    #[tokio::test]
    async fn terminal_decision_and_metadata_cannot_be_overwritten_or_reopened() {
        let db = crate::storage::migrated_test_db("check-adoption-transitions").await;
        insert(&db, &claim("attempt", "running", 30)).await.unwrap();
        transition(
            &db,
            "request",
            "attempt",
            "awaiting_adoption",
            None,
            None,
            Some(50),
        )
        .await
        .unwrap();
        transition(&db, "request", "attempt", "joined", None, None, None)
            .await
            .unwrap();
        let joined = lookup(&db, "request", "attempt").await.unwrap().unwrap();
        assert_eq!(joined.stopped_at_ms, Some(50));
        assert_eq!(joined.evidence.as_deref(), Some("runner stopped"));
        assert_eq!(open(&db).await.unwrap(), vec![joined]);
        transition(
            &db,
            "request",
            "attempt",
            "recorded",
            Some("committed"),
            Some("observation"),
            None,
        )
        .await
        .unwrap();
        let terminal = lookup(&db, "request", "attempt").await.unwrap().unwrap();
        for state in [
            "running",
            "awaiting_adoption",
            "joined",
            "lost",
            "cancelled",
            "recorded",
        ] {
            transition(
                &db,
                "request",
                "attempt",
                state,
                Some("late update"),
                Some("other"),
                Some(99),
            )
            .await
            .unwrap();
            assert_eq!(
                lookup(&db, "request", "attempt").await.unwrap(),
                Some(terminal.clone())
            );
        }
        assert!(open(&db).await.unwrap().is_empty());
        assert!(
            transition(&db, "request", "attempt", "invalid", None, None, None)
                .await
                .is_err()
        );
        for state in ["lost", "cancelled"] {
            insert(&db, &claim(state, state, 30)).await.unwrap();
            transition(&db, "request", state, "recorded", None, Some("late"), None)
                .await
                .unwrap();
            assert_eq!(
                lookup(&db, "request", state).await.unwrap().unwrap().state,
                state
            );
        }
    }

    #[tokio::test]
    async fn pruning_without_eligible_claims_opens_no_write_transaction() {
        let db = crate::storage::migrated_test_db("check-adoption-prune-no-write").await;
        let cutoff = chrono::Utc::now().timestamp_millis() - RETENTION_MS;
        let before = db.write_transaction_count();
        prune_before(&db, cutoff).await.unwrap();
        assert_eq!(db.write_transaction_count(), before);
        insert(&db, &claim("live", "running", cutoff - 1))
            .await
            .unwrap();
        insert(&db, &claim("recent", "recorded", cutoff))
            .await
            .unwrap();
        let before = db.write_transaction_count();
        prune_before(&db, cutoff).await.unwrap();
        assert_eq!(db.write_transaction_count(), before);
        insert(&db, &claim("old", "recorded", cutoff - 1))
            .await
            .unwrap();
        prune_before(&db, cutoff).await.unwrap();
        assert!(lookup(&db, "request", "old").await.unwrap().is_none());
        let before = db.write_transaction_count();
        prune_before(&db, cutoff).await.unwrap();
        assert_eq!(db.write_transaction_count(), before);
    }

    #[tokio::test]
    async fn prune_only_removes_terminal_rows_strictly_older_than_seven_days() {
        let db = crate::storage::migrated_test_db("check-adoption-prune").await;
        let now = chrono::Utc::now().timestamp_millis();
        let cutoff = now - RETENTION_MS;
        for state in [
            "running",
            "awaiting_adoption",
            "joined",
            "recorded",
            "lost",
            "cancelled",
        ] {
            insert(&db, &claim(state, state, cutoff - 1)).await.unwrap();
        }
        insert(&db, &claim("boundary", "recorded", cutoff))
            .await
            .unwrap();
        insert(&db, &claim("recent", "lost", now)).await.unwrap();
        prune_before(&db, cutoff).await.unwrap();
        for state in [
            "running",
            "awaiting_adoption",
            "joined",
            "boundary",
            "recent",
        ] {
            assert!(
                lookup(&db, "request", state).await.unwrap().is_some(),
                "{state}"
            );
        }
        for state in ["recorded", "lost", "cancelled"] {
            assert!(
                lookup(&db, "request", state).await.unwrap().is_none(),
                "{state}"
            );
        }
        prune(&db).await.unwrap();
        assert_eq!(open(&db).await.unwrap().len(), 3);
    }
}
