use super::{DbError, DbResult, LocalDb, RowExt};
use crate::turso::params;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

const COLUMNS: &str = "request_id,workspace_key,resource_lane,launch_key,claim_json,priority,enqueue_seq,state,lease_owner,lease_generation,lease_expires_at,failure_detail,created_at,updated_at";
pub const DEFAULT_URGENT_BURST: i64 = 3;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeResourceClaim {
    pub resident_process_units: u32,
    pub server_instance_key: Option<String>,
    pub logical_stream_units: u32,
    pub estimated_memory_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeResourceCapacity {
    pub resident_process_units: u32,
    pub logical_stream_units: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewAdmissionRequest {
    pub request_id: String,
    pub workspace_key: String,
    pub resource_lane: String,
    pub launch_key: String,
    pub resource_claim: RuntimeResourceClaim,
    pub urgent: bool,
    pub now: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionRequest {
    pub request_id: String,
    pub workspace_key: String,
    pub resource_lane: String,
    pub launch_key: String,
    pub resource_claim: RuntimeResourceClaim,
    pub urgent: bool,
    pub enqueue_seq: i64,
    pub state: String,
    pub lease_owner: Option<String>,
    pub lease_generation: i64,
    pub lease_expires_at: Option<i64>,
    pub failure_detail: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

fn from_row(row: &crate::turso::Row) -> DbResult<AdmissionRequest> {
    Ok(AdmissionRequest {
        request_id: row.text(0)?,
        workspace_key: row.text(1)?,
        resource_lane: row.text(2)?,
        launch_key: row.text(3)?,
        resource_claim: serde_json::from_str(&row.text(4)?).map_err(|error| {
            DbError::internal(format!("invalid runtime resource claim: {error}"))
        })?,
        urgent: row.i64(5)? != 0,
        enqueue_seq: row.i64(6)?,
        state: row.text(7)?,
        lease_owner: row.opt_text(8)?,
        lease_generation: row.i64(9)?,
        lease_expires_at: row.opt_i64(10)?,
        failure_detail: row.opt_text(11)?,
        created_at: row.i64(12)?,
        updated_at: row.i64(13)?,
    })
}

pub async fn get_admission_request(
    db: &LocalDb,
    request_id: &str,
) -> DbResult<Option<AdmissionRequest>> {
    db.query_opt(
        format!("SELECT {COLUMNS} FROM runtime_admission_requests WHERE request_id=?1"),
        (request_id.to_string(),),
        from_row,
    )
    .await
}

/// Idempotently enqueue by workspace and launch identity.
pub async fn enqueue_admission_request(
    db: &LocalDb,
    new: NewAdmissionRequest,
) -> DbResult<AdmissionRequest> {
    let claim_json = serde_json::to_string(&new.resource_claim).map_err(|error| {
        DbError::internal(format!(
            "failed to serialize runtime resource claim: {error}"
        ))
    })?;
    let resolved_id = db.write(move |conn| { let new = new.clone(); let claim_json = claim_json.clone(); Box::pin(async move {
        let mut rows = conn.query("SELECT request_id FROM runtime_admission_requests WHERE workspace_key=?1 AND launch_key=?2", params![new.workspace_key.clone(), new.launch_key.clone()]).await?;
        if let Some(row) = rows.next().await? { return row.text(0); }
        let mut seq_rows = conn.query("SELECT COALESCE(MAX(enqueue_seq),0)+1 FROM runtime_admission_requests WHERE workspace_key=?1 AND resource_lane=?2", params![new.workspace_key.clone(), new.resource_lane.clone()]).await?;
        let seq = seq_rows.next().await?.expect("aggregate row").i64(0)?;
        conn.execute("INSERT INTO runtime_admission_requests(request_id,workspace_key,resource_lane,launch_key,claim_json,priority,enqueue_seq,created_at,updated_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?8)", params![new.request_id.clone(),new.workspace_key,new.resource_lane,new.launch_key,claim_json,i64::from(new.urgent),seq,new.now]).await?;
        Ok(new.request_id)
    })}).await.and_then(|id| if id.is_empty() { Err(DbError::internal("empty admission request id")) } else { Ok(id) })?;
    get_admission_request(db, &resolved_id)
        .await?
        .ok_or_else(|| DbError::internal("enqueued admission request disappeared"))
}

/// Lease one request with restart-persistent bounded priority fairness.
pub async fn lease_next_admission(
    db: &LocalDb,
    workspace: &str,
    lane: &str,
    owner: &str,
    now: i64,
    expires_at: i64,
) -> DbResult<Option<AdmissionRequest>> {
    lease_next_admission_with_capacity(
        db,
        workspace,
        lane,
        owner,
        now,
        expires_at,
        RuntimeResourceCapacity {
            resident_process_units: u32::MAX,
            logical_stream_units: u32::MAX,
        },
    )
    .await
}

/// Lease one request only when the lane has room. Capacity inspection and the
/// queued-to-leased transition share the repository write transaction.
pub async fn lease_next_admission_with_capacity(
    db: &LocalDb,
    workspace: &str,
    lane: &str,
    owner: &str,
    now: i64,
    expires_at: i64,
    capacity: RuntimeResourceCapacity,
) -> DbResult<Option<AdmissionRequest>> {
    let (workspace, lane, owner) = (workspace.to_string(), lane.to_string(), owner.to_string());
    let leased_id = db.write(move |conn| { let (workspace,lane,owner)=(workspace.clone(),lane.clone(),owner.clone()); Box::pin(async move {
        conn.execute("INSERT OR IGNORE INTO runtime_admission_cursors(workspace_key,resource_lane) VALUES(?1,?2)", params![workspace.clone(),lane.clone()]).await?;
        let mut cursor = conn.query("SELECT urgent_streak FROM runtime_admission_cursors WHERE workspace_key=?1 AND resource_lane=?2", params![workspace.clone(),lane.clone()]).await?;
        let streak = cursor.next().await?.expect("cursor row").i64(0)?;
        let prefer_urgent = streak < DEFAULT_URGENT_BURST;
        let mut rows = conn.query("SELECT request_id,priority,claim_json FROM runtime_admission_requests WHERE workspace_key=?1 AND resource_lane=?2 AND state='queued' ORDER BY CASE WHEN ?3=1 THEN priority ELSE 1-priority END DESC, enqueue_seq LIMIT 1", params![workspace.clone(),lane.clone(),i64::from(prefer_urgent)]).await?;
        let Some(row)=rows.next().await? else { return Ok(None) };
        let id=row.text(0)?;
        let urgent=row.i64(1)? != 0;
        let candidate: RuntimeResourceClaim = serde_json::from_str(&row.text(2)?)
            .map_err(|error| DbError::internal(format!("invalid queued runtime resource claim: {error}")))?;

        let mut active_rows = conn.query("SELECT claim_json FROM runtime_admission_requests WHERE workspace_key=?1 AND resource_lane=?2 AND state IN ('leased','starting','active')", params![workspace.clone(),lane.clone()]).await?;
        let mut process_units = 0_u64;
        let mut stream_units = 0_u64;
        let mut resident_servers = HashSet::new();
        while let Some(active_row) = active_rows.next().await? {
            let claim: RuntimeResourceClaim = serde_json::from_str(&active_row.text(0)?)
                .map_err(|error| DbError::internal(format!("invalid active runtime resource claim: {error}")))?;
            stream_units += u64::from(claim.logical_stream_units);
            if claim.resident_process_units > 0
                && claim.server_instance_key.as_ref().is_none_or(|key| resident_servers.insert(key.clone()))
            {
                process_units += u64::from(claim.resident_process_units);
            }
        }
        stream_units += u64::from(candidate.logical_stream_units);
        if candidate.resident_process_units > 0
            && candidate.server_instance_key.as_ref().is_none_or(|key| !resident_servers.contains(key))
        {
            process_units += u64::from(candidate.resident_process_units);
        }
        if process_units > u64::from(capacity.resident_process_units)
            || stream_units > u64::from(capacity.logical_stream_units)
        {
            return Ok(None);
        }
        conn.execute("UPDATE runtime_admission_requests SET state='leased',lease_owner=?2,lease_generation=lease_generation+1,lease_expires_at=?3,updated_at=?4 WHERE request_id=?1 AND state='queued'", params![id.clone(),owner,expires_at,now]).await?;
        conn.execute("UPDATE runtime_admission_cursors SET urgent_streak=?3 WHERE workspace_key=?1 AND resource_lane=?2", params![workspace,lane,if urgent { streak+1 } else { 0 }]).await?;
        Ok(Some(id))
    })}).await?;
    match leased_id {
        Some(id) => get_admission_request(db, &id).await,
        None => Ok(None),
    }
}

/// Fenced lifecycle mutation: stale owners or generations cannot change a replacement lease.
pub async fn transition_admission(
    db: &LocalDb,
    request_id: &str,
    owner: &str,
    generation: i64,
    from: &str,
    to: &str,
    now: i64,
) -> DbResult<bool> {
    let request_id = request_id.to_string();
    let owner = owner.to_string();
    let from = from.to_string();
    let to = to.to_string();
    let changed = db.write(move |conn| {
        let (request_id, owner, from, to) =
            (request_id.clone(), owner.clone(), from.clone(), to.clone());
        Box::pin(async move {
            Ok(conn.execute("UPDATE runtime_admission_requests SET state=?5,updated_at=?6 WHERE request_id=?1 AND lease_owner=?2 AND lease_generation=?3 AND state=?4", params![request_id,owner,generation,from,to,now]).await?)
        })
    }).await?;
    Ok(changed == 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(id: &str, urgent: bool, now: i64) -> NewAdmissionRequest {
        claimed_request(id, urgent, now, 1, None, 1)
    }

    fn claimed_request(
        id: &str,
        urgent: bool,
        now: i64,
        process_units: u32,
        server_key: Option<&str>,
        stream_units: u32,
    ) -> NewAdmissionRequest {
        NewAdmissionRequest {
            request_id: id.into(),
            workspace_key: "w".into(),
            resource_lane: "process".into(),
            launch_key: id.into(),
            resource_claim: RuntimeResourceClaim {
                resident_process_units: process_units,
                server_instance_key: server_key.map(str::to_string),
                logical_stream_units: stream_units,
                estimated_memory_bytes: None,
            },
            urgent,
            now,
        }
    }

    #[tokio::test]
    async fn enqueue_is_idempotent_by_launch_identity() {
        let db = crate::storage::migrated_test_db("admission-idempotency.db").await;
        let first = enqueue_admission_request(&db, request("one", false, 1))
            .await
            .unwrap();
        let mut duplicate = request("two", true, 2);
        duplicate.launch_key = "one".into();
        let second = enqueue_admission_request(&db, duplicate).await.unwrap();
        assert_eq!(second.request_id, first.request_id);
        assert_eq!(second.enqueue_seq, first.enqueue_seq);
    }

    #[tokio::test]
    async fn urgent_burst_is_bounded_and_lease_is_fenced() {
        let db = crate::storage::migrated_test_db("admission-fairness.db").await;
        enqueue_admission_request(&db, request("ordinary", false, 1))
            .await
            .unwrap();
        for n in 1..=4 {
            enqueue_admission_request(&db, request(&format!("urgent-{n}"), true, n + 1))
                .await
                .unwrap();
        }
        let mut leased = Vec::new();
        for n in 0..4 {
            leased.push(
                lease_next_admission(&db, "w", "process", "runner", 10 + n, 100 + n)
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        assert_eq!(
            leased
                .iter()
                .map(|r| r.request_id.as_str())
                .collect::<Vec<_>>(),
            vec!["urgent-1", "urgent-2", "urgent-3", "ordinary"]
        );
        let first = &leased[0];
        assert!(!transition_admission(
            &db,
            &first.request_id,
            "runner",
            first.lease_generation + 1,
            "leased",
            "starting",
            20
        )
        .await
        .unwrap());
        assert!(transition_admission(
            &db,
            &first.request_id,
            "runner",
            first.lease_generation,
            "leased",
            "starting",
            20
        )
        .await
        .unwrap());
    }

    #[tokio::test]
    async fn zero_process_claim_does_not_consume_process_capacity() {
        let db = crate::storage::migrated_test_db("admission-capacity.db").await;
        enqueue_admission_request(&db, claimed_request("stateless", false, 1, 0, None, 1))
            .await
            .unwrap();
        let leased = lease_next_admission_with_capacity(
            &db,
            "w",
            "process",
            "runner",
            2,
            30,
            RuntimeResourceCapacity {
                resident_process_units: 0,
                logical_stream_units: 1,
            },
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(leased.request_id, "stateless");
    }

    #[tokio::test]
    async fn multi_unit_claim_is_charged_at_its_declared_cost() {
        let db = crate::storage::migrated_test_db("admission-multi-unit.db").await;
        enqueue_admission_request(&db, claimed_request("large", false, 1, 2, None, 3))
            .await
            .unwrap();
        let insufficient = RuntimeResourceCapacity {
            resident_process_units: 1,
            logical_stream_units: 3,
        };
        assert!(lease_next_admission_with_capacity(
            &db,
            "w",
            "process",
            "runner",
            2,
            30,
            insufficient,
        )
        .await
        .unwrap()
        .is_none());
    }

    #[tokio::test]
    async fn pooled_server_reuse_deduplicates_process_but_charges_each_stream() {
        let db = crate::storage::migrated_test_db("admission-pooled-reuse.db").await;
        for (id, now) in [("first", 1), ("second", 2), ("third", 3)] {
            enqueue_admission_request(&db, claimed_request(id, false, now, 1, Some("pool-a"), 1))
                .await
                .unwrap();
        }
        let capacity = RuntimeResourceCapacity {
            resident_process_units: 1,
            logical_stream_units: 2,
        };
        for expected in ["first", "second"] {
            let leased =
                lease_next_admission_with_capacity(&db, "w", "process", "runner", 10, 30, capacity)
                    .await
                    .unwrap()
                    .unwrap();
            assert_eq!(leased.request_id, expected);
        }
        assert!(lease_next_admission_with_capacity(
            &db, "w", "process", "runner", 11, 30, capacity,
        )
        .await
        .unwrap()
        .is_none());
    }
}
