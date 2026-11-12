//! The publication ledger (cairn/5962): one row per publication and one per
//! (publication, target, leg). Rows update in place until they settle; a
//! settled row never moves again, which the schema's triggers enforce.
use super::{DbError, DbResult, LocalDb, RowExt, TrackedConnection};
use crate::turso::params;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The reason a leg found `running` at boot is failed with.
pub const RESTARTED_MID_LEG: &str = "runner restarted mid-leg";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LegAsset {
    pub name: String,
    pub sha256: String,
    pub size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Publication {
    pub id: String,
    pub project_id: String,
    /// `executor` or `toolPin`.
    pub kind: String,
    pub commit: Option<String>,
    pub protocol: Option<i64>,
    /// `merge`, `heartbeat` or `manual`.
    pub trigger: String,
    pub requested_by: Option<String>,
    pub force_targets: Vec<String>,
    pub tool: Option<String>,
    pub plan: Option<Value>,
    /// `shadow` or `live`.
    pub mode: String,
    /// `queued`, `running` or `settled`.
    pub state: String,
    pub summary: Option<String>,
    pub created_at: i64,
    pub ended_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PublicationLeg {
    pub publication_id: String,
    pub target: String,
    /// `plan`, `build`, `sign`, `publish`, `combine` or `verify`.
    pub leg: String,
    /// The order legs were laid out in, for display.
    pub seq: i64,
    pub builder: Option<String>,
    /// `fleet` or `hosted`.
    pub builder_kind: String,
    /// `queued`, `running`, `succeeded`, `failed`, `deferred` or `refused`.
    pub state: String,
    pub reason: Option<String>,
    pub started_at: Option<i64>,
    pub ended_at: Option<i64>,
    pub assets: Vec<LegAsset>,
    pub artifact_location: Option<String>,
    pub signer: Option<Value>,
    pub verifier: Option<Value>,
    pub build_jobs: Option<Value>,
    pub log_ref: Option<String>,
    pub request_id: Option<String>,
}

impl PublicationLeg {
    pub fn is_terminal(&self) -> bool {
        is_terminal_leg_state(&self.state)
    }
}

pub fn is_terminal_leg_state(state: &str) -> bool {
    matches!(state, "succeeded" | "failed" | "deferred" | "refused")
}

/// What a start asks for. The ledger assigns nothing but its row.
#[derive(Debug, Clone)]
pub struct NewPublication {
    pub id: String,
    pub project_id: String,
    pub kind: String,
    pub commit: Option<String>,
    pub trigger: String,
    pub requested_by: Option<String>,
    pub force_targets: Vec<String>,
    pub tool: Option<String>,
    pub mode: String,
    pub created_at: i64,
}

/// A start's effect: the new publication, and the queued one it replaced.
#[derive(Debug, Clone)]
pub struct Started {
    pub publication: Publication,
    pub replaced: Option<String>,
}

/// The terminal (or running) facts one leg update writes. `None` leaves the
/// column as it is.
#[derive(Debug, Clone, Default)]
pub struct LegUpdate {
    pub state: Option<String>,
    pub reason: Option<String>,
    pub builder: Option<String>,
    pub started_at: Option<i64>,
    pub ended_at: Option<i64>,
    pub assets: Option<Vec<LegAsset>>,
    pub artifact_location: Option<String>,
    pub signer: Option<Value>,
    pub verifier: Option<Value>,
    pub build_jobs: Option<Value>,
    pub log_ref: Option<String>,
    pub request_id: Option<String>,
}

/// What the ledger says about one target's executor at one protocol, for the
/// rebuild gate (cairn/6236) and anyone else asking "is it there yet".
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "camelCase")]
pub enum TargetState {
    /// A publication's publish leg for it succeeded.
    Live,
    /// A publication that would publish it has not settled.
    InFlight,
    /// The most recent settled attempt failed at this leg.
    Failed { leg: String, reason: String },
    /// The ledger knows nothing about it.
    None,
}

fn json_err(error: serde_json::Error) -> DbError {
    DbError::Row(error.to_string())
}

fn opt_json(row: &crate::turso::Row, idx: usize) -> DbResult<Option<Value>> {
    row.opt_text(idx)?
        .map(|text| serde_json::from_str(&text).map_err(json_err))
        .transpose()
}

fn to_json(value: &impl Serialize) -> DbResult<String> {
    serde_json::to_string(value).map_err(json_err)
}

const PUBLICATION_COLUMNS: &str = "id,project_id,kind,\"commit\",protocol,trigger,requested_by,force_targets_json,tool,plan_json,mode,state,summary,created_at,ended_at";

fn decode_publication(row: &crate::turso::Row) -> DbResult<Publication> {
    Ok(Publication {
        id: row.text(0)?,
        project_id: row.text(1)?,
        kind: row.text(2)?,
        commit: row.opt_text(3)?,
        protocol: row.opt_i64(4)?,
        trigger: row.text(5)?,
        requested_by: row.opt_text(6)?,
        force_targets: serde_json::from_str(&row.text(7)?).map_err(json_err)?,
        tool: row.opt_text(8)?,
        plan: opt_json(row, 9)?,
        mode: row.text(10)?,
        state: row.text(11)?,
        summary: row.opt_text(12)?,
        created_at: row.i64(13)?,
        ended_at: row.opt_i64(14)?,
    })
}

const LEG_COLUMNS: &str = "publication_id,target,leg,seq,builder,builder_kind,state,reason,started_at,ended_at,assets_json,artifact_location,signer_json,verifier_json,build_jobs_json,log_ref,request_id";

fn decode_leg(row: &crate::turso::Row) -> DbResult<PublicationLeg> {
    Ok(PublicationLeg {
        publication_id: row.text(0)?,
        target: row.text(1)?,
        leg: row.text(2)?,
        seq: row.i64(3)?,
        builder: row.opt_text(4)?,
        builder_kind: row.text(5)?,
        state: row.text(6)?,
        reason: row.opt_text(7)?,
        started_at: row.opt_i64(8)?,
        ended_at: row.opt_i64(9)?,
        assets: serde_json::from_str(&row.text(10)?).map_err(json_err)?,
        artifact_location: row.opt_text(11)?,
        signer: opt_json(row, 12)?,
        verifier: opt_json(row, 13)?,
        build_jobs: opt_json(row, 14)?,
        log_ref: row.opt_text(15)?,
        request_id: row.opt_text(16)?,
    })
}

async fn load_publication(conn: &TrackedConnection, id: &str) -> DbResult<Option<Publication>> {
    let mut rows = conn
        .query(
            &format!("SELECT {PUBLICATION_COLUMNS} FROM publications WHERE id=?1"),
            (id.to_string(),),
        )
        .await?;
    match rows.next().await? {
        Some(row) => Ok(Some(decode_publication(&row)?)),
        None => Ok(None),
    }
}

/// Record a start. One pending publication per project (tenet 653): a start
/// replaces a still-queued one, settling it with the reason, and never touches
/// a running one, behind which the new one queues.
pub async fn start(db: &LocalDb, new: NewPublication, why_replaced: &str) -> DbResult<Started> {
    let why_replaced = why_replaced.to_string();
    db.write(move |conn| {
        let new = new.clone();
        let why_replaced = why_replaced.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id FROM publications WHERE project_id=?1 AND state='queued'",
                    (new.project_id.clone(),),
                )
                .await?;
            let mut queued = Vec::new();
            while let Some(row) = rows.next().await? {
                queued.push(row.text(0)?);
            }
            drop(rows);
            for id in &queued {
                conn.execute(
                    "UPDATE publications SET state='settled', ended_at=?2, summary=?3 WHERE id=?1",
                    params![
                        id.clone(),
                        new.created_at,
                        format!("replaced by {} before it ran: {why_replaced}", new.id)
                    ],
                )
                .await?;
            }
            conn.execute(
                "INSERT INTO publications (id,project_id,kind,\"commit\",trigger,requested_by,force_targets_json,tool,mode,state,created_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,'queued',?10)",
                params![
                    new.id.clone(),
                    new.project_id.clone(),
                    new.kind.clone(),
                    new.commit.clone(),
                    new.trigger.clone(),
                    new.requested_by.clone(),
                    to_json(&new.force_targets)?,
                    new.tool.clone(),
                    new.mode.clone(),
                    new.created_at
                ],
            )
            .await?;
            let publication = load_publication(conn, &new.id)
                .await?
                .expect("inserted publication");
            Ok(Started {
                publication,
                replaced: queued.into_iter().next(),
            })
        })
    })
    .await
}

/// Claim the oldest queued publication of a project for running, unless one is
/// already running there. Returns the claimed row.
pub async fn claim_next(db: &LocalDb, project_id: &str) -> DbResult<Option<Publication>> {
    let project_id = project_id.to_string();
    db.write(move |conn| {
        let project_id = project_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT COUNT(*) FROM publications WHERE project_id=?1 AND state='running'",
                    (project_id.clone(),),
                )
                .await?;
            let running = match rows.next().await? {
                Some(row) => row.i64(0)?,
                None => 0,
            };
            drop(rows);
            if running > 0 {
                return Ok(None);
            }
            let mut rows = conn
                .query(
                    "SELECT id FROM publications WHERE project_id=?1 AND state='queued' ORDER BY created_at, id LIMIT 1",
                    (project_id.clone(),),
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Ok(None);
            };
            let id = row.text(0)?;
            drop(rows);
            conn.execute(
                "UPDATE publications SET state='running' WHERE id=?1 AND state='queued'",
                (id.clone(),),
            )
            .await?;
            load_publication(conn, &id).await
        })
    })
    .await
}

/// Projects with a queued publication and none running.
pub async fn projects_with_queued(db: &LocalDb) -> DbResult<Vec<String>> {
    db.query_all(
        "SELECT DISTINCT project_id FROM publications p WHERE state='queued' AND NOT EXISTS (SELECT 1 FROM publications r WHERE r.project_id=p.project_id AND r.state='running')",
        (),
        |row| row.text(0),
    )
    .await
}

/// Record what the publication's plan resolved: the commit, the protocol, and
/// the plan document verbatim.
pub async fn record_plan(
    db: &LocalDb,
    id: &str,
    commit: Option<&str>,
    protocol: Option<i64>,
    plan: Option<&Value>,
) -> DbResult<()> {
    let plan = plan.map(to_json).transpose()?;
    db.execute(
        "UPDATE publications SET \"commit\"=COALESCE(?2,\"commit\"), protocol=COALESCE(?3,protocol), plan_json=COALESCE(?4,plan_json) WHERE id=?1",
        params![id.to_string(), commit.map(str::to_string), protocol, plan],
    )
    .await?;
    Ok(())
}

pub async fn settle(db: &LocalDb, id: &str, summary: &str, at: i64) -> DbResult<()> {
    db.execute(
        "UPDATE publications SET state='settled', summary=?2, ended_at=?3 WHERE id=?1 AND state<>'settled'",
        params![id.to_string(), summary.to_string(), at],
    )
    .await?;
    Ok(())
}

/// Lay out a leg as queued. Laying out an existing leg is a no-op.
pub async fn add_leg(
    db: &LocalDb,
    publication_id: &str,
    target: &str,
    leg: &str,
    seq: i64,
) -> DbResult<()> {
    db.execute(
        "INSERT INTO publication_legs (publication_id,target,leg,seq,builder_kind,state) VALUES (?1,?2,?3,?4,'fleet','queued') ON CONFLICT DO NOTHING",
        params![
            publication_id.to_string(),
            target.to_string(),
            leg.to_string(),
            seq
        ],
    )
    .await?;
    Ok(())
}

/// Apply one update to a leg. A settled leg refuses it (the schema's trigger).
pub async fn update_leg(
    db: &LocalDb,
    publication_id: &str,
    target: &str,
    leg: &str,
    update: LegUpdate,
) -> DbResult<()> {
    let assets = update.assets.as_ref().map(to_json).transpose()?;
    let signer = update.signer.as_ref().map(to_json).transpose()?;
    let verifier = update.verifier.as_ref().map(to_json).transpose()?;
    let build_jobs = update.build_jobs.as_ref().map(to_json).transpose()?;
    db.execute(
        "UPDATE publication_legs SET
            state=COALESCE(?4,state), reason=COALESCE(?5,reason), builder=COALESCE(?6,builder),
            started_at=COALESCE(?7,started_at), ended_at=COALESCE(?8,ended_at),
            assets_json=COALESCE(?9,assets_json), artifact_location=COALESCE(?10,artifact_location),
            signer_json=COALESCE(?11,signer_json), verifier_json=COALESCE(?12,verifier_json),
            build_jobs_json=COALESCE(?13,build_jobs_json), log_ref=COALESCE(?14,log_ref),
            request_id=COALESCE(?15,request_id)
         WHERE publication_id=?1 AND target=?2 AND leg=?3",
        params![
            publication_id.to_string(),
            target.to_string(),
            leg.to_string(),
            update.state,
            update.reason,
            update.builder,
            update.started_at,
            update.ended_at,
            assets,
            update.artifact_location,
            signer,
            verifier,
            build_jobs,
            update.log_ref,
            update.request_id
        ],
    )
    .await?;
    Ok(())
}

pub async fn get(db: &LocalDb, id: &str) -> DbResult<Option<Publication>> {
    db.query_opt(
        format!("SELECT {PUBLICATION_COLUMNS} FROM publications WHERE id=?1"),
        (id.to_string(),),
        decode_publication,
    )
    .await
}

pub async fn list(
    db: &LocalDb,
    project_id: &str,
    limit: usize,
    offset: usize,
) -> DbResult<Vec<Publication>> {
    db.query_all(
        format!("SELECT {PUBLICATION_COLUMNS} FROM publications WHERE project_id=?1 ORDER BY created_at DESC, id DESC LIMIT ?2 OFFSET ?3"),
        (
            project_id.to_string(),
            limit.min(500) as i64,
            offset.min(i64::MAX as usize) as i64,
        ),
        decode_publication,
    )
    .await
}

/// The newest publications of every project on this install.
pub async fn list_recent(db: &LocalDb, limit: usize) -> DbResult<Vec<Publication>> {
    db.query_all(
        format!("SELECT {PUBLICATION_COLUMNS} FROM publications ORDER BY created_at DESC, id DESC LIMIT ?1"),
        (limit.min(50) as i64,),
        decode_publication,
    )
    .await
}

pub async fn legs(db: &LocalDb, publication_id: &str) -> DbResult<Vec<PublicationLeg>> {
    db.query_all(
        format!("SELECT {LEG_COLUMNS} FROM publication_legs WHERE publication_id=?1 ORDER BY seq, target, leg"),
        (publication_id.to_string(),),
        decode_leg,
    )
    .await
}

async fn abandon_in(
    conn: &TrackedConnection,
    id: &str,
    running_reason: &str,
    queued_reason: &str,
    at: i64,
) -> DbResult<()> {
    conn.execute(
        "UPDATE publication_legs SET state='failed', reason=?2, ended_at=?3 WHERE publication_id=?1 AND state='running'",
        params![id.to_string(), running_reason.to_string(), at],
    )
    .await?;
    conn.execute(
        "UPDATE publication_legs SET state='failed', reason=?2, ended_at=?3 WHERE publication_id=?1 AND state='queued'",
        params![id.to_string(), queued_reason.to_string(), at],
    )
    .await?;
    conn.execute(
        "UPDATE publications SET state='settled', summary=?2, ended_at=?3 WHERE id=?1 AND state<>'settled'",
        params![id.to_string(), running_reason.to_string(), at],
    )
    .await?;
    Ok(())
}

/// Settle a publication whose actor stopped before it finished: every leg not
/// yet settled fails, naming `reason`, and the publication settles with it.
pub async fn abandon(db: &LocalDb, id: &str, reason: &str, at: i64) -> DbResult<()> {
    let (id, reason) = (id.to_string(), reason.to_string());
    db.write(move |conn| {
        let (id, reason) = (id.clone(), reason.clone());
        Box::pin(async move {
            abandon_in(conn, &id, &reason, &format!("never ran: {reason}"), at).await
        })
    })
    .await
}

/// Settle what a restart interrupted. A leg found `running` has no live batch
/// behind it any more, so it fails; a queued leg of a running publication never
/// ran and fails with it; the publication settles. The next trigger re-plans
/// from the live release, so nothing is resumed. Returns the publications
/// settled.
pub async fn recover_at_boot(db: &LocalDb, at: i64) -> DbResult<Vec<String>> {
    db.write(move |conn| {
        Box::pin(async move {
            let mut rows = conn
                .query("SELECT id FROM publications WHERE state='running'", ())
                .await?;
            let mut running = Vec::new();
            while let Some(row) = rows.next().await? {
                running.push(row.text(0)?);
            }
            drop(rows);
            for id in &running {
                abandon_in(
                    conn,
                    id,
                    RESTARTED_MID_LEG,
                    "never ran: the runner restarted before this leg's turn",
                    at,
                )
                .await?;
            }
            Ok(running)
        })
    })
    .await
}

/// The ledger's answer for one target's executor at one protocol: live if any
/// publication's publish leg for it succeeded, in flight if an unsettled
/// publication at that protocol (or one not yet planned) still carries a leg
/// for it, failed with the leg and reason of the newest settled failure, and
/// none otherwise. Shadow publications never publish, so they are never live.
pub async fn target_state(db: &LocalDb, protocol: i64, target: &str) -> DbResult<TargetState> {
    let live = db
        .query_opt_i64(
            "SELECT 1 FROM publication_legs l JOIN publications p ON p.id=l.publication_id
             WHERE p.kind='executor' AND p.protocol=?1 AND l.target=?2 AND l.leg='publish' AND l.state='succeeded' LIMIT 1",
            (protocol, target.to_string()),
        )
        .await?;
    if live.is_some() {
        return Ok(TargetState::Live);
    }
    let in_flight = db
        .query_opt_i64(
            "SELECT 1 FROM publications p WHERE p.kind='executor' AND p.mode='live' AND p.state<>'settled'
               AND (p.protocol IS NULL OR (p.protocol=?1 AND EXISTS (
                    SELECT 1 FROM publication_legs l WHERE l.publication_id=p.id AND l.target=?2 AND l.state IN ('queued','running'))))
             LIMIT 1",
            (protocol, target.to_string()),
        )
        .await?;
    if in_flight.is_some() {
        return Ok(TargetState::InFlight);
    }
    let failed = db
        .query_opt(
            "SELECT l.leg, l.reason FROM publication_legs l JOIN publications p ON p.id=l.publication_id
             WHERE p.kind='executor' AND p.mode='live' AND p.protocol=?1 AND l.target=?2 AND l.state IN ('failed','refused','deferred')
             ORDER BY l.ended_at DESC LIMIT 1",
            (protocol, target.to_string()),
            |row| Ok((row.text(0)?, row.opt_text(1)?.unwrap_or_default())),
        )
        .await?;
    Ok(match failed {
        Some((leg, reason)) => TargetState::Failed { leg, reason },
        None => TargetState::None,
    })
}

impl LocalDb {
    pub async fn list_publications(
        &self,
        project_id: &str,
        limit: usize,
        offset: usize,
    ) -> DbResult<Vec<Publication>> {
        list(self, project_id, limit, offset).await
    }

    pub async fn get_publication(&self, id: &str) -> DbResult<Option<Publication>> {
        get(self, id).await
    }

    pub async fn publication_legs(&self, id: &str) -> DbResult<Vec<PublicationLeg>> {
        legs(self, id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::migrated_test_db;

    fn new(id: &str, at: i64, mode: &str) -> NewPublication {
        NewPublication {
            id: id.into(),
            project_id: "p".into(),
            kind: "executor".into(),
            commit: Some("a".repeat(40)),
            trigger: "manual".into(),
            requested_by: None,
            force_targets: vec![],
            tool: None,
            mode: mode.into(),
            created_at: at,
        }
    }

    #[tokio::test]
    async fn a_start_replaces_a_queued_one_and_queues_behind_a_running_one() {
        let db = migrated_test_db("publications-start").await;
        start(&db, new("one", 1, "shadow"), "x").await.unwrap();
        let second = start(&db, new("two", 2, "shadow"), "a newer start")
            .await
            .unwrap();
        assert_eq!(second.replaced.as_deref(), Some("one"));
        let one = get(&db, "one").await.unwrap().unwrap();
        assert_eq!(one.state, "settled");
        assert!(one.summary.unwrap().contains("replaced by two"));
        let claimed = claim_next(&db, "p").await.unwrap().unwrap();
        assert_eq!(
            (claimed.id.as_str(), claimed.state.as_str()),
            ("two", "running")
        );
        let third = start(&db, new("three", 3, "shadow"), "y").await.unwrap();
        assert!(
            third.replaced.is_none(),
            "a running publication is never replaced"
        );
        assert_eq!(get(&db, "two").await.unwrap().unwrap().state, "running");
        assert!(claim_next(&db, "p").await.unwrap().is_none());
        assert!(projects_with_queued(&db).await.unwrap().is_empty());
        settle(&db, "two", "done", 4).await.unwrap();
        assert_eq!(
            projects_with_queued(&db).await.unwrap(),
            vec!["p".to_string()]
        );
        assert_eq!(claim_next(&db, "p").await.unwrap().unwrap().id, "three");
        let recent: Vec<String> = list_recent(&db, 2)
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.id)
            .collect();
        assert_eq!(recent, ["three", "two"]);
    }

    #[tokio::test]
    async fn settled_rows_are_immutable() {
        let db = migrated_test_db("publications-immutable").await;
        start(&db, new("one", 1, "live"), "").await.unwrap();
        add_leg(&db, "one", "t", "build", 1).await.unwrap();
        update_leg(
            &db,
            "one",
            "t",
            "build",
            LegUpdate {
                state: Some("deferred".into()),
                reason: Some("deferred: no eligible builder for t".into()),
                ended_at: Some(2),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let moved = update_leg(
            &db,
            "one",
            "t",
            "build",
            LegUpdate {
                state: Some("running".into()),
                ..Default::default()
            },
        )
        .await;
        assert!(moved.unwrap_err().to_string().contains("immutable"));
        settle(&db, "one", "done", 3).await.unwrap();
        let raw = db
            .execute("UPDATE publications SET summary='x' WHERE id='one'", ())
            .await;
        assert!(raw.unwrap_err().to_string().contains("immutable"));
    }

    #[tokio::test]
    async fn a_shadow_publication_never_records_an_upload() {
        let db = migrated_test_db("publications-shadow").await;
        start(&db, new("s", 1, "shadow"), "").await.unwrap();
        add_leg(&db, "s", "t", "build", 1).await.unwrap();
        let refused = update_leg(
            &db,
            "s",
            "t",
            "build",
            LegUpdate {
                artifact_location: Some("https://example.invalid/a".into()),
                ..Default::default()
            },
        )
        .await;
        assert!(refused.unwrap_err().to_string().contains("never uploads"));
        let reason_missing = update_leg(
            &db,
            "s",
            "t",
            "build",
            LegUpdate {
                state: Some("failed".into()),
                ended_at: Some(2),
                ..Default::default()
            },
        )
        .await;
        assert!(reason_missing.is_err(), "a failed leg names its reason");
    }

    #[tokio::test]
    async fn boot_fails_a_running_leg_and_settles_its_publication() {
        let db = migrated_test_db("publications-boot").await;
        start(&db, new("r", 1, "shadow"), "").await.unwrap();
        claim_next(&db, "p").await.unwrap();
        add_leg(&db, "r", "all", "plan", 0).await.unwrap();
        add_leg(&db, "r", "t", "build", 1).await.unwrap();
        update_leg(
            &db,
            "r",
            "all",
            "plan",
            LegUpdate {
                state: Some("running".into()),
                started_at: Some(2),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(
            recover_at_boot(&db, 9).await.unwrap(),
            vec!["r".to_string()]
        );
        let legs = legs(&db, "r").await.unwrap();
        assert_eq!(legs[0].state, "failed");
        assert_eq!(legs[0].reason.as_deref(), Some(RESTARTED_MID_LEG));
        assert_eq!(legs[1].state, "failed");
        let row = get(&db, "r").await.unwrap().unwrap();
        assert_eq!((row.state.as_str(), row.ended_at), ("settled", Some(9)));
    }

    #[tokio::test]
    async fn target_state_reads_live_in_flight_failed_and_none() {
        let db = migrated_test_db("publications-target").await;
        let t = "x86_64-unknown-linux-gnu";
        assert_eq!(target_state(&db, 7, t).await.unwrap(), TargetState::None);
        start(&db, new("a", 1, "live"), "").await.unwrap();
        assert_eq!(
            target_state(&db, 7, t).await.unwrap(),
            TargetState::InFlight
        );
        record_plan(&db, "a", None, Some(7), None).await.unwrap();
        add_leg(&db, "a", t, "publish", 3).await.unwrap();
        assert_eq!(
            target_state(&db, 7, t).await.unwrap(),
            TargetState::InFlight
        );
        update_leg(
            &db,
            "a",
            t,
            "publish",
            LegUpdate {
                state: Some("failed".into()),
                reason: Some("upload refused".into()),
                ended_at: Some(2),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        settle(&db, "a", "failed", 3).await.unwrap();
        assert_eq!(
            target_state(&db, 7, t).await.unwrap(),
            TargetState::Failed {
                leg: "publish".into(),
                reason: "upload refused".into()
            }
        );
        start(&db, new("b", 4, "live"), "").await.unwrap();
        record_plan(&db, "b", None, Some(7), None).await.unwrap();
        add_leg(&db, "b", t, "publish", 3).await.unwrap();
        update_leg(
            &db,
            "b",
            t,
            "publish",
            LegUpdate {
                state: Some("succeeded".into()),
                ended_at: Some(5),
                artifact_location: Some("https://example.invalid/v7".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(target_state(&db, 7, t).await.unwrap(), TargetState::Live);
        assert_eq!(target_state(&db, 8, t).await.unwrap(), TargetState::None);
    }
}
