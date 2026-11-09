//! Private, per-install rebuild history. Filesystem trust and strict JournalEvent
//! validation belong to the runner; this module consumes only validated events.
use super::{DbError, DbResult, LocalDb, RowExt, TrackedConnection};
use crate::turso::params;
use cairn_common::requests::RequestAttemptOutcome;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IngestionFault {
    pub name: String,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RebuildPhase {
    pub name: String,
    pub started: Option<String>,
    pub ended: Option<String>,
    pub outcome: Option<String>,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RebuildRecord {
    pub id: String,
    pub status: String,
    pub outcome: Option<String>,
    pub summary: Option<String>,
    pub request_id: Option<String>,
    pub attempt_id: Option<String>,
    pub authorization_event_id: Option<String>,
    pub requests: Vec<Value>,
    pub kind: Option<String>,
    pub requesters: Vec<Value>,
    pub waiting: Vec<Value>,
    pub accepted_by: Option<Value>,
    pub accepted_at: Option<String>,
    pub commit_built: Option<String>,
    pub version: Option<String>,
    pub old_build_id: Option<String>,
    pub new_build_id: Option<String>,
    pub old_boot_id: Option<String>,
    pub new_boot_id: Option<String>,
    pub old_commit: Option<String>,
    pub new_commit: Option<String>,
    pub signature_identity: Option<String>,
    pub e1_verdict: Option<Value>,
    pub quiesce_start: Option<String>,
    pub quiesce_end: Option<String>,
    pub quiesce_census: Option<Value>,
    pub reattach_census: Option<Value>,
    pub casualties: Option<Value>,
    pub resumed_by: Option<String>,
    pub phases: Vec<RebuildPhase>,
    pub rollback: Option<Value>,
    pub facts: Value,
    pub events: Vec<Value>,
    pub faults: Vec<IngestionFault>,
    /// The request consumer's launch record; absent for a rebuild no request launched.
    #[serde(default)]
    pub launch: Option<LaunchIntent>,
    /// The report the consumer froze before handing it to the request primitive.
    #[serde(default)]
    pub consumer_report: Option<FrozenReport>,
}

/// What the runner commits to before it stages an actor: the checked attempt it
/// launches under. It is the consumer's record of its own act, never authority;
/// authority stays in the request acceptance journal it names.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LaunchIntent {
    pub request_ids: Vec<String>,
    pub attempt_id: String,
    pub authorization_event_id: String,
    pub kind: String,
    pub target: Option<String>,
    pub commit: String,
    pub facts_version: i64,
    pub recorded_at: i64,
    /// Recorded durably immediately before `launchctl bootstrap`. Absent means
    /// no actor was ever launched under this attempt, whatever launchd shows.
    pub bootstrap_requested_at: Option<i64>,
}

/// A consumer report frozen before it is sent, so a crash between the request
/// primitive's commit and this ledger's acknowledgement replays the same bytes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FrozenReport {
    pub receipt_id: String,
    pub outcome: RequestAttemptOutcome,
    pub document: Value,
    pub settled: bool,
    pub built_commit: Option<String>,
    pub frozen_at: i64,
    pub acknowledged_at: Option<i64>,
}

impl RebuildRecord {
    fn new(id: String) -> Self {
        Self {
            id,
            status: "acting".into(),
            outcome: None,
            summary: None,
            request_id: None,
            attempt_id: None,
            authorization_event_id: None,
            requests: vec![],
            kind: None,
            requesters: vec![],
            waiting: vec![],
            accepted_by: None,
            accepted_at: None,
            commit_built: None,
            version: None,
            old_build_id: None,
            new_build_id: None,
            old_boot_id: None,
            new_boot_id: None,
            old_commit: None,
            new_commit: None,
            signature_identity: None,
            e1_verdict: None,
            quiesce_start: None,
            quiesce_end: None,
            quiesce_census: None,
            reattach_census: None,
            casualties: None,
            resumed_by: None,
            phases: vec![],
            rollback: None,
            facts: json!({}),
            events: vec![],
            faults: vec![],
            launch: None,
            consumer_report: None,
        }
    }

    fn apply(&mut self, event: &Value) {
        match event["type"].as_str() {
            Some("phase") => {
                let name = event["name"].as_str().expect("validated phase name");
                let at = event["at"]
                    .as_str()
                    .expect("validated event timestamp")
                    .to_string();
                if event["state"] == "started" {
                    self.phases.push(RebuildPhase {
                        name: name.into(),
                        started: Some(at.clone()),
                        ended: None,
                        outcome: None,
                        detail: None,
                    });
                } else {
                    let index = self
                        .phases
                        .iter()
                        .rposition(|p| p.name == name && p.ended.is_none())
                        .unwrap_or_else(|| {
                            self.phases.push(RebuildPhase {
                                name: name.into(),
                                started: None,
                                ended: None,
                                outcome: None,
                                detail: None,
                            });
                            self.phases.len() - 1
                        });
                    let phase = &mut self.phases[index];
                    phase.ended = Some(at.clone());
                    phase.outcome = text(event, "outcome");
                    phase.detail = text(event, "detail");
                }
                if name == "quiesce" {
                    if event["state"] == "started" {
                        self.quiesce_start = Some(at);
                    } else {
                        self.quiesce_end = Some(at);
                    }
                }
            }
            Some("fact") => {
                let facts = event["facts"].as_object().expect("validated facts object");
                self.facts
                    .as_object_mut()
                    .expect("facts object")
                    .extend(facts.clone());
                // These are descriptive actor reports, never authority or boot evidence.
                for (key, field) in [
                    ("commit", &mut self.commit_built),
                    ("version", &mut self.version),
                    ("oldBuildId", &mut self.old_build_id),
                    ("newBuildId", &mut self.new_build_id),
                    ("signingIdentity", &mut self.signature_identity),
                    ("resumed_by", &mut self.resumed_by),
                ] {
                    if let Some(value) = facts.get(key).and_then(Value::as_str) {
                        *field = Some(value.to_string());
                    }
                }
                for (key, field) in [
                    ("executorCoverage", &mut self.e1_verdict),
                    ("quiesce_census", &mut self.quiesce_census),
                    ("reattach_census", &mut self.reattach_census),
                    ("casualties", &mut self.casualties),
                ] {
                    if let Some(value) = facts.get(key) {
                        *field = Some(value.clone());
                    }
                }
                if facts.contains_key("rollbackEvidence") || facts.contains_key("restoredBuildId") {
                    let rollback = self.rollback.get_or_insert_with(|| json!({}));
                    for key in ["rollbackEvidence", "restoredBuildId"] {
                        if let Some(value) = facts.get(key) {
                            rollback[key] = value.clone();
                        }
                    }
                }
            }
            Some("outcome") => {
                self.outcome = text(event, "outcome");
                self.status = self.outcome.clone().expect("validated outcome");
                self.summary = text(event, "summary");
            }
            _ => unreachable!("only validated events reach storage"),
        }
        self.events.push(event.clone());
    }
}

fn text(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(Value::as_str).map(str::to_string)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Receipt {
    id: String,
    raw: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Ledger {
    row: RebuildRecord,
    receipts: Vec<Receipt>,
}

#[derive(Debug, Clone, Serialize)]
pub struct IngestionReport {
    pub row: RebuildRecord,
    pub applied: usize,
    pub new_faults: Vec<IngestionFault>,
}

fn decode(row: &crate::turso::Row) -> DbResult<Ledger> {
    let mut ledger: Ledger =
        serde_json::from_str(&row.text(0)?).map_err(|e| DbError::Row(e.to_string()))?;
    ledger.row.request_id = row.opt_text(1)?;
    ledger.row.attempt_id = row.opt_text(2)?;
    ledger.row.authorization_event_id = row.opt_text(3)?;
    ledger.row.old_boot_id = row.opt_text(4)?;
    ledger.row.new_boot_id = row.opt_text(5)?;
    ledger.row.old_commit = row.opt_text(6)?;
    ledger.row.new_commit = row.opt_text(7)?;
    Ok(ledger)
}

const COLUMNS: &str = "record_json,request_id,attempt_id,authorization_event_id,old_boot_id,new_boot_id,old_commit,new_commit";

/// Atomically append validated journal events and named faults. Raw lines exclude
/// their newline, but retain every other byte, including JSON whitespace.
/// A receipt and its projection commit together, so boot replay cannot reorder
/// or reapply a prior event. Updating the same ledger row serializes ingestion.
pub async fn ingest(
    db: &LocalDb,
    id: &str,
    entries: Vec<(String, String, Value)>,
    faults: Vec<IngestionFault>,
    actor_absent: bool,
) -> DbResult<IngestionReport> {
    let id = id.to_string();
    db.write(move |conn| {
        let id = id.clone();
        let entries = entries.clone();
        let faults = faults.clone();
        Box::pin(async move {
            let mut rows = conn.query(&format!("SELECT {COLUMNS} FROM rebuilds WHERE id=?1"), (id.clone(),)).await?;
            let existing = rows.next().await?;
            let existed = existing.is_some();
            let mut ledger = match existing {
                Some(row) => decode(&row)?,
                None => Ledger { row: RebuildRecord::new(id.clone()), receipts: vec![] },
            };
            drop(rows);
            let before = serde_json::to_string(&ledger).map_err(|e| DbError::Row(e.to_string()))?;
            let mut report = IngestionReport { row: ledger.row.clone(), applied: 0, new_faults: vec![] };
            let mut add_fault = |row: &mut RebuildRecord, fault: IngestionFault| {
                if !row.faults.contains(&fault) {
                    row.faults.push(fault.clone());
                    report.new_faults.push(fault);
                }
            };
            for fault in faults { add_fault(&mut ledger.row, fault); }
            for (event_id, raw, event) in entries {
                if let Some(receipt) = ledger.receipts.iter().find(|r| r.id == event_id) {
                    if receipt.raw != raw {
                        add_fault(&mut ledger.row, IngestionFault {
                            name: "event_id_conflict".into(),
                            detail: format!("event {event_id} was already ingested with different bytes"),
                        });
                    }
                    continue;
                }
                if event["type"] == "outcome"
                    && ledger.row.events.iter().any(|e| e["type"] == "outcome")
                {
                    add_fault(&mut ledger.row, IngestionFault {
                        name: "terminal_conflict".into(),
                        detail: format!("event {event_id} would replace an existing terminal outcome"),
                    });
                    continue;
                }
                ledger.row.apply(&event);
                ledger.receipts.push(Receipt { id: event_id, raw });
                report.applied += 1;
            }
            let terminal = ledger.row.events.iter().any(|e| e["type"] == "outcome");
            if !terminal && actor_absent {
                let phase = ledger.row.phases.last().map(|p| p.name.as_str()).unwrap_or("unknown phase");
                ledger.row.status = "failed".into();
                ledger.row.outcome = Some("failed".into());
                ledger.row.summary = Some(format!("actor died during {phase}"));
            } else if !terminal && !actor_absent && report.applied > 0 {
                // Fresh activity contradicts the previous absence observation.
                // A replay or unreadable liveness poll alone proves nothing.
                ledger.row.status = "acting".into();
                ledger.row.outcome = None;
                ledger.row.summary = None;
            }
            let serialized = serde_json::to_string(&ledger).map_err(|e| DbError::Row(e.to_string()))?;
            if !existed || before != serialized {
                conn.execute(
                    "INSERT INTO rebuilds (id,record_json) VALUES (?1,?2) ON CONFLICT(id) DO UPDATE SET record_json=excluded.record_json",
                    params![id, serialized],
                ).await?;
            }
            report.row = ledger.row;
            Ok(report)
        })
    }).await
}

pub async fn get(db: &LocalDb, id: &str) -> DbResult<Option<RebuildRecord>> {
    db.query_opt(
        format!("SELECT {COLUMNS} FROM rebuilds WHERE id=?1"),
        (id.to_string(),),
        |row| Ok(decode(row)?.row),
    )
    .await
}

pub async fn list(db: &LocalDb, limit: usize, offset: usize) -> DbResult<Vec<RebuildRecord>> {
    db.query_all(
        format!(
            "SELECT {COLUMNS} FROM rebuilds ORDER BY created_at DESC,id DESC LIMIT ?1 OFFSET ?2"
        ),
        (limit.min(500) as i64, offset.min(i64::MAX as usize) as i64),
        |row| Ok(decode(row)?.row),
    )
    .await
}

async fn load(conn: &TrackedConnection, id: &str) -> DbResult<Option<Ledger>> {
    let mut rows = conn
        .query(
            &format!("SELECT {COLUMNS} FROM rebuilds WHERE id=?1"),
            (id.to_string(),),
        )
        .await?;
    match rows.next().await? {
        Some(row) => Ok(Some(decode(&row)?)),
        None => Ok(None),
    }
}

async fn save(conn: &TrackedConnection, id: &str, ledger: &Ledger) -> DbResult<()> {
    let serialized = serde_json::to_string(ledger).map_err(|e| DbError::Row(e.to_string()))?;
    conn.execute(
        "UPDATE rebuilds SET record_json=?2 WHERE id=?1",
        params![id.to_string(), serialized],
    )
    .await?;
    Ok(())
}

/// Create the ledger row a request launch acts under. One attempt has one row:
/// an existing row is refused, never adopted, so no second launch can share it.
pub async fn record_launch_intent(
    db: &LocalDb,
    id: &str,
    intent: LaunchIntent,
) -> DbResult<RebuildRecord> {
    let id = id.to_string();
    db.write(move |conn| {
        let id = id.clone();
        let intent = intent.clone();
        Box::pin(async move {
            if load(conn, &id).await?.is_some() {
                return Err(DbError::Row(format!(
                    "rebuild {id} already has a ledger row; a launch is never repeated"
                )));
            }
            let mut row = RebuildRecord::new(id.clone());
            row.launch = Some(intent.clone());
            let ledger = Ledger {
                row,
                receipts: vec![],
            };
            let serialized =
                serde_json::to_string(&ledger).map_err(|e| DbError::Row(e.to_string()))?;
            conn.execute(
                "INSERT INTO rebuilds (id,record_json,request_id,attempt_id,authorization_event_id) VALUES (?1,?2,?3,?4,?5)",
                params![
                    id.clone(),
                    serialized,
                    intent.request_ids.first().cloned(),
                    intent.attempt_id.clone(),
                    intent.authorization_event_id.clone()
                ],
            )
            .await?;
            Ok(load(conn, &id).await?.expect("inserted row").row)
        })
    })
    .await
}

/// Record that bootstrap is about to be requested. Written before the call, so
/// a crash mid-call leaves liveness to launchd rather than to an assumption.
pub async fn mark_bootstrap_requested(db: &LocalDb, id: &str, at: i64) -> DbResult<()> {
    let id = id.to_string();
    db.write(move |conn| {
        let id = id.clone();
        Box::pin(async move {
            let mut ledger = load(conn, &id)
                .await?
                .ok_or_else(|| DbError::Row(format!("rebuild {id} has no ledger row")))?;
            let launch = ledger
                .row
                .launch
                .as_mut()
                .ok_or_else(|| DbError::Row(format!("rebuild {id} has no launch intent")))?;
            if launch.bootstrap_requested_at.is_some() {
                return Err(DbError::Row(format!(
                    "rebuild {id} was already bootstrapped; a launch is never repeated"
                )));
            }
            launch.bootstrap_requested_at = Some(at);
            save(conn, &id, &ledger).await
        })
    })
    .await
}

/// Freeze the consumer's report once. A second freeze returns the first one
/// unchanged, so replay after a crash always sends identical bytes.
pub async fn freeze_report(db: &LocalDb, id: &str, report: FrozenReport) -> DbResult<FrozenReport> {
    let id = id.to_string();
    db.write(move |conn| {
        let id = id.clone();
        let report = report.clone();
        Box::pin(async move {
            let mut ledger = load(conn, &id)
                .await?
                .ok_or_else(|| DbError::Row(format!("rebuild {id} has no ledger row")))?;
            if let Some(existing) = &ledger.row.consumer_report {
                return Ok(existing.clone());
            }
            ledger.row.consumer_report = Some(report.clone());
            save(conn, &id, &ledger).await?;
            Ok(report)
        })
    })
    .await
}

/// The request primitive holds this exact receipt; the consumer is finished.
pub async fn acknowledge_report(db: &LocalDb, id: &str, receipt: &str, at: i64) -> DbResult<()> {
    let id = id.to_string();
    let receipt = receipt.to_string();
    db.write(move |conn| {
        let id = id.clone();
        let receipt = receipt.clone();
        Box::pin(async move {
            let mut ledger = load(conn, &id)
                .await?
                .ok_or_else(|| DbError::Row(format!("rebuild {id} has no ledger row")))?;
            let report = ledger
                .row
                .consumer_report
                .as_mut()
                .filter(|report| report.receipt_id == receipt)
                .ok_or_else(|| DbError::Row(format!("rebuild {id} froze no report {receipt}")))?;
            if report.acknowledged_at.is_none() {
                report.acknowledged_at = Some(at);
                save(conn, &id, &ledger).await?;
            }
            Ok(())
        })
    })
    .await
}

/// Rows a request launched whose report the primitive has not acknowledged.
pub async fn unacknowledged_launches(db: &LocalDb) -> DbResult<Vec<RebuildRecord>> {
    let rows = db
        .query_all(
            format!("SELECT {COLUMNS} FROM rebuilds WHERE attempt_id IS NOT NULL ORDER BY created_at,id"),
            (),
            |row| Ok(decode(row)?.row),
        )
        .await?;
    Ok(rows
        .into_iter()
        .filter(|row| {
            row.launch.is_some()
                && row
                    .consumer_report
                    .as_ref()
                    .is_none_or(|report| report.acknowledged_at.is_none())
        })
        .collect())
}

/// Completed `build` phase durations, newest rebuilds first, in seconds.
pub async fn build_durations(db: &LocalDb, limit: usize) -> DbResult<Vec<i64>> {
    let rows = list(db, limit, 0).await?;
    Ok(rows
        .iter()
        .filter_map(|row| {
            let phase =
                row.phases.iter().rev().find(|phase| {
                    phase.name == "build" && phase.outcome.as_deref() == Some("ok")
                })?;
            let started = chrono::DateTime::parse_from_rfc3339(phase.started.as_deref()?).ok()?;
            let ended = chrono::DateTime::parse_from_rfc3339(phase.ended.as_deref()?).ok()?;
            let seconds = (ended - started).num_seconds();
            (seconds >= 0).then_some(seconds)
        })
        .collect())
}

impl LocalDb {
    pub async fn get_rebuild(&self, id: &str) -> DbResult<Option<RebuildRecord>> {
        get(self, id).await
    }

    pub async fn list_rebuilds(&self, limit: usize, offset: usize) -> DbResult<Vec<RebuildRecord>> {
        list(self, limit, offset).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::migrated_test_db;

    fn entry(id: &str, body: Value) -> (String, String, Value) {
        let mut value = json!({"event":id,"rebuild":"r","at":"2026-11-07T14:00:00Z"});
        value
            .as_object_mut()
            .unwrap()
            .extend(body.as_object().unwrap().clone());
        (id.into(), value.to_string(), value)
    }

    fn phase(id: &str, name: &str, state: &str) -> (String, String, Value) {
        entry(
            id,
            json!({"type":"phase","name":name,"state":state,"outcome":"ok"}),
        )
    }

    fn outcome(id: &str, result: &str) -> (String, String, Value) {
        entry(
            id,
            json!({"type":"outcome","outcome":result,"summary":result}),
        )
    }

    #[tokio::test]
    async fn replay_preserves_order_and_exact_raw_bytes() {
        let db = migrated_test_db("rebuild-replay").await;
        let start = phase("start", "build", "started");
        let end = phase("end", "build", "ended");
        let first = ingest(&db, "r", vec![start.clone(), end.clone()], vec![], false)
            .await
            .unwrap();
        assert_eq!(first.applied, 2);
        let replay = ingest(&db, "r", vec![end.clone(), start.clone()], vec![], false)
            .await
            .unwrap();
        assert_eq!(replay.applied, 0);
        assert_eq!(replay.row, first.row);
        let mut conflict = start.clone();
        // Even semantically identical JSON with changed whitespace conflicts.
        conflict.1 = format!(" {}", conflict.1);
        let rejected = ingest(&db, "r", vec![conflict.clone()], vec![], false)
            .await
            .unwrap();
        assert_eq!(rejected.applied, 0);
        assert_eq!(rejected.new_faults[0].name, "event_id_conflict");
        assert_eq!(rejected.row.events, first.row.events);
        assert_eq!(rejected.row.phases, first.row.phases);
        let again = ingest(&db, "r", vec![conflict], vec![], false)
            .await
            .unwrap();
        assert!(again.new_faults.is_empty());
        assert_eq!(db.get_rebuild("r").await.unwrap().unwrap(), rejected.row);
    }

    #[tokio::test]
    async fn real_terminal_is_immutable_but_supersedes_synthetic_death() {
        let db = migrated_test_db("rebuild-terminal").await;
        let start = phase("start", "install", "started");
        let unknown = ingest(&db, "r", vec![start.clone()], vec![], false)
            .await
            .unwrap();
        assert_eq!(unknown.row.status, "acting");
        assert_eq!(unknown.row.outcome, None);
        let absent = ingest(&db, "r", vec![start], vec![], true).await.unwrap();
        assert_eq!(absent.row.status, "failed");
        assert_eq!(
            absent.row.summary.as_deref(),
            Some("actor died during install")
        );
        let done = outcome("done", "done");
        let recovered = ingest(&db, "r", vec![done.clone()], vec![], true)
            .await
            .unwrap();
        assert_eq!(recovered.row.status, "done");
        let replay = ingest(&db, "r", vec![done], vec![], true).await.unwrap();
        assert_eq!(replay.applied, 0);
        assert!(replay.new_faults.is_empty());
        let rejected = ingest(&db, "r", vec![outcome("other", "failed")], vec![], true)
            .await
            .unwrap();
        assert_eq!(rejected.applied, 0);
        assert_eq!(rejected.new_faults[0].name, "terminal_conflict");
        assert_eq!(rejected.row.status, "done");
        assert_eq!(rejected.row.summary.as_deref(), Some("done"));
        assert_eq!(rejected.row.events, recovered.row.events);
    }

    #[tokio::test]
    async fn fresh_activity_recovers_synthetic_death_but_unknown_replay_does_not() {
        let db = migrated_test_db("rebuild-recovery").await;
        let start = phase("start", "build", "started");
        let dead = ingest(&db, "r", vec![start.clone()], vec![], true)
            .await
            .unwrap();
        assert_eq!(dead.row.status, "failed");
        let unknown = ingest(&db, "r", vec![start], vec![], false).await.unwrap();
        assert_eq!(unknown.row, dead.row);
        let active = ingest(
            &db,
            "r",
            vec![phase("end", "build", "ended")],
            vec![],
            false,
        )
        .await
        .unwrap();
        assert_eq!(active.applied, 1);
        assert_eq!(active.row.status, "acting");
        assert!(active.row.outcome.is_none());
        assert!(active.row.summary.is_none());
        let done = ingest(&db, "r", vec![outcome("done", "done")], vec![], false)
            .await
            .unwrap();
        let after = ingest(
            &db,
            "r",
            vec![entry(
                "late",
                json!({"type":"fact","facts":{"note":"late"}}),
            )],
            vec![],
            false,
        )
        .await
        .unwrap();
        assert_eq!(after.row.outcome, done.row.outcome);
        assert_eq!(after.row.summary, done.row.summary);
        assert_eq!(after.row.status, "done");
    }

    #[tokio::test]
    async fn unchanged_poll_does_not_update_the_ledger() {
        let db = migrated_test_db("rebuild-noop").await;
        let start = phase("start", "build", "started");
        ingest(&db, "r", vec![start.clone()], vec![], false)
            .await
            .unwrap();
        db.execute_batch(
            "CREATE TABLE rebuild_write_probe (updates INTEGER NOT NULL);
             INSERT INTO rebuild_write_probe VALUES (0);
             CREATE TRIGGER rebuild_write_probe_count AFTER UPDATE ON rebuilds
             BEGIN UPDATE rebuild_write_probe SET updates=updates+1; END;",
        )
        .await
        .unwrap();
        ingest(&db, "r", vec![start.clone()], vec![], false)
            .await
            .unwrap();
        ingest(&db, "r", vec![start.clone()], vec![], true)
            .await
            .unwrap();
        ingest(&db, "r", vec![start], vec![], true).await.unwrap();
        assert_eq!(
            db.query_opt_i64("SELECT updates FROM rebuild_write_probe", ())
                .await
                .unwrap(),
            Some(1)
        );
    }

    #[tokio::test]
    async fn named_faults_are_durable_deduplicated_and_visible_without_events() {
        let db = migrated_test_db("rebuild-faults").await;
        let faults = vec![
            IngestionFault {
                name: "unknown_event_type".into(),
                detail: "line 1: unsupported event".into(),
            },
            IngestionFault {
                name: "malformed_event".into(),
                detail: "line 2: invalid JSON".into(),
            },
        ];
        let first = ingest(&db, "r", vec![], faults.clone(), false)
            .await
            .unwrap();
        assert_eq!(first.new_faults, faults);
        assert!(first.row.events.is_empty());
        let replay = ingest(&db, "r", vec![], faults.clone(), false)
            .await
            .unwrap();
        assert!(replay.new_faults.is_empty());
        assert_eq!(db.list_rebuilds(10, 0).await.unwrap()[0].faults, faults);
        assert_eq!(db.get_rebuild("r").await.unwrap().unwrap(), first.row);
    }

    #[tokio::test]
    async fn descriptive_facts_project_without_authenticating_authority_or_boots() {
        let db = migrated_test_db("rebuild-facts").await;
        let facts = entry(
            "facts",
            json!({"type":"fact","facts":{
                "commit":"built-commit","version":"1.2.3","oldBuildId":"old","newBuildId":"new",
                "signingIdentity":"Developer ID","executorCoverage":{"verdict":"cut_over"},
                "quiesce_census":{"finished":2},"reattach_census":{"returned":2},
                "casualties":[],"resumed_by":"successor","rollbackEvidence":"exited",
                "restoredBuildId":"old","request_id":"forged","attempt_id":"forged",
                "authorization_event_id":"forged","accepted_by":"forged","accepted_at":"forged",
                "old_boot_id":"forged","new_boot_id":"forged","old_commit":"forged","new_commit":"forged"
            }}),
        );
        let report = ingest(
            &db,
            "r",
            vec![
                phase("q1", "quiesce", "started"),
                phase("q2", "quiesce", "ended"),
                facts,
            ],
            vec![],
            false,
        )
        .await
        .unwrap();
        let row = report.row;
        assert_eq!(row.commit_built.as_deref(), Some("built-commit"));
        assert_eq!(row.version.as_deref(), Some("1.2.3"));
        assert_eq!(row.old_build_id.as_deref(), Some("old"));
        assert_eq!(row.new_build_id.as_deref(), Some("new"));
        assert_eq!(row.signature_identity.as_deref(), Some("Developer ID"));
        assert_eq!(row.e1_verdict, Some(json!({"verdict":"cut_over"})));
        assert_eq!(row.quiesce_census, Some(json!({"finished":2})));
        assert_eq!(row.reattach_census, Some(json!({"returned":2})));
        assert_eq!(row.casualties, Some(json!([])));
        assert_eq!(row.resumed_by.as_deref(), Some("successor"));
        assert!(row.quiesce_start.is_some() && row.quiesce_end.is_some());
        assert_eq!(
            row.rollback,
            Some(json!({"rollbackEvidence":"exited","restoredBuildId":"old"}))
        );
        assert!(
            row.request_id.is_none()
                && row.attempt_id.is_none()
                && row.authorization_event_id.is_none()
        );
        assert!(row.accepted_by.is_none() && row.accepted_at.is_none());
        assert!(row.old_boot_id.is_none() && row.new_boot_id.is_none());
        assert!(row.old_commit.is_none() && row.new_commit.is_none());
        assert_eq!(row.facts["request_id"], "forged");
        assert_eq!(db.get_rebuild("r").await.unwrap().unwrap(), row);
    }

    fn launch_intent(attempt: &str) -> LaunchIntent {
        LaunchIntent {
            request_ids: vec!["request".into()],
            attempt_id: attempt.into(),
            authorization_event_id: "event".into(),
            kind: "rebuild".into(),
            target: None,
            commit: "b".repeat(40),
            facts_version: 3,
            recorded_at: 1,
            bootstrap_requested_at: None,
        }
    }

    fn frozen(receipt: &str, outcome: RequestAttemptOutcome) -> FrozenReport {
        FrozenReport {
            receipt_id: receipt.into(),
            outcome,
            document: json!({"rebuild":"cairn://rebuilds/r"}),
            settled: true,
            built_commit: None,
            frozen_at: 2,
            acknowledged_at: None,
        }
    }

    #[tokio::test]
    async fn one_attempt_has_one_launch_row_and_one_bootstrap() {
        let db = migrated_test_db("rebuild-launch").await;
        let row = record_launch_intent(&db, "r", launch_intent("attempt"))
            .await
            .unwrap();
        assert_eq!(row.status, "acting");
        assert_eq!(row.attempt_id.as_deref(), Some("attempt"));
        assert_eq!(row.request_id.as_deref(), Some("request"));
        assert_eq!(row.authorization_event_id.as_deref(), Some("event"));
        assert!(record_launch_intent(&db, "r", launch_intent("attempt"))
            .await
            .is_err());
        mark_bootstrap_requested(&db, "r", 5).await.unwrap();
        assert!(mark_bootstrap_requested(&db, "r", 6).await.is_err());
        let row = get(&db, "r").await.unwrap().unwrap();
        assert_eq!(row.launch.unwrap().bootstrap_requested_at, Some(5));
        // A journal-only row (an operator-era rebuild) cannot be bootstrapped.
        ingest(
            &db,
            "plain",
            vec![phase("p", "build", "started")],
            vec![],
            false,
        )
        .await
        .unwrap();
        assert!(mark_bootstrap_requested(&db, "plain", 1).await.is_err());
        // Ingestion keeps the consumer's launch record.
        ingest(
            &db,
            "r",
            vec![phase("s", "build", "started")],
            vec![],
            false,
        )
        .await
        .unwrap();
        assert!(get(&db, "r").await.unwrap().unwrap().launch.is_some());
    }

    #[tokio::test]
    async fn a_frozen_report_replays_unchanged_until_acknowledged() {
        let db = migrated_test_db("rebuild-frozen").await;
        record_launch_intent(&db, "r", launch_intent("attempt"))
            .await
            .unwrap();
        record_launch_intent(&db, "s", launch_intent("other"))
            .await
            .unwrap();
        ingest(
            &db,
            "plain",
            vec![phase("p", "build", "started")],
            vec![],
            false,
        )
        .await
        .unwrap();
        let ids = |rows: Vec<RebuildRecord>| rows.into_iter().map(|row| row.id).collect::<Vec<_>>();
        assert_eq!(ids(unacknowledged_launches(&db).await.unwrap()), ["r", "s"]);
        let first = freeze_report(&db, "r", frozen("receipt", RequestAttemptOutcome::Failed))
            .await
            .unwrap();
        // A second decision never replaces the first.
        let again = freeze_report(&db, "r", frozen("other", RequestAttemptOutcome::Done))
            .await
            .unwrap();
        assert_eq!(again, first);
        assert!(acknowledge_report(&db, "r", "other", 3).await.is_err());
        acknowledge_report(&db, "r", "receipt", 3).await.unwrap();
        acknowledge_report(&db, "r", "receipt", 4).await.unwrap();
        let row = get(&db, "r").await.unwrap().unwrap();
        assert_eq!(row.consumer_report.unwrap().acknowledged_at, Some(3));
        assert_eq!(ids(unacknowledged_launches(&db).await.unwrap()), ["s"]);
    }

    #[tokio::test]
    async fn expected_duration_reads_completed_build_phases() {
        let db = migrated_test_db("rebuild-durations").await;
        let at = |id: &str, name: &str, state: &str, at: &str, outcome: Option<&str>| {
            let mut value =
                json!({"event":id,"rebuild":"r","at":at,"type":"phase","name":name,"state":state});
            if let Some(outcome) = outcome {
                value["outcome"] = json!(outcome);
            }
            (id.to_string(), value.to_string(), value)
        };
        ingest(
            &db,
            "r",
            vec![
                at("a", "build", "started", "2026-11-07T14:00:00Z", None),
                at("b", "build", "ended", "2026-11-07T14:10:00Z", Some("ok")),
            ],
            vec![],
            false,
        )
        .await
        .unwrap();
        ingest(
            &db,
            "failed-build",
            vec![
                at("c", "build", "started", "2026-11-07T14:00:00Z", None),
                at(
                    "d",
                    "build",
                    "ended",
                    "2026-11-07T14:01:00Z",
                    Some("failed"),
                ),
            ],
            vec![],
            false,
        )
        .await
        .unwrap();
        assert_eq!(build_durations(&db, 10).await.unwrap(), [600]);
    }

    #[tokio::test]
    async fn outcomes_and_paginated_reads_are_serializable() {
        let db = migrated_test_db("rebuild-read").await;
        assert!(db.get_rebuild("missing").await.unwrap().is_none());
        for result in ["done", "failed", "inconclusive", "rolled_back", "cancelled"] {
            let report = ingest(&db, result, vec![outcome(result, result)], vec![], false)
                .await
                .unwrap();
            assert_eq!(report.row.status, result);
            let serialized = serde_json::to_string(&report.row).unwrap();
            assert_eq!(
                serde_json::from_str::<RebuildRecord>(&serialized).unwrap(),
                report.row
            );
        }
        let all = db.list_rebuilds(10, 0).await.unwrap();
        assert_eq!(all.len(), 5);
        assert_eq!(db.list_rebuilds(2, 1).await.unwrap(), all[1..3]);
        assert!(db.list_rebuilds(0, 0).await.unwrap().is_empty());
        assert!(db.list_rebuilds(10, 5).await.unwrap().is_empty());
        assert_eq!(db.query_opt_i64(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='self_rebuild_outcomes'", ()
        ).await.unwrap(), Some(0));
    }
}
