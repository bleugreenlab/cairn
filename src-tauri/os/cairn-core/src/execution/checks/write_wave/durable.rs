//! Wave-level replay journal. Lane execution and fold evidence live only in check_requests.
use super::*;

// A reservation is installed before a recovered task is spawned. Its destructor
// releases custody on completion, cancellation or panic; elapsed time never does.
static DRIVERS: std::sync::LazyLock<std::sync::Mutex<std::collections::HashSet<(usize, String)>>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashSet::new()));

pub(super) struct DriverGuard((usize, String));

impl DriverGuard {
    pub(super) fn active(db: &Arc<LocalDb>, id: &str) -> bool {
        DRIVERS
            .lock()
            .unwrap()
            .contains(&(Arc::as_ptr(db) as usize, id.to_string()))
    }

    pub(super) fn acquire(db: &Arc<LocalDb>, id: &str) -> Option<Self> {
        let key = (Arc::as_ptr(db) as usize, id.to_string());
        let acquired = DRIVERS.lock().unwrap().insert(key.clone());
        acquired.then(|| Self(key))
    }
}

impl Drop for DriverGuard {
    fn drop(&mut self) {
        DRIVERS.lock().unwrap().remove(&self.0);
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(super) struct Snapshot {
    pub run: RunContext,
    pub cwd: String,
    pub logical: crate::mcp::handlers::branch::BranchResolution,
    pub base: String,
    pub planned: PlannedWriteWave,
    pub misses: Vec<usize>,
    pub split: WriteWaveSplit,
    pub only_suite: Option<String>,
    pub opened_at_ms: i64,
}

pub(super) fn key(job: &str, tool: &str) -> String {
    format!("write:{job}:{tool}")
}

pub(super) async fn retry<T, F, Fut>(context: &str, mut operation: F) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, String>>,
{
    loop {
        match operation().await {
            Ok(value) => return value,
            Err(error) => {
                log::error!("write-wave retains {context}, waiting for durable storage: {error}");
                tokio::time::sleep(crate::fleet::RETAINED_RESULT_RETRY).await;
            }
        }
    }
}

// The opaque wave id and phase filing keys survive project/job rekeying.
pub(super) async fn resolve_key(db: &Arc<LocalDb>, job: &str, tool: &str) -> String {
    retry("wave identity", || async {
        db.query_text(
            "SELECT id FROM write_check_waves WHERE job_id=?1 AND tool_use_id=?2",
            (job.to_string(), tool.to_string()),
        )
        .await
        .map_err(|e| e.to_string())
    })
    .await
    .unwrap_or_else(|| key(job, tool))
}

pub(super) async fn load(db: &Arc<LocalDb>, id: &str) -> Result<Option<Snapshot>, String> {
    let row = db
        .query_opt(
            "SELECT snapshot_json,run_id FROM write_check_waves WHERE id=?1",
            (id.to_string(),),
            |row| Ok((row.get::<String>(0)?, row.get::<String>(1)?)),
        )
        .await
        .map_err(|e| e.to_string())?;
    let Some((json, run_id)) = row else {
        return Ok(None);
    };
    let mut snapshot: Snapshot = serde_json::from_str(&json).map_err(|e| e.to_string())?;
    // Scalar row references are rekeyed with the project. Serialized identity is
    // historical context, never routing authority after that move.
    snapshot.run = crate::mcp::handlers::run_context::lookup_run_by_id(db, &run_id).await?;
    snapshot.logical.project_id = snapshot.run.project_id.clone();
    Ok(Some(snapshot))
}

pub(super) async fn save(db: &Arc<LocalDb>, id: &str, snapshot: &Snapshot) -> Result<(), String> {
    let json = serde_json::to_string(snapshot).map_err(|e| e.to_string())?;
    db.execute("INSERT INTO write_check_waves(id,project_id,job_id,driven_by,snapshot_json,created_at_ms,run_id,tool_use_id) VALUES(?1,?2,?3,?4,?5,?6,?7,?8) ON CONFLICT(id) DO NOTHING",
        cairn_db::turso::params![id, snapshot.run.project_id.as_str(), snapshot.run.job_id.as_str(),
            requests::this_runner(), json, snapshot.opened_at_ms, snapshot.run.run_id.as_str(),
            id.strip_prefix(&format!("write:{}:",snapshot.run.job_id)).unwrap_or(id)])
        .await.map_err(|e| e.to_string())?;
    Ok(())
}

pub(super) async fn complete(
    db: &Arc<LocalDb>,
    id: &str,
    summary: Option<&str>,
) -> Result<(), String> {
    db.execute(
        "UPDATE write_check_waves SET summary=?2,completed=1 WHERE id=?1",
        cairn_db::turso::params![id, summary],
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(())
}

pub(super) async fn delivered(db: &Arc<LocalDb>, id: &str) -> Result<(), String> {
    db.execute(
        "UPDATE write_check_waves SET delivered=1 WHERE id=?1 OR (superseded=1
            AND job_id=(SELECT job_id FROM write_check_waves WHERE id=?1)
            AND created_at_ms <= (SELECT created_at_ms FROM write_check_waves WHERE id=?1))",
        (id.to_string(),),
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(())
}

pub(super) async fn superseded(db: &Arc<LocalDb>, id: &str) -> Result<(), String> {
    db.execute(
        "UPDATE write_check_waves SET superseded=1,delivered=EXISTS(
        SELECT 1 FROM write_check_waves newer WHERE newer.job_id=write_check_waves.job_id
        AND newer.created_at_ms>write_check_waves.created_at_ms AND newer.delivered=1)
        WHERE id=?1",
        (id.to_string(),),
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(())
}

#[derive(serde::Serialize, serde::Deserialize)]
pub(super) struct Rekey {
    keyed: Vec<(CheckPlan, String)>,
    tree_hash: String,
    invalidated: Vec<usize>,
    transport: Vec<(String, String)>,
}

impl Rekey {
    pub(super) fn capture(fixed: &FixedWave) -> Self {
        Self {
            keyed: fixed.keyed.clone(),
            tree_hash: fixed.tree_hash.clone(),
            invalidated: fixed.invalidated.clone(),
            transport: fixed
                .process_env
                .iter()
                .filter(|(name, _)| {
                    CHECK_CONTENT_TRANSPORTS
                        .iter()
                        .any(|(content, _)| name == content)
                })
                .cloned()
                .collect(),
        }
    }

    pub(super) fn restore(self, cell_env: &[(String, String)]) -> FixedWave {
        FixedWave {
            keyed: self.keyed,
            tree_hash: self.tree_hash,
            invalidated: self.invalidated,
            process_env: write_check_transport_env(cell_env, self.transport),
        }
    }
}

pub(super) async fn rekey(
    db: &Arc<LocalDb>,
    id: &str,
    index: usize,
) -> Result<Option<Rekey>, String> {
    db.query_opt_text(
        "SELECT json_extract(rekeys_json,?2) FROM write_check_waves WHERE id=?1",
        cairn_db::turso::params![id, format!("$.\"{index}\"")],
    )
    .await
    .map_err(|e| e.to_string())?
    .map(|json| serde_json::from_str(&json).map_err(|e| e.to_string()))
    .transpose()
}

pub(super) async fn save_rekey(
    db: &Arc<LocalDb>,
    id: &str,
    index: usize,
    fixed: &FixedWave,
) -> Result<(), String> {
    let json = serde_json::to_string(&Rekey::capture(fixed)).map_err(|e| e.to_string())?;
    db.execute(
        "UPDATE write_check_waves SET rekeys_json=json_set(rekeys_json,?2,json(?3)) WHERE id=?1",
        cairn_db::turso::params![id, format!("$.\"{index}\""), json],
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(())
}

pub(super) async fn convergence(db: &Arc<LocalDb>, id: &str) -> Result<Option<Vec<usize>>, String> {
    db.query_opt_text(
        "SELECT convergence_json FROM write_check_waves WHERE id=?1",
        (id.to_string(),),
    )
    .await
    .map_err(|e| e.to_string())?
    .map(|json| serde_json::from_str(&json).map_err(|e| e.to_string()))
    .transpose()
}

pub(super) async fn save_convergence(
    db: &Arc<LocalDb>,
    id: &str,
    lanes: &[usize],
) -> Result<(), String> {
    let json = serde_json::to_string(lanes).map_err(|e| e.to_string())?;
    db.execute(
        "UPDATE write_check_waves SET convergence_json=?2 WHERE id=?1 AND convergence_json IS NULL",
        cairn_db::turso::params![id, json],
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(())
}

pub(super) async fn save_verdicts(
    db: &Arc<LocalDb>,
    id: &str,
    verdicts: &[(String, bool)],
) -> Result<(), String> {
    let json = serde_json::to_string(verdicts).map_err(|e| e.to_string())?;
    db.execute(
        "UPDATE write_check_waves SET verdicts_json=?2 WHERE id=?1",
        cairn_db::turso::params![id, json],
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(())
}

pub(super) async fn rearm(orch: &Orchestrator) {
    for db in orch.db.all_dbs().await {
        let rows = match db.query_all(
            "SELECT w.id,w.tool_use_id,w.completed,w.summary,w.verdicts_json,w.superseded,w.delivered,w.created_at_ms
             FROM write_check_waves w WHERE w.driven_by=?1 AND EXISTS(
                 SELECT 1 FROM write_check_waves pending WHERE pending.job_id=w.job_id
                   AND pending.driven_by=?1 AND pending.delivered=0 AND pending.created_at_ms<=w.created_at_ms)
             ORDER BY w.created_at_ms,w.id",
            (requests::this_runner(),),
            |row| Ok((row.get::<String>(0)?, row.get::<String>(1)?, row.get::<i64>(2)?, row.get::<Option<String>>(3)?, row.get::<Option<String>>(4)?, row.get::<i64>(5)?, row.get::<i64>(6)?, row.get::<i64>(7)?))
        ).await {
            Ok(rows) => rows,
            Err(error) => { log::error!("write-wave recovery scan failed: {error}"); continue; }
        };
        // Restore ordering before any recovered wave can finish and supersede another.
        let mut recovered = Vec::new();
        let mut evidence = Vec::new();
        for (id, tool, completed, summary, verdicts, superseded, delivered, order) in rows {
            let driver = if delivered != 0 {
                None
            } else {
                let Some(driver) = DriverGuard::acquire(&db, &id) else {
                    continue;
                };
                Some(driver)
            };
            match load(&db, &id).await {
                Ok(Some(snapshot)) => {
                    let ticket = Arc::new(orch.late_write_checks.open_recovered(
                        &snapshot.run.job_id,
                        &tool,
                        order,
                    ));
                    ticket.planned(&snapshot.planned.sealed_commit);
                    if let Some(verdicts) = verdicts {
                        match serde_json::from_str::<Vec<(String, bool)>>(&verdicts) {
                            Ok(verdicts) => {
                                ticket.recorded(verdicts.iter().map(|(name, passed)| {
                                    (
                                        name.as_str(),
                                        if *passed {
                                            crate::execution::late_write_checks::LaneVerdict::Passed
                                        } else {
                                            crate::execution::late_write_checks::LaneVerdict::Failed
                                        },
                                    )
                                }))
                            }
                            Err(error) => {
                                log::error!(
                                    "write-wave {id} verdict aggregation unreadable: {error}"
                                );
                                continue;
                            }
                        }
                    }
                    if delivered != 0 {
                        evidence.push(ticket);
                        continue;
                    }
                    recovered.push((
                        id,
                        snapshot,
                        ticket,
                        completed != 0,
                        summary,
                        driver.expect("pending waves reserve their coordinator"),
                        superseded != 0,
                    ));
                }
                Ok(None) => {}
                Err(error) => log::error!("write-wave {id} snapshot unreadable: {error}"),
            }
        }
        for ticket in &evidence {
            ticket.restore_finished();
        }
        let evidence = Arc::new(evidence);
        for (_, snapshot, ticket, completed, summary, _, superseded) in &recovered {
            if *completed && *superseded {
                if let Some(summary) = summary {
                    deliver_late_write_check_summary(
                        orch.clone(),
                        snapshot.run.run_id.clone(),
                        ticket.clone(),
                        summary.clone(),
                    )
                    .await;
                }
            }
        }
        for (id, snapshot, ticket, completed, summary, driver, superseded) in recovered {
            if completed && superseded {
                continue;
            }
            let orch = orch.clone();
            let db = db.clone();
            let evidence = evidence.clone();
            cairn_common::task::spawn(async move {
                let _ordering_evidence = evidence;
                let _driver = driver;
                let summary = if completed {
                    summary
                } else {
                    let guard = WriteChecksInFlightGuard::new(&orch, &snapshot.run.job_id);
                    let claims = orch.lane_claims.open_wave(&snapshot.run.job_id);
                    let (progress, _) = tokio::sync::watch::channel(WaveProgress::default());
                    let result = run_owned_write_wave(
                        &orch,
                        Some(&snapshot.run),
                        &snapshot.cwd,
                        Some(&snapshot.logical),
                        progress,
                        claims,
                        &ticket,
                        snapshot.only_suite.as_deref(),
                    )
                    .await;
                    drop(guard);
                    if let Err(error) = complete(&db, &id, result.as_deref()).await {
                        log::error!("write-wave {id} completion checkpoint failed: {error}");
                        return;
                    }
                    result
                };
                if let Some(summary) = summary {
                    deliver_late_write_check_summary(orch, snapshot.run.run_id, ticket, summary)
                        .await;
                } else if let Err(error) = delivered(&db, &id).await {
                    log::error!("write-wave {id} empty completion checkpoint failed: {error}");
                }
            });
        }
    }
}
