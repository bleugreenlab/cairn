//! Background, one-way upgrades from mechanical era fallbacks to generated summaries.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::json;
use tokio::sync::mpsc;

use crate::orchestrator::Orchestrator;
use crate::responses::{OutputContract, ResponseCaller, ResponseDefinition, ResponseVariable};

const QUEUE_CAPACITY: usize = 64;
const SUMMARY_LIMIT: usize = 1_200;
const RESPONSE_ID: &str = "builtin:thread-era-summary";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EraSummaryJob {
    pub job_id: String,
    pub seq: i32,
}

pub type EraSummarySender = Arc<Mutex<Option<mpsc::Sender<EraSummaryJob>>>>;

pub fn sender_slot() -> EraSummarySender {
    Arc::new(Mutex::new(None))
}

/// Best-effort, non-blocking ingress used after an era has been persisted.
/// A full queue is intentionally harmless: the next process startup sweep
/// discovers every fallback row again.
pub fn enqueue(sender: &EraSummarySender, job_id: impl Into<String>, seq: i32) -> bool {
    let Ok(slot) = sender.lock() else {
        return false;
    };
    let Some(tx) = slot.as_ref() else {
        return false;
    };
    tx.try_send(EraSummaryJob {
        job_id: job_id.into(),
        seq,
    })
    .is_ok()
}

pub fn spawn(orch: Orchestrator, sender: &EraSummarySender) {
    let (tx, rx) = mpsc::channel(QUEUE_CAPACITY);
    if let Ok(mut slot) = sender.lock() {
        *slot = Some(tx.clone());
    }
    tokio::spawn(run(orch.clone(), rx));
    tokio::spawn(async move {
        for job in fallback_eras(&orch).await {
            if tx.send(job).await.is_err() {
                break;
            }
        }
    });
}

async fn fallback_eras(orch: &Orchestrator) -> Vec<EraSummaryJob> {
    let mut jobs = Vec::new();
    for db in orch.db.all_dbs().await {
        match db.query_all(
            "SELECT job_id, seq FROM thread_eras WHERE summary_status = 'fallback' ORDER BY created_at DESC, seq DESC",
            (),
            |row| Ok(EraSummaryJob { job_id: row.get(0)?, seq: row.get::<i64>(1)? as i32 }),
        ).await {
            Ok(mut found) => jobs.append(&mut found),
            Err(error) => log::warn!("thread era startup sweep failed: {error}"),
        }
    }
    jobs
}

async fn run(orch: Orchestrator, mut rx: mpsc::Receiver<EraSummaryJob>) {
    let mut pending = HashSet::new();
    while let Some(job) = rx.recv().await {
        if !pending.insert(job.clone()) {
            continue;
        }
        if let Err(error) = upgrade(&orch, &job).await {
            log::warn!(
                "thread era summary upgrade failed for {}#{}: {error}",
                job.job_id,
                job.seq
            );
        }
        pending.remove(&job);
    }
}

async fn upgrade(orch: &Orchestrator, job: &EraSummaryJob) -> Result<(), String> {
    let db = crate::execution::routing::owning_db_for_job(&orch.db, &job.job_id)
        .await
        .map_err(|error| error.to_string())?;
    let Some(era) = crate::threads::eras::get_era(&db, &job.job_id, job.seq)
        .await
        .map_err(|error| error.to_string())?
    else {
        return Ok(());
    };
    if era.summary_status != "fallback" {
        return Ok(());
    }
    let source =
        crate::resources::era_source_digest(&db, &job.job_id, era.start_block, era.end_block)
            .await?;
    let children = crate::threads::eras::era_children(&db, &job.job_id, job.seq)
        .await
        .map_err(|error| error.to_string())?;
    let child_context = children
        .iter()
        .map(|child| {
            format!(
                "{} — {} ({})",
                child.child_issue_uri, child.child_title, child.final_status
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    let outcome = crate::responses::invoke_definition(
        orch,
        &definition(),
        &json!({"source": source, "children": child_context}),
        RESPONSE_ID,
        ResponseCaller::Internal {
            label: format!("thread era {}#{}", job.job_id, job.seq),
        },
    )
    .await
    .map_err(|error| error.to_string())?;
    let summary = outcome.text.trim();
    if summary.is_empty() {
        return Err("summary completion returned empty text".into());
    }
    let summary = &summary[..summary.floor_char_boundary(SUMMARY_LIMIT)];
    let project_db = match project_key_for_job(&db, &job.job_id).await {
        Some(key) => orch.db.for_project(&key).await,
        None => db.clone(),
    };
    let upgraded = record_generated_summary(
        &db,
        &project_db,
        job,
        summary,
        &outcome.model,
        &orch.anon_device_manager.device_id(),
        orch.services.clock.now(),
    )
    .await?;
    if upgraded {
        orch.notify_tenet_evidence();
    }
    Ok(())
}

/// Upgrade a fallback era to its generated summary, and snapshot the summary
/// as a tenet evidence event owned by `device`. Reports whether this call
/// upgraded the era; a lost compare-and-swap records nothing.
///
/// The event belongs in the project's routed database, where the tenets it is
/// judged against live. `thread_eras` is private, so an era only ever persists
/// where its thread's project routes to the private database and the two
/// handles are the same; the event then commits with the upgrade. Should they
/// ever differ, the event follows the upgrade's commit instead.
pub(crate) async fn record_generated_summary(
    db: &Arc<crate::storage::LocalDb>,
    project_db: &Arc<crate::storage::LocalDb>,
    job: &EraSummaryJob,
    summary: &str,
    model: &str,
    device: &str,
    now: i64,
) -> Result<bool, String> {
    let same_db = Arc::ptr_eq(project_db, db);
    let summary = summary.to_string();
    let device = device.to_string();
    let job_id = job.job_id.clone();
    let seq = job.seq;
    let model = model.to_string();
    let (changed, deferred) = db
        .write(move |conn| {
            let job_id = job_id.clone();
            let summary = summary.clone();
            let model = model.clone();
            let device = device.clone();
            Box::pin(async move {
                let changed = conn.execute(
                    "UPDATE thread_eras SET summary = ?1, summary_status = 'generated', summary_model = ?2, summarized_at = ?3
                     WHERE job_id = ?4 AND seq = ?5 AND summary_status = 'fallback'",
                    cairn_db::turso::params![summary.as_str(), model.as_str(), chrono::Utc::now().timestamp_millis(), job_id.as_str(), seq],
                ).await?;
                if changed > 1 {
                    return Err(crate::storage::DbError::internal(
                        "era upgrade changed more than one row".to_string(),
                    ));
                }
                if changed == 0 {
                    return Ok((0, None));
                }
                let event = crate::tenets::evidence::events::era_event_conn(
                    conn, &job_id, seq, &summary, &device, now,
                )
                .await?;
                match event {
                    Some(event) if same_db => {
                        crate::tenets::evidence::events::insert_event_conn(conn, &event).await?;
                        Ok((changed, None))
                    }
                    event => Ok((changed, event)),
                }
            })
        })
        .await
        .map_err(|error| error.to_string())?;
    if let Some(event) = deferred {
        let inserted = project_db
            .write(move |conn| {
                let event = event.clone();
                Box::pin(async move {
                    crate::tenets::evidence::events::insert_event_conn(conn, &event).await
                })
            })
            .await;
        if let Err(error) = inserted {
            log::warn!(
                "tenet evidence event for era {}#{} was not recorded: {error}",
                job.job_id,
                job.seq
            );
        }
    }
    Ok(changed == 1)
}

async fn project_key_for_job(db: &crate::storage::LocalDb, job_id: &str) -> Option<String> {
    db.query_opt(
        "SELECT p.key FROM jobs j JOIN projects p ON p.id = j.project_id WHERE j.id = ?1",
        (job_id.to_string(),),
        |row| crate::storage::RowExt::text(row, 0),
    )
    .await
    .ok()
    .flatten()
}

fn definition() -> ResponseDefinition {
    ResponseDefinition {
        name: "Thread era summary".into(),
        description: "Summarize one folded span of a durable thread".into(),
        tier: Some("sm".into()), model: None, backend: None, options: HashMap::new(),
        variables: vec![
            ResponseVariable { name: "source".into(), description: None, required: true, default: None, values: vec![] },
            ResponseVariable { name: "children".into(), description: None, required: false, default: Some(json!("None")), values: vec![] },
        ],
        output: OutputContract::Named("text".into()), timeout: Duration::from_secs(90), examples: vec![],
        template: "Write one short paragraph (maximum 1,200 characters) summarizing this folded thread era. State what it was about, what settled, and which child issues resolved. Preserve every cairn:// issue URI mentioned. Do not add a heading or bullets.\n\nResolved children:\n{{children}}\n\nTranscript:\n{{source}}".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn built_in_definition_is_valid() {
        definition().validate().unwrap();
    }
}
