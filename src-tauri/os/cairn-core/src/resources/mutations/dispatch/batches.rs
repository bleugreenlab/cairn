//! `patch {action:"cancel"}` on a node's or task's `batches/<n>`: cancel that
//! one run batch ([`crate::mcp::handlers::run::cancel`]).
//!
//! The address is the batch's own handle, by the number it took within its
//! job; a legacy address naming its request id still reaches it. A cancel
//! addressed under the wrong node is refused naming the right one, so a
//! principal never cancels a batch it did not mean.
//!
//! The operator and any thread (its sessions and their tasks) may cancel any
//! batch on the install. Other runs may cancel their own job's batch, or one
//! of a live job on the same branch. Anyone else is refused, naming who can.

use super::super::{build_failure, payload_non_empty_str, ResourceMutationResult};
use super::Writer;
use crate::mcp::handlers::run::{cancel, detached};
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;
use cairn_uri::CairnResource;

pub(super) async fn dispatch(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    resource: &CairnResource,
) -> ResourceMutationResult<Option<String>> {
    let (project, number, exec_seq, node_id, task_name, request_id) = match (resource, item.mode) {
        (
            CairnResource::NodeBatch {
                project,
                number,
                exec_seq,
                node_id,
                request_id,
            },
            ChangeMode::Patch,
        ) => (project, *number, *exec_seq, node_id, None, request_id),
        (
            CairnResource::TaskBatch {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
                request_id,
            },
            ChangeMode::Patch,
        ) => (
            project,
            *number,
            *exec_seq,
            node_id,
            Some(task_name.as_str()),
            request_id,
        ),
        _ => return Ok(None),
    };
    let payload = item
        .payload
        .as_ref()
        .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
    match payload_non_empty_str(payload, "action", &[]) {
        Some("cancel") => {}
        other => {
            return Err(build_failure(
                index,
                item,
                format!(
                    "payload.action must be 'cancel', got {}",
                    other.map_or_else(|| "nothing".to_string(), |action| format!("'{action}'"))
                ),
            ))
        }
    }
    let db = orch.db.for_project(project).await;
    let fail = |error: String| build_failure(index, item, error);
    let addressed = crate::resources::resolve_node_or_task_job_id_for_read(
        &db, project, number, exec_seq, node_id, task_name,
    )
    .await
    .map_err(fail)?;
    use cairn_db::storage::address_numbers::{number_of, resolve, Numbered};
    let segment = request_id;
    let resolved = resolve(&db, Numbered::RunBatch, Some(&addressed), segment)
        .await
        .map_err(fail)?;
    // A batch received before the ledger numbered batches is still addressed
    // by its request id, which then names it directly.
    let (request_id, legacy) = match &resolved {
        Some(resolved) => (resolved.id.clone(), resolved.legacy),
        None => (segment.clone(), false),
    };
    let request_id = &request_id;
    let shown = resolved
        .as_ref()
        .map_or_else(|| segment.clone(), |resolved| resolved.number.to_string());
    let adjusted: Vec<_> = legacy
        .then(|| {
            let canonical = match resource {
                CairnResource::NodeBatch { .. } => cairn_uri::build_node_batch_uri(
                    project,
                    number,
                    exec_seq,
                    node_id,
                    resolved.as_ref().map_or(0, |r| r.number),
                ),
                _ => cairn_uri::build_task_batch_uri(
                    project,
                    number,
                    exec_seq,
                    node_id,
                    task_name.unwrap_or_default(),
                    resolved.as_ref().map_or(0, |r| r.number),
                ),
            };
            cairn_db::storage::address_numbers::legacy_adjustment(&resource.to_uri(), &canonical)
        })
        .into_iter()
        .collect();
    let owner = batch_owner(&db, request_id).await.map_err(fail)?;
    let Some(owner) = owner else {
        return Err(fail(not_in_flight(&db, request_id, &shown).await));
    };
    if owner != addressed {
        let theirs = number_of(&db, Numbered::RunBatch, None, request_id)
            .await
            .ok()
            .flatten();
        let right = crate::jobs::queries::home_uri_for_job(&db, &owner)
            .await
            .ok()
            .flatten()
            .map(|home| {
                format!(
                    " Address it at `{home}/batches/{}`.",
                    detached::batch_segment(theirs, request_id)
                )
            })
            .unwrap_or_default();
        return Err(fail(format!(
            "Refused: run batch {shown} belongs to a different job than this address names.{right} Nothing was cancelled."
        )));
    }
    let writer = super::writer(orch, request).await.map_err(fail)?;
    let thread = super::threads::run_thread_home(orch, request).await;
    authorize(&db, &writer, thread.as_deref(), &owner, &shown)
        .await
        .map_err(fail)?;
    let principal = thread.as_deref().unwrap_or_else(|| writer.principal());
    let by = match payload.get("reason") {
        None => principal.to_string(),
        Some(serde_json::Value::String(reason)) if reason.trim().is_empty() => {
            principal.to_string()
        }
        Some(serde_json::Value::String(reason)) => {
            format!("{principal} (reason: {})", reason.trim())
        }
        Some(_) => return Err(fail("payload.reason must be a string".to_string())),
    };
    if dry_run {
        return Ok(Some(cairn_common::adjustment::with_summary(
            format!("Would cancel run batch {shown} on behalf of {by}"),
            &adjusted,
        )));
    }
    let report = cancel::cancel_batch(orch, &db, request_id, &by, cancel::CANCEL_SETTLE_WAIT).await;
    if !report.reached() {
        return Err(fail(report.sentence(&shown)));
    }
    Ok(Some(cairn_common::adjustment::with_summary(
        report.sentence(&shown),
        &adjusted,
    )))
}

/// The job a batch runs for: its live control's, its detached row's, or its
/// open run-batch wait's. `None` when no record of it is in flight.
pub(super) async fn batch_owner(db: &LocalDb, request_id: &str) -> Result<Option<String>, String> {
    if let Some(owner) = detached::live_owner(request_id) {
        return Ok(Some(owner.job_id));
    }
    if let Some(batch) = detached::load(db, request_id).await? {
        if matches!(
            batch.state,
            detached::DetachedState::Parked | detached::DetachedState::Running
        ) {
            return Ok(Some(batch.job_id));
        }
    }
    // A settled row may still have a pending wait: a lost batch whose call
    // was never resumed, which a repeated cancel finishes.
    db.query_opt_text(
        "SELECT job_id FROM agent_waits
          WHERE state IN ('pending', 'resolving')
            AND json_extract(condition_json, '$.kind') = 'run_batch'
            AND json_extract(condition_json, '$.request_id') = ?1
          LIMIT 1",
        (request_id.to_string(),),
    )
    .await
    .map_err(|error| format!("read run batch `{request_id}`: {error}"))
}

/// Why a batch with no in-flight record cannot be cancelled.
async fn not_in_flight(db: &LocalDb, request_id: &str, shown: &str) -> String {
    match detached::load(db, request_id).await {
        Ok(Some(batch)) => format!(
            "Nothing was cancelled: run batch {shown} already ended ({}).",
            batch
                .verdict
                .as_deref()
                .or(batch.lost_evidence.as_deref())
                .unwrap_or("settled")
        ),
        _ => format!(
            "Nothing was cancelled: no run batch {shown} is in flight. A batch whose call already answered has ended; one an earlier runner placed and nothing records settles from its machine's own report."
        ),
    }
}

/// Any thread on the install, the operator, the owner, or a live branch sharer.
async fn authorize(
    db: &LocalDb,
    writer: &Writer,
    thread: Option<&str>,
    owner: &str,
    shown: &str,
) -> Result<(), String> {
    let Writer::Run { job_id, .. } = writer else {
        return Ok(());
    };
    if thread.is_some()
        || job_id == owner
        || crate::orchestrator::base_advance::shares_live_branch(db, job_id, owner).await?
    {
        return Ok(());
    }
    let holder = crate::jobs::queries::home_uri_for_job(db, owner)
        .await
        .ok()
        .flatten()
        .unwrap_or_else(|| format!("job `{owner}`"));
    Err(format!(
        "Refused: run batch {shown} belongs to {holder}. Any thread (its sessions and their tasks), that node, a node sharing its branch, or the operator may cancel it; caller {} acts for no thread and owns neither this batch nor its branch. Nothing was cancelled.",
        writer.principal()
    ))
}
