//! The portable node scratch (cairn/5253, tenet #1258).
//!
//! Every node owns one scratch: a thread, an issue node, and the sub-agent
//! tasks beneath it, which share their parent's. It is the node's `$TMPDIR` on
//! whatever machine its work lands, and it lives until the node closes.
//!
//! - [`owner`] resolves a job to its scratch and keeps the authority rows:
//!   `node_scratches.head_commit` is what the scratch holds, advanced by
//!   compare-and-swap, and `node_scratch_replicas` records which machines hold a
//!   copy and how current it is.
//! - [`store`] is the content: an isolated bare Git store per scratch on this
//!   runner, whose one ref is derived from the row.
//! - [`merge`] folds a write-back into the head path by path.
//! - [`plane`] serves the object channel's `scratch/*` routes every machine
//!   syncs through.
//! - [`lifecycle`] deletes a scratch everywhere when its node closes.

pub mod holders;
pub mod lifecycle;
pub mod merge;
pub mod owner;
pub mod placement;
pub mod plane;
pub mod report;
pub mod store;

#[cfg(test)]
mod sync_tests;

pub use owner::{resolve_scratch, NodeScratch, ResolvedScratch};

use cairn_common::executor_protocol::NodeScratchGrant;

use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;

/// The grant a run's job's work carries, resolved through the database the run
/// routes to, or the runner's own when there is no run.
pub async fn grant_for_run(
    orch: &Orchestrator,
    run_id: Option<&str>,
    job_id: &str,
) -> Option<(NodeScratchGrant, ResolvedScratch, std::sync::Arc<LocalDb>)> {
    let db = match run_id {
        Some(run_id) => crate::execution::routing::routing_db_for_id(&orch.db, run_id)
            .await
            .unwrap_or_else(|_| orch.db.local.clone()),
        None => orch.db.local.clone(),
    };
    let (grant, resolved) = grant_for_job(orch, &db, job_id).await?;
    Some((grant, resolved, db))
}

/// The grant a job's work carries, through the database its run routes to.
/// `None` for a job that belongs to no node, or whose scratch could not be
/// resolved (logged): that work runs with a private `$TMPDIR`, never refused.
pub async fn grant_for_job(
    orch: &Orchestrator,
    db: &LocalDb,
    job_id: &str,
) -> Option<(NodeScratchGrant, ResolvedScratch)> {
    lifecycle::ensure_subscribed(orch);
    match resolve_scratch(orch, db, job_id).await {
        Ok(Some(resolved)) => Some((resolved.scratch.grant(resolved.ceiling_bytes), resolved)),
        Ok(None) => None,
        Err(error) => {
            log::warn!("could not resolve the node scratch of job {job_id}: {error}");
            None
        }
    }
}
