//! A node scratch's end, on every machine that held it.
//!
//! Closing a node (its issue resolves, or its thread closes) closes its
//! scratch: the row is stamped `closed_at`, the runner's store is deleted, and
//! every connected machine holding a replica is told to remove it. The row
//! stays as a tombstone until each of those machines acknowledges, and a
//! machine that was offline is told the next time its advertisement names the
//! closed scratch. An unknown scratch is retained: absence from this runner's
//! database is not evidence that the owner has ended.
//! A node that ended while the runner was down has its scratch closed by
//! [`close_ended`] at the next start.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use cairn_common::executor_protocol::{ExecutorMessage, NodeScratchReplica};

use super::owner::{self, NodeScratch, Replica};
use super::store;
use crate::db::DbState;
use crate::fleet::{Fleet, NodeScratchEvent};
use crate::orchestrator::object_plane::ObjectPlaneState;
use crate::orchestrator::Orchestrator;

/// What scratch lifecycle work touches. Deliberately not the orchestrator: the
/// fleet's subscriber must not keep the fleet (or anything holding it) alive.
#[derive(Clone)]
struct Lifecycle {
    db: Arc<DbState>,
    plane: Arc<ObjectPlaneState>,
    config_dir: PathBuf,
}

impl Lifecycle {
    fn of(orch: &Orchestrator) -> Self {
        Self {
            db: orch.db.clone(),
            plane: orch.object_plane.clone(),
            config_dir: orch.config_dir.clone(),
        }
    }
}

/// Subscribe the fleet's scratch events to this module, once per orchestrator.
/// Called when a grant is issued and at host start, so a machine that
/// reconnects with replicas before any work arrives is still reconciled.
pub fn ensure_subscribed(orch: &Orchestrator) {
    if orch
        .object_plane
        .scratch
        .subscribed()
        .swap(true, Ordering::SeqCst)
    {
        return;
    }
    let lifecycle = Lifecycle::of(orch);
    orch.fleet
        .subscribe_node_scratch_events(move |fleet, event| {
            // Granted as the event is emitted, not in the reconcile spawned
            // below: an attach emits it before the runner sends `Ready`, and
            // the executor retries the write-backs it owes the moment Ready's
            // credential arrives, which a spawned grant could trail. A grant
            // for a closed or unknown scratch confers nothing, because every
            // scratch route refuses one. Reconciliation deletes only known
            // closed scratches; unknown replicas remain untouched.
            if let NodeScratchEvent::Advertised {
                executor_id,
                generation,
                replicas,
            } = &event
            {
                for replica in replicas {
                    lifecycle.plane.scratch.authorize(
                        executor_id,
                        *generation,
                        &replica.scratch_id,
                    );
                }
            }
            let Ok(runtime) = tokio::runtime::Handle::try_current() else {
                log::warn!("node scratch event arrived outside a runtime; dropped: {event:?}");
                return;
            };
            let lifecycle = lifecycle.clone();
            let fleet = fleet.clone();
            runtime.spawn(async move { handle_event(&lifecycle, &fleet, event).await });
        });
}

async fn handle_event(lifecycle: &Lifecycle, fleet: &Fleet, event: NodeScratchEvent) {
    match event {
        NodeScratchEvent::Advertised {
            executor_id,
            generation,
            replicas,
        } => reconcile_advertised(lifecycle, fleet, &executor_id, generation, &replicas).await,
        NodeScratchEvent::Deleted {
            executor_id,
            scratch_id,
        } => {
            let _ = owner::forget_replica(&lifecycle.db.local, &scratch_id, &executor_id).await;
            retire_tombstone(lifecycle, &scratch_id).await;
        }
    }
}

/// Bring the authority in line with what a machine says it holds.
///
/// A replica of an open scratch has its state recorded; the connection was
/// granted it when the advertisement arrived (see [`ensure_subscribed`]), so a
/// dirty replica can finish its write-back after a link bounce. A replica of a
/// known closed scratch is told to go; an unknown one is left alone.
async fn reconcile_advertised(
    lifecycle: &Lifecycle,
    fleet: &Fleet,
    executor_id: &str,
    generation: u64,
    replicas: &[NodeScratchReplica],
) {
    let local = &lifecycle.db.local;
    for replica in replicas {
        let scratch = match owner::load(local, &replica.scratch_id).await {
            Ok(scratch) => scratch,
            Err(error) => {
                log::warn!(
                    "reconcile node scratch {} on {executor_id}: {error}",
                    replica.scratch_id
                );
                continue;
            }
        };
        let Some(scratch) = scratch else {
            log::warn!(
                "leaving unknown node scratch {} on {executor_id} generation {generation} untouched; authority {} has no closure record",
                replica.scratch_id,
                lifecycle.config_dir.display()
            );
            continue;
        };
        if scratch.closed_at.is_some() {
            log::info!(
                "authority {} requesting deletion of closed node scratch {} of {} on {executor_id} generation {generation}",
                lifecycle.config_dir.display(), scratch.id, scratch.owner_uri
            );
            fleet.send_to_connected(
                executor_id,
                ExecutorMessage::DeleteNodeScratch {
                    scratch_id: replica.scratch_id.clone(),
                },
            );
            continue;
        }
        let known = owner::replicas(local, &scratch.id)
            .await
            .unwrap_or_default()
            .into_iter()
            .find(|known| known.executor_id == executor_id);
        // The monotonic byte count at the commit this machine says it holds:
        // known exactly when that is the head or what was last recorded.
        let synced_written_bytes = match (&known, &replica.synced_commit) {
            (Some(known), Some(synced)) if known.synced_commit.as_ref() == Some(synced) => {
                known.synced_written_bytes
            }
            (_, Some(synced)) if scratch.head_commit.as_ref() == Some(synced) => {
                scratch.written_bytes
            }
            (Some(known), _) => known.synced_written_bytes,
            _ => 0,
        };
        let _ = owner::record_replica(
            local,
            &Replica {
                scratch_id: scratch.id.clone(),
                executor_id: executor_id.to_owned(),
                synced_commit: replica.synced_commit.clone(),
                synced_written_bytes,
                dirty: replica.dirty,
                refused_bytes: if replica.dirty {
                    known.and_then(|known| known.refused_bytes)
                } else {
                    None
                },
            },
        )
        .await;
    }
}

/// Drop a closed scratch's row once no machine holds a replica.
async fn retire_tombstone(lifecycle: &Lifecycle, scratch_id: &str) {
    let local = &lifecycle.db.local;
    let remaining = owner::replicas(local, scratch_id)
        .await
        .map(|replicas| replicas.len())
        .unwrap_or(usize::MAX);
    if remaining == 0 {
        let _ = local
            .execute(
                "DELETE FROM node_scratches WHERE id = ?1 AND closed_at IS NOT NULL",
                (scratch_id.to_owned(),),
            )
            .await;
    }
}

/// Close one scratch everywhere.
pub async fn close_scratch(orch: &Orchestrator, scratch: &NodeScratch) {
    ensure_subscribed(orch);
    let lifecycle = Lifecycle::of(orch);
    let local = &lifecycle.db.local;
    let now = chrono::Utc::now().timestamp_millis();
    if let Err(error) = local
        .execute(
            "UPDATE node_scratches SET closed_at = ?2 WHERE id = ?1 AND closed_at IS NULL",
            (scratch.id.clone(), now),
        )
        .await
    {
        log::warn!("close node scratch {}: {error}", scratch.id);
        return;
    }
    lifecycle.plane.scratch.revoke_scratch(&scratch.id);
    let path = store::store_path(&lifecycle.config_dir, &scratch.project_id, &scratch.id);
    match tokio::task::spawn_blocking(move || store::delete(&path)).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => log::warn!("delete node scratch store {}: {error}", scratch.id),
        Err(error) => log::warn!("delete node scratch store {}: {error}", scratch.id),
    }
    // Every machine the authority knows of, and every connected machine that
    // advertises one: a replica exists from its first sync, before any
    // write-back records it.
    let mut holders: Vec<String> = owner::replicas(local, &scratch.id)
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|replica| replica.executor_id)
        .collect();
    for advertised in orch.fleet.node_scratch_replicas(&scratch.id) {
        if !holders.contains(&advertised.executor_id) {
            holders.push(advertised.executor_id.clone());
            let _ = owner::record_replica(
                local,
                &Replica {
                    scratch_id: scratch.id.clone(),
                    executor_id: advertised.executor_id,
                    synced_commit: advertised.replica.synced_commit,
                    synced_written_bytes: 0,
                    dirty: advertised.replica.dirty,
                    refused_bytes: None,
                },
            )
            .await;
        }
    }
    for executor_id in &holders {
        log::info!(
            "authority {} requesting deletion of closed node scratch {} of {} on {executor_id}",
            lifecycle.config_dir.display(),
            scratch.id,
            scratch.owner_uri
        );
        orch.fleet.send_to_connected(
            executor_id,
            ExecutorMessage::DeleteNodeScratch {
                scratch_id: scratch.id.clone(),
            },
        );
    }
    retire_tombstone(&lifecycle, &scratch.id).await;
}

/// Close the scratches of an issue's nodes, as its resolution tears it down.
pub async fn close_for_issue(orch: &Orchestrator, issue_id: &str) {
    match owner::open_for(&orch.db.local, Some(issue_id), None).await {
        Ok(scratches) => {
            for scratch in scratches {
                close_scratch(orch, &scratch).await;
            }
        }
        Err(error) => log::warn!("list node scratches of issue {issue_id}: {error}"),
    }
}

/// Close every open scratch whose node has ended: its issue resolved or its
/// thread closed while the runner was down, or by a path that never reached
/// [`close_for_issue`] or [`close_for_thread`]. Runs at runner start and on
/// every scratch maintenance tick. A scratch whose owner cannot be judged (a
/// team replica that is not open) stays open. Returns how many closed.
pub async fn close_ended(orch: &Orchestrator) -> usize {
    let open = match owner::open_all(&orch.db.local).await {
        Ok(open) => open,
        Err(error) => {
            log::warn!("list open node scratches: {error}");
            return 0;
        }
    };
    let mut closed = 0;
    for scratch in open {
        if let owner::OwnerFate::Ended(reason) = owner::owner_fate(
            &orch.db,
            scratch.issue_id.as_deref(),
            scratch.thread_id.as_deref(),
        )
        .await
        {
            log::info!(
                "closing node scratch {} of {}: {reason}",
                scratch.id,
                scratch.owner_uri
            );
            close_scratch(orch, &scratch).await;
            closed += 1;
        }
    }
    closed
}

/// Close every scratch of a project, as the project is deleted.
pub async fn close_for_project(orch: &Orchestrator, project_id: &str) {
    match owner::open_in_project(&orch.db.local, project_id).await {
        Ok(scratches) => {
            for scratch in scratches {
                close_scratch(orch, &scratch).await;
            }
        }
        Err(error) => log::warn!("list node scratches of project {project_id}: {error}"),
    }
}

/// Close a thread's scratch, as the thread closes. A reopened thread gets a
/// fresh one.
pub async fn close_for_thread(orch: &Orchestrator, thread_id: &str) {
    match owner::open_for(&orch.db.local, None, Some(thread_id)).await {
        Ok(scratches) => {
            for scratch in scratches {
                close_scratch(orch, &scratch).await;
            }
        }
        Err(error) => log::warn!("list node scratches of thread {thread_id}: {error}"),
    }
}

/// The subscription flag, kept with the plane state so one orchestrator
/// subscribes once however many clones of it issue grants.
#[derive(Clone, Default)]
pub struct Subscribed(Arc<AtomicBool>);

impl Subscribed {
    pub fn swap(&self, value: bool, ordering: Ordering) -> bool {
        self.0.swap(value, ordering)
    }
}
