//! Child attention: which node currently coordinates a child issue.
//!
//! A child issue's question, permission, review, resolved, and message facts are
//! addressed to whichever node is **currently driving the parent issue**. That
//! recipient is derived from the parent edge on every wake and is never
//! materialized as a subscription row.
//!
//! Materializing it is what CAIRN-3293 removed. A row minted at parent-edge
//! creation named whichever job coordinated the parent at that instant, and
//! nothing ever moved it: starting a new coordinator execution minted nothing,
//! so the live coordinator silently missed every child gate, while the retired
//! coordinator kept its row and kept waking for children it no longer owned.
//! Both directions were one bug — a cache of a fact that changes without the
//! cache being told.
//!
//! The scope of the watch is fixed (the child issue, plus
//! [`DEFAULT_CHILD_FACT_KINDS`]); only the recipient varies, so the recipient is
//! the only thing resolved live. A node that wants to opt out writes an explicit
//! row (`mute` or `unsubscribe`), and an explicit row always wins over the
//! derived watch — see [`subscriptions_governing_issue`].

use cairn_db::turso::params;

use crate::storage::{DbResult, LocalDb, RowExt};

use super::types::*;

/// The node currently driving `child_issue_uri`'s parent issue, or `None` when
/// the issue has no parent edge (a root issue, or one orphaned by a patch).
///
/// "Currently driving" is the newest non-failed **recipe-root** job on the
/// parent issue that has actually run (`current_session_id IS NOT NULL`).
/// Recipe-root (`jobs.parent_job_id IS NULL`) excludes the coordinator's own
/// delegated sub-task jobs, which is the mis-pick that originally motivated
/// snapshotting the spawning job in CAIRN-1302; newest-first is what makes a new
/// coordinator execution take over the parent's children without any hand-off
/// step.
pub async fn coordinating_job_for_child_issue(
    db: &LocalDb,
    child_issue_uri: &str,
) -> Result<Option<String>, String> {
    let child_issue_uri = child_issue_uri.to_string();
    db.read(|conn| {
        let child_issue_uri = child_issue_uri.clone();
        Box::pin(async move {
            let Some(child) =
                crate::issues::relations::resolve_issue_uri(conn, &child_issue_uri).await?
            else {
                return Ok(None);
            };
            let Some(parent_issue_id) = parent_issue_id(conn, &child.issue_id).await? else {
                return Ok(None);
            };
            coordinating_job_for_issue(conn, &parent_issue_id).await
        })
    })
    .await
    .map_err(|error| format!("Failed to resolve the coordinating job for a child issue: {error}"))
}

/// The jobs that watch `issue_uri` — the recipient set for every issue-sourced
/// attention push (question, permission, review, resolved).
///
/// The derived parent-axis coordinator plus every job holding an explicit
/// non-`unsubscribed` row. Order is explicit rows first, then the derived
/// coordinator, deduplicated by job.
pub async fn watcher_jobs_for_issue(db: &LocalDb, issue_uri: &str) -> Result<Vec<String>, String> {
    let mut jobs: Vec<String> = Vec::new();
    for subscription in subscriptions_governing_issue(db, issue_uri).await? {
        if subscription.state == WakeSubscriptionState::Unsubscribed
            || jobs.contains(&subscription.job_id)
        {
            continue;
        }
        jobs.push(subscription.job_id);
    }
    Ok(jobs)
}

/// Every subscription governing `issue_uri`: the persisted rows for that source,
/// plus the derived parent-axis watch when the coordinating node holds no row of
/// its own.
///
/// A node's own explicit row is authoritative for that node, whatever its state
/// or fact scope. That is what makes `mute` and `unsubscribe` work against a
/// derived watch: a narrowed row cannot be widened back by the default, and an
/// `unsubscribed` row keeps the derivation from re-adding the node.
pub(super) async fn subscriptions_governing_issue(
    db: &LocalDb,
    issue_uri: &str,
) -> Result<Vec<WakeSubscription>, String> {
    let mut subscriptions =
        super::matching::subscriptions_for_source(db, SOURCE_KIND_ISSUE, Some(issue_uri)).await?;
    let Some(coordinator) = coordinating_job_for_child_issue(db, issue_uri).await? else {
        return Ok(subscriptions);
    };
    if subscriptions
        .iter()
        .any(|subscription| subscription.job_id == coordinator)
    {
        return Ok(subscriptions);
    }
    subscriptions.push(derived_child_subscription(&coordinator, issue_uri));
    Ok(subscriptions)
}

/// The child issues `job_id` currently coordinates, as canonical issue URIs.
///
/// The inverse of [`coordinating_job_for_child_issue`], for the `/wakes` read
/// projection: since the derived watch has no row, this is the only way a node
/// can see the child attention it is subscribed to. Empty when the job is not
/// the current coordinator of its own issue (a superseded execution's node) or
/// when that issue has no children.
pub async fn coordinated_child_issue_uris_for_job(
    db: &LocalDb,
    job_id: &str,
) -> Result<Vec<String>, String> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let Some(issue_id) = job_issue_id(conn, &job_id).await? else {
                return Ok(Vec::new());
            };
            if coordinating_job_for_issue(conn, &issue_id).await? != Some(job_id) {
                return Ok(Vec::new());
            }
            let mut rows = conn
                .query(
                    "SELECT p.key, i.number
                     FROM issues i
                     JOIN projects p ON p.id = i.project_id
                     WHERE i.parent_issue_id = ?1
                     ORDER BY i.number ASC",
                    params![issue_id.as_str()],
                )
                .await?;
            let mut uris = Vec::new();
            while let Some(row) = rows.next().await? {
                uris.push(cairn_common::uri::build_issue_uri(
                    &row.text(0)?,
                    row.i64(1)? as i32,
                ));
            }
            Ok(uris)
        })
    })
    .await
    .map_err(|error| format!("Failed to list coordinated child issues: {error}"))
}

async fn parent_issue_id(
    conn: &cairn_db::turso::Connection,
    issue_id: &str,
) -> DbResult<Option<String>> {
    let mut rows = conn
        .query(
            "SELECT parent_issue_id FROM issues WHERE id = ?1 LIMIT 1",
            params![issue_id],
        )
        .await?;
    match rows.next().await? {
        Some(row) => row.opt_text(0),
        None => Ok(None),
    }
}

async fn job_issue_id(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
) -> DbResult<Option<String>> {
    let mut rows = conn
        .query(
            "SELECT issue_id FROM jobs WHERE id = ?1 LIMIT 1",
            params![job_id],
        )
        .await?;
    match rows.next().await? {
        Some(row) => row.opt_text(0),
        None => Ok(None),
    }
}

async fn coordinating_job_for_issue(
    conn: &cairn_db::turso::Connection,
    issue_id: &str,
) -> DbResult<Option<String>> {
    let mut rows = conn
        .query(
            "SELECT id
             FROM jobs
             WHERE issue_id = ?1
               AND parent_job_id IS NULL
               AND status != 'failed'
               AND current_session_id IS NOT NULL
             ORDER BY created_at DESC
             LIMIT 1",
            params![issue_id],
        )
        .await?;
    crate::storage::next_text(&mut rows, 0).await
}
