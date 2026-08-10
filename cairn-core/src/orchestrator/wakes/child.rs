//! Child attention: which node currently coordinates a child issue.
//!
//! A child issue's question, permission, review, resolved, and message facts are
//! addressed to the node that **spawned** the child — the node that filed it owns
//! what it filed — falling back to whichever node is currently driving the parent
//! issue when there is no usable spawner.
//!
//! Both layers are resolved live, on every wake. `issues.parent_job_id` is a
//! durable fact (which node created this child), never a cached route: it names
//! the recipient only while it is still a valid target — live, sitting on the
//! child's *current* parent issue, and not stranded on a superseded execution.
//! That validation is what keeps CAIRN-3293's staleness out. Its two failure
//! modes are exactly the two checks: a coordinator whose execution has been
//! superseded stops receiving, and a job that filed a child under someone else's
//! parent never receives at all.
//!
//! Dropping the spawner entirely (CAIRN-3293's fix) traded that staleness for a
//! worse mis-delivery: `create_jobs_for_execution` mints every node of a graph in
//! one pass with one shared `created_at`, so on any multi-agent recipe
//! (plan>coordinator, planbuild) a pure newest-first derivation resolved the tie
//! to whichever recipe-root sorted first — the upstream planner, not the
//! coordinator that actually spawned the children.
//!
//! The scope of the watch is fixed (the child issue, plus
//! [`DEFAULT_CHILD_FACT_KINDS`]); only the recipient varies, so the recipient is
//! the only thing resolved live. A node that wants to opt out writes an explicit
//! row (`mute` or `unsubscribe`), and an explicit row always wins over the
//! derived watch — see [`subscriptions_governing_issue`].

use cairn_db::turso::params;

use crate::storage::{DbResult, LocalDb, RowExt};

use super::types::*;

/// The node that currently owns `child_issue_uri`'s attention, or `None` when
/// the issue has no parent edge (a root issue, or one orphaned by a patch) and
/// no node is driving it.
///
/// See [`coordinating_job_for_child`] for the resolution rule; this is its
/// URI-addressed entry point.
pub async fn coordinating_job_for_child_issue(
    db: &LocalDb,
    child_issue_uri: &str,
) -> Result<Option<String>, String> {
    let child_issue_uri = child_issue_uri.to_string();
    db.write(|conn| {
        let child_issue_uri = child_issue_uri.clone();
        Box::pin(async move {
            let Some(child) =
                crate::issues::relations::resolve_issue_uri(conn, &child_issue_uri).await?
            else {
                return Ok(None);
            };
            coordinating_job_for_child(conn, &child.issue_id).await
        })
    })
    .await
    .map_err(|error| format!("Failed to resolve the coordinating job for a child issue: {error}"))
}

/// The canonical child-attention recipient for `child_issue_id`.
///
/// Layer 1 — the **spawning node** recorded in `issues.parent_job_id`, when it is
/// still a valid target ([`validated_spawning_job`]).
///
/// Layer 2 — no spawner recorded (a human-created child, a legacy row, an
/// adopted issue), or the recorded one is no longer valid: the node currently
/// driving the parent issue ([`coordinating_job_for_issue`]).
///
/// A child linked to a thread instead resolves to that thread's canonical live
/// session, establishing one transactionally when the thread has none. A CLOSED
/// thread resolves to no coordinator at all: closure makes the thread dormant,
/// not the child issue, so the child's fact is still generated and still reaches
/// its explicit and issue-owned watchers — there is simply no derived recipient
/// on the thread axis until the thread is reopened.
///
/// Every child-attention path resolves through here — wake routing, the `/wakes`
/// projection, and `parent_wake::load_parent_job` — so the rule is stated once.
pub(crate) async fn coordinating_job_for_child(
    conn: &cairn_db::turso::Connection,
    child_issue_id: &str,
) -> DbResult<Option<String>> {
    let Some((spawning_job_id, parent_issue_id, parent_thread_id)) =
        parent_edge(conn, child_issue_id).await?
    else {
        return Ok(None);
    };
    if let Some(parent_issue_id) = parent_issue_id {
        if let Some(spawning_job_id) = spawning_job_id {
            if let Some(job_id) =
                validated_spawning_job(conn, &spawning_job_id, &parent_issue_id).await?
            {
                return Ok(Some(job_id));
            }
        }
        return coordinating_job_for_issue(conn, &parent_issue_id).await;
    }
    match parent_thread_id {
        Some(thread_id) => {
            // Asked before establishing rather than caught after: a closed thread
            // is "no derived coordinator", which routing drops quietly, and not a
            // routing failure, which it would log and treat as breakage.
            if !crate::threads::thread_status_conn(conn, &thread_id)
                .await?
                .is_some_and(|status| status.is_active())
            {
                return Ok(None);
            }
            // A child fact carries no model choice; a session established on this
            // path launches with the thread definition's default.
            crate::threads::ensure_thread_session_conn(conn, &thread_id, None)
                .await
                .map(Some)
        }
        None => Ok(None),
    }
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
/// The exact inverse of [`coordinating_job_for_child`], for the `/wakes` read
/// projection: since the derived watch has no row, this is the only way a node
/// can see the child attention it is subscribed to. It asks the same question of
/// each child of the job's own issue or thread, so a node sees precisely the
/// children whose facts would reach it.
pub async fn coordinated_child_issue_uris_for_job(
    db: &LocalDb,
    job_id: &str,
) -> Result<Vec<String>, String> {
    let job_id = job_id.to_string();
    db.write(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let (issue_id, thread_id) = job_parent_id(conn, &job_id).await?;
            let Some(parent_id) = issue_id.as_deref().or(thread_id.as_deref()) else {
                return Ok(Vec::new());
            };
            let mut rows = conn
                .query(
                    "SELECT i.id, p.key, i.number
                     FROM issues i
                     JOIN projects p ON p.id = i.project_id
                     WHERE i.parent_issue_id = ?1 OR i.parent_thread_id = ?1
                     ORDER BY i.number ASC",
                    params![parent_id],
                )
                .await?;
            let mut children = Vec::new();
            while let Some(row) = rows.next().await? {
                children.push((
                    row.text(0)?,
                    cairn_common::uri::build_issue_uri(&row.text(1)?, row.i64(2)? as i32),
                ));
            }
            let mut uris = Vec::new();
            for (child_issue_id, child_uri) in children {
                if coordinating_job_for_child(conn, &child_issue_id)
                    .await?
                    .as_deref()
                    == Some(job_id.as_str())
                {
                    uris.push(child_uri);
                }
            }
            Ok(uris)
        })
    })
    .await
    .map_err(|error| format!("Failed to list coordinated child issues: {error}"))
}

/// A child issue's parent edge: `(spawning job, parent issue, parent thread)`.
/// `None` when the issue row itself is gone.
async fn parent_edge(
    conn: &cairn_db::turso::Connection,
    issue_id: &str,
) -> DbResult<Option<(Option<String>, Option<String>, Option<String>)>> {
    let mut rows = conn
        .query(
            "SELECT parent_job_id, parent_issue_id, parent_thread_id FROM issues WHERE id = ?1 LIMIT 1",
            params![issue_id],
        )
        .await?;
    match rows.next().await? {
        Some(row) => Ok(Some((row.opt_text(0)?, row.opt_text(1)?, row.opt_text(2)?))),
        None => Ok(None),
    }
}

/// `spawning_job_id` if it is still a valid recipient for a child now parented
/// under `parent_issue_id`; otherwise `None`, so resolution falls through to the
/// live derivation.
///
/// Four conditions, each answering a way a recorded spawner goes stale:
/// - it still exists, is not `failed`/`cancelled`, and has a session to resume —
///   a dead node cannot receive attention;
/// - it sits on the child's *current* parent issue — a job that filed the child
///   somewhere else, or under a parent since re-pointed, is not its owner;
/// - no newer execution exists on the parent issue — a superseded coordinator is
///   retired, and the parent's children move to whoever drives it now.
async fn validated_spawning_job(
    conn: &cairn_db::turso::Connection,
    spawning_job_id: &str,
    parent_issue_id: &str,
) -> DbResult<Option<String>> {
    let mut rows = conn
        .query(
            "SELECT j.id
             FROM jobs j
             WHERE j.id = ?1
               AND j.issue_id = ?2
               AND j.status NOT IN ('failed', 'cancelled')
               AND j.current_session_id IS NOT NULL
               AND NOT EXISTS (
                     SELECT 1 FROM executions newer
                     WHERE newer.issue_id = ?2
                       AND newer.seq > (
                             SELECT own.seq FROM executions own WHERE own.id = j.execution_id
                           )
                   )
             LIMIT 1",
            params![spawning_job_id, parent_issue_id],
        )
        .await?;
    crate::storage::next_text(&mut rows, 0).await
}

async fn job_parent_id(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
) -> DbResult<(Option<String>, Option<String>)> {
    let mut rows = conn
        .query(
            "SELECT issue_id, thread_id FROM jobs WHERE id = ?1 LIMIT 1",
            params![job_id],
        )
        .await?;
    match rows.next().await? {
        Some(row) => Ok((row.opt_text(0)?, row.opt_text(1)?)),
        None => Ok((None, None)),
    }
}

/// The node currently driving `issue_id`: the newest live **recipe-root** job on
/// it. Recipe-root (`jobs.parent_job_id IS NULL`) excludes a coordinator's own
/// delegated sub-task jobs, which share its `issue_id` and would otherwise win on
/// recency.
///
/// "Newest" is start order first, creation order second. Every node of one
/// execution is minted in a single pass with one shared `created_at`
/// (`execution::advancement::job_creation`), so `created_at` alone cannot tell an
/// upstream planner from the downstream coordinator it feeds. `started_at` can:
/// it is stamped on Pending→Running, a downstream node starts strictly after its
/// upstream completes, and a NULL (never started) sorts last in SQLite's DESC —
/// so a pending coordinator never outranks the running planner, and a started one
/// always outranks the finished planner. `created_at DESC` then preserves
/// succession across executions, and `rowid DESC` makes a same-second tie
/// deterministic rather than insertion-ordered.
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
               AND status NOT IN ('failed', 'cancelled')
               AND current_session_id IS NOT NULL
             ORDER BY started_at DESC, created_at DESC, rowid DESC
             LIMIT 1",
            params![issue_id],
        )
        .await?;
    crate::storage::next_text(&mut rows, 0).await
}
