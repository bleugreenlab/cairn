//! Accept native rebuild publication without erasing newer replay obligations.
use crate::orchestrator::base_advance::{consume_held_replay_source_conn, HeldReplaySource};
use crate::storage::{DbError, DbResult, LocalDb, RowExt, TrackedConnection};
use cairn_db::turso::params;
use std::path::Path;

// Include the session, request and held-source coordinates, not merely a
// second-resolution timestamp: a fresh conflict may replace a row in one second.
const GENERATION: &str = "json_array(i.updated_at, i.diagnostic_version,
    i.conflict_condition, i.base_commit, i.ours_commit, i.theirs_commit,
    i.conflicted_tip, i.incoming_pr_number, i.incoming_issue, i.incoming_base_branch,
    i.resolution_state, i.resolution_basis_json, i.marker_state, i.marker_diagnostic,
    i.replay_request_id, i.replay_resolution, i.replay_drop_incoming_reason,
    i.replay_requested_at, i.replay_requested_tip, i.replay_requested_destination,
    i.replay_served_at, i.replay_result_json,
    i.held_replay_source_cut, i.held_replay_source_tip)";

#[derive(Clone, Debug, PartialEq, Eq)]
struct Item {
    intent: String,
    destination: String,
    base_branch: String,
    generation: String,
    request_destination: Option<String>,
    held_cut: Option<String>,
    held_tip: Option<String>,
    session_tip: Option<String>,
    resolution_state: Option<String>,
    request_tip: Option<String>,
    request_pending: bool,
}

#[derive(Clone)]
struct Retirement {
    item: Item,
    held: bool,
    session: bool,
    request: bool,
    state: &'static str,
    complete: bool,
}

async fn items(conn: &TrackedConnection, store: &str, branch: &str) -> DbResult<Vec<Item>> {
    let mut rows = conn
        .query(
            &format!(
                "SELECT i.intent_id, n.destination_commit, n.target_branch, {GENERATION},
            i.replay_requested_destination, i.held_replay_source_cut, i.held_replay_source_tip,
            i.ours_commit, i.resolution_state, i.replay_requested_tip,
            (i.replay_request_id IS NOT NULL AND i.replay_served_at IS NULL)
         FROM jj_reconcile_items i JOIN jj_reconcile_intents n ON n.id=i.intent_id
         WHERE n.store_path=?1 AND i.bookmark=?2
           AND (i.resolution_state='open' OR i.held_replay_source_tip IS NOT NULL
                OR (i.replay_request_id IS NOT NULL AND i.replay_served_at IS NULL))
         ORDER BY i.intent_id"
            ),
            params![store, branch],
        )
        .await?;
    let mut result = Vec::new();
    while let Some(row) = rows.next().await? {
        result.push(Item {
            intent: row.text(0)?,
            destination: row.text(1)?,
            base_branch: row.text(2)?,
            generation: row.text(3)?,
            request_destination: row.opt_text(4)?,
            held_cut: row.opt_text(5)?,
            held_tip: row.opt_text(6)?,
            session_tip: row.opt_text(7)?,
            resolution_state: row.opt_text(8)?,
            request_tip: row.opt_text(9)?,
            request_pending: row.i64(10)? != 0,
        });
    }
    Ok(result)
}

#[derive(Clone)]
struct Owner {
    id: String,
    base: Option<String>,
    execution: String,
    node: String,
    snapshot: String,
    parent: Option<String>,
    parent_branch: Option<String>,
}

/// Capture while holding the store writer, before the native call. No database
/// transaction spans native work or Git export.
pub(super) struct Snapshot {
    items: Vec<Item>,
    owners: Vec<Owner>,
}

impl Snapshot {
    pub(super) async fn capture(
        db: &LocalDb,
        store: &Path,
        branch: &str,
        owning_job: &str,
    ) -> Result<Self, String> {
        let path = store.to_string_lossy().into_owned();
        let branch_owned = branch.to_string();
        let captured = db
            .read(|conn| {
                let (path, branch_owned) = (path.clone(), branch_owned.clone());
                Box::pin(async move { items(conn, &path, &branch_owned).await })
            })
            .await
            .map_err(|error| format!("snapshot rebuild obligations: {error}"))?;
        let candidates = db
            .query_all(
                "SELECT j.id, j.base_commit, j.execution_id, j.recipe_node_id, e.snapshot, j.parent_job_id, p.branch
             FROM jobs j JOIN executions e ON e.id=j.execution_id LEFT JOIN jobs p ON p.id=j.parent_job_id
             WHERE j.branch=?1 AND j.project_id=(SELECT project_id FROM jobs WHERE id=?2)",
                params![branch, owning_job],
                |row| {
                    Ok((
                        row.text(0)?,
                        row.opt_text(1)?,
                        row.text(2)?,
                        row.opt_text(3)?,
                        row.opt_text(4)?,
                        row.opt_text(5)?,
                        row.opt_text(6)?,
                    ))
                },
            )
            .await
            .map_err(|error| format!("snapshot rebuild owners: {error}"))?;
        let mut owners = Vec::new();
        for (id, base, execution, node, snapshot, parent, parent_branch) in candidates {
            let (Some(node), Some(snapshot)) = (node, snapshot) else {
                continue;
            };
            if parent_branch.as_deref() == Some(branch) {
                continue;
            }
            if super::rebuild_branch_owner_refusal(db, &id, branch)
                .await?
                .is_none()
            {
                owners.push(Owner {
                    id,
                    base,
                    execution,
                    node,
                    snapshot,
                    parent,
                    parent_branch,
                });
            }
        }
        if !owners.iter().any(|owner| owner.id == owning_job) {
            return Err("rebuild owning node changed while its ownership was captured".into());
        }
        Ok(Self {
            items: captured,
            owners,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn accept(
        self,
        db: &LocalDb,
        jj: &crate::jj::JjEnv,
        store: &Path,
        branch: &str,
        base_branch: &str,
        previous_tip: &str,
        rebuilt_tip: &str,
        base_commit: &str,
    ) -> Result<usize, String> {
        if previous_tip == rebuilt_tip
            || crate::jj::bookmark_commit_checked(jj, store, branch)?.as_deref()
                != Some(rebuilt_tip)
            || crate::jj::revset_commit_checked(jj, store, &format!("parents({rebuilt_tip})"))?
                .as_deref()
                != Some(base_commit)
        {
            return Err("accepted rebuild no longer has its exact native head and base".into());
        }
        crate::jj::require_clean_tree(jj, store, rebuilt_tip, "accept rebuilt branch metadata")?;
        // Original obligations are superseded by the attested rebuild, even
        // after their old destination was rewritten away. Newer obligations
        // bound to the rebuilt head cannot inherit that original-tip authority.
        let original = |tip: Option<&str>| {
            tip.is_some_and(|tip| crate::jj::revset_descends_from(jj, store, previous_tip, tip))
        };
        let carries = |destination: &str| {
            crate::jj::revset_descends_from(jj, store, rebuilt_tip, destination)
        };
        let mut covered = Vec::new();
        let mut retained = 0;
        for item in self.items {
            let same_base = item.base_branch == base_branch;
            let destination_covered = same_base && carries(&item.destination);
            let held = item.held_tip.is_some()
                && (destination_covered || original(item.held_tip.as_deref()));
            let open = item.resolution_state.as_deref() == Some("open");
            let session = open && (destination_covered || original(item.session_tip.as_deref()));
            let request = item.request_pending
                && ((same_base
                    && carries(
                        item.request_destination
                            .as_deref()
                            .unwrap_or(&item.destination),
                    ))
                    || original(item.request_tip.as_deref()));
            let complete = (item.held_tip.is_none() || held)
                && (!open || session)
                && (!item.request_pending || request);
            if !complete {
                retained += 1;
            }
            if held || session || request {
                covered.push(Retirement {
                    item,
                    held,
                    session,
                    request,
                    complete,
                    state: if destination_covered {
                        "resolved"
                    } else {
                        "superseded"
                    },
                });
            }
        }
        let path = store.to_string_lossy().into_owned();
        let branch = branch.to_string();
        let base_commit = base_commit.to_string();
        db.write(|conn| {
            let (covered, owners, path, branch, base_commit) =
                (covered.clone(), self.owners.clone(), path.clone(), branch.clone(), base_commit.clone());
            Box::pin(async move {
                let current = items(conn, &path, &branch).await?;
                for retirement in &covered {
                    if !current.contains(&retirement.item) {
                        return Err(DbError::Row("rebuild obligation generation changed; retry the same request".into()));
                    }
                }
                for retirement in &covered {
                    let item = &retirement.item;
                    if let Some(tip) = item.held_tip.as_ref().filter(|_| retirement.held) {
                        let expected = HeldReplaySource {
                            intent_id: item.intent.clone(), destination: item.destination.clone(),
                            base_branch: item.base_branch.clone(), cut: item.held_cut.clone(), tip: tip.clone(),
                        };
                        if consume_held_replay_source_conn(conn, Path::new(&path), &branch, &expected).await? != 1 {
                            return Err(DbError::Row("rebuild held-source generation changed".into()));
                        }
                    }
                    // Supersession withdraws only captured old authority, never a
                    // descendant's receipt or a request installed after capture.
                    conn.execute(
                        "UPDATE jj_reconcile_items SET
                           resolution_state=CASE WHEN ?4 THEN ?7 ELSE resolution_state END,
                           marker_state=CASE WHEN ?4 THEN 'not_materialized' ELSE marker_state END,
                           replay_served_at=CASE WHEN ?5 THEN ?3 ELSE replay_served_at END,
                           replay_result_json=CASE WHEN ?5 THEN NULL ELSE replay_result_json END,
                           replay_requested_at=CASE WHEN ?5 THEN NULL ELSE replay_requested_at END,
                           replay_requested_tip=CASE WHEN ?5 THEN NULL ELSE replay_requested_tip END,
                           replay_requested_destination=CASE WHEN ?5 THEN NULL ELSE replay_requested_destination END,
                           status=CASE WHEN ?6 THEN 'completed' ELSE status END,
                           outcome_kind=CASE WHEN ?6 THEN 'rebuild_superseded' ELSE outcome_kind END,
                           failure_kind=CASE WHEN ?6 THEN NULL ELSE failure_kind END
                         WHERE intent_id=?1 AND bookmark=?2",
                        params![item.intent.as_str(), branch.as_str(), chrono::Utc::now().timestamp(),
                            retirement.session, retirement.request, retirement.complete, retirement.state],
                    ).await?;
                }
                for owner in &owners {
                    let changed = conn.execute(
                        "UPDATE jobs SET base_commit=?2, updated_at=?3
                         WHERE id=?1 AND branch=?4 AND base_commit IS ?5
                           AND execution_id=?6 AND recipe_node_id=?7
                           AND EXISTS(SELECT 1 FROM executions e WHERE e.id=?6 AND e.snapshot=?8)
                           AND parent_job_id IS ?9
                           AND (SELECT p.branch FROM jobs p WHERE p.id=jobs.parent_job_id) IS ?10",
                        params![owner.id.as_str(), base_commit.as_str(), chrono::Utc::now().timestamp(),
                            branch.as_str(), owner.base.as_deref(), owner.execution.as_str(), owner.node.as_str(), owner.snapshot.as_str(), owner.parent.as_deref(), owner.parent_branch.as_deref()],
                    ).await?;
                    if changed != 1 {
                        return Err(DbError::Row("rebuild owner generation changed; retry the same request".into()));
                    }
                }
                #[cfg(test)]
                if FAIL_ACCEPTANCE.with(|flag| flag.replace(false)) {
                    return Err(DbError::Row("injected rebuild metadata acceptance failure".into()));
                }
                Ok(retained)
            })
        }).await.map_err(|error| format!("accept rebuild metadata: {error}"))
    }
}

#[cfg(test)]
thread_local! {
    static FAIL_ACCEPTANCE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
mod tests;
