//! The store-side replay action on `cairn:~/rebase`.
//!
//! An agent's slot is a plain git worktree whose refs are downstream exports of
//! the runner's private jj store, so nothing done there can move a branch's
//! ancestry. This is the sanctioned request: it enqueues durable reconcile work
//! and returns. No jj runs in the agent's slot, and there is deliberately no
//! git/jj escape hatch beside it — the shared store stays the single authority on
//! ancestry.

use super::super::{build_failure, payload_non_empty_str, ResourceMutationResult};
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use cairn_common::uri::CairnResource;

pub(super) async fn dispatch(
    orch: &Orchestrator,
    _request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    resource: &CairnResource,
) -> ResourceMutationResult<Option<String>> {
    let summary = match (resource, item.mode) {
        (
            CairnResource::NodeRebase {
                project,
                number,
                exec_seq,
                node_id,
            },
            ChangeMode::Patch,
        ) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
            let action = payload_non_empty_str(payload, "action", &[])
                .ok_or_else(|| build_failure(index, item, "payload.action is required"))?;
            if !matches!(action, "replay" | "fast-forward-from-origin" | "repair") {
                return Err(build_failure(
                    index,
                    item,
                    format!(
                        "payload.action must be 'replay', 'fast-forward-from-origin' or 'repair', got '{action}'"
                    ),
                ));
            }
            let fast_forward_branch = payload_non_empty_str(payload, "branch", &[]);
            if fast_forward_branch.is_some() && action != "fast-forward-from-origin" {
                return Err(build_failure(
                    index,
                    item,
                    "payload.branch applies only to fast-forward-from-origin",
                ));
            }
            if fast_forward_branch.is_some_and(|value| value != "base") {
                return Err(build_failure(
                    index,
                    item,
                    "payload.branch must be 'base' when provided: fast-forward-from-origin acts on this node's branch, or with branch:'base' on the branch it is cut from",
                ));
            }
            let fingerprint = payload_non_empty_str(payload, "fingerprint", &[]);
            let resolution = payload_non_empty_str(payload, "resolution", &[]);
            if resolution.is_some_and(|value| value != "take-committed-tip") {
                return Err(build_failure(
                    index,
                    item,
                    "payload.resolution must be 'take-committed-tip' when provided",
                ));
            }
            // The documented escape from the loss guard. It only means anything
            // alongside the resolution whose whole-file restore does the
            // dropping, so a request carrying it without one is confused about
            // what it is asking for and is told so rather than obeyed.
            let drop_incoming_reason = payload_non_empty_str(payload, "drop_incoming_reason", &[]);
            if drop_incoming_reason.is_some() && resolution.is_none() {
                return Err(build_failure(
                    index,
                    item,
                    "payload.drop_incoming_reason only applies to \
                     resolution:'take-committed-tip', which is what discards incoming hunks; a \
                     plain replay drops nothing",
                ));
            }

            let db = orch.db.for_project(project).await;
            // Read, then release: the origin fetch and the replay below both
            // wait on the store lock, which this same write batch may hold, so
            // nothing here may keep a read open across them (cairn/5502).
            let rows =
                crate::resources::NodeRebaseRows::read(&db, project, *number, *exec_seq, node_id)
                    .await
                    .map_err(|error| build_failure(index, item, error))?;
            let branch = rows.branch.clone().ok_or_else(|| {
                build_failure(
                    index,
                    item,
                    "This node has no branch to replay.".to_string(),
                )
            })?;

            if action == "repair" {
                // The bounded managed retry of a recovery fence's targeted
                // repair: the same attempt the sweep makes, brought forward.
                if dry_run {
                    format!("Would retry the targeted recovery repair of `{branch}` now")
                } else {
                    let store = rows
                        .store_path(orch)
                        .map_err(|error| build_failure(index, item, error))?;
                    crate::orchestrator::branch_recovery::repair_now(orch, &db, &store, &branch)
                        .await
                        .map_err(|error| build_failure(index, item, error))?
                }
            } else if action == "fast-forward-from-origin" {
                let target = if fast_forward_branch.is_some() {
                    crate::resources::node_base_branch(orch, &rows, &branch)
                        .await
                        .map_err(|error| build_failure(index, item, error))?
                } else {
                    branch.clone()
                };
                if dry_run {
                    format!("Would take origin's copy of `{target}` if it is a pure fast-forward")
                } else {
                    let project_id = crate::mcp::handlers::run_context::project_id_by_key(
                        &orch.db.local,
                        project,
                    )
                    .await
                    .map_err(|error| build_failure(index, item, error))?;
                    crate::orchestrator::base_branch_sync::fast_forward_now(
                        orch,
                        &project_id,
                        &target,
                    )
                    .await
                    .map_err(|error| build_failure(index, item, error))?
                }
            } else if dry_run {
                format!("Would request a store-side replay of `{branch}`")
            } else {
                crate::orchestrator::base_advance::request_branch_replay(
                    orch,
                    &db,
                    &rows.job_id,
                    &branch,
                    fingerprint,
                    resolution.is_some(),
                    drop_incoming_reason,
                )
                .await
                .map_err(|error| build_failure(index, item, error))?
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(summary))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::SearchIndex;
    use std::sync::Arc;
    use std::time::Duration;

    /// The 10-29 holder (cairn/5502): a replay request whose store lock is
    /// already held, as it is when the same write batch carries file items.
    /// The request waits on the lock; while it does, it must hold no database
    /// connection, because a transaction parked there for the whole wait blocks
    /// every checkpoint and nothing can cut it.
    #[tokio::test]
    async fn a_replay_waiting_on_the_store_lock_holds_no_connection() {
        let db = Arc::new(crate::storage::migrated_test_db("replay-holds-nothing.db").await);
        db.execute_script(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
               VALUES ('proj-1', 'default', 'Project', 'proj', '/repo', 'main', 1, 1);
             INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
               VALUES ('issue-1', 'proj-1', 1, 'Issue', 'active', 1, 1);
             INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
               VALUES ('exec-1', 'recipe-default', 'issue-1', 'proj-1', 'running', 1, 1);
             INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, branch, base_branch, uri_segment, created_at, updated_at)
               VALUES ('job-1', 'exec-1', 'node', 'issue-1', 'proj-1', 'running', 'agent/one', 'main', 'builder', 1, 1);",
        )
        .await
        .unwrap();
        let root = tempfile::tempdir().unwrap().keep();
        let orch = crate::orchestrator::Orchestrator::builder(
            Arc::new(DbState::new(
                db.clone(),
                Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap()),
            )),
            Arc::new(TestServicesBuilder::new().build()),
            root,
        )
        .build();

        let store = crate::jj::project_store_dir(&orch.config_dir, std::path::Path::new("/repo"));
        let held = orch
            .acquire_jj_store_lock(
                &store,
                crate::orchestrator::StoreLockClass::Background,
                "the write batch's file items",
            )
            .await;

        let item = ChangeItem {
            target: "cairn:~/rebase".into(),
            mode: ChangeMode::Patch,
            payload: Some(serde_json::json!({ "action": "replay" })),
        };
        let request = McpCallbackRequest {
            thread_id: None,
            cwd: String::new(),
            run_id: None,
            tool: "write".into(),
            payload: serde_json::Value::Null,
            tool_use_id: None,
            answer_bound_ms: None,
            origin: None,
        };
        let resource = CairnResource::NodeRebase {
            project: "proj".into(),
            number: 1,
            exec_seq: 1,
            node_id: "builder".into(),
        };
        let replay = dispatch(&orch, &request, 0, &item, false, &resource);
        let mut replay = std::pin::pin!(replay);

        // Give the request every chance to reach the lock and park there.
        let parked = tokio::time::timeout(Duration::from_millis(500), replay.as_mut()).await;
        assert!(
            parked.is_err(),
            "the replay must still be waiting on the held store lock"
        );
        assert_eq!(
            db.live_connections(),
            0,
            "a replay waiting on the store lock is holding a database connection: {:?}",
            db.overdue_holders(Duration::ZERO, Duration::ZERO)
        );

        drop(held);
        // Whatever the replay concludes against a store that does not exist,
        // it concludes, and leaves nothing open.
        let _ = tokio::time::timeout(Duration::from_secs(30), replay.as_mut())
            .await
            .expect("the replay finishes once the lock is released");
        assert_eq!(db.live_connections(), 0);
    }
}
