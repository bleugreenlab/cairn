//! The store-side replay action on `cairn:~/rebase`.
//!
//! An agent's slot is a plain git worktree whose refs are downstream exports of
//! the runner's private jj store, so nothing done there can move a branch's
//! ancestry. This is the sanctioned request: it enqueues durable reconcile work
//! and returns. No jj runs in the agent's slot, and there is deliberately no
//! git/jj escape hatch beside it — the shared store stays the single authority on
//! ancestry.

//!
//! Two more actions concern the branch's catch-up mode (see
//! `orchestrator::catch_up`). `catch-up-mode` sets or clears it, and is the
//! operator's to approve: [`rebase_authority`] names the authority ahead of any
//! side effect, the write handler raises the operator's card, and the dispatch
//! re-derives the same request and consumes the grant just before it acts.
//! `mark-reviewed` moves the owner's review mark forward and is direct: the
//! operator decides which paths need review, the owner says what it has read.

use super::super::{
    build_failure, payload_non_empty_str, target_resource_for_request, ResourceMutationResult,
};
use crate::authorization::prepare::Prepared;
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use cairn_common::authorization::AuthorityDecision;
use cairn_common::uri::CairnResource;

/// A parsed `catch-up-mode` payload.
struct CatchUpSetting {
    mode: String,
    review_paths: Vec<String>,
    reviewed_through: Option<String>,
}

fn parse_catch_up_setting(payload: &serde_json::Value) -> Result<CatchUpSetting, String> {
    let mode = payload_non_empty_str(payload, "mode", &[]).ok_or_else(|| {
        "catch-up-mode needs payload.mode: 'merge' (catch up by one merge commit) or 'replay' \
         (end the mode)"
            .to_string()
    })?;
    if !matches!(mode, "merge" | "replay") {
        return Err(format!(
            "payload.mode must be 'merge' or 'replay', got '{mode}'"
        ));
    }
    let review_paths = match payload.get("review_paths") {
        None | Some(serde_json::Value::Null) => Vec::new(),
        Some(serde_json::Value::Array(items)) => items
            .iter()
            .map(|item| {
                item.as_str().map(ToOwned::to_owned).ok_or_else(|| {
                    "payload.review_paths is a list of path globs (strings)".to_string()
                })
            })
            .collect::<Result<Vec<_>, _>>()?,
        Some(_) => return Err("payload.review_paths is a list of path globs".to_string()),
    };
    let reviewed_through =
        payload_non_empty_str(payload, "reviewed_through", &[]).map(str::to_string);
    if mode == "replay" && (!review_paths.is_empty() || reviewed_through.is_some()) {
        return Err(
            "review_paths and reviewed_through apply only to mode:'merge'; mode:'replay' ends the \
             mode"
                .to_string(),
        );
    }
    crate::orchestrator::catch_up::validate_review_paths(&review_paths)?;
    Ok(CatchUpSetting {
        mode: mode.to_string(),
        review_paths,
        reviewed_through,
    })
}

/// The authority a `catch-up-mode` write crosses, for `authorization::prepare`:
/// `None` unless the change is that action on a node's rebase page.
pub(crate) async fn rebase_authority(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    item: &ChangeItem,
) -> Option<Prepared> {
    if item.mode != ChangeMode::Patch {
        return None;
    }
    let payload = item.payload.as_ref()?;
    if payload_non_empty_str(payload, "action", &[]) != Some("catch-up-mode") {
        return None;
    }
    let CairnResource::NodeRebase {
        project,
        number,
        exec_seq,
        node_id,
    } = target_resource_for_request(orch, request, item)
        .await
        .ok()?
    else {
        return None;
    };
    Some(
        match catch_up_authority(orch, &project, number, exec_seq, &node_id, payload).await {
            Ok(authority) => Prepared::Authority(authority),
            Err(error) => Prepared::Invalid(error),
        },
    )
}

/// Prepare and dispatch both derive the request here, so they name the same
/// one.
async fn catch_up_authority(
    orch: &Orchestrator,
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    payload: &serde_json::Value,
) -> Result<cairn_common::authorization::AuthorityRequest, String> {
    let setting = parse_catch_up_setting(payload)?;
    let db = orch.db.for_project(project).await;
    let rows =
        crate::resources::NodeRebaseRows::read(&db, project, number, exec_seq, node_id).await?;
    let branch = rows
        .branch
        .clone()
        .ok_or_else(|| "This node has no branch to set a catch-up mode on.".to_string())?;
    let project_id = rows
        .project_id()
        .ok_or_else(|| "this node's project could not be read".to_string())?;
    Ok(crate::authorization::normalize::branch_catch_up_request(
        project_id,
        &branch,
        &setting.mode,
        &setting.review_paths,
        setting.reviewed_through.as_deref(),
    ))
}

/// Who is writing: the run's node home, or `operator` for a write no run makes.
/// A run may write only for its own node.
async fn writer(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    owning_job: &str,
) -> Result<String, String> {
    if request.run_id.as_deref().is_none_or(str::is_empty) {
        return Ok("operator".to_string());
    }
    let (context, _) =
        crate::mcp::handlers::run_context::lookup_run_routed(&orch.db, request).await?;
    if context.job_id != owning_job {
        return Err(
            "Refused: only this branch's owning node or the operator may mark its review. \
             Nothing changed."
                .to_string(),
        );
    }
    Ok(
        crate::mcp::handlers::run_context::lookup_home_uri_routed(&orch.db, request)
            .await
            .unwrap_or_else(|_| format!("run {}", context.run_id)),
    )
}

/// The commit `revision` names, proven to lie in both the branch's and the
/// default branch's history.
fn shared_commit(
    jj: &crate::jj::JjEnv,
    store: &std::path::Path,
    branch: &str,
    default_branch: &str,
    revision: &str,
) -> Result<String, String> {
    let commit = crate::jj::revset_commit_checked(jj, store, revision)?
        .ok_or_else(|| format!("`{revision}` does not name one commit in the store"))?;
    let branch_revset = format!("bookmarks(exact:{branch:?})");
    let default_revset = format!("bookmarks(exact:{default_branch:?})");
    if !crate::jj::revset_descends_from(jj, store, &branch_revset, &commit)
        || !crate::jj::revset_descends_from(jj, store, &default_revset, &commit)
    {
        return Err(format!(
            "{commit} is not in the history of both `{branch}` and `{default_branch}`; a review \
             mark is a base commit this branch already contains"
        ));
    }
    Ok(commit)
}

async fn project_default_branch(
    db: &crate::storage::LocalDb,
    project_id: &str,
) -> Result<String, String> {
    db.query_opt_text(
        "SELECT default_branch FROM projects WHERE id = ?1",
        (project_id.to_string(),),
    )
    .await
    .map_err(|error| error.to_string())?
    .ok_or_else(|| "the project's default branch could not be read".to_string())
}

#[allow(clippy::too_many_arguments)]
async fn set_catch_up_mode(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    db: &crate::storage::LocalDb,
    rows: &crate::resources::NodeRebaseRows,
    branch: &str,
    resource: (&str, i32, i32, &str),
    payload: &serde_json::Value,
    dry_run: bool,
) -> Result<String, String> {
    let setting = parse_catch_up_setting(payload)?;
    let project_id = rows
        .project_id()
        .ok_or_else(|| "this node's project could not be read".to_string())?
        .to_string();
    let store = rows.store_path(orch)?;
    let default_branch = project_default_branch(db, &project_id).await?;
    if branch == default_branch {
        return Err(format!(
            "Refused: `{branch}` is the project's default branch, which never catches up with \
             anything. Nothing changed."
        ));
    }
    if db
        .query_opt_text(
            "SELECT id FROM jobs WHERE project_id = ?1 AND base_branch = ?2 AND id != ?3 LIMIT 1",
            (project_id.clone(), branch.to_string(), rows.job_id.clone()),
        )
        .await
        .map_err(|error| error.to_string())?
        .is_some()
    {
        return Err(format!(
            "Refused: `{branch}` is an integration branch other work is cut from; the catch-up \
             mode applies only to a branch nothing else is based on. Nothing changed."
        ));
    }
    let (project, number, exec_seq, node_id) = resource;
    let authority = catch_up_authority(orch, project, number, exec_seq, node_id, payload).await?;
    if dry_run {
        return Ok(format!("Would {}", authority.summary));
    }
    let Some(actor) = crate::authorization::resolve_actor(orch, request).await else {
        return Err(format!(
            "Denied: a branch's catch-up mode is the operator's to approve, and this request comes \
             from no run that could ask. Ask an agent on this node to request it; the operator's \
             approval of that card sets it. Asked: {}.",
            authority.summary
        ));
    };
    let (decision, event_id) = crate::authorization::authorize_recorded(&actor, &authority).await?;
    match decision {
        AuthorityDecision::Direct | AuthorityDecision::AllowedByGrant { .. } => {}
        AuthorityDecision::ApprovalRequired(reason) | AuthorityDecision::Forbidden(reason) => {
            return Err(crate::authorization::refusal_message(&authority, reason))
        }
    }
    let asker = actor
        .principal
        .node_uri
        .clone()
        .or_else(|| actor.principal.run_id.clone())
        .unwrap_or_else(|| "an unnamed run".to_string());
    let who = match event_id {
        Some(event) => format!("operator (approved {event}, asked by {asker})"),
        None => format!("operator (asked by {asker})"),
    };

    if setting.mode == "replay" {
        let cleared = crate::orchestrator::catch_up::clear(
            db,
            &project_id,
            &store,
            branch,
            &who,
            "returned to catching up by replay",
        )
        .await?;
        return Ok(if cleared {
            format!("`{branch}` catches up by replay again; its merge catch-up mode is cleared.")
        } else {
            format!("`{branch}` had no catch-up mode; it catches up by replay.")
        });
    }

    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let (read_store, read_branch, read_default, through) = (
        store.clone(),
        branch.to_string(),
        default_branch.clone(),
        setting.reviewed_through.clone(),
    );
    let reviewed_through = tokio::task::spawn_blocking(move || -> Result<String, String> {
        match through {
            Some(revision) => {
                shared_commit(&jj, &read_store, &read_branch, &read_default, &revision)
            }
            None => {
                let tip = crate::jj::bookmark_commit_checked(&jj, &read_store, &read_branch)?
                    .ok_or_else(|| format!("`{read_branch}` does not resolve in the store"))?;
                let default_tip =
                    crate::jj::bookmark_commit_checked(&jj, &read_store, &read_default)?
                        .ok_or_else(|| format!("`{read_default}` does not resolve in the store"))?;
                let bases = crate::jj::merge_bases(&jj, &read_store, &tip, &default_tip)?;
                match bases.as_slice() {
                    [base] => Ok(base.clone()),
                    _ => Err(format!(
                        "`{read_branch}` and `{read_default}` share {} merge bases ({}), so there \
                         is no one default review mark; pass reviewed_through explicitly",
                        bases.len(),
                        bases.join(", ")
                    )),
                }
            }
        }
    })
    .await
    .map_err(|error| format!("review mark worker failed: {error}"))??;
    let mode = crate::orchestrator::catch_up::set(
        db,
        crate::orchestrator::catch_up::SetCatchUpMode {
            project_id: &project_id,
            store: &store,
            bookmark: branch,
            job_id: &rows.job_id,
            actor: &who,
            review_paths: &setting.review_paths,
            reviewed_through: &reviewed_through,
        },
    )
    .await?;
    Ok(format!(
        "`{branch}` now catches up by merge (set by {who}). Review-required paths: {}. Reviewed \
         through {}. It is never replayed; the next base advance merges the base into it.",
        if mode.review_paths.is_empty() {
            "none".to_string()
        } else {
            mode.review_paths.join(", ")
        },
        mode.reviewed_through
    ))
}

async fn mark_reviewed(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    db: &crate::storage::LocalDb,
    rows: &crate::resources::NodeRebaseRows,
    branch: &str,
    payload: &serde_json::Value,
    dry_run: bool,
) -> Result<String, String> {
    let through = payload_non_empty_str(payload, "through", &[])
        .ok_or_else(|| {
            "mark-reviewed needs payload.through: the base commit you reviewed through".to_string()
        })?
        .to_string();
    let project_id = rows
        .project_id()
        .ok_or_else(|| "this node's project could not be read".to_string())?
        .to_string();
    let store = rows.store_path(orch)?;
    let mode = crate::orchestrator::catch_up::mode_for(db, &project_id, &store, branch)
        .await?
        .ok_or_else(|| {
            format!("`{branch}` has no catch-up mode, so there is no review mark to move")
        })?;
    let who = writer(orch, request, &rows.job_id).await?;
    let default_branch = project_default_branch(db, &project_id).await?;
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let (read_store, read_branch, previous) = (
        store.clone(),
        branch.to_string(),
        mode.reviewed_through.clone(),
    );
    let commit = tokio::task::spawn_blocking(move || -> Result<String, String> {
        let commit = shared_commit(&jj, &read_store, &read_branch, &default_branch, &through)?;
        if !crate::jj::revset_descends_from(&jj, &read_store, &commit, &previous) {
            return Err(format!(
                "{commit} is not at or after the current review mark {previous}; a review mark \
                 only moves forward"
            ));
        }
        Ok(commit)
    })
    .await
    .map_err(|error| format!("review mark worker failed: {error}"))??;
    if dry_run {
        return Ok(format!("Would mark `{branch}` reviewed through {commit}"));
    }
    crate::orchestrator::catch_up::mark_reviewed(db, &mode, &commit, &who).await?;
    Ok(format!(
        "`{branch}` is marked reviewed through {commit} by {who} (was {}).",
        mode.reviewed_through
    ))
}

pub(super) async fn dispatch(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
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
            if !matches!(
                action,
                "replay"
                    | "fast-forward-from-origin"
                    | "repair"
                    | "catch-up-mode"
                    | "mark-reviewed"
            ) {
                return Err(build_failure(
                    index,
                    item,
                    format!(
                        "payload.action must be 'replay', 'fast-forward-from-origin', 'repair', 'catch-up-mode' or 'mark-reviewed', got '{action}'"
                    ),
                ));
            }
            if matches!(action, "catch-up-mode" | "mark-reviewed") {
                let db = orch.db.for_project(project).await;
                let rows = crate::resources::NodeRebaseRows::read(
                    &db, project, *number, *exec_seq, node_id,
                )
                .await
                .map_err(|error| build_failure(index, item, error))?;
                let branch = rows.branch.clone().ok_or_else(|| {
                    build_failure(index, item, "This node has no branch.".to_string())
                })?;
                let outcome = if action == "catch-up-mode" {
                    set_catch_up_mode(
                        orch,
                        request,
                        &db,
                        &rows,
                        &branch,
                        (project.as_str(), *number, *exec_seq, node_id.as_str()),
                        payload,
                        dry_run,
                    )
                    .await
                } else {
                    mark_reviewed(orch, request, &db, &rows, &branch, payload, dry_run).await
                };
                return outcome
                    .map(Some)
                    .map_err(|error| build_failure(index, item, error));
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
