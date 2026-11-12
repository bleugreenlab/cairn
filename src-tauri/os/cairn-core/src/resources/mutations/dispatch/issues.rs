//! Issue and comment resource mutation dispatch, relocated from dispatch.rs.

use super::super::{
    build_failure, payload_non_empty_str, payload_str, payload_trimmed_non_empty_str,
    payload_value, ResourceMutationResult,
};
use super::append_payload;
use crate::mcp::handlers::{comments_artifacts, issues};
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use cairn_uri::CairnResource;

pub(super) async fn dispatch(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    resource: &CairnResource,
    applied_data: &mut Option<serde_json::Value>,
) -> ResourceMutationResult<Option<String>> {
    let summary = match (resource, item.mode) {
        (CairnResource::ProjectIssues { project }, ChangeMode::Append) => {
            let payload = append_payload(index, item)?;
            let title = payload_trimmed_non_empty_str(payload, "title", &[]).ok_or_else(|| {
                build_failure(
                    index,
                    item,
                    "payload.title is required and must be a non-empty string",
                )
            })?;
            if let Some(description) = payload.get("description") {
                if !description.is_string() {
                    return Err(build_failure(
                        index,
                        item,
                        "payload.description must be a string",
                    ));
                }
            }
            // The same shape gate the patch path takes, so "what may parent an
            // issue" reads identically whether the issue is being created under
            // a parent or adopted by one later.
            let parent = if let Some(parent) = payload.get("parent") {
                let Some(parent) = parent
                    .as_str()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                else {
                    return Err(build_failure(
                        index,
                        item,
                        "payload.parent must be a non-empty string",
                    ));
                };
                Some(
                    issues::canonicalize_parent_uri(parent)
                        .map_err(|e| build_failure(index, item, e))?,
                )
            } else {
                None
            };
            let mut execution = parse_create_execution_spec(index, item, payload)?;
            // `place:"head"` is checked before the issue exists, against the
            // thread the new issue will be a child of: the one `parent` names,
            // or else the thread this write acts in, which a thread's filing
            // is parented under.
            if let Some(spec) = execution
                .as_mut()
                .filter(|spec| spec.place != crate::execution::recipe::QueuePlace::Tail)
            {
                let parent_thread = match parent.as_deref() {
                    Some(uri) => match cairn_uri::parse_uri(uri) {
                        Some(cairn_uri::CairnResource::Thread { name, path, .. })
                            if path.is_empty() =>
                        {
                            Some(name)
                        }
                        _ => None,
                    },
                    None => super::threads::acting_thread_name(orch, request, project).await,
                };
                let placed_by = super::threads::head_placed_by(
                    orch,
                    request,
                    project,
                    parent_thread.as_deref(),
                    "the new issue",
                    !spec.after.is_empty(),
                )
                .await
                .map_err(|error| build_failure(index, item, error))?;
                spec.place = crate::execution::recipe::QueuePlace::Head { placed_by };
            }
            let labels = parse_string_array_field(index, item, payload, "labels", &[])?;
            if let Some(spec) = execution.as_mut() {
                spec.creator = crate::execution::recipe_resolution::ResolutionCreator::of_request(
                    orch, request,
                )
                .await;
            }
            if dry_run {
                match &execution {
                    Some(spec) => {
                        let resolution = issues::preview_create_recipe(
                            orch,
                            project,
                            title,
                            payload_str(payload, "description", &[]),
                            labels.as_deref(),
                            parent.as_deref(),
                            spec.recipe.as_deref(),
                        )
                        .await
                        .map_err(|error| build_failure(index, item, error))?;
                        format!(
                            "Would create issue in project {project}: {title} and start an \
                             execution\n{}",
                            resolution.line()
                        )
                    }
                    None => format!("Would create issue in project {project}: {title}"),
                }
            } else {
                let agent =
                    crate::mcp::handlers::run_context::authenticated_agent(&orch.db, request)
                        .await
                        .map_err(|error| build_failure(index, item, error))?;
                let (author, appearance) = agent
                    .appearance(orch.services.clock.now())
                    .map_err(|error| build_failure(index, item, error))?;
                let authorship = crate::issues::crud::IssueAuthorship::new(author, appearance)
                    .map_err(|error| build_failure(index, item, error.to_string()))?;
                let description = match payload_str(payload, "description", &[]) {
                    Some(description) => Some(
                        crate::durable_content::normalize_text(orch, request, project, description)
                            .await
                            .map_err(|error| build_failure(index, item, error))?,
                    ),
                    None => None,
                };
                let outcome = issues::create_issue_in_project(
                    orch,
                    project,
                    title.to_string(),
                    description,
                    labels,
                    execution,
                    parent,
                    // The calling run identifies the node filing this child, so
                    // a child issue records the node that spawned it
                    // (`docs/wakes.md`, "Child attention follows the spawning
                    // node"). Without this the child has no owner to route its
                    // questions, reviews, and messages back to.
                    request.run_id.clone(),
                    authorship,
                )
                .await
                .map_err(|error| build_failure(index, item, error))?;
                *applied_data = Some(serde_json::json!({
                    "projectKey": outcome.project_key,
                    "number": outcome.number,
                    "uri": outcome.uri,
                }));
                outcome.summary
            }
        }
        (
            CairnResource::IssueComment {
                project,
                number,
                comment_seq,
            },
            ChangeMode::Patch,
        ) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
            let content = payload_non_empty_str(payload, "content", &[])
                .ok_or_else(|| build_failure(index, item, "payload.content is required"))?;
            if dry_run {
                format!("Would edit comment {comment_seq} on issue {project}/{number}")
            } else {
                // Route to the owning project's database (CAIRN-2132); a local
                // project resolves to the private DB, a shared one to its replica.
                let db = orch.db.for_project(project).await;
                let comment_id =
                    resolve_issue_comment_id(&db, index, item, project, *number, *comment_seq)
                        .await?;
                crate::issues::comments::update(&db, &comment_id, content)
                    .await
                    .map_err(|error| build_failure(index, item, error.to_string()))?;
                format!("Edited comment {comment_seq} on issue {project}/{number}")
            }
        }
        (
            CairnResource::IssueComment {
                project,
                number,
                comment_seq,
            },
            ChangeMode::Delete,
        ) => {
            if dry_run {
                format!("Would delete comment {comment_seq} on issue {project}/{number}")
            } else {
                let db = orch.db.for_project(project).await;
                let comment_id =
                    resolve_issue_comment_id(&db, index, item, project, *number, *comment_seq)
                        .await?;
                crate::issues::comments::delete(&db, &comment_id)
                    .await
                    .map_err(|error| build_failure(index, item, error.to_string()))?;
                format!("Deleted comment {comment_seq} on issue {project}/{number}")
            }
        }
        (CairnResource::Issue { project, number }, ChangeMode::Patch) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
            if let Some(title) = payload.get("title") {
                if !title.is_string() {
                    return Err(build_failure(index, item, "payload.title must be a string"));
                }
            }
            if let Some(description) = payload.get("description") {
                if !description.is_string() {
                    return Err(build_failure(
                        index,
                        item,
                        "payload.description must be a string",
                    ));
                }
            }
            let labels = parse_string_array_field(index, item, payload, "labels", &[])?;
            // `status` is the resolution the UI's IssueMenu sets. Only the two
            // settable resolutions are accepted here: `backlog` is a derived
            // state, not a value a user sets, so it is rejected like any other.
            let status = match payload.get("status") {
                None | Some(serde_json::Value::Null) => None,
                Some(value) => {
                    let status = value.as_str().ok_or_else(|| {
                        build_failure(index, item, "payload.status must be a string")
                    })?;
                    if !matches!(status, "merged" | "closed") {
                        return Err(build_failure(
                            index,
                            item,
                            format!("Invalid status '{status}'. Allowed values: merged, closed"),
                        ));
                    }
                    Some(status.to_string())
                }
            };
            // `confirm` acknowledges that resolving the issue stops the work
            // still live on it (CAIRN-3212). It has meaning only alongside a
            // resolution, so it is rejected on its own rather than silently
            // ignored.
            let confirm = match payload.get("confirm") {
                None | Some(serde_json::Value::Null) => false,
                Some(serde_json::Value::Bool(value)) => *value,
                Some(_) => {
                    return Err(build_failure(
                        index,
                        item,
                        "payload.confirm must be a boolean",
                    ))
                }
            };
            if confirm && status.is_none() {
                return Err(build_failure(
                    index,
                    item,
                    "payload.confirm applies to a resolution; pass it with payload.status",
                ));
            }
            // Re-parenting: absent leaves the parent untouched, null/empty
            // orphans the issue, a string adopts it under that canonical issue
            // or thread URI. Only the URI's shape is decided here; which edge it
            // names, plus existence, same-project, and cycle checks, happen in
            // the txn through the same resolver the create path uses.
            let parent = match payload.get("parent") {
                None => None,
                Some(serde_json::Value::Null) => Some(None),
                Some(value) => {
                    let raw = value.as_str().ok_or_else(|| {
                        build_failure(
                            index,
                            item,
                            "payload.parent must be an issue or thread URI string, or null",
                        )
                    })?;
                    if raw.trim().is_empty() {
                        Some(None)
                    } else {
                        let canonical = issues::canonicalize_parent_uri(raw)
                            .map_err(|e| build_failure(index, item, e))?;
                        Some(Some(canonical))
                    }
                }
            };
            if dry_run {
                let mut details = Vec::new();
                if let Some(status) = status.as_deref() {
                    details.push(format!("status={status}"));
                }
                match &parent {
                    None => {}
                    Some(None) => details.push("parent=cleared".to_string()),
                    Some(Some(uri)) => details.push(format!("parent={uri}")),
                }
                if details.is_empty() {
                    format!("Would patch issue {project}/{number}")
                } else {
                    format!(
                        "Would patch issue {project}/{number} ({})",
                        details.join(", ")
                    )
                }
            } else {
                let title = payload_str(payload, "title", &[]).map(ToOwned::to_owned);
                let description = match payload_str(payload, "description", &[]) {
                    Some(description) => Some(
                        crate::durable_content::normalize_text(orch, request, project, description)
                            .await
                            .map_err(|error| build_failure(index, item, error))?,
                    ),
                    None => None,
                };
                issues::update_issue_by_project_number(
                    orch,
                    request,
                    project,
                    *number,
                    issues::IssuePatchFields {
                        title,
                        description,
                        labels,
                        status,
                        confirm,
                        parent,
                    },
                )
                .await
                .map_err(|error| build_failure(index, item, error))?
            }
        }
        (CairnResource::Issue { project, number }, ChangeMode::Delete) => {
            // Resolve against the owning DB (CAIRN-2181): a team project's issue
            // row lives in its team replica, so the lookup must route there or a
            // team-project delete would falsely report "not found".
            let owning_db = orch.db.for_project(project).await;
            let issue_id =
                crate::issues::relations::issue_id_for_project_number(&owning_db, project, *number)
                    .await
                    .map_err(|error| build_failure(index, item, error.to_string()))?
                    .ok_or_else(|| {
                        build_failure(index, item, format!("Issue {project}/{number} not found"))
                    })?;
            if dry_run {
                format!("Would delete issue {project}/{number}")
            } else {
                crate::issues::delete::delete_issue(orch, &issue_id)
                    .await
                    .map_err(|error| build_failure(index, item, error))?;
                format!("Deleted issue {project}/{number}")
            }
        }
        (CairnResource::Issue { project, number }, ChangeMode::Append) => {
            let payload = append_payload(index, item)?;
            let content = payload_non_empty_str(payload, "content", &[])
                .ok_or_else(|| build_failure(index, item, "payload.content is required"))?;
            if dry_run {
                format!(
                    "Would append {} chars as a comment to issue {project}/{number}",
                    content.len()
                )
            } else {
                comments_artifacts::append_issue_comment(orch, request, project, *number, content)
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(summary))
}

fn parse_string_array_field(
    index: usize,
    item: &ChangeItem,
    payload: &serde_json::Value,
    key: &str,
    aliases: &[&str],
) -> ResourceMutationResult<Option<Vec<String>>> {
    let value = payload_value(payload, key, aliases);
    let Some(value) = value else {
        return Ok(None);
    };
    let values = value.as_array().ok_or_else(|| {
        build_failure(
            index,
            item,
            format!("payload.{key} must be an array of non-empty strings"),
        )
    })?;
    let mut parsed = Vec::with_capacity(values.len());
    for value in values {
        let item_value = value
            .as_str()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                build_failure(
                    index,
                    item,
                    format!("payload.{key} must be an array of non-empty strings"),
                )
            })?;
        parsed.push(item_value.to_string());
    }
    Ok(Some(parsed))
}

/// Resolve a per-issue comment `seq` to its stable comment id, mapping a missing
/// issue or comment to a clean not-found failure. The member URI embeds the
/// issue number, so a seq that belongs to a different issue is rejected too.
async fn resolve_issue_comment_id(
    db: &crate::storage::LocalDb,
    index: usize,
    item: &ChangeItem,
    project: &str,
    number: i32,
    comment_seq: i32,
) -> ResourceMutationResult<String> {
    let issue_id = crate::issues::relations::issue_id_for_project_number(db, project, number)
        .await
        .map_err(|error| build_failure(index, item, error.to_string()))?
        .ok_or_else(|| build_failure(index, item, format!("Issue {project}/{number} not found")))?;
    crate::issues::comments::id_for_issue_seq(db, &issue_id, comment_seq as i64)
        .await
        .map_err(|error| build_failure(index, item, error.to_string()))?
        .ok_or_else(|| {
            build_failure(
                index,
                item,
                format!("Comment {comment_seq} not found on issue {project}/{number}"),
            )
        })
}

/// Parse the optional `execution` key on an issue-create payload into a
/// create+start spec. Absent or null -> None (create only). `true` is an
/// empty spec: start, with Cairn choosing the recipe. Otherwise it must be an
/// object whose `recipe`, if set, is a string.
fn parse_create_execution_spec(
    index: usize,
    item: &ChangeItem,
    payload: &serde_json::Value,
) -> ResourceMutationResult<Option<issues::CreateExecutionSpec>> {
    let value = match payload.get("execution") {
        None | Some(serde_json::Value::Null) => return Ok(None),
        Some(serde_json::Value::Bool(true)) => {
            return Ok(Some(issues::CreateExecutionSpec::default()))
        }
        Some(value) => value,
    };
    let obj = value.as_object().ok_or_else(|| {
        build_failure(
            index,
            item,
            "payload.execution must be true (start, Cairn chooses the recipe) or an object \
             {recipe?, overrides?, after?, place?}; omit it to create the issue without starting \
             one",
        )
    })?;
    if let Some(refusal) =
        crate::tenets::filing_tenet_keys_refusal(Some(value), "payload.execution.")
    {
        return Err(build_failure(index, item, refusal));
    }
    let str_field = |key: &str| -> ResourceMutationResult<Option<String>> {
        match obj.get(key) {
            None | Some(serde_json::Value::Null) => Ok(None),
            Some(value) => value.as_str().map(|s| Some(s.to_string())).ok_or_else(|| {
                build_failure(
                    index,
                    item,
                    format!("payload.execution.{key} must be a string"),
                )
            }),
        }
    };
    if obj.contains_key("backend") {
        return Err(build_failure(
            index,
            item,
            super::executions::RETIRED_EXECUTION_BACKEND.replace("payload.", "payload.execution."),
        ));
    }
    Ok(Some(issues::CreateExecutionSpec {
        recipe: str_field("recipe")?,
        creator: Default::default(),
        overrides_filed: obj.get("overrides").cloned(),
        overrides: super::executions::parse_launch_overrides(index, item, Some(value)).map_err(
            |mut failure| {
                // The key is `execution.overrides` here, not the top-level
                // `overrides` the collection append takes.
                failure.error = failure
                    .error
                    .replace("payload.overrides", "payload.execution.overrides");
                failure
            },
        )?,
        after: super::executions::parse_after_clauses(
            index,
            item,
            obj.get("after"),
            "payload.execution.after",
        )?,
        // Who placed it is settled once the parent thread is known; until
        // then `Head` only records that the head was asked for.
        place: if super::executions::parse_place(
            index,
            item,
            obj.get("place"),
            "payload.execution.place",
        )? {
            crate::execution::recipe::QueuePlace::Head {
                placed_by: String::new(),
            }
        } else {
            crate::execution::recipe::QueuePlace::Tail
        },
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{LocalDb, RowExt, SearchIndex};
    use cairn_db::turso::params;
    use std::sync::Arc;

    fn test_orchestrator(db: LocalDb) -> Orchestrator {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.keep();
        let config_dir = root.join("config");
        std::fs::create_dir_all(config_dir.join("agents")).unwrap();
        std::fs::create_dir_all(config_dir.join("recipes")).unwrap();
        let search_index = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
        let db_state = Arc::new(DbState::new(Arc::new(db), search_index));
        OrchestratorBuilder::new(
            db_state,
            Arc::new(TestServicesBuilder::new().build()),
            config_dir,
        )
        .build()
    }

    /// The seam between an agent's `write` and issue creation: the calling run
    /// has to reach `create_issue_in_project`, or a spawned child records no
    /// owner and its questions, reviews, and messages route by derivation alone.
    #[tokio::test]
    async fn appending_a_child_issue_records_the_calling_node() {
        let db = crate::storage::migrated_test_db("issue-dispatch.db").await;
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, next_issue_number, created_at, updated_at)
              VALUES('p','w','Project','proj','/tmp/repo',2,1,1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
              VALUES('parent','p',1,'Parent','active','active','none',1,1);
            INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
              VALUES('exec','recipe','parent','p','running',1,1);
            INSERT INTO jobs(id, execution_id, project_id, issue_id, node_name, status, current_session_id, uri_segment, created_at, updated_at)
              VALUES('coordinator','exec','p','parent','Coordinator','running','sess','coordinator',1,1);
            INSERT INTO runs(id, project_id, job_id, issue_id, status, created_at, updated_at)
              VALUES('run-coordinator','p','coordinator','parent','live',1,1);
            ",
        )
        .await
        .unwrap();
        let orch = test_orchestrator(db);

        let item = ChangeItem {
            target: "cairn://p/proj/issues".to_string(),
            mode: ChangeMode::Append,
            payload: Some(serde_json::json!({
                "title": "Spawned child",
                "parent": "cairn://p/proj/1",
            })),
        };
        let request = McpCallbackRequest {
            answer_bound_ms: None,
            thread_id: None,
            cwd: "/tmp/repo".to_string(),
            run_id: Some("run-coordinator".to_string()),
            tool: "write".to_string(),
            payload: serde_json::Value::Null,
            tool_use_id: None,
            origin: None,
        };
        let mut applied_data = None;
        dispatch(
            &orch,
            &request,
            0,
            &item,
            false,
            &CairnResource::ProjectIssues {
                project: "proj".to_string(),
            },
            &mut applied_data,
        )
        .await
        .unwrap();

        let spawner = orch
            .db
            .local
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT parent_job_id FROM issues WHERE number = 2",
                            params![],
                        )
                        .await?;
                    match rows.next().await? {
                        Some(row) => row.opt_text(0),
                        None => Ok(None),
                    }
                })
            })
            .await
            .unwrap();
        assert_eq!(spawner.as_deref(), Some("coordinator"));
    }

    #[tokio::test]
    async fn appending_children_to_both_thread_addresses_records_the_thread_edge() {
        let db = crate::storage::migrated_test_db("thread-child-dispatch.db").await;
        db.execute_script(
            "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id, workspace_id, name, key, repo_path, next_issue_number, created_at, updated_at)
               VALUES('p','w','Project','proj','/tmp/repo',1,1,1);
             INSERT INTO threads(id, project_id, name, status, attention, migrated_from_number, created_at, updated_at)
               VALUES('thread','p','general','active','none',3404,1,1);
             INSERT INTO jobs(id, project_id, thread_id, node_name, status, uri_segment, created_at, updated_at)
               VALUES('thread-session','p','thread','Thread','running','thread',1,1);
             INSERT INTO runs(id, project_id, job_id, status, created_at, updated_at)
               VALUES('run-thread-session','p','thread-session','live',1,1);",
        )
        .await
        .unwrap();
        let orch = test_orchestrator(db);
        let request = McpCallbackRequest {
            answer_bound_ms: None,
            thread_id: None,
            cwd: "/tmp/repo".to_string(),
            run_id: Some("run-thread-session".to_string()),
            tool: "write".to_string(),
            payload: serde_json::Value::Null,
            tool_use_id: None,
            origin: None,
        };

        for (title, parent) in [
            ("Name child", "cairn://p/proj/general"),
            ("Alias child", "cairn://p/proj/3404"),
        ] {
            let item = ChangeItem {
                target: "cairn://p/proj/issues".to_string(),
                mode: ChangeMode::Append,
                payload: Some(serde_json::json!({"title": title, "parent": parent})),
            };
            let mut applied_data = None;
            dispatch(
                &orch,
                &request,
                0,
                &item,
                false,
                &CairnResource::ProjectIssues {
                    project: "proj".to_string(),
                },
                &mut applied_data,
            )
            .await
            .unwrap();
        }

        let edges = orch
            .db
            .local
            .query_all(
                "SELECT parent_issue_id, parent_thread_id, parent_job_id FROM issues ORDER BY number",
                (),
                |row| Ok((row.opt_text(0)?, row.opt_text(1)?, row.opt_text(2)?)),
            )
            .await
            .unwrap();
        assert_eq!(
            edges,
            vec![
                (None, Some("thread".into()), Some("thread-session".into())),
                (None, Some("thread".into()), Some("thread-session".into()))
            ]
        );
    }
}
