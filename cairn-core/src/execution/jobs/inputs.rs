use super::*;

pub(super) async fn load_execution_snapshot_conn(
    conn: &turso::Connection,
    execution_id: &str,
) -> DbResult<Option<ExecutionSnapshot>> {
    let mut rows = conn
        .query(
            "SELECT snapshot FROM executions WHERE id = ?1",
            (execution_id,),
        )
        .await?;
    let snapshot_json = rows
        .next()
        .await?
        .map(|row| row.opt_text(0))
        .transpose()?
        .flatten();
    snapshot_json
        .map(|json| {
            ExecutionSnapshot::from_json(&json)
                .map_err(|e| DbError::Row(format!("Failed to parse execution snapshot: {e}")))
        })
        .transpose()
}

pub(super) async fn require_execution_snapshot_conn(
    conn: &turso::Connection,
    execution_id: &str,
) -> DbResult<ExecutionSnapshot> {
    load_execution_snapshot_conn(conn, execution_id)
        .await?
        .ok_or_else(|| DbError::Row(format!("Execution has no snapshot: {execution_id}")))
}

pub(super) async fn load_nodes_from_execution(
    db: Arc<LocalDb>,
    execution_id: String,
) -> Result<Vec<DbRecipeNode>, String> {
    db.read(|conn| {
        let execution_id = execution_id.clone();
        Box::pin(async move {
            let snapshot = require_execution_snapshot_conn(conn, &execution_id).await?;
            let recipe_id = &snapshot.recipe.id;
            Ok(snapshot
                .recipe
                .nodes
                .iter()
                .map(|node| recipe_node_to_db(node, recipe_id))
                .collect())
        })
    })
    .await
    .map_err(|e| db_error("Failed to load execution nodes", e))
}

pub(super) fn snapshot_edges_to_db(snapshot: &ExecutionSnapshot) -> Vec<DbRecipeEdge> {
    let recipe_id = &snapshot.recipe.id;
    snapshot
        .recipe
        .edges
        .iter()
        .map(|edge| recipe_edge_to_db(edge, recipe_id))
        .collect()
}

pub(super) async fn resolve_inputs_and_schema(
    db: Arc<LocalDb>,
    job: DbJob,
) -> Result<(Vec<ResolvedInput>, Option<OutputSchemaInfo>), String> {
    db.read(|conn| {
        let job = job.clone();
        Box::pin(async move {
            let inputs = resolve_job_inputs_conn(conn, &job).await?;
            let schema = if let (Some(node_id), Some(execution_id)) =
                (&job.recipe_node_id, &job.execution_id)
            {
                find_downstream_artifact_schema_conn(conn, node_id, execution_id).await?
            } else {
                None
            };
            Ok((inputs, schema))
        })
    })
    .await
    .map_err(|e| db_error("Failed to resolve job inputs", e))
}

pub(super) async fn find_job_downstream_artifact_schema(
    db: Arc<LocalDb>,
    job: DbJob,
) -> Result<Option<OutputSchemaInfo>, String> {
    db.read(|conn| {
        let job = job.clone();
        Box::pin(async move {
            if let (Some(node_id), Some(execution_id)) = (&job.recipe_node_id, &job.execution_id) {
                find_downstream_artifact_schema_conn(conn, node_id, execution_id).await
            } else {
                Ok(None)
            }
        })
    })
    .await
    .map_err(|e| db_error("Failed to resolve artifact schema", e))
}

pub(super) async fn resolve_job_inputs_conn(
    conn: &turso::Connection,
    job: &DbJob,
) -> DbResult<Vec<ResolvedInput>> {
    let Some(node_id) = job.recipe_node_id.as_deref() else {
        return Ok(vec![]);
    };
    let Some(execution_id) = job.execution_id.as_deref() else {
        return Ok(vec![]);
    };

    let snapshot = require_execution_snapshot_conn(conn, execution_id).await?;
    let all_nodes: Vec<DbRecipeNode> = snapshot
        .recipe
        .nodes
        .iter()
        .map(|node| recipe_node_to_db(node, &snapshot.recipe.id))
        .collect();
    let all_edges = snapshot_edges_to_db(&snapshot);
    let node_map: HashMap<&str, &DbRecipeNode> = all_nodes
        .iter()
        .map(|node| (node.id.as_str(), node))
        .collect();
    let context_edges: Vec<&DbRecipeEdge> = all_edges
        .iter()
        .filter(|edge| edge.target_node_id == node_id && edge.edge_type == "context")
        .collect();

    let mut inputs = Vec::new();
    for edge in context_edges {
        let Some(source_node) = node_map.get(edge.source_node_id.as_str()).copied() else {
            continue;
        };

        if source_node.node_type == "trigger" {
            inputs.push(build_trigger_context_conn(conn, &snapshot, job).await?);
            continue;
        }

        if source_node.node_type == "context" {
            if let Some(config_str) = &source_node.config {
                if let Ok(config) = serde_json::from_str::<serde_json::Value>(config_str) {
                    if let Some(content) = config.get("content").and_then(|value| value.as_str()) {
                        inputs.push(ResolvedInput {
                            artifact_type: "context".to_string(),
                            data: serde_json::json!({
                                "content": content,
                                "title": source_node.name
                            }),
                        });
                    }
                }
            }
            continue;
        }

        let Some(source_job) = crate::db_records::load_live_job_by_execution_node_conn(
            conn,
            execution_id,
            &edge.source_node_id,
        )
        .await?
        else {
            log::warn!(
                "Source job for node '{}' not found, skipping input",
                edge.source_node_id
            );
            continue;
        };

        if let Some((artifact_type, data_json)) =
            load_artifact_for_edge_conn(conn, &source_job.id, &edge.source_handle).await?
        {
            let data = serde_json::from_str(&data_json).unwrap_or_default();
            inputs.push(ResolvedInput {
                artifact_type,
                data,
            });
        } else if source_job.status == "complete" {
            let last_msg = load_last_assistant_message_conn(conn, &source_job.id).await?;
            if let Some(content) = last_msg {
                inputs.push(ResolvedInput {
                    artifact_type: "task_output".to_string(),
                    data: serde_json::json!({
                        "source_node": source_node.name,
                        "content": content,
                    }),
                });
            }
        }
    }

    Ok(inputs)
}

pub(super) async fn build_trigger_context_conn(
    conn: &turso::Connection,
    snapshot: &ExecutionSnapshot,
    job: &DbJob,
) -> DbResult<ResolvedInput> {
    let mut data = serde_json::json!({});
    let event_payload = snapshot.trigger_context.event_payload.clone();
    if let Some(payload) = event_payload.as_ref() {
        data["event"] = payload.clone();
    }

    let is_accumulated = event_payload
        .as_ref()
        .and_then(|payload| payload.get("accumulated"))
        .and_then(|value| value.as_bool())
        .unwrap_or(false);

    if is_accumulated {
        if let Some(events) = event_payload
            .as_ref()
            .and_then(|payload| payload.get("events"))
            .and_then(|value| value.as_array())
        {
            let mut issues = Vec::new();
            let mut seen_issues = std::collections::HashSet::new();
            for event in events {
                let issue_number = event.get("issueNumber").and_then(|value| value.as_i64());
                let project_key = event
                    .get("projectKey")
                    .and_then(|value| value.as_str())
                    .unwrap_or("");
                if let Some(number) = issue_number {
                    let key = format!("{}:{}", project_key, number);
                    if seen_issues.insert(key) {
                        if let Some((id, title, description)) =
                            load_issue_by_project_key_number_conn(conn, project_key, number as i32)
                                .await?
                        {
                            issues.push(serde_json::json!({
                                "id": id,
                                "key": format!("{}-{}", project_key, number),
                                "title": title,
                                "description": description,
                            }));
                        }
                    }
                }
            }
            if !issues.is_empty() {
                data["issues"] = serde_json::json!(issues);
            }
        }
    } else if let Some(issue_id) = &job.issue_id {
        if let Some((title, description)) =
            load_issue_title_description_conn(conn, issue_id).await?
        {
            data["issue"] = serde_json::json!({
                "id": issue_id,
                "title": title,
                "description": description,
            });
        }
    }

    Ok(ResolvedInput {
        artifact_type: "trigger_context".to_string(),
        data,
    })
}

pub(super) async fn load_issue_by_project_key_number_conn(
    conn: &turso::Connection,
    project_key: &str,
    issue_number: i32,
) -> DbResult<Option<(String, String, Option<String>)>> {
    let mut rows = conn
        .query(
            "SELECT issues.id, issues.title, issues.description
             FROM issues
             INNER JOIN projects ON projects.id = issues.project_id
             WHERE projects.key = ?1 AND issues.number = ?2",
            params![project_key, issue_number],
        )
        .await?;
    rows.next()
        .await?
        .map(|row| Ok::<_, DbError>((row.text(0)?, row.text(1)?, row.opt_text(2)?)))
        .transpose()
}

pub(super) async fn load_issue_title_description_conn(
    conn: &turso::Connection,
    issue_id: &str,
) -> DbResult<Option<(String, Option<String>)>> {
    let mut rows = conn
        .query(
            "SELECT title, description FROM issues WHERE id = ?1",
            (issue_id,),
        )
        .await?;
    rows.next()
        .await?
        .map(|row| Ok::<_, DbError>((row.text(0)?, row.opt_text(1)?)))
        .transpose()
}

pub(super) async fn load_artifact_for_edge_conn(
    conn: &turso::Connection,
    job_id: &str,
    output_name: &str,
) -> DbResult<Option<(String, String)>> {
    let mut exact = conn
        .query(
            "SELECT id, artifact_type, data FROM artifacts
             WHERE job_id = ?1 AND output_name = ?2
             ORDER BY version DESC LIMIT 1",
            params![job_id, output_name],
        )
        .await?;
    if let Some(row) = exact.next().await? {
        return Ok(Some((row.text(1)?, row.text(2)?)));
    }

    let mut fallback = conn
        .query(
            "SELECT id, artifact_type, data FROM artifacts
             WHERE job_id = ?1 ORDER BY version DESC LIMIT 1",
            (job_id,),
        )
        .await?;
    if let Some(row) = fallback.next().await? {
        return Ok(Some((row.text(1)?, row.text(2)?)));
    }

    Ok(None)
}

pub(super) async fn load_last_assistant_message_conn(
    conn: &turso::Connection,
    job_id: &str,
) -> DbResult<Option<String>> {
    let mut rows = conn
        .query(
            "SELECT events.data
             FROM runs
             INNER JOIN events ON events.run_id = runs.id
             WHERE runs.job_id = ?1 AND events.event_type = 'assistant'
             ORDER BY events.sequence DESC LIMIT 1",
            (job_id,),
        )
        .await?;
    let data_json = crate::storage::next_text(&mut rows, 0).await?;
    Ok(data_json.and_then(|data| {
        serde_json::from_str::<TranscriptEvent>(&data)
            .ok()
            .and_then(|event| event.content)
    }))
}

pub(crate) async fn find_downstream_artifact_schema_conn(
    conn: &turso::Connection,
    node_id: &str,
    execution_id: &str,
) -> DbResult<Option<OutputSchemaInfo>> {
    let snapshot = require_execution_snapshot_conn(conn, execution_id).await?;
    find_downstream_artifact_schema_with_snapshot_conn(conn, &snapshot, node_id).await
}

/// Resolve a node's effective output contract against an already-loaded
/// snapshot. Returns the producer's own declared output schema, or — when it
/// declares none — the `inputSchema` of a downstream `action`/`pr`/`artifact`
/// consumer reached by a context edge. `Some` here means "this node was told to
/// produce an artifact"; the job-completion gate keys on exactly that. Splitting
/// the snapshot load out lets the recompute sweep reuse its loaded snapshot.
pub(crate) async fn find_downstream_artifact_schema_with_snapshot_conn(
    conn: &turso::Connection,
    snapshot: &ExecutionSnapshot,
    node_id: &str,
) -> DbResult<Option<OutputSchemaInfo>> {
    let node_map: HashMap<&str, &RecipeNode> = snapshot
        .recipe
        .nodes
        .iter()
        .map(|node| (node.id.as_str(), node))
        .collect();
    let context_edges: Vec<&RecipeEdge> = snapshot
        .recipe
        .edges
        .iter()
        .filter(|edge| edge.edge_type.to_string() == "context" && edge.source_node_id == node_id)
        .collect();

    // The producing node's own output schema. Even when the field *shape* is
    // inherited from a downstream action, the URI the agent writes to and the
    // confirm gate belong to this node, so its declared name and confirm policy
    // win over anything carried by the inherited shape.
    let own_schema =
        node_map
            .get(node_id)
            .and_then(|node| match node.node_type.to_string().as_str() {
                "agent" => node
                    .agent_config
                    .as_ref()
                    .and_then(|c| c.output_schema.as_ref()),
                "action" => node
                    .action_config
                    .as_ref()
                    .and_then(|c| c.output_schema.as_ref()),
                _ => None,
            });
    let own_artifact_name = own_schema.map(|s| s.name.clone()).filter(|n| !n.is_empty());
    let own_confirm_policy = own_schema.map(|s| s.confirm_policy);
    let stamp = |info: Option<OutputSchemaInfo>| {
        info.map(|mut info| {
            if info.artifact_name.is_none() {
                info.artifact_name = own_artifact_name.clone();
            }
            if let Some(policy) = own_confirm_policy {
                info.confirm_policy = policy;
            }
            info
        })
    };

    if let Some(node) = node_map.get(node_id) {
        if node.node_type.to_string() == "agent" {
            if let Some(ref agent_cfg) = node.agent_config {
                if let Some(ref output_schema) = agent_cfg.output_schema {
                    if let Some(schema_info) = extract_schema_from_slot_config(output_schema)? {
                        return Ok(stamp(Some(schema_info)));
                    }
                }
            }
        }
    }

    log::info!(
        "find_downstream_artifact_schema: node_id={}, recipe={}, found {} context edges",
        node_id,
        snapshot.recipe.id,
        context_edges.len()
    );

    for edge in context_edges {
        if let Some(target_node) = node_map.get(edge.target_node_id.as_str()) {
            let target_type = target_node.node_type.to_string();
            // A consumer node — a generic `action` or a first-class `pr` — declares
            // the producer's output contract (name + shape) via its `inputSchema`.
            // This is the single source of the artifact's `cairn:~/<name>` address;
            // the agent is instructed with exactly this name (CAIRN-1219).
            if target_type == "action" || target_type == "pr" {
                if let Some(ref action_cfg) = target_node.action_config {
                    if let Some(ref input_schema) = action_cfg.input_schema {
                        if let Some(mut schema_info) =
                            extract_schema_from_slot_config(input_schema)?
                        {
                            // A `pr` consumer always auto-confirms: the PR lifecycle
                            // (open → merge/close) is the human gate, never a pre-PR
                            // artifact confirmation. The producer declares nothing,
                            // so the contract — name, shape, auto-confirm — is wholly
                            // the pr node's. Bypass `stamp` (no producer override).
                            if target_type == "pr" {
                                schema_info.confirm_policy = crate::models::ConfirmPolicy::Auto;
                                if schema_info.artifact_name.is_none() {
                                    schema_info.artifact_name = own_artifact_name.clone();
                                }
                                return Ok(Some(schema_info));
                            }
                            return Ok(stamp(Some(schema_info)));
                        }
                    }

                    // Legacy `create_pr`-style action: resolve the schema from the
                    // referenced action_config. (A `pr` node has no action_config_id
                    // and an empty `action`, so this is action-only.)
                    let action_config_id = action_cfg.action_config_id.clone().or_else(|| {
                        if !action_cfg.action.is_empty() {
                            Some(format!("builtin:{}", action_cfg.action))
                        } else {
                            None
                        }
                    });

                    if let Some(action_config_id) = action_config_id {
                        if let Some(schema_info) =
                            load_action_config_schema_conn(conn, &action_config_id).await?
                        {
                            return Ok(stamp(Some(schema_info)));
                        }
                    }
                }
            }

            if target_type == "artifact" {
                if let Some(ref agent_cfg) = target_node.agent_config {
                    if let Some(ref output_schema) = agent_cfg.output_schema {
                        if let Some(schema_info) = extract_schema_from_slot_config(output_schema)? {
                            return Ok(stamp(Some(schema_info)));
                        }
                    }
                }
            }
        }
    }

    Ok(stamp(None))
}

pub(super) async fn load_action_config_schema_conn(
    conn: &turso::Connection,
    action_config_id: &str,
) -> DbResult<Option<OutputSchemaInfo>> {
    let mut rows = conn
        .query(
            "SELECT input_schema, tool_name, tool_description
             FROM action_configs WHERE id = ?1",
            (action_config_id,),
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };
    let Some(schema_str) = row.opt_text(0)? else {
        return Ok(None);
    };
    let Ok(schema_json) = serde_json::from_str::<serde_json::Value>(&schema_str) else {
        return Ok(None);
    };
    Ok(Some(OutputSchemaInfo {
        schema: OutputSchema::Custom(schema_json),
        artifact_name: None,
        confirm_policy: crate::models::ConfirmPolicy::default(),
        tool_name: row.opt_text(1)?,
        description: row.opt_text(2)?,
    }))
}

pub(super) fn extract_schema_from_slot_config(
    schema_config: &crate::models::SchemaConfig,
) -> DbResult<Option<OutputSchemaInfo>> {
    let tool_name = schema_config.tool_name.clone();
    let description = schema_config.description.clone();
    let confirm_policy = schema_config.confirm_policy;
    let artifact_name = Some(schema_config.name.clone()).filter(|n| !n.is_empty());

    // The shape is the node's own self-contained inline JSON Schema. `None` means
    // it inherits the shape from a downstream context-edge target.
    if let Some(schema_json) = &schema_config.schema {
        return Ok(Some(OutputSchemaInfo {
            schema: OutputSchema::Custom(schema_json.clone()),
            artifact_name,
            confirm_policy,
            tool_name,
            description,
        }));
    }

    Ok(None)
}
