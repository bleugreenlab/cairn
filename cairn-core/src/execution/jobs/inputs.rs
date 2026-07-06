use super::*;

pub(super) async fn load_execution_snapshot_conn(
    conn: &cairn_db::turso::Connection,
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
            crate::config::snapshot_migrate::load(&json)
                .map_err(|e| DbError::Row(format!("Failed to parse execution snapshot: {e}")))
        })
        .transpose()
}

pub(super) async fn require_execution_snapshot_conn(
    conn: &cairn_db::turso::Connection,
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
    conn: &cairn_db::turso::Connection,
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
    // Typed view of the snapshot nodes, for ArtifactNode source resolution.
    let rnode_map: HashMap<&str, &RecipeNode> = snapshot
        .recipe
        .nodes
        .iter()
        .map(|node| (node.id.as_str(), node))
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

        if source_node.node_type == "artifact" {
            // Port model: the consumer reads from an ArtifactNode. Load the
            // PRODUCER's artifact (whoever's context-out targets this node), keyed
            // by the ArtifactNode's name. A literal-content ArtifactNode with no
            // producer contributes its static content instead.
            if let Some(input) = load_artifact_node_input_conn(
                conn,
                execution_id,
                &rnode_map,
                &all_edges,
                edge.source_node_id.as_str(),
            )
            .await?
            {
                inputs.push(input);
            }
            continue;
        }

        // An Instruction node's content is injected into the running node's
        // SYSTEM PROMPT (resolve_instruction_prompt_conn), never delivered as a
        // job input. It runs no job, so skip it explicitly rather than falling
        // through to a doomed live-job load that would log a spurious "source job
        // not found" warning on every resolve.
        if source_node.node_type == "instruction" {
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

        // Direct agent→consumer context edge (no intermediate ArtifactNode). The
        // producer stores its terminal artifact under its resolved CONTRACT name
        // (e.g. `create-pr`), never under the raw edge handle (`context-out`).
        // Key the load by that name, and pass the producer's context-self
        // living-doc names so the latest-across-job fallback can never serve a
        // patched, higher-versioned ctx-self doc as this consumer's terminal
        // input in place of the real terminal artifact (CAIRN-1953).
        let terminal_name = find_downstream_artifact_schema_with_snapshot_conn(
            conn,
            &snapshot,
            edge.source_node_id.as_str(),
        )
        .await?
        .and_then(|info| info.artifact_name);
        let ctx_self_names = ctx_self_artifact_names(&snapshot, edge.source_node_id.as_str());
        let load_name = terminal_name
            .as_deref()
            .unwrap_or(edge.source_handle.as_str());

        if let Some((artifact_type, data_json)) =
            load_artifact_for_edge_conn(conn, &source_job.id, load_name, &ctx_self_names).await?
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

/// Resolve a consumer's input from an ArtifactNode on its `context-in` edge.
///
/// The producer is whoever's `context-out` edge targets this ArtifactNode; load
/// that producer's artifact keyed by the ArtifactNode's `name` (its stored
/// `output_name`). When the ArtifactNode has no producer but carries inline
/// literal `content`, it is a static input document (the collapsed Context node).
pub(super) async fn load_artifact_node_input_conn(
    conn: &cairn_db::turso::Connection,
    execution_id: &str,
    rnode_map: &HashMap<&str, &RecipeNode>,
    all_edges: &[DbRecipeEdge],
    artifact_node_id: &str,
) -> DbResult<Option<ResolvedInput>> {
    let Some(artifact_node) = rnode_map.get(artifact_node_id).copied() else {
        return Ok(None);
    };
    let cfg = artifact_node.artifact_config.as_ref();
    let name = cfg
        .map(|c| c.name.clone())
        .filter(|n| !n.is_empty())
        .unwrap_or_default();

    // Producer = the node whose context-out edge targets this ArtifactNode.
    let producer_id = all_edges
        .iter()
        .find(|edge| {
            edge.edge_type == "context"
                && edge.source_handle == crate::models::CONTEXT_OUT_HANDLE
                && edge.target_node_id == artifact_node_id
        })
        .map(|edge| edge.source_node_id.clone());

    if let Some(producer_id) = producer_id {
        if let Some(producer_job) = crate::db_records::load_live_job_by_execution_node_conn(
            conn,
            execution_id,
            &producer_id,
        )
        .await?
        {
            let ctx_self_names = ctx_self_names_from_edges(rnode_map, all_edges, &producer_id);
            if let Some((artifact_type, data_json)) =
                load_artifact_for_edge_conn(conn, &producer_job.id, &name, &ctx_self_names).await?
            {
                let data = serde_json::from_str(&data_json).unwrap_or_default();
                return Ok(Some(ResolvedInput {
                    artifact_type,
                    data,
                }));
            }
        }
        return Ok(None);
    }

    // No producer: a literal-content ArtifactNode is a static input document.
    if let Some(content) = cfg.and_then(|c| c.content.as_ref()) {
        return Ok(Some(ResolvedInput {
            artifact_type: "context".to_string(),
            data: serde_json::json!({
                "content": content,
                "title": artifact_node.name,
            }),
        }));
    }

    Ok(None)
}

pub(super) async fn build_trigger_context_conn(
    conn: &cairn_db::turso::Connection,
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
    conn: &cairn_db::turso::Connection,
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
    conn: &cairn_db::turso::Connection,
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

/// The producer's `context-self` living-doc artifact names. These are never a
/// consumer's terminal input, so they are excluded from the latest-across-job
/// fallback in [`load_artifact_for_edge_conn`].
fn ctx_self_artifact_names(snapshot: &ExecutionSnapshot, node_id: &str) -> Vec<String> {
    resolve_ctx_self_schemas_with_snapshot(snapshot, node_id)
        .into_iter()
        .filter_map(|info| info.artifact_name)
        .collect()
}

/// `context-self` artifact names for a producer node, computed directly from the
/// snapshot edges and typed node map (used on the ArtifactNode path, where the
/// full [`ExecutionSnapshot`] isn't threaded through).
fn ctx_self_names_from_edges(
    rnode_map: &HashMap<&str, &RecipeNode>,
    all_edges: &[DbRecipeEdge],
    node_id: &str,
) -> Vec<String> {
    all_edges
        .iter()
        .filter(|edge| {
            edge.edge_type == "context"
                && edge.source_node_id == node_id
                && edge.source_handle == crate::models::CONTEXT_SELF_HANDLE
        })
        .filter_map(|edge| rnode_map.get(edge.target_node_id.as_str()).copied())
        .filter_map(|node| node.artifact_config.as_ref())
        .map(|cfg| cfg.name.clone())
        .filter(|name| !name.is_empty())
        .collect()
}

/// Load a producer's artifact for a consumer edge.
///
/// Match first by the resolved artifact `output_name` (the terminal/ArtifactNode
/// contract name). If that misses, fall back to the latest artifact across the
/// job — but never one whose `output_name` is in `exclude_from_fallback`. That
/// exclusion carries the producer's `context-self` living-doc names so a patched,
/// higher-versioned ctx-self doc (e.g. `plan`) can never be served as a
/// consumer's terminal input in place of the real terminal artifact (e.g.
/// `create-pr`).
pub(super) async fn load_artifact_for_edge_conn(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
    output_name: &str,
    exclude_from_fallback: &[String],
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
            "SELECT id, artifact_type, data, output_name FROM artifacts
             WHERE job_id = ?1 ORDER BY version DESC",
            (job_id,),
        )
        .await?;
    while let Some(row) = fallback.next().await? {
        if let Some(name) = row.opt_text(3)? {
            if exclude_from_fallback
                .iter()
                .any(|excluded| excluded == &name)
            {
                continue;
            }
        }
        return Ok(Some((row.text(1)?, row.text(2)?)));
    }

    Ok(None)
}

pub(super) async fn load_last_assistant_message_conn(
    conn: &cairn_db::turso::Connection,
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
    conn: &cairn_db::turso::Connection,
    node_id: &str,
    execution_id: &str,
) -> DbResult<Option<OutputSchemaInfo>> {
    let snapshot = require_execution_snapshot_conn(conn, execution_id).await?;
    find_downstream_artifact_schema_with_snapshot_conn(conn, &snapshot, node_id).await
}

/// Resolve a node's effective terminal output contract against an already-loaded
/// snapshot.
///
/// Port model: the contract is the schema of the node's terminal `context-out`
/// edge target — an [`ArtifactNode`](crate::models::ArtifactNodeConfig) (carrying
/// `name` + `schema` + `confirm_policy`) or an `action`/`pr` input port. A
/// `context-out` edge may also fan out directly to agent consumers; those edges
/// carry the produced artifact as input but do not declare the terminal contract.
/// `Some` here means "this node was told to produce a terminal artifact"; the
/// job-completion gate keys on exactly that.
///
/// Back-compat: old execution snapshots predate ArtifactNodes and carry an inline
/// agent/action `outputSchema` plus a direct `context-out` edge to the consumer.
/// When no ctx-out target carries a schema (for example it points straight at
/// another agent), the node's own inline `outputSchema` is the contract. Splitting
/// the snapshot load out lets the recompute sweep reuse its loaded snapshot.
pub(crate) async fn find_downstream_artifact_schema_with_snapshot_conn(
    conn: &cairn_db::turso::Connection,
    snapshot: &ExecutionSnapshot,
    node_id: &str,
) -> DbResult<Option<OutputSchemaInfo>> {
    let node_map: HashMap<&str, &RecipeNode> = snapshot
        .recipe
        .nodes
        .iter()
        .map(|node| (node.id.as_str(), node))
        .collect();

    // Port model: follow context-out edges until one declares a terminal schema.
    // Direct agent consumers are fanout inputs, not terminal contract targets, so
    // they are skipped rather than making contract resolution depend on edge order.
    for edge in snapshot.recipe.edges.iter().filter(|edge| {
        edge.edge_type.to_string() == "context"
            && edge.source_node_id == node_id
            && edge.source_handle == crate::models::CONTEXT_OUT_HANDLE
    }) {
        if let Some(info) =
            resolve_ctx_out_target_schema(conn, &node_map, &edge.target_node_id).await?
        {
            return Ok(Some(info));
        }
    }

    // Back-compat: a pre-port snapshot carries the contract inline on the node.
    legacy_own_output_schema(&node_map, node_id)
}

/// Resolve the terminal contract carried by a `context-out` edge's target: an
/// ArtifactNode's typed schema, or an `action`/`pr` node's input port. A `pr`
/// target always auto-confirms — the PR lifecycle (open → merge/close) is the
/// human gate, never a pre-PR artifact confirmation (CAIRN-1219). An ArtifactNode
/// with no `schema` (a pure literal-content input document) is not a terminal
/// contract.
async fn resolve_ctx_out_target_schema(
    conn: &cairn_db::turso::Connection,
    node_map: &HashMap<&str, &RecipeNode>,
    target_node_id: &str,
) -> DbResult<Option<OutputSchemaInfo>> {
    let Some(target) = node_map.get(target_node_id) else {
        return Ok(None);
    };
    match target.node_type.to_string().as_str() {
        "artifact" => {
            let Some(cfg) = target.artifact_config.as_ref() else {
                return Ok(None);
            };
            let Some(schema) = cfg.schema.clone() else {
                return Ok(None);
            };
            Ok(Some(OutputSchemaInfo {
                schema: OutputSchema::Custom(schema),
                artifact_name: Some(cfg.name.clone()).filter(|n| !n.is_empty()),
                confirm_policy: cfg.confirm_policy,
                tool_name: None,
                description: None,
            }))
        }
        "action" | "pr" => {
            let Some(action_cfg) = target.action_config.as_ref() else {
                return Ok(None);
            };
            if let Some(input_schema) = action_cfg.input_schema.as_ref() {
                if let Some(mut info) = extract_schema_from_slot_config(input_schema)? {
                    if target.node_type.to_string() == "pr" {
                        info.confirm_policy = crate::models::ConfirmPolicy::Auto;
                    }
                    return Ok(Some(info));
                }
            }
            // Legacy `create_pr`-style action referencing an action_config row.
            let action_config_id = action_cfg.action_config_id.clone().or_else(|| {
                (!action_cfg.action.is_empty()).then(|| format!("builtin:{}", action_cfg.action))
            });
            if let Some(action_config_id) = action_config_id {
                return load_action_config_schema_conn(conn, &action_config_id).await;
            }
            Ok(None)
        }
        _ => Ok(None),
    }
}

/// Back-compat resolver for pre-port snapshots: the node's own inline output
/// schema (agent or action). Returns `None` for the new authored model, where
/// agents carry no inline `outputSchema`.
fn legacy_own_output_schema(
    node_map: &HashMap<&str, &RecipeNode>,
    node_id: &str,
) -> DbResult<Option<OutputSchemaInfo>> {
    let Some(node) = node_map.get(node_id) else {
        return Ok(None);
    };
    let own = match node.node_type.to_string().as_str() {
        "agent" => node
            .agent_config
            .as_ref()
            .and_then(|c| c.output_schema.as_ref()),
        "action" => node
            .action_config
            .as_ref()
            .and_then(|c| c.output_schema.as_ref()),
        _ => None,
    };
    match own {
        Some(schema) => extract_schema_from_slot_config(schema),
        None => Ok(None),
    }
}

/// Enumerate a node's `context-self` living-doc targets: the ArtifactNodes the
/// node owns and patches across its life. Each carries its own name + schema used
/// to validate ctx-self writes; they are NOT the terminal contract and never gate
/// the job. ArtifactNodes with no schema are skipped (nothing to validate).
pub(crate) async fn resolve_ctx_self_schemas_conn(
    conn: &cairn_db::turso::Connection,
    node_id: &str,
    execution_id: &str,
) -> DbResult<Vec<OutputSchemaInfo>> {
    let snapshot = require_execution_snapshot_conn(conn, execution_id).await?;
    Ok(resolve_ctx_self_schemas_with_snapshot(&snapshot, node_id))
}

pub(crate) fn resolve_ctx_self_schemas_with_snapshot(
    snapshot: &ExecutionSnapshot,
    node_id: &str,
) -> Vec<OutputSchemaInfo> {
    let node_map: HashMap<&str, &RecipeNode> = snapshot
        .recipe
        .nodes
        .iter()
        .map(|node| (node.id.as_str(), node))
        .collect();
    snapshot
        .recipe
        .edges
        .iter()
        .filter(|edge| {
            edge.edge_type.to_string() == "context"
                && edge.source_node_id == node_id
                && edge.source_handle == crate::models::CONTEXT_SELF_HANDLE
        })
        .filter_map(|edge| {
            let target = node_map.get(edge.target_node_id.as_str())?;
            let cfg = target.artifact_config.as_ref()?;
            let schema = cfg.schema.clone()?;
            Some(OutputSchemaInfo {
                schema: OutputSchema::Custom(schema),
                artifact_name: Some(cfg.name.clone()).filter(|n| !n.is_empty()),
                // ctx-self living docs are always auto-confirm; they never gate.
                confirm_policy: crate::models::ConfirmPolicy::Auto,
                tool_name: None,
                description: None,
            })
        })
        .collect()
}

/// Derive whether an agent node is "long-running" — settles `Idle` at clean
/// turn-end instead of `Complete` — from the execution's recipe topology, not
/// from any node flag. A node is long-running iff ALL hold:
///   (a) it declares no resolvable output contract (`requires_output == false`,
///       the caller's already-computed fact — its own schema or one inherited via
///       a context-out edge);
///   (b) it has no outgoing control edge (it is a control-terminal); and
///   (c) the recipe contains no terminal action node — no `pr` node and no
///       `action` node anywhere.
///
/// Intuitively: a standing recipe is one whose shape has no terminal action, so
/// its final contract-less control-terminal agent keeps taking wakes instead of
/// completing. This is the single source of the `long_running` fact — both the
/// completion projection (fact gathering in `execution::advancement::recompute`)
/// and coordinator prompt-mode resolution call it, so the rule lives in exactly
/// one place. Non-agent nodes never qualify.
pub(crate) fn is_long_running_node(
    snapshot: &ExecutionSnapshot,
    node_id: &str,
    requires_output: bool,
) -> bool {
    // Only an agent node with no output contract can stand idle; trigger,
    // artifact, and action/pr nodes always complete to advance the DAG.
    let Some(node) = snapshot.recipe.nodes.iter().find(|n| n.id == node_id) else {
        return false;
    };
    if node.agent_config.is_none() || requires_output {
        return false;
    }
    // (b) A control-terminal has no outgoing control edge — nothing downstream
    // depends on it completing.
    let has_outgoing_control = snapshot.recipe.edges.iter().any(|edge| {
        edge.edge_type == crate::models::RecipeEdgeType::Control && edge.source_node_id == node_id
    });
    if has_outgoing_control {
        return false;
    }
    // (c) A recipe with any terminal action node (`pr` or `action`) is a shipping
    // recipe, not a standing one, so no node in it stands idle.
    !snapshot.recipe.nodes.iter().any(|n| {
        matches!(
            n.node_type,
            crate::models::RecipeNodeType::Pr | crate::models::RecipeNodeType::Action
        )
    })
}

/// Assemble the system-prompt instruction text a running node inherits from its
/// upstream Instruction nodes. Loads the execution snapshot and delegates to
/// [`instruction_prompt_from_snapshot`] for the selection and ordering rules.
/// Returns an empty string when the node has no upstream Instruction node, so
/// injection is purely additive.
pub(crate) async fn resolve_instruction_prompt_conn(
    conn: &cairn_db::turso::Connection,
    node_id: &str,
    execution_id: &str,
) -> DbResult<String> {
    let snapshot = require_execution_snapshot_conn(conn, execution_id).await?;
    Ok(instruction_prompt_from_snapshot(&snapshot, node_id))
}

/// Join the content of every Instruction node whose `context-out` feeds
/// `node_id` via a context edge, in a deterministic top-to-bottom layout order
/// (source `position.y`, then `position.x`, then edge index) so a recipe
/// author's arrangement composes predictably. Multiple Instruction nodes join
/// with a blank line. Only `Instruction` sources contribute — a `Context` node,
/// whose content is a work packet delivered to the initial user message rather
/// than framing, is deliberately ignored — so the result is empty for any
/// recipe with no Instruction node.
pub(crate) fn instruction_prompt_from_snapshot(
    snapshot: &ExecutionSnapshot,
    node_id: &str,
) -> String {
    let mut matched: Vec<(f32, f32, usize, &str)> = Vec::new();
    for (idx, edge) in snapshot.recipe.edges.iter().enumerate() {
        if edge.edge_type != crate::models::RecipeEdgeType::Context
            || edge.target_node_id != node_id
        {
            continue;
        }
        let Some(source) = snapshot
            .recipe
            .nodes
            .iter()
            .find(|n| n.id == edge.source_node_id)
        else {
            continue;
        };
        if source.node_type != crate::models::RecipeNodeType::Instruction {
            continue;
        }
        if let Some(content) = source.context_config.as_ref().map(|c| c.content.as_str()) {
            matched.push((source.position.y, source.position.x, idx, content));
        }
    }
    matched.sort_by(|a, b| {
        a.0.partial_cmp(&b.0)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
            .then(a.2.cmp(&b.2))
    });
    matched
        .iter()
        .map(|(_, _, _, content)| *content)
        .collect::<Vec<_>>()
        .join("\n\n")
}

pub(super) async fn load_action_config_schema_conn(
    conn: &cairn_db::turso::Connection,
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

#[cfg(test)]
mod port_model_tests {
    use super::*;
    use crate::models::{
        ActionNodeConfig, AgentNodeConfig, ArtifactNodeConfig, ConfirmPolicy, ContextNodeConfig,
        NodePosition, RecipeEdge, RecipeEdgeType, RecipeNode, RecipeNodeType, RecipeSnapshot,
        RecipeTrigger, SchemaConfig, TriggerContext, TriggerType,
    };
    use crate::storage::{MigrationRunner, TURSO_MIGRATIONS};
    use std::collections::HashMap;

    fn agent(id: &str) -> RecipeNode {
        RecipeNode {
            id: id.to_string(),
            node_type: RecipeNodeType::Agent,
            name: id.to_string(),
            position: NodePosition { x: 0.0, y: 0.0 },
            parent_id: None,
            trigger_config: None,
            agent_config: Some(AgentNodeConfig {
                agent_config_id: Some(id.to_string()),
                output_schema: None,
                git_config: None,
            }),
            action_config: None,
            checkpoint_config: None,
            artifact_config: None,
            condition_config: None,
            context_config: None,
        }
    }

    fn artifact_node(id: &str, name: &str, policy: ConfirmPolicy) -> RecipeNode {
        RecipeNode {
            id: id.to_string(),
            node_type: RecipeNodeType::Artifact,
            name: id.to_string(),
            position: NodePosition { x: 0.0, y: 0.0 },
            parent_id: None,
            trigger_config: None,
            agent_config: None,
            action_config: None,
            checkpoint_config: None,
            artifact_config: Some(ArtifactNodeConfig {
                name: name.to_string(),
                schema: Some(serde_json::json!({
                    "type": "object",
                    "required": ["title", "content"],
                    "properties": {
                        "title": { "type": "string" },
                        "content": { "type": "string" }
                    }
                })),
                confirm_policy: policy,
                content: None,
            }),
            condition_config: None,
            context_config: None,
        }
    }

    fn pr_node(id: &str, name: &str) -> RecipeNode {
        RecipeNode {
            id: id.to_string(),
            node_type: RecipeNodeType::Pr,
            name: id.to_string(),
            position: NodePosition { x: 0.0, y: 0.0 },
            parent_id: None,
            trigger_config: None,
            agent_config: None,
            action_config: Some(ActionNodeConfig {
                action_config_id: None,
                action: String::new(),
                action_params: serde_json::Value::Null,
                input_schema: Some(SchemaConfig {
                    name: name.to_string(),
                    schema: Some(serde_json::json!({"type": "object"})),
                    // A `user` policy here must be overridden to `auto` by the
                    // resolver: the PR lifecycle is the gate (CAIRN-1219).
                    confirm_policy: ConfirmPolicy::User,
                    tool_name: None,
                    description: None,
                }),
                output_schema: None,
            }),
            checkpoint_config: None,
            artifact_config: None,
            condition_config: None,
            context_config: None,
        }
    }

    fn ctx_edge(id: &str, from: &str, from_handle: &str, to: &str, to_handle: &str) -> RecipeEdge {
        RecipeEdge {
            id: id.to_string(),
            edge_type: RecipeEdgeType::Context,
            source_node_id: from.to_string(),
            source_handle: from_handle.to_string(),
            target_node_id: to.to_string(),
            target_handle: to_handle.to_string(),
        }
    }

    fn snapshot(nodes: Vec<RecipeNode>, edges: Vec<RecipeEdge>) -> ExecutionSnapshot {
        ExecutionSnapshot {
            recipe: RecipeSnapshot {
                id: "recipe-1".to_string(),
                name: "Recipe".to_string(),
                description: None,
                trigger: RecipeTrigger::Manual,
                nodes,
                edges,
            },
            agents: HashMap::new(),
            skills: HashMap::new(),
            trigger_context: TriggerContext {
                issue_id: Some("i-1".to_string()),
                project_id: "p-1".to_string(),
                trigger_type: TriggerType::Manual,
                event_payload: None,
                initiated_via: None,
            },
            presets: None,
            delegated_packets: vec![],
            created_at: 1,
        }
    }

    fn instruction_node(id: &str, content: &str, x: f32, y: f32) -> RecipeNode {
        RecipeNode {
            id: id.to_string(),
            node_type: RecipeNodeType::Instruction,
            name: id.to_string(),
            position: NodePosition { x, y },
            parent_id: None,
            trigger_config: None,
            agent_config: None,
            action_config: None,
            checkpoint_config: None,
            artifact_config: None,
            condition_config: None,
            context_config: Some(ContextNodeConfig {
                content: content.to_string(),
            }),
        }
    }

    #[test]
    fn instruction_prompt_reads_single_upstream_node() {
        // An Instruction node wired context-out -> agent context-in contributes
        // its content to that agent's system-prompt instruction text.
        let snap = snapshot(
            vec![
                agent("coordinator"),
                instruction_node("i1", "framing", 0.0, 0.0),
            ],
            vec![ctx_edge(
                "e1",
                "i1",
                "context-out",
                "coordinator",
                "context-in",
            )],
        );
        assert_eq!(
            instruction_prompt_from_snapshot(&snap, "coordinator"),
            "framing"
        );
    }

    #[test]
    fn instruction_prompt_composes_in_layout_order() {
        // Two Instruction nodes both feed the agent; they compose top-to-bottom
        // by source (y, x), joined with a blank line — independent of edge order.
        let snap = snapshot(
            vec![
                agent("coordinator"),
                instruction_node("lower", "second", 0.0, 100.0),
                instruction_node("upper", "first", 0.0, 10.0),
            ],
            vec![
                ctx_edge("e1", "lower", "context-out", "coordinator", "context-in"),
                ctx_edge("e2", "upper", "context-out", "coordinator", "context-in"),
            ],
        );
        assert_eq!(
            instruction_prompt_from_snapshot(&snap, "coordinator"),
            "first\n\nsecond"
        );
    }

    #[test]
    fn instruction_prompt_empty_without_instruction_edge() {
        // A recipe with no Instruction node yields the bare role prompt: a
        // Context node feeding the agent is deliberately NOT treated as framing.
        let snap = snapshot(
            vec![
                agent("coordinator"),
                artifact_node("a", "plan", ConfirmPolicy::Auto),
            ],
            vec![ctx_edge(
                "e1",
                "a",
                "context-out",
                "coordinator",
                "context-in",
            )],
        );
        assert_eq!(instruction_prompt_from_snapshot(&snap, "coordinator"), "");
    }

    async fn test_db() -> LocalDb {
        let temp = tempfile::tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("inputs.db")).await.unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        // Leak the tempdir so the file outlives the test body.
        std::mem::forget(temp);
        db
    }

    /// planner --context-out--> plan(ArtifactNode) --context-out--> builder,
    /// builder --context-out--> pr. Plus an optional ctx-self `notes` node.
    fn planbuild_snapshot(with_ctx_self: bool) -> ExecutionSnapshot {
        let mut nodes = vec![
            agent("planner"),
            artifact_node("plan-node", "plan", ConfirmPolicy::User),
            agent("builder"),
            pr_node("pr", "create-pr"),
        ];
        let mut edges = vec![
            ctx_edge("e1", "planner", "context-out", "plan-node", "context-in"),
            ctx_edge("e2", "plan-node", "context-out", "builder", "context-in"),
            ctx_edge("e3", "builder", "context-out", "pr", "context-in"),
        ];
        if with_ctx_self {
            nodes.push(artifact_node("notes-node", "notes", ConfirmPolicy::Auto));
            edges.push(ctx_edge(
                "e4",
                "planner",
                "context-self",
                "notes-node",
                "context-in",
            ));
        }
        snapshot(nodes, edges)
    }

    #[tokio::test]
    async fn terminal_contract_follows_context_out_to_artifact_node() {
        let db = test_db().await;
        let snap = planbuild_snapshot(false);
        let info = db
            .read(move |conn| {
                let snap = snap.clone();
                Box::pin(async move {
                    find_downstream_artifact_schema_with_snapshot_conn(conn, &snap, "planner").await
                })
            })
            .await
            .unwrap()
            .expect("planner has a terminal contract");
        assert_eq!(info.artifact_name.as_deref(), Some("plan"));
        assert_eq!(info.confirm_policy, ConfirmPolicy::User);
        assert!(matches!(info.schema, OutputSchema::Custom(_)));
    }

    #[tokio::test]
    async fn pr_target_forces_auto_confirm() {
        let db = test_db().await;
        let snap = planbuild_snapshot(false);
        let info = db
            .read(move |conn| {
                let snap = snap.clone();
                Box::pin(async move {
                    find_downstream_artifact_schema_with_snapshot_conn(conn, &snap, "builder").await
                })
            })
            .await
            .unwrap()
            .expect("builder feeds the pr");
        assert_eq!(info.artifact_name.as_deref(), Some("create-pr"));
        // The pr input schema declared `user`, but the PR lifecycle is the gate.
        assert_eq!(info.confirm_policy, ConfirmPolicy::Auto);
    }

    #[tokio::test]
    async fn direct_agent_context_out_fanout_does_not_hide_terminal_contract() {
        let db = test_db().await;
        let snap = snapshot(
            vec![
                agent("builder"),
                agent("reviewer"),
                pr_node("pr", "create-pr"),
            ],
            vec![
                ctx_edge("e1", "builder", "context-out", "reviewer", "context-in"),
                ctx_edge("e2", "builder", "context-out", "pr", "context-in"),
            ],
        );
        let info = db
            .read(move |conn| {
                let snap = snap.clone();
                Box::pin(async move {
                    find_downstream_artifact_schema_with_snapshot_conn(conn, &snap, "builder").await
                })
            })
            .await
            .unwrap()
            .expect("builder terminal contract survives fanout");
        assert_eq!(info.artifact_name.as_deref(), Some("create-pr"));
        assert_eq!(info.confirm_policy, ConfirmPolicy::Auto);
    }

    #[tokio::test]
    async fn ctx_self_target_is_not_the_terminal_contract() {
        let snap = planbuild_snapshot(true);
        // ctx-self enumeration finds `notes`...
        let selfs = resolve_ctx_self_schemas_with_snapshot(&snap, "planner");
        assert_eq!(selfs.len(), 1);
        assert_eq!(selfs[0].artifact_name.as_deref(), Some("notes"));

        // ...but the terminal contract is still `plan`, never `notes`.
        let db = test_db().await;
        let info = db
            .read(move |conn| {
                let snap = snap.clone();
                Box::pin(async move {
                    find_downstream_artifact_schema_with_snapshot_conn(conn, &snap, "planner").await
                })
            })
            .await
            .unwrap()
            .expect("terminal contract");
        assert_eq!(info.artifact_name.as_deref(), Some("plan"));
    }

    #[tokio::test]
    async fn legacy_inline_output_schema_still_resolves() {
        // Old snapshot: planner carries an inline outputSchema and a direct
        // context-out edge to the builder (no ArtifactNode).
        let mut planner = agent("planner");
        planner.agent_config = Some(AgentNodeConfig {
            agent_config_id: Some("planner".to_string()),
            output_schema: Some(SchemaConfig {
                name: "plan".to_string(),
                schema: Some(serde_json::json!({"type": "object"})),
                confirm_policy: ConfirmPolicy::User,
                tool_name: None,
                description: None,
            }),
            git_config: None,
        });
        let snap = snapshot(
            vec![planner, agent("builder")],
            vec![ctx_edge(
                "e1",
                "planner",
                "context-out",
                "builder",
                "context-in",
            )],
        );
        let db = test_db().await;
        let info = db
            .read(move |conn| {
                let snap = snap.clone();
                Box::pin(async move {
                    find_downstream_artifact_schema_with_snapshot_conn(conn, &snap, "planner").await
                })
            })
            .await
            .unwrap()
            .expect("legacy inline schema resolves");
        assert_eq!(info.artifact_name.as_deref(), Some("plan"));
        assert_eq!(info.confirm_policy, ConfirmPolicy::User);
    }

    #[tokio::test]
    async fn consumer_reads_producer_artifact_through_artifact_node() {
        let db = test_db().await;
        let snap = planbuild_snapshot(false);
        let snapshot_json = snap.to_json().unwrap();
        db.write(move |conn| {
            let snapshot_json = snapshot_json.clone();
            Box::pin(async move {
                conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-1','W',1,1)", ()).await?;
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-1','w-1','P','P','/tmp/p',1,1)", ()).await?;
                conn.execute("INSERT INTO issues (id, project_id, number, title, status, attention, created_at, updated_at) VALUES ('i-1','p-1',1,'T','active','none',1,1)", ()).await?;
                conn.execute("INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot) VALUES ('e-1','recipe-1','i-1','p-1','running',1,1,?1)", (snapshot_json.as_str(),)).await?;
                conn.execute("INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at) VALUES ('j-planner','e-1','planner','i-1','p-1','complete','planner','planner',1,1)", ()).await?;
                conn.execute("INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at) VALUES ('j-builder','e-1','builder','i-1','p-1','running','builder','builder',1,1)", ()).await?;
                conn.execute("INSERT INTO artifacts (id, job_id, artifact_type, confirmed, data, version, output_name, created_at, updated_at) VALUES ('a-plan','j-planner','plan',1,'{\"title\":\"Plan A\",\"content\":\"do the thing\"}',1,'plan',1,1)", ()).await?;
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();

        let inputs = db
            .read(|conn| {
                Box::pin(async move {
                    let builder = crate::db_records::load_live_job_by_execution_node_conn(
                        conn, "e-1", "builder",
                    )
                    .await?
                    .expect("builder job");
                    resolve_job_inputs_conn(conn, &builder).await
                })
            })
            .await
            .unwrap();

        let plan_input = inputs
            .iter()
            .find(|i| i.artifact_type == "plan")
            .expect("builder receives the planner's plan artifact");
        assert_eq!(
            plan_input.data.get("content").and_then(|v| v.as_str()),
            Some("do the thing")
        );
    }

    /// The latest-across-job fallback must skip a higher-versioned context-self
    /// living doc and return the real terminal artifact (CAIRN-1953).
    #[tokio::test]
    async fn load_artifact_for_edge_excludes_ctx_self_from_fallback() {
        let db = test_db().await;
        db.write(|conn| {
            Box::pin(async move {
                conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-1','W',1,1)", ()).await?;
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-1','w-1','P','P','/tmp/p',1,1)", ()).await?;
                conn.execute("INSERT INTO issues (id, project_id, number, title, status, attention, created_at, updated_at) VALUES ('i-1','p-1',1,'T','active','none',1,1)", ()).await?;
                conn.execute("INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot) VALUES ('e-1','recipe-1','i-1','p-1','running',1,1,'{}')", ()).await?;
                conn.execute("INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at) VALUES ('j-coord','e-1','coord','i-1','p-1','complete','coord','coord',1,1)", ()).await?;
                conn.execute("INSERT INTO artifacts (id, job_id, artifact_type, confirmed, data, version, output_name, created_at, updated_at) VALUES ('a-pr','j-coord','create-pr',1,'{\"title\":\"Real PR\"}',1,'create-pr',1,1)", ()).await?;
                conn.execute("INSERT INTO artifacts (id, job_id, artifact_type, confirmed, data, version, output_name, created_at, updated_at) VALUES ('a-plan','j-coord','plan',1,'{\"title\":\"Plan doc\"}',2,'plan',1,1)", ()).await?;
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();

        let (by_name, by_handle_excluded, by_handle_unfiltered) = db
            .read(|conn| {
                Box::pin(async move {
                    let exclude = vec!["plan".to_string()];
                    let by_name =
                        load_artifact_for_edge_conn(conn, "j-coord", "create-pr", &exclude).await?;
                    let by_handle_excluded =
                        load_artifact_for_edge_conn(conn, "j-coord", "context-out", &exclude)
                            .await?;
                    let by_handle_unfiltered =
                        load_artifact_for_edge_conn(conn, "j-coord", "context-out", &[]).await?;
                    Ok::<_, DbError>((by_name, by_handle_excluded, by_handle_unfiltered))
                })
            })
            .await
            .unwrap();

        // Exact match by contract name returns create-pr.
        assert_eq!(by_name.unwrap().0, "create-pr");
        // Handle keying misses; the fallback skips the higher-versioned ctx-self
        // `plan` and returns create-pr.
        assert_eq!(by_handle_excluded.unwrap().0, "create-pr");
        // Without the exclusion, the latest-across-job fallback returns `plan`
        // — the exact pre-fix behavior that opened the PR with the wrong content.
        assert_eq!(by_handle_unfiltered.unwrap().0, "plan");
    }

    /// A consumer on a direct context edge must not receive the producer's
    /// context-self living doc as an input artifact (CAIRN-1953).
    #[tokio::test]
    async fn direct_edge_consumer_skips_producer_ctx_self_doc() {
        let db = test_db().await;
        let snap = snapshot(
            vec![
                agent("planner"),
                agent("builder"),
                artifact_node("notes-node", "notes", ConfirmPolicy::Auto),
            ],
            vec![
                ctx_edge("e1", "planner", "context-out", "builder", "context-in"),
                ctx_edge("e2", "planner", "context-self", "notes-node", "context-in"),
            ],
        );
        let snapshot_json = snap.to_json().unwrap();
        db.write(move |conn| {
            let snapshot_json = snapshot_json.clone();
            Box::pin(async move {
                conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-1','W',1,1)", ()).await?;
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-1','w-1','P','P','/tmp/p',1,1)", ()).await?;
                conn.execute("INSERT INTO issues (id, project_id, number, title, status, attention, created_at, updated_at) VALUES ('i-1','p-1',1,'T','active','none',1,1)", ()).await?;
                conn.execute("INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot) VALUES ('e-1','recipe-1','i-1','p-1','running',1,1,?1)", (snapshot_json.as_str(),)).await?;
                conn.execute("INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at) VALUES ('j-planner','e-1','planner','i-1','p-1','running','planner','planner',1,1)", ()).await?;
                conn.execute("INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at) VALUES ('j-builder','e-1','builder','i-1','p-1','running','builder','builder',1,1)", ()).await?;
                conn.execute("INSERT INTO artifacts (id, job_id, artifact_type, confirmed, data, version, output_name, created_at, updated_at) VALUES ('a-notes','j-planner','notes',1,'{\"title\":\"Notes\",\"content\":\"living doc\"}',1,'notes',1,1)", ()).await?;
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();

        let inputs = db
            .read(|conn| {
                Box::pin(async move {
                    let builder = crate::db_records::load_live_job_by_execution_node_conn(
                        conn, "e-1", "builder",
                    )
                    .await?
                    .expect("builder job");
                    resolve_job_inputs_conn(conn, &builder).await
                })
            })
            .await
            .unwrap();

        assert!(
            inputs.iter().all(|i| i.artifact_type != "notes"),
            "builder must not receive the planner's context-self `notes` doc"
        );
    }
}
