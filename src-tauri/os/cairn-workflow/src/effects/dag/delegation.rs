use super::*;

/// Put the delegated agent's definition in the snapshot, once. A packet's own
/// allotment rides on the node it materializes into, so the shared agent
/// snapshot is never rewritten for one task.
fn ensure_agent_snapshot(
    snapshot: &mut ExecutionSnapshot,
    agent_id: &str,
    config_dir: &Path,
    project_path: Option<&Path>,
) -> Result<(), String> {
    if let std::collections::hash_map::Entry::Vacant(entry) =
        snapshot.agents.entry(agent_id.to_string())
    {
        // Loud: a missing agent fails materialization visibly rather than
        // degrading to a placeholder.
        let file_agent = config_agents::get_agent(config_dir, entry.key(), project_path)
            .map_err(|e| format!("Failed to load agent '{agent_id}': {e}"))?
            .ok_or_else(|| {
                format!("Agent config not found during delegation expansion: {agent_id}")
            })?;
        entry.insert(file_agent.snapshot());
    }
    Ok(())
}

fn schema_config_from_output_contract(contract: &DelegatedOutputContract) -> SchemaConfig {
    // The contract's schema doubles as the artifact name/URI segment. Bake the
    // resolved JSON Schema inline so the node is self-contained: a preset resolves
    // to its embedded schema, a custom inline schema is used verbatim.
    let name = contract.artifact_name();
    let schema = match &contract.schema_type {
        crate::models::OutputSchema::Custom(value) => Some(value.clone()),
        crate::models::OutputSchema::Preset(_) => crate::output_schemas::resolve_output_schema(
            None,
            &crate::models::OutputSchema::Preset(name.clone()),
        )
        .ok(),
    };
    SchemaConfig {
        name,
        schema,
        confirm_policy: crate::models::ConfirmPolicy::default(),
        tool_name: contract.tool_name.clone(),
        description: contract.description.clone(),
    }
}

/// The agent node a delegated packet materializes into.
///
/// Its branch mode is [`BranchMode::Inherit`]: a delegated task continues the
/// coordinate of the job that spawned it. For a branch-bearing parent that is
/// the live logical head; for a deliberately branchless parent such as a thread,
/// it is the live base ref without a branch invented for the child.
///
/// `reparent_delegated_jobs` records that lineage before activation, then
/// `prepare_job` resolves the coordinate and persists the exact commit the child
/// starts from. This preserves the CAIRN-3278 invariant — what you read is what
/// you build — for builders with in-flight work while keeping read-only thread
/// delegation on the project's base context.
///
/// This emitted [`BranchMode::None`] until CAIRN-3309. That discarded lineage
/// entirely, so every delegated task ran against `main` even when its parent held
/// unpushed commits. Inheritance is what distinguishes that bug from intentional
/// branchless lineage.
fn delegated_agent_node(agent_id: String, packet: &DelegatedWorkPacket) -> RecipeNode {
    RecipeNode {
        id: agent_id,
        node_type: RecipeNodeType::Agent,
        name: packet.title.clone(),
        position: NodePosition { x: 400.0, y: 0.0 },
        parent_id: None,
        trigger_config: None,
        agent_config: Some(AgentNodeConfig {
            agent_config_id: Some(packet.agent_config_id.clone()),
            models: packet.models.clone(),
            constraints: Default::default(),
            output_schema: Some(schema_config_from_output_contract(&packet.output_contract)),
            git_config: Some(AgentGitConfig {
                branch_mode: BranchMode::Inherit,
                require_parent_head: true,
            }),
        }),
        action_config: None,
        checkpoint_config: None,
        artifact_config: None,
        condition_config: None,
        decision_config: None,
        context_config: None,
    }
}

pub fn expand_delegated_packets(
    runtime: &WorkflowRuntime,
    db: &Arc<LocalDb>,
    execution_id: &str,
) -> Result<HashSet<String>, String> {
    let mut snapshot = load_execution_snapshot(db.clone(), execution_id)?;
    let mut packets = load_packets(db, execution_id)?;
    let project_path = load_project_repo_path(db.clone(), &snapshot.trigger_context.project_id)?
        .map(PathBuf::from);
    let pending_packet_ids: Vec<String> = packets
        .iter()
        .filter(|packet| packet.status == DelegatedStatus::Pending)
        .map(|packet| packet.id.clone())
        .collect();

    if pending_packet_ids.is_empty() {
        return Ok(HashSet::new());
    }

    let mut new_agent_node_ids = HashSet::new();
    let mut materialized = Vec::new();

    for packet_id in pending_packet_ids {
        let packet_index = packets
            .iter()
            .position(|packet| packet.id == packet_id)
            .ok_or_else(|| format!("Delegated packet missing: {packet_id}"))?;
        let packet_view = packets[packet_index].clone();

        ensure_agent_snapshot(
            &mut snapshot,
            &packet_view.agent_config_id,
            &runtime.config_dir,
            project_path.as_deref(),
        )?;

        let trigger_id = format!("delegated-{}-trigger", packet_view.id);
        let context_id = format!("delegated-{}-context", packet_view.id);
        let agent_id = format!("delegated-{}-agent", packet_view.id);

        if !snapshot
            .recipe
            .nodes
            .iter()
            .any(|node| node.id == trigger_id)
        {
            snapshot.recipe.nodes.push(RecipeNode {
                id: trigger_id.clone(),
                node_type: RecipeNodeType::Trigger,
                name: format!("{} trigger", packet_view.title),
                position: NodePosition { x: 0.0, y: 0.0 },
                parent_id: None,
                trigger_config: None,
                agent_config: None,
                action_config: None,
                checkpoint_config: None,
                artifact_config: None,
                condition_config: None,
                decision_config: None,
                context_config: None,
            });
        }

        if !snapshot
            .recipe
            .nodes
            .iter()
            .any(|node| node.id == context_id)
        {
            let acceptance = if packet_view.acceptance.is_empty() {
                String::new()
            } else {
                format!(
                    "\n\nAcceptance criteria:\n{}",
                    packet_view
                        .acceptance
                        .iter()
                        .map(|item| format!("- {}", item))
                        .collect::<Vec<_>>()
                        .join("\n")
                )
            };
            let policy = format!("\n\nWorking directory: {}", packet_view.ownership.cwd);
            snapshot.recipe.nodes.push(RecipeNode {
                id: context_id.clone(),
                node_type: RecipeNodeType::Context,
                name: format!("{} context", packet_view.title),
                position: NodePosition { x: 200.0, y: 0.0 },
                parent_id: None,
                trigger_config: None,
                agent_config: None,
                action_config: None,
                checkpoint_config: None,
                artifact_config: None,
                condition_config: None,
                decision_config: None,
                context_config: Some(ContextNodeConfig {
                    content: format!(
                        "{}{}{}",
                        packet_view.problem_statement.as_deref().unwrap_or_default(),
                        acceptance,
                        policy
                    ),
                }),
            });
        }

        if !snapshot.recipe.nodes.iter().any(|node| node.id == agent_id) {
            snapshot
                .recipe
                .nodes
                .push(delegated_agent_node(agent_id.clone(), &packet_view));
            new_agent_node_ids.insert(agent_id.clone());
        }

        push_edge_if_missing(
            &mut snapshot.recipe.edges,
            &trigger_id,
            "control-out",
            &agent_id,
            "control-in",
            RecipeEdgeType::Control,
        );
        push_edge_if_missing(
            &mut snapshot.recipe.edges,
            &context_id,
            "context-out",
            &agent_id,
            "context-in",
            RecipeEdgeType::Context,
        );

        let packet = &mut packets[packet_index];
        packet.status = DelegatedStatus::Materialized;
        packet.materialized_node_ids = vec![trigger_id, context_id, agent_id];
        materialized.push((packet.id.clone(), packet.materialized_node_ids.clone()));
    }

    persist_expansion(db, execution_id, &snapshot, materialized)?;

    if new_agent_node_ids.is_empty() {
        return Ok(HashSet::new());
    }

    let created_jobs =
        create_jobs_for_new_nodes(db.clone(), execution_id, &new_agent_node_ids, &snapshot)?;
    assign_delegated_job_metadata(db, &created_jobs, &packets)?;

    let job_by_node: HashMap<String, String> = created_jobs
        .iter()
        .filter_map(|job| {
            job.recipe_node_id
                .as_ref()
                .map(|node_id| (node_id.clone(), job.id.clone()))
        })
        .collect();
    let mut links = Vec::new();
    for packet in &packets {
        if packet.status != DelegatedStatus::Materialized || packet.result_artifact_job_id.is_some()
        {
            continue;
        }
        let Some(agent_node_id) = packet
            .materialized_node_ids
            .iter()
            .find(|node_id| node_id.ends_with("-agent"))
            .cloned()
        else {
            continue;
        };
        let job_id = job_by_node.get(&agent_node_id).cloned().or_else(|| {
            find_job_id_for_node(db, execution_id, &agent_node_id)
                .ok()
                .flatten()
        });
        if let Some(job_id) = job_id {
            links.push((packet.id.clone(), job_id));
        }
    }
    link_packets(db, execution_id, links)?;

    // One scoped jobs event per created delegated job (frontend dedupes).
    for job in &created_jobs {
        let _ = runtime
            .emitter
            .emit("db-change", cairn_db::changes::job_db_change(job, "insert"));
    }

    Ok(new_agent_node_ids)
}

fn load_packets(db: &Arc<LocalDb>, execution_id: &str) -> Result<Vec<DelegatedWorkPacket>, String> {
    let (db, execution_id) = (db.clone(), execution_id.to_string());
    run_advancement_db(async move {
        crate::delegated_packets::list_db(&db, &execution_id)
            .await
            .map_err(|e| format!("Failed to load delegated packets: {e}"))
    })
}

/// Write the expanded recipe and mark its packets materialized in one
/// transaction, so no crash leaves nodes whose packet is still pending (a
/// later expansion would skip their existing agent node and never create its
/// job) or a materialized packet without its nodes.
fn persist_expansion(
    db: &Arc<LocalDb>,
    execution_id: &str,
    snapshot: &ExecutionSnapshot,
    materialized: Vec<(String, Vec<String>)>,
) -> Result<(), String> {
    let snapshot_json = serde_json::to_string(snapshot)
        .map_err(|e| format!("Failed to serialize snapshot: {e}"))?;
    let (db, execution_id) = (db.clone(), execution_id.to_string());
    run_advancement_db(async move {
        db.write(|conn| {
            let (execution_id, snapshot_json) = (execution_id.clone(), snapshot_json.clone());
            let materialized = materialized.clone();
            Box::pin(async move {
                conn.execute(
                    "UPDATE executions SET snapshot = ?1 WHERE id = ?2",
                    params![snapshot_json.as_str(), execution_id.as_str()],
                )
                .await?;
                for (packet_id, node_ids) in materialized {
                    let change = crate::delegated_packets::PacketChange {
                        status: Some(DelegatedStatus::Materialized),
                        materialized_node_ids: Some(node_ids),
                        result_artifact_job_id: None,
                    };
                    crate::delegated_packets::update(conn, &execution_id, &packet_id, &change)
                        .await?;
                }
                Ok(())
            })
        })
        .await
        .map_err(|e| format!("Failed to persist delegated expansion: {e}"))
    })
}

/// Name each materialized packet's child job.
fn link_packets(
    db: &Arc<LocalDb>,
    execution_id: &str,
    links: Vec<(String, String)>,
) -> Result<(), String> {
    if links.is_empty() {
        return Ok(());
    }
    let (db, execution_id) = (db.clone(), execution_id.to_string());
    run_advancement_db(async move {
        db.write(|conn| {
            let (execution_id, links) = (execution_id.clone(), links.clone());
            Box::pin(async move {
                for (packet_id, job_id) in links {
                    let change = crate::delegated_packets::PacketChange {
                        result_artifact_job_id: Some(job_id),
                        ..Default::default()
                    };
                    crate::delegated_packets::update(conn, &execution_id, &packet_id, &change)
                        .await?;
                }
                Ok(())
            })
        })
        .await
        .map_err(|e| format!("Failed to link delegated packets: {e}"))
    })
}

fn find_job_id_for_node(
    db: &Arc<LocalDb>,
    execution_id: &str,
    node_id: &str,
) -> Result<Option<String>, String> {
    let db = db.clone();
    let execution_id = execution_id.to_string();
    let node_id = node_id.to_string();
    run_advancement_db(async move {
        db.read(|conn| {
            let execution_id = execution_id.clone();
            let node_id = node_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT id
                         FROM jobs
                         WHERE execution_id = ?1 AND recipe_node_id = ?2
                           AND status <> 'cancelled'
                         ORDER BY created_at DESC
                         LIMIT 1",
                        params![execution_id.as_str(), node_id.as_str()],
                    )
                    .await?;
                crate::storage::next_text(&mut rows, 0).await
            })
        })
        .await
        .map_err(|e| format!("Failed to find delegated job: {e}"))
    })
}

fn assign_delegated_job_metadata(
    db: &Arc<LocalDb>,
    created_jobs: &[Job],
    packets: &[DelegatedWorkPacket],
) -> Result<(), String> {
    let db = db.clone();
    let mut ordered_jobs: Vec<Job> = created_jobs.to_vec();
    let packets = packets.to_vec();
    ordered_jobs.sort_by_key(|job| {
        job.recipe_node_id
            .as_deref()
            .and_then(|node_id| {
                packets
                    .iter()
                    .find(|packet| packet.materialized_node_ids.iter().any(|id| id == node_id))
            })
            .map(|packet| {
                (
                    packet.task_index.unwrap_or(i32::MAX),
                    packet.created_at,
                    packet.id.clone(),
                )
            })
            .unwrap_or((i32::MAX, i64::MAX, String::new()))
    });

    let ordered: Vec<(String, Option<String>)> = ordered_jobs
        .into_iter()
        .map(|job| (job.id, job.recipe_node_id))
        .collect();
    run_advancement_db(async move { reparent_delegated_jobs(&db, ordered, packets).await })
}

/// Re-parent delegated jobs under their delegating node and assign each a
/// parent-unique `uri_segment` (kept in lockstep with `node_name`). Extracted
/// from `assign_delegated_job_metadata` so the disambiguation is unit-testable.
///
/// This establishes lineage before activation: `parent_job_id`, the parent's
/// branch, and the addressing a child is reachable by. It is not where the
/// child's start commit is decided. The `base_commit` copied here is the
/// parent's archival row, carried forward for `pack_anchor` bookkeeping;
/// `prepare_job` resolves the parent branch's live bookmark at activation and
/// overwrites it with the commit the child actually starts from.
///
/// A branch-bearing parent contributes its logical branch. A deliberately
/// branchless parent contributes its base ref instead, so read-only nodes such
/// as threads can delegate without inventing a writable branch. Activation
/// distinguishes that valid branchless lineage from a vanished parent branch.
///
/// `ordered_jobs` is `(job_id, recipe_node_id)` pre-sorted by packet order.
pub async fn reparent_delegated_jobs(
    db: &LocalDb,
    ordered_jobs: Vec<(String, Option<String>)>,
    packets: Vec<DelegatedWorkPacket>,
) -> Result<(), String> {
    db.write(|conn| {
        let ordered_jobs = ordered_jobs.clone();
        let packets = packets.clone();
        Box::pin(async move {
            let mut assigned_slugs_by_parent: HashMap<String, HashSet<String>> = HashMap::new();

            for (job_id, recipe_node_id) in ordered_jobs {
                let Some(recipe_node_id) = recipe_node_id.as_deref() else {
                    continue;
                };
                let Some(packet) = packets.iter().find(|packet| {
                    packet
                        .materialized_node_ids
                        .iter()
                        .any(|node_id| node_id == recipe_node_id)
                }) else {
                    continue;
                };

                let mut parent_rows = conn
                    .query(
                        "SELECT branch, base_commit, base_branch FROM jobs WHERE id = ?1",
                        (packet.parent_job_id.as_str(),),
                    )
                    .await?;
                let parent_row = parent_rows.next().await?.ok_or_else(|| {
                    DbError::Row(format!(
                        "delegating parent job {} was not found",
                        packet.parent_job_id
                    ))
                })?;
                let parent_branch = parent_row.opt_text(0)?;
                let parent_base_commit = parent_row.opt_text(1)?;
                let parent_base_branch = parent_row.opt_text(2)?;

                // Reserve against existing siblings' uri_segment — the column the
                // (parent_job_id, uri_segment) unique index actually guards — so a
                // new batch whose titles collide with prior children disambiguates
                // with a -N suffix instead of failing the constraint.
                let mut sibling_rows = conn
                    .query(
                        "SELECT uri_segment
                             FROM jobs
                             WHERE parent_job_id = ?1 AND id != ?2",
                        params![packet.parent_job_id.as_str(), job_id.as_str()],
                    )
                    .await?;
                let mut reserved = HashSet::new();
                while let Some(row) = sibling_rows.next().await? {
                    if let Some(segment) = row.opt_text(0)? {
                        if !segment.is_empty() {
                            reserved.insert(segment);
                        }
                    }
                }
                if let Some(assigned) = assigned_slugs_by_parent.get(&packet.parent_job_id) {
                    reserved.extend(assigned.iter().cloned());
                }

                let slug = derive_unique_task_slug(&packet.title, &reserved);
                assigned_slugs_by_parent
                    .entry(packet.parent_job_id.clone())
                    .or_default()
                    .insert(slug.clone());

                // Keep node_name and uri_segment in lockstep so the addressable
                // segment is the disambiguated, parent-unique slug. Carry the
                // packet's parent_tool_use_id onto the job so the transcript can
                // locate spawned children by the originating tool-use id
                // (`list_child_jobs`). Without this the column stays NULL and the
                // live task windows can never resolve.
                conn.execute(
                    "UPDATE jobs
                         SET parent_job_id = ?1,
                             branch = ?2,
                             base_commit = ?3,
                             base_branch = ?4,
                             task_index = ?5,
                             node_name = ?6,
                             uri_segment = ?6,
                             parent_tool_use_id = ?7
                         WHERE id = ?8",
                    params![
                        packet.parent_job_id.as_str(),
                        parent_branch.as_deref(),
                        parent_base_commit.as_deref(),
                        parent_base_branch.as_deref(),
                        packet.task_index,
                        slug.as_str(),
                        packet.parent_tool_use_id.as_deref(),
                        job_id.as_str(),
                    ],
                )
                .await?;
            }

            Ok(())
        })
    })
    .await
    .map_err(|e| format!("Failed to update delegated jobs: {e}"))
}

#[cfg(any(test, feature = "test-utils"))]
pub fn delegated_agent_node_for_test(agent_id: String, packet: &DelegatedWorkPacket) -> RecipeNode {
    delegated_agent_node(agent_id, packet)
}
#[cfg(any(test, feature = "test-utils"))]
pub fn schema_config_from_output_contract_for_test(
    contract: &DelegatedOutputContract,
) -> SchemaConfig {
    schema_config_from_output_contract(contract)
}
