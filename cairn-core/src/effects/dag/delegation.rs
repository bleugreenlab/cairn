use super::*;

fn ensure_agent_snapshot(
    snapshot: &mut ExecutionSnapshot,
    agent_id: &str,
    tier_override: Option<&str>,
    backend_preference: Option<&str>,
    config_dir: &Path,
    project_path: Option<&Path>,
    presets: &PresetsConfig,
) -> Result<(), String> {
    match snapshot.agents.entry(agent_id.to_string()) {
        std::collections::hash_map::Entry::Vacant(entry) => {
            // Resolve-early + loud: a missing or unresolvable agent fails
            // materialization visibly rather than degrading to a placeholder.
            let mut file_agent = config_agents::get_agent(config_dir, entry.key(), project_path)
                .map_err(|e| format!("Failed to load agent '{agent_id}': {e}"))?
                .ok_or_else(|| {
                    format!("Agent config not found during delegation expansion: {agent_id}")
                })?;
            if backend_preference.is_some() {
                file_agent.backend_preference = backend_preference.map(str::to_string);
            }
            let override_sel =
                tier_override.map(|tier| LaunchSelectionOverride::Tier(tier.to_string()));
            let agent_snapshot =
                resolve_agent_snapshot(&file_agent, override_sel.as_ref(), presets)?;
            entry.insert(agent_snapshot);
        }
        std::collections::hash_map::Entry::Occupied(mut entry) => {
            if tier_override.is_some() || backend_preference.is_some() {
                let agent = entry.get_mut();
                let authored_tier =
                    tier_override.or_else(|| agent.tier.as_ref().map(Model::as_str));
                let authored_backend = backend_preference.or(agent.backend_preference.as_deref());
                let (selection, extras) =
                    resolve_runtime_selection(authored_tier, authored_backend, presets)?;
                if let Some(tier) = tier_override {
                    agent.tier = Some(Model::new(
                        crate::config::presets::normalize_tier_selection(tier, presets),
                    ));
                }
                if let Some(backend_preference) = backend_preference {
                    agent.backend_preference = Some(backend_preference.to_string());
                }
                agent.selection = Some(selection);
                agent.extras = Some(extras);
            }
        }
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
/// branch of the job that spawned it, so what it reads, builds, and tests is the
/// parent's logical head at spawn time rather than the base branch the parent
/// was originally cut from. Seeding is the delegation-edge half of the CAIRN-3278
/// invariant — what you read is what you build — and it is what makes a task's
/// tests exercise the parent's in-flight work instead of pre-change code.
///
/// The mode is the whole coordinate decision. `reparent_delegated_jobs` records
/// the lineage inheritance depends on before the node can activate, and
/// `prepare_job` re-resolves the parent's live bookmark at activation and
/// persists the exact commit the child starts from.
///
/// This emitted [`BranchMode::None`] until CAIRN-3309, contradicting every
/// comment around it. That mode leaves `jobs.branch` NULL, and a job without a
/// branch of its own reads and writes its `base_branch` — so every delegated
/// task ran against `main` while its parent held unpushed commits.
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
        context_config: None,
    }
}

pub(super) fn expand_delegated_packets(
    orch: &Orchestrator,
    db: &Arc<LocalDb>,
    execution_id: &str,
) -> Result<HashSet<String>, String> {
    let mut snapshot = load_execution_snapshot(db.clone(), execution_id)?;
    let project_path = load_project_repo_path(db.clone(), &snapshot.trigger_context.project_id)?
        .map(PathBuf::from);
    let pending_packet_ids: Vec<String> = snapshot
        .delegated_packets
        .iter()
        .filter(|packet| packet.status == DelegatedStatus::Pending)
        .map(|packet| packet.id.clone())
        .collect();

    if pending_packet_ids.is_empty() {
        return Ok(HashSet::new());
    }

    // Resolve-early: materialize against current effective presets (loud).
    let presets = load_effective_presets(&orch.config_dir, project_path.as_deref());
    let mut new_agent_node_ids = HashSet::new();

    for packet_id in pending_packet_ids {
        let packet_index = snapshot
            .delegated_packets
            .iter()
            .position(|packet| packet.id == packet_id)
            .ok_or_else(|| format!("Delegated packet missing from snapshot: {packet_id}"))?;
        let packet_view = snapshot.delegated_packets[packet_index].clone();

        ensure_agent_snapshot(
            &mut snapshot,
            &packet_view.agent_config_id,
            packet_view.tier_override.as_deref(),
            packet_view.backend_preference.as_deref(),
            &orch.config_dir,
            project_path.as_deref(),
            &presets,
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
                context_config: Some(ContextNodeConfig {
                    content: format!("{}{}{}", packet_view.problem_statement, acceptance, policy),
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

        let packet = &mut snapshot.delegated_packets[packet_index];
        packet.status = DelegatedStatus::Materialized;
        packet.materialized_node_ids = vec![trigger_id, context_id, agent_id];
    }

    update_execution_snapshot(db.clone(), execution_id, &snapshot)?;

    if new_agent_node_ids.is_empty() {
        return Ok(HashSet::new());
    }

    let created_jobs =
        create_jobs_for_new_nodes(db.clone(), execution_id, &new_agent_node_ids, &snapshot)?;
    assign_delegated_job_metadata(db, &created_jobs, &snapshot)?;

    let job_by_node: HashMap<String, String> = created_jobs
        .iter()
        .filter_map(|job| {
            job.recipe_node_id
                .as_ref()
                .map(|node_id| (node_id.clone(), job.id.clone()))
        })
        .collect();
    for packet in &mut snapshot.delegated_packets {
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
        packet.result_artifact_job_id = job_by_node.get(&agent_node_id).cloned().or_else(|| {
            find_job_id_for_node(db, execution_id, &agent_node_id)
                .ok()
                .flatten()
        });
    }
    update_execution_snapshot(db.clone(), execution_id, &snapshot)?;

    // One scoped jobs event per created delegated job (frontend dedupes).
    for job in &created_jobs {
        let _ = orch
            .services
            .emitter
            .emit("db-change", crate::notify::job_db_change(job, "insert"));
    }

    Ok(new_agent_node_ids)
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
    snapshot: &ExecutionSnapshot,
) -> Result<(), String> {
    let db = db.clone();
    let mut ordered_jobs: Vec<Job> = created_jobs.to_vec();
    let packets = snapshot.delegated_packets.clone();
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
/// A parent with no branch is refused rather than silently re-parented onto the
/// base branch: without a parent coordinate there is nothing for the child to
/// inherit, and starting from base is precisely the failure this edge exists to
/// prevent.
///
/// `ordered_jobs` is `(job_id, recipe_node_id)` pre-sorted by packet order.
pub(super) async fn reparent_delegated_jobs(
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
                let parent_branch = parent_row.opt_text(0)?.ok_or_else(|| {
                    DbError::Row(format!(
                        "delegating parent job {} has no logical branch",
                        packet.parent_job_id
                    ))
                })?;
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
                        parent_branch.as_str(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{DelegatedOwnershipScope, DelegatedSessionStrategy, DelegationOrigin};
    use crate::storage::{DbError, LocalDb, MigrationRunner, TURSO_MIGRATIONS};

    async fn test_db() -> LocalDb {
        let temp = tempfile::tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("delegation.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db
    }

    /// A packet delegating `title` from `parent_job_id`, materialized onto
    /// `node_id`.
    fn packet(id: &str, parent_job_id: &str, title: &str, node_id: &str) -> DelegatedWorkPacket {
        DelegatedWorkPacket {
            id: id.into(),
            parent_job_id: parent_job_id.into(),
            parent_turn_id: Some("turn-1".into()),
            parent_tool_use_id: Some("tool-1".into()),
            origin: DelegationOrigin::TaskTool,
            title: title.into(),
            problem_statement: "Do the child task".into(),
            agent_config_id: "build".into(),
            ownership: DelegatedOwnershipScope {
                cwd: "/scratch".into(),
                fence: None,
                sandbox: None,
                on_escape: None,
            },
            session: DelegatedSessionStrategy::default(),
            acceptance: vec![],
            output_contract: DelegatedOutputContract {
                schema_type: crate::models::OutputSchema::Preset("return".into()),
                tool_name: None,
                description: None,
            },
            status: DelegatedStatus::Materialized,
            materialized_node_ids: vec![node_id.into()],
            result_artifact_job_id: None,
            task_index: Some(7),
            tier_override: None,
            backend_preference: None,
            background: false,
            created_at: 1,
        }
    }

    async fn child_coordinate(
        db: &LocalDb,
        job_id: &'static str,
    ) -> (
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<i64>,
        Option<String>,
        Option<String>,
    ) {
        db.read(|conn| Box::pin(async move {
            let mut rows = conn.query("SELECT parent_job_id, branch, base_commit, base_branch, task_index, uri_segment, parent_tool_use_id FROM jobs WHERE id = ?1", (job_id,)).await?;
            let row = rows.next().await?.ok_or_else(|| DbError::Row("child job missing".into()))?;
            Ok::<_, DbError>((row.opt_text(0)?, row.opt_text(1)?, row.opt_text(2)?, row.opt_text(3)?, row.opt_i64(4)?, row.opt_text(5)?, row.opt_text(6)?))
        })).await.unwrap()
    }

    #[tokio::test]
    async fn reparented_delegated_job_inherits_parent_coordinate() {
        let db = test_db().await;
        db.write(|conn| Box::pin(async move {
            conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-1','W',1,1)", ()).await?;
            conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-1','w-1','P','P','/tmp/p',1,1)", ()).await?;
            conn.execute("INSERT INTO issues (id, project_id, number, title, status, attention, created_at, updated_at) VALUES ('i-1','p-1',1,'T','active','none',1,1)", ()).await?;
            conn.execute("INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq) VALUES ('e-1','default','i-1','p-1','running',1,1)", ()).await?;
            conn.execute("INSERT INTO jobs (id, execution_id, issue_id, project_id, status, branch, base_commit, base_branch, uri_segment, node_name, created_at, updated_at) VALUES ('parent-job','e-1','i-1','p-1','running','agent/parent','head-1','main','executor','Executor',1,1)", ()).await?;
            conn.execute("INSERT INTO jobs (id, execution_id, issue_id, project_id, recipe_node_id, status, uri_segment, node_name, created_at, updated_at) VALUES ('child-job','e-1','i-1','p-1','delegated-agent','pending','child','Child',1,1)", ()).await?;
            Ok::<_, DbError>(())
        })).await.unwrap();
        reparent_delegated_jobs(
            &db,
            vec![("child-job".into(), Some("delegated-agent".into()))],
            vec![packet(
                "packet-1",
                "parent-job",
                "Implement child",
                "delegated-agent",
            )],
        )
        .await
        .unwrap();
        let coordinate = child_coordinate(&db, "child-job").await;
        assert_eq!(coordinate.0.as_deref(), Some("parent-job"));
        assert_eq!(coordinate.1.as_deref(), Some("agent/parent"));
        assert_eq!(coordinate.2.as_deref(), Some("head-1"));
        assert_eq!(coordinate.3.as_deref(), Some("main"));
        assert_eq!(coordinate.4, Some(7));
        assert_eq!(coordinate.5.as_deref(), Some("implement-child"));
        assert_eq!(coordinate.6.as_deref(), Some("tool-1"));
    }

    /// A task that delegates its own task. Nesting needs no special case, but it
    /// only works because a task now carries a branch of its own: while
    /// delegated jobs were left branchless, every nested delegation was refused
    /// or started from base.
    #[tokio::test]
    async fn a_nested_delegation_carries_the_same_branch_one_level_down() {
        let db = test_db().await;
        db.write(|conn| Box::pin(async move {
            conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-1','W',1,1)", ()).await?;
            conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-1','w-1','P','P','/tmp/p',1,1)", ()).await?;
            conn.execute("INSERT INTO issues (id, project_id, number, title, status, attention, created_at, updated_at) VALUES ('i-1','p-1',1,'T','active','none',1,1)", ()).await?;
            conn.execute("INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq) VALUES ('e-1','default','i-1','p-1','running',1,1)", ()).await?;
            conn.execute("INSERT INTO jobs (id, execution_id, issue_id, project_id, status, branch, base_commit, base_branch, uri_segment, node_name, created_at, updated_at) VALUES ('parent-job','e-1','i-1','p-1','running','agent/parent','head-1','main','executor','Executor',1,1)", ()).await?;
            conn.execute("INSERT INTO jobs (id, execution_id, issue_id, project_id, recipe_node_id, status, uri_segment, node_name, created_at, updated_at) VALUES ('child-job','e-1','i-1','p-1','delegated-agent','pending','child','Child',1,1)", ()).await?;
            conn.execute("INSERT INTO jobs (id, execution_id, issue_id, project_id, recipe_node_id, status, uri_segment, node_name, created_at, updated_at) VALUES ('grandchild-job','e-1','i-1','p-1','delegated-nested-agent','pending','grandchild','Grandchild',1,1)", ()).await?;
            Ok::<_, DbError>(())
        })).await.unwrap();

        reparent_delegated_jobs(
            &db,
            vec![("child-job".into(), Some("delegated-agent".into()))],
            vec![packet(
                "packet-1",
                "parent-job",
                "Implement child",
                "delegated-agent",
            )],
        )
        .await
        .unwrap();
        // The child is now itself a delegating parent. Its branch is what its own
        // task must inherit.
        reparent_delegated_jobs(
            &db,
            vec![(
                "grandchild-job".into(),
                Some("delegated-nested-agent".into()),
            )],
            vec![packet(
                "packet-2",
                "child-job",
                "Implement grandchild",
                "delegated-nested-agent",
            )],
        )
        .await
        .unwrap();

        let coordinate = child_coordinate(&db, "grandchild-job").await;
        assert_eq!(coordinate.0.as_deref(), Some("child-job"));
        assert_eq!(
            coordinate.1.as_deref(),
            Some("agent/parent"),
            "a nested task continues the same logical branch as its grandparent"
        );
    }

    // ---- The delegation edge's coordinate --------------------------------
    //
    // Three steps decide where a delegated task starts: expansion emits the
    // node's branch mode, `resolve_node_behavior` reads that mode back off the
    // stored node, and `select_job_coordinate` turns it into the commit the
    // child begins from. The whole defect lived in the first step while the
    // other two were correct, so these run all three against a real jj store
    // rather than asserting an enum in isolation.

    use crate::execution::dag::recipe_node_to_db;
    use crate::execution::jobs::{select_job_coordinate, CoordinateRequest, ParentCoordinate};
    use crate::execution::step_behavior::{resolve_node_behavior, StepBehavior};
    use crate::jj::tests::{git_stdout, init_project, jj_bin};
    use crate::jj::{create_bookmark_at, ensure_project_store, JjEnv};
    use tempfile::TempDir;

    /// The behavior a delegated packet's node resolves to, taken through the
    /// same serialization the execution snapshot stores it with.
    fn delegated_behavior() -> StepBehavior {
        let node = delegated_agent_node(
            "delegated-packet-1-agent".into(),
            &packet(
                "packet-1",
                "parent-job",
                "Implement child",
                "delegated-packet-1-agent",
            ),
        );
        resolve_node_behavior(&recipe_node_to_db(&node, "recipe-1"))
    }

    #[test]
    fn a_delegated_node_requests_its_parents_coordinate() {
        let behavior = delegated_behavior();
        assert!(
            behavior.inherits_branch,
            "a delegated task continues its parent's branch; without inheritance it \
             has no branch of its own and reads and writes its base branch instead"
        );
        assert!(
            !behavior.mints_branch,
            "a task shares its parent's branch rather than minting a sibling"
        );
        assert!(
            behavior.requires_parent_head,
            "a task that cannot be seeded at the parent's head must refuse, not degrade: \
             every rung below that head is code the parent has already moved past"
        );
    }

    struct ParentAhead {
        _home: TempDir,
        _project: TempDir,
        jj: JjEnv,
        store: PathBuf,
        main_tip: String,
        parent_head: String,
    }

    /// A store whose `main` sits at the project's git tip while `agent/parent`
    /// holds one further commit reachable from no other branch: a builder with
    /// unpushed work, at the moment it delegates a task.
    ///
    /// `None` when jj is not resolvable on this machine, matching the skip
    /// convention the rest of the real-store suites use.
    fn parent_ahead_of_main() -> Option<ParentAhead> {
        let bin = jj_bin()?;
        let home = TempDir::new().unwrap();
        let project = TempDir::new().unwrap();
        init_project(project.path());
        let main_tip = git_stdout(project.path(), &["rev-parse", "HEAD"]);

        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, project.path()).unwrap();
        jj.run(
            &store,
            &["new", "main", "-m", "parent work", "--ignore-working-copy"],
            "test: parent commit",
        )
        .unwrap();
        let parent_head = jj
            .run(
                &store,
                &[
                    "log",
                    "-r",
                    "@",
                    "--no-graph",
                    "-T",
                    "commit_id",
                    "--ignore-working-copy",
                ],
                "test: parent head",
            )
            .unwrap();
        create_bookmark_at(&jj, &store, "agent/parent", &parent_head).unwrap();
        assert_ne!(parent_head, main_tip, "the parent is ahead of main");
        Some(ParentAhead {
            _home: home,
            _project: project,
            jj,
            store,
            main_tip,
            parent_head,
        })
    }

    /// THE defect (CAIRN-3309): a task delegated by a job holding unpushed
    /// commits was cut from `main`, so anything it read, built, or tested was
    /// the pre-change code its parent had already moved past.
    ///
    /// Every input here is the one production supplies: the node expansion
    /// pushes, the lineage reparenting records, and a parent row whose
    /// `base_commit` is stale at the commit main was on when the parent started.
    #[test]
    #[serial_test::serial(jj)]
    fn a_delegated_task_starts_at_the_parent_head_not_the_base_branch() {
        let Some(fx) = parent_ahead_of_main() else {
            eprintln!("skipping a_delegated_task_starts_at_the_parent_head: no jj");
            return;
        };
        let stale_recorded_base = fx.main_tip.clone();

        let (branch, base_commit) = select_job_coordinate(
            &delegated_behavior(),
            CoordinateRequest {
                job_id: "child-job",
                parent_job_id: Some("parent-job"),
                existing_branch: Some("agent/parent"),
                base_ref: "main",
            },
            &fx.jj,
            &fx.store,
            |_| {
                Ok(ParentCoordinate {
                    branch: Some("agent/parent".into()),
                    recorded_base: Some(stale_recorded_base),
                })
            },
            || unreachable!("a delegated task never mints a branch of its own"),
            |_| None,
        )
        .unwrap();

        assert_eq!(
            branch.as_deref(),
            Some("agent/parent"),
            "the task continues the parent's branch"
        );
        assert_eq!(
            base_commit.as_deref(),
            Some(fx.parent_head.as_str()),
            "the task starts at the parent's live logical head"
        );
        assert_ne!(
            base_commit.as_deref(),
            Some(fx.main_tip.as_str()),
            "starting from the base branch is the defect: the parent's work would be invisible"
        );
    }

    /// The second form of the same defect: the parent's branch *name* is intact,
    /// so lineage looks healthy, but the store cannot resolve the bookmark. The
    /// degrading ladder would answer with the parent's recorded base and then
    /// `main` — a child row reading `branch = agent/parent` whose commit is
    /// pre-change code. A delegated task refuses instead.
    #[test]
    #[serial_test::serial(jj)]
    fn a_delegated_task_refuses_a_parent_branch_the_store_cannot_resolve() {
        let Some(fx) = parent_ahead_of_main() else {
            eprintln!("skipping a_delegated_task_refuses_an_unresolvable_parent_branch: no jj");
            return;
        };
        let recorded_base = fx.main_tip.clone();

        let error = select_job_coordinate(
            &delegated_behavior(),
            CoordinateRequest {
                job_id: "child-job",
                parent_job_id: Some("parent-job"),
                existing_branch: Some("agent/vanished"),
                base_ref: "main",
            },
            &fx.jj,
            &fx.store,
            |_| {
                Ok(ParentCoordinate {
                    // Never created in this store, and a recorded base that
                    // resolves — the exact shape the ladder would accept.
                    branch: Some("agent/vanished".into()),
                    recorded_base: Some(recorded_base),
                })
            },
            || unreachable!("a delegated task never mints a branch of its own"),
            |_| None,
        )
        .expect_err("an unresolvable parent branch must refuse rather than degrade to base");

        assert!(error.contains("child-job"), "{error}");
        assert!(error.contains("parent job parent-job"), "{error}");
        assert!(error.contains("agent/vanished"), "{error}");
    }

    /// The strict grade is scoped to the delegation edge. An authored `inherit`
    /// node — the PR review node is the one in the bundled recipes — keeps the
    /// ladder that CAIRN-3226 weighed and kept, so this change cannot have
    /// quietly made every inheriting node fail-closed.
    #[test]
    #[serial_test::serial(jj)]
    fn an_authored_inherit_node_still_degrades() {
        let Some(fx) = parent_ahead_of_main() else {
            eprintln!("skipping an_authored_inherit_node_still_degrades: no jj");
            return;
        };
        let recorded_base = fx.main_tip.clone();
        let authored = resolve_node_behavior(&recipe_node_to_db(
            &RecipeNode {
                id: "review-1".into(),
                node_type: RecipeNodeType::Agent,
                name: "Review".into(),
                position: NodePosition { x: 0.0, y: 0.0 },
                parent_id: None,
                trigger_config: None,
                agent_config: Some(AgentNodeConfig {
                    agent_config_id: Some("pr-review".into()),
                    output_schema: None,
                    git_config: Some(AgentGitConfig {
                        branch_mode: BranchMode::Inherit,
                        require_parent_head: false,
                    }),
                }),
                action_config: None,
                checkpoint_config: None,
                artifact_config: None,
                condition_config: None,
                context_config: None,
            },
            "recipe-1",
        ));

        let (_, base_commit) = select_job_coordinate(
            &authored,
            CoordinateRequest {
                job_id: "review-job",
                parent_job_id: Some("parent-job"),
                existing_branch: None,
                base_ref: "main",
            },
            &fx.jj,
            &fx.store,
            |_| {
                Ok(ParentCoordinate {
                    branch: Some("agent/vanished".into()),
                    recorded_base: Some(recorded_base),
                })
            },
            || unreachable!("an inheriting job mints nothing"),
            |_| None,
        )
        .expect("an authored inherit node degrades rather than refusing");

        assert_eq!(
            base_commit.as_deref(),
            Some(fx.main_tip.as_str()),
            "the ladder's verified recorded-base rung still carries an authored inherit node"
        );
    }

    #[test]
    fn schema_config_bakes_inline_custom_schema_into_node() {
        let inline = serde_json::json!({
            "type": "object",
            "properties": {"score": {"type": "number"}},
            "required": ["score"],
            "additionalProperties": false
        });
        let contract = crate::models::DelegatedOutputContract {
            schema_type: crate::models::OutputSchema::Custom(inline.clone()),
            tool_name: None,
            description: Some("Submit the task result".to_string()),
        };
        let config = schema_config_from_output_contract(&contract);
        // A custom inline schema writes to the canonical `return` artifact and
        // bakes the schema verbatim into the node config.
        assert_eq!(config.name, "return");
        assert_eq!(config.schema, Some(inline));
    }

    #[test]
    fn schema_config_resolves_preset_schema_into_node() {
        let contract = crate::models::DelegatedOutputContract {
            schema_type: crate::models::OutputSchema::Preset("review".to_string()),
            tool_name: None,
            description: None,
        };
        let config = schema_config_from_output_contract(&contract);
        assert_eq!(config.name, "review");
        // The preset's embedded JSON Schema is baked in (review requires approval).
        let schema = config.schema.expect("preset schema resolved");
        assert!(schema["required"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v == "approval"));
    }

    #[test]
    fn schema_config_default_return_contract_unchanged() {
        let contract = crate::models::DelegatedOutputContract {
            schema_type: crate::models::OutputSchema::Preset("return".to_string()),
            tool_name: None,
            description: Some("Submit the task result".to_string()),
        };
        let config = schema_config_from_output_contract(&contract);
        assert_eq!(config.name, "return");
        let schema = config.schema.expect("return schema resolved");
        assert!(schema["required"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v == "content"));
    }

    #[test]
    fn child_return_write_validates_against_custom_schema() {
        // The schema a delegated child validates its return artifact against is
        // the one baked into its node config. Reconstruct that schema and confirm
        // it accepts a conforming payload and rejects a violating one.
        let inline = serde_json::json!({
            "type": "object",
            "properties": {"score": {"type": "number"}},
            "required": ["score"],
            "additionalProperties": false
        });
        let contract = crate::models::DelegatedOutputContract {
            schema_type: crate::models::OutputSchema::Custom(inline),
            tool_name: None,
            description: None,
        };
        let schema = schema_config_from_output_contract(&contract)
            .schema
            .expect("custom schema baked in");
        let validator = jsonschema::validator_for(&schema).unwrap();
        assert!(validator.is_valid(&serde_json::json!({"score": 42})));
        assert!(!validator.is_valid(&serde_json::json!({"nope": true})));
    }
}
