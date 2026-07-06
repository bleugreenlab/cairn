use super::*;

pub(super) fn ensure_agent_snapshot(
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
    // The contract's schema_type names the preset shape (e.g. "return"); it
    // doubles as the artifact name/URI segment. Bake the preset's JSON Schema
    // inline so the node is self-contained (no runtime preset reference).
    let name = if contract.schema_type.is_empty() {
        "return".to_string()
    } else {
        contract.schema_type.clone()
    };
    let schema = crate::output_schemas::resolve_output_schema(
        None,
        &crate::models::OutputSchema::Preset(name.clone()),
    )
    .ok();
    SchemaConfig {
        name,
        schema,
        confirm_policy: crate::models::ConfirmPolicy::default(),
        tool_name: contract.tool_name.clone(),
        description: contract.description.clone(),
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

        // An ambient (no-worktree) parent's delegated task cannot inherit a
        // worktree and must not run in the user's live checkout, so the node owns
        // a fresh ephemeral worktree (WorktreeMode::Own) off the default branch,
        // reclaimed when the task job terminalizes. A worktree-backed parent's
        // task inherits the parent's worktree via reparenting (WorktreeMode::None,
        // unchanged). `reparent_delegated_jobs` marks the ambient case
        // `owns_ephemeral_worktree`.
        let worktree_mode = if parent_job_has_worktree(db, &packet_view.parent_job_id)? {
            WorktreeMode::None
        } else {
            WorktreeMode::Own
        };

        if !snapshot.recipe.nodes.iter().any(|node| node.id == agent_id) {
            snapshot.recipe.nodes.push(RecipeNode {
                id: agent_id.clone(),
                node_type: RecipeNodeType::Agent,
                name: packet_view.title.clone(),
                position: NodePosition { x: 400.0, y: 0.0 },
                parent_id: None,
                trigger_config: None,
                agent_config: Some(AgentNodeConfig {
                    agent_config_id: Some(packet_view.agent_config_id.clone()),
                    output_schema: Some(schema_config_from_output_contract(
                        &packet_view.output_contract,
                    )),
                    git_config: Some(AgentGitConfig { worktree_mode }),
                }),
                action_config: None,
                checkpoint_config: None,
                artifact_config: None,
                condition_config: None,
                context_config: None,
            });
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

/// Whether the delegating parent job owns a worktree. An ambient (Branch: main /
/// no-worktree) parent returns `false`, which routes its task onto its own
/// ephemeral worktree (`WorktreeMode::Own` at materialization,
/// `owns_ephemeral_worktree` at reparenting).
fn parent_job_has_worktree(db: &Arc<LocalDb>, parent_job_id: &str) -> Result<bool, String> {
    let db = db.clone();
    let parent_job_id = parent_job_id.to_string();
    run_advancement_db(async move {
        db.read(|conn| {
            let parent_job_id = parent_job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT worktree_path FROM jobs WHERE id = ?1",
                        params![parent_job_id.as_str()],
                    )
                    .await?;
                let has_worktree = match rows.next().await? {
                    Some(row) => row.opt_text(0)?.is_some(),
                    None => false,
                };
                Ok(has_worktree)
            })
        })
        .await
        .map_err(|e| format!("Failed to read parent worktree state: {e}"))
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
                        "SELECT worktree_path FROM jobs WHERE id = ?1",
                        (packet.parent_job_id.as_str(),),
                    )
                    .await?;
                let parent_worktree = parent_rows
                    .next()
                    .await?
                    .map(|row| row.opt_text(0))
                    .transpose()?
                    .flatten();

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

                // An ambient (no-worktree) parent leaves `parent_worktree` NULL:
                // the reparented job keeps a NULL worktree_path (its
                // WorktreeMode::Own node mints a fresh worktree at activation) and
                // is marked `owns_ephemeral_worktree` so it is reclaimed on
                // completion. A worktree-backed parent inherits its path here and
                // is not an ephemeral owner.
                let owns_ephemeral_worktree: i64 = if parent_worktree.is_none() { 1 } else { 0 };

                // Keep node_name and uri_segment in lockstep so the addressable
                // segment is the disambiguated, parent-unique slug. Carry the
                // packet's parent_tool_use_id onto the job so the transcript can
                // locate spawned children by the originating tool-use id
                // (`list_child_jobs`). Without this the column stays NULL and the
                // live task windows can never resolve.
                conn.execute(
                    "UPDATE jobs
                         SET parent_job_id = ?1,
                             worktree_path = ?2,
                             task_index = ?3,
                             node_name = ?4,
                             uri_segment = ?4,
                             parent_tool_use_id = ?5,
                             owns_ephemeral_worktree = ?6
                         WHERE id = ?7",
                    params![
                        packet.parent_job_id.as_str(),
                        parent_worktree.as_deref(),
                        packet.task_index,
                        slug.as_str(),
                        packet.parent_tool_use_id.as_deref(),
                        owns_ephemeral_worktree,
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

    #[tokio::test]
    async fn reparented_delegated_job_inherits_parent_worktree_path() {
        let db = test_db().await;
        db.write(|conn| {
            Box::pin(async move {
                conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-1','W',1,1)", ()).await?;
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-1','w-1','P','P','/tmp/p',1,1)", ()).await?;
                conn.execute("INSERT INTO issues (id, project_id, number, title, status, attention, created_at, updated_at) VALUES ('i-1','p-1',1,'T','active','none',1,1)", ()).await?;
                conn.execute("INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq) VALUES ('e-1','default','i-1','p-1','running',1,1)", ()).await?;
                conn.execute("INSERT INTO jobs (id, execution_id, issue_id, project_id, status, worktree_path, uri_segment, node_name, created_at, updated_at) VALUES ('parent-job','e-1','i-1','p-1','running','/tmp/parent-worktree','executor','Executor',1,1)", ()).await?;
                conn.execute("INSERT INTO jobs (id, execution_id, issue_id, project_id, recipe_node_id, status, uri_segment, node_name, created_at, updated_at) VALUES ('child-job','e-1','i-1','p-1','delegated-agent','pending','child','Child',1,1)", ()).await?;
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();

        let packet = DelegatedWorkPacket {
            id: "packet-1".to_string(),
            parent_job_id: "parent-job".to_string(),
            parent_turn_id: Some("turn-1".to_string()),
            parent_tool_use_id: Some("tool-1".to_string()),
            origin: DelegationOrigin::TaskTool,
            title: "Implement child".to_string(),
            problem_statement: "Do the child task".to_string(),
            agent_config_id: "build".to_string(),
            ownership: DelegatedOwnershipScope {
                cwd: "/not/the/process/cwd".to_string(),
                fence: None,
                sandbox: None,
                on_escape: None,
            },
            session: DelegatedSessionStrategy::default(),
            acceptance: vec![],
            output_contract: DelegatedOutputContract {
                schema_type: "return".to_string(),
                tool_name: None,
                description: None,
            },
            status: DelegatedStatus::Materialized,
            materialized_node_ids: vec!["delegated-agent".to_string()],
            result_artifact_job_id: None,
            task_index: Some(7),
            tier_override: None,
            backend_preference: None,
            background: false,
            created_at: 1,
        };

        reparent_delegated_jobs(
            &db,
            vec![("child-job".to_string(), Some("delegated-agent".to_string()))],
            vec![packet],
        )
        .await
        .unwrap();

        let inherited = db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT parent_job_id, worktree_path, task_index, uri_segment, parent_tool_use_id FROM jobs WHERE id = 'child-job'",
                            (),
                        )
                        .await?;
                    let row = rows
                        .next()
                        .await?
                        .ok_or_else(|| DbError::Row("child job missing".to_string()))?;
                    Ok::<_, DbError>((
                        row.opt_text(0)?,
                        row.opt_text(1)?,
                        row.opt_i64(2)?,
                        row.opt_text(3)?,
                        row.opt_text(4)?,
                    ))
                })
            })
            .await
            .unwrap();

        assert_eq!(inherited.0.as_deref(), Some("parent-job"));
        assert_eq!(inherited.1.as_deref(), Some("/tmp/parent-worktree"));
        assert_eq!(inherited.2, Some(7));
        assert_eq!(inherited.3.as_deref(), Some("implement-child"));
        assert_eq!(inherited.4.as_deref(), Some("tool-1"));
    }

    #[tokio::test]
    async fn reparented_delegated_job_ambient_parent_marks_ephemeral() {
        // An ambient (Branch: main / no-worktree) parent's delegated task keeps a
        // NULL worktree_path (its WorktreeMode::Own node mints a fresh worktree at
        // activation) and is marked owns_ephemeral_worktree so it is reclaimed on
        // completion — never inheriting, never running in the live checkout.
        let db = test_db().await;
        db.write(|conn| {
            Box::pin(async move {
                conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-1','W',1,1)", ()).await?;
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-1','w-1','P','P','/tmp/p',1,1)", ()).await?;
                conn.execute("INSERT INTO issues (id, project_id, number, title, status, attention, created_at, updated_at) VALUES ('i-1','p-1',1,'T','active','none',1,1)", ()).await?;
                conn.execute("INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq) VALUES ('e-1','default','i-1','p-1','running',1,1)", ()).await?;
                // Ambient parent: a branch but NO worktree_path.
                conn.execute("INSERT INTO jobs (id, execution_id, issue_id, project_id, status, branch, uri_segment, node_name, created_at, updated_at) VALUES ('parent-job','e-1','i-1','p-1','running','agent/main','parent','Parent',1,1)", ()).await?;
                conn.execute("INSERT INTO jobs (id, execution_id, issue_id, project_id, recipe_node_id, status, uri_segment, node_name, created_at, updated_at) VALUES ('child-job','e-1','i-1','p-1','delegated-agent','pending','child','Child',1,1)", ()).await?;
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();

        let packet = DelegatedWorkPacket {
            id: "packet-1".to_string(),
            parent_job_id: "parent-job".to_string(),
            parent_turn_id: Some("turn-1".to_string()),
            parent_tool_use_id: Some("tool-1".to_string()),
            origin: DelegationOrigin::TaskTool,
            title: "Explore something".to_string(),
            problem_statement: "Do the child task".to_string(),
            agent_config_id: "explore".to_string(),
            ownership: DelegatedOwnershipScope {
                cwd: "/not/the/process/cwd".to_string(),
                fence: None,
                sandbox: None,
                on_escape: None,
            },
            session: DelegatedSessionStrategy::default(),
            acceptance: vec![],
            output_contract: DelegatedOutputContract {
                schema_type: "return".to_string(),
                tool_name: None,
                description: None,
            },
            status: DelegatedStatus::Materialized,
            materialized_node_ids: vec!["delegated-agent".to_string()],
            result_artifact_job_id: None,
            task_index: Some(0),
            tier_override: None,
            backend_preference: None,
            background: false,
            created_at: 1,
        };

        reparent_delegated_jobs(
            &db,
            vec![("child-job".to_string(), Some("delegated-agent".to_string()))],
            vec![packet],
        )
        .await
        .unwrap();

        let (worktree_path, owns_ephemeral): (Option<String>, i64) = db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT worktree_path, owns_ephemeral_worktree FROM jobs WHERE id = 'child-job'",
                            (),
                        )
                        .await?;
                    let row = rows
                        .next()
                        .await?
                        .ok_or_else(|| DbError::Row("child job missing".to_string()))?;
                    Ok::<_, DbError>((row.opt_text(0)?, row.i64(1)?))
                })
            })
            .await
            .unwrap();

        assert_eq!(
            worktree_path, None,
            "ambient task keeps a NULL worktree_path"
        );
        assert_eq!(owns_ephemeral, 1, "ambient task is marked ephemeral-owning");
    }
}
