use super::*;

pub fn create_jobs_for_new_nodes(
    db: Arc<LocalDb>,
    execution_id: &str,
    new_node_ids: &HashSet<String>,
    snapshot: &ExecutionSnapshot,
) -> Result<Vec<Job>, String> {
    let execution_id = execution_id.to_string();
    let new_node_ids = new_node_ids.clone();
    let snapshot = snapshot.clone();
    run_advancement_db(async move {
        db.write(|conn| {
            let execution_id = execution_id.clone();
            let new_node_ids = new_node_ids.clone();
            let snapshot = snapshot.clone();
            Box::pin(async move {
                create_jobs_for_new_nodes_conn(conn, &execution_id, &new_node_ids, &snapshot).await
            })
        })
        .await
        .map_err(|e| format!("Failed to create jobs for new nodes: {e}"))
    })
}

pub(crate) fn create_jobs_for_execution(
    db: Arc<LocalDb>,
    execution_id: &str,
) -> Result<Vec<Job>, String> {
    let execution_id = execution_id.to_string();
    run_advancement_db(async move {
        db.write(|conn| {
            let execution_id = execution_id.clone();
            Box::pin(async move {
                let snapshot = load_execution_snapshot_conn(conn, &execution_id).await?;
                let node_ids: HashSet<String> = snapshot
                    .recipe
                    .nodes
                    .iter()
                    .map(|node| node.id.clone())
                    .collect();
                create_jobs_for_new_nodes_conn(conn, &execution_id, &node_ids, &snapshot).await
            })
        })
        .await
        .map_err(|e| format!("Failed to create jobs for execution: {e}"))
    })
}

pub(crate) async fn create_jobs_for_new_nodes_conn(
    conn: &cairn_db::turso::Connection,
    execution_id: &str,
    new_node_ids: &HashSet<String>,
    snapshot: &ExecutionSnapshot,
) -> DbResult<Vec<Job>> {
    let issue_id = snapshot.trigger_context.issue_id.as_deref().unwrap_or("");
    let project_id = &snapshot.trigger_context.project_id;
    let db_nodes = snapshot_nodes_to_db(snapshot);
    let db_edges = snapshot_edges_to_db(snapshot);
    let node_map: HashMap<String, &DbRecipeNode> = db_nodes
        .iter()
        .map(|node| (node.id.clone(), node))
        .collect();

    let mut reverse_control_edges: HashMap<String, Vec<String>> = HashMap::new();
    for edge in &db_edges {
        if edge.edge_type == "control" {
            reverse_control_edges
                .entry(edge.target_node_id.clone())
                .or_default()
                .push(edge.source_node_id.clone());
        }
    }

    let mut existing_rows = conn
        .query(
            "SELECT recipe_node_id, id
             FROM jobs
             WHERE execution_id = ?1",
            (execution_id,),
        )
        .await?;
    let mut node_to_job: HashMap<String, String> = HashMap::new();
    while let Some(row) = existing_rows.next().await? {
        if let Some(node_id) = row.opt_text(0)? {
            node_to_job.insert(node_id, row.text(1)?);
        }
    }

    let node_ids: Vec<String> = crate::execution::dag::find_reachable_nodes(&db_nodes, &db_edges)
        .into_iter()
        .filter(|node_id| new_node_ids.contains(node_id))
        .collect();

    let now = chrono::Utc::now().timestamp() as i32;
    let mut created_jobs = Vec::new();
    for node_id in node_ids {
        let Some(node) = node_map.get(&node_id).copied() else {
            continue;
        };
        if node.node_type != "agent" {
            continue;
        }

        let agent_config_id = node
            .config
            .as_ref()
            .and_then(|config| serde_json::from_str::<serde_json::Value>(config).ok())
            .and_then(|value| {
                value
                    .get("agentConfigId")
                    .and_then(|id| id.as_str())
                    .map(ToOwned::to_owned)
            });

        let behavior = crate::execution::step_behavior::resolve_node_behavior(node);
        let parent_job_id = if behavior.inherits_worktree {
            find_parent_agent_job_id(&node_id, &reverse_control_edges, &node_map, &node_to_job)
        } else {
            None
        };

        let job = insert_job_for_node_conn(
            conn,
            execution_id,
            issue_id,
            project_id,
            node,
            agent_config_id.as_deref(),
            parent_job_id.as_deref(),
            now,
            snapshot,
        )
        .await?;

        node_to_job.insert(node_id, job.id.clone());
        created_jobs.push(job);
    }

    Ok(created_jobs)
}

fn find_parent_agent_job_id(
    node_id: &str,
    reverse_edges: &HashMap<String, Vec<String>>,
    node_map: &HashMap<String, &DbRecipeNode>,
    node_to_job: &HashMap<String, String>,
) -> Option<String> {
    let mut visited = HashSet::new();
    let mut queue = VecDeque::new();

    if let Some(parents) = reverse_edges.get(node_id) {
        queue.extend(parents.iter().cloned());
    }

    while let Some(parent_id) = queue.pop_front() {
        if !visited.insert(parent_id.clone()) {
            continue;
        }

        if let Some(parent_node) = node_map.get(&parent_id) {
            if parent_node.node_type == "agent" {
                return node_to_job.get(&parent_id).cloned();
            }
        }

        if let Some(grandparents) = reverse_edges.get(&parent_id) {
            queue.extend(grandparents.iter().cloned());
        }
    }

    None
}

#[allow(clippy::too_many_arguments)]
async fn insert_job_for_node_conn(
    conn: &cairn_db::turso::Connection,
    execution_id: &str,
    issue_id: &str,
    project_id: &str,
    node: &DbRecipeNode,
    agent_config_id: Option<&str>,
    parent_job_id: Option<&str>,
    now: i32,
    snapshot: &ExecutionSnapshot,
) -> DbResult<Job> {
    let job_id = ids::mint_child(execution_id);
    let model_str = agent_config_id
        .and_then(|id| snapshot.agents.get(id))
        .and_then(|agent| agent.selection.as_ref())
        .map(|selection| selection.model.to_string());
    let delegated_packet = snapshot.delegated_packets.iter().find(|packet| {
        packet
            .materialized_node_ids
            .iter()
            .any(|materialized_node_id| materialized_node_id == &node.id)
    });
    let requested_forked_session = delegated_packet
        .map(|packet| packet.session.mode == DelegatedSessionMode::Fork)
        .unwrap_or(false);
    let parent_backend = if let Some(packet) = delegated_packet {
        let mut rows = conn
            .query(
                "SELECT model FROM jobs WHERE id = ?1",
                (packet.parent_job_id.as_str(),),
            )
            .await?;
        rows.next()
            .await?
            .map(|row| row.opt_text(0))
            .transpose()?
            .flatten()
            .as_deref()
            .and_then(crate::backends::backend_for_model)
            .map(str::to_string)
    } else {
        None
    };
    let child_backend = backend_for_job_session(snapshot, agent_config_id);
    let create_forked_session = requested_forked_session
        && parent_backend
            .as_deref()
            .map(|backend| backend == child_backend)
            .unwrap_or(false);
    let session_id = (!create_forked_session).then(|| ids::mint_session_id().into_string());

    let uri_segment = if let Some(parent_job_id) = parent_job_id {
        let base = crate::node_segments::task_segment_base(
            Some(node.name.as_str()),
            Some(node.name.as_str()),
            agent_config_id,
        );
        crate::node_segments::allocate_child_task_segment(conn, parent_job_id, &base).await?
    } else if issue_id.is_empty() {
        let base = crate::node_segments::node_segment_base(
            Some(node.name.as_str()),
            None,
            Some(node.id.as_str()),
        );
        crate::node_segments::allocate_project_job_segment(conn, project_id, &base).await?
    } else {
        let base = crate::node_segments::node_segment_base(
            Some(node.name.as_str()),
            None,
            Some(node.id.as_str()),
        );
        crate::node_segments::allocate_top_level_segment(conn, issue_id, execution_id, &base)
            .await?
    };

    let base_branch = base_branch_for_issue_job(conn, project_id, issue_id).await?;

    conn.execute(
        "INSERT INTO jobs(
            id, execution_id, recipe_node_id, parent_job_id,
            worktree_path, branch, base_commit, current_session_id, resume_session_id,
            status, agent_config_id, issue_id, project_id, task_description,
            created_at, updated_at, completed_at, parent_tool_use_id, task_index,
            started_at, model, node_name, base_branch, current_turn_id, uri_segment
         )
         VALUES(
            ?1, ?2, ?3, ?4,
            NULL, NULL, NULL, ?5, NULL,
            'pending', ?6, ?7, ?8, ?9,
            ?10, ?10, NULL, NULL, NULL,
            NULL, ?11, ?12, ?13, NULL, ?14
         )",
        params![
            job_id.as_str(),
            execution_id,
            node.id.as_str(),
            parent_job_id,
            session_id.as_deref(),
            agent_config_id,
            if issue_id.is_empty() {
                None
            } else {
                Some(issue_id)
            },
            project_id,
            Some(node.name.as_str()),
            now,
            model_str.as_deref(),
            Some(node.name.as_str()),
            base_branch.as_deref(),
            uri_segment.as_str(),
        ],
    )
    .await?;

    if create_forked_session {
        let packet = delegated_packet.expect("delegated fork packet must exist");
        let mut parent_rows = conn
            .query(
                "SELECT current_session_id FROM jobs WHERE id = ?1",
                (packet.parent_job_id.as_str(),),
            )
            .await?;
        let parent_session_id = parent_rows
            .next()
            .await?
            .map(|row| row.opt_text(0))
            .transpose()?
            .flatten();

        if let Some(source_session_id) = parent_session_id {
            let (source_backend, source_sequence) =
                load_session_backend_sequence_conn(conn, &source_session_id).await?;
            let forked_session_id = ids::mint_session_id().into_string();
            insert_session_conn(
                conn,
                &forked_session_id,
                Some(&job_id),
                &source_backend,
                Some(&source_session_id),
                source_sequence + 1,
                now,
            )
            .await?;
            conn.execute(
                "UPDATE jobs SET current_session_id = ?1, updated_at = ?2 WHERE id = ?3",
                params![forked_session_id.as_str(), now, job_id.as_str()],
            )
            .await?;
        } else {
            let fallback_session_id = ids::mint_session_id().into_string();
            insert_session_conn(
                conn,
                &fallback_session_id,
                Some(&job_id),
                &child_backend,
                None,
                1,
                now,
            )
            .await?;
            conn.execute(
                "UPDATE jobs SET current_session_id = ?1, updated_at = ?2 WHERE id = ?3",
                params![fallback_session_id.as_str(), now, job_id.as_str()],
            )
            .await?;
        }
    } else {
        let session_id = session_id.ok_or_else(|| db_internal("Missing generated session id"))?;
        insert_session_conn(
            conn,
            &session_id,
            Some(&job_id),
            &child_backend,
            None,
            1,
            now,
        )
        .await?;
    }

    let db_job = load_job_by_id_conn(conn, &job_id)
        .await?
        .ok_or_else(|| db_internal(format!("Failed to load created job: {job_id}")))?;
    Job::try_from(db_job).map_err(db_internal)
}

async fn base_branch_for_issue_job(
    conn: &cairn_db::turso::Connection,
    project_id: &str,
    issue_id: &str,
) -> DbResult<Option<String>> {
    if issue_id.is_empty() {
        return Ok(None);
    }

    match crate::issues::relations::resolve_parent_branch(conn, issue_id).await? {
        Some(branch) => Ok(Some(branch)),
        None => Ok(Some(default_branch_for_project(conn, project_id).await?)),
    }
}

async fn default_branch_for_project(
    conn: &cairn_db::turso::Connection,
    project_id: &str,
) -> DbResult<String> {
    let mut rows = conn
        .query(
            "SELECT repo_path, default_branch FROM projects WHERE id = ?1",
            (project_id,),
        )
        .await?;
    let (repo_path, stored_default_branch) = match rows.next().await? {
        Some(row) => (row.text(0)?, row.opt_text(1)?),
        None => (String::new(), None),
    };

    // The project-config `defaultBranch` override wins over the stored column,
    // mirroring how a project is projected for the UI, so worktree creation and
    // display always agree on the base branch.
    let config =
        crate::config::project_settings::load_project_settings(std::path::Path::new(&repo_path));
    Ok(crate::config::project_settings::resolve_default_branch(
        &config,
        stored_default_branch.as_deref(),
    ))
}

fn backend_for_job_session(snapshot: &ExecutionSnapshot, agent_config_id: Option<&str>) -> String {
    agent_config_id
        .and_then(|id| snapshot.agents.get(id))
        .and_then(|agent| {
            agent
                .selection
                .as_ref()
                .map(|selection| selection.backend.clone())
                .or_else(|| agent.backend_preference.clone())
        })
        .unwrap_or_else(|| "claude".to_string())
}

async fn load_session_backend_sequence_conn(
    conn: &cairn_db::turso::Connection,
    session_id: &str,
) -> DbResult<(String, i32)> {
    let mut rows = conn
        .query(
            "SELECT backend, sequence FROM sessions WHERE id = ?1",
            (session_id,),
        )
        .await?;
    rows.next()
        .await?
        .map(|row| Ok::<_, DbError>((row.text(0)?, row.i64(1)? as i32)))
        .transpose()?
        .ok_or_else(|| db_internal(format!("Session not found: {session_id}")))
}

async fn insert_session_conn(
    conn: &cairn_db::turso::Connection,
    session_id: &str,
    job_id: Option<&str>,
    backend: &str,
    parent_session_id: Option<&str>,
    sequence: i32,
    now: i32,
) -> DbResult<()> {
    conn.execute(
        "INSERT INTO sessions(
            id, job_id, chat_id, backend, status, parent_session_id, replaced_by_id,
            terminal_reason, sequence, created_at, closed_at, updated_at, backend_id
        )
        VALUES (?1, ?2, NULL, ?3, 'open', ?4, NULL, NULL, ?5, ?6, NULL, ?6, NULL)",
        params![
            session_id,
            job_id,
            backend,
            parent_session_id,
            sequence,
            now
        ],
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::LocalDb;

    async fn migrated_db() -> LocalDb {
        crate::storage::migrated_test_db("job-creation-test.db").await
    }

    #[tokio::test(flavor = "current_thread")]
    async fn base_branch_falls_back_to_default_for_top_level_issue() {
        let db = migrated_db().await;
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
                     VALUES ('proj-1', 'default', 'Project', 'PROJ', '/repo', 'trunk', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO issues (id, project_id, number, title, created_at, updated_at)
                     VALUES ('issue-1', 'proj-1', 1, 'Issue', 1, 1)",
                    (),
                )
                .await?;
                let branch = base_branch_for_issue_job(conn, "proj-1", "issue-1").await?;
                assert_eq!(branch.as_deref(), Some("trunk"));
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn base_branch_uses_parent_active_branch_for_child_issue() {
        let db = migrated_db().await;
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
                     VALUES ('proj-1', 'default', 'Project', 'PROJ', '/repo', 'main', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO issues (id, project_id, number, title, created_at, updated_at)
                     VALUES ('parent', 'proj-1', 1, 'Parent', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO issues (id, project_id, number, title, parent_issue_id, created_at, updated_at)
                     VALUES ('child', 'proj-1', 2, 'Child', 'parent', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
                     VALUES ('exec-1', 'recipe-default', 'parent', 'proj-1', 'running', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, worktree_path, branch, created_at, updated_at)
                     VALUES ('job-parent', 'exec-1', 'node', 'parent', 'proj-1', 'running', '/wt/parent', 'integration', 1, 1)",
                    (),
                )
                .await?;
                let branch = base_branch_for_issue_job(conn, "proj-1", "child").await?;
                assert_eq!(branch.as_deref(), Some("integration"));
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn base_branch_honors_project_config_override() {
        // Repo with a `.cairn/config.yaml` that overrides the default branch.
        let repo = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(repo.path().join(".cairn")).unwrap();
        std::fs::write(
            repo.path().join(".cairn/config.yaml"),
            "defaultBranch: staging\n",
        )
        .unwrap();
        let repo_path = repo.path().to_string_lossy().to_string();

        let db = migrated_db().await;
        db.write(|conn| {
            let repo_path = repo_path.clone();
            Box::pin(async move {
                // Stored column is the stale "main"; the config override should win.
                conn.execute(
                    "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
                     VALUES ('proj-1', 'default', 'Project', 'PROJ', ?1, 'main', 1, 1)",
                    (repo_path.as_str(),),
                )
                .await?;
                conn.execute(
                    "INSERT INTO issues (id, project_id, number, title, created_at, updated_at)
                     VALUES ('issue-1', 'proj-1', 1, 'Issue', 1, 1)",
                    (),
                )
                .await?;
                let branch = base_branch_for_issue_job(conn, "proj-1", "issue-1").await?;
                assert_eq!(branch.as_deref(), Some("staging"));
                Ok(())
            })
        })
        .await
        .unwrap();
    }
}
