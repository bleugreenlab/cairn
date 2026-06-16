use crate::execution::delegation::{
    DelegatedTaskPayload, DelegatedTaskSessionMode, SpawnTaskPacketsInput,
};
use crate::execution::jobs::CreateChildTaskInput;
use crate::mcp::types::{
    AskUserPayload, ChangeItem, ChangeMode, McpCallbackRequest, TaskPayload, TaskSessionMode,
};
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};
use cairn_common::uri::{build_node_uri, parse_uri, CairnResource};

/// A blocking append routed to a node's tasks/questions collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BlockingKind {
    Tasks,
    Questions,
}

/// Classify a change item as a blocking append, if it is one. Only `mode=append`
/// on a node tasks/questions collection qualifies.
pub(crate) fn blocking_append_kind(item: &ChangeItem) -> Option<BlockingKind> {
    if item.mode != ChangeMode::Append {
        return None;
    }
    match parse_uri(&item.target) {
        Some(CairnResource::NodeTasks { .. }) => Some(BlockingKind::Tasks),
        Some(CairnResource::NodeQuestions { .. }) => Some(BlockingKind::Questions),
        _ => None,
    }
}

/// Validate the blocking-append group within a single change call.
///
/// At most one blocking group is allowed: any number of task appends (run as a
/// batch) OR a single question append. Mixing the two, or multiple question
/// appends, is rejected so the suspend/resume contract stays unambiguous.
pub(crate) fn validate_blocking_group(
    changes: &[ChangeItem],
    indices: &[usize],
) -> Result<Option<BlockingKind>, String> {
    if indices.is_empty() {
        return Ok(None);
    }
    let mut tasks = 0usize;
    let mut questions = 0usize;
    for &i in indices {
        match blocking_append_kind(&changes[i]) {
            Some(BlockingKind::Tasks) => tasks += 1,
            Some(BlockingKind::Questions) => questions += 1,
            None => {}
        }
    }
    if tasks > 0 && questions > 0 {
        return Err("Cannot mix task and question appends in a single change call".to_string());
    }
    if questions > 1 {
        return Err("Only one questions append is supported per change call".to_string());
    }
    Ok(Some(if tasks > 0 {
        BlockingKind::Tasks
    } else {
        BlockingKind::Questions
    }))
}

/// Run the validated blocking append group, returning the tool result text
/// (a task result / suspend marker, or a question answer / recorded marker).
pub(crate) async fn run_blocking_group(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    changes: &[ChangeItem],
    indices: &[usize],
    kind: BlockingKind,
) -> String {
    match kind {
        BlockingKind::Tasks => run_tasks_group(orch, request, changes, indices).await,
        BlockingKind::Questions => {
            let Some(payload) = changes[indices[0]].payload.clone() else {
                return "Question append requires payload with a questions array".to_string();
            };
            let background = payload
                .get("background")
                .and_then(|value| value.as_bool())
                .unwrap_or(false);
            let ask: AskUserPayload = match serde_json::from_value(payload) {
                Ok(ask) => ask,
                Err(e) => return format!("Invalid question append payload: {e}"),
            };
            let resolved_tool_use_id = match request.run_id.as_deref() {
                Some(run_id) => resolve_change_tool_use_id(orch, run_id, "/questions").await,
                None => None,
            };
            let tool_use_id = request
                .tool_use_id
                .as_deref()
                .or(resolved_tool_use_id.as_deref());
            crate::mcp::handlers::planning::ask_questions(
                orch,
                request,
                ask,
                background,
                tool_use_id,
            )
            .await
        }
    }
}

/// Node-tasks URI coordinates: `(project_key, number, exec_seq, node_id)`.
type NodeTasksCoords = (String, i32, i32, String);

/// Extract the node coordinates from a tasks-collection append target.
/// Returns `None` if the target is not a `NodeTasks` URI.
fn node_tasks_coords(target: &str) -> Option<NodeTasksCoords> {
    match parse_uri(target) {
        Some(CairnResource::NodeTasks {
            project,
            number,
            exec_seq,
            node_id,
        }) => Some((project, number, exec_seq, node_id)),
        _ => None,
    }
}

/// How a batch of tasks appends routes, decided by comparing each addressed
/// node's job against the caller's own node job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TaskRoute {
    /// Every append targets the caller's own node: the existing delegated
    /// task-packet pipeline (batching, inline wait, durable suspend, background).
    SelfNode,
    /// Every append targets a different node: each spawns a detached child task
    /// under that node via `create_child_task`, inheriting its worktree.
    CrossNode,
    /// A mix of self and cross-node targets in one call — rejected, because self
    /// may block/suspend while cross-node returns immediately (incompatible
    /// result semantics).
    Mixed,
}

/// Decide routing from the caller's job and each append's resolved target job.
/// Pure so it is unit-testable in isolation. An empty batch is `SelfNode`
/// (the existing pipeline handles the no-op).
fn classify_task_route(caller_job_id: &str, target_job_ids: &[String]) -> TaskRoute {
    let mut any_self = false;
    let mut any_cross = false;
    for target in target_job_ids {
        if target == caller_job_id {
            any_self = true;
        } else {
            any_cross = true;
        }
    }
    match (any_self, any_cross) {
        (true, true) => TaskRoute::Mixed,
        (false, true) => TaskRoute::CrossNode,
        _ => TaskRoute::SelfNode,
    }
}

/// Run a validated tasks-append group: parse every item, resolve the caller and
/// each target node, then route to the self pipeline or the cross-node
/// `create_child_task` path (or reject a mixed batch).
async fn run_tasks_group(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    changes: &[ChangeItem],
    indices: &[usize],
) -> String {
    // Parse every item's payload and target coordinates up front.
    let mut tasks: Vec<TaskPayload> = Vec::with_capacity(indices.len());
    let mut coords: Vec<NodeTasksCoords> = Vec::with_capacity(indices.len());
    for &i in indices {
        let Some(payload) = changes[i].payload.clone() else {
            return "Task append requires payload with at least subagentType".to_string();
        };
        let task: TaskPayload = match serde_json::from_value(payload) {
            Ok(task) => task,
            Err(e) => return format!("Invalid task append payload: {e}"),
        };
        // Belt-and-suspenders: items only reach here after `blocking_append_kind`
        // already matched `NodeTasks`, so re-parsing the same target always yields
        // `Some`. Guard anyway rather than index into a wrong assumption.
        let Some(node_coords) = node_tasks_coords(&changes[i].target) else {
            return format!(
                "Task append target is not a node tasks collection: {}",
                changes[i].target
            );
        };
        tasks.push(task);
        coords.push(node_coords);
    }

    // Resolve the caller's node job and each append's addressed node job, then
    // decide routing. The CLI rewrites `cairn:~/tasks` to the caller's full node
    // URI before dispatch, so every item arrives as an explicit `NodeTasks` URI —
    // including self-targeted ones, which resolve back to `caller_job_id`.
    let routing = match resolve_task_routing(
        &orch.db.local,
        request.run_id.as_deref(),
        &request.cwd,
        &coords,
    )
    .await
    {
        Ok(routing) => routing,
        // Surface caller-resolution / "Node '…' not found" errors rather than
        // spawning under the caller.
        Err(e) => return e,
    };

    match routing.route {
        TaskRoute::Mixed => {
            "A tasks append cannot mix self-targeted and cross-node targets in one call".to_string()
        }
        TaskRoute::SelfNode => run_self_task_spawn(orch, request, &tasks).await,
        TaskRoute::CrossNode => {
            run_cross_node_task_spawn(orch, &tasks, &routing.target_job_ids, &coords).await
        }
    }
}

/// The resolved routing decision for a tasks-append batch: the caller's own node
/// job, the job each append addresses, and how the batch routes.
#[derive(Debug)]
struct TaskRouting {
    target_job_ids: Vec<String>,
    route: TaskRoute,
}

/// Resolve the caller's node job and each append's addressed node job from the
/// database, then classify the route. Surfaces a caller-resolution error or a
/// clear "Node '…' not found" error rather than silently falling back to the
/// caller. Separated from `run_tasks_group` so the URI→target-job resolution —
/// the core of the cross-node fix — is exercised against a real DB in tests.
async fn resolve_task_routing(
    db: &LocalDb,
    run_id: Option<&str>,
    cwd: &str,
    coords: &[NodeTasksCoords],
) -> Result<TaskRouting, String> {
    let caller_job_id = crate::execution::delegation::lookup_caller_job_id(db, run_id, cwd)
        .await
        .map_err(|e| format!("Failed to resolve caller node for task spawn: {e}"))?;
    let mut target_job_ids: Vec<String> = Vec::with_capacity(coords.len());
    for (project, number, exec_seq, node_id) in coords {
        let (_, job) = crate::resources::common::connect_and_find_node_job(
            db, project, *number, *exec_seq, node_id,
        )
        .await?;
        target_job_ids.push(job.id);
    }
    let route = classify_task_route(&caller_job_id, &target_job_ids);
    Ok(TaskRouting {
        target_job_ids,
        route,
    })
}

/// Spawn tasks under the caller's own node via the canonical delegated
/// task-packet pipeline (batching, inline wait, durable suspend, background).
async fn run_self_task_spawn(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    tasks: &[TaskPayload],
) -> String {
    let mut payloads = Vec::with_capacity(tasks.len());
    let mut background: Option<bool> = None;
    for task in tasks {
        let task_bg = task.run_in_background.unwrap_or(false);
        match background {
            Some(prev) if prev != task_bg => {
                return "All task appends in a single change call must share the same background value".to_string();
            }
            _ => background = Some(task_bg),
        }
        payloads.push(delegated_task_payload(task.clone()));
    }
    let group_id = uuid::Uuid::new_v4().to_string();
    // cairn-cli forwards no tool-use id on the callback, so correlate the
    // originating `write` tool-use id from the run transcript (the same
    // approach the preview→apply path uses). This is the id the frontend
    // links child jobs by; without it the live task windows can't resolve.
    let resolved_tool_use_id = match request.run_id.as_deref() {
        Some(run_id) => resolve_change_tool_use_id(orch, run_id, "/tasks").await,
        None => None,
    };
    let parent_tool_use_id = request
        .tool_use_id
        .as_deref()
        .or(resolved_tool_use_id.as_deref());
    let response = crate::execution::delegation::spawn_task_packets(
        orch,
        SpawnTaskPacketsInput {
            run_id: request.run_id.as_deref(),
            cwd: &request.cwd,
            payloads: &payloads,
            group_id: &group_id,
            parent_tool_use_id,
            background: background.unwrap_or(false),
        },
    )
    .await;
    response.result
}

/// Spawn each task as a detached child under its addressed node via
/// `create_child_task`, inheriting that node's worktree (the rescue/injection
/// path). Cross-node spawns never suspend the caller, so they return
/// immediately regardless of any `background` flag.
async fn run_cross_node_task_spawn(
    orch: &Orchestrator,
    tasks: &[TaskPayload],
    target_job_ids: &[String],
    coords: &[NodeTasksCoords],
) -> String {
    let mut lines = Vec::with_capacity(tasks.len());
    let mut ignored_fork = false;
    for (idx, task) in tasks.iter().enumerate() {
        let target_job_id = &target_job_ids[idx];
        let (project, number, exec_seq, node_id) = &coords[idx];
        // Cross-node always starts a fresh session; forking another node's
        // session is not meaningful for a rescue.
        if matches!(task.session, Some(TaskSessionMode::Fork)) {
            ignored_fork = true;
        }
        let input = CreateChildTaskInput {
            parent_job_id: target_job_id.clone(),
            description: task.description.clone(),
            prompt: task.prompt.clone(),
            subagent_type: task.subagent_type.clone(),
            tier: task.tier.clone(),
            backend_preference: task.backend_preference.clone(),
        };
        match crate::execution::jobs::create_child_task(orch, input) {
            Ok(result) => {
                let uri =
                    cross_node_task_uri(orch, project, *number, *exec_seq, node_id, &result.job_id)
                        .await;
                lines.push(format!("- {} ({})", uri, task.description));
            }
            // Surface clear errors (e.g. target node has no worktree) rather than
            // silently spawning under the caller.
            Err(e) => {
                return format!(
                    "Failed to spawn cross-node task '{}': {e}",
                    task.description
                )
            }
        }
    }
    let mut result = format!(
        "Spawned {} cross-node task(s) under the addressed node(s); each runs in that node's worktree and is detached (the caller is not suspended). Results will appear at:\n{}",
        tasks.len(),
        lines.join("\n")
    );
    if ignored_fork {
        result.push_str(
            "\n\nNote: `session: fork` was ignored — cross-node tasks always start a fresh session.",
        );
    }
    result
}

/// Build the canonical task URI (`.../{node}/task/{segment}`) for a freshly
/// created cross-node child job, from the target node coordinates and the
/// child's allocated `uri_segment`.
async fn cross_node_task_uri(
    orch: &Orchestrator,
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    job_id: &str,
) -> String {
    let node_uri = build_node_uri(project, number, exec_seq, node_id);
    let segment = job_uri_segment(orch, job_id)
        .await
        .unwrap_or_else(|| job_id.to_string());
    format!("{}/task/{}", node_uri, segment)
}

/// Read a job's allocated `uri_segment`, if any.
async fn job_uri_segment(orch: &Orchestrator, job_id: &str) -> Option<String> {
    let job_id = job_id.to_string();
    orch.db
        .local
        .query_opt_text(
            "SELECT uri_segment FROM jobs WHERE id = ?1",
            turso::params![job_id.as_str()],
        )
        .await
        .ok()
        .flatten()
}

/// Pure: given recent assistant-event `data` blobs (newest first), return the id
/// of the `write` (or legacy `change`) tool-use whose input appends to a matching
/// collection.
fn find_change_tool_id(event_data: &[String], collection_suffix: &str) -> Option<String> {
    for data in event_data {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(data) else {
            continue;
        };
        let Some(tools) = value.get("toolUses").and_then(|v| v.as_array()) else {
            continue;
        };
        for tool in tools {
            let is_change = tool
                .get("name")
                .and_then(|v| v.as_str())
                .map(|n| {
                    n == "write"
                        || n.ends_with("__write")
                        || n == "change"
                        || n.ends_with("__change")
                })
                .unwrap_or(false);
            if !is_change {
                continue;
            }
            let appends_collection = tool
                .get("input")
                .and_then(|i| i.get("changes"))
                .and_then(|c| c.as_array())
                .map(|changes| {
                    changes.iter().any(|item| {
                        let target = item.get("target").and_then(|t| t.as_str()).unwrap_or("");
                        let mode = item.get("mode").and_then(|m| m.as_str()).unwrap_or("");
                        let path = target.split('?').next().unwrap_or(target);
                        mode == "append" && path.ends_with(collection_suffix)
                    })
                })
                .unwrap_or(false);
            if appends_collection {
                if let Some(id) = tool
                    .get("id")
                    .or_else(|| tool.get("toolUseId"))
                    .and_then(|v| v.as_str())
                {
                    return Some(id.to_string());
                }
            }
        }
    }
    None
}

/// Correlate the originating `write` tool-use id from the run's transcript.
/// Briefly retries because the assistant event carrying the tool call may not be
/// persisted at the instant the MCP callback fires.
async fn resolve_change_tool_use_id(
    orch: &Orchestrator,
    run_id: &str,
    collection_suffix: &str,
) -> Option<String> {
    for _ in 0..20 {
        let run_id = run_id.to_string();
        let rows: Vec<String> = orch
            .db
            .local
            .read(|conn| {
                let run_id = run_id.clone();
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT data FROM events
                             WHERE run_id = ?1 AND event_type = 'assistant'
                             ORDER BY sequence DESC LIMIT 8",
                            (run_id.as_str(),),
                        )
                        .await?;
                    let mut out = Vec::new();
                    while let Some(row) = rows.next().await? {
                        out.push(row.text(0)?);
                    }
                    Ok(out)
                })
            })
            .await
            .unwrap_or_default();
        if let Some(id) = find_change_tool_id(&rows, collection_suffix) {
            return Some(id);
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    None
}

fn delegated_task_payload(task: TaskPayload) -> DelegatedTaskPayload {
    let session = match task.session.unwrap_or(TaskSessionMode::New) {
        TaskSessionMode::New => DelegatedTaskSessionMode::New,
        TaskSessionMode::Fork => DelegatedTaskSessionMode::Fork,
    };
    DelegatedTaskPayload {
        description: task.description,
        prompt: task.prompt,
        subagent_type: task.subagent_type,
        tier: task.tier,
        backend_preference: task.backend_preference,
        session,
        task_index: task.task_index,
    }
}

#[cfg(test)]
mod blocking_group_tests {
    use super::*;

    fn append(target: &str) -> ChangeItem {
        ChangeItem {
            target: target.to_string(),
            mode: ChangeMode::Append,
            payload: None,
        }
    }

    #[test]
    fn classifies_node_tasks_and_questions_appends() {
        assert_eq!(
            blocking_append_kind(&append("cairn://p/CAIRN/1/1/builder/tasks")),
            Some(BlockingKind::Tasks)
        );
        assert_eq!(
            blocking_append_kind(&append("cairn://p/CAIRN/1/1/builder/questions")),
            Some(BlockingKind::Questions)
        );
        // Non-collection or non-append targets are not blocking.
        assert_eq!(
            blocking_append_kind(&append("cairn://p/CAIRN/1/messages")),
            None
        );
        let mut create = append("cairn://p/CAIRN/1/1/builder/tasks");
        create.mode = ChangeMode::Create;
        assert_eq!(blocking_append_kind(&create), None);
    }

    #[test]
    fn validates_group_combinations() {
        let tasks = vec![
            append("cairn://p/CAIRN/1/1/builder/tasks"),
            append("cairn://p/CAIRN/1/1/builder/tasks"),
        ];
        assert_eq!(
            validate_blocking_group(&tasks, &[0, 1]).unwrap(),
            Some(BlockingKind::Tasks)
        );

        let question = vec![append("cairn://p/CAIRN/1/1/builder/questions")];
        assert_eq!(
            validate_blocking_group(&question, &[0]).unwrap(),
            Some(BlockingKind::Questions)
        );

        let mixed = vec![
            append("cairn://p/CAIRN/1/1/builder/tasks"),
            append("cairn://p/CAIRN/1/1/builder/questions"),
        ];
        assert!(validate_blocking_group(&mixed, &[0, 1]).is_err());

        let two_questions = vec![
            append("cairn://p/CAIRN/1/1/builder/questions"),
            append("cairn://p/CAIRN/1/1/builder/questions"),
        ];
        assert!(validate_blocking_group(&two_questions, &[0, 1]).is_err());

        assert_eq!(validate_blocking_group(&[], &[]).unwrap(), None);
    }

    fn task_payload(description: &str, prompt: &str) -> TaskPayload {
        TaskPayload {
            description: description.to_string(),
            prompt: prompt.to_string(),
            subagent_type: "Explore".to_string(),
            tier: None,
            backend_preference: None,
            run_in_background: None,
            session: None,
            task_index: None,
        }
    }

    #[test]
    fn delegated_payload_keeps_explicit_description() {
        let task = task_payload("Explicit title", "do the thing");
        assert_eq!(delegated_task_payload(task).description, "Explicit title");
    }

    #[test]
    fn classify_task_route_partitions_self_cross_and_mixed() {
        // All targets equal the caller -> self pipeline.
        assert_eq!(
            classify_task_route(
                "job-self",
                &["job-self".to_string(), "job-self".to_string()]
            ),
            TaskRoute::SelfNode
        );
        // All targets differ from the caller -> cross-node (even to distinct nodes).
        assert_eq!(
            classify_task_route("job-self", &["job-a".to_string(), "job-b".to_string()]),
            TaskRoute::CrossNode
        );
        // A mix of self and cross-node in one batch -> rejected.
        assert_eq!(
            classify_task_route("job-self", &["job-self".to_string(), "job-a".to_string()]),
            TaskRoute::Mixed
        );
        // An empty batch defers to the self pipeline (no-op).
        assert_eq!(classify_task_route("job-self", &[]), TaskRoute::SelfNode);
    }

    #[test]
    fn node_tasks_coords_extracts_addressed_node() {
        assert_eq!(
            node_tasks_coords("cairn://p/CAIRN/1295/1/builder/tasks"),
            Some(("CAIRN".to_string(), 1295, 1, "builder".to_string()))
        );
        // Query strings on the target do not break extraction.
        assert_eq!(
            node_tasks_coords("cairn://p/CAIRN/42/2/planner/tasks?limit=5"),
            Some(("CAIRN".to_string(), 42, 2, "planner".to_string()))
        );
        // Non-tasks targets yield no coordinates.
        assert_eq!(
            node_tasks_coords("cairn://p/CAIRN/1/1/builder/questions"),
            None
        );
        assert_eq!(node_tasks_coords("cairn://p/CAIRN/1/messages"), None);
    }

    /// Seed a minimal project/issue/execution with two sibling node jobs
    /// (`builder`, `coordinator`) and a live run on the coordinator (the caller),
    /// then assert that resolving a tasks URI addressed to *another* node yields
    /// that node's job — not the caller's. This is the behavioral pin for the
    /// cross-node fix: the spawn lands under the addressed node, so it inherits
    /// that node's worktree and lists in its tasks collection.
    #[tokio::test]
    async fn resolve_task_routing_addresses_target_node_not_caller() {
        use crate::storage::{MigrationRunner, TURSO_MIGRATIONS};

        async fn exec(db: &LocalDb, sql: &'static str) {
            db.write(|conn| {
                Box::pin(async move {
                    conn.execute(sql, ()).await?;
                    Ok(())
                })
            })
            .await
            .unwrap();
        }

        let temp = tempfile::tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("blocking-routing.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();

        exec(
            &db,
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('proj-1', 'default', 'Test', 'MCP', '/tmp/repo', 1, 1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
             VALUES ('issue-1', 'proj-1', 1, 'T', 'active', 1, 1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
             VALUES ('exec-1', 'recipe', 'issue-1', 'proj-1', 'running', 1, 1)",
        )
        .await;
        // Target node: carries a worktree the cross-node child would inherit.
        exec(
            &db,
            "INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, status, created_at, updated_at, uri_segment, worktree_path)
             VALUES ('job-builder', 'exec-1', 'issue-1', 'proj-1', 'Builder', 'running', 1, 1, 'builder', '/tmp/repo-builder')",
        )
        .await;
        // Caller node.
        exec(
            &db,
            "INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, status, created_at, updated_at, uri_segment, worktree_path)
             VALUES ('job-coord', 'exec-1', 'issue-1', 'proj-1', 'Coordinator', 'running', 1, 1, 'coordinator', '/tmp/repo-coord')",
        )
        .await;
        exec(
            &db,
            "INSERT INTO runs(id, job_id, issue_id, created_at, updated_at)
             VALUES ('run-coord', 'job-coord', 'issue-1', 1, 1)",
        )
        .await;

        let builder = node_tasks_coords("cairn://p/MCP/1/1/builder/tasks").unwrap();
        let coordinator = node_tasks_coords("cairn://p/MCP/1/1/coordinator/tasks").unwrap();

        // Cross-node: the builder URI, issued by the coordinator caller, resolves
        // to the BUILDER's job (the fix) and routes cross-node — i.e. it would
        // spawn under the builder, inheriting its worktree, not the caller's.
        let cross =
            resolve_task_routing(&db, Some("run-coord"), "", std::slice::from_ref(&builder))
                .await
                .unwrap();
        assert_eq!(cross.target_job_ids, vec!["job-builder".to_string()]);
        assert_eq!(cross.route, TaskRoute::CrossNode);

        // Self: the coordinator URI resolves back to the caller's own job, so the
        // existing delegated pipeline handles it unchanged.
        let self_route = resolve_task_routing(
            &db,
            Some("run-coord"),
            "",
            std::slice::from_ref(&coordinator),
        )
        .await
        .unwrap();
        assert_eq!(self_route.target_job_ids, vec!["job-coord".to_string()]);
        assert_eq!(self_route.route, TaskRoute::SelfNode);

        // Mixed self + cross-node in one batch is rejected.
        let mixed = resolve_task_routing(
            &db,
            Some("run-coord"),
            "",
            &[coordinator.clone(), builder.clone()],
        )
        .await
        .unwrap();
        assert_eq!(mixed.route, TaskRoute::Mixed);

        // A nonexistent target surfaces a clear error rather than a silent
        // fallback to the caller's node.
        let ghost = node_tasks_coords("cairn://p/MCP/1/1/ghost/tasks").unwrap();
        let err = resolve_task_routing(&db, Some("run-coord"), "", std::slice::from_ref(&ghost))
            .await
            .unwrap_err();
        assert!(err.contains("not found"), "unexpected error: {err}");
    }

    #[test]
    fn finds_task_change_tool_id_from_transcript() {
        // Newest-first: the in-flight change→tasks call is the latest assistant event.
        let newest = serde_json::json!({
            "toolUses": [{
                "id": "toolu_change_abc",
                "name": "mcp__cairn__change",
                "input": { "changes": [
                    { "target": "cairn:~/tasks", "mode": "append", "payload": { "subagentType": "Explore" } }
                ] }
            }]
        })
        .to_string();
        let older = serde_json::json!({
            "toolUses": [{ "id": "toolu_read", "name": "mcp__cairn__read", "input": {} }]
        })
        .to_string();
        assert_eq!(
            find_change_tool_id(&[newest, older], "/tasks"),
            Some("toolu_change_abc".to_string())
        );
    }

    #[test]
    fn finds_question_change_tool_id_from_transcript() {
        let newest = serde_json::json!({
            "toolUses": [{
                "toolUseId": "toolu_question_abc",
                "name": "change",
                "input": { "changes": [
                    { "target": "cairn:~/questions", "mode": "append", "payload": { "questions": [] } }
                ] }
            }]
        })
        .to_string();

        assert_eq!(
            find_change_tool_id(&[newest], "/questions"),
            Some("toolu_question_abc".to_string())
        );
    }

    #[test]
    fn ignores_non_task_change_calls() {
        // A file-only change must not be mistaken for the task spawn.
        let file_change = serde_json::json!({
            "toolUses": [{
                "id": "toolu_file",
                "name": "change",
                "input": { "changes": [
                    { "target": "file:src/lib.rs", "mode": "create", "content": "x" }
                ] }
            }]
        })
        .to_string();
        assert_eq!(find_change_tool_id(&[file_change], "/tasks"), None);
    }

    #[test]
    fn ignores_other_blocking_collections() {
        let question_change = serde_json::json!({
            "toolUses": [{
                "id": "toolu_question",
                "name": "change",
                "input": { "changes": [
                    { "target": "cairn:~/questions", "mode": "append", "payload": { "questions": [] } }
                ] }
            }]
        })
        .to_string();
        assert_eq!(find_change_tool_id(&[question_change], "/tasks"), None);
    }

    #[test]
    fn finds_task_tool_id_under_write_name() {
        // The current verb name is `write`; the matcher must recognize it just
        // like the legacy `change` name covered by the tests above.
        let newest = serde_json::json!({
            "toolUses": [{
                "toolUseId": "toolu_task_write",
                "name": "write",
                "input": { "changes": [
                    { "target": "cairn:~/tasks", "mode": "append", "payload": {} }
                ] }
            }]
        })
        .to_string();
        assert_eq!(
            find_change_tool_id(&[newest], "/tasks"),
            Some("toolu_task_write".to_string())
        );
    }
}
