use crate::execution::delegation::{
    DelegatedCallPayload, DelegatedTaskPayload, DelegatedTaskSessionMode, SpawnCallPacketsInput,
    SpawnTaskPacketsInput, TaskSpawnOutcome,
};
use crate::execution::jobs::{CallBranchPolicy, CreateChildTaskInput};
use crate::mcp::handlers::planning::QuestionsOutcome;
#[cfg(test)]
use crate::mcp::handlers::tool_use_correlation::claim_from_events;
use crate::mcp::handlers::tool_use_correlation::{claim_tool_use_id, Claim};
use crate::mcp::types::{
    AskUserPayload, CallBranchMode, CallPayload, ChangeItem, ChangeMode, McpCallbackRequest,
    TaskPayload, TaskSessionMode,
};
use crate::models::question::QuestionAnswer;
use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;
use cairn_common::protocol::CallbackResponse;
use cairn_uri::{build_node_uri, parse_uri, CairnResource};
use serde::{Deserialize, Serialize};

/// A blocking append routed to a node's tasks/questions collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BlockingKind {
    Tasks,
    Calls,
    Questions,
}

/// Classify a change item as a blocking append, if it is one. Only `mode=append`
/// on a node tasks/calls/questions collection qualifies.
///
/// A thread session addresses those collections beneath its own name, and the
/// resolver is what turns `<thread>/tasks` into `NodeTasks` — the same call read
/// and write dispatch make. Restating a subset of that mapping here is how a
/// thread's `calls` append came to miss this classification entirely while
/// `tasks` and `questions` were recognized.
pub(crate) fn blocking_append_kind(item: &ChangeItem) -> Option<BlockingKind> {
    if item.mode != ChangeMode::Append {
        return None;
    }
    let resource = cairn_uri::delegate_thread_descendant(parse_uri(&item.target)?).ok()?;
    match resource {
        CairnResource::NodeTasks { .. } => Some(BlockingKind::Tasks),
        CairnResource::NodeCalls { .. } => Some(BlockingKind::Calls),
        CairnResource::NodeQuestions { .. } => Some(BlockingKind::Questions),
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
    let mut calls = 0usize;
    let mut questions = 0usize;
    for &i in indices {
        match blocking_append_kind(&changes[i]) {
            Some(BlockingKind::Tasks) => tasks += 1,
            Some(BlockingKind::Calls) => calls += 1,
            Some(BlockingKind::Questions) => questions += 1,
            None => {}
        }
    }
    let categories = (tasks > 0) as u8 + (calls > 0) as u8 + (questions > 0) as u8;
    if categories > 1 {
        return Err(
            "Cannot mix task, call, and question appends in a single change call".to_string(),
        );
    }
    if questions > 1 {
        return Err("Only one questions append is supported per change call".to_string());
    }
    Ok(Some(if tasks > 0 {
        BlockingKind::Tasks
    } else if calls > 0 {
        BlockingKind::Calls
    } else {
        BlockingKind::Questions
    }))
}

/// The host-owned marker for output that is not an ordinary resource mutation.
/// Keep the body opaque: task output can itself contain change-report JSON.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct BlockingResult {
    pub(crate) text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) artifact_uri: Option<String>,
    /// A question's answers keyed by question index, beside the rendered
    /// `text`, so a caller reads each answer without parsing the rendering.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) answers: Option<Vec<QuestionAnswer>>,
}

impl From<String> for BlockingResult {
    fn from(text: String) -> Self {
        Self {
            text,
            artifact_uri: None,
            answers: None,
        }
    }
}

impl From<CallbackResponse> for BlockingResult {
    fn from(response: CallbackResponse) -> Self {
        Self {
            text: response.result,
            artifact_uri: response.artifact_uri,
            answers: None,
        }
    }
}

impl From<QuestionsOutcome> for BlockingResult {
    fn from(outcome: QuestionsOutcome) -> Self {
        Self {
            text: outcome.text,
            artifact_uri: outcome.uri,
            answers: outcome.answers,
        }
    }
}

/// A blocking append that created something the write report should list.
#[derive(Debug)]
pub(crate) struct BlockingApplied {
    pub(crate) index: usize,
    pub(crate) summary: String,
    pub(crate) data: serde_json::Value,
}

/// What a blocking group did, per change index.
///
/// The blocking text stays opaque payload. A task append that created a child
/// is an applied change and one that created nothing is a failure, so neither
/// has to be read out of that text.
#[derive(Debug, Default)]
pub(crate) struct BlockingGroupOutcome {
    pub(crate) blocking: Option<BlockingResult>,
    pub(crate) applied: Vec<BlockingApplied>,
    pub(crate) failures: Vec<(usize, String)>,
}

impl BlockingGroupOutcome {
    fn refuse_all(indices: &[usize], error: impl Into<String>) -> Self {
        let error = error.into();
        Self {
            failures: indices
                .iter()
                .map(|&index| (index, error.clone()))
                .collect(),
            ..Default::default()
        }
    }
}

impl From<BlockingResult> for BlockingGroupOutcome {
    fn from(blocking: BlockingResult) -> Self {
        Self {
            blocking: Some(blocking),
            ..Default::default()
        }
    }
}

impl From<String> for BlockingGroupOutcome {
    fn from(text: String) -> Self {
        BlockingResult::from(text).into()
    }
}

impl From<CallbackResponse> for BlockingGroupOutcome {
    fn from(response: CallbackResponse) -> Self {
        BlockingResult::from(response).into()
    }
}

/// Map a task spawn onto the change indices its payloads came from, one task
/// per index in order.
fn task_spawn_group_outcome(indices: &[usize], outcome: TaskSpawnOutcome) -> BlockingGroupOutcome {
    let applied = outcome
        .spawned
        .into_iter()
        .map(|task| {
            let address = task.task_uri.clone().unwrap_or_else(|| task.job_id.clone());
            let summary = match task.launch.waiting_note() {
                Some(note) => format!("Spawned task {address}, {note}"),
                None => format!("Spawned task {address}"),
            };
            BlockingApplied {
                index: indices[task.index],
                summary,
                data: serde_json::json!({
                    "taskUri": task.task_uri,
                    "jobId": task.job_id,
                    "description": task.description,
                    "launch": task.launch,
                }),
            }
        })
        .collect();
    BlockingGroupOutcome {
        blocking: outcome.response.map(BlockingResult::from),
        applied,
        failures: outcome
            .refused
            .into_iter()
            .map(|(index, error)| (indices[index], error))
            .collect(),
    }
}

/// Run the validated blocking group without discarding structured spawn metadata.
pub(crate) async fn run_blocking_group(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    changes: &[ChangeItem],
    indices: &[usize],
    kind: BlockingKind,
) -> BlockingGroupOutcome {
    match kind {
        BlockingKind::Tasks => run_tasks_group(orch, request, changes, indices).await,
        BlockingKind::Calls => run_calls_group(orch, request, changes, indices).await,
        BlockingKind::Questions => {
            let Some(payload) = changes[indices[0]].payload.clone() else {
                return "Question append requires payload with a questions array"
                    .to_string()
                    .into();
            };
            let mode = match QuestionAskMode::of(&payload) {
                Ok(mode) => mode,
                Err(refusal) => return refusal.into(),
            };
            let ask: AskUserPayload = match serde_json::from_value(payload) {
                Ok(ask) => ask,
                Err(e) => return format!("Invalid question append payload: {e}").into(),
            };
            let memo = match mode.ordinal {
                Some(ordinal) => match question_memo(orch, request, ordinal, &ask).await {
                    Ok(QuestionMemo::Replayed(uri)) => {
                        return BlockingResult {
                            text: format!(
                            "Question already asked at ordinal {ordinal}; its answer is at {uri}"
                        ),
                            artifact_uri: Some(uri),
                            answers: None,
                        }
                        .into()
                    }
                    Ok(QuestionMemo::Ask(key)) => Some(key),
                    Err(refusal) => return refusal.into(),
                },
                None => None,
            };
            let resolved_tool_use_id = match request.run_id.as_deref() {
                Some(run_id) => claim_change_tool_use_id(orch, run_id, "/questions").await,
                None => None,
            };
            let tool_use_id = request
                .tool_use_id
                .as_deref()
                .or(resolved_tool_use_id.as_deref());
            let outcome = crate::mcp::handlers::planning::ask_questions(
                orch,
                request,
                ask,
                !mode.blocking,
                tool_use_id,
            )
            .await;
            if let (Some(key), Some(uri)) = (memo, outcome.uri.as_deref()) {
                if let Err(error) = crate::workflow_journal::store_entry(
                    &orch.db.local,
                    &key.run_id,
                    key.ordinal,
                    &key.hash,
                    Some(uri),
                    crate::workflow_journal::JournalStatus::Success,
                )
                .await
                {
                    log::warn!("failed to journal question {uri}: {error}");
                }
            }
            BlockingResult::from(outcome).into()
        }
    }
}

/// How a questions append asks: blocking (the default) or not, and the
/// journal ordinal a workflow node keys a non-blocking ask by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct QuestionAskMode {
    blocking: bool,
    ordinal: Option<i64>,
}

impl QuestionAskMode {
    fn of(payload: &serde_json::Value) -> Result<Self, String> {
        if payload.get("background").is_some() {
            return Err(
                "A questions append is non-blocking with blocking: false, not background"
                    .to_string(),
            );
        }
        let blocking = match payload.get("blocking") {
            None | Some(serde_json::Value::Null) => true,
            Some(serde_json::Value::Bool(blocking)) => *blocking,
            Some(_) => return Err("payload.blocking is true or false".to_string()),
        };
        let ordinal = match payload.get("ordinal") {
            None | Some(serde_json::Value::Null) => None,
            Some(value) => Some(value.as_i64().filter(|n| *n >= 0).ok_or_else(|| {
                "payload.ordinal is a non-negative integer, the workflow's dispatch ordinal"
                    .to_string()
            })?),
        };
        if ordinal.is_some() && blocking {
            return Err(
                "payload.ordinal keys a non-blocking ask; set blocking: false with it".to_string(),
            );
        }
        Ok(Self { blocking, ordinal })
    }
}

/// The journal key a workflow's non-blocking ask is recorded under.
struct QuestionJournalKey {
    run_id: String,
    ordinal: i64,
    hash: String,
}

enum QuestionMemo {
    /// The same questions were asked at this ordinal before: their URI.
    Replayed(String),
    /// Not asked yet: ask, then journal under this key.
    Ask(QuestionJournalKey),
}

/// A workflow's ordinal-keyed ask, looked up in the journal its `agent()`
/// calls share, so a replayed script gets the question it already asked
/// (answered or not) instead of a second prompt. Only a workflow node keys
/// asks by ordinal.
async fn question_memo(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    ordinal: i64,
    ask: &AskUserPayload,
) -> Result<QuestionMemo, String> {
    let run_id = request
        .run_id
        .clone()
        .ok_or("payload.ordinal needs an authenticated workflow run")?;
    let db = crate::execution::routing::routing_db_for_id(&orch.db, &run_id)
        .await
        .map_err(|error| error.to_string())?;
    let agent = {
        let run_id = run_id.clone();
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT j.agent_config_id FROM jobs j JOIN runs r ON r.job_id = j.id WHERE r.id = ?1",
                        (run_id.as_str(),),
                    )
                    .await?;
                Ok(match rows.next().await? {
                    Some(row) => crate::storage::RowExt::opt_text(&row, 0)?,
                    None => None,
                })
            })
        })
        .await
        .map_err(|error| error.to_string())?
    };
    if agent.as_deref() != Some("workflow") {
        return Err(
            "payload.ordinal is a workflow's dispatch ordinal; only a workflow node keys its asks by one"
                .to_string(),
        );
    }
    let questions = serde_json::to_string(&ask.questions).map_err(|error| error.to_string())?;
    let hash = crate::workflow_journal::hash_prompt(&format!("questions:{questions}"));
    if let Ok(Some(entry)) =
        crate::workflow_journal::get_entry(&orch.db.local, &run_id, ordinal).await
    {
        if entry.prompt_hash == hash {
            if let Some(uri) = entry.result_json {
                return Ok(QuestionMemo::Replayed(uri));
            }
        }
    }
    Ok(QuestionMemo::Ask(QuestionJournalKey {
        run_id,
        ordinal,
        hash,
    }))
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
        Some(CairnResource::Thread {
            project,
            name,
            path,
        }) if path == ["tasks"] => Some((project, 0, 0, name)),
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
    /// under that node via `create_child_task`, inheriting its branch coordinate.
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
///
/// Anything that stops the group before a child exists is a failure of every
/// task append in it, never blocking text.
async fn run_tasks_group(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    changes: &[ChangeItem],
    indices: &[usize],
) -> BlockingGroupOutcome {
    // Parse every item's payload and target coordinates up front.
    let mut tasks: Vec<TaskPayload> = Vec::with_capacity(indices.len());
    let mut coords: Vec<NodeTasksCoords> = Vec::with_capacity(indices.len());
    for &i in indices {
        let Some(payload) = changes[i].payload.clone() else {
            return BlockingGroupOutcome::refuse_all(
                indices,
                "Task append requires payload with at least subagentType",
            );
        };
        if let Some(problem) = crate::mcp::types::model_keys_problem(&payload) {
            return BlockingGroupOutcome::refuse_all(
                indices,
                format!("Invalid task append payload: {problem}"),
            );
        }
        let task: TaskPayload = match serde_json::from_value(payload) {
            Ok(task) => task,
            Err(e) => {
                return BlockingGroupOutcome::refuse_all(
                    indices,
                    format!("Invalid task append payload: {e}"),
                )
            }
        };
        // Belt-and-suspenders: items only reach here after `blocking_append_kind`
        // already matched `NodeTasks`, so re-parsing the same target always yields
        // `Some`. Guard anyway rather than index into a wrong assumption.
        let Some(node_coords) = node_tasks_coords(&changes[i].target) else {
            return BlockingGroupOutcome::refuse_all(
                indices,
                format!(
                    "Task append target is not a node tasks collection: {}",
                    changes[i].target
                ),
            );
        };
        tasks.push(task);
        coords.push(node_coords);
    }

    // Resolve the caller's node job and each append's addressed node job, then
    // decide routing. The write handler resolves `cairn:~/tasks` from the
    // authenticated run before blocking classification, so every item arrives as
    // an explicit tasks URI, including self-targeted ones that resolve back to
    // `caller_job_id`.
    // A team run's job/run rows live in its replica (CAIRN-2182): resolve the
    // caller's owning DB by run id so `lookup_caller_job_id` and the target-node
    // lookups read the database the rows actually live in.
    let Some(run_id) = request.run_id.as_deref() else {
        return BlockingGroupOutcome::refuse_all(
            indices,
            "Failed to resolve caller node for task spawn: authenticated agent request is missing its run ID",
        );
    };
    let routing_db = match crate::execution::routing::routing_db_for_id(&orch.db, run_id).await {
        Ok(db) => db,
        Err(e) => {
            return BlockingGroupOutcome::refuse_all(
                indices,
                format!("Failed to resolve caller node for task spawn: {e}"),
            )
        }
    };
    let routing = match resolve_task_routing(&routing_db, run_id, &coords).await {
        Ok(routing) => routing,
        // Surface caller-resolution / "Node '…' not found" errors rather than
        // spawning under the caller.
        Err(e) => return BlockingGroupOutcome::refuse_all(indices, e),
    };

    match routing.route {
        TaskRoute::Mixed => BlockingGroupOutcome::refuse_all(
            indices,
            "A tasks append cannot mix self-targeted and cross-node targets in one call",
        ),
        TaskRoute::SelfNode => run_self_task_spawn(orch, request, &tasks, indices).await,
        TaskRoute::CrossNode => {
            run_cross_node_task_spawn(orch, &tasks, &routing.target_job_ids, &coords)
                .await
                .into()
        }
    }
}

/// Run a validated calls-append group (CAIRN-2481). Calls are always self-spawned
/// under the caller (never cross-node), so this parses each item, enforces a
/// shared background disposition, and hands the batch to `spawn_call_packets` —
/// the caller is resolved from its authenticated run ID, not the addressed node or process cwd.
async fn run_calls_group(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    changes: &[ChangeItem],
    indices: &[usize],
) -> BlockingGroupOutcome {
    let mut calls: Vec<DelegatedCallPayload> = Vec::with_capacity(indices.len());
    let mut background: Option<bool> = None;
    for &i in indices {
        let Some(payload) = changes[i].payload.clone() else {
            return "Call append requires payload with at least a prompt"
                .to_string()
                .into();
        };
        if let Some(problem) = crate::mcp::types::model_keys_problem(&payload) {
            return format!("Invalid call append payload: {problem}").into();
        }
        let call: CallPayload = match serde_json::from_value(payload) {
            Ok(call) => call,
            Err(e) => return format!("Invalid call append payload: {e}").into(),
        };
        let call_bg = call.run_in_background.unwrap_or(false);
        match background {
            Some(prev) if prev != call_bg => {
                return "All call appends in a single change call must share the same background value".to_string().into();
            }
            _ => background = Some(call_bg),
        }
        calls.push(delegated_call_payload(call));
    }

    let resolved_tool_use_id = match request.run_id.as_deref() {
        Some(run_id) => claim_change_tool_use_id(orch, run_id, "/calls").await,
        None => None,
    };
    let parent_tool_use_id = request
        .tool_use_id
        .as_deref()
        .or(resolved_tool_use_id.as_deref());
    let response = crate::execution::delegation::spawn_call_packets(
        orch,
        SpawnCallPacketsInput {
            run_id: request.run_id.as_deref(),
            payloads: &calls,
            parent_tool_use_id,
            background: background.unwrap_or(false),
        },
    )
    .await;
    response.into()
}

/// Convert a wire `CallPayload` into the resolved `DelegatedCallPayload`,
/// applying the `Explore` default worker and deriving a display title from the
/// description, label, or a prompt prefix.
fn delegated_call_payload(call: CallPayload) -> DelegatedCallPayload {
    let branch_policy = match call.branch.unwrap_or_default() {
        CallBranchMode::Inherit => CallBranchPolicy::Inherit,
        CallBranchMode::None => CallBranchPolicy::None,
    };
    let description = call
        .description
        .filter(|d| !d.is_empty())
        .or_else(|| call.label.clone())
        .unwrap_or_else(|| {
            let prefix: String = call.prompt.trim().chars().take(60).collect();
            if prefix.is_empty() {
                "call".to_string()
            } else {
                prefix
            }
        });
    DelegatedCallPayload {
        description,
        prompt: call.prompt,
        subagent_type: call
            .subagent_type
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "Explore".to_string()),
        models: call.models,
        output_schema: call.output_schema,
        branch_policy,
        label: call.label,
        phase: call.phase,
        task_index: call.task_index,
        ordinal: call.ordinal,
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
    run_id: &str,
    coords: &[NodeTasksCoords],
) -> Result<TaskRouting, String> {
    let caller_job_id = crate::execution::delegation::lookup_caller_job_id(db, run_id)
        .await
        .map_err(|e| format!("Failed to resolve caller node for task spawn: {e}"))?;
    let mut target_job_ids: Vec<String> = Vec::with_capacity(coords.len());
    for (project, number, exec_seq, node_id) in coords {
        let job_id = crate::resources::resolve_node_or_task_job_id(
            db, project, *number, *exec_seq, node_id, None,
        )
        .await?;
        target_job_ids.push(job_id);
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
    indices: &[usize],
) -> BlockingGroupOutcome {
    let mut payloads = Vec::with_capacity(tasks.len());
    let mut background: Option<bool> = None;
    for task in tasks {
        let task_bg = task.run_in_background.unwrap_or(false);
        match background {
            Some(prev) if prev != task_bg => {
                return BlockingGroupOutcome::refuse_all(
                    indices,
                    "All task appends in a single change call must share the same background value",
                );
            }
            _ => background = Some(task_bg),
        }
        payloads.push(delegated_task_payload(task.clone()));
    }
    // cairn-cmd forwards no tool-use id on the callback, so correlate the
    // originating `write` tool-use id from the run transcript (the same
    // approach the preview→apply path uses). This is the id the frontend
    // links child jobs by; without it the live task windows can't resolve.
    let resolved_tool_use_id = match request.run_id.as_deref() {
        Some(run_id) => claim_change_tool_use_id(orch, run_id, "/tasks").await,
        None => None,
    };
    let parent_tool_use_id = request
        .tool_use_id
        .as_deref()
        .or(resolved_tool_use_id.as_deref());
    let outcome = crate::execution::delegation::spawn_task_packets(
        orch,
        SpawnTaskPacketsInput {
            run_id: request.run_id.as_deref(),
            cwd: &request.cwd,
            payloads: &payloads,
            parent_tool_use_id,
            background: background.unwrap_or(false),
        },
    )
    .await;
    task_spawn_group_outcome(indices, outcome)
}

/// Spawn each task as a detached child under its addressed node via
/// `create_child_task`, inheriting that node's branch coordinate (the rescue/injection
/// path). Cross-node spawns never suspend the caller, so they return
/// immediately regardless of any `background` flag.
/// Cross-node (rescue) task jobs are bare child jobs with no recipe node, so the
/// artifact-write handler has no per-task schema to resolve and validates their
/// return against the fixed `return` contract. Rather than silently drop a
/// caller's `outputSchema` (or worse, tell the child to produce a shape whose
/// write is then rejected against `return`), reject the append with a clear
/// error. Per-task output schemas are honored only on the self route, which
/// materializes the task as a recipe node carrying the schema.
fn reject_cross_node_output_schema(tasks: &[TaskPayload]) -> Option<String> {
    tasks.iter().find(|t| t.output_schema.is_some()).map(|t| {
        format!(
            "outputSchema is not supported on cross-node task appends: the target node's rescue worker returns freeform output. Omit outputSchema, or spawn under your own node (cairn:~/tasks), where a per-task schema is enforced. (task: '{}')",
            t.description
        )
    })
}

async fn run_cross_node_task_spawn(
    orch: &Orchestrator,
    tasks: &[TaskPayload],
    target_job_ids: &[String],
    coords: &[NodeTasksCoords],
) -> String {
    // Reject up front so a caller who attached a schema learns their contract
    // cannot be honored here, instead of receiving a silent freeform result.
    if let Some(err) = reject_cross_node_output_schema(tasks) {
        return err;
    }

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
            models: task.models.clone(),
        };
        match crate::execution::jobs::create_child_task(orch, input) {
            Ok(result) => {
                let uri =
                    cross_node_task_uri(orch, project, *number, *exec_seq, node_id, &result.job_id)
                        .await;
                lines.push(format!("- {} ({})", uri, task.description));
            }
            // Surface clear errors (e.g. target node has no branch coordinate) rather than
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
        "Spawned {} cross-node task(s) under the addressed node(s); each works on that node's branch and is detached (the caller is not suspended). Results will appear at:\n{}",
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
        .query_opt_text("SELECT uri_segment FROM jobs WHERE id = ?1", (job_id,))
        .await
        .ok()
        .flatten()
}

/// Pure: given recent assistant-event `data` blobs (newest first), return the id
/// of the `write` (or legacy `change`) tool-use whose input appends to a matching
/// collection.
fn change_matches_collection(
    name: &str,
    input: &serde_json::Value,
    collection_suffix: &str,
) -> bool {
    (name == "write" || name.ends_with("__write") || name == "change" || name.ends_with("__change"))
        && input
            .get("changes")
            .and_then(|c| c.as_array())
            .map(|changes| {
                changes.iter().any(|item| {
                    let target = item.get("target").and_then(|t| t.as_str()).unwrap_or("");
                    let mode = item.get("mode").and_then(|m| m.as_str()).unwrap_or("");
                    let path = target.split('?').next().unwrap_or(target);
                    mode == "append" && path.ends_with(collection_suffix)
                })
            })
            .unwrap_or(false)
}

/// The matcher above applied to raw assistant-event blobs, so the shape rules
/// can be pinned without a database behind them.
#[cfg(test)]
fn claim_change_tool_id(event_data: &[String], collection_suffix: &str) -> Claim {
    claim_from_events(
        event_data,
        &std::collections::HashSet::new(),
        |name, input| change_matches_collection(name, input, collection_suffix),
    )
}

/// Claim the originating `write` tool-use id from the run's transcript. Briefly
/// retries because the assistant event carrying the tool call may not be
/// persisted at the instant the MCP callback fires.
///
/// This is the id the frontend links spawned child jobs by and the id a blocking
/// append's result is later attributed to, so it must name THIS call rather than
/// the newest one that looks like it. Two `write` calls in one assistant event
/// appending to the same collection are indistinguishable at this boundary;
/// taking the newest would hang one call's live task window off the other's
/// children, so a tie is refused instead (CAIRN-3232).
///
/// Refusing costs that append its parent linkage — a visible degradation, and a
/// much smaller one than attributing work to the wrong call. Every spawn path
/// already handles `None`.
///
/// It is scoped to the CURRENT TURN, where the earlier recency lookup searched
/// the whole run. A `write` recorded by an earlier turn is never the call this
/// callback came from, and including it only widened what could be mistaken for
/// one.
async fn claim_change_tool_use_id(
    orch: &Orchestrator,
    run_id: &str,
    collection_suffix: &str,
) -> Option<String> {
    let turn_id = orch.process_state.get_current_turn_id(run_id)?;
    match claim_tool_use_id(&orch.db.local, run_id, &turn_id, |name, input| {
        change_matches_collection(name, input, collection_suffix)
    })
    .await
    {
        Claim::One(id) => Some(id),
        Claim::None => None,
        Claim::Ambiguous(count) => {
            log::warn!(
                "a blocking {collection_suffix} append on run {run_id} matches {count} indistinguishable open write calls, so it cannot claim one without risking another call's linkage"
            );
            None
        }
    }
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
        models: task.models,
        session,
        task_index: task.task_index,
        output_schema: task.output_schema,
    }
}

#[cfg(test)]
mod non_blocking_question_tests {
    use super::*;
    use crate::db::DbState;
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{LocalDb, RowExt, SearchIndex};
    use std::sync::Arc;

    async fn orch_with_db() -> (Orchestrator, Arc<LocalDb>) {
        let local = Arc::new(
            LocalDb::open_migrated_for_test(tempfile::tempdir().unwrap().keep().join("ask.db"))
                .await
                .unwrap(),
        );
        let search =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        let db = Arc::new(DbState::new(local.clone(), search));
        let orch = OrchestratorBuilder::new(
            db,
            Arc::new(TestServicesBuilder::new().build()),
            tempfile::tempdir().unwrap().keep(),
        )
        .build();
        (orch, local)
    }

    /// A workflow node (`flow`, run `wf-run`) and an ordinary agent node
    /// (`agent`, run `ag-run`) on issue 7.
    async fn seed(db: &LocalDb) {
        for sql in [
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w','W',1,1)",
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p','w','P','prj','/tmp/p',1,1)",
            "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at) VALUES ('i','p',7,'T','active',1,1)",
            "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq) VALUES ('e','recipe','i','p','running',1,1)",
            "INSERT INTO jobs (id, execution_id, issue_id, project_id, status, uri_segment, node_name, agent_config_id, created_at, updated_at) VALUES ('j-wf','e','i','p','running','flow','Flow','workflow',1,1)",
            "INSERT INTO jobs (id, execution_id, issue_id, project_id, status, uri_segment, node_name, agent_config_id, created_at, updated_at) VALUES ('j-ag','e','i','p','running','agent','Agent','build',1,1)",
            "INSERT INTO sessions (id, job_id, created_at, updated_at) VALUES ('sess','j-wf',1,1)",
            "INSERT INTO runs (id, job_id, issue_id, status, created_at, updated_at) VALUES ('wf-run','j-wf','i','live',1,1)",
            "INSERT INTO runs (id, job_id, issue_id, status, created_at, updated_at) VALUES ('ag-run','j-ag','i','live',1,1)",
        ] {
            db.execute(sql, ()).await.unwrap();
        }
    }

    async fn ask(
        orch: &Orchestrator,
        run: &str,
        node: &str,
        payload: serde_json::Value,
    ) -> serde_json::Value {
        let request = McpCallbackRequest {
            answer_bound_ms: None,
            thread_id: None,
            cwd: "/tmp/p".to_string(),
            run_id: Some(run.to_string()),
            tool: "write".to_string(),
            tool_use_id: None,
            payload: serde_json::json!({
                "changes": [{
                    "target": format!("cairn://p/prj/7/1/{node}/questions"),
                    "mode": "append",
                    "payload": payload,
                }]
            }),
            origin: None,
        };
        let result = crate::mcp::handlers::write::handle_write(orch, &request).await;
        serde_json::from_str(&result).unwrap()
    }

    fn questions() -> serde_json::Value {
        serde_json::json!([{
            "question": "Quantization?",
            "header": "Quant",
            "options": [
                {"label": "q8_0", "description": "fits", "recommended": true},
                {"label": "bf16", "description": "does not fit"}
            ]
        }])
    }

    async fn prompt_count(db: &LocalDb) -> i64 {
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn.query("SELECT COUNT(*) FROM prompts", ()).await?;
                Ok(rows
                    .next()
                    .await?
                    .map(|r| r.i64(0))
                    .transpose()?
                    .unwrap_or(0))
            })
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn a_workflow_asks_once_per_ordinal_and_waits_on_the_answer() {
        let (orch, db) = orch_with_db().await;
        seed(&db).await;
        let payload =
            serde_json::json!({"questions": questions(), "blocking": false, "ordinal": 3});
        let first = ask(&orch, "wf-run", "flow", payload.clone()).await;
        let uri = first["blocking"]["artifact_uri"]
            .as_str()
            .expect("the question's URI")
            .to_string();
        assert_eq!(uri, "cairn://p/prj/7/1/flow/questions/q-1");
        assert_eq!(prompt_count(&db).await, 1);

        let replayed = ask(&orch, "wf-run", "flow", payload).await;
        assert_eq!(
            replayed["blocking"]["artifact_uri"].as_str(),
            Some(uri.as_str())
        );
        assert_eq!(
            prompt_count(&db).await,
            1,
            "a replay asks no second question"
        );

        crate::mcp::handlers::planning::answer_node_question(
            &orch,
            "prj",
            7,
            1,
            "flow",
            "q-1",
            &serde_json::json!({"answers": [{"index": 0, "selection": "q8_0"}]}),
        )
        .await
        .unwrap();
        let waited: serde_json::Value = serde_json::from_str(
            &crate::resources::node::wait_for_question(
                &orch, &db, "prj", 7, 1, "flow", "q-1", None,
            )
            .await,
        )
        .unwrap();
        assert_eq!(waited["state"], "answered");
        assert_eq!(waited["answers"][0]["answer"], "q8_0");

        // A changed question at a new ordinal is a new prompt; withdrawn, the
        // wait says so rather than waiting on.
        let second = ask(
            &orch,
            "wf-run",
            "flow",
            serde_json::json!({"questions": questions(), "blocking": false, "ordinal": 4}),
        )
        .await;
        assert_eq!(
            second["blocking"]["artifact_uri"].as_str(),
            Some("cairn://p/prj/7/1/flow/questions/q-2"),
            "the same form at another ordinal is a second question"
        );
        // The memo is keyed by ordinal: a replay of each ask returns its own
        // question, never the other's.
        for (ordinal, own) in [(3, "q-1"), (4, "q-2")] {
            let replay = ask(
                &orch,
                "wf-run",
                "flow",
                serde_json::json!({"questions": questions(), "blocking": false, "ordinal": ordinal}),
            )
            .await;
            assert_eq!(
                replay["blocking"]["artifact_uri"].as_str(),
                Some(format!("cairn://p/prj/7/1/flow/questions/{own}").as_str())
            );
        }
        assert_eq!(prompt_count(&db).await, 2);
        // A different form at an asked ordinal is a divergence: it is asked
        // afresh, never answered from the memo.
        let diverged = ask(
            &orch,
            "wf-run",
            "flow",
            serde_json::json!({"questions": [{"question": "Something else?", "header": null, "options": []}], "blocking": false, "ordinal": 3}),
        )
        .await;
        assert_eq!(
            diverged["blocking"]["artifact_uri"].as_str(),
            Some("cairn://p/prj/7/1/flow/questions/q-3")
        );
        db.execute(
            "UPDATE prompts SET withdrawn_at = 9, withdrawn_cause = 'operator_stop' WHERE uri_segment = 'q-2'",
            (),
        )
        .await
        .unwrap();
        let waited: serde_json::Value = serde_json::from_str(
            &crate::resources::node::wait_for_question(
                &orch, &db, "prj", 7, 1, "flow", "q-2", None,
            )
            .await,
        )
        .unwrap();
        assert_eq!(
            waited,
            serde_json::json!({"state": "withdrawn", "cause": "operator_stop"})
        );
    }

    #[tokio::test]
    async fn the_non_blocking_ask_is_spelled_blocking_false_and_only_a_workflow_keys_it_by_ordinal()
    {
        let (orch, db) = orch_with_db().await;
        seed(&db).await;
        let refused = |report: &serde_json::Value| report.to_string();
        let background = ask(
            &orch,
            "wf-run",
            "flow",
            serde_json::json!({"questions": questions(), "background": true}),
        )
        .await;
        assert!(
            refused(&background).contains("blocking: false, not background"),
            "{background}"
        );
        let blocking_ordinal = ask(
            &orch,
            "wf-run",
            "flow",
            serde_json::json!({"questions": questions(), "ordinal": 0}),
        )
        .await;
        assert!(
            refused(&blocking_ordinal).contains("set blocking: false with it"),
            "{blocking_ordinal}"
        );
        let agent_ordinal = ask(
            &orch,
            "ag-run",
            "agent",
            serde_json::json!({"questions": questions(), "blocking": false, "ordinal": 0}),
        )
        .await;
        assert!(
            refused(&agent_ordinal).contains("only a workflow node keys its asks by one"),
            "{agent_ordinal}"
        );
        let two_recommended = ask(
            &orch,
            "ag-run",
            "agent",
            serde_json::json!({"blocking": false, "questions": [{
                "question": "Engine?", "header": null,
                "options": [
                    {"label": "vLLM", "description": "", "recommended": true},
                    {"label": "llama.cpp", "description": "", "recommended": true}
                ]
            }]}),
        )
        .await;
        assert!(
            refused(&two_recommended).contains("recommends 2 options"),
            "{two_recommended}"
        );
        assert_eq!(prompt_count(&db).await, 0);
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

    /// Every blocking collection classifies the same from a node's address and
    /// from a thread's.
    ///
    /// Driven by one list per kind rather than a case per address. The thread
    /// arms used to be written out by hand beside the node ones, and only
    /// `tasks` and `questions` ever got one — a thread session's `calls` append
    /// was silently not a blocking append, so it would have been applied as an
    /// ordinary write and never suspended the caller. A hand-written pair list
    /// cannot fail when half of it is missing; this one can.
    #[test]
    fn classifies_blocking_appends_at_node_and_thread_addresses() {
        for (segment, kind) in [
            ("tasks", BlockingKind::Tasks),
            ("calls", BlockingKind::Calls),
            ("questions", BlockingKind::Questions),
        ] {
            for prefix in [
                "cairn://p/cairn/1/1/builder",
                "cairn://p/cairn/design-review",
            ] {
                let target = format!("{prefix}/{segment}");
                assert_eq!(
                    blocking_append_kind(&append(&target)),
                    Some(kind),
                    "{target} must be a blocking {kind:?} append"
                );
            }
        }
        // Non-collection or non-append targets are not blocking.
        assert_eq!(
            blocking_append_kind(&append("cairn://p/cairn/1/messages")),
            None
        );
        assert_eq!(
            blocking_append_kind(&append("cairn://p/cairn/design-review/messages")),
            None
        );
        let mut create = append("cairn://p/cairn/1/1/builder/tasks");
        create.mode = ChangeMode::Create;
        assert_eq!(blocking_append_kind(&create), None);
    }

    #[test]
    fn validates_group_combinations() {
        let tasks = vec![
            append("cairn://p/cairn/1/1/builder/tasks"),
            append("cairn://p/cairn/1/1/builder/tasks"),
        ];
        assert_eq!(
            validate_blocking_group(&tasks, &[0, 1]).unwrap(),
            Some(BlockingKind::Tasks)
        );

        let question = vec![append("cairn://p/cairn/1/1/builder/questions")];
        assert_eq!(
            validate_blocking_group(&question, &[0]).unwrap(),
            Some(BlockingKind::Questions)
        );

        let mixed = vec![
            append("cairn://p/cairn/1/1/builder/tasks"),
            append("cairn://p/cairn/1/1/builder/questions"),
        ];
        assert!(validate_blocking_group(&mixed, &[0, 1]).is_err());

        let two_questions = vec![
            append("cairn://p/cairn/1/1/builder/questions"),
            append("cairn://p/cairn/1/1/builder/questions"),
        ];
        assert!(validate_blocking_group(&two_questions, &[0, 1]).is_err());

        assert_eq!(validate_blocking_group(&[], &[]).unwrap(), None);
    }

    fn task_payload(description: &str, prompt: &str) -> TaskPayload {
        TaskPayload {
            description: description.to_string(),
            prompt: prompt.to_string(),
            subagent_type: "Explore".to_string(),
            models: None,
            run_in_background: None,
            session: None,
            task_index: None,
            output_schema: None,
        }
    }

    /// A task or call that still names a retired model key is refused with
    /// what replaced it, and so is an empty allotment.
    #[test]
    fn retired_model_keys_in_a_payload_are_refused_with_the_instruction() {
        use crate::mcp::types::model_keys_problem;
        for key in ["tier", "model", "backend", "backendPreference"] {
            let problem = model_keys_problem(&serde_json::json!({
                "subagentType": "Explore", "prompt": "p", key: "lg"
            }))
            .unwrap_or_else(|| panic!("{key} is refused"));
            assert!(problem.contains("models: ["), "{key}: {problem}");
            assert!(
                problem.contains("docs/backends.md#model-selection"),
                "{problem}"
            );
        }
        assert!(model_keys_problem(&serde_json::json!({"models": []}))
            .unwrap()
            .contains("empty"));
        assert_eq!(
            model_keys_problem(&serde_json::json!({"models": ["sm", "claude/opus"]})),
            None
        );
        let task: TaskPayload = serde_json::from_value(serde_json::json!({
            "description": "d", "prompt": "p", "subagentType": "Explore", "models": ["sm"]
        }))
        .unwrap();
        assert_eq!(
            delegated_task_payload(task).models,
            Some(vec![cairn_config::allotment::AllotmentEntry::Pool(
                "sm".into()
            )])
        );
    }

    #[test]
    fn delegated_payload_keeps_explicit_description() {
        let task = task_payload("Explicit title", "do the thing");
        assert_eq!(delegated_task_payload(task).description, "Explicit title");
    }

    fn call_payload(prompt: &str) -> CallPayload {
        CallPayload {
            prompt: prompt.to_string(),
            subagent_type: None,
            description: None,
            models: None,
            output_schema: None,
            branch: None,
            label: None,
            phase: None,
            run_in_background: None,
            task_index: None,
            ordinal: None,
        }
    }

    #[test]
    fn call_payload_defaults_explore_and_inherit() {
        let d = delegated_call_payload(call_payload("Summarize the parser"));
        assert_eq!(d.subagent_type, "Explore");
        assert_eq!(d.branch_policy, CallBranchPolicy::Inherit);
        // Description derives from the prompt when none is given.
        assert_eq!(d.description, "Summarize the parser");
    }

    #[test]
    fn call_payload_none_branch_and_label_title() {
        let mut c = call_payload("do it");
        c.branch = Some(CallBranchMode::None);
        c.label = Some("verifier".to_string());
        let d = delegated_call_payload(c);
        assert_eq!(d.branch_policy, CallBranchPolicy::None);
        // Description falls back to the label before the prompt.
        assert_eq!(d.description, "verifier");
    }

    #[test]
    fn classifies_node_calls_append() {
        assert_eq!(
            blocking_append_kind(&append("cairn://p/cairn/1/1/builder/calls")),
            Some(BlockingKind::Calls)
        );
    }

    #[test]
    fn validate_blocking_group_calls_only_and_mixes() {
        let calls = vec![
            append("cairn://p/cairn/1/1/builder/calls"),
            append("cairn://p/cairn/1/1/builder/calls"),
        ];
        assert_eq!(
            validate_blocking_group(&calls, &[0, 1]).unwrap(),
            Some(BlockingKind::Calls)
        );

        // Mixing calls with tasks or questions is rejected.
        let call_task = vec![
            append("cairn://p/cairn/1/1/builder/calls"),
            append("cairn://p/cairn/1/1/builder/tasks"),
        ];
        assert!(validate_blocking_group(&call_task, &[0, 1]).is_err());
        let call_question = vec![
            append("cairn://p/cairn/1/1/builder/calls"),
            append("cairn://p/cairn/1/1/builder/questions"),
        ];
        assert!(validate_blocking_group(&call_question, &[0, 1]).is_err());
    }

    #[test]
    fn cross_node_append_rejects_output_schema() {
        // No schema -> no rejection.
        let mut task = task_payload("rescue", "finish it");
        assert!(reject_cross_node_output_schema(std::slice::from_ref(&task)).is_none());
        // A schema on the cross-node route is rejected with a clear error naming
        // the limitation, rather than silently dropped.
        task.output_schema = Some(crate::models::OutputSchema::Preset("review".to_string()));
        let err = reject_cross_node_output_schema(std::slice::from_ref(&task))
            .expect("schema on cross-node append is rejected");
        assert!(
            err.contains("outputSchema is not supported on cross-node"),
            "{err}"
        );
        assert!(err.contains("rescue"), "{err}");
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
            node_tasks_coords("cairn://p/cairn/1295/1/builder/tasks"),
            Some(("cairn".to_string(), 1295, 1, "builder".to_string()))
        );
        // Query strings on the target do not break extraction.
        assert_eq!(
            node_tasks_coords("cairn://p/cairn/42/2/planner/tasks?limit=5"),
            Some(("cairn".to_string(), 42, 2, "planner".to_string()))
        );
        // Non-tasks targets yield no coordinates.
        assert_eq!(
            node_tasks_coords("cairn://p/cairn/1/1/builder/questions"),
            None
        );
        assert_eq!(node_tasks_coords("cairn://p/cairn/1/messages"), None);
    }

    /// Seed a minimal project/issue/execution with two sibling node jobs
    /// (`builder`, `coordinator`) and a live run on the coordinator (the caller),
    /// then assert that resolving a tasks URI addressed to *another* node yields
    /// that node's job — not the caller's. This is the behavioral pin for the
    /// cross-node fix: the spawn lands under the addressed node, so it inherits
    /// that node's worktree and lists in its tasks collection.
    #[tokio::test]
    async fn resolve_task_routing_addresses_target_node_not_caller() {
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
        let db = LocalDb::open_migrated_for_test(temp.path().join("blocking-routing.db"))
            .await
            .unwrap();

        exec(
            &db,
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('proj-1', 'default', 'Test', 'mcp', '/tmp/repo', 1, 1)",
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
        // Target node carries the durable branch the cross-node child inherits.
        exec(
            &db,
            "INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, status, created_at, updated_at, uri_segment, branch)
             VALUES ('job-builder', 'exec-1', 'issue-1', 'proj-1', 'Builder', 'running', 1, 1, 'builder', 'agent/builder')",
        )
        .await;
        // Caller node.
        exec(
            &db,
            "INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, status, created_at, updated_at, uri_segment, branch)
             VALUES ('job-coord', 'exec-1', 'issue-1', 'proj-1', 'Coordinator', 'running', 1, 1, 'coordinator', 'agent/coordinator')",
        )
        .await;
        exec(
            &db,
            "INSERT INTO runs(id, job_id, issue_id, created_at, updated_at)
             VALUES ('run-coord', 'job-coord', 'issue-1', 1, 1)",
        )
        .await;

        let builder = node_tasks_coords("cairn://p/mcp/1/1/builder/tasks").unwrap();
        let coordinator = node_tasks_coords("cairn://p/mcp/1/1/coordinator/tasks").unwrap();

        // Cross-node: the builder URI, issued by the coordinator caller, resolves
        // to the BUILDER's job (the fix) and routes cross-node — i.e. it would
        // spawn under the builder, inheriting its branch coordinate, not the caller's.
        let cross = resolve_task_routing(&db, "run-coord", std::slice::from_ref(&builder))
            .await
            .unwrap();
        assert_eq!(cross.target_job_ids, vec!["job-builder".to_string()]);
        assert_eq!(cross.route, TaskRoute::CrossNode);

        // Self: the coordinator URI resolves back to the caller's own job, so the
        // existing delegated pipeline handles it unchanged.
        let self_route = resolve_task_routing(&db, "run-coord", std::slice::from_ref(&coordinator))
            .await
            .unwrap();
        assert_eq!(self_route.target_job_ids, vec!["job-coord".to_string()]);
        assert_eq!(self_route.route, TaskRoute::SelfNode);

        // Mixed self + cross-node in one batch is rejected.
        let mixed = resolve_task_routing(&db, "run-coord", &[coordinator.clone(), builder.clone()])
            .await
            .unwrap();
        assert_eq!(mixed.route, TaskRoute::Mixed);

        // A nonexistent target surfaces a clear error rather than a silent
        // fallback to the caller's node.
        let ghost = node_tasks_coords("cairn://p/mcp/1/1/ghost/tasks").unwrap();
        let err = resolve_task_routing(&db, "run-coord", std::slice::from_ref(&ghost))
            .await
            .unwrap_err();
        assert!(err.contains("not found"), "unexpected error: {err}");

        // A thread session remains the caller after its mutable name changes. The
        // write handler resolves `cairn:~/tasks` to this new canonical address;
        // routing must then resolve that address back to the existing session job.
        for sql in [
            "INSERT INTO threads(id, project_id, name, status, attention, created_at, updated_at) VALUES ('thread-1', 'proj-1', 'channels-old', 'active', 'none', 1, 1)",
            "INSERT INTO jobs(id, thread_id, project_id, node_name, agent_config_id, status, created_at, updated_at, uri_segment) VALUES ('job-thread', 'thread-1', 'proj-1', 'thread', 'thread', 'running', 1, 1, 'thread')",
            "INSERT INTO runs(id, project_id, job_id, status, created_at, updated_at) VALUES ('run-thread', 'proj-1', 'job-thread', 'live', 1, 1)",
            "UPDATE threads SET name='channels' WHERE id='thread-1'",
        ] {
            exec(&db, sql).await;
        }
        let renamed = node_tasks_coords("cairn://p/mcp/channels/tasks").unwrap();
        let renamed_route = resolve_task_routing(&db, "run-thread", &[renamed])
            .await
            .unwrap();
        assert_eq!(renamed_route.target_job_ids, vec!["job-thread".to_string()]);
        assert_eq!(renamed_route.route, TaskRoute::SelfNode);
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
            claim_change_tool_id(&[newest, older], "/tasks"),
            Claim::One("toolu_change_abc".to_string())
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
            claim_change_tool_id(&[newest], "/questions"),
            Claim::One("toolu_question_abc".to_string())
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
        assert_eq!(claim_change_tool_id(&[file_change], "/tasks"), Claim::None);
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
        assert_eq!(
            claim_change_tool_id(&[question_change], "/tasks"),
            Claim::None
        );
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
            claim_change_tool_id(&[newest], "/tasks"),
            Claim::One("toolu_task_write".to_string())
        );
    }
}
