//! A thread's tasks are ordinary delegated jobs: they complete, and their
//! completion wakes the thread parked on them. Only the thread's session job is
//! liveness-only. The boot repair settles the tasks a release that projected
//! every thread-owned job by liveness left stranded `idle` (cairn/5058).

use super::transition_to_warm_state;
use super::warm_completion_tests::{register_warm_process, test_orchestrator};
use crate::models::{
    ContextNodeConfig, CreateThread, DelegatedWorkPacket, ExecutionSnapshot, JobStatus,
    NodePosition, RecipeEdge, RecipeEdgeType, RecipeNode, RecipeNodeType, RecipeSnapshot,
    RecipeTrigger, TriggerContext, TriggerType, TurnEndReason,
};
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};
use cairn_db::turso::params;

const THREAD: &str = "t-roadmap";
const SESSION: &str = "j-session";
const EXECUTION: &str = "e-host";
const STALE_BOOT_AT: i64 = 1_000_000;

/// How the parent session's head rests when the repair runs.
#[derive(Clone, Copy, PartialEq)]
enum ParentHead {
    /// Still the yielded `dependency_wait` anchor the packets name.
    Parked,
    /// A later turn has replaced the anchor as the session's head.
    MovedOn,
}

/// How a task's last work turn ended.
#[derive(Clone, Copy, PartialEq)]
enum LastTurn {
    /// Complete, with the packet's `return` artifact written.
    Delivered,
    /// Interrupted by a user stop, with no artifact.
    Interrupted,
    /// Still running on a live run (the live regression shape).
    Running,
}

struct Task {
    job: &'static str,
    packet: &'static str,
    background: bool,
    last_turn: LastTurn,
    finished_at: i64,
}

impl Task {
    fn delivered(job: &'static str, packet: &'static str, finished_at: i64) -> Self {
        Self {
            job,
            packet,
            background: false,
            last_turn: LastTurn::Delivered,
            finished_at,
        }
    }

    fn node_id(&self) -> String {
        format!("delegated-{}-agent", self.packet)
    }
}

async fn test_db() -> LocalDb {
    let temp = tempfile::tempdir().unwrap();
    let db = LocalDb::open_migrated_for_test(temp.keep().join("thread-tasks.db"))
        .await
        .unwrap();
    db.execute_script(
        "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w','W',1,1);
         INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
           VALUES ('p','w','P','prj','/tmp/prj',1,1);",
    )
    .await
    .unwrap();
    db
}

fn packet(task: &Task, index: i32) -> DelegatedWorkPacket {
    let node_id = task.node_id();
    serde_json::from_value(serde_json::json!({
        "id": task.packet,
        "parentJobId": SESSION,
        "parentTurnId": "anchor",
        "parentToolUseId": "tool-1",
        "origin": "task_tool",
        "title": task.packet,
        "problemStatement": "x",
        "agentConfigId": "Explore",
        "ownership": {"cwd": "/tmp"},
        "outputContract": {"schemaType": "return"},
        "status": "materialized",
        "materializedNodeIds": [
            format!("delegated-{}-trigger", task.packet),
            format!("delegated-{}-context", task.packet),
            node_id,
        ],
        "resultArtifactJobId": task.job,
        "background": task.background,
        "taskIndex": index,
        "createdAt": 0
    }))
    .unwrap()
}

fn plain_node(id: String, node_type: RecipeNodeType) -> RecipeNode {
    RecipeNode {
        id: id.clone(),
        node_type,
        name: id,
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
    }
}

/// The thread's host execution with each task lowered the way the DAG expander
/// lowers a packet: trigger and context nodes feeding the real delegated agent
/// node, whose inline output schema is the packet's contract.
fn host_snapshot(tasks: &[Task]) -> String {
    let mut nodes = Vec::new();
    let mut edges = Vec::new();
    let mut packets = Vec::new();
    for (index, task) in tasks.iter().enumerate() {
        let packet = packet(task, index as i32);
        let trigger = format!("delegated-{}-trigger", task.packet);
        let context = format!("delegated-{}-context", task.packet);
        let agent = task.node_id();
        nodes.push(plain_node(trigger.clone(), RecipeNodeType::Trigger));
        let mut context_node = plain_node(context.clone(), RecipeNodeType::Context);
        context_node.context_config = Some(ContextNodeConfig {
            content: "x".to_string(),
        });
        nodes.push(context_node);
        nodes.push(
            cairn_workflow::effects::dag::delegation::delegated_agent_node_for_test(
                agent.clone(),
                &packet,
            ),
        );
        edges.push(RecipeEdge {
            id: format!("{trigger}-edge"),
            edge_type: RecipeEdgeType::Control,
            source_node_id: trigger,
            source_handle: "control-out".to_string(),
            target_node_id: agent.clone(),
            target_handle: "control-in".to_string(),
        });
        edges.push(RecipeEdge {
            id: format!("{context}-edge"),
            edge_type: RecipeEdgeType::Context,
            source_node_id: context,
            source_handle: "context-out".to_string(),
            target_node_id: agent,
            target_handle: "context-in".to_string(),
        });
        packets.push(packet);
    }
    serde_json::to_string(&ExecutionSnapshot {
        continuity: Default::default(),
        branch_target: Default::default(),
        model_routing: None,
        recipe: RecipeSnapshot {
            id: "host-thread".to_string(),
            name: "Session Host".to_string(),
            description: None,
            trigger: RecipeTrigger::Manual,
            nodes,
            edges,
        },
        agents: std::collections::HashMap::new(),
        skills: std::collections::HashMap::new(),
        trigger_context: TriggerContext {
            issue_id: None,
            project_id: "p".to_string(),
            trigger_type: TriggerType::Manual,
            event_payload: None,
            initiated_via: None,
        },
        presets: None,
        delegated_packets: packets,
        created_at: 0,
    })
    .unwrap()
}

/// A thread's session job parked (or not) on the `anchor` turn, in its host
/// execution, beside the given tasks. Every task carries the thread's id, as a
/// task a thread spawns inherits it. Stranded tasks rest `idle`, the status the
/// liveness-only projection left them in. The session has no current session
/// id, so a resume that claims the unblock fails fast in `continue_job_impl`
/// rather than launching a provider: the claimed turn is the observable proof.
async fn seed(db: &LocalDb, head: ParentHead, tasks: &[Task]) {
    db.execute(
        "INSERT INTO threads (id, project_id, name, status, attention, created_at, updated_at)
         VALUES (?1, 'p', 'roadmap', 'active', 'none', 1, 1)",
        params![THREAD],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO executions (id, recipe_id, project_id, status, started_at, snapshot, triggered_by)
         VALUES (?1, 'host-thread', 'p', 'running', 1, ?2, 'manual')",
        params![EXECUTION, host_snapshot(tasks)],
    )
    .await
    .unwrap();
    let (session_status, head_turn) = match head {
        ParentHead::Parked => ("running", "anchor"),
        ParentHead::MovedOn => ("idle", "later"),
    };
    db.execute(
        "INSERT INTO jobs (id, execution_id, thread_id, project_id, status, node_name, uri_segment,
                           created_at, updated_at)
         VALUES (?1, ?2, ?3, 'p', ?4, 'thread', 'thread', 1, 1)",
        params![SESSION, EXECUTION, THREAD, session_status],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO turns (id, session_id, job_id, sequence, state, yield_reason, start_reason,
                            created_at, ended_at, updated_at)
         VALUES ('anchor', 's-session', ?1, 1, 'yielded', 'dependency_wait', 'initial', 1, 1, 1)",
        params![SESSION],
    )
    .await
    .unwrap();
    if head == ParentHead::MovedOn {
        db.execute(
            "INSERT INTO turns (id, session_id, job_id, sequence, predecessor_id, state, start_reason,
                                created_at, ended_at, updated_at)
             VALUES ('later', 's-session', ?1, 2, 'anchor', 'complete', 'follow_up', 2, 3, 3)",
            params![SESSION],
        )
        .await
        .unwrap();
    }
    db.execute(
        "UPDATE jobs SET current_turn_id = ?1 WHERE id = ?2",
        params![head_turn, SESSION],
    )
    .await
    .unwrap();
    for task in tasks {
        let status = if task.last_turn == LastTurn::Running {
            "running"
        } else {
            "idle"
        };
        db.execute(
            "INSERT INTO jobs (id, execution_id, recipe_node_id, parent_job_id, thread_id, project_id,
                               status, node_name, uri_segment, parent_tool_use_id, agent_config_id,
                               created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, 'p', ?6, ?7, ?7, 'tool-1', 'Explore', 1, 1)",
            params![
                task.job,
                EXECUTION,
                task.node_id(),
                SESSION,
                THREAD,
                status,
                task.packet
            ],
        )
        .await
        .unwrap();
        let (state, end_reason) = match task.last_turn {
            LastTurn::Delivered => ("complete", Some("artifact_handoff")),
            LastTurn::Interrupted => ("interrupted", None),
            LastTurn::Running => ("running", None),
        };
        let run_id = (task.last_turn == LastTurn::Running).then(|| format!("run-{}", task.job));
        if let Some(run_id) = run_id.as_deref() {
            db.execute(
                "INSERT INTO runs (id, job_id, project_id, status, session_id, created_at, updated_at)
                 VALUES (?1, ?2, 'p', 'live', ?3, 2000000, 2000000)",
                params![run_id, task.job, format!("s-{}", task.job)],
            )
            .await
            .unwrap();
        }
        let ended_at = (task.last_turn != LastTurn::Running).then_some(task.finished_at);
        db.execute(
            "INSERT INTO turns (id, session_id, job_id, run_id, sequence, state, start_reason,
                                end_reason, created_at, ended_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, 1, ?5, 'initial', ?6, ?7, ?8, ?7)",
            params![
                format!("turn-{}", task.job),
                format!("s-{}", task.job),
                task.job,
                run_id.as_deref(),
                state,
                end_reason,
                task.finished_at - 10,
                ended_at
            ],
        )
        .await
        .unwrap();
        db.execute(
            "UPDATE jobs SET current_turn_id = ?1 WHERE id = ?2",
            params![format!("turn-{}", task.job), task.job],
        )
        .await
        .unwrap();
        if task.last_turn != LastTurn::Interrupted {
            db.execute(
                "INSERT INTO artifacts (id, job_id, artifact_type, confirmed, data, version,
                                        output_name, created_at, updated_at)
                 VALUES (?1, ?2, 'return', 0, '{\"summary\":\"done\"}', 1, 'return', 1, 1)",
                params![format!("artifact-{}", task.job), task.job],
            )
            .await
            .unwrap();
        }
    }
}

async fn text(orch: &Orchestrator, sql: &str, id: &str) -> Option<String> {
    orch.db
        .local
        .query_opt_text(sql, params![id])
        .await
        .unwrap()
}

async fn status(orch: &Orchestrator, job_id: &str) -> String {
    text(orch, "SELECT status FROM jobs WHERE id = ?1", job_id)
        .await
        .expect("job exists")
}

async fn count(orch: &Orchestrator, sql: &str) -> i64 {
    orch.db
        .local
        .query_one(sql, (), |row| row.i64(0))
        .await
        .unwrap()
}

async fn unblock_turns(orch: &Orchestrator) -> i64 {
    count(
        orch,
        "SELECT COUNT(*) FROM turns
         WHERE predecessor_id = 'anchor' AND start_reason = 'dependency_unblock'",
    )
    .await
}

fn reconcile(orch: &Orchestrator) {
    crate::runs::queries::reconcile_stale_runs(orch, orch.services.emitter.as_ref(), STALE_BOOT_AT);
}

/// The defect itself: a thread task whose turn ends with its artifact derived
/// Idle under the thread's liveness rule, so its packet never resolved and the
/// parked thread was never unblocked.
#[tokio::test]
async fn thread_task_completion_resumes_parked_thread_once() {
    let db = test_db().await;
    let task = Task {
        last_turn: LastTurn::Running,
        ..Task::delivered("j-task", "pkt-task", 1_757_000_000)
    };
    seed(&db, ParentHead::Parked, &[task]).await;
    let orch = test_orchestrator(db);
    register_warm_process(&orch, "run-j-task", Some("j-task"));
    orch.process_state
        .set_current_turn_id("run-j-task", Some("turn-j-task"));

    assert!(transition_to_warm_state(
        &orch,
        "run-j-task",
        Some(TurnEndReason::ArtifactHandoff),
    ));

    assert_eq!(status(&orch, "j-task").await, "complete");
    assert_eq!(
        unblock_turns(&orch).await,
        1,
        "the parked thread is unblocked exactly once"
    );
}

/// The session itself is a standing conversation: whatever its turns and
/// artifacts say, it rests Idle or runs, through both recompute entry points.
#[tokio::test]
async fn thread_session_never_derives_complete() {
    let db = test_db().await;
    seed(&db, ParentHead::MovedOn, &[]).await;
    db.execute(
        "INSERT INTO artifacts (id, job_id, artifact_type, confirmed, data, version, output_name,
                                created_at, updated_at)
         VALUES ('arc', ?1, 'arc', 1, '{}', 1, 'arc', 1, 1)",
        params![SESSION],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE jobs SET status = 'running' WHERE id = ?1",
        params![SESSION],
    )
    .await
    .unwrap();
    let orch = test_orchestrator(db);

    let direct = orch
        .db
        .local
        .read(|conn| {
            Box::pin(async move {
                crate::execution::advancement::recompute_job_status_conn(conn, SESSION).await
            })
        })
        .await
        .unwrap();
    assert_eq!(direct, JobStatus::Idle);
    crate::execution::advancement::recompute_job(&orch, SESSION).unwrap();
    assert_eq!(status(&orch, SESSION).await, "idle");

    orch.db
        .local
        .execute(
            "INSERT INTO turns (id, session_id, job_id, sequence, predecessor_id, state, start_reason,
                                created_at, updated_at)
             VALUES ('next', 's-session', ?1, 3, 'later', 'pending', 'follow_up', 4, 4)",
            params![SESSION],
        )
        .await
        .unwrap();
    crate::execution::advancement::recompute_job(&orch, SESSION).unwrap();
    assert_eq!(status(&orch, SESSION).await, "running");
}

/// A thread created from within a thread has its own `threads` row and its own
/// root session job, so it stays liveness-only. A task that merely runs the
/// Thread agent has a parent, so it is a task and completes like one.
#[tokio::test]
async fn child_thread_session_stays_liveness_only() {
    let db = test_db().await;
    let orch = test_orchestrator(db);
    let mut sessions = Vec::new();
    for name in ["roadmap", "spinoff"] {
        let thread = crate::threads::crud::create(
            &orch.db.local,
            CreateThread {
                project_id: "p".into(),
                name: Some(name.into()),
                jurisdiction: None,
                definition: None,
                migrated_from_number: None,
                model: None,
            },
        )
        .await
        .unwrap();
        let session = orch
            .db
            .local
            .query_opt_text(
                &format!(
                    "SELECT j.id FROM jobs j WHERE j.thread_id = ?1 AND {}",
                    cairn_db::lineage::SESSION_JOB_SHAPE
                ),
                params![thread.id.as_str()],
            )
            .await
            .unwrap()
            .expect("each thread has its own session job");
        sessions.push((thread.id, session));
    }
    let (parent_thread, parent_session) = sessions[0].clone();
    let (_, child_session) = sessions[1].clone();

    let thread_agent_task = Task::delivered("j-thread-agent", "pkt-thread-agent", 10);
    orch.db
        .local
        .execute(
            "INSERT INTO executions (id, recipe_id, project_id, status, started_at, snapshot, triggered_by)
             VALUES (?1, 'host-thread', 'p', 'running', 1, ?2, 'manual')",
            params![EXECUTION, host_snapshot(std::slice::from_ref(&thread_agent_task))],
        )
        .await
        .unwrap();
    orch.db
        .local
        .execute(
            "UPDATE jobs SET execution_id = ?1, status = 'running' WHERE id = ?2",
            params![EXECUTION, child_session.as_str()],
        )
        .await
        .unwrap();
    orch.db
        .local
        .execute(
            "INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, created_at,
                                ended_at, updated_at)
             VALUES ('child-turn', 's-child', ?1, 1, 'complete', 'initial', 1, 2, 2)",
            params![child_session.as_str()],
        )
        .await
        .unwrap();
    orch.db
        .local
        .execute(
            "INSERT INTO jobs (id, execution_id, recipe_node_id, parent_job_id, thread_id, project_id,
                               status, node_name, uri_segment, agent_config_id, created_at, updated_at)
             VALUES ('j-thread-agent', ?1, ?2, ?3, ?4, 'p', 'running', 'spin', 'spin', 'thread', 1, 1)",
            params![
                EXECUTION,
                thread_agent_task.node_id(),
                parent_session.as_str(),
                parent_thread.as_str()
            ],
        )
        .await
        .unwrap();
    orch.db
        .local
        .execute_script(
            "INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, end_reason,
                                created_at, ended_at, updated_at)
               VALUES ('agent-turn', 's-agent', 'j-thread-agent', 1, 'complete', 'initial',
                       'artifact_handoff', 1, 2, 2);
             INSERT INTO artifacts (id, job_id, artifact_type, confirmed, data, version, output_name,
                                    created_at, updated_at)
               VALUES ('agent-return', 'j-thread-agent', 'return', 0, '{}', 1, 'return', 1, 1);",
        )
        .await
        .unwrap();

    crate::execution::advancement::recompute_execution_jobs(&orch, EXECUTION).unwrap();

    assert_eq!(
        status(&orch, &child_session).await,
        "idle",
        "a child thread's session is a session: it rests, it never completes"
    );
    assert_eq!(
        status(&orch, "j-thread-agent").await,
        "complete",
        "a task running the Thread agent is still a task"
    );
}

/// (a) The parent moved on long ago: the stranded task settles Complete, and
/// nothing at all reaches the parent.
#[tokio::test]
async fn repair_settles_a_stranded_task_without_touching_a_parent_that_moved_on() {
    let db = test_db().await;
    seed(
        &db,
        ParentHead::MovedOn,
        &[Task::delivered("j-measure", "pkt-measure", 1_757_000_000)],
    )
    .await;
    let orch = test_orchestrator(db);
    let messages_before = count(&orch, "SELECT COUNT(*) FROM messages").await;

    reconcile(&orch);

    assert_eq!(status(&orch, "j-measure").await, "complete");
    assert_eq!(
        count(
            &orch,
            "SELECT COUNT(*) FROM turns WHERE job_id = 'j-session'"
        )
        .await,
        2,
        "no turn is written for a parent whose head moved on"
    );
    assert_eq!(
        count(&orch, "SELECT COUNT(*) FROM attention_pushes").await,
        0
    );
    assert_eq!(
        count(&orch, "SELECT COUNT(*) FROM messages").await,
        messages_before,
        "a repaired settlement announces nothing"
    );
    assert_eq!(
        count(
            &orch,
            "SELECT COUNT(*) FROM effect_outbox WHERE kind = 'advance_dag'"
        )
        .await,
        0,
        "a repaired settlement advances nothing"
    );
}

/// (b) and (f): a blocking batch of three whose parent is still parked on the
/// exact anchor wakes that parent exactly once, and a second boot is a no-op.
#[tokio::test]
async fn repair_wakes_a_parent_still_parked_on_its_batch_exactly_once() {
    let db = test_db().await;
    seed(
        &db,
        ParentHead::Parked,
        &[
            Task::delivered("j-quic", "pkt-quic", 1_757_000_000),
            Task::delivered("j-tar", "pkt-tar", 1_757_000_600),
            Task::delivered("j-jwt", "pkt-jwt", 1_757_001_200),
        ],
    )
    .await;
    let orch = test_orchestrator(db);

    reconcile(&orch);

    for job in ["j-quic", "j-tar", "j-jwt"] {
        assert_eq!(status(&orch, job).await, "complete", "{job}");
    }
    assert_eq!(unblock_turns(&orch).await, 1);
    assert_eq!(
        count(&orch, "SELECT COUNT(*) FROM attention_pushes").await,
        0
    );

    reconcile(&orch);

    assert_eq!(
        unblock_turns(&orch).await,
        1,
        "a second boot changes nothing"
    );
    for job in ["j-quic", "j-tar", "j-jwt"] {
        assert_eq!(status(&orch, job).await, "complete", "{job}");
    }
}

/// (c) A background task settles, and its spawner is not pushed: nobody was
/// parked on it, and a late wake would only interrupt.
#[tokio::test]
async fn repair_never_pushes_for_a_background_task() {
    let db = test_db().await;
    let task = Task {
        background: true,
        ..Task::delivered("j-map", "pkt-map", 1_757_000_000)
    };
    seed(&db, ParentHead::MovedOn, &[task]).await;
    let orch = test_orchestrator(db);

    reconcile(&orch);

    assert_eq!(status(&orch, "j-map").await, "complete");
    assert_eq!(
        count(&orch, "SELECT COUNT(*) FROM attention_pushes").await,
        0
    );
    assert_eq!(unblock_turns(&orch).await, 0);
}

/// (d) A task whose last turn was interrupted settles Cancelled. Re-deriving it
/// would say Pending, and the next advance would relaunch long-dead work.
#[tokio::test]
async fn repair_cancels_a_task_whose_last_turn_was_interrupted() {
    let db = test_db().await;
    let task = Task {
        last_turn: LastTurn::Interrupted,
        ..Task::delivered("j-deck", "pkt-deck", 1_757_000_000)
    };
    seed(&db, ParentHead::MovedOn, &[task]).await;
    let orch = test_orchestrator(db);

    reconcile(&orch);
    assert_eq!(status(&orch, "j-deck").await, "cancelled");

    crate::execution::advancement::recompute_execution_jobs(&orch, EXECUTION).unwrap();
    assert_eq!(
        status(&orch, "j-deck").await,
        "cancelled",
        "cancelled is sticky: the next sweep does not lift it to Pending"
    );
    assert_eq!(
        count(
            &orch,
            "SELECT COUNT(*) FROM effect_outbox WHERE kind = 'advance_dag'"
        )
        .await,
        0
    );
}

/// What a parked task is still waiting on.
#[derive(Clone, Copy, Debug)]
enum OwnWait {
    Question,
    Permission,
    AgentWait,
}

/// Give `job` a terminal run (so the active-run screen does not hide it) and
/// an open wait of `kind` raised from it.
async fn park_on_own_wait(db: &LocalDb, job: &str, kind: OwnWait) {
    let run = format!("run-{job}");
    let turn = format!("turn-{job}");
    db.execute(
        "INSERT INTO sessions (id, job_id, backend, status, sequence, created_at, updated_at)
         VALUES (?1, ?2, 'claude', 'open', 1, 1, 1)",
        params![format!("s-{job}"), job],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO runs (id, job_id, project_id, status, session_id, created_at, updated_at)
         VALUES (?1, ?2, 'p', 'exited', ?3, 1, 1)",
        params![run.as_str(), job, format!("s-{job}")],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE turns SET run_id = ?1 WHERE id = ?2",
        params![run.as_str(), turn.as_str()],
    )
    .await
    .unwrap();
    let sql = match kind {
        OwnWait::Question => {
            "INSERT INTO prompts (id, run_id, questions, created_at, turn_id, job_id)
             VALUES ('wait-' || ?3, ?1, '[]', 1, ?2, ?3)"
        }
        OwnWait::Permission => {
            "INSERT INTO permission_requests (id, run_id, tool_use_id, tool_name, tool_input, status,
                                              created_at, turn_id, job_id)
             VALUES ('wait-' || ?3, ?1, 'tool-wait', 'Bash', '{}', 'pending', 1, ?2, ?3)"
        }
        OwnWait::AgentWait => {
            "INSERT INTO agent_waits (id, job_id, run_id, session_id, predecessor_turn_id, tool_use_id,
                                      condition_json, state, created_at)
             VALUES ('wait-' || ?3, ?3, ?1, 's-' || ?3, ?2, 'tool-wait', '{}', 'pending', 1)"
        }
    };
    db.execute(sql, params![run.as_str(), turn.as_str(), job])
        .await
        .unwrap();
}

async fn wait_is_open(orch: &Orchestrator, job: &str, kind: OwnWait) -> bool {
    let sql = match kind {
        OwnWait::Question => {
            "SELECT COUNT(*) FROM prompts WHERE job_id = ?1 AND response IS NULL AND withdrawn_at IS NULL"
        }
        OwnWait::Permission => {
            "SELECT COUNT(*) FROM permission_requests WHERE job_id = ?1 AND status = 'pending'"
        }
        OwnWait::AgentWait => {
            "SELECT COUNT(*) FROM agent_waits WHERE job_id = ?1 AND state = 'pending'"
        }
    };
    orch.db
        .local
        .query_one(sql, params![job], |row| row.i64(0))
        .await
        .unwrap()
        == 1
}

/// The safety guard. Under the defect a task parked on its own question also
/// read `idle`. Its latest turn is yielded and its question is open: it is not
/// finished, so the repair must leave it exactly as it is rather than settle it
/// Cancelled and tell its parent it ended.
#[tokio::test]
async fn repair_leaves_a_task_parked_on_its_own_question_alone() {
    let db = test_db().await;
    let task = Task {
        last_turn: LastTurn::Interrupted,
        ..Task::delivered("j-asking", "pkt-asking", 1_757_000_000)
    };
    seed(&db, ParentHead::Parked, &[task]).await;
    db.execute(
        "UPDATE turns SET state = 'yielded', yield_reason = 'user_input', ended_at = NULL
         WHERE id = 'turn-j-asking'",
        (),
    )
    .await
    .unwrap();
    park_on_own_wait(&db, "j-asking", OwnWait::Question).await;
    let orch = test_orchestrator(db);

    reconcile(&orch);

    assert_eq!(status(&orch, "j-asking").await, "idle", "not settled");
    assert_eq!(
        text(
            &orch,
            "SELECT state FROM turns WHERE id = ?1",
            "turn-j-asking"
        )
        .await
        .as_deref(),
        Some("yielded"),
        "the task's own wait is untouched"
    );
    assert!(wait_is_open(&orch, "j-asking", OwnWait::Question).await);
    assert_eq!(
        unblock_turns(&orch).await,
        0,
        "its parent is not told it ended"
    );
}

/// Each open wait guards on its own, even when the last turn reads terminal (a
/// user stop interrupted the turn that raised it).
#[tokio::test]
async fn repair_leaves_a_task_with_any_open_wait_alone() {
    for kind in [OwnWait::Question, OwnWait::Permission, OwnWait::AgentWait] {
        let db = test_db().await;
        let task = Task {
            last_turn: LastTurn::Interrupted,
            ..Task::delivered("j-waiting", "pkt-waiting", 1_757_000_000)
        };
        seed(&db, ParentHead::MovedOn, &[task]).await;
        park_on_own_wait(&db, "j-waiting", kind).await;
        let orch = test_orchestrator(db);

        reconcile(&orch);

        assert_eq!(status(&orch, "j-waiting").await, "idle", "{kind:?}");
        assert!(wait_is_open(&orch, "j-waiting", kind).await, "{kind:?}");
    }
}

/// (e) A node-less task that never started (a Thread-agent task with no packet,
/// the stranded `automations` shape) settles Cancelled.
#[tokio::test]
async fn repair_cancels_a_node_less_task_that_never_started() {
    let db = test_db().await;
    seed(&db, ParentHead::MovedOn, &[]).await;
    db.execute(
        "INSERT INTO jobs (id, execution_id, parent_job_id, thread_id, project_id, status, node_name,
                           uri_segment, agent_config_id, created_at, updated_at)
         VALUES ('j-automations', ?1, ?2, ?3, 'p', 'idle', 'automations', 'automations', 'thread',
                 1, 1)",
        params![EXECUTION, SESSION, THREAD],
    )
    .await
    .unwrap();
    let orch = test_orchestrator(db);

    reconcile(&orch);

    assert_eq!(status(&orch, "j-automations").await, "cancelled");
    assert_eq!(
        status(&orch, SESSION).await,
        "idle",
        "the session in the same execution is never settled"
    );
}
