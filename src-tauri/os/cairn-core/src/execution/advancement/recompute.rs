use super::*;
#[cfg(test)]
use cairn_workflow::advancement::recompute::*;

/// Recompute one job by id. Recipe-backed jobs route through their execution
/// sweep; standalone jobs write the direct liveness projection through this same
/// entry point so every caller has one answer to "what is this job's status?".
pub fn recompute_job(orch: &Orchestrator, job_id: &str) -> Result<(), String> {
    cairn_workflow::advancement::recompute::recompute_job(
        &crate::workflow_host::runtime(orch),
        job_id,
    )
}

/// Owning-db wrapper around [`reduce_delegated_child_job_conn`] for the turn-end
/// callers that hold an `Orchestrator` + `job_id`. Emits a coarse `jobs`/
/// `executions` change on a write so the workflow monitoring panel's spinner
/// clears. Node-backed / non-terminal cases are a cheap no-op.
pub fn reduce_delegated_child_job(
    orch: &Orchestrator,
    job_id: &str,
) -> Result<Option<JobStatus>, String> {
    cairn_workflow::advancement::recompute::reduce_delegated_child_job(
        &crate::workflow_host::runtime(orch),
        job_id,
    )
}

/// Startup safety net for the CAIRN-2559 stuck shape: a node-less delegated
/// child (call/workflow) that finalized its run but whose job status was never
/// terminalized — run terminal, job `running`, caller `blocked`. The forward fix
/// (reduce at finalize) prevents this going forward, but a host that crashed
/// under the old code is left holding it, and `redispatch_crashed_workflows`
/// only re-spawns workflows whose latest run is NON-terminal, so it never
/// revisits this shape. For each stuck child, reduce its status from its
/// turn+artifact and — if that terminalized it — fire the resume gate so the
/// durably suspended caller wakes. Idempotent and self-gating: reduce no-ops
/// once terminal, and resume no-ops when there is no suspended parent to wake.
/// Best-effort over EVERY open database (local + team replicas): a team-owned
/// workflow/call writes its job, run, turns, artifacts, and execution snapshot
/// into the team's replica (the forward paths route through
/// `owning_db_for_job(parent_job_id)`), so a local-only scan would miss a stuck
/// team child entirely. `reduce`/`resume` already route by job id, so only the
/// candidate discovery has to fan out across databases. Errors are logged, never
/// fatal.
pub async fn heal_stuck_delegated_children(orch: &Orchestrator) {
    cairn_workflow::advancement::recompute::heal_stuck_delegated_children(
        &crate::workflow_host::runtime(orch),
    )
    .await
}

/// The single entry point every former decider calls: run the execution sweep in
/// a write transaction, then emit the follow-on effects keyed by each derived edge.
pub fn recompute_execution_jobs(orch: &Orchestrator, execution_id: &str) -> Result<(), String> {
    cairn_workflow::advancement::recompute::recompute_execution_jobs(
        &crate::workflow_host::runtime(orch),
        execution_id,
    )
}

/// Boot repair: sweep `execution_id` once, settling the stranded jobs in
/// `settle_job_ids` quietly, and return the status changes the sweep wrote.
pub fn settle_stranded_jobs(
    orch: &Orchestrator,
    execution_id: &str,
    settle_job_ids: &std::collections::HashSet<String>,
) -> Result<Vec<JobStatusChange>, String> {
    cairn_workflow::advancement::recompute::settle_stranded_jobs(
        &crate::workflow_host::runtime(orch),
        execution_id,
        settle_job_ids,
    )
}

#[cfg(test)]
mod artifact_present_tests {
    use super::{
        artifact_fact_for_test as artifact_fact, recompute_execution_jobs_conn, recompute_job,
        recompute_job_status_conn,
    };
    use crate::db::DbState;
    use crate::models::{
        AgentNodeConfig, ConfirmPolicy, ExecutionSnapshot, JobStatus, NodePosition, RecipeFile,
        RecipeNode, RecipeNodeType, RecipeSnapshot, RecipeTrigger, SchemaConfig, TriggerContext,
        TriggerType,
    };
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{
        DbError, LocalDb, MigrationRunner, RowExt, SearchIndex, TURSO_MIGRATIONS,
    };
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    async fn test_db() -> LocalDb {
        let temp = tempfile::tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("recompute.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db
    }

    /// Seed the minimal FK chain (workspace -> project -> issue -> execution ->
    /// job) plus a stray `checkpoint` artifact on the job. Deliberately does NOT
    /// write the required `create-pr` output.
    async fn seed_job_with_stray_artifact(db: &LocalDb) {
        db.write(|conn| {
            Box::pin(async move {
                conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-1','W',1,1)", ()).await?;
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-1','w-1','P','p','/tmp/p',1,1)", ()).await?;
                conn.execute("INSERT INTO issues (id, project_id, number, title, status, attention, created_at, updated_at) VALUES ('i-1','p-1',1,'T','active','none',1,1)", ()).await?;
                conn.execute("INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq) VALUES ('e-1','default','i-1','p-1','running',1,1)", ()).await?;
                conn.execute("INSERT INTO jobs (id, execution_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at) VALUES ('j-1','e-1','i-1','p-1','running','builder','builder',1,1)", ()).await?;
                conn.execute("INSERT INTO artifacts (id, job_id, artifact_type, data, version, output_name, created_at, updated_at) VALUES ('a-chk','j-1','checkpoint','{}',1,'checkpoint',1,1)", ()).await?;
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();
    }

    fn review_snapshot_json() -> String {
        let node = RecipeNode {
            id: "builder".to_string(),
            node_type: RecipeNodeType::Agent,
            name: "Builder".to_string(),
            position: NodePosition { x: 0.0, y: 0.0 },
            parent_id: None,
            trigger_config: None,
            agent_config: Some(AgentNodeConfig {
                agent_config_id: Some("builder".to_string()),
                output_schema: Some(SchemaConfig {
                    name: "create-pr".to_string(),
                    schema: None,
                    confirm_policy: ConfirmPolicy::Auto,
                    tool_name: None,
                    description: None,
                }),
                git_config: None,
            }),
            action_config: None,
            checkpoint_config: None,
            artifact_config: None,
            condition_config: None,
            decision_config: None,
            context_config: None,
        };
        // The review fixture models a builder inside a SHIPPING recipe (it
        // writes a create-pr artifact), so the snapshot carries the pr node
        // real build/planbuild shapes have. Without it the topology derivation
        // (`is_long_running_node`) reads a single-agent snapshot as a standing
        // recipe — the fixture's name-only inline schema resolves to no output
        // contract — and settles the job Idle where these tests assert the
        // completion path.
        let pr = RecipeNode {
            id: "pr".to_string(),
            node_type: RecipeNodeType::Pr,
            name: "PR".to_string(),
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
        };
        serde_json::to_string(&ExecutionSnapshot {
            continuity: Default::default(),
            branch_target: Default::default(),
            model_routing: None,
            recipe: RecipeSnapshot {
                id: "recipe".to_string(),
                name: "Recipe".to_string(),
                description: None,
                trigger: RecipeTrigger::Manual,
                nodes: vec![node, pr],
                edges: vec![],
            },
            agents: HashMap::new(),
            skills: HashMap::new(),
            trigger_context: TriggerContext {
                issue_id: Some("i-review".to_string()),
                project_id: "p-review".to_string(),
                trigger_type: TriggerType::Manual,
                event_payload: None,
                initiated_via: None,
            },
            presets: None,
            delegated_packets: vec![],
            created_at: 1,
        })
        .unwrap()
    }

    /// A single agent node with no output contract and no edges — no terminal
    /// action node and a contract-less control-terminal — the coordinator-on-main
    /// shape the topology derivation marks long-running, so it settles Idle at
    /// clean turn-end.
    pub(super) fn long_running_snapshot_json() -> String {
        let node = RecipeNode {
            id: "coordinator".to_string(),
            node_type: RecipeNodeType::Agent,
            name: "Coordinator".to_string(),
            position: NodePosition { x: 0.0, y: 0.0 },
            parent_id: None,
            trigger_config: None,
            agent_config: Some(AgentNodeConfig {
                agent_config_id: Some("coordinator".to_string()),
                output_schema: None,
                git_config: None,
            }),
            action_config: None,
            checkpoint_config: None,
            artifact_config: None,
            condition_config: None,
            decision_config: None,
            context_config: None,
        };
        serde_json::to_string(&ExecutionSnapshot {
            continuity: Default::default(),
            branch_target: Default::default(),
            model_routing: None,
            recipe: RecipeSnapshot {
                id: "recipe".to_string(),
                name: "Recipe".to_string(),
                description: None,
                trigger: RecipeTrigger::Manual,
                nodes: vec![node],
                edges: vec![],
            },
            agents: HashMap::new(),
            skills: HashMap::new(),
            trigger_context: TriggerContext {
                issue_id: Some("i-review".to_string()),
                project_id: "p-review".to_string(),
                trigger_type: TriggerType::Manual,
                event_payload: None,
                initiated_via: None,
            },
            presets: None,
            delegated_packets: vec![],
            created_at: 1,
        })
        .unwrap()
    }

    /// Build an `ExecutionSnapshot` from a bundled recipe YAML, mirroring the
    /// production snapshot build (nodes + edges carried verbatim). Agents/skills
    /// are irrelevant to the topology derivation and left empty.
    fn snapshot_from_yaml(yaml: &str) -> ExecutionSnapshot {
        let recipe = RecipeFile::from_yaml(yaml)
            .expect("bundled recipe parses")
            .into_recipe(Some("default".to_string()), None);
        ExecutionSnapshot {
            continuity: Default::default(),
            branch_target: Default::default(),
            model_routing: None,
            recipe: RecipeSnapshot {
                id: recipe.id.clone(),
                name: recipe.name.clone(),
                description: recipe.description.clone(),
                trigger: recipe.trigger.clone(),
                nodes: recipe.nodes.clone(),
                edges: recipe.edges.clone(),
            },
            agents: HashMap::new(),
            skills: HashMap::new(),
            trigger_context: TriggerContext {
                issue_id: None,
                project_id: "p".to_string(),
                trigger_type: TriggerType::Manual,
                event_payload: None,
                initiated_via: None,
            },
            presets: None,
            delegated_packets: vec![],
            created_at: 1,
        }
    }

    /// Assert `is_long_running_node` over every node of `snapshot`, expecting
    /// exactly the agent node whose `agent_config_id` equals `long_running_agent`
    /// (if any) to derive true, and every other node false. Nodes are keyed by
    /// `agent_config_id` rather than node id because `into_recipe` reassigns node
    /// ids to fresh UUIDs. `requires_output` is resolved through the real
    /// terminal-contract resolver, so the derivation is exercised end to end.
    async fn assert_recipe_derivation(
        db: &LocalDb,
        name: &str,
        snapshot: ExecutionSnapshot,
        long_running_agent: Option<&str>,
    ) {
        let name = name.to_string();
        let long_running_agent = long_running_agent.map(str::to_string);
        db.read(move |conn| {
            Box::pin(async move {
                for node in &snapshot.recipe.nodes {
                    let requires_output =
                        crate::execution::jobs::find_downstream_artifact_schema_with_snapshot_conn(
                            conn, &snapshot, &node.id,
                        )
                        .await?
                        .is_some();
                    let derived = crate::execution::jobs::is_long_running_node(
                        &snapshot,
                        &node.id,
                        requires_output,
                    );
                    let agent_config_id = node
                        .agent_config
                        .as_ref()
                        .and_then(|c| c.agent_config_id.as_deref());
                    let expected =
                        long_running_agent.is_some() && agent_config_id == long_running_agent.as_deref();
                    assert_eq!(
                        derived, expected,
                        "{name}: node `{}` (agent {agent_config_id:?}) long-running derivation (requires_output={requires_output})",
                        node.id
                    );
                }
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn long_running_derives_from_recipe_topology() {
        // The long-running fact is derived from recipe topology, not a node flag:
        // an agent node stands idle at clean turn-end iff it declares no output
        // contract, is a control-terminal, AND its recipe ships no terminal action
        // (`pr`/`action`) node. Every shipping recipe derives false; the standing
        // shape now comes from transforming the Coordinator to the base branch
        // target rather than from a second recipe file.
        //
        // Every node of every shipping recipe derives false — crucially including
        // planbuild's contract-less review sink, which is node-shape-identical to
        // the standing coordinator (a contract-less control-terminal agent) yet
        // sits in a recipe that DOES contain a `pr` node. That single recipe-level
        // fact, condition (c), is exactly what separates the two.
        //
        // `assert_recipe_derivation` checks EVERY node, so it also pins non-agent
        // nodes (triggers, artifacts, instructions) to false: without an
        // agent_config a node can never derive long-running.
        let db = test_db().await;

        let shipping: &[(&str, &str)] = &[
            (
                "planbuild",
                include_str!("../../../../../packs/core/recipes/planbuild.yaml"),
            ),
            (
                "build",
                include_str!("../../../../../packs/core/recipes/build.yaml"),
            ),
            (
                "coordinator",
                include_str!("../../../../../packs/core/recipes/coordinator.yaml"),
            ),
            (
                "task-list",
                include_str!("../../../../../packs/core/recipes/task-list.yaml"),
            ),
            (
                "setup",
                include_str!("../../../../../packs/core/recipes/setup.yaml"),
            ),
            (
                "memory-triage",
                include_str!("../../../../../packs/core/recipes/memory-triage.yaml"),
            ),
        ];
        for (name, yaml) in shipping {
            assert_recipe_derivation(&db, name, snapshot_from_yaml(yaml), None).await;
        }

        // The Coordinator transformed to the base branch target is the standing
        // shape: the transform prunes its `pr` node, leaving no terminal action
        // node, so its `coordinator` agent node is the single node that derives
        // long-running (the board artifact and trigger nodes do not).
        let mut standing = snapshot_from_yaml(include_str!(
            "../../../../../packs/core/recipes/coordinator.yaml"
        ));
        crate::execution::branch_target::apply_branch_target(
            &mut standing,
            Some(crate::models::BranchTarget::Base),
            &[
                crate::models::BranchTarget::New,
                crate::models::BranchTarget::Base,
            ],
        )
        .unwrap();
        assert_recipe_derivation(&db, "coordinator (base)", standing, Some("coordinator")).await;
    }

    #[tokio::test]
    async fn long_running_node_settles_idle_without_advancing() {
        // A topology-derived long-running coordinator ends its turn with no output contract: it derives
        // Idle (non-terminal), the execution stays running, the issue reads
        // waiting/idle, and crucially NO advance_dag fires and the job never
        // completes — the re-firing loop the bug exhibited is gone.
        let temp = tempfile::tempdir().unwrap();
        let db = test_db().await;
        let snapshot = long_running_snapshot_json();
        db.write(|conn| {
            let snapshot = snapshot.clone();
            Box::pin(async move {
                conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-review','W',1,1)", ()).await?;
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-review','w-review','P','prj','/tmp/prj',1,1)", ()).await?;
                conn.execute("INSERT INTO issues (id, project_id, number, title, status, progress, attention, created_at, updated_at) VALUES ('i-review','p-review',2,'T','active','active','none',1,1)", ()).await?;
                conn.execute("INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot) VALUES ('e-review','recipe','i-review','p-review','running',1,1,?1)", (snapshot.as_str(),)).await?;
                conn.execute("INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at) VALUES ('j-review','e-review','coordinator','i-review','p-review','running','coordinator','coordinator',1,1)", ()).await?;
                conn.execute("INSERT INTO sessions (id, job_id, backend, status, sequence, created_at, updated_at) VALUES ('s-review','j-review','codex','open',1,1,1)", ()).await?;
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, created_at, ended_at, updated_at) VALUES ('t-review','s-review','j-review',1,'complete','initial',1,2,2)", ()).await?;
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();

        let derived = db
            .read(|conn| Box::pin(async move { recompute_job_status_conn(conn, "j-review").await }))
            .await
            .unwrap();
        assert_eq!(derived, JobStatus::Idle);

        let search_index =
            Arc::new(SearchIndex::open_or_create(temp.path().join("search")).unwrap());
        let db_state = Arc::new(DbState::new(Arc::new(db), search_index));
        let services = Arc::new(TestServicesBuilder::new().build());
        let (effect_tx, _effect_rx) = tokio::sync::mpsc::unbounded_channel();
        let orch = OrchestratorBuilder::new(db_state, services, temp.path().join("config"))
            .effect_tx(Some(effect_tx))
            .build();

        recompute_job(&orch, "j-review").unwrap();

        let job_status = orch
            .db
            .local
            .query_one("SELECT status FROM jobs WHERE id = 'j-review'", (), |row| {
                row.text(0)
            })
            .await
            .unwrap();
        assert_eq!(job_status, "idle");

        let exec_status = orch
            .db
            .local
            .query_one(
                "SELECT status FROM executions WHERE id = 'e-review'",
                (),
                |row| row.text(0),
            )
            .await
            .unwrap();
        assert_eq!(exec_status, "running");

        let (issue_status, issue_attention) = orch
            .db
            .local
            .query_one(
                "SELECT status, attention FROM issues WHERE id = 'i-review'",
                (),
                |row| Ok((row.text(0)?, row.text(1)?)),
            )
            .await
            .unwrap();
        assert_eq!(issue_status, "waiting");
        assert_eq!(issue_attention, "idle");

        let advance_count = orch
            .db
            .local
            .query_one(
                "SELECT COUNT(*) FROM effect_outbox WHERE kind = 'advance_dag' AND dedupe_key = 'e-review'",
                (),
                |row| row.i64(0),
            )
            .await
            .unwrap();
        assert_eq!(advance_count, 0, "an idle node must not advance the DAG");
    }

    #[tokio::test]
    async fn pending_memory_review_allows_completion_and_advancement() {
        let temp = tempfile::tempdir().unwrap();
        let db = test_db().await;
        let snapshot = review_snapshot_json();
        db.write(|conn| {
            let snapshot = snapshot.clone();
            Box::pin(async move {
                conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-review','W',1,1)", ()).await?;
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-review','w-review','P','prj','/tmp/prj',1,1)", ()).await?;
                conn.execute("INSERT INTO issues (id, project_id, number, title, status, attention, created_at, updated_at) VALUES ('i-review','p-review',2,'T','active','none',1,1)", ()).await?;
                conn.execute("INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot) VALUES ('e-review','recipe','i-review','p-review','running',1,1,?1)", (snapshot.as_str(),)).await?;
                conn.execute("INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, uri_segment, node_name, memory_review_state, created_at, updated_at) VALUES ('j-review','e-review','builder','i-review','p-review','running','builder','builder','sent',1,1)", ()).await?;
                conn.execute("INSERT INTO sessions (id, job_id, backend, status, sequence, created_at, updated_at) VALUES ('s-review','j-review','codex','open',1,1,1)", ()).await?;
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, created_at, ended_at, updated_at) VALUES ('t-review','s-review','j-review',1,'complete','initial',1,2,2)", ()).await?;
                conn.execute("INSERT INTO artifacts (id, job_id, artifact_type, confirmed, data, version, output_name, created_at, updated_at) VALUES ('a-review','j-review','create-pr',1,'{}',1,'create-pr',1,1)", ()).await?;
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();

        let derived = db
            .read(|conn| Box::pin(async move { recompute_job_status_conn(conn, "j-review").await }))
            .await
            .unwrap();
        assert_eq!(derived, JobStatus::Complete);

        let search_index =
            Arc::new(SearchIndex::open_or_create(temp.path().join("search")).unwrap());
        let db_state = Arc::new(DbState::new(Arc::new(db), search_index));
        let services = Arc::new(TestServicesBuilder::new().build());
        let (effect_tx, _effect_rx) = tokio::sync::mpsc::unbounded_channel();
        let orch = OrchestratorBuilder::new(db_state, services, temp.path().join("config"))
            .effect_tx(Some(effect_tx))
            .build();

        recompute_job(&orch, "j-review").unwrap();

        let status = orch
            .db
            .local
            .query_one("SELECT status FROM jobs WHERE id = 'j-review'", (), |row| {
                row.text(0)
            })
            .await
            .unwrap();
        assert_eq!(status, "complete");
        let outbox_count = orch
            .db
            .local
            .query_one(
                "SELECT COUNT(*) FROM effect_outbox WHERE kind = 'advance_dag' AND dedupe_key = 'e-review'",
                (),
                |row| row.i64(0),
            )
            .await
            .unwrap();
        assert_eq!(outbox_count, 1);
    }

    #[tokio::test]
    async fn live_review_turn_keeps_job_running() {
        // A running MemoryReview turn keeps the job Running — the agent is active,
        // so the job must not be artificially blocked. Confirmation is decoupled
        // from run state (it keys off the unconfirmed artifact), so a live review
        // no longer needs the job forced Blocked. The terminal outcome still
        // derives from the work turn once the review ends (CAIRN-1576).
        let db = test_db().await;
        let snapshot = review_snapshot_json();
        db.write(|conn| {
            let snapshot = snapshot.clone();
            Box::pin(async move {
                conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-review','W',1,1)", ()).await?;
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-review','w-review','P','prj','/tmp/prj',1,1)", ()).await?;
                conn.execute("INSERT INTO issues (id, project_id, number, title, status, attention, created_at, updated_at) VALUES ('i-review','p-review',2,'T','active','none',1,1)", ()).await?;
                conn.execute("INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot) VALUES ('e-review','recipe','i-review','p-review','running',1,1,?1)", (snapshot.as_str(),)).await?;
                conn.execute("INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, uri_segment, node_name, memory_review_state, created_at, updated_at) VALUES ('j-review','e-review','builder','i-review','p-review','running','builder','builder','sent',1,1)", ()).await?;
                conn.execute("INSERT INTO sessions (id, job_id, backend, status, sequence, created_at, updated_at) VALUES ('s-review','j-review','codex','open',1,1,1)", ()).await?;
                // Work turn: completed earlier.
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, created_at, ended_at, updated_at) VALUES ('t-work','s-review','j-review',1,'complete','initial',1,2,2)", ()).await?;
                // Review turn: running, created after the work turn.
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, created_at, ended_at, updated_at) VALUES ('t-review','s-review','j-review',2,'running','memory_review',3,NULL,3)", ()).await?;
                conn.execute("INSERT INTO artifacts (id, job_id, artifact_type, confirmed, data, version, output_name, created_at, updated_at) VALUES ('a-review','j-review','create-pr',1,'{}',1,'create-pr',1,1)", ()).await?;
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();

        let derived = db
            .read(|conn| Box::pin(async move { recompute_job_status_conn(conn, "j-review").await }))
            .await
            .unwrap();
        assert_eq!(derived, JobStatus::Running);
    }

    #[tokio::test]
    async fn failed_review_turn_does_not_fail_a_confirmed_complete_job() {
        // A failed MemoryReview turn is excluded from the facts, so a job whose
        // work turn completed (and whose output is confirmed) stays Complete
        // rather than being flipped to Failed by the review turn's failure.
        let db = test_db().await;
        let snapshot = review_snapshot_json();
        db.write(|conn| {
            let snapshot = snapshot.clone();
            Box::pin(async move {
                conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-review','W',1,1)", ()).await?;
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-review','w-review','P','prj','/tmp/prj',1,1)", ()).await?;
                conn.execute("INSERT INTO issues (id, project_id, number, title, status, attention, created_at, updated_at) VALUES ('i-review','p-review',2,'T','active','none',1,1)", ()).await?;
                conn.execute("INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot) VALUES ('e-review','recipe','i-review','p-review','running',1,1,?1)", (snapshot.as_str(),)).await?;
                conn.execute("INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, uri_segment, node_name, memory_review_state, created_at, updated_at) VALUES ('j-review','e-review','builder','i-review','p-review','complete','builder','builder','done',1,1)", ()).await?;
                conn.execute("INSERT INTO sessions (id, job_id, backend, status, sequence, created_at, updated_at) VALUES ('s-review','j-review','codex','open',1,1,1)", ()).await?;
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, created_at, ended_at, updated_at) VALUES ('t-work','s-review','j-review',1,'complete','initial',1,2,2)", ()).await?;
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, created_at, ended_at, updated_at) VALUES ('t-review','s-review','j-review',2,'failed','memory_review',3,4,4)", ()).await?;
                conn.execute("INSERT INTO artifacts (id, job_id, artifact_type, confirmed, data, version, output_name, created_at, updated_at) VALUES ('a-review','j-review','create-pr',1,'{}',1,'create-pr',1,1)", ()).await?;
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();

        let derived = db
            .read(|conn| Box::pin(async move { recompute_job_status_conn(conn, "j-review").await }))
            .await
            .unwrap();
        assert_eq!(derived, JobStatus::Complete);
    }

    #[tokio::test]
    async fn interrupt_and_cancel_turns_stay_resumable_for_execution_attached_job() {
        // The execution-attached branch of `recompute_job_status_conn` must honor
        // the `JobFacts` contract: an Interrupted/Cancelled work turn is a
        // user-initiated pause, NOT a failure. The job rests resumable (derives
        // Pending) so it never cascades to downstream — matching the standalone
        // branch and the bulk sweep. Counting interrupt/cancel as failure here
        // would report Failed while the persisted bulk value stays resumable.
        let db = test_db().await;
        let snapshot = review_snapshot_json();
        db.write(|conn| {
            let snapshot = snapshot.clone();
            Box::pin(async move {
                conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-review','W',1,1)", ()).await?;
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-review','w-review','P','prj','/tmp/prj',1,1)", ()).await?;
                conn.execute("INSERT INTO issues (id, project_id, number, title, status, attention, created_at, updated_at) VALUES ('i-review','p-review',2,'T','active','none',1,1)", ()).await?;
                conn.execute("INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot) VALUES ('e-review','recipe','i-review','p-review','running',1,1,?1)", (snapshot.as_str(),)).await?;
                // Two execution-attached jobs on the same node: one paused via
                // Interrupt, one via Cancel (turn-level cancel, not the sticky
                // job-status override).
                conn.execute("INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at) VALUES ('j-int','e-review','builder','i-review','p-review','running','builder','builder',1,1)", ()).await?;
                conn.execute("INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at) VALUES ('j-cancel','e-review','builder','i-review','p-review','running','builder-2','builder',1,1)", ()).await?;
                conn.execute("INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at) VALUES ('j-retry','e-review','builder','i-review','p-review','failed','builder-3','builder',1,1)", ()).await?;
                conn.execute("INSERT INTO sessions (id, job_id, backend, status, sequence, created_at, updated_at) VALUES ('s-int','j-int','codex','open',1,1,1)", ()).await?;
                conn.execute("INSERT INTO sessions (id, job_id, backend, status, sequence, created_at, updated_at) VALUES ('s-cancel','j-cancel','codex','open',1,1,1)", ()).await?;
                conn.execute("INSERT INTO sessions (id, job_id, backend, status, sequence, created_at, updated_at) VALUES ('s-retry','j-retry','codex','open',1,1,1)", ()).await?;
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, created_at, ended_at, updated_at) VALUES ('t-int','s-int','j-int',1,'interrupted','initial',1,2,2)", ()).await?;
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, created_at, ended_at, updated_at) VALUES ('t-cancel','s-cancel','j-cancel',1,'cancelled','initial',1,2,2)", ()).await?;
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, created_at, updated_at) VALUES ('t-retry','s-retry','j-retry',1,'pending','retry',3,3)", ()).await?;
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();

        let interrupted = db
            .read(|conn| Box::pin(async move { recompute_job_status_conn(conn, "j-int").await }))
            .await
            .unwrap();
        assert_eq!(
            interrupted,
            JobStatus::Pending,
            "an interrupted work turn must rest resumable (Pending), not Failed"
        );

        let cancelled = db
            .read(|conn| Box::pin(async move { recompute_job_status_conn(conn, "j-cancel").await }))
            .await
            .unwrap();
        assert_eq!(
            cancelled,
            JobStatus::Pending,
            "a turn-cancelled work turn must rest resumable (Pending), not Failed"
        );

        let retrying = db
            .read(|conn| Box::pin(async move { recompute_job_status_conn(conn, "j-retry").await }))
            .await
            .unwrap();
        assert_eq!(
            retrying,
            JobStatus::Running,
            "a failed recipe job with a pending retry turn must derive live again"
        );
    }

    #[tokio::test]
    async fn sweep_only_preserves_running_to_pending_for_a_claimed_launch() {
        let db = test_db().await;
        let snapshot = review_snapshot_json();
        db.write(|conn| {
            let snapshot = snapshot.clone();
            Box::pin(async move {
                conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-claim','W',1,1)", ()).await?;
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-claim','w-claim','P','claim','/tmp/claim',1,1)", ()).await?;
                conn.execute("INSERT INTO issues (id, project_id, number, title, status, attention, created_at, updated_at) VALUES ('i-claim','p-claim',3,'T','active','none',1,1)", ()).await?;
                conn.execute("INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot) VALUES ('e-claim','recipe','i-claim','p-claim','running',1,1,?1)", (snapshot.as_str(),)).await?;
                conn.execute("INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at) VALUES ('j-unclaimed','e-claim','builder','i-claim','p-claim','running','builder','builder',1,1)", ()).await?;
                conn.execute("INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at) VALUES ('j-claimed','e-claim','builder','i-claim','p-claim','running','builder-2','builder',1,1)", ()).await?;
                conn.execute("INSERT INTO sessions (id, job_id, backend, status, sequence, created_at, updated_at) VALUES ('s-unclaimed','j-unclaimed','codex','open',1,1,1)", ()).await?;
                conn.execute("INSERT INTO sessions (id, job_id, backend, status, sequence, created_at, updated_at) VALUES ('s-claimed','j-claimed','codex','open',1,1,1)", ()).await?;
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, created_at, ended_at, updated_at) VALUES ('t-unclaimed','s-unclaimed','j-unclaimed',1,'cancelled','initial',1,2,2)", ()).await?;
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, created_at, ended_at, updated_at) VALUES ('t-claimed','s-claimed','j-claimed',1,'cancelled','initial',1,2,2)", ()).await?;
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();

        db.write(|conn| {
            Box::pin(async move {
                recompute_execution_jobs_conn(
                    conn,
                    "e-claim",
                    &HashSet::from(["j-claimed".to_string()]),
                )
                .await?;
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();

        let statuses = db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT id, status FROM jobs WHERE execution_id = 'e-claim' ORDER BY id",
                            (),
                        )
                        .await?;
                    let mut statuses = Vec::new();
                    while let Some(row) = rows.next().await? {
                        statuses.push((row.text(0)?, row.text(1)?));
                    }
                    Ok::<_, DbError>(statuses)
                })
            })
            .await
            .unwrap();
        assert_eq!(
            statuses,
            vec![
                ("j-claimed".to_string(), "running".to_string()),
                ("j-unclaimed".to_string(), "pending".to_string()),
            ]
        );
    }

    #[tokio::test]
    async fn stray_artifact_does_not_satisfy_missing_required_output() {
        // The Part C guarantee: a node that declares `create-pr` but only wrote
        // some other artifact must NOT be treated as having produced its output.
        let db = test_db().await;
        seed_job_with_stray_artifact(&db).await;

        let required = db
            .read(|conn| {
                Box::pin(async move { artifact_fact(conn, "j-1", Some("create-pr")).await })
            })
            .await
            .unwrap();
        assert!(
            required.is_none(),
            "a stray checkpoint artifact must not satisfy a missing create-pr output"
        );

        // The unnamed-contract fallback still sees any artifact.
        let any = db
            .read(|conn| Box::pin(async move { artifact_fact(conn, "j-1", None).await }))
            .await
            .unwrap();
        assert_eq!(
            any,
            Some(false),
            "unnamed contract falls back to any-artifact presence, unconfirmed"
        );
    }

    #[tokio::test]
    async fn named_required_artifact_satisfies_gate() {
        let db = test_db().await;
        seed_job_with_stray_artifact(&db).await;
        db.write(|conn| {
            Box::pin(async move {
                conn.execute("INSERT INTO artifacts (id, job_id, artifact_type, data, version, output_name, created_at, updated_at) VALUES ('a-pr','j-1','pull_request','{}',2,'create-pr',1,1)", ()).await?;
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();

        let present = db
            .read(|conn| {
                Box::pin(async move { artifact_fact(conn, "j-1", Some("create-pr")).await })
            })
            .await
            .unwrap();
        assert_eq!(
            present,
            Some(false),
            "the required create-pr artifact is present and unconfirmed"
        );
    }

    #[tokio::test]
    async fn standalone_job_derives_from_live_turn_and_run_facts() {
        let db = test_db().await;
        db.write(|conn| {
            Box::pin(async move {
                conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-standalone','W',1,1)", ()).await?;
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-standalone','w-standalone','P','standalone','/tmp/standalone',1,1)", ()).await?;
                conn.execute("INSERT INTO jobs (id, project_id, status, node_name, created_at, updated_at) VALUES ('j-idle','p-standalone','running','thread',1,1)", ()).await?;
                conn.execute("INSERT INTO sessions (id, job_id, backend, status, sequence, created_at, updated_at) VALUES ('s-idle','j-idle','codex','open',1,1,1)", ()).await?;
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, created_at, ended_at, updated_at) VALUES ('t-idle','s-idle','j-idle',1,'cancelled','initial',1,2,2)", ()).await?;
                conn.execute("INSERT INTO jobs (id, project_id, status, node_name, created_at, updated_at) VALUES ('j-live','p-standalone','idle','thread',1,1)", ()).await?;
                conn.execute("INSERT INTO sessions (id, job_id, backend, status, sequence, created_at, updated_at) VALUES ('s-live','j-live','codex','open',1,1,1)", ()).await?;
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, created_at, updated_at) VALUES ('t-live','s-live','j-live',1,'pending','initial',1,1)", ()).await?;
                conn.execute("INSERT INTO runs (id, job_id, project_id, status, created_at, updated_at) VALUES ('r-live','j-live','p-standalone','live',1,1)", ()).await?;
                Ok::<_, DbError>(())
            })
        }).await.unwrap();

        let idle = db
            .read(|conn| Box::pin(async move { recompute_job_status_conn(conn, "j-idle").await }))
            .await
            .unwrap();
        let running = db
            .read(|conn| Box::pin(async move { recompute_job_status_conn(conn, "j-live").await }))
            .await
            .unwrap();
        assert_eq!(idle, JobStatus::Idle);
        assert_eq!(running, JobStatus::Running);
    }

    #[tokio::test]
    async fn standalone_recompute_preserves_delegated_child_terminal_outcomes() {
        let db = test_db().await;
        db.write(|conn| {
            Box::pin(async move {
                conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-child','W',1,1)", ()).await?;
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-child','w-child','P','child','/tmp/child',1,1)", ()).await?;
                conn.execute("INSERT INTO jobs (id, project_id, status, node_name, created_at, updated_at) VALUES ('j-parent','p-child','running','parent',1,1)", ()).await?;
                conn.execute("INSERT INTO jobs (id, project_id, parent_job_id, status, node_name, created_at, updated_at) VALUES ('j-complete','p-child','j-parent','complete','child',1,1)", ()).await?;
                conn.execute("INSERT INTO jobs (id, project_id, parent_job_id, status, node_name, created_at, updated_at) VALUES ('j-failed','p-child','j-parent','failed','child',1,1)", ()).await?;
                Ok::<_, DbError>(())
            })
        }).await.unwrap();

        let complete = db
            .read(|conn| {
                Box::pin(async move { recompute_job_status_conn(conn, "j-complete").await })
            })
            .await
            .unwrap();
        let failed = db
            .read(|conn| Box::pin(async move { recompute_job_status_conn(conn, "j-failed").await }))
            .await
            .unwrap();

        assert_eq!(complete, JobStatus::Complete);
        assert_eq!(failed, JobStatus::Failed);
    }
}

#[cfg(test)]
mod port_gate_tests {
    use super::recompute_job_status_conn;
    use crate::models::{
        AgentNodeConfig, ArtifactNodeConfig, ConfirmPolicy, ExecutionSnapshot, JobStatus,
        NodePosition, RecipeEdge, RecipeEdgeType, RecipeNode, RecipeNodeType, RecipeSnapshot,
        RecipeTrigger, TriggerContext, TriggerType,
    };
    use crate::storage::{DbError, LocalDb, MigrationRunner, RowExt, TURSO_MIGRATIONS};
    use std::collections::HashMap;

    async fn test_db() -> LocalDb {
        let temp = tempfile::tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("port_gate.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        std::mem::forget(temp);
        db
    }

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
            decision_config: None,
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
                schema: Some(serde_json::json!({"type": "object"})),
                confirm_policy: policy,
                content: None,
            }),
            condition_config: None,
            decision_config: None,
            context_config: None,
        }
    }

    fn ctx_edge(id: &str, from: &str, from_handle: &str, to: &str) -> RecipeEdge {
        RecipeEdge {
            id: id.to_string(),
            edge_type: RecipeEdgeType::Context,
            source_node_id: from.to_string(),
            source_handle: from_handle.to_string(),
            target_node_id: to.to_string(),
            target_handle: "context-in".to_string(),
        }
    }

    /// planner --context-out--> plan(user) --context-out--> builder, plus a
    /// ctx-self `notes` doc the planner owns.
    fn snapshot_json() -> String {
        let snap = ExecutionSnapshot {
            continuity: Default::default(),
            branch_target: Default::default(),
            model_routing: None,
            recipe: RecipeSnapshot {
                id: "recipe-1".to_string(),
                name: "Recipe".to_string(),
                description: None,
                trigger: RecipeTrigger::Manual,
                nodes: vec![
                    agent("planner"),
                    artifact_node("plan-node", "plan", ConfirmPolicy::User),
                    agent("builder"),
                    artifact_node("notes-node", "notes", ConfirmPolicy::Auto),
                ],
                edges: vec![
                    ctx_edge("e1", "planner", "context-out", "plan-node"),
                    ctx_edge("e2", "plan-node", "context-out", "builder"),
                    ctx_edge("e3", "planner", "context-self", "notes-node"),
                ],
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
        };
        snap.to_json().unwrap()
    }

    /// Seed the FK chain + a complete planner turn. Artifacts are added by the
    /// caller to exercise the gate.
    async fn seed(db: &LocalDb) {
        let snapshot = snapshot_json();
        db.write(move |conn| {
            let snapshot = snapshot.clone();
            Box::pin(async move {
                conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-1','W',1,1)", ()).await?;
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-1','w-1','P','p','/tmp/p',1,1)", ()).await?;
                conn.execute("INSERT INTO issues (id, project_id, number, title, status, attention, created_at, updated_at) VALUES ('i-1','p-1',1,'T','active','none',1,1)", ()).await?;
                conn.execute("INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot) VALUES ('e-1','recipe-1','i-1','p-1','running',1,1,?1)", (snapshot.as_str(),)).await?;
                conn.execute("INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at) VALUES ('j-planner','e-1','planner','i-1','p-1','running','planner','planner',1,1)", ()).await?;
                conn.execute("INSERT INTO sessions (id, job_id, backend, status, sequence, created_at, updated_at) VALUES ('s-1','j-planner','codex','open',1,1,1)", ()).await?;
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, created_at, ended_at, updated_at) VALUES ('t-1','s-1','j-planner',1,'complete','initial',1,2,2)", ()).await?;
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();
    }

    async fn add_artifact(db: &LocalDb, id: &str, name: &str, confirmed: i64, version: i64) {
        let id = id.to_string();
        let name = name.to_string();
        db.write(move |conn| {
            let id = id.clone();
            let name = name.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO artifacts (id, job_id, artifact_type, confirmed, data, version, output_name, created_at, updated_at) VALUES (?1,'j-planner',?2,?3,'{}',?4,?2,1,1)",
                    (id.as_str(), name.as_str(), confirmed, version),
                )
                .await?;
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();
    }

    async fn planner_status(db: &LocalDb) -> JobStatus {
        db.read(|conn| Box::pin(async move { recompute_job_status_conn(conn, "j-planner").await }))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn user_gate_blocks_until_terminal_artifact_confirmed() {
        let db = test_db().await;
        seed(&db).await;
        // Turn complete, no plan yet -> Blocked (required output missing).
        assert_eq!(planner_status(&db).await, JobStatus::Blocked);

        // Plan written but unconfirmed under a `user` ctx-out gate -> Blocked.
        add_artifact(&db, "a-plan", "plan", 0, 1).await;
        assert_eq!(planner_status(&db).await, JobStatus::Blocked);
    }

    #[tokio::test]
    async fn user_gate_completes_when_terminal_artifact_confirmed() {
        let db = test_db().await;
        seed(&db).await;
        add_artifact(&db, "a-plan", "plan", 1, 1).await;
        assert_eq!(planner_status(&db).await, JobStatus::Complete);
    }

    #[tokio::test]
    async fn ctx_self_confirmation_does_not_resolve_terminal_gate() {
        let db = test_db().await;
        seed(&db).await;
        // Terminal plan is unconfirmed; a later, confirmed ctx-self `notes`
        // artifact must NOT flip the gate (confirmation is name-scoped). Under a
        // job-scoped latest-confirmed read this would wrongly derive Complete.
        add_artifact(&db, "a-plan", "plan", 0, 1).await;
        add_artifact(&db, "a-notes", "notes", 1, 2).await;
        assert_eq!(planner_status(&db).await, JobStatus::Blocked);
    }

    #[tokio::test]
    async fn approve_confirms_terminal_not_higher_versioned_ctx_self() {
        let db = test_db().await;
        seed(&db).await;
        // Terminal `plan` written once (v1, unconfirmed under the user gate).
        add_artifact(&db, "a-plan", "plan", 0, 1).await;
        // ctx-self `notes` patched repeatedly so its per-name chain outruns the
        // terminal in version count (v2, v3). "Latest overall" by version DESC
        // would resolve to notes, not plan.
        add_artifact(&db, "a-notes-1", "notes", 1, 2).await;
        add_artifact(&db, "a-notes-2", "notes", 1, 3).await;
        // Pre-fix this derived Blocked forever: approve confirmed the higher
        // versioned `notes` and never the terminal `plan`.
        assert_eq!(planner_status(&db).await, JobStatus::Blocked);

        let confirmed = db
            .write(|conn| {
                Box::pin(async move {
                    crate::execution::checkpoints::confirm_latest_artifact_conn(conn, "j-planner")
                        .await
                })
            })
            .await
            .unwrap();
        // Approve targets the terminal `plan`, not the higher-versioned `notes`.
        assert_eq!(confirmed.output_name.as_deref(), Some("plan"));
        // Gate now resolves -> the job completes.
        assert_eq!(planner_status(&db).await, JobStatus::Complete);
    }

    // ------------------------------------------------------------------
    // Turn-less ephemeral CALL reduction (CAIRN-2677 bug 2)
    //
    // A node-less ephemeral call establishes NO DB turn, so its job status can't
    // be derived from turn facts and used to spin `running` forever after the run
    // finalized. `reduce_delegated_child_job_conn` must derive completion from the
    // run outcome + the return artifact (the call's completion contract) instead.
    // ------------------------------------------------------------------

    /// Seed an execution snapshot carrying a materialized CallTool packet whose
    /// result job is `j-call`, plus the node-less call job, a run with
    /// `run_status`, and optionally its return artifact. NO turn is created — the
    /// exact turn-less shape the reduction must handle.
    async fn seed_call_child(db: &LocalDb, run_status: &str, has_artifact: bool) {
        let contract =
            crate::execution::delegation::resolve_delegated_output_contract(None).unwrap();
        let artifact_name = contract.artifact_name();
        let mut snap = ExecutionSnapshot {
            continuity: Default::default(),
            branch_target: Default::default(),
            model_routing: None,
            recipe: RecipeSnapshot {
                id: "recipe-1".to_string(),
                name: "R".to_string(),
                description: None,
                trigger: RecipeTrigger::Manual,
                nodes: vec![],
                edges: vec![],
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
        };
        crate::execution::delegation::create_call_packet(
            &mut snap,
            crate::execution::delegation::CreateCallPacketInput {
                parent_job_id: "j-wf",
                parent_turn_id: None,
                parent_tool_use_id: None,
                title: "call",
                problem_statement: "do it",
                agent_config_id: "Explore",
                cwd: "/tmp",
                output_contract: contract,
                result_artifact_job_id: "j-call",
                task_index: Some(0),
                tier_override: None,
                backend_preference: None,
                background: false,
            },
        );
        let snapshot = snap.to_json().unwrap();
        let run_status = run_status.to_string();
        db.write(move |conn| {
            let snapshot = snapshot.clone();
            let run_status = run_status.clone();
            let artifact_name = artifact_name.clone();
            Box::pin(async move {
                conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-1','W',1,1)", ()).await?;
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-1','w-1','P','p','/tmp/p',1,1)", ()).await?;
                conn.execute("INSERT INTO issues (id, project_id, number, title, status, attention, created_at, updated_at) VALUES ('i-1','p-1',1,'T','active','none',1,1)", ()).await?;
                conn.execute("INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot) VALUES ('e-1','recipe-1','i-1','p-1','running',1,1,?1)", (snapshot.as_str(),)).await?;
                conn.execute("INSERT INTO jobs (id, execution_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at) VALUES ('j-wf','e-1','i-1','p-1','running','flow','Flow',1,1)", ()).await?;
                conn.execute("INSERT INTO jobs (id, execution_id, parent_job_id, issue_id, project_id, status, uri_segment, node_name, agent_config_id, created_at, updated_at) VALUES ('j-call','e-1','j-wf','i-1','p-1','running','c1','Call','Explore',1,1)", ()).await?;
                conn.execute("INSERT INTO runs (id, job_id, issue_id, status, created_at, updated_at) VALUES ('r-call','j-call','i-1',?1,1,1)", (run_status.as_str(),)).await?;
                if has_artifact {
                    conn.execute("INSERT INTO artifacts (id, job_id, artifact_type, confirmed, data, version, output_name, created_at, updated_at) VALUES ('a1','j-call',?1,1,'{}',1,?1,1,1)", (artifact_name.as_str(),)).await?;
                }
                Ok::<_, DbError>(())
            })
        }).await.unwrap();
    }

    async fn reduce_call(db: &LocalDb) -> Option<JobStatus> {
        db.write(|conn| {
            Box::pin(async move { super::reduce_delegated_child_job_conn(conn, "j-call").await })
        })
        .await
        .unwrap()
    }

    async fn call_job_status(db: &LocalDb) -> Option<String> {
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query("SELECT status FROM jobs WHERE id = 'j-call'", ())
                    .await?;
                Ok(rows.next().await?.and_then(|r| r.text(0).ok()))
            })
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn turnless_call_with_artifact_completes_even_when_run_crashed() {
        // The call wrote its return artifact but its CLI stream crashed. The job
        // must terminalize Complete — the artifact IS the completion contract — not
        // spin `running` forever (CAIRN-2677 bug 2 compounding bug 1).
        let db = test_db().await;
        seed_call_child(&db, "crashed", true).await;
        assert_eq!(reduce_call(&db).await, Some(JobStatus::Complete));
        assert_eq!(call_job_status(&db).await.as_deref(), Some("complete"));
    }

    #[tokio::test]
    async fn turnless_call_with_artifact_completes_on_clean_exit() {
        let db = test_db().await;
        seed_call_child(&db, "exited", true).await;
        assert_eq!(reduce_call(&db).await, Some(JobStatus::Complete));
    }

    #[tokio::test]
    async fn turnless_call_terminal_without_artifact_fails() {
        let db = test_db().await;
        seed_call_child(&db, "exited", false).await;
        assert_eq!(reduce_call(&db).await, Some(JobStatus::Failed));
    }

    #[tokio::test]
    async fn turnless_call_with_live_run_stays_running() {
        // Still working (no artifact, run non-terminal): leave it running.
        let db = test_db().await;
        seed_call_child(&db, "live", false).await;
        assert_eq!(reduce_call(&db).await, None);
        assert_eq!(call_job_status(&db).await.as_deref(), Some("running"));
    }
}

/// A warm provider process must not stand in for a completed work turn.
///
/// Codex (and Claude holding a session open) keeps its process live/warm after a
/// terminal artifact handoff so the session can take a reply. Counting that run
/// as work liveness pinned every artifact producer `Running`, and DAG
/// advancement never dispatched the node downstream of a confirmed Plan or
/// create-PR. These tests pin both halves of the fact boundary: a warm run never
/// masks a work outcome, and it still supplies liveness in the real start/resume
/// gap where there is no outcome to mask.
#[cfg(test)]
mod warm_provider_run_tests {
    use super::{recompute_job, recompute_job_status_conn};
    use crate::db::DbState;
    use crate::execution::advancement::find_ready_action_nodes;
    use crate::models::{
        ExecutionSnapshot, JobStatus, RecipeFile, RecipeNodeType, RecipeSnapshot, TriggerContext,
        TriggerType,
    };
    use crate::orchestrator::{Orchestrator, OrchestratorBuilder};
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{
        DbError, LocalDb, MigrationRunner, RowExt, SearchIndex, TURSO_MIGRATIONS,
    };
    use cairn_db::turso::params;
    use std::collections::HashMap;
    use std::sync::Arc;

    const PLANBUILD: &str = include_str!("../../../../../packs/core/recipes/planbuild.yaml");

    /// The bundled PlanBuild recipe as an execution snapshot, plus the node ids
    /// of its two artifact producers and its PR node. `into_recipe` mints fresh
    /// node ids, so nodes are located by role rather than by the YAML's ids.
    struct Fixture {
        snapshot: ExecutionSnapshot,
        planner_node: String,
        builder_node: String,
        pr_node: String,
    }

    fn fixture() -> Fixture {
        let recipe = RecipeFile::from_yaml(PLANBUILD)
            .expect("bundled planbuild recipe parses")
            .into_recipe(Some("default".to_string()), None);
        let agent_node = |agent: &str| -> String {
            recipe
                .nodes
                .iter()
                .find(|node| {
                    node.agent_config
                        .as_ref()
                        .and_then(|cfg| cfg.agent_config_id.as_deref())
                        == Some(agent)
                })
                .unwrap_or_else(|| panic!("planbuild carries an agent node for `{agent}`"))
                .id
                .clone()
        };
        let pr_node = recipe
            .nodes
            .iter()
            .find(|node| node.node_type == RecipeNodeType::Pr)
            .expect("planbuild carries a pr node")
            .id
            .clone();
        Fixture {
            planner_node: agent_node("planner"),
            builder_node: agent_node("build"),
            pr_node,
            snapshot: ExecutionSnapshot {
                continuity: Default::default(),
                branch_target: Default::default(),
                model_routing: None,
                recipe: RecipeSnapshot {
                    id: recipe.id.clone(),
                    name: recipe.name.clone(),
                    description: recipe.description.clone(),
                    trigger: recipe.trigger.clone(),
                    nodes: recipe.nodes.clone(),
                    edges: recipe.edges.clone(),
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
            },
        }
    }

    async fn test_db(dir: &std::path::Path) -> LocalDb {
        let db = LocalDb::open(dir.join("warm_run.db")).await.unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db
    }

    /// Seed the FK chain and one PlanBuild execution carrying both agent jobs:
    /// the planner (`running`, the shape the wedge left behind) and the builder
    /// (`pending`, awaiting the planner's control-out).
    async fn seed(db: &LocalDb, fx: &Fixture) {
        let snapshot = fx.snapshot.to_json().unwrap();
        let planner_node = fx.planner_node.clone();
        let builder_node = fx.builder_node.clone();
        db.write(move |conn| {
            let snapshot = snapshot.clone();
            let planner_node = planner_node.clone();
            let builder_node = builder_node.clone();
            Box::pin(async move {
                conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-1','W',1,1)", ()).await?;
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-1','w-1','P','p','/tmp/p',1,1)", ()).await?;
                conn.execute("INSERT INTO issues (id, project_id, number, title, status, attention, created_at, updated_at) VALUES ('i-1','p-1',1,'T','active','none',1,1)", ()).await?;
                conn.execute("INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot) VALUES ('e-1','planbuild','i-1','p-1','running',1,1,?1)", (snapshot.as_str(),)).await?;
                conn.execute("INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at) VALUES ('j-planner','e-1',?1,'i-1','p-1','running','planner','Planner',1,1)", (planner_node.as_str(),)).await?;
                conn.execute("INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at) VALUES ('j-builder','e-1',?1,'i-1','p-1','pending','builder','Builder',2,2)", (builder_node.as_str(),)).await?;
                conn.execute("INSERT INTO sessions (id, job_id, backend, status, sequence, created_at, updated_at) VALUES ('s-planner','j-planner','codex','open',1,1,1)", ()).await?;
                conn.execute("INSERT INTO sessions (id, job_id, backend, status, sequence, created_at, updated_at) VALUES ('s-builder','j-builder','codex','open',1,1,1)", ()).await?;
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();
    }

    /// A turn on `job`'s session. `end_reason` is the handoff evidence the wedge
    /// hinged on: `complete` + `artifact_handoff` is a finished producer.
    async fn add_turn(
        db: &LocalDb,
        job: &str,
        id: &str,
        state: &str,
        end_reason: Option<&str>,
        created_at: i64,
    ) {
        let (job, id) = (job.to_string(), id.to_string());
        let state = state.to_string();
        let end_reason = end_reason.map(str::to_string);
        db.write(move |conn| {
            let (job, id, state) = (job.clone(), id.clone(), state.clone());
            let end_reason = end_reason.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, end_reason, created_at, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, 'initial', ?6, ?4, ?4)",
                    params![
                        id.as_str(),
                        format!("s-{}", job.trim_start_matches("j-")).as_str(),
                        job.as_str(),
                        created_at,
                        state.as_str(),
                        end_reason.as_deref()
                    ],
                )
                .await?;
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();
    }

    /// A provider run row. `live`/`starting` is what `has_active_run_conn` reads;
    /// a warm Codex process parks here long after its turn ends.
    async fn add_run(db: &LocalDb, job: &str, id: &str, status: &str) {
        let (job, id, status) = (job.to_string(), id.to_string(), status.to_string());
        db.write(move |conn| {
            let (job, id, status) = (job.clone(), id.clone(), status.clone());
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO runs (id, job_id, issue_id, project_id, status, session_id, created_at, updated_at)
                     VALUES (?1, ?2, 'i-1', 'p-1', ?3, ?4, 1, 1)",
                    params![
                        id.as_str(),
                        job.as_str(),
                        status.as_str(),
                        format!("s-{}", job.trim_start_matches("j-")).as_str()
                    ],
                )
                .await?;
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();
    }

    async fn add_artifact(db: &LocalDb, job: &str, id: &str, name: &str, confirmed: i64) {
        let (job, id, name) = (job.to_string(), id.to_string(), name.to_string());
        db.write(move |conn| {
            let (job, id, name) = (job.clone(), id.clone(), name.clone());
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO artifacts (id, job_id, artifact_type, confirmed, data, version, output_name, created_at, updated_at)
                     VALUES (?1, ?2, ?3, ?4, '{}', 1, ?3, 1, 1)",
                    params![id.as_str(), job.as_str(), name.as_str(), confirmed],
                )
                .await?;
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();
    }

    async fn set_job_status(db: &LocalDb, job: &str, status: &str) {
        let (job, status) = (job.to_string(), status.to_string());
        db.write(move |conn| {
            let (job, status) = (job.clone(), status.clone());
            Box::pin(async move {
                conn.execute(
                    "UPDATE jobs SET status = ?1 WHERE id = ?2",
                    params![status.as_str(), job.as_str()],
                )
                .await?;
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();
    }

    async fn derived(db: &LocalDb, job: &str) -> JobStatus {
        let job = job.to_string();
        db.read(move |conn| {
            let job = job.clone();
            Box::pin(async move { recompute_job_status_conn(conn, &job).await })
        })
        .await
        .unwrap()
    }

    /// The receiver is returned alongside the orchestrator only to keep the
    /// effect channel open for the life of the test.
    type Harness = (
        Orchestrator,
        tokio::sync::mpsc::UnboundedReceiver<crate::effects::types::WorkflowEffect>,
    );

    fn orchestrator(db: LocalDb, dir: &std::path::Path) -> Harness {
        let search_index = Arc::new(SearchIndex::open_or_create(dir.join("search")).unwrap());
        let db_state = Arc::new(DbState::new(Arc::new(db), search_index));
        let services = Arc::new(TestServicesBuilder::new().build());
        let (effect_tx, effect_rx) = tokio::sync::mpsc::unbounded_channel();
        let orch = OrchestratorBuilder::new(db_state, services, dir.join("config"))
            .effect_tx(Some(effect_tx))
            .build();
        (orch, effect_rx)
    }

    #[tokio::test]
    async fn warm_run_does_not_hold_a_confirmed_plan_handoff_open() {
        // The Planner handed off its Plan and the user confirmed it, but the
        // Codex process stayed warm. Pre-fix the warm run counted as a live turn,
        // the Planner read Running forever, and the Builder was never dispatched.
        let temp = tempfile::tempdir().unwrap();
        let fx = fixture();
        let db = test_db(temp.path()).await;
        seed(&db, &fx).await;
        add_turn(
            &db,
            "j-planner",
            "t-plan",
            "complete",
            Some("artifact_handoff"),
            2,
        )
        .await;
        add_artifact(&db, "j-planner", "a-plan", "plan", 1).await;
        add_run(&db, "j-planner", "r-planner", "live").await;

        assert_eq!(
            derived(&db, "j-planner").await,
            JobStatus::Complete,
            "a warm provider run must not mask a confirmed plan handoff"
        );

        let (orch, _effects) = orchestrator(db, temp.path());
        recompute_job(&orch, "j-planner").unwrap();

        let planner_status = orch
            .db
            .local
            .query_one(
                "SELECT status FROM jobs WHERE id = 'j-planner'",
                (),
                |row| row.text(0),
            )
            .await
            .unwrap();
        assert_eq!(planner_status, "complete");

        let claimed = cairn_workflow::advancement::advance_execution_impl(
            &crate::workflow_host::runtime(&orch),
            "e-1",
        )
        .unwrap();
        assert_eq!(
            claimed
                .iter()
                .map(|job| job.id.as_str())
                .collect::<Vec<_>>(),
            vec!["j-builder"],
            "a completed planner dispatches the builder"
        );

        // The fix is a derivation change, never a teardown: the warm session and
        // its backend id are exactly as the provider left them.
        let (run_status, run_session) = orch
            .db
            .local
            .query_one(
                "SELECT status, session_id FROM runs WHERE id = 'r-planner'",
                (),
                |row| Ok((row.text(0)?, row.text(1)?)),
            )
            .await
            .unwrap();
        assert_eq!(
            (run_status.as_str(), run_session.as_str()),
            ("live", "s-planner"),
            "the warm run keeps its live status and its session"
        );
        let (session_status, session_backend) = orch
            .db
            .local
            .query_one(
                "SELECT status, backend FROM sessions WHERE id = 's-planner'",
                (),
                |row| Ok((row.text(0)?, row.text(1)?)),
            )
            .await
            .unwrap();
        assert_eq!(
            (session_status.as_str(), session_backend.as_str()),
            ("open", "codex"),
            "the Codex session stays open with its backend id intact"
        );
    }

    #[tokio::test]
    async fn warm_run_does_not_hold_an_auto_confirmed_create_pr_open() {
        // Same wedge one node downstream: the Builder wrote its create-PR, the
        // `pr` target auto-confirms it, and the Codex process stayed warm. The
        // Builder must complete so the PR/Review continuation becomes ready.
        let temp = tempfile::tempdir().unwrap();
        let fx = fixture();
        let db = test_db(temp.path()).await;
        seed(&db, &fx).await;
        add_turn(
            &db,
            "j-planner",
            "t-plan",
            "complete",
            Some("artifact_handoff"),
            2,
        )
        .await;
        add_artifact(&db, "j-planner", "a-plan", "plan", 1).await;
        set_job_status(&db, "j-planner", "complete").await;
        set_job_status(&db, "j-builder", "running").await;
        add_turn(
            &db,
            "j-builder",
            "t-build",
            "complete",
            Some("artifact_handoff"),
            4,
        )
        .await;
        add_artifact(&db, "j-builder", "a-pr", "create-pr", 1).await;
        add_run(&db, "j-builder", "r-builder", "live").await;

        assert_eq!(
            derived(&db, "j-builder").await,
            JobStatus::Complete,
            "a warm provider run must not mask an auto-confirmed create-pr handoff"
        );

        let (orch, _effects) = orchestrator(db, temp.path());
        recompute_job(&orch, "j-builder").unwrap();

        let builder_status = orch
            .db
            .local
            .query_one(
                "SELECT status FROM jobs WHERE id = 'j-builder'",
                (),
                |row| row.text(0),
            )
            .await
            .unwrap();
        assert_eq!(builder_status, "complete");

        let ready = find_ready_action_nodes(orch.db.local.clone(), "e-1").unwrap();
        assert!(
            ready.iter().any(|node| node.id == fx.pr_node),
            "a completed builder makes the pr node ready to dispatch, got {:?}",
            ready
                .iter()
                .map(|n| n.node_type.as_str())
                .collect::<Vec<_>>()
        );

        let advance_count = orch
            .db
            .local
            .query_one(
                "SELECT COUNT(*) FROM effect_outbox WHERE kind = 'advance_dag' AND dedupe_key = 'e-1'",
                (),
                |row| row.i64(0),
            )
            .await
            .unwrap();
        assert_eq!(advance_count, 1, "the completed builder advances the DAG");

        let run_status = orch
            .db
            .local
            .query_one(
                "SELECT status FROM runs WHERE id = 'r-builder'",
                (),
                |row| row.text(0),
            )
            .await
            .unwrap();
        assert_eq!(run_status, "live", "the warm run is left running");
    }

    #[tokio::test]
    async fn genuinely_running_turn_with_active_run_stays_running() {
        // The other side of the boundary: real work in flight is still Running,
        // and its confirmed-but-earlier artifact does not complete it early.
        let temp = tempfile::tempdir().unwrap();
        let fx = fixture();
        let db = test_db(temp.path()).await;
        seed(&db, &fx).await;
        add_turn(&db, "j-planner", "t-plan", "running", None, 2).await;
        add_run(&db, "j-planner", "r-planner", "live").await;
        assert_eq!(derived(&db, "j-planner").await, JobStatus::Running);

        // A pending turn (queued work) with an admitted run reads the same.
        set_job_status(&db, "j-builder", "running").await;
        add_turn(&db, "j-builder", "t-build", "pending", None, 3).await;
        add_run(&db, "j-builder", "r-builder", "starting").await;
        assert_eq!(derived(&db, "j-builder").await, JobStatus::Running);
    }

    #[tokio::test]
    async fn start_and_resume_gaps_with_an_active_run_stay_running() {
        // The gap the active-run fact exists for: a run is admitted before its
        // turn row exists (start), or resumes past a user-initiated pause
        // (resume). Neither carries a work outcome, so the run still supplies
        // liveness and the job must not fall back to Pending.
        let temp = tempfile::tempdir().unwrap();
        let fx = fixture();
        let db = test_db(temp.path()).await;
        seed(&db, &fx).await;
        add_run(&db, "j-planner", "r-planner", "starting").await;
        assert_eq!(
            derived(&db, "j-planner").await,
            JobStatus::Running,
            "a start gap has no turn row yet; the admitted run is the liveness"
        );

        set_job_status(&db, "j-builder", "running").await;
        add_turn(&db, "j-builder", "t-build", "interrupted", None, 3).await;
        add_run(&db, "j-builder", "r-builder", "starting").await;
        assert_eq!(
            derived(&db, "j-builder").await,
            JobStatus::Running,
            "an interrupted turn is a pause, not an outcome: the resume run is live"
        );
    }
}

/// The terminal-edge emit (CAIRN-4756).
///
/// The defect these pin was a level-triggered announcement: the sweep emitted
/// `Resolved` for every touched issue that *read* terminal, so any sweep over an
/// execution whose issue had merged long ago re-announced that merge. Startup
/// recovery settles stale jobs across old executions, which turned every rebuild
/// into a replay of the whole resolved tail at the watching coordinators.
#[cfg(test)]
mod resolution_edge_tests {
    use super::artifact_present_tests::long_running_snapshot_json;
    use super::recompute_execution_jobs;
    use crate::db::DbState;
    use crate::orchestrator::{Orchestrator, OrchestratorBuilder};
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{
        DbError, LocalDb, MigrationRunner, RowExt, SearchIndex, TURSO_MIGRATIONS,
    };
    use cairn_db::turso::params;
    use std::path::Path;
    use std::sync::Arc;

    const CHILD_URI: &str = "cairn://p/res/2";

    async fn test_db(dir: &Path) -> Arc<LocalDb> {
        let db = LocalDb::open(dir.join("resolution-edge.db")).await.unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        Arc::new(db)
    }

    /// A coordinator on a parent issue, watching a child issue whose execution
    /// carries one long-running node. The node's turn is complete and its job
    /// row still says `running` — the stale shape a host that died mid-flight
    /// leaves behind, and exactly what startup recovery settles.
    async fn seed(db: &LocalDb) {
        let snapshot = long_running_snapshot_json();
        db.write(move |conn| {
            let snapshot = snapshot.clone();
            Box::pin(async move {
                conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w','W',1,1)", ()).await?;
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p','w','P','res','/tmp/res',1,1)", ()).await?;
                conn.execute("INSERT INTO issues (id, project_id, number, title, status, progress, attention, created_at, updated_at) VALUES ('parent','p',1,'Parent','active','active','none',1,1)", ()).await?;
                conn.execute("INSERT INTO issues (id, project_id, number, title, status, progress, attention, parent_issue_id, created_at, updated_at) VALUES ('child','p',2,'Child','active','active','none','parent',1,1)", ()).await?;
                conn.execute("INSERT INTO jobs (id, project_id, issue_id, uri_segment, node_name, status, created_at, updated_at) VALUES ('coordinator','p','parent','coordinator','coordinator','running',1,1)", ()).await?;
                conn.execute("INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot) VALUES ('e-child','recipe','child','p','running',1,1,?1)", (snapshot.as_str(),)).await?;
                conn.execute("INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at) VALUES ('j-child','e-child','coordinator','child','p','running','coordinator','coordinator',1,1)", ()).await?;
                conn.execute("INSERT INTO sessions (id, job_id, backend, status, sequence, created_at, updated_at) VALUES ('s-child','j-child','codex','open',1,1,1)", ()).await?;
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, created_at, ended_at, updated_at) VALUES ('t-child','s-child','j-child',1,'complete','initial',1,2,2)", ()).await?;
                conn.execute("INSERT INTO wake_subscriptions (id, job_id, source_kind, source_ref, state, created_by, created_at, updated_at, one_shot) VALUES ('sub','coordinator','issue',?1,'active','agent',1,1,0)", (CHILD_URI,)).await?;
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();
    }

    type Boot = (
        Orchestrator,
        tokio::sync::mpsc::UnboundedReceiver<crate::effects::types::WorkflowEffect>,
    );

    /// One host lifetime over `db`. Two of these against the same store is what
    /// a rebuild looks like from the database's point of view.
    fn boot(db: Arc<LocalDb>, dir: &Path, name: &str) -> Boot {
        let search =
            Arc::new(SearchIndex::open_or_create(dir.join(format!("search-{name}"))).unwrap());
        let db_state = Arc::new(DbState::new(db, search));
        let services = Arc::new(TestServicesBuilder::new().build());
        let (effect_tx, effect_rx) = tokio::sync::mpsc::unbounded_channel();
        let orch = OrchestratorBuilder::new(db_state, services, dir.join(format!("config-{name}")))
            .effect_tx(Some(effect_tx))
            .build();
        (orch, effect_rx)
    }

    async fn resolved_push_count(db: &LocalDb) -> i64 {
        db.query_one(
            "SELECT COUNT(*) FROM attention_pushes WHERE key = ?1",
            params![format!("resolved:{CHILD_URI}")],
            |row| row.i64(0),
        )
        .await
        .unwrap()
    }

    async fn text_of(db: &LocalDb, sql: &str) -> String {
        db.query_one(sql, (), |row| row.text(0)).await.unwrap()
    }

    /// The stale job is settled again, the way a second crash-and-recover leaves
    /// it. Without this the second sweep would have nothing to do, and a sweep
    /// that does nothing cannot demonstrate anything.
    async fn restore_stale_job(db: &LocalDb) {
        db.execute(
            "UPDATE jobs SET status = 'running' WHERE id = 'j-child'",
            (),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn a_sweep_over_an_already_resolved_child_announces_nothing() {
        let temp = tempfile::tempdir().unwrap();
        let db = test_db(temp.path()).await;
        seed(&db).await;
        // Merged long ago, with its projection already settled: nothing about
        // this issue is news.
        db.execute(
            "UPDATE issues SET merged_at = 100, status = 'merged', progress = 'merged' WHERE id = 'child'",
            (),
        )
        .await
        .unwrap();

        let (orch, _effects) = boot(db.clone(), temp.path(), "only");
        recompute_execution_jobs(&orch, "e-child").unwrap();

        // A long-running node rests Idle only until its issue resolves; on a
        // merged issue its completed turn settles it Complete.
        assert_eq!(
            text_of(&db, "SELECT status FROM jobs WHERE id = 'j-child'").await,
            "complete",
            "the sweep must really settle the stale job, or it proves nothing"
        );
        assert_eq!(
            resolved_push_count(&db).await,
            0,
            "an issue that was already resolved must not be announced again"
        );
    }

    #[tokio::test]
    async fn the_sweep_that_resolves_a_child_announces_it_once() {
        let temp = tempfile::tempdir().unwrap();
        let db = test_db(temp.path()).await;
        seed(&db).await;
        // Merged just now: `issues::crud::merge` stamps the instant, and this
        // sweep is the one that projects it.
        db.execute("UPDATE issues SET merged_at = 100 WHERE id = 'child'", ())
            .await
            .unwrap();

        let (orch, _effects) = boot(db.clone(), temp.path(), "only");
        recompute_execution_jobs(&orch, "e-child").unwrap();

        assert_eq!(
            text_of(&db, "SELECT status FROM issues WHERE id = 'child'").await,
            "merged"
        );
        assert_eq!(
            resolved_push_count(&db).await,
            1,
            "the sweep that crosses the edge owns the announcement"
        );
    }

    /// The acceptance criterion: a rebuild must not replay a resolution the
    /// coordinator already received.
    ///
    /// Both halves of the repair are load-bearing here and neither alone is
    /// enough to reproduce the defect. The edge stops the second sweep emitting
    /// at all; the projection's idempotence keeps `issues.updated_at` still, so
    /// even a level-triggered emit would carry the same generation and be
    /// deduplicated. Reproducing the original replay takes the pre-fix code
    /// entire.
    #[tokio::test]
    async fn a_second_boot_does_not_replay_a_resolution_the_first_delivered() {
        let temp = tempfile::tempdir().unwrap();
        let db = test_db(temp.path()).await;
        seed(&db).await;
        db.execute("UPDATE issues SET merged_at = 100 WHERE id = 'child'", ())
            .await
            .unwrap();

        {
            let (first, _effects) = boot(db.clone(), temp.path(), "boot-1");
            recompute_execution_jobs(&first, "e-child").unwrap();
        }
        assert_eq!(
            resolved_push_count(&db).await,
            1,
            "the boot that resolves the child announces it"
        );

        // The coordinator ran and received it. From here supersede-by-key can no
        // longer absorb a repeat: a second emit means a second notification.
        db.execute(
            "UPDATE attention_pushes SET delivered_event_id = 'delivered' WHERE delivered_event_id IS NULL",
            (),
        )
        .await
        .unwrap();
        restore_stale_job(&db).await;
        // Two host lifetimes are separated in time, and every timestamp here has
        // second resolution. Without the gap both boots would stamp the same
        // second, the old `updated_at` fingerprint would match by coincidence,
        // and the test would report success against the very code it exists to
        // catch.
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;

        {
            let (second, _effects) = boot(db.clone(), temp.path(), "boot-2");
            recompute_execution_jobs(&second, "e-child").unwrap();
        }

        assert_eq!(
            text_of(&db, "SELECT status FROM jobs WHERE id = 'j-child'").await,
            "complete",
            "the second boot must really settle the stale job again"
        );
        assert_eq!(
            resolved_push_count(&db).await,
            1,
            "a rebuild must not re-announce a resolution the coordinator already had"
        );
    }
}

/// A merged or closed issue settles every job on it to its recorded outcome or
/// Cancelled, and says nothing while doing so.
#[cfg(test)]
mod terminal_issue_tests {
    use super::{recompute_execution_jobs_conn, recompute_job_status_conn};
    use crate::models::JobStatus;
    use crate::storage::{DbError, LocalDb};
    use cairn_db::turso::params;
    use cairn_workflow::advancement::ChangeCause;
    use std::collections::HashSet;

    fn snapshot_json() -> String {
        serde_json::json!({
            "recipe": {
                "id": "r", "name": "R", "description": null, "trigger": "manual",
                "nodes": [
                    {"id": "builder", "nodeType": "agent", "name": "Builder", "position": {"x": 0.0, "y": 0.0}},
                    {"id": "planner", "nodeType": "agent", "name": "Planner", "position": {"x": 0.0, "y": 0.0}},
                    {"id": "shipper", "nodeType": "agent", "name": "Shipper", "position": {"x": 0.0, "y": 0.0}},
                    {"id": "pr", "nodeType": "pr", "name": "PR", "position": {"x": 1.0, "y": 0.0}}
                ],
                "edges": []
            },
            "agents": {}, "skills": {},
            "triggerContext": {"issueId": "i", "projectId": "p", "triggerType": "manual"},
            "delegatedPackets": [], "createdAt": 1
        })
        .to_string()
    }

    /// One execution on issue `i` holding:
    /// - `builder`: cached running, its only work turn interrupted (no outcome);
    /// - `planner`: cached complete, its work turn complete;
    /// - `shipper`: completed its work, then had a post-resolution launch
    ///   refused — a later cancelled follow-up turn, and the job left `running`
    ///   by the leak (the cairn/4908 shape);
    /// - `child`: a node-less delegated child of the builder, cached running
    ///   with no turn of its own.
    async fn seed(db: &LocalDb, issue_status: &str) {
        let snapshot = snapshot_json();
        let issue_status = issue_status.to_string();
        db.write(|conn| {
            let snapshot = snapshot.clone();
            let issue_status = issue_status.clone();
            Box::pin(async move {
                conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w','W',1,1)", ()).await?;
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p','w','P','p','/tmp/p',1,1)", ()).await?;
                conn.execute(
                    "INSERT INTO issues (id, project_id, number, title, status, progress, attention, merged_at, created_at, updated_at)
                     VALUES ('i','p',1,'I',?1,?1,'none',CASE WHEN ?1 = 'merged' THEN 1 END,1,1)",
                    params![issue_status.as_str()],
                )
                .await?;
                conn.execute("INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot) VALUES ('e','r','i','p','running',1,1,?1)", params![snapshot.as_str()]).await?;
                for (job, node, status) in [
                    ("builder", Some("builder"), "running"),
                    ("planner", Some("planner"), "complete"),
                    ("shipper", Some("shipper"), "running"),
                ] {
                    conn.execute(
                        "INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, uri_segment, node_name, current_session_id, created_at, updated_at)
                         VALUES (?1,'e',?2,'i','p',?3,?1,?1,'s-' || ?1,1,1)",
                        params![job, node, status],
                    )
                    .await?;
                    conn.execute(
                        "INSERT INTO sessions (id, job_id, backend, status, sequence, created_at, updated_at) VALUES ('s-' || ?1, ?1, 'claude', 'closed', 1, 1, 1)",
                        params![job],
                    )
                    .await?;
                }
                conn.execute("INSERT INTO jobs (id, execution_id, parent_job_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at) VALUES ('child','e','builder','i','p','running','child','child',1,1)", ()).await?;
                // Node-less children owe the fixed `result` artifact.
                for job in ["child-done", "child-missing", "call-done"] {
                    conn.execute(
                        "INSERT INTO jobs (id, execution_id, parent_job_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at) VALUES (?1,'e','builder','i','p','running',?1,?1,1,1)",
                        params![job],
                    )
                    .await?;
                }
                for job in ["child-done", "child-missing"] {
                    conn.execute(
                        "INSERT INTO sessions (id, job_id, backend, status, sequence, created_at, updated_at) VALUES ('s-' || ?1, ?1, 'claude', 'closed', 1, 1, 1)",
                        params![job],
                    )
                    .await?;
                    conn.execute(
                        "INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, created_at, ended_at, updated_at) VALUES ('t-' || ?1, 's-' || ?1, ?1, 1, 'complete', 'initial', 1, 2, 2)",
                        params![job],
                    )
                    .await?;
                }
                for job in ["child-done", "call-done"] {
                    conn.execute(
                        "INSERT INTO artifacts (id, job_id, artifact_type, data, version, output_name, created_at, updated_at) VALUES ('a-' || ?1, ?1, 'result', '{}', 1, 'result', 1, 1)",
                        params![job],
                    )
                    .await?;
                }
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, created_at, ended_at, updated_at) VALUES ('t-builder','s-builder','builder',1,'interrupted','initial',1,2,2)", ()).await?;
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, created_at, ended_at, updated_at) VALUES ('t-planner','s-planner','planner',1,'complete','initial',1,2,2)", ()).await?;
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, created_at, ended_at, updated_at) VALUES ('t-shipper-1','s-shipper','shipper',1,'complete','initial',1,2,2)", ()).await?;
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, end_reason, created_at, ended_at, updated_at) VALUES ('t-shipper-2','s-shipper','shipper',2,'cancelled','follow_up','launch_refused',3,3,3)", ()).await?;
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();
    }

    async fn status_of(db: &LocalDb, job_id: &str) -> String {
        db.query_opt_text("SELECT status FROM jobs WHERE id = ?1", params![job_id])
            .await
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn sweep_on_a_merged_issue_settles_every_job_quietly() {
        let db = crate::storage::migrated_test_db("terminal-sweep.db").await;
        seed(&db, "merged").await;

        // A launch claim on the builder must not hold it Running: nothing on a
        // resolved issue is a real launch.
        let claimed: HashSet<String> = ["builder".to_string()].into_iter().collect();
        let outcome = db
            .write(|conn| {
                let claimed = claimed.clone();
                Box::pin(async move { recompute_execution_jobs_conn(conn, "e", &claimed).await })
            })
            .await
            .unwrap();

        assert_eq!(status_of(&db, "builder").await, "cancelled");
        assert_eq!(status_of(&db, "planner").await, "complete");
        assert_eq!(
            status_of(&db, "shipper").await,
            "complete",
            "a refused post-merge turn must not erase the completion the PR merged"
        );
        assert_eq!(
            status_of(&db, "child").await,
            "cancelled",
            "node-less children settle too"
        );
        assert_eq!(
            status_of(&db, "child-done").await,
            "complete",
            "a child that completed with its owed artifact is Complete"
        );
        assert_eq!(
            status_of(&db, "child-missing").await,
            "cancelled",
            "a completed turn without the owed artifact is not a completion"
        );
        assert_eq!(
            status_of(&db, "call-done").await,
            "complete",
            "a turn-less call completes on its artifact alone"
        );
        assert!(
            db.query_opt_i64("SELECT completed_at FROM jobs WHERE id = 'builder'", ())
                .await
                .unwrap()
                .is_some(),
            "settling stamps completed_at"
        );

        assert!(!outcome.changes.is_empty());
        for change in &outcome.changes {
            assert_eq!(change.cause, ChangeCause::IssueResolved, "{change:?}");
            assert!(
                !change.is_announced(),
                "a settlement onto a resolved issue never announces, advances, or wakes"
            );
        }
        assert!(outcome.newly_terminal_issue_ids.is_empty());
    }

    #[tokio::test]
    async fn sweep_on_an_active_issue_keeps_the_ordinary_rule() {
        let db = crate::storage::migrated_test_db("terminal-sweep-active.db").await;
        seed(&db, "active").await;

        let outcome = db
            .write(|conn| {
                Box::pin(
                    async move { recompute_execution_jobs_conn(conn, "e", &HashSet::new()).await },
                )
            })
            .await
            .unwrap();

        // An interrupted or cancelled latest turn rests resumable on an open
        // issue; the latest-outcome reading belongs to resolved issues only.
        assert_eq!(status_of(&db, "builder").await, "pending");
        assert_eq!(status_of(&db, "shipper").await, "pending");
        assert_eq!(status_of(&db, "child").await, "running");
        assert!(outcome
            .changes
            .iter()
            .all(|change| change.cause == ChangeCause::Derived));
    }

    #[tokio::test]
    async fn single_job_derivation_settles_node_less_children_on_a_resolved_issue() {
        let db = crate::storage::migrated_test_db("terminal-single.db").await;
        seed(&db, "merged").await;
        let derived = db
            .read(|conn| Box::pin(async move { recompute_job_status_conn(conn, "child").await }))
            .await
            .unwrap();
        assert_eq!(derived, JobStatus::Cancelled);
    }
}
