use crate::models::{
    AgentGitConfig, AgentNodeConfig, BranchMode, DelegatedOutputContract, DelegatedStatus,
    DelegatedWorkPacket, ExecutionSnapshot, NodePosition, RecipeEdge, RecipeEdgeType, RecipeNode,
    RecipeNodeType,
};
use crate::storage::{DbError, LocalDb, RowExt};
use cairn_db::turso::params;
use std::{collections::HashMap, path::PathBuf, sync::Arc};

mod delegation;

use delegation::*;

#[cfg(test)]
mod tests {
    use super::*;

    // ---- reparent_delegated_jobs disambiguation -------------------------------

    async fn migrated_db() -> LocalDb {
        let temp = tempfile::tempdir().unwrap();
        let db = LocalDb::open_migrated_for_test(temp.keep().join("dag-reparent-test.db"))
            .await
            .unwrap();
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at) \
                     VALUES ('proj', 'default', 'Proj', 'proj', '/tmp/proj', 0, 0)",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
        db
    }

    async fn insert_job(
        db: &LocalDb,
        id: &str,
        recipe_node_id: Option<&str>,
        parent_job_id: Option<&str>,
        uri_segment: Option<&str>,
    ) {
        let id = id.to_string();
        let recipe_node_id = recipe_node_id.map(str::to_string);
        let parent_job_id = parent_job_id.map(str::to_string);
        let uri_segment = uri_segment.map(str::to_string);
        db.write(move |conn| {
            let id = id.clone();
            let recipe_node_id = recipe_node_id.clone();
            let parent_job_id = parent_job_id.clone();
            let uri_segment = uri_segment.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO jobs(id, status, project_id, created_at, updated_at, \
                     recipe_node_id, parent_job_id, uri_segment, branch, base_branch) \
                     VALUES (?1, 'pending', 'proj', 0, 0, ?2, ?3, ?4, ?5, 'main')",
                    params![
                        id.as_str(),
                        recipe_node_id.as_deref(),
                        parent_job_id.as_deref(),
                        uri_segment.as_deref(),
                        format!("agent/{id}")
                    ],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    async fn job_row(db: &LocalDb, id: &str) -> (Option<String>, Option<String>) {
        let id = id.to_string();
        db.read(move |conn| {
            let id = id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT uri_segment, parent_job_id FROM jobs WHERE id = ?1",
                        (id.as_str(),),
                    )
                    .await?;
                let row = rows.next().await?.unwrap();
                Ok((row.opt_text(0)?, row.opt_text(1)?))
            })
        })
        .await
        .unwrap()
    }

    fn packet(
        id: &str,
        parent: &str,
        node_id: &str,
        title: &str,
        task_index: i32,
    ) -> DelegatedWorkPacket {
        serde_json::from_value(serde_json::json!({
            "id": id,
            "parentJobId": parent,
            "origin": "task_tool",
            "title": title,
            "problemStatement": "x",
            "agentConfigId": "Explore",
            "ownership": { "cwd": "/tmp" },
            "outputContract": { "schemaType": "return" },
            "status": "materialized",
            "materializedNodeIds": [node_id],
            "taskIndex": task_index,
            "createdAt": 0
        }))
        .unwrap()
    }

    /// A batch whose task titles collide with an existing child of the same node
    /// must disambiguate with -N suffixes (not violate the unique index) and
    /// re-parent every job (no top-level sibling leakage). This is the exact
    /// failure that sank the first attempt.
    #[tokio::test]
    async fn reparent_disambiguates_against_existing_children() {
        let db = migrated_db().await;
        insert_job(&db, "parent", None, None, Some("builder")).await;
        insert_job(&db, "existing", None, Some("parent"), Some("explore")).await;
        // New jobs start top-level with stale colliding segments.
        insert_job(&db, "new-1", Some("node-1"), None, Some("explore")).await;
        insert_job(&db, "new-2", Some("node-2"), None, Some("explore")).await;

        let packets = vec![
            packet("pkt-1", "parent", "node-1", "Explore", 0),
            packet("pkt-2", "parent", "node-2", "Explore", 1),
        ];
        let ordered = vec![
            ("new-1".to_string(), Some("node-1".to_string())),
            ("new-2".to_string(), Some("node-2".to_string())),
        ];
        reparent_delegated_jobs(&db, ordered, packets)
            .await
            .unwrap();

        let (seg1, parent1) = job_row(&db, "new-1").await;
        let (seg2, parent2) = job_row(&db, "new-2").await;
        assert_eq!(parent1.as_deref(), Some("parent"));
        assert_eq!(parent2.as_deref(), Some("parent"));
        let mut segs = vec![seg1.unwrap(), seg2.unwrap()];
        segs.sort();
        assert_eq!(segs, vec!["explore-2".to_string(), "explore-3".to_string()]);
    }

    /// Two jobs in the same batch with identical titles must disambiguate against
    /// each other via the per-parent reserved set.
    #[tokio::test]
    async fn reparent_disambiguates_within_batch() {
        let db = migrated_db().await;
        insert_job(&db, "parent", None, None, Some("builder")).await;
        insert_job(&db, "a", Some("node-a"), None, Some("explore")).await;
        insert_job(&db, "b", Some("node-b"), None, Some("explore")).await;

        let packets = vec![
            packet("pkt-a", "parent", "node-a", "Explore", 0),
            packet("pkt-b", "parent", "node-b", "Explore", 1),
        ];
        let ordered = vec![
            ("a".to_string(), Some("node-a".to_string())),
            ("b".to_string(), Some("node-b".to_string())),
        ];
        reparent_delegated_jobs(&db, ordered, packets)
            .await
            .unwrap();

        let (seg_a, _) = job_row(&db, "a").await;
        let (seg_b, _) = job_row(&db, "b").await;
        let mut segs = vec![seg_a.unwrap(), seg_b.unwrap()];
        segs.sort();
        assert_eq!(segs, vec!["explore".to_string(), "explore-2".to_string()]);
    }

    async fn job_link(db: &LocalDb, id: &str) -> (Option<String>, Option<i64>) {
        let id = id.to_string();
        db.read(move |conn| {
            let id = id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT parent_tool_use_id, task_index FROM jobs WHERE id = ?1",
                        (id.as_str(),),
                    )
                    .await?;
                let row = rows.next().await?.unwrap();
                Ok((row.opt_text(0)?, row.opt_i64(1)?))
            })
        })
        .await
        .unwrap()
    }

    fn packet_with_tool_use(
        id: &str,
        parent: &str,
        node_id: &str,
        title: &str,
        task_index: i32,
        parent_tool_use_id: &str,
    ) -> DelegatedWorkPacket {
        serde_json::from_value(serde_json::json!({
            "id": id,
            "parentJobId": parent,
            "parentToolUseId": parent_tool_use_id,
            "origin": "task_tool",
            "title": title,
            "problemStatement": "x",
            "agentConfigId": "Explore",
            "ownership": { "cwd": "/tmp" },
            "outputContract": { "schemaType": "return" },
            "status": "materialized",
            "materializedNodeIds": [node_id],
            "taskIndex": task_index,
            "createdAt": 0
        }))
        .unwrap()
    }

    /// Child jobs spawned by one change→tasks call must carry the originating
    /// tool-use id (shared across the batch) and their task_index, so the
    /// transcript can locate them via `list_child_jobs` ordered by task_index.
    /// Without the parent_tool_use_id write the column stays NULL and the live
    /// task windows can never resolve — the core CAIRN-1149 linking fix.
    #[tokio::test]
    async fn reparent_links_children_by_parent_tool_use_id() {
        let db = migrated_db().await;
        insert_job(&db, "parent", None, None, Some("builder")).await;
        insert_job(&db, "child-0", Some("node-0"), None, Some("explore")).await;
        insert_job(&db, "child-1", Some("node-1"), None, Some("build")).await;

        let packets = vec![
            packet_with_tool_use("pkt-0", "parent", "node-0", "Explore", 0, "toolu_change_1"),
            packet_with_tool_use("pkt-1", "parent", "node-1", "Build", 1, "toolu_change_1"),
        ];
        let ordered = vec![
            ("child-0".to_string(), Some("node-0".to_string())),
            ("child-1".to_string(), Some("node-1".to_string())),
        ];
        reparent_delegated_jobs(&db, ordered, packets)
            .await
            .unwrap();

        let (tool_use_0, idx_0) = job_link(&db, "child-0").await;
        let (tool_use_1, idx_1) = job_link(&db, "child-1").await;
        assert_eq!(tool_use_0.as_deref(), Some("toolu_change_1"));
        assert_eq!(tool_use_1.as_deref(), Some("toolu_change_1"));
        assert_eq!(idx_0, Some(0));
        assert_eq!(idx_1, Some(1));
    }

    /// A single-task change append persists the link just the same.
    #[tokio::test]
    async fn reparent_links_single_child() {
        let db = migrated_db().await;
        insert_job(&db, "parent", None, None, Some("builder")).await;
        insert_job(&db, "child", Some("node-0"), None, Some("explore")).await;

        let packets = vec![packet_with_tool_use(
            "pkt",
            "parent",
            "node-0",
            "Explore",
            0,
            "toolu_solo",
        )];
        let ordered = vec![("child".to_string(), Some("node-0".to_string()))];
        reparent_delegated_jobs(&db, ordered, packets)
            .await
            .unwrap();

        let (tool_use, idx) = job_link(&db, "child").await;
        assert_eq!(tool_use.as_deref(), Some("toolu_solo"));
        assert_eq!(idx, Some(0));
    }

    // ── CAIRN-2197: team-execution advancement + display route to the replica ──

    use crate::db::DbState;
    use crate::models::{RecipeSnapshot, RecipeTrigger, TriggerContext, TriggerType};
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::SearchIndex;

    /// Minimal snapshot: trigger -> agent("builder"), so the agent job is ready
    /// to claim the moment the DAG advances.
    fn team_exec_snapshot() -> String {
        let trigger = RecipeNode {
            id: "trigger-1".to_string(),
            node_type: RecipeNodeType::Trigger,
            name: "Trigger".to_string(),
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
        let builder = RecipeNode {
            id: "builder".to_string(),
            node_type: RecipeNodeType::Agent,
            name: "builder".to_string(),
            position: NodePosition { x: 200.0, y: 0.0 },
            parent_id: None,
            trigger_config: None,
            agent_config: Some(AgentNodeConfig {
                agent_config_id: Some("builder".to_string()),
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
        let edge = RecipeEdge {
            id: "edge-1".to_string(),
            edge_type: RecipeEdgeType::Control,
            source_node_id: "trigger-1".to_string(),
            source_handle: "control-out".to_string(),
            target_node_id: "builder".to_string(),
            target_handle: "control-in".to_string(),
        };
        let snap = ExecutionSnapshot {
            continuity: Default::default(),
            branch_target: Default::default(),
            model_routing: None,
            recipe: RecipeSnapshot {
                id: "recipe-1".to_string(),
                name: "Recipe".to_string(),
                description: None,
                trigger: RecipeTrigger::Manual,
                nodes: vec![trigger, builder],
                edges: vec![edge],
            },
            agents: HashMap::new(),
            skills: HashMap::new(),
            trigger_context: TriggerContext {
                issue_id: Some("team1~00000000-0000-4000-8000-000000000004".to_string()),
                project_id: "p-team".to_string(),
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

    async fn seed_team_execution(db: &LocalDb) {
        let snapshot = team_exec_snapshot();
        db.write(move |conn| {
            let snapshot = snapshot.clone();
            Box::pin(async move {
                conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-team','W',1,1)", ()).await?;
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-team','w-team','Team','TEAM','/tmp/team-repo',1,1)", ()).await?;
                conn.execute("INSERT INTO issues (id, project_id, number, title, status, attention, created_at, updated_at) VALUES ('team1~00000000-0000-4000-8000-000000000004','p-team',1,'T','active','none',1,1)", ()).await?;
                conn.execute("INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot) VALUES ('team1~00000000-0000-4000-8000-000000000003','recipe-1','team1~00000000-0000-4000-8000-000000000004','p-team','running',1,1,?1)", (snapshot.as_str(),)).await?;
                conn.execute("INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at) VALUES ('j-builder','team1~00000000-0000-4000-8000-000000000003','builder','team1~00000000-0000-4000-8000-000000000004','p-team','pending','builder','builder',1,1)", ()).await?;
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();
    }

    async fn job_status(db: &LocalDb, job_id: &str) -> String {
        let job_id = job_id.to_string();
        db.read(move |conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query("SELECT status FROM jobs WHERE id = ?1", (job_id.as_str(),))
                    .await?;
                rows.next().await?.unwrap().text(0)
            })
        })
        .await
        .unwrap()
    }

    /// CAIRN-2197: a team execution lives ONLY in its synced replica. The effect
    /// driver must resolve that owning database to advance the DAG — it used to
    /// read the private DB and fail with "Execution has no snapshot", so the
    /// execution never advanced and no jobs ran. The display readers must resolve
    /// the same replica so the execution shows on the issue.
    #[tokio::test(flavor = "current_thread")]
    async fn team_execution_advances_and_displays_from_replica() {
        let temp = tempfile::tempdir().unwrap();
        let config_dir = temp.path().join("config");
        std::fs::create_dir_all(&config_dir).unwrap();

        let local = Arc::new(crate::storage::migrated_test_db("cairn2197-local.db").await);
        let team = Arc::new(crate::storage::migrated_test_db("cairn2197-team.db").await);
        seed_team_execution(&team).await;

        let index =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        let dbs = Arc::new(DbState::new(local, index));
        dbs.insert_team_db_for_test("team1", team.clone()).await;
        // The team execution AND the issue carry their team prefix, so routing
        // parses each back to the replica. The project row stays bare (no
        // resolver parses it in this test).
        let e_team = "team1~00000000-0000-4000-8000-000000000003";

        let services = Arc::new(TestServicesBuilder::new().build());
        let orch = OrchestratorBuilder::new(dbs, services, config_dir).build();

        // Precondition: the execution exists ONLY in the team replica.
        assert!(
            orch.db
                .local
                .query_text("SELECT id FROM executions WHERE id = ?1", (e_team,))
                .await
                .unwrap()
                .is_none(),
            "execution must live only in the team replica, not the private DB"
        );

        // Advance: previously errored with "Execution has no snapshot"; now the
        // DAG advances and claims the pending agent job.
        let agent_jobs =
            crate::execution::advancement::advance_execution_with_actions(&orch, e_team)
                .await
                .expect("team execution must advance without 'no snapshot'");
        assert!(
            agent_jobs.iter().any(|j| j.id == "j-builder"),
            "the pending agent job must be claimed ready: {:?}",
            agent_jobs.iter().map(|j| j.id.clone()).collect::<Vec<_>>()
        );
        assert_eq!(job_status(&team, "j-builder").await, "running");

        // Display: the execution-snapshot reader routes to the replica.
        let exec_db = crate::execution::routing::owning_db_for_execution(&orch.db, e_team)
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&exec_db, &team));
        assert!(
            crate::execution::queries::get_execution_snapshot(exec_db, e_team)
                .unwrap()
                .is_some()
        );

        // Display: the issue-overview execution readers route to the replica.
        let issue_db = crate::issues::crud::owning_db_for_issue(
            &orch.db,
            "team1~00000000-0000-4000-8000-000000000004",
        )
        .await
        .unwrap();
        assert!(Arc::ptr_eq(&issue_db, &team));
        assert!(crate::execution::queries::get_execution_for_issue(
            issue_db.clone(),
            "team1~00000000-0000-4000-8000-000000000004"
        )
        .unwrap()
        .is_some());
        assert_eq!(
            crate::execution::queries::list_executions_for_issue(
                issue_db,
                "team1~00000000-0000-4000-8000-000000000004"
            )
            .unwrap()
            .len(),
            1
        );
    }
}
