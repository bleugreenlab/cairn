use super::*;
pub use cairn_workflow::effects::dag::delegation::*;
use cairn_workflow::effects::dag::delegation::{
    delegated_agent_node_for_test as delegated_agent_node,
    schema_config_from_output_contract_for_test as schema_config_from_output_contract,
};

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
            conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-1','w-1','P','p','/tmp/p',1,1)", ()).await?;
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

    #[tokio::test]
    async fn branchless_parent_delegates_on_its_base_coordinate() {
        let db = test_db().await;
        db.write(|conn| Box::pin(async move {
            conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-1','W',1,1)", ()).await?;
            conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-1','w-1','P','p','/tmp/p',1,1)", ()).await?;
            conn.execute("INSERT INTO issues (id, project_id, number, title, status, attention, created_at, updated_at) VALUES ('i-1','p-1',1,'T','active','none',1,1)", ()).await?;
            conn.execute("INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq) VALUES ('e-1','default','i-1','p-1','running',1,1)", ()).await?;
            conn.execute("INSERT INTO jobs (id, execution_id, issue_id, project_id, status, branch, base_commit, base_branch, uri_segment, node_name, created_at, updated_at) VALUES ('thread-job','e-1','i-1','p-1','running',NULL,'main-head','main','thread','Thread',1,1)", ()).await?;
            conn.execute("INSERT INTO jobs (id, execution_id, issue_id, project_id, recipe_node_id, status, uri_segment, node_name, created_at, updated_at) VALUES ('child-job','e-1','i-1','p-1','delegated-agent','pending','child','Child',1,1)", ()).await?;
            Ok::<_, DbError>(())
        })).await.unwrap();

        reparent_delegated_jobs(
            &db,
            vec![("child-job".into(), Some("delegated-agent".into()))],
            vec![packet(
                "packet-1",
                "thread-job",
                "Explore",
                "delegated-agent",
            )],
        )
        .await
        .unwrap();

        let coordinate = child_coordinate(&db, "child-job").await;
        assert_eq!(coordinate.0.as_deref(), Some("thread-job"));
        assert_eq!(coordinate.1, None, "a thread task must not invent a branch");
        assert_eq!(coordinate.2.as_deref(), Some("main-head"));
        assert_eq!(coordinate.3.as_deref(), Some("main"));
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
            conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-1','w-1','P','p','/tmp/p',1,1)", ()).await?;
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

        let jj = crate::jj::environment(&bin, home.path());
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
                adopt_head: None,
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

    #[test]
    #[serial_test::serial(jj)]
    fn a_branchless_parents_task_starts_on_the_live_base_branch() {
        let Some(fx) = parent_ahead_of_main() else {
            eprintln!("skipping a_branchless_parents_task_starts_on_the_live_base_branch: no jj");
            return;
        };
        let stale_recorded_base = fx.parent_head.clone();
        let resolved_base = fx.main_tip.clone();

        let (branch, base_commit) = select_job_coordinate(
            &delegated_behavior(),
            CoordinateRequest {
                adopt_head: None,
                job_id: "child-job",
                parent_job_id: Some("thread-job"),
                existing_branch: None,
                base_ref: "main",
            },
            &fx.jj,
            &fx.store,
            |_| {
                Ok(ParentCoordinate {
                    branch: None,
                    recorded_base: Some(stale_recorded_base),
                })
            },
            || unreachable!("a delegated task never mints a branch"),
            move |revision| (revision == "main").then(|| resolved_base.clone()),
        )
        .unwrap();

        assert_eq!(branch, None, "the task preserves branchless lineage");
        assert_eq!(base_commit.as_deref(), Some(fx.main_tip.as_str()));
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
                adopt_head: None,
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
                decision_config: None,
                context_config: None,
            },
            "recipe-1",
        ));

        let (_, base_commit) = select_job_coordinate(
            &authored,
            CoordinateRequest {
                adopt_head: None,
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
