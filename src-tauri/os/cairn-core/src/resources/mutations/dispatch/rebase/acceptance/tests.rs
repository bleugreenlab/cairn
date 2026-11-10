use super::*;
use crate::db::DbState;
use crate::jj::tests::{git, git_stdout, init_project, rebase_recording_conflict};
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::base_advance::preparation::held_by_parent_conflict;
use crate::orchestrator::Orchestrator;
use crate::services::testing::TestServicesBuilder;
use crate::storage::SearchIndex;
use cairn_uri::CairnResource;
use std::sync::Arc;

const CHILD: &str = "agent/child";
const GRANDCHILD: &str = "agent/grandchild";

struct Fixture {
    _home: tempfile::TempDir,
    _repo: tempfile::TempDir,
    store: std::path::PathBuf,
    jj: crate::jj::JjEnv,
    db: Arc<LocalDb>,
    orch: Orchestrator,
    source: String,
    flagged: String,
    grandchild: String,
    base: String,
}

impl Fixture {
    async fn new() -> Self {
        let bin = crate::jj::tests::resolve_jj().expect("provisioned real jj required");
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        let p = repo.path();
        init_project(p);
        let cut = git_stdout(p, &["rev-parse", "HEAD"]);
        git(p, &["checkout", "-q", "-b", CHILD]);
        std::fs::write(p.join("shared.rs"), "child\n").unwrap();
        git(p, &["add", "-A"]);
        git(p, &["commit", "-q", "-m", "own child delta"]);
        let source = git_stdout(p, &["rev-parse", "HEAD"]);
        git(p, &["checkout", "-q", "-b", GRANDCHILD]);
        std::fs::write(p.join("grandchild.txt"), "grandchild own work\n").unwrap();
        git(p, &["add", "-A"]);
        git(p, &["commit", "-q", "-m", "own grandchild delta"]);
        let grandchild = git_stdout(p, &["rev-parse", "HEAD"]);
        git(p, &["checkout", "-q", "-b", "bad", &cut]);
        std::fs::write(p.join("shared.rs"), "obsolete parent\n").unwrap();
        git(p, &["add", "-A"]);
        git(p, &["commit", "-q", "-m", "bad parent"]);
        let obsolete_parent = git_stdout(p, &["rev-parse", "HEAD"]);
        git(p, &["checkout", "-q", "main"]);
        std::fs::write(p.join("incoming.txt"), "current parent\n").unwrap();
        git(p, &["add", "-A"]);
        git(p, &["commit", "-q", "-m", "current parent"]);
        let base = git_stdout(p, &["rev-parse", "HEAD"]);
        let jj = crate::jj::environment(&bin, home.path());
        let store = crate::jj::project_store_dir(home.path(), p);
        crate::jj::ensure_project_store(&jj, &store, p).unwrap();
        rebase_recording_conflict(&jj, &store, CHILD, "bad");
        let flagged = crate::jj::bookmark_commit(&jj, &store, CHILD).unwrap();
        jj.run(
            &store,
            &[
                "bookmark",
                "set",
                GRANDCHILD,
                "-r",
                &grandchild,
                "--allow-backwards",
                "--ignore-working-copy",
            ],
            "hold original grandchild",
        )
        .unwrap();
        assert!(crate::jj::branch_has_conflict(&jj, &store, CHILD).unwrap());
        crate::jj::require_clean_tree(&jj, &store, GRANDCHILD, "fixture held grandchild").unwrap();

        let db = Arc::new(crate::storage::migrated_test_db("rebuild-acceptance.db").await);
        let repo_sql = p.to_string_lossy().replace('\'', "''");
        db.execute_script(format!(
            "INSERT INTO projects (id,workspace_id,name,key,repo_path,default_branch,created_at,updated_at)
             VALUES ('p','default','Project','proj','{repo_sql}','main',1,1);
             INSERT INTO issues (id,project_id,number,title,status,created_at,updated_at)
             VALUES ('i','p',1,'Issue','active',1,1);
             INSERT INTO executions (id,recipe_id,issue_id,project_id,status,started_at,seq)
             VALUES ('e','recipe','i','p','running',1,1);
             INSERT INTO jobs (id,execution_id,recipe_node_id,issue_id,project_id,status,branch,base_branch,base_commit,uri_segment,created_at,updated_at)
             VALUES ('c','e','c','i','p','running','{CHILD}','main','{cut}','child',1,1),
                    ('g','e','g','i','p','running','{GRANDCHILD}','{CHILD}','{source}','grandchild',1,1),
                    ('observer','e','observer','i','p','running','{CHILD}','main','observer-base','observer',1,1);
             INSERT INTO jobs (id,execution_id,parent_job_id,issue_id,project_id,status,branch,base_branch,base_commit,uri_segment,created_at,updated_at)
             VALUES ('task','e','c','i','p','running','{CHILD}','main','task-base','task',1,1);"
        )).await.unwrap();
        let mut snapshot: crate::models::ExecutionSnapshot = serde_json::from_str(
            &crate::test_support::shipping_execution_snapshot_json("i", "p"),
        )
        .unwrap();
        let prototype = snapshot.recipe.nodes[0].clone();
        snapshot.recipe.nodes = ["c", "g", "observer"]
            .into_iter()
            .map(|id| {
                let mut node = prototype.clone();
                node.id = id.into();
                node.agent_config.as_mut().unwrap().git_config =
                    Some(crate::models::AgentGitConfig {
                        branch_mode: if id == "observer" {
                            crate::models::BranchMode::Inherit
                        } else {
                            crate::models::BranchMode::Isolate
                        },
                        ..Default::default()
                    });
                node
            })
            .collect();
        db.execute(
            "UPDATE executions SET snapshot=?1 WHERE id='e'",
            (serde_json::to_string(&snapshot).unwrap(),),
        )
        .await
        .unwrap();
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(
                db.clone(),
                Arc::new(SearchIndex::open_or_create(home.path().join("search")).unwrap()),
            )),
            Arc::new(TestServicesBuilder::new().build()),
            home.path().to_path_buf(),
        )
        .jj_binary_path(&bin)
        .build();
        let f = Self {
            _home: home,
            _repo: repo,
            store,
            jj,
            db,
            orch,
            source,
            flagged,
            grandchild,
            base,
        };
        // The child's session records an obsolete parent the current base no
        // longer carries.
        assert!(!crate::jj::revset_descends_from(
            &f.jj,
            &f.store,
            &f.base,
            &obsolete_parent
        ));
        f.install(
            "old-c",
            CHILD,
            "removed-parent",
            &obsolete_parent,
            None,
            &f.flagged,
        )
        .await;
        f.install(
            "old-g",
            GRANDCHILD,
            CHILD,
            &f.flagged,
            Some(&f.source),
            &f.grandchild,
        )
        .await;
        f
    }

    async fn install(
        &self,
        intent: &str,
        branch: &str,
        base_branch: &str,
        destination: &str,
        cut: Option<&str>,
        tip: &str,
    ) {
        self.db.execute(
            "INSERT INTO jj_reconcile_intents (id,project_id,store_path,target_branch,destination_commit,trigger_sources_json,status,created_at,updated_at)
             VALUES (?1,'p',?2,?3,?4,'[]','completed',1,1)",
            params![intent, self.store.to_string_lossy().as_ref(), base_branch, destination],
        ).await.unwrap();
        self.db.execute(
            "INSERT INTO jj_reconcile_items
             (intent_id,bookmark,observed_tip,status,updated_at,diagnostic_version,conflict_condition,base_commit,ours_commit,theirs_commit,conflicted_tip,resolution_state,marker_state,
              replay_request_id,replay_requested_at,replay_requested_tip,replay_requested_destination,replay_resolution)
             VALUES (?1,?2,?3,'completed',1,1,'content_conflict',?4,?3,?5,?3,'open','not_materialized',?6,1,?3,?5,'plain')",
            params![intent, branch, tip, cut, destination, format!("request-{intent}")],
        ).await.unwrap();
    }

    async fn metadata(&self, branch: &str) -> Vec<Item> {
        let branch = branch.to_string();
        self.db
            .read(|conn| {
                let path = self.store.to_string_lossy().into_owned();
                let branch = branch.clone();
                Box::pin(async move { items(conn, &path, &branch).await })
            })
            .await
            .unwrap()
    }

    async fn rebuild_node(
        &self,
        node_id: &str,
        payload: serde_json::Value,
        dry_run: bool,
    ) -> Result<String, String> {
        let item = ChangeItem {
            target: format!("cairn://p/proj/1/1/{node_id}/rebase"),
            mode: ChangeMode::Patch,
            payload: Some(payload),
        };
        let request = McpCallbackRequest {
            thread_id: None,
            cwd: String::new(),
            run_id: None,
            tool: "write".into(),
            payload: serde_json::Value::Null,
            tool_use_id: None,
            answer_bound_ms: None,
            origin: None,
        };
        let resource = CairnResource::NodeRebase {
            project: "proj".into(),
            number: 1,
            exec_seq: 1,
            node_id: node_id.into(),
        };
        super::super::dispatch(&self.orch, &request, 0, &item, dry_run, &resource)
            .await
            .map_err(|error| format!("{error:?}"))?
            .ok_or_else(|| "missing rebuild dispatch".into())
    }

    async fn assert_descendant_and_observers_unchanged(&self, grandchild: &[Item]) {
        assert_eq!(self.metadata(GRANDCHILD).await, grandchild);
        assert_eq!(
            crate::jj::bookmark_commit(&self.jj, &self.store, GRANDCHILD).as_deref(),
            Some(self.grandchild.as_str())
        );
        for (id, expected) in [("task", "task-base"), ("observer", "observer-base")] {
            assert_eq!(
                self.db
                    .query_opt_text("SELECT base_commit FROM jobs WHERE id=?1", (id,))
                    .await
                    .unwrap()
                    .as_deref(),
                Some(expected)
            );
        }
    }
}

/// Rebuild reads the branch's one hold source: a branch whose immediate parent
/// has an open conflict session is refused, preview included, and nothing it
/// owns moves; once the parent's session is no longer open the same read
/// releases it with nothing recorded to clean up.
#[tokio::test]
#[serial_test::serial(jj)]
async fn rebuild_refuses_a_branch_held_behind_its_parents_open_conflict_session() {
    let f = Fixture::new().await;
    let child = f.metadata(CHILD).await;
    let grandchild = f.metadata(GRANDCHILD).await;
    let payload = serde_json::json!({
        "action":"rebuild", "source_commit":f.grandchild, "source_base":f.source,
        "source_base_reason":"fixture grandchild mint", "expected_tip":f.grandchild,
    });
    for dry_run in [true, false] {
        let error = f
            .rebuild_node("grandchild", payload.clone(), dry_run)
            .await
            .unwrap_err();
        assert!(
            error.contains("held behind its parent `agent/child`") && error.contains("old-c"),
            "{error}"
        );
        assert_eq!(f.metadata(CHILD).await, child);
        f.assert_descendant_and_observers_unchanged(&grandchild)
            .await;
    }
    f.db.execute(
        "UPDATE jj_reconcile_items SET resolution_state='resolved' WHERE bookmark=?1",
        (CHILD,),
    )
    .await
    .unwrap();
    assert_eq!(
        held_by_parent_conflict(&f.db, &f.store, GRANDCHILD, CHILD)
            .await
            .unwrap(),
        None
    );
    if let Err(error) = f.rebuild_node("grandchild", payload, true).await {
        assert!(!error.contains("held behind"), "{error}");
    }
}
