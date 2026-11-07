use super::*;
use crate::db::DbState;
use crate::jj::tests::{git, git_stdout, init_project, rebase_recording_conflict};
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::base_advance::{held_replay_source, held_replay_source_cut};
use crate::orchestrator::conflict_session::{load_active_session, load_pending_replay_request};
use crate::orchestrator::Orchestrator;
use crate::services::testing::TestServicesBuilder;
use crate::storage::SearchIndex;
use cairn_common::uri::CairnResource;
use std::sync::Arc;

const CHILD: &str = "agent/child";
const GRANDCHILD: &str = "agent/grandchild";

struct Fixture {
    _home: tempfile::TempDir,
    repo: tempfile::TempDir,
    store: std::path::PathBuf,
    jj: crate::jj::JjEnv,
    db: Arc<LocalDb>,
    orch: Orchestrator,
    cut: String,
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
            repo,
            store,
            jj,
            db,
            orch,
            cut,
            source,
            flagged,
            grandchild,
            base,
        };
        // A legacy-flagged child can be held with no machine-proven source cut.
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
              held_replay_source_cut,held_replay_source_tip,replay_request_id,replay_requested_at,replay_requested_tip,replay_requested_destination,replay_resolution)
             VALUES (?1,?2,?3,'completed',1,1,'content_conflict',?4,?3,?5,?3,'open','not_materialized',?4,?3,?6,1,?3,?5,'plain')",
            params![intent, branch, tip, cut, destination, format!("request-{intent}")],
        ).await.unwrap();
    }

    fn tip(&self) -> String {
        crate::jj::bookmark_commit(&self.jj, &self.store, CHILD).unwrap()
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

    async fn request(&self, dry_run: bool, expected: &str) -> Result<String, String> {
        let item = ChangeItem {
            target: "cairn://p/proj/1/1/child/rebase".into(),
            mode: ChangeMode::Patch,
            payload: Some(serde_json::json!({
                "action":"rebuild", "source_commit":self.source, "source_base":self.cut,
                "source_base_reason":"fixture original isolated mint", "expected_tip":expected,
            })),
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
            node_id: "child".into(),
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

#[tokio::test]
#[serial_test::serial(jj)]
async fn rebuild_acceptance_retires_only_parent_metadata_even_after_export_or_database_failure() {
    for failure in ["none", "export", "database"] {
        let f = Fixture::new().await;
        let before = f.metadata(CHILD).await;
        let grandchild = f.metadata(GRANDCHILD).await;
        assert!(load_active_session(&f.db, CHILD).await.unwrap().is_some());
        let error = held_replay_source_cut(&f.orch, &f.db, &f.jj, &f.store, GRANDCHILD, CHILD)
            .await
            .unwrap_err();
        assert!(error.contains("waits for immediate parent"), "{error}");
        assert!(f
            .request(true, &f.flagged)
            .await
            .unwrap()
            .contains("Would rebuild"));
        assert_eq!(f.metadata(CHILD).await, before);
        assert_eq!(f.tip(), f.flagged);
        let error = f.request(false, &f.source).await.unwrap_err();
        assert!(error.contains("branch moved"), "{error}");
        assert_eq!(f.metadata(CHILD).await, before);
        assert_eq!(f.tip(), f.flagged);
        if failure == "export" {
            git(
                f.repo.path(),
                &["update-ref", &format!("refs/heads/{CHILD}"), &f.base],
            );
        }
        if failure == "database" {
            FAIL_ACCEPTANCE.with(|flag| flag.set(true));
        }
        let result = f.request(false, &f.flagged).await;
        let rebuilt = f.tip();
        assert_ne!(rebuilt, f.flagged);
        if failure == "database" {
            assert!(result.unwrap_err().contains("metadata acceptance failed"));
            assert_eq!(
                f.metadata(CHILD).await,
                before,
                "database failure rolls all metadata back"
            );
            f.request(false, &f.flagged).await.unwrap();
        } else if failure == "export" {
            assert!(result
                .unwrap_err()
                .contains("Metadata accepted, but Git export failed"));
        } else {
            result.unwrap();
        }
        assert!(
            held_replay_source(&f.db, &f.store, CHILD)
                .await
                .unwrap()
                .is_none(),
            "including NULL-cut receipts"
        );
        assert!(load_active_session(&f.db, CHILD).await.unwrap().is_none());
        assert_eq!(
            f.db.query_opt_text(
                "SELECT resolution_state FROM jj_reconcile_items WHERE intent_id='old-c'",
                ()
            )
            .await
            .unwrap()
            .as_deref(),
            Some("superseded")
        );
        assert!(load_pending_replay_request(&f.db, &f.store, CHILD)
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            held_replay_source_cut(&f.orch, &f.db, &f.jj, &f.store, GRANDCHILD, CHILD)
                .await
                .unwrap()
                .as_deref(),
            Some(f.source.as_str())
        );
        assert_eq!(
            f.db.query_opt_text("SELECT base_commit FROM jobs WHERE id='c'", ())
                .await
                .unwrap()
                .as_deref(),
            Some(f.base.as_str())
        );
        f.assert_descendant_and_observers_unchanged(&grandchild)
            .await;
        if failure == "export" {
            git(
                f.repo.path(),
                &["update-ref", &format!("refs/heads/{CHILD}"), &f.source],
            );
        }
        f.request(false, &f.flagged).await.unwrap();
        assert_eq!(f.tip(), rebuilt, "retry never creates another native head");
        f.assert_descendant_and_observers_unchanged(&grandchild)
            .await;
    }
}

#[tokio::test]
#[serial_test::serial(jj)]
async fn rebuild_retry_preserves_a_new_parent_hold_not_covered_by_the_original_result() {
    let f = Fixture::new().await;
    let grandchild = f.metadata(GRANDCHILD).await;
    FAIL_ACCEPTANCE.with(|flag| flag.set(true));
    assert!(f
        .request(false, &f.flagged)
        .await
        .unwrap_err()
        .contains("metadata acceptance failed"));
    let rebuilt = f.tip();
    std::fs::write(f.repo.path().join("new-parent.txt"), "new P2 work\n").unwrap();
    git(f.repo.path(), &["add", "-A"]);
    git(f.repo.path(), &["commit", "-q", "-m", "new P2"]);
    let p2 = git_stdout(f.repo.path(), &["rev-parse", "HEAD"]);
    f.jj.run(
        &f.store,
        &["git", "import", "--ignore-working-copy"],
        "import P2",
    )
    .unwrap();
    f.db.execute("UPDATE jj_reconcile_items SET held_replay_source_cut=NULL,held_replay_source_tip=NULL,resolution_state='superseded' WHERE bookmark=?1", (CHILD,)).await.unwrap();
    f.install("new-c", CHILD, "main", &p2, Some(&f.base), &rebuilt)
        .await;
    let newer = f
        .metadata(CHILD)
        .await
        .into_iter()
        .find(|item| item.intent == "new-c")
        .unwrap();
    f.request(false, &f.flagged).await.unwrap();
    assert_eq!(f.tip(), rebuilt);
    assert!(
        f.metadata(CHILD).await.contains(&newer),
        "an export-only retry cannot discharge P2"
    );
    assert_eq!(
        held_replay_source(&f.db, &f.store, CHILD)
            .await
            .unwrap()
            .unwrap()
            .destination,
        p2
    );
    assert!(load_active_session(&f.db, CHILD).await.unwrap().is_some());
    assert!(load_pending_replay_request(&f.db, &f.store, CHILD)
        .await
        .unwrap()
        .is_some());
    f.assert_descendant_and_observers_unchanged(&grandchild)
        .await;
}
