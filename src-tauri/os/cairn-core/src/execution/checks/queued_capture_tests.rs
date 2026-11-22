use super::write_check_stack_headroom::{commit, passed_batch};
use super::*;
use crate::db::DbState;
use crate::jj::tests::{git, init_project, jj_bin};
use crate::services::testing::TestServicesBuilder;
use crate::stack_headroom::within_budget;
use crate::storage::{RowExt, SearchIndex};
use cairn_common::executor_protocol::ExecutorMessage;
use tokio::sync::{mpsc, oneshot};

const FOREIGN: &str = "agent/foreign";
const OLD_CONTRACT: &str = "checks:\n  unit:\n    command: echo old-contract\n    when: review\n";
const NEW_CONTRACT: &str = "checks:\n  unit:\n    command: echo new-contract\n    when: review\n";

#[test]
#[serial_test::serial(jj)]
fn a_queued_manual_check_stays_on_its_capture_while_branch_base_and_contract_move() {
    let Some(bin) = jj_bin() else {
        return;
    };
    let home = tempfile::tempdir().unwrap();
    let project = tempfile::tempdir().unwrap();
    let repo = project.path().to_path_buf();
    init_project(&repo);
    commit(
        &repo,
        &[(".cairn/config.yaml", OLD_CONTRACT)],
        "declare unit",
    );
    git(&repo, &["checkout", "-q", "-b", FOREIGN]);
    let foreign_before = commit(&repo, &[("foreign.rs", "foreign\n")], "foreign work");
    git(&repo, &["checkout", "-q", "main"]);
    let base = commit(&repo, &[("main.rs", "main\n")], "main moves first");

    within_budget("queued manual check capture", move || async move {
        let config_dir = home.path().to_path_buf();
        let jj = crate::jj::environment(&bin, &config_dir);
        let store = {
            let (jj, config_dir, repo) = (jj.clone(), config_dir.clone(), repo.clone());
            tokio::task::spawn_blocking(move || {
                crate::jj::coordinate_repository(&jj, &config_dir, &repo)
            })
            .await
            .unwrap()
            .unwrap()
        };
        let local = LocalDb::open_migrated_for_test(config_dir.join("checks.db"))
            .await
            .unwrap();
        let repo_path = repo.to_string_lossy().into_owned();
        local
            .execute(
                "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES ('ws','w',1,1)",
                (),
            )
            .await
            .unwrap();
        local
            .execute(
                "INSERT INTO projects(id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
                     VALUES ('proj','ws','P','p',?1,'main',1,1)",
                (repo_path.as_str(),),
            )
            .await
            .unwrap();
        for statement in [
            "INSERT INTO jobs(id, project_id, branch, status, created_at, updated_at)
                 VALUES ('job','proj','agent/requester','running',1,1)",
            "INSERT INTO jobs(id, project_id, branch, status, created_at, updated_at)
                 VALUES ('other','proj','agent/foreign','running',1,1)",
            "INSERT INTO runs(id, project_id, job_id, created_at, updated_at)
                 VALUES ('run','proj','job',1,1)",
        ] {
            local.execute(statement, ()).await.unwrap();
        }
        let search = Arc::new(SearchIndex::open_or_create(config_dir.join("search")).unwrap());
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(Arc::new(local), search)),
            Arc::new(TestServicesBuilder::new().build()),
            config_dir.clone(),
        )
        .jj_binary_path(bin.clone())
        .build();

        // The colocated machine holds the offer (the check is queued) until
        // the test lets it claim, then reports what it was granted.
        let (sender, mut executor) = mpsc::unbounded_channel();
        let generation = orch.fleet.attach_announced_executor(sender);
        let fleet = orch.fleet.clone();
        let (offered_tx, offered) = oneshot::channel::<()>();
        let (go, go_rx) = oneshot::channel::<()>();
        let (granted_tx, granted) = oneshot::channel();
        let machine = cairn_common::task::spawn(async move {
            let entry_id = loop {
                match executor.recv().await {
                    Some(ExecutorMessage::Offer { entry_id, .. }) => break entry_id,
                    Some(_) => continue,
                    None => panic!("the check is offered to the colocated executor"),
                }
            };
            offered_tx.send(()).unwrap();
            go_rx.await.unwrap();
            fleet.handle_executor_message(
                crate::fleet::COLOCATED_EXECUTOR_ID,
                generation,
                ExecutorMessage::Claim { entry_id },
            );
            let (request, batch) = loop {
                match executor.recv().await {
                    Some(ExecutorMessage::Granted {
                        request,
                        batch: Some(batch),
                        ..
                    }) => break (request, batch),
                    Some(_) => continue,
                    None => panic!("the claimed check is granted with its batch"),
                }
            };
            let outcome = passed_batch(&request, &batch.items[0].header, generation);
            let materialized = (
                request.base_commit.clone(),
                serde_json::to_string(&batch).unwrap(),
            );
            fleet.handle_executor_message(
                crate::fleet::COLOCATED_EXECUTOR_ID,
                generation,
                ExecutorMessage::Result {
                    request_id: request.request_id.clone(),
                    attempt_id: request.attempt_id.clone(),
                    outcome,
                },
            );
            granted_tx.send(materialized).unwrap();
        });

        let revision = classify_manual_check_revision(&orch, "job", FOREIGN)
            .await
            .unwrap();
        assert!(
            matches!(revision, ManualCheckRevision::Managed(_)),
            "{revision:?}"
        );
        let check_orch = orch.clone();
        let check = cairn_common::task::spawn(async move {
            run_manual_configured_check_with_progress(
                &check_orch,
                "job",
                ManualCheckRequest {
                    check_name: "unit",
                    revision,
                    placement: None,
                    retry: false,
                    check_request: None,
                },
                Arc::new(|_| {}),
            )
            .await
        });

        offered.await.expect("the captured check reaches the queue");
        let captured = cairn_vcs::resolve_coordinate(&store, FOREIGN)
            .await
            .unwrap();
        assert_ne!(captured, foreign_before, "the capture prepared the branch");
        let captured_tree = crate::jj::logical_tree_hash(&jj, &repo, &captured).unwrap();

        // While it is queued: the base advances with a new contract and a
        // new file, and the branch name moves off the captured head.
        git(&repo, &["checkout", "-q", "main"]);
        let moved_base = commit(
            &repo,
            &[
                (".cairn/config.yaml", NEW_CONTRACT),
                ("moved.rs", "moved\n"),
            ],
            "base moves while the check is queued",
        );
        crate::jj::ensure_project_store(&jj, &store, &repo).unwrap();
        jj.run(
            &store,
            &[
                "--ignore-working-copy",
                "bookmark",
                "set",
                FOREIGN,
                "--allow-backwards",
                "-r",
                &foreign_before,
            ],
            "move the branch while the check is queued",
        )
        .unwrap();
        assert_eq!(
            cairn_vcs::resolve_coordinate(&store, "main").await.unwrap(),
            moved_base
        );
        assert_eq!(
            cairn_vcs::resolve_coordinate(&store, FOREIGN)
                .await
                .unwrap(),
            foreign_before
        );

        go.send(()).unwrap();
        let (materialized_commit, batch) = granted.await.unwrap();
        let result = check.await.unwrap().expect("the queued check completes");
        machine.await.unwrap();

        assert_eq!(
            materialized_commit, captured,
            "the cell checks out the captured head"
        );
        assert!(
            batch.contains("old-contract"),
            "the captured contract runs: {batch}"
        );
        assert!(
            !batch.contains("new-contract"),
            "the moved contract leaked in: {batch}"
        );
        assert!(
            batch.contains("foreign.rs"),
            "the captured change rides: {batch}"
        );
        assert!(
            !batch.contains("moved.rs"),
            "the base's later file leaked in: {batch}"
        );

        assert!(result.passed);
        assert_eq!(
            result.commit_sha, captured,
            "the result names the captured head"
        );
        assert_eq!(result.tree_hash, captured_tree);
        let capture = result
            .capture
            .clone()
            .expect("the result reports its capture");
        assert_eq!(
            capture.head_before.as_deref(),
            Some(foreign_before.as_str())
        );
        assert_eq!(capture.head_after, captured);
        assert_eq!(capture.base_branch.as_deref(), Some("main"));
        assert_eq!(capture.base_commit.as_deref(), Some(base.as_str()));
        assert!(result.observation_ref.is_some(), "the verdict was recorded");

        let (input_hash, tree) = (result.input_hash.clone(), captured_tree.clone());
        let recorded: i64 = orch
            .db
            .local
            .read(move |conn| {
                let (input_hash, tree) = (input_hash.clone(), tree.clone());
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT COUNT(*) FROM check_result_cache
                                  WHERE check_name = 'unit' AND tree_hash = ?1 AND input_hash = ?2",
                            (tree.as_str(), input_hash.as_str()),
                        )
                        .await?;
                    rows.next().await?.unwrap().i64(0)
                })
            })
            .await
            .unwrap();
        assert_eq!(
            recorded, 1,
            "the verdict is keyed by the captured tree and input"
        );
    });
}
