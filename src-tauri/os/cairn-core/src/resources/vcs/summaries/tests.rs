use super::*;

#[test]
fn distinct_names_include_shared_bases_only_once() {
    let rows = vec![
        BranchRow {
            issue: 1,
            branch: "a".into(),
            base: "main".into(),
        },
        BranchRow {
            issue: 2,
            branch: "a".into(),
            base: "main".into(),
        },
        BranchRow {
            issue: 3,
            branch: "b".into(),
            base: "a".into(),
        },
    ];
    assert_eq!(
        branch_names(&rows),
        BTreeSet::from(["a".into(), "b".into(), "main".into()])
    );
}

fn answer(ahead: usize, behind: usize, capped_count: bool) -> AncestryAnswer {
    AncestryAnswer {
        merge_base: Some("fork".into()),
        from_in_to: false,
        to_in_from: false,
        ahead,
        ahead_capped: capped_count,
        behind,
        behind_capped: false,
    }
}

#[test]
fn summaries_preserve_exact_coordinates_caps_and_unknowns() {
    let rows = vec![
        BranchRow {
            issue: 1,
            branch: "a".into(),
            base: "parent".into(),
        },
        BranchRow {
            issue: 2,
            branch: "b".into(),
            base: "missing".into(),
        },
        BranchRow {
            issue: 3,
            branch: "c".into(),
            base: "parent".into(),
        },
    ];
    let coordinates = HashMap::from([
        ("a".into(), Ok("head-a".into())),
        ("parent".into(), Ok("base-now".into())),
        ("b".into(), Ok("head-b".into())),
        ("missing".into(), Err("bookmark missing".into())),
        ("c".into(), Ok("head-c".into())),
    ]);
    let answers = HashMap::from([
        (("a".into(), "parent".into()), Ok(answer(10_000, 7, true))),
        (
            ("b".into(), "missing".into()),
            Err("head or base is unresolved".into()),
        ),
        (
            ("c".into(), "parent".into()),
            Err("ancestry timed out".into()),
        ),
    ]);
    let rendered = render_summaries("Child branches", &rows, &coordinates, &answers);
    assert!(
        rendered.contains("| 1 | `a` | `head-a` | `parent` | `base-now` | ≥10000 | 7 | known |"),
        "{rendered}"
    );
    assert!(
        rendered.contains(
            "| 2 | `b` | `head-b` | `missing` | unknown (bookmark missing) | unknown | unknown |"
        ),
        "{rendered}"
    );
    assert!(rendered.contains("| 3 | `c` | `head-c` | `parent` | `base-now` | unknown | unknown | ancestry timed out |"), "{rendered}");
    let mut unrelated = answer(2, 3, false);
    unrelated.merge_base = None;
    let rendered = render_summaries(
        "Branches",
        &rows[..1],
        &coordinates,
        &HashMap::from([(("a".into(), "parent".into()), Ok(unrelated))]),
    );
    assert!(
        rendered.contains("| 2 | 3 | no common ancestor |"),
        "{rendered}"
    );
}

async fn orchestrator(home: std::path::PathBuf) -> Orchestrator {
    use crate::{
        db::DbState, orchestrator::OrchestratorBuilder, services::testing::TestServicesBuilder,
        storage::SearchIndex,
    };
    use std::sync::Arc;
    let local = LocalDb::open_migrated_for_test(home.join("literal.db"))
        .await
        .unwrap();
    let search = Arc::new(SearchIndex::open_or_create(home.join("search")).unwrap());
    OrchestratorBuilder::new(
        Arc::new(DbState::new(Arc::new(local), search)),
        Arc::new(TestServicesBuilder::new().build()),
        home,
    )
    .build()
}

async fn seed(orch: &Orchestrator, path: &std::path::Path) {
    let path = path.to_string_lossy().into_owned();
    orch.db.local.write(move |conn| {
        let path = path.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1)", (),
            ).await?;
            conn.execute(
                "INSERT INTO projects(id,workspace_id,name,key,repo_path,default_branch,created_at,updated_at)
                 VALUES('p','w','P','p',?1,'main',1,1)",
                params![path],
            ).await?;
            Ok(())
        })
    }).await.unwrap();
    orch.db.local.execute_script(
        "INSERT INTO threads(id,project_id,name,created_at,updated_at) VALUES('t','p','topic',1,1);
         INSERT INTO issues(id,project_id,number,title,status,parent_thread_id,created_at,updated_at)
           VALUES('i','p',1,'Literal','active','t',1,1),('i2','p',2,'Nested','active','t',1,1);
         INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq)
           VALUES('e','r','i','p','running',1,1),('e2','r','i2','p','running',1,1);
         INSERT INTO jobs(id,execution_id,issue_id,project_id,node_name,uri_segment,branch,base_branch,status,created_at,updated_at)
           VALUES('j','e','i','p','Builder','builder','agent/literal','main','running',1,1),
                 ('j2','e','i','p','Reviewer','reviewer','agent/literal','main','running',1,1),
                 ('j3','e2','i2','p','Builder','builder','agent/nested','agent/literal','running',1,1);"
    ).await.unwrap();
}

#[tokio::test]
async fn absent_store_is_unknown_and_is_not_initialized() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    let orch = orchestrator(home.path().to_path_buf()).await;
    seed(&orch, repo.path()).await;
    let store = crate::jj::project_store_dir(home.path(), repo.path());
    let issue = branch_summaries(&orch, &orch.db.local, "p", BranchSummaryScope::Issue(1)).await;
    assert!(issue.contains("| unknown | unknown |"), "{issue}");
    assert_eq!(issue.matches("| 1 | `agent/literal`").count(), 1, "{issue}");
    let thread = branch_summaries(
        &orch,
        &orch.db.local,
        "p",
        BranchSummaryScope::Thread("topic".into()),
    )
    .await;
    assert!(thread.contains("| 2 | `agent/nested`"), "{thread}");
    assert!(thread.contains("`agent/literal` | unknown"), "{thread}");
    let request = crate::mcp::types::McpCallbackRequest {
        answer_bound_ms: None,
        thread_id: None,
        cwd: String::new(),
        run_id: None,
        tool: "read".to_string(),
        payload: serde_json::json!({}),
        tool_use_id: None,
        origin: None,
    };
    let issue_page =
        crate::resources::produce_cairn_resource(&orch, &request, "cairn://p/p/1").await;
    assert!(
        issue_page.content.contains("## Branches"),
        "{}",
        issue_page.content
    );
    assert!(
        issue_page.content.contains("`agent/literal`"),
        "{}",
        issue_page.content
    );
    let thread_page =
        crate::resources::produce_cairn_resource(&orch, &request, "cairn://p/p/topic").await;
    assert!(
        thread_page.content.contains("## Child branches"),
        "{}",
        thread_page.content
    );
    assert!(
        thread_page.content.contains("`agent/nested`"),
        "{}",
        thread_page.content
    );
    assert!(
        !store.exists(),
        "a summary read must not initialize the store"
    );
}

#[tokio::test]
async fn overview_budget_keeps_all_rows_and_stops_admitting_graph_work() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let computed = AtomicUsize::new(0);
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(30);
    let answers = collect_with_deadline((0..100).collect(), deadline, |key| {
        computed.fetch_add(1, Ordering::SeqCst);
        async move {
            if key == 0 {
                Ok(42)
            } else {
                std::future::pending().await
            }
        }
    })
    .await;
    assert_eq!(answers.len(), 100);
    assert_eq!(answers[&0], Ok(42));
    assert_eq!(answers[&99], Err("overview graph budget exhausted".into()));
    assert!(
        computed.load(Ordering::SeqCst) <= 9,
        "only one completed item and eight in flight"
    );
    let exhausted: HashMap<_, Result<usize, String>> =
        collect_with_deadline(vec!["pair"], tokio::time::Instant::now(), |_| async {
            panic!("no work may start after the shared deadline");
        })
        .await;
    assert_eq!(
        exhausted["pair"],
        Err("overview graph budget exhausted".into())
    );
}

#[tokio::test]
async fn undeclared_job_bases_use_the_canonical_effective_project_default() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    let orch = orchestrator(home.path().to_path_buf()).await;
    seed(&orch, repo.path()).await;
    orch.db
        .local
        .execute(
            "UPDATE jobs SET base_branch = NULL WHERE id IN ('j', 'j2')",
            (),
        )
        .await
        .unwrap();
    std::fs::create_dir(repo.path().join(".cairn")).unwrap();
    let config = repo.path().join(".cairn/config.yaml");
    let original = "defaultBranch: integration\n";
    std::fs::write(&config, original).unwrap();
    let summary = branch_summaries(&orch, &orch.db.local, "p", BranchSummaryScope::Issue(1)).await;
    assert!(summary.contains("| `integration` | unknown"), "{summary}");
    assert!(!summary.contains("| `main` |"), "{summary}");
    assert_eq!(
        std::fs::read_to_string(&config).unwrap(),
        original,
        "default resolution stays read-only"
    );
    assert!(!crate::jj::project_store_dir(home.path(), repo.path()).exists());
}
