use super::*;
use crate::storage::RowExt;

const AGENT_BRANCH: &str = "agent/proj-7-builder-0";
const NODE_URI: &str = "cairn://p/proj/7/1/builder";
const FOREIGN_NODE_URI: &str = "cairn://p/other/8/1/builder";
const SUBJECT_JOB: &str = "j";

/// A project whose managed store holds `main` and one agent branch a commit
/// ahead of it, a builder node recording that branch, and a run of it:
/// everything `--branch` can name. A second project's node records the
/// same branch name, so a node URI resolving across projects would be
/// indistinguishable from one resolving correctly.
struct BranchFixture {
    _root: tempfile::TempDir,
    orch: Orchestrator,
    repo: std::path::PathBuf,
    store: std::path::PathBuf,
    main: String,
    agent: String,
}

impl BranchFixture {
    /// Where each branch points in the managed store right now.
    async fn tips(&self) -> Vec<String> {
        let mut tips = Vec::new();
        for branch in ["main", AGENT_BRANCH] {
            tips.push(
                cairn_vcs::resolve_coordinate(&self.store, branch)
                    .await
                    .unwrap(),
            );
        }
        tips
    }
}

async fn branch_fixture(bin: &str) -> BranchFixture {
    branch_fixture_with(bin, |_| {}, "").await
}

/// [`branch_fixture`], with `setup` run on the checkout (on `main`) before
/// the managed store imports it, and `extra_sql` run after the base rows.
async fn branch_fixture_with(
    bin: &str,
    setup: impl FnOnce(&std::path::Path),
    extra_sql: &str,
) -> BranchFixture {
    use crate::jj::testing::{git, git_stdout, init_project};
    let root = tempfile::tempdir().unwrap();
    let repo = root.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    init_project(&repo);
    git(&repo, &["checkout", "-q", "-b", AGENT_BRANCH]);
    std::fs::write(repo.join("shared.rs"), "agent\n").unwrap();
    git(&repo, &["commit", "-q", "-am", "agent work"]);
    let agent = git_stdout(&repo, &["rev-parse", "HEAD"]);
    git(&repo, &["checkout", "-q", "main"]);
    setup(&repo);
    let main = git_stdout(&repo, &["rev-parse", "main"]);

    let db = crate::storage::migrated_test_db("check-request-branch.db").await;
    let repo_path = repo.to_string_lossy();
    db.execute_script(format!(
        "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
         INSERT INTO projects(id,workspace_id,name,key,repo_path,default_branch,created_at,updated_at)
           VALUES('p','w','P','proj','{repo_path}','main',1,1);
         INSERT INTO projects(id,workspace_id,name,key,repo_path,default_branch,created_at,updated_at)
           VALUES('q','w','Q','other','{repo_path}','main',1,1);
         INSERT INTO issues(id,project_id,number,title,status,progress,attention,created_at,updated_at)
           VALUES('i','p',7,'I','active','active','none',1,1);
         INSERT INTO issues(id,project_id,number,title,status,progress,attention,created_at,updated_at)
           VALUES('k','q',8,'K','active','active','none',1,1);
         INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq)
           VALUES('e','r','i','p','running',1,1);
         INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq)
           VALUES('f','r','k','q','running',1,1);
         INSERT INTO jobs(id,project_id,issue_id,execution_id,status,node_name,uri_segment,branch,base_branch,created_at,updated_at)
           VALUES('j','p','i','e','running','builder','builder','{AGENT_BRANCH}','main',1,1);
         INSERT INTO jobs(id,project_id,issue_id,execution_id,status,node_name,uri_segment,branch,base_branch,created_at,updated_at)
           VALUES('m','q','k','f','running','builder','builder','{AGENT_BRANCH}','main',1,1);
         INSERT INTO runs(id,issue_id,project_id,job_id,status,created_at,updated_at)
           VALUES('run-1','i','p','j','running',1,1);
         {extra_sql}"
    ))
    .await
    .unwrap();

    let config_dir = root.path().join("config");
    std::fs::create_dir_all(&config_dir).unwrap();
    let search =
        Arc::new(crate::storage::SearchIndex::open_or_create(config_dir.join("search")).unwrap());
    let orch = Orchestrator::builder(
        Arc::new(crate::db::DbState::new(Arc::new(db), search)),
        Arc::new(crate::services::testing::TestServicesBuilder::new().build()),
        config_dir,
    )
    .jj_binary_path(bin.to_string())
    .build();
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let store = crate::jj::project_store_dir(&orch.config_dir, &repo);
    crate::jj::ensure_project_store(&jj, &store, &repo).unwrap();
    BranchFixture {
        _root: root,
        orch,
        repo,
        store,
        main,
        agent,
    }
}

fn spec(suite: &str, revision: Option<&str>) -> CheckRequestSpec {
    CheckRequestSpec {
        consumer: super::super::CheckConsumer::Requested,
        cadence: Some("requested"),
        priority: cairn_common::wire_vocabulary::CellPriority::ReviewCheck,
        project_id: "p".into(),
        project_key: "proj".into(),
        suite: suite.into(),
        revision: revision.map(str::to_string),
        subject_job_id: SUBJECT_JOB.into(),
        placement: None,
        retry: false,
        requester_ref: NODE_URI.into(),
        requester_job_id: Some(SUBJECT_JOB.into()),
    }
}

/// Classify `requested` as filing does, then capture it as the driver does:
/// an exact revision is already captured, a managed one is prepared.
async fn captured(
    orch: &Orchestrator,
    requested: &str,
) -> Result<crate::mcp::handlers::branch::CapturedRevision, String> {
    match crate::execution::checks::classify_manual_check_revision(orch, SUBJECT_JOB, requested)
        .await?
    {
        ManualCheckRevision::Captured(captured) => Ok(*captured),
        ManualCheckRevision::Managed(revision) => {
            let store = crate::jj::project_store_dir(&orch.config_dir, &revision.project_path);
            crate::mcp::handlers::branch::capture_revision(
                orch,
                *revision,
                store,
                crate::mcp::handlers::branch::CapturePurpose::Check,
            )
            .await
            .map_err(|error| error.to_string())
        }
        other => panic!("--branch {requested:?} classified as {other:?}"),
    }
}

/// `--branch` takes every form `read ?branch=` and `run branch:` take, and
/// each lands on the commit that form names.
#[tokio::test]
async fn branch_takes_a_branch_name_a_commit_id_and_a_node_uri() {
    let Some(bin) = crate::jj::testing::jj_bin() else {
        return;
    };
    let fixture = branch_fixture(&bin).await;
    for (requested, expected) in [
        ("main", &fixture.main),
        (AGENT_BRANCH, &fixture.agent),
        (fixture.agent.as_str(), &fixture.agent),
        (&fixture.agent[..12], &fixture.agent),
        (NODE_URI, &fixture.agent),
    ] {
        let resolved = captured(&fixture.orch, requested)
            .await
            .unwrap_or_else(|error| panic!("{requested:?} did not resolve: {error}"));
        assert_eq!(&resolved.commit_id, expected, "{requested:?}");
    }
}

/// The form decides authority, never where a commit sits: the managed
/// branch's name and its node prepare, while its head's commit id, the
/// default branch, and a name no live job owns are exact snapshots. None
/// of the exact forms touches the store's branches.
#[tokio::test]
async fn only_a_managed_name_or_node_authorizes_preparation() {
    use crate::mcp::handlers::branch::{ExactRevisionKind, RevisionAuthority};
    let Some(bin) = crate::jj::testing::jj_bin() else {
        return;
    };
    let fixture = branch_fixture(&bin).await;
    let before = fixture.tips().await;
    for (requested, kind) in [
        ("main", ExactRevisionKind::IntegrationBaseline),
        (fixture.agent.as_str(), ExactRevisionKind::Commit),
    ] {
        let resolved = captured(&fixture.orch, requested).await.unwrap();
        assert!(resolved.prepared.is_none(), "{requested:?} was prepared");
        assert!(
            matches!(resolved.revision.authority, RevisionAuthority::Exact { kind: k, .. } if k == kind),
            "{requested:?}: {:?}",
            resolved.revision.authority
        );
    }
    assert_eq!(
        fixture.tips().await,
        before,
        "an exact capture moved a branch"
    );
    for (requested, owner) in [(AGENT_BRANCH, None), (NODE_URI, Some("j"))] {
        let resolved = captured(&fixture.orch, requested).await.unwrap();
        assert_eq!(
            resolved.revision.authority,
            RevisionAuthority::Managed {
                branch: AGENT_BRANCH.into(),
                owner_job_id: owner.map(str::to_string),
            },
            "{requested:?}"
        );
        let prepared = resolved
            .prepared
            .as_ref()
            .expect("a managed capture is prepared");
        assert_eq!(prepared.base_commit, fixture.main);
        assert_eq!(prepared.head_after, fixture.agent);
    }
}

/// A revision that names nothing is refused before a request exists, so the
/// requester never sees a queued check turn into a failure, and the refusal
/// names the forms `--branch` takes.
#[tokio::test]
async fn an_unresolvable_branch_is_refused_before_anything_is_filed() {
    let Some(bin) = crate::jj::testing::jj_bin() else {
        return;
    };
    let fixture = branch_fixture(&bin).await;
    for requested in ["agent/proj-7-builder-1", "cairn://p/proj/7/1/coordinator"] {
        let error = file_check_request(&fixture.orch, spec("rust-tests", Some(requested)))
            .await
            .unwrap_err();
        assert!(error.contains(requested), "{error}");
        for form in ["branch name", "commit id", "node URI"] {
            assert!(
                error.contains(form),
                "{requested:?} omits {form:?}: {error}"
            );
        }
    }
    let filed = fixture
        .orch
        .db
        .local
        .query_opt("SELECT COUNT(*) FROM check_requests", (), |row| row.i64(0))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(filed, 0, "a refused revision filed a request");
}

/// A node of another project is refused rather than resolved, even when
/// its branch name happens to exist here.
#[tokio::test]
async fn a_node_of_another_project_is_refused() {
    let Some(bin) = crate::jj::testing::jj_bin() else {
        return;
    };
    let fixture = branch_fixture(&bin).await;
    let error = crate::execution::checks::classify_manual_check_revision(
        &fixture.orch,
        SUBJECT_JOB,
        FOREIGN_NODE_URI,
    )
    .await
    .unwrap_err();
    assert!(error.contains("another project"), "{error}");
}

/// An exact revision's commit is on the row from the moment it is accepted,
/// so nothing that moves afterwards can change what the request judges.
#[tokio::test]
async fn an_exact_revision_is_resolved_at_acceptance() {
    let Some(bin) = crate::jj::testing::jj_bin() else {
        return;
    };
    let fixture = branch_fixture(&bin).await;
    let filed = file_check_request(&fixture.orch, spec("unit", Some(&fixture.main)))
        .await
        .unwrap();
    let row = load_request(&fixture.orch.db.local, "p", &filed.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.commit_sha.as_deref(), Some(fixture.main.as_str()));
    assert_eq!(row.consumer, "requested");
    assert_eq!(row.requester_ref, NODE_URI);
    assert_eq!(row.requester_job_id.as_deref(), Some(SUBJECT_JOB));
    assert_eq!(
        filed.uri,
        format!("cairn://p/proj/check-requests/{}", filed.id)
    );
    let _ = &fixture.repo;
}

// --- rows without a repository -----------------------------------------------

pub(super) async fn row_fixture() -> (tempfile::TempDir, Orchestrator) {
    let db = crate::storage::migrated_test_db("check-request-rows.db").await;
    db.execute_script(
        "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
         INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
           VALUES('p','w','P','proj','/unused',1,1),('q','w','Q','other','/unused',1,1);
         INSERT INTO jobs(id,project_id,status,created_at,updated_at) VALUES('j','p','running',1,1),('x','p','running',1,1);
         INSERT INTO check_result_observations(id,project_id,commit_sha,tree_hash,check_name,input_hash,environment_fingerprint,exit_code,verdict,complete,reusable,parser_version,result_schema_version,ran_at,duration_ms,cadence,output_tail,public_handle,verdict_result_json)
           VALUES('source','p','source-commit','source-tree','rust-tests','input','env',0,'passed',1,1,1,1,1,1,'manual','source output','source-public',
             '{\"parser\":\"nextest\",\"complete\":true,\"selection\":\"fast\",\"passed\":9,\"failed\":0,\"skipped\":0,\"deferred\":3,\"failures\":[]}');",
    )
    .await
    .unwrap();
    let root = tempfile::tempdir().unwrap();
    let search =
        Arc::new(crate::storage::SearchIndex::open_or_create(root.path().join("search")).unwrap());
    let orch = Orchestrator::builder(
        Arc::new(crate::db::DbState::new(Arc::new(db), search)),
        Arc::new(crate::services::testing::TestServicesBuilder::new().build()),
        root.path().to_path_buf(),
    )
    .build();
    (root, orch)
}

const RECOVERY_CONTEXT: &str = r#"{"project_key":"proj","placement":{"selector":null,"verdict_platforms":[],"executor_name":null,"note":null},"runtime_instruments":[]}"#;

/// An open request row in `state`, then `set` applied to it.
async fn insert_request(orch: &Orchestrator, state: &str, set: &str) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    orch.db
        .local
        .execute(
            "INSERT INTO check_requests(id,project_id,suite,subject_job_id,consumer,requester_ref,requester_job_id,
                priority,state,accepted_at_ms,state_since_ms,updated_ms)
             VALUES(?1,'p','rust-tests','j','requested','cairn://p/proj/7/1/builder','j','review_check',?2,1,1,1)",
            cairn_db::turso::params![id.as_str(), state],
        )
        .await
        .unwrap();
    if !set.is_empty() {
        orch.db
            .local
            .execute(
                format!("UPDATE check_requests SET {set} WHERE id=?1"),
                (id.clone(),),
            )
            .await
            .unwrap();
    }
    id
}

async fn row(orch: &Orchestrator, id: &str) -> CheckRequestRow {
    load_request(&orch.db.local, "p", id)
        .await
        .unwrap()
        .unwrap()
}

async fn answer(orch: &Orchestrator, id: &str) -> serde_json::Value {
    let row = row(orch, id).await;
    serde_json::to_value(
        request_answer(orch, orch.db.local.clone(), &row)
            .await
            .unwrap(),
    )
    .unwrap()
}

/// The same requester asking for the same suite on the same revision and
/// placement while one is open gets that request back, not a second one. A
/// different requester gets its own row (both later join one physical lane).
#[tokio::test]
async fn an_identical_open_request_is_joined_not_duplicated() {
    let (_root, orch) = row_fixture().await;
    let mut same = spec("rust-tests", None);
    same.subject_job_id = "j".into();
    let key = open_key(&same, "rust-tests", &Coordinate::Unresolved);
    let open = insert_request(&orch, "queued", &format!("open_key='{key}'")).await;
    let joined = file_check_request(&orch, same.clone()).await.unwrap();
    assert!(joined.joined);
    assert_eq!(joined.id, open);

    let mut other = same;
    other.requester_ref = "cairn://p/proj/9/1/builder".into();
    other.requester_job_id = Some("x".into());
    let fresh = file_check_request(&orch, other).await.unwrap();
    assert!(!fresh.joined);
    assert_ne!(fresh.id, open);
}

/// Identical filings racing each other get one request: one is accepted and
/// every other joins it. Once it settles, its identity is free again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn identical_filings_racing_each_other_get_one_request() {
    let (_root, orch) = row_fixture().await;
    let same = spec("rust-tests", None);
    let head = Coordinate::Commit("head-a".into());
    let racers: Vec<_> = (0..8)
        .map(|_| {
            let db = orch.db.local.clone();
            let same = same.clone();
            let head = head.clone();
            cairn_common::task::spawn(async move {
                accept(&db, &same, "rust-tests", &head).await.unwrap()
            })
        })
        .collect();
    let mut ids = std::collections::HashSet::new();
    let mut accepted = 0;
    for racer in racers {
        let filed = racer.await.unwrap();
        accepted += usize::from(!filed.joined);
        ids.insert(filed.id);
    }
    assert_eq!(accepted, 1, "exactly one filing is accepted");
    assert_eq!(ids.len(), 1, "every other filing joins it");
    let id = ids.into_iter().next().unwrap();
    let first = row(&orch, &id).await;
    assert_eq!(first.commit_sha.as_deref(), Some("head-a"));
    settle(
        &orch,
        &orch.db.local,
        &first,
        CheckRequestAnswer::Failed {
            error: "done".into(),
        },
    )
    .await;
    let again = accept(&orch.db.local, &same, "rust-tests", &head)
        .await
        .unwrap();
    assert!(!again.joined, "a settled request is never joined");
    assert_ne!(again.id, id);
}

/// A request on its own head is about the head it was filed at, captured as
/// that exact commit. With S open for an earlier head, filing S at the current
/// head is a request of its own. Once the head moves on after filing, the
/// request still judges the commit it was filed at, on its first drive and on
/// a re-drive after a restart alike.
#[tokio::test]
async fn an_own_head_request_judges_the_head_it_was_filed_at() {
    use crate::jj::testing::{git, git_stdout};
    let Some(bin) = crate::jj::testing::jj_bin() else {
        return;
    };
    let fixture = branch_fixture(&bin).await;
    let orch = &fixture.orch;
    let accepted_commit = |revision: ManualCheckRevision| match revision {
        ManualCheckRevision::Captured(captured) => captured.commit_id,
        other => panic!("an own-head request was not captured at acceptance: {other:?}"),
    };
    let first_drive = accepted_revision(orch, SUBJECT_JOB, None).await.unwrap();
    assert_eq!(
        accepted_commit(first_drive),
        fixture.agent,
        "filed at head A"
    );

    let own = spec("unit", None);
    let earlier = open_key(&own, "unit", &Coordinate::Commit(fixture.main.clone()));
    let open_earlier = insert_request(
        orch,
        "running",
        &format!("open_key='{earlier}',commit_sha='{}'", fixture.main),
    )
    .await;
    let filed = file_check_request(orch, own).await.unwrap();
    assert!(
        !filed.joined,
        "head A joined the request for an earlier head"
    );
    assert_ne!(filed.id, open_earlier);
    let row = load_request(&orch.db.local, "p", &filed.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.commit_sha.as_deref(), Some(fixture.agent.as_str()));

    // The head moves on to B after the request was filed.
    git(&fixture.repo, &["checkout", "-q", AGENT_BRANCH]);
    std::fs::write(fixture.repo.join("shared.rs"), "agent, later\n").unwrap();
    git(&fixture.repo, &["commit", "-q", "-am", "later agent work"]);
    let b = git_stdout(&fixture.repo, &["rev-parse", "HEAD"]);
    git(&fixture.repo, &["checkout", "-q", "main"]);
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    crate::jj::ensure_project_store(&jj, &fixture.store, &fixture.repo).unwrap();
    let head_now = crate::execution::cache::resolve_job_logical_head(orch, SUBJECT_JOB)
        .await
        .unwrap();
    assert_eq!(head_now, b, "the job's head moved to B");

    let redrive = revision_for_redrive(orch, &row, SUBJECT_JOB).await.unwrap();
    assert_eq!(
        accepted_commit(redrive),
        fixture.agent,
        "a re-drive judges head A, not B"
    );
}

/// A team project's requests live in its replica. The driver finds its row
/// there, so a filed request is driven to an answer rather than left accepted,
/// and a restart re-arms the replica's open rows this runner drives, leaving
/// the ones another runner drives to it.
#[tokio::test]
async fn a_team_projects_request_is_driven_and_rearmed_from_its_replica() {
    const TEAM_PROJECT: &str = "team1~00000000-0000-4000-8000-000000000002";
    let (_root, orch) = row_fixture().await;
    let team = Arc::new(crate::storage::migrated_test_db("check-request-team.db").await);
    team.execute_script(format!(
        "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
         INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
           VALUES('{TEAM_PROJECT}','w','T','team','/unused',1,1);"
    ))
    .await
    .unwrap();
    orch.db.insert_team_db_for_test("team1", team.clone()).await;
    let mut filing = spec("rust-tests", None);
    filing.project_id = TEAM_PROJECT.into();
    filing.project_key = "team".into();
    let filed = file_check_request(&orch, filing).await.unwrap();
    assert!(load_request(&orch.db.local, TEAM_PROJECT, &filed.id)
        .await
        .unwrap()
        .is_none());
    let settled_row = tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            let mut settled = subscribe_settlements();
            let current = load_request(&team, TEAM_PROJECT, &filed.id)
                .await
                .unwrap()
                .expect("the request is in the team replica");
            if !current.is_open() {
                return current;
            }
            let _ = tokio::time::timeout(std::time::Duration::from_millis(200), settled.changed())
                .await;
        }
    })
    .await
    .expect("the driver found the replica's row and drove it to an answer");
    assert!(settled_row.outcome_json.is_some());

    let open_in_team = |runner: String| {
        let team = team.clone();
        async move {
            let id = uuid::Uuid::new_v4().to_string();
            team.execute(
                "INSERT INTO check_requests(id,project_id,suite,subject_job_id,consumer,requester_ref,
                    priority,state,cancel_requested_by,driven_by,accepted_at_ms,state_since_ms,updated_ms)
                 VALUES(?1,?2,'rust-tests','j','requested','operator','review_check','queued','operator',?3,1,1,1)",
                cairn_db::turso::params![id.as_str(), TEAM_PROJECT, runner.as_str()],
            )
            .await
            .unwrap();
            id
        }
    };
    let mine = open_in_team(this_runner()).await;
    let theirs = open_in_team("another-runner".into()).await;
    rearm_at_boot(&orch).await;
    let state = |id: String| {
        let team = team.clone();
        async move {
            load_request(&team, TEAM_PROJECT, &id)
                .await
                .unwrap()
                .unwrap()
                .state
        }
    };
    assert_eq!(state(mine).await, "cancelled", "this runner's row re-armed");
    assert_eq!(
        state(theirs).await,
        "queued",
        "another runner's row was taken over"
    );
}

/// A lane left running on its machine because the runner is shutting down is
/// not an answer: the request stays open and `running`, says so, and keeps
/// the fleet request a restart adopts. It is never settled `recorded`.
#[tokio::test]
async fn a_lane_left_running_at_shutdown_keeps_its_request_open() {
    let (_root, orch) = row_fixture().await;
    let recorded = insert_request(
        &orch,
        "running",
        &format!("context_json='{RECOVERY_CONTEXT}'"),
    )
    .await;
    crate::execution::cache::attach_manual_check_observation(
        &orch.db.local,
        "p",
        &recorded,
        "source",
        "evaluated-commit",
        "evaluated-tree",
        "input",
        "fresh",
    )
    .await
    .unwrap();
    let CheckRequestAnswer::Complete { mut result } =
        request_answer(&orch, orch.db.local.clone(), &row(&orch, &recorded).await)
            .await
            .unwrap()
    else {
        panic!("an attached observation reads as complete");
    };
    result.left_running = Some(crate::fleet::LeftRunningAttempt {
        machine: "bginf-sp2".into(),
        request_id: "fleet-left".into(),
        attempt_id: "attempt-left".into(),
        executor_id: "executor-left".into(),
        stopped_at_ms: 1_700_000_000_000,
    });
    let id = insert_request(&orch, "running", "executor='bginf-sp2'").await;
    let before = row(&orch, &id).await;
    conclude(
        &orch,
        &orch.db.local,
        &id,
        before,
        CheckRequestAnswer::Complete { result },
    )
    .await;
    let after = row(&orch, &id).await;
    assert_eq!(after.state, "running");
    assert!(after.outcome_json.is_none(), "no answer was written");
    assert_eq!(after.request_id.as_deref(), Some("fleet-left"));
    let standing = standing(&after);
    assert!(standing.contains("left running on bginf-sp2"), "{standing}");
    assert!(standing.contains("next runner adopts"), "{standing}");
}

/// A recorded answer is read from the row alone, and only in its own
/// project: an opaque id is not authority to read another project's request.
#[tokio::test]
async fn a_settled_answer_reads_from_its_row_and_is_project_fenced() {
    let (_root, orch) = row_fixture().await;
    let id = insert_request(
        &orch,
        "failed",
        r#"outcome_json='{"state":"failed","error":"runner refused"}',evidence='runner refused'"#,
    )
    .await;
    assert_eq!(answer(&orch, &id).await["error"], "runner refused");
    assert!(load_request(&orch.db.local, "q", &id)
        .await
        .unwrap()
        .is_none());
}

/// A row whose recorder attached its ledger source before the driver wrote
/// the answer (the restart window) recovers that verdict from the ledger.
#[tokio::test]
async fn an_attached_ledger_source_recovers_without_a_written_answer() {
    let (_root, orch) = row_fixture().await;
    for disposition in ["fresh", "cached"] {
        let id = insert_request(
            &orch,
            "running",
            &format!("context_json='{RECOVERY_CONTEXT}'"),
        )
        .await;
        crate::execution::cache::attach_manual_check_observation(
            &orch.db.local,
            "p",
            &id,
            "source",
            "evaluated-commit",
            "evaluated-tree",
            "input",
            disposition,
        )
        .await
        .unwrap();
        let value = answer(&orch, &id).await;
        assert_eq!(value["state"], "complete", "{value}");
        let result = &value["result"];
        assert_eq!(result["commitSha"], "evaluated-commit");
        assert_eq!(result["disposition"], disposition);
        assert_eq!(
            result["observationRef"],
            "cairn://p/proj/check-observations/source-public"
        );
        assert_eq!(result["tier"], "fast tier: 9 ran, 3 deferred");
    }
}

/// A queued request explains itself from its row: since when, the machine it
/// is pinned to, and that machine's latest refusal verbatim, which names the
/// running commands it waits behind (the 78a872da shape).
#[tokio::test]
async fn a_pinned_queued_request_names_its_machine_and_its_latest_refusal() {
    let (_root, orch) = row_fixture().await;
    let refusal = "declined by bgsrv-ub (pressure: memory short: available 9.2 GiB less 6.1 GiB owed by 2 running commands (parity in slot-32: 6.5 GiB charged), under the 6.4 GiB floor)";
    let id = insert_request(
        &orch,
        "queued",
        "pinned_executor='bgsrv-ub',queued_at_ms=1700000000000",
    )
    .await;
    for _ in 0..3 {
        record_progress(
            &orch.db.local,
            &id,
            &ManualCheckProgressUpdate {
                state: "parked".into(),
                diagnostic: format!("queued; parked since 22:13:20Z; {refusal}"),
                executor: None,
                since_unix_ms: Some(1_700_000_000_000),
                request_id: Some("fleet-request".into()),
                held_by: Some(refusal.into()),
            },
        )
        .await
        .unwrap();
    }
    let current = row(&orch, &id).await;
    assert_eq!(current.state, "queued");
    assert_eq!(current.request_id.as_deref(), Some("fleet-request"));
    assert_eq!(current.held_count, 1, "an unchanged refusal is one refusal");
    let standing = standing(&current);
    assert!(standing.contains("queued since"), "{standing}");
    assert!(
        standing.contains("pinned to [bgsrv-ub](cairn://executors/bgsrv-ub)"),
        "{standing}"
    );
    assert!(standing.contains(refusal), "{standing}");
    assert!(standing.contains("parity in slot-32"), "{standing}");
    let value = answer(&orch, &id).await;
    assert_eq!(value["state"], "parked");
    assert_eq!(value["heldBy"], refusal);
    assert_eq!(value["executor"], "bgsrv-ub");
    let pinned = requests_pinned_to(&orch, "bgsrv-ub").await.unwrap();
    assert_eq!(pinned.len(), 1);
    assert!(requests_pinned_to(&orch, "fedora")
        .await
        .unwrap()
        .is_empty());
}

/// A cancel recorded before a restart is honoured after it: the request is
/// settled cancelled, never re-driven, and never reads as a verdict.
#[tokio::test]
async fn a_cancel_issued_before_a_restart_is_honoured_after_it() {
    let (_root, orch) = row_fixture().await;
    let id = insert_request(&orch, "queued", "cancel_requested_by='operator'").await;
    let mut settled = subscribe_settlements();
    rearm_at_boot(&orch).await;
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), settled.changed()).await;
    let current = row(&orch, &id).await;
    assert_eq!(current.state, "cancelled");
    let reason = current.evidence.unwrap();
    assert!(
        reason.contains("cancelled by operator before placement"),
        "{reason}"
    );
    assert!(!driving(&id), "a cancelled request was re-driven");
}

/// Cancelling a queued request that was never submitted settles it at once
/// as cancelled before placement; a running one names its machine and is
/// incomplete. Neither is a verdict, and a second cancel says it settled.
#[tokio::test]
async fn cancel_settles_queued_before_placement_and_running_as_incomplete() {
    let (_root, orch) = row_fixture().await;
    let queued = insert_request(&orch, "accepted", "").await;
    let said = cancel_check_request(
        &orch,
        &orch.db.local,
        &row(&orch, &queued).await,
        "operator",
    )
    .await
    .unwrap();
    assert!(said.contains("before placement"), "{said}");
    let settled = row(&orch, &queued).await;
    assert_eq!(settled.state, "cancelled");
    assert!(answer(&orch, &queued).await["reason"]
        .as_str()
        .unwrap()
        .contains("before placement"));
    let again = cancel_check_request(&orch, &orch.db.local, &settled, "operator")
        .await
        .unwrap_err();
    assert!(again.contains("already settled"), "{again}");

    let running = insert_request(
        &orch,
        "running",
        "executor='bginf-sp2',request_id='fleet-running'",
    )
    .await;
    cancel_check_request(
        &orch,
        &orch.db.local,
        &row(&orch, &running).await,
        "operator",
    )
    .await
    .unwrap();
    let settled = row(&orch, &running).await;
    assert_eq!(settled.state, "cancelled");
    let reason = settled.evidence.unwrap();
    assert!(reason.contains("while running on bginf-sp2"), "{reason}");
    assert!(reason.contains("not a verdict"), "{reason}");
}

/// A row whose lane recorded before the restart takes that observation at
/// re-arm and runs nothing.
#[tokio::test]
async fn a_request_recorded_before_a_restart_takes_its_observation_and_runs_nothing() {
    let (_root, orch) = row_fixture().await;
    let id = insert_request(
        &orch,
        "running",
        &format!(
            "observation_id='source',commit_sha='source-commit',tree_hash='source-tree',input_hash='input',disposition='fresh',context_json='{RECOVERY_CONTEXT}'"
        ),
    )
    .await;
    let mut settled = subscribe_settlements();
    rearm_at_boot(&orch).await;
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), settled.changed()).await;
    let current = row(&orch, &id).await;
    assert_eq!(current.state, "recorded");
    assert_eq!(current.observation_id.as_deref(), Some("source"));
    assert!(current.request_id.is_none(), "nothing was submitted");
    assert!(!driving(&id));
}

/// The terminal guard writes an answer once: a second settlement of the same
/// request changes nothing.
#[tokio::test]
async fn a_request_settles_exactly_once() {
    let (_root, orch) = row_fixture().await;
    let id = insert_request(&orch, "queued", "").await;
    let first = row(&orch, &id).await;
    settle(
        &orch,
        &orch.db.local,
        &first,
        CheckRequestAnswer::Failed {
            error: "first".into(),
        },
    )
    .await;
    settle(
        &orch,
        &orch.db.local,
        &first,
        CheckRequestAnswer::Lost {
            evidence: "second".into(),
        },
    )
    .await;
    let current = row(&orch, &id).await;
    assert_eq!(current.state, "failed");
    assert_eq!(current.evidence.as_deref(), Some("first"));
}

/// The migration carries every old handle: a settled one keeps its answer,
/// and one that never settled reads as interrupted, as it always did.
#[tokio::test]
async fn old_manual_handles_become_settled_requests() {
    let migration = include_str!("../../../../../turso_migrations/0470_check_requests.sql");
    assert!(migration.contains("FROM manual_check_handles"));
    assert!(migration.contains("DROP TABLE manual_check_handles"));
    assert!(migration.contains("interrupted before its terminal answer was persisted"));
}

/// Filing from an agent places exactly one thing on the fleet, the lane, and
/// no run batch. A runner restart before the lane is granted re-drives the
/// same request, which then runs and records exactly once.
#[test]
#[serial_test::serial(jj)]
fn a_filed_request_places_one_lane_and_survives_a_restart_before_its_grant() {
    for consumer in [
        super::super::CheckConsumer::Requested,
        super::super::CheckConsumer::Write,
    ] {
        filed_request_restart_before_grant_for(consumer);
    }
}

fn filed_request_restart_before_grant_for(consumer: super::super::CheckConsumer) {
    use crate::execution::checks::write_check_stack_headroom::{commit, passed_batch};
    use crate::jj::tests::{init_project, jj_bin};
    use cairn_common::executor_protocol::ExecutorMessage;
    let Some(bin) = jj_bin() else {
        return;
    };
    let home = tempfile::tempdir().unwrap();
    let project = tempfile::tempdir().unwrap();
    let repo = project.path().to_path_buf();
    init_project(&repo);
    commit(
        &repo,
        &[(
            ".cairn/config.yaml",
            "checks:\n  unit:\n    command: echo ok\n    when: review\n",
        )],
        "declare unit",
    );
    crate::stack_headroom::within_budget("check request end to end", move || async move {
        let config_dir = home.path().to_path_buf();
        let local = Arc::new(
            crate::storage::LocalDb::open_migrated_for_test(config_dir.join("checks.db"))
                .await
                .unwrap(),
        );
        local
            .execute_script(format!(
                "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('ws','w',1,1);
                 INSERT INTO projects(id,workspace_id,name,key,repo_path,default_branch,created_at,updated_at)
                   VALUES('p','ws','P','proj','{}','main',1,1);
                 INSERT INTO jobs(id,project_id,branch,status,created_at,updated_at)
                   VALUES('j','p','agent/requester','running',1,1);",
                repo.to_string_lossy()
            ))
            .await
            .unwrap();
        let runner = |search: &str| {
            let search = Arc::new(
                crate::storage::SearchIndex::open_or_create(config_dir.join(search)).unwrap(),
            );
            Orchestrator::builder(
                Arc::new(crate::db::DbState::new(local.clone(), search)),
                Arc::new(crate::services::testing::TestServicesBuilder::new().build()),
                config_dir.clone(),
            )
            .jj_binary_path(bin.clone())
            .build()
        };

        // The first runner: the colocated machine is offered the lane and
        // never claims it.
        let first = runner("search-1");
        let (sender, mut machine) = tokio::sync::mpsc::unbounded_channel();
        first.fleet.attach_announced_executor(sender);
        let filed = if consumer == super::super::CheckConsumer::Requested {
            file_check_request(&first, spec("unit", Some("main")))
                .await
                .unwrap()
        } else {
            let contract = super::super::manual::resolve_manual_check_contract_snapshot(
                &first,
                SUBJECT_JOB,
                "unit",
                accepted_revision(&first, SUBJECT_JOB, Some("main"))
                    .await
                    .unwrap(),
                None,
            )
            .await
            .unwrap();
            let producer = super::super::CheckProducer::new(
                super::super::CheckConsumer::Write,
                "never-granted-wave",
            );
            let mut item = super::super::manual::manual_check_batch_item(
                "unit",
                &contract.context.input_hash,
                &contract.configured_check,
                &contract.placement,
                contract.plan.resource_class,
                contract.timeout_ms,
                crate::execution::cache::InfraScopes::head_only(
                    SUBJECT_JOB,
                    &contract.context.commit_sha,
                ),
                Vec::new(),
            );
            item.recording_plan = Some(super::super::CheckAdoptionRecordingPlan::new(
                &contract.plan,
                &contract.sealed_coordinate,
                SUBJECT_JOB,
                "never-granted-write",
                &[],
                &producer,
            ));
            let planned = super::super::PlannedCheckRequest {
                producer,
                project_id: "p".into(),
                repository: contract.repository_path.clone(),
                store_dir: crate::jj::project_store_dir(
                    &first.config_dir,
                    std::path::Path::new(&contract.repository_path),
                ),
                sealed_commit: contract.context.commit_sha.clone(),
                requesting_job_id: SUBJECT_JOB.into(),
                owner: cairn_common::executor_protocol::CellOwnerRef {
                    project_id: "p".into(),
                    project_key: Some("proj".into()),
                    issue_number: Some(7),
                    job_id: Some(SUBJECT_JOB.into()),
                    execution_seq: Some(1),
                    node_kind: Some("builder".into()),
                },
                affinity_key: Some("j:unit".into()),
                priority: crate::fleet::CellPriority::WriteCheck,
                env: Vec::new(),
                item,
                run_context: None,
                status_board: None,
                on_complete: None,
                purpose: None,
                publication: None,
            };
            file_check_request(
                &first,
                CheckRequestInput::Planned {
                    key: "never-granted-write".into(),
                    request: Box::new(planned),
                    phase: WriteLanePhase::Verifier,
                },
            )
            .await
            .unwrap()
        };
        let mut offers = 0;
        tokio::time::timeout(std::time::Duration::from_secs(60), async {
            loop {
                if let Some(ExecutorMessage::Offer { .. }) = machine.recv().await {
                    offers += 1;
                    return;
                }
            }
        })
        .await
        .expect("the request is offered to a machine");
        while let Ok(Some(frame)) =
            tokio::time::timeout(std::time::Duration::from_millis(500), machine.recv()).await
        {
            if matches!(frame, ExecutorMessage::Offer { .. }) {
                offers += 1;
            }
        }
        assert_eq!(offers, 1, "one request, one lane offered");
        let count = |sql: &'static str| {
            let local = local.clone();
            async move {
                local
                    .query_opt(sql, (), |row| row.i64(0))
                    .await
                    .unwrap()
                    .unwrap()
            }
        };
        assert_eq!(count("SELECT COUNT(*) FROM run_batch_detached").await, 0);
        assert_eq!(count("SELECT COUNT(*) FROM agent_waits").await, 0);
        let queued = load_request(&local, "p", &filed.id).await.unwrap().unwrap();
        assert_eq!(queued.state, "queued");
        assert!(queued.request_id.is_some(), "the lane it was submitted as");
        assert!(queued.commit_sha.is_some());

        // The runner stops: its driver and its in-memory fleet are gone.
        forget_driver_for_test(&filed.id);
        drop(machine);
        let second = runner("search-2");
        let (sender, mut machine) = tokio::sync::mpsc::unbounded_channel();
        let generation = second.fleet.attach_announced_executor(sender);
        let fleet = second.fleet.clone();
        let executor = cairn_common::task::spawn(async move {
            let entry_id = loop {
                match machine.recv().await {
                    Some(ExecutorMessage::Offer { entry_id, .. }) => break entry_id,
                    Some(_) => continue,
                    None => panic!("the re-driven request is offered again"),
                }
            };
            fleet.handle_executor_message(
                crate::fleet::COLOCATED_EXECUTOR_ID,
                generation,
                ExecutorMessage::Claim { entry_id },
            );
            let (request, batch) = loop {
                match machine.recv().await {
                    Some(ExecutorMessage::Granted {
                        request,
                        batch: Some(batch),
                        ..
                    }) => break (request, batch),
                    Some(_) => continue,
                    None => panic!("the claimed request is granted"),
                }
            };
            let outcome = passed_batch(&request, &batch.items[0].header, generation);
            fleet.handle_executor_message(
                crate::fleet::COLOCATED_EXECUTOR_ID,
                generation,
                ExecutorMessage::Result {
                    request_id: request.request_id.clone(),
                    attempt_id: request.attempt_id.clone(),
                    outcome,
                },
            );
        });
        rearm_at_boot(&second).await;
        tokio::time::timeout(std::time::Duration::from_secs(120), async {
            loop {
                let mut settled = subscribe_settlements();
                let row = load_request(&local, "p", &filed.id).await.unwrap().unwrap();
                if !row.is_open() {
                    return;
                }
                let _ =
                    tokio::time::timeout(std::time::Duration::from_millis(500), settled.changed())
                        .await;
            }
        })
        .await
        .expect("the re-driven request settles");
        executor.await.unwrap();
        let recorded = load_request(&local, "p", &filed.id).await.unwrap().unwrap();
        assert_eq!(recorded.state, "recorded", "{:?}", recorded.evidence);
        assert_eq!(recorded.commit_sha, queued.commit_sha, "the same commit");
        let observation = recorded.observation_id.clone().expect("its observation");
        assert_eq!(
            count("SELECT COUNT(*) FROM check_result_observations WHERE check_name='unit'").await,
            1,
            "recorded once"
        );
        assert!(!observation.is_empty());
        assert_eq!(count("SELECT COUNT(*) FROM run_batch_detached").await, 0);
    });
}

// --- filed from a thread -------------------------------------------------------

const THREAD: &str = "design-review";
const THREAD_HOME: &str = "cairn://p/proj/design-review";
const THREAD_RUN: &str = "run-thread";

/// A thread with a live session run, the writer every `cairn:~/` write from
/// that session (and `cairn check run` in its batches) is attributed to.
fn thread_rows_sql() -> String {
    format!(
        "INSERT INTO threads(id,project_id,name,status,attention,created_at,updated_at)
           VALUES('th','p','{THREAD}','active','none',1,1);
         INSERT INTO jobs(id,project_id,thread_id,node_name,status,uri_segment,created_at,updated_at)
           VALUES('job-thread','p','th','Thread','running','thread',1,1);
         INSERT INTO runs(id,project_id,job_id,status,created_at,updated_at)
           VALUES('{THREAD_RUN}','p','job-thread','live',1,1);"
    )
}

/// A write as the thread's session run, through the write handler: the path
/// an agent's `write` and the CLI's filing both take.
async fn write_as_thread(orch: &Orchestrator, payload: serde_json::Value) -> String {
    thread_write(orch, payload, false).await
}

async fn thread_write(orch: &Orchestrator, payload: serde_json::Value, preview: bool) -> String {
    let request = crate::mcp::types::McpCallbackRequest {
        answer_bound_ms: None,
        thread_id: None,
        cwd: String::new(),
        run_id: Some(THREAD_RUN.into()),
        tool: "write".into(),
        payload: serde_json::json!({ "changes": [{
            "target": "cairn:~/checks", "mode": "append", "payload": payload
        }], "preview": preview }),
        tool_use_id: None,
        origin: None,
    };
    crate::mcp::handlers::write::handle_write(orch, &request).await
}

async fn read_uri(orch: &Orchestrator, uri: &str) -> String {
    let request = crate::mcp::types::McpCallbackRequest {
        answer_bound_ms: None,
        thread_id: None,
        cwd: String::new(),
        run_id: None,
        tool: "read_issue_resource".into(),
        payload: serde_json::json!({ "uri": uri }),
        tool_use_id: None,
        origin: None,
    };
    crate::mcp::handlers::issue_resources::handle_read_issue_resource(orch, &request).await
}

/// A thread owns no branch, so a request from one that names no revision has
/// nothing to judge. It is refused at filing, with the way through, and no
/// row is written for a driver to fail later. Its preview refuses in the same
/// words rather than say it would file what the write refuses.
#[tokio::test]
async fn a_threads_request_without_a_revision_is_refused_before_anything_is_filed() {
    let (_root, orch) = row_fixture().await;
    orch.db
        .local
        .execute_script(thread_rows_sql())
        .await
        .unwrap();

    let previewed = thread_write(&orch, serde_json::json!({ "suite": "rust-tests" }), true).await;
    // The reply is JSON, so the refusal's quotes arrive escaped; its opening
    // sentence has none.
    let refusal = THREAD_REQUEST_NEEDS_A_REVISION
        .split_once(':')
        .map_or(THREAD_REQUEST_NEEDS_A_REVISION, |(head, _)| head);
    assert!(
        previewed.contains("a thread has no branch of its own")
            && previewed.contains(refusal)
            && !previewed.contains("Would file"),
        "{previewed}"
    );

    let refused = write_as_thread(&orch, serde_json::json!({ "suite": "rust-tests" })).await;

    assert!(
        refused.contains("a thread has no branch of its own") && refused.contains(refusal),
        "{refused}"
    );
    let filed = orch
        .db
        .local
        .query_opt("SELECT COUNT(*) FROM check_requests", (), |row| row.i64(0))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(filed, 0, "a revisionless thread request filed a row");
}

/// A thread files a check request through its own `cairn:~/checks` exactly as
/// a node does: the row names the thread's home as its requester and the
/// revision it was given, the lane runs to a verdict, and the verdict is in
/// the project ledger for that revision, where anyone can cite it. The
/// thread's checks read shows the request and offers the revision as
/// required.
#[test]
#[serial_test::serial(jj)]
fn a_threads_request_records_a_verdict_in_the_ledger() {
    use crate::execution::checks::write_check_stack_headroom::{commit, passed_batch};
    use crate::jj::tests::{init_project, jj_bin};
    use cairn_common::executor_protocol::ExecutorMessage;
    let Some(bin) = jj_bin() else {
        return;
    };
    let home = tempfile::tempdir().unwrap();
    let project = tempfile::tempdir().unwrap();
    let repo = project.path().to_path_buf();
    init_project(&repo);
    commit(
        &repo,
        &[(
            ".cairn/config.yaml",
            "checks:\n  unit:\n    command: echo ok\n    when: review\n",
        )],
        "declare unit",
    );
    crate::stack_headroom::within_budget("thread check request", move || async move {
        let config_dir = home.path().to_path_buf();
        let local = Arc::new(
            crate::storage::LocalDb::open_migrated_for_test(config_dir.join("checks.db"))
                .await
                .unwrap(),
        );
        local
            .execute_script(format!(
                "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('ws','w',1,1);
                 INSERT INTO projects(id,workspace_id,name,key,repo_path,default_branch,created_at,updated_at)
                   VALUES('p','ws','P','proj','{}','main',1,1);
                 {}",
                repo.to_string_lossy(),
                thread_rows_sql()
            ))
            .await
            .unwrap();
        let search = Arc::new(
            crate::storage::SearchIndex::open_or_create(config_dir.join("search")).unwrap(),
        );
        let orch = Orchestrator::builder(
            Arc::new(crate::db::DbState::new(local.clone(), search)),
            Arc::new(crate::services::testing::TestServicesBuilder::new().build()),
            config_dir.clone(),
        )
        .jj_binary_path(bin.clone())
        .build();
        let (sender, mut machine) = tokio::sync::mpsc::unbounded_channel();
        let generation = orch.fleet.attach_announced_executor(sender);
        let fleet = orch.fleet.clone();
        let executor = cairn_common::task::spawn(async move {
            let entry_id = loop {
                match machine.recv().await {
                    Some(ExecutorMessage::Offer { entry_id, .. }) => break entry_id,
                    Some(_) => continue,
                    None => panic!("the thread's request is offered to a machine"),
                }
            };
            fleet.handle_executor_message(
                crate::fleet::COLOCATED_EXECUTOR_ID,
                generation,
                ExecutorMessage::Claim { entry_id },
            );
            let (request, batch) = loop {
                match machine.recv().await {
                    Some(ExecutorMessage::Granted {
                        request,
                        batch: Some(batch),
                        ..
                    }) => break (request, batch),
                    Some(_) => continue,
                    None => panic!("the claimed request is granted"),
                }
            };
            let outcome = passed_batch(&request, &batch.items[0].header, generation);
            fleet.handle_executor_message(
                crate::fleet::COLOCATED_EXECUTOR_ID,
                generation,
                ExecutorMessage::Result {
                    request_id: request.request_id.clone(),
                    attempt_id: request.attempt_id.clone(),
                    outcome,
                },
            );
        });

        let reply = write_as_thread(
            &orch,
            serde_json::json!({ "suite": "unit", "revision": "main" }),
        )
        .await;
        let rows: Vec<CheckRequestRow> = local
            .query_all(
                format!("SELECT {ROW_COLUMNS} FROM check_requests"),
                (),
                CheckRequestRow::from_row,
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 1, "one suite, one request: {reply}");
        let filed = &rows[0];
        assert_eq!(filed.requester_ref, THREAD_HOME, "{reply}");
        assert_eq!(filed.requester_job_id.as_deref(), Some("job-thread"));
        assert_eq!(filed.revision_spec.as_deref(), Some("main"));

        tokio::time::timeout(std::time::Duration::from_secs(120), async {
            loop {
                let mut settled = subscribe_settlements();
                let row = load_request(&local, "p", &filed.id).await.unwrap().unwrap();
                if !row.is_open() {
                    return;
                }
                let _ =
                    tokio::time::timeout(std::time::Duration::from_millis(500), settled.changed())
                        .await;
            }
        })
        .await
        .expect("the thread's request settles");
        executor.await.unwrap();
        let recorded = load_request(&local, "p", &filed.id).await.unwrap().unwrap();
        assert_eq!(recorded.state, "recorded", "{:?}", recorded.evidence);
        assert!(recorded.observation_id.is_some(), "its ledger observation");

        let ledger = read_uri(&orch, "cairn://p/proj/check-results/main?suite=unit").await;
        assert!(
            ledger.contains("passed"),
            "the verdict is citable at the revision it named: {ledger}"
        );

        let own = read_uri(&orch, &format!("{THREAD_HOME}/checks")).await;
        assert!(own.contains("no turn-end checks run"), "{own}");
        assert!(own.contains("[unit]") && own.contains("on main"), "{own}");
        assert!(
            own.contains("a thread has no branch of its own"),
            "the filing action offers revision as required: {own}"
        );
    });
}

#[tokio::test]
async fn requested_checks_keep_the_running_machine_link_and_slot() {
    let (_root, orch) = row_fixture().await;
    let id = insert_request(&orch, "running", "").await;
    let diagnostic = "running on [fedora](cairn://executors/fedora), slot-43 since 09:01:00Z";
    record_progress(
        &orch.db.local,
        &id,
        &ManualCheckProgressUpdate {
            state: "running".into(),
            diagnostic: diagnostic.into(),
            executor: Some("fedora".into()),
            since_unix_ms: Some(1),
            request_id: Some("fleet-request".into()),
            held_by: None,
        },
    )
    .await
    .unwrap();
    let current = row(&orch, &id).await;
    assert_eq!(standing(&current), diagnostic);
    let mut awaiting_slot = current;
    awaiting_slot.diagnostic = None;
    assert!(standing(&awaiting_slot).contains("[fedora](cairn://executors/fedora), slot pending"));
}

#[tokio::test]
async fn issue_resolution_only_selects_handoff_and_carried_requests() {
    let db = crate::storage::migrated_test_db("cadence-cancellation.db").await;
    db.execute_script(
        "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
         INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
           VALUES('p','w','P','proj','/unused',1,1);
         INSERT INTO issues(id,project_id,number,title,status,progress,attention,created_at,updated_at)
           VALUES('i','p',7,'I','active','active','none',1,1);
         INSERT INTO jobs(id,project_id,issue_id,status,created_at,updated_at)
           VALUES('j','p','i','running',1,1);",
    )
    .await
    .unwrap();
    for cadence in ["requested", "write", "main_cadence", "handoff", "carried"] {
        let mut request = spec("smoke", Some("commit"));
        request.cadence = Some(cadence);
        // Attribution deliberately gives no clue to filing cadence.
        request.requester_ref = "misleading:write".into();
        accept(&db, &request, "smoke", &Coordinate::Commit("commit".into()))
            .await
            .unwrap();
    }
    let mut cadences: Vec<_> = open_cadence_requests_of_issue(&db, "i")
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.cadence.unwrap())
        .collect();
    cadences.sort();
    assert_eq!(cadences, ["carried", "handoff"]);
}
