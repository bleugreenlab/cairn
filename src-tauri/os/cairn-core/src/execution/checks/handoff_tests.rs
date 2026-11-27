use super::*;
use crate::execution::checks::requests::{load_request, subscribe_settlements};
use crate::execution::checks::write_check_stack_headroom::{commit, passed_batch};

const AGENT_BRANCH: &str = "agent/proj-7-builder-0";
const HOME: &str = "cairn://p/proj/7/1/builder";

fn checks(yaml: &str) -> HashMap<String, CheckCommand> {
    serde_yaml::from_str(yaml).unwrap()
}

const CADENCES: &str = "
w: {command: w, when: write}
r: {command: r, when: review}
h: {command: h, when: handoff}
m: {command: m, when: main, mainCadence: {platforms: [linux], notify: testing}}
";

#[test]
fn a_handoff_files_only_handoff_suites_and_a_carried_head_only_write_suites() {
    let checks = checks(CADENCES);
    assert_eq!(handoff_suites(&checks, None), vec!["h"]);
    assert_eq!(
        carried_head_suites(&checks, None),
        vec!["w"],
        "a base-advance replay never re-arms a handoff suite"
    );
    let none_apply = HashSet::new();
    assert!(handoff_suites(&checks, Some(&none_apply)).is_empty());
    assert!(carried_head_suites(&checks, Some(&none_apply)).is_empty());
}

fn open_row(id: &str, suite: &str, commit: Option<&str>, state: &str) -> CheckRequestRow {
    CheckRequestRow {
        id: id.into(),
        project_id: "p".into(),
        suite: suite.into(),
        revision_spec: commit.map(str::to_string),
        subject_job_id: Some("j".into()),
        selector_json: None,
        retry: false,
        consumer: "requested".into(),
        requester_ref: requester_label(HANDOFF, HOME),
        requester_job_id: Some("j".into()),
        state: state.into(),
        commit_sha: commit.map(str::to_string),
        request_id: None,
        executor: None,
        diagnostic: None,
        held_by: None,
        held_at_ms: None,
        held_count: 0,
        pinned_executor: None,
        waiting_to_learn: None,
        observation_id: None,
        outcome_json: None,
        evidence: None,
        cancel_requested_by: None,
        accepted_at_ms: 1,
        queued_at_ms: None,
        state_since_ms: 1,
        updated_ms: 1,
        publication_id: None,
        cadence: Some(HANDOFF.into()),
        number: 1,
    }
}

fn ids<'a>(rows: &[&'a CheckRequestRow]) -> Vec<&'a str> {
    rows.iter().map(|row| row.id.as_str()).collect()
}

/// A PR-artifact patch that did not move the head files nothing and cancels
/// nothing, whether its lane is still open or already recorded. One that
/// failed without a verdict at this head is filed again.
#[test]
fn a_handoff_on_a_head_whose_lane_is_open_or_recorded_files_nothing() {
    let changed = |_: &str| true;
    for state in ["queued", "running", "recorded"] {
        let rows = vec![
            open_row("at-head", "rust-tests", Some("new"), state),
            open_row("older", "rust-tests", Some("old"), "running"),
        ];
        assert_eq!(
            plan_suite(&rows, "rust-tests", "new", changed),
            SuitePlan::AlreadyAtHead,
            "{state}"
        );
    }
    let rows = vec![open_row("died", "rust-tests", Some("new"), "failed")];
    assert_eq!(
        plan_suite(&rows, "rust-tests", "new", changed),
        SuitePlan::File { supersede: vec![] }
    );
}

/// Per (job, suite), a handoff on a newer head cancels an open older-head lane
/// only when the suite's inputs changed between the two heads; a running lane
/// whose inputs did not change is kept, so a refresh never kills a long run to
/// restart the identical one. Settled lanes and other suites are untouched.
#[test]
fn a_newer_head_supersedes_an_older_lane_only_when_its_inputs_changed() {
    let rows = vec![
        open_row("changed", "rust-tests", Some("old-a"), "running"),
        open_row("unresolved", "rust-tests", None, "queued"),
        open_row("settled", "rust-tests", Some("old-a"), "recorded"),
        open_row("sibling", "rust-lint", Some("old-a"), "running"),
    ];
    let SuitePlan::File { supersede } = plan_suite(&rows, "rust-tests", "new", |_| true) else {
        panic!("changed inputs file a new lane");
    };
    assert_eq!(ids(&supersede), vec!["changed", "unresolved"]);

    let rows = vec![
        open_row("same-inputs", "rust-tests", Some("old-b"), "running"),
        open_row("changed", "rust-tests", Some("old-a"), "queued"),
    ];
    match plan_suite(&rows, "rust-tests", "new", |commit| commit == "old-a") {
        SuitePlan::Keep { running, supersede } => {
            assert_eq!(running.id, "same-inputs");
            assert_eq!(ids(&supersede), vec!["changed"]);
        }
        other => panic!("an unchanged running lane is kept: {other:?}"),
    }
}

#[test]
fn a_rowless_handoff_lane_says_when_it_runs_or_which_head_it_last_ran_on() {
    let head = "bbbbbbbbbbbbbbbbbbbb";
    let before = lane_notice("rust-tests", None, head);
    assert!(before.starts_with("runs at handoff"), "{before}");
    assert!(before.contains("{suite:\"rust-tests\"}"), "{before}");

    let settled = HandoffStanding {
        commit: Some("aaaaaaaaaaaaaaaaaaaa".into()),
        open: false,
        uri: "cairn://p/proj/check-requests/r1".into(),
        words: "recorded passed (fresh), cairn://p/proj/check-results/o1".into(),
    };
    let replayed = lane_notice("rust-tests", Some(&settled), head);
    assert!(
        replayed.starts_with("not checked since handoff at aaaaaaaaaaaa (there: recorded passed"),
        "the older head's verdict is named as that head's: {replayed}"
    );
    assert!(replayed.contains("cairn://p/proj/check-requests/r1"));

    let in_flight = HandoffStanding {
        commit: Some(head.into()),
        open: true,
        words: "queued since 09:01Z".into(),
        ..settled.clone()
    };
    let current = lane_notice("rust-tests", Some(&in_flight), head);
    assert!(
        current.starts_with("handoff lane at bbbbbbbbbbbb: queued since 09:01Z"),
        "{current}"
    );
}

// --- the arming seam ---------------------------------------------------------

const CONTRACT: &str = "checks:
  a-lane:
    command: echo a
    when: write
    impact: [a/**]
  b-lane:
    command: echo b
    when: write
    impact: [b/**]
  heavy:
    command: echo heavy
    when: handoff
    impact: [a/**, b/**]
";

fn recipe_snapshot() -> String {
    serde_json::json!({
        "recipe": {
            "id": "r", "name": "Recipe", "description": null, "trigger": "manual",
            "nodes": [
                {"id": "builder", "nodeType": "agent", "name": "Builder", "position": {"x": 0.0, "y": 0.0}},
                {"id": "pr", "nodeType": "pr", "name": "PR", "position": {"x": 1.0, "y": 0.0}}
            ],
            "edges": [
                {"id": "edge-1", "edgeType": "context", "sourceNodeId": "builder", "sourceHandle": "context-out", "targetNodeId": "pr", "targetHandle": "context-in"}
            ]
        },
        "agents": {}, "skills": {},
        "triggerContext": {"issueId": "i", "projectId": "p", "triggerType": "manual"},
        "createdAt": 1
    })
    .to_string()
}

/// A repository whose `main` declares [`CONTRACT`], an agent branch a commit
/// ahead touching both lanes' inputs, a PR-shipping builder job `j` on it,
/// and a job `x` whose node ships nothing.
struct Fixture {
    _home: tempfile::TempDir,
    _project: tempfile::TempDir,
    orch: Orchestrator,
    local: Arc<LocalDb>,
    repo: std::path::PathBuf,
    main: String,
    agent: String,
}

impl Fixture {
    /// The managed store re-imports the checkout, as a branch advance does.
    fn import(&self) {
        let jj = crate::jj::environment(&self.orch.jj_binary_path, &self.orch.config_dir);
        let store = crate::jj::project_store_dir(&self.orch.config_dir, &self.repo);
        crate::jj::ensure_project_store(&jj, &store, &self.repo).unwrap();
    }

    async fn rows_of(&self, job: &str) -> Vec<CheckRequestRow> {
        let mut rows = Vec::new();
        for cadence in [HANDOFF, CARRIED] {
            rows.extend(
                requests::requests_of_cadence(&self.local, job, cadence, 50)
                    .await
                    .unwrap(),
            );
        }
        rows
    }
}

async fn fixture(bin: &str) -> Fixture {
    use crate::jj::testing::{git, init_project};
    let home = tempfile::tempdir().unwrap();
    let project = tempfile::tempdir().unwrap();
    let repo = project.path().to_path_buf();
    init_project(&repo);
    let main = commit(
        &repo,
        &[(".cairn/config.yaml", CONTRACT), ("a/x", "1"), ("b/y", "1")],
        "contract",
    );
    git(&repo, &["checkout", "-q", "-b", AGENT_BRANCH]);
    let agent = commit(&repo, &[("a/x", "2"), ("b/y", "2")], "agent work");
    git(&repo, &["checkout", "-q", "main"]);

    let config_dir = home.path().to_path_buf();
    let local = Arc::new(
        LocalDb::open_migrated_for_test(config_dir.join("checks.db"))
            .await
            .unwrap(),
    );
    local
        .execute_script(format!(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('ws','w',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,default_branch,created_at,updated_at)
               VALUES('p','ws','P','proj','{repo}','main',1,1);
             INSERT INTO issues(id,project_id,number,title,status,progress,attention,created_at,updated_at)
               VALUES('i','p',7,'I','active','active','none',1,1);
             INSERT INTO jobs(id,project_id,issue_id,status,node_name,uri_segment,branch,base_branch,created_at,updated_at)
               VALUES('x','p','i','running','helper','helper','{AGENT_BRANCH}','main',1,1);",
            repo = repo.to_string_lossy()
        ))
        .await
        .unwrap();
    local
        .execute(
            "INSERT INTO executions(id,recipe_id,issue_id,project_id,status,snapshot,started_at,seq)
               VALUES('e','r','i','p','running',?1,1,1)",
            (recipe_snapshot(),),
        )
        .await
        .unwrap();
    local
        .execute(
            "INSERT INTO jobs(id,project_id,issue_id,execution_id,recipe_node_id,status,node_name,uri_segment,branch,base_branch,created_at,updated_at)
               VALUES('j','p','i','e','builder','running','builder','builder',?1,'main',1,1)",
            (AGENT_BRANCH,),
        )
        .await
        .unwrap();
    let search =
        Arc::new(crate::storage::SearchIndex::open_or_create(config_dir.join("search")).unwrap());
    let orch = Orchestrator::builder(
        Arc::new(crate::db::DbState::new(local.clone(), search)),
        Arc::new(crate::services::testing::TestServicesBuilder::new().build()),
        config_dir,
    )
    .jj_binary_path(bin.to_string())
    .build();
    let fixture = Fixture {
        _home: home,
        _project: project,
        orch,
        local,
        repo,
        main,
        agent,
    };
    fixture.import();
    fixture
}

/// The arming seam, end to end through the request seam: a PR-artifact write
/// by a PR-shipping job files one handoff lane per handoff suite at the job's
/// logical head and supersedes the lane an earlier handoff left open on an
/// older head whose inputs differ; a second write on the same head files and
/// cancels nothing; a base-advance replay files the carried head's write
/// lanes and no handoff suite; and a job whose node ships no PR arms nothing.
#[tokio::test]
async fn a_pr_artifact_write_arms_one_handoff_wave_and_a_replay_arms_none() {
    let Some(bin) = crate::jj::testing::jj_bin() else {
        return;
    };
    let f = fixture(&bin).await;
    let orch = &f.orch;
    f.local
        .execute(
            "INSERT INTO check_requests(id,project_id,suite,subject_job_id,consumer,requester_ref,requester_job_id,
                priority,state,commit_sha,open_key,accepted_at_ms,state_since_ms,updated_ms,cadence)
             VALUES('older','p','heavy','j','requested',?1,'j','review_check','running',?2,'older-key',1,1,1,'handoff')",
            (requester_label(HANDOFF, HOME), f.main.clone()),
        )
        .await
        .unwrap();

    let (head, filed, superseded) = match arm_on_pr_artifact(orch, "j").await.unwrap() {
        Armed::Wave {
            head,
            filed,
            superseded,
            ..
        } => (head, filed, superseded),
        other => panic!("a PR-shipping job's artifact write arms a wave: {other:?}"),
    };
    assert_eq!(head, f.agent, "at the job's logical head");
    assert_eq!(
        filed.iter().map(|r| r.suite.as_str()).collect::<Vec<_>>(),
        vec!["heavy"],
        "one lane per handoff suite, and no write or review suite"
    );
    let lane = load_request(&f.local, "p", &filed[0].id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(lane.commit_sha.as_deref(), Some(f.agent.as_str()));
    assert_eq!(lane.cadence.as_deref(), Some(HANDOFF));
    assert_eq!(lane.requester_ref, requester_label(HANDOFF, HOME));
    assert_eq!(lane.consumer, "requested", "filed as a requested check");
    assert_eq!(superseded, vec!["older".to_string()]);
    let older = load_request(&f.local, "p", "older").await.unwrap().unwrap();
    assert_eq!(older.state, "cancelled", "{older:?}");
    assert!(
        older
            .evidence
            .as_deref()
            .is_some_and(|evidence| evidence.contains("superseded")),
        "{older:?}"
    );

    // A PR-artifact patch that did not move the head (a body edit). The first
    // lane's driver is stopped so the row stays open, as a queued lane's does
    // while it waits for a machine.
    requests::forget_driver_for_test(&lane.id);
    let open_before = f
        .rows_of("j")
        .await
        .iter()
        .filter(|row| row.is_open())
        .count();
    match arm_on_pr_artifact(orch, "j").await.unwrap() {
        Armed::Wave {
            filed,
            superseded,
            already,
            ..
        } => {
            let lane = load_request(&f.local, "p", &lane.id)
                .await
                .unwrap()
                .unwrap();
            assert!(filed.is_empty(), "nothing filed: {filed:?} ({lane:?})");
            assert!(superseded.is_empty(), "nothing cancelled: {superseded:?}");
            assert_eq!(already, vec!["heavy"]);
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(
        f.rows_of("j")
            .await
            .iter()
            .filter(|row| row.is_open())
            .count(),
        open_before,
        "one open row per suite"
    );

    let before = f.rows_of("j").await.len();
    let Armed::Wave { filed, .. } = file_carried_head_lanes(orch, "j").await.unwrap() else {
        panic!("a replayed PR-shipping job owes its carried head's write lanes");
    };
    assert_eq!(
        filed.iter().map(|r| r.suite.as_str()).collect::<Vec<_>>(),
        vec!["a-lane", "b-lane"],
        "a base-advance replay arms no handoff suite"
    );
    let handoff_rows = f
        .rows_of("j")
        .await
        .into_iter()
        .filter(|row| row.cadence.as_deref() == Some(HANDOFF))
        .count();
    assert_eq!(
        handoff_rows, 2,
        "still only `older` and the one handoff lane"
    );
    assert_eq!(f.rows_of("j").await.len(), before + 2);

    assert!(matches!(
        arm_on_pr_artifact(orch, "x").await.unwrap(),
        Armed::NotShipping
    ));
    assert!(matches!(
        file_carried_head_lanes(orch, "x").await.unwrap(),
        Armed::NotShipping
    ));
    let any_for_x = f
        .local
        .query_opt(
            "SELECT COUNT(*) FROM check_requests WHERE requester_job_id='x'",
            (),
            |row| row.i64(0),
        )
        .await
        .unwrap();
    assert_eq!(any_for_x, Some(0), "a non-PR-shipping job arms nothing");
}

async fn settle_all(local: &LocalDb, ids: &[String]) {
    tokio::time::timeout(std::time::Duration::from_secs(120), async {
        loop {
            let mut settled = subscribe_settlements();
            let mut open = false;
            for id in ids {
                open |= load_request(local, "p", id)
                    .await
                    .unwrap()
                    .unwrap()
                    .is_open();
            }
            if !open {
                return;
            }
            let _ = tokio::time::timeout(std::time::Duration::from_millis(500), settled.changed())
                .await;
        }
    })
    .await
    .expect("the lanes settle");
}

async fn disposition(local: &LocalDb, filed: &FiledRequest) -> String {
    let row = load_request(local, "p", &filed.id).await.unwrap().unwrap();
    match CheckRequestAnswer::of_settled(&row) {
        Some(CheckRequestAnswer::Complete { result }) => {
            assert!(result.passed && result.no_verdict.is_none(), "{row:?}");
            result.disposition.clone()
        }
        other => panic!("{} settled without a verdict: {other:?}", filed.suite),
    }
}

/// A carried head owes its cheap lanes a verdict, not a rerun of each: after
/// a base advance that changed one write lane's inputs and not the other's,
/// the changed lane runs and the unchanged one is answered from the ledger.
#[test]
#[serial_test::serial(jj)]
fn a_carried_head_runs_its_changed_write_lane_and_reuses_the_unchanged_one() {
    use crate::jj::testing::git;
    use cairn_common::executor_protocol::ExecutorMessage;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let Some(bin) = crate::jj::testing::jj_bin() else {
        return;
    };
    crate::stack_headroom::within_budget("carried head lanes", move || async move {
        let f = fixture(&bin).await;
        let (sender, mut machine) = tokio::sync::mpsc::unbounded_channel();
        let generation = f.orch.fleet.attach_announced_executor(sender);
        let fleet = f.orch.fleet.clone();
        let runs = Arc::new(AtomicUsize::new(0));
        let offers = Arc::new(AtomicUsize::new(0));
        let counted = runs.clone();
        let offered = offers.clone();
        let executor = cairn_common::task::spawn(async move {
            while let Some(message) = machine.recv().await {
                match message {
                    ExecutorMessage::Offer { entry_id, .. } => {
                        offered.fetch_add(1, Ordering::SeqCst);
                        fleet.handle_executor_message(
                            crate::fleet::COLOCATED_EXECUTOR_ID,
                            generation,
                            ExecutorMessage::Claim { entry_id },
                        );
                    }
                    ExecutorMessage::Granted {
                        request,
                        batch: Some(batch),
                        ..
                    } => {
                        counted.fetch_add(1, Ordering::SeqCst);
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
                    }
                    _ => {}
                }
            }
        });

        let Armed::Wave { filed, .. } = file_carried_head_lanes(&f.orch, "j").await.unwrap() else {
            panic!("the first head's write lanes are filed");
        };
        let ids: Vec<String> = filed.iter().map(|r| r.id.clone()).collect();
        settle_all(&f.local, &ids).await;
        assert_eq!(
            runs.load(Ordering::SeqCst),
            2,
            "both lanes ran on the first head"
        );

        // Main moves under `a/` only, and the branch is replayed onto it.
        commit(&f.repo, &[("a/w", "main moved")], "main advance");
        git(&f.repo, &["checkout", "-q", AGENT_BRANCH]);
        git(&f.repo, &["rebase", "-q", "main"]);
        git(&f.repo, &["checkout", "-q", "main"]);
        f.import();

        let Armed::Wave { head, filed, .. } = file_carried_head_lanes(&f.orch, "j").await.unwrap()
        else {
            panic!("the carried head's write lanes are filed");
        };
        assert_ne!(head, f.agent, "the replay carried the branch to a new head");
        let ids: Vec<String> = filed.iter().map(|r| r.id.clone()).collect();
        settle_all(&f.local, &ids).await;
        let by_suite: HashMap<&str, &FiledRequest> =
            filed.iter().map(|r| (r.suite.as_str(), r)).collect();
        assert_eq!(disposition(&f.local, by_suite["a-lane"]).await, "fresh");
        assert_eq!(disposition(&f.local, by_suite["b-lane"]).await, "cached");
        assert_eq!(
            runs.load(Ordering::SeqCst),
            3,
            "only the lane whose inputs changed ran again"
        );
        // The unchanged lane was answered from the ledger at filing: it never
        // reached the fleet, so the dispatcher saw one entry for this head.
        assert_eq!(
            offers.load(Ordering::SeqCst),
            3,
            "one offer per lane that ran, none for the reused one"
        );
        let reused = load_request(&f.local, "p", &by_suite["b-lane"].id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            reused.request_id, None,
            "the reused lane was never submitted to the fleet: {reused:?}"
        );
        executor.abort();
    });
}
