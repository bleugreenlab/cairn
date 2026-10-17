//! The commissioning gate end to end: a thread's filing through both doors,
//! the `tenet_verdict` gate the sweep resolves, the verdict's delivery, and
//! the foreclosure that wakes the proposing thread.

use std::sync::Arc;

use cairn_common::identity::PrincipalRef;
use serde_json::{json, Value};

use super::filing::{TenetClaim, TenetFiling};
use super::{Answer, Answered, Proposal, Proposer, TenetOrigin, TenetStatus, Verdict};
use crate::db::DbState;
use crate::execution::gates::sweep::sweep_at;
use crate::execution::recipe::LaunchIntent;
use crate::mcp::handlers::executions::{
    patch_execution_gates, start_execution_from_collection, GatePatch,
};
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::{Orchestrator, OrchestratorBuilder};
use crate::services::testing::{RecordingProcessSpawner, TestServicesBuilder};
use crate::storage::{LocalDb, RowExt, SearchIndex};

const NOW: i64 = 1_900_000_000;
const RECIPE: &str = "memory-triage";
/// The planning thread's session run.
const THREAD_RUN: &str = "run-thread";
/// A coordinator working issue 9: an agent, but acting for no thread.
const COORD_RUN: &str = "run-coord";
const TENET_1: &str = "cairn://p/prj/tenets/1";

struct Env {
    _temp: tempfile::TempDir,
    orch: Orchestrator,
    db: Arc<LocalDb>,
}

/// A project `prj` whose operator gates `Memory-Triage` (spelled in another
/// case, to hold the comparison to case-insensitivity), a planning thread with
/// a live session run, another thread, a coordinator on issue 9, and its live
/// children 1..=3.
async fn env(name: &str) -> Env {
    let temp = tempfile::tempdir().unwrap();
    let db = Arc::new(crate::storage::migrated_test_db(name).await);
    let config_dir = temp.path().join("config");
    let project_dir = temp.path().join("project");
    std::fs::create_dir_all(config_dir.join("agents")).unwrap();
    std::fs::create_dir_all(config_dir.join("recipes")).unwrap();
    std::fs::create_dir_all(project_dir.join(".cairn/agents")).unwrap();
    std::fs::write(
        config_dir.join("recipes/memory-triage.yaml"),
        include_str!("../../../../packs/core/recipes/memory-triage.yaml"),
    )
    .unwrap();
    std::fs::write(
        config_dir.join("agents/integrator.md"),
        include_str!("../../../../packs/core/agents/integrator.md"),
    )
    .unwrap();
    let mut seed = format!(
        "INSERT INTO projects(id, workspace_id, name, key, repo_path, next_issue_number, created_at, updated_at)
           VALUES('p', 'default', 'Project', 'prj', '{0}', 10, 1, 1);
         INSERT INTO threads (id, project_id, name, status, attention, created_at, updated_at)
           VALUES ('t-plan', 'p', 'plan', 'active', 'none', 1, 1);
         INSERT INTO jobs (id, thread_id, project_id, node_name, agent_config_id, status, created_at, updated_at, uri_segment)
           VALUES ('j-thread', 't-plan', 'p', 'thread', 'thread', 'running', 1, 1, 'thread');
         INSERT INTO runs (id, project_id, job_id, status, created_at, updated_at)
           VALUES ('run-thread', 'p', 'j-thread', 'live', 1, 1);
         INSERT INTO threads (id, project_id, name, status, attention, created_at, updated_at)
           VALUES ('t-other', 'p', 'other', 'active', 'none', 1, 1);
         INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
           VALUES('ip', 'p', 9, 'Parent', 'active', 'active', 'none', 1, 1);
         INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
           VALUES('ep', 'recipe', 'ip', 'p', 'running', 1, 1);
         INSERT INTO jobs(id, execution_id, project_id, issue_id, node_name, status, current_session_id, uri_segment, created_at, updated_at)
           VALUES('coord', 'ep', 'p', 'ip', 'Coordinator', 'running', 'sess', 'coordinator', 1, 1);
         INSERT INTO runs(id, project_id, job_id, issue_id, status, created_at, updated_at)
           VALUES('run-coord', 'p', 'coord', 'ip', 'live', 1, 1);
         INSERT INTO projects(id, workspace_id, name, key, repo_path, next_issue_number, created_at, updated_at)
           VALUES('q', 'default', 'Other', 'oth', '{0}', 10, 1, 1);
         INSERT INTO threads (id, project_id, name, status, attention, created_at, updated_at)
           VALUES ('t-q', 'q', 'qplan', 'active', 'none', 1, 1);
         INSERT INTO jobs (id, thread_id, project_id, node_name, agent_config_id, status, created_at, updated_at, uri_segment)
           VALUES ('j-q', 't-q', 'q', 'thread', 'thread', 'running', 1, 1, 'thread');
         INSERT INTO runs (id, project_id, job_id, status, created_at, updated_at)
           VALUES ('run-q', 'q', 'j-q', 'live', 1, 1);
         INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
           VALUES('iq1', 'q', 1, 'Other issue', 'active', 'active', 'none', 1, 1);",
        project_dir.display()
    );
    for number in 1..=3 {
        seed.push_str(&format!(
            "INSERT INTO issues(id, project_id, number, title, status, progress, attention, parent_issue_id, parent_job_id, created_at, updated_at)
               VALUES('i{number}', 'p', {number}, 'Issue {number}', 'active', 'active', 'none', 'ip', 'coord', 1, 1);"
        ));
    }
    db.execute_script(&seed).await.unwrap();

    let search = Arc::new(SearchIndex::open_or_create(temp.path().join("search")).unwrap());
    let mut clock = crate::services::testing::MockClock::new();
    clock.expect_now().returning(|| NOW);
    clock.expect_now_u64().returning(|| NOW as u64);
    let services = Arc::new(
        TestServicesBuilder::new()
            .with_clock(clock)
            .with_process(RecordingProcessSpawner::new().clone())
            .build(),
    );
    let orch = OrchestratorBuilder::new(
        Arc::new(DbState::new(db.clone(), search)),
        services,
        config_dir,
    )
    .build();
    super::set_gated_recipes(
        &db,
        orch.services.emitter.as_ref(),
        "p",
        &[
            "Memory-Triage".to_string(),
            "nonexistent-recipe".to_string(),
        ],
    )
    .await
    .unwrap();
    Env {
        _temp: temp,
        orch,
        db,
    }
}

fn proposal() -> TenetClaim {
    TenetClaim::Proposal {
        question: "Plan the memory triage now?".into(),
        options: vec!["Plan it now".into(), "Wait a release".into()],
    }
}

async fn file_recipe(
    env: &Env,
    number: i32,
    recipe: &str,
    run: Option<&str>,
    claim: Option<TenetClaim>,
) -> Result<String, String> {
    file_in(env, "prj", number, recipe, run, claim).await
}

async fn file_in(
    env: &Env,
    project: &str,
    number: i32,
    recipe: &str,
    run: Option<&str>,
    claim: Option<TenetClaim>,
) -> Result<String, String> {
    start_execution_from_collection(
        &env.orch,
        project,
        number,
        Some(recipe),
        None,
        None,
        None,
        None,
        LaunchIntent::NewAttempt,
        Vec::new(),
        TenetFiling {
            caller_run_id: run.map(str::to_string),
            claim,
            key_prefix: "payload",
        },
    )
    .await
}

async fn file(
    env: &Env,
    number: i32,
    run: Option<&str>,
    claim: Option<TenetClaim>,
) -> Result<String, String> {
    file_recipe(env, number, RECIPE, run, claim).await
}

async fn texts(env: &Env, sql: &str) -> Vec<String> {
    env.db.query_all(sql, (), |row| row.text(0)).await.unwrap()
}

async fn count(env: &Env, sql: &str) -> i64 {
    env.db.query_opt_i64(sql, ()).await.unwrap().unwrap()
}

/// The newest execution on issue `number`: (id, status).
async fn execution(env: &Env, number: i32) -> (String, String) {
    env.db
        .query_all(
            &format!(
                "SELECT e.id, e.status FROM executions e JOIN issues i ON i.id = e.issue_id
                  WHERE i.number = {number} ORDER BY e.seq DESC LIMIT 1"
            ),
            (),
            |row| Ok((row.text(0)?, row.text(1)?)),
        )
        .await
        .unwrap()
        .pop()
        .expect("an execution")
}

fn human() -> PrincipalRef {
    PrincipalRef::Human {
        issuer: "https://auth.example".into(),
        subject: "alice".into(),
        organization: None,
    }
}

async fn answer(env: &Env, number: i64, verdict: Verdict, comment: Option<&str>) -> Answered {
    super::answer_and_wake(
        &env.orch,
        &env.db,
        "p",
        number,
        Answer {
            verdict,
            comment: comment.map(str::to_string),
            principal: human(),
        },
    )
    .await
    .unwrap()
}

async fn thread_messages(env: &Env) -> Vec<String> {
    texts(
        env,
        "SELECT content FROM messages WHERE recipient_run_id = 'run-thread' ORDER BY created_at, id",
    )
    .await
}

fn request(run: Option<&str>) -> McpCallbackRequest {
    McpCallbackRequest {
        thread_id: None,
        cwd: "/tmp".to_string(),
        run_id: run.map(str::to_string),
        tool: "write".to_string(),
        payload: Value::Null,
        tool_use_id: None,
    }
}

async fn write(env: &Env, run: &str, target: &str, payload: Value) -> Result<String, String> {
    crate::resources::mutations::dispatch_resource_change(
        &env.orch,
        &request(Some(run)),
        0,
        &ChangeItem {
            target: target.to_string(),
            mode: ChangeMode::Append,
            payload: Some(payload),
        },
        false,
    )
    .await
    .map(|change| change.summary)
    .map_err(|failure| failure.error)
}

async fn patch_gates(env: &Env, run: &str, patch: GatePatch) -> Result<String, String> {
    patch_gates_in(env, run, "prj", patch).await
}

async fn patch_gates_in(
    env: &Env,
    run: &str,
    project: &str,
    patch: GatePatch,
) -> Result<String, String> {
    patch_execution_gates(&env.orch, &request(Some(run)), project, 1, 1, patch, false).await
}

/// A standing tenet the planning thread proposed on its own.
async fn standing_tenet(env: &Env) -> i64 {
    standing_tenet_of(env, "p", "t-plan", "cairn://p/prj/plan", THREAD_RUN).await
}

async fn standing_tenet_of(env: &Env, project: &str, thread: &str, node: &str, run: &str) -> i64 {
    super::propose(
        &env.db,
        env.orch.services.emitter.as_ref(),
        NOW,
        &Proposer {
            project_id: project.into(),
            thread_id: thread.into(),
            principal: PrincipalRef::Agent {
                node: node.into(),
                run_id: Some(run.into()),
            },
        },
        Proposal {
            question: "Which plan?".into(),
            options: vec!["The whole plan".into()],
            supersedes: None,
            origin: TenetOrigin::Standing,
        },
    )
    .await
    .unwrap()
    .number
}

#[tokio::test]
async fn a_thread_filing_a_gated_recipe_with_a_proposal_is_held_on_it() {
    let env = env("tenet-gate-held.db").await;
    let summary = file(&env, 1, Some(THREAD_RUN), Some(proposal()))
        .await
        .unwrap();
    assert!(summary.contains("HELD"), "{summary}");
    assert!(summary.contains(TENET_1), "names the tenet: {summary}");
    assert!(summary.contains("Tenets tab"), "{summary}");
    assert!(
        !summary.contains("release:true"),
        "a thread is not offered the early release it would be refused: {summary}"
    );
    assert!(
        summary.contains("after tenet #1 answered (unverified desktop answer) (no verdict yet)"),
        "{summary}"
    );

    let (execution_id, status) = execution(&env, 1).await;
    assert_eq!(status, "held");
    let tenet = super::get(&env.db, "p", 1).await.unwrap().unwrap();
    assert_eq!(tenet.row.status, TenetStatus::Proposed);
    assert_eq!(tenet.row.origin, TenetOrigin::FilingGate);
    assert_eq!(tenet.row.proposing_thread_id, "t-plan");
    assert_eq!(
        tenet.row.proposer,
        PrincipalRef::Agent {
            node: "cairn://p/prj/plan".into(),
            run_id: Some(THREAD_RUN.into()),
        }
    );
    assert_eq!(tenet.gates.len(), 1, "the read projection finds the gate");
    assert!(tenet.gates[0].held);

    let predicate: Value =
        serde_json::from_str(&texts(&env, "SELECT predicate_json FROM execution_gates").await[0])
            .unwrap();
    assert_eq!(
        predicate,
        json!({"fact": "tenet_verdict", "tenet": TENET_1, "outcome": "answered"}),
        "the policy at filing time is stamped on the gate"
    );

    // Census prose is the fact's own rendering.
    let census = env
        .db
        .read(move |conn| {
            let execution_id = execution_id.clone();
            Box::pin(async move {
                crate::execution::gates::census::census_for_executions_conn(conn, &[execution_id])
                    .await
            })
        })
        .await
        .unwrap();
    let census = census.values().next().unwrap();
    assert_eq!(
        census.gates[0].predicate,
        "tenet #1 answered (unverified desktop answer)"
    );
}

#[tokio::test]
async fn a_gated_filing_naming_no_tenet_or_both_is_refused_before_anything_is_written() {
    let env = env("tenet-gate-refused.db").await;
    let missing = file(&env, 1, Some(THREAD_RUN), None).await.unwrap_err();
    assert!(missing.contains("payload.proposal"), "{missing}");
    assert!(missing.contains("payload.tenet"), "{missing}");
    assert!(missing.contains("`build` is for repair only"), "{missing}");

    let both = write(
        &env,
        THREAD_RUN,
        "cairn://p/prj/1/executions",
        json!({"recipe": RECIPE, "proposal": {"question": "Q?", "options": ["a"]}, "tenet": 1}),
    )
    .await
    .unwrap_err();
    assert!(
        both.contains("exactly one of payload.proposal or payload.tenet"),
        "{both}"
    );

    // The issue-create door refuses before the issue exists.
    let create = write(
        &env,
        THREAD_RUN,
        "cairn://p/prj/issues",
        json!({"title": "Unasked plan", "execution": {"recipe": RECIPE}}),
    )
    .await
    .unwrap_err();
    assert!(create.contains("payload.execution.proposal"), "{create}");

    assert_eq!(
        count(
            &env,
            "SELECT COUNT(*) FROM executions WHERE issue_id != 'ip'"
        )
        .await,
        0
    );
    assert_eq!(count(&env, "SELECT COUNT(*) FROM tenets").await, 0);
    assert_eq!(
        count(
            &env,
            "SELECT COUNT(*) FROM issues WHERE title = 'Unasked plan'"
        )
        .await,
        0,
        "a refused filing leaves no orphan issue"
    );
}

#[tokio::test]
async fn ungated_recipes_and_filers_acting_for_no_thread_pass_through() {
    let env = env("tenet-gate-ungated.db").await;
    // A coordinator's filing of a gated recipe is not the thread's to gate.
    let summary = file(&env, 1, Some(COORD_RUN), None).await.unwrap();
    assert!(summary.starts_with("Started"), "{summary}");
    // Neither is a caller with no run at all.
    let summary = file(&env, 2, None, None).await.unwrap();
    assert!(summary.starts_with("Started"), "{summary}");
    // A caller acting for no thread cannot name a tenet it does not own.
    let refusal = file(&env, 3, Some(COORD_RUN), Some(proposal()))
        .await
        .unwrap_err();
    assert_eq!(refusal, super::filing::NON_THREAD_FILING_REFUSAL);

    // With the recipe ungated, the thread's own filing starts unheld: repair
    // stays the thread's call.
    super::set_gated_recipes(
        &env.db,
        env.orch.services.emitter.as_ref(),
        "p",
        &["planbuild".to_string()],
    )
    .await
    .unwrap();
    let summary = file(&env, 3, Some(THREAD_RUN), None).await.unwrap();
    assert!(summary.starts_with("Started"), "{summary}");
    assert_eq!(count(&env, "SELECT COUNT(*) FROM execution_gates").await, 0);
    assert_eq!(count(&env, "SELECT COUNT(*) FROM tenets").await, 0);
}

#[tokio::test]
async fn both_doors_gate_a_thread_filing() {
    let env = env("tenet-gate-doors.db").await;
    let appended = write(
        &env,
        THREAD_RUN,
        "cairn://p/prj/1/executions",
        json!({"recipe": RECIPE, "proposal": {"question": "Plan it?", "options": ["yes", "no"]}}),
    )
    .await
    .unwrap();
    assert!(
        appended.contains("HELD") && appended.contains(TENET_1),
        "{appended}"
    );

    let created = write(
        &env,
        THREAD_RUN,
        "cairn://p/prj/issues",
        json!({"title": "Second plan", "execution": {"recipe": RECIPE, "tenet": 1}}),
    )
    .await
    .unwrap();
    assert!(
        created.contains("HELD") && created.contains(TENET_1),
        "{created}"
    );
    assert_eq!(
        count(
            &env,
            "SELECT COUNT(*) FROM executions WHERE status = 'held'"
        )
        .await,
        2,
        "one tenet holds a whole plan's filings"
    );
    assert_eq!(count(&env, "SELECT COUNT(*) FROM tenets").await, 1);
}

/// The gate checks the recipe the launch resolves, not the caller's spelling:
/// every path or file-name spelling the loader could once reach is refused at
/// both doors, naming the recipe id, and a case variant is the same gated
/// recipe. None of them starts the recipe ungated.
#[tokio::test]
async fn no_spelling_of_a_gated_recipe_files_it_past_the_gate() {
    let env = env("tenet-gate-spellings.db").await;
    let absolute = env
        .orch
        .config_dir
        .join("recipes")
        .join(RECIPE)
        .display()
        .to_string();
    let spellings = [
        format!("./{RECIPE}"),
        format!("{RECIPE}.yaml"),
        format!("../recipes/{RECIPE}"),
        format!("recipes/../{RECIPE}"),
        format!("..\\recipes\\{RECIPE}"),
        format!(".{RECIPE}"),
        absolute,
    ];
    for spelling in &spellings {
        let appended = write(
            &env,
            THREAD_RUN,
            "cairn://p/prj/1/executions",
            json!({"recipe": spelling}),
        )
        .await
        .unwrap_err();
        assert!(
            appended.contains("is not a recipe id") && appended.contains("Use `memory-triage`"),
            "{spelling}: {appended}"
        );
        let created = write(
            &env,
            THREAD_RUN,
            "cairn://p/prj/issues",
            json!({"title": "Spelled plan", "execution": {"recipe": spelling}}),
        )
        .await
        .unwrap_err();
        assert!(
            created.contains("is not a recipe id") && created.contains("Use `memory-triage`"),
            "{spelling}: {created}"
        );
    }
    assert_eq!(
        count(
            &env,
            "SELECT COUNT(*) FROM issues WHERE title = 'Spelled plan'"
        )
        .await,
        0,
        "a refused spelling leaves no orphan issue"
    );

    // A case variant names the same file, so it is the same gated recipe.
    let bare = write(
        &env,
        THREAD_RUN,
        "cairn://p/prj/1/executions",
        json!({"recipe": "MEMORY-Triage"}),
    )
    .await
    .unwrap_err();
    assert!(bare.contains("payload.proposal"), "{bare}");
    let created = write(
        &env,
        THREAD_RUN,
        "cairn://p/prj/issues",
        json!({"title": "Cased plan", "execution": {"recipe": "MEMORY-Triage"}}),
    )
    .await
    .unwrap_err();
    assert!(created.contains("payload.execution.proposal"), "{created}");

    // Behind a tenet it files HELD, and launches the canonical id it checked.
    let held = write(
        &env,
        THREAD_RUN,
        "cairn://p/prj/1/executions",
        json!({"recipe": "Memory-TRIAGE", "proposal": {"question": "Plan it?", "options": ["yes"]}}),
    )
    .await
    .unwrap();
    assert!(
        held.contains("HELD") && held.contains("(recipe 'memory-triage')"),
        "{held}"
    );

    assert_eq!(
        texts(
            &env,
            "SELECT status || ' ' || recipe_id FROM executions WHERE issue_id != 'ip'"
        )
        .await,
        vec!["held memory-triage".to_string()],
        "nothing started ungated"
    );
}

#[tokio::test]
async fn naming_an_approved_tenet_releases_at_creation() {
    let env = env("tenet-gate-approved.db").await;
    let number = standing_tenet(&env).await;
    answer(&env, number, Verdict::Option(1), Some("go")).await;
    let summary = file(
        &env,
        1,
        Some(THREAD_RUN),
        Some(TenetClaim::Existing(number)),
    )
    .await
    .unwrap();
    assert!(summary.starts_with("Started"), "{summary}");
    assert!(summary.contains("already approved"), "{summary}");
    assert_eq!(execution(&env, 1).await.1, "running");
    // The planner that starts reads the decision from its own issue, though
    // the tenet was answered before the issue was gated on it.
    let comments = texts(&env, "SELECT content FROM comments WHERE issue_id = 'i1'").await;
    assert_eq!(
        comments,
        vec![
            "Tenet #1 approved by the operator (signed in as alice): The whole plan. \
             Rejected: none. Comment: go."
                .to_string()
        ]
    );
    // A second filing of the plan gets its own copy, and a re-sweep no other.
    file(
        &env,
        2,
        Some(THREAD_RUN),
        Some(TenetClaim::Existing(number)),
    )
    .await
    .unwrap();
    sweep_at(&env.orch, NOW).await.unwrap();
    assert_eq!(
        count(&env, "SELECT COUNT(*) FROM comments WHERE issue_id = 'i1'").await,
        1
    );
    assert_eq!(
        count(&env, "SELECT COUNT(*) FROM comments WHERE issue_id = 'i2'").await,
        1
    );
}

#[tokio::test]
async fn a_regate_onto_another_projects_same_numbered_tenet_is_refused() {
    let env = env("tenet-gate-foreign.db").await;
    // Another project's #1, approved.
    let foreign = standing_tenet_of(&env, "q", "t-q", "cairn://p/oth/qplan", "run-q").await;
    super::answer_and_wake(
        &env.orch,
        &env.db,
        "q",
        foreign,
        Answer {
            verdict: Verdict::Option(1),
            comment: None,
            principal: human(),
        },
    )
    .await
    .unwrap();
    // This project's #1, still unanswered.
    file(&env, 1, Some(THREAD_RUN), Some(proposal()))
        .await
        .unwrap();

    let onto_foreign = json!([{"fact": "tenet_verdict", "tenet": "cairn://p/oth/tenets/1", "outcome": "answered"}]);
    let clauses =
        crate::execution::gates::authoring::parse_after(Some(&onto_foreign), "payload.after")
            .unwrap();
    let refusal = patch_gates(&env, THREAD_RUN, GatePatch::Replace(clauses))
        .await
        .unwrap_err();
    assert!(refusal.contains("another project's tenet"), "{refusal}");
    sweep_at(&env.orch, NOW).await.unwrap();
    assert_eq!(
        execution(&env, 1).await.1,
        "held",
        "an unrelated approval releases nothing"
    );
}

#[tokio::test]
async fn a_thread_of_another_project_cannot_commission_or_lift_a_gate_here() {
    let env = env("tenet-gate-cross-project.db").await;
    super::set_gated_recipes(
        &env.db,
        env.orch.services.emitter.as_ref(),
        "q",
        &[RECIPE.to_string()],
    )
    .await
    .unwrap();
    // The planning thread of `prj` files gated work into `oth`: neither bare
    // nor carrying a proposal passes as a person's filing.
    for claim in [None, Some(proposal())] {
        let refusal = file_in(&env, "oth", 1, RECIPE, Some(THREAD_RUN), claim)
            .await
            .unwrap_err();
        assert_eq!(refusal, super::filing::CROSS_PROJECT_REFUSAL);
    }
    assert_eq!(
        count(
            &env,
            "SELECT COUNT(*) FROM executions WHERE issue_id = 'iq1'"
        )
        .await,
        0
    );

    // `oth`'s own thread files it behind its own tenet, and `prj`'s thread
    // cannot release that filing.
    let held = file_in(&env, "oth", 1, RECIPE, Some("run-q"), Some(proposal()))
        .await
        .unwrap();
    assert!(held.contains("cairn://p/oth/tenets/1"), "{held}");
    let refusal = patch_gates_in(&env, THREAD_RUN, "oth", GatePatch::Release)
        .await
        .unwrap_err();
    assert_eq!(refusal, super::filing::CROSS_PROJECT_REFUSAL);
    assert_eq!(
        texts(&env, "SELECT status FROM executions WHERE issue_id = 'iq1'").await,
        vec!["held".to_string()]
    );
}

#[tokio::test]
async fn a_filing_holds_only_on_its_own_threads_live_tenet() {
    let env = env("tenet-gate-existing.db").await;
    let missing = file(&env, 1, Some(THREAD_RUN), Some(TenetClaim::Existing(7)))
        .await
        .unwrap_err();
    assert!(missing.contains("no tenet #7"), "{missing}");
    let number = standing_tenet(&env).await;
    answer(&env, number, Verdict::None, Some("no")).await;
    let declined = file(
        &env,
        1,
        Some(THREAD_RUN),
        Some(TenetClaim::Existing(number)),
    )
    .await
    .unwrap_err();
    assert!(declined.contains("declined"), "{declined}");
}

#[tokio::test]
async fn a_failed_launch_withdraws_the_tenet_its_filing_proposed() {
    let env = env("tenet-gate-compensate.db").await;
    let error = file_recipe(
        &env,
        1,
        "nonexistent-recipe",
        Some(THREAD_RUN),
        Some(proposal()),
    )
    .await
    .unwrap_err();
    assert!(error.contains("nonexistent-recipe"), "{error}");
    let tenet = super::get(&env.db, "p", 1).await.unwrap().unwrap();
    assert_eq!(
        tenet.row.status,
        TenetStatus::Withdrawn,
        "the operator is never asked about a filing that does not exist"
    );
}

#[tokio::test]
async fn an_option_verdict_releases_and_comments_on_the_gated_issue() {
    let env = env("tenet-gate-option.db").await;
    file(&env, 1, Some(THREAD_RUN), Some(proposal()))
        .await
        .unwrap();
    let answered = answer(&env, 1, Verdict::Option(2), Some("after the release")).await;
    assert_eq!(answered.wake, None, "an approval is delivered quietly");

    let report = sweep_at(&env.orch, NOW).await.unwrap();
    let (execution_id, status) = execution(&env, 1).await;
    assert_eq!(report.released, vec![execution_id]);
    assert_eq!(status, "running");

    let comments = texts(&env, "SELECT content FROM comments WHERE issue_id = 'i1'").await;
    assert_eq!(
        comments,
        vec![
            "Tenet #1 approved by the operator (signed in as alice): Wait a release. \
             Rejected: Plan it now. Comment: after the release."
                .to_string()
        ]
    );

    let messages = thread_messages(&env).await;
    assert_eq!(messages.len(), 1, "{messages:?}");
    let message = &messages[0];
    assert!(message.contains("approved option 2"), "{message}");
    assert!(message.contains("Chosen: Wait a release"), "{message}");
    assert!(message.contains("Rejected: Plan it now"), "{message}");
    assert!(message.contains("Comment: after the release"), "{message}");
    assert!(
        message.contains("cairn://p/prj/1/executions/1"),
        "{message}"
    );
    assert_eq!(
        texts(
            &env,
            "SELECT wake FROM attention_pushes WHERE key LIKE 'tenet-verdict:%'"
        )
        .await,
        vec!["passive".to_string()]
    );
}

#[tokio::test]
async fn your_call_releases_and_adopts_option_1() {
    let env = env("tenet-gate-your-call.db").await;
    file(&env, 1, Some(THREAD_RUN), Some(proposal()))
        .await
        .unwrap();
    let answered = answer(&env, 1, Verdict::YourCall, None).await;
    assert_eq!(answered.tenet.text(), Some("Plan it now"));

    sweep_at(&env.orch, NOW).await.unwrap();
    assert_eq!(execution(&env, 1).await.1, "running");
    let comment = &texts(&env, "SELECT content FROM comments WHERE issue_id = 'i1'").await[0];
    assert!(
        comment.contains(": Plan it now. Rejected: Wait a release."),
        "{comment}"
    );
    assert!(thread_messages(&env).await[0].contains("your call"));
}

#[tokio::test]
async fn none_forecloses_and_wakes_the_proposer_not_the_coordinator() {
    let env = env("tenet-gate-none.db").await;
    file(&env, 1, Some(THREAD_RUN), Some(proposal()))
        .await
        .unwrap();
    let answered = answer(&env, 1, Verdict::None, Some("not this quarter")).await;
    assert_eq!(
        answered.wake.as_deref(),
        Some("j-thread"),
        "a decline steers"
    );

    let first = sweep_at(&env.orch, NOW).await.unwrap();
    let second = sweep_at(&env.orch, NOW).await.unwrap();
    assert_eq!(first.foreclosed.len(), 1);
    assert!(second.foreclosed.is_empty());
    assert_eq!(execution(&env, 1).await.1, "held", "held, not cancelled");
    assert_eq!(
        texts(&env, "SELECT foreclosed_reason FROM execution_gates").await,
        vec!["tenet #1 declined: not this quarter".to_string()]
    );
    assert_eq!(
        count(&env, "SELECT COUNT(*) FROM comments").await,
        0,
        "a decline comments on nothing"
    );

    let messages = thread_messages(&env).await;
    assert_eq!(
        messages.len(),
        2,
        "the verdict, then the foreclosure: {messages:?}"
    );
    // Both land within one second, so their row order is not theirs to keep.
    assert!(
        messages
            .iter()
            .any(|message| message.contains("declined it")),
        "{messages:?}"
    );
    let foreclosure = messages
        .iter()
        .find(|message| message.contains("can no longer start"))
        .expect("the foreclosure message");
    assert!(
        foreclosure.contains("tenet #1 declined: not this quarter"),
        "{foreclosure}"
    );
    assert!(foreclosure.contains("ask again"), "{foreclosure}");
    assert!(
        !foreclosure.contains("release:true"),
        "no release around the operator: {foreclosure}"
    );
    assert_eq!(
        count(
            &env,
            "SELECT COUNT(*) FROM messages WHERE recipient_run_id = 'run-coord'"
        )
        .await,
        0,
        "the tenet's judgment is the proposing thread's, not the issue's coordinator's"
    );
}

#[tokio::test]
async fn withdrawing_the_tenet_forecloses_its_filing() {
    let env = env("tenet-gate-withdraw.db").await;
    file(&env, 1, Some(THREAD_RUN), Some(proposal()))
        .await
        .unwrap();
    super::withdraw(
        &env.db,
        env.orch.services.emitter.as_ref(),
        NOW,
        "p",
        1,
        "t-plan",
    )
    .await
    .unwrap();
    let report = sweep_at(&env.orch, NOW).await.unwrap();
    assert_eq!(report.foreclosed.len(), 1);
    assert_eq!(
        texts(&env, "SELECT foreclosed_reason FROM execution_gates").await,
        vec!["tenet #1 was withdrawn by the thread that proposed it".to_string()]
    );
    assert_eq!(thread_messages(&env).await.len(), 1);
}

#[tokio::test]
async fn a_thread_cannot_release_or_patch_off_its_own_tenet_gate() {
    let env = env("tenet-gate-bypass.db").await;
    file(&env, 1, Some(THREAD_RUN), Some(proposal()))
        .await
        .unwrap();

    let release = patch_gates(&env, THREAD_RUN, GatePatch::Release)
        .await
        .unwrap_err();
    assert!(release.contains("does not release its own"), "{release}");
    let dropped = patch_gates(&env, THREAD_RUN, GatePatch::Replace(Vec::new()))
        .await
        .unwrap_err();
    assert!(dropped.contains("not off a tenet gate"), "{dropped}");

    // Moving onto another of its own tenets is how it asks again.
    let next = standing_tenet(&env).await;
    let moved = json!([{"fact": "tenet_verdict", "tenet": format!("cairn://p/PRJ/tenets/{next}"), "outcome": "answered"}]);
    let clauses =
        crate::execution::gates::authoring::parse_after(Some(&moved), "payload.after").unwrap();
    patch_gates(&env, THREAD_RUN, GatePatch::Replace(clauses))
        .await
        .unwrap();
    assert_eq!(
        texts(
            &env,
            "SELECT json_extract(predicate_json, '$.tenet') FROM execution_gates"
        )
        .await,
        vec![format!("cairn://p/prj/tenets/{next}")],
        "the stored tenet URI is canonical"
    );
    assert_eq!(execution(&env, 1).await.1, "held");

    // A coordinator is not the thread the gate binds.
    patch_gates(&env, COORD_RUN, GatePatch::Release)
        .await
        .unwrap();
    assert_eq!(execution(&env, 1).await.1, "running");
}
