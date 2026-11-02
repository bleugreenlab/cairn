//! Holding a filing on a tenet, end to end: the explicit `tenet_verdict` clause
//! in a filing's `after` through both doors, the sweep that resolves it, the
//! verdict's delivery, and the foreclosure that wakes the proposing thread.
//! No recipe requires a tenet, and a filing takes no tenet keys of its own.

use std::sync::Arc;

use cairn_common::identity::PrincipalRef;
use serde_json::{json, Value};

use super::{Answer, Answered, Proposal, Proposer, TenetOrigin, TenetStatus, Verdict};
use crate::db::DbState;
use crate::execution::gates::sweep::sweep_at;
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::{Orchestrator, OrchestratorBuilder};
use crate::services::testing::{RecordingProcessSpawner, TestServicesBuilder};
use crate::storage::{LocalDb, RowExt, SearchIndex};

const NOW: i64 = 1_900_000_000;
/// The one recipe on disk; it stands for any recipe, planning ones included.
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

/// A project `prj` with a planning thread on a live session run, a
/// coordinator on issue 9, and its live children 1..=3.
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
           VALUES('p', 'default', 'Project', 'prj', '{}', 10, 1, 1);
         INSERT INTO threads (id, project_id, name, status, attention, created_at, updated_at)
           VALUES ('t-plan', 'p', 'plan', 'active', 'none', 1, 1);
         INSERT INTO jobs (id, thread_id, project_id, node_name, agent_config_id, status, created_at, updated_at, uri_segment)
           VALUES ('j-thread', 't-plan', 'p', 'thread', 'thread', 'running', 1, 1, 'thread');
         INSERT INTO runs (id, project_id, job_id, status, created_at, updated_at)
           VALUES ('run-thread', 'p', 'j-thread', 'live', 1, 1);
         INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
           VALUES('ip', 'p', 9, 'Parent', 'active', 'active', 'none', 1, 1);
         INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
           VALUES('ep', 'recipe', 'ip', 'p', 'running', 1, 1);
         INSERT INTO jobs(id, execution_id, project_id, issue_id, node_name, status, current_session_id, uri_segment, created_at, updated_at)
           VALUES('coord', 'ep', 'p', 'ip', 'Coordinator', 'running', 'sess', 'coordinator', 1, 1);
         INSERT INTO runs(id, project_id, job_id, issue_id, status, created_at, updated_at)
           VALUES('run-coord', 'p', 'coord', 'ip', 'live', 1, 1);",
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
    Env {
        _temp: temp,
        orch,
        db,
    }
}

/// The `after` list that holds a filing on tenet `number` of `prj`.
fn on_tenet(number: i64) -> Value {
    json!([{"fact": "tenet_verdict", "tenet": format!("cairn://p/prj/tenets/{number}"), "outcome": "answered"}])
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
            format!(
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
        answer_bound_ms: None,
        thread_id: None,
        cwd: "/tmp".to_string(),
        run_id: run.map(str::to_string),
        tool: "write".to_string(),
        payload: Value::Null,
        tool_use_id: None,
        origin: None,
    }
}

/// One append through the agent's own `write` path: its summary and the
/// change data it reports, or the refusal.
async fn append(
    env: &Env,
    run: &str,
    target: &str,
    payload: Value,
) -> Result<(String, Option<Value>), String> {
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
    .map(|change| (change.summary, change.data))
    .map_err(|failure| failure.error)
}

async fn write(env: &Env, run: &str, target: &str, payload: Value) -> Result<String, String> {
    append(env, run, target, payload)
        .await
        .map(|(summary, _)| summary)
}

/// A tenet the planning thread proposed through the tenets collection.
async fn proposed_tenet(env: &Env, rationale: Option<&str>) -> i64 {
    super::propose(
        &env.db,
        env.orch.services.emitter.as_ref(),
        NOW,
        &Proposer {
            project_id: "p".into(),
            thread_id: "t-plan".into(),
            principal: PrincipalRef::Agent {
                node: "cairn://p/prj/plan".into(),
                run_id: Some(THREAD_RUN.into()),
            },
        },
        Proposal {
            statement: "Plan the memory triage now, not after the release.".into(),
            rationale: rationale.map(str::to_string),
            supersedes: None,
            origin: TenetOrigin::Standing,
        },
    )
    .await
    .unwrap()
    .number
}

/// The issue-`number` filing that holds on tenet `tenet`, by `run`.
async fn file_on(env: &Env, run: &str, number: i32, tenet: i64) -> String {
    write(
        env,
        run,
        &format!("cairn://p/prj/{number}/executions"),
        json!({"recipe": RECIPE, "after": on_tenet(tenet)}),
    )
    .await
    .unwrap()
}

/// The blanket rule is gone: a thread files whatever recipe it judges right,
/// with no tenet, through either door, and the work starts.
#[tokio::test]
async fn a_thread_files_any_recipe_without_a_tenet_and_it_starts() {
    let env = env("tenet-gate-none.db").await;
    let appended = write(
        &env,
        THREAD_RUN,
        "cairn://p/prj/1/executions",
        json!({"recipe": RECIPE}),
    )
    .await
    .unwrap();
    assert!(appended.starts_with("Started"), "{appended}");
    assert_eq!(execution(&env, 1).await.1, "running");

    let created = write(
        &env,
        THREAD_RUN,
        "cairn://p/prj/issues",
        json!({"title": "A plan", "execution": {"recipe": RECIPE}}),
    )
    .await
    .unwrap();
    assert!(
        created.contains("\nStarted execution 1 for prj/10 \"A plan\" (recipe '"),
        "{created}"
    );

    assert_eq!(count(&env, "SELECT COUNT(*) FROM execution_gates").await, 0);
    assert_eq!(count(&env, "SELECT COUNT(*) FROM tenets").await, 0);
}

/// `proposal` and `tenet` are no filing's keys. Either, on either door, is
/// refused before anything is written, and the refusal names the two forms
/// that remain: the tenets append, and the `after` clause.
#[tokio::test]
async fn a_filing_carrying_proposal_or_tenet_is_refused_pointing_at_after() {
    let env = env("tenet-gate-keys.db").await;
    let tenet = proposed_tenet(&env, None).await;
    for keys in [
        json!({"proposal": {"statement": "Plan it."}}),
        json!({"tenet": tenet}),
        json!({"proposal": {"statement": "Plan it."}, "tenet": tenet}),
    ] {
        let mut appended = json!({"recipe": RECIPE});
        let mut execution = json!({"recipe": RECIPE});
        for (key, value) in keys.as_object().unwrap() {
            appended[key] = value.clone();
            execution[key] = value.clone();
        }
        for (target, payload, prefix) in [
            ("cairn://p/prj/1/executions", appended, "payload."),
            (
                "cairn://p/prj/issues",
                json!({"title": "Unasked plan", "execution": execution}),
                "payload.execution.",
            ),
        ] {
            let refusal = write(&env, THREAD_RUN, target, payload).await.unwrap_err();
            assert!(refusal.starts_with("Unknown payload key(s)"), "{refusal}");
            for key in keys.as_object().unwrap().keys() {
                assert!(
                    refusal.contains(&format!("`{prefix}{key}`")),
                    "{key}: {refusal}"
                );
            }
            assert!(refusal.contains(&format!("`{prefix}after`")), "{refusal}");
            assert!(refusal.contains("tenet_verdict"), "{refusal}");
            assert!(refusal.contains("/tenets"), "{refusal}");
        }
    }
    assert_eq!(
        count(
            &env,
            "SELECT COUNT(*) FROM executions WHERE issue_id != 'ip'"
        )
        .await,
        0
    );
    assert_eq!(
        count(
            &env,
            "SELECT COUNT(*) FROM issues WHERE title = 'Unasked plan'"
        )
        .await,
        0,
        "a refused filing leaves no orphan issue"
    );
    assert_eq!(count(&env, "SELECT COUNT(*) FROM tenets").await, 1);
}

/// Proposing is the tenets append alone, and it still names the tenet it
/// proposed as change data; a filing proposes nothing and names none.
#[tokio::test]
async fn only_the_tenets_append_reports_a_proposed_tenet() {
    let env = env("tenet-gate-data.db").await;
    let (_, direct) = append(
        &env,
        THREAD_RUN,
        "cairn://p/prj/tenets",
        json!({"statement": "Standing."}),
    )
    .await
    .unwrap();
    assert_eq!(direct, Some(json!({"tenet": TENET_1})));

    let (_, filed) = append(
        &env,
        THREAD_RUN,
        "cairn://p/prj/1/executions",
        json!({"recipe": RECIPE, "after": on_tenet(1)}),
    )
    .await
    .unwrap();
    assert_eq!(filed, None);

    let (_, created) = append(
        &env,
        THREAD_RUN,
        "cairn://p/prj/issues",
        json!({"title": "A plan", "execution": {"recipe": RECIPE, "after": on_tenet(1)}}),
    )
    .await
    .unwrap();
    let created = created.unwrap();
    assert_eq!(
        created,
        json!({"projectKey": "prj", "number": 10, "uri": "cairn://p/prj/10"})
    );
}

#[tokio::test]
async fn an_after_tenet_clause_holds_the_filing_on_the_verdict() {
    let env = env("tenet-gate-held.db").await;
    let tenet = proposed_tenet(&env, None).await;
    let summary = file_on(&env, THREAD_RUN, 1, tenet).await;
    assert!(summary.contains("HELD"), "{summary}");
    assert!(
        summary.contains("after tenet #1 answered (unverified desktop answer) (no verdict yet)"),
        "{summary}"
    );

    let (execution_id, status) = execution(&env, 1).await;
    assert_eq!(status, "held");
    let view = super::get(&env.db, "p", 1).await.unwrap().unwrap();
    assert_eq!(view.row.status, TenetStatus::Proposed);
    assert_eq!(view.gates.len(), 1, "the read projection finds the gate");
    assert!(view.gates[0].held);

    let predicate: Value =
        serde_json::from_str(&texts(&env, "SELECT predicate_json FROM execution_gates").await[0])
            .unwrap();
    assert_eq!(
        predicate,
        json!({"fact": "tenet_verdict", "tenet": TENET_1, "outcome": "answered"}),
        "the clause is stored as the filing wrote it"
    );

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

    // The issue-create door holds the same way.
    let created = write(
        &env,
        THREAD_RUN,
        "cairn://p/prj/issues",
        json!({"title": "Second plan", "execution": {"recipe": RECIPE, "after": on_tenet(tenet)}}),
    )
    .await
    .unwrap();
    assert!(created.contains("HELD"), "{created}");
    assert_eq!(
        count(
            &env,
            "SELECT COUNT(*) FROM executions WHERE status = 'held'"
        )
        .await,
        2,
        "one tenet can hold a whole plan's filings"
    );
}

#[tokio::test]
async fn a_yes_releases_and_comments_on_the_held_issue() {
    let env = env("tenet-gate-yes.db").await;
    let tenet = proposed_tenet(
        &env,
        Some("Rejected: waiting, because the backlog doubles by then."),
    )
    .await;
    file_on(&env, THREAD_RUN, 1, tenet).await;
    let answered = answer(&env, tenet, Verdict::Yes, Some("after the release")).await;
    assert_eq!(
        answered.wake.as_deref(),
        Some("j-thread"),
        "an approval wakes the proposer"
    );

    let report = sweep_at(&env.orch, NOW).await.unwrap();
    let (execution_id, status) = execution(&env, 1).await;
    assert_eq!(report.released, vec![execution_id]);
    assert_eq!(status, "running");

    let comments = texts(&env, "SELECT content FROM comments WHERE issue_id = 'i1'").await;
    assert_eq!(
        comments,
        vec![
            "Tenet #1 approved by the operator (signed in as alice): Plan the memory triage \
             now, not after the release.\nRationale: Rejected: waiting, because the backlog \
             doubles by then.\nComment: after the release."
                .to_string()
        ]
    );

    let messages = thread_messages(&env).await;
    assert_eq!(messages.len(), 1, "{messages:?}");
    let message = &messages[0];
    assert!(message.contains("approved it: yes"), "{message}");
    assert!(message.contains("Comment: after the release"), "{message}");
    assert!(
        message.contains("cairn://p/prj/1/executions/1"),
        "the verdict names what the tenet held: {message}"
    );
    assert_eq!(
        texts(
            &env,
            "SELECT wake FROM attention_pushes WHERE key LIKE 'tenet-verdict:%'"
        )
        .await,
        vec!["wake".to_string()]
    );
}

#[tokio::test]
async fn your_call_releases_like_yes() {
    let env = env("tenet-gate-your-call.db").await;
    let tenet = proposed_tenet(&env, None).await;
    file_on(&env, THREAD_RUN, 1, tenet).await;
    let answered = answer(&env, tenet, Verdict::YourCall, None).await;
    assert_eq!(answered.tenet.status, TenetStatus::Approved);
    assert_eq!(
        answered.wake.as_deref(),
        Some("j-thread"),
        "your call wakes the proposer like any verdict"
    );
    assert_eq!(
        texts(
            &env,
            "SELECT wake FROM attention_pushes WHERE key LIKE 'tenet-verdict:%'"
        )
        .await,
        vec!["wake".to_string()]
    );

    sweep_at(&env.orch, NOW).await.unwrap();
    assert_eq!(execution(&env, 1).await.1, "running");
    assert!(thread_messages(&env).await[0].contains("your call"));
}

#[tokio::test]
async fn an_already_approved_tenet_releases_at_creation_and_comments_once() {
    let env = env("tenet-gate-approved.db").await;
    let tenet = proposed_tenet(&env, None).await;
    answer(&env, tenet, Verdict::Yes, Some("go")).await;
    let summary = file_on(&env, THREAD_RUN, 1, tenet).await;
    assert!(summary.starts_with("Started"), "{summary}");
    assert_eq!(execution(&env, 1).await.1, "running");
    // The planner that starts reads the decision from its own issue, though
    // the tenet was answered before the issue was held on it.
    let comments = texts(&env, "SELECT content FROM comments WHERE issue_id = 'i1'").await;
    assert_eq!(
        comments,
        vec![
            "Tenet #1 approved by the operator (signed in as alice): Plan the memory triage \
             now, not after the release.\nComment: go."
                .to_string()
        ]
    );
    // A second filing gets its own copy, and a re-sweep no other.
    file_on(&env, THREAD_RUN, 2, tenet).await;
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

/// A decline forecloses the filing and wakes the thread that PROPOSED the
/// tenet, whoever filed the work held on it: here a coordinator filed it, and
/// the coordinator hears nothing.
#[tokio::test]
async fn no_forecloses_and_wakes_the_proposer_not_the_filer() {
    let env = env("tenet-gate-no.db").await;
    let tenet = proposed_tenet(&env, None).await;
    file_on(&env, COORD_RUN, 1, tenet).await;
    let answered = answer(&env, tenet, Verdict::No, Some("not this quarter")).await;
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
        "the tenet's judgment is the proposing thread's, not the filer's"
    );
}

#[tokio::test]
async fn withdrawing_the_tenet_forecloses_its_filing() {
    let env = env("tenet-gate-withdraw.db").await;
    let tenet = proposed_tenet(&env, None).await;
    file_on(&env, THREAD_RUN, 1, tenet).await;
    super::withdraw(
        &env.db,
        env.orch.services.emitter.as_ref(),
        NOW,
        "p",
        tenet,
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

/// A filing launches the recipe id its file declares: a path or file-name
/// spelling is refused at both doors, naming the id, before anything is
/// written; a case variant launches the canonical id.
#[tokio::test]
async fn a_filing_names_its_recipe_by_id() {
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

    let cased = write(
        &env,
        THREAD_RUN,
        "cairn://p/prj/1/executions",
        json!({"recipe": "MEMORY-Triage"}),
    )
    .await
    .unwrap();
    assert!(cased.contains("(recipe 'memory-triage')"), "{cased}");
    assert_eq!(
        texts(
            &env,
            "SELECT recipe_id FROM executions WHERE issue_id != 'ip'"
        )
        .await,
        vec!["memory-triage".to_string()]
    );
}
