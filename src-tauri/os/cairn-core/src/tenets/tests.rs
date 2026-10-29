use cairn_common::identity::PrincipalRef;

use super::*;
use crate::mcp::handlers::run_context::AuthenticatedAgent;
use crate::services::testing::CapturingEmitter;
use crate::storage::LocalDb;

const PROJECT: &str = "p";
const DESIGN: &str = "t-design";
const CHANNELS: &str = "t-channels";

async fn fixture(name: &str) -> LocalDb {
    let db = crate::storage::migrated_test_db(name).await;
    db.execute_script(
        "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
           VALUES ('p', 'default', 'P', 'prj', '/tmp/p', 1, 1);
         INSERT INTO threads (id, project_id, name, status, attention, created_at, updated_at)
           VALUES ('t-design', 'p', 'design', 'active', 'none', 1, 1);
         INSERT INTO threads (id, project_id, name, status, attention, created_at, updated_at)
           VALUES ('t-channels', 'p', 'channels', 'active', 'none', 1, 1);
         INSERT INTO jobs (id, thread_id, project_id, node_name, agent_config_id, status, created_at, updated_at, uri_segment)
           VALUES ('j-design', 't-design', 'p', 'thread', 'thread', 'running', 1, 1, 'thread');
         INSERT INTO jobs (id, thread_id, parent_job_id, project_id, node_name, status, created_at, updated_at, uri_segment)
           VALUES ('j-design-task', 't-design', 'j-design', 'p', 'Explore', 'running', 1, 1, 'explore');
         INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
           VALUES ('i', 'p', 7, 'T', 'active', 1, 1);
         INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
           VALUES ('e', 'planbuild', 'i', 'p', 'running', 1, 1);
         INSERT INTO jobs (id, execution_id, issue_id, project_id, node_name, status, created_at, updated_at, uri_segment)
           VALUES ('j-builder', 'e', 'i', 'p', 'Builder', 'running', 1, 1, 'builder');",
    )
    .await
    .unwrap();
    db
}

fn proposer(thread_id: &str, name: &str) -> Proposer {
    Proposer {
        project_id: PROJECT.into(),
        thread_id: thread_id.into(),
        principal: PrincipalRef::Agent {
            node: format!("cairn://p/prj/{name}"),
            run_id: Some("run-1".into()),
        },
    }
}

fn design() -> Proposer {
    proposer(DESIGN, "design")
}

fn proposal(statement: &str) -> Proposal {
    Proposal {
        statement: statement.into(),
        supersedes: None,
        origin: TenetOrigin::Standing,
    }
}

fn successor(statement: &str, supersedes: i64) -> Proposal {
    Proposal {
        supersedes: Some(supersedes),
        ..proposal(statement)
    }
}

fn human() -> PrincipalRef {
    PrincipalRef::Human {
        issuer: "https://auth.example".into(),
        subject: "alice".into(),
        organization: None,
    }
}

fn machine() -> PrincipalRef {
    PrincipalRef::Machine {
        device_id: "dev42".into(),
    }
}

fn by(verdict: Verdict, principal: PrincipalRef) -> Answer {
    Answer {
        verdict,
        comment: None,
        principal,
    }
}

async fn propose_ok(db: &LocalDb, who: &Proposer, proposal: Proposal) -> TenetRow {
    propose(db, &CapturingEmitter::new(), 10, who, proposal)
        .await
        .unwrap()
}

async fn answer_with(db: &LocalDb, number: i64, answer: Answer) -> Result<Answered, String> {
    super::answer(db, &CapturingEmitter::new(), 20, PROJECT, number, answer).await
}

async fn view(db: &LocalDb, number: i64) -> TenetView {
    get(db, PROJECT, number).await.unwrap().unwrap()
}

/// Numbers are allocated inside the insert's own transaction, so proposals
/// racing each other still take distinct, gapless numbers.
#[tokio::test]
async fn concurrent_proposals_take_distinct_gapless_numbers() {
    let db = fixture("tenets-concurrency.db").await;
    let who = design();
    let emitter = CapturingEmitter::new();
    let results = futures_util::future::join_all((0..8).map(|i| {
        let db = &db;
        let who = &who;
        let emitter = &emitter;
        async move {
            propose(db, emitter, 10, who, proposal(&format!("question {i}")))
                .await
                .unwrap()
                .number
        }
    }))
    .await;
    let mut numbers = results;
    numbers.sort_unstable();
    assert_eq!(numbers, (1..=8).collect::<Vec<i64>>());
    assert_eq!(
        emitter.events_named("db-change").len(),
        8,
        "every proposal announces itself"
    );
    assert!(emitter
        .events_named("db-change")
        .iter()
        .all(|event| event["table"] == "tenets"));
}

#[tokio::test]
async fn a_proposal_is_refused_when_its_statement_is_blank_or_too_long() {
    let db = fixture("tenets-validation.db").await;
    let who = design();
    let emitter = CapturingEmitter::new();
    let refuse = |p: Proposal| {
        let db = &db;
        let who = &who;
        let emitter = &emitter;
        async move { propose(db, emitter, 10, who, p).await.unwrap_err() }
    };

    let blank = refuse(proposal("   ")).await;
    assert!(blank.contains("non-empty statement"), "{blank}");
    let over = refuse(proposal(&"x".repeat(MAX_PROPOSAL_CHARS + 1))).await;
    assert!(over.starts_with(OVER_LENGTH_REFUSAL), "{over}");

    assert!(
        emitter.captured().is_empty(),
        "a refused proposal writes nothing"
    );
    assert_eq!(pending_count(&db, PROJECT).await.unwrap(), 0);
}

#[tokio::test]
async fn yes_approves_the_statement_and_records_the_signed_in_person() {
    let db = fixture("tenets-yes.db").await;
    let tenet = propose_ok(&db, &design(), proposal("  X is done via Y, not Z.  ")).await;
    assert_eq!(tenet.status, TenetStatus::Proposed);
    assert_eq!(tenet.statement, "X is done via Y, not Z.");
    assert_eq!(pending_count(&db, PROJECT).await.unwrap(), 1);

    let answered = answer_with(
        &db,
        tenet.number,
        Answer {
            comment: Some("  Y is cheaper  ".into()),
            ..by(Verdict::Yes, human())
        },
    )
    .await
    .unwrap();
    assert_eq!(answered.superseded, None);
    let row = &answered.tenet;
    assert_eq!(row.status, TenetStatus::Approved);
    assert_eq!(row.verdict, Some(Verdict::Yes));
    assert_eq!(row.answer_source, Some(AnswerSource::Principal));
    assert_eq!(row.answered_by, Some(human()));
    assert_eq!(row.comment.as_deref(), Some("Y is cheaper"));

    let view = view(&db, tenet.number).await;
    assert_eq!(view.row.statement, "X is done via Y, not Z.");
    assert_eq!(view.answerer_label.as_deref(), Some("signed in as alice"));
    assert_eq!(view.proposing_thread, "prj/design");
    assert_eq!(view.uri, "cairn://p/prj/tenets/1");
    assert_eq!(
        view.policy_label,
        "Releases on: answered (unverified desktop answer)"
    );
    assert_eq!(pending_count(&db, PROJECT).await.unwrap(), 0);

    let json = serde_json::to_value(&view).unwrap();
    assert_eq!(json["statement"], "X is done via Y, not Z.");
    assert_eq!(json["verdict"], "yes");
    for retired in ["question", "options", "selectedOption", "text", "rejected"] {
        assert!(json.get(retired).is_none(), "{retired} retired: {json}");
    }
}

#[tokio::test]
async fn your_call_approves_and_a_desktop_answer_records_its_device() {
    let db = fixture("tenets-your-call.db").await;
    let tenet = propose_ok(&db, &design(), proposal("Via Y.")).await;
    let row = answer_with(&db, tenet.number, by(Verdict::YourCall, machine()))
        .await
        .unwrap()
        .tenet;
    assert_eq!(row.status, TenetStatus::Approved);
    assert_eq!(row.verdict, Some(Verdict::YourCall));
    assert_eq!(row.answer_source, Some(AnswerSource::Device));
    let view = view(&db, tenet.number).await;
    assert_eq!(
        view.answerer_label.as_deref(),
        Some("anonymous device dev42")
    );
    assert_eq!(view.thread_your_calls, 1);
}

#[tokio::test]
async fn no_declines() {
    let db = fixture("tenets-no.db").await;
    let tenet = propose_ok(&db, &design(), proposal("Via Y.")).await;
    let row = answer_with(&db, tenet.number, by(Verdict::No, human()))
        .await
        .unwrap()
        .tenet;
    assert_eq!(row.status, TenetStatus::Declined);
    assert_eq!(row.verdict, Some(Verdict::No));
    assert_eq!(row.statement, "Via Y.");
}

#[tokio::test]
async fn agents_cannot_answer_and_a_verdict_is_given_once() {
    let db = fixture("tenets-answer-refusals.db").await;
    let tenet = propose_ok(&db, &design(), proposal("How?")).await;

    let agent = answer_with(&db, tenet.number, by(Verdict::Yes, design().principal))
        .await
        .unwrap_err();
    assert_eq!(agent, AGENT_VERDICT_REFUSAL);

    assert_eq!(
        view(&db, tenet.number).await.row.status,
        TenetStatus::Proposed
    );

    answer_with(&db, tenet.number, by(Verdict::Yes, human()))
        .await
        .unwrap();
    let again = answer_with(&db, tenet.number, by(Verdict::No, human()))
        .await
        .unwrap_err();
    assert!(again.contains("already approved"), "{again}");

    let missing = answer_with(&db, 99, by(Verdict::No, human()))
        .await
        .unwrap_err();
    assert!(missing.contains("no tenet #99"), "{missing}");
}

#[tokio::test]
async fn only_the_proposing_thread_withdraws_and_only_before_a_verdict() {
    let db = fixture("tenets-withdraw.db").await;
    let emitter = CapturingEmitter::new();
    let first = propose_ok(&db, &design(), proposal("First?")).await;
    let second = propose_ok(&db, &design(), proposal("Second?")).await;

    let foreign = withdraw(&db, &emitter, 30, PROJECT, first.number, CHANNELS)
        .await
        .unwrap_err();
    assert!(
        foreign.contains("only the thread that proposed"),
        "{foreign}"
    );

    let withdrawn = withdraw(&db, &emitter, 30, PROJECT, first.number, DESIGN)
        .await
        .unwrap();
    assert_eq!(withdrawn.status, TenetStatus::Withdrawn);
    assert_eq!(withdrawn.verdict, None);
    let twice = withdraw(&db, &emitter, 30, PROJECT, first.number, DESIGN)
        .await
        .unwrap_err();
    assert!(twice.contains("already withdrawn"), "{twice}");
    let answer_withdrawn = answer_with(&db, first.number, by(Verdict::Yes, human()))
        .await
        .unwrap_err();
    assert!(answer_withdrawn.contains("withdrawn"), "{answer_withdrawn}");

    answer_with(&db, second.number, by(Verdict::Yes, human()))
        .await
        .unwrap();
    let after_verdict = withdraw(&db, &emitter, 30, PROJECT, second.number, DESIGN)
        .await
        .unwrap_err();
    assert_eq!(after_verdict, successor_refusal(second.number));
    assert!(after_verdict.contains("supersedes: 2"), "{after_verdict}");
}

/// A successor replaces its predecessor only when the successor is approved.
#[tokio::test]
async fn supersession_applies_only_on_approval() {
    let db = fixture("tenets-supersede.db").await;
    let original = propose_ok(&db, &design(), proposal("How?")).await;
    answer_with(&db, original.number, by(Verdict::Yes, human()))
        .await
        .unwrap();

    let declined = propose_ok(&db, &design(), successor("Change it?", original.number)).await;
    assert_eq!(
        view(&db, original.number).await.row.status,
        TenetStatus::Approved
    );
    answer_with(&db, declined.number, by(Verdict::No, human()))
        .await
        .unwrap();
    let still = view(&db, original.number).await;
    assert_eq!(still.row.status, TenetStatus::Approved);
    assert_eq!(
        still.superseded_by, None,
        "a declined successor replaces nothing"
    );
    let declined_read =
        crate::resources::tenets::read_entity(&db, "prj", &declined.number.to_string(), &[]).await;
    assert!(
        declined_read.contains("Proposed to supersede [#1]")
            && declined_read.contains("not in effect, because this tenet was declined")
            && !declined_read.contains("- Supersedes:"),
        "{declined_read}"
    );
    let withdrawn = propose_ok(
        &db,
        &design(),
        successor("Withdrawn change?", original.number),
    )
    .await;
    let pending_read =
        crate::resources::tenets::read_entity(&db, "prj", &withdrawn.number.to_string(), &[]).await;
    assert!(
        pending_read.contains("Would supersede [#1]"),
        "{pending_read}"
    );
    withdraw(
        &db,
        &CapturingEmitter::new(),
        25,
        PROJECT,
        withdrawn.number,
        DESIGN,
    )
    .await
    .unwrap();
    let withdrawn_read =
        crate::resources::tenets::read_entity(&db, "prj", &withdrawn.number.to_string(), &[]).await;
    assert!(
        withdrawn_read.contains("not in effect, because this tenet was withdrawn"),
        "{withdrawn_read}"
    );
    assert_eq!(
        view(&db, original.number).await.row.status,
        TenetStatus::Approved
    );

    let approved = propose_ok(&db, &design(), successor("Change it now?", original.number)).await;
    let answered = answer_with(&db, approved.number, by(Verdict::Yes, human()))
        .await
        .unwrap();
    assert_eq!(answered.superseded, Some(original.number));
    let replaced = view(&db, original.number).await;
    assert_eq!(replaced.row.status, TenetStatus::Superseded);
    assert_eq!(
        replaced.row.verdict,
        Some(Verdict::Yes),
        "a superseded tenet keeps its verdict"
    );
    assert_eq!(replaced.superseded_by, Some(approved.number));
    assert_eq!(
        view(&db, approved.number).await.row.supersedes,
        Some(original.number)
    );
    let approved_read =
        crate::resources::tenets::read_entity(&db, "prj", &approved.number.to_string(), &[]).await;
    assert!(
        approved_read.contains("- Supersedes: [#1]"),
        "{approved_read}"
    );
    let replaced_read = crate::resources::tenets::read_entity(&db, "prj", "1", &[]).await;
    assert!(
        replaced_read.contains(&format!("- Superseded by: [#{}]", approved.number)),
        "{replaced_read}"
    );
}

#[tokio::test]
async fn supersedes_must_name_an_approved_tenet_and_a_tenet_is_superseded_once() {
    let db = fixture("tenets-double-supersede.db").await;
    let who = design();
    let emitter = CapturingEmitter::new();
    let pending = propose_ok(&db, &who, proposal("Pending?")).await;
    let refused = propose(&db, &emitter, 10, &who, successor("x", pending.number))
        .await
        .unwrap_err();
    assert!(
        refused.contains("only an approved tenet can be superseded"),
        "{refused}"
    );
    let missing = propose(&db, &emitter, 10, &who, successor("x", 404))
        .await
        .unwrap_err();
    assert!(missing.contains("names no tenet"), "{missing}");

    let base = propose_ok(&db, &who, proposal("Base?")).await;
    answer_with(&db, base.number, by(Verdict::Yes, human()))
        .await
        .unwrap();
    // Two successors race for the same predecessor.
    let left = propose_ok(&db, &who, successor("Left?", base.number)).await;
    let right = propose_ok(&db, &who, successor("Right?", base.number)).await;
    answer_with(&db, left.number, by(Verdict::Yes, human()))
        .await
        .unwrap();
    let late = answer_with(&db, right.number, by(Verdict::Yes, human()))
        .await
        .unwrap_err();
    assert!(
        late.contains("already superseded")
            && late.contains(&format!("supersedes #{}", base.number)),
        "{late}"
    );
    assert_eq!(
        view(&db, right.number).await.row.status,
        TenetStatus::Proposed
    );
    assert_eq!(
        view(&db, base.number).await.superseded_by,
        Some(left.number)
    );
    // The loser can still be declined; that supersedes nothing.
    answer_with(&db, right.number, by(Verdict::No, human()))
        .await
        .unwrap();

    let after = propose(&db, &emitter, 10, &who, successor("Again?", base.number))
        .await
        .unwrap_err();
    assert!(after.contains("already superseded"), "{after}");
}

#[tokio::test]
async fn a_proposer_needs_thread_lineage_which_a_delegated_task_inherits() {
    let db = fixture("tenets-lineage.db").await;
    let agent = |job: &str, home: &str| AuthenticatedAgent {
        home: home.into(),
        run_id: "run-1".into(),
        job_id: job.into(),
    };
    let principal = design().principal;

    let thread = Proposer::for_agent(
        &db,
        &agent("j-design", "cairn://p/prj/design"),
        principal.clone(),
    )
    .await
    .unwrap();
    assert_eq!(
        (thread.thread_id.as_str(), thread.project_id.as_str()),
        (DESIGN, PROJECT)
    );

    let task = Proposer::for_agent(
        &db,
        &agent("j-design-task", "cairn://p/prj/design/task/explore"),
        principal.clone(),
    )
    .await
    .unwrap();
    assert_eq!(task.thread_id, DESIGN);

    let builder = Proposer::for_agent(
        &db,
        &agent("j-builder", "cairn://p/prj/7/1/builder"),
        principal,
    )
    .await
    .unwrap_err();
    assert_eq!(builder, super::write::NON_THREAD_REFUSAL);
}

#[tokio::test]
async fn a_migrated_slug_resolves_to_its_canonical_number() {
    let db = fixture("tenets-slug.db").await;
    propose_ok(&db, &design(), proposal("Unrelated")).await;
    let new = NewTenet {
        project_id: PROJECT.into(),
        proposing_thread_id: DESIGN.into(),
        slug: Some("rulings-become-tenets".into()),
        statement: "Rulings become tenets".into(),
        status: TenetStatus::Approved,
        verdict: Some(Verdict::Yes),
        comment: None,
        answered_by: None,
        answer_source: Some(AnswerSource::Migrated),
        answered_at: Some(5),
        supersedes: None,
        origin: TenetOrigin::MigratedRuling,
        legacy: Some(serde_json::json!({"rationale": "why", "provenance": "cairn://p/prj/1"})),
        proposer: design().principal,
        now: 5,
    };
    let (_, number) = db
        .write(|conn| {
            let new = new.clone();
            Box::pin(async move { insert_conn(conn, &new).await })
        })
        .await
        .unwrap();
    assert_eq!(number, 2);
    assert_eq!(
        resolve_number(&db, PROJECT, "rulings-become-tenets")
            .await
            .unwrap(),
        Some(2)
    );
    assert_eq!(resolve_number(&db, PROJECT, "2").await.unwrap(), Some(2));
    assert_eq!(
        resolve_number(&db, PROJECT, "no-such-slug").await.unwrap(),
        None
    );
    let view = view(&db, 2).await;
    assert_eq!(view.answerer_label.as_deref(), Some("migrated ruling"));
    assert_eq!(view.row.legacy.unwrap()["rationale"], "why");

    let by_slug = list(
        &db,
        PROJECT,
        &TenetFilter {
            slug: Some("rulings-become-tenets".into()),
            ..TenetFilter::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(by_slug.len(), 1);
}

#[tokio::test]
async fn views_name_the_executions_a_tenet_gates_and_filter_by_thread() {
    let db = fixture("tenets-gates.db").await;
    let tenet = propose_ok(&db, &design(), proposal("Gate?")).await;
    propose_ok(&db, &proposer(CHANNELS, "channels"), proposal("Other?")).await;
    db.execute(
        "INSERT INTO execution_gates (id, execution_id, seq, predicate_json, created_at)
         VALUES ('g', 'e', 0, ?1, 1)",
        (serde_json::json!({
            "fact": TENET_GATE_FACT,
            "tenet": "cairn://p/prj/tenets/1",
            "outcome": "answered",
        })
        .to_string(),),
    )
    .await
    .unwrap();
    let gates = view(&db, tenet.number).await.gates;
    assert_eq!(gates.len(), 1);
    assert_eq!(gates[0].issue.as_deref(), Some("cairn://p/prj/7"));
    assert!(gates[0].held && !gates[0].foreclosed && !gates[0].satisfied);
    assert!(view(&db, 2).await.gates.is_empty());

    // The verdict satisfies this gate while another gate keeps the execution
    // waiting: the tenet no longer holds it, and it has not started.
    db.execute(
        "UPDATE execution_gates SET satisfied_at = 2 WHERE id = 'g'",
        (),
    )
    .await
    .unwrap();
    let gate = view(&db, tenet.number).await.gates.remove(0);
    assert!(gate.satisfied && !gate.held && !gate.released, "{gate:?}");
    let read = crate::resources::tenets::read_entity(&db, "prj", "1", &[]).await;
    assert!(
        read.contains(
            "this verdict satisfied its gate; the execution still waits on its other gates"
        ),
        "{read}"
    );
    db.execute("UPDATE executions SET released_at = 3 WHERE id = 'e'", ())
        .await
        .unwrap();
    let gate = view(&db, tenet.number).await.gates.remove(0);
    assert!(gate.released && !gate.held, "{gate:?}");

    let mine = for_thread(&db, DESIGN).await.unwrap();
    assert_eq!(
        mine.iter().map(|v| v.row.number).collect::<Vec<_>>(),
        vec![1]
    );
    let pending = list(
        &db,
        PROJECT,
        &TenetFilter {
            statuses: vec![TenetStatus::Proposed],
            oldest_first: true,
            ..TenetFilter::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(
        pending.iter().map(|v| v.row.number).collect::<Vec<_>>(),
        vec![1, 2]
    );
    let searched = list(
        &db,
        PROJECT,
        &TenetFilter {
            search: Some("OTHER".into()),
            ..TenetFilter::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(searched.len(), 1);
}

/// The tenet log is project history: a thread whose tenets carry verdicts is
/// closed, never deleted, while unanswered proposals go with a deleted thread.
#[tokio::test]
async fn deleting_a_thread_keeps_decided_tenets_and_drops_unanswered_ones() {
    let db = fixture("tenets-thread-delete.db").await;
    let decided = propose_ok(&db, &design(), proposal("Decided?")).await;
    answer_with(&db, decided.number, by(Verdict::Yes, human()))
        .await
        .unwrap();
    let emitter = CapturingEmitter::new();
    let refusal = crate::threads::crud::delete(&db, &emitter, DESIGN)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        refusal.contains("#1") && refusal.contains("close the thread instead"),
        "{refusal}"
    );
    assert!(view(&db, decided.number)
        .await
        .proposing_thread_name
        .is_some());

    let channels = proposer(CHANNELS, "channels");
    let pending = propose_ok(&db, &channels, proposal("Pending?")).await;
    let withdrawn = propose_ok(&db, &channels, proposal("Withdrawn?")).await;
    withdraw(
        &db,
        &CapturingEmitter::new(),
        30,
        PROJECT,
        withdrawn.number,
        CHANNELS,
    )
    .await
    .unwrap();
    assert!(
        emitter.captured().is_empty(),
        "a refused delete announces nothing"
    );
    crate::threads::crud::delete(&db, &emitter, CHANNELS)
        .await
        .unwrap();
    assert!(get(&db, PROJECT, pending.number).await.unwrap().is_none());
    assert!(get(&db, PROJECT, withdrawn.number).await.unwrap().is_none());
    assert_eq!(
        emitter.events_named("db-change"),
        vec![serde_json::json!({"table": "tenets", "action": "delete"})],
        "removing a thread's proposals announces the tenets change once"
    );
}

#[test]
fn a_verdict_is_yes_no_or_your_call() {
    use serde_json::json;
    assert_eq!(Verdict::from_value(&json!("yes")).unwrap(), Verdict::Yes);
    assert_eq!(Verdict::from_value(&json!(" no ")).unwrap(), Verdict::No);
    assert_eq!(
        Verdict::from_value(&json!("your_call")).unwrap(),
        Verdict::YourCall
    );
    for bad in [
        json!(1),
        json!("1"),
        json!("none"),
        json!("maybe"),
        json!(true),
    ] {
        let refusal = Verdict::from_value(&bad).unwrap_err();
        assert!(
            refusal.contains("\"yes\", \"no\", or \"your_call\""),
            "{bad}: {refusal}"
        );
    }
    assert_eq!(
        serde_json::to_value(Verdict::YourCall).unwrap(),
        json!("your_call")
    );
}

#[tokio::test]
async fn the_release_policy_refuses_ratified_and_is_operator_only() {
    let db = fixture("tenets-policy.db").await;
    let emitter = CapturingEmitter::new();
    let policy = load_policy(&db, PROJECT).await.unwrap();
    assert_eq!(policy.release, TenetRelease::Answered);
    assert!(policy.gates_recipe("planbuild") && policy.gates_recipe("plancoord"));
    assert!(!policy.gates_recipe("build"));
    assert_eq!(
        policy.label(),
        "Releases on: answered (unverified desktop answer)"
    );

    let ratified = set_release(&db, &emitter, PROJECT, "ratified")
        .await
        .unwrap_err();
    assert!(ratified.contains(RATIFIED_REFUSAL), "{ratified}");
    assert_eq!(
        set_release(&db, &emitter, PROJECT, "answered")
            .await
            .unwrap(),
        TenetRelease::Answered
    );
    assert_eq!(
        set_gated_recipes(
            &db,
            &emitter,
            PROJECT,
            &["planbuild".into(), "PlanBuild".into(), "coordinator".into()]
        )
        .await
        .unwrap(),
        vec!["planbuild", "coordinator"]
    );
    assert!(!load_policy(&db, PROJECT)
        .await
        .unwrap()
        .gates_recipe("PlanCoord"));

    for key in [
        "tenetRelease",
        "tenet_release",
        "tenetGatedRecipes",
        "tenet_gated_recipes",
    ] {
        let payload = serde_json::json!({ key: "answered" });
        assert_eq!(
            operator_only_policy_refusal(Some(&payload)),
            Some(OPERATOR_ONLY_POLICY_REFUSAL)
        );
    }
    assert_eq!(
        operator_only_policy_refusal(Some(&serde_json::json!({"name": "x"}))),
        None
    );
}
