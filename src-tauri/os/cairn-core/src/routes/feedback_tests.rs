//! The ambient label set end to end against seeded facts and firings: the
//! sweep's outcome rows and cursor, the feedback write's labeller, row choice
//! and re-route delivery, and the 48-hour engagement pass. The firing journal
//! is seeded in exactly the shape the dispatcher writes (one decision entry per
//! firing, `asked` plus `answers`); `routes::worker::ambient_tests` drives the
//! real route into the same sweep.

use super::*;
const AMBIENT_ROUTE: &str = "ambient-channel";
use crate::db::DbState;
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::OrchestratorBuilder;
use crate::resources::mutations::dispatch_resource_change;
use crate::services::testing::TestServicesBuilder;
use crate::storage::{LogDedupe, NewFact, NewRouteFiring, SearchIndex};
use cairn_common::protocol::DispatchOrigin;
use serde_json::json;
use std::sync::Arc;

const T0: i64 = 1_790_000_000;
const CONVERSATION: &str = "discord:1/2";
const DEVOPS: &str = "cairn://p/cairn/devops";
const DOCS: &str = "cairn://p/cairn/docs";
const HORIZON: i64 = ENGAGEMENT_HORIZON_SECONDS;

#[test]
fn an_event_is_cited_by_its_uri_or_by_name_never_by_a_longer_id() {
    assert!(cites_event("see cairn://p/cairn/events/12 for context", 12));
    assert!(cites_event("[#12](cairn://routes/events/12)", 12));
    assert!(cites_event("Picked up Event #12.", 12));
    assert!(cites_event("event 12 is mine", 12));
    assert!(!cites_event("cairn://p/cairn/events/120", 12));
    assert!(!cites_event("prevent 12 regressions", 12));
    assert!(!cites_event("12", 12));
}

#[test]
fn a_citation_carries_its_revision() {
    assert_eq!(
        citation_revision("devops (0.9, native, rev abc123)").as_deref(),
        Some("abc123")
    );
    assert_eq!(citation_revision("devops (native)"), None);
}

async fn seed(local: &LocalDb) {
    local
        .execute_script(
            "INSERT INTO workspaces (id,name,created_at,updated_at) VALUES ('w','W',1,1);
             INSERT INTO projects (id,workspace_id,name,key,repo_path,created_at,updated_at)
               VALUES ('p','w','Cairn','cairn','/tmp/cairn',1,1);
             INSERT INTO threads (id, project_id, name, jurisdiction, status, created_at, updated_at) VALUES
               ('t-devops', 'p', 'devops', 'How the project ships.', 'active', 1, 1),
               ('t-docs', 'p', 'docs', 'The documentation.', 'active', 1, 1),
               ('t-old', 'p', 'old', 'Retired.', 'closed', 1, 1),
               ('t-scratch', 'p', 'scratch', NULL, 'active', 1, 1);
             INSERT INTO issues (id, project_id, number, title, created_at, updated_at)
               VALUES ('i-7', 'p', 7, 'An issue', 1, 1);
             INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
               VALUES ('e-7', 'recipe-default', 'i-7', 'p', 'running', 1, 1);
             INSERT INTO jobs (id, execution_id, issue_id, project_id, node_name, uri_segment, status, created_at, updated_at)
               VALUES ('j-builder', 'e-7', 'i-7', 'p', 'builder', 'builder', 'running', 1, 1);
             INSERT INTO runs (id, issue_id, project_id, job_id, status, created_at, updated_at)
               VALUES ('r-builder', 'i-7', 'p', 'j-builder', 'live', 1, 1);",
        )
        .await
        .unwrap();
    for (thread, run) in [("t-devops", "r-devops"), ("t-docs", "r-docs")] {
        let session = crate::threads::ensure_thread_session(local, thread)
            .await
            .unwrap();
        local
            .execute_script(format!(
                "INSERT INTO runs (id, project_id, job_id, status, created_at, updated_at)
                 VALUES ('{run}', 'p', '{session}', 'live', 1, 1);"
            ))
            .await
            .unwrap();
    }
}

async fn orchestrator() -> Orchestrator {
    let local = LocalDb::open_migrated_for_test(tempfile::tempdir().unwrap().keep().join("t.db"))
        .await
        .unwrap();
    seed(&local).await;
    let search =
        Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
    let orch = OrchestratorBuilder::new(
        Arc::new(DbState::new(Arc::new(local), search)),
        Arc::new(TestServicesBuilder::new().build()),
        tempfile::tempdir().unwrap().keep(),
    )
    .build();
    std::fs::create_dir_all(orch.config_dir.join("routes")).unwrap();
    std::fs::write(
        orch.config_dir.join("routes/ambient-channel.yaml"),
        std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../packs/core/routes/ambient-channel.yaml"),
        )
        .unwrap(),
    )
    .unwrap();
    orch
}

/// An ambient message's inbound record and its fact, settled in `state`,
/// minted at `at` (epoch seconds).
async fn fact(orch: &Orchestrator, n: i64, at: i64, state: &str, binding_kind: &str) -> i64 {
    let local = &orch.db.local;
    let inbound = format!("in-{n}");
    crate::channels::ledger::insert_inbound(
        local,
        &crate::channels::ledger::InboundRecord {
            id: inbound.clone(),
            channel: "discord".into(),
            provider_guid: None,
            sender: "ada".into(),
            text: format!("message {n}"),
            received_at: at * 1000,
            rejection_reason: None,
            acknowledged_at: None,
            conversation: Some(CONVERSATION.into()),
            target_uri: Some("cairn://p/cairn".into()),
            binding_kind: Some(binding_kind.into()),
            addressed_by: None,
            message_guid: None,
        },
    )
    .await
    .unwrap();
    let fields = json!({
        "project": "cairn", "provider": "discord", "conversation": CONVERSATION,
        "sender": "ada", "bindingKind": binding_kind, "addressed": false,
        "target": "cairn://p/cairn", "receivedAt": at * 1000,
    });
    let id = crate::storage::insert_fact(
        local,
        NewFact {
            project_id: Some("p".into()),
            source: "channel_message".into(),
            identity: format!("channel-message:{inbound}"),
            record_ref: CairnResource::ChannelsConversation {
                provider: "discord".into(),
                path: vec!["cairn".into()],
            }
            .to_uri(),
            record_id: Some(inbound),
            fields_json: fields.to_string(),
            summary: None,
            presence: "away".into(),
            actor_job_id: None,
            origin_principal_json: None,
            origin_appearance_json: None,
            route_provenance: None,
            minted_at: at * 1000,
        },
        LogDedupe::Unique,
    )
    .await
    .unwrap()
    .unwrap();
    crate::storage::finish_fact(local, id, state, None)
        .await
        .unwrap();
    id
}

/// The journal entry the dispatcher writes for the `pick` decision.
fn journal(outcome: &str, picks: &[(&str, f64)]) -> String {
    let citation = |(name, p): &(&str, f64)| format!("{name} ({p}, native, rev rev-{name})");
    let admitted = outcome == "admitted";
    let mut answer = json!({
        "name": "thread",
        "outcome": outcome,
        "pick": picks.first().map(|(name, _)| *name),
        "picks": if admitted { picks.iter().map(|(name, _)| *name).collect::<Vec<_>>() } else { vec![] },
        "citations": if admitted { picks.iter().map(citation).collect::<Vec<_>>() } else { vec![] },
        "measuredBy": "native",
        "probabilities": picks.iter().map(|(name, p)| json!([name, p])).collect::<Vec<_>>(),
    });
    if admitted {
        answer["addresses"] = json!(picks
            .iter()
            .map(|(name, _)| format!("cairn://p/cairn/{name}"))
            .collect::<Vec<_>>());
    }
    if let Some(closest) = picks.first().filter(|_| !admitted) {
        answer["citation"] = json!(citation(closest));
    }
    json!([{
        "decision": "pick",
        "status": "ok",
        "asked": {"questions": [{"name": "thread", "instructions": "Which thread owns this?", "type": "choice", "topK": 2, "options": [
            {"value": "devops", "address": DEVOPS, "revision": "rev-devops"},
            {"value": "docs", "address": DOCS, "revision": "rev-docs"}
        ]}]},
        "answers": {"thread": answer},
        "error": null
    }])
    .to_string()
}

async fn firing(orch: &Orchestrator, fact_id: i64, journal: &str, sink_kind: &str, sink: &str) {
    firing_for_route(
        orch,
        AMBIENT_ROUTE,
        "channel_message",
        fact_id,
        journal,
        sink_kind,
        sink,
    )
    .await;
}

async fn firing_for_route(
    orch: &Orchestrator,
    route: &str,
    source: &str,
    fact_id: i64,
    journal: &str,
    sink_kind: &str,
    sink: &str,
) {
    crate::storage::insert_route_firing(
        &orch.db.local,
        NewRouteFiring {
            route_id: route.into(),
            scope_key: "workspace".into(),
            project_id: Some("p".into()),
            trigger_source: source.into(),
            fact_identity: format!("channel-message:in-{fact_id}"),
            fact_summary: None,
            status: "fired".into(),
            drop_reason: None,
            transforms_json: Some(journal.to_string()),
            sink_kind: sink_kind.into(),
            sink_ref: Some(sink.into()),
            payload_text: None,
            error: None,
            created_at: T0 * 1000,
            fact_log_id: Some(fact_id),
        },
    )
    .await
    .unwrap();
}

/// A routed fact as the route leaves it: one message firing per delivered
/// thread, then the acknowledgement's channel firing.
async fn routed(orch: &Orchestrator, n: i64, at: i64, outcome: &str, picks: &[(&str, f64)]) -> i64 {
    let id = fact(orch, n, at, route_state::DONE, "ambient").await;
    let journal = journal(outcome, picks);
    if outcome == "admitted" {
        for (name, _) in picks {
            firing(
                orch,
                id,
                &journal,
                "message",
                &format!("cairn://p/cairn/{name}"),
            )
            .await;
        }
    }
    firing(orch, id, &journal, "channel", CONVERSATION).await;
    id
}

async fn rows(orch: &Orchestrator) -> Vec<RouteFeedbackRow> {
    let mut rows = feedback::list(&orch.db.local, &Default::default(), 500)
        .await
        .unwrap();
    rows.sort_by(|a, b| (a.event_id, &a.recipient_thread).cmp(&(b.event_id, &b.recipient_thread)));
    rows
}

async fn row(orch: &Orchestrator, event: i64, recipient: Option<&str>) -> RouteFeedbackRow {
    rows(orch)
        .await
        .into_iter()
        .find(|row| row.event_id == event && row.recipient_thread.as_deref() == recipient)
        .unwrap_or_else(|| panic!("a row for event {event} and {recipient:?}"))
}

fn request(run_id: Option<&str>, origin: Option<DispatchOrigin>) -> McpCallbackRequest {
    McpCallbackRequest {
        answer_bound_ms: None,
        thread_id: None,
        cwd: "/tmp".to_string(),
        run_id: run_id.map(str::to_string),
        tool: "change".to_string(),
        payload: json!({}),
        tool_use_id: None,
        origin,
    }
}

/// A label written by `run_id`'s agent.
async fn write(
    orch: &Orchestrator,
    run_id: Option<&str>,
    payload: Value,
) -> Result<String, String> {
    write_as(orch, run_id, None, payload).await
}

async fn write_as(
    orch: &Orchestrator,
    run_id: Option<&str>,
    origin: Option<DispatchOrigin>,
    payload: Value,
) -> Result<String, String> {
    let item = ChangeItem {
        target: "cairn://p/cairn/events/feedback".into(),
        mode: ChangeMode::Append,
        payload: Some(payload),
    };
    dispatch_resource_change(orch, &request(run_id, origin), 0, &item, false)
        .await
        .map(|applied| applied.summary)
        .map_err(|failure| failure.error)
}

async fn refusal(orch: &Orchestrator, run_id: Option<&str>, payload: Value) -> String {
    refusal_as(orch, run_id, None, payload).await
}

async fn refusal_as(
    orch: &Orchestrator,
    run_id: Option<&str>,
    origin: Option<DispatchOrigin>,
    payload: Value,
) -> String {
    let before = rows(orch).await;
    let error = write_as(orch, run_id, origin, payload).await.unwrap_err();
    assert_eq!(
        rows(orch).await,
        before,
        "a refused label changes nothing: {error}"
    );
    error
}

fn event(id: i64) -> String {
    format!("cairn://p/cairn/events/{id}")
}

/// `(sender_run_id, content)` of every message in `thread_id`.
async fn thread_messages(orch: &Orchestrator, thread_id: &str) -> Vec<(Option<String>, String)> {
    orch.db
        .local
        .query_all(
            "SELECT sender_run_id, content FROM messages WHERE channel_type = 'thread' AND channel_id = ?1 ORDER BY created_at",
            (thread_id.to_string(),),
            |row| Ok((row.opt_text(0)?, row.text(1)?)),
        )
        .await
        .unwrap()
}

async fn outbound_count(orch: &Orchestrator) -> i64 {
    orch.db
        .local
        .query_opt_i64("SELECT COUNT(*) FROM channel_outbound", ())
        .await
        .unwrap()
        .unwrap_or_default()
}

#[tokio::test]
async fn one_pass_records_each_delivery_and_each_refused_event_once() {
    let orch = orchestrator().await;
    let one = routed(&orch, 1, T0, "admitted", &[("devops", 0.9)]).await;
    let two = routed(
        &orch,
        2,
        T0,
        "admitted",
        &[("devops", 0.48), ("docs", 0.47)],
    )
    .await;
    let mut refused_side = Vec::new();
    for (n, outcome) in [
        (3, "below_threshold"),
        (4, "refused"),
        (5, "unanswerable"),
        (6, "failed"),
    ] {
        let picks: &[(&str, f64)] = if outcome == "unanswerable" {
            &[]
        } else {
            &[("devops", 0.3)]
        };
        refused_side.push((routed(&orch, n, T0, outcome, picks).await, outcome));
    }
    // A structural binding's message is not the ambient route's to label.
    fact(&orch, 7, T0, route_state::DONE, "structural").await;

    let report = sweep_once(&orch, T0 + 60).await.unwrap();
    assert_eq!(
        (report.inserted, report.unrecorded, report.held_at),
        (7, 0, None)
    );

    let all = rows(&orch).await;
    let keys: Vec<_> = all
        .iter()
        .map(|row| {
            (
                row.event_id,
                row.recipient_thread.clone(),
                row.outcome.clone(),
            )
        })
        .collect();
    let mut expected = vec![
        (one, Some(DEVOPS.to_string()), "admitted".to_string()),
        (two, Some(DEVOPS.to_string()), "admitted".to_string()),
        (two, Some(DOCS.to_string()), "admitted".to_string()),
    ];
    expected.extend(
        refused_side
            .iter()
            .map(|(id, outcome)| (*id, None, outcome.to_string())),
    );
    assert_eq!(keys, expected);

    let docs = row(&orch, two, Some(DOCS)).await;
    assert_eq!(
        (
            docs.pick.as_deref(),
            docs.probability,
            docs.measured_by.as_deref(),
            docs.revision.as_deref(),
            docs.picks_json.as_deref(),
            docs.addresses_from.as_str(),
            docs.event_at,
        ),
        (
            Some("devops"),
            Some(0.48),
            Some("native"),
            Some("rev-devops"),
            Some(r#"["devops","docs"]"#),
            "firings",
            T0,
        ),
        "every row snapshots the decision, not its own address"
    );
    let below = row(&orch, refused_side[0].0, None).await;
    assert_eq!(
        (
            below.pick.as_deref(),
            below.probability,
            below.picks_json.as_deref()
        ),
        (Some("devops"), Some(0.3), Some("[]"))
    );

    let again = sweep_once(&orch, T0 + 120).await.unwrap();
    assert_eq!(
        (again.inserted, again.filled),
        (0, 0),
        "a second pass inserts nothing"
    );
    assert_eq!(rows(&orch).await.len(), 7);
}

#[tokio::test]
async fn an_event_whose_firings_were_evicted_is_counted_unrecorded() {
    let orch = orchestrator().await;
    let lost = fact(&orch, 1, T0, route_state::DONE, "ambient").await;
    let report = sweep_once(&orch, T0 + 11 * 60).await.unwrap();
    assert_eq!((report.inserted, report.unrecorded), (0, 1));
    assert!(
        rows(&orch).await.is_empty(),
        "lost route identity must not be fabricated"
    );
    assert_eq!(feedback::sweep_cursor(&orch.db.local).await.unwrap(), lost);
    let again = sweep_once(&orch, T0 + 12 * 60).await.unwrap();
    assert_eq!((again.inserted, again.unrecorded), (0, 0));
}

#[tokio::test]
async fn a_dispatched_event_with_no_firing_yet_holds_the_cursor_until_it_settles() {
    let orch = orchestrator().await;
    let waiting = fact(&orch, 1, T0, route_state::DONE, "ambient").await;
    let behind = routed(&orch, 2, T0, "refused", &[("devops", 0.2)]).await;

    let report = sweep_once(&orch, T0 + 60).await.unwrap();
    assert_eq!((report.inserted, report.held_at), (0, Some(waiting)));
    assert!(
        rows(&orch).await.is_empty(),
        "nothing behind the held fact is swept"
    );

    // Its acknowledgement journals; the next pass takes both.
    firing(
        &orch,
        waiting,
        &journal("refused", &[("docs", 0.2)]),
        "channel",
        CONVERSATION,
    )
    .await;
    let report = sweep_once(&orch, T0 + 120).await.unwrap();
    assert_eq!((report.inserted, report.held_at), (2, None));
    assert_eq!(row(&orch, behind, None).await.outcome, "refused");
}

#[tokio::test]
async fn an_in_flight_event_holds_and_an_unmatched_one_is_passed_over() {
    let orch = orchestrator().await;
    fact(&orch, 1, T0, route_state::UNMATCHED, "ambient").await;
    let running = fact(&orch, 2, T0, route_state::RUNNING, "ambient").await;
    let report = sweep_once(&orch, T0 + 3600).await.unwrap();
    assert_eq!((report.inserted, report.held_at), (0, Some(running)));
    assert_eq!(
        feedback::sweep_cursor(&orch.db.local).await.unwrap(),
        running - 1,
        "the cursor passes the unmatched fact and stops at the running one"
    );
}

#[tokio::test]
async fn a_recipients_not_mine_and_reroute_label_its_own_row_and_deliver_once() {
    let orch = orchestrator().await;
    let id = routed(&orch, 1, T0, "admitted", &[("devops", 0.9)]).await;

    // (a) Labelled before the sweep: the row is made from its firing.
    write(
        &orch,
        Some("r-devops"),
        json!({"event": event(id), "verdict": "not_mine"}),
    )
    .await
    .unwrap();
    let first = row(&orch, id, Some(DEVOPS)).await;
    assert_eq!(
        (
            first.verdict.as_deref(),
            first.labelled_by.as_deref(),
            first.addresses_from.as_str()
        ),
        (Some("not_mine"), Some(DEVOPS), "firings")
    );
    assert!(first.first_labelled_at.is_some());
    sweep_once(&orch, T0 + 60).await.unwrap();
    assert_eq!(
        rows(&orch).await.len(),
        1,
        "the sweep keeps the labelled row"
    );

    // (b) A second write replaces the verdict, keeps the first time, and
    // delivers the original message once.
    let outbound_before = outbound_count(&orch).await;
    let applied = write(
        &orch,
        Some("r-devops"),
        json!({"event": id, "verdict": "reroute", "to": DOCS, "note": "release notes"}),
    )
    .await
    .unwrap();
    assert!(
        applied.contains("Delivered the original message to cairn://p/cairn/docs"),
        "{applied}"
    );
    let second = row(&orch, id, Some(DEVOPS)).await;
    assert_eq!(
        (
            second.verdict.as_deref(),
            second.reroute_to.as_deref(),
            second.note.as_deref(),
            second.first_labelled_at
        ),
        (
            Some("reroute"),
            Some(DOCS),
            Some("release notes"),
            first.first_labelled_at
        )
    );
    let expected = format!(
        "Re-routed from {DEVOPS} (event {}): originally Decision: devops (0.9, native, rev rev-devops)\n\nmessage 1",
        event(id)
    );
    assert_eq!(
        thread_messages(&orch, "t-docs").await,
        [(Some("r-devops".to_string()), expected)]
    );
    assert_eq!(
        outbound_count(&orch).await,
        outbound_before,
        "nothing goes to Discord"
    );

    write(
        &orch,
        Some("r-devops"),
        json!({"event": event(id), "verdict": "reroute", "to": DOCS}),
    )
    .await
    .unwrap();
    assert_eq!(
        thread_messages(&orch, "t-docs").await.len(),
        1,
        "the same reroute delivers nothing more"
    );
}

#[tokio::test]
async fn only_a_chartered_thread_or_the_operator_labels() {
    let orch = orchestrator().await;
    let id = routed(&orch, 1, T0, "refused", &[("devops", 0.2)]).await;
    let payload = json!({"event": event(id), "verdict": "reroute", "to": DOCS});
    let error = refusal(&orch, Some("r-builder"), payload.clone()).await;
    assert!(
        error.contains("Only a chartered thread can label a decision routing")
            && error.contains("cairn://p/cairn/7/1/builder"),
        "{error}"
    );
    let error = refusal(&orch, Some("r-gone"), payload.clone()).await;
    assert!(
        error.contains("Only a chartered thread or the operator"),
        "{error}"
    );

    // A run-less write without the operator's stamp speaks for nobody, whether
    // it carries no origin or only the machine credential's.
    for origin in [None, Some(DispatchOrigin::UnverifiedLocal)] {
        let error = refusal_as(&orch, None, origin.clone(), payload.clone()).await;
        assert!(
            error.contains("speaks for no run and carries no operator credential, so it cannot label a decision routing"),
            "{origin:?}: {error}"
        );
    }

    // The verified desktop operator, through the channels door, labels the
    // refused side under its installation principal.
    crate::resources::channels::operator_write(
        &orch,
        "cairn://p/cairn/events/feedback",
        ChangeMode::Append,
        Some(payload),
    )
    .await
    .unwrap();
    let labelled = row(&orch, id, None).await;
    assert_eq!(labelled.verdict.as_deref(), Some("reroute"));
    let principal: Value = serde_json::from_str(labelled.labelled_by.as_deref().unwrap()).unwrap();
    assert_eq!(principal["kind"], "machine", "{principal}");
}

/// The operator's labelling branch is reached through the desktop channels
/// door, which both reads the label set and writes a label.
#[tokio::test]
async fn the_operator_labels_project_event_feedback_through_the_desktop_door() {
    let orch = orchestrator().await;
    let id = routed(&orch, 1, T0, "refused", &[("devops", 0.2)]).await;
    crate::resources::channels::operator_write(
        &orch,
        "cairn://p/cairn/events/feedback",
        ChangeMode::Append,
        Some(json!({"event": event(id), "verdict": "reroute", "to": DOCS})),
    )
    .await
    .unwrap();
    let labelled = row(&orch, id, None).await;
    assert_eq!(labelled.verdict.as_deref(), Some("reroute"));
    let principal: Value = serde_json::from_str(labelled.labelled_by.as_deref().unwrap()).unwrap();
    assert_eq!(principal["kind"], "machine", "{principal}");

    let read = crate::resources::channels::operator_read(&orch, "cairn://p/cairn/events/feedback")
        .await
        .unwrap();
    assert!(read.contains(&event(id)), "{read}");
}

#[tokio::test]
async fn every_malformed_label_is_refused_by_name() {
    let orch = orchestrator().await;
    let id = routed(&orch, 1, T0, "admitted", &[("devops", 0.9)]).await;
    let structural = fact(&orch, 2, T0, route_state::DONE, "structural").await;
    let cases = [
        (
            json!({"event": event(id), "verdict": "reroute"}),
            "payload.to is required with verdict reroute",
        ),
        (
            json!({"event": event(id), "verdict": "not_mine", "to": DOCS}),
            "payload.to is refused with verdict not_mine",
        ),
        (
            json!({"event": event(id), "verdict": "wrong"}),
            "is not a verdict",
        ),
        (
            json!({"event": event(id), "verdict": "not_mine", "bogus": 1}),
            "`bogus`",
        ),
        (
            json!({"event": event(structural), "verdict": "not_mine"}),
            "has no dispatched decision routing",
        ),
        (
            json!({"event": "cairn://p/cairn/events/999", "verdict": "not_mine"}),
            "is not in this runner's event log",
        ),
        (
            json!({"event": "cairn://p/other/events/1", "verdict": "not_mine"}),
            "names project other",
        ),
        (
            json!({"event": "the deploy one", "verdict": "not_mine"}),
            "payload.event must be an event URI",
        ),
        (
            json!({"event": event(id), "verdict": "reroute", "to": "cairn://p/cairn/scratch"}),
            "is not an active chartered thread",
        ),
        (
            json!({"event": event(id), "verdict": "reroute", "to": "cairn://p/cairn/old"}),
            "is not an active chartered thread",
        ),
        (
            json!({"event": event(id), "verdict": "reroute", "to": "cairn://p/cairn/nobody"}),
            "is not a thread of project cairn",
        ),
        (
            json!({"event": event(id), "verdict": "reroute", "to": DEVOPS}),
            "cannot re-route a message to itself",
        ),
    ];
    for (payload, expected) in cases {
        let error = refusal(&orch, Some("r-devops"), payload.clone()).await;
        assert!(error.contains(expected), "{payload}: {error}");
    }
}

#[tokio::test]
async fn a_non_recipients_reroute_lands_on_the_refused_side_and_delivers_nothing() {
    let orch = orchestrator().await;
    let id = routed(&orch, 1, T0, "below_threshold", &[("devops", 0.3)]).await;

    let error = refusal(
        &orch,
        Some("r-docs"),
        json!({"event": event(id), "verdict": "not_mine"}),
    )
    .await;
    assert!(error.contains("Nothing to disown"), "{error}");

    // Labelled before the sweep reaches it: the refused-side row is made now.
    write(
        &orch,
        Some("r-docs"),
        json!({"event": event(id), "verdict": "reroute", "to": DEVOPS}),
    )
    .await
    .unwrap();
    let refused = row(&orch, id, None).await;
    assert_eq!(
        (
            refused.outcome.as_str(),
            refused.verdict.as_deref(),
            refused.reroute_to.as_deref(),
            refused.labelled_by.as_deref()
        ),
        ("below_threshold", Some("reroute"), Some(DEVOPS), Some(DOCS))
    );
    assert!(
        thread_messages(&orch, "t-devops").await.is_empty(),
        "a refused-side label delivers nothing"
    );
    sweep_once(&orch, T0 + 60).await.unwrap();
    assert_eq!(
        rows(&orch).await,
        vec![refused],
        "the sweep keeps the label"
    );
}

async fn message(orch: &Orchestrator, run: &str, at: i64, content: &str) {
    orch.db
        .local
        .execute(
            "INSERT INTO messages (id, channel_type, channel_id, sender_run_id, sender_name, content, created_at)
             VALUES (?1, 'thread', 't-devops', ?2, 'agent', ?3, ?4)",
            (uuid::Uuid::new_v4().to_string(), run.to_string(), content.to_string(), at),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn engagement_is_judged_once_at_the_horizon_from_citations_and_sends() {
    let orch = orchestrator().await;
    let cited = routed(&orch, 1, T0, "admitted", &[("devops", 0.9)]).await;
    let fanned = routed(
        &orch,
        2,
        T0,
        "admitted",
        &[("devops", 0.48), ("docs", 0.47)],
    )
    .await;
    let refused = routed(&orch, 3, T0, "refused", &[("devops", 0.2)]).await;
    let fresh = routed(&orch, 4, T0 + HORIZON, "admitted", &[("devops", 0.9)]).await;
    sweep_once(&orch, T0 + 60).await.unwrap();

    // devops cites the first event; a longer id that starts with the second's
    // does not count.
    message(
        &orch,
        "r-devops",
        T0 + 600,
        &format!("Picked up {}", event(cited)),
    )
    .await;
    message(
        &orch,
        "r-devops",
        T0 + 600,
        &format!("see {}0", event(fanned)),
    )
    .await;
    // docs answers the second event's conversation as agent text.
    crate::channels::agent_message::enqueue(
        &orch.db.local,
        &CONVERSATION
            .parse::<crate::channels::ConversationAddress>()
            .unwrap(),
        DOCS,
        None,
        "on it",
        None,
    )
    .await
    .unwrap();
    orch.db
        .local
        .execute(
            "UPDATE channel_outbound SET created_at = ?1",
            ((T0 + 900) * 1000,),
        )
        .await
        .unwrap();
    // docs cites the refused event in prose; a citation after the horizon is late.
    message(
        &orch,
        "r-docs",
        T0 + 1200,
        &format!("event #{refused} is ours"),
    )
    .await;
    message(
        &orch,
        "r-devops",
        T0 + HORIZON + 10,
        &format!("event #{refused}"),
    )
    .await;

    let report = sweep_once(&orch, T0 + HORIZON + 60).await.unwrap();
    assert_eq!(report.engagement_checked, 4);
    let engaged = |row: RouteFeedbackRow| {
        (
            row.engaged,
            row.engaged_by,
            row.engagement_checked_at.is_some(),
        )
    };
    assert_eq!(
        engaged(row(&orch, cited, Some(DEVOPS)).await),
        (Some(true), None, true)
    );
    assert_eq!(
        engaged(row(&orch, fanned, Some(DEVOPS)).await),
        (Some(false), None, true)
    );
    assert_eq!(
        engaged(row(&orch, fanned, Some(DOCS)).await),
        (Some(true), None, true)
    );
    assert_eq!(
        engaged(row(&orch, refused, None).await),
        (Some(true), Some(DOCS.to_string()), true),
        "the refused side names the thread that cited it"
    );
    assert_eq!(
        engaged(row(&orch, fresh, Some(DEVOPS)).await),
        (None, None, false),
        "a row younger than 48 hours is untouched"
    );
    let again = sweep_once(&orch, T0 + HORIZON + 120).await.unwrap();
    assert_eq!(again.engagement_checked, 0, "each row is judged once");
}

#[tokio::test]
async fn the_read_filters_and_refuses_unknown_values() {
    let orch = orchestrator().await;
    routed(
        &orch,
        1,
        T0,
        "admitted",
        &[("devops", 0.48), ("docs", 0.47)],
    )
    .await;
    routed(&orch, 2, T0 + 100, "refused", &[("devops", 0.2)]).await;
    sweep_once(&orch, T0 + 200).await.unwrap();
    let param = |key: &str, value: &str| cairn_common::query::QueryParam {
        key: key.into(),
        value: value.into(),
    };
    let read = |params: Vec<cairn_common::query::QueryParam>| {
        let orch = orch.clone();
        async move { crate::resources::events::render_feedback(&orch, "cairn", &params).await }
    };
    let all = read(vec![]).await;
    assert!(
        all.contains("3 rows; 0 historical unrecorded rows"),
        "{all}"
    );
    let limited = read(vec![param("limit", "1")]).await;
    assert!(
        limited.contains("3 rows; 0 historical unrecorded rows") && limited.contains("Showing 1."),
        "{limited}"
    );
    assert_eq!(
        limited
            .lines()
            .filter(|line| line.starts_with("- "))
            .count(),
        1,
        "{limited}"
    );
    let docs = read(vec![param("thread", DOCS)]).await;
    assert!(
        docs.contains("1 rows") && docs.contains("→ cairn://p/cairn/docs"),
        "{docs}"
    );
    let refused = read(vec![
        param("outcome", "refused"),
        param("since", &(T0 + 50).to_string()),
    ])
    .await;
    assert!(
        refused.contains("1 rows") && refused.contains("(refused side)"),
        "{refused}"
    );
    assert!(read(vec![param("engaged", "unchecked")])
        .await
        .contains("3 rows"));
    assert!(read(vec![param("engaged", "maybe")])
        .await
        .starts_with("Unknown engaged 'maybe'"));
    assert!(read(vec![param("verdict", "accepted")])
        .await
        .starts_with("Unknown verdict"));
    assert!(read(vec![param("bogus", "1")])
        .await
        .starts_with("Unsupported query parameter 'bogus'"));
}

/// A second registered source, route, node and question exercise every generic
/// boundary, rather than merely renaming an ambient fixture.
#[tokio::test]
async fn non_channel_decisions_are_labelled_engaged_and_dropped_deliveries_are_not_recipients() {
    let (orch, server, _) = crate::orchestrator::decide_fixtures::workspace_with_server(
        crate::credentials::CustomProtocol::Decisions, Some("gateway-key"), "typesafe-ai/jev",
        vec![json!({
            "model":"typesafe-ai/jev",
            "answers":[
                {"type":"choice","name":"category","choice":"announcement","probabilities":[
                    {"value":"announcement","probability":0.9},{"value":"incident","probability":0.1}]},
                {"type":"choice","name":"owner","choice":"devops","probabilities":[
                    {"value":"devops","probability":0.9},{"value":"docs","probability":0.1}]}
            ],
            "usage":{"input_tokens":30,"output_tokens":0}
        })],
    ).await;
    seed(&orch.db.local).await;
    std::fs::create_dir_all(orch.config_dir.join("routes")).unwrap();
    let route = r#"
name: Post owner fixture
description: A second decision route over a registered non-channel source.
enabled: true
nodes:
  - id: incoming
    type: trigger
    when: { fact: post_created }
  - id: choose-owner
    type: decision
    input: { field: title }
    questions:
      - name: category
        type: choice
        instructions: What kind?
        options:
          - { value: announcement }
          - { value: incident }
      - name: owner
        type: choice
        source: chartered_threads
        instructions: Which thread owns this?
  - id: deliver
    type: sink
    sink:
      kind: message
      target: { field: answers.owner.addresses }
      body: "{{title}}"
edges:
  - { from: incoming, to: choose-owner }
  - { from: choose-owner, to: deliver, when: { answers.owner.outcome: admitted } }
"#;
    super::super::parse_definition(route, &super::super::FactRegistry::default())
        .expect("the non-channel fixture is a valid route");
    std::fs::write(orch.config_dir.join("routes/post-owner.yaml"), route).unwrap();
    let id = fact(&orch, 1, T0, route_state::DONE, "structural").await;
    orch.db
        .local
        .execute(
            "UPDATE fact_log SET source='post_created', summary='A source-independent routed record', fields_json=?1 WHERE id=?2",
            (
                json!({"project":"cairn", "title":"A source-independent routed record"})
                    .to_string(),
                id,
            ),
        )
        .await
        .unwrap();
    let fact = crate::storage::get_fact(&orch.db.local, id)
        .await
        .unwrap()
        .unwrap();
    let hydrated = super::super::log::hydrate(&orch, &fact).await.unwrap();
    let routes = current_routes(&orch, &fact, T0).await.unwrap();
    let event_uri = event(id);
    super::super::dispatcher::dispatch_row(
        &orch,
        &hydrated.fact,
        hydrated.presence,
        super::super::RouteContext {
            project_id: Some("p"),
            project_path: None,
            event: Some(super::super::LoggedEvent {
                id,
                uri: &event_uri,
            }),
        },
        &routes,
        &mut Vec::new(),
    )
    .await
    .unwrap();
    assert_eq!(
        server.calls(),
        1,
        "the real decision route asked the provider once"
    );
    let journal = crate::storage::list_firings_for_fact(&orch.db.local, id)
        .await
        .unwrap();
    assert_eq!(journal.len(), 1);
    let actual_history = journal[0].transforms_json.as_deref().unwrap();
    firing_for_route(
        &orch,
        "post-owner",
        "post_created",
        id,
        actual_history,
        "message",
        DOCS,
    )
    .await;
    orch.db.local.execute("UPDATE route_firings SET status='dropped', drop_reason='actor' WHERE fact_log_id=?1 AND sink_ref=?2",
        (id,DOCS.to_string())).await.unwrap();
    let report = sweep_once(&orch, T0 + 60).await.unwrap();
    assert_eq!(
        report.inserted, 1,
        "the dropped firing creates no recipient"
    );
    let all = rows(&orch).await;
    assert_eq!(all[0].route_id, "post-owner");
    assert_eq!(all[0].decision_node, "choose-owner");
    assert_eq!(all[0].question, "owner");
    assert_eq!(all[0].source, "post_created");
    assert_eq!(
        all[0].question_text.as_deref(),
        Some("Which thread owns this?")
    );
    assert_eq!(all[0].recipient_thread.as_deref(), Some(DEVOPS));
    write(
        &orch,
        Some("r-devops"),
        json!({"event":event(id),"verdict":"not_mine"}),
    )
    .await
    .unwrap();
    assert_eq!(
        row(&orch, id, Some(DEVOPS)).await.verdict.as_deref(),
        Some("not_mine")
    );
    message(
        &orch,
        "r-devops",
        T0 + 600,
        &format!("Picked up {}", event(id)),
    )
    .await;
    sweep_once(&orch, T0 + HORIZON + 60).await.unwrap();
    assert_eq!(row(&orch, id, Some(DEVOPS)).await.engaged, Some(true));
    write(
        &orch,
        Some("r-devops"),
        json!({"event":id,"verdict":"reroute","to":DOCS}),
    )
    .await
    .unwrap();
    let rerouted = thread_messages(&orch, "t-docs").await;
    assert_eq!(rerouted.len(), 1);
    assert!(
        rerouted[0]
            .1
            .ends_with("A source-independent routed record"),
        "{rerouted:?}"
    );
    let error = refusal(
        &orch,
        Some("r-devops"),
        json!({"event":format!("cairn://routes/events/{id}"),"verdict":"not_mine"}),
    )
    .await;
    assert!(error.contains("Project-less event"), "{error}");
}

#[tokio::test]
async fn an_all_dropped_delivery_does_not_recover_a_recipient_from_answer_addresses() {
    let orch = orchestrator().await;
    let id = routed(&orch, 1, T0, "admitted", &[("devops", 0.9)]).await;
    orch.db.local.execute("UPDATE route_firings SET status='dropped', drop_reason='actor' WHERE fact_log_id=?1 AND sink_kind='message'",(id,)).await.unwrap();
    sweep_once(&orch, T0 + 60).await.unwrap();
    let all = rows(&orch).await;
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].recipient_thread, None);
    assert_eq!(all[0].outcome, "admitted");
    assert!(refusal(
        &orch,
        Some("r-devops"),
        json!({"event":id,"verdict":"not_mine"})
    )
    .await
    .contains("Nothing to disown"));
}

#[tokio::test]
async fn event_keyed_labels_update_all_matching_decision_rows_and_reroute_only_once() {
    let orch = orchestrator().await;
    let id = routed(&orch, 1, T0, "admitted", &[("devops", 0.9)]).await;
    let mut second: Value = serde_json::from_str(&journal("admitted", &[("devops", 0.9)])).unwrap();
    second[0]["decision"] = json!("second-pick");
    firing(&orch, id, &second.to_string(), "message", DEVOPS).await;
    sweep_once(&orch, T0 + 60).await.unwrap();
    assert_eq!(rows(&orch).await.len(), 2);
    write(
        &orch,
        Some("r-devops"),
        json!({"event":id,"verdict":"reroute","to":DOCS}),
    )
    .await
    .unwrap();
    assert!(rows(&orch)
        .await
        .iter()
        .all(|r| r.verdict.as_deref() == Some("reroute")));
    assert_eq!(thread_messages(&orch, "t-docs").await.len(), 1);
    write(
        &orch,
        Some("r-devops"),
        json!({"event":id,"verdict":"reroute","to":DOCS}),
    )
    .await
    .unwrap();
    assert_eq!(thread_messages(&orch, "t-docs").await.len(), 1);
}

#[tokio::test]
async fn an_outsider_labels_retained_decisions_after_journal_eviction() {
    let orch = orchestrator().await;
    let id = routed(&orch, 1, T0, "admitted", &[("devops", 0.9)]).await;
    sweep_once(&orch, T0 + 60).await.unwrap();
    orch.db
        .local
        .execute("DELETE FROM route_firings WHERE fact_log_id=?1", (id,))
        .await
        .unwrap();
    write(
        &orch,
        Some("r-docs"),
        json!({"event":id,"verdict":"reroute","to":DEVOPS}),
    )
    .await
    .unwrap();
    assert_eq!(
        row(&orch, id, None).await.verdict.as_deref(),
        Some("reroute")
    );
    assert!(thread_messages(&orch, "t-devops").await.is_empty());
}

#[tokio::test]
async fn unrelated_missing_content_does_not_wedge_the_global_sweep() {
    let orch = orchestrator().await;
    let missing = fact(&orch, 1, T0, route_state::DONE, "structural").await;
    orch.db
        .local
        .execute(
            "UPDATE fact_log SET source='ingress', record_id='999999' WHERE id=?1",
            (missing,),
        )
        .await
        .unwrap();
    let behind = routed(&orch, 2, T0, "admitted", &[("devops", 0.9)]).await;
    let report = sweep_once(&orch, T0 + 60).await.unwrap();
    assert_eq!(
        (report.inserted, report.unrecorded, report.held_at),
        (1, 0, None)
    );
    assert_eq!(row(&orch, behind, Some(DEVOPS)).await.outcome, "admitted");
}

#[tokio::test]
async fn a_label_without_surviving_route_identity_is_refused_instead_of_guessing() {
    let orch = orchestrator().await;
    let id = fact(&orch, 1, T0, route_state::DONE, "ambient").await;
    let error = crate::resources::channels::operator_write(
        &orch,
        "cairn://p/cairn/events/feedback",
        ChangeMode::Append,
        Some(json!({"event":id,"verdict":"reroute","to":DOCS})),
    )
    .await
    .unwrap_err();
    assert!(error.contains("no dispatched decision routing"), "{error}");
}

fn two_questions(first: &str, second: &str) -> String {
    let mut history: Value = serde_json::from_str(&journal(first, &[("devops", 0.9)])).unwrap();
    let mut other: Value = serde_json::from_str(&journal(second, &[("devops", 0.9)])).unwrap();
    let mut asked = other[0]["asked"]["questions"][0].take();
    asked["name"] = json!("reviewer");
    history[0]["asked"]["questions"]
        .as_array_mut()
        .unwrap()
        .push(asked);
    history[0]["answers"]["reviewer"] = other[0]["answers"]["thread"].take();
    history.to_string()
}

#[tokio::test]
async fn two_routing_questions_keep_distinct_rows_and_one_event_label_and_delivery() {
    let orch = orchestrator().await;
    let id = fact(&orch, 1, T0, route_state::DONE, "ambient").await;
    firing(
        &orch,
        id,
        &two_questions("admitted", "admitted"),
        "message",
        DEVOPS,
    )
    .await;
    sweep_once(&orch, T0 + 60).await.unwrap();
    let all = rows(&orch).await;
    assert_eq!(all.len(), 2);
    assert_eq!(
        all.iter()
            .map(|row| row.question.as_str())
            .collect::<std::collections::BTreeSet<_>>(),
        std::collections::BTreeSet::from(["thread", "reviewer"])
    );
    write(
        &orch,
        Some("r-devops"),
        json!({"event":id,"verdict":"not_mine"}),
    )
    .await
    .unwrap();
    assert!(rows(&orch)
        .await
        .iter()
        .all(|row| row.verdict.as_deref() == Some("not_mine")));
    let first: Vec<_> = rows(&orch)
        .await
        .iter()
        .map(|row| row.first_labelled_at)
        .collect();
    write(
        &orch,
        Some("r-devops"),
        json!({"event":id,"verdict":"reroute","to":DOCS}),
    )
    .await
    .unwrap();
    assert!(rows(&orch)
        .await
        .iter()
        .all(|row| row.verdict.as_deref() == Some("reroute")));
    assert_eq!(
        rows(&orch)
            .await
            .iter()
            .map(|row| row.first_labelled_at)
            .collect::<Vec<_>>(),
        first
    );
    assert_eq!(thread_messages(&orch, "t-docs").await.len(), 1);
    write(
        &orch,
        Some("r-devops"),
        json!({"event":id,"verdict":"reroute","to":DOCS}),
    )
    .await
    .unwrap();
    assert_eq!(thread_messages(&orch, "t-docs").await.len(), 1);
}

#[tokio::test]
async fn mixed_and_refused_questions_keep_their_own_recipient_sides() {
    let orch = orchestrator().await;
    let mixed = fact(&orch, 1, T0, route_state::DONE, "ambient").await;
    firing(
        &orch,
        mixed,
        &two_questions("admitted", "refused"),
        "message",
        DEVOPS,
    )
    .await;
    let refused = fact(&orch, 2, T0, route_state::DONE, "ambient").await;
    firing(
        &orch,
        refused,
        &two_questions("refused", "refused"),
        "channel",
        CONVERSATION,
    )
    .await;
    sweep_once(&orch, T0 + 60).await.unwrap();
    let all = rows(&orch).await;
    assert_eq!(all.len(), 4);
    assert_eq!(
        all.iter()
            .filter(|r| r.event_id == mixed && r.recipient_thread.as_deref() == Some(DEVOPS))
            .count(),
        1
    );
    assert_eq!(
        all.iter()
            .filter(|r| r.event_id == mixed
                && r.question == "reviewer"
                && r.recipient_thread.is_none())
            .count(),
        1
    );
    assert_eq!(
        all.iter()
            .filter(|r| r.event_id == refused && r.recipient_thread.is_none())
            .count(),
        2
    );
}

#[tokio::test]
async fn outsider_labels_every_retained_identity_with_partial_or_evicted_histories() {
    let orch = orchestrator().await;
    let id = fact(&orch, 1, T0, route_state::DONE, "ambient").await;
    firing(
        &orch,
        id,
        &journal("refused", &[("devops", 0.2)]),
        "channel",
        CONVERSATION,
    )
    .await;
    firing(
        &orch,
        id,
        &journal("admitted", &[("devops", 0.9)]),
        "message",
        DEVOPS,
    )
    .await;
    orch.db.local.execute("UPDATE route_firings SET route_id='second-owner' WHERE fact_log_id=?1 AND sink_kind='message'",(id,)).await.unwrap();
    sweep_once(&orch, T0 + 60).await.unwrap();
    orch.db
        .local
        .execute(
            "DELETE FROM route_firings WHERE fact_log_id=?1 AND route_id='second-owner'",
            (id,),
        )
        .await
        .unwrap();
    write(
        &orch,
        Some("r-docs"),
        json!({"event":id,"verdict":"reroute","to":DEVOPS}),
    )
    .await
    .unwrap();
    let refused: Vec<_> = rows(&orch)
        .await
        .into_iter()
        .filter(|r| r.recipient_thread.is_none())
        .collect();
    assert_eq!(
        refused.len(),
        2,
        "both surviving and retained-only decisions are labelled"
    );
    assert!(refused
        .iter()
        .all(|r| r.verdict.as_deref() == Some("reroute")));
    orch.db
        .local
        .execute("DELETE FROM route_firings WHERE fact_log_id=?1", (id,))
        .await
        .unwrap();
    crate::resources::channels::operator_write(
        &orch,
        "cairn://p/cairn/events/feedback",
        ChangeMode::Append,
        Some(json!({"event":id,"verdict":"reroute","to":DOCS})),
    )
    .await
    .unwrap();
    assert!(rows(&orch)
        .await
        .iter()
        .filter(|r| r.recipient_thread.is_none())
        .all(|r| r.reroute_to.as_deref() == Some(DOCS)));
}

#[tokio::test]
async fn carried_cursor_does_not_duplicate_migrated_ambient_rows() {
    let orch = orchestrator().await;
    let id = routed(&orch, 1, T0, "admitted", &[("devops", 0.9)]).await;
    sweep_once(&orch, T0 + 60).await.unwrap();
    let before = rows(&orch).await;
    assert_eq!(feedback::sweep_cursor(&orch.db.local).await.unwrap(), id);
    orch.db
        .local
        .execute("DELETE FROM route_firings WHERE fact_log_id=?1", (id,))
        .await
        .unwrap();
    let report = sweep_once(&orch, T0 + 120).await.unwrap();
    assert_eq!((report.inserted, report.unrecorded), (0, 0));
    assert_eq!(rows(&orch).await, before);
}

#[tokio::test]
async fn an_unavailable_chartered_question_keeps_its_refused_row_without_options() {
    let orch = orchestrator().await;
    let id = fact(&orch, 1, T0, route_state::DONE, "ambient").await;
    let mut history: Value = serde_json::from_str(&journal("unanswerable", &[])).unwrap();
    history[0]["asked"]["questions"][0]["source"] = json!("chartered_threads");
    history[0]["asked"]["questions"][0]["options"] = json!([]);
    firing(&orch, id, &history.to_string(), "channel", CONVERSATION).await;
    sweep_once(&orch, T0 + 60).await.unwrap();
    assert_eq!(row(&orch, id, None).await.outcome, "unanswerable");
}

#[tokio::test]
async fn one_questions_addresses_do_not_acquire_another_questions_delivery() {
    let orch = orchestrator().await;
    let id = fact(&orch, 1, T0, route_state::DONE, "ambient").await;
    let mut history: Value = serde_json::from_str(&two_questions("admitted", "admitted")).unwrap();
    history[0]["answers"]["reviewer"]["addresses"] = json!([DOCS]);
    firing(&orch, id, &history.to_string(), "message", DEVOPS).await;
    sweep_once(&orch, T0 + 60).await.unwrap();
    let all = rows(&orch).await;
    assert_eq!(all.len(), 2);
    assert!(all
        .iter()
        .any(|r| r.question == "thread" && r.recipient_thread.as_deref() == Some(DEVOPS)));
    assert!(all
        .iter()
        .any(|r| r.question == "reviewer" && r.recipient_thread.is_none()));
}

#[tokio::test]
async fn the_feedback_resource_project_must_own_the_event() {
    let orch = orchestrator().await;
    let id = routed(&orch, 1, T0, "admitted", &[("devops", 0.9)]).await;
    let item = ChangeItem {
        target: "cairn://p/other/events/feedback".into(),
        mode: ChangeMode::Append,
        payload: Some(json!({"event":id,"verdict":"not_mine"})),
    };
    let error = dispatch_resource_change(&orch, &request(Some("r-devops"), None), 0, &item, false)
        .await
        .unwrap_err();
    assert!(
        error.error.contains("not writer project other"),
        "{}",
        error.error
    );
    assert!(rows(&orch).await.is_empty());
}

#[tokio::test]
async fn a_known_channel_only_route_is_not_thread_routing_feedback() {
    let orch = orchestrator().await;
    let path = orch.config_dir.join("routes/ambient-channel.yaml");
    let mut route: Value = serde_yaml::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    route["nodes"]
        .as_array_mut()
        .unwrap()
        .retain(|node| node["id"] != "deliver");
    route["edges"]
        .as_array_mut()
        .unwrap()
        .retain(|edge| edge["to"] != "deliver");
    std::fs::write(path, serde_yaml::to_string(&route).unwrap()).unwrap();
    let id = fact(&orch, 1, T0, route_state::DONE, "ambient").await;
    firing(
        &orch,
        id,
        &journal("refused", &[("devops", 0.2)]),
        "channel",
        CONVERSATION,
    )
    .await;
    let report = sweep_once(&orch, T0 + 60).await.unwrap();
    assert_eq!((report.inserted, report.unrecorded), (0, 0));
    assert!(rows(&orch).await.is_empty());
}

#[tokio::test]
async fn deleted_route_needs_historical_fired_thread_evidence_not_just_charter_options() {
    let orch = orchestrator().await;
    let refused = routed(&orch, 1, T0, "refused", &[("devops", 0.2)]).await;
    let delivered = routed(&orch, 2, T0, "admitted", &[("devops", 0.9)]).await;
    std::fs::remove_file(orch.config_dir.join("routes/ambient-channel.yaml")).unwrap();
    super::super::invalidate_trigger_index(&orch);
    let report = sweep_once(&orch, T0 + 60).await.unwrap();
    assert_eq!((report.inserted, report.unrecorded), (1, 0));
    let all = rows(&orch).await;
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].event_id, delivered);
    assert_ne!(all[0].event_id, refused);
    assert_eq!(all[0].recipient_thread.as_deref(), Some(DEVOPS));
}
