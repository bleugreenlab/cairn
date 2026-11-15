//! The shipped post-relevance route, end to end on the route lane: the core
//! pack's own file installed as a workspace route, a post minted where posts
//! are written, its `text` hydrated from the post record, the real decide path
//! against a scripted provider, delivery to the chosen thread as the route at
//! `queue`, and the post's attribution read back by reference from the
//! firings.
//!
//! The route's floors (native 0.4, logprobs 0.6) mirror the ambient route's.
//! Confidence is chance-normalized over the declared options, so the fixture
//! charters ten threads, each holding a running session.

use super::posts_tests::{agent, human, post};
use super::*;
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::messages::delivery::{append_thread_message, ThreadSender};
use crate::messages::queued::DeliveryUrgency;
use crate::models::{Post, PostRouting};
use crate::orchestrator::decide_fixtures::{self, Scripted};
use crate::routes::decision::revision;
use crate::storage::RowExt;
use cairn_common::identity::{AppearanceSnapshot, PrincipalRef};
use cairn_uri::build_thread_uri;
use serde_json::json;
use std::sync::Arc;

/// The route's id: the shipped file's stem.
const ROUTE: &str = "post-relevance";
const PROJECT: &str = "cairn";
const SENDER: &str = "route:post-relevance";
const FEED: &str = r#"{"fact":"post_created"}"#;

/// The project's chartered threads, in option (name) order.
const CHARTERS: [(&str, &str); 10] = [
    ("api", "The public API."),
    ("billing", "Plans and invoices."),
    ("cli", "The command line."),
    ("design", "Visual design."),
    ("devops", "How the project builds and ships."),
    ("docs", "The documentation."),
    ("infra", "Servers and networks."),
    ("mobile", "The phone apps."),
    ("security", "Threats and secrets."),
    ("ui", "The interface."),
];

fn charter(name: &str) -> &'static str {
    CHARTERS
        .iter()
        .find(|(chartered, _)| *chartered == name)
        .map(|(_, charter)| *charter)
        .unwrap()
}

/// The route exactly as the core pack ships it, read when the test runs.
fn shipped_route() -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../packs/core/routes/post-relevance.yaml");
    std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("reading {}: {error}", path.display()))
}

/// The URI the route's deliveries name it by, which a thread mutes.
fn route_uri() -> String {
    cairn_uri::build_route_uri(ROUTE)
}

fn session(thread: &str) -> String {
    format!("j-{thread}")
}

/// Project `cairn`, its ten chartered threads each with a running session,
/// and `reader`, an unchartered thread that watches the Posts feed. The
/// shipped route is installed as a workspace route when `with_route`.
async fn install(orch: &Orchestrator, with_route: bool) {
    std::fs::create_dir_all(orch.config_dir.join("routes")).unwrap();
    if with_route {
        std::fs::write(
            orch.config_dir.join(format!("routes/{ROUTE}.yaml")),
            shipped_route(),
        )
        .unwrap();
    }
    let mut sql = String::from(
        "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w', 'W', 1, 1);
         INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
           VALUES('p', 'w', 'Cairn', 'cairn', '/tmp/cairn', 1, 1);",
    );
    let threads = CHARTERS
        .iter()
        .map(|(name, charter)| (*name, format!("'{charter}'")))
        .chain([("reader", "NULL".to_string())]);
    for (name, jurisdiction) in threads {
        sql.push_str(&format!(
            "INSERT INTO threads(id, project_id, name, jurisdiction, status, attention, created_at, updated_at)
               VALUES('t-{name}', 'p', '{name}', {jurisdiction}, 'active', 'none', 1, 1);
             INSERT INTO jobs(id, thread_id, project_id, status, uri_segment, created_at, updated_at)
               VALUES('j-{name}', 't-{name}', 'p', 'running', 'thread', 1, 1);
             INSERT INTO runs(id, job_id, status, created_at, updated_at)
               VALUES('r-{name}', 'j-{name}', 'running', 1, 1);"
        ));
    }
    orch.db.local.execute_batch(&sql).await.unwrap();
    super::fixture::standing_triggers(&orch.db.local, &session("reader"), &format!("[{FEED}]"))
        .await;
}

/// A workspace whose one decision offer is a native Decisions server answering
/// from `replies`, with the route installed.
async fn workspace(replies: Vec<Value>) -> (Orchestrator, Arc<Scripted>) {
    let (orch, scripted, _) = decide_fixtures::workspace_with_server(
        crate::credentials::CustomProtocol::Decisions,
        Some("gateway-key"),
        "typesafe-ai/jev",
        replies,
    )
    .await;
    install(&orch, true).await;
    (orch, scripted)
}

/// A workspace whose one decision offer is a logprobs chat server.
async fn logprobs_workspace(named: &[(&str, f64)]) -> (Orchestrator, Arc<Scripted>) {
    let (orch, scripted, _) = decide_fixtures::workspace_with_server(
        crate::credentials::CustomProtocol::OpenAiChatCompletions,
        None,
        "qwen3",
        vec![decide_fixtures::with_logprobs(), logprobs_reply(named)],
    )
    .await;
    install(&orch, true).await;
    (orch, scripted)
}

/// Every chartered thread's probability: `named` as given, the remaining mass
/// spread evenly over the rest.
fn distribution(named: &[(&str, f64)]) -> Vec<(&'static str, f64)> {
    let rest =
        (1.0 - named.iter().map(|(_, p)| p).sum::<f64>()) / (CHARTERS.len() - named.len()) as f64;
    CHARTERS
        .iter()
        .map(|(name, _)| {
            let given = named.iter().find(|(named, _)| named == name);
            (*name, given.map_or(rest, |(_, p)| *p))
        })
        .collect()
}

/// A Decisions reply picking `choice` over [`distribution`]`(named)`.
fn choice_reply(choice: &str, named: &[(&str, f64)]) -> Value {
    json!({
        "model": "typesafe-ai/jev",
        "answers": [{
            "type": "choice", "name": "route", "choice": choice,
            "probabilities": distribution(named).iter().map(|(value, probability)| json!({"value": value, "probability": probability})).collect::<Vec<_>>()
        }],
        "usage": {"input_tokens": 30, "output_tokens": 0}
    })
}

/// A chat reply whose first token's logprobs put [`distribution`]`(named)`
/// on the option labels (`A` for the first chartered thread, and so on).
fn logprobs_reply(named: &[(&str, f64)]) -> Value {
    let labelled: Vec<(String, f64)> = distribution(named)
        .iter()
        .enumerate()
        .map(|(index, (_, p))| (char::from(b'A' + index as u8).to_string(), *p))
        .collect();
    let (best, best_p) = labelled
        .iter()
        .cloned()
        .max_by(|a, b| a.1.total_cmp(&b.1))
        .unwrap();
    json!({"model": "qwen3", "choices": [{
        "message": {"role": "assistant", "content": best},
        "logprobs": {"content": [{"token": best, "logprob": best_p.ln(), "top_logprobs":
            labelled.iter().map(|(token, p)| json!({"token": token, "logprob": p.ln()})).collect::<Vec<_>>()
        }]}
    }]})
}

/// One post as the lane saw it.
struct Published {
    post: Post,
    row: FactRow,
}

impl Published {
    fn uri(&self) -> String {
        format!("cairn://posts/{}", self.post.id)
    }

    fn event(&self) -> String {
        fact_row_uri(Some(PROJECT), self.row.id)
    }

    fn fields(&self) -> BTreeMap<String, Value> {
        serde_json::from_str(&self.row.fields_json).unwrap()
    }
}

/// Publish a post where posts are written, drain the log, and keep its row.
/// The stored row never carries the writing: the lane hydrates `text`.
async fn publish(
    orch: &Orchestrator,
    project: Option<&str>,
    content: &str,
    author: (PrincipalRef, AppearanceSnapshot),
) -> Published {
    let post = post(orch, project, "Release notes", content, author, None).await;
    let subject = format!("cairn://posts/{}", post.id);
    let stored = crate::storage::latest_fact_for_subject(&orch.db.local, "post_created", &subject)
        .await
        .unwrap()
        .expect("the post's row");
    let stored_fields: BTreeMap<String, Value> = serde_json::from_str(&stored.fields_json).unwrap();
    assert!(
        !stored_fields.contains_key("text") && !stored.fields_json.contains(content),
        "the row stores pointers and labels, not the post: {}",
        stored.fields_json
    );
    drain_fact_log(orch).await;
    let row = crate::storage::get_fact(&orch.db.local, stored.id)
        .await
        .unwrap()
        .unwrap();
    Published { post, row }
}

/// `(thread name, sender, content)` of every thread message, by name.
async fn thread_messages(orch: &Orchestrator) -> Vec<(String, String, String)> {
    orch.db
        .local
        .query_all(
            "SELECT t.name, m.sender_name, m.content FROM messages m JOIN threads t ON t.id = m.channel_id
              WHERE m.channel_type = 'thread' ORDER BY t.name, m.created_at",
            (),
            |row| Ok((row.text(0)?, row.text(1)?, row.text(2)?)),
        )
        .await
        .unwrap()
}

/// `(recipient job, urgency)` of every direct message `sender` wrote: the
/// delivery a wake persists for a session.
async fn directs_from(orch: &Orchestrator, sender: &str) -> Vec<(String, Option<String>)> {
    orch.db
        .local
        .query_all(
            "SELECT r.recipient_job_id, m.urgency FROM messages m JOIN message_recipients r ON r.message_id = m.id
              WHERE m.channel_type = 'direct' AND m.sender_name = ?1 ORDER BY r.recipient_job_id",
            (sender.to_string(),),
            |row| Ok((row.text(0)?, row.opt_text(1)?)),
        )
        .await
        .unwrap()
}

/// `(recipient job, sender)` of every direct message the route did not write.
async fn other_directs(orch: &Orchestrator) -> Vec<(String, String)> {
    orch.db
        .local
        .query_all(
            "SELECT r.recipient_job_id, m.sender_name FROM messages m JOIN message_recipients r ON r.message_id = m.id
              WHERE m.channel_type = 'direct' AND m.sender_name != ?1 ORDER BY r.recipient_job_id, m.sender_name",
            (SENDER.to_string(),),
            |row| Ok((row.text(0)?, row.text(1)?)),
        )
        .await
        .unwrap()
}

/// How many pushes marked their turn as the operator's own.
async fn operator_pushes(orch: &Orchestrator) -> i64 {
    orch.db
        .local
        .query_opt_i64(
            "SELECT COUNT(*) FROM attention_pushes WHERE key LIKE '%:operator_message:%'",
            (),
        )
        .await
        .unwrap()
        .unwrap_or_default()
}

/// Who the row woke, as `(job, via)` sorted by job.
fn woke(row: &FactRow) -> Vec<(String, Option<String>)> {
    let wakes: Vec<Value> = row
        .wakes_json
        .as_deref()
        .map(|json| serde_json::from_str(json).unwrap())
        .unwrap_or_default();
    let mut woke: Vec<_> = wakes
        .iter()
        .map(|wake| {
            (
                wake["job"].as_str().unwrap().to_owned(),
                wake["via"].as_str().map(str::to_owned),
            )
        })
        .collect();
    woke.sort();
    woke
}

async fn firings(orch: &Orchestrator) -> Vec<crate::storage::RouteFiringRecord> {
    let mut firings = crate::storage::list_route_firings(&orch.db.local, "workspace", ROUTE, 100)
        .await
        .unwrap();
    firings.sort_by_key(|firing| firing.seq);
    firings
}

/// The one decision record a firing's journal carries.
fn decision_record(firing: &crate::storage::RouteFiringRecord) -> Value {
    let history: Vec<Value> = serde_json::from_str(
        firing
            .transforms_json
            .as_deref()
            .expect("a journaled decision"),
    )
    .unwrap();
    let [entry] = history.as_slice() else {
        panic!("one decision on the path: {history:?}");
    };
    assert_eq!(entry["decision"], "relevance");
    assert_eq!(
        entry["asked"]["thresholds"],
        json!({"native": 0.4, "logprobs": 0.6}),
        "the route's own method-keyed floors"
    );
    entry.clone()
}

/// The one unrouted firing a non-admitted outcome journals, as
/// `(outcome, reason)`.
async fn one_unrouted(orch: &Orchestrator, published: &Published) -> (String, String) {
    let firings = firings(orch).await;
    let [firing] = firings.as_slice() else {
        panic!("exactly one journaled row: {firings:#?}");
    };
    assert_eq!(
        (
            firing.status.as_str(),
            firing.drop_reason.as_deref(),
            firing.fact_log_id
        ),
        ("dropped", Some("unrouted"), Some(published.row.id))
    );
    let answers = &decision_record(firing)["answers"]["route"];
    (
        answers["outcome"].as_str().unwrap().to_string(),
        answers["reason"].as_str().unwrap().to_string(),
    )
}

fn citation(name: &str, probability: &str, method: &str) -> String {
    format!(
        "{name} ({probability}, {method}, rev {})",
        revision(charter(name))
    )
}

/// The message the route delivers, whole.
async fn delivered(orch: &Orchestrator, citations: &str, published: &Published) -> String {
    let text = crate::orchestrator::wakes::render_post_wake(&orch.db.local, &published.uri())
        .await
        .expect("the post renders");
    let (post, event) = (published.uri(), published.event());
    format!(
        "Decision: {citations}.\nPost {post} named no thread; this route judged it yours. Event {event}.\nIf this isn't yours, answer the event: write cairn:@/events/feedback {{event: \"{event}\", verdict: not_mine | reroute, to?}}.\n\n{text}\n"
    )
}

async fn assert_nothing_delivered(orch: &Orchestrator) {
    assert!(thread_messages(orch).await.is_empty());
    assert!(directs_from(orch, SENDER).await.is_empty());
}

async fn read_post(orch: &Orchestrator, post: &Post) -> String {
    crate::resources::posts::read_post(&orch.db.local, Some(orch), post.id, &[]).await
}

const UNADDRESSED: &str = "The release notes build fails on every tag since Tuesday.";

/// Acceptance 2, 3 (attribution) and 6: one admitted pick hears the post as
/// the route, queued, with the decision cited; nobody else hears anything the
/// route did not cause; and the post renders the attribution by reference.
#[tokio::test(flavor = "multi_thread")]
async fn an_unaddressed_post_reaches_the_thread_it_concerns_as_the_route_queued() {
    let (orch, server) = workspace(vec![choice_reply("devops", &[("devops", 0.82)])]).await;
    let published = publish(&orch, Some("p"), UNADDRESSED, human()).await;

    assert_eq!(published.fields()["addressed"], json!(false));
    assert_eq!(server.calls(), 1, "one decision, asked once");
    let devops = citation("devops", "0.82", "native");
    let body = delivered(&orch, &devops, &published).await;
    assert_eq!(
        thread_messages(&orch).await,
        [("devops".to_string(), SENDER.to_string(), body.clone())],
        "exactly one message, to the pick, from the route"
    );
    assert_eq!(
        directs_from(&orch, SENDER).await,
        [(session("devops"), Some("queue".to_string()))],
        "the route's wake queues for the thread's next turn rather than steering this one"
    );
    assert_eq!(
        operator_pushes(&orch).await,
        0,
        "a route's judgment never wakes a thread as the operator"
    );

    let firings = firings(&orch).await;
    let [firing] = firings.as_slice() else {
        panic!("one delivery and nothing dropped: {firings:#?}");
    };
    assert_eq!(
        (
            firing.status.as_str(),
            firing.sink_kind.as_str(),
            firing.sink_ref.clone(),
            firing.fact_log_id,
        ),
        (
            "fired",
            "message",
            Some(build_thread_uri(PROJECT, "devops")),
            Some(published.row.id),
        )
    );
    let answers = &decision_record(firing)["answers"]["route"];
    assert_eq!(answers["outcome"], "admitted");
    assert_eq!(answers["measuredBy"], "native");
    assert_eq!(answers["citations"], json!([devops]));

    // Everyone else is reached exactly as they would be with no route: the
    // feed reader by its standing trigger, and nobody by anything new.
    let (without, _, _) = decide_fixtures::workspace_with_server(
        crate::credentials::CustomProtocol::Decisions,
        Some("gateway-key"),
        "typesafe-ai/jev",
        vec![],
    )
    .await;
    install(&without, false).await;
    let baseline = publish(&without, Some("p"), UNADDRESSED, human()).await;
    assert_eq!(woke(&published.row), woke(&baseline.row));
    assert!(
        woke(&published.row)
            .iter()
            .any(|(job, _)| *job == session("reader")),
        "the Posts feed still carries the post: {:?}",
        woke(&published.row)
    );
    assert_eq!(other_directs(&orch).await, other_directs(&without).await);

    // The post renders the attribution by reference to the firing.
    let event = published.event();
    let rendered = read_post(&orch, &published.post).await;
    assert!(
        rendered.contains(&format!(
            "- Routed by Post relevance: {devops} (event {event})\n"
        )),
        "{rendered}"
    );
    let mut posts = vec![published.post.clone()];
    crate::posts::attach_routing(&orch, &mut posts).await;
    assert_eq!(
        posts[0].routing,
        [PostRouting {
            route: "Post relevance".into(),
            citations: vec![devops.clone()],
            event: event.clone(),
        }]
    );
    assert!(
        !read_post(&without, &baseline.post)
            .await
            .contains("Routed by"),
        "a post no route delivered renders no attribution"
    );

    // The receiving thread declines through the decision-route feedback
    // surface, against this route's event.
    let request = McpCallbackRequest {
        answer_bound_ms: None,
        thread_id: None,
        cwd: "/tmp".to_string(),
        run_id: Some("r-devops".to_string()),
        tool: "change".to_string(),
        payload: json!({}),
        tool_use_id: None,
        origin: None,
    };
    let item = ChangeItem {
        target: cairn_uri::build_project_event_feedback_uri(PROJECT),
        mode: ChangeMode::Append,
        payload: Some(json!({"event": event, "verdict": "not_mine"})),
    };
    let labelled =
        crate::resources::mutations::dispatch_resource_change(&orch, &request, 0, &item, false)
            .await
            .map(|applied| applied.summary)
            .unwrap_or_else(|failure| panic!("the decline is accepted: {}", failure.error));
    assert!(labelled.contains("not_mine"), "{labelled}");
}

/// Acceptance 2, the trigger clause: the same post naming the thread reaches
/// it by its mention, and the route is never asked.
#[tokio::test(flavor = "multi_thread")]
async fn a_post_naming_the_thread_wakes_it_by_mention_and_asks_no_decision() {
    let (orch, server) = workspace(vec![]).await;
    let published = publish(
        &orch,
        Some("p"),
        &format!("{UNADDRESSED} cairn://p/cairn/devops"),
        human(),
    )
    .await;

    assert_eq!(published.fields()["addressed"], json!(true));
    assert_eq!(server.calls(), 0, "no decision was asked");
    assert_eq!(
        published.row.route_state,
        crate::storage::route_state::UNMATCHED
    );
    assert!(firings(&orch).await.is_empty());
    assert_nothing_delivered(&orch).await;
    assert!(
        woke(&published.row).contains(&(session("devops"), Some("mentions".to_string()))),
        "{:?}",
        woke(&published.row)
    );
    assert!(!read_post(&orch, &published.post)
        .await
        .contains("Routed by"));
}

/// Acceptance 3: below threshold, refusal and provider error each deliver
/// nothing and journal one unrouted row, and the three rows differ.
#[tokio::test(flavor = "multi_thread")]
async fn each_non_admitted_outcome_journals_its_own_unrouted_row_and_delivers_nothing() {
    let mut journaled = Vec::new();
    for reply in [
        // devops at 0.4 normalizes to 0.33, under the native floor.
        choice_reply("devops", &[("devops", 0.4), ("docs", 0.3)]),
        json!({
            "model": "typesafe-ai/jev",
            "answers": [{"type": "refusal", "name": "route"}],
            "usage": {"input_tokens": 30, "output_tokens": 0}
        }),
        json!({"upstreamError": "gateway exploded"}),
    ] {
        let (orch, server) = workspace(vec![reply]).await;
        let published = publish(&orch, Some("p"), UNADDRESSED, human()).await;
        assert_eq!(server.calls(), 1);
        assert_nothing_delivered(&orch).await;
        journaled.push(one_unrouted(&orch, &published).await);
        assert!(
            !read_post(&orch, &published.post)
                .await
                .contains("Routed by"),
            "an unrouted post renders no attribution, and no 'not relevant' either"
        );
    }
    let outcomes: Vec<_> = journaled
        .iter()
        .map(|(outcome, _)| outcome.as_str())
        .collect();
    assert_eq!(outcomes, ["below_threshold", "refused", "failed"]);
    for (index, (_, reason)) in journaled.iter().enumerate() {
        for (_, other) in &journaled[index + 1..] {
            assert_ne!(reason, other, "each outcome journals its own reason");
        }
    }
}

/// A workspace post has no project, so there are no chartered threads to
/// choose among: it journals `unanswerable` and asks nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_workspace_post_is_unanswerable_without_asking() {
    let (orch, server) = workspace(vec![]).await;
    let published = publish(&orch, None, UNADDRESSED, human()).await;
    assert_eq!(server.calls(), 0);
    assert_nothing_delivered(&orch).await;
    let firings = firings(&orch).await;
    let [firing] = firings.as_slice() else {
        panic!("one unrouted row: {firings:#?}");
    };
    assert_eq!(
        (firing.status.as_str(), firing.drop_reason.as_deref()),
        ("dropped", Some("unrouted"))
    );
    let answers = &decision_record(firing)["answers"]["route"];
    assert_eq!(answers["outcome"], "unanswerable");
    assert!(
        answers["reason"]
            .as_str()
            .unwrap()
            .contains("belongs to no project"),
        "{answers}"
    );
    assert_eq!(published.row.project_id, None);
}

/// Acceptance 4: the floors are keyed by method. devops at 0.55 normalizes to
/// 0.5, over the native floor (0.4) and under the logprobs floor (0.6), so the
/// same probabilities deliver when measured natively and not by logprobs.
#[tokio::test(flavor = "multi_thread")]
async fn a_confidence_between_the_floors_delivers_under_native_and_not_under_logprobs() {
    let named = [("devops", 0.55)];
    let (native, server) = workspace(vec![choice_reply("devops", &named)]).await;
    let published = publish(&native, Some("p"), UNADDRESSED, human()).await;
    assert_eq!(server.calls(), 1);
    let devops = citation("devops", "0.55", "native");
    assert_eq!(
        thread_messages(&native).await,
        [(
            "devops".to_string(),
            SENDER.to_string(),
            delivered(&native, &devops, &published).await
        )]
    );

    let (logprobs, server) = logprobs_workspace(&named).await;
    let published = publish(&logprobs, Some("p"), UNADDRESSED, human()).await;
    assert_eq!(
        server.calls(),
        2,
        "the offer's logprobs probe, then the question"
    );
    assert_nothing_delivered(&logprobs).await;
    let (outcome, reason) = one_unrouted(&logprobs, &published).await;
    assert_eq!(outcome, "below_threshold");
    assert_eq!(
        reason,
        "'route' picked 'devops' with logprobs confidence 0.50, below the logprobs threshold 0.60"
    );
}

/// Acceptance 5: the route's message obeys a mute on its sender, the route,
/// and only that. A muted Posts feed governs feed wakes, not the route.
#[tokio::test(flavor = "multi_thread")]
async fn a_thread_that_muted_the_route_keeps_the_message_without_a_wake_and_a_feed_mute_does_not_silence_it(
) {
    let (orch, _) = workspace(vec![choice_reply("devops", &[("devops", 0.82)])]).await;
    cairn_attention::wakes::mute(
        &orch.db.local,
        &session("devops"),
        "peer",
        Some(&route_uri()),
        None,
        None,
        None,
        "agent",
    )
    .await
    .unwrap();
    let published = publish(&orch, Some("p"), UNADDRESSED, human()).await;
    let devops = citation("devops", "0.82", "native");
    assert_eq!(
        thread_messages(&orch).await,
        [(
            "devops".to_string(),
            SENDER.to_string(),
            delivered(&orch, &devops, &published).await
        )],
        "the conversation record is kept"
    );
    assert!(
        directs_from(&orch, SENDER).await.is_empty(),
        "the muted route wakes nothing"
    );
    assert_eq!(
        orch.db
            .local
            .query_opt_i64(
                "SELECT COUNT(*) FROM attention_pushes WHERE recipient = ?1",
                (session("devops"),),
            )
            .await
            .unwrap(),
        Some(0),
        "no push reaches the thread that muted the route"
    );

    let (orch, _) = workspace(vec![choice_reply("devops", &[("devops", 0.82)])]).await;
    super::fixture::standing_triggers(&orch.db.local, &session("devops"), &format!("[{FEED}]"))
        .await;
    cairn_attention::wakes::mute(
        &orch.db.local,
        &session("devops"),
        "fact",
        Some(FEED),
        None,
        None,
        None,
        "agent",
    )
    .await
    .unwrap();
    publish(&orch, Some("p"), UNADDRESSED, human()).await;
    assert_eq!(
        directs_from(&orch, SENDER).await,
        [(session("devops"), Some("queue".to_string()))],
        "a muted Posts feed does not mute the route"
    );
}

/// The funnel's other senders are unchanged: the operator's own message is
/// still the operator's turn, steering.
#[tokio::test(flavor = "multi_thread")]
async fn an_operator_message_to_a_thread_still_steers_as_the_operator() {
    let (orch, _) = workspace(vec![]).await;
    append_thread_message(
        &orch,
        &orch.db.local,
        "t-devops",
        ThreadSender::Operator { name: "external" },
        "Take a look at the release.",
        DeliveryUrgency::Steer,
    )
    .await
    .unwrap();
    assert_eq!(
        directs_from(&orch, "external").await,
        [(session("devops"), Some("steer".to_string()))]
    );
    assert_eq!(operator_pushes(&orch).await, 1);
}

/// A post row minted directly with `author` as given, for a URI form the
/// post write path would have canonicalized away.
async fn publish_minted(orch: &Orchestrator, author: &str) -> Published {
    let (principal, appearance) = human();
    let post = orch
        .db
        .local
        .create_post(crate::models::CreatePost {
            project_id: Some("p".into()),
            camp_id: None,
            title: Some("Release notes".into()),
            content: UNADDRESSED.into(),
            author: principal,
            appearance,
        })
        .await
        .unwrap();
    let uri = format!("cairn://posts/{}", post.id);
    let fields = BTreeMap::from([
        ("postUri".to_string(), json!(uri)),
        ("scope".to_string(), json!(PROJECT)),
        ("project".to_string(), json!(PROJECT)),
        ("title".to_string(), json!("Release notes")),
        ("mentions".to_string(), json!([])),
        ("addressed".to_string(), json!(false)),
        ("author".to_string(), json!(author)),
    ]);
    let id = crate::routes::mint(
        orch,
        crate::routes::Mint {
            fact: crate::routes::RouteFact {
                source: "post_created".into(),
                identity: uri.clone(),
                fields,
                origin: None,
                summary: Some("Release notes".into()),
                route_provenance: None,
            },
            record_ref: uri,
            record_id: Some(post.id.to_string()),
            project_id: Some("p"),
            presence: crate::routes::Presence::Away,
            actor_job_id: None,
        },
    )
    .await
    .unwrap()
    .expect("a post_created row");
    drain_fact_log(orch).await;
    let row = crate::storage::get_fact(&orch.db.local, id)
        .await
        .unwrap()
        .unwrap();
    Published { post, row }
}

/// A route never hands a thread its own post. The author's thread is pick 1
/// of 2; it drops as `actor` and pick 2 is delivered, and the Playground
/// preview shows the drop. Homes are compared resolved, so the author's home
/// written as `cairn://p/CAIRN/devops` (a different string naming the same
/// thread) drops just the same.
#[tokio::test(flavor = "multi_thread")]
async fn the_authors_own_thread_is_dropped_as_actor_and_the_other_pick_delivered() {
    // devops 0.48 and docs 0.47 normalize to 0.42 and 0.41, both admitted.
    let tie = [("devops", 0.48), ("docs", 0.47)];
    for author in ["cairn://p/cairn/devops", "cairn://p/CAIRN/devops"] {
        let (orch, server) = workspace(vec![
            choice_reply("devops", &tie),
            choice_reply("devops", &tie),
        ])
        .await;
        let published = if author == "cairn://p/cairn/devops" {
            // Through the real write path, which stores the canonical form.
            publish(&orch, Some("p"), UNADDRESSED, agent(author)).await
        } else {
            publish_minted(&orch, author).await
        };
        assert_eq!(published.fields()["author"], author, "stored as written");
        assert_eq!(server.calls(), 1);

        let citations = format!(
            "{}, {}",
            citation("devops", "0.48", "native"),
            citation("docs", "0.47", "native")
        );
        assert_eq!(
            thread_messages(&orch).await,
            [(
                "docs".to_string(),
                SENDER.to_string(),
                delivered(&orch, &citations, &published).await
            )],
            "{author}: only the thread that did not write the post hears it"
        );
        let journal: Vec<_> = firings(&orch)
            .await
            .into_iter()
            .map(|firing| (firing.status, firing.drop_reason, firing.sink_ref))
            .collect();
        assert_eq!(
            journal,
            [
                (
                    "dropped".to_string(),
                    Some("actor".to_string()),
                    Some(build_thread_uri(PROJECT, "devops"))
                ),
                (
                    "fired".to_string(),
                    None,
                    Some(build_thread_uri(PROJECT, "docs"))
                ),
            ],
            "{author}"
        );
        let mut posts = vec![published.post.clone()];
        crate::posts::attach_routing(&orch, &mut posts).await;
        assert_eq!(
            posts[0].routing[0].citations,
            [citation("docs", "0.47", "native")],
            "the attribution cites only the delivered pick"
        );

        let definition = crate::routes::parse_definition(
            &shipped_route(),
            &crate::routes::FactRegistry::default(),
        )
        .unwrap();
        let hydrated = crate::routes::log::hydrate(&orch, &published.row)
            .await
            .unwrap();
        let preview = crate::routes::test_definition(
            &orch,
            &definition,
            hydrated.fact,
            hydrated.presence,
            crate::routes::RouteContext {
                project_id: Some("p"),
                project_path: None,
                event: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(
            preview.sink_previews[0]["targets"],
            json!([
                {"address": build_thread_uri(PROJECT, "devops"), "dropped": "actor",
                 "error": format!("{} wrote this fact, and a route never delivers a fact to its own actor", build_thread_uri(PROJECT, "devops"))},
                {"address": build_thread_uri(PROJECT, "docs")},
            ]),
            "{author}: the Playground shows the drop as a drop"
        );
    }
}

/// On an install with no decide offer the shipped route still loads, the
/// editor shows the blocking diagnostic, a firing journals `unanswerable`,
/// and the feed still carries the post.
#[tokio::test(flavor = "multi_thread")]
async fn with_no_decide_offer_the_route_is_incomplete_and_the_feed_still_carries_the_post() {
    let orch = crate::orchestrator::models::tests::bare_orchestrator().await;
    install(&orch, true).await;
    let definition =
        crate::routes::parse_definition(&shipped_route(), &crate::routes::FactRegistry::default())
            .unwrap();
    let diagnostics =
        crate::routes::decision::route_decision_diagnostics(&orch, &definition, Some("p")).await;
    assert!(
        diagnostics.iter().any(|diagnostic| diagnostic.blocking),
        "{diagnostics:?}"
    );
    assert!(
        !shipped_route().contains("models"),
        "the workspace default applies"
    );

    let published = publish(&orch, Some("p"), UNADDRESSED, human()).await;
    assert_nothing_delivered(&orch).await;
    let (outcome, _) = one_unrouted(&orch, &published).await;
    assert_eq!(outcome, "unanswerable");
    assert!(
        woke(&published.row)
            .iter()
            .any(|(job, _)| *job == session("reader")),
        "{:?}",
        woke(&published.row)
    );
}
