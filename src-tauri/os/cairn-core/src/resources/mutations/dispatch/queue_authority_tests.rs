//! The two grantable queue acts end to end below the card (cairn/5942): a
//! thread's cross-thread move on `cairn://p/<project>/queue` and its
//! `{start_now:true}` on a held execution.
//!
//! [`attempt`] runs what the write handler runs for one change: preparation
//! names the boundary, the gate asks whether a grant covers it (the handler
//! raises the operator's card exactly when it does not, which is where an
//! attempt stops), and dispatch re-derives the request, authorizes it
//! (consuming a once grant) and acts. The card itself, answered over the
//! desktop credential, is pinned in the transport tests
//! (`a_desktop_answered_standing_card_lets_the_threads_move_land`). Grants
//! here carry exactly what a card's answer mints
//! (`AuthorityConstraintSet::for_approval`), never a hand-built constraint set.

use cairn_common::authorization::{
    AuthorityConstraintSet, AuthorityLifetimeKind, AuthorityProvenance, AuthorityReason,
    AuthorityRequest,
};
use serde_json::{json, Value};

use crate::authorization::prepare::{prepare_batch, Prepared};
use crate::execution::gates::capacity::tests::is_held;
use crate::execution::gates::capacity::FLEET_SLOT_FACT as FLEET;
use crate::execution::gates::reorder::tests::{
    hold, ids, operator_move, placed_by, request, rows, thread_run, view_ids, world, write, World,
};
use crate::execution::gates::reorder::{move_held_execution, QueueMover};
use crate::execution::gates::sweep::tests::count;
use crate::mcp::types::{ChangeItem, ChangeMode};
use crate::models::{Pin, PinBy};

const QUEUE: &str = "cairn://p/prj/queue";
const ALPHA: &str = "run-alpha";
const BETA: &str = "run-beta";

#[derive(Debug)]
enum Outcome {
    /// No grant covers it: the write handler raises this card and moves nothing.
    Card(Box<AuthorityRequest>),
    Done(String),
    Refused(String),
}

impl Outcome {
    fn card(self) -> AuthorityRequest {
        match self {
            Outcome::Card(request) => *request,
            other => panic!("expected a card, got {other:?}"),
        }
    }

    fn done(self) -> String {
        match self {
            Outcome::Done(summary) => summary,
            other => panic!("expected it to land, got {other:?}"),
        }
    }

    fn refused(self) -> String {
        match self {
            Outcome::Refused(message) => message,
            other => panic!("expected a refusal, got {other:?}"),
        }
    }
}

/// A world with alpha's and beta's sessions live: fleet order a1, b1, a2, a3.
/// A move into the pinned prefix spends a turn of the moved row's lane
/// (cairn/5943), so behind alpha's moved rows beta's b1 comes next.
async fn queue_world(name: &str, thread_capped: bool) -> World {
    let w = world(name, thread_capped).await;
    thread_run(&w.env, ALPHA, Some("ta"), None).await;
    thread_run(&w.env, BETA, Some("tb"), None).await;
    w
}

async fn attempt(w: &World, run: Option<&str>, target: &str, payload: Value) -> Outcome {
    let item = ChangeItem {
        target: target.to_string(),
        mode: ChangeMode::Patch,
        payload: Some(payload),
    };
    let req = request(run);
    for (_, outcome) in prepare_batch(&w.env.orch, &req, std::slice::from_ref(&item)).await {
        match outcome {
            Prepared::Refused(message) | Prepared::Invalid(message) => {
                return Outcome::Refused(message)
            }
            Prepared::Authority(authority) => {
                let actor = crate::authorization::resolve_actor(&w.env.orch, &req)
                    .await
                    .expect("a run resolves");
                let decision = crate::authorization::gate(&actor, &authority)
                    .await
                    .unwrap();
                if !decision.is_allowed() {
                    return Outcome::Card(Box::new(authority));
                }
            }
        }
    }
    match super::super::dispatch_resource_change(&w.env.orch, &req, 0, &item, false).await {
        Ok(change) => Outcome::Done(change.summary),
        Err(failure) => Outcome::Refused(failure.error),
    }
}

async fn move_to(w: &World, run: &str, number: i64, position: usize) -> Outcome {
    attempt(
        w,
        Some(run),
        QUEUE,
        json!({"move": {"issue": number, "position": position}}),
    )
    .await
}

async fn seq(w: &World, execution_id: &str) -> i64 {
    count(
        &w.env,
        &format!("SELECT seq FROM executions WHERE id = '{execution_id}'"),
    )
    .await
}

async fn start_now(w: &World, run: &str, number: i64, execution_id: &str) -> Outcome {
    let seq = seq(w, execution_id).await;
    attempt(
        w,
        Some(run),
        &format!("cairn://p/prj/{number}/executions/{seq}"),
        json!({"start_now": true}),
    )
    .await
}

/// The operator answers `card` for `run` with `lifetime`: the grant a card's
/// answer mints, constraints included.
async fn grant(
    w: &World,
    run: &str,
    card: &AuthorityRequest,
    lifetime: AuthorityLifetimeKind,
) -> String {
    let actor = crate::authorization::resolve_actor(&w.env.orch, &request(Some(run)))
        .await
        .unwrap();
    crate::authorization::issue_grant(
        &actor.db,
        crate::authorization::GrantIssue {
            request: card.clone(),
            principal: actor.principal.clone(),
            audience: actor.audience.clone(),
            lifetime,
            request_id: Some(format!("perm-{}", uuid::Uuid::new_v4())),
            turn_id: None,
            session_id: None,
            expires_at: None,
            provenance: AuthorityProvenance {
                issuer: "operator_prompt".to_string(),
                ..Default::default()
            },
            constraints: AuthorityConstraintSet::for_approval(
                card,
                card.bind_act().as_ref(),
                lifetime,
            )
            .unwrap(),
        },
    )
    .await
    .unwrap()
    .id
}

/// `run` patches `cairn://grants/<id> {revoke:true}`: a thread revokes only a
/// grant it holds (`authorization::revoke`).
async fn revoke_as(w: &World, run: &str, grant_id: &str) -> Result<String, String> {
    write(
        &w.env,
        Some(run),
        &format!("cairn://grants/{grant_id}"),
        ChangeMode::Patch,
        json!({"revoke": true}),
    )
    .await
}

fn thread_pin(thread: &str) -> Pin {
    Pin::placed(PinBy::Thread {
        thread: thread.to_string(),
    })
}

// -- the move ----------------------------------------------------------------

#[tokio::test]
async fn a_thread_without_a_grant_raises_a_card_and_moves_nothing() {
    let w = queue_world("qa-no-grant.db", false).await;
    let before = rows(&w.env).await;

    let card = move_to(&w, ALPHA, 203, 1).await.card();
    assert_eq!(card.scope.shorthand(), "workspace/default/queue:write");
    assert_eq!(card.facts.acting_thread.as_deref(), Some("ta"));
    assert!(
        card.summary
            .starts_with("move prj/203 'Issue 203' from fleet 4th to 1st, ahead of work from beta"),
        "{}",
        card.summary
    );
    assert!(card.summary.ends_with("(alpha asks)"), "{}", card.summary);
    assert_eq!(
        crate::authorization::policy::classify(&card.scope, false).reason(),
        Some(AuthorityReason::QueueReorderAcrossThreads)
    );
    assert_eq!(rows(&w.env).await, before, "nothing moved");

    // The real handler suspends on that card; a dispatch that skipped the
    // card would still be refused at authorization.
    let item = ChangeItem {
        target: QUEUE.to_string(),
        mode: ChangeMode::Patch,
        payload: Some(json!({"move": {"issue": 203, "position": 1}})),
    };
    let refused =
        super::super::dispatch_resource_change(&w.env.orch, &request(Some(ALPHA)), 0, &item, false)
            .await
            .unwrap_err();
    assert!(
        refused.error.contains("requires operator approval"),
        "{}",
        refused.error
    );
    assert_eq!(rows(&w.env).await, before);
}

#[tokio::test]
async fn a_once_grant_lets_exactly_that_move_through() {
    let w = queue_world("qa-once.db", false).await;
    let card = move_to(&w, ALPHA, 203, 1).await.card();
    grant(&w, ALPHA, &card, AuthorityLifetimeKind::Once).await;

    // Bound to the act the card showed: a different move asks, and does not
    // spend the approval.
    let other = move_to(&w, ALPHA, 202, 2).await.card();
    assert_ne!(other.facts.act, card.facts.act);
    assert_eq!(view_ids(&w.env).await, ids(&[&w.a1, &w.b1, &w.a2, &w.a3]));

    let landed = move_to(&w, ALPHA, 203, 1).await.done();
    assert!(
        landed.contains("prj/203 to fleet 1st (was 4th)"),
        "{landed}"
    );
    assert_eq!(view_ids(&w.env).await, ids(&[&w.a3, &w.b1, &w.a1, &w.a2]));
    let after = rows(&w.env).await;
    assert_eq!(hold(&after, &w.a3, FLEET).pin(), Some(&thread_pin("alpha")));
    assert_eq!(placed_by(&after, &w.a3, FLEET).as_deref(), Some("alpha"));

    // Spent: the same move asks again.
    let again = move_to(&w, ALPHA, 203, 1).await.card();
    assert_eq!(again.facts.act, card.facts.act);
}

#[tokio::test]
async fn a_standing_grant_to_a_thread_covers_only_that_thread() {
    let w = queue_world("qa-standing.db", false).await;
    let card = move_to(&w, ALPHA, 203, 1).await.card();
    grant(&w, ALPHA, &card, AuthorityLifetimeKind::Standing).await;

    move_to(&w, ALPHA, 203, 1).await.done();
    move_to(&w, ALPHA, 202, 2).await.done();
    assert_eq!(view_ids(&w.env).await, ids(&[&w.a3, &w.a2, &w.b1, &w.a1]));

    let other = move_to(&w, BETA, 301, 1).await.card();
    assert_eq!(other.facts.acting_thread.as_deref(), Some("tb"));
    assert!(other.summary.ends_with("(beta asks)"), "{}", other.summary);
    assert_eq!(view_ids(&w.env).await, ids(&[&w.a3, &w.a2, &w.b1, &w.a1]));
}

#[tokio::test]
async fn revoking_the_grant_restores_the_card() {
    let w = queue_world("qa-revoke.db", false).await;
    let card = move_to(&w, ALPHA, 203, 1).await.card();
    let id = grant(&w, ALPHA, &card, AuthorityLifetimeKind::Standing).await;
    move_to(&w, ALPHA, 203, 1).await.done();

    // The grant is alpha's: beta may not take it away, and is told whose it is.
    let refusal = revoke_as(&w, BETA, &id).await.unwrap_err();
    assert!(refusal.contains("held by thread ta"), "{refusal}");

    let revoked = revoke_as(&w, ALPHA, &id).await.unwrap();
    assert!(revoked.starts_with("Revoked"), "{revoked}");
    move_to(&w, ALPHA, 202, 1).await.card();
    assert_eq!(view_ids(&w.env).await, ids(&[&w.a3, &w.b1, &w.a1, &w.a2]));
}

#[tokio::test]
async fn an_agent_move_that_would_displace_an_operator_pin_is_refused_naming_it() {
    let w = queue_world("qa-displace.db", false).await;
    operator_move(&w.env.orch, &w.b1, 1).await.unwrap();
    let before = rows(&w.env).await;

    // At preparation: no card is raised for a move that would be refused.
    let refused = move_to(&w, ALPHA, 203, 1).await.refused();
    assert!(
        refused.contains("prj/301 (fleet 1st) was placed by the operator"),
        "{refused}"
    );
    assert!(refused.contains("Nothing moved"), "{refused}");
    assert_eq!(rows(&w.env).await, before);
    // Behind it is fine, and asks.
    move_to(&w, ALPHA, 203, 2).await.card();

    // Under the lock: the operator pins between the card and the move.
    let w = queue_world("qa-displace-late.db", false).await;
    let card = move_to(&w, ALPHA, 203, 1).await.card();
    grant(&w, ALPHA, &card, AuthorityLifetimeKind::Standing).await;
    operator_move(&w.env.orch, &w.b1, 1).await.unwrap();
    let before = rows(&w.env).await;
    let refused = move_held_execution(
        &w.env.orch,
        &[(w.a3.clone(), 1)],
        QueueMover::Thread {
            name: "alpha".to_string(),
        },
    )
    .await
    .unwrap_err();
    assert!(
        refused.contains("prj/301 (fleet 1st) was placed by the operator"),
        "{refused}"
    );
    assert_eq!(
        rows(&w.env).await,
        before,
        "refused under the lock, nothing written"
    );
}

#[tokio::test]
async fn a_run_acting_for_no_thread_and_a_runless_caller_are_refused() {
    let w = queue_world("qa-who.db", false).await;
    thread_run(&w.env, "run-loose", None, None).await;
    let before = rows(&w.env).await;
    let handle = |run: Option<&str>| {
        let mut req = request(run);
        req.payload = json!({"changes": [{
            "target": QUEUE, "mode": "patch",
            "payload": {"move": {"issue": 203, "position": 1}}
        }]});
        let orch = w.env.orch.clone();
        async move { crate::mcp::handlers::write::handle_write(&orch, &req).await }
    };

    let loose = handle(Some("run-loose")).await;
    assert!(loose.contains("this run acts for no thread"), "{loose}");
    let runless = handle(None).await;
    assert!(
        runless.contains("only a run acting for a thread can ask the operator"),
        "{runless}"
    );
    assert!(runless.contains("Settings → Queue"), "{runless}");
    assert_eq!(rows(&w.env).await, before);
}

/// A project service acting for alpha does not ask as alpha: even with alpha's
/// standing grants in place, its move and its start are refused at
/// preparation, before any card.
#[tokio::test]
async fn a_service_acting_for_a_thread_is_refused_even_with_its_grant() {
    use crate::project_services::authority::{DelegatedScope, ServiceRun, ServiceRunRole};
    let w = queue_world("qa-service.db", true).await;
    let move_card = move_to(&w, ALPHA, 203, 1).await.card();
    grant(&w, ALPHA, &move_card, AuthorityLifetimeKind::Standing).await;
    let start_card = start_now(&w, ALPHA, 203, &w.a3).await.card();
    grant(&w, ALPHA, &start_card, AuthorityLifetimeKind::Standing).await;

    thread_run(&w.env, "run-service", Some("ta"), None).await;
    w.env.orch.project_services.runs.insert(
        "run-service",
        ServiceRun {
            project_id: "p".into(),
            project_key: "prj".into(),
            owner: crate::project_services::ServiceOwner::Project,
            service: "sorter".into(),
            generation: 1,
            default_branch: "main".into(),
            role: ServiceRunRole::Delegated {
                caller_kind: crate::project_services::protocol::CallerKind::Agent,
                acting_for: "cairn://p/prj/alpha".into(),
                scope: DelegatedScope::ProjectAgent {
                    thread: Some("alpha".into()),
                },
            },
        },
    );
    let before = rows(&w.env).await;
    let moved = move_to(&w, "run-service", 203, 1).await.refused();
    assert!(moved.contains("a project service may not ask"), "{moved}");
    let started = start_now(&w, "run-service", 203, &w.a3).await.refused();
    assert!(
        started.contains("a project service may not ask"),
        "{started}"
    );
    assert_eq!(rows(&w.env).await, before);
    assert!(is_held(&w.env, 203).await);
}

/// The asking thread is the run's job's, never anything the request says: a
/// request naming beta's thread from alpha's run still asks as alpha.
#[tokio::test]
async fn the_asking_thread_comes_from_the_job_not_the_request() {
    let w = queue_world("qa-claim.db", false).await;
    let mut req = request(Some(ALPHA));
    req.thread_id = Some("tb".to_string());
    let item = ChangeItem {
        target: QUEUE.to_string(),
        mode: ChangeMode::Patch,
        payload: Some(json!({"move": {"issue": 203, "position": 1}})),
    };
    let prepared = prepare_batch(&w.env.orch, &req, std::slice::from_ref(&item)).await;
    let Some((_, Prepared::Authority(card))) = prepared.into_iter().next() else {
        panic!("expected a card");
    };
    assert_eq!(card.facts.acting_thread.as_deref(), Some("ta"));
    assert!(card.summary.ends_with("(alpha asks)"), "{}", card.summary);

    // A grant minted from that card binds alpha, and beta's real run does not
    // inherit it.
    grant(&w, ALPHA, &card, AuthorityLifetimeKind::Standing).await;
    let constraints = AuthorityConstraintSet::for_approval(
        &card,
        card.bind_act().as_ref(),
        AuthorityLifetimeKind::Standing,
    )
    .unwrap();
    assert!(constraints.constraints.contains(
        &cairn_common::authorization::AuthorityConstraint::ActingThread {
            thread_id: "ta".to_string()
        }
    ));
    move_to(&w, BETA, 301, 1).await.card();
}

#[tokio::test]
async fn the_order_form_places_the_listed_rows_first_under_one_card() {
    let w = queue_world("qa-order.db", false).await;
    let order = json!({"order": [203, 202]});
    let card = attempt(&w, Some(ALPHA), QUEUE, order.clone()).await.card();
    assert!(
        card.summary.contains("move prj/203") && card.summary.contains("; move prj/202"),
        "{}",
        card.summary
    );
    grant(&w, ALPHA, &card, AuthorityLifetimeKind::Once).await;

    attempt(&w, Some(ALPHA), QUEUE, order).await.done();
    assert_eq!(view_ids(&w.env).await, ids(&[&w.a3, &w.a2, &w.b1, &w.a1]));
    let after = rows(&w.env).await;
    for id in [&w.a3, &w.a2] {
        assert_eq!(hold(&after, id, FLEET).pin(), Some(&thread_pin("alpha")));
    }
    assert_eq!(
        w.env
            .orch
            .admission_lock
            .acquisitions(crate::execution::gates::capacity::AdmissionSite::QueueMove),
        1,
        "both moves under one acquisition"
    );
}

// -- Start now ---------------------------------------------------------------

#[tokio::test]
async fn start_now_without_a_grant_raises_a_card_naming_the_caps_and_starts_nothing() {
    let w = queue_world("qa-start-card.db", true).await;
    let card = start_now(&w, ALPHA, 203, &w.a3).await.card();
    assert_eq!(card.scope.shorthand(), "workspace/default/queue:run");
    assert_eq!(card.facts.acting_thread.as_deref(), Some("ta"));
    for part in [
        "start execution 1 on cairn://p/prj/203 ('Issue 203') now",
        "the fleet cap (",
        "running)",
        "focus cap (1 of 1 children open)",
        "A standing grant lets alpha start its held work past every cap whenever it asks.",
    ] {
        assert!(card.summary.contains(part), "{part}: {}", card.summary);
    }
    assert!(is_held(&w.env, 203).await, "nothing started");
}

#[tokio::test]
async fn a_once_start_now_grant_starts_exactly_one_execution() {
    let w = queue_world("qa-start-once.db", true).await;
    let card = start_now(&w, ALPHA, 203, &w.a3).await.card();
    grant(&w, ALPHA, &card, AuthorityLifetimeKind::Once).await;

    // Bound to the execution the card named: another one asks.
    let other = start_now(&w, ALPHA, 202, &w.a2).await.card();
    assert_ne!(other.facts.act, card.facts.act);
    assert!(is_held(&w.env, 202).await);

    let started = start_now(&w, ALPHA, 203, &w.a3).await.done();
    assert!(started.contains("under the operator's grant"), "{started}");
    assert!(!is_held(&w.env, 203).await);
    start_now(&w, ALPHA, 202, &w.a2).await.card();
    assert!(is_held(&w.env, 202).await);
}

#[tokio::test]
async fn a_standing_start_now_grant_covers_only_its_thread() {
    let w = queue_world("qa-start-standing.db", true).await;
    let card = start_now(&w, ALPHA, 203, &w.a3).await.card();
    grant(&w, ALPHA, &card, AuthorityLifetimeKind::Standing).await;

    start_now(&w, ALPHA, 203, &w.a3).await.done();
    start_now(&w, ALPHA, 202, &w.a2).await.done();
    start_now(&w, BETA, 301, &w.b1).await.card();
    assert!(is_held(&w.env, 301).await);
}

/// A standing Start now grant is the thread's own work only: the card says
/// "its held work", and starting another thread's is refused before any card.
#[tokio::test]
async fn a_thread_starts_only_its_own_held_work() {
    let w = queue_world("qa-start-own.db", true).await;
    let refused = start_now(&w, ALPHA, 301, &w.b1).await.refused();
    assert!(
        refused.contains("is not alpha's work") && refused.contains("Nothing started"),
        "{refused}"
    );
    let card = start_now(&w, ALPHA, 203, &w.a3).await.card();
    grant(&w, ALPHA, &card, AuthorityLifetimeKind::Standing).await;
    let refused = start_now(&w, ALPHA, 301, &w.b1).await.refused();
    assert!(refused.contains("is not alpha's work"), "{refused}");
    assert!(is_held(&w.env, 301).await, "beta's work is not started");
}

#[tokio::test]
async fn revoking_a_start_now_grant_restores_the_card() {
    let w = queue_world("qa-start-revoke.db", true).await;
    let card = start_now(&w, ALPHA, 203, &w.a3).await.card();
    let id = grant(&w, ALPHA, &card, AuthorityLifetimeKind::Standing).await;
    // The operator's door, Settings → Grants, revokes any grant.
    crate::authorization::revoke::revoke_as_operator(&w.env.orch.db.local, &id, "the operator")
        .await
        .unwrap();
    start_now(&w, ALPHA, 203, &w.a3).await.card();
    assert!(is_held(&w.env, 203).await);
}

#[tokio::test]
async fn a_move_grant_is_not_a_start_now_grant() {
    let w = queue_world("qa-kinds.db", true).await;
    let card = move_to(&w, ALPHA, 203, 1).await.card();
    grant(&w, ALPHA, &card, AuthorityLifetimeKind::Standing).await;
    start_now(&w, ALPHA, 203, &w.a3).await.card();
}

#[tokio::test]
async fn a_granted_start_records_an_override_naming_the_thread() {
    let w = queue_world("qa-start-record.db", true).await;
    let card = start_now(&w, ALPHA, 203, &w.a3).await.card();
    grant(&w, ALPHA, &card, AuthorityLifetimeKind::Once).await;
    let started = start_now(&w, ALPHA, 203, &w.a3).await.done();

    let record: Value = serde_json::from_str(
        &w.env
            .db
            .query_opt_text(
                "SELECT release_override_json FROM executions WHERE id = ?1",
                (w.a3.clone(),),
            )
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(record["granted"]["thread"], "alpha");
    assert!(record["granted"]["decision"].is_string(), "{record}");
    assert_eq!(record["operator"], false);
    let skipped: Vec<&str> = record["skipped"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(Value::as_str)
        .collect();
    assert_eq!(skipped.len(), 2, "{record}");

    for line in [started, census_summary(&w, &w.a3).await] {
        assert!(
            line.contains("started now by alpha under the operator's grant ("),
            "{line}"
        );
        assert!(
            line.contains("skipping the fleet and thread caps"),
            "{line}"
        );
    }
}

async fn census_summary(w: &World, execution_id: &str) -> String {
    let ids = vec![execution_id.to_string()];
    w.env
        .db
        .read(move |conn| {
            let ids = ids.clone();
            Box::pin(async move {
                crate::execution::gates::census::census_for_executions_conn(conn, &ids).await
            })
        })
        .await
        .unwrap()
        .remove(execution_id)
        .unwrap()
        .summary
}
