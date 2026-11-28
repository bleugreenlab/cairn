//! The chain: an accepted act whose bound declaration names `then` yields,
//! once its verified `Done` report lands a commit, one request of that kind
//! for exactly that commit, and the acceptance for it.
//!
//! Storage writes the chain row inside the parent's report transaction and
//! wakes this processor through the outbox. The processor re-reads the row on
//! every pass, so a replay after a crash resumes where the last one stopped:
//! it files the chained request once, then settles the chain or writes the
//! chained acceptance. It never creates authority of its own: the acceptance
//! it writes names its parent, which storage re-verifies on every checked
//! read, and the child then takes the ordinary dispatch path with its own
//! live rechecks.
use std::sync::Arc;

use cairn_common::authorization::ActBinding;
use cairn_common::requests::*;
use cairn_db::storage::requests as store;
use serde_json::json;

use super::registry::{self, ActorFacts, RequestConsumer};
use super::{consumer, now, refresh_facts_for};
use crate::orchestrator::Orchestrator;

/// What a chain past its window says. The chained request stays open, so the
/// operator accepts it as an ordinary card.
pub(crate) const WINDOW_CLOSED: &str =
    "the acceptance's chain window closed; accept the rebuild card";

/// Why a pass did not write the chained acceptance.
enum Stop {
    /// The chain cannot be accepted as it stands: it settles refused.
    Refuse(String),
    /// A transient store refusal: the outbox retries, bounded by the window.
    Retry(String),
}

/// One pass over a chain. `Ok` acknowledges the wake: the chain is settled,
/// accepted, or waiting on an acting attempt whose settlement, or the close of
/// the chain's window, wakes it again.
pub(crate) async fn process(orch: &Orchestrator, parent_attempt_id: &str) -> Result<(), String> {
    let db = &orch.db.local;
    let chain = store::get_chain(db, parent_attempt_id)
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("attempt {parent_attempt_id} has no chain"))?;
    if chain.state != store::ChainState::Pending {
        return Ok(());
    }
    if chain.kind != RequestKind::Rebuild {
        return refuse(
            orch,
            &chain,
            format!("a chain into {} is not supported", chain.kind.as_str()),
        )
        .await;
    }
    // Filed before anything that can refuse the chain: the child card, which
    // requires the landed commit, is the operator's retry for every refusal.
    let child = match &chain.child_request_id {
        Some(child) => child.clone(),
        None => file_child(orch, &chain).await?,
    };
    let adapter = match consumer(orch, chain.kind).await {
        Ok(adapter) => adapter,
        Err(error) => return refuse(orch, &chain, error).await,
    };
    // An attempt that was already acting may have built the commit itself.
    let done = store::done_attempts_since(db, chain.kind, chain.created_at)
        .await
        .map_err(|error| error.to_string())?;
    for (attempt_id, built) in done {
        if adapter
            .contains_commits(orch, &built, std::slice::from_ref(&chain.commit))
            .await
            .is_ok()
        {
            store::settle_chain(
                db,
                &chain.parent_attempt_id,
                store::ChainSettlement::Satisfied { attempt_id },
                now(),
            )
            .await
            .map_err(|error| error.to_string())?;
            return Ok(());
        }
    }
    if chrono::Utc::now().timestamp_millis() >= chain.deadline_unix_ms {
        return refuse(orch, &chain, WINDOW_CLOSED.into()).await;
    }
    if store::kind_has_unresolved_attempt(db, chain.kind)
        .await
        .map_err(|error| error.to_string())?
    {
        // Storage wakes every pending chain of this kind when that attempt
        // settles, and the outbox sweep wakes it once its window closes, so
        // the chain waits without holding the outbox.
        return Ok(());
    }
    match accept_child(orch, &chain, &child, adapter).await {
        Ok(()) => Ok(()),
        Err(Stop::Refuse(reason)) => refuse(orch, &chain, reason).await,
        Err(Stop::Retry(error)) => Err(error),
    }
}

async fn refuse(
    orch: &Orchestrator,
    chain: &store::RequestChain,
    reason: String,
) -> Result<(), String> {
    store::settle_chain(
        &orch.db.local,
        &chain.parent_attempt_id,
        store::ChainSettlement::Refused(super::scrub_text(reason)),
        now(),
    )
    .await
    .map_err(|error| error.to_string())?;
    Ok(())
}

/// The chained rebuild: this install's rebuild cohort, joined or opened, with
/// a contribution storage attributes to the parent request.
async fn file_child(orch: &Orchestrator, chain: &store::RequestChain) -> Result<String, String> {
    let install = orch.anon_device_manager.device_id();
    let parent = store::get_attempt(&orch.db.local, &chain.parent_attempt_id)
        .await
        .map_err(|error| error.to_string())?
        .ok_or("the chain's parent attempt is missing")?;
    let label = parent
        .acceptance
        .fenced_command
        .as_ref()
        .and_then(|fenced| fenced.act.declaration.as_ref())
        .map(|declared| declared.label.clone())
        .unwrap_or_else(|| parent.acceptance.kind.as_str().to_string());
    let input = store::NormalizedRequest {
        workspace_id: crate::authorization::WORKSPACE_ID.into(),
        kind: chain.kind,
        target: None,
        target_identity: install.clone(),
        coalescing_key: Some(format!("rebuild:{install}")),
        contribution: RequestContribution {
            id: String::new(),
            request_id: String::new(),
            origin_uri: String::new(),
            run_id: String::new(),
            principal: cairn_common::identity::PrincipalRef::Machine { device_id: install },
            wake_destination: String::new(),
            rationale: format!(
                "{label} landed {}; its acceptance chains into a rebuild of exactly that commit",
                chain.commit
            ),
            waiting: vec![],
            requirements: RequestRequirements::default(),
            minimum_target: None,
            normalized_target: json!({}),
            active: true,
            satisfied_by_attempt: None,
            created_at: 0,
        },
    };
    store::file_chained_request(&orch.db.local, &chain.parent_attempt_id, &input, now())
        .await
        .map_err(|error| error.to_string())
}

async fn accept_child(
    orch: &Orchestrator,
    chain: &store::RequestChain,
    child: &str,
    adapter: Arc<dyn RequestConsumer>,
) -> Result<(), Stop> {
    let db = &orch.db.local;
    let read = |error: cairn_db::storage::DbError| Stop::Retry(error.to_string());
    let parent = store::get_attempt(db, &chain.parent_attempt_id)
        .await
        .map_err(read)?
        .ok_or_else(|| Stop::Refuse("the chain's parent attempt is missing".into()))?;
    let parent = parent.acceptance;
    if !parent
        .kind
        .acceptance_policy()
        .covers(chain.kind.acceptance_policy())
    {
        return Err(Stop::Refuse(format!(
            "{} requires a stronger acceptance than its parent carried",
            chain.kind.as_str()
        )));
    }
    let request = store::get_request(db, child)
        .await
        .map_err(read)?
        .ok_or_else(|| Stop::Refuse("the chained request is missing".into()))?;
    if request.state != RequestState::Open {
        return Err(Stop::Refuse(format!(
            "the chained request is {}, not open",
            request.state.as_str()
        )));
    }
    refresh_facts_for(orch, &request, adapter.as_ref())
        .await
        .map_err(Stop::Refuse)?;
    let request = store::get_request(db, child)
        .await
        .map_err(read)?
        .ok_or_else(|| Stop::Refuse("the chained request is missing".into()))?;
    let shown = request
        .facts
        .as_ref()
        .ok_or_else(|| Stop::Refuse("the chained request has no actor facts".into()))?;
    let facts: ActorFacts = serde_json::from_value(shown.document.clone())
        .map_err(|error| Stop::Refuse(format!("actor facts are unreadable: {error}")))?;
    // The landed commit was the default branch's tip when it landed, so it
    // normally contains every other ask in the cohort. One it does not cover
    // leaves the card for the operator.
    let required: Vec<String> = request
        .requesters
        .iter()
        .filter(|entry| {
            entry.active
                && entry.satisfied_by_attempt.is_none()
                && request.member_request_ids.contains(&entry.request_id)
        })
        .flat_map(|entry| entry.requirements.commits.iter().cloned())
        .collect();
    adapter
        .contains_commits(orch, &chain.commit, &required)
        .await
        .map_err(|error| {
            Stop::Refuse(format!(
                "the rebuild card also asks for a commit the landed {} does not contain ({error}); accept the rebuild card",
                chain.commit
            ))
        })?;
    let mut ids = request.member_request_ids.clone();
    ids.sort();
    ids.dedup();
    let definition = registry::definition(chain.kind.as_str()).map_err(Stop::Refuse)?;
    let envelope = RequestAcceptance {
        schema_version: 1,
        install_id: orch.anon_device_manager.device_id(),
        workspace_id: request.workspace_id.clone(),
        cohort_id: request.cohort_id.clone(),
        request_ids: ids,
        attempt_id: uuid::Uuid::new_v4().to_string(),
        actor_id: definition.actor.into(),
        kind: chain.kind,
        target: request.target.clone(),
        target_identity: request.target_identity.clone(),
        binding: ActBinding::fresh(&registry::act_digest(
            chain.kind,
            request.target.as_deref(),
            None,
        )),
        facts_version: shown.version,
        revision: request.revision,
        candidate_commit: Some(chain.commit.clone()),
        base_commit: facts.base_commit.clone(),
        policy_project_id: facts.policy_project_id.clone(),
        drain: None,
        fenced_command: None,
        chained_from: Some(ChainedFrom {
            parent_attempt_id: parent.attempt_id.clone(),
            parent_authorization_event_id: chain_parent_event(orch, &chain.parent_attempt_id)
                .await?,
            parent_binding_digest: parent.binding.digest.clone(),
        }),
        operator: parent.operator.clone(),
        appearance: parent.appearance.clone(),
        accepted_at: now(),
    };
    adapter
        .validate_snapshot(orch, &envelope)
        .await
        .map_err(Stop::Refuse)?;
    store::accept_chained_request(db, &envelope)
        .await
        .map_err(read)?;
    Ok(())
}

async fn chain_parent_event(orch: &Orchestrator, parent: &str) -> Result<String, Stop> {
    store::get_attempt(&orch.db.local, parent)
        .await
        .map_err(|error| Stop::Retry(error.to_string()))?
        .map(|attempt| attempt.authorization_event_id)
        .ok_or_else(|| Stop::Refuse("the chain's parent attempt is missing".into()))
}
