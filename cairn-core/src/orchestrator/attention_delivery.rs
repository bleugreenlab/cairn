//! Attention delivery engine (CAIRN-1647).
//!
//! Bridges the attention [`ledger`](super::attention_ledger) to the existing job
//! resume machinery. Three responsibilities:
//!
//! 1. **Open/bump/resolve items from facts.** [`open_and_schedule_for_fact`] is
//!    called from `emit_attention_event`, the single typed funnel every
//!    issue-attention fact already flows through. Each fact maps to a ledger
//!    item; an open or bump schedules a coalesced evaluation for every watcher
//!    subscribed to the issue.
//! 2. **Decide whether a watcher should resume.** [`has_deliverable_briefing`]
//!    is consulted by `collect_pending_for_flush`: a pending evaluation whose
//!    current deliverable set is non-empty is a reason to resume, exactly like a
//!    pending direct message. An empty set is dropped (the stale-wake fix).
//! 3. **Render the briefing at resume time.** [`claim_and_render_briefing`] is
//!    called inside `continue_job_impl`'s prompt assembly. It drains the pending
//!    evaluation, renders the deliverable items from their CURRENT content, and
//!    stamps the watcher's seen cursor — so a bump after delivery re-surfaces the
//!    item and an unchanged state never re-wakes.
//!
//! This replaces the frozen `[Child update] … Read X.` system-direct path: there
//! is no message frozen at emit time, so nothing can deliver stale or lift a
//! mute placed after the fact was handled.

use turso::params;

use super::attention_ledger::{
    self as ledger, ItemIdentity, ItemKind, ResolvedBy, SOURCE_KIND_ISSUE,
};
use super::wakes::{WakeSubscription, WakeSubscriptionState};
use super::Orchestrator;
use crate::messages::queued::DeliveryUrgency;
use crate::orchestrator::{AttentionEvent, AttentionFact};
use crate::storage::{run_db_blocking, LocalDb, RowExt};

/// Stable key for the single per-issue review item (PR / gated artifact). One
/// item collapses today's triple fan-out (webhook PR change + builder idle +
/// pr-node idle) into one briefing line.
const REVIEW_KEY: &str = "review";
/// Stable key for the single per-issue terminal-resolution event item.
const RESOLVED_KEY: &str = "resolved";
/// Stable key for the single per-child-issue request-response message item. One
/// item per child collapses a burst of messages (sent while the parent sleeps)
/// into one chat-windowed briefing line.
const MESSAGE_KEY: &str = "message";

/// Map an `AttentionFact` to a ledger mutation, then schedule watcher
/// evaluations if it opened or bumped an item. Fire-and-forget: errors are
/// logged, never propagated (this rides the hot emit path).
pub fn open_and_schedule_for_fact(orch: &Orchestrator, event: &AttentionEvent) {
    let db = &orch.db.local;
    let issue_uri = event.issue_uri.clone();
    let issue_id = Some(event.issue_id.clone());

    match &event.fact {
        // Blockers (question/permission) open PASSIVE and arm a timeout: the user
        // is the primary answerer, so the parent isn't woken the instant the
        // blocker lands. It folds into the parent's next natural briefing, and a
        // background worker escalates it to a steering wake only if it's still
        // open at the deadline (see schedule_blocker).
        AttentionFact::Question {
            detail_uri,
            escalate,
            ..
        } => {
            // The URI is the identity; while it stays open the fingerprint is
            // constant, so re-emitting the same pending question never re-wakes.
            if let Some(o) = open(
                db,
                &issue_uri,
                ItemKind::Question,
                detail_uri,
                &issue_id,
                "pending".into(),
                Some(detail_uri.clone()),
                *escalate,
            ) {
                schedule_blocker(orch, &issue_uri, &o);
            }
        }
        AttentionFact::Permission {
            detail_uri,
            escalate,
            ..
        } => {
            if let Some(o) = open(
                db,
                &issue_uri,
                ItemKind::Permission,
                detail_uri,
                &issue_id,
                "pending".into(),
                Some(detail_uri.clone()),
                *escalate,
            ) {
                schedule_blocker(orch, &issue_uri, &o);
            }
        }
        AttentionFact::ArtifactWritten {
            detail_uri,
            content,
            escalate,
        } => {
            // A child writing its output artifact (create-pr, plan, …) is the
            // canonical "reviewable work is ready" signal for a watching parent,
            // whether or not it autoconfirmed — create-pr autoconfirms, so gating
            // on `confirmed` would mean the parent is never woken to review. Open
            // (or refresh) the single review item; it resolves on PR merge/close
            // or when the issue reaches terminal. The version fingerprint makes a
            // confirm re-emit of the same version a no-op, while a new version
            // (fix-after-review) re-surfaces it.
            let fingerprint = format!("artifact:{}", content.version);
            if let Some(o) = open(
                db,
                &issue_uri,
                ItemKind::Review,
                REVIEW_KEY,
                &issue_id,
                fingerprint,
                Some(detail_uri.clone()),
                *escalate,
            ) {
                if o.change != ledger::OpenChange::Unchanged {
                    schedule_evaluation_for_issue(orch, &issue_uri, DeliveryUrgency::Queue);
                }
            }
        }
        AttentionFact::PrStateChange {
            detail_uri,
            content,
            escalate,
        } => {
            if matches!(content.state.as_str(), "merged" | "closed") {
                resolve(
                    db,
                    &issue_uri,
                    ItemKind::Review,
                    REVIEW_KEY,
                    ResolvedBy::User,
                );
            } else {
                // Fingerprint by the reviewable surface: state, mergeability, and
                // diffstat. New commits (changed +/-) or a mergeability flip bump.
                let fingerprint = format!(
                    "pr:{}:{}:{}:{}",
                    content.state,
                    content.mergeable.as_deref().unwrap_or("?"),
                    content.additions.unwrap_or(-1),
                    content.deletions.unwrap_or(-1),
                );
                if let Some(o) = open(
                    db,
                    &issue_uri,
                    ItemKind::Review,
                    REVIEW_KEY,
                    &issue_id,
                    fingerprint,
                    Some(detail_uri.clone()),
                    *escalate,
                ) {
                    if o.change != ledger::OpenChange::Unchanged {
                        schedule_evaluation_for_issue(orch, &issue_uri, DeliveryUrgency::Queue);
                    }
                }
            }
        }
        AttentionFact::Resolved {
            final_status,
            escalate,
        } => {
            // Terminal: cascade-resolve any still-open question/permission/review
            // items for this issue, then open the resolved event-item.
            if let Some(id) = &issue_id {
                let _ = run_db_blocking(|| async {
                    ledger::resolve_open_items_for_issue(db, id, ResolvedBy::System)
                        .await
                        .map_err(|e| e.to_string())
                });
            }
            let fingerprint = format!("resolved:{}", final_status);
            if let Some(o) = open(
                db,
                &issue_uri,
                ItemKind::Resolved,
                RESOLVED_KEY,
                &issue_id,
                fingerprint,
                Some(issue_uri.clone()),
                *escalate,
            ) {
                if o.change != ledger::OpenChange::Unchanged {
                    schedule_evaluation_for_issue(orch, &issue_uri, DeliveryUrgency::Queue);
                }
            }
        }
        // AgentIdleWithWork is deleted as a wake driver (the idle heartbeat).
        // ExternalMessageReply stays on the legacy watch-broadcast path.
        AttentionFact::AgentIdleWithWork { .. } | AttentionFact::ExternalMessageReply { .. } => {}
    }
}

/// Schedule a freshly opened/bumped blocker: (re)arm its escalation deadline,
/// queue it passively (folds into the parent's next briefing without a wake),
/// and wake the escalation worker so it re-computes its next sleep. An
/// `Unchanged` open keeps the existing armed deadline untouched.
fn schedule_blocker(orch: &Orchestrator, issue_uri: &str, outcome: &ledger::OpenOutcome) {
    if outcome.change == ledger::OpenChange::Unchanged {
        return;
    }
    let timeout = crate::config::settings::load_pending_blocker_timeout_secs(&orch.config_dir);
    let escalate_at = chrono::Utc::now().timestamp() + timeout as i64;
    if let Err(e) = ledger::arm_escalation_blocking(&orch.db.local, &outcome.id, escalate_at) {
        log::warn!("attention arm_escalation failed: {}", e);
    }
    schedule_evaluation_for_issue(orch, issue_uri, DeliveryUrgency::Passive);
    orch.blocker_escalation_notify.notify_one();
}

/// Escalate every blocker whose deadline has passed: clear its one-shot timer
/// and schedule a steering evaluation (wakes an idle watcher; respects mute and
/// the liveness net, so an already-answered blocker resolves to no wake).
/// Driven by the orchestrator's escalation worker.
pub async fn fire_due_blocker_escalations(orch: &Orchestrator) {
    let now = chrono::Utc::now().timestamp();
    let due = ledger::due_escalation_items(&orch.db.local, now)
        .await
        .unwrap_or_default();
    for (item_id, issue_uri) in due {
        let _ = ledger::clear_escalation(&orch.db.local, &item_id).await;
        schedule_evaluation_for_issue(orch, &issue_uri, DeliveryUrgency::Steer);
    }
}

#[allow(clippy::too_many_arguments)]
fn open(
    db: &LocalDb,
    issue_uri: &str,
    kind: ItemKind,
    key: &str,
    issue_id: &Option<String>,
    fingerprint: String,
    detail_uri: Option<String>,
    escalate: bool,
) -> Option<ledger::OpenOutcome> {
    let identity = ItemIdentity::issue(issue_uri, kind, key);
    match ledger::open_item_blocking(
        db,
        identity,
        issue_id.clone(),
        fingerprint,
        detail_uri,
        escalate,
    ) {
        Ok(outcome) => Some(outcome),
        Err(e) => {
            log::warn!("attention_ledger open {} failed: {}", kind.as_str(), e);
            None
        }
    }
}

fn resolve(db: &LocalDb, issue_uri: &str, kind: ItemKind, key: &str, by: ResolvedBy) {
    let identity = ItemIdentity::issue(issue_uri, kind, key);
    if let Err(e) = ledger::resolve_item_blocking(db, identity, by) {
        log::warn!("attention_ledger resolve {} failed: {}", kind.as_str(), e);
    }
}

/// Find every watcher job subscribed to this issue and request a coalesced
/// evaluation, then nudge each per the urgency ladder (idle → resume now,
/// busy+interrupt → stop, busy otherwise → evaluated at next turn boundary).
pub fn schedule_evaluation_for_issue(
    orch: &Orchestrator,
    issue_uri: &str,
    urgency: DeliveryUrgency,
) {
    let db = orch.db.local.clone();
    let issue_uri_owned = issue_uri.to_string();
    let watchers = run_db_blocking(move || async move {
        subscriber_jobs_for_issue(&db, &issue_uri_owned)
            .await
            .map_err(|e| e.to_string())
    })
    .unwrap_or_default();

    for job_id in watchers {
        if let Err(e) = ledger::request_evaluation_blocking(&orch.db.local, &job_id, urgency) {
            log::warn!(
                "attention_ledger request_evaluation for {} failed: {}",
                &job_id[..job_id.len().min(8)],
                e
            );
            continue;
        }
        if let Err(e) = crate::messages::delivery::nudge_job_for_urgency(orch, &job_id, urgency) {
            log::warn!(
                "attention nudge for watcher {} failed: {}",
                &job_id[..job_id.len().min(8)],
                e
            );
        }
    }
}

/// Distinct job ids with an active or muted issue subscription for `issue_uri`.
async fn subscriber_jobs_for_issue(db: &LocalDb, issue_uri: &str) -> Result<Vec<String>, String> {
    let issue_uri = issue_uri.to_string();
    db.read(|conn| {
        let issue_uri = issue_uri.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT DISTINCT job_id FROM wake_subscriptions
                     WHERE source_kind='issue' AND source_ref=?1 AND state != 'unsubscribed'",
                    params![issue_uri.as_str()],
                )
                .await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                out.push(row.text(0)?);
            }
            Ok(out)
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// Open a `message` item on the child's issue for the watching parent
/// (request-response). Passive: it never wakes the parent by itself, but folds
/// into the parent's next briefing carrying the current handling state. Opens
/// with `responded:false`; the child's consuming turn end bumps it with the
/// response (see [`enrich_message_items_on_turn_end`]). Replaces the frozen,
/// message-only side-channel copy that produced phantom work orders
/// (CAIRN-1663).
pub fn record_child_message(
    orch: &Orchestrator,
    child_issue_uri: &str,
    child_issue_id: &str,
    child_uri: &str,
    _sender: &str,
    _message: &str,
) {
    let db = orch.db.local.clone();
    let issue_id = child_issue_id.to_string();
    let child_uri_owned = child_uri.to_string();
    let child_issue_uri_owned = child_issue_uri.to_string();
    let result = run_db_blocking(move || async move {
        // One message item per child. If a burst is already open (undelivered),
        // keep its chat window start so the briefing spans the whole burst;
        // otherwise open a fresh window a turn back from the child's chat tail so
        // the parent always gets recent context plus the incoming message and the
        // child's eventual response.
        let open_items = ledger::list_open_items_for_issue(&db, &issue_id, ItemKind::Message)
            .await
            .map_err(|e| e.to_string())?;
        let existing_offset = open_items
            .into_iter()
            .find_map(|(_, _, _, _, detail)| detail.and_then(|d| offset_from_chat_uri(&d)));
        let offset = match existing_offset {
            Some(n) => n,
            None => child_chat_turn_count(&db, &issue_id)
                .await
                .saturating_sub(1)
                .max(0),
        };
        let detail_uri = format!("{}/chat?offset={}", child_uri_owned, offset);
        // A fresh nonce each message bumps the item (re-deliverable). The `msg:`
        // prefix marks "awaiting the child's response"; the first turn end flips
        // it to `resp:` exactly once (see enrich_message_items_on_turn_end).
        let fingerprint = format!("msg:{}", uuid::Uuid::new_v4());
        let identity = ItemIdentity::issue(&child_issue_uri_owned, ItemKind::Message, MESSAGE_KEY);
        ledger::open_item(
            &db,
            identity,
            Some(issue_id.clone()),
            fingerprint,
            Some(detail_uri),
            false,
        )
        .await
        .map_err(|e| e.to_string())?;
        Ok(())
    });
    match result {
        Ok(()) => schedule_evaluation_for_issue(orch, child_issue_uri, DeliveryUrgency::Passive),
        Err(e) => log::warn!("attention record_child_message failed: {}", e),
    }
}

/// Parse the `offset=N` value out of a `.../chat?offset=N` detail URI.
fn offset_from_chat_uri(uri: &str) -> Option<i64> {
    uri.split_once("offset=")
        .and_then(|(_, rest)| rest.split('&').next().unwrap_or(rest).parse::<i64>().ok())
}

/// Count distinct turns currently in a child issue's chat, used as the chat
/// window start for a fresh message burst. Approximate (±1 turn) by design —
/// the window is a coarse "recent + new" view, not an exact cursor.
async fn child_chat_turn_count(db: &LocalDb, issue_id: &str) -> i64 {
    let issue_id = issue_id.to_string();
    db.read(|conn| {
        let issue_id = issue_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT COUNT(DISTINCT e.turn_id) FROM events e
                     JOIN runs r ON e.run_id = r.id
                     WHERE r.issue_id = ?1 AND e.turn_id IS NOT NULL",
                    params![issue_id.as_str()],
                )
                .await?;
            match rows.next().await? {
                Some(row) => Ok(row.i64(0)?),
                None => Ok(0),
            }
        })
    })
    .await
    .unwrap_or(0)
}

/// At a child's turn end, bump each still-awaiting `message` item for the child's
/// issue exactly once so the requesting parent's next briefing re-resolves the
/// chat window now including the child's response. The `msg:` → `resp:`
/// fingerprint flip is the one-shot guard: a long child task with many turns
/// re-delivers only on the first turn after the message, not on every turn.
/// Passive: folds into the next briefing, does not wake.
pub fn enrich_message_items_on_turn_end(orch: &Orchestrator, issue_id: &str, issue_uri: &str) {
    let db = orch.db.local.clone();
    let issue_id_owned = issue_id.to_string();
    let items = run_db_blocking(move || async move {
        ledger::list_open_items_for_issue(&db, &issue_id_owned, ItemKind::Message)
            .await
            .map_err(|e| e.to_string())
    })
    .unwrap_or_default();

    let mut bumped = false;
    for (_, source_ref, key, fingerprint, detail_uri) in items {
        if fingerprint.starts_with("resp:") {
            continue; // already re-delivered with the response
        }
        let identity = ItemIdentity::issue(&source_ref, ItemKind::Message, &key);
        let new_fingerprint = format!("resp:{}", uuid::Uuid::new_v4());
        if let Err(e) = ledger::open_item_blocking(
            &orch.db.local,
            identity,
            Some(issue_id.to_string()),
            new_fingerprint,
            detail_uri,
            false,
        ) {
            log::warn!("attention enrich message failed: {}", e);
        } else {
            bumped = true;
        }
    }
    if bumped {
        schedule_evaluation_for_issue(orch, issue_uri, DeliveryUrgency::Passive);
    }
}

// ---- Briefing computation & rendering ----------------------------------------

/// What to deliver to a watcher, split by what it asks of the watcher:
/// `active` items need a decision/action now (state-items: question / permission
/// / review); `catchup` items are FYI — things that happened while away
/// (resolved / message events) plus muted-but-folded items. Rendering is
/// separated from selection so the cheap deliverability check
/// ([`has_deliverable_briefing`]) never pays for URI resolution.
struct BriefingPlan {
    /// State-items the watcher must act on now.
    active: Vec<ledger::DeliverableItem>,
    /// Event-items and muted-folded items — catch-up, no action required.
    catchup: Vec<ledger::DeliverableItem>,
    /// (item_id, version) pairs to stamp into the watcher's seen cursor.
    seen: Vec<(String, i64)>,
    /// Muted subscription ids to lift (one-shot, at delivery time).
    lift_sub_ids: Vec<String>,
}

/// One briefing item as the UI draws it: a kind, a one-line headline, and the
/// URI to open. Active items render brighter; catch-up items dimmer.
#[derive(serde::Serialize)]
struct BriefingItemView {
    kind: &'static str,
    headline: &'static str,
    uri: String,
}

/// Structured briefing for the UI event (`attention:briefing`): the same items
/// the prompt renders, as a compact list the frontend draws as a wake card. The
/// agent receives the resolved markdown via the prompt; the UI gets this.
#[derive(serde::Serialize)]
struct BriefingEventData {
    active: Vec<BriefingItemView>,
    catchup: Vec<BriefingItemView>,
}

/// What [`claim_and_render_briefing`] hands back: the resolved markdown for the
/// agent's prompt, and the structured item list (JSON) for the UI event.
pub struct BriefingDelivery {
    pub prompt: String,
    pub items_json: String,
}

fn item_view(item: &ledger::DeliverableItem) -> BriefingItemView {
    BriefingItemView {
        kind: item.kind.as_str(),
        headline: item_headline(item.kind),
        uri: item
            .detail_uri
            .clone()
            .unwrap_or_else(|| item.source_ref.clone()),
    }
}

/// Fact-kind aliases a subscription may carry for a given item kind. The legacy
/// `pr_state_change` / `agent_idle_with_work` kinds both map onto `review`.
fn kind_aliases(kind: ItemKind) -> &'static [&'static str] {
    match kind {
        ItemKind::Question => &["question"],
        ItemKind::Permission => &["permission"],
        ItemKind::Review => &["review", "pr_state_change", "agent_idle_with_work"],
        ItemKind::Resolved => &["resolved"],
        ItemKind::Message => &["message"],
    }
}

fn covers(sub: &WakeSubscription, kind: ItemKind) -> bool {
    // A user→child side-channel message is always parent-relevant: it is
    // directed work the parent must see, independent of the subscription's
    // fact-kind filter. This also keeps older parent subscriptions — created
    // before `message` joined the default child fact kinds — surfacing messages.
    if kind == ItemKind::Message {
        return true;
    }
    match &sub.fact_kinds {
        None => true,
        Some(kinds) => {
            let aliases = kind_aliases(kind);
            kinds.iter().any(|k| aliases.contains(&k.as_str()))
        }
    }
}

/// Whether `job_id` has a pending evaluation whose current deliverable set is
/// non-empty. Sync; consulted by the idle-flush decision.
pub fn has_deliverable_briefing(db: &LocalDb, job_id: &str) -> bool {
    let job = job_id.to_string();
    run_db_blocking(move || async move {
        let pending = ledger::has_pending_evaluation(db, &job)
            .await
            .map_err(|e| e.to_string())?;
        if !pending {
            return Ok(false);
        }
        Ok(compute_briefing(db, &job)
            .await
            .map_err(|e| e.to_string())?
            .is_some())
    })
    .unwrap_or(false)
}

/// Claim the pending evaluation for `job_id`, render the briefing from current
/// ledger state, stamp the seen cursor, lift any folded mutes, and return the
/// rendered text. `None` when no evaluation is pending or the deliverable set is
/// empty (the empty-set drop). Called inside `continue_job_impl`.
pub fn claim_and_render_briefing(orch: &Orchestrator, job_id: &str) -> Option<BriefingDelivery> {
    let db = orch.db.local.clone();
    let job = job_id.to_string();
    run_db_blocking(move || async move {
        // Drain the evaluation regardless; an empty set means "nothing to say".
        let urgency = ledger::take_evaluation(&db, &job)
            .await
            .map_err(|e| e.to_string())?;
        if urgency.is_none() {
            return Ok(None);
        }
        let Some(plan) = compute_briefing(&db, &job)
            .await
            .map_err(|e| e.to_string())?
        else {
            return Ok(None);
        };
        // Resolve each item's detail_uri through the in-process read so the
        // prompt carries current content AND its affordance/actions block.
        let prompt = render_briefing(orch, &plan).await;
        for (item_id, version) in &plan.seen {
            ledger::mark_seen(&db, item_id, &job, *version)
                .await
                .map_err(|e| e.to_string())?;
        }
        for sub_id in &plan.lift_sub_ids {
            lift_subscription(&db, sub_id)
                .await
                .map_err(|e| e.to_string())?;
        }
        // Resolve delivered message items so the next message to this child opens
        // a fresh chat window. Deliverability is gated by the per-watcher seen
        // cursor, so resolving here never hides the item from another watcher
        // that has not yet seen it (single-watcher side channel is the norm).
        for item in plan.active.iter().chain(plan.catchup.iter()) {
            if item.kind == ItemKind::Message {
                let identity = ItemIdentity {
                    source_kind: item.source_kind.clone(),
                    source_ref: item.source_ref.clone(),
                    kind: item.kind,
                    key: item.key.clone(),
                };
                let _ = ledger::resolve_item(&db, identity, ResolvedBy::User).await;
            }
        }
        let event = BriefingEventData {
            active: plan.active.iter().map(item_view).collect(),
            catchup: plan.catchup.iter().map(item_view).collect(),
        };
        let items_json = serde_json::to_string(&event).unwrap_or_else(|_| "{}".into());
        Ok(Some(BriefingDelivery { prompt, items_json }))
    })
    .unwrap_or(None)
}

async fn lift_subscription(db: &LocalDb, sub_id: &str) -> Result<(), String> {
    let sub_id = sub_id.to_string();
    let now = chrono::Utc::now().timestamp();
    db.write(|conn| {
        let sub_id = sub_id.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE wake_subscriptions SET state='active', mute_until_kind=NULL,
                 mute_until_ref=NULL, updated_at=?1 WHERE id=?2",
                params![now, sub_id.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|e| e.to_string())
}

async fn compute_briefing(db: &LocalDb, job_id: &str) -> Result<Option<BriefingPlan>, String> {
    let subs = super::wakes::list_subscriptions_for_job(db, job_id).await?;
    let issue_subs: Vec<&WakeSubscription> = subs
        .iter()
        .filter(|s| {
            s.source_kind == SOURCE_KIND_ISSUE && s.state != WakeSubscriptionState::Unsubscribed
        })
        .collect();
    if issue_subs.is_empty() {
        return Ok(None);
    }

    // Distinct subscribed sources.
    let mut sources: Vec<String> = issue_subs
        .iter()
        .filter_map(|s| s.source_ref.clone())
        .collect();
    sources.sort();
    sources.dedup();

    let mut actionable: Vec<ledger::DeliverableItem> = Vec::new();
    let mut digest: Vec<ledger::DeliverableItem> = Vec::new();
    let mut lift_sub_ids: Vec<String> = Vec::new();

    for source_ref in &sources {
        let raw = ledger::deliverable_items_for_source(db, job_id, SOURCE_KIND_ISSUE, source_ref)
            .await
            .map_err(|e| e.to_string())?;
        if raw.is_empty() {
            continue;
        }
        // Delivery-time liveness reconciliation: a state-item whose underlying
        // state was handled (question answered, permission decided, PR
        // merged/closed) through ANY path is no longer deliverable. Lazily
        // resolve it and exclude — this is the stale-wake fix's safety net, so
        // correctness does not depend on every answer site wiring a resolve.
        let mut items: Vec<ledger::DeliverableItem> = Vec::new();
        for item in raw {
            if item.kind.is_state_item() && !item_is_live(db, &item).await? {
                let identity = ItemIdentity {
                    source_kind: item.source_kind.clone(),
                    source_ref: item.source_ref.clone(),
                    kind: item.kind,
                    key: item.key.clone(),
                };
                let _ = ledger::resolve_item(db, identity, ResolvedBy::User).await;
                continue;
            }
            // A message item IS the windowed child chat (`.../chat?offset=N`).
            // A row without a chat detail_uri can only be a stale pre-pivot
            // message (the old per-message code stored the bare node URI, and the
            // old turn-end bump nulled it). Rendering it would fall back to the
            // issue overview — which the parent, the issue's author, does not
            // need. Resolve and drop it.
            if item.kind == ItemKind::Message
                && item
                    .detail_uri
                    .as_deref()
                    .is_none_or(|d| !d.contains("/chat"))
            {
                let identity = ItemIdentity {
                    source_kind: item.source_kind.clone(),
                    source_ref: item.source_ref.clone(),
                    kind: item.kind,
                    key: item.key.clone(),
                };
                let _ = ledger::resolve_item(db, identity, ResolvedBy::System).await;
                continue;
            }
            items.push(item);
        }
        if items.is_empty() {
            continue;
        }
        let deliverable_kinds: Vec<ItemKind> = items.iter().map(|i| i.kind).collect();
        // Subs governing this source.
        let source_subs: Vec<&WakeSubscription> = issue_subs
            .iter()
            .copied()
            .filter(|s| s.source_ref.as_deref() == Some(source_ref.as_str()))
            .collect();
        for item in items.iter() {
            let covering: Vec<&WakeSubscription> = source_subs
                .iter()
                .copied()
                .filter(|s| covers(s, item.kind))
                .collect();
            if covering.is_empty() {
                continue; // not subscribed to this kind
            }
            let has_active = covering
                .iter()
                .any(|s| s.state == WakeSubscriptionState::Active);
            // A muted "until kind" lifts when an item of that kind is deliverable.
            let until_lift: Vec<String> = covering
                .iter()
                .filter(|s| s.state == WakeSubscriptionState::Muted)
                .filter(|s| match &s.mute_until_kind {
                    Some(until) => deliverable_kinds
                        .iter()
                        .any(|k| kind_aliases(*k).contains(&until.as_str())),
                    None => false,
                })
                .map(|s| s.id.clone())
                .collect();
            if has_active || item.escalate || !until_lift.is_empty() {
                actionable.push(item.clone());
                lift_sub_ids.extend(until_lift);
            } else {
                digest.push(item.clone());
            }
        }
    }

    if actionable.is_empty() {
        return Ok(None);
    }

    // Fold the muted digest only for sources that produced actionable items, and
    // lift those muted subs (one-shot mute lift at delivery time).
    let actionable_sources: std::collections::HashSet<String> =
        actionable.iter().map(|i| i.source_ref.clone()).collect();
    let folded_digest: Vec<ledger::DeliverableItem> = digest
        .into_iter()
        .filter(|i| actionable_sources.contains(&i.source_ref))
        .collect();
    for source_ref in &actionable_sources {
        for s in issue_subs.iter().copied().filter(|s| {
            s.state == WakeSubscriptionState::Muted
                && s.source_ref.as_deref() == Some(source_ref.as_str())
        }) {
            if !lift_sub_ids.contains(&s.id) {
                lift_sub_ids.push(s.id.clone());
            }
        }
    }

    let mut seen: Vec<(String, i64)> = Vec::new();
    for item in actionable.iter().chain(folded_digest.iter()) {
        seen.push((item.id.clone(), item.version));
    }
    // Split presentation by what the item asks of the watcher: state-items
    // (question/permission/review) need action → active; event-items
    // (resolved/message) and muted-folded items are catch-up.
    let mut active = Vec::new();
    let mut catchup = Vec::new();
    for item in actionable {
        if item.kind.is_state_item() {
            active.push(item);
        } else {
            catchup.push(item);
        }
    }
    catchup.extend(folded_digest);
    Ok(Some(BriefingPlan {
        active,
        catchup,
        seen,
        lift_sub_ids,
    }))
}

/// Whether a state-item's underlying actionable state is still live. Coarse,
/// issue-scoped checks: any unanswered prompt / pending permission / open PR or
/// unconfirmed artifact keeps the item deliverable. Event-items are always live
/// until seen.
async fn item_is_live(db: &LocalDb, item: &ledger::DeliverableItem) -> Result<bool, String> {
    let Some(issue_id) = item.issue_id.clone() else {
        return Ok(true); // can't check without an issue id; don't drop
    };
    let kind = item.kind;
    db.read(|conn| {
        let issue_id = issue_id.clone();
        Box::pin(async move {
            let live = match kind {
                ItemKind::Question => {
                    exists(
                        conn,
                        "SELECT 1 FROM prompts p JOIN runs r ON p.run_id=r.id
                     WHERE r.issue_id=?1 AND p.response IS NULL LIMIT 1",
                        &issue_id,
                    )
                    .await?
                }
                ItemKind::Permission => {
                    exists(
                        conn,
                        "SELECT 1 FROM permission_requests pr JOIN runs r ON pr.run_id=r.id
                     WHERE r.issue_id=?1 AND pr.status='pending' LIMIT 1",
                        &issue_id,
                    )
                    .await?
                }
                // A review item is deliverable while open and resolved
                // explicitly on PR merge/close or issue-terminal cascade — NOT
                // gated on a local merge_request / unconfirmed-artifact row. A
                // create-pr autoconfirms and (in dev, with no GitHub webhook) has
                // no merge_request row, so the strict check would wrongly drop a
                // legitimately-open review. The seen cursor prevents re-delivery.
                ItemKind::Review => true,
                _ => true,
            };
            Ok::<_, crate::storage::DbError>(live)
        })
    })
    .await
    .map_err(|e| e.to_string())
}

async fn exists(
    conn: &turso::Connection,
    sql: &str,
    issue_id: &str,
) -> crate::storage::DbResult<bool> {
    let mut rows = conn.query(sql, params![issue_id]).await?;
    Ok(rows.next().await?.is_some())
}

/// Render the briefing for the agent's prompt by resolving each item's
/// `detail_uri` through the in-process read — the watcher receives current
/// content AND its affordance/actions block inline, with no second read. Two
/// sections: what needs action now, then what to catch up on.
async fn render_briefing(orch: &Orchestrator, plan: &BriefingPlan) -> String {
    let mut out = String::from("[Attention briefing]\n");
    if !plan.active.is_empty() {
        out.push_str("\nNeeds your action:\n");
        for (idx, item) in plan.active.iter().enumerate() {
            out.push('\n');
            out.push_str(&format!(
                "{}. {} — {}\n",
                idx + 1,
                item_headline(item.kind),
                item.source_ref
            ));
            out.push_str(&resolve_item_markdown(orch, item).await);
            out.push('\n');
        }
    }
    if !plan.catchup.is_empty() {
        out.push_str("\nCatch up (no action required):\n");
        for item in &plan.catchup {
            out.push('\n');
            out.push_str(&format!(
                "• {} — {}\n",
                item_headline(item.kind),
                item.source_ref
            ));
            out.push_str(&resolve_item_markdown(orch, item).await);
            out.push('\n');
        }
    }
    out
}

/// One-line human label for an item kind (the briefing line header).
fn item_headline(kind: ItemKind) -> &'static str {
    match kind {
        ItemKind::Question => "Question awaiting an answer",
        ItemKind::Permission => "Permission awaiting a decision",
        ItemKind::Review => "Work product ready for review",
        ItemKind::Resolved => "Issue resolved",
        ItemKind::Message => "Message to a child issue",
    }
}

/// Resolve an item's detail URI to rendered markdown (content + affordances).
/// Falls back to a bare "read X" pointer if resolution fails or yields nothing.
async fn resolve_item_markdown(orch: &Orchestrator, item: &ledger::DeliverableItem) -> String {
    let uri = item
        .detail_uri
        .as_deref()
        .unwrap_or(item.source_ref.as_str());
    match resolve_uri_to_markdown(orch, uri).await {
        Some(text) => text,
        None => format!("   (read {uri})"),
    }
}

/// Resolve a single `cairn://` (or file/web) URI to rendered markdown via the
/// same `read_batch` path that backs `cairn read [uri]`. `run_id` is `None` so
/// the briefing never pollutes the agent's read-dedup state. Returns `None` on a
/// resolution failure (e.g. a fence suspension string, which is not an
/// envelope) or an empty body, so the caller can fall back to a bare pointer.
async fn resolve_uri_to_markdown(orch: &Orchestrator, uri: &str) -> Option<String> {
    let request = crate::mcp::types::McpCallbackRequest {
        cwd: String::new(),
        run_id: None,
        tool: "read_batch".to_string(),
        payload: serde_json::json!({ "paths": [uri] }),
        tool_use_id: None,
    };
    let cursors = std::sync::Mutex::new(std::collections::HashMap::new());
    let raw = crate::mcp::handlers::read::handle_read_batch(orch, &request, &cursors).await;
    let envelope: cairn_common::read::ReadBatchEnvelope = serde_json::from_str(&raw).ok()?;
    let text = envelope.text.trim();
    if text.is_empty() {
        None
    } else {
        Some(text.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::LocalDb;

    const CHILD_URI: &str = "cairn://p/PROJ/2";

    async fn migrated_db() -> LocalDb {
        crate::storage::migrated_test_db("attention-delivery.db").await
    }

    /// Parent issue + watcher job, child issue-1 with a child job + run, and a
    /// watcher subscription to the child issue.
    async fn seed(
        db: &LocalDb,
        sub_state: &str,
        fact_kinds_json: Option<&str>,
        until_kind: Option<&str>,
    ) {
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
              VALUES('p','w','Project','PROJ','/tmp/repo',1,1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
              VALUES('parent','p',1,'Parent','active','active','none',1,1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
              VALUES('issue-1','p',2,'Child','active','active','none',1,1);
            INSERT INTO jobs(id, project_id, issue_id, status, current_session_id, created_at, updated_at)
              VALUES('watcher','p','parent','running','sess',1,1);
            INSERT INTO jobs(id, project_id, issue_id, status, current_session_id, created_at, updated_at)
              VALUES('child-job','p','issue-1','running','sess2',1,1);
            INSERT INTO runs(id, project_id, job_id, issue_id, created_at, updated_at)
              VALUES('run-1','p','child-job','issue-1',1,1);
            ",
        )
        .await
        .unwrap();
        let sub_state = sub_state.to_string();
        let fact_kinds_json = fact_kinds_json.map(str::to_string);
        let until_kind = until_kind.map(str::to_string);
        db.write(move |conn| {
            let sub_state = sub_state.clone();
            let fact_kinds_json = fact_kinds_json.clone();
            let until_kind = until_kind.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO wake_subscriptions
                       (id, job_id, source_kind, source_ref, fact_kinds_json, state,
                        mute_until_kind, mute_until_ref, created_by, created_at, updated_at, one_shot)
                     VALUES('sub-1','watcher','issue',?1,?2,?3,?4,NULL,'agent',1,1,0)",
                    params![CHILD_URI, fact_kinds_json.as_deref(), sub_state.as_str(), until_kind.as_deref()],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    async fn add_unanswered_prompt(db: &LocalDb) {
        db.execute_script(
            "INSERT INTO prompts(id, run_id, questions, response, created_at)
             VALUES('pr-1','run-1','[]',NULL,1);",
        )
        .await
        .unwrap();
    }

    async fn open_question(db: &LocalDb) {
        ledger::open_item(
            db,
            ItemIdentity::issue(CHILD_URI, ItemKind::Question, "q-1"),
            Some("issue-1".into()),
            "{\"questions\":[]}".into(),
            Some("cairn://p/PROJ/2/1/planner/questions/q-1".into()),
            false,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn active_sub_with_live_question_yields_briefing() {
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        add_unanswered_prompt(&db).await;
        open_question(&db).await;

        let briefing = compute_briefing(&db, "watcher").await.unwrap();
        assert!(briefing.is_some(), "live open question is deliverable");
        let plan = briefing.unwrap();
        assert_eq!(plan.active.len(), 1);
        assert_eq!(plan.active[0].kind, ItemKind::Question);
    }

    #[tokio::test]
    async fn answered_question_drops_via_liveness_net() {
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        // No unanswered prompt exists -> the open question item is stale.
        open_question(&db).await;

        let briefing = compute_briefing(&db, "watcher").await.unwrap();
        assert!(
            briefing.is_none(),
            "stale question reconciled away at delivery"
        );
        // It was lazily resolved, so it stays gone even if a prompt later appears
        // unrelated.
        let again = compute_briefing(&db, "watcher").await.unwrap();
        assert!(again.is_none());
    }

    #[tokio::test]
    async fn seen_cursor_suppresses_redelivery() {
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        add_unanswered_prompt(&db).await;
        open_question(&db).await;

        let data = compute_briefing(&db, "watcher").await.unwrap().unwrap();
        for (item_id, version) in &data.seen {
            ledger::mark_seen(&db, item_id, "watcher", *version)
                .await
                .unwrap();
        }
        let after = compute_briefing(&db, "watcher").await.unwrap();
        assert!(
            after.is_none(),
            "seen at current version -> not redelivered"
        );
    }

    #[tokio::test]
    async fn muted_sub_excludes_item_from_actionable() {
        let db = migrated_db().await;
        seed(&db, "muted", None, None).await;
        add_unanswered_prompt(&db).await;
        open_question(&db).await;

        let briefing = compute_briefing(&db, "watcher").await.unwrap();
        assert!(
            briefing.is_none(),
            "muted source produces no actionable wake"
        );
    }

    #[tokio::test]
    async fn open_review_item_delivers_as_active_without_local_pr_tables() {
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        // A confirmed create-pr review item with no merge_request row and no
        // unconfirmed artifact (the dev / autoconfirm case) must still deliver.
        ledger::open_item(
            &db,
            ItemIdentity::issue(CHILD_URI, ItemKind::Review, "review"),
            Some("issue-1".into()),
            "artifact:1".into(),
            Some(format!("{CHILD_URI}/1/builder/create-pr")),
            false,
        )
        .await
        .unwrap();

        let plan = compute_briefing(&db, "watcher").await.unwrap();
        assert!(
            plan.is_some(),
            "an open review item is deliverable without local PR tables"
        );
        let plan = plan.unwrap();
        // Review needs the parent's action → active tier.
        assert!(plan.active.iter().any(|i| i.kind == ItemKind::Review));
    }

    #[tokio::test]
    async fn stale_message_without_chat_uri_is_dropped_not_rendered_as_overview() {
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        // A pre-pivot message row: no chat detail_uri. It must NOT fall back to
        // the issue overview; it is resolved and excluded.
        ledger::open_item(
            &db,
            ItemIdentity::issue(CHILD_URI, ItemKind::Message, "stale"),
            Some("issue-1".into()),
            "legacy".into(),
            None,
            false,
        )
        .await
        .unwrap();

        let briefing = compute_briefing(&db, "watcher").await.unwrap();
        assert!(
            briefing.is_none(),
            "stale message with no chat uri is dropped"
        );
        // A well-formed message item (chat window) still delivers.
        ledger::open_item(
            &db,
            ItemIdentity::issue(CHILD_URI, ItemKind::Message, MESSAGE_KEY),
            Some("issue-1".into()),
            "msg:1".into(),
            Some(format!("{CHILD_URI}/1/builder/chat?offset=0")),
            false,
        )
        .await
        .unwrap();
        let plan = compute_briefing(&db, "watcher").await.unwrap();
        assert!(
            plan.is_some(),
            "a chat-windowed message item is deliverable"
        );
        let plan = plan.unwrap();
        // A message is a catch-up (event) item, not an active one.
        assert!(plan.active.is_empty());
        assert!(plan.catchup.iter().all(|i| i.kind == ItemKind::Message));
    }

    #[tokio::test]
    async fn mute_until_resolved_lifts_when_resolved_item_appears() {
        let db = migrated_db().await;
        seed(&db, "muted", None, Some("resolved")).await;
        // A terminal resolution event-item appears on the muted child issue.
        ledger::open_item(
            &db,
            ItemIdentity::issue(CHILD_URI, ItemKind::Resolved, "resolved"),
            Some("issue-1".into()),
            "{\"final_status\":\"merged\"}".into(),
            Some(CHILD_URI.into()),
            false,
        )
        .await
        .unwrap();

        let briefing = compute_briefing(&db, "watcher").await.unwrap();
        assert!(briefing.is_some(), "until-kind item pierces its own mute");
        let plan = briefing.unwrap();
        // Resolved is a catch-up (event) item.
        assert!(plan.catchup.iter().any(|i| i.kind == ItemKind::Resolved));
        assert!(
            plan.lift_sub_ids.contains(&"sub-1".to_string()),
            "mute lifts at delivery"
        );
    }
}
