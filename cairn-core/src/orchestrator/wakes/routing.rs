use cairn_db::turso::params;

use crate::messages::queued::DeliveryUrgency;
use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;

use super::matching::{matching_subscription, matching_subscriptions_for_source};
use super::types::*;

pub(super) fn child_attention_message(
    issue_uri: &str,
    attention: &str,
    fact_kind: &str,
    detail_uri: Option<&str>,
) -> String {
    match detail_uri {
        Some(detail_uri) => format!("[Child update] {attention}/{fact_kind}. Read {detail_uri}."),
        None => format!("[Child update] {attention}/{fact_kind}. Read {issue_uri}."),
    }
}

pub fn route_child_attention(
    orch: &Orchestrator,
    _child_issue_id: &str,
    issue_uri: &str,
    attention: &str,
    fact_kind: &str,
    detail_uri: Option<&str>,
    urgency: DeliveryUrgency,
) -> Result<(), String> {
    let message = child_attention_message(issue_uri, attention, fact_kind, detail_uri);
    let event = WakeEvent {
        source: WakeSource::Issue {
            reference: issue_uri.to_string(),
        },
        fact_kind: fact_kind.to_string(),
        detail_uri: detail_uri.map(ToString::to_string),
        delivery: WakeDelivery::Broadcast { message },
        urgency,
    };
    route_wake_sync(orch, event).map(|_| ())
}

pub(super) fn route_wake_sync(
    orch: &Orchestrator,
    event: WakeEvent,
) -> Result<WakeRouteAction, String> {
    let orch = orch.clone();
    std::thread::scope(|scope| {
        scope
            .spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| format!("Failed to start database runtime: {e}"))?
                    .block_on(async move { route_wake(&orch, event).await })
            })
            .join()
            .map_err(|payload| {
                format!(
                    "Database task panicked: {}",
                    crate::storage::panic_message(&*payload)
                )
            })?
    })
}

/// Route one external wake through the subscription registry.
///
/// This is the subscription-governed attention choke point: it resolves either
/// the targeted subscriber named by the delivery or every job with a matching
/// subscription, delivers active subscriptions, records muted subscriptions into
/// the digest, and drops absent/unsubscribed scopes.
pub fn route_resource_updated(
    orch: &Orchestrator,
    resource_uri: &str,
) -> Result<WakeRouteAction, String> {
    let event = WakeEvent {
        source: WakeSource::Resource {
            reference: resource_uri.to_string(),
        },
        fact_kind: "updated".to_string(),
        detail_uri: Some(resource_uri.to_string()),
        delivery: WakeDelivery::Broadcast {
            message: format!("[Resource update] {resource_uri} was updated. Read {resource_uri}."),
        },
        urgency: DeliveryUrgency::Queue,
    };
    route_wake_sync(orch, event)
}

pub fn route_process_event(
    orch: &Orchestrator,
    process_ref: &str,
    fact_kind: &str,
    detail_uri: Option<&str>,
) -> Result<WakeRouteAction, String> {
    let event = WakeEvent {
        source: WakeSource::Process {
            reference: process_ref.to_string(),
        },
        fact_kind: fact_kind.to_string(),
        detail_uri: detail_uri.map(ToString::to_string),
        delivery: WakeDelivery::Broadcast {
            message: format!("[Process update] {process_ref} emitted {fact_kind}."),
        },
        urgency: DeliveryUrgency::Queue,
    };
    route_wake_sync(orch, event)
}

pub fn route_condition_event(
    orch: &Orchestrator,
    condition_ref: &str,
    fact_kind: &str,
    detail_uri: Option<&str>,
) -> Result<WakeRouteAction, String> {
    let event = WakeEvent {
        source: WakeSource::Condition {
            reference: condition_ref.to_string(),
        },
        fact_kind: fact_kind.to_string(),
        detail_uri: detail_uri.map(ToString::to_string),
        delivery: WakeDelivery::Broadcast {
            message: format!("[Condition update] {condition_ref} emitted {fact_kind}."),
        },
        urgency: DeliveryUrgency::Queue,
    };
    route_wake_sync(orch, event)
}

pub async fn route_wake(orch: &Orchestrator, event: WakeEvent) -> Result<WakeRouteAction, String> {
    let subscriptions = subscriptions_for_event(&orch.db.local, &event).await?;
    if subscriptions.is_empty() {
        return Ok(WakeRouteAction::Dropped);
    }

    let mut delivered = false;
    let mut suppressed = false;
    for subscription in subscriptions {
        match route_wake_to_subscription(orch, &event, subscription).await? {
            WakeRouteAction::Delivered => delivered = true,
            WakeRouteAction::Suppressed => suppressed = true,
            WakeRouteAction::Dropped => {}
        }
    }

    if delivered {
        Ok(WakeRouteAction::Delivered)
    } else if suppressed {
        Ok(WakeRouteAction::Suppressed)
    } else {
        Ok(WakeRouteAction::Dropped)
    }
}

async fn subscriptions_for_event(
    db: &LocalDb,
    event: &WakeEvent,
) -> Result<Vec<WakeSubscription>, String> {
    match &event.delivery {
        WakeDelivery::Targeted {
            subscriber_job_id, ..
        }
        | WakeDelivery::MessageDigest {
            subscriber_job_id, ..
        } => matching_subscription(
            db,
            subscriber_job_id,
            event.source.kind(),
            event.source.reference(),
            &event.fact_kind,
        )
        .await
        .map(|sub| sub.into_iter().collect()),
        WakeDelivery::Broadcast { .. } => {
            matching_subscriptions_for_source(
                db,
                event.source.kind(),
                event.source.reference(),
                &event.fact_kind,
            )
            .await
        }
    }
}

async fn route_wake_to_subscription(
    orch: &Orchestrator,
    event: &WakeEvent,
    subscription: WakeSubscription,
) -> Result<WakeRouteAction, String> {
    // A closed thread's session is not a deliverable recipient. Dropping here,
    // before the state match, is what keeps closure from consuming anything: a
    // one-shot subscription is only spent by an action, so a terminal-exit watch
    // held by a thread that was closed mid-wait survives to fire on reopen.
    if recipient_is_dormant(orch, event, &subscription).await {
        return Ok(WakeRouteAction::Dropped);
    }
    let action = match subscription.state {
        WakeSubscriptionState::Active => {
            deliver_active_wake(orch, event, &subscription, None).await?;
            WakeRouteAction::Delivered
        }
        WakeSubscriptionState::Muted if event.urgency == DeliveryUrgency::Interrupt => {
            deliver_active_wake(
                orch,
                event,
                &subscription,
                Some("[Interrupt wake pierced mute] "),
            )
            .await?;
            WakeRouteAction::Delivered
        }
        // Mute is now downgrade-at-creation for pushes (CAIRN-1900); the legacy
        // suppressed_wakes digest store is gone. A non-interrupt wake to a muted
        // subscription on these live non-push callers (terminal exit, condition,
        // resource, process, child-attention broadcast) is dropped — there is no
        // digest to accrue and no ride-along channel for them.
        WakeSubscriptionState::Muted => WakeRouteAction::Dropped,
        WakeSubscriptionState::Unsubscribed => WakeRouteAction::Dropped,
    };

    // A one-shot subscription (terminal exit) is consumed the first time a
    // matching wake routes to it — delivered or suppressed into the digest — so
    // it can never fire twice. An unsubscribed scope never fired, so leave it.
    if subscription.one_shot && action != WakeRouteAction::Dropped {
        consume_one_shot_subscription(orch, &subscription).await?;
    }

    Ok(action)
}

/// The job this wake would actually reach, and whether it is dormant.
///
/// A targeted delivery names its recipient; a broadcast reaches the subscribing
/// job. Both resolve through the one dormancy rule in
/// [`crate::threads::is_dormant_thread_session`], which the direct parent-push
/// path and turn admission take as well — so no two axes can disagree about
/// whether a closed thread is listening.
async fn recipient_is_dormant(
    orch: &Orchestrator,
    event: &WakeEvent,
    subscription: &WakeSubscription,
) -> bool {
    let job_id = match &event.delivery {
        WakeDelivery::Targeted {
            subscriber_job_id, ..
        }
        | WakeDelivery::MessageDigest {
            subscriber_job_id, ..
        } => subscriber_job_id.as_str(),
        WakeDelivery::Broadcast { .. } => subscription.job_id.as_str(),
    };
    crate::threads::is_dormant_thread_session(&orch.db.local, job_id).await
}

async fn consume_one_shot_subscription(
    orch: &Orchestrator,
    subscription: &WakeSubscription,
) -> Result<(), String> {
    let id = subscription.id.clone();
    orch.db
        .local
        .write(|conn| {
            let id = id.clone();
            Box::pin(async move {
                conn.execute(
                    "DELETE FROM wake_subscriptions WHERE id = ?1",
                    params![id.as_str()],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .map_err(|error| format!("Failed to consume one-shot wake subscription: {error}"))?;
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "wake_subscriptions", "action": "delete"}),
    );
    Ok(())
}

async fn deliver_active_wake(
    orch: &Orchestrator,
    event: &WakeEvent,
    subscription: &WakeSubscription,
    prefix: Option<&str>,
) -> Result<(), String> {
    let (job_id, sender_name, message) = match &event.delivery {
        WakeDelivery::Targeted {
            subscriber_job_id,
            sender_name,
            message,
        } => (
            subscriber_job_id.as_str(),
            sender_name.as_deref(),
            format_message_with_prefix(prefix, message),
        ),
        WakeDelivery::Broadcast { message } => (
            subscription.job_id.as_str(),
            None,
            format_message_with_prefix(prefix, message),
        ),
        WakeDelivery::MessageDigest { .. } => {
            // Active message-like wakes are carried by the durable message or
            // side-channel row that created the wake; nothing to resume here.
            return Ok(());
        }
    };
    crate::orchestrator::parent_wake::queue_or_resume_parent(
        orch,
        job_id,
        sender_name,
        &message,
        event.urgency,
    );
    Ok(())
}

fn format_message_with_prefix(prefix: Option<&str>, message: &str) -> String {
    match prefix {
        Some(prefix) => format!("{prefix}{message}"),
        None => message.to_string(),
    }
}
