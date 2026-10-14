use cairn_db::turso::params;

use crate::runtime::AttentionRuntime;
use cairn_models::DeliveryUrgency;

use super::matching::{matching_subscription, matching_subscriptions_for_source};
use super::types::*;

pub fn child_attention_message(
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
    runtime: &AttentionRuntime,
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
        muted: MutedDelivery::Drop,
    };
    route_wake_sync(runtime, event).map(|_| ())
}

pub fn route_wake_sync(
    runtime: &AttentionRuntime,
    event: WakeEvent,
) -> Result<WakeRouteAction, String> {
    let runtime = runtime.clone();
    std::thread::scope(|scope| {
        scope
            .spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| format!("Failed to start database runtime: {e}"))?
                    .block_on(async move { route_wake(&runtime, event).await })
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
    runtime: &AttentionRuntime,
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
        muted: MutedDelivery::Drop,
    };
    route_wake_sync(runtime, event)
}

pub fn route_process_event(
    runtime: &AttentionRuntime,
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
        muted: MutedDelivery::Drop,
    };
    route_wake_sync(runtime, event)
}

pub fn route_condition_event(
    runtime: &AttentionRuntime,
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
        muted: MutedDelivery::Drop,
    };
    route_wake_sync(runtime, event)
}

pub async fn route_wake(
    runtime: &AttentionRuntime,
    event: WakeEvent,
) -> Result<WakeRouteAction, String> {
    let subscriptions = subscriptions_for_event(runtime, &event).await?;
    if subscriptions.is_empty() {
        return Ok(WakeRouteAction::Dropped);
    }

    let mut delivered = false;
    let mut suppressed = false;
    for subscription in subscriptions {
        match route_wake_to_subscription(runtime, &event, subscription).await? {
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
    runtime: &AttentionRuntime,
    event: &WakeEvent,
) -> Result<Vec<WakeSubscription>, String> {
    let db = &runtime.private_db;
    match &event.delivery {
        WakeDelivery::Targeted {
            subscriber_job_id, ..
        }
        | WakeDelivery::MessageDigest {
            subscriber_job_id, ..
        } => {
            let persisted = matching_subscription(
                db,
                subscriber_job_id,
                event.source.kind(),
                event.source.reference(),
                &event.fact_kind,
            )
            .await?;
            Ok(persisted
                .or_else(|| match &event.source {
                    WakeSource::Fact { clause_ref }
                        if clause_ref.starts_with(ADDRESSED_FACT_REF_PREFIX) =>
                    {
                        Some(addressed_fact_subscription(
                            subscriber_job_id,
                            clause_ref,
                            &event.fact_kind,
                        ))
                    }
                    _ => None,
                })
                .into_iter()
                .collect())
        }
        WakeDelivery::Broadcast { .. } => {
            matching_subscriptions_for_source(
                runtime.host.clone(),
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
    runtime: &AttentionRuntime,
    event: &WakeEvent,
    subscription: WakeSubscription,
) -> Result<WakeRouteAction, String> {
    // A closed thread's session is not a deliverable recipient. Dropping here,
    // before the state match, is what keeps closure from consuming anything: a
    // one-shot subscription is only spent by an action, so a terminal-exit watch
    // held by a thread that was closed mid-wait survives to fire on reopen.
    if recipient_is_dormant(runtime, event, &subscription).await {
        return Ok(WakeRouteAction::Dropped);
    }
    // CAIRN-4665: a superseded execution's node is not a deliverable recipient
    // either — peer messages, post comments, condition wakes, and settled-checks
    // broadcasts all reach a node that can no longer take a turn.
    //
    // Unlike dormancy, this DOES consume a one-shot subscription. A closed
    // thread reopens, so its terminal watch must survive; a superseded execution
    // never becomes canonical again except by explicit operator transfer, and
    // leaving its one-shots armed would keep firing them at a fenced node
    // forever.
    if recipient_is_fenced(runtime, event, &subscription).await {
        if subscription.one_shot {
            consume_one_shot_subscription(runtime, &subscription).await?;
        }
        return Ok(WakeRouteAction::Dropped);
    }
    // A merged or closed issue takes no more work, so its jobs are not
    // deliverable recipients either. Like dormancy and unlike the fence, this
    // leaves a one-shot subscription armed: a resolved issue can reopen, and its
    // watch should still fire then.
    if recipient_issue_is_resolved(runtime, event, &subscription).await {
        log::info!(
            "dropping wake: recipient {} is on a resolved issue",
            recipient_job_id(event, &subscription)
        );
        return Ok(WakeRouteAction::Dropped);
    }
    let action = match subscription.state {
        WakeSubscriptionState::Active => {
            deliver_active_wake(runtime, event, &subscription, None, event.urgency).await?;
            WakeRouteAction::Delivered
        }
        WakeSubscriptionState::Muted if event.urgency == DeliveryUrgency::Interrupt => {
            deliver_active_wake(
                runtime,
                event,
                &subscription,
                Some("[Interrupt wake pierced mute] "),
                event.urgency,
            )
            .await?;
            WakeRouteAction::Delivered
        }
        // An event whose producer declared it rides along is still told to a
        // muted recipient, passively: it lands with the next run rather than
        // waking this one.
        WakeSubscriptionState::Muted if event.muted == MutedDelivery::RideAlong => {
            deliver_active_wake(
                runtime,
                event,
                &subscription,
                None,
                DeliveryUrgency::Passive,
            )
            .await?;
            WakeRouteAction::Suppressed
        }
        // Mute is now downgrade-at-creation for pushes (CAIRN-1900); the legacy
        // suppressed_wakes digest store is gone. A non-interrupt wake to a muted
        // subscription on these live non-push callers (terminal exit, condition,
        // resource, process, child-attention broadcast, and fact sources that
        // declare `Drop`) is dropped — there is no digest to accrue and no
        // ride-along channel for them.
        WakeSubscriptionState::Muted => WakeRouteAction::Dropped,
        WakeSubscriptionState::Unsubscribed => WakeRouteAction::Dropped,
    };

    // A one-shot subscription (terminal exit) is consumed the first time a
    // matching wake routes to it — delivered or suppressed into the digest — so
    // it can never fire twice. An unsubscribed scope never fired, so leave it.
    if subscription.one_shot && action != WakeRouteAction::Dropped {
        consume_one_shot_subscription(runtime, &subscription).await?;
    }

    Ok(action)
}

/// The job this wake would actually reach, and whether it is dormant.
///
/// A targeted delivery names its recipient; a broadcast reaches the subscribing
/// job. Both resolve through the one dormancy rule in
/// [`cairn_db::lineage::is_dormant_thread_session`], which the direct parent-push
/// path and turn admission take as well — so no two axes can disagree about
/// whether a closed thread is listening.
async fn recipient_is_dormant(
    runtime: &AttentionRuntime,
    event: &WakeEvent,
    subscription: &WakeSubscription,
) -> bool {
    let job_id = recipient_job_id(event, subscription);
    cairn_db::lineage::is_dormant_thread_session(&runtime.private_db, job_id).await
}

/// Whether the job this wake would reach belongs to a superseded execution.
///
/// Fails CLOSED on a read error, like every other canonical-fence site: not
/// delivering a wake is recoverable, waking a fenced agent is the incident.
async fn recipient_is_fenced(
    runtime: &AttentionRuntime,
    event: &WakeEvent,
    subscription: &WakeSubscription,
) -> bool {
    let job_id = recipient_job_id(event, subscription);
    let Ok(db) = runtime.host.database_for_job(job_id).await else {
        return false;
    };
    let host = runtime.host.clone();
    let recipient_job_id = job_id.to_owned();
    let fence = db
        .read(move |conn| {
            let host = host.clone();
            let job_id = recipient_job_id.clone();
            Box::pin(async move { host.job_is_fenced(conn, &job_id).await })
        })
        .await;
    match fence {
        Ok(fenced) => fenced,
        Err(error) => {
            log::warn!(
                "canonical_fence: could not read lineage for wake recipient {}; dropping the \
                 wake rather than risking a resurrection: {error}",
                &job_id[..job_id.len().min(8)]
            );
            true
        }
    }
}

/// The job a wake would actually reach: a targeted delivery names its
/// recipient; a broadcast reaches the subscribing job.
fn recipient_job_id<'a>(event: &'a WakeEvent, subscription: &'a WakeSubscription) -> &'a str {
    match &event.delivery {
        WakeDelivery::Targeted {
            subscriber_job_id, ..
        }
        | WakeDelivery::MessageDigest {
            subscriber_job_id, ..
        } => subscriber_job_id.as_str(),
        WakeDelivery::Broadcast { .. } => subscription.job_id.as_str(),
    }
}

/// Whether the job this wake would reach belongs to a merged or closed issue.
///
/// Fails CLOSED on a read error, like the fence: not delivering a wake is
/// recoverable, while waking work on a resolved issue is what relaunched merged
/// builders. Kept a separate predicate from the fence because the two differ in
/// what a drop does to a one-shot subscription.
async fn recipient_issue_is_resolved(
    runtime: &AttentionRuntime,
    event: &WakeEvent,
    subscription: &WakeSubscription,
) -> bool {
    let job_id = recipient_job_id(event, subscription);
    let db = match runtime.host.database_for_job(job_id).await {
        Ok(db) => db,
        Err(error) => {
            log::warn!(
                "resolved-issue screen: could not route wake recipient {}; dropping the wake: {error}",
                &job_id[..job_id.len().min(8)]
            );
            return true;
        }
    };
    let host = runtime.host.clone();
    let recipient_job_id = job_id.to_owned();
    let resolved = db
        .read(move |conn| {
            let host = host.clone();
            let job_id = recipient_job_id.clone();
            Box::pin(async move { host.job_issue_is_resolved(conn, &job_id).await })
        })
        .await;
    match resolved {
        Ok(resolved) => resolved,
        Err(error) => {
            log::warn!(
                "resolved-issue screen: could not read the issue of wake recipient {}; dropping \
                 the wake rather than waking work on a resolved issue: {error}",
                &job_id[..job_id.len().min(8)]
            );
            true
        }
    }
}

async fn consume_one_shot_subscription(
    runtime: &AttentionRuntime,
    subscription: &WakeSubscription,
) -> Result<(), String> {
    let id = subscription.id.clone();
    runtime
        .private_db
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
    runtime
        .host
        .emit_change("wake_subscriptions", Some("delete"));
    Ok(())
}

async fn deliver_active_wake(
    runtime: &AttentionRuntime,
    event: &WakeEvent,
    subscription: &WakeSubscription,
    prefix: Option<&str>,
    urgency: DeliveryUrgency,
) -> Result<(), String> {
    let operator_message_id = if matches!(&event.source, WakeSource::User) {
        event.detail_uri.as_deref()
    } else {
        None
    };
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
    runtime
        .host
        .deliver(job_id, sender_name, &message, urgency, operator_message_id);
    Ok(())
}

fn format_message_with_prefix(prefix: Option<&str>, message: &str) -> String {
    match prefix {
        Some(prefix) => format!("{prefix}{message}"),
        None => message.to_string(),
    }
}
