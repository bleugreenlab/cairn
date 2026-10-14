pub use cairn_attention::wakes::matching::{
    issue_watch_stood_down, matching_subscription, mute_downgrade, subscription_accepts_fact,
    subscriptions_for_source,
};

#[cfg(test)]
pub(super) async fn matching_subscriptions_for_source(
    db: &crate::storage::LocalDb,
    source_kind: &str,
    source_ref: Option<&str>,
    fact_kind: &str,
) -> Result<Vec<super::WakeSubscription>, String> {
    cairn_attention::wakes::matching_subscriptions_for_source(
        std::sync::Arc::new(crate::attention_host::CoreRecipients),
        db,
        source_kind,
        source_ref,
        fact_kind,
    )
    .await
}
