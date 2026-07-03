use crate::common;

use cairn_core::internal::execution::accumulator::{try_accumulate, AccumulationPolicy};
use cairn_core::models::AccumulationScope;
use serde_json::json;

fn policy(every: i32, scope: AccumulationScope) -> AccumulationPolicy {
    AccumulationPolicy {
        every,
        group_by: "agentConfigId".to_string(),
        scope,
        time_window_secs: None,
    }
}

#[tokio::test]
async fn accumulator_fires_at_threshold_and_clears_state() {
    let (_temp, db) = common::migrated_db().await;
    let policy = policy(2, AccumulationScope::Project);

    let first = try_accumulate(
        &db,
        "recipe-1",
        &policy,
        &json!({"eventId": "evt-1", "agentConfigId": "builder"}),
        "project-1",
        Some("issue-1"),
    )
    .await
    .unwrap();
    assert!(first.is_none());

    let fired = try_accumulate(
        &db,
        "recipe-1",
        &policy,
        &json!({"eventId": "evt-2", "agentConfigId": "builder"}),
        "project-1",
        Some("issue-1"),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(fired["accumulated"], true);
    assert_eq!(fired["groupKey"], "builder");
    assert_eq!(fired["threshold"], 2);
    assert_eq!(fired["eventCount"], 2);
    assert_eq!(fired["events"][0]["eventId"], "evt-1");
    assert_eq!(fired["events"][1]["eventId"], "evt-2");

    assert_eq!(
        common::query_i64(&db, "SELECT COUNT(*) FROM trigger_accumulator_state")
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn accumulator_dedupes_event_ids_and_isolates_scope() {
    let (_temp, db) = common::migrated_db().await;
    let project_policy = policy(2, AccumulationScope::Project);
    let issue_policy = policy(2, AccumulationScope::Issue);

    try_accumulate(
        &db,
        "recipe-1",
        &project_policy,
        &json!({"eventId": "evt-1", "agentConfigId": "builder"}),
        "project-1",
        Some("issue-1"),
    )
    .await
    .unwrap();
    let duplicate = try_accumulate(
        &db,
        "recipe-1",
        &project_policy,
        &json!({"eventId": "evt-1", "agentConfigId": "builder"}),
        "project-1",
        Some("issue-1"),
    )
    .await
    .unwrap();
    assert!(duplicate.is_none());

    try_accumulate(
        &db,
        "recipe-1",
        &issue_policy,
        &json!({"eventId": "evt-2", "agentConfigId": "builder"}),
        "project-1",
        Some("issue-2"),
    )
    .await
    .unwrap();

    assert_eq!(
        common::query_i64(&db, "SELECT COUNT(*) FROM trigger_accumulator_state")
            .await
            .unwrap(),
        2
    );
}
