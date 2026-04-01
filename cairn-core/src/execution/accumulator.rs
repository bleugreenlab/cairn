//! Event accumulation engine for batched recipe triggers.
//!
//! When a recipe's `EventFilter` specifies `every > 1`, events are stored
//! in `trigger_accumulator_state` and the recipe only fires when the
//! threshold is reached. Events are grouped by a configurable field
//! (`group_by`) and scoped by project, issue, or global.

use diesel::prelude::*;
use serde_json::Value;
use uuid::Uuid;

use crate::diesel_models::{DbAccumulatorState, NewAccumulatorState};
use crate::models::{AccumulationScope, Recipe, RecipeNodeType};
use crate::schema::trigger_accumulator_state;

/// Accumulation policy extracted from a recipe's trigger config.
pub struct AccumulationPolicy {
    pub every: i32,
    pub group_by: String,
    pub scope: AccumulationScope,
    pub time_window_secs: Option<i64>,
}

/// Default groupBy per trigger type. Groups by the natural identity field
/// so each distinct entity gets its own counter.
fn default_group_by(trigger: &crate::models::RecipeTrigger) -> &'static str {
    match trigger {
        crate::models::RecipeTrigger::SkillCalled => "skillId",
        crate::models::RecipeTrigger::JobEnded => "agentConfigId",
        _ => "projectKey",
    }
}

/// Extract accumulation policy from recipe. Returns None if every <= 1 (immediate fire).
pub fn get_accumulation_policy(recipe: &Recipe) -> Option<AccumulationPolicy> {
    let trigger_node = recipe
        .nodes
        .iter()
        .find(|n| n.node_type == RecipeNodeType::Trigger)?;

    let trigger_config = trigger_node.trigger_config.as_ref()?;
    let filter = trigger_config.event_filter.as_ref()?;

    let every = filter.every.unwrap_or(1);
    if every <= 1 {
        return None;
    }

    let group_by = filter
        .group_by
        .clone()
        .unwrap_or_else(|| default_group_by(&trigger_config.trigger_type).to_string());

    Some(AccumulationPolicy {
        every,
        group_by,
        scope: filter
            .accumulation_scope
            .clone()
            .unwrap_or(AccumulationScope::Project),
        time_window_secs: filter.time_window_secs,
    })
}

/// Extract group key value from event payload by field name.
pub fn extract_group_key(payload: &Value, group_by: &str) -> Option<String> {
    // Try camelCase first (JSON serialized form), then snake_case
    payload
        .get(group_by)
        .or_else(|| payload.get(to_camel_case(group_by).as_str()))
        .and_then(|v| match v {
            Value::String(s) => Some(s.clone()),
            Value::Number(n) => Some(n.to_string()),
            Value::Bool(b) => Some(b.to_string()),
            Value::Null => Some("_null".to_string()),
            _ => None,
        })
}

/// Build scope key for DB lookup.
pub fn build_scope_key(
    scope: &AccumulationScope,
    project_id: &str,
    issue_id: Option<&str>,
) -> String {
    match scope {
        AccumulationScope::Global => "global".to_string(),
        AccumulationScope::Project => format!("proj:{}", project_id),
        AccumulationScope::Issue => match issue_id {
            Some(iid) => format!("issue:{}:{}", project_id, iid),
            // Fall back to project scope if no issue context
            None => format!("proj:{}", project_id),
        },
    }
}

/// Try to accumulate an event. Returns:
/// - `Ok(Some(payload))` — threshold met, accumulated payload ready
/// - `Ok(None)` — below threshold, event stored
/// - `Err` — DB or extraction error
pub fn try_accumulate(
    conn: &mut SqliteConnection,
    recipe_id: &str,
    policy: &AccumulationPolicy,
    event_payload: &Value,
    project_id: &str,
    issue_id: Option<&str>,
) -> Result<Option<Value>, String> {
    // Extract event_id for dedup
    let event_id = event_payload
        .get("eventId")
        .or_else(|| event_payload.get("event_id"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| "Event payload missing event_id/eventId field".to_string())?;

    // Extract group key
    let group_key_value = extract_group_key(event_payload, &policy.group_by)
        .ok_or_else(|| format!("Event payload missing group_by field '{}'", policy.group_by))?;

    let scope_key = build_scope_key(&policy.scope, project_id, issue_id);
    let now = chrono::Utc::now().timestamp() as i32;

    // Look up existing row
    let existing: Option<DbAccumulatorState> = trigger_accumulator_state::table
        .filter(trigger_accumulator_state::recipe_id.eq(recipe_id))
        .filter(trigger_accumulator_state::group_key.eq(&group_key_value))
        .filter(trigger_accumulator_state::scope_key.eq(&scope_key))
        .first(conn)
        .optional()
        .map_err(|e| format!("DB read error: {}", e))?;

    if let Some(row) = existing {
        // Check dedup
        let seen: Vec<String> = serde_json::from_str(&row.seen_event_ids).unwrap_or_default();
        if seen.contains(&event_id.to_string()) {
            // Already seen — skip
            return Ok(None);
        }

        // Parse stored events
        let mut events: Vec<Value> = serde_json::from_str(&row.events).unwrap_or_default();
        let mut seen_ids = seen;

        // Time window pruning
        if let Some(window) = policy.time_window_secs {
            let cutoff = now as i64 - window;
            events.retain(|e| {
                e.get("_accumulatedAt")
                    .and_then(|v| v.as_i64())
                    .map(|t| t >= cutoff)
                    .unwrap_or(true)
            });
            // Rebuild seen_ids from remaining events
            seen_ids = events
                .iter()
                .filter_map(|e| {
                    e.get("eventId")
                        .or_else(|| e.get("event_id"))
                        .and_then(|v| v.as_str())
                        .map(String::from)
                })
                .collect();
        }

        // Append new event
        let mut enriched = event_payload.clone();
        if let Value::Object(ref mut map) = enriched {
            map.insert(
                "_accumulatedAt".to_string(),
                Value::Number(serde_json::Number::from(now as i64)),
            );
        }
        events.push(enriched);
        seen_ids.push(event_id.to_string());

        let new_count = events.len() as i32;

        if new_count >= policy.every {
            // Threshold met — build accumulated payload and delete row
            let payload = build_accumulated_payload(
                &group_key_value,
                policy.every,
                &events,
                row.first_event_at as i64,
                now as i64,
            );

            diesel::delete(trigger_accumulator_state::table.find(&row.id))
                .execute(conn)
                .map_err(|e| format!("DB delete error: {}", e))?;

            Ok(Some(payload))
        } else {
            // Update row
            let events_json =
                serde_json::to_string(&events).map_err(|e| format!("JSON error: {}", e))?;
            let seen_json =
                serde_json::to_string(&seen_ids).map_err(|e| format!("JSON error: {}", e))?;

            diesel::update(trigger_accumulator_state::table.find(&row.id))
                .set((
                    trigger_accumulator_state::events.eq(&events_json),
                    trigger_accumulator_state::event_count.eq(new_count),
                    trigger_accumulator_state::seen_event_ids.eq(&seen_json),
                    trigger_accumulator_state::last_event_at.eq(now),
                ))
                .execute(conn)
                .map_err(|e| format!("DB update error: {}", e))?;

            Ok(None)
        }
    } else {
        // First event for this group — check if threshold is 1 (shouldn't reach here, but guard)
        if policy.every <= 1 {
            return Ok(Some(event_payload.clone()));
        }

        // Create new row
        let mut enriched = event_payload.clone();
        if let Value::Object(ref mut map) = enriched {
            map.insert(
                "_accumulatedAt".to_string(),
                Value::Number(serde_json::Number::from(now as i64)),
            );
        }
        let events = vec![enriched];
        let seen_ids = vec![event_id.to_string()];

        let events_json =
            serde_json::to_string(&events).map_err(|e| format!("JSON error: {}", e))?;
        let seen_json =
            serde_json::to_string(&seen_ids).map_err(|e| format!("JSON error: {}", e))?;

        let id = Uuid::new_v4().to_string();
        diesel::insert_into(trigger_accumulator_state::table)
            .values(NewAccumulatorState {
                id: &id,
                recipe_id,
                group_key: &group_key_value,
                scope_key: &scope_key,
                events: &events_json,
                event_count: 1,
                seen_event_ids: &seen_json,
                first_event_at: now,
                last_event_at: now,
                created_at: now,
            })
            .execute(conn)
            .map_err(|e| format!("DB insert error: {}", e))?;

        Ok(None)
    }
}

/// Build the accumulated payload that gets passed to the triggered execution.
fn build_accumulated_payload(
    group_key: &str,
    threshold: i32,
    events: &[Value],
    first_event_at: i64,
    last_event_at: i64,
) -> Value {
    // Strip internal _accumulatedAt from individual events
    let clean_events: Vec<Value> = events
        .iter()
        .map(|e| {
            if let Value::Object(map) = e {
                let mut clean = map.clone();
                clean.remove("_accumulatedAt");
                Value::Object(clean)
            } else {
                e.clone()
            }
        })
        .collect();

    serde_json::json!({
        "accumulated": true,
        "groupKey": group_key,
        "threshold": threshold,
        "events": clean_events,
        "eventCount": clean_events.len(),
        "firstEventAt": first_event_at,
        "lastEventAt": last_event_at,
    })
}

/// Convert snake_case to camelCase.
fn to_camel_case(s: &str) -> String {
    let mut result = String::new();
    let mut capitalize_next = false;
    for c in s.chars() {
        if c == '_' {
            capitalize_next = true;
        } else if capitalize_next {
            result.push(c.to_ascii_uppercase());
            capitalize_next = false;
        } else {
            result.push(c);
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::EventFilter;

    // ---- extract_group_key ----

    #[test]
    fn extract_group_key_string_field() {
        let payload = serde_json::json!({"skillId": "code-review", "status": "complete"});
        assert_eq!(
            extract_group_key(&payload, "skillId"),
            Some("code-review".to_string())
        );
    }

    #[test]
    fn extract_group_key_snake_case_lookup() {
        // field in payload is camelCase, but group_by is snake_case
        let payload = serde_json::json!({"skillId": "code-review"});
        assert_eq!(
            extract_group_key(&payload, "skill_id"),
            Some("code-review".to_string())
        );
    }

    #[test]
    fn extract_group_key_number_field() {
        let payload = serde_json::json!({"issueNumber": 42});
        assert_eq!(
            extract_group_key(&payload, "issueNumber"),
            Some("42".to_string())
        );
    }

    #[test]
    fn extract_group_key_missing_field() {
        let payload = serde_json::json!({"skillId": "code-review"});
        assert_eq!(extract_group_key(&payload, "nonexistent"), None);
    }

    #[test]
    fn extract_group_key_null_field() {
        let payload = serde_json::json!({"skillId": null});
        assert_eq!(
            extract_group_key(&payload, "skillId"),
            Some("_null".to_string())
        );
    }

    // ---- build_scope_key ----

    #[test]
    fn scope_key_global() {
        assert_eq!(
            build_scope_key(&AccumulationScope::Global, "proj-1", None),
            "global"
        );
    }

    #[test]
    fn scope_key_project() {
        assert_eq!(
            build_scope_key(&AccumulationScope::Project, "proj-1", None),
            "proj:proj-1"
        );
    }

    #[test]
    fn scope_key_issue() {
        assert_eq!(
            build_scope_key(&AccumulationScope::Issue, "proj-1", Some("issue-1")),
            "issue:proj-1:issue-1"
        );
    }

    #[test]
    fn scope_key_issue_fallback_to_project() {
        // Issue scope but no issue_id — falls back to project scope
        assert_eq!(
            build_scope_key(&AccumulationScope::Issue, "proj-1", None),
            "proj:proj-1"
        );
    }

    // ---- to_camel_case ----

    #[test]
    fn camel_case_conversion() {
        assert_eq!(to_camel_case("skill_id"), "skillId");
        assert_eq!(to_camel_case("project_key"), "projectKey");
        assert_eq!(to_camel_case("already"), "already");
        assert_eq!(to_camel_case("a_b_c"), "aBC");
    }

    // ---- get_accumulation_policy ----

    #[test]
    fn policy_none_when_no_every() {
        let recipe = make_recipe(None);
        assert!(get_accumulation_policy(&recipe).is_none());
    }

    #[test]
    fn policy_none_when_every_is_1() {
        let recipe = make_recipe(Some(EventFilter {
            job_status: None,
            skill_ids: None,
            node_filter: None,
            every: Some(1),
            group_by: Some("skillId".to_string()),
            accumulation_scope: None,
            time_window_secs: None,
        }));
        assert!(get_accumulation_policy(&recipe).is_none());
    }

    #[test]
    fn policy_some_when_every_gt_1() {
        let recipe = make_recipe(Some(EventFilter {
            job_status: None,
            skill_ids: Some(vec!["code-review".to_string()]),
            node_filter: None,
            every: Some(4),
            group_by: Some("skillId".to_string()),
            accumulation_scope: None,
            time_window_secs: None,
        }));
        let policy = get_accumulation_policy(&recipe).unwrap();
        assert_eq!(policy.every, 4);
        assert_eq!(policy.group_by, "skillId");
        assert_eq!(policy.scope, AccumulationScope::Project);
    }

    #[test]
    fn policy_infers_group_by_when_absent() {
        let recipe = make_recipe(Some(EventFilter {
            job_status: None,
            skill_ids: None,
            node_filter: None,
            every: Some(4),
            group_by: None,
            accumulation_scope: None,
            time_window_secs: None,
        }));
        // group_by absent → inferred from trigger type (skill_called → skillId)
        let policy = get_accumulation_policy(&recipe).unwrap();
        assert_eq!(policy.every, 4);
        assert_eq!(policy.group_by, "skillId");
    }

    #[test]
    fn policy_respects_scope_override() {
        let recipe = make_recipe(Some(EventFilter {
            job_status: None,
            skill_ids: None,
            node_filter: None,
            every: Some(3),
            group_by: Some("status".to_string()),
            accumulation_scope: Some(AccumulationScope::Global),
            time_window_secs: Some(3600),
        }));
        let policy = get_accumulation_policy(&recipe).unwrap();
        assert_eq!(policy.scope, AccumulationScope::Global);
        assert_eq!(policy.time_window_secs, Some(3600));
    }

    // ---- build_accumulated_payload ----

    #[test]
    fn accumulated_payload_structure() {
        let events = vec![
            serde_json::json!({"eventId": "e1", "skillId": "cr", "_accumulatedAt": 100}),
            serde_json::json!({"eventId": "e2", "skillId": "cr", "_accumulatedAt": 200}),
        ];
        let payload = build_accumulated_payload("cr", 2, &events, 100, 200);

        assert_eq!(payload["accumulated"], true);
        assert_eq!(payload["groupKey"], "cr");
        assert_eq!(payload["threshold"], 2);
        assert_eq!(payload["eventCount"], 2);
        assert_eq!(payload["firstEventAt"], 100);
        assert_eq!(payload["lastEventAt"], 200);

        // _accumulatedAt should be stripped from individual events
        let evts = payload["events"].as_array().unwrap();
        assert!(evts[0].get("_accumulatedAt").is_none());
        assert_eq!(evts[0]["eventId"], "e1");
    }

    // ---- EventFilter serde backward compat ----

    #[test]
    fn event_filter_backward_compat_no_new_fields() {
        // Old-style JSON without accumulation fields should deserialize fine
        let json = r#"{"jobStatus":["complete"]}"#;
        let filter: EventFilter = serde_json::from_str(json).unwrap();
        assert_eq!(filter.job_status, Some(vec!["complete".to_string()]));
        assert!(filter.every.is_none());
        assert!(filter.group_by.is_none());
        assert!(filter.accumulation_scope.is_none());
        assert!(filter.time_window_secs.is_none());
    }

    #[test]
    fn event_filter_with_accumulation_roundtrip() {
        let filter = EventFilter {
            job_status: None,
            skill_ids: Some(vec!["code-review".to_string()]),
            node_filter: None,
            every: Some(4),
            group_by: Some("skillId".to_string()),
            accumulation_scope: Some(AccumulationScope::Issue),
            time_window_secs: Some(7200),
        };
        let json = serde_json::to_string(&filter).unwrap();
        let parsed: EventFilter = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.every, Some(4));
        assert_eq!(parsed.group_by, Some("skillId".to_string()));
        assert_eq!(parsed.accumulation_scope, Some(AccumulationScope::Issue));
        assert_eq!(parsed.time_window_secs, Some(7200));
    }

    // ---- Helpers ----

    fn make_recipe(event_filter: Option<EventFilter>) -> Recipe {
        use crate::models::{NodePosition, RecipeNode, RecipeTrigger, TriggerConfig};
        Recipe {
            id: "r1".to_string(),
            name: "Test".to_string(),
            description: None,
            trigger: RecipeTrigger::SkillCalled,
            workspace_id: None,
            project_id: None,
            is_default: false,
            version: 1,
            parent_recipe_id: None,
            child_recipe_id: None,
            nodes: vec![RecipeNode {
                id: "t1".to_string(),
                node_type: RecipeNodeType::Trigger,
                name: "Trigger".to_string(),
                position: NodePosition { x: 0.0, y: 0.0 },
                parent_id: None,
                trigger_config: Some(TriggerConfig {
                    trigger_type: RecipeTrigger::SkillCalled,
                    scope: None,
                    schedule_config: None,
                    event_filter,
                }),
                agent_config: None,
                action_config: None,
                checkpoint_config: None,
                artifact_config: None,
                condition_config: None,
                context_config: None,
            }],
            edges: vec![],
            created_at: 0,
            updated_at: 0,
        }
    }

    // =========================================================================
    // try_accumulate — DB-backed state machine tests
    // =========================================================================

    fn test_conn() -> diesel::SqliteConnection {
        crate::test_utils::test_diesel_conn()
    }

    fn make_policy(every: i32) -> AccumulationPolicy {
        AccumulationPolicy {
            every,
            group_by: "skillId".to_string(),
            scope: AccumulationScope::Project,
            time_window_secs: None,
        }
    }

    fn make_event(event_id: &str, skill_id: &str) -> Value {
        serde_json::json!({
            "eventId": event_id,
            "skillId": skill_id,
            "status": "complete"
        })
    }

    #[test]
    fn try_accumulate_first_event_below_threshold() {
        let mut conn = test_conn();
        let policy = make_policy(3);
        let event = make_event("e1", "code-review");

        let result =
            try_accumulate(&mut conn, "recipe-1", &policy, &event, "proj-1", None).unwrap();
        assert!(result.is_none(), "First event should not fire (need 3)");

        // Verify row was inserted
        let count: i64 = trigger_accumulator_state::table
            .count()
            .get_result(&mut conn)
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn try_accumulate_fires_at_threshold() {
        let mut conn = test_conn();
        let policy = make_policy(3);

        // Events 1 and 2: stored
        assert!(try_accumulate(
            &mut conn,
            "recipe-1",
            &policy,
            &make_event("e1", "code-review"),
            "proj-1",
            None
        )
        .unwrap()
        .is_none());
        assert!(try_accumulate(
            &mut conn,
            "recipe-1",
            &policy,
            &make_event("e2", "code-review"),
            "proj-1",
            None
        )
        .unwrap()
        .is_none());

        // Event 3: threshold met
        let result = try_accumulate(
            &mut conn,
            "recipe-1",
            &policy,
            &make_event("e3", "code-review"),
            "proj-1",
            None,
        )
        .unwrap();

        let payload = result.expect("Should fire at threshold");
        assert_eq!(payload["accumulated"], true);
        assert_eq!(payload["threshold"], 3);
        assert_eq!(payload["eventCount"], 3);
        assert_eq!(payload["groupKey"], "code-review");

        let events = payload["events"].as_array().unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0]["eventId"], "e1");
        assert_eq!(events[2]["eventId"], "e3");

        // DB row should be deleted after firing
        let count: i64 = trigger_accumulator_state::table
            .count()
            .get_result(&mut conn)
            .unwrap();
        assert_eq!(count, 0, "Row should be deleted after threshold met");
    }

    #[test]
    fn try_accumulate_dedup_same_event_id() {
        let mut conn = test_conn();
        let policy = make_policy(3);

        // Insert event e1
        try_accumulate(
            &mut conn,
            "recipe-1",
            &policy,
            &make_event("e1", "code-review"),
            "proj-1",
            None,
        )
        .unwrap();

        // Duplicate e1 — should be silently ignored
        let result = try_accumulate(
            &mut conn,
            "recipe-1",
            &policy,
            &make_event("e1", "code-review"),
            "proj-1",
            None,
        )
        .unwrap();
        assert!(result.is_none());

        // Verify count is still 1, not 2
        let row: DbAccumulatorState = trigger_accumulator_state::table.first(&mut conn).unwrap();
        assert_eq!(row.event_count, 1, "Duplicate should not increment count");
    }

    #[test]
    fn try_accumulate_separate_groups() {
        let mut conn = test_conn();
        let policy = make_policy(2);

        // Two events for different skills — separate groups
        try_accumulate(
            &mut conn,
            "recipe-1",
            &policy,
            &make_event("e1", "code-review"),
            "proj-1",
            None,
        )
        .unwrap();
        try_accumulate(
            &mut conn,
            "recipe-1",
            &policy,
            &make_event("e2", "testing"),
            "proj-1",
            None,
        )
        .unwrap();

        // Two separate rows
        let count: i64 = trigger_accumulator_state::table
            .count()
            .get_result(&mut conn)
            .unwrap();
        assert_eq!(count, 2, "Different group keys should create separate rows");

        // Second event for code-review should fire
        let result = try_accumulate(
            &mut conn,
            "recipe-1",
            &policy,
            &make_event("e3", "code-review"),
            "proj-1",
            None,
        )
        .unwrap();
        assert!(result.is_some(), "Second code-review event should fire");

        // Only testing group should remain
        let remaining: i64 = trigger_accumulator_state::table
            .count()
            .get_result(&mut conn)
            .unwrap();
        assert_eq!(remaining, 1, "Only the testing group should remain");
    }

    #[test]
    fn try_accumulate_time_window_prunes_stale() {
        let mut conn = test_conn();
        let policy = AccumulationPolicy {
            every: 3,
            group_by: "skillId".to_string(),
            scope: AccumulationScope::Project,
            time_window_secs: Some(60), // 60-second window
        };

        // Insert first event normally
        try_accumulate(
            &mut conn,
            "recipe-1",
            &policy,
            &make_event("e1", "code-review"),
            "proj-1",
            None,
        )
        .unwrap();

        // Manually backdate the stored event's _accumulatedAt to make it stale
        let row: DbAccumulatorState = trigger_accumulator_state::table.first(&mut conn).unwrap();
        let mut events: Vec<Value> = serde_json::from_str(&row.events).unwrap();
        if let Value::Object(ref mut map) = events[0] {
            let stale_time = chrono::Utc::now().timestamp() - 120; // 2 minutes ago
            map.insert(
                "_accumulatedAt".to_string(),
                Value::Number(serde_json::Number::from(stale_time)),
            );
        }
        let events_json = serde_json::to_string(&events).unwrap();
        diesel::update(trigger_accumulator_state::table.find(&row.id))
            .set(trigger_accumulator_state::events.eq(&events_json))
            .execute(&mut conn)
            .unwrap();

        // Add second event — the stale first event should be pruned
        try_accumulate(
            &mut conn,
            "recipe-1",
            &policy,
            &make_event("e2", "code-review"),
            "proj-1",
            None,
        )
        .unwrap();

        // Check: should have only 1 event (e2), since e1 was pruned
        let row: DbAccumulatorState = trigger_accumulator_state::table.first(&mut conn).unwrap();
        assert_eq!(row.event_count, 1, "Stale event should have been pruned");
        let events: Vec<Value> = serde_json::from_str(&row.events).unwrap();
        assert_eq!(events[0]["eventId"], "e2");
    }

    #[test]
    fn try_accumulate_scope_isolation() {
        let mut conn = test_conn();
        let policy = AccumulationPolicy {
            every: 2,
            group_by: "skillId".to_string(),
            scope: AccumulationScope::Issue,
            time_window_secs: None,
        };

        // Same skill, same recipe, different issues — should not mix
        try_accumulate(
            &mut conn,
            "recipe-1",
            &policy,
            &make_event("e1", "code-review"),
            "proj-1",
            Some("issue-1"),
        )
        .unwrap();
        try_accumulate(
            &mut conn,
            "recipe-1",
            &policy,
            &make_event("e2", "code-review"),
            "proj-1",
            Some("issue-2"),
        )
        .unwrap();

        // Two rows, one per issue
        let count: i64 = trigger_accumulator_state::table
            .count()
            .get_result(&mut conn)
            .unwrap();
        assert_eq!(count, 2, "Different issues should create separate rows");

        // Second event for issue-1 should fire
        let result = try_accumulate(
            &mut conn,
            "recipe-1",
            &policy,
            &make_event("e3", "code-review"),
            "proj-1",
            Some("issue-1"),
        )
        .unwrap();
        assert!(result.is_some(), "issue-1 should hit threshold");

        // issue-2 still has 1 event
        let remaining: i64 = trigger_accumulator_state::table
            .count()
            .get_result(&mut conn)
            .unwrap();
        assert_eq!(remaining, 1);
    }

    #[test]
    fn try_accumulate_missing_event_id_is_error() {
        let mut conn = test_conn();
        let policy = make_policy(2);

        let bad_event = serde_json::json!({"skillId": "code-review"});
        let result = try_accumulate(&mut conn, "recipe-1", &policy, &bad_event, "proj-1", None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("event_id"));
    }

    #[test]
    fn try_accumulate_missing_group_by_field_is_error() {
        let mut conn = test_conn();
        let policy = make_policy(2);

        // Has eventId but missing skillId (the group_by field)
        let bad_event = serde_json::json!({"eventId": "e1", "otherField": "value"});
        let result = try_accumulate(&mut conn, "recipe-1", &policy, &bad_event, "proj-1", None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("group_by"));
    }

    #[test]
    fn try_accumulate_strips_internal_fields_from_output() {
        let mut conn = test_conn();
        let policy = make_policy(2);

        try_accumulate(
            &mut conn,
            "recipe-1",
            &policy,
            &make_event("e1", "code-review"),
            "proj-1",
            None,
        )
        .unwrap();
        let result = try_accumulate(
            &mut conn,
            "recipe-1",
            &policy,
            &make_event("e2", "code-review"),
            "proj-1",
            None,
        )
        .unwrap()
        .unwrap();

        // _accumulatedAt should be stripped from the events in the output
        for event in result["events"].as_array().unwrap() {
            assert!(
                event.get("_accumulatedAt").is_none(),
                "_accumulatedAt should be stripped from output events"
            );
        }
    }
}
