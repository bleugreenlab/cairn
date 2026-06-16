//! Event accumulation engine for batched recipe triggers.
//!
//! When a recipe's `EventFilter` specifies `every > 1`, events are stored
//! in `trigger_accumulator_state` and the recipe only fires when the
//! threshold is reached. Events are grouped by a configurable field
//! (`group_by`) and scoped by project, issue, or global.

use serde_json::Value;
use turso::params;
use uuid::Uuid;

use crate::models::{AccumulationScope, Recipe, RecipeNodeType};
use crate::storage::{DbError, DbResult, LocalDb, RowExt};

/// Accumulation policy extracted from a recipe's trigger config.
pub struct AccumulationPolicy {
    pub every: i32,
    pub group_by: String,
    pub scope: AccumulationScope,
    pub time_window_secs: Option<i64>,
}

struct AccumulatorState {
    id: String,
    events: String,
    seen_event_ids: String,
    first_event_at: i32,
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
pub async fn try_accumulate(
    db: &LocalDb,
    recipe_id: &str,
    policy: &AccumulationPolicy,
    event_payload: &Value,
    project_id: &str,
    issue_id: Option<&str>,
) -> Result<Option<Value>, String> {
    // Extract event_id for dedup
    let event_id = event_id_from_payload(event_payload)
        .ok_or_else(|| "Event payload missing event_id/eventId field".to_string())?;

    // Extract group key
    let group_key_value = extract_group_key(event_payload, &policy.group_by)
        .ok_or_else(|| format!("Event payload missing group_by field '{}'", policy.group_by))?;

    let scope_key = build_scope_key(&policy.scope, project_id, issue_id);
    let recipe_id = recipe_id.to_string();
    let event_id = event_id.to_string();
    let event_payload = event_payload.clone();
    let every = policy.every;
    let time_window_secs = policy.time_window_secs;

    db.write(|conn| {
        let recipe_id = recipe_id.clone();
        let event_id = event_id.clone();
        let group_key_value = group_key_value.clone();
        let scope_key = scope_key.clone();
        let event_payload = event_payload.clone();
        Box::pin(async move {
            let now = chrono::Utc::now().timestamp() as i32;
            let existing =
                load_accumulator_state(conn, &recipe_id, &group_key_value, &scope_key).await?;

            if let Some(row) = existing {
                let seen: Vec<String> =
                    serde_json::from_str(&row.seen_event_ids).unwrap_or_default();
                if seen.contains(&event_id) {
                    return Ok(None);
                }

                let mut events: Vec<Value> = serde_json::from_str(&row.events).unwrap_or_default();
                let mut seen_ids = seen;

                if let Some(window) = time_window_secs {
                    let cutoff = now as i64 - window;
                    events.retain(|event| {
                        event
                            .get("_accumulatedAt")
                            .and_then(|value| value.as_i64())
                            .map(|timestamp| timestamp >= cutoff)
                            .unwrap_or(true)
                    });
                    seen_ids = events
                        .iter()
                        .filter_map(event_id_from_payload)
                        .map(ToOwned::to_owned)
                        .collect();
                }

                events.push(enrich_event(event_payload.clone(), now));
                seen_ids.push(event_id.clone());

                let new_count = events.len() as i32;
                if new_count >= every {
                    let payload = build_accumulated_payload(
                        &group_key_value,
                        every,
                        &events,
                        row.first_event_at as i64,
                        now as i64,
                    );
                    conn.execute(
                        "DELETE FROM trigger_accumulator_state WHERE id = ?1",
                        params![row.id.as_str()],
                    )
                    .await?;
                    Ok(Some(payload))
                } else {
                    let events_json = serde_json::to_string(&events).map_err(json_error)?;
                    let seen_json = serde_json::to_string(&seen_ids).map_err(json_error)?;
                    conn.execute(
                        "UPDATE trigger_accumulator_state
                         SET events = ?1, event_count = ?2, seen_event_ids = ?3,
                             last_event_at = ?4
                         WHERE id = ?5",
                        params![
                            events_json.as_str(),
                            new_count,
                            seen_json.as_str(),
                            now,
                            row.id.as_str()
                        ],
                    )
                    .await?;
                    Ok(None)
                }
            } else {
                if every <= 1 {
                    return Ok(Some(event_payload.clone()));
                }

                let events = vec![enrich_event(event_payload.clone(), now)];
                let seen_ids = vec![event_id.clone()];
                let events_json = serde_json::to_string(&events).map_err(json_error)?;
                let seen_json = serde_json::to_string(&seen_ids).map_err(json_error)?;
                let id = Uuid::new_v4().to_string();

                conn.execute(
                    "INSERT INTO trigger_accumulator_state(
                        id, recipe_id, group_key, scope_key, events, event_count,
                        seen_event_ids, first_event_at, last_event_at, created_at
                     )
                     VALUES (?1, ?2, ?3, ?4, ?5, 1, ?6, ?7, ?8, ?9)",
                    params![
                        id.as_str(),
                        recipe_id.as_str(),
                        group_key_value.as_str(),
                        scope_key.as_str(),
                        events_json.as_str(),
                        seen_json.as_str(),
                        now,
                        now,
                        now
                    ],
                )
                .await?;

                Ok(None)
            }
        })
    })
    .await
    .map_err(|e| e.to_string())
}

async fn load_accumulator_state(
    conn: &turso::Connection,
    recipe_id: &str,
    group_key: &str,
    scope_key: &str,
) -> DbResult<Option<AccumulatorState>> {
    let mut rows = conn
        .query(
            "SELECT id, events, seen_event_ids, first_event_at
             FROM trigger_accumulator_state
             WHERE recipe_id = ?1 AND group_key = ?2 AND scope_key = ?3
             LIMIT 1",
            params![recipe_id, group_key, scope_key],
        )
        .await?;

    rows.next()
        .await?
        .map(|row| {
            Ok(AccumulatorState {
                id: row.text(0)?,
                events: row.text(1)?,
                seen_event_ids: row.text(2)?,
                first_event_at: row.i64(3)? as i32,
            })
        })
        .transpose()
}

fn enrich_event(mut event: Value, now: i32) -> Value {
    if let Value::Object(ref mut map) = event {
        map.insert(
            "_accumulatedAt".to_string(),
            Value::Number(serde_json::Number::from(now as i64)),
        );
    }
    event
}

fn event_id_from_payload(event: &Value) -> Option<&str> {
    event
        .get("eventId")
        .or_else(|| event.get("event_id"))
        .and_then(|value| value.as_str())
}

fn json_error(error: serde_json::Error) -> DbError {
    DbError::internal(format!("JSON error: {error}"))
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
        assert_eq!(
            build_scope_key(&AccumulationScope::Issue, "proj-1", None),
            "proj:proj-1"
        );
    }

    #[test]
    fn camel_case_conversion() {
        assert_eq!(to_camel_case("skill_id"), "skillId");
        assert_eq!(to_camel_case("project_key"), "projectKey");
        assert_eq!(to_camel_case("already"), "already");
        assert_eq!(to_camel_case("a_b_c"), "aBC");
    }

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

        let events = payload["events"].as_array().unwrap();
        assert!(events[0].get("_accumulatedAt").is_none());
        assert_eq!(events[0]["eventId"], "e1");
    }

    #[test]
    fn event_filter_backward_compat_no_new_fields() {
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
}
