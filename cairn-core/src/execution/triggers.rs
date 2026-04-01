//! Recipe matching and event dispatch for event-driven triggers.
//!
//! When a job completes or a skill is called, this module:
//! 1. Checks anti-recursion guards (event-triggered executions don't fire further events)
//! 2. Finds matching recipes via `EventFilter` criteria
//! 3. Starts new executions for each matched recipe
//! 4. Enqueues DAG advancement on the effect queue

use diesel::prelude::*;

use crate::diesel_models::NewTriggerSource;
use crate::execution::creation::create_jobs_for_execution;
use crate::execution::Initiator;
use crate::models::{
    ExecutionSnapshot, JobEndedEvent, Recipe, RecipeNodeType, RecipeTrigger, SkillCalledEvent,
    TriggerScope, TriggerType,
};
use crate::orchestrator::Orchestrator;
use crate::schema::execution_trigger_sources;

use super::accumulator;
use super::recipe::start_event_triggered_execution;

/// Check if a single recipe matches a JobEnded event.
fn matches_job_ended(recipe: &Recipe, event: &JobEndedEvent) -> bool {
    if recipe.trigger != RecipeTrigger::JobEnded {
        return false;
    }

    // Get event filter from trigger node
    let filter = recipe
        .nodes
        .iter()
        .find(|n| n.node_type == RecipeNodeType::Trigger)
        .and_then(|n| n.trigger_config.as_ref())
        .and_then(|tc| tc.event_filter.as_ref());

    if let Some(filter) = filter {
        // Check job_status filter (empty list = no filter)
        if let Some(ref statuses) = filter.job_status {
            if !statuses.is_empty() && !statuses.contains(&event.status) {
                return false;
            }
        }
        // Check node_filter (agent allow/exclude list)
        if let Some(ref agent_filter) = filter.node_filter {
            if !agent_filter.ids.is_empty() {
                let agent_id = event.agent_config_id.as_deref().unwrap_or("");
                let in_list = agent_filter.ids.iter().any(|id| id == agent_id);
                match agent_filter.mode {
                    crate::models::AgentFilterMode::Allow => {
                        if !in_list {
                            return false;
                        }
                    }
                    crate::models::AgentFilterMode::Exclude => {
                        if in_list {
                            return false;
                        }
                    }
                }
            }
        }
    }

    true
}

/// Find all recipes that match a JobEnded event.
pub fn find_recipes_for_job_ended(
    orch: &Orchestrator,
    project_id: &str,
    event: &JobEndedEvent,
) -> Result<Vec<Recipe>, String> {
    let recipes = orch.list_recipes_for_context(project_id)?;
    Ok(recipes
        .into_iter()
        .filter(|r| matches_job_ended(r, event))
        .collect())
}

/// Check if a single recipe matches a SkillCalled event.
fn matches_skill_called(recipe: &Recipe, event: &SkillCalledEvent) -> bool {
    if recipe.trigger != RecipeTrigger::SkillCalled {
        log::debug!(
            "trigger-match: recipe '{}' trigger={}, want SkillCalled — skip",
            recipe.name,
            recipe.trigger,
        );
        return false;
    }

    let trigger_node = recipe
        .nodes
        .iter()
        .find(|n| n.node_type == RecipeNodeType::Trigger);

    let filter = trigger_node
        .and_then(|n| n.trigger_config.as_ref())
        .and_then(|tc| tc.event_filter.as_ref());

    log::info!(
        "trigger-match: recipe '{}' has_trigger_node={} has_filter={} skill='{}'",
        recipe.name,
        trigger_node.is_some(),
        filter.is_some(),
        event.skill_id,
    );

    if let Some(filter) = filter {
        if let Some(ref skill_ids) = filter.skill_ids {
            // Empty list = no filter (treat same as None)
            if !skill_ids.is_empty() && !skill_ids.contains(&event.skill_id) {
                log::info!(
                    "trigger-match: recipe '{}' filter rejects skill '{}' (allowed: {:?})",
                    recipe.name,
                    event.skill_id,
                    skill_ids,
                );
                return false;
            }
        }
    }

    true
}

/// Find all recipes that match a SkillCalled event.
pub fn find_recipes_for_skill_called(
    orch: &Orchestrator,
    project_id: &str,
    event: &SkillCalledEvent,
) -> Result<Vec<Recipe>, String> {
    let recipes = orch.list_recipes_for_context(project_id)?;
    Ok(recipes
        .into_iter()
        .filter(|r| matches_skill_called(r, event))
        .collect())
}

/// Check if an execution was itself event-triggered (anti-recursion guard).
///
/// Event-triggered executions never emit further events. Only `Manual` and
/// `Schedule` executions are allowed to trigger event-based recipes.
pub fn is_event_triggered(snapshot: &ExecutionSnapshot) -> bool {
    matches!(
        snapshot.trigger_context.trigger_type,
        TriggerType::JobEnded | TriggerType::SkillCalled
    )
}

/// Dispatch matched recipes for a given event.
///
/// For each recipe, starts an execution, creates jobs, and enqueues
/// `AdvanceDag` so the host effect drainer picks up newly ready jobs.
pub fn dispatch_event_recipes(
    orch: &Orchestrator,
    recipes: Vec<Recipe>,
    trigger_type: TriggerType,
    event_payload: serde_json::Value,
    event_issue_id: Option<&str>,
    project_id: &str,
    initiator: Option<Initiator>,
) {
    for recipe in recipes {
        // Check if this recipe uses accumulation
        let effective_payload = if let Some(policy) = accumulator::get_accumulation_policy(&recipe)
        {
            let Ok(mut conn) = orch.db.conn.lock() else {
                log::error!("Failed to lock DB for accumulator");
                continue;
            };
            match accumulator::try_accumulate(
                &mut conn,
                &recipe.id,
                &policy,
                &event_payload,
                project_id,
                event_issue_id,
            ) {
                Ok(Some(accumulated)) => accumulated,
                Ok(None) => {
                    log::info!(
                        "Accumulator for '{}': event stored ({}/{})",
                        recipe.name,
                        // Can't easily get current count here, just log the threshold
                        "?",
                        policy.every
                    );
                    continue;
                }
                Err(e) => {
                    log::error!("Accumulator error for recipe '{}': {}", recipe.name, e);
                    continue;
                }
            }
        } else {
            event_payload.clone()
        };

        let scope = recipe.scope();
        let issue_id = match scope {
            TriggerScope::Issue => {
                // Issue-scoped recipes require an issue context from the event.
                // Skip silently when the source event has no issue — starting
                // without one would create a project-scoped execution that
                // downstream nodes aren't authored to handle.
                match event_issue_id {
                    Some(id) => Some(id),
                    None => {
                        log::info!(
                            "Skipping issue-scoped recipe '{}': source event has no issue_id",
                            recipe.name
                        );
                        continue;
                    }
                }
            }
            TriggerScope::Project => None,
        };

        match start_event_triggered_execution(
            orch,
            &recipe,
            trigger_type.clone(),
            effective_payload.clone(),
            issue_id,
            project_id,
            initiator.clone(),
        ) {
            Ok(execution) => {
                // Create jobs for the execution
                if let Ok(mut conn) = orch.db.conn.lock() {
                    let job_issue_id = issue_id.unwrap_or("");
                    if let Err(e) = create_jobs_for_execution(
                        &mut conn,
                        &execution.id,
                        job_issue_id,
                        project_id,
                    ) {
                        log::error!(
                            "Failed to create jobs for event-triggered execution {}: {}",
                            &execution.id[..8],
                            e
                        );
                        continue;
                    }

                    // Insert trigger source junction rows
                    let source_job_ids = extract_source_job_ids(&effective_payload);
                    let now = chrono::Utc::now().timestamp() as i32;
                    for job_id in &source_job_ids {
                        if let Err(e) = diesel::insert_into(execution_trigger_sources::table)
                            .values(NewTriggerSource {
                                id: &uuid::Uuid::new_v4().to_string(),
                                source_job_id: job_id,
                                triggered_execution_id: &execution.id,
                                created_at: now,
                            })
                            .execute(&mut *conn)
                        {
                            log::warn!(
                                "Failed to insert trigger source for execution {}: {}",
                                &execution.id[..8],
                                e
                            );
                        }
                    }
                }

                // Notify frontend so execution/job lists refresh
                let _ = orch.services.emitter.emit(
                    "db-change",
                    serde_json::json!({"table": "executions", "action": "insert"}),
                );
                let _ = orch.services.emitter.emit(
                    "db-change",
                    serde_json::json!({"table": "jobs", "action": "insert"}),
                );
                let _ = orch.services.emitter.emit(
                    "db-change",
                    serde_json::json!({"table": "execution_trigger_sources", "action": "insert"}),
                );

                if let Some(ref tx) = orch.effect_tx {
                    let _ = tx.send(crate::effects::types::WorkflowEffect::AdvanceDag {
                        execution_id: execution.id.clone(),
                        outbox_entry_id: None,
                    });
                } else {
                    log::error!(
                        "No effect_tx configured — cannot advance event-triggered execution {}",
                        &execution.id[..execution.id.len().min(8)]
                    );
                }

                log::info!(
                    "Event-triggered recipe '{}' started (execution {})",
                    recipe.name,
                    &execution.id[..8]
                );
            }
            Err(e) => {
                log::error!(
                    "Failed to start event-triggered recipe '{}': {}",
                    recipe.name,
                    e
                );
            }
        }
    }
}

/// Extract source job IDs from an event payload.
///
/// Handles both single-event payloads (with `sourceJobId`) and
/// accumulated payloads (array of events, each with `sourceJobId`).
fn extract_source_job_ids(payload: &serde_json::Value) -> Vec<String> {
    let mut ids = Vec::new();

    // Single event: { "sourceJobId": "..." }
    if let Some(id) = payload.get("sourceJobId").and_then(|v| v.as_str()) {
        ids.push(id.to_string());
        return ids;
    }

    // Accumulated payload: { "events": [{ "sourceJobId": "..." }, ...] }
    if let Some(events) = payload.get("events").and_then(|v| v.as_array()) {
        let mut seen = std::collections::HashSet::new();
        for event in events {
            if let Some(id) = event.get("sourceJobId").and_then(|v| v.as_str()) {
                if seen.insert(id.to_string()) {
                    ids.push(id.to_string());
                }
            }
        }
    }

    ids
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{
        AgentFilter, AgentFilterMode, EventFilter, NodePosition, RecipeNode, TriggerConfig,
    };

    fn make_job_ended_recipe(name: &str, filter: Option<EventFilter>) -> Recipe {
        Recipe {
            id: format!("recipe-{}", name),
            name: name.to_string(),
            description: None,
            trigger: RecipeTrigger::JobEnded,
            workspace_id: None,
            project_id: None,
            is_default: false,
            version: 1,
            parent_recipe_id: None,
            child_recipe_id: None,
            nodes: vec![RecipeNode {
                id: "trigger-1".to_string(),
                node_type: RecipeNodeType::Trigger,
                name: "Trigger".to_string(),
                position: NodePosition { x: 0.0, y: 0.0 },
                parent_id: None,
                trigger_config: Some(TriggerConfig {
                    trigger_type: RecipeTrigger::JobEnded,
                    scope: None,
                    schedule_config: None,
                    event_filter: filter,
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

    fn make_skill_called_recipe(name: &str, filter: Option<EventFilter>) -> Recipe {
        Recipe {
            id: format!("recipe-{}", name),
            name: name.to_string(),
            description: None,
            trigger: RecipeTrigger::SkillCalled,
            workspace_id: None,
            project_id: None,
            is_default: false,
            version: 1,
            parent_recipe_id: None,
            child_recipe_id: None,
            nodes: vec![RecipeNode {
                id: "trigger-1".to_string(),
                node_type: RecipeNodeType::Trigger,
                name: "Trigger".to_string(),
                position: NodePosition { x: 0.0, y: 0.0 },
                parent_id: None,
                trigger_config: Some(TriggerConfig {
                    trigger_type: RecipeTrigger::SkillCalled,
                    scope: None,
                    schedule_config: None,
                    event_filter: filter,
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

    fn make_job_ended_event(status: &str, agent_config_id: Option<&str>) -> JobEndedEvent {
        JobEndedEvent {
            event_id: "test-job-id".to_string(),
            source_job_id: "test-job-id".to_string(),
            project_key: "TST".to_string(),
            issue_number: Some(1),
            execution_seq: Some(1),
            agent_config_id: agent_config_id.map(String::from),
            node_name: Some("Builder".to_string()),
            status: status.to_string(),
            completed_at: 0,
            transcript_uri: Some("cairn://TST/1/1/Builder/chat".to_string()),
        }
    }

    fn make_skill_called_event(skill_id: &str) -> SkillCalledEvent {
        SkillCalledEvent {
            event_id: format!("test-run:{}", skill_id),
            source_job_id: "test-job-id".to_string(),
            skill_id: skill_id.to_string(),
            skill_name: "Test Skill".to_string(),
            project_key: "TST".to_string(),
            issue_number: Some(1),
            execution_seq: Some(1),
            node_name: Some("Builder".to_string()),
            transcript_uri: Some("cairn://TST/1/1/Builder/chat".to_string()),
        }
    }

    // ---- is_event_triggered ----

    #[test]
    fn test_is_event_triggered() {
        use crate::models::{RecipeSnapshot, TriggerContext};
        use std::collections::HashMap;

        let make_snapshot = |tt: TriggerType| ExecutionSnapshot {
            recipe: RecipeSnapshot {
                id: "r".to_string(),
                name: "R".to_string(),
                description: None,
                trigger: RecipeTrigger::Manual,
                nodes: vec![],
                edges: vec![],
            },
            agents: HashMap::new(),
            skills: HashMap::new(),
            tools: HashMap::new(),
            trigger_context: TriggerContext {
                issue_id: None,
                project_id: "p".to_string(),
                trigger_type: tt,

                event_payload: None,
            },
            presets: None,
            delegated_packets: vec![],
            created_at: 0,
        };

        assert!(!is_event_triggered(&make_snapshot(TriggerType::Manual)));
        assert!(!is_event_triggered(&make_snapshot(TriggerType::Schedule)));
        assert!(is_event_triggered(&make_snapshot(TriggerType::JobEnded)));
        assert!(is_event_triggered(&make_snapshot(TriggerType::SkillCalled)));
    }

    // ---- matches_job_ended ----

    #[test]
    fn job_ended_no_filter_matches_any_event() {
        let recipe = make_job_ended_recipe("no-filter", None);
        assert!(matches_job_ended(
            &recipe,
            &make_job_ended_event("complete", None)
        ));
        assert!(matches_job_ended(
            &recipe,
            &make_job_ended_event("failed", None)
        ));
        assert!(matches_job_ended(
            &recipe,
            &make_job_ended_event("complete", Some("build"))
        ));
    }

    #[test]
    fn job_ended_status_filter_matches_included_status() {
        let recipe = make_job_ended_recipe(
            "status-filter",
            Some(EventFilter {
                job_status: Some(vec!["complete".to_string()]),
                skill_ids: None,
                node_filter: None,
                every: None,
                group_by: None,
                accumulation_scope: None,
                time_window_secs: None,
            }),
        );
        assert!(matches_job_ended(
            &recipe,
            &make_job_ended_event("complete", None)
        ));
    }

    #[test]
    fn job_ended_status_filter_rejects_excluded_status() {
        let recipe = make_job_ended_recipe(
            "status-filter",
            Some(EventFilter {
                job_status: Some(vec!["complete".to_string()]),
                skill_ids: None,
                node_filter: None,
                every: None,
                group_by: None,
                accumulation_scope: None,
                time_window_secs: None,
            }),
        );
        assert!(!matches_job_ended(
            &recipe,
            &make_job_ended_event("failed", None)
        ));
    }

    #[test]
    fn job_ended_status_filter_accepts_multiple_statuses() {
        let recipe = make_job_ended_recipe(
            "multi-status",
            Some(EventFilter {
                job_status: Some(vec!["complete".to_string(), "failed".to_string()]),
                skill_ids: None,
                node_filter: None,
                every: None,
                group_by: None,
                accumulation_scope: None,
                time_window_secs: None,
            }),
        );
        assert!(matches_job_ended(
            &recipe,
            &make_job_ended_event("complete", None)
        ));
        assert!(matches_job_ended(
            &recipe,
            &make_job_ended_event("failed", None)
        ));
    }

    #[test]
    fn job_ended_node_filter_allow_matches_agent_config() {
        let recipe = make_job_ended_recipe(
            "node-filter",
            Some(EventFilter {
                job_status: None,
                skill_ids: None,
                node_filter: Some(AgentFilter {
                    mode: AgentFilterMode::Allow,
                    ids: vec!["build".to_string()],
                }),
                every: None,
                group_by: None,
                accumulation_scope: None,
                time_window_secs: None,
            }),
        );
        assert!(matches_job_ended(
            &recipe,
            &make_job_ended_event("complete", Some("build"))
        ));
    }

    #[test]
    fn job_ended_node_filter_allow_rejects_wrong_agent() {
        let recipe = make_job_ended_recipe(
            "node-filter",
            Some(EventFilter {
                job_status: None,
                skill_ids: None,
                node_filter: Some(AgentFilter {
                    mode: AgentFilterMode::Allow,
                    ids: vec!["build".to_string()],
                }),
                every: None,
                group_by: None,
                accumulation_scope: None,
                time_window_secs: None,
            }),
        );
        assert!(!matches_job_ended(
            &recipe,
            &make_job_ended_event("complete", Some("review"))
        ));
    }

    #[test]
    fn job_ended_node_filter_allow_rejects_missing_agent() {
        let recipe = make_job_ended_recipe(
            "node-filter",
            Some(EventFilter {
                job_status: None,
                skill_ids: None,
                node_filter: Some(AgentFilter {
                    mode: AgentFilterMode::Allow,
                    ids: vec!["build".to_string()],
                }),
                every: None,
                group_by: None,
                accumulation_scope: None,
                time_window_secs: None,
            }),
        );
        assert!(!matches_job_ended(
            &recipe,
            &make_job_ended_event("complete", None)
        ));
    }

    #[test]
    fn job_ended_node_filter_allow_multiple_ids() {
        let recipe = make_job_ended_recipe(
            "multi-allow",
            Some(EventFilter {
                job_status: None,
                skill_ids: None,
                node_filter: Some(AgentFilter {
                    mode: AgentFilterMode::Allow,
                    ids: vec!["build".to_string(), "review".to_string()],
                }),
                every: None,
                group_by: None,
                accumulation_scope: None,
                time_window_secs: None,
            }),
        );
        assert!(matches_job_ended(
            &recipe,
            &make_job_ended_event("complete", Some("build"))
        ));
        assert!(matches_job_ended(
            &recipe,
            &make_job_ended_event("complete", Some("review"))
        ));
        assert!(!matches_job_ended(
            &recipe,
            &make_job_ended_event("complete", Some("deploy"))
        ));
    }

    #[test]
    fn job_ended_node_filter_exclude_rejects_listed_agent() {
        let recipe = make_job_ended_recipe(
            "exclude",
            Some(EventFilter {
                job_status: None,
                skill_ids: None,
                node_filter: Some(AgentFilter {
                    mode: AgentFilterMode::Exclude,
                    ids: vec!["build".to_string()],
                }),
                every: None,
                group_by: None,
                accumulation_scope: None,
                time_window_secs: None,
            }),
        );
        assert!(!matches_job_ended(
            &recipe,
            &make_job_ended_event("complete", Some("build"))
        ));
        assert!(matches_job_ended(
            &recipe,
            &make_job_ended_event("complete", Some("review"))
        ));
        assert!(matches_job_ended(
            &recipe,
            &make_job_ended_event("complete", None)
        ));
    }

    #[test]
    fn job_ended_node_filter_empty_ids_matches_all() {
        let recipe = make_job_ended_recipe(
            "empty-ids",
            Some(EventFilter {
                job_status: None,
                skill_ids: None,
                node_filter: Some(AgentFilter {
                    mode: AgentFilterMode::Allow,
                    ids: vec![],
                }),
                every: None,
                group_by: None,
                accumulation_scope: None,
                time_window_secs: None,
            }),
        );
        assert!(matches_job_ended(
            &recipe,
            &make_job_ended_event("complete", Some("anything"))
        ));
        assert!(matches_job_ended(
            &recipe,
            &make_job_ended_event("complete", None)
        ));
    }

    #[test]
    fn job_ended_combined_filters_both_must_pass() {
        let recipe = make_job_ended_recipe(
            "combined",
            Some(EventFilter {
                job_status: Some(vec!["complete".to_string()]),
                skill_ids: None,
                node_filter: Some(AgentFilter {
                    mode: AgentFilterMode::Allow,
                    ids: vec!["build".to_string()],
                }),
                every: None,
                group_by: None,
                accumulation_scope: None,
                time_window_secs: None,
            }),
        );
        // Both match
        assert!(matches_job_ended(
            &recipe,
            &make_job_ended_event("complete", Some("build"))
        ));
        // Status wrong
        assert!(!matches_job_ended(
            &recipe,
            &make_job_ended_event("failed", Some("build"))
        ));
        // Node wrong
        assert!(!matches_job_ended(
            &recipe,
            &make_job_ended_event("complete", Some("review"))
        ));
    }

    #[test]
    fn manual_recipe_never_matches_job_ended() {
        let recipe = Recipe {
            id: "manual".to_string(),
            name: "Manual".to_string(),
            description: None,
            trigger: RecipeTrigger::Manual,
            workspace_id: None,
            project_id: None,
            is_default: true,
            version: 1,
            parent_recipe_id: None,
            child_recipe_id: None,
            nodes: vec![],
            edges: vec![],
            created_at: 0,
            updated_at: 0,
        };
        assert!(!matches_job_ended(
            &recipe,
            &make_job_ended_event("complete", None)
        ));
    }

    // ---- matches_skill_called ----

    #[test]
    fn skill_called_no_filter_matches_any_skill() {
        let recipe = make_skill_called_recipe("no-filter", None);
        assert!(matches_skill_called(
            &recipe,
            &make_skill_called_event("anything")
        ));
    }

    #[test]
    fn skill_called_filter_matches_included_skill() {
        let recipe = make_skill_called_recipe(
            "skill-filter",
            Some(EventFilter {
                job_status: None,
                skill_ids: Some(vec!["code-review".to_string()]),
                node_filter: None,
                every: None,
                group_by: None,
                accumulation_scope: None,
                time_window_secs: None,
            }),
        );
        assert!(matches_skill_called(
            &recipe,
            &make_skill_called_event("code-review")
        ));
    }

    #[test]
    fn skill_called_filter_rejects_excluded_skill() {
        let recipe = make_skill_called_recipe(
            "skill-filter",
            Some(EventFilter {
                job_status: None,
                skill_ids: Some(vec!["code-review".to_string()]),
                node_filter: None,
                every: None,
                group_by: None,
                accumulation_scope: None,
                time_window_secs: None,
            }),
        );
        assert!(!matches_skill_called(
            &recipe,
            &make_skill_called_event("testing")
        ));
    }

    #[test]
    fn skill_called_filter_accepts_multiple_skills() {
        let recipe = make_skill_called_recipe(
            "multi-skill",
            Some(EventFilter {
                job_status: None,
                skill_ids: Some(vec!["code-review".to_string(), "testing".to_string()]),
                node_filter: None,
                every: None,
                group_by: None,
                accumulation_scope: None,
                time_window_secs: None,
            }),
        );
        assert!(matches_skill_called(
            &recipe,
            &make_skill_called_event("code-review")
        ));
        assert!(matches_skill_called(
            &recipe,
            &make_skill_called_event("testing")
        ));
        assert!(!matches_skill_called(
            &recipe,
            &make_skill_called_event("deploy")
        ));
    }

    #[test]
    fn job_ended_recipe_never_matches_skill_called() {
        let recipe = make_job_ended_recipe("je", None);
        assert!(!matches_skill_called(
            &recipe,
            &make_skill_called_event("anything")
        ));
    }

    #[test]
    fn skill_called_recipe_never_matches_job_ended() {
        let recipe = make_skill_called_recipe("sc", None);
        assert!(!matches_job_ended(
            &recipe,
            &make_job_ended_event("complete", None)
        ));
    }
}
