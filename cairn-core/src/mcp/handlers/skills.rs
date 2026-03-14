//! Skill tool handler - retrieves skill content on demand.

use super::{lookup_project_context, lookup_run_by_cwd};
use crate::config::{skills as config_skills, ConfigResult};
use crate::jobs::queries::load_execution_snapshot;
use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;
use crate::schema::projects;
use diesel::prelude::*;
use serde::Deserialize;
use std::path::PathBuf;

/// Payload for skill tool
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SkillInput {
    skill_id: String,
}

/// Handle skill tool - retrieve a skill's instructions by ID or name.
pub async fn handle_skill(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let mut conn = match orch.db.conn.lock() {
        Ok(c) => c,
        Err(e) => return format!("Database lock error: {}", e),
    };

    let input: SkillInput = match serde_json::from_value(request.payload.clone()) {
        Ok(i) => i,
        Err(e) => return format!("Invalid input: {}", e),
    };

    // Try to get run context first (has execution_id)
    let execution_id = lookup_run_by_cwd(&mut conn, &request.cwd)
        .ok()
        .and_then(|ctx| ctx.execution_id);

    // If this is part of an execution, load from snapshot
    if let Some(execution_id) = &execution_id {
        if let Ok(snapshot) = load_execution_snapshot(&mut conn, execution_id) {
            // Try direct ID lookup
            if let Some(skill) = snapshot.skills.get(&input.skill_id) {
                return format!("## Skill: {}\n\n{}", skill.name, skill.prompt);
            }

            // Fall back to searching by name
            for skill in snapshot.skills.values() {
                if skill.name == input.skill_id || skill.id == input.skill_id {
                    return format!("## Skill: {}\n\n{}", skill.name, skill.prompt);
                }
            }

            return format!("Skill not found in execution snapshot: {}", input.skill_id);
        }
    }

    // Fallback to files (for non-execution runs or if snapshot load fails)
    let ctx = match lookup_project_context(&mut conn, request) {
        Ok(ctx) => ctx,
        Err(e) => return format!("Error: {}", e),
    };

    let project_path: Option<PathBuf> = projects::table
        .find(&ctx.project_id)
        .select(projects::repo_path)
        .first::<String>(&mut *conn)
        .ok()
        .map(PathBuf::from);

    let config_dir = &orch.config_dir;

    // First try direct ID lookup
    if let Ok(Some(skill)) =
        config_skills::get_skill(config_dir, &input.skill_id, project_path.as_deref())
    {
        return format!("## Skill: {}\n\n{}", skill.name, skill.prompt);
    }

    // Fall back to searching by name in all skills
    if let Ok(skills) = config_skills::list_skills(config_dir, project_path.as_deref()) {
        for result in skills {
            if let ConfigResult::Ok(skill) = result {
                if skill.name == input.skill_id || skill.id == input.skill_id {
                    return format!("## Skill: {}\n\n{}", skill.name, skill.prompt);
                }
            }
        }
    }

    format!("Skill not found: {}", input.skill_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_skill_input_deserialize() {
        let json = r#"{"skillId": "code-review"}"#;
        let input: SkillInput = serde_json::from_str(json).unwrap();
        assert_eq!(input.skill_id, "code-review");
    }

    #[test]
    fn test_skill_input_deserialize_with_dashes() {
        let json = r#"{"skillId": "test-skill-with-dashes"}"#;
        let input: SkillInput = serde_json::from_str(json).unwrap();
        assert_eq!(input.skill_id, "test-skill-with-dashes");
    }
}
