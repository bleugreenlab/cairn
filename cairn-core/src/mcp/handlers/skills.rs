//! Skill tool handlers - retrieve, list, create, update, delete skills.

use super::{lookup_project_context, lookup_run};
use crate::config::{skills as config_skills, ConfigResult};
use crate::jobs::queries::load_execution_snapshot;
use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;
use crate::schema::projects;
use crate::skills::{replace_section_in_prompt, validate_skill_name};
use diesel::prelude::*;
use serde::Deserialize;
use std::path::PathBuf;

/// Payload for skill tool (read single skill)
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SkillInput {
    #[serde(alias = "skillId")]
    skill: String,
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
    let run_ctx = lookup_run(&mut conn, request).ok();
    let execution_id = run_ctx.as_ref().and_then(|ctx| ctx.execution_id.clone());

    // Track found skill info for event emission: (skill_id, skill_name)
    let mut found_skill: Option<(String, String)> = None;

    // If this is part of an execution, load from snapshot
    let result = if let Some(ref execution_id) = execution_id {
        if let Ok(snapshot) = load_execution_snapshot(&mut conn, execution_id) {
            // Try direct ID lookup
            if let Some(skill) = snapshot.skills.get(&input.skill) {
                found_skill = Some((skill.id.clone(), skill.name.clone()));
                Some(format!("## Skill: {}\n\n{}", skill.name, skill.prompt))
            } else {
                // Fall back to searching by name
                let mut r = None;
                for skill in snapshot.skills.values() {
                    if skill.name == input.skill || skill.id == input.skill {
                        found_skill = Some((skill.id.clone(), skill.name.clone()));
                        r = Some(format!("## Skill: {}\n\n{}", skill.name, skill.prompt));
                        break;
                    }
                }
                r.or(Some(format!(
                    "Skill not found in execution snapshot: {}",
                    input.skill
                )))
            }
        } else {
            None
        }
    } else {
        None
    };

    let result = if let Some(r) = result {
        r
    } else {
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
            config_skills::get_skill(config_dir, &input.skill, project_path.as_deref())
        {
            found_skill = Some((skill.id.clone(), skill.name.clone()));
            format!("## Skill: {}\n\n{}", skill.name, skill.prompt)
        } else {
            // Fall back to searching by name in all skills
            let mut r = format!("Skill not found: {}", input.skill);
            if let Ok(skills) = config_skills::list_skills(config_dir, project_path.as_deref()) {
                for file_result in skills {
                    if let ConfigResult::Ok(skill) = file_result {
                        if skill.name == input.skill || skill.id == input.skill {
                            found_skill = Some((skill.id.clone(), skill.name.clone()));
                            r = format!("## Skill: {}\n\n{}", skill.name, skill.prompt);
                            break;
                        }
                    }
                }
            }
            r
        }
    };

    // Emit SkillCalled trigger event via channel
    if let Some((skill_id, skill_name)) = found_skill {
        if let Some(ref run_ctx) = run_ctx {
            let _ = orch
                .trigger_events
                .send(crate::models::TriggerEvent::SkillCalled {
                    skill_id,
                    skill_name,
                    run_id: run_ctx.run_id.clone(),
                    job_id: run_ctx.job_id.clone(),
                    execution_id: run_ctx.execution_id.clone(),
                    issue_id: run_ctx.issue_id.clone(),
                    project_id: run_ctx.project_id.clone(),
                    project_key: run_ctx.project_key.clone(),
                    issue_number: run_ctx.issue_number,
                    exec_seq: run_ctx.exec_seq,
                    node_name: run_ctx.job_name.clone(),
                });
        }
    }

    result
}

// ============================================================================
// List Skills
// ============================================================================

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListSkillsInput {
    /// Filter by scope: "workspace", "project", or "all" (default)
    #[serde(default)]
    scope: Option<String>,
}

/// Handle list_skills - list all available skills with metadata.
pub async fn handle_list_skills(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let mut conn = match orch.db.conn.lock() {
        Ok(c) => c,
        Err(e) => return format!("Database lock error: {}", e),
    };

    let input: ListSkillsInput =
        serde_json::from_value(request.payload.clone()).unwrap_or(ListSkillsInput { scope: None });

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

    let skills = match config_skills::list_skills(&orch.config_dir, project_path.as_deref()) {
        Ok(s) => s,
        Err(e) => return format!("Error listing skills: {}", e),
    };

    let scope_filter = input.scope.as_deref().unwrap_or("all");

    let mut output = Vec::new();
    let mut count = 0;

    for result in skills {
        if let ConfigResult::Ok(skill) = result {
            // Apply scope filter
            match scope_filter {
                "workspace" if skill.is_project_scoped => continue,
                "project" if !skill.is_project_scoped => continue,
                _ => {}
            }

            count += 1;
            let scope_label = if skill.is_project_scoped {
                "project"
            } else {
                "workspace"
            };

            let mut line = format!(
                "- **{}** ({}): {}",
                skill.id, scope_label, skill.description
            );

            // Add metadata from .meta.json
            let mut details = Vec::new();
            if let Some(ref meta) = skill.meta {
                if let Some(ref updated) = meta.updated_at {
                    // Just the date part
                    details.push(format!("Updated: {}", &updated[..10.min(updated.len())]));
                }
                if let Some(ref issue) = meta.source_issue {
                    details.push(format!("Source: {}", issue));
                }
            }

            // Add supporting file flags
            let mut has_flags = Vec::new();
            if skill.has_references {
                has_flags.push("references");
            }
            if skill.has_scripts {
                has_flags.push("scripts");
            }
            if skill.has_assets {
                has_flags.push("assets");
            }
            if !has_flags.is_empty() {
                details.push(format!("Has: {}", has_flags.join(", ")));
            }

            if !details.is_empty() {
                line.push_str(&format!("\n  {}", details.join(" | ")));
            }

            output.push(line);
        }
    }

    if count == 0 {
        "No skills found.".to_string()
    } else {
        format!("Found {} skill(s):\n\n{}", count, output.join("\n\n"))
    }
}

// ============================================================================
// Create Skill
// ============================================================================

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateSkillInput {
    /// Skill name (must be valid slug per spec)
    name: String,
    /// Skill description (max 1024 chars)
    description: String,
    /// SKILL.md body (prompt content)
    prompt: String,
    /// "workspace" or "project" (default: "workspace")
    #[serde(default)]
    scope: Option<String>,
    #[serde(default)]
    allowed_tools: Option<Vec<String>>,
    /// Source issue (auto-derived from run context if omitted)
    #[serde(default)]
    source_issue: Option<String>,
}

/// Handle create_skill - create a new skill in directory format.
pub async fn handle_create_skill(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let mut conn = match orch.db.conn.lock() {
        Ok(c) => c,
        Err(e) => return format!("Database lock error: {}", e),
    };

    let input: CreateSkillInput = match serde_json::from_value(request.payload.clone()) {
        Ok(i) => i,
        Err(e) => return format!("Invalid input: {}", e),
    };

    // Validate name against spec rules
    if let Err(e) = validate_skill_name(&input.name) {
        return format!("Invalid skill name: {}", e);
    }

    if input.description.is_empty() {
        return "Description cannot be empty".to_string();
    }
    if input.description.len() > 1024 {
        return "Description must be 1024 characters or fewer".to_string();
    }

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

    let is_project_scoped = input.scope.as_deref() == Some("project");

    // Check for conflicts in the target scope only
    let conflict_exists = if is_project_scoped {
        // Project-scoped: only check the project directory
        project_path.as_ref().is_some_and(|pp| {
            pp.join(".cairn")
                .join("skills")
                .join(&input.name)
                .join("SKILL.md")
                .exists()
        })
    } else {
        // Workspace-scoped: only check the workspace directory
        orch.config_dir
            .join("skills")
            .join(&input.name)
            .join("SKILL.md")
            .exists()
    };
    if conflict_exists {
        return format!("Skill already exists: {}", input.name);
    }

    let skill = config_skills::FileSkill {
        id: input.name.clone(),
        name: crate::skills::titlecase_slug(&input.name),
        description: input.description,
        prompt: input.prompt,
        allowed_tools: input.allowed_tools,
        is_project_scoped,
        file_path: PathBuf::new(),
        dir_path: PathBuf::new(),
        meta: None,
        has_references: false,
        has_scripts: false,
        has_assets: false,
    };

    // Derive source_issue from run context if not provided
    let source_issue = input.source_issue.or_else(|| {
        lookup_run(&mut conn, request)
            .ok()
            .and_then(|ctx| ctx.issue_key())
    });

    let meta_update = config_skills::SkillMetaUpdate {
        updated_by: request.run_id.clone(),
        source_issue,
        source_run_id: request.run_id.clone(),
    };

    match config_skills::save_skill(
        &orch.config_dir,
        &skill,
        project_path.as_deref(),
        Some(meta_update),
    ) {
        Ok(path) => format!("Created skill '{}' at {}", input.name, path.display()),
        Err(e) => format!("Failed to create skill: {}", e),
    }
}

// ============================================================================
// Update Skill
// ============================================================================

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct UpdateSkillInput {
    /// Skill ID (name/slug)
    id: String,
    #[serde(default)]
    description: Option<String>,
    /// Full body replacement
    #[serde(default)]
    prompt: Option<String>,
    /// Append to existing body
    #[serde(default)]
    append_to_prompt: Option<String>,
    /// Replace a markdown section by heading
    #[serde(default)]
    replace_section: Option<ReplaceSectionInput>,
    #[serde(default)]
    allowed_tools: Option<Vec<String>>,
    #[serde(default)]
    source_issue: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReplaceSectionInput {
    heading: String,
    content: String,
}

/// Handle update_skill - update an existing skill.
pub async fn handle_update_skill(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let mut conn = match orch.db.conn.lock() {
        Ok(c) => c,
        Err(e) => return format!("Database lock error: {}", e),
    };

    let input: UpdateSkillInput = match serde_json::from_value(request.payload.clone()) {
        Ok(i) => i,
        Err(e) => return format!("Invalid input: {}", e),
    };

    // Validate at most one prompt modification
    let prompt_mods = [
        input.prompt.is_some(),
        input.append_to_prompt.is_some(),
        input.replace_section.is_some(),
    ]
    .iter()
    .filter(|&&b| b)
    .count();
    if prompt_mods > 1 {
        return "At most one of prompt, append_to_prompt, or replace_section may be set"
            .to_string();
    }

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

    // Load existing skill
    let mut skill =
        match config_skills::get_skill(&orch.config_dir, &input.id, project_path.as_deref()) {
            Ok(Some(s)) => s,
            Ok(None) => return format!("Skill not found: {}", input.id),
            Err(e) => return format!("Error loading skill: {}", e),
        };

    // Apply changes
    if let Some(desc) = input.description {
        skill.description = desc;
    }
    if let Some(prompt) = input.prompt {
        skill.prompt = prompt;
    } else if let Some(append) = input.append_to_prompt {
        skill.prompt.push('\n');
        skill.prompt.push_str(&append);
    } else if let Some(section) = input.replace_section {
        match replace_section_in_prompt(&skill.prompt, &section.heading, &section.content) {
            Ok(new_prompt) => skill.prompt = new_prompt,
            Err(e) => return format!("Section replacement failed: {}", e),
        }
    }
    if let Some(tools) = input.allowed_tools {
        skill.allowed_tools = if tools.is_empty() { None } else { Some(tools) };
    }

    // Derive source_issue from run context if not provided
    let source_issue = input.source_issue.or_else(|| {
        lookup_run(&mut conn, request)
            .ok()
            .and_then(|ctx| ctx.issue_key())
    });

    let meta_update = config_skills::SkillMetaUpdate {
        updated_by: request.run_id.clone(),
        source_issue,
        source_run_id: request.run_id.clone(),
    };

    match config_skills::save_skill(
        &orch.config_dir,
        &skill,
        project_path.as_deref(),
        Some(meta_update),
    ) {
        Ok(_) => format!("Updated skill '{}'", input.id),
        Err(e) => format!("Failed to update skill: {}", e),
    }
}

// ============================================================================
// Delete Skill
// ============================================================================

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DeleteSkillInput {
    id: String,
    #[serde(default)]
    reason: Option<String>,
}

/// Handle delete_skill - remove a skill.
pub async fn handle_delete_skill(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let mut conn = match orch.db.conn.lock() {
        Ok(c) => c,
        Err(e) => return format!("Database lock error: {}", e),
    };

    let input: DeleteSkillInput = match serde_json::from_value(request.payload.clone()) {
        Ok(i) => i,
        Err(e) => return format!("Invalid input: {}", e),
    };

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

    match config_skills::delete_skill(&orch.config_dir, &input.id, project_path.as_deref()) {
        Ok(()) => {
            let reason_msg = input
                .reason
                .map(|r| format!(" (reason: {})", r))
                .unwrap_or_default();
            format!("Deleted skill '{}'{}", input.id, reason_msg)
        }
        Err(e) => format!("Failed to delete skill: {}", e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_skill_input_deserialize() {
        let json = r#"{"skill": "code-review"}"#;
        let input: SkillInput = serde_json::from_str(json).unwrap();
        assert_eq!(input.skill, "code-review");
    }

    #[test]
    fn test_skill_input_deserialize_alias() {
        // Backward compat: skillId still works
        let json = r#"{"skillId": "code-review"}"#;
        let input: SkillInput = serde_json::from_str(json).unwrap();
        assert_eq!(input.skill, "code-review");
    }

    #[test]
    fn test_skill_input_deserialize_with_dashes() {
        let json = r#"{"skill": "test-skill-with-dashes"}"#;
        let input: SkillInput = serde_json::from_str(json).unwrap();
        assert_eq!(input.skill, "test-skill-with-dashes");
    }

    #[test]
    fn test_create_skill_input_deserialize() {
        let json = r##"{
            "name": "code-review",
            "description": "Reviews code",
            "prompt": "# Code Review\n\nReview code.",
            "allowedTools": ["Read", "Grep"],
            "model": "sonnet"
        }"##;
        let input: CreateSkillInput = serde_json::from_str(json).unwrap();
        assert_eq!(input.name, "code-review");
        assert_eq!(
            input.allowed_tools,
            Some(vec!["Read".to_string(), "Grep".to_string()])
        );
    }

    #[test]
    fn test_update_skill_input_deserialize() {
        let json = r##"{
            "id": "testing",
            "appendToPrompt": "\n## New Section\n\nNew content."
        }"##;
        let input: UpdateSkillInput = serde_json::from_str(json).unwrap();
        assert_eq!(input.id, "testing");
        assert!(input.append_to_prompt.is_some());
        assert!(input.prompt.is_none());
    }

    #[test]
    fn test_update_skill_replace_section_input() {
        let heading = "## Details";
        let json = format!(
            r#"{{"id":"testing","replaceSection":{{"heading":"{}","content":"Updated details."}}}}"#,
            heading
        );
        let input: UpdateSkillInput = serde_json::from_str(&json).unwrap();
        assert!(input.replace_section.is_some());
        let section = input.replace_section.unwrap();
        assert_eq!(section.heading, "## Details");
    }

    #[test]
    fn test_list_skills_input_defaults() {
        let json = r#"{}"#;
        let input: ListSkillsInput = serde_json::from_str(json).unwrap();
        assert_eq!(input.scope, None);
    }

    #[test]
    fn test_list_skills_input_with_scope() {
        let json = r#"{"scope": "project"}"#;
        let input: ListSkillsInput = serde_json::from_str(json).unwrap();
        assert_eq!(input.scope, Some("project".to_string()));
    }

    #[test]
    fn test_delete_skill_input_with_reason() {
        let json = r#"{"id": "old-skill", "reason": "Replaced by new-skill"}"#;
        let input: DeleteSkillInput = serde_json::from_str(json).unwrap();
        assert_eq!(input.id, "old-skill");
        assert_eq!(input.reason, Some("Replaced by new-skill".to_string()));
    }

    #[test]
    fn test_delete_skill_input_without_reason() {
        let json = r#"{"id": "old-skill"}"#;
        let input: DeleteSkillInput = serde_json::from_str(json).unwrap();
        assert_eq!(input.id, "old-skill");
        assert_eq!(input.reason, None);
    }

    #[test]
    fn test_create_skill_input_minimal() {
        let json = r#"{"name": "test", "description": "Desc", "prompt": "Do things."}"#;
        let input: CreateSkillInput = serde_json::from_str(json).unwrap();
        assert_eq!(input.name, "test");
        assert!(input.scope.is_none());
        assert!(input.allowed_tools.is_none());
        assert!(input.source_issue.is_none());
    }

    #[test]
    fn test_update_skill_input_all_fields() {
        let json = r##"{
            "id": "testing",
            "description": "Updated desc",
            "prompt": "New prompt.",
            "allowedTools": ["Read"],
            "sourceIssue": "CAIRN-123"
        }"##;
        let input: UpdateSkillInput = serde_json::from_str(json).unwrap();
        assert_eq!(input.id, "testing");
        assert_eq!(input.description, Some("Updated desc".to_string()));
        assert_eq!(input.prompt, Some("New prompt.".to_string()));
        assert_eq!(input.allowed_tools, Some(vec!["Read".to_string()]));
        assert_eq!(input.source_issue, Some("CAIRN-123".to_string()));
    }
}
