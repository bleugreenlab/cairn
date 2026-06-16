use crate::config::skills::{self as config_skills, FileSkill, SkillMetaUpdate};
use crate::mcp::handlers::{run_context, skills_resources};
use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;
use cairn_common::uri::{build_project_skill_uri, build_skill_uri};

fn payload_string_array(
    payload: &serde_json::Value,
    key: &str,
) -> Result<Option<Vec<String>>, String> {
    match payload.get(key) {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::Array(values)) => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                let entry = value
                    .as_str()
                    .ok_or_else(|| format!("payload.{key} must be an array of strings"))?;
                out.push(entry.to_string());
            }
            Ok(Some(out))
        }
        Some(_) => Err(format!("payload.{key} must be an array of strings")),
    }
}

/// The skill directory to stage for a commit, given the SKILL.md path returned
/// by `save_skill`.
fn skill_dir_for_commit(skill_md: &std::path::Path) -> std::path::PathBuf {
    skill_md
        .parent()
        .map(std::path::Path::to_path_buf)
        .unwrap_or_else(|| skill_md.to_path_buf())
}

fn skill_not_found_message(skill_id: &str, explicit_project: Option<&str>) -> String {
    match explicit_project {
        Some(project) => {
            format!(
                "Skill not found in project {}: {skill_id}",
                project.to_uppercase()
            )
        }
        None => format!("Skill not found: {skill_id}"),
    }
}

/// Resolve source issue: explicit payload value, else the current run's issue key.
async fn skill_source_issue(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    explicit: Option<String>,
) -> Option<String> {
    if explicit.is_some() {
        return explicit;
    }
    run_context::lookup_run(&orch.db.local, request)
        .await
        .ok()
        .and_then(|ctx| ctx.issue_key())
}

pub(super) async fn apply_skill_create(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    payload: &serde_json::Value,
    explicit_project: Option<&str>,
) -> Result<String, String> {
    let name = super::payload_trimmed_non_empty_str(payload, "name", &[])
        .ok_or("payload.name is required and must be a non-empty string")?;
    crate::skills::validate_skill_name(name).map_err(|e| format!("Invalid skill name: {e}"))?;
    let description = super::payload_non_empty_str(payload, "description", &[])
        .ok_or("payload.description is required and must be a non-empty string")?;
    if description.len() > 1024 {
        return Err("payload.description must be 1024 characters or fewer".to_string());
    }
    let prompt = super::payload_str(payload, "prompt", &[]).ok_or("payload.prompt is required")?;
    let allowed_tools = payload_string_array(payload, "allowedTools")?;
    let source_issue_explicit =
        super::payload_str(payload, "sourceIssue", &[]).map(ToOwned::to_owned);

    let is_project_scoped = explicit_project.is_some();
    let project_path = match explicit_project {
        Some(project) => Some(skills_resources::project_path_by_key(orch, project).await?),
        None => None,
    };

    let conflict_exists = if is_project_scoped {
        project_path.as_ref().is_some_and(|pp| {
            pp.join(".cairn")
                .join("skills")
                .join(name)
                .join("SKILL.md")
                .exists()
        })
    } else {
        orch.config_dir
            .join("skills")
            .join(name)
            .join("SKILL.md")
            .exists()
    };
    if conflict_exists {
        return Err(format!("Skill already exists: {name}"));
    }

    let skill = FileSkill {
        id: name.to_string(),
        name: crate::skills::titlecase_slug(name),
        description: description.to_string(),
        prompt: prompt.to_string(),
        allowed_tools,
        is_project_scoped,
        file_path: std::path::PathBuf::new(),
        dir_path: std::path::PathBuf::new(),
        meta: None,
        has_references: false,
        has_scripts: false,
        has_assets: false,
    };

    let source_issue = skill_source_issue(orch, request, source_issue_explicit).await;
    let meta_update = SkillMetaUpdate {
        updated_by: request.run_id.clone(),
        source_issue,
        source_run_id: request.run_id.clone(),
    };

    let path = config_skills::save_skill(
        &orch.config_dir,
        &skill,
        project_path.as_deref(),
        Some(meta_update),
    )?;

    // Commit the skill directory (SKILL.md + .meta.json) to the owning repo: the
    // project checkout for project scope, ~/.cairn for workspace scope.
    crate::config::commit_config_paths(
        &[skill_dir_for_commit(&path)],
        &format!("cairn: create skill {name}"),
    );

    let skill_uri = match explicit_project {
        Some(project) => build_project_skill_uri(project, name, &[]),
        None => build_skill_uri(name, &[]),
    };
    orch.enqueue_resource_embed(&skill_uri, description.to_string());

    Ok(format!("Created skill '{name}' at {}", path.display()))
}

pub(super) async fn apply_skill_patch(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    payload: &serde_json::Value,
    skill_id: &str,
    explicit_project: Option<&str>,
) -> Result<String, String> {
    let prompt = super::payload_str(payload, "prompt", &[]);
    let append = super::payload_str(payload, "appendToPrompt", &["append_to_prompt"]);
    let replace_section = super::payload_value(payload, "replaceSection", &["replace_section"]);
    let prompt_mods = [
        prompt.is_some(),
        append.is_some(),
        replace_section.is_some(),
    ]
    .iter()
    .filter(|set| **set)
    .count();
    if prompt_mods > 1 {
        return Err(
            "At most one of prompt, appendToPrompt, or replaceSection may be set".to_string(),
        );
    }

    let project_path = super::scope_project_path(orch, request, explicit_project).await?;
    let mut skill = skills_resources::resolve_skill_for_scope(
        &orch.config_dir,
        skill_id,
        explicit_project.is_some(),
        project_path.as_deref(),
    )?
    .ok_or_else(|| skill_not_found_message(skill_id, explicit_project))?;

    if let Some(description) = super::payload_str(payload, "description", &[]) {
        skill.description = description.to_string();
    }
    if let Some(prompt) = prompt {
        skill.prompt = prompt.to_string();
    } else if let Some(append) = append {
        skill.prompt.push('\n');
        skill.prompt.push_str(append);
    } else if let Some(section) = replace_section {
        let heading = section
            .get("heading")
            .and_then(|value| value.as_str())
            .ok_or("payload.replaceSection.heading is required")?;
        let content = section
            .get("content")
            .and_then(|value| value.as_str())
            .ok_or("payload.replaceSection.content is required")?;
        skill.prompt = crate::skills::replace_section_in_prompt(&skill.prompt, heading, content)
            .map_err(|e| format!("Section replacement failed: {e}"))?;
    }
    if let Some(tools) = payload_string_array(payload, "allowedTools")? {
        skill.allowed_tools = if tools.is_empty() { None } else { Some(tools) };
    }

    let source_issue_explicit =
        super::payload_str(payload, "sourceIssue", &[]).map(ToOwned::to_owned);
    let source_issue = skill_source_issue(orch, request, source_issue_explicit).await;
    let meta_update = SkillMetaUpdate {
        updated_by: request.run_id.clone(),
        source_issue,
        source_run_id: request.run_id.clone(),
    };

    let path = config_skills::save_skill(
        &orch.config_dir,
        &skill,
        project_path.as_deref(),
        Some(meta_update),
    )?;
    crate::config::commit_config_paths(
        &[skill_dir_for_commit(&path)],
        &format!("cairn: update skill {skill_id}"),
    );

    let skill_uri = match explicit_project {
        Some(project) => build_project_skill_uri(project, skill_id, &[]),
        None => build_skill_uri(skill_id, &[]),
    };
    orch.enqueue_resource_embed(&skill_uri, skill.description.clone());

    Ok(format!("Updated skill '{skill_id}'"))
}

pub(super) async fn apply_skill_delete(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    payload: Option<&serde_json::Value>,
    skill_id: &str,
    explicit_project: Option<&str>,
) -> Result<String, String> {
    let project_path = super::scope_project_path(orch, request, explicit_project).await?;
    // Enforce scope before deleting: an explicit project URI must not delete the
    // shared workspace skill via get_skill/delete_skill's workspace fallthrough.
    let Some(skill) = skills_resources::resolve_skill_for_scope(
        &orch.config_dir,
        skill_id,
        explicit_project.is_some(),
        project_path.as_deref(),
    )?
    else {
        return Err(skill_not_found_message(skill_id, explicit_project));
    };
    config_skills::delete_skill(&orch.config_dir, skill_id, project_path.as_deref())?;
    crate::config::commit_config_paths(
        std::slice::from_ref(&skill.dir_path),
        &format!("cairn: delete skill {skill_id}"),
    );

    let skill_uri = match explicit_project {
        Some(project) => build_project_skill_uri(project, skill_id, &[]),
        None => build_skill_uri(skill_id, &[]),
    };
    orch.enqueue_resource_delete(&skill_uri);

    let reason = payload
        .and_then(|payload| payload.get("reason"))
        .and_then(|value| value.as_str())
        .map(|reason| format!(" (reason: {reason})"))
        .unwrap_or_default();
    Ok(format!("Deleted skill '{skill_id}'{reason}"))
}
