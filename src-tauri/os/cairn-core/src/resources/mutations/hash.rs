use super::ResourceTargetHash;
use crate::mcp::handlers::skills_resources;
use crate::mcp::types::{ChangeItem, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use crate::storage::RowExt;
use cairn_common::uri::{parse_uri, CairnResource};
use cairn_db::turso::params;
use sha2::{Digest, Sha256};

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn hash_json_value(value: &serde_json::Value) -> String {
    sha256_hex(serde_json::to_string(value).unwrap_or_default().as_bytes())
}

/// Hash a skill mutation target for optimistic concurrency. Returns None for non-skill resources.
async fn hash_skill_target(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    item: &ChangeItem,
    resource: &CairnResource,
) -> Result<Option<ResourceTargetHash>, String> {
    let (skill_id, explicit_project) = match resource {
        CairnResource::Skills | CairnResource::ProjectSkills { .. } => {
            // Collections have no per-target prior state to guard.
            return Ok(Some(ResourceTargetHash {
                target: item.target.clone(),
                kind: "resource".to_string(),
                exists: true,
                hash: hash_json_value(&serde_json::json!({
                    "kind": "skills_collection",
                    "target": item.target,
                })),
            }));
        }
        CairnResource::Skill { skill_id, .. } => (skill_id.clone(), None),
        CairnResource::ProjectSkill {
            project, skill_id, ..
        } => (skill_id.clone(), Some(project.clone())),
        _ => return Ok(None),
    };

    let project_path =
        super::scope_project_path(orch, request, explicit_project.as_deref()).await?;
    let skill = skills_resources::resolve_skill_for_scope(
        &orch.config_dir,
        &skill_id,
        explicit_project.is_some(),
        project_path.as_deref(),
    )?;
    let exists = skill.is_some();
    let value = match skill {
        Some(skill) => serde_json::json!({
            "kind": "skill",
            "exists": true,
            "description": skill.description,
            "prompt": skill.prompt,
            "allowed_tools": skill.allowed_tools,
        }),
        None => serde_json::json!({"kind": "skill", "exists": false}),
    };
    Ok(Some(ResourceTargetHash {
        target: item.target.clone(),
        kind: "resource".to_string(),
        exists,
        hash: hash_json_value(&value),
    }))
}

pub(crate) async fn hash_resource_target(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    item: &ChangeItem,
) -> Result<ResourceTargetHash, String> {
    let target =
        super::super::common::resolve_home_relative_resource_uri(&orch.db, request, &item.target)
            .await?;
    let resource =
        parse_uri(&target).ok_or_else(|| format!("Unrecognized URI format: {}", item.target))?;
    if let Some(hash) = hash_skill_target(orch, request, item, &resource).await? {
        return Ok(hash);
    }
    let db = orch.db.local.clone();
    let value = db
        .read(|conn| {
            let resource = resource.clone();
            Box::pin(async move {
                match resource {
                    CairnResource::ProjectIssues { project } => {
                        let mut rows = conn
                            .query(
                                "SELECT i.id, i.number, i.title, i.updated_at
                                 FROM issues i
                                 JOIN projects p ON i.project_id = p.id
                                 WHERE p.key = ?1
                                 ORDER BY i.number ASC",
                                (project.as_str(),),
                            )
                            .await?;
                        let mut items = Vec::new();
                        while let Some(row) = rows.next().await? {
                            items.push(serde_json::json!({
                                "id": row.text(0)?,
                                "number": row.i64(1)?,
                                "title": row.text(2)?,
                                "updated_at": row.opt_i64(3)?,
                            }));
                        }
                        Ok(serde_json::json!({"kind":"project_issues","items":items}))
                    }
                    CairnResource::Issue { project, number } => {
                        let mut rows = conn
                            .query(
                                "SELECT i.id, i.title, i.description, i.updated_at
                                 FROM issues i
                                 JOIN projects p ON i.project_id = p.id
                                 WHERE p.key = ?1 AND i.number = ?2
                                 LIMIT 1",
                                params![project.as_str(), number as i64],
                            )
                            .await?;
                        let Some(row) = rows.next().await? else {
                            return Ok(serde_json::json!({"kind":"issue","missing":true}));
                        };
                        let issue_id = row.text(0)?;
                        let title = row.text(1)?;
                        let description = row.opt_text(2)?;
                        let updated_at = row.opt_i64(3)?;
                        let mut dep_rows = conn
                            .query(
                                "SELECT d.depends_on_uri
                                 FROM issue_dependencies d
                                 WHERE d.issue_id = ?1
                                 ORDER BY d.depends_on_uri ASC",
                                (issue_id.as_str(),),
                            )
                            .await?;
                        let mut dependencies = Vec::new();
                        while let Some(dep_row) = dep_rows.next().await? {
                            dependencies.push(dep_row.text(0)?);
                        }
                        Ok(serde_json::json!({
                            "kind":"issue",
                            "id": issue_id,
                            "title": title,
                            "description": description,
                            "updated_at": updated_at,
                            "depends_on": dependencies,
                        }))
                    }
                    CairnResource::IssueMessages { project, number } => {
                        let mut rows = conn
                            .query(
                                "SELECT m.id, m.content, m.created_at
                                 FROM messages m
                                 WHERE m.channel_type = 'issue' AND m.channel_id = ?1
                                 ORDER BY m.created_at ASC, m.id ASC",
                                (format!("{}/{}", project, number).as_str(),),
                            )
                            .await?;
                        let mut messages = Vec::new();
                        while let Some(row) = rows.next().await? {
                            messages.push(serde_json::json!({
                                "id": row.text(0)?,
                                "content": row.text(1)?,
                                "created_at": row.i64(2)?,
                            }));
                        }
                        Ok(serde_json::json!({"kind":"issue_messages","messages":messages}))
                    }
                    CairnResource::ProjectMessages { project } => {
                        let mut rows = conn
                            .query(
                                "SELECT m.id, m.content, m.created_at
                                 FROM messages m
                                 WHERE m.channel_type = 'project' AND m.channel_id = ?1
                                 ORDER BY m.created_at ASC, m.id ASC",
                                (project.as_str(),),
                            )
                            .await?;
                        let mut messages = Vec::new();
                        while let Some(row) = rows.next().await? {
                            messages.push(serde_json::json!({
                                "id": row.text(0)?,
                                "content": row.text(1)?,
                                "created_at": row.i64(2)?,
                            }));
                        }
                        Ok(serde_json::json!({"kind":"project_messages","messages":messages}))
                    }
                    CairnResource::Node {
                        project,
                        number,
                        exec_seq,
                        node_id,
                    } => Ok(serde_json::json!({
                        "kind":"node_messages",
                        "project": project,
                        "number": number,
                        "exec_seq": exec_seq,
                        "node_id": node_id,
                    })),
                    _ => Ok(serde_json::json!({"kind":"resource","target": target})),
                }
            })
        })
        .await
        .map_err(|e| e.to_string())?;

    Ok(ResourceTargetHash {
        target: item.target.clone(),
        kind: "resource".to_string(),
        exists: !value
            .get("missing")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        hash: hash_json_value(&value),
    })
}
