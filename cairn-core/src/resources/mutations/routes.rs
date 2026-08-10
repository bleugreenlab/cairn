use crate::{
    mcp::types::McpCallbackRequest,
    orchestrator::Orchestrator,
    routes::{FactRegistry, RouteDefinition},
};
use std::path::PathBuf;
async fn root(o: &Orchestrator, p: Option<&str>) -> Result<PathBuf, String> {
    Ok(if let Some(k) = p {
        crate::mcp::handlers::skills_resources::project_path_by_key(o, k)
            .await?
            .join(".cairn/routes")
    } else {
        o.config_dir.join("routes")
    })
}
/// Announce that a route file moved.
///
/// A route is a file, not a database row, so nothing else tells the settings
/// surface that this list changed. The client already listens for this event and
/// refetches on it; without the emit, a route an agent creates through
/// `cairn://routes` stays invisible until the window is reloaded.
fn emit_change(o: &Orchestrator, action: &str, id: &str) {
    let _ = o.services.emitter.emit(
        "config-changed",
        serde_json::json!({"entity_type": "route", "action": action, "id": id}),
    );
}
fn reject(r: &McpCallbackRequest, id: &str, p: Option<&str>) -> Result<(), String> {
    if p.is_some() && r.run_id.is_some() {
        Err(format!("Project route '{id}' must be edited through a relative .cairn/routes file target with commit_msg"))
    } else {
        Ok(())
    }
}
fn definition(pay: &serde_json::Value) -> Result<(String, String, RouteDefinition), String> {
    let value = pay
        .get("definition")
        .ok_or("payload.definition is required")?;
    let route = crate::routes::parse_definition(
        &serde_yaml::to_string(value).map_err(|e| e.to_string())?,
        &FactRegistry::default(),
    )?;
    // The file is written from the parsed definition, not from what was sent, so
    // a route submitted in the older linear form lands on disk as the graph it
    // healed into rather than as a second serialization that lives on.
    let text = serde_yaml::to_string(&route).map_err(|e| e.to_string())?;
    Ok((crate::config::slugify(&route.name), text, route))
}
async fn context_path(
    o: &Orchestrator,
    r: &McpCallbackRequest,
    project: Option<&str>,
) -> Result<Option<PathBuf>, String> {
    if let Some(project) = project {
        return crate::mcp::handlers::skills_resources::project_path_by_key(o, project)
            .await
            .map(Some);
    }
    Ok(
        crate::mcp::handlers::skills_resources::current_run_project(o, r)
            .await
            .and_then(|(_, path)| path),
    )
}
pub(super) async fn create(
    o: &Orchestrator,
    r: &McpCallbackRequest,
    pay: &serde_json::Value,
    p: Option<&str>,
    dry: bool,
) -> Result<String, String> {
    let (id, text, route) = definition(pay)?;
    reject(r, &id, p)?;
    let context_path = context_path(o, r, p).await?;
    crate::routes::validate_references(o, &route, context_path.as_deref()).await?;
    let path = root(o, p).await?.join(format!("{id}.yaml"));
    if path.exists() {
        return Err(format!("Route already exists: {id}"));
    }
    if dry {
        return Ok(format!("Would create route '{id}'"));
    }
    std::fs::create_dir_all(path.parent().unwrap()).map_err(|e| e.to_string())?;
    std::fs::write(&path, text).map_err(|e| e.to_string())?;
    crate::config::commit_config_paths(&[path], &format!("cairn: create route {id}"));
    emit_change(o, "created", &id);
    Ok(format!("Created route '{id}'"))
}
pub(super) async fn patch(
    o: &Orchestrator,
    r: &McpCallbackRequest,
    pay: &serde_json::Value,
    id: &str,
    p: Option<&str>,
    dry: bool,
) -> Result<String, String> {
    reject(r, id, p)?;
    let (_, text, route) = definition(pay)?;
    let context_path = context_path(o, r, p).await?;
    crate::routes::validate_references(o, &route, context_path.as_deref()).await?;
    let path = root(o, p).await?.join(format!("{id}.yaml"));
    if !path.exists() {
        return Err(format!("Route not found: {id}"));
    }
    if dry {
        return Ok(format!("Would update route '{id}'"));
    }
    std::fs::write(&path, text).map_err(|e| e.to_string())?;
    crate::config::commit_config_paths(&[path], &format!("cairn: update route {id}"));
    emit_change(o, "modified", id);
    Ok(format!("Updated route '{id}'"))
}
pub(super) async fn delete(
    o: &Orchestrator,
    r: &McpCallbackRequest,
    id: &str,
    p: Option<&str>,
    dry: bool,
) -> Result<String, String> {
    reject(r, id, p)?;
    let path = root(o, p).await?.join(format!("{id}.yaml"));
    if !path.exists() {
        return Err(format!("Route not found: {id}"));
    }
    if dry {
        return Ok(format!("Would delete route '{id}'"));
    }
    std::fs::remove_file(&path).map_err(|e| e.to_string())?;
    crate::config::commit_config_paths(&[path], &format!("cairn: delete route {id}"));
    emit_change(o, "removed", id);
    Ok(format!("Deleted route '{id}'"))
}
