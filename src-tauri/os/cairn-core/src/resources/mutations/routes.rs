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
    crate::routes::invalidate_trigger_index(o);
    let _ = o.services.emitter.emit(
        "config-changed",
        serde_json::json!({"entity_type": "route", "action": action, "id": id}),
    );
}
/// The route id `cairn://routes/events` would collide with: the workspace view
/// of the fact log lives there.
const RESERVED_ROUTE_ID: &str = "events";

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
/// The settings canvas and this door hold a route to one standard: a decision
/// nothing in scope can answer is refused when it is written, not discovered
/// when it fires.
async fn refuse_unanswerable(
    o: &Orchestrator,
    route: &RouteDefinition,
    project_key: Option<&str>,
) -> Result<(), String> {
    let project_id = match project_key {
        Some(key) => {
            o.db.local
                .query_opt_text(
                    "SELECT id FROM projects WHERE UPPER(key) = UPPER(?1)",
                    (key.to_string(),),
                )
                .await
                .map_err(|error| error.to_string())?
        }
        None => None,
    };
    crate::routes::decision::refuse_unanswerable_decisions(o, route, project_id.as_deref()).await
}

pub(super) async fn create(
    o: &Orchestrator,
    r: &McpCallbackRequest,
    pay: &serde_json::Value,
    p: Option<&str>,
    dry: bool,
) -> Result<String, String> {
    let (id, text, route) = definition(pay)?;
    if id == RESERVED_ROUTE_ID {
        return Err(format!(
            "'{RESERVED_ROUTE_ID}' is reserved: cairn://routes/{RESERVED_ROUTE_ID} is the workspace fact log. Rename the route."
        ));
    }
    reject(r, &id, p)?;
    let context_path = context_path(o, r, p).await?;
    crate::routes::validate_references(o, &route, context_path.as_deref()).await?;
    refuse_unanswerable(o, &route, p).await?;
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
    refuse_unanswerable(o, &route, p).await?;
    let mut path = root(o, p).await?.join(format!("{id}.yaml"));
    if !path.exists() {
        // Only a pack's own route may stand behind a missing local file, and
        // it is never edited: naming the workspace layer defines one in
        // front of it.
        let roots = cairn_config::library::LibraryRoots::workspace(&o.config_dir);
        match crate::config::routes::resolve_route(&roots, id) {
            Ok(resolved) => match resolved.origin.pack_id() {
                Some(pack)
                    if p.is_none()
                        && super::payload_trimmed_non_empty_str(pay, "layer", &[])
                            == Some("workspace") =>
                {
                    let _ = pack;
                    path = o.config_dir.join("routes").join(format!("{id}.yaml"));
                }
                Some(pack) => {
                    return Err(cairn_config::library::pack_edit_refusal(
                        cairn_config::library::LibraryKind::Route,
                        id,
                        pack,
                    ))
                }
                None => return Err(format!("Route not found: {id}")),
            },
            Err(_) => return Err(format!("Route not found: {id}")),
        }
    }
    if dry {
        return Ok(format!("Would update route '{id}'"));
    }
    std::fs::create_dir_all(path.parent().expect("a route file has a directory"))
        .map_err(|e| e.to_string())?;
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
    let project = match p {
        Some(key) => {
            Some(crate::mcp::handlers::skills_resources::project_path_by_key(o, key).await?)
        }
        None => None,
    };
    let roots = cairn_config::library::LibraryRoots::at(&o.config_dir, project.as_deref());
    let layer = p.and(roots.project_origin());
    let exists = match &layer {
        Some(_) => crate::config::routes::get_project_route(&roots, id)
            .map(|route| route.is_some())
            .unwrap_or(true),
        None => crate::config::routes::get_route(&roots, id)
            .map(|route| route.is_some())
            .unwrap_or(false),
    };
    if !exists {
        return Err(format!("Route not found: {id}"));
    }
    if dry {
        return Ok(format!("Would delete route '{id}'"));
    }
    // An explicit project deletes only the project's own file; otherwise the
    // definition the id resolves to goes: a local file outright, or a pack's
    // own route recorded as removed from its pack.
    let changed = crate::config::routes::delete_route(&roots, id, layer.as_ref())?
        .ok_or_else(|| format!("Route not found: {id}"))?;
    crate::config::commit_config_paths(&[changed], &format!("cairn: delete route {id}"));
    emit_change(o, "removed", id);
    if project.is_none() {
        o.emit_pack_registry_change();
    }
    Ok(format!("Deleted route '{id}'"))
}
