use crate::{mcp::types::McpCallbackRequest, orchestrator::Orchestrator};
use cairn_config::library::{self, LibraryKind, LibraryRoots, Origin};
use std::path::PathBuf;

async fn project_path(o: &Orchestrator, p: Option<&str>) -> Result<Option<PathBuf>, String> {
    match p {
        Some(key) => crate::mcp::handlers::skills_resources::project_path_by_key(o, key)
            .await
            .map(Some),
        None => Ok(None),
    }
}
fn reject(r: &McpCallbackRequest, id: &str, p: Option<&str>) -> Result<(), String> {
    if p.is_some() && r.run_id.is_some() {
        Err(format!("Project response '{id}' must be edited through a relative .cairn/responses file target with commit_msg"))
    } else {
        Ok(())
    }
}
async fn root(
    o: &Orchestrator,
    _r: &McpCallbackRequest,
    p: Option<&str>,
) -> Result<PathBuf, String> {
    Ok(if let Some(k) = p {
        crate::mcp::handlers::skills_resources::project_path_by_key(o, k)
            .await?
            .join(".cairn/responses")
    } else {
        o.config_dir.join("responses")
    })
}
fn markdown(
    payload: &serde_json::Value,
    old: Option<&crate::config::responses::FileResponse>,
) -> Result<String, String> {
    let mut v = old
        .map(|x| serde_json::to_value(&x.definition).unwrap())
        .unwrap_or_else(|| serde_json::json!({}));
    let m = v.as_object_mut().unwrap();
    for k in [
        "name",
        "description",
        "models",
        "options",
        "variables",
        "output",
        "timeout",
        "examples",
    ] {
        if let Some(x) = payload.get(k) {
            m.insert(k.into(), x.clone());
        }
    }
    let prompt = payload
        .get("prompt")
        .and_then(|x| x.as_str())
        .map(str::to_owned)
        .or_else(|| old.map(|x| x.definition.template.clone()))
        .ok_or("payload.prompt is required")?;
    let text = format!(
        "---\n{}---\n{}",
        serde_yaml::to_string(&v).map_err(|e| e.to_string())?,
        prompt
    );
    crate::responses::parse_definition(&text)?;
    Ok(text)
}
pub(super) async fn create(
    o: &Orchestrator,
    r: &McpCallbackRequest,
    pay: &serde_json::Value,
    p: Option<&str>,
) -> Result<String, String> {
    let name =
        super::payload_trimmed_non_empty_str(pay, "name", &[]).ok_or("payload.name is required")?;
    let id = crate::config::slugify(name);
    reject(r, &id, p)?;
    let path = root(o, r, p).await?.join(format!("{id}.md"));
    if path.exists() {
        return Err(format!("Response already exists: {id}"));
    }
    std::fs::create_dir_all(path.parent().unwrap()).map_err(|e| e.to_string())?;
    std::fs::write(&path, markdown(pay, None)?).map_err(|e| e.to_string())?;
    crate::config::commit_config_paths(&[path], &format!("cairn: create response {id}"));
    Ok(format!("Created response '{id}'"))
}
pub(super) async fn patch(
    o: &Orchestrator,
    r: &McpCallbackRequest,
    pay: &serde_json::Value,
    id: &str,
    p: Option<&str>,
) -> Result<String, String> {
    reject(r, id, p)?;
    let roots = LibraryRoots::at(&o.config_dir, project_path(o, p).await?.as_deref());
    let resolved = match p {
        Some(_) => match roots.project_origin() {
            Some(layer) => crate::config::responses::resolve_response_in(&roots, &layer, id),
            None => return Err(format!("Response not found: {id}")),
        },
        None => crate::config::responses::resolve_response(&roots, id),
    };
    let x = match resolved {
        Ok(resolved) => resolved.item,
        Err(error) if error.is_not_found() => return Err(format!("Response not found: {id}")),
        Err(error) => return Err(error.to_string()),
    };
    let target = super::write_layer(pay, &roots, p)?.unwrap_or_else(|| x.origin.clone());
    let path = match &target {
        Origin::Pack { id: pack } => {
            return Err(library::pack_edit_refusal(LibraryKind::Response, id, pack))
        }
        layer if *layer == x.origin => x.file_path.clone(),
        layer => roots
            .layer_dir(LibraryKind::Response, layer)
            .ok_or("Project path required for a project-scoped response")?
            .join(format!("{id}.md")),
    };
    std::fs::create_dir_all(path.parent().expect("a response file has a directory"))
        .map_err(|e| e.to_string())?;
    std::fs::write(&path, markdown(pay, Some(&x))?).map_err(|e| e.to_string())?;
    crate::config::commit_config_paths(&[path], &format!("cairn: update response {id}"));
    Ok(format!("Updated response '{id}'"))
}
pub(super) async fn delete(
    o: &Orchestrator,
    r: &McpCallbackRequest,
    id: &str,
    p: Option<&str>,
) -> Result<String, String> {
    reject(r, id, p)?;
    let project = project_path(o, p).await?;
    let roots = LibraryRoots::at(&o.config_dir, project.as_deref());
    // An explicit project deletes only the project's own file; otherwise the
    // definition the id resolves to goes: a local file outright, or a pack's
    // own response recorded as removed from its pack.
    let layer = p.and(roots.project_origin());
    let changed = crate::config::responses::delete_response(&roots, id, layer.as_ref())?
        .ok_or_else(|| format!("Response not found: {id}"))?;
    crate::config::commit_config_paths(&[changed], &format!("cairn: delete response {id}"));
    if project.is_none() {
        o.emit_pack_registry_change();
    }
    Ok(format!("Deleted response '{id}'"))
}
