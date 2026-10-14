use crate::{mcp::types::McpCallbackRequest, orchestrator::Orchestrator};
use std::path::PathBuf;
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
        "tier",
        "model",
        "backend",
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
    let base = root(o, r, p).await?;
    let x = crate::config::responses::get_response(
        &o.config_dir,
        id,
        if p.is_some() {
            base.parent().and_then(|x| x.parent())
        } else {
            None
        },
    )?
    .filter(|x| p.is_none() || x.is_project_scoped)
    .ok_or_else(|| format!("Response not found: {id}"))?;
    std::fs::write(&x.file_path, markdown(pay, Some(&x))?).map_err(|e| e.to_string())?;
    crate::config::commit_config_paths(&[x.file_path], &format!("cairn: update response {id}"));
    Ok(format!("Updated response '{id}'"))
}
pub(super) async fn delete(
    o: &Orchestrator,
    r: &McpCallbackRequest,
    id: &str,
    p: Option<&str>,
) -> Result<String, String> {
    reject(r, id, p)?;
    let path = root(o, r, p).await?.join(format!("{id}.md"));
    if !path.exists() {
        return Err(format!("Response not found: {id}"));
    }
    std::fs::remove_file(&path).map_err(|e| e.to_string())?;
    crate::config::commit_config_paths(&[path], &format!("cairn: delete response {id}"));
    Ok(format!("Deleted response '{id}'"))
}
