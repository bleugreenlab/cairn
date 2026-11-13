//! Mutation dispatch for the publish action (`cairn://p/<project>/publish`).
//!
//! `append {action:"start", kind?, commit?, force?, tool?}` asks for a
//! publication. The operator (a request with no run) or an agent run in the
//! same project may start one; a page or a service may not, because a
//! publication places release builds on the fleet.

use super::super::{build_failure, payload_str, ResourceMutationResult};
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use crate::publications::{self, StartRequest};
use cairn_uri::{build_project_publication_uri, CairnResource};

const KEYS: &[&str] = &["action", "kind", "commit", "force", "tool"];

fn start_request(payload: &serde_json::Value) -> Result<StartRequest, String> {
    let object = payload.as_object().ok_or(
        "the payload must be an object: {action:\"start\", kind?, commit?, force?, tool?}",
    )?;
    if let Some(unknown) = object.keys().find(|key| !KEYS.contains(&key.as_str())) {
        return Err(format!(
            "payload.{unknown} is not a publish key; expected {}",
            KEYS.join(", ")
        ));
    }
    match payload_str(payload, "action", &[]) {
        Some("start") => {}
        Some(other) => return Err(format!("payload.action `{other}` must be start")),
        None => return Err("payload.action must be start".into()),
    }
    let text = |key: &str| -> Result<Option<String>, String> {
        match object.get(key) {
            None | Some(serde_json::Value::Null) => Ok(None),
            Some(serde_json::Value::String(value)) => Ok(Some(value.clone())),
            Some(_) => Err(format!("payload.{key} must be a string")),
        }
    };
    let force = match object.get("force") {
        None | Some(serde_json::Value::Null) => Vec::new(),
        Some(serde_json::Value::Array(values)) => values
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_string)
                    .ok_or_else(|| "payload.force must be an array of target triples".to_string())
            })
            .collect::<Result<_, _>>()?,
        Some(_) => return Err("payload.force must be an array of target triples".into()),
    };
    let request = StartRequest {
        kind: text("kind")?,
        commit: text("commit")?,
        force,
        tool: text("tool")?,
        requested_by: None,
        trigger: "manual".into(),
    };
    publications::validate(&request)?;
    Ok(request)
}

/// Who is starting it: the operator, or the agent run's home.
async fn requester(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    project_key: &str,
) -> Result<String, String> {
    if request.row_write_origin().is_some() {
        return Err("a page or a service cannot start a publication".into());
    }
    if request.run_id.as_deref().is_none_or(str::is_empty) {
        return Ok("operator".into());
    }
    let agent = crate::mcp::handlers::run_context::authenticated_agent(&orch.db, request).await?;
    let (run, _) = crate::mcp::handlers::run_context::lookup_run_routed(&orch.db, request).await?;
    if run.project_key != project_key {
        return Err(format!(
            "a publication is started from its own project; this run belongs to {}",
            run.project_key
        ));
    }
    Ok(agent.home)
}

pub(super) async fn dispatch(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    resource: &CairnResource,
) -> ResourceMutationResult<Option<String>> {
    let CairnResource::ProjectPublish { project } = resource else {
        return Ok(None);
    };
    if item.mode != ChangeMode::Append {
        return Err(build_failure(
            index,
            item,
            "the publish action takes mode=append with {action:\"start\"}",
        ));
    }
    let payload = item
        .payload
        .as_ref()
        .ok_or_else(|| build_failure(index, item, "mode=append requires {action:\"start\"}"))?;
    let mut start = start_request(payload).map_err(|error| build_failure(index, item, error))?;
    let project_key = cairn_uri::canonical_project(project);
    let project_id = crate::project_services::project_id_for_key(orch, &project_key)
        .await
        .map_err(|error| build_failure(index, item, error))?;
    start.requested_by = Some(
        requester(orch, request, &project_key)
            .await
            .map_err(|error| build_failure(index, item, error))?,
    );
    if dry_run {
        return Ok(Some(format!(
            "Would start a publication in {project_key} (live for the targets its commit lists in publish.liveTargets, shadow for the rest)"
        )));
    }
    let started = publications::start(orch, &project_id, start)
        .await
        .map_err(|error| build_failure(index, item, error))?;
    let uri = build_project_publication_uri(&project_key, &started.publication.id);
    let replaced = started.replaced.map_or_else(String::new, |old| {
        format!(
            " It replaced {}, which had not begun.",
            build_project_publication_uri(&project_key, &old)
        )
    });
    Ok(Some(format!(
        "Publication {uri} queued ({} {}, shadow): it runs once no other publication of {project_key} is running, and its legs appear on that resource as they settle.{replaced}",
        started.publication.kind,
        started
            .publication
            .commit
            .as_deref()
            .map_or_else(|| "at the base tip".to_string(), |c| format!("at {c}")),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_start_payload_is_checked_key_by_key() {
        let ok = start_request(&json!({"action": "start", "force": ["x86_64-unknown-linux-gnu"]}))
            .unwrap();
        assert_eq!(ok.force, ["x86_64-unknown-linux-gnu"]);
        assert_eq!(ok.trigger, "manual");
        for (payload, expected) in [
            (json!({"action": "stop"}), "must be start"),
            (json!({}), "must be start"),
            (
                json!({"action": "start", "mode": "live"}),
                "not a publish key",
            ),
            (json!({"action": "start", "force": "all"}), "array"),
            (json!({"action": "start", "commit": 7}), "string"),
            (json!({"action": "start", "kind": "toolPin"}), "tool"),
        ] {
            let error = start_request(&payload).unwrap_err();
            assert!(error.contains(expected), "{payload}: {error}");
        }
    }
}
