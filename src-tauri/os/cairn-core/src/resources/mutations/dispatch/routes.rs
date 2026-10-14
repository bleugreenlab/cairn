use super::super::{build_failure, ResourceMutationResult};
use crate::{
    mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest},
    orchestrator::Orchestrator,
};
use cairn_common::uri::CairnResource;
pub(super) async fn dispatch(
    o: &Orchestrator,
    r: &McpCallbackRequest,
    i: usize,
    x: &ChangeItem,
    dry: bool,
    u: &CairnResource,
) -> ResourceMutationResult<Option<String>> {
    let (a, id, p) = match (u, x.mode) {
        (CairnResource::Routes, ChangeMode::Create) => (0, None, None),
        (CairnResource::ProjectRoutes { project }, ChangeMode::Create) => {
            (0, None, Some(project.as_str()))
        }
        (CairnResource::Route { route_id }, ChangeMode::Patch) => {
            (1, Some(route_id.as_str()), None)
        }
        (CairnResource::ProjectRoute { project, route_id }, ChangeMode::Patch) => {
            (1, Some(route_id.as_str()), Some(project.as_str()))
        }
        (CairnResource::Route { route_id }, ChangeMode::Delete) => {
            (2, Some(route_id.as_str()), None)
        }
        (CairnResource::ProjectRoute { project, route_id }, ChangeMode::Delete) => {
            (2, Some(route_id.as_str()), Some(project.as_str()))
        }
        _ => return Ok(None),
    };
    let z = match a {
        0 => {
            super::super::routes::create(
                o,
                r,
                x.payload
                    .as_ref()
                    .ok_or_else(|| build_failure(i, x, "payload required"))?,
                p,
                dry,
            )
            .await
        }
        1 => {
            super::super::routes::patch(
                o,
                r,
                x.payload
                    .as_ref()
                    .ok_or_else(|| build_failure(i, x, "payload required"))?,
                id.unwrap(),
                p,
                dry,
            )
            .await
        }
        _ => super::super::routes::delete(o, r, id.unwrap(), p, dry).await,
    }
    .map_err(|e| build_failure(i, x, e))?;
    Ok(Some(z))
}
