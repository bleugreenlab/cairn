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
        (CairnResource::Responses, ChangeMode::Create) => (0, None, None),
        (CairnResource::ProjectResponses { project }, ChangeMode::Create) => {
            (0, None, Some(project.as_str()))
        }
        (CairnResource::Response { response_id }, ChangeMode::Patch) => {
            (1, Some(response_id.as_str()), None)
        }
        (
            CairnResource::ProjectResponse {
                project,
                response_id,
            },
            ChangeMode::Patch,
        ) => (1, Some(response_id.as_str()), Some(project.as_str())),
        (CairnResource::Response { response_id }, ChangeMode::Delete) => {
            (2, Some(response_id.as_str()), None)
        }
        (
            CairnResource::ProjectResponse {
                project,
                response_id,
            },
            ChangeMode::Delete,
        ) => (2, Some(response_id.as_str()), Some(project.as_str())),
        _ => return Ok(None),
    };
    if dry {
        return Ok(Some("Would mutate response".into()));
    }
    let z = match a {
        0 => {
            super::super::responses::create(
                o,
                r,
                x.payload
                    .as_ref()
                    .ok_or_else(|| build_failure(i, x, "payload required"))?,
                p,
            )
            .await
        }
        1 => {
            super::super::responses::patch(
                o,
                r,
                x.payload
                    .as_ref()
                    .ok_or_else(|| build_failure(i, x, "payload required"))?,
                id.unwrap(),
                p,
            )
            .await
        }
        _ => super::super::responses::delete(o, r, id.unwrap(), p).await,
    }
    .map_err(|e| build_failure(i, x, e))?;
    Ok(Some(z))
}
