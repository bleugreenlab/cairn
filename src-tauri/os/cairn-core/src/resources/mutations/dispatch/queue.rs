//! Queue acts retain the operator approval boundary and are restricted to the
//! authenticated asking thread's own slice.
use super::super::{build_failure, target_resource_for_request, ResourceMutationResult};
use crate::authorization::prepare::Prepared;
use crate::execution::queue;
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use cairn_common::authorization::{
    ActDigest, AuthorityAction, AuthorityDecision, AuthorityMutation, AuthorityPlace,
    AuthorityRequest, AuthorityScope,
};
use cairn_uri::CairnResource;

/// The thread asking for a queue act.
pub(crate) struct Asker {
    pub thread_id: String,
    pub thread_name: String,
}

/// Why no thread can ask.
enum NoAsker {
    /// No run, or a page: the write handler's own refusals apply, and the
    /// operator's path is Settings → Queue.
    Unauthenticated,
    /// A run that acts for no thread.
    NoThread,
    /// A project service's run, even one acting for a thread: unattended
    /// code, which the card ("alpha asks") does not describe.
    Service,
}

const NO_THREAD: &str = "Refused: queue acts across threads are a thread's to ask for, and \
     this run acts for no thread. Nothing changed.";

const SERVICE: &str = "Refused: a project service may not ask for queue acts, even acting for \
     a thread. The operator's card says a thread asked, and a grant to that thread does not \
     cover unattended code moving or starting work on a loop. Nothing changed.";

/// Why `request` cannot ask for a queue act as a thread, whichever thread it
/// would resolve to: no run (a page's row write included), or a project
/// service's run.
fn excluded(orch: &Orchestrator, request: &McpCallbackRequest) -> Option<NoAsker> {
    let no_run = request
        .run_id
        .as_deref()
        .filter(|run| !run.is_empty())
        .is_none();
    if no_run || request.row_write_origin().is_some() {
        return Some(NoAsker::Unauthenticated);
    }
    // Checked before the thread resolves: a delegated service run would
    // otherwise resolve to the thread it acts for and match that thread's
    // grant. Excluded on security's ruling (cairn/5942).
    if request
        .run_id
        .as_deref()
        .is_some_and(|run| orch.project_services.runs.get(run).is_some())
    {
        return Some(NoAsker::Service);
    }
    None
}

/// The thread a queue grant would bind for `request`, by id: what holds an
/// `ActingThread` grant when it is revoked (cairn/5998). The same exclusions
/// as [`asker`], then the run's own job's thread.
pub(crate) async fn asking_thread_id(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
) -> Option<String> {
    if excluded(orch, request).is_some() {
        return None;
    }
    super::threads::run_thread_id(orch, request).await
}

async fn asker(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    project: &str,
) -> Result<Asker, NoAsker> {
    if let Some(refusal) = excluded(orch, request) {
        return Err(refusal);
    }
    super::threads::acting_thread(orch, request, project)
        .await
        .map(|(thread_id, thread_name)| Asker {
            thread_id,
            thread_name,
        })
        .ok_or(NoAsker::NoThread)
}

#[derive(Debug, PartialEq, Eq)]
enum Act {
    Move(usize),
    Park,
    Unpark,
}

fn parse(payload: &serde_json::Value) -> Result<(String, Act), String> {
    let object = payload.as_object().filter(|o| o.len() == 1)
        .ok_or("Send exactly one of {move:{execution:ID,position:1}}, {park:{execution:ID}}, {unpark:{execution:ID}}.")?;
    let (name, value) = object.iter().next().unwrap();
    let expected = if name == "move" { 2 } else { 1 };
    let entry = value
        .as_object()
        .filter(|o| o.len() == expected)
        .ok_or("A queue action names one execution; move also requires a 1-based position.")?;
    let id = entry
        .get("execution")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or("Queue actions require execution:ID.")?
        .to_string();
    let act = match name.as_str() {
        "move" => {
            let position = entry
                .get("position")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0)
                .and_then(|n| usize::try_from(n).ok())
                .ok_or("move.position must be a positive 1-based position.")?;
            Act::Move(position)
        }
        "park" => Act::Park,
        "unpark" => Act::Unpark,
        _ => return Err("Unknown queue action; use move, park or unpark.".into()),
    };
    Ok((id, act))
}

async fn request_for(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    project: &str,
    payload: &serde_json::Value,
) -> Result<(AuthorityRequest, String, Act), String> {
    let (id, act) = parse(payload)?;
    let asker = asker(orch, request, project).await.map_err(|reason| match reason {
        NoAsker::Unauthenticated => "Denied: queue actions require an authenticated thread; the operator uses Settings → Queue.".to_string(),
        NoAsker::NoThread => NO_THREAD.to_string(),
        NoAsker::Service => SERVICE.to_string(),
    })?;
    let state = queue::read(orch).await?;
    let item = state
        .item(&id)
        .ok_or("Execution is not in this runner's queue.")?;
    if item.thread_id.as_deref() != Some(&asker.thread_id) {
        return Err(
            "Refused: an agent may act only on executions in its own thread's slice.".into(),
        );
    }
    if matches!(act, Act::Move(_)) && item.parked {
        return Err("Execution is parked; unpark it before moving it.".into());
    }
    let verb = match act {
        Act::Move(_) => "move",
        Act::Park => "park",
        Act::Unpark => "unpark",
    };
    let mut summary = format!("{verb} execution {id}");
    if let Act::Move(position) = act {
        summary.push_str(&format!(" to position {position}"));
    }
    summary.push_str(&format!(" ({} asks)", asker.thread_name));
    let authority = AuthorityRequest::new(
        AuthorityScope::new(
            AuthorityPlace::ExecutionQueue {
                workspace_id: crate::authorization::WORKSPACE_ID.into(),
            },
            AuthorityAction::Write,
        ),
        AuthorityMutation::Update,
        summary,
    )
    .with_acting_thread(&asker.thread_id)
    .with_act(ActDigest::of(&format!("queue.{verb}"), |encoder| {
        encoder.sequence("executions", std::slice::from_ref(&id));
        if let Act::Move(position) = act {
            encoder.sequence("positions", &[position.to_string()]);
        }
    }));
    Ok((authority, id, act))
}

pub(crate) async fn queue_authority(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    item: &ChangeItem,
) -> Option<Prepared> {
    if item.mode != ChangeMode::Patch {
        return None;
    }
    let CairnResource::ProjectQueue { project } = target_resource_for_request(orch, request, item)
        .await
        .ok()?
    else {
        return None;
    };
    let payload = item.payload.as_ref()?;
    Some(match request_for(orch, request, &project, payload).await {
        Ok((authority, _, _)) => Prepared::Authority(authority),
        Err(error) => Prepared::Refused(error),
    })
}

async fn authorize(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    authority: &AuthorityRequest,
) -> Result<(), String> {
    let actor = crate::authorization::resolve_actor(orch, request)
        .await
        .ok_or("Denied: queue actions require an authenticated run.")?;
    let (decision, _) = crate::authorization::authorize_recorded(&actor, authority).await?;
    match decision {
        AuthorityDecision::Direct | AuthorityDecision::AllowedByGrant { .. } => Ok(()),
        AuthorityDecision::ApprovalRequired(reason) | AuthorityDecision::Forbidden(reason) => {
            Err(crate::authorization::refusal_message(authority, reason))
        }
    }
}

pub(super) async fn dispatch(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    resource: &CairnResource,
) -> ResourceMutationResult<Option<String>> {
    let (CairnResource::ProjectQueue { project }, ChangeMode::Patch) = (resource, item.mode) else {
        return Ok(None);
    };
    let fail = |error: String| build_failure(index, item, error);
    let payload = item
        .payload
        .as_ref()
        .ok_or_else(|| fail("Queue action payload required.".into()))?;
    let (authority, id, act) = request_for(orch, request, project, payload)
        .await
        .map_err(fail)?;
    if dry_run {
        return Ok(Some(format!("Would {}", authority.summary)));
    }
    authorize(orch, request, &authority).await.map_err(fail)?;
    // Revalidate ownership immediately before the queue authority mutates.
    request_for(orch, request, project, payload)
        .await
        .map_err(fail)?;
    match act {
        Act::Move(position) => queue::move_to(orch, &id, position).await,
        Act::Park => queue::set_parked(orch, &id, true).await,
        Act::Unpark => queue::set_parked(orch, &id, false).await,
    }
    .map_err(fail)?;
    Ok(Some(authority.summary))
}

#[cfg(test)]
mod payload_tests {
    use super::{parse, Act};
    use serde_json::json;

    #[test]
    fn each_action_is_one_execution_and_move_requires_a_positive_position() {
        assert_eq!(
            parse(&json!({"move":{"execution":"e","position":1}})).unwrap(),
            ("e".into(), Act::Move(1))
        );
        assert_eq!(
            parse(&json!({"park":{"execution":"e"}})).unwrap(),
            ("e".into(), Act::Park)
        );
        assert_eq!(
            parse(&json!({"unpark":{"execution":"e"}})).unwrap(),
            ("e".into(), Act::Unpark)
        );
        for payload in [
            json!({"move":{"execution":"e","position":0}}),
            json!({"move":{"execution":"e","position":-1}}),
            json!({"move":{"execution":"e","position":1},"park":{"execution":"e"}}),
            json!({"order":["e"]}),
            json!({"park":{"execution":""}}),
        ] {
            assert!(parse(&payload).is_err(), "{payload}");
        }
    }
}
