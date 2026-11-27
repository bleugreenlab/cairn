//! Check requests through their resources: `append` on a node's or task's
//! `checks` files one request per suite, and `patch {action:"cancel"}` on
//! `check-requests/<id>` withdraws one
//! ([`crate::execution::checks::requests`]).
//!
//! The requester is always the authenticated writer, never the payload: the
//! operator, or the run's own node. The node the address names is whose head
//! the request judges when it names no revision.
//!
//! A cancel is the requester's, a node sharing the requester's branch, or the
//! operator's to make. Anyone else is refused, naming who can.

use super::super::{build_failure, payload_non_empty_str, ResourceMutationResult};
use super::Writer;
use crate::execution::check_placement::CheckPlacementOverride;
use crate::execution::checks::requests::{self, CheckRequestSpec};
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;
use cairn_common::executor_protocol::{CheckSelectorSource, ExecutorSelector};
use cairn_uri::CairnResource;
use serde::Deserialize;

/// How long a filing waits for a request the ledger already answers, so a
/// cached verdict is in the reply rather than one read away.
const SETTLE_IN_REPLY: std::time::Duration = std::time::Duration::from_millis(1500);

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FilePayload {
    #[serde(default)]
    suite: Option<String>,
    #[serde(default)]
    suites: Option<Vec<String>>,
    #[serde(default)]
    revision: Option<String>,
    #[serde(default)]
    retry: bool,
    /// Narrow where the lane runs: a machine name or a platform, nothing else.
    #[serde(default)]
    executor: Option<ExecutorSelector>,
    /// Who chose `executor`. Explicit when omitted.
    #[serde(default)]
    selector_source: Option<CheckSelectorSource>,
}

impl FilePayload {
    fn suites(&self) -> Result<Vec<String>, String> {
        let mut suites: Vec<String> = self
            .suite
            .iter()
            .chain(self.suites.iter().flatten())
            .map(|suite| suite.trim().to_string())
            .collect();
        if suites.iter().any(String::is_empty) || suites.is_empty() {
            return Err(
                "name at least one configured suite: payload.suite, or payload.suites for several"
                    .into(),
            );
        }
        let mut seen = std::collections::HashSet::new();
        suites.retain(|suite| seen.insert(suite.clone()));
        Ok(suites)
    }

    /// The validated override this request carries, refused before anything
    /// is filed when it asks for more than a name or a platform.
    fn placement(&self) -> Result<Option<CheckPlacementOverride>, String> {
        match (&self.executor, self.selector_source) {
            (None, None) => Ok(None),
            (None, Some(_)) => Err(
                "selectorSource names where an executor selector came from, and this request carries none"
                    .into(),
            ),
            (Some(selector), source) => {
                CheckPlacementOverride::new(selector.clone(), source.unwrap_or_default()).map(Some)
            }
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
    applied_data: &mut Option<serde_json::Value>,
) -> ResourceMutationResult<Option<String>> {
    match (resource, item.mode) {
        (
            CairnResource::NodeChecks {
                project,
                number,
                exec_seq,
                node_id,
            },
            ChangeMode::Append,
        ) => {
            file(
                orch,
                request,
                index,
                item,
                dry_run,
                (project, *number, *exec_seq, node_id, None),
                applied_data,
            )
            .await
        }
        (
            CairnResource::TaskChecks {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
            },
            ChangeMode::Append,
        ) => {
            file(
                orch,
                request,
                index,
                item,
                dry_run,
                (
                    project,
                    *number,
                    *exec_seq,
                    node_id,
                    Some(task_name.as_str()),
                ),
                applied_data,
            )
            .await
        }
        (CairnResource::ProjectCheckRequest { project, id }, ChangeMode::Patch) => {
            cancel(orch, request, index, item, dry_run, project, id).await
        }
        _ => Ok(None),
    }
}

type NodeAddress<'a> = (&'a String, i32, i32, &'a String, Option<&'a str>);

async fn file(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    (project, number, exec_seq, node_id, task_name): NodeAddress<'_>,
    applied_data: &mut Option<serde_json::Value>,
) -> ResourceMutationResult<Option<String>> {
    let fail = |error: String| build_failure(index, item, error);
    let payload = item
        .payload
        .as_ref()
        .ok_or_else(|| fail("mode=append requires payload {suite} or {suites}".into()))?;
    let payload: FilePayload = serde_json::from_value(payload.clone())
        .map_err(|error| fail(format!("invalid check request: {error}")))?;
    let suites = payload.suites().map_err(fail)?;
    let placement = payload.placement().map_err(fail)?;
    let db = orch.db.for_project(project).await;
    let project_id = crate::mcp::handlers::run_context::project_id_by_key(&db, project)
        .await
        .map_err(fail)?;
    let subject = crate::resources::resolve_node_or_task_job_id_for_read(
        &db, project, number, exec_seq, node_id, task_name,
    )
    .await
    .map_err(fail)?;
    let writer = super::writer(orch, request).await.map_err(fail)?;
    let (requester_ref, requester_job_id) = match &writer {
        Writer::Operator => ("operator".to_string(), None),
        Writer::Run { job_id, home } => (home.clone(), Some(job_id.clone())),
    };
    if dry_run {
        // The write refuses this in `requests::accepted_revision` once it reads
        // the subject's head; a preview reads no head, and the reserved thread
        // coordinate already says there is none, so it refuses in the same words
        // rather than promise a filing the write will not make.
        if payload.revision.is_none()
            && matches!(
                cairn_uri::NodeAddress::new(number, exec_seq, node_id),
                cairn_uri::NodeAddress::Thread { .. }
            )
        {
            return Err(fail(requests::THREAD_REQUEST_NEEDS_A_REVISION.to_string()));
        }
        return Ok(Some(format!(
            "Would file {} check request(s) ({}) on behalf of {requester_ref}",
            suites.len(),
            suites.join(", ")
        )));
    }
    let mut settled = requests::subscribe_settlements();
    let mut filed = Vec::new();
    for suite in suites {
        let spec = CheckRequestSpec {
            consumer: crate::execution::checks::CheckConsumer::Requested,
            cadence: Some("requested"),
            priority: cairn_common::wire_vocabulary::CellPriority::ReviewCheck,
            project_id: project_id.clone(),
            project_key: project.clone(),
            suite,
            revision: payload.revision.clone(),
            subject_job_id: subject.clone(),
            placement: placement.clone(),
            retry: payload.retry,
            requester_ref: requester_ref.clone(),
            requester_job_id: requester_job_id.clone(),
        };
        filed.push(
            requests::file_check_request(orch, spec)
                .await
                .map_err(fail)?,
        );
    }
    let request_db = requests::request_db(orch, &project_id)
        .await
        .map_err(fail)?;
    let _ = tokio::time::timeout(SETTLE_IN_REPLY, async {
        loop {
            if all_settled(&request_db, &project_id, &filed).await {
                return;
            }
            if settled.changed().await.is_err() {
                return;
            }
        }
    })
    .await;
    let mut lines = Vec::new();
    let mut data = Vec::new();
    for request in &filed {
        let row = requests::load_request(&request_db, &project_id, &request.id)
            .await
            .ok()
            .flatten();
        let state = row
            .as_ref()
            .map_or_else(|| "accepted".to_string(), |row| row.state.clone());
        let wait = requests::wait_line(&request.uri);
        let verdict = match &row {
            Some(row) if !row.is_open() => requests::request_answer(orch, request_db.clone(), row)
                .await
                .ok()
                .map(|answer| requests::verdict_words(&answer)),
            _ => None,
        };
        let joined = if request.joined {
            " (an identical open request; this is it)"
        } else {
            ""
        };
        match &verdict {
            Some(verdict) => lines.push(format!(
                "{} {}: {verdict}{joined}",
                request.suite, request.uri
            )),
            None => lines.push(format!(
                "{} {} is {state}{joined}; nothing is placed for you but the check. Wait for its verdict: {wait}",
                request.suite, request.uri
            )),
        }
        data.push(serde_json::json!({
            "id": request.id,
            "uri": request.uri,
            "suite": request.suite,
            "state": state,
            "joined": request.joined,
            "verdict": verdict,
            "waitFor": wait,
        }));
    }
    *applied_data = Some(serde_json::json!({ "requests": data }));
    Ok(Some(lines.join("\n")))
}

async fn all_settled(db: &LocalDb, project_id: &str, filed: &[requests::FiledRequest]) -> bool {
    for request in filed {
        match requests::load_request(db, project_id, &request.id).await {
            Ok(Some(row)) if !row.is_open() => {}
            _ => return false,
        }
    }
    true
}

async fn cancel(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    project: &str,
    id: &str,
) -> ResourceMutationResult<Option<String>> {
    let fail = |error: String| build_failure(index, item, error);
    let payload = item
        .payload
        .as_ref()
        .ok_or_else(|| fail("mode=patch requires payload {action:\"cancel\"}".into()))?;
    match payload_non_empty_str(payload, "action", &[]) {
        Some("cancel") => {}
        other => {
            return Err(fail(format!(
                "payload.action must be 'cancel', got {}",
                other.map_or_else(|| "nothing".to_string(), |action| format!("'{action}'"))
            )))
        }
    }
    let db = orch.db.for_project(project).await;
    let project_id = crate::mcp::handlers::run_context::project_id_by_key(&db, project)
        .await
        .map_err(fail)?;
    let request_db = requests::request_db(orch, &project_id)
        .await
        .map_err(fail)?;
    let (row, legacy) = requests::load_request_by_segment(&request_db, &project_id, id)
        .await
        .map_err(fail)?
        .ok_or_else(|| {
            fail(format!(
                "Nothing was cancelled: no check request `{id}` in {project}."
            ))
        })?;
    let adjustments: Vec<_> = legacy
        .then(|| {
            let received = CairnResource::ProjectCheckRequest {
                project: project.to_string(),
                id: id.to_string(),
            }
            .to_uri();
            cairn_db::storage::address_numbers::legacy_adjustment(&received, &row.uri(project))
        })
        .into_iter()
        .collect();
    let writer = super::writer(orch, request).await.map_err(fail)?;
    authorize(&request_db, &writer, &row).await.map_err(fail)?;
    if dry_run {
        return Ok(Some(cairn_common::adjustment::with_summary(
            format!(
                "Would cancel check request {} ({}) on behalf of {}",
                row.number,
                row.suite,
                writer.principal()
            ),
            &adjustments,
        )));
    }
    requests::cancel_check_request(orch, &request_db, &row, writer.principal())
        .await
        .map(|summary| {
            Some(cairn_common::adjustment::with_summary(
                summary,
                &adjustments,
            ))
        })
        .map_err(fail)
}

/// The operator, the requester, or a live node on the requester's branch.
async fn authorize(
    db: &LocalDb,
    writer: &Writer,
    row: &requests::CheckRequestRow,
) -> Result<(), String> {
    let Writer::Run { job_id, .. } = writer else {
        return Ok(());
    };
    if let Some(requester) = &row.requester_job_id {
        if job_id == requester
            || crate::orchestrator::base_advance::shares_live_branch(db, job_id, requester).await?
        {
            return Ok(());
        }
    }
    Err(format!(
        "Refused: check request `{}` was filed by {}. Only that node, a node sharing its branch, or the operator may cancel it. Nothing was cancelled.",
        row.id, row.requester_ref
    ))
}
