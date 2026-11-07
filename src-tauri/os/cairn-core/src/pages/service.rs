//! `page_service`: a page's call to a service, carried for its frame.
//!
//! The frame never reaches the service. Its only I/O is the bridge, and this is
//! the bridge's `service` verb: the runner checks the page's manifest, calls
//! the service with the page's caller frame, and logs the call in `page_calls`.
//! A page names a project service by its id and a thread's service by its
//! canonical URI; a thread's service answers only its own thread's pages.
//!
//! What the call may do inside the service is decided by
//! `project_services::authority::delegation_for`, the same function an
//! agent's call takes: for a page, read and write the page's owning thread's
//! database and nothing else (the operator's decision on cairn/5518). Every
//! record the service wrote for the call is a target of the call's
//! `page_calls` row.

use serde_json::{json, Value};
use std::time::Instant;

use super::data::{service_names, DeclaredService};
use super::log::{PageCallOutcome, PageCallVerb};
use super::read::{log_call, resolve, PageCallError, ResolvedPage};
use crate::orchestrator::Orchestrator;
use crate::project_services::protocol::{CallerFrame, CallerKind, Instrument};
use crate::project_services::supervisor::{self, CallRequest, MAX_PARAMS_BYTES};
use crate::project_services::{thread_store, ServiceOwner};

/// The largest answer a page receives from a service.
const MAX_ANSWER_BYTES: usize = 1024 * 1024;

/// A page's call to a project service.
#[derive(Debug, Clone)]
pub struct PageServiceRequest {
    pub project_id: String,
    pub page: String,
    pub version: i32,
    pub service: String,
    pub method: String,
    pub params: Value,
}

fn answer_error(code: &str, message: &str) -> Value {
    json!({ "ok": false, "error": { "code": code, "message": message } })
}

async fn refuse(
    orch: &Orchestrator,
    page: &ResolvedPage,
    viewer: &Value,
    target: &str,
    code: &str,
    message: &str,
) -> Value {
    log_call(
        orch,
        page,
        viewer,
        PageCallVerb::Service,
        &[target.to_string()],
        PageCallOutcome::Refused,
        &format!("{code}: {message}"),
    )
    .await;
    answer_error(code, message)
}

/// Only the viewer's classification crosses to the service: it is context for
/// the service's own decisions, and the service has no need of a person's
/// issuer or subject.
fn viewer_context(viewer: &Value) -> Value {
    json!({ "kind": viewer.get("kind").cloned().unwrap_or(json!("anonymous")) })
}

/// Carry one service call for a page. Answers `{ok: true, value}` or
/// `{ok: false, error: {code, message}}`; the error form is an answer, not a
/// transport failure, so the frame always sees the code.
pub async fn page_service(
    orch: &Orchestrator,
    request: &PageServiceRequest,
    viewer: &Value,
) -> Result<Value, PageCallError> {
    let page = resolve(orch, &request.project_id, &request.page, request.version).await?;
    let project_key = page.address.project.clone();
    let declared = DeclaredService::parse(&request.service);
    let target = format!(
        "{}#{}",
        match &declared {
            Ok(DeclaredService::Thread { .. }) => declared
                .as_ref()
                .map(DeclaredService::canonical)
                .unwrap_or_default(),
            _ => cairn_uri::build_project_service_uri(&project_key, &request.service),
        },
        request.method
    );
    let declared = match declared {
        Ok(declared) if !request.method.trim().is_empty() => declared,
        _ => {
            return Ok(refuse(
                orch,
                &page,
                viewer,
                &target,
                "bad_request",
                "a service call names a service (a project service id, or a thread service's \
                 URI) and a method",
            )
            .await)
        }
    };
    let is_declared = service_names(&page.row.data).iter().any(|entry| {
        DeclaredService::parse(entry).is_ok_and(|entry| entry.canonical() == declared.canonical())
    });
    if !is_declared {
        let message = format!(
            "page {}@{} does not declare service `{}` in manifest.services",
            page.address.uri(),
            page.row.version,
            request.service
        );
        return Ok(refuse(orch, &page, viewer, &target, "not_declared", &message).await);
    }
    let (owner, service) = match &declared {
        DeclaredService::Project(id) => (ServiceOwner::Project, id.clone()),
        DeclaredService::Thread {
            project,
            thread,
            service,
        } => {
            if *project != project_key {
                let message = format!(
                    "a page calls services of its own project ({project_key}), not {project}'s"
                );
                return Ok(refuse(orch, &page, viewer, &target, "not_owner", &message).await);
            }
            let db = orch.db.for_project(&project_key).await;
            match thread_store::resolve_owner(&db, &project_key, thread).await {
                Ok((_, owner)) => (owner, service.clone()),
                Err(message) => {
                    return Ok(
                        refuse(orch, &page, viewer, &target, "unknown_service", &message).await,
                    )
                }
            }
        }
    };
    let params_bytes = serde_json::to_vec(&request.params).map_or(0, |bytes| bytes.len());
    if params_bytes > MAX_PARAMS_BYTES {
        let message =
            format!("params are {params_bytes} bytes; a call may carry at most {MAX_PARAMS_BYTES}");
        return Ok(refuse(orch, &page, viewer, &target, "params_too_large", &message).await);
    }

    let caller = CallerFrame {
        kind: CallerKind::Page,
        acting_for: page.address.home_uri(),
        agent: None,
        viewer: Some(viewer_context(viewer)),
        instrument: Some(Instrument {
            page: page.address.uri(),
            version: page.row.version as i64,
        }),
    };
    let started = Instant::now();
    let outcome = supervisor::call_recording(
        orch,
        CallRequest {
            project_id: page.project_id.clone(),
            project_key,
            owner,
            service,
            method: request.method.clone(),
            params: request.params.clone(),
            caller,
            timeout: None,
        },
    )
    .await;
    let millis = started.elapsed().as_millis();
    // The call's row names what it touched: the service, and every record the
    // service wrote into the thread's database for it.
    let mut targets = vec![target];
    targets.extend(outcome.writes);
    // And the code it ran: each batch the service started for the page, under
    // the call's own run.
    let ran: String = outcome
        .batches
        .iter()
        .map(|batch| format!("; {}", batch.describe()))
        .collect();
    match outcome.result {
        Ok(mut value) => {
            // The service's answer becomes the page's data, and through a
            // headless render, model-visible text.
            let policy = crate::security::redaction_policy(&orch.config_dir);
            crate::security::Sanitizer::structural(&policy).json(&mut value);
            let bytes = serde_json::to_vec(&value).map_or(0, |bytes| bytes.len());
            if bytes > MAX_ANSWER_BYTES {
                let message = format!(
                    "the service answered {bytes} bytes; a page receives at most {MAX_ANSWER_BYTES}"
                );
                log_call(
                    orch,
                    &page,
                    viewer,
                    PageCallVerb::Service,
                    &targets,
                    PageCallOutcome::Error,
                    &format!("result_too_large: {message} ({millis} ms){ran}"),
                )
                .await;
                return Ok(answer_error("result_too_large", &message));
            }
            log_call(
                orch,
                &page,
                viewer,
                PageCallVerb::Service,
                &targets,
                PageCallOutcome::Ok,
                &format!("{millis} ms{ran}"),
            )
            .await;
            Ok(json!({ "ok": true, "value": value }))
        }
        Err(error) => {
            let outcome = if matches!(error.code.as_str(), "unknown_service" | "not_owner") {
                PageCallOutcome::Refused
            } else {
                PageCallOutcome::Error
            };
            log_call(
                orch,
                &page,
                viewer,
                PageCallVerb::Service,
                &targets,
                outcome,
                &format!("{}: {} ({millis} ms){ran}", error.code, error.message),
            )
            .await;
            Ok(answer_error(&error.code, &error.message))
        }
    }
}
