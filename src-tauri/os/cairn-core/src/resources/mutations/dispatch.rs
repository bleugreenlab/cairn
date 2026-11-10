//! Write-side dispatcher for `cairn://` resource mutations.
//!
//! `dispatch_resource_change` gates each `(resource, mode)` pair against the
//! contract table, then routes it to the owning resource-family submodule under
//! `dispatch/`. Each family module re-matches its own arms verbatim and returns
//! `Ok(None)` for pairs it does not own, so the chain below preserves the
//! original single-`match` dispatch order and rejection paths. The contract
//! gate, `append_payload`, and `wants_return_content` stay here as shared
//! infrastructure; per-family payload parsing and arm bodies live in the
//! submodules.

mod actions;
mod agents;
mod artifacts;
mod attachments;
pub(crate) use attachments::attachment_removal_authority;
mod batches;
mod browsers;
mod bug;
mod channels;
mod events;
mod executions;
mod executors;
mod feed;
mod grants;
mod inference;
pub(crate) use inference::inference_records_authority;
mod dev_launch_ticket;
mod issues;
mod labels;
mod mcp;
mod memories;
mod messages;
mod models;
mod models_accounts;
mod nodes;
mod packs;
mod pages;
mod posts;
mod progress;
mod projects;
mod prompts;
mod queue;
pub(crate) use queue::{asking_thread_id, authorize_start_now, queue_authority};
mod rebase;
pub(crate) use rebase::{acceptance_debt_section, rebase_authority};
mod camps;
mod recipes;
mod repls;
mod requests;
mod responses;
mod routes;
mod services;
mod settings;
mod skills;
mod tenets;
mod terminals;
mod threads;
mod todos;
mod wakes;

#[cfg(test)]
mod camp_mutation_tests;
#[cfg(test)]
mod issue_mutation_tests;
#[cfg(test)]
mod models_account_tests;
#[cfg(test)]
mod queue_authority_tests;
#[cfg(test)]
mod resource_gate_tests;
#[cfg(test)]
mod thread_mutation_tests;

use super::{
    build_failure, mode_name, target_resource_for_request, ResourceAppliedChange,
    ResourceMutationResult,
};
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use cairn_common::contract::{contract_for, MutationSpec};
use cairn_platform::stack::off_frame;
use cairn_uri::CairnResource;
use cairn_uri::ResourceKind;

/// Who a resource write acts for: the operator, for a write no run makes, or
/// the run's job and that job's home URI.
pub(super) enum Writer {
    Operator,
    Run { job_id: String, home: String },
}

impl Writer {
    /// The principal a write names in what it records and answers.
    pub(super) fn principal(&self) -> &str {
        match self {
            Self::Operator => "operator",
            Self::Run { home, .. } => home,
        }
    }
}

pub(super) async fn writer(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
) -> Result<Writer, String> {
    if request.run_id.as_deref().is_none_or(str::is_empty) {
        return Ok(Writer::Operator);
    }
    let (context, _) =
        crate::mcp::handlers::run_context::lookup_run_routed(&orch.db, request).await?;
    let home = crate::mcp::handlers::run_context::lookup_home_uri_routed(&orch.db, request)
        .await
        .unwrap_or_else(|_| format!("run {}", context.run_id));
    Ok(Writer::Run {
        job_id: context.job_id,
        home,
    })
}

fn append_payload(index: usize, item: &ChangeItem) -> ResourceMutationResult<&serde_json::Value> {
    item.payload
        .as_ref()
        .ok_or_else(|| build_failure(index, item, "mode=append requires payload"))
}

/// Build the "unsupported mutation" rejection by enumerating the resource's
/// valid mutations from the contract table.
fn render_unsupported(kind: ResourceKind, mode: ChangeMode) -> String {
    let mut out = format!(
        "Unsupported resource mutation: mode '{}' is not valid for this resource.",
        mode_name(mode)
    );
    match contract_for(kind) {
        Some(contract) if !contract.mutations.is_empty() => {
            out.push_str(" Supported mutations:");
            for spec in contract.mutations {
                out.push_str(&format!(
                    "\n- {} (mode={}): {}",
                    spec.label,
                    mode_name(spec.mode),
                    spec.example
                ));
            }
        }
        _ => out.push_str(" This resource is read-only."),
    }
    out.push_str(" See cairn://help for the full (resource, mode) mutation matrix.");
    out
}

/// Build the "missing required key" rejection naming the absent keys + example.
fn render_missing_keys(spec: &MutationSpec, missing: &[&str]) -> String {
    format!(
        "Missing required payload key(s) for '{}': {}. Example: {}",
        spec.label,
        missing.join(", "),
        spec.example
    )
}

/// Collect the required keys (by canonical name) absent from the payload.
/// Aliases count as present; an empty `required` set never reports a miss.
fn missing_required_keys<'a>(
    spec: &'a MutationSpec,
    payload: Option<&serde_json::Value>,
) -> Vec<&'a str> {
    if spec.required.is_empty() {
        return Vec::new();
    }
    let keys: Vec<&str> = payload
        .and_then(|p| p.as_object())
        .map(|map| map.keys().map(String::as_str).collect())
        .unwrap_or_default();
    spec.required
        .iter()
        .filter(|req| !req.satisfied_by(keys.iter().copied()))
        .map(|req| req.key)
        .collect()
}

/// Build the "unknown payload key" rejection naming the offending keys and
/// enumerating what the mutation does accept, mirroring the missing-key
/// rejection so one round trip teaches the correct payload.
fn render_unknown_keys(spec: &MutationSpec, unknown: &[&str]) -> String {
    let accepted = match spec.accepted_keys_display() {
        Some(keys) => format!("Accepted keys: {keys}."),
        None => "This mutation takes no payload keys.".to_string(),
    };
    format!(
        "Unknown payload key(s) for '{}': {}. {} Example: {}",
        spec.label,
        unknown
            .iter()
            .map(|key| format!("`{key}`"))
            .collect::<Vec<_>>()
            .join(", "),
        accepted,
        spec.example
    )
}

/// Collect the top-level payload keys the spec neither requires nor accepts.
/// Aliases count as known, mirroring `missing_required_keys`. A non-object or
/// absent payload contributes no keys.
///
/// Only the top level is checked: the contract enumerates a mutation's payload
/// shape, while the interior of a nested value (`{snapshot:{...}}`, a todo item,
/// a settings sub-object) stays the handler's to validate.
fn unknown_payload_keys<'a>(
    spec: &MutationSpec,
    payload: Option<&'a serde_json::Value>,
) -> Vec<&'a str> {
    payload
        .and_then(|p| p.as_object())
        .map(|map| {
            map.keys()
                .map(String::as_str)
                .filter(|key| !spec.accepts_key(key))
                .collect()
        })
        .unwrap_or_default()
}

/// Table-authoritative gate: confirm the (kind, mode) pair is supported, then
/// shallow-check the payload's top-level keys against the contract in both
/// directions — required keys must be present, and every key present must be
/// one the mutation declares. Deep validation happens in the dispatch arm
/// afterwards.
///
/// Rejecting unknown keys is what keeps a caller error from becoming phantom
/// success: before this, a mistyped or unsupported key was silently dropped and
/// the write reported as delivered (CAIRN #4136).
fn gate_resource_change(
    index: usize,
    item: &ChangeItem,
    resource: &CairnResource,
) -> ResourceMutationResult<&'static MutationSpec> {
    if let Some(refusal) = crate::issues::retired_depends_on(item.payload.as_ref()) {
        return Err(build_failure(index, item, refusal));
    }
    let kind = resource.kind();
    if let Some(refusal) = retired_model_keys_refusal(kind, item.payload.as_ref()) {
        return Err(build_failure(index, item, refusal));
    }
    if kind == ResourceKind::IssueExecutions && item.mode == ChangeMode::Append {
        if let Some(refusal) =
            crate::tenets::filing_tenet_keys_refusal(item.payload.as_ref(), "payload.")
        {
            return Err(build_failure(index, item, refusal));
        }
    }
    if matches!(
        kind,
        ResourceKind::Projects | ResourceKind::Project | ResourceKind::ProjectSettings
    ) {
        if let Some(refusal) = crate::tenets::operator_only_policy_refusal(item.payload.as_ref()) {
            return Err(build_failure(index, item, refusal));
        }
    }
    if matches!(kind, ResourceKind::Packs | ResourceKind::Pack) {
        if let Some(key) = item.payload.as_ref().and_then(crate::library::source_key) {
            return Err(build_failure(
                index,
                item,
                format!(
                    "payload.{key} is not accepted. {}",
                    crate::library::ADD_SOURCE_REFUSAL
                ),
            ));
        }
    }
    let candidates = candidate_specs(kind, item.mode);
    if candidates.is_empty() {
        return Err(build_failure(
            index,
            item,
            render_unsupported(kind, item.mode),
        ));
    }
    let schema_resolved = cairn_common::contract::payload_keys_are_schema_resolved(kind);
    let payload = item.payload.as_ref();

    // Track the closest near-miss so a rejection quotes the shape the caller was
    // evidently reaching for rather than whichever one happens to be first.
    let mut closest: Option<(usize, &'static MutationSpec, Vec<&str>, Vec<&str>)> = None;
    for spec in &candidates {
        let missing = missing_required_keys(spec, payload);
        let unknown = if schema_resolved {
            Vec::new()
        } else {
            unknown_payload_keys(spec, payload)
        };
        if missing.is_empty() && unknown.is_empty() {
            return Ok(spec);
        }
        let distance = missing.len() + unknown.len();
        if closest.as_ref().is_none_or(|(best, ..)| distance < *best) {
            closest = Some((distance, spec, missing, unknown));
        }
    }

    let (_, spec, missing, unknown) = closest.expect("candidates is non-empty");
    // A missing required key is reported ahead of an unknown one: it is the more
    // fundamental error, and naming it first keeps the correction ordered.
    let mut error = if missing.is_empty() {
        render_unknown_keys(spec, &unknown)
    } else {
        render_missing_keys(spec, &missing)
    };
    if candidates.len() > 1 {
        error.push_str(&render_alternatives(&candidates, spec));
    }
    Err(build_failure(index, item, error))
}

/// Every resource whose writes choose a model through `models`, the allotment.
const ALLOTMENT_KINDS: &[ResourceKind] = &[
    ResourceKind::NodeTasks,
    ResourceKind::NodeCalls,
    ResourceKind::Agents,
    ResourceKind::Agent,
    ResourceKind::ProjectAgents,
    ResourceKind::ProjectAgent,
    ResourceKind::Responses,
    ResourceKind::Response,
    ResourceKind::ProjectResponses,
    ResourceKind::ProjectResponse,
];

/// The refusal for a write that still names a model key `models` retired.
///
/// The contract no longer declares these keys, so without this the gate would
/// refuse them as merely unknown; a caller still sending `tier` believes it is
/// choosing a model, and is told what replaced it instead.
fn retired_model_keys_refusal(
    kind: ResourceKind,
    payload: Option<&serde_json::Value>,
) -> Option<String> {
    let payload = payload?;
    if ALLOTMENT_KINDS.contains(&kind) {
        return crate::mcp::types::model_keys_problem(payload);
    }
    match kind {
        ResourceKind::IssueExecutions => payload
            .get("backend")
            .map(|_| executions::RETIRED_EXECUTION_BACKEND.to_string()),
        ResourceKind::Settings => payload
            .as_object()?
            .keys()
            .find_map(|key| super::settings::retired_settings_key(key)),
        _ => None,
    }
}

/// Every mutation this resource declares for `mode`. More than one means the
/// mutation accepts alternative payload shapes.
fn candidate_specs(kind: ResourceKind, mode: ChangeMode) -> Vec<&'static MutationSpec> {
    contract_for(kind)
        .map(|contract| {
            contract
                .mutations
                .iter()
                .filter(|spec| spec.mode == mode)
                .collect()
        })
        .unwrap_or_default()
}

/// Name the payload shapes a rejection did not quote, so a caller who aimed at
/// the other one is not told to fix the wrong thing.
fn render_alternatives(candidates: &[&'static MutationSpec], quoted: &MutationSpec) -> String {
    let mut out = String::from(" This mode also accepts a different payload shape:");
    for spec in candidates {
        if std::ptr::eq(*spec, quoted) {
            continue;
        }
        out.push_str(&format!(
            " '{}' ({}).",
            spec.label,
            spec.accepted_keys_display()
                .unwrap_or_else(|| "no payload".to_string())
        ));
    }
    out
}

/// Single dispatcher for resource-target mutations. `dry_run` selects between
/// computing a preview summary (no side effects) and executing the mutation.
///
/// Gate-first: the contract table decides whether a `(kind, mode)` pair is
/// routable before any typed parser runs. A rejection here enumerates the
/// resource's valid mutations; the typed arms below still perform deep
/// validation.
pub(crate) async fn dispatch_resource_change(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
) -> ResourceMutationResult<ResourceAppliedChange> {
    dispatch_resource_change_under_store(orch, request, index, item, dry_run, None).await
}

/// [`dispatch_resource_change`] for a caller that holds the store guard of the
/// project it is writing, as a write batch with file targets does. A resource
/// that writes a declared-base relationship on that store runs under `held_store`
/// rather than re-acquiring the non-reentrant lock.
pub(crate) async fn dispatch_resource_change_under_store(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    held_store: Option<&crate::orchestrator::JjStoreGuard>,
) -> ResourceMutationResult<ResourceAppliedChange> {
    let resource = target_resource_for_request(orch, request, item)
        .await
        .map_err(|e| build_failure(index, item, e))?;
    if let Some(refusal) = tenets::retired_shape_refusal(&resource, item) {
        return Err(build_failure(index, item, refusal));
    }
    if let Some(refusal) = tenets::edit_refusal(orch, &resource, item).await {
        return Err(build_failure(index, item, refusal));
    }
    gate_resource_change(index, item, &resource)?;

    // Optional structured echo of the post-mutation state, surfaced to UI
    // renderers via the change result. Set by the issue, todos, and pack arms,
    // and as `{tenet: <uri>}` by every door that proposes a tenet (a tenets
    // append, an executions append, an issue create's execution block) so the
    // transcript finds the proposal without parsing the summary.
    let mut applied_data: Option<serde_json::Value> = None;
    let mut promoted_memory = None;
    let mut deferred_pr_sync = None;

    let summary = if let Some(summary) =
        off_frame(|| posts::dispatch(orch, request, index, item, dry_run, &resource)).await?
    {
        summary
    } else if let Some(summary) = {
        let (resource, applied_data) = (&resource, &mut applied_data);
        off_frame(move || {
            tenets::dispatch(orch, request, index, item, dry_run, resource, applied_data)
        })
    }
    .await?
    {
        summary
    } else if let Some(summary) =
        off_frame(|| feed::dispatch(orch, request, index, item, dry_run, &resource)).await?
    {
        summary
    } else if let Some(summary) =
        off_frame(|| camps::dispatch(orch, request, index, item, dry_run, &resource)).await?
    {
        summary
    } else if let Some(summary) = {
        let (resource, applied_data) = (&resource, &mut applied_data);
        off_frame(move || {
            threads::dispatch(orch, request, index, item, dry_run, resource, applied_data)
        })
    }
    .await?
    {
        summary
    } else if let Some(summary) = {
        let (resource, applied_data) = (&resource, &mut applied_data);
        off_frame(move || {
            issues::dispatch(orch, request, index, item, dry_run, resource, applied_data)
        })
    }
    .await?
    {
        summary
    } else if let Some(summary) =
        off_frame(|| messages::dispatch(orch, request, index, item, dry_run, &resource)).await?
    {
        summary
    } else if let Some(summary) =
        off_frame(|| progress::dispatch(orch, request, index, item, dry_run, &resource)).await?
    {
        summary
    } else if let Some(summary) =
        off_frame(|| rebase::dispatch(orch, request, index, item, dry_run, &resource)).await?
    {
        summary
    } else if let Some(summary) =
        off_frame(|| batches::dispatch(orch, request, index, item, dry_run, &resource)).await?
    {
        summary
    } else if let Some(summary) =
        off_frame(|| attachments::dispatch(orch, request, index, item, dry_run, &resource)).await?
    {
        summary
    } else if let Some(summary) =
        off_frame(|| terminals::dispatch(orch, request, index, item, dry_run, &resource)).await?
    {
        summary
    } else if let Some(summary) =
        off_frame(|| services::dispatch(orch, request, index, item, dry_run, &resource)).await?
    {
        summary
    } else if let Some(summary) =
        off_frame(|| repls::dispatch(orch, request, index, item, dry_run, &resource)).await?
    {
        summary
    } else if let Some(summary) =
        off_frame(|| browsers::dispatch(orch, request, index, item, dry_run, &resource)).await?
    {
        summary
    } else if let Some(summary) =
        off_frame(|| executions::dispatch(orch, request, index, item, dry_run, &resource)).await?
    {
        summary
    } else if let Some(summary) =
        off_frame(|| queue::dispatch(orch, request, index, item, dry_run, &resource)).await?
    {
        summary
    } else if let Some(summary) =
        off_frame(|| nodes::dispatch(orch, request, index, item, dry_run, &resource)).await?
    {
        summary
    } else if let Some(summary) =
        off_frame(|| skills::dispatch(orch, request, index, item, dry_run, &resource)).await?
    {
        summary
    } else if let Some(summary) =
        off_frame(|| projects::dispatch(orch, request, index, item, dry_run, &resource, held_store))
            .await?
    {
        summary
    } else if let Some(summary) = {
        let (resource, promoted_memory) = (&resource, &mut promoted_memory);
        off_frame(move || {
            memories::dispatch(
                orch,
                request,
                index,
                item,
                dry_run,
                resource,
                promoted_memory,
            )
        })
    }
    .await?
    {
        summary
    } else if let Some(summary) =
        off_frame(|| labels::dispatch(orch, request, index, item, dry_run, &resource)).await?
    {
        summary
    } else if let Some(summary) = off_frame(|| {
        requests::dispatch(
            orch,
            request,
            index,
            item,
            dry_run,
            &resource,
            &mut applied_data,
        )
    })
    .await?
    {
        summary
    } else if let Some(summary) =
        off_frame(|| grants::dispatch(orch, request, index, item, dry_run, &resource)).await?
    {
        summary
    } else if let Some(summary) =
        off_frame(|| models::dispatch(orch, request, index, item, dry_run, &resource)).await?
    {
        summary
    } else if let Some(summary) = off_frame(|| {
        super::models_clients::dispatch(orch, request, index, item, dry_run, &resource)
    })
    .await?
    {
        summary
    } else if let Some(summary) = {
        let (resource, applied_data) = (&resource, &mut applied_data);
        off_frame(move || {
            inference::dispatch(orch, request, index, item, dry_run, resource, applied_data)
        })
    }
    .await?
    {
        summary
    } else if let Some(summary) =
        off_frame(|| models_accounts::dispatch(orch, request, index, item, dry_run, &resource))
            .await?
    {
        summary
    } else if let Some(summary) = {
        let (resource, applied_data) = (&resource, &mut applied_data);
        off_frame(move || {
            packs::dispatch(orch, request, index, item, dry_run, resource, applied_data)
        })
    }
    .await?
    {
        summary
    } else if let Some(summary) = {
        let (resource, applied_data) = (&resource, &mut applied_data);
        off_frame(move || {
            todos::dispatch(orch, request, index, item, dry_run, resource, applied_data)
        })
    }
    .await?
    {
        summary
    } else if let Some(summary) =
        off_frame(|| pages::dispatch(orch, request, index, item, dry_run, &resource)).await?
    {
        summary
    } else if let Some(summary) = {
        let (resource, deferred_pr_sync) = (&resource, &mut deferred_pr_sync);
        off_frame(move || {
            artifacts::dispatch(
                orch,
                request,
                index,
                item,
                dry_run,
                resource,
                held_store,
                deferred_pr_sync,
            )
        })
    }
    .await?
    {
        summary
    } else if let Some(summary) =
        off_frame(|| bug::dispatch(orch, request, index, item, dry_run, &resource)).await?
    {
        summary
    } else if let Some(summary) = dev_launch_ticket::dispatch(
        orch,
        request,
        index,
        item,
        dry_run,
        &resource,
        &mut applied_data,
    )
    .await?
    {
        summary
    } else if let Some(summary) =
        off_frame(|| wakes::dispatch(orch, request, index, item, dry_run, &resource)).await?
    {
        summary
    } else if let Some(summary) =
        off_frame(|| prompts::dispatch(orch, request, index, item, dry_run, &resource)).await?
    {
        summary
    } else if let Some(summary) =
        off_frame(|| recipes::dispatch(orch, request, index, item, dry_run, &resource)).await?
    {
        summary
    } else if let Some(summary) =
        off_frame(|| routes::dispatch(orch, request, index, item, dry_run, &resource)).await?
    {
        summary
    } else if let Some(summary) =
        off_frame(|| responses::dispatch(orch, request, index, item, dry_run, &resource)).await?
    {
        summary
    } else if let Some(summary) =
        off_frame(|| events::dispatch(orch, request, index, item, dry_run, &resource)).await?
    {
        summary
    } else if let Some(summary) =
        off_frame(|| agents::dispatch(orch, request, index, item, dry_run, &resource)).await?
    {
        summary
    } else if let Some(summary) =
        off_frame(|| actions::dispatch(orch, request, index, item, dry_run, &resource)).await?
    {
        summary
    } else if let Some(summary) =
        off_frame(|| settings::dispatch(orch, request, index, item, dry_run, &resource)).await?
    {
        summary
    } else if let Some(summary) =
        off_frame(|| mcp::dispatch(orch, request, index, item, dry_run, &resource)).await?
    {
        summary
    } else if let Some(summary) =
        off_frame(|| executors::dispatch(orch, request, index, item, dry_run, &resource)).await?
    {
        summary
    } else if let Some(summary) =
        off_frame(|| channels::dispatch(orch, request, index, item, dry_run, &resource)).await?
    {
        summary
    } else {
        return Err(build_failure(
            index,
            item,
            format!(
                "internal: contract allows mode '{}' on this resource but no dispatch arm handles it",
                mode_name(item.mode)
            ),
        ));
    };
    // Browser writes can opt into an inline post-action page read via
    // `?return_content=true` on the target URI, halving the round-trip for a
    // "navigate/click then read". Browser-scoped and best-effort: the mutation
    // already applied, so a render failure rides along as appended text rather
    // than failing the change.
    let summary = if !dry_run
        && wants_return_content(&item.target)
        && matches!(
            resource,
            CairnResource::NodeBrowser { .. }
                | CairnResource::TaskBrowser { .. }
                | CairnResource::ProjectBrowser { .. }
        ) {
        let page = crate::resources::browsers::render_browser(
            orch,
            &resource,
            crate::browsers::BridgeFormat::Markdown,
        )
        .await;
        format!("{summary}\n\n{page}")
    } else {
        summary
    };

    if !dry_run {
        let resource_uri = resource.to_uri();
        if let Err(error) = crate::orchestrator::wakes::route_resource_updated(orch, &resource_uri)
        {
            log::warn!("failed to route resource wake for {resource_uri}: {error}");
        }
    }

    Ok(ResourceAppliedChange {
        index,
        target: item.target.clone(),
        mode: mode_name(item.mode).to_string(),
        kind: "resource".to_string(),
        summary,
        data: applied_data,
        promoted_memory,
        deferred_pr_sync,
    })
}

/// Whether a browser write target opted into an inline post-action page read
/// via `?return_content=true` on the target URI. The mutation path parses the
/// resource without its query string, so the flag is read off the raw target.
/// `return_content`, `return_content=true`, and `return_content=1` all enable it.
fn wants_return_content(target: &str) -> bool {
    target
        .split_once('?')
        .map(|(_, query)| {
            query.split('&').any(|pair| {
                let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
                key == "return_content" && matches!(value, "" | "true" | "1")
            })
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::{retired_model_keys_refusal, ALLOTMENT_KINDS};
    use cairn_common::contract::contract_for;
    use cairn_uri::ResourceKind;
    use serde_json::json;

    /// Each retired model key is refused with what replaced it, on every
    /// resource that took it, while `models` itself passes.
    #[test]
    fn a_retired_model_key_is_refused_with_its_replacement() {
        for kind in ALLOTMENT_KINDS {
            for key in ["tier", "model", "backend", "backendPreference"] {
                let refusal = retired_model_keys_refusal(*kind, Some(&json!({key: "lg"})))
                    .unwrap_or_else(|| panic!("{kind:?} refuses {key}"));
                assert!(refusal.contains("models"), "{kind:?} {key}: {refusal}");
            }
            assert_eq!(
                retired_model_keys_refusal(*kind, Some(&json!({"models": ["lg"]}))),
                None
            );
        }
        let execution = retired_model_keys_refusal(
            ResourceKind::IssueExecutions,
            Some(&json!({"recipe": "build", "backend": "codex"})),
        )
        .unwrap();
        assert!(execution.contains("overrides.agents"), "{execution}");
        for key in ["tierDefaults", "tiers", "backends"] {
            let settings =
                retired_model_keys_refusal(ResourceKind::Settings, Some(&json!({key: {}})))
                    .unwrap_or_else(|| panic!("settings refuses {key}"));
            assert!(settings.contains("cairn://models/ladder"), "{settings}");
        }
        assert_eq!(
            retired_model_keys_refusal(ResourceKind::Settings, Some(&json!({"models": {}}))),
            None
        );
    }

    /// What the gate refuses as retired, no contract still advertises, so the
    /// affordance block never teaches a key the write would refuse.
    #[test]
    fn no_contract_still_declares_a_retired_model_key() {
        let declared = |kind: ResourceKind, key: &str| {
            contract_for(kind)
                .is_some_and(|contract| contract.mutations.iter().any(|spec| spec.accepts_key(key)))
        };
        for kind in ALLOTMENT_KINDS {
            for key in ["tier", "model", "backend", "backendPreference"] {
                assert!(!declared(*kind, key), "{kind:?} still declares {key}");
            }
        }
        assert!(!declared(ResourceKind::IssueExecutions, "backend"));
        for key in ["tierDefaults", "tiers", "backends"] {
            assert!(
                !declared(ResourceKind::Settings, key),
                "settings declares {key}"
            );
        }
    }
}
