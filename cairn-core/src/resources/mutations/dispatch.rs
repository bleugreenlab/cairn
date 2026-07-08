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
mod browsers;
mod bug;
mod executions;
mod issues;
mod labels;
mod mcp;
mod memories;
mod messages;
mod nodes;
mod progress;
mod projects;
mod prompts;
mod recipes;
mod repls;
mod settings;
mod skills;
mod terminals;
mod todos;
mod wakes;

#[cfg(test)]
mod issue_mutation_tests;
#[cfg(test)]
mod resource_gate_tests;

use super::{
    build_failure, mode_name, target_resource_for_request, ResourceAppliedChange,
    ResourceMutationResult,
};
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use cairn_common::contract::{contract_for, mutation_spec, MutationSpec, ResourceKind};
use cairn_common::uri::CairnResource;

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

/// Table-authoritative gate: confirm the (kind, mode) pair is supported and
/// shallow-check required payload keys. Deep validation happens in the dispatch
/// arm afterwards.
fn gate_resource_change(
    index: usize,
    item: &ChangeItem,
    resource: &CairnResource,
) -> ResourceMutationResult<&'static MutationSpec> {
    let kind = resource.kind();
    let spec = mutation_spec(kind, item.mode)
        .ok_or_else(|| build_failure(index, item, render_unsupported(kind, item.mode)))?;
    let missing = missing_required_keys(spec, item.payload.as_ref());
    if !missing.is_empty() {
        return Err(build_failure(
            index,
            item,
            render_missing_keys(spec, &missing),
        ));
    }
    Ok(spec)
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
    let resource = target_resource_for_request(orch, request, item)
        .await
        .map_err(|e| build_failure(index, item, e))?;
    gate_resource_change(index, item, &resource)?;

    // Optional structured echo of the post-mutation state, surfaced to UI
    // renderers via the change result. Currently set only by the todos arms.
    let mut applied_data: Option<serde_json::Value> = None;
    let mut promoted_memory = None;

    let summary = if let Some(summary) = issues::dispatch(
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
        messages::dispatch(orch, request, index, item, dry_run, &resource).await?
    {
        summary
    } else if let Some(summary) =
        progress::dispatch(orch, request, index, item, dry_run, &resource).await?
    {
        summary
    } else if let Some(summary) =
        terminals::dispatch(orch, request, index, item, dry_run, &resource).await?
    {
        summary
    } else if let Some(summary) =
        repls::dispatch(orch, request, index, item, dry_run, &resource).await?
    {
        summary
    } else if let Some(summary) =
        browsers::dispatch(orch, request, index, item, dry_run, &resource).await?
    {
        summary
    } else if let Some(summary) =
        executions::dispatch(orch, request, index, item, dry_run, &resource).await?
    {
        summary
    } else if let Some(summary) =
        nodes::dispatch(orch, request, index, item, dry_run, &resource).await?
    {
        summary
    } else if let Some(summary) =
        skills::dispatch(orch, request, index, item, dry_run, &resource).await?
    {
        summary
    } else if let Some(summary) =
        projects::dispatch(orch, request, index, item, dry_run, &resource).await?
    {
        summary
    } else if let Some(summary) = memories::dispatch(
        orch,
        request,
        index,
        item,
        dry_run,
        &resource,
        &mut promoted_memory,
    )
    .await?
    {
        summary
    } else if let Some(summary) =
        labels::dispatch(orch, request, index, item, dry_run, &resource).await?
    {
        summary
    } else if let Some(summary) = todos::dispatch(
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
        artifacts::dispatch(orch, request, index, item, dry_run, &resource).await?
    {
        summary
    } else if let Some(summary) =
        bug::dispatch(orch, request, index, item, dry_run, &resource).await?
    {
        summary
    } else if let Some(summary) =
        wakes::dispatch(orch, request, index, item, dry_run, &resource).await?
    {
        summary
    } else if let Some(summary) =
        prompts::dispatch(orch, request, index, item, dry_run, &resource).await?
    {
        summary
    } else if let Some(summary) =
        recipes::dispatch(orch, request, index, item, dry_run, &resource).await?
    {
        summary
    } else if let Some(summary) =
        agents::dispatch(orch, request, index, item, dry_run, &resource).await?
    {
        summary
    } else if let Some(summary) =
        actions::dispatch(orch, request, index, item, dry_run, &resource).await?
    {
        summary
    } else if let Some(summary) =
        settings::dispatch(orch, request, index, item, dry_run, &resource).await?
    {
        summary
    } else if let Some(summary) =
        mcp::dispatch(orch, request, index, item, dry_run, &resource).await?
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
