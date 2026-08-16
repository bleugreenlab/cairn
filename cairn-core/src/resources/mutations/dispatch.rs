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
mod executors;
mod feed;
mod grants;
mod issues;
mod labels;
mod mcp;
mod memories;
mod messages;
mod nodes;
mod packs;
mod posts;
mod progress;
mod projects;
mod prompts;
mod rebase;
mod recipes;
mod repls;
mod responses;
mod routes;
mod settings;
mod skills;
mod terminals;
mod threads;
mod todos;
mod wakes;

#[cfg(test)]
mod issue_mutation_tests;
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
use cairn_common::contract::{contract_for, MutationSpec, ResourceKind};
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
    let kind = resource.kind();
    let candidates = candidate_specs(kind, item.mode);
    if candidates.is_empty() {
        return Err(build_failure(
            index,
            item,
            render_unsupported(kind, item.mode),
        ));
    }
    let schema_resolved = kind.payload_keys_are_schema_resolved();
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
    let resource = target_resource_for_request(orch, request, item)
        .await
        .map_err(|e| build_failure(index, item, e))?;
    gate_resource_change(index, item, &resource)?;

    // Optional structured echo of the post-mutation state, surfaced to UI
    // renderers via the change result. Set by the issue, todos, and pack arms.
    let mut applied_data: Option<serde_json::Value> = None;
    let mut promoted_memory = None;

    let summary = if let Some(summary) =
        posts::dispatch(orch, request, index, item, dry_run, &resource).await?
    {
        summary
    } else if let Some(summary) =
        feed::dispatch(orch, request, index, item, dry_run, &resource).await?
    {
        summary
    } else if let Some(summary) =
        threads::dispatch(orch, request, index, item, dry_run, &resource).await?
    {
        summary
    } else if let Some(summary) = issues::dispatch(
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
        rebase::dispatch(orch, request, index, item, dry_run, &resource).await?
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
    } else if let Some(summary) =
        grants::dispatch(orch, request, index, item, dry_run, &resource).await?
    {
        summary
    } else if let Some(summary) = packs::dispatch(
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
        routes::dispatch(orch, request, index, item, dry_run, &resource).await?
    {
        summary
    } else if let Some(summary) =
        responses::dispatch(orch, request, index, item, dry_run, &resource).await?
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
    } else if let Some(summary) =
        executors::dispatch(orch, request, index, item, dry_run, &resource).await?
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
