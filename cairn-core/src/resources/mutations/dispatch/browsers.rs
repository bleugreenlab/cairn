//! Browser resource mutation dispatch, relocated from dispatch.rs.

use super::super::browsers::{
    apply_browser_action, apply_browser_delete, apply_browser_ensure, BrowserInteractionArgs,
};
use super::super::{
    build_failure, payload_bool, payload_str, payload_trimmed_non_empty_str, ResourceMutationResult,
};
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use cairn_common::uri::CairnResource;

pub(super) async fn dispatch(
    orch: &Orchestrator,
    _request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    resource: &CairnResource,
) -> ResourceMutationResult<Option<String>> {
    let summary = match (resource, item.mode) {
        (
            CairnResource::NodeBrowser { slug, .. }
            | CairnResource::TaskBrowser { slug, .. }
            | CairnResource::ProjectBrowser { slug, .. },
            ChangeMode::Create,
        ) => {
            let url = item
                .payload
                .as_ref()
                .and_then(|payload| payload_trimmed_non_empty_str(payload, "url", &["navigate"]))
                .map(ToOwned::to_owned);
            if dry_run {
                match &url {
                    Some(url) => format!("Would open browser {slug} at {url}"),
                    None => format!("Would open browser {slug}"),
                }
            } else {
                apply_browser_ensure(orch, resource, url)
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        (
            CairnResource::NodeBrowser { slug, .. }
            | CairnResource::TaskBrowser { slug, .. }
            | CairnResource::ProjectBrowser { slug, .. },
            ChangeMode::Replace,
        ) => {
            // `replace` is the agent-intuitive "go to a URL / set the page":
            // an idempotent ensure + navigate, identical to `patch` with a url
            // but with the url required. Missing url is a hard error with guidance.
            let url = item
                .payload
                .as_ref()
                .and_then(|payload| payload_trimmed_non_empty_str(payload, "url", &["navigate"]))
                .map(ToOwned::to_owned)
                .ok_or_else(|| {
                    build_failure(
                        index,
                        item,
                        "mode=replace sets the page URL; provide payload.url",
                    )
                })?;
            if dry_run {
                format!("Would navigate browser {slug} to {url}")
            } else {
                apply_browser_ensure(orch, resource, Some(url))
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        (
            CairnResource::NodeBrowser { slug, .. }
            | CairnResource::TaskBrowser { slug, .. }
            | CairnResource::ProjectBrowser { slug, .. },
            ChangeMode::Patch,
        ) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
            let url =
                payload_trimmed_non_empty_str(payload, "url", &["navigate"]).map(ToOwned::to_owned);
            let action =
                payload_trimmed_non_empty_str(payload, "action", &[]).map(ToOwned::to_owned);
            if url.is_none() && action.is_none() {
                return Err(build_failure(
                    index,
                    item,
                    "payload requires url/navigate or action (back|forward|reload|click|type|scroll|waitFor|waitForNavigation|waitForLoad)",
                ));
            }
            let args = BrowserInteractionArgs {
                selector: payload_trimmed_non_empty_str(payload, "selector", &[])
                    .map(ToOwned::to_owned),
                text: payload_trimmed_non_empty_str(payload, "text", &[]).map(ToOwned::to_owned),
                handle: payload_trimmed_non_empty_str(payload, "handle", &["ref"])
                    .map(ToOwned::to_owned),
                // value may legitimately be empty (clearing a field), so it is
                // not trimmed-non-empty filtered.
                value: payload_str(payload, "value", &[]).map(ToOwned::to_owned),
                to: payload_trimmed_non_empty_str(payload, "to", &[]).map(ToOwned::to_owned),
                by: payload.get("by").and_then(serde_json::Value::as_i64),
                timeout_ms: payload
                    .get("timeoutMs")
                    .or_else(|| payload.get("timeout_ms"))
                    .and_then(serde_json::Value::as_u64),
                submit: payload_bool(payload, "submit"),
                kinds: payload
                    .get("kinds")
                    .and_then(serde_json::Value::as_array)
                    .map(|values| {
                        values
                            .iter()
                            .filter_map(|value| value.as_str().map(ToOwned::to_owned))
                            .collect()
                    }),
            };
            if dry_run {
                match (&url, &action) {
                    (Some(url), _) => format!("Would navigate browser {slug} to {url}"),
                    (None, Some(action)) => format!("Would {action} browser {slug}"),
                    (None, None) => unreachable!("guarded above"),
                }
            } else {
                match url {
                    // navigate (idempotent ensure) vs drive an action.
                    Some(url) => apply_browser_ensure(orch, resource, Some(url)).await,
                    None => {
                        let action = action.expect("guarded: url or action is present");
                        apply_browser_action(orch, resource, action, args).await
                    }
                }
                .map_err(|error| build_failure(index, item, error))?
            }
        }
        (
            CairnResource::NodeBrowser { slug, .. }
            | CairnResource::TaskBrowser { slug, .. }
            | CairnResource::ProjectBrowser { slug, .. },
            ChangeMode::Delete,
        ) => {
            if item.payload.is_some() {
                return Err(build_failure(
                    index,
                    item,
                    "mode=delete does not accept payload",
                ));
            }
            if dry_run {
                format!("Would close browser {slug}")
            } else {
                apply_browser_delete(orch, resource)
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(summary))
}
