//! URI resource mutation dispatch and resource-target preview hashing.

mod actions;
mod agents;
mod blocking;
mod browsers;
mod dispatch;
mod hash;
mod labels;
mod mcp;
mod memories;
mod projects;
mod recipes;
mod settings;
mod skills;

pub(crate) use blocking::{blocking_append_kind, run_blocking_group, validate_blocking_group};
pub(crate) use dispatch::dispatch_resource_change;
pub(crate) use hash::hash_resource_target;
pub(crate) use mcp::is_workspace_mcp_mutation;
pub(crate) use projects::project_write_crossing_path;
pub(crate) use settings::is_workspace_settings_mutation;

use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use cairn_common::uri::{parse_uri, CairnResource};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResourceTargetHash {
    pub(crate) target: String,
    pub(crate) kind: String,
    pub(crate) exists: bool,
    pub(crate) hash: String,
}

// `data` carries a structured, machine-readable echo of the post-mutation
// state for UI renderers (e.g. the todos snapshot). `serde_json::Value` is not
// `Eq`, so this struct is `PartialEq` only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PromotedMemoryRef {
    pub(crate) memory_id: String,
    pub(crate) memory_uri: String,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ResourceAppliedChange {
    pub(crate) index: usize,
    pub(crate) target: String,
    pub(crate) mode: String,
    pub(crate) kind: String,
    pub(crate) summary: String,
    pub(crate) data: Option<serde_json::Value>,
    pub(crate) promoted_memory: Option<PromotedMemoryRef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResourceMutationFailure {
    pub(crate) index: usize,
    pub(crate) target: String,
    pub(crate) mode: String,
    pub(crate) kind: String,
    pub(crate) error: String,
}

pub(super) type ResourceMutationResult<T> = Result<T, ResourceMutationFailure>;

pub(super) fn mode_name(mode: ChangeMode) -> &'static str {
    mode.as_str()
}

pub(super) fn build_failure(
    index: usize,
    item: &ChangeItem,
    error: impl Into<String>,
) -> ResourceMutationFailure {
    ResourceMutationFailure {
        index,
        target: item.target.clone(),
        mode: mode_name(item.mode).to_string(),
        kind: "resource".to_string(),
        error: error.into(),
    }
}

pub(super) fn payload_value<'a>(
    payload: &'a serde_json::Value,
    key: &str,
    aliases: &[&str],
) -> Option<&'a serde_json::Value> {
    std::iter::once(key)
        .chain(aliases.iter().copied())
        .find_map(|name| payload.get(name))
}

pub(super) fn payload_str<'a>(
    payload: &'a serde_json::Value,
    key: &str,
    aliases: &[&str],
) -> Option<&'a str> {
    payload_value(payload, key, aliases).and_then(|value| value.as_str())
}

pub(super) fn payload_bool(payload: &serde_json::Value, key: &str) -> Option<bool> {
    payload_value(payload, key, &[]).and_then(|value| value.as_bool())
}

pub(super) fn payload_non_empty_str<'a>(
    payload: &'a serde_json::Value,
    key: &str,
    aliases: &[&str],
) -> Option<&'a str> {
    payload_str(payload, key, aliases).filter(|value| !value.is_empty())
}

pub(super) fn payload_trimmed_non_empty_str<'a>(
    payload: &'a serde_json::Value,
    key: &str,
    aliases: &[&str],
) -> Option<&'a str> {
    payload_str(payload, key, aliases)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

pub(super) async fn scope_project_path(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    explicit_project: Option<&str>,
) -> Result<Option<std::path::PathBuf>, String> {
    match explicit_project {
        Some(project) => Ok(Some(
            crate::mcp::handlers::skills_resources::project_path_by_key(orch, project).await?,
        )),
        None => Ok(
            crate::mcp::handlers::skills_resources::current_run_project(orch, request)
                .await
                .and_then(|(_, path)| path),
        ),
    }
}

/// Resolve a home-relative (`cairn:~/...`) change target to its canonical URI.
/// The dispatch path resolves this itself, but blocking-append classification
/// runs on the raw target BEFORE dispatch, so the write handler resolves up
/// front to classify uniformly. A canonical or `file:` target is returned
/// unchanged. SDK writers send `cairn:~/` raw; `cairn-cmd` resolves it
/// client-side, so agents already send canonical targets.
pub(crate) async fn resolve_change_target_uri(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    target: &str,
) -> Result<String, String> {
    super::common::resolve_home_relative_resource_uri(&orch.db, request, target).await
}

pub(super) async fn target_resource_for_request(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    item: &ChangeItem,
) -> Result<CairnResource, String> {
    let target =
        super::common::resolve_home_relative_resource_uri(&orch.db, request, &item.target).await?;
    parse_uri(&target).ok_or_else(|| {
        crate::mcp::handlers::resources::task_terminal_hint(&target)
            .unwrap_or_else(|| format!("Unrecognized URI format: {}", item.target))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_string_helpers_handle_aliases_and_trimming() {
        let payload = serde_json::json!({
            "name": "  Alpha  ",
            "snake_name": "beta",
            "empty": "",
            "number": 42,
        });

        assert_eq!(
            payload_value(&payload, "missing", &["snake_name"]),
            payload.get("snake_name")
        );
        assert_eq!(payload_str(&payload, "name", &[]), Some("  Alpha  "));
        assert_eq!(
            payload_trimmed_non_empty_str(&payload, "name", &[]),
            Some("Alpha")
        );
        assert_eq!(payload_non_empty_str(&payload, "empty", &[]), None);
        assert_eq!(payload_str(&payload, "number", &[]), None);
    }

    #[test]
    fn payload_bool_reads_boolean_values_only() {
        let payload = serde_json::json!({
            "submit": false,
            "string_submit": "false",
        });

        assert_eq!(payload_bool(&payload, "submit"), Some(false));
        assert_eq!(payload_bool(&payload, "string_submit"), None);
        assert_eq!(payload_bool(&payload, "missing"), None);
    }
}
