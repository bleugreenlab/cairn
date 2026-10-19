//! The Codex app-server's thread parameters and output-schema normalization.

use cairn_domain::Model;
use serde_json::Value;

use crate::interface::session::{ApprovalMode, CodexNativeTools};

/// Codex's `approvalPolicy` for an approval mode. Cairn's verb handlers make
/// the user-facing fence decision; this only stops Codex asking on its own.
pub fn codex_approval_policy(approval: ApprovalMode) -> &'static str {
    match approval {
        ApprovalMode::Allow => "never",
        ApprovalMode::Ask | ApprovalMode::Deny => "on-request",
    }
}

pub fn codex_sandbox_mode(approval: ApprovalMode) -> &'static str {
    match approval {
        // Ask/deny agents get Codex's workspace-write sandbox; Cairn's verb
        // handlers provide the user-facing fence decision.
        ApprovalMode::Ask | ApprovalMode::Deny => "workspace-write",
        ApprovalMode::Allow => "danger-full-access",
    }
}

// Codex thread-start takes more parameters than clippy's default threshold;
// they are distinct primitive knobs, so a params struct would add indirection
// without improving clarity.
#[allow(clippy::too_many_arguments)]
pub fn build_thread_start_params(
    cwd: &str,
    approval_policy: &str,
    sandbox_mode: &str,
    model: Option<&str>,
    reasoning_effort: Option<&str>,
    service_tier: Option<&str>,
    base_instructions: &str,
    developer_instructions: Option<&str>,
    tool_free: bool,
    process_mcp_servers: &[String],
    native_tools: CodexNativeTools,
) -> Value {
    let mut params = serde_json::json!({
        "model": model.unwrap_or(Model::GPT_5_6_LUNA),
        "cwd": cwd,
        "approvalPolicy": approval_policy,
        "sandbox": sandbox_mode,
        "serviceName": "cairn",
        "baseInstructions": base_instructions,
    });
    let mut config = serde_json::Map::new();
    if tool_free {
        // No rollout persisted, no shell or environment, nothing writable, and
        // every tool Codex would offer without one switched off for this
        // thread only: each MCP server the process was started with
        // (verified against Codex 0.153.3: the thread never starts it), hosted
        // web search, the plan tool, and the ask-the-user tool. The one-shot
        // reader still fails closed on any item that is not a message, for
        // tools a later Codex adds.
        params["ephemeral"] = serde_json::json!(true);
        params["environments"] = serde_json::json!([]);
        params["sandbox"] = serde_json::json!("read-only");
        for server in process_mcp_servers {
            config.insert(
                crate::session::mcp::codex_thread_disable_key(server),
                serde_json::json!(false),
            );
        }
        config.insert("web_search".to_string(), serde_json::json!("disabled"));
        config.insert(
            "tools.update_plan.enabled".to_string(),
            serde_json::json!(false),
        );
        config.insert(
            "tools.experimental_request_user_input.enabled".to_string(),
            serde_json::json!(false),
        );
        // The home Cairn renders turns Codex's image tool off for every
        // thread; an image worker turns it back on for itself alone
        // (measured on codex-cli 0.162.1: a thread with this line is offered
        // `image_gen.imagegen`, a plain one on the same server is not).
        if native_tools == CodexNativeTools::ImageGeneration {
            config.insert(
                "features.image_generation".to_string(),
                serde_json::json!(true),
            );
        }
    }
    insert_effort_override(&mut config, reasoning_effort);
    if !config.is_empty() {
        params["config"] = Value::Object(config);
    }
    if let Some(model) = model {
        params["model"] = serde_json::json!(model);
    }
    if let Some(tier) = service_tier {
        params["serviceTier"] = serde_json::json!(tier);
    }
    if let Some(instructions) = developer_instructions {
        params["developerInstructions"] = serde_json::json!(instructions);
    }
    params
}

/// The thread methods (`thread/start`, `resume`, `fork`) have no effort
/// field; Codex takes a thread's effort only as a `config` override of
/// `model_reasoning_effort`. A key the protocol lacks is silently dropped,
/// which is how a top-level `reasoningEffort` went unapplied from March to
/// October 2026. Without an override the model's catalog default applies.
fn insert_effort_override(
    config: &mut serde_json::Map<String, Value>,
    reasoning_effort: Option<&str>,
) {
    if let Some(effort) = reasoning_effort {
        config.insert(
            "model_reasoning_effort".to_string(),
            serde_json::json!(effort),
        );
    }
}

#[allow(clippy::too_many_arguments)]
pub fn build_thread_resume_params(
    thread_id: &str,
    cwd: &str,
    approval_policy: &str,
    sandbox_mode: &str,
    model: Option<&str>,
    reasoning_effort: Option<&str>,
    service_tier: Option<&str>,
    base_instructions: &str,
    developer_instructions: Option<&str>,
) -> Value {
    let mut params = serde_json::json!({
        "threadId": thread_id,
        "cwd": cwd,
        "approvalPolicy": approval_policy,
        "sandbox": sandbox_mode,
        "baseInstructions": base_instructions,
    });
    let mut config = serde_json::Map::new();
    insert_effort_override(&mut config, reasoning_effort);
    if !config.is_empty() {
        params["config"] = Value::Object(config);
    }
    if let Some(model) = model {
        params["model"] = serde_json::json!(model);
    }
    if let Some(tier) = service_tier {
        params["serviceTier"] = serde_json::json!(tier);
    }
    if let Some(instructions) = developer_instructions {
        params["developerInstructions"] = serde_json::json!(instructions);
    }
    params
}

#[allow(clippy::too_many_arguments)]
pub fn build_thread_fork_params(
    thread_id: &str,
    cwd: &str,
    approval_policy: &str,
    sandbox_mode: &str,
    model: Option<&str>,
    reasoning_effort: Option<&str>,
    service_tier: Option<&str>,
    base_instructions: &str,
    developer_instructions: Option<&str>,
) -> Value {
    let mut params = build_thread_resume_params(
        thread_id,
        cwd,
        approval_policy,
        sandbox_mode,
        model,
        reasoning_effort,
        service_tier,
        base_instructions,
        developer_instructions,
    );
    params["deferGoalContinuation"] = serde_json::json!(true);
    params
}

pub fn is_missing_rollout_error(err: &str, thread_id: &str) -> bool {
    err.contains("no rollout found for thread id")
        && (thread_id.is_empty() || err.contains(thread_id))
}

pub fn codex_strict_output_schema(schema: &serde_json::Value) -> serde_json::Value {
    let mut schema = schema.clone();
    normalize_codex_strict_schema_node(&mut schema);
    schema
}

fn normalize_codex_strict_schema_node(schema: &mut serde_json::Value) {
    match schema {
        serde_json::Value::Object(map) => {
            for key in [
                "$defs",
                "definitions",
                "properties",
                "patternProperties",
                "dependentSchemas",
            ] {
                if let Some(serde_json::Value::Object(children)) = map.get_mut(key) {
                    for child in children.values_mut() {
                        normalize_codex_strict_schema_node(child);
                    }
                }
            }

            for key in [
                "items",
                "additionalItems",
                "contains",
                "propertyNames",
                "not",
                "if",
                "then",
                "else",
            ] {
                if let Some(child) = map.get_mut(key) {
                    normalize_codex_strict_schema_node(child);
                }
            }

            for key in ["anyOf", "oneOf", "allOf", "prefixItems"] {
                if let Some(serde_json::Value::Array(children)) = map.get_mut(key) {
                    for child in children {
                        normalize_codex_strict_schema_node(child);
                    }
                }
            }

            if is_json_object_schema(map) {
                let property_names = map
                    .get("properties")
                    .and_then(|value| value.as_object())
                    .map(|properties| properties.keys().cloned().collect::<Vec<_>>())
                    .unwrap_or_default();

                let mut required = map
                    .get("required")
                    .and_then(|value| value.as_array())
                    .map(|values| {
                        values
                            .iter()
                            .filter_map(|value| value.as_str().map(str::to_owned))
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();

                let originally_required = required
                    .iter()
                    .cloned()
                    .collect::<std::collections::HashSet<_>>();

                if let Some(serde_json::Value::Object(properties)) = map.get_mut("properties") {
                    for property_name in &property_names {
                        if !originally_required.contains(property_name) {
                            if let Some(property_schema) = properties.get_mut(property_name) {
                                make_schema_nullable(property_schema);
                            }
                        }
                    }
                }

                for property_name in property_names {
                    if !required
                        .iter()
                        .any(|required_name| required_name == &property_name)
                    {
                        required.push(property_name);
                    }
                }

                map.insert(
                    "required".to_string(),
                    serde_json::Value::Array(
                        required
                            .into_iter()
                            .map(serde_json::Value::String)
                            .collect(),
                    ),
                );
                map.insert(
                    "additionalProperties".to_string(),
                    serde_json::Value::Bool(false),
                );
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                normalize_codex_strict_schema_node(value);
            }
        }
        _ => {}
    }
}

fn is_json_object_schema(map: &serde_json::Map<String, serde_json::Value>) -> bool {
    if map.contains_key("properties") {
        return true;
    }

    match map.get("type") {
        Some(serde_json::Value::String(kind)) => kind == "object",
        Some(serde_json::Value::Array(kinds)) => kinds.iter().any(|kind| kind == "object"),
        _ => false,
    }
}

fn make_schema_nullable(schema: &mut serde_json::Value) {
    normalize_codex_strict_schema_node(schema);

    let serde_json::Value::Object(map) = schema else {
        return;
    };

    if let Some(serde_json::Value::Array(any_of)) = map.get_mut("anyOf") {
        if !any_of.iter().any(is_null_schema) {
            any_of.push(serde_json::json!({ "type": "null" }));
        }
        return;
    }

    if let Some(serde_json::Value::Array(one_of)) = map.get_mut("oneOf") {
        if !one_of.iter().any(is_null_schema) {
            one_of.push(serde_json::json!({ "type": "null" }));
        }
        return;
    }

    if let Some(serde_json::Value::Array(enum_values)) = map.get_mut("enum") {
        if !enum_values.iter().any(serde_json::Value::is_null) {
            enum_values.push(serde_json::Value::Null);
        }
        return;
    }

    match map.get_mut("type") {
        Some(serde_json::Value::String(kind)) if kind != "null" => {
            let kind = kind.clone();
            map.insert(
                "type".to_string(),
                serde_json::Value::Array(vec![
                    serde_json::Value::String(kind),
                    serde_json::Value::String("null".to_string()),
                ]),
            );
        }
        Some(serde_json::Value::Array(kinds)) => {
            if !kinds.iter().any(|kind| kind == "null") {
                kinds.push(serde_json::Value::String("null".to_string()));
            }
        }
        Some(_) => {}
        None => {
            let original = serde_json::Value::Object(map.clone());
            map.clear();
            map.insert(
                "anyOf".to_string(),
                serde_json::Value::Array(vec![original, serde_json::json!({ "type": "null" })]),
            );
        }
    }
}

fn is_null_schema(schema: &serde_json::Value) -> bool {
    schema
        .as_object()
        .and_then(|map| map.get("type"))
        .is_some_and(|kind| kind == "null")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn thread_start_params_send_base_and_developer_instructions_separately() {
        let params = build_thread_start_params(
            "/tmp/worktree",
            "on-request",
            "workspace-write",
            Some("gpt-test"),
            Some("medium"),
            Some("priority"),
            "BASE",
            Some("agent content"),
            false,
            &["cairn".to_string()],
            CodexNativeTools::None,
        );

        assert_eq!(params["baseInstructions"], "BASE");
        assert_eq!(params["developerInstructions"], "agent content");
        assert_eq!(params["serviceTier"], "priority");
        for key in ["ephemeral", "environments"] {
            assert!(params.get(key).is_none(), "{key} on a session with tools");
        }
        // A session with tools carries no tool-free overrides, only its effort.
        assert_eq!(
            params["config"],
            serde_json::json!({ "model_reasoning_effort": "medium" })
        );
    }

    /// A tool-free thread persists nothing, reaches no environment, can write
    /// nothing, and has the process-level `cairn` server off for itself.
    #[test]
    fn a_tool_free_thread_start_carries_the_tool_free_params() {
        let params = build_thread_start_params(
            "/tmp/scratch",
            "on-request",
            "workspace-write",
            Some("gpt-test"),
            None,
            None,
            "BASE",
            None,
            true,
            &["cairn".to_string()],
            CodexNativeTools::None,
        );
        assert_eq!(params["ephemeral"], true);
        assert_eq!(params["environments"], serde_json::json!([]));
        assert_eq!(params["sandbox"], "read-only");
        assert_eq!(
            params["config"],
            serde_json::json!({
                "mcp_servers.cairn.enabled": false,
                "web_search": "disabled",
                "tools.update_plan.enabled": false,
                "tools.experimental_request_user_input.enabled": false,
            })
        );

        // Every server the process carries is switched off, under its name.
        let several = build_thread_start_params(
            "/tmp/scratch",
            "on-request",
            "workspace-write",
            None,
            None,
            None,
            "BASE",
            None,
            true,
            &["tm".to_string(), "deck".to_string()],
            CodexNativeTools::None,
        );
        assert_eq!(several["config"]["mcp_servers.tm.enabled"], false);
        assert_eq!(several["config"]["mcp_servers.deck.enabled"], false);
        assert!(several["config"].get("mcp_servers.cairn.enabled").is_none());
    }

    /// An image worker is the tool-free thread plus exactly one line: its own
    /// image tool switched back on.
    #[test]
    fn an_image_worker_thread_adds_only_its_image_tool() {
        let plain = build_thread_start_params(
            "/tmp/scratch",
            "on-request",
            "workspace-write",
            None,
            Some("low"),
            None,
            "WORKER",
            None,
            true,
            &[],
            CodexNativeTools::None,
        );
        let worker = build_thread_start_params(
            "/tmp/scratch",
            "on-request",
            "workspace-write",
            None,
            Some("low"),
            None,
            "WORKER",
            None,
            true,
            &[],
            CodexNativeTools::ImageGeneration,
        );
        let mut expected = plain.clone();
        expected["config"]["features.image_generation"] = serde_json::json!(true);
        assert_eq!(worker, expected);
        assert!(plain["config"].get("features.image_generation").is_none());
    }

    /// Why a prompt edit does not need a fresh thread on Codex (CAIRN-4618).
    ///
    /// `thread/resume` carries the instruction stack in the request itself, and
    /// it is assembled fresh on every start. Whatever the thread was created
    /// with, resuming it applies the current prompt — so an edited agent prompt
    /// reaches a live Codex thread without replacing it.
    #[test]
    fn a_resumed_thread_carries_current_instructions() {
        let resumed = build_thread_resume_params(
            "thread-1",
            "/tmp/worktree",
            "on-request",
            "workspace-write",
            Some("gpt-test"),
            Some("medium"),
            None,
            "EDITED BASE",
            Some("EDITED AGENT PROMPT"),
        );

        assert_eq!(resumed["threadId"], "thread-1");
        assert_eq!(resumed["baseInstructions"], "EDITED BASE");
        assert_eq!(resumed["developerInstructions"], "EDITED AGENT PROMPT");

        // The same instructions a brand-new thread would be started with, so
        // neither path can carry an edit the other misses.
        let started = build_thread_start_params(
            "/tmp/worktree",
            "on-request",
            "workspace-write",
            Some("gpt-test"),
            Some("medium"),
            None,
            "EDITED BASE",
            Some("EDITED AGENT PROMPT"),
            false,
            &[],
            CodexNativeTools::None,
        );
        assert_eq!(resumed["baseInstructions"], started["baseInstructions"]);
        assert_eq!(
            resumed["developerInstructions"],
            started["developerInstructions"]
        );
    }

    #[test]
    fn thread_resume_params_send_base_and_developer_instructions_separately() {
        let params = build_thread_resume_params(
            "thread-1",
            "/tmp/worktree",
            "on-request",
            "workspace-write",
            Some("gpt-test"),
            Some("medium"),
            None,
            "BASE",
            Some("agent content"),
        );

        assert_eq!(params["baseInstructions"], "BASE");
        assert_eq!(params["developerInstructions"], "agent content");
    }

    #[test]
    fn empty_developer_instructions_are_omitted() {
        let developer: Option<&str> = None;
        let params = build_thread_start_params(
            "/tmp/worktree",
            "on-request",
            "workspace-write",
            Some("gpt-test"),
            Some("medium"),
            None,
            "BASE",
            developer,
            false,
            &[],
            CodexNativeTools::None,
        );

        assert_eq!(params["baseInstructions"], "BASE");
        assert!(params.get("developerInstructions").is_none());
    }

    fn key_names(params: &Value) -> Vec<&str> {
        let mut keys: Vec<&str> = params
            .as_object()
            .expect("params are an object")
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        keys
    }

    /// The exact keys `thread/start` sends, checked against Codex 0.163.0's
    /// `ThreadStartParams`: it has no reasoning-effort field, so the effort
    /// rides in the `config` override map as `model_reasoning_effort`.
    #[test]
    fn thread_start_sends_only_protocol_keys_with_effort_in_config() {
        let params = build_thread_start_params(
            "/tmp/worktree",
            "on-request",
            "workspace-write",
            Some("gpt-test"),
            Some("high"),
            Some("priority"),
            "BASE",
            Some("AGENT"),
            false,
            &[],
            CodexNativeTools::None,
        );
        assert_eq!(
            key_names(&params),
            [
                "approvalPolicy",
                "baseInstructions",
                "config",
                "cwd",
                "developerInstructions",
                "model",
                "sandbox",
                "serviceName",
                "serviceTier",
            ]
        );
        assert_eq!(
            params["config"],
            serde_json::json!({ "model_reasoning_effort": "high" })
        );
    }

    /// `thread/resume` has neither a reasoning-effort field nor `serviceName`
    /// in Codex 0.163.0's `ThreadResumeParams`.
    #[test]
    fn thread_resume_sends_only_protocol_keys_with_effort_in_config() {
        let params = build_thread_resume_params(
            "thread-1",
            "/tmp/worktree",
            "on-request",
            "workspace-write",
            Some("gpt-test"),
            Some("high"),
            Some("priority"),
            "BASE",
            Some("AGENT"),
        );
        assert_eq!(
            key_names(&params),
            [
                "approvalPolicy",
                "baseInstructions",
                "config",
                "cwd",
                "developerInstructions",
                "model",
                "sandbox",
                "serviceTier",
                "threadId",
            ]
        );
        assert_eq!(
            params["config"],
            serde_json::json!({ "model_reasoning_effort": "high" })
        );
    }

    /// `thread/fork` is resume's keys plus `deferGoalContinuation`, which
    /// 0.163.0 declares only in its experimental schema.
    #[test]
    fn thread_fork_sends_only_protocol_keys_with_effort_in_config() {
        let params = build_thread_fork_params(
            "thread-1",
            "/tmp/worktree",
            "on-request",
            "workspace-write",
            Some("gpt-test"),
            Some("high"),
            Some("priority"),
            "BASE",
            Some("AGENT"),
        );
        assert_eq!(
            key_names(&params),
            [
                "approvalPolicy",
                "baseInstructions",
                "config",
                "cwd",
                "deferGoalContinuation",
                "developerInstructions",
                "model",
                "sandbox",
                "serviceTier",
                "threadId",
            ]
        );
        assert_eq!(
            params["config"],
            serde_json::json!({ "model_reasoning_effort": "high" })
        );
    }

    /// With no configured effort nothing is overridden: the thread keeps
    /// the model's catalog default, and a resume sends no `config` at all.
    #[test]
    fn no_configured_effort_sends_no_effort_override() {
        let resumed = build_thread_resume_params(
            "thread-1",
            "/tmp/worktree",
            "on-request",
            "workspace-write",
            None,
            None,
            None,
            "BASE",
            None,
        );
        assert!(resumed.get("config").is_none());
        let started = build_thread_start_params(
            "/tmp/worktree",
            "on-request",
            "workspace-write",
            None,
            None,
            None,
            "BASE",
            None,
            false,
            &[],
            CodexNativeTools::None,
        );
        assert!(started.get("config").is_none());
    }

    #[test]
    fn codex_strict_output_schema_normalizes_deep_research_scope_schema() {
        let schema = serde_json::json!({
            "type": "object",
            "required": ["question", "summary", "angles"],
            "properties": {
                "question": { "type": "string" },
                "summary": { "type": "string" },
                "angles": {
                    "type": "array",
                    "minItems": 3,
                    "maxItems": 6,
                    "items": {
                        "type": "object",
                        "required": ["label", "query"],
                        "properties": {
                            "label": { "type": "string" },
                            "query": { "type": "string" },
                            "rationale": { "type": "string" }
                        }
                    }
                }
            }
        });

        let normalized = codex_strict_output_schema(&schema);

        assert_eq!(
            normalized["additionalProperties"],
            serde_json::Value::Bool(false)
        );
        assert_required_keys(&normalized, &["question", "summary", "angles"]);

        let angle_schema = &normalized["properties"]["angles"]["items"];
        assert_eq!(
            angle_schema["additionalProperties"],
            serde_json::Value::Bool(false)
        );
        assert_required_keys(angle_schema, &["label", "query", "rationale"]);
        assert_eq!(
            angle_schema["properties"]["rationale"]["type"],
            serde_json::json!(["string", "null"])
        );
        assert_eq!(
            angle_schema["properties"]["label"],
            serde_json::json!({ "type": "string" })
        );
    }

    fn assert_required_keys(schema: &serde_json::Value, expected: &[&str]) {
        let mut actual = schema["required"]
            .as_array()
            .expect("schema required must be an array")
            .iter()
            .map(|value| value.as_str().expect("required item must be a string"))
            .collect::<Vec<_>>();
        actual.sort_unstable();

        let mut expected = expected.to_vec();
        expected.sort_unstable();

        assert_eq!(actual, expected);
    }
}
