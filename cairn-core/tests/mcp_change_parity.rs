//! Bidirectional parity between the ResourceContract table and the real change
//! dispatcher. For every (kind, mode), routing through the live dispatcher (in
//! preview/dry-run mode) must agree with `mutation_spec(kind, mode)`:
//!
//! - table entry present  => the dispatcher routes to a real arm (no gate
//!   rejection, no "no dispatch arm" catch-all)
//! - table entry absent   => the gate rejects with "Unsupported resource mutation"
//!
//! A table entry with no dispatch arm trips the catch-all; an arm reachable only
//! without a table entry is dead because the gate blocks it. Either way this
//! test fails, so the table and the dispatch match cannot drift apart.

mod common;

use cairn_common::contract::{
    mutation_spec, ChangeMode, KeyType, MutationSpec, ResourceContract, RESOURCE_CONTRACTS,
};
use cairn_common::uri::parse_uri;
use cairn_core::internal::mcp::handlers::files::handle_change;
use cairn_core::internal::mcp::types::McpCallbackRequest;
use serde_json::json;

/// Build a representative URI for a contract straight from its template, so the
/// fixture stays in lockstep with the table (no separate URI list to maintain).
fn sample_uri(contract: &ResourceContract) -> String {
    contract
        .uri_template
        .replace("{anchor}", "cairn://p/MCP/1/1/builder/plan")
        .replace("{id}", "ann-1")
        .replace("{seq}", "1")
        .replace("{exec_seq}", "1")
        .replace("{comment_seq}", "1")
        .replace("{memory_seq}", "1")
        .replace("{action_id}", "action-1")
        .replace("{agent_id}", "agent-1")
        .replace("{label_id}", "label-1")
        .replace("{recipe_id}", "recipe-1")
        .replace("{recipe}", "recipe-1")
        .replace("{server}", "axon")
        .replace("{task}", "task-1")
        .replace("{content}", "x")
        .replace("{project}", "MCP")
        .replace("{number}", "1")
        .replace("{exec}", "1")
        .replace("{node}", "builder")
        .replace("{slug}", "dev")
        .replace("{name}", "Explore")
        .replace("{turn}", "1")
        .replace("{run_seq}", "1")
        .replace("{event_seq}", "2")
        .replace("{skill_id}", "ui")
        .replace("{segment}", "q-1")
}

/// Minimal payload satisfying a mutation's required keys (canonical names).
fn payload_for(spec: &MutationSpec) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for key in spec.required {
        let value = match key.ty {
            KeyType::Str => json!("x"),
            KeyType::Bool => json!(true),
            KeyType::Int => json!(1),
            KeyType::Array => json!([]),
            KeyType::Object => json!({}),
        };
        map.insert(key.key.to_string(), value);
    }
    serde_json::Value::Object(map)
}

#[tokio::test]
async fn table_and_dispatch_match_for_every_kind_and_mode() {
    let (_temp, orch) = common::test_orchestrator().await;
    let cwd = std::env::temp_dir().to_string_lossy().to_string();

    for contract in RESOURCE_CONTRACTS {
        let uri = sample_uri(contract);
        // Sanity: the template-derived URI parses back to this kind.
        let parsed = parse_uri(&uri).unwrap_or_else(|| panic!("sample URI did not parse: {uri}"));
        assert_eq!(
            parsed.kind(),
            contract.kind,
            "sample URI {uri} resolved to the wrong kind"
        );

        for &mode in ChangeMode::ALL {
            // Apply is a control mode for the preview->apply round trip, not a
            // per-resource mutation; preview rejects it before dispatch.
            // Annotate is a universal facet handled by a catch-all dispatcher arm
            // rather than a per-kind mutation table row, so it has no owning
            // ResourceKind to compare in this per-resource parity matrix.
            if matches!(mode, ChangeMode::Apply) {
                continue;
            }
            let spec = mutation_spec(contract.kind, mode);
            let mut item = json!({ "target": uri, "mode": mode.as_str() });
            if let Some(spec) = spec {
                item["payload"] = payload_for(spec);
            }
            let request = McpCallbackRequest {
                cwd: cwd.clone(),
                run_id: None,
                tool: "write".to_string(),
                payload: json!({ "preview": true, "changes": [item] }),
                tool_use_id: None,
            };
            let result = handle_change(&orch, &request).await;
            let context = format!("{uri} mode={}", mode.as_str());

            if spec.is_some() {
                assert!(
                    !result.contains("Unsupported resource mutation"),
                    "supported mutation was gate-rejected: {context}: {result}"
                );
                assert!(
                    !result.contains("no dispatch arm handles it"),
                    "table entry without a dispatch arm: {context}: {result}"
                );
                assert!(
                    !result.contains("This resource is read-only"),
                    "supported mutation marked read-only: {context}: {result}"
                );
            } else {
                assert!(
                    result.contains("Unsupported resource mutation"),
                    "unsupported mutation was not rejected: {context}: {result}"
                );
            }
        }
    }
}
