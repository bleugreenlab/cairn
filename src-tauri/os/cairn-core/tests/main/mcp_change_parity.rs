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
//!
//! Every mismatch is collected and reported together: one drift must not mask
//! the next behind its panic.

use crate::common;

use cairn_common::contract::{
    mutation_spec, ChangeMode, KeyType, MutationSpec, ResourceKind, RESOURCE_CONTRACTS,
};
use cairn_common::uri::samples::every_resource_variant;
use cairn_core::internal::mcp::handlers::write::handle_write;
use cairn_core::internal::mcp::types::McpCallbackRequest;
use serde_json::json;

/// Kinds whose dispatcher deliberately refuses every unsupported mode ahead of
/// the contract gate, with a more instructive message than the gate's. Each
/// entry names the exact refusal, so the exception covers only that refusal
/// and an unsupported mode reaching a real arm still fails the test.
fn pre_gate_refusal(kind: ResourceKind) -> Option<&'static str> {
    match kind {
        ResourceKind::ProjectTenet => Some(cairn_core::tenets::IMMUTABLE_REFUSAL),
        _ => None,
    }
}

/// Minimal payload satisfying a mutation's required keys (canonical names).
fn payload_for(spec: &MutationSpec) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for key in spec.required {
        let value = match key.ty {
            KeyType::Str => json!("x"),
            KeyType::Bool => json!(true),
            KeyType::Int => json!(1),
            KeyType::Float => json!(1.0),
            KeyType::Array => json!([]),
            KeyType::Object => json!({}),
            KeyType::Any => serde_json::Value::Null,
        };
        map.insert(key.key.to_string(), value);
    }
    serde_json::Value::Object(map)
}

#[tokio::test]
async fn table_and_dispatch_match_for_every_kind_and_mode() {
    let (_temp, orch) = common::test_orchestrator().await;
    let cwd = std::env::temp_dir().to_string_lossy().to_string();
    let samples = every_resource_variant();
    let mut mismatches: Vec<String> = Vec::new();

    for contract in RESOURCE_CONTRACTS {
        // The canonical sample registry owns what a URI of each kind looks like
        // and pins that it reads back as that kind, thread sub-resources
        // included; this test owns only what the dispatcher does with it.
        let Some(sample) = samples
            .iter()
            .find(|sample| sample.resource.kind() == contract.kind)
        else {
            mismatches.push(format!("no canonical sample for {:?}", contract.kind));
            continue;
        };
        let uri = sample.resource.to_uri();

        for &mode in ChangeMode::ALL {
            // Apply is a control mode for the preview->apply round trip, and
            // revert is a commit-wide bare-file operation. Neither is a
            // per-resource mutation, so both are validated before dispatch.
            if matches!(mode, ChangeMode::Apply | ChangeMode::Revert) {
                continue;
            }
            let spec = mutation_spec(contract.kind, mode);
            let mut item = json!({ "target": uri, "mode": mode.as_str() });
            if let Some(spec) = spec {
                item["payload"] = payload_for(spec);
            }
            let request = McpCallbackRequest {
                thread_id: None,
                cwd: cwd.clone(),
                run_id: None,
                tool: "write".to_string(),
                payload: json!({ "preview": true, "changes": [item] }),
                tool_use_id: None,
            };
            let result = handle_write(&orch, &request).await;
            let context = format!("{uri} mode={}", mode.as_str());

            if spec.is_some() {
                if result.contains("Unsupported resource mutation") {
                    mismatches.push(format!(
                        "supported mutation was gate-rejected: {context}: {result}"
                    ));
                }
                if result.contains("no dispatch arm handles it") {
                    mismatches.push(format!(
                        "table entry without a dispatch arm: {context}: {result}"
                    ));
                }
                if result.contains("This resource is read-only") {
                    mismatches.push(format!(
                        "supported mutation marked read-only: {context}: {result}"
                    ));
                }
            } else {
                let refused = result.contains("Unsupported resource mutation")
                    || pre_gate_refusal(contract.kind).is_some_and(|r| result.contains(r));
                if !refused {
                    mismatches.push(format!(
                        "unsupported mutation was not rejected: {context}: {result}"
                    ));
                }
            }
        }
    }

    assert!(
        mismatches.is_empty(),
        "{} contract/dispatch mismatches:\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}
