//! Every structured carrier that would reach the workspace configuration
//! document converges on the same answer.
//!
//! The residual CAIRN-3803 shipped was that the authority gate matched resource
//! *syntax*: `cairn://settings` and `cairn://mcp` were adjudicated, and a
//! `file:` write to the same bytes was not. Which URI an agent happened to use
//! decided whether workspace capability could change without approval. These
//! tests pin that the carrier no longer decides.

use std::sync::Arc;

use crate::common::orchestrator;
use cairn_core::internal::mcp::handlers::write::handle_write;
use cairn_core::internal::mcp::types::McpCallbackRequest;
use serde_json::json;

fn write_request(changes: serde_json::Value) -> McpCallbackRequest {
    McpCallbackRequest {
        thread_id: None,
        cwd: String::new(),
        run_id: None,
        tool: "write".to_string(),
        payload: json!({"changes": changes, "commit_msg": "edit settings"}),
        tool_use_id: None,
    }
}

/// Every file mode, aimed at the configuration document by absolute path.
#[tokio::test]
async fn no_file_mode_can_write_the_configuration_document() {
    let (temp, db) = crate::common::migrated_db().await;
    let orch = orchestrator(&temp, Arc::new(db));
    let settings = orch.config_dir.join("settings.yaml");
    std::fs::create_dir_all(&orch.config_dir).unwrap();
    std::fs::write(&settings, "logLevel: standard\n").unwrap();
    let target = format!("file:{}", settings.display());

    let carriers: Vec<(&str, serde_json::Value)> = vec![
        (
            "create",
            json!({"target": target, "mode": "create", "payload": {"content": "backends: {}\n"}}),
        ),
        (
            "replace",
            json!({"target": target, "mode": "replace", "payload": {"content": "backends: {}\n"}}),
        ),
        (
            "append",
            json!({"target": target, "mode": "append", "payload": {"content": "backends: {}\n"}}),
        ),
        (
            "patch",
            json!({"target": target, "mode": "patch", "payload": {
                "old_string": "logLevel: standard", "new_string": "logLevel: verbose"}}),
        ),
        ("delete", json!({"target": target, "mode": "delete"})),
    ];

    for (mode, change) in carriers {
        let result = handle_write(&orch, &write_request(json!([change]))).await;
        assert!(
            result.contains("cannot be written\ndirectly")
                || result.contains("cannot be written directly"),
            "mode {mode} was not refused; got: {result}"
        );
        assert_eq!(
            std::fs::read_to_string(&settings).unwrap(),
            "logLevel: standard\n",
            "mode {mode} changed the document"
        );
    }
}

/// A unified-patch envelope carries its own paths and is addressed at the bare
/// worktree root, so a check that only looked at the item's target would miss
/// it entirely.
#[tokio::test]
async fn a_unified_patch_envelope_cannot_smuggle_the_document_in() {
    let (temp, db) = crate::common::migrated_db().await;
    let orch = orchestrator(&temp, Arc::new(db));
    std::fs::create_dir_all(&orch.config_dir).unwrap();
    let settings = orch.config_dir.join("settings.yaml");
    std::fs::write(&settings, "logLevel: standard\n").unwrap();

    let envelope = format!(
        "*** Begin Patch\n*** Update File: {}\n@@ -1,1 +1,1 @@\n-logLevel: standard\n+logLevel: verbose\n*** End Patch\n",
        settings.display()
    );
    let result = handle_write(
        &orch,
        &write_request(json!([{
            "target": "file:",
            "mode": "unified_patch",
            "payload": {"patch": envelope}
        }])),
    )
    .await;

    assert!(
        result.contains("brokered") || result.contains("cairn://settings"),
        "envelope was not refused; got: {result}"
    );
    assert_eq!(
        std::fs::read_to_string(&settings).unwrap(),
        "logLevel: standard\n"
    );
}

/// A batch is refused whole. Preparation runs over every item before any of
/// them applies, so a protected write cannot ride in behind an innocuous one
/// that has already landed.
#[tokio::test]
async fn a_mixed_batch_is_refused_before_anything_applies() {
    let (temp, db) = crate::common::migrated_db().await;
    let orch = orchestrator(&temp, Arc::new(db));
    std::fs::create_dir_all(&orch.config_dir).unwrap();
    let settings = orch.config_dir.join("settings.yaml");
    std::fs::write(&settings, "logLevel: standard\n").unwrap();
    let innocuous = temp.path().join("notes.md");

    let result = handle_write(
        &orch,
        &write_request(json!([
            {"target": format!("file:{}", innocuous.display()), "mode": "create",
             "payload": {"content": "hello\n"}},
            {"target": format!("file:{}", settings.display()), "mode": "append",
             "payload": {"content": "backends: {}\n"}}
        ])),
    )
    .await;

    assert!(
        result.contains("brokered") || result.contains("cairn://settings"),
        "the batch was not refused; got: {result}"
    );
    assert_eq!(
        std::fs::read_to_string(&settings).unwrap(),
        "logLevel: standard\n"
    );
    assert!(
        !innocuous.exists(),
        "the sibling item applied before the batch was refused, so a protected write could \
         ride in behind a partially-applied batch"
    );
}
