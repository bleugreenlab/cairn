//! End-to-end integration tests combining memory DB operations with the matching engine.
//!
//! Verifies the full lifecycle: create memories in DB → load → match against hook input.

mod common;

use cairn_core::memories::{db, matching};
use cairn_core::models::MemoryConfidence;
use serde_json::json;

#[test]
fn create_load_and_match() {
    let mut conn = common::test_conn();

    // Create a memory with triggers that match Write to schema.rs
    let triggers = vec![
        (0, "$.tool_name", "Write|Edit"),
        (0, "$.tool_input.file_path", "schema\\.rs$"),
    ];
    db::create_memory(
        &mut conn,
        "match-1",
        "Run diesel migration after schema changes",
        None,
        "established",
        None,
        &triggers,
    )
    .unwrap();

    // Load active memories
    let memories = db::load_active_memories(&mut conn, None).unwrap();
    assert_eq!(memories.len(), 1);

    // Match against hook input
    let hook_input = json!({
        "tool_name": "Write",
        "tool_input": {"file_path": "/src/schema.rs"}
    });
    let matches = matching::match_memories(&hook_input, &memories);
    assert_eq!(matches, vec![0]);

    // Non-matching input
    let hook_input = json!({
        "tool_name": "Read",
        "tool_input": {"file_path": "/src/main.rs"}
    });
    let matches = matching::match_memories(&hook_input, &memories);
    assert!(matches.is_empty());
}

#[test]
fn inactive_memory_not_matched() {
    let mut conn = common::test_conn();

    let triggers = vec![(0, "$.tool_name", "Write")];
    db::create_memory(
        &mut conn,
        "gone-1",
        "This will be deactivated",
        None,
        "tentative",
        None,
        &triggers,
    )
    .unwrap();

    // Deactivate
    db::update_memory(&mut conn, "gone-1", None, None, Some(false)).unwrap();

    // load_active should return empty
    let memories = db::load_active_memories(&mut conn, None).unwrap();
    assert!(memories.is_empty());

    // Even if we loaded all and tried matching, inactive memories just aren't returned by load_active
    let all = db::load_all_memories(&mut conn, None).unwrap();
    assert_eq!(all.len(), 1);
    assert!(!all[0].active);
}

#[test]
fn full_lifecycle() {
    let mut conn = common::test_conn();

    // 1. Create
    let triggers = vec![(0, "$.tool_name", "Write")];
    db::create_memory(
        &mut conn,
        "life-1",
        "Original content",
        None,
        "tentative",
        None,
        &triggers,
    )
    .unwrap();

    // 2. Match
    let memories = db::load_active_memories(&mut conn, None).unwrap();
    let hook_input = json!({"tool_name": "Write"});
    let matches = matching::match_memories(&hook_input, &memories);
    assert_eq!(matches, vec![0]);

    // 3. Record surfacing
    db::record_surfacing(&mut conn, &["life-1"]).unwrap();
    let memory = db::load_memory(&mut conn, "life-1").unwrap();
    assert_eq!(memory.surfaced_count, 1);

    // 4. Update confidence
    let memory = db::update_memory(&mut conn, "life-1", None, Some("established"), None).unwrap();
    assert_eq!(memory.confidence, MemoryConfidence::Established);

    // 5. Replace triggers (now match Edit instead of Write)
    let new_triggers = vec![(0, "$.tool_name", "Edit")];
    db::replace_triggers(&mut conn, "life-1", &new_triggers).unwrap();

    // 6. Verify new triggers match
    let memories = db::load_active_memories(&mut conn, None).unwrap();

    let hook_write = json!({"tool_name": "Write"});
    assert!(matching::match_memories(&hook_write, &memories).is_empty());

    let hook_edit = json!({"tool_name": "Edit"});
    assert_eq!(matching::match_memories(&hook_edit, &memories), vec![0]);
}
