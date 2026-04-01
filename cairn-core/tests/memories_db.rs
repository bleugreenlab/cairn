//! Integration tests for cairn_core::memories::db.
//!
//! Tests memory CRUD operations, trigger management, and surfacing tracking
//! against a real in-memory SQLite database with migrations applied.

mod common;

use cairn_core::memories::db;

#[test]
fn create_and_load_memory() {
    let mut conn = common::test_conn();

    let triggers = vec![(0, "$.tool_name", "Write")];
    let memory = db::create_memory(
        &mut conn,
        "memory-1",
        "Always run tests before pushing",
        None,
        "tentative",
        None,
        &triggers,
        "project",
        None,
        None,
    )
    .unwrap();

    assert_eq!(memory.id, "memory-1");
    assert_eq!(memory.content, "Always run tests before pushing");
    assert!(memory.project_id.is_none());
    assert_eq!(
        memory.confidence,
        cairn_core::models::MemoryConfidence::Tentative
    );
    assert!(memory.source_issue.is_none());
    assert!(memory.active);
    assert_eq!(memory.surfaced_count, 0);
    assert!(memory.last_surfaced_at.is_none());
    assert_eq!(memory.scope, "project");
    assert!(memory.keywords.is_empty());

    // Reload and verify
    let loaded = db::load_memory(&mut conn, "memory-1").unwrap();
    assert_eq!(loaded.content, memory.content);
    assert_eq!(loaded.triggers.len(), 1);
}

#[test]
fn create_memory_with_triggers() {
    let mut conn = common::test_conn();

    let triggers = vec![
        (0, "$.tool_name", "Write|Edit"),
        (0, "$.tool_input.file_path", "schema\\.rs$"),
        (1, "$.tool_name", "Read"),
    ];
    let memory = db::create_memory(
        &mut conn,
        "memory-t",
        "Be careful with schema changes",
        None,
        "established",
        Some("CAIRN-42"),
        &triggers,
        "project",
        None,
        None,
    )
    .unwrap();

    assert_eq!(memory.triggers.len(), 3);
    assert_eq!(memory.source_issue, Some("CAIRN-42".to_string()));
    assert_eq!(
        memory.confidence,
        cairn_core::models::MemoryConfidence::Established
    );

    // Verify trigger fields
    let t0: Vec<_> = memory
        .triggers
        .iter()
        .filter(|t| t.trigger_index == 0)
        .collect();
    assert_eq!(t0.len(), 2);
    let t1: Vec<_> = memory
        .triggers
        .iter()
        .filter(|t| t.trigger_index == 1)
        .collect();
    assert_eq!(t1.len(), 1);
    assert_eq!(t1[0].json_path, "$.tool_name");
    assert_eq!(t1[0].pattern, "Read");
}

#[test]
fn load_active_filters_inactive() {
    let mut conn = common::test_conn();

    db::create_memory(
        &mut conn,
        "active-1",
        "Active memory",
        None,
        "tentative",
        None,
        &[],
        "project",
        None,
        None,
    )
    .unwrap();
    db::create_memory(
        &mut conn,
        "inactive-1",
        "Inactive memory",
        None,
        "tentative",
        None,
        &[],
        "project",
        None,
        None,
    )
    .unwrap();
    db::update_memory(&mut conn, "inactive-1", None, None, Some(false), None, None).unwrap();

    let active = db::load_active_memories(&mut conn, None).unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].id, "active-1");
}

#[test]
fn load_active_project_scoping() {
    let mut conn = common::test_conn();
    let project_id = common::create_test_project(&mut conn, "Test Project", "TEST");

    // Global memory
    db::create_memory(
        &mut conn,
        "global-1",
        "Global memory",
        None,
        "tentative",
        None,
        &[],
        "project",
        None,
        None,
    )
    .unwrap();

    // Project-scoped memory
    db::create_memory(
        &mut conn,
        "proj-1",
        "Project memory",
        Some(&project_id),
        "tentative",
        None,
        &[],
        "project",
        None,
        None,
    )
    .unwrap();

    // Another project's memory
    let other_id = common::create_test_project(&mut conn, "Other Project", "OTHER");
    db::create_memory(
        &mut conn,
        "other-1",
        "Other project memory",
        Some(&other_id),
        "tentative",
        None,
        &[],
        "project",
        None,
        None,
    )
    .unwrap();

    // Loading for project should return global + project memories (not other project's)
    let memories = db::load_active_memories(&mut conn, Some(&project_id)).unwrap();
    assert_eq!(memories.len(), 2);
    let ids: Vec<&str> = memories.iter().map(|l| l.id.as_str()).collect();
    assert!(ids.contains(&"global-1"));
    assert!(ids.contains(&"proj-1"));
    assert!(!ids.contains(&"other-1"));

    // Loading with no project should return only globals
    let globals = db::load_active_memories(&mut conn, None).unwrap();
    assert_eq!(globals.len(), 1);
    assert_eq!(globals[0].id, "global-1");
}

#[test]
fn load_all_includes_inactive() {
    let mut conn = common::test_conn();

    db::create_memory(
        &mut conn,
        "a-1",
        "Active",
        None,
        "tentative",
        None,
        &[],
        "project",
        None,
        None,
    )
    .unwrap();
    db::create_memory(
        &mut conn,
        "i-1",
        "Inactive",
        None,
        "tentative",
        None,
        &[],
        "project",
        None,
        None,
    )
    .unwrap();
    db::update_memory(&mut conn, "i-1", None, None, Some(false), None, None).unwrap();

    let all = db::load_all_memories(&mut conn, None).unwrap();
    assert_eq!(all.len(), 2);

    let ids: Vec<&str> = all.iter().map(|l| l.id.as_str()).collect();
    assert!(ids.contains(&"a-1"));
    assert!(ids.contains(&"i-1"));
}

#[test]
fn load_all_ordered_by_created_at() {
    let mut conn = common::test_conn();

    db::create_memory(
        &mut conn,
        "old",
        "Old memory",
        None,
        "tentative",
        None,
        &[],
        "project",
        None,
        None,
    )
    .unwrap();

    db::create_memory(
        &mut conn,
        "new",
        "New memory",
        None,
        "tentative",
        None,
        &[],
        "project",
        None,
        None,
    )
    .unwrap();
    diesel::sql_query("UPDATE memories SET created_at = created_at + 10 WHERE id = 'new'")
        .execute(&mut conn)
        .unwrap();

    let all = db::load_all_memories(&mut conn, None).unwrap();
    assert_eq!(all.len(), 2);
    // Newest first
    assert_eq!(all[0].id, "new");
    assert_eq!(all[1].id, "old");
}

#[test]
fn update_content() {
    let mut conn = common::test_conn();

    let original = db::create_memory(
        &mut conn,
        "upd-1",
        "Original content",
        None,
        "tentative",
        None,
        &[],
        "project",
        None,
        None,
    )
    .unwrap();

    // Bump time so updated_at differs
    diesel::sql_query("UPDATE memories SET created_at = created_at - 10 WHERE id = 'upd-1'")
        .execute(&mut conn)
        .unwrap();

    let updated = db::update_memory(
        &mut conn,
        "upd-1",
        Some("Updated content"),
        None,
        None,
        None,
        None,
    )
    .unwrap();

    assert_eq!(updated.content, "Updated content");
    assert!(updated.updated_at >= original.updated_at);
}

#[test]
fn update_confidence() {
    let mut conn = common::test_conn();

    db::create_memory(
        &mut conn,
        "conf-1",
        "A memory",
        None,
        "tentative",
        None,
        &[],
        "project",
        None,
        None,
    )
    .unwrap();

    let updated = db::update_memory(
        &mut conn,
        "conf-1",
        None,
        Some("established"),
        None,
        None,
        None,
    )
    .unwrap();

    assert_eq!(
        updated.confidence,
        cairn_core::models::MemoryConfidence::Established
    );
}

#[test]
fn update_deactivate() {
    let mut conn = common::test_conn();

    db::create_memory(
        &mut conn,
        "deact-1",
        "Will be deactivated",
        None,
        "tentative",
        None,
        &[],
        "project",
        None,
        None,
    )
    .unwrap();

    let updated =
        db::update_memory(&mut conn, "deact-1", None, None, Some(false), None, None).unwrap();
    assert!(!updated.active);

    // Should no longer appear in active memories
    let active = db::load_active_memories(&mut conn, None).unwrap();
    assert!(active.is_empty());
}

#[test]
fn update_no_fields_errors() {
    let mut conn = common::test_conn();

    db::create_memory(
        &mut conn,
        "noop-1",
        "No-op memory",
        None,
        "tentative",
        None,
        &[],
        "project",
        None,
        None,
    )
    .unwrap();

    let result = db::update_memory(&mut conn, "noop-1", None, None, None, None, None);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("No fields to update"));
}

#[test]
fn delete_cascades_triggers() {
    let mut conn = common::test_conn();

    let triggers = vec![(0, "$.tool_name", "Write"), (1, "$.error", "locked")];
    db::create_memory(
        &mut conn,
        "del-1",
        "Will be deleted",
        None,
        "tentative",
        None,
        &triggers,
        "project",
        None,
        None,
    )
    .unwrap();

    // Verify triggers exist
    let memory = db::load_memory(&mut conn, "del-1").unwrap();
    assert_eq!(memory.triggers.len(), 2);

    // Delete
    db::delete_memory(&mut conn, "del-1").unwrap();

    // Memory should be gone
    assert!(db::load_memory(&mut conn, "del-1").is_err());

    // Triggers should be gone too (cascade delete via FK)
    let trigger_count: i64 = diesel::sql_query(
        "SELECT COUNT(*) as count FROM memory_triggers WHERE memory_id = 'del-1'",
    )
    .get_result::<CountRow>(&mut conn)
    .map(|r| r.count)
    .unwrap_or(0);
    assert_eq!(trigger_count, 0);
}

// Helper for raw SQL count queries
#[derive(diesel::QueryableByName)]
struct CountRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    count: i64,
}

#[test]
fn record_surfacing() {
    let mut conn = common::test_conn();

    db::create_memory(
        &mut conn,
        "surf-1",
        "Surfaceable",
        None,
        "tentative",
        None,
        &[],
        "project",
        None,
        None,
    )
    .unwrap();

    db::record_surfacing(&mut conn, &["surf-1"]).unwrap();

    let memory = db::load_memory(&mut conn, "surf-1").unwrap();
    assert_eq!(memory.surfaced_count, 1);
    assert!(memory.last_surfaced_at.is_some());

    // Surface again
    db::record_surfacing(&mut conn, &["surf-1"]).unwrap();
    let memory = db::load_memory(&mut conn, "surf-1").unwrap();
    assert_eq!(memory.surfaced_count, 2);
}

#[test]
fn record_surfacing_empty_noop() {
    let mut conn = common::test_conn();

    // Should not error with empty slice
    let result = db::record_surfacing(&mut conn, &[]);
    assert!(result.is_ok());
}

#[test]
fn replace_triggers() {
    let mut conn = common::test_conn();

    let original_triggers = vec![(0, "$.tool_name", "Write")];
    db::create_memory(
        &mut conn,
        "repl-1",
        "Replaceable triggers",
        None,
        "tentative",
        None,
        &original_triggers,
        "project",
        None,
        None,
    )
    .unwrap();

    // Replace with new triggers
    let new_triggers = vec![
        (0, "$.tool_name", "Edit"),
        (0, "$.tool_input.file_path", "main\\.rs$"),
    ];
    db::replace_triggers(&mut conn, "repl-1", &new_triggers).unwrap();

    let memory = db::load_memory(&mut conn, "repl-1").unwrap();
    assert_eq!(memory.triggers.len(), 2);

    let patterns: Vec<&str> = memory.triggers.iter().map(|t| t.pattern.as_str()).collect();
    assert!(patterns.contains(&"Edit"));
    assert!(patterns.contains(&"main\\.rs$"));
    assert!(!patterns.contains(&"Write"));
}

#[test]
fn create_with_scope_and_keywords() {
    let mut conn = common::test_conn();

    let memory = db::create_memory(
        &mut conn,
        "sk-1",
        "Branch-scoped with keywords",
        None,
        "tentative",
        None,
        &[],
        "branch:feature/dom",
        Some(r#"["DOM","CSP"]"#),
        None,
    )
    .unwrap();

    assert_eq!(memory.scope, "branch:feature/dom");
    assert_eq!(memory.keywords, vec!["DOM", "CSP"]);

    // Reload and verify
    let loaded = db::load_memory(&mut conn, "sk-1").unwrap();
    assert_eq!(loaded.scope, "branch:feature/dom");
    assert_eq!(loaded.keywords, vec!["DOM", "CSP"]);
}

#[test]
fn update_scope_and_keywords() {
    let mut conn = common::test_conn();

    db::create_memory(
        &mut conn,
        "sk-upd",
        "Will update scope",
        None,
        "tentative",
        None,
        &[],
        "project",
        None,
        None,
    )
    .unwrap();

    let updated = db::update_memory(
        &mut conn,
        "sk-upd",
        None,
        None,
        None,
        Some("branch:main"),
        Some(Some(r#"["test","CI"]"#)),
    )
    .unwrap();

    assert_eq!(updated.scope, "branch:main");
    assert_eq!(updated.keywords, vec!["test", "CI"]);
}

#[test]
fn update_keywords_to_null_clears_them() {
    let mut conn = common::test_conn();

    db::create_memory(
        &mut conn,
        "kw-clear",
        "Has keywords",
        None,
        "tentative",
        None,
        &[],
        "project",
        Some(r#"["test","CI"]"#),
        None,
    )
    .unwrap();

    // Clear keywords by setting to null
    let updated = db::update_memory(
        &mut conn,
        "kw-clear",
        None,
        None,
        None,
        None,
        Some(None), // keywords = Some(None) → set column to NULL
    )
    .unwrap();

    assert!(updated.keywords.is_empty());

    // Reload to verify persistence
    let loaded = db::load_memory(&mut conn, "kw-clear").unwrap();
    assert!(loaded.keywords.is_empty());
}

#[test]
fn malformed_keywords_json_defaults_to_empty() {
    let mut conn = common::test_conn();

    // Insert with malformed JSON directly
    db::create_memory(
        &mut conn,
        "bad-json",
        "Bad keywords",
        None,
        "tentative",
        None,
        &[],
        "project",
        Some("not valid json"),
        None,
    )
    .unwrap();

    let loaded = db::load_memory(&mut conn, "bad-json").unwrap();
    // to_memory uses serde_json::from_str(...).ok().unwrap_or_default()
    assert!(loaded.keywords.is_empty());
}

#[test]
fn create_memory_with_source_run_id() {
    let mut conn = common::test_conn();

    let memory = db::create_memory(
        &mut conn,
        "run-src-1",
        "Memory with provenance",
        None,
        "tentative",
        None,
        &[],
        "project",
        None,
        Some("run-abc-12345678"),
    )
    .unwrap();

    assert_eq!(memory.source_run_id, Some("run-abc-12345678".to_string()));

    // Reload and verify persistence
    let loaded = db::load_memory(&mut conn, "run-src-1").unwrap();
    assert_eq!(loaded.source_run_id, Some("run-abc-12345678".to_string()));
}

#[test]
fn create_memory_without_source_run_id() {
    let mut conn = common::test_conn();

    let memory = db::create_memory(
        &mut conn,
        "no-run-1",
        "Memory without provenance",
        None,
        "tentative",
        None,
        &[],
        "project",
        None,
        None,
    )
    .unwrap();

    assert!(memory.source_run_id.is_none());

    let loaded = db::load_memory(&mut conn, "no-run-1").unwrap();
    assert!(loaded.source_run_id.is_none());
}

use diesel::RunQueryDsl;
