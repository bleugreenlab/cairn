use super::Migration;

pub const TURSO_MIGRATIONS: &[Migration] = &[
    Migration::new(
        "0001",
        "initial_schema",
        include_str!("../../../../turso_migrations/0001_initial_schema.sql"),
    ),
    Migration::new(
        "0002",
        "search_outbox",
        include_str!("../../../../turso_migrations/0002_search_outbox.sql"),
    ),
    Migration::new(
        "0003",
        "seed_default_workspace",
        include_str!("../../../../turso_migrations/0003_seed_default_workspace.sql"),
    ),
    Migration::new(
        "0004",
        "add_issue_dependencies",
        include_str!("../../../../turso_migrations/0004_add_issue_dependencies.sql"),
    ),
    Migration::new(
        "0005",
        "change_preview_events",
        include_str!("../../../../turso_migrations/0005_change_preview_events.sql"),
    ),
    Migration::new(
        "0006",
        "uri_segments",
        include_str!("../../../../turso_migrations/0006_uri_segments.sql"),
    ),
    Migration::new(
        "0007",
        "add_uri_segment_to_prompts",
        include_str!("../../../../turso_migrations/0007_add_uri_segment_to_prompts.sql"),
    ),
    Migration::new(
        "0008",
        "add_job_id_to_prompts",
        include_str!("../../../../turso_migrations/0008_add_job_id_to_prompts.sql"),
    ),
    Migration::new(
        "0009",
        "cohere_embeddings",
        include_str!("../../../../turso_migrations/0009_cohere_embeddings.sql"),
    ),
    Migration::new(
        "0010",
        "anon_device",
        include_str!("../../../../turso_migrations/0010_anon_device.sql"),
    ),
    Migration::new(
        "0011",
        "session_current_pos",
        include_str!("../../../../turso_migrations/0011_session_current_pos.sql"),
    ),
    Migration::new(
        "0012",
        "resource_surfacings",
        include_str!("../../../../turso_migrations/0012_resource_surfacings.sql"),
    ),
    Migration::new(
        "0013",
        "memory_redux",
        include_str!("../../../../turso_migrations/0013_memory_redux.sql"),
    ),
    Migration::new(
        "0014",
        "add_tool_use_id_to_prompts",
        include_str!("../../../../turso_migrations/0014_add_tool_use_id_to_prompts.sql"),
    ),
    Migration::new(
        "0015",
        "add_artifact_confirmed",
        include_str!("../../../../turso_migrations/0015_add_artifact_confirmed.sql"),
    ),
    Migration::new(
        "0016",
        "remove_ready_status",
        include_str!("../../../../turso_migrations/0016_remove_ready_status.sql"),
    ),
    Migration::new(
        "0017",
        "messages_delivered_at",
        include_str!("../../../../turso_migrations/0017_messages_delivered_at.sql"),
    ),
    Migration::new(
        "0018",
        "pr_node_port_fires",
        include_str!("../../../../turso_migrations/0018_pr_node_port_fires.sql"),
    ),
    Migration::new(
        "0019",
        "merge_request_owner",
        include_str!("../../../../turso_migrations/0019_merge_request_owner.sql"),
    ),
    Migration::new(
        "0020",
        "add_uri_segment_to_action_runs",
        include_str!("../../../../turso_migrations/0020_add_uri_segment_to_action_runs.sql"),
    ),
    Migration::new(
        "0021",
        "vibe_axes",
        include_str!("../../../../turso_migrations/0021_vibe_axes.sql"),
    ),
    Migration::new(
        "0022",
        "add_segments_to_permission_requests",
        include_str!("../../../../turso_migrations/0022_add_segments_to_permission_requests.sql"),
    ),
    Migration::new(
        "0023",
        "add_labels",
        include_str!("../../../../turso_migrations/0023_add_labels.sql"),
    ),
    Migration::new(
        "0024",
        "add_parent_issue",
        include_str!("../../../../turso_migrations/0024_add_parent_issue.sql"),
    ),
    Migration::rebuild_fk_off(
        "0025",
        "remove_managers",
        include_str!("../../../../turso_migrations/0025_remove_managers.sql"),
    ),
    Migration::new(
        "0026",
        "child_side_channel_notices",
        include_str!("../../../../turso_migrations/0026_child_side_channel_notices.sql"),
    ),
    Migration::new(
        "0027",
        "session_channel_cursor",
        include_str!("../../../../turso_migrations/0027_session_channel_cursor.sql"),
    ),
    Migration::new(
        "0028",
        "issue_parent_job",
        include_str!("../../../../turso_migrations/0028_issue_parent_job.sql"),
    ),
    Migration::new(
        "0029",
        "queued_messages",
        include_str!("../../../../turso_migrations/0029_queued_messages.sql"),
    ),
    Migration::new(
        "0030",
        "checkpoint_runs",
        include_str!("../../../../turso_migrations/0030_checkpoint_runs.sql"),
    ),
    Migration::rebuild_fk_off(
        "0031",
        "drop_dead_chats_table",
        include_str!("../../../../turso_migrations/0031_drop_dead_chats_table.sql"),
    ),
    Migration::new(
        "0032",
        "drop_workspaces_timezone_column",
        include_str!("../../../../turso_migrations/0032_drop_workspaces_timezone_column.sql"),
    ),
    Migration::new(
        "0033",
        "annotations",
        include_str!("../../../../turso_migrations/0033_annotations.sql"),
    ),
    Migration::new(
        "0034",
        "annotation_message_links",
        include_str!("../../../../turso_migrations/0034_annotation_message_links.sql"),
    ),
    Migration::new(
        "0035",
        "annotation_uri_seq",
        include_str!("../../../../turso_migrations/0035_annotation_uri_seq.sql"),
    ),
    Migration::new(
        "0036",
        "wake_subscriptions",
        include_str!("../../../../turso_migrations/0036_wake_subscriptions.sql"),
    ),
    Migration::new(
        "0037",
        "unify_side_channel_notices",
        include_str!("../../../../turso_migrations/0037_unify_side_channel_notices.sql"),
    ),
    Migration::rebuild_fk_off(
        "0038",
        "drop_annotation_tables",
        include_str!("../../../../turso_migrations/0038_drop_annotation_tables.sql"),
    ),
    Migration::new(
        "0039",
        "memory_intake_ledger",
        include_str!("../../../../turso_migrations/0039_memory_intake_ledger.sql"),
    ),
    Migration::new(
        "0040",
        "add_is_workspace_to_projects",
        include_str!("../../../../turso_migrations/0040_add_is_workspace_to_projects.sql"),
    ),
    Migration::new(
        "0041",
        "memory_triage_batches_and_drop_when_to_use",
        include_str!(
            "../../../../turso_migrations/0041_memory_triage_batches_and_drop_when_to_use.sql"
        ),
    ),
    Migration::rebuild_fk_off(
        "0042",
        "memory_scope_node_id_and_status_lattice",
        include_str!(
            "../../../../turso_migrations/0042_memory_scope_node_id_and_status_lattice.sql"
        ),
    ),
    Migration::new(
        "0043",
        "memory_triage_decision",
        include_str!("../../../../turso_migrations/0043_memory_triage_decision.sql"),
    ),
    Migration::new(
        "0044",
        "jobs_memory_review_state",
        include_str!("../../../../turso_migrations/0044_jobs_memory_review_state.sql"),
    ),
    Migration::rebuild_fk_off(
        "0045",
        "memory_canon_v2_consolidation",
        include_str!("../../../../turso_migrations/0045_memory_canon_v2_consolidation.sql"),
    ),
    Migration::rebuild_fk_off(
        "0046",
        "memory_review_sent_state",
        include_str!("../../../../turso_migrations/0046_memory_review_sent_state.sql"),
    ),
    Migration::new(
        "0047",
        "add_message_urgency",
        include_str!("../../../../turso_migrations/0047_add_message_urgency.sql"),
    ),
    Migration::new(
        "0048",
        "add_event_thinking_tokens",
        include_str!("../../../../turso_migrations/0048_add_event_thinking_tokens.sql"),
    ),
    Migration::new(
        "0049",
        "event_vibes_session_id",
        include_str!("../../../../turso_migrations/0049_event_vibes_session_id.sql"),
    ),
    Migration::new(
        "0050",
        "session_skyline_cache_vibe_watermark",
        include_str!("../../../../turso_migrations/0050_session_skyline_cache_vibe_watermark.sql"),
    ),
    Migration::new(
        "0051",
        "clear_skyline_cache_for_content_bar_decomposition",
        include_str!(
            "../../../../turso_migrations/0051_clear_skyline_cache_for_content_bar_decomposition.sql"
        ),
    ),
    Migration::new(
        "0052",
        "clear_skyline_cache_for_visual_height_sizing",
        include_str!(
            "../../../../turso_migrations/0052_clear_skyline_cache_for_visual_height_sizing.sql"
        ),
    ),
    Migration::new(
        "0053",
        "job_pack_anchor",
        include_str!("../../../../turso_migrations/0053_job_pack_anchor.sql"),
    ),
    Migration::new(
        "0054",
        "archival_storage",
        include_str!("../../../../turso_migrations/0054_archival_storage.sql"),
    ),
    Migration::new(
        "0055",
        "archival_backfill_state",
        include_str!("../../../../turso_migrations/0055_archival_backfill_state.sql"),
    ),
    Migration::new(
        "0056",
        "archival_blobs",
        include_str!("../../../../turso_migrations/0056_archival_blobs.sql"),
    ),
    Migration::new(
        "0057",
        "event_read_tokens",
        include_str!("../../../../turso_migrations/0057_event_read_tokens.sql"),
    ),
    Migration::new(
        "0058",
        "terminal_exit_wakes",
        include_str!("../../../../turso_migrations/0058_terminal_exit_wakes.sql"),
    ),
    Migration::new(
        "0059",
        "jobs_needs_fresh_session",
        include_str!("../../../../turso_migrations/0059_jobs_needs_fresh_session.sql"),
    ),
    Migration::new(
        "0060",
        "attention_items",
        include_str!("../../../../turso_migrations/0060_attention_items.sql"),
    ),
    Migration::new(
        "0061",
        "attention_escalate_at",
        include_str!("../../../../turso_migrations/0061_attention_escalate_at.sql"),
    ),
    Migration::new(
        "0062",
        "attention_fingerprint",
        include_str!("../../../../turso_migrations/0062_attention_fingerprint.sql"),
    ),
    Migration::new(
        "0063",
        "comment_seq",
        include_str!("../../../../turso_migrations/0063_comment_seq.sql"),
    ),
    Migration::new(
        "0064",
        "clear_skyline_cache_for_system_event_filter",
        include_str!(
            "../../../../turso_migrations/0064_clear_skyline_cache_for_system_event_filter.sql"
        ),
    ),
    Migration::new(
        "0065",
        "merge_request_is_local",
        include_str!("../../../../turso_migrations/0065_merge_request_is_local.sql"),
    ),
    Migration::new(
        "0066",
        "config_disables",
        include_str!("../../../../turso_migrations/0066_config_disables.sql"),
    ),
    Migration::new(
        "0067",
        "tool_invocations",
        include_str!("../../../../turso_migrations/0067_tool_invocations.sql"),
    ),
    Migration::new(
        "0068",
        "job_browsers",
        include_str!("../../../../turso_migrations/0068_job_browsers.sql"),
    ),
    Migration::new(
        "0069",
        "add_event_cost_usd",
        include_str!("../../../../turso_migrations/0069_add_event_cost_usd.sql"),
    ),
    Migration::new(
        "0070",
        "attention_pushes",
        include_str!("../../../../turso_migrations/0070_attention_pushes.sql"),
    ),
    Migration::new(
        "0071",
        "attention_push_fingerprint",
        include_str!("../../../../turso_migrations/0071_attention_push_fingerprint.sql"),
    ),
    Migration::new(
        "0072",
        "merge_request_head_sha",
        include_str!("../../../../turso_migrations/0072_merge_request_head_sha.sql"),
    ),
    Migration::new(
        "0073",
        "attention_read_cursors",
        include_str!("../../../../turso_migrations/0073_attention_read_cursors.sql"),
    ),
    Migration::new(
        "0074",
        "drop_attention_ledger",
        include_str!("../../../../turso_migrations/0074_drop_attention_ledger.sql"),
    ),
    Migration::new(
        "0075",
        "drop_messages_delivered_at",
        include_str!("../../../../turso_migrations/0075_drop_messages_delivered_at.sql"),
    ),
    Migration::new(
        "0076",
        "terminal_output_wakes",
        include_str!("../../../../turso_migrations/0076_terminal_output_wakes.sql"),
    ),
    Migration::new(
        "0077",
        "event_content_change_id",
        include_str!("../../../../turso_migrations/0077_event_content_change_id.sql"),
    ),
    Migration::new(
        "0078",
        "browser_last_active_at",
        include_str!("../../../../turso_migrations/0078_browser_last_active_at.sql"),
    ),
    Migration::new(
        "0079",
        "index_runs_session_id_created_at",
        include_str!("../../../../turso_migrations/0079_index_runs_session_id_created_at.sql"),
    ),
    Migration::new(
        "0080",
        "token_rollup",
        include_str!("../../../../turso_migrations/0080_token_rollup.sql"),
    ),
    Migration::new(
        "0081",
        "drop_runs_backend",
        include_str!("../../../../turso_migrations/0081_drop_runs_backend.sql"),
    ),
];

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::storage::{DbError, DbResult, LocalDb, MigrationRunner, RowExt};

    async fn migrated_db() -> DbResult<LocalDb> {
        let temp = tempdir()?;
        let path = temp.keep().join("cairn-real-turso-schema.db");
        let db = LocalDb::open(path).await?;
        let applied = MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await?;
        assert_eq!(
            applied,
            vec![
                "0001_initial_schema".to_string(),
                "0002_search_outbox".to_string(),
                "0003_seed_default_workspace".to_string(),
                "0004_add_issue_dependencies".to_string(),
                "0005_change_preview_events".to_string(),
                "0006_uri_segments".to_string(),
                "0007_add_uri_segment_to_prompts".to_string(),
                "0008_add_job_id_to_prompts".to_string(),
                "0009_cohere_embeddings".to_string(),
                "0010_anon_device".to_string(),
                "0011_session_current_pos".to_string(),
                "0012_resource_surfacings".to_string(),
                "0013_memory_redux".to_string(),
                "0014_add_tool_use_id_to_prompts".to_string(),
                "0015_add_artifact_confirmed".to_string(),
                "0016_remove_ready_status".to_string(),
                "0017_messages_delivered_at".to_string(),
                "0018_pr_node_port_fires".to_string(),
                "0019_merge_request_owner".to_string(),
                "0020_add_uri_segment_to_action_runs".to_string(),
                "0021_vibe_axes".to_string(),
                "0022_add_segments_to_permission_requests".to_string(),
                "0023_add_labels".to_string(),
                "0024_add_parent_issue".to_string(),
                "0025_remove_managers".to_string(),
                "0026_child_side_channel_notices".to_string(),
                "0027_session_channel_cursor".to_string(),
                "0028_issue_parent_job".to_string(),
                "0029_queued_messages".to_string(),
                "0030_checkpoint_runs".to_string(),
                "0031_drop_dead_chats_table".to_string(),
                "0032_drop_workspaces_timezone_column".to_string(),
                "0033_annotations".to_string(),
                "0034_annotation_message_links".to_string(),
                "0035_annotation_uri_seq".to_string(),
                "0036_wake_subscriptions".to_string(),
                "0037_unify_side_channel_notices".to_string(),
                "0038_drop_annotation_tables".to_string(),
                "0039_memory_intake_ledger".to_string(),
                "0040_add_is_workspace_to_projects".to_string(),
                "0041_memory_triage_batches_and_drop_when_to_use".to_string(),
                "0042_memory_scope_node_id_and_status_lattice".to_string(),
                "0043_memory_triage_decision".to_string(),
                "0044_jobs_memory_review_state".to_string(),
                "0045_memory_canon_v2_consolidation".to_string(),
                "0046_memory_review_sent_state".to_string(),
                "0047_add_message_urgency".to_string(),
                "0048_add_event_thinking_tokens".to_string(),
                "0049_event_vibes_session_id".to_string(),
                "0050_session_skyline_cache_vibe_watermark".to_string(),
                "0051_clear_skyline_cache_for_content_bar_decomposition".to_string(),
                "0052_clear_skyline_cache_for_visual_height_sizing".to_string(),
                "0053_job_pack_anchor".to_string(),
                "0054_archival_storage".to_string(),
                "0055_archival_backfill_state".to_string(),
                "0056_archival_blobs".to_string(),
                "0057_event_read_tokens".to_string(),
                "0058_terminal_exit_wakes".to_string(),
                "0059_jobs_needs_fresh_session".to_string(),
                "0060_attention_items".to_string(),
                "0061_attention_escalate_at".to_string(),
                "0062_attention_fingerprint".to_string(),
                "0063_comment_seq".to_string(),
                "0064_clear_skyline_cache_for_system_event_filter".to_string(),
                "0065_merge_request_is_local".to_string(),
                "0066_config_disables".to_string(),
                "0067_tool_invocations".to_string(),
                "0068_job_browsers".to_string(),
                "0069_add_event_cost_usd".to_string(),
                "0070_attention_pushes".to_string(),
                "0071_attention_push_fingerprint".to_string(),
                "0072_merge_request_head_sha".to_string(),
                "0073_attention_read_cursors".to_string(),
                "0074_drop_attention_ledger".to_string(),
                "0075_drop_messages_delivered_at".to_string(),
                "0076_terminal_output_wakes".to_string(),
                "0077_event_content_change_id".to_string(),
                "0078_browser_last_active_at".to_string(),
                "0079_index_runs_session_id_created_at".to_string(),
                "0080_token_rollup".to_string(),
                "0081_drop_runs_backend".to_string()
            ]
        );
        Ok(db)
    }

    async fn explain_plan(db: &LocalDb, sql: &str) -> Vec<String> {
        db.query_all(format!("EXPLAIN QUERY PLAN {sql}"), (), |row| row.text(3))
            .await
            .unwrap()
    }

    /// 0078 indexes the session-transcript loader's hottest query. This asserts
    /// the planner actually changes its plan when the index is present, against
    /// the real schema, so the index cannot silently become dead weight: without
    /// it the query is a full scan plus a sort; with it the query is an index
    /// seek that also satisfies the ORDER BY.
    #[tokio::test]
    async fn migration_0079_indexes_session_runs_query() {
        const SESSION_RUNS: &str =
            "SELECT id FROM runs WHERE session_id = 'x' ORDER BY created_at ASC";

        // Without the 0079 index migration (every other migration applied): no
        // session_id index, so the planner does a full table scan and sorts for
        // ORDER BY. Filter by version rather than slicing the last migration, so
        // the test stays valid as later migrations are appended.
        let before = {
            let temp = tempdir().unwrap();
            let path = temp.keep().join("cairn-runs-index-before.db");
            let db = LocalDb::open(path).await.unwrap();
            let without_index: Vec<_> = TURSO_MIGRATIONS
                .iter()
                .filter(|m| m.version != "0079")
                .copied()
                .collect();
            MigrationRunner::new(without_index).run(&db).await.unwrap();
            explain_plan(&db, SESSION_RUNS).await
        };
        assert!(
            before.iter().any(|step| step.contains("SCAN runs"))
                && before.iter().any(|step| step.contains("SORTER")),
            "expected full scan + sort before the index, got {before:?}"
        );

        // After 0078: the composite (session_id, created_at) index turns the
        // query into an index seek that also satisfies the ORDER BY.
        let db = migrated_db().await.unwrap();
        let after = explain_plan(&db, SESSION_RUNS).await;
        assert!(
            after
                .iter()
                .any(|step| step.contains("SEARCH runs USING INDEX idx_runs_session_id_created_at")),
            "expected an index seek after 0078, got {after:?}"
        );
        assert!(
            !after.iter().any(|step| step.contains("SORTER")),
            "the index should satisfy ORDER BY without a sort, got {after:?}"
        );
    }

    async fn query_i64(db: &LocalDb, sql: &'static str) -> DbResult<i64> {
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn.query(sql, ()).await?;
                let row = rows
                    .next()
                    .await?
                    .ok_or_else(|| DbError::Row("missing integer row".to_string()))?;
                row.i64(0)
            })
        })
        .await
    }

    async fn query_text(db: &LocalDb, sql: &'static str) -> DbResult<String> {
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn.query(sql, ()).await?;
                let row = rows
                    .next()
                    .await?
                    .ok_or_else(|| DbError::Row("missing text row".to_string()))?;
                row.text(0)
            })
        })
        .await
    }

    #[tokio::test]
    async fn migrated_memories_default_to_draft_intake() {
        let db = migrated_db().await.unwrap();

        db.execute_script(
            "
            INSERT OR IGNORE INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('project-1', 'default', 'Project', 'PRJ', '/tmp/project', 1, 1);
            INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
             VALUES ('issue-1', 'project-1', 1, 'Memory issue', 'active', 1, 1);
            INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
             VALUES ('exec-1', 'recipe', 'issue-1', 'project-1', 'running', 1, 1);
            INSERT INTO jobs(id, execution_id, issue_id, project_id, recipe_node_id, node_name, uri_segment, status, created_at, updated_at)
             VALUES ('job-1', 'exec-1', 'issue-1', 'project-1', 'builder', 'builder', 'builder', 'running', 1, 1);
            INSERT INTO memories(id, content, job_id, node_seq, created_at, updated_at)
             VALUES ('capture', 'what happened and where', 'job-1', 1, 1, 1);
            ",
        )
        .await
        .unwrap();

        assert_eq!(
            query_text(&db, "SELECT status FROM memories WHERE id = 'capture'")
                .await
                .unwrap(),
            "draft"
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'idx_memories_pending_created'"
            )
            .await
            .unwrap(),
            1
        );
    }

    /// 0074 drops the dead attention ledger tables; 0075 drops the retired
    /// messages.delivered_at column and its partial index.
    #[tokio::test]
    async fn migrations_0074_0075_drop_attention_ledger_and_delivered_at() {
        let db = migrated_db().await.unwrap();

        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='attention_items'"
            )
            .await
            .unwrap(),
            0,
            "attention_items should be dropped"
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='attention_seen'"
            )
            .await
            .unwrap(),
            0,
            "attention_seen should be dropped"
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='attention_evaluations'"
            )
            .await
            .unwrap(),
            0,
            "attention_evaluations should be dropped"
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('messages') WHERE name='delivered_at'"
            )
            .await
            .unwrap(),
            0,
            "messages.delivered_at should be dropped"
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_messages_pending_directs'"
            )
            .await
            .unwrap(),
            0,
            "idx_messages_pending_directs should be dropped"
        );
    }

    /// 0031 drops the dead project-chat `chats` table. The `chat_id` foreign-key
    /// columns on `runs` and `sessions` survive as vestigial, always-NULL
    /// columns. This proves that with the parent table gone, inserts into both
    /// child tables (chat_id NULL) still succeed under the enforced
    /// `PRAGMA foreign_keys = ON` — i.e. the now-dangling FK does not break the
    /// hot insert paths.
    #[tokio::test]
    async fn migration_0031_drops_chats_and_keeps_child_inserts_working() {
        let db = migrated_db().await.unwrap();

        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'chats'"
            )
            .await
            .unwrap(),
            0
        );

        db.execute_script(
            "
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('proj-x', 'default', 'Project', 'PX', '/tmp/px', 1, 1);
            INSERT INTO runs(id, project_id, created_at, updated_at)
             VALUES ('run-x', 'proj-x', 1, 1);
            INSERT INTO jobs(id, project_id, status, created_at, updated_at)
             VALUES ('job-x', 'proj-x', 'running', 1, 1);
            INSERT INTO sessions(id, job_id, created_at, updated_at)
             VALUES ('sess-x', 'job-x', 1, 1);
            ",
        )
        .await
        .unwrap();

        assert_eq!(
            query_i64(&db, "SELECT COUNT(*) FROM runs WHERE id = 'run-x'")
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            query_i64(&db, "SELECT COUNT(*) FROM sessions WHERE id = 'sess-x'")
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn real_migrations_apply_once_under_mvcc() {
        let db = migrated_db().await.unwrap();

        assert_eq!(
            query_i64(&db, "SELECT COUNT(*) FROM cairn_schema_migrations")
                .await
                .unwrap(),
            TURSO_MIGRATIONS.len() as i64
        );

        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('workspaces') WHERE name = 'timezone'"
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('projects') WHERE name = 'is_workspace'"
            )
            .await
            .unwrap(),
            1
        );
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
                     VALUES ('default-project', 'default', 'Default Project', 'DP', '/tmp/dp', 1, 1)",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
        assert_eq!(
            query_i64(
                &db,
                "SELECT is_workspace FROM projects WHERE id = 'default-project'"
            )
            .await
            .unwrap(),
            0
        );

        // 0025: the manager stack is physically removed. Manager tables gone,
        // manager columns gone, but every non-manager column/index/trigger on
        // the rebuilt tables is preserved. (The aggregate manager-table check
        // is asserted below via `name LIKE '%manager%'`.)
        //
        // manager_id / recipient_manager_id columns are gone from every table.
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('issues') WHERE name = 'manager_id'"
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('jobs') WHERE name = 'manager_id'"
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('turns') WHERE name = 'manager_id'"
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('merge_requests') WHERE name = 'manager_id'"
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('messages') WHERE name = 'recipient_manager_id'"
            )
            .await
            .unwrap(),
            0
        );
        // Non-manager columns survive the rebuild.
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('issues') WHERE name = 'parent_issue_id'"
            )
            .await
            .unwrap(),
            1
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('jobs') WHERE name = 'uri_segment'"
            )
            .await
            .unwrap(),
            1
        );
        // Manager indexes are gone; non-manager indexes and parent index survive.
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name IN ('idx_jobs_manager_id', 'idx_turns_manager_id', 'idx_messages_recipient_manager_id', 'idx_mr_manager')"
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'idx_issues_parent_id'"
            )
            .await
            .unwrap(),
            1
        );
        // Search triggers dropped with the issues/messages rebuilds are restored.
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'trigger' AND name IN ('search_issues_insert', 'search_issues_update', 'search_issues_delete', 'search_messages_insert', 'search_messages_update', 'search_messages_delete')"
            )
            .await
            .unwrap(),
            6
        );
        // No FK in the whole schema still points at a manager table, and no
        // leftover rebuild scratch tables remain.
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name LIKE '%manager%'"
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM sqlite_master WHERE name LIKE '%_new' OR name LIKE '%_old'"
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('issues') WHERE name = 'parent_issue_id'"
            )
            .await
            .unwrap(),
            1
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_index_list('issues') WHERE name = 'idx_issues_parent_id'"
            )
            .await
            .unwrap(),
            1
        );
        // 0022: permission_requests gains job_id + uri_segment for addressability.
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('permission_requests') WHERE name IN ('job_id', 'uri_segment')"
            )
            .await
            .unwrap(),
            2
        );
        // 0021: event_vibes recreated with PHASE/FRICTION coordinates, no locus.
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('event_vibes') WHERE name IN ('phase', 'friction')"
            )
            .await
            .unwrap(),
            2
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('event_vibes') WHERE name IN ('locus', 'similarity')"
            )
            .await
            .unwrap(),
            0
        );
        // 0015: artifacts gains the `confirmed` resolution column.
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('artifacts') WHERE name = 'confirmed'"
            )
            .await
            .unwrap(),
            1
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('jobs') WHERE name = 'uri_segment'"
            )
            .await
            .unwrap(),
            1
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('action_runs') WHERE name = 'uri_segment'"
            )
            .await
            .unwrap(),
            1
        );
        // 0031 dropped the dead project-chat `chats` table entirely.
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'chats'"
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            query_i64(&db, "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'idx_jobs_issue_execution_uri_segment'")
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            query_i64(&db, "SELECT COUNT(*) FROM workspaces WHERE id = 'default'")
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name LIKE '%_fts%'"
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            query_text(&db, "PRAGMA journal_mode").await.unwrap(),
            "mvcc"
        );

        // Memory intake ledger: applicability text was retired; triggers table
        // and the legacy keywords column is gone.
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('memories') WHERE name = 'when_to_use'"
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('memories') WHERE name = 'keywords'"
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'memory_triggers'"
            )
            .await
            .unwrap(),
            0
        );

        let applied = MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        assert!(applied.is_empty());
    }

    #[tokio::test]
    async fn uri_segment_backfill_handles_natural_suffix_collisions() {
        let temp = tempdir().unwrap();
        let path = temp.keep().join("cairn-uri-collision.db");
        let db = LocalDb::open(path).await.unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS[..5].to_vec())
            .run(&db)
            .await
            .unwrap();

        db.execute_script(
            "
            INSERT OR IGNORE INTO workspaces(id, name, created_at, updated_at)
             VALUES ('default', 'Default', 1, 1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('project-1', 'default', 'Project', 'PROJ', '/tmp/project', 1, 1);
            INSERT INTO issues(id, project_id, number, title, created_at, updated_at)
             VALUES ('issue-1', 'project-1', 1, 'Issue', 1, 1);
            INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
             VALUES ('exec-1', 'recipe-1', 'issue-1', 'project-1', 'running', 1, 1);
            INSERT INTO jobs(id, execution_id, recipe_node_id, issue_id, project_id, node_name, status, created_at, updated_at)
             VALUES ('parent-1', 'exec-1', 'parent', 'issue-1', 'project-1', 'Parent', 'running', 1, 1);
            INSERT INTO jobs(id, execution_id, recipe_node_id, issue_id, project_id, node_name, status, created_at, updated_at)
             VALUES ('unsafe-parent', 'exec-1', 'unsafe', 'issue-1', 'project-1', 'Build / Test?#', 'running', 2, 2);
            INSERT INTO jobs(id, execution_id, parent_job_id, issue_id, project_id, node_name, status, task_index, created_at, updated_at)
             VALUES ('task-1', 'exec-1', 'parent-1', 'issue-1', 'project-1', 'Explore', 'running', 0, 3, 3);
            INSERT INTO jobs(id, execution_id, parent_job_id, issue_id, project_id, node_name, status, task_index, created_at, updated_at)
             VALUES ('task-2', 'exec-1', 'parent-1', 'issue-1', 'project-1', 'Explore', 'running', 1, 4, 4);
            INSERT INTO jobs(id, execution_id, parent_job_id, issue_id, project_id, node_name, status, task_index, created_at, updated_at)
             VALUES ('task-3', 'exec-1', 'parent-1', 'issue-1', 'project-1', 'Explore 2', 'running', 2, 5, 5);
            INSERT INTO chats(id, project_id, status, created_at, updated_at)
             VALUES ('chat-1', 'project-1', 'running', 6, 6);
            INSERT INTO chats(id, project_id, status, created_at, updated_at)
             VALUES ('chat-2', 'project-1', 'running', 7, 7);
            ",
        )
        .await
        .unwrap();

        MigrationRunner::new(vec![TURSO_MIGRATIONS[5]])
            .run(&db)
            .await
            .unwrap();

        assert_eq!(
            query_text(&db, "SELECT uri_segment FROM jobs WHERE id = 'parent-1'")
                .await
                .unwrap(),
            "parent"
        );
        assert_eq!(
            query_text(
                &db,
                "SELECT uri_segment FROM jobs WHERE id = 'unsafe-parent'"
            )
            .await
            .unwrap(),
            "build-test"
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM jobs WHERE uri_segment GLOB '*[^a-z0-9-]*'"
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(DISTINCT uri_segment) FROM jobs WHERE parent_job_id = 'parent-1'"
            )
            .await
            .unwrap(),
            3
        );
        assert_eq!(
            query_text(&db, "SELECT uri_segment FROM jobs WHERE id = 'task-1'")
                .await
                .unwrap(),
            "explore"
        );
        assert_eq!(
            query_text(&db, "SELECT uri_segment FROM jobs WHERE id = 'task-2'")
                .await
                .unwrap(),
            "explore-task-2"
        );
        assert_eq!(
            query_text(&db, "SELECT uri_segment FROM jobs WHERE id = 'task-3'")
                .await
                .unwrap(),
            "explore-2"
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM jobs WHERE parent_job_id = 'parent-1' AND uri_segment IS NOT NULL"
            )
            .await
            .unwrap(),
            3
        );
        assert_eq!(
            query_text(&db, "SELECT uri_segment FROM chats WHERE id = 'chat-1'")
                .await
                .unwrap(),
            "chat"
        );
        assert_eq!(
            query_text(&db, "SELECT uri_segment FROM chats WHERE id = 'chat-2'")
                .await
                .unwrap(),
            "chat-chat-2"
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(DISTINCT uri_segment) FROM chats WHERE project_id = 'project-1'"
            )
            .await
            .unwrap(),
            2
        );
    }

    #[tokio::test]
    async fn real_schema_search_outbox_tracks_committed_writes_only() {
        let db = migrated_db().await.unwrap();

        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at)
             VALUES ('workspace-1', 'Workspace', 1, 1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('project-1', 'workspace-1', 'Project', 'PROJ', '/tmp/project', 1, 1);
            INSERT INTO issues(id, project_id, number, title, description, created_at, updated_at)
             VALUES ('issue-1', 'project-1', 1, 'Turso migration', 'Index me', 1, 1);
            INSERT INTO comments(id, issue_id, content, source, created_at)
             VALUES ('comment-1', 'issue-1', 'Committed comment', 'user', 2);
            INSERT INTO messages(id, channel_type, channel_id, sender_name, content, created_at)
             VALUES ('message-1', 'issue', 'issue-1', 'system', 'Committed message', 3);
            ",
        )
        .await
        .unwrap();

        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM search_outbox WHERE status = 'pending'"
            )
            .await
            .unwrap(),
            3
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM search_outbox WHERE source_table IN ('issues', 'comments', 'messages')"
            )
            .await
            .unwrap(),
            3
        );

        let error = db
            .write(|conn| {
                Box::pin(async move {
                    conn.execute(
                        "INSERT INTO issues(id, project_id, number, title, description, created_at, updated_at)
                         VALUES ('rolled-back-issue', 'project-1', 2, 'Rollback', 'Do not index', 4, 4)",
                        (),
                    )
                    .await?;
                    Err::<(), DbError>(DbError::internal("force rollback"))
                })
            })
            .await
            .unwrap_err();
        assert!(matches!(error, DbError::Internal(_)));

        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM issues WHERE id = 'rolled-back-issue'"
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            query_i64(&db, "SELECT COUNT(*) FROM search_outbox")
                .await
                .unwrap(),
            3
        );
    }

    #[tokio::test]
    async fn migration_0042_backfills_memory_spine_columns() {
        let temp = tempdir().unwrap();
        let path = temp.keep().join("cairn-memory-0042.db");
        let db = LocalDb::open(path).await.unwrap();

        let pre = MigrationRunner::new(TURSO_MIGRATIONS[..41].to_vec())
            .run(&db)
            .await
            .unwrap();
        assert_eq!(pre.len(), 41);

        db.execute_script(
            "
            INSERT OR IGNORE INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at, is_workspace)
             VALUES ('workspace', 'default', 'Workspace', 'WKS', '/tmp/workspace', 1, 1, 1);
            INSERT OR IGNORE INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('project-1', 'default', 'Project', 'PRJ', '/tmp/project', 1, 1);
            INSERT INTO memories(id, project_id, content, confidence, status, provenance_uri, created_at, updated_at, surfaced_count, active)
             VALUES ('workspace-memory', NULL, 'workspace content', 'tentative', 'handled', NULL, 1, 1, 0, 1);
            INSERT INTO memories(id, project_id, content, confidence, status, provenance_uri, created_at, updated_at, surfaced_count, active)
             VALUES ('project-memory', 'project-1', 'project content', 'tentative', 'pending', NULL, 2, 2, 0, 1);
            INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
             VALUES ('issue-1', 'project-1', 1, 'Memory triage', 'active', 1, 1);
            INSERT INTO memory_triage_issue_memories(issue_id, memory_id)
             VALUES ('issue-1', 'project-memory');
            ",
        )
        .await
        .unwrap();

        let applied = MigrationRunner::new(vec![TURSO_MIGRATIONS[41]])
            .run(&db)
            .await
            .unwrap();
        assert_eq!(
            applied,
            vec!["0042_memory_scope_node_id_and_status_lattice".to_string()]
        );

        assert_eq!(
            query_text(
                &db,
                "SELECT status FROM memories WHERE id = 'workspace-memory'"
            )
            .await
            .unwrap(),
            "claimed"
        );
        assert_eq!(
            query_text(
                &db,
                "SELECT project_id || ':' || scope || ':' || scope_value FROM memories WHERE id = 'workspace-memory'"
            )
            .await
            .unwrap(),
            "workspace:workspace:workspace"
        );
        let applied_0043 = MigrationRunner::new(vec![TURSO_MIGRATIONS[42]])
            .run(&db)
            .await
            .unwrap();
        assert_eq!(
            applied_0043,
            vec!["0043_memory_triage_decision".to_string()]
        );

        assert_eq!(
            query_text(
                &db,
                "SELECT scope || ':' || scope_value FROM memories WHERE id = 'project-memory'"
            )
            .await
            .unwrap(),
            "project:project-1"
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM memories WHERE status = 'handled'"
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM memories WHERE job_id IS NULL AND node_seq IS NULL AND promoted_commit_sha IS NULL AND reason IS NULL"
            )
            .await
            .unwrap(),
            2
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('memories') WHERE name IN ('scope', 'scope_value', 'job_id', 'node_seq', 'promoted_commit_sha', 'reason')"
            )
            .await
            .unwrap(),
            6
        );
        assert_eq!(
            query_text(
                &db,
                "SELECT dflt_value FROM pragma_table_info('memories') WHERE name = 'status'"
            )
            .await
            .unwrap(),
            "'draft'"
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'idx_memories_job_node_seq'"
            )
            .await
            .unwrap(),
            1
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM memory_triage_issue_memories WHERE issue_id = 'issue-1' AND memory_id = 'project-memory'"
            )
            .await
            .unwrap(),
            1
        );

        let applied_0044 = MigrationRunner::new(vec![TURSO_MIGRATIONS[43]])
            .run(&db)
            .await
            .unwrap();
        assert_eq!(
            applied_0044,
            vec!["0044_jobs_memory_review_state".to_string()]
        );
        let applied_0045 = MigrationRunner::new(vec![TURSO_MIGRATIONS[44]])
            .run(&db)
            .await
            .unwrap();
        assert_eq!(
            applied_0045,
            vec!["0045_memory_canon_v2_consolidation".to_string()]
        );
        assert_eq!(
            query_i64(&db, "SELECT COUNT(*) FROM memories")
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            query_i64(&db, "SELECT COUNT(*) FROM memory_triage_issue_memories")
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('memories') WHERE name IN ('confidence', 'active', 'surfaced_count', 'last_surfaced_at')"
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('memories') WHERE name = 'name'"
            )
            .await
            .unwrap(),
            1
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM pragma_table_info('memories') WHERE name IN ('job_id', 'node_seq') AND \"notnull\" = 1"
            )
            .await
            .unwrap(),
            2
        );
    }

    /// Apply migrations through 0024 (manager schema present), seed a manager
    /// plus a row referencing it in every manager column, then apply the
    /// FK-off migration 0025. Proves the runner's foreign-keys-off path handles
    /// real referencing data without violation, drops the manager surface
    /// physically, preserves every non-manager row, and leaves no foreign key
    /// pointing at a manager table.
    #[tokio::test]
    async fn migration_0025_removes_managers_with_referencing_data() {
        let temp = tempdir().unwrap();
        let path = temp.keep().join("cairn-remove-managers.db");
        let db = LocalDb::open(path).await.unwrap();

        // Everything before 0025 (manager tables + manager_id columns present).
        let pre = MigrationRunner::new(TURSO_MIGRATIONS[..24].to_vec())
            .run(&db)
            .await
            .unwrap();
        assert_eq!(pre.len(), 24);

        // Seed a manager and a referencing row in each manager column.
        db.execute_script(
            "
            INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
            INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','P','P','/tmp',1,1);
            INSERT INTO managers(id,project_id,name,branch,created_at,updated_at) VALUES('m','p','M','b',1,1);
            INSERT INTO issues(id,project_id,number,title,description,manager_id,created_at,updated_at) VALUES('i','p',1,'Issue title','Index me','m',1,1);
            INSERT INTO jobs(id,project_id,node_name,status,manager_id,created_at,updated_at) VALUES('j','p','N','running','m',1,1);
            INSERT INTO turns(id,session_id,sequence,manager_id,created_at,updated_at) VALUES('t','sess',1,'m',1,1);
            INSERT INTO messages(id,channel_type,channel_id,sender_name,content,recipient_manager_id,created_at) VALUES('msg','direct','i','system','hello','m',1);
            INSERT INTO merge_requests(id,job_id,project_id,issue_id,manager_id,title,source_branch,target_branch,opened_at,updated_at) VALUES('mr','j','p','i','m','PR','src','dst',1,1);
            ",
        )
        .await
        .unwrap();

        // Apply 0025 alone (the FK-off rebuild migration).
        let applied = MigrationRunner::new(vec![TURSO_MIGRATIONS[24]])
            .run(&db)
            .await
            .unwrap();
        assert_eq!(applied, vec!["0025_remove_managers".to_string()]);

        // Manager tables are physically gone.
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name LIKE '%manager%'"
            )
            .await
            .unwrap(),
            0
        );
        // No scratch rebuild tables left behind.
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM sqlite_master WHERE name LIKE '%_new' OR name LIKE '%_old'"
            )
            .await
            .unwrap(),
            0
        );
        // Every referencing row survived the rebuild.
        assert_eq!(
            query_i64(&db, "SELECT COUNT(*) FROM issues WHERE id = 'i'")
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            query_i64(&db, "SELECT COUNT(*) FROM jobs WHERE id = 'j'")
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            query_i64(&db, "SELECT COUNT(*) FROM turns WHERE id = 't'")
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            query_i64(&db, "SELECT COUNT(*) FROM messages WHERE id = 'msg'")
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            query_i64(&db, "SELECT COUNT(*) FROM merge_requests WHERE id = 'mr'")
                .await
                .unwrap(),
            1
        );

        // No foreign key anywhere still targets a manager table, and the rebuilt
        // tables keep their non-manager foreign keys (e.g. jobs -> projects).
        let (manager_fks, jobs_to_projects) = db
            .read(|conn| {
                Box::pin(async move {
                    let mut tables = Vec::new();
                    let mut rows = conn
                        .query("SELECT name FROM sqlite_master WHERE type = 'table'", ())
                        .await?;
                    while let Some(row) = rows.next().await? {
                        tables.push(row.text(0)?);
                    }
                    drop(rows);

                    let mut manager_fks = 0i64;
                    let mut jobs_to_projects = 0i64;
                    for table in &tables {
                        let q = format!("PRAGMA foreign_key_list('{table}')");
                        let mut rows = conn.query(&q, ()).await?;
                        while let Some(row) = rows.next().await? {
                            // columns: id, seq, table, from, to, ...
                            let target = row.text(2)?;
                            if target.starts_with("manager") {
                                manager_fks += 1;
                            }
                            if table == "jobs" && target == "projects" {
                                jobs_to_projects += 1;
                            }
                        }
                    }
                    Ok((manager_fks, jobs_to_projects))
                })
            })
            .await
            .unwrap();
        assert_eq!(
            manager_fks, 0,
            "no FK should still point at a manager table"
        );
        assert!(
            jobs_to_projects >= 1,
            "jobs must retain its non-manager FK to projects"
        );

        // Search triggers survived the rebuild: inserting a fresh issue enqueues
        // a search_outbox row.
        let before = query_i64(&db, "SELECT COUNT(*) FROM search_outbox")
            .await
            .unwrap();
        db.execute(
            "INSERT INTO issues(id,project_id,number,title,description,created_at,updated_at) VALUES('i2','p',2,'Another','Index me too',2,2)",
            (),
        )
        .await
        .unwrap();
        let after = query_i64(&db, "SELECT COUNT(*) FROM search_outbox")
            .await
            .unwrap();
        assert_eq!(after, before + 1, "issues search trigger must still fire");
    }
}
