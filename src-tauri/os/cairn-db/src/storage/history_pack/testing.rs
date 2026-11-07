//! Fixtures for reading through history packs, shared with the crates above
//! this one so each reader's parity test packs an execution the same way.

use std::sync::Arc;

use turso::params;

use super::catalog::{publish, PublishFacts};
use super::write::{pack_execution, BuiltPack};
use crate::storage::content_store::InMemoryContentStore;
use crate::storage::events::compress;
use crate::storage::events::history::{HistoryRow, HistorySource};
use crate::storage::events::CODEC_ZSTD_V1;
use crate::storage::LocalDb;

/// Pack `execution_id` and catalog it, installing an in-memory store if the
/// database has none. Hot rows and `evicted_at` are left alone, so reads stay
/// hot until the source is forced.
pub async fn publish_for_test(db: &LocalDb, execution_id: &str) -> BuiltPack {
    if db.content_store().is_none() {
        let _ = db.set_private_content_store(Arc::new(InMemoryContentStore::new()));
    }
    let built = pack_execution(db, execution_id).await.unwrap();
    publish(
        db,
        &built,
        PublishFacts {
            hot_bytes_estimate: built.stats.hot_event_bytes as i64,
            resolved_at: 1,
            packed_at: 2,
        },
    )
    .await
    .unwrap();
    built
}

/// Run `read` hot and then pack-sourced, and return both results.
pub async fn read_both<T, F, Fut>(db: &LocalDb, read: F) -> (T, T)
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    db.set_history_source_override(Some(HistorySource::Hot));
    let hot = read().await;
    db.set_history_source_override(Some(HistorySource::Pack));
    let pack = read().await;
    db.set_history_source_override(None);
    (hot, pack)
}

/// Mark every catalogued execution evicted and delete the hot rows, as eviction
/// will. A read that still answers from here on can only have come from a pack,
/// so comparing it with an earlier hot read proves the pack path served it.
pub async fn evict_all(db: &LocalDb) {
    db.execute_script(
        "UPDATE history_tier SET evicted_at = 5;
         DELETE FROM events;",
    )
    .await
    .unwrap();
    db.mark_history_evicted();
    db.set_history_source_override(None);
}

/// Every field of a row, for comparing a hot read with a pack read. `Event` has
/// no `PartialEq`, and its `Debug` covers the fields serialization skips.
pub fn fingerprint(rows: &[HistoryRow]) -> Vec<String> {
    rows.iter().map(|row| format!("{row:?}")).collect()
}

/// One event row of the fixture.
struct Row<'a> {
    id: &'a str,
    run: &'a str,
    session: &'a str,
    turn: Option<&'a str>,
    sequence: i64,
    created_at: i64,
    event_type: &'a str,
    data: String,
    storage_mode: Option<&'a str>,
    data_blob: Option<Vec<u8>>,
    codec: Option<&'a str>,
}

/// Event ids of the fixture execution, each named for what it exercises.
pub struct FixtureEvents {
    pub plain: String,
    pub tied: String,
    pub restored: String,
    pub degraded: String,
    pub undecodable: String,
    pub read_result: String,
    pub second_run: String,
    pub second_session: String,
}

/// Seed an execution with two jobs' worth of history that exercises every
/// outcome: plain rows, same-second ties, a zstd row, a blobbed row whose blob
/// is gone (degraded), a zstd row with a corrupt blob (undecodable), token and
/// model columns, change-preview columns, and a row in each of the sidecar
/// tables. Event ids are prefixed so a test can quarantine its own rows: the
/// quarantine set is process-wide.
pub async fn seed_fixture_execution(db: &LocalDb, prefix: &str) -> FixtureEvents {
    db.execute_script(
        "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
         INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
           VALUES('p','w','Project','proj','/nonexistent/repo',1,1);
         INSERT INTO executions(id, recipe_id, project_id, status, started_at)
           VALUES('exec-1','r','p','complete',1);
         INSERT INTO jobs(id, execution_id, project_id, status, current_session_id, created_at, updated_at)
           VALUES('job-1','exec-1','p','complete','sess-2',1,1);
         INSERT INTO sessions(id, job_id, status, created_at, updated_at)
           VALUES('sess-1','job-1','replaced',1,1);
         INSERT INTO sessions(id, job_id, status, created_at, updated_at)
           VALUES('sess-2','job-1','active',2,2);
         INSERT INTO runs(id, project_id, job_id, session_id, status, created_at, updated_at)
           VALUES('run-1','p','job-1','sess-1','exited',10,10);
         INSERT INTO runs(id, project_id, job_id, session_id, status, created_at, updated_at)
           VALUES('run-2','p','job-1','sess-2','exited',10,10);
         INSERT INTO turns(id, session_id, run_id, job_id, sequence, state, created_at, updated_at)
           VALUES('turn-1','sess-1','run-1','job-1',1,'complete',10,10);
         INSERT INTO turns(id, session_id, run_id, job_id, sequence, state, created_at, updated_at)
           VALUES('turn-2','sess-2','run-2','job-1',1,'complete',20,20);
         UPDATE sessions SET replaced_by_id = 'sess-2' WHERE id = 'sess-1';
         UPDATE sessions SET parent_session_id = 'sess-1' WHERE id = 'sess-2';",
    )
    .await
    .unwrap();

    let id = |name: &str| format!("{prefix}-{name}");
    let names = FixtureEvents {
        plain: id("plain"),
        tied: id("tied"),
        restored: id("restored"),
        degraded: id("degraded"),
        undecodable: id("undecodable"),
        read_result: id("read-result"),
        second_run: id("second-run"),
        second_session: id("second-session"),
    };
    let restored_body = serde_json::json!({
        "eventType": "assistant",
        "isError": false,
        "content": "the restored body",
        "toolUses": [{"id": "tool-1", "name": "mcp__cairn__read", "input": {"paths": ["file:a.rs"]}}],
    })
    .to_string();
    let rows = [
        Row {
            id: &names.plain,
            run: "run-1",
            session: "sess-1",
            turn: Some("turn-1"),
            sequence: 1,
            created_at: 10,
            event_type: "user",
            data: serde_json::json!({"eventType": "user", "isError": false, "content": "the task"})
                .to_string(),
            storage_mode: None,
            data_blob: None,
            codec: None,
        },
        // Same second as the row before it, and inserted out of sequence order,
        // so rowid and sequence disagree on purpose.
        Row {
            id: &names.tied,
            run: "run-1",
            session: "sess-1",
            turn: Some("turn-1"),
            sequence: 3,
            created_at: 10,
            event_type: "system:status",
            data: serde_json::json!({"eventType": "system:status", "status": "ok"}).to_string(),
            storage_mode: None,
            data_blob: None,
            codec: None,
        },
        Row {
            id: &names.restored,
            run: "run-1",
            session: "sess-1",
            turn: Some("turn-1"),
            sequence: 2,
            created_at: 10,
            event_type: "assistant",
            data: "{\"_archived\":true}".to_string(),
            storage_mode: Some("zstd"),
            data_blob: Some(compress(restored_body.as_bytes()).unwrap()),
            codec: Some(CODEC_ZSTD_V1),
        },
        Row {
            id: &names.read_result,
            run: "run-1",
            session: "sess-1",
            turn: Some("turn-1"),
            sequence: 4,
            created_at: 11,
            event_type: "tool_result",
            data: serde_json::json!({
                "eventType": "tool_result",
                "isError": false,
                "toolUseId": "tool-1",
                "toolResult": "=== file:a.rs ===\nfn main() {}",
            })
            .to_string(),
            storage_mode: None,
            data_blob: None,
            codec: None,
        },
        Row {
            id: &names.degraded,
            run: "run-1",
            session: "sess-1",
            turn: None,
            sequence: 5,
            created_at: 12,
            event_type: "system:prompt",
            data: serde_json::json!({
                "eventType": "system:prompt",
                "archived": "system_prompt",
                "segments": [{"hash": "0".repeat(64), "kind": "base"}, {"inline": "the tail"}],
            })
            .to_string(),
            storage_mode: Some("gitcoord"),
            data_blob: None,
            codec: None,
        },
        Row {
            id: &names.undecodable,
            run: "run-1",
            session: "sess-1",
            turn: None,
            sequence: 6,
            created_at: 13,
            event_type: "assistant",
            data: "{\"_archived\":true}".to_string(),
            storage_mode: Some("zstd"),
            data_blob: Some(b"not a zstd frame".to_vec()),
            codec: Some(CODEC_ZSTD_V1),
        },
        Row {
            id: &names.second_run,
            run: "run-2",
            session: "sess-2",
            turn: Some("turn-2"),
            sequence: 1,
            created_at: 20,
            event_type: "assistant",
            data: serde_json::json!({
                "eventType": "assistant",
                "isError": false,
                "content": "second run",
            })
            .to_string(),
            storage_mode: None,
            data_blob: None,
            codec: None,
        },
        // A heavy body a transcript list never renders.
        Row {
            id: &names.second_session,
            run: "run-2",
            session: "sess-2",
            turn: Some("turn-2"),
            sequence: 2,
            created_at: 20,
            event_type: "user:seed",
            data: serde_json::json!({"eventType": "user:seed", "content": "x".repeat(4096)})
                .to_string(),
            storage_mode: None,
            data_blob: None,
            codec: None,
        },
    ];
    for row in rows {
        db.execute(
            "INSERT INTO events(id, run_id, session_id, turn_id, sequence, timestamp, event_type,
                                data, created_at, storage_mode, data_blob, codec)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?6, ?9, ?10, ?11)",
            params![
                row.id,
                row.run,
                row.session,
                row.turn,
                row.sequence,
                row.created_at,
                row.event_type,
                row.data,
                row.storage_mode,
                row.data_blob,
                row.codec
            ],
        )
        .await
        .unwrap();
    }
    db.execute(
        "UPDATE events SET input_tokens = 7, output_tokens = 11, cost_usd = 0.125,
                reported_model = 'claude-opus-5-5', selected_model = 'opus',
                change_preview_status = 'applied',
                change_applied_at = 99, account_id = 'acct-a'
         WHERE id = ?1",
        params![names.restored.as_str()],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO event_vibes(event_id, css_color, phase, friction, model, created_at, session_id)
         VALUES (?1, 'oklch(0.5 0.1 200)', 0.25, 0.75, 'm', 1, 'sess-1')",
        params![names.restored.as_str()],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO event_read_tokens(event_id, segments_json, total_tokens, created_at)
         VALUES (?1, '[{\"target\":\"file:a.rs\",\"tokens\":5}]', 5, 1)",
        params![names.read_result.as_str()],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO tool_invocations(id, event_id, tool_use_id, run_id, ts, verb, tool_name,
                                      target_scheme, target_kind)
         VALUES ('inv-1', ?1, 'tool-1', 'run-1', 10, 'read', 'read', 'file', 'rs')",
        params![names.restored.as_str()],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO turn_embeddings(turn_id, session_id, job_id, project_id, sequence, embedding,
                                     model, dims, created_at, updated_at)
         VALUES ('turn-1', 'sess-1', 'job-1', 'p', 1, ?1, 'm', 1, 1, 1)",
        params![vec![0u8, 0, 128, 63]],
    )
    .await
    .unwrap();
    names
}

/// A store wrapper that counts actual object fetches in bounded-work tests.
pub struct ReadCountingStore {
    inner: Arc<dyn crate::storage::ContentStore>,
    reads: std::sync::atomic::AtomicUsize,
}

impl ReadCountingStore {
    pub fn new(inner: Arc<dyn crate::storage::ContentStore>) -> Self {
        Self {
            inner,
            reads: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    pub fn reads(&self) -> usize {
        self.reads.load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[async_trait::async_trait]
impl crate::storage::ContentStore for ReadCountingStore {
    fn local_root(&self) -> Option<&std::path::Path> {
        self.inner.local_root()
    }

    async fn put(&self, hash: &str, bytes: &[u8]) -> Result<(), crate::storage::ContentStoreError> {
        self.inner.put(hash, bytes).await
    }

    async fn get(&self, hash: &str) -> Result<Option<Vec<u8>>, crate::storage::ContentStoreError> {
        self.reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.get(hash).await
    }
}
