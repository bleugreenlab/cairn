//! Notifier + WriteEffects — unified sync-and-emit for write operations.
//!
//! Every DB write that needs frontend invalidation and/or cloud sync currently
//! repeats a 2–3 line pattern: `orch.sync(SyncMessage::Foo(…))` then
//! `emitter.emit("db-change", json!({"table":"foos"}))`.
//!
//! `Notifier` combines both into a single typed call:
//!
//! ```ignore
//! orch.notifier.issue(&issue);          // sync + emit
//! orch.notifier.emit_change("todos");   // emit only (local-only table)
//! ```
//!
//! `WriteEffects` collects effects for multi-entity operations and flushes them
//! all at once after the DB writes succeed — the "post-commit effects boundary."

use std::sync::{Arc, Mutex};

use serde_json::json;
use tokio::sync::mpsc;

use crate::models;
use crate::services::EventEmitter;
use crate::sync::message::*;

/// Combines cloud sync and frontend event emission into a single call.
///
/// Created once during Orchestrator construction; shared via `Arc` clone.
/// All methods are fire-and-forget — errors are silently dropped, matching
/// the existing `orch.sync()` behavior.
#[derive(Clone)]
pub struct Notifier {
    sync_tx: Arc<Mutex<Option<mpsc::UnboundedSender<SyncMessage>>>>,
    emitter: Arc<dyn EventEmitter>,
}

impl Notifier {
    pub fn new(
        sync_tx: Arc<Mutex<Option<mpsc::UnboundedSender<SyncMessage>>>>,
        emitter: Arc<dyn EventEmitter>,
    ) -> Self {
        Self { sync_tx, emitter }
    }

    // --- Syncable entities (cloud sync + frontend emit) ---

    pub fn project(&self, p: &models::Project) {
        self.sync_and_emit(SyncMessage::Project(p.into()), "projects");
    }

    pub fn issue(&self, i: &models::Issue) {
        self.sync_and_emit(SyncMessage::Issue(i.into()), "issues");
    }

    pub fn job(&self, j: &models::Job) {
        self.sync_and_emit(SyncMessage::Job(j.into()), "jobs");
    }

    pub fn run(&self, r: &models::Run) {
        self.sync_and_emit(SyncMessage::Run(r.into()), "runs");
    }

    pub fn event(&self, e: &models::Event) {
        self.sync_and_emit(SyncMessage::Event(e.into()), "events");
    }

    pub fn artifact(&self, a: &models::Artifact) {
        self.sync_and_emit(SyncMessage::Artifact(a.into()), "artifacts");
    }

    pub fn comment(&self, c: &models::Comment) {
        self.sync_and_emit(SyncMessage::Comment(c.into()), "comments");
    }

    // --- Delete (cloud sync + frontend emit) ---

    pub fn deleted(&self, table: &str, id: &str) {
        self.sync(SyncMessage::Delete {
            table: table.to_string(),
            id: id.to_string(),
        });
        self.emit_change(table);
    }

    // --- Local-only entities (emit only, no cloud sync) ---

    /// Emit a `db-change` event for a table that doesn't sync to cloud.
    pub fn emit_change(&self, table: &str) {
        let _ = self.emitter.emit("db-change", json!({"table": table}));
    }

    // --- Raw sync+emit (for manual SyncMessage construction) ---

    /// Send a sync message and emit a db-change event.
    pub fn sync_and_emit(&self, msg: SyncMessage, table: &str) {
        self.sync(msg);
        self.emit_change(table);
    }

    // --- Streaming (fire-and-forget, no emit) ---

    pub fn stream_delta(&self, run_id: &str, event_id: &str, tokens: &str) {
        self.sync(SyncMessage::StreamDelta(StreamDelta {
            run_id: run_id.to_string(),
            event_id: event_id.to_string(),
            tokens: tokens.to_string(),
        }));
    }

    // --- Internal ---

    fn sync(&self, msg: SyncMessage) {
        if let Ok(guard) = self.sync_tx.lock() {
            if let Some(tx) = guard.as_ref() {
                let _ = tx.send(msg);
            }
        }
    }
}

// ── WriteEffects ──────────────────────────────────────────────────────

/// A single deferred effect to be flushed after DB writes complete.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
enum Effect {
    /// Cloud sync + frontend invalidation for a syncable entity.
    SyncAndInvalidate {
        msg: SyncMessage,
        table: &'static str,
    },
    /// Frontend invalidation only (local-only table).
    Invalidate { table: &'static str },
}

/// Collects post-write effects and flushes them through a Notifier.
///
/// Use this for operations that touch multiple entities — collect all effects,
/// then flush once after all DB writes succeed. This is the "post-commit
/// effects boundary."
///
/// ```ignore
/// let mut fx = WriteEffects::new();
/// diesel::update(...).execute(&mut conn)?;
/// fx.run(&run);
/// diesel::update(...).execute(&mut conn)?;
/// fx.job(&job);
/// fx.flush(&orch.notifier);
/// ```
pub struct WriteEffects {
    effects: Vec<Effect>,
}

impl WriteEffects {
    pub fn new() -> Self {
        Self {
            effects: Vec::new(),
        }
    }

    // --- Syncable entities ---

    pub fn project(&mut self, p: &models::Project) -> &mut Self {
        self.effects.push(Effect::SyncAndInvalidate {
            msg: SyncMessage::Project(p.into()),
            table: "projects",
        });
        self
    }

    pub fn issue(&mut self, i: &models::Issue) -> &mut Self {
        self.effects.push(Effect::SyncAndInvalidate {
            msg: SyncMessage::Issue(i.into()),
            table: "issues",
        });
        self
    }

    pub fn job(&mut self, j: &models::Job) -> &mut Self {
        self.effects.push(Effect::SyncAndInvalidate {
            msg: SyncMessage::Job(j.into()),
            table: "jobs",
        });
        self
    }

    pub fn run(&mut self, r: &models::Run) -> &mut Self {
        self.effects.push(Effect::SyncAndInvalidate {
            msg: SyncMessage::Run(r.into()),
            table: "runs",
        });
        self
    }

    pub fn event(&mut self, e: &models::Event) -> &mut Self {
        self.effects.push(Effect::SyncAndInvalidate {
            msg: SyncMessage::Event(e.into()),
            table: "events",
        });
        self
    }

    pub fn artifact(&mut self, a: &models::Artifact) -> &mut Self {
        self.effects.push(Effect::SyncAndInvalidate {
            msg: SyncMessage::Artifact(a.into()),
            table: "artifacts",
        });
        self
    }

    pub fn comment(&mut self, c: &models::Comment) -> &mut Self {
        self.effects.push(Effect::SyncAndInvalidate {
            msg: SyncMessage::Comment(c.into()),
            table: "comments",
        });
        self
    }

    // --- Delete ---

    pub fn deleted(&mut self, table: &'static str, id: String) -> &mut Self {
        self.effects.push(Effect::SyncAndInvalidate {
            msg: SyncMessage::Delete {
                table: table.to_string(),
                id,
            },
            table,
        });
        self
    }

    // --- Local-only emit ---

    pub fn emit(&mut self, table: &'static str) -> &mut Self {
        self.effects.push(Effect::Invalidate { table });
        self
    }

    /// Flush all collected effects through the notifier.
    pub fn flush(self, notifier: &Notifier) {
        for effect in self.effects {
            match effect {
                Effect::SyncAndInvalidate { msg, table } => {
                    notifier.sync_and_emit(msg, table);
                }
                Effect::Invalidate { table } => {
                    notifier.emit_change(table);
                }
            }
        }
    }
}

impl Default for WriteEffects {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{Comment, CommentSource, IssueStatus};
    use crate::services::testing::CapturingEmitter;

    fn test_notifier() -> (
        Notifier,
        mpsc::UnboundedReceiver<SyncMessage>,
        Arc<CapturingEmitter>,
    ) {
        let (tx, rx) = mpsc::unbounded_channel();
        let sync_tx = Arc::new(Mutex::new(Some(tx)));
        let emitter = Arc::new(CapturingEmitter::new());
        let notifier = Notifier::new(sync_tx, emitter.clone());
        (notifier, rx, emitter)
    }

    fn test_issue() -> models::Issue {
        models::Issue {
            id: "i-1".into(),
            project_id: "p-1".into(),
            number: 1,
            title: "Test".into(),
            description: "".into(),
            status: IssueStatus::Active,
            progress: models::IssueProgress::Active,
            attention: models::IssueAttention::None,
            priority: 0,
            completed_at: None,
            dismissed_at: None,
            created_at: 1000,
            updated_at: 2000,
            backend_override: None,
            merged_at: None,
            closed_at: None,
            manager_id: None,
        }
    }

    fn test_comment() -> Comment {
        Comment {
            id: "c-1".into(),
            issue_id: "i-1".into(),
            content: "hello".into(),
            source: CommentSource::Agent,
            created_at: 3000,
        }
    }

    // ── Notifier tests ──

    #[test]
    fn notifier_issue_syncs_and_emits() {
        let (notifier, mut rx, emitter) = test_notifier();
        let issue = test_issue();

        notifier.issue(&issue);

        // Sync message sent
        let msg = rx.try_recv().unwrap();
        assert!(matches!(msg, SyncMessage::Issue(_)));

        // db-change emitted
        let events = emitter.events_named("db-change");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["table"], "issues");
    }

    #[test]
    fn notifier_emit_change_no_sync() {
        let (notifier, mut rx, emitter) = test_notifier();

        notifier.emit_change("todos");

        // No sync message
        assert!(rx.try_recv().is_err());

        // db-change emitted
        let events = emitter.events_named("db-change");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["table"], "todos");
    }

    #[test]
    fn notifier_deleted_syncs_and_emits() {
        let (notifier, mut rx, emitter) = test_notifier();

        notifier.deleted("issues", "i-1");

        let msg = rx.try_recv().unwrap();
        assert!(matches!(msg, SyncMessage::Delete { .. }));

        let events = emitter.events_named("db-change");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["table"], "issues");
    }

    #[test]
    fn notifier_stream_delta_no_emit() {
        let (notifier, mut rx, emitter) = test_notifier();

        notifier.stream_delta("run-1", "evt-1", "hello world");

        let msg = rx.try_recv().unwrap();
        assert!(matches!(msg, SyncMessage::StreamDelta(_)));

        // No db-change event
        assert!(emitter.events_named("db-change").is_empty());
    }

    #[test]
    fn notifier_noop_when_sync_not_active() {
        let sync_tx = Arc::new(Mutex::new(None));
        let emitter = Arc::new(CapturingEmitter::new());
        let notifier = Notifier::new(sync_tx, emitter.clone());

        // Should not panic, emit still works
        notifier.issue(&test_issue());

        let events = emitter.events_named("db-change");
        assert_eq!(events.len(), 1);
    }

    // ── WriteEffects tests ──

    #[test]
    fn write_effects_batches_and_flushes() {
        let (notifier, mut rx, emitter) = test_notifier();
        let issue = test_issue();
        let comment = test_comment();

        let mut fx = WriteEffects::new();
        fx.issue(&issue).comment(&comment).emit("todos");

        // Nothing sent yet
        assert!(rx.try_recv().is_err());
        assert!(emitter.events_named("db-change").is_empty());

        // Flush
        fx.flush(&notifier);

        // Two sync messages (issue + comment)
        let msg1 = rx.try_recv().unwrap();
        assert!(matches!(msg1, SyncMessage::Issue(_)));
        let msg2 = rx.try_recv().unwrap();
        assert!(matches!(msg2, SyncMessage::Comment(_)));
        assert!(rx.try_recv().is_err()); // no more

        // Three db-change events (issues, comments, todos)
        let events = emitter.events_named("db-change");
        assert_eq!(events.len(), 3);
        assert_eq!(events[0]["table"], "issues");
        assert_eq!(events[1]["table"], "comments");
        assert_eq!(events[2]["table"], "todos");
    }

    #[test]
    fn write_effects_deleted() {
        let (notifier, mut rx, emitter) = test_notifier();

        let mut fx = WriteEffects::new();
        fx.deleted("issues", "i-1".into());
        fx.flush(&notifier);

        let msg = rx.try_recv().unwrap();
        assert!(matches!(msg, SyncMessage::Delete { .. }));

        let events = emitter.events_named("db-change");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["table"], "issues");
    }

    #[test]
    fn write_effects_empty_flush_is_noop() {
        let (notifier, mut rx, emitter) = test_notifier();

        WriteEffects::new().flush(&notifier);

        assert!(rx.try_recv().is_err());
        assert!(emitter.events_named("db-change").is_empty());
    }
}
