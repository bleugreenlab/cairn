//! SyncSender — thin wrapper that makes sync calls ergonomic at write points.

use tokio::sync::mpsc;

use super::message::*;

/// Thin wrapper around mpsc sender. Never blocks, never fails.
/// Calls are no-ops if the channel is closed.
#[derive(Clone)]
pub struct SyncSender {
    tx: mpsc::UnboundedSender<SyncMessage>,
}

impl SyncSender {
    pub fn new(tx: mpsc::UnboundedSender<SyncMessage>) -> Self {
        Self { tx }
    }

    /// Send a raw sync message.
    pub fn send(&self, msg: SyncMessage) {
        let _ = self.tx.send(msg);
    }

    /// Sync a project.
    pub fn project(&self, project: &crate::models::Project) {
        self.send(SyncMessage::Project(SyncProject::from(project)));
    }

    /// Sync an issue.
    pub fn issue(&self, issue: &crate::models::Issue) {
        self.send(SyncMessage::Issue(SyncIssue::from(issue)));
    }

    /// Sync a job.
    pub fn job(&self, job: &crate::models::Job) {
        self.send(SyncMessage::Job(SyncJob::from(job)));
    }

    /// Sync a run.
    pub fn run(&self, run: &crate::models::Run) {
        self.send(SyncMessage::Run(SyncRun::from(run)));
    }

    /// Sync an event.
    pub fn event(&self, event: &crate::models::Event) {
        self.send(SyncMessage::Event(SyncEvent::from(event)));
    }

    /// Sync an artifact.
    pub fn artifact(&self, artifact: &crate::models::Artifact) {
        self.send(SyncMessage::Artifact(SyncArtifact::from(artifact)));
    }

    /// Sync a comment.
    pub fn comment(&self, comment: &crate::models::Comment) {
        self.send(SyncMessage::Comment(SyncComment::from(comment)));
    }

    /// Send a streaming delta (fire-and-forget).
    pub fn stream_delta(&self, run_id: &str, event_id: &str, tokens: &str) {
        self.send(SyncMessage::StreamDelta(StreamDelta {
            run_id: run_id.to_string(),
            event_id: event_id.to_string(),
            tokens: tokens.to_string(),
        }));
    }

    /// Send a delete notification.
    pub fn delete(&self, table: &str, id: &str) {
        self.send(SyncMessage::Delete {
            table: table.to_string(),
            id: id.to_string(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    #[test]
    fn sender_delivers_messages() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let sender = SyncSender::new(tx);

        sender.stream_delta("run-1", "evt-1", "hello");
        sender.delete("issues", "i-1");

        let msg1 = rx.try_recv().unwrap();
        assert!(matches!(msg1, SyncMessage::StreamDelta(_)));

        let msg2 = rx.try_recv().unwrap();
        assert!(matches!(msg2, SyncMessage::Delete { .. }));
    }

    #[test]
    fn sender_is_noop_when_channel_closed() {
        let (tx, rx) = mpsc::unbounded_channel();
        let sender = SyncSender::new(tx);
        drop(rx); // Close receiver

        // Should not panic
        sender.stream_delta("run-1", "evt-1", "hello");
        sender.delete("issues", "i-1");
    }
}
