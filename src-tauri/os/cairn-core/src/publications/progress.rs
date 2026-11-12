//! A leg's progress while its batch runs: the placement request it was
//! presented as, where it stands (queued, then the machine that claimed it),
//! and its output chunk by chunk. Each event lands in the leg's ledger row and
//! log file as it arrives, so an operator can tell a leg waiting for a builder
//! from one compiling, see which machine has it, and read its output hours
//! before it ends; a restart keeps whatever was written.
use std::path::PathBuf;
use std::sync::Arc;

use cairn_db::storage::publications::{self as ledger, LegUpdate};
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, oneshot};

use crate::execution::checks::{BatchProgress, CheckPhaseStatus};
use crate::security::{RedactionPolicy, Sanitizer};
use crate::storage::LocalDb;

/// The prefix of a line Cairn writes into a leg's log, as against the leg's
/// own output.
pub(crate) const CAIRN_LINE: &str = "[cairn] ";

/// Which row and file a recorder writes.
pub(crate) struct LegLog {
    pub publication: String,
    pub target: String,
    pub leg: String,
    pub path: PathBuf,
}

fn phase_line(phase: &CheckPhaseStatus, builder: Option<&str>) -> Option<String> {
    let what = phase.label.as_deref().or(phase.phase.as_deref())?;
    let mut line = format!("{CAIRN_LINE}{what}");
    if let Some(builder) = builder {
        line.push_str(&format!(" on {builder}"));
    }
    if let Some(diagnostic) = phase.diagnostic.as_deref().filter(|d| !d.is_empty()) {
        line.push_str(&format!(": {diagnostic}"));
    }
    Some(line)
}

/// Record `events` into the leg's row and log until `done` fires, then drain
/// what is left. Returns the lines Cairn wrote, for the final log to keep.
pub(crate) async fn record(
    db: Arc<LocalDb>,
    log: LegLog,
    policy: Arc<RedactionPolicy>,
    name_of: impl Fn(&str) -> String + Send + 'static,
    mut events: mpsc::UnboundedReceiver<BatchProgress>,
    mut done: oneshot::Receiver<()>,
) -> Vec<String> {
    let mut lines: Vec<String> = Vec::new();
    let mut file = match open(&log.path).await {
        Ok(file) => Some(file),
        Err(error) => {
            log::warn!(
                "publication {}: could not open the {} {} log at {}: {error}",
                log.publication,
                log.target,
                log.leg,
                log.path.display()
            );
            None
        }
    };
    let mut last_phase: Option<String> = None;
    let mut builder: Option<String> = None;
    loop {
        let event = tokio::select! {
            event = events.recv() => match event {
                Some(event) => event,
                None => break,
            },
            _ = &mut done => {
                while let Ok(event) = events.try_recv() {
                    apply(&db, &log, &policy, &name_of, &mut file, &mut lines, &mut last_phase, &mut builder, event).await;
                }
                break;
            }
        };
        apply(
            &db,
            &log,
            &policy,
            &name_of,
            &mut file,
            &mut lines,
            &mut last_phase,
            &mut builder,
            event,
        )
        .await;
    }
    lines
}

async fn open(path: &std::path::Path) -> std::io::Result<tokio::fs::File> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::File::create(path).await
}

#[allow(clippy::too_many_arguments)]
async fn apply(
    db: &LocalDb,
    log: &LegLog,
    policy: &RedactionPolicy,
    name_of: &(impl Fn(&str) -> String + Send),
    file: &mut Option<tokio::fs::File>,
    lines: &mut Vec<String>,
    last_phase: &mut Option<String>,
    builder: &mut Option<String>,
    event: BatchProgress,
) {
    let mut update = LegUpdate::default();
    let text = match event {
        BatchProgress::Request(id) => {
            update.request_id = Some(id.clone());
            let line = format!("{CAIRN_LINE}presented to placement as request {id}");
            lines.push(line.clone());
            Some(format!("{line}\n"))
        }
        BatchProgress::Phase(phase) => {
            if let Some(executor) = phase.executor.as_deref().filter(|e| !e.is_empty()) {
                let name = name_of(executor);
                if builder.as_deref() != Some(name.as_str()) {
                    update.builder = Some(name.clone());
                    *builder = Some(name);
                }
            }
            match phase_line(&phase, builder.as_deref()) {
                Some(line) if last_phase.as_deref() != Some(line.as_str()) => {
                    *last_phase = Some(line.clone());
                    lines.push(line.clone());
                    Some(format!("{line}\n"))
                }
                _ => None,
            }
        }
        BatchProgress::Output(mut chunk) => {
            Sanitizer::structural(policy).text_in_place(&mut chunk);
            Some(chunk)
        }
    };
    if update.request_id.is_some() || update.builder.is_some() {
        if let Err(error) =
            ledger::update_leg(db, &log.publication, &log.target, &log.leg, update).await
        {
            log::warn!(
                "publication {}: could not record the {} {} leg's progress: {error}",
                log.publication,
                log.target,
                log.leg
            );
        }
    }
    if let (Some(file), Some(text)) = (file.as_mut(), text) {
        // Flushed per event: the file is what a reader sees mid-build.
        if file.write_all(text.as_bytes()).await.is_ok() {
            let _ = file.flush().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_db::storage::publications::NewPublication;

    #[tokio::test]
    async fn a_running_leg_shows_its_request_builder_and_output_before_it_ends() {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(
            LocalDb::open_migrated_for_test(dir.path().join("ledger.db"))
                .await
                .unwrap(),
        );
        ledger::start(
            &db,
            NewPublication {
                id: "pub-1".into(),
                project_id: "p".into(),
                kind: "executor".into(),
                commit: None,
                trigger: "manual".into(),
                requested_by: None,
                force_targets: vec![],
                tool: None,
                mode: "shadow".into(),
                created_at: 1,
            },
            "",
        )
        .await
        .unwrap();
        ledger::claim_next(&db, "p").await.unwrap();
        ledger::add_leg(&db, "pub-1", "t", "build", 1)
            .await
            .unwrap();
        ledger::update_leg(
            &db,
            "pub-1",
            "t",
            "build",
            LegUpdate {
                state: Some("running".into()),
                started_at: Some(2),
                ..LegUpdate::default()
            },
        )
        .await
        .unwrap();
        let path = dir.path().join("publications/pub-1/t.build.log");
        let (tx, rx) = mpsc::unbounded_channel();
        let (done_tx, done_rx) = oneshot::channel();
        let recorder = tokio::spawn(record(
            db.clone(),
            LegLog {
                publication: "pub-1".into(),
                target: "t".into(),
                leg: "build".into(),
                path: path.clone(),
            },
            crate::security::redaction_policy(dir.path()),
            |id: &str| {
                if id == "exec-7" {
                    "fedora".into()
                } else {
                    id.into()
                }
            },
            rx,
            done_rx,
        ));
        tx.send(BatchProgress::Request("req-1".into())).unwrap();
        let mut queued = CheckPhaseStatus::default();
        queued.phase = Some("queued".into());
        queued.label = Some("queued".into());
        queued.diagnostic = Some("behind cairn/12's rust-tests".into());
        tx.send(BatchProgress::Phase(queued)).unwrap();
        let mut running = CheckPhaseStatus::default();
        running.phase = Some("running".into());
        running.label = Some("running checks".into());
        running.executor = Some("exec-7".into());
        tx.send(BatchProgress::Phase(running)).unwrap();
        tx.send(BatchProgress::Output(
            "   Compiling cairn-core v0.13.7\n".into(),
        ))
        .unwrap();

        // The batch has not ended: the log and the row already say what is happening.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let text = loop {
            let text = tokio::fs::read_to_string(&path).await.unwrap_or_default();
            if text.contains("Compiling cairn-core") || std::time::Instant::now() > deadline {
                break text;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        };
        assert!(
            text.contains("[cairn] presented to placement as request req-1"),
            "{text}"
        );
        assert!(
            text.contains("[cairn] queued: behind cairn/12's rust-tests"),
            "{text}"
        );
        assert!(text.contains("[cairn] running checks on fedora"), "{text}");
        assert!(text.contains("Compiling cairn-core v0.13.7"), "{text}");
        let row = ledger::legs(&db, "pub-1").await.unwrap().remove(0);
        assert_eq!(row.state, "running");
        assert_eq!(row.builder.as_deref(), Some("fedora"));
        assert_eq!(row.request_id.as_deref(), Some("req-1"));

        done_tx.send(()).unwrap();
        let lines = recorder.await.unwrap();
        assert_eq!(lines.len(), 3, "{lines:?}");
    }
}
