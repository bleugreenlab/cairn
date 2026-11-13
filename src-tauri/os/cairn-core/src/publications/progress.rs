//! A leg's progress while its batch runs: the placement request it was
//! presented as, where it stands (queued, then the machine that claimed it),
//! and its output chunk by chunk. Each event lands in the leg's ledger row and
//! log file as it arrives, so an operator can tell a leg waiting for a builder
//! from one compiling, see which machine has it, and read its output hours
//! before it ends; a restart keeps whatever was written.
//!
//! A live target's build and publish run as one batch on the publisher, so
//! the bytes never leave the cell that holds the secret. The ledger still
//! keeps them as two rows with their own times: when the batch's `manifest`
//! result line streams past without a failure, the recorder settles `build`
//! and opens `publish` ([`PhaseSplit`]).
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use cairn_db::storage::publications::{self as ledger, LegUpdate};
use serde_json::Value;
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, oneshot};

use crate::execution::checks::{BatchProgress, CheckPhaseStatus};
use crate::security::{RedactionPolicy, Sanitizer};
use crate::storage::LocalDb;

use super::plan::LEG_RESULT_MARKER;

/// The prefix of a line Cairn writes into a leg's log, as against the leg's
/// own output.
pub(crate) const CAIRN_LINE: &str = "[cairn] ";

/// Which row and file a recorder writes.
pub(crate) struct LegLog {
    pub publication: String,
    pub target: String,
    pub leg: String,
    pub path: PathBuf,
    /// For the publisher's build-and-publish batch: where the `build` row
    /// ends and the `publish` row begins.
    pub split: Option<PhaseSplit>,
}

/// The point in one batch's output where its first row settles and its
/// second opens: the successful `manifest` result, the last step of `build`.
pub(crate) struct PhaseSplit {
    /// The row opened when the first settles.
    pub next_leg: &'static str,
    /// The log the opened row names (the batch's one log).
    pub log_ref: String,
    /// Set once the split has been recorded, so the actor's final account
    /// leaves the settled row alone.
    pub done: Arc<AtomicBool>,
}

/// The leg a result document belongs to, and whether it reports a failure.
fn document_leg(document: &Value) -> Option<&str> {
    document["leg"].as_str()
}

fn document_failed(document: &Value) -> bool {
    ["error", "deferred", "refused"]
        .iter()
        .any(|key| !document[*key].is_null())
}

/// What the recorder carries between events.
#[derive(Default)]
struct Recording {
    lines: Vec<String>,
    last_phase: Option<String>,
    builder: Option<String>,
    request_id: Option<String>,
    /// Output not yet ended by a newline: result lines are read whole.
    partial: String,
    /// The last `build` result document seen, whose assets the build row keeps.
    build_document: Option<Value>,
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
    let mut recording = Recording::default();
    loop {
        let event = tokio::select! {
            event = events.recv() => match event {
                Some(event) => event,
                None => break,
            },
            _ = &mut done => {
                while let Ok(event) = events.try_recv() {
                    apply(&db, &log, &policy, &name_of, &mut file, &mut recording, event).await;
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
            &mut recording,
            event,
        )
        .await;
    }
    recording.lines
}

async fn open(path: &std::path::Path) -> std::io::Result<tokio::fs::File> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::File::create(path).await
}

async fn apply(
    db: &LocalDb,
    log: &LegLog,
    policy: &RedactionPolicy,
    name_of: &(impl Fn(&str) -> String + Send),
    file: &mut Option<tokio::fs::File>,
    recording: &mut Recording,
    event: BatchProgress,
) {
    let mut update = LegUpdate::default();
    let text = match event {
        BatchProgress::Request(id) => {
            update.request_id = Some(id.clone());
            recording.request_id = Some(id.clone());
            let line = format!("{CAIRN_LINE}presented to placement as request {id}");
            recording.lines.push(line.clone());
            Some(format!("{line}\n"))
        }
        BatchProgress::Phase(phase) => {
            if let Some(executor) = phase.executor.as_deref().filter(|e| !e.is_empty()) {
                let name = name_of(executor);
                if recording.builder.as_deref() != Some(name.as_str()) {
                    update.builder = Some(name.clone());
                    recording.builder = Some(name);
                }
            }
            match phase_line(&phase, recording.builder.as_deref()) {
                Some(line) if recording.last_phase.as_deref() != Some(line.as_str()) => {
                    recording.last_phase = Some(line.clone());
                    recording.lines.push(line.clone());
                    Some(format!("{line}\n"))
                }
                _ => None,
            }
        }
        BatchProgress::Output(mut chunk) => {
            Sanitizer::structural(policy).text_in_place(&mut chunk);
            if let Some(split) = &log.split {
                read_results(db, log, split, recording, &chunk).await;
            }
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

/// Read the whole result lines in `chunk`, and record the split when the
/// batch's successful `manifest` result arrives.
async fn read_results(
    db: &LocalDb,
    log: &LegLog,
    split: &PhaseSplit,
    recording: &mut Recording,
    chunk: &str,
) {
    recording.partial.push_str(chunk);
    let Some(end) = recording.partial.rfind('\n') else {
        return;
    };
    let complete: String = recording.partial.drain(..=end).collect();
    for line in complete.lines() {
        let Some(document) = line
            .trim_end()
            .strip_prefix(LEG_RESULT_MARKER)
            .and_then(|json| serde_json::from_str::<Value>(json).ok())
        else {
            continue;
        };
        match document_leg(&document) {
            Some("build") => recording.build_document = Some(document),
            Some("manifest")
                if !document_failed(&document) && !split.done.load(Ordering::SeqCst) =>
            {
                record_split(db, log, split, recording, &document).await;
            }
            _ => {}
        }
    }
}

async fn record_split(
    db: &LocalDb,
    log: &LegLog,
    split: &PhaseSplit,
    recording: &Recording,
    manifest: &Value,
) {
    let now = super::now_ms();
    let build = recording.build_document.as_ref();
    let settled = ledger::update_leg(
        db,
        &log.publication,
        &log.target,
        &log.leg,
        LegUpdate {
            state: Some("succeeded".into()),
            ended_at: Some(now),
            builder: recording.builder.clone(),
            assets: build.and_then(|document| super::assets_of(std::slice::from_ref(document))),
            verifier: Some(manifest["verifier"].clone()).filter(|value| !value.is_null()),
            build_jobs: build
                .map(|document| document["buildJobs"].clone())
                .filter(|value| !value.is_null()),
            ..LegUpdate::default()
        },
    )
    .await;
    if let Err(error) = settled {
        log::warn!(
            "publication {}: could not settle the {} {} leg at its manifest: {error}",
            log.publication,
            log.target,
            log.leg
        );
        return;
    }
    split.done.store(true, Ordering::SeqCst);
    let opened = ledger::update_leg(
        db,
        &log.publication,
        &log.target,
        split.next_leg,
        LegUpdate {
            state: Some("running".into()),
            started_at: Some(now),
            builder: recording.builder.clone(),
            request_id: recording.request_id.clone(),
            log_ref: Some(split.log_ref.clone()),
            ..LegUpdate::default()
        },
    )
    .await;
    if let Err(error) = opened {
        log::warn!(
            "publication {}: could not open the {} {} leg: {error}",
            log.publication,
            log.target,
            split.next_leg
        );
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
                live_targets: vec![],
                created_at: 1,
            },
            "",
        )
        .await
        .unwrap();
        ledger::claim_next(&db, "p").await.unwrap();
        ledger::add_leg(&db, "pub-1", "t", "build", 1, "shadow")
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
                split: None,
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

    /// The publisher's one batch is two rows: when the manifest's result
    /// streams past (even split across chunks), build settles with its assets
    /// and publish opens on the same machine and request, while the batch
    /// still runs.
    #[tokio::test]
    async fn a_build_and_publish_batch_settles_build_at_its_manifest_and_opens_publish() {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(
            LocalDb::open_migrated_for_test(dir.path().join("ledger.db"))
                .await
                .unwrap(),
        );
        ledger::start(
            &db,
            NewPublication {
                id: "pub-2".into(),
                project_id: "p".into(),
                kind: "executor".into(),
                commit: None,
                trigger: "manual".into(),
                requested_by: None,
                force_targets: vec![],
                tool: None,
                mode: "live".into(),
                live_targets: vec!["t".into()],
                created_at: 1,
            },
            "",
        )
        .await
        .unwrap();
        ledger::claim_next(&db, "p").await.unwrap();
        for (leg, seq) in [("build", 1), ("publish", 2)] {
            ledger::add_leg(&db, "pub-2", "t", leg, seq, "live")
                .await
                .unwrap();
        }
        ledger::update_leg(
            &db,
            "pub-2",
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
        let done_flag = Arc::new(AtomicBool::new(false));
        let (tx, rx) = mpsc::unbounded_channel();
        let (done_tx, done_rx) = oneshot::channel();
        let recorder = tokio::spawn(record(
            db.clone(),
            LegLog {
                publication: "pub-2".into(),
                target: "t".into(),
                leg: "build".into(),
                path: dir.path().join("t.build.log"),
                split: Some(PhaseSplit {
                    next_leg: "publish",
                    log_ref: "cairn://p/cairn/publish/pub-2/t/build/log".into(),
                    done: done_flag.clone(),
                }),
            },
            crate::security::redaction_policy(dir.path()),
            |id: &str| id.to_string(),
            rx,
            done_rx,
        ));
        tx.send(BatchProgress::Request("req-9".into())).unwrap();
        let mut running = CheckPhaseStatus::default();
        running.phase = Some("running".into());
        running.executor = Some("cairn-publish".into());
        tx.send(BatchProgress::Phase(running)).unwrap();
        let build = format!(
            "{LEG_RESULT_MARKER}{}\n",
            serde_json::json!({"leg": "build", "assets": [{"name": "a", "sha256": "b".repeat(64), "size": 1}]})
        );
        let manifest = format!(
            "{LEG_RESULT_MARKER}{}\n",
            serde_json::json!({"leg": "manifest", "verifier": {"check": "manifest-shape", "result": "pass"}})
        );
        tx.send(BatchProgress::Output(build)).unwrap();
        let (head, tail) = manifest.split_at(manifest.len() / 2);
        tx.send(BatchProgress::Output(head.to_string())).unwrap();
        tx.send(BatchProgress::Output(tail.to_string())).unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !done_flag.load(Ordering::SeqCst) && std::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let legs = ledger::legs(&db, "pub-2").await.unwrap();
        let (build, publish) = (&legs[0], &legs[1]);
        assert_eq!(build.state, "succeeded");
        assert_eq!(build.assets.len(), 1);
        assert_eq!(build.builder.as_deref(), Some("cairn-publish"));
        assert_eq!(publish.state, "running");
        assert_eq!(publish.builder.as_deref(), Some("cairn-publish"));
        assert_eq!(publish.request_id.as_deref(), Some("req-9"));
        done_tx.send(()).unwrap();
        recorder.await.unwrap();
    }
}
