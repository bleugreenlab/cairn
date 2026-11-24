//! `run_batch_requests`: one durable row per run-batch request, from the
//! call's arrival to its one end.
//!
//! The row is what `cairn://p/<project>/requests/<id>` reads first
//! ([`crate::resources::requests`]). It is written by one task per request,
//! never on the call's path or under the fleet lock: the call hands writes to
//! the task through a channel, and the task applies them in order. The task
//! inserts the identity row when the call arrives, records what the batch
//! became once its items resolve, samples where it runs from the machine's
//! reports (the first look soon after receipt, then every
//! [`SAMPLE_EVERY`]), and writes the end once.
//!
//! A process that ends with the call still open writes no end. That absence
//! is itself the fact: the row's `incarnation` names the runner that received
//! it, and the reader says that runner ended with the call open.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use cairn_db::turso::params;
use tokio::sync::mpsc;

use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;

/// When the first look at where the batch runs is taken.
const FIRST_SAMPLE: Duration = Duration::from_secs(1);
/// How often the writer looks at the machine's latest report, in memory:
/// often enough that a command of a few seconds is seen at all.
const LOOK_EVERY: Duration = Duration::from_secs(5);
/// How often a running batch's peaks and progress are written.
const SAMPLE_EVERY: Duration = Duration::from_secs(30);

/// How a request's call ended. Exactly one is written per request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum End {
    /// The call carried the batch's result.
    Answered,
    /// The call parked, and the batch's result resumed its turn.
    Resumed,
    /// The call answered with a handle; `run_batch_detached` holds the rest.
    Detached,
    /// The batch never ran: placement or admission refused it.
    Refused,
    /// A principal, or Cairn so the call could answer in time, cancelled it.
    Cancelled,
    /// The call's future was dropped before it answered.
    Dropped,
}

impl End {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Answered => "answered",
            Self::Resumed => "resumed",
            Self::Detached => "detached",
            Self::Refused => "refused",
            Self::Cancelled => "cancelled",
            Self::Dropped => "dropped",
        }
    }
}

/// What a request became once its items resolved.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct Description {
    pub(crate) label: Option<String>,
    pub(crate) item_count: usize,
    pub(crate) sequential: bool,
    pub(crate) declared_timeout_ms: Option<i64>,
    pub(crate) verdict_branch: Option<String>,
    pub(crate) cwd: String,
    pub(crate) selector_json: Option<String>,
}

/// The bound a batch declares: the sum of its items' timeouts when they run
/// in order, their maximum when they run together, and none when any item
/// leaves its own unbounded.
pub(crate) fn declared_timeout_ms(timeouts: &[Option<u32>], sequential: bool) -> Option<i64> {
    let bounds: Option<Vec<i64>> = timeouts
        .iter()
        .map(|timeout| timeout.map(i64::from))
        .collect();
    let bounds = bounds.filter(|bounds| !bounds.is_empty())?;
    Some(if sequential {
        bounds.iter().sum()
    } else {
        bounds.iter().copied().max().unwrap_or(0)
    })
}

/// One look at where the request's command runs, from its machine's report.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct Execution {
    pub(crate) attempt_id: String,
    pub(crate) executor_id: String,
    pub(crate) machine: String,
    pub(crate) generation: Option<i64>,
    pub(crate) cell_id: Option<String>,
    pub(crate) pid: Option<i64>,
    pub(crate) started_ms: i64,
    /// Cores times 1000 over the latest measured window.
    pub(crate) cpu_milli: Option<i64>,
    pub(crate) resident_bytes: Option<i64>,
    /// When the machine last measured the command's tree.
    pub(crate) measured_ms: Option<i64>,
}

impl Execution {
    /// The request's command as `machine` reports it in its latest snapshot.
    pub(crate) fn of(
        machine: String,
        generation: u64,
        request: &cairn_common::executor_protocol::ExecutingCellRequest,
    ) -> Self {
        let usage = request.live_usage.as_ref();
        Self {
            attempt_id: request.attempt_id.clone(),
            executor_id: request.executor_id.clone(),
            machine,
            generation: i64::try_from(generation).ok(),
            cell_id: Some(request.cell_id.clone()).filter(|cell| !cell.is_empty()),
            pid: request.process_ids.first().map(|pid| i64::from(*pid)),
            started_ms: request.started_at_unix_ms as i64,
            cpu_milli: usage
                .filter(|usage| usage.window_ms > 0)
                .map(|usage| (usage.window_cpu_ms.saturating_mul(1000) / usage.window_ms) as i64),
            resident_bytes: usage
                .and_then(|usage| usage.resident_bytes)
                .map(|bytes| bytes as i64),
            measured_ms: usage.map(|usage| usage.measured_at_unix_ms as i64),
        }
    }
}

enum Write {
    Describe(Description),
    /// An item started: look at the machine's report now.
    Look,
    End {
        end: End,
        detail: Option<String>,
        output_bytes: Option<i64>,
        at_ms: i64,
    },
}

struct Receipt {
    request_id: String,
    tool_use_id: Option<String>,
    turn_id: Option<String>,
    commits: bool,
    received_ms: i64,
    incarnation: Option<String>,
}

/// A request's ledger handle, held by its batch for its whole life.
#[derive(Default)]
pub(crate) struct Ledger {
    tx: OnceLock<mpsc::UnboundedSender<Write>>,
    ended: AtomicBool,
    /// Why placement refused the batch, when it did; its end names it.
    refusal: Mutex<Option<String>>,
}

impl Ledger {
    /// Start recording a request. A call that belongs to no agent run has no
    /// project store to record into, and is not recorded.
    pub(crate) fn open(
        &self,
        orch: &Orchestrator,
        request: &McpCallbackRequest,
        request_id: &str,
        commits: bool,
        received_ms: i64,
    ) {
        let Some(run_id) = request.run_id.as_deref().filter(|id| !id.is_empty()) else {
            return;
        };
        let (tx, rx) = mpsc::unbounded_channel();
        if self.tx.set(tx).is_err() {
            return;
        }
        let receipt = Receipt {
            request_id: request_id.to_owned(),
            tool_use_id: request.tool_use_id.clone(),
            turn_id: orch.process_state.get_current_turn_id(run_id),
            commits,
            received_ms,
            incarnation: orch.fleet.runner_incarnation().map(str::to_owned),
        };
        cairn_common::task::spawn(write_ledger(orch.clone(), request.clone(), receipt, rx));
    }

    fn send(&self, write: Write) {
        if let Some(tx) = self.tx.get() {
            // A closed channel is a writer that could not find the run's
            // store; it already said so.
            let _ = tx.send(write);
        }
    }

    pub(crate) fn describe(&self, description: Description) {
        self.send(Write::Describe(description));
    }

    /// An item of the batch started on its machine.
    pub(crate) fn look(&self) {
        self.send(Write::Look);
    }

    pub(crate) fn note_refusal(&self, detail: &str) {
        let mut refusal = self.refusal.lock().unwrap_or_else(|p| p.into_inner());
        if refusal.is_none() {
            *refusal = Some(detail.trim().to_owned());
        }
    }

    pub(crate) fn refusal(&self) -> Option<String> {
        self.refusal
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    /// Record how the call ended. Only the first end counts.
    pub(crate) fn end(&self, end: End, detail: Option<String>, output_bytes: Option<usize>) {
        if self.ended.swap(true, Ordering::SeqCst) {
            return;
        }
        self.send(Write::End {
            end,
            detail,
            output_bytes: output_bytes.and_then(|bytes| i64::try_from(bytes).ok()),
            at_ms: chrono::Utc::now().timestamp_millis(),
        });
    }
}

async fn write_ledger(
    orch: Orchestrator,
    request: McpCallbackRequest,
    receipt: Receipt,
    mut rx: mpsc::UnboundedReceiver<Write>,
) {
    let db = match super::super::run_context::lookup_run_routed(&orch.db, &request).await {
        Ok((ctx, db)) => {
            if let Err(error) =
                insert(&db, &receipt, &ctx.job_id, &ctx.run_id, &ctx.project_id).await
            {
                log::warn!(
                    "run batch {} has no ledger row: {error}",
                    receipt.request_id
                );
                return;
            }
            db
        }
        Err(error) => {
            log::debug!(
                "run batch {} is not recorded in the request ledger: {error}",
                receipt.request_id
            );
            return;
        }
    };
    let id = receipt.request_id;
    let mut sampler = Sampler::default();
    let mut ticker =
        tokio::time::interval_at(tokio::time::Instant::now() + FIRST_SAMPLE, LOOK_EVERY);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // The writer lives as long as the batch's control does, not its call: a
    // detached or dropped call's batch keeps running, and its machine, peaks
    // and progress keep landing here after the call's end is written. The
    // channel closes when the batch's last holder lets go.
    loop {
        tokio::select! {
            write = rx.recv() => match write {
                None => {
                    sampler.flush(&db, &id).await;
                    return;
                }
                Some(Write::Describe(description)) => {
                    if let Err(error) = describe(&db, &id, &description).await {
                        log::warn!("run batch {id}: could not record what it became: {error}");
                    }
                }
                Some(Write::Look) => sampler.look(&orch, &db, &id).await,
                Some(Write::End { end: kind, detail, output_bytes, at_ms }) => {
                    sampler.look(&orch, &db, &id).await;
                    sampler.flush(&db, &id).await;
                    if let Err(error) = end(&db, &id, kind, detail.as_deref(), output_bytes, at_ms).await {
                        log::warn!("run batch {id}: could not record its end ({}): {error}", kind.as_str());
                    }
                }
            },
            _ = ticker.tick() => sampler.look(&orch, &db, &id).await,
        }
    }
}

/// What the writer has seen of where the command runs, held in memory
/// between writes: peaks rise with every look, and a write happens when the
/// command first appears or moves (a new attempt, cell or pid), or once
/// [`SAMPLE_EVERY`] has passed since the last.
#[derive(Default)]
struct Sampler {
    held: Option<Execution>,
    written: Option<(Execution, tokio::time::Instant)>,
}

impl Sampler {
    async fn look(&mut self, orch: &Orchestrator, db: &LocalDb, id: &str) {
        let Some(seen) = orch.fleet.executing_command(id) else {
            return;
        };
        let merged = match self.held.take() {
            Some(held) if held.attempt_id == seen.attempt_id => Execution {
                cpu_milli: held.cpu_milli.max(seen.cpu_milli),
                resident_bytes: held.resident_bytes.max(seen.resident_bytes),
                measured_ms: seen.measured_ms.or(held.measured_ms),
                ..seen
            },
            _ => seen,
        };
        let due = match &self.written {
            None => true,
            Some((written, at)) => {
                (&written.attempt_id, &written.cell_id, written.pid)
                    != (&merged.attempt_id, &merged.cell_id, merged.pid)
                    || at.elapsed() >= SAMPLE_EVERY
            }
        };
        self.held = Some(merged);
        if due {
            self.flush(db, id).await;
        }
    }

    async fn flush(&mut self, db: &LocalDb, id: &str) {
        let Some(held) = self.held.clone() else {
            return;
        };
        if self
            .written
            .as_ref()
            .is_some_and(|(written, _)| *written == held)
        {
            return;
        }
        match record_execution(db, id, &held).await {
            Ok(()) => self.written = Some((held, tokio::time::Instant::now())),
            Err(error) => log::warn!("run batch {id}: could not record where it runs: {error}"),
        }
    }
}

async fn insert(
    db: &LocalDb,
    receipt: &Receipt,
    job_id: &str,
    run_id: &str,
    project_id: &str,
) -> Result<(), String> {
    db.execute(
        "INSERT OR IGNORE INTO run_batch_requests \
         (request_id, tool_use_id, job_id, run_id, session_id, turn_id, project_id, commits, received_ms, incarnation) \
         VALUES (?1, ?2, ?3, ?4, (SELECT session_id FROM runs WHERE id=?4), ?5, ?6, ?7, ?8, ?9)",
        params![
            receipt.request_id.as_str(),
            receipt.tool_use_id.as_deref(),
            job_id,
            run_id,
            receipt.turn_id.as_deref(),
            project_id,
            receipt.commits as i64,
            receipt.received_ms,
            receipt.incarnation.as_deref(),
        ],
    )
    .await
    .map(drop)
    .map_err(|error| error.to_string())
}

async fn describe(db: &LocalDb, id: &str, description: &Description) -> Result<(), String> {
    db.execute(
        "UPDATE run_batch_requests SET label=?2, item_count=?3, sequential=?4, declared_timeout_ms=?5, \
         verdict_branch=?6, cwd=?7, selector_json=?8 WHERE request_id=?1",
        params![
            id,
            description.label.as_deref(),
            description.item_count as i64,
            description.sequential as i64,
            description.declared_timeout_ms,
            description.verdict_branch.as_deref(),
            description.cwd.as_str(),
            description.selector_json.as_deref(),
        ],
    )
    .await
    .map(drop)
    .map_err(|error| error.to_string())
}

/// Where the command runs now. Peaks only rise, and the first start stands.
async fn record_execution(db: &LocalDb, id: &str, execution: &Execution) -> Result<(), String> {
    db.execute(
        "UPDATE run_batch_requests SET attempt_id=?2, executor_id=?3, machine=?4, generation=?5, \
         cell_id=COALESCE(?6, cell_id), pid=COALESCE(?7, pid), started_ms=COALESCE(started_ms, ?8), \
         peak_cpu_milli=CASE WHEN ?9 IS NULL THEN peak_cpu_milli \
                             WHEN peak_cpu_milli IS NULL OR ?9 > peak_cpu_milli THEN ?9 \
                             ELSE peak_cpu_milli END, \
         peak_resident_bytes=CASE WHEN ?10 IS NULL THEN peak_resident_bytes \
                             WHEN peak_resident_bytes IS NULL OR ?10 > peak_resident_bytes THEN ?10 \
                             ELSE peak_resident_bytes END, \
         last_progress_ms=COALESCE(?11, last_progress_ms) \
         WHERE request_id=?1",
        params![
            id,
            execution.attempt_id.as_str(),
            execution.executor_id.as_str(),
            execution.machine.as_str(),
            execution.generation,
            execution.cell_id.as_deref(),
            execution.pid,
            execution.started_ms,
            execution.cpu_milli,
            execution.resident_bytes,
            execution.measured_ms,
        ],
    )
    .await
    .map(drop)
    .map_err(|error| error.to_string())
}

async fn end(
    db: &LocalDb,
    id: &str,
    end: End,
    detail: Option<&str>,
    output_bytes: Option<i64>,
    at_ms: i64,
) -> Result<(), String> {
    db.execute(
        "UPDATE run_batch_requests SET \"end\"=?2, end_detail=?3, output_bytes=COALESCE(?4, output_bytes), ended_ms=?5 \
         WHERE request_id=?1 AND \"end\" IS NULL",
        params![id, end.as_str(), detail, output_bytes, at_ms],
    )
    .await
    .map(drop)
    .map_err(|error| error.to_string())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// Seed a ledger row the way receipt writes it, for readers' tests.
    pub(crate) async fn seed(
        db: &LocalDb,
        request_id: &str,
        tool_use_id: &str,
        job_id: &str,
        run_id: &str,
        project_id: &str,
        received_ms: i64,
    ) {
        insert(
            db,
            &Receipt {
                request_id: request_id.into(),
                tool_use_id: Some(tool_use_id.into()),
                turn_id: Some("turn-1".into()),
                commits: false,
                received_ms,
                incarnation: Some("incarnation-1".into()),
            },
            job_id,
            run_id,
            project_id,
        )
        .await
        .unwrap();
    }

    pub(crate) async fn seed_description(
        db: &LocalDb,
        request_id: &str,
        description: &Description,
    ) {
        describe(db, request_id, description).await.unwrap();
    }

    pub(crate) async fn seed_execution(db: &LocalDb, request_id: &str, execution: &Execution) {
        record_execution(db, request_id, execution).await.unwrap();
    }

    pub(crate) async fn seed_end(
        db: &LocalDb,
        request_id: &str,
        kind: End,
        detail: Option<&str>,
        at_ms: i64,
    ) {
        end(db, request_id, kind, detail, Some(42), at_ms)
            .await
            .unwrap();
    }

    #[test]
    fn a_declared_timeout_sums_in_order_and_takes_the_longest_together() {
        assert_eq!(declared_timeout_ms(&[Some(10), Some(20)], true), Some(30));
        assert_eq!(declared_timeout_ms(&[Some(10), Some(20)], false), Some(20));
        assert_eq!(declared_timeout_ms(&[Some(10), None], true), None);
        assert_eq!(declared_timeout_ms(&[], true), None);
    }

    #[test]
    fn only_the_first_end_is_sent() {
        let ledger = Ledger::default();
        let (tx, mut rx) = mpsc::unbounded_channel();
        ledger.tx.set(tx).unwrap();
        ledger.end(End::Detached, None, None);
        ledger.end(End::Dropped, Some("running".into()), None);
        match rx.try_recv() {
            Ok(Write::End { end, .. }) => assert_eq!(end, End::Detached),
            _ => panic!("the first end is sent"),
        }
        assert!(rx.try_recv().is_err(), "a second end is never sent");
    }

    #[test]
    fn a_refusal_keeps_its_first_reason() {
        let ledger = Ledger::default();
        ledger.note_refusal(" no machine matches `os: plan9`. ");
        ledger.note_refusal("later");
        assert_eq!(
            ledger.refusal().as_deref(),
            Some("no machine matches `os: plan9`.")
        );
    }

    #[test]
    fn an_execution_reads_cores_as_milli_and_the_first_pid() {
        let request: cairn_common::executor_protocol::ExecutingCellRequest =
            serde_json::from_value(serde_json::json!({
                "executorId": "exec-1",
                "slotId": "cell-1",
                "requestId": "r",
                "attemptId": "a",
                "startedAtUnixMs": 1000,
                "processIds": [4242, 4243],
                "liveUsage": {
                    "windowCpuMs": 3000,
                    "windowMs": 2000,
                    "residentBytes": 1024,
                    "measuredAtUnixMs": 5000,
                    "quality": "sampled"
                }
            }))
            .unwrap();
        let execution = Execution::of("bginf-sp1".into(), 3, &request);
        assert_eq!(execution.cpu_milli, Some(1500));
        assert_eq!(execution.pid, Some(4242));
        assert_eq!(execution.cell_id.as_deref(), Some("cell-1"));
        assert_eq!(execution.resident_bytes, Some(1024));
        assert_eq!(execution.measured_ms, Some(5000));
        assert_eq!(execution.generation, Some(3));
    }
}
