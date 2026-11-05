//! The write-replay guard: what makes a second delivery of one `write` call a
//! no-op instead of a second application of its patch.
//!
//! # Why a write needs one
//!
//! A single [`handle_write`](super::handle_write) cannot double-apply. It takes
//! the store lock, resolves the logical head, computes whole-file replacement
//! content against that head, and publishes under an expected-head
//! compare-and-swap, all in one serialized writer epoch; a whole-file mutation
//! applied twice is idempotent.
//!
//! What is not idempotent is the *patch*, and nothing made a re-delivery of the
//! whole call a no-op. An anchor-preserving insertion, one that matches an
//! anchor and re-emits that anchor inside its `new_string`, leaves its own
//! `old_string` present in the text it wrote. Deliver it again and it matches
//! again, inserts a second copy, and reports success. The store lock cannot
//! help: it serializes writers but cannot tell two deliveries of one call from
//! two calls. Neither can the CAS: it is satisfied by whatever the first
//! delivery left. That is the CAIRN-3242 edit-echo, in which a builder's own
//! additions kept reappearing in duplicate and a load-bearing arm was then
//! deleted as a false duplicate.
//!
//! # The identity a delivery has
//!
//! The MCP `tools/call` transport carries no provider tool-use id, so `cairn-cmd`
//! sends `tool_use_id: None` for every tool; only the Cairn-native tool loop
//! populates it. The id has to be correlated from the current turn's transcript
//! instead, exactly as a suspending `run` batch, a `waitFor`, and a workflow
//! invocation already do. See [`tool_use_correlation`] for why that claim is
//! exclusive and refuses a tie rather than guessing.
//!
//! Correlating rather than hashing the payload is what makes this a guard on
//! *replay* and not on *content*. Two deliveries of one call resolve to one id
//! and collapse; two calls the agent meant to make are two ids and both apply,
//! even byte-identical ones, because the first is already answered by the time
//! the second is issued and an answered invocation is not a candidate.
//!
//! # What is recorded, and when
//!
//! A file-target batch records the moment it publishes a commit, inside the same
//! store-lock epoch. A resource-only batch has no publication lock, so it first
//! claims a pending row in the database, applies its effects only if it owns that
//! claim, and replaces the pending marker after the complete batch settles. A
//! concurrent redelivery waits for that settled report instead of racing the
//! claimant into the resource dispatcher. Failed resource batches are finalized
//! too: replay means returning the first delivery's truth, not silently retrying
//! a call whose earlier items may already have taken effect.
//!
//! The row is claimed inside the lock but its report is FINALIZED after the
//! publication ladder resolves. Publication's later rungs — the jj→git export
//! and the origin push — can fail after the commit has landed, which turns a
//! `committed` report into `sealed locally; unpublished`. Recording only the
//! in-lock report would durably replay a failed publication as a success, and
//! the row is deliberately immutable to competitors, so the correction has to
//! come from the delivery that owns it. [`record_report`] therefore reports
//! whether it actually claimed the row, and only that delivery may
//! [`finalize_report`].

use crate::mcp::handlers::tool_use_correlation::{claim_tool_use_id, Claim};
use crate::mcp::types::{ChangePayload, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};

#[cfg(feature = "test-utils")]
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex, OnceLock,
};

/// Deterministic integration-test gate at the durable-effect/result-delivery boundary.
#[cfg(feature = "test-utils")]
pub struct SettlementGate {
    entered: AtomicBool,
    released: AtomicBool,
    changed: tokio::sync::Notify,
}

#[cfg(feature = "test-utils")]
impl SettlementGate {
    fn new() -> Self {
        Self {
            entered: AtomicBool::new(false),
            released: AtomicBool::new(false),
            changed: tokio::sync::Notify::new(),
        }
    }

    pub async fn entered(&self) {
        while !self.entered.load(Ordering::Acquire) {
            self.changed.notified().await;
        }
    }

    pub fn release(&self) {
        self.released.store(true, Ordering::Release);
        self.changed.notify_waiters();
    }

    async fn hold(&self) {
        self.entered.store(true, Ordering::Release);
        self.changed.notify_waiters();
        while !self.released.load(Ordering::Acquire) {
            self.changed.notified().await;
        }
    }
}

/// Settlement gates keyed by `(run_id, tool_use_id)`.
#[cfg(feature = "test-utils")]
type SettlementGates = Mutex<std::collections::HashMap<(String, String), Arc<SettlementGate>>>;

#[cfg(feature = "test-utils")]
static SETTLEMENT_GATES: OnceLock<SettlementGates> = OnceLock::new();

/// Install a one-call gate used by process fixtures; production deliveries never register one.
#[cfg(feature = "test-utils")]
pub fn gate_settled_delivery(run_id: &str, tool_use_id: &str) -> Arc<SettlementGate> {
    let gate = Arc::new(SettlementGate::new());
    SETTLEMENT_GATES
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .insert(
            (run_id.to_string(), tool_use_id.to_string()),
            Arc::clone(&gate),
        );
    gate
}

#[cfg(feature = "test-utils")]
async fn hold_settled_delivery(call: &WriteCall) {
    let gate = SETTLEMENT_GATES
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .remove(&(call.run_id.clone(), call.id.clone()));
    if let Some(gate) = gate {
        gate.hold().await;
    }
}

#[cfg(not(feature = "test-utils"))]
async fn hold_settled_delivery(_call: &WriteCall) {}

/// The provider `write` call a delivery came from, or `None` when this delivery
/// has no identity to be deduplicated by.
///
/// `None` is a real answer rather than something to paper over. Inventing a key
/// would be strictly worse than having none: an invented key never collides, so
/// it buys no protection, and a key derived from something other than the call
/// (a payload hash, say) would collapse two edits the agent deliberately made.
/// A delivery with no identity applies exactly as it did before this guard
/// existed.
pub(super) async fn invoking_write_call(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
) -> Option<WriteCall> {
    let run_id = request.run_id.clone()?;
    if let Some(id) = request.tool_use_id.clone() {
        return Some(WriteCall { run_id, id });
    }

    let (ctx, db) = crate::mcp::handlers::run_context::lookup_run_routed(&orch.db, request)
        .await
        .ok()?;
    let turn_id = orch.process_state.get_current_turn_id(&ctx.run_id)?;
    let home_uri = crate::mcp::handlers::run_context::lookup_home_uri(&db, request)
        .await
        .ok()?;
    let expected = change_identity(&request.payload, &home_uri)?;
    match claim_tool_use_id(&db, &ctx.run_id, &turn_id, |name, input| {
        is_this_writes_call(name, input, &expected, &home_uri)
    })
    .await
    {
        Claim::One(id) => Some(WriteCall {
            run_id: ctx.run_id,
            id,
        }),
        // Both refusals cost this delivery its guard and nothing else: it applies
        // as it did before. They are logged rather than passed over in silence,
        // because a write that cannot name its own call is a write the echo can
        // still reach.
        Claim::None => {
            log::debug!(
                "write for run {} found no unanswered `write` call of its own in the transcript; it cannot be guarded against replay",
                ctx.run_id
            );
            None
        }
        Claim::Ambiguous(count) => {
            log::warn!(
                "write for run {} matches {count} indistinguishable open `write` calls, so it cannot claim one without risking another call's identity; it cannot be guarded against replay",
                ctx.run_id
            );
            None
        }
    }
}

const FILE_REPLAY_LEASE: std::time::Duration = std::time::Duration::from_secs(30);

/// The lease clock: wall-clock milliseconds, the unit `claimed_at` is stored in so
/// that deliveries in different processes can judge one another's leases.
fn wall_clock_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

pub(super) async fn await_file_replay_or_takeover(
    db: &LocalDb,
    call: &WriteCall,
) -> FileReplayClaim {
    await_file_replay_or_takeover_with_lease(db, call, FILE_REPLAY_LEASE, wall_clock_ms).await
}

/// `now_ms` reads the lease clock. It is an argument so that how much lease time
/// has passed is something a caller states rather than waits for: real time cannot
/// express "the owner renewed in time" on a host where one ledger write outlasts a
/// short lease. Polling still sleeps in real time; only the lease judgment reads
/// this clock.
async fn await_file_replay_or_takeover_with_lease(
    db: &LocalDb,
    call: &WriteCall,
    lease: std::time::Duration,
    now_ms: impl Fn() -> i64,
) -> FileReplayClaim {
    let poll_interval = std::cmp::max(
        std::time::Duration::from_millis(10),
        std::cmp::min(std::time::Duration::from_millis(50), lease / 4),
    );
    loop {
        let row = db.read(|conn| {
            let run_id = call.run_id.clone(); let tool_use_id = call.id.clone();
            Box::pin(async move {
                let mut rows = conn.query("SELECT report, finalized, owner_token, claimed_at FROM write_replay_ledger WHERE run_id=?1 AND tool_use_id=?2", cairn_db::turso::params![run_id, tool_use_id]).await?;
                Ok(match rows.next().await? { Some(row) => Some((row.text(0)?, row.i64(1)? != 0, row.opt_text(2)?, row.opt_i64(3)?)), None => None })
            })
        }).await.ok().flatten();
        let Some((report, finalized, old_owner, claimed_at)) = row else {
            return FileReplayClaim::Stalled;
        };
        if finalized {
            return FileReplayClaim::Replayed(report);
        }
        let (Some(old_owner), Some(claimed_at)) = (old_owner, claimed_at) else {
            return FileReplayClaim::Stalled;
        };
        let now = now_ms();
        let lease_millis = i64::try_from(lease.as_millis()).unwrap_or(i64::MAX);
        if now.saturating_sub(claimed_at) < lease_millis {
            tokio::time::sleep(poll_interval).await;
            continue;
        }
        let owner = ReplayOwner {
            token: uuid::Uuid::new_v4().to_string(),
        };
        match db.execute("UPDATE write_replay_ledger SET owner_token=?5, claimed_at=?6 WHERE run_id=?1 AND tool_use_id=?2 AND finalized=0 AND owner_token=?3 AND claimed_at=?4 AND claimed_at <= ?7",
            cairn_db::turso::params![call.run_id.clone(), call.id.clone(), old_owner, claimed_at, owner.token.clone(), now, now.saturating_sub(lease_millis)]).await {
            Ok(1) => return FileReplayClaim::TakenOver { owner, report },
            Ok(_) => continue,
            Err(error) => { log::warn!("failed to take over stalled write replay: {error}"); return FileReplayClaim::Stalled; }
        }
    }
}

pub(super) async fn finalize_owned_report(
    db: &LocalDb,
    call: &WriteCall,
    owner: &ReplayOwner,
    report: &str,
) -> bool {
    match db.execute("UPDATE write_replay_ledger SET report=?4, finalized=1, owner_token=NULL WHERE run_id=?1 AND tool_use_id=?2 AND finalized=0 AND owner_token=?3",
        cairn_db::turso::params![call.run_id.clone(), call.id.clone(), owner.token.clone(), report.to_string()]).await {
        Ok(1) => true,
        Ok(_) => false,
        Err(error) => { log::warn!("failed to finalize owned write-replay entry: {error}"); false }
    }
}

#[derive(Debug, Clone)]
pub(super) struct ReplayOwner {
    token: String,
}

pub(super) enum FileReplayClaim {
    Replayed(String),
    TakenOver { owner: ReplayOwner, report: String },
    Stalled,
}

/// The provider call one delivery of a `write` belongs to.
#[derive(Debug, Clone)]
pub(super) struct WriteCall {
    run_id: String,
    id: String,
}

impl WriteCall {
    pub(super) fn tool_use_id(&self) -> &str {
        &self.id
    }
}

/// Whether a recorded tool invocation is the `write` call this delivery came
/// from.
fn is_this_writes_call(
    name: &str,
    input: &serde_json::Value,
    expected: &serde_json::Value,
    home_uri: &str,
) -> bool {
    (name == "write" || name.ends_with("__write"))
        && change_identity(input, home_uri).is_some_and(|identity| &identity == expected)
}

/// A batch's identity for correlation: all its items and batch options,
/// normalized through the change schema.
///
/// Normalizing through the schema is what keeps two spellings of one batch equal
/// and stops a key the schema does not model from making them differ; comparing
/// raw JSON is what made no terminal exit wait correlate in CAIRN-3115.
///
/// Canonicalizing `cairn:~` against this run's current home URI preserves
/// correlation with older transcript deliveries that may carry a client-expanded
/// target, without weakening target identity: two otherwise identical calls aimed
/// at different resources remain distinct.
fn change_identity(input: &serde_json::Value, home_uri: &str) -> Option<serde_json::Value> {
    let mut payload = serde_json::from_value::<ChangePayload>(input.clone()).ok()?;
    for item in &mut payload.changes {
        if let Some(suffix) = item
            .target
            .strip_prefix("cairn:~/")
            .or_else(|| (item.target == "cairn:~").then_some(""))
        {
            item.target = if suffix.is_empty() {
                home_uri.to_string()
            } else {
                format!("{}/{}", home_uri.trim_end_matches('/'), suffix)
            };
        }
    }
    serde_json::to_value(payload).ok()
}

/// The report a previous delivery of this call already produced, if any.
pub(crate) const RESOURCE_PENDING: &str = "__cairn_write_resource_pending__";

/// Save the outer report before a blocking producer can suspend or complete.
/// Resource replay must still see a pending claim, not a settled partial write.
pub(super) async fn checkpoint_blocking_report(
    db: &LocalDb,
    call: &WriteCall,
    ordinary_claim: LedgerClaim,
    report: &str,
) -> Result<(), String> {
    let checkpoint = if ordinary_claim == LedgerClaim::Claimed {
        report.to_string()
    } else {
        format!("{RESOURCE_PENDING}\n{report}")
    };
    let updated = db.execute(
        "UPDATE write_replay_ledger SET report=?3 WHERE run_id=?1 AND tool_use_id=?2 AND (report=?4 OR ?5=1)",
        cairn_db::turso::params![
            call.run_id.clone(), call.id.clone(), checkpoint.clone(), RESOURCE_PENDING,
            i64::from(ordinary_claim == LedgerClaim::Claimed),
        ],
    ).await.map_err(|e| e.to_string())?;
    let saved = updated == 1
        // A batch whose file edits all failed has not inserted a file ledger
        // row, but may still have successful resource effects to preserve.
        || db.execute(
            "INSERT OR IGNORE INTO write_replay_ledger(run_id,tool_use_id,report,created_at) VALUES(?1,?2,?3,?4)",
            cairn_db::turso::params![
                call.run_id.clone(), call.id.clone(), checkpoint,
                chrono::Utc::now().timestamp_millis(),
            ],
        ).await.map_err(|e| e.to_string())? == 1;
    if saved {
        Ok(())
    } else {
        Err("Could not checkpoint the blocking write report".to_string())
    }
}

/// Replace the still-pending blocking checkpoint with a report that now lists
/// what the blocking group created.
///
/// [`checkpoint_blocking_report`] runs before the group, so its report cannot
/// name the tasks the group goes on to create. This rewrites it without
/// settling the call: a resource claim keeps its pending marker, so a
/// redelivery still waits for the settled report rather than replaying a
/// partial one.
pub(super) async fn update_blocking_checkpoint(
    db: &LocalDb,
    call: &WriteCall,
    ordinary_claim: LedgerClaim,
    report: &str,
) -> Result<(), String> {
    let saved = if ordinary_claim == LedgerClaim::Claimed {
        db.execute(
            "UPDATE write_replay_ledger SET report=?3 WHERE run_id=?1 AND tool_use_id=?2",
            cairn_db::turso::params![call.run_id.clone(), call.id.clone(), report.to_string()],
        )
        .await
        .map_err(|e| e.to_string())?
            == 1
    } else {
        db.execute(
            "UPDATE write_replay_ledger SET report=?3
             WHERE run_id=?1 AND tool_use_id=?2 AND substr(report, 1, length(?4)) = ?4",
            cairn_db::turso::params![
                call.run_id.clone(),
                call.id.clone(),
                format!("{RESOURCE_PENDING}\n{report}"),
                RESOURCE_PENDING,
            ],
        )
        .await
        .map_err(|e| e.to_string())?
            == 1
    };
    if saved {
        Ok(())
    } else {
        Err("Could not update the blocking write checkpoint with the tasks it created".to_string())
    }
}

/// A delayed task or answer is payload, never another host report. The replay
/// ledger lives in the local database even when the transcript is team-owned.
pub(crate) async fn complete_blocking_report(
    db: &LocalDb,
    run_id: &str,
    tool_use_id: &str,
    blocking: crate::resources::mutations::BlockingResult,
) -> Result<String, String> {
    let original = db
        .query_opt_text(
            "SELECT report FROM write_replay_ledger WHERE run_id=?1 AND tool_use_id=?2",
            cairn_db::turso::params![run_id, tool_use_id],
        )
        .await
        .map_err(|e| e.to_string())?;
    replace_blocking_payload(original.as_deref(), blocking)
}

/// The checkpoint contains completed nonblocking effects even while the group
/// is pending. Stop-time settlement may report those effects without the marker.
pub(crate) fn blocking_checkpoint_report(report: &str) -> Option<&str> {
    let report = report
        .strip_prefix(RESOURCE_PENDING)
        .unwrap_or(report)
        .trim_start();
    (!report.is_empty()).then_some(report)
}

fn replace_blocking_payload(
    original: Option<&str>,
    blocking: crate::resources::mutations::BlockingResult,
) -> Result<String, String> {
    let original = original.and_then(blocking_checkpoint_report);
    let (mut report, suffix) = match original {
        Some(original) => {
            let mut stream =
                serde_json::Deserializer::from_str(original).into_iter::<serde_json::Value>();
            let report = stream
                .next()
                .ok_or("Missing blocking write report")?
                .map_err(|e| format!("Invalid blocking write report: {e}"))?;
            if !report
                .get("applied")
                .is_some_and(serde_json::Value::is_array)
            {
                return Err("Blocking write report has no applied array".to_string());
            }
            (report, &original[stream.byte_offset()..])
        }
        None => (
            serde_json::to_value(super::empty_change_report(
                Vec::new(),
                Vec::new(),
                None,
                false,
                false,
            ))
            .map_err(|e| e.to_string())?,
            "",
        ),
    };
    report["blocking"] = serde_json::to_value(blocking).map_err(|e| e.to_string())?;
    Ok(format!(
        "{}{suffix}",
        serde_json::to_string(&report).map_err(|e| e.to_string())?
    ))
}

pub(super) enum ResourceClaim {
    Claimed,
    Replayed(String),
    Stalled,
    Unguarded,
}

/// Claim a resource-only call before any resource dispatcher can observe it.
/// The unique ledger key is the serialization point shared by concurrent
/// deliveries; only the inserter may apply effects.
pub(super) async fn claim_resource_call(db: &LocalDb, call: &WriteCall) -> ResourceClaim {
    claim_resource_call_with_timeout(db, call, std::time::Duration::from_secs(30)).await
}

async fn claim_resource_call_with_timeout(
    db: &LocalDb,
    call: &WriteCall,
    timeout: std::time::Duration,
) -> ResourceClaim {
    let claim = record_report(db, call, RESOURCE_PENDING).await;
    if claim == LedgerClaim::Claimed {
        return ResourceClaim::Claimed;
    }

    // A competing delivery may still be applying a blocking or externally
    // side-effecting mutation. Wait for its post-effect report rather than
    // interpreting the pending marker as success or entering the dispatcher.
    // Bound the wait because a cancelled claimant can leave a pending row with
    // no process capable of finalizing it. A stalled replay fails closed instead
    // of hanging forever or risking a duplicate side effect.
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        match recorded_report(db, call).await {
            Some(report) if !report.starts_with(RESOURCE_PENDING) => {
                return ResourceClaim::Replayed(report);
            }
            Some(_) => {
                if tokio::time::Instant::now() >= deadline {
                    return ResourceClaim::Stalled;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            None => return ResourceClaim::Unguarded,
        }
    }
}

pub(super) async fn recorded_report(db: &LocalDb, call: &WriteCall) -> Option<String> {
    let (run_id, tool_use_id) = (call.run_id.clone(), call.id.clone());
    db.read(|conn| {
        let (run_id, tool_use_id) = (run_id.clone(), tool_use_id.clone());
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT report FROM write_replay_ledger
                     WHERE run_id = ?1 AND tool_use_id = ?2",
                    cairn_db::turso::params![run_id, tool_use_id],
                )
                .await?;
            match rows.next().await? {
                Some(row) => Ok(Some(row.text(0)?)),
                None => Ok(None),
            }
        })
    })
    .await
    .unwrap_or_default()
}

/// Whether this delivery claimed the ledger row, and may therefore correct the
/// report it recorded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LedgerClaim {
    /// This delivery inserted the row. Its own final report is the answer every
    /// later delivery of this call will be given.
    Claimed,
    /// A row already existed, or the ledger could not be written. Either way this
    /// delivery must not touch the stored report — in the first case it belongs
    /// to a delivery that may already have been answered with it.
    NotClaimed,
}

/// Record that this call has published file changes, with the report it
/// produced so far. Called inside the store-lock epoch that published them.
///
/// `INSERT OR IGNORE` because the first record wins: if two deliveries somehow
/// both reach publication, the report the agent may already have acted on is the
/// one to keep. The affected-row count is what distinguishes the two cases, and
/// it is the only thing that licenses a later [`finalize_report`].
pub(super) async fn record_report(db: &LocalDb, call: &WriteCall, report: &str) -> LedgerClaim {
    let (run_id, tool_use_id, report) = (call.run_id.clone(), call.id.clone(), report.to_string());
    let now = chrono::Utc::now().timestamp_millis();
    match db
        .execute(
            "INSERT OR IGNORE INTO write_replay_ledger(run_id, tool_use_id, report, created_at)
             VALUES (?1, ?2, ?3, ?4)",
            cairn_db::turso::params![run_id, tool_use_id, report.clone(), now],
        )
        .await
    {
        Ok(1) => {
            if report != RESOURCE_PENDING {
                hold_settled_delivery(call).await;
            }
            LedgerClaim::Claimed
        }
        Ok(_) => LedgerClaim::NotClaimed,
        Err(error) => {
            // A ledger that failed to record costs a future replay its guard; it
            // must never cost this write the commit it just published.
            log::warn!("failed to record write-replay ledger entry: {error}");
            LedgerClaim::NotClaimed
        }
    }
}

/// Replace the report of a row this delivery claimed, once the publication
/// ladder has settled what actually happened.
///
/// Only the claimant calls this, so the "first record wins" rule between
/// competing deliveries is untouched: this corrects one delivery's own report,
/// it does not overwrite another's. Without it a commit whose export or required
/// origin push failed would be replayed forever as `committed`, telling a later
/// delivery that a head nobody can see is published — the exact inversion the
/// fail-closed contract exists to prevent.
pub(super) async fn finalize_report(db: &LocalDb, call: &WriteCall, report: &str) {
    let (run_id, tool_use_id, report) = (call.run_id.clone(), call.id.clone(), report.to_string());
    if let Err(error) = db
        .execute(
            "UPDATE write_replay_ledger SET report = ?3
             WHERE run_id = ?1 AND tool_use_id = ?2",
            cairn_db::turso::params![run_id, tool_use_id, report],
        )
        .await
    {
        log::warn!("failed to finalize write-replay ledger entry: {error}");
    } else {
        hold_settled_delivery(call).await;
    }
}

/// The recorded report, re-marked as the replay it is.
///
/// The agent is told rather than quietly handed an old answer: it asked for a
/// batch to be applied and this call applied nothing, and the difference matters
/// if it is reasoning about what its own edits did. A report that cannot be
/// re-marked is still returned verbatim, since suppressing the double-apply is
/// the load-bearing half.
pub(super) fn mark_as_replay(report: &str) -> String {
    let Ok(mut value) = serde_json::from_str::<serde_json::Value>(report) else {
        return report.to_string();
    };
    let Some(object) = value.as_object_mut() else {
        return report.to_string();
    };
    object.insert(
        "replayed".to_string(),
        serde_json::Value::String(
            "This write was delivered more than once. The report below is the one the first \
             delivery produced; nothing was applied a second time."
                .to_string(),
        ),
    );
    serde_json::to_string(&value).unwrap_or_else(|_| report.to_string())
}

// ---------------------------------------------------------------------------
// A stopped resource write
// ---------------------------------------------------------------------------
//
// A resource-only write applies its items one at a time, and each item's
// effect is real the moment its dispatcher returns. When the turn is stopped
// partway, the report the agent gets (from the stop, or from a redelivery of
// the same call) has to say exactly which items took effect. Three pieces do
// that:
//
// - The carrier checkpoints the ledger row after every item, still behind the
//   pending marker, so a redelivery keeps waiting rather than replaying a
//   partial report.
// - An operator Stop raises the call's flag in [`WriteStops`]; the carrier
//   checks it before each dispatch, so a write stops between items, never in
//   the middle of one, and seals its row.
// - A carrier future dropped before it finalizes (the transport aborting the
//   call) seals the row from what it last knew ([`ResourceWriteProgress`]'s
//   `Drop`).
//
// A sealed row is final, so a redelivery replays it and applies nothing.

/// The failure kind of an item a stopped write did not apply, or cannot
/// vouch for.
pub(crate) const STOPPED_KIND: &str = "stopped";
const NOT_APPLIED: &str = "not applied: the write was stopped before this item";
const NOT_CONFIRMED: &str =
    "not confirmed: the write was stopped while this item ran; read its target before retrying";

/// The stop flags of the resource writes now applying items, by
/// `(run_id, tool_use_id)`.
#[derive(Default)]
pub struct WriteStops {
    flags: std::sync::Mutex<
        std::collections::HashMap<(String, String), std::sync::Arc<std::sync::atomic::AtomicBool>>,
    >,
}

impl WriteStops {
    fn flags(
        &self,
    ) -> std::sync::MutexGuard<
        '_,
        std::collections::HashMap<(String, String), std::sync::Arc<std::sync::atomic::AtomicBool>>,
    > {
        self.flags
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn register(&self, call: &WriteCall) -> std::sync::Arc<std::sync::atomic::AtomicBool> {
        let flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        self.flags()
            .insert((call.run_id.clone(), call.id.clone()), flag.clone());
        flag
    }

    fn unregister(&self, call: &WriteCall) {
        self.flags().remove(&(call.run_id.clone(), call.id.clone()));
    }

    /// Ask every resource write of `run_id` to stop before its next item.
    /// Returns how many were asked.
    pub(crate) fn raise(&self, run_id: &str) -> usize {
        let flags = self.flags();
        let mut raised = 0;
        for ((run, _), flag) in flags.iter() {
            if run == run_id {
                flag.store(true, std::sync::atomic::Ordering::Release);
                raised += 1;
            }
        }
        raised
    }

    /// Whether a stop has asked this call to stop.
    pub fn is_raised(&self, run_id: &str, tool_use_id: &str) -> bool {
        self.flags()
            .get(&(run_id.to_string(), tool_use_id.to_string()))
            .is_some_and(|flag| flag.load(std::sync::atomic::Ordering::Acquire))
    }

    /// Whether this call is applying items right now.
    pub(crate) fn is_applying(&self, run_id: &str, tool_use_id: &str) -> bool {
        self.flags()
            .contains_key(&(run_id.to_string(), tool_use_id.to_string()))
    }
}

/// What a claimed resource write has done so far, kept in memory by its
/// carrier and checkpointed into its ledger row after every item.
pub(super) struct ResourceWriteProgress {
    db: std::sync::Arc<LocalDb>,
    call: WriteCall,
    /// Every payload item's `(target, mode)`, for the items a stop leaves.
    items: Vec<(String, String)>,
    atomic: bool,
    stops: std::sync::Arc<WriteStops>,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    applied: Vec<super::types::AppliedChange>,
    failures: Vec<super::types::ChangeFailure>,
    in_flight: Option<usize>,
    /// Whether dropping this seals the row. Cleared once the carrier
    /// finalizes the row itself or hands it to the blocking group.
    armed: bool,
}

impl ResourceWriteProgress {
    /// Begin tracking a call whose resource claim this delivery holds.
    pub(super) fn claimed(
        orch: &Orchestrator,
        call: &WriteCall,
        payload: &ChangePayload,
        atomic: bool,
    ) -> Self {
        let stop = orch.write_stops.register(call);
        Self {
            db: orch.db.local.clone(),
            call: call.clone(),
            items: payload
                .changes
                .iter()
                .map(|item| {
                    (
                        item.target.clone(),
                        super::types::mode_name(item.mode).to_string(),
                    )
                })
                .collect(),
            atomic,
            stops: orch.write_stops.clone(),
            stop,
            applied: Vec::new(),
            failures: Vec::new(),
            in_flight: None,
            armed: true,
        }
    }

    /// Whether an operator Stop has asked this write to stop.
    pub(super) fn stop_requested(&self) -> bool {
        self.stop.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Item `index` is about to dispatch.
    pub(super) fn begin(
        &mut self,
        index: usize,
        applied: &[super::types::AppliedChange],
        failures: &[super::types::ChangeFailure],
    ) {
        self.applied = applied.to_vec();
        self.failures = failures.to_vec();
        self.in_flight = Some(index);
    }

    /// The item in flight has settled into `applied` or `failures`: record
    /// it, and checkpoint the row behind its pending marker.
    pub(super) async fn settled(
        &mut self,
        applied: &[super::types::AppliedChange],
        failures: &[super::types::ChangeFailure],
    ) {
        self.applied = applied.to_vec();
        self.failures = failures.to_vec();
        self.in_flight = None;
        let partial = self.report(false);
        if let Err(error) =
            update_blocking_checkpoint(&self.db, &self.call, LedgerClaim::NotClaimed, &partial)
                .await
        {
            log::warn!("could not checkpoint a resource write's progress: {error}");
        }
    }

    /// The report as it stands; `stopped` adds a failure for every item not
    /// yet settled.
    fn report(&self, stopped: bool) -> String {
        let partial_success = !self.applied.is_empty() && !self.failures.is_empty();
        let report = super::empty_change_report(
            self.applied.clone(),
            self.failures.clone(),
            None,
            partial_success,
            self.atomic,
        );
        let Ok(mut value) = serde_json::to_value(&report) else {
            return String::new();
        };
        if stopped {
            mark_unsettled(&mut value, &self.items, |index| {
                if Some(index) == self.in_flight {
                    NOT_CONFIRMED.to_string()
                } else {
                    NOT_APPLIED.to_string()
                }
            });
        }
        serde_json::to_string(&value).unwrap_or_default()
    }

    /// Stop before the next item: seal the row with every unsettled item
    /// `not applied`, and return the report.
    pub(super) async fn seal_stopped(
        &mut self,
        applied: &[super::types::AppliedChange],
        failures: &[super::types::ChangeFailure],
    ) -> String {
        self.applied = applied.to_vec();
        self.failures = failures.to_vec();
        self.in_flight = None;
        self.armed = false;
        let report = self.report(true);
        finalize_report(&self.db, &self.call, &report).await;
        report
    }

    /// The carrier finalizes the row itself from here on.
    pub(super) fn disarm(&mut self) {
        self.armed = false;
        self.stops.unregister(&self.call);
    }
}

impl Drop for ResourceWriteProgress {
    fn drop(&mut self) {
        self.stops.unregister(&self.call);
        if !self.armed {
            return;
        }
        // Dropped before it finalized: seal the row from what this delivery
        // last knew, so a redelivery replays it instead of stalling. Only a
        // row still pending is sealed; one the carrier finalized is final.
        let report = self.report(true);
        let (db, call) = (self.db.clone(), self.call.clone());
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                runtime.spawn(async move {
                    seal_if_pending(&db, &call.run_id, &call.id, &report).await
                });
            }
            Err(_) => log::warn!(
                "a resource write was dropped outside a runtime; its replay row stays pending"
            ),
        }
    }
}

/// Replace a still-pending row's marker with `report`, so a redelivery replays
/// it instead of stalling. A row already settled is final and is left alone.
pub(crate) async fn seal_if_pending(db: &LocalDb, run_id: &str, tool_use_id: &str, report: &str) {
    let sealed = db
        .execute(
            "UPDATE write_replay_ledger SET report=?3
             WHERE run_id=?1 AND tool_use_id=?2 AND substr(report, 1, length(?4)) = ?4",
            cairn_db::turso::params![
                run_id.to_string(),
                tool_use_id.to_string(),
                report.to_string(),
                RESOURCE_PENDING,
            ],
        )
        .await;
    if let Err(error) = sealed {
        log::warn!("could not seal a resource write's replay row: {error}");
    }
}

/// Add a `stopped` failure, worded by `phrase`, for every item of `items` the
/// report neither applied nor failed.
fn mark_unsettled(
    report: &mut serde_json::Value,
    items: &[(String, String)],
    phrase: impl Fn(usize) -> String,
) {
    let settled: std::collections::HashSet<u64> = ["applied", "failures"]
        .iter()
        .filter_map(|key| report.get(*key).and_then(serde_json::Value::as_array))
        .flatten()
        .filter_map(|entry| entry.get("index").and_then(serde_json::Value::as_u64))
        .collect();
    let Some(object) = report.as_object_mut() else {
        return;
    };
    let mut added = Vec::new();
    for (index, (target, mode)) in items.iter().enumerate() {
        if settled.contains(&(index as u64)) {
            continue;
        }
        added.push(serde_json::json!({
            "index": index,
            "target": target,
            "mode": mode,
            "kind": STOPPED_KIND,
            "error": phrase(index),
        }));
    }
    if added.is_empty() {
        return;
    }
    let failures = object
        .entry("failures")
        .or_insert_with(|| serde_json::Value::Array(Vec::new()));
    if let Some(failures) = failures.as_array_mut() {
        failures.extend(added);
    }
    let applied = object
        .get("applied")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|applied| !applied.is_empty());
    if applied {
        object.insert("partial_success".to_string(), serde_json::Value::Bool(true));
    }
}

/// A stop's report for a resource write whose row is still pending: what its
/// last checkpoint lists, and every other item of `input` (the call's
/// `{changes:[...]}`) `not confirmed`. Never `not applied`: the carrier may
/// still be running, and must not be contradicted.
pub(crate) fn unsettled_write_report(row: &str, input: &serde_json::Value) -> String {
    unsettled_report(row, input, |_| NOT_CONFIRMED.to_string())
}

/// A restart's report for a resource write whose row is still pending: what its
/// last checkpoint lists, and every other item of `input` `not confirmed`, each
/// naming what to read before retrying it. Unlike a stop, no carrier is left to
/// contradict, but the restart gives no way to tell the item that was running
/// from those after it, so none is called `not applied`.
pub(crate) fn restart_write_report(row: &str, input: &serde_json::Value) -> String {
    let changes = input
        .get("changes")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    unsettled_report(row, input, |index| {
        changes
            .get(index)
            .map(restart_item_phrase)
            .unwrap_or_else(|| NOT_CONFIRMED.to_string())
    })
}

fn restart_item_phrase(change: &serde_json::Value) -> String {
    let target = change
        .get("target")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    if !super::recorded_target_is_rebase(target) {
        return format!(
            "not confirmed: Cairn restarted while this item ran; read `{target}` before retrying it"
        );
    }
    let request = match change
        .pointer("/payload/action")
        .and_then(serde_json::Value::as_str)
    {
        Some("replay") => "the replay",
        Some("fast-forward-from-origin") => "the fast-forward",
        _ => "it",
    };
    format!("not confirmed: re-read `{target}` before requesting {request} again")
}

fn unsettled_report(
    row: &str,
    input: &serde_json::Value,
    phrase: impl Fn(usize) -> String,
) -> String {
    let mut report = blocking_checkpoint_report(row)
        .and_then(|body| {
            serde_json::Deserializer::from_str(body)
                .into_iter::<serde_json::Value>()
                .next()
                .and_then(Result::ok)
        })
        .filter(serde_json::Value::is_object)
        .unwrap_or_else(|| serde_json::json!({ "applied": [] }));
    let items: Vec<(String, String)> = input
        .get("changes")
        .and_then(serde_json::Value::as_array)
        .map(|changes| {
            changes
                .iter()
                .map(|change| {
                    let field = |key: &str| {
                        change
                            .get(key)
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or_default()
                            .to_string()
                    };
                    (field("target"), field("mode"))
                })
                .collect()
        })
        .unwrap_or_default();
    mark_unsettled(&mut report, &items, phrase);
    serde_json::to_string(&report).unwrap_or_else(|_| row.to_string())
}

/// Whether `report` lists an item a stop left unapplied or unconfirmed.
pub(crate) fn report_was_stopped(report: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(report)
        .ok()
        .and_then(|value| value.get("failures").cloned())
        .and_then(|failures| failures.as_array().cloned())
        .is_some_and(|failures| {
            failures.iter().any(|failure| {
                failure.get("kind").and_then(serde_json::Value::as_str) == Some(STOPPED_KIND)
            })
        })
}

/// Whether a ledger row is still behind the pending marker.
pub(crate) fn is_pending(row: &str) -> bool {
    row.starts_with(RESOURCE_PENDING)
}

/// Where an item gate parks a resource write.
#[cfg(feature = "test-utils")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ItemPhase {
    /// Before the stop flag is checked and the item is marked in flight.
    BeforeDispatch,
    /// After the item's dispatcher returned, before it is recorded.
    AfterDispatch,
}

#[cfg(feature = "test-utils")]
type ItemGates =
    Mutex<std::collections::HashMap<(String, String, usize, ItemPhase), Arc<SettlementGate>>>;

#[cfg(feature = "test-utils")]
static ITEM_GATES: OnceLock<ItemGates> = OnceLock::new();

/// Park a resource write at one item's `phase`; production never registers
/// one.
#[cfg(feature = "test-utils")]
pub fn gate_resource_item(
    run_id: &str,
    tool_use_id: &str,
    index: usize,
    phase: ItemPhase,
) -> Arc<SettlementGate> {
    let gate = Arc::new(SettlementGate::new());
    ITEM_GATES
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .insert(
            (run_id.to_string(), tool_use_id.to_string(), index, phase),
            Arc::clone(&gate),
        );
    gate
}

#[cfg(feature = "test-utils")]
pub(super) async fn hold_resource_item(call: &WriteCall, index: usize, phase: ItemPhase) {
    let gate = ITEM_GATES
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .remove(&(call.run_id.clone(), call.id.clone(), index, phase));
    if let Some(gate) = gate {
        gate.hold().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn file_edit() -> serde_json::Value {
        json!({
            "target": "file:src/lib.rs",
            "mode": "patch",
            "payload": { "old_string": "anchor", "new_string": "inserted\nanchor" }
        })
    }

    const HOME_URI: &str = "cairn://p/cairn/3264/1/builder";

    /// The transcript and delivery use different spellings for home-relative
    /// targets. Canonicalization must make them one identity without discarding
    /// the resource item from the batch.
    #[test]
    fn a_resolved_resource_target_does_not_change_a_batchs_identity() {
        let as_the_model_wrote_it = json!({
            "changes": [file_edit(), {
                "target": "cairn:~/todos",
                "mode": "patch",
                "payload": { "updates": [] }
            }],
            "commit_msg": "insert before the anchor"
        });
        let as_the_host_received_it = json!({
            "changes": [file_edit(), {
                "target": "cairn://p/cairn/3264/1/builder/todos",
                "mode": "patch",
                "payload": { "updates": [] }
            }],
            "commit_msg": "insert before the anchor"
        });
        assert_eq!(
            change_identity(&as_the_model_wrote_it, HOME_URI),
            change_identity(&as_the_host_received_it, HOME_URI)
        );
        assert!(change_identity(&as_the_model_wrote_it, HOME_URI).is_some());
    }

    /// Narrowing to the file items must not narrow to nothing. A batch that edits
    /// different files, or commits under a different message, is a different call.
    #[test]
    fn batches_that_edit_differently_have_different_identities() {
        let base = json!({ "changes": [file_edit()], "commit_msg": "one" });
        let other_file = json!({
            "changes": [{
                "target": "file:src/other.rs",
                "mode": "patch",
                "payload": { "old_string": "anchor", "new_string": "inserted\nanchor" }
            }],
            "commit_msg": "one"
        });
        let other_message = json!({ "changes": [file_edit()], "commit_msg": "two" });

        assert_ne!(
            change_identity(&base, HOME_URI),
            change_identity(&other_file, HOME_URI)
        );
        assert_ne!(
            change_identity(&base, HOME_URI),
            change_identity(&other_message, HOME_URI)
        );
    }

    /// Resource-only batches participate in correlation, and their target and
    /// payload keep otherwise similar calls distinct.
    #[test]
    fn resource_only_batches_have_distinct_identities() {
        let payload = json!({
            "changes": [{
                "target": "cairn://p/cairn/3264",
                "mode": "append",
                "payload": { "content": "a comment" }
            }]
        });
        let other = json!({
            "changes": [{
                "target": "cairn://p/cairn/3264",
                "mode": "append",
                "payload": { "content": "a different comment" }
            }]
        });
        assert!(change_identity(&payload, HOME_URI).is_some());
        assert_ne!(
            change_identity(&payload, HOME_URI),
            change_identity(&other, HOME_URI)
        );
    }

    /// Only a `write` invocation can be this delivery's call, and only one whose
    /// file items match. The namespaced spelling is what an MCP-hosted agent
    /// actually records.
    #[test]
    fn only_a_matching_write_invocation_is_this_deliverys_call() {
        let payload = json!({ "changes": [file_edit()], "commit_msg": "one" });
        let expected = change_identity(&payload, HOME_URI).unwrap();

        assert!(is_this_writes_call("write", &payload, &expected, HOME_URI));
        assert!(is_this_writes_call(
            "mcp__cairn__write",
            &payload,
            &expected,
            HOME_URI
        ));
        assert!(!is_this_writes_call("run", &payload, &expected, HOME_URI));
        assert!(!is_this_writes_call(
            "write",
            &json!({ "changes": [file_edit()], "commit_msg": "two" }),
            &expected,
            HOME_URI
        ));
    }

    /// A replay returns the first delivery's report, but says so: the agent asked
    /// for a batch to be applied and this call applied nothing.
    #[test]
    fn a_replayed_report_is_marked_and_otherwise_unchanged() {
        let recorded = json!({
            "applied": [{ "index": 0, "target": "file:src/lib.rs", "mode": "patch",
                          "kind": "file", "summary": "~file:src/lib.rs" }],
            "commit": { "status": "committed", "sha": "abc123", "pr_number": null, "message": null }
        })
        .to_string();

        let marked: serde_json::Value = serde_json::from_str(&mark_as_replay(&recorded)).unwrap();
        assert!(marked["replayed"].is_string());
        assert_eq!(marked["commit"]["sha"], "abc123");
        assert_eq!(marked["applied"].as_array().unwrap().len(), 1);
    }

    /// Suppressing the double-apply is the load-bearing half. A report that
    /// cannot be re-marked is still returned rather than dropped.
    #[test]
    fn an_unmarkable_report_is_returned_verbatim() {
        assert_eq!(mark_as_replay("not json"), "not json");
        assert_eq!(mark_as_replay("[1,2,3]"), "[1,2,3]");
    }

    /// A ledger row references a real run, so the fixture seeds one. Without it
    /// the insert fails its foreign key and `record_report` reports `NotClaimed`
    /// — which is the correct behavior, and would make these tests vacuous.
    async fn ledger_db(name: &str) -> (LocalDb, WriteCall) {
        let db = crate::storage::migrated_test_db(name).await;
        db.execute(
            "INSERT INTO runs(id, status, created_at, updated_at) VALUES ('run-1', 'running', 1, 1)",
            (),
        )
        .await
        .unwrap();
        (
            db,
            WriteCall {
                run_id: "run-1".to_string(),
                id: "toolu-1".to_string(),
            },
        )
    }

    fn report(status: &str) -> String {
        json!({
            "applied": [],
            "commit": { "status": status, "sha": "abc123", "pr_number": null, "message": null }
        })
        .to_string()
    }

    fn status_of(report: &str) -> String {
        serde_json::from_str::<serde_json::Value>(report).unwrap()["commit"]["status"]
            .as_str()
            .unwrap()
            .to_string()
    }

    /// A lease clock the test moves by hand, so lease time passes only when the
    /// test says it does.
    #[derive(Clone)]
    struct LeaseClock(std::sync::Arc<std::sync::atomic::AtomicI64>);

    impl LeaseClock {
        fn at(ms: i64) -> Self {
            Self(std::sync::Arc::new(std::sync::atomic::AtomicI64::new(ms)))
        }

        fn now(&self) -> i64 {
            self.0.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    /// A pending row claimed at `claimed_at`: the shape an interrupted delivery
    /// leaves for takeover to settle.
    async fn pending_report(
        db: &LocalDb,
        call: &WriteCall,
        report: &str,
        claimed_at: i64,
    ) -> ReplayOwner {
        let owner = ReplayOwner {
            token: uuid::Uuid::new_v4().to_string(),
        };
        db.execute(
            "INSERT INTO write_replay_ledger(run_id, tool_use_id, report, created_at, owner_token, finalized, claimed_at) VALUES (?1, ?2, ?3, ?4, ?5, 0, ?4)",
            cairn_db::turso::params![call.run_id.clone(), call.id.clone(), report.to_string(), claimed_at, owner.token.clone()],
        )
        .await
        .unwrap();
        owner
    }

    /// The publication ladder settles AFTER the ledger row is claimed under the
    /// store lock, so the in-lock report can say `committed` about a commit whose
    /// export or required origin push then failed. A redelivery of the same call
    /// must be answered with what actually happened — otherwise the guard tells
    /// the agent a head nobody outside Cairn can see has been published, and the
    /// row is immutable to competitors so nothing else can ever correct it.
    #[tokio::test]
    async fn a_failed_publication_is_replayed_as_unpublished_not_as_committed() {
        let (db, call) = ledger_db("write-replay-finalize.db").await;

        // Inside the store lock: the commit landed, so the row is claimed.
        assert_eq!(
            record_report(&db, &call, &report("committed")).await,
            LedgerClaim::Claimed
        );
        // After the lock: the export or push failed, so the report is corrected.
        finalize_report(&db, &call, &report("sealed locally; unpublished")).await;

        let replayed = recorded_report(&db, &call)
            .await
            .expect("a byte-identical redelivery finds the row");
        assert_eq!(status_of(&replayed), "sealed locally; unpublished");
        assert_eq!(
            status_of(&mark_as_replay(&replayed)),
            "sealed locally; unpublished"
        );
    }

    /// Correcting one's own report must not become a way to overwrite another
    /// delivery's. A second delivery that also reaches publication does not claim
    /// the row, and a non-claimant never finalizes — the first record still wins,
    /// because it is the one the agent may already have been answered with.
    #[tokio::test]
    async fn a_resource_redelivery_waits_for_the_claimants_settled_report() {
        let (db, call) = ledger_db("write-resource-replay-wait.db").await;
        let db = std::sync::Arc::new(db);
        assert!(matches!(
            claim_resource_call_with_timeout(&db, &call, std::time::Duration::from_secs(1)).await,
            ResourceClaim::Claimed
        ));

        let competing_db = std::sync::Arc::clone(&db);
        let competing_call = WriteCall {
            run_id: call.run_id.clone(),
            id: call.id.clone(),
        };
        let competing = tokio::spawn(async move {
            claim_resource_call_with_timeout(
                &competing_db,
                &competing_call,
                std::time::Duration::from_secs(1),
            )
            .await
        });

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(
            !competing.is_finished(),
            "the redelivery must wait while the first delivery is pending"
        );
        let settled = json!({ "applied": [{ "index": 0 }] }).to_string();
        finalize_report(&db, &call, &settled).await;

        match competing.await.unwrap() {
            ResourceClaim::Replayed(report) => assert_eq!(report, settled),
            _ => panic!("the redelivery did not receive the settled report"),
        }
    }

    #[tokio::test]
    async fn blocking_checkpoint_preserves_metadata_without_settling_a_resource_claim() {
        let (db, call) = ledger_db("blocking-checkpoint.db").await;
        assert!(matches!(
            claim_resource_call(&db, &call).await,
            ResourceClaim::Claimed
        ));
        let original = json!({
            "applied": [{"index": 0, "summary": "saved"}],
            "failures": [{"index": 1, "error": "refused"}],
            "commit": {"sha": "abc"}, "partial_success": true
        });
        let suffix = "\n<system-reminder>checks still pending</system-reminder>";
        let prior = format!("{original}{suffix}");
        checkpoint_blocking_report(&db, &call, LedgerClaim::NotClaimed, &prior)
            .await
            .unwrap();
        assert!(matches!(
            claim_resource_call_with_timeout(&db, &call, std::time::Duration::ZERO).await,
            ResourceClaim::Stalled
        ));
        let saved = recorded_report(&db, &call).await.unwrap();
        assert_eq!(blocking_checkpoint_report(&saved), Some(prior.as_str()));
        let child = r#"{"applied":[],"blocking":{"text":"Delegated task 'inner' suspended; the parent run will resume from the real child result(s)."}} "#;
        let result =
            complete_blocking_report(&db, &call.run_id, &call.id, child.to_string().into())
                .await
                .unwrap();
        assert!(result.ends_with(suffix));
        let mut decoded: serde_json::Value = serde_json::Deserializer::from_str(&result)
            .into_iter()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(decoded["blocking"]["text"], child);
        decoded.as_object_mut().unwrap().remove("blocking");
        assert_eq!(decoded, original);
        finalize_report(&db, &call, &result).await;
        assert!(
            matches!(claim_resource_call(&db, &call).await, ResourceClaim::Replayed(report) if report == result)
        );
    }

    #[tokio::test]
    async fn blocking_checkpoint_retains_file_claim_and_inserts_missing_ledger() {
        let (db, call) = ledger_db("blocking-file-checkpoint.db").await;
        let prior = report("committed");
        assert_eq!(
            record_report(&db, &call, &prior).await,
            LedgerClaim::Claimed
        );
        let updated =
            json!({"applied": [], "commit": {"sha": "abc"}, "partial_success": true}).to_string();
        checkpoint_blocking_report(&db, &call, LedgerClaim::Claimed, &updated)
            .await
            .unwrap();
        assert_eq!(
            recorded_report(&db, &call).await.as_deref(),
            Some(updated.as_str())
        );

        let (db, call) = ledger_db("blocking-missing-checkpoint.db").await;
        checkpoint_blocking_report(&db, &call, LedgerClaim::NotClaimed, &updated)
            .await
            .unwrap();
        let saved = recorded_report(&db, &call).await.unwrap();
        assert_eq!(blocking_checkpoint_report(&saved), Some(updated.as_str()));
        assert!(
            checkpoint_blocking_report(&db, &call, LedgerClaim::NotClaimed, &prior)
                .await
                .is_err()
        );
        assert_eq!(
            recorded_report(&db, &call).await.as_deref(),
            Some(saved.as_str())
        );
    }

    #[test]
    fn blocking_completion_keeps_child_data_opaque_without_a_prior_report() {
        let child = r#"{"applied":[],"blocking":{"text":"Prompt suspended"}}"#;
        let encoded = replace_blocking_payload(None, child.to_string().into()).unwrap();
        let report: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        assert_eq!(report["blocking"]["text"], child);
        assert_eq!(report["applied"], json!([]));
        assert!(replace_blocking_payload(Some("not a report"), child.to_string().into()).is_err());
    }

    #[tokio::test]
    async fn an_orphaned_resource_claim_fails_closed_after_a_bounded_wait() {
        let (db, call) = ledger_db("write-resource-replay-stalled.db").await;
        assert_eq!(
            record_report(&db, &call, RESOURCE_PENDING).await,
            LedgerClaim::Claimed
        );

        let started = tokio::time::Instant::now();
        assert!(matches!(
            claim_resource_call_with_timeout(&db, &call, std::time::Duration::from_millis(75))
                .await,
            ResourceClaim::Stalled
        ));
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
    }

    #[tokio::test]
    async fn a_competing_delivery_neither_claims_nor_overwrites_the_first_record() {
        let (db, call) = ledger_db("write-replay-competing.db").await;

        assert_eq!(
            record_report(&db, &call, &report("committed")).await,
            LedgerClaim::Claimed
        );
        assert_eq!(
            record_report(&db, &call, &report("amended")).await,
            LedgerClaim::NotClaimed,
            "the second delivery must not claim a row it did not insert"
        );

        assert_eq!(
            status_of(&recorded_report(&db, &call).await.unwrap()),
            "committed"
        );
    }

    #[tokio::test]
    async fn a_stale_file_claimant_cannot_overwrite_its_takeover() {
        let (db, call) = ledger_db("write-replay-owned-takeover.db").await;
        let stale = pending_report(&db, &call, &report("sealed"), 0).await;

        let (takeover, pending) = match await_file_replay_or_takeover_with_lease(
            &db,
            &call,
            std::time::Duration::from_millis(20),
            wall_clock_ms,
        )
        .await
        {
            FileReplayClaim::TakenOver { owner, report } => (owner, report),
            _ => panic!("stalled claim was not taken over"),
        };
        assert_eq!(status_of(&pending), "sealed");
        assert!(
            !finalize_owned_report(&db, &call, &stale, &report("stale-failure")).await,
            "the replaced token must not finalize"
        );
        assert!(finalize_owned_report(&db, &call, &takeover, &report("settled")).await);
        assert_eq!(
            status_of(&recorded_report(&db, &call).await.unwrap()),
            "settled"
        );
    }

    #[tokio::test]
    async fn a_file_redelivery_waits_for_the_current_owners_final_report() {
        let (db, call) = ledger_db("write-replay-owned-wait.db").await;
        // The lease clock stands still, so only the owner's final report can end
        // the wait.
        let clock = LeaseClock::at(1_000_000);
        let owner = pending_report(&db, &call, &report("sealed"), clock.now()).await;
        let db = std::sync::Arc::new(db);
        let waiter_db = db.clone();
        let waiter_call = WriteCall {
            run_id: call.run_id.clone(),
            id: call.id.clone(),
        };
        let waiter = tokio::spawn(async move {
            await_file_replay_or_takeover_with_lease(
                &waiter_db,
                &waiter_call,
                std::time::Duration::from_secs(1),
                move || clock.now(),
            )
            .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(75)).await;
        assert!(!waiter.is_finished());
        assert!(finalize_owned_report(&db, &call, &owner, &report("settled")).await);
        match waiter.await.unwrap() {
            FileReplayClaim::Replayed(settled) => assert_eq!(status_of(&settled), "settled"),
            _ => panic!("redelivery did not replay the owner's final report"),
        }
    }
}
