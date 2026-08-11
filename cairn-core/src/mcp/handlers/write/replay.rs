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

/// The provider call one delivery of a `write` belongs to.
pub(super) struct WriteCall {
    run_id: String,
    id: String,
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
const RESOURCE_PENDING: &str = "__cairn_write_resource_pending__";

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
            Some(report) if report != RESOURCE_PENDING => return ResourceClaim::Replayed(report),
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
            cairn_db::turso::params![run_id, tool_use_id, report, now],
        )
        .await
    {
        Ok(1) => LedgerClaim::Claimed,
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

    const HOME_URI: &str = "cairn://p/CAIRN/3264/1/builder";

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
                "target": "cairn://p/CAIRN/3264/1/builder/todos",
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
                "target": "cairn://p/CAIRN/3264",
                "mode": "append",
                "payload": { "content": "a comment" }
            }]
        });
        let other = json!({
            "changes": [{
                "target": "cairn://p/CAIRN/3264",
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
}
