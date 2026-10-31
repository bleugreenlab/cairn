//! What a node scratch costs a placement: per connected machine, the bytes it
//! is missing over its measured sync throughput.
//!
//! Missing bytes come from the authority alone, in O(1) per machine: zero for a
//! replica already at the head; the monotonic `written_bytes` since its last
//! sync (capped at the head's size) for an older one; the whole head for a
//! machine with none. This is affinity, never a filter: a cold machine is
//! priced by how long the sync would take and stays selectable.

use cairn_common::executor_protocol::ScratchSyncEstimate;

use super::owner::{self, ResolvedScratch};
use crate::orchestrator::Orchestrator;

fn transfer_ms(bytes: u64, bytes_per_second: f64) -> u64 {
    if bytes == 0 || bytes_per_second <= 0.0 {
        return 0;
    }
    ((bytes as f64 / bytes_per_second) * 1000.0).ceil() as u64
}

/// Bytes a machine is missing, given what it last synced.
pub fn missing_bytes(
    head_commit: Option<&str>,
    head_bytes: u64,
    written_bytes: u64,
    synced: Option<(&str, u64)>,
) -> u64 {
    let Some(head) = head_commit else {
        return 0;
    };
    match synced {
        Some((commit, _)) if commit == head => 0,
        Some((_, synced_written)) => written_bytes.saturating_sub(synced_written).min(head_bytes),
        None => head_bytes,
    }
}

pub async fn estimate(orch: &Orchestrator, resolved: &ResolvedScratch) -> ScratchSyncEstimate {
    let scratch = &resolved.scratch;
    let plane = &orch.object_plane.scratch;
    let uncovered_ms = transfer_ms(
        scratch.head_bytes,
        super::plane::UNMEASURED_REMOTE_BYTES_PER_SECOND,
    );
    if scratch.head_commit.is_none() {
        return ScratchSyncEstimate::default();
    }
    let recorded = owner::replicas(&orch.db.local, &scratch.id)
        .await
        .unwrap_or_default();
    let advertised = orch.fleet.node_scratch_replicas(&scratch.id);
    let mut estimate = ScratchSyncEstimate {
        uncovered_ms,
        ..ScratchSyncEstimate::default()
    };
    for (executor_id, colocated) in orch.fleet.connected_executors() {
        let recorded = recorded
            .iter()
            .find(|replica| replica.executor_id == executor_id);
        // A machine's own advertisement is the freshest word on whether it is
        // already at the head.
        let advertised_current = advertised.iter().any(|replica| {
            replica.executor_id == executor_id
                && replica.replica.synced_commit.is_some()
                && replica.replica.synced_commit == scratch.head_commit
        });
        let missing = if advertised_current {
            0
        } else {
            missing_bytes(
                scratch.head_commit.as_deref(),
                scratch.head_bytes,
                scratch.written_bytes,
                recorded.and_then(|replica| {
                    replica
                        .synced_commit
                        .as_deref()
                        .map(|commit| (commit, replica.synced_written_bytes))
                }),
            )
        };
        let ms = transfer_ms(missing, plane.throughput(&executor_id, colocated));
        estimate.per_executor_ms.insert(executor_id, ms);
    }
    estimate
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_bytes_are_zero_at_the_head_growth_since_the_last_sync_and_the_head_when_cold() {
        let head = "a".repeat(40);
        assert_eq!(missing_bytes(Some(&head), 100, 300, Some((&head, 300))), 0);
        assert_eq!(
            missing_bytes(Some(&head), 100, 300, Some((&"b".repeat(40), 260))),
            40
        );
        assert_eq!(
            missing_bytes(Some(&head), 100, 900, Some((&"b".repeat(40), 0))),
            100,
            "capped at the head's size"
        );
        assert_eq!(missing_bytes(Some(&head), 100, 300, None), 100);
        assert_eq!(missing_bytes(None, 0, 0, None), 0);
    }

    #[test]
    fn the_price_is_bytes_over_throughput() {
        assert_eq!(transfer_ms(10 * 1024 * 1024, 10.0 * 1024.0 * 1024.0), 1000);
        assert_eq!(transfer_ms(0, 1.0), 0);
    }
}
