//! The harness executor answers offers with the production lifecycle.
//!
//! `common::answer_offer` and `common::forget_lost_claim` stand in for the
//! executor's link handler in every integration test, so what they get wrong
//! every executor-backed suite inherits. The property that matters is that a
//! request identity is bound only by a claim that stands: a decline must not
//! bind it, and a lost claim must release it, or the next offer for the same
//! request is declined as already held for the life of the process.

use crate::common;

use cairn_common::executor_protocol::{
    CellAdmissionKind, CellPriority, DeclineReason, ExecutorMessage, OfferSummary,
    RepositoryLocator, ResourceReservation,
};
use cairn_exec::{ExecutorRuntime, Fleet as ExecutorPool};

fn offer(request_id: &str, project_id: &str) -> OfferSummary {
    OfferSummary {
        request_id: request_id.into(),
        attempt_id: "attempt".into(),
        project_id: project_id.into(),
        repository: RepositoryLocator::ScratchOnly {
            owner_id: project_id.into(),
        },
        base_commit: "base".into(),
        reservation: ResourceReservation {
            memory_bytes: 64,
            ..ResourceReservation::default()
        },
        admission_kind: CellAdmissionKind::Command,
        interactive: false,
        priority: CellPriority::ReviewCheck,
        waiting_since_unix_ms: 0,
        wait_horizon_unix_ms: common::unix_time_ms() + 60_000,
        predicted_cost_ms: None,
    }
}

fn pool() -> (tempfile::TempDir, ExecutorPool) {
    let temp = tempfile::tempdir().unwrap();
    let pool = ExecutorPool::new(ExecutorRuntime::new(temp.path().join("home")));
    (temp, pool)
}

/// A declined offer binds nothing. Once the capacity that declined it comes
/// back, the same request is claimed rather than refused as already held.
#[test]
fn a_decline_leaves_no_identity_bound_and_the_reoffer_claims_after_capacity_returns() {
    let (_temp, pool) = pool();
    let occupying = offer("first", "project");
    assert!(matches!(
        common::answer_offer(&pool, "entry-1".into(), &occupying),
        ExecutorMessage::Claim { .. }
    ));
    // The project is provisioning behind the first claim, so the second
    // request is declined from admission, not from identity.
    let contended = offer("second", "project");
    match common::answer_offer(&pool, "entry-2".into(), &contended) {
        ExecutorMessage::Decline { reason, .. } => {
            assert_eq!(reason, DeclineReason::ProjectProvisioning)
        }
        other => panic!("expected a project-provisioning decline, got {other:?}"),
    }
    assert!(
        pool.bind_request_identity("second"),
        "a decline must not leave the request identity bound"
    );
    pool.release_request_identity("second");
    // Capacity returns when the first claim is lost.
    common::forget_lost_claim(&pool, "entry-1");
    assert!(
        matches!(
            common::answer_offer(&pool, "entry-2b".into(), &contended),
            ExecutorMessage::Claim { .. }
        ),
        "the reoffer after capacity returned must claim"
    );
}

/// A claim the runner reports lost releases the identity it bound, so the same
/// request can be offered to this machine again and claimed.
#[test]
fn a_lost_claim_releases_its_identity_and_the_reoffer_claims() {
    let (_temp, pool) = pool();
    let summary = offer("request", "project");
    assert!(matches!(
        common::answer_offer(&pool, "entry-1".into(), &summary),
        ExecutorMessage::Claim { .. }
    ));
    common::forget_lost_claim(&pool, "entry-1");
    assert!(
        matches!(
            common::answer_offer(&pool, "entry-2".into(), &summary),
            ExecutorMessage::Claim { .. }
        ),
        "a request whose earlier claim was lost must be claimable again"
    );
}

/// A claim whose identity is already live on the machine is a duplicate of
/// held work: the capacity goes back and the live binding is left alone.
#[test]
fn a_duplicate_identity_declines_for_constraint_and_keeps_the_live_binding() {
    let (_temp, pool) = pool();
    assert!(pool.bind_request_identity("held"));
    let summary = offer("held", "project");
    match common::answer_offer(&pool, "entry-1".into(), &summary) {
        ExecutorMessage::Decline { reason, .. } => assert_eq!(reason, DeclineReason::Constraint),
        other => panic!("expected a constraint decline, got {other:?}"),
    }
    assert!(
        !pool.bind_request_identity("held"),
        "the live binding must survive the duplicate's decline"
    );
    // The capacity the duplicate claimed is back: another project's offer is
    // not blocked by a provisioning marker the duplicate left behind.
    assert!(matches!(
        common::answer_offer(&pool, "entry-2".into(), &offer("other", "project")),
        ExecutorMessage::Claim { .. }
    ));
}
